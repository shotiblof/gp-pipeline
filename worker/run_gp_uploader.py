"""gayporno → namevids uploader. Title only, full source mp4, custom caption."""
from __future__ import annotations

import json
import os
import tempfile
import time
from pathlib import Path
from typing import Any
from urllib.parse import quote, urlsplit, urlunsplit

import httpx
import re

from namevids_client import (
    clear_pending_drafts,
    delete_draft_fid,
    fetch_api_key,
    login,
    parse_account_metadata,
    publish,
    sanitize_namevids_title,
    upload_stream,
)
from peach_db import abs_url, db_conn, get_setting
from pipeline_config import MAX_PER_RUN, NAMEVIDS_DAILY_CAP

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml",
    "Accept-Language": "ru-RU,ru;q=0.9,en;q=0.8",
}

DOWNLOAD_TIMEOUT = httpx.Timeout(30.0, read=180.0)
HTTP_TIMEOUT = httpx.Timeout(30.0, read=60.0)
STUCK_UPLOADING_MINUTES = int(os.environ.get("STUCK_UPLOADING_MINUTES", "15"))
MIN_DOWNLOAD_BYTES = 50 * 1024
GAP_SEC = float(os.environ.get("UPLOADER_GAP_SEC", "30"))

CUSTOM_CAPTION = (
    "More content on Telegram: https://t.me/gaypartytg\n"
    "More content on Telegram: https://t.me/gaypartytg\n"
    "More content on Telegram: https://t.me/gaypartytg"
)


def _encode_url(url: str) -> str:
    parts = urlsplit(url)
    path = quote(parts.path, safe="/")
    return urlunsplit((parts.scheme, parts.netloc, path, parts.query, parts.fragment))


def _reset_stuck_uploading(conn) -> int:
    cur = conn.execute(
        """
        UPDATE videos
        SET status = 'parsed',
            error_message = 'reset: previous upload timed out',
            updated_at = now()
        WHERE status = 'uploading'
          AND updated_at < now() - make_interval(mins => %s)
        RETURNING id
        """,
        (STUCK_UPLOADING_MINUTES,),
    )
    ids = [str(row["id"]) for row in cur.fetchall()]
    if ids:
        print(f"uploader: reset stuck uploading: {', '.join(ids)}")

    cur_fk = conn.execute(
        """
        UPDATE videos
        SET status = 'parsed',
            error_message = NULL,
            updated_at = now()
        WHERE status = 'failed'
          AND id LIKE 'gp%%'
          AND error_message LIKE '%%FOREIGN KEY%%'
        RETURNING id
        """
    )
    fk_ids = [str(row["id"]) for row in cur_fk.fetchall()]
    if fk_ids:
        print(f"uploader: reset foreign key failed videos: {', '.join(fk_ids)}")

    return len(ids) + len(fk_ids)


def _claim_videos(conn, limit: int) -> list[dict[str, Any]]:
    rows = conn.execute(
        """
        UPDATE videos
        SET status = 'uploading', updated_at = now()
        WHERE id IN (
            SELECT id FROM videos
            WHERE status = 'parsed'
              AND id LIKE 'gp%%'
              AND (
                error_message IS NULL
                OR error_message = ''
                OR (
                  error_message NOT LIKE '%%429%%'
                  AND error_message NOT LIKE '%%too fast%%'
                )
                OR updated_at < now() - interval '60 minutes'
              )
            ORDER BY
              CASE
                WHEN error_message IS NULL OR error_message = '' THEN 0
                ELSE 1
              END,
              duration_seconds ASC NULLS LAST,
              parsed_at ASC,
              id ASC
            LIMIT %s
            FOR UPDATE SKIP LOCKED
        )
        RETURNING *
        """,
        (limit,),
    ).fetchall()
    return [dict(row) for row in rows]


def _release_unprocessed(queue: list[dict[str, Any]], processed: int) -> None:
    remaining = [str(row["id"]) for row in queue[processed:]]
    if not remaining:
        return
    with db_conn() as conn:
        conn.execute(
            """
            UPDATE videos
            SET status = 'parsed', updated_at = now()
            WHERE id = ANY(%s) AND status = 'uploading'
            """,
            (remaining,),
        )
        conn.commit()
    print(f"uploader: released {len(remaining)} unprocessed claim(s)")


def _mark_published(video_id: str, *, file_id: str, namevids_account_id: int) -> None:
    with db_conn() as conn:
        conn.execute(
            """
            UPDATE videos SET
              status = 'published',
              namevids_file_id = %s,
              namevids_account_id = %s,
              published_at = now(),
              updated_at = now(),
              error_message = NULL
            WHERE id = %s
            """,
            (file_id, namevids_account_id, video_id),
        )
        conn.commit()


def _mark_failed(video_id: str, error: str) -> None:
    with db_conn() as conn:
        conn.execute(
            """
            UPDATE videos
            SET status = 'failed', error_message = %s, updated_at = now()
            WHERE id = %s AND status = 'uploading'
            """,
            (error[:500], video_id),
        )
        conn.commit()


def _namevids_published_today(conn, namevids_account_id: int) -> int:
    row = conn.execute(
        """
        SELECT count(*)::int AS n
        FROM videos
        WHERE status = 'published'
          AND namevids_account_id = %s
          AND coalesce(published_at, updated_at) >= date_trunc('day', now() AT TIME ZONE 'UTC')
        """,
        (namevids_account_id,),
    ).fetchone()
    return int(row["n"] if row else 0)


def _recent_namevids_rate_limits(conn) -> int:
    row = conn.execute(
        """
        SELECT count(*)::int AS n
        FROM videos
        WHERE error_message ILIKE '%%too fast%%'
          AND id LIKE 'gp%%'
          AND updated_at > now() - interval '30 minutes'
        """
    ).fetchone()
    return int(row["n"] if row else 0)


def _download_mp4(url: str, dest: Path) -> None:
    encoded = _encode_url(url)
    with httpx.Client(headers=HEADERS, timeout=DOWNLOAD_TIMEOUT, follow_redirects=True) as client:
        with client.stream("GET", encoded) as res:
            res.raise_for_status()
            written = 0
            with dest.open("wb") as handle:
                for chunk in res.iter_bytes():
                    handle.write(chunk)
                    written += len(chunk)
            if written < MIN_DOWNLOAD_BYTES:
                raise RuntimeError(f"Download too small ({written} bytes)")
            print(f"uploader: clip size {written / (1024 * 1024):.1f} MB")


def _resolve_mp4_url(origin: str, row: dict[str, Any]) -> str:
    video_path = str(row.get("video_path") or "").strip()
    if not video_path:
        raise RuntimeError("No video_path")
    with httpx.Client(headers=HEADERS, timeout=HTTP_TIMEOUT, follow_redirects=True) as client:
        page = client.get(abs_url(origin, video_path))
        page.raise_for_status()
    
    match = re.search(r'<source.*?src="([^"]+)"', page.text, re.I)
    if not match:
        raise RuntimeError("No mp4 on video page")
    return match.group(1).strip()


def _upload_one(row: dict[str, Any], *, origin: str, namevids_acc: dict[str, Any]) -> None:
    vid = str(row["id"])
    mp4_url = _resolve_mp4_url(origin, row)
    
    print(f"uploader: logging in to namevids with {namevids_acc['login']}")
    nv_client = login(
        str(namevids_acc["login"]),
        str(namevids_acc["secret"]),
        metadata=parse_account_metadata(namevids_acc.get("metadata")),
    )
    file_id = ""
    api_key = ""
    try:
        with tempfile.TemporaryDirectory() as tmp:
            clip_path = Path(tmp) / f"{vid}.mp4"
            print(f"uploader: download gp mp4 for {vid}")
            _download_mp4(mp4_url, clip_path)
            api_key = fetch_api_key(nv_client)
            file_id = upload_stream(nv_client, api_key, str(clip_path), f"{vid}.mp4")
            title = sanitize_namevids_title(
                title_ru=str(row.get("title_ru") or ""),
                title_en=str(row.get("title_en") or ""),
                video_id=vid,
            )
            print(f"uploader: {vid} namevids title={title!r}")
            publish(
                nv_client,
                api_key,
                file_id,
                title,
                "-",
                fallback_caption=CUSTOM_CAPTION,
                fallback_title=vid,
            )
        _mark_published(
            vid,
            file_id=file_id,
            namevids_account_id=int(namevids_acc["id"]),
        )
        print(f"uploader: published {vid}")
    except Exception:
        if file_id and api_key:
            try:
                delete_draft_fid(nv_client, api_key, file_id)
            except Exception as cleanup_exc:
                print(f"uploader: {vid} draft cleanup failed: {cleanup_exc}")
        raise
    finally:
        nv_client.close()


def _ensure_upload_account(conn, login_str: str, secret_str: str) -> dict[str, Any]:
    conn.execute(
        """
        INSERT INTO upload_accounts (provider, name, login, secret, is_enabled, priority, updated_at)
        VALUES ('namevids', 'gp', %s, %s, 1, 10, now())
        ON CONFLICT (provider, name) DO UPDATE SET
          login = EXCLUDED.login,
          secret = EXCLUDED.secret,
          is_enabled = 1,
          updated_at = now()
        """,
        (login_str, secret_str),
    )
    row = conn.execute(
        """
        SELECT id, provider, name, login, secret, metadata
        FROM upload_accounts
        WHERE provider = 'namevids' AND name = 'gp'
        """
    ).fetchone()
    if not row:
        raise RuntimeError("Failed to resolve gp upload_account in upload_accounts")
    data = dict(row)
    meta = data.get("metadata")
    if isinstance(meta, str) and meta.strip():
        try:
            data["metadata"] = json.loads(meta)
        except Exception:
            data["metadata"] = {}
    elif not meta:
        data["metadata"] = {}
    return data


def run_uploader() -> int:
    done = 0
    gp_login = os.environ.get("GP_NAMEVIDS_LOGIN", "").strip()
    gp_password = os.environ.get("GP_NAMEVIDS_PASSWORD", "").strip()
    
    if not gp_login or not gp_password:
        raise RuntimeError("GP_NAMEVIDS_LOGIN or GP_NAMEVIDS_PASSWORD missing in env")

    with db_conn() as conn:
        namevids_acc = _ensure_upload_account(conn, gp_login, gp_password)
        origin = get_setting(conn, "gp.source_origin", "https://www.gayporno.fm").rstrip("/")
        _reset_stuck_uploading(conn)
        published_today = _namevids_published_today(conn, int(namevids_acc["id"]))
        if published_today >= NAMEVIDS_DAILY_CAP:
            print(
                f"uploader: namevids daily cap {NAMEVIDS_DAILY_CAP} reached "
                f"({published_today} today on account {namevids_acc['login']}) — skip run"
            )
            return 0
        recent_rl = _recent_namevids_rate_limits(conn)
        if recent_rl >= 3:
            print(f"uploader: namevids rate-limited ({recent_rl} recent) — skip run")
            return 0
        remaining_today = NAMEVIDS_DAILY_CAP - published_today
        batch_size = 1 if recent_rl >= 1 else MAX_PER_RUN
        batch_size = min(batch_size, remaining_today)
        if batch_size < 1:
            return 0
        queue = _claim_videos(conn, batch_size)
        conn.commit()

    if not queue:
        print("uploader: queue empty")
        return 0

    prep_client = login(
        str(namevids_acc["login"]),
        str(namevids_acc["secret"]),
        metadata=parse_account_metadata(namevids_acc.get("metadata")),
    )
    try:
        prep_key = fetch_api_key(prep_client)
        clear_pending_drafts(prep_client, prep_key)
    finally:
        prep_client.close()

    processed = 0
    try:
        for index, row in enumerate(queue):
            vid = str(row["id"])
            try:
                _upload_one(row, origin=origin, namevids_acc=namevids_acc)
                done += 1
            except Exception as exc:
                print(f"uploader: {vid} failed: {exc}")
                _mark_failed(vid, str(exc))
            processed += 1
            if index + 1 < len(queue) and GAP_SEC > 0:
                time.sleep(GAP_SEC)
    finally:
        _release_unprocessed(queue, processed)

    print(f"uploader: done {done} video(s)")
    return done


if __name__ == "__main__":
    run_uploader()
