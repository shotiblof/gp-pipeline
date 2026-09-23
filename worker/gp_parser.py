"""gayporno.fm catalog parser."""
from __future__ import annotations

import json
import os
import re
import time
from dataclasses import dataclass
from typing import Any
from urllib.parse import unquote, urlparse

import httpx

from peach_db import abs_url, db_conn, get_setting
from pipeline_config import MAX_BACKLOG_PAGES, MAX_EMPTY_PAGES, MAX_NEW, MAX_VIDEOS, REQUEST_DELAY_SEC
from queue_guard import parser_ingest_allowed

SITE = "https://www.gayporno.fm"
ORIGIN_DEFAULT = SITE
ID_PREFIX = "gp"

CATEGORIES = ("homemade",)

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml",
    "Accept-Language": "ru-RU,ru;q=0.9,en;q=0.8",
    "Referer": f"{SITE}/ru/",
}

# <a class="js-gallery-stats js-gallery-link" href="/ru/next-door-homemade-alpha-studios-bonanza_2940601.html" data-position="72" data-gallery-id="2940601" data-thumb-id="17732928" title="Next Door Homemade: Alpha Studios Bonanza"
ITEM_RE = re.compile(
    r'<a[^>]*class="[^"]*js-gallery-link[^"]*"[^>]*href="(/ru/[^"]+_(\d+)\.html)"[^>]*title="([^"]*)"',
    re.I,
)
DURATION_RE = re.compile(
    r'<span class="b-thumb-item__duration">(\d+):(\d{2}(?::\d{2})?)</span>',
    re.I
)
IMAGE_RE = re.compile(
    r'<img[^>]*data-src="([^"]+\.jpg)"[^>]*alt="([^"]*)"',
    re.I
)

SOURCE_RE = re.compile(r'<source.*?src="([^"]+)"', re.I)


@dataclass
class ListCard:
    id: str
    numeric_id: str
    video_path: str
    slug: str
    title: str
    preview_path: str
    poster_path: str
    duration_seconds: int | None
    category: str


def _sleep() -> None:
    time.sleep(REQUEST_DELAY_SEC)


def _fetch(client: httpx.Client, url: str) -> str:
    _sleep()
    res = client.get(url, timeout=30.0)
    res.raise_for_status()
    return res.text


def _normalize_path(href: str) -> str:
    if href.startswith("/"):
        return href
    parsed = urlparse(href)
    return parsed.path or href


def _parse_duration(duration_str: str) -> int | None:
    parts = duration_str.split(":")
    if len(parts) == 3:
        return int(parts[0]) * 3600 + int(parts[1]) * 60 + int(parts[2])
    elif len(parts) == 2:
        return int(parts[0]) * 60 + int(parts[1])
    return None


def video_id(numeric: str | int) -> str:
    return f"{ID_PREFIX}{numeric}"


def parse_cards(html: str, *, category: str = "") -> list[ListCard]:
    cards: list[ListCard] = []
    seen: set[str] = set()
    
    # We need to correlate title/image/duration since they are in separate tags within a block.
    # We will split by <div class="b-thumb-item">
    blocks = html.split('<div class="b-thumb-item')[1:]
    
    for block in blocks:
        item_match = ITEM_RE.search(block)
        if not item_match:
            continue
            
        href, numeric, title = item_match.groups()
        vid = video_id(numeric)
        if vid in seen:
            continue
        seen.add(vid)
        
        slug = f"video_{numeric}"
        
        dur_match = DURATION_RE.search(block)
        duration = _parse_duration(dur_match.group(1) + ":" + dur_match.group(2)) if dur_match else None
        
        img_match = IMAGE_RE.search(block)
        poster = img_match.group(1) if img_match else ""
        
        cards.append(
            ListCard(
                id=vid,
                numeric_id=str(numeric),
                video_path=_normalize_path(href),
                slug=slug,
                title=title.strip(),
                preview_path="",
                poster_path=poster,
                duration_seconds=duration,
                category=category,
            )
        )
    return cards


def category_list_path(slug: str, page: int = 1) -> str:
    if page <= 1:
        return f"/ru/category/{slug}"
    return f"/ru/category/{slug}?page={page}"


def video_exists(conn: Any, vid: str) -> bool:
    row = conn.execute("SELECT 1 FROM videos WHERE id = %s", (vid,)).fetchone()
    return row is not None


def save_parsed(conn: Any, card: ListCard) -> None:
    conn.execute(
        """
        INSERT INTO videos (
          id, video_path, preview_path, poster_path, duration_seconds,
          title_ru, description_ru,
          title_en, description_en,
          category, category_en, tags, tags_en, status, parsed_at, updated_at
        ) VALUES (
          %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s::jsonb, %s::jsonb, 'parsed', now(), now()
        )
        ON CONFLICT (id) DO NOTHING
        """,
        (
            card.id,
            card.video_path,
            card.preview_path,
            card.poster_path,
            card.duration_seconds,
            card.title,
            "",
            "",
            "",
            card.category,
            card.category,
            json.dumps([], ensure_ascii=False),
            json.dumps([], ensure_ascii=False),
        ),
    )


def _parser_mode() -> str:
    mode = os.environ.get("PARSER_MODE", "full").strip().lower()
    if mode not in ("latest", "backlog", "full"):
        raise ValueError(f"invalid PARSER_MODE: {mode!r}")
    return mode


def _ingest_card(conn: Any, card: ListCard) -> bool:
    if video_exists(conn, card.id):
        return False
    save_parsed(conn, card)
    print(f"parser: queued {card.id} {card.category} {card.title[:60]!r}")
    return True


def _parse_latest(conn: Any, client: httpx.Client, origin: str, *, processed: int) -> int:
    new_count = 0
    for slug in CATEGORIES:
        if processed >= MAX_VIDEOS or new_count >= MAX_NEW:
            break
        html = _fetch(client, abs_url(origin, category_list_path(slug, 1)))
        for card in parse_cards(html, category=slug):
            if processed >= MAX_VIDEOS or new_count >= MAX_NEW:
                break
            if _ingest_card(conn, card):
                processed += 1
                new_count += 1
    return processed


def _load_backlog_cursor(conn: Any) -> tuple[int, int]:
    cat_raw = get_setting(conn, "gp.backlog_cat_index", "0")
    page_raw = get_setting(conn, "gp.backlog_page", "2")
    try:
        cat_index = int(cat_raw)
    except ValueError:
        cat_index = 0
    try:
        page = int(page_raw)
    except ValueError:
        page = 2
    if cat_index < 0 or cat_index >= len(CATEGORIES):
        cat_index = 0
    if page < 2:
        page = 2
    return cat_index, page


def _save_backlog_cursor(conn: Any, cat_index: int, page: int, *, complete: bool = False) -> None:
    conn.execute(
        """
        INSERT INTO app_settings (key, value, updated_at)
        VALUES ('gp.backlog_cat_index', %s, now())
        ON CONFLICT (key) DO UPDATE SET value = excluded.value, updated_at = now()
        """,
        (str(cat_index),),
    )
    conn.execute(
        """
        INSERT INTO app_settings (key, value, updated_at)
        VALUES ('gp.backlog_page', %s, now())
        ON CONFLICT (key) DO UPDATE SET value = excluded.value, updated_at = now()
        """,
        (str(page),),
    )
    if complete:
        pass


def _parse_backlog(conn: Any, client: httpx.Client, origin: str, *, processed: int) -> int:
    cat_index, page = _load_backlog_cursor(conn)
    empty_streak = 0
    for _ in range(MAX_BACKLOG_PAGES):
        if processed >= MAX_VIDEOS:
            break
        if cat_index >= len(CATEGORIES):
            _save_backlog_cursor(conn, 0, 2, complete=True)
            print("parser: gp backlog complete")
            break

        slug = CATEGORIES[cat_index]
        html = _fetch(client, abs_url(origin, category_list_path(slug, page)))
        cards = parse_cards(html, category=slug)
        before = processed
        for card in cards:
            if processed >= MAX_VIDEOS:
                break
            if _ingest_card(conn, card):
                processed += 1

        if not cards:
            # Reached end of pagination
            empty_streak += 1
            if empty_streak >= MAX_EMPTY_PAGES:
                cat_index += 1
                page = 2
                empty_streak = 0
                _save_backlog_cursor(conn, cat_index, page)
                continue
        else:
            empty_streak = 0

        page += 1
        _save_backlog_cursor(conn, cat_index, page)
    return processed


def extract_mp4_url(page_html: str) -> str | None:
    match = SOURCE_RE.search(page_html or "")
    if not match:
        return None
    return match.group(1).strip()


def run_parser() -> int:
    mode = _parser_mode()
    processed = 0
    with db_conn() as conn:
        if not parser_ingest_allowed(conn):
            return 0
        origin = get_setting(conn, "gp.source_origin", ORIGIN_DEFAULT).rstrip("/")
        
        with httpx.Client(headers=HEADERS, follow_redirects=True) as client:
            if mode in ("latest", "full"):
                processed = _parse_latest(conn, client, origin, processed=processed)
            if mode in ("backlog", "full"):
                processed = _parse_backlog(conn, client, origin, processed=processed)
        conn.commit()
    print(f"parser[{mode}]: processed {processed} gp video(s)")
    return processed


if __name__ == "__main__":
    run_parser()
