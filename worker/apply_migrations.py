"""Apply pipeline SQLite schema and force gayporno (gp) settings.

This file is published to shotiblof/gp-pipeline as worker/apply_migrations.py
(workflows call apply_migrations.py).
"""
from __future__ import annotations

from peach_db import SCHEMA_PATH, db_conn


def apply_migrations() -> None:
    if not SCHEMA_PATH.is_file():
        raise RuntimeError(f"missing schema: {SCHEMA_PATH}")
    with db_conn() as conn:
        settings = {
            "parser.primary_source": ("gp", "Catalog source (gayporno only)"),
            "gp.source_origin": ("https://www.gayporno.fm", "gayporno.fm origin"),
            "namevids.caption_link": ("", "gp pipeline caption link"),
        }
        for key, (value, description) in settings.items():
            conn.execute(
                """
                INSERT INTO app_settings (key, value, description, updated_at)
                VALUES (%s, %s, %s, datetime('now'))
                ON CONFLICT (key) DO UPDATE SET
                  value = excluded.value,
                  description = excluded.description,
                  updated_at = datetime('now')
                """,
                (key, value, description),
            )
        row = conn.execute(
            "SELECT value FROM app_settings WHERE key = %s",
            ("gp.backlog_page",),
        ).fetchone()
        try:
            page = int((row or {}).get("value") or "2")
        except (TypeError, ValueError):
            page = 2
        if page > 50:
            conn.execute(
                """
                INSERT INTO app_settings (key, value, updated_at)
                VALUES ('gp.backlog_page', '2', datetime('now'))
                ON CONFLICT (key) DO UPDATE SET value = '2', updated_at = datetime('now')
                """
            )
            print(f"migrations: reset gp.backlog_page {page} → 2")
        tables = conn.execute(
            "SELECT count(*) AS n FROM sqlite_master WHERE type='table'"
        ).fetchone()
        print(f"migrations: sqlite schema ready ({(tables or {}).get('n', 0)} tables), source=gp")
        conn.commit()


if __name__ == "__main__":
    apply_migrations()
