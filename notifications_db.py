import sqlite3
import time

from db_path import get_db_path


def init_notify_table() -> None:
    conn = sqlite3.connect(get_db_path())
    try:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS notify_settings (
                chat_id INTEGER NOT NULL,
                feature TEXT NOT NULL,
                enabled INTEGER NOT NULL DEFAULT 0,
                updated_at INTEGER NOT NULL,
                PRIMARY KEY (chat_id, feature)
            )
            """
        )
        conn.commit()
    finally:
        conn.close()


def set_notify(chat_id: int, feature: str, enabled: bool) -> None:
    now = int(time.time())
    conn = sqlite3.connect(get_db_path())
    try:
        conn.execute(
            """
            INSERT INTO notify_settings (chat_id, feature, enabled, updated_at)
            VALUES (?, ?, ?, ?)
            ON CONFLICT(chat_id, feature)
            DO UPDATE SET enabled = excluded.enabled, updated_at = excluded.updated_at
            """,
            (chat_id, feature, 1 if enabled else 0, now),
        )
        conn.commit()
    finally:
        conn.close()


def is_notify_enabled(chat_id: int, feature: str) -> bool:
    conn = sqlite3.connect(get_db_path())
    try:
        cur = conn.cursor()
        cur.execute(
            "SELECT enabled FROM notify_settings WHERE chat_id = ? AND feature = ?",
            (chat_id, feature),
        )
        row = cur.fetchone()
        return bool(row[0]) if row else False
    finally:
        conn.close()


def list_enabled(feature: str) -> list[int]:
    conn = sqlite3.connect(get_db_path())
    try:
        cur = conn.cursor()
        cur.execute(
            "SELECT chat_id FROM notify_settings WHERE feature = ? AND enabled = 1",
            (feature,),
        )
        rows = cur.fetchall()
        return [row[0] for row in rows]
    finally:
        conn.close()
