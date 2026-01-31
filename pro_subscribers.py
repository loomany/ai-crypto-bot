import sqlite3
import time
from typing import List

from db_path import get_db_path


def init_pro_tables() -> None:
    conn = sqlite3.connect(get_db_path())
    try:
        conn.execute(
            "CREATE TABLE IF NOT EXISTS pro_subscribers (chat_id INTEGER PRIMARY KEY)"
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS pro_daily_limits (
                chat_id INTEGER NOT NULL,
                date TEXT NOT NULL,
                sent_count INTEGER NOT NULL DEFAULT 0,
                updated_at INTEGER NOT NULL,
                PRIMARY KEY (chat_id, date)
            )
            """
        )
        conn.commit()
    finally:
        conn.close()


def pro_add_subscription(chat_id: int) -> bool:
    conn = sqlite3.connect(get_db_path())
    try:
        cur = conn.cursor()
        cur.execute(
            "INSERT OR IGNORE INTO pro_subscribers (chat_id) VALUES (?)",
            (chat_id,),
        )
        conn.commit()
        return cur.rowcount > 0
    finally:
        conn.close()


def pro_remove_subscription(chat_id: int) -> bool:
    conn = sqlite3.connect(get_db_path())
    try:
        cur = conn.cursor()
        cur.execute("DELETE FROM pro_subscribers WHERE chat_id = ?", (chat_id,))
        conn.commit()
        return cur.rowcount > 0
    finally:
        conn.close()


def pro_list_subscribers() -> List[int]:
    conn = sqlite3.connect(get_db_path())
    try:
        cur = conn.cursor()
        cur.execute("SELECT chat_id FROM pro_subscribers")
        return [row[0] for row in cur.fetchall()]
    finally:
        conn.close()


def pro_get_daily_count(chat_id: int, date_str: str) -> int:
    conn = sqlite3.connect(get_db_path())
    try:
        cur = conn.cursor()
        cur.execute(
            "SELECT sent_count FROM pro_daily_limits WHERE chat_id = ? AND date = ?",
            (chat_id, date_str),
        )
        row = cur.fetchone()
        return int(row[0]) if row else 0
    finally:
        conn.close()


def pro_increment_daily_count(chat_id: int, date_str: str) -> int:
    now = int(time.time())
    conn = sqlite3.connect(get_db_path())
    try:
        cur = conn.cursor()
        cur.execute(
            """
            INSERT INTO pro_daily_limits (chat_id, date, sent_count, updated_at)
            VALUES (?, ?, 1, ?)
            ON CONFLICT(chat_id, date)
            DO UPDATE SET sent_count = sent_count + 1, updated_at = excluded.updated_at
            """,
            (chat_id, date_str, now),
        )
        conn.commit()
        cur.execute(
            "SELECT sent_count FROM pro_daily_limits WHERE chat_id = ? AND date = ?",
            (chat_id, date_str),
        )
        row = cur.fetchone()
        return int(row[0]) if row else 0
    finally:
        conn.close()
