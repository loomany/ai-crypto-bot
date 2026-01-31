import sqlite3
from datetime import datetime
from typing import List

from db_path import get_db_path

DB_PATH = get_db_path()


def init_pro_tables() -> None:
    conn = sqlite3.connect(DB_PATH)
    try:
        conn.execute(
            "CREATE TABLE IF NOT EXISTS pro_subscribers (chat_id INTEGER PRIMARY KEY)"
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS pro_daily_limits (
                chat_id INTEGER NOT NULL,
                day TEXT NOT NULL,
                sent_count INTEGER NOT NULL DEFAULT 0,
                PRIMARY KEY(chat_id, day)
            )
            """
        )
        conn.commit()
    finally:
        conn.close()


def pro_add(chat_id: int) -> bool:
    conn = sqlite3.connect(DB_PATH)
    try:
        cur = conn.cursor()
        cur.execute("INSERT OR IGNORE INTO pro_subscribers (chat_id) VALUES (?)", (chat_id,))
        conn.commit()
        return cur.rowcount > 0
    finally:
        conn.close()


def pro_remove(chat_id: int) -> bool:
    conn = sqlite3.connect(DB_PATH)
    try:
        cur = conn.cursor()
        cur.execute("DELETE FROM pro_subscribers WHERE chat_id = ?", (chat_id,))
        conn.commit()
        return cur.rowcount > 0
    finally:
        conn.close()


def pro_list() -> List[int]:
    conn = sqlite3.connect(DB_PATH)
    try:
        cur = conn.cursor()
        cur.execute("SELECT chat_id FROM pro_subscribers")
        return [r[0] for r in cur.fetchall()]
    finally:
        conn.close()


def pro_get_day_key() -> str:
    return datetime.utcnow().strftime("%Y-%m-%d")


def pro_can_send(chat_id: int, max_per_day: int) -> bool:
    day = pro_get_day_key()
    conn = sqlite3.connect(DB_PATH)
    try:
        cur = conn.cursor()
        cur.execute(
            "SELECT sent_count FROM pro_daily_limits WHERE chat_id=? AND day=?",
            (chat_id, day),
        )
        row = cur.fetchone()
        sent = int(row[0]) if row else 0
        return sent < max_per_day
    finally:
        conn.close()


def pro_inc_sent(chat_id: int) -> None:
    day = pro_get_day_key()
    conn = sqlite3.connect(DB_PATH)
    try:
        cur = conn.cursor()
        cur.execute(
            "INSERT INTO pro_daily_limits(chat_id, day, sent_count) VALUES (?,?,1) "
            "ON CONFLICT(chat_id, day) DO UPDATE SET sent_count=sent_count+1",
            (chat_id, day),
        )
        conn.commit()
    finally:
        conn.close()
