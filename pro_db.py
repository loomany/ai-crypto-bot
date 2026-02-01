import sqlite3
from datetime import datetime, timedelta
from typing import List, Optional

from db_path import get_db_path

DB_PATH = get_db_path()


def init_pro_tables() -> None:
    conn = sqlite3.connect(DB_PATH)
    try:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS pro_subscribers (
                chat_id INTEGER PRIMARY KEY,
                expires_at TEXT
            )
            """
        )
        cur = conn.cursor()
        cur.execute("PRAGMA table_info(pro_subscribers)")
        cols = {row[1] for row in cur.fetchall()}
        if "expires_at" not in cols:
            conn.execute("ALTER TABLE pro_subscribers ADD COLUMN expires_at TEXT")
        cur.execute("SELECT chat_id FROM pro_subscribers WHERE expires_at IS NULL")
        missing = [row[0] for row in cur.fetchall()]
        if missing:
            expires_at = (datetime.utcnow() + timedelta(days=30)).isoformat()
            conn.executemany(
                "UPDATE pro_subscribers SET expires_at=? WHERE chat_id=?",
                [(expires_at, chat_id) for chat_id in missing],
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


def pro_activate(chat_id: int, days: int) -> str:
    expires_at = (datetime.utcnow() + timedelta(days=days)).isoformat()
    conn = sqlite3.connect(DB_PATH)
    try:
        conn.execute(
            "INSERT INTO pro_subscribers(chat_id, expires_at) VALUES (?, ?) "
            "ON CONFLICT(chat_id) DO UPDATE SET expires_at=excluded.expires_at",
            (chat_id, expires_at),
        )
        conn.commit()
    finally:
        conn.close()
    return expires_at


def pro_remove(chat_id: int) -> bool:
    conn = sqlite3.connect(DB_PATH)
    try:
        cur = conn.cursor()
        cur.execute("DELETE FROM pro_subscribers WHERE chat_id = ?", (chat_id,))
        conn.commit()
        return cur.rowcount > 0
    finally:
        conn.close()


def pro_get_expires(chat_id: int) -> Optional[str]:
    conn = sqlite3.connect(DB_PATH)
    try:
        cur = conn.cursor()
        cur.execute("SELECT expires_at FROM pro_subscribers WHERE chat_id=?", (chat_id,))
        row = cur.fetchone()
        return row[0] if row else None
    finally:
        conn.close()


def pro_is(chat_id: int) -> bool:
    exp = pro_get_expires(chat_id)
    if not exp:
        return False
    try:
        return datetime.fromisoformat(exp) > datetime.utcnow()
    except ValueError:
        return False


def pro_list() -> List[int]:
    conn = sqlite3.connect(DB_PATH)
    try:
        cur = conn.cursor()
        cur.execute("SELECT chat_id, expires_at FROM pro_subscribers")
        now = datetime.utcnow()
        result = []
        for chat_id, expires_at in cur.fetchall():
            if not expires_at:
                continue
            try:
                if datetime.fromisoformat(expires_at) > now:
                    result.append(int(chat_id))
            except ValueError:
                continue
        return result
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
