import sqlite3
import time

from db_path import get_db_path


def init_alert_dedup() -> None:
    conn = sqlite3.connect(get_db_path())
    try:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS alert_dedup(
                chat_id INTEGER NOT NULL,
                feature TEXT NOT NULL,
                dedup_key TEXT NOT NULL,
                sent_at INTEGER NOT NULL,
                PRIMARY KEY(chat_id, feature, dedup_key)
            )
            """
        )
        conn.commit()
    finally:
        conn.close()


def can_send(chat_id: int, feature: str, dedup_key: str, cooldown_sec: int) -> bool:
    """
    True -> можно отправлять и сразу записывает sent_at.
    False -> нельзя (ещё cooldown).
    """
    now = int(time.time())
    feature_key = str(feature or "").strip().lower()
    dedup_value = str(dedup_key or "").strip().upper()
    if not feature_key or not dedup_value:
        return False
    conn = sqlite3.connect(get_db_path())
    try:
        conn.execute("BEGIN IMMEDIATE")
        cur = conn.execute(
            "SELECT sent_at FROM alert_dedup WHERE chat_id=? AND feature=? AND dedup_key=?",
            (int(chat_id), feature_key, dedup_value),
        )
        row = cur.fetchone()
        if row:
            last = int(row[0])
            if now - last < cooldown_sec:
                conn.rollback()
                return False

        conn.execute(
            """
            INSERT INTO alert_dedup(chat_id, feature, dedup_key, sent_at)
            VALUES(?,?,?,?)
            ON CONFLICT(chat_id, feature, dedup_key)
            DO UPDATE SET sent_at=excluded.sent_at
            """,
            (int(chat_id), feature_key, dedup_value, now),
        )
        conn.commit()
        return True
    finally:
        conn.close()
