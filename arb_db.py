import sqlite3
import time
from typing import List

from db import get_conn


def set_arb_notify_enabled(user_id: int, enabled: bool) -> None:
    now = int(time.time())
    conn = get_conn()
    try:
        conn.execute(
            """
            INSERT INTO arb_notify_settings (user_id, enabled, updated_at)
            VALUES (?, ?, ?)
            ON CONFLICT(user_id) DO UPDATE SET
                enabled = excluded.enabled,
                updated_at = excluded.updated_at
            """,
            (int(user_id), 1 if enabled else 0, now),
        )
        conn.commit()
    finally:
        conn.close()


def get_arb_notify_enabled(user_id: int) -> bool:
    conn = get_conn()
    try:
        row = conn.execute(
            "SELECT enabled FROM arb_notify_settings WHERE user_id = ?",
            (int(user_id),),
        ).fetchone()
        return bool(row and int(row[0] or 0) == 1)
    finally:
        conn.close()


def list_arb_enabled_users() -> List[int]:
    conn = get_conn()
    try:
        rows = conn.execute(
            "SELECT user_id FROM arb_notify_settings WHERE enabled = 1"
        ).fetchall()
        return [int(row[0]) for row in rows]
    finally:
        conn.close()


def can_send_arb_notification(user_id: int, dedup_key: str, *, cooldown_sec: int, dedup_ttl_sec: int) -> bool:
    now = int(time.time())
    conn = get_conn()
    try:
        cooldown_row = conn.execute(
            "SELECT MAX(sent_at) AS last_sent FROM arb_sent_log WHERE user_id = ?",
            (int(user_id),),
        ).fetchone()
        last_sent = int(cooldown_row[0] or 0) if cooldown_row is not None else 0
        if last_sent > 0 and now - last_sent < int(cooldown_sec):
            return False

        dedup_row = conn.execute(
            """
            SELECT 1 FROM arb_sent_log
            WHERE user_id = ? AND dedup_key = ? AND sent_at >= ?
            LIMIT 1
            """,
            (int(user_id), str(dedup_key), now - int(dedup_ttl_sec)),
        ).fetchone()
        return dedup_row is None
    finally:
        conn.close()


def record_arb_notification_sent(user_id: int, dedup_key: str) -> None:
    now = int(time.time())
    conn = get_conn()
    try:
        conn.execute(
            "INSERT INTO arb_sent_log (user_id, dedup_key, sent_at) VALUES (?, ?, ?)",
            (int(user_id), str(dedup_key), now),
        )
        conn.commit()
    finally:
        conn.close()
