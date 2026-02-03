import sqlite3
import time
from typing import Iterable, List, Optional, Tuple

from db_path import get_db_path


def get_conn() -> sqlite3.Connection:
    conn = sqlite3.connect(get_db_path(), check_same_thread=False)
    conn.row_factory = sqlite3.Row
    return conn


def init_db() -> None:
    conn = get_conn()
    try:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS user_prefs (
                user_id INTEGER NOT NULL,
                key TEXT NOT NULL,
                value INTEGER NOT NULL,
                updated_at INTEGER NOT NULL,
                PRIMARY KEY (user_id, key)
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS state_kv (
                key TEXT PRIMARY KEY,
                value TEXT NOT NULL,
                updated_at INTEGER NOT NULL
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS watchlist (
                symbol TEXT PRIMARY KEY,
                score INTEGER NOT NULL,
                reason TEXT NOT NULL,
                ttl_until INTEGER NOT NULL,
                cooldown_until INTEGER NOT NULL,
                last_seen INTEGER NOT NULL,
                added_at INTEGER NOT NULL
            )
            """
        )
        conn.commit()
    finally:
        conn.close()


def get_user_pref(user_id: int, key: str, default: int = 0) -> int:
    conn = get_conn()
    try:
        cur = conn.execute(
            "SELECT value FROM user_prefs WHERE user_id = ? AND key = ?",
            (user_id, key),
        )
        row = cur.fetchone()
        return int(row["value"]) if row is not None else default
    finally:
        conn.close()


def set_user_pref(user_id: int, key: str, value: int) -> None:
    now = int(time.time())
    conn = get_conn()
    try:
        conn.execute(
            """
            INSERT INTO user_prefs (user_id, key, value, updated_at)
            VALUES (?, ?, ?, ?)
            ON CONFLICT(user_id, key)
            DO UPDATE SET value = excluded.value, updated_at = excluded.updated_at
            """,
            (user_id, key, int(value), now),
        )
        conn.commit()
    finally:
        conn.close()


def list_user_ids_with_pref(key: str, value: int = 1) -> List[int]:
    conn = get_conn()
    try:
        cur = conn.execute(
            "SELECT user_id FROM user_prefs WHERE key = ? AND value = ?",
            (key, int(value)),
        )
        return [int(row["user_id"]) for row in cur.fetchall()]
    finally:
        conn.close()


def get_state(key: str, default: Optional[str] = None) -> Optional[str]:
    conn = get_conn()
    try:
        cur = conn.execute("SELECT value FROM state_kv WHERE key = ?", (key,))
        row = cur.fetchone()
        return str(row["value"]) if row is not None else default
    finally:
        conn.close()


def set_state(key: str, value: str) -> None:
    now = int(time.time())
    conn = get_conn()
    try:
        conn.execute(
            """
            INSERT INTO state_kv (key, value, updated_at)
            VALUES (?, ?, ?)
            ON CONFLICT(key)
            DO UPDATE SET value = excluded.value, updated_at = excluded.updated_at
            """,
            (key, value, now),
        )
        conn.commit()
    finally:
        conn.close()


def upsert_watchlist_candidate(
    symbol: str,
    score: int,
    reason: str,
    ttl_until: int,
    *,
    last_seen: Optional[int] = None,
    cooldown_until: Optional[int] = None,
) -> bool:
    now = int(time.time()) if last_seen is None else last_seen
    conn = get_conn()
    try:
        cur = conn.execute(
            "SELECT cooldown_until, added_at FROM watchlist WHERE symbol = ?",
            (symbol,),
        )
        row = cur.fetchone()
        if row is None:
            cooldown = int(cooldown_until or 0)
            conn.execute(
                """
                INSERT INTO watchlist (
                    symbol, score, reason, ttl_until, cooldown_until, last_seen, added_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (symbol, score, reason, ttl_until, cooldown, now, now),
            )
            conn.commit()
            return True

        existing_cooldown = int(row["cooldown_until"])
        added_at = int(row["added_at"])
        conn.execute(
            """
            UPDATE watchlist
            SET score = ?, reason = ?, ttl_until = ?, cooldown_until = ?, last_seen = ?, added_at = ?
            WHERE symbol = ?
            """,
            (
                score,
                reason,
                ttl_until,
                existing_cooldown if cooldown_until is None else int(cooldown_until),
                now,
                added_at,
                symbol,
            ),
        )
        conn.commit()
        return False
    finally:
        conn.close()


def list_watchlist_for_scan(now: int, limit: int) -> List[sqlite3.Row]:
    conn = get_conn()
    try:
        cur = conn.execute(
            """
            SELECT * FROM watchlist
            WHERE ttl_until > ? AND cooldown_until <= ?
            ORDER BY score DESC, last_seen DESC
            LIMIT ?
            """,
            (now, now, limit),
        )
        return cur.fetchall()
    finally:
        conn.close()


def update_watchlist_after_signal(
    symbol: str,
    *,
    ttl_until: int,
    cooldown_until: int,
    last_seen: Optional[int] = None,
) -> None:
    now = int(time.time()) if last_seen is None else last_seen
    conn = get_conn()
    try:
        conn.execute(
            """
            UPDATE watchlist
            SET ttl_until = ?, cooldown_until = ?, last_seen = ?
            WHERE symbol = ?
            """,
            (ttl_until, cooldown_until, now, symbol),
        )
        conn.commit()
    finally:
        conn.close()


def prune_watchlist(now: int, max_size: int) -> int:
    pruned = 0
    conn = get_conn()
    try:
        cur = conn.execute("DELETE FROM watchlist WHERE ttl_until <= ?", (now,))
        pruned += cur.rowcount if cur.rowcount is not None else 0
        cur = conn.execute("SELECT COUNT(*) AS cnt FROM watchlist")
        total = int(cur.fetchone()["cnt"])
        if total > max_size:
            to_remove = total - max_size
            cur = conn.execute(
                """
                SELECT symbol FROM watchlist
                ORDER BY score ASC, ttl_until ASC
                LIMIT ?
                """,
                (to_remove,),
            )
            symbols = [row["symbol"] for row in cur.fetchall()]
            if symbols:
                conn.executemany(
                    "DELETE FROM watchlist WHERE symbol = ?",
                    [(symbol,) for symbol in symbols],
                )
                pruned += len(symbols)
        conn.commit()
    finally:
        conn.close()
    return pruned


def get_watchlist_counts(now: int) -> Tuple[int, int]:
    conn = get_conn()
    try:
        cur = conn.execute("SELECT COUNT(*) AS cnt FROM watchlist")
        total = int(cur.fetchone()["cnt"])
        cur = conn.execute("SELECT COUNT(*) AS cnt FROM watchlist WHERE ttl_until > ?", (now,))
        active = int(cur.fetchone()["cnt"])
        return active, total
    finally:
        conn.close()


def delete_watchlist_symbols(symbols: Iterable[str]) -> None:
    symbols_list = list(symbols)
    if not symbols_list:
        return
    conn = get_conn()
    try:
        conn.executemany("DELETE FROM watchlist WHERE symbol = ?", [(s,) for s in symbols_list])
        conn.commit()
    finally:
        conn.close()
