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
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS signal_events (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                ts INTEGER NOT NULL,
                user_id INTEGER NOT NULL,
                module TEXT NOT NULL,
                symbol TEXT NOT NULL,
                side TEXT NOT NULL,
                timeframe TEXT NOT NULL,
                score REAL NOT NULL,
                poi_low REAL NOT NULL,
                poi_high REAL NOT NULL,
                sl REAL NOT NULL,
                tp1 REAL NOT NULL,
                tp2 REAL NOT NULL,
                status TEXT NOT NULL,
                tg_message_id INTEGER
            )
            """
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_signal_events_user_ts ON signal_events(user_id, ts)"
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_signal_events_symbol_ts ON signal_events(symbol, ts)"
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


def _get_kv_table(conn: sqlite3.Connection) -> str:
    cur = conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name IN ('state_kv', 'kv_store')"
    )
    rows = [row["name"] for row in cur.fetchall()]
    if "state_kv" in rows:
        return "state_kv"
    if "kv_store" in rows:
        return "kv_store"
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS kv_store (
            key TEXT PRIMARY KEY,
            value TEXT NOT NULL,
            updated_at REAL NOT NULL
        )
        """
    )
    conn.commit()
    return "kv_store"


def kv_get_int(key: str, default: int = 0, *, ttl_sec: Optional[int] = None) -> int:
    conn = get_conn()
    try:
        table = _get_kv_table(conn)
        cur = conn.execute(f"SELECT value, updated_at FROM {table} WHERE key = ?", (key,))
        row = cur.fetchone()
        if row is None:
            return default
        if ttl_sec is not None:
            updated_at = float(row["updated_at"]) if row["updated_at"] is not None else 0.0
            if time.time() - updated_at > ttl_sec:
                return default
        try:
            return int(row["value"])
        except (TypeError, ValueError):
            return default
    finally:
        conn.close()


def kv_set_int(key: str, value: int) -> None:
    now = time.time()
    conn = get_conn()
    try:
        table = _get_kv_table(conn)
        conn.execute(
            f"""
            INSERT INTO {table} (key, value, updated_at)
            VALUES (?, ?, ?)
            ON CONFLICT(key)
            DO UPDATE SET value = excluded.value, updated_at = excluded.updated_at
            """,
            (key, str(int(value)), now),
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


def insert_signal_event(
    *,
    ts: int,
    user_id: int,
    module: str,
    symbol: str,
    side: str,
    timeframe: str,
    score: float,
    poi_low: float,
    poi_high: float,
    sl: float,
    tp1: float,
    tp2: float,
    status: str,
    tg_message_id: int | None,
) -> int:
    conn = get_conn()
    try:
        cur = conn.execute(
            """
            INSERT INTO signal_events (
                ts, user_id, module, symbol, side, timeframe, score,
                poi_low, poi_high, sl, tp1, tp2, status, tg_message_id
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                int(ts),
                int(user_id),
                module,
                symbol,
                side,
                timeframe,
                float(score),
                float(poi_low),
                float(poi_high),
                float(sl),
                float(tp1),
                float(tp2),
                status,
                tg_message_id,
            ),
        )
        conn.commit()
        return int(cur.lastrowid)
    finally:
        conn.close()


def update_signal_events_status(
    *,
    module: str,
    symbol: str,
    ts: int,
    status: str,
) -> int:
    conn = get_conn()
    try:
        cur = conn.execute(
            """
            UPDATE signal_events
            SET status = ?
            WHERE module = ? AND symbol = ? AND ts = ?
            """,
            (status, module, symbol, int(ts)),
        )
        conn.commit()
        return cur.rowcount if cur.rowcount is not None else 0
    finally:
        conn.close()


def list_signal_events(
    *,
    user_id: int,
    since_ts: int | None,
    min_score: float | None,
    limit: int,
    offset: int,
) -> List[sqlite3.Row]:
    conn = get_conn()
    try:
        clauses = ["user_id = ?"]
        params: list[object] = [int(user_id)]
        if since_ts is not None:
            clauses.append("ts >= ?")
            params.append(int(since_ts))
        if min_score is not None:
            clauses.append("score >= ?")
            params.append(float(min_score))
        where_clause = " AND ".join(clauses)
        params.extend([int(limit), int(offset)])
        cur = conn.execute(
            f"""
            SELECT *
            FROM signal_events
            WHERE {where_clause}
            ORDER BY ts DESC
            LIMIT ? OFFSET ?
            """,
            params,
        )
        return cur.fetchall()
    finally:
        conn.close()


def count_signal_events(
    *,
    user_id: int,
    since_ts: int | None,
    min_score: float | None,
) -> int:
    conn = get_conn()
    try:
        clauses = ["user_id = ?"]
        params: list[object] = [int(user_id)]
        if since_ts is not None:
            clauses.append("ts >= ?")
            params.append(int(since_ts))
        if min_score is not None:
            clauses.append("score >= ?")
            params.append(float(min_score))
        where_clause = " AND ".join(clauses)
        cur = conn.execute(
            f"SELECT COUNT(*) AS cnt FROM signal_events WHERE {where_clause}",
            params,
        )
        return int(cur.fetchone()["cnt"])
    finally:
        conn.close()


def get_signal_event(
    *,
    user_id: int,
    event_id: int,
) -> Optional[sqlite3.Row]:
    conn = get_conn()
    try:
        cur = conn.execute(
            "SELECT * FROM signal_events WHERE id = ? AND user_id = ?",
            (int(event_id), int(user_id)),
        )
        return cur.fetchone()
    finally:
        conn.close()
