import json
import sqlite3
import time
from typing import Iterable, List, Optional, Tuple

from db_path import get_db_path
from symbol_cache import get_blocked_symbols

TRIAL_AI_LIMIT = 7
TRIAL_PUMP_LIMIT = 7


def get_conn() -> sqlite3.Connection:
    conn = sqlite3.connect(get_db_path(), check_same_thread=False, timeout=5.0)
    conn.row_factory = sqlite3.Row
    try:
        conn.execute("PRAGMA journal_mode=WAL")
    except sqlite3.Error:
        pass
    try:
        conn.execute("PRAGMA busy_timeout=3000")
    except sqlite3.Error:
        pass
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
                is_test INTEGER NOT NULL DEFAULT 0,
                tg_message_id INTEGER,
                reason_json TEXT,
                breakdown_json TEXT,
                result_notified INTEGER NOT NULL DEFAULT 0
            )
            """
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_signal_events_user_ts ON signal_events(user_id, ts)"
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_signal_events_symbol_ts ON signal_events(symbol, ts)"
        )
        cur = conn.execute("PRAGMA table_info(signal_events)")
        cols = {row["name"] for row in cur.fetchall()}
        if "reason_json" not in cols:
            conn.execute("ALTER TABLE signal_events ADD COLUMN reason_json TEXT")
        if "breakdown_json" not in cols:
            conn.execute("ALTER TABLE signal_events ADD COLUMN breakdown_json TEXT")
        if "is_test" not in cols:
            conn.execute("ALTER TABLE signal_events ADD COLUMN is_test INTEGER NOT NULL DEFAULT 0")
        if "result" not in cols:
            conn.execute("ALTER TABLE signal_events ADD COLUMN result TEXT")
        if "last_checked_at" not in cols:
            conn.execute("ALTER TABLE signal_events ADD COLUMN last_checked_at INTEGER")
        if "updated_at" not in cols:
            conn.execute("ALTER TABLE signal_events ADD COLUMN updated_at INTEGER")
        if "entry_touched" not in cols:
            conn.execute("ALTER TABLE signal_events ADD COLUMN entry_touched INTEGER")
        if "tp1_hit" not in cols:
            conn.execute("ALTER TABLE signal_events ADD COLUMN tp1_hit INTEGER")
        if "tp2_hit" not in cols:
            conn.execute("ALTER TABLE signal_events ADD COLUMN tp2_hit INTEGER")
        if "refresh_count" not in cols:
            conn.execute(
                "ALTER TABLE signal_events ADD COLUMN refresh_count INTEGER NOT NULL DEFAULT 0"
            )
        if "closed_at" not in cols:
            conn.execute("ALTER TABLE signal_events ADD COLUMN closed_at INTEGER")
        if "close_reason" not in cols:
            conn.execute("ALTER TABLE signal_events ADD COLUMN close_reason TEXT")
        if "result_notified" not in cols:
            conn.execute(
                "ALTER TABLE signal_events ADD COLUMN result_notified INTEGER NOT NULL DEFAULT 0"
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


def is_user_locked(user_id: int) -> bool:
    return get_user_pref(user_id, "user_locked", 0) == 1


def is_sub_active(user_id: int) -> bool:
    sub_until = get_user_pref(user_id, "sub_until", 0)
    return time.time() < sub_until


def ensure_trial_defaults(user_id: int) -> None:
    conn = get_conn()
    try:
        cur = conn.execute(
            "SELECT key FROM user_prefs WHERE user_id = ? AND key IN (?, ?, ?)",
            (
                user_id,
                "trial_ai_left",
                "trial_pump_left",
                "user_locked",
            ),
        )
        existing = {row["key"] for row in cur.fetchall()}
        now = int(time.time())
        inserts = []
        if "trial_ai_left" not in existing:
            inserts.append((user_id, "trial_ai_left", TRIAL_AI_LIMIT, now))
        if "trial_pump_left" not in existing:
            inserts.append((user_id, "trial_pump_left", TRIAL_PUMP_LIMIT, now))
        if "user_locked" not in existing:
            inserts.append((user_id, "user_locked", 0, now))
        if inserts:
            conn.executemany(
                """
                INSERT INTO user_prefs (user_id, key, value, updated_at)
                VALUES (?, ?, ?, ?)
                ON CONFLICT(user_id, key) DO NOTHING
                """,
                inserts,
            )
            conn.commit()
    finally:
        conn.close()


def try_consume_trial(user_id: int, key: str, amount: int = 1) -> Tuple[bool, int]:
    conn = get_conn()
    try:
        cur = conn.execute(
            """
            UPDATE user_prefs
            SET value = value - ?
            WHERE user_id = ? AND key = ? AND value >= ?
            """,
            (int(amount), user_id, key, int(amount)),
        )
        if cur.rowcount <= 0:
            conn.rollback()
            return False, 0
        cur = conn.execute(
            "SELECT value FROM user_prefs WHERE user_id = ? AND key = ?",
            (user_id, key),
        )
        row = cur.fetchone()
        conn.commit()
        if row is None:
            return True, 0
        return True, int(row["value"])
    finally:
        conn.close()


def delete_user(user_id: int) -> None:
    conn = get_conn()
    try:
        conn.execute("DELETE FROM users WHERE chat_id = ?", (user_id,))
        conn.execute("DELETE FROM user_prefs WHERE user_id = ?", (user_id,))
        conn.execute("DELETE FROM pumpdump_daily_counts WHERE chat_id = ?", (user_id,))
        conn.execute("DELETE FROM signal_events WHERE user_id = ?", (user_id,))
        try:
            conn.execute("DELETE FROM ai_signals_subscribers WHERE chat_id = ?", (user_id,))
        except sqlite3.OperationalError:
            pass
        try:
            conn.execute("DELETE FROM alert_dedup WHERE chat_id = ?", (user_id,))
        except sqlite3.OperationalError:
            pass
        try:
            conn.execute("DELETE FROM notify_settings WHERE chat_id = ?", (user_id,))
        except sqlite3.OperationalError:
            pass
        conn.commit()
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


def get_last_pumpdump_signal() -> Optional[dict]:
    payload = get_state("last_pumpdump_signal")
    if not payload:
        return None
    try:
        parsed = json.loads(payload)
    except (TypeError, ValueError):
        return None
    return parsed if isinstance(parsed, dict) else None


def set_last_pumpdump_signal(signal: dict) -> None:
    set_state("last_pumpdump_signal", json.dumps(signal, ensure_ascii=False))


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


def _append_blocked_symbols_filter(clauses: list[str], params: list[object]) -> None:
    blocked = sorted(get_blocked_symbols())
    if not blocked:
        return
    placeholders = ", ".join("?" for _ in blocked)
    clauses.append(f"symbol NOT IN ({placeholders})")
    params.extend(blocked)


def purge_symbol(symbol: str) -> dict[str, int]:
    normalized = symbol.strip().upper()
    if not normalized:
        return {"events_deleted": 0, "signal_audit_deleted": 0}
    conn = get_conn()
    try:
        cur = conn.execute("DELETE FROM signal_events WHERE symbol = ?", (normalized,))
        events_deleted = cur.rowcount or 0
        cur = conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='signal_audit'"
        )
        signal_audit_deleted = 0
        if cur.fetchone() is not None:
            cur = conn.execute("DELETE FROM signal_audit WHERE symbol = ?", (normalized,))
            signal_audit_deleted = cur.rowcount or 0
        conn.commit()
        return {"events_deleted": events_deleted, "signal_audit_deleted": signal_audit_deleted}
    finally:
        conn.close()


def delete_symbol_everywhere(symbol: str) -> dict[str, int]:
    stats = purge_symbol(symbol)
    return {"events_deleted": stats["events_deleted"]}


def delete_symbols_everywhere(symbols: Iterable[str]) -> dict[str, int]:
    normalized = {symbol.strip().upper() for symbol in symbols if symbol and symbol.strip()}
    if not normalized:
        return {
            "events_deleted": 0,
            "signal_audit_deleted": 0,
            "symbols": 0,
        }
    totals = {"events_deleted": 0, "signal_audit_deleted": 0}
    for symbol in normalized:
        stats = purge_symbol(symbol)
        totals["events_deleted"] += stats["events_deleted"]
        totals["signal_audit_deleted"] += stats["signal_audit_deleted"]
    totals["symbols"] = len(normalized)
    return totals


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
    is_test: bool = False,
    reason_json: str | None = None,
    breakdown_json: str | None = None,
) -> int:
    if is_test:
        return 0
    blocked = get_blocked_symbols()
    if symbol and symbol.upper() in blocked:
        return 0
    conn = get_conn()
    try:
        cur = conn.execute(
            """
            INSERT INTO signal_events (
                ts, user_id, module, symbol, side, timeframe, score,
                poi_low, poi_high, sl, tp1, tp2, status, is_test, tg_message_id,
                reason_json, breakdown_json
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
                1 if is_test else 0,
                tg_message_id,
                reason_json,
                breakdown_json,
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
            SET status = ?,
                result = ?,
                updated_at = ?
            WHERE module = ? AND symbol = ? AND ts = ?
            """,
            (status, status, int(time.time()), module, symbol, int(ts)),
        )
        conn.commit()
        return cur.rowcount if cur.rowcount is not None else 0
    finally:
        conn.close()


def list_signal_events_by_identity(
    *,
    module: str,
    symbol: str,
    ts: int,
) -> List[sqlite3.Row]:
    conn = get_conn()
    try:
        cur = conn.execute(
            """
            SELECT *
            FROM signal_events
            WHERE module = ? AND symbol = ? AND ts = ?
            """,
            (module, symbol, int(ts)),
        )
        return cur.fetchall()
    finally:
        conn.close()




def _history_since_ts(time_window: str, now_ts: int | None = None) -> int | None:
    now = int(now_ts or time.time())
    day_seconds = 24 * 60 * 60
    if time_window == "1d":
        return now - day_seconds
    if time_window == "7d":
        return now - (7 * day_seconds)
    if time_window == "30d":
        return now - (30 * day_seconds)
    return None


def get_signal_history(
    time_window: str,
    user_id: int | None = None,
    limit: int = 10,
    offset: int = 0,
) -> list[sqlite3.Row]:
    since_ts = _history_since_ts(time_window)
    conn = get_conn()
    try:
        clauses = ["(is_test IS NULL OR is_test = 0)"]
        params: list[object] = []
        if user_id is not None:
            clauses.append("user_id = ?")
            params.append(int(user_id))
        if since_ts is not None:
            clauses.append("ts >= ?")
            params.append(int(since_ts))
        clauses.append(
            "NOT ("
            "symbol LIKE 'TEST%' OR "
            "LOWER(COALESCE(reason_json, '')) LIKE '%test%' OR "
            "LOWER(COALESCE(reason_json, '')) LIKE '%тест%' OR "
            "LOWER(COALESCE(breakdown_json, '')) LIKE '%test%' OR "
            "LOWER(COALESCE(breakdown_json, '')) LIKE '%тест%')"
        )
        _append_blocked_symbols_filter(clauses, params)
        where_clause = " AND ".join(clauses)
        params.extend([int(limit), int(offset)])
        cur = conn.execute(
            f"""
            SELECT
                id,
                symbol,
                side,
                CAST(ROUND(score) AS INTEGER) AS score,
                COALESCE(result, status) AS outcome,
                ts AS created_at,
                poi_low AS entry_low,
                poi_high AS entry_high,
                tp1,
                tp2,
                sl AS sl_price,
                timeframe
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


def count_signal_history(
    time_window: str,
    user_id: int | None = None,
    min_score: float | None = None,
) -> int:
    since_ts = _history_since_ts(time_window)
    conn = get_conn()
    try:
        clauses = ["(is_test IS NULL OR is_test = 0)"]
        params: list[object] = []
        if user_id is not None:
            clauses.append("user_id = ?")
            params.append(int(user_id))
        if since_ts is not None:
            clauses.append("ts >= ?")
            params.append(int(since_ts))
        if min_score is not None:
            clauses.append("score >= ?")
            params.append(float(min_score))
        clauses.append(
            "NOT ("
            "symbol LIKE 'TEST%' OR "
            "LOWER(COALESCE(reason_json, '')) LIKE '%test%' OR "
            "LOWER(COALESCE(reason_json, '')) LIKE '%тест%' OR "
            "LOWER(COALESCE(breakdown_json, '')) LIKE '%test%' OR "
            "LOWER(COALESCE(breakdown_json, '')) LIKE '%тест%')"
        )
        _append_blocked_symbols_filter(clauses, params)
        where_clause = " AND ".join(clauses)
        cur = conn.execute(
            f"SELECT COUNT(*) AS cnt FROM signal_events WHERE {where_clause}",
            params,
        )
        row = cur.fetchone()
        return int(row["cnt"]) if row is not None else 0
    finally:
        conn.close()


def get_history_winrate_summary(
    time_window: str,
    user_id: int | None = None,
) -> dict[str, dict[str, int | None]]:
    since_ts = _history_since_ts(time_window)
    conn = get_conn()
    try:
        clauses = ["(is_test IS NULL OR is_test = 0)"]
        params: list[object] = []
        if user_id is not None:
            clauses.append("user_id = ?")
            params.append(int(user_id))
        if since_ts is not None:
            clauses.append("ts >= ?")
            params.append(int(since_ts))
        clauses.append(
            "NOT ("
            "symbol LIKE 'TEST%' OR "
            "LOWER(COALESCE(reason_json, '')) LIKE '%test%' OR "
            "LOWER(COALESCE(reason_json, '')) LIKE '%тест%' OR "
            "LOWER(COALESCE(breakdown_json, '')) LIKE '%test%' OR "
            "LOWER(COALESCE(breakdown_json, '')) LIKE '%тест%')"
        )
        _append_blocked_symbols_filter(clauses, params)
        where_clause = " AND ".join(clauses)

        cur = conn.execute(
            f"""
            SELECT
                CAST(ROUND(score) AS INTEGER) AS score,
                UPPER(TRIM(COALESCE(result, status, ''))) AS outcome
            FROM signal_events
            WHERE {where_clause}
            """,
            params,
        )
        rows = cur.fetchall()
    finally:
        conn.close()

    win_statuses = {"TP", "TP1", "TP2", "BE", "PASSED"}
    loss_statuses = {"SL", "FAILED"}
    closed_statuses = win_statuses | loss_statuses | {
        "NEUTRAL",
        "NO_FILL",
        "NF",
        "EXP",
        "EXPIRED",
        "AMBIGUOUS",
    }

    summary: dict[str, dict[str, int | None]] = {
        "90_100": {"wins": 0, "losses": 0, "winrate": None},
        "80_89": {"wins": 0, "losses": 0, "winrate": None},
    }

    for row in rows:
        score = int(row["score"] or 0)
        if 90 <= score <= 100:
            bucket = summary["90_100"]
        elif 80 <= score <= 89:
            bucket = summary["80_89"]
        else:
            continue

        outcome = str(row["outcome"] or "")
        if outcome not in closed_statuses:
            continue
        if outcome in win_statuses:
            bucket["wins"] = int(bucket["wins"] or 0) + 1
        elif outcome in loss_statuses:
            bucket["losses"] = int(bucket["losses"] or 0) + 1

    for bucket in summary.values():
        wins = int(bucket["wins"] or 0)
        losses = int(bucket["losses"] or 0)
        total = wins + losses
        bucket["winrate"] = round((wins / total) * 100) if total else None

    return summary


def list_signal_events(
    *,
    user_id: int | None,
    since_ts: int | None,
    min_score: float | None,
    limit: int,
    offset: int,
) -> List[sqlite3.Row]:
    conn = get_conn()
    try:
        clauses = ["(is_test IS NULL OR is_test = 0)"]
        params: list[object] = []
        if user_id is not None:
            clauses.append("user_id = ?")
            params.append(int(user_id))
        if since_ts is not None:
            clauses.append("ts >= ?")
            params.append(int(since_ts))
        if min_score is not None:
            clauses.append("score >= ?")
            params.append(float(min_score))
        clauses.append(
            "NOT ("
            "symbol LIKE 'TEST%' OR "
            "LOWER(COALESCE(reason_json, '')) LIKE '%test%' OR "
            "LOWER(COALESCE(reason_json, '')) LIKE '%тест%' OR "
            "LOWER(COALESCE(breakdown_json, '')) LIKE '%test%' OR "
            "LOWER(COALESCE(breakdown_json, '')) LIKE '%тест%')"
        )
        _append_blocked_symbols_filter(clauses, params)
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


def get_signals_for_period(
    *,
    user_id: int | None,
    since_ts: int | None,
) -> List[sqlite3.Row]:
    conn = get_conn()
    try:
        clauses = ["(is_test IS NULL OR is_test = 0)"]
        params: list[object] = []
        if user_id is not None:
            clauses.append("user_id = ?")
            params.append(int(user_id))
        if since_ts is not None:
            clauses.append("ts >= ?")
            params.append(int(since_ts))
        clauses.append(
            "NOT ("
            "symbol LIKE 'TEST%' OR "
            "LOWER(COALESCE(reason_json, '')) LIKE '%test%' OR "
            "LOWER(COALESCE(reason_json, '')) LIKE '%тест%' OR "
            "LOWER(COALESCE(breakdown_json, '')) LIKE '%test%' OR "
            "LOWER(COALESCE(breakdown_json, '')) LIKE '%тест%')"
        )
        _append_blocked_symbols_filter(clauses, params)
        where_clause = " AND ".join(clauses)
        cur = conn.execute(
            f"""
            SELECT id, ts, score, status, result, poi_low, poi_high, sl, tp1
            FROM signal_events
            WHERE {where_clause}
            ORDER BY ts DESC
            """,
            params,
        )
        rows = cur.fetchall()
        return list(rows) if rows else []
    finally:
        conn.close()


def list_open_signal_events(*, max_age_sec: int | None = None) -> List[sqlite3.Row]:
    conn = get_conn()
    try:
        clauses = ["status = 'OPEN'", "(is_test IS NULL OR is_test = 0)"]
        params: list[object] = []
        if max_age_sec is not None:
            since_ts = int(time.time()) - int(max_age_sec)
            clauses.append("ts >= ?")
            params.append(since_ts)
        clauses.append(
            "NOT ("
            "symbol LIKE 'TEST%' OR "
            "LOWER(COALESCE(reason_json, '')) LIKE '%test%' OR "
            "LOWER(COALESCE(reason_json, '')) LIKE '%тест%' OR "
            "LOWER(COALESCE(breakdown_json, '')) LIKE '%test%' OR "
            "LOWER(COALESCE(breakdown_json, '')) LIKE '%тест%')"
        )
        _append_blocked_symbols_filter(clauses, params)
        where_clause = " AND ".join(clauses)
        cur = conn.execute(
            f"""
            SELECT *
            FROM signal_events
            WHERE {where_clause}
            ORDER BY ts ASC
            """,
            params,
        )
        return cur.fetchall()
    finally:
        conn.close()


def count_signal_events(
    *,
    user_id: int | None,
    since_ts: int | None,
    min_score: float | None,
) -> int:
    conn = get_conn()
    try:
        clauses = ["(is_test IS NULL OR is_test = 0)"]
        params: list[object] = []
        if user_id is not None:
            clauses.append("user_id = ?")
            params.append(int(user_id))
        if since_ts is not None:
            clauses.append("ts >= ?")
            params.append(int(since_ts))
        if min_score is not None:
            clauses.append("score >= ?")
            params.append(float(min_score))
        clauses.append(
            "NOT ("
            "symbol LIKE 'TEST%' OR "
            "LOWER(COALESCE(reason_json, '')) LIKE '%test%' OR "
            "LOWER(COALESCE(reason_json, '')) LIKE '%тест%' OR "
            "LOWER(COALESCE(breakdown_json, '')) LIKE '%test%' OR "
            "LOWER(COALESCE(breakdown_json, '')) LIKE '%тест%')"
        )
        _append_blocked_symbols_filter(clauses, params)
        where_clause = " AND ".join(clauses)
        cur = conn.execute(
            f"SELECT COUNT(*) AS cnt FROM signal_events WHERE {where_clause}",
            params,
        )
        return int(cur.fetchone()["cnt"])
    finally:
        conn.close()


def get_last_signal_event_by_module(module: str) -> Optional[sqlite3.Row]:
    conn = get_conn()
    try:
        blocked = sorted(get_blocked_symbols())
        blocked_clause = ""
        params: list[object] = [module]
        if blocked:
            placeholders = ", ".join("?" for _ in blocked)
            blocked_clause = f" AND symbol NOT IN ({placeholders})"
            params.extend(blocked)
        cur = conn.execute(
            """
            SELECT symbol, side, score, ts, reason_json, breakdown_json
            FROM signal_events
            WHERE module = ?
              AND (is_test IS NULL OR is_test = 0)
              AND NOT (
                symbol LIKE 'TEST%' OR
                LOWER(COALESCE(reason_json, '')) LIKE '%test%' OR
                LOWER(COALESCE(reason_json, '')) LIKE '%тест%' OR
                LOWER(COALESCE(breakdown_json, '')) LIKE '%test%' OR
                LOWER(COALESCE(breakdown_json, '')) LIKE '%тест%'
              )
            """
            + blocked_clause
            + """
            ORDER BY ts DESC
            LIMIT 1
            """,
            params,
        )
        return cur.fetchone()
    finally:
        conn.close()


def get_signal_outcome_counts(
    *,
    user_id: int | None,
    since_ts: int | None,
    min_score: float | None,
) -> dict:
    conn = get_conn()
    try:
        clauses = ["(is_test IS NULL OR is_test = 0)"]
        params: list[object] = []
        if user_id is not None:
            clauses.append("user_id = ?")
            params.append(int(user_id))
        if since_ts is not None:
            clauses.append("ts >= ?")
            params.append(int(since_ts))
        if min_score is not None:
            clauses.append("score >= ?")
            params.append(float(min_score))
        clauses.append(
            "NOT ("
            "symbol LIKE 'TEST%' OR "
            "LOWER(COALESCE(reason_json, '')) LIKE '%test%' OR "
            "LOWER(COALESCE(reason_json, '')) LIKE '%тест%' OR "
            "LOWER(COALESCE(breakdown_json, '')) LIKE '%test%' OR "
            "LOWER(COALESCE(breakdown_json, '')) LIKE '%тест%')"
        )
        _append_blocked_symbols_filter(clauses, params)
        where_clause = " AND ".join(clauses)
        cur = conn.execute(
            f"""
            SELECT
                COUNT(*) AS total,
                SUM(CASE WHEN status = 'TP1' THEN 1 ELSE 0 END) AS tp1,
                SUM(CASE WHEN status = 'TP2' THEN 1 ELSE 0 END) AS tp2,
                SUM(CASE WHEN status = 'BE' THEN 1 ELSE 0 END) AS be,
                SUM(CASE WHEN status = 'SL' THEN 1 ELSE 0 END) AS sl,
                SUM(CASE WHEN status IN ('EXP', 'EXPIRED') THEN 1 ELSE 0 END) AS exp,
                SUM(CASE WHEN status IN ('NO_FILL', 'NF') THEN 1 ELSE 0 END) AS no_fill
            FROM signal_events
            WHERE {where_clause}
            """,
            params,
        )
        row = cur.fetchone()
        total = int(row["total"] or 0)
        tp1 = int(row["tp1"] or 0)
        tp2 = int(row["tp2"] or 0)
        be = int(row["be"] or 0)
        sl = int(row["sl"] or 0)
        exp = int(row["exp"] or 0)
        no_fill = int(row["no_fill"] or 0)
        passed = tp1 + tp2 + be
        failed = sl
        neutral = exp + no_fill
        in_progress = max(total - passed - failed - neutral, 0)
        return {
            "total": total,
            "tp1": tp1,
            "tp2": tp2,
            "be": be,
            "sl": sl,
            "exp": exp,
            "no_fill": no_fill,
            "passed": passed,
            "failed": failed,
            "neutral": neutral,
            "in_progress": in_progress,
        }
    finally:
        conn.close()


def get_signal_score_bucket_counts(
    *,
    user_id: int | None,
    since_ts: int | None,
    min_score: float | None,
) -> dict[str, dict[str, int]]:
    conn = get_conn()
    try:
        clauses = ["(is_test IS NULL OR is_test = 0)"]
        params: list[object] = []
        if user_id is not None:
            clauses.append("user_id = ?")
            params.append(int(user_id))
        if since_ts is not None:
            clauses.append("ts >= ?")
            params.append(int(since_ts))
        if min_score is not None:
            clauses.append("score >= ?")
            params.append(float(min_score))
        clauses.append(
            "NOT ("
            "symbol LIKE 'TEST%' OR "
            "LOWER(COALESCE(reason_json, '')) LIKE '%test%' OR "
            "LOWER(COALESCE(reason_json, '')) LIKE '%тест%' OR "
            "LOWER(COALESCE(breakdown_json, '')) LIKE '%test%' OR "
            "LOWER(COALESCE(breakdown_json, '')) LIKE '%тест%')"
        )
        _append_blocked_symbols_filter(clauses, params)
        where_clause = " AND ".join(clauses)
        cur = conn.execute(
            f"""
            SELECT
                SUM(CASE
                    WHEN score BETWEEN 90 AND 100
                    THEN 1 ELSE 0 END) AS b90_total,
                SUM(CASE
                    WHEN score BETWEEN 90 AND 100
                     AND status IN ('TP1', 'TP2', 'BE')
                    THEN 1 ELSE 0 END) AS b90_passed,
                SUM(CASE
                    WHEN score BETWEEN 90 AND 100
                     AND status = 'SL'
                    THEN 1 ELSE 0 END) AS b90_failed,
                SUM(CASE
                    WHEN score BETWEEN 90 AND 100
                     AND status IN ('EXP', 'EXPIRED', 'NO_FILL', 'NF')
                    THEN 1 ELSE 0 END) AS b90_neutral,
                SUM(CASE
                    WHEN score BETWEEN 80 AND 89
                    THEN 1 ELSE 0 END) AS b80_total,
                SUM(CASE
                    WHEN score BETWEEN 80 AND 89
                     AND status IN ('TP1', 'TP2', 'BE')
                    THEN 1 ELSE 0 END) AS b80_passed,
                SUM(CASE
                    WHEN score BETWEEN 80 AND 89
                     AND status = 'SL'
                    THEN 1 ELSE 0 END) AS b80_failed,
                SUM(CASE
                    WHEN score BETWEEN 80 AND 89
                     AND status IN ('EXP', 'EXPIRED', 'NO_FILL', 'NF')
                    THEN 1 ELSE 0 END) AS b80_neutral,
                SUM(CASE
                    WHEN score BETWEEN 70 AND 79
                    THEN 1 ELSE 0 END) AS b70_total,
                SUM(CASE
                    WHEN score BETWEEN 70 AND 79
                     AND status IN ('TP1', 'TP2', 'BE')
                    THEN 1 ELSE 0 END) AS b70_passed,
                SUM(CASE
                    WHEN score BETWEEN 70 AND 79
                     AND status = 'SL'
                    THEN 1 ELSE 0 END) AS b70_failed,
                SUM(CASE
                    WHEN score BETWEEN 70 AND 79
                     AND status IN ('EXP', 'EXPIRED', 'NO_FILL', 'NF')
                    THEN 1 ELSE 0 END) AS b70_neutral,
                SUM(CASE
                    WHEN score BETWEEN 60 AND 69
                    THEN 1 ELSE 0 END) AS b60_total,
                SUM(CASE
                    WHEN score BETWEEN 60 AND 69
                     AND status IN ('TP1', 'TP2', 'BE')
                    THEN 1 ELSE 0 END) AS b60_passed,
                SUM(CASE
                    WHEN score BETWEEN 60 AND 69
                     AND status = 'SL'
                    THEN 1 ELSE 0 END) AS b60_failed,
                SUM(CASE
                    WHEN score BETWEEN 60 AND 69
                     AND status IN ('EXP', 'EXPIRED', 'NO_FILL', 'NF')
                    THEN 1 ELSE 0 END) AS b60_neutral
            FROM signal_events
            WHERE {where_clause}
            """,
            params,
        )
        row = cur.fetchone()
        b90_total = int(row["b90_total"] or 0)
        b80_total = int(row["b80_total"] or 0)
        b70_total = int(row["b70_total"] or 0)
        b60_total = int(row["b60_total"] or 0)
        b90_passed = int(row["b90_passed"] or 0)
        b80_passed = int(row["b80_passed"] or 0)
        b70_passed = int(row["b70_passed"] or 0)
        b60_passed = int(row["b60_passed"] or 0)
        b90_failed = int(row["b90_failed"] or 0)
        b80_failed = int(row["b80_failed"] or 0)
        b70_failed = int(row["b70_failed"] or 0)
        b60_failed = int(row["b60_failed"] or 0)
        b90_neutral = int(row["b90_neutral"] or 0)
        b80_neutral = int(row["b80_neutral"] or 0)
        b70_neutral = int(row["b70_neutral"] or 0)
        b60_neutral = int(row["b60_neutral"] or 0)
        return {
            "90-100": {
                "passed": b90_passed,
                "failed": b90_failed,
                "neutral": b90_neutral,
                "in_progress": max(b90_total - b90_passed - b90_failed - b90_neutral, 0),
            },
            "80-89": {
                "passed": b80_passed,
                "failed": b80_failed,
                "neutral": b80_neutral,
                "in_progress": max(b80_total - b80_passed - b80_failed - b80_neutral, 0),
            },
            "70-79": {
                "passed": b70_passed,
                "failed": b70_failed,
                "neutral": b70_neutral,
                "in_progress": max(b70_total - b70_passed - b70_failed - b70_neutral, 0),
            },
            "60-69": {
                "passed": b60_passed,
                "failed": b60_failed,
                "neutral": b60_neutral,
                "in_progress": max(b60_total - b60_passed - b60_failed - b60_neutral, 0),
            },
        }
    finally:
        conn.close()


def get_signal_avg_rr(
    *,
    user_id: int | None,
    since_ts: int | None,
    min_score: float | None,
    score_min: float | None,
    score_max: float | None,
) -> dict[str, float | int]:
    conn = get_conn()
    try:
        clauses = ["(is_test IS NULL OR is_test = 0)"]
        params: list[object] = []
        if user_id is not None:
            clauses.append("user_id = ?")
            params.append(int(user_id))
        if since_ts is not None:
            clauses.append("ts >= ?")
            params.append(int(since_ts))
        if min_score is not None:
            clauses.append("score >= ?")
            params.append(float(min_score))
        if score_min is not None:
            clauses.append("score >= ?")
            params.append(float(score_min))
        if score_max is not None:
            clauses.append("score <= ?")
            params.append(float(score_max))
        clauses.append(
            "NOT ("
            "symbol LIKE 'TEST%' OR "
            "LOWER(COALESCE(reason_json, '')) LIKE '%test%' OR "
            "LOWER(COALESCE(reason_json, '')) LIKE '%тест%' OR "
            "LOWER(COALESCE(breakdown_json, '')) LIKE '%test%' OR "
            "LOWER(COALESCE(breakdown_json, '')) LIKE '%тест%')"
        )
        _append_blocked_symbols_filter(clauses, params)
        where_clause = " AND ".join(clauses)
        cur = conn.execute(
            f"""
            SELECT poi_low, poi_high, sl, tp1
            FROM signal_events
            WHERE {where_clause}
            """,
            params,
        )
        rr_values: list[float] = []
        for row in cur.fetchall():
            try:
                entry_mid = (float(row["poi_low"]) + float(row["poi_high"])) / 2
                sl = float(row["sl"])
                tp1 = float(row["tp1"])
            except (TypeError, ValueError):
                continue
            risk = abs(entry_mid - sl)
            reward = abs(tp1 - entry_mid)
            if risk > 0 and reward > 0:
                rr_values.append(reward / risk)
        avg_rr = sum(rr_values) / len(rr_values) if rr_values else 0.0
        return {"avg_rr": avg_rr, "samples": int(len(rr_values))}
    finally:
        conn.close()


def get_signal_event(
    *,
    user_id: int | None,
    event_id: int,
) -> Optional[sqlite3.Row]:
    conn = get_conn()
    try:
        params: list[object] = [int(event_id)]
        clauses = ["id = ?", "(is_test IS NULL OR is_test = 0)"]
        if user_id is not None:
            clauses.append("user_id = ?")
            params.append(int(user_id))
        clauses.append(
            "NOT ("
            "symbol LIKE 'TEST%' OR "
            "LOWER(COALESCE(reason_json, '')) LIKE '%test%' OR "
            "LOWER(COALESCE(reason_json, '')) LIKE '%тест%' OR "
            "LOWER(COALESCE(breakdown_json, '')) LIKE '%test%' OR "
            "LOWER(COALESCE(breakdown_json, '')) LIKE '%тест%')"
        )
        _append_blocked_symbols_filter(clauses, params)
        where_clause = " AND ".join(clauses)
        cur = conn.execute(
            f"SELECT * FROM signal_events WHERE {where_clause}",
            params,
        )
        return cur.fetchone()
    finally:
        conn.close()


def get_signal_by_id(signal_id: int) -> Optional[sqlite3.Row]:
    return get_signal_event(user_id=None, event_id=signal_id)


def update_signal_event_refresh(
    *,
    event_id: int,
    status: str,
    result: str | None,
    entry_touched: bool,
    tp1_hit: bool,
    tp2_hit: bool,
    last_checked_at: int,
    close_reason: str | None = None,
    closed_at: int | None = None,
) -> None:
    conn = get_conn()
    try:
        conn.execute(
            """
            UPDATE signal_events
            SET status = ?,
                result = ?,
                entry_touched = ?,
                tp1_hit = ?,
                tp2_hit = ?,
                last_checked_at = ?,
                updated_at = ?,
                close_reason = COALESCE(?, close_reason),
                closed_at = COALESCE(closed_at, ?),
                refresh_count = COALESCE(refresh_count, 0) + 1
            WHERE id = ?
            """,
            (
                status,
                result,
                1 if entry_touched else 0,
                1 if tp1_hit else 0,
                1 if tp2_hit else 0,
                int(last_checked_at),
                int(time.time()),
                close_reason,
                closed_at,
                int(event_id),
            ),
        )
        conn.commit()
    finally:
        conn.close()


def update_signal_event_status_by_id(
    *,
    event_id: int,
    status: str,
    result: str | None,
    last_checked_at: int | None = None,
) -> None:
    conn = get_conn()
    try:
        conn.execute(
            """
            UPDATE signal_events
            SET status = ?,
                result = ?,
                last_checked_at = COALESCE(?, last_checked_at),
                updated_at = ?
            WHERE id = ?
            """,
            (
                status,
                result,
                int(last_checked_at) if last_checked_at is not None else None,
                int(time.time()),
                int(event_id),
            ),
        )
        conn.commit()
    finally:
        conn.close()


def mark_signal_result_notified(event_id: int, *, notified: bool = True) -> None:
    conn = get_conn()
    try:
        conn.execute(
            """
            UPDATE signal_events
            SET result_notified = ?,
                updated_at = ?
            WHERE id = ?
            """,
            (
                1 if notified else 0,
                int(time.time()),
                int(event_id),
            ),
        )
        conn.commit()
    finally:
        conn.close()


def purge_test_signals() -> int:
    conn = get_conn()
    try:
        cur = conn.execute(
            """
            DELETE FROM signal_events
            WHERE is_test = 1
               OR symbol LIKE 'TEST%'
               OR LOWER(COALESCE(reason_json, '')) LIKE '%test%'
               OR LOWER(COALESCE(reason_json, '')) LIKE '%тест%'
               OR LOWER(COALESCE(breakdown_json, '')) LIKE '%test%'
               OR LOWER(COALESCE(breakdown_json, '')) LIKE '%тест%'
            """
        )
        total = cur.rowcount or 0
        cur = conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='signal_audit'"
        )
        if cur.fetchone() is not None:
            cur = conn.execute(
                """
                DELETE FROM signal_audit
                WHERE symbol LIKE 'TEST%'
                   OR LOWER(COALESCE(reason_json, '')) LIKE '%test%'
                   OR LOWER(COALESCE(reason_json, '')) LIKE '%тест%'
                   OR LOWER(COALESCE(breakdown_json, '')) LIKE '%test%'
                   OR LOWER(COALESCE(breakdown_json, '')) LIKE '%тест%'
                   OR LOWER(COALESCE(notes, '')) LIKE '%test%'
                   OR LOWER(COALESCE(notes, '')) LIKE '%тест%'
                """
            )
            total += cur.rowcount or 0
        conn.commit()
        return total
    finally:
        conn.close()
