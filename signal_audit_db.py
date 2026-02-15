import json
import sqlite3
import time
import uuid
from statistics import median
from typing import Any, Dict

from cutoff_config import get_effective_cutoff_ts
from db_path import get_db_path
from symbol_cache import get_blocked_symbols
from utils.safe_math import safe_div, safe_pct


def init_signal_audit_tables() -> None:
    conn = sqlite3.connect(get_db_path())
    try:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS signal_audit (
                signal_id TEXT PRIMARY KEY,
                module TEXT NOT NULL,
                tier TEXT NOT NULL,
                symbol TEXT NOT NULL,
                direction TEXT NOT NULL,
                timeframe TEXT NOT NULL,
                entry_from REAL NOT NULL,
                entry_to REAL NOT NULL,
                sl REAL NOT NULL,
                tp1 REAL NOT NULL,
                tp2 REAL NOT NULL,
                score REAL NOT NULL,
                rr REAL NOT NULL,
                reason_json TEXT NOT NULL,
                breakdown_json TEXT NOT NULL,
                sent_at INTEGER NOT NULL,
                status TEXT NOT NULL DEFAULT 'open',
                filled_at INTEGER,
                outcome TEXT,
                closed_at INTEGER,
                pnl_r REAL,
                notes TEXT,
                ttl_minutes INTEGER NOT NULL DEFAULT 720,
                state TEXT NOT NULL DEFAULT 'WAITING_ENTRY',
                poi_touched_at INTEGER,
                activated_at INTEGER,
                is_activated INTEGER NOT NULL DEFAULT 0,
                entry_price REAL,
                tp1_hit INTEGER NOT NULL DEFAULT 0,
                tp1_hit_at INTEGER,
                be_armed INTEGER NOT NULL DEFAULT 0,
                tp1_notified INTEGER NOT NULL DEFAULT 0,
                tp2_notified INTEGER NOT NULL DEFAULT 0,
                sl_notified INTEGER NOT NULL DEFAULT 0,
                be_notified INTEGER NOT NULL DEFAULT 0,
                exp_notified INTEGER NOT NULL DEFAULT 0,
                nf_notified INTEGER NOT NULL DEFAULT 0,
                confirm_strict INTEGER NOT NULL DEFAULT 0,
                confirm_count INTEGER NOT NULL DEFAULT 0
            )
            """
        )
        conn.execute("CREATE INDEX IF NOT EXISTS idx_signal_audit_status ON signal_audit(status)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_signal_audit_sent_at ON signal_audit(sent_at)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_signal_audit_symbol ON signal_audit(symbol)")
        cur = conn.execute("PRAGMA table_info(signal_audit)")
        cols = {row[1] for row in cur.fetchall()}
        if "ttl_minutes" not in cols:
            conn.execute("ALTER TABLE signal_audit ADD COLUMN ttl_minutes INTEGER NOT NULL DEFAULT 720")
        if "state" not in cols:
            conn.execute("ALTER TABLE signal_audit ADD COLUMN state TEXT NOT NULL DEFAULT 'WAITING_ENTRY'")
        if "poi_touched_at" not in cols:
            conn.execute("ALTER TABLE signal_audit ADD COLUMN poi_touched_at INTEGER")
        if "activated_at" not in cols:
            conn.execute("ALTER TABLE signal_audit ADD COLUMN activated_at INTEGER")
        if "entry_price" not in cols:
            conn.execute("ALTER TABLE signal_audit ADD COLUMN entry_price REAL")
        if "is_activated" not in cols:
            conn.execute("ALTER TABLE signal_audit ADD COLUMN is_activated INTEGER NOT NULL DEFAULT 0")
        if "tp1_hit" not in cols:
            conn.execute("ALTER TABLE signal_audit ADD COLUMN tp1_hit INTEGER NOT NULL DEFAULT 0")
        if "tp1_hit_at" not in cols:
            conn.execute("ALTER TABLE signal_audit ADD COLUMN tp1_hit_at INTEGER")
        if "be_armed" not in cols:
            conn.execute("ALTER TABLE signal_audit ADD COLUMN be_armed INTEGER NOT NULL DEFAULT 0")
        if "tp1_notified" not in cols:
            conn.execute("ALTER TABLE signal_audit ADD COLUMN tp1_notified INTEGER NOT NULL DEFAULT 0")
        if "tp2_notified" not in cols:
            conn.execute("ALTER TABLE signal_audit ADD COLUMN tp2_notified INTEGER NOT NULL DEFAULT 0")
        if "sl_notified" not in cols:
            conn.execute("ALTER TABLE signal_audit ADD COLUMN sl_notified INTEGER NOT NULL DEFAULT 0")
        if "be_notified" not in cols:
            conn.execute("ALTER TABLE signal_audit ADD COLUMN be_notified INTEGER NOT NULL DEFAULT 0")
        if "exp_notified" not in cols:
            conn.execute("ALTER TABLE signal_audit ADD COLUMN exp_notified INTEGER NOT NULL DEFAULT 0")
        if "nf_notified" not in cols:
            conn.execute("ALTER TABLE signal_audit ADD COLUMN nf_notified INTEGER NOT NULL DEFAULT 0")
        if "confirm_strict" not in cols:
            conn.execute("ALTER TABLE signal_audit ADD COLUMN confirm_strict INTEGER NOT NULL DEFAULT 0")
        if "confirm_count" not in cols:
            conn.execute("ALTER TABLE signal_audit ADD COLUMN confirm_count INTEGER NOT NULL DEFAULT 0")
        conn.execute(
            """
            UPDATE signal_audit
            SET state = 'WAITING_ENTRY'
            WHERE COALESCE(state, '') = '' AND status = 'open'
            """
        )
        conn.commit()
    finally:
        conn.close()


def _compute_rr(signal_dict: dict) -> float:
    reason = signal_dict.get("reason") if isinstance(signal_dict.get("reason"), dict) else {}
    rr = reason.get("rr")
    if rr is not None:
        try:
            return float(rr)
        except (TypeError, ValueError):
            pass

    entry_zone = signal_dict.get("entry_zone") or (0.0, 0.0)
    try:
        entry_from, entry_to = entry_zone
        entry_ref = (float(entry_from) + float(entry_to)) / 2
        sl = float(signal_dict.get("sl", 0.0))
        tp1 = float(signal_dict.get("tp1", 0.0))
    except (TypeError, ValueError):
        return 0.0

    risk = abs(entry_ref - sl)
    reward = abs(tp1 - entry_ref)
    return safe_div(reward, risk, 0.0) if risk > 0 else 0.0


def insert_signal_audit(
    signal_dict: dict,
    tier: str,
    module: str,
    *,
    sent_at: int | None = None,
) -> str:
    meta = signal_dict.get("meta") if isinstance(signal_dict, dict) else {}
    if signal_dict.get("is_test") or (isinstance(meta, dict) and meta.get("test")):
        return ""
    blocked = get_blocked_symbols()
    symbol_value = str(signal_dict.get("symbol", ""))
    if symbol_value and symbol_value.upper() in blocked:
        return ""
    signal_id = str(uuid.uuid4())
    entry_from, entry_to = signal_dict.get("entry_zone") or (0.0, 0.0)
    reason = signal_dict.get("reason") if isinstance(signal_dict.get("reason"), dict) else {}
    breakdown = signal_dict.get("breakdown") if isinstance(signal_dict.get("breakdown"), list) else []
    rr_value = _compute_rr(signal_dict)
    signal_sent_at = int(time.time()) if sent_at is None else int(sent_at)

    payload = (
        signal_id,
        module,
        tier,
        symbol_value,
        str(signal_dict.get("direction", "")),
        "1H",
        float(entry_from),
        float(entry_to),
        float(signal_dict.get("sl", 0.0)),
        float(signal_dict.get("tp1", 0.0)),
        float(signal_dict.get("tp2", 0.0)),
        float(signal_dict.get("score", 0.0)),
        float(rr_value),
        json.dumps(reason, ensure_ascii=False),
        json.dumps(breakdown, ensure_ascii=False),
        signal_sent_at,
        "open",
        int(signal_dict.get("ttl_minutes", 720) or 720),
        "WAITING_ENTRY",
        int(bool(signal_dict.get("confirm_strict", False))),
        int(signal_dict.get("confirm_count", 0) or 0),
    )

    conn = sqlite3.connect(get_db_path())
    try:
        conn.execute(
            """
            INSERT INTO signal_audit (
                signal_id, module, tier, symbol, direction, timeframe,
                entry_from, entry_to, sl, tp1, tp2, score, rr,
                reason_json, breakdown_json, sent_at, status, ttl_minutes
                , state, confirm_strict, confirm_count
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            payload,
        )
        conn.commit()
    finally:
        conn.close()

    return signal_id




def _append_cutoff_clause(
    where_clauses: list[str],
    params: list[object],
    *,
    include_legacy: bool,
    field_name: str = "sent_at",
) -> None:
    cutoff_ts = get_effective_cutoff_ts(include_legacy=include_legacy)
    if cutoff_ts > 0:
        where_clauses.append(f"{field_name} >= ?")
        params.append(int(cutoff_ts))

def _blocked_symbols_clause() -> tuple[str, list[str]]:
    blocked = sorted(get_blocked_symbols())
    if not blocked:
        return "", []
    placeholders = ", ".join("?" for _ in blocked)
    return f" AND symbol NOT IN ({placeholders})", blocked


def mark_signal_closed(
    signal_id: str,
    outcome: str,
    pnl_r: float | None,
    filled_at: int | None,
    notes: str | None,
    close_state: str | None = None,
) -> None:
    conn = sqlite3.connect(get_db_path())
    try:
        state_expr = "state = ?," if close_state is not None else ""
        params: list[object] = [
            int(time.time()),
            outcome,
            pnl_r,
            filled_at,
        ]
        if close_state is not None:
            params.append(close_state)
        params.extend([notes, signal_id])
        conn.execute(
            """
            UPDATE signal_audit
            SET status = 'closed',
                closed_at = ?,
                outcome = ?,
                pnl_r = ?,
                filled_at = ?,
                {state_expr}
                notes = ?
            WHERE signal_id = ?
            """.format(state_expr=state_expr),
            params,
        )
        conn.commit()
    finally:
        conn.close()


def mark_signal_state(
    signal_id: str,
    *,
    from_states: tuple[str, ...],
    to_state: str,
    poi_touched_at: int | None = None,
    activated_at: int | None = None,
    entry_price: float | None = None,
    confirm_count: int | None = None,
) -> int:
    if not from_states:
        return 0
    placeholders = ", ".join("?" for _ in from_states)
    params: list[object] = []
    set_clauses = ["state = ?"]
    params.append(str(to_state))
    if poi_touched_at is not None:
        set_clauses.append("poi_touched_at = COALESCE(poi_touched_at, ?)")
        params.append(int(poi_touched_at))
    if activated_at is not None:
        set_clauses.append("activated_at = COALESCE(activated_at, ?)")
        params.append(int(activated_at))
        set_clauses.append("is_activated = 1")
    if entry_price is not None:
        set_clauses.append("entry_price = COALESCE(entry_price, ?)")
        params.append(float(entry_price))
    if confirm_count is not None:
        set_clauses.append("confirm_count = ?")
        params.append(int(confirm_count))
    params.extend([signal_id, *from_states])

    conn = sqlite3.connect(get_db_path())
    try:
        cur = conn.execute(
            f"""
            UPDATE signal_audit
            SET {', '.join(set_clauses)}
            WHERE signal_id = ?
              AND status = 'open'
              AND COALESCE(state, 'WAITING_ENTRY') IN ({placeholders})
            """,
            params,
        )
        conn.commit()
        return int(cur.rowcount or 0)
    finally:
        conn.close()


def mark_signal_activated(signal_id: str, *, activated_at: int, entry_price: float) -> int:
    conn = sqlite3.connect(get_db_path())
    try:
        cur = conn.execute(
            """
            UPDATE signal_audit
            SET activated_at = ?,
                is_activated = 1,
                entry_price = ?
            WHERE signal_id = ?
              AND activated_at IS NULL
              AND status = 'open'
            """,
            (int(activated_at), float(entry_price), signal_id),
        )
        conn.commit()
        return int(cur.rowcount or 0)
    finally:
        conn.close()


def mark_signal_tp1_hit(signal_id: str, *, tp1_hit_at: int) -> int:
    conn = sqlite3.connect(get_db_path())
    try:
        cur = conn.execute(
            """
            UPDATE signal_audit
            SET tp1_hit = 1,
                tp1_hit_at = COALESCE(tp1_hit_at, ?),
                be_armed = 1
            WHERE signal_id = ?
              AND status = 'open'
              AND COALESCE(tp1_hit, 0) = 0
            """,
            (int(tp1_hit_at), signal_id),
        )
        conn.commit()
        return int(cur.rowcount or 0)
    finally:
        conn.close()


def claim_signal_notification(signal_id: str, *, event_type: str) -> bool:
    col_map = {
        "TP1": "tp1_notified",
        "TP2": "tp2_notified",
        "SL": "sl_notified",
        "BE": "be_notified",
        "EXP": "exp_notified",
        "NO_FILL": "nf_notified",
    }
    col = col_map.get(str(event_type).upper())
    if col is None:
        return False
    conn = sqlite3.connect(get_db_path())
    try:
        cur = conn.execute(
            f"""
            UPDATE signal_audit
            SET {col} = 1
            WHERE signal_id = ?
              AND COALESCE({col}, 0) = 0
            """,
            (signal_id,),
        )
        conn.commit()
        return int(cur.rowcount or 0) > 0
    finally:
        conn.close()


def fetch_open_signals(max_age_sec: int = 86400) -> list[dict]:
    conn = sqlite3.connect(get_db_path())
    conn.row_factory = sqlite3.Row
    try:
        blocked_clause, blocked_params = _blocked_symbols_clause()
        cur = conn.cursor()
        cur.execute(
            f"""
            SELECT *
            FROM signal_audit
            WHERE status = 'open' AND sent_at >= ?
              AND NOT (
                symbol LIKE 'TEST%' OR
                LOWER(COALESCE(reason_json, '')) LIKE '%test%' OR
                LOWER(COALESCE(reason_json, '')) LIKE '%тест%' OR
                LOWER(COALESCE(breakdown_json, '')) LIKE '%test%' OR
                LOWER(COALESCE(breakdown_json, '')) LIKE '%тест%' OR
                LOWER(COALESCE(notes, '')) LIKE '%test%' OR
                LOWER(COALESCE(notes, '')) LIKE '%тест%'
              )
              {blocked_clause}
            ORDER BY sent_at ASC
            """,
            [int(time.time()) - max_age_sec, *blocked_params],
        )
        rows = cur.fetchall()
        return [dict(row) for row in rows]
    finally:
        conn.close()


def get_public_stats(days: int = 30, *, include_legacy: bool = False) -> dict:
    since_ts = int(time.time()) - days * 86400
    cutoff_ts = get_effective_cutoff_ts(include_legacy=include_legacy)
    if cutoff_ts > since_ts:
        since_ts = cutoff_ts
    min_score = 80.0
    conn = sqlite3.connect(get_db_path())
    conn.row_factory = sqlite3.Row
    try:
        blocked_clause, blocked_params = _blocked_symbols_clause()
        cur = conn.cursor()
        cur.execute(
            f"""
            SELECT COUNT(*) AS total
            FROM signal_audit
            WHERE sent_at >= ?
              AND score >= ?
              AND (status != 'closed' OR outcome != 'EXPIRED')
              AND NOT (
                symbol LIKE 'TEST%' OR
                LOWER(COALESCE(reason_json, '')) LIKE '%test%' OR
                LOWER(COALESCE(reason_json, '')) LIKE '%тест%' OR
                LOWER(COALESCE(breakdown_json, '')) LIKE '%test%' OR
                LOWER(COALESCE(breakdown_json, '')) LIKE '%тест%' OR
                LOWER(COALESCE(notes, '')) LIKE '%test%' OR
                LOWER(COALESCE(notes, '')) LIKE '%тест%'
              )
              {blocked_clause}
            """,
            [since_ts, min_score, *blocked_params],
        )
        total = int(cur.fetchone()["total"])

        cur.execute(
            f"""
            SELECT outcome, pnl_r, tp1_hit
            FROM signal_audit
            WHERE status = 'closed' AND sent_at >= ?
              AND score >= ?
              AND outcome != 'EXPIRED'
              AND NOT (
                symbol LIKE 'TEST%' OR
                LOWER(COALESCE(reason_json, '')) LIKE '%test%' OR
                LOWER(COALESCE(reason_json, '')) LIKE '%тест%' OR
                LOWER(COALESCE(breakdown_json, '')) LIKE '%test%' OR
                LOWER(COALESCE(breakdown_json, '')) LIKE '%тест%' OR
                LOWER(COALESCE(notes, '')) LIKE '%test%' OR
                LOWER(COALESCE(notes, '')) LIKE '%тест%'
              )
              {blocked_clause}
            """,
            [since_ts, min_score, *blocked_params],
        )
        closed_rows = cur.fetchall()
        closed = len(closed_rows)

        excluded_outcomes = {"NO_FILL", "AMBIGUOUS"}
        filled_rows = [row for row in closed_rows if row["outcome"] not in excluded_outcomes]
        filled_closed = len(filled_rows)

        wins = sum(1 for row in filled_rows if row["outcome"] in ("TP1", "TP2") or (row["outcome"] == "BE" and int(row["tp1_hit"] or 0) == 1))
        winrate = safe_div(wins, filled_closed, 0.0) if filled_closed else 0.0

        pnl_values = [row["pnl_r"] for row in filled_rows if row["pnl_r"] is not None]
        avg_r = safe_div(sum(pnl_values), len(pnl_values), 0.0) if pnl_values else 0.0
        median_r = float(median(pnl_values)) if pnl_values else 0.0

        positive_sum = sum(value for value in pnl_values if value > 0)
        negative_sum = sum(value for value in pnl_values if value < 0)
        profit_factor = (
            safe_div(positive_sum, abs(negative_sum), 0.0) if negative_sum < 0 else None
        )

        cur.execute(
            f"""
            SELECT symbol, direction, outcome, pnl_r, tp1_hit
            FROM signal_audit
            WHERE sent_at >= ?
              AND score >= ?
              AND (status != 'closed' OR outcome != 'EXPIRED')
              AND NOT (
                symbol LIKE 'TEST%' OR
                LOWER(COALESCE(reason_json, '')) LIKE '%test%' OR
                LOWER(COALESCE(reason_json, '')) LIKE '%тест%' OR
                LOWER(COALESCE(breakdown_json, '')) LIKE '%test%' OR
                LOWER(COALESCE(breakdown_json, '')) LIKE '%тест%' OR
                LOWER(COALESCE(notes, '')) LIKE '%test%' OR
                LOWER(COALESCE(notes, '')) LIKE '%тест%'
              )
              {blocked_clause}
            ORDER BY sent_at DESC
            LIMIT 10
            """,
            [since_ts, min_score, *blocked_params],
        )
        last10 = [dict(row) for row in cur.fetchall()]

        cur.execute(
            f"""
            SELECT outcome, tp1_hit
            FROM signal_audit
            WHERE status = 'closed' AND sent_at >= ?
              AND score >= ?
              AND outcome != 'EXPIRED'
              AND NOT (
                symbol LIKE 'TEST%' OR
                LOWER(COALESCE(reason_json, '')) LIKE '%test%' OR
                LOWER(COALESCE(reason_json, '')) LIKE '%тест%' OR
                LOWER(COALESCE(breakdown_json, '')) LIKE '%test%' OR
                LOWER(COALESCE(breakdown_json, '')) LIKE '%тест%' OR
                LOWER(COALESCE(notes, '')) LIKE '%test%' OR
                LOWER(COALESCE(notes, '')) LIKE '%тест%'
              )
              {blocked_clause}
            ORDER BY closed_at DESC
            """,
            [since_ts, min_score, *blocked_params],
        )
        streak_rows = [
            ("BE_TP1" if row["outcome"] == "BE" and int(row["tp1_hit"] or 0) == 1 else row["outcome"])
            for row in cur.fetchall()
            if row["outcome"] not in excluded_outcomes
        ]
        streak = "-"
        if streak_rows:
            first = streak_rows[0]
            is_win = first in ("TP1", "TP2") or first == "BE_TP1"
            count = 0
            for outcome in streak_rows:
                outcome_is_win = outcome in ("TP1", "TP2", "BE_TP1")
                if outcome_is_win == is_win:
                    count += 1
                else:
                    break
            streak = f"{'W' if is_win else 'L'}{count}"

        filled_rate = safe_div(filled_closed, total, 0.0) if total else 0.0

        return {
            "total": total,
            "closed": closed,
            "filled_closed": filled_closed,
            "filled_rate": filled_rate,
            "winrate": winrate,
            "avg_r": avg_r,
            "median_r": median_r,
            "profit_factor": profit_factor,
            "streak": streak,
            "last10": last10,
        }
    finally:
        conn.close()


def get_last_signal_audit(module: str, *, include_legacy: bool = False) -> dict | None:
    conn = sqlite3.connect(get_db_path())
    conn.row_factory = sqlite3.Row
    try:
        blocked_clause, blocked_params = _blocked_symbols_clause()
        cur = conn.cursor()
        cur.execute(
            f"""
            SELECT symbol, direction, score, sent_at
            FROM signal_audit
            WHERE module = ?
              AND sent_at >= ?
              AND NOT (
                symbol LIKE 'TEST%' OR
                LOWER(COALESCE(reason_json, '')) LIKE '%test%' OR
                LOWER(COALESCE(reason_json, '')) LIKE '%тест%' OR
                LOWER(COALESCE(breakdown_json, '')) LIKE '%test%' OR
                LOWER(COALESCE(breakdown_json, '')) LIKE '%тест%' OR
                LOWER(COALESCE(notes, '')) LIKE '%test%' OR
                LOWER(COALESCE(notes, '')) LIKE '%тест%'
              )
              {blocked_clause}
            ORDER BY sent_at DESC
            LIMIT 1
            """,
            [module, get_effective_cutoff_ts(include_legacy=include_legacy), *blocked_params],
        )
        row = cur.fetchone()
        return dict(row) if row else None
    finally:
        conn.close()


def count_signals_sent_since(
    since_ts: int,
    *,
    module: str | None = "ai_signals",
    include_legacy: bool = False,
) -> int:
    conn = sqlite3.connect(get_db_path())
    conn.row_factory = sqlite3.Row
    try:
        blocked_clause, blocked_params = _blocked_symbols_clause()
        module_clause = ""
        params: list[object] = [max(int(since_ts), get_effective_cutoff_ts(include_legacy=include_legacy))]
        if module:
            module_clause = "AND module = ?"
            params.append(module)
        cur = conn.cursor()
        cur.execute(
            f"""
            SELECT COUNT(*) AS total
            FROM signal_audit
            WHERE sent_at >= ?
              {module_clause}
              AND NOT (
                symbol LIKE 'TEST%' OR
                LOWER(COALESCE(reason_json, '')) LIKE '%test%' OR
                LOWER(COALESCE(reason_json, '')) LIKE '%тест%' OR
                LOWER(COALESCE(breakdown_json, '')) LIKE '%test%' OR
                LOWER(COALESCE(breakdown_json, '')) LIKE '%тест%' OR
                LOWER(COALESCE(notes, '')) LIKE '%test%' OR
                LOWER(COALESCE(notes, '')) LIKE '%тест%'
              )
              {blocked_clause}
            """,
            [*params, *blocked_params],
        )
        row = cur.fetchone()
        if not row:
            return 0
        return int(row["total"] or 0)
    finally:
        conn.close()


def get_ai_signal_stats(days: int | None, *, include_legacy: bool = False) -> dict:
    now = int(time.time())
    params: list[Any] = ["TP1", "TP2", "SL", "BE", 80.0]
    since_clauses: list[str] = []
    if days is not None:
        since_clauses.append("sent_at >= ?")
        params.append(now - days * 86400)
    cutoff_ts = get_effective_cutoff_ts(include_legacy=include_legacy)
    if cutoff_ts > 0:
        since_clauses.append("sent_at >= ?")
        params.append(cutoff_ts)
    since_clause = ""
    if since_clauses:
        since_clause = " AND " + " AND ".join(since_clauses)

    blocked_clause, blocked_params = _blocked_symbols_clause()
    params.extend(blocked_params)
    conn = sqlite3.connect(get_db_path())
    conn.row_factory = sqlite3.Row
    try:
        cur = conn.cursor()
        cur.execute(
            f"""
            SELECT outcome, score, tp1_hit
            FROM signal_audit
            WHERE status = 'closed'
              AND outcome IN (?, ?, ?, ?)
              AND score >= ?
              AND NOT (
                symbol LIKE 'TEST%' OR
                LOWER(COALESCE(reason_json, '')) LIKE '%test%' OR
                LOWER(COALESCE(reason_json, '')) LIKE '%тест%' OR
                LOWER(COALESCE(breakdown_json, '')) LIKE '%test%' OR
                LOWER(COALESCE(breakdown_json, '')) LIKE '%тест%' OR
                LOWER(COALESCE(notes, '')) LIKE '%test%' OR
                LOWER(COALESCE(notes, '')) LIKE '%тест%'
              )
              {since_clause}
              {blocked_clause}
            """,
            params,
        )
        rows = cur.fetchall()

        total = len(rows)
        tp2 = sum(1 for row in rows if row["outcome"] == "TP2")
        tp1 = sum(1 for row in rows if row["outcome"] in ("TP1", "TP2") or (row["outcome"] == "BE" and int(row["tp1_hit"] or 0) == 1))
        sl = sum(1 for row in rows if row["outcome"] == "SL")
        exp = 0
        winrate = safe_pct(tp1, total, 0.0) if total else 0.0

        buckets = {
            "0-69": {"total": 0, "tp1plus": 0},
            "70-79": {"total": 0, "tp1plus": 0},
            "80-100": {"total": 0, "tp1plus": 0},
        }
        for row in rows:
            score = row["score"]
            if score is None:
                continue
            score_value = float(score)
            if score_value < 70:
                bucket = buckets["0-69"]
            elif score_value < 80:
                bucket = buckets["70-79"]
            else:
                bucket = buckets["80-100"]
            bucket["total"] += 1
            if row["outcome"] in ("TP1", "TP2") or (row["outcome"] == "BE" and int(row["tp1_hit"] or 0) == 1):
                bucket["tp1plus"] += 1

        for bucket in buckets.values():
            total_bucket = bucket["total"]
            bucket["winrate"] = safe_pct(bucket["tp1plus"], total_bucket, 0.0) if total_bucket else 0.0

        return {
            "total": total,
            "tp1": tp1,
            "tp2": tp2,
            "sl": sl,
            "exp": exp,
            "winrate": winrate,
            "buckets": buckets,
        }
    finally:
        conn.close()
