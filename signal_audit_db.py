import json
import sqlite3
import time
import uuid
from statistics import median
from typing import Any, Dict

from db_path import get_db_path


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
                notes TEXT
            )
            """
        )
        conn.execute("CREATE INDEX IF NOT EXISTS idx_signal_audit_status ON signal_audit(status)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_signal_audit_sent_at ON signal_audit(sent_at)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_signal_audit_symbol ON signal_audit(symbol)")
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
    return reward / risk if risk > 0 else 0.0


def insert_signal_audit(signal_dict: dict, tier: str, module: str) -> str:
    signal_id = str(uuid.uuid4())
    entry_from, entry_to = signal_dict.get("entry_zone") or (0.0, 0.0)
    reason = signal_dict.get("reason") if isinstance(signal_dict.get("reason"), dict) else {}
    breakdown = signal_dict.get("breakdown") if isinstance(signal_dict.get("breakdown"), list) else []
    rr_value = _compute_rr(signal_dict)

    payload = (
        signal_id,
        module,
        tier,
        str(signal_dict.get("symbol", "")),
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
        int(time.time()),
        "open",
    )

    conn = sqlite3.connect(get_db_path())
    try:
        conn.execute(
            """
            INSERT INTO signal_audit (
                signal_id, module, tier, symbol, direction, timeframe,
                entry_from, entry_to, sl, tp1, tp2, score, rr,
                reason_json, breakdown_json, sent_at, status
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            payload,
        )
        conn.commit()
    finally:
        conn.close()

    return signal_id


def mark_signal_closed(
    signal_id: str,
    outcome: str,
    pnl_r: float | None,
    filled_at: int | None,
    notes: str | None,
) -> None:
    conn = sqlite3.connect(get_db_path())
    try:
        conn.execute(
            """
            UPDATE signal_audit
            SET status = 'closed',
                closed_at = ?,
                outcome = ?,
                pnl_r = ?,
                filled_at = ?,
                notes = ?
            WHERE signal_id = ?
            """,
            (
                int(time.time()),
                outcome,
                pnl_r,
                filled_at,
                notes,
                signal_id,
            ),
        )
        conn.commit()
    finally:
        conn.close()


def fetch_open_signals(max_age_sec: int = 86400) -> list[dict]:
    now = int(time.time())
    conn = sqlite3.connect(get_db_path())
    conn.row_factory = sqlite3.Row
    try:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT *
            FROM signal_audit
            WHERE status = 'open' AND sent_at >= ?
            ORDER BY sent_at ASC
            """,
            (now - max_age_sec,),
        )
        rows = cur.fetchall()
        return [dict(row) for row in rows]
    finally:
        conn.close()


def get_public_stats(days: int = 30) -> dict:
    since_ts = int(time.time()) - days * 86400
    conn = sqlite3.connect(get_db_path())
    conn.row_factory = sqlite3.Row
    try:
        cur = conn.cursor()
        cur.execute("SELECT COUNT(*) AS total FROM signal_audit WHERE sent_at >= ?", (since_ts,))
        total = int(cur.fetchone()["total"])

        cur.execute(
            """
            SELECT outcome, pnl_r
            FROM signal_audit
            WHERE status = 'closed' AND sent_at >= ?
            """,
            (since_ts,),
        )
        closed_rows = cur.fetchall()
        closed = len(closed_rows)

        excluded_outcomes = {"NO_FILL", "AMBIGUOUS"}
        filled_rows = [row for row in closed_rows if row["outcome"] not in excluded_outcomes]
        filled_closed = len(filled_rows)

        wins = sum(1 for row in filled_rows if row["outcome"] in ("TP1", "TP2", "BE"))
        winrate = wins / filled_closed if filled_closed else 0.0

        pnl_values = [row["pnl_r"] for row in filled_rows if row["pnl_r"] is not None]
        avg_r = sum(pnl_values) / len(pnl_values) if pnl_values else 0.0
        median_r = float(median(pnl_values)) if pnl_values else 0.0

        positive_sum = sum(value for value in pnl_values if value > 0)
        negative_sum = sum(value for value in pnl_values if value < 0)
        profit_factor = (
            positive_sum / abs(negative_sum) if negative_sum < 0 else None
        )

        cur.execute(
            """
            SELECT symbol, direction, outcome, pnl_r
            FROM signal_audit
            WHERE sent_at >= ?
            ORDER BY sent_at DESC
            LIMIT 10
            """,
            (since_ts,),
        )
        last10 = [dict(row) for row in cur.fetchall()]

        cur.execute(
            """
            SELECT outcome
            FROM signal_audit
            WHERE status = 'closed' AND sent_at >= ?
            ORDER BY closed_at DESC
            """,
            (since_ts,),
        )
        streak_rows = [row["outcome"] for row in cur.fetchall() if row["outcome"] not in excluded_outcomes]
        streak = "-"
        if streak_rows:
            first = streak_rows[0]
            is_win = first in ("TP1", "TP2", "BE")
            count = 0
            for outcome in streak_rows:
                outcome_is_win = outcome in ("TP1", "TP2", "BE")
                if outcome_is_win == is_win:
                    count += 1
                else:
                    break
            streak = f"{'W' if is_win else 'L'}{count}"

        filled_rate = filled_closed / total if total else 0.0

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
