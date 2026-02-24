import json
import sqlite3
import time
from typing import Any, Dict, List

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


def count_arb_notifications_sent_since(since_ts: int) -> int:
    conn = get_conn()
    try:
        row = conn.execute(
            "SELECT COUNT(*) FROM arb_sent_log WHERE sent_at >= ?",
            (int(since_ts),),
        ).fetchone()
        return int(row[0] or 0) if row else 0
    finally:
        conn.close()


def insert_arb_opportunities(items: List[Dict[str, Any]], *, limit: int = 30) -> int:
    if not items:
        return 0
    top = items[: max(1, int(limit))]
    conn = get_conn()
    try:
        rows = []
        for item in top:
            rows.append(
                (
                    int(item.get("ts", time.time())),
                    str(item.get("symbol", "")),
                    str(item.get("buy_exchange", "")),
                    str(item.get("sell_exchange", "")),
                    float(item.get("ask", 0.0) or 0.0),
                    float(item.get("bid", 0.0) or 0.0),
                    float(item.get("gross_pct", 0.0) or 0.0),
                    float(item.get("net_pct", 0.0) or 0.0),
                    json.dumps(item.get("breakdown", {}), ensure_ascii=False),
                    int(item.get("age_sec", 0) or 0),
                )
            )
        conn.executemany(
            """
            INSERT INTO arb_opportunities (
                ts, symbol, buy_ex, sell_ex, ask, bid, gross, net, breakdown_json, age_sec
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            rows,
        )
        conn.commit()
        return len(rows)
    finally:
        conn.close()


def get_arb_opportunities_page(*, page: int, page_size: int) -> tuple[List[dict], int]:
    p = max(1, int(page))
    ps = max(1, min(100, int(page_size)))
    offset = (p - 1) * ps
    conn = get_conn()
    try:
        total_row = conn.execute("SELECT COUNT(*) FROM arb_opportunities").fetchone()
        total = int(total_row[0] or 0) if total_row else 0
        rows = conn.execute(
            """
            SELECT id, ts, symbol, buy_ex, sell_ex, ask, bid, gross, net, breakdown_json, age_sec
            FROM arb_opportunities
            ORDER BY ts DESC, id DESC
            LIMIT ? OFFSET ?
            """,
            (ps, offset),
        ).fetchall()
        out = []
        for row in rows:
            breakdown = {}
            try:
                breakdown = json.loads(row["breakdown_json"] or "{}")
                if not isinstance(breakdown, dict):
                    breakdown = {}
            except Exception:
                breakdown = {}
            out.append(
                {
                    "id": int(row["id"]),
                    "ts": int(row["ts"]),
                    "symbol": str(row["symbol"]),
                    "buy_exchange": str(row["buy_ex"]),
                    "sell_exchange": str(row["sell_ex"]),
                    "ask": float(row["ask"]),
                    "bid": float(row["bid"]),
                    "gross_pct": float(row["gross"]),
                    "net_pct": float(row["net"]),
                    "breakdown": breakdown,
                    "age_sec": int(row["age_sec"] or 0),
                }
            )
        return out, total
    finally:
        conn.close()


def get_arb_max_metrics_since(since_ts: int) -> dict[str, float]:
    conn = get_conn()
    try:
        row = conn.execute(
            """
            SELECT MAX(net) AS max_net, MAX(gross) AS max_gross
            FROM arb_opportunities
            WHERE ts >= ?
            """,
            (int(since_ts),),
        ).fetchone()
        if not row:
            return {"max_net": 0.0, "max_gross": 0.0}
        return {
            "max_net": float(row["max_net"] or 0.0),
            "max_gross": float(row["max_gross"] or 0.0),
        }
    finally:
        conn.close()
