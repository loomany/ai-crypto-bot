import asyncio
import json
import os
import sqlite3
import time
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Optional

from binance_rest import binance_request_context, fetch_klines
from db import update_signal_events_status
from db_path import get_db_path

START_TS = "2026-02-14T00:01:00"

EXPIRED_STATUSES = {
    "EXPIRED",
    "EXP",
    "NO_FILL",
    "NF",
    "EXPIRED_NO_ENTRY",
    "WAIT_CONFIRM_EXPIRED",
}
FINAL_OK_STATUSES = {"TP", "TP1", "TP2", "SL", "BE", "CLOSED_TP1", "CLOSED_TP2", "CLOSED_SL", "CLOSED_BE"}


@dataclass
class BackfillOutcome:
    final_status: str
    closed_at: int
    activated_at: int | None
    entry_price: float | None
    be_activated_at: int | None
    audit_note: str
    notes: str | None = None


def _iso_to_ts(value: str) -> int:
    try:
        return int(datetime.fromisoformat(value).timestamp())
    except ValueError:
        return 0


def _parse_kline(kline: list[Any]) -> Optional[dict[str, float]]:
    try:
        return {
            "open_time": int(kline[0]),
            "open": float(kline[1]),
            "high": float(kline[2]),
            "low": float(kline[3]),
            "close": float(kline[4]),
        }
    except (TypeError, ValueError, IndexError):
        return None


def _entry_filled(candle: dict[str, float], entry_low: float, entry_high: float, mode: str) -> bool:
    if mode == "close":
        return entry_low <= candle["close"] <= entry_high
    return candle["low"] <= entry_high and candle["high"] >= entry_low


def _directional_confirm(candle: dict[str, float], direction: str, entry_low: float, entry_high: float) -> bool:
    close = float(candle["close"])
    if direction == "long":
        return close > entry_high
    return close < entry_low


def _resolve_intrabar(candle: dict[str, float], targets: list[tuple[str, float]]) -> str:
    open_px = float(candle["open"])
    winner = min(targets, key=lambda item: (abs(item[1] - open_px), item[0]))
    return winner[0]


def evaluate_backfill_outcome(
    signal: dict[str, Any],
    candles_5m: list[dict[str, float]],
    candles_15m: list[dict[str, float]],
    *,
    now_ts: int,
    require_15m_confirm: bool,
) -> BackfillOutcome:
    direction = str(signal.get("direction", "")).lower()
    entry_low = min(float(signal.get("entry_from", 0.0)), float(signal.get("entry_to", 0.0)))
    entry_high = max(float(signal.get("entry_from", 0.0)), float(signal.get("entry_to", 0.0)))
    sl = float(signal.get("sl", 0.0))
    tp1 = float(signal.get("tp1", 0.0))
    tp2 = float(signal.get("tp2", 0.0))
    sent_at = int(signal.get("sent_at", 0) or 0)
    ttl_minutes = int(signal.get("ttl_minutes") or 720)
    ttl_sec = max(60, ttl_minutes * 60)
    close_cap = int(signal.get("closed_at") or now_ts)
    window_end = min(sent_at + ttl_sec, close_cap)

    default_mode = str(os.getenv("AUDIT_ENTRY_MODE", "wick") or "wick").strip().lower()
    elite_mode = str(os.getenv("AUDIT_ELITE_ENTRY_MODE", "close") or "close").strip().lower()
    entry_mode = str(signal.get("entry_mode") or "").strip().lower()
    if entry_mode not in {"wick", "close"}:
        score = float(signal.get("score", 0.0) or 0.0)
        elite_gate = float(os.getenv("AUDIT_ELITE_SCORE_GATE", "90") or 90)
        entry_mode = elite_mode if score >= elite_gate else default_mode
    if entry_mode not in {"wick", "close"}:
        entry_mode = "wick"

    be_trigger_pct = signal.get("be_trigger_pct")
    if be_trigger_pct is None:
        be_trigger_pct = os.getenv("BE_TRIGGER_PCT", "0")
    try:
        be_trigger_pct_value = float(be_trigger_pct or 0.0)
    except (TypeError, ValueError):
        be_trigger_pct_value = 0.0

    confirm_15m_map = {int(c["open_time"] // 1000): c for c in candles_15m}

    activated_at: int | None = None
    entry_price: float | None = None
    for candle in candles_5m:
        ts = int(candle["open_time"] // 1000)
        if ts < sent_at or ts > window_end:
            continue
        if not _entry_filled(candle, entry_low, entry_high, entry_mode):
            continue
        if require_15m_confirm:
            tf15_open = ts - (ts % 900)
            c15 = confirm_15m_map.get(tf15_open)
            if c15 is None or not _directional_confirm(c15, direction, entry_low, entry_high):
                continue
        activated_at = ts
        entry_price = float(candle["close"]) if entry_mode == "close" else (entry_low + entry_high) / 2.0
        break

    if activated_at is None or entry_price is None:
        return BackfillOutcome("EXPIRED_NO_ENTRY", window_end, None, None, None, "backfill 5m/15m", "entry not confirmed within TTL")

    be_activated_at: int | None = None

    for candle in candles_5m:
        ts = int(candle["open_time"] // 1000)
        if ts < activated_at or ts > window_end:
            continue

        if direction == "long":
            sl_hit = candle["low"] <= sl
            tp1_hit = candle["high"] >= tp1
            tp2_hit = candle["high"] >= tp2
            favorable_move_pct = ((candle["high"] - entry_price) / entry_price) * 100.0
            be_hit = candle["low"] <= entry_price
        else:
            sl_hit = candle["high"] >= sl
            tp1_hit = candle["low"] <= tp1
            tp2_hit = candle["low"] <= tp2
            favorable_move_pct = ((entry_price - candle["low"]) / entry_price) * 100.0
            be_hit = candle["high"] >= entry_price

        if be_trigger_pct_value > 0 and be_activated_at is None and favorable_move_pct >= be_trigger_pct_value:
            be_activated_at = ts

        touched: list[tuple[str, float]] = []
        if tp2_hit:
            touched.append(("TP2", tp2))
        elif tp1_hit:
            touched.append(("TP1", tp1))
        if sl_hit:
            touched.append(("SL", sl))

        if len(touched) >= 2:
            winner = _resolve_intrabar(candle, touched)
            return BackfillOutcome(winner, ts, activated_at, entry_price, be_activated_at, "backfill 5m/15m")
        if touched:
            return BackfillOutcome(touched[0][0], ts, activated_at, entry_price, be_activated_at, "backfill 5m/15m")

        if be_activated_at is not None and be_hit:
            return BackfillOutcome("BE", ts, activated_at, entry_price, be_activated_at, "backfill 5m/15m")

    return BackfillOutcome("EXPIRED_NO_ENTRY", window_end, activated_at, entry_price, be_activated_at, "backfill 5m/15m", "entry confirmed but final target not reached within TTL")


async def _load_klines_window(
    symbol: str,
    interval: str,
    start_ts: int,
    end_ts: int,
    cache: dict[tuple[str, str, int, int], list[dict[str, float]]],
) -> list[dict[str, float]]:
    key = (symbol, interval, start_ts, end_ts)
    if key in cache:
        return cache[key]

    interval_sec = 300 if interval == "5m" else 900
    limit = max(10, min(1000, int((end_ts - start_ts) / interval_sec) + 8))
    with binance_request_context("signal_backfill"):
        raw = await fetch_klines(symbol, interval, limit, start_ms=start_ts * 1000)
    parsed = []
    for row in raw or []:
        candle = _parse_kline(row)
        if not candle:
            continue
        ts = int(candle["open_time"] // 1000)
        if start_ts <= ts <= end_ts:
            parsed.append(candle)
    cache[key] = parsed
    return parsed


def _parse_reason_json(raw: Any) -> dict[str, Any]:
    if isinstance(raw, dict):
        return raw
    if not raw:
        return {}
    try:
        value = json.loads(raw)
        return value if isinstance(value, dict) else {}
    except (TypeError, ValueError, json.JSONDecodeError):
        return {}


def _fetch_candidates(start_ts: int) -> list[dict[str, Any]]:
    conn = sqlite3.connect(get_db_path())
    conn.row_factory = sqlite3.Row
    try:
        rows = conn.execute("SELECT * FROM signal_audit WHERE sent_at >= ? ORDER BY sent_at ASC", (int(start_ts),)).fetchall()
        out = []
        for row in rows:
            item = dict(row)
            status_tokens = {
                str(item.get("status") or "").upper(),
                str(item.get("outcome") or "").upper(),
                str(item.get("final_status") or "").upper(),
                str(item.get("state") or "").upper(),
            }
            if status_tokens & FINAL_OK_STATUSES:
                continue
            if not (status_tokens & EXPIRED_STATUSES or str(item.get("state") or "").upper().endswith("EXPIRED")):
                continue
            reason = _parse_reason_json(item.get("reason_json"))
            if reason.get("entry_mode"):
                item["entry_mode"] = reason.get("entry_mode")
            if reason.get("be_trigger_pct") is not None:
                item["be_trigger_pct"] = reason.get("be_trigger_pct")
            out.append(item)
        return out
    finally:
        conn.close()


def _apply_backfill(signal: dict[str, Any], outcome: BackfillOutcome) -> int:
    conn = sqlite3.connect(get_db_path())
    try:
        cur = conn.execute(
            """
            UPDATE signal_audit
            SET status = 'closed',
                state = CASE
                    WHEN ? = 'TP1' THEN 'CLOSED_TP1'
                    WHEN ? = 'TP2' THEN 'CLOSED_TP2'
                    WHEN ? = 'SL' THEN 'CLOSED_SL'
                    WHEN ? = 'BE' THEN 'CLOSED_BE'
                    ELSE 'EXPIRED'
                END,
                outcome = ?,
                final_status = ?,
                closed_at = ?,
                activated_at = COALESCE(activated_at, ?),
                is_activated = CASE WHEN ? IS NOT NULL THEN 1 ELSE COALESCE(is_activated, 0) END,
                entry_price = COALESCE(entry_price, ?),
                be_triggered = CASE WHEN ? IS NOT NULL THEN 1 ELSE COALESCE(be_triggered, 0) END,
                be_activated_at = COALESCE(be_activated_at, ?),
                audit_note = ?,
                notes = COALESCE(?, notes)
            WHERE signal_id = ?
            """,
            (
                outcome.final_status,
                outcome.final_status,
                outcome.final_status,
                outcome.final_status,
                outcome.final_status,
                outcome.final_status,
                int(outcome.closed_at),
                outcome.activated_at,
                outcome.activated_at,
                outcome.entry_price,
                outcome.be_activated_at,
                outcome.be_activated_at,
                outcome.audit_note,
                outcome.notes,
                str(signal["signal_id"]),
            ),
        )
        conn.commit()
        return int(cur.rowcount or 0)
    finally:
        conn.close()


async def run_backfill_finals(*, dry_run: bool = True, start_ts: str = START_TS, batch_size: int = 100) -> dict[str, int]:
    start_value = _iso_to_ts(start_ts)
    candidates = _fetch_candidates(start_value)
    now_ts = int(time.time())
    require_15m_confirm = (
        str(os.getenv("ELITE_REQUIRE_CONFIRM", "0") or "0").strip().lower() in {"1", "true", "yes", "on"}
        and str(os.getenv("ELITE_CONFIRM_TF", "5m") or "5m").strip().lower() == "15m"
    )

    stats = {"updated": 0, "TP1": 0, "TP2": 0, "SL": 0, "BE": 0, "EXPIRED_NO_ENTRY": 0}
    cache: dict[tuple[str, str, int, int], list[dict[str, float]]] = {}

    for idx in range(0, len(candidates), max(1, int(batch_size))):
        for signal in candidates[idx : idx + max(1, int(batch_size))]:
            sent_at = int(signal.get("sent_at") or 0)
            ttl_minutes = int(signal.get("ttl_minutes") or 720)
            ttl_sec = max(60, ttl_minutes * 60)
            close_cap = int(signal.get("closed_at") or now_ts)
            end_ts = min(sent_at + ttl_sec, close_cap)
            symbol = str(signal.get("symbol") or "").upper()
            if not symbol:
                continue
            candles_5m, candles_15m = await asyncio.gather(
                _load_klines_window(symbol, "5m", sent_at, end_ts, cache),
                _load_klines_window(symbol, "15m", sent_at - 900, end_ts + 900, cache),
            )
            outcome = evaluate_backfill_outcome(
                signal,
                candles_5m,
                candles_15m,
                now_ts=now_ts,
                require_15m_confirm=require_15m_confirm,
            )
            stats[outcome.final_status] += 1
            if dry_run:
                continue
            changed = _apply_backfill(signal, outcome)
            if changed > 0:
                stats["updated"] += 1
                event_status = "EXP" if outcome.final_status == "EXPIRED_NO_ENTRY" else outcome.final_status
                update_signal_events_status(
                    module=str(signal.get("module") or "ai_signals"),
                    symbol=symbol,
                    ts=int(signal.get("sent_at") or 0),
                    status=event_status,
                )
    return stats
