import asyncio
import os
import time
from typing import Any, Dict, Optional, Tuple

from binance_rest import binance_request_context, fetch_klines
from health import (
    MODULES,
    mark_ok,
    mark_tick,
    safe_worker_loop,
    update_module_progress,
    update_current_symbol,
)
from db import list_signal_events_by_identity, update_signal_events_status
from signal_audit_db import fetch_open_signals, mark_signal_closed
from settings import SIGNAL_TTL_SECONDS

_signal_result_notifier = None


def set_signal_result_notifier(handler) -> None:
    global _signal_result_notifier
    _signal_result_notifier = handler


def _parse_kline(kline: list[Any]) -> Optional[Dict[str, float]]:
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


def _entry_filled(candle: Dict[str, float], entry_from: float, entry_to: float, mode: str = "wick") -> bool:
    if mode == "close":
        return entry_from <= candle["close"] <= entry_to
    return candle["low"] <= entry_to and candle["high"] >= entry_from


def _check_hits(
    candle: Dict[str, float],
    direction: str,
    sl: float,
    tp1: float,
    tp2: float,
    entry_ref: float,
) -> Tuple[bool, bool, bool, bool]:
    if direction == "long":
        sl_hit = candle["low"] <= sl
        tp1_hit = candle["high"] >= tp1
        tp2_hit = candle["high"] >= tp2
        be_hit = candle["low"] <= entry_ref
    else:
        sl_hit = candle["high"] >= sl
        tp1_hit = candle["low"] <= tp1
        tp2_hit = candle["low"] <= tp2
        be_hit = candle["high"] >= entry_ref
    return sl_hit, tp1_hit, tp2_hit, be_hit


def _evaluate_signal(signal: Dict[str, Any], candles: list[Dict[str, float]]) -> Optional[dict]:
    default_mode = os.getenv("AUDIT_ENTRY_MODE", "wick").lower()
    elite_mode = os.getenv("AUDIT_ELITE_ENTRY_MODE", "close").lower()
    try:
        elite_gate = float(os.getenv("AUDIT_ELITE_SCORE_GATE", "90"))
    except (TypeError, ValueError):
        elite_gate = 90.0

    score = float(signal.get("score", 0.0) or 0.0)
    use_mode = elite_mode if score >= elite_gate else default_mode
    if use_mode not in ("wick", "close"):
        use_mode = "wick"

    entry_from = float(signal["entry_from"])
    entry_to = float(signal["entry_to"])
    direction = signal["direction"]
    sl = float(signal["sl"])
    tp1 = float(signal["tp1"])
    tp2 = float(signal["tp2"])
    sent_at = int(signal["sent_at"])

    entry_ref = (entry_from + entry_to) / 2
    r_value = abs(entry_ref - sl)
    if r_value <= 0:
        return None

    tp1_r = abs(tp1 - entry_ref) / r_value
    tp2_r = abs(tp2 - entry_ref) / r_value

    filled_at: int | None = None
    tp1_hit = False

    for candle in candles:
        if filled_at is None:
            if _entry_filled(candle, entry_from, entry_to, use_mode):
                filled_at = int(candle["open_time"] / 1000)
            else:
                continue

        sl_hit, tp1_hit_candle, tp2_hit_candle, be_hit = _check_hits(
            candle, direction, sl, tp1, tp2, entry_ref
        )

        if sl_hit and (tp1_hit_candle or tp2_hit_candle):
            return {
                "outcome": "AMBIGUOUS",
                "pnl_r": None,
                "filled_at": filled_at,
                "notes": "SL and TP touched in same candle",
            }

        if tp2_hit_candle:
            return {
                "outcome": "TP2",
                "pnl_r": 0.5 * tp1_r + 0.5 * tp2_r,
                "filled_at": filled_at,
                "notes": None,
            }

        if tp1_hit_candle and not tp1_hit:
            tp1_hit = True

        if not tp1_hit and sl_hit:
            return {
                "outcome": "SL",
                "pnl_r": -1.0,
                "filled_at": filled_at,
                "notes": None,
            }

        if tp1_hit and be_hit:
            return {
                "outcome": "BE",
                "pnl_r": 0.5 * tp1_r,
                "filled_at": filled_at,
                "notes": None,
            }

    age_sec = int(time.time()) - sent_at
    if age_sec < SIGNAL_TTL_SECONDS:
        return None

    last_close = candles[-1]["close"] if candles else None
    if filled_at is None:
        return {
            "outcome": "NO_FILL",
            "pnl_r": None,
            "filled_at": None,
            "notes": None,
        }

    pnl_r = None
    if last_close is not None:
        if direction == "long":
            pnl_r = (last_close - entry_ref) / r_value * 0.5
        else:
            pnl_r = (entry_ref - last_close) / r_value * 0.5

    return {
        "outcome": "EXPIRED",
        "pnl_r": pnl_r,
        "filled_at": filled_at,
        "notes": None,
    }


async def evaluate_open_signals(
    open_signals: Optional[list[dict]] = None,
    *,
    start_ts: float | None = None,
    budget_sec: int = 45,
) -> None:
    if open_signals is None:
        open_signals = fetch_open_signals(SIGNAL_TTL_SECONDS)
    if not open_signals:
        return

    for signal in open_signals:
        if start_ts is not None and time.time() - start_ts > budget_sec:
            break
        try:
            symbol = signal["symbol"]
            sent_at = int(signal["sent_at"])
            with binance_request_context("signal_audit"):
                data = await fetch_klines(
                    symbol,
                    "5m",
                    500,
                    start_ms=sent_at * 1000,
                )
            if not data:
                continue

            candles = []
            for item in data:
                parsed = _parse_kline(item)
                if parsed and parsed["open_time"] >= sent_at * 1000:
                    candles.append(parsed)

            if not candles:
                continue

            result = _evaluate_signal(signal, candles)
            if result is None:
                continue

            mark_signal_closed(
                signal_id=signal["signal_id"],
                outcome=result["outcome"],
                pnl_r=result["pnl_r"],
                filled_at=result["filled_at"],
                notes=result["notes"],
            )
            status_map = {
                "TP1": "TP1",
                "TP2": "TP2",
                "SL": "SL",
                "EXPIRED": "EXP",
                "BE": "BE",
                "NO_FILL": "NO_FILL",
                "AMBIGUOUS": "AMBIGUOUS",
            }
            status_value = status_map.get(result["outcome"])
            if status_value is not None:
                module = str(signal.get("module", ""))
                symbol = str(signal.get("symbol", ""))
                ts_value = int(signal.get("sent_at", 0))
                update_signal_events_status(
                    module=module,
                    symbol=symbol,
                    ts=ts_value,
                    status=status_value,
                )
                if _signal_result_notifier is not None:
                    events = list_signal_events_by_identity(
                        module=module,
                        symbol=symbol,
                        ts=ts_value,
                    )
                    notify_tasks = [
                        _signal_result_notifier(dict(event)) for event in events
                    ]
                    if notify_tasks:
                        await asyncio.gather(*notify_tasks)
        except Exception as exc:
            print(f"[signal_audit] Failed to evaluate signal {signal.get('signal_id')}: {exc}")
            continue


async def signal_audit_worker_loop() -> None:
    async def _scan_once() -> None:
        start = time.time()
        BUDGET = 45
        mark_tick("signal_audit", extra="audit cycle")
        open_signals = fetch_open_signals(SIGNAL_TTL_SECONDS)
        if not open_signals:
            mark_ok("signal_audit", extra="audit cycle")
            return

        if not hasattr(_scan_once, "cursor"):
            _scan_once.cursor = 0
        chunk_size = 100
        cursor = _scan_once.cursor
        if cursor >= len(open_signals):
            cursor = 0
        chunk = open_signals[cursor : cursor + chunk_size]
        new_cursor = cursor + len(chunk)
        _scan_once.cursor = new_cursor
        total = len(open_signals)
        update_module_progress("signal_audit", total, new_cursor, len(chunk))
        if chunk:
            update_current_symbol("signal_audit", chunk[0].get("symbol", ""))

        await evaluate_open_signals(
            chunk,
            start_ts=start,
            budget_sec=BUDGET,
        )
        current_symbol = (
            MODULES.get("signal_audit").current_symbol if "signal_audit" in MODULES else None
        )
        mark_ok(
            "signal_audit",
            extra=(
                f"progress={new_cursor}/{total} "
                f"checked={len(chunk)}/{len(chunk)} "
                f"current={current_symbol or '-'} cycle={int(time.time() - start)}s"
            ),
        )

    await safe_worker_loop("signal_audit", _scan_once)
