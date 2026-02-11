import asyncio
import logging
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
from signal_audit_db import fetch_open_signals, mark_signal_closed, mark_signal_state
from settings import SIGNAL_TTL_SECONDS
from utils.safe_math import EPS, safe_div

_signal_result_notifier = None
_signal_activation_notifier = None
_signal_poi_touched_notifier = None

logger = logging.getLogger(__name__)


def set_signal_result_notifier(handler) -> None:
    global _signal_result_notifier
    _signal_result_notifier = handler


def set_signal_activation_notifier(handler) -> None:
    global _signal_activation_notifier
    _signal_activation_notifier = handler


def set_signal_poi_touched_notifier(handler) -> None:
    global _signal_poi_touched_notifier
    _signal_poi_touched_notifier = handler


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


def _price_inside_poi(last_price: float, poi_from: float, poi_to: float) -> bool:
    low = min(poi_from, poi_to)
    high = max(poi_from, poi_to)
    return low <= last_price <= high


def _is_close_inside_poi(close_price: float, poi_from: float, poi_to: float) -> bool:
    return _price_inside_poi(close_price, poi_from, poi_to)


def _is_directional_close_outside_poi(close_price: float, direction: str, poi_from: float, poi_to: float) -> bool:
    low = min(poi_from, poi_to)
    high = max(poi_from, poi_to)
    if direction == "long":
        return close_price > high
    return close_price < low


def _resolve_confirm_tf() -> str:
    raw = str(os.getenv("ELITE_CONFIRM_TF", "5m") or "5m").strip().lower()
    return raw or "5m"


def _require_two_closes() -> bool:
    value = str(os.getenv("CONFIRM_REQUIRE_TWO_CLOSES", "1") or "1").strip().lower()
    return value not in {"0", "false", "no"}


def _find_activation_from_candles(signal: Dict[str, Any], candles: list[Dict[str, float]]) -> tuple[int, float, int] | None:
    if not candles:
        return None
    direction = str(signal.get("direction", "")).lower()
    poi_from = float(signal["entry_from"])
    poi_to = float(signal["entry_to"])

    seen_poi_touch = False
    strict = bool(signal.get("confirm_strict", False))
    required_closes = 1
    if strict:
        try:
            required_closes = int(
                os.getenv("SOFT_BTC_STRICT_CONFIRM_N", os.getenv("SOFT_BTC_STRICT_CONFIRM_CLOSES", "2"))
                or "2"
            )
        except (TypeError, ValueError):
            required_closes = 2
        required_closes = max(2, required_closes)
    elif _require_two_closes():
        required_closes = 2

    consecutive = 0
    first_confirm_idx = None
    for idx, candle in enumerate(candles):
        if not seen_poi_touch and _entry_filled(candle, min(poi_from, poi_to), max(poi_from, poi_to)):
            seen_poi_touch = True

        if not seen_poi_touch:
            continue

        close_price = float(candle["close"])
        if _is_close_inside_poi(close_price, poi_from, poi_to):
            consecutive = 0
            first_confirm_idx = None
            continue

        if _is_directional_close_outside_poi(close_price, direction, poi_from, poi_to):
            if consecutive == 0:
                first_confirm_idx = idx
            consecutive += 1
            if consecutive >= required_closes and first_confirm_idx is not None:
                first_candle = candles[first_confirm_idx]
                return int(first_candle["open_time"] / 1000), float(first_candle["close"]), consecutive
        else:
            consecutive = 0
            first_confirm_idx = None
    return None


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
    ttl_minutes = int(signal.get("ttl_minutes") or SIGNAL_TTL_SECONDS // 60)
    ttl_sec = max(60, ttl_minutes * 60)

    entry_ref = (entry_from + entry_to) / 2
    r_value = abs(entry_ref - sl)
    zero_risk = r_value <= EPS
    if zero_risk:
        logger.warning("[zero_div_guard] symbol=%s expr=%s denom=%s", signal.get("symbol", "-"), "risk_r_value", r_value)
    tp1_r = 0.0 if zero_risk else safe_div(abs(tp1 - entry_ref), r_value, 0.0)
    tp2_r = 0.0 if zero_risk else safe_div(abs(tp2 - entry_ref), r_value, 0.0)

    activated_at = signal.get("activated_at")
    if activated_at is None:
        age_sec = int(time.time()) - sent_at
        if age_sec >= ttl_sec:
            return {
                "outcome": "NO_FILL",
                "pnl_r": None,
                "filled_at": None,
                "notes": "closed_without_entry",
            }
        return None

    filled_at: int | None = int(activated_at)
    tp1_hit = False

    for candle in candles:
        candle_ts = int(candle["open_time"] / 1000)
        if candle_ts < int(activated_at):
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
    if age_sec < ttl_sec:
        return None

    last_close = candles[-1]["close"] if candles else None
    pnl_r = None
    notes = None
    if last_close is not None:
        if zero_risk:
            pnl_r = 0.0
            notes = "fail_rr_zero_risk_audit"
        elif direction == "long":
            pnl_r = safe_div((last_close - entry_ref), r_value, 0.0) * 0.5
        else:
            pnl_r = safe_div((entry_ref - last_close), r_value, 0.0) * 0.5

    return {
        "outcome": "EXPIRED",
        "pnl_r": pnl_r,
        "filled_at": filled_at,
        "notes": notes,
    }


async def evaluate_open_signals(
    open_signals: Optional[list[dict]] = None,
    *,
    start_ts: float | None = None,
    budget_sec: int = 45,
) -> Dict[str, Any]:
    cycle_stats: Dict[str, Any] = {
        "checked_signals": 0,
        "skipped_total": 0,
        "skip_reasons": {},
        "triggered_total": 0,
        "last_checked_ts": int(time.time()),
        "last_price_ts": 0,
    }

    def _skip(reason: str) -> None:
        cycle_stats["skipped_total"] = int(cycle_stats.get("skipped_total", 0) or 0) + 1
        reasons = cycle_stats.setdefault("skip_reasons", {})
        reasons[reason] = int(reasons.get(reason, 0) or 0) + 1

    if open_signals is None:
        open_signals = fetch_open_signals()
    if not open_signals:
        _skip("skip_no_active_signals")
        return cycle_stats

    for signal in open_signals:
        if start_ts is not None and time.time() - start_ts > budget_sec:
            break
        try:
            cycle_stats["checked_signals"] = int(cycle_stats.get("checked_signals", 0) or 0) + 1
            symbol = signal["symbol"]
            sent_at = int(signal["sent_at"])
            confirm_tf = _resolve_confirm_tf()
            with binance_request_context("signal_audit"):
                data = await fetch_klines(
                    symbol,
                    confirm_tf,
                    500,
                    start_ms=sent_at * 1000,
                )
            if not data:
                _skip("skip_missing_prices_candles")
                continue

            candles = []
            for item in data:
                parsed = _parse_kline(item)
                if parsed and parsed["open_time"] >= sent_at * 1000:
                    candles.append(parsed)

            if not candles:
                _skip("skip_missing_prices_candles")
                continue

            state = str(signal.get("state") or "WAITING_ENTRY").upper()
            ttl_minutes = int(signal.get("ttl_minutes") or SIGNAL_TTL_SECONDS // 60)
            ttl_sec = max(60, ttl_minutes * 60)
            now_ts = int(time.time())
            if now_ts > sent_at + ttl_sec and state != "ACTIVE_CONFIRMED":
                _skip("skip_signal_expired_ttl")
                mark_signal_closed(
                    signal_id=signal["signal_id"],
                    outcome="NO_FILL",
                    pnl_r=None,
                    filled_at=None,
                    notes="closed_without_entry",
                    close_state="EXPIRED",
                )
                update_signal_events_status(
                    module=str(signal.get("module", "")),
                    symbol=symbol,
                    ts=sent_at,
                    status="NO_FILL",
                )
                continue

            if state == "WAITING_ENTRY":
                last_price = float(candles[-1]["close"])
                if _price_inside_poi(last_price, float(signal["entry_from"]), float(signal["entry_to"])):
                    touched_at = int(time.time())
                    marked = mark_signal_state(
                        str(signal["signal_id"]),
                        from_states=("WAITING_ENTRY",),
                        to_state="POI_TOUCHED",
                        poi_touched_at=touched_at,
                    )
                    if marked > 0:
                        signal["state"] = "POI_TOUCHED"
                        signal["poi_touched_at"] = touched_at
                        if _signal_poi_touched_notifier is not None:
                            await _signal_poi_touched_notifier(signal)
                        state = "POI_TOUCHED"

            if state in {"WAITING_ENTRY", "POI_TOUCHED"}:
                activation = _find_activation_from_candles(signal, candles)
                if activation is not None:
                    activated_at, entry_price, confirm_count = activation
                    marked = mark_signal_state(
                        str(signal["signal_id"]),
                        from_states=("WAITING_ENTRY", "POI_TOUCHED"),
                        to_state="ACTIVE_CONFIRMED",
                        activated_at=activated_at,
                        entry_price=entry_price,
                        confirm_count=confirm_count,
                    )
                    if marked > 0:
                        signal["state"] = "ACTIVE_CONFIRMED"
                        signal["activated_at"] = activated_at
                        signal["entry_price"] = entry_price
                        signal["confirm_count"] = confirm_count
                        logger.info(
                            "[signal_audit] activated signal_id=%s symbol=%s strict=%s confirm_count=%s",
                            signal.get("signal_id"),
                            signal.get("symbol"),
                            bool(signal.get("confirm_strict", False)),
                            confirm_count,
                        )
                        if _signal_activation_notifier is not None:
                            await _signal_activation_notifier(signal)

            result = _evaluate_signal(signal, candles)
            if result is None:
                _skip("skip_no_new_candle_since_last_check")
                continue

            cycle_stats["triggered_total"] = int(cycle_stats.get("triggered_total", 0) or 0) + 1
            if candles:
                cycle_stats["last_price_ts"] = max(
                    int(cycle_stats.get("last_price_ts", 0) or 0),
                    int(candles[-1].get("open_time", 0) // 1000),
                )
            logger.info(
                "[close_notify] close detected (audit) signal_id=%s symbol=%s side=%s reason=%s",
                signal.get("signal_id"),
                signal.get("symbol"),
                str(signal.get("direction", "")).upper(),
                result.get("outcome"),
            )
            mark_signal_closed(
                signal_id=signal["signal_id"],
                outcome=result["outcome"],
                pnl_r=result["pnl_r"],
                filled_at=result["filled_at"],
                notes=result["notes"],
                close_state=("EXPIRED" if result["outcome"] == "NO_FILL" else None),
            )
            logger.info(
                "[close_notify] db updated signal_id=%s outcome=%s",
                signal.get("signal_id"),
                result.get("outcome"),
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
                        logger.info(
                            "[close_notify] notify dispatch signal_id=%s events=%s outcome=%s",
                            signal.get("signal_id"),
                            len(notify_tasks),
                            result.get("outcome"),
                        )
                        await asyncio.gather(*notify_tasks)
                    else:
                        _skip("skip_user_notifications_disabled")
        except Exception as exc:
            print(f"[signal_audit] Failed to evaluate signal {signal.get('signal_id')}: {exc}")
            continue

    return cycle_stats


async def signal_audit_worker_loop() -> None:
    async def _scan_once() -> None:
        start = time.time()
        BUDGET = 45
        mark_tick("signal_audit", extra="audit cycle")
        open_signals = fetch_open_signals()
        if not open_signals:
            module_state = MODULES.get("signal_audit")
            if module_state and isinstance(module_state.state, dict):
                module_state.state["close_detector_checked_signals"] = 0
                module_state.state["close_detector_skipped_signals_total"] = 1
                module_state.state["close_detector_skipped_reasons"] = {"skip_no_active_signals": 1}
                module_state.state["close_detector_triggered_total"] = 0
                module_state.state["last_close_check_snapshot"] = {
                    "last_checked_ts": int(time.time()),
                    "last_price_ts": 0,
                }
            logger.info("[close_detector] checked=0 skipped=1 reasons={'skip_no_active_signals': 1} triggered=0")
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

        cycle_stats = await evaluate_open_signals(
            chunk,
            start_ts=start,
            budget_sec=BUDGET,
        )
        current_symbol = (
            MODULES.get("signal_audit").current_symbol if "signal_audit" in MODULES else None
        )
        module_state = MODULES.get("signal_audit")
        if module_state and isinstance(module_state.state, dict):
            module_state.state["close_detector_checked_signals"] = int(cycle_stats.get("checked_signals", 0) or 0)
            module_state.state["close_detector_skipped_signals_total"] = int(cycle_stats.get("skipped_total", 0) or 0)
            module_state.state["close_detector_skipped_reasons"] = dict(cycle_stats.get("skip_reasons", {}) or {})
            module_state.state["close_detector_triggered_total"] = int(cycle_stats.get("triggered_total", 0) or 0)
            module_state.state["last_close_check_snapshot"] = {
                "last_checked_ts": int(cycle_stats.get("last_checked_ts", int(time.time())) or int(time.time())),
                "last_price_ts": int(cycle_stats.get("last_price_ts", 0) or 0),
            }

        logger.info(
            "[close_detector] checked=%s skipped=%s reasons=%s triggered=%s",
            int(cycle_stats.get("checked_signals", 0) or 0),
            int(cycle_stats.get("skipped_total", 0) or 0),
            dict(cycle_stats.get("skip_reasons", {}) or {}),
            int(cycle_stats.get("triggered_total", 0) or 0),
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
