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
from db import (
    list_signal_events_by_identity,
    mark_signal_events_be_finalised,
    update_signal_events_be_tracking,
    update_signal_events_status,
)
from signal_audit_db import (
    claim_signal_notification,
    fetch_open_signals,
    get_signal_audit_by_identity,
    mark_be_finalised,
    mark_signal_closed,
    mark_signal_state,
    mark_signal_tp1_hit,
    update_signal_be_tracking,
)
from settings import SIGNAL_TTL_SECONDS
from utils.safe_math import EPS, safe_div

_signal_result_notifier = None
_signal_activation_notifier = None
_signal_poi_touched_notifier = None
_signal_progress_notifier = None
_signal_finalizer_notifier = None

logger = logging.getLogger(__name__)

BE_LEVELS = [8.0, 10.0, 12.0]
BE_TRIGGER_PCT = BE_LEVELS[0]
DEFAULT_LEVERAGE = 10.0


def be_level_label(max_profit_pct: float) -> float:
    level = 0.0
    for value in BE_LEVELS:
        if float(max_profit_pct) >= value:
            level = value
    return level


def set_signal_result_notifier(handler) -> None:
    global _signal_result_notifier
    _signal_result_notifier = handler


def set_signal_activation_notifier(handler) -> None:
    global _signal_activation_notifier
    _signal_activation_notifier = handler


def set_signal_poi_touched_notifier(handler) -> None:
    global _signal_poi_touched_notifier
    _signal_poi_touched_notifier = handler


def set_signal_progress_notifier(handler) -> None:
    global _signal_progress_notifier
    _signal_progress_notifier = handler


def set_signal_finalizer_notifier(handler) -> None:
    global _signal_finalizer_notifier
    _signal_finalizer_notifier = handler


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


def _find_poi_touch_ts(signal: Dict[str, Any], candles: list[Dict[str, float]]) -> int | None:
    if not candles:
        return None
    poi_from = min(float(signal["entry_from"]), float(signal["entry_to"]))
    poi_to = max(float(signal["entry_from"]), float(signal["entry_to"]))
    for candle in candles:
        if _entry_filled(candle, poi_from, poi_to):
            return int(candle["open_time"] / 1000)
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


def _current_profit_pct(*, direction: str, entry_ref: float, last_price: float, leverage: float) -> float:
    if entry_ref <= EPS or leverage <= EPS:
        return 0.0
    if direction == "long":
        move_pct = safe_div(last_price - entry_ref, entry_ref, 0.0) * 100.0
    else:
        move_pct = safe_div(entry_ref - last_price, entry_ref, 0.0) * 100.0
    return move_pct * leverage


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
    state = str(signal.get("state") or "WAITING_ENTRY").upper()
    is_activated = bool(signal.get("is_activated")) or activated_at is not None or state == "ACTIVE_CONFIRMED"
    if activated_at is None:
        age_sec = int(time.time()) - sent_at
        if age_sec >= ttl_sec and not is_activated:
            return {
                "outcome": "NO_FILL",
                "pnl_r": None,
                "filled_at": None,
                "notes": "closed_without_entry",
            }
        return None

    filled_at: int | None = int(activated_at)
    tp1_hit = bool(signal.get("tp1_hit"))
    max_profit_pct = float(signal.get("max_profit_pct") or 0.0)
    be_triggered = bool(signal.get("be_triggered"))
    be_trigger_price = signal.get("be_trigger_price")
    leverage = DEFAULT_LEVERAGE

    for candle in candles:
        candle_ts = int(candle["open_time"] / 1000)
        if candle_ts < int(activated_at):
            continue

        sl_hit, tp1_hit_candle, tp2_hit_candle, be_hit = _check_hits(
            candle, direction, sl, tp1, tp2, entry_ref
        )
        current_profit_pct = _current_profit_pct(
            direction=direction,
            entry_ref=entry_ref,
            last_price=float(candle["close"]),
            leverage=leverage,
        )
        if current_profit_pct > max_profit_pct:
            max_profit_pct = current_profit_pct
        be_level_pct = be_level_label(max_profit_pct)

        be_trigger_event = False
        if current_profit_pct >= BE_TRIGGER_PCT and not tp1_hit and not be_triggered:
            be_triggered = True
            be_trigger_price = float(candle["close"])
            be_trigger_event = True

        if sl_hit and (tp1_hit_candle or tp2_hit_candle):
            return {
                "outcome": "AMBIGUOUS",
                "pnl_r": None,
                "filled_at": filled_at,
                "notes": "SL and TP touched in same candle",
                "max_profit_pct": max_profit_pct,
                "be_triggered": be_triggered,
                "be_trigger_price": be_trigger_price,
                "be_trigger_event": be_trigger_event,
                "be_level_pct": be_level_pct,
            }

        if tp2_hit_candle:
            return {
                "outcome": "TP2",
                "pnl_r": 0.5 * tp1_r + 0.5 * tp2_r,
                "filled_at": filled_at,
                "notes": None,
                "max_profit_pct": max_profit_pct,
                "be_triggered": be_triggered,
                "be_trigger_price": be_trigger_price,
                "be_trigger_event": be_trigger_event,
                "be_level_pct": be_level_pct,
            }

        if tp1_hit_candle and not tp1_hit:
            tp1_hit = True
            return {
                "outcome": "TP1",
                "pnl_r": None,
                "filled_at": filled_at,
                "notes": "tp1_hit",
                "progress_event": "TP1",
                "event_at": candle_ts,
                "continue_open": True,
                "max_profit_pct": max_profit_pct,
                "be_triggered": be_triggered,
                "be_trigger_price": be_trigger_price,
                "be_trigger_event": be_trigger_event,
                "be_level_pct": be_level_pct,
            }

        if sl_hit and tp1_hit:
            return {
                "outcome": "TP1",
                "pnl_r": tp1_r,
                "filled_at": filled_at,
                "notes": "tp_zone_reached_before_sl",
                "max_profit_pct": max_profit_pct,
                "be_triggered": be_triggered,
                "be_trigger_price": be_trigger_price,
                "be_trigger_event": be_trigger_event,
                "be_level_pct": be_level_pct,
            }

        if be_triggered and not tp1_hit and sl_hit:
            return {
                "outcome": "BE",
                "pnl_r": 0.0,
                "filled_at": filled_at,
                "notes": "be_finalised_sl",
                "max_profit_pct": max_profit_pct,
                "be_triggered": be_triggered,
                "be_trigger_price": be_trigger_price,
                "be_trigger_event": be_trigger_event,
                "be_level_pct": be_level_pct,
            }

        if not tp1_hit and sl_hit:
            return {
                "outcome": "SL",
                "pnl_r": -1.0,
                "filled_at": filled_at,
                "notes": None,
                "max_profit_pct": max_profit_pct,
                "be_triggered": be_triggered,
                "be_trigger_price": be_trigger_price,
                "be_trigger_event": be_trigger_event,
                "be_level_pct": be_level_pct,
            }

    # TTL applies only while waiting for confirmation/activation.
    # Once active, the position must be tracked until TP/SL/BE and can never become EXPIRED.
    return None


async def evaluate_open_signals(
    open_signals: Optional[list[dict]] = None,
    *,
    start_ts: float | None = None,
    budget_sec: int = 45,
) -> None:
    if open_signals is None:
        open_signals = fetch_open_signals()
    if not open_signals:
        return

    for signal in open_signals:
        if start_ts is not None and time.time() - start_ts > budget_sec:
            break
        try:
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
                continue

            candles = []
            for item in data:
                parsed = _parse_kline(item)
                if parsed and parsed["open_time"] >= sent_at * 1000:
                    candles.append(parsed)

            if not candles:
                continue

            state = str(signal.get("state") or "WAITING_ENTRY").upper()
            is_activated = bool(signal.get("is_activated")) or signal.get("activated_at") is not None
            ttl_minutes = int(signal.get("ttl_minutes") or SIGNAL_TTL_SECONDS // 60)
            ttl_sec = max(60, ttl_minutes * 60)
            now_ts = int(time.time())
            waiting_states = {"WAIT_CONFIRM", "WAITING_ENTRY", "POI_TOUCHED"}
            entry_price = float(signal.get("entry_price") or 0.0)
            if (
                now_ts > sent_at + ttl_sec
                and state in waiting_states
                and not is_activated
                and entry_price <= 0.0
            ):
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
                touched_at = _find_poi_touch_ts(signal, candles)
                if touched_at is not None:
                    marked = mark_signal_state(
                        str(signal["signal_id"]),
                        from_states=("WAITING_ENTRY",),
                        to_state="POI_TOUCHED",
                        poi_touched_at=int(touched_at),
                    )
                    if marked > 0:
                        signal["state"] = "POI_TOUCHED"
                        signal["poi_touched_at"] = int(touched_at)
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
                continue

            update_signal_be_tracking(
                str(signal["signal_id"]),
                max_profit_pct=float(result.get("max_profit_pct") or 0.0),
                be_level_pct=float(result.get("be_level_pct") or 0.0),
                be_triggered=bool(result.get("be_triggered")),
                be_trigger_price=(
                    float(result["be_trigger_price"])
                    if result.get("be_trigger_price") is not None
                    else None
                ),
            )
            update_signal_events_be_tracking(
                module=str(signal.get("module", "")),
                symbol=str(signal.get("symbol", "")),
                ts=int(signal.get("sent_at", 0)),
                max_profit_pct=float(result.get("max_profit_pct") or 0.0),
                be_level_pct=float(result.get("be_level_pct") or 0.0),
                be_triggered=bool(result.get("be_triggered")),
                be_trigger_price=(
                    float(result["be_trigger_price"])
                    if result.get("be_trigger_price") is not None
                    else None
                ),
            )

            if bool(result.get("be_trigger_event")) and claim_signal_notification(
                str(signal["signal_id"]), event_type="BE_ACTIVATED"
            ):
                logger.info("[admin] BE_TRIGGERED signal_id=%s", signal.get("signal_id"))
                if _signal_progress_notifier is not None:
                    progress_signal = dict(signal)
                    progress_signal["be_trigger_price"] = result.get("be_trigger_price")
                    progress_signal["max_profit_pct"] = result.get("max_profit_pct")
                    progress_signal["be_level_pct"] = float(result.get("be_level_pct") or 0.0)
                    await _signal_progress_notifier(progress_signal, "BE_ACTIVATED")

            progress_event = str(result.get("progress_event") or "").upper()
            if progress_event == "TP1":
                event_at = int(result.get("event_at") or time.time())
                marked_tp1 = mark_signal_tp1_hit(str(signal["signal_id"]), tp1_hit_at=event_at)
                if marked_tp1 > 0:
                    module = str(signal.get("module", ""))
                    symbol = str(signal.get("symbol", ""))
                    ts_value = int(signal.get("sent_at", 0))
                    update_signal_events_status(
                        module=module,
                        symbol=symbol,
                        ts=ts_value,
                        status="TP1",
                    )
                continue

            logger.info(
                "[close_notify] close detected (audit) signal_id=%s symbol=%s side=%s reason=%s",
                signal.get("signal_id"),
                signal.get("symbol"),
                str(signal.get("direction", "")).upper(),
                result.get("outcome"),
            )
            close_state_map = {
                "TP1": "CLOSED_TP1",
                "TP2": "CLOSED_TP2",
                "SL": "CLOSED_SL",
                "BE": "CLOSED_BE",
                "NO_FILL": "EXPIRED",
            }
            mark_signal_closed(
                signal_id=signal["signal_id"],
                outcome=result["outcome"],
                pnl_r=result["pnl_r"],
                filled_at=result["filled_at"],
                notes=result["notes"],
                close_state=close_state_map.get(str(result["outcome"])),
            )
            if _signal_finalizer_notifier is not None:
                await _signal_finalizer_notifier(signal, result)
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
                if status_value == "BE":
                    mark_be_finalised(str(signal["signal_id"]))
                    mark_signal_events_be_finalised(module=module, symbol=symbol, ts=ts_value)
                    logger.info("[admin] BE_FINALISED signal_id=%s", signal.get("signal_id"))
                claimed = claim_signal_notification(str(signal["signal_id"]), event_type="FINAL")
                if not claimed:
                    continue
                update_signal_events_status(
                    module=module,
                    symbol=symbol,
                    ts=ts_value,
                    status=status_value,
                )
                if _signal_result_notifier is not None:
                    events = list_signal_events_by_identity(module=module, symbol=symbol, ts=ts_value)
                    audit_row = get_signal_audit_by_identity(module=module, symbol=symbol, sent_at=ts_value)
                    notify_payloads: list[dict[str, Any]] = []
                    for event in events:
                        payload = dict(event)
                        if audit_row is not None:
                            payload["max_profit_pct"] = audit_row.get("max_profit_pct")
                            payload["be_trigger_price"] = audit_row.get("be_trigger_price")
                        notify_payloads.append(payload)
                    notify_tasks = [_signal_result_notifier(payload) for payload in notify_payloads]
                    if notify_tasks:
                        logger.info(
                            "[close_notify] notify dispatch signal_id=%s events=%s outcome=%s",
                            signal.get("signal_id"),
                            len(notify_tasks),
                            result.get("outcome"),
                        )
                        await asyncio.gather(*notify_tasks)
        except Exception as exc:
            print(f"[signal_audit] Failed to evaluate signal {signal.get('signal_id')}: {exc}")
            continue


async def signal_audit_worker_loop() -> None:
    async def _scan_once() -> None:
        start = time.time()
        BUDGET = 45
        mark_tick("signal_audit", extra="audit cycle")
        open_signals = fetch_open_signals()
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
