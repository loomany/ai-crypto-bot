import asyncio
import math
import os
import time
from statistics import mean
from typing import Any, Dict, List, Optional, Sequence, Tuple

from binance_client import Candle
from market_access import get_bundle_with_fallback, get_quick_with_fallback
from binance_rest import is_binance_degraded
from symbol_cache import get_spot_usdt_symbols, get_top_usdt_symbols_by_volume
from ai_patterns import analyze_ai_patterns
from market_context import get_market_context
from market_regime import get_market_regime
from indicators_cache import get_cached_atr, get_cached_ema, get_cached_rsi
from trading_core import (
    _compute_rsi_series,
    _nearest_level,
    analyze_orderflow,
    compute_score_breakdown,
    detect_rsi_divergence,
    detect_trend_and_structure,
    find_key_levels,
    is_bb_extreme_reversal,
    is_liquidity_sweep,
    is_volume_climax,
)

BTC_SYMBOL = "BTCUSDT"
LEVEL_NEAR_PCT_FREE = 1.2
MIN_VOLUME_RATIO_FREE = 1.15
MIN_RR_FREE = 1.8
EMA50_NEAR_PCT_FREE = 1.0
EMA50_GATE_SCORE = int(os.getenv("EMA50_GATE_SCORE", "78"))
EMA50_NEAR_PCT_STRICT = float(os.getenv("EMA50_NEAR_PCT_STRICT", "1.0"))
EMA50_NEAR_PCT_WEAK = float(os.getenv("EMA50_NEAR_PCT_WEAK", "1.5"))
EMA50_NEAR_PCT_MID = float(os.getenv("EMA50_NEAR_PCT_MID", "2.5"))
EMA50_SCORE_WEAK = int(os.getenv("EMA50_SCORE_WEAK", "72"))
EMA50_SCORE_MID = int(os.getenv("EMA50_SCORE_MID", "78"))
MIN_PRE_SCORE = float(os.getenv("MIN_PRE_SCORE", "70"))
PRE_SCORE_THRESHOLD = float(os.getenv("PRE_SCORE_THRESHOLD", "65"))
FINAL_SCORE_THRESHOLD = float(os.getenv("FINAL_SCORE_THRESHOLD", "80"))
AI_MAX_DEEP_PER_CYCLE = int(os.getenv("AI_MAX_DEEP_PER_CYCLE", "3"))
AI_DEEP_TOP_K = int(os.getenv("AI_DEEP_TOP_K", str(AI_MAX_DEEP_PER_CYCLE)))
AI_CHEAP_LIMIT = int(os.getenv("AI_CHEAP_LIMIT", "120"))
AI_CHEAP_TF = os.getenv("AI_CHEAP_TF", "15m")
AGGTRADES_TOP_K = int(os.getenv("AGGTRADES_TOP_K", "2"))
KLINES_CONCURRENCY = int(os.getenv("KLINES_CONCURRENCY", "10"))
REQUIRE_15M_CONFIRM_ON_EXPANDED = os.getenv("REQUIRE_15M_CONFIRM_ON_EXPANDED", "true").lower() in (
    "1",
    "true",
    "yes",
    "y",
)
MAX_FAIL_DEBUG_LOGS_PER_CYCLE = int(os.getenv("MAX_FAIL_DEBUG_LOGS_PER_CYCLE", "8"))
AI_STAGE_A_TOP_K = int(os.getenv("AI_STAGE_A_TOP_K", "10"))
BTC_CONTEXT_TTL_SEC = int(os.getenv("BTC_CONTEXT_TTL_SEC", "90"))

_BTC_CONTEXT_CACHE: Dict[str, Any] | None = None
_BTC_CONTEXT_TS: float = 0.0
_BTC_CONTEXT_LAST_REFRESH_TS: float = 0.0
_BTC_CONTEXT_LAST_ERROR: str | None = None


def get_cached_btc_context() -> tuple[Dict[str, Any], int, int] | None:
    if _BTC_CONTEXT_CACHE is None:
        return None
    age_sec = int(max(0, time.time() - _BTC_CONTEXT_TS))
    return dict(_BTC_CONTEXT_CACHE), age_sec, BTC_CONTEXT_TTL_SEC


def get_btc_context_last_error() -> str | None:
    return _BTC_CONTEXT_LAST_ERROR


def get_btc_context_last_refresh_ts() -> float:
    return _BTC_CONTEXT_LAST_REFRESH_TS


async def get_btc_context(*, force_refresh: bool = False) -> Dict[str, Any]:
    """
    Анализирует BTCUSDT и возвращает контекст рынка:
      - тренды 1d / 1h
      - RSI 15m
      - отношение объёма последней 15m свечи к среднему за N свечей
      - флаги allow_longs / allow_shorts
    """

    global _BTC_CONTEXT_CACHE, _BTC_CONTEXT_TS, _BTC_CONTEXT_LAST_ERROR, _BTC_CONTEXT_LAST_REFRESH_TS

    now = time.time()
    if not force_refresh and _BTC_CONTEXT_CACHE and (now - _BTC_CONTEXT_TS) < BTC_CONTEXT_TTL_SEC:
        return dict(_BTC_CONTEXT_CACHE)

    try:
        candles = await get_bundle_with_fallback(BTC_SYMBOL, ("1d", "4h", "1h", "15m"))
    except Exception as exc:
        _BTC_CONTEXT_LAST_ERROR = str(exc)
        raise
    candles_1d = candles.get("1d", []) if candles else []
    candles_1h = candles.get("4h", []) or candles.get("1h", []) if candles else []
    candles_15m = candles.get("15m", []) if candles else []

    if not candles_1d or not candles_1h or not candles_15m:
        fallback = {
            "trend_1d": "range",
            "trend_1h": "range",
            "rsi_15m": 50.0,
            "volume_ratio_15m": 0.0,
            "allow_longs": True,
            "allow_shorts": True,
            "ctx_reason": "fallback_neutral",
        }
        _BTC_CONTEXT_CACHE = fallback
        _BTC_CONTEXT_TS = now
        _BTC_CONTEXT_LAST_REFRESH_TS = now
        _BTC_CONTEXT_LAST_ERROR = None
        return dict(fallback)

    daily_structure = detect_trend_and_structure(candles_1d)
    h1_structure = detect_trend_and_structure(candles_1h)

    btc_trend_1d = daily_structure["trend"]
    btc_trend_1h = h1_structure["trend"]

    rsi_15m_value = get_cached_rsi(BTC_SYMBOL, "15m", candles_15m)

    vols = [c.volume for c in candles_15m[-21:-1]]
    last_vol = candles_15m[-1].volume if candles_15m else 0.0
    avg_vol = mean(vols) if vols else 0.0
    volume_ratio_15m = last_vol / avg_vol if avg_vol > 0 else 0.0

    allow_longs = (
        btc_trend_1d in ("up", "range")
        and btc_trend_1h == "up"
        and 40 <= rsi_15m_value <= 65
        and volume_ratio_15m >= 1.2
    )

    allow_shorts = (
        btc_trend_1d in ("down", "range")
        and btc_trend_1h == "down"
        and 35 <= rsi_15m_value <= 70
        and volume_ratio_15m >= 1.2
    )

    ctx_reason = None
    if not allow_longs and not allow_shorts:
        allow_longs = True
        allow_shorts = True
        ctx_reason = f"{ctx_reason};fallback_neutral" if ctx_reason else "fallback_neutral"

    context = {
        "trend_1d": btc_trend_1d,
        "trend_1h": btc_trend_1h,
        "rsi_15m": rsi_15m_value,
        "volume_ratio_15m": volume_ratio_15m,
        "allow_longs": allow_longs,
        "allow_shorts": allow_shorts,
    }
    if ctx_reason:
        context["ctx_reason"] = ctx_reason
    _BTC_CONTEXT_CACHE = context
    _BTC_CONTEXT_TS = now
    _BTC_CONTEXT_LAST_REFRESH_TS = now
    _BTC_CONTEXT_LAST_ERROR = None
    return dict(context)


def _volume_ratio(volumes: Sequence[float]) -> Tuple[float, float]:
    if not volumes:
        return 0.0, 0.0
    avg = mean(volumes[:-1]) if len(volumes) > 1 else volumes[0]
    avg = avg if avg > 0 else 0.0
    last = volumes[-1]
    ratio = last / avg if avg else 0.0
    return ratio, avg


async def _gather_klines(symbol: str) -> Optional[Dict[str, List[Candle]]]:
    return await get_bundle_with_fallback(symbol, ("1d", "4h", "1h", "15m", "5m"))


async def _gather_stage_a_klines(
    symbol: str,
    *,
    tf: str,
    limit: int,
) -> Optional[Dict[str, List[Candle]]]:
    return await get_quick_with_fallback(symbol, tfs=(tf,), limit_overrides={tf: limit})


def _pre_score(candles: Dict[str, List[Candle]], *, tf: str, symbol: str) -> float:
    source = candles.get(tf) or []
    if len(source) < 20:
        return 0.0
    closes = [c.close for c in source]
    trend = detect_trend_and_structure(source)
    volume_ratio, _ = _volume_ratio([c.volume for c in source])
    ema50 = get_cached_ema(symbol, tf, source, 50)
    last_close = closes[-1]
    atr = get_cached_atr(symbol, tf, source, 14)
    rsi_value = get_cached_rsi(symbol, tf, source)

    score = 0.0
    if trend.get("trend") in ("up", "down"):
        score += 25
    else:
        score += 10

    if volume_ratio >= 1.5:
        score += 30
    elif volume_ratio >= 1.2:
        score += 20
    elif volume_ratio >= 1.05:
        score += 10

    if ema50 and last_close > 0:
        dist_pct = abs(last_close - ema50) / last_close * 100
        if dist_pct <= 1.2:
            score += 20
        elif dist_pct <= 2.5:
            score += 10

    if atr and atr > 0:
        score += 10

    if 35 <= rsi_value <= 65:
        score += 10

    return min(score, 100.0)


async def _get_hourly_snapshot(symbol: str) -> Optional[Dict[str, float]]:
    bundle = await get_bundle_with_fallback(symbol, ("1h",))
    if not bundle:
        return None
    candles_1h = bundle.get("1h") or []
    if len(candles_1h) < 1:
        return None

    kline = candles_1h[-1]
    open_price = float(kline.open)
    close_price = float(kline.close)
    quote_volume = float(kline.volume) * close_price

    if open_price <= 0:
        return None

    change_pct = (close_price - open_price) / open_price * 100
    return {"change_pct": change_pct, "volume_usdt": max(quote_volume, 0.0)}


async def get_alt_watch_symbol(limit: int = 80, batch_size: int = 10) -> Optional[Dict[str, float]]:
    if is_binance_degraded():
        limit = min(limit, 30)
    symbols = await get_top_usdt_symbols_by_volume(limit)
    if not symbols:
        return None

    best_symbol: Optional[str] = None
    best_score = -1.0
    best_change = 0.0
    best_volume = 0.0

    for i in range(0, len(symbols), batch_size):
        batch = symbols[i : i + batch_size]
        tasks = [asyncio.create_task(_get_hourly_snapshot(symbol)) for symbol in batch]
        snapshots = await asyncio.gather(*tasks)

        for symbol, snapshot in zip(batch, snapshots):
            if not snapshot:
                continue
            if symbol == BTC_SYMBOL:
                continue
            change_pct = snapshot["change_pct"]
            volume_usdt = snapshot["volume_usdt"]
            score = abs(change_pct) * math.log(volume_usdt + 1.0)
            if score > best_score:
                best_score = score
                best_symbol = symbol
                best_change = change_pct
                best_volume = volume_usdt

    if not best_symbol:
        return None

    return {
        "symbol": best_symbol,
        "change_pct": best_change,
        "volume_usdt": best_volume,
    }


async def _prepare_signal(
    symbol: str,
    candles: Dict[str, List[Candle]],
    btc_ctx: Dict[str, Any],
    *,
    free_mode: bool = False,
    min_score: float = 80,
    stats: Dict[str, int] | None = None,
    near_miss: Dict[str, int] | None = None,
    debug_state: Dict[str, int] | None = None,
    fetch_orderflow: bool = True,
) -> Optional[Dict[str, Any]]:
    def _inc_fail(reason: str) -> None:
        if stats is None:
            return
        stats[reason] = stats.get(reason, 0) + 1

    def _inc_near_miss(reason: str) -> None:
        if near_miss is None:
            return
        near_miss[reason] = near_miss.get(reason, 0) + 1

    def detect_side(entry: float, sl: float, tp1: float) -> Optional[str]:
        if tp1 > entry and sl < entry:
            return "LONG"
        if tp1 < entry and sl > entry:
            return "SHORT"
        return None

    def _log_fail_debug(
        reason: str,
        *,
        score_pre: float,
        dist_to_ema50_pct: float,
        allowed_dist_pct: float,
        strict_pct: float,
        ema50_value: float,
        last_price: float,
        scenario: Optional[str],
    ) -> None:
        if debug_state is None:
            return
        if debug_state["used"] >= debug_state["max"]:
            return
        debug_state["used"] += 1
        print(
            "[ai_signals] debug_fail "
            f"{reason} {symbol} score_pre={score_pre:.2f} "
            f"dist_to_ema50_pct={dist_to_ema50_pct:.2f} allowed_dist_pct={allowed_dist_pct:.2f} "
            f"strict_pct={strict_pct:.2f} ema50={ema50_value:.6f} last_price={last_price:.6f} "
            f"side={scenario}"
        )

    candles_1d = candles["1d"]
    candles_4h = candles["4h"]
    candles_1h = candles["1h"]
    candles_15m = candles["15m"]
    candles_5m = candles["5m"]

    current_price = candles_5m[-1].close

    # Отсекаем сверхдешёвые монеты
    if current_price < 0.00001:
        _inc_fail("fail_price_too_low")
        return None

    daily_structure = detect_trend_and_structure(candles_1d)
    h4_structure = detect_trend_and_structure(candles_4h)
    h1_structure = detect_trend_and_structure(candles_1h)

    global_trend = daily_structure["trend"] if daily_structure["trend"] != "range" else h4_structure["trend"]
    local_trend = h1_structure["trend"]

    key_levels = find_key_levels(candles_1d)

    recent_h1 = candles_1h[-24:]
    recent_15m = candles_15m[-32:]
    key_levels["highs"].extend([c.high for c in recent_h1])
    key_levels["lows"].extend([c.low for c in recent_h1])
    key_levels["highs"].extend([c.high for c in recent_15m])
    key_levels["lows"].extend([c.low for c in recent_15m])
    key_levels["highs"] = sorted(set(key_levels["highs"]))
    key_levels["lows"] = sorted(set(key_levels["lows"]))

    nearest_high, dist_high = _nearest_level(current_price, key_levels["highs"])
    nearest_low, dist_low = _nearest_level(current_price, key_levels["lows"])

    candidate_side: Optional[str] = None
    level_touched: Optional[float] = None

    level_near_pct = LEVEL_NEAR_PCT_FREE if free_mode else 0.6
    if dist_low is not None and dist_low <= level_near_pct:
        candidate_side = "LONG"
        level_touched = nearest_low

    if dist_high is not None and dist_high <= level_near_pct:
        if candidate_side is None or (dist_high is not None and dist_high < (dist_low or 100)):
            candidate_side = "SHORT"
            level_touched = nearest_high

    if not candidate_side:
        _inc_fail("fail_not_near_level")
        return None

    sweep = is_liquidity_sweep(
        candles_5m[-6:] if len(candles_5m) >= 6 else candles_5m,
        level_touched,
        "long" if candidate_side == "LONG" else "short",
    )
    volume_spike = is_volume_climax(candles_5m)

    closes_15m = [c.close for c in candles_15m]
    closes_5m = [c.close for c in candles_5m]
    rsi_15m = _compute_rsi_series(closes_15m)
    rsi_5m = _compute_rsi_series(closes_5m)
    rsi_div = False
    if candidate_side == "LONG":
        rsi_div = detect_rsi_divergence(closes_15m, rsi_15m, "bullish") or detect_rsi_divergence(
            closes_5m, rsi_5m, "bullish"
        )
    else:
        rsi_div = detect_rsi_divergence(closes_15m, rsi_15m, "bearish") or detect_rsi_divergence(
            closes_5m, rsi_5m, "bearish"
        )

    if candidate_side == "LONG" and not btc_ctx.get("allow_longs", False):
        _inc_fail("fail_btc_gate_long")
        return None

    if candidate_side == "SHORT" and not btc_ctx.get("allow_shorts", False):
        _inc_fail("fail_btc_gate_short")
        return None

    atr_15m = (
        get_cached_atr(symbol, "15m", candles_15m[-60:], 14) if len(candles_15m) >= 15 else None
    )
    stop_buffer = atr_15m * 0.8 if atr_15m else current_price * 0.003

    if candidate_side == "LONG":
        sl = (level_touched or current_price) - max(stop_buffer, current_price * 0.005)
        entry_from = max((level_touched or current_price) * 0.998, current_price * 0.997)
        entry_to = current_price * 1.001
        risk = entry_to - sl
        tp1 = entry_to + risk * 2
        tp2 = entry_to + risk * 3
    else:
        sl = (level_touched or current_price) + max(stop_buffer, current_price * 0.005)
        entry_to = current_price * 0.999
        entry_from = current_price * 1.001
        risk = sl - entry_to
        tp1 = entry_to - risk * 2
        tp2 = entry_to - risk * 3

    atr_ok = True
    if atr_15m and risk > 0:
        min_stop = atr_15m * 0.5
        max_stop = atr_15m * 2.0
        atr_ok = min_stop <= risk <= max_stop

    bb_extreme_15 = is_bb_extreme_reversal(
        candles_15m[-40:] if len(candles_15m) >= 40 else candles_15m,
        direction="long" if candidate_side == "LONG" else "short",
    )
    bb_extreme_5 = is_bb_extreme_reversal(
        candles_5m[-40:] if len(candles_5m) >= 40 else candles_5m,
        direction="long" if candidate_side == "LONG" else "short",
    )
    bb_extreme = bb_extreme_15 or bb_extreme_5

    level_distance = dist_low if candidate_side == "LONG" else dist_high
    level_distance = level_distance if level_distance is not None else level_near_pct
    level_score = max(0.0, 30.0 * (1.0 - min(level_distance, level_near_pct) / level_near_pct))

    entry_ref = (entry_from + entry_to) / 2
    risk_pre = abs(entry_ref - sl)
    reward_pre = abs(tp1 - entry_ref)
    rr_pre = reward_pre / risk_pre if risk_pre > 0 else 0.0
    min_rr_pre = MIN_RR_FREE if free_mode else 2.0

    def _trend_supports(side_value: str, trend_value: str) -> bool:
        if side_value == "LONG":
            return trend_value in ("up", "bullish")
        if side_value == "SHORT":
            return trend_value in ("down", "bearish")
        return False

    trend_support = _trend_supports(candidate_side, local_trend) or _trend_supports(candidate_side, global_trend)
    score_pre = (
        level_score
        + (12.0 if sweep else 0.0)
        + (12.0 if volume_spike else 0.0)
        + (10.0 if rsi_div else 0.0)
        + (10.0 if atr_ok else 0.0)
        + (10.0 if bb_extreme else 0.0)
        + (8.0 if trend_support else 0.0)
        + (8.0 if rr_pre >= min_rr_pre else 0.0)
    )
    score_pre = min(100.0, max(0.0, score_pre))

    closes_1h = [c.close for c in candles_1h]
    ema50_1h = get_cached_ema(symbol, "1h", candles_1h, 50) if len(closes_1h) >= 50 else None
    ema200_1h = get_cached_ema(symbol, "1h", candles_1h, 200) if len(closes_1h) >= 200 else None
    ma_trend_ok = False
    if ema50_1h and ema200_1h:
        if candidate_side == "LONG" and current_price >= ema50_1h >= ema200_1h:
            ma_trend_ok = True
        if candidate_side == "SHORT" and current_price <= ema50_1h <= ema200_1h:
            ma_trend_ok = True

    if not ma_trend_ok:
        if free_mode and ema50_1h:
            dist_to_ema50_pct = abs(current_price - ema50_1h) / ema50_1h * 100
            allowed_dist_pct = EMA50_NEAR_PCT_WEAK
            if EMA50_SCORE_WEAK <= score_pre < EMA50_SCORE_MID:
                allowed_dist_pct = EMA50_NEAR_PCT_MID

            if score_pre < EMA50_SCORE_MID:
                ema_penalty = 0.0
                if dist_to_ema50_pct > allowed_dist_pct:
                    ema_penalty = min(8.0, dist_to_ema50_pct * 2.0)

                score_pre -= ema_penalty
                if score_pre < MIN_PRE_SCORE:
                    if dist_to_ema50_pct > allowed_dist_pct:
                        _inc_fail("fail_not_near_ema50_weak")
                        if dist_to_ema50_pct <= allowed_dist_pct + 0.5:
                            _inc_near_miss("ema")
                    _inc_fail("fail_pre_score")
                    return None

                is_expanded_pass = (
                    dist_to_ema50_pct > EMA50_NEAR_PCT_STRICT
                    and dist_to_ema50_pct <= allowed_dist_pct
                )
                if REQUIRE_15M_CONFIRM_ON_EXPANDED and is_expanded_pass:
                    confirm_candles = candles_15m
                    if len(confirm_candles) < 3:
                        confirmed = False
                    else:
                        last_closed = confirm_candles[-2]  # последняя закрытая
                        prev_closed = confirm_candles[-3]
                        if candidate_side == "SHORT":
                            confirmed = last_closed.close < last_closed.open and last_closed.close < prev_closed.close
                        else:
                            confirmed = last_closed.close > last_closed.open and last_closed.close > prev_closed.close
                    if not confirmed:
                        _inc_fail("fail_no_15m_confirm")
                        _log_fail_debug(
                            "fail_no_15m_confirm",
                            score_pre=score_pre,
                            dist_to_ema50_pct=dist_to_ema50_pct,
                            allowed_dist_pct=allowed_dist_pct,
                            strict_pct=EMA50_NEAR_PCT_STRICT,
                            ema50_value=ema50_1h,
                            last_price=current_price,
                            scenario=candidate_side,
                        )
                        return None
            else:
                _inc_fail("ema50_bypassed_strong")
        else:
            _inc_fail("fail_ma_trend")
            return None

    if not atr_ok:
        _inc_fail("fail_atr_ok")
        return None

    if fetch_orderflow:
        orderflow = await analyze_orderflow(symbol)
    else:
        orderflow = {"orderflow_bullish": False, "orderflow_bearish": False, "whale_activity": False}

    # --- AI-паттерны и Market Regime ---
    pattern_info = await analyze_ai_patterns(symbol, candles_1h, candles_15m, candles_5m)
    market_info = await get_market_regime()

    context = {
        "candidate_side": candidate_side,
        "global_trend": global_trend,
        "local_trend": local_trend,
        "near_key_level": True,
        "liquidity_sweep": sweep,
        "volume_climax": volume_spike,
        "rsi_divergence": rsi_div,
        "atr_ok": atr_ok,
        "bb_extreme": bb_extreme,
        "ma_trend_ok": ma_trend_ok,
        "orderflow_bullish": orderflow.get("orderflow_bullish", False),
        "orderflow_bearish": orderflow.get("orderflow_bearish", False),
        "whale_activity": orderflow.get("whale_activity", False),
        "ai_pattern_trend": pattern_info.get("pattern_trend"),
        "ai_pattern_strength": pattern_info.get("pattern_strength", 0),
        "market_regime": market_info.get("regime", "neutral"),
    }

    raw_score, breakdown = compute_score_breakdown(context)

    side = detect_side(entry_ref, sl, tp1)
    if side is None:
        _inc_fail("fail_invalid_side")
        print(f"[ai_signals] Invalid side for {symbol}: entry={entry_ref} sl={sl} tp1={tp1}")
        return None

    # --- Market Context Layer gate (BTC-led) ---
    if side == "LONG" and not btc_ctx.get("allow_longs", True):
        _inc_fail("fail_market_mode_long_blocked")
        return None
    if side == "SHORT" and not btc_ctx.get("allow_shorts", True):
        _inc_fail("fail_market_mode_short_blocked")
        return None

    required_min = float(btc_ctx.get("min_score_long", min_score)) if side == "LONG" else float(
        btc_ctx.get("min_score_short", min_score)
    )
    if abs(raw_score) < required_min:
        delta = required_min - abs(raw_score)
        if 0 < delta <= 5:
            _inc_near_miss("score")
        _inc_fail("fail_score")
        return None

    risk = abs(entry_ref - sl)
    reward = abs(tp1 - entry_ref)
    rr = reward / risk if risk > 0 else 0.0

    min_rr = MIN_RR_FREE if free_mode else 2.0
    if rr < min_rr:
        _inc_fail("fail_rr")
        return None

    if side not in ("LONG", "SHORT") or rr < 1.5 or risk <= 0 or reward <= 0:
        _inc_fail("fail_rr")
        print(
            "[ai_signals] Pre-send check failed "
            f"{symbol}: side={side} rr={rr:.2f} risk={risk:.6f} reward={reward:.6f}"
        )
        return None

    rsi_1h_value = get_cached_rsi(symbol, "1h", candles_1h)
    rsi_zone = "комфортная зона"
    if rsi_1h_value >= 70:
        rsi_zone = "перекупленность"
    elif rsi_1h_value <= 30:
        rsi_zone = "перепроданность"

    volume_ratio, volume_avg = _volume_ratio([c.volume for c in candles_1h])

    # Требуем хотя бы 30% всплеска объёма
    min_volume_ratio = MIN_VOLUME_RATIO_FREE if free_mode else 1.3
    if volume_ratio < min_volume_ratio:
        _inc_fail("fail_volume_ratio")
        return None

    support_level = nearest_low if nearest_low is not None else level_touched
    resistance_level = nearest_high if nearest_high is not None else level_touched

    return {
        "symbol": symbol,
        "direction": "long" if side == "LONG" else "short",
        "entry_zone": (round(entry_from, 4), round(entry_to, 4)),
        "sl": round(sl, 4),
        "tp1": round(tp1, 4),
        "tp2": round(tp2, 4),
        "score": min(100, abs(raw_score)),
        "score_raw": int(round(raw_score)),
        "reason": {
            "trend_1d": global_trend,
            "trend_4h": h4_structure["trend"],
            "rsi_1h": rsi_1h_value,
            "rsi_1h_zone": rsi_zone,
            "volume_ratio": volume_ratio,
            "volume_avg": volume_avg,
            "rr": rr,
            "market_mode": btc_ctx.get("mode", "NORMAL"),
            "market_bias": btc_ctx.get("bias", "NEUTRAL"),
            "btc_change_6h_pct": btc_ctx.get("change_6h_pct", 0.0),
            "btc_atr_1h_pct": btc_ctx.get("atr_1h_pct", 0.0),
        },
        "breakdown": breakdown,
        "score_breakdown": breakdown,
        "levels": {
            "support": round(support_level or 0.0, 4),
            "resistance": round(resistance_level or 0.0, 4),
            "atr_30": round(atr_15m or 0.0, 4),
        },
    }


async def scan_market(
    batch_size: Optional[int] = None,
    *,
    symbols: List[str] | None = None,
    use_btc_gate: bool = True,
    free_mode: bool = False,
    min_score: float | None = None,
    return_stats: bool = False,
    time_budget: float | None = None,
    deep_scan_limit: int | None = None,
    priority_scores: Dict[str, float] | None = None,
) -> List[Dict[str, Any]] | Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    """
    Сканирует весь рынок Binance по спотовым USDT-парам и возвращает сигналы.
    """
    if batch_size is None:
        batch_size = int(os.getenv("AI_SCAN_BATCH_SIZE", "8"))
    if min_score is None:
        min_score = FINAL_SCORE_THRESHOLD

    if symbols is None:
        symbols = await get_spot_usdt_symbols()

    btc_ctx = await get_btc_context() if use_btc_gate else {
        "allow_longs": True,
        "allow_shorts": True,
    }
    market_ctx = await get_market_context() if use_btc_gate else {
        "mode": "NORMAL",
        "bias": "NEUTRAL",
        "allow_longs": True,
        "allow_shorts": True,
        "min_score_long": float(min_score),
        "min_score_short": float(min_score),
    }
    btc_ctx = {**btc_ctx, **market_ctx}
    checked = 0
    klines_ok = 0
    fails: Dict[str, int] = {}
    near_miss: Dict[str, int] = {}
    pre_score_stats: Dict[str, Any] = {
        "checked": 0,
        "passed": 0,
        "failed": 0,
        "threshold": float(PRE_SCORE_THRESHOLD),
        "failed_samples": [],
        "passed_samples": [],
        "pass_rate": 0.0,
    }
    start_time = time.time()
    debug_state = {"used": 0, "max": MAX_FAIL_DEBUG_LOGS_PER_CYCLE}
    deep_scans_done = 0
    max_deep_scans = AI_DEEP_TOP_K if deep_scan_limit is None else deep_scan_limit
    semaphore = asyncio.Semaphore(KLINES_CONCURRENCY)

    async def _with_semaphore(awaitable, *args, **kwargs):
        async with semaphore:
            return await awaitable(*args, **kwargs)

    def _add_pre_score_sample(samples: list[tuple[str, float]], symbol: str, score: float) -> None:
        if len(samples) >= 5:
            return
        samples.append((symbol, round(score, 2)))

    if use_btc_gate:
        if not btc_ctx["allow_longs"] and not btc_ctx["allow_shorts"]:
            if return_stats:
                return [], {
                    "checked": checked,
                    "klines_ok": klines_ok,
                    "signals_found": 0,
                    "fails": fails,
                    "near_miss": near_miss,
                    "pre_score": pre_score_stats,
                }
            return []
    else:
        btc_ctx = {**btc_ctx, "allow_longs": True, "allow_shorts": True}

    signals: List[Dict[str, Any]] = []

    scored: List[Tuple[str, float]] = []
    for i in range(0, len(symbols), batch_size):
        if time_budget is not None and time.time() - start_time > time_budget:
            break
        batch = symbols[i : i + batch_size]
        checked += len(batch)
        quick_tasks = [
            asyncio.create_task(
                _with_semaphore(
                    _gather_stage_a_klines,
                    symbol,
                    tf=AI_CHEAP_TF,
                    limit=AI_CHEAP_LIMIT,
                )
            )
            for symbol in batch
        ]
        quick_list = await asyncio.gather(*quick_tasks)
        for symbol, quick in zip(batch, quick_list):
            if not quick:
                fails["fail_no_quick"] = fails.get("fail_no_quick", 0) + 1
                continue
            pre_score = _pre_score(quick, tf=AI_CHEAP_TF, symbol=symbol)
            pre_score_stats["checked"] += 1
            if pre_score < PRE_SCORE_THRESHOLD:
                fails["fail_pre_score"] = fails.get("fail_pre_score", 0) + 1
                pre_score_stats["failed"] += 1
                _add_pre_score_sample(pre_score_stats["failed_samples"], symbol, pre_score)
                continue
            pre_score_stats["passed"] += 1
            _add_pre_score_sample(pre_score_stats["passed_samples"], symbol, pre_score)
            scored.append((symbol, pre_score))

    if not scored:
        pre_score_stats["pass_rate"] = pre_score_stats["passed"] / max(
            1, pre_score_stats["checked"]
        )
        if return_stats:
            return signals, {
                "checked": checked,
                "klines_ok": klines_ok,
                "deep_scans_done": deep_scans_done,
                "signals_found": len(signals),
                "fails": fails,
                "near_miss": near_miss,
                "pre_score": pre_score_stats,
            }
        return signals

    scored.sort(key=lambda item: item[1], reverse=True)
    if free_mode:
        scored = scored[: min(AI_STAGE_A_TOP_K, len(scored))]

    ranked = scored
    if priority_scores:
        ranked = sorted(
            scored,
            key=lambda item: (priority_scores.get(item[0], item[1]), item[1]),
            reverse=True,
        )

    candidate_symbols = [symbol for symbol, _ in ranked[: max_deep_scans]]
    deep_scans_done = len(candidate_symbols)
    if not candidate_symbols:
        pre_score_stats["pass_rate"] = pre_score_stats["passed"] / max(
            1, pre_score_stats["checked"]
        )
        if return_stats:
            return signals, {
                "checked": checked,
                "klines_ok": klines_ok,
                "deep_scans_done": deep_scans_done,
                "signals_found": len(signals),
                "fails": fails,
                "near_miss": near_miss,
                "pre_score": pre_score_stats,
            }
        return signals

    orderflow_candidates = {
        symbol for symbol, _ in ranked[: min(AGGTRADES_TOP_K, len(candidate_symbols))]
    }
    tasks = [asyncio.create_task(_with_semaphore(_gather_klines, symbol)) for symbol in candidate_symbols]
    klines_list = await asyncio.gather(*tasks)

    for symbol, klines in zip(candidate_symbols, klines_list):
        if not klines:
            fails["fail_no_klines"] = fails.get("fail_no_klines", 0) + 1
            continue
        klines_ok += 1

        signal = await _prepare_signal(
            symbol,
            klines,
            btc_ctx,
            free_mode=free_mode,
            min_score=min_score,
            stats=fails if return_stats else None,
            near_miss=near_miss if return_stats else None,
            debug_state=debug_state,
            fetch_orderflow=symbol in orderflow_candidates,
        )
        if signal:
            signals.append(signal)

    if return_stats:
        pre_score_stats["pass_rate"] = pre_score_stats["passed"] / max(
            1, pre_score_stats["checked"]
        )
        return signals, {
            "checked": checked,
            "klines_ok": klines_ok,
            "deep_scans_done": deep_scans_done,
            "signals_found": len(signals),
            "fails": fails,
            "near_miss": near_miss,
            "pre_score": pre_score_stats,
        }
    return signals
