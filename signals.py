import asyncio
import math
from statistics import mean
from typing import Any, Dict, List, Optional, Sequence, Tuple

from binance_client import Candle, get_required_candles
from binance_limits import BINANCE_WEIGHT_TRACKER
from binance_rest import fetch_json
from symbol_cache import get_spot_usdt_symbols, get_top_usdt_symbols_by_volume
from ai_patterns import analyze_ai_patterns
from market_regime import get_market_regime
from trading_core import (
    _compute_rsi_series,
    _nearest_level,
    analyze_orderflow,
    compute_atr,
    compute_ema,
    compute_score_breakdown,
    detect_rsi_divergence,
    detect_trend_and_structure,
    find_key_levels,
    is_bb_extreme_reversal,
    is_liquidity_sweep,
    is_volume_climax,
)

BINANCE_BASE_URL = "https://api.binance.com/api/v3"

KLINES_URL = f"{BINANCE_BASE_URL}/klines"
BTC_SYMBOL = "BTCUSDT"
LEVEL_NEAR_PCT_FREE = 1.2
MIN_VOLUME_RATIO_FREE = 1.15
MIN_RR_FREE = 1.8
EMA50_NEAR_PCT_FREE = 1.0


def is_pro_strict_signal(
    signal: Dict[str, Any],
    min_score: float = 88,
    min_rr: float = 2.5,
    min_volume_ratio: float = 1.3,
) -> bool:
    score = float(signal.get("score", 0.0))
    if score < min_score:
        return False

    reason = signal.get("reason", {}) if isinstance(signal.get("reason"), dict) else {}
    rr = float(reason.get("rr", 0.0))
    volume_ratio = float(reason.get("volume_ratio", 0.0))
    trend_1d = reason.get("trend_1d")
    trend_4h = reason.get("trend_4h")
    direction = signal.get("direction", "long")

    if rr < min_rr:
        return False
    if volume_ratio < min_volume_ratio:
        return False

    def _trend_ok(direction_value: str, trend_value: Any) -> bool:
        if direction_value == "long":
            return trend_value in ("up", "bullish")
        if direction_value == "short":
            return trend_value in ("down", "bearish")
        return False

    if not _trend_ok(direction, trend_4h):
        return False
    if trend_1d and trend_1d not in ("range", "neutral") and not _trend_ok(direction, trend_1d):
        return False

    return True


async def get_btc_context() -> Dict[str, Any]:
    """
    Анализирует BTCUSDT и возвращает контекст рынка:
      - тренды 1d / 1h
      - RSI 15m
      - отношение объёма последней 15m свечи к среднему за N свечей
      - флаги allow_longs / allow_shorts
    """

    candles = await get_required_candles(BTC_SYMBOL)
    candles_1d = candles.get("1d", []) if candles else []
    candles_1h = candles.get("4h", []) or candles.get("1h", []) if candles else []
    candles_15m = candles.get("15m", []) if candles else []

    if not candles_1d or not candles_1h or not candles_15m:
        return {
            "trend_1d": "range",
            "trend_1h": "range",
            "rsi_15m": 50.0,
            "volume_ratio_15m": 0.0,
            "allow_longs": False,
            "allow_shorts": False,
        }

    daily_structure = detect_trend_and_structure(candles_1d)
    h1_structure = detect_trend_and_structure(candles_1h)

    btc_trend_1d = daily_structure["trend"]
    btc_trend_1h = h1_structure["trend"]

    closes_15m = [c.close for c in candles_15m]
    rsi_15m_series = _compute_rsi_series(closes_15m)
    rsi_15m_value = rsi_15m_series[-1] if rsi_15m_series else 50.0

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

    return {
        "trend_1d": btc_trend_1d,
        "trend_1h": btc_trend_1h,
        "rsi_15m": rsi_15m_value,
        "volume_ratio_15m": volume_ratio_15m,
        "allow_longs": allow_longs,
        "allow_shorts": allow_shorts,
    }


def _volume_ratio(volumes: Sequence[float]) -> Tuple[float, float]:
    if not volumes:
        return 0.0, 0.0
    avg = mean(volumes[:-1]) if len(volumes) > 1 else volumes[0]
    avg = avg if avg > 0 else 0.0
    last = volumes[-1]
    ratio = last / avg if avg else 0.0
    return ratio, avg


async def _gather_klines(symbol: str) -> Optional[Dict[str, List[Candle]]]:
    candles = await get_required_candles(symbol)
    if not candles:
        return None

    required_keys = ["1d", "4h", "1h", "15m", "5m"]
    if not all(candles.get(tf) for tf in required_keys):
        return None

    return {tf: candles[tf] for tf in required_keys}


async def _get_hourly_snapshot(symbol: str) -> Optional[Dict[str, float]]:
    data = await fetch_json(KLINES_URL, params={"symbol": symbol, "interval": "1h", "limit": 2})
    if not data or len(data) < 1:
        return None

    kline = data[-1]
    try:
        open_price = float(kline[1])
        close_price = float(kline[4])
        quote_volume = float(kline[7])
    except (TypeError, ValueError, IndexError):
        return None

    if open_price <= 0:
        return None

    change_pct = (close_price - open_price) / open_price * 100
    return {"change_pct": change_pct, "volume_usdt": max(quote_volume, 0.0)}


async def get_alt_watch_symbol(limit: int = 80, batch_size: int = 10) -> Optional[Dict[str, float]]:
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
) -> Optional[Dict[str, Any]]:
    def detect_side(entry: float, sl: float, tp1: float) -> Optional[str]:
        if tp1 > entry and sl < entry:
            return "LONG"
        if tp1 < entry and sl > entry:
            return "SHORT"
        return None

    candles_1d = candles["1d"]
    candles_4h = candles["4h"]
    candles_1h = candles["1h"]
    candles_15m = candles["15m"]
    candles_5m = candles["5m"]

    current_price = candles_5m[-1].close

    # Отсекаем сверхдешёвые монеты
    if current_price < 0.00001:
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
        return None

    if candidate_side == "SHORT" and not btc_ctx.get("allow_shorts", False):
        return None

    atr_15m = compute_atr(candles_15m[-60:]) if len(candles_15m) >= 15 else None
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

    closes_1h = [c.close for c in candles_1h]
    ema50_1h = compute_ema(closes_1h, 50) if len(closes_1h) >= 50 else None
    ema200_1h = compute_ema(closes_1h, 200) if len(closes_1h) >= 200 else None
    ma_trend_ok = False
    if ema50_1h and ema200_1h:
        if candidate_side == "LONG" and current_price >= ema50_1h >= ema200_1h:
            ma_trend_ok = True
        if candidate_side == "SHORT" and current_price <= ema50_1h <= ema200_1h:
            ma_trend_ok = True

    if not ma_trend_ok:
        if free_mode and ema50_1h:
            near_ema50 = abs(current_price - ema50_1h) / current_price * 100 <= EMA50_NEAR_PCT_FREE
            if not near_ema50:
                return None
        else:
            return None

    if not atr_ok:
        return None

    orderflow = await analyze_orderflow(symbol)

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

    if abs(raw_score) < min_score:
        return None

    entry_ref = (entry_from + entry_to) / 2
    side = detect_side(entry_ref, sl, tp1)
    if side is None:
        print(f"[ai_signals] Invalid side for {symbol}: entry={entry_ref} sl={sl} tp1={tp1}")
        return None

    risk = abs(entry_ref - sl)
    reward = abs(tp1 - entry_ref)
    rr = reward / risk if risk > 0 else 0.0

    min_rr = MIN_RR_FREE if free_mode else 2.0
    if rr < min_rr:
        return None

    if side not in ("LONG", "SHORT") or rr < 1.5 or risk <= 0 or reward <= 0:
        print(
            "[ai_signals] Pre-send check failed "
            f"{symbol}: side={side} rr={rr:.2f} risk={risk:.6f} reward={reward:.6f}"
        )
        return None

    rsi_1h_series = _compute_rsi_series(closes_1h)
    rsi_1h_value = rsi_1h_series[-1] if rsi_1h_series else 50.0
    rsi_zone = "комфортная зона"
    if rsi_1h_value >= 70:
        rsi_zone = "перекупленность"
    elif rsi_1h_value <= 30:
        rsi_zone = "перепроданность"

    volume_ratio, volume_avg = _volume_ratio([c.volume for c in candles_1h])

    # Требуем хотя бы 30% всплеска объёма
    min_volume_ratio = MIN_VOLUME_RATIO_FREE if free_mode else 1.3
    if volume_ratio < min_volume_ratio:
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
        "reason": {
            "trend_1d": global_trend,
            "trend_4h": h4_structure["trend"],
            "rsi_1h": rsi_1h_value,
            "rsi_1h_zone": rsi_zone,
            "volume_ratio": volume_ratio,
            "volume_avg": volume_avg,
            "rr": rr,
        },
        "breakdown": breakdown,
        "levels": {
            "support": round(support_level or 0.0, 4),
            "resistance": round(resistance_level or 0.0, 4),
            "atr_30": round(atr_15m or 0.0, 4),
        },
    }


async def scan_market(
    batch_size: int = 5,
    *,
    symbols: List[str] | None = None,
    use_btc_gate: bool = True,
    free_mode: bool = False,
    min_score: float = 80,
    return_stats: bool = False,
) -> List[Dict[str, Any]] | Tuple[List[Dict[str, Any]], Dict[str, int]]:
    """
    Сканирует весь рынок Binance по спотовым USDT-парам и возвращает сигналы.
    """
    if symbols is None:
        symbols = await get_spot_usdt_symbols()

    btc_ctx = await get_btc_context() if use_btc_gate else {
        "allow_longs": True,
        "allow_shorts": True,
    }
    checked = len(symbols)

    if use_btc_gate:
        if not btc_ctx["allow_longs"] and not btc_ctx["allow_shorts"]:
            if return_stats:
                return [], {"checked": checked, "candidates": 0}
            return []
    else:
        btc_ctx = {**btc_ctx, "allow_longs": True, "allow_shorts": True}

    signals: List[Dict[str, Any]] = []

    for i in range(0, len(symbols), batch_size):
        batch = symbols[i : i + batch_size]
        tasks = [asyncio.create_task(_gather_klines(symbol)) for symbol in batch]
        klines_list = await asyncio.gather(*tasks)

        for symbol, klines in zip(batch, klines_list):
            if not klines:
                continue

            signal = await _prepare_signal(
                symbol,
                klines,
                btc_ctx,
                free_mode=free_mode,
                min_score=min_score,
            )
            if signal:
                signals.append(signal)

    if return_stats:
        return signals, {"checked": checked, "candidates": len(signals)}
    return signals
