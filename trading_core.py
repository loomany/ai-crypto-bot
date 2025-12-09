import asyncio
import time
from statistics import mean
from typing import Any, Dict, Iterable, List, Optional, Tuple

import aiohttp

from binance_client import Candle

BINANCE_FAPI_BASE = "https://fapi.binance.com/fapi/v1"
OI_HISTORY_ENDPOINT = "https://fapi.binance.com/futures/data/openInterestHist"

MIN_WHALE_TRADE_USD = 100_000
STRONG_WHALE_TRADE_USD = 300_000
MEGA_WHALE_TRADE_USD = 1_000_000


def _pivot_highs_lows(candles: List[Candle], left: int = 2, right: int = 2) -> tuple[list[Tuple[int, float]], list[Tuple[int, float]]]:
    swing_highs: list[Tuple[int, float]] = []
    swing_lows: list[Tuple[int, float]] = []

    for i in range(left, len(candles) - right):
        high = candles[i].high
        low = candles[i].low
        if all(high > candles[i - j].high for j in range(1, left + 1)) and all(
            high > candles[i + j].high for j in range(1, right + 1)
        ):
            swing_highs.append((i, high))
        if all(low < candles[i - j].low for j in range(1, left + 1)) and all(
            low < candles[i + j].low for j in range(1, right + 1)
        ):
            swing_lows.append((i, low))

    return swing_highs, swing_lows


def detect_trend_and_structure(candles: List[Candle]) -> dict:
    swing_highs, swing_lows = _pivot_highs_lows(candles)
    trend = "range"
    last_swing_high = swing_highs[-1][1] if swing_highs else None
    last_swing_low = swing_lows[-1][1] if swing_lows else None

    hh_hl_pattern = False
    lh_ll_pattern = False

    if len(swing_highs) >= 2 and len(swing_lows) >= 2:
        prev_high, last_high = swing_highs[-2][1], swing_highs[-1][1]
        prev_low, last_low = swing_lows[-2][1], swing_lows[-1][1]

        if last_high > prev_high and last_low > prev_low:
            trend = "up"
            hh_hl_pattern = True
        elif last_high < prev_high and last_low < prev_low:
            trend = "down"
            lh_ll_pattern = True

    return {
        "trend": trend,
        "last_swing_high": last_swing_high,
        "last_swing_low": last_swing_low,
        "hh_hl_pattern": hh_hl_pattern,
        "lh_ll_pattern": lh_ll_pattern,
    }


def find_key_levels(daily_candles: List[Candle], lookback_days: int = 30) -> dict:
    highs: list[float] = []
    lows: list[float] = []
    lookback = daily_candles[-lookback_days:] if len(daily_candles) >= lookback_days else daily_candles

    swing_highs, swing_lows = _pivot_highs_lows(lookback, left=1, right=1)
    highs.extend([h for _, h in swing_highs])
    lows.extend([l for _, l in swing_lows])

    if lookback:
        highs.append(max(c.high for c in lookback))
        lows.append(min(c.low for c in lookback))

    if len(daily_candles) >= 2:
        highs.append(daily_candles[-2].high)
        lows.append(daily_candles[-2].low)

    if daily_candles:
        highs.append(daily_candles[-1].high)
        lows.append(daily_candles[-1].low)

    return {
        "highs": sorted(set(highs)),
        "lows": sorted(set(lows)),
    }


def is_liquidity_sweep(recent_candles: List[Candle], level: float, direction: str) -> bool:
    if len(recent_candles) < 3:
        return False

    last_candle = recent_candles[-1]
    prev_volumes = [c.volume for c in recent_candles[-6:-1]]
    avg_volume = mean(prev_volumes) if prev_volumes else 0

    if direction == "long":
        pierced = last_candle.low < level and last_candle.close > level
    else:
        pierced = last_candle.high > level and last_candle.close < level

    return pierced and last_candle.volume >= avg_volume * 1.2 if avg_volume > 0 else pierced


def is_volume_climax(candles: List[Candle], lookback: int = 20) -> bool:
    if len(candles) <= lookback:
        return False
    prev_volumes = [c.volume for c in candles[-lookback - 1 : -1]]
    current_volume = candles[-1].volume
    sorted_volumes = sorted(prev_volumes)
    idx = int(len(sorted_volumes) * 0.9)
    threshold = sorted_volumes[idx]
    return current_volume >= threshold


def _compute_rsi_series(closes: List[float], period: int = 14) -> List[float]:
    if len(closes) < period + 1:
        return [50.0] * len(closes)

    rsis: List[float] = []
    gains = []
    losses = []
    for i in range(1, period + 1):
        diff = closes[i] - closes[i - 1]
        gains.append(max(diff, 0))
        losses.append(max(-diff, 0))

    avg_gain = sum(gains) / period
    avg_loss = sum(losses) / period if sum(losses) != 0 else 0.000001

    for i in range(period + 1, len(closes)):
        diff = closes[i] - closes[i - 1]
        gain = max(diff, 0)
        loss = max(-diff, 0)
        avg_gain = (avg_gain * (period - 1) + gain) / period
        avg_loss = (avg_loss * (period - 1) + loss) / period if avg_loss != 0 else 0.000001
        rs = avg_gain / avg_loss
        rsi = 100 - (100 / (1 + rs))
        rsis.append(round(rsi, 2))

    prefix = [50.0] * (len(closes) - len(rsis))
    return prefix + rsis


def detect_rsi_divergence(price_series: List[float], rsi_series: List[float], direction: str) -> bool:
    if len(price_series) < 6 or len(price_series) != len(rsi_series):
        return False

    swing_highs, swing_lows = _pivot_highs_lows(
        [Candle(p, p, p, p, 0, 0, 0) for p in price_series], left=1, right=1
    )

    if direction == "bullish":
        lows = swing_lows[-2:]
        if len(lows) < 2:
            return False
        (idx1, price1), (idx2, price2) = lows
        rsi1, rsi2 = rsi_series[idx1], rsi_series[idx2]
        return price2 < price1 and rsi2 > rsi1
    else:
        highs = swing_highs[-2:]
        if len(highs) < 2:
            return False
        (idx1, price1), (idx2, price2) = highs
        rsi1, rsi2 = rsi_series[idx1], rsi_series[idx2]
        return price2 > price1 and rsi2 < rsi1


def _true_range(prev_close: float, candle: Candle) -> float:
    return max(
        candle.high - candle.low,
        abs(candle.high - prev_close),
        abs(candle.low - prev_close),
    )


def compute_atr(candles: List[Candle], period: int = 14) -> Optional[float]:
    if len(candles) < period + 1:
        return None
    trs = []
    for i in range(1, period + 1):
        trs.append(_true_range(candles[i - 1].close, candles[i]))
    atr = sum(trs) / period
    for i in range(period + 1, len(candles)):
        tr = _true_range(candles[i - 1].close, candles[i])
        atr = (atr * (period - 1) + tr) / period
    return atr


def _nearest_level(price: float, levels: Iterable[float]) -> Tuple[Optional[float], Optional[float]]:
    best_level = None
    best_distance = None
    for level in levels:
        dist = abs(price - level) / price * 100
        if best_distance is None or dist < best_distance:
            best_level = level
            best_distance = dist
    return best_level, best_distance


def compute_ema(closes: List[float], period: int) -> Optional[float]:
    if len(closes) < period:
        return None
    k = 2 / (period + 1)
    ema = closes[0]
    for price in closes[1:]:
        ema = price * k + ema * (1 - k)
    return ema


def compute_bollinger_bands(
    closes: List[float], period: int = 20, mult: float = 2.0
) -> Tuple[List[float], List[float], List[float]]:
    """
    Возвращает (middle, upper, lower) списки той же длины, что и closes.
    Для первых period-1 значений можно вернуть те же значения, что и последний рассчитанный.
    """
    if len(closes) < period:
        return [closes[-1]] * len(closes), [closes[-1]] * len(closes), [closes[-1]] * len(closes)

    middles: List[float] = []
    uppers: List[float] = []
    lowers: List[float] = []

    for i in range(len(closes)):
        if i < period - 1:
            middles.append(closes[i])
            uppers.append(closes[i])
            lowers.append(closes[i])
        else:
            window = closes[i - period + 1 : i + 1]
            m = sum(window) / period
            var = sum((x - m) ** 2 for x in window) / period
            std = var ** 0.5
            middles.append(m)
            uppers.append(m + mult * std)
            lowers.append(m - mult * std)

    return middles, uppers, lowers


def is_bb_extreme_reversal(
    candles: List[Candle], period: int = 20, mult: float = 2.0, direction: str = "long"
) -> bool:
    """
    По Боллинджеру ищем экстремум + возврат внутрь канала.

    long:
      - low < lower_band
      - close > lower_band
    short:
      - high > upper_band
      - close < upper_band
    """
    if len(candles) < period + 2:
        return False

    closes = [c.close for c in candles]
    _, upper, lower = compute_bollinger_bands(closes, period=period, mult=mult)

    last = candles[-1]
    last_upper = upper[-1]
    last_lower = lower[-1]

    if direction == "long":
        return last.low < last_lower and last.close > last_lower
    else:
        return last.high > last_upper and last.close < last_upper


async def _of_fetch_json(session: aiohttp.ClientSession, url: str, params: Dict[str, Any]):
    try:
        async with session.get(url, params=params, timeout=10) as resp:
            resp.raise_for_status()
            return await resp.json()
    except Exception:
        return None


async def _of_fetch_agg_trades(session: aiohttp.ClientSession, symbol: str, start_ms: int, end_ms: int):
    params = {
        "symbol": symbol,
        "startTime": start_ms,
        "endTime": end_ms,
        "limit": 1000,
    }
    return await _of_fetch_json(session, f"{BINANCE_FAPI_BASE}/aggTrades", params)


async def _of_fetch_oi_history(session: aiohttp.ClientSession, symbol: str):
    params = {
        "symbol": symbol,
        "period": "5m",
        "limit": 3,
    }
    return await _of_fetch_json(session, OI_HISTORY_ENDPOINT, params)


async def analyze_orderflow(symbol: str) -> Dict[str, bool]:
    """
    Анализирует ордерфлоу по Binance Futures, оценивая дисбаланс агрессоров,
    активность крупных сделок и динамику OI.
    """

    now_ms = int(time.time() * 1000)
    start_ms = now_ms - 15 * 60 * 1000

    async with aiohttp.ClientSession() as session:
        trades_task = asyncio.create_task(_of_fetch_agg_trades(session, symbol, start_ms, now_ms))
        oi_task = asyncio.create_task(_of_fetch_oi_history(session, symbol))
        trades, oi_hist = await asyncio.gather(trades_task, oi_task)

    if not trades:
        return {
            "orderflow_bullish": False,
            "orderflow_bearish": False,
            "whale_activity": False,
        }

    taker_buy_quote = 0.0
    taker_sell_quote = 0.0
    whale_buy_usd = 0.0
    whale_sell_usd = 0.0

    for tr in trades:
        try:
            price = float(tr.get("p", 0.0))
            qty = float(tr.get("q", 0.0))
            usd_value = price * qty
            is_buyer_maker = bool(tr.get("m"))
        except Exception:
            continue

        if usd_value >= MIN_WHALE_TRADE_USD:
            if is_buyer_maker:
                whale_sell_usd += usd_value
            else:
                whale_buy_usd += usd_value

        if is_buyer_maker:
            taker_sell_quote += usd_value
        else:
            taker_buy_quote += usd_value

    orderflow_imbalance_pct = 0.0
    total_flow = taker_buy_quote + taker_sell_quote
    if total_flow > 0:
        orderflow_imbalance_pct = (taker_buy_quote - taker_sell_quote) / total_flow * 100

    oi_change_pct = 0.0
    if oi_hist and len(oi_hist) >= 2:
        try:
            first_oi = float(oi_hist[0]["sumOpenInterest"])
            last_oi = float(oi_hist[-1]["sumOpenInterest"])
            if first_oi > 0:
                oi_change_pct = (last_oi - first_oi) / first_oi * 100
        except Exception:
            oi_change_pct = 0.0

    whale_activity = (whale_buy_usd + whale_sell_usd) >= STRONG_WHALE_TRADE_USD

    orderflow_bullish = (
        whale_buy_usd > whale_sell_usd
        and orderflow_imbalance_pct >= 20
        and oi_change_pct >= 3
    )

    orderflow_bearish = (
        whale_sell_usd > whale_buy_usd
        and orderflow_imbalance_pct <= -20
        and oi_change_pct <= -3
    )

    return {
        "orderflow_bullish": orderflow_bullish,
        "orderflow_bearish": orderflow_bearish,
        "whale_activity": whale_activity,
    }


def compute_score(context: Dict) -> int:
    side = context.get("candidate_side")
    if not side:
        return 0

    score = 0.0

    # Глобальный тренд
    global_trend = context.get("global_trend")
    if global_trend == "up" and side == "LONG":
        score += 25
    elif global_trend == "down" and side == "SHORT":
        score += 25
    elif global_trend in ("up", "down"):
        score -= 15

    # Локальный тренд
    local_trend = context.get("local_trend")
    if local_trend == "up" and side == "LONG":
        score += 15
    elif local_trend == "down" and side == "SHORT":
        score += 15
    elif local_trend in ("up", "down"):
        score -= 10

    # Работа от ключевого уровня
    if context.get("near_key_level"):
        score += 20

    # Снос ликвидности
    if context.get("liquidity_sweep"):
        score += 15

    # Объёмный всплеск
    if context.get("volume_climax"):
        score += 10

    # RSI дивергенция
    if context.get("rsi_divergence"):
        score += 10

    # ATR-адекватность стопа
    if context.get("atr_ok"):
        score += 5
    else:
        score -= 5

    # Bollinger экстремум + возврат
    if context.get("bb_extreme"):
        score += 15

    # EMA тренд (EMA50/EMA200 в нужную сторону)
    if context.get("ma_trend_ok"):
        score += 10

    # Ордерфлоу / киты
    if context.get("orderflow_bullish") and side == "LONG":
        score += 10
    if context.get("orderflow_bearish") and side == "SHORT":
        score += 10

    return int(round(score))
