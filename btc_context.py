import asyncio
import os
import time
from typing import Any, Dict, List, Optional

from ai_types import Candle
from binance_rest import get_klines
from trading_core import compute_ema
from utils_klines import normalize_klines

BTC_REGIME_RISK_ON = "RISK_ON"
BTC_REGIME_RISK_OFF = "RISK_OFF"
BTC_REGIME_CHOP = "CHOP"
BTC_REGIME_SQUEEZE = "SQUEEZE"

_SOFT_BTC_TTL_SEC = int(os.getenv("SOFT_BTC_TTL_SEC", "45") or "45")
_SOFT_BTC_FORCE_REGIME = (os.getenv("SOFT_BTC_FORCE_REGIME", "") or "").strip().upper()
_SOFT_BTC_ALLOW_FORCE = (os.getenv("SOFT_BTC_ALLOW_FORCE", "") or "").strip().lower() in {"1", "true", "yes", "y"}
_APP_ENV = (os.getenv("APP_ENV", os.getenv("ENV", "")) or "").strip().lower()
_IS_DEBUG_ENV = _APP_ENV in {"dev", "debug", "local", "test"}
BTC_SQUEEZE_IMPULSE_3CANDLES_PCT = float(
    os.getenv("BTC_SQUEEZE_IMPULSE_3CANDLES_PCT", "0.9") or "0.9"
)
BTC_SQUEEZE_VOLZ_MIN = float(os.getenv("BTC_SQUEEZE_VOLZ_MIN", "1.2") or "1.2")
BTC_RISKOFF_ATR15M_PCT = float(os.getenv("BTC_RISKOFF_ATR15M_PCT", "0.9") or "0.9")
BTC_RISKOFF_ATR15M_PCT_ON = float(os.getenv("BTC_RISKOFF_ATR15M_PCT_ON", str(BTC_RISKOFF_ATR15M_PCT)) or str(BTC_RISKOFF_ATR15M_PCT))
BTC_RISKOFF_ATR15M_PCT_OFF = float(os.getenv("BTC_RISKOFF_ATR15M_PCT_OFF", "0.75") or "0.75")
BTC_SQUEEZE_CLOSE_NEAR_EXTREME_PCT = float(os.getenv("BTC_SQUEEZE_CLOSE_NEAR_EXTREME_PCT", "0.15") or "0.15")
_BTC_CONTEXT_CACHE: Dict[str, Any] = {"expires_at": 0.0, "value": None}
_BTC_CONTEXT_LOCK = asyncio.Lock()


def _ema_series(closes: List[float], period: int) -> List[float]:
    if len(closes) < period:
        return []
    k = 2 / (period + 1)
    current = closes[0]
    out = [current]
    for price in closes[1:]:
        current = price * k + current * (1 - k)
        out.append(current)
    return out


def _slope_pct(closes: List[float], lookback: int = 24) -> float:
    if len(closes) < lookback + 1:
        return 0.0
    start = closes[-lookback - 1]
    end = closes[-1]
    if abs(start) < 1e-12:
        return 0.0
    return (end - start) / start * 100.0


def _atr_percent(candles: List[Candle], period: int = 14, sample: int = 24) -> float:
    if len(candles) < period + sample + 2:
        return 0.0
    true_ranges: List[float] = []
    for idx in range(1, len(candles)):
        c = candles[idx]
        prev_close = candles[idx - 1].close
        tr = max(c.high - c.low, abs(c.high - prev_close), abs(c.low - prev_close))
        true_ranges.append(tr)
    if len(true_ranges) < period:
        return 0.0
    atr_values: List[float] = []
    for idx in range(period - 1, len(true_ranges)):
        window = true_ranges[idx - period + 1 : idx + 1]
        atr_values.append(sum(window) / period)
    atr_slice = atr_values[-sample:] if len(atr_values) >= sample else atr_values
    if not atr_slice:
        return 0.0
    close_ref = candles[-1].close
    if close_ref <= 0:
        return 0.0
    return (sum(atr_slice) / len(atr_slice)) / close_ref * 100.0


def _count_ema50_crossovers(candles_15m: List[Candle], sample: int = 48) -> int:
    closes = [c.close for c in candles_15m]
    ema50 = _ema_series(closes, 50)
    if not ema50:
        return 0
    start_idx = max(1, len(closes) - sample)
    crosses = 0
    prev_sign = 0
    for idx in range(start_idx, len(closes)):
        ema_idx = idx
        if ema_idx >= len(ema50):
            continue
        diff = closes[idx] - ema50[ema_idx]
        sign = 1 if diff > 0 else -1 if diff < 0 else 0
        if sign == 0:
            continue
        if prev_sign != 0 and sign != prev_sign:
            crosses += 1
        prev_sign = sign
    return crosses


def _count_alternating_returns(candles_15m: List[Candle], sample: int = 32) -> int:
    closes = [c.close for c in candles_15m]
    if len(closes) < sample + 1:
        sample = max(4, len(closes) - 1)
    start = max(1, len(closes) - sample)
    prev_sign = 0
    alternations = 0
    for idx in range(start, len(closes)):
        ret = closes[idx] - closes[idx - 1]
        sign = 1 if ret > 0 else -1 if ret < 0 else 0
        if sign == 0:
            continue
        if prev_sign != 0 and sign != prev_sign:
            alternations += 1
        prev_sign = sign
    return alternations


async def _fetch_btc_context_raw(prev_regime: str = BTC_REGIME_CHOP) -> Dict[str, Any]:
    symbol = "BTCUSDT"
    k1h_raw, k15m_raw = await asyncio.gather(
        get_klines(symbol, "1h", 220),
        get_klines(symbol, "15m", 120),
        return_exceptions=True,
    )
    if isinstance(k1h_raw, BaseException) or isinstance(k15m_raw, BaseException):
        return {
            "btc_regime": BTC_REGIME_CHOP,
            "reasons": ["btc_data_fetch_error"],
        }
    candles_1h = normalize_klines(k1h_raw if isinstance(k1h_raw, list) else [])
    candles_15m = normalize_klines(k15m_raw if isinstance(k15m_raw, list) else [])
    if len(candles_1h) < 200 or len(candles_15m) < 100:
        return {
            "btc_regime": BTC_REGIME_CHOP,
            "reasons": ["btc_data_insufficient"],
        }

    closes_1h = [c.close for c in candles_1h]
    ema50 = compute_ema(closes_1h, 50)
    ema200 = compute_ema(closes_1h, 200)
    slope = _slope_pct(closes_1h, lookback=24)

    atr_pct_15m = _atr_percent(candles_15m, period=14, sample=24)
    crossovers = _count_ema50_crossovers(candles_15m, sample=48)
    alternations = _count_alternating_returns(candles_15m, sample=32)
    closes_15m = [c.close for c in candles_15m]
    impulse = 0.0
    if len(closes_15m) >= 4:
        impulse_base = closes_15m[-4]
        if abs(impulse_base) > 1e-12:
            impulse = (closes_15m[-1] - impulse_base) / impulse_base * 100.0

    vol_z = 0.0
    volumes = [c.volume for c in candles_15m]
    baseline = volumes[-41:-1] if len(volumes) >= 41 else []
    if baseline:
        mean_volume = sum(baseline) / len(baseline)
        variance = sum((v - mean_volume) ** 2 for v in baseline) / len(baseline)
        std_volume = variance ** 0.5
        if std_volume > 1e-12:
            vol_z = (volumes[-1] - mean_volume) / std_volume

    last_15m = candles_15m[-1]
    rng = max(last_15m.high - last_15m.low, 1e-12)
    close_near_high = ((last_15m.high - last_15m.close) / rng) <= BTC_SQUEEZE_CLOSE_NEAR_EXTREME_PCT
    close_near_low = ((last_15m.close - last_15m.low) / rng) <= BTC_SQUEEZE_CLOSE_NEAR_EXTREME_PCT

    is_chop = atr_pct_15m <= 0.6 and (crossovers >= 4 or alternations >= 18)
    reasons: List[str] = [
        f"slope_1h={slope:+.3f}%",
        f"atr15m={atr_pct_15m:.3f}%",
        f"impulse_3x15m={impulse:+.3f}%",
        f"vol_z={vol_z:.2f}",
        f"close_near_high={str(close_near_high).lower()}",
        f"close_near_low={str(close_near_low).lower()}",
        f"ema50_crosses={crossovers}",
        f"alt_returns={alternations}",
    ]
    btc_direction = "NEUTRAL"

    riskoff_on = atr_pct_15m >= BTC_RISKOFF_ATR15M_PCT_ON
    riskoff_hysteresis_hold = prev_regime == BTC_REGIME_RISK_OFF and atr_pct_15m >= BTC_RISKOFF_ATR15M_PCT_OFF
    if riskoff_on or riskoff_hysteresis_hold:
        regime = BTC_REGIME_RISK_OFF
        reasons.append("atr_riskoff_on" if riskoff_on else "atr_riskoff_hysteresis_hold")
    elif (
        abs(impulse) >= BTC_SQUEEZE_IMPULSE_3CANDLES_PCT
        and vol_z >= BTC_SQUEEZE_VOLZ_MIN
        and ((impulse > 0 and close_near_high) or (impulse < 0 and close_near_low))
    ):
        regime = BTC_REGIME_SQUEEZE
        btc_direction = "UP" if impulse > 0 else "DOWN"
        reasons.append("squeeze_detected")
    elif is_chop:
        regime = BTC_REGIME_CHOP
        reasons.append("chop_detected")
    elif ema50 is not None and ema200 is not None and ema50 > ema200 and slope >= 0.08:
        regime = BTC_REGIME_RISK_ON
        reasons.append("ema50_gt_ema200_and_positive_slope")
    elif ema50 is not None and ema200 is not None and ema50 < ema200 and slope <= -0.08:
        regime = BTC_REGIME_RISK_OFF
        reasons.append("ema50_lt_ema200_and_negative_slope")
    else:
        regime = BTC_REGIME_CHOP
        reasons.append("mixed_trend_defaults_to_chop")

    btc_trend = regime == BTC_REGIME_RISK_ON

    return {
        "btc_regime": regime,
        "btc_direction": btc_direction,
        "btc_trend": btc_trend,
        "reasons": reasons,
        "trend_1h": "up" if (ema50 or 0) > (ema200 or 0) else "down",
        "ema50_1h": ema50,
        "ema200_1h": ema200,
        "slope_1h_pct": slope,
        "atr_15m_pct": atr_pct_15m,
        "impulse_15m_pct_3": impulse,
        "vol_z_15m": vol_z,
        "close_near_high_15m": close_near_high,
        "close_near_low_15m": close_near_low,
        "whipsaw": {
            "ema50_crossovers": crossovers,
            "alternating_returns": alternations,
        },
    }


async def get_btc_regime() -> Dict[str, Any]:
    now = time.time()
    cached = _BTC_CONTEXT_CACHE.get("value")
    if cached and now < float(_BTC_CONTEXT_CACHE.get("expires_at", 0.0)):
        return dict(cached)

    async with _BTC_CONTEXT_LOCK:
        now = time.time()
        cached = _BTC_CONTEXT_CACHE.get("value")
        if cached and now < float(_BTC_CONTEXT_CACHE.get("expires_at", 0.0)):
            return dict(cached)

        prev_regime = str((cached or {}).get("btc_regime") or BTC_REGIME_CHOP).upper()
        fresh = await _fetch_btc_context_raw(prev_regime=prev_regime)
        forced = _SOFT_BTC_FORCE_REGIME
        can_force = _SOFT_BTC_ALLOW_FORCE or _IS_DEBUG_ENV
        if forced and not can_force:
            reasons = list(fresh.get("reasons") or [])
            reasons.append("forced_regime_ignored_non_debug")
            fresh["reasons"] = reasons
        elif forced in {BTC_REGIME_RISK_ON, BTC_REGIME_RISK_OFF, BTC_REGIME_CHOP, BTC_REGIME_SQUEEZE}:
            reasons = list(fresh.get("reasons") or [])
            reasons.append(f"forced_regime={forced}")
            fresh["btc_regime"] = forced
            fresh["btc_direction"] = "NEUTRAL"
            fresh["btc_trend"] = forced == BTC_REGIME_RISK_ON
            fresh["reasons"] = reasons

        _BTC_CONTEXT_CACHE["value"] = fresh
        _BTC_CONTEXT_CACHE["expires_at"] = time.time() + max(5, _SOFT_BTC_TTL_SEC)
        return dict(fresh)
