import asyncio
import time
from typing import Dict, List, Optional, Tuple

import os

from ai_types import Candle
from binance_client import get_required_candles, get_quick_candles
from binance_rest import is_binance_degraded
from market_hub import MARKET_HUB

DEFAULT_TFS = ("1d", "4h", "1h", "15m", "5m")


def _use_market_hub() -> bool:
    return os.getenv("USE_MARKET_HUB", "0").strip().lower() in ("1", "true", "yes", "on")


async def get_bundle_with_fallback(
    symbol: str,
    tfs: Tuple[str, ...] = DEFAULT_TFS,
    *,
    wait_tries: int = 0,
    wait_sleep: float = 0.0,
    stale_tfs: Tuple[str, ...] = ("1h", "15m", "5m"),
    timings: dict[str, float] | None = None,
) -> Optional[Dict[str, List[Candle]]]:
    # 1) пытаемся из хаба
    if _use_market_hub():
        attempts = max(1, wait_tries)
        for attempt in range(attempts):
            start = time.perf_counter()
            bundle = MARKET_HUB.get_bundle(symbol, tfs)
            if bundle and all(bundle.get(tf) for tf in tfs):
                # если ключевые TF протухли — считаем что нет данных
                if any(MARKET_HUB.is_stale(symbol, tf) for tf in stale_tfs if tf in tfs):
                    bundle = None
                else:
                    if timings is not None:
                        timings["hub_bundle_dt"] = time.perf_counter() - start
                    return bundle
            if attempt < attempts - 1:
                await asyncio.sleep(wait_sleep)
            if timings is not None:
                timings["hub_bundle_dt"] = time.perf_counter() - start

    if is_binance_degraded():
        return None

    # 2) fallback — прямой Binance, чтобы не умер прод
    candles = await get_required_candles(symbol, timings=timings)
    if not candles:
        return None
    out: Dict[str, List[Candle]] = {}
    for tf in tfs:
        if not candles.get(tf):
            return None
        out[tf] = candles[tf]
    return out


async def get_quick_with_fallback(
    symbol: str,
    *,
    tfs: Tuple[str, ...] = ("1h", "15m"),
    limit_overrides: dict[str, int] | None = None,
    timings: dict[str, float] | None = None,
) -> Optional[Dict[str, List[Candle]]]:
    start = time.perf_counter()
    if _use_market_hub():
        bundle = MARKET_HUB.get_bundle(symbol, tfs)
        if bundle and all(bundle.get(tf) for tf in tfs):
            if any(MARKET_HUB.is_stale(symbol, tf) for tf in tfs):
                bundle = None
            else:
                if timings is not None:
                    timings["hub_bundle_dt"] = time.perf_counter() - start
                return bundle
    if is_binance_degraded():
        return None

    # fallback
    quick = await get_quick_candles(
        symbol,
        tfs=tfs,
        limit_overrides=limit_overrides,
        timings=timings,
    )
    if not quick:
        return None
    for tf in tfs:
        if not quick.get(tf):
            return None
    return {tf: quick[tf] for tf in tfs}
