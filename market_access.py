import time
from typing import Dict, List, Optional, Tuple

from ai_types import Candle
from binance_client import get_required_candles, get_quick_candles

DEFAULT_TFS = ("1d", "4h", "1h", "15m", "5m")


async def get_bundle_with_fallback(
    symbol: str,
    tfs: Tuple[str, ...] = DEFAULT_TFS,
    *,
    wait_tries: int = 0,
    wait_sleep: float = 0.0,
    stale_tfs: Tuple[str, ...] = ("1h", "15m", "5m"),
    timings: dict[str, float] | None = None,
) -> Optional[Dict[str, List[Candle]]]:
    # прямой Binance
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
