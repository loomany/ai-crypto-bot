import asyncio
from typing import Dict, List, Optional, Tuple

from binance_client import Candle, get_required_candles, get_quick_candles
from market_hub import MARKET_HUB

DEFAULT_TFS = ("1d", "4h", "1h", "15m", "5m")


async def get_bundle_with_fallback(
    symbol: str,
    tfs: Tuple[str, ...] = DEFAULT_TFS,
    *,
    wait_tries: int = 3,
    wait_sleep: float = 0.2,
    stale_tfs: Tuple[str, ...] = ("1h", "15m", "5m"),
) -> Optional[Dict[str, List[Candle]]]:
    # 1) пытаемся из хаба
    for _ in range(wait_tries):
        bundle = MARKET_HUB.get_bundle(symbol, tfs)
        if bundle and all(bundle.get(tf) for tf in tfs):
            # если ключевые TF протухли — считаем что нет данных
            if any(MARKET_HUB.is_stale(symbol, tf) for tf in stale_tfs if tf in tfs):
                bundle = None
            else:
                return bundle
        await asyncio.sleep(wait_sleep)

    # 2) fallback — прямой Binance, чтобы не умер прод
    candles = await get_required_candles(symbol)
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
) -> Optional[Dict[str, List[Candle]]]:
    bundle = MARKET_HUB.get_bundle(symbol, tfs)
    if bundle and all(bundle.get(tf) for tf in tfs):
        if any(MARKET_HUB.is_stale(symbol, tf) for tf in tfs):
            bundle = None
        else:
            return bundle
    # fallback
    quick = await get_quick_candles(symbol, tfs=tfs, limit_overrides=limit_overrides)
    if not quick:
        return None
    for tf in tfs:
        if not quick.get(tf):
            return None
    return {tf: quick[tf] for tf in tfs}
