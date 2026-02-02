import asyncio
import time
from typing import Any, Dict, List

from binance_rest import fetch_json, get_shared_session

BINANCE_FAPI_BASE = "https://fapi.binance.com"
BINANCE_SPOT_BASE = "https://api.binance.com/api/v3"

DEFAULT_TTL_SEC = 30

_spot_cache: Dict[str, Any] = {"updated_at": 0.0, "data": None}
_futures_cache: Dict[str, Any] = {"updated_at": 0.0, "data": None}

_spot_lock = asyncio.Lock()
_futures_lock = asyncio.Lock()


def _fresh(cache: Dict[str, Any], ttl_sec: int) -> bool:
    updated_at = float(cache.get("updated_at", 0.0))
    return bool(cache.get("data")) and time.time() - updated_at < ttl_sec


async def _refresh_spot(ttl_sec: int) -> List[Dict[str, Any]]:
    async with _spot_lock:
        if _fresh(_spot_cache, ttl_sec):
            return _spot_cache["data"] or []
        session = await get_shared_session()
        print("[BINANCE] request ALL ticker/24hr spot")
        data = await fetch_json(
            f"{BINANCE_SPOT_BASE}/ticker/24hr",
            session=session,
            stage="ticker",
        )
        if isinstance(data, list):
            _spot_cache["data"] = data
            _spot_cache["updated_at"] = time.time()
            return data
        return _spot_cache.get("data") or []


async def _refresh_futures(ttl_sec: int) -> List[Dict[str, Any]]:
    async with _futures_lock:
        if _fresh(_futures_cache, ttl_sec):
            return _futures_cache["data"] or []
        session = await get_shared_session()
        print("[BINANCE] request ALL ticker/24hr futures")
        data = await fetch_json(
            f"{BINANCE_FAPI_BASE}/fapi/v1/ticker/24hr",
            session=session,
            stage="ticker",
        )
        if isinstance(data, list):
            _futures_cache["data"] = data
            _futures_cache["updated_at"] = time.time()
            return data
        return _futures_cache.get("data") or []


async def get_spot_24h(ttl_sec: int = DEFAULT_TTL_SEC) -> List[Dict[str, Any]]:
    if _fresh(_spot_cache, ttl_sec):
        return _spot_cache["data"] or []

    cached = _spot_cache.get("data")
    if cached:
        if not _spot_lock.locked():
            asyncio.create_task(_refresh_spot(ttl_sec))
        return cached

    return await _refresh_spot(ttl_sec)


async def get_futures_24h(ttl_sec: int = DEFAULT_TTL_SEC) -> List[Dict[str, Any]]:
    if _fresh(_futures_cache, ttl_sec):
        return _futures_cache["data"] or []

    cached = _futures_cache.get("data")
    if cached:
        if not _futures_lock.locked():
            asyncio.create_task(_refresh_futures(ttl_sec))
        return cached

    return await _refresh_futures(ttl_sec)
