import asyncio
import os
import time
from contextlib import contextmanager
from contextvars import ContextVar
from typing import Any, Optional

import aiohttp

from binance_limits import BINANCE_WEIGHT_TRACKER, calc_backoff_seconds
from health import increment_request_count, increment_klines_request_count
from rate_limiter import BINANCE_RATE_LIMITER

# ---- shared session (one per process) ----
_SHARED_SESSION: aiohttp.ClientSession | None = None
_SHARED_LOCK = asyncio.Lock()

BINANCE_BASE_URL = "https://api.binance.com/api/v3"
_KLINES_CACHE: dict[tuple[str, str, int], tuple[float, list]] = {}
_AGGTRADES_CACHE: dict[tuple[str, str, int, int], tuple[float, list]] = {}

def _env_int(name: str, default: int) -> int:
    value = os.getenv(name)
    if value is None:
        return default
    try:
        return int(value)
    except ValueError:
        return default


_KLINES_CACHE_TTL_SEC_DEFAULT = 20
_KLINES_CACHE_TTL_BY_INTERVAL = {
    "1d": _env_int("KLINES_TTL_1D", 60 * 30),
    "4h": _env_int("KLINES_TTL_4H", 60 * 15),
    "1h": _env_int("KLINES_TTL_1H", 60 * 7),
    "15m": _env_int("KLINES_TTL_15M", 60 * 3),
    "5m": _env_int("KLINES_TTL_5M", 60),
}
_KLINES_CACHE_LOCK = asyncio.Lock()
_AGGTRADES_CACHE_LOCK = asyncio.Lock()
_AGGTRADES_CACHE_TTL_SEC = _env_int("AGGTRADES_TTL_SEC", 10)
_BINANCE_REQUEST_MODULE = ContextVar("binance_request_module", default=None)


async def get_shared_session() -> aiohttp.ClientSession:
    global _SHARED_SESSION
    async with _SHARED_LOCK:
        if _SHARED_SESSION is None or _SHARED_SESSION.closed:
            timeout = aiohttp.ClientTimeout(total=5)
            connector = aiohttp.TCPConnector(ssl=True, limit=100, ttl_dns_cache=300)
            _SHARED_SESSION = aiohttp.ClientSession(timeout=timeout, connector=connector)
        return _SHARED_SESSION


async def close_shared_session() -> None:
    global _SHARED_SESSION
    async with _SHARED_LOCK:
        if _SHARED_SESSION is not None and not _SHARED_SESSION.closed:
            await _SHARED_SESSION.close()
        _SHARED_SESSION = None


# ---- optional concurrency gate (reduces socket spikes) ----
# One more layer in addition to rate limiter/weight tracker.
BINANCE_SEM = asyncio.Semaphore(int(os.getenv("BINANCE_HTTP_CONCURRENCY", "10")))
KLINES_SEM = asyncio.Semaphore(int(os.getenv("BINANCE_KLINES_CONCURRENCY", "6")))
AGGTRADES_SEM = asyncio.Semaphore(int(os.getenv("BINANCE_AGGTRADES_CONCURRENCY", "8")))


@contextmanager
def binance_request_context(module: str):
    token = _BINANCE_REQUEST_MODULE.set(module)
    try:
        yield
    finally:
        _BINANCE_REQUEST_MODULE.reset(token)


def _track_request() -> None:
    module = _BINANCE_REQUEST_MODULE.get()
    if module:
        increment_request_count(module)


def _track_klines_request() -> None:
    module = _BINANCE_REQUEST_MODULE.get()
    if module:
        increment_klines_request_count(module)


async def fetch_json(
    url: str,
    params: dict | None = None,
    *,
    session: aiohttp.ClientSession | None = None,
    timeout: int = 5,
    retries: int = 2,
) -> Optional[Any]:
    """
    Safe GET JSON with:
      - shared session (default)
      - global weight tracking & backoff (429/418 + Retry-After)
      - retries on 5xx / timeouts
      - concurrency semaphore to avoid bursts
    """
    if session is None:
        session = await get_shared_session()

    for attempt in range(retries + 1):
        try:
            await BINANCE_WEIGHT_TRACKER.pre_request_wait()

            async def _perform_request():
                timeout_cfg = aiohttp.ClientTimeout(total=timeout)
                async with BINANCE_SEM:
                    async with BINANCE_RATE_LIMITER:
                        _track_request()
                        async with session.get(url, params=params, timeout=timeout_cfg) as resp:
                            await BINANCE_WEIGHT_TRACKER.update_from_headers(resp.headers)
                            status = resp.status
                            headers = resp.headers
                            if status in (418, 429, 502, 503, 504):
                                return status, headers, None
                            resp.raise_for_status()
                            return status, headers, await resp.json()

            status, headers, payload = await asyncio.wait_for(
                _perform_request(), timeout=timeout
            )

            # Rate limit / ban protection
            if status in (418, 429):
                retry_after = headers.get("Retry-After")
                backoff = min(
                    calc_backoff_seconds(
                        attempt=attempt,
                        retry_after_header=retry_after,
                    ),
                    60.0,
                )
                await BINANCE_WEIGHT_TRACKER.block_for(backoff)
                await asyncio.sleep(backoff)
                continue

            # Binance temporary errors
            if status in (502, 503, 504):
                await asyncio.sleep(min(0.4 + 0.2 * attempt, 60.0))
                continue

            return payload

        except asyncio.TimeoutError:
            if attempt < retries:
                await asyncio.sleep(min(0.2 + 0.2 * attempt, 60.0))
                continue
            return None

        except aiohttp.ClientResponseError as exc:
            if exc.status in (418, 429):
                backoff = min(
                    calc_backoff_seconds(attempt=attempt, retry_after_header=None),
                    60.0,
                )
                await BINANCE_WEIGHT_TRACKER.block_for(backoff)
                await asyncio.sleep(backoff)
                continue
            return None

        except Exception as exc:
            print(f"[binance_rest] Error while fetching {url}: {exc}")
            return None

    return None


async def fetch_klines(symbol: str, interval: str, limit: int) -> Optional[list]:
    cache_key = (symbol, interval, limit)
    now = time.time()
    ttl_sec = _KLINES_CACHE_TTL_BY_INTERVAL.get(interval, _KLINES_CACHE_TTL_SEC_DEFAULT)
    async with _KLINES_CACHE_LOCK:
        cached = _KLINES_CACHE.get(cache_key)
        if cached and now - cached[0] < ttl_sec:
            print(
                f"[binance_rest] klines {symbol} {interval} {limit} (cache_hit=True)"
            )
            return cached[1]

    print(f"[binance_rest] klines {symbol} {interval} {limit} (cache_hit=False)")
    url = f"{BINANCE_BASE_URL}/klines"
    params = {
        "symbol": symbol,
        "interval": interval,
        "limit": limit,
    }
    _track_klines_request()
    async with KLINES_SEM:
        data = await fetch_json(url, params)
    if not isinstance(data, list):
        return None
    async with _KLINES_CACHE_LOCK:
        _KLINES_CACHE[cache_key] = (now, data)
    return data


async def fetch_agg_trades(
    symbol: str,
    market: str,
    start_ms: int,
    end_ms: int,
    limit: int,
    *,
    window_sec: int,
) -> Optional[list]:
    cache_key = (symbol, market, window_sec, limit)
    now = time.time()
    async with _AGGTRADES_CACHE_LOCK:
        cached = _AGGTRADES_CACHE.get(cache_key)
        if cached and now - cached[0] < _AGGTRADES_CACHE_TTL_SEC:
            print(
                f"[binance_rest] aggTrades {symbol} {market} (cache_hit=True)"
            )
            return cached[1]

    print(f"[binance_rest] aggTrades {symbol} {market} (cache_hit=False)")
    if market == "spot":
        url = f"{BINANCE_BASE_URL}/aggTrades"
    else:
        url = "https://fapi.binance.com/fapi/v1/aggTrades"
    params = {
        "symbol": symbol,
        "startTime": start_ms,
        "endTime": end_ms,
        "limit": limit,
    }
    async with AGGTRADES_SEM:
        data = await fetch_json(url, params)
    if not isinstance(data, list):
        return None
    async with _AGGTRADES_CACHE_LOCK:
        _AGGTRADES_CACHE[cache_key] = (now, data)
    return data
