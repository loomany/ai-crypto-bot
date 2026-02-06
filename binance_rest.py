import asyncio
import os
import random
import time
from contextlib import contextmanager
from contextvars import ContextVar
from typing import Any, Optional

import aiohttp

from binance_limits import BINANCE_WEIGHT_TRACKER
from health import (
    increment_request_count,
    increment_klines_request_count,
    update_binance_global_state,
    update_binance_stage,
)
from rate_limiter import BINANCE_RATE_LIMITER

# ---- shared session (one per process) ----
_SHARED_SESSION: aiohttp.ClientSession | None = None
_SHARED_CONNECTOR: aiohttp.TCPConnector | None = None
_SHARED_LOCK = asyncio.Lock()

BINANCE_BASE_URL = "https://api.binance.com/api/v3"
# cache by (symbol, interval) to reuse across different LIMIT requests
# value: (ts, data_list)
_KLINES_CACHE: dict[tuple[str, str], tuple[float, list]] = {}
_KLINES_INFLIGHT: dict[tuple[str, str, int, int], asyncio.Task] = {}
_KLINES_CACHE_HITS: dict[str, int] = {}
_KLINES_CACHE_MISSES: dict[str, int] = {}
_KLINES_INFLIGHT_AWAITS: dict[str, int] = {}
_AGGTRADES_CACHE: dict[tuple[str, str, int, int], tuple[float, list]] = {}
_BINANCE_TIMEOUT = aiohttp.ClientTimeout(
    total=12, connect=4, sock_connect=4, sock_read=8
)
_MAX_RETRIES = 1
_STATE_LOCK = asyncio.Lock()
_CONSECUTIVE_TIMEOUTS = 0
_LAST_SUCCESS_TS = 0.0
_HAS_SUCCESS = False
_LAST_RESPONSE_TS = 0.0
_HAS_RESPONSE = False
_WATCHDOG_START_TS = time.time()
_SESSION_RESTARTS = 0
_BINANCE_DEGRADED_UNTIL = 0.0
_DEGRADED_STREAK = 0

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
    "1h": _env_int("KLINES_TTL_1H", 300),
    "15m": _env_int("KLINES_TTL_15M", 120),
    "5m": _env_int("KLINES_TTL_5M", 120),
    "1m": _env_int("KLINES_TTL_1M", 60),
}
_KLINES_CACHE_LOCK = asyncio.Lock()
_KLINES_INFLIGHT_LOCK = asyncio.Lock()
_AGGTRADES_CACHE_LOCK = asyncio.Lock()
_AGGTRADES_CACHE_TTL_SEC = _env_int("AGGTRADES_TTL_SEC", 10)
_BINANCE_WATCHDOG_MAX_AGE_SEC = _env_int("BINANCE_WATCHDOG_MAX_AGE_SEC", 300)
_BINANCE_REQUEST_MODULE = ContextVar("binance_request_module", default=None)


async def get_shared_session() -> aiohttp.ClientSession:
    global _SHARED_SESSION, _SHARED_CONNECTOR
    async with _SHARED_LOCK:
        if _SHARED_SESSION is None or _SHARED_SESSION.closed:
            _SHARED_CONNECTOR = aiohttp.TCPConnector(
                ssl=True, limit=100, ttl_dns_cache=300
            )
            _SHARED_SESSION = aiohttp.ClientSession(
                timeout=_BINANCE_TIMEOUT,
                connector=_SHARED_CONNECTOR,
            )
        return _SHARED_SESSION


async def close_shared_session() -> None:
    global _SHARED_SESSION, _SHARED_CONNECTOR
    async with _SHARED_LOCK:
        if _SHARED_SESSION is not None and not _SHARED_SESSION.closed:
            await _SHARED_SESSION.close()
        if _SHARED_CONNECTOR is not None and not _SHARED_CONNECTOR.closed:
            await _SHARED_CONNECTOR.close()
        _SHARED_SESSION = None
        _SHARED_CONNECTOR = None


async def _restart_shared_session(reason: str | None = None) -> None:
    global _SHARED_SESSION, _SHARED_CONNECTOR, _SESSION_RESTARTS, _CONSECUTIVE_TIMEOUTS
    async with _SHARED_LOCK:
        if _SHARED_SESSION is not None and not _SHARED_SESSION.closed:
            await _SHARED_SESSION.close()
        if _SHARED_CONNECTOR is not None and not _SHARED_CONNECTOR.closed:
            await _SHARED_CONNECTOR.close()
        _SHARED_CONNECTOR = aiohttp.TCPConnector(
            ssl=True, limit=100, ttl_dns_cache=300
        )
        _SHARED_SESSION = aiohttp.ClientSession(
            timeout=_BINANCE_TIMEOUT,
            connector=_SHARED_CONNECTOR,
        )
    async with _STATE_LOCK:
        _SESSION_RESTARTS += 1
        _CONSECUTIVE_TIMEOUTS = 0
        update_binance_global_state(
            consecutive_timeouts=_CONSECUTIVE_TIMEOUTS,
            session_restarts=_SESSION_RESTARTS,
        )
    if reason:
        print(f"[binance_rest] session restarted: {reason}")


async def _record_success(module: Optional[str]) -> None:
    global _CONSECUTIVE_TIMEOUTS, _LAST_SUCCESS_TS, _HAS_SUCCESS
    now = time.time()
    async with _STATE_LOCK:
        _CONSECUTIVE_TIMEOUTS = 0
        _LAST_SUCCESS_TS = now
        _HAS_SUCCESS = True
        update_binance_global_state(
            last_success_ts=_LAST_SUCCESS_TS,
            consecutive_timeouts=_CONSECUTIVE_TIMEOUTS,
        )


async def _record_response() -> None:
    global _LAST_RESPONSE_TS, _HAS_RESPONSE, _DEGRADED_STREAK
    now = time.time()
    async with _STATE_LOCK:
        _LAST_RESPONSE_TS = now
        _HAS_RESPONSE = True
        if _DEGRADED_STREAK:
            _DEGRADED_STREAK = 0


async def _record_timeout_or_network_error(module: Optional[str]) -> None:
    global _CONSECUTIVE_TIMEOUTS
    async with _STATE_LOCK:
        _CONSECUTIVE_TIMEOUTS += 1
        update_binance_global_state(consecutive_timeouts=_CONSECUTIVE_TIMEOUTS)
        should_restart = _CONSECUTIVE_TIMEOUTS >= 3
    if should_restart:
        await _restart_shared_session(reason="consecutive timeouts")


def get_binance_state() -> dict[str, float | int]:
    return {
        "last_success_ts": _LAST_SUCCESS_TS,
        "last_response_ts": _LAST_RESPONSE_TS,
        "consecutive_timeouts": _CONSECUTIVE_TIMEOUTS,
        "session_restarts": _SESSION_RESTARTS,
    }


def is_binance_degraded() -> bool:
    return time.time() < _BINANCE_DEGRADED_UNTIL


def get_binance_degraded_until() -> float:
    return _BINANCE_DEGRADED_UNTIL


async def _enter_degraded(reason: str) -> None:
    global _BINANCE_DEGRADED_UNTIL, _DEGRADED_STREAK
    now = time.time()
    duration = random.uniform(120, 300)
    _BINANCE_DEGRADED_UNTIL = max(_BINANCE_DEGRADED_UNTIL, now + duration)
    _DEGRADED_STREAK += 1
    print(
        "[binance_watchdog] CRITICAL: degraded mode enabled "
        f"for {int(duration)}s ({reason})"
    )


# ---- optional concurrency gate (reduces socket spikes) ----
# One more layer in addition to rate limiter/weight tracker.
BINANCE_SEM = asyncio.Semaphore(_env_int("BINANCE_MAX_CONCURRENCY", 5))
KLINES_SEM = asyncio.Semaphore(int(os.getenv("BINANCE_KLINES_CONCURRENCY", "6")))
AGGTRADES_SEM = asyncio.Semaphore(int(os.getenv("BINANCE_AGGTRADES_CONCURRENCY", "8")))


@contextmanager
def binance_request_context(module: str):
    token = _BINANCE_REQUEST_MODULE.set(module)
    try:
        yield
    finally:
        _BINANCE_REQUEST_MODULE.reset(token)


def get_request_module() -> Optional[str]:
    return _BINANCE_REQUEST_MODULE.get()


def _track_request() -> None:
    module = _BINANCE_REQUEST_MODULE.get()
    if module:
        increment_request_count(module)


def _track_klines_request() -> None:
    module = _BINANCE_REQUEST_MODULE.get()
    if module:
        increment_klines_request_count(module)


def _increment_stat(bucket: dict[str, int], module: Optional[str]) -> None:
    if module:
        bucket[module] = bucket.get(module, 0) + 1


def reset_klines_cache_stats(module: str) -> None:
    _KLINES_CACHE_HITS[module] = 0
    _KLINES_CACHE_MISSES[module] = 0
    _KLINES_INFLIGHT_AWAITS[module] = 0


def get_klines_cache_stats(module: str) -> dict[str, int]:
    return {
        "hits": _KLINES_CACHE_HITS.get(module, 0),
        "misses": _KLINES_CACHE_MISSES.get(module, 0),
        "inflight_awaits": _KLINES_INFLIGHT_AWAITS.get(module, 0),
    }


async def fetch_json(
    url: str,
    params: dict | None = None,
    *,
    session: aiohttp.ClientSession | None = None,
    stage: str = "request",
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

    module = _BINANCE_REQUEST_MODULE.get()
    if module:
        update_binance_stage(module, stage)

    for attempt in range(_MAX_RETRIES + 1):
        try:
            await BINANCE_WEIGHT_TRACKER.pre_request_wait()

            async def _perform_request():
                async with BINANCE_SEM:
                    async with BINANCE_RATE_LIMITER:
                        _track_request()
                        async with session.get(
                            url,
                            params=params,
                            timeout=_BINANCE_TIMEOUT,
                        ) as resp:
                            await BINANCE_WEIGHT_TRACKER.update_from_headers(resp.headers)
                            status = resp.status
                            headers = resp.headers
                            if status in (418, 429) or 500 <= status <= 599:
                                return status, headers, None
                            resp.raise_for_status()
                            return status, headers, await resp.json()

            status, headers, payload = await asyncio.wait_for(
                _perform_request(), timeout=_BINANCE_TIMEOUT.total
            )
            await _record_response()

            # Rate limit / ban protection
            if status in (418, 429) or 500 <= status <= 599:
                if attempt < _MAX_RETRIES:
                    delay = random.uniform(0.5, 1.5) * (2**attempt)
                    if status in (418, 429):
                        await BINANCE_WEIGHT_TRACKER.block_for(delay)
                    print(
                        "[BINANCE] retry "
                        f"status={status} attempt={attempt + 1}/{_MAX_RETRIES + 1} "
                        f"delay={delay:.2f}s url={url}"
                    )
                    await asyncio.sleep(delay)
                    continue
                print(f"[BINANCE] failed status={status} url={url}")
                return None

            await _record_success(module)
            return payload

        except asyncio.TimeoutError:
            print(
                "[BINANCE] timeout "
                f"attempt={attempt + 1}/{_MAX_RETRIES + 1} url={url}"
            )
            await _record_timeout_or_network_error(module)
            if attempt < _MAX_RETRIES:
                delay = random.uniform(0.5, 1.5) * (2**attempt)
                print(f"[BINANCE] retry timeout delay={delay:.2f}s url={url}")
                await asyncio.sleep(delay)
                continue
            return None

        except (aiohttp.ClientConnectorError, aiohttp.ClientPayloadError):
            print(
                "[BINANCE] timeout/network "
                f"attempt={attempt + 1}/{_MAX_RETRIES + 1} url={url}"
            )
            await _record_timeout_or_network_error(module)
            if attempt < _MAX_RETRIES:
                delay = random.uniform(0.5, 1.5) * (2**attempt)
                print(f"[BINANCE] retry network delay={delay:.2f}s url={url}")
                await asyncio.sleep(delay)
                continue
            return None

        except aiohttp.ClientResponseError:
            return None

        except Exception as exc:
            print(f"[binance_rest] Error while fetching {url}: {exc}")
            return None

    return None


async def fetch_klines(
    symbol: str,
    interval: str,
    limit: int,
    *,
    start_ms: int | None = None,
) -> Optional[list]:
    now = time.time()
    module = _BINANCE_REQUEST_MODULE.get()
    ttl_sec = _KLINES_CACHE_TTL_BY_INTERVAL.get(interval, _KLINES_CACHE_TTL_SEC_DEFAULT)
    cache_key = None if start_ms is not None else (symbol, interval)
    if cache_key is not None:
        async with _KLINES_CACHE_LOCK:
            cached = _KLINES_CACHE.get(cache_key)
            if cached:
                cached_ts, cached_data = cached
                if now - cached_ts < ttl_sec and isinstance(cached_data, list):
                    if len(cached_data) >= limit:
                        _increment_stat(_KLINES_CACHE_HITS, module)
                        print(
                            f"[binance_rest] klines {symbol} {interval} {limit} (cache_hit=True)"
                        )
                        return cached_data[-limit:]

    if is_binance_degraded():
        print(f"[binance_rest] degraded: skip klines fetch {symbol} {interval}")
        return None

    _increment_stat(_KLINES_CACHE_MISSES, module)
    inflight_key = (symbol, interval, int(start_ms or 0), limit)
    created = False
    async with _KLINES_INFLIGHT_LOCK:
        task = _KLINES_INFLIGHT.get(inflight_key)
        if task is None:
            created = True
            task = asyncio.create_task(
                _fetch_klines_from_binance(
                    symbol,
                    interval,
                    limit,
                    start_ms=start_ms,
                    cache_key=cache_key,
                    now=now,
                )
            )
            _KLINES_INFLIGHT[inflight_key] = task

    if not created:
        _increment_stat(_KLINES_INFLIGHT_AWAITS, module)
        print(
            f"[binance_rest] INFLIGHT await klines {symbol} {interval} {limit}"
        )
        return await task

    try:
        return await task
    finally:
        async with _KLINES_INFLIGHT_LOCK:
            _KLINES_INFLIGHT.pop(inflight_key, None)


async def _fetch_klines_from_binance(
    symbol: str,
    interval: str,
    limit: int,
    *,
    start_ms: int | None,
    cache_key: tuple[str, str] | None,
    now: float,
) -> Optional[list]:
    print(f"[binance_rest] MISS klines {symbol} {interval} {limit}")
    url = f"{BINANCE_BASE_URL}/klines"
    params = {
        "symbol": symbol,
        "interval": interval,
        "limit": limit,
    }
    if start_ms is not None:
        params["startTime"] = start_ms
    _track_klines_request()
    async with KLINES_SEM:
        data = await fetch_json(url, params, stage="klines")
    if not isinstance(data, list):
        return None
    if cache_key is not None:
        async with _KLINES_CACHE_LOCK:
            prev = _KLINES_CACHE.get(cache_key)
            if prev:
                _, prev_data = prev
                if isinstance(prev_data, list) and len(prev_data) >= len(data):
                    _KLINES_CACHE[cache_key] = (now, prev_data)
                else:
                    _KLINES_CACHE[cache_key] = (now, data)
            else:
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

    if is_binance_degraded():
        print(f"[binance_rest] degraded: skip aggTrades {symbol} {market}")
        return None

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
        data = await fetch_json(url, params, stage="agg_trades")
    if not isinstance(data, list):
        return None
    async with _AGGTRADES_CACHE_LOCK:
        _AGGTRADES_CACHE[cache_key] = (now, data)
    return data


async def binance_watchdog() -> None:
    while True:
        now = time.time()
        if _BINANCE_WATCHDOG_MAX_AGE_SEC <= 0:
            await asyncio.sleep(45)
            continue
        response_age = (
            now - _LAST_RESPONSE_TS if _HAS_RESPONSE else now - _WATCHDOG_START_TS
        )
        success_age = (
            now - _LAST_SUCCESS_TS if _HAS_SUCCESS else now - _WATCHDOG_START_TS
        )
        if response_age > _BINANCE_WATCHDOG_MAX_AGE_SEC:
            print(
                "[binance_watchdog] CRITICAL: no Binance responses "
                f">{int(_BINANCE_WATCHDOG_MAX_AGE_SEC)}s"
            )
            if _DEGRADED_STREAK >= 1:
                print(
                    "[binance_watchdog] CRITICAL: degraded recovery failed, "
                    "retrying without exit"
                )
            await _restart_shared_session(reason="watchdog no responses")
            await _enter_degraded("no responses from Binance")
        if success_age > _BINANCE_WATCHDOG_MAX_AGE_SEC:
            print(
                "[binance_watchdog] WARNING: no successful Binance responses "
                f">{int(_BINANCE_WATCHDOG_MAX_AGE_SEC)}s"
            )
        await asyncio.sleep(45)
