import asyncio
import os
import random
import time
from contextlib import contextmanager
from contextvars import ContextVar
from dataclasses import dataclass, field
from typing import Any, Optional

import aiohttp

from binance_limits import BINANCE_WEIGHT_TRACKER
from rate_limiter import BINANCE_RATE_LIMITER

# ---- shared session (one per process) ----
_SHARED_SESSION: aiohttp.ClientSession | None = None
_SHARED_CONNECTOR: aiohttp.TCPConnector | None = None
_SHARED_LOCK = asyncio.Lock()

BINANCE_BASE_URL = os.getenv("BINANCE_BASE_URL", "https://api.binance.com/api/v3")
BINANCE_FALLBACK_BASE_URL = os.getenv(
    "BINANCE_FALLBACK_BASE_URL",
    "https://api.binance.vision/api/v3",
)
_BINANCE_BASE_URLS = [BINANCE_BASE_URL]
if BINANCE_FALLBACK_BASE_URL and BINANCE_FALLBACK_BASE_URL not in _BINANCE_BASE_URLS:
    _BINANCE_BASE_URLS.append(BINANCE_FALLBACK_BASE_URL)
# cache by (symbol, interval) to reuse across different LIMIT requests
# value: (ts, data_list)
_KLINES_CACHE: dict[tuple[str, str], tuple[float, list]] = {}
_KLINES_INFLIGHT: dict[tuple[str, str, int], tuple[asyncio.Task, int]] = {}
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


@dataclass
class BinanceMetrics:
    requests_total: dict[str, int] = field(default_factory=dict)
    klines_requests: dict[str, int] = field(default_factory=dict)
    candles_received: dict[str, int] = field(default_factory=dict)
    cache_hit: dict[str, int] = field(default_factory=dict)
    cache_miss: dict[str, int] = field(default_factory=dict)

    def reset(self, module: str) -> None:
        self.requests_total[module] = 0
        self.klines_requests[module] = 0
        self.candles_received[module] = 0
        self.cache_hit[module] = 0
        self.cache_miss[module] = 0

    def increment(self, bucket: dict[str, int], module: Optional[str], count: int = 1) -> None:
        if module:
            bucket[module] = bucket.get(module, 0) + count


_BINANCE_METRICS = BinanceMetrics()

def _env_int(name: str, default: int) -> int:
    value = os.getenv(name)
    if value is None:
        return default
    try:
        return int(value)
    except ValueError:
        return default


def get_klines_ttl_sec(interval: str) -> int:
    return _KLINES_CACHE_TTL_BY_INTERVAL.get(interval, _KLINES_CACHE_TTL_SEC_DEFAULT)


_KLINES_CACHE_TTL_SEC_DEFAULT = _env_int("KLINES_TTL_DEFAULT", 45)
_KLINES_CACHE_TTL_BY_INTERVAL = {
    "1d": _env_int("KLINES_TTL_1D", 28800),
    "4h": _env_int("KLINES_TTL_4H", 5400),
    "1h": _env_int("KLINES_TTL_1H", 1800),
    "30m": _env_int("KLINES_TTL_30M", 1200),
    "15m": _env_int("KLINES_TTL_15M", 900),
    "5m": _env_int("KLINES_TTL_5M", 300),
    "3m": _env_int("KLINES_TTL_3M", 150),
    "1m": _env_int("KLINES_TTL_1M", 90),
}
_KLINES_CACHE_MIN_LIMIT_BY_INTERVAL = {
    "1d": _env_int("KLINES_CACHE_MIN_1D", 60),
    "4h": _env_int("KLINES_CACHE_MIN_4H", 120),
    "1h": _env_int("KLINES_CACHE_MIN_1H", 120),
    "30m": _env_int("KLINES_CACHE_MIN_30M", 120),
    "15m": _env_int("KLINES_CACHE_MIN_15M", 120),
    "5m": _env_int("KLINES_CACHE_MIN_5M", 120),
    "3m": _env_int("KLINES_CACHE_MIN_3M", 120),
    "1m": _env_int("KLINES_CACHE_MIN_1M", 120),
}
_KLINES_CACHE_LOCK = asyncio.Lock()
_KLINES_INFLIGHT_LOCK = asyncio.Lock()
_AGGTRADES_CACHE_LOCK = asyncio.Lock()
_AGGTRADES_CACHE_TTL_SEC = _env_int("AGGTRADES_TTL_SEC", 10)
_BINANCE_REQUEST_MODULE = ContextVar("binance_request_module", default=None)


def _get_health_callbacks():
    from health import (
        increment_request_count,
        increment_klines_request_count,
        update_binance_global_state,
        update_binance_stage,
    )

    return (
        increment_request_count,
        increment_klines_request_count,
        update_binance_global_state,
        update_binance_stage,
    )


def _increment_request_count(module: str) -> None:
    increment_request_count, _, _, _ = _get_health_callbacks()
    increment_request_count(module)


def _increment_klines_request_count(module: str) -> None:
    _, increment_klines_request_count, _, _ = _get_health_callbacks()
    increment_klines_request_count(module)


def _update_binance_global_state(**kwargs: float | int) -> None:
    _, _, update_binance_global_state, _ = _get_health_callbacks()
    update_binance_global_state(**kwargs)


def _update_binance_stage(module: str, stage: str) -> None:
    _, _, _, update_binance_stage = _get_health_callbacks()
    update_binance_stage(module, stage)


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
        _update_binance_global_state(
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
        _update_binance_global_state(
            last_success_ts=_LAST_SUCCESS_TS,
            consecutive_timeouts=_CONSECUTIVE_TIMEOUTS,
        )


async def _record_response() -> None:
    global _LAST_RESPONSE_TS, _HAS_RESPONSE
    now = time.time()
    async with _STATE_LOCK:
        _LAST_RESPONSE_TS = now
        _HAS_RESPONSE = True


async def _record_timeout_or_network_error(module: Optional[str]) -> None:
    global _CONSECUTIVE_TIMEOUTS
    async with _STATE_LOCK:
        _CONSECUTIVE_TIMEOUTS += 1
        _update_binance_global_state(consecutive_timeouts=_CONSECUTIVE_TIMEOUTS)
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


# ---- optional concurrency gate (reduces socket spikes) ----
# One more layer in addition to rate limiter/weight tracker.
BINANCE_SEM = asyncio.Semaphore(_env_int("BINANCE_MAX_CONCURRENCY", 5))
KLINES_SEM = asyncio.Semaphore(int(os.getenv("BINANCE_KLINES_CONCURRENCY", "8")))
AGGTRADES_SEM = asyncio.Semaphore(int(os.getenv("BINANCE_AGGTRADES_CONCURRENCY", "10")))


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
        _BINANCE_METRICS.increment(_BINANCE_METRICS.requests_total, module)
        _increment_request_count(module)


def _track_klines_request() -> None:
    module = _BINANCE_REQUEST_MODULE.get()
    if module:
        _BINANCE_METRICS.increment(_BINANCE_METRICS.klines_requests, module)
        _increment_klines_request_count(module)


def _increment_stat(bucket: dict[str, int], module: Optional[str], count: int = 1) -> None:
    if module:
        bucket[module] = bucket.get(module, 0) + count


def reset_klines_cache_stats(module: str) -> None:
    _BINANCE_METRICS.cache_hit[module] = 0
    _BINANCE_METRICS.cache_miss[module] = 0
    _KLINES_INFLIGHT_AWAITS[module] = 0


def get_klines_cache_stats(module: str) -> dict[str, int]:
    return {
        "hits": _BINANCE_METRICS.cache_hit.get(module, 0),
        "misses": _BINANCE_METRICS.cache_miss.get(module, 0),
        "inflight_awaits": _KLINES_INFLIGHT_AWAITS.get(module, 0),
    }


def get_binance_metrics() -> BinanceMetrics:
    return _BINANCE_METRICS


def reset_binance_metrics(module: str) -> None:
    _BINANCE_METRICS.reset(module)
    _KLINES_INFLIGHT_AWAITS[module] = 0


def get_binance_metrics_snapshot(module: str) -> dict[str, int]:
    return {
        "requests_total": _BINANCE_METRICS.requests_total.get(module, 0),
        "klines_requests": _BINANCE_METRICS.klines_requests.get(module, 0),
        "candles_received": _BINANCE_METRICS.candles_received.get(module, 0),
        "cache_hit": _BINANCE_METRICS.cache_hit.get(module, 0),
        "cache_miss": _BINANCE_METRICS.cache_miss.get(module, 0),
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
        _update_binance_stage(module, stage)

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
    ttl_sec = get_klines_ttl_sec(interval)
    cache_key = None if start_ms is not None else (symbol, interval)
    fetch_limit = limit
    if cache_key is not None:
        min_limit = _KLINES_CACHE_MIN_LIMIT_BY_INTERVAL.get(interval)
        if min_limit:
            fetch_limit = max(fetch_limit, min_limit)
    if cache_key is not None:
        async with _KLINES_CACHE_LOCK:
            cached = _KLINES_CACHE.get(cache_key)
            if cached:
                cached_ts, cached_data = cached
                if now - cached_ts < ttl_sec and isinstance(cached_data, list):
                    if len(cached_data) >= limit:
                        _BINANCE_METRICS.increment(_BINANCE_METRICS.cache_hit, module)
                        _BINANCE_METRICS.increment(
                            _BINANCE_METRICS.candles_received,
                            module,
                            count=len(cached_data[-limit:]),
                        )
                        print(
                            f"[binance_rest] klines {symbol} {interval} {limit} (cache_hit=True)"
                        )
                        return cached_data[-limit:]

    inflight_key = (symbol, interval, int(start_ms or 0))
    created = False
    requested_limit = fetch_limit
    async with _KLINES_INFLIGHT_LOCK:
        inflight = _KLINES_INFLIGHT.get(inflight_key)
        if inflight is None:
            created = True
            task = asyncio.create_task(
                _fetch_klines_from_binance(
                    symbol,
                    interval,
                    requested_limit,
                    start_ms=start_ms,
                    cache_key=cache_key,
                    now=now,
                )
            )
            _KLINES_INFLIGHT[inflight_key] = (task, requested_limit)
        else:
            task, inflight_limit = inflight
            if inflight_limit < requested_limit:
                created = True
                task = asyncio.create_task(
                    _fetch_klines_from_binance(
                        symbol,
                        interval,
                        requested_limit,
                        start_ms=start_ms,
                        cache_key=cache_key,
                        now=now,
                    )
                )
                _KLINES_INFLIGHT[inflight_key] = (task, requested_limit)
    if created:
        _BINANCE_METRICS.increment(_BINANCE_METRICS.cache_miss, module)

    if not created:
        _increment_stat(_KLINES_INFLIGHT_AWAITS, module)
        print(
            f"[binance_rest] INFLIGHT await klines {symbol} {interval} {limit}"
        )
        data = await task
        if not isinstance(data, list):
            return None
        return data[-limit:]

    try:
        data = await task
        if not isinstance(data, list):
            return None
        return data[-limit:]
    finally:
        async with _KLINES_INFLIGHT_LOCK:
            inflight = _KLINES_INFLIGHT.get(inflight_key)
            if inflight and inflight[0] is task:
                _KLINES_INFLIGHT.pop(inflight_key, None)


async def get_klines(
    symbol: str,
    interval: str,
    limit: int,
    *,
    start_ms: int | None = None,
) -> Optional[list]:
    return await fetch_klines(symbol, interval, limit, start_ms=start_ms)


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
    params = {
        "symbol": symbol,
        "interval": interval,
        "limit": limit,
    }
    if start_ms is not None:
        params["startTime"] = start_ms
    _track_klines_request()
    data = None
    async with KLINES_SEM:
        for base_url in _BINANCE_BASE_URLS:
            url = f"{base_url}/klines"
            data = await fetch_json(url, params, stage="klines")
            if isinstance(data, list):
                break
    if not isinstance(data, list):
        return None
    module = _BINANCE_REQUEST_MODULE.get()
    _BINANCE_METRICS.increment(_BINANCE_METRICS.candles_received, module, count=len(data))
    if module:
        metrics = _BINANCE_METRICS
        if (
            metrics.candles_received.get(module, 0) > 0
            and metrics.klines_requests.get(module, 0) == 0
            and metrics.cache_hit.get(module, 0) == 0
        ):
            print("[binance_rest] warning: metrics inconsistent (candles without requests)")
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

    print(f"[binance_rest] aggTrades {symbol} {market} (cache_hit=False)")
    params = {
        "symbol": symbol,
        "startTime": start_ms,
        "endTime": end_ms,
        "limit": limit,
    }
    data = None
    async with AGGTRADES_SEM:
        if market == "spot":
            for base_url in _BINANCE_BASE_URLS:
                url = f"{base_url}/aggTrades"
                data = await fetch_json(url, params, stage="agg_trades")
                if isinstance(data, list):
                    break
        else:
            url = "https://fapi.binance.com/fapi/v1/aggTrades"
            data = await fetch_json(url, params, stage="agg_trades")
    if not isinstance(data, list):
        return None
    async with _AGGTRADES_CACHE_LOCK:
        _AGGTRADES_CACHE[cache_key] = (now, data)
    return data


async def binance_watchdog() -> None:
    return None
