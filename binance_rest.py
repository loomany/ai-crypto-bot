import asyncio
from typing import Any, Optional

import aiohttp

from binance_limits import BINANCE_WEIGHT_TRACKER, calc_backoff_seconds
from rate_limiter import BINANCE_RATE_LIMITER

# ---- shared session (one per process) ----
_SHARED_SESSION: aiohttp.ClientSession | None = None
_SHARED_LOCK = asyncio.Lock()


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
BINANCE_SEM = asyncio.Semaphore(4)


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
