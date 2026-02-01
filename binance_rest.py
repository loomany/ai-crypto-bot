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
            timeout = aiohttp.ClientTimeout(total=20)
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
_BINANCE_HTTP_SEM = asyncio.Semaphore(25)


async def fetch_json(
    url: str,
    params: dict | None = None,
    *,
    session: aiohttp.ClientSession | None = None,
    timeout: int = 10,
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

            async with _BINANCE_HTTP_SEM:
                async with BINANCE_RATE_LIMITER:
                    async with session.get(url, params=params, timeout=timeout) as resp:
                        await BINANCE_WEIGHT_TRACKER.update_from_headers(resp.headers)

                        # Rate limit / ban protection
                        if resp.status in (418, 429):
                            retry_after = resp.headers.get("Retry-After")
                            backoff = calc_backoff_seconds(
                                attempt=attempt,
                                retry_after_header=retry_after,
                            )
                            await BINANCE_WEIGHT_TRACKER.block_for(backoff)
                            await asyncio.sleep(backoff)
                            continue

                        # Binance temporary errors
                        if resp.status in (502, 503, 504):
                            await asyncio.sleep(0.4 + 0.2 * attempt)
                            continue

                        resp.raise_for_status()
                        return await resp.json()

        except asyncio.TimeoutError:
            if attempt < retries:
                await asyncio.sleep(0.2 + 0.2 * attempt)
                continue
            return None

        except aiohttp.ClientResponseError as exc:
            if exc.status in (418, 429):
                backoff = calc_backoff_seconds(attempt=attempt, retry_after_header=None)
                await BINANCE_WEIGHT_TRACKER.block_for(backoff)
                await asyncio.sleep(backoff)
                continue
            return None

        except Exception as exc:
            print(f"[binance_rest] Error while fetching {url}: {exc}")
            return None

    return None
