import asyncio
from typing import Any, Optional

import aiohttp

from binance_limits import BINANCE_WEIGHT_TRACKER, calc_backoff_seconds
from rate_limiter import BINANCE_RATE_LIMITER


async def fetch_json(
    url: str,
    params: dict | None = None,
    *,
    session: aiohttp.ClientSession | None = None,
    timeout: int = 10,
    retries: int = 1,
) -> Optional[Any]:
    close_session = False
    if session is None:
        session = aiohttp.ClientSession()
        close_session = True

    try:
        for attempt in range(retries + 1):
            try:
                # Binance recommends monitoring request weight headers and backing off.
                await BINANCE_WEIGHT_TRACKER.pre_request_wait()

                async with BINANCE_RATE_LIMITER:
                    async with session.get(url, params=params, timeout=timeout) as resp:
                        # Update weight usage even on error responses (if headers present)
                        await BINANCE_WEIGHT_TRACKER.update_from_headers(resp.headers)

                        if resp.status in (418, 429):
                            retry_after = resp.headers.get("Retry-After")
                            backoff = calc_backoff_seconds(
                                attempt=attempt,
                                retry_after_header=retry_after,
                            )
                            await BINANCE_WEIGHT_TRACKER.block_for(backoff)
                            await asyncio.sleep(backoff)
                            continue

                        resp.raise_for_status()
                        return await resp.json()

            except asyncio.TimeoutError:
                if attempt < retries:
                    await asyncio.sleep(0.2)
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

    finally:
        if close_session:
            await session.close()

    return None
