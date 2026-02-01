import asyncio
import os
import random
import time
from typing import Mapping, Optional


def _get_header(headers: Mapping[str, str], key: str) -> Optional[str]:
    # aiohttp headers are case-insensitive, but keep this helper for plain dicts/tests.
    for k, v in headers.items():
        if k.lower() == key.lower():
            return v
    return None


def _parse_retry_after(value: Optional[str]) -> Optional[float]:
    """Return seconds to wait, if Retry-After contains a delta-seconds value."""
    if not value:
        return None
    try:
        secs = float(value.strip())
        if secs >= 0:
            return secs
    except Exception:
        return None
    return None


class BinanceWeightTracker:
    """
    Tracks Binance request weight using response headers.

    Binance recommends monitoring:
      - X-MBX-USED-WEIGHT-1M
      - Retry-After (for 429)
    and backing off when near / over limits.
    """

    def __init__(self, *, limit_1m: int = 1200, soft_ratio: float = 0.90) -> None:
        self.limit_1m = int(limit_1m)
        self.soft_ratio = float(soft_ratio)
        self.used_weight_1m: int = 0

        self._lock = asyncio.Lock()
        self._blocked_until: float = 0.0  # monotonic

    async def block_for(self, seconds: float) -> None:
        if seconds <= 0:
            return
        seconds = min(seconds, 60.0)
        async with self._lock:
            until = time.monotonic() + seconds
            if until > self._blocked_until:
                self._blocked_until = until

    async def pre_request_wait(self) -> None:
        """Delay before sending a request if we are currently in a backoff window."""
        while True:
            async with self._lock:
                now = time.monotonic()
                blocked = self._blocked_until
                used = self.used_weight_1m

            if blocked > now:
                await asyncio.sleep(min(blocked - now, 60.0))
                continue

            # If we're close to the 1m limit, slow down a bit to let the window slide.
            soft_limit = int(self.limit_1m * self.soft_ratio)
            if self.limit_1m > 0 and used >= soft_limit:
                # scale ~0.5..2.5s when used goes from soft_limit..limit
                over = min(max(used - soft_limit, 0), max(self.limit_1m - soft_limit, 1))
                ratio = over / max(self.limit_1m - soft_limit, 1)
                await asyncio.sleep(min(0.5 + 2.0 * ratio, 60.0))
                continue

            return

    async def update_from_headers(self, headers: Mapping[str, str]) -> None:
        used_1m = _get_header(headers, "x-mbx-used-weight-1m")
        if used_1m is None:
            return
        try:
            used_int = int(float(str(used_1m).strip()))
        except Exception:
            return

        async with self._lock:
            self.used_weight_1m = max(used_int, 0)


# Shared singleton (imported by binance_rest / signals)
BINANCE_WEIGHT_TRACKER = BinanceWeightTracker(
    limit_1m=int(float(os.environ.get("BINANCE_WEIGHT_LIMIT_1M", "1200"))),
    soft_ratio=float(os.environ.get("BINANCE_WEIGHT_SOFT_RATIO", "0.90")),
)


def calc_backoff_seconds(
    *,
    attempt: int,
    retry_after_header: Optional[str],
    base: float = 0.5,
    cap: float = 60.0,
) -> float:
    """Backoff that prefers Retry-After if present, otherwise exponential + jitter."""
    ra = _parse_retry_after(retry_after_header)
    if ra is not None:
        # tiny jitter so multiple workers don't stampede
        return min(ra + random.uniform(0.05, 0.25), cap, 60.0)

    exp = base * (2**attempt)
    return min(exp + random.uniform(0.05, 0.25), cap, 60.0)
