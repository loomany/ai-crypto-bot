import asyncio
import time


class RateLimiter:
    def __init__(self, rate_per_sec: float = 10.0, capacity: int = 10) -> None:
        self.rate_per_sec = rate_per_sec
        self.capacity = capacity
        self._tokens = float(capacity)
        self._last_ts = time.monotonic()
        self._lock = asyncio.Lock()

    async def acquire(self) -> None:
        while True:
            async with self._lock:
                now = time.monotonic()
                elapsed = now - self._last_ts
                if elapsed > 0:
                    self._tokens = min(self.capacity, self._tokens + elapsed * self.rate_per_sec)
                    self._last_ts = now

                if self._tokens >= 1:
                    self._tokens -= 1
                    return

                wait_time = (1 - self._tokens) / self.rate_per_sec

            await asyncio.sleep(wait_time)

    async def release(self) -> None:
        return None

    async def __aenter__(self) -> "RateLimiter":
        await self.acquire()
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:
        await self.release()


BINANCE_RATE_LIMITER = RateLimiter(rate_per_sec=10.0, capacity=10)
