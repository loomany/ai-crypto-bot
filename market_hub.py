import asyncio
import os
import time
from contextlib import suppress
from typing import Dict, Iterable, List, Optional, Tuple

from binance_client import (
    Candle,
    KLINES_15M_LIMIT,
    KLINES_1D_LIMIT,
    KLINES_1H_LIMIT,
    KLINES_4H_LIMIT,
    KLINES_5M_LIMIT,
    fetch_klines,
)
from binance_rest import (
    binance_request_context,
    get_klines_ttl_sec,
    is_binance_degraded,
)

DEFAULT_TFS: Tuple[str, ...] = ("1d", "4h", "1h", "15m", "5m")
HUB_BATCH_SIZE = int(os.getenv("HUB_BATCH_SIZE", "6"))

TF_LIMITS = {
    "1d": KLINES_1D_LIMIT,
    "4h": KLINES_4H_LIMIT,
    "1h": KLINES_1H_LIMIT,
    "15m": KLINES_15M_LIMIT,
    "5m": KLINES_5M_LIMIT,
}

WARMUP_MIN_SYMBOLS = int(os.getenv("MARKET_HUB_WARMUP_MIN_SYMBOLS", "100"))
WARMUP_AFTER_SEC = int(os.getenv("MARKET_HUB_WARMUP_AFTER_SEC", "120"))
WARMUP_BURST_SIZE = int(os.getenv("MARKET_HUB_WARMUP_BURST_SIZE", "100"))


def _chunked(items: List[str], size: int) -> List[List[str]]:
    return [items[i : i + size] for i in range(0, len(items), size)]


class MarketDataHub:
    def __init__(
        self,
        *,
        tfs: Tuple[str, ...] = DEFAULT_TFS,
        batch_size: int = HUB_BATCH_SIZE,
    ) -> None:
        self._tfs = tfs
        self._batch_size = max(1, batch_size)
        self._candles: Dict[str, Dict[str, List[Candle]]] = {}
        self._updated_at: Dict[str, Dict[str, float]] = {}
        self._symbols: set[str] = set()
        self._running = False
        self._task: Optional[asyncio.Task] = None
        self.last_ok_at = 0.0
        self.last_error: Optional[str] = None
        self.last_cycle_attempted = 0
        self.last_cycle_refreshed = 0
        self.last_cycle_unchanged = 0
        self.last_cycle_skipped_no_klines = 0
        self.last_cycle_errors = 0
        self.last_cycle_ms = 0
        self.last_cycle_cache_size = 0
        self.last_cycle_ts = 0.0
        self._started_at = 0.0
        self._warmup_forced = False

    async def start(self) -> None:
        if self._running:
            return
        self._started_at = time.time()
        self._warmup_forced = False
        self._running = True
        self._task = asyncio.create_task(self._run())

    async def stop(self) -> None:
        self._running = False
        if self._task:
            self._task.cancel()
            with suppress(asyncio.CancelledError):
                await self._task
        self._task = None

    def register_symbols(self, symbols: Iterable[str]) -> None:
        for symbol in symbols:
            if symbol:
                self._symbols.add(symbol)

    def get_candles(self, symbol: str, tf: str) -> Optional[List[Candle]]:
        if symbol:
            self._symbols.add(symbol)
        return self._candles.get(symbol, {}).get(tf)

    def get_bundle(self, symbol: str, tfs: Tuple[str, ...]) -> Optional[Dict[str, List[Candle]]]:
        if symbol:
            self._symbols.add(symbol)
        if symbol not in self._candles:
            return None
        return {tf: self._candles.get(symbol, {}).get(tf, []) for tf in tfs}

    def is_stale(self, symbol: str, tf: str) -> bool:
        candles = self._candles.get(symbol, {}).get(tf)
        if not candles:
            return True
        last_update = self._updated_at.get(symbol, {}).get(tf)
        if not last_update:
            return True
        ttl_sec = get_klines_ttl_sec(tf)
        return (time.time() - last_update) > ttl_sec

    def _should_force_warmup(self, now: float) -> bool:
        if self._warmup_forced or not self._started_at:
            return False
        if now - self._started_at < WARMUP_AFTER_SEC:
            return False
        return self.last_cycle_cache_size < WARMUP_MIN_SYMBOLS

    async def _run(self) -> None:
        while self._running:
            cycle_start = time.perf_counter()
            symbols = list(self._symbols)
            if not symbols:
                self.last_cycle_attempted = 0
                self.last_cycle_refreshed = 0
                self.last_cycle_unchanged = 0
                self.last_cycle_skipped_no_klines = 0
                self.last_cycle_errors = 0
                self.last_cycle_ms = int((time.perf_counter() - cycle_start) * 1000)
                self.last_cycle_cache_size = 0
                self.last_cycle_ts = time.time()
                await asyncio.sleep(0.5)
                continue
            cycle_refreshed: set[str] = set()
            cycle_skipped = 0
            cycle_errors = 0
            cycle_attempted = 0
            cycle_unchanged = 0
            force_symbols: set[str] = set()
            if self._should_force_warmup(time.time()):
                force_symbols = set(sorted(symbols)[:WARMUP_BURST_SIZE])
                self._warmup_forced = True
            for tf in self._tfs:
                for batch in _chunked(symbols, self._batch_size):
                    attempted, refreshed, unchanged, skipped, errors = await self._refresh_batch(
                        tf, batch, force_symbols=force_symbols
                    )
                    cycle_attempted += attempted
                    cycle_unchanged += unchanged
                    cycle_refreshed.update(refreshed)
                    cycle_skipped += skipped
                    cycle_errors += errors
                    await asyncio.sleep(0)
            self.last_cycle_attempted = cycle_attempted
            self.last_cycle_refreshed = len(cycle_refreshed)
            self.last_cycle_unchanged = cycle_unchanged
            self.last_cycle_skipped_no_klines = cycle_skipped
            self.last_cycle_errors = cycle_errors
            self.last_cycle_ms = int((time.perf_counter() - cycle_start) * 1000)
            self.last_cycle_cache_size = sum(
                1
                for symbol_candles in self._candles.values()
                if any(symbol_candles.values())
            )
            self.last_cycle_ts = time.time()
            await asyncio.sleep(0.2)

    async def _refresh_batch(
        self,
        tf: str,
        symbols: List[str],
        *,
        force_symbols: Optional[set[str]] = None,
    ) -> tuple[int, set[str], int, int, int]:
        updated_symbols: set[str] = set()
        skipped_no_klines = 0
        errors_count = 0
        attempted = len(symbols)
        unchanged = 0
        if is_binance_degraded():
            return attempted, updated_symbols, unchanged, skipped_no_klines, errors_count
        symbols_to_fetch = [
            symbol
            for symbol in symbols
            if (force_symbols and symbol in force_symbols) or self.is_stale(symbol, tf)
        ]
        if not symbols_to_fetch:
            unchanged = len(symbols)
            return attempted, updated_symbols, unchanged, skipped_no_klines, errors_count
        unchanged = max(0, len(symbols) - len(symbols_to_fetch))

        limit = TF_LIMITS.get(tf, KLINES_1H_LIMIT)

        async def _safe_fetch(symbol: str) -> tuple[str, Optional[List[Candle]], Optional[Exception]]:
            try:
                with binance_request_context("market_hub"):
                    data = await fetch_klines(symbol, tf, limit)
                return symbol, data, None
            except Exception as exc:
                return symbol, None, exc

        tasks = [asyncio.create_task(_safe_fetch(symbol)) for symbol in symbols_to_fetch]
        results = await asyncio.gather(*tasks)
        any_ok = False
        now = time.time()
        for symbol, data, error in results:
            if error:
                errors_count += 1
                self.last_error = f"{symbol} {tf}: {type(error).__name__} {error}"
                continue
            if data is None:
                skipped_no_klines += 1
                continue
            self._candles.setdefault(symbol, {})[tf] = data
            self._updated_at.setdefault(symbol, {})[tf] = now
            updated_symbols.add(symbol)
            any_ok = True
        if any_ok:
            self.last_ok_at = now
        return attempted, updated_symbols, unchanged, skipped_no_klines, errors_count


MARKET_HUB = MarketDataHub()
