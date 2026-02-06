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
WARMUP_TARGET_SYMBOLS = int(os.getenv("MARKET_HUB_WARMUP_TARGET_SYMBOLS", "250"))
WARMUP_AFTER_SEC = int(os.getenv("MARKET_HUB_WARMUP_AFTER_SEC", "120"))
WARMUP_BURST_SIZE = int(os.getenv("MARKET_HUB_WARMUP_BURST_SIZE", "100"))
WARMUP_MIN_BURST = int(os.getenv("MARKET_HUB_WARMUP_MIN_BURST", "50"))


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
        self._symbols_ordered: List[str] = []
        self._universe_size = 0
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
        self.last_cycle_reason: Optional[str] = None
        self._started_at = 0.0
        self._warmup_forced = False
        self._warmup_active = False
        self._warmup_offset = 0

    async def start(self) -> None:
        if self._running and self._task and not self._task.done():
            return
        self._started_at = time.time()
        self._warmup_forced = False
        self._running = True
        self._task = asyncio.create_task(self._run())
        self._task.add_done_callback(self._handle_task_done)

    async def stop(self) -> None:
        self._running = False
        if self._task:
            self._task.cancel()
            with suppress(asyncio.CancelledError):
                await self._task
        self._task = None

    def is_task_alive(self) -> bool:
        return bool(self._task) and not self._task.done()

    def register_symbols(self, symbols: Iterable[str]) -> None:
        for symbol in symbols:
            if symbol and symbol not in self._symbols:
                self._symbols.add(symbol)
                self._symbols_ordered.append(symbol)

    def set_symbols(self, symbols: Iterable[str]) -> None:
        clean = [symbol for symbol in symbols if symbol]
        seen = set()
        ordered: List[str] = []
        for symbol in clean:
            if symbol in seen:
                continue
            seen.add(symbol)
            ordered.append(symbol)
        self._symbols = set(ordered)
        self._symbols_ordered = ordered
        self._universe_size = len(ordered)

    def get_candles(self, symbol: str, tf: str) -> Optional[List[Candle]]:
        if symbol:
            self.register_symbols([symbol])
        return self._candles.get(symbol, {}).get(tf)

    def get_bundle(self, symbol: str, tfs: Tuple[str, ...]) -> Optional[Dict[str, List[Candle]]]:
        if symbol:
            self.register_symbols([symbol])
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

    def _should_warmup(self, cache_size: int) -> bool:
        if cache_size < WARMUP_MIN_SYMBOLS:
            return True
        if self._warmup_active and cache_size < WARMUP_TARGET_SYMBOLS:
            return True
        return False

    def _select_warmup_symbols(self, symbols: List[str]) -> List[str]:
        if not symbols:
            return []
        burst_size = max(WARMUP_MIN_BURST, WARMUP_BURST_SIZE)
        burst_size = min(len(symbols), burst_size)
        start = self._warmup_offset % len(symbols)
        end = start + burst_size
        self._warmup_offset = (self._warmup_offset + burst_size) % len(symbols)
        if end <= len(symbols):
            return symbols[start:end]
        return symbols[start:] + symbols[: end - len(symbols)]

    def _handle_task_done(self, task: asyncio.Task) -> None:
        if not self._running:
            return
        if task.cancelled():
            return
        error = task.exception()
        if error:
            self.last_error = f"market_hub task crashed: {type(error).__name__} {error}"
            print(f"[market_hub] crashed: {self.last_error}")
        self._task = asyncio.create_task(self._run())
        self._task.add_done_callback(self._handle_task_done)

    async def _run(self) -> None:
        while self._running:
            cycle_start = time.monotonic()
            cycle_refreshed: set[str] = set()
            cycle_skipped = 0
            cycle_errors = 0
            cycle_attempted = 0
            cycle_unchanged = 0
            cycle_reason: Optional[str] = None
            sleep_delay = 0.2
            try:
                symbols = list(self._symbols_ordered or self._symbols)
                if not symbols:
                    cycle_reason = "no_symbols"
                    self._warmup_active = False
                    sleep_delay = 1.0
                else:
                    cache_size = sum(
                        1
                        for symbol_candles in self._candles.values()
                        if any(symbol_candles.values())
                    )
                    warmup_active = self._should_warmup(cache_size)
                    self._warmup_active = warmup_active
                    force_symbols: set[str] = set()
                    if warmup_active:
                        cycle_reason = "warmup"
                        force_symbols = set(self._select_warmup_symbols(sorted(symbols)))
                    elif self._should_force_warmup(time.time()):
                        cycle_reason = "forced_warmup"
                        force_symbols = set(self._select_warmup_symbols(sorted(symbols)))
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
                    self.last_cycle_cache_size = sum(
                        1
                        for symbol_candles in self._candles.values()
                        if any(symbol_candles.values())
                    )
            except Exception as exc:
                cycle_errors += 1
                self.last_error = f"market_hub loop error: {type(exc).__name__} {exc}"
                cycle_reason = "error"
                print(f"[market_hub] loop error: {self.last_error}")
            finally:
                self.last_cycle_attempted = cycle_attempted
                self.last_cycle_refreshed = len(cycle_refreshed)
                self.last_cycle_unchanged = cycle_unchanged
                self.last_cycle_skipped_no_klines = cycle_skipped
                self.last_cycle_errors = cycle_errors
                self.last_cycle_reason = cycle_reason
                elapsed_ms = int((time.monotonic() - cycle_start) * 1000)
                if cycle_attempted > 0 or cycle_reason == "no_symbols":
                    self.last_cycle_ms = max(1, elapsed_ms)
                else:
                    self.last_cycle_ms = elapsed_ms
                if cycle_reason == "no_symbols":
                    self.last_cycle_cache_size = 0
                elif self.last_cycle_cache_size == 0 and self._candles:
                    self.last_cycle_cache_size = sum(
                        1
                        for symbol_candles in self._candles.values()
                        if any(symbol_candles.values())
                    )
                self.last_cycle_ts = time.time()
            await asyncio.sleep(sleep_delay)

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
