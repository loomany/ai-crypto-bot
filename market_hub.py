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
from binance_rest import binance_request_context

DEFAULT_TFS: Tuple[str, ...] = ("1d", "4h", "1h", "15m", "5m")
HUB_BATCH_SIZE = int(os.getenv("HUB_BATCH_SIZE", "6"))

TF_LIMITS = {
    "1d": KLINES_1D_LIMIT,
    "4h": KLINES_4H_LIMIT,
    "1h": KLINES_1H_LIMIT,
    "15m": KLINES_15M_LIMIT,
    "5m": KLINES_5M_LIMIT,
}

TF_SECONDS = {
    "1d": 60 * 60 * 24,
    "4h": 60 * 60 * 4,
    "1h": 60 * 60,
    "15m": 60 * 15,
    "5m": 60 * 5,
}

TF_STALE_SECONDS = {
    "1d": 60 * 60 * 30,
    "4h": 60 * 60 * 6,
    "1h": 60 * 60 * 2,
    "15m": 60 * 30,
    "5m": 60 * 10,
}


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

    async def start(self) -> None:
        if self._running:
            return
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
        last = candles[-1]
        now_ms = int(time.time() * 1000)
        close_time = int(getattr(last, "close_time", 0))
        max_age_sec = TF_STALE_SECONDS.get(tf, TF_SECONDS.get(tf, 60) * 2)
        return now_ms - close_time > max_age_sec * 1000

    async def _run(self) -> None:
        while self._running:
            symbols = list(self._symbols)
            if not symbols:
                await asyncio.sleep(0.5)
                continue
            for tf in self._tfs:
                for batch in _chunked(symbols, self._batch_size):
                    await self._refresh_batch(tf, batch)
                    await asyncio.sleep(0)
            await asyncio.sleep(0.2)

    async def _refresh_batch(self, tf: str, symbols: List[str]) -> None:
        symbols_to_fetch = [symbol for symbol in symbols if self.is_stale(symbol, tf)]
        if not symbols_to_fetch:
            return

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
                self.last_error = f"{symbol} {tf}: {type(error).__name__} {error}"
                continue
            if data is None:
                continue
            self._candles.setdefault(symbol, {})[tf] = data
            self._updated_at.setdefault(symbol, {})[tf] = now
            any_ok = True
        if any_ok:
            self.last_ok_at = now


MARKET_HUB = MarketDataHub()
