import asyncio
import os
import time
from typing import Any, Dict, List, Set

import aiohttp

EXCHANGES = ("Binance", "OKX", "Bybit", "KuCoin", "Gate.io")


class ArbScanner:
    def __init__(self) -> None:
        self.symbol_refresh_sec = int(os.getenv("ARB_SYMBOLS_REFRESH_SEC", "3600"))
        self.top_symbols_limit = int(os.getenv("ARB_TOP_SYMBOLS_LIMIT", "120"))
        self.max_quote_age_sec = int(os.getenv("ARB_MAX_QUOTE_AGE_SEC", "12"))
        self.exclude_symbols = {
            s.strip().upper()
            for s in os.getenv(
                "ARB_EXCLUDE_SYMBOLS",
                "BTCUSDT,USDCUSDT,TUSDUSDT,FDUSDUSDT,USDPUSDT",
            ).split(",")
            if s.strip()
        }
        self.last_symbols_refresh = 0.0
        self.cached_symbols: List[str] = []

    @staticmethod
    def normalize_symbol(raw: str) -> str:
        return str(raw or "").upper().replace("-", "").replace("_", "")

    @staticmethod
    def to_dash_symbol(symbol: str) -> str:
        normalized = ArbScanner.normalize_symbol(symbol)
        if not normalized.endswith("USDT"):
            return normalized
        return f"{normalized[:-4]}-USDT"

    @staticmethod
    def to_gate_symbol(symbol: str) -> str:
        normalized = ArbScanner.normalize_symbol(symbol)
        if not normalized.endswith("USDT"):
            return normalized
        return f"{normalized[:-4]}_USDT"

    async def _fetch_json(self, session: aiohttp.ClientSession, url: str) -> Any:
        async with session.get(url, timeout=12) as resp:
            resp.raise_for_status()
            return await resp.json()

    async def _fetch_symbols_binance(self, session: aiohttp.ClientSession) -> Set[str]:
        data = await self._fetch_json(
            session,
            "https://api.binance.com/api/v3/exchangeInfo?permissions=SPOT",
        )
        symbols: Set[str] = set()
        for row in data.get("symbols", []):
            symbol = self.normalize_symbol(row.get("symbol", ""))
            if symbol.endswith("USDT") and str(row.get("status", "")).upper() == "TRADING":
                symbols.add(symbol)
        return symbols

    async def _fetch_symbols_okx(self, session: aiohttp.ClientSession) -> Set[str]:
        data = await self._fetch_json(session, "https://www.okx.com/api/v5/public/instruments?instType=SPOT")
        symbols: Set[str] = set()
        for row in data.get("data", []):
            symbol = self.normalize_symbol(row.get("instId", ""))
            if symbol.endswith("USDT"):
                symbols.add(symbol)
        return symbols

    async def _fetch_symbols_bybit(self, session: aiohttp.ClientSession) -> Set[str]:
        data = await self._fetch_json(session, "https://api.bybit.com/v5/market/instruments-info?category=spot")
        symbols: Set[str] = set()
        for row in data.get("result", {}).get("list", []):
            symbol = self.normalize_symbol(row.get("symbol", ""))
            status = str(row.get("status", "")).lower()
            quote = str(row.get("quoteCoin", "")).upper()
            if symbol.endswith("USDT") and status == "trading" and quote == "USDT":
                symbols.add(symbol)
        return symbols

    async def _fetch_symbols_kucoin(self, session: aiohttp.ClientSession) -> Set[str]:
        data = await self._fetch_json(session, "https://api.kucoin.com/api/v2/symbols")
        symbols: Set[str] = set()
        for row in data.get("data", []):
            symbol = self.normalize_symbol(row.get("symbol", ""))
            quote = str(row.get("quoteCurrency", "")).upper()
            enable = bool(row.get("enableTrading", False))
            if symbol.endswith("USDT") and quote == "USDT" and enable:
                symbols.add(symbol)
        return symbols

    async def _fetch_symbols_gate(self, session: aiohttp.ClientSession) -> Set[str]:
        urls = (
            "https://api.gateio.ws/api/v4/spot/currency_pairs",
            "https://api.gate.us/api/v4/spot/currency_pairs",
        )
        for url in urls:
            try:
                data = await self._fetch_json(session, url)
                symbols: Set[str] = set()
                for row in data:
                    symbol = self.normalize_symbol(row.get("id", "") or row.get("name", "") or row.get("currency_pair", ""))
                    quote = str(row.get("quote", "") or row.get("quote_currency", "")).upper()
                    trade_status = str(row.get("trade_status", "tradable")).lower()
                    if symbol.endswith("USDT") and (quote in {"", "USDT"}) and trade_status != "untradable":
                        symbols.add(symbol)
                return symbols
            except Exception:
                continue
        return set()

    async def _refresh_symbols(self, session: aiohttp.ClientSession) -> List[str]:
        now = time.time()
        if self.cached_symbols and now - self.last_symbols_refresh < self.symbol_refresh_sec:
            return self.cached_symbols

        symbols_payload = await asyncio.gather(
            self._fetch_symbols_binance(session),
            self._fetch_symbols_okx(session),
            self._fetch_symbols_bybit(session),
            self._fetch_symbols_kucoin(session),
            self._fetch_symbols_gate(session),
            return_exceptions=True,
        )

        common_symbols: Set[str] | None = None
        for payload in symbols_payload:
            if isinstance(payload, Exception):
                continue
            if common_symbols is None:
                common_symbols = set(payload)
            else:
                common_symbols &= set(payload)

        if common_symbols is None:
            common_symbols = set()

        # Optional ranking by Binance quote volume, keeping exact symbol universe from public list endpoints.
        try:
            volume_rows = await self._fetch_json(session, "https://api.binance.com/api/v3/ticker/24hr")
        except Exception:
            volume_rows = []
        ranked: List[tuple[str, float]] = []
        for row in volume_rows:
            symbol = self.normalize_symbol(row.get("symbol", ""))
            if symbol not in common_symbols:
                continue
            try:
                qv = float(row.get("quoteVolume", 0.0) or 0.0)
            except (TypeError, ValueError):
                qv = 0.0
            ranked.append((symbol, qv))

        if ranked:
            ranked.sort(key=lambda x: x[1], reverse=True)
            symbols = [s for s, _ in ranked]
        else:
            symbols = sorted(common_symbols)

        symbols = [s for s in symbols if s.endswith("USDT") and s not in self.exclude_symbols]
        self.cached_symbols = symbols[: self.top_symbols_limit]
        self.last_symbols_refresh = now
        return self.cached_symbols

    async def collect_opportunities(self) -> List[Dict[str, Any]]:
        async with aiohttp.ClientSession() as session:
            symbols = await self._refresh_symbols(session)
            raw = await asyncio.gather(
                self._fetch_binance(session),
                self._fetch_okx(session),
                self._fetch_bybit(session),
                self._fetch_kucoin(session),
                self._fetch_gate(session),
                return_exceptions=True,
            )

        quotes: Dict[str, Dict[str, Dict[str, float]]] = {}
        for payload in raw:
            if isinstance(payload, Exception):
                continue
            for exchange, ex_quotes in payload.items():
                quotes.setdefault(exchange, {}).update(ex_quotes)

        now_ms = int(time.time() * 1000)
        opportunities: List[Dict[str, Any]] = []
        for symbol in symbols:
            best_buy = None
            best_sell = None
            for exchange in EXCHANGES:
                q = quotes.get(exchange, {}).get(symbol)
                if not q:
                    continue
                age_sec = max(0.0, (now_ms - q["ts"]) / 1000.0)
                if age_sec > self.max_quote_age_sec:
                    continue
                ask = q["ask"]
                bid = q["bid"]
                if ask <= 0 or bid <= 0:
                    continue
                if best_buy is None or ask < best_buy["price"]:
                    best_buy = {"exchange": exchange, "price": ask, "ts": q["ts"]}
                if best_sell is None or bid > best_sell["price"]:
                    best_sell = {"exchange": exchange, "price": bid, "ts": q["ts"]}

            if not best_buy or not best_sell:
                continue
            if best_sell["exchange"] == best_buy["exchange"]:
                continue
            if best_sell["price"] <= best_buy["price"]:
                continue

            gross_pct = (best_sell["price"] - best_buy["price"]) / best_buy["price"] * 100.0
            age_sec = max(0.0, (now_ms - min(best_buy["ts"], best_sell["ts"])) / 1000.0)
            opportunities.append(
                {
                    "symbol": symbol,
                    "buy_exchange": best_buy["exchange"],
                    "sell_exchange": best_sell["exchange"],
                    "ask": best_buy["price"],
                    "bid": best_sell["price"],
                    "gross_pct": gross_pct,
                    "age_sec": age_sec,
                }
            )

        opportunities.sort(key=lambda x: x["gross_pct"], reverse=True)
        return opportunities

    async def _fetch_binance(self, session: aiohttp.ClientSession) -> Dict[str, Dict[str, Dict[str, float]]]:
        data = await self._fetch_json(session, "https://api.binance.com/api/v3/ticker/bookTicker")
        out: Dict[str, Dict[str, float]] = {}
        now_ms = int(time.time() * 1000)
        for row in data:
            symbol = self.normalize_symbol(row.get("symbol", ""))
            if not symbol.endswith("USDT"):
                continue
            out[symbol] = {
                "ask": float(row.get("askPrice", 0.0) or 0.0),
                "bid": float(row.get("bidPrice", 0.0) or 0.0),
                "ts": now_ms,
            }
        return {"Binance": out}

    async def _fetch_okx(self, session: aiohttp.ClientSession) -> Dict[str, Dict[str, Dict[str, float]]]:
        data = await self._fetch_json(session, "https://www.okx.com/api/v5/market/tickers?instType=SPOT")
        out: Dict[str, Dict[str, float]] = {}
        for row in data.get("data", []):
            symbol = self.normalize_symbol(row.get("instId", ""))
            if not symbol.endswith("USDT"):
                continue
            out[symbol] = {
                "ask": float(row.get("askPx", 0.0) or 0.0),
                "bid": float(row.get("bidPx", 0.0) or 0.0),
                "ts": int(float(row.get("ts", 0.0) or 0.0)),
            }
        return {"OKX": out}

    async def _fetch_bybit(self, session: aiohttp.ClientSession) -> Dict[str, Dict[str, Dict[str, float]]]:
        data = await self._fetch_json(session, "https://api.bybit.com/v5/market/tickers?category=spot")
        out: Dict[str, Dict[str, float]] = {}
        now_ms = int(time.time() * 1000)
        for row in data.get("result", {}).get("list", []):
            symbol = self.normalize_symbol(row.get("symbol", ""))
            if not symbol.endswith("USDT"):
                continue
            out[symbol] = {
                "ask": float(row.get("ask1Price", 0.0) or 0.0),
                "bid": float(row.get("bid1Price", 0.0) or 0.0),
                "ts": now_ms,
            }
        return {"Bybit": out}

    async def _fetch_kucoin(self, session: aiohttp.ClientSession) -> Dict[str, Dict[str, Dict[str, float]]]:
        data = await self._fetch_json(session, "https://api.kucoin.com/api/v1/market/allTickers")
        out: Dict[str, Dict[str, float]] = {}
        ts = int(data.get("data", {}).get("time", 0) or 0)
        for row in data.get("data", {}).get("ticker", []):
            symbol = self.normalize_symbol(row.get("symbol", ""))
            if not symbol.endswith("USDT"):
                continue
            out[symbol] = {
                "ask": float(row.get("sell", 0.0) or 0.0),
                "bid": float(row.get("buy", 0.0) or 0.0),
                "ts": ts,
            }
        return {"KuCoin": out}

    async def _fetch_gate(self, session: aiohttp.ClientSession) -> Dict[str, Dict[str, Dict[str, float]]]:
        urls = (
            "https://api.gateio.ws/api/v4/spot/tickers",
            "https://api.gate.us/api/v4/spot/tickers",
        )
        out: Dict[str, Dict[str, float]] = {}
        for url in urls:
            try:
                data = await self._fetch_json(session, url)
                now_ms = int(time.time() * 1000)
                for row in data:
                    symbol = self.normalize_symbol(row.get("currency_pair", ""))
                    if not symbol.endswith("USDT"):
                        continue
                    out[symbol] = {
                        "ask": float(row.get("lowest_ask", 0.0) or 0.0),
                        "bid": float(row.get("highest_bid", 0.0) or 0.0),
                        "ts": now_ms,
                    }
                break
            except Exception:
                continue
        return {"Gate.io": out}
