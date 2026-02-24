import asyncio
import gzip
import json
import os
import time
from typing import Any, Dict, List, Set

import aiohttp

EXCHANGES = ("Binance", "OKX", "Bybit", "KuCoin", "Gate.io", "MEXC", "Bitget", "HTX", "BingX", "BitMart")


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
        self._spot_symbols_by_exchange: Dict[str, Set[str]] = {}
        self.rest_refresh_sec = float(os.getenv("ARB_REST_REFRESH_SEC", "4") or 4)
        self._rest_cache: Dict[str, Dict[str, Any]] = {}
        self._ws_cache: Dict[str, Dict[str, Dict[str, float]]] = {"HTX": {}, "BitMart": {}}
        self._ws_last_message_ts: Dict[str, float] = {"HTX": 0.0, "BitMart": 0.0}
        self._ws_last_error: Dict[str, str] = {"HTX": "", "BitMart": ""}
        self._ws_tasks: Dict[str, asyncio.Task] = {}

    @staticmethod
    def normalize_symbol(raw: str) -> str:
        return str(raw or "").upper().replace("-", "").replace("_", "")

    async def _fetch_json(self, session: aiohttp.ClientSession, url: str) -> Any:
        async with session.get(url, timeout=12) as resp:
            resp.raise_for_status()
            return await resp.json()

    @staticmethod
    def _to_float(value: Any) -> float:
        try:
            return float(value or 0.0)
        except (TypeError, ValueError):
            return 0.0

    @staticmethod
    def _to_int(value: Any, default: int) -> int:
        try:
            return int(float(value or 0.0))
        except (TypeError, ValueError):
            return default

    async def _fetch_symbols_binance(self, session: aiohttp.ClientSession) -> Set[str]:
        data = await self._fetch_json(session, "https://api.binance.com/api/v3/exchangeInfo?permissions=SPOT")
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


    async def _fetch_symbols_mexc(self, session: aiohttp.ClientSession) -> Set[str]:
        data = await self._fetch_json(session, "https://api.mexc.com/api/v3/ticker/bookTicker")
        return {self.normalize_symbol(row.get("symbol", "")) for row in data if self.normalize_symbol(row.get("symbol", "")).endswith("USDT")}

    async def _fetch_symbols_bitget(self, session: aiohttp.ClientSession) -> Set[str]:
        data = await self._fetch_json(session, "https://api.bitget.com/api/v2/spot/market/tickers")
        symbols: Set[str] = set()
        for row in data.get("data", []):
            symbol = self.normalize_symbol(row.get("symbol", ""))
            if symbol.endswith("USDT"):
                symbols.add(symbol)
        return symbols

    async def _fetch_symbols_bingx(self, session: aiohttp.ClientSession) -> Set[str]:
        data = await self._fetch_json(session, "https://open-api.bingx.com/openApi/spot/v1/ticker/24hr")
        rows = data.get("data", data if isinstance(data, list) else [])
        symbols: Set[str] = set()
        for row in rows:
            symbol = self.normalize_symbol(row.get("symbol", ""))
            if symbol.endswith("USDT"):
                symbols.add(symbol)
        return symbols

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
            self._fetch_symbols_mexc(session),
            self._fetch_symbols_bitget(session),
            self._fetch_symbols_bingx(session),
            return_exceptions=True,
        )
        spot_symbol_sources = (
            "Binance",
            "OKX",
            "Bybit",
            "KuCoin",
            "Gate.io",
            "MEXC",
            "Bitget",
            "BingX",
        )
        self._spot_symbols_by_exchange = {}

        common_symbols: Set[str] | None = None
        union_symbols: Set[str] = set()
        for idx, payload in enumerate(symbols_payload):
            if isinstance(payload, Exception):
                continue
            self._spot_symbols_by_exchange[spot_symbol_sources[idx]] = set(payload)
            union_symbols |= set(payload)
            if common_symbols is None:
                common_symbols = set(payload)
            else:
                common_symbols &= set(payload)

        if common_symbols is None:
            common_symbols = set()

        base_symbols = common_symbols or union_symbols

        try:
            volume_rows = await self._fetch_json(session, "https://api.binance.com/api/v3/ticker/24hr")
        except Exception:
            volume_rows = []

        ranked: List[tuple[str, float]] = []
        for row in volume_rows:
            symbol = self.normalize_symbol(row.get("symbol", ""))
            if symbol not in base_symbols:
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
            symbols = sorted(base_symbols)

        symbols = [s for s in symbols if s.endswith("USDT") and s not in self.exclude_symbols]
        self.cached_symbols = symbols[: self.top_symbols_limit]
        self.last_symbols_refresh = now
        return self.cached_symbols

    async def collect_opportunities_details(self, *, min_net_pct: float, fees_buy_pct: float, fees_sell_pct: float, slippage_pct: float) -> Dict[str, Any]:
        api_errors = 0
        started_ms = int(time.time() * 1000)
        async with aiohttp.ClientSession() as session:
            symbols = await self._refresh_symbols(session)
            self._ensure_ws_tasks(symbols)
            raw = await asyncio.gather(
                self._fetch_binance(session),
                self._fetch_okx(session),
                self._fetch_bybit(session),
                self._fetch_kucoin(session),
                self._fetch_gate(session),
                self.mexc_rest_fetch_all_bookticker(session),
                self.bitget_rest_fetch_all_tickers(session),
                self.bingx_rest_fetch_all_24hr(session),
                return_exceptions=True,
            )

        quotes: Dict[str, Dict[str, Dict[str, float]]] = {}
        exchange_meta: Dict[str, Dict[str, Any]] = {}
        exchanges_ok = 0
        for payload in raw:
            if isinstance(payload, Exception):
                api_errors += 1
                continue
            exchanges_ok += 1
            for exchange, ex_payload in payload.items():
                ex_quotes = ex_payload.get("quotes", {}) if isinstance(ex_payload, dict) else {}
                quotes.setdefault(exchange, {}).update(ex_quotes)
                exchange_meta[exchange] = {
                    "status": "OK" if ex_quotes else "ERROR",
                    "latency_ms": int(ex_payload.get("latency_ms", 0) or 0),
                    "received_symbols_count": len(ex_quotes),
                    "last_error": str(ex_payload.get("last_error", "") or ""),
                    "last_ws_update_age_sec": None,
                }

        now_s = time.time()
        for exchange in ("HTX", "BitMart"):
            ws_quotes = dict(self._ws_cache.get(exchange, {}))
            quotes.setdefault(exchange, {}).update(ws_quotes)
            age = None
            if self._ws_last_message_ts.get(exchange):
                age = max(0.0, now_s - self._ws_last_message_ts[exchange])
            ws_ok = bool(ws_quotes)
            exchange_meta[exchange] = {
                "status": "OK" if ws_ok else "ERROR",
                "latency_ms": 0,
                "received_symbols_count": len(ws_quotes),
                "last_error": self._ws_last_error.get(exchange, ""),
                "last_ws_update_age_sec": age,
            }
            if ws_ok:
                exchanges_ok += 1
            else:
                api_errors += 1

        now_ms = int(time.time() * 1000)
        fees_total = fees_buy_pct + fees_sell_pct
        candidates_gross = 0
        skipped_anomalies_count = 0
        skipped_anomalies: Dict[str, int] = {
            "zero_price": 0,
            "extreme_spread": 0,
            "invalid_symbol": 0,
        }
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
                spot_symbols = self._spot_symbols_by_exchange.get(exchange)
                if spot_symbols is not None and symbol not in spot_symbols:
                    skipped_anomalies_count += 1
                    skipped_anomalies["invalid_symbol"] += 1
                    continue
                ask = q["ask"]
                bid = q["bid"]
                if ask <= 0 or bid <= 0 or ask < 0.0000001:
                    skipped_anomalies_count += 1
                    skipped_anomalies["zero_price"] += 1
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
            if gross_pct > 50.0:
                skipped_anomalies_count += 1
                skipped_anomalies["extreme_spread"] += 1
                continue
            mid = (best_sell["price"] + best_buy["price"]) / 2.0
            if mid <= 0:
                skipped_anomalies_count += 1
                skipped_anomalies["zero_price"] += 1
                continue
            spread_ratio = abs(best_sell["price"] - best_buy["price"]) / mid
            if spread_ratio > 0.5:
                skipped_anomalies_count += 1
                skipped_anomalies["extreme_spread"] += 1
                continue
            if gross_pct > 0:
                candidates_gross += 1
            net_pct = gross_pct - fees_total - slippage_pct
            age_sec = max(0.0, (now_ms - min(best_buy["ts"], best_sell["ts"])) / 1000.0)
            breakdown = {
                "trading_fees": fees_total,
                "slippage": slippage_pct,
            }
            opportunities.append(
                {
                    "ts": int(time.time()),
                    "symbol": symbol,
                    "buy_exchange": best_buy["exchange"],
                    "sell_exchange": best_sell["exchange"],
                    "ask": best_buy["price"],
                    "bid": best_sell["price"],
                    "gross_pct": gross_pct,
                    "net_pct": net_pct,
                    "breakdown": breakdown,
                    "age_sec": int(age_sec),
                    "dedup_key": f"{symbol}|{best_buy['exchange']}->{best_sell['exchange']}|{net_pct:.2f}",
                }
            )

        opportunities.sort(key=lambda x: x["net_pct"], reverse=True)
        qualified = [item for item in opportunities if item["net_pct"] >= min_net_pct]
        ended_ms = int(time.time() * 1000)
        return {
            "exchanges_polled": exchanges_ok,
            "total_exchanges_active": exchanges_ok,
            "exchange_stats": exchange_meta,
            "symbols_collected": len(symbols),
            "candidates_gross": candidates_gross,
            "skipped_anomalies_count": skipped_anomalies_count,
            "reason": skipped_anomalies,
            "skipped_anomalies": skipped_anomalies,
            "qualified": qualified,
            "all_opportunities": opportunities,
            "api_errors": api_errors,
            "cycle_ms": max(0, ended_ms - started_ms),
        }

    def _ensure_ws_tasks(self, symbols: List[str]) -> None:
        for ex_name, worker in (("HTX", self.htx_ws_bbo_collector), ("BitMart", self.bitmart_ws_ticker_collector)):
            task = self._ws_tasks.get(ex_name)
            if task and not task.done():
                continue
            self._ws_tasks[ex_name] = asyncio.create_task(worker(symbols))

    async def collect_opportunities(self) -> List[Dict[str, Any]]:
        details = await self.collect_opportunities_details(
            min_net_pct=float(os.getenv("ARB_MIN_NET_PCT", "0.5") or 0.5),
            fees_buy_pct=float(os.getenv("FEE_TAKER_BUY_PCT", "0.10") or 0.10),
            fees_sell_pct=float(os.getenv("FEE_TAKER_SELL_PCT", "0.10") or 0.10),
            slippage_pct=float(os.getenv("SLIPPAGE_PCT", "0.10") or 0.10),
        )
        return details["all_opportunities"]

    async def _fetch_cached_rest(self, exchange: str, loader):
        now = time.time()
        cached = self._rest_cache.get(exchange)
        if cached and now - cached.get("updated_at", 0.0) < self.rest_refresh_sec:
            return {exchange: cached["payload"]}
        started = time.perf_counter()
        try:
            quotes = await loader()
            payload = {
                "quotes": quotes,
                "latency_ms": int((time.perf_counter() - started) * 1000),
                "last_error": "",
            }
        except Exception as exc:
            payload = {
                "quotes": cached["payload"]["quotes"] if cached else {},
                "latency_ms": int((time.perf_counter() - started) * 1000),
                "last_error": str(exc),
            }
        self._rest_cache[exchange] = {"updated_at": now, "payload": payload}
        return {exchange: payload}

    async def _fetch_binance(self, session: aiohttp.ClientSession) -> Dict[str, Dict[str, Dict[str, float]]]:
        async def _load() -> Dict[str, Dict[str, float]]:
            data = await self._fetch_json(session, "https://api.binance.com/api/v3/ticker/bookTicker")
            out: Dict[str, Dict[str, float]] = {}
            now_ms = int(time.time() * 1000)
            for row in data:
                symbol = self.normalize_symbol(row.get("symbol", ""))
                if not symbol.endswith("USDT"):
                    continue
                out[symbol] = {"ask": self._to_float(row.get("askPrice")), "bid": self._to_float(row.get("bidPrice")), "ts": now_ms}
            return out

        return await self._fetch_cached_rest("Binance", _load)

    async def _fetch_okx(self, session: aiohttp.ClientSession) -> Dict[str, Dict[str, Dict[str, float]]]:
        async def _load() -> Dict[str, Dict[str, float]]:
            data = await self._fetch_json(session, "https://www.okx.com/api/v5/market/tickers?instType=SPOT")
            out: Dict[str, Dict[str, float]] = {}
            now_ms = int(time.time() * 1000)
            for row in data.get("data", []):
                symbol = self.normalize_symbol(row.get("instId", ""))
                if not symbol.endswith("USDT"):
                    continue
                out[symbol] = {"ask": self._to_float(row.get("askPx")), "bid": self._to_float(row.get("bidPx")), "ts": self._to_int(row.get("ts"), now_ms)}
            return out

        return await self._fetch_cached_rest("OKX", _load)

    async def _fetch_bybit(self, session: aiohttp.ClientSession) -> Dict[str, Dict[str, Dict[str, float]]]:
        async def _load() -> Dict[str, Dict[str, float]]:
            data = await self._fetch_json(session, "https://api.bybit.com/v5/market/tickers?category=spot")
            out: Dict[str, Dict[str, float]] = {}
            now_ms = int(time.time() * 1000)
            for row in data.get("result", {}).get("list", []):
                symbol = self.normalize_symbol(row.get("symbol", ""))
                if not symbol.endswith("USDT"):
                    continue
                out[symbol] = {"ask": self._to_float(row.get("ask1Price")), "bid": self._to_float(row.get("bid1Price")), "ts": now_ms}
            return out

        return await self._fetch_cached_rest("Bybit", _load)

    async def _fetch_kucoin(self, session: aiohttp.ClientSession) -> Dict[str, Dict[str, Dict[str, float]]]:
        async def _load() -> Dict[str, Dict[str, float]]:
            data = await self._fetch_json(session, "https://api.kucoin.com/api/v1/market/allTickers")
            out: Dict[str, Dict[str, float]] = {}
            now_ms = int(time.time() * 1000)
            ts = self._to_int(data.get("data", {}).get("time"), now_ms)
            for row in data.get("data", {}).get("ticker", []):
                symbol = self.normalize_symbol(row.get("symbol", ""))
                if not symbol.endswith("USDT"):
                    continue
                out[symbol] = {"ask": self._to_float(row.get("sell")), "bid": self._to_float(row.get("buy")), "ts": ts}
            return out

        return await self._fetch_cached_rest("KuCoin", _load)

    async def _fetch_gate(self, session: aiohttp.ClientSession) -> Dict[str, Dict[str, Dict[str, float]]]:
        async def _load() -> Dict[str, Dict[str, float]]:
            urls = ("https://api.gateio.ws/api/v4/spot/tickers", "https://api.gate.us/api/v4/spot/tickers")
            for url in urls:
                try:
                    data = await self._fetch_json(session, url)
                    now_ms = int(time.time() * 1000)
                    out: Dict[str, Dict[str, float]] = {}
                    for row in data:
                        symbol = self.normalize_symbol(row.get("currency_pair", ""))
                        if not symbol.endswith("USDT"):
                            continue
                        out[symbol] = {"ask": self._to_float(row.get("lowest_ask")), "bid": self._to_float(row.get("highest_bid")), "ts": now_ms}
                    return out
                except Exception:
                    continue
            return {}

        return await self._fetch_cached_rest("Gate.io", _load)

    async def mexc_rest_fetch_all_bookticker(self, session: aiohttp.ClientSession) -> Dict[str, Dict[str, Dict[str, float]]]:
        async def _load() -> Dict[str, Dict[str, float]]:
            data = await self._fetch_json(session, "https://api.mexc.com/api/v3/ticker/bookTicker")
            out: Dict[str, Dict[str, float]] = {}
            now_ms = int(time.time() * 1000)
            for row in data:
                symbol = self.normalize_symbol(row.get("symbol", ""))
                if symbol.endswith("USDT"):
                    out[symbol] = {"ask": self._to_float(row.get("askPrice")), "bid": self._to_float(row.get("bidPrice")), "ts": now_ms}
            return out

        return await self._fetch_cached_rest("MEXC", _load)

    async def bitget_rest_fetch_all_tickers(self, session: aiohttp.ClientSession) -> Dict[str, Dict[str, Dict[str, float]]]:
        async def _load() -> Dict[str, Dict[str, float]]:
            data = await self._fetch_json(session, "https://api.bitget.com/api/v2/spot/market/tickers")
            out: Dict[str, Dict[str, float]] = {}
            now_ms = int(time.time() * 1000)
            for row in data.get("data", []):
                symbol = self.normalize_symbol(row.get("symbol", ""))
                if symbol.endswith("USDT"):
                    out[symbol] = {"ask": self._to_float(row.get("askPr")), "bid": self._to_float(row.get("bidPr")), "ts": self._to_int(row.get("ts"), now_ms)}
            return out

        return await self._fetch_cached_rest("Bitget", _load)

    async def bingx_rest_fetch_all_24hr(self, session: aiohttp.ClientSession) -> Dict[str, Dict[str, Dict[str, float]]]:
        async def _load() -> Dict[str, Dict[str, float]]:
            now_ms = int(time.time() * 1000)
            data = await self._fetch_json(session, "https://open-api.bingx.com/openApi/spot/v1/ticker/24hr")
            rows = data.get("data", data if isinstance(data, list) else [])
            out: Dict[str, Dict[str, float]] = {}
            for row in rows:
                symbol = self.normalize_symbol(row.get("symbol", ""))
                if not symbol.endswith("USDT"):
                    continue
                bid = self._to_float(row.get("bidPrice") or row.get("bid") or row.get("bestBid"))
                ask = self._to_float(row.get("askPrice") or row.get("ask") or row.get("bestAsk"))
                if bid > 0 and ask > 0:
                    out[symbol] = {"ask": ask, "bid": bid, "ts": now_ms}
            if out:
                return out
            depth = await self._fetch_json(session, "https://open-api.bingx.com/openApi/spot/v1/ticker/bookTicker")
            rows = depth.get("data", depth if isinstance(depth, list) else [])
            for row in rows:
                symbol = self.normalize_symbol(row.get("symbol", ""))
                if symbol.endswith("USDT"):
                    out[symbol] = {"ask": self._to_float(row.get("askPrice") or row.get("ask")), "bid": self._to_float(row.get("bidPrice") or row.get("bid")), "ts": now_ms}
            return out

        return await self._fetch_cached_rest("BingX", _load)

    async def htx_ws_bbo_collector(self, symbols: List[str]) -> None:
        ws_symbols = [s.lower() for s in symbols[: min(80, len(symbols))]]
        while True:
            try:
                async with aiohttp.ClientSession() as session:
                    async with session.ws_connect("wss://api-aws.huobi.pro/ws", heartbeat=20) as ws:
                        for symbol in ws_symbols:
                            await ws.send_json({"sub": f"market.{symbol}.bbo", "id": f"arb-{symbol}"})
                        async for msg in ws:
                            payload = msg.data
                            if isinstance(payload, (bytes, bytearray)):
                                try:
                                    payload = gzip.decompress(payload).decode("utf-8", "ignore")
                                except Exception:
                                    continue
                            if not isinstance(payload, str):
                                continue
                            try:
                                data = json.loads(payload)
                            except Exception:
                                continue
                            if "ping" in data:
                                await ws.send_json({"pong": data["ping"]})
                                continue
                            ch = str(data.get("ch", ""))
                            tick = data.get("tick", {})
                            if ".bbo" not in ch:
                                continue
                            raw_symbol = ch.split(".")[1] if "." in ch else ""
                            symbol = self.normalize_symbol(raw_symbol)
                            bids = tick.get("bids") or []
                            asks = tick.get("asks") or []
                            if not bids or not asks:
                                continue
                            bid = self._to_float(bids[0][0] if isinstance(bids[0], list) else 0)
                            ask = self._to_float(asks[0][0] if isinstance(asks[0], list) else 0)
                            ts = self._to_int(tick.get("ts") or data.get("ts"), int(time.time() * 1000))
                            self._ws_cache["HTX"][symbol] = {"bid": bid, "ask": ask, "ts": ts}
                            self._ws_last_message_ts["HTX"] = time.time()
                            self._ws_last_error["HTX"] = ""
            except Exception as exc:
                self._ws_last_error["HTX"] = str(exc)
                await asyncio.sleep(2)

    async def bitmart_ws_ticker_collector(self, symbols: List[str]) -> None:
        ws_symbols = [f"{s[:-4]}_USDT" for s in symbols if s.endswith("USDT")][: min(80, len(symbols))]
        while True:
            try:
                async with aiohttp.ClientSession() as session:
                    async with session.ws_connect("wss://ws-manager-compress.bitmart.com/api?protocol=1.1", heartbeat=20) as ws:
                        if ws_symbols:
                            await ws.send_json({"op": "subscribe", "args": [f"spot/ticker:{symbol}" for symbol in ws_symbols]})
                        async for msg in ws:
                            if msg.type not in (aiohttp.WSMsgType.TEXT, aiohttp.WSMsgType.BINARY):
                                continue
                            payload = msg.data
                            if isinstance(payload, (bytes, bytearray)):
                                try:
                                    payload = gzip.decompress(payload).decode("utf-8", "ignore")
                                except Exception:
                                    continue
                            data = json.loads(payload)
                            if data.get("event") == "ping":
                                await ws.send_json({"event": "pong"})
                                continue
                            table = str(data.get("table", ""))
                            if not table.startswith("spot/ticker"):
                                continue
                            for row in data.get("data", []):
                                symbol = self.normalize_symbol(row.get("symbol", ""))
                                bid = self._to_float(row.get("best_bid") or row.get("bestBid") or row.get("bid"))
                                ask = self._to_float(row.get("best_ask") or row.get("bestAsk") or row.get("ask"))
                                ts = self._to_int(row.get("ms_t") or row.get("ts"), int(time.time() * 1000))
                                if symbol.endswith("USDT") and bid > 0 and ask > 0:
                                    self._ws_cache["BitMart"][symbol] = {"bid": bid, "ask": ask, "ts": ts}
                                    self._ws_last_message_ts["BitMart"] = time.time()
                                    self._ws_last_error["BitMart"] = ""
            except Exception as exc:
                self._ws_last_error["BitMart"] = str(exc)
                await asyncio.sleep(2)
