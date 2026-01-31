import asyncio
import time
from typing import Any, Dict, List, Optional

import aiohttp

from health import mark_tick, mark_ok, mark_error
from pro_db import pro_list

BINANCE_FAPI_BASE = "https://fapi.binance.com"

MIN_WHALE_TRADE_USD = 120_000
MIN_WHALE_FLOW_USD = 250_000
FLOW_WINDOW_SEC = 60
DIGEST_INTERVAL_SEC = 60
SYMBOLS_REFRESH_SEC = 60 * 20
BATCH_SIZE = 15
BATCH_DELAY_SEC = 0.25

_symbols_cache: dict[str, Any] = {"updated_at": 0.0, "symbols": []}


async def _fetch_json(
    session: aiohttp.ClientSession, url: str, params: Dict[str, Any] | None = None
):
    try:
        async with session.get(url, params=params, timeout=10) as resp:
            resp.raise_for_status()
            return await resp.json()
    except Exception as exc:
        print(f"[whales] fetch error {url}: {exc}")
        return None


async def _get_futures_usdt_symbols(session: aiohttp.ClientSession) -> List[str]:
    now = time.time()
    cached_symbols = _symbols_cache.get("symbols", [])
    if cached_symbols and now - float(_symbols_cache.get("updated_at", 0.0)) < SYMBOLS_REFRESH_SEC:
        return cached_symbols

    data = await _fetch_json(session, f"{BINANCE_FAPI_BASE}/fapi/v1/exchangeInfo")
    if not data:
        return cached_symbols

    symbols = []
    for row in data.get("symbols", []):
        if row.get("contractType") != "PERPETUAL":
            continue
        symbol = row.get("symbol")
        quote = row.get("quoteAsset")
        if not symbol or quote != "USDT":
            continue
        symbols.append(symbol)

    _symbols_cache["symbols"] = symbols
    _symbols_cache["updated_at"] = now
    return symbols


async def _fetch_agg_trades(
    session: aiohttp.ClientSession, symbol: str, start_ms: int, end_ms: int
) -> Optional[List[Dict[str, Any]]]:
    params = {"symbol": symbol, "startTime": start_ms, "endTime": end_ms, "limit": 1000}
    data = await _fetch_json(session, f"{BINANCE_FAPI_BASE}/fapi/v1/aggTrades", params)
    if isinstance(data, list):
        return data
    return None


def _calc_flow(trades: List[Dict[str, Any]]) -> Optional[Dict[str, float]]:
    whale_buy = 0.0
    whale_sell = 0.0

    for tr in trades:
        try:
            price = float(tr.get("p", 0.0))
            qty = float(tr.get("q", 0.0))
            usd_value = price * qty
        except (TypeError, ValueError):
            continue

        if usd_value < MIN_WHALE_TRADE_USD:
            continue

        is_buyer_maker = bool(tr.get("m"))
        if is_buyer_maker:
            whale_sell += usd_value
        else:
            whale_buy += usd_value

    total_flow = whale_buy + whale_sell
    if total_flow < MIN_WHALE_FLOW_USD:
        return None

    netflow = whale_buy - whale_sell
    return {"buy": whale_buy, "sell": whale_sell, "netflow": netflow}


def _format_usd(value: float) -> str:
    value = abs(value)
    if value >= 1_000_000_000:
        return f"${value / 1_000_000_000:.2f}B"
    if value >= 1_000_000:
        return f"${value / 1_000_000:.2f}M"
    if value >= 1_000:
        return f"${value / 1_000:.2f}K"
    return f"${value:.0f}"


def _format_symbol(symbol: str) -> str:
    return symbol.replace("USDT", "")


def _format_flow_line(symbol: str, netflow: float) -> str:
    sign = "+" if netflow >= 0 else "−"
    return f"{_format_symbol(symbol)} {sign}{_format_usd(netflow)}"


async def whales_market_flow_worker(bot):
    await asyncio.sleep(5)
    flow_buffer: Dict[str, float] = {}
    last_digest_ts = 0.0

    async with aiohttp.ClientSession() as session:
        symbols = await _get_futures_usdt_symbols(session)
        index = 0

        while True:
            try:
                subscribers = pro_list()
                mark_tick("whales_flow", extra=f"подписчиков: {len(subscribers)}")
                if not subscribers:
                    await asyncio.sleep(3)
                    continue

                if not symbols or index >= len(symbols):
                    symbols = await _get_futures_usdt_symbols(session)
                    index = 0

                batch = symbols[index : index + BATCH_SIZE]
                index += BATCH_SIZE

                now = time.time()
                start_ms = int((now - FLOW_WINDOW_SEC) * 1000)
                end_ms = int(now * 1000)

                tasks = [
                    asyncio.create_task(_fetch_agg_trades(session, symbol, start_ms, end_ms))
                    for symbol in batch
                ]
                results = await asyncio.gather(*tasks, return_exceptions=True)

                for symbol, trades in zip(batch, results):
                    if isinstance(trades, Exception) or not trades:
                        continue
                    flow = _calc_flow(trades)
                    if not flow:
                        continue
                    flow_buffer[symbol] = flow_buffer.get(symbol, 0.0) + flow["netflow"]

                if now - last_digest_ts >= DIGEST_INTERVAL_SEC:
                    if flow_buffer:
                        top_in = sorted(
                            [(sym, netflow) for sym, netflow in flow_buffer.items() if netflow >= 0],
                            key=lambda item: item[1],
                            reverse=True,
                        )[:10]
                        top_out = sorted(
                            [(sym, netflow) for sym, netflow in flow_buffer.items() if netflow < 0],
                            key=lambda item: item[1],
                        )[:10]

                        lines = [
                            f"🐳 Whale Flow (последние {FLOW_WINDOW_SEC} сек)",
                            "",
                            "Входят:",
                            ", ".join(_format_flow_line(sym, flow) for sym, flow in top_in)
                            if top_in
                            else "—",
                            "",
                            "Выходят:",
                            ", ".join(_format_flow_line(sym, flow) for sym, flow in top_out)
                            if top_out
                            else "—",
                        ]
                        text = "\n".join(lines)

                        for chat_id in subscribers:
                            try:
                                await bot.send_message(chat_id=chat_id, text=text)
                            except Exception:
                                continue

                        mark_ok(
                            "whales_flow",
                            extra=f"входы: {len(top_in)}, выходы: {len(top_out)}",
                        )

                    flow_buffer.clear()
                    last_digest_ts = now

            except Exception as e:
                msg = f"error: {e}"
                print(f"[whales_market_flow_worker] {msg}")
                mark_error("whales_flow", msg)

            await asyncio.sleep(BATCH_DELAY_SEC)
