import asyncio
import time
from typing import Any, Dict, List, Optional

import aiohttp

from binance_rest import fetch_json
from health import mark_tick, mark_ok, mark_error
from pro_db import pro_list
from symbol_cache import get_futures_usdt_symbols

BINANCE_FAPI_BASE = "https://fapi.binance.com"

MIN_WHALE_TRADE_USD = 100_000
MIN_WHALE_FLOW_USD = 250_000
FLOW_WINDOW_SEC = 60
DIGEST_INTERVAL_SEC = 60
BATCH_SIZE = 12
BATCH_DELAY_SEC = 0.25


async def _fetch_agg_trades(
    session: aiohttp.ClientSession, symbol: str, start_ms: int, end_ms: int
) -> Optional[List[Dict[str, Any]]]:
    params = {"symbol": symbol, "startTime": start_ms, "endTime": end_ms, "limit": 1000}
    data = await fetch_json(
        f"{BINANCE_FAPI_BASE}/fapi/v1/aggTrades",
        params=params,
        session=session,
    )
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
    flow_buffer: Dict[str, Dict[str, float]] = {}
    last_digest_ts = 0.0
    last_signature: Optional[tuple] = None
    checked_since_digest = 0
    digest_start_ts = time.time()

    async with aiohttp.ClientSession() as session:
        symbols = await get_futures_usdt_symbols(session)
        index = 0

        while True:
            try:
                subscribers = pro_list()
                mark_tick("whales_flow", extra=f"подписчиков: {len(subscribers)}")
                if not subscribers:
                    await asyncio.sleep(3)
                    continue

                if not symbols or index >= len(symbols):
                    symbols = await get_futures_usdt_symbols(session)
                    index = 0

                batch = symbols[index : index + BATCH_SIZE]
                index += BATCH_SIZE
                checked_since_digest += len(batch)

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
                    existing = flow_buffer.get(symbol, {"buy": 0.0, "sell": 0.0, "netflow": 0.0})
                    existing["buy"] += flow["buy"]
                    existing["sell"] += flow["sell"]
                    existing["netflow"] += flow["netflow"]
                    flow_buffer[symbol] = existing

                if now - last_digest_ts >= DIGEST_INTERVAL_SEC:
                    top_in = sorted(
                        [
                            (sym, flow["netflow"])
                            for sym, flow in flow_buffer.items()
                            if flow.get("netflow", 0.0) >= 0
                        ],
                        key=lambda item: item[1],
                        reverse=True,
                    )[:10]
                    top_out = sorted(
                        [
                            (sym, flow["netflow"])
                            for sym, flow in flow_buffer.items()
                            if flow.get("netflow", 0.0) < 0
                        ],
                        key=lambda item: item[1],
                    )[:10]

                    btc_flow = flow_buffer.get("BTCUSDT", {"buy": 0.0, "sell": 0.0})
                    btc_line = (
                        "BTC netflow 60s: "
                        f"+{_format_usd(btc_flow.get('buy', 0.0))} / "
                        f"−{_format_usd(btc_flow.get('sell', 0.0))}"
                    )

                    signature = (
                        tuple(sym for sym, _ in top_in),
                        tuple(sym for sym, _ in top_out),
                    )

                    should_send = signature != last_signature or not flow_buffer
                    if should_send:
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
                            "",
                            btc_line,
                        ]
                        text = "\n".join(lines)

                        for chat_id in subscribers:
                            try:
                                await bot.send_message(chat_id=chat_id, text=text)
                            except Exception:
                                continue

                        last_signature = signature

                    cycle_sec = time.time() - digest_start_ts
                    top_in_sym = top_in[0][0] if top_in else "-"
                    top_out_sym = top_out[0][0] if top_out else "-"
                    mark_ok(
                        "whales_flow",
                        extra=(
                            f"checked={checked_since_digest} whales={len(flow_buffer)} "
                            f"top_in={top_in_sym} top_out={top_out_sym} cycle={cycle_sec:.1f}s"
                        ),
                    )

                    flow_buffer.clear()
                    last_digest_ts = now
                    checked_since_digest = 0
                    digest_start_ts = time.time()

            except Exception as e:
                msg = f"error: {e}"
                print(f"[whales_market_flow_worker] {msg}")
                mark_error("whales_flow", msg)

            await asyncio.sleep(BATCH_DELAY_SEC)
