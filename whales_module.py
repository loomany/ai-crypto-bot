import asyncio
import time
from typing import Any, Dict, List, Optional

import aiohttp

from binance_rest import fetch_json, get_shared_session
from health import mark_tick, mark_ok, mark_error, safe_worker_loop
from pro_db import pro_list
from symbol_cache import get_futures_usdt_symbols

BINANCE_FAPI_BASE = "https://fapi.binance.com"

MIN_WHALE_TRADE_USD = 100_000
MIN_WHALE_FLOW_USD = 250_000
FLOW_WINDOW_SEC = 60
DIGEST_INTERVAL_SEC = 60
SCAN_BATCH_SIZE = 12

last_whale_state = {
    "btc_netflow_usd": 0.0,
}

_whales_state = {
    "flow_buffer": {},
    "last_digest_ts": 0.0,
    "checked_since_digest": 0,
    "digest_start_ts": time.time(),
    "symbols": [],
    "index": 0,
}


def should_send_whale_alert(
    inflow: bool,
    outflow: bool,
    btc_netflow_usd: float,
    prev_state: Dict[str, float],
) -> bool:
    if inflow or outflow:
        return True

    if abs(btc_netflow_usd) >= 500_000:
        return True

    if prev_state:
        if prev_state.get("btc_netflow_usd") != btc_netflow_usd:
            return True

    return False


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


async def whales_scan_once(bot) -> None:
    subscribers = pro_list()
    mark_tick("whales_flow", extra=f"подписчиков: {len(subscribers)}")
    if not subscribers:
        return

    session = await get_shared_session()
    symbols = _whales_state["symbols"]
    index = _whales_state["index"]
    if not symbols or index >= len(symbols):
        symbols = await get_futures_usdt_symbols(session)
        index = 0
        _whales_state["symbols"] = symbols

    if not symbols:
        mark_error("whales_flow", "no symbols to scan")
        return

    batch = symbols[index : index + SCAN_BATCH_SIZE]
    _whales_state["index"] = (index + SCAN_BATCH_SIZE) % len(symbols)
    _whales_state["checked_since_digest"] += len(batch)

    now = time.time()
    start_ms = int((now - FLOW_WINDOW_SEC) * 1000)
    end_ms = int(now * 1000)

    tasks = [
        asyncio.create_task(_fetch_agg_trades(session, symbol, start_ms, end_ms))
        for symbol in batch
    ]
    results = await asyncio.gather(*tasks, return_exceptions=True)

    flow_buffer: Dict[str, Dict[str, float]] = _whales_state["flow_buffer"]
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

    last_digest_ts = _whales_state["last_digest_ts"]
    if last_digest_ts == 0.0:
        _whales_state["last_digest_ts"] = now
        return

    if now - last_digest_ts < DIGEST_INTERVAL_SEC:
        return

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
    btc_netflow_usd = btc_flow.get("buy", 0.0) - btc_flow.get("sell", 0.0)
    btc_line = (
        "BTC netflow 60s: "
        f"+{_format_usd(btc_flow.get('buy', 0.0))} / "
        f"−{_format_usd(btc_flow.get('sell', 0.0))}"
    )

    should_send = should_send_whale_alert(
        inflow=bool(top_in),
        outflow=bool(top_out),
        btc_netflow_usd=btc_netflow_usd,
        prev_state=last_whale_state,
    )
    if should_send:
        lines = [
            f"🐳 Whale Flow (последние {FLOW_WINDOW_SEC} сек)",
            "",
            "Входят:",
            ", ".join(_format_flow_line(sym, flow) for sym, flow in top_in) if top_in else "—",
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

        last_whale_state["btc_netflow_usd"] = btc_netflow_usd

    cycle_sec = time.time() - _whales_state["digest_start_ts"]
    top_in_sym = top_in[0][0] if top_in else "-"
    top_out_sym = top_out[0][0] if top_out else "-"
    mark_ok(
        "whales_flow",
        extra=(
            f"checked={_whales_state['checked_since_digest']} whales={len(flow_buffer)} "
            f"top_in={top_in_sym} top_out={top_out_sym} cycle={cycle_sec:.1f}s"
        ),
    )

    flow_buffer.clear()
    _whales_state["last_digest_ts"] = now
    _whales_state["checked_since_digest"] = 0
    _whales_state["digest_start_ts"] = time.time()


async def whales_market_flow_worker(bot):
    await safe_worker_loop("whales_flow", lambda: whales_scan_once(bot))
