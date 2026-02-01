import asyncio
import time
from typing import Any, Dict, List, Optional

import aiohttp

from binance_rest import fetch_json, get_shared_session
from health import mark_tick, mark_ok, mark_error, safe_worker_loop
from market_cache import get_futures_24h, get_spot_24h
from pro_db import pro_list
from symbol_cache import get_futures_usdt_symbols, get_spot_usdt_symbols

BINANCE_FAPI_BASE = "https://fapi.binance.com"
BINANCE_SPOT_BASE = "https://api.binance.com/api/v3"

FLOW_WINDOW_SEC = 60
SCAN_BATCH_SIZE = 12
CHUNK_SIZE = 50
PRIORITY_TOP_N = 50

_whales_state = {
    "quote_volume": {},
    "quote_volume_ts": 0.0,
}


async def _fetch_agg_trades(
    session: aiohttp.ClientSession,
    symbol: str,
    start_ms: int,
    end_ms: int,
    market: str,
) -> Optional[List[Dict[str, Any]]]:
    if market == "spot":
        base_url = f"{BINANCE_SPOT_BASE}/aggTrades"
    else:
        base_url = f"{BINANCE_FAPI_BASE}/fapi/v1/aggTrades"
    params = {"symbol": symbol, "startTime": start_ms, "endTime": end_ms, "limit": 1000}
    endpoint = "aggTrades spot" if market == "spot" else "aggTrades futures"
    print(f"[BINANCE] request {symbol} {endpoint}")
    try:
        data = await fetch_json(
            base_url,
            params=params,
            session=session,
        )
    except Exception as exc:
        print(f"[BINANCE] ERROR {symbol}: {exc}")
        return None
    if isinstance(data, list):
        return data
    return None


def _calc_flow(trades: List[Dict[str, Any]]) -> Optional[Dict[str, float]]:
    buy = 0.0
    sell = 0.0

    for tr in trades:
        try:
            price = float(tr.get("p", 0.0))
            qty = float(tr.get("q", 0.0))
            usd_value = price * qty
        except (TypeError, ValueError):
            continue

        is_buyer_maker = bool(tr.get("m"))
        if is_buyer_maker:
            sell += usd_value
        else:
            buy += usd_value

    if buy == 0.0 and sell == 0.0:
        return None

    return {"buy": buy, "sell": sell}


def _fmt_usd(value: float) -> str:
    return f"{int(round(value)):,} $"


def _format_symbol(symbol: str) -> str:
    return symbol.replace("USDT", "")


def _chunked(items: List[Dict[str, str]], size: int) -> List[List[Dict[str, str]]]:
    return [items[i : i + size] for i in range(0, len(items), size)]


async def _get_usdt_pairs(session: aiohttp.ClientSession) -> List[Dict[str, str]]:
    spot_symbols = await get_spot_usdt_symbols(session)
    futures_symbols = await get_futures_usdt_symbols(session)
    pairs = [{"symbol": symbol, "market": "spot"} for symbol in spot_symbols]
    pairs.extend({"symbol": symbol, "market": "futures"} for symbol in futures_symbols)
    return pairs


async def _get_quote_volumes(session: aiohttp.ClientSession) -> Dict[str, float]:
    now = time.time()
    cached = _whales_state.get("quote_volume", {})
    if cached and now - float(_whales_state.get("quote_volume_ts", 0.0)) < 300:
        return cached

    spot_data = await get_spot_24h()
    futures_data = await get_futures_24h()

    volumes: Dict[str, float] = {}
    for row in spot_data or []:
        symbol = row.get("symbol")
        if not symbol or not symbol.endswith("USDT"):
            continue
        try:
            quote_volume = float(row.get("quoteVolume", 0.0))
        except (TypeError, ValueError):
            continue
        volumes[symbol] = volumes.get(symbol, 0.0) + quote_volume

    for row in futures_data or []:
        symbol = row.get("symbol")
        if not symbol or not symbol.endswith("USDT"):
            continue
        try:
            quote_volume = float(row.get("quoteVolume", 0.0))
        except (TypeError, ValueError):
            continue
        volumes[symbol] = volumes.get(symbol, 0.0) + quote_volume

    _whales_state["quote_volume"] = volumes
    _whales_state["quote_volume_ts"] = now
    return volumes


def _threshold_for_volume(quote_volume: float) -> float:
    return max(500_000.0, min(5_000_000.0, 0.005 * quote_volume))


def _prioritize_pairs(
    pairs: List[Dict[str, str]],
    quote_volumes: Dict[str, float],
    top_n: int = PRIORITY_TOP_N,
) -> List[Dict[str, str]]:
    ranked = sorted(quote_volumes.items(), key=lambda item: item[1], reverse=True)
    top_symbols = {symbol for symbol, _ in ranked[:top_n]}
    top_pairs = [pair for pair in pairs if pair["symbol"] in top_symbols]
    rest_pairs = [pair for pair in pairs if pair["symbol"] not in top_symbols]
    return top_pairs + rest_pairs


async def whales_scan_once(bot) -> None:
    start = time.time()
    BUDGET = 30
    print("[WHALE] scan_once start")
    cycle_start = time.time()
    try:
        subscribers = pro_list()
        mark_tick("whales_flow", extra=f"подписчиков: {len(subscribers)}")
        if not subscribers:
            return

        if not hasattr(whales_scan_once, "state"):
            whales_scan_once.state = {
                "pairs": [],
                "cursor": 0,
            }
        state = whales_scan_once.state

        session = await get_shared_session()
        pairs = state["pairs"]
        cursor = state["cursor"]
        if not pairs or cursor >= len(pairs):
            pairs = await _get_usdt_pairs(session)
            cursor = 0
            state["pairs"] = pairs
        if not pairs:
            mark_error("whales_flow", "no symbols to scan")
            return

        quote_volumes = await _get_quote_volumes(session)
        if cursor == 0:
            pairs = _prioritize_pairs(pairs, quote_volumes)
            state["pairs"] = pairs

        chunk = pairs[cursor : cursor + CHUNK_SIZE]
        state["cursor"] = cursor + len(chunk)

        now = time.time()
        start_ms = int((now - FLOW_WINDOW_SEC) * 1000)
        end_ms = int(now * 1000)

        flow_buffer: Dict[str, Dict[str, float]] = {}
        for batch in _chunked(chunk, SCAN_BATCH_SIZE):
            if time.time() - start > BUDGET:
                print("[WHALE] budget exceeded, stopping early")
                break
            tasks = [
                asyncio.create_task(
                    _fetch_agg_trades(
                        session,
                        pair["symbol"],
                        start_ms,
                        end_ms,
                        pair["market"],
                    )
                )
                for pair in batch
            ]
            results = await asyncio.gather(*tasks, return_exceptions=True)

            for pair, trades in zip(batch, results):
                if time.time() - start > BUDGET:
                    print("[WHALE] budget exceeded, stopping early")
                    break
                if isinstance(trades, Exception) or not trades:
                    continue
                flow = _calc_flow(trades)
                if not flow:
                    continue
                symbol = pair["symbol"]
                existing = flow_buffer.get(symbol, {"buy": 0.0, "sell": 0.0})
                existing["buy"] += flow["buy"]
                existing["sell"] += flow["sell"]
                flow_buffer[symbol] = existing

        top_in: List[tuple[str, float]] = []
        top_out: List[tuple[str, float]] = []
        for symbol, flow in flow_buffer.items():
            quote_volume = quote_volumes.get(symbol, 0.0)
            threshold = _threshold_for_volume(quote_volume)
            if flow["buy"] >= threshold:
                top_in.append((symbol, flow["buy"]))
            if flow["sell"] >= threshold:
                top_out.append((symbol, flow["sell"]))

        top_in.sort(key=lambda item: item[1], reverse=True)
        top_out.sort(key=lambda item: item[1], reverse=True)
        top_in = top_in[:5]
        top_out = top_out[:5]

        found = len(top_in) + len(top_out)
        sent = 0
        if found:
            lines = [
                f"🐳 Whale Flow ({FLOW_WINDOW_SEC} сек)",
                "",
                "ВХОД:",
            ]
            if top_in:
                lines.extend(
                    f"{_format_symbol(symbol)} — {_fmt_usd(amount)}"
                    for symbol, amount in top_in
                )
            else:
                lines.append("—")

            lines.extend(["", "ВЫХОД:"])
            if top_out:
                lines.extend(
                    f"{_format_symbol(symbol)} — {_fmt_usd(amount)}"
                    for symbol, amount in top_out
                )
            else:
                lines.append("—")

            text = "\n".join(lines)

            for chat_id in subscribers:
                try:
                    await bot.send_message(chat_id=chat_id, text=text)
                except Exception:
                    continue

            sent = 1

        cycle_sec = time.time() - cycle_start
        mark_ok(
            "whales_flow",
            extra=(
                f"checked={len(chunk)} found={found} sent={sent} cycle={cycle_sec:.1f}s"
            ),
        )
    finally:
        print("[WHALE] scan_once end")


async def whales_market_flow_worker(bot):
    await safe_worker_loop("whales_flow", lambda: whales_scan_once(bot))
