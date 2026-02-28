import os
import time
from decimal import Decimal, ROUND_DOWN
from typing import Any

from binance.client import Client


class TradingBlockedError(RuntimeError):
    pass


def _is_testnet() -> bool:
    return str(os.getenv("BINANCE_TESTNET", "1") or "1").strip() == "1"


def _trading_allowed() -> None:
    if not _is_testnet():
        raise TradingBlockedError("MAINNET DISABLED")


def get_futures_client() -> Client:
    api_key = os.getenv("BINANCE_API_KEY", "")
    api_secret = os.getenv("BINANCE_API_SECRET", "")
    client = Client(api_key=api_key, api_secret=api_secret)
    if _is_testnet():
        client.FUTURES_URL = "https://testnet.binancefuture.com/fapi"
    return client


def futures_ping() -> dict[str, Any]:
    client = get_futures_client()
    server = client.futures_time()
    return {"serverTime": int(server.get("serverTime", 0) or 0), "localTime": int(time.time() * 1000)}


def futures_balance_usdt() -> float:
    client = get_futures_client()
    balances = client.futures_account_balance()
    for row in balances:
        if str(row.get("asset", "")).upper() == "USDT":
            return float(row.get("availableBalance", 0.0) or 0.0)
    return 0.0


def futures_positions() -> list[dict[str, Any]]:
    client = get_futures_client()
    rows = client.futures_position_information()
    return [row for row in rows if float(row.get("positionAmt", 0.0) or 0.0) != 0.0]


def futures_open_orders(symbol: str | None = None) -> list[dict[str, Any]]:
    client = get_futures_client()
    if symbol:
        return client.futures_get_open_orders(symbol=symbol)
    return client.futures_get_open_orders()


def futures_set_leverage(symbol: str, leverage: int) -> dict[str, Any]:
    _trading_allowed()
    client = get_futures_client()
    return client.futures_change_leverage(symbol=symbol, leverage=int(leverage))


def futures_set_margin_type(symbol: str, margin_type: str = "ISOLATED") -> dict[str, Any]:
    _trading_allowed()
    client = get_futures_client()
    return client.futures_change_margin_type(symbol=symbol, marginType=margin_type)


def exchange_filters(symbol: str) -> dict[str, float]:
    client = get_futures_client()
    info = client.futures_exchange_info()
    normalized = str(symbol or "").upper()
    for row in info.get("symbols", []):
        if str(row.get("symbol", "")).upper() != normalized:
            continue
        step_size = 0.0
        min_qty = 0.0
        for flt in row.get("filters", []):
            if flt.get("filterType") == "LOT_SIZE":
                step_size = float(flt.get("stepSize", 0.0) or 0.0)
                min_qty = float(flt.get("minQty", 0.0) or 0.0)
                break
        return {"stepSize": step_size, "minQty": min_qty}
    raise ValueError(f"Unknown symbol: {symbol}")


def round_qty_to_step(qty: float, step_size: float) -> float:
    if step_size <= 0:
        return float(qty)
    d_qty = Decimal(str(qty))
    d_step = Decimal(str(step_size))
    rounded = d_qty.quantize(d_step, rounding=ROUND_DOWN)
    return float(rounded)


def place_market_order(
    symbol: str,
    side: str,
    qty: float,
    reduce_only: bool = False,
    client_order_id: str | None = None,
) -> dict[str, Any]:
    _trading_allowed()
    client = get_futures_client()
    payload: dict[str, Any] = {
        "symbol": symbol,
        "side": side,
        "type": "MARKET",
        "quantity": qty,
        "reduceOnly": "true" if reduce_only else "false",
    }
    if client_order_id:
        payload["newClientOrderId"] = client_order_id
    return client.futures_create_order(**payload)


def close_position_market(symbol: str) -> dict[str, Any]:
    _trading_allowed()
    client = get_futures_client()
    positions = client.futures_position_information(symbol=symbol)
    if not positions:
        raise ValueError(f"No positions for {symbol}")
    amt = float(positions[0].get("positionAmt", 0.0) or 0.0)
    if amt == 0.0:
        return {"status": "NO_POSITION", "symbol": symbol}
    side = "SELL" if amt > 0 else "BUY"
    filters = exchange_filters(symbol)
    qty = round_qty_to_step(abs(amt), filters.get("stepSize", 0.0))
    if qty <= 0:
        return {"status": "NO_POSITION", "symbol": symbol}
    return place_market_order(symbol=symbol, side=side, qty=qty, reduce_only=True)
