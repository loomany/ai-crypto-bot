from services.binance_futures import exchange_filters, round_qty_to_step


def calc_qty_by_risk(
    symbol: str,
    entry_price: float,
    stop_price: float,
    balance_usdt: float,
    risk_pct: float,
    leverage: float,
) -> dict:
    if stop_price <= 0:
        raise ValueError("STOP_PRICE_REQUIRED")
    if entry_price <= 0:
        raise ValueError("ENTRY_PRICE_INVALID")
    stop_pct = abs(entry_price - stop_price) / entry_price
    if stop_pct <= 0:
        raise ValueError("STOP_DISTANCE_INVALID")

    risk_usdt = float(balance_usdt) * (float(risk_pct) / 100.0)
    position_notional = risk_usdt / stop_pct
    qty_raw = position_notional / float(entry_price)

    filters = exchange_filters(symbol)
    step_size = float(filters.get("stepSize", 0.0) or 0.0)
    min_qty = float(filters.get("minQty", 0.0) or 0.0)
    qty = round_qty_to_step(qty_raw, step_size)
    if qty < min_qty:
        raise ValueError(f"QTY_LT_MIN_QTY ({qty} < {min_qty})")

    return {
        "qty": qty,
        "risk_usdt": risk_usdt,
        "stop_pct": stop_pct,
        "position_notional": position_notional,
        "qty_raw": qty_raw,
        "step_size": step_size,
        "min_qty": min_qty,
        "leverage": float(leverage),
    }
