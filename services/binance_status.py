import asyncio
import os
from typing import Any

import i18n
from db import get_state
from services.binance_futures import (
    futures_balance_usdt,
    futures_open_orders,
    futures_positions,
)


def _trading_enabled() -> bool:
    return get_state("trading_enabled", os.getenv("TRADING_ENABLED", "0")) == "1"


def _status_visibility() -> str:
    return str(os.getenv("BINANCE_STATUS_VISIBILITY", "admin_only") or "admin_only").strip().lower()


async def _call_with_timeout(func: Any, *args: Any, timeout_sec: float = 12.0, **kwargs: Any) -> Any:
    return await asyncio.wait_for(
        asyncio.to_thread(func, *args, **kwargs),
        timeout=timeout_sec,
    )


def _safe_float(value: Any) -> float:
    try:
        return float(value or 0.0)
    except (TypeError, ValueError):
        return 0.0


async def build_binance_status_text(user_lang: str, *, user_id: int, admin_user_id: int) -> str:
    lines: list[str] = [i18n.t(user_lang, "binance_status_title")]

    is_testnet = str(os.getenv("BINANCE_TESTNET", "1") or "1").strip() == "1"
    if is_testnet:
        lines.append(i18n.t(user_lang, "binance_status_testnet"))
    else:
        lines.append(i18n.t(user_lang, "binance_status_mainnet_disabled"))

    lines.append(i18n.t(user_lang, "binance_trading_on") if _trading_enabled() else i18n.t(user_lang, "binance_trading_off"))
    lines.append("")

    visibility = _status_visibility()
    if visibility == "admin_only" and int(user_id) != int(admin_user_id):
        lines.append(i18n.t(user_lang, "binance_details_admin_only"))
        return "\n".join(lines)

    if not is_testnet:
        return "\n".join(lines)

    try:
        balance = await _call_with_timeout(futures_balance_usdt)
        positions = await _call_with_timeout(futures_positions)
        orders = await _call_with_timeout(futures_open_orders)
    except Exception as exc:
        lines.append(f"{i18n.t(user_lang, 'binance_error')} {str(exc)[:160]}")
        return "\n".join(lines)

    lines.append(f"{i18n.t(user_lang, 'binance_balance')} {float(balance):.4f}")
    lines.append("")

    lines.append(i18n.t(user_lang, "binance_positions"))
    visible_positions = [row for row in positions if _safe_float(row.get("positionAmt")) != 0.0]
    if not visible_positions:
        lines.append(f"• {i18n.t(user_lang, 'binance_none')}")
    else:
        for row in visible_positions[:10]:
            amt = _safe_float(row.get("positionAmt"))
            direction = "LONG" if amt > 0 else "SHORT"
            symbol = str(row.get("symbol") or "-")
            entry = _safe_float(row.get("entryPrice"))
            pnl = _safe_float(row.get("unRealizedProfit"))
            lines.append(f"• {symbol} | {direction} | {abs(amt)} | entry {entry:.6g} | PnL {pnl:+.4f}")

    lines.append("")
    lines.append(i18n.t(user_lang, "binance_orders"))
    if not orders:
        lines.append(f"• {i18n.t(user_lang, 'binance_none')}")
    else:
        allowlist = {
            item.strip().upper()
            for item in str(os.getenv("TRADING_ALLOWLIST", "") or "").split(",")
            if item.strip()
        }
        filtered_orders = [
            row for row in orders
            if not allowlist or str(row.get("symbol") or "").upper() in allowlist
        ]
        if not filtered_orders:
            lines.append(f"• {i18n.t(user_lang, 'binance_none')}")
        else:
            for row in filtered_orders[:10]:
                symbol = str(row.get("symbol") or "-")
                side = str(row.get("side") or "-")
                order_type = str(row.get("type") or "-")
                stop_price = _safe_float(row.get("stopPrice"))
                price = _safe_float(row.get("price"))
                status = str(row.get("status") or "-")
                price_part = f"stop {stop_price:.6g}" if stop_price > 0 else f"price {price:.6g}"
                lines.append(f"• {symbol} | {side} | {order_type} | {price_part} | {status}")

    return "\n".join(lines)
