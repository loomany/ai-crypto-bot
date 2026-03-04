import os
from typing import Any, Dict

import aiohttp

CRYPTOBOT_API_BASE = "https://pay.crypt.bot/api"


class CryptoBotAPIError(RuntimeError):
    pass


def _token() -> str:
    token = (os.getenv("CRYPTOBOT_TOKEN") or "").strip()
    if not token:
        raise CryptoBotAPIError("CRYPTOBOT_TOKEN is not set")
    return token


async def _post(method: str, payload: Dict[str, Any]) -> Dict[str, Any]:
    headers = {
        "Crypto-Pay-API-Token": _token(),
        "Content-Type": "application/json",
    }
    async with aiohttp.ClientSession() as session:
        async with session.post(
            f"{CRYPTOBOT_API_BASE}/{method}",
            json=payload,
            headers=headers,
            timeout=aiohttp.ClientTimeout(total=20),
        ) as response:
            data = await response.json(content_type=None)
    if not data.get("ok"):
        raise CryptoBotAPIError(f"CryptoBot API {method} failed: {data}")
    result = data.get("result")
    if result is None:
        raise CryptoBotAPIError(f"CryptoBot API {method} returned empty result")
    return result


async def create_invoice_usdt(user_id: int, plan: str, amount: str) -> Dict[str, Any]:
    result = await _post(
        "createInvoice",
        {
            "asset": "USDT",
            "amount": str(amount),
            "description": "Krypton AI подписка",
            "hidden_message": "После оплаты доступ откроется автоматически",
            "payload": f"sub:{plan}:user:{user_id}",
            "allow_comments": False,
            "allow_anonymous": False,
        },
    )
    return {
        "invoice_id": result.get("invoice_id") or result.get("id"),
        "pay_url": result.get("pay_url") or result.get("mini_app_invoice_url"),
        "status": result.get("status"),
        "amount": result.get("amount"),
        "asset": result.get("asset"),
        "payload": result.get("payload"),
    }


async def get_invoice(invoice_id: str) -> Dict[str, Any]:
    result = await _post("getInvoices", {"invoice_ids": [str(invoice_id)]})
    items = result.get("items") if isinstance(result, dict) else None
    if not items:
        raise CryptoBotAPIError(f"Invoice not found: {invoice_id}")
    return items[0]
