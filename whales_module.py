import asyncio
import time
from dataclasses import dataclass
from typing import Any, Dict, List

import aiohttp

FAPI_BASE_URL = "https://fapi.binance.com"
TOP5_SYMBOLS = ["BTCUSDT", "ETHUSDT", "SOLUSDT", "BNBUSDT", "XRPUSDT"]


@dataclass
class WhaleSignal:
    symbol: str
    alert: bool
    side: str
    whale_buy_usd: float
    whale_sell_usd: float
    orderflow_imbalance_pct: float
    cvd_direction: str
    oi_change_pct: float
    funding: float
    explanation: str


class WhaleStorage:
    def __init__(self):
        self.whales_notifications: dict[int, bool] = {}
        self._last_oi: dict[str, float] = {}

    def set_notifications(self, user_id: int, enabled: bool) -> None:
        self.whales_notifications[user_id] = enabled

    def get_whales_users(self) -> List[int]:
        return [uid for uid, enabled in self.whales_notifications.items() if enabled]

    def is_enabled(self, user_id: int) -> bool:
        return self.whales_notifications.get(user_id, False)

    def remember_open_interest(self, symbol: str, current_oi: float) -> float:
        prev_oi = self._last_oi.get(symbol)
        self._last_oi[symbol] = current_oi
        if prev_oi is None or prev_oi == 0:
            return 0.0
        return (current_oi - prev_oi) / prev_oi * 100


storage = WhaleStorage()


async def _fetch_json(session: aiohttp.ClientSession, url: str, params: Dict[str, Any] | None = None) -> Dict[str, Any] | List[Dict[str, Any]] | None:
    try:
        async with session.get(url, params=params, timeout=10) as resp:
            resp.raise_for_status()
            return await resp.json()
    except Exception as e:
        print(f"[whales] fetch error {url}: {e}")
        return None


def _calc_orderflow_imbalance(buy: float, sell: float) -> float:
    total = buy + sell
    if total <= 0:
        return 0.0
    return (buy - sell) / total * 100


def _format_usd(value: float) -> str:
    if value >= 1_000_000:
        return f"{value / 1_000_000:.1f}M$"
    if value >= 1_000:
        return f"{value / 1_000:.1f}k$"
    return f"{value:.0f}$"


async def analyze_whales(symbol: str) -> Dict[str, Any]:
    now_ms = int(time.time() * 1000)
    start_ms = now_ms - 30_000

    whale_buy_usd = 0.0
    whale_sell_usd = 0.0
    taker_buy_volume = 0.0
    taker_sell_volume = 0.0
    cvd_direction = "neutral"
    oi_change_pct = 0.0
    funding_rate = 0.0

    async with aiohttp.ClientSession() as session:
        trades = await _fetch_json(
            session,
            f"{FAPI_BASE_URL}/fapi/v1/aggTrades",
            params={"symbol": symbol, "startTime": start_ms, "endTime": now_ms},
        )
        if trades:
            for t in trades:
                price = float(t.get("p", 0))
                qty = float(t.get("q", 0))
                usd = price * qty
                if t.get("m"):
                    whale_sell_usd += usd
                else:
                    whale_buy_usd += usd

        taker_ratio = await _fetch_json(
            session,
            f"{FAPI_BASE_URL}/futures/data/takerlongshortRatio",
            params={"symbol": symbol, "period": "5m", "limit": 1},
        )
        if taker_ratio and isinstance(taker_ratio, list):
            last = taker_ratio[-1]
            taker_buy_volume = float(last.get("buyVol", 0))
            taker_sell_volume = float(last.get("sellVol", 0))
            cvd_direction = "up" if float(last.get("buySellRatio", 1)) > 1 else "down"

        oi_resp = await _fetch_json(
            session,
            f"{FAPI_BASE_URL}/fapi/v1/openInterest",
            params={"symbol": symbol},
        )
        if oi_resp and isinstance(oi_resp, dict):
            current_oi = float(oi_resp.get("openInterest", 0))
            oi_change_pct = storage.remember_open_interest(symbol, current_oi)

        funding_resp = await _fetch_json(
            session,
            f"{FAPI_BASE_URL}/fapi/v1/premiumIndex",
            params={"symbol": symbol},
        )
        if funding_resp and isinstance(funding_resp, dict):
            funding_rate = float(funding_resp.get("lastFundingRate", 0.0))

    orderflow_imbalance_pct = _calc_orderflow_imbalance(taker_buy_volume, taker_sell_volume)

    buy_alert = (
        whale_buy_usd > 300_000
        and taker_buy_volume > taker_sell_volume * 1.2
        and cvd_direction == "up"
        and oi_change_pct >= 3
        and funding_rate <= 0.01
    )

    sell_alert = (
        whale_sell_usd > 300_000
        and taker_sell_volume > taker_buy_volume
        and cvd_direction == "down"
        and oi_change_pct <= -4
        and funding_rate > 0
    )

    alert = buy_alert or sell_alert
    side = "BUY" if buy_alert else "SELL" if sell_alert else "NEUTRAL"

    explanation_lines = [
        f"Крупные сделки: BUY {_format_usd(whale_buy_usd)}, SELL {_format_usd(whale_sell_usd)}",
        f"Orderflow: {orderflow_imbalance_pct:+.1f}% в сторону покупок",
        f"CVD: {'растёт' if cvd_direction == 'up' else 'падает' if cvd_direction == 'down' else 'нет данных'}",
        f"OI: {oi_change_pct:+.2f}%",
        f"Funding: {funding_rate:+.4f}",
    ]

    return {
        "alert": alert,
        "side": side,
        "whale_buy_usd": whale_buy_usd,
        "whale_sell_usd": whale_sell_usd,
        "orderflow_imbalance_pct": orderflow_imbalance_pct,
        "cvd_direction": cvd_direction,
        "oi_change_pct": oi_change_pct,
        "funding": funding_rate,
        "explanation": " | ".join(explanation_lines),
    }


def format_whale_alert(symbol: str, data: Dict[str, Any]) -> str:
    side = data.get("side", "NEUTRAL")
    is_buy = side == "BUY"
    direction_emoji = "BUY" if is_buy else "SELL"
    whale_amount = data.get("whale_buy_usd") if is_buy else data.get("whale_sell_usd")
    orderflow_pct = data.get("orderflow_imbalance_pct", 0.0)
    funding = data.get("funding", 0.0)
    oi_change = data.get("oi_change_pct", 0.0)
    cvd_direction = data.get("cvd_direction", "neutral")

    cvd_text = "растёт" if cvd_direction == "up" else "падает"
    orderflow_sign = "+" if orderflow_pct >= 0 else ""

    main_block = (
        f"🐳 WHALES {direction_emoji} — {symbol}\n"
        f"Крупные сделки: {'BUY' if is_buy else 'SELL'} {_format_usd(whale_amount)}\n"
        f"Orderflow: {orderflow_sign}{orderflow_pct:.0f}% в пользу {'покупок' if orderflow_pct >= 0 else 'продаж'}\n"
        f"CVD: {cvd_text}\n"
        f"Open Interest: {oi_change:+.2f}%\n"
        f"Funding: {funding:+.4f}\n\n"
        "Что это значит?\n"
        f"• {'Крупные игроки накапливают позицию' if is_buy else 'Крупный капитал выходит из позиции'}\n"
        f"• {'Вероятность роста повышена' if is_buy else 'Риск разворота вниз высокий'}\n"
        f"• {'При подтверждении — возможна точка входа в LONG' if is_buy else 'Можно готовиться к SHORT или выходу из LONG'}\n\n"
        "Что это значит для тебя?\n"
        f"• {'киты накапливают позицию' if is_buy else 'киты выходят'}\n"
        f"• {'тренд может развернуться вверх' if is_buy else 'рынок теряет поддержку'}\n"
        f"• {'шанс сильного импульса' if is_buy else 'может начаться дамп'}\n"
        f"• {'хороший момент следить за LONG' if is_buy else 'возможно открыть SHORT или закрыть LONG'}"
    )

    return main_block


async def whales_realtime_worker(bot):
    while True:
        try:
            for symbol in TOP5_SYMBOLS:
                data = await analyze_whales(symbol)
                if data.get("alert"):
                    for user in storage.get_whales_users():
                        try:
                            await bot.send_message(user, format_whale_alert(symbol, data))
                        except Exception as e:
                            print(f"[whales] failed to send to {user}: {e}")
            await asyncio.sleep(2)
        except Exception as e:
            print(f"[whales] worker error: {e}")
            await asyncio.sleep(3)
