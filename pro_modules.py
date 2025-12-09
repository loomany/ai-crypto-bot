import asyncio
from dataclasses import dataclass
from typing import Dict, List

from aiogram import Router, F
from aiogram.types import Message, ReplyKeyboardMarkup, KeyboardButton
from aiogram import Bot

from trading_core import analyze_orderflow
from ai_patterns import analyze_ai_patterns
from market_regime import get_market_regime

router = Router(name="pro_modules")


# ============================================================
# In-memory настройки PRO-модулей для каждого юзера
# ============================================================


@dataclass
class ProSettings:
    orderflow_enabled: bool = False
    smart_money_enabled: bool = False
    ai_patterns_enabled: bool = False
    market_regime_enabled: bool = False


class ProStorage:
    def __init__(self) -> None:
        self._settings: Dict[int, ProSettings] = {}

    def get(self, user_id: int) -> ProSettings:
        if user_id not in self._settings:
            self._settings[user_id] = ProSettings()
        return self._settings[user_id]

    def set_orderflow(self, user_id: int, enabled: bool) -> None:
        s = self.get(user_id)
        s.orderflow_enabled = enabled

    def set_smart_money(self, user_id: int, enabled: bool) -> None:
        s = self.get(user_id)
        s.smart_money_enabled = enabled

    def set_ai_patterns(self, user_id: int, enabled: bool) -> None:
        s = self.get(user_id)
        s.ai_patterns_enabled = enabled

    def set_market_regime(self, user_id: int, enabled: bool) -> None:
        s = self.get(user_id)
        s.market_regime_enabled = enabled

    def users_for_orderflow(self) -> List[int]:
        return [uid for uid, s in self._settings.items() if s.orderflow_enabled]

    def users_for_smart_money(self) -> List[int]:
        return [uid for uid, s in self._settings.items() if s.smart_money_enabled]

    def users_for_ai_patterns(self) -> List[int]:
        return [uid for uid, s in self._settings.items() if s.ai_patterns_enabled]

    def users_for_market_regime(self) -> List[int]:
        return [uid for uid, s in self._settings.items() if s.market_regime_enabled]


pro_storage = ProStorage()


# ============================================================
# Клавиатура PRO-модулей
# ============================================================


def get_pro_main_keyboard() -> ReplyKeyboardMarkup:
    kb = [
        [KeyboardButton(text="📊 Orderflow PRO: уведомления")],
        [KeyboardButton(text="💼 Smart Money (on-chain) уведомления")],
        [KeyboardButton(text="🧠 AI-паттерны: уведомления")],
        [KeyboardButton(text="🌍 Market Regime уведомления")],
        [KeyboardButton(text="⬅️ Главное меню")],
    ]
    return ReplyKeyboardMarkup(keyboard=kb, resize_keyboard=True)


def pro_menu_text() -> str:
    return (
        "🧠 PRO-модули бота:\n\n"
        "1) 📊 Orderflow PRO — алерты по дисбалансу агрессивных покупок/продаж,\n"
        "   всплескам объёма и активности китов.\n\n"
        "2) 💼 Smart Money — on-chain трекинг кошельков и крупных входов (Codex добавит API).\n\n"
        "3) 🧠 AI-паттерны — сигналы, когда на графике формируются сильные фигуры\n"
        "   (двойная вершина/дно, клин, треугольники и т.д.).\n\n"
        "4) 🌍 Market Regime — уведомления о смене макро-режима рынка (risk-on/risk-off)\n"
        "   по BTC и общему фону.\n\n"
        "В этом меню ты можешь включить или отключить уведомления по каждому модулю.\n"
    )


# ============================================================
# Хендлеры меню PRO-модулей
# ============================================================


@router.message(F.text == "🧠 PRO-модули")
async def open_pro_menu(message: Message):
    await message.answer(pro_menu_text(), reply_markup=get_pro_main_keyboard())


@router.message(F.text == "📊 Orderflow PRO: уведомления")
async def toggle_orderflow(message: Message):
    s = pro_storage.get(message.from_user.id)
    new_state = not s.orderflow_enabled
    pro_storage.set_orderflow(message.from_user.id, new_state)
    status = "включены" if new_state else "отключены"
    await message.answer(f"📊 Orderflow PRO: уведомления {status}.", reply_markup=get_pro_main_keyboard())


@router.message(F.text == "💼 Smart Money (on-chain) уведомления")
async def toggle_smart_money(message: Message):
    s = pro_storage.get(message.from_user.id)
    new_state = not s.smart_money_enabled
    pro_storage.set_smart_money(message.from_user.id, new_state)
    status = "включены" if new_state else "отключены"
    await message.answer(f"💼 Smart Money: уведомления {status}.", reply_markup=get_pro_main_keyboard())


@router.message(F.text == "🧠 AI-паттерны: уведомления")
async def toggle_ai_patterns(message: Message):
    s = pro_storage.get(message.from_user.id)
    new_state = not s.ai_patterns_enabled
    pro_storage.set_ai_patterns(message.from_user.id, new_state)
    status = "включены" if new_state else "отключены"
    await message.answer(f"🧠 AI-паттерны: уведомления {status}.", reply_markup=get_pro_main_keyboard())


@router.message(F.text == "🌍 Market Regime уведомления")
async def toggle_market_regime(message: Message):
    s = pro_storage.get(message.from_user.id)
    new_state = not s.market_regime_enabled
    pro_storage.set_market_regime(message.from_user.id, new_state)
    status = "включены" if new_state else "отключены"
    await message.answer(f"🌍 Market Regime: уведомления {status}.", reply_markup=get_pro_main_keyboard())


# ============================================================
# Воркеры PRO-модулей
# ============================================================


ORDERFLOW_SYMBOLS = ["BTCUSDT", "ETHUSDT", "SOLUSDT"]


async def orderflow_pro_worker(bot: Bot):
    """
    Фоновый воркер по Orderflow PRO.

    Codex:
      - доработать trading_core.analyze_orderflow так, чтобы он возвращал
        не только булевые флаги, но и силу сигнала, величину дисбаланса,
        крупные сделки и т.п.
    """
    await asyncio.sleep(5)
    last_side: Dict[str, str] = {}

    while True:
        try:
            user_ids = pro_storage.users_for_orderflow()
            if not user_ids:
                await asyncio.sleep(10)
                continue

            for symbol in ORDERFLOW_SYMBOLS:
                orderflow = await analyze_orderflow(symbol)
                side = None
                reason_parts = []

                if orderflow.get("orderflow_bullish"):
                    side = "LONG"
                    reason_parts.append("дисбаланс агрессивных покупок")
                elif orderflow.get("orderflow_bearish"):
                    side = "SHORT"
                    reason_parts.append("дисбаланс агрессивных продаж")

                if orderflow.get("whale_activity"):
                    reason_parts.append("заметна активность китов")

                if not side:
                    continue

                if last_side.get(symbol) == side:
                    continue
                last_side[symbol] = side

                reason = ", ".join(reason_parts) if reason_parts else "сильный дисбаланс ордерфлоу"
                text = (
                    f"📊 Orderflow PRO — {symbol}\n\n"
                    f"Сторона: {side}\n"
                    f"Причина: {reason}\n\n"
                    "Используй это как фильтр: не лезь против потока агрессивных ордеров."
                )

                for uid in user_ids:
                    try:
                        await bot.send_message(chat_id=uid, text=text)
                    except Exception:
                        continue

        except Exception as e:
            print(f"[orderflow_pro_worker] error: {e}")

        await asyncio.sleep(5)


async def smart_money_worker(bot: Bot):
    """
    Фоновый воркер Smart Money (on-chain).

    Codex:
      - Подключить on-chain API (Etherscan, Solscan, Nansen, Glassnode и т.п.).
      - Отслеживать крупные входы/выходы по топовым мемкоинам и мейджорам.
      - Возвращать список сигналов вида:
          { 'symbol': 'NEIROUSDT', 'side': 'BUY', 'usd': 250000, 'tx_url': '...' }
    """
    await asyncio.sleep(5)

    while True:
        try:
            user_ids = pro_storage.users_for_smart_money()
            if not user_ids:
                await asyncio.sleep(30)
                continue

            signals = []  # Codex: заменить на реальный вызов on-chain сканера

            for sig in signals:
                symbol = sig["symbol"]
                side = sig.get("side", "BUY")
                usd = sig.get("usd", 0)
                tx_url = sig.get("tx_url", "")

                side_text = "ПОКУПКА" if side.upper() == "BUY" else "ПРОДАЖА"
                text = (
                    f"💼 Smart Money — {symbol}\n\n"
                    f"Сторона: {side_text}\n"
                    f"Объём: ~{usd:,.0f} $\n"
                )
                if tx_url:
                    text += f"\nТранзакция: {tx_url}\n"

                text += "\nСледи за smart money: часто рынок идёт за ними."

                for uid in user_ids:
                    try:
                        await bot.send_message(chat_id=uid, text=text)
                    except Exception:
                        continue

        except Exception as e:
            print(f"[smart_money_worker] error: {e}")

        await asyncio.sleep(60)


async def ai_patterns_worker(bot: Bot):
    """
    Воркер AI-паттернов:
      - Использует ai_patterns.analyze_ai_patterns по BTC/ETH/SOL и др.
      - Шлёт уведомления, если найден сильный паттерн (strength >= 70).
    """
    await asyncio.sleep(5)
    symbols = ["BTCUSDT", "ETHUSDT", "SOLUSDT"]
    last_pattern: Dict[str, str] = {}

    from binance_client import get_required_candles  # локальный импорт, чтобы избежать циклов

    while True:
        try:
            user_ids = pro_storage.users_for_ai_patterns()
            if not user_ids:
                await asyncio.sleep(30)
                continue

            for symbol in symbols:
                candles = await get_required_candles(symbol)
                c1h = candles.get("1h") or []
                c15 = candles.get("15m") or []
                c5 = candles.get("5m") or []
                if not (c1h and c15 and c5):
                    continue

                info = await analyze_ai_patterns(symbol, c1h, c15, c5)
                strength = info.get("pattern_strength", 0)
                name = info.get("pattern_name") or ""
                direction = info.get("pattern_trend")  # bullish / bearish / neutral

                if strength < 70 or not direction or not name:
                    continue

                signature = f"{direction}:{name}"
                if last_pattern.get(symbol) == signature:
                    continue
                last_pattern[symbol] = signature

                dir_text = "разворот ВВЕРХ" if direction == "bullish" else "разворот ВНИЗ"
                text = (
                    f"🧠 AI-паттерн — {symbol}\n\n"
                    f"Фигура: {name}\n"
                    f"Направление: {dir_text}\n"
                    f"Сила сигнала: {strength}/100\n\n"
                    "Это не финсовет, но сильный паттерн на графике. Сверь с уровнем и риском."
                )

                for uid in user_ids:
                    try:
                        await bot.send_message(chat_id=uid, text=text)
                    except Exception:
                        continue

        except Exception as e:
            print(f"[ai_patterns_worker] error: {e}")

        await asyncio.sleep(60)


async def market_regime_worker(bot: Bot):
    """
    Воркер Market Regime:
      - 1–2 раза в день обновляет режим рынка и шлёт подписчикам.
    """
    await asyncio.sleep(5)
    last_regime = None

    while True:
        try:
            user_ids = pro_storage.users_for_market_regime()
            if not user_ids:
                await asyncio.sleep(60 * 30)
                continue

            info = await get_market_regime()
            regime = info.get("regime", "neutral")
            desc = info.get("description", "")
            if regime == last_regime:
                await asyncio.sleep(60 * 30)
                continue
            last_regime = regime

            emoji = "🟢" if regime == "risk_on" else "🔴" if regime == "risk_off" else "⚪️"
            name = {
                "risk_on": "Risk-ON (рынок готов к риску)",
                "risk_off": "Risk-OFF (осторожный режим)",
                "neutral": "Нейтральный режим",
            }.get(regime, "Нейтральный режим")

            text = f"🌍 Market Regime обновлён:\n\n{emoji} {name}\n\n{desc}"

            for uid in user_ids:
                try:
                    await bot.send_message(chat_id=uid, text=text)
                except Exception:
                    continue

        except Exception as e:
            print(f"[market_regime_worker] error: {e}")

        await asyncio.sleep(60 * 60)
