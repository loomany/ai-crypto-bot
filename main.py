import asyncio
import os

from aiogram import Bot, Dispatcher, F
from aiogram.types import (
    Message,
    ReplyKeyboardMarkup,
    KeyboardButton,
)
from aiogram.filters import CommandStart
from dotenv import load_dotenv

from market_data import get_coin_analysis


# ===== ЗАГРУЖАЕМ НАСТРОЙКИ =====

def load_settings() -> str:
    load_dotenv()
    bot_token = os.getenv("BOT_TOKEN")

    if not bot_token:
        raise ValueError("Нет BOT_TOKEN в .env файле")

    return bot_token


# ===== КНОПКИ МЕНЮ =====

def main_menu_keyboard() -> ReplyKeyboardMarkup:
    kb = [
        [
            KeyboardButton(text="📊 Анализ монеты"),
            KeyboardButton(text="🎯 AI-сигналы"),
        ],
        [
            KeyboardButton(text="🚀 Pump Detector"),
            KeyboardButton(text="🧠 ML прогноз"),
        ],
        [
            KeyboardButton(text="⚠️ Безопасность сделки"),
            KeyboardButton(text="ℹ️ Обучение терминам"),
        ],
    ]
    return ReplyKeyboardMarkup(keyboard=kb, resize_keyboard=True)


# ===== СОЗДАЁМ БОТА =====

BOT_TOKEN = load_settings()
bot = Bot(BOT_TOKEN)
dp = Dispatcher()
waiting_for_symbol: set[int] = set()


# ===== ХЭНДЛЕРЫ =====

@dp.message(CommandStart())
async def cmd_start(message: Message):
    text = (
        "Привет! Я будущий AI-крипто бот 🚀\n\n"
        "Сейчас я в режиме разработки. Меню уже работает.\n"
        "Нажми кнопку ниже 👇"
    )
    await message.answer(text, reply_markup=main_menu_keyboard())


@dp.message(F.text == "📊 Анализ монеты")
async def analyze_coin(message: Message):
    waiting_for_symbol.add(message.chat.id)

    await message.answer(
        "📊 *Анализ монеты*\n\n"
        "Введи тикер монеты (например: BTC, ETH, SOL)\n"
        "_Можно писать: BTC или BTCUSDT_",
        parse_mode="Markdown",
    )


@dp.message(F.text == "🎯 AI-сигналы")
async def ai_signals(message: Message):
    await message.answer("Здесь будут AI-сигналы (Buy/Sell, TP/SL).")


@dp.message(F.text == "🚀 Pump Detector")
async def pump_detector(message: Message):
    await message.answer("Здесь будет Pump Detector.")


@dp.message(F.text == "🧠 ML прогноз")
async def ml_forecast(message: Message):
    await message.answer("Здесь будет ML-прогноз на 1ч/4ч/сутки.")


@dp.message(F.text == "⚠️ Безопасность сделки")
async def safety(message: Message):
    await message.answer("Здесь будет риск-менеджмент и подсказки.")


@dp.message(F.text == "ℹ️ Обучение терминам")
async def education(message: Message):
    await message.answer("Здесь будет справочник: RSI, MACD, orderflow и т.д.")


def _trend_to_text(trend: str) -> str:
    if trend == "bullish":
        return "восходящий (бычий) 🚀"
    if trend == "bearish":
        return "нисходящий (медвежий) 🐻"
    return "флет (боковик)"


def _rsi_zone_text(rsi: float) -> str:
    if rsi < 30:
        return "сильная перепроданность"
    if rsi < 40:
        return "зона перепроданности"
    if rsi <= 60:
        return "нормальная зона"
    if rsi <= 70:
        return "лёгкая перекупленность"
    return "сильная перекупленность"


def _volume_text(desc: str) -> str:
    if desc == "high":
        return "выше среднего, растут 🔥"
    if desc == "low":
        return "ниже среднего"
    return "около среднего"


def _macd_text(signal: str) -> str:
    if signal == "bullish":
        return "бычий (подтверждает тренд)"
    if signal == "bearish":
        return "медвежий (ослабляет тренд)"
    return "нейтральный"


@dp.message()
async def process_symbol(message: Message):
    chat_id = message.chat.id

    if chat_id not in waiting_for_symbol:
        return

    waiting_for_symbol.remove(chat_id)

    symbol = (message.text or "").strip().upper()
    if not symbol:
        await message.answer("Я ожидал тикер монеты. Попробуй ещё раз нажать «📊 Анализ монеты».")
        return

    if not symbol.endswith("USDT"):
        symbol_pair = symbol + "USDT"
    else:
        symbol_pair = symbol

    await message.answer("⏳ Делаю анализ по Binance, пару секунд...")

    analysis = await get_coin_analysis(symbol_pair)

    if not analysis:
        await message.answer("❌ Не удалось получить данные. Проверь тикер (например: BTC, ETH, SOL).")
        return

    price = analysis["price"]
    change = analysis["change_24h"]
    emoji_change = "📈" if change >= 0 else "📉"

    tf = analysis["tf"]
    levels = analysis["levels"]
    risk = analysis["risk"]

    tf4 = tf.get("4h", {})
    tf1 = tf.get("1h", {})
    tf15 = tf.get("15m", {})

    # 4h
    trend_4h = _trend_to_text(tf4.get("trend", "neutral"))
    rsi_4h = tf4.get("rsi", 50.0)
    rsi_4h_txt = _rsi_zone_text(rsi_4h)

    # 1h
    trend_1h = _trend_to_text(tf1.get("trend", "neutral"))
    rsi_1h = tf1.get("rsi", 50.0)
    rsi_1h_txt = _rsi_zone_text(rsi_1h)
    vol_1h_txt = _volume_text(tf1.get("volume_desc", "normal"))
    macd_1h_txt = _macd_text(tf1.get("macd", "neutral"))

    # 15m
    rsi_15 = tf15.get("rsi", 50.0)
    rsi_15_txt = _rsi_zone_text(rsi_15)
    trend_15 = _trend_to_text(tf15.get("trend", "neutral"))

    support = levels["support"]
    resistance = levels["resistance"]
    entry_low = levels["entry_low"]
    entry_high = levels["entry_high"]
    tp1 = levels["tp1"]
    tp2 = levels["tp2"]
    sl = levels["sl"]

    # Вердикт по-человечески (очень упрощённо)
    verdict_lines = []
    if tf4.get("trend") == "bullish":
        verdict_lines.append("Глобально монета в устойчивом восходящем тренде.")
    elif tf4.get("trend") == "bearish":
        verdict_lines.append("Глобально монета под давлением, тренд скорее нисходящий.")
    else:
        verdict_lines.append("Глобально тренд больше похож на боковой.")

    if rsi_15 >= 65:
        verdict_lines.append("На мелком таймфрейме есть признаки перегретости — возможен локальный откат.")
    elif rsi_15 <= 35:
        verdict_lines.append("Локально монета перепродана — возможен отскок.")
    else:
        verdict_lines.append("Локально ситуация по RSI близка к нормальной зоне.")

    verdict_text = " ".join(verdict_lines)

    risk_text = {
        "low": "низкий",
        "medium": "средний",
        "high": "повышенный",
    }.get(risk, "средний")

    text = (
        f"📊 Анализ {symbol_pair}\n\n"
        f"💰 Цена: {price:.2f} USDT\n"
        f"{emoji_change} Изм. 24ч: {change:+.2f}%\n\n"
        f"🔭 Глобально (4ч):\n"
        f"• Тренд: {trend_4h}\n"
        f"• RSI: {rsi_4h:.1f} — {rsi_4h_txt}\n"
        f"• Уровни:\n"
        f"  • Поддержка: {support:.2f}\n"
        f"  • Сопротивление: {resistance:.2f}\n\n"
        f"⏱ Основной тренд (1ч):\n"
        f"• Тренд: {trend_1h}\n"
        f"• RSI: {rsi_1h:.1f} — {rsi_1h_txt}\n"
        f"• Объёмы: {vol_1h_txt}\n"
        f"• MACD: {macd_1h_txt}\n\n"
        f"🕒 Локально (15м):\n"
        f"• Тренд: {trend_15}\n"
        f"• RSI: {rsi_15:.1f} — {rsi_15_txt}\n"
        f"• Возможна коррекция к зоне {entry_low:.2f}–{entry_high:.2f}\n\n"
        f"🧠 Вердикт:\n"
        f"{verdict_text}\n\n"
        f"🎯 Пример уровней для сделки (для обучения, не финсовет):\n"
        f"• TP1: {tp1:.2f}\n"
        f"• TP2: {tp2:.2f}\n"
        f"• SL: {sl:.2f}\n\n"
        f"⚠️ Риск сделки: {risk_text}.\n"
        f"Источник данных: Binance"
    )

    await message.answer(text)


@dp.message()
async def fallback(message: Message):
    await message.answer("Нажми кнопку в меню — пока я понимаю только их.")


# ===== ТОЧКА ВХОДА =====

async def main():
    print("Бот запущен!")
    await dp.start_polling(bot)


if __name__ == "__main__":
    asyncio.run(main())
