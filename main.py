import asyncio
import os
import sqlite3
import time
from contextlib import suppress
from datetime import datetime
from typing import Any, Dict, List, Tuple

from aiogram import Bot, Dispatcher, F
from aiogram.types import (
    Message,
    ReplyKeyboardMarkup,
    KeyboardButton,
)
from aiogram.filters import CommandStart
from dotenv import load_dotenv

from market_data import get_coin_analysis
from pump_detector import scan_pumps, format_pump_message
from pump_db import add_pump_subscriber, remove_pump_subscriber, get_pump_subscribers
from signals import scan_market


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
            KeyboardButton(text="ℹ️ Обучение терминам"),
        ],
    ]
    return ReplyKeyboardMarkup(keyboard=kb, resize_keyboard=True)


def pump_menu_keyboard() -> ReplyKeyboardMarkup:
    kb = [
        [KeyboardButton(text="🔥 Пампы сейчас")],
        [KeyboardButton(text="🔔 Включить авто-пампы")],
        [KeyboardButton(text="🚫 Отключить авто-пампы")],
        [KeyboardButton(text="⬅️ Назад в главное меню")],
    ]
    return ReplyKeyboardMarkup(keyboard=kb, resize_keyboard=True)


def ai_signals_keyboard() -> ReplyKeyboardMarkup:
    kb = [
        [KeyboardButton(text="🔥 Активные сигналы сейчас")],
        [
            KeyboardButton(text="🔔 Включить авто-сигналы"),
            KeyboardButton(text="🚫 Отключить авто-сигналы"),
        ],
        [KeyboardButton(text="⬅️ Главное меню")],
    ]
    return ReplyKeyboardMarkup(keyboard=kb, resize_keyboard=True)


# ===== РАБОТА С ПОДПИСКАМИ =====


def init_db():
    conn = sqlite3.connect(DB_PATH)
    try:
        conn.execute(
            "CREATE TABLE IF NOT EXISTS ai_signals_subscribers (chat_id INTEGER PRIMARY KEY)"
        )
        conn.commit()
    finally:
        conn.close()


def add_subscription(chat_id: int) -> bool:
    conn = sqlite3.connect(DB_PATH)
    try:
        cur = conn.cursor()
        cur.execute(
            "INSERT OR IGNORE INTO ai_signals_subscribers (chat_id) VALUES (?)",
            (chat_id,),
        )
        conn.commit()
        return cur.rowcount > 0
    finally:
        conn.close()


def remove_subscription(chat_id: int) -> bool:
    conn = sqlite3.connect(DB_PATH)
    try:
        cur = conn.cursor()
        cur.execute("DELETE FROM ai_signals_subscribers WHERE chat_id = ?", (chat_id,))
        conn.commit()
        return cur.rowcount > 0
    finally:
        conn.close()


def list_subscriptions() -> List[int]:
    conn = sqlite3.connect(DB_PATH)
    try:
        cur = conn.cursor()
        cur.execute("SELECT chat_id FROM ai_signals_subscribers")
        return [row[0] for row in cur.fetchall()]
    finally:
        conn.close()


# ===== СОЗДАЁМ БОТА =====

BOT_TOKEN = load_settings()
bot = Bot(BOT_TOKEN)
dp = Dispatcher()
waiting_for_symbol: set[int] = set()
signal_cache: Dict[Tuple[str, str, float, float], float] = {}

DB_PATH = "ai_signals.db"
# Сканируем рынок каждые 30 секунд, чтобы рассылка была оперативной
AI_SCAN_INTERVAL = 30  # seconds


# ===== ХЭНДЛЕРЫ =====

@dp.message(CommandStart())
async def cmd_start(message: Message):
    waiting_for_symbol.discard(message.chat.id)
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
async def ai_signals_menu(message: Message):
    waiting_for_symbol.discard(message.chat.id)
    await message.answer(
        "🎯 AI-сигналы\n\nВыбери режим:\n1) 🔥 Активные сигналы сейчас\n"
        "2) 🔔 Включить авто-сигналы\n3) 🚫 Отключить авто-сигналы",
        reply_markup=ai_signals_keyboard(),
    )


@dp.message(F.text == "🔥 Активные сигналы сейчас")
async def ai_signals_now(message: Message):
    waiting_for_symbol.discard(message.chat.id)
    await message.answer("⏳ Сканируем рынок Binance по USDT-парам, подожди...")

    signals = await scan_market()
    if not signals:
        await message.answer("Сейчас нет сетапов с высокой вероятностью (score >= 80).")
        return

    signals = sorted(signals, key=lambda s: s.get("score", 0), reverse=True)
    for signal in signals[:10]:
        await message.answer(_format_signal(signal))


@dp.message(F.text == "🔔 Включить авто-сигналы")
async def ai_signals_subscribe(message: Message):
    waiting_for_symbol.discard(message.chat.id)
    is_new = add_subscription(message.chat.id)
    if is_new:
        await message.answer(
            "Готово! Ты подписан на авто-рассылку AI-сигналов.",
            reply_markup=ai_signals_keyboard(),
        )
    else:
        await message.answer(
            "Подписка уже активна. Будем присылать новые сигналы автоматически.",
            reply_markup=ai_signals_keyboard(),
        )


@dp.message(F.text == "🚫 Отключить авто-сигналы")
async def ai_signals_unsubscribe(message: Message):
    waiting_for_symbol.discard(message.chat.id)
    removed = remove_subscription(message.chat.id)
    if removed:
        await message.answer(
            "Авто-сигналы отключены. Возвращайся, когда потребуется!",
            reply_markup=ai_signals_keyboard(),
        )
    else:
        await message.answer(
            "У тебя не было активной подписки.", reply_markup=ai_signals_keyboard()
        )


@dp.message(F.text == "🚀 Pump Detector")
async def pump_detector_entry(message: Message):
    waiting_for_symbol.discard(message.chat.id)
    await message.answer(
        "🚀 Pump Detector\n\n"
        "Я ищу реальные пампы по всем монетам Binance (USDT).\n"
        "Выбери режим:",
        reply_markup=pump_menu_keyboard(),
    )


@dp.message(F.text == "🔥 Пампы сейчас")
async def pumps_now(message: Message):
    waiting_for_symbol.discard(message.chat.id)
    await message.answer("⏳ Ищу пампы по всем монетам Binance...")
    signals = await scan_pumps()
    if not signals:
        await message.answer("Сейчас явных пампов не найдено.")
        return

    signals = sorted(signals, key=lambda s: s["change_1m"], reverse=True)[:5]

    for sig in signals:
        await message.answer(
            format_pump_message(sig),
            parse_mode="Markdown",
        )


@dp.message(F.text == "🔔 Включить авто-пампы")
async def subscribe_pumps(message: Message):
    waiting_for_symbol.discard(message.chat.id)
    add_pump_subscriber(message.chat.id)
    await message.answer(
        "✅ Авто-оповещения Pump Detector включены.\n"
        "Я буду присылать пампы по монетам Binance, когда найду их.",
        reply_markup=pump_menu_keyboard(),
    )


@dp.message(F.text == "🚫 Отключить авто-пампы")
async def unsubscribe_pumps(message: Message):
    waiting_for_symbol.discard(message.chat.id)
    remove_pump_subscriber(message.chat.id)
    await message.answer(
        "⭕ Авто-оповещения Pump Detector выключены.",
        reply_markup=pump_menu_keyboard(),
    )


@dp.message(F.text == "⬅️ Назад в главное меню")
async def back_to_main_menu(message: Message):
    waiting_for_symbol.discard(message.chat.id)
    await message.answer("Главное меню:", reply_markup=main_menu_keyboard())


@dp.message(F.text == "⬅️ Главное меню")
async def back_to_main(message: Message):
    waiting_for_symbol.discard(message.chat.id)
    await message.answer("Возвращаемся в главное меню.", reply_markup=main_menu_keyboard())


@dp.message(F.text == "ℹ️ Обучение терминам")
async def education(message: Message):
    waiting_for_symbol.discard(message.chat.id)
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


def _remember_signal(signal: Dict[str, Any], ttl: int = 3600) -> bool:
    key = (
        signal["symbol"],
        signal.get("direction", "long"),
        round(signal["entry_zone"][0], 4),
        round(signal["entry_zone"][1], 4),
    )
    now = asyncio.get_event_loop().time()
    expires_at = now + ttl

    # cleanup
    for cached_key, exp in list(signal_cache.items()):
        if exp <= now:
            del signal_cache[cached_key]

    if key in signal_cache:
        return False

    signal_cache[key] = expires_at
    return True


def _format_signal(signal: Dict[str, Any]) -> str:
    entry_low, entry_high = signal["entry_zone"]
    valid_until = datetime.fromtimestamp(signal["valid_until"]).strftime(
        "%Y-%m-%d %H:%M"
    )
    direction_text = "ЛОНГ" if signal.get("direction") == "long" else "ШОРТ"

    text = (
        "🔔 AI-сигнал (intraday)\n\n"
        f"Монета: {signal['symbol']}\n"
        f"Тип: {direction_text}\n\n"
        f"Зона входа: {entry_low:.4f}–{entry_high:.4f}\n"
        f"Стоп (SL): {signal['sl']:.4f}\n"
        "Цели:\n"
        f"• TP1: {signal['tp1']:.4f}\n"
        f"• TP2: {signal['tp2']:.4f}\n\n"
        f"Оценка сигнала: {signal['score']}/100\n"
        f"Актуален до: {valid_until}\n\n"
        "Кратко:\n"
        f"{signal['reason']}\n\n"
        "⚠️ Бот не знает твоего депозита и не даёт размер позиции.\n"
        "Решение по объёму входа принимаешь сам.\n"
        "Источник данных: Binance"
    )
    return text


async def send_signal_to_all(signal_dict: Dict[str, Any]):
    """
    Отправляет сигнал всем подписчикам без блокировки event loop.
    """
    subscribers = list_subscriptions()
    if not subscribers:
        return

    if not _remember_signal(signal_dict):
        return

    text = _format_signal(signal_dict)

    tasks = []
    for chat_id in subscribers:
        tasks.append(asyncio.create_task(bot.send_message(chat_id, text)))

    # Выполняем отправки параллельно и логируем ошибки
    results = await asyncio.gather(*tasks, return_exceptions=True)
    for chat_id, res in zip(subscribers, results):
        if isinstance(res, Exception):
            print(f"[ai_signals] Failed to send to {chat_id}: {res}")


async def pump_worker(bot: Bot):
    """
    Периодически сканирует рынок и рассылает авто-пампы подписчикам.
    """
    last_sent: dict[str, int] = {}

    while True:
        try:
            subscribers = get_pump_subscribers()
            if not subscribers:
                await asyncio.sleep(15)
                continue

            signals = await scan_pumps()
            now_min = int(time.time() // 60)

            for sig in signals:
                symbol = sig["symbol"]

                if last_sent.get(symbol) == now_min:
                    continue

                last_sent[symbol] = now_min
                text = format_pump_message(sig)

                for chat_id in subscribers:
                    try:
                        await bot.send_message(chat_id, text, parse_mode="Markdown")
                    except Exception:
                        continue

        except Exception:
            await asyncio.sleep(10)

        await asyncio.sleep(10)


async def signals_worker():
    while True:
        try:
            signals = await scan_market()
            print("SCAN OK", len(signals))
            for signal in signals:
                await send_signal_to_all(signal)
        except Exception as e:
            print(f"[ai_signals] Worker error: {e}")
        await asyncio.sleep(AI_SCAN_INTERVAL)


@dp.message()
async def process_symbol(message: Message):
    chat_id = message.chat.id

    if chat_id not in waiting_for_symbol:
        return

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
    init_db()
    signals_task = asyncio.create_task(signals_worker())
    pump_task = asyncio.create_task(pump_worker(bot))
    try:
        await dp.start_polling(bot)
    finally:
        signals_task.cancel()
        with suppress(asyncio.CancelledError):
            await signals_task
        pump_task.cancel()
        with suppress(asyncio.CancelledError):
            await pump_task


if __name__ == "__main__":
    asyncio.run(main())
