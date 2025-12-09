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

from coin_info import get_coin_description
from btc_module import (
    router as btc_router,
    btc_realtime_signal_worker,
    get_btc_main_keyboard,
)
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
        [KeyboardButton(text="₿ BTC (intraday)")],
        [KeyboardButton(text="🚀 Pump Detector")],
    ]
    return ReplyKeyboardMarkup(keyboard=kb, resize_keyboard=True)


def pump_menu_keyboard() -> ReplyKeyboardMarkup:
    kb = [
        [KeyboardButton(text="🔔 Включить авто-пампы")],
        [KeyboardButton(text="🚫 Отключить авто-пампы")],
        [KeyboardButton(text="⬅️ Назад в главное меню")],
    ]
    return ReplyKeyboardMarkup(keyboard=kb, resize_keyboard=True)


def ai_signals_keyboard() -> ReplyKeyboardMarkup:
    kb = [
        [KeyboardButton(text="🔔 Включить авто-сигналы")],
        [KeyboardButton(text="🚫 Отключить авто-сигналы")],
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
dp.include_router(btc_router)
waiting_for_symbol: set[int] = set()
signal_cache: Dict[Tuple[str, str, float, float], float] = {}
LAST_SIGNALS: Dict[str, Dict[str, Any]] = {}
COOLDOWN_PER_SYMBOL = 60 * 60 * 3  # 3 hours
ENTRY_DUP_THRESHOLD = 0.1  # percent

DB_PATH = "ai_signals.db"
# Сканируем рынок каждые 30 секунд, чтобы рассылка была оперативной
AI_SCAN_INTERVAL = 30  # seconds


# ===== ХЭНДЛЕРЫ =====

@dp.message(CommandStart())
async def cmd_start(message: Message):
    text = (
        "Привет! Я AI-крипто бот для анализа рынка Binance 🧠📈\n"
        "Я не беру доступ к твоему депозиту и не торгую за тебя.\n"
        "Моя задача — давать удобный, понятный анализ монет и готовые торговые сетапы, "
        "чтобы тебе было проще принимать решения.\n\n"

        "🔬 *Как делается анализ под капотом*\n\n"
        "Я работаю только с открытыми рыночными данными Binance:\n\n"
        "• беру котировки и свечи по монете на нескольких таймфреймах: 1D, 4H, 1H, 15M;\n"
        "• считаю изменение цены, чтобы понять тренд (бычий, медвежий или флет);\n"
        "• считаю RSI, чтобы понять, перекуплена монета или перепродана;\n"
        "• анализирую объёмы и сравниваю их со средними значениями;\n"
        "• ищу локальные минимумы и максимумы для определения уровней поддержки/сопротивления;\n"
        "• считаю риск/прибыль (R:R), чтобы TP были в несколько раз больше стопа;\n"
        "• по набору правил формирую вердикт и сигналы.\n\n"
        "Это не магия и не гарантированный профит, а системный теханализ + логика отбора, "
        "упакованные в удобный формат.\n\n"
        "Нажми кнопку ниже 👇"
    )

    await message.answer(text, reply_markup=main_menu_keyboard(), parse_mode="Markdown")


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
        "🎯 AI-сигналы\n\nВыбери режим:\n1) 🔔 Включить авто-сигналы\n2) 🚫 Отключить авто-сигналы",
        reply_markup=ai_signals_keyboard(),
    )


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


@dp.message(F.text == "₿ BTC (intraday)")
async def open_btc_menu(message: Message):
    waiting_for_symbol.discard(message.chat.id)
    await message.answer(
        "BTC-модуль (интрадей) — только BTCUSDT:\n\n"
        "• Разовый сигнал LONG/SHORT\n"
        "• Автоуведомления раз в 15 минут\n\n"
        "Выбирай действие:",
        reply_markup=get_btc_main_keyboard(),
    )


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


def fmt_price(value: float) -> str:
    v = abs(value)
    if v >= 100:
        return f"{value:.0f}"
    elif v >= 1:
        return f"{value:.2f}"
    elif v >= 0.01:
        return f"{value:.4f}"
    else:
        return f"{value:.8f}"


def _trend_short_text(trend: str) -> str:
    if trend == "bullish":
        return "бычий"
    if trend == "bearish":
        return "медвежий"
    return "нейтральный"


def _rsi_short_zone(rsi: float) -> str:
    if 40 <= rsi <= 60:
        return "комфортная зона"
    if rsi < 40:
        return "зона перепроданности"
    return "зона перекупленности"


def _format_signed_number(value: float, decimals: int = 1) -> str:
    sign = "−" if value < 0 else "+"
    return f"{sign}{abs(value):.{decimals}f}"


def _remember_signal(signal: Dict[str, Any], ttl: int = COOLDOWN_PER_SYMBOL) -> bool:
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


def _entry_diff_percent(prev_entry: Tuple[float, float], new_entry: Tuple[float, float]) -> float:
    prev_mid = (prev_entry[0] + prev_entry[1]) / 2 if prev_entry else 0
    new_mid = (new_entry[0] + new_entry[1]) / 2 if new_entry else 0
    if prev_mid == 0:
        return 0.0
    return abs(new_mid - prev_mid) / prev_mid * 100


def _is_new_ai_signal(signal: Dict[str, Any]) -> bool:
    now = time.time()
    symbol = signal["symbol"]
    direction = signal.get("direction", "long")
    entry_zone = (
        round(signal["entry_zone"][0], 6),
        round(signal["entry_zone"][1], 6),
    )

    last = LAST_SIGNALS.get(symbol)
    if last:
        if now - last["timestamp"] < COOLDOWN_PER_SYMBOL:
            return False
        if last["direction"] == direction:
            diff = _entry_diff_percent(last["entry"], entry_zone)
            if diff < ENTRY_DUP_THRESHOLD:
                return False

    LAST_SIGNALS[symbol] = {
        "entry": entry_zone,
        "direction": direction,
        "timestamp": now,
    }

    return True


def _format_signal(signal: Dict[str, Any]) -> str:
    entry_low, entry_high = signal["entry_zone"]
    direction_text = "ЛОНГ" if signal.get("direction") == "long" else "ШОРТ"
    symbol = signal["symbol"]
    if symbol.endswith("USDT"):
        base = symbol[:-4]
        quote = "USDT"
    else:
        base = symbol
        quote = ""
    symbol_text = f"{base} / {quote}" if quote else base

    entry_mid = (entry_low + entry_high) / 2
    tp1_pct = (signal["tp1"] / entry_mid - 1) * 100
    tp2_pct = (signal["tp2"] / entry_mid - 1) * 100
    sl_pct = (signal["sl"] / entry_mid - 1) * 100

    base_capital = 100
    tp1_usdt = base_capital * tp1_pct / 100
    tp2_usdt = base_capital * tp2_pct / 100
    sl_usdt = base_capital * sl_pct / 100

    raw_reason = signal.get("reason")
    reason = raw_reason if isinstance(raw_reason, dict) else {}
    trend_1d = _trend_short_text(reason.get("trend_1d", "neutral"))
    trend_4h = _trend_short_text(reason.get("trend_4h", "neutral"))
    rsi_1h = float(reason.get("rsi_1h", 50.0))
    rsi_zone = reason.get("rsi_1h_zone") or _rsi_short_zone(rsi_1h)
    volume_ratio = reason.get("volume_ratio", 0.0)
    volume_avg = reason.get("volume_avg", 0.0)
    rr = reason.get("rr", 0.0)

    short_block = (
        "Кратко:\n"
        f"• 1D тренд: {trend_1d}\n"
        f"• 4H тренд: {trend_4h}\n"
        f"• RSI 1H: {rsi_1h:.1f} ({rsi_zone})\n"
        f"• Объём: {volume_ratio:.2f}x от среднего {volume_avg:.2f}\n"
        f"• R:R: ~{rr:.2f}:1"
    )

    text = (
        "🔔 AI-сигнал (intraday)\n\n"
        f"Монета: {symbol_text}\n"
        f"Тип: {direction_text}\n\n"
        "Зона входа:\n"
        f"• {entry_low:.4f} – {entry_high:.4f}\n"
        "Стоп (SL):\n"
        f"• {signal['sl']:.4f}  ({_format_signed_number(sl_pct)}%)\n\n"
        "Цели:\n"
        f"• TP1: {signal['tp1']:.4f}  ({_format_signed_number(tp1_pct)}%)\n"
        f"• TP2: {signal['tp2']:.4f}  ({_format_signed_number(tp2_pct)}%)\n\n"
        "Пример для позиции 100 USDT:\n"
        f"• До TP1: {_format_signed_number(tp1_usdt)} USDT\n"
        f"• До TP2: {_format_signed_number(tp2_usdt)} USDT\n"
        f"• До SL: {_format_signed_number(sl_usdt)} USDT\n\n"
        f"Оценка сигнала: {signal['score']}/100\n\n"
        f"{short_block}\n\n"
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
                if signal.get("score", 0) < 90:
                    continue
                if not _is_new_ai_signal(signal):
                    print(
                        f"[ai_signals] Duplicate skipped: {signal.get('symbol')} {signal.get('direction')}"
                    )
                    continue
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

    if symbol_pair.endswith("USDT"):
        base = symbol_pair[:-4]
        quote = "USDT"
    else:
        base = symbol_pair
        quote = ""

    symbol_human = f"{base} / {quote}" if quote else base

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

    analysis_text = (
        f"📊 Анализ {symbol_human}\n\n"
        f"💰 Цена: {fmt_price(price)} USDT\n"
        f"{emoji_change} Изм. 24ч: {change:+.2f}%\n\n"
        f"🔭 Глобально (4ч):\n"
        f"• Тренд: {trend_4h}\n"
        f"• RSI: {rsi_4h:.1f} — {rsi_4h_txt}\n"
        f"• Уровни:\n"
        f"  • Поддержка: {fmt_price(support)}\n"
        f"  • Сопротивление: {fmt_price(resistance)}\n\n"
        f"⏱ Основной тренд (1ч):\n"
        f"• Тренд: {trend_1h}\n"
        f"• RSI: {rsi_1h:.1f} — {rsi_1h_txt}\n"
        f"• Объёмы: {vol_1h_txt}\n"
        f"• MACD: {macd_1h_txt}\n\n"
        f"🕒 Локально (15м):\n"
        f"• Тренд: {trend_15}\n"
        f"• RSI: {rsi_15:.1f} — {rsi_15_txt}\n"
        f"• Возможна коррекция к зоне {fmt_price(entry_low)}–{fmt_price(entry_high)}\n\n"
        f"🧠 Вердикт:\n"
        f"{verdict_text}\n\n"
        f"🎯 Пример уровней для сделки (для обучения, не финсовет):\n"
        f"• TP1: {fmt_price(tp1)}\n"
        f"• TP2: {fmt_price(tp2)}\n"
        f"• SL: {fmt_price(sl)}\n\n"
        f"⚠️ Риск сделки: {risk_text}.\n"
        "Источник данных: Binance\n\n"
    )

    coin_desc = await get_coin_description(symbol_pair)
    analysis_text += f"ℹ️ О монете:\n{coin_desc}"

    await message.answer(analysis_text)


@dp.message()
async def fallback(message: Message):
    await message.answer("Нажми кнопку в меню — пока я понимаю только их.")


# ===== ТОЧКА ВХОДА =====

async def main():
    print("Бот запущен!")
    init_db()
    signals_task = asyncio.create_task(signals_worker())
    pump_task = asyncio.create_task(pump_worker(bot))
    btc_task = asyncio.create_task(btc_realtime_signal_worker(bot))
    try:
        await dp.start_polling(bot)
    finally:
        signals_task.cancel()
        with suppress(asyncio.CancelledError):
            await signals_task
        pump_task.cancel()
        with suppress(asyncio.CancelledError):
            await pump_task
        btc_task.cancel()
        with suppress(asyncio.CancelledError):
            await btc_task


if __name__ == "__main__":
    asyncio.run(main())
