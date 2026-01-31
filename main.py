import asyncio
import os
import sqlite3
import time
from contextlib import suppress
from datetime import datetime, timezone, timedelta
from typing import Any, Dict, List, Tuple

from aiogram import Bot, Dispatcher, F
from aiogram.client.default import DefaultBotProperties
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
from whales_module import (
    router as whales_router,
    whales_realtime_worker,
)
from pro_modules import (
    router as pro_router,
    orderflow_pro_worker,
    smart_money_worker,
    ai_patterns_worker,
    market_regime_worker,
)
from market_data import get_coin_analysis
from pump_detector import scan_pumps, format_pump_message
from pump_db import disable_pump_subscriber, enable_pump_subscriber, get_pump_subscribers
from signals import scan_market, get_alt_watch_symbol
from market_regime import get_market_regime
from health import MODULES, mark_tick, mark_ok, mark_error
from signal_filter import (
    init_filter_table,
    set_user_filter,
    get_user_filter,
    btc_min_probability,
    whales_min_probability,
    pumps_min_strength,
)
from db_path import get_db_path
from notifications_db import init_notify_table


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
            KeyboardButton(text="₿ BTC (intraday)"),
            KeyboardButton(text="🚀 Pump Detector"),
        ],
        [
            KeyboardButton(text="🐳 Киты (ТОП-5)"),
            KeyboardButton(text="🧠 PRO-модули"),
        ],
        [
            KeyboardButton(text="⚙️ Фильтр сигналов"),
        ],
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


def signal_filter_keyboard(current: str | None = None) -> ReplyKeyboardMarkup:
    postfix = {
        "aggressive": " (текущий)",
        "strict": " (текущий)",
    }
    cur = current or "aggressive"
    if cur == "normal":
        cur = "aggressive"

    kb = [
        [KeyboardButton(text="🔥 Больше сетапов (FREE)" + (postfix["aggressive"] if cur == "aggressive" else ""))],
        [KeyboardButton(text="🧊 Только топ-сигналы (PRO)" + (postfix["strict"] if cur == "strict" else ""))],
        [KeyboardButton(text="⬅️ Главное меню")],
    ]
    return ReplyKeyboardMarkup(keyboard=kb, resize_keyboard=True)


# ===== ВРЕМЯ ТОРГОВ =====

ALMATY_TZ = timezone(timedelta(hours=5))


def is_trading_time() -> bool:
    """
    Возвращает True, если сейчас можно отправлять новые сигналы.
    Правила:
      - Не торгуем в глубокую ночь: 02:00–08:00 по Алматы.
      - Не торгуем в субботу и воскресенье.
    """

    now = datetime.now(ALMATY_TZ)
    if now.weekday() >= 5:
        return False
    if 2 <= now.hour < 8:
        return False
    return True


# ===== РАБОТА С ПОДПИСКАМИ =====


def init_db():
    conn = sqlite3.connect(get_db_path())
    try:
        conn.execute(
            "CREATE TABLE IF NOT EXISTS ai_signals_subscribers (chat_id INTEGER PRIMARY KEY)"
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS users (
                chat_id     INTEGER PRIMARY KEY,
                username    TEXT,
                first_name  TEXT,
                last_name   TEXT,
                full_name   TEXT,
                language    TEXT,
                started_at  INTEGER,
                last_seen   INTEGER
            )
            """
        )
        conn.commit()
    finally:
        conn.close()

    init_filter_table()
    init_notify_table()


def upsert_user(
    chat_id: int,
    username: str | None,
    first_name: str | None,
    last_name: str | None,
    full_name: str | None,
    language: str | None,
) -> bool:
    now = int(time.time())
    conn = sqlite3.connect(get_db_path())
    try:
        cur = conn.cursor()
        cur.execute("SELECT 1 FROM users WHERE chat_id = ?", (chat_id,))
        exists = cur.fetchone() is not None
        if not exists:
            cur.execute(
                """
                INSERT INTO users (
                    chat_id, username, first_name, last_name,
                    full_name, language, started_at, last_seen
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    chat_id,
                    username,
                    first_name,
                    last_name,
                    full_name,
                    language,
                    now,
                    now,
                ),
            )
            conn.commit()
            return True

        cur.execute(
            """
            UPDATE users
            SET username = ?, first_name = ?, last_name = ?, full_name = ?,
                language = ?, last_seen = ?
            WHERE chat_id = ?
            """,
            (
                username,
                first_name,
                last_name,
                full_name,
                language,
                now,
                chat_id,
            ),
        )
        conn.commit()
        return False
    finally:
        conn.close()


def add_subscription(chat_id: int) -> bool:
    conn = sqlite3.connect(get_db_path())
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
    conn = sqlite3.connect(get_db_path())
    try:
        cur = conn.cursor()
        cur.execute("DELETE FROM ai_signals_subscribers WHERE chat_id = ?", (chat_id,))
        conn.commit()
        return cur.rowcount > 0
    finally:
        conn.close()


def list_subscriptions() -> List[int]:
    conn = sqlite3.connect(get_db_path())
    try:
        cur = conn.cursor()
        cur.execute("SELECT chat_id FROM ai_signals_subscribers")
        return [row[0] for row in cur.fetchall()]
    finally:
        conn.close()


# ===== СОЗДАЁМ БОТА =====

BOT_TOKEN = load_settings()
ADMIN_CHAT_ID = int(os.getenv("ADMIN_CHAT_ID", "0"))
bot: Bot | None = None
dp = Dispatcher()
dp.include_router(btc_router)
dp.include_router(whales_router)
dp.include_router(pro_router)
waiting_for_symbol: set[int] = set()
FREE_MIN_SCORE = 85
PRO_MIN_SCORE = 93
COOLDOWN_FREE_SEC = 60 * 60 * 2
COOLDOWN_PRO_SEC = 60 * 60 * 4
MAX_SIGNALS_PER_CYCLE = 3
MAX_BTC_PER_CYCLE = 1
PULSE_INTERVAL_SEC = 60 * 60

LAST_SENT_FREE: Dict[Tuple[str, str], float] = {}
LAST_SENT_PRO: Dict[Tuple[str, str], float] = {}
LAST_PULSE_SENT_AT = 0.0
# Сканируем рынок каждые 30 секунд, чтобы рассылка была оперативной
AI_SCAN_INTERVAL = 30  # seconds


# ===== ХЭНДЛЕРЫ =====

@dp.message(CommandStart())
async def cmd_start(message: Message):
    user = message.from_user
    if user is not None:
        is_new = upsert_user(
            chat_id=message.chat.id,
            username=user.username,
            first_name=user.first_name,
            last_name=user.last_name,
            full_name=user.full_name,
            language=user.language_code,
        )
        if is_new and ADMIN_CHAT_ID != 0:
            username = f"@{user.username}" if user.username else "-"
            full_name = user.full_name or "-"
            language = user.language_code or "-"
            admin_text = (
                "🆕 Новый пользователь\n"
                f"ID: {message.chat.id}\n"
                f"Username: {username}\n"
                f"Имя: {full_name}\n"
                f"Язык: {language}"
            )
            await message.bot.send_message(ADMIN_CHAT_ID, admin_text)

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


@dp.message(F.text == "⚙️ Фильтр сигналов")
async def open_filter_menu(message: Message):
    level = get_user_filter(message.chat.id)
    if level == "normal":
        set_user_filter(message.chat.id, "aggressive")
        level = "aggressive"
    text = (
        "⚙️ Настройка фильтра сигналов\n\n"
        "Выбери режим, насколько жёстко фильтровать авто-сигналы:\n\n"
        "🔥 Больше сетапов (FREE) — больше сделок, но качество чуть ниже.\n"
        "🧊 Только топ-сигналы (PRO) — мало, но самые сильные сетапы.\n\n"
        "Режим влияет на AI-сигналы, BTC-модуль, Pump Detector и Китов."
    )
    await message.answer(text, reply_markup=signal_filter_keyboard(current=level))


@dp.message(F.text.startswith("🔥 Больше сетапов"))
async def set_filter_aggressive(message: Message):
    set_user_filter(message.chat.id, "aggressive")
    await message.answer(
        "🔥 Режим фильтра: БОЛЬШЕ СЕТАПОВ (FREE).\n\n"
        "Сигналов будет больше, но они чуть агрессивнее.",
        reply_markup=signal_filter_keyboard(current="aggressive"),
    )


@dp.message(F.text.startswith("🧊 Только топ-сигналы"))
async def set_filter_strict(message: Message):
    set_user_filter(message.chat.id, "strict")
    await message.answer(
        "🧊 Режим фильтра: ТОЛЬКО ТОП-СИГНАЛЫ (PRO).\n\n"
        "Будем присылать только самые сильные сетапы.",
        reply_markup=signal_filter_keyboard(current="strict"),
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
    changed = enable_pump_subscriber(message.chat.id)
    await message.answer(
        "✅ Авто-оповещения Pump Detector включены.\n"
        "Я буду присылать пампы по монетам Binance, когда найду их."
        if changed
        else "✅ Авто-оповещения Pump Detector уже включены.",
        reply_markup=pump_menu_keyboard(),
    )


@dp.message(F.text == "🚫 Отключить авто-пампы")
async def unsubscribe_pumps(message: Message):
    waiting_for_symbol.discard(message.chat.id)
    changed = disable_pump_subscriber(message.chat.id)
    await message.answer(
        "⭕ Авто-оповещения Pump Detector выключены."
        if changed
        else "✅ Авто-оповещения Pump Detector уже отключены.",
        reply_markup=pump_menu_keyboard(),
    )


@dp.message(F.text == "/testadmin")
async def test_admin(message: Message):
    lines = ["🛠 Статус модулей:\n"]
    for key, st in MODULES.items():
        lines.append(f"{st.name}:\n{st.as_text()}\n")

    await message.answer("\n".join(lines))


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
        "• Автоматические сигналы LONG/SHORT\n"
        "• Сигнал приходит сразу, как только появляется сетап\n"
        "• Горизонт сделок: внутри 24 часов\n\n"
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

def _cooldown_ready(
    signal: Dict[str, Any], last_sent: Dict[Tuple[str, str], float], cooldown_sec: int
) -> bool:
    now = time.time()
    key = (signal["symbol"], signal.get("direction", "long"))
    last = last_sent.get(key)
    if last and now - last < cooldown_sec:
        return False
    last_sent[key] = now
    return True


def _format_signal(signal: Dict[str, Any], tier: str) -> str:
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

    tier_title = "🔥 AI-сигнал (FREE)" if tier == "free" else "🧊 AI-сигнал (PRO)"
    text = (
        f"{tier_title}\n\n"
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


async def send_signal_to_all(signal_dict: Dict[str, Any], tier: str):
    """
    Отправляет FREE/PRO сигнал всем подписчикам без блокировки event loop.
    """
    if bot is None:
        print("[ai_signals] Bot is not initialized; skipping send.")
        return

    subscribers = list_subscriptions()
    if not subscribers:
        return

    if tier == "free":
        if not _cooldown_ready(signal_dict, LAST_SENT_FREE, COOLDOWN_FREE_SEC):
            return
    else:
        if not _cooldown_ready(signal_dict, LAST_SENT_PRO, COOLDOWN_PRO_SEC):
            return

    text = _format_signal(signal_dict, tier)

    tasks = [asyncio.create_task(bot.send_message(chat_id, text)) for chat_id in subscribers]

    results = await asyncio.gather(*tasks, return_exceptions=True)
    for chat_id, res in zip(subscribers, results):
        if isinstance(res, Exception):
            print(f"[ai_signals] Failed to send to {chat_id}: {res}")


def _format_symbol_pair(symbol: str) -> str:
    if symbol.endswith("USDT"):
        return f"{symbol[:-4]}/USDT"
    return symbol


def _format_volume_usdt(value: float) -> str:
    if value >= 1_000_000_000:
        return f"{value / 1_000_000_000:.2f}B"
    if value >= 1_000_000:
        return f"{value / 1_000_000:.2f}M"
    if value >= 1_000:
        return f"{value / 1_000:.2f}K"
    return f"{value:.0f}"


async def market_pulse_worker():
    global LAST_PULSE_SENT_AT

    while True:
        try:
            if bot is None:
                await asyncio.sleep(5)
                continue

            subscribers = list_subscriptions()
            if not subscribers:
                await asyncio.sleep(30)
                continue

            now = time.time()
            if now - LAST_PULSE_SENT_AT < PULSE_INTERVAL_SEC:
                await asyncio.sleep(30)
                continue

            regime_info = await get_market_regime()
            regime = regime_info.get("regime", "neutral")
            regime_label = {
                "risk_on": "RISK-ON",
                "risk_off": "RISK-OFF",
                "neutral": "NEUTRAL",
            }.get(regime, "NEUTRAL")

            alt_watch = await get_alt_watch_symbol()
            if alt_watch:
                alt_symbol = _format_symbol_pair(str(alt_watch.get("symbol", "")))
                change_pct = float(alt_watch.get("change_pct", 0.0))
                volume_usdt = float(alt_watch.get("volume_usdt", 0.0))
                alt_line = (
                    f"Монета для наблюдения: {alt_symbol} — "
                    f"{change_pct:+.2f}% за 1ч, объём ~{_format_volume_usdt(volume_usdt)} USDT."
                )
            else:
                alt_line = "Монета для наблюдения: SOL/USDT — повышенный объём, ждём подтверждения."

            text = (
                "📡 Market Pulse (каждый час)\n"
                f"BTC режим: {regime_label}\n"
                "Сетапов нет — фильтр строгий. Это нормально.\n"
                f"{alt_line}"
            )

            tasks = [asyncio.create_task(bot.send_message(chat_id, text)) for chat_id in subscribers]
            await asyncio.gather(*tasks, return_exceptions=True)
            LAST_PULSE_SENT_AT = now

        except Exception as e:
            msg = f"pulse error: {e}"
            print(f"[pulse_worker] {msg}")
            mark_error("ai_signals", msg)

        await asyncio.sleep(30)


def _select_signals_for_cycle(signals: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    sorted_signals = sorted(signals, key=lambda item: item.get("score", 0), reverse=True)
    has_alt = any(sig.get("symbol") != "BTCUSDT" for sig in sorted_signals)
    max_btc = MAX_BTC_PER_CYCLE if has_alt else MAX_SIGNALS_PER_CYCLE

    selected: List[Dict[str, Any]] = []
    used_symbols: set[str] = set()
    btc_count = 0

    for signal in sorted_signals:
        if len(selected) >= MAX_SIGNALS_PER_CYCLE:
            break
        symbol = signal.get("symbol")
        if not symbol or symbol in used_symbols:
            continue
        if symbol == "BTCUSDT" and btc_count >= max_btc:
            continue

        selected.append(signal)
        used_symbols.add(symbol)
        if symbol == "BTCUSDT":
            btc_count += 1

    return selected


async def pump_worker(bot: Bot):
    """
    Периодически сканирует рынок и рассылает авто-пампы подписчикам.
    """
    last_sent: dict[str, int] = {}

    while True:
        try:
            subscribers = get_pump_subscribers()
            mark_tick("pumps", extra=f"подписчиков: {len(subscribers)}")
            if not subscribers:
                await asyncio.sleep(15)
                continue

            signals = await scan_pumps()
            mark_ok("pumps", extra=f"найдено пампов: {len(signals)}")
            now_min = int(time.time() // 60)

            for sig in signals:
                symbol = sig["symbol"]

                if last_sent.get(symbol) == now_min:
                    continue

                text = format_pump_message(sig)

                # ФИЛЬТР ПО СИЛЕ ПАМПА
                strength = float(sig.get("strength", 0.0))

                for chat_id in subscribers:
                    level = get_user_filter(chat_id)
                    min_strength = pumps_min_strength(level)
                    if strength < min_strength:
                        continue

                    last_sent[symbol] = now_min
                    try:
                        await bot.send_message(chat_id, text, parse_mode="Markdown")
                    except Exception:
                        continue

        except Exception as e:
            msg = f"error: {e}"
            print(f"[pump_worker] {msg}")
            mark_error("pumps", msg)
            await asyncio.sleep(10)

        await asyncio.sleep(10)


async def signals_worker():
    while True:
        try:
            signals = await scan_market()
            mark_ok("ai_signals", extra=f"кандидатов: {len(signals)}")
            print("SCAN OK", len(signals))
            for signal in _select_signals_for_cycle(signals):
                score = signal.get("score", 0)
                if score >= FREE_MIN_SCORE:
                    print(
                        f"[ai_signals] SEND FREE {signal['symbol']} {signal['direction']} score={score}"
                    )
                    await send_signal_to_all(signal, "free")
                if score >= PRO_MIN_SCORE:
                    print(
                        f"[ai_signals] SEND PRO {signal['symbol']} {signal['direction']} score={score}"
                    )
                    await send_signal_to_all(signal, "pro")
        except Exception as e:
            msg = f"Worker error: {e}"
            print(f"[ai_signals] {msg}")
            mark_error("ai_signals", msg)
        mark_tick("ai_signals")
        await asyncio.sleep(AI_SCAN_INTERVAL)


@dp.message(lambda message: message.chat.id in waiting_for_symbol)
async def process_symbol(message: Message):
    chat_id = message.chat.id

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


# ===== ТОЧКА ВХОДА =====

async def main():
    global bot
    bot = Bot(token=BOT_TOKEN, default=DefaultBotProperties(parse_mode="HTML"))
    print("Бот запущен!")
    init_db()
    signals_task = asyncio.create_task(signals_worker())
    pulse_task = asyncio.create_task(market_pulse_worker())
    pump_task = asyncio.create_task(pump_worker(bot))
    btc_task = asyncio.create_task(btc_realtime_signal_worker(bot))
    whales_task = asyncio.create_task(whales_realtime_worker(bot))
    orderflow_task = asyncio.create_task(orderflow_pro_worker(bot))
    smart_money_task = asyncio.create_task(smart_money_worker(bot))
    ai_patterns_task = asyncio.create_task(ai_patterns_worker(bot))
    regime_task = asyncio.create_task(market_regime_worker(bot))
    try:
        await dp.start_polling(bot)
    finally:
        signals_task.cancel()
        with suppress(asyncio.CancelledError):
            await signals_task
        pulse_task.cancel()
        with suppress(asyncio.CancelledError):
            await pulse_task
        pump_task.cancel()
        with suppress(asyncio.CancelledError):
            await pump_task
        btc_task.cancel()
        with suppress(asyncio.CancelledError):
            await btc_task
        whales_task.cancel()
        with suppress(asyncio.CancelledError):
            await whales_task
        orderflow_task.cancel()
        with suppress(asyncio.CancelledError):
            await orderflow_task

        smart_money_task.cancel()
        with suppress(asyncio.CancelledError):
            await smart_money_task

        ai_patterns_task.cancel()
        with suppress(asyncio.CancelledError):
            await ai_patterns_task

        regime_task.cancel()
        with suppress(asyncio.CancelledError):
            await regime_task


if __name__ == "__main__":
    asyncio.run(main())
