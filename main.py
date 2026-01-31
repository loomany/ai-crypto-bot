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

from btc_module import (
    router as btc_router,
    btc_realtime_signal_worker,
    get_btc_main_keyboard,
)
from whales_module import whales_market_flow_worker
from pro_modules import (
    router as pro_router,
)
from pump_detector import scan_pumps, format_pump_message
from signals import scan_market, get_alt_watch_symbol, is_pro_strict_signal
from symbol_cache import get_spot_usdt_symbols
from market_regime import get_market_regime
from health import MODULES, mark_tick, mark_ok, mark_error
from db_path import get_db_path
from notifications_db import init_notify_table
from pro_db import init_pro_tables, pro_list, pro_can_send, pro_inc_sent


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
            KeyboardButton(text="₿ BTC (intraday)"),
            KeyboardButton(text="🎯 AI-сигналы"),
        ],
        [
            KeyboardButton(text="🧠 PRO-модули"),
        ],
    ]
    return ReplyKeyboardMarkup(keyboard=kb, resize_keyboard=True)


def ai_signals_keyboard() -> ReplyKeyboardMarkup:
    kb = [
        [KeyboardButton(text="🔔 Включить авто-сигналы")],
        [KeyboardButton(text="🚫 Отключить авто-сигналы")],
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

    init_notify_table()
    init_pro_tables()


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
dp.include_router(pro_router)
FREE_MIN_SCORE = 70
COOLDOWN_FREE_SEC = 60 * 45
MAX_SIGNALS_PER_CYCLE = 3
MAX_BTC_PER_CYCLE = 1
PULSE_INTERVAL_SEC = 60 * 60
PRO_MIN_SCORE = 88
PRO_MIN_RR = 2.5
PRO_MIN_VOLUME_RATIO = 1.3
PRO_SYMBOL_COOLDOWN_SEC = 60 * 60 * 6
MAX_PRO_SIGNALS_PER_DAY = 4
MAX_PRO_SIGNALS_PER_CYCLE = 2
CHUNK_SIZE = 60
SCAN_MARKET_TIMEOUT_SEC = 20

LAST_SENT_FREE: Dict[Tuple[str, str], float] = {}
LAST_PRO_SYMBOL_SENT: Dict[str, float] = {}
LAST_PULSE_SENT_AT = 0.0
CURRENT_CHUNK_IDX = 0
# Сканируем рынок каждые 60 секунд
AI_SCAN_INTERVAL = 60  # seconds
PRO_AI_SCAN_INTERVAL = 60 * 10


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
        "Привет! Я AI-крипто бот для рынка Binance 🧠📈\n\n"
        "Что умею:\n"
        "• ₿ BTC (intraday) — быстрые сигналы по BTCUSDT\n"
        "• 🎯 AI-сигналы — автоматические сетапы по рынку\n"
        "• 🧠 PRO — Pump/Dump, Whale Flow и PRO AI-сигналы\n\n"
        "Нажми кнопку ниже 👇"
    )

    await message.answer(text, reply_markup=main_menu_keyboard(), parse_mode="Markdown")


@dp.message(F.text == "🎯 AI-сигналы")
async def ai_signals_menu(message: Message):
    await message.answer(
        "🎯 AI-сигналы\n\nВыбери режим:\n1) 🔔 Включить авто-сигналы\n2) 🚫 Отключить авто-сигналы",
        reply_markup=ai_signals_keyboard(),
    )


@dp.message(F.text == "🔔 Включить авто-сигналы")
async def ai_signals_subscribe(message: Message):
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


@dp.message(F.text == "/testadmin")
async def test_admin(message: Message):
    pro_subscribers = pro_list()
    ai_subscribers = list_subscriptions()
    ai_extra = MODULES.get("ai_signals").extra if "ai_signals" in MODULES else ""
    ai_extra = ai_extra.strip()
    def _merge_extra(base: str, extra: str) -> str:
        return f"{base}; {extra}" if extra else base

    if "pro" in MODULES:
        MODULES["pro"].extra = f"подписчиков: {len(pro_subscribers)}"
    if "ai_signals" in MODULES:
        base = f"подписчиков: {len(ai_subscribers)}"
        MODULES["ai_signals"].extra = _merge_extra(base, ai_extra)
    if "pumpdump" in MODULES:
        base = f"подписчиков: {len(pro_subscribers)}"
        MODULES["pumpdump"].extra = _merge_extra(base, MODULES["pumpdump"].extra)
    if "whales_flow" in MODULES:
        base = f"подписчиков: {len(pro_subscribers)}"
        MODULES["whales_flow"].extra = _merge_extra(base, MODULES["whales_flow"].extra)
    if "pro_ai" in MODULES:
        base = f"подписчиков: {len(pro_subscribers)}"
        MODULES["pro_ai"].extra = _merge_extra(base, MODULES["pro_ai"].extra)

    lines = ["🛠 Статус модулей:\n"]
    for key, st in MODULES.items():
        lines.append(f"{st.name}:\n{st.as_text()}\n")

    await message.answer("\n".join(lines))


@dp.message(F.text == "⬅️ Главное меню")
async def back_to_main(message: Message):
    await message.answer("Возвращаемся в главное меню.", reply_markup=main_menu_keyboard())


@dp.message(F.text == "₿ BTC (intraday)")
async def open_btc_menu(message: Message):
    await message.answer(
        "BTC-модуль (интрадей) — только BTCUSDT:\n\n"
        "• Автоматические сигналы LONG/SHORT\n"
        "• Сигнал приходит сразу, как только появляется сетап\n"
        "• Горизонт сделок: внутри 24 часов\n\n"
        "Выбирай действие:",
        reply_markup=get_btc_main_keyboard(),
    )



def _trend_short_text(trend: str) -> str:
    if trend in ("bullish", "up"):
        return "бычий"
    if trend in ("bearish", "down"):
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
    is_long = signal.get("direction") == "long"
    direction_text = "ЛОНГ" if is_long else "ШОРТ"
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
        f"• R:R: {rr:.2f} : 1"
    )

    tier_title = "🔥 AI-сигнал (FREE)" if tier == "free" else "🧊 AI-сигнал (PRO)"
    prob = int(signal.get("score", 0))
    if tier == "free":
        probability_line = (
            f"Вероятность (оценка модели): {prob}%\n"
            f"FREE порог: {FREE_MIN_SCORE}%\n"
            "⚠️ Оценка модели, не гарантия."
        )
    else:
        probability_line = f"📌 Вероятность: {prob}%"

    direction_block = (
        f"📈 Тип: {direction_text}\nВход: ниже\nSL: ниже входа\nTP: выше входа"
        if is_long
        else f"📉 Тип: {direction_text}\nВход: выше\nSL: выше входа\nTP: ниже входа"
    )

    text = (
        f"{tier_title}\n\n"
        f"Монета: {symbol_text}\n"
        f"{direction_block}\n\n"
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
        f"{probability_line}\n\n"
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

    if not _cooldown_ready(signal_dict, LAST_SENT_FREE, COOLDOWN_FREE_SEC):
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
            subscribers = pro_list()
            mark_tick("pumpdump", extra=f"подписчиков: {len(subscribers)}")
            if not subscribers:
                await asyncio.sleep(15)
                continue

            cycle_start = time.time()
            signals, stats = await scan_pumps(return_stats=True)
            now_min = int(time.time() // 60)
            sent_count = 0

            for sig in signals:
                symbol = sig["symbol"]

                if last_sent.get(symbol) == now_min:
                    continue

                text = format_pump_message(sig)

                last_sent[symbol] = now_min
                for chat_id in subscribers:
                    try:
                        await bot.send_message(chat_id, text, parse_mode="Markdown")
                        sent_count += 1
                    except Exception:
                        continue

            cycle_sec = time.time() - cycle_start
            mark_ok(
                "pumpdump",
                extra=(
                    f"checked={stats.get('checked', 0)} found={stats.get('found', 0)} "
                    f"sent={sent_count} cycle={cycle_sec:.1f}s"
                ),
            )
        except Exception as e:
            msg = f"error: {e}"
            print(f"[pump_worker] {msg}")
            mark_error("pumpdump", msg)
            await asyncio.sleep(10)

        await asyncio.sleep(15)


async def signals_worker():
    global CURRENT_CHUNK_IDX
    while True:
        mark_tick("ai_signals", extra="сканирую рынок...")

        try:
            symbols = await get_spot_usdt_symbols()
            if not symbols:
                mark_error("ai_signals", "no symbols to scan")
                await asyncio.sleep(AI_SCAN_INTERVAL)
                continue

            chunks = [symbols[i : i + CHUNK_SIZE] for i in range(0, len(symbols), CHUNK_SIZE)]
            if CURRENT_CHUNK_IDX >= len(chunks):
                CURRENT_CHUNK_IDX = 0
            chunk_idx = CURRENT_CHUNK_IDX
            chunk = chunks[chunk_idx]
            CURRENT_CHUNK_IDX = (CURRENT_CHUNK_IDX + 1) % len(chunks)

            signals, stats = await asyncio.wait_for(
                scan_market(
                    symbols=chunk,
                    use_btc_gate=False,
                    free_mode=True,
                    min_score=FREE_MIN_SCORE,
                    return_stats=True,
                ),
                timeout=SCAN_MARKET_TIMEOUT_SEC,
            )
            print("SCAN OK", len(signals))
            sent_count = 0
            for signal in _select_signals_for_cycle(signals):
                score = signal.get("score", 0)
                if score < FREE_MIN_SCORE:
                    continue
                print(
                    f"[ai_signals] SEND FREE {signal['symbol']} {signal['direction']} score={score}"
                )
                await send_signal_to_all(signal, "free")
                sent_count += 1
            checked = stats.get("checked", 0)
            mark_ok(
                "ai_signals",
                extra=(
                    f"chunk={chunk_idx + 1}/{len(chunks)} checked={checked} sent={sent_count}"
                ),
            )
        except asyncio.TimeoutError:
            mark_error("ai_signals", f"scan exceeded {SCAN_MARKET_TIMEOUT_SEC}s")
        except Exception as e:
            msg = f"Worker error: {e}"
            print(f"[ai_signals] {msg}")
            mark_error("ai_signals", msg)

        await asyncio.sleep(AI_SCAN_INTERVAL)


def _pro_symbol_cooldown_ready(symbol: str) -> bool:
    now = time.time()
    last = LAST_PRO_SYMBOL_SENT.get(symbol)
    if last and now - last < PRO_SYMBOL_COOLDOWN_SEC:
        return False
    return True


def _mark_pro_symbol_sent(symbol: str) -> None:
    if symbol:
        LAST_PRO_SYMBOL_SENT[symbol] = time.time()


async def _send_pro_signal(signal_dict: Dict[str, Any], subscribers: List[int]):
    if bot is None:
        print("[pro_ai] Bot is not initialized; skipping send.")
        return

    text = _format_signal(signal_dict, "pro")
    for chat_id in subscribers:
        if not pro_can_send(chat_id, MAX_PRO_SIGNALS_PER_DAY):
            continue
        try:
            await bot.send_message(chat_id, text)
            pro_inc_sent(chat_id)
        except Exception:
            continue

    _mark_pro_symbol_sent(signal_dict.get("symbol", ""))


async def pro_ai_signals_worker():
    buffer: Dict[str, Dict[str, Any]] = {}
    buffer_timestamps: Dict[str, float] = {}
    buffer_ttl_sec = 60 * 60 * 6

    while True:
        try:
            subscribers = pro_list()
            mark_tick("pro_ai", extra=f"подписчиков: {len(subscribers)}")
            mark_tick("pro", extra=f"подписчиков: {len(subscribers)}")
            if not subscribers:
                await asyncio.sleep(20)
                continue

            signals = await scan_market()
            now = time.time()
            for sig in signals:
                if not is_pro_strict_signal(
                    sig,
                    min_score=PRO_MIN_SCORE,
                    min_rr=PRO_MIN_RR,
                    min_volume_ratio=PRO_MIN_VOLUME_RATIO,
                ):
                    continue
                symbol = sig.get("symbol")
                if not symbol or not _pro_symbol_cooldown_ready(symbol):
                    continue
                current = buffer.get(symbol)
                if not current or sig.get("score", 0) > current.get("score", 0):
                    buffer[symbol] = sig
                    buffer_timestamps[symbol] = now

            stale_symbols = [
                symbol
                for symbol, ts in buffer_timestamps.items()
                if now - ts > buffer_ttl_sec
            ]
            for symbol in stale_symbols:
                buffer.pop(symbol, None)
                buffer_timestamps.pop(symbol, None)

            candidates = sorted(buffer.values(), key=lambda item: item.get("score", 0), reverse=True)

            if candidates:
                mark_ok("pro_ai", extra=f"кандидатов: {len(candidates)}")
            else:
                mark_tick("pro_ai", extra="кандидатов: 0")

            for signal in candidates[:MAX_PRO_SIGNALS_PER_CYCLE]:
                await _send_pro_signal(signal, subscribers)
                symbol = signal.get("symbol", "")
                buffer.pop(symbol, None)
                buffer_timestamps.pop(symbol, None)

        except Exception as e:
            msg = f"Worker error: {e}"
            print(f"[pro_ai] {msg}")
            mark_error("pro_ai", msg)

        await asyncio.sleep(PRO_AI_SCAN_INTERVAL)


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
    whales_task = asyncio.create_task(whales_market_flow_worker(bot))
    pro_ai_task = asyncio.create_task(pro_ai_signals_worker())
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
        pro_ai_task.cancel()
        with suppress(asyncio.CancelledError):
            await pro_ai_task


if __name__ == "__main__":
    asyncio.run(main())
