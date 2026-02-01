import asyncio
import os
import sqlite3
import time
from contextlib import suppress
from datetime import datetime, timezone, timedelta
from typing import Any, Dict, List

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
from binance_rest import close_shared_session
from whales_module import whales_market_flow_worker
from pro_modules import (
    router as pro_router,
)
from pump_detector import scan_pumps, format_pump_message
from signals import scan_market, get_alt_watch_symbol, is_pro_strict_signal
from symbol_cache import get_all_usdt_symbols, get_top_usdt_symbols_by_volume
from market_regime import get_market_regime
from health import MODULES, mark_tick, mark_ok, mark_error
from db_path import get_db_path
from alert_dedup_db import init_alert_dedup, can_send
from notifications_db import init_notify_table, enable_notify, disable_notify, list_enabled
from message_templates import format_scenario_message
from pro_db import init_pro_tables, pro_list, pro_can_send, pro_inc_sent
from signal_audit_db import init_signal_audit_tables, insert_signal_audit, get_public_stats
from signal_audit_worker import signal_audit_worker_loop


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
            KeyboardButton(text="📊 Статистика"),
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
    init_alert_dedup()
    init_pro_tables()
    init_signal_audit_tables()
    migrate_ai_subscribers_to_notify()


def migrate_ai_subscribers_to_notify() -> None:
    """
    Разово переносим подписчиков из legacy таблицы ai_signals_subscribers
    в notify_settings(feature='ai_signals'). Старую таблицу не трогаем.
    """
    conn = sqlite3.connect(get_db_path())
    try:
        cur = conn.cursor()
        cur.execute("SELECT chat_id FROM ai_signals_subscribers")
        rows = cur.fetchall()
    finally:
        conn.close()

    for (chat_id,) in rows:
        try:
            enable_notify(int(chat_id), "ai_signals")
        except Exception:
            continue


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


def list_ai_subscribers() -> List[int]:
    """
    Единый источник подписчиков AI:
    - основной: notify_settings(feature='ai_signals', enabled=1)
    - legacy: ai_signals_subscribers (на случай старых баз)
    """
    subs = set(list_enabled("ai_signals"))

    # legacy fallback (не мешает после миграции)
    conn = sqlite3.connect(get_db_path())
    try:
        cur = conn.cursor()
        cur.execute("SELECT chat_id FROM ai_signals_subscribers")
        for (chat_id,) in cur.fetchall():
            subs.add(int(chat_id))
    finally:
        conn.close()

    return sorted(subs)


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
CHUNK_SIZE = 50

LAST_PRO_SYMBOL_SENT: Dict[str, float] = {}
LAST_PULSE_SENT_AT = 0.0
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
    is_new = enable_notify(message.chat.id, "ai_signals")
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
    removed = disable_notify(message.chat.id, "ai_signals")
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
    ai_subscribers = list_ai_subscribers()
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


def _format_stats_message(stats: Dict[str, Any]) -> str:
    total = stats.get("total", 0)
    closed = stats.get("closed", 0)
    filled_closed = stats.get("filled_closed", 0)
    filled_rate = stats.get("filled_rate", 0.0) * 100
    winrate = stats.get("winrate", 0.0) * 100
    avg_r = stats.get("avg_r", 0.0)
    median_r = stats.get("median_r", 0.0)
    profit_factor = stats.get("profit_factor")
    streak = stats.get("streak", "-")
    last10 = stats.get("last10", [])

    pf_text = f"{profit_factor:.2f}" if isinstance(profit_factor, (int, float)) else "—"

    lines = [
        "📊 Статистика сигналов (30d)",
        "",
        f"• Всего: {total}",
        f"• Закрыто: {closed}",
        f"• Filled rate: {filled_rate:.1f}% ({filled_closed} из {total})",
        f"• Winrate (filled): {winrate:.1f}%",
        f"• Profit factor: {pf_text}",
        f"• Avg R: {avg_r:.2f}",
        f"• Median R: {median_r:.2f}",
        f"• Streak: {streak}",
        "",
        "Последние 10 сигналов:",
    ]

    if not last10:
        lines.append("• Нет данных")
    else:
        for row in last10:
            symbol = row.get("symbol", "-")
            direction = row.get("direction", "-")
            outcome = row.get("outcome", "-")
            pnl_r = row.get("pnl_r")
            pnl_text = f"{pnl_r:+.2f}R" if isinstance(pnl_r, (int, float)) else "-"
            lines.append(f"• {symbol} {direction.upper()} → {outcome} ({pnl_text})")

    return "\n".join(lines)


@dp.message(F.text == "📊 Статистика")
@dp.message(F.text == "/stats")
async def show_stats(message: Message):
    stats = get_public_stats(days=30)
    await message.answer(_format_stats_message(stats))


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

def _format_signal(signal: Dict[str, Any], tier: str) -> str:
    entry_low, entry_high = signal["entry_zone"]
    symbol = signal["symbol"]
    if symbol.endswith("USDT"):
        base = symbol[:-4]
        quote = "USDT"
    else:
        base = symbol
        quote = ""
    symbol_text = f"{base} / {quote}" if quote else base

    raw_reason = signal.get("reason")
    reason = raw_reason if isinstance(raw_reason, dict) else {}
    rsi_1h = float(reason.get("rsi_1h", 50.0))
    volume_ratio = float(reason.get("volume_ratio", 0.0))
    rr = float(reason.get("rr", 0.0))

    side = "LONG" if signal.get("direction") == "long" else "SHORT"
    score = int(signal.get("score", 0))

    return format_scenario_message(
        symbol_text=symbol_text,
        side=side,
        timeframe="1H",
        entry_from=entry_low,
        entry_to=entry_high,
        sl=float(signal["sl"]),
        tp1=float(signal["tp1"]),
        tp2=float(signal["tp2"]),
        score=score,
        trend_1d=reason.get("trend_1d"),
        trend_4h=reason.get("trend_4h"),
        rsi_1h=rsi_1h,
        volume_ratio=volume_ratio,
        rr=rr,
        price_precision=4,
    )


async def send_signal_to_all(signal_dict: Dict[str, Any], tier: str):
    """
    Отправляет FREE/PRO сигнал всем подписчикам без блокировки event loop.
    """
    if bot is None:
        print("[ai_signals] Bot is not initialized; skipping send.")
        return

    subscribers = list_ai_subscribers()
    if not subscribers:
        return

    # dedup ключ: символ + направление + зона входа (округление чтобы не шумело)
    entry_low, entry_high = signal_dict["entry_zone"]
    symbol = signal_dict.get("symbol", "")
    direction = signal_dict.get("direction", "long")
    dedup_key = (
        f"{symbol}:{direction}:"
        f"{round(float(entry_low), 6)}-{round(float(entry_high), 6)}"
    )

    text = _format_signal(signal_dict, tier)
    insert_signal_audit(signal_dict, tier=tier, module="ai_signals")

    tasks = []
    recipients = []
    for chat_id in subscribers:
        # индивидуальный cooldown на пользователя
        if not can_send(chat_id, "ai_signals", dedup_key, COOLDOWN_FREE_SEC):
            continue
        tasks.append(asyncio.create_task(bot.send_message(chat_id, text)))
        recipients.append(chat_id)

    if not tasks:
        return

    results = await asyncio.gather(*tasks, return_exceptions=True)
    for chat_id, res in zip(recipients, results):
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

            subscribers = list_ai_subscribers()
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
    while True:
        mark_tick("ai_signals", extra="сканирую рынок...")

        try:
            symbols = await get_all_usdt_symbols()
            if not symbols:
                mark_error("ai_signals", "no symbols to scan")
                await asyncio.sleep(AI_SCAN_INTERVAL)
                continue

            chunks = [symbols[i : i + CHUNK_SIZE] for i in range(0, len(symbols), CHUNK_SIZE)]
            if not hasattr(signals_worker, "chunk_idx"):
                signals_worker.chunk_idx = 0
            if signals_worker.chunk_idx >= len(chunks):
                signals_worker.chunk_idx = 0
            chunk = chunks[signals_worker.chunk_idx]
            signals_worker.chunk_idx = (signals_worker.chunk_idx + 1) % len(chunks)

            signals = await scan_market(
                symbols=chunk,
                use_btc_gate=False,
                free_mode=True,
                min_score=FREE_MIN_SCORE,
                return_stats=False,
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
            mark_ok(
                "ai_signals",
                extra=f"chunk={signals_worker.chunk_idx}/{len(chunks)} checked={len(chunk)} sent={sent_count}",
            )
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
    insert_signal_audit(signal_dict, tier="pro", module="pro_ai")
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

            top_n = int(os.getenv("PRO_SCAN_TOP_N", "250"))
            symbols = await get_top_usdt_symbols_by_volume(top_n)

            # (опционально) если top не подтянулся — не сканим весь рынок, просто пауза
            if not symbols:
                await asyncio.sleep(60)
                continue

            signals = await scan_market(
                symbols=symbols,
                batch_size=5,
                batch_delay=0.2,
                use_btc_gate=True,
                free_mode=False,
                min_score=PRO_MIN_SCORE,
                return_stats=False,
            )
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
    audit_task = asyncio.create_task(signal_audit_worker_loop())
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
        audit_task.cancel()
        with suppress(asyncio.CancelledError):
            await audit_task
        await close_shared_session()


if __name__ == "__main__":
    asyncio.run(main())
