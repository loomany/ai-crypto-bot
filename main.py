import asyncio
import os
import sqlite3
import time
from contextlib import suppress
from datetime import datetime, timezone, timedelta
from typing import Any, Dict, List, Awaitable

from aiogram import Bot, Dispatcher, F
from aiogram.client.default import DefaultBotProperties
from aiogram.types import CallbackQuery, Message
from aiogram.filters import CommandStart, Command
from dotenv import load_dotenv

from binance_rest import (
    binance_request_context,
    binance_watchdog,
    close_shared_session,
    get_shared_session,
)
from pump_detector import (
    PUMP_CHUNK_SIZE,
    build_pump_symbol_list,
    scan_pumps_chunk,
    format_pump_message,
)
from signals import scan_market
from market_access import get_quick_with_fallback
from trading_core import compute_atr, compute_ema
from symbol_cache import get_all_usdt_symbols, get_top_usdt_symbols_by_volume
from market_regime import get_market_regime
from market_hub import MARKET_HUB
from health import (
    MODULES,
    get_klines_request_count,
    get_request_count,
    mark_tick,
    mark_ok,
    mark_error,
    reset_klines_request_count,
    reset_request_count,
    safe_worker_loop,
    watchdog,
    update_module_progress,
    update_current_symbol,
)
from db import (
    init_db as init_storage_db,
    get_user_pref,
    set_user_pref,
    list_user_ids_with_pref,
    get_state,
    set_state,
    upsert_watchlist_candidate,
    list_watchlist_for_scan,
    update_watchlist_after_signal,
    prune_watchlist,
    get_watchlist_counts,
)
from db_path import get_db_path
from alert_dedup_db import init_alert_dedup, can_send
from status_utils import is_notify_enabled
from message_templates import format_scenario_message
from signal_audit_db import init_signal_audit_tables, insert_signal_audit, get_public_stats
from signal_audit_worker import signal_audit_worker_loop
from keyboards import ai_signals_inline_kb, main_menu_kb, pumpdump_inline_kb
from texts import AI_SIGNALS_TEXT, PUMPDUMP_TEXT, START_TEXT


# ===== ЗАГРУЖАЕМ НАСТРОЙКИ =====

def load_settings() -> str:
    load_dotenv()
    bot_token = os.getenv("BOT_TOKEN")

    if not bot_token:
        raise ValueError("Нет BOT_TOKEN в .env файле")

    return bot_token


ADMIN_USER_ID = int(os.getenv("ADMIN_USER_ID", "0"))


def is_admin(user_id: int) -> bool:
    return ADMIN_USER_ID != 0 and user_id == ADMIN_USER_ID


def _hidden_status_modules() -> set[str]:
    raw = os.getenv("STATUS_HIDE_MODULES", "market_pulse,signal_audit")
    return {item.strip() for item in raw.split(",") if item.strip()}


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


# ===== БАЗА ДАННЫХ =====


def init_app_db():
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

    init_storage_db()
    init_alert_dedup()
    init_signal_audit_tables()
    migrate_legacy_notify_settings()


def migrate_legacy_notify_settings() -> None:
    """
    Переносим legacy подписки в user_prefs.
    """
    conn = sqlite3.connect(get_db_path())
    try:
        cur = conn.cursor()
        try:
            cur.execute("SELECT chat_id, feature, enabled FROM notify_settings")
            notify_rows = cur.fetchall()
        except sqlite3.OperationalError:
            notify_rows = []
        try:
            cur.execute("SELECT chat_id FROM ai_signals_subscribers")
            legacy_rows = cur.fetchall()
        except sqlite3.OperationalError:
            legacy_rows = []
    finally:
        conn.close()

    for chat_id, feature, enabled in notify_rows:
        key = None
        if feature == "ai_signals":
            key = "ai_signals_enabled"
        elif feature == "pumpdump":
            key = "pumpdump_enabled"
        if not key:
            continue
        try:
            set_user_pref(int(chat_id), key, int(enabled))
        except Exception:
            continue

    for (chat_id,) in legacy_rows:
        try:
            set_user_pref(int(chat_id), "ai_signals_enabled", 1)
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
    - основной: user_prefs(key='ai_signals_enabled', value=1)
    - legacy: ai_signals_subscribers (на случай старых баз)
    """
    subs = set(list_user_ids_with_pref("ai_signals_enabled", 1))

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
FREE_MIN_SCORE = 70
COOLDOWN_FREE_SEC = int(os.getenv("AI_SIGNALS_COOLDOWN_SEC", "3600"))
MAX_SIGNALS_PER_CYCLE = 3
MAX_BTC_PER_CYCLE = 1
PULSE_INTERVAL_SEC = 60 * 60
AI_CHUNK_SIZE = int(os.getenv("AI_CHUNK_SIZE", "40"))
AI_UNIVERSE_TOP_N = int(os.getenv("AI_UNIVERSE_TOP_N", "250"))
WATCHLIST_MAX = int(os.getenv("WATCHLIST_MAX", "30"))
WATCHLIST_TTL_MIN = int(os.getenv("WATCHLIST_TTL_MIN", "30"))
WATCHLIST_COOLDOWN_MIN = int(os.getenv("WATCHLIST_COOLDOWN_MIN", "45"))
WATCHLIST_SCAN_EVERY_SEC = int(os.getenv("WATCHLIST_SCAN_EVERY_SEC", "60"))
CANDIDATE_SCORE_MIN = int(os.getenv("CANDIDATE_SCORE_MIN", "60"))
LAST_PULSE_SENT_AT = 0.0


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
    await message.answer(START_TEXT, reply_markup=main_menu_kb())
    await message.answer(f"Ваш ID: {message.chat.id}")


@dp.message(F.text == "🤖 AI-сигналы")
async def ai_signals_menu(message: Message):
    status = "✅ включено" if get_user_pref(message.chat.id, "ai_signals_enabled", 0) else "⛔ выключено"
    await message.answer(
        f"{AI_SIGNALS_TEXT}\n\nСтатус: {status}",
        reply_markup=ai_signals_inline_kb(),
    )


@dp.message(F.text == "⚡ Pump/Dump")
async def pumpdump_menu(message: Message):
    status = "✅ включено" if get_user_pref(message.chat.id, "pumpdump_enabled", 0) else "⛔ выключено"
    await message.answer(
        f"{PUMPDUMP_TEXT}\n\nСтатус: {status}",
        reply_markup=pumpdump_inline_kb(),
    )


@dp.callback_query(F.data == "ai_notify_on")
async def ai_notify_on(callback: CallbackQuery):
    if callback.from_user is None:
        return
    chat_id = callback.from_user.id
    if get_user_pref(chat_id, "ai_signals_enabled", 0):
        await callback.answer("Уже включено.")
        if callback.message:
            await callback.message.answer("ℹ️ AI-уведомления уже включены.")
        return
    set_user_pref(chat_id, "ai_signals_enabled", 1)
    await callback.answer()
    if callback.message:
        await callback.message.answer(
            "✅ AI-уведомления включены."
        )


@dp.callback_query(F.data == "ai_notify_off")
async def ai_notify_off(callback: CallbackQuery):
    if callback.from_user is None:
        return
    chat_id = callback.from_user.id
    if not get_user_pref(chat_id, "ai_signals_enabled", 0):
        await callback.answer("Уже выключено.")
        if callback.message:
            await callback.message.answer("ℹ️ AI-уведомления уже выключены.")
        return
    set_user_pref(chat_id, "ai_signals_enabled", 0)
    await callback.answer()
    if callback.message:
        await callback.message.answer("🚫 Уведомления отключены.")


@dp.callback_query(F.data == "pumpdump_notify_on")
async def pumpdump_notify_on(callback: CallbackQuery):
    if callback.from_user is None:
        return
    chat_id = callback.from_user.id
    if get_user_pref(chat_id, "pumpdump_enabled", 0):
        await callback.answer("Уже включено.")
        if callback.message:
            await callback.message.answer("ℹ️ Pump/Dump уведомления уже включены.")
        return
    set_user_pref(chat_id, "pumpdump_enabled", 1)
    await callback.answer()
    if callback.message:
        await callback.message.answer(
            "✅ Pump/Dump уведомления включены.\n"
            "Теперь бот будет присылать алерты при резких движениях рынка."
        )


@dp.callback_query(F.data == "pumpdump_notify_off")
async def pumpdump_notify_off(callback: CallbackQuery):
    if callback.from_user is None:
        return
    chat_id = callback.from_user.id
    if not get_user_pref(chat_id, "pumpdump_enabled", 0):
        await callback.answer("Уже выключено.")
        if callback.message:
            await callback.message.answer("ℹ️ Pump/Dump уведомления уже выключены.")
        return
    set_user_pref(chat_id, "pumpdump_enabled", 0)
    await callback.answer()
    if callback.message:
        await callback.message.answer("🚫 Pump/Dump уведомления отключены.")


@dp.message(F.text == "/testadmin")
async def test_admin(message: Message):
    ai_subscribers = list_ai_subscribers()
    pump_subscribers = list_user_ids_with_pref("pumpdump_enabled", 1)
    ai_extra = MODULES.get("ai_signals").extra if "ai_signals" in MODULES else ""
    ai_extra = ai_extra.strip()

    def _merge_extra(base: str, extra: str) -> str:
        if not extra:
            return base
        extra_items = [item.strip() for item in extra.split(";") if item.strip()]
        extra_items = [item for item in extra_items if not item.startswith("подписчиков:")]
        if not extra_items:
            return base
        return f"{base}; {'; '.join(extra_items)}"

    ai_subscribers_count = len(ai_subscribers)
    if "ai_signals" in MODULES:
        base = f"подписчиков: {ai_subscribers_count}"
        MODULES["ai_signals"].extra = _merge_extra(base, ai_extra)
    if "pumpdump" in MODULES:
        base = f"подписчиков: {len(pump_subscribers)}"
        MODULES["pumpdump"].extra = _merge_extra(base, MODULES["pumpdump"].extra)

    lines = ["🛠 Статус модулей:\n"]
    now = time.time()
    if MARKET_HUB.last_ok_at:
        ok_ago = int(now - MARKET_HUB.last_ok_at)
        ok_text = f"ok {ok_ago}s ago"
    else:
        ok_text = "ok n/a"
    hub_err = MARKET_HUB.last_error or "-"
    lines.append(
        f"MarketHub: {ok_text} | err: {hub_err} | symbols: {len(MARKET_HUB._symbols)}"
    )
    hidden = _hidden_status_modules()
    for key, st in MODULES.items():
        if key in hidden:
            continue
        lines.append(f"{st.name}:\n{st.as_text()}\n")

    await message.answer("\n".join(lines))


@dp.message(Command("test_notify"))
async def test_notify_cmd(message: Message):
    user_id = message.from_user.id if message.from_user else None
    print("[notify] /test_notify received", user_id, message.chat.id)
    if message.from_user is None or not is_admin(message.from_user.id):
        await message.answer("⛔ Нет доступа")
        return
    target_chat_id = message.chat.id
    try:
        await message.bot.send_message(
            target_chat_id,
            "🧪 Тестовое уведомление: доставка работает.",
        )
        print("[notify] test sent ok")
    except Exception as e:
        print(f"[notify] test failed: {e}")
        await message.answer(f"❌ Ошибка: {e}")


@dp.message(Command("my_id"))
async def my_id_cmd(message: Message):
    user_id = message.from_user.id if message.from_user else "unknown"
    await message.answer(f"user_id={user_id}\nchat_id={message.chat.id}")


def _format_feature_status(chat_id: int, feature: str, label: str) -> str:
    enabled = is_notify_enabled(chat_id, feature)
    status = "✅ включено" if enabled else "⛔ выключено"
    return f"{label}: {status}"


@dp.message(Command("status"))
async def status_cmd(message: Message):
    chat_id = message.chat.id
    lines = [
        _format_feature_status(chat_id, "ai_signals", "AI-сигналы"),
        _format_feature_status(chat_id, "pumpdump", "Pump/Dump"),
    ]
    await message.answer("\n".join(lines))


@dp.message(F.text == "⬅️ Главное меню")
async def back_to_main(message: Message):
    await message.answer("Возвращаемся в главное меню.", reply_markup=main_menu_kb())


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


@dp.message(F.text == "/stats")
async def show_stats(message: Message):
    if message.from_user is None or not is_admin(message.from_user.id):
        return
    stats = get_public_stats(days=30)
    await message.answer(_format_stats_message(stats), reply_markup=main_menu_kb())


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

def _format_signal(signal: Dict[str, Any]) -> str:
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


async def send_signal_to_all(signal_dict: Dict[str, Any]):
    """Отправляет сигнал всем подписчикам без блокировки event loop."""
    if bot is None:
        print("[ai_signals] Bot is not initialized; skipping send.")
        return

    skipped_dedup = 0
    skipped_no_subs = 0
    subscribers = list_ai_subscribers()
    if not subscribers:
        skipped_no_subs += 1
        print("[ai_signals] deliver: subs=0 queued=0 dedup=0")
        return

    entry_low, entry_high = signal_dict["entry_zone"]
    symbol = signal_dict.get("symbol", "")
    direction = signal_dict.get("direction", "long")
    time_bucket = int(time.time() // 3600)
    if symbol == "BTCUSDT":
        bucket = 50.0
        e1 = round(float(entry_low) / bucket) * bucket
        e2 = round(float(entry_high) / bucket) * bucket
    else:
        e1 = round(float(entry_low), 4)
        e2 = round(float(entry_high), 4)

    dedup_key = f"{symbol}:{direction}:1h:{time_bucket}:{e1}-{e2}"

    text = _format_signal(signal_dict)
    insert_signal_audit(signal_dict, tier="free", module="ai_signals")

    tasks = []
    recipients = []
    for chat_id in subscribers:
        # индивидуальный cooldown на пользователя
        if not can_send(chat_id, "ai_signals", dedup_key, COOLDOWN_FREE_SEC):
            skipped_dedup += 1
            continue
        tasks.append(asyncio.create_task(bot.send_message(chat_id, text)))
        recipients.append(chat_id)

    print(
        "[ai_signals] deliver: "
        f"subs={len(subscribers)} queued={len(tasks)} "
        f"dedup={skipped_dedup}"
    )
    if not tasks:
        return

    results = await asyncio.gather(*tasks, return_exceptions=True)
    for chat_id, res in zip(recipients, results):
        if isinstance(res, Exception):
            print(f"[ai_signals] Failed to send to {chat_id}: {res}")


async def market_pulse_scan_once() -> None:
    global LAST_PULSE_SENT_AT

    if bot is None:
        mark_tick("market_pulse", extra="bot not ready")
        return

    subscribers = list_ai_subscribers()
    if not subscribers:
        mark_tick("market_pulse", extra="нет подписчиков")
        return

    now = time.time()
    since_last = now - LAST_PULSE_SENT_AT
    if since_last < PULSE_INTERVAL_SEC:
        wait_left = int(PULSE_INTERVAL_SEC - since_last)
        mark_ok("market_pulse", extra=f"следующий пульс через {wait_left}s")
        return

    regime_info = await get_market_regime()
    regime = regime_info.get("regime", "neutral")
    regime_label = {
        "risk_on": "RISK-ON",
        "risk_off": "RISK-OFF",
        "neutral": "NEUTRAL",
    }.get(regime, "NEUTRAL")

    trend_1d = regime_info.get("trend_1d", "n/a")
    trend_4h = regime_info.get("trend_4h", "n/a")
    trend_1h = regime_info.get("trend_1h", "n/a")
    ema_fast = regime_info.get("ema_fast")
    ema_slow = regime_info.get("ema_slow")
    rsi_15m = regime_info.get("rsi_15m")
    allow_longs = regime_info.get("allow_longs")
    allow_shorts = regime_info.get("allow_shorts")
    reason = regime_info.get("reason") or regime_info.get("description") or "Нет причины."

    def _fmt_ema(value: Any) -> str:
        return f"{value:.2f}" if isinstance(value, (int, float)) else "n/a"

    def _fmt_bool(value: Any) -> str:
        if value is True:
            return "yes"
        if value is False:
            return "no"
        return "n/a"

    rsi_text = f"{rsi_15m:.1f}" if isinstance(rsi_15m, (int, float)) else "n/a"

    text = (
        "📡 Market Pulse (каждый час)\n"
        f"BTC режим: {regime_label}\n"
        f"trend: 1D={trend_1d} 4H={trend_4h} 1H={trend_1h}\n"
        f"ema_fast={_fmt_ema(ema_fast)} ema_slow={_fmt_ema(ema_slow)}\n"
        f"rsi15={rsi_text}\n"
        f"allow_longs={_fmt_bool(allow_longs)} allow_shorts={_fmt_bool(allow_shorts)}\n"
        f"reason: {reason}"
    )

    tasks = [asyncio.create_task(bot.send_message(chat_id, text)) for chat_id in subscribers]
    await asyncio.gather(*tasks, return_exceptions=True)
    LAST_PULSE_SENT_AT = now
    mark_ok("market_pulse", extra="пульс отправлен")


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


async def _get_ai_universe() -> List[str]:
    symbols: List[str] = []
    try:
        symbols = await get_top_usdt_symbols_by_volume(AI_UNIVERSE_TOP_N)
    except Exception as exc:
        print(f"[ai_signals] top-n universe failed: {exc}")
    if not symbols:
        symbols = await get_all_usdt_symbols()
    return symbols


async def _compute_candidate_score(symbol: str) -> tuple[int, str]:
    candles = await get_quick_with_fallback(symbol)
    if not candles:
        return 0, ""
    candles_1h = candles.get("1h") or []
    candles_15m = candles.get("15m") or []
    if len(candles_1h) < 2 or len(candles_15m) < 2:
        return 0, ""

    score = 0
    reason_scores: dict[str, int] = {}

    vols = [float(c.volume) for c in candles_15m[-21:]]
    if len(vols) > 1:
        avg_vol = sum(vols[:-1]) / max(len(vols) - 1, 1)
        last_vol = vols[-1]
        volume_ratio = last_vol / avg_vol if avg_vol > 0 else 0.0
        if volume_ratio >= 1.6:
            reason_scores["volume_spike"] = 30

    atr_now = compute_atr(candles_1h, 14)
    atr_prev = compute_atr(candles_1h[:-1], 14) if len(candles_1h) > 15 else None
    if atr_now and atr_prev and atr_prev > 0 and atr_now >= atr_prev * 1.15:
        reason_scores["atr"] = 20

    closes_1h = [float(c.close) for c in candles_1h]
    ema50 = compute_ema(closes_1h, 50)
    last_close = closes_1h[-1]
    if ema50 and last_close > 0:
        distance_pct = abs(last_close - ema50) / last_close * 100
        if distance_pct <= 1.0:
            reason_scores["near_poi"] = 20

    last_candle = candles_1h[-1]
    if float(last_candle.open) > 0:
        change_pct = abs((float(last_candle.close) - float(last_candle.open)) / float(last_candle.open) * 100)
        if change_pct >= 3.0:
            reason_scores["pump"] = 30

    score = sum(reason_scores.values())
    if not reason_scores:
        return 0, ""
    reason = max(reason_scores.items(), key=lambda item: item[1])[0]
    return min(score, 100), reason


async def pump_scan_once(bot: Bot) -> None:
    start = time.time()
    BUDGET = 35
    log_level = int(os.getenv("PUMPDUMP_LOG_LEVEL", "1"))  # 0=off,1=cycle,2=candidates,3=sends
    print("[PUMP] scan_once start")
    if not hasattr(pump_scan_once, "state"):
        pump_scan_once.state = {
            "last_sent": {},
            "symbols": [],
            "cursor": 0,
        }

    try:
        state = pump_scan_once.state

        subscribers = list_user_ids_with_pref("pumpdump_enabled", 1)

        if log_level >= 1:
            print(f"[pumpdump] subs: notify={len(subscribers)}")

        mark_tick("pumpdump", extra=f"подписчиков: {len(subscribers)}")

        if not subscribers:
            if log_level >= 1:
                print("[pumpdump] no notify subscribers -> skip")
            return

        session = await get_shared_session()
        symbols = state["symbols"]
        cursor = state["cursor"]
        if not symbols or cursor >= len(symbols):
            symbols = await build_pump_symbol_list(session)
            cursor = 0
            state["symbols"] = symbols

        if not symbols:
            mark_error("pumpdump", "no symbols to scan")
            return
        total = len(symbols)
        update_current_symbol("pumpdump", symbols[cursor] if cursor < total else "")

        cycle_start = time.time()
        try:
            signals, stats, next_cursor = await asyncio.wait_for(
                scan_pumps_chunk(
                    symbols,
                    start_idx=cursor,
                    time_budget_sec=BUDGET,
                    return_stats=True,
                ),
                timeout=BUDGET,
            )
        except asyncio.TimeoutError:
            return
        state["cursor"] = next_cursor
        found = stats.get("found", len(signals) if isinstance(signals, list) else 0)

        if log_level >= 1:
            print(
                f"[pumpdump] chunk: cursor={cursor} -> next={next_cursor} "
                f"total={len(symbols)} checked={stats.get('checked',0)} found={found}"
            )

        if log_level >= 2 and signals:
            for s in signals[:10]:
                print(
                    f"[pumpdump] candidate {s.get('symbol')} "
                    f"type={s.get('type')} "
                    f"1m={s.get('change_1m')}% "
                    f"5m={s.get('change_5m')}% "
                    f"volx={s.get('volume_mul')}"
                )
        chunk_len = min(PUMP_CHUNK_SIZE, total - cursor) if total else 0
        checked = stats.get("checked", 0)
        update_module_progress("pumpdump", total, next_cursor, checked)

        now_min = int(time.time() // 60)
        sent_count = 0
        last_sent: dict[str, int] = state["last_sent"]

        for sig in signals:
            if time.time() - start > BUDGET:
                print("[PUMP] budget exceeded, stopping early")
                break
            symbol = sig["symbol"]
            update_current_symbol("pumpdump", symbol)

            if last_sent.get(symbol) == now_min:
                continue

            text = format_pump_message(sig)

            last_sent[symbol] = now_min
            for chat_id in subscribers:
                try:
                    await bot.send_message(chat_id, text, parse_mode="Markdown")
                    sent_count += 1
                except Exception as e:
                    print(f"[pumpdump] send failed chat_id={chat_id} symbol={symbol}: {e}")
                    continue

        cycle_sec = time.time() - cycle_start
        current_symbol = MODULES.get("pumpdump").current_symbol if "pumpdump" in MODULES else None
        if log_level >= 1:
            print(f"[pumpdump] cycle done: found={found} sent={sent_count}")
        mark_ok(
            "pumpdump",
            extra=(
                f"progress={next_cursor}/{total} "
                f"checked={checked}/{chunk_len} found={found} sent={sent_count} "
                f"current={current_symbol or '-'} cycle={int(cycle_sec)}s"
            ),
        )
    finally:
        print("[PUMP] scan_once end")


async def ai_scan_once() -> None:
    start = time.time()
    BUDGET = 35
    print("[AI] scan_once start")
    try:
        reset_request_count("ai_signals")
        reset_klines_request_count("ai_signals")
        mark_tick("ai_signals", extra="сканирую рынок...")

        symbols = await _get_ai_universe()
        if not symbols:
            mark_error("ai_signals", "no symbols to scan")
            return
        total = len(symbols)

        if not hasattr(ai_scan_once, "cursor"):
            stored_cursor = get_state("ai_cursor", "0")
            try:
                ai_scan_once.cursor = int(stored_cursor) if stored_cursor is not None else 0
            except (TypeError, ValueError):
                ai_scan_once.cursor = 0
        cursor = ai_scan_once.cursor
        if cursor >= total:
            cursor = 0
            set_state("ai_cursor", "0")
        chunk = symbols[cursor : cursor + AI_CHUNK_SIZE]
        new_cursor = cursor + len(chunk)
        if new_cursor >= total:
            new_cursor = 0
        ai_scan_once.cursor = new_cursor
        set_state("ai_cursor", str(new_cursor))
        update_module_progress("ai_signals", total, new_cursor, len(chunk))
        if chunk:
            update_current_symbol("ai_signals", chunk[0])

        now = int(time.time())
        added = 0
        with binance_request_context("ai_signals"):
            for symbol in chunk:
                if time.time() - start > BUDGET:
                    print("[AI] budget exceeded, stopping early")
                    break
                score, reason = await _compute_candidate_score(symbol)
                if score < CANDIDATE_SCORE_MIN:
                    continue
                ttl_until = now + WATCHLIST_TTL_MIN * 60
                inserted = upsert_watchlist_candidate(
                    symbol,
                    score,
                    reason,
                    ttl_until,
                    last_seen=now,
                )
                if inserted:
                    added += 1

        pruned = prune_watchlist(now, WATCHLIST_MAX)
        active_watchlist, total_watchlist = get_watchlist_counts(now)
        current_symbol = MODULES.get("ai_signals").current_symbol if "ai_signals" in MODULES else None
        req_count = get_request_count("ai_signals")
        klines_count = get_klines_request_count("ai_signals")
        print(
            "[AI] "
            f"universe={total} chunk={len(chunk)} cursor={new_cursor} "
            f"watchlist={active_watchlist}/{total_watchlist} added={added} pruned={pruned}"
        )
        mark_ok(
            "ai_signals",
            extra=(
                f"universe={total} chunk={len(chunk)} cursor={new_cursor} "
                f"watchlist={active_watchlist}/{total_watchlist} added={added} pruned={pruned} "
                f"current={current_symbol or '-'} cycle={int(time.time() - start)}s "
                f"req={req_count} klines={klines_count}"
            ),
        )
    finally:
        print("[AI] scan_once end")


async def watchlist_scan_once() -> None:
    start = time.time()
    BUDGET = 35
    if bot is None:
        mark_tick("ai_signals", extra="bot not ready")
        return
    now = int(time.time())
    rows = list_watchlist_for_scan(now, WATCHLIST_MAX)
    symbols = [row["symbol"] for row in rows]
    if not symbols:
        active_watchlist, total_watchlist = get_watchlist_counts(now)
        mark_ok(
            "ai_signals",
            extra=f"watchlist={active_watchlist}/{total_watchlist} symbols=0",
        )
        return

    update_current_symbol("ai_signals", symbols[0])
    with binance_request_context("ai_signals"):
        signals, stats = await scan_market(
            symbols=symbols,
            use_btc_gate=False,
            free_mode=True,
            min_score=FREE_MIN_SCORE,
            return_stats=True,
            time_budget=BUDGET,
        )
    print("[ai_signals] watchlist stats:", stats)
    sent_count = 0
    for signal in _select_signals_for_cycle(signals):
        if time.time() - start > BUDGET:
            print("[AI] watchlist budget exceeded, stopping early")
            break
        score = signal.get("score", 0)
        if score < FREE_MIN_SCORE:
            continue
        update_current_symbol("ai_signals", signal.get("symbol", ""))
        print(
            f"[ai_signals] WATCHLIST SEND {signal['symbol']} {signal['direction']} score={score}"
        )
        await send_signal_to_all(signal)
        sent_count += 1
        ttl_until = now + WATCHLIST_TTL_MIN * 60
        cooldown_until = now + WATCHLIST_COOLDOWN_MIN * 60
        update_watchlist_after_signal(
            signal["symbol"],
            ttl_until=ttl_until,
            cooldown_until=cooldown_until,
            last_seen=now,
        )

    pruned = prune_watchlist(now, WATCHLIST_MAX)
    active_watchlist, total_watchlist = get_watchlist_counts(now)
    mark_ok(
        "ai_signals",
        extra=(
            f"watchlist={active_watchlist}/{total_watchlist} "
            f"scanned={len(symbols)} sent={sent_count} pruned={pruned} "
            f"cycle={int(time.time() - start)}s"
        ),
    )


async def watchlist_worker_loop() -> None:
    while True:
        cycle_start = time.perf_counter()
        timeout_s = max(15, min(55, WATCHLIST_SCAN_EVERY_SEC))
        print("[ai_signals] watchlist cycle start")
        mark_tick("ai_signals", extra="watchlist heartbeat")
        t0 = time.perf_counter()
        try:
            await asyncio.wait_for(watchlist_scan_once(), timeout=timeout_s)
            print(f"[ai_signals] watchlist cycle ok, dt={time.perf_counter() - t0:.2f}s")
        except asyncio.TimeoutError:
            print(
                f"[ai_signals] watchlist TIMEOUT >{timeout_s}s, dt={time.perf_counter() - t0:.2f}s"
            )
            mark_error("ai_signals", f"watchlist timeout >{timeout_s}s")
        except Exception as e:
            print(f"[ai_signals] watchlist ERROR {type(e).__name__}: {e}")
            mark_error("ai_signals", str(e))
        elapsed = time.perf_counter() - cycle_start
        await asyncio.sleep(max(0, WATCHLIST_SCAN_EVERY_SEC - elapsed))


# ===== ТОЧКА ВХОДА =====

async def main():
    global bot
    bot = Bot(token=BOT_TOKEN, default=DefaultBotProperties(parse_mode="HTML"))
    print("Бот запущен!")
    init_app_db()
    async def _delayed_task(delay_sec: float, coro: Awaitable[Any]):
        await asyncio.sleep(delay_sec)
        return await coro

    pulse_task = asyncio.create_task(safe_worker_loop("market_pulse", market_pulse_scan_once))
    pump_task = asyncio.create_task(
        _delayed_task(6, safe_worker_loop("pumpdump", lambda: pump_scan_once(bot)))
    )
    print(f"[ai_signals] AI_CHUNK_SIZE={AI_CHUNK_SIZE}")
    signals_task = asyncio.create_task(
        _delayed_task(12, safe_worker_loop("ai_signals", ai_scan_once))
    )
    watchlist_task = asyncio.create_task(_delayed_task(14, watchlist_worker_loop()))
    audit_task = asyncio.create_task(_delayed_task(18, signal_audit_worker_loop()))
    watchdog_task = asyncio.create_task(watchdog())
    binance_watchdog_task = asyncio.create_task(binance_watchdog())
    try:
        await MARKET_HUB.start()
        print("[market_hub] started")
        await dp.start_polling(bot)
    finally:
        await MARKET_HUB.stop()
        signals_task.cancel()
        with suppress(asyncio.CancelledError):
            await signals_task
        pulse_task.cancel()
        with suppress(asyncio.CancelledError):
            await pulse_task
        pump_task.cancel()
        with suppress(asyncio.CancelledError):
            await pump_task
        watchlist_task.cancel()
        with suppress(asyncio.CancelledError):
            await watchlist_task
        audit_task.cancel()
        with suppress(asyncio.CancelledError):
            await audit_task
        watchdog_task.cancel()
        with suppress(asyncio.CancelledError):
            await watchdog_task
        binance_watchdog_task.cancel()
        with suppress(asyncio.CancelledError):
            await binance_watchdog_task
        await close_shared_session()


if __name__ == "__main__":
    asyncio.run(main())
