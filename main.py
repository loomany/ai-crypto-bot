import asyncio
import json
import os
import sqlite3
import time
import random
from contextlib import suppress
from datetime import datetime, timezone, timedelta
from typing import Any, Dict, List, Awaitable

from aiogram import Bot, Dispatcher, F
from aiogram.client.default import DefaultBotProperties
from aiogram.types import CallbackQuery, InlineKeyboardButton, InlineKeyboardMarkup, Message
from aiogram.filters import CommandStart, Command
from dotenv import load_dotenv

from binance_rest import (
    binance_request_context,
    binance_watchdog,
    close_shared_session,
    get_shared_session,
    get_klines_cache_stats,
    reset_klines_cache_stats,
)
from pump_detector import (
    PUMP_CHUNK_SIZE,
    get_candidate_symbols,
    scan_pumps_chunk,
    format_pump_message,
)
from signals import (
    scan_market,
    get_cached_btc_context,
    get_btc_context,
    get_btc_context_last_error,
    get_btc_context_last_refresh_ts,
)
from market_access import get_quick_with_fallback
from trading_core import compute_atr, compute_ema
from symbol_cache import (
    filter_tradeable_symbols,
    get_all_usdt_symbols,
    get_top_usdt_symbols_by_volume,
)
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
    kv_get_int,
    kv_set_int,
    upsert_watchlist_candidate,
    list_watchlist_for_scan,
    update_watchlist_after_signal,
    prune_watchlist,
    get_watchlist_counts,
    insert_signal_event,
    list_signal_events,
    count_signal_events,
    get_signal_outcome_counts,
    get_signal_event,
)
from db_path import ensure_db_writable, get_db_path
from market_cache import get_ticker_request_count, reset_ticker_request_count
from alert_dedup_db import init_alert_dedup, can_send
from status_utils import is_notify_enabled
from message_templates import format_scenario_message
from signal_audit_db import (
    get_public_stats,
    init_signal_audit_tables,
    insert_signal_audit,
)
from signal_audit_worker import signal_audit_worker_loop
from keyboards import (
    ai_signals_inline_kb,
    build_main_menu_kb,
    build_system_menu_kb,
    pumpdump_inline_kb,
    stats_inline_kb,
)
from texts import AI_SIGNALS_TEXT, PUMPDUMP_TEXT, START_TEXT
from settings import SIGNAL_TTL_SECONDS


# ===== ЗАГРУЖАЕМ НАСТРОЙКИ =====

def load_settings() -> str:
    load_dotenv()
    bot_token = os.getenv("BOT_TOKEN")

    if not bot_token:
        raise ValueError("Нет BOT_TOKEN в .env файле")

    return bot_token


ADMIN_USER_ID = int(os.getenv("ADMIN_USER_ID", "0"))
PUMP_COOLDOWN_SYMBOL_SEC = int(os.getenv("PUMP_COOLDOWN_SYMBOL_SEC", "21600"))  # 6h
PUMP_COOLDOWN_GLOBAL_SEC = int(os.getenv("PUMP_COOLDOWN_GLOBAL_SEC", "3600"))  # 1h
PUMP_DAILY_LIMIT = int(os.getenv("PUMP_DAILY_LIMIT", "6"))


def _env_bool(name: str, default: str = "0") -> bool:
    return os.getenv(name, default).strip().lower() in ("1", "true", "yes", "on")


def get_use_btc_gate() -> bool:
    return _env_bool("USE_BTC_GATE", "0")


def is_admin(user_id: int) -> bool:
    return ADMIN_USER_ID != 0 and user_id == ADMIN_USER_ID


def _hidden_status_modules() -> set[str]:
    raw = os.getenv("STATUS_HIDE_MODULES", "signal_audit")
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
    db_path = ensure_db_writable()
    print(f"[DB] using sqlite at: {db_path}")
    conn = sqlite3.connect(get_db_path())
    try:
        conn.execute(
            "CREATE TABLE IF NOT EXISTS ai_signals_subscribers (chat_id INTEGER PRIMARY KEY)"
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS pumpdump_daily_counts (
                chat_id INTEGER NOT NULL,
                date TEXT NOT NULL,
                feature TEXT NOT NULL,
                count INTEGER NOT NULL DEFAULT 0,
                PRIMARY KEY (chat_id, date, feature)
            )
            """
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


def _get_pumpdump_date_key(now: datetime | None = None) -> str:
    if now is None:
        now = datetime.now(timezone.utc)
    return now.date().isoformat()


def get_pumpdump_daily_count(chat_id: int, date_key: str) -> int:
    conn = sqlite3.connect(get_db_path())
    try:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT count
            FROM pumpdump_daily_counts
            WHERE chat_id = ? AND date = ? AND feature = ?
            """,
            (chat_id, date_key, "pumpdump"),
        )
        row = cur.fetchone()
        return int(row[0]) if row else 0
    finally:
        conn.close()


def increment_pumpdump_daily_count(chat_id: int, date_key: str) -> None:
    conn = sqlite3.connect(get_db_path())
    try:
        conn.execute(
            """
            INSERT INTO pumpdump_daily_counts (chat_id, date, feature, count)
            VALUES (?, ?, ?, 1)
            ON CONFLICT(chat_id, date, feature)
            DO UPDATE SET count = count + 1
            """,
            (chat_id, date_key, "pumpdump"),
        )
        conn.commit()
    finally:
        conn.close()


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
AI_CHUNK_SIZE = int(os.getenv("AI_CHUNK_SIZE", "40"))
AI_PRIORITY_N = int(os.getenv("AI_PRIORITY_N", "15"))
AI_UNIVERSE_TOP_N = int(os.getenv("AI_UNIVERSE_TOP_N", "250"))
WATCHLIST_MAX = int(os.getenv("WATCHLIST_MAX", "30"))
WATCHLIST_TTL_MIN = int(os.getenv("WATCHLIST_TTL_MIN", "30"))
WATCHLIST_COOLDOWN_MIN = int(os.getenv("WATCHLIST_COOLDOWN_MIN", "45"))
WATCHLIST_SCAN_EVERY_SEC = int(os.getenv("WATCHLIST_SCAN_EVERY_SEC", "60"))
AI_DEEP_TOP_K = int(os.getenv("AI_DEEP_TOP_K", os.getenv("AI_MAX_DEEP_PER_CYCLE", "3")))
CANDIDATE_SCORE_MIN = int(os.getenv("CANDIDATE_SCORE_MIN", "60"))


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
    await message.answer(
        START_TEXT,
        reply_markup=build_main_menu_kb(
            is_admin=is_admin(message.from_user.id) if message.from_user else False
        ),
    )
    await message.answer(f"Ваш ID: {message.chat.id}")


@dp.message(F.text == "🎯 AI-сигналы")
async def ai_signals_menu(message: Message):
    status = "✅ включено" if get_user_pref(message.chat.id, "ai_signals_enabled", 0) else "⛔ выключено"
    await message.answer(
        f"{AI_SIGNALS_TEXT}\n\nСтатус: {status}",
        reply_markup=ai_signals_inline_kb(),
    )


@dp.message(F.text == "⚡ Pump / Dump")
async def pumpdump_menu(message: Message):
    status = "✅ включено" if get_user_pref(message.chat.id, "pumpdump_enabled", 0) else "⛔ выключено"
    await message.answer(
        f"{PUMPDUMP_TEXT}\n\nСтатус: {status}",
        reply_markup=pumpdump_inline_kb(),
    )


def _period_label(period_key: str) -> str:
    mapping = {
        "1d": "1 день",
        "7d": "7 дней",
        "30d": "30 дней",
        "all": "Все время",
    }
    return mapping.get(period_key, "Все время")


def _format_ai_stats_message(stats: Dict[str, Any], period_key: str) -> str:
    title = f"📊 Статистика AI-сигналов ({_period_label(period_key)})"
    total = stats.get("total", 0)
    disclaimer = "ℹ️ Это статистика отработки сценариев по рынку, не гарантия прибыли."

    if total == 0:
        return f"{title}\nНет завершенных сигналов за период.\n\n{disclaimer}"

    tp1 = stats.get("tp1", 0)
    tp2 = stats.get("tp2", 0)
    sl = stats.get("sl", 0)
    exp = stats.get("exp", 0)
    winrate = stats.get("winrate", 0.0)
    buckets = stats.get("buckets", {})

    def _bucket_line(label: str, key: str) -> str:
        data = buckets.get(key, {"total": 0, "winrate": 0.0})
        total_bucket = data.get("total", 0)
        win_bucket = data.get("winrate", 0.0)
        return f"{label}:  {total_bucket} (TP1+: {win_bucket:.0f}%)"

    lines = [
        title,
        "",
        f"Сигналов: {total}",
        f"TP1+: {tp1} | TP2: {tp2} | SL: {sl} | Exp: {exp}",
        f"Winrate (TP1+): {winrate:.1f}%",
        "",
        "Score:",
        _bucket_line("0–69", "0-69"),
        _bucket_line("70–79", "70-79"),
        _bucket_line("80+", "80-100"),
        "",
        disclaimer,
    ]
    return "\n".join(lines)


def _period_days(period_key: str) -> int | None:
    if period_key == "1d":
        return 1
    if period_key == "7d":
        return 7
    if period_key == "30d":
        return 30
    return None


def _status_icon(status: str) -> str:
    passed = {"TP1", "TP2", "BE"}
    failed = {"SL", "EXP", "EXPIRED", "NO_FILL"}
    if status in passed:
        return "✅"
    if status in failed:
        return "❌"
    return "⏳"


def _format_signal_event_status(raw_status: str) -> str:
    status_map = {
        "OPEN": "Открыт",
        "TP1": "TP1",
        "TP2": "TP2",
        "SL": "SL",
        "EXP": "EXP",
        "EXPIRED": "EXP",
        "BE": "BE",
        "NO_FILL": "Нет входа",
        "AMBIGUOUS": "Спорно",
    }
    return status_map.get(raw_status, raw_status)


def _format_event_time(ts: int) -> str:
    dt = datetime.fromtimestamp(ts, tz=ALMATY_TZ)
    return dt.strftime("%d.%m %H:%M")


def _format_archive_list(
    period_key: str,
    events: list[dict],
    page: int,
    pages: int,
    outcome_counts: dict,
) -> str:
    title = f"📊 История сигналов ({_period_label(period_key)})"
    lines = [title]
    lines.append(
        f"✅ Прошло: {outcome_counts.get('passed', 0)} | "
        f"❌ Не прошло: {outcome_counts.get('failed', 0)}"
    )
    lines.append(
        "TP1: {tp1} | TP2: {tp2} | BE: {be} | SL: {sl} | EXP: {exp} | NF: {no_fill}".format(
            tp1=outcome_counts.get("tp1", 0),
            tp2=outcome_counts.get("tp2", 0),
            be=outcome_counts.get("be", 0),
            sl=outcome_counts.get("sl", 0),
            exp=outcome_counts.get("exp", 0),
            no_fill=outcome_counts.get("no_fill", 0),
        )
    )
    if not events:
        lines.append("Нет сигналов за период.")
        return "\n".join(lines)

    lines.append(f"Страница {page}/{pages}")
    lines.append("")
    for idx, event in enumerate(events, start=1):
        status_icon = _status_icon(str(event.get("status", "")))
        lines.append(
            f"{status_icon} {idx}) Score {int(event.get('score', 0))} — "
            f"{event.get('symbol')} {event.get('side')} | "
            f"{_format_event_time(int(event.get('ts', 0)))}"
        )
    return "\n".join(lines)


def _format_archive_detail(event: dict) -> str:
    score = int(event.get("score", 0))
    breakdown_lines: list[str] = []
    raw_breakdown = event.get("breakdown_json")
    if raw_breakdown:
        try:
            breakdown_items = json.loads(raw_breakdown)
        except json.JSONDecodeError:
            breakdown_items = []
        if isinstance(breakdown_items, list):
            label_map = {
                "global_trend": "Глобальный тренд (1D)",
                "local_trend": "Локальный тренд (1H)",
                "near_key_level": "Реакция на ключевую зону (POI)",
                "liquidity_sweep": "Снос ликвидности",
                "volume_climax": "Объём относительно среднего",
                "rsi_divergence": "RSI-дивергенция",
                "atr_ok": "Волатильность (ATR)",
                "bb_extreme": "Экстремум Bollinger",
                "ma_trend_ok": "EMA-согласование",
                "orderflow": "Ордерфлоу",
                "whale_activity": "Китовая активность",
                "ai_pattern": "AI-паттерны",
                "market_regime": "Рыночный режим",
            }
            for item in breakdown_items:
                if not isinstance(item, dict):
                    continue
                key = item.get("key")
                label = item.get("label")
                if key in label_map:
                    label = label_map[key]
                label = label or key or "Фактор"
                delta = item.get("points", item.get("delta", 0))
                try:
                    delta_value = int(round(float(delta)))
                except (TypeError, ValueError):
                    delta_value = 0
                sign = "−" if delta_value < 0 else "+"
                breakdown_lines.append(f"• {label}: {sign}{abs(delta_value)}")

    lines = [
        f"📌 {event.get('symbol')} {event.get('side')} {score}",
        f"🕒 {_format_event_time(int(event.get('ts', 0)))}",
        f"POI: {float(event.get('poi_low')):.4f} - {float(event.get('poi_high')):.4f}",
        f"SL: {float(event.get('sl')):.4f}",
        f"TP1: {float(event.get('tp1')):.4f}",
        f"TP2: {float(event.get('tp2')):.4f}",
        f"⏱ Время жизни сценария: {SIGNAL_TTL_SECONDS // 3600} часов",
    ]
    if breakdown_lines:
        lines.extend(
            [
                "",
                f"🧠 Почему выбран сигнал (Score {score}):",
                *breakdown_lines,
            ]
        )
    return "\n".join(lines)


@dp.message(F.text == "📊 Статистика")
async def stats_menu(message: Message):
    await message.answer(
        "📊 История сигналов\nВыбери период:",
        reply_markup=stats_inline_kb(),
    )


@dp.callback_query(F.data.regexp(r"^history:(1d|7d|30d|all)$"))
async def history_callback(callback: CallbackQuery):
    if callback.message is None or callback.from_user is None:
        return
    period_key = callback.data.split(":", 1)[1]
    page = 1
    days = _period_days(period_key)
    since_ts = int(time.time()) - days * 86400 if days is not None else None
    total = count_signal_events(
        user_id=callback.from_user.id,
        since_ts=since_ts,
        min_score=None,
    )
    print(f"[history] period={period_key} total={total}")
    pages = max(1, (total + 9) // 10)
    events_rows = list_signal_events(
        user_id=callback.from_user.id,
        since_ts=since_ts,
        min_score=None,
        limit=10,
        offset=0,
    )
    events = [dict(row) for row in events_rows]
    outcome_counts = get_signal_outcome_counts(
        user_id=callback.from_user.id,
        since_ts=since_ts,
        min_score=None,
    )
    await callback.answer()
    await callback.message.edit_text(
        _format_archive_list(period_key, events, page, pages, outcome_counts),
        reply_markup=_archive_inline_kb(period_key, page, pages, events),
    )


def _archive_inline_kb(
    period_key: str,
    page: int,
    pages: int,
    events: list[dict],
) -> InlineKeyboardMarkup:
    rows: list[list[InlineKeyboardButton]] = []
    for idx, event in enumerate(events, start=1):
        status_icon = _status_icon(str(event.get("status", "")))
        rows.append(
            [
                InlineKeyboardButton(
                    text=(
                        f"{status_icon} Score {int(event.get('score', 0))} — "
                        f"{event.get('symbol')} {event.get('side')}"
                    ),
                    callback_data=f"archive:detail:{period_key}:{page}:{event.get('id')}",
                )
            ]
        )

    nav_row: list[InlineKeyboardButton] = []
    if page > 1:
        nav_row.append(
            InlineKeyboardButton(
                text="⬅️ Prev",
                callback_data=f"archive:list:{period_key}:{page - 1}",
            )
        )
    if page < pages:
        nav_row.append(
            InlineKeyboardButton(
                text="Next ➡️",
                callback_data=f"archive:list:{period_key}:{page + 1}",
            )
        )
    if nav_row:
        rows.append(nav_row)
    rows.append(
        [
            InlineKeyboardButton(
                text="⬅️ Назад",
                callback_data=f"archive:back:{period_key}",
            )
        ]
    )
    return InlineKeyboardMarkup(inline_keyboard=rows)


def _archive_detail_kb(period_key: str, page: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(
                    text="⬅️ Назад",
                    callback_data=f"archive:list:{period_key}:{page}",
                )
            ]
        ]
    )


@dp.callback_query(F.data.regexp(r"^archive:list:(1d|7d|30d|all):\d+$"))
async def archive_list(callback: CallbackQuery):
    if callback.message is None or callback.from_user is None:
        return
    _, _, period_key, page_raw = callback.data.split(":")
    page = max(1, int(page_raw))
    days = _period_days(period_key)
    since_ts = int(time.time()) - days * 86400 if days is not None else None
    total = count_signal_events(
        user_id=callback.from_user.id,
        since_ts=since_ts,
        min_score=None,
    )
    pages = max(1, (total + 9) // 10)
    if page > pages:
        page = pages
    events_rows = list_signal_events(
        user_id=callback.from_user.id,
        since_ts=since_ts,
        min_score=None,
        limit=10,
        offset=(page - 1) * 10,
    )
    events = [dict(row) for row in events_rows]
    outcome_counts = get_signal_outcome_counts(
        user_id=callback.from_user.id,
        since_ts=since_ts,
        min_score=None,
    )
    await callback.answer()
    await callback.message.edit_text(
        _format_archive_list(period_key, events, page, pages, outcome_counts),
        reply_markup=_archive_inline_kb(period_key, page, pages, events),
    )


@dp.callback_query(F.data.regexp(r"^archive:detail:(1d|7d|30d|all):\d+:\d+$"))
async def archive_detail(callback: CallbackQuery):
    if callback.message is None or callback.from_user is None:
        return
    _, _, period_key, page_raw, event_id_raw = callback.data.split(":")
    page = max(1, int(page_raw))
    event = get_signal_event(
        user_id=callback.from_user.id,
        event_id=int(event_id_raw),
    )
    if event is None:
        await callback.answer("Сигнал не найден.", show_alert=True)
        return
    await callback.answer()
    await callback.message.edit_text(
        _format_archive_detail(dict(event)),
        reply_markup=_archive_detail_kb(period_key, page),
    )


@dp.callback_query(F.data.regexp(r"^archive:back:(1d|7d|30d|all)$"))
async def archive_back(callback: CallbackQuery):
    if callback.message is None:
        return
    await callback.answer()
    await callback.message.edit_text(
        "📊 История сигналов\nВыбери период:",
        reply_markup=stats_inline_kb(),
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


def _human_ago_ru(seconds: int) -> str:
    if seconds < 0:
        seconds = 0
    if seconds < 60:
        return f"{seconds} сек назад"
    minutes = seconds // 60
    if minutes < 60:
        return f"{minutes} мин назад"
    hours = minutes // 60
    return f"{hours} ч назад"


def _parse_extra_kv(extra: str) -> dict:
    """
    Превращает строку extra в словарь.
    Поддерживает:
      - "подписчиков: 1"
      - "key=value"
    Разделители могут быть пробелы и ';'
    """
    out = {}
    if not extra:
        return out

    parts = []
    for chunk in extra.replace(";", " ").split():
        if chunk.strip():
            parts.append(chunk.strip())

    # склеим "подписчиков:" + "1"
    i = 0
    while i < len(parts):
        token = parts[i]
        if token.endswith(":") and i + 1 < len(parts):
            key = token[:-1].strip().lower()
            out[key] = parts[i + 1].strip()
            i += 2
            continue

        if "=" in token:
            k, v = token.split("=", 1)
            out[k.strip().lower()] = v.strip()
        i += 1

    return out


def _format_fails_top(fails: dict, top_n: int = 5) -> str:
    if not fails:
        return ""
    ordered = sorted(fails.items(), key=lambda item: item[1], reverse=True)[:top_n]
    lines = ["Причины (топ):"]
    for reason, count in ordered:
        lines.append(f"• {reason} — {count}")
    return "\n".join(lines)


def _format_near_miss(near_miss: dict) -> str:
    if not near_miss:
        return ""
    lines = ["Near-miss:"]
    for reason, count in sorted(near_miss.items(), key=lambda item: item[1], reverse=True):
        lines.append(f"• {reason} — {count}")
    return "\n".join(lines)


def _build_rotation_order(symbols: list[str], *, shuffle: bool) -> list[str]:
    if not shuffle:
        return list(symbols)
    seed = datetime.utcnow().strftime("%Y-%m-%d")
    rng = random.Random(seed)
    ordered = list(symbols)
    rng.shuffle(ordered)
    return ordered


def _take_rotation_slice(
    symbols: list[str],
    rot_n: int,
    *,
    shuffle: bool,
    ttl_sec: int,
) -> tuple[list[str], int, int]:
    if rot_n <= 0 or not symbols:
        return [], 0, 0
    ordered = _build_rotation_order(symbols, shuffle=shuffle)
    cursor = kv_get_int("pumpdump_rotation_cursor", 0, ttl_sec=ttl_sec)
    if cursor < 0 or cursor >= len(ordered):
        cursor = 0
    count = min(rot_n, len(ordered))
    rotation_slice = [ordered[(cursor + i) % len(ordered)] for i in range(count)]
    next_cursor = (cursor + count) % len(ordered)
    kv_set_int("pumpdump_rotation_cursor", next_cursor)
    return rotation_slice, cursor, len(ordered)


def _format_market_hub_ru(now: float) -> str:
    # MARKET_HUB уже есть в проекте
    if MARKET_HUB.last_ok_at:
        ok_ago = int(now - MARKET_HUB.last_ok_at)
        last_tick = _human_ago_ru(ok_ago)
    else:
        last_tick = "нет данных"

    err = MARKET_HUB.last_error or "нет"
    symbols_count = len(getattr(MARKET_HUB, "_symbols", []) or [])
    return (
        "🔧 MarketHub (базовый модуль рынка)\n"
        "• Статус: работает\n"
        f"• Последний тик: {last_tick}\n"
        f"• Ошибки: {err}\n"
        f"• Активных пар в MarketHub (кеш свечей): {symbols_count}"
    )


def _format_db_status() -> str:
    path = get_db_path()
    if not os.path.exists(path):
        return f"🗄 База данных\n• Путь: {path}\n• Файл не найден"
    size_bytes = os.path.getsize(path)
    mtime = datetime.fromtimestamp(os.path.getmtime(path))
    return (
        "🗄 База данных\n"
        f"• Путь: {path}\n"
        f"• Размер: {size_bytes} байт\n"
        f"• Изменена: {mtime:%Y-%m-%d %H:%M:%S}"
    )


def _format_module_ru(key: str, st, now: float) -> str:
    # st — это ModuleStatus из health.py
    def _short_symbol(symbol: str) -> str:
        if symbol.endswith("USDT"):
            return symbol[:-4]
        return symbol

    def _format_samples(samples: list[tuple[str, float]]) -> str:
        formatted = []
        for symbol, score in samples:
            score_str = f"{score:.2f}".rstrip("0").rstrip(".")
            formatted.append(f"{_short_symbol(symbol)}({score_str})")
        return ", ".join(formatted)

    if st.last_tick:
        tick = _human_ago_ru(int(now - st.last_tick))
        status_line = "работает"
    else:
        tick = "ещё не запускался"
        status_line = "не запускался"

    ok_line = ""
    if st.last_ok:
        ok_line = f"• Последний успешный запрос: {_human_ago_ru(int(now - st.last_ok))}"

    extra = _parse_extra_kv(st.extra or "")

    # Общие поля
    lines = [
        f"{st.name}",
        f"• Статус: {status_line}",
        f"• Последний цикл: {tick}",
    ]
    if ok_line:
        lines.append(ok_line)

    if st.last_error:
        lines.append(f"• Ошибка: {st.last_error}")
    if st.last_warn:
        lines.append(f"• Предупреждение: {st.last_warn}")

    # Подписчики
    subs = extra.get("подписчиков")
    if subs is not None:
        lines.append("")
        lines.append("Пользователи")
        lines.append(f"• Подписчиков: {subs}")

    # Сканирование / прогресс (берём из st + extra)
    # AI-сигналы
    if key == "ai_signals":
        lines.append("")
        lines.append("Сканирование рынка")
        universe = extra.get("universe") or (
            str(st.total_symbols) if st.total_symbols else None
        )
        if universe:
            lines.append(f"• Монет в рынке: {universe}")
        chunk = extra.get("chunk")
        if chunk:
            lines.append(f"• Монет за цикл: {chunk}")
        exclude_btc = os.getenv("EXCLUDE_BTC_FROM_AI_UNIVERSE", "0").lower() in (
            "1",
            "true",
            "yes",
            "y",
        )
        lines.append(
            f"• BTC candidate scanning: {'disabled' if exclude_btc else 'enabled'}"
        )
        # cursor
        cur = extra.get("cursor") or (str(st.cursor) if st.cursor else None)
        if cur and universe:
            lines.append(f"• Текущая позиция: {cur} / {universe}")
        elif cur:
            lines.append(f"• Текущая позиция: {cur}")
        scan_symbol = st.current_symbol or "-"
        lines.append(f"• Current scan symbol: {scan_symbol}")
        use_btc_gate = bool(st.state.get("use_btc_gate", False))
        lines.append(f"• BTC gate: {'enabled' if use_btc_gate else 'disabled'}")
        btc_cache = get_cached_btc_context()
        btc_error = get_btc_context_last_error()
        if not use_btc_gate:
            btc_line = "disabled (use_btc_gate=false)"
        elif btc_cache is None:
            btc_line = f"error: {btc_error}" if btc_error else "pending"
        else:
            btc_ctx, age_sec, ttl_sec = btc_cache
            allow_longs = btc_ctx.get("allow_longs", False)
            allow_shorts = btc_ctx.get("allow_shorts", False)
            ctx_reason = btc_ctx.get("ctx_reason")
            last_cycle_ts = float(st.state.get("last_cycle_ts", 0.0))
            last_refresh_ts = get_btc_context_last_refresh_ts()
            label = "refreshed" if last_refresh_ts and last_refresh_ts >= last_cycle_ts else "cached"
            btc_line = (
                f"{label} age={age_sec}s ttl={ttl_sec}s "
                f"allow_longs={allow_longs} allow_shorts={allow_shorts}"
            )
            if ctx_reason:
                btc_line = f"{btc_line} reason={ctx_reason}"
        lines.append(f"• BTC context: {btc_line}")
        cyc = extra.get("cycle")
        if cyc:
            lines.append(f"• Время цикла: ~{cyc}")

        pre_score = (st.last_stats or {}).get("pre_score") if st.last_stats else None
        if pre_score:
            threshold = pre_score.get("threshold")
            threshold_str = (
                f"{threshold:.1f}" if isinstance(threshold, (int, float)) else "-"
            )
            checked = pre_score.get("checked", 0)
            passed = pre_score.get("passed", 0)
            failed = pre_score.get("failed", 0)
            pass_rate = pre_score.get("pass_rate")
            pass_rate_str = None
            if isinstance(pass_rate, (int, float)):
                pass_rate_str = f"{int(round(pass_rate * 100))}%"

            failed_samples = pre_score.get("failed_samples") or []
            passed_samples = pre_score.get("passed_samples") or []
            lines.append("")
            lines.append("Pre-score")
            lines.append(f"• threshold: {threshold_str}")
            summary = f"• checked: {checked} | passed: {passed} | failed: {failed}"
            if pass_rate_str:
                summary += f" | pass rate: {pass_rate_str}"
            lines.append(summary)
            if failed_samples:
                lines.append(f"• failed examples: {_format_samples(failed_samples)}")
            if passed_samples:
                lines.append(f"• passed examples: {_format_samples(passed_samples)}")

        if st.fails_top or st.near_miss or st.universe_debug:
            lines.append("")
            if st.fails_top:
                lines.append(st.fails_top)
            if st.near_miss:
                lines.append(st.near_miss)
            if st.universe_debug:
                lines.append(st.universe_debug)

        # запросы
        req = extra.get("req")
        kl = extra.get("klines")
        hits = extra.get("klines_hits")
        misses = extra.get("klines_misses")
        inflight = extra.get("klines_inflight")
        ticker_req = extra.get("ticker_req")
        deep_scans = extra.get("deep_scans")
        if req or kl or hits or misses or inflight or ticker_req or deep_scans:
            lines.append("")
            lines.append("Запросы к Binance")
            if req:
                lines.append(f"• Запросов сделано: {req}")
            if kl:
                lines.append(f"• Свечей получено: {kl}")
            if hits or misses:
                lines.append(f"• Кеш свечей: hit={hits or 0} miss={misses or 0}")
            if inflight:
                lines.append(f"• In-flight ожиданий свечей: {inflight}")
            if ticker_req:
                lines.append(f"• Ticker/24h запросов: {ticker_req}")
            if deep_scans:
                lines.append(f"• Deep-scan за цикл: {deep_scans}")

    # Pump/Dump
    if key == "pumpdump":
        lines.append("")
        lines.append("Поиск пампов / дампов")
        prog = extra.get("progress")
        checked = extra.get("checked")
        found = extra.get("found")
        sent = extra.get("sent")
        if prog:
            lines.append(f"• Прогресс: {prog}")
        if checked:
            lines.append(f"• Проверено: {checked}")
        if found is not None:
            lines.append(f"• Найдено сигналов: {found}")
        if sent is not None:
            lines.append(f"• Отправлено сигналов: {sent}")
        current = extra.get("current") or (st.current_symbol or None)
        if current:
            lines.append(f"• Текущая монета: {current}")
        cyc = extra.get("cycle")
        if cyc:
            lines.append(f"• Время цикла: ~{cyc}")
        rotation_flag = extra.get("rotation")
        rotation_n = extra.get("rotation_n")
        rotation_cursor = extra.get("rotation_cursor")
        rotation_slice = extra.get("rotation_slice")
        universe_size = extra.get("universe_size")
        rotation_added = extra.get("rotation_added")
        final_candidates = extra.get("final_candidates")
        scanned = extra.get("scanned")
        if rotation_flag is not None:
            cursor_line = f" cursor={rotation_cursor}" if rotation_cursor else ""
            n_line = f"{rotation_n}" if rotation_n is not None else "0"
            lines.append(f"• Rotation: {rotation_flag} (N={n_line}){cursor_line}")
        if rotation_slice is not None:
            lines.append(f"• Rotation last slice size: {rotation_slice}")
        if universe_size or rotation_added or final_candidates or scanned:
            lines.append(
                "• Universe size="
                f"{universe_size or 0} rotation_added={rotation_added or 0} "
                f"final_candidates={final_candidates or 0} scanned={scanned or 0}"
            )

        if st.fails_top or st.universe_debug:
            lines.append("")
            if st.fails_top:
                lines.append(st.fails_top)
            if st.universe_debug:
                lines.append(st.universe_debug)
        hits = extra.get("klines_hits")
        misses = extra.get("klines_misses")
        inflight = extra.get("klines_inflight")
        ticker_req = extra.get("ticker_req")
        req = extra.get("req")
        kl = extra.get("klines")
        if req or kl or hits or misses or inflight or ticker_req:
            lines.append("")
            lines.append("Запросы к Binance")
            if req:
                lines.append(f"• Запросов сделано: {req}")
            if kl:
                lines.append(f"• Свечей получено: {kl}")
            if hits or misses:
                lines.append(f"• Кеш свечей: hit={hits or 0} miss={misses or 0}")
            if inflight:
                lines.append(f"• In-flight ожиданий свечей: {inflight}")
            if ticker_req:
                lines.append(f"• Ticker/24h запросов: {ticker_req}")

    # Binance секция (общая)
    lines.append("")
    lines.append("Запросы к Binance")
    if st.binance_last_success_ts:
        lines.append(
            f"• Последний успешный ответ: "
            f"{_human_ago_ru(int(now - st.binance_last_success_ts))}"
        )
    else:
        lines.append("• Последний успешный ответ: нет данных")
    lines.append(f"• Таймауты подряд: {st.binance_consecutive_timeouts}")
    lines.append(f"• Текущий этап: {st.binance_current_stage or '—'}")

    # стабильность
    lines.append("")
    lines.append("Стабильность")
    lines.append(f"• Перезапусков сессии: {st.binance_session_restarts}")

    return "\n".join(lines)


@dp.message(Command("testadmin"))
async def test_admin(message: Message):
    if message.from_user is None or not is_admin(message.from_user.id):
        await message.answer("⛔ Нет доступа")
        return
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

    now = time.time()
    blocks = []
    blocks.append("🛠 Диагностика бота (админ)\n")
    use_btc_gate_raw = os.getenv("USE_BTC_GATE")
    use_btc_gate_value = "" if use_btc_gate_raw is None else use_btc_gate_raw
    blocks.append(f"BTC gate: {'enabled' if get_use_btc_gate() else 'disabled'}")
    blocks.append(f'USE_BTC_GATE raw: "{use_btc_gate_value}"')
    ai_module = MODULES.get("ai_signals")
    if ai_module and ai_module.last_error:
        blocks.append(f"AI errors: {ai_module.last_error}")
    blocks.append("")
    blocks.append(_format_db_status())
    blocks.append("")
    blocks.append(_format_market_hub_ru(now))
    blocks.append("")

    hidden = _hidden_status_modules()
    for key, st in MODULES.items():
        if key in hidden:
            continue
        if key not in ("ai_signals", "pumpdump"):
            continue
        blocks.append(_format_module_ru(key, st, now))
        blocks.append("\n" + ("—" * 22) + "\n")

    await message.answer("\n".join(blocks).strip())


@dp.message(F.text == "🛠 Диагностика (админ)")
async def test_admin_button(message: Message):
    if message.from_user is None or not is_admin(message.from_user.id):
        await message.answer("⛔ Нет доступа")
        return
    await test_admin(message)


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


def _human_ago(seconds: int) -> str:
    if seconds < 0:
        seconds = 0
    if seconds < 60:
        return f"{seconds} сек"
    minutes = seconds // 60
    if minutes < 60:
        return f"{minutes} мин"
    hours = minutes // 60
    return f"{hours} ч"


def _format_user_bot_status(chat_id: int) -> str:
    """Понятный для пользователя статус (без тех. мусора)."""
    now = time.time()

    ai_enabled = is_notify_enabled(chat_id, "ai_signals")
    pd_enabled = is_notify_enabled(chat_id, "pumpdump")

    ai = MODULES.get("ai_signals")
    pd = MODULES.get("pumpdump")

    def _module_line(title: str, st) -> str:
        if not st:
            return f"• {title}: нет данных"
        if st.last_tick == 0:
            return f"• {title}: ещё не запускался"
        tick_ago = _human_ago(int(now - st.last_tick))
        ok_part = ""
        if st.last_ok:
            ok_part = f", успешный запрос: {_human_ago(int(now - st.last_ok))} назад"
        return f"• {title}: активен ({tick_ago} назад{ok_part})"

    def _scan_hint(st) -> str:
        if not st:
            return ""
        parts = []
        if getattr(st, "total_symbols", 0):
            parts.append(f"рынок: {st.total_symbols} монет")
        if getattr(st, "checked_last_cycle", 0):
            parts.append(f"проверено за цикл: {st.checked_last_cycle}")
        if getattr(st, "current_symbol", ""):
            parts.append(f"сейчас: {st.current_symbol}")
        return " | ".join(parts)

    def _binance_hint(st) -> str:
        ts = getattr(st, "binance_last_success_ts", 0)
        if not ts:
            return "Binance: нет свежих данных"
        return f"Binance: обновление {_human_ago(int(now - ts))} назад"

    lines = [
        "📡 Статус системы",
        "",
        f"🔔 AI-сигналы: {'✅ включены' if ai_enabled else '⛔ выключены'}",
        f"🔔 Pump/Dump: {'✅ включены' if pd_enabled else '⛔ выключены'}",
        "",
        "Что сейчас делает бот:",
        _module_line("AI-сигналы", ai),
        (f"  ↳ {_scan_hint(ai)}" if _scan_hint(ai) else ""),
        _module_line("Pump/Dump", pd),
        "",
        _binance_hint(ai or pd),
        "",
        "Если хочешь получать сигналы автоматически — включи уведомления в нужном разделе.",
    ]
    compact = [x for x in lines if x != ""]
    return "\n".join(compact)


@dp.message(Command("status"))
async def status_cmd(message: Message):
    await message.answer(_format_user_bot_status(message.chat.id))


@dp.message(F.text == "ℹ️ О системе")
async def system_menu(message: Message):
    await message.answer(
        "ℹ️ Раздел системы. Здесь статус работы и сервисные функции.",
        reply_markup=build_system_menu_kb(
            is_admin=is_admin(message.from_user.id) if message.from_user else False
        ),
    )


@dp.message(F.text == "📡 Статус системы")
async def status_button(message: Message):
    await message.answer(
        _format_user_bot_status(message.chat.id),
        reply_markup=build_system_menu_kb(
            is_admin=is_admin(message.from_user.id) if message.from_user else False
        ),
    )


@dp.message(F.text == "⬅️ Назад")
async def back_to_main(message: Message):
    await message.answer(
        "Возвращаемся в главное меню.",
        reply_markup=build_main_menu_kb(
            is_admin=is_admin(message.from_user.id) if message.from_user else False
        ),
    )


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
    await message.answer(
        _format_stats_message(stats),
        reply_markup=build_main_menu_kb(is_admin=True),
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
    breakdown = signal.get("score_breakdown") or signal.get("breakdown") or []

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
        score_breakdown=breakdown,
        market_mode=reason.get("market_mode"),
        market_bias=reason.get("market_bias"),
        btc_change_6h_pct=float(reason.get("btc_change_6h_pct", 0.0)),
        btc_atr_1h_pct=float(reason.get("btc_atr_1h_pct", 0.0)),
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

    refresh_on_send = _env_bool("BTC_REFRESH_ON_SEND", "0")
    module_state = MODULES.get("ai_signals")
    use_btc_gate = bool(module_state and module_state.state.get("use_btc_gate", False))
    if refresh_on_send and use_btc_gate:
        btc_ctx = None
        cached = get_cached_btc_context()
        age_sec = None
        ttl_sec = None
        if cached:
            btc_ctx, age_sec, ttl_sec = cached
        needs_refresh = age_sec is None or ttl_sec is None or age_sec >= ttl_sec
        if needs_refresh:
            try:
                btc_ctx = await get_btc_context(force_refresh=True)
                age_sec = 0
            except Exception as exc:
                print(f"[ai_signals] BTC refresh on send failed: {exc}")
        if btc_ctx:
            side = "LONG" if signal_dict.get("direction") == "long" else "SHORT"
            if side == "LONG" and not btc_ctx.get("allow_longs", False):
                print("[ai_signals] BTC gate blocked LONG signal on refresh.")
                return
            if side == "SHORT" and not btc_ctx.get("allow_shorts", False):
                print("[ai_signals] BTC gate blocked SHORT signal on refresh.")
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
    sent_at = int(time.time())
    insert_signal_audit(signal_dict, tier="free", module="ai_signals", sent_at=sent_at)
    reason = signal_dict.get("reason")
    breakdown = (
        signal_dict.get("score_breakdown")
        or signal_dict.get("breakdown")
        or []
    )
    reason_json = json.dumps(reason, ensure_ascii=False) if reason is not None else None
    breakdown_json = json.dumps(breakdown, ensure_ascii=False) if breakdown is not None else None

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
            continue
        try:
            insert_signal_event(
                ts=sent_at,
                user_id=chat_id,
                module="ai_signals",
                symbol=symbol,
                side="LONG" if direction == "long" else "SHORT",
                timeframe="1H",
                score=float(signal_dict.get("score", 0.0)),
                poi_low=float(entry_low),
                poi_high=float(entry_high),
                sl=float(signal_dict.get("sl", 0.0)),
                tp1=float(signal_dict.get("tp1", 0.0)),
                tp2=float(signal_dict.get("tp2", 0.0)),
                status="OPEN",
                tg_message_id=int(res.message_id),
                reason_json=reason_json,
                breakdown_json=breakdown_json,
            )
        except Exception as exc:
            print(f"[ai_signals] Failed to log signal event for {chat_id}: {exc}")


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
    filtered, removed = filter_tradeable_symbols(symbols)
    exclude_btc = os.getenv("EXCLUDE_BTC_FROM_AI_UNIVERSE", "0").lower() in (
        "1",
        "true",
        "yes",
        "y",
    )
    if exclude_btc:
        filtered = [symbol for symbol in filtered if symbol != "BTCUSDT"]
    if removed:
        print(
            f"[ai_signals] universe filtered: total={len(symbols)} removed={removed} final={len(filtered)}"
        )
    return filtered


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

        reset_request_count("pumpdump")
        reset_klines_request_count("pumpdump")
        reset_klines_cache_stats("pumpdump")
        reset_ticker_request_count("pumpdump")

        session = await get_shared_session()
        universe_limit = int(os.getenv("PUMPDUMP_UNIVERSE_LIMIT", "120"))
        rotation_n = int(os.getenv("PUMPDUMP_ROTATION_N", "0"))
        rotation_ttl_sec = int(os.getenv("PUMPDUMP_ROTATION_STATE_TTL_SEC", "86400"))
        rotation_shuffle = os.getenv("PUMPDUMP_ROTATION_SHUFFLE", "1") != "0"
        with binance_request_context("pumpdump"):
            symbols, symbol_stats, all_symbols = await get_candidate_symbols(
                session,
                limit=universe_limit,
                return_stats=True,
            )
        if not symbols:
            mark_error("pumpdump", "no symbols to scan")
            return
        debug_symbol = os.getenv("DEBUG_SYMBOL", "USUALUSDT").upper()
        in_universe = debug_symbol in symbols
        if in_universe:
            index = symbols.index(debug_symbol)
            universe_debug = (
                f"Universe check: {debug_symbol} in universe = yes "
                f"(index={index} / total={len(symbols)})"
            )
        else:
            universe_debug = (
                f"Universe check: {debug_symbol} in universe = no "
                f"(total={len(symbols)})"
            )
        module_state = MODULES.get("pumpdump")
        if module_state:
            module_state.universe_debug = universe_debug
        if log_level >= 1 and symbol_stats.get("removed"):
            print(
                "[pumpdump] universe filtered "
                f"total={symbol_stats.get('total')} "
                f"removed={symbol_stats.get('removed')} "
                f"final={symbol_stats.get('final')}"
            )
        rotation_enabled = rotation_n > 0
        rotation_slice: list[str] = []
        rotation_cursor = 0
        rotation_total = 0
        if rotation_enabled:
            rotation_slice, rotation_cursor, rotation_total = _take_rotation_slice(
                all_symbols,
                rotation_n,
                shuffle=rotation_shuffle,
                ttl_sec=rotation_ttl_sec,
            )
        seen: set[str] = set()
        candidates: list[str] = []
        for sym in symbols + rotation_slice:
            if sym not in seen:
                seen.add(sym)
                candidates.append(sym)
        rotation_added = max(0, len(candidates) - len(symbols))
        total = len(candidates)
        try:
            cursor = int(get_state("pumpdump_cursor", "0") or "0")
        except Exception:
            cursor = 0
        if cursor >= len(candidates):
            cursor = 0

        update_current_symbol("pumpdump", candidates[cursor] if candidates else "")

        cycle_start = time.time()
        try:
            signals, stats, next_cursor = await asyncio.wait_for(
                scan_pumps_chunk(
                    candidates,
                    start_idx=cursor,
                    time_budget_sec=BUDGET,
                    return_stats=True,
                    progress_cb=lambda sym: update_current_symbol("pumpdump", sym),
                ),
                timeout=BUDGET,
            )
        except asyncio.TimeoutError:
            return
        set_state("pumpdump_cursor", str(next_cursor))
        found = stats.get("found", len(signals) if isinstance(signals, list) else 0)

        if log_level >= 1:
            print(
                f"[pumpdump] chunk: total={len(candidates)} "
                f"checked={stats.get('checked',0)} found={found}"
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
        checked = stats.get("checked", 0)
        chunk_len = min(PUMP_CHUNK_SIZE, total) if total else 0
        update_module_progress(
            "pumpdump",
            total_symbols=total,
            cursor=next_cursor,
            checked_last_cycle=checked,
        )

        now_min = int(time.time() // 60)
        sent_count = 0
        last_sent: dict[str, int] = state["last_sent"]
        date_key = _get_pumpdump_date_key()

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
                    if get_pumpdump_daily_count(chat_id, date_key) >= PUMP_DAILY_LIMIT:
                        continue
                    time_bucket = int(time.time() // PUMP_COOLDOWN_GLOBAL_SEC)
                    dedup_global = f"pumpdump:global:{time_bucket}"
                    if not can_send(
                        chat_id,
                        "pumpdump",
                        dedup_global,
                        PUMP_COOLDOWN_GLOBAL_SEC,
                    ):
                        continue
                    dedup_symbol = f"pumpdump:{symbol}"
                    if not can_send(
                        chat_id,
                        "pumpdump",
                        dedup_symbol,
                        PUMP_COOLDOWN_SYMBOL_SEC,
                    ):
                        continue
                    await bot.send_message(chat_id, text, parse_mode="Markdown")
                    increment_pumpdump_daily_count(chat_id, date_key)
                    sent_count += 1
                except Exception as e:
                    print(f"[pumpdump] send failed chat_id={chat_id} symbol={symbol}: {e}")
                    continue

        cycle_sec = time.time() - cycle_start
        current_symbol = MODULES.get("pumpdump").current_symbol if "pumpdump" in MODULES else None
        req_count = get_request_count("pumpdump")
        klines_count = get_klines_request_count("pumpdump")
        cache_stats = get_klines_cache_stats("pumpdump")
        ticker_count = get_ticker_request_count("pumpdump")
        fails = stats.get("fails", {}) if isinstance(stats, dict) else {}
        fails_top_str = _format_fails_top(fails)
        fails_top = sorted(fails.items(), key=lambda x: x[1], reverse=True)[:3]
        fails_str = ",".join([f"{k}={v}" for k, v in fails_top]) if fails_top else "-"
        if module_state:
            module_state.last_stats = stats
            module_state.fails_top = fails_top_str
        if log_level >= 1:
            print(f"[pumpdump] cycle done: found={found} sent={sent_count}")
        mark_ok(
            "pumpdump",
            extra=(
                f"progress={checked}/{total} "
                f"checked={checked}/{chunk_len} found={found} sent={sent_count} "
                f"current={current_symbol or '-'} cycle={int(cycle_sec)}s "
                f"req={req_count} klines={klines_count} "
                f"klines_hits={cache_stats.get('hits')} klines_misses={cache_stats.get('misses')} "
                f"klines_inflight={cache_stats.get('inflight_awaits')} "
                f"ticker_req={ticker_count} fails={fails_str} "
                f"rotation={'on' if rotation_enabled else 'off'} "
                f"rotation_n={rotation_n} rotation_cursor={rotation_cursor}/{rotation_total} "
                f"rotation_slice={len(rotation_slice)} universe_size={len(symbols)} "
                f"rotation_added={rotation_added} final_candidates={total} scanned={checked}"
            ),
        )
    finally:
        print("[PUMP] scan_once end")


async def ai_scan_once() -> None:
    start = time.time()
    BUDGET = 35
    print("[AI] scan_once start")
    try:
        use_btc_gate = get_use_btc_gate()
        module_state = MODULES.get("ai_signals")
        if module_state:
            module_state.state["use_btc_gate"] = use_btc_gate
            module_state.state["last_cycle_ts"] = time.time()
        reset_request_count("ai_signals")
        reset_klines_request_count("ai_signals")
        reset_klines_cache_stats("ai_signals")
        reset_ticker_request_count("ai_signals")
        mark_tick("ai_signals", extra="сканирую рынок...")

        with binance_request_context("ai_signals"):
            symbols = await _get_ai_universe()
        if not symbols:
            mark_error("ai_signals", "no symbols to scan")
            return
        total = len(symbols)
        debug_symbol = os.getenv("DEBUG_SYMBOL", "USUALUSDT").upper()
        in_universe = debug_symbol in symbols
        if in_universe:
            index = symbols.index(debug_symbol)
            universe_debug = (
                f"Universe check: {debug_symbol} in universe = yes "
                f"(index={index} / total={len(symbols)})"
            )
        else:
            universe_debug = (
                f"Universe check: {debug_symbol} in universe = no "
                f"(total={len(symbols)})"
            )
        module_state = MODULES.get("ai_signals")
        if module_state:
            module_state.universe_debug = universe_debug

        if not hasattr(ai_scan_once, "cursor"):
            stored_cursor = get_state("ai_cursor", "0")
            try:
                ai_scan_once.cursor = int(stored_cursor) if stored_cursor is not None else 0
            except (TypeError, ValueError):
                ai_scan_once.cursor = 0
        cursor = ai_scan_once.cursor
        # Ensure symbols are sorted by volume desc (24h quoteVolume) BEFORE this block.
        # If MarketHub already provides sorted list -> OK; else sort here.
        priority = symbols[:max(0, min(AI_PRIORITY_N, len(symbols)))]
        priority_set = set(priority)
        pool = [s for s in symbols if s not in priority_set]

        if cursor >= len(pool):
            cursor = 0

        rotating_size = max(0, AI_CHUNK_SIZE - len(priority))
        rotating = pool[cursor : cursor + rotating_size]

        chunk = priority + rotating

        new_cursor = cursor + len(rotating)
        if new_cursor >= len(pool):
            new_cursor = 0

        ai_scan_once.cursor = new_cursor
        set_state("ai_cursor", str(new_cursor))

        update_module_progress(
            "ai_signals",
            total_symbols=len(symbols),
            cursor=new_cursor,
            checked_last_cycle=len(chunk),
        )
        if chunk:
            update_current_symbol("ai_signals", chunk[0])

        now = int(time.time())
        added = 0
        with binance_request_context("ai_signals"):
            for symbol in chunk:
                if time.time() - start > BUDGET:
                    print("[AI] budget exceeded, stopping early")
                    break
                update_current_symbol("ai_signals", symbol)
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
        cache_stats = get_klines_cache_stats("ai_signals")
        ticker_count = get_ticker_request_count("ai_signals")
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
                f"req={req_count} klines={klines_count} "
                f"klines_hits={cache_stats.get('hits')} klines_misses={cache_stats.get('misses')} "
                f"klines_inflight={cache_stats.get('inflight_awaits')} "
                f"ticker_req={ticker_count}"
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
    module_state = MODULES.get("ai_signals")
    use_btc_gate = get_use_btc_gate()
    if module_state:
        module_state.state["use_btc_gate"] = use_btc_gate
        module_state.state["last_cycle_ts"] = time.time()
    now = int(time.time())
    rows = list_watchlist_for_scan(now, WATCHLIST_MAX)
    symbols = [row["symbol"] for row in rows]
    priority_scores = {row["symbol"]: float(row["score"]) for row in rows}
    exclude_btc = os.getenv("EXCLUDE_BTC_FROM_AI_UNIVERSE", "0").lower() in (
        "1",
        "true",
        "yes",
        "y",
    )
    if exclude_btc:
        symbols = [symbol for symbol in symbols if symbol != "BTCUSDT"]
        priority_scores = {
            symbol: score for symbol, score in priority_scores.items() if symbol != "BTCUSDT"
        }
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
            use_btc_gate=use_btc_gate,
            free_mode=True,
            min_score=FREE_MIN_SCORE,
            return_stats=True,
            time_budget=BUDGET,
            deep_scan_limit=AI_DEEP_TOP_K,
            priority_scores=priority_scores,
        )
    print("[ai_signals] watchlist stats:", stats)
    module_state = MODULES.get("ai_signals")
    if module_state and isinstance(stats, dict):
        module_state.last_stats = stats
        module_state.fails_top = _format_fails_top(stats.get("fails", {}))
        module_state.near_miss = _format_near_miss(stats.get("near_miss", {}))
    deep_scans_done = stats.get("deep_scans_done", 0) if isinstance(stats, dict) else 0
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
            f"deep_scans={deep_scans_done} "
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
