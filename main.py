import asyncio
import json
import logging
import os
import sqlite3
import time
import random
import re
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
    close_shared_session,
    get_shared_session,
    get_binance_metrics_snapshot,
    reset_binance_metrics,
    fetch_klines,
)
from pump_detector import (
    MIN_VOLUME_5M_USDT,
    PUMP_CHUNK_SIZE,
    PUMP_MAX_SYMBOLS_PER_SEC,
    PUMP_RATE_LIMIT_ENABLED,
    PUMP_VOLUME_MUL,
    PUMPDUMP_1M_INTERVAL,
    PUMPDUMP_1M_LIMIT,
    PUMPDUMP_5M_INTERVAL,
    PUMPDUMP_5M_LIMIT,
    format_pump_message,
    get_candidate_symbols,
    scan_pumps_chunk,
)
from signals import (
    scan_market,
    AI_MAX_DEEP_PER_CYCLE,
    AI_STAGE_A_TOP_K,
    PRE_SCORE_THRESHOLD,
    MIN_PRE_SCORE,
    FINAL_SCORE_THRESHOLD,
)
from symbol_cache import (
    filter_tradeable_symbols,
    get_all_usdt_symbols,
    get_blocked_symbols,
    get_top_usdt_symbols_by_volume,
)
from health import (
    MODULES,
    mark_tick,
    mark_ok,
    mark_warn,
    mark_error,
    safe_worker_loop,
    watchdog,
    update_module_progress,
    update_current_symbol,
    PUMP_CYCLE_SLEEP_SEC,
)

from db import (
    init_db as init_storage_db,
    get_user_pref,
    set_user_pref,
    list_user_ids_with_pref,
    is_user_locked,
    is_sub_active,
    ensure_trial_defaults,
    try_consume_trial,
    delete_user,
    TRIAL_AI_LIMIT,
    TRIAL_PUMP_LIMIT,
    get_state,
    set_state,
    kv_get_int,
    kv_set_int,
    purge_symbol,
    insert_signal_event,
    list_signal_events,
    list_open_signal_events,
    count_signal_events,
    get_signal_outcome_counts,
    get_signal_score_bucket_counts,
    get_signal_event,
    get_signal_by_id,
    update_signal_event_refresh,
    update_signal_event_status_by_id,
    mark_signal_result_notified,
    get_last_pumpdump_signal,
    set_last_pumpdump_signal,
    purge_test_signals,
)
from db_path import ensure_db_writable, get_db_path
from market_cache import get_ticker_request_count, reset_ticker_request_count
from alert_dedup_db import init_alert_dedup, can_send
from status_utils import is_notify_enabled
from message_templates import format_scenario_message
from signal_audit_db import (
    get_public_stats,
    get_last_signal_audit,
    init_signal_audit_tables,
    insert_signal_audit,
)
from signal_audit_worker import signal_audit_worker_loop
import i18n
from keyboards import (
    ai_signals_inline_kb,
    build_admin_diagnostics_kb,
    build_lang_select_kb,
    build_main_menu_kb,
    build_offer_inline_kb,
    build_payment_inline_kb,
    build_system_menu_kb,
    pumpdump_inline_kb,
    stats_inline_kb,
)
from settings import SIGNAL_TTL_SECONDS

logger = logging.getLogger(__name__)
DEFAULT_LANG = "ru"
_LOG_THROTTLE_SEC = 30.0
_LAST_LOG_TS: Dict[str, float] = {}


def _log_throttled(key: str, message: str, *args: Any, exc: Exception | None = None) -> None:
    now = time.monotonic()
    last = _LAST_LOG_TS.get(key, 0.0)
    if now - last < _LOG_THROTTLE_SEC:
        return
    _LAST_LOG_TS[key] = now
    if exc is not None:
        logger.exception(message, *args)
    else:
        logger.warning(message, *args)


# ===== ЗАГРУЖАЕМ НАСТРОЙКИ =====

def load_settings() -> str:
    load_dotenv()
    bot_token = os.getenv("BOT_TOKEN")

    if not bot_token:
        raise ValueError("Нет BOT_TOKEN в .env файле")

    return bot_token


ADMIN_USER_ID = int(os.getenv("ADMIN_USER_ID", "0"))
PUMP_COOLDOWN_SYMBOL_SEC = int(os.getenv("PUMP_COOLDOWN_SYMBOL_SEC", "86400"))  # 24h
PUMP_COOLDOWN_GLOBAL_SEC = int(os.getenv("PUMP_COOLDOWN_GLOBAL_SEC", "3600"))  # 1h
PUMP_DAILY_LIMIT = int(os.getenv("PUMP_DAILY_LIMIT", "6"))
SUB_DAYS = 30
SUB_PRICE_USD = 39
PAY_WALLET_TRX = "TGnSveNVrBHytZyA5AfqAj3hDK3FbFCtBY"
ADMIN_CONTACT = "@loomany"
PREF_AWAITING_RECEIPT = "awaiting_receipt"
PAYWALL_COOLDOWN_SEC = int(os.getenv("PAYWALL_COOLDOWN_SEC", "60"))
SUB_SOURCE_SYSTEM = 1
SUB_SOURCE_AI = 2
SUB_SOURCE_PUMP = 3


def _env_bool(name: str, default: str = "0") -> bool:
    return os.getenv(name, default).strip().lower() in ("1", "true", "yes", "on")


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
    init_signal_audit_tables()
    blocked_symbols = sorted(get_blocked_symbols())
    if blocked_symbols:
        totals = {"events_deleted": 0, "signal_audit_deleted": 0}
        for symbol in blocked_symbols:
            stats = purge_symbol(symbol)
            print(f"[purge] {symbol} deleted from stats: {stats}")
            totals["events_deleted"] += stats["events_deleted"]
            totals["signal_audit_deleted"] += stats["signal_audit_deleted"]
        print(
            "[blocklist] purge startup: "
            f"symbols={len(blocked_symbols)} "
            f"events={totals['events_deleted']} "
            f"audit={totals['signal_audit_deleted']}"
        )
    init_alert_dedup()
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
        except Exception as exc:
            _log_throttled(
                "db_migrate_pref",
                "[db_error] op=set_user_pref chat_id=%s key=%s err=%s",
                chat_id,
                key,
                exc,
                exc=exc,
            )
            continue

    for (chat_id,) in legacy_rows:
        try:
            set_user_pref(int(chat_id), "ai_signals_enabled", 1)
        except Exception as exc:
            _log_throttled(
                "db_migrate_legacy_pref",
                "[db_error] op=set_user_pref chat_id=%s key=ai_signals_enabled err=%s",
                chat_id,
                exc,
                exc=exc,
            )
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


def _clean_lang(lang: str | None) -> str | None:
    if not lang:
        return None
    raw = lang.strip().lower()
    if raw.startswith("ru"):
        return "ru"
    if raw.startswith("en"):
        return "en"
    return None


def get_user_lang(chat_id: int) -> str | None:
    conn = sqlite3.connect(get_db_path())
    try:
        cur = conn.execute("SELECT language FROM users WHERE chat_id = ?", (chat_id,))
        row = cur.fetchone()
    finally:
        conn.close()
    if not row:
        return None
    return _clean_lang(row[0])


def set_user_lang(chat_id: int, lang: str) -> None:
    normalized = i18n.normalize_lang(lang)
    conn = sqlite3.connect(get_db_path())
    try:
        conn.execute(
            "UPDATE users SET language = ?, last_seen = ? WHERE chat_id = ?",
            (normalized, int(time.time()), chat_id),
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
        cur.execute("SELECT language FROM users WHERE chat_id = ?", (chat_id,))
        row = cur.fetchone()
        exists = row is not None
        existing_lang = _clean_lang(row[0]) if row else None
        incoming_lang = _clean_lang(language)
        if exists:
            resolved_lang = existing_lang or incoming_lang
        else:
            resolved_lang = None
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
                    resolved_lang,
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
                resolved_lang,
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


def get_pumpdump_subscribers() -> list[int]:
    return list(list_user_ids_with_pref("pumpdump_enabled", 1))


# ===== СОЗДАЁМ БОТА =====

BOT_TOKEN = load_settings()
ADMIN_CHAT_ID = int(os.getenv("ADMIN_CHAT_ID", "0"))
bot: Bot | None = None
dp = Dispatcher()
FREE_MIN_SCORE = 70
COOLDOWN_FREE_SEC = int(os.getenv("AI_SIGNALS_COOLDOWN_SEC", "86400"))
MAX_SIGNALS_PER_CYCLE = 3
MAX_BTC_PER_CYCLE = 1
AI_CHUNK_SIZE = int(os.getenv("AI_CHUNK_SIZE", "30"))
AI_CHUNK_MIN = int(os.getenv("AI_CHUNK_MIN", "10"))
AI_CHUNK_MAX = int(os.getenv("AI_CHUNK_MAX", "50"))
AI_SAFE_MODE = os.getenv("AI_SAFE_MODE", "1").lower() in ("1", "true", "yes", "y")
AI_SAFE_CHUNK_MAX = int(os.getenv("AI_SAFE_CHUNK_MAX", "35"))
AI_CHUNK_MAX_EFFECTIVE = (
    min(AI_CHUNK_MAX, AI_SAFE_CHUNK_MAX) if AI_SAFE_MODE else AI_CHUNK_MAX
)
AI_CYCLE_SLEEP_SEC = float(os.getenv("AI_CYCLE_SLEEP_SEC", "2"))
AI_ADAPT_ENABLED = os.getenv("AI_ADAPT_ENABLED", "1").lower() in ("1", "true", "yes", "y")
AI_ADAPT_FAIL_NO_KLINES_HIGH = float(os.getenv("AI_ADAPT_FAIL_NO_KLINES_HIGH", "0.40"))
AI_ADAPT_FAIL_TIMEOUT_HIGH = float(os.getenv("AI_ADAPT_FAIL_TIMEOUT_HIGH", "0.30"))
AI_ADAPT_STEP_DOWN = int(os.getenv("AI_ADAPT_STEP_DOWN", "10"))
AI_ADAPT_STEP_UP = int(os.getenv("AI_ADAPT_STEP_UP", "5"))
AI_ADAPT_STABLE_CYCLES_FOR_UP = int(os.getenv("AI_ADAPT_STABLE_CYCLES_FOR_UP", "3"))
_AI_CHUNK_SIZE_CURRENT = max(
    AI_CHUNK_MIN, min(AI_CHUNK_MAX_EFFECTIVE, AI_CHUNK_SIZE)
)
_AI_STABLE_CYCLES = 0
AI_PRIORITY_N = int(os.getenv("AI_PRIORITY_N", "15"))
AI_UNIVERSE_TOP_N = int(os.getenv("AI_UNIVERSE_TOP_N", "250"))
AI_DEEP_TOP_K = int(os.getenv("AI_DEEP_TOP_K", os.getenv("AI_MAX_DEEP_PER_CYCLE", "3")))
AI_EXCLUDE_SYMBOLS_DEFAULT = "BTCUSDT"


def _get_ai_excluded_symbols() -> set[str]:
    raw = os.getenv("AI_EXCLUDE_SYMBOLS")
    if raw is None:
        raw = AI_EXCLUDE_SYMBOLS_DEFAULT
    symbols = {item.strip().upper() for item in raw.split(",") if item.strip()}
    exclude_btc = os.getenv("EXCLUDE_BTC_FROM_AI_UNIVERSE", "0").lower() in (
        "1",
        "true",
        "yes",
        "y",
    )
    if exclude_btc:
        symbols.add("BTCUSDT")
    return symbols


def _format_symbol_list(symbols: set[str]) -> str:
    return ", ".join(sorted(symbols)) if symbols else "-"


def _get_pump_excluded_symbols() -> set[str]:
    raw = os.getenv("PUMP_EXCLUDE_SYMBOLS", "")
    return {item.strip().upper() for item in raw.split(",") if item.strip()}


def _get_ai_chunk_size() -> int:
    return _AI_CHUNK_SIZE_CURRENT


# ===== ХЭНДЛЕРЫ =====

@dp.message(CommandStart())
async def cmd_start(message: Message):
    user = message.from_user
    lang: str | None = None
    if user is not None:
        is_new = upsert_user(
            chat_id=message.chat.id,
            username=user.username,
            first_name=user.first_name,
            last_name=user.last_name,
            full_name=user.full_name,
            language=user.language_code,
        )
        lang = get_user_lang(message.chat.id)
        if is_new and ADMIN_CHAT_ID != 0:
            username = f"@{user.username}" if user.username else "-"
            full_name = user.full_name or "-"
            language = user.language_code or "-"
            admin_lang = get_user_lang(ADMIN_CHAT_ID) or "ru"
            admin_text = i18n.t(
                admin_lang,
                "ADMIN_NEW_USER",
                user_id=message.chat.id,
                username=username,
                full_name=full_name,
                language=language,
            )
            await message.bot.send_message(ADMIN_CHAT_ID, admin_text)
        if is_new and not is_admin(message.chat.id):
            set_user_pref(message.chat.id, "trial_ai_left", TRIAL_AI_LIMIT)
            set_user_pref(message.chat.id, "trial_pump_left", TRIAL_PUMP_LIMIT)
            set_user_pref(message.chat.id, "user_locked", 0)
    if not lang:
        await message.answer(
            i18n.t("ru", "LANG_PICK_TEXT"),
            reply_markup=build_lang_select_kb(),
        )
        return
    await message.answer(
        i18n.t(lang, "START_TEXT"),
        reply_markup=build_main_menu_kb(
            lang,
            is_admin=is_admin(message.from_user.id) if message.from_user else False,
        ),
    )


@dp.callback_query(F.data.in_({"lang:ru", "lang:en"}))
async def lang_select_callback(callback: CallbackQuery):
    if callback.from_user is None:
        return
    lang = i18n.normalize_lang(callback.data.split(":", 1)[1])
    set_user_lang(callback.from_user.id, lang)
    await callback.answer()
    if callback.message:
        with suppress(Exception):
            await callback.message.delete()
        await callback.message.answer(
            i18n.t(lang, "START_TEXT"),
            reply_markup=build_main_menu_kb(
                lang,
                is_admin=is_admin(callback.from_user.id),
            ),
        )


@dp.message(F.text.in_(i18n.all_labels("MENU_AI")))
async def ai_signals_menu(message: Message):
    lang = get_user_lang(message.chat.id) or "ru"
    status = (
        i18n.t(lang, "STATUS_ON")
        if get_user_pref(message.chat.id, "ai_signals_enabled", 0)
        else i18n.t(lang, "STATUS_OFF")
    )
    await message.answer(
        f"{i18n.t(lang, 'AI_SIGNALS_TEXT')}\n\n{i18n.t(lang, 'STATUS_LABEL')}: {status}",
        reply_markup=ai_signals_inline_kb(lang),
    )


@dp.message(F.text.in_(i18n.all_labels("MENU_PD")))
async def pumpdump_menu(message: Message):
    lang = get_user_lang(message.chat.id) or "ru"
    status = (
        i18n.t(lang, "STATUS_ON")
        if get_user_pref(message.chat.id, "pumpdump_enabled", 0)
        else i18n.t(lang, "STATUS_OFF")
    )
    await message.answer(
        f"{i18n.t(lang, 'PUMPDUMP_TEXT')}\n\n{i18n.t(lang, 'STATUS_LABEL')}: {status}",
        reply_markup=pumpdump_inline_kb(lang),
    )


def _period_label(period_key: str, lang: str) -> str:
    mapping = {
        "1d": i18n.t(lang, "PERIOD_1D"),
        "7d": i18n.t(lang, "PERIOD_7D"),
        "30d": i18n.t(lang, "PERIOD_30D"),
        "all": i18n.t(lang, "PERIOD_ALL"),
    }
    return mapping.get(period_key, i18n.t(lang, "PERIOD_ALL"))


def _format_ai_stats_message(stats: Dict[str, Any], period_key: str, lang: str) -> str:
    title = i18n.t(lang, "AI_STATS_TITLE", period=_period_label(period_key, lang))
    total = stats.get("total", 0)
    disclaimer = i18n.t(lang, "AI_STATS_DISCLAIMER")

    if total == 0:
        return f"{title}\n{i18n.t(lang, 'AI_STATS_NO_COMPLETED')}\n\n{disclaimer}"

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
        return i18n.t(
            lang,
            "AI_STATS_BUCKET_LINE",
            label=label,
            total=total_bucket,
            winrate=win_bucket,
        )

    lines = [
        title,
        "",
        i18n.t(lang, "AI_STATS_SIGNALS_COUNT", total=total),
        i18n.t(lang, "AI_STATS_SUMMARY", tp1=tp1, tp2=tp2, sl=sl, exp=exp),
        i18n.t(lang, "AI_STATS_WINRATE", winrate=winrate),
        "",
        i18n.t(lang, "AI_STATS_SCORE_LABEL"),
        _bucket_line("0–69", "0-69"),
        _bucket_line("70–79", "70-79"),
        _bucket_line("80+", "80-100"),
        "",
        disclaimer,
    ]
    return "\n".join(lines)


def window_since(window: str, now_ts: int) -> int | None:
    day_seconds = 24 * 60 * 60
    if window == "1d":
        return now_ts - day_seconds
    if window == "7d":
        return now_ts - (7 * day_seconds)
    if window == "30d":
        return now_ts - (30 * day_seconds)
    return None


def _status_icon(status: str | None) -> str:
    passed = {"TP1", "TP2", "BE"}
    failed = {"SL"}
    neutral = {"NF", "NO_FILL", "EXP", "EXPIRED"}
    normalized = (status or "").upper().strip()
    if normalized in passed:
        return "✅"
    if normalized in failed:
        return "❌"
    if normalized in neutral:
        return "⏳"
    return "⏰"


def _format_signal_event_status(raw_status: str, lang: str) -> str:
    status_map = {
        "OPEN": i18n.t(lang, "STATUS_OPEN"),
        "TP1": "TP1",
        "TP2": "TP2",
        "SL": "SL",
        "EXP": "EXP",
        "EXPIRED": "EXP",
        "BE": "BE",
        "NO_FILL": i18n.t(lang, "STATUS_NO_FILL"),
        "AMBIGUOUS": i18n.t(lang, "STATUS_AMBIGUOUS"),
    }
    return status_map.get(raw_status, raw_status)


def _format_event_time(ts: int) -> str:
    dt = datetime.fromtimestamp(ts, tz=ALMATY_TZ)
    return dt.strftime("%d.%m %H:%M")


def _format_archive_list(
    lang: str,
    time_window: str,
    events: list[dict],
    page: int,
    pages: int,
    outcome_counts: dict,
    score_bucket_counts: dict[str, dict[str, int]],
) -> str:
    period_label = _period_label(time_window, lang)
    title = i18n.t(lang, "HISTORY_TITLE", period=period_label)
    lines = [title]
    lines.append("")
    lines.append(
        i18n.t(
            lang,
            "HISTORY_SUMMARY",
            passed=outcome_counts.get("passed", 0),
            failed=outcome_counts.get("failed", 0),
            neutral=outcome_counts.get("neutral", 0),
            in_progress=outcome_counts.get("in_progress", 0),
        )
    )
    def _score_bucket_line(bucket_key: str, label: str) -> str:
        bucket = score_bucket_counts.get(bucket_key, {})
        passed = int(bucket.get("passed", 0))
        failed = int(bucket.get("failed", 0))
        neutral = int(bucket.get("neutral", 0))
        in_progress = int(bucket.get("in_progress", 0))
        total = passed + failed
        percent = round((passed / total) * 100) if total > 0 else 0
        return i18n.t(
            lang,
            "HISTORY_SCORE_BUCKET_LINE",
            label=label,
            passed=passed,
            failed=failed,
            neutral=neutral,
            in_progress=in_progress,
            percent=percent,
        )

    tp1_total = outcome_counts.get("tp1", 0) + outcome_counts.get("tp2", 0)
    lines.extend(
        [
            "",
            i18n.t(
                lang,
                "HISTORY_STATS_TITLE",
                period=period_label,
            ),
            _score_bucket_line("90-100", "90–100"),
            _score_bucket_line("80-89", "80–89"),
            _score_bucket_line("70-79", "70–79"),
            "",
            i18n.t(
                lang,
                "SCORE_EXPLANATION",
                tp1=tp1_total,
                be=outcome_counts.get("be", 0),
                sl=outcome_counts.get("sl", 0),
                exp=outcome_counts.get("exp", 0),
                nf=outcome_counts.get("no_fill", 0),
            ),
        ]
    )
    lines.append("")
    if not events:
        lines.append(i18n.t(lang, "HISTORY_NO_SIGNALS", period=period_label))
        return "\n".join(lines)
    return "\n".join(lines)


def _history_state_key(user_id: int) -> str:
    return f"history_ctx:{user_id}"


def _set_history_context(user_id: int, time_window: str, page: int) -> None:
    payload = json.dumps({"window": time_window, "page": page})
    set_state(_history_state_key(user_id), payload)


def _get_history_context(user_id: int) -> tuple[str, int] | None:
    payload = get_state(_history_state_key(user_id))
    if not payload:
        return None
    try:
        parsed = json.loads(payload)
    except (TypeError, ValueError):
        return None
    time_window = str(parsed.get("window", ""))
    page = parsed.get("page")
    if time_window not in {"1d", "7d", "30d", "all"}:
        return None
    try:
        page_value = int(page)
    except (TypeError, ValueError):
        return None
    return time_window, max(0, page_value)


def _get_history_page(
    *,
    time_window: str,
    page: int,
) -> tuple[int, int, list[dict], dict, dict[str, dict[str, int]]]:
    enforce_signal_ttl()
    now_ts = int(time.time())
    since_ts = window_since(time_window, now_ts)
    total = count_signal_events(
        user_id=None,
        since_ts=since_ts,
        min_score=None,
    )
    pages = max(1, (total + 9) // 10)
    page = max(0, min(page, pages - 1))
    events_rows = list_signal_events(
        user_id=None,
        since_ts=since_ts,
        min_score=None,
        limit=10,
        offset=page * 10,
    )
    events = [dict(row) for row in events_rows]
    outcome_counts = get_signal_outcome_counts(
        user_id=None,
        since_ts=since_ts,
        min_score=None,
    )
    score_bucket_counts = get_signal_score_bucket_counts(
        user_id=None,
        since_ts=since_ts,
        min_score=None,
    )
    return page, pages, events, outcome_counts, score_bucket_counts


async def _render_history(
    *,
    callback: CallbackQuery,
    time_window: str,
    page: int,
) -> None:
    if callback.message is None or callback.from_user is None:
        return
    page, pages, events, outcome_counts, score_bucket_counts = _get_history_page(
        time_window=time_window,
        page=page,
    )
    await callback.answer()
    lang = get_user_lang(callback.from_user.id) if callback.from_user else None
    lang = lang or "ru"
    _set_history_context(callback.from_user.id, time_window, page)
    await callback.message.edit_text(
        _format_archive_list(
            lang,
            time_window,
            events,
            page,
            pages,
            outcome_counts,
            score_bucket_counts,
        ),
        reply_markup=_archive_inline_kb(
            lang,
            time_window,
            page,
            pages,
            events,
            is_admin_user=is_admin(callback.from_user.id),
        ),
    )


def enforce_signal_ttl() -> int:
    now = int(time.time())
    updated = 0
    open_events = list_open_signal_events()
    for row in open_events:
        event = dict(row)
        created_at = int(event.get("ts", 0))
        if created_at <= 0:
            continue
        age_sec = now - created_at
        if age_sec < SIGNAL_TTL_SECONDS:
            continue
        tp2_hit = bool(event.get("tp2_hit"))
        tp1_hit = bool(event.get("tp1_hit"))
        entry_touched = bool(event.get("entry_touched"))
        if tp2_hit:
            status_value = "TP2"
        elif tp1_hit:
            status_value = "TP1"
        else:
            status_value = "EXP" if entry_touched else "NO_FILL"
        update_signal_event_status_by_id(
            event_id=int(event.get("id", 0)),
            status=status_value,
            result=status_value,
            last_checked_at=now,
        )
        updated_event = dict(event)
        updated_event["status"] = status_value
        updated_event["result"] = status_value
        updated_event["last_checked_at"] = now
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            loop = None
        if loop and loop.is_running():
            loop.create_task(notify_signal_result_short(updated_event))
        updated += 1
    return updated


def _format_price(value: float) -> str:
    if value == 0:
        return "0"
    if value >= 100:
        return f"{value:,.2f}".replace(",", " ")
    if value >= 1:
        return f"{value:,.4f}".replace(",", " ")
    return f"{value:.6f}"


def _format_duration(seconds: int) -> str:
    seconds = max(0, int(seconds))
    hours = seconds // 3600
    minutes = (seconds % 3600) // 60
    if hours > 0:
        return f"{hours}ч {minutes}м"
    return f"{minutes}м"


def _normalize_signal_status(status: str) -> str:
    normalized = (status or "").upper().strip()
    if normalized == "NO_FILL":
        return "NF"
    if normalized == "EXPIRED":
        return "EXP"
    return normalized


def _is_final_signal_status(status: str) -> bool:
    normalized = _normalize_signal_status(status)
    return normalized in {"TP1", "TP2", "BE", "SL", "EXP", "NF"}


def _format_short_result_message(event: dict) -> str | None:
    status_raw = str(event.get("result") or event.get("status") or "OPEN")
    status = _normalize_signal_status(status_raw)
    symbol = str(event.get("symbol", "")).upper()
    side = str(event.get("side", "")).upper()
    score = int(event.get("score", 0))
    header = ""
    subtitle = ""
    detail_lines: list[str] = []

    if status == "TP1":
        header = "✅ TP1"
        subtitle = "✅ Сигнал закрылся в плюс"
        detail_lines = [
            f"{symbol} {side}",
            "Результат: TP1 🎯",
            f"Score: {score}",
        ]
    elif status == "TP2":
        header = "✅ TP2"
        subtitle = "🚀 Сигнал выполнен полностью"
        detail_lines = [
            f"{symbol} {side}",
            "Результат: TP2 🎯",
            f"Score: {score}",
        ]
    elif status == "BE":
        header = "⚪ BE"
        subtitle = "⚪ Сигнал ушёл в безубыток"
        detail_lines = [
            f"{symbol} {side}",
            "Результат: BE",
            "Риск снят",
        ]
    elif status == "SL":
        header = "❌ SL"
        subtitle = "❌ Сигнал закрылся по стопу"
        detail_lines = [
            f"{symbol} {side}",
            "Результат: SL",
            f"Score: {score}",
        ]
    elif status == "NF":
        header = "⏳ NF"
        subtitle = "⏳ Сигнал не активировался"
        detail_lines = [
            f"{symbol} {side}",
            "Результат: NF",
            "Цена не дошла до входа",
        ]
    elif status == "EXP":
        header = "⏳ EXP"
        subtitle = "⏳ Сценарий устарел"
        detail_lines = [
            f"{symbol} {side}",
            "Результат: EXP",
            "Истёк лимит 12 часов",
        ]
    else:
        return None

    return "\n".join([header, subtitle, "", *detail_lines])


async def notify_signal_result_short(signal: dict) -> bool:
    if bot is None:
        return False
    status_raw = str(signal.get("result") or signal.get("status") or "")
    if not _is_final_signal_status(status_raw):
        return False
    if bool(signal.get("result_notified")):
        return False

    user_id = int(signal.get("user_id", 0))
    if user_id <= 0:
        return False
    if is_user_locked(user_id):
        return False
    if not is_sub_active(user_id):
        return False
    if not is_notify_enabled(user_id, "ai_signals"):
        return False

    message_text = _format_short_result_message(signal)
    if not message_text:
        return False

    try:
        await bot.send_message(user_id, message_text)
    except Exception as exc:
        print(f"[ai_signals] Failed to send result notification to {user_id}: {exc}")
        return False

    event_id = int(signal.get("id", 0))
    if event_id:
        mark_signal_result_notified(event_id)
    return True


def _format_outcome_block(event: dict) -> list[str]:
    status_raw = str(event.get("result") or event.get("status") or "OPEN")
    status = _normalize_signal_status(status_raw)
    entry_touched = bool(event.get("entry_touched"))
    created_at = int(event.get("ts", 0))
    closed_at = int(event.get("closed_at") or 0)
    updated_at = int(event.get("updated_at") or 0)
    last_checked_at = int(event.get("last_checked_at") or 0)
    finalized_at = closed_at or updated_at
    close_reason = event.get("close_reason")
    now = int(time.time())
    final_statuses = {"TP1", "TP2", "BE", "SL", "EXP", "NF"}

    lines: list[str] = []
    if status in final_statuses:
        if status == "TP1":
            header = "📌 Итог: ✅ TP1 достигнут"
            comment = "цена дошла до TP1, дальше до TP2 не дошла (это нормально)"
        elif status == "TP2":
            header = "📌 Итог: ✅ TP2 достигнут"
            comment = "цена дошла до TP2, сценарий выполнен полностью"
        elif status == "BE":
            header = "📌 Итог: ✅ BE (безубыток)"
            comment = "сценарий дал движение, риск снят"
        elif status == "SL":
            header = "📌 Итог: ❌ SL"
            comment = "цена дошла до стопа до выполнения TP1"
        elif status == "NF":
            header = "📌 Итог: ⏳ NF (вход не был активирован)"
            comment = "цена не дошла до зоны POI"
        else:
            entry_label = "вход был активирован" if entry_touched else "вход не был активирован"
            header = "📌 Итог: ⏳ EXP (сценарий устарел)"
            comment = f"{entry_label}, но условия не реализовались"

        lines.append(header)
        if status in {"NF", "EXP"}:
            lines.append("⏱ Прошло 12 часов")
        elif finalized_at:
            lines.append(f"⏱ Время: {_format_event_time(finalized_at)}")
        elif last_checked_at:
            lines.append(f"⏱ Время: {_format_event_time(last_checked_at)}")
        if close_reason:
            lines.append(f"🧾 Причина: {close_reason}")
        lines.append(f"💬 Комментарий: {comment}")
        return lines

    remaining = SIGNAL_TTL_SECONDS - (now - created_at)
    status_hint = "ожидает подтверждение" if entry_touched else "ожидает вход"
    lines.extend(
        [
            "📌 Итог: ⏰ В процессе",
            f"🕒 До истечения: {_format_duration(remaining)}",
            f"💬 Сейчас: {status_hint}",
        ]
    )
    if last_checked_at:
        lines.append(f"🔎 Последняя проверка: {_format_event_time(last_checked_at)}")
    return lines


def _format_issue_hint_block(event: dict) -> list[str]:
    status_raw = str(event.get("result") or event.get("status") or "OPEN")
    status = _normalize_signal_status(status_raw)
    close_reason = event.get("close_reason")
    if status not in {"SL", "EXP", "NF"}:
        return []
    lines = ["", "🧩 Что не так было:"]
    if close_reason:
        lines.append(f"• {close_reason}")
        return lines
    if status == "SL":
        lines.extend(
            [
                "• стоп ближе, чем реальная волатильность (ATR высокий)",
                "• вход без подтверждения 5–15m или контекст рынка сменился",
            ]
        )
    elif status == "NF":
        lines.append("• цена не дошла до POI — это нормально, сигнал просто не активировался")
    else:
        lines.append("• время жизни 12ч истекло — сценарий больше не актуален")
    return lines


def _format_refresh_report(event: dict, lang: str) -> str:
    status_raw = str(event.get("status", "OPEN")).upper()
    status_map = {
        "OPEN": i18n.t(lang, "STATUS_OPEN"),
        "TP1": "TP1 ✅",
        "TP2": "TP2 ✅",
        "SL": "SL ⛔",
        "EXP": "EXP ⌛",
        "EXPIRED": "EXP ⌛",
        "NO_FILL": "NF ⏳",
        "BE": "BE",
        "AMBIGUOUS": i18n.t(lang, "STATUS_AMBIGUOUS"),
    }
    status_label = status_map.get(status_raw, status_raw)
    symbol = event.get("symbol")
    side = event.get("side")
    score = int(event.get("score", 0))
    last_price = event.get("last_price")
    last_checked_at = int(event.get("last_checked_at") or time.time())
    created_at = int(event.get("ts", 0))
    entry_from = float(event.get("poi_low", 0.0))
    entry_to = float(event.get("poi_high", 0.0))
    entry_touched = bool(event.get("entry_touched"))
    lines = [
        "✅ Обновлено",
        "",
        f"Сигнал: {symbol} {side} (Score {score})",
    ]
    if last_price is not None:
        lines.append(f"Текущая цена: {_format_price(float(last_price))}")
    lines.append(f"Последняя проверка: {_format_event_time(last_checked_at)}")
    lines.append("")
    if status_raw == "OPEN":
        status_label = (
            "OPEN / активирован" if entry_touched else "OPEN / ожидает вход"
        )
    lines.append(f"Статус: {status_label}")
    if status_raw == "OPEN":
        touched_label = "тронуто" if entry_touched else "не тронуто"
        lines.append(f"• entry: {entry_from:.4f}–{entry_to:.4f} ({touched_label})")
        remaining = SIGNAL_TTL_SECONDS - (int(time.time()) - created_at)
        lines.append(f"• до конца жизни: {_format_duration(remaining)}")
    elif status_raw in {"NO_FILL"}:
        lines.append("• прошло 12ч")
        lines.append("• цена не дошла до входа")
    elif status_raw in {"EXP", "EXPIRED"}:
        lines.append("• прошло 12ч после активации")
        lines.append("• сценарий устарел")
    return "\n".join(lines)


def _parse_refresh_kline(kline: list[Any]) -> dict | None:
    try:
        return {
            "open_time": int(kline[0]),
            "high": float(kline[2]),
            "low": float(kline[3]),
            "close": float(kline[4]),
        }
    except (TypeError, ValueError, IndexError):
        return None


def _entry_filled(candle: dict, entry_from: float, entry_to: float) -> bool:
    return float(candle["low"]) <= entry_to and float(candle["high"]) >= entry_from


def _check_hits(
    candle: dict,
    direction: str,
    sl: float,
    tp1: float,
    tp2: float,
) -> tuple[bool, bool, bool]:
    if direction == "long":
        sl_hit = float(candle["low"]) <= sl
        tp1_hit = float(candle["high"]) >= tp1
        tp2_hit = float(candle["high"]) >= tp2
    else:
        sl_hit = float(candle["high"]) >= sl
        tp1_hit = float(candle["low"]) <= tp1
        tp2_hit = float(candle["low"]) <= tp2
    return sl_hit, tp1_hit, tp2_hit


async def refresh_signal(event_id: int) -> dict | None:
    event = get_signal_event(user_id=None, event_id=event_id)
    if event is None:
        return None
    previous_status = _normalize_signal_status(
        str(event.get("result") or event.get("status") or "OPEN")
    )
    now = int(time.time())
    last_checked = int(event["last_checked_at"] or 0)
    refresh_cooldown = int(os.getenv("SIGNAL_REFRESH_COOLDOWN_SEC", "12"))
    if last_checked and now - last_checked < refresh_cooldown:
        return {"error": "cooldown", "retry_after": refresh_cooldown - (now - last_checked)}

    symbol = str(event.get("symbol", ""))
    created_at = int(event.get("ts", 0))
    cutoff_ts = min(now, created_at + SIGNAL_TTL_SECONDS)
    start_ms = created_at * 1000
    limit = max(200, int(SIGNAL_TTL_SECONDS / 300) + 20)
    with binance_request_context("signal_refresh"):
        data = await fetch_klines(symbol, "5m", limit, start_ms=start_ms)
    candles: list[dict] = []
    last_price: float | None = None
    if data:
        cutoff_ms = cutoff_ts * 1000
        for item in data:
            parsed = _parse_refresh_kline(item)
            if not parsed:
                continue
            if parsed["open_time"] < start_ms:
                continue
            if parsed["open_time"] > cutoff_ms:
                break
            candles.append(parsed)
        if candles:
            last_price = candles[-1]["close"]

    entry_from = float(event.get("poi_low", 0.0))
    entry_to = float(event.get("poi_high", 0.0))
    sl = float(event.get("sl", 0.0))
    tp1 = float(event.get("tp1", 0.0))
    tp2 = float(event.get("tp2", 0.0))
    side = str(event.get("side", "")).upper()
    direction = "long" if side in {"LONG", "BUY"} else "short"

    entry_touched = False
    tp1_hit = False
    tp2_hit = False
    outcome: str | None = None

    for candle in candles:
        if not entry_touched and _entry_filled(candle, entry_from, entry_to):
            entry_touched = True
        if not entry_touched:
            continue
        sl_hit, tp1_hit_candle, tp2_hit_candle = _check_hits(
            candle, direction, sl, tp1, tp2
        )
        if tp2_hit_candle:
            tp2_hit = True
            outcome = "TP2"
            break
        if tp1_hit_candle:
            tp1_hit = True
        if sl_hit and not tp1_hit:
            outcome = "SL"
            break

    if outcome is None:
        if tp1_hit:
            outcome = "TP1"
        elif now - created_at >= SIGNAL_TTL_SECONDS:
            if entry_touched:
                outcome = "EXP"
            else:
                outcome = "NO_FILL"

    status_value = outcome or str(event.get("status", "OPEN"))
    result_value = outcome or str(event.get("result") or status_value)
    close_reason = None
    closed_at = None
    if outcome in {"TP1", "TP2"}:
        close_reason = f"hit_{outcome.lower()}"
        closed_at = now
    elif outcome == "SL":
        close_reason = "hit_sl"
        closed_at = now
    elif outcome == "EXP":
        close_reason = "expired_after_entry"
        closed_at = now
    elif outcome == "NO_FILL":
        close_reason = "expired_no_fill"
        closed_at = now
    update_signal_event_refresh(
        event_id=event_id,
        status=status_value,
        result=result_value,
        entry_touched=entry_touched,
        tp1_hit=tp1_hit,
        tp2_hit=tp2_hit,
        last_checked_at=now,
        close_reason=close_reason,
        closed_at=closed_at,
    )
    updated = dict(event)
    updated["status"] = status_value
    updated["result"] = result_value
    updated["entry_touched"] = entry_touched
    updated["tp1_hit"] = tp1_hit
    updated["tp2_hit"] = tp2_hit
    updated["last_checked_at"] = now
    updated["last_price"] = last_price
    if close_reason:
        updated["close_reason"] = close_reason
    if closed_at:
        updated["closed_at"] = closed_at
    if _is_final_signal_status(status_value) and not _is_final_signal_status(previous_status):
        await notify_signal_result_short(updated)
    return updated


def _format_archive_detail(event: dict, lang: str) -> str:
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
                "global_trend": i18n.t(lang, "BREAKDOWN_GLOBAL_TREND"),
                "local_trend": i18n.t(lang, "BREAKDOWN_LOCAL_TREND"),
                "near_key_level": i18n.t(lang, "BREAKDOWN_NEAR_KEY_LEVEL"),
                "liquidity_sweep": i18n.t(lang, "BREAKDOWN_LIQUIDITY_SWEEP"),
                "volume_climax": i18n.t(lang, "BREAKDOWN_VOLUME_CLIMAX"),
                "rsi_divergence": i18n.t(lang, "BREAKDOWN_RSI_DIVERGENCE"),
                "atr_ok": i18n.t(lang, "BREAKDOWN_ATR_OK"),
                "bb_extreme": i18n.t(lang, "BREAKDOWN_BB_EXTREME"),
                "ma_trend_ok": i18n.t(lang, "BREAKDOWN_MA_TREND_OK"),
                "orderflow": i18n.t(lang, "BREAKDOWN_ORDERFLOW"),
                "whale_activity": i18n.t(lang, "BREAKDOWN_WHALE_ACTIVITY"),
                "ai_pattern": i18n.t(lang, "BREAKDOWN_AI_PATTERN"),
                "market_regime": i18n.t(lang, "BREAKDOWN_MARKET_REGIME"),
            }
            for item in breakdown_items:
                if not isinstance(item, dict):
                    continue
                key = item.get("key")
                label = item.get("label")
                if key in label_map:
                    label = label_map[key]
                label = label or key or i18n.t(lang, "BREAKDOWN_FALLBACK")
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
        i18n.t(
            lang,
            "ARCHIVE_DETAIL_LIFETIME",
            hours=SIGNAL_TTL_SECONDS // 3600,
        ),
    ]
    lines.extend(["", *_format_outcome_block(event)])
    lines.extend(_format_issue_hint_block(event))
    if breakdown_lines:
        lines.extend(
            [
                "",
                i18n.t(lang, "ARCHIVE_DETAIL_REASON_HEADER", score=score),
                *breakdown_lines,
            ]
        )
    return "\n".join(lines)


@dp.message(F.text.in_(i18n.all_labels("MENU_STATS")))
async def stats_menu(message: Message):
    lang = get_user_lang(message.chat.id) or "ru"
    await message.answer(
        i18n.t(lang, "STATS_PICK_TEXT"),
        reply_markup=stats_inline_kb(lang),
    )


@dp.callback_query(F.data.regexp(r"^hist:(1d|7d|30d|all):\d+$"))
async def history_callback(callback: CallbackQuery):
    if callback.message is None or callback.from_user is None:
        return
    _, time_window, page_raw = callback.data.split(":")
    page_value = max(0, int(page_raw))
    print(f"[history] window={time_window} page={page_value}")
    await _render_history(callback=callback, time_window=time_window, page=page_value)


def _archive_inline_kb(
    lang: str,
    time_window: str,
    page: int,
    pages: int,
    events: list[dict],
    *,
    is_admin_user: bool,
) -> InlineKeyboardMarkup:
    rows: list[list[InlineKeyboardButton]] = []
    period_label = _period_label(time_window, lang)
    if not events:
        rows.append(
            [
                InlineKeyboardButton(
                    text=i18n.t(lang, "HISTORY_NO_SIGNALS_BUTTON", period=period_label),
                    callback_data="hist_noop",
                )
            ]
        )
    for event in events:
        event_status = str(event.get("status", ""))
        status_icon = _status_icon(event_status)
        rows.append(
            [
                InlineKeyboardButton(
                    text=(
                        f"{status_icon} Score {int(event.get('score', 0))} • "
                        f"{event.get('symbol')} {event.get('side')} | "
                        f"{_format_event_time(int(event.get('ts', 0)))}"
                    ),
                    callback_data=f"sig_open:{time_window}:{event.get('id')}",
                )
            ]
        )
        if is_admin_user:
            rows.append(
                [
                    InlineKeyboardButton(
                        text="🔄 Обновить",
                        callback_data=f"sig_refresh:{event.get('id')}",
                    )
                ]
            )

    nav_row: list[InlineKeyboardButton] = []
    if page > 0:
        nav_row.append(
            InlineKeyboardButton(
                text=i18n.t(lang, "NAV_PREV"),
                callback_data=f"hist:{time_window}:{page - 1}",
            )
        )
    if page < pages - 1:
        nav_row.append(
            InlineKeyboardButton(
                text=i18n.t(lang, "NAV_NEXT"),
                callback_data=f"hist:{time_window}:{page + 1}",
            )
        )
    if nav_row:
        rows.append(nav_row)
    rows.append(
        [
            InlineKeyboardButton(
                text=i18n.t(lang, "NAV_BACK"),
                callback_data="hist_back",
            )
        ]
    )
    return InlineKeyboardMarkup(inline_keyboard=rows)


def _archive_detail_kb(
    *,
    lang: str,
    back_callback: str,
    event_id: int,
    event_status: str,
    is_admin_user: bool,
) -> InlineKeyboardMarkup:
    rows: list[list[InlineKeyboardButton]] = []
    if is_admin_user:
        rows.append(
            [
                InlineKeyboardButton(
                    text="🔄 Обновить",
                    callback_data=f"sig_refresh:{event_id}",
                )
            ]
        )
    rows.append(
        [
            InlineKeyboardButton(
                text=i18n.t(lang, "NAV_BACK"),
                callback_data=back_callback,
            )
        ]
    )
    return InlineKeyboardMarkup(inline_keyboard=rows)


@dp.callback_query(F.data.regexp(r"^sig_open:(1d|7d|30d|all):\d+$"))
async def sig_open(callback: CallbackQuery):
    logger.warning("SIG_OPEN HANDLER FIRED: %s", callback.data)
    if callback.message is None or callback.from_user is None:
        return
    await callback.answer()
    try:
        logger.info("sig_open callback: %s", callback.data)
        _, time_window, event_id_raw = callback.data.split(":")
        event_id = int(event_id_raw)
        event_row = get_signal_by_id(event_id)
        if event_row is None:
            lang = get_user_lang(callback.from_user.id) or "ru"
            await callback.answer(i18n.t(lang, "SIGNAL_NOT_FOUND"), show_alert=True)
            return
        event = dict(event_row)
        lang = get_user_lang(callback.from_user.id) if callback.from_user else None
        lang = lang or "ru"
        back_callback = f"hist:{time_window}:0"
        context = _get_history_context(callback.from_user.id)
        if context:
            stored_window, page = context
            back_callback = f"hist:{stored_window}:{page}"
        detail_text = _format_archive_detail(event, lang)
        detail_markup = _archive_detail_kb(
            lang=lang,
            back_callback=back_callback,
            event_id=event_id,
            event_status=str(event.get("status", "")),
            is_admin_user=is_admin(callback.from_user.id),
        )
        try:
            await callback.message.edit_text(detail_text, reply_markup=detail_markup)
        except Exception:
            await callback.message.answer(detail_text, reply_markup=detail_markup)
    except Exception as exc:
        mark_error("sig_open", str(exc))
        logger.exception("sig_open failed: %s", exc)
        with suppress(Exception):
            await callback.answer(
                f"Ошибка открытия: {type(exc).__name__}",
                show_alert=True,
            )


@dp.callback_query(F.data == "hist_back")
async def archive_back(callback: CallbackQuery):
    if callback.message is None:
        return
    lang = get_user_lang(callback.from_user.id) if callback.from_user else None
    lang = lang or "ru"
    await callback.answer()
    await callback.message.edit_text(
        i18n.t(lang, "STATS_PICK_TEXT"),
        reply_markup=stats_inline_kb(lang),
    )


@dp.callback_query(F.data == "hist_noop")
async def history_noop(callback: CallbackQuery):
    await callback.answer()


@dp.callback_query(F.data.regexp(r"^sig_refresh:\d+$"))
async def sig_refresh(callback: CallbackQuery):
    if callback.from_user is None:
        return
    try:
        if not is_admin(callback.from_user.id):
            await callback.answer("Недостаточно прав", show_alert=True)
            return
        await callback.answer("🔄 Проверяю Binance…")
        event_id = int(callback.data.split(":", 1)[1])
        refreshed = await refresh_signal(event_id)
        if refreshed is None:
            await callback.answer(
                i18n.t(get_user_lang(callback.from_user.id) or "ru", "SIGNAL_NOT_FOUND"),
                show_alert=True,
            )
            return
        if isinstance(refreshed, dict) and refreshed.get("error") == "cooldown":
            retry_after = int(refreshed.get("retry_after", 0))
            await callback.answer(f"Подождите {retry_after} сек.", show_alert=True)
            return

        lang = get_user_lang(callback.from_user.id) if callback.from_user else None
        lang = lang or "ru"
        report_text = _format_refresh_report(refreshed, lang)

        if callback.message is None:
            return
        if callback.message.text and callback.message.text.startswith("📊"):
            context = _get_history_context(callback.from_user.id)
            if context:
                time_window, page = context
                page, pages, events, outcome_counts, score_bucket_counts = _get_history_page(
                    time_window=time_window,
                    page=page,
                )
                await callback.message.edit_text(
                    _format_archive_list(
                        lang,
                        time_window,
                        events,
                        page,
                        pages,
                        outcome_counts,
                        score_bucket_counts,
                    ),
                    reply_markup=_archive_inline_kb(
                        lang,
                        time_window,
                        page,
                        pages,
                        events,
                        is_admin_user=True,
                    ),
                )
                await callback.message.answer(report_text)
                return

        event = get_signal_event(user_id=None, event_id=event_id)
        if event is None:
            await callback.message.answer(report_text)
            return
        back_callback = "hist_back"
        context = _get_history_context(callback.from_user.id)
        if context:
            time_window, page = context
            back_callback = f"hist:{time_window}:{page}"
        with suppress(Exception):
            await callback.message.edit_text(
                _format_archive_detail(dict(event), lang),
                reply_markup=_archive_detail_kb(
                    lang=lang,
                    back_callback=back_callback,
                    event_id=event_id,
                    event_status=str(event.get("status", "")),
                    is_admin_user=True,
                ),
            )
        await callback.message.answer(report_text)
    except Exception as exc:
        mark_error("sig_refresh", str(exc))
        with suppress(Exception):
            await callback.answer("Ошибка, см. логи", show_alert=True)

@dp.callback_query(F.data == "ai_notify_on")
async def ai_notify_on(callback: CallbackQuery):
    if callback.from_user is None:
        return
    chat_id = callback.from_user.id
    lang = get_user_lang(chat_id) or "ru"
    if get_user_pref(chat_id, "ai_signals_enabled", 0):
        await callback.answer(i18n.t(lang, "ALREADY_ON"))
        if callback.message:
            await callback.message.answer(i18n.t(lang, "AI_ALREADY_ON"))
        return
    ensure_trial_defaults(chat_id)
    set_user_pref(chat_id, "ai_signals_enabled", 1)
    await callback.answer()
    if callback.message:
        await callback.message.answer(i18n.t(lang, "AI_ON_OK"))


@dp.callback_query(F.data == "ai_notify_off")
async def ai_notify_off(callback: CallbackQuery):
    if callback.from_user is None:
        return
    chat_id = callback.from_user.id
    lang = get_user_lang(chat_id) or "ru"
    if not get_user_pref(chat_id, "ai_signals_enabled", 0):
        await callback.answer(i18n.t(lang, "ALREADY_OFF"))
        if callback.message:
            await callback.message.answer(i18n.t(lang, "AI_ALREADY_OFF"))
        return
    set_user_pref(chat_id, "ai_signals_enabled", 0)
    await callback.answer()
    if callback.message:
        await callback.message.answer(i18n.t(lang, "AI_OFF_OK"))


@dp.callback_query(F.data == "pumpdump_notify_on")
async def pumpdump_notify_on(callback: CallbackQuery):
    if callback.from_user is None:
        return
    chat_id = callback.from_user.id
    lang = get_user_lang(chat_id) or "ru"
    if get_user_pref(chat_id, "pumpdump_enabled", 0):
        await callback.answer(i18n.t(lang, "ALREADY_ON"))
        if callback.message:
            await callback.message.answer(i18n.t(lang, "PD_ALREADY_ON"))
        return
    ensure_trial_defaults(chat_id)
    set_user_pref(chat_id, "pumpdump_enabled", 1)
    await callback.answer()
    if callback.message:
        await callback.message.answer(i18n.t(lang, "PD_ENABLED_TEXT"))


@dp.callback_query(F.data == "pumpdump_notify_off")
async def pumpdump_notify_off(callback: CallbackQuery):
    if callback.from_user is None:
        return
    chat_id = callback.from_user.id
    lang = get_user_lang(chat_id) or "ru"
    if not get_user_pref(chat_id, "pumpdump_enabled", 0):
        await callback.answer(i18n.t(lang, "ALREADY_OFF"))
        if callback.message:
            await callback.message.answer(i18n.t(lang, "PD_ALREADY_OFF"))
        return
    set_user_pref(chat_id, "pumpdump_enabled", 0)
    await callback.answer()
    if callback.message:
        await callback.message.answer(i18n.t(lang, "PD_OFF_OK"))


@dp.callback_query(F.data.startswith("sub_paywall:"))
async def subscription_paywall_callback(callback: CallbackQuery):
    if callback.from_user is None:
        return
    await callback.answer()
    source = callback.data.split(":", 1)[1] if callback.data else "system"
    if callback.message:
        await show_subscribe_offer(
            callback.message,
            callback.from_user.id,
            source=source,
            edit=True,
        )


def _human_ago(seconds: int, lang: str) -> str:
    if seconds < 0:
        seconds = 0
    if seconds < 60:
        return i18n.t(lang, "STATUS_AGO_SECONDS", seconds=seconds)
    minutes = seconds // 60
    if minutes < 60:
        return i18n.t(lang, "STATUS_AGO_MINUTES", minutes=minutes)
    hours = minutes // 60
    return i18n.t(lang, "STATUS_AGO_HOURS", hours=hours)


def _parse_user_id_arg(text: str | None) -> int | None:
    if not text:
        return None
    parts = text.strip().split()
    if len(parts) < 2:
        return None
    try:
        return int(parts[1])
    except (TypeError, ValueError):
        return None


def _parse_symbol_arg(text: str | None) -> str | None:
    if not text:
        return None
    parts = text.strip().split()
    if len(parts) < 2:
        return None
    symbol = parts[1].strip().upper()
    return symbol or None


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


def _format_fails_top(fails: dict, lang: str, top_n: int = 5) -> str:
    if not fails:
        return ""
    ordered = sorted(fails.items(), key=lambda item: item[1], reverse=True)[:top_n]
    lines = [f"• {i18n.t(lang, 'DIAG_FAILS_TOP')}"]
    for reason, count in ordered:
        lines.append(f"  • {reason} — {count}")
    return "\n".join(lines)


def _format_near_miss(near_miss: dict, lang: str) -> str:
    if not near_miss:
        return ""
    lines = [f"• {i18n.t(lang, 'DIAG_NEAR_MISS')}"]
    for reason, count in sorted(near_miss.items(), key=lambda item: item[1], reverse=True):
        lines.append(f"  • {reason} — {count}")
    return "\n".join(lines)


def _build_rotation_order(symbols: list[str], *, shuffle: bool) -> list[str]:
    if not shuffle:
        return list(symbols)
    seed = datetime.now(timezone.utc).strftime("%Y-%m-%d")
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


def _build_status_label(
    *,
    ok: bool,
    warn: bool,
    error: bool,
    ok_text: str,
    warn_text: str,
    error_text: str,
) -> str:
    if error:
        return f"ERROR ❌ {error_text}"
    if warn:
        return f"WARN ⚠️ {warn_text}"
    return f"OK ✅ {ok_text}"


def _format_section(title: str, status_label: str, details: list[str], lang: str) -> str:
    lines = [
        title,
        i18n.t(lang, "DIAG_MODULE_STATUS", status=status_label),
    ]
    lines.extend(details)
    return "\n".join(lines)


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


def _format_slowest_symbols(items: list[dict[str, Any]]) -> str:
    if not items:
        return "-"
    parts = []
    for item in items:
        symbol = item.get("symbol", "-")
        total_dt = item.get("total_dt", 0.0)
        steps = item.get("steps", {}) or {}
        step_parts = [
            f"{key}={value:.2f}s" for key, value in sorted(steps.items(), key=lambda kv: kv[1], reverse=True)
        ]
        steps_str = ", ".join(step_parts) if step_parts else "-"
        parts.append(f"{symbol} {total_dt:.2f}s ({steps_str})")
    return "; ".join(parts)


def _format_db_status(lang: str) -> str:
    path = get_db_path()
    exists = os.path.exists(path)
    status_label = _build_status_label(
        ok=exists,
        warn=False,
        error=not exists,
        ok_text=i18n.t(lang, "DIAG_STATUS_OK"),
        warn_text=i18n.t(lang, "DIAG_STATUS_ISSUES"),
        error_text=i18n.t(lang, "DIAG_STATUS_MISSING"),
    )
    details = [i18n.t(lang, "DIAG_DB_PATH", path=path)]
    if not exists:
        details.append(i18n.t(lang, "DIAG_DB_MISSING"))
        return _format_section(i18n.t(lang, "DIAG_DB_TITLE"), status_label, details, lang)
    size_bytes = os.path.getsize(path)
    mtime = datetime.fromtimestamp(os.path.getmtime(path))
    details.extend(
        [
            i18n.t(lang, "DIAG_DB_SIZE", size=size_bytes),
            i18n.t(lang, "DIAG_DB_MODIFIED", mtime=f"{mtime:%Y-%m-%d %H:%M:%S}"),
        ]
    )
    return _format_section(i18n.t(lang, "DIAG_DB_TITLE"), status_label, details, lang)


def _format_overall_status(now: float, lang: str) -> str:
    modules = [MODULES.get("ai_signals"), MODULES.get("pumpdump")]
    has_tick = any(st and st.last_tick for st in modules)
    has_error = any(st and st.last_error for st in modules)
    has_warn = any(st and st.last_warn for st in modules)
    status_label = _build_status_label(
        ok=has_tick and not has_error and not has_warn,
        warn=(not has_tick) or has_warn,
        error=has_error,
        ok_text=i18n.t(lang, "DIAG_STATUS_WORKING"),
        warn_text=i18n.t(lang, "DIAG_STATUS_ISSUES"),
        error_text=i18n.t(lang, "DIAG_STATUS_ERROR"),
    )
    details = []
    error_lines = [st for st in modules if st and st.last_error]
    warn_lines = [st for st in modules if st and st.last_warn]
    if error_lines:
        details.append(i18n.t(lang, "DIAG_ERRORS_HEADER"))
        for st in error_lines:
            details.append(f"  • ⚠️ {st.name}: {st.last_error}")
    else:
        details.append(i18n.t(lang, "DIAG_ERRORS_NONE"))
    if warn_lines:
        details.append(i18n.t(lang, "DIAG_WARNINGS_HEADER"))
        for st in warn_lines:
            details.append(f"  • ⚠️ {st.name}: {st.last_warn}")
    else:
        details.append(i18n.t(lang, "DIAG_WARNINGS_NONE"))
    restarts = 0
    if modules and modules[0]:
        restarts = modules[0].binance_session_restarts
    details.append(i18n.t(lang, "DIAG_SESSION_RESTARTS", count=restarts))
    return _format_section(i18n.t(lang, "DIAG_SECTION_OVERALL"), status_label, details, lang)


def _format_ai_section(st, now: float, lang: str) -> str:
    extra = _parse_extra_kv(st.extra or "")
    status_label = _build_status_label(
        ok=bool(st.last_tick) and not st.last_error and not st.last_warn,
        warn=bool(st.last_warn) or not st.last_tick,
        error=bool(st.last_error),
        ok_text=i18n.t(lang, "DIAG_STATUS_WORKING"),
        warn_text=i18n.t(lang, "DIAG_STATUS_ISSUES"),
        error_text=i18n.t(lang, "DIAG_STATUS_ERROR"),
    )
    tick = (
        _human_ago(int(now - st.last_tick), lang)
        if st.last_tick
        else i18n.t(lang, "DIAG_STATUS_NOT_STARTED")
    )
    details = [i18n.t(lang, "DIAG_MODULE_LAST_CYCLE", tick=tick)]
    if st.last_ok:
        details.append(
            i18n.t(
                lang,
                "DIAG_MODULE_LAST_OK",
                tick=_human_ago(int(now - st.last_ok), lang),
            )
        )
    if st.last_error:
        details.append(i18n.t(lang, "DIAG_MODULE_ERROR", error=f"⚠️ {st.last_error}"))
    if st.last_warn:
        details.append(i18n.t(lang, "DIAG_MODULE_WARNING", warning=f"⚠️ {st.last_warn}"))
    universe = extra.get("universe") or (str(st.total_symbols) if st.total_symbols else None)
    chunk = extra.get("chunk")
    cur = extra.get("cursor") or (str(st.cursor) if st.cursor else None)
    current_symbol = extra.get("current") or st.current_symbol or "-"
    details.append(i18n.t(lang, "DIAG_MARKET_SCAN_HEADER"))
    if universe:
        details.append(i18n.t(lang, "DIAG_MARKET_UNIVERSE", count=universe))
    if chunk:
        details.append(i18n.t(lang, "DIAG_MARKET_CHUNK", count=chunk))
    if cur and universe:
        details.append(i18n.t(lang, "DIAG_MARKET_POSITION_TOTAL", current=cur, total=universe))
    elif cur:
        details.append(i18n.t(lang, "DIAG_MARKET_POSITION", current=cur))
    details.append(i18n.t(lang, "DIAG_MARKET_CURRENT", symbol=current_symbol))
    excluded_symbols = _get_ai_excluded_symbols()
    details.append(
        i18n.t(lang, "DIAG_AI_EXCLUDED", symbols=_format_symbol_list(excluded_symbols))
    )
    state = st.state or {}
    adapt_enabled = state.get("ai_adapt_enabled")
    chunk_current = state.get("ai_chunk_current")
    chunk_min = state.get("ai_chunk_min")
    chunk_max = state.get("ai_chunk_max")
    safe_mode = state.get("ai_safe_mode")
    if safe_mode is None:
        safe_mode = AI_SAFE_MODE
    chunk_max_effective = chunk_max if chunk_max is not None else AI_CHUNK_MAX_EFFECTIVE
    chunk_max_base = state.get("ai_chunk_max_base") or AI_CHUNK_MAX
    chunk_max_safe_cap = state.get("ai_chunk_max_safe_cap") or AI_SAFE_CHUNK_MAX
    if adapt_enabled is not None:
        details.append(
            f"• Adaptive chunk: {'enabled' if adapt_enabled else 'disabled'}"
        )
    if chunk_current is not None and chunk_min is not None and chunk_max is not None:
        details.append(
            f"• Chunk size (current): {chunk_current} | min: {chunk_min} | max: {chunk_max}"
        )
    details.append(f"• Safe mode: {'on' if safe_mode else 'off'}")
    details.append(
        "• Chunk max (effective): "
        f"{chunk_max_effective} (base={chunk_max_base}, safe_cap={chunk_max_safe_cap})"
    )
    no_klines_rate = state.get("ai_no_klines_rate")
    timeout_rate = state.get("ai_timeout_rate")
    stable_cycles = state.get("ai_stable_cycles")
    if no_klines_rate is not None and timeout_rate is not None:
        stable_label = stable_cycles if stable_cycles is not None else 0
        details.append(
            "• Cycle fail rates: "
            f"no_klines={no_klines_rate:.0%} "
            f"timeout={timeout_rate:.0%} "
            f"stable_cycles={stable_label}"
        )
    cyc = extra.get("cycle")
    if cyc:
        details.append(i18n.t(lang, "DIAG_CYCLE_TIME", cycle=cyc))
    details.append(f"• Cycle sleep: {max(0.0, AI_CYCLE_SLEEP_SEC):g}s")
    timeout_count = state.get("fail_symbol_timeout_count")
    if isinstance(timeout_count, int):
        details.append(f"• Symbol timeouts: {timeout_count}")
    slowest_symbols = state.get("top_slowest_symbols")
    if slowest_symbols:
        details.append(f"• Top slowest: {_format_slowest_symbols(slowest_symbols)}")

    details.append(i18n.t(lang, "DIAG_AI_CONFIG_TITLE"))
    details.extend(
        [
            i18n.t(lang, "DIAG_AI_CONFIG_MAX_DEEP", value=AI_MAX_DEEP_PER_CYCLE),
            i18n.t(lang, "DIAG_AI_CONFIG_STAGE_A", value=AI_STAGE_A_TOP_K),
            i18n.t(lang, "DIAG_AI_CONFIG_PRESCORE_THRESHOLD", value=PRE_SCORE_THRESHOLD),
            i18n.t(lang, "DIAG_AI_CONFIG_PRESCORE_MIN", value=MIN_PRE_SCORE),
            i18n.t(lang, "DIAG_AI_CONFIG_FINAL_THRESHOLD", value=FINAL_SCORE_THRESHOLD),
            i18n.t(lang, "DIAG_AI_CONFIG_MIN_VOLUME", value=MIN_VOLUME_5M_USDT),
            i18n.t(lang, "DIAG_AI_CONFIG_PUMP_VOLUME", value=PUMP_VOLUME_MUL),
        ]
    )
    return _format_section(i18n.t(lang, "DIAG_SECTION_AI"), status_label, details, lang)


def _format_filters_section(st, lang: str) -> str:
    pre_score = (st.last_stats or {}).get("pre_score") if st else None
    status_label = _build_status_label(
        ok=bool(pre_score),
        warn=not pre_score,
        error=False,
        ok_text=i18n.t(lang, "DIAG_STATUS_WORKING"),
        warn_text=i18n.t(lang, "DIAG_STATUS_NO_DATA"),
        error_text=i18n.t(lang, "DIAG_STATUS_ERROR"),
    )
    details: list[str] = []
    if pre_score:
        threshold = pre_score.get("threshold")
        threshold_str = f"{threshold:.1f}" if isinstance(threshold, (int, float)) else "-"
        checked = pre_score.get("checked", 0)
        passed = pre_score.get("passed", 0)
        failed = pre_score.get("failed", 0)
        pass_rate = pre_score.get("pass_rate")
        pass_rate_str = None
        if isinstance(pass_rate, (int, float)):
            pass_rate_str = f"{int(round(pass_rate * 100))}%"
        failed_samples = pre_score.get("failed_samples") or []
        passed_samples = pre_score.get("passed_samples") or []
        details.append(i18n.t(lang, "DIAG_PRESCORE_THRESHOLD", threshold=threshold_str))
        summary = i18n.t(
            lang,
            "DIAG_PRESCORE_SUMMARY",
            checked=checked,
            passed=passed,
            failed=failed,
            rate=pass_rate_str or "-",
        )
        details.append(summary)
        if failed_samples:
            details.append(
                i18n.t(
                    lang,
                    "DIAG_PRESCORE_FAILED",
                    samples=_format_samples(failed_samples),
                )
            )
        if passed_samples:
            details.append(
                i18n.t(
                    lang,
                    "DIAG_PRESCORE_PASSED",
                    samples=_format_samples(passed_samples),
                )
            )
    if st and st.fails_top:
        if isinstance(st.fails_top, dict):
            details.append(_format_fails_top(st.fails_top, lang))
        else:
            details.append(str(st.fails_top))
    if st and st.near_miss:
        if isinstance(st.near_miss, dict):
            details.append(_format_near_miss(st.near_miss, lang))
        else:
            details.append(st.near_miss)
    if st and st.universe_debug:
        details.append(st.universe_debug)
    if not details:
        details.append(i18n.t(lang, "DIAG_NO_DATA_LINE"))
    return _format_section(i18n.t(lang, "DIAG_SECTION_FILTERS"), status_label, details, lang)


def _format_binance_section(st, now: float, lang: str) -> str:
    extra = _parse_extra_kv(st.extra or "") if st else {}
    last_ok = bool(st and st.binance_last_success_ts)
    has_timeouts = bool(st and st.binance_consecutive_timeouts)
    status_label = _build_status_label(
        ok=last_ok and not has_timeouts,
        warn=has_timeouts or not last_ok,
        error=False,
        ok_text=i18n.t(lang, "DIAG_STATUS_WORKING"),
        warn_text=i18n.t(lang, "DIAG_STATUS_ISSUES"),
        error_text=i18n.t(lang, "DIAG_STATUS_ERROR"),
    )
    details = []
    if st and st.binance_last_success_ts:
        details.append(
            i18n.t(
                lang,
                "DIAG_BINANCE_LAST_SUCCESS",
                ago=_human_ago(int(now - st.binance_last_success_ts), lang),
            )
        )
    else:
        details.append(i18n.t(lang, "DIAG_BINANCE_LAST_SUCCESS_NO_DATA"))
    details.append(
        i18n.t(lang, "DIAG_BINANCE_TIMEOUTS", count=st.binance_consecutive_timeouts if st else 0)
    )
    details.append(
        i18n.t(lang, "DIAG_BINANCE_STAGE", stage=st.binance_current_stage or "—" if st else "—")
    )
    req = extra.get("req")
    kl = extra.get("klines")
    hits = extra.get("klines_hits")
    misses = extra.get("klines_misses")
    inflight = extra.get("klines_inflight")
    ticker_req = extra.get("ticker_req")
    deep_scans = extra.get("deep_scans")
    if req:
        details.append(i18n.t(lang, "DIAG_REQUESTS_MADE", count=req))
    if kl:
        details.append(i18n.t(lang, "DIAG_CANDLES", count=kl))
    if hits or misses:
        details.append(i18n.t(lang, "DIAG_CACHE", hits=hits or 0, misses=misses or 0))
    if inflight:
        details.append(i18n.t(lang, "DIAG_INFLIGHT", count=inflight))
    if ticker_req:
        details.append(i18n.t(lang, "DIAG_TICKER_REQ", count=ticker_req))
    if deep_scans:
        details.append(i18n.t(lang, "DIAG_DEEP_SCAN", count=deep_scans))
    return _format_section(i18n.t(lang, "DIAG_SECTION_BINANCE"), status_label, details, lang)


def _format_pump_section(st, now: float, lang: str) -> str:
    extra = _parse_extra_kv(st.extra or "")
    status_label = _build_status_label(
        ok=bool(st.last_tick) and not st.last_error and not st.last_warn,
        warn=bool(st.last_warn) or not st.last_tick,
        error=bool(st.last_error),
        ok_text=i18n.t(lang, "DIAG_STATUS_WORKING"),
        warn_text=i18n.t(lang, "DIAG_STATUS_ISSUES"),
        error_text=i18n.t(lang, "DIAG_STATUS_ERROR"),
    )
    tick = (
        _human_ago(int(now - st.last_tick), lang)
        if st.last_tick
        else i18n.t(lang, "DIAG_STATUS_NOT_STARTED")
    )
    details = [i18n.t(lang, "DIAG_MODULE_LAST_CYCLE", tick=tick)]
    if st.last_error:
        details.append(i18n.t(lang, "DIAG_MODULE_ERROR", error=f"⚠️ {st.last_error}"))
    if st.last_warn:
        details.append(i18n.t(lang, "DIAG_MODULE_WARNING", warning=f"⚠️ {st.last_warn}"))
    prog = extra.get("progress")
    checked = extra.get("checked")
    found = extra.get("found")
    sent = extra.get("sent")
    if prog:
        details.append(i18n.t(lang, "DIAG_PROGRESS", progress=prog))
    if checked:
        details.append(i18n.t(lang, "DIAG_CHECKED", count=checked))
    if found is not None:
        details.append(i18n.t(lang, "DIAG_FOUND", count=found))
    if sent is not None:
        details.append(i18n.t(lang, "DIAG_SENT", count=sent))
    current = extra.get("current") or (st.current_symbol or None)
    if current:
        details.append(i18n.t(lang, "DIAG_CURRENT_COIN", symbol=current))
    excluded_symbols = _get_pump_excluded_symbols()
    details.append(
        f"• Excluded symbols: {_format_symbol_list(excluded_symbols)}"
    )
    details.append(f"• Cycle sleep: {max(0.0, PUMP_CYCLE_SLEEP_SEC):g}s")
    rate_limit_on = PUMP_RATE_LIMIT_ENABLED and PUMP_MAX_SYMBOLS_PER_SEC > 0
    details.append(f"• Rate limit: {'on' if rate_limit_on else 'off'}")
    details.append(f"• Max symbols/sec: {PUMP_MAX_SYMBOLS_PER_SEC:g}")
    cyc = extra.get("cycle")
    if cyc:
        details.append(i18n.t(lang, "DIAG_CYCLE_TIME", cycle=cyc))
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
        details.append(
            i18n.t(
                lang,
                "DIAG_ROTATION",
                flag=rotation_flag,
                n=n_line,
                cursor=cursor_line,
            )
        )
    if rotation_slice is not None:
        details.append(i18n.t(lang, "DIAG_ROTATION_SLICE", size=rotation_slice))
    if universe_size or rotation_added or final_candidates or scanned:
        details.append(
            i18n.t(
                lang,
                "DIAG_UNIVERSE_LINE",
                universe=universe_size or 0,
                added=rotation_added or 0,
                final=final_candidates or 0,
                scanned=scanned or 0,
            )
        )
    if st.fails_top:
        if isinstance(st.fails_top, dict):
            details.append(_format_fails_top(st.fails_top, lang))
        else:
            details.append(str(st.fails_top))
    if st.universe_debug:
        details.append(st.universe_debug)
    hits = extra.get("klines_hits")
    misses = extra.get("klines_misses")
    inflight = extra.get("klines_inflight")
    ticker_req = extra.get("ticker_req")
    req = extra.get("req")
    kl = extra.get("klines")
    if req:
        details.append(i18n.t(lang, "DIAG_REQUESTS_MADE", count=req))
    if kl:
        details.append(i18n.t(lang, "DIAG_CANDLES", count=kl))
    if hits or misses:
        details.append(i18n.t(lang, "DIAG_CACHE", hits=hits or 0, misses=misses or 0))
    if inflight:
        details.append(i18n.t(lang, "DIAG_INFLIGHT", count=inflight))
    if ticker_req:
        details.append(i18n.t(lang, "DIAG_TICKER_REQ", count=ticker_req))
    return _format_section(i18n.t(lang, "DIAG_SECTION_PUMPDUMP"), status_label, details, lang)


@dp.message(Command("testadmin"))
async def test_admin(message: Message):
    lang = get_user_lang(message.chat.id) or "ru"
    if message.from_user is None or not is_admin(message.from_user.id):
        await message.answer(i18n.t(lang, "NO_ACCESS"))
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
    blocks.append(i18n.t(lang, "DIAG_TITLE"))
    blocks.append(_format_overall_status(now, lang))
    blocks.append(_format_db_status(lang))
    ai_module = MODULES.get("ai_signals")
    if ai_module:
        blocks.append(_format_ai_section(ai_module, now, lang))
        blocks.append(_format_filters_section(ai_module, lang))
        blocks.append(_format_binance_section(ai_module, now, lang))
    pump_module = MODULES.get("pumpdump")
    if pump_module:
        blocks.append(_format_pump_section(pump_module, now, lang))

    lang = get_user_lang(message.chat.id) or "ru"
    await message.answer(
        "\n".join(blocks).strip(),
        reply_markup=build_admin_diagnostics_kb(lang),
    )


@dp.message(F.text.in_(i18n.all_labels("SYS_DIAG_ADMIN")))
async def test_admin_button(message: Message):
    lang = get_user_lang(message.chat.id) or "ru"
    if message.from_user is None or not is_admin(message.from_user.id):
        await message.answer(i18n.t(lang, "NO_ACCESS"))
        return
    await test_admin(message)


@dp.message(F.text.in_(i18n.all_labels("SYS_TEST_AI")))
async def test_ai_signal_all(message: Message):
    lang = get_user_lang(message.chat.id) or "ru"
    if message.from_user is None or not is_admin(message.from_user.id):
        await message.answer(i18n.t(lang, "NO_ACCESS"))
        return
    signal_dict = {
        "symbol": "TESTUSDT",
        "direction": "long",
        "entry_zone": (100.0, 101.0),
        "sl": 98.0,
        "tp1": 104.0,
        "tp2": 108.0,
        "score": 75,
        "is_test": True,
        "reason": {
            "trend_1d": "up",
            "trend_4h": "up",
            "rsi_1h": 55.0,
            "volume_ratio": 1.5,
            "rr": 2.0,
        },
        "title_prefix": {
            "ru": i18n.t("ru", "TEST_AI_PREFIX"),
            "en": i18n.t("en", "TEST_AI_PREFIX"),
        },
    }
    stats = await send_signal_to_all(
        signal_dict,
        allow_admin_bypass=False,
        bypass_cooldown=True,
        return_stats=True,
    )
    if stats["subscribers"] <= 0:
        await message.answer(i18n.t(lang, "TEST_NO_SUBSCRIBERS"))
        return
    await message.answer(
        i18n.t(
            lang,
            "TEST_AI_DONE",
            sent=stats["sent"],
            locked=stats["locked"],
            paywall=stats["paywall"],
            errors=stats["errors"],
            subscribers=stats["subscribers"],
        )
    )


@dp.message(F.text.in_(i18n.all_labels("SYS_TEST_PD")))
async def test_pumpdump_signal_all(message: Message):
    lang = get_user_lang(message.chat.id) or "ru"
    if message.from_user is None or not is_admin(message.from_user.id):
        await message.answer(i18n.t(lang, "NO_ACCESS"))
        return
    subscribers = get_pumpdump_subscribers()
    if not subscribers:
        await message.answer(i18n.t(lang, "TEST_NO_SUBSCRIBERS"))
        return
    signal_dict = {
        "symbol": "TESTUSDT",
        "price": 100.0,
        "change_1m": 1.2,
        "change_5m": 3.5,
        "volume_mul": 2.1,
        "type": "pump",
        "is_test": True,
    }
    stats = await _deliver_pumpdump_signal_stats(
        bot=message.bot,
        signal=signal_dict,
        symbol=signal_dict["symbol"],
        subscribers=subscribers,
        allow_admin_bypass=False,
        bypass_cooldown=True,
        bypass_limits=True,
        prefix_key="TEST_PD_PREFIX",
        suffix_key="TEST_PD_WARNING",
    )
    await message.answer(
        i18n.t(
            lang,
            "TEST_PD_DONE",
            sent=stats["sent"],
            locked=stats["locked"],
            paywall=stats["paywall"],
            errors=stats["errors"],
            subscribers=stats["subscribers"],
        )
    )


@dp.message(Command("test_notify"))
async def test_notify_cmd(message: Message):
    user_id = message.from_user.id if message.from_user else None
    print("[notify] /test_notify received", user_id, message.chat.id)
    lang = get_user_lang(message.chat.id) or "ru"
    if message.from_user is None or not is_admin(message.from_user.id):
        await message.answer(i18n.t(lang, "NO_ACCESS"))
        return
    target_chat_id = message.chat.id
    try:
        await message.bot.send_message(
            target_chat_id,
            i18n.t(lang, "TEST_NOTIFY_TEXT"),
        )
        print("[notify] test sent ok")
    except Exception as e:
        print(f"[notify] test failed: {e}")
        await message.answer(i18n.t(lang, "TEST_NOTIFY_ERROR", error=e))


@dp.message(Command("purge_tests"))
async def purge_tests_cmd(message: Message):
    lang = get_user_lang(message.chat.id) or "ru"
    if message.from_user is None or not is_admin(message.from_user.id):
        await message.answer(i18n.t(lang, "NO_ACCESS"))
        return
    removed = purge_test_signals()
    await message.answer(i18n.t(lang, "PURGE_TESTS_DONE", removed=removed))


@dp.message(Command("purge"))
async def purge_symbol_cmd(message: Message):
    lang = get_user_lang(message.chat.id) or "ru"
    if message.from_user is None or not is_admin(message.from_user.id):
        await message.answer(i18n.t(lang, "NO_ACCESS"))
        return
    symbol = _parse_symbol_arg(message.text)
    if not symbol:
        await message.answer(i18n.t(lang, "CMD_USAGE_PURGE"))
        return
    stats = purge_symbol(symbol)
    await message.answer(
        i18n.t(
            lang,
            "PURGE_SYMBOL_DONE",
            symbol=symbol,
            events=stats["events_deleted"],
            audit=stats["signal_audit_deleted"],
        )
    )


@dp.message(Command("my_id"))
async def my_id_cmd(message: Message):
    user_id = message.from_user.id if message.from_user else "unknown"
    await message.answer(f"user_id={user_id}\nchat_id={message.chat.id}")


def _format_user_bot_status(chat_id: int) -> str:
    """Понятный для пользователя статус (без тех. мусора)."""
    now = time.time()
    lang = get_user_lang(chat_id) or "ru"

    ai = MODULES.get("ai_signals")
    pd = MODULES.get("pumpdump")

    def _sec_ago(ts: float | int) -> int:
        return max(0, int(now - ts))

    def _extract_from_extra(extra: str, key: str) -> int | None:
        match = re.search(rf"{key}=(\\d+)", extra)
        return int(match.group(1)) if match else None

    def _extract_progress(extra: str) -> tuple[int, int] | None:
        match = re.search(r"progress=(\\d+)/(\\d+)", extra)
        if match:
            return int(match.group(1)), int(match.group(2))
        return None

    def _extract_cycle(extra: str) -> int | None:
        match = re.search(r"cycle=(\\d+)s", extra)
        return int(match.group(1)) if match else None

    def _format_last_ai_signal() -> str:
        row = get_last_signal_audit("ai_signals")
        if not row:
            return i18n.t(
                lang,
                "SYSTEM_STATUS_LAST_SIGNAL_LINE",
                text=i18n.t(lang, "SYSTEM_STATUS_LAST_SIGNAL_NONE"),
            )
        symbol = str(row.get("symbol", "-"))
        direction = str(row.get("direction", "")).upper()
        side = "LONG" if direction == "LONG" else "SHORT" if direction == "SHORT" else direction
        score_raw = row.get("score", 0)
        try:
            score = int(round(float(score_raw)))
        except (TypeError, ValueError):
            score = 0
        sent_at = int(row.get("sent_at", 0) or 0)
        stamp = _format_event_time(sent_at) if sent_at else "-"
        return i18n.t(
            lang,
            "SYSTEM_STATUS_LAST_SIGNAL_LINE",
            text=f"{symbol} {side} (Score {score}) — {stamp}",
        )

    def _format_last_pumpdump() -> str:
        payload = get_last_pumpdump_signal()
        if not payload:
            return i18n.t(
                lang,
                "SYSTEM_STATUS_LAST_SIGNAL_LINE",
                text=i18n.t(lang, "SYSTEM_STATUS_LAST_SIGNAL_NONE_PD"),
            )
        symbol = str(payload.get("symbol", "-"))
        direction = str(payload.get("direction", "")).upper()
        direction_text = "PUMP" if direction == "PUMP" else "DUMP" if direction == "DUMP" else direction
        change_raw = payload.get("change_5m")
        change_text = ""
        if change_raw is not None:
            try:
                change_val = float(change_raw)
                change_text = f" ({change_val:+.1f}%)"
            except (TypeError, ValueError):
                change_text = ""
        ts = int(payload.get("ts", 0) or 0)
        stamp = _format_event_time(ts) if ts else "-"
        if direction_text:
            line_text = f"{symbol} {direction_text}{change_text} — {stamp}"
        else:
            line_text = f"{symbol} — {stamp}"
        return i18n.t(lang, "SYSTEM_STATUS_LAST_SIGNAL_LINE", text=line_text)

    def _seconds_ago_label(seconds: int) -> str:
        return i18n.t(lang, "SYSTEM_STATUS_SECONDS_AGO", seconds=seconds)

    binance_ts = max(
        (st.binance_last_success_ts for st in MODULES.values() if st.binance_last_success_ts),
        default=0,
    )
    if binance_ts:
        binance_line = i18n.t(
            lang,
            "SYSTEM_STATUS_BINANCE_OK",
            seconds_ago=_seconds_ago_label(_sec_ago(binance_ts)),
        )
    else:
        binance_line = i18n.t(lang, "SYSTEM_STATUS_BINANCE_NO_DATA")

    ai_status_line = (
        i18n.t(lang, "SYSTEM_STATUS_AI_RUNNING_LINE")
        if ai and ai.last_tick
        else i18n.t(lang, "SYSTEM_STATUS_AI_STOPPED_LINE")
    )
    ai_last_cycle = (
        i18n.t(
            lang,
            "SYSTEM_STATUS_LAST_CYCLE_LINE",
            seconds_ago=_seconds_ago_label(_sec_ago(ai.last_tick)),
        )
        if ai and ai.last_tick
        else i18n.t(lang, "SYSTEM_STATUS_LAST_CYCLE_NO_DATA")
    )
    ai_cursor = ai.cursor if ai else 0
    ai_total = ai.total_symbols if ai else 0
    if (ai_cursor == 0 and ai_total == 0) and ai and ai.extra:
        extra_cursor = _extract_from_extra(ai.extra, "cursor")
        extra_total = _extract_from_extra(ai.extra, "universe")
        if extra_cursor is not None:
            ai_cursor = extra_cursor
        if extra_total is not None:
            ai_total = extra_total
    ai_scan_line = (
        i18n.t(lang, "SYSTEM_STATUS_SCAN_LINE", current=ai_cursor, total=ai_total)
        if ai_cursor or ai_total
        else i18n.t(lang, "SYSTEM_STATUS_SCAN_NO_DATA")
    )
    ai_current = ai.current_symbol if ai else None
    ai_current_line = (
        i18n.t(lang, "SYSTEM_STATUS_CURRENT_LINE", symbol=ai_current)
        if ai_current
        else i18n.t(lang, "SYSTEM_STATUS_CURRENT_NO_DATA")
    )
    ai_cycle = _extract_cycle(ai.extra) if ai and ai.extra else None
    ai_cycle_line = (
        i18n.t(lang, "SYSTEM_STATUS_CYCLE_LINE", seconds=ai_cycle)
        if ai_cycle
        else None
    )

    pd_status_line = (
        i18n.t(lang, "SYSTEM_STATUS_PD_RUNNING_LINE")
        if pd and pd.last_tick
        else i18n.t(lang, "SYSTEM_STATUS_PD_STOPPED_LINE")
    )
    pd_last_cycle = (
        i18n.t(
            lang,
            "SYSTEM_STATUS_LAST_CYCLE_LINE",
            seconds_ago=_seconds_ago_label(_sec_ago(pd.last_tick)),
        )
        if pd and pd.last_tick
        else i18n.t(lang, "SYSTEM_STATUS_LAST_CYCLE_NO_DATA")
    )
    pd_checked = pd.checked_last_cycle if pd else 0
    pd_total = pd.total_symbols if pd else 0
    if (pd_checked == 0 and pd_total == 0) and pd and pd.extra:
        progress = _extract_progress(pd.extra)
        if progress:
            pd_checked, pd_total = progress
    pd_progress_line = (
        i18n.t(lang, "SYSTEM_STATUS_PROGRESS_LINE", current=pd_checked, total=pd_total)
        if pd_checked or pd_total
        else i18n.t(lang, "SYSTEM_STATUS_PROGRESS_NO_DATA")
    )
    pd_current = pd.current_symbol if pd else None
    pd_current_line = (
        i18n.t(lang, "SYSTEM_STATUS_CURRENT_LINE", symbol=pd_current)
        if pd_current
        else i18n.t(lang, "SYSTEM_STATUS_CURRENT_NO_DATA")
    )

    ai_cycle_line = f"{ai_cycle_line}\n" if ai_cycle_line else ""
    return i18n.t(
        lang,
        "SYSTEM_STATUS_TEXT",
        binance_line=binance_line,
        ai_status_line=ai_status_line,
        ai_last_cycle=ai_last_cycle,
        ai_scan_line=ai_scan_line,
        ai_current_line=ai_current_line,
        ai_cycle_line=ai_cycle_line,
        ai_last_signal=_format_last_ai_signal(),
        pd_status_line=pd_status_line,
        pd_last_cycle=pd_last_cycle,
        pd_progress_line=pd_progress_line,
        pd_current_line=pd_current_line,
        pd_last_signal=_format_last_pumpdump(),
    )


@dp.message(Command("status"))
async def status_cmd(message: Message):
    await message.answer(_format_user_bot_status(message.chat.id))


@dp.message(F.text.in_(i18n.all_labels("MENU_SYSTEM")))
async def system_menu(message: Message):
    await show_system_menu(message)


async def show_system_menu(message: Message) -> None:
    lang = get_user_lang(message.chat.id) or "ru"
    await message.answer(
        i18n.t(lang, "SYSTEM_SECTION_TEXT"),
        reply_markup=build_system_menu_kb(
            lang,
            is_admin=is_admin(message.from_user.id) if message.from_user else False
        ),
    )


@dp.callback_query(F.data == "about_back")
async def about_back_callback(callback: CallbackQuery):
    await callback.answer()
    if callback.message:
        lang = get_user_lang(callback.from_user.id) if callback.from_user else None
        lang = lang or "ru"
        await callback.message.answer(
            i18n.t(lang, "SYSTEM_SECTION_TEXT"),
            reply_markup=build_system_menu_kb(
                lang,
                is_admin=is_admin(callback.from_user.id) if callback.from_user else False
            ),
        )


@dp.callback_query(F.data == "system_back")
async def system_back_callback(callback: CallbackQuery):
    await callback.answer()
    if callback.message:
        lang = get_user_lang(callback.from_user.id) if callback.from_user else None
        lang = lang or "ru"
        await callback.message.answer(
            i18n.t(lang, "SYSTEM_SECTION_TEXT"),
            reply_markup=build_system_menu_kb(
                lang,
                is_admin=is_admin(callback.from_user.id) if callback.from_user else False
            ),
        )


@dp.callback_query(F.data == "sub_contact")
async def subscription_contact_callback(callback: CallbackQuery):
    user_id = callback.from_user.id if callback.from_user else 0
    lang = get_user_lang(user_id) or "ru"
    text = i18n.t(
        lang,
        "CONTACT_ADMIN_BLOCK",
        admin_contact=ADMIN_CONTACT,
        user_id=user_id,
    )
    await callback.answer()
    if callback.message:
        await callback.message.answer(text)


@dp.message(F.text.in_(i18n.all_labels("SYS_PAY")))
async def subscription_offer_message(message: Message):
    if message.from_user is None:
        return
    await show_subscribe_offer(message, message.from_user.id, source="system")


@dp.callback_query(F.data == "sub_pay")
async def subscription_pay_callback(callback: CallbackQuery):
    await callback.answer()
    if callback.from_user is None:
        return
    if callback.message:
        await show_subscribe_offer(
            callback.message,
            callback.from_user.id,
            source="system",
            edit=True,
        )


@dp.callback_query(F.data == "sub_accept")
async def subscription_accept_callback(callback: CallbackQuery):
    user_id = callback.from_user.id if callback.from_user else 0
    lang = get_user_lang(user_id) or "ru"
    payment_text = i18n.t(lang, "PAYMENT_TEXT_TRX", wallet=PAY_WALLET_TRX, user_id=user_id)
    await callback.answer()
    if callback.message:
        await callback.message.edit_text(
            payment_text,
            reply_markup=build_payment_inline_kb(lang),
        )


@dp.callback_query(F.data == "sub_pay_back")
async def subscription_pay_back_callback(callback: CallbackQuery):
    await callback.answer()
    if callback.from_user is None:
        return
    source = _subscribe_source_from_code(
        get_user_pref(callback.from_user.id, "last_sub_source", SUB_SOURCE_SYSTEM)
    )
    if callback.message:
        lang = get_user_lang(callback.from_user.id) or "ru"
        await callback.message.edit_text(
            i18n.t(lang, "OFFER_TEXT"),
            reply_markup=build_offer_inline_kb(
                lang,
                back_callback=_subscribe_back_callback(source)
            ),
        )


@dp.callback_query(F.data.startswith("sub_back:"))
async def subscription_back_callback(callback: CallbackQuery):
    await callback.answer()
    if callback.message:
        with suppress(Exception):
            await callback.message.delete()


@dp.callback_query(F.data == "sub_copy_address")
async def subscription_copy_address_callback(callback: CallbackQuery):
    if callback.from_user is None:
        return
    await callback.answer()
    if callback.message:
        await callback.message.bot.send_message(
            callback.from_user.id,
            i18n.t(
                get_user_lang(callback.from_user.id) or "ru",
                "PAYMENT_COPY_ADDRESS",
                wallet=PAY_WALLET_TRX,
            ),
        )


@dp.callback_query(F.data == "sub_send_receipt")
async def subscription_send_receipt_callback(callback: CallbackQuery):
    if callback.from_user is None:
        return
    set_user_pref(callback.from_user.id, PREF_AWAITING_RECEIPT, 1)
    await callback.answer()
    if callback.message:
        await callback.message.answer(
            i18n.t(get_user_lang(callback.from_user.id) or "ru", "RECEIPT_REQUEST_TEXT")
        )


def _format_user_time(ts: int | None) -> str:
    if not ts:
        return "-"
    dt = datetime.fromtimestamp(int(ts), tz=ALMATY_TZ)
    return dt.strftime("%Y-%m-%d %H:%M")


def _load_users(limit: int = 50) -> list[sqlite3.Row]:
    conn = sqlite3.connect(get_db_path())
    conn.row_factory = sqlite3.Row
    try:
        cur = conn.execute(
            """
            SELECT chat_id, username, started_at, last_seen
            FROM users
            ORDER BY started_at DESC
            LIMIT ?
            """,
            (limit,),
        )
        return cur.fetchall()
    finally:
        conn.close()


def _build_users_list_markup(rows: list[sqlite3.Row], lang: str) -> InlineKeyboardMarkup:
    buttons: list[list[InlineKeyboardButton]] = []
    for row in rows:
        chat_id = int(row["chat_id"])
        username = row["username"]
        username_text = f"@{username}" if username else "-"
        status_icon = "🔴" if is_user_locked(chat_id) else "🟢"
        buttons.append(
            [
                InlineKeyboardButton(
                    text=f"{status_icon} {chat_id} ({username_text})",
                    callback_data=f"user_view:{chat_id}",
                )
            ]
        )
    buttons.append(
        [
            InlineKeyboardButton(
                text=i18n.t(lang, "NAV_BACK"),
                callback_data="admin_back",
            )
        ]
    )
    return InlineKeyboardMarkup(inline_keyboard=buttons)


def _users_list_payload(
    lang: str,
    prefix: str | None = None,
) -> tuple[str, InlineKeyboardMarkup | None]:
    rows = _load_users()
    if not rows:
        markup = InlineKeyboardMarkup(
            inline_keyboard=[
                [
                    InlineKeyboardButton(
                        text=i18n.t(lang, "NAV_BACK"),
                        callback_data="admin_back",
                    )
                ]
            ]
        )
        return (i18n.t(lang, "USER_LIST_EMPTY"), markup)
    header = i18n.t(lang, "USER_LIST_HEADER")
    text = f"{prefix}\n\n{header}" if prefix else header
    return text, _build_users_list_markup(rows, lang)


def _load_user_row(user_id: int) -> sqlite3.Row | None:
    conn = sqlite3.connect(get_db_path())
    conn.row_factory = sqlite3.Row
    try:
        cur = conn.execute(
            """
            SELECT chat_id, username, started_at, last_seen
            FROM users
            WHERE chat_id = ?
            """,
            (user_id,),
        )
        return cur.fetchone()
    finally:
        conn.close()


def _build_user_card(user_id: int, lang: str) -> tuple[str, InlineKeyboardMarkup]:
    ensure_trial_defaults(user_id)
    row = _load_user_row(user_id)
    username_text = "-"
    started_text = "-"
    last_seen_text = "-"
    if row is not None:
        username = row["username"]
        username_text = f"@{username}" if username else "-"
        started_text = _format_user_time(row["started_at"])
        last_seen_text = _format_user_time(row["last_seen"])

    locked = is_user_locked(user_id)
    status_icon = "🔴" if locked else "🟢"
    trial_ai_left = get_user_pref(user_id, "trial_ai_left", TRIAL_AI_LIMIT)
    trial_pump_left = get_user_pref(user_id, "trial_pump_left", TRIAL_PUMP_LIMIT)
    sub_until = get_user_pref(user_id, "sub_until", 0)
    subscription_text = (
        i18n.t(lang, "USER_CARD_ACTIVE_UNTIL", date=_format_user_time(sub_until))
        if is_sub_active(user_id)
        else i18n.t(lang, "USER_CARD_SUB_NONE")
    )

    lines = [
        i18n.t(lang, "USER_CARD_TITLE"),
        "",
        f"ID: {user_id}",
        f"Username: {username_text}",
        i18n.t(lang, "USER_CARD_STATUS", status=status_icon),
        i18n.t(lang, "USER_CARD_SUBSCRIPTION", subscription=subscription_text),
        i18n.t(lang, "USER_CARD_AI_LEFT", left=trial_ai_left, limit=TRIAL_AI_LIMIT),
        i18n.t(lang, "USER_CARD_PD_LEFT", left=trial_pump_left, limit=TRIAL_PUMP_LIMIT),
        i18n.t(lang, "USER_CARD_STARTED_AT", date=started_text),
        i18n.t(lang, "USER_CARD_LAST_SEEN", date=last_seen_text),
    ]

    if locked:
        lock_button = InlineKeyboardButton(
            text=i18n.t(lang, "USER_BTN_UNLOCK"),
            callback_data=f"user_unlock:{user_id}",
        )
    else:
        lock_button = InlineKeyboardButton(
            text=i18n.t(lang, "USER_BTN_LOCK"),
            callback_data=f"user_lock:{user_id}",
        )

    markup = InlineKeyboardMarkup(
        inline_keyboard=[
            [lock_button],
            [
                InlineKeyboardButton(
                    text=i18n.t(lang, "USER_BTN_DELETE"),
                    callback_data=f"user_del_confirm:{user_id}",
                )
            ],
            [
                InlineKeyboardButton(
                    text=i18n.t(lang, "NAV_BACK"),
                    callback_data="users_list",
                )
            ],
        ]
    )
    return "\n".join(lines), markup


async def _ensure_admin_callback(callback: CallbackQuery) -> bool:
    if callback.from_user is None or not is_admin(callback.from_user.id):
        lang = get_user_lang(callback.from_user.id) if callback.from_user else None
        await callback.answer(i18n.t(lang or "ru", "NO_ACCESS"))
        return False
    return True


@dp.message(F.text.in_(i18n.all_labels("SYS_USERS")))
async def users_list(message: Message):
    if message.from_user is None or not is_admin(message.from_user.id):
        return
    lang = get_user_lang(message.from_user.id) or "ru"
    text, markup = _users_list_payload(lang)
    await message.answer(text, reply_markup=markup)


@dp.callback_query(F.data == "users_list")
async def users_list_callback(callback: CallbackQuery):
    if not await _ensure_admin_callback(callback):
        return
    lang = get_user_lang(callback.from_user.id) if callback.from_user else None
    text, markup = _users_list_payload(lang or "ru")
    await callback.answer()
    if callback.message:
        await callback.message.edit_text(text, reply_markup=markup)


@dp.callback_query(F.data == "admin_back")
async def admin_back_callback(callback: CallbackQuery):
    if not await _ensure_admin_callback(callback):
        return
    await callback.answer()
    if callback.message:
        lang = get_user_lang(callback.from_user.id) if callback.from_user else None
        await callback.message.edit_text(i18n.t(lang or "ru", "BACK_TO_MAIN_TEXT"))


@dp.callback_query(F.data.regexp(r"^user_view:\d+$"))
async def user_view_callback(callback: CallbackQuery):
    if not await _ensure_admin_callback(callback):
        return
    if callback.message is None:
        return
    user_id = int(callback.data.split(":", 1)[1])
    lang = get_user_lang(callback.from_user.id) if callback.from_user else None
    text, markup = _build_user_card(user_id, lang or "ru")
    await callback.answer()
    await callback.message.edit_text(text, reply_markup=markup)


@dp.callback_query(F.data.regexp(r"^user_lock:\d+$"))
async def user_lock_callback(callback: CallbackQuery):
    if not await _ensure_admin_callback(callback):
        return
    if callback.message is None:
        return
    user_id = int(callback.data.split(":", 1)[1])
    set_user_pref(user_id, "user_locked", 1)
    lang = get_user_lang(callback.from_user.id) if callback.from_user else None
    text, markup = _build_user_card(user_id, lang or "ru")
    await callback.answer()
    await callback.message.edit_text(text, reply_markup=markup)
    try:
        await callback.message.bot.send_message(
            user_id,
            i18n.t(
                get_user_lang(user_id) or "ru",
                "USER_LOCKED_NOTICE",
                admin_contact=ADMIN_CONTACT,
                user_id=user_id,
            ),
        )
    except Exception as exc:
        _log_throttled(
            "tg_send_fail_user_lock",
            "[tg_send_fail] user_id=%s action=user_lock_notice err=%s",
            user_id,
            exc,
            exc=exc,
        )


@dp.callback_query(F.data.regexp(r"^user_unlock:\d+$"))
async def user_unlock_callback(callback: CallbackQuery):
    if not await _ensure_admin_callback(callback):
        return
    if callback.message is None:
        return
    user_id = int(callback.data.split(":", 1)[1])
    set_user_pref(user_id, "user_locked", 0)
    now = int(time.time())
    old_sub_until = get_user_pref(user_id, "sub_until", 0)
    new_sub_until = max(old_sub_until, now) + 30 * 24 * 3600
    set_user_pref(user_id, "sub_until", new_sub_until)
    lang = get_user_lang(callback.from_user.id) if callback.from_user else None
    text, markup = _build_user_card(user_id, lang or "ru")
    await callback.answer()
    await callback.message.edit_text(text, reply_markup=markup)
    try:
        await callback.message.bot.send_message(
            user_id,
            i18n.t(get_user_lang(user_id) or "ru", "USER_UNLOCKED_NOTICE"),
        )
    except Exception as exc:
        _log_throttled(
            "tg_send_fail_user_unlock",
            "[tg_send_fail] user_id=%s action=user_unlock_notice err=%s",
            user_id,
            exc,
            exc=exc,
        )


@dp.callback_query(F.data.regexp(r"^user_del_confirm:\d+$"))
async def user_delete_confirm_callback(callback: CallbackQuery):
    if not await _ensure_admin_callback(callback):
        return
    if callback.message is None:
        return
    user_id = int(callback.data.split(":", 1)[1])
    lang = get_user_lang(callback.from_user.id) if callback.from_user else None
    lang = lang or "ru"
    text = i18n.t(lang, "USER_DELETE_CONFIRM", user_id=user_id)
    markup = InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(
                    text=i18n.t(lang, "USER_DELETE_CONFIRM_YES"),
                    callback_data=f"user_delete:{user_id}",
                )
            ],
            [
                InlineKeyboardButton(
                    text=i18n.t(lang, "USER_DELETE_CONFIRM_NO"),
                    callback_data=f"user_view:{user_id}",
                )
            ],
        ]
    )
    await callback.answer()
    await callback.message.edit_text(text, reply_markup=markup)


@dp.callback_query(F.data.regexp(r"^user_delete:\d+$"))
async def user_delete_callback(callback: CallbackQuery):
    if not await _ensure_admin_callback(callback):
        return
    if callback.message is None:
        return
    user_id = int(callback.data.split(":", 1)[1])
    delete_user(user_id)
    try:
        await callback.message.bot.send_message(
            user_id,
            i18n.t(get_user_lang(user_id) or "ru", "USER_DELETED_NOTICE"),
        )
    except Exception as exc:
        _log_throttled(
            "tg_send_fail_user_delete",
            "[tg_send_fail] user_id=%s action=user_deleted_notice err=%s",
            user_id,
            exc,
            exc=exc,
        )
    lang = get_user_lang(callback.from_user.id) if callback.from_user else None
    lang = lang or "ru"
    text, markup = _users_list_payload(
        lang,
        prefix=i18n.t(lang, "USER_DELETED_PREFIX", user_id=user_id),
    )
    await callback.answer(i18n.t(lang, "USER_DELETED_ALERT", user_id=user_id))
    await callback.message.edit_text(text, reply_markup=markup)


@dp.message(F.text.in_(i18n.all_labels("SYS_STATUS")))
async def status_button(message: Message):
    await message.answer(
        _format_user_bot_status(message.chat.id),
        reply_markup=build_system_menu_kb(
            get_user_lang(message.chat.id) or "ru",
            is_admin=is_admin(message.from_user.id) if message.from_user else False
        ),
    )


@dp.message(F.text.in_(i18n.all_labels("SYS_DIAG")))
async def diagnostics_button(message: Message):
    await message.answer(
        _format_user_bot_status(message.chat.id),
        reply_markup=build_system_menu_kb(
            get_user_lang(message.chat.id) or "ru",
            is_admin=is_admin(message.from_user.id) if message.from_user else False
        ),
    )


@dp.message(F.text.in_(i18n.all_labels("MENU_BACK")))
async def back_to_main(message: Message):
    lang = get_user_lang(message.chat.id) or "ru"
    await message.answer(
        i18n.t(lang, "BACK_TO_MAIN_TEXT"),
        reply_markup=build_main_menu_kb(
            lang,
            is_admin=is_admin(message.from_user.id) if message.from_user else False,
        ),
    )


@dp.message(
    lambda message: message.from_user is not None
    and get_user_pref(message.from_user.id, PREF_AWAITING_RECEIPT, 0) == 1
)
async def receipt_message_handler(message: Message):
    user = message.from_user
    if user is None:
        return
    set_user_pref(user.id, PREF_AWAITING_RECEIPT, 0)
    username_text = f"@{user.username}" if user.username else "-"
    timestamp = datetime.now(ALMATY_TZ).strftime("%Y-%m-%d %H:%M")
    admin_lang = get_user_lang(ADMIN_CHAT_ID or ADMIN_USER_ID) or "ru"
    admin_text = i18n.t(
        admin_lang,
        "ADMIN_RECEIPT_TEXT",
        user_id=user.id,
        username=username_text,
        timestamp=timestamp,
        price=SUB_PRICE_USD,
        days=SUB_DAYS,
        wallet=PAY_WALLET_TRX,
    )
    admin_chat_id = ADMIN_CHAT_ID or ADMIN_USER_ID
    if admin_chat_id != 0:
        try:
            await message.bot.send_message(admin_chat_id, admin_text)
        except Exception as exc:
            _log_throttled(
                "tg_send_fail_receipt",
                "[tg_send_fail] user_id=%s action=receipt_admin_text err=%s",
                user.id,
                exc,
                exc=exc,
            )
        try:
            await message.copy_to(admin_chat_id)
        except Exception as exc:
            _log_throttled(
                "tg_send_fail_receipt_copy",
                "[tg_send_fail] user_id=%s action=receipt_copy err=%s",
                user.id,
                exc,
                exc=exc,
            )
    await message.answer(i18n.t(get_user_lang(user.id) or "ru", "RECEIPT_SENT_CONFIRM"))


def _format_stats_message(stats: Dict[str, Any], lang: str) -> str:
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
        i18n.t(lang, "ADMIN_STATS_TITLE"),
        "",
        i18n.t(lang, "ADMIN_STATS_TOTAL", total=total),
        i18n.t(lang, "ADMIN_STATS_CLOSED", closed=closed),
        i18n.t(
            lang,
            "ADMIN_STATS_FILLED_RATE",
            rate=filled_rate,
            filled=filled_closed,
            total=total,
        ),
        i18n.t(lang, "ADMIN_STATS_WINRATE", winrate=winrate),
        i18n.t(lang, "ADMIN_STATS_PROFIT_FACTOR", profit_factor=pf_text),
        i18n.t(lang, "ADMIN_STATS_AVG_R", avg_r=avg_r),
        i18n.t(lang, "ADMIN_STATS_MEDIAN_R", median_r=median_r),
        i18n.t(lang, "ADMIN_STATS_STREAK", streak=streak),
        "",
        i18n.t(lang, "ADMIN_STATS_LAST10"),
    ]

    if not last10:
        lines.append(i18n.t(lang, "ADMIN_STATS_NO_DATA"))
    else:
        for row in last10:
            symbol = row.get("symbol", "-")
            direction = row.get("direction", "-")
            outcome = row.get("outcome", "-")
            pnl_r = row.get("pnl_r")
            pnl_text = f"{pnl_r:+.2f}R" if isinstance(pnl_r, (int, float)) else "-"
            lines.append(
                i18n.t(
                    lang,
                    "ADMIN_STATS_ROW",
                    symbol=symbol,
                    direction=direction.upper(),
                    outcome=outcome,
                    pnl=pnl_text,
                )
            )

    return "\n".join(lines)


@dp.message(F.text == "/stats")
async def show_stats(message: Message):
    if message.from_user is None or not is_admin(message.from_user.id):
        return
    stats = get_public_stats(days=30)
    lang = get_user_lang(message.chat.id) or "ru"
    await message.answer(
        _format_stats_message(stats, lang),
        reply_markup=build_main_menu_kb(lang, is_admin=True),
    )


@dp.message(Command("lock"))
async def lock_user_cmd(message: Message):
    if message.from_user is None or not is_admin(message.from_user.id):
        return
    user_id = _parse_user_id_arg(message.text)
    if user_id is None:
        await message.answer(i18n.t(get_user_lang(message.chat.id) or "ru", "CMD_USAGE_LOCK"))
        return
    set_user_pref(user_id, "user_locked", 1)
    await message.answer(
        i18n.t(get_user_lang(message.chat.id) or "ru", "CMD_LOCK_OK", user_id=user_id)
    )


@dp.message(Command("unlock"))
async def unlock_user_cmd(message: Message):
    if message.from_user is None or not is_admin(message.from_user.id):
        return
    user_id = _parse_user_id_arg(message.text)
    if user_id is None:
        await message.answer(
            i18n.t(get_user_lang(message.chat.id) or "ru", "CMD_USAGE_UNLOCK")
        )
        return
    set_user_pref(user_id, "user_locked", 0)
    now = int(time.time())
    old_sub_until = get_user_pref(user_id, "sub_until", 0)
    new_sub_until = max(old_sub_until, now) + 30 * 24 * 3600
    set_user_pref(user_id, "sub_until", new_sub_until)
    await message.answer(
        i18n.t(get_user_lang(message.chat.id) or "ru", "CMD_UNLOCK_OK", user_id=user_id)
    )


@dp.message(Command("delete"))
async def delete_user_cmd(message: Message):
    if message.from_user is None or not is_admin(message.from_user.id):
        return
    user_id = _parse_user_id_arg(message.text)
    if user_id is None:
        await message.answer(
            i18n.t(get_user_lang(message.chat.id) or "ru", "CMD_USAGE_DELETE")
        )
        return
    delete_user(user_id)
    await message.answer(
        i18n.t(get_user_lang(message.chat.id) or "ru", "CMD_DELETE_OK", user_id=user_id)
    )


def _trend_short_text(trend: str, lang: str) -> str:
    if trend in ("bullish", "up"):
        return i18n.t(lang, "SCENARIO_TREND_BULLISH")
    if trend in ("bearish", "down"):
        return i18n.t(lang, "SCENARIO_TREND_BEARISH")
    return i18n.t(lang, "SCENARIO_TREND_NEUTRAL")


def _rsi_short_zone(rsi: float, lang: str) -> str:
    if 40 <= rsi <= 60:
        return i18n.t(lang, "SCENARIO_RSI_COMFORT")
    if rsi < 40:
        return i18n.t(lang, "SCENARIO_RSI_OVERSOLD_ZONE")
    return i18n.t(lang, "SCENARIO_RSI_OVERBOUGHT_ZONE")


def _format_signed_number(value: float, decimals: int = 1) -> str:
    sign = "−" if value < 0 else "+"
    return f"{sign}{abs(value):.{decimals}f}"

def _format_signal(signal: Dict[str, Any], lang: str) -> str:
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

    text = format_scenario_message(
        lang=lang,
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
        lifetime_hours=SIGNAL_TTL_SECONDS // 3600,
    )
    prefix = signal.get("title_prefix")
    if isinstance(prefix, dict):
        prefix = prefix.get(lang) or prefix.get("ru")
    if prefix:
        return f"{prefix}{text}"
    return text


def _subscription_kb_for(source: str, lang: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(
                    text=i18n.t(lang, "BTN_BUY_SUB"),
                    callback_data=f"sub_paywall:{source}",
                )
            ]
        ]
    )


def _subscribe_source_code(source: str) -> int:
    if source == "ai":
        return SUB_SOURCE_AI
    if source == "pump":
        return SUB_SOURCE_PUMP
    return SUB_SOURCE_SYSTEM


def _subscribe_source_from_code(code: int) -> str:
    if code == SUB_SOURCE_AI:
        return "ai"
    if code == SUB_SOURCE_PUMP:
        return "pump"
    return "system"


def _subscribe_back_callback(source: str) -> str:
    if source in {"ai", "pump"}:
        return f"sub_back:{source}"
    return "system_back"


def _should_send_paywall(user_id: int, source: str, now_ts: int | None = None) -> bool:
    now_ts = now_ts or int(time.time())
    key = f"last_paywall_ts_{source}"
    last_ts = get_user_pref(user_id, key, 0)
    if last_ts and now_ts - last_ts < PAYWALL_COOLDOWN_SEC:
        return False
    set_user_pref(user_id, key, now_ts)
    return True


async def show_subscribe_offer(
    message: Message,
    user_id: int,
    *,
    source: str,
    edit: bool = False,
) -> None:
    set_user_pref(user_id, "last_sub_source", _subscribe_source_code(source))
    lang = get_user_lang(user_id) or "ru"
    reply_markup = build_offer_inline_kb(lang, back_callback=_subscribe_back_callback(source))
    if edit:
        await message.edit_text(i18n.t(lang, "OFFER_TEXT"), reply_markup=reply_markup)
        return
    await message.answer(i18n.t(lang, "OFFER_TEXT"), reply_markup=reply_markup)


async def send_signal_to_all(
    signal_dict: Dict[str, Any],
    *,
    allow_admin_bypass: bool = True,
    bypass_cooldown: bool = False,
    return_stats: bool = False,
) -> int | dict[str, int]:
    """Отправляет сигнал всем подписчикам без блокировки event loop."""
    if bot is None:
        print("[ai_signals] Bot is not initialized; skipping send.")
        return 0

    symbol = signal_dict.get("symbol", "")
    blocked_symbols = get_blocked_symbols()
    if symbol and symbol.upper() in blocked_symbols:
        print(f"[blocklist] skip AI signal send {symbol}")
        if return_stats:
            return {
                "sent": 0,
                "locked": 0,
                "paywall": 0,
                "errors": 0,
                "subscribers": 0,
            }
        return 0

    skipped_dedup = 0
    skipped_no_subs = 0
    subscribers = list(list_ai_subscribers())
    meta = signal_dict.get("meta") if isinstance(signal_dict, dict) else {}
    is_test = bool(signal_dict.get("is_test") or (isinstance(meta, dict) and meta.get("test")))
    effective_bypass_cooldown = bypass_cooldown or is_test
    stats = {
        "sent": 0,
        "locked": 0,
        "paywall": 0,
        "errors": 0,
        "subscribers": len(subscribers),
    }
    if not subscribers:
        skipped_no_subs += 1
        print("[ai_signals] deliver: subs=0 queued=0 dedup=0")
        return stats if return_stats else 0

    dedup_key = f"{symbol}:24h"

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

    tasks: list[asyncio.Task] = []
    recipients: list[tuple[int, bool, str]] = []
    for chat_id in subscribers:
        if is_user_locked(chat_id):
            stats["locked"] += 1
            continue
        # индивидуальный cooldown на пользователя
        if not effective_bypass_cooldown:
            if not can_send(chat_id, "ai_signals", dedup_key, COOLDOWN_FREE_SEC):
                skipped_dedup += 1
                continue
        lang = get_user_lang(chat_id) or "ru"
        message_text = _format_signal(signal_dict, lang)
        if allow_admin_bypass and is_admin(chat_id):
            tasks.append(asyncio.create_task(bot.send_message(chat_id, message_text)))
            recipients.append((chat_id, True, "signal"))
            continue
        if is_sub_active(chat_id):
            tasks.append(asyncio.create_task(bot.send_message(chat_id, message_text)))
            recipients.append((chat_id, True, "signal"))
            continue
        ensure_trial_defaults(chat_id)
        allowed, left = try_consume_trial(chat_id, "trial_ai_left", 1)
        if allowed:
            message_text = message_text + i18n.t(
                lang,
                "TRIAL_SUFFIX_AI",
                left=left,
                limit=TRIAL_AI_LIMIT,
            )
            tasks.append(asyncio.create_task(bot.send_message(chat_id, message_text)))
            recipients.append((chat_id, True, "signal"))
        else:
            if not _should_send_paywall(chat_id, "ai", sent_at):
                continue
            tasks.append(
                asyncio.create_task(
                    bot.send_message(
                        chat_id,
                        i18n.t(lang, "PAYWALL_AI"),
                        reply_markup=_subscription_kb_for("ai", lang),
                    )
                )
            )
            recipients.append((chat_id, False, "paywall"))

    print(
        "[ai_signals] deliver: "
        f"subs={len(subscribers)} queued={len(tasks)} "
        f"dedup={skipped_dedup}"
    )
    if not tasks:
        return stats if return_stats else 0

    results = await asyncio.gather(*tasks, return_exceptions=True)
    for (chat_id, should_log, kind), res in zip(recipients, results):
        if isinstance(res, asyncio.CancelledError):
            raise res
        if isinstance(res, BaseException):
            print(f"[ai_signals] Failed to send to {chat_id}: {res}")
            stats["errors"] += 1
            continue
        stats["sent"] += 1
        if kind == "paywall":
            stats["paywall"] += 1
        if not should_log:
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
                is_test=is_test,
                reason_json=reason_json,
                breakdown_json=breakdown_json,
            )
        except Exception as exc:
            print(f"[ai_signals] Failed to log signal event for {chat_id}: {exc}")
    return stats if return_stats else stats["sent"]


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
    excluded = _get_ai_excluded_symbols()
    if excluded:
        filtered = [symbol for symbol in filtered if symbol.upper() not in excluded]
    if removed:
        print(
            f"[ai_signals] universe filtered: total={len(symbols)} removed={removed} final={len(filtered)}"
        )
    return filtered


async def _deliver_pumpdump_signal_stats(
    *,
    bot: Bot,
    signal: Dict[str, Any],
    symbol: str,
    subscribers: list[int],
    allow_admin_bypass: bool = True,
    bypass_cooldown: bool = False,
    bypass_limits: bool = False,
    prefix_key: str | None = None,
    suffix_key: str | None = None,
) -> dict[str, int]:
    blocked_symbols = get_blocked_symbols()
    if symbol and symbol.upper() in blocked_symbols:
        print(f"[blocklist] skip pump/dump send {symbol}")
        return {
            "sent": 0,
            "locked": 0,
            "paywall": 0,
            "errors": 0,
            "subscribers": len(subscribers),
            "recipient_count": 0,
        }
    sent_count = 0
    recipient_count = 0
    locked_count = 0
    paywall_count = 0
    error_count = 0
    date_key = _get_pumpdump_date_key()

    for chat_id in subscribers:
        try:
            lang = get_user_lang(chat_id) or "ru"
            message_text = format_pump_message(signal, lang)
            if prefix_key:
                message_text = f"{i18n.t(lang, prefix_key)}{message_text}"
            if suffix_key:
                message_text = f"{message_text}\n\n{i18n.t(lang, suffix_key)}"
            is_admin_user = is_admin(chat_id) if allow_admin_bypass else False
            if not is_admin_user:
                if not bypass_limits:
                    if get_pumpdump_daily_count(chat_id, date_key) >= PUMP_DAILY_LIMIT:
                        continue
                if is_user_locked(chat_id):
                    locked_count += 1
                    continue
            if not bypass_cooldown:
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
            if is_admin_user:
                await bot.send_message(chat_id, message_text, parse_mode="Markdown")
                increment_pumpdump_daily_count(chat_id, date_key)
                sent_count += 1
                recipient_count += 1
                continue
            if is_sub_active(chat_id):
                await bot.send_message(chat_id, message_text, parse_mode="Markdown")
                increment_pumpdump_daily_count(chat_id, date_key)
                sent_count += 1
                recipient_count += 1
                continue
            ensure_trial_defaults(chat_id)
            allowed, left = try_consume_trial(chat_id, "trial_pump_left", 1)
            if allowed:
                message_text = message_text + i18n.t(
                    lang,
                    "TRIAL_SUFFIX_PD",
                    left=left,
                    limit=TRIAL_PUMP_LIMIT,
                )
                await bot.send_message(chat_id, message_text, parse_mode="Markdown")
                increment_pumpdump_daily_count(chat_id, date_key)
                sent_count += 1
                recipient_count += 1
            else:
                if not _should_send_paywall(chat_id, "pump"):
                    continue
                await bot.send_message(
                    chat_id,
                    i18n.t(lang, "PAYWALL_PD"),
                    reply_markup=_subscription_kb_for("pump", lang),
                )
                paywall_count += 1
                recipient_count += 1
        except Exception as e:
            print(f"[pumpdump] send failed chat_id={chat_id} symbol={symbol}: {e}")
            error_count += 1
            continue
    return {
        "sent": sent_count + paywall_count,
        "locked": locked_count,
        "paywall": paywall_count,
        "errors": error_count,
        "subscribers": len(subscribers),
        "recipient_count": recipient_count,
    }


async def _deliver_pumpdump_signal(
    *,
    bot: Bot,
    signal: Dict[str, Any],
    symbol: str,
    subscribers: list[int],
    allow_admin_bypass: bool = True,
) -> tuple[int, int]:
    stats = await _deliver_pumpdump_signal_stats(
        bot=bot,
        signal=signal,
        symbol=symbol,
        subscribers=subscribers,
        allow_admin_bypass=allow_admin_bypass,
    )
    return stats["sent"] - stats["paywall"], stats["recipient_count"]


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

        subscribers = get_pumpdump_subscribers()

        if log_level >= 1:
            print(f"[pumpdump] subs: notify={len(subscribers)}")

        mark_tick("pumpdump", extra=f"подписчиков: {len(subscribers)}")

        if not subscribers:
            if log_level >= 1:
                print("[pumpdump] no notify subscribers -> skip")
            return

        reset_binance_metrics("pumpdump")
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
        excluded_symbols = _get_pump_excluded_symbols()
        if excluded_symbols:
            candidates = [sym for sym in candidates if sym.upper() not in excluded_symbols]
        rotation_added = max(0, len(candidates) - len(symbols))
        total = len(candidates)
        if not candidates:
            mark_error("pumpdump", "no symbols to scan after exclusion")
            return
        try:
            cursor = int(get_state("pumpdump_cursor", "0") or "0")
        except Exception as exc:
            _log_throttled(
                "db_pumpdump_cursor",
                "[db_error] op=get_state key=pumpdump_cursor err=%s",
                exc,
                exc=exc,
            )
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

        for sig in signals:
            if time.time() - start > BUDGET:
                print("[PUMP] budget exceeded, stopping early")
                break
            symbol = sig["symbol"]
            update_current_symbol("pumpdump", symbol)

            if last_sent.get(symbol) == now_min:
                continue

            last_sent[symbol] = now_min
            sent_delta, _ = await _deliver_pumpdump_signal(
                bot=bot,
                signal=sig,
                symbol=symbol,
                subscribers=subscribers,
                allow_admin_bypass=True,
            )
            sent_count += sent_delta
            if sent_delta > 0:
                direction = "PUMP" if sig.get("type") == "pump" else "DUMP"
                set_last_pumpdump_signal(
                    {
                        "symbol": symbol,
                        "ts": int(time.time()),
                        "direction": direction,
                        "change_5m": sig.get("change_5m"),
                    }
                )

        cycle_sec = time.time() - cycle_start
        current_symbol = MODULES.get("pumpdump").current_symbol if "pumpdump" in MODULES else None
        metrics = get_binance_metrics_snapshot("pumpdump")
        cache_stats = {
            "hits": metrics.get("cache_hit"),
            "misses": metrics.get("cache_miss"),
            "inflight_awaits": metrics.get("inflight_awaits"),
        }
        ticker_count = get_ticker_request_count("pumpdump")
        req_count = metrics.get("requests_total")
        klines_count = metrics.get("candles_received")
        fails = stats.get("fails", {}) if isinstance(stats, dict) else {}
        fails_top = sorted(fails.items(), key=lambda x: x[1], reverse=True)[:3]
        fails_str = ",".join([f"{k}={v}" for k, v in fails_top]) if fails_top else "-"
        if module_state:
            module_state.last_stats = stats
            module_state.fails_top = fails
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
                f"klines_source=binance_rest_shared_cache "
                f"klines_interval_1m={PUMPDUMP_1M_INTERVAL} "
                f"klines_interval_5m={PUMPDUMP_5M_INTERVAL} "
                f"klines_limit_1m={PUMPDUMP_1M_LIMIT} "
                f"klines_limit_5m={PUMPDUMP_5M_LIMIT} "
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
    global _AI_CHUNK_SIZE_CURRENT, _AI_STABLE_CYCLES
    try:
        module_state = MODULES.get("ai_signals")
        if module_state:
            module_state.state["last_cycle_ts"] = time.time()
        reset_binance_metrics("ai_signals")
        reset_ticker_request_count("ai_signals")
        mark_tick("ai_signals", extra="сканирую рынок...")

        with binance_request_context("ai_signals"):
            symbols = await _get_ai_universe()
        if not symbols:
            mark_error("ai_signals", "no symbols to scan")
            return
        excluded = _get_ai_excluded_symbols()
        if excluded:
            symbols = [symbol for symbol in symbols if symbol.upper() not in excluded]
        if not symbols:
            mark_error("ai_signals", "no symbols to scan after exclusion")
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
        priority = symbols[:max(0, min(AI_PRIORITY_N, len(symbols)))]
        priority_set = set(priority)
        pool = [s for s in symbols if s not in priority_set]

        if cursor >= len(pool):
            cursor = 0

        chunk_size = _get_ai_chunk_size()
        rotating_size = max(0, chunk_size - len(priority))
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
        with binance_request_context("ai_signals"):
            signals, stats = await scan_market(
                symbols=chunk,
                free_mode=True,
                min_score=FREE_MIN_SCORE,
                return_stats=True,
                time_budget=BUDGET,
                deep_scan_limit=AI_DEEP_TOP_K,
                excluded_symbols=excluded,
                diag_state=module_state.state if module_state else None,
            )
        module_state = MODULES.get("ai_signals")
        if module_state and isinstance(stats, dict):
            module_state.last_stats = stats
            module_state.fails_top = stats.get("fails", {})
            module_state.state["top_slowest_symbols"] = stats.get("slowest_symbols", [])
            module_state.state["fail_symbol_timeout_count"] = stats.get("fails", {}).get(
                "fail_symbol_timeout", 0
            )
            attempted_symbols = stats.get("checked", len(chunk))
            fails = stats.get("fails", {}) if isinstance(stats.get("fails", {}), dict) else {}
            fail_no_klines_count = int(fails.get("fail_no_klines", 0))
            fail_timeout_count = int(fails.get("fail_symbol_timeout", 0))
            success_symbols = max(
                0, int(attempted_symbols) - fail_no_klines_count - fail_timeout_count
            )
            no_klines_rate = (
                fail_no_klines_count / attempted_symbols if attempted_symbols else 0.0
            )
            timeout_rate = (
                fail_timeout_count / attempted_symbols if attempted_symbols else 0.0
            )
            module_state.state.update(
                {
                    "attempted_symbols": attempted_symbols,
                    "fail_no_klines_count": fail_no_klines_count,
                    "fail_symbol_timeout_count": fail_timeout_count,
                    "success_symbols": success_symbols,
                    "ai_no_klines_rate": no_klines_rate,
                    "ai_timeout_rate": timeout_rate,
                }
            )
            if AI_ADAPT_ENABLED and attempted_symbols:
                if (
                    no_klines_rate >= AI_ADAPT_FAIL_NO_KLINES_HIGH
                    or timeout_rate >= AI_ADAPT_FAIL_TIMEOUT_HIGH
                ):
                    _AI_CHUNK_SIZE_CURRENT = max(
                        AI_CHUNK_MIN, _AI_CHUNK_SIZE_CURRENT - AI_ADAPT_STEP_DOWN
                    )
                    _AI_STABLE_CYCLES = 0
                else:
                    _AI_STABLE_CYCLES += 1
                    if _AI_STABLE_CYCLES >= AI_ADAPT_STABLE_CYCLES_FOR_UP:
                        _AI_CHUNK_SIZE_CURRENT = min(
                            AI_CHUNK_MAX_EFFECTIVE,
                            _AI_CHUNK_SIZE_CURRENT + AI_ADAPT_STEP_UP,
                        )
                        _AI_STABLE_CYCLES = 0
            module_state.state.update(
                {
                    "ai_adapt_enabled": AI_ADAPT_ENABLED,
                    "ai_chunk_current": _AI_CHUNK_SIZE_CURRENT,
                    "ai_chunk_min": AI_CHUNK_MIN,
                    "ai_chunk_max": AI_CHUNK_MAX_EFFECTIVE,
                    "ai_chunk_max_base": AI_CHUNK_MAX,
                    "ai_chunk_max_safe_cap": AI_SAFE_CHUNK_MAX,
                    "ai_safe_mode": AI_SAFE_MODE,
                    "ai_stable_cycles": _AI_STABLE_CYCLES,
                }
            )
            prev_near_miss = module_state.near_miss
            try:
                module_state.near_miss = _format_near_miss(stats.get("near_miss", {}), DEFAULT_LANG)
            except Exception as exc:
                module_state.near_miss = prev_near_miss
                module_state.last_error = str(exc)
                logger.exception("AI signals error")
        deep_scans_done = stats.get("deep_scans_done", 0) if isinstance(stats, dict) else 0
        sent_count = 0
        for signal in _select_signals_for_cycle(signals):
            if time.time() - start > BUDGET:
                print("[AI] budget exceeded, stopping early")
                break
            score = signal.get("score", 0)
            if score < FREE_MIN_SCORE:
                continue
            update_current_symbol("ai_signals", signal.get("symbol", ""))
            print(f"[ai_signals] DIRECT SEND {signal['symbol']} {signal['direction']} score={score}")
            await send_signal_to_all(signal)
            sent_count += 1

        current_symbol = MODULES.get("ai_signals").current_symbol if "ai_signals" in MODULES else None
        metrics = get_binance_metrics_snapshot("ai_signals")
        cache_stats = {
            "hits": metrics.get("cache_hit"),
            "misses": metrics.get("cache_miss"),
            "inflight_awaits": metrics.get("inflight_awaits"),
        }
        ticker_count = get_ticker_request_count("ai_signals")
        req_count = metrics.get("requests_total")
        klines_count = metrics.get("candles_received")
        print(
            "[AI] "
            f"universe={total} chunk={len(chunk)} cursor={new_cursor} "
            f"signals_found={len(signals)} sent={sent_count} deep_scans={deep_scans_done}"
        )
        mark_ok(
            "ai_signals",
            extra=(
                f"universe={total} chunk={len(chunk)} cursor={new_cursor} "
                f"signals_found={len(signals)} sent={sent_count} deep_scans={deep_scans_done} "
                f"current={current_symbol or '-'} cycle={int(time.time() - start)}s "
                f"req={req_count} klines={klines_count} "
                f"klines_hits={cache_stats.get('hits')} klines_misses={cache_stats.get('misses')} "
                f"klines_inflight={cache_stats.get('inflight_awaits')} "
                f"ticker_req={ticker_count}"
            ),
        )
    finally:
        print("[AI] scan_once end")


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
    print(
        f"[ai_signals] AI_CHUNK_SIZE={_AI_CHUNK_SIZE_CURRENT} "
        f"AI_ADAPT_ENABLED={'1' if AI_ADAPT_ENABLED else '0'}"
    )
    signals_task = asyncio.create_task(
        _delayed_task(12, safe_worker_loop("ai_signals", ai_scan_once))
    )
    audit_task = asyncio.create_task(_delayed_task(18, signal_audit_worker_loop()))
    watchdog_task = asyncio.create_task(watchdog())
    try:
        await dp.start_polling(bot)
    finally:
        signals_task.cancel()
        with suppress(asyncio.CancelledError):
            await signals_task
        pump_task.cancel()
        with suppress(asyncio.CancelledError):
            await pump_task
        audit_task.cancel()
        with suppress(asyncio.CancelledError):
            await audit_task
        watchdog_task.cancel()
        with suppress(asyncio.CancelledError):
            await watchdog_task
        await close_shared_session()


if __name__ == "__main__":
    asyncio.run(main())
