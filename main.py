import asyncio
import json
import logging
import os
import sqlite3
import traceback
import time
import random
import re
from contextlib import suppress
from datetime import datetime, timezone, timedelta
from typing import Any, Dict, List, Awaitable

from aiogram import Bot, Dispatcher, F
from aiogram.client.default import DefaultBotProperties
from aiogram.exceptions import TelegramBadRequest, TelegramForbiddenError, TelegramRetryAfter
from aiogram.types import (
    CallbackQuery,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    Message,
    ReplyKeyboardMarkup,
)
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
    process_confirm_retry_queue,
    register_confirm_retry_sent,
    AI_MAX_DEEP_PER_CYCLE,
    AI_STAGE_A_TOP_K,
    PRE_SCORE_THRESHOLD,
    MIN_PRE_SCORE,
    AI_EMA50_NEAR_PCT,
    AI_POI_MAX_DISTANCE_PCT,
    AI_MIN_RR,
    AI_STRUCTURE_MODE,
    AI_STRUCTURE_PENALTY_NEUTRAL,
    AI_STRUCTURE_HARD_FAIL_ON_OPPOSITE,
    AI_STRUCTURE_WINDOW,
    apply_btc_soft_gate,
)
from config import cfg
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
    load_module_statuses,
)

from cutoff_config import allow_legacy_for_user, get_stats_cutoff_ts
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
    get_inversion_enabled,
    set_inversion_enabled,
    kv_get_int,
    kv_set_int,
    purge_symbol,
    insert_signal_event,
    list_signal_events,
    list_signal_events_by_identity,
    get_signal_history,
    count_signal_history,
    get_history_winrate_summary,
    list_open_signal_events,
    count_signal_events,
    get_signal_outcome_counts,
    get_signal_score_bucket_counts,
    get_signal_avg_rr,
    get_signal_event,
    get_signal_event_by_message,
    get_signal_by_id,
    update_signal_event_refresh,
    update_signal_event_status_by_id,
    activate_signal_events,
    mark_signal_events_poi_touched,
    mark_signal_result_notified,
    claim_signal_result_notification,
    release_signal_result_notification_claim,
    list_pending_result_notifications,
    set_last_pumpdump_signal,
    purge_test_signals,
)
from db_path import ensure_db_writable, get_db_path
from history_status import get_signal_badge, get_signal_status_key
from market_cache import get_ticker_request_count, reset_ticker_request_count
from btc_context import get_btc_regime
from alert_dedup_db import init_alert_dedup, can_send
from status_utils import is_notify_enabled
from message_templates import (
    format_signal_poi_touched_message,
    format_scenario_message,
    format_signal_activation_message,
)
from signal_audit_db import (
    get_public_stats,
    get_last_signal_audit,
    count_signals_sent_since,
    init_signal_audit_tables,
    insert_signal_audit,
)
from signal_audit_worker import (
    signal_audit_worker_loop,
    set_signal_activation_notifier,
    set_signal_poi_touched_notifier,
    set_signal_progress_notifier,
    set_signal_result_notifier,
)
import i18n
from utils_symbols import ui_symbol
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
from signal_inversion import apply_inversion

logger = logging.getLogger(__name__)
DEFAULT_LANG = "ru"
_LOG_THROTTLE_SEC = 30.0
_LAST_LOG_TS: Dict[str, float] = {}

_CLOSE_NOTIFY_METRICS: Dict[str, Any] = {
    "close_events_detected_total": 0,
    "close_notifications_sent_total": 0,
    "close_notifications_failed_total": 0,
    "last_close_event": None,
}


def _record_close_event(*, symbol: str, side: str, reason: str) -> None:
    _CLOSE_NOTIFY_METRICS["close_events_detected_total"] = int(_CLOSE_NOTIFY_METRICS.get("close_events_detected_total", 0) or 0) + 1
    _CLOSE_NOTIFY_METRICS["last_close_event"] = {
        "symbol": str(symbol or ""),
        "side": str(side or ""),
        "reason": str(reason or ""),
        "time": int(time.time()),
    }


def _record_close_notify_sent() -> None:
    _CLOSE_NOTIFY_METRICS["close_notifications_sent_total"] = int(_CLOSE_NOTIFY_METRICS.get("close_notifications_sent_total", 0) or 0) + 1


def _record_close_notify_failed() -> None:
    _CLOSE_NOTIFY_METRICS["close_notifications_failed_total"] = int(_CLOSE_NOTIFY_METRICS.get("close_notifications_failed_total", 0) or 0) + 1


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
    load_module_statuses()
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


_PUMP_TOGGLE_TTL_SEC = 24 * 60 * 60
_PUMP_MESSAGE_STATE: dict[tuple[int, int], dict[str, Any]] = {}


def _pump_state_cleanup(now_ts: int | None = None) -> None:
    now = int(time.time()) if now_ts is None else int(now_ts)
    expired_keys = [
        key for key, value in _PUMP_MESSAGE_STATE.items() if now - int(value.get("ts", now)) > _PUMP_TOGGLE_TTL_SEC
    ]
    for key in expired_keys:
        _PUMP_MESSAGE_STATE.pop(key, None)


def _save_pump_message_state(*, chat_id: int, message_id: int, collapsed_text: str, expanded_text: str, lang: str) -> None:
    _pump_state_cleanup()
    _PUMP_MESSAGE_STATE[(int(chat_id), int(message_id))] = {
        "ts": int(time.time()),
        "collapsed": collapsed_text,
        "expanded": expanded_text,
        "is_expanded": False,
        "lang": lang,
    }


def _get_pump_message_state(chat_id: int, message_id: int) -> dict[str, Any] | None:
    _pump_state_cleanup()
    state = _PUMP_MESSAGE_STATE.get((int(chat_id), int(message_id)))
    if state is None:
        return None
    if int(time.time()) - int(state.get("ts", 0)) > _PUMP_TOGGLE_TTL_SEC:
        _PUMP_MESSAGE_STATE.pop((int(chat_id), int(message_id)), None)
        return None
    return state


def _pump_toggle_inline_kb(*, lang: str, chat_id: int, message_id: int, expanded: bool) -> InlineKeyboardMarkup:
    key = "PUMP_BUTTON_COLLAPSE" if expanded else "PUMP_BUTTON_EXPAND"
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(
                    text=i18n.t(lang, key),
                    callback_data=f"pump_toggle:{int(chat_id)}:{int(message_id)}",
                )
            ]
        ]
    )


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


def _resolve_user_lang(chat_id: int | None, fallback: str = "ru") -> str:
    if chat_id is None:
        return fallback
    try:
        lang = get_user_lang(chat_id)
    except Exception as exc:
        logger.exception("get_user_lang failed for chat_id=%s: %s", chat_id, exc)
        return fallback
    return lang or fallback


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
        try:
            cur.execute("SELECT chat_id FROM ai_signals_subscribers")
        except sqlite3.OperationalError as exc:
            if "no such table" not in str(exc).lower():
                raise
        else:
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
FREE_MIN_SCORE = cfg.final_score_threshold
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
    return ", ".join(sorted(ui_symbol(symbol) for symbol in symbols)) if symbols else "-"


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
    lang = _resolve_user_lang(message.chat.id)
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
    lang = _resolve_user_lang(message.chat.id)
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
    passed = {"TP1", "TP2", "BE", "TP"}
    failed = {"SL", "FAILED"}
    neutral = {"NF", "NO_FILL", "EXP", "EXPIRED", "NEUTRAL", "NO_CONFIRMATION", "AMBIGUOUS"}
    normalized = (status or "").upper().strip()
    if normalized in passed:
        return "🟢"
    if normalized in failed:
        return "🔴"
    if normalized in neutral:
        return "⚪"
    if normalized in {"ACTIVE"}:
        return "🟡"
    return "🟡"


def _format_signal_event_status(raw_status: str, lang: str) -> str:
    status_map = {
        "OPEN": i18n.t(lang, "STATUS_OPEN"),
        "ACTIVE": i18n.t(lang, "STATUS_ACTIVE_WAITING"),
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


def _safe_int(value: Any, default: int = 0) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def _format_event_time(ts: int) -> str:
    try:
        dt = datetime.fromtimestamp(int(ts), tz=ALMATY_TZ)
    except (OSError, OverflowError, TypeError, ValueError):
        return "—"
    return dt.strftime("%d.%m - %H:%M")



def _format_cutoff_note_date(lang: str) -> str | None:
    cutoff_ts = get_stats_cutoff_ts()
    if cutoff_ts <= 0:
        return None
    try:
        dt = datetime.fromtimestamp(cutoff_ts, tz=ALMATY_TZ)
    except (OSError, OverflowError, TypeError, ValueError):
        return None
    if lang == "ru":
        return dt.strftime("%d.%m.%Y")
    return dt.strftime("%Y-%m-%d")

def _safe_float(value: Any) -> float | None:
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _signal_status_group(row: dict[str, Any]) -> str:
    status_raw = str(row.get("result") or row.get("status") or "").upper().strip()
    if status_raw in {"TP1", "TP2", "BE", "TP"}:
        return "tp"
    if status_raw == "SL":
        return "sl"
    if status_raw in {"EXP", "EXPIRED", "NO_FILL", "NF", "NEUTRAL"}:
        return "neutral"
    return "in_progress"


def _calc_rr(row: dict[str, Any]) -> float | None:
    poi_low = _safe_float(row.get("poi_low"))
    poi_high = _safe_float(row.get("poi_high"))
    sl = _safe_float(row.get("sl"))
    tp1 = _safe_float(row.get("tp1"))
    if None in {poi_low, poi_high, sl, tp1}:
        return None
    entry_mid = ((poi_low or 0.0) + (poi_high or 0.0)) / 2
    risk = abs(entry_mid - (sl or 0.0))
    reward = abs((tp1 or 0.0) - entry_mid)
    if risk <= 0 or reward <= 0:
        return None
    return reward / risk


def _format_bucket_winrate(bucket: dict[str, int]) -> str:
    passed = int(bucket.get("passed", 0) or 0)
    failed = int(bucket.get("failed", 0) or 0)
    denominator = passed + failed
    if denominator <= 0:
        return "—"
    return f"{(passed / denominator) * 100:.1f}%"


def _format_avg_rr(rr_value: float | None) -> str:
    if rr_value is None or rr_value <= 0:
        return "—"
    return f"~1 : {rr_value:.2f}"


def build_pro_stats_text(
    *,
    period_key: str,
    lang: str,
    outcome_counts: dict[str, int],
    score_bucket_counts: dict[str, dict[str, int]],
    avg_rr_90_100: float | None,
) -> str:
    bucket_90 = score_bucket_counts.get("90-100", {})
    bucket_80 = score_bucket_counts.get("80-89", {})

    winrate_90_text = _format_bucket_winrate(bucket_90)
    winrate_80_text = _format_bucket_winrate(bucket_80)
    avg_rr_text = _format_avg_rr(avg_rr_90_100)

    count_90_100 = sum(int(bucket_90.get(key, 0) or 0) for key in ("passed", "failed", "neutral", "in_progress"))
    count_80_89 = sum(int(bucket_80.get(key, 0) or 0) for key in ("passed", "failed", "neutral", "in_progress"))

    tp_total = int(outcome_counts.get("passed", 0) or 0)
    sl_total = int(outcome_counts.get("failed", 0) or 0)
    neutral_total = int(outcome_counts.get("neutral", 0) or 0)
    in_progress_total = int(outcome_counts.get("in_progress", 0) or 0)

    lines = [
        i18n.t(lang, "STATS_PRO_TITLE"),
        "",
        i18n.t(lang, "STATS_PRO_RECOMMENDED_HEADER"),
        i18n.t(lang, "STATS_PRO_RECOMMENDED_SUB"),
        "",
        i18n.t(lang, "STATS_PRO_SCORE_RANGE_90_100"),
        i18n.t(lang, "STATS_PRO_WINRATE_LINE", winrate=winrate_90_text),
        i18n.t(lang, "STATS_PRO_AVG_RR_LINE", avg_rr=avg_rr_text),
        i18n.t(lang, "STATS_PRO_TOTAL_SIGNALS_LINE", count=count_90_100),
        i18n.t(lang, "STATS_PRO_STATUS_PRIMARY"),
        "",
        i18n.t(lang, "STATS_PRO_RR_NOTE"),
        "",
        i18n.t(lang, "STATS_PRO_DIVIDER"),
        "",
        i18n.t(lang, "STATS_PRO_HIGH_RISK_HEADER"),
        i18n.t(lang, "STATS_PRO_HIGH_RISK_SUB"),
        "",
        i18n.t(lang, "STATS_PRO_SCORE_RANGE_80_89"),
        i18n.t(lang, "STATS_PRO_WINRATE_LINE", winrate=winrate_80_text),
        i18n.t(lang, "STATS_PRO_TOTAL_SIGNALS_LINE", count=count_80_89),
        i18n.t(lang, "STATS_PRO_STATUS_SELECTIVE"),
        "",
        i18n.t(lang, "STATS_PRO_DIVIDER"),
        "",
        i18n.t(lang, "STATS_PRO_BELOW_THRESHOLD_HEADER"),
        i18n.t(lang, "STATS_PRO_BELOW_THRESHOLD_SUB"),
        "",
        i18n.t(lang, "STATS_PRO_BELOW_THRESHOLD_SCORE"),
        i18n.t(lang, "STATS_PRO_BELOW_THRESHOLD_LINE1"),
        i18n.t(lang, "STATS_PRO_BELOW_THRESHOLD_LINE2"),
        "",
        i18n.t(lang, "STATS_PRO_DIVIDER"),
        "",
        i18n.t(lang, "STATS_PRO_SUMMARY_HEADER"),
        i18n.t(lang, "STATS_PRO_SUMMARY_SUB", period=_period_label(period_key, lang)),
        "",
        i18n.t(lang, "STATS_PRO_TP_TOTAL", tp_total=tp_total),
        i18n.t(lang, "STATS_PRO_SL_TOTAL", sl_total=sl_total),
        i18n.t(lang, "STATS_PRO_NEUTRAL_TOTAL", neutral_total=neutral_total),
        i18n.t(lang, "STATS_PRO_IN_PROGRESS_TOTAL", in_progress_total=in_progress_total),
        "",
        i18n.t(lang, "STATS_PRO_NEUTRAL_NOTE"),
        i18n.t(lang, "STATS_PRO_NEUTRAL_NOTE_2"),
        "",
        i18n.t(lang, "STATS_PRO_DIVIDER"),
        "",
        i18n.t(lang, "STATS_PRO_USAGE_HEADER"),
        i18n.t(lang, "STATS_PRO_USAGE_PRIMARY"),
        i18n.t(lang, "STATS_PRO_USAGE_HIGH_RISK"),
        i18n.t(lang, "STATS_PRO_USAGE_AVOID"),
        "",
        i18n.t(lang, "STATS_PRO_RISK_NOTE"),
        i18n.t(lang, "STATS_PRO_LEVERAGE_NOTE"),
    ]
    return "\n".join(lines)


def _split_message_text(text: str, limit: int = 3800) -> list[str]:
    if len(text) <= limit:
        return [text]
    lines = text.split("\n")
    chunks: list[str] = []
    current = ""
    for line in lines:
        candidate = line if not current else f"{current}\n{line}"
        if len(candidate) > limit and current:
            chunks.append(current)
            current = line
        else:
            current = candidate
    if current:
        chunks.append(current)
    return chunks


async def _edit_message_with_chunks(
    message: Message,
    text: str,
    reply_markup: InlineKeyboardMarkup | None = None,
) -> None:
    chunks = _split_message_text(text)
    first_chunk = chunks[0] if chunks else ""
    try:
        await message.edit_text(first_chunk, reply_markup=reply_markup)
    except Exception as exc:
        logger.warning("edit message failed, sending new: %s", exc)
        with suppress(Exception):
            await message.answer(first_chunk, reply_markup=reply_markup)
    for chunk in chunks[1:]:
        with suppress(Exception):
            await message.answer(chunk)


def _history_state_key(user_id: int) -> str:
    return f"history_ctx:{user_id}"


def _set_history_context(
    user_id: int,
    time_window: str,
    page: int,
    winrate_summary: dict[str, Any] | None = None,
) -> None:
    payload: dict[str, Any] = {"window": time_window, "page": page}
    if winrate_summary is not None:
        payload["winrate_summary"] = winrate_summary
    set_state(_history_state_key(user_id), json.dumps(payload))


def _get_history_context(user_id: int) -> tuple[str, int, dict[str, Any] | None] | None:
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

    raw_summary = parsed.get("winrate_summary")
    winrate_summary: dict[str, Any] | None = None
    if isinstance(raw_summary, dict):
        winrate_summary = raw_summary

    return time_window, max(1, page_value), winrate_summary


def _normalize_history_status(raw_status: str | None) -> str:
    status_key = get_signal_status_key({"outcome": raw_status})
    if status_key in {"ACTIVATED", "POI_TOUCHED"}:
        return "IN_PROGRESS"
    return status_key


def _history_status_label(status_key: str, lang: str) -> str:
    status_labels = {
        "TP": i18n.t(lang, "history_status_tp"),
        "SL": i18n.t(lang, "history_status_sl"),
        "EXPIRED_NO_ENTRY": i18n.t(lang, "history_status_expired_no_entry"),
        "NO_CONFIRMATION": i18n.t(lang, "history_status_no_confirmation"),
        "IN_PROGRESS": i18n.t(lang, "history_status_in_progress"),
    }
    return status_labels.get(status_key, status_labels["IN_PROGRESS"])


def _history_status_icon(status_key: str) -> str:
    icon_map = {
        "TP": "🟢",
        "SL": "🔴",
        "EXPIRED_NO_ENTRY": "⚪",
        "NO_CONFIRMATION": "🔵",
        "IN_PROGRESS": "🟣",
    }
    return icon_map.get(status_key, "🟣")


def _history_row_icon(row: dict[str, Any]) -> str:
    return get_signal_badge(row)


def _signal_side_label(side: Any) -> str:
    normalized_side = str(side or "").upper().strip()
    if normalized_side in {"LONG", "BUY"}:
        return "LONG"
    if normalized_side in {"SHORT", "SELL"}:
        return "SHORT"
    return "SHORT"


def _format_signal_list_row(
    *,
    side: Any,
    icon: str,
    score: Any,
    symbol: Any,
    created_at: Any,
) -> str:
    side_label = _signal_side_label(side)
    side_prefix = side_label.ljust(5)
    icon_value = str(icon or "🟣").strip() or "🟣"
    score_value = _safe_int(score, 0)
    symbol_value = _short_symbol(str(symbol or "—"))
    created_at_value = _safe_int(created_at, 0)
    return f"{side_prefix} {icon_value} | Score {score_value} | {symbol_value} | {_format_event_time(created_at_value)}"


def _get_history_page(
    *,
    time_window: str,
    page: int,
    page_size: int = 12,
    include_legacy: bool = False,
) -> tuple[int, int, int, list[dict]]:
    total = count_signal_history(
        time_window=time_window,
        user_id=None,
        min_score=None,
        include_legacy=include_legacy,
    )
    pages = max(1, (total + page_size - 1) // page_size)
    page_value = max(1, min(page, pages))
    offset = (page_value - 1) * page_size
    rows = get_signal_history(
        time_window=time_window,
        user_id=None,
        limit=page_size,
        offset=offset,
        include_legacy=include_legacy,
    )
    return page_value, pages, total, [dict(row) for row in rows]


def _format_history_item(row: dict[str, Any], lang: str) -> str:
    del lang
    icon = _history_row_icon(row)
    return _format_signal_list_row(
        side=row.get("side"),
        icon=icon,
        score=row.get("score"),
        symbol=row.get("symbol"),
        created_at=row.get("created_at") or row.get("ts"),
    )


def _format_history_pro_block(lang: str, history_summary: dict[str, Any]) -> str:
    bucket_90 = history_summary.get("90_100", {}) if isinstance(history_summary, dict) else {}
    bucket_80 = history_summary.get("80_89", {}) if isinstance(history_summary, dict) else {}
    totals = history_summary.get("totals", {}) if isinstance(history_summary, dict) else {}

    avg_rr_value = bucket_90.get("avg_rr") if isinstance(bucket_90, dict) else None
    avg_rr = f"{float(avg_rr_value):.2f}" if isinstance(avg_rr_value, (int, float)) else "—"

    winrate_90 = (
        str(_safe_int(bucket_90.get("winrate"), 0))
        if isinstance(bucket_90, dict) and bucket_90.get("winrate") is not None
        else "—"
    )
    winrate_80 = (
        str(_safe_int(bucket_80.get("winrate"), 0))
        if isinstance(bucket_80, dict) and bucket_80.get("winrate") is not None
        else "—"
    )
    closed_90 = _safe_int(bucket_90.get("closed"), 0) if isinstance(bucket_90, dict) else 0
    closed_80 = _safe_int(bucket_80.get("closed"), 0) if isinstance(bucket_80, dict) else 0
    tp_total = _safe_int(totals.get("tp"), 0) if isinstance(totals, dict) else 0
    sl_total = _safe_int(totals.get("sl"), 0) if isinstance(totals, dict) else 0
    expired_no_entry_total = _safe_int(totals.get("expired_no_entry"), 0) if isinstance(totals, dict) else 0
    no_confirmation_total = _safe_int(totals.get("no_confirmation"), 0) if isinstance(totals, dict) else 0
    in_progress_total = _safe_int(totals.get("in_progress"), 0) if isinstance(totals, dict) else 0

    return "\n".join(
        [
            i18n.t(lang, "section_recommended_title"),
            i18n.t(lang, "line_winrate", value=winrate_90),
            i18n.t(lang, "line_avg_rr", value=avg_rr),
            i18n.t(lang, "line_trades", value=closed_90),
            i18n.t(lang, "line_status", value=i18n.t(lang, "status_main_focus")),
            "",
            i18n.t(lang, "section_higher_risk_title"),
            i18n.t(lang, "line_winrate", value=winrate_80),
            i18n.t(lang, "line_trades", value=closed_80),
            i18n.t(lang, "line_status", value=i18n.t(lang, "status_use_selectively")),
            "",
            i18n.t(lang, "section_score_below_title"),
            i18n.t(lang, "line_not_included"),
            i18n.t(lang, "line_market_analysis_only"),
            "",
            i18n.t(lang, "totals_title"),
            i18n.t(lang, "totals_tp", value=tp_total),
            i18n.t(lang, "totals_sl", value=sl_total),
            i18n.t(lang, "totals_expired_no_entry", value=expired_no_entry_total),
            i18n.t(lang, "totals_no_confirmation", value=no_confirmation_total),
            i18n.t(lang, "totals_in_progress", value=in_progress_total),
            i18n.t(lang, "history_expired_helper"),
        ]
    )


def _build_history_text(
    *,
    time_window: str,
    page: int,
    pages: int,
    total: int,
    rows: list[dict],
    lang: str,
    history_summary: dict[str, Any],
    include_legacy: bool,
) -> str:
    period_label = _period_label(time_window, lang)
    lines = [
        i18n.t(lang, "history_title", period=period_label),
        i18n.t(lang, "page_total", page=page, pages=pages, total=total),
    ]
    if not include_legacy:
        cutoff_label = _format_cutoff_note_date(lang)
        if cutoff_label:
            lines.extend([i18n.t(lang, "stats_since_date_note", date=cutoff_label), ""])
    lines.extend([
        "",
        _format_history_pro_block(lang, history_summary),
        "",
        "━━━━━━━━━━━━━━━━",
        i18n.t(lang, "explanation_title"),
        "━━━━━━━━━━━━━━━━",
        i18n.t(lang, "explanation_line_1"),
        i18n.t(lang, "explanation_line_2"),
        i18n.t(lang, "history_indicator_waiting"),
        i18n.t(lang, "history_indicator_poi_touched"),
        i18n.t(lang, "history_indicator_activated"),
    ])
    if not rows:
        lines.append("")
        lines.append(i18n.t(lang, "HISTORY_EMPTY_PERIOD"))
        return "\n".join(lines)
    return "\n".join(lines)


def _history_nav_kb(
    *,
    lang: str,
    time_window: str,
    page: int,
    pages: int,
    events: list[dict],
) -> InlineKeyboardMarkup:
    kb_rows: list[list[InlineKeyboardButton]] = []
    for event in events:
        event_id = _safe_int(event.get("id"), 0)
        if event_id <= 0:
            continue
        kb_rows.append(
            [
                InlineKeyboardButton(
                    text=_format_history_item(event, lang),
                    callback_data=f"history_open:{event_id}",
                )
            ]
        )

    nav_row: list[InlineKeyboardButton] = []
    if page > 1:
        nav_row.append(
            InlineKeyboardButton(
                text=i18n.t(lang, "nav_prev_page"),
                callback_data=f"history:{time_window}:page={page - 1}",
            )
        )
    if page < pages:
        nav_row.append(
            InlineKeyboardButton(
                text=i18n.t(lang, "nav_next_page"),
                callback_data=f"history:{time_window}:page={page + 1}",
            )
        )
    if nav_row:
        kb_rows.append(nav_row)

    kb_rows.append(
        [
            InlineKeyboardButton(
                text=i18n.t(lang, "nav_back_to_periods"),
                callback_data="hist_back",
            )
        ]
    )

    return InlineKeyboardMarkup(inline_keyboard=kb_rows)


async def _render_history(*, callback: CallbackQuery, time_window: str, page: int) -> None:
    if callback.message is None or callback.from_user is None:
        return
    lang = _resolve_user_lang(callback.from_user.id)
    include_legacy = allow_legacy_for_user(is_admin_user=is_admin(callback.from_user.id))
    page_value, pages, total, rows = _get_history_page(
        time_window=time_window,
        page=page,
        include_legacy=include_legacy,
    )

    history_summary = get_history_winrate_summary(
        time_window=time_window,
        user_id=None,
        include_legacy=include_legacy,
    )

    _set_history_context(
        callback.from_user.id,
        time_window,
        page_value,
        winrate_summary=history_summary,
    )
    text = _build_history_text(
        time_window=time_window,
        page=page_value,
        pages=pages,
        total=total,
        rows=rows,
        lang=lang,
        history_summary=history_summary,
        include_legacy=include_legacy,
    )
    markup = _history_nav_kb(
        lang=lang,
        time_window=time_window,
        page=page_value,
        pages=pages,
        events=rows,
    )
    await _edit_message_with_chunks(callback.message, text, reply_markup=markup)


def enforce_signal_ttl() -> int:
    now = int(time.time())
    updated = 0
    open_events = list_open_signal_events()
    for row in open_events:
        event = dict(row)
        created_at = int(event.get("ts", 0))
        if created_at <= 0:
            continue
        ttl_minutes = int(event.get("ttl_minutes") or SIGNAL_TTL_SECONDS // 60)
        ttl_sec = max(60, ttl_minutes * 60)
        age_sec = now - created_at
        if age_sec < ttl_sec:
            continue
        state = str(event.get("state") or "").upper()
        status_raw = str(event.get("status") or "").upper()
        result_raw = str(event.get("result") or "").upper()
        is_activated = (
            bool(event.get("activated_at"))
            or bool(event.get("is_activated"))
            or bool(event.get("entry_touched"))
            or state in {"ACTIVE_CONFIRMED", "ACTIVATED", "ENTRY_CONFIRMED"}
            or status_raw == "ACTIVE"
            or result_raw == "ACTIVE"
        )
        if is_activated:
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


def _alerts_bucket_from_score(score: int) -> str:
    return "elite" if int(score) >= 90 else "regular"


def _alerts_pref_key_for_bucket(bucket: str) -> str:
    return "notif_elite_enabled" if bucket == "elite" else "notif_regular_enabled"


def _status_toggle_inline_kb(*, lang: str, score: int, enabled: bool) -> InlineKeyboardMarkup:
    bucket = _alerts_bucket_from_score(score)
    label_key = "SIGNAL_STATUS_TOGGLE_ON" if enabled else "SIGNAL_STATUS_TOGGLE_OFF"
    return InlineKeyboardMarkup(
        inline_keyboard=[[
            InlineKeyboardButton(
                text=i18n.t(lang, label_key),
                callback_data=f"toggle_alerts:{bucket}",
            )
        ]]
    )


def _format_short_result_message(event: dict, lang: str) -> str | None:
    status_raw = str(event.get("result") or event.get("status") or "OPEN")
    status = _normalize_signal_status(status_raw)
    symbol = ui_symbol(str(event.get("symbol", "")).upper())
    side = str(event.get("side", "")).upper()
    score = int(event.get("score", 0))
    entry_from = float(event.get("poi_low", 0.0))
    entry_to = float(event.get("poi_high", 0.0))
    sl = float(event.get("sl", 0.0))
    tp1 = float(event.get("tp1", 0.0))
    tp2 = float(event.get("tp2", 0.0))
    if not sl:
        return None

    header = ""
    detail_lines: list[str] = []

    entry_price = float(event.get("entry_price") or 0.0)
    if status in {"TP1", "TP2", "SL"} and entry_price > 0:
        entry_value = _format_price(entry_price)
    elif entry_from or entry_to:
        if entry_from and entry_to:
            entry_value = f"{_format_price(entry_from)}–{_format_price(entry_to)}"
        else:
            entry_value = _format_price(entry_from or entry_to)
    else:
        entry_value = ""

    body_lines: list[str] = []
    if status == "TP1":
        header = i18n.t(lang, "CLOSE_TP1_TITLE")
        detail_lines = [f"{symbol} {side}"]
        if entry_value:
            body_lines.append(i18n.t(lang, "SIGNAL_RESULT_ENTRY_LINE", entry=entry_value))
        if tp1:
            body_lines.append(i18n.t(lang, "SIGNAL_RESULT_EXIT_LINE", price=_format_price(tp1)))
        pnl_pct = event.get("pnl_pct")
        if pnl_pct is None and entry_price > 0 and tp1 > 0:
            side_mult = 1.0 if side == "LONG" else -1.0
            pnl_pct = ((tp1 - entry_price) / entry_price) * 100.0 * side_mult
        if pnl_pct is not None:
            body_lines.append(i18n.t(lang, "SIGNAL_RESULT_PNL_LINE", pnl=f"{float(pnl_pct):.2f}"))
        if tp1:
            body_lines.append(i18n.t(lang, "SIGNAL_RESULT_TP1_LINE", price=_format_price(tp1)))
        if tp2:
            body_lines.append(i18n.t(lang, "SIGNAL_RESULT_TP2_LINE", price=_format_price(tp2)))
    elif status == "TP2":
        header = i18n.t(lang, "CLOSE_TP2_TITLE")
        detail_lines = [f"{symbol} {side}"]
        if entry_value:
            body_lines.append(i18n.t(lang, "SIGNAL_RESULT_ENTRY_LINE", entry=entry_value))
        if tp2:
            body_lines.append(i18n.t(lang, "SIGNAL_RESULT_EXIT_LINE", price=_format_price(tp2)))
        pnl_pct = event.get("pnl_pct")
        if pnl_pct is None and entry_price > 0 and tp2 > 0:
            side_mult = 1.0 if side == "LONG" else -1.0
            pnl_pct = ((tp2 - entry_price) / entry_price) * 100.0 * side_mult
        if pnl_pct is not None:
            body_lines.append(i18n.t(lang, "SIGNAL_RESULT_PNL_LINE", pnl=f"{float(pnl_pct):.2f}"))
        if tp1:
            body_lines.append(i18n.t(lang, "SIGNAL_RESULT_TP1_LINE", price=_format_price(tp1)))
        if tp2:
            body_lines.append(i18n.t(lang, "SIGNAL_RESULT_TP2_LINE", price=_format_price(tp2)))
    elif status == "BE":
        header = i18n.t(lang, "SIGNAL_RESULT_HEADER_BE_AFTER_TP1")
        detail_lines = [
            f"{symbol} {side}",
        ]
    elif status == "SL":
        header = i18n.t(lang, "CLOSE_SL_TITLE")
        detail_lines = [
            f"{symbol} {side}",
        ]
    elif status == "NF":
        header = i18n.t(lang, "SIGNAL_EXPIRED_NO_ENTRY_HEADER")
        detail_lines = [
            i18n.t(lang, "SIGNAL_EXPIRED_NO_ENTRY_LINE_1", symbol=symbol, side=side),
            i18n.t(lang, "SIGNAL_EXPIRED_NO_ENTRY_LINE_2"),
            i18n.t(lang, "SIGNAL_EXPIRED_NO_ENTRY_LINE_3"),
        ]
    elif status == "EXP":
        header = i18n.t(lang, "SIGNAL_RESULT_HEADER_EXP")
        detail_lines = [
            f"{symbol} {side}",
        ]
    else:
        return None

    if status in {"SL", "EXP", "NF", "BE"}:
        if entry_value:
            body_lines.append(i18n.t(lang, "SIGNAL_RESULT_ENTRY_LINE", entry=entry_value))
        if status in {"SL", "EXP"}:
            body_lines.append(i18n.t(lang, "SIGNAL_RESULT_SL_LINE", price=_format_price(sl)))
        if tp1:
            body_lines.append(i18n.t(lang, "SIGNAL_RESULT_TP1_LINE", price=_format_price(tp1)))
        if tp2:
            body_lines.append(i18n.t(lang, "SIGNAL_RESULT_TP2_LINE", price=_format_price(tp2)))
    detail_lines.extend(body_lines)
    if status in {"TP1", "TP2", "SL"}:
        close_reason_key = {
            "TP1": "SIGNAL_RESULT_CLOSED_BY_TP1_LINE",
            "TP2": "SIGNAL_RESULT_CLOSED_BY_TP2_LINE",
            "SL": "SIGNAL_RESULT_CLOSED_BY_SL_LINE",
        }.get(status)
        if close_reason_key:
            detail_lines.append(i18n.t(lang, close_reason_key))
        detail_lines.append(i18n.t(lang, "SIGNAL_RESULT_SCORE_LINE", score=score))
    return "\n".join([header, "", *detail_lines])


async def notify_signal_result_short(signal: dict) -> bool:
    if bot is None:
        return False
    status_raw = str(signal.get("result") or signal.get("status") or "")
    if not _is_final_signal_status(status_raw):
        return False
    event_id = int(signal.get("id", 0) or 0)
    if bool(signal.get("result_notified")) and signal.get("result_notified") != 2:
        return False
    claimed_here = False
    if event_id:
        claimed_here = claim_signal_result_notification(event_id)
        if not claimed_here and signal.get("result_notified") != 2:
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

    lang = _resolve_user_lang(user_id)
    message_text = _format_short_result_message(signal, lang)
    if not message_text:
        return False

    logger.info(
        "[close_notify] notify attempt event_id=%s user_id=%s status=%s",
        event_id,
        user_id,
        _normalize_signal_status(status_raw),
    )
    try:
        await bot.send_message(
            user_id,
            message_text,
            disable_notification=_disable_notification_for_event(
                user_id=user_id,
                event_type=status_raw,
            ),
            reply_markup=InlineKeyboardMarkup(
                inline_keyboard=[[build_binance_button(lang, str(signal.get("symbol", "")))]],
            ),
        )
    except Exception as exc:
        _record_close_notify_failed()
        logger.warning(
            "[ai_signals] close notify failed event_id=%s user_id=%s status=%s error=%s",
            event_id,
            user_id,
            _normalize_signal_status(status_raw),
            exc,
        )
        if claimed_here and event_id:
            release_signal_result_notification_claim(event_id)
        return False

    if event_id:
        mark_signal_result_notified(event_id)
    _record_close_notify_sent()
    logger.info(
        "[close_notify] notify success event_id=%s user_id=%s status=%s",
        event_id,
        user_id,
        _normalize_signal_status(status_raw),
    )
    return True


async def retry_pending_close_notifications(limit: int = 200) -> int:
    pending_rows = list_pending_result_notifications(limit=limit)
    sent = 0
    for row in pending_rows:
        event = dict(row)
        status_raw = str(event.get("result") or event.get("status") or "")
        if not _is_final_signal_status(status_raw):
            continue
        ok = await notify_signal_result_short(event)
        if ok:
            sent += 1
    return sent


async def notify_signal_activation(signal: dict) -> bool:
    if bot is None:
        return False

    module = str(signal.get("module", ""))
    symbol = str(signal.get("symbol", ""))
    ts_value = int(signal.get("sent_at", 0))
    if not module or not symbol or ts_value <= 0:
        return False

    activated_at = int(signal.get("activated_at") or time.time())
    entry_price = float(signal.get("entry_price") or 0.0)
    print(
        "[ai_signals] activation confirmed "
        f"{symbol} {signal.get('direction', '')} "
        f"confirm_strict={bool(signal.get('confirm_strict', False))} "
        f"confirm_count={int(signal.get('confirm_count', 0) or 0)}"
    )
    if entry_price <= 0:
        return False

    updated_rows = activate_signal_events(
        module=module,
        symbol=symbol,
        ts=ts_value,
        activated_at=activated_at,
        entry_price=entry_price,
    )
    if updated_rows <= 0:
        return False

    events = list_signal_events_by_identity(module=module, symbol=symbol, ts=ts_value)
    if not events:
        return False

    sent = False
    for row in events:
        event = dict(row)
        if str(event.get("status", "")).upper() != "ACTIVE":
            continue
        user_id = int(event.get("user_id", 0))
        if user_id <= 0:
            continue
        if is_user_locked(user_id) or not is_sub_active(user_id):
            continue
        if not is_notify_enabled(user_id, "ai_signals"):
            continue

        lang = _resolve_user_lang(user_id)
        message_text = format_signal_activation_message(
            lang=lang,
            symbol=symbol,
            side=str(signal.get("direction", "")).upper(),
            score=int(float(signal.get("score", 0.0) or 0.0)),
            entry_price=entry_price,
            sl=float(signal.get("sl", 0.0) or 0.0),
            tp1=float(signal.get("tp1", 0.0) or 0.0),
            tp2=float(signal.get("tp2", 0.0) or 0.0),
            market_regime=signal.get("btc_regime"),
            market_direction=signal.get("btc_direction"),
            market_trend=signal.get("btc_trend"),
        )
        try:
            await bot.send_message(
                user_id,
                message_text,
                disable_notification=_disable_notification_for_event(
                    user_id=user_id,
                    event_type="ACTIVE_CONFIRMED",
                ),
                reply_markup=InlineKeyboardMarkup(
                    inline_keyboard=[[build_binance_button(lang, symbol)]],
                ),
            )
            sent = True
        except Exception as exc:
            print(f"[ai_signals] Failed to send activation notification to {user_id}: {exc}")

    return sent


async def notify_signal_poi_touched(signal: dict) -> bool:
    if bot is None:
        return False

    module = str(signal.get("module", ""))
    symbol = str(signal.get("symbol", ""))
    ts_value = int(signal.get("sent_at", 0))
    if not module or not symbol or ts_value <= 0:
        return False

    touched_at = int(signal.get("poi_touched_at") or time.time())
    updated_rows = mark_signal_events_poi_touched(
        module=module,
        symbol=symbol,
        ts=ts_value,
        poi_touched_at=touched_at,
    )
    if updated_rows <= 0:
        return False

    events = list_signal_events_by_identity(module=module, symbol=symbol, ts=ts_value)
    if not events:
        return False

    sent = False
    for row in events:
        event = dict(row)
        if str(event.get("state", "")).upper() != "POI_TOUCHED":
            continue
        user_id = int(event.get("user_id", 0))
        if user_id <= 0:
            continue
        if is_user_locked(user_id) or not is_sub_active(user_id):
            continue
        if not is_notify_enabled(user_id, "ai_signals"):
            continue
        lang = _resolve_user_lang(user_id)
        message_text = format_signal_poi_touched_message(
            lang=lang,
            symbol=symbol,
            side=str(signal.get("direction", "")).upper(),
            score=int(float(signal.get("score", 0.0) or 0.0)),
            poi_from=float(signal.get("entry_from", 0.0) or 0.0),
            poi_to=float(signal.get("entry_to", 0.0) or 0.0),
            market_regime=signal.get("btc_regime"),
            market_direction=signal.get("btc_direction"),
            market_trend=signal.get("btc_trend"),
        )
        try:
            await bot.send_message(
                user_id,
                message_text,
                disable_notification=_disable_notification_for_event(
                    user_id=user_id,
                    event_type="POI_TOUCHED",
                ),
                reply_markup=InlineKeyboardMarkup(
                    inline_keyboard=[[build_binance_button(lang, symbol)]],
                ),
            )
            sent = True
        except Exception as exc:
            print(f"[ai_signals] Failed to send POI touched notification to {user_id}: {exc}")
    return sent


async def notify_signal_progress(signal: dict, event_type: str) -> bool:
    if bot is None:
        return False

    normalized = _normalize_signal_status(str(event_type or "").upper())
    if normalized != "TP1":
        return False

    module = str(signal.get("module", ""))
    symbol = str(signal.get("symbol", ""))
    ts_value = int(signal.get("sent_at", 0))
    if not module or not symbol or ts_value <= 0:
        return False

    events = list_signal_events_by_identity(module=module, symbol=symbol, ts=ts_value)
    if not events:
        return False

    sent = False
    for row in events:
        event = dict(row)
        user_id = int(event.get("user_id", 0))
        if user_id <= 0:
            continue
        if is_user_locked(user_id) or not is_sub_active(user_id):
            continue
        if not is_notify_enabled(user_id, "ai_signals"):
            continue

        lang = _resolve_user_lang(user_id)
        side = str(signal.get("direction", "")).upper()
        try:
            await bot.send_message(
                user_id,
                "\n".join(
                    [
                        i18n.t(lang, "SIGNAL_PROGRESS_TP1_HEADER"),
                        "",
                        f"{ui_symbol(symbol)} {side}",
                    ]
                ),
                disable_notification=False,
                reply_markup=InlineKeyboardMarkup(
                    inline_keyboard=[[build_binance_button(lang, symbol)]],
                ),
            )
            sent = True
        except Exception as exc:
            logger.warning(
                "[ai_signals] progress notify failed user_id=%s event=%s error=%s",
                user_id,
                normalized,
                exc,
            )
    return sent


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
            header = "📌 Итог: ⚪ EXP (сценарий устарел)"
            comment = f"{entry_label}, но условия не реализовались"

        lines.append(header)
        if status in {"NF", "EXP"}:
            ttl_minutes = int(event.get("ttl_minutes") or SIGNAL_TTL_SECONDS // 60)
            lines.append(f"⏱ Прошло ~{ttl_minutes} минут")
        elif finalized_at:
            lines.append(f"⏱ Время: {_format_event_time(finalized_at)}")
        elif last_checked_at:
            lines.append(f"⏱ Время: {_format_event_time(last_checked_at)}")
        if close_reason:
            lines.append(f"🧾 Причина: {close_reason}")
        lines.append(f"💬 Комментарий: {comment}")
        return lines

    if status == "ACTIVE":
        lines.extend(
            [
                "📌 Итог: 🟡 Активирован — ожидается результат",
                "💬 Сейчас: Активирован — ожидается результат",
            ]
        )
    else:
        ttl_minutes = int(event.get("ttl_minutes") or SIGNAL_TTL_SECONDS // 60)
        remaining = ttl_minutes * 60 - (now - created_at)
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
        ttl_minutes = int(event.get("ttl_minutes") or SIGNAL_TTL_SECONDS // 60)
        lines.append(f"• время жизни ~{ttl_minutes}м истекло — сценарий больше не актуален")
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
    symbol = ui_symbol(str(event.get("symbol") or ""))
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
        status_label = "OPEN / ожидает вход"
    elif status_raw == "ACTIVE":
        status_label = i18n.t(lang, "STATUS_ACTIVE_WAITING")
    lines.append(f"Статус: {status_label}")
    if status_raw == "OPEN":
        touched_label = "тронуто" if entry_touched else "не тронуто"
        lines.append(f"• entry: {entry_from:.4f}–{entry_to:.4f} ({touched_label})")
        ttl_minutes = int(event.get("ttl_minutes") or SIGNAL_TTL_SECONDS // 60)
        remaining = ttl_minutes * 60 - (int(time.time()) - created_at)
        lines.append(f"• до конца жизни: {_format_duration(remaining)}")
    elif status_raw == "ACTIVE":
        lines.append(f"• entry: {entry_from:.4f}–{entry_to:.4f} (тронуто)")
        if event.get("entry_price") is not None:
            lines.append(f"• цена входа: {_format_price(float(event.get('entry_price')))}")
    elif status_raw in {"NO_FILL"}:
        ttl_minutes = int(event.get("ttl_minutes") or SIGNAL_TTL_SECONDS // 60)
        lines.append(f"• прошло ~{ttl_minutes}м")
        lines.append("• цена не дошла до входа")
    elif status_raw in {"EXP", "EXPIRED"}:
        ttl_minutes = int(event.get("ttl_minutes") or SIGNAL_TTL_SECONDS // 60)
        lines.append(f"• прошло ~{ttl_minutes}м после активации")
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
    ttl_minutes = int(event.get("ttl_minutes") or SIGNAL_TTL_SECONDS // 60)
    ttl_sec = max(60, ttl_minutes * 60)
    cutoff_ts = min(now, created_at + ttl_sec)
    start_ms = created_at * 1000
    limit = max(200, int(ttl_sec / 300) + 20)
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
        elif now - created_at >= ttl_sec:
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
        _record_close_event(symbol=symbol, side=side, reason=_normalize_signal_status(status_value))
        logger.info(
            "[close_notify] close detected (refresh) event_id=%s symbol=%s side=%s reason=%s",
            event_id,
            symbol,
            side,
            _normalize_signal_status(status_value),
        )
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

    status_raw = str(event.get("result") or event.get("status") or "OPEN").upper()
    compact_lines = [
        f"📌 {event.get('symbol')} {event.get('side')} {score}",
        f"🕒 {_format_event_time(int(event.get('ts', 0)))}",
        f"POI: {float(event.get('poi_low')):.4f} - {float(event.get('poi_high')):.4f}",
        f"SL: {float(event.get('sl')):.4f}",
        f"TP1: {float(event.get('tp1')):.4f}",
        f"TP2: {float(event.get('tp2')):.4f}",
    ]
    if status_raw == "OPEN":
        compact_lines.append(
            i18n.t(
                lang,
                "ARCHIVE_DETAIL_LIFETIME",
                hours=max(1, int(round(float(event.get("ttl_minutes", SIGNAL_TTL_SECONDS // 60)) / 60))),
            )
        )

    lines = list(compact_lines)
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


def _signal_detail_expand_state_key(*, user_id: int, signal_id: int) -> str:
    return f"sig_detail_expand:{int(user_id)}:{int(signal_id)}"


def _get_signal_detail_expanded(*, user_id: int, signal_id: int) -> bool:
    raw = get_state(_signal_detail_expand_state_key(user_id=user_id, signal_id=signal_id), "0")
    return str(raw or "0") == "1"


def _set_signal_detail_expanded(*, user_id: int, signal_id: int, expanded: bool) -> None:
    set_state(_signal_detail_expand_state_key(user_id=user_id, signal_id=signal_id), "1" if expanded else "0")


def _format_archive_detail_view(event: dict, lang: str, *, expanded: bool) -> str:
    full_text = _format_archive_detail(event, lang)
    if expanded:
        return full_text
    compact_lines = full_text.split("\n")[:6]
    return "\n".join(compact_lines)


@dp.message(F.text.in_(i18n.all_labels("MENU_STATS")))
async def stats_menu(message: Message):
    lang = _resolve_user_lang(message.chat.id)
    await message.answer(
        i18n.t(lang, "STATS_PICK_TEXT"),
        reply_markup=stats_inline_kb(lang),
    )


@dp.callback_query(F.data.regexp(r"^history:(1d|7d|30d|all)(:page=\d+)?$"))
async def history_callback(callback: CallbackQuery):
    if callback.message is None or callback.from_user is None:
        return
    match = re.match(r"^history:(1d|7d|30d|all)(?::page=(\d+))?$", callback.data or "")
    if not match:
        lang = _resolve_user_lang(callback.from_user.id)
        await callback.answer(i18n.t(lang, "UNKNOWN_PERIOD"), show_alert=True)
        return
    time_window = match.group(1)
    page_value = max(1, int(match.group(2) or 1))
    try:
        await callback.answer()
    except Exception:
        pass
    try:
        await _render_history(callback=callback, time_window=time_window, page=page_value)
    except Exception:
        user_id = callback.from_user.id if callback.from_user else None
        logger.exception(
            "history_callback failed: window=%s page=%s user_id=%s",
            time_window,
            page_value,
            user_id,
        )
        lang = _resolve_user_lang(user_id)
        with suppress(Exception):
            await callback.message.answer(i18n.t(lang, "HISTORY_LOAD_ERROR"))


@dp.callback_query(F.data.regexp(r"^history:"))
async def history_unknown_period(callback: CallbackQuery):
    if callback.message is None or callback.from_user is None:
        return
    user_id = callback.from_user.id
    logger.warning("history_unknown_period: data=%s user_id=%s", callback.data, user_id)
    lang = _resolve_user_lang(user_id)
    await callback.answer(i18n.t(lang, "UNKNOWN_PERIOD"), show_alert=True)


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
                    text=_format_signal_list_row(
                        side=event.get("side"),
                        icon=status_icon,
                        score=event.get("score"),
                        symbol=event.get("symbol"),
                        created_at=event.get("created_at") or event.get("ts"),
                    ),
                    callback_data=f"history_open:{event.get('id')}",
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
                text=i18n.t(lang, "nav_prev_page"),
                callback_data=f"history:{time_window}:page={page}",
            )
        )
    if page < pages - 1:
        nav_row.append(
            InlineKeyboardButton(
                text=i18n.t(lang, "nav_next_page"),
                callback_data=f"history:{time_window}:page={page + 2}",
            )
        )
    if nav_row:
        rows.append(nav_row)

    rows.append(
        [
            InlineKeyboardButton(
                text=i18n.t(lang, "nav_back_to_periods"),
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
    expanded: bool,
    symbol: str,
) -> InlineKeyboardMarkup:
    rows: list[list[InlineKeyboardButton]] = []
    rows.append(
        [
            InlineKeyboardButton(
                text=i18n.t(lang, "BTN_COLLAPSE" if expanded else "BTN_EXPAND"),
                callback_data=f"sig_toggle:{event_id}",
            )
        ]
    )
    rows.append([build_binance_button(lang, symbol)])
    rows.append(
        [
            InlineKeyboardButton(
                text=i18n.t(lang, "nav_back_label"),
                callback_data=back_callback,
            )
        ]
    )
    return InlineKeyboardMarkup(inline_keyboard=rows)


@dp.callback_query(F.data.regexp(r"^history_open:\d+$"))
async def sig_open(callback: CallbackQuery):
    logger.warning("SIG_OPEN HANDLER FIRED: %s", callback.data)
    if callback.message is None or callback.from_user is None:
        return
    await callback.answer()
    try:
        logger.info("sig_open callback: %s", callback.data)
        event_id = int((callback.data or "").split(":", 1)[1])
        lang = get_user_lang(callback.from_user.id) or "ru"
        include_legacy = allow_legacy_for_user(is_admin_user=is_admin(callback.from_user.id))
        event_row = get_signal_by_id(event_id, include_legacy=include_legacy)
        if event_row is None:
            await callback.answer(i18n.t(lang, "legacy_hidden_notice"), show_alert=True)
            return
        event = dict(event_row)
        back_callback = "history:all:page=1"
        context = _get_history_context(callback.from_user.id)
        if context:
            stored_window, page, _ = context
            back_callback = f"history:{stored_window}:page={max(1, page)}"
        expanded = _get_signal_detail_expanded(user_id=callback.from_user.id, signal_id=event_id)
        detail_text = _format_archive_detail_view(event, lang, expanded=expanded)
        detail_markup = _archive_detail_kb(
            lang=lang,
            back_callback=back_callback,
            event_id=event_id,
            expanded=expanded,
            symbol=str(event.get("symbol", "")),
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


@dp.callback_query(F.data.regexp(r"^sig_toggle:\d+$"))
async def sig_toggle(callback: CallbackQuery):
    if callback.message is None or callback.from_user is None:
        return
    lang = get_user_lang(callback.from_user.id) or "ru"
    try:
        event_id = int((callback.data or "").split(":", 1)[1])
    except (ValueError, IndexError):
        await callback.answer()
        return

    include_legacy = allow_legacy_for_user(is_admin_user=is_admin(callback.from_user.id))
    event_row = get_signal_by_id(event_id, include_legacy=include_legacy)
    if event_row is None:
        await callback.answer(i18n.t(lang, "legacy_hidden_notice"), show_alert=True)
        return
    event = dict(event_row)

    current = _get_signal_detail_expanded(user_id=callback.from_user.id, signal_id=event_id)
    next_state = not current
    _set_signal_detail_expanded(user_id=callback.from_user.id, signal_id=event_id, expanded=next_state)

    back_callback = "history:all:page=1"
    context = _get_history_context(callback.from_user.id)
    if context:
        stored_window, page, _ = context
        back_callback = f"history:{stored_window}:page={max(1, page)}"

    with suppress(Exception):
        await callback.answer()
    await callback.message.edit_text(
        _format_archive_detail_view(event, lang, expanded=next_state),
        reply_markup=_archive_detail_kb(
            lang=lang,
            back_callback=back_callback,
            event_id=event_id,
            expanded=next_state,
            symbol=str(event.get("symbol", "")),
        ),
    )


@dp.callback_query(F.data.regexp(r"^sig_refresh:\d+$"))
async def sig_refresh(callback: CallbackQuery):
    if callback.from_user is None:
        return
    try:
        lang = get_user_lang(callback.from_user.id) or "ru"
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

        report_text = _format_refresh_report(refreshed, lang)

        if callback.message is None:
            return
        if callback.message.text and callback.message.text.startswith("📊"):
            context = _get_history_context(callback.from_user.id)
            if context:
                time_window, page, _ = context
                page, pages, events, outcome_counts, score_bucket_counts, avg_rr_90_100 = _get_history_page(
                    time_window=time_window,
                    page=page,
                )
                await _edit_message_with_chunks(
                    callback.message,
                    _format_archive_list(
                        lang,
                        time_window,
                        events,
                        page,
                        pages,
                        outcome_counts,
                        score_bucket_counts,
                        avg_rr_90_100,
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

        event = get_signal_event(
            user_id=None,
            event_id=event_id,
            include_legacy=allow_legacy_for_user(is_admin_user=True),
        )
        if event is None:
            await callback.message.answer(report_text)
            return
        back_callback = "hist_back"
        context = _get_history_context(callback.from_user.id)
        if context:
            time_window, page, _ = context
            back_callback = f"history:{time_window}:page={max(1, page + 1)}"
        with suppress(Exception):
            expanded = _get_signal_detail_expanded(user_id=callback.from_user.id, signal_id=event_id)
            await callback.message.edit_text(
                _format_archive_detail_view(dict(event), lang, expanded=expanded),
                reply_markup=_archive_detail_kb(
                    lang=lang,
                    back_callback=back_callback,
                    event_id=event_id,
                    expanded=expanded,
                    symbol=str(event.get("symbol", "")),
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


@dp.callback_query(F.data.regexp(r"^expand_signal:\d+$"))
async def sig_expand_callback(callback: CallbackQuery):
    if callback.from_user is None or callback.message is None:
        return
    match = re.match(r"^expand_signal:(\d+)$", callback.data or "")
    if not match:
        await callback.answer()
        return
    signal_id = int(match.group(1))
    lang = get_user_lang(callback.from_user.id) or "ru"
    include_legacy = allow_legacy_for_user(is_admin_user=is_admin(callback.from_user.id))
    event = get_signal_by_id(signal_id, include_legacy=include_legacy)
    if event is None or int(event["user_id"]) != callback.from_user.id:
        await callback.answer(i18n.t(lang, "SIGNAL_NOT_FOUND"), show_alert=True)
        return
    payload = _signal_payload_from_event(dict(event))
    try:
        full_text = _format_signal(payload, lang)
    except Exception:
        full_text = _format_compact_signal(payload, lang)
    await callback.message.edit_text(
        full_text,
        reply_markup=_expanded_signal_inline_kb(
            lang=lang,
            signal_id=signal_id,
            symbol=str(event.get("symbol", "")),
        ),
        parse_mode=None,
        disable_web_page_preview=True,
    )
    await callback.answer()


@dp.callback_query(F.data.regexp(r"^collapse_signal:\d+$"))
async def sig_collapse_callback(callback: CallbackQuery):
    if callback.from_user is None or callback.message is None:
        return
    match = re.match(r"^collapse_signal:(\d+)$", callback.data or "")
    if not match:
        await callback.answer()
        return
    signal_id = int(match.group(1))
    lang = get_user_lang(callback.from_user.id) or "ru"
    include_legacy = allow_legacy_for_user(is_admin_user=is_admin(callback.from_user.id))
    event = get_signal_by_id(signal_id, include_legacy=include_legacy)
    if event is None or int(event["user_id"]) != callback.from_user.id:
        await callback.answer(i18n.t(lang, "SIGNAL_NOT_FOUND"), show_alert=True)
        return
    payload = _signal_payload_from_event(dict(event))
    try:
        compact_text = _format_compact_signal(payload, lang)
    except Exception:
        compact_text = _format_compact_signal({"score": payload.get("score", 0)}, lang)
    await callback.message.edit_text(
        compact_text,
        reply_markup=_compact_signal_inline_kb(
            lang=lang,
            signal_id=signal_id,
            symbol=str(event.get("symbol", "")),
        ),
        parse_mode=None,
        disable_web_page_preview=True,
    )
    await callback.answer()


@dp.callback_query(F.data.regexp(r"^toggle_alerts:(regular|elite)$"))
async def toggle_alerts_callback(callback: CallbackQuery):
    if callback.from_user is None or callback.message is None:
        return
    match = re.match(r"^toggle_alerts:(regular|elite)$", callback.data or "")
    if not match:
        await callback.answer()
        return
    bucket = match.group(1)
    user_id = callback.from_user.id
    lang = get_user_lang(user_id) or "ru"
    pref_key = _alerts_pref_key_for_bucket(bucket)
    current_enabled = bool(get_user_pref(user_id, pref_key, 1))
    next_enabled = not current_enabled
    set_user_pref(user_id, pref_key, 1 if next_enabled else 0)

    score = 95 if bucket == "elite" else 89
    with suppress(Exception):
        await callback.message.edit_reply_markup(
            reply_markup=_status_toggle_inline_kb(lang=lang, score=score, enabled=next_enabled)
        )

    toast_key = "SIGNAL_STATUS_TOGGLE_TOAST_ON" if next_enabled else "SIGNAL_STATUS_TOGGLE_TOAST_OFF"
    await callback.answer(i18n.t(lang, toast_key))


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


@dp.callback_query(F.data.regexp(r"^pump_toggle:-?\d+:\d+$"))
async def pump_toggle_callback(callback: CallbackQuery):
    if callback.from_user is None or callback.message is None:
        return
    match = re.match(r"^pump_toggle:(-?\d+):(\d+)$", callback.data or "")
    if not match:
        await callback.answer()
        return
    chat_id = int(match.group(1))
    message_id = int(match.group(2))
    lang = get_user_lang(callback.from_user.id) or "ru"

    current_chat = int(callback.message.chat.id)
    current_message = int(callback.message.message_id)
    if current_chat != chat_id or current_message != message_id:
        await callback.answer(i18n.t(lang, "PUMP_TOGGLE_EXPIRED"), show_alert=True)
        return

    state = _get_pump_message_state(chat_id, message_id)
    if state is None:
        await callback.answer(i18n.t(lang, "PUMP_TOGGLE_EXPIRED"), show_alert=True)
        return

    state_lang = str(state.get("lang") or lang)
    is_expanded = bool(state.get("is_expanded", False))
    next_expanded = not is_expanded
    next_text = state.get("expanded") if next_expanded else state.get("collapsed")
    if not isinstance(next_text, str):
        await callback.answer(i18n.t(lang, "PUMP_TOGGLE_EXPIRED"), show_alert=True)
        return

    state["is_expanded"] = next_expanded
    state["ts"] = int(time.time())

    await callback.message.edit_text(
        next_text,
        parse_mode="Markdown",
        reply_markup=_pump_toggle_inline_kb(
            lang=state_lang,
            chat_id=chat_id,
            message_id=message_id,
            expanded=next_expanded,
        ),
    )
    toast_key = "PUMP_TOGGLE_EXPANDED" if next_expanded else "PUMP_TOGGLE_COLLAPSED"
    await callback.answer(i18n.t(state_lang, toast_key))


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
    return ui_symbol(symbol)


def _format_samples(samples: list[tuple[str, float]]) -> str:
    formatted = []
    for symbol, score in samples:
        score_str = f"{score:.2f}".rstrip("0").rstrip(".")
        formatted.append(f"{_short_symbol(symbol)}({score_str})")
    return ", ".join(formatted)


def _format_symbol_tags(samples: list[str], tag: str) -> str:
    if not samples:
        return "-"
    return ", ".join(f"{_short_symbol(symbol)}({tag})" for symbol in samples)


def _format_reason_counts(reasons: dict, top_n: int = 5) -> str:
    if not reasons:
        return "-"
    ordered = sorted(reasons.items(), key=lambda item: item[1], reverse=True)[:top_n]
    return ", ".join(f"{reason}={count}" for reason, count in ordered)


def _format_score_adjustments(adjustments: list[dict]) -> str:
    if not adjustments:
        return "—"
    parts = []
    for adj in adjustments:
        key = adj.get("key", "?")
        delta = adj.get("delta")
        if isinstance(delta, (int, float)):
            delta_label = f"{delta:+g}"
        else:
            delta_label = str(delta)
        parts.append(f"{key}({delta_label})")
    return ", ".join(parts)


def _sum_score_adjustments(adjustments: list[dict]) -> float:
    total = 0.0
    for adj in adjustments:
        delta = adj.get("delta")
        if isinstance(delta, (int, float)):
            total += float(delta)
    return total


def _format_final_score_sample(sample: dict, include_delta: bool = True) -> str:
    symbol = sample.get("symbol", "-")
    side = sample.get("side", "-")
    tf = sample.get("tf", "-")
    pre_score = sample.get("pre_score", "-")
    score_before = sample.get("score_before_adj", "-")
    score_after = sample.get("score_after_adj", "-")
    adjustments = sample.get("score_adjustments", []) or []
    delta_sum = _sum_score_adjustments(adjustments)
    delta_part = f" ({delta_sum:+g})" if include_delta else ""
    return (
        f"{symbol} {side} {tf} pre_score={pre_score} "
        f"score={score_before}->{score_after}{delta_part}"
    )


def _format_final_blockers(sample: dict) -> str:
    blockers = sample.get("blockers") or []
    if blockers:
        return ", ".join(blockers)
    adjustments = sample.get("score_adjustments", []) or []
    negative = [
        adj for adj in adjustments if isinstance(adj.get("delta"), (int, float)) and adj["delta"] < 0
    ]
    if not negative:
        return "—"
    return ", ".join(f"{adj.get('key')}({adj.get('delta'):+g})" for adj in negative)


def _format_confirm_retry_info(state: dict | None, lang: str) -> list[str]:
    if not state:
        return []
    info = state.get("confirm_retry")
    if not isinstance(info, dict):
        return []
    enabled = info.get("enabled", False)
    pending = info.get("pending", 0)
    sent = info.get("sent_after_retry", 0)
    dropped = info.get("dropped_after_retry", 0)
    samples = info.get("samples") or []
    sample_text = ", ".join(samples) if samples else "-"
    return [
        i18n.t(lang, "DIAG_CONFIRM_RETRY_HEADER"),
        i18n.t(
            lang,
            "DIAG_CONFIRM_RETRY_STATUS",
            enabled="true" if enabled else "false",
            pending=pending,
            sent=sent,
            dropped=dropped,
        ),
        i18n.t(lang, "DIAG_CONFIRM_RETRY_SAMPLES", samples=sample_text),
    ]


def _format_setup_near_miss_examples(examples: dict) -> str:
    if not examples:
        return "-"

    def _format_key(key: str, suffix: str) -> str:
        items = examples.get(key) or []
        parts = []
        for item in items:
            symbol = item.get("symbol", "-")
            value = item.get("value")
            limit = item.get("limit")
            if not isinstance(value, (int, float)) or not isinstance(limit, (int, float)):
                continue
            value_str = f"{value:.2f}".rstrip("0").rstrip(".")
            limit_str = f"{limit:.2f}".rstrip("0").rstrip(".")
            if suffix:
                value_str += suffix
                limit_str += suffix
            parts.append(f"{_short_symbol(symbol)}({value_str}/{limit_str})")
        return ", ".join(parts) if parts else "-"

    poi = _format_key("poi_dist", "%")
    rr = _format_key("rr", "")
    ema = _format_key("ema_dist", "%")
    return f"poi_dist: {poi} | rr: {rr} | ema_dist: {ema}"


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


async def _answer_long_message(
    message: Message,
    text: str,
    reply_markup: ReplyKeyboardMarkup | InlineKeyboardMarkup | None = None,
    max_len: int = 3900,
) -> None:
    if len(text) <= max_len:
        await message.answer(text, reply_markup=reply_markup)
        return

    lines = text.splitlines()
    chunks: list[str] = []
    current: list[str] = []
    current_len = 0

    for line in lines:
        line_len = len(line) + 1
        if current and current_len + line_len > max_len:
            chunks.append("\n".join(current))
            current = [line]
            current_len = len(line)
        else:
            current.append(line)
            current_len += line_len

    if current:
        chunks.append("\n".join(current))

    for idx, chunk in enumerate(chunks):
        await message.answer(
            chunk,
            reply_markup=reply_markup if idx == len(chunks) - 1 else None,
        )


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
    details.append(i18n.t(lang, "DIAG_MARKET_CURRENT", symbol=ui_symbol(current_symbol)))
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
            i18n.t(
                lang, "DIAG_AI_CONFIG_FINAL_THRESHOLD", value=cfg.final_score_threshold
            ),
            i18n.t(lang, "DIAG_AI_CONFIG_MIN_VOLUME", value=MIN_VOLUME_5M_USDT),
            i18n.t(lang, "DIAG_AI_CONFIG_PUMP_VOLUME", value=PUMP_VOLUME_MUL),
        ]
    )
    raw_final_threshold = os.getenv("FINAL_SCORE_THRESHOLD")
    raw_final_threshold_display = (
        raw_final_threshold if raw_final_threshold is not None else ""
    )
    details.append(
        "  • final_threshold_source: env FINAL_SCORE_THRESHOLD "
        f'raw="{raw_final_threshold_display}" '
        f"parsed={cfg.final_score_threshold:g}"
    )
    final_stage = (st.last_stats or {}).get("final_stage") if st else None
    final_fails = final_stage.get("fail_reasons", {}) if isinstance(final_stage, dict) else {}
    structure_penalty = final_stage.get("structure_penalty_neutral", 0) if isinstance(final_stage, dict) else 0
    fail_structure_opposite = final_fails.get("fail_structure_opposite", 0) if isinstance(final_fails, dict) else 0
    fail_setup_structure = final_fails.get("fail_setup_structure", 0) if isinstance(final_fails, dict) else 0
    structure_samples = final_stage.get("structure_samples") if isinstance(final_stage, dict) else None
    sample_line = structure_samples[0] if isinstance(structure_samples, list) and structure_samples else None
    details.append(i18n.t(lang, "DIAG_AI_STRUCTURE_TITLE"))
    details.append(i18n.t(lang, "DIAG_AI_STRUCTURE_MODE", value=AI_STRUCTURE_MODE))
    details.append(
        i18n.t(
            lang,
            "DIAG_AI_STRUCTURE_PENALTY",
            value=f"{AI_STRUCTURE_PENALTY_NEUTRAL:g}",
        )
    )
    details.append(
        i18n.t(
            lang,
            "DIAG_AI_STRUCTURE_HARD_FAIL",
            value="yes" if AI_STRUCTURE_HARD_FAIL_ON_OPPOSITE else "no",
        )
    )
    details.append(i18n.t(lang, "DIAG_AI_STRUCTURE_WINDOW", value=AI_STRUCTURE_WINDOW))
    details.append(
        i18n.t(
            lang,
            "DIAG_AI_STRUCTURE_COUNTS",
            neutral=structure_penalty,
            opposite=fail_structure_opposite,
            legacy=fail_setup_structure,
        )
    )
    if sample_line:
        details.append(i18n.t(lang, "DIAG_AI_STRUCTURE_SAMPLE", sample=sample_line))
    final_stage_debug = (st.last_stats or {}).get("final_stage") if st else None
    if isinstance(final_stage_debug, dict) and final_stage_debug.get("score_samples"):
        samples = final_stage_debug.get("score_samples", [])
        last_pass = next((s for s in reversed(samples) if s.get("final_pass")), None)
        last_fail = next((s for s in reversed(samples) if not s.get("final_pass")), None)
        near_miss = None
        near_miss_samples = final_stage_debug.get("near_miss_samples") or []
        if near_miss_samples:
            near_miss = near_miss_samples[0]
        threshold = final_stage_debug.get("threshold", cfg.final_score_threshold)
        checked = final_stage_debug.get("checked", 0)
        passed = final_stage_debug.get("passed", 0)
        failed = final_stage_debug.get("failed", 0)
        details.append(i18n.t(lang, "DIAG_FINAL_SCORE_HEADER"))
        details.append(i18n.t(lang, "DIAG_FINAL_SCORE_THRESHOLD", threshold=f"{threshold:.1f}"))
        details.append(
            i18n.t(
                lang,
                "DIAG_FINAL_SCORE_SUMMARY",
                checked=checked,
                passed=passed,
                failed=failed,
            )
        )
        if last_pass:
            details.append(
                i18n.t(
                    lang,
                    "DIAG_FINAL_SCORE_LAST_PASS",
                    sample=_format_final_score_sample(last_pass, include_delta=True),
                )
            )
            details.append(
                i18n.t(
                    lang,
                    "DIAG_FINAL_SCORE_ADJUSTMENTS",
                    adjustments=_format_score_adjustments(last_pass.get("score_adjustments", [])),
                )
            )
        if last_fail:
            details.append(
                i18n.t(
                    lang,
                    "DIAG_FINAL_SCORE_LAST_FAIL",
                    sample=_format_final_score_sample(last_fail, include_delta=False),
                )
            )
            details.append(
                i18n.t(
                    lang,
                    "DIAG_FINAL_SCORE_ADJUSTMENTS",
                    adjustments=_format_score_adjustments(last_fail.get("score_adjustments", [])),
                )
            )
            fail_reason = last_fail.get("final_fail_reason") or "—"
            details.append(
                i18n.t(
                    lang,
                    "DIAG_FINAL_SCORE_FAIL_REASON",
                    reason=fail_reason,
                )
            )
        if near_miss:
            missing = near_miss.get("missing", "-")
            score_after = near_miss.get("score_after_adj", "-")
            threshold_line = near_miss.get("final_threshold", threshold)
            details.append(
                i18n.t(
                    lang,
                    "DIAG_FINAL_SCORE_NEAR_MISS",
                    sample=(
                        f"{near_miss.get('symbol', '-')}"
                        f" score_after={score_after} threshold={threshold_line} missing={missing}"
                    ),
                )
            )
            details.append(
                i18n.t(
                    lang,
                    "DIAG_FINAL_SCORE_NEAR_MISS_BLOCKERS",
                    blockers=_format_final_blockers(near_miss),
                )
            )
    return _format_section(i18n.t(lang, "DIAG_SECTION_AI"), status_label, details, lang)


def _format_filters_section(st, lang: str) -> str:
    pre_score = (st.last_stats or {}).get("pre_score") if st else None
    final_stage = (st.last_stats or {}).get("final_stage") if st else None
    setup_stage = (st.last_stats or {}).get("setup_stage") if st else None
    status_label = _build_status_label(
        ok=bool(pre_score),
        warn=not pre_score,
        error=False,
        ok_text=i18n.t(lang, "DIAG_STATUS_WORKING"),
        warn_text=i18n.t(lang, "DIAG_STATUS_NO_DATA"),
        error_text=i18n.t(lang, "DIAG_STATUS_ERROR"),
    )
    details: list[str] = []
    details.append(
        i18n.t(
            lang,
            "DIAG_LIMITS_LINE",
            ema=f"{AI_EMA50_NEAR_PCT:g}",
            poi=f"{AI_POI_MAX_DISTANCE_PCT:g}",
            rr=f"{AI_MIN_RR:g}",
        )
    )
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
        bluechip_bypasses = pre_score.get("bluechip_bypasses", 0)
        bluechip_samples = pre_score.get("bluechip_samples") or []
        if bluechip_bypasses:
            details.append(
                i18n.t(
                    lang,
                    "DIAG_PRESCORE_BLUECHIP",
                    count=bluechip_bypasses,
                    samples=_format_symbol_tags(bluechip_samples, "bypass"),
                )
            )
    if setup_stage:
        details.append(
            i18n.t(
                lang,
                "DIAG_SETUP_STAGE_SUMMARY",
                checked=setup_stage.get("checked", 0),
                passed=setup_stage.get("passed", 0),
                failed=setup_stage.get("failed", 0),
            )
        )
        details.append(
            i18n.t(
                lang,
                "DIAG_SETUP_FAIL_REASONS",
                reasons=_format_reason_counts(setup_stage.get("fail_reasons", {})),
            )
        )
        details.append(
            i18n.t(
                lang,
                "DIAG_SETUP_NEAR_MISS_EXAMPLES",
                examples=_format_setup_near_miss_examples(
                    setup_stage.get("near_miss_examples", {})
                ),
            )
        )
        trend_stats = setup_stage.get("trend", {})
        if isinstance(trend_stats, dict):
            detected = trend_stats.get("detected", {}) if isinstance(trend_stats.get("detected"), dict) else {}
            details.append(i18n.t(lang, "DIAG_TREND_TITLE"))
            details.append(
                i18n.t(
                    lang,
                    "DIAG_TREND_MODE",
                    value="yes" if trend_stats.get("enabled") else "no",
                )
            )
            details.append(
                i18n.t(
                    lang,
                    "DIAG_TREND_DETECTED",
                    up=detected.get("up", 0),
                    down=detected.get("down", 0),
                    none=detected.get("none", 0),
                )
            )
            details.append(
                i18n.t(
                    lang,
                    "DIAG_TREND_SETUP_SUMMARY",
                    checked=trend_stats.get("setup_checked", 0),
                    passed=trend_stats.get("passed", 0),
                    failed=trend_stats.get("failed", 0),
                )
            )
            details.append(
                i18n.t(
                    lang,
                    "DIAG_TREND_FAIL_REASONS",
                    reasons=_format_reason_counts(trend_stats.get("fail_reasons", {})),
                )
            )
            sample = trend_stats.get("sample")
            if sample:
                details.append(i18n.t(lang, "DIAG_TREND_SAMPLE", sample=sample))
    if final_stage:
        details.append(
            i18n.t(
                lang,
                "DIAG_FINAL_STAGE_SUMMARY",
                checked=final_stage.get("checked", 0),
                passed=final_stage.get("passed", 0),
                failed=final_stage.get("failed", 0),
            )
        )
        details.append(
            i18n.t(
                lang,
                "DIAG_FINAL_FAIL_REASONS",
                reasons=_format_reason_counts(final_stage.get("fail_reasons", {})),
            )
        )
    if st and isinstance(st.state, dict):
        gate_enabled = str(os.getenv("SOFT_BTC_GATE_ENABLED", "0") or "0").strip().lower() in {"1", "true", "yes", "y"}
        details.append(f"BTC soft gate: {'enabled' if gate_enabled else 'disabled'}")
        btc_regime = st.state.get("btc_regime")
        if btc_regime:
            details.append(f"btc_regime: {btc_regime}")
        btc_reasons = st.state.get("btc_regime_reasons")
        if isinstance(btc_reasons, list) and btc_reasons:
            details.append(f"btc_regime_reasons: {', '.join(str(item) for item in btc_reasons[:4])}")
        skipped_total = int(st.state.get("skipped_by_btc_gate_total", 0) or 0)
        details.append(f"skipped_by_btc_gate_total: {skipped_total}")
        skipped_reasons = st.state.get("skipped_by_btc_gate_reasons")
        if isinstance(skipped_reasons, dict) and skipped_reasons:
            top_items = sorted(
                ((str(key), int(value or 0)) for key, value in skipped_reasons.items()),
                key=lambda item: item[1],
                reverse=True,
            )[:5]
            details.append(
                "skipped_by_btc_gate_reasons(top): "
                + ", ".join(f"{key}={value}" for key, value in top_items)
            )
        signals_by_regime = st.state.get("signals_by_regime")
        if isinstance(signals_by_regime, dict) and signals_by_regime:
            details.append(
                "signals_by_regime: "
                + ", ".join(
                    f"{name}={int(signals_by_regime.get(name, 0) or 0)}"
                    for name in ("TREND", "CHOP", "SQUEEZE", "RISK_OFF")
                )
            )
        skipped_by_regime_reason = st.state.get("skipped_by_regime_reason")
        if isinstance(skipped_by_regime_reason, dict) and skipped_by_regime_reason:
            details.append(
                "skipped_by_regime_reason: "
                + ", ".join(
                    f"{name}={int(skipped_by_regime_reason.get(name, 0) or 0)}"
                    for name in ("squeeze_up_block_short", "squeeze_down_block_long", "risk_off")
                )
            )
        last_regimes = st.state.get("last_regimes_10")
        if isinstance(last_regimes, list) and last_regimes:
            details.append("last_regimes_10: " + ", ".join(str(item) for item in last_regimes[-10:]))
        details.append(f"close_events_detected_total: {int(_CLOSE_NOTIFY_METRICS.get('close_events_detected_total', 0) or 0)}")
        details.append(f"close_notifications_sent_total: {int(_CLOSE_NOTIFY_METRICS.get('close_notifications_sent_total', 0) or 0)}")
        details.append(f"close_notifications_failed_total: {int(_CLOSE_NOTIFY_METRICS.get('close_notifications_failed_total', 0) or 0)}")
        last_close_event = _CLOSE_NOTIFY_METRICS.get("last_close_event")
        if isinstance(last_close_event, dict) and last_close_event:
            details.append(
                "last_close_event: "
                f"{last_close_event.get('symbol', '-')}/{last_close_event.get('side', '-')} "
                f"{last_close_event.get('reason', '-')} at {last_close_event.get('time', '-') }"
            )
    details.extend(_format_confirm_retry_info(st.state if st else None, lang))
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
        details.append(i18n.t(lang, "DIAG_CURRENT_COIN", symbol=ui_symbol(current)))
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
    await _answer_long_message(
        message,
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
        "is_test": True,
    }
    stats = await send_signal_to_all(
        signal_dict,
        allow_admin_bypass=True,
        bypass_cooldown=True,
        return_stats=True,
    )
    failure_samples = stats.get("error_samples", [])
    sample_lines = []
    for sample in failure_samples[:5]:
        sample_lines.append(
            f"- user_id={sample['user_id']} chat_id={sample['chat_id']} "
            f"{sample['error_class']}: {sample['error_text']}"
        )
    if not sample_lines:
        sample_lines.append("- none")

    report_lines = [
        "✅ Test AI broadcast complete",
        f"attempted: {stats['attempted']}",
        f"sent: {stats['sent']}",
        f"paywall: {stats['paywall']}",
        f"skipped_locked: {stats['locked']}",
        f"skipped_notifications_off: {stats['skipped_notifications_off']}",
        f"errors: {stats['errors']}",
        "samples:",
        *sample_lines,
    ]
    if not stats.get("admin_received", False):
        report_lines.append("⚠️ ADMIN did not receive test signal. Check error samples.")

    await message.answer("\n".join(report_lines))



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
            symbol=ui_symbol(symbol),
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

    def _int_value(value: object) -> int | None:
        if isinstance(value, bool):
            return None
        if isinstance(value, (int, float)):
            return int(value)
        if isinstance(value, str):
            try:
                return int(value)
            except ValueError:
                return None
        return None

    def _format_last_ai_signal_parts() -> tuple[str | None, str | None, str | None]:
        row = get_last_signal_audit("ai_signals")
        if not row:
            return None, None, None
        symbol = ui_symbol(str(row.get("symbol") or ""))
        direction = str(row.get("direction", "")).upper()
        side = "LONG" if direction == "LONG" else "SHORT" if direction == "SHORT" else direction
        sent_at = int(row.get("sent_at", 0) or 0)
        stamp = _format_event_time(sent_at) if sent_at else None
        return symbol or None, side or None, stamp

    binance_ts = max(
        (st.binance_last_success_ts for st in MODULES.values() if st.binance_last_success_ts),
        default=0,
    )
    extra = _parse_extra_kv(ai.extra or "") if ai else {}
    ai_state = ai.state if ai and isinstance(ai.state, dict) else {}
    ai_stats = ai.last_stats if ai and isinstance(ai.last_stats, dict) else {}

    lines: list[str] = [i18n.t(lang, "SYSTEM_STATUS_TITLE"), ""]

    connection_lines: list[str] = []
    if binance_ts:
        age = _sec_ago(binance_ts)
        if age <= 60:
            binance_ok_emoji = "✅"
            binance_status_text = i18n.t(lang, "SYSTEM_STATUS_CONN_OK")
        else:
            binance_ok_emoji = "⚠️"
            binance_status_text = i18n.t(lang, "SYSTEM_STATUS_CONN_WARN")
        connection_lines.append(
            i18n.t(
                lang,
                "SYSTEM_STATUS_BINANCE_LINE",
                status=f"{binance_ok_emoji} {binance_status_text}",
            )
        )
    else:
        connection_lines.append(
            i18n.t(
                lang,
                "SYSTEM_STATUS_BINANCE_LINE",
                status=f"❌ {i18n.t(lang, 'SYSTEM_STATUS_CONN_ERROR')}",
            )
        )

    last_cycle_ts = _int_value(ai_state.get("last_cycle_ts")) if ai_state else None
    if last_cycle_ts is None and ai and ai.last_tick:
        last_cycle_ts = int(ai.last_tick)
    if last_cycle_ts:
        connection_lines.append(
            i18n.t(
                lang,
                "SYSTEM_STATUS_LAST_CYCLE_LINE",
                seconds=_sec_ago(last_cycle_ts),
            )
        )

    if connection_lines:
        lines.extend(connection_lines)
        lines.append("")

    market_lines: list[str] = []
    setup_stage = ai_stats.get("setup_stage") if isinstance(ai_stats.get("setup_stage"), dict) else {}
    trend_stats = setup_stage.get("trend") if isinstance(setup_stage.get("trend"), dict) else {}
    detected = trend_stats.get("detected") if isinstance(trend_stats.get("detected"), dict) else None
    trend_up = _int_value(detected.get("up")) if detected else None
    trend_down = _int_value(detected.get("down")) if detected else None
    if trend_up is not None and trend_down is not None:
        if trend_down > trend_up:
            market_state = i18n.t(lang, "SYSTEM_STATUS_MARKET_STATE_DOWN")
        elif trend_up > trend_down:
            market_state = i18n.t(lang, "SYSTEM_STATUS_MARKET_STATE_UP")
        else:
            market_state = i18n.t(lang, "SYSTEM_STATUS_MARKET_STATE_NEUTRAL")
        market_lines.append(
            i18n.t(lang, "SYSTEM_STATUS_MARKET_STATE_LINE", state=market_state)
        )

    shorts_allowed = ai_state.get("shorts_allowed") if ai_state else None
    longs_allowed = ai_state.get("longs_allowed") if ai_state else None
    shorts_restricted = ai_state.get("shorts_restricted") if ai_state else None
    longs_restricted = ai_state.get("longs_restricted") if ai_state else None
    if isinstance(shorts_allowed, bool) and isinstance(longs_restricted, bool):
        if shorts_allowed and longs_restricted:
            priority = i18n.t(lang, "SYSTEM_STATUS_MARKET_PRIORITY_SHORT")
        else:
            priority = i18n.t(lang, "SYSTEM_STATUS_MARKET_PRIORITY_SELECTIVE")
        market_lines.append(
            i18n.t(lang, "SYSTEM_STATUS_MARKET_PRIORITY_LINE", priority=priority)
        )
    elif isinstance(longs_allowed, bool) and isinstance(shorts_restricted, bool):
        if longs_allowed and shorts_restricted:
            priority = i18n.t(lang, "SYSTEM_STATUS_MARKET_PRIORITY_LONG")
        else:
            priority = i18n.t(lang, "SYSTEM_STATUS_MARKET_PRIORITY_SELECTIVE")
        market_lines.append(
            i18n.t(lang, "SYSTEM_STATUS_MARKET_PRIORITY_LINE", priority=priority)
        )

    signals_sent_cycle = _int_value(extra.get("sent")) if extra else None
    if signals_sent_cycle is not None:
        activity = (
            i18n.t(lang, "SYSTEM_STATUS_MARKET_ACTIVITY_MODERATE")
            if signals_sent_cycle > 0
            else i18n.t(lang, "SYSTEM_STATUS_MARKET_ACTIVITY_LOW")
        )
        market_lines.append(
            i18n.t(lang, "SYSTEM_STATUS_MARKET_ACTIVITY_LINE", activity=activity)
        )

    if market_lines:
        lines.append(i18n.t(lang, "SYSTEM_STATUS_SECTION_MARKET"))
        lines.extend(market_lines)
        lines.append("")

    analysis_lines: list[str] = []
    market_symbols_total = _int_value(extra.get("universe")) if extra else None
    if market_symbols_total is None:
        market_symbols_total = _int_value(ai.total_symbols) if ai else None
    if market_symbols_total is not None:
        analysis_lines.append(
            i18n.t(lang, "SYSTEM_STATUS_MARKET_COVERAGE_LINE", count=market_symbols_total)
        )
    symbols_per_cycle = _int_value(ai_state.get("ai_chunk_current")) if ai_state else None
    if symbols_per_cycle is None:
        symbols_per_cycle = _int_value(extra.get("chunk")) if extra else None
    if symbols_per_cycle is None:
        symbols_per_cycle = _int_value(ai.checked_last_cycle) if ai else None
    if symbols_per_cycle is None:
        symbols_per_cycle = _int_value(ai_state.get("symbols_checked")) if ai_state else None
    if symbols_per_cycle is not None:
        analysis_lines.append(
            i18n.t(lang, "SYSTEM_STATUS_MARKET_CYCLE_LINE", count=symbols_per_cycle)
        )
    if ai_state:
        adapt_flag = ai_state.get("ai_adapt_enabled")
        safe_flag = ai_state.get("ai_safe_mode")
        if adapt_flag is not None or safe_flag is not None:
            safe_enabled = bool(adapt_flag) or bool(safe_flag)
            safe_mode_label = (
                i18n.t(lang, "SYSTEM_STATUS_SAFE_MODE_ON")
                if safe_enabled
                else i18n.t(lang, "SYSTEM_STATUS_SAFE_MODE_OFF")
            )
            analysis_lines.append(
                i18n.t(lang, "SYSTEM_STATUS_SAFE_MODE_LINE", mode=safe_mode_label)
            )
    if analysis_lines:
        lines.append(i18n.t(lang, "SYSTEM_STATUS_SECTION_AI"))
        lines.extend(analysis_lines)
        lines.append("")

    filter_lines: list[str] = []
    pre_score = ai_stats.get("pre_score") if isinstance(ai_stats.get("pre_score"), dict) else {}
    prescore_checked = _int_value(pre_score.get("checked")) if pre_score else None
    prescore_passed = _int_value(pre_score.get("passed")) if pre_score else None
    if prescore_checked is not None:
        filter_lines.append(
            i18n.t(lang, "SYSTEM_STATUS_PRESCORE_CHECKED_LINE", count=prescore_checked)
        )
    if prescore_passed is not None:
        filter_lines.append(
            i18n.t(lang, "SYSTEM_STATUS_PRESCORE_PASSED_LINE", count=prescore_passed)
        )
    if prescore_checked is not None and prescore_passed is not None:
        filter_lines.append(
            i18n.t(
                lang,
                "SYSTEM_STATUS_PRESCORE_FILTERED_LINE",
                count=max(0, prescore_checked - prescore_passed),
            )
        )

    if signals_sent_cycle is not None:
        suffix = ""
        if signals_sent_cycle == 0:
            suffix = f" {i18n.t(lang, 'SYSTEM_STATUS_SIGNALS_SENT_NONE')}"
        filter_lines.append(
            i18n.t(
                lang,
                "SYSTEM_STATUS_SIGNALS_SENT_LINE",
                count=signals_sent_cycle,
                suffix=suffix,
            )
        )

    if filter_lines:
        lines.append(i18n.t(lang, "SYSTEM_STATUS_SECTION_FILTERING"))
        lines.extend(filter_lines)
        lines.append("")

    last_signal_symbol, last_signal_side, last_signal_dt = _format_last_ai_signal_parts()
    lines.append(i18n.t(lang, "SYSTEM_STATUS_SECTION_LAST_SIGNAL"))
    if last_signal_symbol and last_signal_side and last_signal_dt:
        lines.append(
            i18n.t(
                lang,
                "SYSTEM_STATUS_LAST_SIGNAL_LINE",
                symbol=last_signal_symbol,
                side=last_signal_side,
                datetime=last_signal_dt,
            )
        )
    else:
        lines.append(i18n.t(lang, "SYSTEM_STATUS_LAST_SIGNAL_NONE"))
    lines.append("")

    pump_lines = [
        i18n.t(
            lang,
            "SYSTEM_STATUS_PUMP_STATUS_LINE",
            status=(
                i18n.t(lang, "SYSTEM_STATUS_PUMP_ACTIVE")
                if pd and pd.last_tick
                else i18n.t(lang, "SYSTEM_STATUS_PUMP_PAUSED")
            ),
        ),
        i18n.t(lang, "SYSTEM_STATUS_PUMP_IMPULSE_LINE"),
    ]
    lines.append(i18n.t(lang, "SYSTEM_STATUS_SECTION_PUMP"))
    lines.extend(pump_lines)

    return "\n".join(lines)


@dp.message(Command("status"))
async def status_cmd(message: Message):
    await message.answer(_format_user_bot_status(message.chat.id))


@dp.message(F.text.in_(i18n.all_labels("MENU_SYSTEM")))
async def system_menu(message: Message):
    await show_system_menu(message)


def _is_inversion_toggle_text(text: str | None) -> bool:
    if not isinstance(text, str):
        return False
    return text in {
        i18n.t("ru", "INVERSION_TOGGLE_BUTTON", state=i18n.t("ru", "INVERSION_STATE_ON")),
        i18n.t("ru", "INVERSION_TOGGLE_BUTTON", state=i18n.t("ru", "INVERSION_STATE_OFF")),
        i18n.t("en", "INVERSION_TOGGLE_BUTTON", state=i18n.t("en", "INVERSION_STATE_ON")),
        i18n.t("en", "INVERSION_TOGGLE_BUTTON", state=i18n.t("en", "INVERSION_STATE_OFF")),
    }


@dp.message(F.text.func(_is_inversion_toggle_text))
async def inversion_toggle_message(message: Message) -> None:
    lang = get_user_lang(message.chat.id) or "ru"
    if message.from_user is None or not is_admin(message.from_user.id):
        await message.answer(i18n.t(lang, "NO_ACCESS"))
        return

    new_state = not get_inversion_enabled()
    set_inversion_enabled(new_state)
    await message.answer(
        i18n.t(lang, "INVERSION_ENABLED_ALERT" if new_state else "INVERSION_DISABLED_ALERT"),
        reply_markup=build_system_menu_kb(lang, is_admin=True, inversion_enabled=new_state),
    )


async def show_system_menu(message: Message) -> None:
    lang = get_user_lang(message.chat.id) or "ru"
    is_admin_user = is_admin(message.from_user.id) if message.from_user else False
    await message.answer(
        i18n.t(lang, "SYSTEM_SECTION_TEXT"),
        reply_markup=build_system_menu_kb(
            lang,
            is_admin=is_admin_user,
            inversion_enabled=get_inversion_enabled() if is_admin_user else False,
        ),
    )


@dp.callback_query(F.data == "about_back")
async def about_back_callback(callback: CallbackQuery):
    lang = _resolve_user_lang(callback.from_user.id if callback.from_user else None)
    await callback.answer()
    if callback.message:
        await callback.message.answer(
            i18n.t(lang, "SYSTEM_SECTION_TEXT"),
            reply_markup=build_system_menu_kb(
                lang,
                is_admin=is_admin(callback.from_user.id) if callback.from_user else False,
                inversion_enabled=get_inversion_enabled() if callback.from_user and is_admin(callback.from_user.id) else False,
            ),
        )


@dp.callback_query(F.data == "system_back")
async def system_back_callback(callback: CallbackQuery):
    lang = _resolve_user_lang(callback.from_user.id if callback.from_user else None)
    await callback.answer()
    if callback.message:
        await callback.message.answer(
            i18n.t(lang, "SYSTEM_SECTION_TEXT"),
            reply_markup=build_system_menu_kb(
                lang,
                is_admin=is_admin(callback.from_user.id) if callback.from_user else False,
                inversion_enabled=get_inversion_enabled() if callback.from_user and is_admin(callback.from_user.id) else False,
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
                text=i18n.t(lang, "nav_back_label"),
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
                        text=i18n.t(lang, "nav_back_label"),
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
                    text=i18n.t(lang, "nav_back_label"),
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
    is_admin_user = is_admin(message.from_user.id) if message.from_user else False
    await message.answer(
        _format_user_bot_status(message.chat.id),
        reply_markup=build_system_menu_kb(
            get_user_lang(message.chat.id) or "ru",
            is_admin=is_admin_user,
            inversion_enabled=get_inversion_enabled() if is_admin_user else False,
        ),
    )


@dp.message(F.text.in_(i18n.all_labels("SYS_DIAG")))
async def diagnostics_button(message: Message):
    is_admin_user = is_admin(message.from_user.id) if message.from_user else False
    await message.answer(
        _format_user_bot_status(message.chat.id),
        reply_markup=build_system_menu_kb(
            get_user_lang(message.chat.id) or "ru",
            is_admin=is_admin_user,
            inversion_enabled=get_inversion_enabled() if is_admin_user else False,
        ),
    )


@dp.message(F.text.regexp(r"^\s*[◀⬅]️?\s*(Назад|Back)\s*$"))
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
            symbol = ui_symbol(str(row.get("symbol", "-")))
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


def _signal_symbol_text(symbol: str) -> str:
    return ui_symbol(symbol)


def _binance_spot_url(symbol: str) -> str:
    normalized = str(symbol or "").upper().replace("/", "").strip()
    if normalized.endswith("USDT"):
        base = normalized[:-4]
    else:
        base = normalized
    return f"https://www.binance.com/en/trade/{base}_USDT?type=spot"


def build_binance_button(lang: str, symbol: str) -> InlineKeyboardButton:
    return InlineKeyboardButton(
        text=i18n.t(lang, "btn_binance"),
        url=_binance_spot_url(symbol),
    )


def _compact_signal_inline_kb(*, lang: str, signal_id: int, symbol: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(
                    text=i18n.t(lang, "SIGNAL_BUTTON_EXPAND"),
                    callback_data=f"expand_signal:{signal_id}",
                ),
            ],
            [build_binance_button(lang, symbol)],
        ]
    )


def _expanded_signal_inline_kb(*, lang: str, signal_id: int, symbol: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(
                    text=i18n.t(lang, "SIGNAL_BUTTON_COLLAPSE"),
                    callback_data=f"collapse_signal:{signal_id}",
                ),
            ],
            [build_binance_button(lang, symbol)],
        ]
    )


LOUD_NOTIFICATION_EVENTS = {"NEW_SIGNAL", "ACTIVE_CONFIRMED"}
LOUD_CLOSE_EVENTS = {"TP1", "TP2"}
SILENT_CLOSE_EVENTS = {"SL"}


def _is_signal_entry_sound_enabled(user_id: int) -> bool:
    return bool(get_user_pref(user_id, "sound_signal_entry_enabled", 1))


def _disable_notification_for_event(*, user_id: int, event_type: str) -> bool:
    normalized_event = _normalize_signal_status(str(event_type or "").upper())
    if normalized_event in LOUD_NOTIFICATION_EVENTS:
        return not _is_signal_entry_sound_enabled(user_id)
    if normalized_event in LOUD_CLOSE_EVENTS:
        return False
    if normalized_event in SILENT_CLOSE_EVENTS:
        return True
    return True

def _format_signal(signal: Dict[str, Any], lang: str) -> str:
    entry_low, entry_high = signal["entry_zone"]
    symbol = signal["symbol"]
    symbol_text = _signal_symbol_text(symbol)

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
        lifetime_minutes=int(signal.get("ttl_minutes", SIGNAL_TTL_SECONDS // 60) or SIGNAL_TTL_SECONDS // 60),
        market_regime=signal.get("btc_regime"),
        market_direction=signal.get("btc_direction"),
        market_trend=signal.get("btc_trend"),
    )
    prefix = signal.get("title_prefix")
    if isinstance(prefix, dict):
        prefix = prefix.get(lang) or prefix.get("ru")
    if prefix:
        return f"{text}\n\n{prefix}"
    return text


def _market_regime_lines_for_signal(signal: Dict[str, Any], lang: str) -> list[str]:
    regime_text = i18n.t(
        lang,
        {
            "RISK_ON": "SIGNAL_MARKET_REGIME_TREND",
            "RISK_OFF": "SIGNAL_MARKET_REGIME_RISK_OFF",
            "CHOP": "SIGNAL_MARKET_REGIME_CHOP",
            "SQUEEZE": "SIGNAL_MARKET_REGIME_SQUEEZE",
        }.get(str(signal.get("btc_regime") or "").upper(), "SIGNAL_MARKET_REGIME_CHOP"),
    )
    direction_text = i18n.t(
        lang,
        {
            "UP": "SIGNAL_MARKET_DIR_UP",
            "DOWN": "SIGNAL_MARKET_DIR_DOWN",
            "NEUTRAL": "SIGNAL_MARKET_DIR_NEUTRAL",
        }.get(str(signal.get("btc_direction") or "").upper(), "SIGNAL_MARKET_DIR_NEUTRAL"),
    )
    trend_text = i18n.t(lang, "SIGNAL_TREND_YES") if bool(signal.get("btc_trend")) else i18n.t(lang, "SIGNAL_TREND_NO")
    return [
        i18n.t(lang, "SIGNAL_MARKET_REGIME_LINE", regime=regime_text, direction=direction_text),
        i18n.t(lang, "SIGNAL_MARKET_TREND_LINE", trend=trend_text),
    ]


def _format_compact_signal(signal: Dict[str, Any], lang: str) -> str:
    recommended_header = (
        "🔥 РЕКОМЕНДУЕМЫЙ СИГНАЛ\n"
        "Основной рабочий диапазон (Score 90–100)\n"
        "Используется для торговли"
        if lang == "ru"
        else "🔥 RECOMMENDED SIGNAL\n"
        "Primary working range (Score 90–100)\n"
        "Designed for active trading"
    )
    score = max(0, min(100, int(signal.get("score", 0) or 0)))
    symbol_text = _signal_symbol_text(str(signal.get("symbol") or ""))
    is_long = str(signal.get("direction") or "").lower() == "long"
    side_key = "SIGNAL_SHORT_SIDE_LONG" if is_long else "SIGNAL_SHORT_SIDE_SHORT"
    entry_low, entry_high = signal.get("entry_zone", (0.0, 0.0))

    scenario_tf = str(signal.get("tf") or signal.get("timeframe") or "1H").strip() or "1H"
    entry_tf = str(signal.get("entry_tf") or signal.get("confirm_tf") or scenario_tf).strip() or scenario_tf
    scenario_tf = scenario_tf.upper()

    if 80 <= score <= 89:
        lines = [
            i18n.t(lang, "SIGNAL_COMPACT_HIGH_RISK_HEADER"),
            i18n.t(lang, "SIGNAL_SHORT_80_89_SYMBOL_LINE", symbol=symbol_text),
            i18n.t(
                lang,
                "SIGNAL_SHORT_80_89_META_LINE",
                side="LONG" if is_long else "SHORT",
                timeframe=scenario_tf,
                entry_tf=entry_tf,
            ),
            "",
            f"POI: {_format_price(float(entry_low or 0.0))}–{_format_price(float(entry_high or 0.0))}",
            f"SL: {_format_price(float(signal.get('sl') or 0.0))}",
            f"TP1: {_format_price(float(signal.get('tp1') or 0.0))}",
            f"TP2: {_format_price(float(signal.get('tp2') or 0.0))}",
            i18n.t(lang, "SIGNAL_SHORT_80_89_SCORE_LINE", score=score),
            *_market_regime_lines_for_signal(signal, lang),
            i18n.t(
                lang,
                "SIGNAL_SHORT_80_89_TTL_LINE",
                minutes=max(1, int(signal.get("ttl_minutes") or SIGNAL_TTL_SECONDS // 60)),
            ),
        ]
    else:
        lines = [
            i18n.t(
                lang,
                "SIGNAL_SHORT_SYMBOL_SIDE_LINE",
                symbol=symbol_text,
                side=i18n.t(lang, side_key),
            ),
            i18n.t(
                lang,
                "SIGNAL_SHORT_POI_LINE",
                poi_from=_format_price(float(entry_low or 0.0)),
                poi_to=_format_price(float(entry_high or 0.0)),
            ),
            i18n.t(lang, "SIGNAL_SHORT_TP1_LINE", tp1=_format_price(float(signal.get("tp1") or 0.0))),
            i18n.t(lang, "SIGNAL_SHORT_TP2_LINE", tp2=_format_price(float(signal.get("tp2") or 0.0))),
            i18n.t(lang, "SIGNAL_SHORT_SL_LINE", sl=_format_price(float(signal.get("sl") or 0.0))),
            *_market_regime_lines_for_signal(signal, lang),
        ]

    prefix = signal.get("title_prefix")
    if isinstance(prefix, dict):
        prefix = prefix.get(lang) or prefix.get("ru")
    if prefix:
        lines.extend(["", str(prefix)])
    signal_text = "\n".join(lines)
    if score >= 90:
        return f"{recommended_header}\n\n{signal_text}"
    return signal_text


def _signal_payload_from_event(event: Dict[str, Any]) -> Dict[str, Any]:
    reason_raw = event.get("reason_json")
    reason = {}
    if isinstance(reason_raw, str) and reason_raw.strip():
        with suppress(Exception):
            parsed_reason = json.loads(reason_raw)
            if isinstance(parsed_reason, dict):
                reason = parsed_reason

    breakdown_raw = event.get("breakdown_json")
    breakdown = []
    if isinstance(breakdown_raw, str) and breakdown_raw.strip():
        with suppress(Exception):
            parsed_breakdown = json.loads(breakdown_raw)
            if isinstance(parsed_breakdown, list):
                breakdown = parsed_breakdown

    return {
        "symbol": str(event.get("symbol") or ""),
        "direction": "long" if str(event.get("side") or "").upper() == "LONG" else "short",
        "entry_zone": (float(event.get("poi_low") or 0.0), float(event.get("poi_high") or 0.0)),
        "sl": float(event.get("sl") or 0.0),
        "tp1": float(event.get("tp1") or 0.0),
        "tp2": float(event.get("tp2") or 0.0),
        "score": int(round(float(event.get("score") or 0))),
        "reason": reason,
        "score_breakdown": breakdown,
        "ttl_minutes": int(event.get("ttl_minutes") or SIGNAL_TTL_SECONDS // 60),
    }


def build_test_ai_signal(lang: str) -> str:
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
    text = _format_signal(signal_dict, lang)
    text = re.sub(r"</?(?:b|i|code)>", "", text, flags=re.IGNORECASE)
    text = text.replace("<", "‹").replace(">", "›")
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
) -> int | dict[str, Any]:
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
        "attempted": 0,
        "sent": 0,
        "locked": 0,
        "skipped_notifications_off": 0,
        "paywall": 0,
        "errors": 0,
        "error_blocked": 0,
        "error_invalid_chat": 0,
        "subscribers": len(subscribers),
        "error_samples": [],
        "admin_received": False,
    }
    if not subscribers:
        skipped_no_subs += 1
        print("[ai_signals] deliver: subs=0 (admin fallback may still receive message)")

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
    entry_zone = signal_dict.get("entry_zone") or (0.0, 0.0)
    try:
        entry_low, entry_high = entry_zone
    except (TypeError, ValueError):
        entry_low, entry_high = 0.0, 0.0
    direction = str(signal_dict.get("direction") or "").lower()

    admin_chat_id = ADMIN_CHAT_ID or ADMIN_USER_ID
    recipients = list(dict.fromkeys(subscribers + ([admin_chat_id] if admin_chat_id else [])))
    stats["attempted"] = len(recipients)
    log_tag = "test_broadcast" if is_test else "ai_signals"
    print(
        f"[{log_tag}] start recipients={len(recipients)} "
        f"subscribers={len(subscribers)} is_test={is_test}"
    )

    async def _send_with_safe_fallback(
        target_chat_id: int,
        text: str,
        *,
        event_type: str,
        reply_markup: InlineKeyboardMarkup | None = None,
        disable_web_page_preview: bool | None = None,
        parse_mode: str | None = None,
    ):
        send_kwargs: dict[str, Any] = {}
        if reply_markup is not None:
            send_kwargs["reply_markup"] = reply_markup
        if disable_web_page_preview is not None:
            send_kwargs["disable_web_page_preview"] = disable_web_page_preview
        send_kwargs["disable_notification"] = _disable_notification_for_event(
            user_id=target_chat_id,
            event_type=event_type,
        )
        send_kwargs["parse_mode"] = parse_mode
        try:
            return await bot.send_message(target_chat_id, text, **send_kwargs)
        except TelegramBadRequest as exc:
            error_text = str(exc).lower()
            if "can't parse entities" not in error_text:
                raise
            if send_kwargs.get("parse_mode") is None:
                raise
            send_kwargs["parse_mode"] = None
            return await bot.send_message(target_chat_id, text, **send_kwargs)

    for chat_id in recipients:
        if chat_id <= 0:
            stats["errors"] += 1
            sample = {
                "user_id": chat_id,
                "chat_id": chat_id,
                "error_class": "InvalidRecipient",
                "error_text": "chat_id is empty or zero",
            }
            if len(stats["error_samples"]) < 5:
                stats["error_samples"].append(sample)
            print(
                f"[{log_tag}] error user_id={chat_id} chat_id={chat_id} "
                "err=chat_id is empty or zero"
            )
            continue

        if is_user_locked(chat_id):
            stats["locked"] += 1
            print(f"[{log_tag}] skip locked user_id={chat_id} chat_id={chat_id}")
            continue

        if chat_id != admin_chat_id and not get_user_pref(chat_id, "ai_signals_enabled", 0):
            stats["skipped_notifications_off"] += 1
            print(
                f"[{log_tag}] skip notifications_off user_id={chat_id} "
                f"chat_id={chat_id}"
            )
            continue

        signal_score_value = int(round(float(signal_dict.get("score", 0) or 0)))
        is_regular_bucket = signal_score_value < 90
        if chat_id != admin_chat_id:
            bucket_pref_key = _alerts_pref_key_for_bucket(_alerts_bucket_from_score(signal_score_value))
            if not get_user_pref(chat_id, bucket_pref_key, 1):
                stats["skipped_notifications_off"] += 1
                print(
                    f"[{log_tag}] skip bucket_off={bucket_pref_key} user_id={chat_id} "
                    f"chat_id={chat_id}"
                )
                continue

        # индивидуальный cooldown на пользователя
        if not effective_bypass_cooldown:
            if not can_send(chat_id, "ai_signals", dedup_key, COOLDOWN_FREE_SEC):
                skipped_dedup += 1
                continue
        lang = get_user_lang(chat_id) or "ru"
        signal_reply_markup = None
        if is_test:
            message_text = build_test_ai_signal(lang)
            should_log = False
            kind = "signal"
        else:
            try:
                message_text = _format_compact_signal(signal_dict, lang)
            except Exception:
                fallback_symbol = _signal_symbol_text(str(signal_dict.get("symbol") or ""))
                fallback_side = (
                    i18n.t(lang, "SIGNAL_SHORT_SIDE_LONG")
                    if str(signal_dict.get("direction") or "").lower() == "long"
                    else i18n.t(lang, "SIGNAL_SHORT_SIDE_SHORT")
                )
                message_text = i18n.t(
                    lang,
                    "SIGNAL_SHORT_SYMBOL_SIDE_LINE",
                    symbol=fallback_symbol,
                    side=fallback_side,
                )
            should_log = True
            kind = "signal"
            if allow_admin_bypass and is_admin(chat_id):
                pass
            elif is_sub_active(chat_id):
                pass
            else:
                ensure_trial_defaults(chat_id)
                allowed, left = try_consume_trial(chat_id, "trial_ai_left", 1)
                if allowed:
                    message_text = message_text + i18n.t(
                        lang,
                        "TRIAL_SUFFIX_AI",
                        left=left,
                        limit=TRIAL_AI_LIMIT,
                    )
                else:
                    if not _should_send_paywall(chat_id, "ai", sent_at):
                        continue
                    message_text = i18n.t(lang, "PAYWALL_AI")
                    should_log = False
                    kind = "paywall"

        if kind == "paywall":
            stats["paywall"] += 1

        signal_parse_mode = None if is_test else "HTML"
        paywall_parse_mode = None if is_test else "HTML"

        try:
            if kind == "paywall":
                res = await _send_with_safe_fallback(
                    chat_id,
                    message_text,
                    event_type="STATUS",
                    parse_mode=paywall_parse_mode,
                    reply_markup=_subscription_kb_for("ai", lang),
                )
            else:
                res = await _send_with_safe_fallback(
                    chat_id,
                    message_text,
                    event_type="NEW_SIGNAL",
                    parse_mode=signal_parse_mode,
                    disable_web_page_preview=True,
                    reply_markup=signal_reply_markup,
                )
            stats["sent"] += 1
            if chat_id == admin_chat_id:
                stats["admin_received"] = True
            print(f"[{log_tag}] send ok user_id={chat_id} chat_id={chat_id}")
        except TelegramRetryAfter as exc:
            wait_seconds = float(exc.retry_after) + random.uniform(0.05, 0.2)
            print(
                f"[{log_tag}] floodwait user_id={chat_id} chat_id={chat_id} "
                f"retry_after={exc.retry_after:.2f}s"
            )
            await asyncio.sleep(wait_seconds)
            try:
                if kind == "paywall":
                    res = await _send_with_safe_fallback(
                        chat_id,
                        message_text,
                        event_type="STATUS",
                        parse_mode=paywall_parse_mode,
                        reply_markup=_subscription_kb_for("ai", lang),
                    )
                else:
                    res = await _send_with_safe_fallback(
                        chat_id,
                        message_text,
                        event_type="NEW_SIGNAL",
                        parse_mode=signal_parse_mode,
                        disable_web_page_preview=True,
                    )
                stats["sent"] += 1
                if chat_id == admin_chat_id:
                    stats["admin_received"] = True
                print(f"[{log_tag}] send ok after retry user_id={chat_id} chat_id={chat_id}")
            except Exception as retry_exc:
                stats["errors"] += 1
                sample = {
                    "user_id": chat_id,
                    "chat_id": chat_id,
                    "error_class": retry_exc.__class__.__name__,
                    "error_text": str(retry_exc),
                }
                if len(stats["error_samples"]) < 5:
                    stats["error_samples"].append(sample)
                print(
                    f"[{log_tag}] error user_id={chat_id} chat_id={chat_id} "
                    f"err={retry_exc.__class__.__name__}: {retry_exc}"
                )
                logger.exception(
                    f"[{log_tag}] retry failed user_id=%s chat_id=%s", chat_id, chat_id
                )
                continue
        except TelegramForbiddenError as exc:
            stats["errors"] += 1
            stats["error_blocked"] += 1
            set_user_pref(chat_id, "tg_blocked", 1)
            set_user_pref(chat_id, "ai_signals_enabled", 0)
            sample = {
                "user_id": chat_id,
                "chat_id": chat_id,
                "error_class": exc.__class__.__name__,
                "error_text": str(exc),
            }
            if len(stats["error_samples"]) < 5:
                stats["error_samples"].append(sample)
            print(
                f"[{log_tag}] error user_id={chat_id} chat_id={chat_id} "
                f"err={exc.__class__.__name__}: {exc}"
            )
            logger.exception(f"[{log_tag}] forbidden user_id=%s chat_id=%s", chat_id, chat_id)
            continue
        except TelegramBadRequest as exc:
            stats["errors"] += 1
            stats["error_invalid_chat"] += 1
            set_user_pref(chat_id, "invalid_chat_id", 1)
            sample = {
                "user_id": chat_id,
                "chat_id": chat_id,
                "error_class": exc.__class__.__name__,
                "error_text": str(exc),
            }
            if len(stats["error_samples"]) < 5:
                stats["error_samples"].append(sample)
            print(
                f"[{log_tag}] error user_id={chat_id} chat_id={chat_id} "
                f"err={exc.__class__.__name__}: {exc}"
            )
            logger.exception(
                f"[{log_tag}] bad request user_id=%s chat_id=%s", chat_id, chat_id
            )
            continue
        except Exception as exc:
            stats["errors"] += 1
            sample = {
                "user_id": chat_id,
                "chat_id": chat_id,
                "error_class": exc.__class__.__name__,
                "error_text": str(exc),
            }
            if len(stats["error_samples"]) < 5:
                stats["error_samples"].append(sample)
            print(
                f"[{log_tag}] error user_id={chat_id} chat_id={chat_id} "
                f"err={exc.__class__.__name__}: {exc}"
            )
            logger.exception(f"[{log_tag}] unexpected send error user_id=%s chat_id=%s", chat_id, chat_id)
            continue

        if is_test or not should_log:
            await asyncio.sleep(random.uniform(0.05, 0.15))
            continue

        try:
            event_id = insert_signal_event(
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
                ttl_minutes=int(signal_dict.get("ttl_minutes", SIGNAL_TTL_SECONDS // 60) or SIGNAL_TTL_SECONDS // 60),
            )
        except Exception as exc:
            print(f"[ai_signals] Failed to log signal event for {chat_id}: {exc}")
            await asyncio.sleep(random.uniform(0.05, 0.15))
            continue

        with suppress(Exception):
            await bot.edit_message_reply_markup(
                chat_id=chat_id,
                message_id=int(res.message_id),
                reply_markup=_compact_signal_inline_kb(
                    lang=lang,
                    signal_id=event_id,
                    symbol=symbol,
                ),
            )
        await asyncio.sleep(random.uniform(0.05, 0.15))
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
            collapsed_text = format_pump_message(signal, lang, expanded=False)
            expanded_text = format_pump_message(signal, lang, expanded=True)
            if prefix_key:
                prefix_text = i18n.t(lang, prefix_key)
                collapsed_text = f"{prefix_text}{collapsed_text}"
                expanded_text = f"{prefix_text}{expanded_text}"
            if suffix_key:
                suffix_text = i18n.t(lang, suffix_key)
                collapsed_text = f"{collapsed_text}\n\n{suffix_text}"
                expanded_text = f"{expanded_text}\n\n{suffix_text}"

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

            if is_admin_user or is_sub_active(chat_id):
                sent_message = await bot.send_message(chat_id, collapsed_text, parse_mode="Markdown")
                _save_pump_message_state(
                    chat_id=chat_id,
                    message_id=int(sent_message.message_id),
                    collapsed_text=collapsed_text,
                    expanded_text=expanded_text,
                    lang=lang,
                )
                increment_pumpdump_daily_count(chat_id, date_key)
                sent_count += 1
                recipient_count += 1
                continue

            ensure_trial_defaults(chat_id)
            allowed, left = try_consume_trial(chat_id, "trial_pump_left", 1)
            if allowed:
                trial_suffix = i18n.t(
                    lang,
                    "TRIAL_SUFFIX_PD",
                    left=left,
                    limit=TRIAL_PUMP_LIMIT,
                )
                collapsed_with_trial = collapsed_text + trial_suffix
                expanded_with_trial = expanded_text + trial_suffix
                sent_message = await bot.send_message(chat_id, collapsed_with_trial, parse_mode="Markdown")
                _save_pump_message_state(
                    chat_id=chat_id,
                    message_id=int(sent_message.message_id),
                    collapsed_text=collapsed_with_trial,
                    expanded_text=expanded_with_trial,
                    lang=lang,
                )
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
            mark_warn("pumpdump", "no symbols to scan")
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
            mark_warn("pumpdump", "no symbols to scan after exclusion")
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


def _normalize_regime_for_diag(btc_regime: str | None) -> str:
    regime = str(btc_regime or "CHOP").upper()
    if regime == "RISK_ON":
        return "TREND"
    if regime in {"RISK_OFF", "CHOP", "SQUEEZE"}:
        return regime
    return "CHOP"


def _regime_label_for_history(btc_regime: str | None, btc_direction: str | None) -> str:
    regime = str(btc_regime or "CHOP").upper()
    direction = str(btc_direction or "NEUTRAL").upper()
    if regime == "RISK_ON":
        if direction == "UP":
            return "TREND_UP"
        if direction == "DOWN":
            return "TREND_DOWN"
        return "TREND"
    if regime == "SQUEEZE":
        if direction == "UP":
            return "SQUEEZE_UP"
        if direction == "DOWN":
            return "SQUEEZE_DOWN"
        return "SQUEEZE"
    if regime == "RISK_OFF":
        return "RISK_OFF"
    return "CHOP"


async def ai_scan_once() -> None:
    start = time.time()
    BUDGET = 35
    print("[AI] scan_once start")
    global _AI_CHUNK_SIZE_CURRENT, _AI_STABLE_CYCLES
    try:
        module_state = MODULES.get("ai_signals")
        if module_state:
            module_state.state["last_cycle_ts"] = time.time()
        btc_context = await get_btc_regime()
        if module_state and isinstance(module_state.state, dict):
            module_state.state["soft_btc_gate_enabled"] = str(os.getenv("SOFT_BTC_GATE_ENABLED", "0") or "0").strip().lower() in {"1", "true", "yes", "y"}
            module_state.state["btc_regime"] = btc_context.get("btc_regime", "CHOP")
            module_state.state["btc_direction"] = btc_context.get("btc_direction", "NEUTRAL")
            module_state.state["btc_trend"] = bool(btc_context.get("btc_trend", False))
            module_state.state["btc_regime_reasons"] = btc_context.get("reasons", [])
            module_state.state.setdefault("skipped_by_btc_gate_total", 0)
            module_state.state.setdefault("skipped_by_btc_gate_reasons", {})
            module_state.state.setdefault("signals_by_regime", {"TREND": 0, "CHOP": 0, "SQUEEZE": 0, "RISK_OFF": 0})
            module_state.state.setdefault("skipped_by_regime_reason", {
                "squeeze_up_block_short": 0,
                "squeeze_down_block_long": 0,
                "risk_off": 0,
            })
            regime_history = module_state.state.get("last_regimes_10")
            if not isinstance(regime_history, list):
                regime_history = []
            regime_history.append(_regime_label_for_history(btc_context.get("btc_regime"), btc_context.get("btc_direction")))
            module_state.state["last_regimes_10"] = regime_history[-10:]
        reset_binance_metrics("ai_signals")
        reset_ticker_request_count("ai_signals")
        mark_tick("ai_signals", extra="сканирую рынок...")

        retried_close_notifications = await retry_pending_close_notifications(limit=200)

        retry_sent = 0
        cycle_regime_key = _normalize_regime_for_diag(btc_context.get("btc_regime"))

        def _inc_regime_signal() -> None:
            if not module_state or not isinstance(module_state.state, dict):
                return
            buckets = module_state.state.get("signals_by_regime")
            if not isinstance(buckets, dict):
                buckets = {"TREND": 0, "CHOP": 0, "SQUEEZE": 0, "RISK_OFF": 0}
            buckets[cycle_regime_key] = int(buckets.get(cycle_regime_key, 0) or 0) + 1
            module_state.state["signals_by_regime"] = buckets

        def _inc_regime_skip(reason: str | None) -> None:
            if not module_state or not isinstance(module_state.state, dict):
                return
            counters = module_state.state.get("skipped_by_regime_reason")
            if not isinstance(counters, dict):
                counters = {"squeeze_up_block_short": 0, "squeeze_down_block_long": 0, "risk_off": 0}
            if reason == "skip_btc_squeeze_up_block_short":
                counters["squeeze_up_block_short"] = int(counters.get("squeeze_up_block_short", 0) or 0) + 1
            elif reason == "skip_btc_squeeze_down_block_long":
                counters["squeeze_down_block_long"] = int(counters.get("squeeze_down_block_long", 0) or 0) + 1
            elif reason == "skip_btc_risk_off_long_score_lt90":
                counters["risk_off"] = int(counters.get("risk_off", 0) or 0) + 1
            module_state.state["skipped_by_regime_reason"] = counters

        with binance_request_context("ai_signals"):
            retry_signals = await process_confirm_retry_queue(
                diag_state=module_state.state if module_state else None
            )
        for signal in retry_signals:
            if time.time() - start > BUDGET:
                print("[AI] budget exceeded during confirm retry sends")
                break
            if get_inversion_enabled() and not bool(signal.get("_inversion_applied", False)):
                signal = apply_inversion(signal)
                signal["_inversion_applied"] = True
            allow_send, skip_reason, confirm_strict = apply_btc_soft_gate(signal, btc_context)
            if not allow_send:
                _inc_regime_skip(skip_reason)
                if module_state and isinstance(module_state.state, dict):
                    module_state.state["skipped_by_btc_gate_total"] = int(
                        module_state.state.get("skipped_by_btc_gate_total", 0) or 0
                    ) + 1
                    reasons = module_state.state.get("skipped_by_btc_gate_reasons")
                    if not isinstance(reasons, dict):
                        reasons = {}
                    reason_key = skip_reason or "skip_btc_unknown"
                    reasons[reason_key] = int(reasons.get(reason_key, 0) or 0) + 1
                    module_state.state["skipped_by_btc_gate_reasons"] = reasons
                print(
                    f"[ai_signals] BTC gate skip(retry) {signal.get('symbol', '')} "
                    f"{signal.get('direction', '')} score={signal.get('score', 0)} reason={skip_reason}"
                )
                continue
            _inc_regime_signal()
            if confirm_strict:
                signal["confirm_strict"] = True
            else:
                signal.pop("confirm_strict", None)
            signal["btc_regime"] = btc_context.get("btc_regime")
            signal["btc_direction"] = btc_context.get("btc_direction")
            signal["btc_trend"] = btc_context.get("btc_trend")
            try:
                update_current_symbol("ai_signals", signal.get("symbol", ""))
                print(
                    "[ai_signals] RETRY SEND "
                    f"{signal.get('symbol', '')} {signal.get('direction', '')} "
                    f"score={signal.get('score', 0)} confirm_strict={bool(signal.get('confirm_strict', False))}"
                )
                await send_signal_to_all(signal)
                meta = signal.get("meta") if isinstance(signal, dict) else None
                setup_id = meta.get("setup_id") if isinstance(meta, dict) else None
                if setup_id:
                    await register_confirm_retry_sent(setup_id)
                retry_sent += 1
            except Exception:
                logger.exception("[ai_signals] retry send failed for symbol=%s", signal.get("symbol", "-"))
                if module_state:
                    fails = module_state.state.get("fails") if isinstance(module_state.state.get("fails"), dict) else {}
                    fails["fail_retry_send"] = int(fails.get("fail_retry_send", 0) or 0) + 1
                    module_state.state["fails"] = fails
                continue

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
                progress_cb=lambda sym: update_current_symbol("ai_signals", sym),
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
        sent_count = retry_sent
        for signal in _select_signals_for_cycle(signals):
            if time.time() - start > BUDGET:
                print("[AI] budget exceeded, stopping early")
                break
            if get_inversion_enabled() and not bool(signal.get("_inversion_applied", False)):
                signal = apply_inversion(signal)
                signal["_inversion_applied"] = True
            score = signal.get("score", 0)
            if score < FREE_MIN_SCORE:
                continue
            allow_send, skip_reason, confirm_strict = apply_btc_soft_gate(signal, btc_context)
            if not allow_send:
                _inc_regime_skip(skip_reason)
                if module_state and isinstance(module_state.state, dict):
                    module_state.state["skipped_by_btc_gate_total"] = int(
                        module_state.state.get("skipped_by_btc_gate_total", 0) or 0
                    ) + 1
                    reasons = module_state.state.get("skipped_by_btc_gate_reasons")
                    if not isinstance(reasons, dict):
                        reasons = {}
                    reason_key = skip_reason or "skip_btc_unknown"
                    reasons[reason_key] = int(reasons.get(reason_key, 0) or 0) + 1
                    module_state.state["skipped_by_btc_gate_reasons"] = reasons
                print(
                    f"[ai_signals] BTC gate skip {signal.get('symbol', '')} "
                    f"{signal.get('direction', '')} score={score} reason={skip_reason}"
                )
                continue
            _inc_regime_signal()
            if confirm_strict:
                signal["confirm_strict"] = True
            else:
                signal.pop("confirm_strict", None)
            signal["btc_regime"] = btc_context.get("btc_regime")
            signal["btc_direction"] = btc_context.get("btc_direction")
            signal["btc_trend"] = btc_context.get("btc_trend")
            try:
                update_current_symbol("ai_signals", signal.get("symbol", ""))
                print(f"[ai_signals] DIRECT SEND {signal['symbol']} {signal['direction']} score={score} confirm_strict={bool(signal.get('confirm_strict', False))}")
                await send_signal_to_all(signal)
                meta = signal.get("meta") if isinstance(signal, dict) else None
                setup_id = meta.get("setup_id") if isinstance(meta, dict) else None
                if setup_id:
                    await register_confirm_retry_sent(setup_id)
                sent_count += 1
            except Exception:
                logger.exception("[ai_signals] direct send failed for symbol=%s", signal.get("symbol", "-"))
                if module_state:
                    fails = module_state.state.get("fails") if isinstance(module_state.state.get("fails"), dict) else {}
                    fails["fail_direct_send"] = int(fails.get("fail_direct_send", 0) or 0) + 1
                    module_state.state["fails"] = fails
                continue

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
            f"signals_found={len(signals)} sent={sent_count} deep_scans={deep_scans_done} close_retry_sent={retried_close_notifications}"
        )
        module_state = MODULES.get("ai_signals")
        if module_state:
            module_state.last_error = None
            module_state.state["last_exception"] = None
        mark_ok(
            "ai_signals",
            extra=(
                f"universe={total} chunk={len(chunk)} cursor={new_cursor} "
                f"signals_found={len(signals)} sent={sent_count} deep_scans={deep_scans_done} close_retry_sent={retried_close_notifications} "
                f"current={current_symbol or '-'} cycle={int(time.time() - start)}s "
                f"req={req_count} klines={klines_count} "
                f"klines_hits={cache_stats.get('hits')} klines_misses={cache_stats.get('misses')} "
                f"klines_inflight={cache_stats.get('inflight_awaits')} "
                f"ticker_req={ticker_count}"
            ),
        )
    except Exception as e:
        module_state = MODULES.get("ai_signals")
        if module_state:
            module_state.state["last_exception"] = traceback.format_exc()
        mark_error("ai_signals", str(e))
        logger.exception("[ai_signals] cycle crash: %s", e)
        logger.error(traceback.format_exc())
    finally:
        print("[AI] scan_once end")


# ===== ТОЧКА ВХОДА =====

async def main():
    global bot
    bot = Bot(token=BOT_TOKEN, default=DefaultBotProperties(parse_mode="HTML"))
    set_signal_result_notifier(notify_signal_result_short)
    set_signal_activation_notifier(notify_signal_activation)
    set_signal_poi_touched_notifier(notify_signal_poi_touched)
    set_signal_progress_notifier(notify_signal_progress)
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
