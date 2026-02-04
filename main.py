import asyncio
import json
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
    binance_watchdog,
    close_shared_session,
    get_shared_session,
    get_klines_cache_stats,
    reset_klines_cache_stats,
)
from pump_detector import (
    PUMP_CHUNK_SIZE,
    MIN_VOLUME_5M_USDT,
    PUMP_VOLUME_MUL,
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
    AI_MAX_DEEP_PER_CYCLE,
    AI_STAGE_A_TOP_K,
    PRE_SCORE_THRESHOLD,
    MIN_PRE_SCORE,
    FINAL_SCORE_THRESHOLD,
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
    upsert_watchlist_candidate,
    list_watchlist_for_scan,
    update_watchlist_after_signal,
    prune_watchlist,
    get_watchlist_counts,
    insert_signal_event,
    list_signal_events,
    count_signal_events,
    get_signal_outcome_counts,
    get_signal_score_bucket_counts,
    get_signal_event,
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
CANDIDATE_SCORE_MIN = int(os.getenv("CANDIDATE_SCORE_MIN", "40"))


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
    period_key: str,
    events: list[dict],
    page: int,
    pages: int,
    outcome_counts: dict,
    score_bucket_counts: dict[str, dict[str, int]],
) -> str:
    title = i18n.t(lang, "HISTORY_TITLE", period=_period_label(period_key, lang))
    lines = [title]
    lines.append("")
    lines.append(
        i18n.t(
            lang,
            "HISTORY_SUMMARY",
            passed=outcome_counts.get("passed", 0),
            failed=outcome_counts.get("failed", 0),
        )
    )
    def _score_bucket_line(bucket_key: str, label: str) -> str:
        bucket = score_bucket_counts.get(bucket_key, {})
        passed = int(bucket.get("passed", 0))
        failed = int(bucket.get("failed", 0))
        total = passed + failed
        percent = round((passed / total) * 100) if total > 0 else 0
        return i18n.t(
            lang,
            "HISTORY_SCORE_BUCKET_LINE",
            label=label,
            passed=passed,
            failed=failed,
            percent=percent,
        )

    tp1_total = outcome_counts.get("tp1", 0) + outcome_counts.get("tp2", 0)
    lines.extend(
        [
            "",
            i18n.t(
                lang,
                "HISTORY_STATS_TITLE",
                period=_period_label(period_key, lang),
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
        lines.append(i18n.t(lang, "HISTORY_NO_SIGNALS"))
        return "\n".join(lines)

    for idx, event in enumerate(events, start=1):
        status_icon = _status_icon(str(event.get("status", "")))
        lines.append(
            f"{status_icon} {idx}) Score {int(event.get('score', 0))} — "
            f"{event.get('symbol')} {event.get('side')} | "
            f"{_format_event_time(int(event.get('ts', 0)))}"
        )
    return "\n".join(lines)


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


@dp.callback_query(F.data.regexp(r"^history:(1d|7d|30d|all)$"))
async def history_callback(callback: CallbackQuery):
    if callback.message is None or callback.from_user is None:
        return
    period_key = callback.data.split(":", 1)[1]
    page = 1
    days = _period_days(period_key)
    since_ts = int(time.time()) - days * 86400 if days is not None else None
    total = count_signal_events(
        user_id=None,
        since_ts=since_ts,
        min_score=None,
    )
    print(f"[history] period={period_key} total={total}")
    pages = max(1, (total + 9) // 10)
    events_rows = list_signal_events(
        user_id=None,
        since_ts=since_ts,
        min_score=None,
        limit=10,
        offset=0,
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
    await callback.answer()
    lang = get_user_lang(callback.from_user.id) if callback.from_user else None
    lang = lang or "ru"
    await callback.message.edit_text(
        _format_archive_list(
            lang,
            period_key,
            events,
            page,
            pages,
            outcome_counts,
            score_bucket_counts,
        ),
        reply_markup=_archive_inline_kb(lang, period_key, page, pages, events),
    )


def _archive_inline_kb(
    lang: str,
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
                text=i18n.t(lang, "NAV_PREV"),
                callback_data=f"archive:list:{period_key}:{page - 1}",
            )
        )
    if page < pages:
        nav_row.append(
            InlineKeyboardButton(
                text=i18n.t(lang, "NAV_NEXT"),
                callback_data=f"archive:list:{period_key}:{page + 1}",
            )
        )
    if nav_row:
        rows.append(nav_row)
    rows.append(
        [
            InlineKeyboardButton(
                text=i18n.t(lang, "NAV_BACK"),
                callback_data=f"archive:back:{period_key}",
            )
        ]
    )
    return InlineKeyboardMarkup(inline_keyboard=rows)


def _archive_detail_kb(lang: str, period_key: str, page: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(
                    text=i18n.t(lang, "NAV_BACK"),
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
        user_id=None,
        since_ts=since_ts,
        min_score=None,
    )
    pages = max(1, (total + 9) // 10)
    if page > pages:
        page = pages
    events_rows = list_signal_events(
        user_id=None,
        since_ts=since_ts,
        min_score=None,
        limit=10,
        offset=(page - 1) * 10,
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
    await callback.answer()
    lang = get_user_lang(callback.from_user.id) if callback.from_user else None
    lang = lang or "ru"
    await callback.message.edit_text(
        _format_archive_list(
            lang,
            period_key,
            events,
            page,
            pages,
            outcome_counts,
            score_bucket_counts,
        ),
        reply_markup=_archive_inline_kb(lang, period_key, page, pages, events),
    )


@dp.callback_query(F.data.regexp(r"^archive:detail:(1d|7d|30d|all):\d+:\d+$"))
async def archive_detail(callback: CallbackQuery):
    if callback.message is None or callback.from_user is None:
        return
    _, _, period_key, page_raw, event_id_raw = callback.data.split(":")
    page = max(1, int(page_raw))
    event = get_signal_event(
        user_id=None,
        event_id=int(event_id_raw),
    )
    if event is None:
        lang = get_user_lang(callback.from_user.id) or "ru"
        await callback.answer(i18n.t(lang, "SIGNAL_NOT_FOUND"), show_alert=True)
        return
    await callback.answer()
    lang = get_user_lang(callback.from_user.id) if callback.from_user else None
    lang = lang or "ru"
    await callback.message.edit_text(
        _format_archive_detail(dict(event), lang),
        reply_markup=_archive_detail_kb(lang, period_key, page),
    )


@dp.callback_query(F.data.regexp(r"^archive:back:(1d|7d|30d|all)$"))
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
    lines = [i18n.t(lang, "DIAG_FAILS_TOP")]
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


def _format_market_hub(now: float, lang: str) -> str:
    # MARKET_HUB уже есть в проекте
    if MARKET_HUB.last_ok_at:
        ok_ago = int(now - MARKET_HUB.last_ok_at)
        last_tick = _human_ago(ok_ago, lang)
    else:
        last_tick = i18n.t(lang, "SYSTEM_STATUS_LAST_CYCLE_NO_DATA")

    err = MARKET_HUB.last_error or i18n.t(lang, "SYSTEM_STATUS_LAST_CYCLE_NO_DATA")
    symbols_count = len(getattr(MARKET_HUB, "_symbols", []) or [])
    return (
        f"{i18n.t(lang, 'DIAG_MARKET_HUB_TITLE')}\n"
        f"{i18n.t(lang, 'DIAG_MODULE_STATUS', status=i18n.t(lang, 'DIAG_STATUS_WORKING'))}\n"
        f"{i18n.t(lang, 'DIAG_LAST_TICK', tick=last_tick)}\n"
        f"{i18n.t(lang, 'DIAG_ERRORS', error=err)}\n"
        f"{i18n.t(lang, 'DIAG_ACTIVE_SYMBOLS', count=symbols_count)}"
    )


def _format_db_status(lang: str) -> str:
    path = get_db_path()
    if not os.path.exists(path):
        return (
            f"{i18n.t(lang, 'DIAG_DB_TITLE')}\n"
            f"{i18n.t(lang, 'DIAG_DB_PATH', path=path)}\n"
            f"{i18n.t(lang, 'DIAG_DB_MISSING')}"
        )
    size_bytes = os.path.getsize(path)
    mtime = datetime.fromtimestamp(os.path.getmtime(path))
    return (
        f"{i18n.t(lang, 'DIAG_DB_TITLE')}\n"
        f"{i18n.t(lang, 'DIAG_DB_PATH', path=path)}\n"
        f"{i18n.t(lang, 'DIAG_DB_SIZE', size=size_bytes)}\n"
        f"{i18n.t(lang, 'DIAG_DB_MODIFIED', mtime=f'{mtime:%Y-%m-%d %H:%M:%S}')}"
    )


def _format_module(key: str, st, now: float, lang: str) -> str:
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
        tick = _human_ago(int(now - st.last_tick), lang)
        status_line = i18n.t(lang, "DIAG_STATUS_WORKING")
    else:
        tick = i18n.t(lang, "DIAG_STATUS_NOT_STARTED")
        status_line = i18n.t(lang, "DIAG_STATUS_NOT_STARTED")

    ok_line = ""
    if st.last_ok:
        ok_line = i18n.t(
            lang,
            "DIAG_MODULE_LAST_OK",
            tick=_human_ago(int(now - st.last_ok), lang),
        )

    extra = _parse_extra_kv(st.extra or "")

    # Общие поля
    lines = [
        f"{st.name}",
        i18n.t(lang, "DIAG_MODULE_STATUS", status=status_line),
        i18n.t(lang, "DIAG_MODULE_LAST_CYCLE", tick=tick),
    ]
    if ok_line:
        lines.append(ok_line)

    if st.last_error:
        lines.append(i18n.t(lang, "DIAG_MODULE_ERROR", error=st.last_error))
    if st.last_warn:
        lines.append(i18n.t(lang, "DIAG_MODULE_WARNING", warning=st.last_warn))

    # Подписчики
    subs = extra.get("подписчиков")
    if subs is not None:
        lines.append("")
        lines.append(i18n.t(lang, "DIAG_USERS_HEADER"))
        lines.append(i18n.t(lang, "DIAG_SUBSCRIBERS_LINE", count=subs))

    # Сканирование / прогресс (берём из st + extra)
    # AI-сигналы
    if key == "ai_signals":
        lines.append("")
        lines.append(i18n.t(lang, "DIAG_MARKET_SCAN_HEADER"))
        universe = extra.get("universe") or (
            str(st.total_symbols) if st.total_symbols else None
        )
        if universe:
            lines.append(i18n.t(lang, "DIAG_MARKET_UNIVERSE", count=universe))
        chunk = extra.get("chunk")
        if chunk:
            lines.append(i18n.t(lang, "DIAG_MARKET_CHUNK", count=chunk))
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
            lines.append(
                i18n.t(lang, "DIAG_MARKET_POSITION_TOTAL", current=cur, total=universe)
            )
        elif cur:
            lines.append(i18n.t(lang, "DIAG_MARKET_POSITION", current=cur))
        use_btc_gate = bool(st.state.get("use_btc_gate", False))
        lines.append(f"• BTC gate: {'enabled' if use_btc_gate else 'disabled'}")
        btc_cache = get_cached_btc_context()
        btc_error = get_btc_context_last_error()
        btc_symbol = "BTCUSDT"
        if not use_btc_gate:
            btc_line = (
                f"{btc_symbol} (age=- ttl=- allow_longs=- allow_shorts=- reason=disabled)"
            )
        elif btc_cache is None:
            reason = f"error:{btc_error}" if btc_error else "pending"
            btc_line = (
                f"{btc_symbol} (age=- ttl=- allow_longs=- allow_shorts=- reason={reason})"
            )
        else:
            btc_ctx, age_sec, ttl_sec = btc_cache
            allow_longs = btc_ctx.get("allow_longs", False)
            allow_shorts = btc_ctx.get("allow_shorts", False)
            ctx_reason = btc_ctx.get("ctx_reason") or "-"
            btc_line = (
                f"{btc_symbol} (age={age_sec}s ttl={ttl_sec}s "
                f"allow_longs={allow_longs} allow_shorts={allow_shorts} "
                f"reason={ctx_reason})"
            )
        current_symbol = st.current_symbol or None
        extra_current = extra.get("current")

        def _is_btc_candidate(symbol: str | None) -> bool:
            return bool(symbol) and "BTC" in symbol
        btc_candidate_symbol = None
        market_scan_symbol = None
        if _is_btc_candidate(current_symbol):
            btc_candidate_symbol = current_symbol
        elif current_symbol:
            market_scan_symbol = current_symbol
        if btc_candidate_symbol is None and _is_btc_candidate(extra_current):
            btc_candidate_symbol = extra_current
        if market_scan_symbol is None and extra_current and not _is_btc_candidate(extra_current):
            market_scan_symbol = extra_current
        btc_candidate_symbol = btc_candidate_symbol or "-"
        market_scan_symbol = market_scan_symbol or "-"
        pos_display = cur or "-"
        total_display = universe or "-"
        lines.append(f"BTC context: {btc_line}")
        lines.append(f"BTC candidate scan: {btc_candidate_symbol}")
        lines.append(
            f"Market scan: {market_scan_symbol} ({pos_display} / {total_display})"
        )
        cyc = extra.get("cycle")
        if cyc:
            lines.append(i18n.t(lang, "DIAG_CYCLE_TIME", cycle=cyc))

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
            lines.append(i18n.t(lang, "DIAG_PRESCORE_HEADER"))
            lines.append(i18n.t(lang, "DIAG_PRESCORE_THRESHOLD", threshold=threshold_str))
            summary = f"• checked: {checked} | passed: {passed} | failed: {failed}"
            if pass_rate_str:
                summary += f" | pass rate: {pass_rate_str}"
            lines.append(summary)
            if failed_samples:
                lines.append(
                    i18n.t(
                        lang,
                        "DIAG_PRESCORE_FAILED",
                        samples=_format_samples(failed_samples),
                    )
                )
            if passed_samples:
                lines.append(
                    i18n.t(
                        lang,
                        "DIAG_PRESCORE_PASSED",
                        samples=_format_samples(passed_samples),
                    )
                )

        if st.fails_top or st.near_miss or st.universe_debug:
            lines.append("")
            if st.fails_top:
                if isinstance(st.fails_top, dict):
                    lines.append(_format_fails_top(st.fails_top, lang))
                else:
                    lines.append(str(st.fails_top))
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
            lines.append(i18n.t(lang, "DIAG_REQUESTS_HEADER"))
            if req:
                lines.append(i18n.t(lang, "DIAG_REQUESTS_MADE", count=req))
            if kl:
                lines.append(i18n.t(lang, "DIAG_CANDLES", count=kl))
            if hits or misses:
                lines.append(
                    i18n.t(
                        lang,
                        "DIAG_CACHE",
                        hits=hits or 0,
                        misses=misses or 0,
                    )
                )
            if inflight:
                lines.append(i18n.t(lang, "DIAG_INFLIGHT", count=inflight))
            if ticker_req:
                lines.append(i18n.t(lang, "DIAG_TICKER_REQ", count=ticker_req))
            if deep_scans:
                lines.append(i18n.t(lang, "DIAG_DEEP_SCAN", count=deep_scans))

    # Pump/Dump
    if key == "pumpdump":
        lines.append("")
        lines.append(i18n.t(lang, "DIAG_PUMP_HEADER"))
        prog = extra.get("progress")
        checked = extra.get("checked")
        found = extra.get("found")
        sent = extra.get("sent")
        if prog:
            lines.append(i18n.t(lang, "DIAG_PROGRESS", progress=prog))
        if checked:
            lines.append(i18n.t(lang, "DIAG_CHECKED", count=checked))
        if found is not None:
            lines.append(i18n.t(lang, "DIAG_FOUND", count=found))
        if sent is not None:
            lines.append(i18n.t(lang, "DIAG_SENT", count=sent))
        current = extra.get("current") or (st.current_symbol or None)
        if current:
            lines.append(i18n.t(lang, "DIAG_CURRENT_COIN", symbol=current))
        cyc = extra.get("cycle")
        if cyc:
            lines.append(i18n.t(lang, "DIAG_CYCLE_TIME", cycle=cyc))
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
            lines.append(
                i18n.t(
                    lang,
                    "DIAG_ROTATION",
                    flag=rotation_flag,
                    n=n_line,
                    cursor=cursor_line,
                )
            )
        if rotation_slice is not None:
            lines.append(
                i18n.t(lang, "DIAG_ROTATION_SLICE", size=rotation_slice)
            )
        if universe_size or rotation_added or final_candidates or scanned:
            lines.append(
                i18n.t(
                    lang,
                    "DIAG_UNIVERSE_LINE",
                    universe=universe_size or 0,
                    added=rotation_added or 0,
                    final=final_candidates or 0,
                    scanned=scanned or 0,
                )
            )

        if st.fails_top or st.universe_debug:
            lines.append("")
            if st.fails_top:
                if isinstance(st.fails_top, dict):
                    lines.append(_format_fails_top(st.fails_top, lang))
                else:
                    lines.append(str(st.fails_top))
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
            lines.append(i18n.t(lang, "DIAG_REQUESTS_HEADER"))
            if req:
                lines.append(i18n.t(lang, "DIAG_REQUESTS_MADE", count=req))
            if kl:
                lines.append(i18n.t(lang, "DIAG_CANDLES", count=kl))
            if hits or misses:
                lines.append(
                    i18n.t(
                        lang,
                        "DIAG_CACHE",
                        hits=hits or 0,
                        misses=misses or 0,
                    )
                )
            if inflight:
                lines.append(i18n.t(lang, "DIAG_INFLIGHT", count=inflight))
            if ticker_req:
                lines.append(i18n.t(lang, "DIAG_TICKER_REQ", count=ticker_req))

    # Binance секция (общая)
    lines.append("")
    lines.append(i18n.t(lang, "DIAG_REQUESTS_HEADER"))
    if st.binance_last_success_ts:
        lines.append(
            i18n.t(
                lang,
                "DIAG_BINANCE_LAST_SUCCESS",
                ago=_human_ago(int(now - st.binance_last_success_ts), lang),
            )
        )
    else:
        lines.append(i18n.t(lang, "DIAG_BINANCE_LAST_SUCCESS_NO_DATA"))
    lines.append(
        i18n.t(lang, "DIAG_BINANCE_TIMEOUTS", count=st.binance_consecutive_timeouts)
    )
    lines.append(
        i18n.t(lang, "DIAG_BINANCE_STAGE", stage=st.binance_current_stage or "—")
    )

    # стабильность
    lines.append("")
    lines.append(i18n.t(lang, "DIAG_STABILITY_HEADER"))
    lines.append(
        i18n.t(lang, "DIAG_SESSION_RESTARTS", count=st.binance_session_restarts)
    )

    return "\n".join(lines)


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
    blocks.append(f"{i18n.t(lang, 'DIAG_TITLE')}\n")
    use_btc_gate_raw = os.getenv("USE_BTC_GATE")
    use_btc_gate_value = "" if use_btc_gate_raw is None else use_btc_gate_raw
    blocks.append(f"BTC gate: {'enabled' if get_use_btc_gate() else 'disabled'}")
    blocks.append(f'USE_BTC_GATE raw: "{use_btc_gate_value}"')
    blocks.append(
        "CONFIG "
        f"AI_MAX_DEEP_PER_CYCLE={AI_MAX_DEEP_PER_CYCLE} "
        f"AI_STAGE_A_TOP_K={AI_STAGE_A_TOP_K} "
        f"PRE_SCORE_THRESHOLD={PRE_SCORE_THRESHOLD} "
        f"MIN_PRE_SCORE={MIN_PRE_SCORE} "
        f"FINAL_SCORE_THRESHOLD={FINAL_SCORE_THRESHOLD} "
        f"MIN_VOLUME_5M_USDT={MIN_VOLUME_5M_USDT} "
        f"PUMP_VOLUME_MUL={PUMP_VOLUME_MUL}"
    )
    ai_module = MODULES.get("ai_signals")
    if ai_module and ai_module.last_error:
        blocks.append(f"AI errors: {ai_module.last_error}")
    blocks.append("")
    blocks.append(_format_db_status(lang))
    blocks.append("")
    blocks.append(_format_market_hub(now, lang))
    blocks.append("")

    hidden = _hidden_status_modules()
    for key, st in MODULES.items():
        if key in hidden:
            continue
        if key not in ("ai_signals", "pumpdump"):
            continue
        blocks.append(_format_module(key, st, now, lang))
        blocks.append("\n" + ("—" * 22) + "\n")

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
    except Exception:
        pass


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
    except Exception:
        pass


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
    except Exception:
        pass
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
        except Exception:
            pass
        try:
            await message.copy_to(admin_chat_id)
        except Exception:
            pass
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
        market_mode=reason.get("market_mode"),
        market_bias=reason.get("market_bias"),
        btc_change_6h_pct=float(reason.get("btc_change_6h_pct", 0.0)),
        btc_atr_1h_pct=float(reason.get("btc_atr_1h_pct", 0.0)),
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
                return 0
            if side == "SHORT" and not btc_ctx.get("allow_shorts", False):
                print("[ai_signals] BTC gate blocked SHORT signal on refresh.")
                return 0

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
        if isinstance(res, Exception):
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
        elif volume_ratio >= 1.3:
            reason_scores["volume_spike"] = 20

    atr_now = compute_atr(candles_1h, 14)
    atr_prev = compute_atr(candles_1h[:-1], 14) if len(candles_1h) > 15 else None
    if atr_now and atr_prev and atr_prev > 0 and atr_now >= atr_prev * 1.10:
        reason_scores["atr"] = 20

    closes_1h = [float(c.close) for c in candles_1h]
    ema50 = compute_ema(closes_1h, 50)
    last_close = closes_1h[-1]
    if ema50 and last_close > 0:
        distance_pct = abs(last_close - ema50) / last_close * 100
        if distance_pct <= 1.0:
            reason_scores["near_poi"] = 30
        elif distance_pct <= 2.0:
            reason_scores["near_poi"] = 20

    last_candle = candles_1h[-1]
    if float(last_candle.open) > 0:
        change_pct = abs((float(last_candle.close) - float(last_candle.open)) / float(last_candle.open) * 100)
        if change_pct >= 3.0:
            reason_scores["pump"] = 30
        elif change_pct >= 2.0:
            reason_scores["pump"] = 20

    score = sum(reason_scores.values())
    if not reason_scores:
        return 0, ""
    reason = max(reason_scores.items(), key=lambda item: item[1])[0]
    return min(score, 100), reason


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
        req_count = get_request_count("pumpdump")
        klines_count = get_klines_request_count("pumpdump")
        cache_stats = get_klines_cache_stats("pumpdump")
        ticker_count = get_ticker_request_count("pumpdump")
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
        low_score_reasons: dict[str, int] = {}
        with binance_request_context("ai_signals"):
            for symbol in chunk:
                if time.time() - start > BUDGET:
                    print("[AI] budget exceeded, stopping early")
                    break
                update_current_symbol("ai_signals", symbol)
                score, reason = await _compute_candidate_score(symbol)
                if score < CANDIDATE_SCORE_MIN:
                    reason_key = reason or "no_score"
                    low_score_reasons[reason_key] = low_score_reasons.get(reason_key, 0) + 1
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
        if low_score_reasons:
            top_reasons = sorted(
                low_score_reasons.items(),
                key=lambda item: item[1],
                reverse=True,
            )[:5]
            reasons_str = ", ".join([f"{key}={count}" for key, count in top_reasons])
            print(f"[AI] below min score reasons: {reasons_str}")
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
        module_state.fails_top = stats.get("fails", {})
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
