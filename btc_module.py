import datetime as dt
from dataclasses import dataclass
from typing import Optional, List, Tuple, Dict, Iterable

from aiogram import Router, F
from aiogram.types import (
    Message,
    CallbackQuery,
    InlineKeyboardMarkup,
    InlineKeyboardButton,
)
from aiogram.fsm.context import FSMContext

from binance_client import Candle, fetch_klines, get_required_candles
from ai_patterns import analyze_ai_patterns
from market_regime import get_market_regime
from trading_core import (
    detect_trend_and_structure,
    find_key_levels,
    is_liquidity_sweep,
    is_volume_climax,
    _compute_rsi_series,
    detect_rsi_divergence,
    compute_atr,
    _nearest_level,
    compute_ema,
    compute_bollinger_bands,
    is_bb_extreme_reversal,
    analyze_orderflow,
    compute_score,
)
from health import mark_tick, mark_ok, mark_error, safe_worker_loop
from notifications_db import disable_notify, list_enabled
from message_templates import format_scenario_message
from keyboards import btc_inline_kb, paywall_inline_kb
from pro_db import pro_is
from texts import BTC_PAYWALL_TEXT
from trial_db import (
    FREE_TRIAL_LIMIT,
    trial_ensure_user,
    trial_get,
    trial_inc,
    trial_mark_paywall,
)

# ============================================================
# Константы и базовые настройки
# ============================================================

BTC_SYMBOL = "BTCUSDT"
TIMEZONE_OFFSET_HOURS = 5  # например, Asia/Almaty (UTC+5)
BTC_MIN_PROBABILITY = 70

router = Router(name="btc_module")


# ============================================================
# Модель сигнала (dataclass)
# ============================================================

@dataclass
class BTCSingal:
    timestamp: dt.datetime
    side: str  # "LONG" | "SHORT" | "NO_TRADE"
    probability: float  # 0–100
    entry_from: Optional[float] = None
    entry_to: Optional[float] = None
    sl: Optional[float] = None
    tp1: Optional[float] = None
    tp2: Optional[float] = None
    rr: Optional[float] = None
    trend_1d: Optional[str] = None
    trend_4h: Optional[str] = None
    rsi_1h: Optional[float] = None
    volume_ratio: Optional[float] = None
    explanation: str = ""
    raw_score: Optional[int] = None




# ============================================================
# Вход в BTC-меню
# ============================================================

@router.message(F.text == "/btc")
async def btc_menu_command(message: Message, state: FSMContext):
    """
    Команда /btc — показать меню управления сигналами BTC.
    """
    await message.answer(
        "📊 BTC-модуль (интрадей) — только BTCUSDT:\n\n"
        "• Автоматические сигналы LONG/SHORT\n"
        "• Сигнал приходит сразу, как только появляется сетап\n"
        "• Горизонт сделок: внутри 24 часов\n\n"
        "🔔 Авто-сигналы включаются кнопками ниже.",
        reply_markup=btc_inline_kb(),
    )


# ============================================================
# Реальный-тайм воркер: мониторинг BTC и мгновенные сигналы
# ============================================================

async def btc_scan_once(bot) -> None:
    if not hasattr(btc_scan_once, "state"):
        btc_scan_once.state = {
            "last_signature": None,
            "last_checked_candle_close_time": None,
            "last_signal_time": None,
            "symbols": [BTC_SYMBOL],
            "cursor": 0,
        }

    state = btc_scan_once.state
    min_interval = 7 * 60
    symbols = state["symbols"]
    cursor = state["cursor"]
    symbol = symbols[cursor]
    state["cursor"] = (cursor + 1) % len(symbols)

    candles_5m = await fetch_klines(symbol, "5m", 3)
    if len(candles_5m) < 2:
        mark_tick("btc", extra="нет достаточных свечей 5m")
        return

    last_candle = candles_5m[-1]
    mark_ok("btc", extra=f"last_close={last_candle.close:.2f}")
    last_checked = state["last_checked_candle_close_time"]
    if last_checked is not None and last_candle.close_time <= last_checked:
        return

    state["last_checked_candle_close_time"] = last_candle.close_time
    signal = await generate_btc_signal(desired_side=None)
    mark_tick("btc", extra=f"side={signal.side}, prob={signal.probability:.0f}")
    if signal.side not in ("LONG", "SHORT"):
        return

    entry_mid = 0.0
    if signal.entry_from and signal.entry_to:
        entry_mid = (signal.entry_from + signal.entry_to) / 2.0
    elif signal.entry_from:
        entry_mid = signal.entry_from

    signature = (signal.side, int(round(entry_mid)))
    now = dt.datetime.utcnow()
    last_signal_time: Optional[dt.datetime] = state["last_signal_time"]
    interval_ok = last_signal_time is None or (now - last_signal_time).total_seconds() > min_interval

    if signature == state["last_signature"] or not interval_ok:
        return

    state["last_signature"] = signature
    state["last_signal_time"] = now

    text = format_signal_message(signal, desired_side=signal.side)
    user_ids = list_enabled("btc")

    for user_id in user_ids:
        try:
            if int(signal.probability or 0) < BTC_MIN_PROBABILITY:
                continue
            if not pro_is(user_id):
                trial_ensure_user(user_id, "btc")
                used_count, paywall_sent = trial_get(user_id, "btc")
                if used_count >= FREE_TRIAL_LIMIT:
                    if not paywall_sent:
                        await bot.send_message(
                            chat_id=user_id,
                            text=BTC_PAYWALL_TEXT,
                            reply_markup=paywall_inline_kb(),
                        )
                        disable_notify(user_id, "btc")
                        trial_mark_paywall(user_id, "btc")
                    continue
                trial_inc(user_id, "btc")
            await bot.send_message(chat_id=user_id, text=text)
        except Exception:
            continue


async def btc_realtime_signal_worker(bot):
    """
    Фоновая задача:
    - постоянно мониторит рынок BTCUSDT
    - как только появляется новый сильный сигнал LONG или SHORT —
      сразу отправляет его всем пользователям с включёнными уведомлениями.

    ВАЖНО:
    - анализ должен быть интрадей (сделки до 24 часов)
    - Codex должен реализовать generate_btc_signal() так,
      чтобы он возвращал:
        • side = "LONG" / "SHORT" / "NO_TRADE"
        • probability, уровни, объяснение
    """

    await safe_worker_loop("btc", lambda: btc_scan_once(bot))


# ============================================================
# ЯДРО: генерация сигнала (структура, Codex → реализовать)
# ============================================================

async def generate_btc_signal(desired_side: Optional[str]) -> BTCSingal:
    """
    Главная функция генерации сигнала по BTC.

    desired_side:
        - "LONG"  → если пользователь хочет рассмотреть вход в лонг
        - "SHORT" → если пользователь хочет рассмотреть вход в шорт
        - None    → объективный автоанализ (для автоуведомлений)

    Задача Codex:
    1) Подключить Binance (futures или spot, предпочтительно futures BTCUSDT perpetual).
    2) Получать свечи по BTCUSDT за периоды:
        - 30 дней (таймфрейм 1d)
        - 7 дней  (1d)
        - 1 день  (1h)
        - 1 час   (15m)
        - 15 минут (5m)
        - 5 минут (1m или 5m)
    3) Рассчитать индикаторы:
        - EMA/SMA (например, 50/200)
        - RSI, MACD
        - ATR (для стопов)
        - объёмы, возможно taker buy/sell, open interest, funding (по желанию)
    4) Определить:
        - глобальный тренд (30/7/1d)
        - локальный тренд (1h)
        - разворот/сетап на младших ТФ (15m/5m)
    5) Посчитать score от -100 до +100:
        - score ≥ +60 → сильный LONG
        - score ≤ -60 → сильный SHORT
        - иначе → NO_TRADE
    6) Сделать так, чтобы сделки по сигналам были рассчитаны на
       отработку в пределах 24 часов (интрадей).
    7) Сформировать:
        - side ("LONG"/"SHORT"/"NO_TRADE")
        - probability = abs(score) в %
        - entry_from / entry_to
        - sl, tp1, tp2
        - rr (risk:reward)
        - explanation (можно через LLM на основе структурированных данных)
    """

    now = dt.datetime.utcnow() + dt.timedelta(hours=TIMEZONE_OFFSET_HOURS)

    candles = await get_required_candles(BTC_SYMBOL)
    candles_1d = candles.get("1d", [])
    candles_4h = candles.get("4h", [])
    candles_1h = candles.get("1h", [])
    candles_15m = candles.get("15m", [])
    candles_5m = candles.get("5m", [])

    if not all([candles_1d, candles_4h, candles_1h, candles_15m, candles_5m]):
        return BTCSingal(
            timestamp=now,
            side="NO_TRADE",
            probability=0,
            explanation="Нет данных с Binance для полного анализа (проверка соединения).",
        )

    current_price = candles_5m[-1].close

    daily_structure = detect_trend_and_structure(candles_1d)
    h4_structure = detect_trend_and_structure(candles_4h)
    h1_structure = detect_trend_and_structure(candles_1h)

    global_trend = daily_structure["trend"] if daily_structure["trend"] != "range" else h4_structure["trend"]
    local_trend = h1_structure["trend"]

    key_levels = find_key_levels(candles_1d)
    session_1h = candles_1h[-24:] if len(candles_1h) >= 24 else candles_1h
    session_15m = candles_15m[-32:] if len(candles_15m) >= 32 else candles_15m
    if session_1h:
        key_levels["highs"].append(max(c.high for c in session_1h))
        key_levels["lows"].append(min(c.low for c in session_1h))
    if session_15m:
        key_levels["highs"].append(max(c.high for c in session_15m))
        key_levels["lows"].append(min(c.low for c in session_15m))

    key_levels["highs"] = sorted(set(key_levels["highs"]))
    key_levels["lows"] = sorted(set(key_levels["lows"]))

    nearest_high, dist_high = _nearest_level(current_price, key_levels["highs"])
    nearest_low, dist_low = _nearest_level(current_price, key_levels["lows"])
    threshold_pct = 0.8

    candidate_side: Optional[str] = None
    level_touched: Optional[float] = None

    if nearest_low is not None and dist_low is not None and dist_low <= threshold_pct:
        candidate_side = "LONG"
        level_touched = nearest_low
    if nearest_high is not None and dist_high is not None and dist_high <= threshold_pct:
        if candidate_side is None or (dist_high is not None and dist_high < (dist_low or 10)):
            candidate_side = "SHORT"
            level_touched = nearest_high

    if desired_side and candidate_side and desired_side.upper() != candidate_side:
        candidate_side = None

    if not candidate_side:
        return BTCSingal(
            timestamp=now,
            side="NO_TRADE",
            probability=0,
            explanation="Цена не у ключевого уровня или сторона не совпала с запросом.",
        )

    sweep = is_liquidity_sweep(
        candles_5m[-6:] if len(candles_5m) >= 6 else candles_5m,
        level_touched,
        "long" if candidate_side == "LONG" else "short",
    )
    volume_spike = is_volume_climax(candles_5m)

    closes_15m = [c.close for c in candles_15m]
    closes_5m = [c.close for c in candles_5m]
    rsi_15m = _compute_rsi_series(closes_15m)
    rsi_5m = _compute_rsi_series(closes_5m)
    rsi_div = False
    if candidate_side == "LONG":
        rsi_div = detect_rsi_divergence(closes_15m, rsi_15m, "bullish") or detect_rsi_divergence(
            closes_5m, rsi_5m, "bullish"
        )
    else:
        rsi_div = detect_rsi_divergence(closes_15m, rsi_15m, "bearish") or detect_rsi_divergence(
            closes_5m, rsi_5m, "bearish"
        )

    atr_15m = compute_atr(candles_15m[-60:]) if len(candles_15m) >= 15 else None
    stop_buffer = atr_15m * 0.8 if atr_15m else current_price * 0.003

    if candidate_side == "LONG":
        sl = (level_touched or current_price) - max(stop_buffer, current_price * 0.005)
        entry_from = max((level_touched or current_price) * 0.998, current_price * 0.997)
        entry_to = current_price * 1.001
        risk = entry_to - sl
        tp1 = entry_to + risk * 2
        tp2 = entry_to + risk * 3
    else:
        sl = (level_touched or current_price) + max(stop_buffer, current_price * 0.005)
        entry_to = current_price * 0.999
        entry_from = current_price * 1.001
        risk = sl - entry_to
        tp1 = entry_to - risk * 2
        tp2 = entry_to - risk * 3

    atr_ok = True
    if atr_15m and risk > 0:
        min_stop = atr_15m * 0.5
        max_stop = atr_15m * 2.0
        atr_ok = min_stop <= risk <= max_stop

    # Bollinger экстремум на 15m/5m
    bb_extreme_15 = is_bb_extreme_reversal(
        candles_15m[-40:] if len(candles_15m) >= 40 else candles_15m,
        direction="long" if candidate_side == "LONG" else "short",
    )
    bb_extreme_5 = is_bb_extreme_reversal(
        candles_5m[-40:] if len(candles_5m) >= 40 else candles_5m,
        direction="long" if candidate_side == "LONG" else "short",
    )
    bb_extreme = bb_extreme_15 or bb_extreme_5

    # EMA50/EMA200 на 1H
    closes_1h = [c.close for c in candles_1h]
    ema50_1h = compute_ema(closes_1h, 50) if len(closes_1h) >= 50 else None
    ema200_1h = compute_ema(closes_1h, 200) if len(closes_1h) >= 200 else None
    ma_trend_ok = False
    if ema50_1h and ema200_1h:
        if candidate_side == "LONG" and current_price >= ema50_1h >= ema200_1h:
            ma_trend_ok = True
        if candidate_side == "SHORT" and current_price <= ema50_1h <= ema200_1h:
            ma_trend_ok = True

    # Ордерфлоу / киты (заглушка, Codex реализует внутри analyze_orderflow)
    orderflow = await analyze_orderflow(BTC_SYMBOL)

    # AI-паттерны и Market Regime
    pattern_info = await analyze_ai_patterns(BTC_SYMBOL, candles_1h, candles_15m, candles_5m)
    market_info = await get_market_regime()

    context = {
        "candidate_side": candidate_side,
        "global_trend": global_trend,
        "local_trend": local_trend,
        "near_key_level": True,
        "liquidity_sweep": sweep,
        "volume_climax": volume_spike,
        "rsi_divergence": rsi_div,
        "atr_ok": atr_ok,
        "bb_extreme": bb_extreme,
        "ma_trend_ok": ma_trend_ok,
        "orderflow_bullish": orderflow.get("orderflow_bullish", False),
        "orderflow_bearish": orderflow.get("orderflow_bearish", False),
        "whale_activity": orderflow.get("whale_activity", False),
        "ai_pattern_trend": pattern_info.get("pattern_trend"),
        "ai_pattern_strength": pattern_info.get("pattern_strength", 0),
        "market_regime": market_info.get("regime", "neutral"),
    }

    raw_score = compute_score(context)

    if abs(raw_score) < 70:
        return BTCSingal(
            timestamp=now,
            side="NO_TRADE",
            probability=0,
            explanation="Сильного разворотного сетапа нет (score < 70).",
            raw_score=raw_score,
        )

    side = "LONG" if raw_score >= 70 else "SHORT"
    score_for_message = min(100, abs(raw_score))
    entry_mid = (entry_from + entry_to) / 2
    rr = abs((tp1 - entry_mid) / (entry_mid - sl)) if (entry_mid - sl) != 0 else None

    closes_1h = [c.close for c in candles_1h]
    rsi_1h_series = _compute_rsi_series(closes_1h)
    rsi_1h_value = rsi_1h_series[-1] if rsi_1h_series else 50.0
    volumes_1h = [c.volume for c in candles_1h[-21:]]
    avg_volume = sum(volumes_1h[:-1]) / (len(volumes_1h) - 1) if len(volumes_1h) > 1 else 0.0
    last_volume = volumes_1h[-1] if volumes_1h else 0.0
    volume_ratio = last_volume / avg_volume if avg_volume > 0 else 0.0

    explanation_parts = [
        f"1D/4H тренд: {global_trend}, 1H локально: {local_trend}",
        f"Цена у уровня {level_touched:.2f}, поиск {side}",
        "Liquidity sweep присутствует" if sweep else "Снос ликвидности не подтверждён",
        "Объёмный всплеск на закрытии" if volume_spike else "Без объёмного климакса",
        "RSI дивергенция обнаружена" if rsi_div else "Дивергенция не подтверждена",
        "ATR в норме для стопа" if atr_ok else "ATR: стоп вне допустимого диапазона",
        "Bollinger: экстремум + возврат внутрь" if bb_extreme else "Bollinger: явного экстремума нет",
        "EMA50/EMA200 в сторону сделки" if ma_trend_ok else "EMA50/EMA200 не подтверждают тренд",
    ]

    if orderflow.get("orderflow_bullish") or orderflow.get("orderflow_bearish"):
        explanation_parts.append(
            f"Ордерфлоу в пользу {side} (дисбаланс крупных покупок/продаж)"
        )
    else:
        explanation_parts.append("Ордерфлоу/киты: явного перекоса нет или не учитывается.")

    return BTCSingal(
        timestamp=now,
        side=side,
        probability=score_for_message,
        entry_from=entry_from,
        entry_to=entry_to,
        sl=sl,
        tp1=tp1,
        tp2=tp2,
        rr=rr,
        trend_1d=global_trend,
        trend_4h=h4_structure["trend"],
        rsi_1h=rsi_1h_value,
        volume_ratio=volume_ratio,
        explanation="\n• " + "\n• ".join(explanation_parts),
        raw_score=raw_score,
    )


# ============================================================
# Форматирование сообщения сигнала
# ============================================================

def format_signal_message(signal: BTCSingal, desired_side: Optional[str]) -> str:
    """
    Формирование текста сигнала для отправки пользователю.
    """

    if signal.side == "NO_TRADE":
        return (
            f"⚠️ BTC / {desired_side or 'AUTO'}\n\n"
            f"Сейчас сильного сигнала нет.\n\n"
            f"Пояснение:\n{signal.explanation}"
        )

    if not all([signal.entry_from, signal.entry_to, signal.sl, signal.tp1, signal.tp2]):
        return (
            f"⚠️ BTC / {desired_side or 'AUTO'}\n\n"
            "Сейчас сильного сигнала нет.\n\n"
            "Пояснение:\nНедостаточно данных для форматирования сценария."
        )

    entry_mid = (signal.entry_from + signal.entry_to) / 2
    rr_value = (
        abs((signal.tp1 - entry_mid) / (entry_mid - signal.sl))
        if signal.sl and (entry_mid - signal.sl) != 0
        else 0.0
    )

    return format_scenario_message(
        symbol_text="BTC / USDT",
        side=signal.side,
        timeframe="1H",
        entry_from=signal.entry_from,
        entry_to=signal.entry_to,
        sl=signal.sl,
        tp1=signal.tp1,
        tp2=signal.tp2,
        score=int(signal.probability),
        trend_1d=signal.trend_1d,
        trend_4h=signal.trend_4h,
        rsi_1h=signal.rsi_1h or 50.0,
        volume_ratio=signal.volume_ratio or 0.0,
        rr=signal.rr if signal.rr is not None else rr_value,
        price_precision=2,
    )
