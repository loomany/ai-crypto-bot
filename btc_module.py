import asyncio
import datetime as dt
from dataclasses import dataclass
from typing import Optional, List, Tuple, Dict, Iterable

from aiogram import Router, F
from aiogram.types import (
    Message,
    CallbackQuery,
    InlineKeyboardMarkup,
    InlineKeyboardButton,
    ReplyKeyboardMarkup,
    KeyboardButton,
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
from health import mark_tick, mark_ok, mark_error
from signal_filter import get_user_filter, btc_min_probability
from notifications_db import set_notify, list_enabled

# ============================================================
# Константы и базовые настройки
# ============================================================

BTC_SYMBOL = "BTCUSDT"
TIMEZONE_OFFSET_HOURS = 5  # например, Asia/Almaty (UTC+5)

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
    explanation: str = ""
    raw_score: Optional[int] = None




# ============================================================
# Клавиатура (только уведомления)
# ============================================================

def get_btc_main_keyboard() -> ReplyKeyboardMarkup:
    """
    Нижнее меню BTC:
    - Включить уведомления по BTC
    - Отключить уведомления по BTC
    - Назад в главное меню
    """
    kb = [
        [KeyboardButton(text="🔔 Включить уведомления по BTC")],
        [KeyboardButton(text="🚫 Отключить уведомления по BTC")],
        [KeyboardButton(text="⬅️ Главное меню")],
    ]
    return ReplyKeyboardMarkup(keyboard=kb, resize_keyboard=True)


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
        "Выбери действие:",
        reply_markup=get_btc_main_keyboard(),
    )


# ============================================================
# Хендлеры включения / отключения уведомлений
# ============================================================

@router.message(F.text == "🔔 Включить уведомления по BTC")
async def handle_btc_notify_on_message(message: Message):
    user_id = message.from_user.id
    set_notify(user_id, "btc", True)

    await message.answer(
        "✅ Уведомления по BTC включены.\n\n"
        "Бот будет автоматически присылать сигналы LONG/SHORT по BTCUSDT, "
        "как только появляется новый сильный сетап (интрадей, внутри 24 часов).",
        reply_markup=get_btc_main_keyboard(),
    )


@router.message(F.text == "🚫 Отключить уведомления по BTC")
async def handle_btc_notify_off_message(message: Message):
    user_id = message.from_user.id
    set_notify(user_id, "btc", False)

    await message.answer(
        "❌ Уведомления по BTC отключены.",
        reply_markup=get_btc_main_keyboard(),
    )


# ============================================================
# Реальный-тайм воркер: мониторинг BTC и мгновенные сигналы
# ============================================================

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

    # Codex: вызывать это из main.py, например:
    # asyncio.create_task(btc_realtime_signal_worker(bot))

    await asyncio.sleep(5)  # небольшая пауза после старта бота

    last_signature: Optional[Tuple[str, int]] = None
    last_checked_candle_close_time: Optional[int] = None
    last_signal_time: Optional[dt.datetime] = None
    MIN_SIGNAL_INTERVAL_SECONDS = 10 * 60

    while True:
        try:
            candles_5m = await fetch_klines(BTC_SYMBOL, "5m", 3)
            if len(candles_5m) < 2:
                mark_tick("btc", extra="нет достаточных свечей 5m")
                await asyncio.sleep(5)
                continue

            last_candle = candles_5m[-1]
            mark_ok("btc", extra=f"last_close={last_candle.close:.2f}")
            if (
                last_checked_candle_close_time is None
                or last_candle.close_time > last_checked_candle_close_time
            ):
                last_checked_candle_close_time = last_candle.close_time
                signal = await generate_btc_signal(desired_side=None)
                mark_tick("btc", extra=f"side={signal.side}, prob={signal.probability:.0f}")
                if signal.side in ("LONG", "SHORT"):
                    entry_mid = 0.0
                    if signal.entry_from and signal.entry_to:
                        entry_mid = (signal.entry_from + signal.entry_to) / 2.0
                    elif signal.entry_from:
                        entry_mid = signal.entry_from

                    signature = (signal.side, int(round(entry_mid)))
                    now = dt.datetime.utcnow()
                    interval_ok = (
                        last_signal_time is None
                        or (now - last_signal_time).total_seconds()
                        > MIN_SIGNAL_INTERVAL_SECONDS
                    )

                    if signature != last_signature and interval_ok:
                        last_signature = signature
                        last_signal_time = now

                        text = format_signal_message(signal, desired_side=signal.side)
                        user_ids = list_enabled("btc")

                        for user_id in user_ids:
                            try:
                                level = get_user_filter(user_id)
                                min_prob = btc_min_probability(level)
                                if int(signal.probability or 0) < min_prob:
                                    continue
                                await bot.send_message(chat_id=user_id, text=text)
                            except Exception:
                                continue

        except Exception as e:
            msg = f"error: {e}"
            print(f"[btc_realtime_signal_worker] {msg}")
            mark_error("btc", msg)

        await asyncio.sleep(2)


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
    threshold_pct = 0.6

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
    probability = min(95, abs(raw_score))
    rr = abs((tp1 - entry_to) / risk) if risk != 0 else None

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
        probability=probability,
        entry_from=entry_from,
        entry_to=entry_to,
        sl=sl,
        tp1=tp1,
        tp2=tp2,
        rr=rr,
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

    local_time_str = signal.timestamp.strftime("%Y-%m-%d %H:%M:%S")

    if signal.side == "NO_TRADE":
        return (
            f"⚠️ BTC / {desired_side or 'AUTO'}\n\n"
            f"Сейчас сильного сигнала нет.\n\n"
            f"Пояснение:\n{signal.explanation}"
        )

    emoji = "📈" if signal.side == "LONG" else "📉"
    side_str = "LONG" if signal.side == "LONG" else "SHORT"

    lines = [
        f"{emoji} BTC / {side_str}",
        "",
        f"Время сигнала: {local_time_str}",
        "Таймфреймы анализа: 30d, 7d, 1d, 1h, 15m, 5m",
        "",
        f"Вероятность сценария (оценка модели): {signal.probability:.0f}%",
    ]

    if signal.entry_from and signal.entry_to:
        lines.append(
            f"Зона входа: {signal.entry_from:,.2f} – {signal.entry_to:,.2f} USDT"
        )
    if signal.sl:
        lines.append(f"Стоп-лосс (SL): {signal.sl:,.2f} USDT")
    if signal.tp1:
        lines.append(f"Тейк-профит 1 (TP1): {signal.tp1:,.2f} USDT")
    if signal.tp2:
        lines.append(f"Тейк-профит 2 (TP2): {signal.tp2:,.2f} USDT")
    if signal.rr:
        lines.append(
            f"Ожидаемое соотношение риск/прибыль (R:R): ~1:{signal.rr:.1f}"
        )

    lines.append("")
    lines.append("Почему так решил:")
    lines.append(signal.explanation)
    lines.append("")
    lines.append(
        "⚠️ Это не инвестиционная рекомендация.\n"
        "Ты сам принимаешь решение и несёшь риск.\n"
        "Стратегия рассчитана на внутридневную торговлю (до 24 часов)."
    )

    return "\n".join(lines)
