import asyncio
import datetime as dt
from dataclasses import dataclass
from typing import Optional, List, Dict, Sequence, Tuple

import aiohttp
from aiogram import Router, F
from aiogram.types import (
    Message,
    CallbackQuery,
    InlineKeyboardMarkup,
    InlineKeyboardButton,
)
from aiogram.fsm.context import FSMContext

BTC_SYMBOL = "BTCUSDT"
TIMEZONE_OFFSET_HOURS = 5  # Asia/Almaty (UTC+5), при необходимости подправить
AUTO_SIGNAL_INTERVAL_SECONDS = 15 * 60  # раз в 15 минут проверять сигнал

BINANCE_BASE_URL = "https://api.binance.com/api/v3"

router = Router(name="btc_module")


class InMemoryStorage:
    """
    Временное in-memory хранилище для демо.
    Codex должен заменить на базу данных.
    """

    def __init__(self):
        self.notifications: dict[int, bool] = {}
        self.signals_log: list["BTCSignal"] = []

    def set_notifications(self, user_id: int, enabled: bool) -> None:
        self.notifications[user_id] = enabled

    def is_notifications_enabled(self, user_id: int) -> bool:
        return self.notifications.get(user_id, False)

    def get_all_users_with_notifications(self) -> List[int]:
        return [uid for uid, enabled in self.notifications.items() if enabled]

    def add_signal(self, signal: "BTCSignal") -> None:
        self.signals_log.append(signal)


storage = InMemoryStorage()


@dataclass
class BTCSignal:
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


def get_btc_main_keyboard() -> InlineKeyboardMarkup:
    kb = [
        [
            InlineKeyboardButton(text="BTC / Long", callback_data="btc_long"),
            InlineKeyboardButton(text="BTC / Short", callback_data="btc_short"),
        ],
        [
            InlineKeyboardButton(text="Включить уведомление", callback_data="btc_notify_on"),
        ],
        [
            InlineKeyboardButton(text="Отключить уведомление", callback_data="btc_notify_off"),
        ],
    ]
    return InlineKeyboardMarkup(inline_keyboard=kb)


@router.message(F.text == "/btc")
async def btc_menu_command(message: Message, state: FSMContext):
    await message.answer(
        "📊 Меню BTC\n\n"
        "Здесь доступны разовые сигналы и автосигналы только по монете BTC (BTCUSDT).\n\n"
        "Выбери действие:",
        reply_markup=get_btc_main_keyboard(),
    )


@router.message(F.text.lower() == "btc")
async def btc_text_shortcut(message: Message, state: FSMContext):
    await btc_menu_command(message, state)


@router.message(F.text == "₿ BTC (intraday)")
async def btc_menu_from_button(message: Message, state: FSMContext):
    await btc_menu_command(message, state)


@router.callback_query(F.data == "btc_long")
async def handle_btc_long(callback: CallbackQuery):
    await callback.answer()
    await callback.message.answer("⏳ Анализирую BTC на предмет входа в LONG...")

    signal = await generate_btc_signal(desired_side="LONG")

    text = format_signal_message(signal, desired_side="LONG")
    await callback.message.answer(text)


@router.callback_query(F.data == "btc_short")
async def handle_btc_short(callback: CallbackQuery):
    await callback.answer()
    await callback.message.answer("⏳ Анализирую BTC на предмет входа в SHORT...")

    signal = await generate_btc_signal(desired_side="SHORT")

    text = format_signal_message(signal, desired_side="SHORT")
    await callback.message.answer(text)


@router.callback_query(F.data == "btc_notify_on")
async def handle_btc_notify_on(callback: CallbackQuery):
    await callback.answer()

    user_id = callback.from_user.id
    storage.set_notifications(user_id, True)

    await callback.message.answer(
        "✅ Уведомления по BTC включены.\n\n"
        "Бот будет автоматически присылать сигналы по BTC в течение дня "
        "(интрадей, горизонт сделок не более 24 часов)."
    )


@router.callback_query(F.data == "btc_notify_off")
async def handle_btc_notify_off(callback: CallbackQuery):
    await callback.answer()

    user_id = callback.from_user.id
    storage.set_notifications(user_id, False)

    await callback.message.answer("❌ Уведомления по BTC отключены.")


async def btc_auto_signal_worker(bot):
    await asyncio.sleep(5)

    while True:
        try:
            signal = await generate_btc_signal(desired_side=None)

            if signal.side in ("LONG", "SHORT"):
                text = format_signal_message(signal, desired_side=signal.side)

                user_ids = storage.get_all_users_with_notifications()
                for user_id in user_ids:
                    try:
                        await bot.send_message(chat_id=user_id, text=text)
                    except Exception:
                        continue

                storage.add_signal(signal)

        except Exception as e:
            print(f"[btc_auto_signal_worker] error: {e}")

        await asyncio.sleep(AUTO_SIGNAL_INTERVAL_SECONDS)


async def _fetch_json(url: str, params: dict | None = None) -> Optional[Dict]:
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(url, params=params, timeout=10) as resp:
                resp.raise_for_status()
                return await resp.json()
    except Exception as e:
        print(f"[btc_module] fetch error {url}: {e}")
        return None


async def _get_klines(symbol: str, interval: str, limit: int) -> Optional[List[Dict[str, float]]]:
    url = f"{BINANCE_BASE_URL}/klines"
    params = {"symbol": symbol, "interval": interval, "limit": limit}
    data = await _fetch_json(url, params)
    if not data:
        return None

    klines: List[Dict[str, float]] = []
    for k in data:
        klines.append(
            {
                "open": float(k[1]),
                "high": float(k[2]),
                "low": float(k[3]),
                "close": float(k[4]),
                "volume": float(k[5]),
            }
        )
    return klines


def _ema(values: Sequence[float], period: int) -> float:
    if not values:
        return 0.0
    k = 2 / (period + 1)
    ema_val = values[0]
    for v in values[1:]:
        ema_val = v * k + ema_val * (1 - k)
    return ema_val


def _rsi(closes: Sequence[float], period: int = 14) -> float:
    if len(closes) <= period:
        return 50.0

    gains: List[float] = []
    losses: List[float] = []
    for i in range(1, period + 1):
        diff = closes[i] - closes[i - 1]
        gains.append(max(diff, 0))
        losses.append(max(-diff, 0))

    avg_gain = sum(gains) / period
    avg_loss = sum(losses) / period if sum(losses) != 0 else 0.000001

    for i in range(period + 1, len(closes)):
        diff = closes[i] - closes[i - 1]
        gain = max(diff, 0)
        loss = max(-diff, 0)
        avg_gain = (avg_gain * (period - 1) + gain) / period
        avg_loss = (avg_loss * (period - 1) + loss) / period if avg_loss != 0 else 0.000001

    rs = avg_gain / avg_loss
    rsi = 100 - (100 / (1 + rs))
    return round(rsi, 2)


def _macd(closes: Sequence[float]) -> Tuple[float, float]:
    if len(closes) < 26:
        return 0.0, 0.0
    ema12 = _ema(closes[-26:], 12)
    ema26 = _ema(closes[-26:], 26)
    macd_val = ema12 - ema26
    signal_values = [macd_val]
    if len(closes) >= 35:
        macd_series = []
        for i in range(-35, -9):
            window = closes[:i]
            ema12_i = _ema(window, 12)
            ema26_i = _ema(window, 26)
            macd_series.append(ema12_i - ema26_i)
        signal_values = macd_series[-9:]
    signal_line = _ema(signal_values, 9)
    return macd_val, signal_line


def _atr(klines: Sequence[Dict[str, float]], period: int = 14) -> float:
    if len(klines) < 2:
        return 0.0
    trs: List[float] = []
    for i in range(1, len(klines)):
        high = klines[i]["high"]
        low = klines[i]["low"]
        prev_close = klines[i - 1]["close"]
        tr = max(high - low, abs(high - prev_close), abs(low - prev_close))
        trs.append(tr)
    if not trs:
        return 0.0
    window = trs[-period:]
    return sum(window) / len(window)


def _percent_change(values: Sequence[float]) -> float:
    if len(values) < 2:
        return 0.0
    first, last = values[0], values[-1]
    if first == 0:
        return 0.0
    return (last - first) / first * 100


def _volume_ratio(volumes: Sequence[float]) -> Tuple[float, float]:
    if len(volumes) < 5:
        return 1.0, 0.0
    avg = sum(volumes[:-1]) / (len(volumes) - 1)
    last = volumes[-1]
    ratio = last / avg if avg else 1.0
    return ratio, avg


def _clamp(value: float, min_v: float, max_v: float) -> float:
    return max(min_v, min(max_v, value))


async def _load_market_snapshots() -> Dict[str, List[Dict[str, float]]]:
    tasks = {
        "1d_60": asyncio.create_task(_get_klines(BTC_SYMBOL, "1d", 60)),
        "4h_90": asyncio.create_task(_get_klines(BTC_SYMBOL, "4h", 90)),
        "1h_120": asyncio.create_task(_get_klines(BTC_SYMBOL, "1h", 120)),
        "15m_96": asyncio.create_task(_get_klines(BTC_SYMBOL, "15m", 96)),
        "5m_144": asyncio.create_task(_get_klines(BTC_SYMBOL, "5m", 144)),
        "1m_240": asyncio.create_task(_get_klines(BTC_SYMBOL, "1m", 240)),
    }

    results = {name: await task for name, task in tasks.items()}
    if not all(results.values()):
        raise RuntimeError("Не удалось загрузить все таймфреймы с Binance")
    return results


async def generate_btc_signal(desired_side: Optional[str]) -> BTCSignal:
    now = dt.datetime.utcnow() + dt.timedelta(hours=TIMEZONE_OFFSET_HOURS)

    snapshots = await _load_market_snapshots()

    klines_1d = snapshots["1d_60"]
    klines_4h = snapshots["4h_90"]
    klines_1h = snapshots["1h_120"]
    klines_15m = snapshots["15m_96"]
    klines_5m = snapshots["5m_144"]
    klines_1m = snapshots["1m_240"]

    closes_30d = [k["close"] for k in klines_1d[-30:]]
    closes_7d = [k["close"] for k in klines_1d[-7:]]
    closes_1d = [k["close"] for k in klines_4h[-6:]]
    closes_1h = [k["close"] for k in klines_1h[-18:]]
    closes_15m = [k["close"] for k in klines_15m[-32:]]
    closes_5m = [k["close"] for k in klines_5m[-36:]]

    current_price = closes_1m[-1] if klines_1m else closes_5m[-1]

    rsi_1h = _rsi(closes_1h)
    rsi_15m = _rsi(closes_15m)
    rsi_5m = _rsi(closes_5m)

    macd_1h, _ = _macd([k["close"] for k in klines_1h])
    macd_15m, _ = _macd([k["close"] for k in klines_15m])

    vol_ratio_1h, avg_vol_1h = _volume_ratio([k["volume"] for k in klines_1h][-40:])
    vol_ratio_15m, avg_vol_15m = _volume_ratio([k["volume"] for k in klines_15m][-40:])

    atr_1h = _atr(klines_1h)
    atr_15m = _atr(klines_15m)
    intraday_atr = atr_15m or atr_1h or current_price * 0.003

    ema_fast_1h = _ema([k["close"] for k in klines_1h], 21)
    ema_slow_1h = _ema([k["close"] for k in klines_1h], 55)
    ema_fast_15m = _ema([k["close"] for k in klines_15m], 21)
    ema_slow_15m = _ema([k["close"] for k in klines_15m], 50)

    change_30d = _percent_change(closes_30d)
    change_7d = _percent_change(closes_7d)
    change_1d = _percent_change(closes_1d)
    change_1h = _percent_change(closes_1h)

    long_score = 0.0
    short_score = 0.0

    if change_30d > 4:
        long_score += 12
    elif change_30d < -4:
        short_score += 12

    if change_7d > 2:
        long_score += 10
    elif change_7d < -2:
        short_score += 10

    if change_1d > 1:
        long_score += 8
    elif change_1d < -1:
        short_score += 8

    if change_1h > 0.4:
        long_score += 6
    elif change_1h < -0.4:
        short_score += 6

    if ema_fast_1h > ema_slow_1h:
        long_score += 8
    else:
        short_score += 8

    if ema_fast_15m > ema_slow_15m:
        long_score += 6
    else:
        short_score += 6

    if macd_1h > 0:
        long_score += 6
    else:
        short_score += 6

    if macd_15m > 0:
        long_score += 4
    else:
        short_score += 4

    if 40 <= rsi_1h <= 60:
        long_score += 6
    elif rsi_1h > 70:
        short_score += 8
    elif rsi_1h < 35:
        long_score += 4

    if rsi_15m > 70 or rsi_5m > 70:
        short_score += 6
    elif 35 <= rsi_15m <= 65:
        long_score += 5

    if vol_ratio_1h >= 1.3:
        long_score += 5
    elif vol_ratio_1h < 0.8:
        short_score += 3

    if vol_ratio_15m >= 1.4:
        long_score += 4
    elif vol_ratio_15m < 0.8:
        short_score += 4

    if desired_side == "LONG":
        long_score += 3
    elif desired_side == "SHORT":
        short_score += 3

    raw_score = long_score - short_score
    raw_score = _clamp(raw_score, -100, 100)

    if raw_score >= 60:
        side = "LONG"
    elif raw_score <= -60:
        side = "SHORT"
    else:
        side = "NO_TRADE"

    entry_buffer = intraday_atr * 0.6
    tp_buffer = intraday_atr * 2.4

    if side == "LONG":
        entry_from = current_price - entry_buffer
        entry_to = current_price - entry_buffer * 0.3
        sl = entry_from - intraday_atr
        tp1 = current_price + tp_buffer
        tp2 = current_price + tp_buffer * 1.6
    elif side == "SHORT":
        entry_from = current_price + entry_buffer * 0.3
        entry_to = current_price + entry_buffer
        sl = entry_to + intraday_atr
        tp1 = current_price - tp_buffer
        tp2 = current_price - tp_buffer * 1.6
    else:
        entry_from = entry_to = sl = tp1 = tp2 = None

    rr = None
    if side != "NO_TRADE" and entry_from and sl and tp1:
        risk = abs(entry_from - sl)
        reward = abs(tp1 - entry_from)
        rr = round(reward / risk, 2) if risk else None

    explanation_lines = [
        f"Тренд 30d: {change_30d:+.2f}% | 7d: {change_7d:+.2f}%",
        f"Локально 1d: {change_1d:+.2f}% | 1h: {change_1h:+.2f}%",
        f"EMA 1h: {'бычий' if ema_fast_1h > ema_slow_1h else 'медвежий'}, EMA 15m: {'бычий' if ema_fast_15m > ema_slow_15m else 'медвежий'}",
        f"MACD 1h: {macd_1h:+.4f}, MACD 15m: {macd_15m:+.4f}",
        f"RSI 1h: {rsi_1h:.1f}, RSI 15m: {rsi_15m:.1f}, RSI 5m: {rsi_5m:.1f}",
        f"Объёмы 1h: {vol_ratio_1h:.2f}x ср.{avg_vol_1h:.2f}, 15m: {vol_ratio_15m:.2f}x ср.{avg_vol_15m:.2f}",
        "Сделка ориентирована на интрадей: стоп и цели строятся от ATR 15m/1h.",
    ]

    signal = BTCSignal(
        timestamp=now,
        side=side,
        probability=abs(raw_score),
        entry_from=entry_from,
        entry_to=entry_to,
        sl=sl,
        tp1=tp1,
        tp2=tp2,
        rr=rr,
        explanation="\n".join(explanation_lines),
        raw_score=int(raw_score),
    )

    return signal


def format_signal_message(signal: BTCSignal, desired_side: Optional[str]) -> str:
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
        "Таймфреймы анализа: 30d, 7d, 1d, 1h, 15m, 5m, 1m",
        "",
        f"Оценка модели: {signal.raw_score}/100",
        f"Вероятность сценария: {signal.probability:.0f}%",
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
        lines.append(f"Ожидаемое соотношение риск/прибыль (R:R): ~1:{signal.rr:.1f}")

    lines.append("")
    lines.append("Почему так решил:")
    lines.append(signal.explanation)
    lines.append("")
    lines.append(
        "⚠️ Это не инвестиционная рекомендация. "
        "Ты сам принимаешь решение и несёшь риск. "
        "Стратегия рассчитана на внутридневную торговлю (до 24 часов)."
    )

    return "\n".join(lines)
