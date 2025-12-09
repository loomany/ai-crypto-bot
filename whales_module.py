import asyncio
import datetime as dt
import time
from dataclasses import dataclass
from typing import Optional, Dict, List, Any

import aiohttp
from aiogram import Router, F
from aiogram.types import (
    Message,
    CallbackQuery,
    InlineKeyboardMarkup,
    InlineKeyboardButton,
)
from aiogram.fsm.context import FSMContext

# ============================================================
# НАСТРОЙКИ МОДУЛЯ КИТОВ
# ============================================================

router = Router(name="whales_module")

# ТОП-5 монет, за которыми следим
WHALES_SYMBOLS = ["BTCUSDT", "ETHUSDT", "SOLUSDT", "BNBUSDT", "XRPUSDT"]

# Пороговые значения для крупного капитала (можно тюнить)
MIN_WHALE_TRADE_USD = 100_000      # минимальный размер сделки, чтобы считать китовой
STRONG_WHALE_TRADE_USD = 300_000   # сильная активность
MEGA_WHALE_TRADE_USD = 1_000_000   # мега-кит

TIMEZONE_OFFSET_HOURS = 5  # Asia/Almaty

BINANCE_FAPI_BASE = "https://fapi.binance.com/fapi/v1"
OI_HISTORY_ENDPOINT = "https://fapi.binance.com/futures/data/openInterestHist"


# ============================================================
# ХРАНИЛИЩЕ ДЛЯ УВЕДОМЛЕНИЙ ПО КИТАМ
# ============================================================

class WhalesStorage:
    """
    Простое in-memory хранилище для уведомлений по китам.
    Codex: при желании заменить на БД (SQLite/Postgres).
    """

    def __init__(self):
        # user_id -> bool (включены ли уведомления по китам)
        self.whales_notifications: Dict[int, bool] = {}

    def set_whales_notify(self, user_id: int, enabled: bool) -> None:
        self.whales_notifications[user_id] = enabled

    def is_whales_notify_enabled(self, user_id: int) -> bool:
        return self.whales_notifications.get(user_id, False)

    def get_all_whales_users(self) -> List[int]:
        return [uid for uid, enabled in self.whales_notifications.items() if enabled]


whales_storage = WhalesStorage()


# ============================================================
# МОДЕЛЬ КИТОВОГО СИГНАЛА (dataclass)
# ============================================================

@dataclass
class WhaleSignal:
    symbol: str
    timestamp: dt.datetime
    side: str  # "BUY" или "SELL"
    whale_buy_usd: float
    whale_sell_usd: float
    orderflow_imbalance_pct: float
    cvd_direction: str  # "up" / "down" / "flat"
    oi_change_pct: float
    funding: float
    explanation: str
    probability: float  # 0–100


# ============================================================
# КЛАВИАТУРА ДЛЯ УПРАВЛЕНИЯ КИТАМИ
# ============================================================

def get_whales_keyboard() -> InlineKeyboardMarkup:
    """
    Меню управления уведомлениями по китам для ТОП-5 монет.
    """
    kb = [
        [
            InlineKeyboardButton(
                text="🐳 Включить уведомления по китам", callback_data="whales_notify_on"
            )
        ],
        [
            InlineKeyboardButton(
                text="🐳 Отключить уведомления по китам", callback_data="whales_notify_off"
            )
        ],
    ]
    return InlineKeyboardMarkup(inline_keyboard=kb)


# ============================================================
# КОМАНДА /whales — вход в меню китов
# ============================================================


def _whales_menu_text() -> str:
    return (
        "🐳 Модуль КИТОВ (ордерфлоу, крупные сделки, OI, CVD)\n\n"
        "Монеты: BTC, ETH, SOL, BNB, XRP\n"
        "Бот будет присылать сигналы, когда крупные игроки массово ВХОДЯТ или ВЫХОДЯТ из этих монет.\n\n"
        "Это помогает:\n"
        "• Видеть, куда заходит крупный капитал\n"
        "• Раньше замечать начало тренда или разворот\n"
        "• Не заходить против китов\n\n"
        "Выбери действие:"
    )


@router.message(F.text == "/whales")
async def whales_menu_command(message: Message, state: FSMContext):
    """
    Команда /whales — управление сигналами по крупным китам (ТОП-5 монет).
    """
    await message.answer(_whales_menu_text(), reply_markup=get_whales_keyboard())


# ============================================================
# ХЕНДЛЕРЫ ВКЛ/ВЫКЛ УВЕДОМЛЕНИЙ ПО КИТАМ
# ============================================================

@router.callback_query(F.data == "whales_notify_on")
async def handle_whales_notify_on(callback: CallbackQuery):
    await callback.answer()

    user_id = callback.from_user.id
    whales_storage.set_whales_notify(user_id, True)

    await callback.message.answer(
        "✅ Уведомления по КИТАМ включены.\n\n"
        "Теперь ты будешь получать сигналы, когда крупные игроки:\n"
        "• Массово ПОКУПАЮТ или ПРОДАЮТ BTC, ETH, SOL, BNB, XRP\n"
        "• Сильно меняют Open Interest\n"
        "• Формируют мощный перекос ордерфлоу.\n\n"
        "Используй это как фильтр: не лезь против китов."
    )


@router.callback_query(F.data == "whales_notify_off")
async def handle_whales_notify_off(callback: CallbackQuery):
    await callback.answer()

    user_id = callback.from_user.id
    whales_storage.set_whales_notify(user_id, False)

    await callback.message.answer(
        "❌ Уведомления по КИТАМ отключены.\n\n"
        "Ты всегда можешь снова включить их командой /whales."
    )


# ============================================================
# УТИЛИТЫ ДЛЯ РАБОТЫ С BINANCE FUTURES
# ============================================================

async def _fetch_json(session: aiohttp.ClientSession, url: str, params: Dict[str, Any]) -> Optional[Any]:
    try:
        async with session.get(url, params=params, timeout=10) as resp:
            resp.raise_for_status()
            return await resp.json()
    except Exception as exc:
        print(f"[whales] fetch error {url}: {exc}")
        return None


async def _fetch_agg_trades(session: aiohttp.ClientSession, symbol: str, start_time_ms: int, end_time_ms: int):
    params = {
        "symbol": symbol,
        "startTime": start_time_ms,
        "endTime": end_time_ms,
        "limit": 1000,
    }
    return await _fetch_json(session, f"{BINANCE_FAPI_BASE}/aggTrades", params)


async def _fetch_klines(session: aiohttp.ClientSession, symbol: str, limit: int = 5):
    params = {
        "symbol": symbol,
        "interval": "1m",
        "limit": limit,
    }
    return await _fetch_json(session, f"{BINANCE_FAPI_BASE}/klines", params)


async def _fetch_oi_history(session: aiohttp.ClientSession, symbol: str):
    params = {
        "symbol": symbol,
        "period": "5m",
        "limit": 3,
    }
    return await _fetch_json(session, OI_HISTORY_ENDPOINT, params)


async def _fetch_funding(session: aiohttp.ClientSession, symbol: str) -> float:
    params = {"symbol": symbol}
    data = await _fetch_json(session, f"{BINANCE_FAPI_BASE}/premiumIndex", params)
    try:
        return float(data.get("lastFundingRate", 0.0)) if data else 0.0
    except Exception:
        return 0.0


# ============================================================
# ЯДРО: АНАЛИЗ КРУПНЫХ КИТОВ
# ============================================================

async def analyze_whales(symbol: str) -> Optional[WhaleSignal]:
    """
    Анализ активности китов по конкретному символу (BTCUSDT/ETHUSDT/...).
    """

    now_ms = int(time.time() * 1000)
    start_ms = now_ms - 30_000

    async with aiohttp.ClientSession() as session:
        trades_task = asyncio.create_task(_fetch_agg_trades(session, symbol, start_ms, now_ms))
        klines_task = asyncio.create_task(_fetch_klines(session, symbol, limit=5))
        oi_task = asyncio.create_task(_fetch_oi_history(session, symbol))
        funding_task = asyncio.create_task(_fetch_funding(session, symbol))

        trades = await trades_task
        klines = await klines_task
        oi_hist = await oi_task
        funding = await funding_task

    if not trades:
        return None

    whale_buy_usd = 0.0
    whale_sell_usd = 0.0

    for tr in trades:
        try:
            price = float(tr.get("p", 0.0))
            qty = float(tr.get("q", 0.0))
            usd_value = price * qty
            if usd_value < MIN_WHALE_TRADE_USD:
                continue
            is_buyer_maker = bool(tr.get("m"))
            if is_buyer_maker:
                whale_sell_usd += usd_value
            else:
                whale_buy_usd += usd_value
        except Exception:
            continue

    if whale_buy_usd == 0 and whale_sell_usd == 0:
        return None

    taker_buy_quote = 0.0
    taker_sell_quote = 0.0
    cvd_direction = "flat"

    if klines:
        total_quote = 0.0
        cvd_value = 0.0
        for k in klines:
            try:
                taker_buy = float(k[10])
                quote_vol = float(k[7])
            except Exception:
                continue
            sell_quote = max(quote_vol - taker_buy, 0.0)
            taker_buy_quote += taker_buy
            taker_sell_quote += sell_quote
            cvd_value += taker_buy - sell_quote
            total_quote += quote_vol

        threshold = (total_quote * 0.02) if total_quote else 0.0
        if cvd_value > threshold:
            cvd_direction = "up"
        elif cvd_value < -threshold:
            cvd_direction = "down"

    orderflow_imbalance_pct = 0.0
    total_flow = taker_buy_quote + taker_sell_quote
    if total_flow > 0:
        orderflow_imbalance_pct = (taker_buy_quote - taker_sell_quote) / total_flow * 100

    oi_change_pct = 0.0
    if oi_hist and len(oi_hist) >= 2:
        try:
            first_oi = float(oi_hist[0]["sumOpenInterest"])
            last_oi = float(oi_hist[-1]["sumOpenInterest"])
            if first_oi > 0:
                oi_change_pct = (last_oi - first_oi) / first_oi * 100
        except Exception:
            oi_change_pct = 0.0

    bullish = (
        whale_buy_usd >= STRONG_WHALE_TRADE_USD
        and whale_buy_usd > whale_sell_usd
        and orderflow_imbalance_pct >= 20
        and cvd_direction == "up"
        and oi_change_pct >= 3
    )
    bearish = (
        whale_sell_usd >= STRONG_WHALE_TRADE_USD
        and whale_sell_usd > whale_buy_usd
        and orderflow_imbalance_pct <= -20
        and cvd_direction == "down"
        and oi_change_pct <= -4
    )

    if not bullish and not bearish:
        return None

    side = "BUY" if bullish else "SELL"

    probability = 80.0
    if max(whale_buy_usd, whale_sell_usd) >= MEGA_WHALE_TRADE_USD:
        probability += 5
    probability = min(probability, 95.0)

    explanation_parts = [
        f"Крупные сделки: BUY {whale_buy_usd:,.0f} $ vs SELL {whale_sell_usd:,.0f} $",
        f"Ордерфлоу дисбаланс: {orderflow_imbalance_pct:+.1f}%",
        f"CVD направление: {cvd_direction}",
        f"OI изменение за ~15м: {oi_change_pct:+.2f}%",
        f"Funding rate: {funding:.6f}",
    ]

    signal = WhaleSignal(
        symbol=symbol,
        timestamp=dt.datetime.utcnow(),
        side=side,
        whale_buy_usd=whale_buy_usd,
        whale_sell_usd=whale_sell_usd,
        orderflow_imbalance_pct=orderflow_imbalance_pct,
        cvd_direction=cvd_direction,
        oi_change_pct=oi_change_pct,
        funding=funding,
        explanation="; ".join(explanation_parts),
        probability=probability,
    )

    return signal


# ============================================================
# ФОРМАТИРОВАНИЕ СООБЩЕНИЯ ОТ КИТОВ
# ============================================================

def format_whale_alert(signal: WhaleSignal) -> str:
    """
    Красивый текст уведомления о китовом сигнале.
    """

    local_ts = signal.timestamp + dt.timedelta(hours=TIMEZONE_OFFSET_HOURS)
    ts_str = local_ts.strftime("%Y-%m-%d %H:%M:%S")

    emoji = "🟢" if signal.side == "BUY" else "🔴"
    action_str = "ПОКУПАЮТ" if signal.side == "BUY" else "ПРОДАЮТ"

    lines = [
        f"{emoji} WHALES ALERT — {signal.symbol}",
        "",
        f"Время (локальное): {ts_str}",
        f"Сторона: Крупные игроки {action_str}",
        f"Оценка вероятности сценария: {signal.probability:.0f}%",
        "",
        f"Крупные покупки (BUY):  {signal.whale_buy_usd:,.0f} $",
        f"Крупные продажи (SELL): {signal.whale_sell_usd:,.0f} $",
        f"Дисбаланс ордерфлоу: {signal.orderflow_imbalance_pct:+.1f}%",
        f"CVD направление: {signal.cvd_direction}",
        f"Изменение Open Interest (OI): {signal.oi_change_pct:+.2f}%",
        f"Funding rate: {signal.funding:.6f}",
        "",
        "Что это значит для тебя:",
    ]

    # Объяснение «по-человечески»
    if signal.side == "BUY":
        lines.append("• Крупный капитал накапливает позицию в этой монете.")
        lines.append("• Вероятность продолжения движения ВВЕРХ повышена.")
        lines.append("• Можно рассматривать вход в LONG или удержание текущих лонгов,")
        lines.append("  но с учётом твоего риска и стоп-лосса.")
    else:
        lines.append("• Киты массово разгружаются / фиксируют прибыль.")
        lines.append("• Растёт риск движения ВНИЗ или начала дампа.")
        lines.append("• Можно рассматривать фиксацию LONG или поиск точки для SHORT,")
        lines.append("  если твоя стратегия это предполагает.")

    lines.append("")
    lines.append("Почему бот так решил:")
    lines.append(signal.explanation)
    lines.append("")
    lines.append(
        "⚠️ Это не инвестиционная рекомендация.\n"
        "Ты сам принимаешь решения по входу/выходу и несёшь ответственность за риск."
    )

    return "\n".join(lines)


# ============================================================
# ФОНОВЫЙ ВОРКЕР ДЛЯ КИТОВ (ТОП-5 МОНЕТ)
# ============================================================

async def whales_realtime_worker(bot):
    """
    Фоновая задача:
      - каждые несколько секунд обходить ТОП-5 монет
      - анализировать китовую активность
      - при появлении сильного сигнала (BUY/SELL) отправлять уведомления
        всем пользователям, у кого включены уведомления по китам.

    Codex:
      - вызывать эту функцию из main.py:
        asyncio.create_task(whales_realtime_worker(bot))
    """

    await asyncio.sleep(5)  # пауза после старта бота

    # Можно добавить защиту от спама: кэш последних сигналов.
    last_signals: Dict[str, str] = {}  # symbol -> side ("BUY"/"SELL")

    while True:
        try:
            user_ids = whales_storage.get_all_whales_users()
            if not user_ids:
                await asyncio.sleep(5)
                continue

            for symbol in WHALES_SYMBOLS:
                signal = await analyze_whales(symbol)
                if signal is None:
                    continue

                # защита от однотипного спама:
                last_side = last_signals.get(symbol)
                if last_side == signal.side:
                    # уже отправляли такой же сигнал недавно – можно пропустить или
                    # сделать более сложную проверку по timestamp/вероятности.
                    continue

                last_signals[symbol] = signal.side

                text = format_whale_alert(signal)
                for uid in user_ids:
                    try:
                        await bot.send_message(chat_id=uid, text=text)
                    except Exception:
                        continue

        except Exception as e:
            print(f"[whales_realtime_worker] error: {e}")

        # Пауза между проходами по монетам.
        # Codex может тюнить (2–10 секунд).
        await asyncio.sleep(3)
