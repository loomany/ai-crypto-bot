import asyncio
import datetime as dt
from dataclasses import dataclass
from typing import Optional, List, Tuple

from aiogram import Router, F
from aiogram.types import (
    Message,
    CallbackQuery,
    InlineKeyboardMarkup,
    InlineKeyboardButton,
)
from aiogram.fsm.context import FSMContext

# ============================================================
# Константы и базовые настройки
# ============================================================

BTC_SYMBOL = "BTCUSDT"
TIMEZONE_OFFSET_HOURS = 5  # например, Asia/Almaty (UTC+5)

router = Router(name="btc_module")


# ============================================================
# Временное in-memory хранилище (Codex → заменить на БД)
# ============================================================

class InMemoryStorage:
    """
    Временное хранилище.
    Codex: заменить на SQLite/Postgres и нормальные таблицы.
    """

    def __init__(self):
        # user_id -> bool (включены ли уведомления по BTC)
        self.notifications: dict[int, bool] = {}
        # лог сигналов
        self.signals_log: list["BTCSingal"] = []

    def set_notifications(self, user_id: int, enabled: bool) -> None:
        self.notifications[user_id] = enabled

    def is_notifications_enabled(self, user_id: int) -> bool:
        return self.notifications.get(user_id, False)

    def get_all_users_with_notifications(self) -> List[int]:
        return [uid for uid, enabled in self.notifications.items() if enabled]

    def add_signal(self, signal: "BTCSingal") -> None:
        self.signals_log.append(signal)


storage = InMemoryStorage()


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

def get_btc_main_keyboard() -> InlineKeyboardMarkup:
    """
    Меню BTC:
    - Включить уведомление
    - Отключить уведомление
    """
    kb = [
        [
            InlineKeyboardButton(
                text="Включить уведомление", callback_data="btc_notify_on"
            ),
        ],
        [
            InlineKeyboardButton(
                text="Отключить уведомление", callback_data="btc_notify_off"
            ),
        ],
    ]
    return InlineKeyboardMarkup(inline_keyboard=kb)


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

@router.callback_query(F.data == "btc_notify_on")
async def handle_btc_notify_on(callback: CallbackQuery):
    await callback.answer()

    user_id = callback.from_user.id
    storage.set_notifications(user_id, True)

    await callback.message.answer(
        "✅ Уведомления по BTC включены.\n\n"
        "Бот будет автоматически присылать сигналы LONG/SHORT по BTCUSDT, "
        "как только появляется новый сильный сетап (интрадей, внутри 24 часов)."
    )


@router.callback_query(F.data == "btc_notify_off")
async def handle_btc_notify_off(callback: CallbackQuery):
    await callback.answer()

    user_id = callback.from_user.id
    storage.set_notifications(user_id, False)

    await callback.message.answer("❌ Уведомления по BTC отключены.")


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

    # Чтобы не спамить одинаковыми сигналами подряд,
    # запоминаем "подпись" последнего сигнала
    last_signature: Optional[Tuple[str, int]] = None
    # (side, округлённая цена входа)

    while True:
        try:
            # объективный анализ BTC (без предпочтения long/short)
            signal = await generate_btc_signal(desired_side=None)

            # интересуют только реальные сигналы LONG/SHORT
            if signal.side in ("LONG", "SHORT"):
                # строим "подпись" сигнала, чтобы отличать новый от старого
                entry_mid = 0.0
                if signal.entry_from and signal.entry_to:
                    entry_mid = (signal.entry_from + signal.entry_to) / 2.0
                elif signal.entry_from:
                    entry_mid = signal.entry_from

                signature = (signal.side, int(round(entry_mid)))

                # отправляем, только если сигнал новый (подпись изменилась)
                if signature != last_signature:
                    last_signature = signature

                    text = format_signal_message(signal, desired_side=signal.side)
                    user_ids = storage.get_all_users_with_notifications()

                    for user_id in user_ids:
                        try:
                            await bot.send_message(chat_id=user_id, text=text)
                        except Exception:
                            # Codex: логировать ошибки отправки (например, юзер заблокировал бота)
                            continue

                    storage.add_signal(signal)

        except Exception as e:
            # Codex: заменить на нормальное логирование
            print(f"[btc_realtime_signal_worker] error: {e}")

        # Частота проверки рынка.
        # Codex: подобрать значение (1–3 секунды для скальпинга, 5–10 для более спокойной торговли).
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

    # ========= ЗАГЛУШКА ДЛЯ DEMO =========
    # Codex ДОЛЖЕН удалить/переписать этот блок и заменить реальной логикой.

    # Пример: нет сигнала → NO_TRADE
    # return BTCSingal(
    #     timestamp=now,
    #     side="NO_TRADE",
    #     probability=0,
    #     explanation="Сильного сигнала на разворот сейчас нет (заглушка).",
    # )

    # Пример фиктивного LONG-сигнала для демонстрации структуры:
    fake_side = "LONG"
    fake_score = 72
    fake_probability = abs(fake_score)

    current_price = 100_000.0  # Codex: заменить на реальную цену BTCUSDT

    entry_from = current_price * 0.998
    entry_to = current_price * 1.002
    sl = current_price * 0.99
    tp1 = current_price * 1.017
    tp2 = current_price * 1.035
    rr = 2.0

    explanation = (
        "Пример объяснения (заглушка):\n"
        "• 1D: глобальный тренд вверх, цена откатывается к поддержке.\n"
        "• 1H: формируется серия higher lows, объём растёт.\n"
        "• 15m/5m: разворотный паттерн у уровня, повышенный объём.\n"
        "• Волатильность позволяет поставить адекватный стоп в пределах дня."
    )

    return BTCSingal(
        timestamp=now,
        side=fake_side,
        probability=fake_probability,
        entry_from=entry_from,
        entry_to=entry_to,
        sl=sl,
        tp1=tp1,
        tp2=tp2,
        rr=rr,
        explanation=explanation,
        raw_score=fake_score,
    )

    # ========= КОНЕЦ ЗАГЛУШКИ =========


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
