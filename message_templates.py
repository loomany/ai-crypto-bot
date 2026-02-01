from __future__ import annotations

from typing import Optional


def _trend_to_text(value: Optional[str]) -> str:
    if value in ("up", "bullish"):
        return "бычий"
    if value in ("down", "bearish"):
        return "медвежий"
    return "флет"


def _rsi_zone(value: float) -> str:
    if value >= 70:
        return "перекуплен"
    if value <= 30:
        return "перепродан"
    return "нейтр"


def _format_price(value: float, precision: int) -> str:
    return f"{value:.{precision}f}"


def _format_pct(value: float) -> str:
    return f"{value:+.1f}%"


def format_scenario_message(
    *,
    symbol_text: str,
    side: str,
    timeframe: str,
    entry_from: float,
    entry_to: float,
    sl: float,
    tp1: float,
    tp2: float,
    score: int,
    trend_1d: Optional[str],
    trend_4h: Optional[str],
    rsi_1h: float,
    volume_ratio: float,
    rr: float,
    price_precision: int,
) -> str:
    is_long = side == "LONG"
    emoji = "📈" if is_long else "📉"
    scenario_text = "LONG" if is_long else "SHORT"
    condition_line = (
        "• сценарий актуален, если цена не закрепляется ниже зоны"
        if is_long
        else "• сценарий актуален, если цена не закрепляется выше зоны"
    )
    confirmation_line = (
        "• рекомендуется дождаться подтверждения силы на 5–15m"
        if is_long
        else "• рекомендуется дождаться подтверждения слабости на 5–15m"
    )
    stop_condition = "ниже" if is_long else "выше"
    entry_mid = (entry_from + entry_to) / 2
    tp1_pct = (tp1 / entry_mid - 1) * 100
    tp2_pct = (tp2 / entry_mid - 1) * 100

    score = max(0, min(100, int(score)))

    lines = [
        f"Монета: {symbol_text}",
        f"{emoji} Сценарий: возможный {scenario_text}",
        f"⏱ Таймфрейм анализа: {timeframe}",
        "",
        "Зона интереса (POI):",
        f"• {_format_price(entry_from, price_precision)} – {_format_price(entry_to, price_precision)}",
        "",
        "Условие реализации сценария:",
        condition_line,
        confirmation_line,
        "",
        "Уровень отмены сценария:",
        f"• {_format_price(sl, price_precision)} (закрепление {stop_condition} на {timeframe} отменяет сценарий)",
        "",
        "Потенциальные цели движения:",
        f"• 🎯 Цель 1: {_format_price(tp1, price_precision)} ({_format_pct(tp1_pct)} от зоны)",
        f"• 🎯 Цель 2: {_format_price(tp2, price_precision)} ({_format_pct(tp2_pct)} от зоны)",
        "",
        "Оценка модели:",
        f"🧠 Score: {score} / 100",
        "",
        "Краткий рыночный контекст:",
        f"• 1D тренд: {_trend_to_text(trend_1d)}",
        f"• 4H тренд: {_trend_to_text(trend_4h)}",
        f"• RSI 1H: {rsi_1h:.1f} ({_rsi_zone(rsi_1h)})",
        f"• Объём: {volume_ratio:.2f}x выше среднего",
        f"• Соотношение риск/движение: ~{rr:.2f} : 1",
        "",
        "🧾 Шаблон входа (risk-management):",
        "• Риск на сделку: 1% депозита",
        "• Формула объёма: position = risk$ / stop%",
        "• После TP1: 50% фиксация + SL в BE",
        "",
        "⚠️ Бот не знает твой депозит и не управляет рисками.",
        "Решение о входе, объёме позиции и уровне риска ты принимаешь самостоятельно.",
        "",
        "📌 Данный сценарий предназначен для аналитики рынка",
        "и не является инвестиционной рекомендацией.",
    ]
    return "\n".join(lines)
