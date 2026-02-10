from __future__ import annotations

from typing import Optional

import i18n


def _trend_to_text(value: Optional[str], lang: str) -> str:
    if value in ("up", "bullish"):
        return i18n.t(lang, "SCENARIO_TREND_BULLISH")
    if value in ("down", "bearish"):
        return i18n.t(lang, "SCENARIO_TREND_BEARISH")
    return i18n.t(lang, "SCENARIO_TREND_FLAT")


def _rsi_zone(value: float, lang: str) -> str:
    if value >= 70:
        return i18n.t(lang, "SCENARIO_RSI_OVERBOUGHT")
    if value <= 30:
        return i18n.t(lang, "SCENARIO_RSI_OVERSOLD")
    return i18n.t(lang, "SCENARIO_RSI_NEUTRAL")


def _format_price(value: float, precision: int) -> str:
    return f"{value:.{precision}f}"


def _format_pct(value: float) -> str:
    return f"{value:+.1f}%"


def _signal_quality_block(score: int, lang: str) -> str:
    if score >= 90:
        return i18n.t(lang, "SIGNAL_QUALITY_RECOMMENDED")
    if score >= 80:
        return i18n.t(lang, "SIGNAL_QUALITY_HIGH_RISK")
    return i18n.t(lang, "SIGNAL_QUALITY_ANALYSIS_ONLY")


def format_scenario_message(
    *,
    lang: str,
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
    score_breakdown: Optional[list[dict]] = None,
    lifetime_minutes: int = 720,
) -> str:
    is_long = side == "LONG"
    emoji = "📈" if is_long else "📉"
    scenario_text = "LONG" if is_long else "SHORT"
    entry_mid = (entry_from + entry_to) / 2

    score = max(0, min(100, int(score)))
    quality_block = _signal_quality_block(score, lang)

    holds_rule = (
        i18n.t(lang, "SCENARIO_VALID_ABOVE")
        if is_long
        else i18n.t(lang, "SCENARIO_VALID_BELOW")
    )
    stop_condition = (
        i18n.t(lang, "SCENARIO_CONDITION_BELOW")
        if is_long
        else i18n.t(lang, "SCENARIO_CONDITION_ABOVE")
    )
    invalid_level = _format_price(sl, price_precision)
    sl_pct = (sl / entry_mid - 1) * 100

    tp_candidates = [tp1, tp2]
    if is_long:
        tp_candidates = sorted(tp_candidates)
        targets_invalid = max(tp_candidates) <= entry_mid
    else:
        tp_candidates = sorted(tp_candidates, reverse=True)
        targets_invalid = min(tp_candidates) >= entry_mid

    if targets_invalid:
        tp_lines = [
            f"🎯 TP1: {i18n.t(lang, 'SCENARIO_TP_NEEDS_REVIEW')}",
            f"🎯 TP2: {i18n.t(lang, 'SCENARIO_TP_NEEDS_REVIEW')}",
        ]
    else:
        tp1_val, tp2_val = tp_candidates
        tp1_pct = (tp1_val / entry_mid - 1) * 100
        tp2_pct = (tp2_val / entry_mid - 1) * 100
        tp_lines = [
            f"🎯 TP1: {_format_price(tp1_val, price_precision)} ({_format_pct(tp1_pct)})",
            f"🎯 TP2: {_format_price(tp2_val, price_precision)} ({_format_pct(tp2_pct)})",
        ]

    breakdown_items = score_breakdown or []
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
    breakdown_lines = []
    for item in breakdown_items:
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
        quality_block,
        "",
        symbol_text,
        i18n.t(lang, "SCENARIO_POSSIBLE_LINE", emoji=emoji, scenario=scenario_text),
        i18n.t(lang, "SCENARIO_TIMEFRAME_LINE", timeframe=timeframe),
        i18n.t(lang, "SCENARIO_LIFETIME_MINUTES_LINE", minutes=max(1, int(lifetime_minutes))),
        "",
        i18n.t(lang, "SCENARIO_POI_HEADER"),
        f"• {_format_price(entry_from, price_precision)} – {_format_price(entry_to, price_precision)}",
        "",
        i18n.t(lang, "SCENARIO_CONDITIONS_HEADER"),
        holds_rule,
        i18n.t(lang, "SCENARIO_CONFIRMATION_LINE"),
        "",
        i18n.t(lang, "SCENARIO_CONFIRM_HEADER"),
        i18n.t(lang, "SCENARIO_CONFIRM_CLOSE"),
        i18n.t(lang, "SCENARIO_CONFIRM_HOLD"),
        "",
        i18n.t(lang, "SCENARIO_INVALIDATION_HEADER"),
        i18n.t(
            lang,
            "SCENARIO_INVALIDATION_LINE",
            condition=stop_condition,
            level=invalid_level,
        ),
        "",
        i18n.t(lang, "SCENARIO_SL_HEADER"),
        i18n.t(
            lang,
            "SCENARIO_SL_LINE",
            price=invalid_level,
            pct=_format_pct(sl_pct),
        ),
        "",
        i18n.t(lang, "SCENARIO_TARGETS_HEADER"),
        *tp_lines,
        "",
        i18n.t(lang, "SCENARIO_CONTEXT_HEADER"),
        i18n.t(
            lang,
            "SCENARIO_CONTEXT_TREND",
            trend_1d=_trend_to_text(trend_1d, lang),
            trend_4h=_trend_to_text(trend_4h, lang),
        ),
        i18n.t(
            lang,
            "SCENARIO_CONTEXT_RSI",
            rsi=f"{rsi_1h:.1f}",
            zone=_rsi_zone(rsi_1h, lang),
        ),
        i18n.t(lang, "SCENARIO_CONTEXT_VOLUME", volume=f"{volume_ratio:.2f}"),
        i18n.t(lang, "SCENARIO_CONTEXT_RR", rr=f"{rr:.2f}"),
        "",
        i18n.t(lang, "SCENARIO_SCORE_LINE", score=score),
        i18n.t(lang, "SCENARIO_BREAKDOWN_HEADER"),
        *breakdown_lines,
        i18n.t(lang, "SCENARIO_BREAKDOWN_TOTAL", score=score),
        "",
        i18n.t(lang, "SCENARIO_DISCLAIMER_1"),
        i18n.t(lang, "SCENARIO_DISCLAIMER_2"),
        i18n.t(lang, "SCENARIO_DISCLAIMER_3"),
    ]
    return "\n".join(lines)


def format_signal_activation_message(
    *,
    lang: str,
    symbol: str,
    side: str,
    score: int,
    entry_price: float,
    sl: float,
    tp1: float,
    tp2: float,
) -> str:
    header = i18n.t(lang, "SIGNAL_ACTIVATED_HEADER")
    waiting = i18n.t(lang, "SIGNAL_ACTIVATED_WAITING")
    side_value = str(side).upper()
    lines = [
        header,
        "",
        f"{symbol} - {side_value}",
        f"Score: {max(0, min(100, int(score)))}",
        "",
        f"🔹 {i18n.t(lang, 'SIGNAL_ACTIVATED_ENTRY_LABEL')}: {_format_price(float(entry_price), 4)}",
        f"🛑 {i18n.t(lang, 'SIGNAL_ACTIVATED_SL_LABEL')}: {_format_price(float(sl), 4)}",
        f"🎯 TP1: {_format_price(float(tp1), 4)}",
        f"🎯 TP2: {_format_price(float(tp2), 4)}",
        "",
        waiting,
    ]
    return "\n".join(lines)


def format_signal_poi_touched_message(
    *,
    lang: str,
    symbol: str,
    side: str,
    score: int,
    poi_from: float,
    poi_to: float,
) -> str:
    side_value = str(side).upper()
    lines = [
        i18n.t(lang, "SIGNAL_POI_TOUCHED_HEADER"),
        "",
        f"{symbol} · {side_value}",
        f"Score: {max(0, min(100, int(score)))}",
        "",
        i18n.t(lang, "SIGNAL_POI_TOUCHED_ZONE_HEADER"),
        f"{_format_price(float(poi_from), 4)} – {_format_price(float(poi_to), 4)}",
        "",
        i18n.t(lang, "SIGNAL_POI_TOUCHED_WAIT"),
        i18n.t(lang, "SIGNAL_POI_TOUCHED_WAIT_2"),
    ]
    return "\n".join(lines)


def format_compact_scenario_message(
    *,
    lang: str,
    symbol_text: str,
    side: str,
    timeframe: str,
    entry_from: float,
    entry_to: float,
    sl: float,
    tp1: float,
    tp2: float,
    score: int,
    price_precision: int,
    lifetime_minutes: int = 720,
) -> str:
    side_value = str(side).upper()
    score_value = max(0, min(100, int(score)))
    lines = [
        i18n.t(lang, "SIGNAL_COMPACT_HIGH_RISK_HEADER"),
        symbol_text,
        i18n.t(
            lang,
            "SIGNAL_COMPACT_META_LINE",
            side=side_value,
            timeframe=timeframe,
            entry_tf=timeframe,
        ),
        i18n.t(
            lang,
            "SIGNAL_COMPACT_POI_LINE",
            poi_from=_format_price(entry_from, price_precision),
            poi_to=_format_price(entry_to, price_precision),
        ),
        i18n.t(lang, "SIGNAL_COMPACT_SL_LINE", sl=_format_price(sl, price_precision)),
        i18n.t(lang, "SIGNAL_COMPACT_TP1_LINE", tp1=_format_price(tp1, price_precision)),
        i18n.t(lang, "SIGNAL_COMPACT_TP2_LINE", tp2=_format_price(tp2, price_precision)),
        i18n.t(lang, "SIGNAL_COMPACT_SCORE_LINE", score=score_value),
        i18n.t(
            lang,
            "SIGNAL_COMPACT_TTL_LINE",
            minutes=max(1, int(lifetime_minutes)),
        ),
    ]
    return "\n".join(lines)
