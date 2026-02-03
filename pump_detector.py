import asyncio
import os
import re
import time
from typing import Any, Callable, Dict, List, Optional, Tuple

import aiohttp

from binance_client import fetch_klines, Candle
from binance_rest import binance_request_context, fetch_json
from symbol_cache import (
    filter_tradeable_symbols,
    get_spot_usdt_symbols,
    get_top_usdt_symbols_by_movers,
    get_top_usdt_symbols_by_volume,
)

BINANCE_API = "https://api.binance.com"
PUMP_1M_THRESHOLD = float(os.getenv("PUMP_1M_THRESHOLD", "1.6"))
PUMP_5M_THRESHOLD = float(os.getenv("PUMP_5M_THRESHOLD", "3.2"))
DUMP_1M_THRESHOLD = float(os.getenv("DUMP_1M_THRESHOLD", "-1.6"))
DUMP_5M_THRESHOLD = float(os.getenv("DUMP_5M_THRESHOLD", "-3.2"))
PUMP_VOLUME_MUL = float(os.getenv("PUMP_VOLUME_MUL", "1.8"))
COOLDOWN_SEC = 60
DUP_CHANGE_THRESHOLD = 0.15
MIN_PRICE_USDT = 0.0005
MIN_VOLUME_5M_USDT = float(os.getenv("MIN_VOLUME_5M_USDT", "7000"))
PRIORITY_LIMIT = 250
BATCH_SIZE = 20
PUMP_CHUNK_SIZE = int(os.getenv("PUMP_CHUNK_SIZE", "60"))
PUMPDUMP_TOP_GAINERS_N = int(os.getenv("PUMPDUMP_TOP_GAINERS_N", "0"))
PUMPDUMP_TOP_LOSERS_N = int(os.getenv("PUMPDUMP_TOP_LOSERS_N", "0"))
MAX_CYCLE_SEC = 30
SYMBOL_REGEX = re.compile(r"^[A-Z0-9]{2,20}USDT$")
_last_signals: dict[str, Dict[str, Any]] = {}


async def get_usdt_symbols(session: aiohttp.ClientSession) -> list[str]:
    """
    Получаем ВСЕ спотовые пары к USDT, которые сейчас торгуются.
    """
    return await get_spot_usdt_symbols(session)


async def get_candidate_symbols(
    session: aiohttp.ClientSession,
    limit: int = 80,
    *,
    return_stats: bool = False,
) -> list[str] | tuple[list[str], dict[str, int]]:
    symbols = await get_usdt_symbols(session)
    symbols = [sym for sym in symbols if SYMBOL_REGEX.match(sym)]
    symbols, removed = filter_tradeable_symbols(symbols)
    top_symbols = await get_top_usdt_symbols_by_volume(limit, session=session)
    top_symbols, _ = filter_tradeable_symbols(top_symbols)
    gainers = []
    losers = []
    if PUMPDUMP_TOP_GAINERS_N > 0 or PUMPDUMP_TOP_LOSERS_N > 0:
        gainers, losers = await get_top_usdt_symbols_by_movers(
            PUMPDUMP_TOP_GAINERS_N,
            PUMPDUMP_TOP_LOSERS_N,
            session=session,
        )
        gainers, _ = filter_tradeable_symbols(gainers)
        losers, _ = filter_tradeable_symbols(losers)
    candidates = top_symbols + gainers + losers
    seen: set[str] = set()
    symbols_set = set(symbols)
    filtered = []
    for sym in candidates:
        if sym in symbols_set and sym not in seen and SYMBOL_REGEX.match(sym):
            seen.add(sym)
            filtered.append(sym)
    result = filtered if filtered else symbols[:limit]
    if return_stats:
        return result, {"total": len(symbols) + removed, "removed": removed, "final": len(result)}
    return result


async def get_klines_1m(symbol: str, limit: int = 25) -> list[Candle] | None:
    try:
        with binance_request_context("pumpdump"):
            return await fetch_klines(symbol, "1m", limit)
    except Exception as exc:
        print(f"[BINANCE] ERROR {symbol}: {exc}")
        return None


def _format_signed(value: float, decimals: int = 2) -> str:
    sign = "+" if value >= 0 else "-"
    return f"{sign}{abs(value):.{decimals}f}"


def _format_price(value: float) -> str:
    v = abs(value)
    if v >= 100:
        return f"{value:.0f}"
    if v >= 1:
        return f"{value:.4f}"
    if v >= 0.01:
        return f"{value:.6f}"
    return f"{value:.8f}"


def _is_near_duplicate(prev: Dict[str, Any], current: Dict[str, Any]) -> bool:
    if prev.get("type") != current.get("type"):
        return False

    def _diff(key: str) -> float:
        return abs(float(prev.get(key, 0)) - float(current.get(key, 0)))

    return (
        _diff("change_1m") < DUP_CHANGE_THRESHOLD
        and _diff("change_5m") < DUP_CHANGE_THRESHOLD
        and _diff("volume_mul") < DUP_CHANGE_THRESHOLD
    )


def _remember_signal(signal: Dict[str, Any]) -> bool:
    now = time.time()
    sym = signal["symbol"]
    prev = _last_signals.get(sym)

    if prev and now - prev.get("detected_at", 0) < COOLDOWN_SEC:
        return False

    if prev and _is_near_duplicate(prev, signal):
        return False

    _last_signals[sym] = signal
    return True


def _calc_signal_with_reason(
    symbol: str,
    klines: list[list[str]] | list[Candle],
) -> tuple[Dict[str, Any] | None, str]:
    if not klines or len(klines) < 6:
        return None, "fail_short_series"

    if isinstance(klines[0], Candle):
        closes = [float(k.close) for k in klines]
        volumes = [float(k.volume) for k in klines]
    else:
        closes = [float(k[4]) for k in klines]
        volumes = [float(k[5]) for k in klines]

    last_price = closes[-1]
    price_1m = closes[-2]
    price_5m = closes[-6]

    change_1m = (last_price / price_1m - 1) * 100
    change_5m = (last_price / price_5m - 1) * 100

    volume_5m = sum(volumes[-5:])
    avg_volume_1m = sum(volumes[:-5]) / max(1, len(volumes) - 5)
    avg_volume_5m = avg_volume_1m * 5
    if avg_volume_5m <= 0:
        return None, "fail_avg_volume"
    volume_mul = volume_5m / avg_volume_5m

    if last_price < MIN_PRICE_USDT:
        return None, "fail_min_price"

    volume_5m_usdt = volume_5m * last_price
    if volume_5m_usdt < MIN_VOLUME_5M_USDT:
        return None, "fail_min_volume_5m_usdt"

    sig_type = None
    if (
        change_1m >= PUMP_1M_THRESHOLD
        and change_5m >= PUMP_5M_THRESHOLD
        and volume_mul >= PUMP_VOLUME_MUL
    ):
        sig_type = "pump"
    elif (
        change_1m <= DUMP_1M_THRESHOLD
        and change_5m <= DUMP_5M_THRESHOLD
        and volume_mul >= PUMP_VOLUME_MUL
    ):
        sig_type = "dump"

    if not sig_type:
        return None, "fail_no_trigger"

    strength = abs(change_5m)

    signal = {
        "symbol": symbol,
        "price": last_price,
        "change_1m": round(change_1m, 2),
        "change_5m": round(change_5m, 2),
        "volume_mul": round(volume_mul, 2),
        "type": sig_type,
        "detected_at": time.time(),
        "strength": strength,
    }

    if not _remember_signal(signal):
        return None, "fail_dedup"

    return signal, "ok"


def _calc_signal_from_klines(
    symbol: str,
    klines: list[list[str]] | list[Candle],
) -> Dict[str, Any] | None:
    sig, _ = _calc_signal_with_reason(symbol, klines)
    return sig


async def build_pump_symbol_list(
    session: aiohttp.ClientSession,
    *,
    priority_limit: int = PRIORITY_LIMIT,
) -> list[str]:
    symbols = await get_usdt_symbols(session)
    symbols = [sym for sym in symbols if SYMBOL_REGEX.match(sym)]
    symbols, _ = filter_tradeable_symbols(symbols)
    top_symbols = await get_top_usdt_symbols_by_volume(priority_limit, session=session)
    top_symbols, _ = filter_tradeable_symbols(top_symbols)
    gainers = []
    losers = []
    if PUMPDUMP_TOP_GAINERS_N > 0 or PUMPDUMP_TOP_LOSERS_N > 0:
        gainers, losers = await get_top_usdt_symbols_by_movers(
            PUMPDUMP_TOP_GAINERS_N,
            PUMPDUMP_TOP_LOSERS_N,
            session=session,
        )
        gainers, _ = filter_tradeable_symbols(gainers)
        losers, _ = filter_tradeable_symbols(losers)
    candidates = top_symbols + gainers + losers
    seen: set[str] = set()
    priority = []
    symbols_set = set(symbols)
    for sym in candidates:
        if sym in symbols_set and sym not in seen and SYMBOL_REGEX.match(sym):
            seen.add(sym)
            priority.append(sym)
    rest = [sym for sym in symbols if sym not in set(priority)]
    return priority + rest


async def scan_pumps_chunk(
    symbols: list[str],
    *,
    start_idx: int = 0,
    batch_size: int = BATCH_SIZE,
    max_symbols: int = PUMP_CHUNK_SIZE,
    time_budget_sec: int | None = None,
    progress_cb: Optional[Callable[[str], None]] = None,
    return_stats: bool = False,
) -> Tuple[List[Dict[str, Any]], Dict[str, int], int]:
    results: list[Dict[str, Any]] = []
    checked = 0
    fails: dict[str, int] = {}
    if not symbols:
        return results, {"checked": 0, "found": 0, "fails": fails}, start_idx

    end_idx = min(start_idx + max_symbols, len(symbols))
    start_ts = time.time()
    for i in range(start_idx, end_idx, batch_size):
        if time_budget_sec is not None and time.time() - start_ts >= time_budget_sec:
            break
        batch = symbols[i : i + batch_size]
        tasks = [asyncio.create_task(get_klines_1m(symbol, limit=25)) for symbol in batch]
        klines_list = await asyncio.gather(*tasks, return_exceptions=True)

        for symbol, klines in zip(batch, klines_list):
            if time_budget_sec is not None and time.time() - start_ts >= time_budget_sec:
                break
            if progress_cb:
                progress_cb(symbol)
            checked += 1
            if isinstance(klines, Exception):
                fails["fail_klines_exception"] = fails.get("fail_klines_exception", 0) + 1
                continue
            sig, reason = _calc_signal_with_reason(symbol, klines)
            if sig:
                results.append(sig)
            else:
                fails[reason] = fails.get(reason, 0) + 1

    stats = {"checked": checked, "found": len(results), "fails": fails}
    next_idx = end_idx if end_idx < len(symbols) else 0
    if return_stats:
        return results, stats, next_idx
    return results, stats, next_idx


async def scan_pumps(
    batch_size: int = BATCH_SIZE,
    priority_limit: int = PRIORITY_LIMIT,
    max_cycle_sec: int = MAX_CYCLE_SEC,
    *,
    return_stats: bool = False,
) -> List[Dict[str, Any]] | Tuple[List[Dict[str, Any]], Dict[str, int]]:
    """
    Сканирует все USDT-пары и возвращает список обнаруженных пампов или дампов.
    """
    results: list[Dict[str, Any]] = []
    checked = 0
    start_ts = time.time()

    async with aiohttp.ClientSession() as session:
        ordered_symbols = await build_pump_symbol_list(session, priority_limit=priority_limit)

        for i in range(0, len(ordered_symbols), batch_size):
            if time.time() - start_ts >= max_cycle_sec:
                break
            batch = ordered_symbols[i : i + batch_size]
            tasks = [asyncio.create_task(get_klines_1m(symbol, limit=25)) for symbol in batch]
            klines_list = await asyncio.gather(*tasks, return_exceptions=True)

            for symbol, klines in zip(batch, klines_list):
                checked += 1
                if isinstance(klines, Exception):
                    continue
                sig = _calc_signal_from_klines(symbol, klines)
                if sig:
                    results.append(sig)

    stats = {"checked": checked, "found": len(results)}
    if return_stats:
        return results, stats
    return results


def _format_symbol_pair(symbol: str) -> str:
    if symbol.endswith("USDT"):
        return f"{symbol[:-4]} / USDT"
    return f"{symbol} / USDT"


def _build_pump_plan(price: float) -> Dict[str, float]:
    entry_high = price * 0.99
    entry_low = price * 0.97
    entry_mid = (entry_low + entry_high) / 2
    sl = entry_mid * 0.95
    tp1 = price * 1.03
    tp2 = price * 1.05
    return {
        "entry_low": entry_low,
        "entry_high": entry_high,
        "entry_mid": entry_mid,
        "sl": sl,
        "tp1": tp1,
        "tp2": tp2,
    }


def _build_dump_plan(price: float) -> Dict[str, float]:
    entry_low = price * 1.01
    entry_high = price * 1.03
    entry_mid = (entry_low + entry_high) / 2
    sl = entry_mid * 1.05
    tp1 = price * 0.97
    tp2 = price * 0.95
    return {
        "entry_low": entry_low,
        "entry_high": entry_high,
        "entry_mid": entry_mid,
        "sl": sl,
        "tp1": tp1,
        "tp2": tp2,
    }


def _format_plan(signal: Dict[str, Any]) -> str:
    price = signal["price"]

    if signal["type"] == "pump":
        plan = _build_pump_plan(price)
        how_to = (
            "Как использовать (наблюдение):\n"
            "• резкие импульсы часто дают откат/перезалив\n"
            "• вход рассматривается только после подтверждения на 1–5m\n\n"
        )
        cancel_text = "• если цена уходит ниже — сценарий отката/продолжения ломается"
    else:
        plan = _build_dump_plan(price)
        how_to = (
            "Как использовать (наблюдение):\n"
            "• резкие импульсы часто дают отскок/перезалив\n"
            "• вход рассматривается только после подтверждения на 1–5m\n\n"
        )
        cancel_text = "• если цена уходит выше — сценарий отката/продолжения ломается"

    text = (
        f"{how_to}"
        "Зона интереса (POI):\n"
        f"• {_format_price(plan['entry_low'])} – {_format_price(plan['entry_high'])}  (откат ~2–3%)\n\n"
        "Уровень отмены сценария:\n"
        f"• {_format_price(plan['sl'])}\n"
        f"{cancel_text}\n\n"
        "Потенциальные уровни движения:\n"
        f"• 🎯 Уровень 1: {_format_price(plan['tp1'])}\n"
        f"• 🎯 Уровень 2: {_format_price(plan['tp2'])}\n\n"
        "⚠️ Резкие импульсы высокорисковые: возможен вход на “вершине”.\n"
        "Бот не знает твой депозит и не управляет рисками.\n"
        "Источник данных: Binance"
    )
    return text


def format_pump_message(signal: Dict[str, Any]) -> str:
    symbol_pair = _format_symbol_pair(signal["symbol"])
    price = signal["price"]
    ch1 = signal["change_1m"]
    ch5 = signal["change_5m"]
    volume_mul = signal["volume_mul"]

    header = (
        "🚀 Pump/Dump Scanner: резкий импульс"
        if signal["type"] == "pump"
        else "📉 Pump/Dump Scanner: резкий импульс"
    )

    text = (
        f"{header}\n\n"
        f"Монета: {symbol_pair}\n"
        f"Текущая цена: {_format_price(price)}\n\n"
        "Движение:\n"
        f"• за 1 мин: {_format_signed(ch1)}%\n"
        f"• за 5 мин: {_format_signed(ch5)}%\n"
        f"• объём: {volume_mul:.2f}× от среднего\n\n"
        f"{_format_plan(signal)}"
    )
    return text


async def generate_pump_alert(symbol: str) -> str | None:
    """
    Получает последние 1m свечи по символу и формирует текст алерта,
    если обнаружен памп или дамп.

    Использует тот же пороговый анализ, что и scan_pumps(), но работает
    через binance_client.fetch_klines, чтобы использовать общую логику
    получения данных.
    """

    klines = await fetch_klines(symbol, "1m", 25)
    if len(klines) < 6:
        return None

    closes = [k.close for k in klines]
    volumes = [k.volume for k in klines]

    last_price = closes[-1]
    price_1m = closes[-2]
    price_5m = closes[-6]

    change_1m = (last_price / price_1m - 1) * 100
    change_5m = (last_price / price_5m - 1) * 100

    volume_5m = sum(volumes[-5:])
    avg_volume_1m = sum(volumes[:-5]) / max(1, len(volumes) - 5)
    avg_volume_5m = avg_volume_1m * 5
    if avg_volume_5m <= 0:
        return None
    volume_mul = volume_5m / avg_volume_5m

    if last_price < MIN_PRICE_USDT:
        return None

    volume_5m_usdt = volume_5m * last_price
    if volume_5m_usdt < MIN_VOLUME_5M_USDT:
        return None

    sig_type = None
    if (
        change_1m >= PUMP_1M_THRESHOLD
        and change_5m >= PUMP_5M_THRESHOLD
        and volume_mul >= PUMP_VOLUME_MUL
    ):
        sig_type = "pump"
    elif (
        change_1m <= DUMP_1M_THRESHOLD
        and change_5m <= DUMP_5M_THRESHOLD
        and volume_mul >= PUMP_VOLUME_MUL
    ):
        sig_type = "dump"

    if not sig_type:
        return None

    signal = {
        "symbol": symbol,
        "price": last_price,
        "change_1m": round(change_1m, 2),
        "change_5m": round(change_5m, 2),
        "volume_mul": round(volume_mul, 2),
        "type": sig_type,
        "detected_at": time.time(),
    }

    if not _remember_signal(signal):
        return None

    return format_pump_message(signal)
