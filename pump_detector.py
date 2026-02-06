import asyncio
import os
import re
import time
from typing import Any, Callable, Dict, List, Optional, Tuple

import aiohttp

import i18n

from binance_client import Candle
from binance_rest import binance_request_context, get_klines, is_binance_degraded
from symbol_cache import (
    filter_tradeable_symbols,
    get_spot_usdt_symbols,
    get_top_usdt_symbols_by_movers,
    get_top_usdt_symbols_by_volume,
)

PUMPDUMP_1M_INTERVAL = "1m"
PUMPDUMP_1M_LIMIT = int(
    os.getenv("PUMP_LIMIT_1M", os.getenv("PUMPDUMP_1M_LIMIT", "120"))
)
PUMPDUMP_5M_INTERVAL = "5m"
PUMPDUMP_5M_LIMIT = int(
    os.getenv("PUMP_LIMIT_5M", os.getenv("PUMPDUMP_5M_LIMIT", "120"))
)
PUMP_1M_THRESHOLD = float(os.getenv("PUMP_1M_THRESHOLD", "1.6"))
PUMP_5M_THRESHOLD = float(os.getenv("PUMP_5M_THRESHOLD", "3.2"))
DUMP_1M_THRESHOLD = float(os.getenv("DUMP_1M_THRESHOLD", "-1.6"))
DUMP_5M_THRESHOLD = float(os.getenv("DUMP_5M_THRESHOLD", "-3.2"))
PUMP_VOLUME_MUL = float(os.getenv("PUMP_VOLUME_MUL", "1.8"))
COOLDOWN_SEC = 60
DUP_CHANGE_THRESHOLD = 0.15
MIN_PRICE_USDT = 0.0005
MIN_VOLUME_5M_USDT_AI = float(
    os.getenv("MIN_VOLUME_5M_USDT_AI", os.getenv("MIN_VOLUME_5M_USDT", "2500"))
)
MIN_VOLUME_5M_USDT_PUMPDUMP = float(
    os.getenv("MIN_VOLUME_5M_USDT_PUMPDUMP", os.getenv("MIN_VOLUME_5M_USDT", "2500"))
)
MIN_VOLUME_5M_USDT = MIN_VOLUME_5M_USDT_PUMPDUMP
PRIORITY_LIMIT = 250
BATCH_SIZE = 20
PUMP_CHUNK_SIZE = int(os.getenv("PUMP_CHUNK_SIZE", "60"))
PUMP_DEGRADED_SYMBOL_LIMIT = int(os.getenv("PUMP_DEGRADED_SYMBOL_LIMIT", "40"))
PUMPDUMP_TOP_GAINERS_N = int(os.getenv("PUMPDUMP_TOP_GAINERS_N", "0"))
PUMPDUMP_TOP_LOSERS_N = int(os.getenv("PUMPDUMP_TOP_LOSERS_N", "0"))
MAX_CYCLE_SEC = 30
SYMBOL_REGEX = re.compile(r"^[A-Z0-9]{2,20}USDT$")
_last_signals: dict[str, Dict[str, Any]] = {}
PUMP_FALLBACK_DIRECT = 0


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
) -> list[str] | tuple[list[str], dict[str, int], list[str]]:
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
        return result, {"total": len(symbols) + removed, "removed": removed, "final": len(result)}, symbols
    return result


async def get_shared_klines(
    symbol: str,
    interval: str,
    limit: int,
) -> list[list[str]] | list[Candle] | None:
    try:
        with binance_request_context("pumpdump"):
            return await get_klines(symbol, interval, limit, start_ms=None)
    except Exception as exc:
        print(f"[BINANCE] ERROR {symbol}: {exc}")
        return None


def get_pump_fallback_direct() -> int:
    return PUMP_FALLBACK_DIRECT


def _inc_pump_fallback_direct() -> None:
    global PUMP_FALLBACK_DIRECT
    PUMP_FALLBACK_DIRECT += 1


def _passes_5m_trigger(
    klines_5m: list[list[str]] | list[Candle],
) -> tuple[bool, str]:
    if not isinstance(klines_5m, list):
        return False, "fail_short_5m_series"
    if len(klines_5m) < 2:
        return False, "fail_short_5m_series"
    if isinstance(klines_5m[0], Candle):
        closes_5m = [float(k.close) for k in klines_5m]
        volumes_5m = [float(k.volume) for k in klines_5m]
    else:
        closes_5m = [float(k[4]) for k in klines_5m]
        volumes_5m = [float(k[5]) for k in klines_5m]

    last_price = closes_5m[-1]
    price_5m = closes_5m[-2]
    change_5m = (last_price / price_5m - 1) * 100
    volume_5m = volumes_5m[-1]
    avg_volume_5m = sum(volumes_5m[:-1]) / max(1, len(volumes_5m) - 1)
    if avg_volume_5m <= 0:
        return False, "fail_avg_volume"
    volume_mul = volume_5m / avg_volume_5m
    if last_price < MIN_PRICE_USDT:
        return False, "fail_min_price"
    volume_5m_usdt = volume_5m * last_price
    if volume_5m_usdt < MIN_VOLUME_5M_USDT:
        return False, "fail_min_volume_5m_usdt"
    if (change_5m >= PUMP_5M_THRESHOLD or change_5m <= DUMP_5M_THRESHOLD) and (
        volume_mul >= PUMP_VOLUME_MUL
    ):
        return True, "ok"
    return False, "fail_no_trigger_5m"


async def _fetch_5m_klines(symbol: str) -> list[list[str]] | list[Candle] | None:
    klines_5m = await get_shared_klines(symbol, PUMPDUMP_5M_INTERVAL, PUMPDUMP_5M_LIMIT)
    if klines_5m:
        _inc_pump_fallback_direct()
    return klines_5m


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
    klines_1m: list[list[str]] | list[Candle],
    klines_5m: list[list[str]] | list[Candle],
) -> tuple[Dict[str, Any] | None, str]:
    if not isinstance(klines_1m, list):
        klines_1m = []
    if not isinstance(klines_5m, list):
        klines_5m = []
    if not klines_1m or len(klines_1m) < 2:
        return None, "fail_short_1m_series"
    if not klines_5m or len(klines_5m) < 2:
        return None, "fail_short_5m_series"

    if isinstance(klines_1m[0], Candle):
        closes_1m = [float(k.close) for k in klines_1m]
    else:
        closes_1m = [float(k[4]) for k in klines_1m]

    if isinstance(klines_5m[0], Candle):
        closes_5m = [float(k.close) for k in klines_5m]
        volumes_5m = [float(k.volume) for k in klines_5m]
    else:
        closes_5m = [float(k[4]) for k in klines_5m]
        volumes_5m = [float(k[5]) for k in klines_5m]

    last_price = closes_1m[-1]
    price_1m = closes_1m[-2]
    change_1m = (last_price / price_1m - 1) * 100

    price_5m = closes_5m[-2]
    last_price_5m = closes_5m[-1]
    change_5m = (last_price_5m / price_5m - 1) * 100

    volume_5m = volumes_5m[-1]
    avg_volume_5m = sum(volumes_5m[:-1]) / max(1, len(volumes_5m) - 1)
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
    klines_1m: list[list[str]] | list[Candle],
    klines_5m: list[list[str]] | list[Candle],
) -> Dict[str, Any] | None:
    sig, _ = _calc_signal_with_reason(symbol, klines_1m, klines_5m)
    return sig


async def build_pump_symbol_list(
    session: aiohttp.ClientSession,
    *,
    priority_limit: int = PRIORITY_LIMIT,
) -> list[str]:
    if is_binance_degraded():
        priority_limit = min(priority_limit, PUMP_DEGRADED_SYMBOL_LIMIT)
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
    if is_binance_degraded():
        return (priority + rest)[:PUMP_DEGRADED_SYMBOL_LIMIT]
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

    if is_binance_degraded():
        max_symbols = min(max_symbols, PUMP_DEGRADED_SYMBOL_LIMIT)
    end_idx = min(start_idx + max_symbols, len(symbols))
    start_ts = time.time()

    async def _fetch_symbol_klines(symbol: str) -> tuple[str, Any, Any]:
        klines_5m = await _fetch_5m_klines(symbol)
        return symbol, klines_5m, None

    for i in range(start_idx, end_idx, batch_size):
        if time_budget_sec is not None and time.time() - start_ts >= time_budget_sec:
            break
        batch = symbols[i : i + batch_size]
        tasks = [asyncio.create_task(_fetch_symbol_klines(symbol)) for symbol in batch]
        klines_list = await asyncio.gather(*tasks, return_exceptions=True)

        for symbol, klines in zip(batch, klines_list):
            if time_budget_sec is not None and time.time() - start_ts >= time_budget_sec:
                break
            if progress_cb:
                progress_cb(symbol)
            checked += 1
            if isinstance(klines, BaseException) or isinstance(klines, asyncio.CancelledError):
                fails["fail_klines_exception"] = fails.get("fail_klines_exception", 0) + 1
                continue
            _, klines_5m, _ = klines
            if not isinstance(klines_5m, list):
                klines_5m = []
            trigger_ok, trigger_reason = _passes_5m_trigger(klines_5m)
            if not trigger_ok:
                fails[trigger_reason] = fails.get(trigger_reason, 0) + 1
                continue

            klines_1m = await get_shared_klines(symbol, PUMPDUMP_1M_INTERVAL, PUMPDUMP_1M_LIMIT)
            if klines_1m:
                _inc_pump_fallback_direct()
            if not isinstance(klines_1m, list):
                klines_1m = []
            sig, reason = _calc_signal_with_reason(symbol, klines_1m, klines_5m)
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
            tasks = [asyncio.create_task(_fetch_5m_klines(symbol)) for symbol in batch]
            klines_list = await asyncio.gather(*tasks, return_exceptions=True)

            for symbol, klines in zip(batch, klines_list):
                checked += 1
                if isinstance(klines, BaseException) or isinstance(klines, asyncio.CancelledError):
                    continue
                klines_5m = klines
                if not isinstance(klines_5m, list):
                    klines_5m = []
                trigger_ok, _ = _passes_5m_trigger(klines_5m)
                if not trigger_ok:
                    continue
                klines_1m = await get_shared_klines(symbol, PUMPDUMP_1M_INTERVAL, PUMPDUMP_1M_LIMIT)
                if klines_1m:
                    _inc_pump_fallback_direct()
                if not isinstance(klines_1m, list):
                    klines_1m = []
                sig = _calc_signal_from_klines(symbol, klines_1m, klines_5m)
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


def format_pump_message(signal: Dict[str, Any], lang: str = "ru") -> str:
    symbol_pair = _format_symbol_pair(signal["symbol"])
    price = signal["price"]
    ch1 = signal["change_1m"]
    ch5 = signal["change_5m"]
    volume_mul = signal["volume_mul"]

    header = i18n.t(
        lang,
        "PUMP_HEADER_PUMP" if signal["type"] == "pump" else "PUMP_HEADER_DUMP",
    )

    text = (
        f"{header}\n\n"
        f"{i18n.t(lang, 'PUMP_COIN_LINE', symbol=symbol_pair)}\n"
        f"{i18n.t(lang, 'PUMP_PRICE_LINE', price=_format_price(price))}\n\n"
        f"{i18n.t(lang, 'PUMP_MOVE_HEADER')}\n"
        f"{i18n.t(lang, 'PUMP_MOVE_1M', change=_format_signed(ch1))}\n"
        f"{i18n.t(lang, 'PUMP_MOVE_5M', change=_format_signed(ch5))}\n"
        f"{i18n.t(lang, 'PUMP_VOLUME_LINE', volume=volume_mul)}\n\n"
        f"{i18n.t(lang, 'PUMP_NOTE_1')}\n"
        f"{i18n.t(lang, 'PUMP_NOTE_2')}\n\n"
        f"{i18n.t(lang, 'PUMP_RISK_1')}\n"
        f"{i18n.t(lang, 'PUMP_RISK_2')}\n\n"
        f"{i18n.t(lang, 'PUMP_SOURCE')}"
    )
    return text


async def generate_pump_alert(symbol: str) -> str | None:
    """
    Получает последние 1m свечи по символу и формирует текст алерта,
    если обнаружен памп или дамп.

    Использует тот же пороговый анализ, что и scan_pumps(), но работает
    через binance_rest.get_klines, чтобы использовать общий кеш свечей.
    """

    with binance_request_context("pumpdump"):
        klines_1m, klines_5m = await asyncio.gather(
            get_klines(symbol, PUMPDUMP_1M_INTERVAL, PUMPDUMP_1M_LIMIT, start_ms=None),
            get_klines(symbol, PUMPDUMP_5M_INTERVAL, PUMPDUMP_5M_LIMIT, start_ms=None),
            return_exceptions=True,
        )
    if isinstance(klines_1m, BaseException):
        klines_1m = []
    if isinstance(klines_5m, BaseException):
        klines_5m = []
    if not isinstance(klines_1m, list):
        klines_1m = []
    if not isinstance(klines_5m, list):
        klines_5m = []
    if not klines_1m or not klines_5m or len(klines_1m) < 2 or len(klines_5m) < 2:
        return None

    if isinstance(klines_1m[0], Candle):
        closes_1m = [k.close for k in klines_1m]
    else:
        closes_1m = [float(k[4]) for k in klines_1m]

    if isinstance(klines_5m[0], Candle):
        closes_5m = [k.close for k in klines_5m]
        volumes_5m = [k.volume for k in klines_5m]
    else:
        closes_5m = [float(k[4]) for k in klines_5m]
        volumes_5m = [float(k[5]) for k in klines_5m]

    last_price = closes_1m[-1]
    price_1m = closes_1m[-2]
    price_5m = closes_5m[-2]
    last_price_5m = closes_5m[-1]

    change_1m = (last_price / price_1m - 1) * 100
    change_5m = (last_price_5m / price_5m - 1) * 100

    volume_5m = volumes_5m[-1]
    avg_volume_5m = sum(volumes_5m[:-1]) / max(1, len(volumes_5m) - 1)
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
