import time
from typing import List, Dict, Any

import aiohttp

from binance_client import fetch_klines

BINANCE_API = "https://api.binance.com"
PUMP_1M_THRESHOLD = 2.5
PUMP_5M_THRESHOLD = 5.0
DUMP_1M_THRESHOLD = 2.5
DUMP_5M_THRESHOLD = 5.0
PUMP_VOLUME_THRESHOLD = 2.0
COOLDOWN_SEC = 60 * 30
DUP_CHANGE_THRESHOLD = 0.5
MIN_PRICE_USDT = 0.0005
MIN_VOLUME_5M_USDT = 10_000
_last_signals: dict[str, Dict[str, Any]] = {}


async def fetch_json(session: aiohttp.ClientSession, url: str, params: dict | None = None):
    async with session.get(url, params=params, timeout=10) as resp:
        resp.raise_for_status()
        return await resp.json()


async def get_usdt_symbols(session: aiohttp.ClientSession) -> list[str]:
    """
    Получаем ВСЕ спотовые пары к USDT, которые сейчас торгуются.
    """
    data = await fetch_json(session, f"{BINANCE_API}/api/v3/exchangeInfo")
    symbols = []
    for s in data["symbols"]:
        if (
            s.get("status") == "TRADING"
            and s.get("quoteAsset") == "USDT"
            and s.get("isSpotTradingAllowed", True)
        ):
            sym = s["symbol"]
            if any(x in sym for x in ("UPUSDT", "DOWNUSDT", "3LUSDT", "3SUSDT")):
                continue
            symbols.append(sym)
    return symbols


async def get_klines_1m(session: aiohttp.ClientSession, symbol: str, limit: int = 25):
    params = {"symbol": symbol, "interval": "1m", "limit": limit}
    return await fetch_json(session, f"{BINANCE_API}/api/v3/klines", params=params)


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


def _calc_signal_from_klines(symbol: str, klines: list[list[str]]) -> Dict[str, Any] | None:
    if len(klines) < 6:
        return None

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
        and volume_mul >= PUMP_VOLUME_THRESHOLD
    ):
        sig_type = "pump"
    elif (
        change_1m <= -DUMP_1M_THRESHOLD
        and change_5m <= -DUMP_5M_THRESHOLD
        and volume_mul >= PUMP_VOLUME_THRESHOLD
    ):
        sig_type = "dump"

    if not sig_type:
        return None

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
        return None

    return signal


async def scan_pumps() -> List[Dict[str, Any]]:
    """
    Сканирует все USDT-пары и возвращает список обнаруженных пампов или дампов.
    """
    results: list[Dict[str, Any]] = []

    async with aiohttp.ClientSession() as session:
        symbols = await get_usdt_symbols(session)

        for symbol in symbols:
            try:
                klines = await get_klines_1m(session, symbol, limit=25)
            except Exception:
                continue

            sig = _calc_signal_from_klines(symbol, klines)
            if sig:
                results.append(sig)

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
    base_capital = 100.0

    if signal["type"] == "pump":
        plan = _build_pump_plan(price)
        tp1_pct = (plan["tp1"] / plan["entry_mid"] - 1) * 100
        tp2_pct = (plan["tp2"] / plan["entry_mid"] - 1) * 100
        sl_pct = (plan["sl"] / plan["entry_mid"] - 1) * 100
        header = "Рекомендации (памп):"
        entry_type = "Тип входа: откат после импульса"
        stop_label = "Стоп (SL):"
        stop_line = f"• {_format_price(plan['sl'])}  ({_format_signed(sl_pct, 1)}%)\n\n"
        tp1_label = "Цели:"
        warning = "⚠️ Пампы крайне рискованные — высокая вероятность зайти на вершине."
    else:
        plan = _build_dump_plan(price)
        tp1_pct = (plan["entry_mid"] / plan["tp1"] - 1) * 100
        tp2_pct = (plan["entry_mid"] / plan["tp2"] - 1) * 100
        sl_pct = (plan["sl"] / plan["entry_mid"] - 1) * 100
        header = "Рекомендации (дамп):"
        entry_type = "Тип входа: short после отката вверх"
        stop_label = "Стоп (SL):"
        stop_line = f"• {_format_price(plan['sl'])}  (убыток ~{_format_signed(sl_pct, 1).replace('+', '')}%)\n\n"
        tp1_label = "Цели:"
        warning = "⚠️ Резкий дамп — высокий риск “поймать нож”. Торгуй только при хорошем опыте и строгом стопе."

    tp1_usdt = base_capital * tp1_pct / 100
    tp2_usdt = base_capital * tp2_pct / 100
    sl_pnl_pct = sl_pct if signal["type"] == "pump" else -sl_pct
    sl_usdt = base_capital * sl_pnl_pct / 100

    change_format = lambda v: f"{_format_signed(v, 1)}%"

    if signal["type"] == "dump":
        tp1_display = f"профит ~{_format_signed(tp1_pct, 1).replace('+', '')}%"
        tp2_display = f"профит ~{_format_signed(tp2_pct, 1).replace('+', '')}%"
    else:
        tp1_display = change_format(tp1_pct)
        tp2_display = change_format(tp2_pct)

    text = (
        f"{header}\n"
        f"{entry_type}\n\n"
        "Зона входа:\n"
        f"• {_format_price(plan['entry_low'])} – {_format_price(plan['entry_high'])}  (откат ~2–3%)\n\n"
        f"{stop_label}\n"
        f"{stop_line}"
        f"{tp1_label}\n"
        f"• TP1: {_format_price(plan['tp1'])}  ({tp1_display})\n"
        f"• TP2: {_format_price(plan['tp2'])}  ({tp2_display})\n\n"
        "Пример для позиции 100 USDT:\n"
        f"• До TP1: {_format_signed(tp1_usdt, 1)} USDT\n"
        f"• До TP2: {_format_signed(tp2_usdt, 1)} USDT\n"
        f"• До SL: {_format_signed(sl_usdt, 1)} USDT\n\n"
        f"{warning}\n"
        "Источник данных: Binance"
    )
    return text


def format_pump_message(signal: Dict[str, Any]) -> str:
    symbol_pair = _format_symbol_pair(signal["symbol"])
    price = signal["price"]
    ch1 = signal["change_1m"]
    ch5 = signal["change_5m"]
    volume_mul = signal["volume_mul"]

    header = "🚀 PUMP DETECTED!" if signal["type"] == "pump" else "📉 DUMP DETECTED!"

    text = (
        f"{header}\n\n"
        f"Монета: {symbol_pair}\n"
        f"Текущая цена: {_format_price(price)} USDT\n\n"
        "Движение:\n"
        f"• за 1 мин: {_format_signed(ch1)}%\n"
        f"• за 5 мин: {_format_signed(ch5)}%\n"
        f"• Объём: {volume_mul:.2f}x от среднего\n\n"
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
        and volume_mul >= PUMP_VOLUME_THRESHOLD
    ):
        sig_type = "pump"
    elif (
        change_1m <= -DUMP_1M_THRESHOLD
        and change_5m <= -DUMP_5M_THRESHOLD
        and volume_mul >= PUMP_VOLUME_THRESHOLD
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
