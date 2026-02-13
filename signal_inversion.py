from __future__ import annotations

from typing import Any


def _invert_side(side: str) -> str:
    normalized = str(side or "").upper()
    if normalized in {"LONG", "BUY", "long", "buy"}:
        return "SHORT"
    if normalized in {"SHORT", "SELL", "short", "sell"}:
        return "LONG"
    return normalized


def _side_to_direction(side: str) -> str:
    return "long" if str(side).upper() == "LONG" else "short"


def apply_inversion(signal: dict[str, Any]) -> dict[str, Any]:
    if not isinstance(signal, dict):
        return signal

    out = dict(signal)
    direction_raw = str(out.get("direction") or out.get("side") or "").upper()
    if direction_raw not in {"LONG", "SHORT", "BUY", "SELL"}:
        return out

    current_side = "LONG" if direction_raw in {"LONG", "BUY"} else "SHORT"
    new_side = _invert_side(current_side)

    entry_from = None
    entry_to = None
    if isinstance(out.get("entry_zone"), (list, tuple)):
        try:
            entry_from = float(out["entry_zone"][0])
            entry_to = float(out["entry_zone"][1])
        except (TypeError, ValueError, IndexError):
            entry_from = entry_to = None
    if entry_from is None or entry_to is None:
        try:
            entry_from = float(out.get("entry_from", 0.0) or 0.0)
            entry_to = float(out.get("entry_to", 0.0) or 0.0)
        except (TypeError, ValueError):
            entry_from = entry_to = 0.0

    entry_mid = (entry_from + entry_to) / 2.0
    d_low = abs(entry_mid - entry_from)
    d_high = abs(entry_to - entry_mid)
    new_low = entry_mid - d_high
    new_high = entry_mid + d_low

    def _distance_from_entry(value: Any) -> float:
        try:
            return abs(float(value) - entry_mid)
        except (TypeError, ValueError):
            return 0.0

    d_sl = _distance_from_entry(out.get("sl", 0.0))

    if new_side == "LONG":
        new_sl = entry_mid - d_sl
    else:
        new_sl = entry_mid + d_sl

    out["sl"] = new_sl
    out["entry_zone"] = (new_low, new_high)
    out["entry_from"] = new_low
    out["entry_to"] = new_high

    for tp_key in ("tp1", "tp2", "tp3", "tp4"):
        if tp_key not in out:
            continue
        d_tp = _distance_from_entry(out.get(tp_key, 0.0))
        if new_side == "LONG":
            out[tp_key] = entry_mid + d_tp
        else:
            out[tp_key] = entry_mid - d_tp

    if "direction" in out:
        out["direction"] = _side_to_direction(new_side)
    if "side" in out:
        out["side"] = new_side

    return out
