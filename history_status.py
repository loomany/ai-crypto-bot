from __future__ import annotations

import time
from typing import Any

_STATUS_BADGE_MAP: dict[str, str] = {
    "TP": "🟢",
    "SL": "🔴",
    "EXPIRED_NO_ENTRY": "⚪",
    "NO_CONFIRMATION": "🔵",
    "ACTIVATED": "🟡",
    "POI_TOUCHED": "🟠",
    "IN_PROGRESS": "🟣",
}

_TERMINAL_TP = {"TP", "TP1", "TP2", "TP_HIT", "TP1_HIT", "TP2_HIT", "BE", "PASSED"}
_TERMINAL_SL = {"SL", "SL_HIT", "FAILED"}
_TERMINAL_EXPIRED = {"EXPIRED_NO_ENTRY", "EXPIRED", "EXP", "NO_FILL", "NF", "NEUTRAL"}
_TERMINAL_NO_CONFIRM = {"NO_CONFIRM", "NO_CONFIRMATION", "CONFIRM_TIMEOUT", "AMBIGUOUS"}

_ACTIVATED_STATES = {"ACTIVE", "ACTIVE_CONFIRMED", "ACTIVATED", "ENTRY_CONFIRMED"}
_POI_STATES = {"POI_TOUCHED"}


def _as_int(value: Any) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return 0


def _collect_status_tokens(signal: dict[str, Any]) -> set[str]:
    tokens: set[str] = set()
    for field in ("result", "outcome", "status", "state"):
        raw = str(signal.get(field) or "").strip().upper()
        if raw:
            tokens.add(raw)
    return tokens


def _infer_expired_from_ttl(signal: dict[str, Any], now_ts: int) -> bool:
    expires_at = _as_int(signal.get("expires_at"))
    if expires_at <= 0:
        base_ts = _as_int(signal.get("created_at")) or _as_int(signal.get("ts"))
        ttl_minutes = _as_int(signal.get("ttl_minutes"))
        if base_ts > 0 and ttl_minutes > 0:
            expires_at = base_ts + ttl_minutes * 60
    return expires_at > 0 and now_ts > expires_at


def get_signal_status_key(signal: dict[str, Any], *, now_ts: int | None = None) -> str:
    """Return normalized history status key with strict terminal priority."""
    now_value = int(time.time()) if now_ts is None else int(now_ts)
    tokens = _collect_status_tokens(signal)

    if tokens & _TERMINAL_TP:
        return "TP"
    if tokens & _TERMINAL_SL:
        return "SL"
    if tokens & _TERMINAL_EXPIRED:
        return "EXPIRED_NO_ENTRY"
    if tokens & _TERMINAL_NO_CONFIRM:
        return "NO_CONFIRMATION"

    if _infer_expired_from_ttl(signal, now_value):
        return "EXPIRED_NO_ENTRY"

    activated_at = _as_int(signal.get("activated_at"))
    if activated_at > 0 or (tokens & _ACTIVATED_STATES):
        return "ACTIVATED"

    poi_touched_at = _as_int(signal.get("poi_touched_at"))
    if poi_touched_at > 0 or (tokens & _POI_STATES):
        return "POI_TOUCHED"

    return "IN_PROGRESS"


def is_terminal_status_key(status_key: str) -> bool:
    return status_key in {"TP", "SL", "EXPIRED_NO_ENTRY", "NO_CONFIRMATION"}


def get_signal_badge(signal: dict[str, Any], *, now_ts: int | None = None) -> str:
    status_key = get_signal_status_key(signal, now_ts=now_ts)
    return _STATUS_BADGE_MAP.get(status_key, "🟣")
