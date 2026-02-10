from __future__ import annotations

from typing import Any, MutableMapping, Optional

EPS = 1e-12


def safe_div(a: float, b: float | None, default: float = 0.0, eps: float = EPS) -> float:
    if b is None or abs(float(b)) <= eps:
        return default
    return float(a) / float(b)


def safe_pct(num: float, denom: float | None, default: float = 0.0, eps: float = EPS) -> float:
    return safe_div(float(num) * 100.0, denom, default=default, eps=eps)


def guarded_div(
    a: float,
    b: float | None,
    *,
    default: float = 0.0,
    eps: float = EPS,
    diag_state: Optional[MutableMapping[str, Any]] = None,
    logger: Any = None,
    symbol: str | None = None,
    expr: str = "",
    warned: Optional[set[str]] = None,
) -> float:
    if b is None or abs(float(b)) <= eps:
        if diag_state is not None:
            diag_state["zero_div_guard"] = int(diag_state.get("zero_div_guard", 0) or 0) + 1
        if logger is not None:
            warn_key = f"{symbol or '-'}:{expr}"
            if warned is None or warn_key not in warned:
                logger.warning(
                    "[zero_div_guard] symbol=%s expr=%s denom=%s",
                    symbol or "-",
                    expr,
                    b,
                )
                if warned is not None:
                    warned.add(warn_key)
        return default
    return float(a) / float(b)
