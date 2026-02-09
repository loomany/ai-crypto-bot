import asyncio
import json
import os
import time
from dataclasses import dataclass, field
from typing import Optional, Dict, Callable, Awaitable, Any

from db import get_state, set_state


@dataclass
class ModuleStatus:
    name: str
    last_tick: float = 0.0
    last_ok: float = 0.0
    last_error: Optional[str] = None
    last_warn: Optional[str] = None
    extra: str = ""
    last_stats: Optional[Dict[str, Any]] = None
    fails_top: str = ""
    near_miss: str = ""
    universe_debug: str = ""
    total_symbols: int = 0
    cursor: int = 0
    checked_last_cycle: int = 0
    current_symbol: Optional[str] = None
    last_progress_ts: float = 0.0
    requests_last_cycle: int = 0
    klines_requests_last_cycle: int = 0
    binance_last_success_ts: float = 0.0
    binance_consecutive_timeouts: int = 0
    binance_current_stage: str = ""
    binance_session_restarts: int = 0
    state: Dict[str, Any] = field(default_factory=dict)

    def as_text(self) -> str:
        now = time.time()
        if self.last_tick == 0:
            state = "⛔ ещё ни разу не запускался"
        else:
            sec_ago = int(now - self.last_tick)
            state = f"✅ тикает, {sec_ago} с назад"

        if self.last_ok > 0:
            ok_ago = int(now - self.last_ok)
            state += f" | последний успешный запрос: {ok_ago} с назад"

        if self.last_error:
            state += f"\n   ⚠️ ошибка: {self.last_error}"

        if self.last_warn:
            state += f"\n   ⚠️ предупреждение: {self.last_warn}"

        if self.extra:
            state += f"\n   ℹ️ {self.extra}"

        binance_last_ok = (
            f"{int(now - self.binance_last_success_ts)}s"
            if self.binance_last_success_ts
            else "n/a"
        )
        stage = self.binance_current_stage or "-"
        state += (
            "\n   🌐 Binance:"
            f" last_ok={binance_last_ok}"
            f" | timeouts={self.binance_consecutive_timeouts}"
            f" | stage={stage}"
            f" | session_restart={self.binance_session_restarts}"
        )

        return state


MODULES: Dict[str, ModuleStatus] = {
    "ai_signals": ModuleStatus("🎯 AI-сигналы"),
    "pumpdump": ModuleStatus("🚀 Pump/Dump Scanner"),
    "signal_audit": ModuleStatus("🧾 Signal Audit"),
}

SCAN_INTERVAL = 60  # seconds, strict
AI_CYCLE_SLEEP_SEC = float(os.getenv("AI_CYCLE_SLEEP_SEC", "2"))
PUMP_CYCLE_SLEEP_SEC = float(os.getenv("PUMP_CYCLE_SLEEP_SEC", "3"))
_PERSIST_THROTTLE_SEC = 5.0
_LAST_PERSIST_TS: Dict[str, float] = {}


def _state_key(key: str) -> str:
    return f"module_status:{key}"


def _serialize_module_status(st: ModuleStatus) -> dict:
    return {
        "last_tick": st.last_tick,
        "last_ok": st.last_ok,
        "last_error": st.last_error,
        "last_warn": st.last_warn,
        "extra": st.extra,
        "total_symbols": st.total_symbols,
        "cursor": st.cursor,
        "checked_last_cycle": st.checked_last_cycle,
        "current_symbol": st.current_symbol,
        "last_progress_ts": st.last_progress_ts,
        "binance_last_success_ts": st.binance_last_success_ts,
        "binance_consecutive_timeouts": st.binance_consecutive_timeouts,
        "binance_current_stage": st.binance_current_stage,
        "binance_session_restarts": st.binance_session_restarts,
        "state": st.state,
    }


def persist_module_status(key: str, *, force: bool = False) -> None:
    st = MODULES.get(key)
    if not st:
        return
    now = time.time()
    last_ts = _LAST_PERSIST_TS.get(key, 0.0)
    if not force and now - last_ts < _PERSIST_THROTTLE_SEC:
        return
    payload = _serialize_module_status(st)
    set_state(_state_key(key), json.dumps(payload, ensure_ascii=False, default=str))
    _LAST_PERSIST_TS[key] = now


def load_module_statuses() -> None:
    for key, st in MODULES.items():
        payload = get_state(_state_key(key))
        if not payload:
            continue
        try:
            data = json.loads(payload)
        except (TypeError, ValueError):
            continue
        if not isinstance(data, dict):
            continue
        st.last_tick = float(data.get("last_tick", st.last_tick) or 0.0)
        st.last_ok = float(data.get("last_ok", st.last_ok) or 0.0)
        st.last_error = data.get("last_error")
        st.last_warn = data.get("last_warn")
        st.extra = data.get("extra") or st.extra
        st.total_symbols = int(data.get("total_symbols", st.total_symbols) or 0)
        st.cursor = int(data.get("cursor", st.cursor) or 0)
        st.checked_last_cycle = int(
            data.get("checked_last_cycle", st.checked_last_cycle) or 0
        )
        st.current_symbol = data.get("current_symbol") or st.current_symbol
        st.last_progress_ts = float(data.get("last_progress_ts", st.last_progress_ts) or 0.0)
        st.binance_last_success_ts = float(
            data.get("binance_last_success_ts", st.binance_last_success_ts) or 0.0
        )
        st.binance_consecutive_timeouts = int(
            data.get("binance_consecutive_timeouts", st.binance_consecutive_timeouts) or 0
        )
        st.binance_current_stage = data.get("binance_current_stage") or st.binance_current_stage
        st.binance_session_restarts = int(
            data.get("binance_session_restarts", st.binance_session_restarts) or 0
        )
        state = data.get("state")
        if isinstance(state, dict):
            st.state.update(state)


def mark_tick(key: str, extra: str = ""):
    st = MODULES.get(key)
    if not st:
        return
    st.last_tick = time.time()
    if extra:
        st.extra = extra
    persist_module_status(key)


def mark_ok(key: str, extra: str = ""):
    st = MODULES.get(key)
    if not st:
        return
    now = time.time()
    st.last_tick = now
    st.last_ok = now
    if extra:
        st.extra = extra
    persist_module_status(key)


def mark_error(key: str, err: str):
    st = MODULES.get(key)
    if not st:
        return
    st.last_tick = time.time()
    st.last_error = err[:200]
    persist_module_status(key, force=True)


def mark_warn(key: str, warn: str):
    st = MODULES.get(key)
    if not st:
        return
    st.last_tick = time.time()
    st.last_warn = warn[:200]
    persist_module_status(key, force=True)


def update_module_progress(
    key: str,
    total_symbols: int,
    cursor: int,
    checked_last_cycle: int,
) -> None:
    st = MODULES.get(key)
    if not st:
        return
    st.total_symbols = total_symbols
    st.cursor = cursor
    st.checked_last_cycle = checked_last_cycle
    persist_module_status(key)


def update_current_symbol(
    key: str,
    symbol: str,
    throttle_sec: float = 3.0,
) -> None:
    st = MODULES.get(key)
    if not st or not symbol:
        return
    now = time.time()
    if now - st.last_progress_ts >= throttle_sec:
        st.current_symbol = symbol
        st.last_progress_ts = now
        persist_module_status(key)


def reset_request_count(key: str) -> None:
    st = MODULES.get(key)
    if not st:
        return
    st.requests_last_cycle = 0


def reset_klines_request_count(key: str) -> None:
    st = MODULES.get(key)
    if not st:
        return
    st.klines_requests_last_cycle = 0


def increment_request_count(key: str, count: int = 1) -> None:
    st = MODULES.get(key)
    if not st:
        return
    st.requests_last_cycle += count


def increment_klines_request_count(key: str, count: int = 1) -> None:
    st = MODULES.get(key)
    if not st:
        return
    st.klines_requests_last_cycle += count


def update_binance_global_state(
    *,
    last_success_ts: float | None = None,
    consecutive_timeouts: int | None = None,
    session_restarts: int | None = None,
) -> None:
    for st in MODULES.values():
        if last_success_ts is not None:
            st.binance_last_success_ts = last_success_ts
        if consecutive_timeouts is not None:
            st.binance_consecutive_timeouts = consecutive_timeouts
        if session_restarts is not None:
            st.binance_session_restarts = session_restarts
    for key in MODULES:
        persist_module_status(key)


def update_binance_stage(module: str, stage: str) -> None:
    st = MODULES.get(module)
    if not st:
        return
    st.binance_current_stage = stage
    persist_module_status(module)


def get_request_count(key: str) -> int:
    st = MODULES.get(key)
    if not st:
        return 0
    return st.requests_last_cycle


def get_klines_request_count(key: str) -> int:
    st = MODULES.get(key)
    if not st:
        return 0
    return st.klines_requests_last_cycle


async def safe_worker_loop(module_name: str, scan_once_coro):
    while True:
        cycle_start = time.perf_counter()
        timeout_s = 55
        print(f"[{module_name}] cycle start")

        # 🔴 HEARTBEAT — ВСЕГДА, СРАЗУ
        mark_tick(module_name, extra="cycle heartbeat")

        t0 = time.perf_counter()
        try:
            # ❗ Ограничиваем ВЕСЬ scan_once по времени
            await asyncio.wait_for(scan_once_coro(), timeout=timeout_s)
            print(f"[{module_name}] cycle ok, dt={time.perf_counter() - t0:.2f}s")
        except asyncio.TimeoutError:
            print(
                f"[{module_name}] TIMEOUT >{timeout_s}s, dt={time.perf_counter() - t0:.2f}s"
            )
            mark_warn(module_name, f"timeout >{timeout_s}s")
        except Exception as e:
            print(f"[{module_name}] ERROR {type(e).__name__}: {e}")
            mark_error(module_name, str(e))

        elapsed = time.perf_counter() - cycle_start
        module_state = MODULES.get(module_name)
        if module_state and module_state.extra:
            mark_tick(module_name)
        else:
            mark_tick(module_name, extra=f"cycle={int(elapsed)}s")
        if module_name == "ai_signals":
            cycle_sleep = max(0.0, AI_CYCLE_SLEEP_SEC)
            if cycle_sleep:
                await asyncio.sleep(cycle_sleep)
        if module_name == "pumpdump":
            cycle_sleep = max(0.0, PUMP_CYCLE_SLEEP_SEC)
            if cycle_sleep:
                await asyncio.sleep(cycle_sleep)
        await asyncio.sleep(max(0, SCAN_INTERVAL - elapsed))


async def watchdog() -> None:
    while True:
        now = time.time()
        for name, module in MODULES.items():
            last = module.last_tick
            if last and now - last > 120:
                print(f"[WATCHDOG] {name} stalled: {int(now - last)}s")
        await asyncio.sleep(30)
