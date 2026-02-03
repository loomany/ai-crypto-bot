import asyncio
import time
from dataclasses import dataclass, field
from typing import Optional, Dict, Callable, Awaitable, Any


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


def mark_tick(key: str, extra: str = ""):
    st = MODULES.get(key)
    if not st:
        return
    st.last_tick = time.time()
    if extra:
        st.extra = extra


def mark_ok(key: str, extra: str = ""):
    st = MODULES.get(key)
    if not st:
        return
    now = time.time()
    st.last_tick = now
    st.last_ok = now
    if extra:
        st.extra = extra


def mark_error(key: str, err: str):
    st = MODULES.get(key)
    if not st:
        return
    st.last_tick = time.time()
    st.last_error = err[:200]


def mark_warn(key: str, warn: str):
    st = MODULES.get(key)
    if not st:
        return
    st.last_tick = time.time()
    st.last_warn = warn[:200]


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


def update_binance_stage(module: str, stage: str) -> None:
    st = MODULES.get(module)
    if not st:
        return
    st.binance_current_stage = stage


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
        await asyncio.sleep(max(0, SCAN_INTERVAL - elapsed))


async def watchdog() -> None:
    while True:
        now = time.time()
        for name, module in MODULES.items():
            last = module.last_tick
            if last and now - last > 120:
                print(f"[WATCHDOG] {name} stalled: {int(now - last)}s")
        await asyncio.sleep(30)
