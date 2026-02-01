import asyncio
import time
from dataclasses import dataclass
from typing import Optional, Dict, Callable, Awaitable


@dataclass
class ModuleStatus:
    name: str
    last_tick: float = 0.0
    last_ok: float = 0.0
    last_error: Optional[str] = None
    extra: str = ""

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

        if self.extra:
            state += f"\n   ℹ️ {self.extra}"

        return state


MODULES: Dict[str, ModuleStatus] = {
    "ai_signals": ModuleStatus("🎯 AI-сигналы"),
    "pro": ModuleStatus("🧠 PRO (комбайн)"),
    "pumpdump": ModuleStatus("🚀 Pump/Dump Scanner"),
    "btc": ModuleStatus("₿ BTC (intraday)"),
    "whales_flow": ModuleStatus("🐳 Whale Flow Scanner"),
    "pro_ai": ModuleStatus("🎯 PRO AI-сигналы"),
    "market_pulse": ModuleStatus("📡 Market Pulse"),
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


async def safe_worker_loop(
    module_name: str,
    scan_once_coro: Callable[[], Awaitable[None]],
) -> None:
    while True:
        cycle_start = time.time()
        mark_tick(module_name)

        try:
            await scan_once_coro()
        except Exception as exc:
            mark_error(module_name, f"{type(exc).__name__}: {exc}")

        elapsed = time.time() - cycle_start
        await asyncio.sleep(max(0.0, SCAN_INTERVAL - elapsed))


async def watchdog() -> None:
    while True:
        now = time.time()
        for name, module in MODULES.items():
            last = module.last_tick
            if last and now - last > 120:
                print(f"[WATCHDOG] {name} stalled: {int(now - last)}s")
        await asyncio.sleep(30)
