from __future__ import annotations

from dataclasses import dataclass


@dataclass
class Candle:
    ts: int | None = None
    open: float = 0.0
    high: float = 0.0
    low: float = 0.0
    close: float = 0.0
    volume: float = 0.0
    quote_volume: float | None = None
    open_time: int | None = None
    close_time: int | None = None

    def __post_init__(self) -> None:
        if self.open_time is None and self.ts is not None:
            self.open_time = int(self.ts)
        if self.ts is None and self.open_time is not None:
            self.ts = int(self.open_time)
        if self.ts is not None:
            self.ts = int(self.ts)
        if self.open_time is not None:
            self.open_time = int(self.open_time)
        if self.close_time is not None:
            self.close_time = int(self.close_time)
        self.open = float(self.open)
        self.high = float(self.high)
        self.low = float(self.low)
        self.close = float(self.close)
        self.volume = float(self.volume)
        if self.quote_volume is not None:
            self.quote_volume = float(self.quote_volume)
