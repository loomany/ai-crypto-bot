import os
from dataclasses import dataclass


def get_env_float(key: str, default: float) -> float:
    raw = os.getenv(key)
    if raw is None or raw == "":
        return float(default)
    try:
        return float(raw)
    except ValueError:
        return float(default)


@dataclass(frozen=True)
class Config:
    final_score_threshold: float


cfg = Config(final_score_threshold=get_env_float("FINAL_SCORE_THRESHOLD", default=65))
