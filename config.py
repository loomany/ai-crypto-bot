import os
from dataclasses import dataclass


@dataclass(frozen=True)
class Config:
    final_score_threshold: float


cfg = Config(final_score_threshold=float(os.getenv("FINAL_SCORE_THRESHOLD", "65")))
