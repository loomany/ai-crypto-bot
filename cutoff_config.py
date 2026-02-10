import os
from datetime import datetime
from functools import lru_cache


@lru_cache(maxsize=1)
def get_stats_cutoff_ts() -> int:
    raw = os.getenv("STATS_CUTOFF_TS", "0").strip()
    if not raw or raw == "0":
        return 0
    if raw.isdigit():
        return int(raw)
    try:
        dt = datetime.fromisoformat(raw)
    except ValueError:
        return 0
    return int(dt.timestamp())


def admin_show_legacy_enabled() -> bool:
    return os.getenv("ADMIN_SHOW_LEGACY", "0").strip().lower() in {"1", "true", "yes", "on"}


def allow_legacy_for_user(*, is_admin_user: bool) -> bool:
    return bool(is_admin_user and admin_show_legacy_enabled())


def get_effective_cutoff_ts(*, include_legacy: bool) -> int:
    if include_legacy:
        return 0
    return get_stats_cutoff_ts()
