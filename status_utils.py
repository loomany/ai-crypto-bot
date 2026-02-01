from notifications_db import is_notify_enabled as db_is_notify_enabled
from pro_db import pro_is
from trial_db import FREE_TRIAL_LIMIT, trial_get


def get_user_plan(chat_id: int) -> str:
    return "PRO" if pro_is(chat_id) else "FREE"


def get_usage_today(chat_id: int, feature: str) -> tuple[int, int]:
    used, _ = trial_get(chat_id, feature)
    return used, FREE_TRIAL_LIMIT


def is_notify_enabled(chat_id: int, feature: str) -> bool:
    if feature == "pumpdump":
        return pro_is(chat_id)
    return db_is_notify_enabled(chat_id, feature)
