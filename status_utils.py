from db import get_user_pref


def is_notify_enabled(chat_id: int, feature: str) -> bool:
    key = f"{feature}_enabled"
    return bool(get_user_pref(chat_id, key, default=0))
