from notifications_db import is_notify_enabled as db_is_notify_enabled


def is_notify_enabled(chat_id: int, feature: str) -> bool:
    return db_is_notify_enabled(chat_id, feature)
