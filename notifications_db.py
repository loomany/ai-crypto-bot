import sqlite3

from db import get_user_pref, list_user_ids_with_pref, set_user_pref
from db_path import get_db_path


def init_notify_table() -> None:
    conn = sqlite3.connect(get_db_path())
    try:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS notify_settings (
                chat_id INTEGER NOT NULL,
                feature TEXT NOT NULL,
                enabled INTEGER NOT NULL DEFAULT 0,
                updated_at INTEGER NOT NULL,
                PRIMARY KEY (chat_id, feature)
            )
            """
        )
        conn.commit()
    finally:
        conn.close()


def set_notify(chat_id: int, feature: str, enabled: bool) -> None:
    key = f"{feature}_enabled"
    set_user_pref(chat_id, key, 1 if enabled else 0)


def enable_notify(chat_id: int, feature: str) -> bool:
    key = f"{feature}_enabled"
    before = get_user_pref(chat_id, key, default=0)
    if before:
        return False
    set_user_pref(chat_id, key, 1)
    return True


def disable_notify(chat_id: int, feature: str) -> bool:
    key = f"{feature}_enabled"
    before = get_user_pref(chat_id, key, default=0)
    if not before:
        return False
    set_user_pref(chat_id, key, 0)
    return True


def is_notify_enabled(chat_id: int, feature: str) -> bool:
    key = f"{feature}_enabled"
    return bool(get_user_pref(chat_id, key, default=0))


def list_enabled(feature: str) -> list[int]:
    key = f"{feature}_enabled"
    return list_user_ids_with_pref(key, 1)
