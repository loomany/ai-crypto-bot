import sqlite3

from db_path import get_db_path

DB_PATH = get_db_path()
FREE_TRIAL_LIMIT = 3


def init_trial_tables() -> None:
    conn = sqlite3.connect(DB_PATH)
    try:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS free_trial (
                chat_id INTEGER PRIMARY KEY,
                used_count INTEGER DEFAULT 0,
                paywall_sent INTEGER DEFAULT 0
            )
            """
        )
        conn.commit()
    finally:
        conn.close()


def trial_ensure_user(chat_id: int) -> None:
    conn = sqlite3.connect(DB_PATH)
    try:
        conn.execute(
            "INSERT OR IGNORE INTO free_trial(chat_id, used_count, paywall_sent) "
            "VALUES (?, 0, 0)",
            (chat_id,),
        )
        conn.commit()
    finally:
        conn.close()


def trial_can_send(chat_id: int, limit: int) -> bool:
    conn = sqlite3.connect(DB_PATH)
    try:
        cur = conn.cursor()
        cur.execute("SELECT used_count FROM free_trial WHERE chat_id=?", (chat_id,))
        row = cur.fetchone()
        return row is None or int(row[0]) < limit
    finally:
        conn.close()


def trial_inc(chat_id: int) -> None:
    conn = sqlite3.connect(DB_PATH)
    try:
        conn.execute(
            "UPDATE free_trial SET used_count = used_count + 1 WHERE chat_id=?",
            (chat_id,),
        )
        conn.commit()
    finally:
        conn.close()


def trial_paywall_sent(chat_id: int) -> bool:
    conn = sqlite3.connect(DB_PATH)
    try:
        cur = conn.cursor()
        cur.execute("SELECT paywall_sent FROM free_trial WHERE chat_id=?", (chat_id,))
        row = cur.fetchone()
        return bool(row[0]) if row else False
    finally:
        conn.close()


def trial_mark_paywall(chat_id: int) -> None:
    conn = sqlite3.connect(DB_PATH)
    try:
        conn.execute(
            "UPDATE free_trial SET paywall_sent = 1 WHERE chat_id=?",
            (chat_id,),
        )
        conn.commit()
    finally:
        conn.close()


def trial_reset(chat_id: int) -> None:
    conn = sqlite3.connect(DB_PATH)
    try:
        conn.execute(
            "INSERT INTO free_trial(chat_id, used_count, paywall_sent) VALUES (?, 0, 0) "
            "ON CONFLICT(chat_id) DO UPDATE SET used_count=0, paywall_sent=0",
            (chat_id,),
        )
        conn.commit()
    finally:
        conn.close()


def pro_paywall_text() -> str:
    return (
        "🚫 Лимит бесплатных AI-сигналов исчерпан\n\n"
        f"Ты получил {FREE_TRIAL_LIMIT} бесплатных сигнала.\n"
        "Дальнейший доступ — только с PRO-подпиской.\n\n"
        "🔥 PRO даёт:\n"
        "• Неограниченные AI-сигналы\n"
        "• Лучшие сетапы без trial-фильтра\n"
        "• Сигналы без задержек\n"
        "• Приоритетные движения рынка\n\n"
        "💳 Стоимость: 39$ / 30 дней\n\n"
        "👉 Чтобы получить PRO — напиши администратору."
    )
