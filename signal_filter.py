import sqlite3
from typing import Literal

DB_PATH = "ai_signals.db"

FilterLevel = Literal["aggressive", "normal", "strict"]

DEFAULT_LEVEL: FilterLevel = "normal"


def _get_conn():
    return sqlite3.connect(DB_PATH)


def init_filter_table():
    conn = _get_conn()
    try:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS user_signal_filter (
                chat_id INTEGER PRIMARY KEY,
                level   TEXT NOT NULL
            )
            """
        )
        conn.commit()
    finally:
        conn.close()


def set_user_filter(chat_id: int, level: FilterLevel) -> None:
    if level not in ("aggressive", "normal", "strict"):
        level = DEFAULT_LEVEL
    conn = _get_conn()
    try:
        conn.execute(
            "INSERT INTO user_signal_filter (chat_id, level) VALUES (?, ?) "
            "ON CONFLICT(chat_id) DO UPDATE SET level=excluded.level",
            (chat_id, level),
        )
        conn.commit()
    finally:
        conn.close()


def get_user_filter(chat_id: int) -> FilterLevel:
    conn = _get_conn()
    try:
        cur = conn.cursor()
        cur.execute("SELECT level FROM user_signal_filter WHERE chat_id = ?", (chat_id,))
        row = cur.fetchone()
        if not row:
            return DEFAULT_LEVEL
        level = row[0]
        if level not in ("aggressive", "normal", "strict"):
            return DEFAULT_LEVEL
        return level  # type: ignore[return-value]
    finally:
        conn.close()


# ---- Пороговые значения для модулей ----


def ai_min_score(level: FilterLevel) -> int:
    # оценка сигнала из signals.py (field: score)
    if level == "aggressive":
        return 80
    if level == "strict":
        return 93
    return 90  # normal


def btc_min_probability(level: FilterLevel) -> int:
    # probability из BTCSingal
    if level == "aggressive":
        return 70
    if level == "strict":
        return 90
    return 80  # normal


def whales_min_probability(level: FilterLevel) -> int:
    # probability в WhaleSignal
    if level == "aggressive":
        return 70
    if level == "strict":
        return 90
    return 80


def pumps_min_strength(level: FilterLevel) -> float:
    # Силу пампа Codex выберет сам (например, pct роста за 5–15м)
    if level == "aggressive":
        return 8.0   # % или условная величина
    if level == "strict":
        return 15.0
    return 10.0
