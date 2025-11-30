import sqlite3
from pathlib import Path

DB_PATH = Path("bot.db")


def _get_conn():
    conn = sqlite3.connect(DB_PATH)
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS pump_subscribers (
            chat_id INTEGER PRIMARY KEY
        )
        """
    )
    return conn


def add_pump_subscriber(chat_id: int) -> None:
    conn = _get_conn()
    with conn:
        conn.execute(
            "INSERT OR IGNORE INTO pump_subscribers (chat_id) VALUES (?)",
            (chat_id,),
        )
    conn.close()


def remove_pump_subscriber(chat_id: int) -> None:
    conn = _get_conn()
    with conn:
        conn.execute(
            "DELETE FROM pump_subscribers WHERE chat_id = ?",
            (chat_id,),
        )
    conn.close()


def get_pump_subscribers() -> list[int]:
    conn = _get_conn()
    cur = conn.cursor()
    cur.execute("SELECT chat_id FROM pump_subscribers")
    rows = cur.fetchall()
    conn.close()
    return [r[0] for r in rows]
