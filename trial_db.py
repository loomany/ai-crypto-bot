import sqlite3

from db_path import get_db_path

DB_PATH = get_db_path()
FREE_TRIAL_LIMIT = 3

FEATURES = ("ai_signals", "btc")


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
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS free_trial_v2 (
                chat_id INTEGER NOT NULL,
                feature TEXT NOT NULL,
                used_count INTEGER DEFAULT 0,
                paywall_sent INTEGER DEFAULT 0,
                PRIMARY KEY (chat_id, feature)
            )
            """
        )

        cur = conn.cursor()
        cur.execute("SELECT COUNT(*) FROM free_trial_v2")
        has_rows = cur.fetchone()[0] > 0
        if not has_rows:
            cur.execute("SELECT chat_id, used_count, paywall_sent FROM free_trial")
            rows = cur.fetchall()
            if rows:
                conn.executemany(
                    """
                    INSERT OR REPLACE INTO free_trial_v2
                        (chat_id, feature, used_count, paywall_sent)
                    VALUES (?, 'ai_signals', ?, ?)
                    """,
                    rows,
                )
        conn.commit()
    finally:
        conn.close()


def _trial_insert_default(conn: sqlite3.Connection, chat_id: int, feature: str) -> None:
    conn.execute(
        """
        INSERT OR IGNORE INTO free_trial_v2
            (chat_id, feature, used_count, paywall_sent)
        VALUES (?, ?, 0, 0)
        """,
        (chat_id, feature),
    )


def trial_ensure_user(chat_id: int, feature: str) -> None:
    conn = sqlite3.connect(DB_PATH)
    try:
        _trial_insert_default(conn, chat_id, feature)
        conn.commit()
    finally:
        conn.close()


def trial_get(chat_id: int, feature: str) -> tuple[int, int]:
    conn = sqlite3.connect(DB_PATH)
    try:
        cur = conn.cursor()
        cur.execute(
            "SELECT used_count, paywall_sent FROM free_trial_v2 "
            "WHERE chat_id=? AND feature=?",
            (chat_id, feature),
        )
        row = cur.fetchone()
        if row is None:
            return 0, 0
        return int(row[0]), int(row[1])
    finally:
        conn.close()


def trial_inc(chat_id: int, feature: str) -> None:
    conn = sqlite3.connect(DB_PATH)
    try:
        _trial_insert_default(conn, chat_id, feature)
        conn.execute(
            """
            UPDATE free_trial_v2
            SET used_count = used_count + 1
            WHERE chat_id=? AND feature=?
            """,
            (chat_id, feature),
        )
        conn.commit()
    finally:
        conn.close()


def trial_mark_paywall(chat_id: int, feature: str) -> None:
    conn = sqlite3.connect(DB_PATH)
    try:
        _trial_insert_default(conn, chat_id, feature)
        conn.execute(
            """
            UPDATE free_trial_v2
            SET paywall_sent = 1
            WHERE chat_id=? AND feature=?
            """,
            (chat_id, feature),
        )
        conn.commit()
    finally:
        conn.close()


def trial_reset(chat_id: int, feature: str | None = None) -> None:
    targets = (feature,) if feature else FEATURES
    conn = sqlite3.connect(DB_PATH)
    try:
        for target in targets:
            conn.execute(
                """
                INSERT INTO free_trial_v2(chat_id, feature, used_count, paywall_sent)
                VALUES (?, ?, 0, 0)
                ON CONFLICT(chat_id, feature)
                DO UPDATE SET used_count=0, paywall_sent=0
                """,
                (chat_id, target),
            )
        conn.commit()
    finally:
        conn.close()


def trial_remaining(chat_id: int, feature: str) -> int:
    used, _ = trial_get(chat_id, feature)
    return max(0, FREE_TRIAL_LIMIT - used)
