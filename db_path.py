import os
from pathlib import Path


def get_db_path() -> str:
    db_path = os.getenv("DB_PATH")
    if db_path:
        return db_path
    data_dir = Path("./data")
    data_dir.mkdir(parents=True, exist_ok=True)
    return str(data_dir / "bot.db")
