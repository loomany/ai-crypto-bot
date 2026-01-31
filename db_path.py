import os
from pathlib import Path

DATA_DIR = os.getenv("DATA_DIR", "/data")
DB_FILE = os.getenv("DB_FILE", "kryption.db")
DB_PATH = Path(DATA_DIR) / DB_FILE
Path(DATA_DIR).mkdir(parents=True, exist_ok=True)


def get_db_path() -> str:
    return str(DB_PATH)
