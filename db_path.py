import os
from pathlib import Path


def _default_db_path() -> Path:
    railway_mount = (os.getenv("RAILWAY_VOLUME_MOUNT_PATH") or "").strip()
    if railway_mount:
        mount_path = Path(railway_mount)
        if mount_path.suffix:
            return mount_path
        return mount_path / "bot.db"

    data_path = Path("/data")
    if data_path.exists() and os.access(data_path, os.W_OK):
        return data_path / "bot.db"

    return Path("./data") / "bot.db"


def get_db_path() -> str:
    db_path = os.getenv("DB_PATH")
    if db_path:
        return db_path
    return str(_default_db_path())


def ensure_db_writable() -> str:
    db_path = Path(get_db_path())
    try:
        db_path.parent.mkdir(parents=True, exist_ok=True)
    except Exception as exc:
        print(f"[DB] ERROR unable to create directory {db_path.parent}: {exc}")
        return str(db_path)

    if not os.access(db_path.parent, os.W_OK):
        print(f"[DB] ERROR directory not writable: {db_path.parent}")
        return str(db_path)

    try:
        with open(db_path, "a", encoding="utf-8"):
            pass
    except Exception as exc:
        print(f"[DB] ERROR unable to write database file {db_path}: {exc}")
        return str(db_path)

    return str(db_path)
