import os
from dataclasses import dataclass
from dotenv import load_dotenv


@dataclass(frozen=True)
class DBConfig:
    db_host: str
    db_port: int
    db_name: str
    db_user: str
    db_password: str


def load_db_config() -> DBConfig:
    load_dotenv()

    db_host = os.getenv("DB_HOST", "").strip()
    db_port = os.getenv("DB_PORT", "5432").strip()
    db_name = os.getenv("DB_NAME", "").strip()
    db_user = os.getenv("DB_USER", "").strip()
    db_password = os.getenv("DB_PASSWORD", "")

    if not db_host:
        raise RuntimeError("Missing DB_HOST in .env")
    if not db_name:
        raise RuntimeError("Missing DB_NAME in .env")
    if not db_user:
        raise RuntimeError("Missing DB_USER in .env")
    if not db_password:
        raise RuntimeError("Missing DB_PASSWORD in .env")

    return DBConfig(
        db_host=db_host,
        db_port=int(db_port),
        db_name=db_name,
        db_user=db_user,
        db_password=db_password,
    )