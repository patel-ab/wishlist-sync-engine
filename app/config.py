import os
from dataclasses import dataclass
from dotenv import load_dotenv

@dataclass(frozen=True)
class AppConfig:
    api_base_url: str
    store_id: str
    api_token: str
    default_rows: int
    http_timeout_seconds: int
    api_auth_mode: str
    api_token_param: str

def load_config() -> AppConfig:
    load_dotenv()

    api_base_url = os.getenv("API_BASE_URL", "").rstrip("/")
    store_id = os.getenv("STORE_ID", "").strip()
    api_token = os.getenv("API_TOKEN", "")
    api_auth_mode = os.getenv("API_AUTH_MODE", "header").strip().lower()
    api_token_param = os.getenv("API_TOKEN_PARAM", "access_token").strip()

    if not api_base_url:
        raise RuntimeError("Missing API_BASE_URL in .env")
    if not store_id:
        raise RuntimeError("Missing STORE_ID in .env")

    return AppConfig(
        api_base_url=api_base_url,
        store_id=store_id,
        api_token=api_token,
        default_rows=int(os.getenv("DEFAULT_ROWS", "100")),
        http_timeout_seconds=int(os.getenv("HTTP_TIMEOUT_SECONDS", "30")),
        api_auth_mode=api_auth_mode,
        api_token_param=api_token_param,
    )