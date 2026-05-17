"""Typed application configuration loaded from environment variables.

All configuration lives in a single Pydantic model so every component
imports the same typed object. The .env file is loaded automatically
by pydantic-settings; no os.getenv calls are needed elsewhere in the
codebase.

Secrets are wrapped in SecretStr so they are masked when logged or
printed by accident. get_settings() is cached so the singleton is
reused across the process.
"""

from __future__ import annotations

from functools import lru_cache

from pydantic import SecretStr
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Application settings loaded from environment variables (.env file).

    Attributes
    ----------
    binance_api_key : SecretStr
        Binance API key used for both REST and WebSocket calls.
    binance_secret_key : SecretStr
        Binance API secret matching the API key.
    db_host : str
        MariaDB host (service name in docker-compose, or IP/domain).
    db_port : int
        MariaDB TCP port.
    db_name : str
        MariaDB schema (database) name.
    db_user : str
        MariaDB user with privileges on ``db_name``.
    db_password : SecretStr
        Password for ``db_user``.
    api_host : str
        Host on which the FastAPI app listens.
    api_port : int
        Port on which the FastAPI app listens.
    log_level : str
        Root log level, e.g. ``"INFO"``, ``"DEBUG"``.
    """

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",
    )

    binance_api_key: SecretStr
    binance_secret_key: SecretStr

    db_host: str = "localhost"
    db_port: int = 3306
    db_name: str = "crypto"
    db_user: str = "crypto"
    db_password: SecretStr

    api_host: str = "0.0.0.0"
    api_port: int = 8000
    log_level: str = "INFO"

    rate_limit_per_minute: int = 100


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    """Return the process-wide :class:`Settings` instance (cached).

    Returns
    -------
    Settings
        The singleton settings object. Safe to call from anywhere.
    """
    settings: Settings = Settings()  # type: ignore[call-arg]
    return settings
