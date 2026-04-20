"""Application settings moved to infrastructure package.

This module is a copy of `app.api.config` moved to
`app.infrastructure.config.config` to centralize runtime configuration.
"""

import json
from functools import lru_cache
from typing import Annotated

from pydantic import AliasChoices, Field, field_validator
from pydantic_settings import BaseSettings, NoDecode, SettingsConfigDict


class Settings(BaseSettings):
    """Application settings loaded from environment variables."""

    app_name: str = "QuantMate API"
    app_version: str = "1.0.0-snapshot"
    debug: bool = False
    environment: str = Field(default="development", validation_alias=AliasChoices("QUANTMATE_ENV", "APP_ENV"))

    secret_key: str  # JWT 密钥，必须从环境变量 SECRET_KEY 读取
    algorithm: str = "HS256"
    access_token_expire_minutes: int = 60 * 4
    refresh_token_expire_days: int = 7

    mysql_host: str = "127.0.0.1"
    mysql_port: int = 3306
    mysql_user: str = "root"
    mysql_password: str  # 必须从环境变量 MYSQL_PASSWORD 读取
    tushare_db: str = Field(default="tushare", validation_alias=AliasChoices("TUSHARE_DATABASE", "TUSHARE_DB"))
    quantmate_db: str = "quantmate"

    redis_host: str = "127.0.0.1"
    redis_port: int = 6379
    redis_db: int = 0

    cors_origins: list[str] = ["http://localhost:5173", "http://localhost:3000", "http://127.0.0.1:5173"]

    rdagent_sidecar_url: str = "http://rdagent-service:8001"
    api_host: str = "0.0.0.0"
    api_port: int = 8000
    admin_username: str = "admin"
    admin_email: str = "admin@quantmate.local"

    log_level: str = "INFO"
    log_format: str = Field(default="text", validation_alias=AliasChoices("QUANTMATE_LOG_FORMAT", "LOG_FORMAT"))

    sync_hour: int = 2
    sync_minute: int = 0
    datasync_timezone: str = "Asia/Shanghai"
    sync_parallel_workers: int = 4
    backfill_workers: int = 10
    backfill_idle_interval_hours: int = 4
    backfill_lock_retry_seconds: int = 60
    backfill_lock_stale_hours: int = 6
    batch_size: int = 100
    max_retries: int = 3

    backtest_default_capital: float = 100000.0
    backtest_default_rate: float = 0.0001
    backtest_default_slippage: float = 0.0
    backtest_default_size: int = 1
    backtest_default_pricetick: float = 0.01
    backtest_default_benchmark: str = "399300.SZ"

    backtest_job_timeout_seconds: int = 3600
    backtest_bulk_job_timeout_seconds: int = 7200
    backtest_optimization_job_timeout_seconds: int = 14400
    backtest_result_ttl_seconds: int = 86400 * 7

    queue_high_default_timeout_seconds: int = 600
    queue_default_default_timeout_seconds: int = 1800
    queue_low_default_timeout_seconds: int = 3600
    queue_backtest_default_timeout_seconds: int = 3600
    queue_optimization_default_timeout_seconds: int = 7200
    queue_rdagent_default_timeout_seconds: int = 14400

    worker_default_queue_names: Annotated[list[str], NoDecode] = ["backtest", "optimization", "default", "low"]

    rdagent_request_timeout_seconds: float = 14400.0

    model_config = SettingsConfigDict(
        env_file=".env",
        extra="ignore",
        env_parse_json=True,  # Enable JSON parsing for list fields
    )

    @field_validator("debug", mode="before")
    @classmethod
    def normalize_debug(cls, value):
        if isinstance(value, str):
            lowered = value.strip().lower()
            if lowered in {"release", "prod", "production"}:
                return False
            if lowered in {"debug", "dev", "development"}:
                return True
        return value

    @field_validator("worker_default_queue_names", mode="before")
    @classmethod
    def normalize_worker_default_queue_names(cls, value):
        if value is None:
            return []
        if isinstance(value, str):
            stripped = value.strip()
            if not stripped:
                return []
            if stripped.startswith("["):
                try:
                    parsed = json.loads(stripped)
                except json.JSONDecodeError:
                    stripped = stripped.strip("[]")
                    return [item.strip() for item in stripped.split(",") if item.strip()]
                if not isinstance(parsed, list):
                    raise ValueError("worker_default_queue_names JSON value must be a list")
                return [str(item).strip() for item in parsed if str(item).strip()]
            return [item.strip() for item in stripped.split(",") if item.strip()]
        if isinstance(value, tuple):
            return [str(item).strip() for item in value if str(item).strip()]
        return value

    @property
    def mysql_url(self) -> str:
        # URL-encode password to handle special characters like '@'
        from urllib.parse import quote_plus

        password = quote_plus(self.mysql_password)
        return f"mysql+pymysql://{self.mysql_user}:{password}@{self.mysql_host}:{self.mysql_port}"

    @property
    def tushare_db_url(self) -> str:
        return f"{self.mysql_url}/{self.tushare_db}?charset=utf8mb4"

    @property
    def quantmate_db_url(self) -> str:
        return f"{self.mysql_url}/{self.quantmate_db}?charset=utf8mb4"

    @property
    def vnpy_db_url(self) -> str:
        return self.quantmate_db_url

    @property
    def redis_url(self) -> str:
        return f"redis://{self.redis_host}:{self.redis_port}/{self.redis_db}"


@lru_cache()
def get_settings() -> Settings:
    return Settings()
