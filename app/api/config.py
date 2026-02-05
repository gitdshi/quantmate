"""API Configuration and Settings."""
import os
from datetime import timedelta
from functools import lru_cache
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Application settings loaded from environment variables."""
    
    # Use model_config instead of Config class for pydantic v2
    model_config = SettingsConfigDict(
        env_file=".env",
        extra="ignore"  # Allow extra environment variables
    )
    
    # App
    app_name: str = "TraderMate API"
    app_version: str = "1.0.0"
    debug: bool = False
    
    # Auth
    secret_key: str = "tradermate-secret-key-change-in-production"
    algorithm: str = "HS256"
    access_token_expire_minutes: int = 60 * 24  # 24 hours
    refresh_token_expire_days: int = 7
    
    # Database
    mysql_host: str = "127.0.0.1"
    mysql_port: int = 3306
    mysql_user: str = "root"
    mysql_password: str = "password"
    tushare_db: str = "tushare"
    tradermate_db: str = "tradermate"
    
    # Redis
    redis_host: str = "127.0.0.1"
    redis_port: int = 6379
    redis_db: int = 0
    
    # CORS
    cors_origins: list[str] = ["http://localhost:5173", "http://localhost:3000", "http://127.0.0.1:5173"]
    
    # Backtest
    max_concurrent_backtests: int = 4
    backtest_timeout_seconds: int = 600  # 10 minutes
    
    @property
    def mysql_url(self) -> str:
        return f"mysql+pymysql://{self.mysql_user}:{self.mysql_password}@{self.mysql_host}:{self.mysql_port}"
    
    @property
    def tushare_db_url(self) -> str:
        return f"{self.mysql_url}/{self.tushare_db}?charset=utf8mb4"
    
    @property
    def tradermate_db_url(self) -> str:
        return f"{self.mysql_url}/{self.tradermate_db}?charset=utf8mb4"
    
    @property
    def vnpy_db_url(self) -> str:
        """Backward compatibility alias for tradermate_db_url"""
        return self.tradermate_db_url
    
    @property
    def redis_url(self) -> str:
        return f"redis://{self.redis_host}:{self.redis_port}/{self.redis_db}"


@lru_cache()
def get_settings() -> Settings:
    """Get cached settings instance."""
    return Settings()
