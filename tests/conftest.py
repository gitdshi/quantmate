"""
Pytest configuration and shared fixtures for QuantMate backend tests.
"""
import asyncio
import os
import sys
from pathlib import Path
from typing import AsyncGenerator, Generator

import pytest
from loguru import logger
from unittest.mock import patch

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))


# ─────────────────────────────────────────────────────────────────────────────
# Event Loop Fixtures
# ─────────────────────────────────────────────────────────────────────────────

@pytest.fixture(scope="session")
def event_loop() -> Generator[asyncio.AbstractEventLoop, None, None]:
    """Create an instance of the default event loop for the test session."""
    loop = asyncio.get_event_loop_policy().new_event_loop()
    asyncio.set_event_loop(loop)
    yield loop
    loop.close()
    asyncio.set_event_loop(None)


@pytest.fixture(scope="session", autouse=True)
def install_session_event_loop(event_loop: asyncio.AbstractEventLoop) -> Generator[None, None, None]:
    """Keep a current event loop installed for sync tests that call get_event_loop()."""
    asyncio.set_event_loop(event_loop)
    yield
    asyncio.set_event_loop(None)


# ─────────────────────────────────────────────────────────────────────────────
# Environment Fixtures
# ─────────────────────────────────────────────────────────────────────────────

@pytest.fixture(scope="session", autouse=True)
def setup_test_env() -> None:
    """Configure environment for testing."""
    os.environ["ENV"] = "test"
    os.environ["LOG_LEVEL"] = "DEBUG"
    os.environ["DATABASE_URL"] = "sqlite+aiosqlite:///:memory:"
    os.environ["REDIS_URL"] = "redis://localhost:6379/1"
    # Disable external API calls by default
    os.environ["TUSHARE_TOKEN"] = "test_token_placeholder"
    # Required settings for Settings class validation
    os.environ.setdefault("SECRET_KEY", "test-secret-key-for-unit-tests-only-0123456789abcdef")
    os.environ.setdefault("MYSQL_PASSWORD", "test-password")
    # Clear cached settings so tests get fresh instances
    from app.infrastructure.config.config import get_settings
    get_settings.cache_clear()
    logger.info("Test environment configured")


# ─────────────────────────────────────────────────────────────────────────────
# Database Fixtures
# ─────────────────────────────────────────────────────────────────────────────

@pytest.fixture(scope="function")
async def db_connection():
    """Create an in-memory SQLite database connection for testing (async)."""
    from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
    from sqlalchemy.orm import sessionmaker
    from app.models import Base

    engine = create_async_engine(
        "sqlite+aiosqlite:///:memory:",
        echo=False,
        future=True
    )

    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

    async_session = sessionmaker(
        engine, class_=AsyncSession, expire_on_commit=False
    )

    async with async_session() as session:
        yield session
        await session.rollback()

    await engine.dispose()


@pytest.fixture(scope="function")
def db_connection_sync():
    """Create a synchronous in-memory SQLite connection for DAO tests."""
    import sys
    from pathlib import Path
    from sqlalchemy import create_engine, text
    from datetime import datetime

    # Ensure project root on path
    project_root = Path(__file__).parent.parent
    if str(project_root) not in sys.path:
        sys.path.insert(0, str(project_root))

    # Create in-memory SQLite database and initialize schema
    engine = create_engine("sqlite:///:memory:")
    with engine.begin() as conn:
        # Create users table (simplified schema matching production)
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS users (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                username VARCHAR(50) UNIQUE NOT NULL,
                email VARCHAR(100) UNIQUE,
                hashed_password VARCHAR(255) NOT NULL,
                is_active BOOLEAN DEFAULT 1,
                must_change_password BOOLEAN DEFAULT 0,
                created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                updated_at DATETIME
            )
        """))
        # Add other necessary tables if tests require them
    conn = engine.connect()
    yield conn
    conn.close()
    engine.dispose()


# ─────────────────────────────────────────────────────────────────────────────
# Redis Fixture (Mock)
# ─────────────────────────────────────────────────────────────────────────────

class MockRedis:
    """Simple mock Redis for testing."""

    def __init__(self):
        self._data = {}
        self._expires = {}

    async def get(self, key):
        if key in self._expires and self._expires[key] < asyncio.get_event_loop().time():
            del self._data[key]
            del self._expires[key]
        return self._data.get(key)

    async def set(self, key, value, ex=None):
        self._data[key] = value
        if ex:
            self._expires[key] = asyncio.get_event_loop().time() + ex

    async def delete(self, key):
        self._data.pop(key, None)
        self._expires.pop(key, None)

    async def flushdb(self):
        self._data.clear()
        self._expires.clear()


@pytest.fixture(scope="function")
async def redis_client():
    """Provide a mock Redis client."""
    return MockRedis()


# ─────────────────────────────────────────────────────────────────────────────
# Tushare API Mock Fixture
# ─────────────────────────────────────────────────────────────────────────────

class MockTushareAPI:
    """Mock Tushare API for testing."""

    def __init__(self):
        self.call_count = 0
        self.rate_limit_count = 0

    async def call(self, api_name, params=None):
        """Mock API call with rate limiting simulation."""
        self.call_count += 1

        # Simulate rate limit error for specific test scenarios
        if api_name == "rate_limit_hit":
            self.rate_limit_count += 1
            raise Exception("Frequency limit, please wait")

        # Return sample data structure
        return {
            "api": api_name,
            "params": params or {},
            "data": [
                {"ts_code": "000001.SZ", "name": "平安银行"},
                {"ts_code": "000002.SZ", "name": "万科A"},
            ],
            "count": 2
        }

    def reset(self):
        self.call_count = 0
        self.rate_limit_count = 0


@pytest.fixture(scope="function")
def tushare_mock():
    """Provide a mock Tushare API client."""
    return MockTushareAPI()


# ─────────────────────────────────────────────────────────────────────────────
# Logging Fixture
# ─────────────────────────────────────────────────────────────────────────────

@pytest.fixture(scope="function", autouse=True)
def caplog_handler(caplog):
    """Configure loguru to capture logs for pytest."""
    # Remove default handler and add a test handler
    logger.remove()
    handler_id = logger.add(lambda msg: caplog.info(msg), level="DEBUG")
    yield
    try:
        logger.remove(handler_id)
    except ValueError:
        pass


@pytest.fixture(scope="function", autouse=True)
def allow_rbac_by_default():
    """Keep legacy route tests green unless they explicitly exercise RBAC denial."""
    with patch("app.api.dependencies.permissions.RbacService.check_permission", return_value=True):
        yield
