"""Connection helpers.

Provides simple SQLAlchemy engine factories and a small ``connection``
context manager. This module intentionally does NOT perform any schema
creation or migrations; DDL lives under ``tradermate/mysql/init/*.sql`` and
should be applied during provisioning.
"""

from __future__ import annotations

from contextlib import contextmanager
from typing import Iterator, Literal
import os

from sqlalchemy import create_engine
from sqlalchemy.engine import Connection

from app.infrastructure.config import get_settings

settings = get_settings()

DatabaseName = Literal["tradermate", "tushare", "akshare"]


# Engine singletons (lazy created)
_tradermate_engine = None
_tushare_engine = None
_akshare_engine = None


def get_tradermate_engine():
    global _tradermate_engine
    if _tradermate_engine is None:
        _tradermate_engine = create_engine(settings.tradermate_db_url, pool_pre_ping=True)
    return _tradermate_engine


def get_vnpy_engine():
    return get_tradermate_engine()


def get_tushare_engine():
    global _tushare_engine
    if _tushare_engine is None:
        _tushare_engine = create_engine(settings.tushare_db_url, pool_pre_ping=True)
    return _tushare_engine


def get_akshare_engine():
    global _akshare_engine
    if _akshare_engine is None:
        ak_url = os.getenv("AKSHARE_DATABASE_URL")
        if not ak_url:
            ak_url = f"{settings.mysql_url}/akshare?charset=utf8mb4"
        _akshare_engine = create_engine(ak_url, pool_pre_ping=True)
    return _akshare_engine


def get_tradermate_connection() -> Connection:
    """Return a `tradermate` DB connection (was previously `get_db_connection`)."""
    engine = get_tradermate_engine()
    return engine.connect()


# Back-compat alias: some callers still import `get_db_connection`.
get_db_connection = get_tradermate_connection


def get_tushare_connection() -> Connection:
    engine = get_tushare_engine()
    return engine.connect()


def get_akshare_connection() -> Connection:
    engine = get_akshare_engine()
    return engine.connect()


@contextmanager
def connection(db: DatabaseName) -> Iterator[Connection]:
    """Yield a SQLAlchemy connection and always close it."""
    conn: Connection | None = None
    try:
        if db == "tradermate":
            conn = get_tradermate_connection()
        elif db == "tushare":
            conn = get_tushare_connection()
        elif db == "akshare":
            conn = get_akshare_connection()
        else:
            raise ValueError(f"Unknown db: {db}")
        yield conn
    finally:
        try:
            if conn is not None:
                conn.close()
        except Exception:
            pass


