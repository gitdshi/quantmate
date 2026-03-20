"""Connection helpers.

Provides simple SQLAlchemy engine factories and a small ``connection``
context manager. This module intentionally does NOT perform any schema
creation or migrations; DDL lives under ``quantmate/mysql/init/*.sql`` and
should be applied during provisioning.
"""

from __future__ import annotations

from contextlib import contextmanager
from typing import Iterator, Literal

from sqlalchemy import create_engine
from sqlalchemy.engine import Connection

from app.infrastructure.config import get_settings

settings = get_settings()

DatabaseName = Literal["quantmate", "tushare", "akshare", "qlib"]


# Engine singletons (lazy created)
_quantmate_engine = None
_tushare_engine = None
_akshare_engine = None
_qlib_engine = None
_mysql_server_engine = None


def get_quantmate_engine():
    global _quantmate_engine
    if _quantmate_engine is None:
        _quantmate_engine = create_engine(settings.quantmate_db_url, pool_pre_ping=True)
    return _quantmate_engine


def get_vnpy_engine():
    global _vnpy_engine
    try:
        _vnpy_engine
    except NameError:
        _vnpy_engine = None
    if _vnpy_engine is None:
        vnpy_url = f"{settings.mysql_url}/vnpy?charset=utf8mb4"
        _vnpy_engine = create_engine(vnpy_url, pool_pre_ping=True)
    return _vnpy_engine


def get_tushare_engine():
    global _tushare_engine
    if _tushare_engine is None:
        _tushare_engine = create_engine(settings.tushare_db_url, pool_pre_ping=True)
    return _tushare_engine


def get_akshare_engine():
    global _akshare_engine
    if _akshare_engine is None:
        ak_url = f"{settings.mysql_url}/akshare?charset=utf8mb4"
        _akshare_engine = create_engine(ak_url, pool_pre_ping=True)
    return _akshare_engine


def get_qlib_engine():
    global _qlib_engine
    if _qlib_engine is None:
        qlib_url = f"{settings.mysql_url}/qlib?charset=utf8mb4"
        _qlib_engine = create_engine(qlib_url, pool_pre_ping=True)
    return _qlib_engine


def get_mysql_server_engine():
    """Return an engine without a default database (for admin tasks)."""
    global _mysql_server_engine
    if _mysql_server_engine is None:
        _mysql_server_engine = create_engine(settings.mysql_url, pool_pre_ping=True)
    return _mysql_server_engine


def get_quantmate_connection() -> Connection:
    """Return a `quantmate` DB connection (was previously `get_db_connection`)."""
    engine = get_quantmate_engine()
    return engine.connect()


# Back-compat alias: some callers still import `get_db_connection`.
get_db_connection = get_quantmate_connection


def get_tushare_connection() -> Connection:
    engine = get_tushare_engine()
    return engine.connect()


def get_akshare_connection() -> Connection:
    engine = get_akshare_engine()
    return engine.connect()


def get_qlib_connection() -> Connection:
    engine = get_qlib_engine()
    return engine.connect()


@contextmanager
def connection(db: DatabaseName) -> Iterator[Connection]:
    """Yield a SQLAlchemy connection and always close it."""
    conn: Connection | None = None
    try:
        if db == "quantmate":
            conn = get_quantmate_connection()
        elif db == "tushare":
            conn = get_tushare_connection()
        elif db == "akshare":
            conn = get_akshare_connection()
        elif db == "qlib":
            conn = get_qlib_connection()
        else:
            raise ValueError(f"Unknown db: {db}")
        yield conn
    finally:
        try:
            if conn is not None:
                conn.close()
        except Exception:
            pass
