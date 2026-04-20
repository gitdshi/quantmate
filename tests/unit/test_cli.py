"""Unit tests for app.cli — QuantMate CLI tool."""

from __future__ import annotations

import argparse
from unittest.mock import MagicMock, patch


from app.cli import cmd_health, cmd_db_status, cmd_sync_status, cmd_create_user, main

_CONN_PATH = "app.infrastructure.db.connections.connection"


# ── cmd_health ────────────────────────────────────────────────────

def test_health_ok():
    mock_conn = MagicMock()
    mock_ctx = MagicMock()
    mock_ctx.__enter__ = lambda s: mock_conn
    mock_ctx.__exit__ = MagicMock(return_value=False)

    with patch(_CONN_PATH, return_value=mock_ctx), \
         patch("redis.Redis") as redis_cls:
        redis_cls.return_value = MagicMock()
        result = cmd_health(argparse.Namespace())
    assert result == 0


def test_health_db_fail():
    with patch(_CONN_PATH, side_effect=Exception("db down")):
        result = cmd_health(argparse.Namespace())
    assert result == 1


# ── cmd_db_status ─────────────────────────────────────────────────

def test_db_status_ok():
    mock_conn = MagicMock()
    mock_row = MagicMock()
    mock_row.__iter__ = lambda s: iter(["users"])
    mock_conn.execute.side_effect = [
        MagicMock(fetchall=lambda: [mock_row]),
        MagicMock(fetchone=lambda: (5,)),
    ]
    mock_ctx = MagicMock()
    mock_ctx.__enter__ = lambda s: mock_conn
    mock_ctx.__exit__ = MagicMock(return_value=False)

    with patch(_CONN_PATH, return_value=mock_ctx):
        result = cmd_db_status(argparse.Namespace())
    assert result is None or result == 0


# ── cmd_sync_status ───────────────────────────────────────────────

def test_sync_status_ok():
    mock_row = MagicMock()
    mock_row._mapping = {
        "item_key": "tushare.daily",
        "last_sync_date": "2025-01-01",
        "status": "ok",
        "error_count": 0,
    }
    mock_conn = MagicMock()
    mock_conn.execute.return_value.fetchall.return_value = [mock_row]
    mock_ctx = MagicMock()
    mock_ctx.__enter__ = lambda s: mock_conn
    mock_ctx.__exit__ = MagicMock(return_value=False)

    with patch(_CONN_PATH, return_value=mock_ctx):
        result = cmd_sync_status(argparse.Namespace())
    assert result is None or result == 0


def test_sync_status_fail():
    with patch(_CONN_PATH, side_effect=Exception("fail")):
        result = cmd_sync_status(argparse.Namespace())
    assert result == 1


# ── cmd_create_user ───────────────────────────────────────────────

def test_create_user_success():
    mock_dao = MagicMock()
    mock_dao.get_user_for_login.return_value = None
    with patch("app.domains.auth.dao.user_dao.UserDao", return_value=mock_dao), \
         patch("app.api.services.auth_service.get_password_hash", return_value="hashed"):
        args = argparse.Namespace(username="newuser", email="u@test.com", password="pass123")
        result = cmd_create_user(args)
    assert result == 0
    mock_dao.insert_user.assert_called_once()


def test_create_user_already_exists():
    mock_dao = MagicMock()
    mock_dao.get_user_for_login.return_value = {"id": 1}
    with patch("app.domains.auth.dao.user_dao.UserDao", return_value=mock_dao), \
         patch("app.api.services.auth_service.get_password_hash", return_value="hashed"):
        args = argparse.Namespace(username="dup", email="u@test.com", password="pass")
        result = cmd_create_user(args)
    assert result == 1


def test_create_user_auto_password():
    mock_dao = MagicMock()
    mock_dao.get_user_for_login.return_value = None
    with patch("app.domains.auth.dao.user_dao.UserDao", return_value=mock_dao), \
         patch("app.api.services.auth_service.get_password_hash", return_value="hashed"):
        args = argparse.Namespace(username="new2", email="u@test.com", password=None)
        result = cmd_create_user(args)
    assert result == 0
    assert args.password is not None  # auto-generated


# ── main ──────────────────────────────────────────────────────────

def test_main_no_command(monkeypatch):
    monkeypatch.setattr("sys.argv", ["quantmate"])
    result = main()
    assert result == 1


def test_main_health_command(monkeypatch):
    monkeypatch.setattr("sys.argv", ["quantmate", "health"])
    with patch("app.cli.cmd_health", return_value=0) as mock_cmd:
        result = main()
    mock_cmd.assert_called_once()
    assert result == 0
