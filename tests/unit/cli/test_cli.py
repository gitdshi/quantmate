"""Unit tests for app.cli — QuantMate CLI tool."""

from __future__ import annotations

import argparse
from unittest.mock import MagicMock, patch

import pytest

from app.cli import (
    _build_backfill_analysis_items,
    cmd_create_user,
    cmd_db_status,
    cmd_health,
    cmd_import_backfill_analysis,
    cmd_sync_status,
    main,
)

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


def test_build_backfill_analysis_items(tmp_path):
    csv_path = tmp_path / "analysis.csv"
    csv_path.write_text(
        "source,interface,supports_backfill,backfill_mode,input_params,input_param_details,analysis_date_params\n"
        "tushare,stock_daily,True,date,trade_date,trade_date(required),trade_date\n",
        encoding="utf-8",
    )

    items = _build_backfill_analysis_items(csv_path)

    assert items == [
        {
            "source": "tushare",
            "item_key": "stock_daily",
            "supports_backfill": 1,
            "backfill_mode": "date",
            "input_params": "trade_date",
            "input_param_details": "trade_date(required)",
            "analysis_date_params": "trade_date",
            "input_params_meta": {
                "input_params": ["trade_date"],
                "analysis_date_params": ["trade_date"],
                "supports_backfill": True,
                "backfill_mode": "date",
            },
        }
    ]


def test_build_backfill_analysis_items_rejects_duplicate_keys(tmp_path):
    csv_path = tmp_path / "analysis.csv"
    csv_path.write_text(
        "source,interface,supports_backfill,backfill_mode,input_params,input_param_details,analysis_date_params\n"
        "tushare,stock_daily,True,date,trade_date,trade_date(required),trade_date\n"
        "tushare,stock_daily,True,date,trade_date,trade_date(required),trade_date\n",
        encoding="utf-8",
    )

    with pytest.raises(ValueError, match="Duplicate backfill-analysis row"):
        _build_backfill_analysis_items(csv_path)


def test_import_backfill_analysis_dry_run(tmp_path):
    csv_path = tmp_path / "analysis.csv"
    csv_path.write_text(
        "source,interface,supports_backfill,backfill_mode,input_params,input_param_details,analysis_date_params\n"
        "tushare,stock_daily,True,date,trade_date,trade_date(required),trade_date\n",
        encoding="utf-8",
    )

    dao = MagicMock()
    dao.find_missing_backfill_analysis_items.return_value = []

    args = argparse.Namespace(csv_path=str(csv_path), dry_run=True)
    with patch("app.domains.market.dao.data_source_item_dao.DataSourceItemDao", return_value=dao):
        result = cmd_import_backfill_analysis(args)

    assert result == 0


def test_import_backfill_analysis_fails_for_missing_rows(tmp_path):
    csv_path = tmp_path / "analysis.csv"
    csv_path.write_text(
        "source,interface,supports_backfill,backfill_mode,input_params,input_param_details,analysis_date_params\n"
        "tushare,stock_daily,True,date,trade_date,trade_date(required),trade_date\n",
        encoding="utf-8",
    )

    dao = MagicMock()
    dao.find_missing_backfill_analysis_items.return_value = [("tushare", "stock_daily")]

    args = argparse.Namespace(csv_path=str(csv_path), dry_run=False)
    with patch("app.domains.market.dao.data_source_item_dao.DataSourceItemDao", return_value=dao):
        result = cmd_import_backfill_analysis(args)

    assert result == 1


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


def test_main_import_backfill_analysis_command(monkeypatch):
    monkeypatch.setattr("sys.argv", ["quantmate", "import-backfill-analysis", "--dry-run"])
    with patch("app.cli.cmd_import_backfill_analysis", return_value=0) as mock_cmd:
        result = main()
    mock_cmd.assert_called_once()
    assert result == 0
