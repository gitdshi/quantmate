"""Tests for database connection helpers — focusing on new qlib DB support."""
import pytest
from unittest.mock import patch, MagicMock

from sqlalchemy.exc import OperationalError

# We test the module-level functions and context manager
import app.infrastructure.db.connections as connections


class TestDatabaseNameLiteral:
    """Verify DatabaseName type includes qlib."""

    def test_database_name_includes_qlib(self):
        # DatabaseName is a Literal type — verify the module's connection() handles 'qlib'
        assert "qlib" in connections.DatabaseName.__args__

    def test_database_name_includes_all(self):
        expected = {"quantmate", "tushare", "akshare", "qlib"}
        assert set(connections.DatabaseName.__args__) == expected


class TestGetQlibEngine:

    @patch("app.infrastructure.db.connections.create_engine")
    @patch("app.infrastructure.db.connections.settings")
    def test_creates_engine_on_first_call(self, mock_settings, mock_create):
        # Reset singleton
        connections._qlib_engine = None
        mock_settings.mysql_url = "mysql+pymysql://user:pass@localhost:3306"
        mock_engine = MagicMock()
        mock_create.return_value = mock_engine

        engine = connections.get_qlib_engine()

        mock_create.assert_called_once_with(
            "mysql+pymysql://user:pass@localhost:3306/qlib?charset=utf8mb4",
            pool_pre_ping=True,
        )
        assert engine is mock_engine

    @patch("app.infrastructure.db.connections.create_engine")
    @patch("app.infrastructure.db.connections.settings")
    def test_reuses_engine_singleton(self, mock_settings, mock_create):
        mock_engine = MagicMock()
        connections._qlib_engine = mock_engine

        engine = connections.get_qlib_engine()

        mock_create.assert_not_called()
        assert engine is mock_engine

        # Reset
        connections._qlib_engine = None


class TestQlibConnection:

    @patch("app.infrastructure.db.connections.get_qlib_engine")
    def test_get_qlib_connection(self, mock_engine_fn):
        mock_engine = MagicMock()
        mock_conn = MagicMock()
        mock_engine.connect.return_value = mock_conn
        mock_engine_fn.return_value = mock_engine

        conn = connections.get_qlib_connection()
        assert conn is mock_conn
        mock_engine.connect.assert_called_once()

    @patch("app.infrastructure.db.connections.get_mysql_server_engine")
    @patch("app.infrastructure.db.connections.get_qlib_engine")
    def test_get_qlib_connection_bootstraps_missing_database(self, mock_engine_fn, mock_admin_engine_fn):
        mock_engine = MagicMock()
        mock_conn = MagicMock()
        mock_engine.connect.side_effect = [
            OperationalError("stmt", {}, Exception("Unknown database 'qlib'")),
            mock_conn,
        ]
        mock_engine_fn.return_value = mock_engine

        mock_admin_conn = MagicMock()
        mock_admin_engine = MagicMock()
        mock_admin_engine.begin.return_value.__enter__.return_value = mock_admin_conn
        mock_admin_engine_fn.return_value = mock_admin_engine

        conn = connections.get_qlib_connection()

        assert conn is mock_conn
        assert mock_engine.connect.call_count == 2
        mock_admin_conn.execute.assert_called_once()
        assert "CREATE DATABASE IF NOT EXISTS `qlib`" in str(mock_admin_conn.execute.call_args[0][0])

    @patch("app.infrastructure.db.connections.get_mysql_server_engine")
    @patch("app.infrastructure.db.connections.get_qlib_engine")
    def test_get_qlib_connection_reraises_other_operational_errors(self, mock_engine_fn, mock_admin_engine_fn):
        mock_engine = MagicMock()
        mock_engine.connect.side_effect = OperationalError("stmt", {}, Exception("Connection refused"))
        mock_engine_fn.return_value = mock_engine

        with pytest.raises(OperationalError):
            connections.get_qlib_connection()

        mock_admin_engine_fn.assert_not_called()


class TestConnectionContextManager:

    @patch("app.infrastructure.db.connections.get_qlib_connection")
    def test_connection_qlib(self, mock_get_conn):
        mock_conn = MagicMock()
        mock_get_conn.return_value = mock_conn

        with connections.connection("qlib") as conn:
            assert conn is mock_conn

        mock_conn.close.assert_called_once()

    @patch("app.infrastructure.db.connections.get_quantmate_connection")
    def test_connection_quantmate(self, mock_get_conn):
        mock_conn = MagicMock()
        mock_get_conn.return_value = mock_conn

        with connections.connection("quantmate") as conn:
            assert conn is mock_conn

        mock_conn.close.assert_called_once()

    def test_connection_invalid_db_raises(self):
        with pytest.raises(ValueError):
            with connections.connection("nonexistent"):
                pass
