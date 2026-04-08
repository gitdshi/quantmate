"""Unit tests for app.domains.audit.dao.audit_log_dao."""

from __future__ import annotations

from datetime import date
from unittest.mock import MagicMock

import pytest

import app.domains.audit.dao.audit_log_dao as _mod


def _fake_conn():
    conn = MagicMock()
    ctx = MagicMock()
    ctx.__enter__ = MagicMock(return_value=conn)
    ctx.__exit__ = MagicMock(return_value=False)
    return ctx, conn


@pytest.fixture(autouse=True)
def _mock_connection(monkeypatch):
    ctx, conn = _fake_conn()
    monkeypatch.setattr(_mod, "connection", lambda db: ctx)
    return conn


def _row(**kw):
    m = MagicMock()
    m._mapping = kw
    return m


class TestAuditLogDao:
    def test_insert(self, _mock_connection):
        _mod.AuditLogDao().insert(
            user_id=1, username="admin", operation_type="LOGIN",
            resource_type="auth", ip_address="127.0.0.1"
        )
        _mock_connection.execute.assert_called()
        _mock_connection.commit.assert_called()

    def test_query_no_filters(self, _mock_connection):
        _mock_connection.execute.return_value = MagicMock(
            fetchall=MagicMock(return_value=[_row(id=1, operation_type="LOGIN")])
        )
        result = _mod.AuditLogDao().query()
        assert isinstance(result, list)

    def test_query_with_filters(self, _mock_connection):
        _mock_connection.execute.return_value = MagicMock(
            fetchall=MagicMock(return_value=[])
        )
        result = _mod.AuditLogDao().query(
            user_id=1, operation_type="LOGIN",
            start_date=date(2024, 1, 1), end_date=date(2024, 12, 31)
        )
        assert result == []

    def test_count_no_filters(self, _mock_connection):
        row = MagicMock()
        row._mapping = {"cnt": 42}
        _mock_connection.execute.return_value = MagicMock(
            fetchone=MagicMock(return_value=row)
        )
        result = _mod.AuditLogDao().count()
        assert result == 42

    def test_count_with_filters(self, _mock_connection):
        row = MagicMock()
        row._mapping = {"cnt": 5}
        _mock_connection.execute.return_value = MagicMock(
            fetchone=MagicMock(return_value=row)
        )
        result = _mod.AuditLogDao().count(user_id=1, operation_type="LOGIN")
        assert result == 5
