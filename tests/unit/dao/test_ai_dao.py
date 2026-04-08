"""Unit tests for app.domains.ai.dao.ai_dao."""

from __future__ import annotations

from unittest.mock import MagicMock, patch, call

import pytest

import app.domains.ai.dao.ai_dao as _ai_mod


def _fake_conn():
    conn = MagicMock()
    ctx = MagicMock()
    ctx.__enter__ = MagicMock(return_value=conn)
    ctx.__exit__ = MagicMock(return_value=False)
    return ctx, conn


@pytest.fixture(autouse=True)
def _mock_connection(monkeypatch):
    ctx, conn = _fake_conn()
    monkeypatch.setattr(_ai_mod, "connection", lambda db: ctx)
    return conn


def _row(**kw):
    m = MagicMock()
    m._mapping = kw
    return m


# ── AIConversationDao ────────────────────────────────────────────

class TestAIConversationDao:
    def test_list_for_user(self, _mock_connection):
        _mock_connection.execute.return_value = MagicMock(
            fetchall=MagicMock(return_value=[_row(id=1, title="Chat")])
        )
        dao = _ai_mod.AIConversationDao()
        result = dao.list_for_user(user_id=1)
        assert isinstance(result, list)

    def test_list_for_user_with_status(self, _mock_connection):
        _mock_connection.execute.return_value = MagicMock(
            fetchall=MagicMock(return_value=[])
        )
        result = _ai_mod.AIConversationDao().list_for_user(user_id=1, status="active")
        assert result == []

    def test_count_for_user(self, _mock_connection):
        row = MagicMock()
        row._mapping = {"cnt": 5}
        _mock_connection.execute.return_value = MagicMock(
            fetchone=MagicMock(return_value=row)
        )
        result = _ai_mod.AIConversationDao().count_for_user(user_id=1)
        assert result == 5

    def test_get_found(self, _mock_connection):
        _mock_connection.execute.return_value = MagicMock(
            fetchone=MagicMock(return_value=_row(id=1, title="Chat"))
        )
        result = _ai_mod.AIConversationDao().get(conversation_id=1, user_id=1)
        assert result is not None

    def test_get_not_found(self, _mock_connection):
        _mock_connection.execute.return_value = MagicMock(
            fetchone=MagicMock(return_value=None)
        )
        result = _ai_mod.AIConversationDao().get(conversation_id=999, user_id=1)
        assert result is None

    def test_create(self, _mock_connection):
        _mock_connection.execute.return_value = MagicMock(lastrowid=42)
        result = _ai_mod.AIConversationDao().create(
            user_id=1, session_id="sess-1", title="My Chat"
        )
        assert result == 42

    def test_update(self, _mock_connection):
        _ai_mod.AIConversationDao().update(conversation_id=1, user_id=1, title="New")
        _mock_connection.execute.assert_called()

    def test_delete(self, _mock_connection):
        _mock_connection.execute.return_value = MagicMock(rowcount=1)
        result = _ai_mod.AIConversationDao().delete(conversation_id=1, user_id=1)
        assert result is True

    def test_delete_not_found(self, _mock_connection):
        _mock_connection.execute.return_value = MagicMock(rowcount=0)
        result = _ai_mod.AIConversationDao().delete(conversation_id=999, user_id=1)
        assert result is False

    def test_list_messages(self, _mock_connection):
        _mock_connection.execute.return_value = MagicMock(
            fetchall=MagicMock(return_value=[_row(id=1, role="user", content="Hi")])
        )
        result = _ai_mod.AIConversationDao().list_messages(conversation_id=1)
        assert isinstance(result, list)

    def test_add_message(self, _mock_connection):
        _mock_connection.execute.return_value = MagicMock(lastrowid=10)
        result = _ai_mod.AIConversationDao().add_message(
            conversation_id=1, role="user", content="Hello"
        )
        assert result == 10


# ── AIModelConfigDao ─────────────────────────────────────────────

class TestAIModelConfigDao:
    def test_list_all(self, _mock_connection):
        _mock_connection.execute.return_value = MagicMock(
            fetchall=MagicMock(return_value=[_row(id=1, model_name="gpt-4")])
        )
        result = _ai_mod.AIModelConfigDao().list_all()
        assert isinstance(result, list)

    def test_list_all_enabled_only(self, _mock_connection):
        _mock_connection.execute.return_value = MagicMock(
            fetchall=MagicMock(return_value=[])
        )
        result = _ai_mod.AIModelConfigDao().list_all(enabled_only=True)
        assert result == []

    def test_get(self, _mock_connection):
        _mock_connection.execute.return_value = MagicMock(
            fetchone=MagicMock(return_value=_row(id=1, model_name="gpt-4"))
        )
        result = _ai_mod.AIModelConfigDao().get(model_id=1)
        assert result is not None

    def test_get_by_name(self, _mock_connection):
        _mock_connection.execute.return_value = MagicMock(
            fetchone=MagicMock(return_value=_row(id=1, model_name="gpt-4"))
        )
        result = _ai_mod.AIModelConfigDao().get_by_name(model_name="gpt-4")
        assert result is not None

    def test_create(self, _mock_connection):
        _mock_connection.execute.return_value = MagicMock(lastrowid=5)
        result = _ai_mod.AIModelConfigDao().create(
            model_name="gpt-4o", provider="openai"
        )
        assert result == 5

    def test_update(self, _mock_connection):
        _ai_mod.AIModelConfigDao().update(model_id=1, temperature=0.5)
        _mock_connection.execute.assert_called()

    def test_delete(self, _mock_connection):
        _mock_connection.execute.return_value = MagicMock(rowcount=1)
        result = _ai_mod.AIModelConfigDao().delete(model_id=1)
        assert result is True
