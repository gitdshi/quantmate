"""Unit tests for StrategiesService."""

from __future__ import annotations

import json
from datetime import datetime
from unittest.mock import MagicMock, patch

import pytest

from app.domains.strategies.service import StrategiesService


class _ValidationResult:
    def __init__(self, valid, errors=None, warnings=None):
        self.valid = valid
        self.errors = errors or []
        self.warnings = warnings or []


@pytest.fixture
def svc():
    with patch("app.domains.strategies.service.StrategyDao") as DaoCls, \
         patch("app.domains.strategies.service.StrategyHistoryDao") as HistCls:
        s = StrategiesService()
        s._dao = DaoCls.return_value
        s._history = HistCls.return_value
        yield s


# ── list / count ──────────────────────────────────────────────────

def test_list_strategies(svc):
    svc._dao.list_for_user.return_value = [{"id": 1}]
    assert svc.list_strategies(10) == [{"id": 1}]
    svc._dao.list_for_user.assert_called_once_with(10)


def test_count_strategies(svc):
    svc._dao.count_for_user.return_value = 5
    assert svc.count_strategies(10) == 5


def test_list_strategies_paginated(svc):
    svc._dao.list_for_user_paginated.return_value = [{"id": 2}]
    assert svc.list_strategies_paginated(1, 10, 0) == [{"id": 2}]
    svc._dao.list_for_user_paginated.assert_called_once_with(1, 10, 0)


# ── create ────────────────────────────────────────────────────────

@patch("app.domains.strategies.service.validate_strategy_code")
def test_create_strategy_success(mock_validate, svc):
    mock_validate.return_value = _ValidationResult(True)
    svc._dao.name_exists_for_user.return_value = False
    svc._dao.insert_strategy.return_value = 99
    svc._dao.get_for_user.return_value = {
        "id": 99, "name": "s1", "class_name": "S1",
        "description": None, "parameters": "{}", "code": "x",
        "version": 1, "is_active": True,
    }
    result = svc.create_strategy(1, "s1", "S1", None, {}, "code")
    assert result["id"] == 99
    svc._dao.insert_strategy.assert_called_once()


@patch("app.domains.strategies.service.validate_strategy_code")
def test_create_strategy_invalid_code(mock_validate, svc):
    mock_validate.return_value = _ValidationResult(False, errors=["bad"])
    with pytest.raises(ValueError, match="Invalid strategy code"):
        svc.create_strategy(1, "s1", "S1", None, {}, "bad_code")


def test_create_strategy_duplicate_name(svc):
    svc._dao.name_exists_for_user.return_value = True
    with pytest.raises(ValueError, match="already exists"):
        svc.create_strategy(1, "dup", "D", None, {}, "")


# ── get ───────────────────────────────────────────────────────────

def test_get_strategy_found(svc):
    svc._dao.get_for_user.return_value = {
        "id": 1, "parameters": '{"a": 1}'
    }
    result = svc.get_strategy(1, 1)
    assert result["parameters"] == {"a": 1}


def test_get_strategy_not_found(svc):
    svc._dao.get_for_user.return_value = None
    with pytest.raises(KeyError, match="not found"):
        svc.get_strategy(1, 999)


def test_get_strategy_bad_json_params(svc):
    svc._dao.get_for_user.return_value = {"id": 1, "parameters": "invalid"}
    result = svc.get_strategy(1, 1)
    assert result["parameters"] == {}


# ── update ────────────────────────────────────────────────────────

@patch("app.domains.strategies.service.validate_strategy_code")
def test_update_strategy_name_change_bumps_version(mock_validate, svc):
    svc._dao.get_existing_for_update.return_value = {
        "id": 1, "name": "old", "class_name": "C", "description": "",
        "code": "x", "version": 1, "parameters": "{}",
    }
    svc._dao.get_for_user.return_value = {
        "id": 1, "name": "new", "parameters": "{}",
    }
    svc.update_strategy(1, 1, name="new")
    svc._history.insert_history.assert_called_once()
    svc._history.rotate_keep_latest.assert_called_once_with(1, keep=5)
    svc._dao.update_strategy.assert_called_once()


@patch("app.domains.strategies.service.validate_strategy_code")
def test_update_strategy_code_validates(mock_validate, svc):
    mock_validate.return_value = _ValidationResult(False, errors=["err"])
    svc._dao.get_existing_for_update.return_value = {
        "id": 1, "name": "s", "class_name": "C", "description": "",
        "code": "old", "version": 1, "parameters": "{}",
    }
    with pytest.raises(ValueError, match="Invalid strategy code"):
        svc.update_strategy(1, 1, code="bad")


def test_update_strategy_not_found(svc):
    svc._dao.get_existing_for_update.return_value = None
    with pytest.raises(KeyError):
        svc.update_strategy(1, 999, name="x")


def test_update_strategy_empty_name(svc):
    svc._dao.get_existing_for_update.return_value = {
        "id": 1, "name": "s", "class_name": "C", "description": "",
        "code": "x", "version": 1, "parameters": "{}",
    }
    with pytest.raises(ValueError, match="cannot be empty"):
        svc.update_strategy(1, 1, name="  ")


def test_update_strategy_no_version_bump_for_is_active(svc):
    svc._dao.get_existing_for_update.return_value = {
        "id": 1, "name": "s", "class_name": "C", "description": "",
        "code": "x", "version": 1, "parameters": "{}",
    }
    svc._dao.get_for_user.return_value = {"id": 1, "parameters": "{}"}
    svc.update_strategy(1, 1, is_active=False)
    svc._history.insert_history.assert_not_called()


def test_update_strategy_params_change_bumps_version(svc):
    svc._dao.get_existing_for_update.return_value = {
        "id": 1, "name": "s", "class_name": "C", "description": "",
        "code": "x", "version": 1, "parameters": '{"a": 1}',
    }
    svc._dao.get_for_user.return_value = {"id": 1, "parameters": "{}"}
    svc.update_strategy(1, 1, parameters={"a": 2})
    svc._history.insert_history.assert_called_once()


# ── delete ────────────────────────────────────────────────────────

def test_delete_strategy_ok(svc):
    svc._dao.delete_for_user.return_value = True
    svc.delete_strategy(1, 1)
    svc._dao.delete_for_user.assert_called_once_with(1, 1)


def test_delete_strategy_not_found(svc):
    svc._dao.delete_for_user.return_value = False
    with pytest.raises(KeyError):
        svc.delete_strategy(1, 999)


# ── code history ──────────────────────────────────────────────────

def test_list_code_history(svc):
    svc._dao.get_for_user.return_value = {"id": 1, "parameters": "{}"}
    svc._history.list_history.return_value = [
        {"id": 10, "created_at": datetime(2025, 1, 1), "size": 100,
         "strategy_name": "s", "class_name": "C", "description": "",
         "version": 1, "parameters": "{}"},
    ]
    result = svc.list_code_history(1, 1)
    assert len(result) == 1
    assert result[0]["id"] == 10


def test_get_code_history(svc):
    svc._dao.get_for_user.return_value = {"id": 1, "parameters": "{}"}
    svc._history.get_history.return_value = {
        "id": 5, "code": "x", "strategy_name": "s", "class_name": "C",
        "description": "", "version": 1, "parameters": "{}",
    }
    result = svc.get_code_history(1, 1, 5)
    assert result["code"] == "x"


def test_get_code_history_not_found(svc):
    svc._dao.get_for_user.return_value = {"id": 1, "parameters": "{}"}
    svc._history.get_history.return_value = None
    with pytest.raises(KeyError, match="History not found"):
        svc.get_code_history(1, 1, 999)


# ── restore ───────────────────────────────────────────────────────

def test_restore_code_history(svc):
    svc._dao.get_existing_for_update.return_value = {
        "id": 1, "name": "s", "class_name": "C", "description": "",
        "code": "old", "version": 2, "parameters": "{}",
    }
    svc._history.get_history.return_value = {
        "strategy_name": "s_old", "class_name": "C", "description": "d",
        "version": 1, "parameters": '{"x": 1}', "code": "restored",
    }
    svc.restore_code_history(1, 1, 5)
    svc._history.insert_history.assert_called_once()
    svc._dao.update_strategy.assert_called_once()


def test_restore_code_history_strategy_not_found(svc):
    svc._dao.get_existing_for_update.return_value = None
    with pytest.raises(KeyError, match="Strategy not found"):
        svc.restore_code_history(1, 1, 5)


def test_restore_code_history_history_not_found(svc):
    svc._dao.get_existing_for_update.return_value = {"id": 1, "code": "x"}
    svc._history.get_history.return_value = None
    with pytest.raises(KeyError, match="History not found"):
        svc.restore_code_history(1, 1, 999)


def test_restore_code_history_no_current_code_skips_history_save(svc):
    svc._dao.get_existing_for_update.return_value = {
        "id": 1, "name": "s", "class_name": "C", "description": "",
        "code": "", "version": 1, "parameters": "{}",
    }
    svc._history.get_history.return_value = {
        "strategy_name": "s", "class_name": "C", "description": "",
        "version": 1, "parameters": None, "code": "new",
    }
    svc.restore_code_history(1, 1, 5)
    svc._history.insert_history.assert_not_called()
    svc._dao.update_strategy.assert_called_once()
