"""Unit tests for app.domains.composite.service — CompositeStrategyService."""

from __future__ import annotations

import json
from datetime import datetime
from unittest.mock import MagicMock, patch

import pytest

_SVC_MOD = "app.domains.composite.service"


def _make_svc():
    """Create a CompositeStrategyService with mocked DAOs."""
    with patch(f"{_SVC_MOD}.StrategyComponentDao") as CmpDao, \
         patch(f"{_SVC_MOD}.CompositeStrategyDao") as CsDao, \
         patch(f"{_SVC_MOD}.CompositeBacktestDao") as CbDao:
        from app.domains.composite.service import CompositeStrategyService
        svc = CompositeStrategyService()
        return svc, svc._component_dao, svc._composite_dao, svc._backtest_dao


# ── _parse_json ──────────────────────────────────────────────────

class TestParseJson:
    def test_none(self):
        from app.domains.composite.service import _parse_json
        assert _parse_json(None) is None

    def test_dict(self):
        from app.domains.composite.service import _parse_json
        assert _parse_json({"a": 1}) == {"a": 1}

    def test_list(self):
        from app.domains.composite.service import _parse_json
        assert _parse_json([1, 2]) == [1, 2]

    def test_json_string(self):
        from app.domains.composite.service import _parse_json
        assert _parse_json('{"x": 1}') == {"x": 1}

    def test_invalid_string(self):
        from app.domains.composite.service import _parse_json
        assert _parse_json("not json") is None


# ── Components ───────────────────────────────────────────────────

class TestListComponents:
    def test_list(self):
        svc, comp, _, _ = _make_svc()
        comp.list_for_user.return_value = [{"id": 1}]
        assert svc.list_components(1) == [{"id": 1}]

    def test_count(self):
        svc, comp, _, _ = _make_svc()
        comp.count_for_user.return_value = 5
        assert svc.count_components(1) == 5

    def test_paginated(self):
        svc, comp, _, _ = _make_svc()
        comp.list_for_user_paginated.return_value = [{"id": 2}]
        assert svc.list_components_paginated(1, 10, 0) == [{"id": 2}]

    def test_list_with_layer(self):
        svc, comp, _, _ = _make_svc()
        comp.list_for_user.return_value = []
        svc.list_components(1, layer="signal")
        comp.list_for_user.assert_called_once_with(1, layer="signal")


class TestCreateComponent:
    def test_success(self):
        svc, comp, _, _ = _make_svc()
        comp.name_exists_for_user.return_value = False
        comp.insert.return_value = 42
        comp.get_for_user.return_value = {"id": 42, "name": "C1", "config": None, "parameters": None}
        result = svc.create_component(1, "C1", "signal", "momentum", "desc", "code", {"k": "v"}, {"p": 1})
        assert result["id"] == 42
        comp.insert.assert_called_once()

    def test_duplicate_name(self):
        svc, comp, _, _ = _make_svc()
        comp.name_exists_for_user.return_value = True
        with pytest.raises(ValueError, match="already exists"):
            svc.create_component(1, "Dup", "signal", "m", None, None, None, None)


class TestGetComponent:
    def test_found(self):
        svc, comp, _, _ = _make_svc()
        comp.get_for_user.return_value = {"id": 1, "config": '{"a":1}', "parameters": None}
        result = svc.get_component(1, 1)
        assert result["config"] == {"a": 1}
        assert result["parameters"] is None

    def test_not_found(self):
        svc, comp, _, _ = _make_svc()
        comp.get_for_user.return_value = None
        with pytest.raises(KeyError):
            svc.get_component(1, 999)


class TestUpdateComponent:
    def test_name_change_bumps_version(self):
        svc, comp, _, _ = _make_svc()
        comp.get_for_user.side_effect = [
            {"id": 1, "name": "Old", "config": None, "parameters": None},
            {"id": 1, "name": "New", "config": None, "parameters": None},
        ]
        result = svc.update_component(1, 1, name="New")
        call_args = comp.update.call_args
        assert "version = version + 1" in call_args[0][2]

    def test_not_found(self):
        svc, comp, _, _ = _make_svc()
        comp.get_for_user.return_value = None
        with pytest.raises(KeyError):
            svc.update_component(1, 999, name="X")

    def test_no_updates(self):
        svc, comp, _, _ = _make_svc()
        comp.get_for_user.return_value = {"id": 1, "config": None, "parameters": None}
        svc.update_component(1, 1)
        comp.update.assert_not_called()

    def test_is_active_no_version_bump(self):
        svc, comp, _, _ = _make_svc()
        comp.get_for_user.side_effect = [
            {"id": 1, "name": "X", "config": None, "parameters": None},
            {"id": 1, "name": "X", "config": None, "parameters": None},
        ]
        svc.update_component(1, 1, is_active=False)
        call_args = comp.update.call_args
        assert "version = version + 1" not in call_args[0][2]

    def test_code_change_bumps_version(self):
        svc, comp, _, _ = _make_svc()
        comp.get_for_user.side_effect = [
            {"id": 1, "name": "X", "code": "old", "config": None, "parameters": None},
            {"id": 1, "name": "X", "code": "new", "config": None, "parameters": None},
        ]
        svc.update_component(1, 1, code="new")
        call_args = comp.update.call_args
        assert "version = version + 1" in call_args[0][2]

    def test_config_change(self):
        svc, comp, _, _ = _make_svc()
        comp.get_for_user.side_effect = [
            {"id": 1, "config": None, "parameters": None},
            {"id": 1, "config": '{"k": 1}', "parameters": None},
        ]
        svc.update_component(1, 1, config={"k": 1})
        call_args = comp.update.call_args
        assert "version = version + 1" in call_args[0][2]


class TestDeleteComponent:
    def test_success(self):
        svc, comp, _, _ = _make_svc()
        comp.delete_for_user.return_value = True
        svc.delete_component(1, 1)

    def test_not_found(self):
        svc, comp, _, _ = _make_svc()
        comp.delete_for_user.return_value = False
        with pytest.raises(KeyError):
            svc.delete_component(1, 999)


# ── Composites ───────────────────────────────────────────────────

class TestListComposites:
    def test_list(self):
        svc, _, cs, _ = _make_svc()
        cs.list_for_user.return_value = [{"id": 1}]
        assert svc.list_composites(1) == [{"id": 1}]

    def test_count(self):
        svc, _, cs, _ = _make_svc()
        cs.count_for_user.return_value = 3
        assert svc.count_composites(1) == 3

    def test_paginated(self):
        svc, _, cs, _ = _make_svc()
        cs.list_for_user_paginated.return_value = []
        assert svc.list_composites_paginated(1, 10, 0) == []


class TestCreateComposite:
    def test_success_with_bindings(self):
        svc, comp, cs, _ = _make_svc()
        cs.name_exists_for_user.return_value = False
        comp.get_ids_for_user.return_value = [10, 20]
        cs.insert.return_value = 1
        cs.get_for_user.return_value = {"id": 1, "portfolio_config": None, "market_constraints": None}
        cs.get_bindings.return_value = []
        bindings = [
            {"component_id": 10, "layer": "signal"},
            {"component_id": 20, "layer": "risk", "ordinal": 1, "weight": 0.5, "config_override": {"x": 1}},
        ]
        result = svc.create_composite(1, "CS1", "desc", {"eq": 0.5}, None, "auto", bindings)
        cs.replace_bindings.assert_called_once()

    def test_duplicate_name(self):
        svc, _, cs, _ = _make_svc()
        cs.name_exists_for_user.return_value = True
        with pytest.raises(ValueError, match="already exists"):
            svc.create_composite(1, "Dup", None, None, None, "auto", [])

    def test_invalid_component_ids(self):
        svc, comp, cs, _ = _make_svc()
        cs.name_exists_for_user.return_value = False
        comp.get_ids_for_user.return_value = [10]
        with pytest.raises(ValueError, match="Component IDs not found"):
            svc.create_composite(1, "X", None, None, None, "auto", [
                {"component_id": 10, "layer": "signal"},
                {"component_id": 999, "layer": "risk"},
            ])

    def test_no_bindings(self):
        svc, comp, cs, _ = _make_svc()
        cs.name_exists_for_user.return_value = False
        cs.insert.return_value = 1
        cs.get_for_user.return_value = {"id": 1, "portfolio_config": None, "market_constraints": None}
        cs.get_bindings.return_value = []
        svc.create_composite(1, "X", None, None, None, "auto", [])
        cs.replace_bindings.assert_not_called()


class TestGetComposite:
    def test_found(self):
        svc, _, cs, _ = _make_svc()
        cs.get_for_user.return_value = {"id": 1, "portfolio_config": '{"k":1}', "market_constraints": None}
        result = svc.get_composite(1, 1)
        assert result["portfolio_config"] == {"k": 1}

    def test_not_found(self):
        svc, _, cs, _ = _make_svc()
        cs.get_for_user.return_value = None
        with pytest.raises(KeyError):
            svc.get_composite(1, 999)


class TestGetCompositeDetail:
    def test_with_bindings(self):
        svc, _, cs, _ = _make_svc()
        cs.get_for_user.return_value = {"id": 1, "portfolio_config": None, "market_constraints": None}
        cs.get_bindings.return_value = [{"config_override": '{"x": 1}'}]
        result = svc.get_composite_detail(1, 1)
        assert result["bindings"][0]["config_override"] == {"x": 1}


class TestUpdateComposite:
    def test_name_update(self):
        svc, _, cs, _ = _make_svc()
        cs.get_for_user.return_value = {"id": 1, "portfolio_config": None, "market_constraints": None}
        cs.get_bindings.return_value = []
        svc.update_composite(1, 1, name="Updated")
        cs.update.assert_called_once()

    def test_not_found(self):
        svc, _, cs, _ = _make_svc()
        cs.get_for_user.return_value = None
        with pytest.raises(KeyError):
            svc.update_composite(1, 999, name="X")

    def test_no_updates(self):
        svc, _, cs, _ = _make_svc()
        cs.get_for_user.return_value = {"id": 1, "portfolio_config": None, "market_constraints": None}
        cs.get_bindings.return_value = []
        svc.update_composite(1, 1)
        cs.update.assert_not_called()

    def test_all_fields(self):
        svc, _, cs, _ = _make_svc()
        cs.get_for_user.return_value = {"id": 1, "portfolio_config": None, "market_constraints": None}
        cs.get_bindings.return_value = []
        svc.update_composite(
            1, 1,
            name="N", description="D",
            portfolio_config={"eq": 1}, market_constraints={"max": 10},
            execution_mode="semi-auto", is_active=False,
        )
        call_args = cs.update.call_args
        set_clause = call_args[0][2]
        assert "name = :name" in set_clause
        assert "is_active = :is_active" in set_clause


class TestReplaceBindings:
    def test_success(self):
        svc, comp, cs, _ = _make_svc()
        cs.get_for_user.return_value = {"id": 1}
        comp.get_ids_for_user.return_value = [10]
        cs.get_bindings.return_value = [{"config_override": None}]
        result = svc.replace_bindings(1, 1, [{"component_id": 10, "layer": "signal"}])
        cs.replace_bindings.assert_called_once()
        assert len(result) == 1

    def test_not_found(self):
        svc, _, cs, _ = _make_svc()
        cs.get_for_user.return_value = None
        with pytest.raises(KeyError):
            svc.replace_bindings(1, 999, [])

    def test_invalid_component(self):
        svc, comp, cs, _ = _make_svc()
        cs.get_for_user.return_value = {"id": 1}
        comp.get_ids_for_user.return_value = []
        with pytest.raises(ValueError, match="Component IDs not found"):
            svc.replace_bindings(1, 1, [{"component_id": 999, "layer": "signal"}])


class TestDeleteComposite:
    def test_success(self):
        svc, _, cs, _ = _make_svc()
        cs.delete_for_user.return_value = True
        svc.delete_composite(1, 1)

    def test_not_found(self):
        svc, _, cs, _ = _make_svc()
        cs.delete_for_user.return_value = False
        with pytest.raises(KeyError):
            svc.delete_composite(1, 999)


# ── Backtests ────────────────────────────────────────────────────

class TestSubmitBacktest:
    def test_success(self):
        svc, _, cs, cb = _make_svc()
        cs.get_for_user.return_value = {"id": 1}
        cb.insert.return_value = None
        cb.get_by_job_id.return_value = {"job_id": "cbt_abc", "status": "queued"}
        with patch("app.worker.service.config.get_queue") as mock_q:
            mock_queue = MagicMock()
            mock_q.return_value = mock_queue
            result = svc.submit_backtest(1, 1, "2024-01-01", "2024-12-31", 100000, "399300.SZ")
        assert result["status"] == "queued"
        mock_queue.enqueue.assert_called_once()

    def test_not_found(self):
        svc, _, cs, _ = _make_svc()
        cs.get_for_user.return_value = None
        with pytest.raises(KeyError):
            svc.submit_backtest(1, 999, "2024-01-01", "2024-12-31", 100000, "399300.SZ")


class TestListBacktests:
    def test_list_all(self):
        svc, _, _, cb = _make_svc()
        cb.list_for_user.return_value = [{"id": 1}]
        assert svc.list_backtests(1) == [{"id": 1}]

    def test_list_filtered(self):
        svc, _, _, cb = _make_svc()
        cb.list_for_user.return_value = []
        svc.list_backtests(1, composite_strategy_id=5)
        cb.list_for_user.assert_called_with(1, composite_strategy_id=5)


class TestGetBacktest:
    def test_found(self):
        svc, _, _, cb = _make_svc()
        cb.get_by_job_id.return_value = {"user_id": 1, "result": '{"r":1}', "attribution": None}
        result = svc.get_backtest(1, "cbt_abc")
        assert result["result"] == {"r": 1}

    def test_not_found(self):
        svc, _, _, cb = _make_svc()
        cb.get_by_job_id.return_value = None
        with pytest.raises(KeyError):
            svc.get_backtest(1, "bad")

    def test_wrong_user(self):
        svc, _, _, cb = _make_svc()
        cb.get_by_job_id.return_value = {"user_id": 2, "result": None, "attribution": None}
        with pytest.raises(KeyError):
            svc.get_backtest(1, "cbt_abc")


class TestDeleteBacktest:
    def test_success(self):
        svc, _, _, cb = _make_svc()
        cb.get_by_job_id.return_value = {"id": 1, "user_id": 1}
        svc.delete_backtest(1, "cbt_abc")
        cb.delete_for_user.assert_called_once_with(1, 1)

    def test_not_found(self):
        svc, _, _, cb = _make_svc()
        cb.get_by_job_id.return_value = None
        with pytest.raises(KeyError):
            svc.delete_backtest(1, "bad")

    def test_wrong_user(self):
        svc, _, _, cb = _make_svc()
        cb.get_by_job_id.return_value = {"id": 1, "user_id": 2}
        with pytest.raises(KeyError):
            svc.delete_backtest(1, "cbt_abc")
