"""Tests for RDAgentService — start_mining, get_run, list_runs, cancel, iterations, import."""
import json
import pytest
from unittest.mock import patch, MagicMock, call

import app.domains.factors.rdagent_service as rdagent_service_module

from app.domains.factors.rdagent_service import (
    RDAgentService,
    RDAgentMiningConfig,
    RunStatus,
    _update_run_status,
    save_iteration,
    save_discovered_factor,
    _serialize_json,
)


_REAL_ENSURE_RDAGENT_SCHEMA = rdagent_service_module._ensure_rdagent_schema


@pytest.fixture(autouse=True)
def _stub_schema_bootstrap(monkeypatch):
    monkeypatch.setattr(rdagent_service_module, "_ensure_rdagent_schema", lambda conn: None)


class TestRDAgentMiningConfig:

    def test_default_values(self):
        cfg = RDAgentMiningConfig()
        assert cfg.scenario == "fin_factor"
        assert cfg.max_iterations == 10
        assert cfg.llm_model == "gpt-4o-mini"
        assert cfg.universe == "csi300"
        assert cfg.feature_columns == []
        assert cfg.hypothesis_type == "factor"

    def test_custom_values(self):
        cfg = RDAgentMiningConfig(
            scenario="fin_model",
            max_iterations=20,
            llm_model="gpt-4o",
        )
        assert cfg.scenario == "fin_model"
        assert cfg.max_iterations == 20

    def test_to_dict(self):
        cfg = RDAgentMiningConfig()
        d = cfg.to_dict()
        assert isinstance(d, dict)
        assert "scenario" in d
        assert "max_iterations" in d
        assert d["scenario"] == "fin_factor"

    def test_to_dict_roundtrip(self):
        cfg = RDAgentMiningConfig(scenario="fin_quant", max_iterations=5)
        d = cfg.to_dict()
        cfg2 = RDAgentMiningConfig(**d)
        assert cfg2.scenario == "fin_quant"
        assert cfg2.max_iterations == 5


class TestRunStatus:

    def test_enum_values(self):
        assert RunStatus.QUEUED == "queued"
        assert RunStatus.RUNNING == "running"
        assert RunStatus.PAUSED == "paused"
        assert RunStatus.COMPLETED == "completed"
        assert RunStatus.FAILED == "failed"
        assert RunStatus.CANCELLED == "cancelled"

    def test_is_string(self):
        assert isinstance(RunStatus.QUEUED, str)


class TestRDAgentServiceStartMining:

    @patch("app.domains.factors.rdagent_service.connection")
    def test_start_mining_returns_run_id(self, mock_conn_ctx):
        mock_conn = MagicMock()
        mock_conn_ctx.return_value.__enter__ = MagicMock(return_value=mock_conn)
        mock_conn_ctx.return_value.__exit__ = MagicMock(return_value=False)

        svc = RDAgentService()
        config = RDAgentMiningConfig()
        result = svc.start_mining(user_id=1, config=config)

        assert "run_id" in result
        assert result["status"] == "queued"
        assert len(result["run_id"]) == 36  # UUID format
        mock_conn_ctx.assert_called_once_with("qlib")
        mock_conn.execute.assert_called_once()


class TestRDAgentServiceGetRun:

    @patch("app.domains.factors.rdagent_service.connection")
    def test_get_run_found(self, mock_conn_ctx):
        mock_conn = MagicMock()
        mock_conn_ctx.return_value.__enter__ = MagicMock(return_value=mock_conn)
        mock_conn_ctx.return_value.__exit__ = MagicMock(return_value=False)

        mock_result = MagicMock()
        mock_result.mappings.return_value.first.return_value = {
            "run_id": "abc-123",
            "user_id": 1,
            "scenario": "fin_factor",
            "config": "{}",
            "status": "running",
            "current_iteration": 3,
            "total_iterations": 10,
            "error_message": None,
            "created_at": "2024-01-01T00:00:00",
            "started_at": None,
            "completed_at": None,
        }
        mock_conn.execute.return_value = mock_result

        svc = RDAgentService()
        run = svc.get_run(user_id=1, run_id="abc-123")
        assert run is not None
        assert run["run_id"] == "abc-123"
        assert run["status"] == "running"
        mock_conn_ctx.assert_called_once_with("qlib")

    @patch("app.domains.factors.rdagent_service.connection")
    def test_get_run_not_found(self, mock_conn_ctx):
        mock_conn = MagicMock()
        mock_conn_ctx.return_value.__enter__ = MagicMock(return_value=mock_conn)
        mock_conn_ctx.return_value.__exit__ = MagicMock(return_value=False)

        mock_result = MagicMock()
        mock_result.mappings.return_value.first.return_value = None
        mock_conn.execute.return_value = mock_result

        svc = RDAgentService()
        run = svc.get_run(user_id=1, run_id="nonexistent")
        assert run is None


class TestRDAgentServiceListRuns:

    @patch("app.domains.factors.rdagent_service.connection")
    def test_list_runs(self, mock_conn_ctx):
        mock_conn = MagicMock()
        mock_conn_ctx.return_value.__enter__ = MagicMock(return_value=mock_conn)
        mock_conn_ctx.return_value.__exit__ = MagicMock(return_value=False)

        mock_result = MagicMock()
        mock_result.mappings.return_value.all.return_value = [
            {"run_id": "r1", "scenario": "fin_factor", "status": "completed",
             "current_iteration": 10, "total_iterations": 10,
             "created_at": "2024-01-01", "completed_at": "2024-01-02"},
        ]
        mock_conn.execute.return_value = mock_result

        svc = RDAgentService()
        runs = svc.list_runs(user_id=1, limit=10, offset=0)
        assert len(runs) == 1
        assert runs[0]["run_id"] == "r1"
        mock_conn_ctx.assert_called_once_with("qlib")


class TestRDAgentServiceCancelRun:

    @patch("app.domains.factors.rdagent_service._update_run_status")
    @patch("app.domains.factors.rdagent_service.RDAgentService.get_run")
    def test_cancel_queued_run(self, mock_get, mock_update):
        mock_get.return_value = {"run_id": "r1", "status": "queued"}

        svc = RDAgentService()
        result = svc.cancel_run(user_id=1, run_id="r1")
        assert result["status"] == "cancelled"
        mock_update.assert_called_once_with("r1", "cancelled")

    @patch("app.domains.factors.rdagent_service.RDAgentService.get_run")
    def test_cancel_not_found(self, mock_get):
        mock_get.return_value = None

        svc = RDAgentService()
        with pytest.raises(KeyError):
            svc.cancel_run(user_id=1, run_id="nonexistent")

    @patch("app.domains.factors.rdagent_service.RDAgentService.get_run")
    def test_cancel_completed_run_raises(self, mock_get):
        mock_get.return_value = {"run_id": "r1", "status": "completed"}

        svc = RDAgentService()
        with pytest.raises(ValueError, match="Cannot cancel"):
            svc.cancel_run(user_id=1, run_id="r1")


class TestRDAgentServiceGetIterations:

    @patch("app.domains.factors.rdagent_service.connection")
    @patch("app.domains.factors.rdagent_service.RDAgentService.get_run")
    def test_get_iterations(self, mock_get, mock_conn_ctx):
        mock_get.return_value = {"run_id": "r1", "status": "completed"}

        mock_conn = MagicMock()
        mock_conn_ctx.return_value.__enter__ = MagicMock(return_value=mock_conn)
        mock_conn_ctx.return_value.__exit__ = MagicMock(return_value=False)

        mock_result = MagicMock()
        mock_result.mappings.return_value.all.return_value = [
            {"id": 1, "run_id": "r1", "iteration_number": 1,
             "hypothesis": "test", "experiment_code": None,
             "metrics": None, "feedback": None, "status": "completed",
             "created_at": "2024-01-01"},
        ]
        mock_conn.execute.return_value = mock_result

        svc = RDAgentService()
        iters = svc.get_iterations(user_id=1, run_id="r1")
        assert len(iters) == 1
        assert iters[0]["iteration_number"] == 1
        mock_conn_ctx.assert_called_once_with("qlib")

    @patch("app.domains.factors.rdagent_service.RDAgentService.get_run")
    def test_get_iterations_run_not_found(self, mock_get):
        mock_get.return_value = None

        svc = RDAgentService()
        with pytest.raises(KeyError):
            svc.get_iterations(user_id=1, run_id="nonexistent")


class TestRDAgentServiceDiscoveredFactors:

    @patch("app.domains.factors.rdagent_service.connection")
    @patch("app.domains.factors.rdagent_service.RDAgentService.get_run")
    def test_get_discovered_factors(self, mock_get, mock_conn_ctx):
        mock_get.return_value = {"run_id": "r1", "status": "completed"}

        mock_conn = MagicMock()
        mock_conn_ctx.return_value.__enter__ = MagicMock(return_value=mock_conn)
        mock_conn_ctx.return_value.__exit__ = MagicMock(return_value=False)

        mock_result = MagicMock()
        mock_result.mappings.return_value.all.return_value = [
            {"id": 1, "run_id": "r1", "factor_name": "Alpha1",
             "expression": "Rank(close/open)", "description": "test",
             "ic_mean": 0.05, "icir": 0.3, "sharpe": 1.2,
             "status": "discovered", "created_at": "2024-01-01"},
        ]
        mock_conn.execute.return_value = mock_result

        svc = RDAgentService()
        factors = svc.get_discovered_factors(user_id=1, run_id="r1")
        assert len(factors) == 1
        assert factors[0]["factor_name"] == "Alpha1"
        mock_conn_ctx.assert_called_once_with("qlib")

    @patch("app.domains.factors.rdagent_service.RDAgentService.get_run")
    def test_get_discovered_factors_run_not_found(self, mock_get):
        mock_get.return_value = None

        svc = RDAgentService()
        with pytest.raises(KeyError):
            svc.get_discovered_factors(user_id=1, run_id="nonexistent")


class TestRDAgentServiceImportFactor:

    @patch("app.domains.factors.rdagent_service.connection")
    @patch("app.domains.factors.rdagent_service.FactorService" if False else "app.domains.factors.service.FactorService")
    @patch("app.domains.factors.rdagent_service.RDAgentService.get_run")
    def test_import_factor(self, mock_get, MockFactorService, mock_conn_ctx):
        mock_get.return_value = {"run_id": "r1", "status": "completed"}

        # First call: select discovered factor
        # Second call: update status
        mock_conn = MagicMock()
        mock_conn_ctx.return_value.__enter__ = MagicMock(return_value=mock_conn)
        mock_conn_ctx.return_value.__exit__ = MagicMock(return_value=False)

        # The first connection context returns the discovered factor
        mock_result1 = MagicMock()
        mock_result1.mappings.return_value.first.return_value = {
            "id": 42, "factor_name": "Alpha1",
            "expression": "Rank(close)", "description": "test",
        }
        # The second connection context is for the UPDATE
        mock_result2 = MagicMock()

        mock_conn.execute.side_effect = [mock_result1, mock_result2]

        # Mock FactorService
        mock_svc_instance = MagicMock()
        mock_svc_instance.create_factor.return_value = {"id": 100, "name": "Alpha1"}
        MockFactorService.return_value = mock_svc_instance

        svc = RDAgentService()
        result = svc.import_factor(user_id=1, run_id="r1", factor_id=42)
        assert result["id"] == 100
        assert mock_conn_ctx.call_args_list == [call("qlib"), call("qlib")]

    @patch("app.domains.factors.rdagent_service.RDAgentService.get_run")
    def test_import_factor_run_not_found(self, mock_get):
        mock_get.return_value = None

        svc = RDAgentService()
        with pytest.raises(KeyError):
            svc.import_factor(user_id=1, run_id="nonexistent", factor_id=1)

    @patch("app.domains.factors.rdagent_service.connection")
    @patch("app.domains.factors.rdagent_service.RDAgentService.get_run")
    def test_import_factor_not_found(self, mock_get, mock_conn_ctx):
        mock_get.return_value = {"run_id": "r1", "status": "completed"}

        mock_conn = MagicMock()
        mock_conn_ctx.return_value.__enter__ = MagicMock(return_value=mock_conn)
        mock_conn_ctx.return_value.__exit__ = MagicMock(return_value=False)

        mock_result = MagicMock()
        mock_result.mappings.return_value.first.return_value = None
        mock_conn.execute.return_value = mock_result

        svc = RDAgentService()
        with pytest.raises(KeyError, match="Discovered factor not found"):
            svc.import_factor(user_id=1, run_id="r1", factor_id=999)


class TestDBHelpers:

    def test_ensure_rdagent_schema_bootstraps_once(self, monkeypatch):
        mock_conn = MagicMock()

        monkeypatch.setattr(rdagent_service_module, "_ensure_rdagent_schema", _REAL_ENSURE_RDAGENT_SCHEMA)
        monkeypatch.setattr(rdagent_service_module, "_RDAGENT_SCHEMA_READY", False)

        _REAL_ENSURE_RDAGENT_SCHEMA(mock_conn)

        assert mock_conn.execute.call_count == len(rdagent_service_module._RDAGENT_SCHEMA_STATEMENTS)
        mock_conn.commit.assert_called_once()

        mock_conn.execute.reset_mock()
        mock_conn.commit.reset_mock()

        _REAL_ENSURE_RDAGENT_SCHEMA(mock_conn)

        mock_conn.execute.assert_not_called()
        mock_conn.commit.assert_not_called()

    @patch("app.domains.factors.rdagent_service.connection")
    def test_update_run_status(self, mock_conn_ctx):
        mock_conn = MagicMock()
        mock_conn_ctx.return_value.__enter__ = MagicMock(return_value=mock_conn)
        mock_conn_ctx.return_value.__exit__ = MagicMock(return_value=False)

        _update_run_status("run-1", "running")
        mock_conn_ctx.assert_called_once_with("qlib")
        mock_conn.execute.assert_called_once()

    @patch("app.domains.factors.rdagent_service.connection")
    def test_update_run_status_with_error(self, mock_conn_ctx):
        mock_conn = MagicMock()
        mock_conn_ctx.return_value.__enter__ = MagicMock(return_value=mock_conn)
        mock_conn_ctx.return_value.__exit__ = MagicMock(return_value=False)

        _update_run_status("run-1", "failed", "Something went wrong")
        mock_conn_ctx.assert_called_once_with("qlib")
        mock_conn.execute.assert_called_once()

    @patch("app.domains.factors.rdagent_service.connection")
    def test_save_iteration(self, mock_conn_ctx):
        mock_conn = MagicMock()
        mock_conn_ctx.return_value.__enter__ = MagicMock(return_value=mock_conn)
        mock_conn_ctx.return_value.__exit__ = MagicMock(return_value=False)

        mock_result = MagicMock()
        mock_result.lastrowid = 42
        mock_conn.execute.return_value = mock_result

        result = save_iteration(
            run_id="r1",
            iteration_number=1,
            hypothesis="test hypothesis",
            status="completed",
        )
        assert result == 42
        mock_conn_ctx.assert_called_once_with("qlib")
        mock_conn.execute.assert_called_once()

    @patch("app.domains.factors.rdagent_service.connection")
    def test_save_discovered_factor(self, mock_conn_ctx):
        mock_conn = MagicMock()
        mock_conn_ctx.return_value.__enter__ = MagicMock(return_value=mock_conn)
        mock_conn_ctx.return_value.__exit__ = MagicMock(return_value=False)

        mock_result = MagicMock()
        mock_result.lastrowid = 7
        mock_conn.execute.return_value = mock_result

        result = save_discovered_factor(
            run_id="r1",
            factor_name="Alpha1",
            expression="Rank(close)",
            ic_mean=0.05,
            icir=0.3,
            sharpe=1.2,
        )
        assert result == 7
        mock_conn_ctx.assert_called_once_with("qlib")


class TestSerializeJson:

    def test_serialize_dict(self):
        result = _serialize_json({"key": "value"})
        assert isinstance(result, str)
        parsed = json.loads(result)
        assert parsed["key"] == "value"

    def test_serialize_list(self):
        result = _serialize_json([1, 2, 3])
        parsed = json.loads(result)
        assert parsed == [1, 2, 3]

    def test_serialize_with_non_serializable(self):
        from datetime import datetime
        result = _serialize_json({"dt": datetime(2024, 1, 1)})
        assert "2024" in result
