"""Tests for rdagent_tasks — run_rdagent_mining_task and helpers."""
import json
import pytest
from unittest.mock import patch, MagicMock

from app.worker.service.rdagent_tasks import (
    run_rdagent_mining_task,
    _call_sidecar_mining,
    _serialize,
)


class TestRunRDAgentMiningTask:

    @patch("app.worker.service.rdagent_tasks._call_sidecar_mining")
    @patch("app.worker.service.rdagent_tasks._get_feature_descriptor")
    @patch("app.domains.factors.rdagent_service._update_run_status")
    @patch("app.domains.factors.rdagent_service.save_discovered_factor")
    @patch("app.domains.factors.rdagent_service.save_iteration")
    def test_successful_mining(self, mock_save_iter, mock_save_factor, mock_update_status,
                                mock_get_fd, mock_sidecar):
        mock_fd = MagicMock()
        mock_fd.build_prompt_context.return_value = "test prompt"
        mock_get_fd.return_value = mock_fd

        mock_sidecar.return_value = {
            "status": "completed",
            "iterations": [
                {"iteration": 1, "hypothesis": "H1", "code": "x=1", "metrics": {"ic": 0.05}, "feedback": "good", "status": "completed"},
                {"iteration": 2, "hypothesis": "H2", "code": "x=2", "metrics": {"ic": 0.08}, "feedback": "better", "status": "completed"},
            ],
            "discovered_factors": [
                {"name": "Alpha1", "expression": "Rank(close)", "description": "test", "ic_mean": 0.05, "icir": 0.3, "sharpe": 1.2},
            ],
        }

        result = run_rdagent_mining_task(
            user_id=1,
            run_id="test-run-id",
            config_dict={"scenario": "fin_factor", "max_iterations": 2},
        )

        assert result["status"] == "completed"
        assert result["iterations"] == 2
        assert result["discovered_factors"] == 1
        assert mock_save_iter.call_count == 2
        assert mock_save_factor.call_count == 1

    @patch("app.worker.service.rdagent_tasks._call_sidecar_mining")
    @patch("app.worker.service.rdagent_tasks._get_feature_descriptor")
    @patch("app.domains.factors.rdagent_service._update_run_status")
    def test_sidecar_failure(self, mock_update_status, mock_get_fd, mock_sidecar):
        mock_fd = MagicMock()
        mock_fd.build_prompt_context.return_value = "test prompt"
        mock_get_fd.return_value = mock_fd

        mock_sidecar.return_value = {
            "status": "failed",
            "error": "Sidecar crashed",
        }

        result = run_rdagent_mining_task(
            user_id=1,
            run_id="test-run-id",
            config_dict={"scenario": "fin_factor"},
        )

        assert result["status"] == "failed"
        assert result["error"] == "Sidecar crashed"

    @patch("app.worker.service.rdagent_tasks._call_sidecar_mining")
    @patch("app.worker.service.rdagent_tasks._get_feature_descriptor")
    @patch("app.domains.factors.rdagent_service._update_run_status")
    def test_exception_handling(self, mock_update_status, mock_get_fd, mock_sidecar):
        mock_fd = MagicMock()
        mock_fd.build_prompt_context.side_effect = Exception("feature descriptor error")
        mock_get_fd.return_value = mock_fd

        result = run_rdagent_mining_task(
            user_id=1,
            run_id="test-run-id",
            config_dict={},
        )

        assert result["status"] == "failed"
        assert "feature descriptor error" in result["error"]

    @patch("app.worker.service.rdagent_tasks._call_sidecar_mining")
    @patch("app.worker.service.rdagent_tasks._get_feature_descriptor")
    @patch("app.domains.factors.rdagent_service._update_run_status")
    @patch("app.domains.factors.rdagent_service.save_discovered_factor")
    @patch("app.domains.factors.rdagent_service.save_iteration")
    def test_no_iterations_or_factors(self, mock_save_iter, mock_save_factor,
                                      mock_update_status, mock_get_fd, mock_sidecar):
        mock_fd = MagicMock()
        mock_fd.build_prompt_context.return_value = "test"
        mock_get_fd.return_value = mock_fd

        mock_sidecar.return_value = {
            "status": "completed",
            "iterations": [],
            "discovered_factors": [],
        }

        result = run_rdagent_mining_task(
            user_id=1, run_id="empty-run", config_dict={},
        )

        assert result["status"] == "completed"
        assert result["iterations"] == 0
        assert result["discovered_factors"] == 0
        mock_save_iter.assert_not_called()
        mock_save_factor.assert_not_called()


class TestCallSidecarMining:

    @patch("app.infrastructure.config.config.get_settings")
    @patch("httpx.Client")
    def test_successful_call(self, MockClient, mock_settings):
        mock_settings.return_value = MagicMock(rdagent_sidecar_url="http://test:8001")

        mock_resp = MagicMock()
        mock_resp.json.return_value = {"status": "completed", "iterations": [], "discovered_factors": []}
        mock_resp.raise_for_status = MagicMock()

        mock_client_instance = MagicMock()
        mock_client_instance.post.return_value = mock_resp
        mock_client_instance.__enter__ = MagicMock(return_value=mock_client_instance)
        mock_client_instance.__exit__ = MagicMock(return_value=False)
        MockClient.return_value = mock_client_instance

        result = _call_sidecar_mining(
            run_id="r1",
            config={"scenario": "fin_factor"},
            prompt_context="test",
        )
        assert result["status"] == "completed"

    @patch("app.infrastructure.config.config.get_settings")
    @patch("httpx.Client")
    def test_connection_error(self, MockClient, mock_settings):
        import httpx
        mock_settings.return_value = MagicMock(rdagent_sidecar_url="http://test:8001")

        mock_client_instance = MagicMock()
        mock_client_instance.post.side_effect = httpx.ConnectError("Connection refused")
        mock_client_instance.__enter__ = MagicMock(return_value=mock_client_instance)
        mock_client_instance.__exit__ = MagicMock(return_value=False)
        MockClient.return_value = mock_client_instance

        result = _call_sidecar_mining(
            run_id="r1",
            config={},
            prompt_context="test",
        )
        assert result["status"] == "failed"
        assert "Cannot connect" in result["error"]

    @patch("app.infrastructure.config.config.get_settings")
    @patch("httpx.Client")
    def test_http_error(self, MockClient, mock_settings):
        import httpx
        mock_settings.return_value = MagicMock(rdagent_sidecar_url="http://test:8001")

        mock_response = MagicMock()
        mock_response.status_code = 500
        mock_response.text = "Internal Server Error"

        mock_client_instance = MagicMock()
        mock_client_instance.post.return_value = mock_response
        mock_response.raise_for_status.side_effect = httpx.HTTPStatusError(
            "Server Error", request=MagicMock(), response=mock_response
        )
        mock_client_instance.__enter__ = MagicMock(return_value=mock_client_instance)
        mock_client_instance.__exit__ = MagicMock(return_value=False)
        MockClient.return_value = mock_client_instance

        result = _call_sidecar_mining(
            run_id="r1",
            config={},
            prompt_context="test",
        )
        assert result["status"] == "failed"
        assert "500" in result["error"]

    @patch("app.infrastructure.config.config.get_settings")
    @patch("httpx.Client")
    def test_generic_exception(self, MockClient, mock_settings):
        mock_settings.return_value = MagicMock(rdagent_sidecar_url="http://test:8001")

        mock_client_instance = MagicMock()
        mock_client_instance.post.side_effect = RuntimeError("Unexpected")
        mock_client_instance.__enter__ = MagicMock(return_value=mock_client_instance)
        mock_client_instance.__exit__ = MagicMock(return_value=False)
        MockClient.return_value = mock_client_instance

        result = _call_sidecar_mining(
            run_id="r1",
            config={},
            prompt_context="test",
        )
        assert result["status"] == "failed"
        assert "Unexpected" in result["error"]


class TestSerialize:

    def test_serialize_none(self):
        assert _serialize(None) is None

    def test_serialize_dict(self):
        result = _serialize({"ic": 0.05})
        parsed = json.loads(result)
        assert parsed["ic"] == 0.05

    def test_serialize_list(self):
        result = _serialize([1, 2])
        parsed = json.loads(result)
        assert parsed == [1, 2]


class TestLazyLoaders:
    """Tests for _get_rdagent_service and _get_feature_descriptor lazy loaders."""

    def test_get_rdagent_service_returns_class(self):
        import app.worker.service.rdagent_tasks as mod
        original = mod._RDAgentService
        try:
            mod._RDAgentService = None
            cls = mod._get_rdagent_service()
            from app.domains.factors.rdagent_service import RDAgentService
            assert cls is RDAgentService
        finally:
            mod._RDAgentService = original

    def test_get_rdagent_service_cached(self):
        import app.worker.service.rdagent_tasks as mod
        original = mod._RDAgentService
        try:
            mod._RDAgentService = None
            first = mod._get_rdagent_service()
            second = mod._get_rdagent_service()
            assert first is second
        finally:
            mod._RDAgentService = original

    def test_get_feature_descriptor_returns_module(self):
        import app.worker.service.rdagent_tasks as mod
        original = mod._feature_descriptor
        try:
            mod._feature_descriptor = None
            fd = mod._get_feature_descriptor()
            from app.domains.factors import feature_descriptor
            assert fd is feature_descriptor
        finally:
            mod._feature_descriptor = original

    def test_get_feature_descriptor_cached(self):
        import app.worker.service.rdagent_tasks as mod
        original = mod._feature_descriptor
        try:
            mod._feature_descriptor = None
            first = mod._get_feature_descriptor()
            second = mod._get_feature_descriptor()
            assert first is second
        finally:
            mod._feature_descriptor = original


class TestExceptionHandlerUpdateFails:
    """Test the inner except when _update_run_status also fails."""

    @patch("app.worker.service.rdagent_tasks._call_sidecar_mining")
    @patch("app.worker.service.rdagent_tasks._get_feature_descriptor")
    def test_double_exception(self, mock_get_fd, mock_sidecar):
        """When the outer except tries to update run status and that also fails."""
        mock_fd = MagicMock()
        mock_fd.build_prompt_context.side_effect = RuntimeError("boom")
        mock_get_fd.return_value = mock_fd

        # First call (set to running) succeeds; second call (set to failed) raises
        with patch(
            "app.domains.factors.rdagent_service._update_run_status",
            side_effect=[None, Exception("DB connection lost")],
        ):
            result = run_rdagent_mining_task(
                user_id=1,
                run_id="fail-run",
                config_dict={},
            )

        assert result["status"] == "failed"
        assert "boom" in result["error"]
