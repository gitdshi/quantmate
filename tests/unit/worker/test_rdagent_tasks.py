"""Tests for rdagent_tasks — run_rdagent_mining_task and helpers."""
import json
from datetime import date
from unittest.mock import patch, MagicMock

import pandas as pd

from app.worker.service.rdagent_tasks import (
    run_rdagent_mining_task,
    _call_sidecar_mining,
    _serialize,
)


class TestRunRDAgentMiningTask:

    @patch("app.worker.service.rdagent_tasks._call_sidecar_mining")
    @patch("app.worker.service.rdagent_tasks._evaluate_discovered_factor_metrics")
    @patch("app.worker.service.rdagent_tasks._build_discovered_factor_eval_context")
    @patch("app.worker.service.rdagent_tasks._get_feature_descriptor")
    @patch("app.domains.factors.rdagent_service._update_run_status")
    @patch("app.domains.factors.rdagent_service.save_discovered_factor")
    @patch("app.domains.factors.rdagent_service.save_iteration")
    def test_successful_mining(self, mock_save_iter, mock_save_factor, mock_update_status,
                                mock_get_fd, mock_build_eval_context, mock_eval_metrics, mock_sidecar):
        mock_fd = MagicMock()
        mock_fd.build_prompt_context.return_value = "test prompt"
        mock_get_fd.return_value = mock_fd
        mock_build_eval_context.return_value = {"ohlcv": object(), "forward_returns": object()}
        mock_eval_metrics.return_value = {"ic_mean": 0.12, "icir": 0.34, "sharpe": 1.56}

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
        mock_eval_metrics.assert_not_called()
        mock_update_status.assert_any_call("test-run-id", "completed", current_iteration=2, total_iterations=2)

    @patch("app.worker.service.rdagent_tasks._call_sidecar_mining")
    @patch("app.worker.service.rdagent_tasks._evaluate_discovered_factor_metrics")
    @patch("app.worker.service.rdagent_tasks._build_discovered_factor_eval_context")
    @patch("app.worker.service.rdagent_tasks._get_feature_descriptor")
    @patch("app.domains.factors.rdagent_service._update_run_status")
    @patch("app.domains.factors.rdagent_service.save_discovered_factor")
    @patch("app.domains.factors.rdagent_service.save_iteration")
    def test_missing_factor_metrics_are_backfilled_from_real_evaluation(
        self,
        mock_save_iter,
        mock_save_factor,
        mock_update_status,
        mock_get_fd,
        mock_build_eval_context,
        mock_eval_metrics,
        mock_sidecar,
    ):
        mock_fd = MagicMock()
        mock_fd.build_prompt_context.return_value = "test prompt"
        mock_get_fd.return_value = mock_fd
        mock_build_eval_context.return_value = {"ohlcv": object(), "forward_returns": object()}
        mock_eval_metrics.return_value = {"ic_mean": 0.12, "icir": 0.34, "sharpe": 1.56}

        mock_sidecar.return_value = {
            "status": "completed",
            "iterations": [],
            "discovered_factors": [
                {"name": "Alpha1", "expression": "volume / mean(volume, 20)", "description": "test", "ic_mean": 0.0, "icir": 0.0, "sharpe": 0.0},
            ],
        }

        run_rdagent_mining_task(
            user_id=1,
            run_id="test-run-id",
            config_dict={"scenario": "fin_factor", "max_iterations": 2, "universe": "csi300"},
        )

        mock_eval_metrics.assert_called_once()
        mock_save_factor.assert_called_once_with(
            run_id="test-run-id",
            factor_name="Alpha1",
            expression="volume / mean(volume, 20)",
            description="test",
            ic_mean=0.12,
            icir=0.34,
            sharpe=1.56,
        )

    @patch("app.worker.service.rdagent_tasks._call_sidecar_mining")
    @patch("app.worker.service.rdagent_tasks._get_feature_descriptor")
    @patch("app.domains.factors.rdagent_service._get_run_status")
    @patch("app.domains.factors.rdagent_service._update_run_status")
    def test_sidecar_failure(self, mock_update_status, mock_get_run_status, mock_get_fd, mock_sidecar):
        mock_fd = MagicMock()
        mock_fd.build_prompt_context.return_value = "test prompt"
        mock_get_fd.return_value = mock_fd
        mock_get_run_status.return_value = "running"

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
    @patch("app.domains.factors.rdagent_service._get_run_status")
    @patch("app.domains.factors.rdagent_service._update_run_status")
    def test_sidecar_failure_redacts_secret_tokens(
        self,
        mock_update_status,
        mock_get_run_status,
        mock_get_fd,
        mock_sidecar,
    ):
        mock_fd = MagicMock()
        mock_fd.build_prompt_context.return_value = "test prompt"
        mock_get_fd.return_value = mock_fd
        mock_get_run_status.return_value = "running"

        mock_sidecar.return_value = {
            "status": "failed",
            "error": "openai_api_key='sk-secret-token' Authorization: Bearer sk-another-secret",
        }

        result = run_rdagent_mining_task(
            user_id=1,
            run_id="test-run-id",
            config_dict={"scenario": "fin_factor"},
        )

        assert result["status"] == "failed"
        assert "sk-secret-token" not in result["error"]
        assert "sk-another-secret" not in result["error"]
        assert "[REDACTED]" in result["error"]

    @patch("app.worker.service.rdagent_tasks._call_sidecar_mining")
    @patch("app.worker.service.rdagent_tasks._get_feature_descriptor")
    @patch("app.domains.factors.rdagent_service._get_run_status")
    @patch("app.domains.factors.rdagent_service._update_run_status")
    def test_sidecar_cancelled_preserves_cancelled_status(
        self, mock_update_status, mock_get_run_status, mock_get_fd, mock_sidecar
    ):
        mock_fd = MagicMock()
        mock_fd.build_prompt_context.return_value = "test prompt"
        mock_get_fd.return_value = mock_fd
        mock_get_run_status.return_value = "cancelled"

        mock_sidecar.return_value = {
            "status": "cancelled",
            "error": "cancelled by user",
        }

        result = run_rdagent_mining_task(
            user_id=1,
            run_id="test-run-id",
            config_dict={"scenario": "fin_factor"},
        )

        assert result["status"] == "cancelled"
        mock_update_status.assert_any_call("test-run-id", "running")
        mock_update_status.assert_any_call("test-run-id", "cancelled", "cancelled by user")

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
        mock_update_status.assert_any_call("empty-run", "completed", current_iteration=0, total_iterations=0)


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
    def test_http_error_redacts_secret_text(self, MockClient, mock_settings):
        import httpx

        mock_settings.return_value = MagicMock(rdagent_sidecar_url="http://test:8001")

        mock_response = MagicMock()
        mock_response.status_code = 500
        mock_response.text = "openai_api_key='sk-secret-token'"
        mock_response.json.return_value = {"error": "Authorization: Bearer sk-secret-token"}

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
        assert "sk-secret-token" not in result["error"]
        assert "[REDACTED]" in result["error"]

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


class TestDiscoveredFactorExpressionNormalization:

    def test_normalize_discovered_factor_expression(self):
        import app.worker.service.rdagent_tasks as mod

        result = mod._normalize_discovered_factor_expression("$volume / mean(volume, 20) + rolling_std(return, 5)")

        assert result == "volume / ts_mean(volume, 20) + ts_std(ret_1d, 5)"

    def test_normalize_latex_momentum_expression(self):
        import app.worker.service.rdagent_tasks as mod

        result = mod._normalize_discovered_factor_expression(
            r"M_{20d} = \frac{close_t}{close_{t-20}} - 1"
        )

        assert result == "(close) / (delay(close, 20)) - 1"

    def test_normalize_latex_volatility_expression(self):
        import app.worker.service.rdagent_tasks as mod

        result = mod._normalize_discovered_factor_expression(
            r"\sigma_{20d} = \sqrt{\frac{1}{19} \sum_{i=0}^{19} (r_{t-i} - \bar{r}_{20d})^2} \text{ where } r_{t-i} = \frac{close_{t-i}}{close_{t-i-1}} - 1"
        )

        assert result == "ts_std(ret_1d, 20)"

    def test_normalize_latex_volume_ratio_expression(self):
        import app.worker.service.rdagent_tasks as mod

        result = mod._normalize_discovered_factor_expression(
            r"VR_{20d} = \frac{volume_t}{\frac{1}{20} \sum_{i=0}^{19} volume_{t-i}}"
        )

        assert result == "volume / ts_mean(volume, 20)"

    def test_normalize_latex_close_range_expression(self):
        import app.worker.service.rdagent_tasks as mod

        result = mod._normalize_discovered_factor_expression(
            r"CR_{HL} = \frac{close_t - low_t}{high_t - low_t}"
        )

        assert result == "(close - low) / (high - low)"

    def test_normalize_stage_like_momentum_expression(self):
        import app.worker.service.rdagent_tasks as mod

        result = mod._normalize_discovered_factor_expression(
            r"$Momentum_{5d} = \frac{Close_t}{Close_{t-5}} - 1$"
        )

        assert result == "(close) / (delay(close, 5)) - 1"

    def test_normalize_stage_like_volume_ratio_expression(self):
        import app.worker.service.rdagent_tasks as mod

        result = mod._normalize_discovered_factor_expression(
            r"$VolumeRatio_{20d} = \frac{Volume_t}{Mean(Volume_{t-19:t})}$"
        )

        assert result == "(volume) / (ts_mean(volume, 20))"

    def test_normalize_stage_like_vwap_ratio_expression(self):
        import app.worker.service.rdagent_tasks as mod

        result = mod._normalize_discovered_factor_expression(
            r"$VWAPCloseRatio_{5d} = \frac{Mean(VWAP_{t-4:t})}{Close_t}$"
        )

        assert result == "(ts_mean(vwap, 5)) / (close)"

    def test_normalize_assignment_style_momentum_expression(self):
        import app.worker.service.rdagent_tasks as mod

        result = mod._normalize_discovered_factor_expression(
            r"momentum\_20d = \frac{close_t}{close_{t-20}} - 1"
        )

        assert result == "(close) / (delay(close, 20)) - 1"

    def test_normalize_assignment_style_volume_ratio_expression(self):
        import app.worker.service.rdagent_tasks as mod

        result = mod._normalize_discovered_factor_expression(
            r"volume\_ratio\_20d = \frac{volume_t}{\frac{1}{20}\sum_{i=0}^{19} volume_{t-i}}"
        )

        assert result == "volume / ts_mean(volume, 20)"

    def test_normalize_text_wrapped_volume_ratio_expression(self):
        import app.worker.service.rdagent_tasks as mod

        result = mod._normalize_discovered_factor_expression(
            r"$VR_{20d} = \frac{volume_t}{\text{mean}(volume_{t-19:t}]}$"
        )

        assert result == "(volume) / (ts_mean(volume, 20))"

    def test_normalize_price_relative_ma_expression(self):
        import app.worker.service.rdagent_tasks as mod

        result = mod._normalize_discovered_factor_expression(
            r"$P_{MA20} = \frac{close_t}{\text{MA}_{20,t}} - 1 = \frac{close_t}{\frac{1}{20}\sum_{i=0}^{19}close_{t-i}} - 1$"
        )

        assert result == "(close) / (ts_mean(close, 20)) - 1"

    def test_normalize_price_return_expression_with_symbol_aliases(self):
        import app.worker.service.rdagent_tasks as mod

        result = mod._normalize_discovered_factor_expression(
            r"r_{10d} = \frac{P_t - P_{t-10}}{P_{t-10}}"
        )

        assert result == "(close - delay(close, 10)) / (delay(close, 10))"

    def test_normalize_longer_price_return_expression_with_symbol_aliases(self):
        import app.worker.service.rdagent_tasks as mod

        result = mod._normalize_discovered_factor_expression(
            r"r_{20d} = \frac{P_t - P_{t-20}}{P_{t-20}}"
        )

        assert result == "(close - delay(close, 20)) / (delay(close, 20))"

    def test_normalize_volume_ratio_expression_with_future_window_notation(self):
        import app.worker.service.rdagent_tasks as mod

        result = mod._normalize_discovered_factor_expression(
            r"VR_{20d} = \frac{V_t}{\text{mean}(V_{t-19:t+1})}"
        )

        assert result == "(volume) / (ts_mean(volume, 20))"

    def test_normalize_indexed_momentum_expression(self):
        import app.worker.service.rdagent_tasks as mod

        result = mod._normalize_discovered_factor_expression(
            r"momentum\_5d_{i,t} = \frac{close_{i,t} - close_{i,t-5}}{close_{i,t-5}}"
        )

        assert result == "(close - delay(close, 5)) / (delay(close, 5))"

    def test_normalize_indexed_volume_ratio_expression(self):
        import app.worker.service.rdagent_tasks as mod

        result = mod._normalize_discovered_factor_expression(
            r"volume\_ratio\_20d_{i,t} = \frac{volume_{i,t}}{\text{mean}(volume_{i,t-19}:volume_{i,t})}"
        )

        assert result == "(volume) / (ts_mean(volume, 20))"

    def test_normalize_indexed_volume_momentum_expression(self):
        import app.worker.service.rdagent_tasks as mod

        result = mod._normalize_discovered_factor_expression(
            r"volume\_momentum\_20d_{i,t} = \frac{volume_{i,t} - volume_{i,t-20}}{volume_{i,t-20}}"
        )

        assert result == "(volume - delay(volume, 20)) / (delay(volume, 20))"

    def test_normalize_staging_price_return_expression_with_field_superscript(self):
        import app.worker.service.rdagent_tasks as mod

        result = mod._normalize_discovered_factor_expression(
            r"r_t = \frac{P_t^{close} - P_{t-1}^{close}}{P_{t-1}^{close}}"
        )

        assert result == "(close - delay(close, 1)) / (delay(close, 1))"

    def test_normalize_staging_momentum_expression_with_field_superscript(self):
        import app.worker.service.rdagent_tasks as mod

        result = mod._normalize_discovered_factor_expression(
            r"Mom_{10d,t} = \frac{P_t^{close} - P_{t-10}^{close}}{P_{t-10}^{close}}"
        )

        assert result == "(close - delay(close, 10)) / (delay(close, 10))"

    def test_normalize_staging_volume_ratio_expression_with_symbol_aliases(self):
        import app.worker.service.rdagent_tasks as mod

        result = mod._normalize_discovered_factor_expression(
            r"VolRatio_{20d,t} = \frac{V_t}{\frac{1}{20}\sum_{i=0}^{19} V_{t-i}}"
        )

        assert result == "volume / ts_mean(volume, 20)"

    def test_normalize_staging_volatility_expression(self):
        import app.worker.service.rdagent_tasks as mod

        result = mod._normalize_discovered_factor_expression(
            r"Vol_{10d,t} = \sqrt{\frac{1}{9}\sum_{i=0}^{9}(r_{t-i} - \bar{r}_{10d})^2}"
        )

        assert result == "ts_std(ret_1d, 10)"


class TestResolveEvalInstruments:

    @patch("app.infrastructure.db.connections.connection")
    def test_falls_back_to_latest_index_snapshot_when_historical_members_missing(self, mock_connection):
        import app.worker.service.rdagent_tasks as mod

        mock_conn = MagicMock()
        mock_context = MagicMock()
        mock_context.__enter__.return_value = mock_conn
        mock_context.__exit__.return_value = False
        mock_connection.return_value = mock_context

        historical_snapshot = MagicMock()
        historical_snapshot.scalar.return_value = None

        latest_snapshot = MagicMock()
        latest_snapshot.scalar.return_value = "2025-06-30"

        members_result = MagicMock()
        members_result.fetchall.return_value = [("000001.SZ",), ("000002.SZ",)]

        mock_conn.execute.side_effect = [historical_snapshot, latest_snapshot, members_result]

        instruments = mod._resolve_eval_instruments("csi300", date(2024, 3, 31))

        assert instruments == ["000001.SZ", "000002.SZ"]


class TestBuildDiscoveredFactorEvalContext:

    @patch("app.domains.factors.expression_engine.compute_forward_returns")
    @patch("app.domains.factors.expression_engine.fetch_ohlcv")
    @patch("app.infrastructure.db.connections.connection")
    @patch("app.worker.service.rdagent_tasks._resolve_eval_instruments")
    def test_retries_with_latest_available_window_when_requested_range_is_empty(
        self,
        mock_resolve_instruments,
        mock_connection,
        mock_fetch_ohlcv,
        mock_compute_forward_returns,
    ):
        import app.worker.service.rdagent_tasks as mod

        mock_resolve_instruments.return_value = ["000001.SZ"]

        mock_conn = MagicMock()
        mock_context = MagicMock()
        mock_context.__enter__.return_value = mock_conn
        mock_context.__exit__.return_value = False
        mock_connection.return_value = mock_context

        range_result = MagicMock()
        range_result.first.return_value = ("2024-12-30", "2025-03-31")
        mock_conn.execute.return_value = range_result

        fetched = pd.DataFrame(
            {
                "open": [10.0, 10.5],
                "high": [10.2, 10.7],
                "low": [9.8, 10.1],
                "close": [10.1, 10.6],
                "volume": [100, 120],
                "amount": [1000, 1100],
                "factor": [1.0, 1.0],
            },
            index=pd.MultiIndex.from_tuples(
                [
                    ("000001.SZ", pd.Timestamp("2025-03-30")),
                    ("000001.SZ", pd.Timestamp("2025-03-31")),
                ],
                names=["instrument", "date"],
            ),
        )
        mock_fetch_ohlcv.side_effect = [pd.DataFrame(), fetched]
        mock_compute_forward_returns.return_value = pd.Series([0.01, 0.02], index=fetched.index)

        result = mod._build_discovered_factor_eval_context(
            {
                "universe": "csi300",
                "start_date": "2024-01-01",
                "end_date": "2024-03-31",
            }
        )

        assert result is not None
        assert mock_fetch_ohlcv.call_count == 2
        assert mock_fetch_ohlcv.call_args_list[1].kwargs == {
            "start_date": date(2024, 12, 31),
            "end_date": date(2025, 3, 31),
            "instruments": ["000001.SZ"],
        }
        assert list(result["forward_returns"]) == [0.01, 0.02]


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
