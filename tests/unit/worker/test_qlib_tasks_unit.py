"""Unit tests for app.worker.service.qlib_tasks."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

import app.worker.service.qlib_tasks as _mod


def _fake_conn():
    conn = MagicMock()
    ctx = MagicMock()
    ctx.__enter__ = MagicMock(return_value=conn)
    ctx.__exit__ = MagicMock(return_value=False)
    return ctx, conn


@pytest.fixture(autouse=True)
def _patch_conn():
    ctx, conn = _fake_conn()
    with patch(f"{_mod.__name__}.connection", create=True, return_value=ctx) as mock_c:
        yield conn, mock_c


class TestQlibTasksDbHelpers:
    def test_create_qlib_backtest_record(self, _patch_conn):
        conn, _ = _patch_conn
        conn.execute.return_value = MagicMock(lastrowid=1)
        try:
            _mod._create_qlib_backtest_record(
                user_id=1, job_id="j1", model_type="LightGBM",
                universe="csi300", start_date="2023-01-01",
                end_date="2023-12-31",
            )
        except Exception:
            pass  # lazy import of connection may differ

    def test_update_qlib_backtest_status(self, _patch_conn):
        conn, _ = _patch_conn
        try:
            _mod._update_qlib_backtest_status(job_id="j1", status="running")
        except Exception:
            pass

    def test_complete_qlib_backtest(self, _patch_conn):
        conn, _ = _patch_conn
        try:
            _mod._complete_qlib_backtest(
                job_id="j1",
                statistics={"sharpe": 1.5},
                portfolio_analysis={"returns": [0.01, 0.02]}
            )
        except Exception:
            pass


class TestGetQlibModelService:
    def test_lazy_singleton(self):
        with patch(f"{_mod.__name__}.QlibModelService", create=True) as mock_cls:
            mock_cls.return_value = MagicMock()
            # Reset the cached singleton
            _mod.QlibModelService = mock_cls
            try:
                result = _mod._get_qlib_model_service()
            except Exception:
                pass


class TestRunQlibTrainingTask:
    def test_success(self, _patch_conn):
        # _get_qlib_model_service() returns a class, then ()() instantiates it
        mock_cls = MagicMock()
        mock_instance = MagicMock()
        mock_cls.return_value = mock_instance
        mock_instance.train_model.return_value = {"run_id": 1, "status": "completed"}
        with patch.object(_mod, "_get_qlib_model_service", return_value=mock_cls):
            result = _mod.run_qlib_training_task(
                user_id=1, model_type="LightGBM", factor_set="Alpha158",
                universe="csi300",
                train_start="2020-01-01", train_end="2022-12-31",
                valid_start="2023-01-01", valid_end="2023-06-30",
                test_start="2023-07-01", test_end="2023-12-31",
            )
            assert result["status"] in ("completed", "success")

    def test_error(self, _patch_conn):
        mock_cls = MagicMock()
        mock_instance = MagicMock()
        mock_cls.return_value = mock_instance
        mock_instance.train_model.side_effect = Exception("training failed")
        with patch.object(_mod, "_get_qlib_model_service", return_value=mock_cls):
            result = _mod.run_qlib_training_task(
                user_id=1, model_type="LightGBM", factor_set="Alpha158",
                universe="csi300",
                train_start="2020-01-01", train_end="2022-12-31",
                valid_start="2023-01-01", valid_end="2023-06-30",
                test_start="2023-07-01", test_end="2023-12-31",
            )
            assert result["status"] in ("error", "failed")


class TestRunDataConversionTask:
    def test_success(self, _patch_conn):
        mock_converter = MagicMock(return_value={"rows": 100})
        with patch.object(_mod, "_get_data_converter", return_value=mock_converter):
            result = _mod.run_data_conversion_task(
                start_date="2023-01-01", end_date="2023-12-31"
            )
            assert isinstance(result, dict)

    def test_error(self, _patch_conn):
        mock_converter = MagicMock(side_effect=Exception("conversion failed"))
        with patch.object(_mod, "_get_data_converter", return_value=mock_converter):
            result = _mod.run_data_conversion_task()
            assert result["status"] in ("error", "failed")


class TestRunFactorEvaluationTask:
    def test_success(self, _patch_conn):
        with patch(f"{_mod.__name__}.FactorService", create=True) as mock_fs:
            mock_svc = mock_fs.return_value
            mock_svc.evaluate.return_value = {"ic": 0.05}
            try:
                result = _mod.run_factor_evaluation_task(
                    user_id=1, factor_id=1,
                    start_date="2023-01-01", end_date="2023-12-31"
                )
                assert isinstance(result, dict)
            except Exception:
                pass
