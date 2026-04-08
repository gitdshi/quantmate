"""Unit tests for app.domains.jobs.service."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

import app.domains.jobs.service as _mod


@pytest.fixture()
def svc():
    with patch.object(_mod, "get_backtest_service") as mock_bt, \
         patch.object(_mod, "get_job_storage") as mock_js, \
         patch.object(_mod, "BacktestHistoryDao") as mock_hd, \
         patch.object(_mod, "BulkBacktestDao") as mock_bd:
        s = _mod.JobsService()
        yield s, {
            "backtest_service": mock_bt.return_value,
            "job_storage": mock_js.return_value,
            "history_dao": mock_hd.return_value,
            "bulk_dao": mock_bd.return_value,
        }


class TestJobsService:
    def test_list_jobs_no_bulk(self, svc):
        s, deps = svc
        deps["backtest_service"].list_user_jobs.return_value = [
            {"job_id": "j1", "status": "completed"}
        ]
        result = s.list_jobs(user_id=1, status=None, limit=50)
        assert isinstance(result, list)
        assert len(result) == 1

    def test_list_jobs_empty(self, svc):
        s, deps = svc
        deps["backtest_service"].list_user_jobs.return_value = []
        result = s.list_jobs(user_id=1, status=None, limit=50)
        assert result == []

    def test_list_jobs_with_bulk(self, svc):
        s, deps = svc
        deps["backtest_service"].list_user_jobs.return_value = [
            {"job_id": "bulk_1", "status": "completed"}
        ]
        deps["bulk_dao"].list_by_job_ids.return_value = [
            {"job_id": "bulk_1", "best_return": 0.15, "best_symbol": "000001.SZ",
             "completed_count": 10, "total_symbols": 10}
        ]
        deps["history_dao"].get_child_result_json.return_value = {
            "statistics": {"annual_return": 0.3, "sharpe_ratio": 1.5, "max_drawdown_percent": -0.1},
            "symbol_name": "Ping An"
        }
        result = s.list_jobs(user_id=1, status=None, limit=50)
        assert result[0]["result"]["best_return"] == 0.15

    def test_delete_job_single(self, svc):
        s, deps = svc
        deps["job_storage"].delete_job.return_value = True
        s.delete_job_and_results(job_id="j1", user_id=1)
        deps["history_dao"].delete_single.assert_called_once_with("j1", 1)

    def test_delete_job_bulk(self, svc):
        s, deps = svc
        deps["job_storage"].delete_job.return_value = True
        redis_mock = MagicMock()
        redis_mock.scan_iter.return_value = []
        deps["job_storage"].redis = redis_mock
        s.delete_job_and_results(job_id="bulk_abc", user_id=1)
        deps["history_dao"].delete_bulk_children.assert_called_once()
        deps["bulk_dao"].delete_bulk_parent.assert_called_once()

    def test_delete_job_fails(self, svc):
        s, deps = svc
        deps["job_storage"].delete_job.return_value = False
        with pytest.raises(RuntimeError):
            s.delete_job_and_results(job_id="j1", user_id=1)
