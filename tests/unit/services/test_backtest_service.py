"""Tests for api/services/backtest_service.py — 201 uncovered stmts.

Covers: calculate_alpha_beta, get_benchmark_data, get_stock_name,
convert_to_tushare_symbol, ensure_vnpy_history_data,
BacktestServiceV2 (job management), BacktestService._load_builtin_strategies.
"""

import pytest
import numpy as np
from datetime import date, datetime
from unittest.mock import MagicMock, patch, PropertyMock
import json

import app.api.services.backtest_service as _bs


# =====================================================================
# calculate_alpha_beta
# =====================================================================

@pytest.mark.unit
class TestCalculateAlphaBeta:
    def test_basic(self):
        strategy = np.array([0.01, 0.02, -0.01, 0.03, 0.01, -0.02, 0.015, 0.005, -0.005, 0.008])
        benchmark = np.array([0.005, 0.015, -0.005, 0.025, 0.008, -0.015, 0.01, 0.003, -0.003, 0.006])
        alpha, beta = _bs.calculate_alpha_beta(strategy, benchmark)
        assert isinstance(alpha, float)
        assert isinstance(beta, float)

    def test_too_short(self):
        a, b = _bs.calculate_alpha_beta(np.array([0.01]), np.array([0.01]))
        assert a is None and b is None

    def test_empty(self):
        a, b = _bs.calculate_alpha_beta(np.array([]), np.array([]))
        assert a is None and b is None

    def test_nan_handling(self):
        s = np.array([0.01, np.nan, 0.02, 0.03, -0.01])
        b = np.array([0.005, 0.01, np.nan, 0.02, -0.005])
        alpha, beta = _bs.calculate_alpha_beta(s, b)
        # After NaN masking, 3 valid pairs remain
        assert alpha is not None

    def test_all_nan(self):
        a, b = _bs.calculate_alpha_beta(np.array([np.nan, np.nan]), np.array([np.nan, np.nan]))
        assert a is None and b is None

    def test_different_lengths(self):
        s = np.array([0.01, 0.02, 0.03, 0.04, 0.05])
        bm = np.array([0.005, 0.01, 0.015])
        alpha, beta = _bs.calculate_alpha_beta(s, bm)
        assert alpha is not None


# =====================================================================
# get_benchmark_data
# =====================================================================

@pytest.mark.unit
class TestGetBenchmarkData:
    def test_success(self, monkeypatch):
        mock_dao = MagicMock()
        mock_dao.get_benchmark_data.return_value = {"returns": [0.01], "total_return": 5.0}
        monkeypatch.setattr(_bs, "AkshareBenchmarkDao", lambda: mock_dao)
        result = _bs.get_benchmark_data(date(2024, 1, 1), date(2024, 12, 31))
        assert result["total_return"] == 5.0

    def test_custom_benchmark(self, monkeypatch):
        mock_dao = MagicMock()
        mock_dao.get_benchmark_data.return_value = {"returns": [], "total_return": 3.0}
        monkeypatch.setattr(_bs, "AkshareBenchmarkDao", lambda: mock_dao)
        result = _bs.get_benchmark_data(date(2024, 1, 1), date(2024, 12, 31), "000001.SZ")
        assert result is not None
        mock_dao.get_benchmark_data.assert_called_once()

    def test_exception(self, monkeypatch):
        mock_dao = MagicMock()
        mock_dao.get_benchmark_data.side_effect = Exception("db error")
        monkeypatch.setattr(_bs, "AkshareBenchmarkDao", lambda: mock_dao)
        assert _bs.get_benchmark_data(date(2024, 1, 1), date(2024, 12, 31)) is None


# =====================================================================
# get_stock_name
# =====================================================================

@pytest.mark.unit
class TestGetStockName:
    def test_success(self, monkeypatch):
        mock_svc = MagicMock()
        mock_svc.resolve_symbol_name.return_value = "平安银行"
        monkeypatch.setattr(_bs, "MarketService", lambda: mock_svc)
        assert _bs.get_stock_name("000001.SZ") == "平安银行"

    def test_empty_name(self, monkeypatch):
        mock_svc = MagicMock()
        mock_svc.resolve_symbol_name.return_value = ""
        monkeypatch.setattr(_bs, "MarketService", lambda: mock_svc)
        assert _bs.get_stock_name("000001.SZ") is None

    def test_exception(self, monkeypatch):
        mock_svc = MagicMock()
        mock_svc.resolve_symbol_name.side_effect = Exception("fail")
        monkeypatch.setattr(_bs, "MarketService", lambda: mock_svc)
        assert _bs.get_stock_name("000001.SZ") is None


# =====================================================================
# convert_to_tushare_symbol (module-level function)
# =====================================================================

@pytest.mark.unit
class TestBsConvertToTushareSymbol:
    def test_szse(self):
        assert _bs.convert_to_tushare_symbol("000001.SZSE") == "000001.SZ"

    def test_sse(self):
        assert _bs.convert_to_tushare_symbol("600000.SSE") == "600000.SH"

    def test_bse(self):
        assert _bs.convert_to_tushare_symbol("430047.BSE") == "430047.BJ"

    def test_already_tushare(self):
        assert _bs.convert_to_tushare_symbol("000001.SZ") == "000001.SZ"

    def test_empty(self):
        assert _bs.convert_to_tushare_symbol("") == ""

    def test_no_dot(self):
        assert _bs.convert_to_tushare_symbol("000001") == "000001"


# =====================================================================
# ensure_vnpy_history_data (module-level function)
# =====================================================================

@pytest.mark.unit
class TestBsEnsureVnpyHistoryData:
    def test_success(self, monkeypatch):
        monkeypatch.setattr(_bs, "sync_symbol_to_vnpy", lambda *a, **kw: 100)
        monkeypatch.setattr(_bs, "update_bar_overview", lambda *a: None)
        monkeypatch.setattr(_bs, "get_ts_symbol", lambda x: "000001")
        monkeypatch.setattr(_bs, "map_ts_exchange", lambda x: "SZSE")
        result = _bs.ensure_vnpy_history_data("000001.SZSE", date(2024, 1, 1))
        assert result == 100

    def test_no_sync(self, monkeypatch):
        monkeypatch.setattr(_bs, "sync_symbol_to_vnpy", lambda *a, **kw: 0)
        monkeypatch.setattr(_bs, "get_ts_symbol", lambda x: "000001")
        monkeypatch.setattr(_bs, "map_ts_exchange", lambda x: "SZSE")
        result = _bs.ensure_vnpy_history_data("000001.SZSE", date(2024, 1, 1))
        assert result == 0

    def test_invalid_symbol(self):
        assert _bs.ensure_vnpy_history_data("nodot", date(2024, 1, 1)) == 0

    def test_empty_symbol(self):
        assert _bs.ensure_vnpy_history_data("", date(2024, 1, 1)) == 0


# =====================================================================
# BacktestServiceV2
# =====================================================================

@pytest.mark.unit
class TestBacktestServiceV2:
    """Test BacktestServiceV2 job management methods."""

    @pytest.fixture()
    def svc(self, monkeypatch):
        """Create a BacktestServiceV2 with mocked job_storage."""
        monkeypatch.setattr(_bs, "get_job_storage", lambda: MagicMock())
        s = _bs.BacktestServiceV2()
        s.job_storage = MagicMock()
        return s

    # --- get_job_status ---
    def test_get_job_status_from_storage(self, svc):
        svc.job_storage.get_job_metadata.return_value = {
            "user_id": 1, "status": "running", "type": "backtest",
            "progress": 50, "created_at": "2024-01-01", "updated_at": None,
            "symbol": "000001.SZ",
        }
        result = svc.get_job_status("bt_123", user_id=1)
        assert result["status"] == "running"
        assert result["symbol"] == "000001.SZ"

    def test_get_job_status_wrong_user(self, svc):
        svc.job_storage.get_job_metadata.return_value = {"user_id": 2}
        assert svc.get_job_status("bt_123", user_id=1) is None

    def test_get_job_status_completed_with_result(self, svc):
        svc.job_storage.get_job_metadata.return_value = {
            "user_id": 1, "status": "completed", "type": "backtest",
            "progress": 100, "created_at": "2024-01-01",
        }
        svc.job_storage.get_result.return_value = {"total_return": 15.0}
        result = svc.get_job_status("bt_123", user_id=1)
        assert result["result"]["total_return"] == 15.0

    def test_get_job_status_not_found_fallback(self, svc, monkeypatch):
        svc.job_storage.get_job_metadata.return_value = None
        mock_dao = MagicMock()
        mock_dao.get_job_row.return_value = None
        monkeypatch.setattr(_bs, "BacktestHistoryDao", lambda: mock_dao)
        assert svc.get_job_status("bt_999", user_id=1) is None

    def test_get_job_status_bulk_with_metrics(self, svc, monkeypatch):
        svc.job_storage.get_job_metadata.return_value = {
            "user_id": 1, "status": "completed", "type": "bulk_backtest",
            "progress": 100, "created_at": "2024-01-01",
        }
        svc.job_storage.get_result.return_value = {}
        mock_dao = MagicMock()
        mock_dao.get_metrics.return_value = {
            "best_return": 25.5, "best_symbol": "600000.SH", "completed_count": 10,
        }
        monkeypatch.setattr(_bs, "BulkBacktestDao", lambda: mock_dao)
        result = svc.get_job_status("bulk_123", user_id=1)
        assert result["result"]["best_return"] == 25.5

    # --- list_user_jobs ---
    def test_list_user_jobs(self, svc):
        svc.job_storage.list_user_jobs.return_value = [{"job_id": "bt_1"}, {"job_id": "bt_2"}]
        result = svc.list_user_jobs(user_id=1, status="completed", limit=10)
        assert len(result) == 2
        svc.job_storage.list_user_jobs.assert_called_once_with(1, "completed", 10)

    def test_list_user_jobs_empty(self, svc):
        svc.job_storage.list_user_jobs.return_value = []
        assert svc.list_user_jobs(user_id=1) == []

    # --- cancel_job ---
    def test_cancel_job_success(self, svc, monkeypatch):
        svc.job_storage.get_job_metadata.return_value = {"user_id": 1, "type": "backtest"}
        svc.job_storage.cancel_job.return_value = True
        mock_queue = MagicMock()
        monkeypatch.setattr(_bs, "get_queue", lambda n: mock_queue)
        assert svc.cancel_job("bt_123", user_id=1) is True

    def test_cancel_wrong_user(self, svc):
        svc.job_storage.get_job_metadata.return_value = {"user_id": 2, "type": "backtest"}
        assert svc.cancel_job("bt_123", user_id=1) is False

    def test_cancel_not_found(self, svc):
        svc.job_storage.get_job_metadata.return_value = None
        assert svc.cancel_job("bt_999", user_id=1) is False

    def test_cancel_optimization_queue(self, svc, monkeypatch):
        svc.job_storage.get_job_metadata.return_value = {"user_id": 1, "type": "optimization"}
        svc.job_storage.cancel_job.return_value = True
        queues = {}
        def fake_queue(name):
            q = MagicMock()
            queues[name] = q
            return q
        monkeypatch.setattr(_bs, "get_queue", fake_queue)
        svc.cancel_job("opt_123", user_id=1)
        assert "optimization" in queues

    # --- _get_strategy_from_db ---
    def test_get_strategy_from_db_success(self, svc, monkeypatch):
        mock_dao = MagicMock()
        mock_dao.get_strategy_source_for_user.return_value = ("code", "ClassName", "v1")
        monkeypatch.setattr(_bs, "StrategySourceDao", lambda: mock_dao)
        code, cls, ver = svc._get_strategy_from_db(1, 1)
        assert cls == "ClassName"

    def test_get_strategy_from_db_not_found(self, svc, monkeypatch):
        mock_dao = MagicMock()
        mock_dao.get_strategy_source_for_user.side_effect = KeyError("not found")
        monkeypatch.setattr(_bs, "StrategySourceDao", lambda: mock_dao)
        with pytest.raises(ValueError, match="not found"):
            svc._get_strategy_from_db(999, 1)

    # --- _get_child_job_from_db ---
    def test_get_child_job_found(self, svc, monkeypatch):
        mock_dao = MagicMock()
        mock_dao.get_job_row.return_value = {
            "user_id": 1, "vt_symbol": "000001.SZ", "strategy_class": "Test",
            "status": "completed", "result": json.dumps({"total_return": 10}),
            "parameters": json.dumps({"p": 1}),
            "created_at": datetime(2024, 1, 1), "completed_at": datetime(2024, 1, 2),
            "start_date": date(2024, 1, 1), "end_date": date(2024, 12, 31),
            "strategy_id": 1, "strategy_version": "v1", "bulk_job_id": None,
        }
        monkeypatch.setattr(_bs, "BacktestHistoryDao", lambda: mock_dao)
        mock_ms = MagicMock()
        mock_ms.resolve_symbol_name.return_value = "平安银行"
        monkeypatch.setattr(_bs, "MarketService", lambda: mock_ms)

        result = svc._get_child_job_from_db("bt_123", user_id=1)
        assert result["status"] == "completed"
        assert result["symbol_name"] == "平安银行"
        assert result["result"]["total_return"] == 10

    def test_get_child_job_wrong_user(self, svc, monkeypatch):
        mock_dao = MagicMock()
        mock_dao.get_job_row.return_value = {"user_id": 2}
        monkeypatch.setattr(_bs, "BacktestHistoryDao", lambda: mock_dao)
        assert svc._get_child_job_from_db("bt_123", user_id=1) is None

    def test_get_child_job_not_found(self, svc, monkeypatch):
        mock_dao = MagicMock()
        mock_dao.get_job_row.return_value = None
        monkeypatch.setattr(_bs, "BacktestHistoryDao", lambda: mock_dao)
        assert svc._get_child_job_from_db("bt_999", user_id=1) is None

    def test_get_child_job_exception(self, svc, monkeypatch):
        mock_dao = MagicMock()
        mock_dao.get_job_row.side_effect = Exception("db error")
        monkeypatch.setattr(_bs, "BacktestHistoryDao", lambda: mock_dao)
        assert svc._get_child_job_from_db("bt_123", user_id=1) is None

    # --- submit_backtest ---
    def test_submit_backtest(self, svc, monkeypatch):
        monkeypatch.setattr(_bs, "get_queue", lambda n: MagicMock())
        mock_ms = MagicMock()
        mock_ms.resolve_symbol_name.return_value = "平安银行"
        monkeypatch.setattr(_bs, "MarketService", lambda: mock_ms)

        job_id = svc.submit_backtest(
            user_id=1, strategy_id=None, version_id=None, source=None,
            strategy_class_name="TripleMAStrategy",
            symbol="000001.SZ", start_date=date(2024, 1, 1), end_date=date(2024, 12, 31),
        )
        assert job_id.startswith("bt_")
        svc.job_storage.save_job_metadata.assert_called_once()

    def test_submit_backtest_with_strategy_id(self, svc, monkeypatch):
        monkeypatch.setattr(_bs, "get_queue", lambda n: MagicMock())
        mock_ms = MagicMock()
        mock_ms.resolve_symbol_name.return_value = ""
        monkeypatch.setattr(_bs, "MarketService", lambda: mock_ms)
        mock_dao = MagicMock()
        mock_dao.get_strategy_source_for_user.return_value = ("code", "MyStrat", "v1")
        monkeypatch.setattr(_bs, "StrategySourceDao", lambda: mock_dao)

        job_id = svc.submit_backtest(
            user_id=1, strategy_id=42, version_id=None, source=None,
            strategy_class_name=None,
            symbol="000001.SZ", start_date=date(2024, 1, 1), end_date=date(2024, 12, 31),
        )
        assert job_id.startswith("bt_")

    # --- submit_batch_backtest ---
    def test_submit_batch_backtest(self, svc, monkeypatch):
        monkeypatch.setattr(_bs, "get_queue", lambda n: MagicMock())
        mock_dao = MagicMock()
        monkeypatch.setattr(_bs, "BulkBacktestDao", lambda: mock_dao)

        job_id = svc.submit_batch_backtest(
            user_id=1, strategy_id=None, version_id=None, source=None,
            strategy_class_name="TripleMAStrategy",
            symbols=["000001.SZ", "600000.SH"],
            start_date=date(2024, 1, 1), end_date=date(2024, 12, 31),
        )
        assert job_id.startswith("bulk_")
        mock_dao.insert_parent.assert_called_once()

    def test_submit_batch_bulk_dao_error(self, svc, monkeypatch):
        monkeypatch.setattr(_bs, "get_queue", lambda n: MagicMock())
        mock_dao = MagicMock()
        mock_dao.insert_parent.side_effect = Exception("db error")
        monkeypatch.setattr(_bs, "BulkBacktestDao", lambda: mock_dao)

        # Should not raise — logs exception internally
        job_id = svc.submit_batch_backtest(
            user_id=1, strategy_id=None, version_id=None, source=None,
            strategy_class_name="TripleMAStrategy",
            symbols=["000001.SZ"], start_date=date(2024, 1, 1), end_date=date(2024, 12, 31),
        )
        assert job_id.startswith("bulk_")

    # --- submit_optimization ---
    def test_submit_optimization(self, svc, monkeypatch):
        monkeypatch.setattr(_bs, "get_queue", lambda n: MagicMock())

        job_id = svc.submit_optimization(
            user_id=1, strategy_id=None, strategy_class_name="TripleMAStrategy",
            symbol="000001.SZ", start_date=date(2024, 1, 1), end_date=date(2024, 12, 31),
            optimization_settings={"param_space": {"fast": {"min": 5, "max": 20, "step": 5}}},
        )
        assert job_id.startswith("opt_")
        svc.job_storage.save_job_metadata.assert_called_once()


# =====================================================================
# BacktestService (extends V2)
# =====================================================================

@pytest.mark.unit
class TestBacktestService:
    def test_load_builtin_strategies(self, monkeypatch):
        monkeypatch.setattr(_bs, "get_job_storage", lambda: MagicMock())
        svc = _bs.BacktestService()
        # Should have loaded at least some strategies (or empty if import fails)
        assert isinstance(svc.builtin_strategies, dict)

    def test_get_strategy_class_builtin(self, monkeypatch):
        monkeypatch.setattr(_bs, "get_job_storage", lambda: MagicMock())
        svc = _bs.BacktestService()
        if svc.builtin_strategies:
            name = next(iter(svc.builtin_strategies))
            cls = svc._get_strategy_class(strategy_class=name)
            assert cls is not None

    def test_get_strategy_class_not_found(self, monkeypatch):
        monkeypatch.setattr(_bs, "get_job_storage", lambda: MagicMock())
        svc = _bs.BacktestService()
        with pytest.raises(ValueError, match="Strategy not found"):
            svc._get_strategy_class(strategy_id=None, strategy_class="NonExistent")

    def test_get_strategy_class_by_id_no_user(self, monkeypatch):
        monkeypatch.setattr(_bs, "get_job_storage", lambda: MagicMock())
        svc = _bs.BacktestService()
        with pytest.raises(ValueError, match="user_id is required"):
            svc._get_strategy_class(strategy_id=1, strategy_class=None)


# =====================================================================
# Singleton accessors
# =====================================================================

@pytest.mark.unit
class TestSingletons:
    def test_get_backtest_service(self, monkeypatch):
        monkeypatch.setattr(_bs, "_backtest_service", None)
        monkeypatch.setattr(_bs, "get_job_storage", lambda: MagicMock())
        svc = _bs.get_backtest_service()
        assert isinstance(svc, _bs.BacktestService)

    def test_get_backtest_service_v2(self, monkeypatch):
        monkeypatch.setattr(_bs, "_backtest_service", None)
        monkeypatch.setattr(_bs, "get_job_storage", lambda: MagicMock())
        svc = _bs.get_backtest_service_v2()
        assert isinstance(svc, _bs.BacktestService)

    def test_singleton_reuse(self, monkeypatch):
        monkeypatch.setattr(_bs, "_backtest_service", None)
        monkeypatch.setattr(_bs, "get_job_storage", lambda: MagicMock())
        svc1 = _bs.get_backtest_service()
        svc2 = _bs.get_backtest_service()
        assert svc1 is svc2
