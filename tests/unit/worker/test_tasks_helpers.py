"""Unit tests for pure helper functions in app.worker.service.tasks."""

from __future__ import annotations

import sys
from unittest.mock import MagicMock, patch

import numpy as np
import pytest

_MOD = "app.worker.service.tasks"


# ── stub out heavy vnpy imports ──────────────────────────────────

@pytest.fixture(autouse=True)
def _stub_vnpy():
    stubs = {}
    for name in [
        "vnpy.trader.constant",
        "vnpy.trader.object",
        "vnpy.trader.engine",
        "vnpy_ctastrategy",
        "vnpy_ctastrategy.backtesting",
        "vnpy_ctastrategy.engine",
        "vnpy_ctastrategy.template",
        "rq",
        "rq.job",
    ]:
        if name not in sys.modules:
            stubs[name] = sys.modules[name] = MagicMock()
    yield
    for name in stubs:
        sys.modules.pop(name, None)


# ── convert_to_vnpy_symbol ───────────────────────────────────────

class TestConvertToVnpySymbol:
    def test_sz(self):
        from app.worker.service.tasks import convert_to_vnpy_symbol
        assert convert_to_vnpy_symbol("000001.SZ") == "000001.SZSE"

    def test_sh(self):
        from app.worker.service.tasks import convert_to_vnpy_symbol
        assert convert_to_vnpy_symbol("600000.SH") == "600000.SSE"

    def test_bj(self):
        from app.worker.service.tasks import convert_to_vnpy_symbol
        assert convert_to_vnpy_symbol("830001.BJ") == "830001.BSE"

    def test_already_mapped(self):
        from app.worker.service.tasks import convert_to_vnpy_symbol
        assert convert_to_vnpy_symbol("000001.SZSE") == "000001.SZSE"

    def test_unknown_suffix(self):
        from app.worker.service.tasks import convert_to_vnpy_symbol
        result = convert_to_vnpy_symbol("AAPL.US")
        assert "AAPL" in result


# ── convert_to_tushare_symbol ────────────────────────────────────

class TestConvertToTushareSymbol:
    def test_szse(self):
        from app.worker.service.tasks import convert_to_tushare_symbol
        assert convert_to_tushare_symbol("000001.SZSE") == "000001.SZ"

    def test_sse(self):
        from app.worker.service.tasks import convert_to_tushare_symbol
        assert convert_to_tushare_symbol("600000.SSE") == "600000.SH"

    def test_bse(self):
        from app.worker.service.tasks import convert_to_tushare_symbol
        assert convert_to_tushare_symbol("830001.BSE") == "830001.BJ"

    def test_already_tushare(self):
        from app.worker.service.tasks import convert_to_tushare_symbol
        assert convert_to_tushare_symbol("000001.SZ") == "000001.SZ"


# ── calculate_alpha_beta_for_worker ──────────────────────────────

class TestCalculateAlphaBeta:
    def test_basic(self):
        from app.worker.service.tasks import calculate_alpha_beta_for_worker
        strat = np.array([0.01, 0.02, -0.01, 0.03, 0.01])
        bench = np.array([0.005, 0.01, -0.005, 0.015, 0.005])
        alpha, beta = calculate_alpha_beta_for_worker(strat, bench)
        assert alpha is not None
        assert beta is not None

    def test_all_nan(self):
        from app.worker.service.tasks import calculate_alpha_beta_for_worker
        strat = np.array([np.nan, np.nan])
        bench = np.array([np.nan, np.nan])
        alpha, beta = calculate_alpha_beta_for_worker(strat, bench)
        assert alpha is None
        assert beta is None

    def test_empty(self):
        from app.worker.service.tasks import calculate_alpha_beta_for_worker
        strat = np.array([])
        bench = np.array([])
        alpha, beta = calculate_alpha_beta_for_worker(strat, bench)
        assert alpha is None
        assert beta is None

    def test_mismatched_length(self):
        from app.worker.service.tasks import calculate_alpha_beta_for_worker
        strat = np.array([0.01, 0.02, -0.01, 0.03, 0.01])
        bench = np.array([0.005, 0.01, -0.005])
        alpha, beta = calculate_alpha_beta_for_worker(strat, bench)
        # Should truncate to min length and still work
        assert alpha is not None


# ── _normalize_optimization_results ──────────────────────────────

class TestNormalizeOptimizationResults:
    def test_normal(self):
        from app.worker.service.tasks import _normalize_optimization_results
        raw = [
            ({"fast_window": 5}, 1.5, {"sharpe_ratio": 1.5, "total_return": 0.2}),
            ({"fast_window": 10}, 1.0, {"sharpe_ratio": 1.0, "total_return": 0.1}),
        ]
        result = _normalize_optimization_results(raw, "sharpe_ratio")
        assert len(result) == 2
        assert result[0]["rank_order"] == 1

    def test_empty(self):
        from app.worker.service.tasks import _normalize_optimization_results
        assert _normalize_optimization_results([], "sharpe_ratio") == []

    def test_non_dict_stats(self):
        from app.worker.service.tasks import _normalize_optimization_results
        raw = [({"x": 1}, 1.0, "not_a_dict")]
        result = _normalize_optimization_results(raw, "sharpe_ratio")
        assert len(result) == 1

    def test_short_tuple_skipped(self):
        from app.worker.service.tasks import _normalize_optimization_results
        raw = [("only_one",)]
        result = _normalize_optimization_results(raw, "sharpe_ratio")
        assert len(result) == 0


# ── _build_optimization_setting ──────────────────────────────────

class TestBuildOptimizationSetting:
    def test_builds(self):
        from app.worker.service.tasks import _build_optimization_setting
        space = {
            "fast_window": {"min": 5, "max": 20, "step": 5},
            "slow_window": {"min": 20, "max": 60, "step": 10},
        }
        setting = _build_optimization_setting(space, "sharpe_ratio")
        assert setting is not None

    def test_invalid_param_skipped(self):
        from app.worker.service.tasks import _build_optimization_setting
        space = {
            "fast_window": {"min": 5, "max": 20, "step": 5},
            "bad": "not_a_dict",
        }
        setting = _build_optimization_setting(space, "sharpe_ratio")
        assert setting is not None


# ── resolve_symbol_name ──────────────────────────────────────────

class TestResolveSymbolName:
    def test_resolves(self):
        from app.worker.service.tasks import resolve_symbol_name
        with patch(f"{_MOD}.MarketService") as ms_cls:
            ms = ms_cls.return_value
            ms.resolve_symbol_name.return_value = "平安银行"
            result = resolve_symbol_name("000001.SZ")
        assert result == "平安银行"

    def test_fallback(self):
        from app.worker.service.tasks import resolve_symbol_name
        with patch(f"{_MOD}.MarketService") as ms_cls:
            ms = ms_cls.return_value
            ms.resolve_symbol_name.side_effect = Exception("DB error")
            result = resolve_symbol_name("000001.SZ")
        assert result == ""


# ── save_backtest_to_db ──────────────────────────────────────────

class TestSaveBacktestToDb:
    def test_saves(self):
        from app.worker.service.tasks import save_backtest_to_db
        with patch(f"{_MOD}.BacktestHistoryDao") as dao_cls:
            dao = dao_cls.return_value
            save_backtest_to_db(
                "job-1", 1, 10, "MyStrategy", "000001.SZ",
                "2024-01-01", "2024-06-01", {}, "completed",
                {"total_return": 0.1}
            )
        dao.upsert_history.assert_called_once()


# ── get_benchmark_data_for_worker ────────────────────────────────

class TestGetBenchmarkData:
    def test_returns_data(self):
        from app.worker.service.tasks import get_benchmark_data_for_worker
        mock_data = {"dates": ["2024-01-01"], "close": [100.0]}
        with patch(f"{_MOD}.AkshareBenchmarkDao") as dao_cls:
            dao_cls.return_value.get_benchmark_data.return_value = mock_data
            result = get_benchmark_data_for_worker("2024-01-01", "2024-06-01")
        assert result == mock_data

    def test_returns_none_on_error(self):
        from app.worker.service.tasks import get_benchmark_data_for_worker
        with patch(f"{_MOD}.AkshareBenchmarkDao") as dao_cls:
            dao_cls.return_value.get_benchmark_data.side_effect = Exception("fail")
            result = get_benchmark_data_for_worker("2024-01-01", "2024-06-01")
        assert result is None


# ── _resolve_optimization_context ────────────────────────────────

class TestResolveOptimizationContext:
    def test_with_history(self):
        from app.worker.service.tasks import _resolve_optimization_context
        mock_history = {"vt_symbol": "000002.SZ", "start_date": "2023-06-01", "end_date": "2024-01-01"}
        with patch(f"{_MOD}.BacktestHistoryDao") as dao_cls:
            dao_cls.return_value.get_latest_strategy_run.return_value = mock_history
            sym, start, end = _resolve_optimization_context(1, 10)
        assert sym == "000002.SZ"
        assert start == "2023-06-01"

    def test_without_history(self):
        from app.worker.service.tasks import _resolve_optimization_context
        with patch(f"{_MOD}.BacktestHistoryDao") as dao_cls:
            dao_cls.return_value.get_latest_strategy_run.return_value = None
            sym, start, end = _resolve_optimization_context(1, 10)
        assert sym == "000001.SZ"


# ── run_datasync_task ────────────────────────────────────────────

class TestRunDatasyncTask:
    def test_calls_scheduler(self):
        from app.worker.service.tasks import run_datasync_task
        with patch("app.datasync.scheduler.run_daily_sync") as mock_sync:
            mock_sync.return_value = {"step1": {"status": "success"}}
            result = run_datasync_task("2024-01-05")
        mock_sync.assert_called_once()
        assert result["status"] == "ok"
        assert "results" in result
