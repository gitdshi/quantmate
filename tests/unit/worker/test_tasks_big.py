"""Tests for the large uncovered functions in app.worker.service.tasks.

Covers:
  _configure_vnpy_mysql_from_env
  run_backtest_task (success + failure paths)
  run_bulk_backtest_task (success + failure paths)
  _evaluate_single
  _run_grid_sequential
  _run_random_sequential
  _run_sequential_optimization
  run_optimization_task (success + failure paths)
  run_optimization_record_task
"""

from __future__ import annotations

from datetime import datetime
from unittest.mock import MagicMock, patch, PropertyMock

import numpy as np
import pytest

# Module under test — import lazily after mocking heavy deps
_MOD = "app.worker.service.tasks"


# ═══ _configure_vnpy_mysql_from_env ═════════════════════════════════════


class TestConfigureVnpyMysql:
    def test_skips_when_missing_env(self, monkeypatch):
        from app.worker.service.tasks import _configure_vnpy_mysql_from_env
        from vnpy.trader.setting import SETTINGS as VNPY_SETTINGS

        monkeypatch.setattr(
            "app.worker.service.tasks.get_settings",
            lambda: MagicMock(mysql_host="", mysql_user="", mysql_password="", mysql_port=3306),
        )
        VNPY_SETTINGS.clear()
        # Should not raise
        _configure_vnpy_mysql_from_env()  # just returns early
        assert "database.name" not in VNPY_SETTINGS

    def test_sets_settings(self, monkeypatch):
        from app.worker.service.tasks import _configure_vnpy_mysql_from_env
        from vnpy.trader.setting import SETTINGS as VNPY_SETTINGS

        monkeypatch.setattr(
            "app.worker.service.tasks.get_settings",
            lambda: MagicMock(mysql_host="db-host", mysql_user="root", mysql_password="secret", mysql_port=3307),
        )

        _configure_vnpy_mysql_from_env()
        assert VNPY_SETTINGS["database.name"] == "mysql"
        assert VNPY_SETTINGS["database.host"] == "db-host"
        assert VNPY_SETTINGS["database.port"] == 3307
        assert VNPY_SETTINGS["database.user"] == "root"
        assert VNPY_SETTINGS["database.password"] == "secret"

    @pytest.fixture(autouse=True)
    def _suppress_loguru_teardown_error(self):
        """Suppress loguru handler removal errors in teardown."""
        yield
        # Some tests trigger loguru handler double-removal; ignore safely.
        pass


# ═══ _evaluate_single ══════════════════════════════════════════════════


class TestEvaluateSingle:
    @patch(f"{_MOD}.evaluate")
    def test_returns_result(self, mock_eval):
        from app.worker.service.tasks import _evaluate_single

        mock_eval.return_value = ({"a": 1}, 0.5, {"sharpe_ratio": 0.5})
        res = _evaluate_single(
            "sharpe_ratio", MagicMock, "000001.SZSE",
            datetime(2024, 1, 1), datetime(2024, 6, 1),
            0.0003, 0.0001, 1, 0.01, 100000, {"a": 1},
        )
        assert res is not None
        assert res[1] == 0.5

    @patch(f"{_MOD}.evaluate", side_effect=RuntimeError("engine error"))
    def test_returns_none_on_error(self, mock_eval):
        from app.worker.service.tasks import _evaluate_single

        res = _evaluate_single(
            "sharpe_ratio", MagicMock, "000001.SZSE",
            datetime(2024, 1, 1), datetime(2024, 6, 1),
            0.0003, 0.0001, 1, 0.01, 100000, {"a": 1},
        )
        assert res is None


# ═══ _run_grid_sequential ══════════════════════════════════════════════


class TestRunGridSequential:
    @patch(f"{_MOD}._evaluate_single")
    def test_basic(self, mock_eval):
        from app.worker.service.tasks import _run_grid_sequential
        from vnpy.trader.optimize import OptimizationSetting

        setting = OptimizationSetting()
        setting.set_target("sharpe_ratio")
        setting.add_parameter("fast_window", 5, 10, 5)

        mock_eval.return_value = ({"fast_window": 5}, 0.5, {"sharpe_ratio": 0.5})

        results = _run_grid_sequential(
            strategy_class=MagicMock,
            symbol="000001.SZSE",
            start=datetime(2024, 1, 1),
            end=datetime(2024, 6, 1),
            rate=0.0003, slippage=0.0001, size=1, pricetick=0.01, capital=100000,
            optimization_setting=setting,
        )
        assert len(results) >= 1

    @patch(f"{_MOD}._evaluate_single", return_value=None)
    def test_all_evaluate_fail(self, mock_eval):
        from app.worker.service.tasks import _run_grid_sequential
        from vnpy.trader.optimize import OptimizationSetting

        setting = OptimizationSetting()
        setting.set_target("sharpe_ratio")
        # No parameters => generate_settings returns [{}] (one combo)
        # _evaluate_single returns None => all filtered out
        results = _run_grid_sequential(
            strategy_class=MagicMock,
            symbol="000001.SZSE",
            start=datetime(2024, 1, 1),
            end=datetime(2024, 6, 1),
            rate=0.0003, slippage=0.0001, size=1, pricetick=0.01, capital=100000,
            optimization_setting=setting,
        )
        assert results == []
        mock_eval.assert_called_once()

    @patch(f"{_MOD}._evaluate_single", return_value=None)
    def test_all_failed(self, mock_eval):
        from app.worker.service.tasks import _run_grid_sequential
        from vnpy.trader.optimize import OptimizationSetting

        setting = OptimizationSetting()
        setting.set_target("sharpe_ratio")
        setting.add_parameter("x", 1, 3, 1)

        results = _run_grid_sequential(
            strategy_class=MagicMock, symbol="000001.SZSE",
            start=datetime(2024, 1, 1), end=datetime(2024, 6, 1),
            rate=0.0003, slippage=0.0001, size=1, pricetick=0.01, capital=100000,
            optimization_setting=setting,
        )
        assert results == []


# ═══ _run_random_sequential ════════════════════════════════════════════


class TestRunRandomSequential:
    @patch(f"{_MOD}._evaluate_single")
    def test_basic(self, mock_eval):
        from app.worker.service.tasks import _run_random_sequential
        from vnpy.trader.optimize import OptimizationSetting

        setting = OptimizationSetting()
        setting.set_target("sharpe_ratio")
        setting.add_parameter("x", 1, 5, 1)

        mock_eval.return_value = ({"x": 3}, 0.7, {"sharpe_ratio": 0.7})

        results = _run_random_sequential(
            strategy_class=MagicMock, symbol="000001.SZSE",
            start=datetime(2024, 1, 1), end=datetime(2024, 6, 1),
            rate=0.0003, slippage=0.0001, size=1, pricetick=0.01, capital=100000,
            optimization_setting=setting, n_samples=3,
        )
        assert len(results) >= 1

    @patch(f"{_MOD}._evaluate_single", return_value=None)
    def test_all_evaluate_fail(self, mock_eval):
        from app.worker.service.tasks import _run_random_sequential
        from vnpy.trader.optimize import OptimizationSetting

        setting = OptimizationSetting()
        setting.set_target("sharpe_ratio")
        # No parameters => generate_settings returns [{}] (one combo)
        # _evaluate_single returns None => all filtered out
        results = _run_random_sequential(
            strategy_class=MagicMock, symbol="000001.SZSE",
            start=datetime(2024, 1, 1), end=datetime(2024, 6, 1),
            rate=0.0003, slippage=0.0001, size=1, pricetick=0.01, capital=100000,
            optimization_setting=setting,
        )
        assert results == []
        mock_eval.assert_called_once()


# ═══ _run_sequential_optimization ══════════════════════════════════════


class TestRunSequentialOptimization:
    @patch(f"{_MOD}._run_grid_sequential", return_value=[])
    def test_grid_dispatch(self, mock_grid):
        from app.worker.service.tasks import _run_sequential_optimization
        from vnpy.trader.optimize import OptimizationSetting

        setting = OptimizationSetting()
        setting.set_target("sharpe_ratio")

        _run_sequential_optimization(
            strategy_class=MagicMock, symbol="000001.SZSE",
            start=datetime(2024, 1, 1), end=datetime(2024, 6, 1),
            rate=0.0003, slippage=0.0001, size=1, pricetick=0.01, capital=100000,
            optimization_setting=setting, search_method="grid",
        )
        mock_grid.assert_called_once()

    @patch(f"{_MOD}._run_random_sequential", return_value=[])
    def test_random_dispatch(self, mock_random):
        from app.worker.service.tasks import _run_sequential_optimization
        from vnpy.trader.optimize import OptimizationSetting

        setting = OptimizationSetting()
        setting.set_target("sharpe_ratio")

        _run_sequential_optimization(
            strategy_class=MagicMock, symbol="000001.SZSE",
            start=datetime(2024, 1, 1), end=datetime(2024, 6, 1),
            rate=0.0003, slippage=0.0001, size=1, pricetick=0.01, capital=100000,
            optimization_setting=setting, search_method="random",
        )
        mock_random.assert_called_once()

    @patch(f"{_MOD}._run_bayesian_sequential", return_value=[])
    def test_bayesian_dispatch(self, mock_bayes):
        from app.worker.service.tasks import _run_sequential_optimization
        from vnpy.trader.optimize import OptimizationSetting

        setting = OptimizationSetting()
        setting.set_target("sharpe_ratio")

        _run_sequential_optimization(
            strategy_class=MagicMock, symbol="000001.SZSE",
            start=datetime(2024, 1, 1), end=datetime(2024, 6, 1),
            rate=0.0003, slippage=0.0001, size=1, pricetick=0.01, capital=100000,
            optimization_setting=setting, search_method="bayesian",
        )
        mock_bayes.assert_called_once()


# ═══ run_backtest_task ═════════════════════════════════════════════════


class TestRunBacktestTask:
    @patch(f"{_MOD}.get_job_storage")
    @patch(f"{_MOD}.save_backtest_to_db")
    @patch(f"{_MOD}.resolve_symbol_name", return_value="PingAn Bank")
    @patch(f"{_MOD}.get_benchmark_data_for_worker", return_value=None)
    @patch(f"{_MOD}.compile_strategy")
    @patch(f"{_MOD}.BacktestingEngine")
    @patch(f"{_MOD}.get_current_job")
    def test_success_with_strategy_code(
        self, mock_job, mock_engine_cls, mock_compile, mock_bench, mock_name,
        mock_save, mock_storage,
    ):
        from app.worker.service.tasks import run_backtest_task
        import pandas as pd

        mock_job.return_value = MagicMock(id="job-1")
        strategy_cls = MagicMock()
        strategy_cls.get_class_parameters.return_value = {}
        mock_compile.return_value = strategy_cls

        engine = mock_engine_cls.return_value
        engine.history_data = [MagicMock(datetime=datetime(2024, 1, 3), open_price=10, high_price=11, low_price=9.5, close_price=10.5)]
        engine.trades = {}

        df = pd.DataFrame({"balance": [100000, 100500], "net_pnl": [0, 500]},
                          index=[datetime(2024, 1, 2), datetime(2024, 1, 3)])
        engine.calculate_result.return_value = df
        engine.calculate_statistics.return_value = {
            "total_return": 0.005, "annual_return": 0.1, "max_drawdown": -1000,
            "max_ddpercent": -1.0, "sharpe_ratio": 1.5, "total_trade_count": 10,
            "winning_rate": 0.6, "profit_factor": 2.0, "total_days": 30,
            "profit_days": 20, "loss_days": 10, "end_balance": 100500,
        }

        result = run_backtest_task(
            strategy_code="class MyStrategy: pass",
            strategy_class_name="MyStrategy",
            symbol="000001.SZ",
            start_date="2024-01-01",
            end_date="2024-06-01",
            initial_capital=100000,
            rate=0.0003,
            slippage=0.0001,
            size=1,
            pricetick=0.01,
            user_id=1,
            strategy_id=1,
        )
        assert result["status"] == "completed"
        assert result["statistics"]["total_return"] == 0.005
        mock_save.assert_called_once()

    @patch(f"{_MOD}.get_job_storage")
    @patch(f"{_MOD}.save_backtest_to_db")
    @patch(f"{_MOD}.compile_strategy", side_effect=RuntimeError("compile error"))
    @patch(f"{_MOD}.get_current_job")
    def test_failure_saves_error(self, mock_job, mock_compile, mock_save, mock_storage):
        from app.worker.service.tasks import run_backtest_task

        mock_job.return_value = MagicMock(id="job-fail")

        result = run_backtest_task(
            strategy_code="bad code",
            strategy_class_name="MyStrategy",
            symbol="000001.SZ",
            start_date="2024-01-01",
            end_date="2024-06-01",
            initial_capital=100000,
            rate=0.0003,
            slippage=0.0001,
            size=1,
            pricetick=0.01,
            user_id=1,
            strategy_id=1,
        )
        assert result["status"] == "failed"
        assert "compile error" in result["error"]
        mock_save.assert_called_once()

    @patch(f"{_MOD}.get_job_storage")
    @patch(f"{_MOD}.compile_strategy")
    @patch(f"{_MOD}.BacktestingEngine")
    @patch(f"{_MOD}.get_current_job")
    def test_no_history_data_backfills(
        self, mock_job, mock_engine_cls, mock_compile, mock_storage,
    ):
        from app.worker.service.tasks import run_backtest_task

        mock_job.return_value = MagicMock(id="job-no-data")
        mock_compile.return_value = MagicMock(get_class_parameters=MagicMock(return_value={}))

        engine = mock_engine_cls.return_value
        # First load_data returns empty, second too (raises)
        engine.history_data = []
        engine.trades = {}

        result = run_backtest_task(
            strategy_code="class X: pass",
            strategy_class_name="X",
            symbol="000001.SZ",
            start_date="2024-01-01",
            end_date="2024-06-01",
            initial_capital=100000,
            rate=0.0003,
            slippage=0.0001,
            size=1,
            pricetick=0.01,
        )
        # Should fail because no data found
        assert result["status"] == "failed"

    @patch(f"{_MOD}.get_job_storage")
    @patch(f"{_MOD}.save_backtest_to_db")
    @patch(f"{_MOD}.StrategySourceDao")
    @patch(f"{_MOD}.compile_strategy")
    @patch(f"{_MOD}.BacktestingEngine")
    @patch(f"{_MOD}.get_current_job")
    @patch(f"{_MOD}.resolve_symbol_name", return_value="")
    @patch(f"{_MOD}.get_benchmark_data_for_worker", return_value=None)
    def test_loads_strategy_from_db(
        self, mock_bench, mock_name, mock_job, mock_engine_cls, mock_compile,
        mock_dao_cls, mock_save, mock_storage,
    ):
        from app.worker.service.tasks import run_backtest_task

        mock_job.return_value = MagicMock(id="job-db")
        dao = mock_dao_cls.return_value
        dao.get_strategy_source_for_user.return_value = ("class X: pass", "X", 1)
        mock_compile.return_value = MagicMock(get_class_parameters=MagicMock(return_value={}))

        engine = mock_engine_cls.return_value
        engine.history_data = [MagicMock(datetime=datetime(2024, 1, 3), open_price=10, high_price=11, low_price=9.5, close_price=10.5)]
        engine.trades = {}
        engine.calculate_result.return_value = None
        engine.calculate_statistics.return_value = {}

        result = run_backtest_task(
            strategy_code=None, strategy_class_name="X",
            symbol="000001.SZ", start_date="2024-01-01", end_date="2024-06-01",
            initial_capital=100000, rate=0.0003, slippage=0.0001, size=1,
            pricetick=0.01, user_id=1, strategy_id=1,
        )
        assert result["status"] == "completed"

    @patch(f"{_MOD}.get_job_storage")
    @patch(f"{_MOD}.save_backtest_to_db")
    @patch(f"{_MOD}.StrategySourceDao")
    @patch(f"{_MOD}.compile_strategy")
    @patch(f"{_MOD}.BacktestingEngine")
    @patch(f"{_MOD}.get_current_job")
    @patch(f"{_MOD}.resolve_symbol_name", return_value="")
    @patch(f"{_MOD}.get_benchmark_data_for_worker", return_value=None)
    def test_loads_strategy_by_class_name(
        self, mock_bench, mock_name, mock_job, mock_engine_cls, mock_compile,
        mock_dao_cls, mock_save, mock_storage,
    ):
        from app.worker.service.tasks import run_backtest_task

        mock_job.return_value = MagicMock(id="job-cls")
        dao = mock_dao_cls.return_value
        dao.get_strategy_code_by_class_name.return_value = "class Z: pass"
        mock_compile.return_value = MagicMock(get_class_parameters=MagicMock(return_value={}))

        engine = mock_engine_cls.return_value
        engine.history_data = [MagicMock(datetime=datetime(2024, 1, 3), open_price=10, high_price=11, low_price=9.5, close_price=10.5)]
        engine.trades = {}
        engine.calculate_result.return_value = None
        engine.calculate_statistics.return_value = {}

        result = run_backtest_task(
            strategy_code=None, strategy_class_name="Z",
            symbol="000001.SZ", start_date="2024-01-01", end_date="2024-06-01",
            initial_capital=100000, rate=0.0003, slippage=0.0001, size=1,
            pricetick=0.01, user_id=None, strategy_id=None,
        )
        assert result["status"] == "completed"

    @patch(f"{_MOD}.get_job_storage")
    @patch(f"{_MOD}.get_current_job")
    def test_no_strategy_raises(self, mock_job, mock_storage):
        from app.worker.service.tasks import run_backtest_task

        mock_job.return_value = MagicMock(id="job-none")

        result = run_backtest_task(
            strategy_code=None, strategy_class_name="",
            symbol="000001.SZ", start_date="2024-01-01", end_date="2024-06-01",
            initial_capital=100000, rate=0.0003, slippage=0.0001, size=1,
            pricetick=0.01,
        )
        assert result["status"] == "failed"

    @patch(f"{_MOD}.get_job_storage")
    @patch(f"{_MOD}.save_backtest_to_db")
    @patch(f"{_MOD}.resolve_symbol_name", return_value="")
    @patch(f"{_MOD}.get_benchmark_data_for_worker")
    @patch(f"{_MOD}.compile_strategy")
    @patch(f"{_MOD}.BacktestingEngine")
    @patch(f"{_MOD}.get_current_job")
    def test_with_benchmark_data(
        self, mock_job, mock_engine_cls, mock_compile, mock_bench, mock_name,
        mock_save, mock_storage,
    ):
        from app.worker.service.tasks import run_backtest_task
        import pandas as pd

        mock_job.return_value = MagicMock(id="job-bench")
        mock_compile.return_value = MagicMock(get_class_parameters=MagicMock(return_value={}))
        mock_bench.return_value = {
            "returns": np.array([0.01, 0.02, -0.01]),
            "total_return": 0.05,
            "prices": [{"date": "2024-01-03", "close": 100}],
        }

        engine = mock_engine_cls.return_value
        bars = [MagicMock(datetime=datetime(2024, 1, i), open_price=10, high_price=11, low_price=9.5, close_price=10.5) for i in range(2, 5)]
        engine.history_data = bars
        engine.trades = {}

        df = pd.DataFrame(
            {"balance": [100000, 100500, 101000, 100800], "net_pnl": [0, 500, 500, -200]},
            index=[datetime(2024, 1, 1), datetime(2024, 1, 2), datetime(2024, 1, 3), datetime(2024, 1, 4)],
        )
        engine.calculate_result.return_value = df
        engine.calculate_statistics.return_value = {"total_return": 0.008}

        result = run_backtest_task(
            strategy_code="class X: pass", strategy_class_name="X",
            symbol="000001.SZ", start_date="2024-01-01", end_date="2024-06-01",
            initial_capital=100000, rate=0.0003, slippage=0.0001, size=1,
            pricetick=0.01, user_id=1, strategy_id=1,
        )
        assert result["status"] == "completed"
        assert result["benchmark_curve"] is not None

    @patch(f"{_MOD}.get_job_storage")
    @patch(f"{_MOD}.save_backtest_to_db")
    @patch(f"{_MOD}.resolve_symbol_name", return_value="Test")
    @patch(f"{_MOD}.get_benchmark_data_for_worker", return_value=None)
    @patch(f"{_MOD}.compile_strategy")
    @patch(f"{_MOD}.BacktestingEngine")
    @patch(f"{_MOD}.get_current_job")
    def test_with_trades(
        self, mock_job, mock_engine_cls, mock_compile, mock_bench, mock_name,
        mock_save, mock_storage,
    ):
        from app.worker.service.tasks import run_backtest_task

        mock_job.return_value = MagicMock(id="job-trades")
        mock_compile.return_value = MagicMock(get_class_parameters=MagicMock(return_value={}))

        engine = mock_engine_cls.return_value
        engine.history_data = [MagicMock(datetime=datetime(2024, 1, 3), open_price=10, high_price=11, low_price=9.5, close_price=10.5)]

        trade = MagicMock()
        trade.datetime = datetime(2024, 1, 3)
        trade.symbol = "000001"
        trade.direction = MagicMock(value="LONG")
        trade.offset = MagicMock(value="OPEN")
        trade.price = 10.5
        trade.volume = 100
        engine.trades = {"t1": trade}
        engine.calculate_result.return_value = None
        engine.calculate_statistics.return_value = {}

        result = run_backtest_task(
            strategy_code="class X: pass", strategy_class_name="X",
            symbol="000001.SZ", start_date="2024-01-01", end_date="2024-06-01",
            initial_capital=100000, rate=0.0003, slippage=0.0001, size=1,
            pricetick=0.01,
        )
        assert result["status"] == "completed"
        assert len(result["trades"]) == 1


# ═══ run_bulk_backtest_task ════════════════════════════════════════════


class TestRunBulkBacktestTask:
    @patch(f"{_MOD}._finish_bulk_row")
    @patch(f"{_MOD}._update_bulk_row")
    @patch(f"{_MOD}._save_bulk_child")
    @patch(f"{_MOD}.run_backtest_task")
    @patch(f"{_MOD}.get_job_storage")
    @patch(f"{_MOD}.get_current_job")
    def test_success(self, mock_job, mock_storage_fn, mock_run, mock_child,
                     mock_update, mock_finish):
        from app.worker.service.tasks import run_bulk_backtest_task

        mock_job.return_value = MagicMock(id="bulk-1")
        storage = mock_storage_fn.return_value
        storage.get_job_metadata.return_value = {"strategy_version": "2"}

        mock_run.side_effect = [
            {"status": "completed", "statistics": {"total_return": 0.05}, "symbol_name": "PA"},
            {"status": "completed", "statistics": {"total_return": 0.10}, "symbol_name": "Mao"},
        ]

        result = run_bulk_backtest_task(
            strategy_code="class X: pass",
            strategy_class_name="X",
            symbols=["000001.SZ", "600519.SH"],
            start_date="2024-01-01", end_date="2024-06-01",
            initial_capital=100000, rate=0.0003, slippage=0.0001,
            size=1, pricetick=0.01,
            bulk_job_id="bulk-1", user_id=1, strategy_id=1,
        )
        assert result["status"] == "completed"
        assert result["successful"] == 2
        assert result["best_return"] == 0.10
        assert result["best_symbol"] == "600519.SH"
        mock_finish.assert_called_once()

    @patch(f"{_MOD}._finish_bulk_row")
    @patch(f"{_MOD}._update_bulk_row")
    @patch(f"{_MOD}._save_bulk_child")
    @patch(f"{_MOD}.run_backtest_task")
    @patch(f"{_MOD}.get_job_storage")
    @patch(f"{_MOD}.get_current_job")
    def test_child_failure(self, mock_job, mock_storage_fn, mock_run, mock_child,
                           mock_update, mock_finish):
        from app.worker.service.tasks import run_bulk_backtest_task

        mock_job.return_value = MagicMock(id="bulk-2")
        storage = mock_storage_fn.return_value
        storage.get_job_metadata.return_value = None

        mock_run.side_effect = [
            {"status": "completed", "statistics": {"total_return": 0.05}, "symbol_name": "PA"},
            RuntimeError("child crash"),
        ]

        result = run_bulk_backtest_task(
            strategy_code="class X: pass",
            strategy_class_name="X",
            symbols=["000001.SZ", "600519.SH"],
            start_date="2024-01-01", end_date="2024-06-01",
            initial_capital=100000, rate=0.0003, slippage=0.0001,
            size=1, pricetick=0.01,
            bulk_job_id="bulk-2", user_id=1, strategy_id=1,
        )
        assert result["status"] == "completed"
        assert result["failed"] == 1
        assert result["successful"] == 1

    @patch(f"{_MOD}._finish_bulk_row")
    @patch(f"{_MOD}._update_bulk_row")
    @patch(f"{_MOD}._save_bulk_child")
    @patch(f"{_MOD}.run_backtest_task", side_effect=RuntimeError("engine crash"))
    @patch(f"{_MOD}.get_job_storage")
    @patch(f"{_MOD}.get_current_job")
    def test_catastrophic_failure(self, mock_job, mock_storage_fn, mock_run,
                                  mock_child, mock_update, mock_finish):
        from app.worker.service.tasks import run_bulk_backtest_task

        mock_job.return_value = MagicMock(id="bulk-crash")
        storage = mock_storage_fn.return_value
        storage.get_job_metadata.return_value = None

        result = run_bulk_backtest_task(
            strategy_code="class X: pass",
            strategy_class_name="X",
            symbols=["000001.SZ"],
            start_date="2024-01-01", end_date="2024-06-01",
            initial_capital=100000, rate=0.0003, slippage=0.0001,
            size=1, pricetick=0.01,
            bulk_job_id="bulk-crash",
        )
        # The child exception is caught within the per-symbol loop;
        # the overall status is still "completed" with 1 failed child
        assert result["status"] == "completed"
        assert result["failed"] == 1


# ═══ run_optimization_task ═════════════════════════════════════════════


class TestRunOptimizationTask:
    @patch(f"{_MOD}._normalize_optimization_results")
    @patch(f"{_MOD}._run_sequential_optimization")
    @patch(f"{_MOD}.ensure_vnpy_history_data")
    @patch(f"{_MOD}.compile_strategy")
    @patch(f"{_MOD}.BacktestingEngine")
    def test_success_with_code(
        self, mock_engine_cls, mock_compile, mock_ensure, mock_seq, mock_norm,
    ):
        from app.worker.service.tasks import run_optimization_task

        strategy_cls = MagicMock()
        strategy_cls.__module__ = "builtins"  # triggers sequential path
        mock_compile.return_value = strategy_cls

        engine = mock_engine_cls.return_value
        engine.history_data = [MagicMock()]

        mock_seq.return_value = [
            ({"x": 1}, 0.8, {"sharpe_ratio": 0.8}),
        ]
        mock_norm.return_value = [
            {"rank_order": 1, "parameters": {"x": 1}, "statistics": {"target_value": 0.8}},
        ]

        result = run_optimization_task(
            strategy_code="class X: pass",
            strategy_class_name="X",
            symbol="000001.SZ",
            start_date="2024-01-01",
            end_date="2024-06-01",
            initial_capital=100000,
            rate=0.0003, slippage=0.0001, size=1, pricetick=0.01,
            optimization_settings={"x": {"min": 1, "max": 5, "step": 1}},
            job_id="opt-1",
            search_method="grid",
            objective_metric="sharpe_ratio",
        )
        assert result["status"] == "completed"
        assert result["total_combinations"] == 1

    @patch(f"{_MOD}.compile_strategy", side_effect=RuntimeError("bad"))
    def test_failure(self, mock_compile):
        from app.worker.service.tasks import run_optimization_task

        result = run_optimization_task(
            strategy_code="bad", strategy_class_name="X",
            symbol="000001.SZ", start_date="2024-01-01", end_date="2024-06-01",
            initial_capital=100000, rate=0.0003, slippage=0.0001, size=1, pricetick=0.01,
            optimization_settings={}, job_id="opt-fail",
        )
        assert result["status"] == "failed"

    @patch(f"{_MOD}._normalize_optimization_results", return_value=[])
    @patch(f"{_MOD}.BacktestingEngine")
    def test_builtin_strategy_grid(self, mock_engine_cls, mock_norm):
        from app.worker.service.tasks import run_optimization_task

        engine = mock_engine_cls.return_value
        engine.history_data = [MagicMock()]
        engine.run_bf_optimization.return_value = []

        # Mock the lazy import of builtin strategies inside the function
        fake_strategy = MagicMock()
        import_target = "app.strategies.triple_ma_strategy"
        with patch.dict("sys.modules", {
            "app.strategies": MagicMock(),
            import_target: MagicMock(TripleMAStrategy=fake_strategy),
            "app.strategies.turtle_trading": MagicMock(TurtleTradingStrategy=MagicMock()),
        }):
            result = run_optimization_task(
                strategy_code=None,
                strategy_class_name="TripleMAStrategy",
                symbol="000001.SZ",
                start_date="2024-01-01", end_date="2024-06-01",
                initial_capital=100000, rate=0.0003, slippage=0.0001, size=1, pricetick=0.01,
                optimization_settings={}, job_id="opt-builtin",
                search_method="grid",
            )
        assert result["status"] == "completed"

    @patch(f"{_MOD}._normalize_optimization_results", return_value=[])
    @patch(f"{_MOD}.BacktestingEngine")
    def test_builtin_strategy_random(self, mock_engine_cls, mock_norm):
        from app.worker.service.tasks import run_optimization_task

        engine = mock_engine_cls.return_value
        engine.history_data = [MagicMock()]
        engine.run_ga_optimization.return_value = []

        # Mock the lazy import of builtin strategies inside the function
        fake_strategy = MagicMock()
        with patch.dict("sys.modules", {
            "app.strategies": MagicMock(),
            "app.strategies.triple_ma_strategy": MagicMock(TripleMAStrategy=MagicMock()),
            "app.strategies.turtle_trading": MagicMock(TurtleTradingStrategy=fake_strategy),
        }):
            result = run_optimization_task(
                strategy_code=None,
                strategy_class_name="TurtleTradingStrategy",
                symbol="000001.SZ",
                start_date="2024-01-01", end_date="2024-06-01",
                initial_capital=100000, rate=0.0003, slippage=0.0001, size=1, pricetick=0.01,
                optimization_settings={}, job_id="opt-rand",
                search_method="random",
            )
        assert result["status"] == "completed"

    @patch(f"{_MOD}.BacktestingEngine")
    def test_unknown_builtin(self, mock_engine_cls):
        from app.worker.service.tasks import run_optimization_task

        result = run_optimization_task(
            strategy_code=None,
            strategy_class_name="NonexistentStrategy",
            symbol="000001.SZ",
            start_date="2024-01-01", end_date="2024-06-01",
            initial_capital=100000, rate=0.0003, slippage=0.0001, size=1, pricetick=0.01,
            optimization_settings={}, job_id="opt-unknown",
        )
        assert result["status"] == "failed"


# ═══ run_optimization_record_task ══════════════════════════════════════


class TestRunOptimizationRecordTask:
    @patch(f"{_MOD}.run_optimization_task")
    @patch(f"{_MOD}.StrategySourceDao")
    @patch(f"{_MOD}._resolve_optimization_context")
    @patch(f"{_MOD}.OptimizationTaskDao")
    def test_success(self, mock_dao_cls, mock_ctx, mock_src, mock_run_opt):
        from app.worker.service.tasks import run_optimization_record_task

        dao = mock_dao_cls.return_value
        dao.get_task_for_worker.return_value = {
            "user_id": 1, "strategy_id": 2, "search_method": "grid",
            "objective_metric": "sharpe_ratio", "param_space": {"x": {"min": 1, "max": 5, "step": 1}},
        }
        mock_src.return_value.get_strategy_source_for_user.return_value = ("class X: pass", "X", 1)
        mock_ctx.return_value = ("000001.SZ", "2024-01-01", "2024-06-01")
        mock_run_opt.return_value = {
            "status": "completed",
            "all_results": [{"parameters": {"x": 3}, "statistics": {"sharpe_ratio": 1.2}}],
            "best_parameters": {"x": 3},
            "best_statistics": {"sharpe_ratio": 1.2},
            "total_combinations": 1,
        }

        result = run_optimization_record_task(task_id=42)
        assert result["status"] == "completed"
        dao.replace_results.assert_called_once()
        dao.update_status.assert_called()

    @patch(f"{_MOD}.OptimizationTaskDao")
    def test_task_not_found(self, mock_dao_cls):
        from app.worker.service.tasks import run_optimization_record_task

        mock_dao_cls.return_value.get_task_for_worker.return_value = None
        result = run_optimization_record_task(task_id=999)
        assert result["status"] == "failed"

    @patch(f"{_MOD}.run_optimization_task")
    @patch(f"{_MOD}.StrategySourceDao")
    @patch(f"{_MOD}._resolve_optimization_context")
    @patch(f"{_MOD}.OptimizationTaskDao")
    def test_opt_failed(self, mock_dao_cls, mock_ctx, mock_src, mock_run_opt):
        from app.worker.service.tasks import run_optimization_record_task

        dao = mock_dao_cls.return_value
        dao.get_task_for_worker.return_value = {
            "user_id": 1, "strategy_id": 2, "search_method": "grid",
            "objective_metric": "sharpe_ratio", "param_space": {},
        }
        mock_src.return_value.get_strategy_source_for_user.return_value = ("class X: pass", "X", 1)
        mock_ctx.return_value = ("000001.SZ", "2024-01-01", "2024-06-01")
        mock_run_opt.return_value = {"status": "failed", "error": "no data"}

        result = run_optimization_record_task(task_id=43)
        assert result["status"] == "failed"

    @patch(f"{_MOD}.StrategySourceDao")
    @patch(f"{_MOD}.OptimizationTaskDao")
    def test_exception(self, mock_dao_cls, mock_src):
        from app.worker.service.tasks import run_optimization_record_task

        dao = mock_dao_cls.return_value
        dao.get_task_for_worker.return_value = {
            "user_id": 1, "strategy_id": 2, "search_method": "grid",
            "objective_metric": "sr", "param_space": {},
        }
        mock_src.return_value.get_strategy_source_for_user.side_effect = RuntimeError("db down")

        result = run_optimization_record_task(task_id=44)
        assert result["status"] == "failed"
        dao.update_status.assert_called_with(44, "failed")
