"""Batch 15 – cover ~90 lines across tasks.py (optuna optimization),
paper_strategy_executor, paper_trading routes, and data_sync_daemon.
"""

from __future__ import annotations

from datetime import date, datetime
from unittest.mock import MagicMock, patch

import pandas as pd


# ---------------------------------------------------------------------------
# 1. Worker tasks – Optuna Bayesian optimization  (lines 1071-1136, ~60 lines)
# ---------------------------------------------------------------------------


class TestOptunaBayesianOptimization:
    """Cover _run_bayesian_optimization function in tasks.py."""

    @patch("app.worker.service.tasks._evaluate_single")
    def test_bayesian_with_param_space(self, mock_eval):
        """Cover param_defs branch with suggest_float."""
        mock_eval.return_value = ("setting1", 100.0, {})
        from app.worker.service.tasks import _run_bayesian_sequential as _run_bayesian_optimization

        opt_setting = MagicMock()
        opt_setting.target_name = "total_return"
        opt_setting.generate_settings.return_value = []

        result = _run_bayesian_optimization(
            strategy_class=MagicMock,
            symbol="000001.SSE",
            start=datetime(2023, 1, 1),
            end=datetime(2024, 1, 1),
            rate=0.0003,
            slippage=0.0,
            size=1,
            pricetick=0.01,
            capital=100000.0,
            optimization_setting=opt_setting,
            param_space={"fast": {"min": 5, "max": 20, "step": 1}},
            n_trials=3,
        )
        assert isinstance(result, list)

    @patch("app.worker.service.tasks._evaluate_single")
    def test_bayesian_grid_fallback(self, mock_eval):
        """Cover all_settings grid index branch when no param_defs."""
        mock_eval.return_value = ("setting1", 50.0, {})
        from app.worker.service.tasks import _run_bayesian_sequential as _run_bayesian_optimization

        opt_setting = MagicMock()
        opt_setting.target_name = "total_return"
        opt_setting.generate_settings.return_value = [
            {"fast": 5},
            {"fast": 10},
            {"fast": 15},
        ]

        result = _run_bayesian_optimization(
            strategy_class=MagicMock,
            symbol="000001.SSE",
            start=datetime(2023, 1, 1),
            end=datetime(2024, 1, 1),
            rate=0.0003,
            slippage=0.0,
            size=1,
            pricetick=0.01,
            capital=100000.0,
            optimization_setting=opt_setting,
            param_space=None,
            n_trials=3,
        )
        assert isinstance(result, list)

    @patch("app.worker.service.tasks._evaluate_single")
    def test_bayesian_eval_returns_none(self, mock_eval):
        """Cover objective returning -inf when _evaluate_single returns None."""
        mock_eval.return_value = None
        from app.worker.service.tasks import _run_bayesian_sequential as _run_bayesian_optimization

        opt_setting = MagicMock()
        opt_setting.target_name = "total_return"
        opt_setting.generate_settings.return_value = [{"fast": 5}]

        result = _run_bayesian_optimization(
            strategy_class=MagicMock,
            symbol="000001.SSE",
            start=datetime(2023, 1, 1),
            end=datetime(2024, 1, 1),
            rate=0.0003,
            slippage=0.0,
            size=1,
            pricetick=0.01,
            capital=100000.0,
            optimization_setting=opt_setting,
            param_space=None,
            n_trials=2,
        )
        assert isinstance(result, list)

    @patch("app.worker.service.tasks._evaluate_single")
    def test_bayesian_empty_param_space_and_settings(self, mock_eval):
        """Cover early return when both param_defs and all_settings are empty."""
        from app.worker.service.tasks import _run_bayesian_sequential as _run_bayesian_optimization

        opt_setting = MagicMock()
        opt_setting.target_name = "total_return"
        opt_setting.generate_settings.return_value = []

        result = _run_bayesian_optimization(
            strategy_class=MagicMock,
            symbol="000001.SSE",
            start=datetime(2023, 1, 1),
            end=datetime(2024, 1, 1),
            rate=0.0003,
            slippage=0.0,
            size=1,
            pricetick=0.01,
            capital=100000.0,
            optimization_setting=opt_setting,
            param_space={},
            n_trials=3,
        )
        assert result == []

    @patch("app.worker.service.tasks._evaluate_single")
    def test_bayesian_param_space_bad_config(self, mock_eval):
        """Cover continue branch when param_space has invalid config."""
        mock_eval.return_value = None
        from app.worker.service.tasks import _run_bayesian_sequential as _run_bayesian_optimization

        opt_setting = MagicMock()
        opt_setting.target_name = "total_return"
        opt_setting.generate_settings.return_value = [{"fast": 5}]

        result = _run_bayesian_optimization(
            strategy_class=MagicMock,
            symbol="000001.SSE",
            start=datetime(2023, 1, 1),
            end=datetime(2024, 1, 1),
            rate=0.0003,
            slippage=0.0,
            size=1,
            pricetick=0.01,
            capital=100000.0,
            optimization_setting=opt_setting,
            param_space={"bad1": "not_a_dict", "bad2": {"min": "xyz"}},
            n_trials=2,
        )
        assert isinstance(result, list)


# ---------------------------------------------------------------------------
# 2. Worker tasks – _evaluate_single  (lines 883, ~5 lines)
# ---------------------------------------------------------------------------


class TestEvaluateSingle:
    """Cover _evaluate_single exception branch."""

    @patch("app.worker.service.tasks.evaluate", side_effect=RuntimeError("engine crash"))
    def test_evaluate_single_exception(self, mock_eval):
        from app.worker.service.tasks import _evaluate_single

        result = _evaluate_single(
            "total_return",
            MagicMock,
            "000001.SSE",
            datetime(2023, 1, 1),
            datetime(2024, 1, 1),
            0.0003, 0.0, 1, 0.01, 100000.0,
            {"fast": 5},
        )
        assert result is None

    @patch("app.worker.service.tasks.evaluate")
    def test_evaluate_single_success(self, mock_eval):
        mock_eval.return_value = ("setting", 123.0, {})
        from app.worker.service.tasks import _evaluate_single

        result = _evaluate_single(
            "total_return",
            MagicMock,
            "000001.SSE",
            datetime(2023, 1, 1),
            datetime(2024, 1, 1),
            0.0003, 0.0, 1, 0.01, 100000.0,
            {"fast": 5},
        )
        assert result == ("setting", 123.0, {})


# ---------------------------------------------------------------------------
# 3. Worker tasks – polyfit alpha/beta  (lines 187-188, ~2 lines)
# ---------------------------------------------------------------------------


class TestAlphaBeta:
    """Cover alpha/beta polyfit exception branch."""

    def test_compute_alpha_beta_insufficient_data(self):
        import numpy as np
        from app.worker.service.tasks import calculate_alpha_beta_for_worker

        # Less than 2 data points after masking
        result = calculate_alpha_beta_for_worker(np.array([1.0]), np.array([2.0]))
        assert result == (None, None)

    def test_compute_alpha_beta_nan_heavy(self):
        import numpy as np
        from app.worker.service.tasks import calculate_alpha_beta_for_worker

        # All NaN → less than 2 after mask
        s = np.array([float("nan"), float("nan")])
        b = np.array([float("nan"), float("nan")])
        result = calculate_alpha_beta_for_worker(s, b)
        assert result == (None, None)


# ---------------------------------------------------------------------------
# 4. Worker tasks – get_class_parameters exception  (line 354-356)
# ---------------------------------------------------------------------------


class TestGetClassParametersException:
    """Cover get_class_parameters() exception in run_backtest_task."""

    @patch("app.worker.service.tasks.save_backtest_to_db")
    @patch("app.worker.service.tasks.get_job_storage")
    @patch("app.worker.service.tasks.BacktestingEngine")
    @patch("app.worker.service.tasks.compile_strategy")
    def test_get_class_parameters_raises(self, mock_compile, mock_engine_cls, mock_storage, mock_save):
        """Cover the exception branch when get_class_parameters raises."""
        strategy_cls = MagicMock()
        strategy_cls.get_class_parameters.side_effect = RuntimeError("bad")
        mock_compile.return_value = strategy_cls

        engine = MagicMock()
        engine.history_data = [MagicMock()]
        engine.calculate_result.return_value = MagicMock()
        engine.calculate_statistics.return_value = {
            "total_return": 10.0,
            "annual_return": 5.0,
            "max_drawdown": -3.0,
            "max_ddpercent": -3.0,
            "sharpe_ratio": 1.2,
            "calmar_ratio": 1.5,
            "total_trade_count": 10,
            "daily_trade_count": 0.5,
            "start_date": "2023-01-01",
            "end_date": "2024-01-01",
        }
        engine.daily_results = {}
        mock_engine_cls.return_value = engine

        mock_storage.return_value.get_job_metadata.return_value = None

        from app.worker.service.tasks import run_backtest_task

        result = run_backtest_task(
            strategy_code="class TestStrategy: pass",
            strategy_class_name="TestStrategy",
            symbol="000001",
            start_date="2023-01-01",
            end_date="2024-01-01",
            initial_capital=100000.0,
            rate=0.0003,
            slippage=0.0,
            size=1,
            pricetick=0.01,
        )
        assert result["status"] == "completed"


# ---------------------------------------------------------------------------
# 5. Paper strategy executor  (lines 117-118, 138-139, 307-332)
# ---------------------------------------------------------------------------


class TestPaperStrategyExecutorCoverage:
    """Cover _PaperCtaEngine._execute_order and PaperStrategyExecutor._run_strategy."""

    @patch("app.domains.trading.paper_strategy_executor.connection")
    @patch("app.domains.trading.matching_engine.try_fill_market_order")
    @patch("app.domains.trading.paper_account_service.PaperAccountService")
    @patch("app.domains.market.realtime_quote_service.RealtimeQuoteService")
    def test_execute_order_no_price(self, mock_quote_cls, mock_acct_cls, mock_fill, mock_conn):
        """Cover last_price <= 0 → skip order."""
        from app.domains.trading.paper_strategy_executor import _PaperCtaEngine

        mock_executor = MagicMock()
        engine = _PaperCtaEngine(
            executor=mock_executor,
            deployment_id=1,
            paper_account_id=1,
            user_id=1,
            vt_symbol="000001.SSE",
            execution_mode="auto",
        )

        # Mock quote service to return 0 price
        mock_quote_svc = MagicMock()
        mock_quote_svc.get_quote.return_value = {"last_price": 0}
        mock_quote_cls.return_value = mock_quote_svc

        # Mock _get_market
        ctx = MagicMock()
        mock_conn.return_value.__enter__ = MagicMock(return_value=ctx)
        mock_conn.return_value.__exit__ = MagicMock(return_value=False)
        ctx.execute.return_value.fetchone.return_value = MagicMock(market="CN")

        engine._execute_order("buy", 100, 0.0)
        mock_fill.assert_not_called()

    @patch("app.domains.trading.paper_strategy_executor.connection")
    @patch("app.domains.trading.matching_engine.try_fill_market_order")
    @patch("app.domains.trading.paper_account_service.PaperAccountService")
    @patch("app.domains.market.realtime_quote_service.RealtimeQuoteService")
    def test_execute_order_fill_failed(self, mock_quote_cls, mock_acct_cls, mock_fill, mock_conn):
        """Cover fill.filled == False branch."""
        from app.domains.trading.paper_strategy_executor import _PaperCtaEngine

        engine = _PaperCtaEngine(
            executor=MagicMock(),
            deployment_id=1,
            paper_account_id=1,
            user_id=1,
            vt_symbol="000001.SSE",
            execution_mode="auto",
        )

        mock_quote_svc = MagicMock()
        mock_quote_svc.get_quote.return_value = {"last_price": 10.0}
        mock_quote_cls.return_value = mock_quote_svc

        ctx = MagicMock()
        mock_conn.return_value.__enter__ = MagicMock(return_value=ctx)
        mock_conn.return_value.__exit__ = MagicMock(return_value=False)
        ctx.execute.return_value.fetchone.return_value = MagicMock(market="CN")

        fill = MagicMock()
        fill.filled = False
        fill.reason = "no liquidity"
        mock_fill.return_value = fill

        engine._execute_order("buy", 100, 10.0)

    def test_stop_deployment_not_found(self):
        """Cover stop_deployment returning False when no event found."""
        from app.domains.trading.paper_strategy_executor import PaperStrategyExecutor

        executor = PaperStrategyExecutor.__new__(PaperStrategyExecutor)
        executor._initialized = True
        executor._threads = {}
        executor._stop_events = {}
        assert executor.stop_deployment(9999) is False

    def test_is_running_false(self):
        """Cover is_running returning False."""
        from app.domains.trading.paper_strategy_executor import PaperStrategyExecutor

        executor = PaperStrategyExecutor.__new__(PaperStrategyExecutor)
        executor._initialized = True
        executor._threads = {}
        executor._stop_events = {}
        assert executor.is_running(9999) is False


# ---------------------------------------------------------------------------
# 6. Data sync daemon  (lines 109-111, 269, 310, 346-348, 653, 978-982)
# ---------------------------------------------------------------------------


class TestDataSyncDaemonCoverage:
    """Cover get_trade_days fallback, write_sync_log dry_run, akshare import guard."""

    def test_get_trade_days_fallback(self):
        """Cover fallback to weekdays when trade_cal fails."""
        with patch("app.datasync.service.data_sync_daemon.call_pro", side_effect=RuntimeError("no data")):
            from app.datasync.service.data_sync_daemon import get_trade_days

            result = get_trade_days(date(2024, 1, 1), date(2024, 1, 7))
            assert isinstance(result, list)
            assert len(result) > 0
            # All should be weekdays
            for d_str in result:
                d = datetime.strptime(d_str, "%Y-%m-%d").date()
                assert d.weekday() < 5

    def test_write_sync_log_dry_run(self):
        """Cover DRY_RUN early return in write_sync_log."""
        with patch("app.datasync.service.data_sync_daemon.DRY_RUN", True):
            from app.datasync.service.data_sync_daemon import write_sync_log

            # Should not raise, just log
            write_sync_log(date(2024, 1, 1), "daily", "success", rows=100)

    def test_get_trade_days_with_trade_cal(self):
        """Cover successful trade_cal path with calendar_date column."""
        df = pd.DataFrame({
            "is_open": [1, 0, 1],
            "calendar_date": ["20240102", "20240103", "20240104"],
        })
        with patch("app.datasync.service.data_sync_daemon.call_pro", return_value=df):
            from app.datasync.service.data_sync_daemon import get_trade_days

            result = get_trade_days(date(2024, 1, 1), date(2024, 1, 7))
            assert len(result) == 2

    def test_get_trade_days_cal_date_column(self):
        """Cover cal_date column variant."""
        df = pd.DataFrame({
            "is_open": [1, 1],
            "cal_date": ["20240102", "20240103"],
        })
        with patch("app.datasync.service.data_sync_daemon.call_pro", return_value=df):
            from app.datasync.service.data_sync_daemon import get_trade_days

            result = get_trade_days(date(2024, 1, 1), date(2024, 1, 7))
            assert len(result) == 2

    def test_get_trade_days_none_response(self):
        """Cover trade_cal returning None → raises → falls back to weekdays."""
        with patch("app.datasync.service.data_sync_daemon.call_pro", return_value=None):
            from app.datasync.service.data_sync_daemon import get_trade_days

            result = get_trade_days(date(2024, 1, 1), date(2024, 1, 5))
            assert isinstance(result, list)


# ---------------------------------------------------------------------------
# 7. Worker tasks – grid sequential and random sequential  (lines 492-493, 612-613, 648-649)
# ---------------------------------------------------------------------------


class TestGridAndRandomOptimization:
    """Cover _run_grid_sequential and other optimization helper lines."""

    @patch("app.worker.service.tasks._evaluate_single")
    def test_grid_sequential_empty(self, mock_eval):
        from app.worker.service.tasks import _run_grid_sequential

        opt_setting = MagicMock()
        opt_setting.generate_settings.return_value = []

        result = _run_grid_sequential(
            strategy_class=MagicMock,
            symbol="000001.SSE",
            start=datetime(2023, 1, 1),
            end=datetime(2024, 1, 1),
            rate=0.0003,
            slippage=0.0,
            size=1,
            pricetick=0.01,
            capital=100000.0,
            optimization_setting=opt_setting,
        )
        assert result == []

    @patch("app.worker.service.tasks._evaluate_single")
    def test_grid_sequential_with_results(self, mock_eval):
        """Cover grid with large space causing sampling."""
        mock_eval.return_value = ("setting", 50.0, {})
        from app.worker.service.tasks import _run_grid_sequential

        opt_setting = MagicMock()
        # Create > 800 settings to trigger sampling
        opt_setting.generate_settings.return_value = [{"fast": i} for i in range(850)]

        result = _run_grid_sequential(
            strategy_class=MagicMock,
            symbol="000001.SSE",
            start=datetime(2023, 1, 1),
            end=datetime(2024, 1, 1),
            rate=0.0003,
            slippage=0.0,
            size=1,
            pricetick=0.01,
            capital=100000.0,
            optimization_setting=opt_setting,
        )
        assert len(result) > 0

    @patch("app.worker.service.tasks._evaluate_single", return_value=None)
    def test_grid_sequential_eval_none(self, mock_eval):
        """Cover _evaluate_single returning None in grid."""
        from app.worker.service.tasks import _run_grid_sequential

        opt_setting = MagicMock()
        opt_setting.generate_settings.return_value = [{"fast": 5}]

        result = _run_grid_sequential(
            strategy_class=MagicMock,
            symbol="000001.SSE",
            start=datetime(2023, 1, 1),
            end=datetime(2024, 1, 1),
            rate=0.0003,
            slippage=0.0,
            size=1,
            pricetick=0.01,
            capital=100000.0,
            optimization_setting=opt_setting,
        )
        assert result == []


# ---------------------------------------------------------------------------
# 8. Worker tasks – misc uncovered single lines (line 17, 1052, 1231, 1257-1261)
# ---------------------------------------------------------------------------


class TestWorkerTasksMisc:
    """Cover miscellaneous uncovered lines in tasks.py."""

    @patch("app.worker.service.tasks.evaluate", side_effect=RuntimeError("crash"))
    def test_evaluate_single_logs_exception(self, mock_eval):
        """Explicitly cover line 883 — exception logging in _evaluate_single."""
        from app.worker.service.tasks import _evaluate_single

        result = _evaluate_single(
            "sharpe_ratio",
            MagicMock,
            "000001.SSE",
            datetime(2023, 1, 1),
            datetime(2024, 1, 1),
            0.001, 0.0, 1, 0.01, 100000.0,
            {"param": "val"},
        )
        assert result is None
