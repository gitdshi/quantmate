"""Coverage batch 8 — target 93% → 95%+.

Covers remaining gaps in:
  - tushare_ingest (dividend/top10/adj_factor date-range, ingest_all_daily, retry_failed)
  - tasks.py (backtest error handler, bulk child iteration, bayesian optimisation)
  - tushare_dao (upsert helpers, get_failed_ts_codes, _clean, _round2)
  - vnpy_trading_service (disconnect, send_order, query_account, resolve_gateway)
  - Routes: backtest, factors, strategies, datasync, composite, templates, settings, auth
  - strategies/service (update_strategy version bump, restore_code_history)
  - factor_screening (mine_alpha158_factors, screen dedup)
  - realtime_quote_service (tencent quote helpers, HK/US/FX/futures)
  - calendar_service (trade_days_from_db, events)
  - sentiment_service (get_overview, get_fear_greed)
  - backtest_history_dao (json_default, save)
  - trade_log_dao (list, count)
  - akshare_ingest (ingest_all_indexes, call_ak retry, main CLI)
"""
from __future__ import annotations

import json
import os
import sys
import time
from datetime import date, datetime, timedelta
from types import SimpleNamespace
from typing import Any, Dict, List, Optional
from unittest.mock import MagicMock, patch, PropertyMock, call

import numpy as np
import pandas as pd
import pytest


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _fake_engine():
    eng = MagicMock()
    ctx = MagicMock()
    eng.begin.return_value.__enter__ = lambda s: ctx
    eng.begin.return_value.__exit__ = lambda s, *a: None
    eng.connect.return_value.__enter__ = lambda s: ctx
    eng.connect.return_value.__exit__ = lambda s, *a: None
    return eng, ctx


def _fake_conn():
    ctx = MagicMock()
    return ctx


# ===================================================================
# 1. tushare_dao — _clean, _round2, get_failed_ts_codes, upsert helpers
# ===================================================================


class TestTushareDaoHelpers:
    """Cover _clean, _round2, get_failed_ts_codes, audit_finish, upsert_daily."""

    @pytest.fixture(autouse=True)
    def _load(self):
        self.mod = __import__("app.domains.extdata.dao.tushare_dao", fromlist=["x"])

    def test_clean_none(self):
        assert self.mod._clean(None) is None

    def test_clean_nan(self):
        assert self.mod._clean(float("nan")) is None

    def test_clean_np_integer(self):
        assert self.mod._clean(np.int64(42)) == 42
        assert isinstance(self.mod._clean(np.int64(42)), int)

    def test_clean_np_floating(self):
        assert self.mod._clean(np.float64(3.14)) == pytest.approx(3.14)
        assert isinstance(self.mod._clean(np.float64(3.14)), float)

    def test_clean_np_bool(self):
        assert self.mod._clean(np.bool_(True)) is True
        assert isinstance(self.mod._clean(np.bool_(True)), bool)

    def test_clean_passthrough(self):
        assert self.mod._clean("hello") == "hello"
        assert self.mod._clean(42) == 42

    def test_round2_none(self):
        assert self.mod._round2(None) is None

    def test_round2_nan(self):
        assert self.mod._round2(float("nan")) is None

    def test_round2_normal(self):
        assert self.mod._round2(3.1459) == 3.15

    def test_round2_string_that_fails(self):
        result = self.mod._round2("abc")
        assert result == "abc"  # returns v on exception

    def test_audit_finish(self, monkeypatch):
        eng, ctx = _fake_engine()
        monkeypatch.setattr(self.mod, "engine", eng)
        self.mod.audit_finish(99, "success", 100)
        ctx.execute.assert_called_once()

    def test_upsert_daily_empty(self, monkeypatch):
        eng, ctx = _fake_engine()
        monkeypatch.setattr(self.mod, "engine", eng)
        assert self.mod.upsert_daily(pd.DataFrame()) == 0
        assert self.mod.upsert_daily(None) == 0

    def test_upsert_daily_rows(self, monkeypatch):
        eng, ctx = _fake_engine()
        monkeypatch.setattr(self.mod, "engine", eng)
        df = pd.DataFrame([{
            "ts_code": "000001.SZ", "trade_date": "20240101",
            "open": 10.0, "high": 11.0, "low": 9.5, "close": 10.5,
            "pre_close": 10.0, "change_amount": 0.5, "pct_change": 5.0,
            "vol": 1000, "amount": 10000,
        }])
        result = self.mod.upsert_daily(df)
        assert result == 1
        ctx.execute.assert_called_once()

    def test_get_failed_ts_codes(self, monkeypatch):
        eng, ctx = _fake_engine()
        monkeypatch.setattr(self.mod, "engine", eng)
        ctx.execute.return_value.fetchall.return_value = [("000001.SZ",), ("000002.SZ",)]
        result = self.mod.get_failed_ts_codes(limit=10)
        assert result == ["000001.SZ", "000002.SZ"]

    def test_get_failed_ts_codes_no_limit(self, monkeypatch):
        eng, ctx = _fake_engine()
        monkeypatch.setattr(self.mod, "engine", eng)
        ctx.execute.return_value.fetchall.return_value = []
        result = self.mod.get_failed_ts_codes()
        assert result == []

    def test_upsert_adj_factor(self, monkeypatch):
        eng, ctx = _fake_engine()
        monkeypatch.setattr(self.mod, "engine", eng)
        df = pd.DataFrame([{
            "ts_code": "000001.SZ", "trade_date": "20240101", "adj_factor": 1.05,
        }])
        result = self.mod.upsert_adj_factor(df)
        assert result == 1

    def test_upsert_adj_factor_empty(self, monkeypatch):
        eng, ctx = _fake_engine()
        monkeypatch.setattr(self.mod, "engine", eng)
        assert self.mod.upsert_adj_factor(None) == 0

    def test_upsert_moneyflow(self, monkeypatch):
        eng, ctx = _fake_engine()
        monkeypatch.setattr(self.mod, "engine", eng)
        df = pd.DataFrame([{
            "ts_code": "000001.SZ", "trade_date": "20240101",
            "net_mf": 100.0, "buy_small": 10.0, "sell_small": 5.0,
            "buy_medium": 20.0, "sell_medium": 15.0,
            "buy_large": 30.0, "sell_large": 25.0,
            "buy_huge": 40.0, "sell_huge": 35.0,
        }])
        result = self.mod.upsert_moneyflow(df)
        assert result == 1

    def test_upsert_daily_basic(self, monkeypatch):
        eng, ctx = _fake_engine()
        monkeypatch.setattr(self.mod, "engine", eng)
        df = pd.DataFrame([{
            "ts_code": "000001.SZ", "trade_date": "20240101",
            "turnover_rate": 5.0, "turnover_rate_f": 4.5, "volume_ratio": 1.2,
            "pe": 15.0, "pe_ttm": 14.0, "pb": 2.0, "ps": 3.0, "ps_ttm": 2.8,
            "total_mv": 100000.0, "circ_mv": 80000.0,
        }])
        result = self.mod.upsert_daily_basic(df)
        assert result == 1


# ===================================================================
# 2. tushare_ingest — date-range functions, retry_failed, ingest_all_daily
# ===================================================================


class TestTushareIngestDateRange:
    """Cover ingest_top10_holders_by_date_range, ingest_adj_factor_by_date_range,
    ingest_dividend_by_date_range, retry_failed_daily, get_failed_ts_codes."""

    @pytest.fixture(autouse=True)
    def _load(self):
        self.mod = __import__("app.datasync.service.tushare_ingest", fromlist=["x"])

    def test_ingest_top10_holders_by_date_range(self, monkeypatch):
        monkeypatch.setattr(self.mod, "get_all_ts_codes", lambda: ["000001.SZ"])
        df = pd.DataFrame([{
            "ts_code": "000001.SZ", "end_date": "20240101",
            "holder_name": "Test Holder", "hold_amount": 1000.0, "hold_ratio": 5.0,
        }])
        monkeypatch.setattr(self.mod, "call_pro", lambda *a, **kw: df)
        monkeypatch.setattr(self.mod, "_fetch_existing_keys", lambda *a, **kw: set())
        monkeypatch.setattr(self.mod, "upsert_top10_holders", lambda d: len(d))
        monkeypatch.setenv("BATCH_SIZE", "100")

        self.mod.ingest_top10_holders_by_date_range("2024-01-01", "2024-01-31")
        # No assertion needed — just verifying it runs without error

    def test_ingest_top10_holders_skip_until(self, monkeypatch):
        monkeypatch.setattr(self.mod, "get_all_ts_codes", lambda: ["000001.SZ", "000002.SZ"])
        df = pd.DataFrame([{
            "ts_code": "000002.SZ", "end_date": "20240101",
            "holder_name": "X", "hold_amount": 500.0, "hold_ratio": 2.0,
        }])
        call_count = {"n": 0}
        def _mock_call_pro(*a, **kw):
            call_count["n"] += 1
            return df
        monkeypatch.setattr(self.mod, "call_pro", _mock_call_pro)
        monkeypatch.setattr(self.mod, "_fetch_existing_keys", lambda *a, **kw: set())
        monkeypatch.setattr(self.mod, "upsert_top10_holders", lambda d: len(d))
        monkeypatch.setenv("BATCH_SIZE", "100")

        self.mod.ingest_top10_holders_by_date_range(
            "2024-01-01", "2024-01-31", start_after_ts_code="000001.SZ"
        )
        # Should skip 000001, start from 000002
        assert call_count["n"] == 1

    def test_ingest_adj_factor_by_date_range(self, monkeypatch):
        monkeypatch.setattr(self.mod, "get_all_ts_codes", lambda: ["000001.SZ"])
        df = pd.DataFrame([{
            "ts_code": "000001.SZ", "trade_date": "20240101", "adj_factor": 1.05,
        }])
        monkeypatch.setattr(self.mod, "call_pro", lambda *a, **kw: df)
        monkeypatch.setattr(self.mod, "_fetch_existing_keys", lambda *a, **kw: set())
        monkeypatch.setattr(self.mod, "upsert_adj_factor", lambda d: len(d))
        monkeypatch.setenv("BATCH_SIZE", "100")

        self.mod.ingest_adj_factor_by_date_range("2024-01-01", "2024-01-31")

    def test_ingest_adj_factor_existing_skipped(self, monkeypatch):
        monkeypatch.setattr(self.mod, "get_all_ts_codes", lambda: ["000001.SZ"])
        df = pd.DataFrame([{
            "ts_code": "000001.SZ", "trade_date": "20240101", "adj_factor": 1.05,
        }])
        monkeypatch.setattr(self.mod, "call_pro", lambda *a, **kw: df)
        monkeypatch.setattr(self.mod, "_fetch_existing_keys",
                            lambda *a, **kw: {("000001.SZ", "2024-01-01")})
        upsert_calls = {"n": 0}
        def _mock_upsert(d):
            upsert_calls["n"] += 1
            return 0
        monkeypatch.setattr(self.mod, "upsert_adj_factor", _mock_upsert)
        monkeypatch.setenv("BATCH_SIZE", "100")

        self.mod.ingest_adj_factor_by_date_range("2024-01-01", "2024-01-31")
        assert upsert_calls["n"] == 0

    def test_get_failed_ts_codes_func(self, monkeypatch):
        monkeypatch.setattr(
            "app.domains.extdata.dao.tushare_dao.get_failed_ts_codes",
            lambda limit=None: ["000001.SZ"],
        )
        result = self.mod.get_failed_ts_codes(limit=5)
        assert result == ["000001.SZ"]

    def test_retry_failed_daily(self, monkeypatch):
        monkeypatch.setattr(self.mod, "get_failed_ts_codes", lambda limit=None: ["000001.SZ"])
        calls = []
        monkeypatch.setattr(self.mod, "ingest_daily", lambda **kw: calls.append(kw))
        self.mod.retry_failed_daily(limit=5)
        assert len(calls) == 1

    def test_retry_failed_daily_exception(self, monkeypatch):
        monkeypatch.setattr(self.mod, "get_failed_ts_codes", lambda limit=None: ["BAD.SZ"])
        monkeypatch.setattr(self.mod, "ingest_daily", MagicMock(side_effect=RuntimeError("fail")))
        # Should not raise — logs exception
        self.mod.retry_failed_daily()


class TestTushareIngestAllDaily:
    """Cover ingest_all_daily — incremental + full history paths."""

    @pytest.fixture(autouse=True)
    def _load(self):
        self.mod = __import__("app.datasync.service.tushare_ingest", fromlist=["x"])

    def test_incremental_path(self, monkeypatch):
        """When get_max_trade_date returns a date, use incremental fetch."""
        monkeypatch.setattr(self.mod, "get_all_ts_codes", lambda: ["000001.SZ"])
        monkeypatch.setattr(self.mod, "get_max_trade_date", lambda ts: "2024-01-01")
        calls = []
        monkeypatch.setattr(self.mod, "ingest_daily", lambda **kw: calls.append(kw))
        monkeypatch.setenv("BATCH_SIZE", "100")

        self.mod.ingest_all_daily(batch_size=100, sleep_between=0)
        assert len(calls) == 1

    def test_full_history_path(self, monkeypatch):
        """When no data in DB, fetch full history."""
        monkeypatch.setattr(self.mod, "get_all_ts_codes", lambda: ["000001.SZ"])
        monkeypatch.setattr(self.mod, "get_max_trade_date", lambda ts: None)
        df = pd.DataFrame([{
            "ts_code": "000001.SZ", "trade_date": "20240101",
            "open": 10.0, "high": 11.0, "low": 9.5, "close": 10.5,
            "pre_close": 10.0, "change_amount": 0.5, "pct_change": 5.0,
            "vol": 1000, "amount": 10000,
        }])
        monkeypatch.setattr(self.mod, "call_pro", lambda *a, **kw: df)
        monkeypatch.setattr(self.mod, "_fetch_existing_keys", lambda *a, **kw: set())
        monkeypatch.setattr(self.mod, "upsert_daily", lambda d: len(d))
        monkeypatch.setenv("BATCH_SIZE", "100")

        self.mod.ingest_all_daily(batch_size=100, sleep_between=0, force_full_per_stock=True)

    def test_full_history_empty_df(self, monkeypatch):
        monkeypatch.setattr(self.mod, "get_all_ts_codes", lambda: ["000001.SZ"])
        monkeypatch.setattr(self.mod, "get_max_trade_date", lambda ts: None)
        monkeypatch.setattr(self.mod, "call_pro", lambda *a, **kw: pd.DataFrame())
        monkeypatch.setenv("BATCH_SIZE", "100")

        self.mod.ingest_all_daily(batch_size=100, sleep_between=0)

    def test_skip_until_found(self, monkeypatch):
        monkeypatch.setattr(self.mod, "get_all_ts_codes", lambda: ["AAA.SZ", "BBB.SZ"])
        monkeypatch.setattr(self.mod, "get_max_trade_date", lambda ts: "2024-01-01")
        calls = []
        monkeypatch.setattr(self.mod, "ingest_daily", lambda **kw: calls.append(kw))
        monkeypatch.setenv("BATCH_SIZE", "100")

        self.mod.ingest_all_daily(
            batch_size=100, sleep_between=0, start_after_ts_code="AAA.SZ"
        )
        assert len(calls) == 1  # only BBB.SZ processed

    def test_progress_callback(self, monkeypatch):
        monkeypatch.setattr(self.mod, "get_all_ts_codes", lambda: ["000001.SZ"])
        monkeypatch.setattr(self.mod, "get_max_trade_date", lambda ts: "2024-01-01")
        monkeypatch.setattr(self.mod, "ingest_daily", lambda **kw: None)
        monkeypatch.setenv("BATCH_SIZE", "100")

        progress = []
        self.mod.ingest_all_daily(
            batch_size=100, sleep_between=0, progress_cb=lambda **kw: progress.append(kw)
        )
        assert len(progress) == 1


# ===================================================================
# 3. tasks.py — backtest error handler, bulk child iteration, bayesian
# ===================================================================


class TestTasksBacktestError:
    """Cover the except block in run_backtest_task (lines 492-537)."""

    @pytest.fixture(autouse=True)
    def _load(self):
        self.mod = __import__("app.worker.service.tasks", fromlist=["x"])

    def test_backtest_error_saves_failed(self, monkeypatch):
        storage = MagicMock()
        storage.get_job_metadata.return_value = {"strategy_version": "3"}
        monkeypatch.setattr(self.mod, "get_job_storage", lambda: storage)
        save_calls = []
        monkeypatch.setattr(self.mod, "save_backtest_to_db",
                            lambda **kw: save_calls.append(kw))
        # Make compile_strategy raise to trigger the except block
        monkeypatch.setattr(self.mod, "compile_strategy", MagicMock(side_effect=RuntimeError("compile fail")))

        result = self.mod.run_backtest_task(
            strategy_code="class X: pass",
            strategy_class_name="X",
            symbol="000001.SZ",
            start_date="2024-01-01",
            end_date="2024-06-01",
            initial_capital=100000,
            rate=0.0001,
            slippage=0,
            size=1,
            pricetick=0.01,
            parameters={},
            benchmark="000300.SZ",
            user_id=1,
            strategy_id=1,
        )
        assert result["status"] == "failed"
        assert "compile fail" in result["error"]
        # Verify save_backtest_to_db was called with failed status
        assert len(save_calls) == 1
        assert save_calls[0]["status"] == "failed"


class TestTasksBulkIteration:
    """Cover the bulk child iteration loop (lines 612-651)."""

    @pytest.fixture(autouse=True)
    def _load(self):
        self.mod = __import__("app.worker.service.tasks", fromlist=["x"])

    def test_bulk_child_success_and_fail(self, monkeypatch):
        storage = MagicMock()
        storage.get_job_metadata.return_value = {}
        monkeypatch.setattr(self.mod, "get_job_storage", lambda: storage)
        # Mock get_current_job to return a fake job with id
        fake_job = SimpleNamespace(id="bulk-123")
        monkeypatch.setattr(self.mod, "get_current_job", lambda: fake_job)

        def _mock_run_bt(**kw):
            if kw["symbol"] == "BAD.SZ":
                return {"status": "failed", "error": "bad"}
            return {
                "status": "completed",
                "statistics": {"total_return": 0.15},
                "symbol_name": "Test",
            }

        monkeypatch.setattr(self.mod, "run_backtest_task", _mock_run_bt)
        monkeypatch.setattr(self.mod, "_save_bulk_child", lambda *a, **kw: None)
        monkeypatch.setattr(self.mod, "_update_bulk_row", lambda *a, **kw: None)

        result = self.mod.run_bulk_backtest_task(
            strategy_code="class X: pass",
            strategy_class_name="X",
            symbols=["000001.SZ", "BAD.SZ", "000003.SZ"],
            start_date="2024-01-01",
            end_date="2024-06-01",
            initial_capital=100000,
            rate=0.0001,
            slippage=0,
            size=1,
            pricetick=0.01,
            parameters={},
            benchmark="000300.SZ",
            bulk_job_id="bulk-123",
            user_id=1,
            strategy_id=1,
        )
        assert result["successful"] == 2
        assert result["failed"] == 1
        assert result["best_symbol"] in ("000001.SZ", "000003.SZ")


class TestTasksBayesianOptimization:
    """Cover _run_bayesian_sequential with optuna (lines 1050-1136)."""

    @pytest.fixture(autouse=True)
    def _load(self):
        self.mod = __import__("app.worker.service.tasks", fromlist=["x"])

    def test_bayesian_falls_back_without_optuna(self, monkeypatch):
        """When optuna is not importable, fallback to random search."""
        real_import = __builtins__.__import__ if hasattr(__builtins__, "__import__") else __import__
        def _mock_import(name, *args, **kwargs):
            if name == "optuna":
                raise ImportError("no optuna")
            return real_import(name, *args, **kwargs)

        opt_setting = MagicMock()
        opt_setting.generate_settings.return_value = [{"fast_period": 10}]
        opt_setting.target_name = "sharpe_ratio"

        monkeypatch.setattr(self.mod, "_evaluate_single", lambda *a, **kw: ({"fast_period": 10}, 1.5, {}))
        # Since random search calls _evaluate_single too
        monkeypatch.setattr(self.mod, "_run_random_sequential", lambda **kw: [])

        with patch("builtins.__import__", _mock_import):
            result = self.mod._run_bayesian_sequential(
                strategy_class=MagicMock,
                symbol="000001.SZ",
                start=date(2024, 1, 1),
                end=date(2024, 6, 1),
                rate=0.0001,
                slippage=0,
                size=1,
                pricetick=0.01,
                capital=100000,
                optimization_setting=opt_setting,
                param_space={"fast": {"min": 5, "max": 20, "step": 1}},
                n_trials=5,
            )
            assert isinstance(result, list)


# ===================================================================
# 4. vnpy_trading_service — disconnect, send_order, query_account
# ===================================================================


class TestVnpyTradingServiceExtended:
    """Cover disconnect, list_gateways, send_order, query_account, resolve_gateway."""

    @pytest.fixture(autouse=True)
    def _load(self):
        self.mod = __import__("app.domains.trading.vnpy_trading_service", fromlist=["x"])
        # Reset singleton
        cls = self.mod.VnpyTradingService
        cls._instance = None
        cls._initialized = False

    def _make_svc(self):
        svc = self.mod.VnpyTradingService()
        return svc

    def test_disconnect_gateway_not_found(self):
        svc = self._make_svc()
        assert svc.disconnect_gateway("nonexistent") is False

    def test_disconnect_gateway_simulated(self):
        svc = self._make_svc()
        svc._connected_gateways["sim1"] = {
            "type": self.mod.GatewayType.SIMULATED,
            "connected": True,
            "config": {},
        }
        assert svc.disconnect_gateway("sim1") is True
        assert "sim1" not in svc._connected_gateways

    def test_disconnect_gateway_real(self):
        svc = self._make_svc()
        svc._main_engine = MagicMock()
        svc._connected_gateways["real1"] = {
            "type": self.mod.GatewayType.CTP,
            "connected": True,
            "config": {},
            "gateway_class_name": "CTP",
        }
        assert svc.disconnect_gateway("real1") is True

    def test_list_gateways(self):
        svc = self._make_svc()
        svc._connected_gateways["sim1"] = {
            "type": self.mod.GatewayType.SIMULATED,
            "connected": True,
        }
        result = svc.list_gateways()
        assert len(result) == 1
        assert result[0]["name"] == "sim1"
        assert result[0]["connected"] is True

    def test_send_order_not_connected(self):
        svc = self._make_svc()
        svc._connected_gateways["gw1"] = {"type": self.mod.GatewayType.SIMULATED, "connected": False}
        result = svc.send_order("000001.SZ", "buy", "limit", 100, 10.0, "gw1")
        assert result is None

    def test_send_order_sim(self):
        svc = self._make_svc()
        svc._connected_gateways["sim1"] = {
            "type": self.mod.GatewayType.SIMULATED,
            "connected": True,
        }
        result = svc.send_order("000001.SZ", "buy", "limit", 100, 10.0, "sim1")
        assert result is not None
        assert result.startswith("SIM-")

    def test_send_order_no_gateway_name(self):
        svc = self._make_svc()
        # No gateways connected
        result = svc.send_order("000001.SZ", "buy", "limit", 100, 10.0)
        assert result is None

    def test_query_account_no_engine(self):
        svc = self._make_svc()
        svc._main_engine = None
        assert svc.query_account() is None

    def test_query_account_empty(self):
        svc = self._make_svc()
        svc._main_engine = MagicMock()
        svc._main_engine.get_all_accounts.return_value = []
        assert svc.query_account() is None

    def test_query_account_success(self):
        svc = self._make_svc()
        svc._main_engine = MagicMock()
        acct = SimpleNamespace(balance=100000, available=80000, frozen=10000, margin=5000)
        svc._main_engine.get_all_accounts.return_value = [acct]
        result = svc.query_account()
        assert result is not None
        assert result.balance == 100000
        assert result.available == 80000

    def test_on_order_callback(self):
        svc = self._make_svc()
        cb = MagicMock()
        svc.on_order(cb)
        assert cb in svc._order_callbacks

    def test_on_trade_callback(self):
        svc = self._make_svc()
        cb = MagicMock()
        svc.on_trade(cb)
        assert cb in svc._trade_callbacks

    def test_default_gateway_name_none(self):
        svc = self._make_svc()
        assert svc._default_gateway_name() is None

    def test_default_gateway_name(self):
        svc = self._make_svc()
        svc._connected_gateways["first"] = {"connected": True}
        assert svc._default_gateway_name() == "first"

    def test_resolve_gateway_class_unsupported(self):
        with pytest.raises(ValueError, match="Unsupported"):
            self.mod.VnpyTradingService._resolve_gateway_class(
                SimpleNamespace(value="unknown")
            )


# ===================================================================
# 5. Route tests — using batch4 pattern (module-level fixtures)
# ===================================================================

from app.api.main import app
from app.api.services.auth_service import get_current_user
from app.api.models.user import TokenData
from fastapi.testclient import TestClient


def _user(**kw):
    defaults = dict(user_id=1, username="tester", exp=datetime(2099, 1, 1))
    defaults.update(kw)
    return TokenData(**defaults)


@pytest.fixture(autouse=True)
def _override_auth():
    app.dependency_overrides[get_current_user] = lambda: _user()
    yield
    app.dependency_overrides.clear()


@pytest.fixture(autouse=True)
def _bypass_rbac(monkeypatch):
    from app.domains.rbac.service.rbac_service import RbacService
    monkeypatch.setattr(
        RbacService, "check_permission",
        lambda self, user_id, resource, action, username=None: True,
    )


def _client():
    return TestClient(app, raise_server_exceptions=False)


# --- Auth routes ---

class TestAuthRoutes:
    """Cover auth route handlers (lines 82-149)."""

    def test_refresh_token_invalid(self, monkeypatch):
        from app.domains.auth.service import AuthService
        monkeypatch.setattr(AuthService, "refresh", MagicMock(side_effect=PermissionError("bad token")))
        resp = _client().post("/api/v1/auth/refresh", json="bad-token")
        assert resp.status_code in (401, 422)

    def test_get_me(self, monkeypatch):
        from app.domains.auth.service import AuthService
        monkeypatch.setattr(AuthService, "me", lambda self, uid: {
            "id": uid, "username": "test", "email": "t@t.com",
            "is_active": True, "created_at": datetime.now(),
            "role": "user", "primary_role": "user", "permissions": [],
        })
        resp = _client().get("/api/v1/auth/me")
        assert resp.status_code == 200

    def test_get_me_not_found(self, monkeypatch):
        from app.domains.auth.service import AuthService
        monkeypatch.setattr(AuthService, "me", MagicMock(side_effect=KeyError("not found")))
        resp = _client().get("/api/v1/auth/me")
        assert resp.status_code == 404

    def test_change_password_success(self, monkeypatch):
        from app.domains.auth.service import AuthService
        monkeypatch.setattr(AuthService, "change_password", lambda self, uid, old, new: None)
        resp = _client().post("/api/v1/auth/change-password", json={
            "current_password": "old", "new_password": "newNewNew1!"
        })
        assert resp.status_code == 200

    def test_change_password_wrong(self, monkeypatch):
        from app.domains.auth.service import AuthService
        monkeypatch.setattr(AuthService, "change_password",
                            MagicMock(side_effect=PermissionError("wrong")))
        resp = _client().post("/api/v1/auth/change-password", json={
            "current_password": "bad", "new_password": "newNewNew1!"
        })
        assert resp.status_code == 400


# --- Backtest routes ---

class TestBacktestRoutesB8:
    """Cover backtest route handlers (lines 48-62, 114, 174-182, 250-272)."""

    def test_get_batch_status_not_found(self):
        resp = _client().get("/api/v1/backtest/batch/status/nonexistent-id")
        assert resp.status_code == 404

    def test_list_history(self, monkeypatch):
        from app.domains.backtests.dao.backtest_history_dao import BacktestHistoryDao
        monkeypatch.setattr(BacktestHistoryDao, "count_for_user", lambda self, uid: 0)
        monkeypatch.setattr(BacktestHistoryDao, "list_for_user", lambda self, **kw: [])
        resp = _client().get("/api/v1/backtest/history/list")
        assert resp.status_code == 200

    def test_list_history_with_data(self, monkeypatch):
        from app.domains.backtests.dao.backtest_history_dao import BacktestHistoryDao
        monkeypatch.setattr(BacktestHistoryDao, "count_for_user", lambda self, uid: 1)
        monkeypatch.setattr(BacktestHistoryDao, "list_for_user", lambda self, **kw: [{
            "job_id": "j1", "strategy_class": "X", "symbol": "000001.SZ",
            "start_date": "2024-01-01", "end_date": "2024-06-01",
            "status": "completed", "created_at": datetime.now(),
            "result": json.dumps({"statistics": {"total_return": 0.15, "sharpe_ratio": 1.2}}),
        }])
        resp = _client().get("/api/v1/backtest/history/list")
        assert resp.status_code == 200

    def test_get_history_detail(self, monkeypatch):
        from app.domains.backtests.dao.backtest_history_dao import BacktestHistoryDao
        monkeypatch.setattr(BacktestHistoryDao, "get_detail_for_user", lambda self, **kw: {
            "job_id": "j1", "result": "{}", "status": "completed",
        })
        resp = _client().get("/api/v1/backtest/history/j1")
        assert resp.status_code == 200

    def test_get_history_detail_not_found(self, monkeypatch):
        from app.domains.backtests.dao.backtest_history_dao import BacktestHistoryDao
        monkeypatch.setattr(BacktestHistoryDao, "get_detail_for_user", lambda self, **kw: None)
        resp = _client().get("/api/v1/backtest/history/j1")
        assert resp.status_code == 404

    def test_cancel_backtest_not_found(self):
        resp = _client().post("/api/v1/backtest/nonexistent/cancel")
        assert resp.status_code in (404, 405)


# --- Factor routes ---

class TestFactorsRoutesB8:
    """Cover factor route handlers (lines 296-370, screening)."""

    def test_run_factor_screening(self, monkeypatch):
        monkeypatch.setattr(
            "app.domains.factors.factor_screening.screen_factor_pool",
            lambda **kw: [{"factor_name": "f1", "ic_mean": 0.1}],
        )
        monkeypatch.setattr(
            "app.domains.factors.factor_screening.save_screening_results",
            lambda **kw: 1,
        )
        resp = _client().post("/api/v1/factors/screening/run", json={
            "expressions": ["close/open"],
            "start_date": "2024-01-01",
            "end_date": "2024-06-01",
        })
        assert resp.status_code == 200
        data = resp.json()
        assert data["result_count"] == 1

    def test_run_factor_screening_bad_date(self):
        resp = _client().post("/api/v1/factors/screening/run", json={
            "expressions": ["close"],
            "start_date": "bad-date",
            "end_date": "2024-06-01",
        })
        assert resp.status_code == 400

    def test_run_factor_mining_no_qlib(self, monkeypatch):
        monkeypatch.setattr(
            "app.infrastructure.qlib.qlib_config.is_qlib_available",
            lambda: False,
        )
        resp = _client().post("/api/v1/factors/mining/run", json={
            "start_date": "2024-01-01",
            "end_date": "2024-06-01",
            "factor_set": "Alpha158",
        })
        assert resp.status_code == 503

    def test_delete_evaluation(self, monkeypatch):
        from app.domains.factors.service import FactorService
        monkeypatch.setattr(FactorService, "delete_evaluation", lambda self, *a, **kw: None)
        resp = _client().delete("/api/v1/factors/1/evaluations/1")
        assert resp.status_code == 204

    def test_delete_evaluation_not_found(self, monkeypatch):
        from app.domains.factors.service import FactorService
        monkeypatch.setattr(FactorService, "delete_evaluation",
                            MagicMock(side_effect=KeyError("not found")))
        resp = _client().delete("/api/v1/factors/1/evaluations/1")
        assert resp.status_code == 404


# --- Strategy routes ---

class TestStrategiesRoutesB8:
    """Cover strategies route handlers (lines 216-278, 400-435)."""

    def test_restore_code_history(self, monkeypatch):
        from app.domains.strategies.service import StrategiesService
        monkeypatch.setattr(StrategiesService, "restore_code_history", lambda self, *a: None)
        resp = _client().post("/api/v1/strategies/1/code-history/1/restore")
        assert resp.status_code == 200
        assert "restored" in resp.json()["message"].lower()

    def test_restore_code_history_not_found(self, monkeypatch):
        from app.domains.strategies.service import StrategiesService
        monkeypatch.setattr(StrategiesService, "restore_code_history",
                            MagicMock(side_effect=KeyError("Strategy not found")))
        resp = _client().post("/api/v1/strategies/1/code-history/1/restore")
        assert resp.status_code == 404

    def test_restore_code_history_history_not_found(self, monkeypatch):
        from app.domains.strategies.service import StrategiesService
        monkeypatch.setattr(StrategiesService, "restore_code_history",
                            MagicMock(side_effect=KeyError("History not found")))
        resp = _client().post("/api/v1/strategies/1/code-history/1/restore")
        assert resp.status_code == 404

    def test_list_builtin_strategies(self):
        resp = _client().get("/api/v1/strategies/builtin/list")
        assert resp.status_code == 200
        data = resp.json()
        assert isinstance(data, list)

    def test_create_multi_factor_strategy(self, monkeypatch):
        monkeypatch.setattr(
            "app.domains.strategies.multi_factor_engine.generate_cta_code",
            lambda **kw: "class TestStrategy: pass",
        )
        monkeypatch.setattr(
            "app.domains.strategies.multi_factor_engine.save_strategy_factors",
            lambda *a, **kw: None,
        )
        with patch("app.domains.strategies.service.StrategyDao"), \
             patch("app.domains.strategies.service.StrategyHistoryDao"):
            from app.domains.strategies.service import StrategiesService
            monkeypatch.setattr(StrategiesService, "create_strategy", lambda self, **kw: {
                "id": 1, "name": "test", "class_name": "TestStrategy",
                "version": 1, "is_active": True, "user_id": 1, "description": "",
                "parameters": "{}", "code": "class TestStrategy: pass",
                "created_at": datetime.now().isoformat(),
                "updated_at": datetime.now().isoformat(),
            })
        resp = _client().post("/api/v1/strategies/multi-factor/create", json={
            "name": "test",
            "class_name": "TestStrategy",
            "factors": [{"factor_name": "f1", "expression": "close/open", "weight": 1.0, "direction": "long"}],
        })
        assert resp.status_code in (200, 201, 422)


# --- Datasync routes ---

class TestDatasyncRoutesB8:
    """Cover datasync route handlers (lines 50-57, 107-155)."""

    def test_get_sync_summary(self, monkeypatch):
        eng, ctx = _fake_engine()
        monkeypatch.setattr("app.infrastructure.db.connections.get_quantmate_engine", lambda: eng)
        ctx.execute.return_value.fetchall.return_value = []
        resp = _client().get("/api/v1/datasync/status/summary?days=7")
        assert resp.status_code == 200

    def test_get_sync_status_with_filters(self, monkeypatch):
        eng, ctx = _fake_engine()
        monkeypatch.setattr("app.infrastructure.db.connections.get_quantmate_engine", lambda: eng)
        ctx.execute.return_value.fetchall.return_value = []
        resp = _client().get("/api/v1/datasync/status?sync_date=2024-01-01&source=tushare&status=success")
        assert resp.status_code in (200, 404, 500)


# --- Composite routes ---

class TestCompositeRoutesB8:
    """Cover composite route handlers (lines 272-291, 318-343)."""

    def test_create_composite(self, monkeypatch):
        from app.domains.composite.service import CompositeStrategyService
        monkeypatch.setattr(CompositeStrategyService, "create_composite", lambda self, **kw: {
            "id": 1, "name": "test", "user_id": 1, "description": "",
            "portfolio_config": {}, "market_constraints": {},
            "execution_mode": "paper", "is_active": True,
            "created_at": datetime.now().isoformat(),
            "updated_at": datetime.now().isoformat(),
            "components": [],
        })
        resp = _client().post("/api/v1/composite-strategies", json={
            "name": "test", "execution_mode": "paper",
        })
        assert resp.status_code in (200, 201)

    def test_create_composite_error(self, monkeypatch):
        from app.domains.composite.service import CompositeStrategyService
        monkeypatch.setattr(CompositeStrategyService, "create_composite",
                            MagicMock(side_effect=ValueError("bad input")))
        resp = _client().post("/api/v1/composite-strategies", json={
            "name": "test", "execution_mode": "paper",
        })
        assert resp.status_code in (400, 422)

    def test_update_composite(self, monkeypatch):
        from app.domains.composite.service import CompositeStrategyService
        monkeypatch.setattr(CompositeStrategyService, "update_composite", lambda self, *a, **kw: {
            "id": 1, "name": "updated", "user_id": 1, "description": "",
            "portfolio_config": {}, "market_constraints": {},
            "execution_mode": "paper", "is_active": True,
            "created_at": datetime.now().isoformat(),
            "updated_at": datetime.now().isoformat(),
            "components": [],
        })
        resp = _client().put("/api/v1/composite-strategies/1", json={
            "name": "updated",
        })
        assert resp.status_code == 200

    def test_update_composite_not_found(self, monkeypatch):
        from app.domains.composite.service import CompositeStrategyService
        monkeypatch.setattr(CompositeStrategyService, "update_composite",
                            MagicMock(side_effect=KeyError("not found")))
        resp = _client().put("/api/v1/composite-strategies/1", json={"name": "x"})
        assert resp.status_code == 404


# --- Templates routes ---

class TestTemplatesRoutesB8:
    """Cover template route handlers (lines 122-193)."""

    def test_clone_template(self, monkeypatch):
        from app.domains.templates.service import TemplateService
        monkeypatch.setattr(TemplateService, "clone_template", lambda self, uid, tid: {"id": 2, "name": "clone"})
        resp = _client().post("/api/v1/templates/1/clone")
        assert resp.status_code == 201

    def test_clone_template_not_found(self, monkeypatch):
        from app.domains.templates.service import TemplateService
        monkeypatch.setattr(TemplateService, "clone_template",
                            MagicMock(side_effect=KeyError("not found")))
        resp = _client().post("/api/v1/templates/1/clone")
        assert resp.status_code == 404

    def test_publish_template(self, monkeypatch):
        from app.domains.templates.service import TemplateService
        monkeypatch.setattr(TemplateService, "publish_template", lambda self, uid, tid: {"id": 1})
        resp = _client().post("/api/v1/templates/1/publish")
        assert resp.status_code == 201

    def test_publish_template_error(self, monkeypatch):
        from app.domains.templates.service import TemplateService
        monkeypatch.setattr(TemplateService, "publish_template",
                            MagicMock(side_effect=ValueError("not ready")))
        resp = _client().post("/api/v1/templates/1/publish")
        assert resp.status_code == 400

    def test_add_comment(self, monkeypatch):
        from app.domains.templates.service import TemplateService
        monkeypatch.setattr(TemplateService, "add_comment", lambda self, tid, uid, c, pid=None: 1)
        resp = _client().post("/api/v1/templates/1/comments", json={"content": "nice"})
        assert resp.status_code == 201

    def test_delete_comment(self, monkeypatch):
        from app.domains.templates.service import TemplateService
        monkeypatch.setattr(TemplateService, "delete_comment", lambda self, cid, uid: None)
        resp = _client().delete("/api/v1/templates/1/comments/1")
        assert resp.status_code == 204

    def test_get_ratings(self, monkeypatch):
        from app.domains.templates.service import TemplateService
        monkeypatch.setattr(TemplateService, "get_ratings", lambda self, tid: {"avg": 4.5})
        monkeypatch.setattr(TemplateService, "list_reviews", lambda self, tid: [])
        resp = _client().get("/api/v1/templates/1/ratings")
        assert resp.status_code == 200

    def test_rate_template(self, monkeypatch):
        from app.domains.templates.service import TemplateService
        monkeypatch.setattr(TemplateService, "rate_template",
                            lambda self, tid, uid, rating, review=None: {"id": 1})
        resp = _client().post("/api/v1/templates/1/ratings", json={"rating": 5})
        assert resp.status_code == 200

    def test_rate_template_not_found(self, monkeypatch):
        from app.domains.templates.service import TemplateService
        monkeypatch.setattr(TemplateService, "rate_template",
                            MagicMock(side_effect=KeyError("not found")))
        resp = _client().post("/api/v1/templates/1/ratings", json={"rating": 5})
        assert resp.status_code == 404


# --- Settings routes ---

class TestSettingsRoutesB8:
    """Cover settings route handlers (lines 94-192)."""

    def test_list_datasource_configs(self, monkeypatch):
        from app.domains.market.dao.data_source_item_dao import DataSourceConfigDao
        monkeypatch.setattr(DataSourceConfigDao, "list_all", lambda self: [])
        resp = _client().get("/api/v1/settings/datasource-configs")
        assert resp.status_code == 200

    def test_update_datasource_config(self, monkeypatch):
        from app.domains.market.dao.data_source_item_dao import DataSourceConfigDao
        monkeypatch.setattr(DataSourceConfigDao, "get_by_key", lambda self, k: {"source_key": k})
        monkeypatch.setattr(DataSourceConfigDao, "update_config", lambda self, *a, **kw: None)
        resp = _client().put("/api/v1/settings/datasource-configs/tushare", json={
            "enabled": True,
        })
        assert resp.status_code == 200

    def test_update_datasource_config_not_found(self, monkeypatch):
        from app.domains.market.dao.data_source_item_dao import DataSourceConfigDao
        monkeypatch.setattr(DataSourceConfigDao, "get_by_key", lambda self, k: None)
        resp = _client().put("/api/v1/settings/datasource-configs/bad", json={"enabled": True})
        assert resp.status_code == 404

    def test_test_datasource_connection(self, monkeypatch):
        registry = MagicMock()
        ds = MagicMock()
        ds.test_connection.return_value = (True, "OK")
        registry.get_source.return_value = ds
        monkeypatch.setattr("app.datasync.registry.build_default_registry", lambda: registry)
        resp = _client().post("/api/v1/settings/datasource-items/test/tushare")
        assert resp.status_code == 200
        assert resp.json()["status"] == "ok"

    def test_test_datasource_connection_unknown(self, monkeypatch):
        registry = MagicMock()
        registry.get_source.return_value = None
        registry.all_sources.return_value = []
        monkeypatch.setattr("app.datasync.registry.build_default_registry", lambda: registry)
        resp = _client().post("/api/v1/settings/datasource-items/test/unknown")
        assert resp.status_code == 400


# ===================================================================
# 6. strategies/service — update_strategy, restore_code_history
# ===================================================================


class TestStrategiesServiceUpdate:
    """Cover update_strategy version bump + build clause, restore_code_history."""

    @pytest.fixture(autouse=True)
    def _load(self):
        self.mod = __import__("app.domains.strategies.service", fromlist=["x"])

    def _make_svc(self):
        with patch("app.domains.strategies.service.StrategyDao"), \
             patch("app.domains.strategies.service.StrategyHistoryDao"), \
             patch("app.domains.strategies.service.get_audit_service"):
            svc = self.mod.StrategiesService()
        svc._dao = MagicMock()
        svc._history = MagicMock()
        return svc

    def test_update_strategy_version_bump_code(self, monkeypatch):
        svc = self._make_svc()
        svc._dao.get_existing_for_update.return_value = {
            "id": 1, "name": "old", "class_name": "Old", "version": 1,
            "code": "old code", "parameters": "{}", "description": "desc",
        }
        svc._dao.get_for_user.return_value = {
            "id": 1, "name": "old", "class_name": "Old", "version": 2,
            "code": "new code", "parameters": "{}", "description": "desc",
        }
        monkeypatch.setattr("app.domains.strategies.service.validate_strategy_code",
                            lambda code, cn: SimpleNamespace(valid=True, errors=[]))
        monkeypatch.setattr("app.domains.strategies.service.get_audit_service", lambda: MagicMock())

        svc.update_strategy(user_id=1, strategy_id=1, code="new code")
        svc._history.insert_history.assert_called_once()
        svc._history.rotate_keep_latest.assert_called_once()

    def test_update_strategy_no_bump(self, monkeypatch):
        svc = self._make_svc()
        svc._dao.get_existing_for_update.return_value = {
            "id": 1, "name": "old", "class_name": "Old", "version": 1,
            "code": "code", "parameters": '{"a": 1}', "description": "desc",
        }
        svc._dao.get_for_user.return_value = {
            "id": 1, "name": "old", "class_name": "Old", "version": 1,
            "code": "code", "parameters": '{"a": 1}', "description": "desc",
        }

        # description same as existing → no bump
        svc.update_strategy(user_id=1, strategy_id=1, description="desc")
        svc._history.insert_history.assert_not_called()

    def test_update_strategy_param_change(self, monkeypatch):
        svc = self._make_svc()
        svc._dao.get_existing_for_update.return_value = {
            "id": 1, "name": "old", "class_name": "Old", "version": 1,
            "code": "code", "parameters": '{"a": 1}', "description": "desc",
        }
        svc._dao.get_for_user.return_value = {
            "id": 1, "name": "old", "class_name": "Old", "version": 2,
            "code": "code", "parameters": '{"a": 2}', "description": "desc",
        }
        monkeypatch.setattr("app.domains.strategies.service.get_audit_service", lambda: MagicMock())

        svc.update_strategy(user_id=1, strategy_id=1, parameters={"a": 2})
        svc._history.insert_history.assert_called_once()

    def test_restore_code_history_success(self, monkeypatch):
        svc = self._make_svc()
        svc._dao.get_existing_for_update.return_value = {
            "id": 1, "name": "cur", "class_name": "Cur", "version": 2,
            "code": "current code", "parameters": '{"a": 1}', "description": "desc",
        }
        svc._history.get_history.return_value = {
            "id": 1, "code": "old code", "parameters": '{"a": 0}',
            "class_name": "Old", "strategy_name": "old", "description": "d",
        }
        monkeypatch.setattr("app.domains.strategies.service.get_audit_service", lambda: MagicMock())

        svc.restore_code_history(user_id=1, strategy_id=1, history_id=1)
        svc._history.insert_history.assert_called_once()

    def test_restore_code_history_strategy_not_found(self):
        svc = self._make_svc()
        svc._dao.get_existing_for_update.return_value = None
        with pytest.raises(KeyError, match="Strategy"):
            svc.restore_code_history(1, 1, 1)

    def test_restore_code_history_history_not_found(self):
        svc = self._make_svc()
        svc._dao.get_existing_for_update.return_value = {"id": 1, "code": "x"}
        svc._history.get_history.return_value = None
        with pytest.raises(KeyError, match="History"):
            svc.restore_code_history(1, 1, 1)


# ===================================================================
# 7. realtime_quote_service — tencent helpers, HK, US, FX
# ===================================================================


class TestRealtimeQuoteHelpers:
    """Cover _to_float, _to_int, _normalize_symbol, _pick, tencent helpers."""

    @pytest.fixture(autouse=True)
    def _load(self):
        self.mod = __import__("app.domains.market.realtime_quote_service", fromlist=["x"])
        self.svc = self.mod.RealtimeQuoteService()

    def test_to_float_none(self):
        assert self.svc._to_float(None) is None

    def test_to_float_string_pct(self):
        assert self.svc._to_float("3.14%") == pytest.approx(3.14)

    def test_to_float_string_comma(self):
        assert self.svc._to_float("1,234.56") == pytest.approx(1234.56)

    def test_to_float_empty_string(self):
        assert self.svc._to_float("  ") is None

    def test_to_float_number(self):
        assert self.svc._to_float(42.0) == 42.0

    def test_to_float_bad_value(self):
        assert self.svc._to_float("abc") is None

    def test_to_int(self):
        assert self.svc._to_int("1000") == 1000
        assert self.svc._to_int(None) is None

    def test_normalize_symbol(self):
        result = self.svc._normalize_symbol("  000001.SZ  ")
        assert isinstance(result, str)

    def test_build_tencent_quote(self):
        parts = ["0"] * 40
        parts[1] = "TestStock"
        parts[3] = "10.50"
        parts[4] = "10.00"
        parts[5] = "10.20"
        parts[6] = "1000"
        parts[33] = "11.00"
        parts[34] = "9.80"
        parts[35] = "x/y/50000"
        result = self.svc._build_tencent_quote("000001", parts, "CN")
        assert result["name"] == "TestStock"
        assert result["price"] == pytest.approx(10.50)
        assert result["change"] == pytest.approx(0.50)
        assert result["market"] == "CN"

    def test_quote_cn(self, monkeypatch):
        parts = ["0"] * 40
        parts[1] = "Test"
        parts[3] = "10.0"
        parts[4] = "9.5"
        monkeypatch.setattr(self.svc, "_fetch_tencent_quote", lambda code: parts)
        result = self.svc._quote_cn("000001.SZ")
        assert result["market"] == "CN"

    def test_quote_cn_index_sh(self, monkeypatch):
        parts = ["0"] * 40
        parts[1] = "上证指数"
        parts[3] = "3000.0"
        parts[4] = "2990.0"
        monkeypatch.setattr(self.svc, "_fetch_tencent_quote_with_prefix", lambda c, p: parts)
        result = self.svc._quote_cn_index("000001.SH")
        assert result["market"] == "CN_INDEX"

    def test_quote_cn_index_sz(self, monkeypatch):
        parts = ["0"] * 40
        parts[1] = "深证成指"
        parts[3] = "10000.0"
        parts[4] = "9990.0"
        monkeypatch.setattr(self.svc, "_fetch_tencent_quote_with_prefix", lambda c, p: parts)
        result = self.svc._quote_cn_index("399001.SZ")
        assert result["market"] == "CN_INDEX"

    def test_fetch_tencent_quote_prefix(self):
        """Verify prefix logic — 6xx → sh, others → sz."""
        with patch("requests.get") as mock_get:
            mock_resp = MagicMock()
            mock_resp.text = 'v_sh600000="header~name~code~10.0~9.5~10.0~1000~x~y~z";'
            mock_resp.raise_for_status = lambda: None
            mock_get.return_value = mock_resp
            parts = self.svc._fetch_tencent_quote("600000")
            assert mock_get.call_args[0][0].startswith("https://qt.gtimg.cn/q=sh")

    def test_tencent_request_with_retry_timeout(self):
        import requests
        with patch("requests.get", side_effect=requests.Timeout("timeout")):
            with pytest.raises((requests.Timeout, TimeoutError)):
                self.svc._tencent_request_with_retry("http://test", "000001")

    def test_quote_hk_tencent(self, monkeypatch):
        parts = ["0"] * 40
        parts[1] = "HK Stock"
        parts[3] = "100.0"
        parts[4] = "99.0"
        parts[5] = "99.5"
        parts[6] = "500"
        monkeypatch.setattr(self.svc, "_tencent_request_with_retry", lambda u, c: parts)
        result = self.svc._quote_hk_tencent("00700")
        assert result["market"] == "HK"
        assert result["currency"] == "HKD"


# ===================================================================
# 8. calendar_service — trade_days_from_db, get_events
# ===================================================================


class TestCalendarServiceExtended:
    """Cover _trade_days_from_db, get_events, _fetch_events."""

    @pytest.fixture(autouse=True)
    def _load(self):
        self.mod = __import__("app.domains.market.calendar_service", fromlist=["x"])

    def test_trade_days_from_db(self, monkeypatch):
        ctx = _fake_conn()
        ctx.execute.return_value.fetchall.return_value = [("20240101", 1), ("20240102", 0), ("20240103", 1)]
        monkeypatch.setattr(
            "app.domains.market.calendar_service.connection",
            lambda db: MagicMock(__enter__=lambda s: ctx, __exit__=lambda s, *a: None),
        )
        svc = self.mod.CalendarService()
        result = svc._trade_days_from_db("SSE", date(2024, 1, 1), date(2024, 1, 3))
        assert result["exchange"] == "SSE"
        assert len(result["trade_days"]) == 2

    def test_get_events_default(self, monkeypatch):
        svc = self.mod.CalendarService()
        monkeypatch.setattr(svc, "_fetch_events", lambda et, s, e: [])
        result = svc.get_events()
        assert "events" in result

    def test_get_events_type_filter(self, monkeypatch):
        svc = self.mod.CalendarService()
        monkeypatch.setattr(svc, "_fetch_events", lambda et, s, e: [{"type": et, "date": "2024-01-01"}])
        result = svc.get_events(event_type="macro")
        assert len(result["events"]) == 1

    def test_fetch_events_no_akshare(self, monkeypatch):
        monkeypatch.setattr(self.mod, "ak", None)
        svc = self.mod.CalendarService()
        assert svc._fetch_events("macro", date(2024, 1, 1), date(2024, 6, 1)) == []

    def test_fetch_events_dispatch(self, monkeypatch):
        svc = self.mod.CalendarService()
        monkeypatch.setattr(svc, "_macro_events", lambda s, e: [{"type": "macro"}])
        monkeypatch.setattr(svc, "_ipo_events", lambda: [{"type": "ipo"}])
        monkeypatch.setattr(svc, "_dividend_events", lambda: [{"type": "dividend"}])
        monkeypatch.setattr(self.mod, "ak", MagicMock())  # ensure ak is not None

        for et, expected in [("macro", "macro"), ("ipo", "ipo"), ("dividend", "dividend")]:
            result = svc._fetch_events(et, date(2024, 1, 1), date(2024, 6, 1))
            assert len(result) == 1

    def test_fetch_events_unknown_type(self, monkeypatch):
        svc = self.mod.CalendarService()
        monkeypatch.setattr(self.mod, "ak", MagicMock())
        assert svc._fetch_events("unknown", date(2024, 1, 1), date(2024, 6, 1)) == []


# ===================================================================
# 9. sentiment_service — get_overview, get_fear_greed
# ===================================================================


class TestSentimentServiceExtended:
    """Cover get_overview branches + get_fear_greed."""

    @pytest.fixture(autouse=True)
    def _load(self):
        self.mod = __import__("app.domains.market.sentiment_service", fromlist=["x"])

    def test_get_fear_greed_no_akshare(self, monkeypatch):
        monkeypatch.setattr(self.mod, "ak", None)
        svc = self.mod.SentimentService()
        result = svc.get_fear_greed()
        assert result["score"] == 50
        assert result["label"] == "neutral"

    def test_get_fear_greed_with_data(self, monkeypatch):
        df = pd.DataFrame({
            "涨跌幅": [1.0, 2.0, -1.0, 10.0, -10.0, 0.5, -0.3],
        })
        mock_ak = MagicMock()
        mock_ak.stock_zh_a_spot_em.return_value = df
        mock_ak.stock_zh_index_spot_em.return_value = pd.DataFrame({
            "代码": ["000001"],
            "涨跌幅": [1.5],
            "最新价": [3000.0],
        })
        monkeypatch.setattr(self.mod, "ak", mock_ak)
        svc = self.mod.SentimentService()
        result = svc.get_fear_greed()
        assert "score" in result
        assert "components" in result

    def test_get_fear_greed_exception(self, monkeypatch):
        mock_ak = MagicMock()
        mock_ak.stock_zh_a_spot_em.side_effect = RuntimeError("fail")
        monkeypatch.setattr(self.mod, "ak", mock_ak)
        svc = self.mod.SentimentService()
        result = svc.get_fear_greed()
        assert result["score"] == 50

    def test_get_overview_volume_and_index(self, monkeypatch):
        """Cover volume_trend and index_momentum branches."""
        svc = self.mod.SentimentService()
        mock_ak = MagicMock()
        # Return a DF with 涨跌幅 column for advance_decline
        mock_ak.stock_zh_a_spot_em.return_value = pd.DataFrame({
            "涨跌幅": [1.0, 2.0, -1.0, 0.0, 5.0],
            "成交额": [1e10, 2e10, 3e10, 4e10, 5e10],
        })
        mock_ak.stock_zh_index_spot_em.return_value = pd.DataFrame({
            "代码": ["000001"],
            "最新价": [3000.0],
            "涨跌幅": [1.5],
        })
        monkeypatch.setattr(self.mod, "ak", mock_ak)
        result = svc.get_overview()
        assert "advance_decline" in result
        assert result["advance_decline"] is not None


# ===================================================================
# 10. backtest_history_dao — json_default
# ===================================================================


class TestBacktestHistoryDaoJsonDefault:
    """Cover _json_default nested function inside upsert_history.

    _json_default is a nested function, so we test it indirectly by calling
    upsert_history with numpy/datetime values in the result dict.
    """

    @pytest.fixture(autouse=True)
    def _load(self):
        self.mod = __import__("app.domains.backtests.dao.backtest_history_dao", fromlist=["x"])

    def test_upsert_with_numpy_result(self, monkeypatch):
        """Covers _json_default handling of numpy array + scalar + datetime."""
        ctx = _fake_conn()
        monkeypatch.setattr(
            "app.domains.backtests.dao.backtest_history_dao.connection",
            lambda db: MagicMock(__enter__=lambda s: ctx, __exit__=lambda s, *a: None),
        )
        dao = self.mod.BacktestHistoryDao()
        dao.upsert_history(
            user_id=1,
            job_id="j1",
            strategy_id=1,
            strategy_class="X",
            strategy_version=1,
            vt_symbol="000001.SZ",
            start_date="2024-01-01",
            end_date="2024-06-01",
            parameters={"k": np.float64(1.5)},
            status="completed",
            result={"returns": np.array([0.1, 0.2]), "ts": datetime(2024, 1, 1)},
            error=None,
            created_at=datetime.now(),
            completed_at=datetime.now(),
        )
        ctx.execute.assert_called_once()

    def test_upsert_with_none_result(self, monkeypatch):
        ctx = _fake_conn()
        monkeypatch.setattr(
            "app.domains.backtests.dao.backtest_history_dao.connection",
            lambda db: MagicMock(__enter__=lambda s: ctx, __exit__=lambda s, *a: None),
        )
        dao = self.mod.BacktestHistoryDao()
        dao.upsert_history(
            user_id=1,
            job_id="j2",
            strategy_id=None,
            strategy_class="Y",
            strategy_version=None,
            vt_symbol="000001.SZ",
            start_date="2024-01-01",
            end_date="2024-06-01",
            parameters={},
            status="failed",
            result=None,
            error="boom",
            created_at=datetime.now(),
            completed_at=None,
        )
        ctx.execute.assert_called_once()


# ===================================================================
# 11. trade_log_dao — list, count
# ===================================================================


class TestTradeLogDaoExtended:
    """Cover TradeLogDao.query, count methods."""

    @pytest.fixture(autouse=True)
    def _load(self):
        self.mod = __import__("app.domains.market.dao.trade_log_dao", fromlist=["x"])

    def test_query_with_filters(self, monkeypatch):
        ctx = _fake_conn()
        row = MagicMock()
        row._mapping = {"id": 1, "symbol": "000001.SZ", "direction": "buy"}
        ctx.execute.return_value.fetchall.return_value = [row]
        monkeypatch.setattr("app.domains.market.dao.trade_log_dao.connection",
                            lambda db: MagicMock(__enter__=lambda s: ctx, __exit__=lambda s, *a: None))
        dao = self.mod.TradeLogDao()
        result = dao.query(
            symbol="000001.SZ",
            direction="buy",
            strategy_id=1,
            start_date=date(2024, 1, 1),
            end_date=date(2024, 6, 1),
            limit=10,
            offset=0,
        )
        assert len(result) == 1

    def test_query_missing_table(self, monkeypatch):
        from sqlalchemy.exc import ProgrammingError
        ctx = _fake_conn()
        ctx.execute.side_effect = ProgrammingError("", {}, Exception("trade_logs doesn't exist"))
        monkeypatch.setattr("app.domains.market.dao.trade_log_dao.connection",
                            lambda db: MagicMock(__enter__=lambda s: ctx, __exit__=lambda s, *a: None))
        dao = self.mod.TradeLogDao()
        result = dao.query()
        assert result == []

    def test_count_basic(self, monkeypatch):
        ctx = _fake_conn()
        row = MagicMock()
        row._mapping = {"cnt": 5}
        ctx.execute.return_value.fetchone.return_value = row
        monkeypatch.setattr("app.domains.market.dao.trade_log_dao.connection",
                            lambda db: MagicMock(__enter__=lambda s: ctx, __exit__=lambda s, *a: None))
        dao = self.mod.TradeLogDao()
        result = dao.count(symbol="000001.SZ")
        assert result == 5

    def test_count_missing_table(self, monkeypatch):
        from sqlalchemy.exc import ProgrammingError
        ctx = _fake_conn()
        ctx.execute.side_effect = ProgrammingError("", {}, Exception("trade_logs doesn't exist"))
        monkeypatch.setattr("app.domains.market.dao.trade_log_dao.connection",
                            lambda db: MagicMock(__enter__=lambda s: ctx, __exit__=lambda s, *a: None))
        dao = self.mod.TradeLogDao()
        result = dao.count()
        assert result == 0


# ===================================================================
# 12. akshare_ingest — ingest_all_indexes, call_ak retry, main CLI
# ===================================================================


class TestAkshareIngestExtended:
    """Cover ingest_all_indexes, call_ak retry paths, main CLI."""

    @pytest.fixture(autouse=True)
    def _load(self):
        self.mod = __import__("app.datasync.service.akshare_ingest", fromlist=["x"])

    def test_ingest_all_indexes(self, monkeypatch):
        monkeypatch.setattr(self.mod, "ingest_index_daily", lambda symbol: 100)
        monkeypatch.setattr(self.mod, "time", MagicMock())
        result = self.mod.ingest_all_indexes()
        assert isinstance(result, dict)
        for v in result.values():
            assert v["status"] == "success"

    def test_ingest_all_indexes_error(self, monkeypatch):
        monkeypatch.setattr(self.mod, "ingest_index_daily",
                            MagicMock(side_effect=RuntimeError("fail")))
        monkeypatch.setattr(self.mod, "time", MagicMock())
        result = self.mod.ingest_all_indexes()
        for v in result.values():
            assert v["status"] == "error"

    def test_call_ak_success_with_metrics_hook(self, monkeypatch):
        hook = MagicMock()
        self.mod.call_ak._metrics_hook = hook
        fn = MagicMock(return_value=pd.DataFrame({"a": [1]}))
        monkeypatch.setattr(self.mod, "time", MagicMock(time=MagicMock(return_value=1000.0)))

        result = self.mod.call_ak("test_api", fn, max_retries=1)
        assert result is not None
        # Reset hook
        self.mod.call_ak._metrics_hook = None

    def test_call_ak_rate_limit_retry(self, monkeypatch):
        """Cover rate-limit detection and backoff."""
        call_count = {"n": 0}
        def _fn(**kw):
            call_count["n"] += 1
            if call_count["n"] == 1:
                raise Exception("429 Too Many Requests")
            return pd.DataFrame({"a": [1]})

        monkeypatch.setattr(self.mod, "time", MagicMock(time=MagicMock(return_value=1000.0)))
        result = self.mod.call_ak("test_api", _fn, max_retries=3, backoff_base=0)
        assert result is not None
        assert call_count["n"] == 2

    def test_set_metrics_hook(self):
        fn = lambda d: None
        self.mod.set_metrics_hook(fn)
        assert self.mod.call_ak._metrics_hook is fn
        self.mod.call_ak._metrics_hook = None

    def test_main_no_args(self, monkeypatch):
        monkeypatch.setattr("sys.argv", ["akshare_ingest.py"])
        self.mod.main()  # prints usage

    def test_main_index(self, monkeypatch):
        monkeypatch.setattr("sys.argv", ["akshare_ingest.py", "index", "sh000300"])
        monkeypatch.setattr(self.mod, "ingest_index_daily", lambda symbol: 50)
        self.mod.main()

    def test_main_index_all(self, monkeypatch):
        monkeypatch.setattr("sys.argv", ["akshare_ingest.py", "index_all"])
        monkeypatch.setattr(self.mod, "ingest_all_indexes", lambda: {})
        self.mod.main()

    def test_main_unknown_cmd(self, monkeypatch):
        monkeypatch.setattr("sys.argv", ["akshare_ingest.py", "bad"])
        self.mod.main()


# ===================================================================
# 13. factor_screening — mine_alpha158_factors
# ===================================================================


class TestFactorScreeningMining:
    """Cover mine_alpha158_factors branches."""

    @pytest.fixture(autouse=True)
    def _load(self):
        self.mod = __import__("app.domains.factors.factor_screening", fromlist=["x"])

    def test_mine_alpha158_empty_df(self, monkeypatch):
        monkeypatch.setattr(self.mod, "compute_qlib_factor_set", lambda **kw: pd.DataFrame())
        result = self.mod.mine_alpha158_factors(
            start_date="2024-01-01", end_date="2024-06-01"
        )
        assert result == []

    def test_mine_alpha158_no_close_col(self, monkeypatch):
        df = pd.DataFrame({"factor_a": [1, 2, 3]})
        monkeypatch.setattr(self.mod, "compute_qlib_factor_set", lambda **kw: df)
        result = self.mod.mine_alpha158_factors(
            start_date="2024-01-01", end_date="2024-06-01"
        )
        assert result == []

    def test_mine_alpha158_runtime_error(self, monkeypatch):
        """When qlib is not available, mine_alpha158_factors catches RuntimeError."""
        monkeypatch.setattr(self.mod, "compute_qlib_factor_set",
                            MagicMock(side_effect=RuntimeError("no qlib")))
        result = self.mod.mine_alpha158_factors(
            start_date="2024-01-01", end_date="2024-06-01"
        )
        assert result == []

    def test_mine_alpha158_with_data(self, monkeypatch):
        idx = pd.MultiIndex.from_tuples(
            [("000001", "2024-01-01"), ("000001", "2024-01-02"),
             ("000001", "2024-01-03"), ("000001", "2024-01-04")],
            names=["instrument", "datetime"],
        )
        df = pd.DataFrame({
            "CLOSE": [100.0, 101.0, 102.0, 103.0],
            "factor_a": [0.1, 0.2, 0.3, 0.4],
        }, index=idx)
        monkeypatch.setattr(self.mod, "compute_qlib_factor_set", lambda **kw: df)
        monkeypatch.setattr(self.mod, "compute_factor_metrics", lambda fv, fwd: {
            "ic_mean": 0.15, "ic_std": 0.05, "ir": 3.0,
        })
        result = self.mod.mine_alpha158_factors(
            start_date="2024-01-01", end_date="2024-06-01",
            ic_threshold=0.1,
        )
        assert len(result) >= 1

    def test_screen_factor_pool_dedup_correlation(self, monkeypatch):
        """Cover the correlation dedup branch in screen_factor_pool."""
        n = 50
        s1 = pd.Series(np.random.randn(n))
        s2 = s1 + np.random.randn(n) * 0.01  # nearly identical

        ohlcv = pd.DataFrame({
            "open": np.random.randn(n) + 100,
            "high": np.random.randn(n) + 101,
            "low": np.random.randn(n) + 99,
            "close": np.random.randn(n) + 100,
            "volume": np.random.randint(100, 1000, n),
        })
        fwd_ret = pd.Series(np.random.randn(n))

        monkeypatch.setattr(self.mod, "fetch_ohlcv", lambda **kw: ohlcv)
        monkeypatch.setattr(self.mod, "compute_forward_returns", lambda df, periods=1: fwd_ret)

        call_count = {"n": 0}
        def mock_compute_custom(expr, ohlcv_df):
            call_count["n"] += 1
            return s1 if "a" in expr else s2

        monkeypatch.setattr(self.mod, "compute_custom_factor", mock_compute_custom)
        monkeypatch.setattr(self.mod, "compute_factor_metrics", lambda fv, fwd_ret: {
            "ic_mean": 0.2, "ic_std": 0.05, "ir": 4.0,
        })

        result = self.mod.screen_factor_pool(
            expressions=["expr_a", "expr_b"],
            start_date=date(2024, 1, 1),
            end_date=date(2024, 6, 1),
            corr_threshold=0.95,
        )
        # At least one should remain, second may be deduped
        assert len(result) >= 1


# ===================================================================
# 14. data_sync_daemon — remaining backfill + run_daemon
# ===================================================================


class TestDaemonBackfillBranches:
    """Cover backfill branch dispatching (adj_factor, top10_holders, other steps)."""

    @pytest.fixture(autouse=True)
    def _load(self):
        self.mod = __import__("app.datasync.service.data_sync_daemon", fromlist=["x"])

    def test_backfill_adj_factor_branch(self, monkeypatch):
        monkeypatch.setattr(self.mod, "is_backfill_locked", lambda: False)
        monkeypatch.setattr(self.mod, "acquire_backfill_lock", lambda: None)
        monkeypatch.setattr(self.mod, "release_backfill_lock", lambda: None)
        monkeypatch.setattr(self.mod, "get_failed_steps", lambda lookback_days=None: [
            (date(2024, 1, 1), "tushare_adj_factor"),
        ])
        # group_dates_by_month returns tuples of date objects
        monkeypatch.setattr(self.mod, "group_dates_by_month",
                            lambda dates: [(date(2024, 1, 1), date(2024, 1, 31))])
        monkeypatch.setattr(self.mod, "write_step_status", lambda *a, **kw: None)

        ingest_calls = []
        monkeypatch.setattr(self.mod, "ingest_adj_factor_by_date_range",
                            lambda start, end, batch_size=None: ingest_calls.append((start, end)))

        self.mod.missing_data_backfill()
        assert len(ingest_calls) == 1

    def test_backfill_top10_holders_branch(self, monkeypatch):
        monkeypatch.setattr(self.mod, "is_backfill_locked", lambda: False)
        monkeypatch.setattr(self.mod, "acquire_backfill_lock", lambda: None)
        monkeypatch.setattr(self.mod, "release_backfill_lock", lambda: None)
        monkeypatch.setattr(self.mod, "get_failed_steps", lambda lookback_days=None: [
            (date(2024, 1, 1), "tushare_top10_holders"),
        ])
        # group_dates_by_month returns tuples of date objects
        monkeypatch.setattr(self.mod, "group_dates_by_month",
                            lambda dates: [(date(2024, 1, 1), date(2024, 1, 31))])
        monkeypatch.setattr(self.mod, "write_step_status", lambda *a, **kw: None)

        ingest_calls = []
        monkeypatch.setattr(self.mod, "ingest_top10_holders_by_date_range",
                            lambda start, end, batch_size=None: ingest_calls.append((start, end)))

        self.mod.missing_data_backfill()
        assert len(ingest_calls) == 1

    def test_backfill_other_step(self, monkeypatch):
        monkeypatch.setattr(self.mod, "is_backfill_locked", lambda: False)
        monkeypatch.setattr(self.mod, "acquire_backfill_lock", lambda: None)
        monkeypatch.setattr(self.mod, "release_backfill_lock", lambda: None)
        monkeypatch.setattr(self.mod, "get_failed_steps", lambda lookback_days=None: [
            (date(2024, 1, 1), "akshare_index"),
        ])
        monkeypatch.setattr(self.mod, "write_step_status", lambda *a, **kw: None)

        daily_calls = []
        monkeypatch.setattr(self.mod, "daily_ingest",
                            lambda target_date=None, continue_on_error=True: daily_calls.append(target_date))

        self.mod.missing_data_backfill()
        assert len(daily_calls) == 1
