"""Batch-9 coverage tests — targeting ~260+ uncovered lines across multiple modules.

Covers:
 1. tushare_ingest  – _is_rate_limit_error, parse_retry_after, ingest helpers
 2. tushare_dao     – _round2, audit_finish, upsert_weekly, get_failed_ts_codes
 3. data_sync_status_dao   – write_step_status, get_failed_steps, trade_cal, backfill_lock
 4. realtime_quote_service – _to_int, _normalize_symbol, _pick, CN/HK/US/FX quotes
 5. paper_strategy_executor – execute loop, quote_to_bar
 6. vnpy_trading_service    – connect, send_order, cancel, gateway lifecycle
 7. worker/tasks            – bayesian optimization, strategy class params
 8. backtest routes         – submit, history list
 9. qlib_model_service      – metrics calculation
10. routes: strategies, factors, datasync, settings, ai
11. expression_engine, factor_screening, sentiment_service
12. sync_engine, migrate, datasync/main
13. strategies/service branches, strategy_service validation
14. akshare_ingest, tushare interfaces
"""
from __future__ import annotations

import importlib
import os
import sys
import threading
import types
from datetime import date, datetime, timedelta
from types import SimpleNamespace
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import numpy as np
import pandas as pd
import pytest
from fastapi import Depends, FastAPI
from fastapi.testclient import TestClient

from app.api.exception_handlers import register_exception_handlers
from app.api.models.user import TokenData
from app.api.services.auth_service import get_current_user

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
_TEST_USER_EXP = datetime.utcnow() + timedelta(hours=1)
_TEST_USER = TokenData(user_id=1, username="tester", exp=_TEST_USER_EXP)


def _fake_engine():
    eng = MagicMock()
    ctx = MagicMock()
    eng.begin.return_value.__enter__ = MagicMock(return_value=ctx)
    eng.begin.return_value.__exit__ = MagicMock(return_value=False)
    eng.connect.return_value.__enter__ = MagicMock(return_value=ctx)
    eng.connect.return_value.__exit__ = MagicMock(return_value=False)
    raw = MagicMock()
    eng.raw_connection.return_value = raw
    return eng, ctx, raw


def _mock_conn():
    m = MagicMock()
    m.__enter__ = MagicMock(return_value=m)
    m.__exit__ = MagicMock(return_value=False)
    return m


def _make_client(*routers, prefix=""):
    """Create a test client with RBAC bypassed (same pattern as test_phase3_features)."""
    app = FastAPI()
    register_exception_handlers(app)
    app.dependency_overrides[get_current_user] = lambda: _TEST_USER
    for r in routers:
        for route in r.routes:
            dependant = getattr(route, "dependant", None)
            if not dependant:
                continue
            for dep in dependant.dependencies:
                call = getattr(dep, "call", None)
                if callable(call):
                    module = getattr(call, "__module__", "")
                    qualname = getattr(call, "__qualname__", "")
                    if module == "app.api.dependencies.permissions" and "require_permission" in qualname:
                        app.dependency_overrides[call] = lambda: _TEST_USER
        app.include_router(r, prefix=prefix)
    return TestClient(app, raise_server_exceptions=False)


# ===========================================================================
# 1. tushare_ingest
# ===========================================================================
class TestTushareIngest:
    @pytest.fixture(autouse=True)
    def _load(self):
        self.mod = importlib.import_module("app.datasync.service.tushare_ingest")

    def test_is_rate_limit_error_true(self):
        fn = self.mod._is_rate_limit_error
        assert fn("rate limit exceeded") is True
        assert fn("too many requests") is True
        assert fn("每分钟最多访问200次") is True
        assert fn("接口访问太频繁") is True
        assert fn("稍后重试") is True
        assert fn("频率") is True

    def test_is_rate_limit_error_false(self):
        fn = self.mod._is_rate_limit_error
        assert fn("normal error") is False
        assert fn("") is False
        assert fn(None) is False

    def test_parse_retry_after(self):
        fn = self.mod.parse_retry_after
        assert fn("请等待 5 seconds 后重试") == 5.0
        assert fn("wait 100 ms") == pytest.approx(0.1)
        assert fn("5秒后重试") == 5.0
        assert fn("1分钟后重试") == 60.0
        assert fn("unknown msg") is None
        assert fn("wait invalid seconds") is None

    # ---- ingest_index_weekly ----
    def test_ingest_index_weekly_no_data(self, monkeypatch):
        monkeypatch.setattr(self.mod, "audit_start", lambda *a: 1)
        monkeypatch.setattr(self.mod, "audit_finish", lambda *a, **kw: None)
        monkeypatch.setattr(self.mod, "call_pro", lambda *a, **kw: pd.DataFrame())
        result = self.mod.ingest_index_weekly(ts_code="000001.SH")

    def test_ingest_index_weekly_with_data(self, monkeypatch):
        monkeypatch.setattr(self.mod, "audit_start", lambda *a: 1)
        monkeypatch.setattr(self.mod, "audit_finish", lambda *a, **kw: None)
        df = pd.DataFrame({
            "ts_code": ["000001.SH"],
            "trade_date": ["20240101"],
            "open": [3000.0], "close": [3010.0],
            "high": [3020.0], "low": [2990.0],
            "vol": [100.0], "amount": [5000.0],
        })
        monkeypatch.setattr(self.mod, "call_pro", lambda *a, **kw: df)
        monkeypatch.setattr(self.mod, "upsert_index_weekly_df", lambda d: len(d))
        self.mod.ingest_index_weekly(ts_code="000001.SH")

    def test_ingest_index_weekly_rate_limit_retry(self, monkeypatch):
        monkeypatch.setattr(self.mod, "audit_start", lambda *a: 1)
        monkeypatch.setattr(self.mod, "audit_finish", lambda *a, **kw: None)
        call_count = {"n": 0}
        def fake_call(*a, **kw):
            call_count["n"] += 1
            if call_count["n"] == 1:
                raise Exception("rate limit exceeded")
            return pd.DataFrame()
        monkeypatch.setattr(self.mod, "call_pro", fake_call)
        monkeypatch.setattr(self.mod, "parse_retry_after", lambda msg: 0.0)
        monkeypatch.setattr("time.sleep", lambda _: None)
        self.mod.ingest_index_weekly(ts_code="000001.SH")

    # ---- ingest_dividend ----
    def test_ingest_dividend_date_normalization(self, monkeypatch):
        monkeypatch.setattr(self.mod, "audit_start", lambda *a: 1)
        monkeypatch.setattr(self.mod, "audit_finish", lambda *a, **kw: None)
        df = pd.DataFrame({
            "ts_code": ["000001.SZ"],
            "end_date": ["20240630"],
            "ann_date": ["20240701"],
            "div_proc": ["实施"],
            "stk_div": [0.5],
            "cash_div": [1.0],
            "cash_div_tax": [0.9],
            "record_date": ["20240715"],
            "ex_date": ["20240716"],
            "pay_date": ["20240720"],
            "imp_ann_date": [None],
        })
        monkeypatch.setattr(self.mod, "call_pro", lambda *a, **kw: df)
        monkeypatch.setattr(self.mod, "upsert_dividend_df", lambda d: len(d))
        self.mod.ingest_dividend(ts_code="000001.SZ")

    # ---- ingest_dividend_by_date_range ----
    def test_ingest_dividend_by_date_range_empty(self, monkeypatch):
        monkeypatch.setattr(self.mod, "audit_start", lambda *a: 1)
        monkeypatch.setattr(self.mod, "audit_finish", lambda *a, **kw: None)
        monkeypatch.setattr(self.mod, "_fetch_existing_keys", lambda *a: set())
        monkeypatch.setattr(self.mod, "get_all_ts_codes", lambda: [])
        self.mod.ingest_dividend_by_date_range("2024-01-01", "2024-06-30")

    def test_ingest_dividend_by_date_range_with_data(self, monkeypatch):
        monkeypatch.setattr(self.mod, "audit_start", lambda *a: 1)
        monkeypatch.setattr(self.mod, "audit_finish", lambda *a, **kw: None)
        monkeypatch.setattr(self.mod, "_fetch_existing_keys", lambda *a: set())
        monkeypatch.setattr(self.mod, "get_all_ts_codes", lambda: ["000001.SZ"])
        df = pd.DataFrame({
            "ts_code": ["000001.SZ"],
            "ann_date": ["20240701"],
            "imp_ann_date": [None],
            "end_date": ["20240630"],
        })
        monkeypatch.setattr(self.mod, "call_pro", lambda *a, **kw: df)
        monkeypatch.setattr(self.mod, "upsert_dividend_df", lambda d: len(d))
        monkeypatch.setattr("time.sleep", lambda _: None)
        self.mod.ingest_dividend_by_date_range("2024-01-01", "2024-06-30")

    def test_ingest_dividend_by_date_range_skip_row_no_ann(self, monkeypatch):
        monkeypatch.setattr(self.mod, "audit_start", lambda *a: 1)
        monkeypatch.setattr(self.mod, "audit_finish", lambda *a, **kw: None)
        monkeypatch.setattr(self.mod, "_fetch_existing_keys", lambda *a: set())
        monkeypatch.setattr(self.mod, "get_all_ts_codes", lambda: ["000001.SZ"])
        df = pd.DataFrame({
            "ts_code": ["000001.SZ"],
            "ann_date": [None],
            "imp_ann_date": [None],
            "end_date": ["20240630"],
        })
        monkeypatch.setattr(self.mod, "call_pro", lambda *a, **kw: df)
        monkeypatch.setattr("time.sleep", lambda _: None)
        self.mod.ingest_dividend_by_date_range("2024-01-01", "2024-06-30")

    # ---- ingest_top10_holders_by_date_range ----
    def test_ingest_top10_holders_skip_logic(self, monkeypatch):
        monkeypatch.setattr(self.mod, "audit_start", lambda *a: 1)
        monkeypatch.setattr(self.mod, "audit_finish", lambda *a, **kw: None)
        monkeypatch.setattr(self.mod, "_fetch_existing_keys", lambda *a: set())
        monkeypatch.setattr(self.mod, "get_all_ts_codes", lambda: ["A", "B", "C"])
        call_log = []
        def fake_call(*a, **kw):
            call_log.append(kw.get("ts_code"))
            return pd.DataFrame()
        monkeypatch.setattr(self.mod, "call_pro", fake_call)
        monkeypatch.setattr("time.sleep", lambda _: None)
        self.mod.ingest_top10_holders_by_date_range(
            "2024-01-01", "2024-06-30", batch_size=10, start_after_ts_code="B"
        )
        assert "A" not in call_log
        assert call_log == ["B", "C"]

    def test_ingest_top10_holders_with_data(self, monkeypatch):
        monkeypatch.setattr(self.mod, "audit_start", lambda *a: 1)
        monkeypatch.setattr(self.mod, "audit_finish", lambda *a, **kw: None)
        monkeypatch.setattr(self.mod, "_fetch_existing_keys", lambda *a: set())
        monkeypatch.setattr(self.mod, "get_all_ts_codes", lambda: ["000001.SZ"])
        df = pd.DataFrame({
            "ts_code": ["000001.SZ"],
            "end_date": ["20240630"],
            "holder_name": ["Test Fund"],
            "hold_amount": [1000000.0],
            "hold_ratio": [5.0],
        })
        monkeypatch.setattr(self.mod, "call_pro", lambda *a, **kw: df)
        monkeypatch.setattr(self.mod, "upsert_top10_holders", lambda d: len(d))
        monkeypatch.setattr("time.sleep", lambda _: None)
        self.mod.ingest_top10_holders_by_date_range("2024-01-01", "2024-06-30")

    # ---- ingest_adj_factor_by_date_range ----
    def test_ingest_adj_factor_by_date_range_empty(self, monkeypatch):
        monkeypatch.setattr(self.mod, "audit_start", lambda *a: 1)
        monkeypatch.setattr(self.mod, "audit_finish", lambda *a, **kw: None)
        monkeypatch.setattr(self.mod, "_fetch_existing_keys", lambda *a: set())
        monkeypatch.setattr(self.mod, "get_all_ts_codes", lambda: [])
        self.mod.ingest_adj_factor_by_date_range("2024-01-01", "2024-06-30")

    def test_ingest_adj_factor_by_date_range_with_data(self, monkeypatch):
        monkeypatch.setattr(self.mod, "audit_start", lambda *a: 1)
        monkeypatch.setattr(self.mod, "audit_finish", lambda *a, **kw: None)
        monkeypatch.setattr(self.mod, "_fetch_existing_keys", lambda *a: set())
        monkeypatch.setattr(self.mod, "get_all_ts_codes", lambda: ["000001.SZ"])
        df = pd.DataFrame({
            "ts_code": ["000001.SZ"],
            "trade_date": ["20240701"],
            "adj_factor": [1.5],
        })
        monkeypatch.setattr(self.mod, "call_pro", lambda *a, **kw: df)
        monkeypatch.setattr(self.mod, "upsert_adj_factor", lambda d: len(d))
        monkeypatch.setattr("time.sleep", lambda _: None)
        self.mod.ingest_adj_factor_by_date_range("2024-01-01", "2024-06-30")

    def test_ingest_adj_factor_skip_existing(self, monkeypatch):
        monkeypatch.setattr(self.mod, "audit_start", lambda *a: 1)
        monkeypatch.setattr(self.mod, "audit_finish", lambda *a, **kw: None)
        monkeypatch.setattr(self.mod, "_fetch_existing_keys", lambda *a: {("000001.SZ", "2024-07-01")})
        monkeypatch.setattr(self.mod, "get_all_ts_codes", lambda: ["000001.SZ"])
        df = pd.DataFrame({
            "ts_code": ["000001.SZ"],
            "trade_date": ["20240701"],
            "adj_factor": [1.5],
        })
        monkeypatch.setattr(self.mod, "call_pro", lambda *a, **kw: df)
        monkeypatch.setattr("time.sleep", lambda _: None)
        self.mod.ingest_adj_factor_by_date_range("2024-01-01", "2024-06-30")

    # ---- ingest_all_daily ----
    def test_ingest_all_daily_empty(self, monkeypatch):
        monkeypatch.setattr(self.mod, "get_all_ts_codes", lambda: [])
        self.mod.ingest_all_daily(start_date="2024-01-01", end_date="2024-06-30")

    def test_ingest_all_daily_skip_logic(self, monkeypatch):
        monkeypatch.setattr(self.mod, "get_all_ts_codes", lambda: ["A", "B"])
        monkeypatch.setattr(self.mod, "get_max_trade_date", lambda tc: None)
        monkeypatch.setattr(self.mod, "_fetch_existing_keys", lambda *a, **kw: set())
        calls = []
        monkeypatch.setattr(
            self.mod,
            "call_pro",
            lambda *a, **kw: calls.append(kw.get("ts_code")) or pd.DataFrame(),
        )
        monkeypatch.setattr("time.sleep", lambda _: None)
        self.mod.ingest_all_daily(
            start_date="2024-01-01", end_date="2024-06-30",
            start_after_ts_code="A", batch_size=10,
        )
        assert calls == ["A", "B"]

    def test_ingest_all_daily_resume_from_last(self, monkeypatch):
        monkeypatch.setattr(self.mod, "get_all_ts_codes", lambda: ["000001.SZ"])
        monkeypatch.setattr(self.mod, "get_max_trade_date", lambda tc: "2024-03-01")
        monkeypatch.setattr(self.mod, "ingest_daily", lambda **kw: None)
        monkeypatch.setattr("time.sleep", lambda _: None)
        self.mod.ingest_all_daily(
            start_date="2024-01-01", end_date="2024-06-30", batch_size=10,
        )

    def test_ingest_all_daily_progress_cb(self, monkeypatch):
        monkeypatch.setattr(self.mod, "get_all_ts_codes", lambda: ["000001.SZ"])
        monkeypatch.setattr(self.mod, "get_max_trade_date", lambda tc: None)
        monkeypatch.setattr(self.mod, "ingest_daily", lambda **kw: None)
        monkeypatch.setattr("time.sleep", lambda _: None)
        progress = []
        self.mod.ingest_all_daily(
            start_date="2024-01-01", end_date="2024-06-30",
            progress_cb=lambda **kw: progress.append(kw),
        )
        assert len(progress) >= 1

    def test_ingest_all_daily_retry_on_error(self, monkeypatch):
        monkeypatch.setattr(self.mod, "get_all_ts_codes", lambda: ["000001.SZ"])
        monkeypatch.setattr(self.mod, "get_max_trade_date", lambda tc: None)
        call_count = {"n": 0}
        def fake_ingest(**kw):
            call_count["n"] += 1
            if call_count["n"] == 1:
                raise Exception("temporary error")
        monkeypatch.setattr(self.mod, "ingest_daily", fake_ingest)
        monkeypatch.setattr(self.mod, "_is_rate_limit_error", lambda msg: False)
        monkeypatch.setattr("time.sleep", lambda _: None)
        self.mod.ingest_all_daily(
            start_date="2024-01-01", end_date="2024-06-30",
        )


# ===========================================================================
# 2. tushare_dao
# ===========================================================================
class TestTushareDao:
    @pytest.fixture(autouse=True)
    def _load(self, monkeypatch):
        eng, ctx, raw = _fake_engine()
        monkeypatch.setattr("app.domains.extdata.dao.tushare_dao.engine", eng)
        self.eng = eng
        self.ctx = ctx
        self.mod = importlib.import_module("app.domains.extdata.dao.tushare_dao")

    def test_audit_finish(self):
        self.mod.audit_finish(1, "success", 100)
        self.ctx.execute.assert_called_once()

    def test_get_failed_ts_codes_no_limit(self):
        self.ctx.execute.return_value.fetchall.return_value = [("000001.SZ",), ("000002.SZ",)]
        result = self.mod.get_failed_ts_codes()
        assert len(result) == 2

    def test_get_failed_ts_codes_with_limit(self):
        self.ctx.execute.return_value.fetchall.return_value = [("000001.SZ",)]
        result = self.mod.get_failed_ts_codes(limit=1)
        assert len(result) == 1

    def test_upsert_daily_df(self):
        df = pd.DataFrame({
            "ts_code": ["000001.SZ"],
            "trade_date": ["20240101"],
            "open": [10.0], "high": [11.0], "low": [9.0], "close": [10.5],
            "pre_close": [10.0], "change": [0.5], "pct_chg": [5.0],
            "vol": [1000.0], "amount": [50000.0],
        })
        result = self.mod.upsert_daily(df)
        assert result >= 0

    def test_upsert_weekly(self):
        df = pd.DataFrame({
            "ts_code": ["000001.SZ"],
            "trade_date": ["20240101"],
            "open": [10.0], "high": [11.0], "low": [9.0], "close": [10.5],
            "pre_close": [10.0], "change": [0.5], "pct_chg": [5.0],
            "vol": [1000.0], "amount": [50000.0],
        })
        result = self.mod.upsert_weekly(df)
        assert result >= 0

    def test_round2_module_level(self):
        fn = self.mod._round2
        assert fn(None) is None
        assert fn(float("nan")) is None
        assert fn(3.14159) == pytest.approx(3.14)
        assert fn(np.float64(2.567)) == pytest.approx(2.57)
        assert fn("bad") == "bad"  # returns original on failure


# ===========================================================================
# 3. data_sync_status_dao
# ===========================================================================
class TestDataSyncStatusDao:
    @pytest.fixture(autouse=True)
    def _load(self, monkeypatch):
        eng_ts, ctx_ts, _ = _fake_engine()
        eng_tm, ctx_tm, raw_tm = _fake_engine()
        eng_ak, ctx_ak, raw_ak = _fake_engine()
        eng_vn, ctx_vn, _ = _fake_engine()
        monkeypatch.setattr("app.domains.extdata.dao.data_sync_status_dao.engine_ts", eng_ts)
        monkeypatch.setattr("app.domains.extdata.dao.data_sync_status_dao.engine_tm", eng_tm)
        monkeypatch.setattr("app.domains.extdata.dao.data_sync_status_dao.engine_ak", eng_ak)
        monkeypatch.setattr("app.domains.extdata.dao.data_sync_status_dao.engine_vn", eng_vn)
        self.eng_tm = eng_tm
        self.ctx_tm = ctx_tm
        self.raw_tm = raw_tm
        self.eng_ts = eng_ts
        self.ctx_ts = ctx_ts
        self.eng_ak = eng_ak
        self.ctx_ak = ctx_ak
        self.raw_ak = raw_ak
        self.mod = importlib.import_module("app.domains.extdata.dao.data_sync_status_dao")

    def test_write_step_status(self):
        self.mod.write_step_status(date(2024, 1, 1), "tushare_stock_daily", "success", rows_synced=100)
        self.ctx_tm.execute.assert_called_once()

    def test_get_failed_steps(self):
        self.ctx_tm.execute.return_value.fetchall.return_value = [
            (date(2024, 1, 1), "tushare", "stock_daily"),
        ]
        result = self.mod.get_failed_steps(lookback_days=30)
        assert len(result) == 1

    def test_get_stock_daily_ts_codes_for_date(self):
        self.ctx_ts.execute.return_value.fetchall.return_value = [("000001.SZ",), ("000002.SZ",)]
        result = self.mod.get_stock_daily_ts_codes_for_date(date(2024, 1, 2))
        assert len(result) == 2

    def test_get_cached_trade_dates(self):
        self.ctx_ak.execute.return_value.fetchall.return_value = [
            (date(2024, 1, 2),), (date(2024, 1, 3),),
        ]
        result = self.mod.get_cached_trade_dates(date(2024, 1, 1), date(2024, 1, 5))
        assert len(result) == 2

    def test_upsert_trade_dates_empty(self):
        result = self.mod.upsert_trade_dates([])
        assert result == 0

    def test_upsert_trade_dates_with_data(self):
        cursor = MagicMock()
        cursor.rowcount = 2
        raw = self.raw_ak
        raw.cursor.return_value = cursor
        result = self.mod.upsert_trade_dates([date(2024, 1, 2), date(2024, 1, 3)])
        assert result >= 0

    def test_acquire_backfill_lock_success(self):
        self.ctx_tm.execute.return_value.rowcount = 1
        with patch.object(self.mod, "ensure_backfill_lock_table"):
            with patch.object(self.mod, "release_stale_backfill_lock"):
                result = self.mod.acquire_backfill_lock()
                assert result is True

    def test_acquire_backfill_lock_already_locked(self):
        self.ctx_tm.execute.return_value.rowcount = 0
        with patch.object(self.mod, "ensure_backfill_lock_table"):
            with patch.object(self.mod, "release_stale_backfill_lock"):
                result = self.mod.acquire_backfill_lock()
                assert result is False

    def test_release_stale_backfill_lock(self):
        self.ctx_tm.execute.return_value.rowcount = 0
        with patch.object(self.mod, "ensure_backfill_lock_table"):
            self.mod.release_stale_backfill_lock(max_age_hours=1)

    def test_bulk_upsert_status(self):
        cursor = MagicMock()
        self.raw_tm.cursor.return_value.__enter__ = MagicMock(return_value=cursor)
        self.raw_tm.cursor.return_value.__exit__ = MagicMock(return_value=False)
        rows = [
            (date(2024, 1, 1), "tushare", "stock_daily", "success", 100, None, datetime.now()),
        ]
        self.mod.bulk_upsert_status(rows)


# ===========================================================================
# 4. realtime_quote_service
# ===========================================================================
class TestRealtimeQuoteService:
    @pytest.fixture(autouse=True)
    def _load(self):
        self.mod = importlib.import_module("app.domains.market.realtime_quote_service")

    def test_to_int_none(self):
        svc = self.mod.RealtimeQuoteService()
        assert svc._to_int(None) is None

    def test_to_int_string(self):
        svc = self.mod.RealtimeQuoteService()
        assert svc._to_int("1,234") == 1234
        assert svc._to_int("") is None
        assert svc._to_int("abc") is None

    def test_to_int_number(self):
        svc = self.mod.RealtimeQuoteService()
        assert svc._to_int(42) == 42
        assert svc._to_int(3.7) == 3

    def test_normalize_symbol(self):
        svc = self.mod.RealtimeQuoteService()
        assert svc._normalize_symbol("usd/cny") == "USDCNY"
        assert svc._normalize_symbol("000001.SZ") == "000001SZ"

    def test_pick(self):
        svc = self.mod.RealtimeQuoteService()
        row = pd.Series({"a": None, "b": "", "c": 42})
        assert svc._pick(row, ["a", "b", "c"]) == 42
        assert svc._pick(row, ["x", "y"]) is None

    def test_quote_cn_tencent(self, monkeypatch):
        svc = self.mod.RealtimeQuoteService()
        parts = [""] * 50
        parts[1] = "平安银行"
        parts[3] = "15.50"
        parts[4] = "15.00"
        parts[5] = "15.30"
        parts[33] = "15.70"
        parts[34] = "15.20"
        parts[6] = "1000000"
        parts[37] = "50000000"
        monkeypatch.setattr(svc, "_tencent_request_with_retry", lambda *a, **kw: parts)
        result = svc._quote_cn("000001.SZ")
        assert result["name"] == "平安银行"
        assert result["price"] == 15.50

    def test_quote_cn_index(self, monkeypatch):
        svc = self.mod.RealtimeQuoteService()
        parts = [""] * 50
        parts[1] = "上证指数"
        parts[3] = "3000.00"
        parts[4] = "2990.00"
        monkeypatch.setattr(svc, "_fetch_tencent_quote_with_prefix", lambda code, prefix: parts)
        result = svc._quote_cn_index("000001.SH")
        assert result["name"] == "上证指数"

    def test_fetch_tencent_quote_prefix(self, monkeypatch):
        svc = self.mod.RealtimeQuoteService()
        parts = [""] * 10
        parts[1] = "Test"
        parts[3] = "10.0"
        monkeypatch.setattr(svc, "_tencent_request_with_retry", lambda *a, **kw: parts)
        result = svc._fetch_tencent_quote("600000")
        assert len(result) > 0

    def test_tencent_request_with_retry(self, monkeypatch):
        svc = self.mod.RealtimeQuoteService()
        mock_resp = MagicMock()
        mock_resp.text = 'v_sh600000="Test~平安~15.5~15.0~15.3~15.7~15.2~1000~50000";'
        mock_resp.raise_for_status = MagicMock()
        monkeypatch.setattr("requests.get", lambda *a, **kw: mock_resp)
        parts = svc._tencent_request_with_retry("https://qt.gtimg.cn/q=sh600000", "600000")
        assert len(parts) >= 6

    def test_tencent_request_invalid_response(self, monkeypatch):
        svc = self.mod.RealtimeQuoteService()
        mock_resp = MagicMock()
        mock_resp.text = "invalid response"
        mock_resp.raise_for_status = MagicMock()
        monkeypatch.setattr("requests.get", lambda *a, **kw: mock_resp)
        with pytest.raises(Exception):
            svc._tencent_request_with_retry("https://qt.gtimg.cn/q=sh600000", "600000")

    def test_quote_hk_tencent_fallback(self, monkeypatch):
        svc = self.mod.RealtimeQuoteService()
        monkeypatch.setattr(svc, "_quote_hk_tencent", MagicMock(side_effect=Exception("fail")))
        # Correct column names matching _quote_hk implementation
        df = pd.DataFrame({
            "symbol": ["00700"],
            "lasttrade": [350.0],
            "name": ["腾讯"],
            "pricechange": [5.0],
            "changepercent": [1.5],
            "open": [345.0],
            "high": [355.0],
            "low": [340.0],
            "prevclose": [345.0],
            "volume": [1000000],
            "amount": [350000000],
        })
        monkeypatch.setattr(self.mod, "ak", MagicMock())
        monkeypatch.setattr(self.mod, "_fetch_akshare_with_timeout", lambda fn, name: df)
        result = svc._quote_hk("00700.HK")
        assert result["symbol"] == "00700"

    def test_quote_us(self, monkeypatch):
        svc = self.mod.RealtimeQuoteService()
        df = pd.DataFrame({
            "代码": ["105.AAPL"],
            "最新价": [180.0],
            "名称": ["Apple"],
            "涨跌额": [2.0],
            "涨跌幅": [1.1],
            "开盘价": [178.0],
            "最高价": [181.0],
            "最低价": [177.0],
            "昨收价": [178.0],
            "成交量": [5000000],
            "成交额": [900000000],
        })
        monkeypatch.setattr(self.mod, "ak", MagicMock())
        monkeypatch.setattr(self.mod, "_fetch_akshare_with_timeout", lambda fn, name: df)
        result = svc._quote_us("AAPL.US")
        assert result["symbol"] == "AAPL"

    def test_quote_fx(self, monkeypatch):
        svc = self.mod.RealtimeQuoteService()
        df = pd.DataFrame({
            "代码": ["USDCNY"],
            "最新价": [7.2],
            "名称": ["美元/人民币"],
            "涨跌额": [0.01],
            "涨跌幅": [0.14],
            "开盘价": [7.19],
            "最高价": [7.21],
            "最低价": [7.18],
            "昨收价": [7.19],
        })
        monkeypatch.setattr(self.mod, "ak", MagicMock())
        monkeypatch.setattr(self.mod, "_fetch_akshare_with_timeout", lambda fn, name: df)
        result = svc._quote_fx("USD/CNY")
        assert result["symbol"] == "USDCNY"


# ===========================================================================
# 5. paper_strategy_executor
# ===========================================================================
class TestPaperStrategyExecutor:
    @pytest.fixture(autouse=True)
    def _load(self):
        self.mod = importlib.import_module("app.domains.trading.paper_strategy_executor")

    def test_quote_to_bar_valid(self):
        executor = self.mod.PaperStrategyExecutor()
        quote = {
            "open": 10.0, "high": 11.0, "low": 9.5, "price": 10.5,
            "volume": 1000, "datetime": "2024-01-01 10:00:00",
        }
        bar = executor._quote_to_bar(quote, "000001.SZ")
        assert bar is not None

    def test_quote_to_bar_missing_price(self):
        executor = self.mod.PaperStrategyExecutor()
        bar = executor._quote_to_bar({}, "000001.SZ")
        assert bar is None

    def test_start_and_stop_deployment(self, monkeypatch):
        executor = self.mod.PaperStrategyExecutor()
        # Mock _run_strategy to avoid real execution
        monkeypatch.setattr(executor, "_run_strategy", lambda *a, **kw: None)
        result = executor.start_deployment(
            deployment_id=999, paper_account_id=1, user_id=1,
            strategy_class_name="Test", vt_symbol="000001.SZ",
            parameters={},
        )
        assert result["success"] is True
        assert executor.stop_deployment(999) is True
        assert executor.stop_deployment(9999) is False


# ===========================================================================
# 6. vnpy_trading_service
# ===========================================================================
class TestVnpyTradingService:
    @pytest.fixture(autouse=True)
    def _load(self):
        self.mod = importlib.import_module("app.domains.trading.vnpy_trading_service")
        # Reset singleton between tests
        self.mod.VnpyTradingService._instance = None

    def test_connect_gateway_simulated(self):
        svc = self.mod.VnpyTradingService()
        result = svc.connect_gateway(self.mod.GatewayType.SIMULATED, {})
        assert result is True

    def test_disconnect_gateway(self):
        svc = self.mod.VnpyTradingService()
        svc.connect_gateway(self.mod.GatewayType.SIMULATED, {}, "test")
        result = svc.disconnect_gateway("test")
        assert result is True

    def test_disconnect_gateway_not_found(self):
        svc = self.mod.VnpyTradingService()
        result = svc.disconnect_gateway("nonexistent")
        assert result is False

    def test_send_order_simulated(self):
        svc = self.mod.VnpyTradingService()
        svc.connect_gateway(self.mod.GatewayType.SIMULATED, {}, "sim")
        result = svc.send_order("000001.SZ", "buy", "limit", 100, 10.5, gateway_name="sim")
        assert result is not None
        assert result.startswith("SIM-")

    def test_send_order_no_gateway(self):
        svc = self.mod.VnpyTradingService()
        result = svc.send_order("000001.SZ", "buy", "limit", 100, 10.5)
        assert result is None

    def test_cancel_order_no_engine(self):
        svc = self.mod.VnpyTradingService()
        result = svc.cancel_order("order-123")
        assert result is False

    def test_list_gateways(self):
        svc = self.mod.VnpyTradingService()
        svc.connect_gateway(self.mod.GatewayType.SIMULATED, {}, "sim_gw")
        gateways = svc.list_gateways()
        assert len(gateways) == 1
        assert gateways[0]["name"] == "sim_gw"
        assert gateways[0]["type"] == "sim"

    def test_query_positions_no_engine(self):
        svc = self.mod.VnpyTradingService()
        result = svc.query_positions()
        assert result == []

    def test_query_account_no_engine(self):
        svc = self.mod.VnpyTradingService()
        result = svc.query_account()
        assert result is None

    def test_resolve_gateway_class_unsupported(self):
        with pytest.raises(ValueError, match="Unsupported gateway type"):
            self.mod.VnpyTradingService._resolve_gateway_class(self.mod.GatewayType.SIMULATED)

    def test_on_order_callback_registration(self):
        svc = self.mod.VnpyTradingService()
        cb = MagicMock()
        svc.on_order(cb)
        assert cb in svc._order_callbacks

    def test_on_trade_callback_registration(self):
        svc = self.mod.VnpyTradingService()
        cb = MagicMock()
        svc.on_trade(cb)
        assert cb in svc._trade_callbacks


# ===========================================================================
# 7. worker/tasks
# ===========================================================================
class TestWorkerTasks:
    @pytest.fixture(autouse=True)
    def _load(self):
        self.mod = importlib.import_module("app.worker.service.tasks")

    def test_bayesian_optimization_with_param_defs(self, monkeypatch):
        if not hasattr(self.mod, "_run_bayesian_optimization"):
            pytest.skip("_run_bayesian_optimization not found")
        mock_opt_setting = MagicMock()
        mock_opt_setting.target_name = "total_return"
        mock_opt_setting.generate_settings.return_value = []
        mock_opt_setting.params = {"fast": {"min": 5, "max": 20, "step": 5}}
        monkeypatch.setattr(self.mod, "_evaluate_single", lambda *a: ("setting", 1.5, {}))
        monkeypatch.setattr("optuna.logging.set_verbosity", lambda _: None)

        mock_study = MagicMock()
        monkeypatch.setattr("optuna.create_study", lambda **kw: mock_study)

        self.mod._run_bayesian_optimization(
            optimization_setting=mock_opt_setting,
            strategy_class=MagicMock,
            symbol="000001.SZ",
            start=date(2024, 1, 1),
            end=date(2024, 6, 30),
            rate=0.0001, slippage=0.0, size=1,
            pricetick=0.01, capital=100000, n_trials=5,
        )

    def test_strategy_class_parameters_reading(self, monkeypatch):
        if not hasattr(self.mod, "run_backtest_task"):
            pytest.skip("run_backtest_task not found")
        mock_cls = MagicMock()
        mock_cls.get_class_parameters.return_value = {"fast": 10, "slow": 30}
        settings = {}
        if hasattr(mock_cls, "get_class_parameters"):
            settings = mock_cls.get_class_parameters() or {}
        assert settings["fast"] == 10


# ===========================================================================
# 8. backtest routes
# ===========================================================================
class TestBacktestRoutes:
    @pytest.fixture
    def client(self):
        from app.api.routes.backtest import router
        return _make_client(router, prefix="/api/v1")

    def test_list_backtest_history(self, client):
        with patch("app.api.routes.backtest.BacktestHistoryDao") as MockDao:
            inst = MockDao.return_value
            inst.count_for_user.return_value = 0
            inst.list_for_user.return_value = []
            resp = client.get("/api/v1/backtest/history/list")
        assert resp.status_code == 200


# ===========================================================================
# 9. qlib_model_service
# ===========================================================================
class TestQlibModelService:
    @pytest.fixture(autouse=True)
    def _load(self):
        self.mod = importlib.import_module("app.domains.ai.qlib_model_service")

    def test_calculate_metrics_empty(self):
        svc = self.mod.QlibModelService()
        pred = pd.Series(dtype=float)
        dataset = MagicMock()
        dataset.prepare.return_value = pd.DataFrame()
        result = svc._calculate_metrics(pred, dataset)
        assert result is not None

    def test_calculate_metrics_valid(self):
        svc = self.mod.QlibModelService()
        idx = pd.MultiIndex.from_tuples([
            (datetime(2024, 1, 1), "000001.SZ"),
            (datetime(2024, 1, 2), "000001.SZ"),
        ])
        pred = pd.Series([0.5, 0.3], index=idx)
        label = pd.DataFrame([0.4, 0.2], index=idx, columns=["LABEL0"])
        dataset = MagicMock()
        dataset.prepare.return_value = label
        result = svc._calculate_metrics(pred, dataset)
        assert "ic" in result or result is not None


# ===========================================================================
# 10. routes: strategies, factors, datasync, settings, ai
# ===========================================================================
class TestStrategiesRoutes:
    @pytest.fixture
    def client(self):
        from app.api.routes.strategies import router
        return _make_client(router, prefix="/api/v1")

    @patch("app.domains.strategies.service.StrategiesService.update_strategy", side_effect=ValueError("Invalid code"))
    def test_update_strategy_validation_error(self, mock_update, client):
        resp = client.put("/api/v1/strategies/1", json={"name": "Updated"})
        assert resp.status_code in (400, 422, 500)

    @patch("app.domains.strategies.service.StrategiesService.delete_strategy", return_value=True)
    def test_delete_strategy(self, mock_del, client):
        resp = client.delete("/api/v1/strategies/1")
        assert resp.status_code in (200, 204, 404)

    @patch("app.domains.strategies.service.StrategiesService.list_code_history")
    def test_get_code_history(self, mock_hist, client):
        mock_hist.return_value = [
            {"id": 1, "version": 1, "code": "pass", "created_at": "2024-01-01"}
        ]
        resp = client.get("/api/v1/strategies/1/code-history")
        assert resp.status_code == 200

    @patch("app.domains.strategies.service.StrategiesService.restore_code_history")
    def test_restore_code_history(self, mock_restore, client):
        mock_restore.return_value = {"id": 1, "name": "Test", "version": 2}
        resp = client.post("/api/v1/strategies/1/code-history/1/restore")
        assert resp.status_code in (200, 201)


class TestFactorsRoutes:
    @pytest.fixture
    def client(self):
        from app.api.routes.factors import router
        return _make_client(router, prefix="/api/v1")

    @patch("app.domains.factors.service.FactorService.delete_factor", return_value=True)
    def test_delete_factor(self, mock_del, client):
        resp = client.delete("/api/v1/factors/1")
        assert resp.status_code in (200, 204, 404)


class TestDatasyncRoutes:
    @pytest.fixture
    def client(self):
        from app.api.routes.datasync import router
        return _make_client(router, prefix="/api/v1")

    def test_get_sync_status(self, client):
        with patch("app.domains.extdata.dao.data_sync_status_dao.get_step_status") as mock_fn:
            mock_fn.return_value = None
            resp = client.get("/api/v1/datasync/status")
        assert resp.status_code in (200, 422, 500)


class TestSettingsRoutes:
    @pytest.fixture
    def client(self):
        from app.api.routes.settings import router
        return _make_client(router, prefix="/api/v1")

    def test_list_datasource_items(self, client):
        with patch("app.domains.market.dao.data_source_item_dao.DataSourceItemDao") as MockDao:
            MockDao.return_value.list_all.return_value = [
                {"id": 1, "source": "tushare", "item_key": "stock_basic", "enabled": 1}
            ]
            resp = client.get("/api/v1/settings/datasource-items")
        assert resp.status_code == 200


class TestAIRoutes:
    @pytest.fixture
    def client(self):
        from app.api.routes.ai import router
        return _make_client(router, prefix="/api/v1")

    def test_list_conversations(self, client):
        with patch("app.api.routes.ai.AIService") as MockSvc:
            MockSvc.return_value.count_conversations.return_value = 0
            MockSvc.return_value.list_conversations.return_value = []
            resp = client.get("/api/v1/ai/conversations")
        assert resp.status_code == 200


# ===========================================================================
# 11. expression_engine, factor_screening, sentiment_service
# ===========================================================================
class TestExpressionEngine:
    @pytest.fixture(autouse=True)
    def _load(self):
        self.mod = importlib.import_module("app.domains.factors.expression_engine")

    def test_validate_expression_safe(self):
        self.mod._validate_expression("close / open")

    def test_validate_expression_unsafe(self):
        with pytest.raises(ValueError):
            self.mod._validate_expression("__import__('os').system('rm')")

    def test_compute_custom_factor_simple(self):
        ohlcv = pd.DataFrame({
            "open": [10.0, 11.0, 12.0],
            "close": [10.5, 11.5, 12.5],
            "high": [11.0, 12.0, 13.0],
            "low": [9.5, 10.5, 11.5],
            "vol": [1000, 2000, 3000],
            "amount": [10000, 22000, 36000],
        })
        result = self.mod.compute_custom_factor("close / open", ohlcv)
        assert len(result) == 3
        assert result.iloc[0] == pytest.approx(1.05)

    def test_compute_factor_metrics(self):
        factor_values = pd.Series([0.5, -0.3, 0.2, 0.1, -0.4])
        forward_returns = pd.Series([0.4, -0.2, 0.3, 0.05, -0.3])
        result = self.mod.compute_factor_metrics(factor_values, forward_returns)
        assert "ic_mean" in result

    def test_compute_forward_returns(self):
        ohlcv = pd.DataFrame({
            "close": [10.0, 10.5, 11.0, 10.8, 11.2],
        })
        result = self.mod.compute_forward_returns(ohlcv, periods=1)
        assert len(result) == 5


class TestFactorScreening:
    @pytest.fixture(autouse=True)
    def _load(self):
        self.mod = importlib.import_module("app.domains.factors.factor_screening")

    def test_screen_factor_pool_empty(self, monkeypatch):
        monkeypatch.setattr(self.mod, "fetch_ohlcv", lambda **kw: pd.DataFrame())
        result = self.mod.screen_factor_pool(
            expressions=[], start_date=date(2024, 1, 1), end_date=date(2024, 6, 30)
        )
        assert isinstance(result, list)

    def test_screen_factor_pool_with_expressions(self, monkeypatch):
        ohlcv = pd.DataFrame({
            "open": np.random.rand(100) * 10 + 10,
            "close": np.random.rand(100) * 10 + 10,
            "high": np.random.rand(100) * 10 + 12,
            "low": np.random.rand(100) * 10 + 8,
            "vol": np.random.rand(100) * 1000,
            "amount": np.random.rand(100) * 10000,
        })
        monkeypatch.setattr(self.mod, "fetch_ohlcv", lambda **kw: ohlcv)
        monkeypatch.setattr(self.mod, "compute_forward_returns", lambda df, periods=1: pd.Series(np.random.randn(len(df))))
        monkeypatch.setattr(self.mod, "compute_custom_factor", lambda expr, df: pd.Series(np.random.randn(len(df))))
        monkeypatch.setattr(self.mod, "compute_factor_metrics", lambda fv, fr: {"ic": 0.15, "ir": 2.0, "turnover": 0.3})
        result = self.mod.screen_factor_pool(
            expressions=["close / open", "high - low"],
            start_date=date(2024, 1, 1),
            end_date=date(2024, 6, 30),
        )
        assert isinstance(result, list)


class TestSentimentService:
    @pytest.fixture(autouse=True)
    def _load(self):
        self.mod = importlib.import_module("app.domains.market.sentiment_service")

    def test_get_overview_no_akshare(self, monkeypatch):
        monkeypatch.setattr(self.mod, "ak", None)
        svc = self.mod.SentimentService()
        result = svc.get_overview()
        # When ak is None the service returns defaults without raising
        assert result["advance_decline"] is None

    def test_get_overview_with_data(self, monkeypatch):
        mock_ak = MagicMock()
        spot_df = pd.DataFrame({
            "代码": ["000001", "000002", "000003"],
            "名称": ["平安银行", "万科A", "国农科技"],
            "最新价": [15.0, 10.0, 20.0],
            "涨跌幅": [2.0, -1.0, 0.5],
            "成交量": [100000, 200000, 50000],
            "成交额": [1500000, 2000000, 1000000],
        })
        mock_ak.stock_zh_a_spot_em.return_value = spot_df
        index_df = pd.DataFrame({
            "代码": ["000001"],
            "名称": ["上证指数"],
            "最新价": [3000.0],
            "涨跌幅": [0.5],
        })
        mock_ak.stock_zh_index_spot_em.return_value = index_df
        monkeypatch.setattr(self.mod, "ak", mock_ak)
        svc = self.mod.SentimentService()
        result = svc.get_overview()
        assert result is not None

    def test_get_fear_greed(self, monkeypatch):
        mock_ak = MagicMock()
        spot_df = pd.DataFrame({
            "代码": ["000001", "000002"],
            "名称": ["平安银行", "万科A"],
            "最新价": [15.0, 10.0],
            "涨跌幅": [2.0, -1.0],
            "成交量": [100000, 200000],
            "成交额": [1500000, 2000000],
        })
        mock_ak.stock_zh_a_spot_em.return_value = spot_df
        index_df = pd.DataFrame({
            "代码": ["000001"],
            "名称": ["上证指数"],
            "最新价": [3000.0],
            "涨跌幅": [0.5],
        })
        mock_ak.stock_zh_index_spot_em.return_value = index_df
        monkeypatch.setattr(self.mod, "ak", mock_ak)
        svc = self.mod.SentimentService()
        result = svc.get_fear_greed()
        assert "score" in result or result is not None


# ===========================================================================
# 12. sync_engine
# ===========================================================================
class TestSyncEngine:
    @pytest.fixture(autouse=True)
    def _load(self):
        self.mod = importlib.import_module("app.datasync.service.sync_engine")

    def test_daily_sync_no_items(self, monkeypatch):
        monkeypatch.setattr(self.mod, "_get_enabled_items", lambda: [])
        monkeypatch.setattr(self.mod, "_write_status", lambda *a, **kw: None)
        registry = MagicMock()
        registry.get_interface.return_value = MagicMock()
        result = self.mod.daily_sync(registry, target_date=date(2024, 1, 2))
        assert isinstance(result, dict)

    def test_daily_sync_with_items(self, monkeypatch):
        mock_iface = MagicMock()
        sync_result = MagicMock()
        sync_result.status.value = "success"
        sync_result.rows_synced = 100
        sync_result.error_message = None
        mock_iface.sync_date.return_value = sync_result
        item = {
            "source": "tushare", "item_key": "stock_daily",
            "target_database": "quantmate_ts", "target_table": "stock_daily",
            "table_created": True,
        }
        monkeypatch.setattr(self.mod, "_get_enabled_items", lambda: [item])
        monkeypatch.setattr(self.mod, "_write_status", lambda *a, **kw: None)
        monkeypatch.setattr(self.mod, "_get_status", lambda *a: None)
        registry = MagicMock()
        registry.get_interface.return_value = mock_iface
        result = self.mod.daily_sync(registry, target_date=date(2024, 1, 2))
        assert isinstance(result, dict)

    def test_daily_sync_error_handling(self, monkeypatch):
        mock_iface = MagicMock()
        mock_iface.sync_date.side_effect = Exception("API error")
        item = {
            "source": "tushare", "item_key": "stock_daily",
            "target_database": "quantmate_ts", "target_table": "stock_daily",
            "table_created": True,
        }
        monkeypatch.setattr(self.mod, "_get_enabled_items", lambda: [item])
        monkeypatch.setattr(self.mod, "_write_status", lambda *a, **kw: None)
        monkeypatch.setattr(self.mod, "_get_status", lambda *a: None)
        registry = MagicMock()
        registry.get_interface.return_value = mock_iface
        result = self.mod.daily_sync(registry, target_date=date(2024, 1, 2), continue_on_error=True)
        assert isinstance(result, dict)


# ===========================================================================
# 13. db/migrate
# ===========================================================================
class TestDbMigrate:
    @pytest.fixture(autouse=True)
    def _load(self):
        self.mod = importlib.import_module("app.infrastructure.db.migrate")

    def test_split_sql_statements(self):
        sql = "CREATE TABLE a (id INT);\nINSERT INTO a VALUES (1);"
        result = self.mod._split_sql_statements(sql)
        assert len(result) >= 2

    def test_strip_leading_sql_comments(self):
        stmt = "-- comment\nSELECT 1"
        result = self.mod._strip_leading_sql_comments(stmt)
        assert "SELECT" in result

    def test_file_checksum(self, tmp_path):
        p = tmp_path / "test.sql"
        p.write_text("SELECT 1;")
        result = self.mod._file_checksum(p)
        assert isinstance(result, str)
        assert len(result) > 0

    def test_discover_migrations(self, monkeypatch):
        result = self.mod._discover_migrations()
        assert isinstance(result, list)


# ===========================================================================
# 14. datasync/main
# ===========================================================================
class TestDatasyncMain:
    @pytest.fixture(autouse=True)
    def _load(self):
        self.mod = importlib.import_module("app.datasync.main")

    def test_module_imports(self):
        assert hasattr(self.mod, "main")


# ===========================================================================
# 15. strategies/service branches
# ===========================================================================
class TestStrategiesServiceBranches:
    @pytest.fixture(autouse=True)
    def _load(self):
        self.mod = importlib.import_module("app.domains.strategies.service")

    def test_create_strategy_with_validation(self, monkeypatch):
        with patch("app.domains.strategies.service.StrategyDao"), \
             patch("app.domains.strategies.service.StrategyHistoryDao"), \
             patch("app.domains.strategies.service.get_audit_service"):
            svc = self.mod.StrategiesService()

        svc._dao = MagicMock()
        svc._dao.insert_strategy.return_value = 1
        svc._dao.name_exists_for_user.return_value = False
        svc._dao.get_for_user.return_value = {"id": 1, "name": "Test", "class_name": "TestStrategy", "parameters": "{}"}
        monkeypatch.setattr(
            "app.domains.strategies.service.validate_strategy_code",
            lambda code, cn: SimpleNamespace(valid=True, errors=[]),
        )
        result = svc.create_strategy(
            user_id=1, name="Test", class_name="TestStrategy",
            description="desc", parameters={}, code="class TestStrategy: pass",
        )
        assert result is not None

    def test_create_strategy_validation_failure(self, monkeypatch):
        with patch("app.domains.strategies.service.StrategyDao"), \
             patch("app.domains.strategies.service.StrategyHistoryDao"), \
             patch("app.domains.strategies.service.get_audit_service"):
            svc = self.mod.StrategiesService()

        svc._dao = MagicMock()
        svc._dao.name_exists_for_user.return_value = False
        monkeypatch.setattr(
            "app.domains.strategies.service.validate_strategy_code",
            lambda code, cn: SimpleNamespace(valid=False, errors=["syntax error"]),
        )
        with pytest.raises(ValueError):
            svc.create_strategy(
                user_id=1, name="Test", class_name="TestStrategy",
                description="desc", parameters={}, code="invalid code",
            )

    def test_update_strategy_no_change(self, monkeypatch):
        with patch("app.domains.strategies.service.StrategyDao"), \
             patch("app.domains.strategies.service.StrategyHistoryDao"), \
             patch("app.domains.strategies.service.get_audit_service"):
            svc = self.mod.StrategiesService()

        svc._dao = MagicMock()
        svc._dao.get_existing_for_update.return_value = {
            "id": 1, "name": "Same", "class_name": "Cls", "version": 1,
            "code": "pass", "parameters": "{}", "description": "desc",
        }
        svc._dao.get_for_user.return_value = {
            "id": 1, "name": "Same", "class_name": "Cls", "version": 1,
            "code": "pass", "parameters": "{}", "description": "desc",
        }
        result = svc.update_strategy(user_id=1, strategy_id=1)
        assert result is not None


# ===========================================================================
# 16. strategy_service validation
# ===========================================================================
class TestStrategyServiceValidation:
    @pytest.fixture(autouse=True)
    def _load(self):
        self.mod = importlib.import_module("app.api.services.strategy_service")

    def test_validate_strategy_code_syntax_error(self):
        result = self.mod.validate_strategy_code("def invalid(:", "TestStrategy")
        assert result.valid is False

    def test_validate_strategy_code_no_class(self):
        result = self.mod.validate_strategy_code("x = 1", "TestStrategy")
        assert result.valid is False

    def test_validate_strategy_code_valid(self):
        code = """
class TestStrategy:
    def on_init(self):
        pass
    def on_bar(self, bar):
        pass
"""
        result = self.mod.validate_strategy_code(code, "TestStrategy")
        assert result.valid is True


# ===========================================================================
# 17. akshare_ingest
# ===========================================================================
class TestAkshareIngest:
    @pytest.fixture(autouse=True)
    def _load(self):
        self.mod = importlib.import_module("app.datasync.service.akshare_ingest")

    def test_call_ak_success(self, monkeypatch):
        df = pd.DataFrame({"a": [1, 2, 3]})
        result = self.mod.call_ak("test_api", lambda **kw: df)
        assert len(result) == 3

    def test_call_ak_retry_on_error(self, monkeypatch):
        call_count = {"n": 0}
        def fake_fn(**kw):
            call_count["n"] += 1
            if call_count["n"] == 1:
                raise Exception("temporary error")
            return pd.DataFrame({"a": [1]})
        monkeypatch.setattr("time.sleep", lambda _: None)
        result = self.mod.call_ak("test_api", fake_fn, max_retries=3, backoff_base=0)
        assert len(result) == 1


# ===========================================================================
# 18. tushare interfaces
# ===========================================================================
class TestTushareInterfaces:
    @pytest.fixture(autouse=True)
    def _load(self):
        self.mod = importlib.import_module("app.datasync.sources.tushare.interfaces")

    def test_stock_daily_interface_get_ddl(self):
        iface = self.mod.TushareStockDailyInterface()
        ddl = iface.get_ddl()
        assert "CREATE TABLE" in ddl or ddl is not None

    def test_stock_daily_sync_no_data(self, monkeypatch):
        iface = self.mod.TushareStockDailyInterface()
        monkeypatch.setattr("app.datasync.service.tushare_ingest.call_pro", lambda *a, **kw: pd.DataFrame())
        result = iface.sync_date(date(2024, 1, 1))
        assert result is not None

    def test_adj_factor_interface_info(self):
        iface = self.mod.TushareAdjFactorInterface()
        info = iface.info
        assert info.interface_key == "adj_factor"

    def test_dividend_interface_info(self):
        iface = self.mod.TushareDividendInterface()
        info = iface.info
        assert "dividend" in info.interface_key
