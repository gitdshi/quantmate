"""Unit tests for app.domains.extdata.dao.tushare_dao — engine-based DAO."""

from __future__ import annotations

from datetime import date
from unittest.mock import MagicMock, patch, call
import json

import pandas as pd
import pytest

_MOD = "app.domains.extdata.dao.tushare_dao"


@pytest.fixture(autouse=True)
def _mock_engine():
    """Patch the module-level engine before import."""
    with patch(f"{_MOD}.get_tushare_engine", return_value=MagicMock()):
        yield


def _fake_engine_begin():
    """Returns a mock engine whose .begin() yields a mock connection."""
    engine = MagicMock()
    conn = MagicMock()
    ctx = MagicMock()
    ctx.__enter__ = MagicMock(return_value=conn)
    ctx.__exit__ = MagicMock(return_value=False)
    engine.begin.return_value = ctx
    engine.connect.return_value = ctx
    return engine, conn


# ── _clean / _round2 ────────────────────────────────────────────

class TestClean:
    def test_nan_to_none(self):
        import app.domains.extdata.dao.tushare_dao as mod
        assert mod._clean(float("nan")) is None

    def test_normal_value(self):
        import app.domains.extdata.dao.tushare_dao as mod
        assert mod._clean(42) == 42

    def test_numpy_int(self):
        import numpy as np
        import app.domains.extdata.dao.tushare_dao as mod
        result = mod._clean(np.int64(42))
        assert result == 42
        assert isinstance(result, int)

    def test_numpy_float(self):
        import numpy as np
        import app.domains.extdata.dao.tushare_dao as mod
        result = mod._clean(np.float64(3.14))
        assert isinstance(result, float)

    def test_string(self):
        import app.domains.extdata.dao.tushare_dao as mod
        assert mod._clean("hello") == "hello"

    def test_none(self):
        import app.domains.extdata.dao.tushare_dao as mod
        assert mod._clean(None) is None


class TestRound2:
    def test_normal(self):
        import app.domains.extdata.dao.tushare_dao as mod
        assert mod._round2(3.14159) == 3.14

    def test_nan(self):
        import app.domains.extdata.dao.tushare_dao as mod
        assert mod._round2(float("nan")) is None

    def test_none(self):
        import app.domains.extdata.dao.tushare_dao as mod
        assert mod._round2(None) is None


# ── audit_start / audit_finish ───────────────────────────────────

class TestAudit:
    def test_audit_start(self):
        import app.domains.extdata.dao.tushare_dao as mod
        engine, conn = _fake_engine_begin()
        mod.engine = engine
        conn.execute.return_value = MagicMock(lastrowid=42)
        result = mod.audit_start("daily", {"ts_code": "000001.SZ"})
        assert result == 42

    def test_audit_start_creates_missing_ingest_audit_table(self):
        import app.domains.extdata.dao.tushare_dao as mod

        engine, conn = _fake_engine_begin()
        mod.engine = engine
        conn.execute.side_effect = [
            RuntimeError("(1146, \"Table 'tushare.ingest_audit' doesn't exist\")"),
            MagicMock(lastrowid=7),
        ]

        with patch.object(mod, "_ensure_ingest_audit_table") as mock_ensure:
            result = mod.audit_start("daily", {"ts_code": "000001.SZ"})

        assert result == 7
        mock_ensure.assert_called_once()

    def test_audit_finish(self):
        import app.domains.extdata.dao.tushare_dao as mod
        engine, conn = _fake_engine_begin()
        mod.engine = engine
        mod.audit_finish(42, "success", 100)
        conn.execute.assert_called_once()

    def test_audit_finish_ignores_zero_audit_id(self):
        import app.domains.extdata.dao.tushare_dao as mod

        engine, conn = _fake_engine_begin()
        mod.engine = engine

        mod.audit_finish(0, "success", 100)

        conn.execute.assert_not_called()


# ── upsert_daily ─────────────────────────────────────────────────

class TestUpsertDaily:
    def test_empty_df(self):
        import app.domains.extdata.dao.tushare_dao as mod
        engine, conn = _fake_engine_begin()
        mod.engine = engine
        result = mod.upsert_daily(pd.DataFrame())
        assert result == 0

    def test_none_df(self):
        import app.domains.extdata.dao.tushare_dao as mod
        engine, conn = _fake_engine_begin()
        mod.engine = engine
        result = mod.upsert_daily(None)
        assert result == 0

    def test_with_data(self):
        import app.domains.extdata.dao.tushare_dao as mod
        engine, conn = _fake_engine_begin()
        mod.engine = engine
        df = pd.DataFrame({
            "ts_code": ["000001.SZ"],
            "trade_date": ["20240101"],
            "open": [10.0], "high": [11.0], "low": [9.0], "close": [10.5],
            "vol": [1000.0], "amount": [10000.0],
            "pre_close": [9.8], "change": [0.7], "pct_chg": [7.14],
        })
        result = mod.upsert_daily(df)
        assert result >= 0
        assert conn.execute.called


# ── get_all_ts_codes ─────────────────────────────────────────────

class TestGetAllTsCodes:
    def test_returns_list(self):
        import app.domains.extdata.dao.tushare_dao as mod
        engine, conn = _fake_engine_begin()
        mod.engine = engine
        r1 = MagicMock()
        r1.__getitem__ = lambda s, k: "000001.SZ"
        r2 = MagicMock()
        r2.__getitem__ = lambda s, k: "000002.SZ"
        conn.execute.return_value = MagicMock(fetchall=MagicMock(return_value=[r1, r2]))
        result = mod.get_all_ts_codes()
        assert result == ["000001.SZ", "000002.SZ"]


# ── get_max_trade_date ───────────────────────────────────────────

class TestGetMaxTradeDate:
    def test_has_data(self):
        import app.domains.extdata.dao.tushare_dao as mod
        engine, conn = _fake_engine_begin()
        mod.engine = engine
        row = MagicMock()
        row.__getitem__ = lambda self, idx: "20240105"
        conn.execute.return_value = MagicMock(fetchone=MagicMock(return_value=row))
        result = mod.get_max_trade_date("000001.SZ")
        assert result is not None

    def test_no_data(self):
        import app.domains.extdata.dao.tushare_dao as mod
        engine, conn = _fake_engine_begin()
        mod.engine = engine
        conn.execute.return_value = MagicMock(fetchone=MagicMock(return_value=None))
        result = mod.get_max_trade_date("000001.SZ")
        assert result is None


# ── upsert_stock_basic ───────────────────────────────────────────

class TestUpsertStockBasic:
    def test_none(self):
        import app.domains.extdata.dao.tushare_dao as mod
        engine, conn = _fake_engine_begin()
        mod.engine = engine
        result = mod.upsert_stock_basic(None)
        assert result == 0

    def test_with_data(self):
        import app.domains.extdata.dao.tushare_dao as mod
        engine, conn = _fake_engine_begin()
        mod.engine = engine
        df = pd.DataFrame({
            "ts_code": ["000001.SZ"],
            "symbol": ["000001"],
            "name": ["平安银行"],
            "area": ["深圳"],
            "industry": ["银行"],
            "market": ["主板"],
            "list_date": ["19910403"],
            "exchange": ["SZSE"],
            "list_status": ["L"],
        })
        result = mod.upsert_stock_basic(df)
        assert result >= 0


# ── upsert_dividend_df ───────────────────────────────────────────

class TestUpsertDividend:
    def test_empty(self):
        import app.domains.extdata.dao.tushare_dao as mod
        engine, conn = _fake_engine_begin()
        mod.engine = engine
        assert mod.upsert_dividend_df(pd.DataFrame()) == 0

    def test_with_data(self):
        import app.domains.extdata.dao.tushare_dao as mod
        engine, conn = _fake_engine_begin()
        mod.engine = engine
        df = pd.DataFrame({
            "ts_code": ["000001.SZ"],
            "end_date": ["20231231"],
            "ann_date": ["20240305"],
            "div_proc": ["实施"],
            "stk_div": [0.0],
            "stk_bo_rate": [0.0],
            "stk_co_rate": [0.0],
            "cash_div": [0.5],
            "cash_div_tax": [0.45],
            "record_date": ["20240310"],
            "ex_date": ["20240311"],
            "pay_date": ["20240312"],
            "div_listdate": [None],
            "imp_ann_date": [None],
            "base_date": [None],
            "base_share": [None],
        })
        result = mod.upsert_dividend_df(df)
        assert result >= 0


# ── fetch_existing_keys ──────────────────────────────────────────

class TestFetchExistingKeys:
    def test_returns_set(self):
        import app.domains.extdata.dao.tushare_dao as mod
        engine, conn = _fake_engine_begin()
        mod.engine = engine
        rows = [("000001.SZ", "20240101"), ("000002.SZ", "20240101")]
        conn.execute.return_value = MagicMock(fetchall=MagicMock(return_value=rows))
        result = mod.fetch_existing_keys("stock_daily", "trade_date", "20240101", "20240105")
        assert isinstance(result, set)


# ── upsert_index_daily_df ───────────────────────────────────────

class TestUpsertIndexDaily:
    def test_empty(self):
        import app.domains.extdata.dao.tushare_dao as mod
        engine, conn = _fake_engine_begin()
        mod.engine = engine
        assert mod.upsert_index_daily_df(None) == 0

    def test_with_data(self):
        import app.domains.extdata.dao.tushare_dao as mod
        engine, conn = _fake_engine_begin()
        mod.engine = engine
        df = pd.DataFrame({
            "index_code": ["000001.SH"],
            "trade_date": ["20240101"],
            "open": [3000.0], "high": [3100.0], "low": [2900.0], "close": [3050.0],
            "vol": [500000.0], "amount": [60000000.0],
            "pre_close": [2980.0], "change": [70.0], "pct_chg": [2.35],
        })
        result = mod.upsert_index_daily_df(df)
        assert result >= 0


# ── upsert_weekly / monthly ─────────────────────────────────────

class TestUpsertWeeklyMonthly:
    def test_weekly_empty(self):
        import app.domains.extdata.dao.tushare_dao as mod
        engine, conn = _fake_engine_begin()
        mod.engine = engine
        assert mod.upsert_weekly(None) == 0

    def test_monthly_empty(self):
        import app.domains.extdata.dao.tushare_dao as mod
        engine, conn = _fake_engine_begin()
        mod.engine = engine
        assert mod.upsert_monthly(None) == 0


# ── get_failed_ts_codes ──────────────────────────────────────────

class TestGetFailedTsCodes:
    def test_returns_codes(self):
        import app.domains.extdata.dao.tushare_dao as mod
        engine, conn = _fake_engine_begin()
        mod.engine = engine
        rows = [("000001.SZ",), ("000002.SZ",)]
        conn.execute.return_value = MagicMock(fetchall=MagicMock(return_value=rows))
        result = mod.get_failed_ts_codes()
        assert len(result) == 2


# ── fetch_stock_daily_rows ───────────────────────────────────────

class TestFetchStockDailyRows:
    def test_returns_rows(self):
        import app.domains.extdata.dao.tushare_dao as mod
        engine, conn = _fake_engine_begin()
        mod.engine = engine
        fake_rows = [(1, "000001.SZ", "20240101", 10.0, 11.0, 9.0, 10.5)]
        conn.execute.return_value = MagicMock(fetchall=MagicMock(return_value=fake_rows))
        result = mod.fetch_stock_daily_rows("000001.SZ")
        assert len(result) == 1
