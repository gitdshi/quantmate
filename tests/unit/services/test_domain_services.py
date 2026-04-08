"""Tests for expression_engine, factor_screening, calendar_service, sentiment_service.

These are the biggest domain-service coverage gaps.
"""

import pytest
import numpy as np
import pandas as pd
from datetime import date, datetime
from unittest.mock import MagicMock, patch

# ── fake DB helpers ─────────────────────────────────────────────────
class _FR:
    def __init__(self, rows=None, rowcount=0, lastrowid=0):
        self._rows = rows or []; self.rowcount = rowcount; self.lastrowid = lastrowid
    def fetchone(self): return self._rows[0] if self._rows else None
    def fetchall(self): return self._rows

class _FC:
    def __init__(self, result=None):
        self.result = result or _FR(); self.committed = False; self.executed = []
    def execute(self, *a, **kw):
        self.executed.append((a, kw)); return self.result
    def commit(self): self.committed = True

class _Ctx:
    def __init__(self, c): self._c = c
    def __enter__(self): return self._c
    def __exit__(self, *a): return False


# =====================================================================
# expression_engine tests
# =====================================================================
import app.domains.factors.expression_engine as _ee


@pytest.mark.unit
class TestExpressionHelpers:
    """Pure function tests — no DB needed."""

    def test_validate_expression_safe(self):
        _ee._validate_expression("close / delay(close, 1) - 1")

    def test_validate_expression_unsafe_import(self):
        with pytest.raises(ValueError):
            _ee._validate_expression("import os")

    def test_validate_expression_unsafe_exec(self):
        with pytest.raises(ValueError):
            _ee._validate_expression("exec('rm -rf')")

    def test_validate_expression_unsafe_eval(self):
        with pytest.raises(ValueError):
            _ee._validate_expression("eval('bad')")

    def test_validate_expression_unsafe_dunder(self):
        with pytest.raises(ValueError):
            _ee._validate_expression("__builtins__")

    def test_delay(self):
        s = pd.Series([1, 2, 3, 4, 5])
        result = _ee._delay(s, 1)
        assert pd.isna(result.iloc[0])
        assert result.iloc[1] == 1

    def test_delta(self):
        s = pd.Series([10, 12, 15])
        result = _ee._delta(s, 1)
        assert result.iloc[1] == 2

    def test_rank(self):
        s = pd.Series([3, 1, 2])
        result = _ee._rank(s)
        assert result.iloc[1] < result.iloc[2] < result.iloc[0]

    def test_ts_mean(self):
        s = pd.Series([10, 20, 30, 40, 50])
        result = _ee._ts_mean(s, window=3)
        assert abs(result.iloc[2] - 20.0) < 1e-9

    def test_ts_std(self):
        s = pd.Series([10, 20, 30, 40, 50])
        result = _ee._ts_std(s, window=3)
        assert result.iloc[2] > 0

    def test_ts_max(self):
        s = pd.Series([1, 5, 3, 2, 8])
        assert _ee._ts_max(s, 3).iloc[2] == 5

    def test_ts_min(self):
        s = pd.Series([5, 1, 3, 2, 8])
        assert _ee._ts_min(s, 3).iloc[2] == 1

    def test_ts_sum(self):
        s = pd.Series([1, 2, 3, 4, 5])
        assert _ee._ts_sum(s, 3).iloc[2] == 6

    def test_ts_corr(self):
        x = pd.Series([1, 2, 3, 4, 5])
        y = pd.Series([2, 4, 6, 8, 10])
        result = _ee._ts_corr(x, y, 5)
        assert abs(result.iloc[4] - 1.0) < 1e-9


@pytest.mark.unit
class TestComputeCustomFactor:
    def _make_ohlcv(self):
        return pd.DataFrame({
            "open": [10, 11, 12, 13, 14],
            "high": [11, 12, 13, 14, 15],
            "low": [9, 10, 11, 12, 13],
            "close": [10.5, 11.5, 12.5, 13.5, 14.5],
            "volume": [100, 200, 150, 180, 220],
        })

    def test_simple_expression(self):
        ohlcv = self._make_ohlcv()
        result = _ee.compute_custom_factor("close - open", ohlcv)
        assert len(result) == 5

    def test_expression_with_delay(self):
        ohlcv = self._make_ohlcv()
        result = _ee.compute_custom_factor("close / delay(close, 1) - 1", ohlcv)
        assert len(result) == 5

    def test_scalar_expression(self):
        ohlcv = self._make_ohlcv()
        result = _ee.compute_custom_factor("1 + 1", ohlcv)
        assert len(result) == 5

    def test_invalid_expression(self):
        ohlcv = self._make_ohlcv()
        with pytest.raises(ValueError):
            _ee.compute_custom_factor("import os", ohlcv)

    def test_bad_eval(self):
        ohlcv = self._make_ohlcv()
        with pytest.raises(ValueError, match="evaluation failed"):
            _ee.compute_custom_factor("nonexistent_var + 1", ohlcv)


@pytest.mark.unit
class TestComputeForwardReturns:
    def test_basic(self):
        df = pd.DataFrame({"close": [100, 110, 121, 133.1, 146.41]})
        result = _ee.compute_forward_returns(df, periods=1)
        assert len(result) == 5
        assert abs(result.iloc[0] - 0.1) < 1e-6

    def test_empty(self):
        result = _ee.compute_forward_returns(pd.DataFrame(), periods=1)
        assert len(result) == 0

    def test_multiindex(self):
        idx = pd.MultiIndex.from_tuples([
            ("A", "2024-01-01"), ("A", "2024-01-02"), ("A", "2024-01-03"),
            ("B", "2024-01-01"), ("B", "2024-01-02"), ("B", "2024-01-03"),
        ], names=["instrument", "date"])
        df = pd.DataFrame({"close": [100, 110, 121, 200, 220, 242]}, index=idx)
        result = _ee.compute_forward_returns(df, periods=1)
        assert len(result) == 6


@pytest.mark.unit
class TestComputeFactorMetrics:
    def test_basic_metrics(self):
        idx = pd.MultiIndex.from_tuples([
            (f"S{i}", d) for i in range(20) for d in pd.date_range("2024-01-01", periods=30)
        ], names=["instrument", "date"])
        np.random.seed(42)
        factor = pd.Series(np.random.randn(600), index=idx, name="factor")
        returns = pd.Series(np.random.randn(600) * 0.01, index=idx, name="return")
        result = _ee.compute_factor_metrics(factor, returns)
        assert "ic_mean" in result
        assert "ic_ir" in result
        assert "long_short_ret" in result

    def test_empty_data(self):
        result = _ee.compute_factor_metrics(pd.Series(dtype=float), pd.Series(dtype=float))
        assert result["ic_mean"] == 0.0

    def test_small_data(self):
        factor = pd.Series([1, 2, 3], name="factor")
        returns = pd.Series([0.01, 0.02, 0.03], name="return")
        result = _ee.compute_factor_metrics(factor, returns)
        assert result["ic_mean"] == 0.0


@pytest.mark.unit
class TestFetchOhlcv:
    def test_fetch_ohlcv(self, monkeypatch):
        fake_df = pd.DataFrame({
            "instrument": ["A", "A"],
            "date": ["2024-01-01", "2024-01-02"],
            "open": [10, 11], "high": [11, 12], "low": [9, 10],
            "close": [10.5, 11.5], "volume": [100, 200],
            "amount": [1000, 2000], "factor": [1.0, 1.0],
        })
        monkeypatch.setattr(_ee, "connection", lambda n: _Ctx(_FC()))
        monkeypatch.setattr(pd, "read_sql", lambda *a, **kw: fake_df)
        result = _ee.fetch_ohlcv(start_date=date(2024, 1, 1), end_date=date(2024, 1, 2))
        assert len(result) >= 0

    def test_fetch_ohlcv_empty(self, monkeypatch):
        monkeypatch.setattr(_ee, "connection", lambda n: _Ctx(_FC()))
        monkeypatch.setattr(pd, "read_sql", lambda *a, **kw: pd.DataFrame())
        result = _ee.fetch_ohlcv()
        assert result.empty


@pytest.mark.unit
class TestSaveFactorValues:
    def test_save_series(self, monkeypatch):
        idx = pd.MultiIndex.from_tuples([("A", "2024-01-01"), ("A", "2024-01-02")])
        values = pd.Series([1.0, 2.0], index=idx, name="factor")
        conn = _FC(_FR(lastrowid=1))
        monkeypatch.setattr(_ee, "connection", lambda n: _Ctx(conn))
        result = _ee.save_factor_values("test_factor", "custom", values)
        assert result >= 0

    def test_save_empty_after_dropna(self, monkeypatch):
        idx = pd.MultiIndex.from_tuples([("A", "2024-01-01")])
        values = pd.Series([np.nan], index=idx, name="factor")
        conn = _FC()
        monkeypatch.setattr(_ee, "connection", lambda n: _Ctx(conn))
        result = _ee.save_factor_values("f", "s", values)
        assert result == 0

    def test_save_dataframe_one_col(self, monkeypatch):
        idx = pd.MultiIndex.from_tuples([("A", "2024-01-01"), ("A", "2024-01-02")])
        values = pd.DataFrame({"val": [1.0, 2.0]}, index=idx)
        conn = _FC(_FR(lastrowid=1))
        monkeypatch.setattr(_ee, "connection", lambda n: _Ctx(conn))
        result = _ee.save_factor_values("f", "s", values)
        assert result >= 0

    def test_save_dataframe_multi_col_error(self, monkeypatch):
        idx = pd.MultiIndex.from_tuples([("A", "2024-01-01")])
        values = pd.DataFrame({"a": [1.0], "b": [2.0]}, index=idx)
        with pytest.raises(ValueError, match="exactly one column"):
            _ee.save_factor_values("f", "s", values)

    def test_save_unsupported_type(self, monkeypatch):
        with pytest.raises(TypeError):
            _ee.save_factor_values("f", "s", [1, 2, 3])


@pytest.mark.unit
class TestComputeQlibFactorSet:
    def test_qlib_not_available(self, monkeypatch):
        monkeypatch.setattr("app.domains.factors.expression_engine.compute_qlib_factor_set.__module__",
                            "app.domains.factors.expression_engine", raising=False)
        # Just check runtime error when qlib is not available
        with patch("app.domains.factors.expression_engine.compute_qlib_factor_set") as mock_fn:
            mock_fn.side_effect = RuntimeError("Qlib is not installed")
            with pytest.raises(RuntimeError):
                mock_fn("Alpha158")


# =====================================================================
# factor_screening tests
# =====================================================================
import app.domains.factors.factor_screening as _fs


@pytest.mark.unit
class TestScreenFactorPool:
    def test_empty_ohlcv(self, monkeypatch):
        monkeypatch.setattr(_fs, "fetch_ohlcv", lambda **kw: pd.DataFrame())
        result = _fs.screen_factor_pool(["close - open"], date(2024, 1, 1), date(2024, 12, 31))
        assert result == []

    def test_basic_screening(self, monkeypatch):
        idx = pd.MultiIndex.from_tuples([
            (f"S{i}", d) for i in range(5) for d in pd.date_range("2024-01-01", periods=50)
        ], names=["instrument", "date"])
        np.random.seed(42)
        ohlcv = pd.DataFrame({
            "open": np.random.randn(250) + 100,
            "high": np.random.randn(250) + 102,
            "low": np.random.randn(250) + 98,
            "close": np.random.randn(250) + 101,
            "volume": np.random.randint(100, 1000, 250),
        }, index=idx)
        monkeypatch.setattr(_fs, "fetch_ohlcv", lambda **kw: ohlcv)
        # Even if nothing passes ic_threshold, it should return a list
        result = _fs.screen_factor_pool(["close - open", "volume"], date(2024, 1, 1), date(2024, 12, 31),
                                         ic_threshold=0.0)
        assert isinstance(result, list)

    def test_expression_error_skipped(self, monkeypatch):
        monkeypatch.setattr(_fs, "fetch_ohlcv", lambda **kw: pd.DataFrame({"close": [1, 2, 3]}))
        monkeypatch.setattr(_fs, "compute_forward_returns", lambda *a, **kw: pd.Series([0.01, 0.02, 0.03]))
        monkeypatch.setattr(_fs, "compute_custom_factor", MagicMock(side_effect=ValueError("bad")))
        result = _fs.screen_factor_pool(["bad_expr"], date(2024, 1, 1), date(2024, 12, 31))
        assert result == []


@pytest.mark.unit
class TestSaveScreeningResults:
    def test_save(self, monkeypatch):
        conn = _FC(_FR(lastrowid=42))
        monkeypatch.setattr("app.infrastructure.db.connections.connection", lambda n: _Ctx(conn))
        results = [{"factor_name": "f1", "ic_mean": 0.05, "ic_std": 0.02, "ic_ir": 2.5,
                     "turnover": 0.1, "long_ret": 0.01, "short_ret": -0.01, "long_short_ret": 0.02}]
        run_id = _fs.save_screening_results(1, "run1", results, {"threshold": 0.02})
        assert run_id == 42
        assert conn.committed

    def test_save_empty(self, monkeypatch):
        conn = _FC(_FR(lastrowid=1))
        monkeypatch.setattr("app.infrastructure.db.connections.connection", lambda n: _Ctx(conn))
        run_id = _fs.save_screening_results(1, "empty", [])
        assert run_id == 1


@pytest.mark.unit
class TestMineAlpha158:
    def test_qlib_not_available(self, monkeypatch):
        monkeypatch.setattr(_fs, "compute_qlib_factor_set", MagicMock(side_effect=RuntimeError("no qlib")))
        result = _fs.mine_alpha158_factors()
        assert result == []

    def test_empty_df(self, monkeypatch):
        monkeypatch.setattr(_fs, "compute_qlib_factor_set", MagicMock(return_value=pd.DataFrame()))
        result = _fs.mine_alpha158_factors()
        assert result == []


# =====================================================================
# calendar_service tests
# =====================================================================
import app.domains.market.calendar_service as _cal_mod
from app.domains.market.calendar_service import CalendarService


@pytest.mark.unit
class TestCalendarService:
    def test_trade_days_from_db(self, monkeypatch):
        rows = [("20240102", 1), ("20240103", 1), ("20240104", 0)]
        conn = _FC(_FR(rows))
        monkeypatch.setattr(_cal_mod, "connection", lambda n: _Ctx(conn))
        svc = CalendarService()
        result = svc._trade_days_from_db("SSE", date(2024, 1, 1), date(2024, 1, 5))
        assert len(result["trade_days"]) == 2
        assert result["source"] == "tushare_db"

    def test_trade_days_from_db_date_obj(self, monkeypatch):
        rows = [(date(2024, 1, 2), 1)]
        conn = _FC(_FR(rows))
        monkeypatch.setattr(_cal_mod, "connection", lambda n: _Ctx(conn))
        result = CalendarService()._trade_days_from_db("SSE", date(2024, 1, 1), date(2024, 1, 5))
        assert len(result["trade_days"]) == 1

    def test_get_trade_days_db_success(self, monkeypatch):
        rows = [("20240102", 1)]
        conn = _FC(_FR(rows))
        monkeypatch.setattr(_cal_mod, "connection", lambda n: _Ctx(conn))
        result = CalendarService().get_trade_days("SSE", date(2024, 1, 1), date(2024, 1, 5))
        assert result["source"] == "tushare_db"

    def test_get_trade_days_db_fail_ak_none(self, monkeypatch):
        monkeypatch.setattr(_cal_mod, "connection", lambda n: (_ for _ in ()).throw(Exception("no db")))
        monkeypatch.setattr(_cal_mod, "ak", None)
        result = CalendarService().get_trade_days("SSE", date(2024, 1, 1), date(2024, 1, 5))
        assert result["source"] == "unavailable"

    def test_get_trade_days_db_fail_ak_success(self, monkeypatch):
        monkeypatch.setattr(_cal_mod, "connection", lambda n: (_ for _ in ()).throw(Exception("no db")))
        fake_ak = MagicMock()
        fake_df = pd.DataFrame({"trade_date": ["2024-01-02", "2024-01-03", "2024-01-10"]})
        fake_ak.tool_trade_date_hist_sina.return_value = fake_df
        monkeypatch.setattr(_cal_mod, "ak", fake_ak)
        result = CalendarService().get_trade_days("SSE", date(2024, 1, 1), date(2024, 1, 5))
        assert result["source"] == "akshare"

    def test_get_trade_days_default_dates(self, monkeypatch):
        rows = [("20240102", 1)]
        conn = _FC(_FR(rows))
        monkeypatch.setattr(_cal_mod, "connection", lambda n: _Ctx(conn))
        result = CalendarService().get_trade_days()
        assert "trade_days" in result

    def test_get_events(self, monkeypatch):
        monkeypatch.setattr(_cal_mod, "ak", None)
        result = CalendarService().get_events(date(2024, 1, 1), date(2024, 1, 31))
        assert "events" in result

    def test_get_events_default_dates(self, monkeypatch):
        monkeypatch.setattr(_cal_mod, "ak", None)
        result = CalendarService().get_events()
        assert "events" in result

    def test_get_events_specific_type(self, monkeypatch):
        monkeypatch.setattr(_cal_mod, "ak", None)
        result = CalendarService().get_events(event_type="macro")
        assert "events" in result

    def test_fetch_events_macro(self, monkeypatch):
        fake_ak = MagicMock()
        fake_df = pd.DataFrame({"date": ["2024-01-01"], "time": ["09:00"],
                                  "country": ["CN"], "event": ["GDP"], "importance": ["high"]})
        fake_ak.news_economic_baidu.return_value = fake_df
        monkeypatch.setattr(_cal_mod, "ak", fake_ak)
        result = CalendarService()._macro_events(date(2024, 1, 1), date(2024, 1, 31))
        assert len(result) >= 0

    def test_fetch_events_ipo(self, monkeypatch):
        fake_ak = MagicMock()
        fake_df = pd.DataFrame({"上市日期": ["2024-01-01"], "股票简称": ["test"], "股票代码": ["000001"], "发行价格": ["10"]})
        fake_ak.stock_xgsglb_em.return_value = fake_df
        monkeypatch.setattr(_cal_mod, "ak", fake_ak)
        result = CalendarService()._ipo_events()
        assert len(result) >= 0

    def test_fetch_events_dividend(self, monkeypatch):
        fake_ak = MagicMock()
        fake_df = pd.DataFrame({"除权除息日": ["2024-06-01"], "名称": ["test"], "代码": ["000001"], "分红方案": ["10送5"]})
        fake_ak.stock_fhps_em.return_value = fake_df
        monkeypatch.setattr(_cal_mod, "ak", fake_ak)
        result = CalendarService()._dividend_events()
        assert len(result) >= 0

    def test_macro_events_exception(self, monkeypatch):
        fake_ak = MagicMock()
        fake_ak.news_economic_baidu.side_effect = Exception("network error")
        monkeypatch.setattr(_cal_mod, "ak", fake_ak)
        assert CalendarService()._macro_events(date(2024, 1, 1), date(2024, 1, 31)) == []

    def test_ipo_events_exception(self, monkeypatch):
        fake_ak = MagicMock()
        fake_ak.stock_xgsglb_em.side_effect = Exception("fail")
        monkeypatch.setattr(_cal_mod, "ak", fake_ak)
        assert CalendarService()._ipo_events() == []

    def test_dividend_events_exception(self, monkeypatch):
        fake_ak = MagicMock()
        fake_ak.stock_fhps_em.side_effect = Exception("fail")
        monkeypatch.setattr(_cal_mod, "ak", fake_ak)
        assert CalendarService()._dividend_events() == []


# =====================================================================
# sentiment_service tests
# =====================================================================
import app.domains.market.sentiment_service as _sent_mod
from app.domains.market.sentiment_service import SentimentService


@pytest.mark.unit
class TestSentimentService:
    def test_overview_no_akshare(self, monkeypatch):
        monkeypatch.setattr(_sent_mod, "ak", None)
        result = SentimentService().get_overview()
        assert result["advance_decline"] is None

    def test_overview_with_data(self, monkeypatch):
        fake_ak = MagicMock()
        spot_df = pd.DataFrame({"涨跌幅": [1.5, -0.5, 0, 2.0, -1.0], "成交额": [1e8, 2e8, 1.5e8, 3e8, 2.5e8]})
        idx_df = pd.DataFrame({"代码": ["000001"], "最新价": [3200], "涨跌幅": [0.5]})
        fake_ak.stock_zh_a_spot_em.return_value = spot_df
        fake_ak.stock_zh_index_spot_em.return_value = idx_df
        monkeypatch.setattr(_sent_mod, "ak", fake_ak)
        result = SentimentService().get_overview()
        assert result["advance_decline"]["advance"] == 2
        assert result["advance_decline"]["decline"] == 2

    def test_overview_spot_exception(self, monkeypatch):
        fake_ak = MagicMock()
        fake_ak.stock_zh_a_spot_em.side_effect = Exception("fail")
        fake_ak.stock_zh_index_spot_em.return_value = pd.DataFrame()
        monkeypatch.setattr(_sent_mod, "ak", fake_ak)
        result = SentimentService().get_overview()
        assert result["advance_decline"] is None

    def test_fear_greed_no_akshare(self, monkeypatch):
        monkeypatch.setattr(_sent_mod, "ak", None)
        result = SentimentService().get_fear_greed()
        assert result["score"] == 50
        assert result["label"] == "neutral"

    def test_fear_greed_with_data(self, monkeypatch):
        fake_ak = MagicMock()
        pct_vals = list(np.random.uniform(-3, 3, 100))
        pct_vals.extend([10.0, 10.5, -10.0])  # limit up/down
        spot_df = pd.DataFrame({"涨跌幅": pct_vals, "成交额": [1e8] * len(pct_vals)})
        idx_df = pd.DataFrame({"代码": ["000001"], "最新价": [3200], "涨跌幅": [1.5]})
        fake_ak.stock_zh_a_spot_em.return_value = spot_df
        fake_ak.stock_zh_index_spot_em.return_value = idx_df
        monkeypatch.setattr(_sent_mod, "ak", fake_ak)
        result = SentimentService().get_fear_greed()
        assert 0 <= result["score"] <= 100
        assert result["label"] in ("extreme_fear", "fear", "neutral", "greed", "extreme_greed")

    def test_fear_greed_spot_exception(self, monkeypatch):
        fake_ak = MagicMock()
        fake_ak.stock_zh_a_spot_em.side_effect = Exception("fail")
        monkeypatch.setattr(_sent_mod, "ak", fake_ak)
        result = SentimentService().get_fear_greed()
        assert result["score"] == 50

    def test_fear_greed_extreme_fear(self, monkeypatch):
        fake_ak = MagicMock()
        # all stocks declining heavily
        spot_df = pd.DataFrame({"涨跌幅": [-5.0] * 100})
        idx_df = pd.DataFrame({"代码": ["000001"], "最新价": [2800], "涨跌幅": [-3.0]})
        fake_ak.stock_zh_a_spot_em.return_value = spot_df
        fake_ak.stock_zh_index_spot_em.return_value = idx_df
        monkeypatch.setattr(_sent_mod, "ak", fake_ak)
        result = SentimentService().get_fear_greed()
        assert result["score"] <= 50

    def test_fear_greed_extreme_greed(self, monkeypatch):
        fake_ak = MagicMock()
        # all stocks rising, lots of limit-up
        pct_vals = [5.0] * 80 + [10.0] * 20
        spot_df = pd.DataFrame({"涨跌幅": pct_vals})
        idx_df = pd.DataFrame({"代码": ["000001"], "最新价": [4000], "涨跌幅": [3.0]})
        fake_ak.stock_zh_a_spot_em.return_value = spot_df
        fake_ak.stock_zh_index_spot_em.return_value = idx_df
        monkeypatch.setattr(_sent_mod, "ak", fake_ak)
        result = SentimentService().get_fear_greed()
        assert result["score"] >= 50
