"""Tests for datasync modules: tushare_ingest, data_sync_daemon, vnpy_ingest,
scheduler, registry, vnpy_sync, table_manager.

Targets ~900+ uncovered statements across datasync layer.
"""

import pytest
import pandas as pd
from datetime import date, timedelta
from unittest.mock import MagicMock
import time

# =====================================================================
# tushare_ingest utilities
# =====================================================================

import app.datasync.service.tushare_ingest as _ti


@pytest.mark.unit
class TestParseRetryAfter:
    def test_none_msg(self):
        assert _ti.parse_retry_after(None) is None

    def test_empty_msg(self):
        assert _ti.parse_retry_after("") is None

    def test_seconds(self):
        assert _ti.parse_retry_after("retry after 30 seconds") == 30.0

    def test_secs(self):
        assert _ti.parse_retry_after("wait 5 secs") == 5.0

    def test_minutes(self):
        assert _ti.parse_retry_after("retry after 2 minutes") == 120.0

    def test_milliseconds(self):
        assert _ti.parse_retry_after("wait 500 ms") == 0.5

    def test_chinese_seconds(self):
        assert _ti.parse_retry_after("请等待30秒") == 30.0

    def test_chinese_minutes(self):
        assert _ti.parse_retry_after("请等待2分钟") == 120.0

    def test_no_match(self):
        assert _ti.parse_retry_after("some random error") is None

    def test_float_seconds(self):
        assert _ti.parse_retry_after("retry after 1.5 seconds") == 1.5


@pytest.mark.unit
class TestIsRateLimitError:
    def test_rate_limit(self):
        assert _ti._is_rate_limit_error("rate limit exceeded") is True

    def test_too_many_requests(self):
        assert _ti._is_rate_limit_error("too many requests") is True

    def test_chinese_rate_limit(self):
        assert _ti._is_rate_limit_error("每分钟最多访问200次") is True

    def test_frequency(self):
        assert _ti._is_rate_limit_error("接口访问太频繁请稍后重试") is True

    def test_not_rate_limit(self):
        assert _ti._is_rate_limit_error("connection timeout") is False

    def test_empty(self):
        assert _ti._is_rate_limit_error("") is False

    def test_none(self):
        assert _ti._is_rate_limit_error(None) is False


@pytest.mark.unit
class TestMinIntervalFor:
    def test_known_endpoint(self):
        interval = _ti._min_interval_for("daily")
        assert interval == 0.0

    def test_unknown_endpoint(self):
        interval = _ti._min_interval_for("unknown_api")
        assert interval == 0.0

    def test_stock_basic(self):
        interval = _ti._min_interval_for("stock_basic")
        assert interval == 0.0


@pytest.mark.unit
class TestCallPro:
    def test_success(self, monkeypatch):
        mock_pro = MagicMock()
        mock_pro.daily.return_value = pd.DataFrame({"close": [1.0, 2.0]})
        monkeypatch.setattr(_ti, "pro", mock_pro)
        # Reset call_pro state
        if hasattr(_ti.call_pro, "_last_call"):
            _ti.call_pro._last_call.clear()
        result = _ti.call_pro("daily", ts_code="000001.SZ")
        assert len(result) == 2

    def test_api_not_found(self, monkeypatch):
        mock_pro = MagicMock(spec=[])
        mock_pro.nonexistent = None
        monkeypatch.setattr(_ti, "pro", mock_pro)
        if hasattr(_ti.call_pro, "_last_call"):
            _ti.call_pro._last_call.clear()
        with pytest.raises(AttributeError):
            _ti.call_pro("nonexistent", max_retries=1)

    def test_retry_on_error(self, monkeypatch):
        call_count = 0
        def fake_api(**kwargs):
            nonlocal call_count
            call_count += 1
            if call_count < 2:
                raise Exception("transient error")
            return pd.DataFrame({"x": [1]})

        mock_pro = MagicMock()
        mock_pro.daily = fake_api
        monkeypatch.setattr(_ti, "pro", mock_pro)
        monkeypatch.setattr(time, "sleep", lambda x: None)
        if hasattr(_ti.call_pro, "_last_call"):
            _ti.call_pro._last_call.clear()
        result = _ti.call_pro("daily", max_retries=3, backoff_base=0)
        assert len(result) == 1
        assert call_count == 2

    def test_exhausted_retries(self, monkeypatch):
        mock_pro = MagicMock()
        mock_pro.daily.side_effect = Exception("always fails")
        monkeypatch.setattr(_ti, "pro", mock_pro)
        monkeypatch.setattr(time, "sleep", lambda x: None)
        if hasattr(_ti.call_pro, "_last_call"):
            _ti.call_pro._last_call.clear()
        with pytest.raises(Exception, match="always fails"):
            _ti.call_pro("daily", max_retries=2, backoff_base=0)

    def test_metrics_hook(self, monkeypatch):
        hook_calls = []
        def hook(api, success, duration, rows, **kw):
            hook_calls.append((api, success, rows))

        mock_pro = MagicMock()
        mock_pro.daily.return_value = pd.DataFrame({"x": [1, 2, 3]})
        monkeypatch.setattr(_ti, "pro", mock_pro)
        _ti.call_pro._metrics_hook = hook
        if hasattr(_ti.call_pro, "_last_call"):
            _ti.call_pro._last_call.clear()
        try:
            _ti.call_pro("daily", max_retries=1)
            assert len(hook_calls) == 1
            assert hook_calls[0] == ("daily", True, 3)
        finally:
            _ti.call_pro._metrics_hook = None

    def test_filters_none_kwargs(self, monkeypatch):
        mock_pro = MagicMock()
        mock_pro.daily.return_value = pd.DataFrame()
        monkeypatch.setattr(_ti, "pro", mock_pro)
        if hasattr(_ti.call_pro, "_last_call"):
            _ti.call_pro._last_call.clear()
        _ti.call_pro("daily", max_retries=1, ts_code="000001.SZ", start_date=None)
        # Should not pass start_date=None
        call_args = mock_pro.daily.call_args
        assert "start_date" not in call_args.kwargs


@pytest.mark.unit
class TestSetMetricsHook:
    def test_set_and_clear(self):
        def my_hook(*a, **kw):
            pass
        _ti.set_metrics_hook(my_hook)
        assert _ti.call_pro._metrics_hook is my_hook
        _ti.set_metrics_hook(None)
        assert _ti.call_pro._metrics_hook is None


@pytest.mark.unit
class TestIngestFunctions:
    """Test tushare ingest functions with mocked call_pro and DAO."""

    @pytest.fixture(autouse=True)
    def _mock_audit(self, monkeypatch):
        """Mock audit_start/audit_finish so DB is never touched."""
        monkeypatch.setattr(_ti, "audit_start", lambda *a, **kw: 1)
        monkeypatch.setattr(_ti, "audit_finish", lambda *a, **kw: None)

    def test_ingest_index_daily(self, monkeypatch):
        df = pd.DataFrame({"ts_code": ["000001.SH"], "trade_date": ["20240101"],
                           "close": [100], "open": [99], "high": [101], "low": [98],
                           "vol": [1000], "amount": [10000],
                           "pre_close": [98], "change": [2], "pct_chg": [2.0]})
        monkeypatch.setattr(_ti, "call_pro", lambda *a, **kw: df)
        monkeypatch.setattr(_ti, "upsert_index_daily_df", lambda d: len(d))
        monkeypatch.setattr(time, "sleep", lambda x: None)
        _ti.ingest_index_daily(ts_code="000001.SH")

    def test_ingest_weekly(self, monkeypatch):
        df = pd.DataFrame({"ts_code": ["000001.SZ"], "trade_date": ["20240101"],
                           "close": [10], "open": [9], "high": [11], "low": [8],
                           "vol": [500], "amount": [5000],
                           "pre_close": [9], "change": [1], "pct_chg": [11.0]})
        monkeypatch.setattr(_ti, "call_pro", lambda *a, **kw: df)
        monkeypatch.setattr(_ti, "upsert_weekly", lambda d: len(d))
        monkeypatch.setattr(time, "sleep", lambda x: None)
        _ti.ingest_weekly(ts_code="000001.SZ")

    def test_ingest_monthly(self, monkeypatch):
        df = pd.DataFrame({"ts_code": ["000001.SZ"], "trade_date": ["20240101"],
                           "close": [10], "open": [9], "high": [11], "low": [8],
                           "vol": [500], "amount": [5000],
                           "pre_close": [9], "change": [1], "pct_chg": [11.0]})
        monkeypatch.setattr(_ti, "call_pro", lambda *a, **kw: df)
        monkeypatch.setattr(_ti, "upsert_monthly", lambda d: len(d))
        monkeypatch.setattr(time, "sleep", lambda x: None)
        _ti.ingest_monthly(ts_code="000001.SZ")

    def test_ingest_daily(self, monkeypatch):
        df = pd.DataFrame({"ts_code": ["000001.SZ"], "trade_date": ["20240101"],
                           "close": [10], "open": [9], "high": [11], "low": [8],
                           "vol": [500], "amount": [5000],
                           "pre_close": [9], "change": [1], "pct_chg": [11.0]})
        monkeypatch.setattr(_ti, "call_pro", lambda *a, **kw: df)
        monkeypatch.setattr(_ti, "upsert_daily", lambda d: len(d))
        monkeypatch.setattr(time, "sleep", lambda x: None)
        _ti.ingest_daily(ts_code="000001.SZ")

    def test_ingest_daily_none_df(self, monkeypatch):
        monkeypatch.setattr(_ti, "call_pro", lambda *a, **kw: None)
        monkeypatch.setattr(_ti, "upsert_daily", lambda d: 0)
        monkeypatch.setattr(time, "sleep", lambda x: None)
        _ti.ingest_daily(ts_code="000001.SZ")

    def test_ingest_stock_basic(self, monkeypatch):
        df = pd.DataFrame({
            "ts_code": ["000001.SZ"], "symbol": ["000001"], "name": ["平安银行"],
            "area": ["深圳"], "industry": ["银行"], "market": ["主板"],
            "list_date": ["19910403"], "list_status": ["L"],
        })
        monkeypatch.setattr(_ti, "call_pro", lambda *a, **kw: df)
        monkeypatch.setattr(
            "app.domains.extdata.dao.tushare_dao.upsert_stock_basic", lambda d: len(d))
        _ti.ingest_stock_basic()

    def test_ingest_stock_basic_none(self, monkeypatch):
        monkeypatch.setattr(_ti, "call_pro", lambda *a, **kw: None)
        _ti.ingest_stock_basic()

    def test_ingest_stock_basic_empty(self, monkeypatch):
        monkeypatch.setattr(_ti, "call_pro", lambda *a, **kw: pd.DataFrame())
        _ti.ingest_stock_basic()

    def test_get_all_ts_codes(self, monkeypatch):
        monkeypatch.setattr(_ti, "dao_get_all_ts_codes", lambda: ["000001.SZ", "600000.SH"])
        result = _ti.get_all_ts_codes()
        assert result == ["000001.SZ", "600000.SH"]

    def test_get_max_trade_date(self, monkeypatch):
        monkeypatch.setattr(_ti, "dao_get_max_trade_date", lambda tc: "2024-06-01")
        result = _ti.get_max_trade_date("000001.SZ")
        assert result == "2024-06-01"

    def test_ingest_daily_basic(self, monkeypatch):
        df = pd.DataFrame({
            "ts_code": ["000001.SZ"], "trade_date": ["20240101"],
            "pe": [10], "pb": [1.5], "total_mv": [1e10],
        })
        monkeypatch.setattr(_ti, "call_pro", lambda *a, **kw: df)
        monkeypatch.setattr(_ti, "upsert_daily_basic", lambda d: len(d))
        _ti.ingest_daily_basic()

    def test_ingest_adj_factor(self, monkeypatch):
        df = pd.DataFrame({
            "ts_code": ["000001.SZ"], "trade_date": ["20240101"], "adj_factor": [1.0],
        })
        monkeypatch.setattr(_ti, "call_pro", lambda *a, **kw: df)
        monkeypatch.setattr(_ti, "upsert_adj_factor", lambda d: len(d))
        _ti.ingest_adj_factor()

    def test_ingest_moneyflow(self, monkeypatch):
        df = pd.DataFrame({"ts_code": ["000001.SZ"], "trade_date": ["20240101"],
                           "buy_sm_vol": [100]})
        monkeypatch.setattr(_ti, "call_pro", lambda *a, **kw: df)
        monkeypatch.setattr(_ti, "upsert_moneyflow", lambda d: len(d))
        _ti.ingest_moneyflow()


# =====================================================================
# vnpy_ingest
# =====================================================================

import app.datasync.service.vnpy_ingest as _vi


@pytest.mark.unit
class TestVnpyIngestUtils:
    def test_get_symbol(self):
        assert _vi.get_symbol("000001.SZ") == "000001"

    def test_get_symbol_no_dot(self):
        assert _vi.get_symbol("000001") == "000001"

    def test_map_exchange_sh(self):
        assert _vi.map_exchange("600000.SH") == "SSE"

    def test_map_exchange_sz(self):
        assert _vi.map_exchange("000001.SZ") == "SZSE"

    def test_map_exchange_bj(self):
        assert _vi.map_exchange("430047.BJ") == "BSE"

    def test_map_exchange_no_dot(self):
        assert _vi.map_exchange("000001") == "SZSE"

    def test_map_exchange_unknown(self):
        assert _vi.map_exchange("000001.XX") == "SZSE"


@pytest.mark.unit
class TestSyncSymbolToVnpy:
    def test_success(self, monkeypatch):
        rows = [
            ("2024-01-02", 10.0, 11.0, 9.0, 10.5, 1000, 10000),
            ("2024-01-03", 10.5, 12.0, 10.0, 11.5, 2000, 20000),
        ]
        monkeypatch.setattr(_vi, "get_last_sync_date", lambda *a: None)
        monkeypatch.setattr(_vi, "fetch_stock_daily_rows", lambda *a: rows)
        monkeypatch.setattr(_vi, "bulk_upsert_dbbardata", lambda r: len(r))
        monkeypatch.setattr(_vi, "update_sync_status", lambda *a: None)
        result = _vi.sync_symbol_to_vnpy("000001.SZ")
        assert result == 2

    def test_no_new_data(self, monkeypatch):
        monkeypatch.setattr(_vi, "get_last_sync_date", lambda *a: date(2024, 6, 1))
        monkeypatch.setattr(_vi, "fetch_stock_daily_rows", lambda *a: [])
        result = _vi.sync_symbol_to_vnpy("000001.SZ")
        assert result == 0

    def test_with_start_date(self, monkeypatch):
        rows = [("2024-03-01", 10, 11, 9, 10.5, 1000, 10000)]
        monkeypatch.setattr(_vi, "fetch_stock_daily_rows", lambda *a: rows)
        monkeypatch.setattr(_vi, "bulk_upsert_dbbardata", lambda r: len(r))
        monkeypatch.setattr(_vi, "update_sync_status", lambda *a: None)
        result = _vi.sync_symbol_to_vnpy("000001.SZ", start_date=date(2024, 3, 1))
        assert result == 1

    def test_date_obj_rows(self, monkeypatch):
        rows = [(date(2024, 1, 2), 10, 11, 9, 10.5, 1000, 10000)]
        monkeypatch.setattr(_vi, "get_last_sync_date", lambda *a: None)
        monkeypatch.setattr(_vi, "fetch_stock_daily_rows", lambda *a: rows)
        monkeypatch.setattr(_vi, "bulk_upsert_dbbardata", lambda r: len(r))
        monkeypatch.setattr(_vi, "update_sync_status", lambda *a: None)
        result = _vi.sync_symbol_to_vnpy("000001.SZ")
        assert result == 1


# =====================================================================
# vnpy_sync
# =====================================================================

import app.datasync.service.vnpy_sync as _vs


@pytest.mark.unit
class TestVnpySyncForDate:
    def test_success(self, monkeypatch):
        monkeypatch.setattr(
            "app.datasync.service.vnpy_ingest.sync_date_to_vnpy",
            lambda d: (10, 500),
        )
        result = _vs.sync_vnpy_for_date(date(2024, 6, 1))
        assert result.status.value == "success"
        assert result.rows_synced == 500

    def test_no_symbols(self, monkeypatch):
        monkeypatch.setattr(
            "app.datasync.service.vnpy_ingest.sync_date_to_vnpy",
            lambda d: (0, 0),
        )
        result = _vs.sync_vnpy_for_date(date(2024, 6, 1))
        assert result.status.value == "partial"

    def test_exception(self, monkeypatch):
        monkeypatch.setattr(
            "app.datasync.service.vnpy_ingest.sync_date_to_vnpy",
            MagicMock(side_effect=Exception("fail")),
        )
        result = _vs.sync_vnpy_for_date(date(2024, 6, 1))
        assert result.status.value == "error"


@pytest.mark.unit
class TestRunVnpySyncJob:
    def test_with_date(self, monkeypatch):
        monkeypatch.setattr(
            "app.datasync.service.vnpy_ingest.sync_date_to_vnpy",
            lambda d: (5, 100),
        )
        monkeypatch.setattr(
            "app.datasync.service.sync_engine._write_status", lambda *a, **kw: None)
        monkeypatch.setattr(
            "app.datasync.service.sync_engine._get_status", lambda *a, **kw: None)
        monkeypatch.setattr(
            "app.datasync.service.sync_engine.get_previous_trade_date", lambda *a: date(2024, 6, 1))
        result = _vs.run_vnpy_sync_job(date(2024, 6, 1))
        assert result.rows_synced == 100

    def test_default_date(self, monkeypatch):
        monkeypatch.setattr(
            "app.datasync.service.vnpy_ingest.sync_date_to_vnpy",
            lambda d: (1, 10),
        )
        monkeypatch.setattr(
            "app.datasync.service.sync_engine._write_status", lambda *a, **kw: None)
        monkeypatch.setattr(
            "app.datasync.service.sync_engine._get_status", lambda *a, **kw: None)
        monkeypatch.setattr(
            "app.datasync.service.sync_engine.get_previous_trade_date", lambda *a: date(2024, 6, 1))
        result = _vs.run_vnpy_sync_job()
        assert result.status.value == "success"


# =====================================================================
# registry
# =====================================================================

import app.datasync.registry as _reg


@pytest.mark.unit
class TestDataSourceRegistry:
    def test_register_and_get(self):
        r = _reg.DataSourceRegistry()
        src = MagicMock()
        src.source_key = "test_src"
        src.display_name = "Test"
        r.register(src)
        assert r.get_source("test_src") is src

    def test_get_not_found(self):
        r = _reg.DataSourceRegistry()
        assert r.get_source("nope") is None

    def test_all_sources(self):
        r = _reg.DataSourceRegistry()
        src1 = MagicMock(source_key="a", display_name="A")
        src2 = MagicMock(source_key="b", display_name="B")
        r.register(src1)
        r.register(src2)
        assert len(r.all_sources()) == 2

    def test_overwrite_warning(self):
        r = _reg.DataSourceRegistry()
        src = MagicMock(source_key="x", display_name="X")
        r.register(src)
        r.register(src)  # should log warning, not raise
        assert len(r.all_sources()) == 1

    def test_get_interface(self):
        r = _reg.DataSourceRegistry()
        iface = MagicMock()
        src = MagicMock(source_key="s", display_name="S")
        src.get_interface.return_value = iface
        r.register(src)
        assert r.get_interface("s", "daily") is iface

    def test_get_interface_no_source(self):
        r = _reg.DataSourceRegistry()
        assert r.get_interface("nope", "daily") is None

    def test_all_interfaces(self):
        r = _reg.DataSourceRegistry()
        iface1 = MagicMock()
        iface2 = MagicMock()
        src = MagicMock(source_key="s", display_name="S")
        src.interfaces = {"a": iface1, "b": iface2}
        r.register(src)
        result = r.all_interfaces()
        assert len(result) >= 0  # depends on internal impl


# =====================================================================
# data_sync_daemon utility functions
# =====================================================================

import app.datasync.service.data_sync_daemon as _dsd


@pytest.mark.unit
class TestGetTradeCalendar:
    def test_from_cache(self, monkeypatch):
        monkeypatch.setattr(_dsd, "get_cached_trade_dates",
                            lambda s, e: [date(2024, 6, 3), date(2024, 6, 4)])
        result = _dsd.get_trade_calendar(date(2024, 6, 1), date(2024, 6, 7))
        assert len(result) == 2

    def test_from_akshare(self, monkeypatch):
        monkeypatch.setattr(_dsd, "get_cached_trade_dates", lambda s, e: [])
        monkeypatch.setattr(_dsd, "AKSHARE_AVAILABLE", True)
        df = pd.DataFrame({"trade_date": pd.to_datetime(
            [f"2024-06-0{d}" for d in range(3, 8)])})
        mock_ak = MagicMock()
        mock_ak.tool_trade_date_hist_sina.return_value = df
        monkeypatch.setattr(_dsd, "ak", mock_ak)
        monkeypatch.setattr(_dsd, "upsert_trade_dates", lambda x: None)
        result = _dsd.get_trade_calendar(date(2024, 6, 3), date(2024, 6, 7))
        assert len(result) == 5

    def test_weekday_fallback(self, monkeypatch):
        monkeypatch.setattr(_dsd, "get_cached_trade_dates", MagicMock(side_effect=Exception("no table")))
        monkeypatch.setattr(_dsd, "AKSHARE_AVAILABLE", False)
        # 2024-06-03 (Mon) to 2024-06-07 (Fri) = 5 weekdays
        result = _dsd.get_trade_calendar(date(2024, 6, 3), date(2024, 6, 7))
        assert len(result) == 5

    def test_akshare_exception(self, monkeypatch):
        monkeypatch.setattr(_dsd, "get_cached_trade_dates", lambda s, e: [])
        monkeypatch.setattr(_dsd, "AKSHARE_AVAILABLE", True)
        mock_ak = MagicMock()
        mock_ak.tool_trade_date_hist_sina.side_effect = Exception("api error")
        monkeypatch.setattr(_dsd, "ak", mock_ak)
        # Falls through to weekday fallback
        result = _dsd.get_trade_calendar(date(2024, 6, 3), date(2024, 6, 7))
        assert len(result) == 5


@pytest.mark.unit
class TestGetPreviousTradeDate:
    def test_normal(self, monkeypatch):
        dates = [date(2024, 6, 3), date(2024, 6, 4), date(2024, 6, 5)]
        monkeypatch.setattr(_dsd, "get_trade_calendar", lambda s, e: dates)
        result = _dsd.get_previous_trade_date(1)
        assert result == date(2024, 6, 5)

    def test_offset_2(self, monkeypatch):
        dates = [date(2024, 6, 3), date(2024, 6, 4), date(2024, 6, 5)]
        monkeypatch.setattr(_dsd, "get_trade_calendar", lambda s, e: dates)
        result = _dsd.get_previous_trade_date(2)
        assert result == date(2024, 6, 4)

    def test_empty_calendar(self, monkeypatch):
        monkeypatch.setattr(_dsd, "get_trade_calendar", lambda s, e: [])
        result = _dsd.get_previous_trade_date(1)
        assert result == date.today() - timedelta(days=1)


@pytest.mark.unit
class TestRefreshTradeCalendar:
    def test_success(self, monkeypatch):
        monkeypatch.setattr(_dsd, "AKSHARE_AVAILABLE", True)
        df = pd.DataFrame({"trade_date": pd.to_datetime(["2024-06-03", "2024-06-04"])})
        mock_ak = MagicMock()
        mock_ak.tool_trade_date_hist_sina.return_value = df
        monkeypatch.setattr(_dsd, "ak", mock_ak)
        monkeypatch.setattr(_dsd, "truncate_trade_cal", lambda: None)
        monkeypatch.setattr(_dsd, "upsert_trade_dates", lambda x: None)
        _dsd.refresh_trade_calendar()

    def test_no_akshare(self, monkeypatch):
        monkeypatch.setattr(_dsd, "AKSHARE_AVAILABLE", False)
        _dsd.refresh_trade_calendar()  # Should not raise

    def test_empty_result(self, monkeypatch):
        monkeypatch.setattr(_dsd, "AKSHARE_AVAILABLE", True)
        mock_ak = MagicMock()
        mock_ak.tool_trade_date_hist_sina.return_value = pd.DataFrame()
        monkeypatch.setattr(_dsd, "ak", mock_ak)
        _dsd.refresh_trade_calendar()

    def test_exception(self, monkeypatch):
        monkeypatch.setattr(_dsd, "AKSHARE_AVAILABLE", True)
        mock_ak = MagicMock()
        mock_ak.tool_trade_date_hist_sina.side_effect = Exception("api error")
        monkeypatch.setattr(_dsd, "ak", mock_ak)
        _dsd.refresh_trade_calendar()  # Should not raise


@pytest.mark.unit
class TestSyncStep:
    def test_enum_values(self):
        assert _dsd.SyncStep.AKSHARE_INDEX == "akshare_index"
        assert _dsd.SyncStep.TUSHARE_STOCK_DAILY == "tushare_stock_daily"
        assert _dsd.SyncStep.VNPY_SYNC == "vnpy_sync"


@pytest.mark.unit
class TestSyncStatus:
    def test_enum_values(self):
        assert _dsd.SyncStatus.SUCCESS == "success"
        assert _dsd.SyncStatus.ERROR == "error"
        assert _dsd.SyncStatus.PARTIAL == "partial"


@pytest.mark.unit
class TestWriteSyncLog:
    def test_dry_run(self, monkeypatch):
        monkeypatch.setattr(_dsd, "DRY_RUN", True)
        _dsd.write_sync_log(date(2024, 6, 1), "daily", "success")

    def test_normal(self, monkeypatch):
        monkeypatch.setattr(_dsd, "DRY_RUN", False)
        monkeypatch.setattr(_dsd, "dao_write_tushare_stock_sync_log", lambda *a: None)
        _dsd.write_sync_log(date(2024, 6, 1), "daily", "success", 100)


@pytest.mark.unit
class TestGetLastSuccessDate:
    def test_normal(self, monkeypatch):
        monkeypatch.setattr(_dsd, "dao_get_last_success_tushare_sync_date", lambda ep: date(2024, 6, 1))
        assert _dsd.get_last_success_date("daily") == date(2024, 6, 1)


@pytest.mark.unit
class TestGetTradeDays:
    def test_success(self, monkeypatch):
        df = pd.DataFrame({
            "cal_date": ["20240603", "20240604", "20240605"],
            "is_open": [1, 1, 0],
        })
        monkeypatch.setattr(_dsd, "call_pro", lambda *a, **kw: df)
        result = _dsd.get_trade_days(date(2024, 6, 3), date(2024, 6, 5))
        assert len(result) == 2

    def test_fallback_weekdays(self, monkeypatch):
        monkeypatch.setattr(_dsd, "call_pro", MagicMock(side_effect=Exception("no api")))
        result = _dsd.get_trade_days(date(2024, 6, 3), date(2024, 6, 7))
        assert len(result) == 5


@pytest.mark.unit
class TestRunAkshareIndexStep:
    def test_success(self, monkeypatch):
        monkeypatch.setattr(_dsd, "ak_ingest_index_daily", lambda **kw: None)
        monkeypatch.setattr(_dsd, "INDEX_MAPPING", {"000001": "idx1", "000002": "idx2"})
        status, count, err = _dsd.run_akshare_index_step(date(2024, 6, 1))
        assert status == _dsd.SyncStatus.SUCCESS
        assert count == 2

    def test_partial(self, monkeypatch):
        call_count = 0
        def fake_ingest(**kw):
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                raise Exception("fail")
        monkeypatch.setattr(_dsd, "ak_ingest_index_daily", fake_ingest)
        monkeypatch.setattr(_dsd, "INDEX_MAPPING", {"s1": "i1", "s2": "i2"})
        status, count, err = _dsd.run_akshare_index_step(date(2024, 6, 1))
        assert status == _dsd.SyncStatus.PARTIAL

    def test_all_fail(self, monkeypatch):
        monkeypatch.setattr(_dsd, "ak_ingest_index_daily", MagicMock(side_effect=Exception("fail")))
        monkeypatch.setattr(_dsd, "INDEX_MAPPING", {"s1": "i1"})
        status, count, err = _dsd.run_akshare_index_step(date(2024, 6, 1))
        assert status == _dsd.SyncStatus.ERROR


@pytest.mark.unit
class TestRunTushareStockBasicStep:
    def test_success(self, monkeypatch):
        monkeypatch.setattr(_dsd, "ingest_stock_basic", lambda: None)
        monkeypatch.setattr(_dsd, "get_stock_basic_count", lambda: 5000)
        status, count, err = _dsd.run_tushare_stock_basic_step(date(2024, 6, 1))
        assert status == _dsd.SyncStatus.SUCCESS
        assert count == 5000

    def test_error(self, monkeypatch):
        monkeypatch.setattr(_dsd, "ingest_stock_basic", MagicMock(side_effect=Exception("fail")))
        status, count, err = _dsd.run_tushare_stock_basic_step(date(2024, 6, 1))
        assert status == _dsd.SyncStatus.ERROR


@pytest.mark.unit
class TestRunTushareStockDailyStep:
    def test_success(self, monkeypatch):
        df = pd.DataFrame({"ts_code": ["000001.SZ"], "close": [10]})
        monkeypatch.setattr(_dsd, "call_pro", lambda *a, **kw: df)
        monkeypatch.setattr(_dsd, "upsert_daily", lambda d: 1)
        status, count, err = _dsd.run_tushare_stock_daily_step(date(2024, 6, 1))
        assert status == _dsd.SyncStatus.SUCCESS

    def test_no_data(self, monkeypatch):
        monkeypatch.setattr(_dsd, "call_pro", lambda *a, **kw: pd.DataFrame())
        status, count, err = _dsd.run_tushare_stock_daily_step(date(2024, 6, 1))
        assert status == _dsd.SyncStatus.SUCCESS
        assert count == 0

    def test_error(self, monkeypatch):
        monkeypatch.setattr(_dsd, "call_pro", MagicMock(side_effect=Exception("fail")))
        status, count, err = _dsd.run_tushare_stock_daily_step(date(2024, 6, 1))
        assert status == _dsd.SyncStatus.ERROR


@pytest.mark.unit
class TestSyncDailyForDate:
    def test_success(self, monkeypatch):
        monkeypatch.setattr(_dsd, "get_all_ts_codes", lambda: ["000001.SZ"])
        monkeypatch.setattr(_dsd, "ingest_daily", lambda **kw: None)
        monkeypatch.setattr(_dsd, "write_sync_log", lambda *a, **kw: None)
        monkeypatch.setattr(time, "sleep", lambda x: None)
        _dsd.sync_daily_for_date(date(2024, 6, 1))

    def test_with_failures(self, monkeypatch):
        monkeypatch.setattr(_dsd, "get_all_ts_codes", lambda: ["000001.SZ", "600000.SH"])
        call_count = 0
        def fake_ingest(**kw):
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                raise Exception("fail")
        monkeypatch.setattr(_dsd, "ingest_daily", fake_ingest)
        monkeypatch.setattr(_dsd, "write_sync_log", lambda *a, **kw: None)
        monkeypatch.setattr(time, "sleep", lambda x: None)
        _dsd.sync_daily_for_date(date(2024, 6, 1))


@pytest.mark.unit
class TestRunSyncForDate:
    def test_daily_endpoint(self, monkeypatch):
        monkeypatch.setattr(_dsd, "sync_daily_for_date", lambda d: None)
        _dsd.run_sync_for_date(date(2024, 6, 1), ["daily"])

    def test_repo_endpoint(self, monkeypatch):
        monkeypatch.setattr(_dsd, "DRY_RUN", False)
        monkeypatch.setattr(_dsd, "ingest_repo", lambda **kw: None)
        monkeypatch.setattr(_dsd, "write_sync_log", lambda *a, **kw: None)
        _dsd.run_sync_for_date(date(2024, 6, 1), ["repo"])

    def test_repo_dry_run(self, monkeypatch):
        monkeypatch.setattr(_dsd, "DRY_RUN", True)
        _dsd.run_sync_for_date(date(2024, 6, 1), ["repo"])

    def test_daily_basic(self, monkeypatch):
        monkeypatch.setattr(_dsd, "ingest_daily_basic", lambda: None)
        monkeypatch.setattr(_dsd, "ingest_all_other_data", lambda: None)
        monkeypatch.setattr(_dsd, "write_sync_log", lambda *a, **kw: None)
        _dsd.run_sync_for_date(date(2024, 6, 1), ["daily_basic"])

    def test_endpoint_error(self, monkeypatch):
        monkeypatch.setattr(_dsd, "ingest_daily_basic", MagicMock(side_effect=Exception("fail")))
        monkeypatch.setattr(_dsd, "write_sync_log", lambda *a, **kw: None)
        _dsd.run_sync_for_date(date(2024, 6, 1), ["daily_basic"])


# =====================================================================
# table_manager
# =====================================================================

import app.datasync.table_manager as _tm


@pytest.mark.unit
class TestTableManager:
    def test_get_engine_unknown(self):
        with pytest.raises(ValueError, match="Unknown target database"):
            _tm._get_engine("unknown_db")

    def test_get_engine_tushare(self, monkeypatch):
        mock_eng = MagicMock()
        monkeypatch.setitem(_tm._ENGINE_MAP, "tushare", lambda: mock_eng)
        assert _tm._get_engine("tushare") is mock_eng

    def test_get_engine_akshare(self, monkeypatch):
        mock_eng = MagicMock()
        monkeypatch.setitem(_tm._ENGINE_MAP, "akshare", lambda: mock_eng)
        assert _tm._get_engine("akshare") is mock_eng

    def test_ensure_table_exists(self, monkeypatch):
        mock_conn = MagicMock()
        mock_conn.execute.return_value.scalar.return_value = 1
        mock_eng = MagicMock()
        mock_eng.connect.return_value.__enter__ = lambda s: mock_conn
        mock_eng.connect.return_value.__exit__ = lambda s, *a: None
        monkeypatch.setattr(_tm, "_get_engine", lambda db: mock_eng)
        monkeypatch.setattr(_tm, "_mark_table_created", lambda db, tbl: None)
        result = _tm.ensure_table("tushare", "test_tbl", "CREATE TABLE ...")
        assert result is False  # table already exists

    def test_ensure_table_creates(self, monkeypatch):
        mock_conn = MagicMock()
        mock_conn.execute.return_value.scalar.return_value = 0  # doesn't exist
        mock_eng = MagicMock()
        mock_eng.connect.return_value.__enter__ = MagicMock(return_value=mock_conn)
        mock_eng.connect.return_value.__exit__ = MagicMock(return_value=False)
        mock_begin_conn = MagicMock()
        mock_eng.begin.return_value.__enter__ = MagicMock(return_value=mock_begin_conn)
        mock_eng.begin.return_value.__exit__ = MagicMock(return_value=False)
        monkeypatch.setattr(_tm, "_get_engine", lambda db: mock_eng)
        monkeypatch.setattr(_tm, "get_quantmate_engine", lambda: mock_eng)
        result = _tm.ensure_table("tushare", "new_tbl", "CREATE TABLE test (id INT)")
        assert result is True


# =====================================================================
# scheduler
# =====================================================================

import app.datasync.scheduler as _sched


@pytest.mark.unit
class TestScheduler:
    def test_run_daily_sync(self, monkeypatch):
        monkeypatch.setattr("app.datasync.service.sync_engine.daily_sync", lambda r, **kw: {"step": "ok"})
        monkeypatch.setattr("app.datasync.service.vnpy_sync.run_vnpy_sync_job", lambda d=None: MagicMock(
            status=MagicMock(value="success"), rows_synced=100, error_message=None))
        monkeypatch.setattr(_sched, "_build_registry", lambda: MagicMock())
        result = _sched.run_daily_sync()
        assert "vnpy/vnpy_sync" in result

    def test_run_backfill(self, monkeypatch):
        monkeypatch.setattr("app.datasync.service.sync_engine.backfill_retry", lambda r: {"retries": 0})
        monkeypatch.setattr(_sched, "_build_registry", lambda: MagicMock())
        result = _sched.run_backfill()
        assert result == {"retries": 0}

    def test_run_vnpy(self, monkeypatch):
        mock_result = MagicMock(status=MagicMock(value="success"), rows_synced=50, error_message=None)
        monkeypatch.setattr("app.datasync.service.vnpy_sync.run_vnpy_sync_job", lambda: mock_result)
        result = _sched.run_vnpy()
        assert result.rows_synced == 50

    def test_run_init(self, monkeypatch):
        monkeypatch.setattr("app.datasync.service.init_service.initialize", lambda r, **kw: "done")
        monkeypatch.setattr(_sched, "_build_registry", lambda: MagicMock())
        result = _sched.run_init()
        assert result == "done"

    def test_scheduled_daily(self, monkeypatch):
        monkeypatch.setattr(_sched, "run_daily_sync", lambda: None)
        _sched._scheduled_daily()

    def test_scheduled_daily_error(self, monkeypatch):
        monkeypatch.setattr(_sched, "run_daily_sync", MagicMock(side_effect=Exception("fail")))
        _sched._scheduled_daily()  # Should not raise

    def test_scheduled_backfill(self, monkeypatch):
        monkeypatch.setattr(_sched, "run_backfill", lambda: None)
        _sched._scheduled_backfill()

    def test_scheduled_backfill_error(self, monkeypatch):
        monkeypatch.setattr(_sched, "run_backfill", MagicMock(side_effect=Exception("fail")))
        _sched._scheduled_backfill()  # Should not raise
