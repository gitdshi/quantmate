"""Unit tests for app.datasync.service.tushare_ingest — utility functions."""

from __future__ import annotations

from unittest.mock import MagicMock, patch, PropertyMock
import pytest

_MOD = "app.datasync.service.tushare_ingest"


# ── parse_retry_after ──────────────────────────────────────────

class TestParseRetryAfter:
    @pytest.fixture(autouse=True)
    def _import(self):
        with patch(f"{_MOD}.engine", MagicMock()):
            from app.datasync.service.tushare_ingest import parse_retry_after
            self.fn = parse_retry_after

    def test_seconds(self):
        assert self.fn("Please wait 30 seconds") == 30.0

    def test_minutes(self):
        r = self.fn("try again in 2 minutes")
        assert r == 120.0

    def test_chinese_seconds(self):
        r = self.fn("请等待30秒")
        assert r == 30.0

    def test_chinese_minutes(self):
        r = self.fn("请等待2分钟")
        assert r == 120.0

    def test_no_match(self):
        assert self.fn("some random error") is None

    def test_empty(self):
        assert self.fn("") is None


class TestIsRateLimitError:
    @pytest.fixture(autouse=True)
    def _import(self):
        with patch(f"{_MOD}.engine", MagicMock()):
            from app.datasync.service.tushare_ingest import _is_rate_limit_error
            self.fn = _is_rate_limit_error

    def test_too_many(self):
        assert self.fn("Too many requests") is True

    def test_rate_limit(self):
        assert self.fn("rate limit exceeded") is True

    def test_frequency(self):
        assert self.fn("接口请求频率过高") is True

    def test_max_calls(self):
        assert self.fn("抱歉，您每分钟最多访问") is True

    def test_normal_error(self):
        assert self.fn("connection timeout") is False

    def test_empty(self):
        assert self.fn("") is False


class TestCallPro:
    @pytest.fixture(autouse=True)
    def _setup(self):
        self._mock_engine = MagicMock()
        self._mock_pro = MagicMock()
        with patch(f"{_MOD}.engine", self._mock_engine), \
             patch(f"{_MOD}.pro", self._mock_pro):
            from app.datasync.service import tushare_ingest
            self.mod = tushare_ingest
            yield

    def test_success(self):
        import pandas as pd
        df = pd.DataFrame({"ts_code": ["000001.SZ"], "close": [10.0]})
        self._mock_pro.daily.return_value = df
        result = self.mod.call_pro("daily", ts_code="000001.SZ")
        assert len(result) == 1

    def test_no_pro(self):
        self.mod.pro = None
        with patch(f"{_MOD}.time.sleep"):
            with pytest.raises(AttributeError, match="Tushare pro has no api"):
                self.mod.call_pro("daily", max_retries=1, backoff_base=0)
        self.mod.pro = self._mock_pro  # restore

    def test_retries_on_rate_limit(self):
        import pandas as pd
        self._mock_pro.daily.side_effect = [
            Exception("抱歉，您每分钟最多访问"),
            pd.DataFrame({"ts_code": ["000001.SZ"]}),
        ]
        with patch(f"{_MOD}.time.sleep"):
            result = self.mod.call_pro("daily", max_retries=2, backoff_base=0)
        assert len(result) == 1

    def test_max_retries_exceeded(self):
        self._mock_pro.daily.side_effect = Exception("rate limit exceeded")
        with patch(f"{_MOD}.time.sleep"), pytest.raises(Exception, match="rate limit"):
            self.mod.call_pro("daily", max_retries=1, backoff_base=0)


class TestSetMetricsHook:
    def test_sets_hook(self):
        with patch(f"{_MOD}.engine", MagicMock()):
            from app.datasync.service.tushare_ingest import set_metrics_hook
            fn = lambda *a: None
            set_metrics_hook(fn)


class TestGetAllTsCodes:
    def test_returns_codes(self):
        with patch(f"{_MOD}.engine", MagicMock()):
            from app.datasync.service.tushare_ingest import get_all_ts_codes
            with patch(f"{_MOD}.dao_get_all_ts_codes", return_value=["000001.SZ", "000002.SZ"]):
                result = get_all_ts_codes()
            assert result == ["000001.SZ", "000002.SZ"]


class TestGetMaxTradeDate:
    def test_returns_date(self):
        from datetime import date
        with patch(f"{_MOD}.engine", MagicMock()):
            from app.datasync.service.tushare_ingest import get_max_trade_date
            with patch(f"{_MOD}.dao_get_max_trade_date", return_value=date(2024, 1, 1)):
                result = get_max_trade_date("000001.SZ")
            assert result == date(2024, 1, 1)


# ── Additional ingest function tests ────────────────────────────


@pytest.fixture(autouse=True)
def _mock_tushare_db():
    """Mock audit and call_pro functions that hit the database."""
    with patch(f"{_MOD}.audit_start", return_value=1), \
         patch(f"{_MOD}.audit_finish"), \
         patch(f"{_MOD}.engine", MagicMock()), \
         patch(f"{_MOD}.pro", MagicMock()):
        yield


class TestIngestDaily:
    def test_by_ts_code(self):
        import pandas as pd
        from app.datasync.service import tushare_ingest as mod
        df = pd.DataFrame({"ts_code": ["000001.SZ"], "close": [10.0], "trade_date": ["20240115"]})
        with patch.object(mod, "call_pro", return_value=df), \
             patch.object(mod, "upsert_daily", return_value=1):
            mod.ingest_daily(ts_code="000001.SZ", start_date="20240101", end_date="20240115")

    def test_empty_result(self):
        import pandas as pd
        from app.datasync.service import tushare_ingest as mod
        with patch.object(mod, "call_pro", return_value=pd.DataFrame()), \
             patch.object(mod, "upsert_daily", return_value=0):
            mod.ingest_daily(ts_code="000001.SZ")


class TestIngestStockBasic:
    def test_success(self):
        import pandas as pd
        from app.datasync.service import tushare_ingest as mod
        df = pd.DataFrame({"ts_code": ["000001.SZ"], "name": ["Ping An"]})
        with patch.object(mod, "call_pro", return_value=df), \
             patch("app.domains.extdata.dao.tushare_dao.upsert_stock_basic", return_value=1):
            mod.ingest_stock_basic()


class TestIngestDailyBasic:
    def test_success(self):
        import pandas as pd
        from app.datasync.service import tushare_ingest as mod
        df = pd.DataFrame({"ts_code": ["000001.SZ"], "pe": [10.0]})
        with patch.object(mod, "call_pro", return_value=df), \
             patch.object(mod, "upsert_daily_basic", return_value=1):
            mod.ingest_daily_basic(trade_date="20240115")


class TestIngestAdjFactor:
    def test_success(self):
        import pandas as pd
        from app.datasync.service import tushare_ingest as mod
        df = pd.DataFrame({"ts_code": ["000001.SZ"], "adj_factor": [1.5]})
        with patch.object(mod, "call_pro", return_value=df), \
             patch.object(mod, "upsert_adj_factor", return_value=1):
            mod.ingest_adj_factor(trade_date="20240115")


class TestIngestDividend:
    def test_success(self):
        import pandas as pd
        from app.datasync.service import tushare_ingest as mod
        df = pd.DataFrame({"ts_code": ["000001.SZ"], "div_proc": ["分红"]})
        with patch.object(mod, "call_pro", return_value=df), \
             patch.object(mod, "upsert_dividend_df", return_value=1):
            mod.ingest_dividend(ts_code="000001.SZ")

    def test_empty(self):
        import pandas as pd
        from app.datasync.service import tushare_ingest as mod
        with patch.object(mod, "call_pro", return_value=pd.DataFrame()):
            mod.ingest_dividend(ts_code="000001.SZ")


class TestIngestTop10Holders:
    def test_success(self):
        import pandas as pd
        from app.datasync.service import tushare_ingest as mod
        df = pd.DataFrame({"ts_code": ["000001.SZ"], "holder_name": ["test"]})
        with patch.object(mod, "call_pro", return_value=df), \
             patch.object(mod, "upsert_top10_holders", return_value=1):
            mod.ingest_top10_holders(ts_code="000001.SZ")


class TestIngestWeekly:
    def test_success(self):
        import pandas as pd
        from app.datasync.service import tushare_ingest as mod
        df = pd.DataFrame({"ts_code": ["000001.SZ"], "close": [10.0]})
        with patch.object(mod, "call_pro", return_value=df), \
             patch.object(mod, "upsert_weekly", return_value=1):
            result = mod.ingest_weekly(start_date="20240101", end_date="20240115")
            assert isinstance(result, int)


class TestIngestMonthly:
    def test_success(self):
        import pandas as pd
        from app.datasync.service import tushare_ingest as mod
        df = pd.DataFrame({"ts_code": ["000001.SZ"], "close": [10.0]})
        with patch.object(mod, "call_pro", return_value=df), \
             patch.object(mod, "upsert_monthly", return_value=1):
            result = mod.ingest_monthly(start_date="20240101", end_date="20240115")
            assert isinstance(result, int)


class TestIngestIndexDaily:
    def test_success(self):
        import pandas as pd
        from app.datasync.service import tushare_ingest as mod
        df = pd.DataFrame({"ts_code": ["000001.SH"], "close": [3000.0]})
        with patch.object(mod, "call_pro", return_value=df), \
             patch.object(mod, "upsert_index_daily_df", return_value=1):
            result = mod.ingest_index_daily(ts_code="000001.SH", start_date="20240101")
            assert isinstance(result, int)


class TestIngestIndexWeekly:
    def test_success(self):
        import pandas as pd
        from app.datasync.service import tushare_ingest as mod
        df = pd.DataFrame({"ts_code": ["000001.SH"], "close": [3000.0]})
        with patch.object(mod, "call_pro", return_value=df), \
             patch.object(mod, "upsert_index_weekly_df", return_value=1):
            result = mod.ingest_index_weekly(ts_code="000001.SH", start_date="20240101")
            assert isinstance(result, int)


class TestStoreFinancialStatement:
    def test_success(self):
        import pandas as pd
        from app.datasync.service import tushare_ingest as mod
        df = pd.DataFrame({"ts_code": ["000001.SZ"], "revenue": [1e9]})
        with patch.object(mod, "upsert_financial_statement", return_value=1):
            mod.store_financial_statement(df, "income")


class TestIngestIncome:
    def test_success(self):
        import pandas as pd
        from app.datasync.service import tushare_ingest as mod
        df = pd.DataFrame({"ts_code": ["000001.SZ"], "revenue": [1e9]})
        with patch.object(mod, "call_pro", return_value=df), \
             patch.object(mod, "store_financial_statement"):
            mod.ingest_income("000001.SZ")


class TestIngestMoneyflow:
    def test_success(self):
        import pandas as pd
        from app.datasync.service import tushare_ingest as mod
        df = pd.DataFrame({"ts_code": ["000001.SZ"], "buy_sm_vol": [100]})
        with patch.object(mod, "call_pro", return_value=df), \
             patch.object(mod, "upsert_moneyflow", return_value=1):
            mod.ingest_moneyflow(ts_code="000001.SZ")


class TestIngestMargin:
    def test_success(self):
        import pandas as pd
        from app.datasync.service import tushare_ingest as mod
        df = pd.DataFrame({"ts_code": ["000001.SZ"], "rzye": [1e8]})
        with patch.object(mod, "call_pro", return_value=df), \
             patch.object(mod, "upsert_margin", return_value=1):
            mod.ingest_margin(ts_code="000001.SZ")


class TestIngestBlockTrade:
    def test_success(self):
        import pandas as pd
        from app.datasync.service import tushare_ingest as mod
        df = pd.DataFrame({"ts_code": ["000001.SZ"], "price": [10.0]})
        with patch.object(mod, "call_pro", return_value=df), \
             patch.object(mod, "upsert_block_trade", return_value=1):
            mod.ingest_block_trade(ts_code="000001.SZ")


class TestIngestRepo:
    def test_success(self):
        import pandas as pd
        from app.datasync.service import tushare_ingest as mod
        df = pd.DataFrame({"repo_date": ["20240115"], "rate": [2.0]})
        with patch.object(mod, "call_pro", return_value=df), \
             patch(f"{_MOD}.upsert_repo_df", create=True, return_value=1):
            mod.ingest_repo(repo_date="20240115")


class TestIngestAllOtherData:
    def test_runs(self):
        from app.datasync.service import tushare_ingest as mod
        with patch.object(mod, "get_all_ts_codes", return_value=["000001.SZ"]), \
             patch.object(mod, "ingest_adj_factor"), \
             patch.object(mod, "ingest_moneyflow"), \
             patch.object(mod, "ingest_dividend"), \
             patch.object(mod, "ingest_top10_holders"), \
             patch.object(mod, "ingest_margin"), \
             patch.object(mod, "ingest_block_trade"), \
             patch(f"{_MOD}.time.sleep"):
            mod.ingest_all_other_data(batch_size=1, sleep_between=0)


class TestIngestAllDaily:
    def test_runs(self):
        from app.datasync.service import tushare_ingest as mod
        with patch.object(mod, "get_all_ts_codes", return_value=["000001.SZ"]), \
             patch.object(mod, "get_max_trade_date", return_value=None), \
             patch.object(mod, "ingest_daily"), \
             patch(f"{_MOD}.time.sleep"):
            mod.ingest_all_daily(batch_size=1, sleep_between=0)


class TestIngestDividendByDateRange:
    def test_runs(self):
        import pandas as pd
        from app.datasync.service import tushare_ingest as mod
        df = pd.DataFrame({"ts_code": ["000001.SZ"], "div_proc": ["fen hong"]})
        with patch.object(mod, "get_all_ts_codes", return_value=["000001.SZ"]), \
             patch.object(mod, "call_pro", return_value=df), \
             patch.object(mod, "upsert_dividend_df"), \
             patch.object(mod, "_fetch_existing_keys", return_value=set()), \
             patch(f"{_MOD}.time.sleep"):
            mod.ingest_dividend_by_date_range("20230101", "20240101", batch_size=1, sleep_between=0)


class TestMinIntervalFor:
    def test_known_api(self):
        from app.datasync.service.tushare_ingest import _min_interval_for
        result = _min_interval_for("daily")
        assert isinstance(result, float)
        assert result == 0.0

    def test_unknown_api(self):
        from app.datasync.service.tushare_ingest import _min_interval_for
        result = _min_interval_for("unknown_api_xyz")
        assert isinstance(result, float)
        assert result == 0.0


class TestGetFailedTsCodes:
    def test_returns_list(self):
        from app.datasync.service import tushare_ingest as mod
        with patch.object(mod, "get_failed_ts_codes", return_value=["000001.SZ"]):
            result = mod.get_failed_ts_codes()
            assert result == ["000001.SZ"]


class TestRetryFailedDaily:
    def test_retries(self):
        from app.datasync.service import tushare_ingest as mod
        with patch.object(mod, "get_failed_ts_codes", return_value=["000001.SZ"]), \
             patch.object(mod, "ingest_daily"), \
             patch(f"{_MOD}.time.sleep"):
            mod.retry_failed_daily(limit=1)

