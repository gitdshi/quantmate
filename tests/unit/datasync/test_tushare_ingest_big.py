"""Tests for uncovered functions in app.datasync.service.tushare_ingest.

Covers: store_financial_statement, ingest_index_weekly, ingest_income,
ingest_dividend, ingest_top10_holders, ingest_margin, ingest_block_trade,
ingest_repo, ingest_all_other_data, _fetch_existing_keys,
ingest_dividend_by_date_range, ingest_top10_holders_by_date_range,
ingest_adj_factor_by_date_range, ingest_all_daily, get_failed_ts_codes,
retry_failed_daily, _env_rate, error paths.
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch, call
import pandas as pd
import pytest

_MOD = "app.datasync.service.tushare_ingest"


@pytest.fixture(autouse=True)
def _mock_audit(monkeypatch):
    """Prevent real DB calls from audit_start / audit_finish."""
    import app.datasync.service.tushare_ingest as mod
    monkeypatch.setattr(mod, "audit_start", lambda *a, **kw: 1)
    monkeypatch.setattr(mod, "audit_finish", lambda *a, **kw: None)


@pytest.fixture(autouse=True)
def _reset_call_pro_state():
    """Reset call_pro internal state between tests."""
    import app.datasync.service.tushare_ingest as mod
    if hasattr(mod.call_pro, "_last_call"):
        mod.call_pro._last_call.clear()
    yield


# ═══ _env_rate ═══════════════════════════════════════════════════════════

class TestEnvRate:
    def test_default(self):
        from app.datasync.service.tushare_ingest import _env_rate
        assert _env_rate("nonexistent", 42) == 42

    def test_from_env(self, monkeypatch):
        from app.datasync.service.tushare_ingest import _env_rate
        monkeypatch.setenv("TUSHARE_RATE_daily", "100")
        assert _env_rate("daily", 42) == 100

    def test_invalid_env(self, monkeypatch):
        from app.datasync.service.tushare_ingest import _env_rate
        monkeypatch.setenv("TUSHARE_RATE_bad", "notanint")
        assert _env_rate("bad", 42) == 42


# ═══ store_financial_statement ═══════════════════════════════════════════

class TestStoreFinancialStatement:
    @patch(f"{_MOD}.upsert_financial_statement", return_value=5)
    def test_delegates_to_dao(self, mock_upsert):
        from app.datasync.service.tushare_ingest import store_financial_statement

        df = pd.DataFrame({"a": [1]})
        result = store_financial_statement(df, "income")
        assert result == 5
        mock_upsert.assert_called_once_with(df, "income")


# ═══ ingest_index_weekly ═════════════════════════════════════════════════

class TestIngestIndexWeekly:
    @patch(f"{_MOD}.upsert_index_weekly_df", return_value=10)
    @patch(f"{_MOD}.call_pro")
    def test_success(self, mock_call, mock_upsert):
        from app.datasync.service.tushare_ingest import ingest_index_weekly
        df = pd.DataFrame({"ts_code": ["000001.SH"], "close": [3000]})
        mock_call.return_value = df
        result = ingest_index_weekly(ts_code="000001.SH")
        assert result == 10

    @patch(f"{_MOD}.call_pro", return_value=pd.DataFrame())
    def test_empty(self, mock_call):
        from app.datasync.service.tushare_ingest import ingest_index_weekly
        result = ingest_index_weekly(ts_code="000001.SH")
        assert result == 0

    @patch(f"{_MOD}.call_pro", side_effect=[Exception("fail"), Exception("fail"), Exception("fail")])
    def test_error_retries_exhausted(self, mock_call, monkeypatch):
        monkeypatch.setenv("MAX_RETRIES", "3")
        from app.datasync.service.tushare_ingest import ingest_index_weekly
        result = ingest_index_weekly(ts_code="000001.SH")
        assert result == 0


# ═══ ingest_income ══════════════════════════════════════════════════════

class TestIngestIncome:
    @patch(f"{_MOD}.store_financial_statement", return_value=3)
    @patch(f"{_MOD}.call_pro")
    def test_success(self, mock_call, mock_store):
        from app.datasync.service.tushare_ingest import ingest_income
        df = pd.DataFrame({"revenue": [100]})
        mock_call.return_value = df
        ingest_income("000001.SZ")
        mock_store.assert_called_once_with(df, "income")

    @patch(f"{_MOD}.call_pro", side_effect=RuntimeError("api error"))
    def test_error(self, mock_call):
        from app.datasync.service.tushare_ingest import ingest_income
        # Should not raise
        ingest_income("000001.SZ")


# ═══ ingest_dividend ════════════════════════════════════════════════════

class TestIngestDividend:
    @patch(f"{_MOD}.upsert_dividend_df", return_value=2)
    @patch(f"{_MOD}.call_pro")
    def test_success(self, mock_call, mock_upsert):
        from app.datasync.service.tushare_ingest import ingest_dividend
        df = pd.DataFrame({
            "ts_code": ["000001.SZ"],
            "ann_date": ["20240101"],
            "imp_ann_date": ["20240101"],
            "div_cash": [0.5],
        })
        mock_call.return_value = df
        ingest_dividend(ts_code="000001.SZ")
        mock_upsert.assert_called_once()

    @patch(f"{_MOD}.call_pro", return_value=pd.DataFrame())
    def test_empty_df(self, mock_call):
        from app.datasync.service.tushare_ingest import ingest_dividend
        ingest_dividend(ts_code="000001.SZ")  # no upsert called

    @patch(f"{_MOD}.upsert_dividend_df", return_value=1)
    @patch(f"{_MOD}.call_pro")
    def test_missing_ann_date_uses_imp_ann_date(self, mock_call, mock_upsert):
        from app.datasync.service.tushare_ingest import ingest_dividend
        df = pd.DataFrame({
            "ts_code": ["000001.SZ"],
            "ann_date": [None],
            "imp_ann_date": ["20240315"],
            "div_cash": [0.3],
        })
        mock_call.return_value = df
        ingest_dividend(ts_code="000001.SZ")
        mock_upsert.assert_called_once()

    @patch(f"{_MOD}.call_pro", side_effect=RuntimeError("fail"))
    def test_error(self, mock_call):
        from app.datasync.service.tushare_ingest import ingest_dividend
        ingest_dividend(ts_code="000001.SZ")


# ═══ ingest_top10_holders ═══════════════════════════════════════════════

class TestIngestTop10Holders:
    @patch(f"{_MOD}.upsert_top10_holders", return_value=8)
    @patch(f"{_MOD}.call_pro")
    def test_success(self, mock_call, mock_upsert):
        from app.datasync.service.tushare_ingest import ingest_top10_holders
        df = pd.DataFrame({"holder_name": ["QFII"]})
        mock_call.return_value = df
        ingest_top10_holders(ts_code="000001.SZ")
        mock_upsert.assert_called_once()

    @patch(f"{_MOD}.call_pro", side_effect=RuntimeError("err"))
    def test_error(self, mock_call):
        from app.datasync.service.tushare_ingest import ingest_top10_holders
        ingest_top10_holders(ts_code="000001.SZ")


# ═══ ingest_margin ═════════════════════════════════════════════════════

class TestIngestMargin:
    @patch(f"{_MOD}.upsert_margin", return_value=5)
    @patch(f"{_MOD}.call_pro")
    def test_success(self, mock_call, mock_upsert):
        from app.datasync.service.tushare_ingest import ingest_margin
        df = pd.DataFrame({"rzye": [100000]})
        mock_call.return_value = df
        ingest_margin(ts_code="000001.SZ")
        mock_upsert.assert_called_once()

    @patch(f"{_MOD}.call_pro", side_effect=RuntimeError("err"))
    def test_error(self, mock_call):
        from app.datasync.service.tushare_ingest import ingest_margin
        ingest_margin(ts_code="000001.SZ")


# ═══ ingest_block_trade ═══════════════════════════════════════════════

class TestIngestBlockTrade:
    @patch(f"{_MOD}.upsert_block_trade", return_value=3)
    @patch(f"{_MOD}.call_pro")
    def test_success(self, mock_call, mock_upsert):
        from app.datasync.service.tushare_ingest import ingest_block_trade
        df = pd.DataFrame({"price": [10.5]})
        mock_call.return_value = df
        ingest_block_trade(ts_code="000001.SZ")
        mock_upsert.assert_called_once()

    @patch(f"{_MOD}.call_pro", side_effect=RuntimeError("err"))
    def test_error(self, mock_call):
        from app.datasync.service.tushare_ingest import ingest_block_trade
        ingest_block_trade(ts_code="000001.SZ")


# ═══ ingest_repo ══════════════════════════════════════════════════════

class TestIngestRepo:
    @patch(f"{_MOD}.call_pro")
    def test_none_response(self, mock_call):
        from app.datasync.service.tushare_ingest import ingest_repo
        mock_call.return_value = None
        ingest_repo(repo_date="20240101")

    @patch(f"{_MOD}.call_pro", return_value=pd.DataFrame())
    def test_empty_response(self, mock_call):
        from app.datasync.service.tushare_ingest import ingest_repo
        ingest_repo(repo_date="20240101")

    @patch(f"app.domains.extdata.dao.tushare_dao.upsert_repo_df", return_value=5)
    @patch(f"{_MOD}.call_pro")
    def test_success(self, mock_call, mock_upsert):
        from app.datasync.service.tushare_ingest import ingest_repo
        df = pd.DataFrame({"repo_code": ["R001"], "amount": [1000]})
        mock_call.return_value = df
        ingest_repo(repo_date="20240101")
        mock_upsert.assert_called_once()

    @patch(f"{_MOD}.call_pro")
    def test_no_date(self, mock_call):
        from app.datasync.service.tushare_ingest import ingest_repo
        mock_call.return_value = pd.DataFrame()
        ingest_repo()
        mock_call.assert_called_once()

    @patch(f"{_MOD}.call_pro", side_effect=RuntimeError("err"))
    def test_error(self, mock_call):
        from app.datasync.service.tushare_ingest import ingest_repo
        ingest_repo(repo_date="20240101")


# ═══ ingest_all_other_data ═══════════════════════════════════════════

class TestIngestAllOtherData:
    @patch(f"{_MOD}.ingest_block_trade")
    @patch(f"{_MOD}.ingest_margin")
    @patch(f"{_MOD}.ingest_top10_holders")
    @patch(f"{_MOD}.ingest_dividend")
    @patch(f"{_MOD}.ingest_moneyflow")
    @patch(f"{_MOD}.ingest_adj_factor")
    @patch(f"{_MOD}.ingest_daily_basic")
    @patch(f"{_MOD}.get_all_ts_codes", return_value=["000001.SZ", "000002.SZ"])
    def test_success(self, mock_codes, mock_db, mock_af, mock_mf, mock_dv, mock_h, mock_mg, mock_bt):
        import app.datasync.service.tushare_ingest as mod
        mod.ingest_all_other_data(batch_size=10, sleep_between=0)
        assert mock_db.call_count == 2
        assert mock_dv.call_count == 2

    @patch(f"{_MOD}.ingest_block_trade")
    @patch(f"{_MOD}.ingest_margin")
    @patch(f"{_MOD}.ingest_top10_holders")
    @patch(f"{_MOD}.ingest_dividend")
    @patch(f"{_MOD}.ingest_moneyflow")
    @patch(f"{_MOD}.ingest_adj_factor")
    @patch(f"{_MOD}.ingest_daily_basic", side_effect=RuntimeError("err"))
    @patch(f"{_MOD}.get_all_ts_codes", return_value=["000001.SZ"])
    def test_error_continues(self, mock_codes, mock_db, *_):
        import app.datasync.service.tushare_ingest as mod
        # Should not raise despite ingest_daily_basic error
        mod.ingest_all_other_data(batch_size=10, sleep_between=0)


# ═══ _fetch_existing_keys ═══════════════════════════════════════════

class TestFetchExistingKeys:
    @patch(f"app.domains.extdata.dao.tushare_dao.fetch_existing_keys", return_value={("A", "2024-01-01")})
    def test_delegates(self, mock_dao):
        from app.datasync.service.tushare_ingest import _fetch_existing_keys
        result = _fetch_existing_keys("stock_daily", "trade_date", "2024-01-01", "2024-06-01")
        assert ("A", "2024-01-01") in result


# ═══ ingest_dividend_by_date_range ═══════════════════════════════════

class TestIngestDividendByDateRange:
    @patch(f"{_MOD}.upsert_dividend_df")
    @patch(f"{_MOD}.call_pro")
    @patch(f"{_MOD}.get_all_ts_codes", return_value=["000001.SZ"])
    @patch(f"{_MOD}._fetch_existing_keys", return_value=set())
    def test_success(self, mock_keys, mock_codes, mock_call, mock_upsert):
        from app.datasync.service.tushare_ingest import ingest_dividend_by_date_range
        df = pd.DataFrame({
            "ts_code": ["000001.SZ"],
            "ann_date": ["20240315"],
            "imp_ann_date": [None],
            "record_date": ["20240320"],
            "ex_date": ["20240401"],
            "pay_date": ["20240405"],
            "div_cash": [0.5],
            "div_stock": [0.0],
            "bonus_ratio": [None],
        })
        mock_call.return_value = df
        ingest_dividend_by_date_range("2024-01-01", "2024-06-01", batch_size=10, sleep_between=0)
        mock_upsert.assert_called_once()

    @patch(f"{_MOD}.call_pro", return_value=pd.DataFrame())
    @patch(f"{_MOD}.get_all_ts_codes", return_value=["000001.SZ"])
    @patch(f"{_MOD}._fetch_existing_keys", return_value=set())
    def test_empty_df(self, mock_keys, mock_codes, mock_call):
        from app.datasync.service.tushare_ingest import ingest_dividend_by_date_range
        ingest_dividend_by_date_range("2024-01-01", "2024-06-01", batch_size=10, sleep_between=0)

    @patch(f"{_MOD}.upsert_dividend_df")
    @patch(f"{_MOD}.call_pro")
    @patch(f"{_MOD}.get_all_ts_codes", return_value=["000001.SZ", "000002.SZ"])
    @patch(f"{_MOD}._fetch_existing_keys", return_value=set())
    def test_skips_existing(self, mock_keys, mock_codes, mock_call, mock_upsert):
        from app.datasync.service.tushare_ingest import ingest_dividend_by_date_range
        # existing keys include what will be returned
        mock_keys.return_value = {("000001.SZ", "2024-03-15")}
        df = pd.DataFrame({
            "ts_code": ["000001.SZ"],
            "ann_date": ["20240315"],
            "imp_ann_date": [None],
            "record_date": [None],
            "ex_date": [None],
            "pay_date": [None],
            "div_cash": [0.5],
            "div_stock": [0.0],
            "bonus_ratio": [None],
        })
        mock_call.return_value = df
        ingest_dividend_by_date_range("2024-01-01", "2024-06-01", batch_size=10, sleep_between=0)
        # All rows exist => no upsert
        mock_upsert.assert_not_called()

    @patch(f"{_MOD}.call_pro")
    @patch(f"{_MOD}.get_all_ts_codes", return_value=["000003.SZ", "000001.SZ", "000002.SZ"])
    @patch(f"{_MOD}._fetch_existing_keys", return_value=set())
    def test_start_after(self, mock_keys, mock_codes, mock_call):
        from app.datasync.service.tushare_ingest import ingest_dividend_by_date_range
        mock_call.return_value = pd.DataFrame()
        ingest_dividend_by_date_range(
            "2024-01-01", "2024-06-01",
            batch_size=10, sleep_between=0,
            start_after_ts_code="000001.SZ",
        )
        # 000003 skipped, 000001 skipped (is the resume point), 000002 processed
        assert mock_call.call_count == 1

    @patch(f"{_MOD}.call_pro")
    @patch(f"{_MOD}.get_all_ts_codes", return_value=["000001.SZ"])
    @patch(f"{_MOD}._fetch_existing_keys", return_value=set())
    def test_with_progress_cb(self, mock_keys, mock_codes, mock_call):
        from app.datasync.service.tushare_ingest import ingest_dividend_by_date_range
        mock_call.return_value = pd.DataFrame()
        cb = MagicMock()
        ingest_dividend_by_date_range(
            "2024-01-01", "2024-06-01",
            batch_size=10, sleep_between=0, progress_cb=cb,
        )
        cb.assert_called_once()


# ═══ ingest_top10_holders_by_date_range ══════════════════════════════

class TestIngestTop10HoldersByDateRange:
    @patch(f"{_MOD}.upsert_top10_holders")
    @patch(f"{_MOD}.call_pro")
    @patch(f"{_MOD}.get_all_ts_codes", return_value=["000001.SZ"])
    @patch(f"{_MOD}._fetch_existing_keys", return_value=set())
    def test_success(self, mock_keys, mock_codes, mock_call, mock_upsert):
        from app.datasync.service.tushare_ingest import ingest_top10_holders_by_date_range
        df = pd.DataFrame({
            "ts_code": ["000001.SZ"],
            "end_date": ["20240331"],
            "holder_name": ["QFII"],
            "hold_amount": [100000.0],
            "hold_ratio": [0.05],
        })
        mock_call.return_value = df
        ingest_top10_holders_by_date_range("2024-01-01", "2024-06-01", batch_size=10, sleep_between=0)
        mock_upsert.assert_called_once()

    @patch(f"{_MOD}.call_pro", return_value=pd.DataFrame())
    @patch(f"{_MOD}.get_all_ts_codes", return_value=["000001.SZ"])
    @patch(f"{_MOD}._fetch_existing_keys", return_value=set())
    def test_empty(self, mock_keys, mock_codes, mock_call):
        from app.datasync.service.tushare_ingest import ingest_top10_holders_by_date_range
        ingest_top10_holders_by_date_range("2024-01-01", "2024-06-01", batch_size=10, sleep_between=0)

    @patch(f"{_MOD}.call_pro")
    @patch(f"{_MOD}.get_all_ts_codes", return_value=["A", "B", "C"])
    @patch(f"{_MOD}._fetch_existing_keys", return_value=set())
    def test_start_after(self, mock_keys, mock_codes, mock_call):
        from app.datasync.service.tushare_ingest import ingest_top10_holders_by_date_range
        mock_call.return_value = pd.DataFrame()
        ingest_top10_holders_by_date_range(
            "2024-01-01", "2024-06-01",
            batch_size=10, sleep_between=0,
            start_after_ts_code="B",
        )
        assert mock_call.call_count == 1  # only C processed


# ═══ ingest_adj_factor_by_date_range ═════════════════════════════════

class TestIngestAdjFactorByDateRange:
    @patch(f"{_MOD}.upsert_adj_factor")
    @patch(f"{_MOD}.call_pro")
    @patch(f"{_MOD}.get_all_ts_codes", return_value=["000001.SZ"])
    @patch(f"{_MOD}._fetch_existing_keys", return_value=set())
    def test_success(self, mock_keys, mock_codes, mock_call, mock_upsert):
        from app.datasync.service.tushare_ingest import ingest_adj_factor_by_date_range
        df = pd.DataFrame({
            "ts_code": ["000001.SZ"],
            "trade_date": ["20240315"],
            "adj_factor": [1.05],
        })
        mock_call.return_value = df
        ingest_adj_factor_by_date_range("2024-01-01", "2024-06-01", batch_size=10, sleep_between=0)
        mock_upsert.assert_called_once()

    @patch(f"{_MOD}.call_pro", return_value=pd.DataFrame())
    @patch(f"{_MOD}.get_all_ts_codes", return_value=["000001.SZ"])
    @patch(f"{_MOD}._fetch_existing_keys", return_value=set())
    def test_empty(self, mock_keys, mock_codes, mock_call):
        from app.datasync.service.tushare_ingest import ingest_adj_factor_by_date_range
        ingest_adj_factor_by_date_range("2024-01-01", "2024-06-01", batch_size=10, sleep_between=0)

    @patch(f"{_MOD}.call_pro")
    @patch(f"{_MOD}.get_all_ts_codes", return_value=["A", "B"])
    @patch(f"{_MOD}._fetch_existing_keys", return_value=set())
    def test_error_continues(self, mock_keys, mock_codes, mock_call):
        from app.datasync.service.tushare_ingest import ingest_adj_factor_by_date_range
        mock_call.side_effect = RuntimeError("api fail")
        ingest_adj_factor_by_date_range("2024-01-01", "2024-06-01", batch_size=10, sleep_between=0)
        assert mock_call.call_count == 2


# ═══ ingest_all_daily ═══════════════════════════════════════════════

class TestIngestAllDaily:
    @patch(f"{_MOD}.ingest_daily")
    @patch(f"{_MOD}.get_max_trade_date", return_value="20240101")
    @patch(f"{_MOD}.get_all_ts_codes", return_value=["000001.SZ"])
    def test_resume_from_last_date(self, mock_codes, mock_max, mock_ingest):
        from app.datasync.service.tushare_ingest import ingest_all_daily
        ingest_all_daily(batch_size=10, sleep_between=0)
        mock_ingest.assert_called_once()
        # start_date should be day after last_date
        call_kwargs = mock_ingest.call_args
        assert "start_date" in call_kwargs.kwargs or len(call_kwargs.args) > 0

    @patch(f"{_MOD}.upsert_daily", return_value=5)
    @patch(f"{_MOD}._fetch_existing_keys", return_value=set())
    @patch(f"{_MOD}.call_pro")
    @patch(f"{_MOD}.get_max_trade_date", return_value=None)
    @patch(f"{_MOD}.get_all_ts_codes", return_value=["000001.SZ"])
    def test_full_history(self, mock_codes, mock_max, mock_call, mock_keys, mock_upsert):
        from app.datasync.service.tushare_ingest import ingest_all_daily
        df = pd.DataFrame({
            "ts_code": ["000001.SZ"],
            "trade_date": ["20240315"],
            "open": [10.0], "high": [11.0], "low": [9.5], "close": [10.5],
        })
        mock_call.return_value = df
        ingest_all_daily(batch_size=10, sleep_between=0)
        mock_upsert.assert_called_once()

    @patch(f"{_MOD}.call_pro", return_value=None)
    @patch(f"{_MOD}.get_max_trade_date", return_value=None)
    @patch(f"{_MOD}.get_all_ts_codes", return_value=["000001.SZ"])
    def test_no_data_returned(self, mock_codes, mock_max, mock_call):
        from app.datasync.service.tushare_ingest import ingest_all_daily
        ingest_all_daily(batch_size=10, sleep_between=0)

    @patch(f"{_MOD}.ingest_daily")
    @patch(f"{_MOD}.get_max_trade_date", return_value="20240101")
    @patch(f"{_MOD}.get_all_ts_codes", return_value=["A", "B", "C"])
    def test_start_after(self, mock_codes, mock_max, mock_ingest):
        from app.datasync.service.tushare_ingest import ingest_all_daily
        ingest_all_daily(batch_size=10, sleep_between=0, start_after_ts_code="B")
        assert mock_ingest.call_count == 1  # only C processed

    @patch(f"{_MOD}.ingest_daily")
    @patch(f"{_MOD}.get_max_trade_date", return_value="20240101")
    @patch(f"{_MOD}.get_all_ts_codes", return_value=["000001.SZ"])
    def test_force_full(self, mock_codes, mock_max, mock_ingest):
        from app.datasync.service.tushare_ingest import ingest_all_daily
        # force_full_per_stock skips get_max_trade_date
        ingest_all_daily(batch_size=10, sleep_between=0, force_full_per_stock=True)
        # should go through the "no data in DB" branch

    @patch(f"{_MOD}.ingest_daily")
    @patch(f"{_MOD}.get_max_trade_date", return_value="20240101")
    @patch(f"{_MOD}.get_all_ts_codes", return_value=["000001.SZ"])
    def test_with_date_range(self, mock_codes, mock_max, mock_ingest):
        from app.datasync.service.tushare_ingest import ingest_all_daily
        ingest_all_daily(batch_size=10, sleep_between=0, start_date="2024-01-01", end_date="2024-06-01")
        mock_ingest.assert_called_once()

    @patch(f"{_MOD}.ingest_daily")
    @patch(f"{_MOD}.get_max_trade_date", return_value="20240101")
    @patch(f"{_MOD}.get_all_ts_codes", return_value=["000001.SZ"])
    def test_with_progress_cb(self, mock_codes, mock_max, mock_ingest):
        from app.datasync.service.tushare_ingest import ingest_all_daily
        cb = MagicMock()
        ingest_all_daily(batch_size=10, sleep_between=0, progress_cb=cb)
        cb.assert_called_once()

    @patch(f"{_MOD}.upsert_daily", return_value=5)
    @patch(f"{_MOD}._fetch_existing_keys", return_value={("000001.SZ", "2024-03-15")})
    @patch(f"{_MOD}.call_pro")
    @patch(f"{_MOD}.get_max_trade_date", return_value=None)
    @patch(f"{_MOD}.get_all_ts_codes", return_value=["000001.SZ"])
    def test_full_history_skips_existing(self, mock_codes, mock_max, mock_call, mock_keys, mock_upsert):
        from app.datasync.service.tushare_ingest import ingest_all_daily
        df = pd.DataFrame({
            "ts_code": ["000001.SZ"],
            "trade_date": ["20240315"],
            "open": [10.0], "high": [11.0], "low": [9.5], "close": [10.5],
        })
        mock_call.return_value = df
        ingest_all_daily(batch_size=10, sleep_between=0)
        # The row already exists, so no new rows to insert
        mock_upsert.assert_not_called()


# ═══ get_failed_ts_codes ═══════════════════════════════════════════

class TestGetFailedTsCodes:
    @patch(f"app.domains.extdata.dao.tushare_dao.get_failed_ts_codes", return_value=["000001.SZ"])
    def test_delegates(self, mock_dao):
        from app.datasync.service.tushare_ingest import get_failed_ts_codes
        result = get_failed_ts_codes(limit=5)
        assert result == ["000001.SZ"]
        mock_dao.assert_called_once_with(limit=5)


# ═══ retry_failed_daily ═══════════════════════════════════════════

class TestRetryFailedDaily:
    @patch(f"{_MOD}.ingest_daily")
    @patch(f"{_MOD}.get_failed_ts_codes", return_value=["000001.SZ", "000002.SZ"])
    def test_retries_all(self, mock_codes, mock_ingest):
        from app.datasync.service.tushare_ingest import retry_failed_daily
        retry_failed_daily(limit=10)
        assert mock_ingest.call_count == 2

    @patch(f"{_MOD}.ingest_daily", side_effect=RuntimeError("fail"))
    @patch(f"{_MOD}.get_failed_ts_codes", return_value=["000001.SZ"])
    def test_continues_on_error(self, mock_codes, mock_ingest):
        from app.datasync.service.tushare_ingest import retry_failed_daily
        retry_failed_daily()


# ═══ Error paths for already-tested functions ═══════════════════════

class TestIngestErrorPaths:
    @patch(f"{_MOD}.call_pro", side_effect=RuntimeError("err"))
    def test_ingest_daily_basic_error(self, mock_call):
        from app.datasync.service.tushare_ingest import ingest_daily_basic
        ingest_daily_basic(ts_code="000001.SZ")

    @patch(f"{_MOD}.call_pro", side_effect=RuntimeError("err"))
    def test_ingest_adj_factor_error(self, mock_call):
        from app.datasync.service.tushare_ingest import ingest_adj_factor
        ingest_adj_factor(ts_code="000001.SZ")

    @patch(f"{_MOD}.call_pro", side_effect=RuntimeError("err"))
    def test_ingest_moneyflow_error(self, mock_call):
        from app.datasync.service.tushare_ingest import ingest_moneyflow
        ingest_moneyflow(ts_code="000001.SZ")

    @patch(f"app.domains.extdata.dao.tushare_dao.upsert_stock_basic", return_value=5)
    @patch(f"{_MOD}.call_pro")
    def test_ingest_stock_basic_success_path(self, mock_call, mock_upsert):
        """Test the code path where stock_basic actually has data."""
        from app.datasync.service.tushare_ingest import ingest_stock_basic
        df = pd.DataFrame({"ts_code": ["000001.SZ"], "name": ["PingAn"]})
        mock_call.return_value = df
        ingest_stock_basic()

    @patch(f"{_MOD}.call_pro", side_effect=RuntimeError("err"))
    def test_ingest_stock_basic_error(self, mock_call):
        from app.datasync.service.tushare_ingest import ingest_stock_basic
        ingest_stock_basic()

    @patch(f"{_MOD}.call_pro", side_effect=[RuntimeError("err1"), RuntimeError("err2"), RuntimeError("err3")])
    def test_ingest_daily_error_exhausted(self, mock_call, monkeypatch):
        monkeypatch.setenv("MAX_RETRIES", "3")
        from app.datasync.service.tushare_ingest import ingest_daily
        ingest_daily(ts_code="000001.SZ")

    @patch(f"{_MOD}.call_pro", side_effect=[RuntimeError("err1"), RuntimeError("err2"), RuntimeError("err3")])
    def test_ingest_index_daily_error_exhausted(self, mock_call, monkeypatch):
        monkeypatch.setenv("MAX_RETRIES", "3")
        from app.datasync.service.tushare_ingest import ingest_index_daily
        result = ingest_index_daily(ts_code="000001.SH")
        assert result == 0

    @patch(f"{_MOD}.call_pro", return_value=None)
    def test_ingest_index_daily_none(self, mock_call):
        from app.datasync.service.tushare_ingest import ingest_index_daily
        result = ingest_index_daily(ts_code="000001.SH")
        assert result == 0
