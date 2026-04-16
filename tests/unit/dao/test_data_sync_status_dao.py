"""Unit tests for app.domains.extdata.dao.data_sync_status_dao."""

from __future__ import annotations

from datetime import date, datetime, timedelta
from unittest.mock import MagicMock

import pytest

import app.domains.extdata.dao.data_sync_status_dao as _mod


def _engine_conn(engine_mock):
    """Configure engine_mock.begin()/.connect() to yield a fake conn via context manager."""
    conn = MagicMock()
    ctx = MagicMock()
    ctx.__enter__ = MagicMock(return_value=conn)
    ctx.__exit__ = MagicMock(return_value=False)
    engine_mock.begin.return_value = ctx
    engine_mock.connect.return_value = ctx
    return conn


@pytest.fixture(autouse=True)
def _patch_engines(monkeypatch):
    """Replace all four engines with MagicMock objects."""
    _mod._load_step_resolution_metadata.cache_clear()
    monkeypatch.setattr(_mod, "engine_tm", MagicMock())
    monkeypatch.setattr(_mod, "engine_ts", MagicMock())
    monkeypatch.setattr(_mod, "engine_vn", MagicMock())
    monkeypatch.setattr(_mod, "engine_ak", MagicMock())
    yield
    _mod._load_step_resolution_metadata.cache_clear()


# ── Pure helpers ──────────────────────────────────────────────────

class TestStepMapping:
    def test_known_step(self):
        src, iface = _mod._step_to_source_interface("tushare_stock_daily")
        assert src == "tushare"
        assert iface == "stock_daily"

    def test_legacy_alias_step(self):
        src, iface = _mod._step_to_source_interface("akshare_index")
        assert src == "akshare"
        assert iface == "index_daily"

    def test_unknown_step(self):
        src, iface = _mod._step_to_source_interface("unknown_xyz")
        assert src == "legacy"
        assert iface == "unknown_xyz"

    def test_round_trip(self):
        src, iface = _mod._step_to_source_interface("tushare_stock_daily")
        step = _mod._source_interface_to_step(src, iface)
        assert step == "tushare_stock_daily"

    def test_dynamic_source_from_catalog(self):
        conn = _engine_conn(_mod.engine_tm)
        conn.execute.return_value = MagicMock(fetchall=MagicMock(return_value=[("customfeed", "bars", "bars_daily")]))

        src, iface = _mod._step_to_source_interface("customfeed_bars")
        step = _mod._source_interface_to_step("customfeed", "bars")

        assert (src, iface) == ("customfeed", "bars")
        assert step == "customfeed_bars"


# ── write_step_status / get_step_status ──────────────────────────

class TestStepStatus:
    def test_write(self):
        conn = _engine_conn(_mod.engine_tm)
        _mod.write_step_status(date(2024, 1, 5), "tushare_stock_daily", "success", 100)
        conn.execute.assert_called_once()

    def test_get_found(self):
        conn = _engine_conn(_mod.engine_tm)
        row = MagicMock()
        row.__getitem__ = lambda s, k: "success"
        conn.execute.return_value = MagicMock(fetchone=MagicMock(return_value=row))
        result = _mod.get_step_status(date(2024, 1, 5), "tushare_stock_daily")
        assert result == "success"

    def test_get_not_found(self):
        conn = _engine_conn(_mod.engine_tm)
        conn.execute.return_value = MagicMock(fetchone=MagicMock(return_value=None))
        assert _mod.get_step_status(date(2024, 1, 5), "tushare_stock_daily") is None


# ── counts ───────────────────────────────────────────────────────

class TestCounts:
    def test_stock_daily_counts(self):
        conn = _engine_conn(_mod.engine_ts)
        r = MagicMock()
        r.__getitem__ = lambda s, k: date(2024, 1, 3) if k == 0 else 500
        conn.execute.return_value = MagicMock(fetchall=MagicMock(return_value=[r]))
        result = _mod.get_stock_daily_counts(date(2024, 1, 1), date(2024, 1, 5))
        assert isinstance(result, dict)

    def test_adj_factor_counts(self):
        conn = _engine_conn(_mod.engine_ts)
        conn.execute.return_value = MagicMock(fetchall=MagicMock(return_value=[]))
        result = _mod.get_adj_factor_counts(date(2024, 1, 1), date(2024, 1, 5))
        assert result == {}

    def test_vnpy_counts(self):
        conn = _engine_conn(_mod.engine_vn)
        conn.execute.return_value = MagicMock(fetchall=MagicMock(return_value=[]))
        result = _mod.get_vnpy_counts(date(2024, 1, 1), date(2024, 1, 5))
        assert result == {}

    def test_stock_basic_count(self):
        conn = _engine_conn(_mod.engine_ts)
        row = MagicMock()
        row.__getitem__ = lambda s, k: 5000
        conn.execute.return_value = MagicMock(fetchone=MagicMock(return_value=row))
        assert _mod.get_stock_basic_count() == 5000

    def test_stock_basic_count_none(self):
        conn = _engine_conn(_mod.engine_ts)
        conn.execute.return_value = MagicMock(fetchone=MagicMock(return_value=None))
        assert _mod.get_stock_basic_count() == 0

    def test_adj_factor_count_for_date(self):
        conn = _engine_conn(_mod.engine_ts)
        row = MagicMock()
        row.__getitem__ = lambda s, k: 100
        conn.execute.return_value = MagicMock(fetchone=MagicMock(return_value=row))
        assert _mod.get_adj_factor_count_for_date(date(2024, 1, 5)) == 100

    def test_index_daily_count_for_date(self):
        conn = _engine_conn(_mod.engine_ak)
        row = MagicMock()
        row.__getitem__ = lambda s, k: 42
        conn.execute.return_value = MagicMock(fetchone=MagicMock(return_value=row))
        assert _mod.get_index_daily_count_for_date(date(2024, 1, 5)) == 42

    def test_stock_daily_ts_codes_for_date(self):
        conn = _engine_conn(_mod.engine_ts)
        r = MagicMock()
        r.__getitem__ = lambda s, k: "000001.SZ"
        conn.execute.return_value = MagicMock(fetchall=MagicMock(return_value=[r]))
        result = _mod.get_stock_daily_ts_codes_for_date(date(2024, 1, 5))
        assert result == ["000001.SZ"]


# ── get_cached_trade_dates ───────────────────────────────────────

class TestCachedTradeDates:
    def test_returns_dates(self):
        conn = _engine_conn(_mod.engine_ak)
        r1 = MagicMock()
        r1.__getitem__ = lambda s, k: date(2024, 1, 3)
        r2 = MagicMock()
        r2.__getitem__ = lambda s, k: date(2024, 1, 4)
        conn.execute.return_value = MagicMock(fetchall=MagicMock(return_value=[r1, r2]))
        result = _mod.get_cached_trade_dates(date(2024, 1, 1), date(2024, 1, 5))
        assert len(result) == 2


# ── failed steps ─────────────────────────────────────────────────

class TestFailedSteps:
    def test_returns_list(self):
        conn = _engine_conn(_mod.engine_tm)
        r = MagicMock()
        r.__getitem__ = lambda s, k: {0: date(2024, 1, 3), 1: "tushare", 2: "stock_daily"}[k]
        conn.execute.return_value = MagicMock(fetchall=MagicMock(return_value=[r]))
        result = _mod.get_failed_steps(lookback_days=30)
        assert len(result) == 1
        assert result[0][1] == "tushare_stock_daily"


# ── ensure_tables ────────────────────────────────────────────────

class TestEnsureTables:
    def test_runs(self):
        conn_tm = _engine_conn(_mod.engine_tm)
        conn_ak = _engine_conn(_mod.engine_ak)
        _mod.ensure_tables()
        assert conn_tm.execute.called
        assert conn_ak.execute.called


# ── truncate_trade_cal ───────────────────────────────────────────

class TestTruncateTradeCal:
    def test_runs(self):
        conn = _engine_conn(_mod.engine_ak)
        _mod.truncate_trade_cal()
        conn.execute.assert_called_once()


# ── bulk_upsert_status ───────────────────────────────────────────

class TestBulkUpsert:
    def test_empty(self):
        assert _mod.bulk_upsert_status([]) == 0

    def test_with_rows(self):
        cursor = MagicMock()
        raw = MagicMock()
        raw.cursor.return_value = cursor
        _mod.engine_tm.raw_connection.return_value = raw

        rows = [
            (date(2024, 1, 3), "tushare_stock_daily", "success", 100, None,
             datetime(2024, 1, 3, 9, 0), datetime(2024, 1, 3, 9, 1)),
        ]
        result = _mod.bulk_upsert_status(rows, chunk_size=100)
        assert result == 1
        cursor.executemany.assert_called_once()
        raw.commit.assert_called_once()


# ── upsert_trade_dates ───────────────────────────────────────────

class TestUpsertTradeDates:
    def test_empty(self):
        assert _mod.upsert_trade_dates([]) == 0

    def test_with_dates(self):
        cursor = MagicMock()
        cursor.rowcount = 3
        raw = MagicMock()
        raw.cursor.return_value = cursor
        _mod.engine_ak.raw_connection.return_value = raw

        result = _mod.upsert_trade_dates([date(2024, 1, 3), date(2024, 1, 4)])
        assert result == 3
        cursor.executemany.assert_called_once()


# ── backfill lock functions ──────────────────────────────────────

class TestBackfillLock:
    def test_ensure_backfill_lock_table(self):
        conn = _engine_conn(_mod.engine_tm)
        _mod.ensure_backfill_lock_table()
        assert conn.execute.call_count == 2  # CREATE TABLE + INSERT IGNORE

    def test_acquire_success(self):
        conn = _engine_conn(_mod.engine_tm)
        # ensure_backfill_lock_table calls begin(), release_stale calls begin(), then acquire calls begin()
        # All will share the same mocked conn since engine_tm hasn't changed
        # rowcount > 0 for acquire
        conn.execute.return_value = MagicMock(rowcount=1, fetchone=MagicMock(return_value=None))
        result = _mod.acquire_backfill_lock()
        assert result is True

    def test_acquire_fail(self):
        conn = _engine_conn(_mod.engine_tm)
        conn.execute.return_value = MagicMock(rowcount=0, fetchone=MagicMock(return_value=None))
        result = _mod.acquire_backfill_lock()
        assert result is False

    def test_release(self):
        conn = _engine_conn(_mod.engine_tm)
        _mod.release_backfill_lock()
        assert conn.execute.called

    def test_is_locked_true(self):
        conn = _engine_conn(_mod.engine_tm)
        row = MagicMock()
        row.__getitem__ = lambda s, k: 1
        conn.execute.return_value = MagicMock(fetchone=MagicMock(return_value=row))
        assert _mod.is_backfill_locked() is True

    def test_is_locked_false(self):
        conn = _engine_conn(_mod.engine_tm)
        row = MagicMock()
        row.__getitem__ = lambda s, k: 0
        conn.execute.return_value = MagicMock(fetchone=MagicMock(return_value=row))
        assert _mod.is_backfill_locked() is False

    def test_release_stale_no_row(self):
        conn = _engine_conn(_mod.engine_tm)
        conn.execute.return_value = MagicMock(rowcount=0)
        assert _mod.release_stale_backfill_lock(max_age_hours=6) is False

    def test_release_stale_fresh_lock(self):
        conn = _engine_conn(_mod.engine_tm)
        conn.execute.return_value = MagicMock(rowcount=0)
        assert _mod.release_stale_backfill_lock(max_age_hours=6) is False

    def test_release_stale_old_lock(self):
        conn = _engine_conn(_mod.engine_tm)
        conn.execute.return_value = MagicMock(rowcount=1)
        result = _mod.release_stale_backfill_lock(max_age_hours=6)
        assert result is True


# ── token-based lock ─────────────────────────────────────────────

class TestTokenLock:
    def test_acquire_with_token(self):
        conn = _engine_conn(_mod.engine_tm)
        conn.execute.return_value = MagicMock(rowcount=1, fetchone=MagicMock(return_value=None))
        assert _mod.acquire_backfill_lock_with_token("tok-123") is True

    def test_refresh(self):
        conn = _engine_conn(_mod.engine_tm)
        conn.execute.return_value = MagicMock(rowcount=1)
        assert _mod.refresh_backfill_lock("tok-123") is True

    def test_refresh_not_owner(self):
        conn = _engine_conn(_mod.engine_tm)
        conn.execute.return_value = MagicMock(rowcount=0)
        assert _mod.refresh_backfill_lock("wrong-token") is False

    def test_release_with_token(self):
        conn = _engine_conn(_mod.engine_tm)
        conn.execute.return_value = MagicMock(rowcount=1)
        assert _mod.release_backfill_lock_token("tok-123") is True

    def test_release_wrong_token(self):
        conn = _engine_conn(_mod.engine_tm)
        conn.execute.return_value = MagicMock(rowcount=0)
        assert _mod.release_backfill_lock_token("bad") is False
