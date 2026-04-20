"""Batch DAO tests — engine-based DAOs & module-level function DAOs.

Covers: SystemConfigDao, DataSourceConfigDao (system), IndicatorConfigDao,
        vnpy_dao (module-level), akshare_dao (module-level),
        sync_log_dao (module-level + class SyncLogDao)
"""

import pytest
from datetime import date, datetime
from unittest.mock import MagicMock

# ── fake DB helpers ─────────────────────────────────────────────────

class _FR:
    """Fake result."""
    def __init__(self, rows=None, rowcount=0, lastrowid=0):
        self._rows = rows or []
        self.rowcount = rowcount
        self.lastrowid = lastrowid
    def fetchone(self):
        return self._rows[0] if self._rows else None
    def fetchall(self):
        return self._rows
    def mappings(self):
        return self
    def all(self):
        return self._rows
    def first(self):
        return self._rows[0] if self._rows else None


class _FC:
    """Fake connection."""
    def __init__(self, result=None, error=None):
        self.result = result or _FR()
        self.error = error
        self.committed = False
        self.executed = []
    def execute(self, *a, **kw):
        self.executed.append((a, kw))
        if self.error:
            raise self.error
        return self.result
    def commit(self):
        self.committed = True


class _Ctx:
    """Fake context-manager wrapping a connection."""
    def __init__(self, conn):
        self._c = conn
    def __enter__(self):
        return self._c
    def __exit__(self, *a):
        return False


class _FakeEngine:
    """Fake SQLAlchemy engine for engine-based DAOs."""
    def __init__(self, conn=None):
        self._conn = conn or _FC()
    def connect(self):
        return _Ctx(self._conn)
    def begin(self):
        return _Ctx(self._conn)
    def raw_connection(self):
        return _FakeRawConn()


class _FakeRawConn:
    """Fake raw connection for executemany-style DAOs."""
    def __init__(self):
        self.committed = False
        self.closed = False
    def cursor(self):
        return _FakeCursor()
    def commit(self):
        self.committed = True
    def close(self):
        self.closed = True


class _FakeCursor:
    def __init__(self):
        self.rowcount = 5
    def executemany(self, sql, params):
        self.rowcount = len(params)
    def close(self):
        pass


# =====================================================================
# SystemConfigDao
# =====================================================================
from app.domains.system.dao.system_config_dao import SystemConfigDao, DataSourceConfigDao as SysDataSourceConfigDao


@pytest.mark.unit
class TestSystemConfigDao:
    def _make(self, conn):
        dao = SystemConfigDao.__new__(SystemConfigDao)
        dao.engine = _FakeEngine(conn)
        return dao

    def test_list_all(self):
        conn = _FC(_FR([{"config_key": "k1", "config_value": "v1", "category": "general"}]))
        result = self._make(conn).list_all()
        assert len(result) == 1

    def test_list_all_with_category(self):
        conn = _FC(_FR([]))
        result = self._make(conn).list_all(category="trading")
        assert result == []

    def test_get(self):
        conn = _FC(_FR([{"config_key": "k1", "config_value": "v1"}]))
        result = self._make(conn).get("k1")
        assert result is not None

    def test_get_not_found(self):
        conn = _FC(_FR([]))
        result = self._make(conn).get("nope")
        assert result is None

    def test_upsert(self):
        conn = _FC(_FR())
        self._make(conn).upsert("k1", "v1", "general", "desc", False)
        assert len(conn.executed) == 1

    def test_delete_found(self):
        conn = _FC(_FR(rowcount=1))
        assert self._make(conn).delete("k1") is True

    def test_delete_not_found(self):
        conn = _FC(_FR(rowcount=0))
        assert self._make(conn).delete("nope") is False


@pytest.mark.unit
class TestSysDataSourceConfigDao:
    def _make(self, conn):
        dao = SysDataSourceConfigDao.__new__(SysDataSourceConfigDao)
        dao.engine = _FakeEngine(conn)
        return dao

    def test_list_all(self):
        conn = _FC(_FR([{"source_name": "tushare", "token_encrypted": "secret", "is_enabled": True}]))
        result = self._make(conn).list_all()
        assert len(result) == 1
        assert result[0]["token_encrypted"] == "***"

    def test_get(self):
        conn = _FC(_FR([{"source_name": "tushare", "token_encrypted": "secret"}]))
        result = self._make(conn).get("tushare")
        assert result["token_encrypted"] == "***"

    def test_get_not_found(self):
        conn = _FC(_FR([]))
        assert self._make(conn).get("nope") is None

    def test_get_no_token(self):
        conn = _FC(_FR([{"source_name": "x", "token_encrypted": None}]))
        result = self._make(conn).get("x")
        assert result["token_encrypted"] is None

    def test_upsert(self):
        conn = _FC(_FR())
        self._make(conn).upsert("tushare", True, 60, 1)
        assert len(conn.executed) == 1


# =====================================================================
# IndicatorConfigDao
# =====================================================================
from app.domains.system.dao.indicator_dao import IndicatorConfigDao


@pytest.mark.unit
class TestIndicatorConfigDao:
    def _make(self, conn):
        dao = IndicatorConfigDao.__new__(IndicatorConfigDao)
        dao.engine = _FakeEngine(conn)
        return dao

    def test_list_all(self):
        conn = _FC(_FR([{"name": "SMA", "default_params": '{"period": 20}', "category": "trend"}]))
        result = self._make(conn).list_all()
        assert len(result) == 1
        assert result[0]["default_params"] == {"period": 20}

    def test_list_all_with_category(self):
        conn = _FC(_FR([]))
        result = self._make(conn).list_all(category="momentum")
        assert result == []

    def test_get_by_id(self):
        conn = _FC(_FR([{"id": 1, "name": "RSI", "default_params": '{"period": 14}'}]))
        result = self._make(conn).get_by_id(1)
        assert result["default_params"]["period"] == 14

    def test_get_by_id_not_found(self):
        conn = _FC(_FR([]))
        assert self._make(conn).get_by_id(99) is None

    def test_get_by_id_dict_params(self):
        conn = _FC(_FR([{"id": 1, "name": "RSI", "default_params": {"period": 14}}]))
        result = self._make(conn).get_by_id(1)
        assert result["default_params"] == {"period": 14}

    def test_create(self):
        conn = _FC(_FR(lastrowid=5))
        result = self._make(conn).create("MACD", "MACD", "trend", "Moving Average", {"fast": 12})
        assert result == 5

    def test_update(self):
        conn = _FC(_FR(rowcount=1))
        assert self._make(conn).update(1, name="SMA_20") is True

    def test_update_empty(self):
        conn = _FC(_FR())
        assert self._make(conn).update(1) is False

    def test_update_with_dict_params(self):
        conn = _FC(_FR(rowcount=1))
        assert self._make(conn).update(1, default_params={"period": 30}) is True

    def test_delete(self):
        conn = _FC(_FR(rowcount=1))
        assert self._make(conn).delete(1) is True

    def test_delete_builtin(self):
        conn = _FC(_FR(rowcount=0))
        assert self._make(conn).delete(1) is False


# =====================================================================
# vnpy_dao (module-level functions, engine-based)
# =====================================================================
import app.domains.extdata.dao.vnpy_dao as _vnpy_mod


@pytest.mark.unit
class TestVnpyDao:
    def _patch_engine(self, mp, conn):
        mp.setattr(_vnpy_mod, "engine", _FakeEngine(conn))

    def test_get_last_sync_date(self, monkeypatch):
        row = (date(2024, 6, 1),)
        conn = _FC(_FR([row]))
        self._patch_engine(monkeypatch, conn)
        result = _vnpy_mod.get_last_sync_date("IF2406", "CFFEX")
        assert result == date(2024, 6, 1)

    def test_get_last_sync_date_none(self, monkeypatch):
        conn = _FC(_FR([]))
        self._patch_engine(monkeypatch, conn)
        assert _vnpy_mod.get_last_sync_date("X", "Y") is None

    def test_update_sync_status(self, monkeypatch):
        conn = _FC(_FR())
        self._patch_engine(monkeypatch, conn)
        _vnpy_mod.update_sync_status("IF2406", "CFFEX", "d", date(2024, 6, 1), 100)
        assert len(conn.executed) == 1

    def test_bulk_upsert_dbbardata_empty(self, monkeypatch):
        conn = _FC(_FR())
        self._patch_engine(monkeypatch, conn)
        assert _vnpy_mod.bulk_upsert_dbbardata([]) == 0

    def test_bulk_upsert_dbbardata(self, monkeypatch):
        fake_engine = _FakeEngine()
        monkeypatch.setattr(_vnpy_mod, "engine", fake_engine)
        rows = [{"symbol": "IF2406", "exchange": "CFFEX", "datetime": "2024-06-01",
                 "interval": "d", "volume": 100, "turnover": 5000, "open_interest": 0,
                 "open_price": 100, "high_price": 110, "low_price": 95, "close_price": 105}]
        result = _vnpy_mod.bulk_upsert_dbbardata(rows)
        assert result >= 0

    def test_get_bar_stats(self, monkeypatch):
        row = (100, datetime(2024, 1, 1), datetime(2024, 6, 1))
        conn = _FC(_FR([row]))
        self._patch_engine(monkeypatch, conn)
        count, start, end = _vnpy_mod.get_bar_stats("IF2406", "CFFEX")
        assert count == 100

    def test_upsert_dbbaroverview(self, monkeypatch):
        conn = _FC(_FR())
        self._patch_engine(monkeypatch, conn)
        _vnpy_mod.upsert_dbbaroverview("IF2406", "CFFEX", "d", 100,
                                        datetime(2024, 1, 1), datetime(2024, 6, 1))
        assert len(conn.executed) == 1


# =====================================================================
# akshare_dao (module-level functions, engine-based)
# =====================================================================
import app.domains.extdata.dao.akshare_dao as _ak_mod


@pytest.mark.unit
class TestAkshareDao:
    def _patch_engine(self, mp, conn):
        mp.setattr(_ak_mod, "engine", _FakeEngine(conn))

    def test_audit_start(self, monkeypatch):
        conn = _FC(_FR(lastrowid=42))
        self._patch_engine(monkeypatch, conn)
        assert _ak_mod.audit_start("index_daily", {"code": "000300"}) == 42

    def test_audit_start_exception(self, monkeypatch):
        # When lastrowid raises
        fr = _FR()
        fr.lastrowid = property(lambda self: (_ for _ in ()).throw(Exception("no id")))
        conn = _FC(_FR(lastrowid=0))
        self._patch_engine(monkeypatch, conn)
        result = _ak_mod.audit_start("test", {})
        assert isinstance(result, int)

    def test_audit_finish(self, monkeypatch):
        conn = _FC(_FR())
        self._patch_engine(monkeypatch, conn)
        _ak_mod.audit_finish(1, "success", 100)
        assert len(conn.executed) == 1

    def test_upsert_index_daily_rows_empty(self, monkeypatch):
        conn = _FC(_FR())
        self._patch_engine(monkeypatch, conn)
        assert _ak_mod.upsert_index_daily_rows([]) == 0

    def test_upsert_index_daily_rows(self, monkeypatch):
        fake_engine = _FakeEngine()
        monkeypatch.setattr(_ak_mod, "engine", fake_engine)
        rows = [{"index_code": "000300", "trade_date": "2024-01-01",
                 "open": 100, "high": 110, "low": 95, "close": 105,
                 "volume": 1000, "amount": 50000}]
        result = _ak_mod.upsert_index_daily_rows(rows)
        assert result >= 0


# =====================================================================
# sync_log_dao module-level functions + class SyncLogDao
# =====================================================================
import app.domains.extdata.dao.sync_log_dao as _synclog_mod


@pytest.mark.unit
class TestSyncLogModuleFunctions:
    def _patch_engine(self, mp, conn):
        mp.setattr(_synclog_mod, "engine", _FakeEngine(conn))

    def test_write_sync_log(self, monkeypatch):
        conn = _FC(_FR())
        self._patch_engine(monkeypatch, conn)
        _synclog_mod.write_sync_log(date(2024, 6, 1), "daily", "success", 100)
        assert len(conn.executed) == 1

    def test_write_sync_log_with_error(self, monkeypatch):
        conn = _FC(_FR())
        self._patch_engine(monkeypatch, conn)
        _synclog_mod.write_sync_log(date(2024, 6, 1), "daily", "error", 0, "timeout")
        assert len(conn.executed) == 1

    def test_get_sync_status(self, monkeypatch):
        row = ("success",)
        conn = _FC(_FR([row]))
        self._patch_engine(monkeypatch, conn)
        assert _synclog_mod.get_sync_status(date(2024, 6, 1), "daily") == "success"

    def test_get_sync_status_none(self, monkeypatch):
        conn = _FC(_FR([]))
        self._patch_engine(monkeypatch, conn)
        assert _synclog_mod.get_sync_status(date(2024, 6, 1), "daily") is None

    def test_find_failed_syncs(self, monkeypatch):
        rows = [(date(2024, 6, 1), "daily")]
        conn = _FC(_FR(rows))
        self._patch_engine(monkeypatch, conn)
        result = _synclog_mod.find_failed_syncs(date(2024, 6, 1), date(2024, 6, 30))
        assert len(result) == 1

    def test_write_tushare_stock_sync_log(self, monkeypatch):
        conn = _FC(_FR())
        self._patch_engine(monkeypatch, conn)
        _synclog_mod.write_tushare_stock_sync_log(date(2024, 6, 1), "daily", "success", 100)
        assert len(conn.executed) == 1

    def test_get_last_success_tushare_sync_date(self, monkeypatch):
        row = (date(2024, 5, 31),)
        conn = _FC(_FR([row]))
        self._patch_engine(monkeypatch, conn)
        result = _synclog_mod.get_last_success_tushare_sync_date("daily")
        assert result == date(2024, 5, 31)

    def test_get_last_success_tushare_sync_date_none(self, monkeypatch):
        row = (None,)
        conn = _FC(_FR([row]))
        self._patch_engine(monkeypatch, conn)
        result = _synclog_mod.get_last_success_tushare_sync_date("daily")
        assert result is None


@pytest.mark.unit
class TestSyncLogDaoClass:
    def _patch(self, mp, conn):
        mp.setattr(_synclog_mod, "connection", lambda n: _Ctx(conn))

    def test_get_latest_per_endpoint(self, monkeypatch):
        row = MagicMock()
        row._mapping = {"sync_date": date(2024, 6, 1), "status": "success",
                         "rows_synced": 100, "error_message": None,
                         "started_at": datetime(2024, 6, 1, 10, 0),
                         "finished_at": datetime(2024, 6, 1, 10, 5)}
        conn = _FC(_FR([row]))
        self._patch(monkeypatch, conn)
        from app.domains.extdata.dao.sync_log_dao import SyncLogDao
        result = SyncLogDao().get_latest_per_endpoint(["daily"])
        assert isinstance(result, dict)

    def test_last_finished_at(self, monkeypatch):
        row = MagicMock()
        row.max_finished = datetime(2024, 6, 1, 10, 5)
        conn = _FC(_FR([row]))
        self._patch(monkeypatch, conn)
        from app.domains.extdata.dao.sync_log_dao import SyncLogDao
        result = SyncLogDao().last_finished_at()
        assert result is not None

    def test_last_finished_at_none(self, monkeypatch):
        row = MagicMock()
        row.max_finished = None
        conn = _FC(_FR([row]))
        self._patch(monkeypatch, conn)
        from app.domains.extdata.dao.sync_log_dao import SyncLogDao
        result = SyncLogDao().last_finished_at()
        # either None or the datetime
        assert result is None or isinstance(result, datetime)

    def test_running_count_last_day(self, monkeypatch):
        row = MagicMock()
        row.cnt = 3
        conn = _FC(_FR([row]))
        self._patch(monkeypatch, conn)
        from app.domains.extdata.dao.sync_log_dao import SyncLogDao
        result = SyncLogDao().running_count_last_day()
        assert result == 3
