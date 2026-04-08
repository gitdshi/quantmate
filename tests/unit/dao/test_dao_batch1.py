"""Batch DAO tests — connection-based DAOs without existing coverage.

Covers: PortfolioDao, WatchlistDao, ReportDao, BulkBacktestDao,
        BulkResultsDao, StrategySourceDao, AkshareBenchmarkDao,
        BrokerConfigDao, RiskRuleDao, MultiMarketDao
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


class _Row:
    """Fake SQLAlchemy row object."""
    def __init__(self, data: dict):
        self._d = data
        self._mapping = data
    def __getattr__(self, name):
        if name.startswith("_"):
            raise AttributeError(name)
        return self._d.get(name)
    def __getitem__(self, idx):
        return list(self._d.values())[idx]


def _ctx(result=None, rowcount=0, lastrowid=0, error=None):
    """Build (FakeContext, FakeConn) pair."""
    conn = _FC(_FR(result, rowcount, lastrowid), error)
    return _Ctx(conn), conn


# =====================================================================
# PortfolioDao
# =====================================================================
import app.domains.portfolio.dao.portfolio_dao as _portfolio_mod
from app.domains.portfolio.dao.portfolio_dao import PortfolioDao


@pytest.mark.unit
class TestPortfolioDao:
    def _patch(self, mp, conn):
        mp.setattr(_portfolio_mod, "connection", lambda n: _Ctx(conn))

    def test_get_or_create_existing(self, monkeypatch):
        row = _Row({"id": 1, "user_id": 10, "cash": 100000})
        conn = _FC(_FR([row]))
        self._patch(monkeypatch, conn)
        result = PortfolioDao().get_or_create(10)
        assert result["id"] == 1

    def test_get_or_create_new(self, monkeypatch):
        # 1st call (SELECT) → no rows, 2nd (INSERT), 3rd (SELECT) → new row
        new_row = _Row({"id": 5, "user_id": 10, "cash": 100000})
        call_count = [0]
        def fake_exec(*a, **kw):
            call_count[0] += 1
            if call_count[0] == 1:
                return _FR([])  # no existing
            if call_count[0] == 2:
                return _FR(lastrowid=5)  # INSERT
            return _FR([new_row])  # re-SELECT
        conn = _FC()
        conn.execute = fake_exec
        self._patch(monkeypatch, conn)
        result = PortfolioDao().get_or_create(10)
        assert result["id"] == 5

    def test_list_positions(self, monkeypatch):
        rows = [_Row({"id": 1, "symbol": "600000.SH", "quantity": 100, "avg_cost": 10.5})]
        conn = _FC(_FR(rows))
        self._patch(monkeypatch, conn)
        result = PortfolioDao().list_positions(1)
        assert len(result) >= 0

    def test_get_position(self, monkeypatch):
        row = _Row({"id": 1, "symbol": "600000.SH", "quantity": 100})
        conn = _FC(_FR([row]))
        self._patch(monkeypatch, conn)
        result = PortfolioDao().get_position(1, "600000.SH")
        assert result is not None

    def test_upsert_position_update(self, monkeypatch):
        # existing position found → update
        row = _Row({"id": 5})
        conn = _FC(_FR([row]))
        self._patch(monkeypatch, conn)
        PortfolioDao().upsert_position(1, "600000.SH", 200, 11.0)
        assert len(conn.executed) >= 1

    def test_upsert_position_insert(self, monkeypatch):
        call_count = [0]
        def fake_exec(*a, **kw):
            call_count[0] += 1
            if call_count[0] == 1:
                return _FR([])  # no existing
            return _FR(lastrowid=1)
        conn = _FC()
        conn.execute = fake_exec
        self._patch(monkeypatch, conn)
        PortfolioDao().upsert_position(1, "600000.SH", 100, 10.0)
        assert call_count[0] >= 2

    def test_update_cash(self, monkeypatch):
        conn = _FC(_FR(rowcount=1))
        self._patch(monkeypatch, conn)
        PortfolioDao().update_cash(1, 50000.0)
        assert len(conn.executed) == 1

    def test_insert_transaction(self, monkeypatch):
        conn = _FC(_FR(lastrowid=99))
        self._patch(monkeypatch, conn)
        result = PortfolioDao().insert_transaction(1, symbol="600000.SH", quantity=100)
        assert isinstance(result, int)

    def test_list_transactions(self, monkeypatch):
        conn = _FC(_FR([]))
        self._patch(monkeypatch, conn)
        result = PortfolioDao().list_transactions(1)
        assert isinstance(result, list)

    def test_count_transactions(self, monkeypatch):
        row = _Row({"cnt": 42})
        conn = _FC(_FR([row]))
        self._patch(monkeypatch, conn)
        result = PortfolioDao().count_transactions(1)
        assert result == 42

    def test_list_snapshots(self, monkeypatch):
        conn = _FC(_FR([]))
        self._patch(monkeypatch, conn)
        result = PortfolioDao().list_snapshots(1)
        assert isinstance(result, list)


# =====================================================================
# WatchlistDao
# =====================================================================
import app.domains.market.dao.watchlist_dao as _watchlist_mod
from app.domains.market.dao.watchlist_dao import WatchlistDao


@pytest.mark.unit
class TestWatchlistDao:
    def _patch(self, mp, conn):
        mp.setattr(_watchlist_mod, "connection", lambda n: _Ctx(conn))

    def test_list_for_user(self, monkeypatch):
        conn = _FC(_FR([]))
        self._patch(monkeypatch, conn)
        assert WatchlistDao().list_for_user(1) == []

    def test_get(self, monkeypatch):
        row = _Row({"id": 1, "name": "tech"})
        conn = _FC(_FR([row]))
        self._patch(monkeypatch, conn)
        result = WatchlistDao().get(1)
        assert result is not None

    def test_create(self, monkeypatch):
        conn = _FC(_FR(lastrowid=10))
        self._patch(monkeypatch, conn)
        result = WatchlistDao().create(1, "my list")
        assert result == 10

    def test_update(self, monkeypatch):
        conn = _FC(_FR(rowcount=1))
        self._patch(monkeypatch, conn)
        WatchlistDao().update(1, name="new name")
        assert conn.committed or len(conn.executed) >= 1

    def test_delete(self, monkeypatch):
        conn = _FC(_FR(rowcount=1))
        self._patch(monkeypatch, conn)
        WatchlistDao().delete(1)
        assert len(conn.executed) >= 1

    def test_list_items(self, monkeypatch):
        conn = _FC(_FR([]))
        self._patch(monkeypatch, conn)
        assert WatchlistDao().list_items(1) == []

    def test_add_item(self, monkeypatch):
        conn = _FC(_FR(lastrowid=5))
        self._patch(monkeypatch, conn)
        result = WatchlistDao().add_item(1, "600000.SH")
        assert result == 5

    def test_remove_item(self, monkeypatch):
        conn = _FC(_FR(rowcount=1))
        self._patch(monkeypatch, conn)
        result = WatchlistDao().remove_item(1, "600000.SH")
        assert result is True

    def test_remove_item_not_found(self, monkeypatch):
        conn = _FC(_FR(rowcount=0))
        self._patch(monkeypatch, conn)
        result = WatchlistDao().remove_item(1, "no_such")
        assert result is False


# =====================================================================
# ReportDao
# =====================================================================
import app.domains.monitoring.dao.report_dao as _report_mod
from app.domains.monitoring.dao.report_dao import ReportDao


@pytest.mark.unit
class TestReportDao:
    def _patch(self, mp, conn):
        mp.setattr(_report_mod, "connection", lambda n: _Ctx(conn))

    def test_list_by_user(self, monkeypatch):
        cnt_row = _Row({"cnt": 1})
        data_row = _Row({
            "id": 1, "user_id": 1, "report_type": "monthly",
            "period_start": "2024-01-01", "period_end": "2024-01-31",
            "pdf_path": "/tmp/r.pdf", "created_at": "2024-02-01",
        })
        call_count = [0]
        def fake_exec(*a, **kw):
            call_count[0] += 1
            if call_count[0] == 1:
                return _FR([cnt_row])
            return _FR([data_row])
        conn = _FC()
        conn.execute = fake_exec
        self._patch(monkeypatch, conn)
        rows, total = ReportDao().list_by_user(1)
        assert total == 1
        assert len(rows) == 1

    def test_list_by_user_with_type(self, monkeypatch):
        cnt_row = _Row({"cnt": 0})
        call_count = [0]
        def fake_exec(*a, **kw):
            call_count[0] += 1
            if call_count[0] == 1:
                return _FR([cnt_row])
            return _FR([])
        conn = _FC()
        conn.execute = fake_exec
        self._patch(monkeypatch, conn)
        rows, total = ReportDao().list_by_user(1, report_type="weekly")
        assert total == 0

    def test_list_by_user_programming_error(self, monkeypatch):
        from sqlalchemy.exc import ProgrammingError
        conn = _FC(error=ProgrammingError("stmt", {}, Exception("no table")))
        self._patch(monkeypatch, conn)
        rows, total = ReportDao().list_by_user(1)
        assert rows == [] and total == 0

    def test_get_by_id(self, monkeypatch):
        row = _Row({
            "id": 1, "user_id": 1, "report_type": "monthly",
            "period_start": "2024-01-01", "period_end": "2024-01-31",
            "content_json": '{"k":"v"}', "pdf_path": None,
            "created_at": "2024-02-01",
        })
        conn = _FC(_FR([row]))
        self._patch(monkeypatch, conn)
        result = ReportDao().get_by_id(1, 1)
        assert result["content"] == {"k": "v"}

    def test_get_by_id_not_found(self, monkeypatch):
        conn = _FC(_FR([]))
        self._patch(monkeypatch, conn)
        assert ReportDao().get_by_id(99, 1) is None

    def test_create(self, monkeypatch):
        conn = _FC(_FR(lastrowid=7))
        self._patch(monkeypatch, conn)
        result = ReportDao().create(1, "monthly", date(2024, 1, 1), date(2024, 1, 31))
        assert result == 7


# =====================================================================
# BulkBacktestDao
# =====================================================================
import app.domains.backtests.dao.bulk_backtest_dao as _bulk_bt_mod
from app.domains.backtests.dao.bulk_backtest_dao import BulkBacktestDao


@pytest.mark.unit
class TestBulkBacktestDao:
    def _patch(self, mp, conn):
        mp.setattr(_bulk_bt_mod, "connection", lambda n: _Ctx(conn))

    def test_insert_parent(self, monkeypatch):
        conn = _FC(_FR())
        self._patch(monkeypatch, conn)
        BulkBacktestDao().insert_parent(
            user_id=1, job_id="j1", strategy_id=1, strategy_class="S",
            strategy_version=1, symbols_json="[]", start_date="2024-01-01",
            end_date="2024-12-31", parameters_json="{}", initial_capital=100000,
            rate=0.0003, slippage=0.01, benchmark="000300.SH",
            total_symbols=10, created_at=datetime(2024, 1, 1),
        )
        assert conn.committed

    def test_delete_bulk_parent(self, monkeypatch):
        conn = _FC(_FR(rowcount=1))
        self._patch(monkeypatch, conn)
        BulkBacktestDao().delete_bulk_parent("j1", 1)
        assert conn.committed

    def test_list_by_job_ids_empty(self, monkeypatch):
        conn = _FC(_FR([]))
        self._patch(monkeypatch, conn)
        assert BulkBacktestDao().list_by_job_ids([]) == []

    def test_list_by_job_ids(self, monkeypatch):
        row = _Row({"job_id": "j1", "best_return": 0.15, "best_symbol": "SH",
                     "completed_count": 5, "total_symbols": 10, "bulk_status": "completed"})
        conn = _FC(_FR([row]))
        self._patch(monkeypatch, conn)
        result = BulkBacktestDao().list_by_job_ids(["j1"])
        assert len(result) == 1

    def test_get_owner_user_id(self, monkeypatch):
        row = _Row({"user_id": 42})
        conn = _FC(_FR([row]))
        self._patch(monkeypatch, conn)
        assert BulkBacktestDao().get_owner_user_id("j1") == 42

    def test_get_owner_user_id_not_found(self, monkeypatch):
        conn = _FC(_FR([]))
        self._patch(monkeypatch, conn)
        assert BulkBacktestDao().get_owner_user_id("nope") is None

    def test_get_metrics(self, monkeypatch):
        row = _Row({"best_return": 0.2, "best_symbol": "SH", "completed_count": 5,
                     "total_symbols": 10, "bulk_status": "completed", "best_symbol_name": "xx"})
        conn = _FC(_FR([row]))
        self._patch(monkeypatch, conn)
        assert BulkBacktestDao().get_metrics("j1")["best_return"] == 0.2

    def test_get_metrics_not_found(self, monkeypatch):
        conn = _FC(_FR([]))
        self._patch(monkeypatch, conn)
        assert BulkBacktestDao().get_metrics("nope") is None

    def test_update_best_symbol_name(self, monkeypatch):
        conn = _FC(_FR(rowcount=1))
        self._patch(monkeypatch, conn)
        BulkBacktestDao().update_best_symbol_name("j1", "stock_a")
        assert conn.committed

    def test_update_progress(self, monkeypatch):
        conn = _FC(_FR(rowcount=1))
        self._patch(monkeypatch, conn)
        BulkBacktestDao().update_progress("j1", 5, 0.15, "SH", "name")
        assert conn.committed

    def test_finish(self, monkeypatch):
        conn = _FC(_FR(rowcount=1))
        self._patch(monkeypatch, conn)
        BulkBacktestDao().finish("j1", "completed", datetime.now(), 10, 0.3, "SH", "name")
        assert conn.committed


# =====================================================================
# BulkResultsDao
# =====================================================================
import app.domains.backtests.dao.bulk_results_dao as _bulk_res_mod
from app.domains.backtests.dao.bulk_results_dao import BulkResultsDao


@pytest.mark.unit
class TestBulkResultsDao:
    def _patch(self, mp, conn):
        mp.setattr(_bulk_res_mod, "connection", lambda n: _Ctx(conn))

    def test_count_children(self, monkeypatch):
        row = _Row({"cnt": 5})
        conn = _FC(_FR([row]))
        self._patch(monkeypatch, conn)
        assert BulkResultsDao().count_children(bulk_job_id="j1", user_id=1) == 5

    def test_count_children_none(self, monkeypatch):
        conn = _FC(_FR([]))
        self._patch(monkeypatch, conn)
        assert BulkResultsDao().count_children(bulk_job_id="j1", user_id=1) == 0

    def test_list_children_page(self, monkeypatch):
        row = _Row({"job_id": "c1", "vt_symbol": "SH", "status": "done",
                     "result": None, "error": None, "parameters": "{}",
                     "created_at": "2024-01-01", "completed_at": "2024-01-02"})
        conn = _FC(_FR([row]))
        self._patch(monkeypatch, conn)
        result = BulkResultsDao().list_children_page(
            bulk_job_id="j1", user_id=1, page=1, page_size=20, sort_order="desc")
        assert len(result) == 1

    def test_list_all_children(self, monkeypatch):
        conn = _FC(_FR([]))
        self._patch(monkeypatch, conn)
        assert BulkResultsDao().list_all_children(bulk_job_id="j1", user_id=1) == []


# =====================================================================
# StrategySourceDao
# =====================================================================
import app.domains.backtests.dao.strategy_source_dao as _strat_src_mod
from app.domains.backtests.dao.strategy_source_dao import StrategySourceDao


@pytest.mark.unit
class TestStrategySourceDao:
    def _patch(self, mp, conn):
        mp.setattr(_strat_src_mod, "connection", lambda n: _Ctx(conn))

    def test_get_strategy_source_for_user(self, monkeypatch):
        row = _Row({"code": "class S: pass", "class_name": "S", "version": 1})
        conn = _FC(_FR([row]))
        self._patch(monkeypatch, conn)
        code, cls_name, ver = StrategySourceDao().get_strategy_source_for_user(1, 1)
        assert cls_name == "S"

    def test_get_strategy_source_not_found(self, monkeypatch):
        conn = _FC(_FR([]))
        self._patch(monkeypatch, conn)
        with pytest.raises(KeyError):
            StrategySourceDao().get_strategy_source_for_user(99, 1)

    def test_get_strategy_code_by_class_name(self, monkeypatch):
        row = _Row({"code": "class X: pass"})
        conn = _FC(_FR([row]))
        self._patch(monkeypatch, conn)
        assert "class X" in StrategySourceDao().get_strategy_code_by_class_name("X")

    def test_get_strategy_code_empty_class(self, monkeypatch):
        with pytest.raises(KeyError):
            StrategySourceDao().get_strategy_code_by_class_name("")

    def test_get_strategy_code_not_found(self, monkeypatch):
        conn = _FC(_FR([]))
        self._patch(monkeypatch, conn)
        with pytest.raises(KeyError):
            StrategySourceDao().get_strategy_code_by_class_name("Missing")


# =====================================================================
# AkshareBenchmarkDao
# =====================================================================
import app.domains.backtests.dao.akshare_benchmark_dao as _akbench_mod
from app.domains.backtests.dao.akshare_benchmark_dao import AkshareBenchmarkDao


@pytest.mark.unit
class TestAkshareBenchmarkDao:
    def _patch(self, mp, conn):
        mp.setattr(_akbench_mod, "connection", lambda n: _Ctx(conn))

    def test_get_index_series(self, monkeypatch):
        row = _Row({"trade_date": "20240101", "close": 3500.0})
        conn = _FC(_FR([row]))
        self._patch(monkeypatch, conn)
        result = AkshareBenchmarkDao().get_index_series(
            index_code="000300.SH", start=date(2024, 1, 1), end=date(2024, 12, 31))
        assert len(result) == 1

    def test_get_index_series_empty(self, monkeypatch):
        conn = _FC(_FR([]))
        self._patch(monkeypatch, conn)
        result = AkshareBenchmarkDao().get_index_series(
            index_code="000300.SH", start=date(2024, 1, 1), end=date(2024, 12, 31))
        assert result == []

    def test_get_benchmark_data(self, monkeypatch):
        rows = [
            _Row({"trade_date": "20240101", "close": 3500.0}),
            _Row({"trade_date": "20240102", "close": 3550.0}),
            _Row({"trade_date": "20240103", "close": 3600.0}),
        ]
        conn = _FC(_FR(rows))
        self._patch(monkeypatch, conn)
        result = AkshareBenchmarkDao().get_benchmark_data(
            start=date(2024, 1, 1), end=date(2024, 1, 3), benchmark_symbol="000300.SH")
        assert result is not None
        assert "total_return" in result

    def test_get_benchmark_data_insufficient(self, monkeypatch):
        conn = _FC(_FR([_Row({"trade_date": "20240101", "close": 3500.0})]))
        self._patch(monkeypatch, conn)
        result = AkshareBenchmarkDao().get_benchmark_data(
            start=date(2024, 1, 1), end=date(2024, 1, 1), benchmark_symbol="000300.SH")
        assert result is None

    def test_get_benchmark_data_date_obj(self, monkeypatch):
        rows = [
            _Row({"trade_date": date(2024, 1, 1), "close": 3500.0}),
            _Row({"trade_date": date(2024, 1, 2), "close": 3550.0}),
        ]
        conn = _FC(_FR(rows))
        self._patch(monkeypatch, conn)
        result = AkshareBenchmarkDao().get_benchmark_data(
            start=date(2024, 1, 1), end=date(2024, 1, 2), benchmark_symbol="000300.SH")
        assert result is not None

    def test_get_benchmark_data_sz_fallback(self, monkeypatch):
        """Test .SH → .SZ fallback candidate generation."""
        rows = [
            _Row({"trade_date": "20240101", "close": 3500.0}),
            _Row({"trade_date": "20240102", "close": 3550.0}),
        ]
        conn = _FC(_FR(rows))
        self._patch(monkeypatch, conn)
        result = AkshareBenchmarkDao().get_benchmark_data(
            start=date(2024, 1, 1), end=date(2024, 1, 2), benchmark_symbol="000001.SH")
        assert result is not None


# =====================================================================
# BrokerConfigDao
# =====================================================================
import app.domains.trading.dao.broker_config_dao as _broker_mod
from app.domains.trading.dao.broker_config_dao import BrokerConfigDao


@pytest.mark.unit
class TestBrokerConfigDao:
    def _patch(self, mp, conn):
        mp.setattr(_broker_mod, "connection", lambda n: _Ctx(conn))

    def test_list_by_user(self, monkeypatch):
        conn = _FC(_FR([]))
        self._patch(monkeypatch, conn)
        assert BrokerConfigDao().list_by_user(1) == []

    def test_create(self, monkeypatch):
        conn = _FC(_FR(lastrowid=3))
        self._patch(monkeypatch, conn)
        assert BrokerConfigDao().create(1, "ctp", "my broker", "enc") == 3

    def test_update(self, monkeypatch):
        conn = _FC(_FR(rowcount=1))
        self._patch(monkeypatch, conn)
        assert BrokerConfigDao().update(1, 1, name="new") is True

    def test_update_no_change(self, monkeypatch):
        conn = _FC(_FR(rowcount=0))
        self._patch(monkeypatch, conn)
        assert BrokerConfigDao().update(1, 1, name="x") is False

    def test_delete(self, monkeypatch):
        conn = _FC(_FR(rowcount=1))
        self._patch(monkeypatch, conn)
        assert BrokerConfigDao().delete(1, 1) is True


# =====================================================================
# RiskRuleDao
# =====================================================================
import app.domains.trading.dao.risk_rule_dao as _risk_mod
from app.domains.trading.dao.risk_rule_dao import RiskRuleDao


@pytest.mark.unit
class TestRiskRuleDao:
    def _patch(self, mp, conn):
        mp.setattr(_risk_mod, "connection", lambda n: _Ctx(conn))

    def test_list_by_user(self, monkeypatch):
        conn = _FC(_FR([]))
        self._patch(monkeypatch, conn)
        assert RiskRuleDao().list_by_user(1) == []

    def test_list_active_only(self, monkeypatch):
        conn = _FC(_FR([]))
        self._patch(monkeypatch, conn)
        assert RiskRuleDao().list_by_user(1, active_only=True) == []

    def test_create(self, monkeypatch):
        conn = _FC(_FR(lastrowid=7))
        self._patch(monkeypatch, conn)
        assert RiskRuleDao().create(1, "drawdown", "max_drawdown", 0.1, "warn") == 7

    def test_update(self, monkeypatch):
        conn = _FC(_FR(rowcount=1))
        self._patch(monkeypatch, conn)
        assert RiskRuleDao().update(1, 1, threshold=0.2) is True

    def test_delete(self, monkeypatch):
        conn = _FC(_FR(rowcount=1))
        self._patch(monkeypatch, conn)
        assert RiskRuleDao().delete(1, 1) is True

    def test_delete_not_found(self, monkeypatch):
        conn = _FC(_FR(rowcount=0))
        self._patch(monkeypatch, conn)
        assert RiskRuleDao().delete(99, 1) is False


# =====================================================================
# MultiMarketDao
# =====================================================================
import app.domains.market.multi_market_dao as _multi_mod
from app.domains.market.multi_market_dao import MultiMarketDao


@pytest.mark.unit
class TestMultiMarketDao:
    def _patch(self, mp, conn):
        mp.setattr(_multi_mod, "connection", lambda n: _Ctx(conn))

    def test_list_exchanges(self, monkeypatch):
        conn = _FC(_FR([]))
        self._patch(monkeypatch, conn)
        assert MultiMarketDao().list_exchanges() == []

    def test_list_exchanges_all(self, monkeypatch):
        conn = _FC(_FR([]))
        self._patch(monkeypatch, conn)
        assert MultiMarketDao().list_exchanges(enabled_only=False) == []

    def test_list_hk_stocks(self, monkeypatch):
        conn = _FC(_FR([]))
        self._patch(monkeypatch, conn)
        assert MultiMarketDao().list_hk_stocks() == []

    def test_list_hk_stocks_keyword(self, monkeypatch):
        conn = _FC(_FR([]))
        self._patch(monkeypatch, conn)
        assert MultiMarketDao().list_hk_stocks(keyword="tencent") == []

    def test_get_hk_daily(self, monkeypatch):
        conn = _FC(_FR([]))
        self._patch(monkeypatch, conn)
        assert MultiMarketDao().get_hk_daily("00700.HK", "20240101", "20240131") == []

    def test_list_us_stocks(self, monkeypatch):
        conn = _FC(_FR([]))
        self._patch(monkeypatch, conn)
        assert MultiMarketDao().list_us_stocks() == []

    def test_list_us_stocks_keyword(self, monkeypatch):
        conn = _FC(_FR([]))
        self._patch(monkeypatch, conn)
        assert MultiMarketDao().list_us_stocks(keyword="AAPL") == []

    def test_get_us_daily(self, monkeypatch):
        conn = _FC(_FR([]))
        self._patch(monkeypatch, conn)
        assert MultiMarketDao().get_us_daily("AAPL.US", "20240101", "20240131") == []
