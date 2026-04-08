"""Coverage batch 3 for DAOs: tushare_dao, user_dao.

Targets ~147 miss across:
  - tushare_dao.py  121 miss
  - user_dao.py  26 miss
"""
from __future__ import annotations

import types
from datetime import date, datetime
from unittest.mock import MagicMock, patch, call

import pytest


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# Helpers
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

class _Cursor:
    def __init__(self):
        self.stmts: list[str] = []
        self._rows: list = []
        self.lastrowid = 1
        self.rowcount = 0
        self.description = None

    def execute(self, sql, params=None):
        if hasattr(sql, 'text'):
            self.stmts.append(str(sql))
        else:
            self.stmts.append(str(sql))
        self.rowcount = len(self._rows)
        return self  # for SQLAlchemy-like chaining

    def executemany(self, sql, seq):
        self.stmts.append(str(sql))
        self.rowcount = len(list(seq))

    def fetchone(self):
        return self._rows[0] if self._rows else None

    def fetchall(self):
        return self._rows

    def close(self):
        pass

    # For SQLAlchemy result proxy
    @property
    def inserted_primary_key(self):
        return [self.lastrowid]


class _FakeResult:
    """Mimic SQLAlchemy CursorResult."""
    def __init__(self, cursor):
        self._cursor = cursor
        self.lastrowid = cursor.lastrowid

    @property
    def rowcount(self):
        return self._cursor.rowcount

    def fetchone(self):
        return self._cursor.fetchone()

    def fetchall(self):
        return self._cursor.fetchall()

    def mappings(self):
        return self

    def all(self):
        return self._cursor.fetchall()

    def first(self):
        return self._cursor.fetchone()


class _FakeConn:
    """Mimic SQLAlchemy Connection with execute()."""
    def __init__(self, cursor):
        self._cursor = cursor

    def execute(self, stmt, params=None):
        self._cursor.execute(stmt, params)
        return _FakeResult(self._cursor)

    def commit(self):
        pass

    def close(self):
        pass

    def __enter__(self):
        return self

    def __exit__(self, *a):
        pass


class _FakeEngine:
    """Mimic SQLAlchemy Engine with begin() / connect()."""
    def __init__(self, cursor):
        self._cursor = cursor

    def begin(self):
        return _FakeConn(self._cursor)

    def connect(self):
        return _FakeConn(self._cursor)


def _mk_df(data: dict, *, nrows: int = 2):
    """Build a minimal pandas-like DataFrame mock."""
    import pandas as pd
    return pd.DataFrame(data)


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# tushare_dao.py
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

class TestTushareDao:

    @pytest.fixture(autouse=True)
    def _patch_conn(self, monkeypatch):
        self.cur = _Cursor()
        import app.domains.extdata.dao.tushare_dao as mod
        self.mod = mod
        monkeypatch.setattr(mod, "engine", _FakeEngine(self.cur))

    # ── audit_start / audit_finish ──────────────────────────────
    def test_audit_start(self):
        self.cur.lastrowid = 42
        r = self.mod.audit_start("stock_daily", {"ts_code": "000001.SZ"})
        assert r == 42

    def test_audit_finish_success(self):
        self.mod.audit_finish(42, "success", 100)
        assert any("UPDATE" in s for s in self.cur.stmts)

    def test_audit_finish_error(self):
        self.mod.audit_finish(42, "error", 0)

    # ── get_all_ts_codes ───────────────────────────────────────
    def test_get_all_ts_codes_empty(self):
        self.cur._rows = []
        r = self.mod.get_all_ts_codes()
        assert r == []

    def test_get_all_ts_codes_with_data(self):
        self.cur._rows = [("000001.SZ",), ("000002.SZ",)]
        r = self.mod.get_all_ts_codes()
        assert len(r) == 2

    # ── get_max_trade_date ─────────────────────────────────────
    def test_get_max_trade_date_none(self):
        self.cur._rows = [(None,)]
        r = self.mod.get_max_trade_date("000001.SZ")
        assert r is None

    def test_get_max_trade_date_found(self):
        self.cur._rows = [(date(2024, 6, 1),)]
        r = self.mod.get_max_trade_date("000001.SZ")
        assert r == date(2024, 6, 1)

    # ── upsert_daily ──────────────────────────────────────────
    def test_upsert_daily_empty(self):
        import pandas as pd
        df = pd.DataFrame()
        r = self.mod.upsert_daily(df)
        assert r == 0

    def test_upsert_daily_with_data(self):
        import pandas as pd
        df = pd.DataFrame({
            "ts_code": ["000001.SZ"],
            "trade_date": ["20240101"],
            "open": [10.0], "high": [11.0], "low": [9.0], "close": [10.5],
            "vol": [1000.0], "amount": [10000.0],
            "pre_close": [9.5], "change": [1.0], "pct_chg": [10.0],
        })
        self.cur.rowcount = 1
        r = self.mod.upsert_daily(df)
        assert r >= 0

    # ── upsert_index_daily_df ──────────────────────────────────
    def test_upsert_index_daily_df_empty(self):
        import pandas as pd
        df = pd.DataFrame()
        r = self.mod.upsert_index_daily_df(df)
        assert r == 0

    def test_upsert_index_daily_df_with_data(self):
        import pandas as pd
        df = pd.DataFrame({
            "ts_code": ["000300.SH"],
            "trade_date": ["20240101"],
            "open": [3000.0], "high": [3050.0], "low": [2990.0], "close": [3020.0],
            "vol": [1e6], "amount": [1e9],
            "pre_close": [2990.0], "change": [30.0], "pct_chg": [1.0],
        })
        self.cur.rowcount = 1
        r = self.mod.upsert_index_daily_df(df)
        assert r >= 0

    # ── upsert_dividend_df ─────────────────────────────────────
    def test_upsert_dividend_df_empty(self):
        import pandas as pd
        r = self.mod.upsert_dividend_df(pd.DataFrame())
        assert r == 0

    def test_upsert_dividend_df_with_data(self):
        import pandas as pd
        df = pd.DataFrame({
            "ts_code": ["000001.SZ"],
            "end_date": ["20231231"],
            "ann_date": ["20240301"],
            "div_proc": ["实施"],
            "stk_div": [0.0], "stk_bo_rate": [0.0], "stk_co_rate": [0.0],
            "cash_div": [0.5], "cash_div_tax": [0.45],
            "record_date": ["20240315"], "ex_date": ["20240316"],
            "pay_date": ["20240320"], "div_listdate": [None],
            "imp_ann_date": [None], "base_date": [None], "base_share": [None],
        })
        r = self.mod.upsert_dividend_df(df)
        assert r >= 0

    # ── upsert_financial_statement ─────────────────────────────
    def test_upsert_financial_income_empty(self):
        import pandas as pd
        r = self.mod.upsert_financial_statement(pd.DataFrame(), "income")
        assert r == 0

    def test_upsert_financial_income_with_data(self):
        import pandas as pd
        df = pd.DataFrame({
            "ts_code": ["000001.SZ"],
            "ann_date": ["20240301"],
            "f_ann_date": ["20240301"],
            "end_date": ["20231231"],
            "report_type": ["1"],
            "comp_type": ["1"],
        })
        r = self.mod.upsert_financial_statement(df, "income")
        assert r >= 0

    # ── upsert_daily_basic ─────────────────────────────────────
    def test_upsert_daily_basic_empty(self):
        import pandas as pd
        r = self.mod.upsert_daily_basic(pd.DataFrame())
        assert r == 0

    def test_upsert_daily_basic_with_data(self):
        import pandas as pd
        df = pd.DataFrame({
            "ts_code": ["000001.SZ"],
            "trade_date": ["20240101"],
            "turnover_rate": [1.5], "turnover_rate_f": [1.2],
            "volume_ratio": [0.8], "pe": [10.0], "pe_ttm": [9.5],
            "pb": [1.2], "ps": [2.0], "ps_ttm": [1.8],
            "dv_ratio": [3.0], "dv_ttm": [2.8],
            "total_share": [1e9], "float_share": [8e8],
            "free_share": [7e8], "total_mv": [1e10],
            "circ_mv": [8e9],
        })
        r = self.mod.upsert_daily_basic(df)
        assert r >= 0

    # ── upsert_adj_factor ──────────────────────────────────────
    def test_upsert_adj_factor_empty(self):
        import pandas as pd
        r = self.mod.upsert_adj_factor(pd.DataFrame())
        assert r == 0

    def test_upsert_adj_factor_with_data(self):
        import pandas as pd
        df = pd.DataFrame({
            "ts_code": ["000001.SZ"],
            "trade_date": ["20240101"],
            "adj_factor": [1.05],
        })
        r = self.mod.upsert_adj_factor(df)
        assert r >= 0

    # ── upsert_moneyflow ───────────────────────────────────────
    def test_upsert_moneyflow_empty(self):
        import pandas as pd
        r = self.mod.upsert_moneyflow(pd.DataFrame())
        assert r == 0

    def test_upsert_moneyflow_with_data(self):
        import pandas as pd
        cols = ["ts_code", "trade_date",
                "buy_sm_vol", "buy_sm_amount", "sell_sm_vol", "sell_sm_amount",
                "buy_md_vol", "buy_md_amount", "sell_md_vol", "sell_md_amount",
                "buy_lg_vol", "buy_lg_amount", "sell_lg_vol", "sell_lg_amount",
                "buy_elg_vol", "buy_elg_amount", "sell_elg_vol", "sell_elg_amount",
                "net_mf_vol", "net_mf_amount"]
        vals = ["000001.SZ", "20240101"] + [100.0] * 18
        df = pd.DataFrame([vals], columns=cols)
        r = self.mod.upsert_moneyflow(df)
        assert r >= 0

    # ── upsert_top10_holders ───────────────────────────────────
    def test_upsert_top10_holders_empty(self):
        import pandas as pd
        r = self.mod.upsert_top10_holders(pd.DataFrame())
        assert r == 0

    def test_upsert_top10_holders_with_data(self):
        import pandas as pd
        df = pd.DataFrame({
            "ts_code": ["000001.SZ"],
            "ann_date": ["20240301"],
            "end_date": ["20231231"],
            "holder_name": ["Test Fund"],
            "hold_amount": [1e6],
            "hold_ratio": [5.0],
        })
        r = self.mod.upsert_top10_holders(df)
        assert r >= 0

    # ── upsert_margin ──────────────────────────────────────────
    def test_upsert_margin_empty(self):
        import pandas as pd
        r = self.mod.upsert_margin(pd.DataFrame())
        assert r == 0

    def test_upsert_margin_with_data(self):
        import pandas as pd
        df = pd.DataFrame({
            "trade_date": ["20240101"],
            "exchange_id": ["SSE"],
            "rzye": [1e9], "rzmre": [1e8], "rzche": [5e7],
            "rqye": [2e8], "rqmcl": [1e7], "rzrqye": [1.2e9],
            "rqyl": [None],
        })
        r = self.mod.upsert_margin(df)
        assert r >= 0

    # ── upsert_block_trade ─────────────────────────────────────
    def test_upsert_block_trade_empty(self):
        import pandas as pd
        r = self.mod.upsert_block_trade(pd.DataFrame())
        assert r == 0

    def test_upsert_block_trade_with_data(self):
        import pandas as pd
        df = pd.DataFrame({
            "ts_code": ["000001.SZ"],
            "trade_date": ["20240101"],
            "price": [10.5], "vol": [50000.0], "amount": [525000.0],
            "buyer": ["Buyer A"], "seller": ["Seller B"],
        })
        r = self.mod.upsert_block_trade(df)
        assert r >= 0

    # ── upsert_stock_basic ─────────────────────────────────────
    def test_upsert_stock_basic_empty(self):
        import pandas as pd
        r = self.mod.upsert_stock_basic(pd.DataFrame())
        assert r == 0

    def test_upsert_stock_basic_with_data(self):
        import pandas as pd
        df = pd.DataFrame({
            "ts_code": ["000001.SZ"],
            "symbol": ["000001"],
            "name": ["平安银行"],
            "area": ["深圳"],
            "industry": ["银行"],
            "market": ["主板"],
            "list_date": ["19910403"],
            "list_status": ["L"],
            "exchange": ["SZSE"],
            "is_hs": ["H"],
        })
        r = self.mod.upsert_stock_basic(df)
        assert r >= 0

    # ── upsert_repo_df ─────────────────────────────────────────
    def test_upsert_repo_df_empty(self):
        import pandas as pd
        r = self.mod.upsert_repo_df(pd.DataFrame())
        assert r == 0

    def test_upsert_repo_df_with_data(self):
        import pandas as pd
        df = pd.DataFrame({
            "ts_code": ["204001.SH"],
            "trade_date": ["20240101"],
            "repo_maturity": ["1D"],
            "open": [1.5], "high": [2.0], "low": [1.0], "close": [1.8],
            "vol": [1e6], "amount": [1e8],
            "pre_close": [1.6],
        })
        r = self.mod.upsert_repo_df(df)
        assert r >= 0

    # ── fetch_stock_daily_rows ─────────────────────────────────
    def test_fetch_stock_daily_rows_empty(self):
        self.cur._rows = []
        r = self.mod.fetch_stock_daily_rows("000001.SZ")
        assert r == []

    def test_fetch_stock_daily_rows_with_data(self):
        self.cur._rows = [
            ("20240101", 10.0, 11.0, 9.0, 10.5, 1000, 10000),
        ]
        r = self.mod.fetch_stock_daily_rows("000001.SZ", start_date=date(2024, 1, 1))
        assert len(r) == 1

    # ── fetch_existing_keys ────────────────────────────────────
    def test_fetch_existing_keys(self):
        self.cur._rows = [("000001.SZ", "20240101"), ("000002.SZ", "20240101")]
        r = self.mod.fetch_existing_keys(
            "stock_daily", "ts_code", date(2024, 1, 1), date(2024, 1, 31))
        assert len(r) == 2

    # ── get_failed_ts_codes ────────────────────────────────────
    def test_get_failed_ts_codes(self):
        self.cur._rows = [("000001.SZ",), ("000002.SZ",)]
        r = self.mod.get_failed_ts_codes(10)
        assert len(r) == 2

    def test_get_failed_ts_codes_empty(self):
        self.cur._rows = []
        r = self.mod.get_failed_ts_codes(10)
        assert r == []


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# user_dao.py
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

class TestUserDao:

    @pytest.fixture(autouse=True)
    def _setup(self, monkeypatch):
        self.cur = _Cursor()
        import app.domains.auth.dao.user_dao as mod
        self.mod = mod
        monkeypatch.setattr(mod, "connection", lambda name: _FakeConn(self.cur))
        self.dao = mod.UserDao()

    def test_username_exists_true(self):
        self.cur._rows = [(1,)]
        r = self.dao.username_exists("admin")
        assert r is True

    def test_username_exists_false(self):
        self.cur._rows = []
        r = self.dao.username_exists("nope")
        assert r is False

    def test_email_exists_true(self):
        self.cur._rows = [(1,)]
        r = self.dao.email_exists("a@b.com")
        assert r is True

    def test_email_exists_false(self):
        self.cur._rows = []
        r = self.dao.email_exists("no@b.com")
        assert r is False

    def test_email_exists_empty(self):
        r = self.dao.email_exists("")
        assert r is False

    def test_insert_user(self):
        self.cur.lastrowid = 5
        r = self.dao.insert_user("u", "e@e.com", "hash", datetime.now())
        assert r == 5

    def test_get_user_for_login_found(self):
        from types import SimpleNamespace
        self.cur._rows = [SimpleNamespace(id=1, username="admin", email="a@q.com",
                           hashed_password="hash", is_active=True,
                           must_change_password=False, created_at=datetime.now())]
        r = self.dao.get_user_for_login("admin")
        assert r is not None

    def test_get_user_for_login_not_found(self):
        self.cur._rows = []
        r = self.dao.get_user_for_login("nobody")
        assert r is None

    def test_get_user_by_id_found(self):
        from types import SimpleNamespace
        self.cur._rows = [SimpleNamespace(id=1, username="admin", email="a@b.com",
                           hashed_password="hash", is_active=True,
                           must_change_password=False, created_at=datetime.now())]
        r = self.dao.get_user_by_id(1)
        assert r is not None

    def test_get_user_by_id_not_found(self):
        self.cur._rows = []
        r = self.dao.get_user_by_id(999)
        assert r is None

    def test_update_user_password(self):
        self.dao.update_user_password(1, "newhash")
        assert len(self.cur.stmts) > 0

    def test_update_user_password_must_change(self):
        self.dao.update_user_password(1, "newhash", must_change_password=True)
        assert len(self.cur.stmts) > 0

    def test_update_user_status(self):
        # Set rowcount AFTER execute will occur (via property on _FakeResult)
        original_execute = self.cur.execute
        def patched_execute(sql, params=None):
            original_execute(sql, params)
            self.cur.rowcount = 1
        self.cur.execute = patched_execute
        r = self.dao.update_user_status(1, False)
        assert r is True

    def test_update_user_status_not_found(self):
        # rowcount stays 0
        r = self.dao.update_user_status(999, True)
        assert r is False
