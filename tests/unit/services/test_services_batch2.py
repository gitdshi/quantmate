"""Service/domain coverage batch — external_history, factor_screening, paper_strategy_executor,
qlib_model_service, paper_trading_service, market_rules, component_backtest, qlib_tasks.

Targets ~550 miss across 8 modules.
"""
from __future__ import annotations

from datetime import date, datetime
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest
import pandas as pd


# ═══════════════════════════════════════════════════════════════════════
# external_history_service.py  (61 miss → ~0)
# ═══════════════════════════════════════════════════════════════════════

class TestExternalHistoryService:
    def _svc(self):
        with patch("app.domains.market.external_history_service.MultiMarketDao"):
            from app.domains.market.external_history_service import ExternalHistoryService
            return ExternalHistoryService()

    def test_hk_history(self):
        svc = self._svc()
        svc._dao.get_hk_daily.return_value = [
            {"trade_date": "20240101", "open": 1, "high": 2, "low": 0.5, "close": 1.5, "vol": 100}
        ]
        r = svc.get_history("HK", "00700", date(2024, 1, 1), date(2024, 6, 1))
        assert r["market"] == "HK"
        assert len(r["bars"]) == 1
        assert r["bars"][0]["datetime"] == "2024-01-01T00:00:00"

    def test_hk_with_dot_symbol(self):
        svc = self._svc()
        svc._dao.get_hk_daily.return_value = []
        r = svc.get_history("HK", "00700.HK", date(2024, 1, 1), date(2024, 6, 1))
        assert r["symbol"] == "00700.HK"

    def test_us_history(self):
        svc = self._svc()
        svc._dao.get_us_daily.return_value = [
            {"trade_date": "20240115", "open": 150, "high": 155, "low": 148, "close": 153, "volume": 5000}
        ]
        r = svc.get_history("US", "AAPL", date(2024, 1, 1), date(2024, 6, 1))
        assert r["market"] == "US"

    def test_crypto_history(self):
        svc = self._svc()
        mock_df = pd.DataFrame({
            "date": ["2024-01-01"],
            "open": [42000], "high": [43000], "low": [41000], "close": [42500],
            "volume": [1000],
        })
        with patch("app.domains.market.external_history_service.ak") as ak_mock:
            ak_mock.crypto_hist.return_value = mock_df
            r = svc.get_history("CRYPTO", "BTC", date(2024, 1, 1), date(2024, 6, 1))
        assert r["market"] == "CRYPTO"
        assert len(r["bars"]) == 1

    def test_crypto_no_akshare(self):
        svc = self._svc()
        with patch("app.domains.market.external_history_service.ak", None):
            with pytest.raises(RuntimeError, match="AkShare"):
                svc.get_history("CRYPTO", "BTC", date(2024, 1, 1), date(2024, 6, 1))

    def test_crypto_api_error(self):
        svc = self._svc()
        with patch("app.domains.market.external_history_service.ak") as ak_mock:
            ak_mock.crypto_hist.side_effect = Exception("API down")
            with pytest.raises(ValueError, match="Cannot fetch crypto"):
                svc.get_history("CRYPTO", "BTC", date(2024, 1, 1), date(2024, 6, 1))

    def test_futures_history(self):
        svc = self._svc()
        mock_df = pd.DataFrame({
            "date": ["2024-01-15"],
            "open": [5000], "high": [5100], "low": [4900], "close": [5050],
            "volume": [2000],
        })
        with patch("app.domains.market.external_history_service.ak") as ak_mock:
            ak_mock.futures_zh_daily_sina.return_value = mock_df
            r = svc.get_history("FUTURES", "RB2401", date(2024, 1, 1), date(2024, 6, 1))
        assert r["market"] == "FUTURES"

    def test_futures_no_akshare(self):
        svc = self._svc()
        with patch("app.domains.market.external_history_service.ak", None):
            with pytest.raises(RuntimeError):
                svc.get_history("FUT", "RB", date(2024, 1, 1), date(2024, 6, 1))

    def test_futures_api_error(self):
        svc = self._svc()
        with patch("app.domains.market.external_history_service.ak") as ak_mock:
            ak_mock.futures_zh_daily_sina.side_effect = Exception("timeout")
            with pytest.raises(ValueError, match="Cannot fetch futures"):
                svc.get_history("FUTURES", "RB", date(2024, 1, 1), date(2024, 6, 1))

    def test_unsupported_market(self):
        svc = self._svc()
        with pytest.raises(ValueError, match="Unsupported market"):
            svc.get_history("FOREX", "EURUSD", date(2024, 1, 1), date(2024, 6, 1))

    def test_tushare_row_to_bar_short_date(self):
        from app.domains.market.external_history_service import ExternalHistoryService
        bar = ExternalHistoryService._tushare_row_to_bar({"trade_date": "2024", "close": 10})
        assert bar["datetime"] == "2024"

    def test_df_to_bars_empty_cols(self):
        from app.domains.market.external_history_service import ExternalHistoryService
        df = pd.DataFrame({"x": [1]})
        bars = ExternalHistoryService._df_to_bars(df)
        assert len(bars) == 1
        assert bars[0]["datetime"] == ""


# ═══════════════════════════════════════════════════════════════════════
# factor_screening.py  (59 miss → ~0)
# ═══════════════════════════════════════════════════════════════════════

class TestFactorScreening:
    @patch("app.domains.factors.factor_screening.compute_factor_metrics")
    @patch("app.domains.factors.factor_screening.compute_custom_factor")
    @patch("app.domains.factors.factor_screening.compute_forward_returns")
    @patch("app.domains.factors.factor_screening.fetch_ohlcv")
    def test_screen_empty_ohlcv(self, fetch, fwd, ccf, cfm):
        from app.domains.factors.factor_screening import screen_factor_pool
        fetch.return_value = pd.DataFrame()
        assert screen_factor_pool(["close"], date(2024, 1, 1), date(2024, 6, 1)) == []

    @patch("app.domains.factors.factor_screening.compute_factor_metrics")
    @patch("app.domains.factors.factor_screening.compute_custom_factor")
    @patch("app.domains.factors.factor_screening.compute_forward_returns")
    @patch("app.domains.factors.factor_screening.fetch_ohlcv")
    def test_screen_below_ic_threshold(self, fetch, fwd, ccf, cfm):
        from app.domains.factors.factor_screening import screen_factor_pool
        fetch.return_value = pd.DataFrame({"close": [1, 2, 3]})
        fwd.return_value = pd.Series([0.01, 0.02, 0.03])
        ccf.return_value = pd.Series([0.5, 0.6, 0.7])
        cfm.return_value = {"ic_mean": 0.001, "ic_std": 0.01, "ic_ir": 0.1}
        assert screen_factor_pool(["close"], date(2024, 1, 1), date(2024, 6, 1)) == []

    @patch("app.domains.factors.factor_screening.compute_factor_metrics")
    @patch("app.domains.factors.factor_screening.compute_custom_factor")
    @patch("app.domains.factors.factor_screening.compute_forward_returns")
    @patch("app.domains.factors.factor_screening.fetch_ohlcv")
    def test_screen_successful(self, fetch, fwd, ccf, cfm):
        from app.domains.factors.factor_screening import screen_factor_pool
        fetch.return_value = pd.DataFrame({"close": range(100)})
        fwd.return_value = pd.Series(range(100))
        ccf.return_value = pd.Series(range(100))
        cfm.return_value = {"ic_mean": 0.05, "ic_std": 0.01, "ic_ir": 5.0}
        r = screen_factor_pool(["close"], date(2024, 1, 1), date(2024, 6, 1))
        assert len(r) == 1
        assert r[0]["ic_mean"] == 0.05

    @patch("app.domains.factors.factor_screening.compute_factor_metrics")
    @patch("app.domains.factors.factor_screening.compute_custom_factor")
    @patch("app.domains.factors.factor_screening.compute_forward_returns")
    @patch("app.domains.factors.factor_screening.fetch_ohlcv")
    def test_screen_dedup_by_correlation(self, fetch, fwd, ccf, cfm):
        from app.domains.factors.factor_screening import screen_factor_pool
        data = list(range(100))
        fetch.return_value = pd.DataFrame({"close": data})
        fwd.return_value = pd.Series(data)
        # Two identical series → highly correlated → dedup
        ccf.return_value = pd.Series(data)
        cfm.side_effect = [
            {"ic_mean": 0.05, "ic_std": 0.01, "ic_ir": 5.0},
            {"ic_mean": 0.04, "ic_std": 0.01, "ic_ir": 4.0},
        ]
        r = screen_factor_pool(["expr1", "expr2"], date(2024, 1, 1), date(2024, 6, 1))
        assert len(r) == 1  # second should be deduped

    @patch("app.domains.factors.factor_screening.compute_factor_metrics")
    @patch("app.domains.factors.factor_screening.compute_custom_factor")
    @patch("app.domains.factors.factor_screening.compute_forward_returns")
    @patch("app.domains.factors.factor_screening.fetch_ohlcv")
    def test_screen_expression_error(self, fetch, fwd, ccf, cfm):
        from app.domains.factors.factor_screening import screen_factor_pool
        fetch.return_value = pd.DataFrame({"close": [1]})
        fwd.return_value = pd.Series([0.01])
        ccf.side_effect = RuntimeError("bad expression")
        assert screen_factor_pool(["bad"], date(2024, 1, 1), date(2024, 6, 1)) == []

    @patch("app.domains.factors.factor_screening.compute_qlib_factor_set")
    def test_mine_alpha158_runtime_error(self, cqfs):
        from app.domains.factors.factor_screening import mine_alpha158_factors
        cqfs.side_effect = RuntimeError("qlib not available")
        assert mine_alpha158_factors() == []

    @patch("app.domains.factors.factor_screening.compute_qlib_factor_set")
    def test_mine_alpha158_empty_df(self, cqfs):
        from app.domains.factors.factor_screening import mine_alpha158_factors
        cqfs.return_value = pd.DataFrame()
        assert mine_alpha158_factors() == []

    @patch("app.domains.factors.factor_screening.compute_factor_metrics")
    @patch("app.domains.factors.factor_screening.compute_qlib_factor_set")
    def test_mine_alpha158_no_close_col(self, cqfs, cfm):
        from app.domains.factors.factor_screening import mine_alpha158_factors
        cqfs.return_value = pd.DataFrame({"factor1": [1, 2, 3]})
        assert mine_alpha158_factors() == []

    @patch("app.domains.factors.factor_screening.compute_factor_metrics")
    @patch("app.domains.factors.factor_screening.compute_qlib_factor_set")
    def test_mine_alpha158_success(self, cqfs, cfm):
        from app.domains.factors.factor_screening import mine_alpha158_factors
        idx = pd.MultiIndex.from_tuples([("SH600000", "2024-01-01"), ("SH600000", "2024-01-02"),
                                          ("SH600000", "2024-01-03")])
        df = pd.DataFrame({"CLOSE": [10, 11, 12], "FACTOR1": [0.1, 0.2, 0.3]}, index=idx)
        cqfs.return_value = df
        cfm.return_value = {"ic_mean": 0.05, "ic_std": 0.01, "ic_ir": 5.0}
        r = mine_alpha158_factors(top_n=1)
        assert len(r) >= 1

    @patch("app.infrastructure.db.connections.connection")
    def test_save_screening_results(self, conn_mock):
        from app.domains.factors.factor_screening import save_screening_results
        ctx = MagicMock()
        ctx.__enter__ = MagicMock(return_value=ctx)
        ctx.__exit__ = MagicMock(return_value=False)
        result_mock = MagicMock()
        result_mock.lastrowid = 42
        ctx.execute.return_value = result_mock
        conn_mock.return_value = ctx
        run_id = save_screening_results(1, "test_run", [
            {"factor_name": "f1", "factor_set": "custom", "expression": "close",
             "ic_mean": 0.05, "ic_std": 0.01, "ic_ir": 5.0}
        ], config={"a": 1})
        assert run_id == 42


# ═══════════════════════════════════════════════════════════════════════
# market_rules.py  (39 miss → ~0)
# ═══════════════════════════════════════════════════════════════════════

class TestMarketRules:
    def test_cn_board_star(self):
        from app.domains.trading.market_rules import _cn_board
        assert _cn_board("688001.SH") == "star"
        assert _cn_board("689001.SH") == "star"

    def test_cn_board_gem(self):
        from app.domains.trading.market_rules import _cn_board
        assert _cn_board("300001.SZ") == "gem"
        assert _cn_board("301001.SZ") == "gem"

    def test_cn_board_main(self):
        from app.domains.trading.market_rules import _cn_board
        assert _cn_board("600000.SH") == "main"

    def test_is_st(self):
        from app.domains.trading.market_rules import _is_st
        assert _is_st("ST中天") is True
        assert _is_st("*ST退市") is True
        assert _is_st(None) is False
        assert _is_st("招商银行") is False

    def test_cn_price_limit_main(self):
        from app.domains.trading.market_rules import cn_price_limit_pct
        assert cn_price_limit_pct("600000.SH") == 0.10

    def test_cn_price_limit_star(self):
        from app.domains.trading.market_rules import cn_price_limit_pct
        assert cn_price_limit_pct("688001.SH") == 0.20

    def test_cn_price_limit_st(self):
        from app.domains.trading.market_rules import cn_price_limit_pct
        assert cn_price_limit_pct("600000.SH", "ST中天") == 0.05

    def test_cn_price_limits(self):
        from app.domains.trading.market_rules import cn_price_limits
        down, up = cn_price_limits(10.0, "600000.SH")
        assert down == 9.0
        assert up == 11.0

    def test_is_cn_trading_hours(self):
        from app.domains.trading.market_rules import is_cn_trading_hours
        morning = datetime(2024, 1, 15, 10, 0, 0)
        assert is_cn_trading_hours(morning) is True
        night = datetime(2024, 1, 15, 20, 0, 0)
        assert is_cn_trading_hours(night) is False
        afternoon = datetime(2024, 1, 15, 14, 0, 0)
        assert is_cn_trading_hours(afternoon) is True

    def test_is_hk_trading_hours(self):
        from app.domains.trading.market_rules import is_hk_trading_hours
        assert is_hk_trading_hours(datetime(2024, 1, 15, 10, 0)) is True
        assert is_hk_trading_hours(datetime(2024, 1, 15, 12, 30)) is False

    def test_is_us_trading_hours(self):
        from app.domains.trading.market_rules import is_us_trading_hours
        assert is_us_trading_hours(datetime(2024, 1, 15, 10, 0)) is True
        assert is_us_trading_hours(datetime(2024, 1, 15, 17, 0)) is False

    def test_validate_order_negative_qty(self):
        from app.domains.trading.market_rules import validate_order
        r = validate_order(market="CN", symbol="600000.SH", direction="buy",
                          quantity=0, price=10, order_type="limit")
        assert not r.valid

    def test_validate_order_bad_direction(self):
        from app.domains.trading.market_rules import validate_order
        r = validate_order(market="CN", symbol="600000.SH", direction="hold",
                          quantity=100, price=10, order_type="limit")
        assert not r.valid

    def test_validate_cn_buy_not_round_lot(self):
        from app.domains.trading.market_rules import validate_order
        r = validate_order(market="CN", symbol="600000.SH", direction="buy",
                          quantity=50, price=10, order_type="limit")
        assert not r.valid
        assert "100" in r.error

    def test_validate_cn_sell_not_round_lot(self):
        from app.domains.trading.market_rules import validate_order
        r = validate_order(market="CN", symbol="600000.SH", direction="sell",
                          quantity=50, price=10, order_type="limit")
        assert not r.valid

    def test_validate_cn_sell_star_odd_lot_ok(self):
        from app.domains.trading.market_rules import validate_order
        r = validate_order(market="CN", symbol="688001.SH", direction="sell",
                          quantity=50, price=10, order_type="limit")
        assert r.valid

    def test_validate_cn_t_plus_1(self):
        from app.domains.trading.market_rules import validate_order
        today = date(2024, 6, 15)
        r = validate_order(market="CN", symbol="600000.SH", direction="sell",
                          quantity=100, price=10, order_type="limit",
                          buy_date=today, today=today)
        assert not r.valid
        assert "T+1" in r.error

    def test_validate_cn_t_plus_1_ok(self):
        from app.domains.trading.market_rules import validate_order
        r = validate_order(market="CN", symbol="600000.SH", direction="sell",
                          quantity=100, price=10, order_type="limit",
                          buy_date=date(2024, 6, 14), today=date(2024, 6, 15))
        assert r.valid

    def test_validate_cn_insufficient_position(self):
        from app.domains.trading.market_rules import validate_order
        r = validate_order(market="CN", symbol="600000.SH", direction="sell",
                          quantity=200, price=10, order_type="limit",
                          available_position=100)
        assert not r.valid

    def test_validate_cn_price_exceeds_limit_up(self):
        from app.domains.trading.market_rules import validate_order
        r = validate_order(market="CN", symbol="600000.SH", direction="buy",
                          quantity=100, price=12.0, order_type="limit",
                          prev_close=10.0)
        assert not r.valid
        assert "涨停" in r.error

    def test_validate_cn_price_below_limit_down(self):
        from app.domains.trading.market_rules import validate_order
        r = validate_order(market="CN", symbol="600000.SH", direction="buy",
                          quantity=100, price=8.0, order_type="limit",
                          prev_close=10.0)
        assert not r.valid
        assert "跌停" in r.error

    def test_validate_cn_insufficient_funds(self):
        from app.domains.trading.market_rules import validate_order
        r = validate_order(market="CN", symbol="600000.SH", direction="buy",
                          quantity=100, price=10, order_type="limit",
                          available_balance=500)
        assert not r.valid
        assert "Insufficient funds" in r.error

    def test_validate_cn_valid(self):
        from app.domains.trading.market_rules import validate_order
        r = validate_order(market="CN", symbol="600000.SH", direction="buy",
                          quantity=100, price=10, order_type="limit",
                          prev_close=10, available_balance=100000)
        assert r.valid

    def test_validate_hk_valid(self):
        from app.domains.trading.market_rules import validate_order
        r = validate_order(market="HK", symbol="00700.HK", direction="buy",
                          quantity=100, price=350, order_type="limit")
        assert r.valid

    def test_validate_hk_insufficient_position(self):
        from app.domains.trading.market_rules import validate_order
        r = validate_order(market="HK", symbol="00700.HK", direction="sell",
                          quantity=200, price=350, order_type="limit",
                          available_position=100)
        assert not r.valid

    def test_validate_us_valid(self):
        from app.domains.trading.market_rules import validate_order
        r = validate_order(market="US", symbol="AAPL", direction="buy",
                          quantity=10, price=190, order_type="limit")
        assert r.valid

    def test_validate_us_insufficient_position(self):
        from app.domains.trading.market_rules import validate_order
        r = validate_order(market="US", symbol="AAPL", direction="sell",
                          quantity=50, price=190, order_type="limit",
                          available_position=10)
        assert not r.valid

    def test_validate_unsupported_market(self):
        from app.domains.trading.market_rules import validate_order
        with pytest.raises(ValueError):
            validate_order(market="FOREX", symbol="X", direction="buy",
                          quantity=1, price=1, order_type="limit")


# ═══════════════════════════════════════════════════════════════════════
# paper_trading_service.py  (30 miss → ~0)
# ═══════════════════════════════════════════════════════════════════════

class TestPaperTradingService:
    def _mock_conn(self):
        ctx = MagicMock()
        ctx.__enter__ = MagicMock(return_value=ctx)
        ctx.__exit__ = MagicMock(return_value=False)
        return ctx

    @patch("app.domains.trading.paper_trading_service.connection")
    def test_deploy_strategy_not_found(self, conn_mock):
        from app.domains.trading.paper_trading_service import PaperTradingService
        ctx = self._mock_conn()
        ctx.execute.return_value.fetchone.return_value = None
        conn_mock.return_value = ctx
        svc = PaperTradingService()
        r = svc.deploy(user_id=1, strategy_id=999, vt_symbol="000001.SZ")
        assert not r["success"]

    @patch("app.domains.trading.paper_trading_service.connection")
    def test_deploy_success(self, conn_mock):
        from app.domains.trading.paper_trading_service import PaperTradingService
        ctx = self._mock_conn()
        row = SimpleNamespace(id=1, name="MyStrat")
        ctx.execute.return_value.fetchone.return_value = row
        insert_result = MagicMock()
        insert_result.lastrowid = 42
        ctx.execute.side_effect = [MagicMock(fetchone=MagicMock(return_value=row)), insert_result]
        conn_mock.return_value = ctx
        svc = PaperTradingService()
        r = svc.deploy(user_id=1, strategy_id=1, vt_symbol="000001.SZ")
        assert r["success"]

    @patch("app.domains.trading.paper_trading_service.connection")
    def test_list_deployments(self, conn_mock):
        from app.domains.trading.paper_trading_service import PaperTradingService
        ctx = self._mock_conn()
        row = SimpleNamespace(id=1, strategy_id=1, strategy_name="S", vt_symbol="X",
                             parameters='{"a":1}', status="running",
                             started_at=datetime(2024, 1, 1), stopped_at=None)
        ctx.execute.return_value.fetchall.return_value = [row]
        conn_mock.return_value = ctx
        svc = PaperTradingService()
        r = svc.list_deployments(user_id=1)
        assert len(r) == 1
        assert r[0]["parameters"] == {"a": 1}

    @patch("app.domains.trading.paper_trading_service.connection")
    def test_stop_deployment_ok(self, conn_mock):
        from app.domains.trading.paper_trading_service import PaperTradingService
        ctx = self._mock_conn()
        ctx.execute.return_value.rowcount = 1
        conn_mock.return_value = ctx
        svc = PaperTradingService()
        assert svc.stop_deployment(1, 1) is True

    @patch("app.domains.trading.paper_trading_service.connection")
    def test_stop_deployment_not_found(self, conn_mock):
        from app.domains.trading.paper_trading_service import PaperTradingService
        ctx = self._mock_conn()
        ctx.execute.return_value.rowcount = 0
        conn_mock.return_value = ctx
        svc = PaperTradingService()
        assert svc.stop_deployment(999, 1) is False

    @patch("app.domains.trading.paper_trading_service.connection")
    def test_get_positions_empty(self, conn_mock):
        from app.domains.trading.paper_trading_service import PaperTradingService
        ctx = self._mock_conn()
        ctx.execute.return_value.fetchall.return_value = []
        conn_mock.return_value = ctx
        svc = PaperTradingService()
        assert svc.get_positions(1) == []

    @patch("app.domains.trading.paper_trading_service.connection")
    def test_get_positions_with_data(self, conn_mock):
        from app.domains.trading.paper_trading_service import PaperTradingService
        ctx = self._mock_conn()
        row = SimpleNamespace(symbol="000001", direction="buy", total_qty=200,
                             avg_cost=10.5, total_fee=5.0)
        ctx.execute.return_value.fetchall.return_value = [row]
        conn_mock.return_value = ctx
        svc = PaperTradingService()
        r = svc.get_positions(1)
        assert len(r) == 1
        assert r[0]["quantity"] == 200

    @patch("app.domains.trading.paper_trading_service.connection")
    def test_get_performance_no_orders(self, conn_mock):
        from app.domains.trading.paper_trading_service import PaperTradingService
        ctx = self._mock_conn()
        ctx.execute.return_value.fetchall.return_value = []
        conn_mock.return_value = ctx
        svc = PaperTradingService()
        r = svc.get_performance(1)
        assert r["total_trades"] == 0

    @patch("app.domains.trading.paper_trading_service.connection")
    def test_get_performance_with_trades(self, conn_mock):
        from app.domains.trading.paper_trading_service import PaperTradingService
        ctx = self._mock_conn()
        rows = [
            SimpleNamespace(id=1, symbol="X", direction="buy", filled_quantity=100,
                           avg_fill_price=10, fee=1, created_at=datetime(2024, 1, 1)),
            SimpleNamespace(id=2, symbol="X", direction="sell", filled_quantity=100,
                           avg_fill_price=12, fee=1, created_at=datetime(2024, 1, 2)),
        ]
        ctx.execute.return_value.fetchall.return_value = rows
        conn_mock.return_value = ctx
        svc = PaperTradingService()
        r = svc.get_performance(1)
        assert r["total_trades"] == 2
        assert r["total_pnl"] != 0


# ═══════════════════════════════════════════════════════════════════════
# component_backtest.py  (41 miss → ~0)
# ═══════════════════════════════════════════════════════════════════════

class TestComponentBacktest:
    def test_unknown_module(self):
        from app.domains.composite.component_backtest import run_component_backtest
        r = run_component_backtest("universe", "nonexistent_xyz", None, {}, {})
        assert "error" in r
        assert "not found" in r["error"]

    def test_unknown_layer(self):
        from app.domains.composite.component_backtest import run_component_backtest
        with patch("app.domains.composite.component_backtest.importlib") as imp:
            imp.import_module.return_value = MagicMock()
            r = run_component_backtest("unknown_layer", "test", None, {}, {})
        assert "error" in r
        assert "Unknown layer" in r["error"]

    def test_universe_backtest(self):
        from app.domains.composite.component_backtest import run_component_backtest
        mock_mod = MagicMock()
        mock_mod.select.return_value = ["SYM0001", "SYM0002"]
        with patch("app.domains.composite.component_backtest.importlib") as imp:
            imp.import_module.return_value = mock_mod
            r = run_component_backtest("universe", "test", None, {}, {})
        assert r["layer"] == "universe"
        assert r["selected_count"] == 2

    def test_trading_backtest(self):
        from app.domains.composite.component_backtest import run_component_backtest
        mock_mod = MagicMock()
        mock_mod.generate_signals.return_value = [
            {"symbol": "SYM0001", "direction": "long", "strength": 0.8},
            {"symbol": "SYM0002", "direction": "short", "strength": 0.5},
        ]
        with patch("app.domains.composite.component_backtest.importlib") as imp:
            imp.import_module.return_value = mock_mod
            r = run_component_backtest("trading", "test", None, {}, {})
        assert r["layer"] == "trading"
        assert r["long_count"] == 1
        assert r["short_count"] == 1

    def test_risk_backtest(self):
        from app.domains.composite.component_backtest import run_component_backtest
        mock_mod = MagicMock()
        mock_mod.filter_and_size.return_value = [
            {"symbol": "SYM0001", "volume": 100, "price": 10.0},
        ]
        with patch("app.domains.composite.component_backtest.importlib") as imp:
            imp.import_module.return_value = mock_mod
            r = run_component_backtest("risk", "test", None, {}, {})
        assert r["layer"] == "risk"
        assert r["output_orders"] == 1


# ═══════════════════════════════════════════════════════════════════════
# qlib_model_service.py  (55 miss → ~0)
# ═══════════════════════════════════════════════════════════════════════

class TestQlibModelService:
    def _mock_conn(self):
        ctx = MagicMock()
        ctx.__enter__ = MagicMock(return_value=ctx)
        ctx.__exit__ = MagicMock(return_value=False)
        return ctx

    @patch("app.domains.ai.qlib_model_service.connection")
    def test_train_unsupported_model(self, conn_mock):
        from app.domains.ai.qlib_model_service import QlibModelService
        svc = QlibModelService()
        with pytest.raises(ValueError, match="Unsupported model"):
            svc.train_model(user_id=1, model_type="NonExistent")

    @patch("app.domains.ai.qlib_model_service.connection")
    def test_train_unsupported_dataset(self, conn_mock):
        from app.domains.ai.qlib_model_service import QlibModelService
        svc = QlibModelService()
        with pytest.raises(ValueError, match="Unsupported factor"):
            svc.train_model(user_id=1, model_type="LightGBM", factor_set="NonExistent")

    @patch("app.domains.ai.qlib_model_service.connection")
    def test_get_predictions(self, conn_mock):
        from app.domains.ai.qlib_model_service import QlibModelService
        ctx = self._mock_conn()
        row = MagicMock()
        row._mapping = {"instrument": "SH600000", "trade_date": "2024-01-01", "score": 0.5, "rank_pct": 0.9}
        ctx.execute.return_value.fetchall.return_value = [row]
        conn_mock.return_value = ctx
        svc = QlibModelService()
        r = svc.get_predictions(1, trade_date="2024-01-01")
        assert len(r) == 1

    @patch("app.domains.ai.qlib_model_service.connection")
    def test_get_predictions_no_date(self, conn_mock):
        from app.domains.ai.qlib_model_service import QlibModelService
        ctx = self._mock_conn()
        ctx.execute.return_value.fetchall.return_value = []
        conn_mock.return_value = ctx
        svc = QlibModelService()
        r = svc.get_predictions(1)
        assert r == []

    @patch("app.domains.ai.qlib_model_service.connection")
    def test_list_training_runs(self, conn_mock):
        from app.domains.ai.qlib_model_service import QlibModelService
        ctx = self._mock_conn()
        row = MagicMock()
        row._mapping = {"id": 1, "status": "completed"}
        ctx.execute.return_value.fetchall.return_value = [row]
        conn_mock.return_value = ctx
        svc = QlibModelService()
        r = svc.list_training_runs(1, status="completed")
        assert len(r) == 1

    @patch("app.domains.ai.qlib_model_service.connection")
    def test_list_training_runs_no_status(self, conn_mock):
        from app.domains.ai.qlib_model_service import QlibModelService
        ctx = self._mock_conn()
        ctx.execute.return_value.fetchall.return_value = []
        conn_mock.return_value = ctx
        svc = QlibModelService()
        r = svc.list_training_runs(1)
        assert r == []

    @patch("app.domains.ai.qlib_model_service.connection")
    def test_get_training_run(self, conn_mock):
        from app.domains.ai.qlib_model_service import QlibModelService
        ctx = self._mock_conn()
        row = MagicMock()
        row._mapping = {"id": 1, "status": "completed"}
        ctx.execute.return_value.fetchone.return_value = row
        conn_mock.return_value = ctx
        svc = QlibModelService()
        r = svc.get_training_run(1)
        assert r["id"] == 1

    @patch("app.domains.ai.qlib_model_service.connection")
    def test_get_training_run_not_found(self, conn_mock):
        from app.domains.ai.qlib_model_service import QlibModelService
        ctx = self._mock_conn()
        ctx.execute.return_value.fetchone.return_value = None
        conn_mock.return_value = ctx
        svc = QlibModelService()
        assert svc.get_training_run(999) is None

    def test_list_supported_models(self):
        from app.domains.ai.qlib_model_service import QlibModelService
        svc = QlibModelService()
        r = svc.list_supported_models()
        assert len(r) > 0
        assert all("name" in item for item in r)

    def test_list_supported_datasets(self):
        from app.domains.ai.qlib_model_service import QlibModelService
        svc = QlibModelService()
        r = svc.list_supported_datasets()
        assert len(r) > 0

    @patch("app.domains.ai.qlib_model_service.connection")
    def test_create_training_run(self, conn_mock):
        from app.domains.ai.qlib_model_service import QlibModelService
        ctx = self._mock_conn()
        ctx.execute.return_value.lastrowid = 10
        conn_mock.return_value = ctx
        svc = QlibModelService()
        rid = svc._create_training_run(
            user_id=1, model_type="LightGBM", factor_set="Alpha158",
            universe="csi300", train_start="2020-01-01", train_end="2022-12-31",
            hyperparams={"num_leaves": 31})
        assert rid == 10

    @patch("app.domains.ai.qlib_model_service.connection")
    def test_update_training_status(self, conn_mock):
        from app.domains.ai.qlib_model_service import QlibModelService
        ctx = self._mock_conn()
        conn_mock.return_value = ctx
        svc = QlibModelService()
        svc._update_training_status(1, "running")
        ctx.execute.assert_called_once()

    @patch("app.domains.ai.qlib_model_service.connection")
    def test_complete_training_run(self, conn_mock):
        from app.domains.ai.qlib_model_service import QlibModelService
        ctx = self._mock_conn()
        conn_mock.return_value = ctx
        svc = QlibModelService()
        svc._complete_training_run(1, {"ic": 0.05})
        ctx.execute.assert_called_once()

    @patch("app.domains.ai.qlib_model_service.connection")
    def test_fail_training_run(self, conn_mock):
        from app.domains.ai.qlib_model_service import QlibModelService
        ctx = self._mock_conn()
        conn_mock.return_value = ctx
        svc = QlibModelService()
        svc._fail_training_run(1, "something broke")
        ctx.execute.assert_called_once()

    def test_save_predictions_none(self):
        from app.domains.ai.qlib_model_service import QlibModelService
        svc = QlibModelService()
        svc._save_predictions(1, None)  # should not raise

    def test_save_predictions_empty_df(self):
        from app.domains.ai.qlib_model_service import QlibModelService
        svc = QlibModelService()
        svc._save_predictions(1, pd.DataFrame())  # should not raise

    def test_calculate_metrics_none_test(self):
        from app.domains.ai.qlib_model_service import QlibModelService
        dataset = MagicMock()
        dataset.prepare.return_value = None
        r = QlibModelService._calculate_metrics(pd.Series([1, 2, 3]), dataset)
        assert r == {}


# ═══════════════════════════════════════════════════════════════════════
# qlib_tasks.py  (53 miss → ~0)
# ═══════════════════════════════════════════════════════════════════════

class TestQlibTasks:
    @patch("app.worker.service.qlib_tasks._get_qlib_model_service")
    def test_run_training_task_success(self, getter):
        from app.worker.service.qlib_tasks import run_qlib_training_task
        svc = MagicMock()
        svc.train_model.return_value = {"training_run_id": 1, "status": "completed"}
        getter.return_value = lambda: svc
        # _get_qlib_model_service returns a CLASS, then we call ()
        # Actually looking at code: service = _get_qlib_model_service()()
        # So getter returns a class, whose instance returns svc
        mock_cls = MagicMock(return_value=svc)
        getter.return_value = mock_cls
        r = run_qlib_training_task(user_id=1)
        assert r["status"] == "completed"

    @patch("app.worker.service.qlib_tasks._get_qlib_model_service")
    def test_run_training_task_failure(self, getter):
        from app.worker.service.qlib_tasks import run_qlib_training_task
        svc = MagicMock()
        svc.train_model.side_effect = RuntimeError("train failed")
        getter.return_value = MagicMock(return_value=svc)
        r = run_qlib_training_task(user_id=1)
        assert r["status"] == "failed"

    @patch("app.worker.service.qlib_tasks._get_data_converter")
    def test_run_data_conversion_success(self, getter):
        from app.worker.service.qlib_tasks import run_data_conversion_task
        converter = MagicMock(return_value={"status": "ok", "converted": 100})
        getter.return_value = converter
        r = run_data_conversion_task(start_date="2024-01-01", end_date="2024-06-01")
        assert r["status"] == "ok"

    @patch("app.worker.service.qlib_tasks._get_data_converter")
    def test_run_data_conversion_failure(self, getter):
        from app.worker.service.qlib_tasks import run_data_conversion_task
        getter.return_value = MagicMock(side_effect=RuntimeError("conv failed"))
        r = run_data_conversion_task()
        assert r["status"] == "failed"

    @patch("app.worker.service.qlib_tasks._get_data_converter")
    def test_run_data_conversion_no_dates(self, getter):
        from app.worker.service.qlib_tasks import run_data_conversion_task
        getter.return_value = MagicMock(return_value={"status": "ok"})
        r = run_data_conversion_task()
        assert r["status"] == "ok"

    @patch("app.infrastructure.db.connections.connection")
    def test_create_qlib_backtest_record(self, conn_mock):
        from app.worker.service.qlib_tasks import _create_qlib_backtest_record
        ctx = MagicMock()
        ctx.__enter__ = MagicMock(return_value=ctx)
        ctx.__exit__ = MagicMock(return_value=False)
        conn_mock.return_value = ctx
        _create_qlib_backtest_record(
            user_id=1, job_id="j1", start_date="2024-01-01", end_date="2024-06-01")

    @patch("app.infrastructure.db.connections.connection")
    def test_create_qlib_backtest_record_error(self, conn_mock):
        from app.worker.service.qlib_tasks import _create_qlib_backtest_record
        conn_mock.side_effect = RuntimeError("db error")
        _create_qlib_backtest_record(
            user_id=1, job_id="j1", start_date="2024-01-01", end_date="2024-06-01")

    @patch("app.infrastructure.db.connections.connection")
    def test_update_qlib_backtest_status(self, conn_mock):
        from app.worker.service.qlib_tasks import _update_qlib_backtest_status
        ctx = MagicMock()
        ctx.__enter__ = MagicMock(return_value=ctx)
        ctx.__exit__ = MagicMock(return_value=False)
        conn_mock.return_value = ctx
        _update_qlib_backtest_status("j1", "running")

    @patch("app.infrastructure.db.connections.connection")
    def test_update_qlib_backtest_status_with_error(self, conn_mock):
        from app.worker.service.qlib_tasks import _update_qlib_backtest_status
        ctx = MagicMock()
        ctx.__enter__ = MagicMock(return_value=ctx)
        ctx.__exit__ = MagicMock(return_value=False)
        conn_mock.return_value = ctx
        _update_qlib_backtest_status("j1", "failed", "something broke")

    @patch("app.infrastructure.db.connections.connection")
    def test_complete_qlib_backtest(self, conn_mock):
        from app.worker.service.qlib_tasks import _complete_qlib_backtest
        ctx = MagicMock()
        ctx.__enter__ = MagicMock(return_value=ctx)
        ctx.__exit__ = MagicMock(return_value=False)
        conn_mock.return_value = ctx
        _complete_qlib_backtest("j1", {"ic": 0.05}, {"analysis": "data"})

    @patch("app.infrastructure.db.connections.connection")
    def test_complete_qlib_backtest_error(self, conn_mock):
        from app.worker.service.qlib_tasks import _complete_qlib_backtest
        conn_mock.side_effect = RuntimeError("db error")
        _complete_qlib_backtest("j1", {}, None)

    def test_run_factor_evaluation_task_success(self):
        from app.worker.service.qlib_tasks import run_factor_evaluation_task
        with patch("app.domains.factors.service.FactorService") as FS:
            FS.return_value.run_evaluation.return_value = {"id": 1, "ic_mean": 0.05}
            r = run_factor_evaluation_task(1, 1, "2024-01-01", "2024-06-01")
        assert r["status"] == "completed"

    def test_run_factor_evaluation_task_failure(self):
        from app.worker.service.qlib_tasks import run_factor_evaluation_task
        with patch("app.domains.factors.service.FactorService") as FS:
            FS.return_value.run_evaluation.side_effect = RuntimeError("eval fail")
            r = run_factor_evaluation_task(1, 1, "2024-01-01", "2024-06-01")
        assert r["status"] == "failed"


# ═══════════════════════════════════════════════════════════════════════
# paper_strategy_executor.py  (58 miss → ~20)
# ═══════════════════════════════════════════════════════════════════════

class TestPaperStrategyExecutor:
    def test_paper_cta_engine_cancel(self):
        from app.domains.trading.paper_strategy_executor import _PaperCtaEngine
        engine = _PaperCtaEngine(MagicMock(), 1, 1, 1, "000001.SZ", "auto")
        engine.cancel_order(MagicMock(), "vt1")
        engine.cancel_all(MagicMock())
        engine.write_log("test")
        engine.put_event()
        engine.send_email("test")
        assert engine.get_pricetick("X") == 0.01

    def test_paper_cta_engine_semi_auto(self):
        from app.domains.trading.paper_strategy_executor import _PaperCtaEngine
        engine = _PaperCtaEngine(MagicMock(), 1, 1, 1, "000001.SZ", "semi_auto")
        with patch("app.domains.trading.paper_strategy_executor.connection") as conn_mock:
            ctx = MagicMock()
            ctx.__enter__ = MagicMock(return_value=ctx)
            ctx.__exit__ = MagicMock(return_value=False)
            conn_mock.return_value = ctx
            with patch.dict("sys.modules", {"vnpy.trader.constant": MagicMock()}):
                mock_dir = MagicMock()
                import sys
                sys.modules["vnpy.trader.constant"].Direction.LONG = mock_dir
                r = engine.send_order(MagicMock(strategy_name="test"), mock_dir, "open", 10.0, 100)
                assert r == []

    @patch("app.domains.trading.paper_strategy_executor.connection")
    def test_get_market(self, conn_mock):
        from app.domains.trading.paper_strategy_executor import _PaperCtaEngine
        ctx = MagicMock()
        ctx.__enter__ = MagicMock(return_value=ctx)
        ctx.__exit__ = MagicMock(return_value=False)
        ctx.execute.return_value.fetchone.return_value = SimpleNamespace(market="HK")
        conn_mock.return_value = ctx
        engine = _PaperCtaEngine(MagicMock(), 1, 1, 1, "00700.HK", "auto")
        assert engine._get_market() == "HK"

    @patch("app.domains.trading.paper_strategy_executor.connection")
    def test_get_market_none(self, conn_mock):
        from app.domains.trading.paper_strategy_executor import _PaperCtaEngine
        ctx = MagicMock()
        ctx.__enter__ = MagicMock(return_value=ctx)
        ctx.__exit__ = MagicMock(return_value=False)
        ctx.execute.return_value.fetchone.return_value = None
        conn_mock.return_value = ctx
        engine = _PaperCtaEngine(MagicMock(), 1, 1, 1, "000001.SZ", "auto")
        assert engine._get_market() == "CN"

    @patch("app.domains.trading.paper_strategy_executor.connection")
    def test_get_strategy_id(self, conn_mock):
        from app.domains.trading.paper_strategy_executor import _PaperCtaEngine
        ctx = MagicMock()
        ctx.__enter__ = MagicMock(return_value=ctx)
        ctx.__exit__ = MagicMock(return_value=False)
        ctx.execute.return_value.fetchone.return_value = SimpleNamespace(strategy_id=5)
        conn_mock.return_value = ctx
        engine = _PaperCtaEngine(MagicMock(), 1, 1, 1, "000001.SZ", "auto")
        assert engine._get_strategy_id() == 5

    def test_executor_singleton(self):
        from app.domains.trading.paper_strategy_executor import PaperStrategyExecutor
        # Reset singleton for testing
        PaperStrategyExecutor._instance = None
        e1 = PaperStrategyExecutor()
        e2 = PaperStrategyExecutor()
        assert e1 is e2
        PaperStrategyExecutor._instance = None  # cleanup

    def test_executor_stop_not_running(self):
        from app.domains.trading.paper_strategy_executor import PaperStrategyExecutor
        PaperStrategyExecutor._instance = None
        e = PaperStrategyExecutor()
        assert e.stop_deployment(9999) is False
        PaperStrategyExecutor._instance = None

    def test_executor_is_running_false(self):
        from app.domains.trading.paper_strategy_executor import PaperStrategyExecutor
        PaperStrategyExecutor._instance = None
        e = PaperStrategyExecutor()
        assert e.is_running(9999) is False
        PaperStrategyExecutor._instance = None

    def test_quote_to_bar_no_vnpy(self):
        from app.domains.trading.paper_strategy_executor import PaperStrategyExecutor
        with patch.dict("sys.modules", {"vnpy.trader.object": None, "vnpy.trader.constant": None}):
            r = PaperStrategyExecutor._quote_to_bar({"last_price": 10.0}, "000001.SSE")
            # ImportError path → returns None
            assert r is None

    def test_quote_to_bar_zero_price(self):
        from app.domains.trading.paper_strategy_executor import PaperStrategyExecutor
        with patch.dict("sys.modules", {
            "vnpy.trader.object": MagicMock(),
            "vnpy.trader.constant": MagicMock(),
        }):
            r = PaperStrategyExecutor._quote_to_bar({"last_price": 0}, "000001.SSE")
            assert r is None
