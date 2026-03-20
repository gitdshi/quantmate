"""Tests for PaperTradingService — deploy, positions, performance."""
import json
import pytest
from unittest.mock import patch, MagicMock
from types import SimpleNamespace

from app.domains.trading.paper_trading_service import PaperTradingService


def _make_conn():
    """Create a mock connection context manager."""
    conn = MagicMock()
    ctx = MagicMock()
    ctx.__enter__ = MagicMock(return_value=conn)
    ctx.__exit__ = MagicMock(return_value=False)
    return ctx, conn


class TestDeploy:
    @patch("app.domains.trading.paper_trading_service.connection")
    def test_deploy_success(self, mock_connection):
        ctx, conn = _make_conn()
        mock_connection.return_value = ctx

        # Strategy exists
        conn.execute.return_value.fetchone.return_value = SimpleNamespace(id=1, name="DMA Strategy")
        insert_result = MagicMock()
        insert_result.lastrowid = 42
        conn.execute.return_value = insert_result
        # Override fetchone only for the first call
        conn.execute.side_effect = [
            MagicMock(fetchone=MagicMock(return_value=SimpleNamespace(id=1, name="DMA Strategy"))),
            insert_result,
        ]

        svc = PaperTradingService()
        result = svc.deploy(user_id=1, strategy_id=1, vt_symbol="IF2406.CFFEX", parameters={"fast": 10})

        assert result["success"] is True
        assert result["deployment_id"] == 42
        assert result["status"] == "running"

    @patch("app.domains.trading.paper_trading_service.connection")
    def test_deploy_strategy_not_found(self, mock_connection):
        ctx, conn = _make_conn()
        mock_connection.return_value = ctx
        conn.execute.return_value.fetchone.return_value = None

        svc = PaperTradingService()
        result = svc.deploy(user_id=1, strategy_id=999, vt_symbol="IF2406.CFFEX")

        assert result["success"] is False
        assert "not found" in result["error"].lower()


class TestListDeployments:
    @patch("app.domains.trading.paper_trading_service.connection")
    def test_list_deployments(self, mock_connection):
        ctx, conn = _make_conn()
        mock_connection.return_value = ctx
        conn.execute.return_value.fetchall.return_value = [
            SimpleNamespace(
                id=1, strategy_id=10, strategy_name="DMA",
                vt_symbol="IF2406.CFFEX", parameters='{"fast": 10}',
                status="running", started_at="2026-01-01 10:00:00", stopped_at=None,
            ),
        ]

        svc = PaperTradingService()
        result = svc.list_deployments(user_id=1)

        assert len(result) == 1
        assert result[0]["strategy_name"] == "DMA"
        assert result[0]["parameters"] == {"fast": 10}

    @patch("app.domains.trading.paper_trading_service.connection")
    def test_list_deployments_empty(self, mock_connection):
        ctx, conn = _make_conn()
        mock_connection.return_value = ctx
        conn.execute.return_value.fetchall.return_value = []

        svc = PaperTradingService()
        result = svc.list_deployments(user_id=1)

        assert result == []


class TestStopDeployment:
    @patch("app.domains.trading.paper_trading_service.connection")
    def test_stop_deployment_success(self, mock_connection):
        ctx, conn = _make_conn()
        mock_connection.return_value = ctx
        conn.execute.return_value.rowcount = 1

        svc = PaperTradingService()
        assert svc.stop_deployment(deployment_id=1, user_id=1) is True

    @patch("app.domains.trading.paper_trading_service.connection")
    def test_stop_deployment_not_found(self, mock_connection):
        ctx, conn = _make_conn()
        mock_connection.return_value = ctx
        conn.execute.return_value.rowcount = 0

        svc = PaperTradingService()
        assert svc.stop_deployment(deployment_id=999, user_id=1) is False


class TestGetPositions:
    @patch("app.domains.trading.paper_trading_service.connection")
    def test_get_positions(self, mock_connection):
        ctx, conn = _make_conn()
        mock_connection.return_value = ctx
        conn.execute.return_value.fetchall.return_value = [
            SimpleNamespace(symbol="000001.SZ", direction="buy", total_qty=200, avg_cost=10.5, total_fee=0.63),
        ]

        svc = PaperTradingService()
        positions = svc.get_positions(user_id=1)

        assert len(positions) == 1
        assert positions[0]["symbol"] == "000001.SZ"
        assert positions[0]["quantity"] == 200
        assert positions[0]["avg_cost"] == 10.5

    @patch("app.domains.trading.paper_trading_service.connection")
    def test_get_positions_empty(self, mock_connection):
        ctx, conn = _make_conn()
        mock_connection.return_value = ctx
        conn.execute.return_value.fetchall.return_value = []

        svc = PaperTradingService()
        positions = svc.get_positions(user_id=1)

        assert positions == []

    @patch("app.domains.trading.paper_trading_service.connection")
    def test_get_positions_zero_qty_excluded(self, mock_connection):
        ctx, conn = _make_conn()
        mock_connection.return_value = ctx
        conn.execute.return_value.fetchall.return_value = [
            SimpleNamespace(symbol="000001.SZ", direction="buy", total_qty=0, avg_cost=0, total_fee=0),
        ]

        svc = PaperTradingService()
        positions = svc.get_positions(user_id=1)

        assert positions == []


class TestGetPerformance:
    @patch("app.domains.trading.paper_trading_service.connection")
    def test_get_performance_no_trades(self, mock_connection):
        ctx, conn = _make_conn()
        mock_connection.return_value = ctx
        conn.execute.return_value.fetchall.return_value = []

        svc = PaperTradingService()
        perf = svc.get_performance(user_id=1)

        assert perf["total_pnl"] == 0.0
        assert perf["total_trades"] == 0
        assert perf["equity_curve"] == []

    @patch("app.domains.trading.paper_trading_service.connection")
    def test_get_performance_with_trades(self, mock_connection):
        ctx, conn = _make_conn()
        mock_connection.return_value = ctx
        conn.execute.return_value.fetchall.return_value = [
            SimpleNamespace(id=1, symbol="000001.SZ", direction="buy", filled_quantity=100, avg_fill_price=10.0, fee=0.3, created_at="2026-01-01 10:00:00"),
            SimpleNamespace(id=2, symbol="000001.SZ", direction="sell", filled_quantity=100, avg_fill_price=11.0, fee=0.33, created_at="2026-01-02 10:00:00"),
        ]

        svc = PaperTradingService()
        perf = svc.get_performance(user_id=1)

        assert perf["total_trades"] == 2
        # P&L = sell_value(1100) - buy_value(1000) - fees(0.63) = 99.37
        assert perf["total_pnl"] == 99.37
        assert len(perf["equity_curve"]) == 2
        assert perf["max_drawdown"] >= 0

    @patch("app.domains.trading.paper_trading_service.connection")
    def test_get_performance_equity_curve(self, mock_connection):
        ctx, conn = _make_conn()
        mock_connection.return_value = ctx
        conn.execute.return_value.fetchall.return_value = [
            SimpleNamespace(id=1, symbol="A", direction="buy", filled_quantity=10, avg_fill_price=100.0, fee=0.3, created_at="2026-01-01"),
            SimpleNamespace(id=2, symbol="A", direction="sell", filled_quantity=10, avg_fill_price=110.0, fee=0.33, created_at="2026-01-05"),
        ]

        svc = PaperTradingService()
        perf = svc.get_performance(user_id=1)

        # First trade (buy): cumulative = -(1000 + 0.3) = -1000.3
        assert perf["equity_curve"][0]["value"] == -1000.3
        # Second trade (sell): cumulative = -1000.3 + (1100 - 0.33) = 99.37
        assert perf["equity_curve"][1]["value"] == 99.37
