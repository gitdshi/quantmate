"""Unit tests for the paper runtime service and paper gateway skeleton."""

from __future__ import annotations

from app.domains.trading.paper_gateway import PaperGateway, PaperGatewayOrderRequest
from app.domains.trading.paper_runtime_service import PaperRuntimeMode, PaperRuntimeService


def test_paper_gateway_tracks_orders_and_ticks():
    gateway = PaperGateway(gateway_name="PAPER.7")

    state = gateway.submit_order(
        PaperGatewayOrderRequest(
            vt_symbol="000001.SZSE",
            direction="buy",
            order_type="limit",
            volume=100,
            price=10.5,
        )
    )
    gateway.publish_tick("000001.SZSE", {"last_price": 10.6})

    snapshot = gateway.snapshot()

    assert state.order_id == "PAPER.7.0"
    assert snapshot.gateway_name == "PAPER.7"
    assert snapshot.order_count == 1
    assert snapshot.tick_count == 1
    assert gateway.get_last_tick("000001.SZSE") == {"last_price": 10.6}


def test_runtime_service_starts_and_stops_legacy_bridge(monkeypatch):
    service = PaperRuntimeService()
    service._sessions.clear()
    service._gateways.clear()

    monkeypatch.setattr(
        service,
        "_start_legacy_executor",
        lambda *, session, execution_mode, gateway=None: {"success": True, "execution_mode": execution_mode},
    )
    monkeypatch.setattr(service, "_stop_legacy_executor", lambda deployment_id: True)

    start_result = service.start_deployment(
        deployment_id=11,
        paper_account_id=2,
        user_id=3,
        strategy_id=4,
        strategy_name="TripleMA",
        vt_symbol="000001.SZSE",
        parameters={"fast": 5},
        execution_mode="auto",
    )

    runtime = start_result["runtime"]

    assert start_result["success"] is True
    assert runtime["runtime_mode"] == PaperRuntimeMode.NATIVE_CTA_RUNTIME.value
    assert runtime["strategy_kind"] == "cta"
    assert runtime["capabilities"]["native_gateway_execution"] is True
    assert runtime["gateway_name"] == "PAPER.11"

    stop_result = service.stop_deployment(11)

    assert stop_result["success"] is True
    assert stop_result["runtime"]["status"] == "stopped"


def test_runtime_service_rejects_duplicate_active_session(monkeypatch):
    service = PaperRuntimeService()
    service._sessions.clear()
    service._gateways.clear()

    monkeypatch.setattr(
        service,
        "_start_legacy_executor",
        lambda *, session, execution_mode, gateway=None: {"success": True},
    )

    first = service.start_deployment(
        deployment_id=12,
        paper_account_id=2,
        user_id=3,
        strategy_id=4,
        strategy_name="TripleMA",
        vt_symbol="000001.SZSE",
        parameters={},
    )
    second = service.start_deployment(
        deployment_id=12,
        paper_account_id=2,
        user_id=3,
        strategy_id=4,
        strategy_name="TripleMA",
        vt_symbol="000001.SZSE",
        parameters={},
    )

    assert first["success"] is True
    assert second["success"] is False
    assert "already registered" in second["error"].lower()


def test_runtime_service_preview_runtime():
    service = PaperRuntimeService()
    runtime = service.preview_runtime(
        deployment_id=30,
        paper_account_id=2,
        user_id=3,
        strategy_id=4,
        strategy_name="TripleMA",
        vt_symbol="000001.SZSE",
        parameters={"fast": 5},
    )

    assert runtime["status"] == "pending"
    assert runtime["runtime_mode"] == "native_cta_runtime"
    assert runtime["strategy_kind"] == "cta"


def test_runtime_service_routes_portfolio_strategies(monkeypatch):
    from vnpy_portfoliostrategy import StrategyTemplate as PortfolioStrategyTemplate

    class FakePortfolioStrategy(PortfolioStrategyTemplate):
        pass

    service = PaperRuntimeService()
    service._sessions.clear()
    service._gateways.clear()

    class FakeSourceDao:
        def get_strategy_source_for_user(self, strategy_id, user_id):
            return "code", "FakePortfolioStrategy", None

    monkeypatch.setattr("app.domains.backtests.dao.strategy_source_dao.StrategySourceDao", FakeSourceDao)
    monkeypatch.setattr("app.api.services.strategy_service.compile_strategy", lambda code, cls: FakePortfolioStrategy)
    monkeypatch.setattr(
        service,
        "_start_portfolio_executor",
        lambda *, session, execution_mode, gateway=None: {"success": True, "execution_mode": execution_mode},
    )

    result = service.start_deployment(
        deployment_id=41,
        paper_account_id=2,
        user_id=3,
        strategy_id=4,
        strategy_name="SpreadPortfolio",
        vt_symbol="000001.SZSE,000002.SZSE",
        parameters={"window": 5},
    )

    runtime = result["runtime"]

    assert result["success"] is True
    assert runtime["strategy_kind"] == "portfolio"
    assert runtime["runtime_mode"] == PaperRuntimeMode.PORTFOLIO_STRATEGY_BRIDGE.value
    assert runtime["capabilities"]["portfolio_runtime"] is True


def test_runtime_service_routes_composite_deployments(monkeypatch):
    service = PaperRuntimeService()
    service._sessions.clear()
    service._gateways.clear()

    monkeypatch.setattr(
        service,
        "_start_composite_executor",
        lambda *, session, execution_mode, gateway=None: {"success": True, "execution_mode": execution_mode},
    )

    result = service.start_deployment(
        deployment_id=52,
        paper_account_id=2,
        user_id=3,
        strategy_id=None,
        composite_strategy_id=99,
        strategy_source_type="composite",
        strategy_name="Composite Alpha",
        vt_symbol="600519.SH,000858.SZ",
        parameters={},
        execution_mode="auto",
    )

    runtime = result["runtime"]

    assert result["success"] is True
    assert runtime["strategy_source_type"] == "composite"
    assert runtime["strategy_kind"] == "portfolio"
    assert runtime["runtime_mode"] == PaperRuntimeMode.COMPOSITE_STRATEGY_BRIDGE.value
    assert runtime["capabilities"]["composite_runtime"] is True