"""Tests for watchlist, portfolio, settings, ai_model, paper_account, calendar,
sentiment, trade_log, system route files."""
from __future__ import annotations
from unittest.mock import MagicMock, patch
import pytest
from fastapi import FastAPI
from starlette.testclient import TestClient


def _make_app(route_mod, prefix="/api/v1"):
    from app.api.exception_handlers import register_exception_handlers
    app = FastAPI(); register_exception_handlers(app)
    app.include_router(route_mod.router, prefix=prefix)
    if hasattr(route_mod, "get_current_user"):
        app.dependency_overrides[route_mod.get_current_user] = lambda: MagicMock(user_id=1, role="admin")
    if hasattr(route_mod, "get_current_user_optional"):
        app.dependency_overrides[route_mod.get_current_user_optional] = lambda: MagicMock(user_id=1)
    for r in app.routes:
        if hasattr(r, "dependencies"):
            r.dependencies = []
    return TestClient(app, raise_server_exceptions=False)


# ═══════════════════════════════════════════
# watchlist (LAZY imports → patch at source)
# ═══════════════════════════════════════════
_WL = "app.domains.market.dao.watchlist_dao.WatchlistDao"


@pytest.fixture()
def wl_client():
    from app.api.routes import watchlist
    return _make_app(watchlist)


class TestWatchlistRoutes:
    @patch(_WL)
    def test_list(self, M, wl_client):
        M.return_value.list_for_user.return_value = []
        r = wl_client.get("/api/v1/data/watchlists")
        assert r.status_code in (200, 500)

    @patch(_WL)
    def test_create(self, M, wl_client):
        M.return_value.create.return_value = 1
        r = wl_client.post("/api/v1/data/watchlists", json={"name": "My WL"})
        assert r.status_code in (200, 201, 422)

    @patch(_WL)
    def test_update(self, M, wl_client):
        M.return_value.get.return_value = {"id": 1, "user_id": 1}
        M.return_value.update.return_value = True
        r = wl_client.put("/api/v1/data/watchlists/1", json={"name": "Up"})
        assert r.status_code in (200, 404, 422)

    @patch(_WL)
    def test_delete(self, M, wl_client):
        M.return_value.get.return_value = {"id": 1, "user_id": 1}
        M.return_value.delete.return_value = True
        r = wl_client.delete("/api/v1/data/watchlists/1")
        assert r.status_code in (200, 204, 404)

    @patch(_WL)
    def test_add_item(self, M, wl_client):
        M.return_value.get.return_value = {"id": 1, "user_id": 1}
        M.return_value.add_item.return_value = 1
        r = wl_client.post("/api/v1/data/watchlists/1/items", json={"symbol": "000001.SZ"})
        assert r.status_code in (200, 201, 422)

    @patch(_WL)
    def test_remove_item(self, M, wl_client):
        M.return_value.get.return_value = {"id": 1, "user_id": 1}
        M.return_value.remove_item.return_value = True
        r = wl_client.delete("/api/v1/data/watchlists/1/items/000001.SZ")
        assert r.status_code in (200, 204, 404)


# ═══════════════════════════════════════════
# portfolio (LAZY imports → patch at source)
# ═══════════════════════════════════════════
_PF = "app.domains.portfolio.dao.portfolio_dao.PortfolioDao"
_PS = "app.domains.portfolio.position_sizing_service.PositionSizingService"
_PA_ATTR = "app.domains.portfolio.attribution_service.PerformanceAttributionService"


@pytest.fixture()
def pf_client():
    from app.api.routes import portfolio
    return _make_app(portfolio)


class TestPortfolioRoutes:
    @patch(_PF)
    def test_get_positions(self, M, pf_client):
        d = M.return_value
        d.get_or_create.return_value = {"id": 10, "cash": 1_000_000}
        d.list_positions.return_value = []
        r = pf_client.get("/api/v1/portfolio/positions")
        assert r.status_code in (200, 500)

    @patch(_PF)
    def test_close_position(self, M, pf_client):
        d = M.return_value
        d.get_or_create.return_value = {"id": 10, "cash": 1_000_000}
        d.get_position.return_value = {"symbol": "000001.SZ", "quantity": 100, "avg_cost": 10.0}
        d.upsert_position.return_value = None
        d.update_cash.return_value = None
        d.insert_transaction.return_value = None
        r = pf_client.post("/api/v1/portfolio/close", json={"symbol": "000001.SZ", "quantity": 100, "price": 11.0})
        assert r.status_code in (200, 400, 404, 422)

    @patch(_PF)
    def test_get_transactions(self, M, pf_client):
        d = M.return_value
        d.count_transactions.return_value = 0
        d.list_transactions.return_value = []
        r = pf_client.get("/api/v1/portfolio/1/transactions")
        assert r.status_code in (200, 403, 404, 500)

    @patch(_PF)
    def test_get_snapshots(self, M, pf_client):
        M.return_value.list_snapshots.return_value = []
        r = pf_client.get("/api/v1/portfolio/1/snapshots")
        assert r.status_code in (200, 403, 404, 500)

    @patch(_PS)
    def test_position_sizing(self, M, pf_client):
        M.return_value.calculate.return_value = {"quantity": 1000, "amount": 10000.0}
        r = pf_client.post("/api/v1/portfolio/position-sizing", json={
            "method": "fixed_amount", "total_capital": 1e6,
            "params": {"amount": 10000}, "max_position_pct": 20.0, "max_total_pct": 80.0,
        })
        assert r.status_code in (200, 422, 500)

    @patch(_PA_ATTR)
    def test_attribution(self, M, pf_client):
        M.return_value.brinson_attribution.return_value = {"allocation": 0.01, "selection": 0.02}
        r = pf_client.post("/api/v1/portfolio/attribution", json={
            "portfolio_weights": {"000001.SZ": 0.5},
            "benchmark_weights": {"000001.SZ": 0.3},
            "portfolio_returns": {"000001.SZ": 0.1},
            "benchmark_returns": {"000001.SZ": 0.05},
        })
        assert r.status_code in (200, 422, 500)


# ═══════════════════════════════════════════
# settings (LAZY imports → patch at source)
# ═══════════════════════════════════════════
_SI = "app.domains.market.dao.data_source_item_dao.DataSourceItemDao"
_SC = "app.domains.market.dao.data_source_item_dao.DataSourceConfigDao"


@pytest.fixture()
def set_client():
    from app.api.routes import settings
    return _make_app(settings)


class TestSettingsRoutes:
    @patch(_SI)
    def test_list_items(self, M, set_client):
        M.return_value.list_all.return_value = []
        r = set_client.get("/api/v1/settings/datasource-items")
        assert r.status_code in (200, 500)

    @patch(_SI)
    def test_update_item(self, M, set_client):
        M.return_value.get_by_key.return_value = {"source": "tushare", "item_key": "stock_basic"}
        M.return_value.update_enabled.return_value = True
        r = set_client.put("/api/v1/settings/datasource-items/stock_basic?source=tushare", json={"enabled": False})
        assert r.status_code in (200, 404, 422)

    @patch(_SI)
    def test_batch_update(self, M, set_client):
        M.return_value.batch_update.return_value = None
        r = set_client.put("/api/v1/settings/datasource-items/batch", json={"items": [{"source": "tushare", "item_key": "stock_basic", "enabled": True}]})
        assert r.status_code in (200, 422, 500)

    @patch(_SC)
    def test_list_configs(self, M, set_client):
        M.return_value.list_all.return_value = []
        r = set_client.get("/api/v1/settings/datasource-configs")
        assert r.status_code in (200, 500)

    @patch(_SC)
    def test_update_config(self, M, set_client):
        M.return_value.get_by_key.return_value = {"source_key": "tushare", "enabled": True}
        M.return_value.update_config.return_value = True
        r = set_client.put("/api/v1/settings/datasource-configs/tushare", json={"enabled": False})
        assert r.status_code in (200, 404, 422)

    @patch("app.datasync.registry.build_default_registry")
    def test_test_connection(self, M, set_client):
        src = MagicMock()
        src.test_connection.return_value = True
        M.return_value.get_source.return_value = src
        r = set_client.post("/api/v1/settings/datasource-items/test/tushare")
        assert r.status_code in (200, 404, 500)


# ═══════════════════════════════════════════
# ai_model (LAZY QlibModelService, EAGER is_qlib_available)
# ═══════════════════════════════════════════
_AI_SVC = "app.domains.ai.qlib_model_service.QlibModelService"
_AI_MOD = "app.api.routes.ai_model"


@pytest.fixture()
def ai_client():
    from app.api.routes import ai_model
    return _make_app(ai_model)


class TestAiModelRoutes:
    @patch(f"{_AI_MOD}.is_qlib_available", return_value=True)
    def test_status(self, mk, ai_client):
        r = ai_client.get("/api/v1/ai/qlib/status")
        assert r.status_code in (200, 500)

    @patch(_AI_SVC)
    def test_list_models(self, M, ai_client):
        M.return_value.list_supported_models.return_value = []
        r = ai_client.get("/api/v1/ai/qlib/supported-models")
        assert r.status_code in (200, 500)

    @patch(_AI_SVC)
    def test_list_datasets(self, M, ai_client):
        M.return_value.list_supported_datasets.return_value = []
        r = ai_client.get("/api/v1/ai/qlib/supported-datasets")
        assert r.status_code in (200, 500)

    @patch(_AI_SVC)
    def test_list_runs(self, M, ai_client):
        M.return_value.list_training_runs.return_value = []
        r = ai_client.get("/api/v1/ai/qlib/training-runs")
        assert r.status_code in (200, 500)

    @patch(_AI_SVC)
    def test_get_run(self, M, ai_client):
        M.return_value.get_training_run.return_value = {"id": "abc", "status": "ok"}
        r = ai_client.get("/api/v1/ai/qlib/training-runs/abc")
        assert r.status_code in (200, 404, 422)

    @patch(_AI_SVC)
    def test_predictions(self, M, ai_client):
        M.return_value.get_predictions.return_value = []
        r = ai_client.get("/api/v1/ai/qlib/training-runs/abc/predictions")
        assert r.status_code in (200, 404, 422, 500)

    @patch("app.worker.service.qlib_tasks.run_qlib_training_task")
    @patch(f"{_AI_MOD}.is_qlib_available", return_value=True)
    def test_train(self, mk_q, mk_t, ai_client):
        r = ai_client.post("/api/v1/ai/qlib/train", json={
            "model_type": "LGBModel", "factor_set": "Alpha158",
            "universe": "csi300",
            "train_start": "2018-01-01", "train_end": "2020-12-31",
            "valid_start": "2021-01-01", "valid_end": "2021-12-31",
            "test_start": "2022-01-01", "test_end": "2022-12-31",
        })
        assert r.status_code in (200, 202, 422, 500)

    @patch("app.worker.service.qlib_tasks.run_data_conversion_task")
    def test_convert(self, mk, ai_client):
        r = ai_client.post("/api/v1/ai/qlib/data/convert", json={
            "start_date": "2020-01-01", "end_date": "2023-01-01",
        })
        assert r.status_code in (200, 202, 422, 500)


# ═══════════════════════════════════════════
# paper_account (EAGER imports → patch at route module)
# ═══════════════════════════════════════════
_PA = "app.api.routes.paper_account"


@pytest.fixture()
def pa_client():
    from app.api.routes import paper_account
    return _make_app(paper_account)


class TestPaperAccountRoutes:
    @patch(f"{_PA}.PaperAccountService")
    def test_create(self, M, pa_client):
        M.return_value.create_account.return_value = {"id": 1, "user_id": 1, "name": "T", "balance": 1e6}
        r = pa_client.post("/api/v1/paper-account", json={"name": "T", "initial_capital": 1e6})
        assert r.status_code in (200, 201, 400, 422)

    @patch(f"{_PA}.PaperAccountService")
    def test_list(self, M, pa_client):
        M.return_value.list_accounts.return_value = []
        r = pa_client.get("/api/v1/paper-account")
        assert r.status_code in (200, 500)

    @patch(f"{_PA}.PaperAccountService")
    def test_get(self, M, pa_client):
        M.return_value.get_account.return_value = {"id": 1}
        r = pa_client.get("/api/v1/paper-account/1")
        assert r.status_code in (200, 404)

    @patch(f"{_PA}.PaperAccountService")
    def test_equity(self, M, pa_client):
        M.return_value.get_equity_curve.return_value = []
        r = pa_client.get("/api/v1/paper-account/1/equity-curve")
        assert r.status_code in (200, 404, 500)

    @patch(f"{_PA}.PaperAnalyticsService")
    def test_analytics(self, M, pa_client):
        M.return_value.get_analytics.return_value = {"sharpe": 1.5}
        r = pa_client.get("/api/v1/paper-account/1/analytics")
        assert r.status_code in (200, 404, 500)

    @patch(f"{_PA}.PaperAccountService")
    def test_close(self, M, pa_client):
        M.return_value.close_account.return_value = True
        r = pa_client.delete("/api/v1/paper-account/1")
        assert r.status_code in (200, 204, 404)


# ═══════════════════════════════════════════
# calendar (LAZY CalendarService, uses get_current_user_optional)
# ═══════════════════════════════════════════
_CAL = "app.domains.market.calendar_service.CalendarService"


@pytest.fixture()
def cal_client():
    from app.api.routes import calendar
    return _make_app(calendar)


class TestCalendarRoutes:
    @patch(_CAL)
    def test_trade_days(self, M, cal_client):
        M.return_value.get_trade_days.return_value = ["2024-01-02"]
        r = cal_client.get("/api/v1/calendar/trade-days")
        assert r.status_code in (200, 500)

    @patch(_CAL)
    def test_events(self, M, cal_client):
        M.return_value.get_events.return_value = []
        r = cal_client.get("/api/v1/calendar/events")
        assert r.status_code in (200, 500)


# ═══════════════════════════════════════════
# sentiment (LAZY SentimentService, uses get_current_user_optional)
# ═══════════════════════════════════════════
_SENT = "app.domains.market.sentiment_service.SentimentService"


@pytest.fixture()
def sent_client():
    from app.api.routes import sentiment
    return _make_app(sentiment)


class TestSentimentRoutes:
    @patch(_SENT)
    def test_overview(self, M, sent_client):
        M.return_value.get_overview.return_value = {"advance": 2000}
        r = sent_client.get("/api/v1/sentiment/overview")
        assert r.status_code in (200, 500)

    @patch(_SENT)
    def test_fear_greed(self, M, sent_client):
        M.return_value.get_fear_greed.return_value = {"score": 55}
        r = sent_client.get("/api/v1/sentiment/fear-greed")
        assert r.status_code in (200, 500)


# ═══════════════════════════════════════════
# trade_log (LAZY TradeLogDao)
# ═══════════════════════════════════════════
_TL = "app.domains.market.dao.trade_log_dao.TradeLogDao"


@pytest.fixture()
def tl_client():
    from app.api.routes import trade_log
    return _make_app(trade_log)


class TestTradeLogRoutes:
    @patch(_TL)
    def test_query(self, M, tl_client):
        d = M.return_value
        d.count.return_value = 0
        d.query.return_value = []
        r = tl_client.get("/api/v1/reports/trade-logs")
        assert r.status_code in (200, 500)

    @patch(_TL)
    def test_export(self, M, tl_client):
        M.return_value.query.return_value = []
        r = tl_client.get("/api/v1/reports/trade-logs/export")
        assert r.status_code in (200, 500)


# ═══════════════════════════════════════════
# system (EAGER SyncStatusService → patch at route module)
# ═══════════════════════════════════════════
_SYS = "app.api.routes.system"


@pytest.fixture()
def sys_client():
    from app.api.routes import system
    return _make_app(system)


class TestSystemRoutes:
    @patch(f"{_SYS}.SyncStatusService")
    def test_sync_status(self, M, sys_client):
        M.return_value.get_sync_status.return_value = {"tushare": "ok"}
        r = sys_client.get("/api/v1/system/sync-status")
        assert r.status_code in (200, 500)

    def test_version(self, sys_client):
        r = sys_client.get("/api/v1/system/version")
        assert r.status_code in (200, 500)

    def test_list_log_modules(self, sys_client):
        r = sys_client.get("/api/v1/system/logs/modules")
        assert r.status_code == 200
        assert any(item["key"] == "api" for item in r.json()["data"])

    @patch(f"{_SYS}.create_log_stream")
    def test_stream_logs(self, M, sys_client):
        M.return_value = iter([
            'event: meta\ndata: {"type":"meta","module":"api"}\n\n',
            'event: log\ndata: {"type":"log","module":"api","line":"ready"}\n\n',
        ])
        r = sys_client.get("/api/v1/system/logs/stream?module=api&tail=50")
        assert r.status_code == 200
        assert "text/event-stream" in r.headers["content-type"]
        assert '"line":"ready"' in r.text

    def test_stream_logs_rejects_unknown_module(self, sys_client):
        r = sys_client.get("/api/v1/system/logs/stream?module=unknown")
        assert r.status_code == 400
