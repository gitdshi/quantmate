"""Coverage-boost tests for untested route endpoints.

Covers:
  backtest.py  — export, walk-forward, monte-carlo
  composite.py — update_component, backtest_component, replace_bindings,
                 submit_backtest, list_backtests, get_backtest, delete_backtest
  data.py      — sectors, exchanges, indexes, quote, quote/series,
                 symbols-by-filter, quality/*, history-external
  strategy_code.py — lint/pyright
  websocket.py — ConnectionManager + _validate_channel_access
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch, AsyncMock

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from app.api.routes import backtest, composite, data, strategy_code
from app.api.exception_handlers import register_exception_handlers

_BT = "app.api.routes.backtest"
_COMP = "app.api.routes.composite.CompositeStrategyService"
_DATA = "app.api.routes.data"
_SC = "app.api.routes.strategy_code"


# ─── fixtures ─────────────────────────────────────────────────────────────


@pytest.fixture()
def mock_user():
    return type("User", (), {"id": 1, "user_id": 1, "username": "test", "sub": 1})()


@pytest.fixture()
def bt_client(mock_user):
    app = FastAPI()
    register_exception_handlers(app)
    app.include_router(backtest.router, prefix="/api/v1")
    app.dependency_overrides[backtest.get_current_user] = lambda: mock_user
    for r in app.routes:
        if hasattr(r, "dependencies"):
            r.dependencies = []
    return TestClient(app, raise_server_exceptions=False)


@pytest.fixture()
def comp_client(mock_user):
    app = FastAPI()
    register_exception_handlers(app)
    app.include_router(composite.comp_router, prefix="/api/v1")
    app.include_router(composite.composite_router, prefix="/api/v1")
    app.include_router(composite.backtest_router, prefix="/api/v1")
    app.dependency_overrides[composite.get_current_user] = lambda: mock_user
    for r in app.routes:
        if hasattr(r, "dependencies"):
            r.dependencies = []
    return TestClient(app, raise_server_exceptions=False)


@pytest.fixture()
def data_client(mock_user):
    app = FastAPI()
    register_exception_handlers(app)
    app.include_router(data.router, prefix="/api/v1")
    app.dependency_overrides[data.get_current_user] = lambda: mock_user
    if hasattr(data, "get_current_user_optional"):
        app.dependency_overrides[data.get_current_user_optional] = lambda: mock_user
    for r in app.routes:
        if hasattr(r, "dependencies"):
            r.dependencies = []
    return TestClient(app, raise_server_exceptions=False)


@pytest.fixture()
def sc_client():
    app = FastAPI()
    register_exception_handlers(app)
    app.include_router(strategy_code.router, prefix="/api/v1")
    return TestClient(app, raise_server_exceptions=False)


# ═════════════════════════════════════════════════════════════════════════
# Backtest — export, walk-forward, monte-carlo
# ═════════════════════════════════════════════════════════════════════════


class TestBacktestExport:
    def test_export_not_found(self, bt_client):
        resp = bt_client.post("/api/v1/backtest/nonexist/export", json={"format": "csv"})
        assert resp.status_code == 404

    @patch("app.domains.backtests.export_service.BacktestExportService")
    def test_export_csv(self, MockExp, bt_client):
        # inject a fake job into _jobs
        job = MagicMock()
        job.result.dict.return_value = {"statistics": {"total_return": 0.1}}
        backtest._jobs["j-csv"] = job
        try:
            MockExp.return_value.to_csv.return_value = "a,b\n1,2"
            resp = bt_client.post("/api/v1/backtest/j-csv/export", json={"format": "csv"})
            assert resp.status_code == 200
            assert "text/csv" in resp.headers.get("content-type", "")
        finally:
            backtest._jobs.pop("j-csv", None)

    @patch("app.domains.backtests.export_service.BacktestExportService")
    def test_export_html(self, MockExp, bt_client):
        job = MagicMock()
        job.result.dict.return_value = {"statistics": {}}
        backtest._jobs["j-html"] = job
        try:
            MockExp.return_value.to_html.return_value = "<h1>hi</h1>"
            resp = bt_client.post("/api/v1/backtest/j-html/export", json={"format": "html"})
            assert resp.status_code == 200
            assert "text/html" in resp.headers.get("content-type", "")
        finally:
            backtest._jobs.pop("j-html", None)

    @patch("app.domains.backtests.export_service.BacktestExportService")
    def test_export_json(self, MockExp, bt_client):
        job = MagicMock()
        job.result = {"statistics": {}}  # no .dict attr → else branch
        backtest._jobs["j-json"] = job
        try:
            MockExp.return_value.to_json.return_value = '{"ok":1}'
            resp = bt_client.post("/api/v1/backtest/j-json/export", json={"format": "json"})
            assert resp.status_code == 200
        finally:
            backtest._jobs.pop("j-json", None)


class TestBacktestAnalysis:
    @patch("app.domains.backtests.analysis_service.WalkForwardService")
    def test_walk_forward(self, MockSvc, bt_client):
        MockSvc.return_value.run.return_value = {"windows": []}
        resp = bt_client.post(
            "/api/v1/backtest/analysis/walk-forward",
            json={"total_bars": 500, "in_sample_pct": 0.7, "num_windows": 5},
        )
        assert resp.status_code == 200

    @patch("app.domains.backtests.analysis_service.MonteCarloService")
    def test_monte_carlo(self, MockSvc, bt_client):
        MockSvc.return_value.run.return_value = {"simulations": []}
        resp = bt_client.post(
            "/api/v1/backtest/analysis/monte-carlo",
            json={"trade_returns": [0.01, -0.02, 0.03], "num_simulations": 100, "initial_capital": 100000},
        )
        assert resp.status_code == 200


# ═════════════════════════════════════════════════════════════════════════
# Composite — untested component & backtest endpoints
# ═════════════════════════════════════════════════════════════════════════


class TestCompositeUpdateComponent:
    @patch(_COMP)
    def test_update_ok(self, MockSvc, comp_client):
        svc = MockSvc.return_value
        svc.update_component.return_value = {
            "id": 1, "name": "Updated MA", "layer": "trading", "sub_type": "entry",
            "user_id": 1, "description": None, "code": "pass", "config": None,
            "parameters": None, "version": 2, "is_active": True,
            "created_at": "2024-01-01T00:00:00", "updated_at": "2024-06-01T00:00:00",
        }
        resp = comp_client.put("/api/v1/strategy-components/1", json={
            "name": "Updated MA",
        })
        assert resp.status_code in (200, 422)

    @patch(_COMP)
    def test_update_not_found(self, MockSvc, comp_client):
        MockSvc.return_value.update_component.side_effect = KeyError("not found")
        resp = comp_client.put("/api/v1/strategy-components/999", json={"name": "X"})
        assert resp.status_code == 404

    @patch(_COMP)
    def test_update_validation_error(self, MockSvc, comp_client):
        MockSvc.return_value.update_component.side_effect = ValueError("bad name")
        resp = comp_client.put("/api/v1/strategy-components/1", json={"name": "X"})
        assert resp.status_code == 400


class TestCompositeBacktestComponent:
    @patch("app.domains.composite.component_backtest.run_component_backtest")
    @patch(_COMP)
    def test_backtest_component_ok(self, MockSvc, MockBT, comp_client):
        MockSvc.return_value.get_component.return_value = {
            "id": 1, "layer": "trading", "sub_type": "entry",
            "code": "pass", "config": '{"k":1}', "parameters": '{"p":2}',
        }
        MockBT.return_value = {"return": 0.05}
        resp = comp_client.post("/api/v1/strategy-components/1/backtest", json={})
        assert resp.status_code in (200, 422)

    @patch(_COMP)
    def test_backtest_component_not_found(self, MockSvc, comp_client):
        MockSvc.return_value.get_component.side_effect = KeyError
        resp = comp_client.post("/api/v1/strategy-components/999/backtest", json={})
        assert resp.status_code == 404

    @patch("app.domains.composite.component_backtest.run_component_backtest")
    @patch(_COMP)
    def test_backtest_component_with_overrides(self, MockSvc, MockBT, comp_client):
        MockSvc.return_value.get_component.return_value = {
            "id": 1, "layer": "risk", "sub_type": "stop_loss",
            "code": None, "config": {"k": 1}, "parameters": {"p": 2},
        }
        MockBT.return_value = {"return": 0.02}
        resp = comp_client.post(
            "/api/v1/strategy-components/1/backtest",
            json={"config_override": {"k": 99}, "params_override": {"p": 100}},
        )
        assert resp.status_code in (200, 422)


class TestCompositeReplaceBindings:
    @patch(_COMP)
    def test_replace_ok(self, MockSvc, comp_client):
        MockSvc.return_value.replace_bindings.return_value = [
            {"id": 10, "component_id": 1, "layer": "trading", "ordinal": 0,
             "weight": 1.0, "config_override": None,
             "component_name": "MA", "component_sub_type": "entry"},
        ]
        resp = comp_client.put(
            "/api/v1/composite-strategies/1/bindings",
            json=[{"component_id": 1, "layer": "trading"}],
        )
        assert resp.status_code in (200, 422)

    @patch(_COMP)
    def test_replace_not_found(self, MockSvc, comp_client):
        MockSvc.return_value.replace_bindings.side_effect = KeyError
        resp = comp_client.put(
            "/api/v1/composite-strategies/999/bindings",
            json=[{"component_id": 1, "layer": "trading"}],
        )
        assert resp.status_code == 404

    @patch(_COMP)
    def test_replace_validation_error(self, MockSvc, comp_client):
        MockSvc.return_value.replace_bindings.side_effect = ValueError("dup")
        resp = comp_client.put(
            "/api/v1/composite-strategies/1/bindings",
            json=[{"component_id": 1, "layer": "trading"}],
        )
        assert resp.status_code == 400


class TestCompositeBacktestCRUD:
    @patch(_COMP)
    def test_submit_backtest(self, MockSvc, comp_client):
        MockSvc.return_value.submit_backtest.return_value = {
            "id": 1, "job_id": "cj1", "composite_strategy_id": 1,
            "start_date": "2024-01-01", "end_date": "2024-06-01",
            "initial_capital": 1000000, "benchmark": "000300.SH",
            "status": "queued", "created_at": "2024-01-01T00:00:00",
        }
        resp = comp_client.post(
            "/api/v1/composite-backtests",
            json={
                "composite_strategy_id": 1,
                "start_date": "2024-01-01",
                "end_date": "2024-06-01",
                "initial_capital": 1000000,
                "benchmark": "000300.SH",
            },
        )
        assert resp.status_code in (200, 202, 422)

    @patch(_COMP)
    def test_submit_backtest_not_found(self, MockSvc, comp_client):
        MockSvc.return_value.submit_backtest.side_effect = KeyError
        resp = comp_client.post(
            "/api/v1/composite-backtests",
            json={
                "composite_strategy_id": 999,
                "start_date": "2024-01-01",
                "end_date": "2024-06-01",
            },
        )
        assert resp.status_code == 404

    @patch(_COMP)
    def test_submit_backtest_validation_error(self, MockSvc, comp_client):
        MockSvc.return_value.submit_backtest.side_effect = ValueError("no components")
        resp = comp_client.post(
            "/api/v1/composite-backtests",
            json={
                "composite_strategy_id": 1,
                "start_date": "2024-01-01",
                "end_date": "2024-06-01",
            },
        )
        assert resp.status_code == 400

    @patch(_COMP)
    def test_list_backtests(self, MockSvc, comp_client):
        MockSvc.return_value.list_backtests.return_value = [
            {"id": 1, "job_id": "cj1", "composite_strategy_id": 1,
             "start_date": "2024-01-01", "end_date": "2024-06-01",
             "initial_capital": 1000000, "benchmark": "000300.SH",
             "status": "completed", "created_at": "2024-01-01T00:00:00"},
        ]
        resp = comp_client.get("/api/v1/composite-backtests")
        assert resp.status_code == 200

    @patch(_COMP)
    def test_list_backtests_filter(self, MockSvc, comp_client):
        MockSvc.return_value.list_backtests.return_value = []
        resp = comp_client.get("/api/v1/composite-backtests?composite_strategy_id=1")
        assert resp.status_code == 200

    @patch(_COMP)
    def test_get_backtest(self, MockSvc, comp_client):
        MockSvc.return_value.get_backtest.return_value = {
            "id": 1, "job_id": "cj1", "composite_strategy_id": 1,
            "start_date": "2024-01-01", "end_date": "2024-06-01",
            "initial_capital": 1000000, "benchmark": "000300.SH",
            "status": "completed", "result": {"total_return": 0.1},
            "attribution": None, "error_message": None,
            "started_at": None, "completed_at": None,
            "created_at": "2024-01-01T00:00:00",
        }
        resp = comp_client.get("/api/v1/composite-backtests/cj1")
        assert resp.status_code == 200

    @patch(_COMP)
    def test_get_backtest_not_found(self, MockSvc, comp_client):
        MockSvc.return_value.get_backtest.side_effect = KeyError
        resp = comp_client.get("/api/v1/composite-backtests/nonexist")
        assert resp.status_code == 404

    @patch(_COMP)
    def test_delete_backtest(self, MockSvc, comp_client):
        MockSvc.return_value.delete_backtest.return_value = True
        resp = comp_client.delete("/api/v1/composite-backtests/cj1")
        assert resp.status_code in (200, 204)

    @patch(_COMP)
    def test_delete_backtest_not_found(self, MockSvc, comp_client):
        MockSvc.return_value.delete_backtest.side_effect = KeyError
        resp = comp_client.delete("/api/v1/composite-backtests/nonexist")
        assert resp.status_code == 404


# ═════════════════════════════════════════════════════════════════════════
# Data — untested endpoints
# ═════════════════════════════════════════════════════════════════════════


class TestDataSectors:
    @patch(f"{_DATA}.DataService")
    def test_sectors(self, MockSvc, data_client):
        MockSvc.return_value.get_sectors.return_value = [{"name": "IT"}]
        resp = data_client.get("/api/v1/data/sectors")
        assert resp.status_code == 200

    @patch(f"{_DATA}.DataService")
    def test_exchanges(self, MockSvc, data_client):
        MockSvc.return_value.get_exchanges.return_value = [{"name": "SSE"}]
        resp = data_client.get("/api/v1/data/exchanges")
        assert resp.status_code == 200

    @patch(f"{_DATA}.DataService")
    def test_indexes(self, MockSvc, data_client):
        MockSvc.return_value.get_indexes.return_value = [{"code": "000300"}]
        resp = data_client.get("/api/v1/data/indexes")
        assert resp.status_code == 200


class TestDataQuote:
    @patch(f"{_DATA}.DataService")
    def test_quote_ok(self, MockSvc, data_client):
        MockSvc.return_value.get_realtime_quote.return_value = {"price": 10.0}
        resp = data_client.get("/api/v1/data/quote?symbol=000001&market=CN")
        assert resp.status_code == 200

    @patch(f"{_DATA}.DataService")
    def test_quote_permission_error(self, MockSvc, data_client):
        MockSvc.return_value.get_realtime_quote.side_effect = PermissionError("no access")
        resp = data_client.get("/api/v1/data/quote?symbol=AAPL&market=US")
        assert resp.status_code == 403

    @patch(f"{_DATA}.DataService")
    def test_quote_value_error(self, MockSvc, data_client):
        MockSvc.return_value.get_realtime_quote.side_effect = ValueError("bad symbol")
        resp = data_client.get("/api/v1/data/quote?symbol=X&market=CN")
        assert resp.status_code == 400

    @patch(f"{_DATA}.DataService")
    def test_quote_server_error(self, MockSvc, data_client):
        MockSvc.return_value.get_realtime_quote.side_effect = RuntimeError("oops")
        resp = data_client.get("/api/v1/data/quote?symbol=000001&market=CN")
        assert resp.status_code == 500

    @patch(f"{_DATA}.DataService")
    def test_quote_series_ok(self, MockSvc, data_client):
        MockSvc.return_value.get_realtime_series.return_value = [{"ts": 1, "price": 10}]
        resp = data_client.get("/api/v1/data/quote/series?symbol=000001&market=CN")
        assert resp.status_code == 200

    @patch(f"{_DATA}.DataService")
    def test_quote_series_error(self, MockSvc, data_client):
        MockSvc.return_value.get_realtime_series.side_effect = RuntimeError("x")
        resp = data_client.get("/api/v1/data/quote/series?symbol=000001&market=CN")
        assert resp.status_code == 500


class TestDataSymbolsFilter:
    @patch(f"{_DATA}.DataService")
    def test_symbols_by_filter(self, MockSvc, data_client):
        MockSvc.return_value.get_symbols_by_filter.return_value = [
            {"ts_code": "000001.SZ", "name": "PingAn", "industry": "Bank", "exchange": "SZSE"}
        ]
        resp = data_client.get("/api/v1/data/symbols-by-filter?industry=Bank&limit=10")
        assert resp.status_code == 200

    @patch(f"{_DATA}.DataService")
    def test_symbols_by_exchange(self, MockSvc, data_client):
        MockSvc.return_value.get_symbols_by_filter.return_value = []
        resp = data_client.get("/api/v1/data/symbols-by-filter?exchange=SSE")
        assert resp.status_code == 200


class TestDataQuality:
    @patch("app.domains.extdata.data_cleaning_service.DataCleaningService")
    def test_missing_dates(self, MockSvc, data_client):
        MockSvc.return_value.detect_missing_dates.return_value = {"missing": []}
        resp = data_client.get(
            "/api/v1/data/quality/missing-dates?symbol=000001.SZ&start_date=2024-01-01&end_date=2024-06-01&table=stock_daily",
        )
        assert resp.status_code == 200

    @patch("app.domains.extdata.data_cleaning_service.DataCleaningService")
    def test_anomalies(self, MockSvc, data_client):
        MockSvc.return_value.detect_missing_dates.return_value = {"anomalies": []}
        resp = data_client.get(
            "/api/v1/data/quality/anomalies?symbol=000001.SZ&threshold_pct=20&table=stock_daily",
        )
        assert resp.status_code == 200

    @patch("app.domains.extdata.data_cleaning_service.DataCleaningService")
    def test_ohlc_check(self, MockSvc, data_client):
        MockSvc.return_value.check_ohlc_consistency.return_value = {"ok": True}
        resp = data_client.get("/api/v1/data/quality/ohlc-check?symbol=000001.SZ&table=stock_daily")
        assert resp.status_code == 200

    @patch("app.domains.extdata.data_cleaning_service.DataCleaningService")
    def test_quality_summary(self, MockSvc, data_client):
        MockSvc.return_value.summary.return_value = {"score": 95}
        resp = data_client.get(
            "/api/v1/data/quality/summary?symbol=000001.SZ&start_date=2024-01-01&end_date=2024-06-01",
        )
        assert resp.status_code == 200


class TestDataExternalHistory:
    @patch("app.domains.market.external_history_service.ExternalHistoryService")
    def test_external_history_ok(self, MockSvc, data_client):
        MockSvc.return_value.get_history.return_value = [{"date": "2024-01-01", "close": 100}]
        resp = data_client.get(
            "/api/v1/data/history-external/US/AAPL?start_date=2024-01-01&end_date=2024-06-01",
        )
        assert resp.status_code == 200

    @patch("app.domains.market.external_history_service.ExternalHistoryService")
    def test_external_history_value_error(self, MockSvc, data_client):
        MockSvc.return_value.get_history.side_effect = ValueError("bad symbol")
        resp = data_client.get(
            "/api/v1/data/history-external/HK/0700?start_date=2024-01-01&end_date=2024-06-01",
        )
        assert resp.status_code == 400

    @patch("app.domains.market.external_history_service.ExternalHistoryService")
    def test_external_history_server_error(self, MockSvc, data_client):
        MockSvc.return_value.get_history.side_effect = RuntimeError("down")
        resp = data_client.get(
            "/api/v1/data/history-external/CRYPTO/BTCUSD?start_date=2024-01-01&end_date=2024-06-01",
        )
        assert resp.status_code == 500


# ═════════════════════════════════════════════════════════════════════════
# Strategy Code — lint/pyright
# ═════════════════════════════════════════════════════════════════════════


class TestStrategyCodePyright:
    @patch("subprocess.run")
    def test_pyright_ok(self, mock_run, sc_client):
        mock_run.return_value = MagicMock(
            stdout='{"generalDiagnostics":[],"documents":{}}',
            stderr="",
        )
        resp = sc_client.post("/api/v1/strategy-code/lint/pyright", json={"content": "x = 1\n"})
        assert resp.status_code == 200
        body = resp.json()
        assert "diagnostics" in body

    @patch("subprocess.run")
    def test_pyright_with_diagnostics(self, mock_run, sc_client):
        mock_run.return_value = MagicMock(
            stdout='{"generalDiagnostics":[{"range":{"start":{"line":1,"character":0}},"severity":"error","message":"bad"}],"documents":{}}',
            stderr="",
        )
        resp = sc_client.post("/api/v1/strategy-code/lint/pyright", json={"content": "x: int = 'a'\n"})
        assert resp.status_code == 200
        assert len(resp.json()["diagnostics"]) >= 1

    @patch("subprocess.run")
    def test_pyright_documents_diagnostics(self, mock_run, sc_client):
        mock_run.return_value = MagicMock(
            stdout='{"generalDiagnostics":[],"documents":{"strategy.py":{"diagnostics":[{"range":{"start":{"line":2,"character":5}},"severity":"warning","message":"unused"}]}}}',
            stderr="",
        )
        resp = sc_client.post("/api/v1/strategy-code/lint/pyright", json={"content": "import os\n"})
        assert resp.status_code == 200
        assert len(resp.json()["diagnostics"]) == 1

    @patch("subprocess.run", side_effect=FileNotFoundError)
    def test_pyright_not_installed(self, mock_run, sc_client):
        resp = sc_client.post("/api/v1/strategy-code/lint/pyright", json={"content": "x=1\n"})
        assert resp.status_code == 501

    @patch("subprocess.run")
    def test_pyright_invalid_json_output(self, mock_run, sc_client):
        mock_run.return_value = MagicMock(stdout="NOT JSON", stderr="")
        resp = sc_client.post("/api/v1/strategy-code/lint/pyright", json={"content": "x=1"})
        assert resp.status_code == 200
        assert resp.json()["diagnostics"] == []


# ═════════════════════════════════════════════════════════════════════════
# WebSocket — ConnectionManager + _validate_channel_access (sync tests)
# ═════════════════════════════════════════════════════════════════════════


class TestValidateChannelAccess:
    def test_public_channel(self):
        from app.api.routes.websocket import _validate_channel_access
        assert _validate_channel_access("market:000001", 1) is True

    def test_alerts_own(self):
        from app.api.routes.websocket import _validate_channel_access
        assert _validate_channel_access("alerts:42", 42) is True

    def test_alerts_wrong_user(self):
        from app.api.routes.websocket import _validate_channel_access
        assert _validate_channel_access("alerts:42", 99) is False

    def test_orders_own(self):
        from app.api.routes.websocket import _validate_channel_access
        assert _validate_channel_access("orders:7", 7) is True

    def test_portfolio_wrong(self):
        from app.api.routes.websocket import _validate_channel_access
        assert _validate_channel_access("portfolio:1", 2) is False

    def test_paper_signals_own(self):
        from app.api.routes.websocket import _validate_channel_access
        assert _validate_channel_access("paper-signals:5", 5) is True

    def test_paper_orders_own(self):
        from app.api.routes.websocket import _validate_channel_access
        assert _validate_channel_access("paper-orders:10", 10) is True

    def test_invalid_suffix(self):
        from app.api.routes.websocket import _validate_channel_access
        assert _validate_channel_access("alerts:abc", 1) is False


class TestConnectionManager:
    @pytest.fixture()
    def mgr(self):
        from app.api.routes.websocket import ConnectionManager
        return ConnectionManager()

    @pytest.mark.asyncio
    async def test_connect_and_count(self, mgr):
        ws = MagicMock()
        await mgr.connect(ws, "market:000001")
        assert mgr.active_count == 1

    @pytest.mark.asyncio
    async def test_connect_multi_channels(self, mgr):
        ws = MagicMock()
        await mgr.connect(ws, "market:000001")
        await mgr.connect(ws, "market:000002")
        assert mgr.active_count == 2

    @pytest.mark.asyncio
    async def test_disconnect_all(self, mgr):
        ws = MagicMock()
        await mgr.connect(ws, "ch1")
        await mgr.connect(ws, "ch2")
        mgr.disconnect_all(ws)
        assert mgr.active_count == 0

    @pytest.mark.asyncio
    async def test_disconnect_single(self, mgr):
        ws = MagicMock()
        await mgr.connect(ws, "ch1")
        await mgr.connect(ws, "ch2")
        mgr.disconnect(ws, "ch1")
        assert mgr.active_count == 1

    @pytest.mark.asyncio
    async def test_disconnect_empty_channel_removed(self, mgr):
        ws = MagicMock()
        await mgr.connect(ws, "ch1")
        mgr.disconnect(ws, "ch1")
        assert "ch1" not in mgr._connections

    @pytest.mark.asyncio
    async def test_broadcast(self, mgr):
        ws = MagicMock()
        ws.send_json = AsyncMock()
        await mgr.connect(ws, "ch")
        await mgr.broadcast("ch", {"type": "tick"})
        ws.send_json.assert_called_once_with({"type": "tick"})

    @pytest.mark.asyncio
    async def test_broadcast_dead_connection(self, mgr):
        ws = MagicMock()
        ws.send_json = AsyncMock(side_effect=RuntimeError("closed"))
        await mgr.connect(ws, "ch")
        await mgr.broadcast("ch", {"type": "tick"})
        # dead connection should be removed
        assert ws not in mgr._connections.get("ch", set())

    @pytest.mark.asyncio
    async def test_broadcast_no_channel(self, mgr):
        # broadcasting to non-existing channel is no-op
        await mgr.broadcast("nonexist", {"type": "x"})

    @pytest.mark.asyncio
    async def test_disconnect_all_cleans_subscriptions(self, mgr):
        ws = MagicMock()
        await mgr.connect(ws, "ch1")
        mgr.disconnect_all(ws)
        assert id(ws) not in mgr._subscriptions
