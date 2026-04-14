"""Route coverage boost batch 2 — backtest (bg tasks), paper_trading, strategies, websocket.

Covers ~250 miss across:
  - backtest.py run_backtest_task / run_batch_backtest_task / cancel / history
  - paper_trading.py deploy / orders / positions / signals
  - strategies.py CRUD / multi-factor / builtin / code-history
  - websocket.py ConnectionManager / validate_channel_access
"""
from __future__ import annotations

import json
from datetime import date, datetime
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch, PropertyMock

import pytest
from fastapi.testclient import TestClient

from app.api.models.user import TokenData


# ── helpers ──────────────────────────────────────────────────────────────

def _user():
    return TokenData(user_id=1, username="u", exp=datetime(2099, 1, 1))


def _client():
    from app.api.main import app
    from app.api.services.auth_service import get_current_user
    app.dependency_overrides[get_current_user] = lambda: _user()
    return TestClient(app, raise_server_exceptions=False)


# ═══════════════════════════════════════════════════════════════════════
# backtest routes — run_backtest_task / run_batch_backtest_task
# ═══════════════════════════════════════════════════════════════════════

class TestBacktestBgTasks:
    """Test background task functions in backtest.py."""

    @pytest.mark.asyncio
    async def test_run_backtest_task_success(self):
        from app.api.routes.backtest import run_backtest_task, _jobs, BacktestJob, BacktestStatus
        job_id = "bt-success"
        _jobs[job_id] = BacktestJob(
            job_id=job_id, status=BacktestStatus.PENDING, progress=0.0,
            message="q", created_at=datetime.utcnow(),
        )
        mock_result = MagicMock()
        mock_result.dict.return_value = {"statistics": {"total_return": 0.1}, "parameters": {}}
        mock_result.total_return = 0.1
        with patch("app.api.routes.backtest.BacktestService") as S, \
             patch("app.api.routes.backtest.save_backtest_to_db"), \
             patch("app.api.routes.backtest.get_job_storage") as gjs:
            S.return_value.run_single_backtest.return_value = mock_result
            gjs.return_value = MagicMock()
            await run_backtest_task(job_id, MagicMock(
                strategy_id=1, strategy_class="X", vt_symbol="000001.SZ",
                start_date="2024-01-01", end_date="2024-06-01",
                parameters={}, capital=1e6, rate=0.0003, slippage=0.2, size=300,
            ), user_id=1)
        assert _jobs[job_id].status == BacktestStatus.COMPLETED
        del _jobs[job_id]

    @pytest.mark.asyncio
    async def test_run_backtest_task_failure(self):
        from app.api.routes.backtest import run_backtest_task, _jobs, BacktestJob, BacktestStatus
        job_id = "bt-fail"
        _jobs[job_id] = BacktestJob(
            job_id=job_id, status=BacktestStatus.PENDING, progress=0.0,
            message="q", created_at=datetime.utcnow(),
        )
        with patch("app.api.routes.backtest.BacktestService") as S, \
             patch("app.api.routes.backtest.save_backtest_to_db"):
            S.return_value.run_single_backtest.side_effect = RuntimeError("boom")
            await run_backtest_task(job_id, MagicMock(
                strategy_id=1, strategy_class="X", vt_symbol="000001.SZ",
                start_date="2024-01-01", end_date="2024-06-01",
                parameters={}, capital=1e6, rate=0.0003, slippage=0.2, size=300,
            ), user_id=1)
        assert _jobs[job_id].status == BacktestStatus.FAILED
        del _jobs[job_id]

    @pytest.mark.asyncio
    async def test_run_batch_backtest_task_success(self):
        from app.api.routes.backtest import run_batch_backtest_task, _batch_jobs, BatchBacktestJob, BacktestStatus
        job_id = "batch-ok"
        _batch_jobs[job_id] = BatchBacktestJob(
            job_id=job_id, status=BacktestStatus.PENDING,
            total_symbols=2, completed_symbols=0, progress=0.0,
            created_at=datetime.utcnow(),
        )
        mock_r = MagicMock()
        mock_r.total_return = 0.05
        mock_r.dict.return_value = {"parameters": {}}
        with patch("app.api.routes.backtest.BacktestService") as S, \
             patch("app.api.routes.backtest.save_backtest_to_db"):
            S.return_value.run_single_backtest.return_value = mock_r
            req = MagicMock(
                strategy_id=1, strategy_class="X",
                symbols=["000001.SZ", "000002.SZ"],
                start_date="2024-01-01", end_date="2024-06-01",
                parameters={}, capital=1e6, rate=0.0003, slippage=0.2, size=300, top_n=5,
            )
            await run_batch_backtest_task(job_id, req, user_id=1)
        assert _batch_jobs[job_id].status == BacktestStatus.COMPLETED
        assert _batch_jobs[job_id].completed_symbols == 2
        del _batch_jobs[job_id]

    @pytest.mark.asyncio
    async def test_run_batch_cancelled_midway(self):
        from app.api.routes.backtest import run_batch_backtest_task, _batch_jobs, BatchBacktestJob, BacktestStatus
        job_id = "batch-cancel"
        job = BatchBacktestJob(
            job_id=job_id, status=BacktestStatus.PENDING,
            total_symbols=3, completed_symbols=0, progress=0.0,
            created_at=datetime.utcnow(),
        )
        _batch_jobs[job_id] = job

        call_count = 0
        def side_effect(**kw):
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                job.status = BacktestStatus.CANCELLED
            r = MagicMock()
            r.total_return = 0.01
            return r

        with patch("app.api.routes.backtest.BacktestService") as S:
            S.return_value.run_single_backtest.side_effect = side_effect
            req = MagicMock(
                strategy_id=1, strategy_class="X", symbols=["A", "B", "C"],
                start_date="2024-01-01", end_date="2024-06-01",
                parameters={}, capital=1e6, rate=0.0003, slippage=0.2, size=300, top_n=5,
            )
            await run_batch_backtest_task(job_id, req, 1)
        # Should have stopped after 1
        assert _batch_jobs[job_id].completed_symbols == 1
        del _batch_jobs[job_id]

    @pytest.mark.asyncio
    async def test_run_batch_exception(self):
        from app.api.routes.backtest import run_batch_backtest_task, _batch_jobs, BatchBacktestJob, BacktestStatus
        job_id = "batch-exc"
        _batch_jobs[job_id] = BatchBacktestJob(
            job_id=job_id, status=BacktestStatus.PENDING,
            total_symbols=1, completed_symbols=0, progress=0.0,
            created_at=datetime.utcnow(),
        )
        with patch("app.api.routes.backtest.BacktestService") as S:
            S.side_effect = RuntimeError("init fail")
            req = MagicMock(symbols=["A"], strategy_id=1, strategy_class="X",
                start_date="2024-01-01", end_date="2024-06-01",
                parameters={}, capital=1e6, rate=0.0003, slippage=0.2, size=300, top_n=5)
            await run_batch_backtest_task(job_id, req, 1)
        assert _batch_jobs[job_id].status == BacktestStatus.FAILED
        del _batch_jobs[job_id]


class TestBacktestCancelRoute:
    def test_cancel_pending(self):
        from app.api.routes.backtest import _jobs, BacktestJob, BacktestStatus
        _jobs["cancel-1"] = BacktestJob(job_id="cancel-1", status=BacktestStatus.PENDING,
            progress=0.0, message="q", created_at=datetime.utcnow())
        c = _client()
        r = c.delete("/api/v1/backtest/cancel-1")
        assert r.status_code == 200
        del _jobs["cancel-1"]

    def test_cancel_completed_fails(self):
        from app.api.routes.backtest import _jobs, BacktestJob, BacktestStatus
        _jobs["cancel-2"] = BacktestJob(job_id="cancel-2", status=BacktestStatus.COMPLETED,
            progress=100, message="done", created_at=datetime.utcnow())
        c = _client()
        r = c.delete("/api/v1/backtest/cancel-2")
        assert r.status_code == 400
        del _jobs["cancel-2"]

    def test_cancel_not_found(self):
        c = _client()
        r = c.delete("/api/v1/backtest/nonexistent-999")
        assert r.status_code == 404


class TestBacktestHistoryRoutes:
    @patch("app.api.routes.backtest.BacktestHistoryDao")
    def test_list_history(self, Dao):
        dao = Dao.return_value
        dao.count_for_user.return_value = 1
        dao.list_for_user.return_value = [{
            "id": 1, "job_id": "j1", "strategy_id": 1, "strategy_class": "X",
            "strategy_version": 1, "vt_symbol": "000001.SZ",
            "start_date": "2024-01-01", "end_date": "2024-06-01",
            "status": "completed", "result": json.dumps({"statistics": {"total_return": 0.1, "sharpe_ratio": 1.5}}),
            "created_at": datetime(2024, 1, 1), "completed_at": datetime(2024, 1, 2),
        }]
        c = _client()
        r = c.get("/api/v1/backtest/history/list")
        assert r.status_code == 200
        d = r.json()
        assert d.get("total", d.get("pagination", {}).get("total", 0)) >= 1 or "items" in d or "data" in d

    @patch("app.api.routes.backtest.BacktestHistoryDao")
    def test_history_detail(self, Dao):
        dao = Dao.return_value
        dao.get_detail_for_user.return_value = {
            "id": 1, "job_id": "j1", "strategy_id": 1, "strategy_class": "X",
            "strategy_version": 1, "vt_symbol": "000001.SZ",
            "start_date": "2024-01-01", "end_date": "2024-06-01",
            "parameters": '{"a":1}', "status": "completed",
            "result": '{"statistics":{}}', "error": None,
            "created_at": datetime(2024, 1, 1), "completed_at": datetime(2024, 1, 2),
        }
        c = _client()
        r = c.get("/api/v1/backtest/history/j1")
        assert r.status_code == 200
        assert r.json()["job_id"] == "j1"

    @patch("app.api.routes.backtest.BacktestHistoryDao")
    def test_history_detail_not_found(self, Dao):
        Dao.return_value.get_detail_for_user.return_value = None
        c = _client()
        r = c.get("/api/v1/backtest/history/missing")
        assert r.status_code == 404


# ═══════════════════════════════════════════════════════════════════════
# paper_trading routes
# ═══════════════════════════════════════════════════════════════════════

class TestPaperTradingRoutes:
    @patch("app.api.routes.paper_trading.PaperTradingService")
    @patch("app.api.routes.paper_trading.PaperAccountService")
    def test_deploy_success_no_account(self, AcctSvc, PTS):
        PTS.return_value.deploy.return_value = {"success": True, "deployment_id": 10, "strategy_name": "X"}
        c = _client()
        r = c.post("/api/v1/paper-trade/deploy", json={
            "strategy_id": 1, "vt_symbol": "000001.SZ", "parameters": {},
        })
        assert r.status_code == 201

    @patch("app.api.routes.paper_trading.PaperStrategyExecutor", create=True)
    @patch("app.api.routes.paper_trading.PaperTradingService")
    @patch("app.api.routes.paper_trading.PaperAccountService")
    def test_deploy_with_account_and_executor(self, AcctSvc, PTS, Exec):
        AcctSvc.return_value.get_account.return_value = {"status": "active", "market": "CN"}
        PTS.return_value.deploy.return_value = {"success": True, "deployment_id": 10, "strategy_name": "X"}
        # patch lazy import
        with patch("app.domains.trading.paper_strategy_executor.PaperStrategyExecutor") as PE:
            PE.return_value.start_deployment.return_value = {"success": True}
            c = _client()
            r = c.post("/api/v1/paper-trade/deploy", json={
                "strategy_id": 1, "vt_symbol": "000001.SZ", "parameters": {},
                "paper_account_id": 5,
            })
        assert r.status_code == 201

    @patch("app.api.routes.paper_trading.PaperTradingService")
    def test_deploy_bad_mode(self, PTS):
        c = _client()
        r = c.post("/api/v1/paper-trade/deploy", json={
            "strategy_id": 1, "vt_symbol": "000001.SZ",
            "execution_mode": "invalid",
        })
        assert r.status_code == 400

    @patch("app.api.routes.paper_trading.PaperTradingService")
    def test_deploy_failure(self, PTS):
        PTS.return_value.deploy.return_value = {"success": False, "error": "nope"}
        c = _client()
        r = c.post("/api/v1/paper-trade/deploy", json={
            "strategy_id": 1, "vt_symbol": "000001.SZ",
        })
        assert r.status_code == 400

    @patch("app.api.routes.paper_trading.PaperTradingService")
    def test_list_deployments(self, PTS):
        PTS.return_value.list_deployments.return_value = []
        c = _client()
        r = c.get("/api/v1/paper-trade/deployments")
        assert r.status_code == 200

    @patch("app.api.routes.paper_trading.PaperTradingService")
    def test_stop_deployment_ok(self, PTS):
        PTS.return_value.stop_deployment.return_value = True
        with patch("app.domains.trading.paper_strategy_executor.PaperStrategyExecutor") as PE:
            PE.return_value.stop_deployment.return_value = True
            c = _client()
            r = c.post("/api/v1/paper-trade/deployments/1/stop")
        assert r.status_code == 200

    @patch("app.api.routes.paper_trading.PaperTradingService")
    def test_stop_deployment_not_found(self, PTS):
        PTS.return_value.stop_deployment.return_value = False
        with patch("app.domains.trading.paper_strategy_executor.PaperStrategyExecutor") as PE:
            PE.return_value.stop_deployment.return_value = False
            c = _client()
            r = c.post("/api/v1/paper-trade/deployments/999/stop")
        assert r.status_code == 404

    @patch("app.api.routes.paper_trading.OrderDao")
    @patch("app.api.routes.paper_trading.validate_order")
    @patch("app.api.routes.paper_trading.try_fill_market_order")
    @patch("app.api.routes.paper_trading.RealtimeQuoteService")
    @patch("app.api.routes.paper_trading.PaperAccountService")
    def test_create_market_order_no_account(self, AcctSvc, QSvc, Fill, ValOrd, ODao):
        QSvc.return_value.get_quote.return_value = {"last_price": 10.0, "prev_close": 9.5}
        ValOrd.return_value = SimpleNamespace(valid=True, error=None)
        fill_mock = MagicMock()
        fill_mock.filled = True
        fill_mock.fill_price = 10.0
        fill_mock.fill_quantity = 100
        fill_mock.fee = MagicMock(total=0.5)
        Fill.return_value = fill_mock
        ODao.return_value.create.return_value = 42
        ODao.return_value.get_by_id.return_value = {"id": 42, "status": "filled"}
        c = _client()
        r = c.post("/api/v1/paper-trade/orders", json={
            "symbol": "000001", "direction": "buy", "order_type": "market", "quantity": 100,
        })
        assert r.status_code == 201

    @patch("app.api.routes.paper_trading.OrderDao")
    @patch("app.api.routes.paper_trading.validate_order")
    @patch("app.api.routes.paper_trading.RealtimeQuoteService")
    @patch("app.api.routes.paper_trading.PaperAccountService")
    def test_create_limit_order(self, AcctSvc, QSvc, ValOrd, ODao):
        QSvc.return_value.get_quote.return_value = {"last_price": 10.0, "prev_close": 9.5}
        ValOrd.return_value = SimpleNamespace(valid=True, error=None)
        ODao.return_value.create.return_value = 43
        ODao.return_value.get_by_id.return_value = {"id": 43, "status": "pending"}
        c = _client()
        r = c.post("/api/v1/paper-trade/orders", json={
            "symbol": "000001", "direction": "buy", "order_type": "limit",
            "quantity": 100, "price": 9.8,
        })
        assert r.status_code == 201

    def test_create_order_bad_direction(self):
        c = _client()
        r = c.post("/api/v1/paper-trade/orders", json={
            "symbol": "000001", "direction": "hold", "order_type": "market", "quantity": 100,
        })
        assert r.status_code == 400

    def test_create_order_bad_type(self):
        c = _client()
        r = c.post("/api/v1/paper-trade/orders", json={
            "symbol": "000001", "direction": "buy", "order_type": "stop", "quantity": 100,
        })
        assert r.status_code == 400

    def test_create_order_zero_qty(self):
        c = _client()
        r = c.post("/api/v1/paper-trade/orders", json={
            "symbol": "000001", "direction": "buy", "order_type": "market", "quantity": 0,
        })
        assert r.status_code == 400

    @patch("app.api.routes.paper_trading.OrderDao")
    def test_list_paper_orders(self, ODao):
        ODao.return_value.list_by_user.return_value = ([], 0)
        c = _client()
        r = c.get("/api/v1/paper-trade/orders")
        assert r.status_code == 200

    @patch("app.api.routes.paper_trading.OrderDao")
    def test_cancel_order_ok(self, ODao):
        ODao.return_value.get_by_id.return_value = {
            "id": 1, "direction": "buy", "quantity": 100,
            "price": 10.0, "paper_account_id": None,
        }
        ODao.return_value.cancel.return_value = True
        c = _client()
        r = c.post("/api/v1/paper-trade/orders/1/cancel")
        assert r.status_code == 200

    @patch("app.api.routes.paper_trading.OrderDao")
    def test_cancel_order_not_found(self, ODao):
        ODao.return_value.get_by_id.return_value = None
        c = _client()
        r = c.post("/api/v1/paper-trade/orders/99/cancel")
        assert r.status_code == 404

    @patch("app.api.routes.paper_trading.PaperTradingService")
    def test_get_positions(self, PTS):
        PTS.return_value.get_positions.return_value = []
        c = _client()
        r = c.get("/api/v1/paper-trade/positions")
        assert r.status_code == 200

    @patch("app.api.routes.paper_trading.PaperTradingService")
    def test_get_performance(self, PTS):
        PTS.return_value.get_performance.return_value = {"total_pnl": 0}
        c = _client()
        r = c.get("/api/v1/paper-trade/performance")
        assert r.status_code == 200

    @patch("app.api.routes.paper_trading.connection")
    def test_list_signals(self, conn_mock):
        ctx = MagicMock()
        ctx.__enter__ = MagicMock(return_value=ctx)
        ctx.__exit__ = MagicMock(return_value=False)
        ctx.execute.return_value.fetchall.return_value = []
        conn_mock.return_value = ctx
        c = _client()
        r = c.get("/api/v1/paper-trade/signals")
        assert r.status_code == 200

    @patch("app.api.routes.paper_trading.connection")
    def test_reject_signal_not_found(self, conn_mock):
        ctx = MagicMock()
        ctx.__enter__ = MagicMock(return_value=ctx)
        ctx.__exit__ = MagicMock(return_value=False)
        result = MagicMock()
        result.rowcount = 0
        ctx.execute.return_value = result
        conn_mock.return_value = ctx
        c = _client()
        r = c.post("/api/v1/paper-trade/signals/999/reject")
        assert r.status_code == 404

    @patch("app.api.routes.paper_trading.connection")
    def test_reject_signal_ok(self, conn_mock):
        ctx = MagicMock()
        ctx.__enter__ = MagicMock(return_value=ctx)
        ctx.__exit__ = MagicMock(return_value=False)
        result = MagicMock()
        result.rowcount = 1
        ctx.execute.return_value = result
        conn_mock.return_value = ctx
        c = _client()
        r = c.post("/api/v1/paper-trade/signals/1/reject")
        assert r.status_code == 200


# ═══════════════════════════════════════════════════════════════════════
# strategies routes
# ═══════════════════════════════════════════════════════════════════════

class TestStrategiesRoutes:
    @patch("app.api.routes.strategies.StrategiesService")
    def test_list(self, Svc):
        Svc.return_value.count_strategies.return_value = 1
        Svc.return_value.list_strategies_paginated.return_value = [{
            "id": 1, "name": "S1", "class_name": "X", "description": "d",
            "version": 1, "is_active": True,
            "created_at": datetime.utcnow(), "updated_at": datetime.utcnow(),
        }]
        c = _client()
        r = c.get("/api/v1/strategies")
        assert r.status_code == 200

    @patch("app.api.routes.strategies.StrategiesService")
    def test_create(self, Svc):
        Svc.return_value.create_strategy.return_value = {
            "id": 1, "user_id": 1, "name": "S1", "class_name": "X",
            "description": "", "parameters": {}, "code": "",
            "version": 1, "is_active": True,
            "created_at": datetime.utcnow(), "updated_at": datetime.utcnow(),
        }
        c = _client()
        r = c.post("/api/v1/strategies", json={
            "name": "S1", "class_name": "X",
        })
        assert r.status_code == 201

    @patch("app.api.routes.strategies.StrategiesService")
    def test_create_value_error(self, Svc):
        Svc.return_value.create_strategy.side_effect = ValueError("dup")
        c = _client()
        r = c.post("/api/v1/strategies", json={"name": "S1", "class_name": "X"})
        assert r.status_code == 400

    @patch("app.api.routes.strategies.StrategiesService")
    def test_get_strategy(self, Svc):
        Svc.return_value.get_strategy.return_value = {
            "id": 1, "user_id": 1, "name": "S1", "class_name": "X",
            "description": "", "parameters": {}, "code": "",
            "version": 1, "is_active": True,
            "created_at": datetime.utcnow(), "updated_at": datetime.utcnow(),
        }
        c = _client()
        r = c.get("/api/v1/strategies/1")
        assert r.status_code == 200

    @patch("app.api.routes.strategies.StrategiesService")
    def test_get_strategy_not_found(self, Svc):
        Svc.return_value.get_strategy.side_effect = KeyError("not found")
        c = _client()
        r = c.get("/api/v1/strategies/999")
        assert r.status_code == 404

    @patch("app.api.routes.strategies.StrategiesService")
    def test_update_strategy(self, Svc):
        Svc.return_value.update_strategy.return_value = {
            "id": 1, "user_id": 1, "name": "S1u", "class_name": "X",
            "description": "", "parameters": {}, "code": "",
            "version": 2, "is_active": True,
            "created_at": datetime.utcnow(), "updated_at": datetime.utcnow(),
        }
        c = _client()
        r = c.put("/api/v1/strategies/1", json={"name": "S1u"})
        assert r.status_code == 200

    @patch("app.api.routes.strategies.StrategiesService")
    def test_update_not_found(self, Svc):
        Svc.return_value.update_strategy.side_effect = KeyError
        c = _client()
        r = c.put("/api/v1/strategies/999", json={"name": "X"})
        assert r.status_code == 404

    @patch("app.api.routes.strategies.StrategiesService")
    def test_update_value_error(self, Svc):
        Svc.return_value.update_strategy.side_effect = ValueError("bad")
        c = _client()
        r = c.put("/api/v1/strategies/1", json={"name": "X"})
        assert r.status_code == 400

    @patch("app.api.routes.strategies.StrategiesService")
    def test_delete_strategy(self, Svc):
        c = _client()
        r = c.delete("/api/v1/strategies/1")
        assert r.status_code == 204

    @patch("app.api.routes.strategies.StrategiesService")
    def test_delete_not_found(self, Svc):
        Svc.return_value.delete_strategy.side_effect = KeyError
        c = _client()
        r = c.delete("/api/v1/strategies/999")
        assert r.status_code == 404

    @patch("app.api.routes.strategies.StrategiesService")
    @patch("app.api.routes.strategies.validate_strategy_code")
    def test_validate(self, val_fn, Svc):
        Svc.return_value.get_strategy.return_value = {
            "id": 1, "user_id": 1, "name": "S1", "class_name": "X",
            "description": "", "parameters": {}, "code": "pass",
            "version": 1, "is_active": True,
            "created_at": datetime.utcnow(), "updated_at": datetime.utcnow(),
        }
        val_fn.return_value = {"valid": True, "errors": [], "warnings": []}
        c = _client()
        r = c.post("/api/v1/strategies/1/validate")
        assert r.status_code == 200

    @patch("app.api.routes.strategies.StrategiesService")
    def test_list_code_history(self, Svc):
        Svc.return_value.list_code_history.return_value = []
        c = _client()
        r = c.get("/api/v1/strategies/1/code-history")
        assert r.status_code == 200

    @patch("app.api.routes.strategies.StrategiesService")
    def test_list_code_history_not_found(self, Svc):
        Svc.return_value.list_code_history.side_effect = KeyError("not found")
        c = _client()
        r = c.get("/api/v1/strategies/999/code-history")
        assert r.status_code == 404

    @patch("app.api.routes.strategies.StrategiesService")
    def test_get_code_history_entry(self, Svc):
        Svc.return_value.get_code_history.return_value = {"id": 1, "code": "x"}
        c = _client()
        r = c.get("/api/v1/strategies/1/code-history/1")
        assert r.status_code == 200

    @patch("app.api.routes.strategies.StrategiesService")
    def test_get_code_history_entry_not_found(self, Svc):
        Svc.return_value.get_code_history.side_effect = KeyError("History not found")
        c = _client()
        r = c.get("/api/v1/strategies/1/code-history/999")
        assert r.status_code == 404

    @patch("app.api.routes.strategies.StrategiesService")
    def test_restore_code_history(self, Svc):
        c = _client()
        r = c.post("/api/v1/strategies/1/code-history/1/restore")
        assert r.status_code == 200

    @patch("app.api.routes.strategies.StrategiesService")
    def test_restore_code_history_not_found(self, Svc):
        Svc.return_value.restore_code_history.side_effect = KeyError("History not found")
        c = _client()
        r = c.post("/api/v1/strategies/1/code-history/999/restore")
        assert r.status_code == 404

    def test_generate_multi_factor_code(self):
        with patch("app.domains.strategies.multi_factor_engine.generate_cta_code", return_value="code"):
            c = _client()
            r = c.post("/api/v1/strategies/multi-factor/generate-code", json={
                "name": "MF", "class_name": "MFStrat", "factors": [
                    {"factor_name": "f1", "expression": "close/open", "weight": 1.0, "direction": 1}
                ],
            })
            assert r.status_code == 200
            assert r.json()["code"] == "code"

    @patch("app.domains.strategies.multi_factor_engine.save_strategy_factors")
    @patch("app.domains.strategies.multi_factor_engine.generate_cta_code", return_value="code")
    @patch("app.api.routes.strategies.StrategiesService")
    def test_create_multi_factor_strategy(self, Svc, gen, save):
        Svc.return_value.create_strategy.return_value = {
            "id": 5, "user_id": 1, "name": "MF", "class_name": "MFStrat",
            "description": "d", "parameters": {}, "code": "code",
            "version": 1, "is_active": True,
            "created_at": datetime.utcnow(), "updated_at": datetime.utcnow(),
        }
        c = _client()
        r = c.post("/api/v1/strategies/multi-factor/create", json={
            "name": "MF", "class_name": "MFStrat",
            "factors": [{"factor_name": "f1", "expression": "close", "weight": 1.0, "direction": 1}],
        })
        assert r.status_code == 201

    @patch("app.domains.strategies.multi_factor_engine.get_strategy_factors", return_value=[])
    @patch("app.api.routes.strategies.StrategiesService")
    def test_get_strategy_factors(self, Svc, gsf):
        Svc.return_value.get_strategy.return_value = {"id": 1}
        c = _client()
        r = c.get("/api/v1/strategies/1/factors")
        assert r.status_code == 200

    @patch("app.domains.strategies.multi_factor_engine.generate_qlib_config", return_value={"config": True})
    def test_generate_qlib_config(self, gqc):
        c = _client()
        r = c.post("/api/v1/strategies/multi-factor/qlib-config", json={
            "factors": [{"factor_name": "f1", "expression": "close", "weight": 1, "direction": 1}],
        })
        assert r.status_code == 200


# ═══════════════════════════════════════════════════════════════════════
# websocket helpers (unit tests — no actual WS connection)
# ═══════════════════════════════════════════════════════════════════════

class TestWebSocketHelpers:
    def test_validate_channel_access_public(self):
        from app.api.routes.websocket import _validate_channel_access
        assert _validate_channel_access("market:000001", 99) is True

    def test_validate_channel_access_own(self):
        from app.api.routes.websocket import _validate_channel_access
        assert _validate_channel_access("alerts:5", 5) is True

    def test_validate_channel_access_other(self):
        from app.api.routes.websocket import _validate_channel_access
        assert _validate_channel_access("alerts:5", 99) is False

    def test_validate_channel_access_bad_id(self):
        from app.api.routes.websocket import _validate_channel_access
        assert _validate_channel_access("orders:abc", 1) is False

    def test_connection_manager_connect_disconnect(self):
        from app.api.routes.websocket import ConnectionManager
        import asyncio
        mgr = ConnectionManager()
        ws = MagicMock()
        ws.send_json = AsyncMock()
        asyncio.run(mgr.connect(ws, "market:test"))
        assert mgr.active_count == 1
        mgr.disconnect_all(ws)
        assert mgr.active_count == 0

    def test_connection_manager_broadcast(self):
        from app.api.routes.websocket import ConnectionManager
        import asyncio
        mgr = ConnectionManager()
        ws = MagicMock()
        ws.send_json = AsyncMock()
        asyncio.run(mgr.connect(ws, "ch1"))
        asyncio.run(mgr.broadcast("ch1", {"data": 1}))
        ws.send_json.assert_called_once_with({"data": 1})

    def test_connection_manager_disconnect_channel(self):
        from app.api.routes.websocket import ConnectionManager
        import asyncio
        mgr = ConnectionManager()
        ws = MagicMock()
        ws.send_json = AsyncMock()
        asyncio.run(mgr.connect(ws, "ch1"))
        asyncio.run(mgr.connect(ws, "ch2"))
        assert mgr.active_count == 2
        mgr.disconnect(ws, "ch1")
        assert mgr.active_count == 1

    def test_broadcast_dead_ws(self):
        from app.api.routes.websocket import ConnectionManager
        import asyncio
        mgr = ConnectionManager()
        ws = MagicMock()
        ws.send_json = AsyncMock(side_effect=RuntimeError("closed"))
        asyncio.run(mgr.connect(ws, "ch1"))
        asyncio.run(mgr.broadcast("ch1", {"data": 1}))
        # dead ws should be removed
        assert mgr.active_count == 0

    def test_broadcast_empty_channel(self):
        from app.api.routes.websocket import ConnectionManager
        import asyncio
        mgr = ConnectionManager()
        asyncio.run(mgr.broadcast("nonexistent", {}))
