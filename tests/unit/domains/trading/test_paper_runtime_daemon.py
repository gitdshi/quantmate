"""Unit tests for the paper runtime daemon."""

from __future__ import annotations

from unittest.mock import MagicMock

import app.domains.trading.paper_runtime_daemon as _mod


def _fake_conn():
    conn = MagicMock()
    ctx = MagicMock()
    ctx.__enter__ = MagicMock(return_value=conn)
    ctx.__exit__ = MagicMock(return_value=False)
    return ctx, conn


def _row(**kw):
    m = MagicMock()
    for k, v in kw.items():
        setattr(m, k, v)
    return m


def test_run_once_starts_running_deployment_and_persists_heartbeat(monkeypatch):
    runtime_service = MagicMock()
    runtime_service._sessions = {}
    runtime_service.get_runtime.return_value = None
    runtime_service.start_deployment.return_value = {
        "success": True,
        "runtime": {
            "status": "running",
            "runtime_mode": "legacy_executor_bridge",
            "strategy_kind": "cta",
            "gateway_name": "PAPER.5",
            "warnings": [],
        },
    }

    daemon = _mod.PaperRuntimeDaemon(runtime_service=runtime_service, worker_id="worker-a")
    ctx, conn = _fake_conn()
    conn.execute.side_effect = [
        MagicMock(fetchall=MagicMock(return_value=[
            _row(
                id=5,
                user_id=1,
                paper_account_id=2,
                strategy_id=3,
                strategy_name="TripleMA",
                vt_symbol="000001.SZSE",
                parameters='{"fast": 5}',
                execution_mode="auto",
                desired_status="running",
            )
        ])),
        MagicMock(),
        MagicMock(),
    ]
    monkeypatch.setattr(_mod, "connection", lambda db: ctx)

    daemon.run_once()

    runtime_service.start_deployment.assert_called_once()
    assert conn.execute.call_count == 3
    conn.commit.assert_called_once()


def test_run_once_stops_stopped_deployment(monkeypatch):
    runtime_service = MagicMock()
    runtime_service._sessions = {7: object()}
    runtime_service.stop_deployment.return_value = {
        "success": True,
        "runtime": {
            "runtime_mode": "legacy_executor_bridge",
            "strategy_kind": "cta",
            "gateway_name": "PAPER.7",
        },
    }

    daemon = _mod.PaperRuntimeDaemon(runtime_service=runtime_service, worker_id="worker-b")
    ctx, conn = _fake_conn()
    conn.execute.side_effect = [
        MagicMock(fetchall=MagicMock(return_value=[
            _row(
                id=7,
                user_id=1,
                paper_account_id=2,
                strategy_id=3,
                strategy_name="TripleMA",
                vt_symbol="000001.SZSE",
                parameters='{}',
                execution_mode="auto",
                desired_status="stopped",
            )
        ])),
        MagicMock(),
        MagicMock(),
    ]
    monkeypatch.setattr(_mod, "connection", lambda db: ctx)

    daemon.run_once()

    runtime_service.stop_deployment.assert_called_once_with(7)
    conn.commit.assert_called_once()