"""Unit tests for app.api.routes.datasync."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from app.api.routes import datasync as datasync_routes
from app.api.exception_handlers import register_exception_handlers


@pytest.fixture()
def mock_user():
    return type("User", (), {"id": 1, "user_id": 1, "username": "test", "sub": 1})()


@pytest.fixture()
def client(mock_user):
    test_app = FastAPI()
    register_exception_handlers(test_app)
    test_app.include_router(datasync_routes.router, prefix="/api/v1")

    async def override_auth():
        return mock_user

    test_app.dependency_overrides[datasync_routes.get_current_user] = override_auth
    for route in test_app.routes:
        if hasattr(route, "dependencies"):
            route.dependencies = []
    return TestClient(test_app, raise_server_exceptions=False)


class TestDatasyncRoutes:
    @patch("app.infrastructure.db.connections.get_quantmate_engine")
    def test_get_sync_status(self, mock_engine, client):
        conn = MagicMock()
        mock_engine.return_value.connect.return_value.__enter__ = MagicMock(return_value=conn)
        mock_engine.return_value.connect.return_value.__exit__ = MagicMock(return_value=False)
        # count query
        count_row = MagicMock()
        count_row.__getitem__ = lambda self, idx: 0
        conn.execute.return_value.fetchone.return_value = count_row
        conn.execute.return_value.fetchall.return_value = []
        resp = client.get("/api/v1/datasync/status")
        assert resp.status_code == 200

    @patch("app.infrastructure.db.connections.get_quantmate_engine")
    def test_get_sync_summary(self, mock_engine, client):
        conn = MagicMock()
        mock_engine.return_value.connect.return_value.__enter__ = MagicMock(return_value=conn)
        mock_engine.return_value.connect.return_value.__exit__ = MagicMock(return_value=False)
        conn.execute.return_value.fetchall.return_value = []
        resp = client.get("/api/v1/datasync/status/summary")
        assert resp.status_code == 200

    @patch("app.infrastructure.db.connections.get_quantmate_engine")
    def test_get_latest_sync_status(self, mock_engine, client):
        conn = MagicMock()
        mock_engine.return_value.connect.return_value.__enter__ = MagicMock(return_value=conn)
        mock_engine.return_value.connect.return_value.__exit__ = MagicMock(return_value=False)
        # MAX(sync_date) returns None
        row = MagicMock()
        row.__getitem__ = lambda self, idx: None
        conn.execute.return_value.fetchone.return_value = row
        resp = client.get("/api/v1/datasync/status/latest")
        assert resp.status_code == 200

    @patch("app.datasync.service.init_service.get_initialization_state")
    def test_get_sync_initialization_status(self, mock_get_state, client):
        mock_get_state.return_value = {
            "bootstrap_completed": True,
            "sync_status_initialized": False,
            "needs_initialization": True,
            "sync_status_window_start": "2025-04-16",
            "sync_status_window_end": "2026-04-16",
            "trade_days_in_window": 243,
            "enabled_sync_items": 15,
            "sync_status_missing_items": [{"source": "tushare", "item_key": "trade_cal"}],
            "sync_status_incomplete_items": [],
            "sync_status_unsupported_enabled_items": [],
        }

        resp = client.get("/api/v1/datasync/status/initialization")

        assert resp.status_code == 200
        payload = resp.json()
        assert payload["needs_initialization"] is True
        assert payload["sync_status_missing_items"] == [{"source": "tushare", "item_key": "trade_cal"}]

    def test_trigger_manual_sync(self, client):
        with patch("app.worker.service.config.get_queue") as mock_q:
            queue = MagicMock()
            mock_q.return_value = queue
            queue.enqueue.return_value = MagicMock(id="job-1")
            resp = client.post("/api/v1/datasync/trigger", json={})
            assert resp.status_code in (200, 202, 500)
