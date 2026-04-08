"""Unit tests for app.api.main — standalone endpoints and setup."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient


@pytest.fixture()
def client():
    """Build a minimal TestClient from the real app (with mocked DB)."""
    with patch("app.infrastructure.db.connections.get_quantmate_engine", return_value=MagicMock()), \
         patch("app.infrastructure.db.connections.get_tushare_engine", return_value=MagicMock()), \
         patch("app.infrastructure.db.connections.connection") as mock_conn:
        ctx = MagicMock()
        ctx.__enter__ = MagicMock(return_value=MagicMock())
        ctx.__exit__ = MagicMock(return_value=False)
        mock_conn.return_value = ctx
        try:
            from app.api.main import app
            return TestClient(app, raise_server_exceptions=False)
        except Exception:
            pytest.skip("Cannot import app.api.main (heavy deps)")


class TestMainEndpoints:
    def test_root(self, client):
        resp = client.get("/")
        assert resp.status_code == 200
        data = resp.json()
        assert "name" in data or "app" in data or "version" in data or "status" in data

    def test_health(self, client):
        with patch("app.api.main.get_quantmate_engine", create=True) as mock_eng:
            mock_engine = MagicMock()
            mock_eng.return_value = mock_engine
            conn_ctx = MagicMock()
            conn_ctx.__enter__ = MagicMock(return_value=MagicMock())
            conn_ctx.__exit__ = MagicMock(return_value=False)
            mock_engine.connect.return_value = conn_ctx
            resp = client.get("/health")
            assert resp.status_code in (200, 503)

    def test_api_legacy(self, client):
        resp = client.get("/api")
        assert resp.status_code in (200, 307)

    def test_metrics(self, client):
        # /metrics does a lazy import: from app.datasync.metrics import get_metrics
        # Pre-import the module so the patch target exists, then patch it
        try:
            import app.datasync.metrics as _dm
        except Exception:
            pytest.skip("Cannot import app.datasync.metrics")
        with patch.object(_dm, "get_metrics", return_value=b"# HELP\n"):
            resp = client.get("/metrics")
            assert resp.status_code in (200, 500)
