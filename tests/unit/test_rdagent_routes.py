"""Tests for RD-Agent routes — /api/v1/rdagent/*."""
import pytest
from unittest.mock import patch, MagicMock
from fastapi import FastAPI
from fastapi.testclient import TestClient

from app.api.routes import rdagent
from app.api.exception_handlers import register_exception_handlers


@pytest.fixture
def mock_user():
    return type("User", (), {"id": 1, "user_id": 1, "username": "testuser"})()


@pytest.fixture
def client(mock_user):
    test_app = FastAPI()
    register_exception_handlers(test_app)
    test_app.include_router(rdagent.router, prefix="/api/v1")

    async def override_auth():
        return mock_user

    test_app.dependency_overrides[rdagent.get_current_user] = override_auth
    return TestClient(test_app)


class TestStartMining:

    @patch("app.worker.service.config.get_queue")
    @patch("app.api.routes.rdagent.RDAgentService")
    def test_start_mining_success(self, MockService, mock_queue, client):
        mock_svc = MockService.return_value
        mock_svc.start_mining.return_value = {"run_id": "abc-123", "status": "queued"}

        mock_q = MagicMock()
        mock_queue.return_value = mock_q

        resp = client.post("/api/v1/rdagent/runs", json={
            "scenario": "fin_factor",
            "max_iterations": 5,
        })
        assert resp.status_code == 201
        data = resp.json()
        assert data["run_id"] == "abc-123"
        assert data["status"] == "queued"
        mock_q.enqueue.assert_called_once()

    @patch("app.worker.service.config.get_queue")
    @patch("app.api.routes.rdagent.RDAgentService")
    def test_start_mining_default_config(self, MockService, mock_queue, client):
        mock_svc = MockService.return_value
        mock_svc.start_mining.return_value = {"run_id": "def-456", "status": "queued"}
        mock_queue.return_value = MagicMock()

        resp = client.post("/api/v1/rdagent/runs", json={})
        assert resp.status_code == 201

    def test_start_mining_invalid_scenario(self, client):
        resp = client.post("/api/v1/rdagent/runs", json={
            "scenario": "invalid_scenario",
        })
        assert resp.status_code == 422


class TestListRuns:

    @patch("app.api.routes.rdagent.RDAgentService")
    def test_list_runs(self, MockService, client):
        mock_svc = MockService.return_value
        mock_svc.list_runs.return_value = [
            {"run_id": "r1", "scenario": "fin_factor", "status": "completed"},
        ]

        resp = client.get("/api/v1/rdagent/runs")
        assert resp.status_code == 200
        data = resp.json()
        assert len(data) == 1
        assert data[0]["run_id"] == "r1"

    @patch("app.api.routes.rdagent.RDAgentService")
    def test_list_runs_with_params(self, MockService, client):
        mock_svc = MockService.return_value
        mock_svc.list_runs.return_value = []

        resp = client.get("/api/v1/rdagent/runs?limit=5&offset=10")
        assert resp.status_code == 200
        mock_svc.list_runs.assert_called_once_with(1, limit=5, offset=10)


class TestGetRun:

    @patch("app.api.routes.rdagent.RDAgentService")
    def test_get_run_found(self, MockService, client):
        mock_svc = MockService.return_value
        mock_svc.get_run.return_value = {
            "run_id": "abc-123", "scenario": "fin_factor",
            "status": "running", "current_iteration": 3,
        }

        resp = client.get("/api/v1/rdagent/runs/abc-123")
        assert resp.status_code == 200
        assert resp.json()["run_id"] == "abc-123"

    @patch("app.api.routes.rdagent.RDAgentService")
    def test_get_run_not_found(self, MockService, client):
        mock_svc = MockService.return_value
        mock_svc.get_run.return_value = None

        resp = client.get("/api/v1/rdagent/runs/nonexistent")
        assert resp.status_code == 404


class TestCancelRun:

    @patch("app.api.routes.rdagent.RDAgentService")
    def test_cancel_success(self, MockService, client):
        mock_svc = MockService.return_value
        mock_svc.cancel_run.return_value = {"run_id": "r1", "status": "cancelled"}

        resp = client.delete("/api/v1/rdagent/runs/r1")
        assert resp.status_code == 200
        assert resp.json()["status"] == "cancelled"

    @patch("app.api.routes.rdagent.RDAgentService")
    def test_cancel_not_found(self, MockService, client):
        mock_svc = MockService.return_value
        mock_svc.cancel_run.side_effect = KeyError("Run not found")

        resp = client.delete("/api/v1/rdagent/runs/nonexistent")
        assert resp.status_code == 404

    @patch("app.api.routes.rdagent.RDAgentService")
    def test_cancel_invalid_status(self, MockService, client):
        mock_svc = MockService.return_value
        mock_svc.cancel_run.side_effect = ValueError("Cannot cancel run in status: completed")

        resp = client.delete("/api/v1/rdagent/runs/r1")
        assert resp.status_code == 400


class TestGetIterations:

    @patch("app.api.routes.rdagent.RDAgentService")
    def test_get_iterations(self, MockService, client):
        mock_svc = MockService.return_value
        mock_svc.get_iterations.return_value = [
            {"id": 1, "iteration_number": 1, "hypothesis": "H1", "status": "completed"},
        ]

        resp = client.get("/api/v1/rdagent/runs/r1/iterations")
        assert resp.status_code == 200
        data = resp.json()
        assert len(data) == 1

    @patch("app.api.routes.rdagent.RDAgentService")
    def test_get_iterations_not_found(self, MockService, client):
        mock_svc = MockService.return_value
        mock_svc.get_iterations.side_effect = KeyError("Run not found")

        resp = client.get("/api/v1/rdagent/runs/nonexistent/iterations")
        assert resp.status_code == 404


class TestGetDiscoveredFactors:

    @patch("app.api.routes.rdagent.RDAgentService")
    def test_get_discovered_factors(self, MockService, client):
        mock_svc = MockService.return_value
        mock_svc.get_discovered_factors.return_value = [
            {"id": 1, "factor_name": "Alpha1", "icir": 0.3, "status": "discovered"},
        ]

        resp = client.get("/api/v1/rdagent/runs/r1/factors")
        assert resp.status_code == 200
        data = resp.json()
        assert len(data) == 1
        assert data[0]["factor_name"] == "Alpha1"

    @patch("app.api.routes.rdagent.RDAgentService")
    def test_get_discovered_factors_not_found(self, MockService, client):
        mock_svc = MockService.return_value
        mock_svc.get_discovered_factors.side_effect = KeyError("Run not found")

        resp = client.get("/api/v1/rdagent/runs/nonexistent/factors")
        assert resp.status_code == 404


class TestImportFactor:

    @patch("app.api.routes.rdagent.RDAgentService")
    def test_import_success(self, MockService, client):
        mock_svc = MockService.return_value
        mock_svc.import_factor.return_value = {"id": 100, "name": "Alpha1"}

        resp = client.post("/api/v1/rdagent/runs/r1/import", json={
            "discovered_factor_id": 42,
        })
        assert resp.status_code == 200
        assert resp.json()["id"] == 100

    @patch("app.api.routes.rdagent.RDAgentService")
    def test_import_not_found(self, MockService, client):
        mock_svc = MockService.return_value
        mock_svc.import_factor.side_effect = KeyError("Discovered factor not found")

        resp = client.post("/api/v1/rdagent/runs/r1/import", json={
            "discovered_factor_id": 999,
        })
        assert resp.status_code == 404

    def test_import_missing_body(self, client):
        resp = client.post("/api/v1/rdagent/runs/r1/import", json={})
        assert resp.status_code == 422


class TestDataCatalog:

    @patch("app.domains.factors.data_catalog.get_catalog_summary")
    def test_get_data_catalog(self, mock_catalog, client):
        mock_catalog.return_value = {
            "categories": {"price": ["close", "open"]},
            "total_fields": 2,
            "sources": ["tushare"],
        }

        resp = client.get("/api/v1/rdagent/data-catalog")
        assert resp.status_code == 200
        data = resp.json()
        assert data["total_fields"] == 2
        assert "price" in data["categories"]


class TestFeatureDescriptor:

    @patch("app.domains.factors.feature_descriptor.build_feature_descriptor")
    def test_get_feature_descriptor(self, mock_desc, client):
        mock_desc.return_value = {
            "available_features": {"price": {"fields": ["close"], "count": 1}},
            "total_fields": 1,
            "sources": ["tushare"],
        }

        resp = client.get("/api/v1/rdagent/feature-descriptor")
        assert resp.status_code == 200
        data = resp.json()
        assert data["total_fields"] == 1
