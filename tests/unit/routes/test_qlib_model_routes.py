"""Tests for Qlib AI Model routes."""
import pytest
from unittest.mock import patch
from fastapi import FastAPI
from fastapi.testclient import TestClient

from app.api.routes import ai_model
from app.api.exception_handlers import register_exception_handlers


@pytest.fixture
def mock_user():
    return type("User", (), {"id": 1, "user_id": 1, "username": "testuser"})()


@pytest.fixture
def client(mock_user):
    test_app = FastAPI()
    register_exception_handlers(test_app)
    test_app.include_router(ai_model.router, prefix="/api/v1")

    async def override_auth():
        return mock_user

    test_app.dependency_overrides[ai_model.get_current_user] = override_auth
    return TestClient(test_app)


class TestQlibStatus:

    @patch("app.api.routes.ai_model.is_qlib_available", return_value=True)
    def test_status_available(self, mock_avail, client):
        resp = client.get("/api/v1/ai/qlib/status")
        assert resp.status_code == 200
        data = resp.json()
        assert data["available"] is True
        assert "ready" in data["message"].lower()

    @patch("app.api.routes.ai_model.is_qlib_available", return_value=False)
    def test_status_unavailable(self, mock_avail, client):
        resp = client.get("/api/v1/ai/qlib/status")
        assert resp.status_code == 200
        data = resp.json()
        assert data["available"] is False
        assert "not installed" in data["message"].lower()


class TestSupportedModelsAndDatasets:

    @patch("app.domains.ai.qlib_model_service.QlibModelService.list_supported_models")
    def test_list_supported_models(self, mock_list, client):
        mock_list.return_value = [
            {"name": "LightGBM", "class": "qlib.contrib.model.gbdt.LGBModel"},
            {"name": "Linear", "class": "qlib.contrib.model.linear.LinearModel"},
        ]
        resp = client.get("/api/v1/ai/qlib/supported-models")
        assert resp.status_code == 200
        assert len(resp.json()) == 2

    @patch("app.domains.ai.qlib_model_service.QlibModelService.list_supported_datasets")
    def test_list_supported_datasets(self, mock_list, client):
        mock_list.return_value = [
            {"name": "Alpha158", "class": "qlib.contrib.data.handler.Alpha158"},
        ]
        resp = client.get("/api/v1/ai/qlib/supported-datasets")
        assert resp.status_code == 200
        assert len(resp.json()) == 1


class TestTrainModel:

    @patch("app.worker.service.qlib_tasks.run_qlib_training_task")
    @patch("app.api.routes.ai_model.is_qlib_available", return_value=True)
    def test_train_model_queued(self, mock_avail, mock_task, client):
        resp = client.post("/api/v1/ai/qlib/train", json={
            "model_type": "LightGBM",
            "factor_set": "Alpha158",
            "universe": "csi300",
        })
        assert resp.status_code == 200
        data = resp.json()
        assert data["status"] == "queued"
        assert "LightGBM" in data["message"]

    @patch("app.api.routes.ai_model.is_qlib_available", return_value=False)
    def test_train_model_qlib_not_available(self, mock_avail, client):
        resp = client.post("/api/v1/ai/qlib/train", json={
            "model_type": "LightGBM",
        })
        assert resp.status_code == 503

    @patch("app.worker.service.qlib_tasks.run_qlib_training_task")
    @patch("app.api.routes.ai_model.is_qlib_available", return_value=True)
    def test_train_model_with_custom_dates(self, mock_avail, mock_task, client):
        resp = client.post("/api/v1/ai/qlib/train", json={
            "model_type": "LSTM",
            "train_start": "2019-01-01",
            "train_end": "2023-12-31",
            "valid_start": "2024-01-01",
            "valid_end": "2024-06-30",
            "test_start": "2024-07-01",
            "test_end": "2024-12-31",
        })
        assert resp.status_code == 200

    @patch("app.worker.service.qlib_tasks.run_qlib_training_task")
    @patch("app.api.routes.ai_model.is_qlib_available", return_value=True)
    def test_train_model_with_hyperparams(self, mock_avail, mock_task, client):
        resp = client.post("/api/v1/ai/qlib/train", json={
            "model_type": "LightGBM",
            "hyperparams": {"num_leaves": 128, "learning_rate": 0.05},
        })
        assert resp.status_code == 200


class TestTrainingRuns:

    @patch("app.domains.ai.qlib_model_service.QlibModelService.list_training_runs")
    def test_list_training_runs(self, mock_list, client):
        mock_list.return_value = [
            {"id": 1, "model_type": "LightGBM", "status": "completed"},
            {"id": 2, "model_type": "LSTM", "status": "running"},
        ]
        resp = client.get("/api/v1/ai/qlib/training-runs")
        assert resp.status_code == 200
        assert len(resp.json()) == 2

    @patch("app.domains.ai.qlib_model_service.QlibModelService.list_training_runs")
    def test_list_training_runs_with_filter(self, mock_list, client):
        mock_list.return_value = []
        resp = client.get("/api/v1/ai/qlib/training-runs?status=completed&limit=10&offset=5")
        assert resp.status_code == 200
        mock_list.assert_called_once_with(user_id=1, status="completed", limit=10, offset=5)

    @patch("app.domains.ai.qlib_model_service.QlibModelService.get_training_run")
    def test_get_training_run(self, mock_get, client):
        mock_get.return_value = {"id": 1, "model_type": "LightGBM", "status": "completed"}
        resp = client.get("/api/v1/ai/qlib/training-runs/1")
        assert resp.status_code == 200
        assert resp.json()["id"] == 1

    @patch("app.domains.ai.qlib_model_service.QlibModelService.get_training_run")
    def test_get_training_run_not_found(self, mock_get, client):
        mock_get.return_value = None
        resp = client.get("/api/v1/ai/qlib/training-runs/999")
        assert resp.status_code == 404


class TestPredictions:

    @patch("app.domains.ai.qlib_model_service.QlibModelService.get_predictions")
    def test_get_predictions(self, mock_preds, client):
        mock_preds.return_value = [
            {"instrument": "SZ000001", "date": "2024-01-02", "score": 0.05, "rank_pct": 0.1},
        ]
        resp = client.get("/api/v1/ai/qlib/training-runs/1/predictions")
        assert resp.status_code == 200
        assert len(resp.json()) == 1

    @patch("app.domains.ai.qlib_model_service.QlibModelService.get_predictions")
    def test_get_predictions_with_filter(self, mock_preds, client):
        mock_preds.return_value = []
        resp = client.get("/api/v1/ai/qlib/training-runs/1/predictions?trade_date=2024-01-02&top_n=10")
        assert resp.status_code == 200
        mock_preds.assert_called_once_with(training_run_id=1, trade_date="2024-01-02", top_n=10)


class TestDataConvert:

    @patch("app.worker.service.qlib_tasks.run_data_conversion_task")
    def test_convert_data(self, mock_task, client):
        resp = client.post("/api/v1/ai/qlib/data/convert", json={})
        assert resp.status_code == 200
        data = resp.json()
        assert data["status"] == "queued"

    @patch("app.worker.service.qlib_tasks.run_data_conversion_task")
    def test_convert_data_with_params(self, mock_task, client):
        resp = client.post("/api/v1/ai/qlib/data/convert", json={
            "start_date": "2023-01-01",
            "end_date": "2024-12-31",
            "use_akshare_supplement": True,
        })
        assert resp.status_code == 200
