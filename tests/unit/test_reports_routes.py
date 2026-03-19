"""Tests for P2: Report routes."""
import pytest
from unittest.mock import patch
from fastapi import FastAPI
from fastapi.testclient import TestClient

from app.api.routes import reports
from app.api.exception_handlers import register_exception_handlers


@pytest.fixture
def mock_user():
    return type("User", (), {"id": 1, "user_id": 1, "username": "testuser"})()


@pytest.fixture
def client(mock_user):
    test_app = FastAPI()
    register_exception_handlers(test_app)
    test_app.include_router(reports.router, prefix="/api/v1")

    async def override_auth():
        return mock_user

    test_app.dependency_overrides[reports.get_current_user] = override_auth
    return TestClient(test_app)


class TestReports:
    @patch("app.api.routes.reports.ReportDao")
    def test_list_reports(self, MockDao, client):
        instance = MockDao.return_value
        instance.list_by_user.return_value = (
            [{"id": 1, "title": "Daily Report", "report_type": "daily"}], 1
        )
        resp = client.get("/api/v1/reports")
        assert resp.status_code == 200
        assert resp.json()["meta"]["total"] == 1

    @patch("app.api.routes.reports.ReportDao")
    def test_list_reports_with_type_filter(self, MockDao, client):
        instance = MockDao.return_value
        instance.list_by_user.return_value = ([], 0)
        resp = client.get("/api/v1/reports?report_type=weekly")
        assert resp.status_code == 200
        instance.list_by_user.assert_called_once_with(1, report_type="weekly", page=1, page_size=20)

    @patch("app.api.routes.reports.ReportDao")
    def test_get_report(self, MockDao, client):
        instance = MockDao.return_value
        instance.get_by_id.return_value = {"id": 1, "title": "Daily Report", "content_json": {}}
        resp = client.get("/api/v1/reports/1")
        assert resp.status_code == 200
        assert resp.json()["title"] == "Daily Report"

    @patch("app.api.routes.reports.ReportDao")
    def test_get_report_not_found(self, MockDao, client):
        instance = MockDao.return_value
        instance.get_by_id.return_value = None
        resp = client.get("/api/v1/reports/999")
        assert resp.status_code == 404

    @patch("app.api.routes.reports.ReportDao")
    def test_generate_report(self, MockDao, client):
        instance = MockDao.return_value
        instance.create.return_value = 1
        resp = client.post("/api/v1/reports", json={
            "report_type": "daily", "title": "Test Report",
            "content_json": {"summary": "test"}
        })
        assert resp.status_code == 201
        assert resp.json()["id"] == 1

    @patch("app.api.routes.reports.ReportDao")
    def test_generate_report_default_title(self, MockDao, client):
        instance = MockDao.return_value
        instance.create.return_value = 2
        resp = client.post("/api/v1/reports", json={"report_type": "weekly"})
        assert resp.status_code == 201
        instance.create.assert_called_once()
        call_kwargs = instance.create.call_args
        assert "Weekly Report" in str(call_kwargs)

    @patch("app.api.routes.reports.ReportDao")
    def test_generate_report_invalid_type(self, MockDao, client):
        resp = client.post("/api/v1/reports", json={"report_type": "invalid"})
        assert resp.status_code == 400

