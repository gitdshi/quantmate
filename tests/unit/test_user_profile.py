"""Tests for Issue #8: User Profile API."""
import pytest
from datetime import datetime, timedelta
from unittest.mock import MagicMock, patch
from fastapi import FastAPI, Depends
from fastapi.testclient import TestClient

from app.api.routes.auth import router
from app.api.services.auth_service import get_current_user
from app.api.models.user import TokenData, VALID_TIMEZONES, VALID_LANGUAGES
from app.api.exception_handlers import register_exception_handlers


@pytest.fixture
def client():
    app = FastAPI()
    register_exception_handlers(app)

    future_exp = datetime.utcnow() + timedelta(hours=1)
    test_user = TokenData(user_id=42, username="testuser", exp=future_exp)

    app.dependency_overrides[get_current_user] = lambda: test_user
    app.include_router(router, prefix="/api/v1")
    return TestClient(app, raise_server_exceptions=False)


class TestGetProfile:
    def test_returns_default_when_no_profile(self, client):
        with patch("app.domains.auth.dao.user_profile_dao.UserProfileDao") as MockDao:
            MockDao.return_value.get.return_value = None
            resp = client.get("/api/v1/auth/profile")
        assert resp.status_code == 200
        body = resp.json()
        assert body["user_id"] == 42
        assert body["timezone"] == "Asia/Shanghai"
        assert body["language"] == "zh-CN"

    def test_returns_existing_profile(self, client):
        profile = {
            "user_id": 42,
            "display_name": "Test User",
            "avatar_url": None,
            "phone": "+86-138-0000-0000",
            "timezone": "US/Eastern",
            "language": "en-US",
            "bio": "hello",
            "created_at": datetime(2025, 1, 1),
            "updated_at": datetime(2025, 1, 2),
        }
        with patch("app.domains.auth.dao.user_profile_dao.UserProfileDao") as MockDao:
            MockDao.return_value.get.return_value = profile
            resp = client.get("/api/v1/auth/profile")
        assert resp.status_code == 200
        body = resp.json()
        assert body["display_name"] == "Test User"
        assert body["timezone"] == "US/Eastern"


class TestUpdateProfile:
    def test_update_display_name(self, client):
        result = {
            "user_id": 42,
            "display_name": "New Name",
            "avatar_url": None,
            "phone": None,
            "timezone": "Asia/Shanghai",
            "language": "zh-CN",
            "bio": None,
            "created_at": datetime(2025, 1, 1),
            "updated_at": datetime(2025, 1, 2),
        }
        with patch("app.domains.auth.dao.user_profile_dao.UserProfileDao") as MockDao:
            MockDao.return_value.upsert.return_value = result
            resp = client.put("/api/v1/auth/profile", json={"display_name": "New Name"})
        assert resp.status_code == 200
        assert resp.json()["display_name"] == "New Name"

    def test_invalid_timezone_rejected(self, client):
        resp = client.put("/api/v1/auth/profile", json={"timezone": "Mars/Olympus"})
        assert resp.status_code == 400
        assert "VALIDATION_ERROR" in resp.text

    def test_invalid_language_rejected(self, client):
        resp = client.put("/api/v1/auth/profile", json={"language": "klingon"})
        assert resp.status_code == 400
        assert "VALIDATION_ERROR" in resp.text

    def test_valid_timezone_accepted(self, client):
        result = {
            "user_id": 42,
            "display_name": None,
            "avatar_url": None,
            "phone": None,
            "timezone": "UTC",
            "language": "zh-CN",
            "bio": None,
            "created_at": datetime(2025, 1, 1),
            "updated_at": datetime(2025, 1, 2),
        }
        with patch("app.domains.auth.dao.user_profile_dao.UserProfileDao") as MockDao:
            MockDao.return_value.upsert.return_value = result
            resp = client.put("/api/v1/auth/profile", json={"timezone": "UTC"})
        assert resp.status_code == 200

    def test_phone_validation(self, client):
        """Phone with invalid format should be rejected at pydantic level."""
        resp = client.put("/api/v1/auth/profile", json={"phone": "notaphone!"})
        assert resp.status_code == 422 or resp.status_code == 400


class TestProfileModels:
    def test_valid_timezones_nonempty(self):
        assert len(VALID_TIMEZONES) > 5

    def test_valid_languages_nonempty(self):
        assert len(VALID_LANGUAGES) >= 3
