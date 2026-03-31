"""Authentication route coverage for enriched user payloads."""

from datetime import datetime, timedelta
from unittest.mock import patch

from fastapi import FastAPI
from fastapi.testclient import TestClient

from app.api.exception_handlers import register_exception_handlers
from app.api.models.user import TokenData
from app.api.routes import auth


future_exp = datetime.utcnow() + timedelta(hours=1)


def test_auth_me_returns_role_and_permissions():
    app = FastAPI()
    register_exception_handlers(app)
    app.include_router(auth.router, prefix="/api/v1")

    async def override_current_user():
        return TokenData(user_id=1, username="admin", exp=future_exp)

    app.dependency_overrides[auth.get_current_user] = override_current_user
    client = TestClient(app, raise_server_exceptions=False)

    enriched_user = {
        "id": 1,
        "username": "admin",
        "email": "admin@example.com",
        "is_active": True,
        "created_at": datetime(2026, 1, 1),
        "role": "admin",
        "primary_role": "admin",
        "permissions": ["system.manage", "account.manage"],
    }

    with patch("app.api.routes.auth.AuthService") as MockService:
        MockService.return_value.me.return_value = enriched_user
        resp = client.get("/api/v1/auth/me")

    assert resp.status_code == 200
    body = resp.json()
    assert body["role"] == "admin"
    assert body["primary_role"] == "admin"
    assert body["permissions"] == ["system.manage", "account.manage"]
