"""Unit tests for app.api.routes.composite."""

from __future__ import annotations

from unittest.mock import patch

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from app.api.routes import composite
from app.api.exception_handlers import register_exception_handlers

# Patch the class where it is USED, not where it is defined
_COMP_SVC = "app.api.routes.composite.CompositeStrategyService"


@pytest.fixture()
def mock_user():
    return type("User", (), {"id": 1, "user_id": 1, "username": "test", "sub": 1})()


@pytest.fixture()
def comp_client(mock_user):
    test_app = FastAPI()
    register_exception_handlers(test_app)
    test_app.include_router(composite.comp_router, prefix="/api/v1")
    test_app.include_router(composite.composite_router, prefix="/api/v1")
    test_app.include_router(composite.backtest_router, prefix="/api/v1")

    async def override_auth():
        return mock_user

    test_app.dependency_overrides[composite.get_current_user] = override_auth
    for route in test_app.routes:
        if hasattr(route, "dependencies"):
            route.dependencies = []
    return TestClient(test_app, raise_server_exceptions=False)


class TestComponentRoutes:
    @patch(_COMP_SVC)
    def test_list_components(self, MockSvc, comp_client):
        svc = MockSvc.return_value
        svc.count_components.return_value = 1
        svc.list_components_paginated.return_value = [
            {"id": 1, "name": "MA", "layer": "trading", "sub_type": "entry",
             "description": None, "version": 1, "is_active": True,
             "created_at": "2024-01-01T00:00:00", "updated_at": "2024-01-01T00:00:00"}
        ]
        resp = comp_client.get("/api/v1/strategy-components")
        assert resp.status_code == 200

    @patch(_COMP_SVC)
    def test_create_component(self, MockSvc, comp_client):
        svc = MockSvc.return_value
        svc.create_component.return_value = {
            "id": 1, "name": "MA Cross", "layer": "trading", "sub_type": "entry",
            "user_id": 1, "description": None, "code": "pass", "config": None,
            "parameters": None, "version": 1, "is_active": True,
            "created_at": "2024-01-01T00:00:00", "updated_at": None,
        }
        resp = comp_client.post("/api/v1/strategy-components", json={
            "name": "MA Cross", "layer": "trading", "sub_type": "entry", "code": "pass"
        })
        assert resp.status_code in (200, 201, 422)

    @patch(_COMP_SVC)
    def test_get_component(self, MockSvc, comp_client):
        svc = MockSvc.return_value
        svc.get_component.return_value = {
            "id": 1, "name": "MA", "layer": "trading", "sub_type": "entry",
            "user_id": 1, "description": None, "code": "pass", "config": None,
            "parameters": None, "version": 1, "is_active": True,
            "created_at": "2024-01-01T00:00:00", "updated_at": None,
        }
        resp = comp_client.get("/api/v1/strategy-components/1")
        assert resp.status_code in (200, 404)

    @patch(_COMP_SVC)
    def test_delete_component(self, MockSvc, comp_client):
        MockSvc.return_value.delete_component.return_value = True
        resp = comp_client.delete("/api/v1/strategy-components/1")
        assert resp.status_code in (200, 204)


class TestCompositeRoutes:
    @patch(_COMP_SVC)
    def test_list_composites(self, MockSvc, comp_client):
        svc = MockSvc.return_value
        svc.count_composites.return_value = 1
        svc.list_composites_paginated.return_value = [
            {"id": 1, "name": "Combo", "user_id": 1, "description": None,
             "is_active": True, "version": 1,
             "created_at": "2024-01-01T00:00:00", "updated_at": "2024-01-01T00:00:00"}
        ]
        resp = comp_client.get("/api/v1/composite-strategies")
        assert resp.status_code == 200

    @patch(_COMP_SVC)
    def test_create_composite(self, MockSvc, comp_client):
        svc = MockSvc.return_value
        svc.create_composite.return_value = {
            "id": 1, "name": "Combo 1", "user_id": 1, "description": None,
            "is_active": True, "version": 1,
            "created_at": "2024-01-01T00:00:00", "updated_at": None,
        }
        resp = comp_client.post("/api/v1/composite-strategies", json={
            "name": "Combo 1",
        })
        assert resp.status_code in (200, 201, 422)

    @patch(_COMP_SVC)
    def test_get_composite(self, MockSvc, comp_client):
        svc = MockSvc.return_value
        svc.get_composite_detail.return_value = {
            "id": 1, "name": "Combo", "user_id": 1, "description": "test",
            "is_active": True, "version": 1,
            "portfolio_config": {}, "market_constraints": {},
            "execution_mode": "backtest",
            "components": [],
            "created_at": "2024-01-01T00:00:00", "updated_at": "2024-01-01T00:00:00",
        }
        resp = comp_client.get("/api/v1/composite-strategies/1")
        assert resp.status_code in (200, 404)

    @patch(_COMP_SVC)
    def test_delete_composite(self, MockSvc, comp_client):
        MockSvc.return_value.delete_composite.return_value = True
        resp = comp_client.delete("/api/v1/composite-strategies/1")
        assert resp.status_code in (200, 204)

    @patch(_COMP_SVC)
    def test_update_composite(self, MockSvc, comp_client):
        svc = MockSvc.return_value
        svc.update_composite.return_value = {
            "id": 1, "name": "Updated", "user_id": 1, "description": None,
            "is_active": True, "version": 1,
            "created_at": "2024-01-01T00:00:00", "updated_at": None,
        }
        resp = comp_client.put("/api/v1/composite-strategies/1", json={"name": "Updated"})
        assert resp.status_code in (200, 422)
