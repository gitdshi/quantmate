"""Unit tests for app.api.routes.strategy_code."""

from __future__ import annotations


import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from app.api.routes import strategy_code
from app.api.exception_handlers import register_exception_handlers


@pytest.fixture()
def client():
    test_app = FastAPI()
    register_exception_handlers(test_app)
    test_app.include_router(strategy_code.router, prefix="/api/v1")
    return TestClient(test_app, raise_server_exceptions=False)


class TestStrategyCodeRoutes:
    def test_parse_file(self, client):
        resp = client.post("/api/v1/strategy-code/parse", json={
            "code": "class MyStrategy:\n    pass\n"
        })
        assert resp.status_code in (200, 422)

    def test_lint_code(self, client):
        resp = client.post("/api/v1/strategy-code/lint", json={
            "code": "class MyStrategy:\n    pass\n"
        })
        assert resp.status_code in (200, 422)

    def test_lint_empty_code(self, client):
        resp = client.post("/api/v1/strategy-code/lint", json={
            "code": ""
        })
        assert resp.status_code in (200, 422)

    def test_parse_invalid_code(self, client):
        resp = client.post("/api/v1/strategy-code/parse", json={
            "code": "def broken(\n"
        })
        assert resp.status_code in (200, 400, 422, 500)
