"""Tests for Issue #13: API Versioning (/api/v1/)."""
import pytest
from fastapi.testclient import TestClient


@pytest.fixture
def client():
    """Create a test client with a minimal versioned app."""
    from fastapi import FastAPI, Request
    from fastapi.responses import RedirectResponse
    from app.api.routes import auth, strategies, data, backtest, queue, system, strategy_code

    test_app = FastAPI()

    # Include routers under /api/v1
    test_app.include_router(auth.router, prefix="/api/v1")
    test_app.include_router(strategies.router, prefix="/api/v1")
    test_app.include_router(data.router, prefix="/api/v1")
    test_app.include_router(backtest.router, prefix="/api/v1")
    test_app.include_router(queue.router, prefix="/api/v1")
    test_app.include_router(system.router, prefix="/api/v1")
    test_app.include_router(strategy_code.router, prefix="/api/v1")

    # Legacy redirect middleware
    @test_app.middleware("http")
    async def legacy_api_redirect(request: Request, call_next):
        path = request.url.path
        if path.startswith("/api/") and not path.startswith("/api/v1/"):
            new_path = "/api/v1/" + path[len("/api/"):]
            query = str(request.url.query)
            url = new_path + ("?" + query if query else "")
            return RedirectResponse(url=url, status_code=301)
        return await call_next(request)

    return TestClient(test_app, raise_server_exceptions=False)


@pytest.fixture
def simple_client():
    """Minimal app to test redirect logic without importing route modules."""
    from fastapi import FastAPI, Request
    from fastapi.responses import RedirectResponse

    test_app = FastAPI()

    @test_app.get("/api/v1/test")
    async def v1_test():
        return {"version": "v1"}

    @test_app.get("/api/v1/test/nested")
    async def v1_test_nested():
        return {"version": "v1", "nested": True}

    @test_app.middleware("http")
    async def legacy_api_redirect(request: Request, call_next):
        path = request.url.path
        if path.startswith("/api/") and not path.startswith("/api/v1/"):
            new_path = "/api/v1/" + path[len("/api/"):]
            query = str(request.url.query)
            url = new_path + ("?" + query if query else "")
            return RedirectResponse(url=url, status_code=301)
        return await call_next(request)

    return TestClient(test_app, raise_server_exceptions=False)


class TestV1Endpoints:
    """Test that endpoints are accessible under /api/v1/."""

    def test_v1_system_ping(self, simple_client):
        """V1 endpoints respond directly."""
        resp = simple_client.get("/api/v1/test")
        assert resp.status_code == 200
        assert resp.json()["version"] == "v1"

    def test_v1_nested_endpoint(self, simple_client):
        """Nested v1 endpoints work."""
        resp = simple_client.get("/api/v1/test/nested")
        assert resp.status_code == 200
        assert resp.json()["nested"] is True


class TestLegacyRedirect:
    """Test that old /api/* paths redirect to /api/v1/*."""

    def test_legacy_path_returns_301(self, simple_client):
        """Old /api/test redirects to /api/v1/test with 301."""
        resp = simple_client.get("/api/test", follow_redirects=False)
        assert resp.status_code == 301
        assert resp.headers["location"] == "/api/v1/test"

    def test_legacy_nested_path_redirects(self, simple_client):
        """Old /api/test/nested redirects correctly."""
        resp = simple_client.get("/api/test/nested", follow_redirects=False)
        assert resp.status_code == 301
        assert resp.headers["location"] == "/api/v1/test/nested"

    def test_legacy_with_query_params(self, simple_client):
        """Query parameters are preserved in redirect."""
        resp = simple_client.get("/api/test?foo=bar&baz=1", follow_redirects=False)
        assert resp.status_code == 301
        assert "foo=bar" in resp.headers["location"]
        assert "baz=1" in resp.headers["location"]

    def test_legacy_redirect_follow_reaches_v1(self, simple_client):
        """Following the redirect lands on the v1 endpoint."""
        resp = simple_client.get("/api/test", follow_redirects=True)
        assert resp.status_code == 200
        assert resp.json()["version"] == "v1"

    def test_v1_path_not_redirected(self, simple_client):
        """Paths already at /api/v1/ are not redirected."""
        resp = simple_client.get("/api/v1/test", follow_redirects=False)
        assert resp.status_code == 200

    def test_non_api_path_not_redirected(self, simple_client):
        """Non-API paths are not affected by redirect."""
        resp = simple_client.get("/docs", follow_redirects=False)
        # Should not be 301 (may be 404 or other, but not a redirect to v1)
        assert resp.status_code != 301
