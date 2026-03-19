"""
Unit tests for standardized error codes and exception handlers.

Issue #16: 统一错误响应格式 (Standardized Error Codes)
"""
import pytest
from fastapi import FastAPI, status
from fastapi.testclient import TestClient

from app.api.errors import ErrorCode, ErrorResponse, ErrorEnvelope
from app.api.exception_handlers import (
    APIError,
    register_exception_handlers,
)


# ─── Fixtures ────────────────────────────────────────────────────────────

@pytest.fixture
def test_app():
    """Create a minimal FastAPI app with exception handlers registered."""
    app = FastAPI()
    register_exception_handlers(app)

    @app.get("/ok")
    async def ok():
        return {"data": "hello"}

    @app.get("/raise-api-error")
    async def raise_api_error():
        raise APIError(
            status_code=404,
            code=ErrorCode.STRATEGY_NOT_FOUND,
            message="Strategy not found",
            detail="strategy_id=999",
        )

    @app.get("/raise-api-error-no-detail")
    async def raise_api_error_no_detail():
        raise APIError(
            status_code=400,
            code=ErrorCode.BAD_REQUEST,
            message="Bad request",
        )

    @app.get("/raise-http-exception")
    async def raise_http():
        from fastapi import HTTPException
        raise HTTPException(status_code=401, detail="Token expired")

    @app.get("/raise-validation")
    async def raise_validation(required_param: int):
        """Missing required_param triggers RequestValidationError."""
        return {"value": required_param}

    @app.get("/raise-unhandled")
    async def raise_unhandled():
        raise RuntimeError("Something went wrong")

    @app.get("/raise-403")
    async def raise_403():
        raise APIError(
            status_code=403,
            code=ErrorCode.AUTH_PASSWORD_CHANGE_REQUIRED,
            message="Password change required",
        )

    return app


@pytest.fixture
def client(test_app):
    return TestClient(test_app, raise_server_exceptions=False)


# ─── ErrorCode enum tests ───────────────────────────────────────────────

class TestErrorCode:
    """Test the ErrorCode enum values and structure."""

    def test_error_code_is_string(self):
        assert ErrorCode.AUTH_INVALID_TOKEN == "AUTH_INVALID_TOKEN"

    def test_error_code_has_auth_codes(self):
        auth_codes = [c for c in ErrorCode if c.value.startswith("AUTH_")]
        assert len(auth_codes) >= 5

    def test_error_code_has_data_codes(self):
        data_codes = [c for c in ErrorCode if c.value.startswith("DATA_")]
        assert len(data_codes) >= 3

    def test_error_code_has_strategy_codes(self):
        strategy_codes = [c for c in ErrorCode if c.value.startswith("STRATEGY_")]
        assert len(strategy_codes) >= 3

    def test_error_code_has_general_codes(self):
        assert ErrorCode.VALIDATION_ERROR == "VALIDATION_ERROR"
        assert ErrorCode.NOT_FOUND == "NOT_FOUND"
        assert ErrorCode.RATE_LIMIT_EXCEEDED == "RATE_LIMIT_EXCEEDED"
        assert ErrorCode.INTERNAL_ERROR == "INTERNAL_ERROR"


# ─── ErrorResponse model tests ──────────────────────────────────────────

class TestErrorResponseModel:
    """Test Pydantic error response models."""

    def test_error_response_with_detail(self):
        resp = ErrorResponse(code="AUTH_INVALID_TOKEN", message="Token expired", detail="jwt expired at ...")
        assert resp.code == "AUTH_INVALID_TOKEN"
        assert resp.message == "Token expired"
        assert resp.detail == "jwt expired at ..."

    def test_error_response_without_detail(self):
        resp = ErrorResponse(code="NOT_FOUND", message="Not found")
        assert resp.detail is None

    def test_error_envelope_serialization(self):
        envelope = ErrorEnvelope(
            error=ErrorResponse(code="BAD_REQUEST", message="Invalid input")
        )
        d = envelope.model_dump(exclude_none=True)
        assert "error" in d
        assert d["error"]["code"] == "BAD_REQUEST"
        assert "detail" not in d["error"]


# ─── APIError exception tests ───────────────────────────────────────────

class TestAPIError:

    def test_api_error_with_enum(self):
        err = APIError(status_code=404, code=ErrorCode.NOT_FOUND, message="Resource not found")
        assert err.status_code == 404
        assert err.code == "NOT_FOUND"
        assert err.message == "Resource not found"
        assert err.detail is None

    def test_api_error_with_string(self):
        err = APIError(status_code=400, code="CUSTOM_CODE", message="custom")
        assert err.code == "CUSTOM_CODE"


# ─── Exception handler integration tests ────────────────────────────────

class TestExceptionHandlers:
    """Test that exception handlers produce the correct JSON structure."""

    def test_ok_endpoint_returns_normal(self, client):
        resp = client.get("/ok")
        assert resp.status_code == 200
        assert resp.json() == {"data": "hello"}

    def test_api_error_returns_standard_format(self, client):
        resp = client.get("/raise-api-error")
        assert resp.status_code == 404
        body = resp.json()
        assert "error" in body
        assert body["error"]["code"] == "STRATEGY_NOT_FOUND"
        assert body["error"]["message"] == "Strategy not found"
        assert body["error"]["detail"] == "strategy_id=999"

    def test_api_error_without_detail_omits_detail_field(self, client):
        resp = client.get("/raise-api-error-no-detail")
        assert resp.status_code == 400
        body = resp.json()
        assert "error" in body
        assert body["error"]["code"] == "BAD_REQUEST"
        assert "detail" not in body["error"]

    def test_http_exception_converted_to_standard_format(self, client):
        resp = client.get("/raise-http-exception")
        assert resp.status_code == 401
        body = resp.json()
        assert "error" in body
        assert body["error"]["code"] == "AUTH_UNAUTHORIZED"
        assert body["error"]["message"] == "Token expired"

    def test_validation_error_format(self, client):
        resp = client.get("/raise-validation")  # missing required_param
        assert resp.status_code == 422
        body = resp.json()
        assert "error" in body
        assert body["error"]["code"] == "VALIDATION_ERROR"
        assert body["error"]["message"] == "Request validation failed"
        assert body["error"]["detail"] is not None

    def test_unhandled_exception_returns_500(self, client):
        resp = client.get("/raise-unhandled")
        assert resp.status_code == 500
        body = resp.json()
        assert "error" in body
        assert body["error"]["code"] == "INTERNAL_ERROR"
        assert body["error"]["message"] == "Internal server error"
        # Should NOT leak internal error details
        assert "Something went wrong" not in body["error"].get("detail", "")

    def test_403_returns_standard_format(self, client):
        resp = client.get("/raise-403")
        assert resp.status_code == 403
        body = resp.json()
        assert body["error"]["code"] == "AUTH_PASSWORD_CHANGE_REQUIRED"

    def test_nonexistent_route_returns_404(self, client):
        resp = client.get("/does-not-exist")
        assert resp.status_code == 404
        body = resp.json()
        assert "error" in body
        assert body["error"]["code"] == "NOT_FOUND"
