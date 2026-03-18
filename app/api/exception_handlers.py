"""
Global exception handlers for FastAPI.

Issue #16: Registers exception handlers that convert all errors
into the standardized ``{"error": {"code", "message", "detail"}}`` format.
"""

from fastapi import FastAPI, Request, status
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from starlette.exceptions import HTTPException as StarletteHTTPException

from app.api.errors import ErrorCode, ErrorResponse, ErrorEnvelope
from app.infrastructure.logging import get_logger

logger = get_logger(__name__)


class APIError(Exception):
    """Application-level error with a standard error code.

    Raise this from route handlers or services to produce a consistent
    JSON error response without constructing HTTPException manually.

    Usage::

        raise APIError(
            status_code=404,
            code=ErrorCode.STRATEGY_NOT_FOUND,
            message="Strategy not found",
        )
    """

    def __init__(
        self,
        status_code: int,
        code: ErrorCode | str,
        message: str,
        detail: str | None = None,
    ):
        self.status_code = status_code
        self.code = code if isinstance(code, str) else code.value
        self.message = message
        self.detail = detail
        super().__init__(message)


# ── Mapping helpers ──────────────────────────────────────────────────────

# Map common HTTP status codes to default error codes when no explicit code is given.
_STATUS_TO_CODE: dict[int, str] = {
    400: ErrorCode.BAD_REQUEST.value,
    401: ErrorCode.AUTH_UNAUTHORIZED.value,
    403: ErrorCode.FORBIDDEN.value,
    404: ErrorCode.NOT_FOUND.value,
    429: ErrorCode.RATE_LIMIT_EXCEEDED.value,
    500: ErrorCode.INTERNAL_ERROR.value,
}


def _error_json(status_code: int, code: str, message: str, detail: str | None = None) -> JSONResponse:
    """Build a standardised error JSONResponse."""
    body = ErrorEnvelope(error=ErrorResponse(code=code, message=message, detail=detail))
    return JSONResponse(status_code=status_code, content=body.model_dump(exclude_none=True))


# ── Handler functions ────────────────────────────────────────────────────


async def api_error_handler(request: Request, exc: APIError) -> JSONResponse:
    """Handle ``APIError`` raised anywhere in the app."""
    return _error_json(exc.status_code, exc.code, exc.message, exc.detail)


async def http_exception_handler(request: Request, exc: StarletteHTTPException) -> JSONResponse:
    """Convert FastAPI/Starlette ``HTTPException`` into the standard format."""
    code = _STATUS_TO_CODE.get(exc.status_code, ErrorCode.INTERNAL_ERROR.value)

    # If *detail* is a dict that already looks like our format, pass through.
    detail_str: str | None = None
    if isinstance(exc.detail, dict):
        code = exc.detail.get("code", code)
        message = exc.detail.get("message", str(exc.detail))
        detail_str = exc.detail.get("detail")
    else:
        message = str(exc.detail) if exc.detail else "An error occurred"

    return _error_json(exc.status_code, code, message, detail_str)


async def validation_error_handler(request: Request, exc: RequestValidationError) -> JSONResponse:
    """Convert Pydantic / request validation errors."""
    errors = exc.errors()
    # Build a human-readable summary
    fields = []
    for e in errors:
        loc = " → ".join(str(x) for x in e.get("loc", []))
        fields.append(f"{loc}: {e.get('msg', '')}")
    detail = "; ".join(fields) if fields else None

    return _error_json(
        status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
        code=ErrorCode.VALIDATION_ERROR.value,
        message="Request validation failed",
        detail=detail,
    )


async def generic_exception_handler(request: Request, exc: Exception) -> JSONResponse:
    """Catch-all for unhandled exceptions — return 500 with safe message."""
    logger.error(f"Unhandled exception on {request.method} {request.url.path}: {exc}", exc_info=True)
    return _error_json(
        status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
        code=ErrorCode.INTERNAL_ERROR.value,
        message="Internal server error",
    )


# ── Registration helper ─────────────────────────────────────────────────


def register_exception_handlers(app: FastAPI) -> None:
    """Register all global exception handlers on the FastAPI app."""
    app.add_exception_handler(APIError, api_error_handler)
    app.add_exception_handler(StarletteHTTPException, http_exception_handler)
    app.add_exception_handler(RequestValidationError, validation_error_handler)
    app.add_exception_handler(Exception, generic_exception_handler)
