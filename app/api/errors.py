"""
Standardized error codes and error response models for QuantMate API.

Issue #16: 统一错误响应格式 (Standardized Error Codes)

All API error responses follow the format:
{
    "error": {
        "code": "AUTH_INVALID_TOKEN",
        "message": "Human-readable message",
        "detail": "Optional technical detail"
    }
}
"""
from enum import Enum
from typing import Optional
from pydantic import BaseModel


class ErrorCode(str, Enum):
    """Standardized error codes grouped by module."""

    # ── AUTH ──────────────────────────────────────────────────────────────
    AUTH_INVALID_CREDENTIALS = "AUTH_INVALID_CREDENTIALS"
    AUTH_INVALID_TOKEN = "AUTH_INVALID_TOKEN"
    AUTH_TOKEN_EXPIRED = "AUTH_TOKEN_EXPIRED"
    AUTH_ACCOUNT_DISABLED = "AUTH_ACCOUNT_DISABLED"
    AUTH_PASSWORD_CHANGE_REQUIRED = "AUTH_PASSWORD_CHANGE_REQUIRED"
    AUTH_REGISTRATION_FAILED = "AUTH_REGISTRATION_FAILED"
    AUTH_USER_EXISTS = "AUTH_USER_EXISTS"
    AUTH_UNAUTHORIZED = "AUTH_UNAUTHORIZED"
    AUTH_ACCOUNT_LOCKED = "AUTH_ACCOUNT_LOCKED"

    # ── DATA ─────────────────────────────────────────────────────────────
    DATA_NOT_FOUND = "DATA_NOT_FOUND"
    DATA_FETCH_FAILED = "DATA_FETCH_FAILED"
    DATA_INVALID_SYMBOL = "DATA_INVALID_SYMBOL"
    DATA_INVALID_DATE_RANGE = "DATA_INVALID_DATE_RANGE"
    DATA_SOURCE_UNAVAILABLE = "DATA_SOURCE_UNAVAILABLE"

    # ── STRATEGY ─────────────────────────────────────────────────────────
    STRATEGY_NOT_FOUND = "STRATEGY_NOT_FOUND"
    STRATEGY_VALIDATION_FAILED = "STRATEGY_VALIDATION_FAILED"
    STRATEGY_NAME_EXISTS = "STRATEGY_NAME_EXISTS"
    STRATEGY_CODE_ERROR = "STRATEGY_CODE_ERROR"

    # ── BACKTEST ─────────────────────────────────────────────────────────
    BACKTEST_NOT_FOUND = "BACKTEST_NOT_FOUND"
    BACKTEST_FAILED = "BACKTEST_FAILED"
    BACKTEST_INVALID_PARAMS = "BACKTEST_INVALID_PARAMS"
    BACKTEST_QUEUE_FULL = "BACKTEST_QUEUE_FULL"

    # ── PORTFOLIO ────────────────────────────────────────────────────────
    PORTFOLIO_NOT_FOUND = "PORTFOLIO_NOT_FOUND"
    PORTFOLIO_INSUFFICIENT_FUNDS = "PORTFOLIO_INSUFFICIENT_FUNDS"
    PORTFOLIO_INSUFFICIENT_POSITION = "PORTFOLIO_INSUFFICIENT_POSITION"

    # ── QUEUE / JOBS ─────────────────────────────────────────────────────
    JOB_NOT_FOUND = "JOB_NOT_FOUND"
    JOB_CANCEL_FAILED = "JOB_CANCEL_FAILED"

    # ── GENERAL ──────────────────────────────────────────────────────────
    VALIDATION_ERROR = "VALIDATION_ERROR"
    NOT_FOUND = "NOT_FOUND"
    RATE_LIMIT_EXCEEDED = "RATE_LIMIT_EXCEEDED"
    INTERNAL_ERROR = "INTERNAL_ERROR"
    BAD_REQUEST = "BAD_REQUEST"
    FORBIDDEN = "FORBIDDEN"


class ErrorResponse(BaseModel):
    """Standardized error response body."""
    code: str
    message: str
    detail: Optional[str] = None


class ErrorEnvelope(BaseModel):
    """Top-level error envelope: ``{"error": {...}}``."""
    error: ErrorResponse
