"""Unit tests for app.api.services.auth_service (JWT + password hashing)."""

from __future__ import annotations

import os
from datetime import timedelta
from unittest.mock import MagicMock

import pytest

os.environ.setdefault("SECRET_KEY", "test-secret-key-for-unit-tests-only-0123456789abcdef")
os.environ.setdefault("MYSQL_PASSWORD", "test")

from app.api.services.auth_service import (
    create_access_token,
    create_refresh_token,
    decode_token,
    get_password_hash,
    verify_password,
)


# ── password hashing ─────────────────────────────────────────────

def test_hash_and_verify_password():
    hashed = get_password_hash("hello123")
    assert verify_password("hello123", hashed)


def test_verify_wrong_password():
    hashed = get_password_hash("correct")
    assert not verify_password("wrong", hashed)


def test_hash_is_different_each_time():
    h1 = get_password_hash("same")
    h2 = get_password_hash("same")
    assert h1 != h2  # salted


# ── access token ──────────────────────────────────────────────────

def test_create_and_decode_access_token():
    tok = create_access_token(42, "alice")
    data = decode_token(tok)
    assert data is not None
    assert data.user_id == 42
    assert data.username == "alice"
    assert data.must_change_password is False


def test_access_token_with_custom_expiry():
    tok = create_access_token(1, "bob", expires_delta=timedelta(minutes=5))
    data = decode_token(tok)
    assert data is not None
    assert data.user_id == 1


def test_access_token_must_change_password():
    tok = create_access_token(1, "user", must_change_password=True)
    data = decode_token(tok)
    assert data.must_change_password is True


# ── refresh token ─────────────────────────────────────────────────

def test_create_and_decode_refresh_token():
    tok = create_refresh_token(7, "carol")
    data = decode_token(tok)
    assert data is not None
    assert data.user_id == 7
    assert data.username == "carol"


# ── expired / invalid tokens ─────────────────────────────────────

def test_decode_expired_token():
    tok = create_access_token(1, "u", expires_delta=timedelta(seconds=-1))
    data = decode_token(tok)
    assert data is None


def test_decode_invalid_token():
    data = decode_token("this.is.not.a.jwt")
    assert data is None


def test_decode_empty_token():
    data = decode_token("")
    assert data is None


# ── get_current_user ──────────────────────────────────────────────

@pytest.mark.asyncio
async def test_get_current_user_valid():
    from app.api.services.auth_service import get_current_user

    tok = create_access_token(99, "admin")
    cred = MagicMock()
    cred.credentials = tok
    result = await get_current_user(cred)
    assert result.user_id == 99


@pytest.mark.asyncio
async def test_get_current_user_invalid():
    from app.api.services.auth_service import get_current_user
    from app.api.exception_handlers import APIError

    cred = MagicMock()
    cred.credentials = "bad"
    with pytest.raises(APIError):
        await get_current_user(cred)


# ── get_current_user_optional ─────────────────────────────────────

@pytest.mark.asyncio
async def test_get_current_user_optional_none():
    from app.api.services.auth_service import get_current_user_optional

    result = await get_current_user_optional(None)
    assert result is None


@pytest.mark.asyncio
async def test_get_current_user_optional_valid():
    from app.api.services.auth_service import get_current_user_optional

    tok = create_access_token(5, "u")
    cred = MagicMock()
    cred.credentials = tok
    result = await get_current_user_optional(cred)
    assert result.user_id == 5


@pytest.mark.asyncio
async def test_get_current_user_optional_invalid():
    from app.api.services.auth_service import get_current_user_optional

    cred = MagicMock()
    cred.credentials = "bad"
    result = await get_current_user_optional(cred)
    assert result is None
