"""API Key management routes (P2 Issue: API Key Management)."""

import hashlib
import hmac
import secrets
from datetime import datetime
from typing import Optional

from fastapi import APIRouter, Depends, status
from pydantic import BaseModel

from app.api.services.auth_service import get_current_user
from app.api.errors import ErrorCode
from app.api.exception_handlers import APIError
from app.domains.auth.dao.api_key_dao import ApiKeyDao
from app.infrastructure.config import get_settings

router = APIRouter(prefix="/auth/api-keys", tags=["API Keys"])

MAX_KEYS_PER_USER = 5
_settings = get_settings()


def _hash_secret(secret: str) -> str:
    """Hash an API key secret using HMAC-SHA256 with the server key."""
    return hmac.new(
        _settings.secret_key.encode(),
        secret.encode(),
        hashlib.sha256,
    ).hexdigest()


class ApiKeyCreateRequest(BaseModel):
    name: str
    permissions: Optional[list[str]] = None
    expires_at: Optional[datetime] = None
    ip_whitelist: Optional[list[str]] = None
    rate_limit: int = 60


class ApiKeyCreateResponse(BaseModel):
    id: int
    key_id: str
    secret: str  # Only shown once at creation
    name: str
    created_at: datetime


class ApiKeyListItem(BaseModel):
    id: int
    key_id: str
    name: str
    permissions: Optional[list] = None
    expires_at: Optional[datetime] = None
    ip_whitelist: Optional[list] = None
    rate_limit: int
    is_active: bool
    created_at: datetime
    last_used_at: Optional[datetime] = None


@router.get("/", response_model=list[ApiKeyListItem])
async def list_api_keys(current_user: dict = Depends(get_current_user)):
    """List all API keys for the current user."""
    dao = ApiKeyDao()
    keys = dao.list_by_user(current_user["id"])
    return keys


@router.post("/", response_model=ApiKeyCreateResponse, status_code=status.HTTP_201_CREATED)
async def create_api_key(req: ApiKeyCreateRequest, current_user: dict = Depends(get_current_user)):
    """Create a new API key. The secret is only shown once."""
    dao = ApiKeyDao()

    # Check limit
    count = dao.count_by_user(current_user["id"])
    if count >= MAX_KEYS_PER_USER:
        raise APIError(
            status_code=status.HTTP_400_BAD_REQUEST,
            code=ErrorCode.VALIDATION_ERROR,
            message=f"Maximum {MAX_KEYS_PER_USER} API keys per user",
        )

    key_id = f"qm_{secrets.token_hex(16)}"
    secret = secrets.token_hex(32)
    secret_hash = _hash_secret(secret)

    row_id = dao.create(
        user_id=current_user["id"],
        key_id=key_id,
        secret_hash=secret_hash,
        name=req.name,
        permissions=req.permissions,
        expires_at=req.expires_at,
        ip_whitelist=req.ip_whitelist,
        rate_limit=req.rate_limit,
    )

    return ApiKeyCreateResponse(
        id=row_id,
        key_id=key_id,
        secret=secret,
        name=req.name,
        created_at=datetime.utcnow(),
    )


@router.delete("/{api_key_id}")
async def revoke_api_key(api_key_id: int, current_user: dict = Depends(get_current_user)):
    """Revoke an API key."""
    dao = ApiKeyDao()
    if not dao.revoke(api_key_id, current_user["id"]):
        raise APIError(
            status_code=status.HTTP_404_NOT_FOUND,
            code=ErrorCode.NOT_FOUND,
            message="API key not found",
        )
    return {"message": "API key revoked"}
