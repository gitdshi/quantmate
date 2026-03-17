"""Authentication utilities and dependencies.

Moved from `app.api.middleware.auth` to `app.api.services.auth_service` to keep
auth-related helpers alongside other API services.
"""
from datetime import datetime, timedelta
from typing import Optional
import jwt
from passlib.context import CryptContext
from fastapi import Depends, HTTPException, status
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials

from app.infrastructure.config import get_settings
from app.api.models.user import TokenData
from app.api.errors import ErrorCode
from app.api.exception_handlers import APIError

settings = get_settings()
pwd_context = CryptContext(schemes=["argon2", "bcrypt_sha256", "bcrypt"], deprecated="auto")
security = HTTPBearer()


def verify_password(plain_password: str, hashed_password: str) -> bool:
    """Verify a password against its hash."""
    return pwd_context.verify(plain_password, hashed_password)


def get_password_hash(password: str) -> str:
    """Hash a password using a safe scheme that pre-hashes long passwords.

    Prefer Argon2 (installed via argon2-cffi) which has no 72-byte limit.
    """
    return pwd_context.hash(password, scheme="argon2")


def create_access_token(user_id: int, username: str, expires_delta: Optional[timedelta] = None, must_change_password: bool = False) -> str:
    """Create a JWT access token."""
    if expires_delta:
        expire = datetime.utcnow() + expires_delta
    else:
        expire = datetime.utcnow() + timedelta(minutes=settings.access_token_expire_minutes)

    payload = {
        "user_id": user_id,
        "username": username,
        "exp": expire,
        "type": "access",
        "must_change_password": must_change_password,
    }
    return jwt.encode(payload, settings.secret_key, algorithm=settings.algorithm)


def create_refresh_token(user_id: int, username: str, must_change_password: bool = False) -> str:
    """Create a JWT refresh token."""
    expire = datetime.utcnow() + timedelta(days=settings.refresh_token_expire_days)
    payload = {
        "user_id": user_id,
        "username": username,
        "exp": expire,
        "type": "refresh",
        "must_change_password": must_change_password,
    }
    return jwt.encode(payload, settings.secret_key, algorithm=settings.algorithm)


def decode_token(token: str) -> Optional[TokenData]:
    """Decode and validate a JWT token."""
    try:
        payload = jwt.decode(token, settings.secret_key, algorithms=[settings.algorithm])
        return TokenData(
            user_id=payload["user_id"],
            username=payload["username"],
            exp=datetime.fromtimestamp(payload["exp"]),
            must_change_password=payload.get("must_change_password", False)
        )
    except jwt.ExpiredSignatureError:
        return None
    except jwt.InvalidTokenError:
        return None


async def get_current_user(credentials: HTTPAuthorizationCredentials = Depends(security)) -> TokenData:
    """Get current user from JWT token."""
    token = credentials.credentials
    token_data = decode_token(token)
    
    if token_data is None:
        raise APIError(
            status_code=status.HTTP_401_UNAUTHORIZED,
            code=ErrorCode.AUTH_INVALID_TOKEN,
            message="Invalid or expired token",
        )
    
    return token_data


async def get_current_user_optional(
    credentials: Optional[HTTPAuthorizationCredentials] = Depends(HTTPBearer(auto_error=False))
) -> Optional[TokenData]:
    """Get current user if token provided, otherwise return None."""
    if credentials is None:
        return None
    
    token_data = decode_token(credentials.credentials)
    return token_data
