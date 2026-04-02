"""Authentication routes."""

from fastapi import APIRouter, Depends, Request, status

from app.infrastructure.config import get_settings
from app.api.models.user import (
    UserCreate,
    UserLogin,
    User,
    Token,
    TokenData,
    PasswordChangeRequest,
    UserProfileUpdate,
    UserProfileResponse,
    VALID_TIMEZONES,
    VALID_LANGUAGES,
)
from app.api.services.auth_service import (
    get_current_user,
)
from app.api.errors import ErrorCode
from app.api.exception_handlers import APIError
from app.api import brute_force

from app.domains.auth.service import AuthService

router = APIRouter(prefix="/auth", tags=["Authentication"])
settings = get_settings()


@router.post("/register", response_model=User, status_code=status.HTTP_201_CREATED)
async def register(user_data: UserCreate):
    """Register a new user."""
    service = AuthService()
    try:
        u = service.register(user_data.username, user_data.email, user_data.password)
    except ValueError as e:
        msg = str(e)
        code = ErrorCode.AUTH_USER_EXISTS if "exists" in msg.lower() else ErrorCode.AUTH_REGISTRATION_FAILED
        raise APIError(status_code=status.HTTP_400_BAD_REQUEST, code=code, message=msg)

    return User(
        id=u["id"],
        username=u["username"],
        email=u.get("email"),
        is_active=u.get("is_active", True),
        created_at=u["created_at"],
    )


@router.post("/login", response_model=Token)
async def login(credentials: UserLogin, request: Request):
    """Login and get access token."""
    client_ip = request.headers.get("x-forwarded-for", "").split(",")[0].strip() or (
        request.client.host if request.client else None
    )

    login_id = (credentials.username or credentials.email or "").strip()
    if not login_id:
        raise APIError(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            code=ErrorCode.VALIDATION_ERROR,
            message="username or email is required",
        )

    # Check lockout
    if brute_force.is_locked(ip=client_ip, username=login_id):
        remaining = brute_force.remaining_lockout(ip=client_ip, username=login_id)
        raise APIError(
            status_code=status.HTTP_429_TOO_MANY_REQUESTS,
            code=ErrorCode.AUTH_ACCOUNT_LOCKED,
            message=f"Too many failed login attempts. Try again in {remaining // 60 + 1} minutes.",
        )

    service = AuthService()
    try:
        t = service.login(login_id, credentials.password)
    except PermissionError as e:
        msg = str(e)
        if "disabled" in msg:
            raise APIError(status_code=status.HTTP_403_FORBIDDEN, code=ErrorCode.AUTH_ACCOUNT_DISABLED, message=msg)
        # Record failure
        brute_force.record_failure(ip=client_ip, username=credentials.username)
        raise APIError(status_code=status.HTTP_401_UNAUTHORIZED, code=ErrorCode.AUTH_INVALID_CREDENTIALS, message=msg)

    # Success — reset counters
    brute_force.reset(ip=client_ip, username=login_id)

    return Token(
        access_token=t["access_token"],
        refresh_token=t["refresh_token"],
        token_type=t["token_type"],
        expires_in=t["expires_in"],
        must_change_password=t.get("must_change_password", False),
        user=t.get("user"),
    )


@router.post("/refresh", response_model=Token)
async def refresh_token(refresh_token: str):
    """Refresh access token using refresh token."""
    service = AuthService()
    try:
        t = service.refresh(refresh_token)
    except PermissionError as e:
        raise APIError(status_code=status.HTTP_401_UNAUTHORIZED, code=ErrorCode.AUTH_INVALID_TOKEN, message=str(e))

    return Token(
        access_token=t["access_token"],
        refresh_token=t["refresh_token"],
        token_type=t["token_type"],
        expires_in=t["expires_in"],
        must_change_password=t.get("must_change_password", False),
        user=t.get("user"),
    )


@router.get("/me", response_model=User)
async def get_me(current_user: TokenData = Depends(get_current_user)):
    """Get current user info."""
    service = AuthService()
    try:
        u = service.me(current_user.user_id)
    except KeyError:
        raise APIError(status_code=status.HTTP_404_NOT_FOUND, code=ErrorCode.NOT_FOUND, message="User not found")

    return User(
        id=u["id"],
        username=u["username"],
        email=u.get("email"),
        is_active=u.get("is_active", True),
        created_at=u["created_at"],
        role=u.get("role"),
        primary_role=u.get("primary_role"),
        permissions=u.get("permissions", []),
    )


@router.post("/change-password")
async def change_password(request: PasswordChangeRequest, current_user: TokenData = Depends(get_current_user)):
    """Change the current user's password. Required if must_change_password flag is set."""
    service = AuthService()
    try:
        service.change_password(current_user.user_id, request.current_password, request.new_password)
    except PermissionError as e:
        raise APIError(status_code=status.HTTP_400_BAD_REQUEST, code=ErrorCode.AUTH_INVALID_CREDENTIALS, message=str(e))
    except KeyError:
        raise APIError(status_code=status.HTTP_404_NOT_FOUND, code=ErrorCode.NOT_FOUND, message="User not found")
    return {"detail": "Password changed successfully"}


# --- User Profile (Issue #8) ---


@router.get("/profile", response_model=UserProfileResponse)
async def get_profile(current_user: TokenData = Depends(get_current_user)):
    """Get the current user's profile."""
    from app.domains.auth.dao.user_profile_dao import UserProfileDao

    dao = UserProfileDao()
    profile = dao.get(current_user.user_id)
    if not profile:
        return UserProfileResponse(user_id=current_user.user_id)
    return UserProfileResponse(**profile)


@router.put("/profile", response_model=UserProfileResponse)
async def update_profile(
    body: UserProfileUpdate,
    current_user: TokenData = Depends(get_current_user),
):
    """Update the current user's profile."""
    if body.timezone and body.timezone not in VALID_TIMEZONES:
        raise APIError(
            status_code=status.HTTP_400_BAD_REQUEST,
            code=ErrorCode.VALIDATION_ERROR,
            message=f"Invalid timezone. Allowed: {', '.join(sorted(VALID_TIMEZONES))}",
        )
    if body.language and body.language not in VALID_LANGUAGES:
        raise APIError(
            status_code=status.HTTP_400_BAD_REQUEST,
            code=ErrorCode.VALIDATION_ERROR,
            message=f"Invalid language. Allowed: {', '.join(sorted(VALID_LANGUAGES))}",
        )

    from app.domains.auth.dao.user_profile_dao import UserProfileDao

    dao = UserProfileDao()
    updated = dao.upsert(current_user.user_id, **body.model_dump(exclude_unset=True))
    return UserProfileResponse(**updated)
