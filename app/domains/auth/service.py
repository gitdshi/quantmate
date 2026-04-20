"""Auth domain service."""

from __future__ import annotations

from datetime import datetime
import logging
import secrets
from typing import Optional

from app.infrastructure.config import get_settings
from app.api.services.auth_service import (
    build_session_expiry,
    create_access_token,
    create_refresh_token,
    decode_token,
    get_password_hash,
    hash_session_token,
    verify_password,
)
from app.domains.auth.dao.session_dao import SessionDao
from app.domains.auth.dao.user_dao import UserDao

settings = get_settings()
logger = logging.getLogger(__name__)


def _frontend_role(primary_role: str) -> str:
    if primary_role == "admin":
        return "admin"
    if primary_role == "viewer":
        return "viewer"
    return "user"


class AuthService:
    def __init__(self) -> None:
        self._users = UserDao()
        self._sessions = SessionDao()

    def _enrich_user(self, user: dict) -> dict:
        from app.domains.rbac.service.rbac_service import RbacService

        rbac_service = RbacService()
        primary_role = rbac_service.get_primary_role(user["id"], user.get("username"))
        permissions = sorted(rbac_service.get_user_permissions(user["id"], user.get("username")))
        return {
            **user,
            "role": _frontend_role(primary_role),
            "primary_role": primary_role,
            "permissions": permissions,
        }

    def register(self, username: str, email: Optional[str], password: str) -> dict:
        if self._users.username_exists(username):
            raise ValueError("Username already registered")
        if email and self._users.email_exists(email):
            raise ValueError("Email already registered")

        now = datetime.utcnow()
        user_id = self._users.insert_user(username, email, get_password_hash(password), now, must_change_password=False)
        return self._enrich_user({
            "id": user_id,
            "username": username,
            "email": email,
            "is_active": True,
            "created_at": now,
            "must_change_password": False,
        })

    def _normalize_session_id(self, value: object) -> Optional[int]:
        return value if isinstance(value, int) and value > 0 else None

    def _create_session(
        self,
        user_id: int,
        *,
        device_info: Optional[str] = None,
        ip_address: Optional[str] = None,
    ) -> Optional[int]:
        try:
            session_token = secrets.token_urlsafe(32)
            return self._sessions.create(
                user_id,
                hash_session_token(session_token),
                device_info,
                ip_address,
                build_session_expiry(),
            )
        except Exception:
            logger.warning("Failed to persist auth session for user %s", user_id, exc_info=True)
            return None

    def _touch_session(self, session_id: int) -> bool:
        try:
            return self._sessions.touch_by_id(session_id, build_session_expiry())
        except Exception:
            logger.warning("Failed to refresh auth session %s", session_id, exc_info=True)
            return False

    def login(
        self,
        username: str,
        password: str,
        *,
        device_info: Optional[str] = None,
        ip_address: Optional[str] = None,
    ) -> dict:
        user = self._users.get_user_for_login(username)
        if not user:
            raise PermissionError("Incorrect username or password")
        if not verify_password(password, user["hashed_password"]):
            raise PermissionError("Incorrect username or password")
        if not user.get("is_active"):
            raise PermissionError("User account is disabled")

        must_change = user.get("must_change_password", False)
        session_id = self._create_session(user["id"], device_info=device_info, ip_address=ip_address)
        access_token = create_access_token(
            user["id"],
            user["username"],
            must_change_password=must_change,
            session_id=session_id,
        )
        refresh_token = create_refresh_token(
            user["id"],
            user["username"],
            must_change_password=must_change,
            session_id=session_id,
        )
        full_user = self._users.get_user_by_id(user["id"]) or {
            "id": user["id"],
            "username": user["username"],
            "email": None,
            "is_active": user.get("is_active", True),
            "created_at": datetime.utcnow(),
        }
        return {
            "access_token": access_token,
            "refresh_token": refresh_token,
            "token_type": "bearer",
            "expires_in": settings.access_token_expire_minutes * 60,
            "must_change_password": must_change,
            "user": self._enrich_user(full_user),
        }

    def refresh(
        self,
        refresh_token: str,
        *,
        device_info: Optional[str] = None,
        ip_address: Optional[str] = None,
    ) -> dict:
        token_data = decode_token(refresh_token)
        if token_data is None:
            raise PermissionError("Invalid or expired refresh token")

        user = self._users.get_user_by_id(token_data.user_id)
        if not user or not user.get("is_active"):
            raise PermissionError("User not found or inactive")

        session_id = self._normalize_session_id(getattr(token_data, "session_id", None))
        if session_id is not None:
            session = self._sessions.get_active(session_id, user_id=token_data.user_id)
            if session is None or not self._touch_session(session_id):
                raise PermissionError("Session expired or revoked")
        else:
            session_id = self._create_session(user["id"], device_info=device_info, ip_address=ip_address)

        must_change = user.get("must_change_password", False)
        new_access_token = create_access_token(
            user["id"],
            user["username"],
            must_change_password=must_change,
            session_id=session_id,
        )
        new_refresh_token = create_refresh_token(
            user["id"],
            user["username"],
            must_change_password=must_change,
            session_id=session_id,
        )
        return {
            "access_token": new_access_token,
            "refresh_token": new_refresh_token,
            "token_type": "bearer",
            "expires_in": settings.access_token_expire_minutes * 60,
            "must_change_password": must_change,
            "user": self._enrich_user(user),
        }

    def me(self, user_id: int) -> dict:
        user = self._users.get_user_by_id(user_id)
        if not user:
            raise KeyError("User not found")
        return self._enrich_user(user)

    def change_password(self, user_id: int, current_password: str, new_password: str) -> None:
        """Change user's password. Verifies current password and clears must_change_password flag."""
        user = self._users.get_user_by_id(user_id)
        if not user:
            raise KeyError("User not found")
        if not verify_password(current_password, user["hashed_password"]):
            raise PermissionError("Incorrect current password")
        new_hash = get_password_hash(new_password)
        # Clear the must_change_password flag on successful change
        self._users.update_user_password(user_id, new_hash, must_change_password=False)
