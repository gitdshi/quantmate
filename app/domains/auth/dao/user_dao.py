"""User DAO.

All SQL touching `quantmate.users` lives here.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Optional

from app.infrastructure.db.connections import connection


@dataclass(frozen=True)
class UserRow:
    id: int
    username: str
    email: Optional[str]
    hashed_password: str
    is_active: bool
    must_change_password: bool
    created_at: datetime


class UserDao:
    def username_exists(self, username: str) -> bool:
        with connection("quantmate") as conn:
            from sqlalchemy import text
            row = conn.execute(
                text("SELECT 1 FROM users WHERE username = :u LIMIT 1"),
                {"u": username},
            ).fetchone()
            return bool(row)

    def email_exists(self, email: str) -> bool:
        if not email:
            return False
        with connection("quantmate") as conn:
            from sqlalchemy import text
            row = conn.execute(
                text("SELECT 1 FROM users WHERE email = :e LIMIT 1"),
                {"e": email},
            ).fetchone()
            return bool(row)

    def insert_user(self, username: str, email: Optional[str], hashed_password: str, created_at: datetime, must_change_password: bool = False) -> int:
        with connection("quantmate") as conn:
            from sqlalchemy import text
            result = conn.execute(
                text(
                    """
                    INSERT INTO users (username, email, hashed_password, is_active, must_change_password, created_at)
                    VALUES (:username, :email, :hashed_password, 1, :must_change_password, :created_at)
                    """
                ),
                {
                    "username": username,
                    "email": email,
                    "hashed_password": hashed_password,
                    "must_change_password": must_change_password,
                    "created_at": created_at,
                },
            )
            conn.commit()
            return int(result.lastrowid)

    def get_user_for_login(self, username: str) -> Optional[dict]:
        with connection("quantmate") as conn:
            from sqlalchemy import text
            row = conn.execute(
                text("SELECT id, username, hashed_password, is_active, must_change_password FROM users WHERE username = :u"),
                {"u": username},
            ).fetchone()
            if not row:
                return None
            return {
                "id": row.id,
                "username": row.username,
                "hashed_password": row.hashed_password,
                "is_active": bool(row.is_active),
                "must_change_password": bool(row.must_change_password),
            }

    def get_user_by_id(self, user_id: int) -> Optional[dict]:
        with connection("quantmate") as conn:
            from sqlalchemy import text
            row = conn.execute(
                text("SELECT id, username, email, hashed_password, is_active, must_change_password, created_at FROM users WHERE id = :uid"),
                {"uid": user_id},
            ).fetchone()
            if not row:
                return None
            return {
                "id": row.id,
                "username": row.username,
                "email": row.email,
                "hashed_password": row.hashed_password,
                "is_active": bool(row.is_active),
                "must_change_password": bool(row.must_change_password),
                "created_at": row.created_at,
            }

    def update_user_password(self, user_id: int, new_hashed_password: str, must_change_password: bool = False) -> None:
        """Update user's password and optionally reset must_change_password flag."""
        with connection("quantmate") as conn:
            from sqlalchemy import text
            conn.execute(
                text(
                    """
                    UPDATE users
                    SET hashed_password = :pwd, must_change_password = :mcp, updated_at = CURRENT_TIMESTAMP
                    WHERE id = :uid
                    """
                ),
                {"pwd": new_hashed_password, "mcp": must_change_password, "uid": user_id},
            )
            conn.commit()
