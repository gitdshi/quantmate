"""Session DAO.

All SQL touching `quantmate.user_sessions` lives here.
"""

from __future__ import annotations

from datetime import datetime
from typing import Optional

from sqlalchemy import text
from sqlalchemy.exc import OperationalError, ProgrammingError

from app.infrastructure.db.connections import connection


def _is_missing_table(exc: Exception, table_name: str) -> bool:
    message = str(getattr(exc, "orig", exc)).lower()
    return table_name.lower() in message and ("doesn't exist" in message or "no such table" in message)


class SessionDao:
    def create(
        self, user_id: int, token_hash: str, device_info: Optional[str], ip_address: Optional[str], expires_at: datetime
    ) -> Optional[int]:
        with connection("quantmate") as conn:
            try:
                result = conn.execute(
                    text("""
                        INSERT INTO user_sessions (user_id, token_hash, device_info, ip_address, expires_at)
                        VALUES (:uid, :thash, :dev, :ip, :exp)
                    """),
                    {"uid": user_id, "thash": token_hash, "dev": device_info, "ip": ip_address, "exp": expires_at},
                )
            except (ProgrammingError, OperationalError) as exc:
                if _is_missing_table(exc, "user_sessions"):
                    return None
                raise
            conn.commit()
            return int(result.lastrowid) if result.lastrowid is not None else None

    def get_active(self, session_id: int, user_id: Optional[int] = None) -> Optional[dict]:
        params: dict[str, object] = {"sid": session_id}
        user_clause = ""
        if user_id is not None:
            user_clause = " AND user_id = :uid"
            params["uid"] = user_id

        with connection("quantmate") as conn:
            try:
                row = conn.execute(
                    text(
                        "SELECT id, user_id, token_hash, device_info, ip_address, login_at, last_active_at, expires_at "
                        "FROM user_sessions "
                        "WHERE id = :sid"
                        f"{user_clause}"
                        " AND expires_at > NOW()"
                    ),
                    params,
                ).fetchone()
            except (ProgrammingError, OperationalError) as exc:
                if _is_missing_table(exc, "user_sessions"):
                    return None
                raise
            return dict(row._mapping) if row else None

    def list_by_user(self, user_id: int) -> list[dict]:
        with connection("quantmate") as conn:
            try:
                rows = conn.execute(
                    text("""
                        SELECT id, user_id, device_info, ip_address, login_at, last_active_at, expires_at
                        FROM user_sessions WHERE user_id = :uid AND expires_at > NOW()
                        ORDER BY last_active_at DESC
                    """),
                    {"uid": user_id},
                ).fetchall()
            except (ProgrammingError, OperationalError) as exc:
                if _is_missing_table(exc, "user_sessions"):
                    return []
                raise
            return [
                {
                    "id": r.id,
                    "user_id": r.user_id,
                    "device_info": r.device_info,
                    "ip_address": r.ip_address,
                    "login_at": r.login_at,
                    "last_active_at": r.last_active_at,
                    "expires_at": r.expires_at,
                }
                for r in rows
            ]

    def delete(self, session_id: int, user_id: int) -> bool:
        with connection("quantmate") as conn:
            try:
                result = conn.execute(
                    text("DELETE FROM user_sessions WHERE id = :sid AND user_id = :uid"),
                    {"sid": session_id, "uid": user_id},
                )
            except (ProgrammingError, OperationalError) as exc:
                if _is_missing_table(exc, "user_sessions"):
                    return False
                raise
            conn.commit()
            return result.rowcount > 0

    def delete_all_for_user(self, user_id: int) -> int:
        with connection("quantmate") as conn:
            try:
                result = conn.execute(
                    text("DELETE FROM user_sessions WHERE user_id = :uid"),
                    {"uid": user_id},
                )
            except (ProgrammingError, OperationalError) as exc:
                if _is_missing_table(exc, "user_sessions"):
                    return 0
                raise
            conn.commit()
            return result.rowcount

    def touch(self, token_hash: str) -> None:
        with connection("quantmate") as conn:
            try:
                conn.execute(
                    text("UPDATE user_sessions SET last_active_at = NOW() WHERE token_hash = :th"),
                    {"th": token_hash},
                )
            except (ProgrammingError, OperationalError) as exc:
                if _is_missing_table(exc, "user_sessions"):
                    return
                raise
            conn.commit()

    def touch_by_id(self, session_id: int, expires_at: datetime) -> bool:
        with connection("quantmate") as conn:
            try:
                result = conn.execute(
                    text(
                        "UPDATE user_sessions "
                        "SET last_active_at = NOW(), expires_at = :exp "
                        "WHERE id = :sid AND expires_at > NOW()"
                    ),
                    {"sid": session_id, "exp": expires_at},
                )
            except (ProgrammingError, OperationalError) as exc:
                if _is_missing_table(exc, "user_sessions"):
                    return False
                raise
            conn.commit()
            return result.rowcount > 0

    def cleanup_expired(self) -> int:
        with connection("quantmate") as conn:
            try:
                result = conn.execute(
                    text("DELETE FROM user_sessions WHERE expires_at < NOW()"),
                )
            except (ProgrammingError, OperationalError) as exc:
                if _is_missing_table(exc, "user_sessions"):
                    return 0
                raise
            conn.commit()
            return result.rowcount
