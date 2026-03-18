"""MFA DAO.

All SQL touching `quantmate.mfa_settings` lives here.
"""

from __future__ import annotations

from typing import Optional

from sqlalchemy import text

from app.infrastructure.db.connections import connection


class MfaDao:
    def get_by_user_id(self, user_id: int) -> Optional[dict]:
        with connection("quantmate") as conn:
            row = conn.execute(
                text(
                    "SELECT id, user_id, mfa_type, secret_encrypted, is_enabled, recovery_codes_hash, created_at FROM mfa_settings WHERE user_id = :uid"
                ),
                {"uid": user_id},
            ).fetchone()
            if not row:
                return None
            return {
                "id": row.id,
                "user_id": row.user_id,
                "mfa_type": row.mfa_type,
                "secret_encrypted": row.secret_encrypted,
                "is_enabled": bool(row.is_enabled),
                "recovery_codes_hash": row.recovery_codes_hash,
                "created_at": row.created_at,
            }

    def upsert(self, user_id: int, mfa_type: str, secret_encrypted: str, recovery_codes_hash: str) -> int:
        with connection("quantmate") as conn:
            existing = conn.execute(
                text("SELECT id FROM mfa_settings WHERE user_id = :uid"),
                {"uid": user_id},
            ).fetchone()
            if existing:
                conn.execute(
                    text("""
                        UPDATE mfa_settings
                        SET mfa_type = :mfa_type, secret_encrypted = :secret, recovery_codes_hash = :codes, is_enabled = 0
                        WHERE user_id = :uid
                    """),
                    {"uid": user_id, "mfa_type": mfa_type, "secret": secret_encrypted, "codes": recovery_codes_hash},
                )
                conn.commit()
                return existing.id
            else:
                result = conn.execute(
                    text("""
                        INSERT INTO mfa_settings (user_id, mfa_type, secret_encrypted, recovery_codes_hash, is_enabled)
                        VALUES (:uid, :mfa_type, :secret, :codes, 0)
                    """),
                    {"uid": user_id, "mfa_type": mfa_type, "secret": secret_encrypted, "codes": recovery_codes_hash},
                )
                conn.commit()
                return int(result.lastrowid)

    def enable(self, user_id: int) -> bool:
        with connection("quantmate") as conn:
            result = conn.execute(
                text("UPDATE mfa_settings SET is_enabled = 1 WHERE user_id = :uid"),
                {"uid": user_id},
            )
            conn.commit()
            return result.rowcount > 0

    def disable(self, user_id: int) -> bool:
        with connection("quantmate") as conn:
            result = conn.execute(
                text("UPDATE mfa_settings SET is_enabled = 0 WHERE user_id = :uid"),
                {"uid": user_id},
            )
            conn.commit()
            return result.rowcount > 0

    def delete(self, user_id: int) -> bool:
        with connection("quantmate") as conn:
            result = conn.execute(
                text("DELETE FROM mfa_settings WHERE user_id = :uid"),
                {"uid": user_id},
            )
            conn.commit()
            return result.rowcount > 0
