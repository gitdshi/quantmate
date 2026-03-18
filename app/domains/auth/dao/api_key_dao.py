"""API Key DAO.

All SQL touching `quantmate.api_keys` lives here.
"""

from __future__ import annotations

import json
from datetime import datetime
from typing import Optional

from sqlalchemy import text

from app.infrastructure.db.connections import connection


class ApiKeyDao:
    def list_by_user(self, user_id: int) -> list[dict]:
        with connection("quantmate") as conn:
            rows = conn.execute(
                text("""
                    SELECT id, user_id, key_id, name, permissions, expires_at,
                           ip_whitelist, rate_limit, is_active, created_at, last_used_at
                    FROM api_keys WHERE user_id = :uid ORDER BY created_at DESC
                """),
                {"uid": user_id},
            ).fetchall()
            return [self._row_to_dict(r) for r in rows]

    def count_by_user(self, user_id: int) -> int:
        with connection("quantmate") as conn:
            row = conn.execute(
                text("SELECT COUNT(*) as cnt FROM api_keys WHERE user_id = :uid"),
                {"uid": user_id},
            ).fetchone()
            return row.cnt

    def get_by_key_id(self, key_id: str) -> Optional[dict]:
        with connection("quantmate") as conn:
            row = conn.execute(
                text("""
                    SELECT id, user_id, key_id, secret_hash, name, permissions, expires_at,
                           ip_whitelist, rate_limit, is_active, created_at, last_used_at
                    FROM api_keys WHERE key_id = :kid
                """),
                {"kid": key_id},
            ).fetchone()
            if not row:
                return None
            d = self._row_to_dict(row)
            d["secret_hash"] = row.secret_hash
            return d

    def create(
        self,
        user_id: int,
        key_id: str,
        secret_hash: str,
        name: str,
        permissions: Optional[list] = None,
        expires_at: Optional[datetime] = None,
        ip_whitelist: Optional[list] = None,
        rate_limit: int = 60,
    ) -> int:
        with connection("quantmate") as conn:
            result = conn.execute(
                text("""
                    INSERT INTO api_keys (user_id, key_id, secret_hash, name, permissions, expires_at, ip_whitelist, rate_limit)
                    VALUES (:uid, :kid, :shash, :name, :perms, :expires, :ips, :rl)
                """),
                {
                    "uid": user_id,
                    "kid": key_id,
                    "shash": secret_hash,
                    "name": name,
                    "perms": json.dumps(permissions) if permissions else None,
                    "expires": expires_at,
                    "ips": json.dumps(ip_whitelist) if ip_whitelist else None,
                    "rl": rate_limit,
                },
            )
            conn.commit()
            return int(result.lastrowid)

    def revoke(self, api_key_id: int, user_id: int) -> bool:
        with connection("quantmate") as conn:
            result = conn.execute(
                text("UPDATE api_keys SET is_active = 0 WHERE id = :id AND user_id = :uid"),
                {"id": api_key_id, "uid": user_id},
            )
            conn.commit()
            return result.rowcount > 0

    def update_last_used(self, key_id: str) -> None:
        with connection("quantmate") as conn:
            conn.execute(
                text("UPDATE api_keys SET last_used_at = NOW() WHERE key_id = :kid"),
                {"kid": key_id},
            )
            conn.commit()

    def _row_to_dict(self, row) -> dict:
        perms = row.permissions
        if isinstance(perms, str):
            perms = json.loads(perms)
        ips = row.ip_whitelist
        if isinstance(ips, str):
            ips = json.loads(ips)
        return {
            "id": row.id,
            "user_id": row.user_id,
            "key_id": row.key_id,
            "name": row.name,
            "permissions": perms,
            "expires_at": row.expires_at,
            "ip_whitelist": ips,
            "rate_limit": row.rate_limit,
            "is_active": bool(row.is_active),
            "created_at": row.created_at,
            "last_used_at": row.last_used_at,
        }
