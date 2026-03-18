"""User profile DAO — CRUD for user_profiles table (Issue #8)."""

from __future__ import annotations

from typing import Any, Optional

from sqlalchemy import text

from app.infrastructure.db.connections import connection


class UserProfileDao:
    """Data access for user_profiles."""

    def get(self, user_id: int) -> Optional[dict[str, Any]]:
        with connection("quantmate") as conn:
            row = conn.execute(
                text("SELECT * FROM user_profiles WHERE user_id = :uid"),
                {"uid": user_id},
            ).fetchone()
            return dict(row._mapping) if row else None

    def upsert(self, user_id: int, **fields) -> dict[str, Any]:
        """Insert or update a profile. Only non-None fields are written."""
        allowed = {"display_name", "avatar_url", "phone", "timezone", "language", "bio"}
        data = {k: v for k, v in fields.items() if k in allowed}

        with connection("quantmate") as conn:
            existing = conn.execute(
                text("SELECT user_id FROM user_profiles WHERE user_id = :uid"),
                {"uid": user_id},
            ).fetchone()

            if existing:
                if data:
                    set_clause = ", ".join(f"{k} = :{k}" for k in data)
                    conn.execute(
                        text(f"UPDATE user_profiles SET {set_clause} WHERE user_id = :uid"),
                        {**data, "uid": user_id},
                    )
            else:
                data["user_id"] = user_id
                cols = ", ".join(data.keys())
                vals = ", ".join(f":{k}" for k in data.keys())
                conn.execute(
                    text(f"INSERT INTO user_profiles ({cols}) VALUES ({vals})"),
                    data,
                )
            conn.commit()

            return self.get(user_id) or {"user_id": user_id}
