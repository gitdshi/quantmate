"""KYC submissions DAO (Issue #9)."""
from __future__ import annotations

from typing import Any, Optional

from sqlalchemy import text

from app.infrastructure.db.connections import connection


class KycDao:
    """Data access for kyc_submissions."""

    def get_latest(self, user_id: int) -> Optional[dict[str, Any]]:
        with connection("quantmate") as conn:
            row = conn.execute(
                text(
                    "SELECT * FROM kyc_submissions WHERE user_id = :uid "
                    "ORDER BY created_at DESC LIMIT 1"
                ),
                {"uid": user_id},
            ).fetchone()
            return dict(row._mapping) if row else None

    def insert(self, user_id: int, **fields) -> int:
        with connection("quantmate") as conn:
            fields["user_id"] = user_id
            cols = ", ".join(fields.keys())
            vals = ", ".join(f":{k}" for k in fields.keys())
            result = conn.execute(
                text(f"INSERT INTO kyc_submissions ({cols}) VALUES ({vals})"),
                fields,
            )
            conn.commit()
            return result.lastrowid  # type: ignore[return-value]

    def update_status(
        self,
        submission_id: int,
        status: str,
        reviewer_id: int,
        review_notes: Optional[str] = None,
    ) -> None:
        with connection("quantmate") as conn:
            conn.execute(
                text(
                    "UPDATE kyc_submissions SET status = :status, reviewer_id = :rid, "
                    "review_notes = :notes, reviewed_at = NOW() WHERE id = :sid"
                ),
                {
                    "status": status,
                    "rid": reviewer_id,
                    "notes": review_notes,
                    "sid": submission_id,
                },
            )
            conn.commit()

    def list_pending(self, limit: int = 50, offset: int = 0) -> list[dict[str, Any]]:
        with connection("quantmate") as conn:
            rows = conn.execute(
                text(
                    "SELECT id, user_id, status, real_name, id_type, created_at "
                    "FROM kyc_submissions WHERE status = 'pending' "
                    "ORDER BY created_at ASC LIMIT :lim OFFSET :off"
                ),
                {"lim": limit, "off": offset},
            ).fetchall()
            return [dict(r._mapping) for r in rows]

    def count_pending(self) -> int:
        with connection("quantmate") as conn:
            row = conn.execute(
                text("SELECT COUNT(*) AS cnt FROM kyc_submissions WHERE status = 'pending'")
            ).fetchone()
            return row._mapping["cnt"] if row else 0
