"""KYC submissions DAO (Issue #9)."""

from __future__ import annotations

from typing import Any, Optional

from sqlalchemy import text
from sqlalchemy.exc import OperationalError, ProgrammingError

from app.infrastructure.db.connections import connection


def _is_missing_table(exc: Exception, table_name: str) -> bool:
    message = str(getattr(exc, "orig", exc)).lower()
    return table_name.lower() in message and ("doesn't exist" in message or "no such table" in message)


class KycDao:
    """Data access for kyc_submissions."""

    def get_latest(self, user_id: int) -> Optional[dict[str, Any]]:
        with connection("quantmate") as conn:
            try:
                row = conn.execute(
                    text("SELECT * FROM kyc_submissions WHERE user_id = :uid ORDER BY created_at DESC LIMIT 1"),
                    {"uid": user_id},
                ).fetchone()
            except (ProgrammingError, OperationalError) as exc:
                if _is_missing_table(exc, "kyc_submissions"):
                    return None
                raise
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
            try:
                rows = conn.execute(
                    text(
                        "SELECT id, user_id, status, real_name, id_type, created_at "
                        "FROM kyc_submissions WHERE status = 'pending' "
                        "ORDER BY created_at ASC LIMIT :lim OFFSET :off"
                    ),
                    {"lim": limit, "off": offset},
                ).fetchall()
            except (ProgrammingError, OperationalError) as exc:
                if _is_missing_table(exc, "kyc_submissions"):
                    return []
                raise
            return [dict(r._mapping) for r in rows]

    def count_pending(self) -> int:
        with connection("quantmate") as conn:
            try:
                row = conn.execute(text("SELECT COUNT(*) AS cnt FROM kyc_submissions WHERE status = 'pending'"))
                row = row.fetchone()
            except (ProgrammingError, OperationalError) as exc:
                if _is_missing_table(exc, "kyc_submissions"):
                    return 0
                raise
            return row._mapping["cnt"] if row else 0
