"""Report DAO.

All SQL touching `quantmate.reports` lives here.
"""
from __future__ import annotations

import json
from datetime import date
from typing import Optional

from sqlalchemy import text

from app.infrastructure.db.connections import connection


class ReportDao:
    def list_by_user(self, user_id: int, report_type: Optional[str] = None,
                     page: int = 1, page_size: int = 20) -> tuple[list[dict], int]:
        with connection("quantmate") as conn:
            conditions = ["user_id = :uid"]
            params: dict = {"uid": user_id}
            if report_type:
                conditions.append("report_type = :rt")
                params["rt"] = report_type
            where = " AND ".join(conditions)

            total_row = conn.execute(
                text(f"SELECT COUNT(*) as cnt FROM reports WHERE {where}"), params
            ).fetchone()

            params["limit"] = page_size
            params["offset"] = (page - 1) * page_size
            rows = conn.execute(
                text(f"""
                    SELECT id, user_id, report_type, period_start, period_end, pdf_path, created_at
                    FROM reports WHERE {where}
                    ORDER BY created_at DESC LIMIT :limit OFFSET :offset
                """),
                params,
            ).fetchall()
            return [
                {
                    "id": r.id, "user_id": r.user_id,
                    "report_type": r.report_type,
                    "period_start": str(r.period_start),
                    "period_end": str(r.period_end),
                    "pdf_path": r.pdf_path,
                    "created_at": r.created_at,
                }
                for r in rows
            ], total_row.cnt

    def get_by_id(self, report_id: int, user_id: int) -> Optional[dict]:
        with connection("quantmate") as conn:
            row = conn.execute(
                text("""
                    SELECT id, user_id, report_type, period_start, period_end, content_json, pdf_path, created_at
                    FROM reports WHERE id = :rid AND user_id = :uid
                """),
                {"rid": report_id, "uid": user_id},
            ).fetchone()
            if not row:
                return None
            content = row.content_json
            if isinstance(content, str):
                content = json.loads(content)
            return {
                "id": row.id, "user_id": row.user_id,
                "report_type": row.report_type,
                "period_start": str(row.period_start),
                "period_end": str(row.period_end),
                "content": content,
                "pdf_path": row.pdf_path,
                "created_at": row.created_at,
            }

    def create(self, user_id: int, report_type: str, period_start: date, period_end: date,
               content: Optional[dict] = None, pdf_path: Optional[str] = None) -> int:
        with connection("quantmate") as conn:
            result = conn.execute(
                text("""
                    INSERT INTO reports (user_id, report_type, period_start, period_end, content_json, pdf_path)
                    VALUES (:uid, :rt, :ps, :pe, :content, :pdf)
                """),
                {
                    "uid": user_id, "rt": report_type, "ps": period_start, "pe": period_end,
                    "content": json.dumps(content) if content else None, "pdf": pdf_path,
                },
            )
            conn.commit()
            return int(result.lastrowid)
