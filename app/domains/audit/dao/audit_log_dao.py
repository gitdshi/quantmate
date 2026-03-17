"""Audit log DAO — insert-only access to audit_logs table."""
from __future__ import annotations

import json
from datetime import date, datetime
from typing import Any, Optional

from sqlalchemy import text

from app.infrastructure.db.connections import connection


class AuditLogDao:
    """Data access for audit_logs. Only INSERT and SELECT — no UPDATE/DELETE."""

    def insert(
        self,
        *,
        user_id: Optional[int],
        username: Optional[str],
        operation_type: str,
        resource_type: Optional[str] = None,
        resource_id: Optional[str] = None,
        details: Optional[dict] = None,
        ip_address: Optional[str] = None,
        user_agent: Optional[str] = None,
        http_method: Optional[str] = None,
        http_path: Optional[str] = None,
        http_status: Optional[int] = None,
    ) -> None:
        with connection("quantmate") as conn:
            conn.execute(
                text("""
                    INSERT INTO audit_logs
                        (user_id, username, operation_type, resource_type, resource_id,
                         details, ip_address, user_agent, http_method, http_path, http_status)
                    VALUES
                        (:user_id, :username, :operation_type, :resource_type, :resource_id,
                         :details, :ip_address, :user_agent, :http_method, :http_path, :http_status)
                """),
                {
                    "user_id": user_id,
                    "username": username,
                    "operation_type": operation_type,
                    "resource_type": resource_type,
                    "resource_id": resource_id,
                    "details": json.dumps(details) if details else None,
                    "ip_address": ip_address,
                    "user_agent": user_agent,
                    "http_method": http_method,
                    "http_path": http_path,
                    "http_status": http_status,
                },
            )
            conn.commit()

    def query(
        self,
        *,
        user_id: Optional[int] = None,
        operation_type: Optional[str] = None,
        resource_type: Optional[str] = None,
        start_date: Optional[date] = None,
        end_date: Optional[date] = None,
        limit: int = 50,
        offset: int = 0,
    ) -> list[dict[str, Any]]:
        conditions = []
        params: dict[str, Any] = {"limit": limit, "offset": offset}

        if user_id is not None:
            conditions.append("user_id = :user_id")
            params["user_id"] = user_id
        if operation_type:
            conditions.append("operation_type = :operation_type")
            params["operation_type"] = operation_type
        if resource_type:
            conditions.append("resource_type = :resource_type")
            params["resource_type"] = resource_type
        if start_date:
            conditions.append("timestamp >= :start_date")
            params["start_date"] = datetime.combine(start_date, datetime.min.time())
        if end_date:
            conditions.append("timestamp <= :end_date")
            params["end_date"] = datetime.combine(end_date, datetime.max.time())

        where = " AND ".join(conditions) if conditions else "1=1"
        sql = f"""
            SELECT id, timestamp, user_id, username, operation_type,
                   resource_type, resource_id, details, ip_address,
                   user_agent, http_method, http_path, http_status
            FROM audit_logs
            WHERE {where}
            ORDER BY timestamp DESC
            LIMIT :limit OFFSET :offset
        """
        with connection("quantmate") as conn:
            rows = conn.execute(text(sql), params).fetchall()
            return [dict(r._mapping) for r in rows]

    def count(
        self,
        *,
        user_id: Optional[int] = None,
        operation_type: Optional[str] = None,
        resource_type: Optional[str] = None,
        start_date: Optional[date] = None,
        end_date: Optional[date] = None,
    ) -> int:
        conditions = []
        params: dict[str, Any] = {}

        if user_id is not None:
            conditions.append("user_id = :user_id")
            params["user_id"] = user_id
        if operation_type:
            conditions.append("operation_type = :operation_type")
            params["operation_type"] = operation_type
        if resource_type:
            conditions.append("resource_type = :resource_type")
            params["resource_type"] = resource_type
        if start_date:
            conditions.append("timestamp >= :start_date")
            params["start_date"] = datetime.combine(start_date, datetime.min.time())
        if end_date:
            conditions.append("timestamp <= :end_date")
            params["end_date"] = datetime.combine(end_date, datetime.max.time())

        where = " AND ".join(conditions) if conditions else "1=1"
        sql = f"SELECT COUNT(*) AS cnt FROM audit_logs WHERE {where}"
        with connection("quantmate") as conn:
            row = conn.execute(text(sql), params).fetchone()
            return row._mapping["cnt"] if row else 0
