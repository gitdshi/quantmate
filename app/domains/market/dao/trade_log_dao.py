"""Trade log DAO — immutable trade event log."""

from __future__ import annotations

from datetime import date, datetime
from typing import Any, Optional

from sqlalchemy import text
from sqlalchemy.exc import OperationalError, ProgrammingError


def _is_missing_table(exc: Exception, table_name: str) -> bool:
    message = str(getattr(exc, "orig", exc)).lower()
    return table_name.lower() in message and ("doesn't exist" in message or "no such table" in message)

from app.infrastructure.db.connections import connection


class TradeLogDao:
    """Insert-only access to trade_logs. No UPDATE/DELETE."""

    def insert(self, **fields) -> int:
        cols = ", ".join(fields.keys())
        vals = ", ".join(f":{k}" for k in fields.keys())
        with connection("quantmate") as conn:
            result = conn.execute(
                text(f"INSERT INTO trade_logs ({cols}) VALUES ({vals})"),
                fields,
            )
            conn.commit()
            return result.lastrowid  # type: ignore[return-value]

    def query(
        self,
        *,
        symbol: Optional[str] = None,
        event_type: Optional[str] = None,
        direction: Optional[str] = None,
        strategy_id: Optional[int] = None,
        start_date: Optional[date] = None,
        end_date: Optional[date] = None,
        limit: int = 50,
        offset: int = 0,
    ) -> list[dict[str, Any]]:
        conditions = []
        params: dict[str, Any] = {"limit": limit, "offset": offset}

        if symbol:
            conditions.append("symbol = :symbol")
            params["symbol"] = symbol
        if event_type:
            conditions.append("event_type = :event_type")
            params["event_type"] = event_type
        if direction:
            conditions.append("direction = :direction")
            params["direction"] = direction
        if strategy_id is not None:
            conditions.append("strategy_id = :strategy_id")
            params["strategy_id"] = strategy_id
        if start_date:
            conditions.append("timestamp >= :start_date")
            params["start_date"] = datetime.combine(start_date, datetime.min.time())
        if end_date:
            conditions.append("timestamp <= :end_date")
            params["end_date"] = datetime.combine(end_date, datetime.max.time())

        where = " AND ".join(conditions) if conditions else "1=1"
        sql = f"SELECT * FROM trade_logs WHERE {where} ORDER BY timestamp DESC LIMIT :limit OFFSET :offset"
        with connection("quantmate") as conn:
            try:
                rows = conn.execute(text(sql), params).fetchall()
            except (ProgrammingError, OperationalError) as exc:
                if _is_missing_table(exc, "trade_logs"):
                    return []
                raise
            return [dict(r._mapping) for r in rows]

    def count(self, **filters) -> int:
        conditions = []
        params: dict[str, Any] = {}
        for k, v in filters.items():
            if v is not None:
                conditions.append(f"{k} = :{k}")
                params[k] = v
        where = " AND ".join(conditions) if conditions else "1=1"
        with connection("quantmate") as conn:
            try:
                row = conn.execute(
                    text(f"SELECT COUNT(*) AS cnt FROM trade_logs WHERE {where}"),
                    params,
                ).fetchone()
            except (ProgrammingError, OperationalError) as exc:
                if _is_missing_table(exc, "trade_logs"):
                    return 0
                raise
            return row._mapping["cnt"] if row else 0
