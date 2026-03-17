"""Portfolio DAO — positions, transactions, snapshots."""
from __future__ import annotations

import json
from typing import Any, Optional

from sqlalchemy import text

from app.infrastructure.db.connections import connection


class PortfolioDao:

    # --- Portfolio ---

    def get_or_create(self, user_id: int) -> dict[str, Any]:
        with connection("quantmate") as conn:
            row = conn.execute(
                text("SELECT * FROM portfolios WHERE user_id = :uid LIMIT 1"),
                {"uid": user_id},
            ).fetchone()
            if row:
                return dict(row._mapping)
            conn.execute(
                text("INSERT INTO portfolios (user_id) VALUES (:uid)"),
                {"uid": user_id},
            )
            conn.commit()
            row = conn.execute(
                text("SELECT * FROM portfolios WHERE user_id = :uid LIMIT 1"),
                {"uid": user_id},
            ).fetchone()
            return dict(row._mapping)

    # --- Positions ---

    def list_positions(self, portfolio_id: int) -> list[dict[str, Any]]:
        with connection("quantmate") as conn:
            rows = conn.execute(
                text(
                    "SELECT * FROM portfolio_positions "
                    "WHERE portfolio_id = :pid AND quantity > 0 ORDER BY symbol"
                ),
                {"pid": portfolio_id},
            ).fetchall()
            return [dict(r._mapping) for r in rows]

    def get_position(self, portfolio_id: int, symbol: str) -> Optional[dict[str, Any]]:
        with connection("quantmate") as conn:
            row = conn.execute(
                text(
                    "SELECT * FROM portfolio_positions "
                    "WHERE portfolio_id = :pid AND symbol = :sym"
                ),
                {"pid": portfolio_id, "sym": symbol},
            ).fetchone()
            return dict(row._mapping) if row else None

    def upsert_position(self, portfolio_id: int, symbol: str, quantity: int, avg_cost: float) -> None:
        with connection("quantmate") as conn:
            existing = conn.execute(
                text(
                    "SELECT id FROM portfolio_positions "
                    "WHERE portfolio_id = :pid AND symbol = :sym"
                ),
                {"pid": portfolio_id, "sym": symbol},
            ).fetchone()
            if existing:
                conn.execute(
                    text(
                        "UPDATE portfolio_positions SET quantity = :qty, avg_cost = :cost "
                        "WHERE portfolio_id = :pid AND symbol = :sym"
                    ),
                    {"qty": quantity, "cost": avg_cost, "pid": portfolio_id, "sym": symbol},
                )
            else:
                conn.execute(
                    text(
                        "INSERT INTO portfolio_positions (portfolio_id, symbol, quantity, avg_cost) "
                        "VALUES (:pid, :sym, :qty, :cost)"
                    ),
                    {"pid": portfolio_id, "sym": symbol, "qty": quantity, "cost": avg_cost},
                )
            conn.commit()

    def update_cash(self, portfolio_id: int, cash: float) -> None:
        with connection("quantmate") as conn:
            conn.execute(
                text("UPDATE portfolios SET cash = :cash WHERE id = :pid"),
                {"cash": cash, "pid": portfolio_id},
            )
            conn.commit()

    # --- Transactions ---

    def insert_transaction(self, portfolio_id: int, **fields) -> int:
        fields["portfolio_id"] = portfolio_id
        cols = ", ".join(fields.keys())
        vals = ", ".join(f":{k}" for k in fields.keys())
        with connection("quantmate") as conn:
            result = conn.execute(
                text(f"INSERT INTO portfolio_transactions ({cols}) VALUES ({vals})"),
                fields,
            )
            conn.commit()
            return result.lastrowid  # type: ignore[return-value]

    def list_transactions(self, portfolio_id: int, limit: int = 50, offset: int = 0) -> list[dict[str, Any]]:
        with connection("quantmate") as conn:
            rows = conn.execute(
                text(
                    "SELECT * FROM portfolio_transactions WHERE portfolio_id = :pid "
                    "ORDER BY created_at DESC LIMIT :lim OFFSET :off"
                ),
                {"pid": portfolio_id, "lim": limit, "off": offset},
            ).fetchall()
            return [dict(r._mapping) for r in rows]

    def count_transactions(self, portfolio_id: int) -> int:
        with connection("quantmate") as conn:
            row = conn.execute(
                text("SELECT COUNT(*) AS cnt FROM portfolio_transactions WHERE portfolio_id = :pid"),
                {"pid": portfolio_id},
            ).fetchone()
            return row._mapping["cnt"] if row else 0

    # --- Snapshots ---

    def list_snapshots(self, portfolio_id: int, limit: int = 30) -> list[dict[str, Any]]:
        with connection("quantmate") as conn:
            rows = conn.execute(
                text(
                    "SELECT * FROM portfolio_snapshots WHERE portfolio_id = :pid "
                    "ORDER BY date DESC LIMIT :lim"
                ),
                {"pid": portfolio_id, "lim": limit},
            ).fetchall()
            return [dict(r._mapping) for r in rows]
