"""Watchlist DAO (Issue #6)."""

from __future__ import annotations

from typing import Any, Optional

from sqlalchemy import text

from app.infrastructure.db.connections import connection


class WatchlistDao:
    """CRUD for watchlists and watchlist_items."""

    # --- Watchlists ---

    def list_for_user(self, user_id: int) -> list[dict[str, Any]]:
        with connection("quantmate") as conn:
            rows = conn.execute(
                text("SELECT * FROM watchlists WHERE user_id = :uid ORDER BY sort_order, id"),
                {"uid": user_id},
            ).fetchall()
            return [dict(r._mapping) for r in rows]

    def get(self, watchlist_id: int) -> Optional[dict[str, Any]]:
        with connection("quantmate") as conn:
            row = conn.execute(
                text("SELECT * FROM watchlists WHERE id = :wid"),
                {"wid": watchlist_id},
            ).fetchone()
            return dict(row._mapping) if row else None

    def create(self, user_id: int, name: str, description: Optional[str] = None) -> int:
        with connection("quantmate") as conn:
            result = conn.execute(
                text("INSERT INTO watchlists (user_id, name, description) VALUES (:uid, :name, :desc)"),
                {"uid": user_id, "name": name, "desc": description},
            )
            conn.commit()
            return result.lastrowid  # type: ignore[return-value]

    def update(self, watchlist_id: int, **fields) -> None:
        allowed = {"name", "description", "sort_order"}
        data = {k: v for k, v in fields.items() if k in allowed and v is not None}
        if not data:
            return
        set_clause = ", ".join(f"{k} = :{k}" for k in data)
        with connection("quantmate") as conn:
            conn.execute(
                text(f"UPDATE watchlists SET {set_clause} WHERE id = :wid"),
                {**data, "wid": watchlist_id},
            )
            conn.commit()

    def delete(self, watchlist_id: int) -> None:
        with connection("quantmate") as conn:
            conn.execute(
                text("DELETE FROM watchlists WHERE id = :wid"),
                {"wid": watchlist_id},
            )
            conn.commit()

    # --- Watchlist items ---

    def list_items(self, watchlist_id: int) -> list[dict[str, Any]]:
        with connection("quantmate") as conn:
            rows = conn.execute(
                text("SELECT * FROM watchlist_items WHERE watchlist_id = :wid ORDER BY added_at DESC"),
                {"wid": watchlist_id},
            ).fetchall()
            return [dict(r._mapping) for r in rows]

    def add_item(self, watchlist_id: int, symbol: str, notes: Optional[str] = None) -> int:
        with connection("quantmate") as conn:
            result = conn.execute(
                text("INSERT INTO watchlist_items (watchlist_id, symbol, notes) VALUES (:wid, :sym, :notes)"),
                {"wid": watchlist_id, "sym": symbol, "notes": notes},
            )
            conn.commit()
            return result.lastrowid  # type: ignore[return-value]

    def remove_item(self, watchlist_id: int, symbol: str) -> bool:
        with connection("quantmate") as conn:
            result = conn.execute(
                text("DELETE FROM watchlist_items WHERE watchlist_id = :wid AND symbol = :sym"),
                {"wid": watchlist_id, "sym": symbol},
            )
            conn.commit()
            return result.rowcount > 0  # type: ignore[union-attr]
