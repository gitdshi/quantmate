"""Order DAO.

All SQL touching `quantmate.orders` and `quantmate.trades` lives here.
"""

from __future__ import annotations

from typing import Optional

from sqlalchemy import text

from app.infrastructure.db.connections import connection


class OrderDao:
    def create(
        self,
        user_id: int,
        symbol: str,
        direction: str,
        order_type: str,
        quantity: int,
        price: Optional[float] = None,
        stop_price: Optional[float] = None,
        strategy_id: Optional[int] = None,
        portfolio_id: Optional[int] = None,
        mode: str = "paper",
        paper_account_id: Optional[int] = None,
        buy_date: Optional[str] = None,
    ) -> int:
        with connection("quantmate") as conn:
            result = conn.execute(
                text("""
                    INSERT INTO orders (user_id, portfolio_id, symbol, direction, order_type,
                                        quantity, price, stop_price, strategy_id, mode, status,
                                        paper_account_id, buy_date)
                    VALUES (:uid, :pid, :sym, :dir, :otype, :qty, :price, :stop, :sid, :mode, 'created',
                            :paid, :bdate)
                """),
                {
                    "uid": user_id,
                    "pid": portfolio_id,
                    "sym": symbol,
                    "dir": direction,
                    "otype": order_type,
                    "qty": quantity,
                    "price": price,
                    "stop": stop_price,
                    "sid": strategy_id,
                    "mode": mode,
                    "paid": paper_account_id,
                    "bdate": buy_date,
                },
            )
            conn.commit()
            return int(result.lastrowid)

    def get_by_id(self, order_id: int, user_id: int) -> Optional[dict]:
        with connection("quantmate") as conn:
            row = conn.execute(
                text("""
                    SELECT id, user_id, portfolio_id, symbol, direction, order_type, quantity, price,
                           stop_price, status, filled_quantity, avg_fill_price, fee, strategy_id, mode,
                           paper_account_id, buy_date, created_at, updated_at
                    FROM orders WHERE id = :oid AND user_id = :uid
                """),
                {"oid": order_id, "uid": user_id},
            ).fetchone()
            if not row:
                return None
            return self._row_to_dict(row)

    def list_by_user(
        self, user_id: int, status: Optional[str] = None, mode: Optional[str] = None, page: int = 1, page_size: int = 20
    ) -> tuple[list[dict], int]:
        with connection("quantmate") as conn:
            conditions = ["user_id = :uid"]
            params: dict = {"uid": user_id}
            if status:
                conditions.append("status = :status")
                params["status"] = status
            if mode:
                conditions.append("mode = :mode")
                params["mode"] = mode

            where = " AND ".join(conditions)

            total_row = conn.execute(
                text(f"SELECT COUNT(*) as cnt FROM orders WHERE {where}"),
                params,
            ).fetchone()
            total = total_row.cnt

            params["limit"] = page_size
            params["offset"] = (page - 1) * page_size
            rows = conn.execute(
                text(f"""
                    SELECT id, user_id, portfolio_id, symbol, direction, order_type, quantity, price,
                           stop_price, status, filled_quantity, avg_fill_price, fee, strategy_id, mode,
                           paper_account_id, buy_date, created_at, updated_at
                    FROM orders WHERE {where}
                    ORDER BY created_at DESC LIMIT :limit OFFSET :offset
                """),
                params,
            ).fetchall()
            return [self._row_to_dict(r) for r in rows], total

    def update_status(
        self,
        order_id: int,
        status: str,
        filled_quantity: Optional[int] = None,
        avg_fill_price: Optional[float] = None,
        fee: Optional[float] = None,
    ) -> bool:
        with connection("quantmate") as conn:
            updates = ["status = :status"]
            params: dict = {"oid": order_id, "status": status}
            if filled_quantity is not None:
                updates.append("filled_quantity = :fq")
                params["fq"] = filled_quantity
            if avg_fill_price is not None:
                updates.append("avg_fill_price = :afp")
                params["afp"] = avg_fill_price
            if fee is not None:
                updates.append("fee = :fee")
                params["fee"] = fee

            result = conn.execute(
                text(f"UPDATE orders SET {', '.join(updates)} WHERE id = :oid"),
                params,
            )
            conn.commit()
            return result.rowcount > 0

    def cancel(self, order_id: int, user_id: int) -> bool:
        with connection("quantmate") as conn:
            result = conn.execute(
                text("""
                    UPDATE orders SET status = 'cancelled'
                    WHERE id = :oid AND user_id = :uid AND status IN ('created', 'submitted', 'partial')
                """),
                {"oid": order_id, "uid": user_id},
            )
            conn.commit()
            return result.rowcount > 0

    def insert_trade(self, order_id: int, filled_quantity: int, filled_price: float, fee: float = 0) -> int:
        with connection("quantmate") as conn:
            result = conn.execute(
                text("""
                    INSERT INTO trades (order_id, filled_quantity, filled_price, fee)
                    VALUES (:oid, :qty, :price, :fee)
                """),
                {"oid": order_id, "qty": filled_quantity, "price": filled_price, "fee": fee},
            )
            conn.commit()
            return int(result.lastrowid)

    def _row_to_dict(self, row) -> dict:
        return {
            "id": row.id,
            "user_id": row.user_id,
            "portfolio_id": row.portfolio_id,
            "symbol": row.symbol,
            "direction": row.direction,
            "order_type": row.order_type,
            "quantity": row.quantity,
            "price": float(row.price) if row.price else None,
            "stop_price": float(row.stop_price) if row.stop_price else None,
            "status": row.status,
            "filled_quantity": row.filled_quantity,
            "avg_fill_price": float(row.avg_fill_price) if row.avg_fill_price else None,
            "fee": float(row.fee) if row.fee else 0,
            "strategy_id": row.strategy_id,
            "mode": row.mode,
            "paper_account_id": row.paper_account_id,
            "buy_date": str(row.buy_date) if row.buy_date else None,
            "created_at": row.created_at,
            "updated_at": row.updated_at,
        }
