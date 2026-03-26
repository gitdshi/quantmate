"""Paper Account DAO.

All SQL touching ``quantmate.paper_accounts`` and ``quantmate.paper_account_snapshots``.
"""

from __future__ import annotations

from datetime import date
from typing import Any, Dict, List, Optional

from sqlalchemy import text

from app.infrastructure.db.connections import connection


class PaperAccountDao:

    # ── CRUD ────────────────────────────────────────────────

    def create(
        self,
        user_id: int,
        name: str,
        initial_capital: float,
        currency: str = "CNY",
        market: str = "CN",
    ) -> int:
        with connection("quantmate") as conn:
            result = conn.execute(
                text("""
                    INSERT INTO paper_accounts
                        (user_id, name, initial_capital, balance, currency, market)
                    VALUES (:uid, :name, :cap, :cap, :cur, :mkt)
                """),
                {"uid": user_id, "name": name, "cap": initial_capital, "cur": currency, "mkt": market},
            )
            conn.commit()
            return int(result.lastrowid)

    def get_by_id(self, account_id: int, user_id: int) -> Optional[Dict[str, Any]]:
        with connection("quantmate") as conn:
            row = conn.execute(
                text("""
                    SELECT id, user_id, name, initial_capital, balance, frozen,
                           market_value, total_pnl, currency, market, status,
                           created_at, updated_at
                    FROM paper_accounts
                    WHERE id = :aid AND user_id = :uid
                """),
                {"aid": account_id, "uid": user_id},
            ).fetchone()
            if not row:
                return None
            return self._row_to_dict(row)

    def list_by_user(self, user_id: int, status: Optional[str] = None) -> List[Dict[str, Any]]:
        with connection("quantmate") as conn:
            conditions = ["user_id = :uid"]
            params: dict = {"uid": user_id}
            if status:
                conditions.append("status = :status")
                params["status"] = status
            where = " AND ".join(conditions)
            rows = conn.execute(
                text(f"""
                    SELECT id, user_id, name, initial_capital, balance, frozen,
                           market_value, total_pnl, currency, market, status,
                           created_at, updated_at
                    FROM paper_accounts
                    WHERE {where}
                    ORDER BY created_at DESC
                """),
                params,
            ).fetchall()
            return [self._row_to_dict(r) for r in rows]

    def close_account(self, account_id: int, user_id: int) -> bool:
        with connection("quantmate") as conn:
            result = conn.execute(
                text("""
                    UPDATE paper_accounts SET status = 'closed'
                    WHERE id = :aid AND user_id = :uid AND status = 'active'
                """),
                {"aid": account_id, "uid": user_id},
            )
            conn.commit()
            return result.rowcount > 0

    # ── Fund operations ─────────────────────────────────────

    def freeze_funds(self, account_id: int, amount: float) -> bool:
        """Atomically move *amount* from balance to frozen."""
        with connection("quantmate") as conn:
            result = conn.execute(
                text("""
                    UPDATE paper_accounts
                    SET balance = balance - :amt, frozen = frozen + :amt
                    WHERE id = :aid AND status = 'active' AND balance >= :amt
                """),
                {"aid": account_id, "amt": amount},
            )
            conn.commit()
            return result.rowcount > 0

    def release_funds(self, account_id: int, amount: float) -> bool:
        """Move *amount* from frozen back to balance (e.g. order cancelled)."""
        with connection("quantmate") as conn:
            result = conn.execute(
                text("""
                    UPDATE paper_accounts
                    SET frozen = frozen - :amt, balance = balance + :amt
                    WHERE id = :aid AND status = 'active' AND frozen >= :amt
                """),
                {"aid": account_id, "amt": amount},
            )
            conn.commit()
            return result.rowcount > 0

    def settle_buy(self, account_id: int, frozen_amount: float, actual_cost: float) -> bool:
        """Settle a buy fill: deduct *frozen_amount* from frozen, refund any diff to balance."""
        diff = frozen_amount - actual_cost
        with connection("quantmate") as conn:
            result = conn.execute(
                text("""
                    UPDATE paper_accounts
                    SET frozen = frozen - :frozen_amt,
                        balance = balance + :diff,
                        total_pnl = total_pnl - :fee_part
                    WHERE id = :aid AND status = 'active'
                """),
                {"aid": account_id, "frozen_amt": frozen_amount, "diff": max(diff, 0), "fee_part": 0},
            )
            conn.commit()
            return result.rowcount > 0

    def settle_sell(self, account_id: int, proceeds: float) -> bool:
        """Credit sell proceeds to balance."""
        with connection("quantmate") as conn:
            result = conn.execute(
                text("""
                    UPDATE paper_accounts
                    SET balance = balance + :amt
                    WHERE id = :aid AND status = 'active'
                """),
                {"aid": account_id, "amt": proceeds},
            )
            conn.commit()
            return result.rowcount > 0

    def update_market_value(self, account_id: int, market_value: float, total_pnl: float) -> bool:
        with connection("quantmate") as conn:
            result = conn.execute(
                text("""
                    UPDATE paper_accounts
                    SET market_value = :mv, total_pnl = :pnl
                    WHERE id = :aid
                """),
                {"aid": account_id, "mv": market_value, "pnl": total_pnl},
            )
            conn.commit()
            return result.rowcount > 0

    # ── Snapshots ───────────────────────────────────────────

    def insert_snapshot(
        self,
        account_id: int,
        snapshot_date: date,
        balance: float,
        market_value: float,
        total_equity: float,
        daily_pnl: float,
    ) -> int:
        with connection("quantmate") as conn:
            result = conn.execute(
                text("""
                    INSERT INTO paper_account_snapshots
                        (account_id, snapshot_date, balance, market_value, total_equity, daily_pnl)
                    VALUES (:aid, :sd, :bal, :mv, :eq, :pnl)
                    ON DUPLICATE KEY UPDATE
                        balance = VALUES(balance),
                        market_value = VALUES(market_value),
                        total_equity = VALUES(total_equity),
                        daily_pnl = VALUES(daily_pnl)
                """),
                {"aid": account_id, "sd": snapshot_date, "bal": balance, "mv": market_value, "eq": total_equity, "pnl": daily_pnl},
            )
            conn.commit()
            return int(result.lastrowid)

    def get_equity_curve(self, account_id: int, limit: int = 365) -> List[Dict[str, Any]]:
        with connection("quantmate") as conn:
            rows = conn.execute(
                text("""
                    SELECT snapshot_date, balance, market_value, total_equity, daily_pnl
                    FROM paper_account_snapshots
                    WHERE account_id = :aid
                    ORDER BY snapshot_date ASC
                    LIMIT :lim
                """),
                {"aid": account_id, "lim": limit},
            ).fetchall()
            return [
                {
                    "date": str(r.snapshot_date),
                    "balance": float(r.balance),
                    "market_value": float(r.market_value),
                    "total_equity": float(r.total_equity),
                    "daily_pnl": float(r.daily_pnl),
                }
                for r in rows
            ]

    # ── Internal ────────────────────────────────────────────

    @staticmethod
    def _row_to_dict(row) -> Dict[str, Any]:
        return {
            "id": row.id,
            "user_id": row.user_id,
            "name": row.name,
            "initial_capital": float(row.initial_capital),
            "balance": float(row.balance),
            "frozen": float(row.frozen),
            "market_value": float(row.market_value),
            "total_pnl": float(row.total_pnl),
            "total_equity": float(row.balance) + float(row.market_value),
            "return_pct": round(
                (float(row.balance) + float(row.market_value) - float(row.initial_capital))
                / float(row.initial_capital)
                * 100,
                2,
            )
            if float(row.initial_capital) > 0
            else 0.0,
            "currency": row.currency,
            "market": row.market,
            "status": row.status,
            "created_at": str(row.created_at) if row.created_at else None,
            "updated_at": str(row.updated_at) if row.updated_at else None,
        }
