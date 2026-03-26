"""Paper Settlement Service — daily mark-to-market and snapshot generation.

Runs after market close (e.g. 15:30 CST for CN) to:
1. Revalue paper positions at latest market prices
2. Update paper_accounts.market_value
3. Insert daily equity snapshots
"""

from __future__ import annotations

import logging
from datetime import date
from typing import Any, Dict

from sqlalchemy import text

from app.infrastructure.db.connections import connection

logger = logging.getLogger(__name__)


class PaperSettlementService:
    """Daily settlement for all active paper accounts."""

    def settle_all(self, settlement_date: date | None = None) -> Dict[str, Any]:
        """Run end-of-day settlement for all active paper accounts."""
        today = settlement_date or date.today()
        settled = 0
        errors = 0

        with connection("quantmate") as conn:
            accounts = conn.execute(
                text("SELECT id, user_id, market, initial_capital FROM paper_accounts WHERE status = 'active'"),
            ).fetchall()

        for acct in accounts:
            try:
                self._settle_account(acct.id, acct.user_id, acct.market, acct.initial_capital, today)
                settled += 1
            except Exception:
                logger.exception("Settlement failed for account %d", acct.id)
                errors += 1

        logger.info("Settlement complete: settled=%d errors=%d date=%s", settled, errors, today)
        return {"settled": settled, "errors": errors, "date": str(today)}

    def _settle_account(self, account_id: int, user_id: int, market: str, initial_capital: float, today: date) -> None:
        """Settle a single paper account: revalue positions, update market_value, snapshot."""
        from app.domains.market.realtime_quote_service import RealtimeQuoteService

        # Compute net positions from filled paper orders
        with connection("quantmate") as conn:
            rows = conn.execute(
                text("""
                    SELECT symbol, direction, SUM(filled_quantity) as total_qty,
                           SUM(filled_quantity * avg_fill_price) / NULLIF(SUM(filled_quantity), 0) as avg_cost
                    FROM orders
                    WHERE user_id = :uid AND paper_account_id = :aid AND mode = 'paper' AND status = 'filled'
                    GROUP BY symbol, direction
                """),
                {"uid": user_id, "aid": account_id},
            ).fetchall()

        # Build net position map: symbol → net_qty (buy positive, sell negative)
        positions: Dict[str, int] = {}
        for r in rows:
            qty = int(r.total_qty) if r.total_qty else 0
            if r.direction == "buy":
                positions[r.symbol] = positions.get(r.symbol, 0) + qty
            else:
                positions[r.symbol] = positions.get(r.symbol, 0) - qty

        # Revalue with latest quotes
        quote_svc = RealtimeQuoteService()
        total_market_value = 0.0
        for symbol, net_qty in positions.items():
            if net_qty <= 0:
                continue
            try:
                quote = quote_svc.get_quote(symbol, market)
                price = quote.get("last_price") or quote.get("price") or quote.get("current") or 0
                total_market_value += net_qty * float(price)
            except Exception:
                logger.debug("Quote failed for %s during settlement", symbol)

        # Update account
        with connection("quantmate") as conn:
            acct = conn.execute(
                text("SELECT balance, frozen FROM paper_accounts WHERE id = :aid"),
                {"aid": account_id},
            ).fetchone()

            balance = float(acct.balance) if acct else 0
            frozen = float(acct.frozen) if acct else 0
            total_equity = balance + frozen + total_market_value
            daily_pnl = total_equity - initial_capital
            total_pnl = total_equity - initial_capital

            conn.execute(
                text("""
                    UPDATE paper_accounts
                    SET market_value = :mv, total_pnl = :tpnl
                    WHERE id = :aid
                """),
                {"mv": round(total_market_value, 2), "tpnl": round(total_pnl, 2), "aid": account_id},
            )

            # Insert daily snapshot (upsert)
            conn.execute(
                text("""
                    INSERT INTO paper_account_snapshots (account_id, snapshot_date, balance, market_value, total_equity, daily_pnl)
                    VALUES (:aid, :sd, :bal, :mv, :te, :dpnl)
                    ON DUPLICATE KEY UPDATE balance = :bal, market_value = :mv, total_equity = :te, daily_pnl = :dpnl
                """),
                {
                    "aid": account_id,
                    "sd": today,
                    "bal": round(balance, 2),
                    "mv": round(total_market_value, 2),
                    "te": round(total_equity, 2),
                    "dpnl": round(daily_pnl, 2),
                },
            )
            conn.commit()

        logger.info("Account %d settled: market_value=%.2f equity=%.2f", account_id, total_market_value, total_equity)
