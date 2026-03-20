"""Paper trading service — simulation environment for strategy deployment and manual orders.

Manages paper trading deployments, computes positions from filled paper orders,
and calculates performance metrics.
"""

from __future__ import annotations

import json
import logging
from typing import Any, Dict, List, Optional

from sqlalchemy import text

from app.infrastructure.db.connections import connection

logger = logging.getLogger(__name__)


class PaperTradingService:
    """Service for paper trading simulation environment."""

    # ── Deploy / Stop ───────────────────────────────────────

    def deploy(
        self,
        user_id: int,
        strategy_id: int,
        vt_symbol: str,
        parameters: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        """Deploy a strategy to paper trading mode."""
        with connection("quantmate") as conn:
            # Verify strategy exists and belongs to user
            row = conn.execute(
                text("SELECT id, name FROM strategies WHERE id = :sid AND user_id = :uid"),
                {"sid": strategy_id, "uid": user_id},
            ).fetchone()
            if not row:
                return {"success": False, "error": "Strategy not found"}

            result = conn.execute(
                text("""
                    INSERT INTO paper_deployments (user_id, strategy_id, strategy_name, vt_symbol, parameters, status)
                    VALUES (:uid, :sid, :sname, :sym, :params, 'running')
                """),
                {
                    "uid": user_id,
                    "sid": strategy_id,
                    "sname": row.name,
                    "sym": vt_symbol,
                    "params": json.dumps(parameters or {}),
                },
            )
            conn.commit()
            deployment_id = int(result.lastrowid)

        logger.info("Paper deployment %d started: strategy=%d symbol=%s", deployment_id, strategy_id, vt_symbol)
        return {"success": True, "deployment_id": deployment_id, "status": "running"}

    def list_deployments(self, user_id: int) -> List[Dict[str, Any]]:
        """List all paper trading deployments for a user."""
        with connection("quantmate") as conn:
            rows = conn.execute(
                text("""
                    SELECT id, strategy_id, strategy_name, vt_symbol, parameters,
                           status, started_at, stopped_at
                    FROM paper_deployments
                    WHERE user_id = :uid
                    ORDER BY started_at DESC
                """),
                {"uid": user_id},
            ).fetchall()
            return [
                {
                    "id": r.id,
                    "strategy_id": r.strategy_id,
                    "strategy_name": r.strategy_name,
                    "vt_symbol": r.vt_symbol,
                    "parameters": json.loads(r.parameters) if r.parameters else {},
                    "status": r.status,
                    "started_at": str(r.started_at) if r.started_at else None,
                    "stopped_at": str(r.stopped_at) if r.stopped_at else None,
                    "pnl": 0.0,  # TODO: compute from paper orders linked to deployment
                }
                for r in rows
            ]

    def stop_deployment(self, deployment_id: int, user_id: int) -> bool:
        """Stop a running paper deployment."""
        with connection("quantmate") as conn:
            result = conn.execute(
                text("""
                    UPDATE paper_deployments
                    SET status = 'stopped', stopped_at = NOW()
                    WHERE id = :did AND user_id = :uid AND status = 'running'
                """),
                {"did": deployment_id, "uid": user_id},
            )
            conn.commit()
            return result.rowcount > 0

    # ── Positions ───────────────────────────────────────────

    def get_positions(self, user_id: int) -> List[Dict[str, Any]]:
        """Compute aggregated paper positions from filled paper orders."""
        with connection("quantmate") as conn:
            rows = conn.execute(
                text("""
                    SELECT symbol, direction,
                           SUM(filled_quantity) as total_qty,
                           SUM(filled_quantity * avg_fill_price) / NULLIF(SUM(filled_quantity), 0) as avg_cost,
                           SUM(fee) as total_fee
                    FROM orders
                    WHERE user_id = :uid AND mode = 'paper' AND status = 'filled'
                    GROUP BY symbol, direction
                """),
                {"uid": user_id},
            ).fetchall()

        positions = []
        for r in rows:
            qty = int(r.total_qty) if r.total_qty else 0
            avg_cost = float(r.avg_cost) if r.avg_cost else 0
            if qty == 0:
                continue
            positions.append(
                {
                    "symbol": r.symbol,
                    "direction": r.direction,
                    "quantity": qty,
                    "avg_cost": round(avg_cost, 4),
                    "current_price": round(avg_cost, 4),  # TODO: integrate real-time quote
                    "pnl": 0.0,
                    "pnl_pct": 0.0,
                }
            )
        return positions

    # ── Performance ─────────────────────────────────────────

    def get_performance(self, user_id: int) -> Dict[str, Any]:
        """Compute paper trading performance metrics from filled orders."""
        with connection("quantmate") as conn:
            rows = conn.execute(
                text("""
                    SELECT id, symbol, direction, filled_quantity, avg_fill_price, fee, created_at
                    FROM orders
                    WHERE user_id = :uid AND mode = 'paper' AND status = 'filled'
                    ORDER BY created_at ASC
                """),
                {"uid": user_id},
            ).fetchall()

        if not rows:
            return {
                "total_pnl": 0.0,
                "total_trades": 0,
                "win_rate": 0.0,
                "max_drawdown": 0.0,
                "sharpe_ratio": None,
                "equity_curve": [],
            }

        total_trades = len(rows)
        total_fees = sum(float(r.fee or 0) for r in rows)

        # Simple P&L: sum of (sell value - buy value) for matched trades
        buy_value = 0.0
        sell_value = 0.0
        for r in rows:
            trade_value = float(r.filled_quantity or 0) * float(r.avg_fill_price or 0)
            if r.direction == "buy":
                buy_value += trade_value
            else:
                sell_value += trade_value

        total_pnl = sell_value - buy_value - total_fees

        # Equity curve (cumulative)
        equity_curve = []
        cumulative = 0.0
        for r in rows:
            trade_value = float(r.filled_quantity or 0) * float(r.avg_fill_price or 0)
            fee = float(r.fee or 0)
            if r.direction == "sell":
                cumulative += trade_value - fee
            else:
                cumulative -= trade_value + fee
            equity_curve.append(
                {
                    "date": str(r.created_at)[:10] if r.created_at else "",
                    "value": round(cumulative, 2),
                }
            )

        # Max drawdown
        peak = 0.0
        max_dd = 0.0
        for point in equity_curve:
            if point["value"] > peak:
                peak = point["value"]
            dd = (peak - point["value"]) / peak if peak > 0 else 0
            if dd > max_dd:
                max_dd = dd

        return {
            "total_pnl": round(total_pnl, 2),
            "total_trades": total_trades,
            "win_rate": 0.0,  # requires trade pairing logic
            "max_drawdown": round(max_dd, 4),
            "sharpe_ratio": None,
            "equity_curve": equity_curve,
        }
