"""Paper trading service — simulation environment for strategy deployment and manual orders.

Manages paper trading deployments, computes positions from filled paper orders,
and calculates performance metrics.
"""

from __future__ import annotations

import json
import logging
from typing import Any, Dict, List, Optional

from sqlalchemy import text

from app.domains.composite.dao.composite_strategy_dao import CompositeStrategyDao
from app.domains.composite.dao.strategy_component_dao import StrategyComponentDao
from app.domains.trading.paper_execution_ledger import PaperExecutionLedger
from app.infrastructure.db.connections import connection

logger = logging.getLogger(__name__)

_DEFAULT_STRATEGY_SOURCE_TYPE = "strategy"
_COMPOSITE_STRATEGY_SOURCE_TYPE = "composite"


class PaperTradingService:
    """Service for paper trading simulation environment."""

    def __init__(self) -> None:
        self._ledger = PaperExecutionLedger()

    # ── Deploy / Stop ───────────────────────────────────────

    def deploy(
        self,
        user_id: int,
        strategy_id: Optional[int],
        vt_symbol: str,
        parameters: Optional[Dict[str, Any]] = None,
        paper_account_id: Optional[int] = None,
        execution_mode: str = "auto",
        source_backtest_job_id: Optional[str] = None,
        source_version_id: Optional[int] = None,
        strategy_source_type: str = _DEFAULT_STRATEGY_SOURCE_TYPE,
        composite_strategy_id: Optional[int] = None,
    ) -> Dict[str, Any]:
        """Deploy a strategy to paper trading mode."""
        source_type = (strategy_source_type or _DEFAULT_STRATEGY_SOURCE_TYPE).strip().lower()
        strategy_name: Optional[str] = None
        resolved_vt_symbol = (vt_symbol or "").strip()

        with connection("quantmate") as conn:
            if source_type == _COMPOSITE_STRATEGY_SOURCE_TYPE:
                if composite_strategy_id is None:
                    return {"success": False, "error": "Composite strategy not found"}

                row = conn.execute(
                    text(
                        """
                        SELECT id, name, is_active
                        FROM composite_strategies
                        WHERE id = :cid AND user_id = :uid
                        """
                    ),
                    {"cid": composite_strategy_id, "uid": user_id},
                ).fetchone()
                if not row:
                    return {"success": False, "error": "Composite strategy not found"}
                if not bool(getattr(row, "is_active", True)):
                    return {"success": False, "error": "Composite strategy is inactive"}

                strategy_name = row.name
                if not resolved_vt_symbol:
                    resolved_vt_symbol = self._derive_composite_vt_symbol(user_id, composite_strategy_id)
                if not resolved_vt_symbol:
                    return {
                        "success": False,
                        "error": "vt_symbol or vt_symbols is required for composite strategies without explicit universe symbols",
                    }
            else:
                if strategy_id is None:
                    return {"success": False, "error": "Strategy not found"}

                row = conn.execute(
                    text("SELECT id, name FROM strategies WHERE id = :sid AND user_id = :uid"),
                    {"sid": strategy_id, "uid": user_id},
                ).fetchone()
                if not row:
                    return {"success": False, "error": "Strategy not found"}
                strategy_name = row.name

            result = conn.execute(
                text("""
                    INSERT INTO paper_deployments (user_id, paper_account_id, strategy_id, composite_strategy_id,
                                                   strategy_source_type, strategy_name,
                                                   vt_symbol, parameters, status, execution_mode,
                                                   desired_status, runtime_status,
                                                   source_backtest_job_id, source_version_id)
                    VALUES (:uid, :paid, :sid, :csid, :source_type, :sname, :sym, :params, 'running', :emode,
                            'running', 'pending',
                            :source_backtest_job_id, :source_version_id)
                """),
                {
                    "uid": user_id,
                    "paid": paper_account_id,
                    "sid": strategy_id,
                    "csid": composite_strategy_id,
                    "source_type": source_type,
                    "sname": strategy_name,
                    "sym": resolved_vt_symbol,
                    "params": json.dumps(parameters or {}),
                    "emode": execution_mode,
                    "source_backtest_job_id": source_backtest_job_id,
                    "source_version_id": source_version_id,
                },
            )
            conn.commit()
            deployment_id = int(result.lastrowid)

        logger.info(
            "Paper deployment %d started: source=%s strategy=%s composite=%s symbol=%s mode=%s",
            deployment_id,
            source_type,
            strategy_id,
            composite_strategy_id,
            resolved_vt_symbol,
            execution_mode,
        )
        return {
            "success": True,
            "deployment_id": deployment_id,
            "status": "running",
            "strategy_name": strategy_name,
            "strategy_source_type": source_type,
            "strategy_id": strategy_id,
            "composite_strategy_id": composite_strategy_id,
            "vt_symbol": resolved_vt_symbol,
        }

    def list_deployments(self, user_id: int) -> List[Dict[str, Any]]:
        """List all paper trading deployments for a user."""
        with connection("quantmate") as conn:
            rows = conn.execute(
                text("""
                    SELECT id, strategy_id, composite_strategy_id, strategy_source_type, strategy_name, vt_symbol, parameters,
                           status, started_at, stopped_at, source_backtest_job_id, source_version_id,
                           risk_check_status, risk_check_summary,
                           desired_status, runtime_status, runtime_worker_id,
                           runtime_heartbeat_at, runtime_error, runtime_warning
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
                    "composite_strategy_id": getattr(r, "composite_strategy_id", None),
                    "strategy_source_type": getattr(r, "strategy_source_type", _DEFAULT_STRATEGY_SOURCE_TYPE),
                    "strategy_name": r.strategy_name,
                    "vt_symbol": r.vt_symbol,
                    "parameters": json.loads(r.parameters) if r.parameters else {},
                    "status": r.status,
                    "started_at": str(r.started_at) if r.started_at else None,
                    "stopped_at": str(r.stopped_at) if r.stopped_at else None,
                    "source_backtest_job_id": getattr(r, "source_backtest_job_id", None),
                    "source_version_id": getattr(r, "source_version_id", None),
                    "risk_check_status": getattr(r, "risk_check_status", None),
                    "risk_check_summary": json.loads(r.risk_check_summary) if getattr(r, "risk_check_summary", None) else None,
                    "desired_status": getattr(r, "desired_status", r.status),
                    "runtime_status": getattr(r, "runtime_status", None),
                    "runtime_worker_id": getattr(r, "runtime_worker_id", None),
                    "runtime_heartbeat_at": str(getattr(r, "runtime_heartbeat_at", None)) if getattr(r, "runtime_heartbeat_at", None) else None,
                    "runtime_error": getattr(r, "runtime_error", None),
                    "runtime_warning": getattr(r, "runtime_warning", None),
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
                    SET status = 'stopped',
                        desired_status = 'stopped',
                        runtime_status = CASE
                            WHEN runtime_status IN ('stopped', 'error') THEN runtime_status
                            ELSE 'stopping'
                        END,
                        stopped_at = COALESCE(stopped_at, NOW())
                    WHERE id = :did
                      AND user_id = :uid
                      AND COALESCE(desired_status, status, '') <> 'stopped'
                """),
                {"did": deployment_id, "uid": user_id},
            )
            conn.commit()
            return result.rowcount > 0

    def get_deployment_runtime(self, deployment_id: int, user_id: int) -> Optional[Dict[str, Any]]:
        """Return desired and actual runtime state for a paper deployment."""
        with connection("quantmate") as conn:
            row = conn.execute(
                text(
                    """
                          SELECT d.id, d.strategy_id, d.composite_strategy_id, d.strategy_source_type,
                              d.strategy_name, d.vt_symbol, d.status,
                           d.desired_status, d.runtime_status, d.runtime_worker_id,
                           d.runtime_heartbeat_at, d.runtime_error, d.runtime_warning,
                           h.runtime_mode, h.strategy_kind, h.gateway_name, h.message,
                           h.heartbeat_at
                    FROM paper_deployments d
                    LEFT JOIN paper_runtime_heartbeats h ON h.deployment_id = d.id
                    WHERE d.id = :did AND d.user_id = :uid
                    LIMIT 1
                    """
                ),
                {"did": deployment_id, "uid": user_id},
            ).fetchone()

        if not row:
            return None

        return {
            "deployment_id": row.id,
            "strategy_id": row.strategy_id,
            "composite_strategy_id": getattr(row, "composite_strategy_id", None),
            "strategy_source_type": getattr(row, "strategy_source_type", _DEFAULT_STRATEGY_SOURCE_TYPE),
            "strategy_name": row.strategy_name,
            "vt_symbol": row.vt_symbol,
            "status": row.status,
            "desired_status": getattr(row, "desired_status", row.status),
            "runtime_status": getattr(row, "runtime_status", None),
            "runtime_worker_id": getattr(row, "runtime_worker_id", None),
            "runtime_heartbeat_at": str(getattr(row, "runtime_heartbeat_at", None)) if getattr(row, "runtime_heartbeat_at", None) else None,
            "runtime_error": getattr(row, "runtime_error", None),
            "runtime_warning": getattr(row, "runtime_warning", None),
            "runtime_mode": getattr(row, "runtime_mode", None),
            "strategy_kind": getattr(row, "strategy_kind", None),
            "gateway_name": getattr(row, "gateway_name", None),
            "heartbeat_message": getattr(row, "message", None),
            "heartbeat_at": str(getattr(row, "heartbeat_at", None)) if getattr(row, "heartbeat_at", None) else None,
        }

    @staticmethod
    def _derive_composite_vt_symbol(user_id: int, composite_strategy_id: int) -> str:
        composite_dao = CompositeStrategyDao()
        component_dao = StrategyComponentDao()
        bindings = composite_dao.get_bindings(composite_strategy_id)

        symbols: list[str] = []
        seen: set[str] = set()
        for binding in sorted(bindings, key=lambda item: item.get("ordinal", 0)):
            if binding.get("layer") != "universe":
                continue

            component = component_dao.get_for_user(binding["component_id"], user_id)
            if not component:
                continue

            config = component.get("config") or {}
            if isinstance(config, str):
                try:
                    config = json.loads(config)
                except (TypeError, json.JSONDecodeError):
                    config = {}

            override = binding.get("config_override")
            if isinstance(override, str):
                try:
                    override = json.loads(override)
                except (TypeError, json.JSONDecodeError):
                    override = {}
            if not isinstance(override, dict):
                override = {}

            merged = {**config, **override}
            for symbol in merged.get("symbols", []) or []:
                normalized = str(symbol).strip()
                if not normalized or normalized in seen:
                    continue
                seen.add(normalized)
                symbols.append(normalized)

        return ",".join(symbols)

    # ── Positions ───────────────────────────────────────────

    def get_positions(self, user_id: int) -> List[Dict[str, Any]]:
        """Compute aggregated paper positions from filled paper orders."""
        try:
            ledger_positions = self._ledger.get_positions(user_id=user_id)
            if ledger_positions:
                return ledger_positions
        except Exception:
            logger.warning("Paper ledger positions unavailable, falling back to orders", exc_info=True)

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
        try:
            ledger_summary = self._ledger.get_performance_summary(user_id=user_id)
            if (
                ledger_summary.get("total_trades")
                or ledger_summary.get("equity_curve")
                or abs(float(ledger_summary.get("total_pnl") or 0.0)) > 0
            ):
                return ledger_summary
        except Exception:
            logger.warning("Paper ledger performance unavailable, falling back to orders", exc_info=True)

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
