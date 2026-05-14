"""Paper execution ledger for runtime checkpoints, events, and position lots."""

from __future__ import annotations

from datetime import date, datetime
import json
import logging
import math
from typing import Any, Dict, Optional

from sqlalchemy import text

from app.domains.trading.dao.paper_account_dao import PaperAccountDao
from app.infrastructure.db.connections import connection

logger = logging.getLogger(__name__)
_TRADING_DAYS_PER_YEAR = 252
_RISK_FREE_RATE = 0.015


class PaperExecutionLedger:
    def __init__(self) -> None:
        self._account_dao = PaperAccountDao()

    def record_order_event(
        self,
        *,
        user_id: int,
        symbol: str,
        event_type: str,
        direction: Optional[str] = None,
        quantity: int = 0,
        price: Optional[float] = None,
        fee: float = 0.0,
        paper_account_id: Optional[int] = None,
        deployment_id: Optional[int] = None,
        order_id: Optional[int] = None,
        payload: Optional[Dict[str, Any]] = None,
        occurred_at: Optional[datetime] = None,
    ) -> None:
        with connection("quantmate") as conn:
            conn.execute(
                text(
                    """
                    INSERT INTO paper_order_events (
                        user_id, paper_account_id, deployment_id, order_id, event_type,
                        symbol, direction, quantity, price, fee, payload, occurred_at
                    ) VALUES (
                        :user_id, :paper_account_id, :deployment_id, :order_id, :event_type,
                        :symbol, :direction, :quantity, :price, :fee, :payload, :occurred_at
                    )
                    """
                ),
                {
                    "user_id": user_id,
                    "paper_account_id": paper_account_id,
                    "deployment_id": deployment_id,
                    "order_id": order_id,
                    "event_type": event_type,
                    "symbol": symbol,
                    "direction": direction,
                    "quantity": quantity,
                    "price": price,
                    "fee": fee,
                    "payload": json.dumps(payload or {}),
                    "occurred_at": occurred_at or datetime.utcnow(),
                },
            )
            conn.commit()

    def record_fill(
        self,
        *,
        user_id: int,
        paper_account_id: int,
        deployment_id: Optional[int],
        order_id: int,
        symbol: str,
        direction: str,
        quantity: int,
        price: float,
        fee: float,
        occurred_at: Optional[datetime] = None,
        payload: Optional[Dict[str, Any]] = None,
    ) -> None:
        fill_time = occurred_at or datetime.utcnow()
        self.record_order_event(
            user_id=user_id,
            paper_account_id=paper_account_id,
            deployment_id=deployment_id,
            order_id=order_id,
            symbol=symbol,
            direction=direction,
            quantity=quantity,
            price=price,
            fee=fee,
            event_type="filled",
            payload=payload,
            occurred_at=fill_time,
        )
        self.record_order_event(
            user_id=user_id,
            paper_account_id=paper_account_id,
            deployment_id=deployment_id,
            order_id=order_id,
            symbol=symbol,
            direction=direction,
            quantity=quantity,
            price=price,
            fee=fee,
            event_type="trade",
            payload=payload,
            occurred_at=fill_time,
        )
        self._apply_fill_to_lots(
            user_id=user_id,
            paper_account_id=paper_account_id,
            deployment_id=deployment_id,
            order_id=order_id,
            symbol=symbol,
            direction=direction,
            quantity=quantity,
            price=price,
            occurred_at=fill_time,
        )
        self.refresh_account_snapshot(paper_account_id=paper_account_id, user_id=user_id)

    def write_checkpoint(self, *, deployment_id: int, runtime_mode: str, strategy_kind: str, checkpoint: Dict[str, Any]) -> None:
        with connection("quantmate") as conn:
            conn.execute(
                text(
                    """
                    INSERT INTO paper_runtime_checkpoints (deployment_id, runtime_mode, strategy_kind, checkpoint_json)
                    VALUES (:deployment_id, :runtime_mode, :strategy_kind, :checkpoint_json)
                    ON DUPLICATE KEY UPDATE
                        runtime_mode = VALUES(runtime_mode),
                        strategy_kind = VALUES(strategy_kind),
                        checkpoint_json = VALUES(checkpoint_json)
                    """
                ),
                {
                    "deployment_id": deployment_id,
                    "runtime_mode": runtime_mode,
                    "strategy_kind": strategy_kind,
                    "checkpoint_json": json.dumps(checkpoint),
                },
            )
            conn.commit()

    def get_positions(self, *, user_id: int, paper_account_id: Optional[int] = None) -> list[Dict[str, Any]]:
        params: Dict[str, Any] = {"uid": user_id}
        account_filter = ""
        if paper_account_id is not None:
            account_filter = "AND l.paper_account_id = :aid"
            params["aid"] = paper_account_id

        with connection("quantmate") as conn:
            rows = conn.execute(
                text(
                    f"""
                    SELECT l.paper_account_id, a.market, l.symbol, l.side,
                           SUM(l.remaining_quantity) AS total_qty,
                           SUM(l.remaining_quantity * l.open_price) / NULLIF(SUM(l.remaining_quantity), 0) AS avg_cost
                    FROM paper_position_lots l
                    JOIN paper_accounts a ON a.id = l.paper_account_id
                    WHERE l.user_id = :uid AND l.status = 'open' AND l.remaining_quantity > 0 {account_filter}
                    GROUP BY l.paper_account_id, a.market, l.symbol, l.side
                    ORDER BY l.symbol ASC
                    """
                ),
                params,
            ).fetchall()

        if not rows:
            return []

        from app.domains.market.realtime_quote_service import RealtimeQuoteService

        quote_svc = RealtimeQuoteService()
        positions: list[Dict[str, Any]] = []
        for row in rows:
            avg_cost = float(row.avg_cost or 0)
            qty = int(row.total_qty or 0)
            if qty <= 0:
                continue
            current_price = avg_cost
            try:
                quote = quote_svc.get_quote(row.symbol, row.market)
                current_price = float(quote.get("last_price") or quote.get("price") or quote.get("current") or avg_cost)
            except Exception:
                logger.debug("[paper-ledger] quote lookup failed for %s", row.symbol, exc_info=True)

            if row.side == "long":
                pnl = (current_price - avg_cost) * qty
                direction = "buy"
            else:
                pnl = (avg_cost - current_price) * qty
                direction = "sell"

            positions.append(
                {
                    "paper_account_id": row.paper_account_id,
                    "symbol": row.symbol,
                    "direction": direction,
                    "quantity": qty,
                    "avg_cost": round(avg_cost, 4),
                    "current_price": round(current_price, 4),
                    "pnl": round(pnl, 2),
                    "pnl_pct": round(((current_price - avg_cost) / avg_cost if avg_cost else 0) * (1 if row.side == "long" else -1), 4),
                }
            )
        return positions

    def get_performance_summary(self, *, user_id: int, paper_account_id: Optional[int] = None) -> Dict[str, Any]:
        params: Dict[str, Any] = {"uid": user_id}
        account_filter = ""
        if paper_account_id is not None:
            account_filter = "AND id = :aid"
            params["aid"] = paper_account_id

        with connection("quantmate") as conn:
            accounts = conn.execute(
                text(f"SELECT id, initial_capital, total_pnl FROM paper_accounts WHERE user_id = :uid {account_filter}"),
                params,
            ).fetchall()
            trade_row = conn.execute(
                text(
                    f"""
                    SELECT COUNT(*) AS total_trades, COALESCE(SUM(fee), 0) AS total_fees
                    FROM paper_order_events
                    WHERE user_id = :uid AND event_type = 'trade' {'AND paper_account_id = :aid' if paper_account_id is not None else ''}
                    """
                ),
                params,
            ).fetchone()
            lot_rows = conn.execute(
                text(
                    f"""
                    SELECT realized_pnl
                    FROM paper_position_lots
                    WHERE user_id = :uid AND status = 'closed' {'AND paper_account_id = :aid' if paper_account_id is not None else ''}
                    """
                ),
                params,
            ).fetchall()
            curve_rows = conn.execute(
                text(
                    f"""
                    SELECT s.snapshot_date,
                           SUM(s.total_equity) AS total_equity,
                           SUM(s.daily_pnl) AS daily_pnl
                    FROM paper_account_snapshots s
                    JOIN paper_accounts a ON a.id = s.account_id
                    WHERE a.user_id = :uid {'AND a.id = :aid' if paper_account_id is not None else ''}
                    GROUP BY s.snapshot_date
                    ORDER BY s.snapshot_date ASC
                    """
                ),
                params,
            ).fetchall()

        if not accounts:
            return {
                "total_pnl": 0.0,
                "total_trades": 0,
                "win_rate": 0.0,
                "max_drawdown": 0.0,
                "sharpe_ratio": None,
                "equity_curve": [],
            }

        total_pnl = round(sum(float(row.total_pnl or 0) for row in accounts), 2)
        closed_lots = [float(row.realized_pnl or 0) for row in lot_rows]
        winning_lots = [pnl for pnl in closed_lots if pnl > 0]
        win_rate = len(winning_lots) / len(closed_lots) if closed_lots else 0.0

        equity_curve = [{"date": str(row.snapshot_date), "value": round(float(row.total_equity or 0), 2)} for row in curve_rows]
        sharpe_ratio, max_drawdown = self._risk_metrics_from_curve(equity_curve)

        return {
            "total_pnl": total_pnl,
            "total_trades": int(getattr(trade_row, "total_trades", 0) or 0),
            "win_rate": round(win_rate, 4),
            "max_drawdown": round(max_drawdown, 4),
            "sharpe_ratio": round(sharpe_ratio, 4) if sharpe_ratio is not None else None,
            "equity_curve": equity_curve,
            "total_fees": round(float(getattr(trade_row, "total_fees", 0) or 0), 2),
        }

    def refresh_account_snapshot(self, *, paper_account_id: int, user_id: int, snapshot_date: Optional[date] = None) -> None:
        snapshot_on = snapshot_date or date.today()
        with connection("quantmate") as conn:
            account = conn.execute(
                text(
                    """
                    SELECT id, balance, frozen, market, initial_capital
                    FROM paper_accounts
                    WHERE id = :aid AND user_id = :uid
                    LIMIT 1
                    """
                ),
                {"aid": paper_account_id, "uid": user_id},
            ).fetchone()

        if not account:
            return

        positions = self.get_positions(user_id=user_id, paper_account_id=paper_account_id)
        total_market_value = 0.0
        for position in positions:
            sign = 1 if position["direction"] == "buy" else -1
            total_market_value += sign * position["quantity"] * position["current_price"]

        balance = float(account.balance or 0)
        frozen = float(account.frozen or 0)
        total_equity = balance + frozen + total_market_value
        total_pnl = total_equity - float(account.initial_capital or 0)

        self._account_dao.update_market_value(paper_account_id, round(total_market_value, 2), round(total_pnl, 2))
        self._account_dao.insert_snapshot(
            account_id=paper_account_id,
            snapshot_date=snapshot_on,
            balance=round(balance, 2),
            market_value=round(total_market_value, 2),
            total_equity=round(total_equity, 2),
            daily_pnl=round(total_pnl, 2),
        )

    def _apply_fill_to_lots(
        self,
        *,
        user_id: int,
        paper_account_id: int,
        deployment_id: Optional[int],
        order_id: int,
        symbol: str,
        direction: str,
        quantity: int,
        price: float,
        occurred_at: datetime,
    ) -> None:
        closing_side = "short" if direction == "buy" else "long"
        opening_side = "long" if direction == "buy" else "short"
        remaining = quantity

        with connection("quantmate") as conn:
            rows = conn.execute(
                text(
                    """
                    SELECT id, remaining_quantity, open_price, realized_pnl
                    FROM paper_position_lots
                    WHERE paper_account_id = :aid AND symbol = :symbol AND side = :side
                      AND status = 'open' AND remaining_quantity > 0
                    ORDER BY opened_at ASC, id ASC
                    """
                ),
                {"aid": paper_account_id, "symbol": symbol, "side": closing_side},
            ).fetchall()

            for row in rows:
                if remaining <= 0:
                    break
                matched_qty = min(remaining, int(row.remaining_quantity or 0))
                if matched_qty <= 0:
                    continue
                lot_price = float(row.open_price or 0)
                pnl = (lot_price - price) * matched_qty if direction == "buy" else (price - lot_price) * matched_qty
                next_remaining = int(row.remaining_quantity or 0) - matched_qty
                conn.execute(
                    text(
                        """
                        UPDATE paper_position_lots
                        SET remaining_quantity = :remaining_quantity,
                            realized_pnl = :realized_pnl,
                            status = CASE WHEN :remaining_quantity = 0 THEN 'closed' ELSE status END,
                            closed_at = CASE WHEN :remaining_quantity = 0 THEN :closed_at ELSE closed_at END
                        WHERE id = :lot_id
                        """
                    ),
                    {
                        "lot_id": row.id,
                        "remaining_quantity": next_remaining,
                        "realized_pnl": float(row.realized_pnl or 0) + pnl,
                        "closed_at": occurred_at,
                    },
                )
                remaining -= matched_qty

            if remaining > 0:
                conn.execute(
                    text(
                        """
                        INSERT INTO paper_position_lots (
                            user_id, paper_account_id, deployment_id, symbol, side,
                            open_quantity, remaining_quantity, open_price, source_order_id,
                            status, opened_at
                        ) VALUES (
                            :user_id, :paper_account_id, :deployment_id, :symbol, :side,
                            :open_quantity, :remaining_quantity, :open_price, :source_order_id,
                            'open', :opened_at
                        )
                        """
                    ),
                    {
                        "user_id": user_id,
                        "paper_account_id": paper_account_id,
                        "deployment_id": deployment_id,
                        "symbol": symbol,
                        "side": opening_side,
                        "open_quantity": remaining,
                        "remaining_quantity": remaining,
                        "open_price": price,
                        "source_order_id": order_id,
                        "opened_at": occurred_at,
                    },
                )
            conn.commit()

    @staticmethod
    def _risk_metrics_from_curve(curve: list[Dict[str, Any]]) -> tuple[Optional[float], float]:
        if len(curve) < 2:
            return None, 0.0

        values = [float(point["value"]) for point in curve]
        returns: list[float] = []
        for index in range(1, len(values)):
            prev = values[index - 1]
            current = values[index]
            if prev > 0:
                returns.append((current - prev) / prev)

        if not returns:
            return None, 0.0

        avg_return = sum(returns) / len(returns)
        variance = sum((item - avg_return) ** 2 for item in returns) / len(returns)
        std_return = math.sqrt(variance)
        daily_rf = _RISK_FREE_RATE / _TRADING_DAYS_PER_YEAR
        sharpe = None
        if std_return > 0:
            sharpe = (avg_return - daily_rf) / std_return * math.sqrt(_TRADING_DAYS_PER_YEAR)

        peak = values[0]
        max_drawdown = 0.0
        for value in values:
            peak = max(peak, value)
            if peak > 0:
                max_drawdown = max(max_drawdown, (peak - value) / peak)
        return sharpe, max_drawdown