"""Paper Analytics Service — compute performance metrics for paper accounts.

Provides Sharpe ratio, Sortino ratio, max drawdown, win rate, profit factor,
and benchmark comparison for paper trading portfolios.
"""

from __future__ import annotations

import logging
import math
from typing import Any, Dict, List

from sqlalchemy import text

from app.infrastructure.db.connections import connection

logger = logging.getLogger(__name__)

# Benchmark annualized return assumption
_RISK_FREE_RATE = 0.015  # 1.5% annualized
_TRADING_DAYS_PER_YEAR = 252


class PaperAnalyticsService:
    """Compute analytics for a paper trading account."""

    def get_analytics(self, account_id: int, user_id: int) -> Dict[str, Any]:
        """Return comprehensive analytics for a paper account."""
        # Verify ownership
        with connection("quantmate") as conn:
            acct = conn.execute(
                text("SELECT id, initial_capital, balance, market_value, total_pnl FROM paper_accounts WHERE id = :aid AND user_id = :uid"),
                {"aid": account_id, "uid": user_id},
            ).fetchone()
        if not acct:
            return {"error": "Account not found"}

        trades = self._get_closed_trades(account_id, user_id)
        equity_curve = self._get_equity_curve(account_id)

        return {
            "account_id": account_id,
            "initial_capital": float(acct.initial_capital),
            "current_equity": float(acct.balance) + float(acct.market_value),
            "total_pnl": float(acct.total_pnl),
            "total_return_pct": float(acct.total_pnl) / float(acct.initial_capital) * 100 if acct.initial_capital else 0,
            **self._compute_trade_metrics(trades),
            **self._compute_risk_metrics(equity_curve, float(acct.initial_capital)),
        }

    def _get_closed_trades(self, account_id: int, user_id: int) -> List[Dict[str, Any]]:
        """Get all filled orders for the account."""
        with connection("quantmate") as conn:
            rows = conn.execute(
                text("""
                    SELECT id, symbol, direction, filled_quantity, avg_fill_price, fee, created_at
                    FROM orders
                    WHERE user_id = :uid AND paper_account_id = :aid AND mode = 'paper' AND status = 'filled'
                    ORDER BY created_at ASC
                """),
                {"uid": user_id, "aid": account_id},
            ).fetchall()
        return [
            {
                "id": r.id,
                "symbol": r.symbol,
                "direction": r.direction,
                "quantity": int(r.filled_quantity) if r.filled_quantity else 0,
                "price": float(r.avg_fill_price) if r.avg_fill_price else 0,
                "fee": float(r.fee) if r.fee else 0,
                "created_at": str(r.created_at) if r.created_at else "",
            }
            for r in rows
        ]

    def _get_equity_curve(self, account_id: int) -> List[Dict[str, Any]]:
        """Get daily snapshots for risk computation."""
        with connection("quantmate") as conn:
            rows = conn.execute(
                text("""
                    SELECT snapshot_date, total_equity, daily_pnl
                    FROM paper_account_snapshots
                    WHERE account_id = :aid
                    ORDER BY snapshot_date ASC
                """),
                {"aid": account_id},
            ).fetchall()
        return [
            {"date": str(r.snapshot_date), "equity": float(r.total_equity), "pnl": float(r.daily_pnl)}
            for r in rows
        ]

    def _compute_trade_metrics(self, trades: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Compute trade-level metrics: total trades, win rate, profit factor."""
        if not trades:
            return {
                "total_trades": 0,
                "buy_trades": 0,
                "sell_trades": 0,
                "win_rate": 0.0,
                "profit_factor": 0.0,
                "avg_trade_pnl": 0.0,
                "total_fees": 0.0,
            }

        # Pair buy/sell trades by symbol to compute round-trip P&L
        buys: Dict[str, List[Dict]] = {}
        round_trips: List[float] = []
        total_fees = sum(t["fee"] for t in trades)

        for t in trades:
            sym = t["symbol"]
            if t["direction"] == "buy":
                buys.setdefault(sym, []).append(t)
            elif t["direction"] == "sell":
                if sym in buys and buys[sym]:
                    buy = buys[sym].pop(0)
                    pnl = (t["price"] - buy["price"]) * min(t["quantity"], buy["quantity"]) - t["fee"] - buy["fee"]
                    round_trips.append(pnl)

        wins = [p for p in round_trips if p > 0]
        losses = [p for p in round_trips if p <= 0]
        win_rate = len(wins) / len(round_trips) * 100 if round_trips else 0

        gross_profit = sum(wins) if wins else 0
        gross_loss = abs(sum(losses)) if losses else 0
        profit_factor = gross_profit / gross_loss if gross_loss > 0 else float("inf") if gross_profit > 0 else 0

        return {
            "total_trades": len(trades),
            "buy_trades": sum(1 for t in trades if t["direction"] == "buy"),
            "sell_trades": sum(1 for t in trades if t["direction"] == "sell"),
            "round_trips": len(round_trips),
            "win_rate": round(win_rate, 2),
            "profit_factor": round(profit_factor, 4) if profit_factor != float("inf") else None,
            "avg_trade_pnl": round(sum(round_trips) / len(round_trips), 2) if round_trips else 0,
            "total_fees": round(total_fees, 2),
        }

    def _compute_risk_metrics(self, equity_curve: List[Dict[str, Any]], initial_capital: float) -> Dict[str, Any]:
        """Compute Sharpe, Sortino, max drawdown from daily equity snapshots."""
        if len(equity_curve) < 2:
            return {
                "sharpe_ratio": None,
                "sortino_ratio": None,
                "max_drawdown": 0.0,
                "max_drawdown_pct": 0.0,
                "calmar_ratio": None,
                "equity_curve": equity_curve,
            }

        equities = [e["equity"] for e in equity_curve]
        daily_returns = []
        for i in range(1, len(equities)):
            if equities[i - 1] > 0:
                daily_returns.append((equities[i] - equities[i - 1]) / equities[i - 1])

        if not daily_returns:
            return {
                "sharpe_ratio": None,
                "sortino_ratio": None,
                "max_drawdown": 0.0,
                "max_drawdown_pct": 0.0,
                "calmar_ratio": None,
                "equity_curve": equity_curve,
            }

        # Sharpe ratio
        avg_return = sum(daily_returns) / len(daily_returns)
        std_return = math.sqrt(sum((r - avg_return) ** 2 for r in daily_returns) / len(daily_returns)) if len(daily_returns) > 1 else 0
        daily_rf = _RISK_FREE_RATE / _TRADING_DAYS_PER_YEAR
        sharpe = (avg_return - daily_rf) / std_return * math.sqrt(_TRADING_DAYS_PER_YEAR) if std_return > 0 else None

        # Sortino ratio (downside deviation)
        downside = [min(r - daily_rf, 0) ** 2 for r in daily_returns]
        downside_std = math.sqrt(sum(downside) / len(downside)) if downside else 0
        sortino = (avg_return - daily_rf) / downside_std * math.sqrt(_TRADING_DAYS_PER_YEAR) if downside_std > 0 else None

        # Max drawdown
        peak = equities[0]
        max_dd = 0.0
        for eq in equities:
            if eq > peak:
                peak = eq
            dd = peak - eq
            if dd > max_dd:
                max_dd = dd

        max_dd_pct = (max_dd / peak * 100) if peak > 0 else 0

        # Calmar ratio
        annualized_return = avg_return * _TRADING_DAYS_PER_YEAR
        calmar = annualized_return / (max_dd_pct / 100) if max_dd_pct > 0 else None

        return {
            "sharpe_ratio": round(sharpe, 4) if sharpe is not None else None,
            "sortino_ratio": round(sortino, 4) if sortino is not None else None,
            "max_drawdown": round(max_dd, 2),
            "max_drawdown_pct": round(max_dd_pct, 2),
            "calmar_ratio": round(calmar, 4) if calmar is not None else None,
            "equity_curve": equity_curve,
        }
