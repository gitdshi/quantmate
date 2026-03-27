"""Composite strategy backtest engine.

Daily-frequency backtest for composite strategies with market constraints.
Orchestrates the Universe → Trading → Risk pipeline across a date range.
"""

import json
import logging
from datetime import date, datetime, timedelta
from typing import Any, Dict, List, Optional

from app.domains.composite.market_constraints import MarketConstraints, Order
from app.domains.composite.orchestrator import CompositeStrategyOrchestrator
from app.domains.composite.portfolio import Portfolio, Trade

logger = logging.getLogger(__name__)


class CompositeBacktestEngine:
    """Daily-frequency composite strategy backtest engine.

    Walks through trading days, runs the orchestrator pipeline each day,
    applies market constraints, executes orders, and tracks portfolio equity.
    """

    def __init__(
        self,
        orchestrator: CompositeStrategyOrchestrator,
        constraints: MarketConstraints,
        initial_capital: float = 1_000_000.0,
        benchmark: str = "000300.SH",
    ):
        self.orchestrator = orchestrator
        self.constraints = constraints
        self.initial_capital = initial_capital
        self.benchmark = benchmark

    def run(
        self,
        start_date: str,
        end_date: str,
        market_data_by_day: Dict[str, Dict[str, Dict[str, float]]],
        all_symbols: List[str],
        benchmark_data: Optional[Dict[str, float]] = None,
    ) -> Dict[str, Any]:
        """Execute the full backtest.

        Args:
            start_date: ISO date string (YYYY-MM-DD).
            end_date: ISO date string (YYYY-MM-DD).
            market_data_by_day: date_str → symbol → {open, high, low, close, volume, prev_close}.
            all_symbols: Full list of tradable symbols.
            benchmark_data: Optional date_str → benchmark close price for alpha/beta.

        Returns:
            Dict with equity_curve, trade_log, position_history, metrics, attribution.
        """
        portfolio = Portfolio(self.initial_capital)
        trading_days = sorted(market_data_by_day.keys())
        trading_days = [d for d in trading_days if start_date <= d <= end_date]

        if not trading_days:
            return self._empty_result()

        # Track layer-level attribution
        universe_contribution: Dict[str, int] = {}  # symbol → times selected
        trading_signals_count = {"buy": 0, "sell": 0}
        risk_filtered_count = 0

        for day_str in trading_days:
            day_data = market_data_by_day.get(day_str, {})
            prices = {s: d.get("close", 0) for s, d in day_data.items()}
            prev_close = {s: d.get("prev_close", 0) for s, d in day_data.items()}

            # Build position info dict for orchestrator
            pos_info: Dict[str, Any] = {}
            for sym, pos in portfolio.positions.items():
                held_days = 0
                if pos.buy_date:
                    held_days = (
                        datetime.strptime(day_str, "%Y-%m-%d").date() - pos.buy_date
                    ).days
                pos_info[sym] = {
                    "quantity": pos.quantity,
                    "avg_cost": pos.avg_cost,
                    "held_days": held_days,
                }

            # Run orchestrator pipeline
            orders = self.orchestrator.run_day(
                trading_day=day_str,
                all_symbols=all_symbols,
                market_data=day_data,
                prices=prices,
                cash=portfolio.cash,
                positions=pos_info,
            )

            # Apply market constraints
            # Step 4: T+N filtering
            orders = self.constraints.apply_t_plus_n(
                orders,
                portfolio.buy_dates,
                datetime.strptime(day_str, "%Y-%m-%d").date(),
            )

            # Step 5: Price limit filtering
            orders = self.constraints.apply_price_limits(
                orders, prev_close, prices
            )

            # Step 6: Lot size rounding
            orders = self.constraints.apply_lot_size(orders)

            risk_filtered_count += len(orders)

            # Step 7: Execute orders with fill prices
            for order in orders:
                fill_price = self.constraints.calculate_fill_price(
                    order.price, order.direction
                )
                amount = fill_price * order.quantity
                commission = self.constraints.calculate_commission(
                    amount, order.direction
                )

                trade = Trade(
                    symbol=order.symbol,
                    direction=order.direction,
                    quantity=order.quantity,
                    price=fill_price,
                    commission=commission,
                    trading_day=day_str,
                    layer_source=order.reason,
                )
                portfolio.execute_trade(
                    trade, datetime.strptime(day_str, "%Y-%m-%d").date()
                )

                if order.direction == "buy":
                    trading_signals_count["buy"] += 1
                    universe_contribution[order.symbol] = (
                        universe_contribution.get(order.symbol, 0) + 1
                    )
                else:
                    trading_signals_count["sell"] += 1

            # Step 8: Update portfolio equity
            portfolio.update_equity(day_str, prices)

        # Compute benchmark returns for alpha/beta
        benchmark_returns = None
        if benchmark_data and len(trading_days) > 1:
            bench_prices = [
                benchmark_data.get(d) for d in trading_days if d in benchmark_data
            ]
            bench_prices = [p for p in bench_prices if p is not None and p > 0]
            if len(bench_prices) > 1:
                benchmark_returns = [
                    (bench_prices[i] - bench_prices[i - 1]) / bench_prices[i - 1]
                    for i in range(1, len(bench_prices))
                ]

        metrics = portfolio.get_metrics(benchmark_returns)

        # Layer attribution summary
        attribution = {
            "universe": {
                "symbols_selected": len(universe_contribution),
                "top_symbols": sorted(
                    universe_contribution.items(), key=lambda x: -x[1]
                )[:10],
            },
            "trading": {
                "total_buy_signals": trading_signals_count["buy"],
                "total_sell_signals": trading_signals_count["sell"],
            },
            "risk": {
                "orders_after_risk": risk_filtered_count,
            },
        }

        return {
            "equity_curve": portfolio.equity_curve,
            "trade_log": portfolio.trade_log,
            "position_history": portfolio.position_history[-5:],  # last 5 snapshots
            "metrics": metrics,
            "attribution": attribution,
        }

    def _empty_result(self) -> Dict[str, Any]:
        return {
            "equity_curve": [],
            "trade_log": [],
            "position_history": [],
            "metrics": {},
            "attribution": {},
        }
