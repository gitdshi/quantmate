"""Portfolio tracker for composite backtest engine.

Tracks positions, cash, trades, and equity curve during a composite backtest run.
"""

from dataclasses import dataclass
from datetime import date
from typing import Any, Dict, List, Optional


@dataclass
class Trade:
    """A completed trade record."""

    symbol: str
    direction: str  # "buy" or "sell"
    quantity: int
    price: float
    commission: float
    trading_day: str
    layer_source: str = ""  # which trading component generated this


@dataclass
class PositionInfo:
    """Current position for a single symbol."""

    symbol: str
    quantity: int = 0
    avg_cost: float = 0.0
    buy_date: Optional[date] = None  # latest buy date for T+N tracking

    @property
    def market_value(self) -> float:
        return 0.0  # calculated externally with current prices


class Portfolio:
    """Tracks portfolio state through a backtest simulation."""

    def __init__(self, initial_capital: float):
        self.initial_capital = initial_capital
        self.cash = initial_capital
        self.positions: Dict[str, PositionInfo] = {}
        self.equity_curve: List[Dict[str, Any]] = []
        self.trade_log: List[Dict[str, Any]] = []
        self.position_history: List[Dict[str, Any]] = []
        self._buy_dates: Dict[str, date] = {}  # symbol → latest buy date

    @property
    def buy_dates(self) -> Dict[str, date]:
        return dict(self._buy_dates)

    def execute_trade(self, trade: Trade, trading_day: date) -> bool:
        """Execute a trade and update portfolio state.

        Returns True if the trade was successfully executed.
        """
        cost = trade.price * trade.quantity
        total_cost = cost + trade.commission

        if trade.direction == "buy":
            if total_cost > self.cash:
                return False  # insufficient funds
            self.cash -= total_cost
            pos = self.positions.get(trade.symbol)
            if pos is None:
                pos = PositionInfo(symbol=trade.symbol)
                self.positions[trade.symbol] = pos
            # Update average cost
            old_value = pos.avg_cost * pos.quantity
            new_value = old_value + cost
            pos.quantity += trade.quantity
            pos.avg_cost = new_value / pos.quantity if pos.quantity > 0 else 0
            pos.buy_date = trading_day
            self._buy_dates[trade.symbol] = trading_day

        elif trade.direction == "sell":
            pos = self.positions.get(trade.symbol)
            if pos is None or pos.quantity < trade.quantity:
                return False  # insufficient position
            self.cash += cost - trade.commission
            pos.quantity -= trade.quantity
            if pos.quantity == 0:
                del self.positions[trade.symbol]
                self._buy_dates.pop(trade.symbol, None)

        self.trade_log.append(
            {
                "symbol": trade.symbol,
                "direction": trade.direction,
                "quantity": trade.quantity,
                "price": round(trade.price, 4),
                "commission": round(trade.commission, 2),
                "trading_day": trade.trading_day,
                "layer_source": trade.layer_source,
            }
        )
        return True

    def update_equity(self, trading_day: str, prices: Dict[str, float]) -> float:
        """Calculate and record portfolio equity for the given day.

        Args:
            trading_day: ISO date string.
            prices: symbol → close price mapping.

        Returns:
            Total equity (cash + market value).
        """
        market_value = 0.0
        position_snapshot: Dict[str, Any] = {}
        for symbol, pos in self.positions.items():
            px = prices.get(symbol, pos.avg_cost)
            mv = px * pos.quantity
            market_value += mv
            position_snapshot[symbol] = {
                "quantity": pos.quantity,
                "avg_cost": round(pos.avg_cost, 4),
                "price": round(px, 4),
                "market_value": round(mv, 2),
                "pnl": round((px - pos.avg_cost) * pos.quantity, 2),
            }

        total_equity = self.cash + market_value
        self.equity_curve.append(
            {
                "date": trading_day,
                "equity": round(total_equity, 2),
                "cash": round(self.cash, 2),
                "market_value": round(market_value, 2),
            }
        )
        if position_snapshot:
            self.position_history.append(
                {"date": trading_day, "positions": position_snapshot}
            )
        return total_equity

    def get_metrics(self, benchmark_returns: Optional[List[float]] = None) -> Dict[str, Any]:
        """Compute performance metrics from the equity curve."""
        if len(self.equity_curve) < 2:
            return {}

        equities = [pt["equity"] for pt in self.equity_curve]
        initial = equities[0]
        final = equities[-1]
        total_return = (final - initial) / initial if initial else 0

        # Daily returns
        returns = []
        for i in range(1, len(equities)):
            prev = equities[i - 1]
            returns.append((equities[i] - prev) / prev if prev else 0)

        trading_days = len(returns)
        annual_factor = 252 / trading_days if trading_days > 0 else 1
        annual_return = (1 + total_return) ** annual_factor - 1

        # Max drawdown
        peak = equities[0]
        max_dd = 0.0
        for eq in equities:
            if eq > peak:
                peak = eq
            dd = (peak - eq) / peak if peak else 0
            if dd > max_dd:
                max_dd = dd

        # Sharpe ratio (annualized, risk-free = 0)
        import numpy as np

        ret_arr = np.array(returns)
        std = float(np.std(ret_arr)) if len(ret_arr) > 1 else 0
        sharpe = (float(np.mean(ret_arr)) / std * np.sqrt(252)) if std > 0 else 0

        # Trade stats
        total_trades = len(self.trade_log)
        buys = [t for t in self.trade_log if t["direction"] == "buy"]
        sells = [t for t in self.trade_log if t["direction"] == "sell"]

        # Win rate from round-trip trades (simplified)
        winning_trades = 0
        for sell in sells:
            sym = sell["symbol"]
            matching_buys = [b for b in buys if b["symbol"] == sym]
            if matching_buys:
                avg_buy = sum(b["price"] for b in matching_buys) / len(matching_buys)
                if sell["price"] > avg_buy:
                    winning_trades += 1
        winning_rate = winning_trades / len(sells) if sells else 0

        # Alpha/Beta
        alpha = None
        beta = None
        if benchmark_returns and len(benchmark_returns) >= 2:
            strat_ret = np.array(returns[: len(benchmark_returns)])
            bench_ret = np.array(benchmark_returns[: len(returns)])
            min_len = min(len(strat_ret), len(bench_ret))
            if min_len >= 2:
                strat_ret = strat_ret[:min_len]
                bench_ret = bench_ret[:min_len]
                mask = ~(np.isnan(strat_ret) | np.isnan(bench_ret))
                strat_ret = strat_ret[mask]
                bench_ret = bench_ret[mask]
                if len(strat_ret) >= 2:
                    try:
                        beta_val, alpha_val = np.polyfit(bench_ret, strat_ret, 1)
                        alpha = float(alpha_val * 252)
                        beta = float(beta_val)
                    except Exception:
                        pass

        metrics = {
            "total_return": round(total_return, 6),
            "annual_return": round(annual_return, 6),
            "max_drawdown": round(max_dd, 6),
            "sharpe_ratio": round(sharpe, 4),
            "total_trades": total_trades,
            "winning_rate": round(winning_rate, 4),
        }
        if alpha is not None:
            metrics["alpha"] = round(alpha, 6)
        if beta is not None:
            metrics["beta"] = round(beta, 4)
        return metrics
