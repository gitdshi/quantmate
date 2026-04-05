"""Market constraints for composite backtest engine.

Handles A-share market rules: T+1, lot size, price limits, suspension, commissions.
"""

from dataclasses import dataclass
from datetime import date
from typing import Any, Dict, List, Optional, Set


@dataclass
class Order:
    """A trading order produced by the risk engine."""

    symbol: str
    direction: str  # "buy" or "sell"
    quantity: int
    price: float = 0.0
    reason: str = ""


@dataclass
class MarketConstraints:
    """Encapsulates market-specific trading constraints.

    Parameters are loaded from the composite strategy's ``market_constraints`` JSON.
    """

    t_plus_n: int = 1  # T+1 for A-shares
    lot_size: int = 100  # 100 shares per lot for A-shares
    price_limit_pct: float = 0.10  # 10% daily price limit
    slippage_bps: float = 5.0  # 5 basis points slippage
    commission_rate: float = 0.0003  # 0.03% commission
    stamp_tax_rate: float = 0.001  # 0.1% stamp tax (sell only)
    min_commission: float = 5.0  # minimum commission per trade

    @classmethod
    def from_dict(cls, cfg: Optional[Dict[str, Any]] = None) -> "MarketConstraints":
        if not cfg:
            return cls()
        return cls(
            t_plus_n=cfg.get("t_plus_n", 1),
            lot_size=cfg.get("lot_size", 100),
            price_limit_pct=cfg.get("price_limit_pct", 0.10),
            slippage_bps=cfg.get("slippage_bps", 5.0),
            commission_rate=cfg.get("commission_rate", 0.0003),
            stamp_tax_rate=cfg.get("stamp_tax_rate", 0.001),
            min_commission=cfg.get("min_commission", 5.0),
        )

    # ── Constraint application methods ───────────────────

    def apply_t_plus_n(
        self,
        orders: List[Order],
        buy_dates: Dict[str, date],
        trading_day: date,
    ) -> List[Order]:
        """Filter out sell orders for positions bought within T+N window."""
        if self.t_plus_n <= 0:
            return orders
        filtered = []
        for o in orders:
            if o.direction == "sell":
                bought_on = buy_dates.get(o.symbol)
                if bought_on and (trading_day - bought_on).days < self.t_plus_n:
                    continue  # T+N not satisfied
            filtered.append(o)
        return filtered

    def apply_price_limits(
        self,
        orders: List[Order],
        prev_close: Dict[str, float],
        current_close: Dict[str, float],
    ) -> List[Order]:
        """Remove orders that hit the daily price limit (cannot fill)."""
        filtered = []
        for o in orders:
            pc = prev_close.get(o.symbol)
            cc = current_close.get(o.symbol)
            if pc and cc and self.price_limit_pct > 0:
                change_pct = abs(cc - pc) / pc
                if change_pct >= self.price_limit_pct * 0.99:
                    # Price at limit — buy at upper limit or sell at lower limit
                    if o.direction == "buy" and cc > pc:
                        continue  # hitting upper limit, cannot buy
                    if o.direction == "sell" and cc < pc:
                        continue  # hitting lower limit, cannot sell
            filtered.append(o)
        return filtered

    def apply_lot_size(self, orders: List[Order]) -> List[Order]:
        """Round order quantities to lot size multiples."""
        if self.lot_size <= 1:
            return orders
        result = []
        for o in orders:
            rounded_qty = (o.quantity // self.lot_size) * self.lot_size
            if rounded_qty > 0:
                result.append(
                    Order(
                        symbol=o.symbol,
                        direction=o.direction,
                        quantity=rounded_qty,
                        price=o.price,
                        reason=o.reason,
                    )
                )
        return result

    def calculate_fill_price(self, price: float, direction: str) -> float:
        """Apply slippage to get the simulated fill price."""
        slippage_pct = self.slippage_bps / 10000.0
        if direction == "buy":
            return price * (1.0 + slippage_pct)
        else:
            return price * (1.0 - slippage_pct)

    def calculate_commission(self, amount: float, direction: str) -> float:
        """Calculate trading commission + stamp tax."""
        commission = max(amount * self.commission_rate, self.min_commission)
        if direction == "sell":
            commission += amount * self.stamp_tax_rate
        return commission
