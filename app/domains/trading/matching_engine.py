"""Matching engine — simulated order fill logic for paper trading.

Handles market/limit/stop order execution with realistic fee models
and slippage simulation. Integrates with realtime_quote_service for
current market prices.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Any, Dict, Optional

logger = logging.getLogger(__name__)


# ── Fee model ───────────────────────────────────────────────


@dataclass
class FeeBreakdown:
    commission: float
    stamp_tax: float
    transfer_fee: float
    other_fee: float

    @property
    def total(self) -> float:
        return self.commission + self.stamp_tax + self.transfer_fee + self.other_fee


def calculate_fee(
    market: str,
    direction: str,
    price: float,
    quantity: int,
) -> FeeBreakdown:
    """Calculate trading fees based on market rules.

    Fee models:
    - CN: commission 0.025% (min ¥5) + stamp tax 0.1% (sell only) + transfer 0.002%
    - HK: commission 0.025% + stamp tax 0.13% (both sides) + levy 0.0027% + settlement 0.005%
    - US: commission $0.005/share (min $1) + SEC fee $0.0000278 per dollar (sell only)
    """
    trade_value = price * quantity
    mkt = market.upper()

    if mkt == "CN":
        commission = max(trade_value * 0.00025, 5.0)
        stamp_tax = trade_value * 0.001 if direction == "sell" else 0.0
        transfer_fee = trade_value * 0.00002
        return FeeBreakdown(
            commission=round(commission, 4),
            stamp_tax=round(stamp_tax, 4),
            transfer_fee=round(transfer_fee, 4),
            other_fee=0.0,
        )

    if mkt == "HK":
        commission = max(trade_value * 0.00025, 0.0)
        stamp_tax = trade_value * 0.0013
        levy = trade_value * 0.000027
        settlement = trade_value * 0.00005
        return FeeBreakdown(
            commission=round(commission, 4),
            stamp_tax=round(stamp_tax, 4),
            transfer_fee=0.0,
            other_fee=round(levy + settlement, 4),
        )

    if mkt == "US":
        commission = max(quantity * 0.005, 1.0)
        sec_fee = trade_value * 0.0000278 if direction == "sell" else 0.0
        return FeeBreakdown(
            commission=round(commission, 4),
            stamp_tax=0.0,
            transfer_fee=0.0,
            other_fee=round(sec_fee, 4),
        )

    # Fallback: flat 0.03%
    total = trade_value * 0.0003
    return FeeBreakdown(commission=round(total, 4), stamp_tax=0.0, transfer_fee=0.0, other_fee=0.0)


# ── Fill result ─────────────────────────────────────────────


@dataclass
class FillResult:
    filled: bool
    fill_price: float = 0.0
    fill_quantity: int = 0
    fee: FeeBreakdown = None  # type: ignore[assignment]
    reason: str = ""

    @property
    def total_cost(self) -> float:
        """Total cost including fees (for buys) or net proceeds (for sells)."""
        return self.fill_price * self.fill_quantity + (self.fee.total if self.fee else 0.0)


# ── Matching logic ──────────────────────────────────────────


_DEFAULT_SLIPPAGE = 0.001  # 0.1% default slippage


def try_fill_market_order(
    *,
    direction: str,
    quantity: int,
    market: str,
    last_price: float,
    slippage: float = _DEFAULT_SLIPPAGE,
) -> FillResult:
    """Attempt to fill a market order at the current price +/- slippage."""
    if last_price <= 0:
        return FillResult(filled=False, reason="No valid market price available")

    if direction == "buy":
        fill_price = round(last_price * (1 + slippage), 4)
    else:
        fill_price = round(last_price * (1 - slippage), 4)

    fee = calculate_fee(market, direction, fill_price, quantity)

    return FillResult(
        filled=True,
        fill_price=fill_price,
        fill_quantity=quantity,
        fee=fee,
    )


def try_fill_limit_order(
    *,
    direction: str,
    quantity: int,
    limit_price: float,
    market: str,
    last_price: float,
) -> FillResult:
    """Check if a limit order can be filled at the current market price.

    Buy limit: fills when last_price <= limit_price (at limit_price).
    Sell limit: fills when last_price >= limit_price (at limit_price).
    """
    if last_price <= 0:
        return FillResult(filled=False, reason="No valid market price")

    if direction == "buy" and last_price <= limit_price:
        fee = calculate_fee(market, direction, limit_price, quantity)
        return FillResult(filled=True, fill_price=limit_price, fill_quantity=quantity, fee=fee)

    if direction == "sell" and last_price >= limit_price:
        fee = calculate_fee(market, direction, limit_price, quantity)
        return FillResult(filled=True, fill_price=limit_price, fill_quantity=quantity, fee=fee)

    return FillResult(filled=False, reason="Price condition not met")


def try_fill_stop_order(
    *,
    direction: str,
    quantity: int,
    stop_price: float,
    market: str,
    last_price: float,
    slippage: float = _DEFAULT_SLIPPAGE,
) -> FillResult:
    """Check if a stop order should be triggered and fill as market.

    Buy stop: triggers when last_price >= stop_price (breakout entry).
    Sell stop: triggers when last_price <= stop_price (stop loss).
    """
    if last_price <= 0:
        return FillResult(filled=False, reason="No valid market price")

    triggered = False
    if direction == "buy" and last_price >= stop_price:
        triggered = True
    elif direction == "sell" and last_price <= stop_price:
        triggered = True

    if not triggered:
        return FillResult(filled=False, reason="Stop price not triggered")

    # Triggered — fill as market order
    return try_fill_market_order(
        direction=direction,
        quantity=quantity,
        market=market,
        last_price=last_price,
        slippage=slippage,
    )


def match_order(
    *,
    order_type: str,
    direction: str,
    quantity: int,
    price: Optional[float],
    stop_price: Optional[float],
    market: str,
    last_price: float,
    slippage: float = _DEFAULT_SLIPPAGE,
) -> FillResult:
    """Unified entry point: try to fill any order type against current market data."""
    if order_type == "market":
        return try_fill_market_order(
            direction=direction, quantity=quantity, market=market,
            last_price=last_price, slippage=slippage,
        )
    if order_type == "limit":
        return try_fill_limit_order(
            direction=direction, quantity=quantity, limit_price=price or 0,
            market=market, last_price=last_price,
        )
    if order_type in ("stop", "stop_limit"):
        return try_fill_stop_order(
            direction=direction, quantity=quantity, stop_price=stop_price or price or 0,
            market=market, last_price=last_price, slippage=slippage,
        )
    return FillResult(filled=False, reason=f"Unknown order type: {order_type}")
