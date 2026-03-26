"""Market rules engine — enforces exchange-specific trading constraints.

Validates orders against A-share (T+1, price limits, lot sizes),
HK, and US market rules before they reach the matching engine.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import date, datetime, time
from enum import Enum
from typing import Optional

logger = logging.getLogger(__name__)


class Market(str, Enum):
    CN = "CN"
    HK = "HK"
    US = "US"


@dataclass
class ValidationResult:
    valid: bool
    error: Optional[str] = None


# ── A-share board classification ────────────────────────────


def _cn_board(symbol: str) -> str:
    """Classify a CN symbol into board type for rule differentiation."""
    code = symbol.split(".")[0]
    if code.startswith("688") or code.startswith("689"):
        return "star"  # 科创板
    if code.startswith("300") or code.startswith("301"):
        return "gem"  # 创业板
    if code.startswith("*ST") or code.startswith("ST"):
        return "st"
    return "main"


def _is_st(symbol_name: Optional[str]) -> bool:
    """Detect ST / *ST from the stock name (if available)."""
    if not symbol_name:
        return False
    name = symbol_name.upper()
    return name.startswith("ST") or name.startswith("*ST")


# ── Price limit helpers ─────────────────────────────────────


def cn_price_limit_pct(symbol: str, symbol_name: Optional[str] = None) -> float:
    """Return the price-limit percentage for a CN symbol."""
    if _is_st(symbol_name):
        return 0.05
    board = _cn_board(symbol)
    if board in ("star", "gem"):
        return 0.20
    return 0.10


def cn_price_limits(prev_close: float, symbol: str, symbol_name: Optional[str] = None) -> tuple[float, float]:
    """Return (limit_down, limit_up) for a CN symbol based on prev_close."""
    pct = cn_price_limit_pct(symbol, symbol_name)
    limit_up = round(prev_close * (1 + pct), 2)
    limit_down = round(prev_close * (1 - pct), 2)
    return limit_down, limit_up


# ── Trading session helpers ─────────────────────────────────


_CN_MORNING_OPEN = time(9, 30)
_CN_MORNING_CLOSE = time(11, 30)
_CN_AFTERNOON_OPEN = time(13, 0)
_CN_AFTERNOON_CLOSE = time(15, 0)


def is_cn_trading_hours(now: Optional[datetime] = None) -> bool:
    """Check if current time falls within A-share continuous trading hours."""
    t = (now or datetime.now()).time()
    return (_CN_MORNING_OPEN <= t <= _CN_MORNING_CLOSE) or (_CN_AFTERNOON_OPEN <= t <= _CN_AFTERNOON_CLOSE)


def is_hk_trading_hours(now: Optional[datetime] = None) -> bool:
    t = (now or datetime.now()).time()
    return (time(9, 30) <= t <= time(12, 0)) or (time(13, 0) <= t <= time(16, 0))


def is_us_trading_hours(now: Optional[datetime] = None) -> bool:
    # US Eastern 9:30-16:00 — simplified, caller should convert tz
    t = (now or datetime.now()).time()
    return time(9, 30) <= t <= time(16, 0)


# ── Order validation ────────────────────────────────────────


def validate_order(
    *,
    market: str,
    symbol: str,
    direction: str,
    quantity: int,
    price: Optional[float],
    order_type: str,
    prev_close: Optional[float] = None,
    available_balance: Optional[float] = None,
    available_position: Optional[int] = None,
    buy_date: Optional[date] = None,
    today: Optional[date] = None,
    symbol_name: Optional[str] = None,
) -> ValidationResult:
    """Validate an order against market rules. Returns ValidationResult."""

    mkt = Market(market)

    # ── Common checks ───────────────────────────────────────
    if quantity <= 0:
        return ValidationResult(False, "Quantity must be positive")
    if direction not in ("buy", "sell"):
        return ValidationResult(False, "Direction must be 'buy' or 'sell'")

    # ── CN-specific rules ───────────────────────────────────
    if mkt == Market.CN:
        return _validate_cn(
            symbol=symbol,
            direction=direction,
            quantity=quantity,
            price=price,
            order_type=order_type,
            prev_close=prev_close,
            available_balance=available_balance,
            available_position=available_position,
            buy_date=buy_date,
            today=today,
            symbol_name=symbol_name,
        )

    # ── HK — simplified rules ──────────────────────────────
    if mkt == Market.HK:
        if quantity <= 0:
            return ValidationResult(False, "Quantity must be positive")
        if direction == "sell" and available_position is not None and quantity > available_position:
            return ValidationResult(False, "Insufficient position")
        return ValidationResult(True)

    # ── US — simplified rules ──────────────────────────────
    if mkt == Market.US:
        if quantity <= 0:
            return ValidationResult(False, "Quantity must be positive")
        if direction == "sell" and available_position is not None and quantity > available_position:
            return ValidationResult(False, "Insufficient position")
        return ValidationResult(True)

    return ValidationResult(False, f"Unsupported market: {market}")


def _validate_cn(
    *,
    symbol: str,
    direction: str,
    quantity: int,
    price: Optional[float],
    order_type: str,
    prev_close: Optional[float],
    available_balance: Optional[float],
    available_position: Optional[int],
    buy_date: Optional[date],
    today: Optional[date],
    symbol_name: Optional[str],
) -> ValidationResult:
    """Full A-share rule validation."""

    # Lot-size: must be multiple of 100 for buys
    if direction == "buy" and quantity % 100 != 0:
        return ValidationResult(False, "Buy quantity must be a multiple of 100 shares (整手交易)")

    # Sell lot-size: STAR market allows odd lots, others require 100
    board = _cn_board(symbol)
    if direction == "sell" and board not in ("star",) and quantity % 100 != 0:
        return ValidationResult(False, "Sell quantity must be a multiple of 100 shares")

    # T+1: cannot sell shares bought today
    if direction == "sell" and buy_date is not None:
        check_today = today or date.today()
        if buy_date >= check_today:
            return ValidationResult(False, "T+1 rule: cannot sell shares bought today (当日买入次日方可卖出)")

    # Position check for sells
    if direction == "sell" and available_position is not None and quantity > available_position:
        return ValidationResult(False, "Insufficient position for sell order")

    # Price-limit check (涨跌停)
    if prev_close is not None and prev_close > 0 and price is not None:
        limit_down, limit_up = cn_price_limits(prev_close, symbol, symbol_name)
        if price > limit_up:
            return ValidationResult(False, f"Price {price} exceeds upper limit {limit_up} (涨停)")
        if price < limit_down:
            return ValidationResult(False, f"Price {price} below lower limit {limit_down} (跌停)")

    # Fund check for buys
    if direction == "buy" and price is not None and available_balance is not None:
        estimated_cost = price * quantity * 1.003  # include ~0.3% buffer for fees
        if available_balance < estimated_cost:
            return ValidationResult(False, f"Insufficient funds: need ~{estimated_cost:.2f}, available {available_balance:.2f}")

    return ValidationResult(True)
