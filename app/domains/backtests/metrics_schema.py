"""Unified backtest metrics schema (TASK-006).

All backtest engines (vnpy / qlib / composite) must convert their raw output
into this schema so the API layer can present and compare them uniformly.

Fields that don't apply to a given engine are kept as ``None`` rather than
omitted, so downstream consumers can rely on a stable set of keys.
"""

from __future__ import annotations

import inspect
from dataclasses import asdict, dataclass
from typing import Any, Dict, Optional


@dataclass
class BacktestMetrics:
    """Unified backtest metrics."""

    # === Returns ===
    total_return: Optional[float]            # Cumulative return (0.15 = 15%)
    annualized_return: Optional[float]       # Annualized return (0.20 = 20%/year)
    benchmark_return: Optional[float]        # Benchmark cumulative return
    excess_return: Optional[float]           # total - benchmark (cumulative)

    # === Risk ===
    max_drawdown: Optional[float]            # Max drawdown (0.15 = 15%, positive)
    max_drawdown_duration: Optional[int]     # Max drawdown duration in trading days
    annualized_volatility: Optional[float]   # Annualized volatility
    downside_deviation: Optional[float]      # Downside deviation

    # === Risk-adjusted returns ===
    sharpe_ratio: Optional[float]            # Sharpe ratio (annualized)
    sortino_ratio: Optional[float]           # Sortino ratio
    information_ratio: Optional[float]       # Information ratio (vs benchmark)
    calmar_ratio: Optional[float]            # Calmar ratio

    # === Trading ===
    win_rate: Optional[float]                # Win rate (0.55 = 55%)
    profit_loss_ratio: Optional[float]       # Profit/loss ratio
    turnover: Optional[float]                # Annualized turnover
    total_trades: Optional[int]              # Total number of trades

    # === Selection metrics (Qlib / Composite only) ===
    ic_mean: Optional[float]                 # IC mean
    ic_std: Optional[float]                  # IC std
    ir: Optional[float]                      # IC-based information ratio
    rank_ic_mean: Optional[float]            # Rank IC mean
    rank_ir: Optional[float]                 # Rank IC-based IR

    # === Metadata ===
    start_date: Optional[str]                # Backtest start date
    end_date: Optional[str]                  # Backtest end date
    trading_days: Optional[int]              # Number of trading days
    benchmark: Optional[str]                 # Benchmark code

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> "BacktestMetrics":
        """Construct from a dict, ignoring unknown keys and treating missing
        keys as ``None``."""
        valid_fields = {f for f in cls.__dataclass_fields__}  # type: ignore[attr-defined]
        safe = {k: v for k, v in d.items() if k in valid_fields}
        # Ensure every required field is present (dataclass with defaults would
        # need them; here we always pass None for missing ones).
        for f in valid_fields:
            safe.setdefault(f, None)
        return cls(**safe)  # type: ignore[arg-type]
