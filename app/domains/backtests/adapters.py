"""Backtest engine adapters (TASK-006).

Convert raw metrics produced by each backtest engine into the unified
:class:`BacktestMetrics` schema so the API can return / compare results
across engines.
"""

from __future__ import annotations

from typing import Any, Dict, Optional

from app.domains.backtests.metrics_schema import BacktestMetrics


def _to_float(v: Any) -> Optional[float]:
    """Best-effort float coercion that preserves ``None``."""
    if v is None:
        return None
    try:
        f = float(v)
    except (TypeError, ValueError):
        return None
    if f != f:  # NaN check
        return None
    return f


def _annualize_return(total_return: Optional[float], trading_days: Optional[int]) -> Optional[float]:
    """Convert cumulative return to annualized return.

    Uses (1+r)^(252/N) - 1, matching vnpy's convention.
    """
    if total_return is None or trading_days is None or trading_days <= 0:
        return None
    try:
        return (1.0 + total_return) ** (252.0 / trading_days) - 1.0
    except (ValueError, OverflowError):
        return None


def from_vnpy(vnpy_metrics: Dict[str, Any], start_date: str, end_date: str) -> BacktestMetrics:
    """Convert vnpy ``BacktestingEngine`` statistics."""
    total_return = _to_float(vnpy_metrics.get("total_return"))
    trading_days_raw = vnpy_metrics.get("total_days") or vnpy_metrics.get("trading_days")
    trading_days = int(trading_days_raw) if trading_days_raw is not None else None

    return BacktestMetrics(
        total_return=total_return,
        annualized_return=_annualize_return(total_return, trading_days),
        benchmark_return=_to_float(vnpy_metrics.get("benchmark_return")),
        excess_return=_to_float(vnpy_metrics.get("excess_return")),
        max_drawdown=_to_float(vnpy_metrics.get("max_drawdown")),
        max_drawdown_duration=vnpy_metrics.get("max_drawdown_duration"),
        annualized_volatility=_to_float(vnpy_metrics.get("annualized_volatility")),
        downside_deviation=None,
        sharpe_ratio=_to_float(vnpy_metrics.get("sharpe_ratio")),
        sortino_ratio=None,
        information_ratio=None,
        calmar_ratio=None,
        win_rate=_to_float(vnpy_metrics.get("win_rate")),
        profit_loss_ratio=_to_float(vnpy_metrics.get("profit_loss_ratio")),
        turnover=_to_float(vnpy_metrics.get("turnover")),
        total_trades=vnpy_metrics.get("total_trade_count"),
        ic_mean=None,
        ic_std=None,
        ir=None,
        rank_ic_mean=None,
        rank_ir=None,
        start_date=start_date,
        end_date=end_date,
        trading_days=trading_days,
        benchmark=vnpy_metrics.get("benchmark"),
    )


def from_qlib(qlib_metrics: Dict[str, Any], start_date: str, end_date: str) -> BacktestMetrics:
    """Convert Qlib backtest statistics.

    Qlib's ``risk_analysis`` returns a DataFrame whose index looks like
    ``mean_annual_return``, ``information_ratio``, ``max_drawdown`` etc.
    The worker flattens it into a dict before calling this adapter.
    """
    return BacktestMetrics(
        total_return=_to_float(qlib_metrics.get("cumulative_return") or qlib_metrics.get("total_return")),
        annualized_return=_to_float(
            qlib_metrics.get("annualized_return") or qlib_metrics.get("mean_annual_return_annual")
        ),
        benchmark_return=_to_float(qlib_metrics.get("benchmark_cumulative_return")),
        excess_return=_to_float(qlib_metrics.get("excess_cumulative_return")),
        max_drawdown=_to_float(qlib_metrics.get("max_drawdown")),
        max_drawdown_duration=qlib_metrics.get("max_drawdown_duration"),
        annualized_volatility=_to_float(
            qlib_metrics.get("annualized_volatility") or qlib_metrics.get("mean_annual_volatility_annual")
        ),
        downside_deviation=None,
        sharpe_ratio=_to_float(qlib_metrics.get("sharpe_ratio")),
        sortino_ratio=None,
        information_ratio=_to_float(qlib_metrics.get("information_ratio")),
        calmar_ratio=None,
        win_rate=None,
        profit_loss_ratio=None,
        turnover=_to_float(qlib_metrics.get("turnover")),
        total_trades=None,
        ic_mean=_to_float(qlib_metrics.get("ic_mean")),
        ic_std=_to_float(qlib_metrics.get("ic_std")),
        ir=_to_float(qlib_metrics.get("ir")),
        rank_ic_mean=_to_float(qlib_metrics.get("rank_ic_mean")),
        rank_ir=_to_float(qlib_metrics.get("rank_ir")),
        start_date=start_date,
        end_date=end_date,
        trading_days=qlib_metrics.get("trading_days"),
        benchmark=qlib_metrics.get("benchmark"),
    )


def from_composite(comp_metrics: Dict[str, Any], start_date: str, end_date: str) -> BacktestMetrics:
    """Convert Composite backtest metrics.

    The composite engine already emits a dict — we just pass it through the
    unified schema and fill metadata.
    """
    payload = dict(comp_metrics)
    payload.setdefault("start_date", start_date)
    payload.setdefault("end_date", end_date)
    return BacktestMetrics.from_dict(payload)
