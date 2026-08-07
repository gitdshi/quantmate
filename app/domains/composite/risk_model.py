"""Portfolio risk constraints + optimizer (TASK-010).

A lightweight portfolio optimizer that applies the most common multi-factor
risk constraints: per-stock / per-industry weight caps, Beta neutrality,
volatility cap, and turnover control.

This is intentionally a heuristic implementation — no convex solver dependency.
For production-grade optimization, swap ``PortfolioOptimizer.optimize`` for a
``cvxpy``-based routine.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

import numpy as np
import pandas as pd


@dataclass
class RiskConstraints:
    """Portfolio risk constraint configuration."""

    max_weight_per_stock: float = 0.05          # 单标的最大权重 5%
    max_weight_per_industry: float = 0.30        # 单行业最大权重 30%
    max_portfolio_volatility: float = 0.25       # 组合年化波动率上限 25%
    target_beta: float = 0.0                     # 目标 Beta（0 = 市场中性）
    beta_tolerance: float = 0.2                  # Beta 容差
    max_turnover: float = 0.50                   # 单日换手率上限 50%
    min_holding_count: int = 20                  # 最少持仓数


class PortfolioOptimizer:
    """Heuristic portfolio optimizer satisfying ``RiskConstraints``."""

    def __init__(self, constraints: Optional[RiskConstraints] = None) -> None:
        self.constraints = constraints or RiskConstraints()

    def optimize(
        self,
        scores: pd.Series,
        current_weights: pd.Series,
        market_data: Optional[pd.DataFrame] = None,
        benchmark_beta: float = 1.0,
    ) -> pd.Series:
        """Compute target weights from factor ``scores``.

        Args:
            scores: Factor scores per instrument (higher = better).
            current_weights: Current portfolio weights (for turnover control).
            market_data: Optional DataFrame indexed by instrument with columns
                ``industry``, ``beta``, ``volatility``.
            benchmark_beta: Benchmark beta for neutral constraint.

        Returns:
            Target weights aligned to ``scores.index``.
        """
        if scores.empty:
            return pd.Series(dtype=float)

        weights = pd.Series(0.0, index=scores.index)

        # 1. Pick top-N by score.
        n_holding = max(
            self.constraints.min_holding_count,
            int(1.0 / max(self.constraints.max_weight_per_stock, 1e-6)),
        )
        top_stocks = scores.nlargest(n_holding).index

        # 2. Equal-weight (simplified; production: score-weighted + cvxpy).
        raw_weight = 1.0 / n_holding
        capped_weight = min(raw_weight, self.constraints.max_weight_per_stock)
        weights.loc[top_stocks] = capped_weight

        # 3. Industry exposure cap.
        if market_data is not None and "industry" in market_data.columns:
            weights = self._apply_industry_constraint(weights, market_data)

        # 4. Beta neutrality.
        if market_data is not None and "beta" in market_data.columns:
            weights = self._apply_beta_constraint(weights, market_data, benchmark_beta)

        # 5. Volatility cap.
        if market_data is not None and "volatility" in market_data.columns:
            weights = self._apply_volatility_constraint(weights, market_data)

        # 6. Turnover control.
        if current_weights is not None and not current_weights.empty:
            weights = weights.reindex(current_weights.index, fill_value=0.0)
            weights = self._apply_turnover_constraint(weights, current_weights)

        # 7. Normalize to sum=1.
        total = weights.sum()
        if total > 0:
            weights = weights / total

        return weights

    # ── constraint implementations ────────────────────────────────

    def _apply_industry_constraint(
        self,
        weights: pd.Series,
        market_data: pd.DataFrame,
    ) -> pd.Series:
        industry_map = market_data.set_index(market_data.index)["industry"]
        for industry in industry_map.dropna().unique():
            mask = industry_map == industry
            industry_weight = weights[mask].sum()
            if industry_weight > self.constraints.max_weight_per_industry:
                scale = self.constraints.max_weight_per_industry / max(industry_weight, 1e-9)
                weights[mask] = weights[mask] * scale
        return weights

    def _apply_beta_constraint(
        self,
        weights: pd.Series,
        market_data: pd.DataFrame,
        benchmark_beta: float,
    ) -> pd.Series:
        beta_map = market_data.set_index(market_data.index)["beta"]
        beta_map = beta_map.reindex(weights.index).fillna(0.0)
        portfolio_beta = (weights * beta_map).sum()

        target = self.constraints.target_beta
        tolerance = self.constraints.beta_tolerance
        if abs(portfolio_beta - target) <= tolerance:
            return weights

        # Heuristic: scale down the side contributing to the excess beta.
        excess = portfolio_beta - target
        median_beta = beta_map.median()
        if excess > 0:
            high_beta_mask = beta_map > median_beta
            weights[high_beta_mask] *= 0.9
        else:
            low_beta_mask = beta_map <= median_beta
            weights[low_beta_mask] *= 0.9
        return weights

    def _apply_volatility_constraint(
        self,
        weights: pd.Series,
        market_data: pd.DataFrame,
    ) -> pd.Series:
        vol_map = market_data.set_index(market_data.index)["volatility"]
        vol_map = vol_map.reindex(weights.index).fillna(0.0)
        # Portfolio volatility (approx, ignores correlations).
        portfolio_vol = float(np.sqrt((weights ** 2 * vol_map ** 2).sum())) * np.sqrt(252)
        if portfolio_vol > self.constraints.max_portfolio_volatility:
            scale = self.constraints.max_portfolio_volatility / max(portfolio_vol, 1e-9)
            weights *= scale
        return weights

    def _apply_turnover_constraint(
        self,
        weights: pd.Series,
        current_weights: pd.Series,
    ) -> pd.Series:
        turnover = (weights - current_weights).abs().sum()
        if turnover > self.constraints.max_turnover:
            scale = self.constraints.max_turnover / max(turnover, 1e-9)
            weights = current_weights + (weights - current_weights) * scale
        return weights


# ── Fill price model ─────────────────────────────────────────────────


class FillPriceModel:
    """Unified fill-price model so backtest and paper trading agree."""

    CLOSE = "close"
    VWAP = "vwap"
    NEXT_OPEN = "next_open"
    OPEN = "open"

    _VALID = {CLOSE, VWAP, NEXT_OPEN, OPEN}

    @classmethod
    def validate(cls, model: str) -> str:
        if model not in cls._VALID:
            raise ValueError(f"Unknown fill model: {model!r}. Valid: {cls._VALID}")
        return model

    @staticmethod
    def get_fill_price(
        market_row: pd.Series,
        next_open: Optional[float] = None,
        fill_model: str = CLOSE,
    ) -> float:
        """Return the fill price for a single bar.

        Args:
            market_row: Series with open/high/low/close/vol/volume/amount.
            next_open: Next bar's open (for NEXT_OPEN model).
            fill_model: One of FillPriceModel constants.
        """
        fill_model = FillPriceModel.validate(fill_model)

        if fill_model == FillPriceModel.CLOSE:
            return float(market_row["close"])
        if fill_model == FillPriceModel.OPEN:
            return float(market_row["open"])
        if fill_model == FillPriceModel.VWAP:
            vol = float(market_row.get("vol", 0) or market_row.get("volume", 0) or 0)
            amount = float(market_row.get("amount", 0) or 0)
            if vol > 0 and amount > 0:
                return amount / vol
            return float(market_row["close"])
        if fill_model == FillPriceModel.NEXT_OPEN:
            if next_open is not None and not np.isnan(next_open):
                return float(next_open)
            # Fallback to current close if next_open unavailable.
            return float(market_row["close"])
        # Should never reach here due to validate().
        return float(market_row["close"])
