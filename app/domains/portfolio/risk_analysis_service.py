"""Value-at-Risk and stress testing service.

Computes parametric VaR, historical VaR, and runs stress-test scenarios
against a portfolio of daily returns.
"""
from __future__ import annotations

import math
import statistics
from typing import Any


class RiskAnalysisService:
    """Portfolio risk analytics: VaR, CVaR, and stress tests."""

    def parametric_var(
        self,
        daily_returns: list[float],
        confidence: float = 0.95,
        holding_period: int = 1,
        portfolio_value: float = 1_000_000,
    ) -> dict[str, Any]:
        """Compute parametric (Gaussian) VaR.

        Assumes returns are normally distributed.
        """
        if len(daily_returns) < 2:
            return {"error": "Need at least 2 data points"}

        mu = statistics.mean(daily_returns)
        sigma = statistics.stdev(daily_returns)

        # Normal quantile approximation (Abramowitz & Stegun)
        z = _norm_ppf(confidence)

        var_1d = -(mu - z * sigma)
        var_hp = var_1d * math.sqrt(holding_period)
        var_dollar = var_hp * portfolio_value

        return {
            "method": "parametric",
            "confidence": confidence,
            "holding_period_days": holding_period,
            "portfolio_value": portfolio_value,
            "daily_mean": round(mu, 6),
            "daily_std": round(sigma, 6),
            "var_pct": round(var_hp, 6),
            "var_dollar": round(var_dollar, 2),
        }

    def historical_var(
        self,
        daily_returns: list[float],
        confidence: float = 0.95,
        holding_period: int = 1,
        portfolio_value: float = 1_000_000,
    ) -> dict[str, Any]:
        """Compute historical VaR from empirical return distribution."""
        if len(daily_returns) < 10:
            return {"error": "Need at least 10 data points"}

        sorted_returns = sorted(daily_returns)
        index = int(len(sorted_returns) * (1 - confidence))
        var_1d = -sorted_returns[index]
        var_hp = var_1d * math.sqrt(holding_period)
        var_dollar = var_hp * portfolio_value

        # CVaR (Expected Shortfall): average of losses beyond VaR
        tail = sorted_returns[: index + 1]
        cvar_1d = -statistics.mean(tail) if tail else var_1d
        cvar_dollar = cvar_1d * math.sqrt(holding_period) * portfolio_value

        return {
            "method": "historical",
            "confidence": confidence,
            "holding_period_days": holding_period,
            "portfolio_value": portfolio_value,
            "observations": len(daily_returns),
            "var_pct": round(var_hp, 6),
            "var_dollar": round(var_dollar, 2),
            "cvar_pct": round(cvar_1d * math.sqrt(holding_period), 6),
            "cvar_dollar": round(cvar_dollar, 2),
        }

    def stress_test(
        self,
        portfolio_value: float,
        position_weights: dict[str, float],
        scenarios: list[dict[str, Any]] | None = None,
    ) -> list[dict[str, Any]]:
        """Run predefined stress-test scenarios.

        Each scenario defines a set of market shocks applied to position weights.
        """
        if scenarios is None:
            scenarios = DEFAULT_SCENARIOS

        results: list[dict[str, Any]] = []
        for scenario in scenarios:
            shocks: dict[str, float] = scenario.get("shocks", {})
            total_impact = 0.0
            details: list[dict[str, Any]] = []

            for symbol, weight in position_weights.items():
                shock_pct = shocks.get(symbol, shocks.get("default", 0))
                pnl = portfolio_value * weight * shock_pct
                total_impact += pnl
                details.append({
                    "symbol": symbol,
                    "weight": round(weight, 4),
                    "shock_pct": round(shock_pct, 4),
                    "pnl_impact": round(pnl, 2),
                })

            results.append({
                "name": scenario["name"],
                "description": scenario.get("description", ""),
                "total_impact": round(total_impact, 2),
                "total_impact_pct": round(total_impact / portfolio_value, 6) if portfolio_value else 0,
                "details": details,
            })

        return results


# ── Default stress scenarios ─────────────────────────────────────────

DEFAULT_SCENARIOS = [
    {
        "name": "Market Crash (-20%)",
        "description": "Broad market drops 20%",
        "shocks": {"default": -0.20},
    },
    {
        "name": "Rate Hike Shock",
        "description": "Interest rate sensitive sectors drop 15%, others -5%",
        "shocks": {"default": -0.05, "banks": -0.15, "real_estate": -0.15},
    },
    {
        "name": "Tech Correction (-25%)",
        "description": "Technology stocks drop 25%, others -10%",
        "shocks": {"default": -0.10, "technology": -0.25, "semiconductors": -0.25},
    },
    {
        "name": "Flash Crash (-10%)",
        "description": "Sudden 10% drop across all positions",
        "shocks": {"default": -0.10},
    },
]


# ── Helpers ──────────────────────────────────────────────────────────

def _norm_ppf(p: float) -> float:
    """Approximate inverse normal CDF (rational approximation)."""
    if p <= 0.5:
        t = math.sqrt(-2 * math.log(p))
    else:
        t = math.sqrt(-2 * math.log(1 - p))
    c0, c1, c2 = 2.515517, 0.802853, 0.010328
    d1, d2, d3 = 1.432788, 0.189269, 0.001308
    z = t - (c0 + c1 * t + c2 * t * t) / (1 + d1 * t + d2 * t * t + d3 * t * t * t)
    return z if p > 0.5 else -z
