"""Performance attribution service — Brinson model."""
from __future__ import annotations

from typing import Any


class PerformanceAttributionService:
    """Brinson-Fachler performance attribution for portfolio analysis."""

    def brinson_attribution(
        self,
        *,
        portfolio_weights: dict[str, float],
        benchmark_weights: dict[str, float],
        portfolio_returns: dict[str, float],
        benchmark_returns: dict[str, float],
    ) -> dict[str, Any]:
        """Compute Brinson attribution: allocation, selection, interaction effects.

        Args:
            portfolio_weights: {sector: weight} for portfolio
            benchmark_weights: {sector: weight} for benchmark
            portfolio_returns: {sector: return} for portfolio
            benchmark_returns: {sector: return} for benchmark
        """
        sectors = set(portfolio_weights.keys()) | set(benchmark_weights.keys())
        details = []
        total_allocation = 0.0
        total_selection = 0.0
        total_interaction = 0.0

        for sector in sorted(sectors):
            wp = portfolio_weights.get(sector, 0.0)
            wb = benchmark_weights.get(sector, 0.0)
            rp = portfolio_returns.get(sector, 0.0)
            rb = benchmark_returns.get(sector, 0.0)

            # Benchmark total return
            rb_total = sum(
                benchmark_weights.get(s, 0) * benchmark_returns.get(s, 0)
                for s in sectors
            )

            allocation = (wp - wb) * (rb - rb_total)
            selection = wb * (rp - rb)
            interaction = (wp - wb) * (rp - rb)

            total_allocation += allocation
            total_selection += selection
            total_interaction += interaction

            details.append({
                "sector": sector,
                "portfolio_weight": round(wp, 4),
                "benchmark_weight": round(wb, 4),
                "portfolio_return": round(rp, 4),
                "benchmark_return": round(rb, 4),
                "allocation_effect": round(allocation, 6),
                "selection_effect": round(selection, 6),
                "interaction_effect": round(interaction, 6),
            })

        total_active = total_allocation + total_selection + total_interaction

        return {
            "allocation_effect": round(total_allocation, 6),
            "selection_effect": round(total_selection, 6),
            "interaction_effect": round(total_interaction, 6),
            "total_active_return": round(total_active, 6),
            "details": details,
        }
