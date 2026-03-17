"""Walk-Forward analysis service."""
from __future__ import annotations

from typing import Any


class WalkForwardService:
    """Walk-Forward analysis for strategy robustness evaluation."""

    def run(
        self,
        *,
        total_bars: int,
        in_sample_pct: float = 0.7,
        num_windows: int = 5,
        metrics_per_window: list[dict[str, Any]] | None = None,
    ) -> dict[str, Any]:
        """Run Walk-Forward analysis (structural scaffold).

        In production, this would:
        1. Split data into rolling in-sample / out-of-sample windows
        2. Optimize parameters on in-sample
        3. Test on out-of-sample
        4. Aggregate results

        This scaffold computes window boundaries and merges provided metrics.
        """
        window_size = total_bars // num_windows if num_windows > 0 else total_bars
        in_sample_size = int(window_size * in_sample_pct)
        out_of_sample_size = window_size - in_sample_size

        windows = []
        for i in range(num_windows):
            start = i * window_size
            split = start + in_sample_size
            end = start + window_size
            window = {
                "window": i + 1,
                "in_sample_range": [start, split],
                "out_of_sample_range": [split, end],
                "in_sample_bars": in_sample_size,
                "out_of_sample_bars": out_of_sample_size,
            }
            if metrics_per_window and i < len(metrics_per_window):
                window["metrics"] = metrics_per_window[i]
            windows.append(window)

        # Aggregate OOS metrics
        oos_returns = []
        if metrics_per_window:
            for m in metrics_per_window:
                if "oos_return" in m:
                    oos_returns.append(m["oos_return"])

        result = {
            "num_windows": num_windows,
            "total_bars": total_bars,
            "in_sample_pct": in_sample_pct,
            "windows": windows,
        }
        if oos_returns:
            result["avg_oos_return"] = sum(oos_returns) / len(oos_returns)
            result["oos_consistency"] = sum(1 for r in oos_returns if r > 0) / len(oos_returns)

        return result


class MonteCarloService:
    """Monte Carlo simulation for strategy robustness evaluation."""

    def run(
        self,
        *,
        trade_returns: list[float],
        num_simulations: int = 1000,
        initial_capital: float = 1000000,
    ) -> dict[str, Any]:
        """Run Monte Carlo simulation by reshuffling trade returns.

        This scaffold computes statistics from provided trade returns.
        In production, this would run full random simulations with numpy.
        """
        if not trade_returns:
            return {
                "num_simulations": num_simulations,
                "error": "No trade returns provided",
            }

        import random
        import statistics

        n = len(trade_returns)
        final_capitals = []

        for _ in range(min(num_simulations, 10000)):
            shuffled = trade_returns.copy()
            random.shuffle(shuffled)
            capital = initial_capital
            max_dd = 0.0
            peak = capital
            for ret in shuffled:
                capital *= (1 + ret)
                if capital > peak:
                    peak = capital
                dd = (peak - capital) / peak if peak > 0 else 0
                if dd > max_dd:
                    max_dd = dd
            final_capitals.append(capital)

        final_capitals.sort()

        return {
            "num_simulations": len(final_capitals),
            "num_trades": n,
            "initial_capital": initial_capital,
            "percentiles": {
                "p5": round(final_capitals[int(len(final_capitals) * 0.05)], 2),
                "p25": round(final_capitals[int(len(final_capitals) * 0.25)], 2),
                "p50": round(final_capitals[int(len(final_capitals) * 0.50)], 2),
                "p75": round(final_capitals[int(len(final_capitals) * 0.75)], 2),
                "p95": round(final_capitals[int(len(final_capitals) * 0.95)], 2),
            },
            "mean_final": round(statistics.mean(final_capitals), 2),
            "std_final": round(statistics.stdev(final_capitals), 2) if len(final_capitals) > 1 else 0,
            "prob_profit": round(sum(1 for c in final_capitals if c > initial_capital) / len(final_capitals), 4),
        }
