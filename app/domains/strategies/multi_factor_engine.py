"""Multi-factor strategy engine — bridge FactorLab → Strategy creation.

Generates executable strategy code from a list of selected factors and weights:
  1. vnpy CtaTemplate code (factor composite → long/short signal)
  2. Qlib TopkDropout config dict (for AI backtest track)

Also manages the strategy_factors bridge table.
"""

from __future__ import annotations

import json
import logging
import textwrap
from typing import Any, Optional

from sqlalchemy import text

from app.infrastructure.db.connections import connection

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Factor spec for strategy composition
# ---------------------------------------------------------------------------


class FactorSpec:
    """Describes a factor to include in a multi-factor strategy."""

    def __init__(
        self,
        factor_name: str,
        expression: str = "",
        weight: float = 1.0,
        direction: int = 1,
        factor_id: Optional[int] = None,
        factor_set: str = "custom",
    ):
        self.factor_name = factor_name
        self.expression = expression
        self.weight = weight
        self.direction = direction  # 1 = long higher values, -1 = short higher values
        self.factor_id = factor_id
        self.factor_set = factor_set


# ---------------------------------------------------------------------------
# Generate vnpy CtaTemplate code
# ---------------------------------------------------------------------------


def generate_cta_code(
    class_name: str,
    factors: list[FactorSpec],
    lookback_window: int = 20,
    rebalance_interval: int = 5,
    fixed_size: int = 1,
    signal_threshold: float = 0.0,
) -> str:
    """Generate a vnpy CtaTemplate strategy that computes a composite factor score.

    The generated strategy:
    - Computes each factor expression from bar data (via ArrayManager)
    - Normalises and combines with weights
    - Enters long when composite > threshold, short when < -threshold
    """
    factor_names = [f.factor_name.replace(" ", "_").replace("-", "_") for f in factors]
    weights_str = ", ".join(f"{f.weight}" for f in factors)
    directions_str = ", ".join(f"{f.direction}" for f in factors)

    # Build per-factor computation snippets
    factor_computations: list[str] = []
    for i, f in enumerate(factors):
        safe_name = factor_names[i]
        expr = f.expression.strip()
        if not expr:
            # Default to a momentum proxy
            expr = "close / delay(close, 20) - 1"
        factor_computations.append(
            f"        # Factor {i}: {f.factor_name}\n"
            f"        raw_{safe_name} = self._compute_factor_{i}()"
        )

    factor_compute_blocks = "\n".join(factor_computations)

    # Build factor method stubs
    factor_methods: list[str] = []
    for i, f in enumerate(factors):
        expr = f.expression.strip()
        # Map common expressions to ArrayManager
        method = _expression_to_am_method(expr, i, factors[i].factor_name)
        factor_methods.append(method)

    factor_methods_str = "\n\n".join(factor_methods)

    code = textwrap.dedent(f'''\
        """Multi-factor strategy: {class_name}
        
        Auto-generated from FactorLab.
        Factors: {", ".join(f.factor_name for f in factors)}
        """
        
        from vnpy_ctastrategy import (
            CtaTemplate,
            StopOrder,
            TickData,
            BarData,
            TradeData,
            OrderData,
            BarGenerator,
            ArrayManager,
        )
        import numpy as np
        
        
        class {class_name}(CtaTemplate):
            """Multi-factor composite strategy."""
        
            author = "QuantMate-FactorLab"
        
            # Parameters
            lookback_window: int = {lookback_window}
            rebalance_interval: int = {rebalance_interval}
            fixed_size: int = {fixed_size}
            signal_threshold: float = {signal_threshold}
        
            parameters = ["lookback_window", "rebalance_interval", "fixed_size", "signal_threshold"]
            variables = ["composite_score", "bar_count"]
        
            def __init__(self, cta_engine, strategy_name, vt_symbol, setting):
                super().__init__(cta_engine, strategy_name, vt_symbol, setting)
                self.bg = BarGenerator(self.on_bar)
                self.am = ArrayManager(size={max(lookback_window + 10, 50)})
        
                self.composite_score: float = 0.0
                self.bar_count: int = 0
        
                # Factor weights and directions
                self._weights = [{weights_str}]
                self._directions = [{directions_str}]
        
            def on_init(self):
                self.write_log("{class_name} initialising")
                self.load_bar({max(lookback_window + 10, 50)})
        
            def on_start(self):
                self.write_log("{class_name} started")
        
            def on_stop(self):
                self.write_log("{class_name} stopped")
        
            def on_tick(self, tick: TickData):
                self.bg.update_tick(tick)
        
            def on_bar(self, bar: BarData):
                self.cancel_all()
                self.am.update_bar(bar)
                if not self.am.inited:
                    return
        
                self.bar_count += 1
        
                # Rebalance every N bars
                if self.bar_count % self.rebalance_interval != 0:
                    return
        
                # Compute factor values
        {factor_compute_blocks}
        
                # Normalise and combine
                raw_values = [{", ".join(f"raw_{n}" for n in factor_names)}]
                score = 0.0
                for val, w, d in zip(raw_values, self._weights, self._directions):
                    if val is not None and not np.isnan(val):
                        score += val * w * d
                self.composite_score = score
        
                # Trading logic
                if self.pos == 0:
                    if score > self.signal_threshold:
                        self.buy(bar.close_price, self.fixed_size)
                    elif score < -self.signal_threshold:
                        self.short(bar.close_price, self.fixed_size)
                elif self.pos > 0:
                    if score < -self.signal_threshold:
                        self.sell(bar.close_price, abs(self.pos))
                        self.short(bar.close_price, self.fixed_size)
                    elif score < 0:
                        self.sell(bar.close_price, abs(self.pos))
                elif self.pos < 0:
                    if score > self.signal_threshold:
                        self.cover(bar.close_price, abs(self.pos))
                        self.buy(bar.close_price, self.fixed_size)
                    elif score > 0:
                        self.cover(bar.close_price, abs(self.pos))
        
                self.put_event()
        
            def on_order(self, order: OrderData): pass
            def on_trade(self, trade: TradeData): pass
            def on_stop_order(self, stop_order: StopOrder): pass
        
            # --- Factor computation methods ---
        
        {factor_methods_str}
    ''')

    return code


def _expression_to_am_method(expr: str, idx: int, name: str) -> str:
    """Generate a _compute_factor_N method for the CtaTemplate class.

    Uses ArrayManager when possible; otherwise falls back to simple close-based momentum.
    """
    expr_lower = expr.lower().strip() if expr else ""

    # Common patterns → ArrayManager
    if "sma" in expr_lower or "ma(" in expr_lower:
        return textwrap.dedent(f"""\
            def _compute_factor_{idx}(self):
                \"\"\"Factor {idx}: {name}\"\"\"
                if len(self.am.close_array) < self.lookback_window:
                    return 0.0
                sma = self.am.sma(self.lookback_window)
                return (self.am.close_array[-1] - sma) / (sma + 1e-9)""")

    if "rsi" in expr_lower:
        return textwrap.dedent(f"""\
            def _compute_factor_{idx}(self):
                \"\"\"Factor {idx}: {name}\"\"\"
                rsi = self.am.rsi(self.lookback_window)
                return (rsi - 50.0) / 50.0  # normalise to [-1, 1]""")

    if "std" in expr_lower or "volatility" in expr_lower:
        return textwrap.dedent(f"""\
            def _compute_factor_{idx}(self):
                \"\"\"Factor {idx}: {name}\"\"\"
                std = self.am.close_array[-self.lookback_window:].std()
                mean = self.am.close_array[-self.lookback_window:].mean()
                return std / (mean + 1e-9)""")

    # Default: momentum (close / delay) normalised
    return textwrap.dedent(f"""\
        def _compute_factor_{idx}(self):
            \"\"\"Factor {idx}: {name} — momentum proxy\"\"\"
            if len(self.am.close_array) < self.lookback_window + 1:
                return 0.0
            prev = self.am.close_array[-self.lookback_window - 1]
            curr = self.am.close_array[-1]
            return (curr - prev) / (prev + 1e-9)""")


# ---------------------------------------------------------------------------
# Generate Qlib backtest config
# ---------------------------------------------------------------------------


def generate_qlib_config(
    factors: list[FactorSpec],
    universe: str = "csi300",
    start_date: str = "2023-01-01",
    end_date: str = "2024-12-31",
    strategy_type: str = "TopkDropout",
    topk: int = 50,
    n_drop: int = 5,
    benchmark: str = "SH000300",
) -> dict[str, Any]:
    """Generate a Qlib backtest config for the selected factors.

    This combines multiple factors into a composite signal
    and uses Qlib's TopkDropout or WeightedAvg strategy.
    """
    from app.infrastructure.qlib.qlib_config import SUPPORTED_STRATEGIES

    factor_weights = {f.factor_name: f.weight * f.direction for f in factors}

    config = {
        "engine": "qlib",
        "factors": factor_weights,
        "universe": universe,
        "start_date": start_date,
        "end_date": end_date,
        "benchmark": benchmark,
        "strategy": {
            "type": strategy_type,
            "class": SUPPORTED_STRATEGIES.get(strategy_type, SUPPORTED_STRATEGIES["TopkDropout"]),
            "topk": topk,
            "n_drop": n_drop,
        },
    }
    return config


# ---------------------------------------------------------------------------
# Bridge table operations
# ---------------------------------------------------------------------------


def save_strategy_factors(strategy_id: int, factors: list[FactorSpec]) -> None:
    """Save factor-strategy linkages to the bridge table.

    Replaces any existing linkages for the given strategy.
    """
    with connection("quantmate") as conn:
        conn.execute(
            text("DELETE FROM strategy_factors WHERE strategy_id = :sid"),
            {"sid": strategy_id},
        )

        for f in factors:
            conn.execute(
                text(
                    "INSERT INTO strategy_factors "
                    "(strategy_id, factor_id, factor_name, factor_set, weight, direction) "
                    "VALUES (:sid, :fid, :fname, :fset, :w, :d)"
                ),
                {
                    "sid": strategy_id,
                    "fid": f.factor_id,
                    "fname": f.factor_name,
                    "fset": f.factor_set,
                    "w": f.weight,
                    "d": f.direction,
                },
            )
        conn.commit()

    logger.info("[multi-factor] Saved %d factors for strategy %d", len(factors), strategy_id)


def get_strategy_factors(strategy_id: int) -> list[dict[str, Any]]:
    """Retrieve linked factors for a strategy."""
    with connection("quantmate") as conn:
        rows = conn.execute(
            text(
                "SELECT sf.*, fd.expression, fd.category "
                "FROM strategy_factors sf "
                "LEFT JOIN factor_definitions fd ON sf.factor_id = fd.id "
                "WHERE sf.strategy_id = :sid ORDER BY sf.id"
            ),
            {"sid": strategy_id},
        ).fetchall()
    return [dict(r._mapping) for r in rows]


def delete_strategy_factors(strategy_id: int) -> None:
    """Remove all factor linkages for a strategy."""
    with connection("quantmate") as conn:
        conn.execute(
            text("DELETE FROM strategy_factors WHERE strategy_id = :sid"),
            {"sid": strategy_id},
        )
        conn.commit()
