"""Component-level backtesting.

Runs isolated backtests for individual strategy components
(universe / trading / risk) with layer-appropriate mock inputs.
"""

from __future__ import annotations

import importlib
from typing import Any, Dict, Optional


def run_component_backtest(
    layer: str,
    sub_type: str,
    code: Optional[str],
    config: Dict[str, Any],
    params: Dict[str, Any],
) -> Dict[str, Any]:
    """Execute a component backtest and return layer-specific results.

    For now this dynamically imports the strategy module from
    ``strategies/<layer>/<sub_type>.py`` and runs it against synthetic
    market data to produce a quick validation result.
    """
    module_path = f"strategies.{layer}.{sub_type}"
    try:
        mod = importlib.import_module(module_path)
    except ModuleNotFoundError:
        return {"error": f"Module {module_path} not found"}

    merged_config = {**config, **params}

    if layer == "universe":
        return _backtest_universe(mod, merged_config)
    elif layer == "trading":
        return _backtest_trading(mod, merged_config)
    elif layer == "risk":
        return _backtest_risk(mod, merged_config)
    else:
        return {"error": f"Unknown layer: {layer}"}


# ── Layer-specific backtests ─────────────────────────────────────────────


def _backtest_universe(mod: Any, config: Dict[str, Any]) -> Dict[str, Any]:
    """Run universe select() against synthetic data and report counts."""
    all_symbols = [f"SYM{i:04d}" for i in range(200)]
    # generate synthetic market data
    import random

    random.seed(42)
    market_data: Dict[str, Dict[str, float]] = {}
    for sym in all_symbols:
        market_data[sym] = {
            "market_cap": random.uniform(1e9, 5e11),
            "avg_volume_20d": random.uniform(5e5, 1e7),
            "turnover_rate": random.uniform(0.001, 0.03),
            "close": random.uniform(5, 200),
            "pe_ratio": random.uniform(5, 80),
            "pb_ratio": random.uniform(0.5, 15),
            "roe": random.uniform(-0.05, 0.30),
            "revenue_growth_yoy": random.uniform(-0.2, 0.5),
            "is_st": random.random() < 0.05,
            "is_suspended": random.random() < 0.02,
            "list_days": random.randint(10, 3000),
            "is_csi300": random.random() < 0.15,
            "sector": random.choice(["Tech", "Finance", "Consumer", "Health", "Energy"]),
            "sector_momentum_20d": random.uniform(-0.1, 0.15),
        }

    selected = mod.select("2025-01-15", all_symbols, market_data, config=config)
    return {
        "layer": "universe",
        "total_input": len(all_symbols),
        "selected_count": len(selected),
        "pass_rate": round(len(selected) / len(all_symbols), 4) if all_symbols else 0,
        "sample_symbols": selected[:10],
    }


def _backtest_trading(mod: Any, config: Dict[str, Any]) -> Dict[str, Any]:
    """Run trading generate_signals() against synthetic data."""
    import random

    random.seed(42)
    universe = [f"SYM{i:04d}" for i in range(50)]
    market_data: Dict[str, Dict[str, float]] = {}
    for sym in universe:
        close = random.uniform(10, 200)
        market_data[sym] = {
            "close": close,
            "ma_5": close * random.uniform(0.97, 1.03),
            "ma_10": close * random.uniform(0.95, 1.05),
            "ma_20": close * random.uniform(0.93, 1.07),
            "std_20": close * random.uniform(0.01, 0.04),
            "macd": random.uniform(-2, 2),
            "macd_signal": random.uniform(-2, 2),
            "macd_hist": random.uniform(-1, 1),
            "macd_hist_prev": random.uniform(-1, 1),
            "bb_upper": close * 1.04,
            "bb_lower": close * 0.96,
            "bb_mid": close,
            "donchian_upper_20": close * 1.05,
            "donchian_lower_20": close * 0.95,
            "donchian_lower_10": close * 0.97,
            "z_value": random.gauss(0, 1),
            "z_momentum": random.gauss(0, 1),
            "z_quality": random.gauss(0, 1),
            "return_20d": random.uniform(-0.15, 0.20),
            "grid_base_price": close * random.uniform(0.95, 1.05),
            "atr_14": close * random.uniform(0.01, 0.03),
        }

    signals = mod.generate_signals("2025-01-15", universe, market_data, {}, config=config)
    long_signals = [s for s in signals if s.get("direction") == "long"]
    short_signals = [s for s in signals if s.get("direction") == "short"]
    return {
        "layer": "trading",
        "universe_size": len(universe),
        "total_signals": len(signals),
        "long_count": len(long_signals),
        "short_count": len(short_signals),
        "avg_strength": round(sum(s.get("strength", 0) for s in signals) / max(len(signals), 1), 4),
        "sample_signals": signals[:5],
    }


def _backtest_risk(mod: Any, config: Dict[str, Any]) -> Dict[str, Any]:
    """Run risk filter_and_size() against synthetic signals."""
    import random

    random.seed(42)
    signals = [
        {
            "symbol": f"SYM{i:04d}",
            "direction": random.choice(["long", "short"]),
            "strength": random.uniform(0.3, 1.0),
            "reason": "synthetic signal",
        }
        for i in range(20)
    ]
    cash = 1_000_000.0
    positions: Dict[str, Any] = {}
    prices = {f"SYM{i:04d}": random.uniform(10, 200) for i in range(20)}

    orders = mod.filter_and_size(signals, cash, positions, prices, config=config)
    total_alloc = sum(o.get("volume", 0) * o.get("price", 0) for o in orders)
    return {
        "layer": "risk",
        "input_signals": len(signals),
        "output_orders": len(orders),
        "total_allocated": round(total_alloc, 2),
        "utilization_pct": round(total_alloc / cash * 100, 2) if cash > 0 else 0,
        "sample_orders": orders[:5],
    }
