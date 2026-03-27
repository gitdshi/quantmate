"""Composite strategy orchestrator.

Runs the Universe → Trading → Risk pipeline for a single trading day,
producing a list of orders from the composite strategy's bound components.
"""

import json
import logging
from typing import Any, Dict, List, Optional

from app.domains.composite.market_constraints import Order

logger = logging.getLogger(__name__)


class ComponentRunner:
    """Evaluates a single strategy component.

    Components can use ``config`` (declarative rules) or ``code`` (executable Python).
    This runner supports config-based evaluation; code-based execution is reserved
    for future expansion.
    """

    def __init__(self, component: Dict[str, Any], config_override: Optional[Dict] = None):
        self.component = component
        self.layer = component["layer"]
        self.sub_type = component.get("sub_type", "")
        self.name = component.get("name", "unknown")
        raw_config = component.get("config")
        if isinstance(raw_config, str):
            try:
                raw_config = json.loads(raw_config)
            except (json.JSONDecodeError, TypeError):
                raw_config = {}
        self.config: Dict[str, Any] = {**(raw_config or {}), **(config_override or {})}
        raw_params = component.get("parameters")
        if isinstance(raw_params, str):
            try:
                raw_params = json.loads(raw_params)
            except (json.JSONDecodeError, TypeError):
                raw_params = {}
        self.parameters: Dict[str, Any] = raw_params or {}


class UniverseRunner(ComponentRunner):
    """Selects tradable symbols from the full market."""

    def select(
        self,
        trading_day: str,
        all_symbols: List[str],
        market_data: Dict[str, Dict[str, float]],
    ) -> List[str]:
        """Return a filtered list of symbols.

        Config-based filtering supports:
        - ``symbols``: explicit symbol list
        - ``top_n``: limit number of symbols (take first N from all_symbols)
        - ``min_volume``: minimum trading volume filter
        """
        symbols = self.config.get("symbols")
        if symbols and isinstance(symbols, list):
            return [s for s in symbols if s in all_symbols]

        result = list(all_symbols)

        min_volume = self.config.get("min_volume")
        if min_volume:
            result = [
                s for s in result
                if market_data.get(s, {}).get("volume", 0) >= min_volume
            ]

        top_n = self.config.get("top_n")
        if top_n and isinstance(top_n, int):
            result = result[:top_n]

        return result


class TradingRunner(ComponentRunner):
    """Generates trading signals for the selected universe."""

    def generate_signals(
        self,
        trading_day: str,
        universe: List[str],
        market_data: Dict[str, Dict[str, float]],
        positions: Dict[str, Any],
    ) -> List[Dict[str, Any]]:
        """Return signal dicts: {symbol, direction, strength, reason}.

        Config-based signal generation supports:
        - ``hold_days``: sell after N days
        - ``buy_all``: buy all universe symbols not in position
        """
        signals: List[Dict[str, Any]] = []

        buy_all = self.config.get("buy_all", True)
        hold_days = self.config.get("hold_days")

        if buy_all:
            for sym in universe:
                if sym not in positions:
                    signals.append(
                        {
                            "symbol": sym,
                            "direction": "buy",
                            "strength": 1.0,
                            "reason": f"universe_select({self.name})",
                        }
                    )

        if hold_days and isinstance(hold_days, int):
            for sym, pos_info in positions.items():
                if sym in universe:
                    held = pos_info.get("held_days", 0)
                    if held >= hold_days:
                        signals.append(
                            {
                                "symbol": sym,
                                "direction": "sell",
                                "strength": 1.0,
                                "reason": f"hold_days_exit({self.name}, {hold_days}d)",
                            }
                        )

        return signals


class RiskRunner(ComponentRunner):
    """Filters signals and produces sized orders."""

    def filter_and_size(
        self,
        signals: List[Dict[str, Any]],
        cash: float,
        positions: Dict[str, Any],
        prices: Dict[str, float],
    ) -> List[Order]:
        """Convert signals to sized orders after risk checks.

        Config-based risk rules:
        - ``max_position_pct``: max % of equity per position (default 0.1)
        - ``max_total_positions``: max number of concurrent positions
        - ``stop_loss_pct``: stop loss percentage
        """
        max_pos_pct = self.config.get("max_position_pct", 0.1)
        max_total = self.config.get("max_total_positions", 20)
        stop_loss_pct = self.config.get("stop_loss_pct")

        # Current portfolio value estimate
        portfolio_value = cash
        for sym, pos_info in positions.items():
            px = prices.get(sym, pos_info.get("avg_cost", 0))
            portfolio_value += px * pos_info.get("quantity", 0)

        orders: List[Order] = []
        current_count = len(positions)

        # Stop-loss sell signals
        if stop_loss_pct:
            for sym, pos_info in positions.items():
                avg_cost = pos_info.get("avg_cost", 0)
                px = prices.get(sym, avg_cost)
                if avg_cost > 0 and px < avg_cost * (1 - stop_loss_pct):
                    orders.append(
                        Order(
                            symbol=sym,
                            direction="sell",
                            quantity=pos_info.get("quantity", 0),
                            price=px,
                            reason=f"stop_loss({self.name}, {stop_loss_pct:.0%})",
                        )
                    )

        for sig in signals:
            sym = sig["symbol"]
            direction = sig["direction"]
            px = prices.get(sym, 0)
            if px <= 0:
                continue

            if direction == "buy":
                if current_count >= max_total:
                    continue  # max positions reached
                # Size: allocate max_pos_pct of portfolio
                alloc = portfolio_value * max_pos_pct
                quantity = int(alloc / px) if px > 0 else 0
                if quantity > 0:
                    orders.append(
                        Order(
                            symbol=sym,
                            direction="buy",
                            quantity=quantity,
                            price=px,
                            reason=sig.get("reason", ""),
                        )
                    )
                    current_count += 1

            elif direction == "sell":
                pos_info = positions.get(sym)
                if pos_info and pos_info.get("quantity", 0) > 0:
                    orders.append(
                        Order(
                            symbol=sym,
                            direction="sell",
                            quantity=pos_info["quantity"],
                            price=px,
                            reason=sig.get("reason", ""),
                        )
                    )

        return orders


class CompositeStrategyOrchestrator:
    """Orchestrates the Universe → Trading → Risk pipeline.

    Takes bound components (resolved from bindings) and runs them in sequence
    for a single trading day.
    """

    def __init__(
        self,
        universe_components: List[Dict[str, Any]],
        trading_components: List[Dict[str, Any]],
        risk_components: List[Dict[str, Any]],
    ):
        self.universe_runners = [
            UniverseRunner(c, c.get("config_override"))
            for c in universe_components
        ]
        self.trading_runners = [
            TradingRunner(c, c.get("config_override"))
            for c in trading_components
        ]
        self.risk_runners = [
            RiskRunner(c, c.get("config_override"))
            for c in risk_components
        ]

    def run_day(
        self,
        trading_day: str,
        all_symbols: List[str],
        market_data: Dict[str, Dict[str, float]],
        prices: Dict[str, float],
        cash: float,
        positions: Dict[str, Any],
    ) -> List[Order]:
        """Run the full pipeline for one trading day.

        Args:
            trading_day: ISO date string.
            all_symbols: Full list of available symbols.
            market_data: symbol → {close, volume, ...} for the day.
            prices: symbol → close price for the day.
            cash: Current cash balance.
            positions: symbol → {quantity, avg_cost, held_days, ...}.

        Returns:
            List of sized and risk-filtered orders.
        """
        # 1. Universe selection: merge results from all universe components
        universe: List[str] = []
        seen: set = set()
        for runner in self.universe_runners:
            selected = runner.select(trading_day, all_symbols, market_data)
            for s in selected:
                if s not in seen:
                    universe.append(s)
                    seen.add(s)

        if not universe and not positions:
            return []

        # 2. Trading signal generation: merge signals with weight-based voting
        all_signals: List[Dict[str, Any]] = []
        for runner in self.trading_runners:
            signals = runner.generate_signals(
                trading_day, universe, market_data, positions
            )
            weight = runner.config.get("weight", 1.0)
            for sig in signals:
                sig["strength"] = sig.get("strength", 1.0) * weight
            all_signals.extend(signals)

        # Deduplicate: if multiple signals for same symbol+direction, pick strongest
        signal_map: Dict[str, Dict[str, Any]] = {}
        for sig in all_signals:
            key = f"{sig['symbol']}_{sig['direction']}"
            existing = signal_map.get(key)
            if existing is None or sig["strength"] > existing["strength"]:
                signal_map[key] = sig
        merged_signals = list(signal_map.values())

        # 3. Risk filtering: chain through risk components sequentially
        orders: List[Order] = []
        if self.risk_runners:
            for runner in self.risk_runners:
                orders = runner.filter_and_size(
                    merged_signals, cash, positions, prices
                )
                # Convert remaining orders back to signals for next risk runner
                merged_signals = [
                    {
                        "symbol": o.symbol,
                        "direction": o.direction,
                        "strength": 1.0,
                        "reason": o.reason,
                    }
                    for o in orders
                ]
        else:
            # No risk components — do basic sizing with defaults
            default_risk = RiskRunner(
                {"layer": "risk", "sub_type": "default", "name": "default_risk"},
            )
            orders = default_risk.filter_and_size(
                merged_signals, cash, positions, prices
            )

        return orders
