from app.domains.composite.orchestrator import CompositeStrategyOrchestrator


def test_factor_expression_components_rank_and_rebalance_positions():
    orchestrator = CompositeStrategyOrchestrator(
        universe_components=[
            {
                "layer": "universe",
                "name": "volume_rank",
                "config": {
                    "factor_expression": "$volume / mean(volume, 3)",
                    "top_n": 1,
                },
            }
        ],
        trading_components=[
            {
                "layer": "trading",
                "name": "factor_trade",
                "config": {
                    "factor_expression": "$volume / mean(volume, 3)",
                    "top_n": 1,
                    "close_on_universe_exit": True,
                },
            }
        ],
        risk_components=[],
    )

    history_data = {
        "AAA": [
            {"datetime": "2025-01-01", "open": 10, "high": 10, "low": 10, "close": 10, "volume": 10},
            {"datetime": "2025-01-02", "open": 10, "high": 10, "low": 10, "close": 10, "volume": 10},
            {"datetime": "2025-01-03", "open": 10, "high": 10, "low": 10, "close": 10, "volume": 30},
        ],
        "BBB": [
            {"datetime": "2025-01-01", "open": 10, "high": 10, "low": 10, "close": 10, "volume": 10},
            {"datetime": "2025-01-02", "open": 10, "high": 10, "low": 10, "close": 10, "volume": 10},
            {"datetime": "2025-01-03", "open": 10, "high": 10, "low": 10, "close": 10, "volume": 10},
        ],
    }
    market_data = {
        "AAA": {"open": 10, "high": 10, "low": 10, "close": 10, "volume": 30},
        "BBB": {"open": 10, "high": 10, "low": 10, "close": 10, "volume": 10},
    }

    orders = orchestrator.run_day(
        trading_day="2025-01-03",
        all_symbols=["AAA", "BBB"],
        market_data=market_data,
        prices={"AAA": 10, "BBB": 10},
        cash=10_000,
        positions={"BBB": {"quantity": 100, "avg_cost": 10, "held_days": 3}},
        history_data=history_data,
    )

    assert {(order.symbol, order.direction) for order in orders} == {
        ("AAA", "buy"),
        ("BBB", "sell"),
    }