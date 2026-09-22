from app.domains.composite.orchestrator import CompositeStrategyOrchestrator, RiskRunner


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


# ── Buy sizing must respect available cash (staging deadlock fix) ────────


def _risk_runner() -> RiskRunner:
    return RiskRunner(
        {"layer": "risk", "sub_type": "default", "name": "default_risk"},
    )


def test_buy_sizing_caps_allocation_to_available_cash():
    runner = _risk_runner()
    # portfolio_value = cash 4977 + 60000.SH position worth ~968k
    orders = runner.filter_and_size(
        signals=[{"symbol": "600018.SH", "direction": "buy", "strength": 1.0, "reason": "universe_select(u)"}],
        cash=4_977.0,
        positions={"600000.SH": {"quantity": 106_000, "avg_cost": 9.43}},
        prices={"600018.SH": 10.32, "600000.SH": 9.43},
    )
    # Before the fix the alloc was portfolio_value * 0.1 ≈ 96.8k and the
    # order was dropped downstream on insufficient funds (deadlock).
    assert len(orders) == 1
    assert orders[0].quantity == int(4_977.0 / 10.32)  # 482 shares, cash-affordable


def test_buy_sizing_skips_orders_when_cash_exhausted():
    runner = _risk_runner()
    orders = runner.filter_and_size(
        signals=[
            {"symbol": "AAA", "direction": "buy", "strength": 1.0, "reason": "r"},
            {"symbol": "BBB", "direction": "buy", "strength": 1.0, "reason": "r"},
        ],
        cash=0.0,
        positions={},
        prices={"AAA": 10.0, "BBB": 20.0},
    )
    assert orders == []


def test_buy_sizing_shares_cash_across_multiple_buys():
    runner = _risk_runner()
    signals = [
        {"symbol": "AAA", "direction": "buy", "strength": 1.0, "reason": "r"},
        {"symbol": "BBB", "direction": "buy", "strength": 1.0, "reason": "r"},
    ]
    # Tight cash (5000): each buy is capped at portfolio_value * 0.1 = 500
    # (cash is not the binding constraint here), so both orders fit.
    orders = runner.filter_and_size(
        signals,
        cash=5_000.0,
        positions={},
        prices={"AAA": 10.0, "BBB": 40.0},
    )
    assert [(o.symbol, o.quantity) for o in orders] == [("AAA", 50), ("BBB", 12)]
    # Same signals with plenty of cash: both sized by max_position_pct.
    orders = runner.filter_and_size(
        signals,
        cash=1_000_000.0,
        positions={},
        prices={"AAA": 10.0, "BBB": 40.0},
    )
    assert [(o.symbol, o.quantity) for o in orders] == [("AAA", 10_000), ("BBB", 2_500)]


def test_buy_sizing_ignores_cash_when_alloc_smaller():
    runner = _risk_runner()
    orders = runner.filter_and_size(
        signals=[{"symbol": "AAA", "direction": "buy", "strength": 1.0, "reason": "r"}],
        cash=50_000.0,
        positions={},
        prices={"AAA": 10.0},
    )
    # portfolio_value (50k) * 0.1 = 5k < cash, so the standard sizing applies.
    assert orders[0].quantity == 500