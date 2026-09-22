"""Unit tests for the composite paper executor (staging deadlock fixes)."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

from app.domains.composite.market_constraints import MarketConstraints, Order
from app.domains.trading.paper_composite_executor import (
    PaperCompositeExecutor,
    _prioritize_sells,
)


def test_prioritize_sells_orders_sells_first_stably():
    orders = [
        Order(symbol="AAA", direction="buy", quantity=100),
        Order(symbol="BBB", direction="sell", quantity=200),
        Order(symbol="CCC", direction="buy", quantity=300),
        Order(symbol="DDD", direction="sell", quantity=400),
    ]
    result = _prioritize_sells(orders)
    assert [o.symbol for o in result] == ["BBB", "DDD", "AAA", "CCC"]


def _execute_order_args():
    """Shared kwargs for _execute_order tests."""
    return dict(
        deployment_id=11,
        paper_account_id=10,
        user_id=1,
        composite_strategy_id=12,
        strategy_name="qlib-full-pipeline-test",
        constraints=MarketConstraints(),
        gateway=None,
        vt_symbol="600018.SSE",
    )


@patch("app.domains.autopilot.alerts.emit_autopilot_alert")
@patch("app.domains.trading.paper_composite_executor.PaperAccountService")
@patch("app.domains.trading.paper_composite_executor.OrderDao")
def test_insufficient_funds_records_rejected_order_once_per_day(MockDao, MockAcct, MockAlert):
    MockAcct.return_value.freeze_funds.return_value = False
    dao = MockDao.return_value
    dao.create.return_value = 123

    executor = PaperCompositeExecutor()
    order = Order(symbol="600018.SH", direction="buy", quantity=400, price=10.0)
    rejected_keys: set[str] = set()

    executor._execute_order(order=order, rejected_keys=rejected_keys, **_execute_order_args())

    dao.create.assert_called_once()
    dao.update_status.assert_called_once_with(123, "rejected", filled_quantity=0, avg_fill_price=0, fee=0)
    assert rejected_keys == {"600018.SH:buy"}
    MockAlert.assert_called_once()
    assert "insufficient_funds" in MockAlert.call_args.args[0]

    # Same day, same symbol: the rejection must not be recorded again.
    executor._execute_order(order=order, rejected_keys=rejected_keys, **_execute_order_args())
    assert dao.create.call_count == 1
    assert dao.update_status.call_count == 1
    assert MockAlert.call_count == 1

    # A different symbol is still recorded.
    other = Order(symbol="600019.SH", direction="buy", quantity=100, price=8.0)
    executor._execute_order(order=other, rejected_keys=rejected_keys, **_execute_order_args())
    assert dao.create.call_count == 2
    assert rejected_keys == {"600018.SH:buy", "600019.SH:buy"}


@patch("app.domains.autopilot.alerts.emit_autopilot_alert")
@patch("app.domains.trading.paper_composite_executor.PaperAccountService")
@patch("app.domains.trading.paper_composite_executor.OrderDao")
def test_no_price_records_rejected_order_once_per_day(MockDao, MockAcct, MockAlert):
    MockAcct.return_value.freeze_funds.return_value = True
    dao = MockDao.return_value
    dao.create.return_value = 456

    executor = PaperCompositeExecutor()
    order = Order(symbol="600018.SH", direction="buy", quantity=400, price=0.0)
    rejected_keys: set[str] = set()

    executor._execute_order(order=order, rejected_keys=rejected_keys, **_execute_order_args())

    dao.create.assert_called_once()
    dao.update_status.assert_called_once_with(456, "rejected", filled_quantity=0, avg_fill_price=0, fee=0)
    assert rejected_keys == {"600018.SH:buy"}
    assert "no_price" in MockAlert.call_args.args[0]

    executor._execute_order(order=order, rejected_keys=rejected_keys, **_execute_order_args())
    assert dao.create.call_count == 1


@patch("app.domains.trading.paper_composite_executor.PaperExecutionLedger")
@patch("app.domains.trading.paper_composite_executor.PaperAccountService")
@patch("app.domains.trading.paper_composite_executor.OrderDao")
def test_successful_buy_does_not_record_rejection(MockDao, MockAcct, MockLedger):
    MockAcct.return_value.freeze_funds.return_value = True
    dao = MockDao.return_value
    dao.create.return_value = 789

    executor = PaperCompositeExecutor()
    order = Order(symbol="600018.SH", direction="buy", quantity=400, price=10.0)
    rejected_keys: set[str] = set()

    executor._execute_order(order=order, rejected_keys=rejected_keys, **_execute_order_args())

    assert dao.update_status.call_args.args[1] == "filled"
    assert rejected_keys == set()


@patch("app.domains.trading.paper_composite_executor.PaperExecutionLedger")
@patch("app.domains.trading.paper_composite_executor.PaperAccountService")
@patch("app.domains.trading.paper_composite_executor.OrderDao")
def test_insufficient_sell_position_records_nothing(MockDao, MockAcct, MockLedger):
    """Sell without enough position is skipped silently (no cash impact)."""
    dao = MockDao.return_value
    ledger = MockLedger.return_value
    ledger.get_position_quantity.return_value = 100  # less than order qty 400

    executor = PaperCompositeExecutor()
    order = Order(symbol="600018.SH", direction="sell", quantity=400, price=10.0)
    rejected_keys: set[str] = set()

    executor._execute_order(order=order, rejected_keys=rejected_keys, **_execute_order_args())

    dao.create.assert_not_called()
    assert rejected_keys == set()
