
from app.domains.trading.matching_engine import (
    FillResult,
    calculate_fee,
    match_order,
    try_fill_limit_order,
    try_fill_market_order,
    try_fill_stop_order,
)


class TestMatchingEngine:
    def test_calculate_fee_cn_hk_us_and_fallback(self):
        cn_buy = calculate_fee("CN", "buy", 10, 100)
        assert cn_buy.commission == 5.0
        assert cn_buy.stamp_tax == 0.0
        assert cn_buy.transfer_fee == 0.02

        cn_sell = calculate_fee("CN", "sell", 10, 100)
        assert cn_sell.stamp_tax == 1.0

        hk = calculate_fee("HK", "buy", 10, 100)
        assert hk.stamp_tax == 1.3
        assert hk.other_fee > 0

        us_sell = calculate_fee("US", "sell", 10, 100)
        assert us_sell.commission == 1.0
        assert us_sell.other_fee > 0

        fallback = calculate_fee("OTHER", "buy", 10, 100)
        assert fallback.commission == 0.3

    def test_fill_result_total_cost_uses_fee_total(self):
        fee = calculate_fee("CN", "buy", 10, 100)
        result = FillResult(filled=True, fill_price=10, fill_quantity=100, fee=fee)
        assert result.total_cost == 1000 + fee.total

    def test_try_fill_market_order_handles_missing_price_and_direction(self):
        missing = try_fill_market_order(direction="buy", quantity=100, market="CN", last_price=0)
        assert missing.filled is False

        buy = try_fill_market_order(direction="buy", quantity=100, market="CN", last_price=10, slippage=0.01)
        sell = try_fill_market_order(direction="sell", quantity=100, market="CN", last_price=10, slippage=0.01)
        assert buy.fill_price == 10.1
        assert sell.fill_price == 9.9

    def test_try_fill_limit_order_buy_sell_and_unmet_conditions(self):
        buy = try_fill_limit_order(direction="buy", quantity=10, limit_price=10, market="CN", last_price=9)
        sell = try_fill_limit_order(direction="sell", quantity=10, limit_price=10, market="CN", last_price=11)
        blocked = try_fill_limit_order(direction="buy", quantity=10, limit_price=10, market="CN", last_price=11)
        no_price = try_fill_limit_order(direction="buy", quantity=10, limit_price=10, market="CN", last_price=0)

        assert buy.filled is True and buy.fill_price == 10
        assert sell.filled is True and sell.fill_price == 10
        assert blocked.filled is False and "condition" in blocked.reason
        assert no_price.filled is False

    def test_try_fill_stop_order_trigger_and_non_trigger(self):
        buy = try_fill_stop_order(direction="buy", quantity=10, stop_price=10, market="CN", last_price=11, slippage=0)
        sell = try_fill_stop_order(direction="sell", quantity=10, stop_price=10, market="CN", last_price=9, slippage=0)
        blocked = try_fill_stop_order(direction="buy", quantity=10, stop_price=10, market="CN", last_price=9)
        no_price = try_fill_stop_order(direction="buy", quantity=10, stop_price=10, market="CN", last_price=0)

        assert buy.filled is True
        assert sell.filled is True
        assert blocked.filled is False and "not triggered" in blocked.reason
        assert no_price.filled is False

    def test_match_order_routes_and_handles_unknown_type(self):
        assert match_order(order_type="market", direction="buy", quantity=1, price=None, stop_price=None, market="CN", last_price=10).filled is True
        assert match_order(order_type="limit", direction="buy", quantity=1, price=10, stop_price=None, market="CN", last_price=9).filled is True
        assert match_order(order_type="stop", direction="buy", quantity=1, price=None, stop_price=10, market="CN", last_price=11).filled is True
        assert match_order(order_type="stop_limit", direction="buy", quantity=1, price=10, stop_price=None, market="CN", last_price=11).filled is True
        unknown = match_order(order_type="weird", direction="buy", quantity=1, price=None, stop_price=None, market="CN", last_price=10)
        assert unknown.filled is False
        assert "Unknown order type" in unknown.reason
