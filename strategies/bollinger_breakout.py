"""Bollinger Breakout — standalone VNPy CTA strategy.

Enters when price closes outside Bollinger Bands with expanding
bandwidth and exits on mean reversion back to the middle band.
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


class BollingerBreakoutStrategy(CtaTemplate):
    """Bollinger Band breakout with bandwidth confirmation."""

    author = "QuantMate"

    # parameters
    bb_period: int = 20
    bb_std: float = 2.0
    bandwidth_threshold: float = 0.04
    fixed_size: int = 1

    parameters = ["bb_period", "bb_std", "bandwidth_threshold", "fixed_size"]
    variables = ["bb_upper", "bb_lower", "bb_mid", "bandwidth"]

    def __init__(self, cta_engine, strategy_name, vt_symbol, setting):
        super().__init__(cta_engine, strategy_name, vt_symbol, setting)

        self.bg = BarGenerator(self.on_bar)
        self.am = ArrayManager()

        self.bb_upper = 0.0
        self.bb_lower = 0.0
        self.bb_mid = 0.0
        self.bandwidth = 0.0

    def on_init(self):
        self.write_log("BollingerBreakoutStrategy initializing")
        self.load_bar(self.bb_period + 20)

    def on_start(self):
        self.write_log("BollingerBreakoutStrategy started")

    def on_stop(self):
        self.write_log("BollingerBreakoutStrategy stopped")

    def on_tick(self, tick: TickData):
        self.bg.update_tick(tick)

    def on_bar(self, bar: BarData):
        self.cancel_all()

        self.am.update_bar(bar)
        if not self.am.inited:
            return

        self.bb_upper, self.bb_mid, self.bb_lower = self.am.boll(
            self.bb_period, self.bb_std
        )
        if self.bb_mid > 0:
            self.bandwidth = (self.bb_upper - self.bb_lower) / self.bb_mid
        else:
            self.bandwidth = 0

        close = bar.close_price
        wide_enough = self.bandwidth >= self.bandwidth_threshold

        if self.pos == 0:
            if close > self.bb_upper and wide_enough:
                self.buy(close * 1.01, self.fixed_size)
                self.write_log(
                    f"Long breakout: close={close:.2f} > upper={self.bb_upper:.2f}"
                )
            elif close < self.bb_lower and wide_enough:
                self.short(close * 0.99, self.fixed_size)
                self.write_log(
                    f"Short breakout: close={close:.2f} < lower={self.bb_lower:.2f}"
                )
        elif self.pos > 0:
            if close <= self.bb_mid:
                self.sell(close * 0.99, abs(self.pos))
                self.write_log("Exit long — reverted to mid band")
        elif self.pos < 0:
            if close >= self.bb_mid:
                self.cover(close * 1.01, abs(self.pos))
                self.write_log("Exit short — reverted to mid band")

        self.put_event()

    def on_order(self, order: OrderData):
        pass

    def on_trade(self, trade: TradeData):
        self.put_event()

    def on_stop_order(self, stop_order: StopOrder):
        pass
