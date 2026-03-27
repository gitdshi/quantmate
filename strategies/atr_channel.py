"""ATR Channel — standalone VNPy CTA strategy.

Uses ATR‑based channels around a moving average for trend‑following
entries and volatility‑scaled exits.
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


class ATRChannelStrategy(CtaTemplate):
    """ATR channel breakout / reversion strategy."""

    author = "QuantMate"

    # parameters
    ma_period: int = 20
    atr_period: int = 14
    atr_multiplier: float = 2.0
    fixed_size: int = 1

    parameters = ["ma_period", "atr_period", "atr_multiplier", "fixed_size"]
    variables = ["ma_value", "atr_value", "upper_band", "lower_band"]

    def __init__(self, cta_engine, strategy_name, vt_symbol, setting):
        super().__init__(cta_engine, strategy_name, vt_symbol, setting)

        self.bg = BarGenerator(self.on_bar)
        self.am = ArrayManager()

        self.ma_value = 0.0
        self.atr_value = 0.0
        self.upper_band = 0.0
        self.lower_band = 0.0

    def on_init(self):
        self.write_log("ATRChannelStrategy initializing")
        self.load_bar(max(self.ma_period, self.atr_period) + 20)

    def on_start(self):
        self.write_log("ATRChannelStrategy started")

    def on_stop(self):
        self.write_log("ATRChannelStrategy stopped")

    def on_tick(self, tick: TickData):
        self.bg.update_tick(tick)

    def on_bar(self, bar: BarData):
        self.cancel_all()

        self.am.update_bar(bar)
        if not self.am.inited:
            return

        self.ma_value = self.am.sma(self.ma_period)
        self.atr_value = self.am.atr(self.atr_period)
        self.upper_band = self.ma_value + self.atr_multiplier * self.atr_value
        self.lower_band = self.ma_value - self.atr_multiplier * self.atr_value

        close = bar.close_price

        if self.pos == 0:
            if close > self.upper_band:
                self.buy(close * 1.01, self.fixed_size)
                self.write_log(
                    f"Long: close={close:.2f} above ATR upper={self.upper_band:.2f}"
                )
            elif close < self.lower_band:
                self.short(close * 0.99, self.fixed_size)
                self.write_log(
                    f"Short: close={close:.2f} below ATR lower={self.lower_band:.2f}"
                )
        elif self.pos > 0:
            # exit when price falls back below MA
            if close < self.ma_value:
                self.sell(close * 0.99, abs(self.pos))
                self.write_log("Exit long — price below MA")
        elif self.pos < 0:
            if close > self.ma_value:
                self.cover(close * 1.01, abs(self.pos))
                self.write_log("Exit short — price above MA")

        self.put_event()

    def on_order(self, order: OrderData):
        pass

    def on_trade(self, trade: TradeData):
        self.put_event()

    def on_stop_order(self, stop_order: StopOrder):
        pass
