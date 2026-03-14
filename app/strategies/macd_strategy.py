"""MACD-based CTA strategy for testing.

Simple MACD strategy:
- Uses ArrayManager.macd to compute MACD, signal and hist
- Long entry: macd > signal and macd_hist > 0
- Long exit: macd < signal
- Short entry: macd < signal and macd_hist < 0
- Short exit: macd > signal

This is intended for local testing/backtests only.
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
from vnpy.trader.constant import Direction, Offset


class MACDStrategy(CtaTemplate):
    """A small MACD crossover strategy for testing/backtests."""

    author = "QuantMate"

    # strategy parameters
    fast_period: int = 12
    slow_period: int = 26
    signal_period: int = 9
    fixed_size: int = 1

    parameters = ["fast_period", "slow_period", "signal_period", "fixed_size"]
    variables = ["macd", "macd_signal", "macd_hist"]

    def __init__(self, cta_engine, strategy_name, vt_symbol, setting):
        super().__init__(cta_engine, strategy_name, vt_symbol, setting)

        self.bg = BarGenerator(self.on_bar)
        self.am = ArrayManager()

        # state
        self.macd = 0.0
        self.macd_signal = 0.0
        self.macd_hist = 0.0

    def on_init(self):
        self.write_log("MACDStrategy initializing")
        self.load_bar(50)

    def on_start(self):
        self.write_log("MACDStrategy started")

    def on_stop(self):
        self.write_log("MACDStrategy stopped")

    def on_tick(self, tick: TickData):
        self.bg.update_tick(tick)

    def on_bar(self, bar: BarData):
        # cancel outstanding orders first
        self.cancel_all()

        # update array manager
        self.am.update_bar(bar)
        if not self.am.inited:
            return

        # compute MACD via ArrayManager utility (returns macd, signal, hist)
        macd_v, signal_v, hist_v = self.am.macd(self.fast_period, self.slow_period, self.signal_period)
        self.macd = macd_v
        self.macd_signal = signal_v
        self.macd_hist = hist_v

        # Trading logic (long-only; use available capital to size positions)
        # No position -> consider long entry only
        if self.pos == 0:
            if self.macd > self.macd_signal and self.macd_hist > 0:
                # Calculate number of contracts to buy using full capital
                try:
                    size_per_contract = int(self.get_size() or 1)
                except Exception:
                    size_per_contract = 1

                try:
                    engine_capital = float(getattr(self.cta_engine, "capital", 0) or 0)
                except Exception:
                    engine_capital = 0.0

                # Guard against zero price or capital
                price = float(bar.close_price or 0.0)
                volume = 1
                if price > 0 and size_per_contract > 0 and engine_capital > 0:
                    # number of contracts = floor(capital / (price * size_per_contract))
                    volume = max(1, int(engine_capital / (price * size_per_contract)))

                # Place a marketable buy using a small price adjustment
                self.buy(price * 1.01, volume)
                self.write_log(f"Long entry signal: buying {volume} @ {price:.4f}")

        # Have long -> exit when macd crosses below signal
        elif self.pos > 0:
            if self.macd < self.macd_signal:
                self.sell(bar.close_price * 0.99, abs(self.pos))
                self.write_log("Exit long (macd < signal)")

        self.put_event()

    def on_order(self, order: OrderData):
        pass

    def on_trade(self, trade: TradeData):
        # Log trade with clearer intent (entry vs exit, long vs short)
        try:
            if trade.direction == Direction.LONG and getattr(trade, 'offset', None) == Offset.OPEN:
                kind = 'Long entry'
            elif trade.direction == Direction.SHORT and getattr(trade, 'offset', None) == Offset.CLOSE:
                kind = 'Exit long'
            elif trade.direction == Direction.SHORT and getattr(trade, 'offset', None) == Offset.OPEN:
                kind = 'Short entry'
            elif trade.direction == Direction.LONG and getattr(trade, 'offset', None) == Offset.CLOSE:
                kind = 'Exit short'
            else:
                kind = f'{trade.direction} {getattr(trade, "offset", "")}'
        except Exception:
            kind = str(trade.direction)

        self.write_log(f"Trade executed: {kind} {trade.volume} @ {trade.price}")

    def on_stop_order(self, stop_order: StopOrder):
        pass
