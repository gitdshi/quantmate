from vnpy_ctastrategy import (
    CtaTemplate,
    StopOrder,
    Direction,
    TickData,
    BarData,
    TradeData,
    OrderData,
    BarGenerator,
    ArrayManager,
)


class TurtleTradingStrategy(CtaTemplate):
    """Turtle Trading strategy implemented for vn.py CTA framework.

    Features:
    - 20-day Donchian breakout entry (configurable)
    - 10-day Donchian exit (configurable)
    - ATR-based volatility measure for stops and pyramiding
    - Up to 4 units pyramiding
    """

    author = "TraderMate"

    entry_window: int = 20
    exit_window: int = 10
    atr_window: int = 20
    fixed_size: int = 1

    entry_up: float = 0
    entry_down: float = 0
    exit_up: float = 0
    exit_down: float = 0
    atr_value: float = 0
    long_entry: float = 0
    short_entry: float = 0
    long_stop: float = 0
    short_stop: float = 0

    parameters = ["entry_window", "exit_window", "atr_window", "fixed_size"]
    variables = ["entry_up", "entry_down", "exit_up", "exit_down", "atr_value"]

    def on_init(self) -> None:
        """Initialize strategy: set up bar generator and array manager."""
        self.write_log("TurtleTradingStrategy initialized")

        self.bg: BarGenerator = BarGenerator(self.on_bar)
        self.am: ArrayManager = ArrayManager()

        # load historical bars for indicators
        self.load_bar(max(self.entry_window, self.atr_window) + 5)

    def on_start(self) -> None:
        self.write_log("TurtleTradingStrategy started")

    def on_stop(self) -> None:
        self.write_log("TurtleTradingStrategy stopped")

    def on_tick(self, tick: TickData) -> None:
        self.bg.update_tick(tick)

    def on_bar(self, bar: BarData) -> None:
        # Called when a new bar is ready
        self.cancel_all()

        self.am.update_bar(bar)
        if not self.am.inited:
            return

        # Only calculate entry channel when no position
        if not self.pos:
            self.entry_up, self.entry_down = self.am.donchian(self.entry_window)

        self.exit_up, self.exit_down = self.am.donchian(self.exit_window)

        if not self.pos:
            self.atr_value = self.am.atr(self.atr_window)

            # reset trackers
            self.long_entry = 0
            self.short_entry = 0
            self.long_stop = 0
            self.short_stop = 0

            # send entry orders at breakout levels (marketable stop orders)
            self.send_buy_orders(self.entry_up)
            self.send_short_orders(self.entry_down)

        elif self.pos > 0:
            # if long, maintain pyramiding and set protective exit
            self.send_buy_orders(self.entry_up)

            sell_price: float = max(self.long_stop, self.exit_down)
            # use stop_order True to indicate stop style
            self.sell(sell_price, abs(self.pos), True)

        elif self.pos < 0:
            self.send_short_orders(self.entry_down)

            cover_price: float = min(self.short_stop, self.exit_up)
            self.cover(cover_price, abs(self.pos), True)

        self.put_event()

    def on_trade(self, trade: TradeData) -> None:
        # Update stops and last entry price on fills
        if trade.direction == Direction.LONG:
            self.long_entry = trade.price
            self.long_stop = self.long_entry - 2 * self.atr_value
        else:
            self.short_entry = trade.price
            self.short_stop = self.short_entry + 2 * self.atr_value

    def on_order(self, order: OrderData) -> None:
        pass

    def on_stop_order(self, stop_order: StopOrder) -> None:
        pass

    def send_buy_orders(self, price: float) -> None:
        """Place up to 4 pyramiding buy orders using ATR offsets."""
        t: float = self.pos / self.fixed_size

        if t < 1:
            self.buy(price, self.fixed_size, True)

        if t < 2:
            self.buy(price + self.atr_value * 0.5, self.fixed_size, True)

        if t < 3:
            self.buy(price + self.atr_value, self.fixed_size, True)

        if t < 4:
            self.buy(price + self.atr_value * 1.5, self.fixed_size, True)

    def send_short_orders(self, price: float) -> None:
        """Place up to 4 pyramiding short orders using ATR offsets."""
        t: float = self.pos / self.fixed_size

        if t > -1:
            self.short(price, self.fixed_size, True)

        if t > -2:
            self.short(price - self.atr_value * 0.5, self.fixed_size, True)

        if t > -3:
            self.short(price - self.atr_value, self.fixed_size, True)

        if t > -4:
            self.short(price - self.atr_value * 1.5, self.fixed_size, True)
