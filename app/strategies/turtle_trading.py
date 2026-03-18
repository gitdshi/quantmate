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

# 导入通用止损模块
from app.strategies.stop_loss import StopLossManager


class TurtleTradingStrategy(CtaTemplate):
    """Turtle Trading strategy implemented for vn.py CTA framework.

    Features:
    - 20-day Donchian breakout entry (configurable)
    - 10-day Donchian exit (configurable)
    - ATR-based volatility measure for stops and pyramiding
    - Up to 4 units pyramiding
    - 集成固定止损和移动止损策略
    """

    author = "QuantMate"

    entry_window: int = 20
    exit_window: int = 10
    atr_window: int = 20
    fixed_size: int = 1

    # 止损参数
    stop_loss_window: int = 10  # 计算标准差的回看周期
    fixed_stop_multiplier: float = 2.0  # 固定止损：2倍标准差
    trailing_stop_multiplier: float = 1.0  # 移动止损：1倍标准差
    use_std_stop_loss: bool = True  # 是否使用基于标准差的止损

    entry_up: float = 0
    entry_down: float = 0
    exit_up: float = 0
    exit_down: float = 0
    atr_value: float = 0
    long_entry: float = 0
    long_stop: float = 0

    # 止损状态变量
    std_fixed_stop: float = 0  # 基于标准差的固定止损价
    std_trailing_stop: float = 0  # 基于标准差的移动止损价

    parameters = [
        "entry_window",
        "exit_window",
        "atr_window",
        "fixed_size",
        "stop_loss_window",
        "fixed_stop_multiplier",
        "trailing_stop_multiplier",
        "use_std_stop_loss",
    ]
    variables = ["entry_up", "entry_down", "exit_up", "exit_down", "atr_value", "std_fixed_stop", "std_trailing_stop"]

    def on_init(self) -> None:
        """Initialize strategy: set up bar generator and array manager."""
        self.write_log("TurtleTradingStrategy initialized")

        self.bg: BarGenerator = BarGenerator(self.on_bar)
        self.am: ArrayManager = ArrayManager()

        # 初始化止损管理器
        self.stop_loss_manager = StopLossManager(
            fixed_std_multiplier=self.fixed_stop_multiplier,
            trailing_std_multiplier=self.trailing_stop_multiplier,
            lookback_period=self.stop_loss_window,
            use_fixed_stop=self.use_std_stop_loss,
            use_trailing_stop=self.use_std_stop_loss,
        )

        # load historical bars for indicators
        self.load_bar(max(self.entry_window, self.atr_window, self.stop_loss_window) + 5)

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

        # 获取最近N天收盘价用于计算标准差止损
        recent_closes = list(self.am.close[-self.stop_loss_window :])
        vt_symbol = f"{bar.symbol}.{bar.exchange.value}"

        if not self.pos:
            self.atr_value = self.am.atr(self.atr_window)

            # reset trackers
            self.long_entry = 0
            self.long_stop = 0
            self.std_fixed_stop = 0
            self.std_trailing_stop = 0

            # 清除止损状态
            self.stop_loss_manager.remove_position(vt_symbol)

            # send entry orders at breakout levels (marketable stop orders)
            self.send_buy_orders(self.entry_up)

        elif self.pos > 0:
            # 更新移动止损
            if self.use_std_stop_loss:
                self.stop_loss_manager.update_trailing_stop(vt_symbol, bar.close_price, recent_closes)
                state = self.stop_loss_manager.get_state(vt_symbol)
                if state:
                    self.std_fixed_stop = state.fixed_stop_price
                    self.std_trailing_stop = state.trailing_stop_price

                    # 检查是否触发止损
                    if self.stop_loss_manager.should_stop_loss(vt_symbol, bar.close_price):
                        reason = self.stop_loss_manager.get_stop_reason(vt_symbol, bar.close_price)
                        self.write_log(f"触发止损: {reason}, 止损价={state.get_active_stop_price():.2f}")
                        self.sell(bar.close_price * 0.99, abs(self.pos), False)
                        return

            # if long, maintain pyramiding and set protective exit
            self.send_buy_orders(self.entry_up)

            # 综合ATR止损和标准差止损，取较高者
            if self.use_std_stop_loss and self.std_trailing_stop > 0:
                sell_price: float = max(self.long_stop, self.exit_down, self.std_trailing_stop)
            else:
                sell_price: float = max(self.long_stop, self.exit_down)
            # use stop_order True to indicate stop style
            self.sell(sell_price, abs(self.pos), True)

        # no short positions supported

        self.put_event()

    def on_trade(self, trade: TradeData) -> None:
        # Update stops and last entry price on fills
        vt_symbol = f"{trade.symbol}.{trade.exchange.value}"

        # 获取最近收盘价用于计算止损
        recent_closes = list(self.am.close[-self.stop_loss_window :])

        if trade.direction == Direction.LONG:
            self.long_entry = trade.price
            self.long_stop = self.long_entry - 2 * self.atr_value

            # 设置基于标准差的止损
            if self.use_std_stop_loss and len(recent_closes) >= 2:
                state = self.stop_loss_manager.set_entry(vt_symbol, trade.price, recent_closes, is_long=True)
                self.std_fixed_stop = state.fixed_stop_price
                self.std_trailing_stop = state.trailing_stop_price
                self.write_log(
                    f"开多仓: 入场价={trade.price:.2f}, "
                    f"固定止损={self.std_fixed_stop:.2f}, "
                    f"移动止损={self.std_trailing_stop:.2f}"
                )
        # short fills ignored (strategy is long-only)

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
        # short orders removed for long-only strategy
        return
