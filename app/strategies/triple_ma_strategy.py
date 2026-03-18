"""
三均线策略 (Triple Moving Average Strategy)
集成固定止损和移动止损

策略逻辑：
1. 使用三条不同周期的移动平均线（快线、中线、慢线）
2. 开多条件：快线 > 中线 > 慢线（多头排列）
3. 开空条件：快线 < 中线 < 慢线（空头排列）
4. 平多条件：快线下穿中线 或 触发止损
5. 平空条件：快线上穿中线 或 触发止损
6. 集成基于标准差的固定止损和移动止损

Author: QuantMate
"""

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

from app.strategies.stop_loss import StopLossManager


class TripleMAStrategy(CtaTemplate):
    """
    三均线策略

    参数说明：
    - fast_window: 快线周期（默认5）
    - mid_window: 中线周期（默认10）
    - slow_window: 慢线周期（默认20）
    - fixed_size: 固定交易手数
    - stop_loss_window: 计算标准差的回看周期
    - fixed_stop_multiplier: 固定止损标准差倍数
    - trailing_stop_multiplier: 移动止损标准差倍数
    """

    author = "QuantMate"

    # 策略参数
    fast_window: int = 5  # 快线周期
    mid_window: int = 10  # 中线周期
    slow_window: int = 20  # 慢线周期
    fixed_size: int = 1  # 固定交易手数

    # 止损参数
    stop_loss_window: int = 10  # 计算标准差的回看周期
    fixed_stop_multiplier: float = 1.0  # 固定止损：1倍标准差
    trailing_stop_multiplier: float = 2.0  # 移动止损：2倍标准差
    use_stop_loss: bool = True  # 是否启用止损

    # 策略变量
    fast_ma: float = 0  # 快线值
    mid_ma: float = 0  # 中线值
    slow_ma: float = 0  # 慢线值

    ma_trend: int = 0  # 均线趋势：1=多头排列，-1=空头排列，0=无趋势

    # 止损状态变量
    entry_price: float = 0  # 入场价格
    fixed_stop: float = 0  # 固定止损价
    trailing_stop: float = 0  # 移动止损价

    parameters = [
        "fast_window",
        "mid_window",
        "slow_window",
        "fixed_size",
        "stop_loss_window",
        "fixed_stop_multiplier",
        "trailing_stop_multiplier",
        "use_stop_loss",
    ]

    variables = ["fast_ma", "mid_ma", "slow_ma", "ma_trend", "entry_price", "fixed_stop", "trailing_stop"]

    def __init__(self, cta_engine, strategy_name, vt_symbol, setting):
        """初始化策略"""
        super().__init__(cta_engine, strategy_name, vt_symbol, setting)

        self.bg = BarGenerator(self.on_bar)
        self.am = ArrayManager()

        # 初始化止损管理器
        self.stop_loss_manager = StopLossManager(
            fixed_std_multiplier=self.fixed_stop_multiplier,
            trailing_std_multiplier=self.trailing_stop_multiplier,
            lookback_period=self.stop_loss_window,
            use_fixed_stop=self.use_stop_loss,
            use_trailing_stop=self.use_stop_loss,
        )

    def on_init(self):
        """策略初始化"""
        self.write_log("三均线策略初始化")

        # 加载历史数据
        self.load_bar(max(self.slow_window, self.stop_loss_window) + 10)

    def on_start(self):
        """策略启动"""
        self.write_log("三均线策略启动")

    def on_stop(self):
        """策略停止"""
        self.write_log("三均线策略停止")

    def on_tick(self, tick: TickData):
        """Tick数据更新"""
        self.bg.update_tick(tick)

    def on_bar(self, bar: BarData):
        """K线数据更新"""
        # 取消所有挂单
        self.cancel_all()

        # 更新K线到数组管理器
        self.am.update_bar(bar)
        if not self.am.inited:
            return

        # 计算三条均线
        self.fast_ma = self.am.sma(self.fast_window)
        self.mid_ma = self.am.sma(self.mid_window)
        self.slow_ma = self.am.sma(self.slow_window)

        # 判断均线趋势（仅用于多头信号，策略为多头优先）
        if self.fast_ma > self.mid_ma > self.slow_ma:
            self.ma_trend = 1  # 多头排列
        else:
            self.ma_trend = 0  # 无明显多头趋势

        # 获取最近收盘价用于止损计算
        recent_closes = list(self.am.close[-self.stop_loss_window :])
        vt_symbol = f"{bar.symbol}.{bar.exchange.value}"

        # 如果有持仓，更新移动止损
        if self.pos != 0 and self.use_stop_loss:
            self.stop_loss_manager.update_trailing_stop(vt_symbol, bar.close_price, recent_closes)
            state = self.stop_loss_manager.get_state(vt_symbol)

            if state:
                self.fixed_stop = state.fixed_stop_price
                self.trailing_stop = state.trailing_stop_price

                # 检查是否触发止损
                if self.stop_loss_manager.should_stop_loss(vt_symbol, bar.close_price):
                    reason = self.stop_loss_manager.get_stop_reason(vt_symbol, bar.close_price)
                    active_stop = state.get_active_stop_price()

                    if self.pos > 0:
                        self.write_log(
                            f"多头止损触发 ({reason}): 当前价={bar.close_price:.2f}, 止损价={active_stop:.2f}"
                        )
                        self.sell(bar.close_price * 0.99, abs(self.pos))
                    elif self.pos < 0:
                        self.write_log(
                            f"空头止损触发 ({reason}): 当前价={bar.close_price:.2f}, 止损价={active_stop:.2f}"
                        )
                        self.cover(bar.close_price * 1.01, abs(self.pos))

                    return

        # 无持仓时的开仓逻辑
        if self.pos == 0:
            # 清除止损状态
            self.stop_loss_manager.remove_position(vt_symbol)
            self.entry_price = 0
            self.fixed_stop = 0
            self.trailing_stop = 0

            # 多头排列，开多（策略为多头方向，仅建多仓）
            if self.ma_trend == 1:
                self.buy(bar.close_price * 1.01, self.fixed_size)
                self.write_log(
                    f"多头开仓信号: 快线={self.fast_ma:.2f} > 中线={self.mid_ma:.2f} > 慢线={self.slow_ma:.2f}"
                )

        # 持有多头时
        elif self.pos > 0:
            # 快线下穿中线，平多
            if self.fast_ma < self.mid_ma:
                self.sell(bar.close_price * 0.99, abs(self.pos))
                self.write_log("多头平仓信号: 快线下穿中线")

        # 不支持空头仓位（策略为多头-only）

        # 更新UI
        self.put_event()

    def on_order(self, order: OrderData):
        """委托回报"""
        pass

    def on_trade(self, trade: TradeData):
        """成交回报"""
        vt_symbol = f"{trade.symbol}.{trade.exchange.value}"
        recent_closes = list(self.am.close[-self.stop_loss_window :])

        # 开仓成交时设置止损
        if self.use_stop_loss and len(recent_closes) >= 2:
            if trade.direction == Direction.LONG:
                # 开多仓
                self.entry_price = trade.price
                state = self.stop_loss_manager.set_entry(vt_symbol, trade.price, recent_closes, is_long=True)
                self.fixed_stop = state.fixed_stop_price
                self.trailing_stop = state.trailing_stop_price

                self.write_log(
                    f"开多仓成交: 价格={trade.price:.2f}, "
                    f"固定止损={self.fixed_stop:.2f}, "
                    f"移动止损={self.trailing_stop:.2f}"
                )

            elif trade.direction == Direction.SHORT:
                # 开空仓
                self.entry_price = trade.price
                state = self.stop_loss_manager.set_entry(vt_symbol, trade.price, recent_closes, is_long=False)
                self.fixed_stop = state.fixed_stop_price
                self.trailing_stop = state.trailing_stop_price

                self.write_log(
                    f"开空仓成交: 价格={trade.price:.2f}, "
                    f"固定止损={self.fixed_stop:.2f}, "
                    f"移动止损={self.trailing_stop:.2f}"
                )

        # 平仓成交时清除止损
        if (trade.direction == Direction.LONG and trade.offset.value != "OPEN") or (
            trade.direction == Direction.SHORT and trade.offset.value != "OPEN"
        ):
            self.stop_loss_manager.remove_position(vt_symbol)
            self.write_log(f"平仓成交: 价格={trade.price:.2f}")

    def on_stop_order(self, stop_order: StopOrder):
        """停止单回报"""
        pass
