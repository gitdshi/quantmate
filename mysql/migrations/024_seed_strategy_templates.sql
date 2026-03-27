-- Migration 024: Add template_type system + seed 32 strategy templates
-- Adds template_type/layer/sub_type/composite_config columns to strategy_templates
-- and seeds 5 standalone + 23 component + 4 composite templates.
-- NOTE: Run scripts/seed_template_code.py after this migration to populate code fields.

SET NAMES 'utf8mb4';
SET CHARACTER SET utf8mb4;

-- ─────────────────────────────────────────────────────────
-- 2.1  ALTER TABLE — add new columns
-- ─────────────────────────────────────────────────────────

ALTER TABLE `quantmate`.`strategy_templates`
  ADD COLUMN template_type ENUM('standalone','component','composite') NOT NULL DEFAULT 'standalone'
    COMMENT 'standalone = VNPy CTA, component = pipeline layer, composite = pipeline blueprint'
    AFTER category,
  ADD COLUMN layer ENUM('universe','trading','risk') DEFAULT NULL
    COMMENT 'Applicable only when template_type = component'
    AFTER template_type,
  ADD COLUMN sub_type VARCHAR(50) DEFAULT NULL
    COMMENT 'Finer subclass label for component templates'
    AFTER layer,
  ADD COLUMN composite_config JSON DEFAULT NULL
    COMMENT 'Composite-only: bindings blueprint referencing sub_type values'
    AFTER sub_type;

ALTER TABLE `quantmate`.`strategy_templates`
  ADD INDEX idx_template_type (template_type),
  ADD INDEX idx_layer (layer);

-- ─────────────────────────────────────────────────────────
-- 2.2  Seed 5 standalone templates (VNPy CtaTemplate)
-- ─────────────────────────────────────────────────────────

INSERT INTO `quantmate`.`strategy_templates`
  (author_id, name, category, template_type, layer, sub_type, description, code, params_schema, default_params, version, visibility)
VALUES
  (1, 'MACD交叉策略', 'cta', 'standalone', NULL, NULL,
   'MACD histogram flip + zero-line cross entry/exit',
   '"""MACD-based CTA strategy for testing.

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
            if trade.direction == Direction.LONG and getattr(trade, "offset", None) == Offset.OPEN:
                kind = "Long entry"
            elif trade.direction == Direction.SHORT and getattr(trade, "offset", None) == Offset.CLOSE:
                kind = "Exit long"
            elif trade.direction == Direction.SHORT and getattr(trade, "offset", None) == Offset.OPEN:
                kind = "Short entry"
            elif trade.direction == Direction.LONG and getattr(trade, "offset", None) == Offset.CLOSE:
                kind = "Exit short"
            else:
                kind = f"{trade.direction} {getattr(trade, ''offset'', '''')}"
        except Exception:
            kind = str(trade.direction)

        self.write_log(f"Trade executed: {kind} {trade.volume} @ {trade.price}")

    def on_stop_order(self, stop_order: StopOrder):
        pass
',
   '{"type":"object","properties":{"fast_period":{"type":"integer","default":12},"slow_period":{"type":"integer","default":26},"signal_period":{"type":"integer","default":9},"fixed_size":{"type":"integer","default":1}}}',
   '{"fast_period":12,"slow_period":26,"signal_period":9,"fixed_size":1}',
   '1.0.0', 'public'),

  (1, '三均线趋势策略', 'cta', 'standalone', NULL, NULL,
   'Triple moving-average trend-following strategy',
   '"""
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
',
   '{"type":"object","properties":{"fast_period":{"type":"integer","default":5},"mid_period":{"type":"integer","default":10},"slow_period":{"type":"integer","default":20},"fixed_size":{"type":"integer","default":1}}}',
   '{"fast_period":5,"mid_period":10,"slow_period":20,"fixed_size":1}',
   '1.0.0', 'public'),

  (1, '海龟交易策略', 'cta', 'standalone', NULL, NULL,
   'Turtle-trading Donchian breakout with ATR stops',
   'from vnpy_ctastrategy import (
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
',
   '{"type":"object","properties":{"entry_window":{"type":"integer","default":20},"exit_window":{"type":"integer","default":10},"atr_window":{"type":"integer","default":20},"fixed_size":{"type":"integer","default":1}}}',
   '{"entry_window":20,"exit_window":10,"atr_window":20,"fixed_size":1}',
   '1.0.0', 'public'),

  (1, '布林带突破策略', 'cta', 'standalone', NULL, NULL,
   'Bollinger Band breakout with bandwidth confirmation',
   '"""Bollinger Breakout — standalone VNPy CTA strategy.

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
',
   '{"type":"object","properties":{"bb_period":{"type":"integer","default":20},"bb_std":{"type":"number","default":2.0},"bandwidth_threshold":{"type":"number","default":0.04},"fixed_size":{"type":"integer","default":1}}}',
   '{"bb_period":20,"bb_std":2.0,"bandwidth_threshold":0.04,"fixed_size":1}',
   '1.0.0', 'public'),

  (1, 'ATR通道策略', 'cta', 'standalone', NULL, NULL,
   'ATR channel breakout / reversion strategy',
   '"""ATR Channel — standalone VNPy CTA strategy.

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
',
   '{"type":"object","properties":{"ma_period":{"type":"integer","default":20},"atr_period":{"type":"integer","default":14},"atr_multiplier":{"type":"number","default":2.0},"fixed_size":{"type":"integer","default":1}}}',
   '{"ma_period":20,"atr_period":14,"atr_multiplier":2.0,"fixed_size":1}',
   '1.0.0', 'public');

-- ─────────────────────────────────────────────────────────
-- 2.3  Seed 23 component templates
-- ─────────────────────────────────────────────────────────

-- Universe components (6)
INSERT INTO `quantmate`.`strategy_templates`
  (author_id, name, category, template_type, layer, sub_type, description, code, params_schema, default_params, version, visibility)
VALUES
  (1, '市值过滤', 'cta', 'component', 'universe', 'market_cap_filter',
   'Filter by market cap range',
   '"""Market Cap Filter — universe component.

Filters the tradable universe by market capitalisation range.
"""

from typing import Any, Dict, List


def select(
    trading_day: str,
    all_symbols: List[str],
    market_data: Dict[str, Dict[str, float]],
    config: Dict[str, Any] | None = None,
) -> List[str]:
    """Return symbols whose market cap falls within [min, max]."""
    cfg = config or {}
    min_cap = cfg.get("min_market_cap", 5_000_000_000)
    max_cap = cfg.get("max_market_cap", 1_000_000_000_000)

    result: List[str] = []
    for symbol in all_symbols:
        bar = market_data.get(symbol, {})
        cap = bar.get("market_cap", 0)
        if min_cap <= cap <= max_cap:
            result.append(symbol)
    return result
',
   '{"type":"object","properties":{"min_market_cap":{"type":"number","default":5000000000},"max_market_cap":{"type":"number","default":1000000000000}}}',
   '{"min_market_cap":5000000000,"max_market_cap":1000000000000}',
   '1.0.0', 'public'),

  (1, '流动性过滤', 'cta', 'component', 'universe', 'liquidity_filter',
   'Filter by average volume and turnover rate',
   '"""Liquidity Filter — universe component.

Filters based on average daily volume and turnover rate.
"""

from typing import Any, Dict, List


def select(
    trading_day: str,
    all_symbols: List[str],
    market_data: Dict[str, Dict[str, float]],
    config: Dict[str, Any] | None = None,
) -> List[str]:
    """Return symbols meeting minimum liquidity thresholds."""
    cfg = config or {}
    min_volume = cfg.get("min_avg_volume", 1_000_000)
    min_turnover = cfg.get("min_turnover_rate", 0.005)

    result: List[str] = []
    for symbol in all_symbols:
        bar = market_data.get(symbol, {})
        vol = bar.get("avg_volume_20d", 0)
        turnover = bar.get("turnover_rate", 0)
        if vol >= min_volume and turnover >= min_turnover:
            result.append(symbol)
    return result
',
   '{"type":"object","properties":{"min_avg_volume":{"type":"number","default":1000000},"min_turnover_rate":{"type":"number","default":0.005}}}',
   '{"min_avg_volume":1000000,"min_turnover_rate":0.005}',
   '1.0.0', 'public'),

  (1, '行业轮动选股', 'cta', 'component', 'universe', 'sector_rotation',
   'Select top-momentum sectors',
   '"""Sector Rotation — universe component.

Selects stocks from the top‑performing industry sectors based on
rolling relative‑strength momentum.
"""

from typing import Any, Dict, List
from collections import defaultdict


def select(
    trading_day: str,
    all_symbols: List[str],
    market_data: Dict[str, Dict[str, float]],
    config: Dict[str, Any] | None = None,
) -> List[str]:
    """Return symbols belonging to the top‑N momentum sectors."""
    cfg = config or {}
    top_n = cfg.get("top_sectors", 3)
    momentum_key = cfg.get("momentum_key", "sector_momentum_20d")

    # group symbols by sector
    sectors: Dict[str, List[str]] = defaultdict(list)
    sector_scores: Dict[str, float] = {}
    for symbol in all_symbols:
        bar = market_data.get(symbol, {})
        sector = bar.get("sector", "Unknown")
        sectors[sector].append(symbol)
        # take the max momentum as sector score
        score = bar.get(momentum_key, 0.0)
        sector_scores[sector] = max(sector_scores.get(sector, float("-inf")), score)

    # pick top sectors
    ranked = sorted(sector_scores, key=sector_scores.get, reverse=True)  # type: ignore[arg-type]
    top_sectors = set(ranked[:top_n])

    result: List[str] = []
    for sector in top_sectors:
        result.extend(sectors[sector])
    return result
',
   '{"type":"object","properties":{"top_sectors":{"type":"integer","default":3},"momentum_key":{"type":"string","default":"sector_momentum_20d"}}}',
   '{"top_sectors":3,"momentum_key":"sector_momentum_20d"}',
   '1.0.0', 'public'),

  (1, '指数成分股', 'cta', 'component', 'universe', 'index_constituents',
   'Filter to major index constituents (CSI 300/500)',
   '"""Index Constituents — universe component.

Selects universe from major index constituent lists
(e.g. CSI 300, CSI 500, S&P 500).
"""

from typing import Any, Dict, List


def select(
    trading_day: str,
    all_symbols: List[str],
    market_data: Dict[str, Dict[str, float]],
    config: Dict[str, Any] | None = None,
) -> List[str]:
    """Return symbols that belong to the configured index."""
    cfg = config or {}
    index_name = cfg.get("index", "csi300")
    index_key = f"is_{index_name}"

    result: List[str] = []
    for symbol in all_symbols:
        bar = market_data.get(symbol, {})
        if bar.get(index_key, False):
            result.append(symbol)
    return result
',
   '{"type":"object","properties":{"index":{"type":"string","default":"csi300"}}}',
   '{"index":"csi300"}',
   '1.0.0', 'public'),

  (1, '基本面筛选', 'alpha', 'component', 'universe', 'fundamental_screen',
   'PE/PB/ROE/revenue growth screen',
   '"""Fundamental Screen — universe component.

Screens stocks by PE, PB, ROE, revenue growth and other
fundamental metrics.
"""

from typing import Any, Dict, List


def select(
    trading_day: str,
    all_symbols: List[str],
    market_data: Dict[str, Dict[str, float]],
    config: Dict[str, Any] | None = None,
) -> List[str]:
    """Return symbols passing all fundamental filters."""
    cfg = config or {}
    max_pe = cfg.get("max_pe", 40.0)
    max_pb = cfg.get("max_pb", 8.0)
    min_roe = cfg.get("min_roe", 0.08)
    min_revenue_growth = cfg.get("min_revenue_growth", 0.0)

    result: List[str] = []
    for symbol in all_symbols:
        bar = market_data.get(symbol, {})
        pe = bar.get("pe_ratio", float("inf"))
        pb = bar.get("pb_ratio", float("inf"))
        roe = bar.get("roe", 0.0)
        rev_g = bar.get("revenue_growth_yoy", 0.0)
        if pe <= max_pe and pb <= max_pb and roe >= min_roe and rev_g >= min_revenue_growth:
            result.append(symbol)
    return result
',
   '{"type":"object","properties":{"max_pe":{"type":"number","default":40},"max_pb":{"type":"number","default":8},"min_roe":{"type":"number","default":0.08},"min_revenue_growth":{"type":"number","default":0}}}',
   '{"max_pe":40,"max_pb":8,"min_roe":0.08,"min_revenue_growth":0}',
   '1.0.0', 'public'),

  (1, 'ST/停牌过滤', 'cta', 'component', 'universe', 'st_halt_filter',
   'Exclude ST, suspended and newly-listed stocks',
   '"""ST / Halt Filter — universe component.

Excludes ST‑flagged, suspended, and newly‑listed stocks.
Essential for A‑share compliance.
"""

from typing import Any, Dict, List


def select(
    trading_day: str,
    all_symbols: List[str],
    market_data: Dict[str, Dict[str, float]],
    config: Dict[str, Any] | None = None,
) -> List[str]:
    """Return symbols that are NOT ST, suspended, or too new."""
    cfg = config or {}
    min_list_days = cfg.get("min_list_days", 60)

    result: List[str] = []
    for symbol in all_symbols:
        bar = market_data.get(symbol, {})
        if bar.get("is_st", False):
            continue
        if bar.get("is_suspended", False):
            continue
        if bar.get("list_days", 0) < min_list_days:
            continue
        result.append(symbol)
    return result
',
   '{"type":"object","properties":{"min_list_days":{"type":"integer","default":60}}}',
   '{"min_list_days":60}',
   '1.0.0', 'public');

-- Trading components (11)
INSERT INTO `quantmate`.`strategy_templates`
  (author_id, name, category, template_type, layer, sub_type, description, code, params_schema, default_params, version, visibility)
VALUES
  (1, '双均线交叉信号', 'cta', 'component', 'trading', 'dual_ma_signal',
   'Fast/slow MA crossover signals',
   '"""Dual MA Signal — trading component.

Generates buy/sell signals based on fast/slow moving average crossovers.
"""

from typing import Any, Dict, List


def generate_signals(
    trading_day: str,
    universe: List[str],
    market_data: Dict[str, Dict[str, float]],
    positions: Dict[str, Any],
    config: Dict[str, Any] | None = None,
) -> List[Dict[str, Any]]:
    """Return a list of signal dicts {symbol, direction, strength, reason}."""
    cfg = config or {}
    fast_period = cfg.get("fast_period", 5)
    slow_period = cfg.get("slow_period", 20)

    signals: List[Dict[str, Any]] = []
    for symbol in universe:
        bar = market_data.get(symbol, {})
        fast_ma = bar.get(f"ma_{fast_period}", 0)
        slow_ma = bar.get(f"ma_{slow_period}", 0)
        close = bar.get("close", 0)

        if fast_ma == 0 or slow_ma == 0 or close == 0:
            continue

        if fast_ma > slow_ma and close > fast_ma:
            signals.append(
                {
                    "symbol": symbol,
                    "direction": "long",
                    "strength": min((fast_ma - slow_ma) / slow_ma * 10, 1.0),
                    "reason": f"MA{fast_period} crossed above MA{slow_period}",
                }
            )
        elif fast_ma < slow_ma and close < fast_ma:
            signals.append(
                {
                    "symbol": symbol,
                    "direction": "short",
                    "strength": min((slow_ma - fast_ma) / slow_ma * 10, 1.0),
                    "reason": f"MA{fast_period} crossed below MA{slow_period}",
                }
            )
    return signals
',
   '{"type":"object","properties":{"fast_period":{"type":"integer","default":5},"slow_period":{"type":"integer","default":20}}}',
   '{"fast_period":5,"slow_period":20}',
   '1.0.0', 'public'),

  (1, '唐奇安突破信号', 'cta', 'component', 'trading', 'donchian_breakout',
   'Donchian channel breakout entry/exit',
   '"""Donchian Breakout — trading component.

Generates signals when price breaks above/below the Donchian channel.
"""

from typing import Any, Dict, List


def generate_signals(
    trading_day: str,
    universe: List[str],
    market_data: Dict[str, Dict[str, float]],
    positions: Dict[str, Any],
    config: Dict[str, Any] | None = None,
) -> List[Dict[str, Any]]:
    """Return breakout signals based on Donchian channels."""
    cfg = config or {}
    entry_period = cfg.get("entry_period", 20)
    exit_period = cfg.get("exit_period", 10)

    signals: List[Dict[str, Any]] = []
    for symbol in universe:
        bar = market_data.get(symbol, {})
        high = bar.get("close", 0)
        upper = bar.get(f"donchian_upper_{entry_period}", 0)
        lower = bar.get(f"donchian_lower_{entry_period}", 0)
        exit_lower = bar.get(f"donchian_lower_{exit_period}", 0)

        held = symbol in positions

        if high >= upper and upper > 0 and not held:
            signals.append(
                {
                    "symbol": symbol,
                    "direction": "long",
                    "strength": 0.8,
                    "reason": f"Breakout above {entry_period}‑day high",
                }
            )
        elif high <= exit_lower and exit_lower > 0 and held:
            signals.append(
                {
                    "symbol": symbol,
                    "direction": "close",
                    "strength": 0.9,
                    "reason": f"Broke below {exit_period}‑day low — exit",
                }
            )
    return signals
',
   '{"type":"object","properties":{"entry_period":{"type":"integer","default":20},"exit_period":{"type":"integer","default":10}}}',
   '{"entry_period":20,"exit_period":10}',
   '1.0.0', 'public'),

  (1, 'MACD信号', 'cta', 'component', 'trading', 'macd_signal',
   'MACD histogram flip + zero-line cross',
   '"""MACD Signal — trading component.

Generates signals from MACD histogram flips and zero‑line crosses.
"""

from typing import Any, Dict, List


def generate_signals(
    trading_day: str,
    universe: List[str],
    market_data: Dict[str, Dict[str, float]],
    positions: Dict[str, Any],
    config: Dict[str, Any] | None = None,
) -> List[Dict[str, Any]]:
    """Return MACD‑based signals."""
    cfg = config or {}
    fast = cfg.get("fast_period", 12)
    slow = cfg.get("slow_period", 26)
    signal_period = cfg.get("signal_period", 9)
    _ = (fast, slow, signal_period)  # used to select the right pre-computed field

    signals: List[Dict[str, Any]] = []
    for symbol in universe:
        bar = market_data.get(symbol, {})
        macd_val = bar.get("macd", 0)
        signal_val = bar.get("macd_signal", 0)
        hist = bar.get("macd_hist", 0)
        prev_hist = bar.get("macd_hist_prev", 0)

        # histogram flip
        if prev_hist <= 0 < hist:
            signals.append(
                {
                    "symbol": symbol,
                    "direction": "long",
                    "strength": min(abs(hist) * 5, 1.0),
                    "reason": "MACD histogram flipped positive",
                }
            )
        elif prev_hist >= 0 > hist:
            signals.append(
                {
                    "symbol": symbol,
                    "direction": "short",
                    "strength": min(abs(hist) * 5, 1.0),
                    "reason": "MACD histogram flipped negative",
                }
            )
        # zero‑line cross
        elif macd_val > 0 and signal_val < 0:
            signals.append(
                {
                    "symbol": symbol,
                    "direction": "long",
                    "strength": 0.6,
                    "reason": "MACD crossed zero line upward",
                }
            )
    return signals
',
   '{"type":"object","properties":{"fast_period":{"type":"integer","default":12},"slow_period":{"type":"integer","default":26},"signal_period":{"type":"integer","default":9}}}',
   '{"fast_period":12,"slow_period":26,"signal_period":9}',
   '1.0.0', 'public'),

  (1, '布林带回归信号', 'cta', 'component', 'trading', 'bollinger_reversion',
   'Mean-reversion at Bollinger extremes',
   '"""Bollinger Reversion — trading component.

Mean‑reversion signals triggered when price touches or exceeds
Bollinger Bands.
"""

from typing import Any, Dict, List


def generate_signals(
    trading_day: str,
    universe: List[str],
    market_data: Dict[str, Dict[str, float]],
    positions: Dict[str, Any],
    config: Dict[str, Any] | None = None,
) -> List[Dict[str, Any]]:
    """Return reversion signals at Bollinger extremes."""
    cfg = config or {}
    bb_period = cfg.get("bb_period", 20)
    bb_std = cfg.get("bb_std", 2.0)
    _ = bb_period  # field pre‑computed

    signals: List[Dict[str, Any]] = []
    for symbol in universe:
        bar = market_data.get(symbol, {})
        close = bar.get("close", 0)
        upper = bar.get("bb_upper", 0)
        lower = bar.get("bb_lower", 0)
        mid = bar.get("bb_mid", 0)

        if close == 0 or mid == 0:
            continue

        pct_b = (close - lower) / (upper - lower) if upper != lower else 0.5

        if close <= lower:
            signals.append(
                {
                    "symbol": symbol,
                    "direction": "long",
                    "strength": min(1.0, (1 - pct_b)),
                    "reason": f"Price touched lower BB ({bb_std}σ)",
                }
            )
        elif close >= upper:
            signals.append(
                {
                    "symbol": symbol,
                    "direction": "short",
                    "strength": min(1.0, pct_b),
                    "reason": f"Price touched upper BB ({bb_std}σ)",
                }
            )
    return signals
',
   '{"type":"object","properties":{"bb_period":{"type":"integer","default":20},"bb_std":{"type":"number","default":2.0}}}',
   '{"bb_period":20,"bb_std":2.0}',
   '1.0.0', 'public'),

  (1, '多因子Alpha信号', 'alpha', 'component', 'trading', 'multi_factor_alpha',
   'Value + momentum + quality composite z-score',
   '"""Multi‑Factor Alpha — trading component.

Combines value, momentum, and quality z‑scores into a composite
alpha signal.
"""

from typing import Any, Dict, List


def generate_signals(
    trading_day: str,
    universe: List[str],
    market_data: Dict[str, Dict[str, float]],
    positions: Dict[str, Any],
    config: Dict[str, Any] | None = None,
) -> List[Dict[str, Any]]:
    """Return alpha signals sorted by composite z‑score."""
    cfg = config or {}
    w_value = cfg.get("weight_value", 0.4)
    w_momentum = cfg.get("weight_momentum", 0.3)
    w_quality = cfg.get("weight_quality", 0.3)
    top_k = cfg.get("top_k", 10)
    threshold = cfg.get("alpha_threshold", 0.5)

    scored: List[tuple[str, float]] = []
    for symbol in universe:
        bar = market_data.get(symbol, {})
        z_val = bar.get("z_value", 0)
        z_mom = bar.get("z_momentum", 0)
        z_qual = bar.get("z_quality", 0)
        composite = w_value * z_val + w_momentum * z_mom + w_quality * z_qual
        scored.append((symbol, composite))

    scored.sort(key=lambda x: x[1], reverse=True)

    signals: List[Dict[str, Any]] = []
    for symbol, score in scored[:top_k]:
        if score < threshold:
            break
        signals.append(
            {
                "symbol": symbol,
                "direction": "long",
                "strength": min(score / 3.0, 1.0),
                "reason": f"Multi‑factor alpha z={score:.2f}",
            }
        )
    return signals
',
   '{"type":"object","properties":{"weight_value":{"type":"number","default":0.4},"weight_momentum":{"type":"number","default":0.3},"weight_quality":{"type":"number","default":0.3},"top_k":{"type":"integer","default":10},"alpha_threshold":{"type":"number","default":0.5}}}',
   '{"weight_value":0.4,"weight_momentum":0.3,"weight_quality":0.3,"top_k":10,"alpha_threshold":0.5}',
   '1.0.0', 'public'),

  (1, '动量信号', 'alpha', 'component', 'trading', 'momentum_signal',
   'Cross-sectional momentum long/short',
   '"""Momentum Signal — trading component.

Cross‑sectional momentum: go long stocks with strongest N‑day
returns, short the weakest.
"""

from typing import Any, Dict, List


def generate_signals(
    trading_day: str,
    universe: List[str],
    market_data: Dict[str, Dict[str, float]],
    positions: Dict[str, Any],
    config: Dict[str, Any] | None = None,
) -> List[Dict[str, Any]]:
    """Return momentum‑ranked long/short signals."""
    cfg = config or {}
    lookback = cfg.get("momentum_days", 20)
    top_pct = cfg.get("long_pct", 0.1)
    bottom_pct = cfg.get("short_pct", 0.1)

    returns: List[tuple[str, float]] = []
    for symbol in universe:
        bar = market_data.get(symbol, {})
        ret = bar.get(f"return_{lookback}d", 0)
        returns.append((symbol, ret))

    returns.sort(key=lambda x: x[1], reverse=True)
    n = len(returns)
    long_n = max(1, int(n * top_pct))
    short_n = max(1, int(n * bottom_pct))

    signals: List[Dict[str, Any]] = []
    for symbol, ret in returns[:long_n]:
        signals.append(
            {
                "symbol": symbol,
                "direction": "long",
                "strength": min(abs(ret) * 5, 1.0),
                "reason": f"{lookback}d momentum top decile ({ret:+.2%})",
            }
        )
    for symbol, ret in returns[-short_n:]:
        signals.append(
            {
                "symbol": symbol,
                "direction": "short",
                "strength": min(abs(ret) * 5, 1.0),
                "reason": f"{lookback}d momentum bottom decile ({ret:+.2%})",
            }
        )
    return signals
',
   '{"type":"object","properties":{"momentum_days":{"type":"integer","default":20},"long_pct":{"type":"number","default":0.1},"short_pct":{"type":"number","default":0.1}}}',
   '{"momentum_days":20,"long_pct":0.1,"short_pct":0.1}',
   '1.0.0', 'public'),

  (1, '均值回归Alpha', 'alpha', 'component', 'trading', 'mean_reversion_alpha',
   'Z-score reversion at extended deviations',
   '"""Mean Reversion Alpha — trading component.

Identifies over‑extended price deviations from a rolling mean and
generates reversion entry signals.
"""

from typing import Any, Dict, List


def generate_signals(
    trading_day: str,
    universe: List[str],
    market_data: Dict[str, Dict[str, float]],
    positions: Dict[str, Any],
    config: Dict[str, Any] | None = None,
) -> List[Dict[str, Any]]:
    """Return mean‑reversion signals for over‑extended stocks."""
    cfg = config or {}
    lookback = cfg.get("lookback", 20)
    entry_z = cfg.get("entry_z_threshold", 2.0)

    signals: List[Dict[str, Any]] = []
    for symbol in universe:
        bar = market_data.get(symbol, {})
        close = bar.get("close", 0)
        mean = bar.get(f"ma_{lookback}", 0)
        std = bar.get(f"std_{lookback}", 0)

        if std == 0 or mean == 0:
            continue

        z = (close - mean) / std

        if z <= -entry_z:
            signals.append(
                {
                    "symbol": symbol,
                    "direction": "long",
                    "strength": min(abs(z) / 4.0, 1.0),
                    "reason": f"Price {z:.1f}σ below mean — reversion long",
                }
            )
        elif z >= entry_z:
            signals.append(
                {
                    "symbol": symbol,
                    "direction": "short",
                    "strength": min(abs(z) / 4.0, 1.0),
                    "reason": f"Price {z:.1f}σ above mean — reversion short",
                }
            )
    return signals
',
   '{"type":"object","properties":{"lookback":{"type":"integer","default":20},"entry_z_threshold":{"type":"number","default":2.0}}}',
   '{"lookback":20,"entry_z_threshold":2.0}',
   '1.0.0', 'public'),

  (1, '固定网格信号', 'grid', 'component', 'trading', 'fixed_grid',
   'Fixed-percentage grid entry levels',
   '"""Fixed Grid — trading component.

Places buy/sell signals at fixed price intervals around a
configurable base price.
"""

from typing import Any, Dict, List


def generate_signals(
    trading_day: str,
    universe: List[str],
    market_data: Dict[str, Dict[str, float]],
    positions: Dict[str, Any],
    config: Dict[str, Any] | None = None,
) -> List[Dict[str, Any]]:
    """Return grid‑level entry/exit signals."""
    cfg = config or {}
    grid_pct = cfg.get("grid_pct", 0.02)
    max_layers = cfg.get("max_layers", 5)

    signals: List[Dict[str, Any]] = []
    for symbol in universe:
        bar = market_data.get(symbol, {})
        close = bar.get("close", 0)
        base = bar.get("grid_base_price", close)

        if close == 0 or base == 0:
            continue

        deviation = (close - base) / base
        layer = int(abs(deviation) / grid_pct)

        if layer == 0 or layer > max_layers:
            continue

        direction = "long" if deviation < 0 else "short"
        signals.append(
            {
                "symbol": symbol,
                "direction": direction,
                "strength": min(layer / max_layers, 1.0),
                "reason": f"Grid layer {layer} ({deviation:+.1%} from base)",
            }
        )
    return signals
',
   '{"type":"object","properties":{"grid_pct":{"type":"number","default":0.02},"max_layers":{"type":"integer","default":5}}}',
   '{"grid_pct":0.02,"max_layers":5}',
   '1.0.0', 'public'),

  (1, '动态网格信号', 'grid', 'component', 'trading', 'dynamic_grid',
   'ATR-adaptive grid spacing',
   '"""Dynamic Grid — trading component.

Like fixed grid but adapts spacing based on ATR (Average True Range).
"""

from typing import Any, Dict, List


def generate_signals(
    trading_day: str,
    universe: List[str],
    market_data: Dict[str, Dict[str, float]],
    positions: Dict[str, Any],
    config: Dict[str, Any] | None = None,
) -> List[Dict[str, Any]]:
    """Return ATR‑adaptive grid signals."""
    cfg = config or {}
    atr_multiplier = cfg.get("atr_multiplier", 1.0)
    max_layers = cfg.get("max_layers", 5)
    atr_period = cfg.get("atr_period", 14)

    signals: List[Dict[str, Any]] = []
    for symbol in universe:
        bar = market_data.get(symbol, {})
        close = bar.get("close", 0)
        base = bar.get("grid_base_price", close)
        atr = bar.get(f"atr_{atr_period}", 0)

        if close == 0 or base == 0 or atr == 0:
            continue

        grid_size = atr * atr_multiplier
        deviation = close - base
        layer = int(abs(deviation) / grid_size)

        if layer == 0 or layer > max_layers:
            continue

        direction = "long" if deviation < 0 else "short"
        signals.append(
            {
                "symbol": symbol,
                "direction": direction,
                "strength": min(layer / max_layers, 1.0),
                "reason": f"Dynamic grid L{layer} (ATR={atr:.2f}, Δ={deviation:+.2f})",
            }
        )
    return signals
',
   '{"type":"object","properties":{"atr_multiplier":{"type":"number","default":1.0},"max_layers":{"type":"integer","default":5},"atr_period":{"type":"integer","default":14}}}',
   '{"atr_multiplier":1.0,"max_layers":5,"atr_period":14}',
   '1.0.0', 'public'),

  (1, '配对交易信号', 'arbitrage', 'component', 'trading', 'pair_trading_signal',
   'Co-integrated pair spread-reversion',
   '"""Pair Trading Signal — trading component.

Identifies co‑integrated pairs and generates spread‑reversion signals.
"""

from typing import Any, Dict, List


def generate_signals(
    trading_day: str,
    universe: List[str],
    market_data: Dict[str, Dict[str, float]],
    positions: Dict[str, Any],
    config: Dict[str, Any] | None = None,
) -> List[Dict[str, Any]]:
    """Return pair spread‑reversion signals."""
    cfg = config or {}
    entry_z = cfg.get("entry_z", 2.0)
    exit_z = cfg.get("exit_z", 0.5)
    pairs = cfg.get("pairs", [])

    signals: List[Dict[str, Any]] = []
    for pair in pairs:
        leg_a = pair.get("leg_a", "")
        leg_b = pair.get("leg_b", "")
        if leg_a not in universe or leg_b not in universe:
            continue

        bar_a = market_data.get(leg_a, {})
        bar_b = market_data.get(leg_b, {})
        spread_z = bar_a.get(f"pair_z_{leg_b}", 0)

        if abs(spread_z) >= entry_z:
            # spread too wide — expect reversion
            if spread_z > 0:
                signals.append(
                    {
                        "symbol": leg_a,
                        "direction": "short",
                        "strength": min(abs(spread_z) / 4, 1.0),
                        "reason": f"Pair {leg_a}/{leg_b} spread z={spread_z:.1f} — short A",
                    }
                )
                signals.append(
                    {
                        "symbol": leg_b,
                        "direction": "long",
                        "strength": min(abs(spread_z) / 4, 1.0),
                        "reason": f"Pair {leg_a}/{leg_b} spread z={spread_z:.1f} — long B",
                    }
                )
            else:
                signals.append(
                    {
                        "symbol": leg_a,
                        "direction": "long",
                        "strength": min(abs(spread_z) / 4, 1.0),
                        "reason": f"Pair {leg_a}/{leg_b} spread z={spread_z:.1f} — long A",
                    }
                )
                signals.append(
                    {
                        "symbol": leg_b,
                        "direction": "short",
                        "strength": min(abs(spread_z) / 4, 1.0),
                        "reason": f"Pair {leg_a}/{leg_b} spread z={spread_z:.1f} — short B",
                    }
                )
        elif abs(spread_z) <= exit_z:
            held_a = leg_a in positions
            held_b = leg_b in positions
            if held_a:
                signals.append(
                    {
                        "symbol": leg_a,
                        "direction": "close",
                        "strength": 0.9,
                        "reason": f"Pair spread converged z={spread_z:.1f} — close A",
                    }
                )
            if held_b:
                signals.append(
                    {
                        "symbol": leg_b,
                        "direction": "close",
                        "strength": 0.9,
                        "reason": f"Pair spread converged z={spread_z:.1f} — close B",
                    }
                )
    return signals
',
   '{"type":"object","properties":{"entry_z":{"type":"number","default":2.0},"exit_z":{"type":"number","default":0.5},"pairs":{"type":"array","items":{"type":"object","properties":{"leg_a":{"type":"string"},"leg_b":{"type":"string"}}}}}}',
   '{"entry_z":2.0,"exit_z":0.5,"pairs":[]}',
   '1.0.0', 'public'),

  (1, 'ETF套利信号', 'arbitrage', 'component', 'trading', 'etf_arbitrage',
   'ETF premium/discount arbitrage',
   '"""ETF Arbitrage — trading component.

Exploits premium/discount between an ETF and its underlying basket.
"""

from typing import Any, Dict, List


def generate_signals(
    trading_day: str,
    universe: List[str],
    market_data: Dict[str, Dict[str, float]],
    positions: Dict[str, Any],
    config: Dict[str, Any] | None = None,
) -> List[Dict[str, Any]]:
    """Return ETF–basket arbitrage signals."""
    cfg = config or {}
    premium_threshold = cfg.get("premium_threshold", 0.005)
    discount_threshold = cfg.get("discount_threshold", -0.005)

    signals: List[Dict[str, Any]] = []
    for symbol in universe:
        bar = market_data.get(symbol, {})
        premium = bar.get("etf_premium", 0)

        if premium == 0:
            continue

        if premium >= premium_threshold:
            # ETF over‑priced vs basket — short ETF, long basket
            signals.append(
                {
                    "symbol": symbol,
                    "direction": "short",
                    "strength": min(abs(premium) / 0.02, 1.0),
                    "reason": f"ETF premium {premium:+.2%} — arbitrage short",
                }
            )
        elif premium <= discount_threshold:
            # ETF under‑priced — long ETF, short basket
            signals.append(
                {
                    "symbol": symbol,
                    "direction": "long",
                    "strength": min(abs(premium) / 0.02, 1.0),
                    "reason": f"ETF discount {premium:+.2%} — arbitrage long",
                }
            )
    return signals
',
   '{"type":"object","properties":{"premium_threshold":{"type":"number","default":0.005},"discount_threshold":{"type":"number","default":-0.005}}}',
   '{"premium_threshold":0.005,"discount_threshold":-0.005}',
   '1.0.0', 'public');

-- Risk components (6)
INSERT INTO `quantmate`.`strategy_templates`
  (author_id, name, category, template_type, layer, sub_type, description, code, params_schema, default_params, version, visibility)
VALUES
  (1, '等权配置', 'cta', 'component', 'risk', 'equal_weight',
   'Equal-weight capital allocation',
   '"""Equal Weight — risk component.

Allocates equal capital weight to every signal that passes through.
"""

from typing import Any, Dict, List


def filter_and_size(
    signals: List[Dict[str, Any]],
    cash: float,
    positions: Dict[str, Any],
    prices: Dict[str, float],
    config: Dict[str, Any] | None = None,
) -> List[Dict[str, Any]]:
    """Return sized orders with equal weight allocation."""
    cfg = config or {}
    max_positions = cfg.get("max_positions", 10)

    # filter to actionable signals only
    actionable = [s for s in signals if s.get("direction") in ("long", "short")]
    actionable = actionable[:max_positions]

    if not actionable:
        return []

    weight = 1.0 / len(actionable)
    alloc = cash * weight

    orders: List[Dict[str, Any]] = []
    for sig in actionable:
        symbol = sig["symbol"]
        price = prices.get(symbol, 0)
        if price <= 0:
            continue
        volume = int(alloc / price / 100) * 100  # round to board lot
        if volume <= 0:
            continue
        orders.append(
            {
                "symbol": symbol,
                "direction": sig["direction"],
                "volume": volume,
                "price": price,
                "reason": sig.get("reason", ""),
            }
        )
    return orders
',
   '{"type":"object","properties":{"max_positions":{"type":"integer","default":10}}}',
   '{"max_positions":10}',
   '1.0.0', 'public'),

  (1, '波动率平价', 'alpha', 'component', 'risk', 'volatility_parity',
   'Inverse-volatility position sizing',
   '"""Volatility Parity — risk component.

Sizes positions inversely proportional to each asset''s recent
volatility so that each contributes equal risk.
"""

from typing import Any, Dict, List


def filter_and_size(
    signals: List[Dict[str, Any]],
    cash: float,
    positions: Dict[str, Any],
    prices: Dict[str, float],
    config: Dict[str, Any] | None = None,
) -> List[Dict[str, Any]]:
    """Return orders sized by inverse volatility."""
    cfg = config or {}
    max_positions = cfg.get("max_positions", 10)
    vol_key = cfg.get("vol_key", "volatility_20d")
    target_vol = cfg.get("target_portfolio_vol", 0.15)

    actionable = [s for s in signals if s.get("direction") in ("long", "short")]
    actionable = actionable[:max_positions]
    if not actionable:
        return []

    # compute inverse-vol weights
    inv_vols: List[float] = []
    for sig in actionable:
        vol = sig.get(vol_key, 0.3)
        inv_vols.append(1.0 / max(vol, 0.01))
    total_inv = sum(inv_vols) or 1.0

    orders: List[Dict[str, Any]] = []
    for sig, inv_v in zip(actionable, inv_vols):
        weight = inv_v / total_inv
        alloc = cash * weight
        symbol = sig["symbol"]
        price = prices.get(symbol, 0)
        if price <= 0:
            continue
        volume = int(alloc / price / 100) * 100
        if volume <= 0:
            continue
        orders.append(
            {
                "symbol": symbol,
                "direction": sig["direction"],
                "volume": volume,
                "price": price,
                "reason": sig.get("reason", ""),
            }
        )
    return orders
',
   '{"type":"object","properties":{"max_positions":{"type":"integer","default":10},"target_portfolio_vol":{"type":"number","default":0.15}}}',
   '{"max_positions":10,"target_portfolio_vol":0.15}',
   '1.0.0', 'public'),

  (1, '固定止损', 'cta', 'component', 'risk', 'fixed_stop_loss',
   'Fixed percentage stop-loss with risk-per-trade sizing',
   '"""Fixed Stop Loss — risk component.

Rejects signals that have already moved beyond the stop threshold
and attaches stop‑loss prices to surviving orders.
"""

from typing import Any, Dict, List


def filter_and_size(
    signals: List[Dict[str, Any]],
    cash: float,
    positions: Dict[str, Any],
    prices: Dict[str, float],
    config: Dict[str, Any] | None = None,
) -> List[Dict[str, Any]]:
    """Return orders with fixed stop‑loss prices attached."""
    cfg = config or {}
    stop_pct = cfg.get("stop_pct", 0.05)
    max_positions = cfg.get("max_positions", 20)
    risk_per_trade = cfg.get("risk_per_trade_pct", 0.02)

    actionable = [s for s in signals if s.get("direction") in ("long", "short")]
    actionable = actionable[:max_positions]

    orders: List[Dict[str, Any]] = []
    for sig in actionable:
        symbol = sig["symbol"]
        price = prices.get(symbol, 0)
        if price <= 0:
            continue

        if sig["direction"] == "long":
            stop = price * (1 - stop_pct)
        else:
            stop = price * (1 + stop_pct)

        risk_per_share = abs(price - stop)
        if risk_per_share == 0:
            continue
        max_loss = cash * risk_per_trade
        volume = int(max_loss / risk_per_share / 100) * 100
        if volume <= 0:
            continue

        orders.append(
            {
                "symbol": symbol,
                "direction": sig["direction"],
                "volume": volume,
                "price": price,
                "stop_price": round(stop, 2),
                "reason": sig.get("reason", ""),
            }
        )
    return orders
',
   '{"type":"object","properties":{"stop_pct":{"type":"number","default":0.05},"risk_per_trade_pct":{"type":"number","default":0.02},"max_positions":{"type":"integer","default":20}}}',
   '{"stop_pct":0.05,"risk_per_trade_pct":0.02,"max_positions":20}',
   '1.0.0', 'public'),

  (1, '追踪止损', 'cta', 'component', 'risk', 'trailing_stop',
   'Trailing stop-loss that ratchets with price',
   '"""Trailing Stop — risk component.

Attaches trailing stop‑loss orders that ratchet with price movement.
"""

from typing import Any, Dict, List


def filter_and_size(
    signals: List[Dict[str, Any]],
    cash: float,
    positions: Dict[str, Any],
    prices: Dict[str, float],
    config: Dict[str, Any] | None = None,
) -> List[Dict[str, Any]]:
    """Return orders with trailing-stop metadata."""
    cfg = config or {}
    trail_pct = cfg.get("trail_pct", 0.03)
    max_positions = cfg.get("max_positions", 20)
    alloc_pct = cfg.get("alloc_pct_per_trade", 0.05)

    actionable = [s for s in signals if s.get("direction") in ("long", "short")]
    actionable = actionable[:max_positions]

    orders: List[Dict[str, Any]] = []
    for sig in actionable:
        symbol = sig["symbol"]
        price = prices.get(symbol, 0)
        if price <= 0:
            continue

        alloc = cash * alloc_pct
        volume = int(alloc / price / 100) * 100
        if volume <= 0:
            continue

        if sig["direction"] == "long":
            trail_stop = price * (1 - trail_pct)
        else:
            trail_stop = price * (1 + trail_pct)

        orders.append(
            {
                "symbol": symbol,
                "direction": sig["direction"],
                "volume": volume,
                "price": price,
                "trail_stop": round(trail_stop, 2),
                "trail_pct": trail_pct,
                "reason": sig.get("reason", ""),
            }
        )
    return orders
',
   '{"type":"object","properties":{"trail_pct":{"type":"number","default":0.03},"max_positions":{"type":"integer","default":20},"alloc_pct_per_trade":{"type":"number","default":0.05}}}',
   '{"trail_pct":0.03,"max_positions":20,"alloc_pct_per_trade":0.05}',
   '1.0.0', 'public'),

  (1, '回撤控制', 'cta', 'component', 'risk', 'drawdown_control',
   'Throttle new entries when portfolio drawdown exceeds threshold',
   '"""Drawdown Control — risk component.

Reduces or blocks new entries when portfolio drawdown exceeds
configurable thresholds.
"""

from typing import Any, Dict, List


def filter_and_size(
    signals: List[Dict[str, Any]],
    cash: float,
    positions: Dict[str, Any],
    prices: Dict[str, float],
    config: Dict[str, Any] | None = None,
) -> List[Dict[str, Any]]:
    """Return orders throttled by current drawdown level."""
    cfg = config or {}
    max_dd = cfg.get("max_drawdown", 0.15)
    reduce_dd = cfg.get("reduce_at_drawdown", 0.10)
    scale_factor = cfg.get("reduce_scale", 0.5)
    alloc_pct = cfg.get("alloc_pct_per_trade", 0.05)

    # compute current drawdown
    peak = cfg.get("portfolio_peak", cash)
    current_value = cash + sum(
        positions.get(s, {}).get("volume", 0) * prices.get(s, 0)
        for s in positions
        if isinstance(positions.get(s), dict)
    )
    dd = (peak - current_value) / peak if peak > 0 else 0

    if dd >= max_dd:
        # drawdown too deep — reject all new entries
        return []

    scale = scale_factor if dd >= reduce_dd else 1.0

    actionable = [s for s in signals if s.get("direction") in ("long", "short")]

    orders: List[Dict[str, Any]] = []
    for sig in actionable:
        symbol = sig["symbol"]
        price = prices.get(symbol, 0)
        if price <= 0:
            continue
        alloc = cash * alloc_pct * scale
        volume = int(alloc / price / 100) * 100
        if volume <= 0:
            continue
        orders.append(
            {
                "symbol": symbol,
                "direction": sig["direction"],
                "volume": volume,
                "price": price,
                "reason": sig.get("reason", ""),
            }
        )
    return orders
',
   '{"type":"object","properties":{"max_drawdown":{"type":"number","default":0.15},"reduce_at_drawdown":{"type":"number","default":0.10},"reduce_scale":{"type":"number","default":0.5},"alloc_pct_per_trade":{"type":"number","default":0.05}}}',
   '{"max_drawdown":0.15,"reduce_at_drawdown":0.10,"reduce_scale":0.5,"alloc_pct_per_trade":0.05}',
   '1.0.0', 'public'),

  (1, '持仓限制', 'cta', 'component', 'risk', 'position_limits',
   'Per-symbol and portfolio position limit enforcement',
   '"""Position Limits — risk component.

Enforces per‑symbol and portfolio‑level position limits.
"""

from typing import Any, Dict, List


def filter_and_size(
    signals: List[Dict[str, Any]],
    cash: float,
    positions: Dict[str, Any],
    prices: Dict[str, float],
    config: Dict[str, Any] | None = None,
) -> List[Dict[str, Any]]:
    """Return orders capped by position-limit constraints."""
    cfg = config or {}
    max_single_pct = cfg.get("max_single_position_pct", 0.10)
    max_total_positions = cfg.get("max_total_positions", 20)
    alloc_pct = cfg.get("alloc_pct_per_trade", 0.05)

    current_count = len(positions)
    remaining_slots = max(0, max_total_positions - current_count)

    actionable = [s for s in signals if s.get("direction") in ("long", "short")]
    actionable = actionable[:remaining_slots]

    portfolio_value = cash + sum(
        positions.get(s, {}).get("volume", 0) * prices.get(s, 0)
        for s in positions
        if isinstance(positions.get(s), dict)
    )
    max_single = portfolio_value * max_single_pct

    orders: List[Dict[str, Any]] = []
    for sig in actionable:
        symbol = sig["symbol"]
        price = prices.get(symbol, 0)
        if price <= 0:
            continue

        alloc = min(cash * alloc_pct, max_single)

        # subtract existing exposure
        existing = positions.get(symbol, {})
        if isinstance(existing, dict):
            existing_value = existing.get("volume", 0) * price
            alloc = min(alloc, max_single - existing_value)

        if alloc <= 0:
            continue
        volume = int(alloc / price / 100) * 100
        if volume <= 0:
            continue

        orders.append(
            {
                "symbol": symbol,
                "direction": sig["direction"],
                "volume": volume,
                "price": price,
                "reason": sig.get("reason", ""),
            }
        )
    return orders
',
   '{"type":"object","properties":{"max_single_position_pct":{"type":"number","default":0.10},"max_total_positions":{"type":"integer","default":20},"alloc_pct_per_trade":{"type":"number","default":0.05}}}',
   '{"max_single_position_pct":0.10,"max_total_positions":20,"alloc_pct_per_trade":0.05}',
   '1.0.0', 'public');

-- ─────────────────────────────────────────────────────────
-- 2.4  Seed 4 composite templates
-- ─────────────────────────────────────────────────────────

INSERT INTO `quantmate`.`strategy_templates`
  (author_id, name, category, template_type, layer, sub_type, composite_config, description, code, params_schema, default_params, version, visibility)
VALUES
  (1, 'CTA趋势跟踪组合', 'cta', 'composite', NULL, NULL,
   '{"bindings":{"universe":["market_cap_filter","liquidity_filter","st_halt_filter"],"trading":["dual_ma_signal"],"risk":["fixed_stop_loss","drawdown_control"]}}',
   'Classic CTA trend-following composite: cap+liquidity+ST filter → dual MA signals → fixed stop + drawdown control',
   '-- composite template, no standalone code',
   NULL, NULL,
   '1.0.0', 'public'),

  (1, 'Alpha多因子组合', 'alpha', 'composite', NULL, NULL,
   '{"bindings":{"universe":["index_constituents","fundamental_screen","st_halt_filter"],"trading":["multi_factor_alpha"],"risk":["volatility_parity","position_limits"]}}',
   'Multi-factor alpha composite: index+fundamentals → alpha z-score → vol-parity sizing + position caps',
   '-- composite template, no standalone code',
   NULL, NULL,
   '1.0.0', 'public'),

  (1, '网格震荡组合', 'grid', 'composite', NULL, NULL,
   '{"bindings":{"universe":["liquidity_filter","st_halt_filter"],"trading":["dynamic_grid"],"risk":["equal_weight","trailing_stop"]}}',
   'Grid-trading composite: liquidity screen → dynamic ATR grid → equal weight + trailing stop',
   '-- composite template, no standalone code',
   NULL, NULL,
   '1.0.0', 'public'),

  (1, '统计套利组合', 'arbitrage', 'composite', NULL, NULL,
   '{"bindings":{"universe":["liquidity_filter","index_constituents"],"trading":["pair_trading_signal"],"risk":["equal_weight","drawdown_control"]}}',
   'Statistical arbitrage composite: liquidity+index filter → pair spread signals → equal weight + drawdown control',
   '-- composite template, no standalone code',
   NULL, NULL,
   '1.0.0', 'public');
