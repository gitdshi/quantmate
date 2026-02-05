# Test edit at 1770282546.2944791
# Version 1 - Added comment
from vnpy_ctastrategy import CtaTemplate, BarGenerator
from vnpy.trader.object import BarData, TickData

class SimpleMAStrategy(CtaTemplate):
    """Simple Moving Average Strategy - VNPy 4.x compatible."""
    
    author = "TraderMate"
    
    ma_window = 20
    fixed_size = 1
    
    parameters = ["ma_window", "fixed_size"]
    variables = ["ma_value"]
    
    def __init__(self, cta_engine, strategy_name, vt_symbol, setting):
        super().__init__(cta_engine, strategy_name, vt_symbol, setting)
        self.bg = BarGenerator(self.on_bar)
        self.ma_value = 0.0
        self.prices = []
    
    def on_init(self):
        """Called when strategy is initialized."""
        self.write_log("策略初始化")
    
    def on_start(self):
        """Called when strategy is started."""
        self.write_log("策略启动")
    
    def on_stop(self):
        """Called when strategy is stopped."""
        self.write_log("策略停止")
    
    def on_tick(self, tick: TickData):
        """Called on tick update."""
        self.bg.update_tick(tick)
    
    def on_bar(self, bar: BarData):
        """Called on bar update."""
        self.prices.append(bar.close_price)
        if len(self.prices) > self.ma_window:
            self.prices.pop(0)
        
        if len(self.prices) >= self.ma_window:
            self.ma_value = sum(self.prices) / len(self.prices)
            
            if bar.close_price > self.ma_value and self.pos == 0:
                self.buy(bar.close_price, self.fixed_size)
            elif bar.close_price < self.ma_value and self.pos > 0:
                self.sell(bar.close_price, self.pos)
        
        self.put_event()
