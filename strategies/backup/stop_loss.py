"""
通用止损策略模块
提供固定止损和移动止损功能，可被其他策略调用

Fixed Stop Loss (固定止损): 基于过去N天股价的2倍标准差
Trailing Stop Loss (移动止损): 基于过去N天股价的1倍标准差，随价格上涨动态调整

Usage:
    from app.strategies.stop_loss import StopLossManager
    
    # 在策略中初始化
    self.stop_loss_manager = StopLossManager(
        fixed_std_multiplier=2.0,
        trailing_std_multiplier=1.0,
        lookback_period=10
    )
    
    # 开仓时设置止损
    self.stop_loss_manager.set_entry(vt_symbol, entry_price, close_prices[-10:])
    
    # 每根K线更新移动止损
    self.stop_loss_manager.update_trailing_stop(vt_symbol, current_price, close_prices[-10:])
    
    # 检查是否触发止损
    if self.stop_loss_manager.should_stop_loss(vt_symbol, current_price):
        # 执行止损平仓
        ...
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
from dataclasses import dataclass, field
from typing import Dict, List, Optional
import numpy as np

# File history/version utilities
from pathlib import Path
import shutil
import datetime
import os


@dataclass
class StopLossState:
    """单个持仓的止损状态"""
    vt_symbol: str
    entry_price: float
    fixed_stop_price: float      # 固定止损价格
    trailing_stop_price: float   # 移动止损价格
    highest_price: float         # 持仓期间最高价（用于移动止损）
    is_long: bool = True         # True=多头, False=空头
    
    def get_active_stop_price(self) -> float:
        """获取当前生效的止损价格（取较高者保护利润）"""
        if self.is_long:
            return max(self.fixed_stop_price, self.trailing_stop_price)
        else:
            return min(self.fixed_stop_price, self.trailing_stop_price)


class StopLossManager:
    """
    通用止损管理器
    
    支持两种止损模式：
    1. 固定止损 (Fixed Stop Loss)
       - 入场时计算，基于过去N天收盘价的2倍标准差
       - 止损价 = 入场价 - 2 * std (多头) 或 入场价 + 2 * std (空头)
       
    2. 移动止损 (Trailing Stop Loss)
       - 动态调整，基于过去N天收盘价的1倍标准差
       - 随着价格上涨，止损价也向上移动
       - 止损价只能向有利方向移动，不会回退
    
    Parameters:
        fixed_std_multiplier: 固定止损的标准差倍数，默认1.0
        trailing_std_multiplier: 移动止损的标准差倍数，默认2.0
        lookback_period: 计算标准差的回看周期，默认10天
        use_fixed_stop: 是否启用固定止损，默认True
        use_trailing_stop: 是否启用移动止损，默认True
    """
    
    def __init__(
        self,
        fixed_std_multiplier: float = 1.0,
        trailing_std_multiplier: float = 2.0,
        lookback_period: int = 10,
        use_fixed_stop: bool = True,
        use_trailing_stop: bool = True
    ):
        self.fixed_std_multiplier = fixed_std_multiplier
        self.trailing_std_multiplier = trailing_std_multiplier
        self.lookback_period = lookback_period
        self.use_fixed_stop = use_fixed_stop
        self.use_trailing_stop = use_trailing_stop
        
        # 存储各持仓的止损状态
        self.positions: Dict[str, StopLossState] = {}
    
    def calculate_std(self, prices: List[float]) -> float:
        """计算价格序列的标准差"""
        if len(prices) < 2:
            return 0.0
        return float(np.std(prices, ddof=1))
    
    def set_entry(
        self,
        vt_symbol: str,
        entry_price: float,
        recent_prices: List[float],
        is_long: bool = True
    ) -> StopLossState:
        """
        开仓时设置止损价格
        
        Args:
            vt_symbol: 合约代码
            entry_price: 入场价格
            recent_prices: 最近N天的收盘价列表
            is_long: 是否为多头仓位
            
        Returns:
            StopLossState: 止损状态对象
        """
        std = self.calculate_std(recent_prices)
        
        if is_long:
            fixed_stop = entry_price - self.fixed_std_multiplier * std
            trailing_stop = entry_price - self.trailing_std_multiplier * std
        else:
            fixed_stop = entry_price + self.fixed_std_multiplier * std
            trailing_stop = entry_price + self.trailing_std_multiplier * std
        
        state = StopLossState(
            vt_symbol=vt_symbol,
            entry_price=entry_price,
            fixed_stop_price=fixed_stop,
            trailing_stop_price=trailing_stop,
            highest_price=entry_price,
            is_long=is_long
        )
        
        self.positions[vt_symbol] = state
        return state
    
    def update_trailing_stop(
        self,
        vt_symbol: str,
        current_price: float,
        recent_prices: List[float]
    ) -> Optional[float]:
        """
        更新移动止损价格
        
        移动止损只会向有利方向移动：
        - 多头：价格创新高时，止损价上移
        - 空头：价格创新低时，止损价下移
        
        Args:
            vt_symbol: 合约代码
            current_price: 当前价格
            recent_prices: 最近N天的收盘价列表
            
        Returns:
            新的移动止损价格，如果没有该持仓则返回None
        """
        if vt_symbol not in self.positions:
            return None
        
        state = self.positions[vt_symbol]
        std = self.calculate_std(recent_prices)
        
        if state.is_long:
            # 多头：价格创新高时更新止损
            if current_price > state.highest_price:
                state.highest_price = current_price
                new_trailing_stop = current_price - self.trailing_std_multiplier * std
                # 移动止损只能上移
                if new_trailing_stop > state.trailing_stop_price:
                    state.trailing_stop_price = new_trailing_stop
        else:
            # 空头：价格创新低时更新止损
            if current_price < state.highest_price:
                state.highest_price = current_price
                new_trailing_stop = current_price + self.trailing_std_multiplier * std
                # 移动止损只能下移
                if new_trailing_stop < state.trailing_stop_price:
                    state.trailing_stop_price = new_trailing_stop
        
        return state.trailing_stop_price
    
    def should_stop_loss(self, vt_symbol: str, current_price: float) -> bool:
        """
        检查是否应该触发止损
        
        Args:
            vt_symbol: 合约代码
            current_price: 当前价格
            
        Returns:
            True表示应该止损，False表示继续持有
        """
        if vt_symbol not in self.positions:
            return False
        
        state = self.positions[vt_symbol]
        
        if state.is_long:
            # 多头：价格低于止损价则止损
            if self.use_fixed_stop and current_price <= state.fixed_stop_price:
                return True
            if self.use_trailing_stop and current_price <= state.trailing_stop_price:
                return True
        else:
            # 空头：价格高于止损价则止损
            if self.use_fixed_stop and current_price >= state.fixed_stop_price:
                return True
            if self.use_trailing_stop and current_price >= state.trailing_stop_price:
                return True
        
        return False
    
    def get_stop_reason(self, vt_symbol: str, current_price: float) -> Optional[str]:
        """
        获取止损触发原因
        
        Returns:
            "fixed" - 固定止损触发
            "trailing" - 移动止损触发
            None - 未触发止损
        """
        if vt_symbol not in self.positions:
            return None
        
        state = self.positions[vt_symbol]
        
        if state.is_long:
            if self.use_fixed_stop and current_price <= state.fixed_stop_price:
                return "fixed"
            if self.use_trailing_stop and current_price <= state.trailing_stop_price:
                return "trailing"
        else:
            if self.use_fixed_stop and current_price >= state.fixed_stop_price:
                return "fixed"
            if self.use_trailing_stop and current_price >= state.trailing_stop_price:
                return "trailing"
        
        return None
    
    def get_state(self, vt_symbol: str) -> Optional[StopLossState]:
        """获取指定合约的止损状态"""
        return self.positions.get(vt_symbol)
    
    def get_active_stop_price(self, vt_symbol: str) -> Optional[float]:
        """获取当前生效的止损价格"""
        state = self.positions.get(vt_symbol)
        if state:
            return state.get_active_stop_price()
        return None
    
    def remove_position(self, vt_symbol: str) -> None:
        """移除持仓的止损状态（平仓后调用）"""
        if vt_symbol in self.positions:
            del self.positions[vt_symbol]
    
    def clear_all(self) -> None:
        """清除所有止损状态"""
        self.positions.clear()
    
    def get_all_positions(self) -> Dict[str, StopLossState]:
        """获取所有持仓的止损状态"""
        return self.positions.copy()


# ============================================================================
# 便捷函数：可直接在策略中调用
# ============================================================================

def calculate_fixed_stop_loss(
    entry_price: float,
    recent_prices: List[float],
    std_multiplier: float = 2.0,
    is_long: bool = True
) -> float:
    """
    计算固定止损价格
    
    Args:
        entry_price: 入场价格
        recent_prices: 最近N天收盘价
        std_multiplier: 标准差倍数，默认2.0
        is_long: 是否多头
        
    Returns:
        止损价格
    """
    if len(recent_prices) < 2:
        # 数据不足，使用5%作为默认止损
        return entry_price * (0.95 if is_long else 1.05)
    
    std = float(np.std(recent_prices, ddof=1))
    
    if is_long:
        return entry_price - std_multiplier * std
    else:
        return entry_price + std_multiplier * std


def calculate_trailing_stop_loss(
    current_price: float,
    highest_price: float,
    recent_prices: List[float],
    current_stop: float,
    std_multiplier: float = 1.0,
    is_long: bool = True
) -> float:
    """
    计算移动止损价格
    
    Args:
        current_price: 当前价格
        highest_price: 持仓期间最高/最低价
        recent_prices: 最近N天收盘价
        current_stop: 当前止损价
        std_multiplier: 标准差倍数，默认1.0
        is_long: 是否多头
        
    Returns:
        新的止损价格
    """
    if len(recent_prices) < 2:
        return current_stop
    
    std = float(np.std(recent_prices, ddof=1))
    
    if is_long:
        if current_price > highest_price:
            new_stop = current_price - std_multiplier * std
            return max(new_stop, current_stop)
    else:
        if current_price < highest_price:
            new_stop = current_price + std_multiplier * std
            return min(new_stop, current_stop)
    
    return current_stop


# ---------------------------------------------------------------------------
# Strategy file versioning utilities
# These helpers keep a `.history/` directory next to the strategy file
# and maintain up to `max_versions` latest versions. They are file-system
# based so the web UI / backend can call them when a user clicks a strategy
# file to view history or recover an older version.
# ---------------------------------------------------------------------------


def _get_history_dir(file_path: Optional[str] = None) -> Path:
    """Return Path to the .history directory for the given strategy file."""
    if file_path:
        path = Path(file_path)
    else:
        path = Path(__file__).resolve()

    history_dir = path.parent / ".history"
    history_dir.mkdir(parents=True, exist_ok=True)
    return history_dir


def save_version(file_path: Optional[str] = None, max_versions: int = 5) -> str:
    """
    Save a snapshot of the current strategy file into the `.history/` folder.

    Returns the path (string) to the saved snapshot.
    """
    src = Path(file_path).resolve() if file_path else Path(__file__).resolve()
    history_dir = _get_history_dir(str(src))

    timestamp = datetime.datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
    dest_name = f"{src.stem}-{timestamp}.py"
    dest = history_dir / dest_name

    # copy atomically: write to temp then replace
    tmp = history_dir / (dest_name + ".tmp")
    with src.open("rb") as fsrc, tmp.open("wb") as fdst:
        shutil.copyfileobj(fsrc, fdst)
    os.replace(str(tmp), str(dest))

    # rotate old versions, keep newest `max_versions`
    versions = sorted(history_dir.glob(f"{src.stem}-*.py"), key=os.path.getmtime, reverse=True)
    for old in versions[max_versions:]:
        try:
            old.unlink()
        except Exception:
            pass

    return str(dest)


def list_versions(file_path: Optional[str] = None) -> List[Dict[str, str]]:
    """
    List available history versions for the given strategy file.

    Returns a list of dicts with keys: `name`, `path`, `mtime`, `size`.
    Sorted newest first.
    """
    src = Path(file_path).resolve() if file_path else Path(__file__).resolve()
    history_dir = _get_history_dir(str(src))

    out: List[Dict[str, str]] = []
    for p in sorted(history_dir.glob(f"{src.stem}-*.py"), key=os.path.getmtime, reverse=True):
        try:
            stat = p.stat()
            out.append({
                "name": p.name,
                "path": str(p),
                "mtime": datetime.datetime.utcfromtimestamp(stat.st_mtime).isoformat() + "Z",
                "size": str(stat.st_size),
            })
        except Exception:
            continue

    return out


def recover_version(version_name: str, file_path: Optional[str] = None, create_backup: bool = True) -> bool:
    """
    Recover a named version from `.history/` and overwrite the current strategy file.

    If `create_backup` is True, the current file is first saved into history (so it can be undone).

    Returns True on success.
    """
    src = Path(file_path).resolve() if file_path else Path(__file__).resolve()
    history_dir = _get_history_dir(str(src))
    candidate = history_dir / version_name
    if not candidate.exists():
        return False

    # backup current before overwrite
    if create_backup and src.exists():
        try:
            save_version(str(src), max_versions=10)
        except Exception:
            pass

    tmp = src.parent / (src.name + ".recover.tmp")
    with candidate.open("rb") as fsrc, tmp.open("wb") as fdst:
        shutil.copyfileobj(fsrc, fdst)
    os.replace(str(tmp), str(src))
    return True


# ============================================================================
# VNPy Strategy Example: Stop Loss Demo Strategy
# ============================================================================

class StopLossStrategy(CtaTemplate):
    """
    止损策略示例 - 演示如何使用StopLossManager
    
    这是一个简单的双均线策略，集成了固定止损和移动止损功能
    """
    
    author = "TraderMate"
    
    # 策略参数
    fast_window: int = 5
    slow_window: int = 20
    fixed_size: int = 1
    
    # 止损参数
    stop_loss_window: int = 10
    fixed_stop_multiplier: float = 2.0
    trailing_stop_multiplier: float = 1.0
    use_stop_loss: bool = True
    
    # 策略变量
    fast_ma: float = 0
    slow_ma: float = 0
    entry_price: float = 0
    fixed_stop: float = 0
    trailing_stop: float = 0
    
    parameters = [
        "fast_window", "slow_window", "fixed_size",
        "stop_loss_window", "fixed_stop_multiplier", "trailing_stop_multiplier", "use_stop_loss"
    ]
    
    variables = [
        "fast_ma", "slow_ma", "entry_price", "fixed_stop", "trailing_stop"
    ]
    
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
            use_trailing_stop=self.use_stop_loss
        )
    
    def on_init(self):
        """策略初始化"""
        self.write_log("止损策略初始化")
        self.load_bar(max(self.slow_window, self.stop_loss_window) + 10)
    
    def on_start(self):
        """策略启动"""
        self.write_log("止损策略启动")
    
    def on_stop(self):
        """策略停止"""
        self.write_log("止损策略停止")
    
    def on_tick(self, tick: TickData):
        """Tick数据更新"""
        self.bg.update_tick(tick)
    
    def on_bar(self, bar: BarData):
        """K线数据更新"""
        self.cancel_all()
        
        self.am.update_bar(bar)
        if not self.am.inited:
            return
        
        # 计算双均线
        self.fast_ma = self.am.sma(self.fast_window)
        self.slow_ma = self.am.sma(self.slow_window)
        
        # 获取最近收盘价用于止损计算
        recent_closes = list(self.am.close[-self.stop_loss_window:])
        
        # 如果有持仓，更新移动止损
        if self.pos != 0 and self.use_stop_loss:
            self.stop_loss_manager.update_trailing_stop(self.vt_symbol, bar.close_price, recent_closes)
            state = self.stop_loss_manager.get_state(self.vt_symbol)
            
            if state:
                self.fixed_stop = state.fixed_stop_price
                self.trailing_stop = state.trailing_stop_price
                
                # 检查是否触发止损
                if self.stop_loss_manager.should_stop_loss(self.vt_symbol, bar.close_price):
                    reason = self.stop_loss_manager.get_stop_reason(self.vt_symbol, bar.close_price)
                    self.write_log(f"触发止损: {reason}, 止损价={state.get_active_stop_price():.2f}")
                    
                    if self.pos > 0:
                        self.sell(bar.close_price * 0.99, abs(self.pos), False)
                    elif self.pos < 0:
                        self.cover(bar.close_price * 1.01, abs(self.pos), False)
                    
                    self.stop_loss_manager.remove_position(self.vt_symbol)
                    return
        
        # 交易逻辑：简单的双均线策略
        if self.pos == 0:
            # 金叉开多
            if self.fast_ma > self.slow_ma:
                self.buy(bar.close_price * 1.01, self.fixed_size, False)
                
                # 开仓后设置止损
                if self.use_stop_loss:
                    state = self.stop_loss_manager.set_entry(
                        self.vt_symbol,
                        bar.close_price,
                        recent_closes,
                        is_long=True
                    )
                    self.entry_price = bar.close_price
                    self.fixed_stop = state.fixed_stop_price
                    self.trailing_stop = state.trailing_stop_price
                    self.write_log(f"开多并设置止损: 固定={self.fixed_stop:.2f}, 移动={self.trailing_stop:.2f}")
        
        elif self.pos > 0:
            # 死叉平多
            if self.fast_ma < self.slow_ma:
                self.sell(bar.close_price * 0.99, abs(self.pos), False)
                self.stop_loss_manager.remove_position(self.vt_symbol)
    
    def on_order(self, order: OrderData):
        """订单回调"""
        pass
    
    def on_trade(self, trade: TradeData):
        """成交回调"""
        self.put_event()
    
    def on_stop_order(self, stop_order: StopOrder):
        """停止单回调"""
        pass
