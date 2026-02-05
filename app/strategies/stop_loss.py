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

from dataclasses import dataclass, field
from typing import Dict, List, Optional
import numpy as np


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
