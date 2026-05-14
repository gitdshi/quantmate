"""Paper gateway skeleton for future vn.py-native paper trading.

This module intentionally starts small. In the current phase it provides a
server-side state container for simulated orders and latest tick payloads.
Later phases can grow this into a full vn.py gateway adapter.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
import threading
from typing import Any, Dict, Optional


@dataclass(slots=True)
class PaperGatewayOrderRequest:
    vt_symbol: str
    direction: str
    order_type: str
    volume: float
    price: float = 0.0
    stop: bool = False
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass(slots=True)
class PaperGatewayOrderState:
    order_id: str
    request: PaperGatewayOrderRequest
    status: str = "created"
    created_at: datetime = field(default_factory=datetime.utcnow)
    updated_at: datetime = field(default_factory=datetime.utcnow)


@dataclass(slots=True)
class PaperGatewaySnapshot:
    gateway_name: str
    order_count: int
    tick_count: int
    last_tick_at: Optional[datetime]


class PaperGateway:
    """Minimal in-memory paper gateway state container.

    The goal in this phase is to create a stable owning abstraction for order
    and quote state before implementing full vn.py gateway semantics.
    """

    def __init__(self, gateway_name: str) -> None:
        self.gateway_name = gateway_name
        self._lock = threading.Lock()
        self._order_counter = 0
        self._orders: dict[str, PaperGatewayOrderState] = {}
        self._last_ticks: dict[str, dict[str, Any]] = {}
        self._last_tick_at: Optional[datetime] = None

    def submit_order(self, request: PaperGatewayOrderRequest) -> PaperGatewayOrderState:
        with self._lock:
            order_id = f"{self.gateway_name}.{self._order_counter}"
            self._order_counter += 1
            state = PaperGatewayOrderState(order_id=order_id, request=request)
            self._orders[order_id] = state
            return state

    def cancel_order(self, order_id: str) -> bool:
        with self._lock:
            state = self._orders.get(order_id)
            if state is None or state.status in {"cancelled", "filled"}:
                return False
            state.status = "cancelled"
            state.updated_at = datetime.utcnow()
            return True

    def update_order_status(self, order_id: str, status: str) -> bool:
        with self._lock:
            state = self._orders.get(order_id)
            if state is None:
                return False
            state.status = status
            state.updated_at = datetime.utcnow()
            return True

    def publish_tick(self, vt_symbol: str, payload: Dict[str, Any]) -> None:
        with self._lock:
            self._last_ticks[vt_symbol] = dict(payload)
            self._last_tick_at = datetime.utcnow()

    def get_last_tick(self, vt_symbol: str) -> Optional[Dict[str, Any]]:
        with self._lock:
            tick = self._last_ticks.get(vt_symbol)
            return dict(tick) if tick is not None else None

    def snapshot(self) -> PaperGatewaySnapshot:
        with self._lock:
            return PaperGatewaySnapshot(
                gateway_name=self.gateway_name,
                order_count=len(self._orders),
                tick_count=len(self._last_ticks),
                last_tick_at=self._last_tick_at,
            )