"""VNPy live trading service — bridges QuantMate orders to vnpy gateways.

This service wraps vnpy's ``MainEngine`` and the CTP/XTP gateways so that
orders submitted through QuantMate's API with ``mode=live`` are forwarded
to a real broker connection, while paper-trading orders still go through the
existing simulated fill logic.
"""

from __future__ import annotations

import logging
import threading
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Gateway type enumeration
# ---------------------------------------------------------------------------


class GatewayType(str, Enum):
    CTP = "ctp"  # Futures (CFFEX / SHFE / DCE / CZCE / INE)
    XTP = "xtp"  # Equities (SSE / SZSE)
    SIMULATED = "sim"  # Simulated gateway for testing


# ---------------------------------------------------------------------------
# Data classes for event payloads
# ---------------------------------------------------------------------------


@dataclass
class OrderEvent:
    order_id: str
    symbol: str
    direction: str
    status: str
    filled_quantity: float = 0
    avg_fill_price: float = 0
    fee: float = 0
    timestamp: Optional[datetime] = None
    gateway: str = ""
    raw: Dict[str, Any] = field(default_factory=dict)


@dataclass
class TradeEvent:
    trade_id: str
    order_id: str
    symbol: str
    direction: str
    price: float
    volume: float
    fee: float = 0
    timestamp: Optional[datetime] = None


@dataclass
class PositionSnapshot:
    symbol: str
    direction: str
    volume: float
    frozen: float = 0
    avg_price: float = 0
    pnl: float = 0


@dataclass
class AccountSnapshot:
    balance: float
    available: float
    frozen: float = 0
    margin: float = 0


# ---------------------------------------------------------------------------
# VNPy Trading Service (singleton per process)
# ---------------------------------------------------------------------------


class VnpyTradingService:
    """Manage vnpy gateway connections and route live orders."""

    _instance: Optional["VnpyTradingService"] = None
    _lock = threading.Lock()

    def __new__(cls) -> "VnpyTradingService":
        with cls._lock:
            if cls._instance is None:
                cls._instance = super().__new__(cls)
                cls._instance._initialized = False
            return cls._instance

    def __init__(self) -> None:
        if self._initialized:
            return
        self._initialized = True

        self._connected_gateways: Dict[str, Any] = {}
        self._order_callbacks: List[Any] = []
        self._trade_callbacks: List[Any] = []
        self._main_engine: Any = None
        logger.info("[vnpy-trading] VnpyTradingService initialized")

    # ------------------------------------------------------------------
    # Gateway lifecycle
    # ------------------------------------------------------------------

    def connect_gateway(
        self,
        gateway_type: GatewayType,
        gateway_config: Dict[str, Any],
        gateway_name: Optional[str] = None,
    ) -> bool:
        """Connect to a broker gateway.

        Args:
            gateway_type: Type of gateway (ctp, xtp, sim).
            gateway_config: Connection credentials (address, user, password, etc.).
            gateway_name: Optional display name; defaults to gateway_type value.

        Returns:
            True if connection was initiated successfully.
        """
        name = gateway_name or gateway_type.value
        try:
            if gateway_type == GatewayType.SIMULATED:
                self._connected_gateways[name] = {
                    "type": gateway_type,
                    "connected": True,
                    "config": gateway_config,
                }
                logger.info("[vnpy-trading] Simulated gateway '%s' connected", name)
                return True

            # Lazy-import vnpy components only when real gateway is requested
            from vnpy.event import EventEngine
            from vnpy.trader.engine import MainEngine

            if self._main_engine is None:
                event_engine = EventEngine()
                self._main_engine = MainEngine(event_engine)
                self._register_event_handlers()

            gateway_cls = self._resolve_gateway_class(gateway_type)
            self._main_engine.add_gateway(gateway_cls)
            self._main_engine.connect(gateway_config, gateway_cls.default_name)

            self._connected_gateways[name] = {
                "type": gateway_type,
                "connected": True,
                "config": {k: "***" for k in gateway_config},  # mask credentials
                "gateway_class_name": gateway_cls.default_name,
            }
            logger.info("[vnpy-trading] Gateway '%s' (%s) connection initiated", name, gateway_type.value)
            return True

        except Exception:
            logger.exception("[vnpy-trading] Failed to connect gateway '%s'", name)
            return False

    def disconnect_gateway(self, gateway_name: str) -> bool:
        info = self._connected_gateways.pop(gateway_name, None)
        if info is None:
            return False
        if info.get("type") != GatewayType.SIMULATED and self._main_engine:
            try:
                info.get("gateway_class_name", gateway_name)
                self._main_engine.close()
            except Exception:
                logger.exception("[vnpy-trading] Error disconnecting gateway '%s'", gateway_name)
        logger.info("[vnpy-trading] Gateway '%s' disconnected", gateway_name)
        return True

    def list_gateways(self) -> List[Dict[str, Any]]:
        return [
            {"name": name, "type": info["type"].value, "connected": info.get("connected", False)}
            for name, info in self._connected_gateways.items()
        ]

    # ------------------------------------------------------------------
    # Order management
    # ------------------------------------------------------------------

    def send_order(
        self,
        symbol: str,
        direction: str,
        order_type: str,
        quantity: float,
        price: float = 0,
        gateway_name: Optional[str] = None,
    ) -> Optional[str]:
        """Submit an order through a connected gateway.

        Returns the vnpy order id (vt_orderid) or None on failure.
        """
        gw_name = gateway_name or self._default_gateway_name()
        if gw_name is None:
            logger.error("[vnpy-trading] No gateway connected")
            return None

        gw_info = self._connected_gateways.get(gw_name)
        if gw_info is None or not gw_info.get("connected"):
            logger.error("[vnpy-trading] Gateway '%s' not connected", gw_name)
            return None

        # Simulated gateway — return synthetic order id
        if gw_info.get("type") == GatewayType.SIMULATED:
            oid = f"SIM-{datetime.utcnow().strftime('%Y%m%d%H%M%S%f')}"
            logger.info(
                "[vnpy-trading][sim] Order %s: %s %s %s qty=%s px=%s",
                oid,
                symbol,
                direction,
                order_type,
                quantity,
                price,
            )
            return oid

        if self._main_engine is None:
            logger.error("[vnpy-trading] MainEngine not initialized")
            return None

        try:
            from vnpy.trader.constant import Direction, OrderType, Offset, Exchange
            from vnpy.trader.object import OrderRequest

            code, exchange_str = symbol.rsplit(".", 1) if "." in symbol else (symbol, "")
            exchange = Exchange(exchange_str)

            dir_map = {"buy": Direction.LONG, "sell": Direction.SHORT}
            type_map = {
                "market": OrderType.MARKET,
                "limit": OrderType.LIMIT,
                "stop": OrderType.STOP,
            }

            req = OrderRequest(
                symbol=code,
                exchange=exchange,
                direction=dir_map.get(direction, Direction.LONG),
                type=type_map.get(order_type, OrderType.LIMIT),
                volume=float(quantity),
                price=float(price),
                offset=Offset.NONE,
            )
            vt_orderid = self._main_engine.send_order(req, gw_info.get("gateway_class_name", gw_name))
            logger.info("[vnpy-trading] Order sent: %s", vt_orderid)
            return vt_orderid

        except Exception:
            logger.exception("[vnpy-trading] Failed to send order")
            return None

    def cancel_order(self, vt_orderid: str, gateway_name: Optional[str] = None) -> bool:
        gw_name = gateway_name or self._default_gateway_name()
        if self._main_engine is None or gw_name is None:
            return False
        try:
            from vnpy.trader.object import CancelRequest

            req = CancelRequest(orderid=vt_orderid, symbol="", exchange=None)  # type: ignore[arg-type]
            self._main_engine.cancel_order(req, gw_name)
            return True
        except Exception:
            logger.exception("[vnpy-trading] Failed to cancel order %s", vt_orderid)
            return False

    # ------------------------------------------------------------------
    # Queries
    # ------------------------------------------------------------------

    def query_positions(self, gateway_name: Optional[str] = None) -> List[PositionSnapshot]:
        if self._main_engine is None:
            return []
        try:
            positions = self._main_engine.get_all_positions()
            return [
                PositionSnapshot(
                    symbol=f"{p.symbol}.{p.exchange.value}",
                    direction=p.direction.value,
                    volume=p.volume,
                    frozen=p.frozen,
                    avg_price=getattr(p, "price", 0),
                    pnl=getattr(p, "pnl", 0),
                )
                for p in positions
            ]
        except Exception:
            logger.exception("[vnpy-trading] Failed to query positions")
            return []

    def query_account(self, gateway_name: Optional[str] = None) -> Optional[AccountSnapshot]:
        if self._main_engine is None:
            return None
        try:
            accounts = self._main_engine.get_all_accounts()
            if not accounts:
                return None
            a = accounts[0]
            return AccountSnapshot(
                balance=a.balance,
                available=a.available,
                frozen=getattr(a, "frozen", 0),
                margin=getattr(a, "margin", 0),
            )
        except Exception:
            logger.exception("[vnpy-trading] Failed to query account")
            return None

    # ------------------------------------------------------------------
    # Event callbacks
    # ------------------------------------------------------------------

    def on_order(self, callback) -> None:
        self._order_callbacks.append(callback)

    def on_trade(self, callback) -> None:
        self._trade_callbacks.append(callback)

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _default_gateway_name(self) -> Optional[str]:
        if not self._connected_gateways:
            return None
        return next(iter(self._connected_gateways))

    @staticmethod
    def _resolve_gateway_class(gateway_type: GatewayType):
        """Dynamically import the vnpy gateway class."""
        if gateway_type == GatewayType.CTP:
            from vnpy_ctp import CtpGateway

            return CtpGateway
        elif gateway_type == GatewayType.XTP:
            from vnpy_xtp import XtpGateway

            return XtpGateway
        else:
            raise ValueError(f"Unsupported gateway type: {gateway_type}")

    def _register_event_handlers(self) -> None:
        """Register vnpy event handlers to relay order/trade updates."""
        if self._main_engine is None:
            return

        from vnpy.event import EVENT_ORDER, EVENT_TRADE

        def _on_vnpy_order(event):
            data = event.data
            evt = OrderEvent(
                order_id=data.vt_orderid,
                symbol=f"{data.symbol}.{data.exchange.value}",
                direction=data.direction.value,
                status=data.status.value,
                filled_quantity=data.traded,
                timestamp=data.datetime,
                gateway=data.gateway_name,
            )
            for cb in self._order_callbacks:
                try:
                    cb(evt)
                except Exception:
                    logger.exception("[vnpy-trading] order callback error")

        def _on_vnpy_trade(event):
            data = event.data
            evt = TradeEvent(
                trade_id=data.vt_tradeid,
                order_id=data.vt_orderid,
                symbol=f"{data.symbol}.{data.exchange.value}",
                direction=data.direction.value,
                price=data.price,
                volume=data.volume,
                timestamp=data.datetime,
            )
            for cb in self._trade_callbacks:
                try:
                    cb(evt)
                except Exception:
                    logger.exception("[vnpy-trading] trade callback error")

        self._main_engine.event_engine.register(EVENT_ORDER, _on_vnpy_order)
        self._main_engine.event_engine.register(EVENT_TRADE, _on_vnpy_trade)
        logger.info("[vnpy-trading] Event handlers registered")
