"""Paper Portfolio Executor.

Runs vn.py portfolio strategies against QuantMate's paper matching engine.
Each deployment polls snapshot quotes for its symbol set and feeds synthetic
BarData dictionaries into StrategyTemplate.on_bars.
"""

from __future__ import annotations

import logging
import threading
from datetime import datetime
from typing import Any, Dict, Optional

from sqlalchemy import text

from app.infrastructure.db.connections import connection
from app.domains.trading.paper_execution_ledger import PaperExecutionLedger
from app.domains.trading.paper_strategy_executor import (
    _POLL_INTERVAL,
    _build_runtime_checkpoint,
    _get_quote_price,
    _normalize_vt_symbols,
    _split_vt_symbol,
    PaperStrategyExecutor,
)

logger = logging.getLogger(__name__)


class _PaperPortfolioEngine:
    """Minimal strategy_engine stub for vn.py portfolio strategies."""

    def __init__(
        self,
        executor: "PaperPortfolioExecutor",
        deployment_id: int,
        paper_account_id: int,
        user_id: int,
        vt_symbols: list[str],
        execution_mode: str,
        gateway: Any = None,
    ) -> None:
        self.executor = executor
        self.deployment_id = deployment_id
        self.paper_account_id = paper_account_id
        self.user_id = user_id
        self.vt_symbols = vt_symbols
        self.execution_mode = execution_mode
        self.gateway = gateway
        self._order_counter = 0
        self._ledger = PaperExecutionLedger()

    def send_order(
        self,
        strategy: Any,
        vt_symbol: str,
        direction,
        offset,
        price: float,
        volume: float,
        lock: bool = False,
        net: bool = False,
    ) -> list[str]:
        from vnpy.trader.constant import Direction as VDirection
        from app.domains.trading.paper_gateway import PaperGatewayOrderRequest

        dir_str = "buy" if direction == VDirection.LONG else "sell"
        qty = int(volume)
        gateway_order_id: Optional[str] = None

        if self.gateway is not None:
            order_state = self.gateway.submit_order(
                PaperGatewayOrderRequest(
                    vt_symbol=vt_symbol,
                    direction=dir_str,
                    order_type="market",
                    volume=qty,
                    price=price,
                    metadata={
                        "strategy_name": getattr(strategy, "strategy_name", ""),
                        "lock": lock,
                        "net": net,
                    },
                )
            )
            gateway_order_id = order_state.order_id

        if self.execution_mode == "semi_auto":
            self._write_signal(vt_symbol, dir_str, qty, price, reason=f"Portfolio signal: {strategy.strategy_name}")
            return []

        vt_orderid = gateway_order_id or f"PAPER.{self.deployment_id}.{self._order_counter}"
        if gateway_order_id is None:
            self._order_counter += 1

        self._execute_order(
            strategy=strategy,
            vt_symbol=vt_symbol,
            direction=dir_str,
            quantity=qty,
            price=price,
            vt_orderid=vt_orderid,
            offset=offset,
        )
        return [vt_orderid]

    def cancel_order(self, strategy: Any, vt_orderid: str) -> None:
        logger.info("[paper-portfolio] cancel_order ignored for %s", vt_orderid)

    def load_bars(self, strategy: Any, days: int, interval) -> None:
        logger.info("[paper-portfolio] load_bars is currently a no-op for deployment %d", self.deployment_id)

    def get_pricetick(self, strategy: Any, vt_symbol: str) -> float:
        return 0.01

    def get_size(self, strategy: Any, vt_symbol: str) -> int:
        return 1

    def sync_strategy_data(self, strategy: Any) -> None:
        self._ledger.write_checkpoint(
            deployment_id=self.deployment_id,
            runtime_mode="portfolio_strategy_bridge",
            strategy_kind="portfolio",
            checkpoint=_build_runtime_checkpoint(
                deployment_id=self.deployment_id,
                vt_symbols=self.vt_symbols,
                gateway=self.gateway,
                strategy=strategy,
            ),
        )

    def put_strategy_event(self, strategy: Any) -> None:
        pass

    def write_log(self, msg: str, strategy: Any = None) -> None:
        logger.info("[paper-portfolio][%d] %s", self.deployment_id, msg)

    def send_email(self, msg: str, strategy: Any = None) -> None:
        pass

    def get_engine_type(self):
        from vnpy_portfoliostrategy.base import EngineType

        return EngineType.LIVE

    def _execute_order(
        self,
        *,
        strategy: Any,
        vt_symbol: str,
        direction: str,
        quantity: int,
        price: float,
        vt_orderid: str,
        offset,
    ) -> None:
        from datetime import date

        from vnpy.trader.constant import Direction as VDirection
        from vnpy.trader.constant import Offset as VOffset
        from vnpy.trader.constant import Status
        from vnpy.trader.object import OrderData, TradeData

        from app.domains.market.realtime_quote_service import RealtimeQuoteService
        from app.domains.trading.dao.order_dao import OrderDao
        from app.domains.trading.matching_engine import try_fill_market_order
        from app.domains.trading.paper_account_service import PaperAccountService

        symbol, exchange = _split_vt_symbol(vt_symbol)
        market = self._get_market()
        quote_svc = RealtimeQuoteService()
        try:
            quote = quote_svc.get_quote(symbol, market)
            last_price = _get_quote_price(quote, price)
        except Exception:
            last_price = price

        if last_price <= 0:
            logger.warning("[paper-portfolio] No price for %s, skip order", vt_symbol)
            if self.gateway is not None:
                self.gateway.update_order_status(vt_orderid, "rejected")
            return

        fill = try_fill_market_order(
            direction=direction,
            quantity=quantity,
            market=market,
            last_price=last_price,
        )
        if not fill.filled:
            logger.warning("[paper-portfolio] Fill failed: %s", fill.reason)
            if self.gateway is not None:
                self.gateway.update_order_status(vt_orderid, "rejected")
            return

        acct_svc = PaperAccountService()
        dao = OrderDao()
        today_str = date.today().isoformat()

        if direction == "buy":
            total_cost = fill.fill_price * fill.fill_quantity + fill.fee.total
            ok = acct_svc.freeze_funds(self.paper_account_id, total_cost)
            if not ok:
                logger.warning("[paper-portfolio] Insufficient funds for buy")
                return
            acct_svc.settle_buy(self.paper_account_id, total_cost, total_cost)
        else:
            proceeds = fill.fill_price * fill.fill_quantity - fill.fee.total
            acct_svc.settle_sell(self.paper_account_id, proceeds)

        db_order_id = dao.create(
            user_id=self.user_id,
            symbol=symbol,
            direction=direction,
            order_type="market",
            quantity=quantity,
            price=fill.fill_price,
            mode="paper",
            paper_account_id=self.paper_account_id,
            paper_deployment_id=self.deployment_id,
            buy_date=today_str if direction == "buy" else None,
            strategy_id=self._get_strategy_id(),
        )
        dao.update_status(db_order_id, "filled", filled_quantity=fill.fill_quantity, avg_fill_price=fill.fill_price, fee=fill.fee.total)
        dao.insert_trade(db_order_id, fill.fill_quantity, fill.fill_price, fill.fee.total)
        self._ledger.record_fill(
            user_id=self.user_id,
            paper_account_id=self.paper_account_id,
            deployment_id=self.deployment_id,
            order_id=db_order_id,
            symbol=symbol,
            direction=direction,
            quantity=fill.fill_quantity,
            price=fill.fill_price,
            fee=fill.fee.total,
            payload={"gateway_order_id": vt_orderid},
        )
        if self.gateway is not None:
            self.gateway.update_order_status(vt_orderid, "filled")

        gateway_name, orderid = vt_orderid.rsplit(".", 1)
        vn_direction = VDirection.LONG if direction == "buy" else VDirection.SHORT
        vn_offset = offset if offset is not None else (VOffset.OPEN if direction == "buy" else VOffset.CLOSE)
        order = OrderData(
            gateway_name=gateway_name,
            symbol=symbol,
            exchange=exchange,
            orderid=orderid,
            direction=vn_direction,
            offset=vn_offset,
            price=fill.fill_price,
            volume=quantity,
            traded=fill.fill_quantity,
            status=Status.ALLTRADED,
            datetime=datetime.utcnow(),
        )
        trade = TradeData(
            gateway_name=gateway_name,
            symbol=symbol,
            exchange=exchange,
            orderid=orderid,
            tradeid=f"trade.{orderid}",
            direction=vn_direction,
            offset=vn_offset,
            price=fill.fill_price,
            volume=fill.fill_quantity,
            datetime=datetime.utcnow(),
        )

        if hasattr(strategy, "update_order"):
            strategy.update_order(order)
        if hasattr(strategy, "update_trade"):
            strategy.update_trade(trade)
        if hasattr(strategy, "on_order"):
            strategy.on_order(order)
        if hasattr(strategy, "on_trade"):
            strategy.on_trade(trade)

        logger.info("[paper-portfolio] Order filled: %s %s %d @ %.4f", direction, vt_symbol, quantity, fill.fill_price)

    def _write_signal(self, vt_symbol: str, direction: str, quantity: int, price: float, reason: str = "") -> None:
        with connection("quantmate") as conn:
            conn.execute(
                text("""
                    INSERT INTO paper_signals (user_id, paper_account_id, deployment_id, symbol,
                                               direction, quantity, suggested_price, reason, status)
                    VALUES (:uid, :paid, :did, :sym, :dir, :qty, :price, :reason, 'pending')
                """),
                {
                    "uid": self.user_id,
                    "paid": self.paper_account_id,
                    "did": self.deployment_id,
                    "sym": vt_symbol.split(".")[0] if "." in vt_symbol else vt_symbol,
                    "dir": direction,
                    "qty": quantity,
                    "price": price if price > 0 else None,
                    "reason": reason,
                },
            )
            conn.commit()

    def _get_market(self) -> str:
        with connection("quantmate") as conn:
            row = conn.execute(
                text("SELECT market FROM paper_accounts WHERE id = :aid"),
                {"aid": self.paper_account_id},
            ).fetchone()
        return row.market if row else "CN"

    def _get_strategy_id(self) -> Optional[int]:
        with connection("quantmate") as conn:
            row = conn.execute(
                text("SELECT strategy_id FROM paper_deployments WHERE id = :did"),
                {"did": self.deployment_id},
            ).fetchone()
        return row.strategy_id if row else None


class PaperPortfolioExecutor:
    """Manages running portfolio paper-strategy threads."""

    _instance: Optional["PaperPortfolioExecutor"] = None
    _lock = threading.Lock()

    def __new__(cls) -> "PaperPortfolioExecutor":
        with cls._lock:
            if cls._instance is None:
                cls._instance = super().__new__(cls)
                cls._instance._initialized = False
            return cls._instance

    def __init__(self) -> None:
        if self._initialized:
            return
        self._initialized = True
        self._threads: Dict[int, threading.Thread] = {}
        self._stop_events: Dict[int, threading.Event] = {}
        self._gateways: Dict[int, Any] = {}

    def start_deployment(
        self,
        deployment_id: int,
        paper_account_id: int,
        user_id: int,
        strategy_class_name: str,
        vt_symbol: str,
        parameters: Dict[str, Any],
        execution_mode: str = "auto",
        strategy_id: Optional[int] = None,
        gateway: Any = None,
    ) -> Dict[str, Any]:
        if deployment_id in self._threads and self._threads[deployment_id].is_alive():
            return {"success": False, "error": "Deployment already running"}

        stop_event = threading.Event()
        self._stop_events[deployment_id] = stop_event
        thread = threading.Thread(
            target=self._run_strategy,
            args=(deployment_id, paper_account_id, user_id, strategy_class_name, vt_symbol, parameters, execution_mode, strategy_id, stop_event, gateway),
            daemon=True,
            name=f"paper-portfolio-{deployment_id}",
        )
        self._threads[deployment_id] = thread
        if gateway is not None:
            self._gateways[deployment_id] = gateway
        thread.start()
        return {"success": True, "deployment_id": deployment_id}

    def stop_deployment(self, deployment_id: int) -> bool:
        event = self._stop_events.get(deployment_id)
        if not event:
            return False
        event.set()
        return True

    def is_running(self, deployment_id: int) -> bool:
        thread = self._threads.get(deployment_id)
        return thread is not None and thread.is_alive()

    def _run_strategy(
        self,
        deployment_id: int,
        paper_account_id: int,
        user_id: int,
        strategy_class_name: str,
        vt_symbol: str,
        parameters: Dict[str, Any],
        execution_mode: str,
        strategy_id: Optional[int],
        stop_event: threading.Event,
        gateway: Any,
    ) -> None:
        try:
            from app.api.services.strategy_service import compile_strategy
            from app.domains.backtests.dao.strategy_source_dao import StrategySourceDao
            from app.domains.market.realtime_quote_service import RealtimeQuoteService

            source_dao = StrategySourceDao()
            if strategy_id is not None:
                db_code, db_class, _sv = source_dao.get_strategy_source_for_user(strategy_id, user_id)
                strategy_cls = compile_strategy(db_code, db_class or strategy_class_name)
            else:
                db_code = source_dao.get_strategy_code_by_class_name(strategy_class_name)
                strategy_cls = compile_strategy(db_code, strategy_class_name)

            vt_symbols = _normalize_vt_symbols(vt_symbol)
            engine = _PaperPortfolioEngine(
                executor=self,
                deployment_id=deployment_id,
                paper_account_id=paper_account_id,
                user_id=user_id,
                vt_symbols=vt_symbols,
                execution_mode=execution_mode,
                gateway=gateway,
            )

            ts = datetime.utcnow().strftime("%Y%m%d%H%M%S")
            strategy_name = f"{strategy_class_name}_paper_{ts}"
            strategy_instance = strategy_cls(engine, strategy_name, vt_symbols, parameters)
            strategy_instance.on_init()
            strategy_instance.inited = True
            strategy_instance.trading = True
            if hasattr(strategy_instance, "on_start"):
                strategy_instance.on_start()

            quote_svc = RealtimeQuoteService()
            market = engine._get_market()
            while not stop_event.is_set():
                try:
                    bars = {}
                    for current_vt_symbol in vt_symbols:
                        symbol = current_vt_symbol.split(".")[0] if "." in current_vt_symbol else current_vt_symbol
                        quote = quote_svc.get_quote(symbol, market)
                        if gateway is not None:
                            gateway.publish_tick(current_vt_symbol, quote)
                        bar = PaperStrategyExecutor._quote_to_bar(quote, current_vt_symbol)
                        if bar is not None:
                            bars[current_vt_symbol] = bar
                    if bars:
                        strategy_instance.on_bars(bars)
                    PaperExecutionLedger().write_checkpoint(
                        deployment_id=deployment_id,
                        runtime_mode="portfolio_strategy_bridge",
                        strategy_kind="portfolio",
                        checkpoint=_build_runtime_checkpoint(
                            deployment_id=deployment_id,
                            vt_symbols=vt_symbols,
                            gateway=gateway,
                            strategy=strategy_instance,
                        ),
                    )
                except Exception:
                    logger.debug("[paper-portfolio] Quote/bar error for deployment %s", deployment_id, exc_info=True)

                stop_event.wait(_POLL_INTERVAL)

            if hasattr(strategy_instance, "on_stop"):
                strategy_instance.on_stop()
            PaperExecutionLedger().write_checkpoint(
                deployment_id=deployment_id,
                runtime_mode="portfolio_strategy_bridge",
                strategy_kind="portfolio",
                checkpoint=_build_runtime_checkpoint(
                    deployment_id=deployment_id,
                    vt_symbols=vt_symbols,
                    gateway=gateway,
                    strategy=strategy_instance,
                ),
            )
        except Exception:
            logger.exception("[paper-portfolio] Deployment %d crashed", deployment_id)
        finally:
            self._threads.pop(deployment_id, None)
            self._stop_events.pop(deployment_id, None)
            self._gateways.pop(deployment_id, None)
            try:
                with connection("quantmate") as conn:
                    conn.execute(
                        text("UPDATE paper_deployments SET status='stopped', stopped_at=NOW() WHERE id=:did AND status='running'"),
                        {"did": deployment_id},
                    )
                    conn.commit()
            except Exception:
                pass