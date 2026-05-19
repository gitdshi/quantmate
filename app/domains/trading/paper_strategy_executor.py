"""Paper Strategy Executor — runs CTA strategies in a sandbox for paper trading.

Instead of routing orders to a vnpy gateway, orders go through the matching engine
with virtual paper accounts. Each deployment runs in its own thread consuming
realtime quotes converted to BarData.
"""

from __future__ import annotations

import logging
import threading
from datetime import date, datetime, timedelta
from typing import Any, Dict, Optional

from sqlalchemy import text

from app.domains.trading.paper_execution_ledger import PaperExecutionLedger
from app.infrastructure.db.connections import connection

logger = logging.getLogger(__name__)

# How often to poll for new bars (seconds)
_POLL_INTERVAL = 5


def _normalize_vt_symbols(vt_symbol: str | list[str]) -> list[str]:
    if isinstance(vt_symbol, list):
        return [item.strip() for item in vt_symbol if item and item.strip()]
    return [item.strip() for item in vt_symbol.split(",") if item and item.strip()]


def _split_vt_symbol(vt_symbol: str):
    from vnpy.trader.constant import Exchange

    parts = vt_symbol.split(".")
    symbol = parts[0]
    exchange_str = parts[1] if len(parts) > 1 else "SSE"
    try:
        exchange = Exchange(exchange_str)
    except ValueError:
        exchange = Exchange.SSE
    return symbol, exchange


def _get_quote_price(quote: dict, fallback: float = 0.0) -> float:
    return float(quote.get("last_price") or quote.get("price") or quote.get("current") or fallback or 0)


def _build_runtime_checkpoint(*, deployment_id: int, vt_symbols: list[str], gateway: Any, strategy: Any = None) -> Dict[str, Any]:
    checkpoint = {
        "deployment_id": deployment_id,
        "vt_symbols": vt_symbols,
        "gateway": gateway.snapshot().__dict__ if gateway is not None else None,
        "captured_at": datetime.utcnow().isoformat(),
    }
    if strategy is not None:
        checkpoint["strategy_name"] = getattr(strategy, "strategy_name", "")
        checkpoint["inited"] = getattr(strategy, "inited", False)
        checkpoint["trading"] = getattr(strategy, "trading", False)
        checkpoint["pos"] = getattr(strategy, "pos", None)
    return checkpoint


class _PaperCtaEngine:
    """Minimal CTA-engine stub that intercepts order calls from a CtaTemplate
    and routes them into the paper-trading matching engine."""

    def __init__(
        self,
        executor: "PaperStrategyExecutor",
        deployment_id: int,
        paper_account_id: int,
        user_id: int,
        vt_symbol: str,
        execution_mode: str,
        gateway: Any = None,
    ) -> None:
        self.executor = executor
        self.deployment_id = deployment_id
        self.paper_account_id = paper_account_id
        self.user_id = user_id
        self.vt_symbol = vt_symbol
        self.execution_mode = execution_mode
        self.gateway = gateway
        self._order_counter = 0
        self._ledger = PaperExecutionLedger()

    def _get_gateway(self):
        return getattr(self, "gateway", None)

    def _get_ledger(self) -> PaperExecutionLedger:
        ledger = getattr(self, "_ledger", None)
        if ledger is None:
            ledger = PaperExecutionLedger()
            self._ledger = ledger
        return ledger

    # -- Methods invoked by CtaTemplate ---------------------

    def send_order(
        self,
        strategy: Any,
        direction=None,
        offset=None,
        price: float = 0.0,
        volume: float = 0.0,
        stop: bool = False,
        lock: bool = False,
        net: bool = False,
    ) -> list[str]:
        """Intercept order from strategy and route to paper trading."""
        from vnpy.trader.constant import Direction as VDirection
        from app.domains.trading.paper_gateway import PaperGatewayOrderRequest

        dir_str = "buy" if direction == VDirection.LONG else "sell"
        qty = int(volume)
        gateway_order_id: Optional[str] = None

        if self.gateway is not None:
            order_state = self.gateway.submit_order(
                PaperGatewayOrderRequest(
                    vt_symbol=self.vt_symbol,
                    direction=dir_str,
                    order_type="stop" if stop else "market",
                    volume=qty,
                    price=price,
                    stop=stop,
                    metadata={"strategy_name": getattr(strategy, "strategy_name", "")},
                )
            )
            gateway_order_id = order_state.order_id

        if self.execution_mode == "semi_auto":
            self._write_signal(dir_str, qty, price, reason=f"Strategy signal: {strategy.strategy_name}")
            return []

        # Auto mode — execute through matching engine immediately
        vt_id = gateway_order_id or f"paper.{self.deployment_id}.{self._order_counter}"
        self._execute_order(dir_str, qty, price, stop=stop, strategy=strategy, order_id=vt_id)
        if gateway_order_id is None:
            self._order_counter += 1
        return [vt_id]

    def cancel_order(self, strategy: Any, vt_orderid: str) -> None:
        logger.info("[paper-engine] cancel_order ignored for %s", vt_orderid)

    def cancel_all(self, strategy: Any) -> None:
        logger.info("[paper-engine] cancel_all ignored for deployment %d", self.deployment_id)

    def write_log(self, msg: str, strategy: Any = None) -> None:
        logger.info("[paper-engine][%d] %s", self.deployment_id, msg)

    def get_engine_type(self):
        from vnpy_ctastrategy.engine import EngineType
        return EngineType.LIVE

    def get_pricetick(self, vt_symbol: str) -> float:
        return 0.01

    def get_size(self, strategy: Any) -> int:
        return 1

    def load_bar(self, vt_symbol: str, days: int, interval, callback, use_database: bool = False) -> None:
        from app.domains.market.service import MarketService

        bars = []
        history_bars: list[dict[str, Any]] = []
        try:
            lookback_days = max(int(days or 0), 1)
            end_date = date.today()
            start_date = end_date - timedelta(days=max(lookback_days * 3, lookback_days + 5))
            history_bars = MarketService().get_history(vt_symbol, start_date, end_date)
        except Exception:
            logger.debug("[paper-engine] load_bar history fallback for %s", vt_symbol, exc_info=True)

        for history_bar in history_bars[-max(int(days or 0), 1):]:
            bar = PaperStrategyExecutor._history_to_bar(history_bar, vt_symbol)
            if bar is not None:
                bars.append(bar)

        if bars:
            return bars

        gateway = self._get_gateway()
        quote = gateway.get_last_tick(vt_symbol) if gateway is not None else None
        if quote is None:
            return []
        bar = PaperStrategyExecutor._quote_to_bar(quote, vt_symbol)
        if bar is not None:
            return [bar]
        return []

    def load_tick(self, vt_symbol: str, days: int, callback) -> None:
        gateway = self._get_gateway()
        quote = gateway.get_last_tick(vt_symbol) if gateway is not None else None
        if quote is None:
            return []
        tick = PaperStrategyExecutor._quote_to_tick(quote, vt_symbol)
        if tick is not None:
            return [tick]
        return []

    def sync_strategy_data(self, strategy: Any) -> None:
        gateway = self._get_gateway()
        checkpoint = _build_runtime_checkpoint(
            deployment_id=self.deployment_id,
            vt_symbols=[self.vt_symbol],
            gateway=gateway,
            strategy=strategy,
        )
        self._get_ledger().write_checkpoint(
            deployment_id=self.deployment_id,
            runtime_mode="native_cta_runtime",
            strategy_kind="cta",
            checkpoint=checkpoint,
        )

    def put_event(self) -> None:
        pass

    def put_strategy_event(self, strategy: Any) -> None:
        self.put_event()

    def send_email(self, msg: str, strategy: Any = None) -> None:
        pass

    # -- Internal helpers -----------------------------------

    def _execute_order(
        self,
        direction: str,
        quantity: int,
        price: float,
        stop: bool = False,
        strategy: Any = None,
        order_id: Optional[str] = None,
    ) -> None:
        """Execute order via matching engine + paper account settlement."""
        from app.domains.trading.matching_engine import try_fill_market_order
        from app.domains.trading.paper_account_service import PaperAccountService
        from app.domains.trading.dao.order_dao import OrderDao
        from app.domains.market.realtime_quote_service import RealtimeQuoteService
        from datetime import date

        gateway = self._get_gateway()
        ledger = self._get_ledger()
        market = self._get_market()
        quote_svc = RealtimeQuoteService()
        try:
            symbol = self.vt_symbol.split(".")[0] if "." in self.vt_symbol else self.vt_symbol
            quote = quote_svc.get_quote(symbol, market)
            last_price = _get_quote_price(quote, price)
        except Exception:
            last_price = price

        if last_price <= 0:
            logger.warning("[paper-engine] No price for %s, skip order", self.vt_symbol)
            if gateway is not None and order_id:
                gateway.update_order_status(order_id, "rejected")
            return

        fill = try_fill_market_order(
            direction=direction,
            quantity=quantity,
            market=market,
            last_price=last_price,
        )
        if not fill.filled:
            logger.warning("[paper-engine] Fill failed: %s", fill.reason)
            if gateway is not None and order_id:
                gateway.update_order_status(order_id, "rejected")
            return

        acct_svc = PaperAccountService()
        dao = OrderDao()
        today_str = date.today().isoformat()

        if direction == "buy":
            total_cost = fill.fill_price * fill.fill_quantity + fill.fee.total
            ok = acct_svc.freeze_funds(self.paper_account_id, total_cost)
            if not ok:
                logger.warning("[paper-engine] Insufficient funds for buy")
                return
            acct_svc.settle_buy(self.paper_account_id, total_cost, total_cost)
        else:
            proceeds = fill.fill_price * fill.fill_quantity - fill.fee.total
            acct_svc.settle_sell(self.paper_account_id, proceeds)

        db_order_id = dao.create(
            user_id=self.user_id,
            symbol=self.vt_symbol.split(".")[0] if "." in self.vt_symbol else self.vt_symbol,
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
        try:
            ledger.record_fill(
                user_id=self.user_id,
                paper_account_id=self.paper_account_id,
                deployment_id=self.deployment_id,
                order_id=db_order_id,
                symbol=self.vt_symbol.split(".")[0] if "." in self.vt_symbol else self.vt_symbol,
                direction=direction,
                quantity=fill.fill_quantity,
                price=fill.fill_price,
                fee=fill.fee.total,
                payload={"gateway_order_id": order_id, "stop": stop},
            )
        except Exception:
            logger.warning("[paper-engine] Failed to record ledger fill", exc_info=True)
        if gateway is not None and order_id:
            gateway.update_order_status(order_id, "filled")
        if strategy is not None:
            callback_order_id = order_id or f"paper.{self.deployment_id}.{db_order_id}"
            self._notify_strategy_order(strategy, callback_order_id, direction, quantity, fill.fill_quantity, fill.fill_price)
            self._notify_strategy_trade(strategy, callback_order_id, direction, fill.fill_quantity, fill.fill_price)
        logger.info("[paper-engine] Order filled: %s %s %d @ %.4f", direction, self.vt_symbol, quantity, fill.fill_price)

    def _notify_strategy_order(
        self,
        strategy: Any,
        vt_orderid: str,
        direction: str,
        quantity: int,
        traded: int,
        price: float,
    ) -> None:
        from vnpy.trader.constant import Direction as VDirection
        from vnpy.trader.constant import Offset as VOffset
        from vnpy.trader.constant import Status
        from vnpy.trader.object import OrderData

        symbol, exchange = _split_vt_symbol(self.vt_symbol)
        gateway_name, orderid = vt_orderid.rsplit(".", 1) if "." in vt_orderid else ("paper", vt_orderid)
        order = OrderData(
            gateway_name=gateway_name,
            symbol=symbol,
            exchange=exchange,
            orderid=orderid,
            direction=VDirection.LONG if direction == "buy" else VDirection.SHORT,
            offset=VOffset.OPEN if direction == "buy" else VOffset.CLOSE,
            price=price,
            volume=quantity,
            traded=traded,
            status=Status.ALLTRADED,
            datetime=datetime.utcnow(),
        )
        if hasattr(strategy, "update_order"):
            strategy.update_order(order)
        if not hasattr(strategy, "on_order"):
            return
        try:
            strategy.on_order(order)
        except Exception:
            logger.debug("[paper-engine] strategy on_order callback failed", exc_info=True)

    def _notify_strategy_trade(
        self,
        strategy: Any,
        vt_orderid: str,
        direction: str,
        quantity: int,
        price: float,
    ) -> None:
        from vnpy.trader.constant import Direction as VDirection
        from vnpy.trader.constant import Offset as VOffset
        from vnpy.trader.object import TradeData

        symbol, exchange = _split_vt_symbol(self.vt_symbol)
        gateway_name, orderid = vt_orderid.rsplit(".", 1) if "." in vt_orderid else ("paper", vt_orderid)
        trade = TradeData(
            gateway_name=gateway_name,
            symbol=symbol,
            exchange=exchange,
            orderid=orderid,
            tradeid=f"trade.{orderid}",
            direction=VDirection.LONG if direction == "buy" else VDirection.SHORT,
            offset=VOffset.OPEN if direction == "buy" else VOffset.CLOSE,
            price=price,
            volume=quantity,
            datetime=datetime.utcnow(),
        )
        if hasattr(strategy, "update_trade"):
            strategy.update_trade(trade)
        if not hasattr(strategy, "on_trade"):
            return
        try:
            strategy.on_trade(trade)
        except Exception:
            logger.debug("[paper-engine] strategy on_trade callback failed", exc_info=True)

    def _write_signal(self, direction: str, quantity: int, price: float, reason: str = "") -> None:
        """Write a signal to paper_signals for semi-auto confirmation."""
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
                    "sym": self.vt_symbol.split(".")[0] if "." in self.vt_symbol else self.vt_symbol,
                    "dir": direction,
                    "qty": quantity,
                    "price": price if price > 0 else None,
                    "reason": reason,
                },
            )
            conn.commit()
        logger.info("[paper-engine] Signal written: %s %s %d (semi-auto)", direction, self.vt_symbol, quantity)

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


class PaperStrategyExecutor:
    """Manages running paper-trading strategy threads."""

    _instance: Optional["PaperStrategyExecutor"] = None
    _lock = threading.Lock()

    def __new__(cls) -> "PaperStrategyExecutor":
        with cls._lock:
            if cls._instance is None:
                cls._instance = super().__new__(cls)
                cls._instance._initialized = False
            return cls._instance

    def __init__(self) -> None:
        if getattr(self, "_initialized", False):
            if not hasattr(self, "_threads"):
                self._threads = {}
            if not hasattr(self, "_stop_events"):
                self._stop_events = {}
            if not hasattr(self, "_gateways"):
                self._gateways = {}
            return
        self._initialized = True
        self._threads: Dict[int, threading.Thread] = {}
        self._stop_events: Dict[int, threading.Event] = {}
        self._gateways: Dict[int, Any] = {}
        logger.info("[paper-executor] PaperStrategyExecutor initialized")

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
        """Start a strategy in a background thread for paper trading."""
        if deployment_id in self._threads and self._threads[deployment_id].is_alive():
            return {"success": False, "error": "Deployment already running"}

        stop_event = threading.Event()
        self._stop_events[deployment_id] = stop_event

        thread = threading.Thread(
            target=self._run_strategy,
            args=(deployment_id, paper_account_id, user_id, strategy_class_name, vt_symbol, parameters, execution_mode, strategy_id, stop_event, gateway),
            daemon=True,
            name=f"paper-strategy-{deployment_id}",
        )
        self._threads[deployment_id] = thread
        if gateway is not None:
            self._gateways[deployment_id] = gateway
        thread.start()

        logger.info("[paper-executor] Deployment %d started: %s on %s mode=%s", deployment_id, strategy_class_name, vt_symbol, execution_mode)
        return {"success": True, "deployment_id": deployment_id}

    def stop_deployment(self, deployment_id: int) -> bool:
        """Signal a running deployment to stop."""
        event = self._stop_events.get(deployment_id)
        if not event:
            return False
        event.set()
        logger.info("[paper-executor] Deployment %d stop requested", deployment_id)
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
        gateway: Any = None,
    ) -> None:
        """Thread entry: load strategy, poll quotes, feed bars."""
        try:
            # Load strategy class
            from app.api.services.strategy_service import compile_strategy
            from app.domains.backtests.dao.strategy_source_dao import StrategySourceDao

            source_dao = StrategySourceDao()
            if strategy_id is not None:
                db_code, db_class, _sv = source_dao.get_strategy_source_for_user(strategy_id, user_id)
                strategy_cls = compile_strategy(db_code, db_class or strategy_class_name)
            else:
                db_code = source_dao.get_strategy_code_by_class_name(strategy_class_name)
                if not db_code:
                    logger.error("[paper-executor] Strategy class '%s' not found", strategy_class_name)
                    return
                strategy_cls = compile_strategy(db_code, strategy_class_name)

            # Create the paper CTA engine stub
            engine = _PaperCtaEngine(
                executor=self,
                deployment_id=deployment_id,
                paper_account_id=paper_account_id,
                user_id=user_id,
                vt_symbol=vt_symbol,
                execution_mode=execution_mode,
                gateway=gateway,
            )

            ts = datetime.utcnow().strftime("%Y%m%d%H%M%S")
            strategy_name = f"{strategy_class_name}_paper_{ts}"
            strategy_instance = strategy_cls(engine, strategy_name, vt_symbol, parameters)
            strategy_instance.on_init()
            strategy_instance.inited = True
            strategy_instance.trading = True
            if hasattr(strategy_instance, "on_start"):
                strategy_instance.on_start()

            logger.info("[paper-executor] Strategy %s initialized for deployment %d", strategy_name, deployment_id)

            # Main loop: poll quotes and feed as bars
            from app.domains.market.realtime_quote_service import RealtimeQuoteService
            quote_svc = RealtimeQuoteService()
            symbol = vt_symbol.split(".")[0] if "." in vt_symbol else vt_symbol
            market = engine._get_market()

            while not stop_event.is_set():
                try:
                    quote = quote_svc.get_quote(symbol, market)
                    if gateway is not None:
                        gateway.publish_tick(vt_symbol, quote)
                    tick = self._quote_to_tick(quote, vt_symbol)
                    bar = self._quote_to_bar(quote, vt_symbol)
                    if tick is not None and hasattr(strategy_instance, "on_tick"):
                        strategy_instance.on_tick(tick)
                    elif bar:
                        strategy_instance.on_bar(bar)
                    PaperExecutionLedger().write_checkpoint(
                        deployment_id=deployment_id,
                        runtime_mode="native_cta_runtime",
                        strategy_kind="cta",
                        checkpoint=_build_runtime_checkpoint(
                            deployment_id=deployment_id,
                            vt_symbols=[vt_symbol],
                            gateway=gateway,
                            strategy=strategy_instance,
                        ),
                    )
                except Exception:
                    logger.debug("[paper-executor] Quote/bar error for %s", vt_symbol, exc_info=True)

                stop_event.wait(_POLL_INTERVAL)

            # Cleanup
            strategy_instance.on_stop()
            PaperExecutionLedger().write_checkpoint(
                deployment_id=deployment_id,
                runtime_mode="native_cta_runtime",
                strategy_kind="cta",
                checkpoint=_build_runtime_checkpoint(
                    deployment_id=deployment_id,
                    vt_symbols=[vt_symbol],
                    gateway=gateway,
                    strategy=strategy_instance,
                ),
            )
            logger.info("[paper-executor] Deployment %d stopped cleanly", deployment_id)

        except Exception:
            logger.exception("[paper-executor] Deployment %d crashed", deployment_id)
        finally:
            self._threads.pop(deployment_id, None)
            self._stop_events.pop(deployment_id, None)
            getattr(self, "_gateways", {}).pop(deployment_id, None)
            # Mark deployment as stopped in DB
            try:
                with connection("quantmate") as conn:
                    conn.execute(
                        text("UPDATE paper_deployments SET status='stopped', stopped_at=NOW() WHERE id=:did AND status='running'"),
                        {"did": deployment_id},
                    )
                    conn.commit()
            except Exception:
                pass

    @staticmethod
    def _quote_to_bar(quote: dict, vt_symbol: str):
        """Convert a realtime quote dict to a vnpy BarData."""
        try:
            from vnpy.trader.object import BarData
            from vnpy.trader.constant import Interval

            last = _get_quote_price(quote)
            if last <= 0:
                return None

            symbol, exchange = _split_vt_symbol(vt_symbol)

            return BarData(
                symbol=symbol,
                exchange=exchange,
                interval=Interval.MINUTE,
                datetime=datetime.now(),
                gateway_name="paper",
                open_price=float(quote.get("open") or last),
                high_price=float(quote.get("high") or last),
                low_price=float(quote.get("low") or last),
                close_price=last,
                volume=float(quote.get("volume") or 0),
                turnover=float(quote.get("turnover") or quote.get("amount") or 0),
            )
        except ImportError:
            logger.debug("[paper-executor] vnpy not available, cannot create BarData")
            return None

    @staticmethod
    def _history_to_bar(history_bar: dict, vt_symbol: str):
        """Convert a stored history row to a vnpy BarData."""
        try:
            from vnpy.trader.constant import Interval
            from vnpy.trader.object import BarData

            close_price = float(history_bar.get("close") or 0)
            if close_price <= 0:
                return None

            bar_dt = history_bar.get("datetime")
            if isinstance(bar_dt, str):
                try:
                    bar_dt = datetime.fromisoformat(bar_dt)
                except ValueError:
                    return None
            if not isinstance(bar_dt, datetime):
                return None

            symbol, exchange = _split_vt_symbol(vt_symbol)
            return BarData(
                symbol=symbol,
                exchange=exchange,
                interval=Interval.DAILY,
                datetime=bar_dt,
                gateway_name="paper_history",
                open_price=float(history_bar.get("open") or close_price),
                high_price=float(history_bar.get("high") or close_price),
                low_price=float(history_bar.get("low") or close_price),
                close_price=close_price,
                volume=float(history_bar.get("volume") or 0),
                turnover=float(history_bar.get("amount") or 0),
            )
        except ImportError:
            logger.debug("[paper-executor] vnpy not available, cannot create history BarData")
            return None

    @staticmethod
    def _quote_to_tick(quote: dict, vt_symbol: str):
        """Convert a realtime quote dict to a vnpy TickData."""
        try:
            from vnpy.trader.object import TickData

            last = _get_quote_price(quote)
            if last <= 0:
                return None

            symbol, exchange = _split_vt_symbol(vt_symbol)

            return TickData(
                symbol=symbol,
                exchange=exchange,
                datetime=datetime.now(),
                gateway_name="paper",
                name=symbol,
                volume=float(quote.get("volume") or 0),
                turnover=float(quote.get("turnover") or quote.get("amount") or 0),
                open_interest=float(quote.get("open_interest") or 0),
                last_price=last,
                open_price=float(quote.get("open") or last),
                high_price=float(quote.get("high") or last),
                low_price=float(quote.get("low") or last),
                pre_close=float(quote.get("prev_close") or quote.get("pre_close") or last),
            )
        except ImportError:
            logger.debug("[paper-executor] vnpy not available, cannot create TickData")
            return None
