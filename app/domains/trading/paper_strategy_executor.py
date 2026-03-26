"""Paper Strategy Executor — runs CTA strategies in a sandbox for paper trading.

Instead of routing orders to a vnpy gateway, orders go through the matching engine
with virtual paper accounts. Each deployment runs in its own thread consuming
realtime quotes converted to BarData.
"""

from __future__ import annotations

import logging
import threading
import time as _time
from datetime import datetime
from typing import Any, Dict, Optional

from sqlalchemy import text

from app.infrastructure.db.connections import connection

logger = logging.getLogger(__name__)

# How often to poll for new bars (seconds)
_POLL_INTERVAL = 5


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
    ) -> None:
        self.executor = executor
        self.deployment_id = deployment_id
        self.paper_account_id = paper_account_id
        self.user_id = user_id
        self.vt_symbol = vt_symbol
        self.execution_mode = execution_mode
        self._order_counter = 0

    # -- Methods invoked by CtaTemplate ---------------------

    def send_order(
        self,
        strategy: Any,
        direction,
        offset,
        price: float,
        volume: float,
        stop: bool = False,
        lock: bool = False,
        net: bool = False,
    ) -> list[str]:
        """Intercept order from strategy and route to paper trading."""
        from vnpy.trader.constant import Direction as VDirection

        dir_str = "buy" if direction == VDirection.LONG else "sell"
        qty = int(volume)

        if self.execution_mode == "semi_auto":
            self._write_signal(dir_str, qty, price, reason=f"Strategy signal: {strategy.strategy_name}")
            return []

        # Auto mode — execute through matching engine immediately
        self._execute_order(dir_str, qty, price, stop=stop)
        vt_id = f"paper.{self.deployment_id}.{self._order_counter}"
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

    def put_event(self) -> None:
        pass

    def send_email(self, msg: str, strategy: Any = None) -> None:
        pass

    # -- Internal helpers -----------------------------------

    def _execute_order(self, direction: str, quantity: int, price: float, stop: bool = False) -> None:
        """Execute order via matching engine + paper account settlement."""
        from app.domains.trading.matching_engine import try_fill_market_order
        from app.domains.trading.paper_account_service import PaperAccountService
        from app.domains.trading.dao.order_dao import OrderDao
        from app.domains.market.realtime_quote_service import RealtimeQuoteService
        from datetime import date

        market = self._get_market()
        quote_svc = RealtimeQuoteService()
        try:
            symbol = self.vt_symbol.split(".")[0] if "." in self.vt_symbol else self.vt_symbol
            quote = quote_svc.get_quote(symbol, market)
            last_price = quote.get("last_price") or quote.get("price") or quote.get("current") or price
        except Exception:
            last_price = price

        if last_price <= 0:
            logger.warning("[paper-engine] No price for %s, skip order", self.vt_symbol)
            return

        fill = try_fill_market_order(
            direction=direction,
            quantity=quantity,
            market=market,
            last_price=last_price,
        )
        if not fill.filled:
            logger.warning("[paper-engine] Fill failed: %s", fill.reason)
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
            acct_svc.settle_buy(self.paper_account_id, total_cost)
        else:
            proceeds = fill.fill_price * fill.fill_quantity - fill.fee.total
            acct_svc.settle_sell(self.paper_account_id, proceeds)

        order_id = dao.create(
            user_id=self.user_id,
            symbol=self.vt_symbol.split(".")[0] if "." in self.vt_symbol else self.vt_symbol,
            direction=direction,
            order_type="market",
            quantity=quantity,
            price=fill.fill_price,
            mode="paper",
            paper_account_id=self.paper_account_id,
            buy_date=today_str if direction == "buy" else None,
            strategy_id=self._get_strategy_id(),
        )
        dao.update_status(order_id, "filled", filled_quantity=fill.fill_quantity, avg_fill_price=fill.fill_price, fee=fill.fee.total)
        dao.insert_trade(order_id, fill.fill_quantity, fill.fill_price, fill.fee.total)
        logger.info("[paper-engine] Order filled: %s %s %d @ %.4f", direction, self.vt_symbol, quantity, fill.fill_price)

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
        if self._initialized:
            return
        self._initialized = True
        self._threads: Dict[int, threading.Thread] = {}
        self._stop_events: Dict[int, threading.Event] = {}
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
    ) -> Dict[str, Any]:
        """Start a strategy in a background thread for paper trading."""
        if deployment_id in self._threads and self._threads[deployment_id].is_alive():
            return {"success": False, "error": "Deployment already running"}

        stop_event = threading.Event()
        self._stop_events[deployment_id] = stop_event

        thread = threading.Thread(
            target=self._run_strategy,
            args=(deployment_id, paper_account_id, user_id, strategy_class_name, vt_symbol, parameters, execution_mode, strategy_id, stop_event),
            daemon=True,
            name=f"paper-strategy-{deployment_id}",
        )
        self._threads[deployment_id] = thread
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
            )

            ts = datetime.utcnow().strftime("%Y%m%d%H%M%S")
            strategy_name = f"{strategy_class_name}_paper_{ts}"
            strategy_instance = strategy_cls(engine, strategy_name, vt_symbol, parameters)
            strategy_instance.on_init()
            strategy_instance.inited = True
            strategy_instance.trading = True

            logger.info("[paper-executor] Strategy %s initialized for deployment %d", strategy_name, deployment_id)

            # Main loop: poll quotes and feed as bars
            from app.domains.market.realtime_quote_service import RealtimeQuoteService
            quote_svc = RealtimeQuoteService()
            symbol = vt_symbol.split(".")[0] if "." in vt_symbol else vt_symbol
            market = engine._get_market()

            while not stop_event.is_set():
                try:
                    quote = quote_svc.get_quote(symbol, market)
                    bar = self._quote_to_bar(quote, vt_symbol)
                    if bar:
                        strategy_instance.on_bar(bar)
                except Exception:
                    logger.debug("[paper-executor] Quote/bar error for %s", vt_symbol, exc_info=True)

                stop_event.wait(_POLL_INTERVAL)

            # Cleanup
            strategy_instance.on_stop()
            logger.info("[paper-executor] Deployment %d stopped cleanly", deployment_id)

        except Exception:
            logger.exception("[paper-executor] Deployment %d crashed", deployment_id)
        finally:
            self._threads.pop(deployment_id, None)
            self._stop_events.pop(deployment_id, None)
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
            from vnpy.trader.constant import Exchange, Interval

            last = float(quote.get("last_price") or quote.get("price") or quote.get("current") or 0)
            if last <= 0:
                return None

            parts = vt_symbol.split(".")
            symbol = parts[0]
            exchange_str = parts[1] if len(parts) > 1 else "SSE"
            try:
                exchange = Exchange(exchange_str)
            except ValueError:
                exchange = Exchange.SSE

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
