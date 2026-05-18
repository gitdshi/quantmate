"""Paper composite executor.

Runs composite strategies against the paper account runtime by polling snapshot
quotes, invoking the composite orchestrator, and converting generated orders
into paper fills or signals.
"""

from __future__ import annotations

import json
import logging
import threading
from datetime import date, datetime
from typing import Any, Dict, Optional

from sqlalchemy import text

from app.domains.composite.dao.composite_strategy_dao import CompositeStrategyDao
from app.domains.composite.dao.strategy_component_dao import StrategyComponentDao
from app.domains.composite.market_constraints import MarketConstraints, Order
from app.domains.composite.orchestrator import CompositeStrategyOrchestrator
from app.domains.market.realtime_quote_service import RealtimeQuoteService
from app.domains.trading.dao.order_dao import OrderDao
from app.domains.trading.paper_execution_ledger import PaperExecutionLedger
from app.domains.trading.paper_gateway import PaperGatewayOrderRequest
from app.domains.trading.paper_strategy_executor import (
    _POLL_INTERVAL,
    _build_runtime_checkpoint,
    _normalize_vt_symbols,
)
from app.domains.trading.paper_account_service import PaperAccountService
from app.infrastructure.db.connections import connection

logger = logging.getLogger(__name__)


def _parse_json(value: Any) -> Dict[str, Any]:
    if isinstance(value, dict):
        return dict(value)
    if isinstance(value, str):
        try:
            parsed = json.loads(value)
            return parsed if isinstance(parsed, dict) else {}
        except (TypeError, json.JSONDecodeError):
            return {}
    return {}


def _symbol_code(symbol: str) -> str:
    return symbol.split(".", 1)[0].strip() if symbol else ""


class PaperCompositeExecutor:
    """Manages running composite-strategy paper threads."""

    _instance: Optional["PaperCompositeExecutor"] = None
    _lock = threading.Lock()

    def __new__(cls) -> "PaperCompositeExecutor":
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
        composite_strategy_id: int,
        strategy_name: str,
        vt_symbol: str,
        parameters: Dict[str, Any],
        execution_mode: str = "auto",
        gateway: Any = None,
    ) -> Dict[str, Any]:
        if deployment_id in self._threads and self._threads[deployment_id].is_alive():
            return {"success": False, "error": "Deployment already running"}

        stop_event = threading.Event()
        self._stop_events[deployment_id] = stop_event
        thread = threading.Thread(
            target=self._run_strategy,
            args=(
                deployment_id,
                paper_account_id,
                user_id,
                composite_strategy_id,
                strategy_name,
                vt_symbol,
                parameters,
                execution_mode,
                stop_event,
                gateway,
            ),
            daemon=True,
            name=f"paper-composite-{deployment_id}",
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
        composite_strategy_id: int,
        strategy_name: str,
        vt_symbol: str,
        parameters: Dict[str, Any],
        execution_mode: str,
        stop_event: threading.Event,
        gateway: Any,
    ) -> None:
        emitted_signal_keys: set[str] = set()
        current_day = date.today()
        try:
            strategy, universe_components, trading_components, risk_components = self._load_composite_definition(
                user_id=user_id,
                composite_strategy_id=composite_strategy_id,
            )
            constraints = MarketConstraints.from_dict(_parse_json(strategy.get("market_constraints")))
            orchestrator = CompositeStrategyOrchestrator(
                universe_components,
                trading_components,
                risk_components,
            )

            vt_symbols = _normalize_vt_symbols(vt_symbol)
            vt_symbol_map = {code: item for item in vt_symbols if (code := _symbol_code(item))}
            all_symbols = list(vt_symbol_map.keys())
            if not all_symbols:
                raise ValueError("Composite paper deployment requires at least one symbol")

            quote_svc = RealtimeQuoteService()
            acct_svc = PaperAccountService()

            while not stop_event.is_set():
                today = date.today()
                if today != current_day:
                    emitted_signal_keys.clear()
                    current_day = today

                account = acct_svc.get_account(paper_account_id, user_id)
                if not account:
                    raise ValueError("Paper account not found")

                positions, buy_dates = self._load_positions(user_id=user_id, paper_account_id=paper_account_id)
                market = account.get("market", "CN")

                active_symbols = list(dict.fromkeys([*all_symbols, *positions.keys()]))
                day_data: Dict[str, Dict[str, float]] = {}
                prices: Dict[str, float] = {}
                prev_close: Dict[str, float] = {}

                for symbol in active_symbols:
                    quote = quote_svc.get_quote(symbol, market)
                    vt_value = vt_symbol_map.get(symbol, symbol)
                    if gateway is not None:
                        gateway.publish_tick(vt_value, quote)

                    last_price = float(quote.get("last_price") or quote.get("price") or quote.get("current") or 0)
                    previous_close = float(quote.get("prev_close") or quote.get("pre_close") or last_price or 0)
                    volume = float(quote.get("volume") or 0)
                    day_data[symbol] = {
                        "close": last_price,
                        "prev_close": previous_close,
                        "volume": volume,
                    }
                    prices[symbol] = last_price
                    prev_close[symbol] = previous_close

                orders = orchestrator.run_day(
                    trading_day=today.isoformat(),
                    all_symbols=active_symbols,
                    market_data=day_data,
                    prices=prices,
                    cash=float(account.get("balance") or 0),
                    positions=positions,
                )

                orders = constraints.apply_t_plus_n(orders, buy_dates, today)
                orders = constraints.apply_price_limits(orders, prev_close, prices)
                orders = constraints.apply_lot_size(orders)

                for order in orders:
                    order_key = f"{today.isoformat()}:{order.symbol}:{order.direction}:{order.quantity}:{order.reason}"
                    if execution_mode == "semi_auto" and order_key in emitted_signal_keys:
                        continue

                    if execution_mode == "semi_auto":
                        self._write_signal(
                            user_id=user_id,
                            paper_account_id=paper_account_id,
                            deployment_id=deployment_id,
                            order=order,
                            reason=f"Composite signal: {strategy_name}; {order.reason}".strip("; "),
                        )
                        emitted_signal_keys.add(order_key)
                        continue

                    self._execute_order(
                        deployment_id=deployment_id,
                        paper_account_id=paper_account_id,
                        user_id=user_id,
                        composite_strategy_id=composite_strategy_id,
                        strategy_name=strategy_name,
                        order=order,
                        constraints=constraints,
                        gateway=gateway,
                        vt_symbol=vt_symbol_map.get(order.symbol, order.symbol),
                    )

                PaperExecutionLedger().write_checkpoint(
                    deployment_id=deployment_id,
                    runtime_mode="composite_strategy_bridge",
                    strategy_kind="portfolio",
                    checkpoint=_build_runtime_checkpoint(
                        deployment_id=deployment_id,
                        vt_symbols=[vt_symbol_map.get(symbol, symbol) for symbol in active_symbols],
                        gateway=gateway,
                    ),
                )

                stop_event.wait(_POLL_INTERVAL)
        except Exception:
            logger.exception("[paper-composite] Deployment %d crashed", deployment_id)
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
                logger.debug("[paper-composite] failed to update stop status", exc_info=True)

    @staticmethod
    def _load_composite_definition(*, user_id: int, composite_strategy_id: int) -> tuple[dict[str, Any], list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]]]:
        composite_dao = CompositeStrategyDao()
        component_dao = StrategyComponentDao()

        strategy = composite_dao.get_for_user(composite_strategy_id, user_id)
        if not strategy:
            raise ValueError(f"Composite strategy {composite_strategy_id} not found")

        bindings = composite_dao.get_bindings(composite_strategy_id)
        if not bindings:
            raise ValueError("No component bindings defined")

        component_ids = {binding["component_id"] for binding in bindings}
        components_map = {
            component_id: component_dao.get_for_user(component_id, user_id)
            for component_id in component_ids
        }

        universe_components: list[dict[str, Any]] = []
        trading_components: list[dict[str, Any]] = []
        risk_components: list[dict[str, Any]] = []

        for binding in sorted(bindings, key=lambda item: item.get("ordinal", 0)):
            component = components_map.get(binding["component_id"])
            if not component:
                continue

            enriched = dict(component)
            enriched["config_override"] = _parse_json(binding.get("config_override"))
            enriched["weight"] = float(binding.get("weight", 1.0))

            layer = binding.get("layer", component.get("layer", ""))
            if layer == "universe":
                universe_components.append(enriched)
            elif layer == "trading":
                trading_components.append(enriched)
            elif layer == "risk":
                risk_components.append(enriched)

        if not trading_components:
            raise ValueError("At least one trading component is required")

        return strategy, universe_components, trading_components, risk_components

    @staticmethod
    def _load_positions(*, user_id: int, paper_account_id: int) -> tuple[Dict[str, Dict[str, Any]], Dict[str, date]]:
        ledger = PaperExecutionLedger()
        raw_positions = ledger.get_positions(user_id=user_id, paper_account_id=paper_account_id)
        buy_dates = PaperCompositeExecutor._load_buy_dates(paper_account_id=paper_account_id)

        positions: Dict[str, Dict[str, Any]] = {}
        today = date.today()
        for position in raw_positions:
            symbol = _symbol_code(str(position.get("symbol") or ""))
            if not symbol:
                continue

            opened_on = buy_dates.get(symbol)
            held_days = (today - opened_on).days if opened_on else 0
            positions[symbol] = {
                "quantity": int(position.get("quantity") or 0),
                "avg_cost": float(position.get("avg_cost") or 0),
                "held_days": held_days,
            }

        return positions, buy_dates

    @staticmethod
    def _load_buy_dates(*, paper_account_id: int) -> Dict[str, date]:
        with connection("quantmate") as conn:
            rows = conn.execute(
                text(
                    """
                    SELECT symbol, MIN(opened_at) AS opened_at
                    FROM paper_position_lots
                    WHERE paper_account_id = :aid AND status = 'open' AND side = 'long'
                    GROUP BY symbol
                    """
                ),
                {"aid": paper_account_id},
            ).fetchall()

        return {
            _symbol_code(str(row.symbol)): row.opened_at.date() if hasattr(row.opened_at, "date") else row.opened_at
            for row in rows
            if getattr(row, "symbol", None) and getattr(row, "opened_at", None)
        }

    def _execute_order(
        self,
        *,
        deployment_id: int,
        paper_account_id: int,
        user_id: int,
        composite_strategy_id: int,
        strategy_name: str,
        order: Order,
        constraints: MarketConstraints,
        gateway: Any,
        vt_symbol: str,
    ) -> None:
        acct_svc = PaperAccountService()
        dao = OrderDao()
        ledger = PaperExecutionLedger()

        market_price = float(order.price or 0)
        if market_price <= 0:
            logger.warning("[paper-composite] No price for %s, skip order", order.symbol)
            return

        fill_price = constraints.calculate_fill_price(market_price, order.direction)
        amount = fill_price * order.quantity
        fee = constraints.calculate_commission(amount, order.direction)
        gateway_order_id: Optional[str] = None

        if gateway is not None:
            order_state = gateway.submit_order(
                PaperGatewayOrderRequest(
                    vt_symbol=vt_symbol,
                    direction=order.direction,
                    order_type="market",
                    volume=order.quantity,
                    price=fill_price,
                    metadata={
                        "strategy_name": strategy_name,
                        "composite_strategy_id": composite_strategy_id,
                        "reason": order.reason,
                    },
                )
            )
            gateway_order_id = order_state.order_id

        if order.direction == "buy":
            total_cost = amount + fee
            ok = acct_svc.freeze_funds(paper_account_id, total_cost)
            if not ok:
                if gateway_order_id and gateway is not None:
                    gateway.update_order_status(gateway_order_id, "rejected")
                logger.warning("[paper-composite] Insufficient funds for buy on %s", order.symbol)
                return
            acct_svc.settle_buy(paper_account_id, total_cost, total_cost)
        else:
            proceeds = amount - fee
            acct_svc.settle_sell(paper_account_id, proceeds)

        today_str = date.today().isoformat()
        db_order_id = dao.create(
            user_id=user_id,
            symbol=order.symbol,
            direction=order.direction,
            order_type="market",
            quantity=order.quantity,
            price=fill_price,
            mode="paper",
            paper_account_id=paper_account_id,
            paper_deployment_id=deployment_id,
            buy_date=today_str if order.direction == "buy" else None,
        )
        dao.update_status(
            db_order_id,
            "filled",
            filled_quantity=order.quantity,
            avg_fill_price=fill_price,
            fee=fee,
        )
        dao.insert_trade(db_order_id, order.quantity, fill_price, fee)
        ledger.record_fill(
            user_id=user_id,
            paper_account_id=paper_account_id,
            deployment_id=deployment_id,
            order_id=db_order_id,
            symbol=order.symbol,
            direction=order.direction,
            quantity=order.quantity,
            price=fill_price,
            fee=fee,
            payload={
                "composite_strategy_id": composite_strategy_id,
                "gateway_order_id": gateway_order_id,
                "reason": order.reason,
            },
        )

        if gateway_order_id and gateway is not None:
            gateway.update_order_status(gateway_order_id, "filled")

    @staticmethod
    def _write_signal(
        *,
        user_id: int,
        paper_account_id: int,
        deployment_id: int,
        order: Order,
        reason: str,
    ) -> None:
        with connection("quantmate") as conn:
            conn.execute(
                text(
                    """
                    INSERT INTO paper_signals (user_id, paper_account_id, deployment_id, symbol,
                                               direction, quantity, suggested_price, reason, status)
                    VALUES (:uid, :paid, :did, :sym, :dir, :qty, :price, :reason, 'pending')
                    """
                ),
                {
                    "uid": user_id,
                    "paid": paper_account_id,
                    "did": deployment_id,
                    "sym": order.symbol,
                    "dir": order.direction,
                    "qty": order.quantity,
                    "price": order.price if order.price > 0 else None,
                    "reason": reason,
                },
            )
            conn.commit()