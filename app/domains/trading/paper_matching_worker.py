"""Paper matching worker — background thread that matches pending paper orders
against real-time market data.

Runs a polling loop every few seconds during trading hours:
1. Fetch all pending limit/stop paper orders from DB
2. Batch-fetch real-time quotes for their symbols
3. Attempt to fill each order via matching_engine
4. Persist fills: update order status, record trade, settle account funds
"""

from __future__ import annotations

import logging
import threading
import time as _time
from datetime import date, datetime
from typing import Any, Dict, List, Optional

from sqlalchemy import text

from app.infrastructure.db.connections import connection

logger = logging.getLogger(__name__)

_worker_thread: Optional[threading.Thread] = None
_stop_event = threading.Event()

POLL_INTERVAL_SECONDS = 3


# ── Public API ──────────────────────────────────────────────


def start_worker() -> None:
    """Start the background matching worker thread (idempotent)."""
    global _worker_thread
    if _worker_thread is not None and _worker_thread.is_alive():
        logger.info("[paper-worker] Already running")
        return
    _stop_event.clear()
    _worker_thread = threading.Thread(target=_run_loop, daemon=True, name="paper-matching-worker")
    _worker_thread.start()
    logger.info("[paper-worker] Started")


def stop_worker() -> None:
    """Signal the worker to stop."""
    _stop_event.set()
    logger.info("[paper-worker] Stop requested")


# ── Main loop ───────────────────────────────────────────────


def _run_loop() -> None:
    logger.info("[paper-worker] Worker loop started")
    while not _stop_event.is_set():
        try:
            _tick()
        except Exception:
            logger.exception("[paper-worker] Error in tick")
        _stop_event.wait(POLL_INTERVAL_SECONDS)
    logger.info("[paper-worker] Worker loop exited")


def _tick() -> None:
    """One iteration of the matching loop."""
    from app.domains.trading.market_rules import is_cn_trading_hours

    # Only process during trading hours (CN for now)
    # Future: check per-market trading hours
    now = datetime.now()
    if not is_cn_trading_hours(now):
        return

    # 1. Fetch pending orders
    pending = _fetch_pending_orders()
    if not pending:
        return

    # 2. Collect unique symbols and their markets
    symbols_markets: Dict[str, str] = {}
    for o in pending:
        sym = o["symbol"]
        market = o.get("market", "CN")
        if sym not in symbols_markets:
            symbols_markets[sym] = market

    # 3. Batch fetch quotes
    quotes = _batch_fetch_quotes(symbols_markets)

    # 4. Try to match each order
    from app.domains.trading.matching_engine import match_order
    from app.domains.trading.dao.order_dao import OrderDao
    from app.domains.trading.dao.paper_account_dao import PaperAccountDao

    dao = OrderDao()
    acct_dao = PaperAccountDao()

    for order in pending:
        sym = order["symbol"]
        quote = quotes.get(sym)
        if not quote or not quote.get("price"):
            continue

        last_price = quote["price"]
        market = order.get("market", "CN")

        result = match_order(
            order_type=order["order_type"],
            direction=order["direction"],
            quantity=order["quantity"],
            price=order.get("price"),
            stop_price=order.get("stop_price"),
            market=market,
            last_price=last_price,
        )

        if not result.filled:
            continue

        # Fill the order
        order_id = order["id"]
        fee_total = result.fee.total if result.fee else 0.0

        dao.update_status(
            order_id,
            "filled",
            filled_quantity=result.fill_quantity,
            avg_fill_price=result.fill_price,
            fee=fee_total,
        )
        dao.insert_trade(order_id, result.fill_quantity, result.fill_price, fee_total)

        # Settle account funds
        account_id = order.get("paper_account_id")
        if account_id:
            if order["direction"] == "buy":
                frozen_amount = (order.get("price") or result.fill_price) * order["quantity"] * 1.003
                actual_cost = result.fill_price * result.fill_quantity + fee_total
                acct_dao.settle_buy(account_id, frozen_amount, actual_cost)
            else:
                proceeds = result.fill_price * result.fill_quantity - fee_total
                acct_dao.settle_sell(account_id, proceeds)

        logger.info(
            "[paper-worker] Order %d filled: %s %s %d @ %.4f fee=%.4f",
            order_id, order["direction"], sym, result.fill_quantity, result.fill_price, fee_total,
        )


# ── Helpers ─────────────────────────────────────────────────


def _fetch_pending_orders() -> List[Dict[str, Any]]:
    """Fetch all pending paper limit/stop orders."""
    with connection("quantmate") as conn:
        rows = conn.execute(
            text("""
                SELECT o.id, o.symbol, o.direction, o.order_type, o.quantity, o.price,
                       o.stop_price, o.paper_account_id,
                       COALESCE(pa.market, 'CN') as market
                FROM orders o
                LEFT JOIN paper_accounts pa ON o.paper_account_id = pa.id
                WHERE o.mode = 'paper'
                  AND o.status IN ('created', 'submitted')
                  AND o.order_type IN ('limit', 'stop', 'stop_limit')
                ORDER BY o.created_at ASC
                LIMIT 500
            """),
        ).fetchall()
        return [
            {
                "id": r.id,
                "symbol": r.symbol,
                "direction": r.direction,
                "order_type": r.order_type,
                "quantity": r.quantity,
                "price": float(r.price) if r.price else None,
                "stop_price": float(r.stop_price) if r.stop_price else None,
                "paper_account_id": r.paper_account_id,
                "market": r.market,
            }
            for r in rows
        ]


def _batch_fetch_quotes(symbols_markets: Dict[str, str]) -> Dict[str, Dict[str, Any]]:
    """Fetch real-time quotes for a batch of symbols."""
    from app.domains.market.realtime_quote_service import RealtimeQuoteService

    svc = RealtimeQuoteService()
    results: Dict[str, Dict[str, Any]] = {}
    for sym, market in symbols_markets.items():
        try:
            quote = svc.get_quote(sym, market)
            results[sym] = quote
        except Exception:
            logger.debug("[paper-worker] Failed to fetch quote for %s", sym)
    return results
