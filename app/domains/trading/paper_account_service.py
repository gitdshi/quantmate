"""Paper Account Service — manages virtual capital accounts for paper trading.

Handles account lifecycle, fund freeze/release, and daily mark-to-market settlement.
"""

from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional

from app.domains.trading.dao.paper_account_dao import PaperAccountDao

logger = logging.getLogger(__name__)

# Currency ↔ market mapping
_MARKET_CURRENCY = {"CN": "CNY", "HK": "HKD", "US": "USD"}


class PaperAccountService:
    """Manage paper trading virtual accounts."""

    def __init__(self) -> None:
        self._dao = PaperAccountDao()

    # ── Account lifecycle ───────────────────────────────────

    def create_account(
        self,
        user_id: int,
        name: str,
        initial_capital: float,
        market: str = "CN",
    ) -> Dict[str, Any]:
        if initial_capital <= 0:
            return {"success": False, "error": "Initial capital must be positive"}
        if market not in _MARKET_CURRENCY:
            return {"success": False, "error": f"Unsupported market: {market}"}

        currency = _MARKET_CURRENCY[market]
        account_id = self._dao.create(
            user_id=user_id,
            name=name,
            initial_capital=initial_capital,
            currency=currency,
            market=market,
        )
        logger.info("Paper account %d created: user=%d market=%s capital=%s", account_id, user_id, market, initial_capital)
        return {"success": True, "account_id": account_id}

    def list_accounts(self, user_id: int, status: Optional[str] = None) -> List[Dict[str, Any]]:
        return self._dao.list_by_user(user_id, status=status)

    def get_account(self, account_id: int, user_id: int) -> Optional[Dict[str, Any]]:
        return self._dao.get_by_id(account_id, user_id)

    def close_account(self, account_id: int, user_id: int) -> bool:
        ok = self._dao.close_account(account_id, user_id)
        if ok:
            logger.info("Paper account %d closed by user %d", account_id, user_id)
        return ok

    # ── Fund operations ─────────────────────────────────────

    def freeze_funds(self, account_id: int, amount: float) -> bool:
        return self._dao.freeze_funds(account_id, amount)

    def release_funds(self, account_id: int, amount: float) -> bool:
        return self._dao.release_funds(account_id, amount)

    def settle_buy(self, account_id: int, frozen_amount: float, actual_cost: float) -> bool:
        return self._dao.settle_buy(account_id, frozen_amount, actual_cost)

    def settle_sell(self, account_id: int, proceeds: float) -> bool:
        return self._dao.settle_sell(account_id, proceeds)

    # ── Equity curve ────────────────────────────────────────

    def get_equity_curve(self, account_id: int, user_id: int) -> List[Dict[str, Any]]:
        account = self._dao.get_by_id(account_id, user_id)
        if not account:
            return []
        return self._dao.get_equity_curve(account_id)
