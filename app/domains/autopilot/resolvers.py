"""Autopilot target resolution helpers (user / paper account auto-detection)."""

from __future__ import annotations

import logging
from typing import Optional

from sqlalchemy import text

from app.domains.autopilot.policies import Policies
from app.infrastructure.db.connections import connection

logger = logging.getLogger(__name__)


def resolve_user_id(policies: Policies) -> int:
    """Resolve the autopilot target user (0 = first user in the system)."""
    if policies.user_id and policies.user_id > 0:
        return policies.user_id
    with connection("quantmate") as conn:
        row = conn.execute(text("SELECT id FROM users ORDER BY id ASC LIMIT 1")).fetchone()
    if not row:
        raise RuntimeError("No users found; cannot resolve autopilot user.")
    return int(row[0])


def resolve_paper_account(user_id: int, policies: Policies) -> int:
    """Resolve the autopilot target paper account (0 = first active, else create)."""
    if policies.paper_account_id and policies.paper_account_id > 0:
        return policies.paper_account_id

    with connection("quantmate") as conn:
        row = conn.execute(
            text("SELECT id FROM paper_accounts WHERE user_id = :uid AND status = 'active' ORDER BY id ASC LIMIT 1"),
            {"uid": user_id},
        ).fetchone()
        if row:
            return int(row[0])

    from app.domains.trading.paper_account_service import PaperAccountService

    result = PaperAccountService().create_account(
        user_id=user_id,
        name="Autopilot Paper",
        initial_capital=1_000_000.0,
        market="CN",
    )
    if not result.get("success"):
        raise RuntimeError(f"Failed to create autopilot paper account: {result.get('error')}")
    return int(result["account_id"])


def get_paper_account(user_id: int, account_id: int) -> Optional[dict]:
    with connection("quantmate") as conn:
        row = conn.execute(
            text("SELECT id, initial_capital, balance, market_value FROM paper_accounts WHERE id = :aid AND user_id = :uid"),
            {"aid": account_id, "uid": user_id},
        ).fetchone()
    return dict(row._mapping) if row else None