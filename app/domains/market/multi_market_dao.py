"""Multi-market data access — HK and US markets."""
from __future__ import annotations

from typing import Any

from sqlalchemy import text

from app.infrastructure.db.connections import connection


class MultiMarketDao:
    """Access HK/US stock basic + daily data."""

    # ── Exchanges ──────────────────────────────────────────────────────

    def list_exchanges(self, enabled_only: bool = True) -> list[dict[str, Any]]:
        with connection("quantmate") as conn:
            where = " WHERE enabled = 1" if enabled_only else ""
            rows = conn.execute(text(f"SELECT * FROM market_exchanges{where} ORDER BY code")).fetchall()
            return [dict(r._mapping) for r in rows]

    # ── HK stocks ──────────────────────────────────────────────────────

    def list_hk_stocks(self, status: str = "L", limit: int = 500) -> list[dict[str, Any]]:
        with connection("tushare") as conn:
            rows = conn.execute(
                text("SELECT * FROM hk_stock_basic WHERE list_status = :s ORDER BY ts_code LIMIT :lim"),
                {"s": status, "lim": limit},
            ).fetchall()
            return [dict(r._mapping) for r in rows]

    def get_hk_daily(self, ts_code: str, start_date: str, end_date: str) -> list[dict[str, Any]]:
        with connection("tushare") as conn:
            rows = conn.execute(
                text(
                    "SELECT * FROM hk_stock_daily "
                    "WHERE ts_code = :ts AND trade_date BETWEEN :s AND :e ORDER BY trade_date"
                ),
                {"ts": ts_code, "s": start_date, "e": end_date},
            ).fetchall()
            return [dict(r._mapping) for r in rows]

    # ── US stocks ──────────────────────────────────────────────────────

    def list_us_stocks(self, status: str = "L", limit: int = 500) -> list[dict[str, Any]]:
        with connection("tushare") as conn:
            rows = conn.execute(
                text("SELECT * FROM us_stock_basic WHERE list_status = :s ORDER BY ts_code LIMIT :lim"),
                {"s": status, "lim": limit},
            ).fetchall()
            return [dict(r._mapping) for r in rows]

    def get_us_daily(self, ts_code: str, start_date: str, end_date: str) -> list[dict[str, Any]]:
        with connection("tushare") as conn:
            rows = conn.execute(
                text(
                    "SELECT * FROM us_stock_daily "
                    "WHERE ts_code = :ts AND trade_date BETWEEN :s AND :e ORDER BY trade_date"
                ),
                {"ts": ts_code, "s": start_date, "e": end_date},
            ).fetchall()
            return [dict(r._mapping) for r in rows]
