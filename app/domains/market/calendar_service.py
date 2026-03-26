"""Financial calendar service — trade days + macro/IPO/dividend events."""

from __future__ import annotations

import logging
from datetime import date, timedelta
from typing import Any

try:
    import akshare as ak
except Exception:
    ak = None  # type: ignore[assignment]

from sqlalchemy import text

from app.infrastructure.db.connections import connection

logger = logging.getLogger(__name__)


class CalendarService:
    """Provide trade-day calendar and upcoming financial events."""

    # ── Trade-day calendar ────────────────────────────────────────────
    def get_trade_days(
        self,
        exchange: str = "SSE",
        start_date: date | None = None,
        end_date: date | None = None,
    ) -> dict[str, Any]:
        if end_date is None:
            end_date = date.today()
        if start_date is None:
            start_date = end_date - timedelta(days=90)

        # Try DB first (trade_calendar table)
        try:
            return self._trade_days_from_db(exchange, start_date, end_date)
        except Exception:
            pass

        # Fallback: AkShare tool_trade_date_hist_sina
        if ak is None:
            return {"exchange": exchange, "trade_days": [], "source": "unavailable"}

        try:
            df = ak.tool_trade_date_hist_sina()
            if "trade_date" in df.columns:
                df["trade_date"] = df["trade_date"].astype(str)
                mask = (df["trade_date"] >= start_date.strftime("%Y-%m-%d")) & (
                    df["trade_date"] <= end_date.strftime("%Y-%m-%d")
                )
                days = df.loc[mask, "trade_date"].tolist()
                return {"exchange": exchange, "trade_days": days, "source": "akshare"}
        except Exception as exc:
            logger.warning("AkShare trade calendar failed: %s", exc)

        return {"exchange": exchange, "trade_days": [], "source": "error"}

    def _trade_days_from_db(self, exchange: str, start: date, end: date) -> dict[str, Any]:
        with connection("tushare") as conn:
            rows = conn.execute(
                text(
                    "SELECT cal_date, is_open FROM trade_calendar "
                    "WHERE exchange = :ex AND cal_date BETWEEN :s AND :e "
                    "ORDER BY cal_date"
                ),
                {"ex": exchange, "s": start.strftime("%Y%m%d"), "e": end.strftime("%Y%m%d")},
            ).fetchall()
        days = [
            r[0] if isinstance(r[0], str) else r[0].strftime("%Y-%m-%d") if hasattr(r[0], "strftime") else str(r[0])
            for r in rows
            if r[1] == 1
        ]
        return {"exchange": exchange, "trade_days": days, "source": "tushare_db"}

    # ── Financial events ──────────────────────────────────────────────
    def get_events(
        self,
        start_date: date | None = None,
        end_date: date | None = None,
        event_type: str | None = None,
    ) -> dict[str, Any]:
        if end_date is None:
            end_date = date.today() + timedelta(days=30)
        if start_date is None:
            start_date = date.today()

        events: list[dict[str, Any]] = []
        types = [event_type] if event_type else ["macro", "ipo", "dividend"]

        for et in types:
            try:
                items = self._fetch_events(et, start_date, end_date)
                events.extend(items)
            except Exception as exc:
                logger.warning("Failed to fetch %s events: %s", et, exc)

        events.sort(key=lambda e: e.get("date", ""))
        return {"events": events, "start_date": str(start_date), "end_date": str(end_date)}

    def _fetch_events(self, event_type: str, start: date, end: date) -> list[dict[str, Any]]:
        if ak is None:
            return []

        if event_type == "macro":
            return self._macro_events(start, end)
        if event_type == "ipo":
            return self._ipo_events()
        if event_type == "dividend":
            return self._dividend_events()
        return []

    def _macro_events(self, start: date, end: date) -> list[dict[str, Any]]:
        try:
            df = ak.news_economic_baidu(date=start.strftime("%Y%m%d"))
            items: list[dict[str, Any]] = []
            for _, row in df.iterrows():
                items.append({
                    "type": "macro",
                    "date": str(row.get("date", row.get("日期", ""))),
                    "time": str(row.get("time", row.get("时间", ""))),
                    "country": str(row.get("country", row.get("国家", ""))),
                    "title": str(row.get("event", row.get("事件", ""))),
                    "importance": str(row.get("importance", row.get("重要性", ""))),
                })
            return items
        except Exception as exc:
            logger.debug("macro events error: %s", exc)
            return []

    def _ipo_events(self) -> list[dict[str, Any]]:
        try:
            df = ak.stock_xgsglb_em(symbol="全部股票")
            items: list[dict[str, Any]] = []
            for _, row in df.head(30).iterrows():
                items.append({
                    "type": "ipo",
                    "date": str(row.get("上市日期", "")),
                    "title": str(row.get("股票简称", "")),
                    "symbol": str(row.get("股票代码", "")),
                    "price": str(row.get("发行价格", "")),
                })
            return items
        except Exception as exc:
            logger.debug("ipo events error: %s", exc)
            return []

    def _dividend_events(self) -> list[dict[str, Any]]:
        try:
            df = ak.stock_fhps_em(date="")
            items: list[dict[str, Any]] = []
            for _, row in df.head(30).iterrows():
                items.append({
                    "type": "dividend",
                    "date": str(row.get("除权除息日", "")),
                    "title": str(row.get("名称", "")),
                    "symbol": str(row.get("代码", "")),
                    "detail": str(row.get("分红方案", "")),
                })
            return items
        except Exception as exc:
            logger.debug("dividend events error: %s", exc)
            return []
