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

from app.domains.extdata.dao.data_sync_status_dao import get_cached_trade_dates
from app.infrastructure.config import get_runtime_int
from app.infrastructure.db.connections import connection
from app.infrastructure.runtime_cache import ExpiringCache

logger = logging.getLogger(__name__)

TRADE_DAYS_CACHE_TTL_SECONDS = get_runtime_int(
    env_keys="MARKET_CALENDAR_TRADE_DAYS_CACHE_TTL_SECONDS",
    db_key="market.calendar.trade_days_cache_ttl_seconds",
    default=300,
)
EVENTS_CACHE_TTL_SECONDS = get_runtime_int(
    env_keys="MARKET_CALENDAR_EVENTS_CACHE_TTL_SECONDS",
    db_key="market.calendar.events_cache_ttl_seconds",
    default=300,
)
MAX_EVENTS_PER_TYPE = get_runtime_int(
    env_keys="MARKET_CALENDAR_MAX_EVENTS_PER_TYPE",
    db_key="market.calendar.max_events_per_type",
    default=30,
)

_TRADE_DAYS_CACHE = ExpiringCache(name="market_calendar_trade_days", maxsize=128)
_EVENTS_CACHE = ExpiringCache(name="market_calendar_events", maxsize=128)


def _format_date_value(value: Any) -> str:
    if value is None:
        return ""
    if hasattr(value, "strftime"):
        return value.strftime("%Y-%m-%d")

    text_value = str(value).strip()
    if len(text_value) == 8 and text_value.isdigit():
        return f"{text_value[:4]}-{text_value[4:6]}-{text_value[6:]}"
    return text_value


def _format_symbol(value: Any) -> str:
    text_value = str(value or "").strip()
    if "." in text_value:
        return text_value.split(".", 1)[0]
    return text_value


def _format_dividend_detail(div_cash: Any, div_stock: Any, bonus_ratio: Any) -> str:
    parts: list[str] = []
    if div_cash not in (None, ""):
        parts.append(f"cash {div_cash}")
    if div_stock not in (None, ""):
        parts.append(f"stock {div_stock}")
    if bonus_ratio not in (None, ""):
        parts.append(f"bonus {bonus_ratio}")
    return " / ".join(parts) or "dividend"


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

        cache_key = (
            exchange,
            start_date.isoformat(),
            end_date.isoformat(),
            id(connection),
            id(ak),
        )
        return _TRADE_DAYS_CACHE.get_or_load(
            cache_key,
            lambda: self._load_trade_days(exchange, start_date, end_date),
            ttl_seconds=TRADE_DAYS_CACHE_TTL_SECONDS,
            stale_if_error=True,
        )

    def _load_trade_days(
        self,
        exchange: str,
        start_date: date,
        end_date: date,
    ) -> dict[str, Any]:
        # Try DB first (trade_calendar table)
        try:
            result = self._trade_days_from_db(exchange, start_date, end_date)
            if result["trade_days"]:
                return result
        except Exception as exc:
            logger.debug("trade_calendar DB read failed: %s", exc)

        # Fallback: cached trade calendar in akshare.trade_cal
        try:
            cached_dates = get_cached_trade_dates(start_date, end_date)
            if cached_dates:
                return {
                    "exchange": exchange,
                    "trade_days": [_format_date_value(item) for item in cached_dates],
                    "source": "akshare_trade_cal_cache",
                }
        except Exception as exc:
            logger.debug("cached trade calendar fallback failed: %s", exc)

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
                days = [_format_date_value(item) for item in df.loc[mask, "trade_date"].tolist()]
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
            _format_date_value(r[0])
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

        cache_key = (
            start_date.isoformat(),
            end_date.isoformat(),
            event_type or "*",
            id(connection),
            id(ak),
        )
        return _EVENTS_CACHE.get_or_load(
            cache_key,
            lambda: self._load_events(start_date, end_date, event_type),
            ttl_seconds=EVENTS_CACHE_TTL_SECONDS,
            stale_if_error=True,
        )

    def _load_events(
        self,
        start_date: date,
        end_date: date,
        event_type: str | None,
    ) -> dict[str, Any]:
        events: list[dict[str, Any]] = []
        types = [event_type] if event_type else ["macro", "ipo", "dividend"]

        for et in types:
            try:
                items = self._fetch_events(et, start_date, end_date)
                events.extend(items)
            except Exception as exc:
                logger.warning("Failed to fetch %s events: %s", et, exc)

        events.sort(key=lambda item: item.get("date", ""))
        return {"events": events, "start_date": str(start_date), "end_date": str(end_date)}

    def _fetch_events(self, event_type: str, start: date, end: date) -> list[dict[str, Any]]:
        if event_type == "macro":
            return self._macro_events(start, end) if ak is not None else []
        if event_type == "ipo":
            return self._ipo_events(start, end)
        if event_type == "dividend":
            return self._dividend_events(start, end)
        return []

    def _macro_events(self, start: date, end: date) -> list[dict[str, Any]]:
        try:
            df = ak.news_economic_baidu(date=start.strftime("%Y%m%d"))
            items: list[dict[str, Any]] = []
            for _, row in df.iterrows():
                item_date = _format_date_value(row.get("date", row.get("日期", "")))
                if item_date and (item_date < start.isoformat() or item_date > end.isoformat()):
                    continue
                items.append(
                    {
                        "type": "macro",
                        "date": item_date,
                        "time": str(row.get("time", row.get("时间", ""))),
                        "country": str(row.get("country", row.get("国家", ""))),
                        "title": str(row.get("event", row.get("事件", ""))),
                        "importance": str(row.get("importance", row.get("重要性", ""))),
                    }
                )
                if len(items) >= MAX_EVENTS_PER_TYPE:
                    break
            return items
        except Exception as exc:
            logger.debug("macro events error: %s", exc)
            return []

    def _ipo_events(self, start: date, end: date) -> list[dict[str, Any]]:
        try:
            with connection("tushare") as conn:
                rows = conn.execute(
                    text(
                        "SELECT ts_code, name, ipo_date, issue_price "
                        "FROM new_share "
                        "WHERE ipo_date BETWEEN :start_date AND :end_date "
                        "ORDER BY ipo_date ASC, ts_code ASC"
                    ),
                    {"start_date": start, "end_date": end},
                ).fetchall()

            items: list[dict[str, Any]] = []
            for row in rows[:MAX_EVENTS_PER_TYPE]:
                items.append(
                    {
                        "type": "ipo",
                        "date": _format_date_value(row[2]),
                        "title": str(row[1] or row[0] or ""),
                        "symbol": _format_symbol(row[0]),
                        "price": str(row[3] or ""),
                    }
                )
            return items
        except Exception as exc:
            logger.debug("ipo events error: %s", exc)
            return []

    def _dividend_events(self, start: date, end: date) -> list[dict[str, Any]]:
        try:
            with connection("tushare") as conn:
                rows = conn.execute(
                    text(
                        "SELECT COALESCE(d.ex_date, d.pay_date, d.record_date, d.ann_date) AS event_date, "
                        "d.ts_code, COALESCE(sb.name, d.ts_code) AS stock_name, sb.symbol, "
                        "d.div_cash, d.div_stock, d.bonus_ratio "
                        "FROM dividend d "
                        "LEFT JOIN stock_basic sb ON sb.ts_code = d.ts_code "
                        "WHERE COALESCE(d.ex_date, d.pay_date, d.record_date, d.ann_date) "
                        "BETWEEN :start_date AND :end_date "
                        "ORDER BY event_date ASC, d.ts_code ASC"
                    ),
                    {"start_date": start, "end_date": end},
                ).fetchall()

            items: list[dict[str, Any]] = []
            for row in rows[:MAX_EVENTS_PER_TYPE]:
                items.append(
                    {
                        "type": "dividend",
                        "date": _format_date_value(row[0]),
                        "title": str(row[2] or row[1] or ""),
                        "symbol": str(row[3] or _format_symbol(row[1])),
                        "detail": _format_dividend_detail(row[4], row[5], row[6]),
                    }
                )
            return items
        except Exception as exc:
            logger.debug("dividend events error: %s", exc)
            return []
