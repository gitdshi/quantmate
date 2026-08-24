"""Autopilot schedule: default stage trigger times and trade-day detection."""

from __future__ import annotations

from datetime import date, datetime, time, timedelta
from zoneinfo import ZoneInfo

from app.domains.autopilot.state import Stage
from app.domains.market.calendar_service import CalendarService

CN_TZ = ZoneInfo("Asia/Shanghai")

# Fixed trigger times (24h, Asia/Shanghai) per stage.
DEFAULT_SCHEDULE: dict[Stage, tuple[int, int]] = {
    Stage.QLIB_INGEST: (2, 30),
    Stage.FACTOR_MINING: (3, 0),
    Stage.FACTOR_BACKTEST: (3, 30),
    Stage.FACTOR_SELECTION: (4, 0),
    Stage.STRATEGY_DEPLOY: (4, 30),
    Stage.PREMARKET_CHECK: (8, 0),
    Stage.SETTLEMENT: (15, 30),
    Stage.ANALYSIS: (16, 0),
    Stage.STRATEGY_ADJUST: (17, 0),
}


def now_cn() -> datetime:
    return datetime.now(CN_TZ)


def business_date_cn(dt: datetime | None = None) -> date:
    return (dt or now_cn()).date()


def stage_trigger_time(stage: Stage, business_date: date) -> datetime:
    hour, minute = DEFAULT_SCHEDULE[stage]
    return datetime(business_date.year, business_date.month, business_date.day, hour, minute, tzinfo=CN_TZ)


def is_trading_day(business_date: date) -> bool:
    result = CalendarService().get_trade_days(
        exchange="SSE",
        start_date=business_date - timedelta(days=3),
        end_date=business_date + timedelta(days=1),
    )
    iso = business_date.isoformat()
    return iso in (result.get("trade_days") or [])


def default_schedule() -> dict[str, tuple[int, int]]:
    return {stage.value: times for stage, times in DEFAULT_SCHEDULE.items()}