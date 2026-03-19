"""Trade log query/export API routes (Issue: Trade Audit Log)."""

from __future__ import annotations

import csv
import io
import json
from datetime import date
from typing import Optional

from fastapi import APIRouter, Depends, Query
from fastapi.responses import StreamingResponse

from app.api.services.auth_service import get_current_user
from app.api.models.user import TokenData
from app.api.pagination import PaginationParams, paginate

router = APIRouter(prefix="/reports/trade-logs", tags=["Reports"])


@router.get("")
async def query_trade_logs(
    symbol: Optional[str] = Query(None),
    event_type: Optional[str] = Query(None),
    direction: Optional[str] = Query(None),
    strategy_id: Optional[int] = Query(None),
    start_date: Optional[date] = Query(None),
    end_date: Optional[date] = Query(None),
    pagination: PaginationParams = Depends(),
    current_user: TokenData = Depends(get_current_user),
):
    """Query trade logs with filters and pagination."""
    from app.domains.market.dao.trade_log_dao import TradeLogDao

    dao = TradeLogDao()
    total = dao.count(symbol=symbol, event_type=event_type, direction=direction, strategy_id=strategy_id)
    rows = dao.query(
        symbol=symbol,
        event_type=event_type,
        direction=direction,
        strategy_id=strategy_id,
        start_date=start_date,
        end_date=end_date,
        limit=pagination.limit,
        offset=pagination.offset,
    )
    items = []
    for r in rows:
        item = dict(r)
        if item.get("timestamp"):
            item["timestamp"] = item["timestamp"].isoformat()
        items.append(item)
    return paginate(items, total, pagination)


@router.get("/export")
async def export_trade_logs(
    format: str = Query("csv", description="csv or json"),
    symbol: Optional[str] = Query(None),
    event_type: Optional[str] = Query(None),
    limit: int = Query(10000, le=50000),
    current_user: TokenData = Depends(get_current_user),
):
    """Export trade logs as CSV or JSON."""
    from app.domains.market.dao.trade_log_dao import TradeLogDao

    dao = TradeLogDao()
    rows = dao.query(symbol=symbol, event_type=event_type, limit=limit, offset=0)
    items = []
    for r in rows:
        item = dict(r)
        if item.get("timestamp"):
            item["timestamp"] = item["timestamp"].isoformat()
        items.append(item)

    if format == "csv":
        output = io.StringIO()
        if items:
            writer = csv.DictWriter(output, fieldnames=items[0].keys())
            writer.writeheader()
            for item in items:
                writer.writerow(item)
        output.seek(0)
        return StreamingResponse(
            iter([output.getvalue()]),
            media_type="text/csv",
            headers={"Content-Disposition": "attachment; filename=trade_logs.csv"},
        )

    return StreamingResponse(
        iter([json.dumps(items, ensure_ascii=False, indent=2)]),
        media_type="application/json",
        headers={"Content-Disposition": "attachment; filename=trade_logs.json"},
    )
