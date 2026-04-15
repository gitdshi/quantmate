"""Data and history routes."""

from datetime import date, datetime
from typing import Any, List, Literal, Optional
from fastapi import APIRouter, Depends, Query
from pydantic import BaseModel, Field

from app.api.dependencies.permissions import require_permission
from app.api.services.auth_service import get_current_user, get_current_user_optional
from app.api.models.user import TokenData
from app.api.services.data_service import DataService
from app.api.errors import ErrorCode
from app.api.exception_handlers import APIError
from app.api.pagination import PaginationParams, paginate

router = APIRouter(prefix="/data", tags=["Data"], dependencies=[require_permission("data", "read")])


class SymbolInfo(BaseModel):
    """Symbol information."""

    symbol: str
    name: str
    exchange: str
    vt_symbol: str
    industry: Optional[str] = None
    list_date: Optional[date] = None


class OHLCBar(BaseModel):
    """OHLC bar data."""

    datetime: datetime
    open: float
    high: float
    low: float
    close: float
    volume: float
    amount: Optional[float] = None


class IndicatorData(BaseModel):
    """Indicator data."""

    datetime: datetime
    value: float
    name: str


class TushareTableFilter(BaseModel):
    column: str
    operator: Literal["eq", "ne", "gt", "gte", "lt", "lte", "like", "in", "between", "is_null", "is_not_null"]
    value: Optional[Any] = None
    values: Optional[list[Any]] = None


class TushareTableRowsRequest(BaseModel):
    page: int = Field(1, ge=1)
    page_size: int = Field(50, ge=1, le=100)
    sort_by: Optional[str] = None
    sort_dir: Literal["asc", "desc"] = "desc"
    filters: list[TushareTableFilter] = Field(default_factory=list)


@router.get("/symbols", response_model=List[SymbolInfo])
async def list_symbols(
    exchange: Optional[str] = Query(None, description="Filter by exchange (SZSE, SSE)"),
    keyword: Optional[str] = Query(None, description="Search by symbol or name"),
    limit: int = Query(100, le=1000),
    offset: int = Query(0, ge=0),
    current_user: Optional[TokenData] = Depends(get_current_user_optional),
):
    """List available symbols."""
    service = DataService()
    return service.get_symbols(exchange=exchange, keyword=keyword, limit=limit, offset=offset)


@router.get("/history/{vt_symbol}")
async def get_history(
    vt_symbol: str,
    start_date: date = Query(..., description="Start date"),
    end_date: date = Query(..., description="End date"),
    interval: str = Query("daily", description="Interval: daily, weekly, monthly"),
    pagination: PaginationParams = Depends(),
    current_user: Optional[TokenData] = Depends(get_current_user_optional),
):
    """Get historical OHLC data for a symbol (paginated)."""
    service = DataService()

    try:
        bars = service.get_history(vt_symbol, start_date, end_date, interval)
        total = len(bars)
        page_data = bars[pagination.offset : pagination.offset + pagination.limit]
        return paginate(page_data, total, pagination)
    except ValueError as e:
        raise APIError(status_code=400, code=ErrorCode.DATA_INVALID_DATE_RANGE, message=str(e))
    except Exception as e:
        raise APIError(
            status_code=500, code=ErrorCode.DATA_FETCH_FAILED, message="Failed to fetch history", detail=str(e)
        )


@router.get("/indicators/{vt_symbol}")
async def get_indicators(
    vt_symbol: str,
    start_date: date = Query(...),
    end_date: date = Query(...),
    indicators: str = Query("ma_10,ma_20,ma_60", description="Comma-separated indicator names"),
    current_user: Optional[TokenData] = Depends(get_current_user_optional),
):
    """Get computed indicators for a symbol."""
    service = DataService()
    indicator_list = [i.strip() for i in indicators.split(",")]

    try:
        data = service.get_indicators(vt_symbol, start_date, end_date, indicator_list)
        return data
    except ValueError as e:
        raise APIError(status_code=400, code=ErrorCode.DATA_INVALID_DATE_RANGE, message=str(e))


@router.get("/overview")
async def get_market_overview(current_user: Optional[TokenData] = Depends(get_current_user_optional)):
    """Get realtime market index overview."""
    service = DataService()
    try:
        return {"indexes": service.get_index_overview(), "stats": service.get_market_overview()}
    except Exception as e:
        raise APIError(
            status_code=500,
            code=ErrorCode.DATA_FETCH_FAILED,
            message="Failed to fetch market overview",
            detail=str(e),
        )


@router.get("/sectors")
async def get_sectors(current_user: Optional[TokenData] = Depends(get_current_user_optional)):
    """Get sector information."""
    service = DataService()
    return service.get_sectors()


@router.get("/exchanges")
async def get_exchanges(current_user: Optional[TokenData] = Depends(get_current_user_optional)):
    """Get exchange-level stock groupings (SSE, SZSE, BSE)."""
    service = DataService()
    return service.get_exchanges()


@router.get("/indexes")
async def get_indexes(current_user: Optional[TokenData] = Depends(get_current_user_optional)):
    """Return available index codes from AkShare for benchmark selection."""
    service = DataService()
    return service.get_indexes()


@router.get("/quote")
async def get_realtime_quote(
    symbol: str = Query(..., description="Symbol code, e.g. 000001, AAPL, BTCUSD"),
    market: str = Query("CN", description="Market: CN, HK, US, FX, FUTURES, CRYPTO"),
    current_user: Optional[TokenData] = Depends(get_current_user_optional),
):
    """Get realtime spot quote for a symbol."""
    service = DataService()
    try:
        return service.get_realtime_quote(symbol=symbol, market=market)
    except PermissionError as e:
        raise APIError(status_code=403, code=ErrorCode.DATA_SOURCE_UNAVAILABLE, message=str(e))
    except ValueError as e:
        raise APIError(status_code=400, code=ErrorCode.DATA_INVALID_SYMBOL, message=str(e))
    except Exception as e:
        raise APIError(
            status_code=500,
            code=ErrorCode.DATA_FETCH_FAILED,
            message="Failed to fetch realtime quote",
            detail=str(e),
        )


@router.get("/quote/series")
async def get_realtime_quote_series(
    symbol: str = Query(..., description="Symbol code, e.g. 000001, AAPL, BTCUSD"),
    market: str = Query("CN", description="Market: CN, HK, US, FX, FUTURES, CRYPTO"),
    start_ts: Optional[int] = Query(None, description="Epoch seconds start"),
    end_ts: Optional[int] = Query(None, description="Epoch seconds end"),
    current_user: Optional[TokenData] = Depends(get_current_user_optional),
):
    """Get cached intraday realtime quote series."""
    service = DataService()
    try:
        return service.get_realtime_series(symbol=symbol, market=market, start_ts=start_ts, end_ts=end_ts)
    except Exception as e:
        raise APIError(
            status_code=500,
            code=ErrorCode.DATA_FETCH_FAILED,
            message="Failed to fetch realtime series",
            detail=str(e),
        )


@router.get("/tushare/tables")
async def list_tushare_tables(
    keyword: Optional[str] = Query(None, description="Optional table-name keyword filter"),
    current_user: Optional[TokenData] = Depends(get_current_user_optional),
):
    """List physical tables in the Tushare database."""
    service = DataService()
    return {"data": service.list_tushare_tables(keyword=keyword)}


@router.get("/tushare/tables/{table_name}/schema")
async def get_tushare_table_schema(
    table_name: str,
    current_user: Optional[TokenData] = Depends(get_current_user_optional),
):
    """Return column metadata for a Tushare table."""
    service = DataService()
    try:
        return service.get_tushare_table_schema(table_name)
    except ValueError as e:
        raise APIError(status_code=400, code=ErrorCode.VALIDATION_ERROR, message=str(e))
    except Exception as e:
        raise APIError(
            status_code=500,
            code=ErrorCode.DATA_FETCH_FAILED,
            message="Failed to fetch Tushare table schema",
            detail=str(e),
        )


@router.post("/tushare/tables/{table_name}/rows")
async def query_tushare_table_rows(
    table_name: str,
    body: TushareTableRowsRequest,
    current_user: Optional[TokenData] = Depends(get_current_user_optional),
):
    """Return paginated rows from a Tushare table with structured filters."""
    service = DataService()
    try:
        return service.query_tushare_rows(
            table_name,
            page=body.page,
            page_size=body.page_size,
            sort_by=body.sort_by,
            sort_dir=body.sort_dir,
            filters=[item.model_dump(exclude_none=True) for item in body.filters],
        )
    except ValueError as e:
        raise APIError(status_code=400, code=ErrorCode.VALIDATION_ERROR, message=str(e))
    except Exception as e:
        raise APIError(
            status_code=500,
            code=ErrorCode.DATA_FETCH_FAILED,
            message="Failed to query Tushare table rows",
            detail=str(e),
        )


@router.get("/symbols-by-filter")
async def get_symbols_by_filter(
    industry: Optional[str] = Query(None, description="Filter by industry name"),
    exchange: Optional[str] = Query(None, description="Filter by exchange: SSE, SZSE, BSE"),
    limit: int = Query(500, le=2000),
    current_user: Optional[TokenData] = Depends(get_current_user_optional),
):
    """
    Get symbol list filtered by industry and/or exchange.
    Returns ts_code, name, industry, exchange for use in bulk backtest symbol picker.
    """
    service = DataService()
    return service.get_symbols_by_filter(industry=industry, exchange=exchange, limit=limit)


# ── Data Quality / Cleaning endpoints ────────────────────────────────────


@router.get("/quality/missing-dates")
async def check_missing_dates(
    symbol: str = Query(..., description="TS code, e.g. 000001.SZ"),
    start_date: date = Query(...),
    end_date: date = Query(...),
    table: str = Query("stock_daily"),
    current_user: TokenData = Depends(get_current_user),
):
    """Detect missing trading dates for a symbol."""
    from app.domains.extdata.data_cleaning_service import DataCleaningService

    svc = DataCleaningService()
    return svc.detect_missing_dates(symbol, start_date, end_date, table)


@router.get("/quality/anomalies")
async def check_price_anomalies(
    symbol: str = Query(...),
    threshold_pct: float = Query(20.0, ge=1, le=100),
    table: str = Query("stock_daily"),
    current_user: TokenData = Depends(get_current_user),
):
    """Detect abnormal daily price changes."""
    from app.domains.extdata.data_cleaning_service import DataCleaningService

    svc = DataCleaningService()
    return svc.detect_price_anomalies(symbol, threshold_pct, table)


@router.get("/quality/ohlc-check")
async def check_ohlc_consistency(
    symbol: str = Query(...),
    table: str = Query("stock_daily"),
    current_user: TokenData = Depends(get_current_user),
):
    """Verify OHLC data consistency."""
    from app.domains.extdata.data_cleaning_service import DataCleaningService

    svc = DataCleaningService()
    return svc.check_ohlc_consistency(symbol, table)


@router.get("/quality/summary")
async def data_quality_summary(
    symbol: str = Query(...),
    start_date: date = Query(...),
    end_date: date = Query(...),
    current_user: TokenData = Depends(get_current_user),
):
    """Run all data quality checks and return a score."""
    from app.domains.extdata.data_cleaning_service import DataCleaningService

    svc = DataCleaningService()
    return svc.summary(symbol, start_date, end_date)


@router.get("/history-external/{market}/{symbol}")
async def get_external_history(
    market: str,
    symbol: str,
    start_date: date = Query(...),
    end_date: date = Query(...),
    current_user: Optional[TokenData] = Depends(get_current_user_optional),
):
    """Get historical K-line data for non-CN markets (HK/US from tushare DB, CRYPTO/FUTURES via AkShare)."""
    from app.domains.market.external_history_service import ExternalHistoryService

    svc = ExternalHistoryService()
    try:
        return svc.get_history(market=market, symbol=symbol, start_date=start_date, end_date=end_date)
    except ValueError as e:
        raise APIError(status_code=400, code=ErrorCode.DATA_INVALID_SYMBOL, message=str(e))
    except Exception as e:
        raise APIError(
            status_code=500,
            code=ErrorCode.DATA_FETCH_FAILED,
            message="Failed to fetch external history",
            detail=str(e),
        )
