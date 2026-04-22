"""Database-backed catalog Tushare interfaces."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date
import logging

from sqlalchemy import text

from app.datasync.base import BaseIngestInterface, InterfaceInfo, SyncResult, SyncStatus
from app.datasync.sources.tushare import ddl
from app.datasync.sources.tushare.sync_error_handling import handle_tushare_sync_exception
from app.infrastructure.db.connections import get_quantmate_engine

logger = logging.getLogger(__name__)

_TUSHARE_CATALOG_SQL = """
SELECT
    item_key,
    item_name,
    COALESCE(NULLIF(TRIM(api_name), ''), item_key) AS api_name,
    COALESCE(NULLIF(TRIM(target_table), ''), item_key) AS target_table,
    sync_priority,
    requires_permission
FROM data_source_items
WHERE source = 'tushare'
ORDER BY sync_priority, item_key
"""

_TUSHARE_CATALOG_LEGACY_SQL = """
SELECT
    item_key,
    item_name,
    item_key AS api_name,
    item_key AS target_table,
    sync_priority,
    requires_permission
FROM data_source_items
WHERE source = 'tushare'
ORDER BY sync_priority, item_key
"""

_FALLBACK_CATALOG_ROWS = (
    ("stock_basic", "股票基础列表", "stock_basic", "stock_basic", 10, "0"),
    ("stock_daily", "A股日线行情", "daily", "stock_daily", 20, "0"),
    ("bak_daily", "备用行情", "bak_daily", "bak_daily", 22, "0"),
    ("suspend_d", "每日停复牌信息", "suspend_d", "suspend_d", 23, "0"),
    ("suspend", "停复牌历史", "suspend", "suspend", 24, "0"),
    ("moneyflow", "个股资金流向", "moneyflow", "moneyflow", 25, "0"),
    ("stock_weekly", "周线行情", "weekly", "stock_weekly", 25, "0"),
    ("stock_monthly", "月线行情", "monthly", "stock_monthly", 26, "0"),
    ("adj_factor", "复权因子", "adj_factor", "adj_factor", 30, "0"),
    ("daily_basic", "每日指标数据", "daily_basic", "daily_basic", 31, "0"),
    ("fina_indicator", "财务指标数据", "fina_indicator", "fina_indicator", 55, "0"),
    ("income", "利润表", "income", "income", 56, "0"),
    ("limit_list_d", "涨跌停统计", "limit_list_d", "limit_list_d", 70, "0"),
    ("margin_detail", "融资融券明细", "margin_detail", "margin_detail", 80, "0"),
    ("margin", "融资融券", "margin", "margin", 81, "0"),
    ("block_trade", "大宗交易", "block_trade", "block_trade", 90, "0"),
    ("hsgt_top10", "沪深股通成份股", "hsgt_top10", "hsgt_top10", 100, "0"),
    ("namechange", "股票曾用名", "namechange", "namechange", 103, "0"),
    ("new_share", "IPO新股列表", "new_share", "new_share", 104, "0"),
    ("stk_holdertrade", "董监高持股", "stk_holdertrade", "stk_holdertrade", 109, "0"),
    ("balancesheet", "资产负债表", "balancesheet", "balancesheet", 201, "0"),
    ("cashflow", "现金流量表", "cashflow", "cashflow", 203, "0"),
    ("forecast", "业绩预告", "forecast", "forecast", 205, "0"),
    ("express", "业绩快报", "express", "express", 206, "0"),
    ("fina_audit", "财务审计意见", "fina_audit", "fina_audit", 208, "0"),
    ("fina_mainbz", "主营业务构成", "fina_mainbz", "fina_mainbz", 209, "0"),
    ("disclosure_date", "财报披露计划", "disclosure_date", "disclosure_date", 211, "0"),
    ("index_monthly", "指数月线行情", "index_monthly", "index_monthly", 401, "0"),
    ("index_dailybasic", "大盘指数每日指标", "index_dailybasic", "index_dailybasic", 403, "0"),
    ("stock_st", "ST股票列表", "stock_st", "stock_st", 1001, "0"),
)

_TRADE_DATE_APIS = {
    "daily",
    "bak_daily",
    "hsgt_top10",
    "hsgt_stk_hold",
    "hsgt_cash_flow",
    "daily_basic",
    "index_dailybasic",
    "moneyflow",
    "adj_factor",
    "margin_detail",
    "margin",
    "limit_list",
    "limit_list_d",
    "digital_currency",
    "cyq_perf",
    "report_rc",
    "index_monthly",
    "fund_daily",
    "fund_adj",
    "etf_daily",
    "etf_weight",
    "etf_nav",
    "fut_daily",
    "fut_holding",
    "fut_wsr",
    "fut_settle",
    "opt_daily",
    "opt_daily_s",
    "cb_daily",
    "fx_daily",
    "hk_daily",
    "weekly",
    "monthly",
    "stk_factor_pro",
    "us_daily",
    "industry_daily",
    "industry_moneyflow",
    "capital_flow",
    "daily_info",
    "block_trade",
}

_ANN_DATE_APIS = {
    "income",
    "income_vip",
    "balancesheet",
    "balancesheet_vip",
    "cashflow",
    "cashflow_vip",
    "forecast",
    "express",
    "fina_indicator",
    "fina_indicator_vip",
    "fina_audit",
    "disclosure_date",
}

_END_DATE_APIS = {
    "fina_mainbz",
    "fina_mainbz_vip",
}

_RANGE_APIS = {
    "new_share",
    "ipo",
}

_KEY_DATE_OVERRIDES = {
    "namechange": "end_date",
    "repurchase": "end_date",
    "report_rc": "report_date",
}

_REQUEST_DATE_OVERRIDES = {
    "suspend": "suspend_date",
}

_DEFAULT_API_PARAMS = {
    "stock_basic": {"list_status": "L"},
}

_NONEMPTY_TRADING_DAY_APIS = {
    "daily",
    "bak_daily",
    "hsgt_top10",
    "hsgt_stk_hold",
    "hsgt_cash_flow",
    "daily_basic",
    "moneyflow",
    "adj_factor",
    "index_dailybasic",
    "margin_detail",
    "limit_list",
    "limit_list_d",
    "fund_daily",
    "etf_daily",
    "fut_daily",
    "cb_daily",
    "fx_daily",
    "hk_daily",
    "stk_factor_pro",
    "us_daily",
    "industry_daily",
}

# These catalog interfaces stay registered so bootstrap code can still resolve
# DDL and metadata, but the runtime scheduler should not execute them. Keep this
# list small and canonical so auto-sync support is driven by the current CSV
# catalog rather than stale aliases.
_RUNTIME_UNSUPPORTED_INTERFACE_KEYS = {
    "bo_monthly",
    "bo_weekly",
    "fund_div",
    "fund_nav",
    "fund_portfolio",
    "index_weight",
    "pledge_detail",
}


@dataclass(frozen=True)
class TushareCatalogSpec:
    interface_key: str
    display_name: str
    api_name: str
    target_table: str
    sync_priority: int
    requires_permission: str | None = None


class TushareCatalogInterface(BaseIngestInterface):
    def __init__(self, spec: TushareCatalogSpec):
        self._spec = spec

    @property
    def info(self) -> InterfaceInfo:
        return InterfaceInfo(
            interface_key=self._spec.interface_key,
            display_name=self._spec.display_name,
            source_key="tushare",
            target_database="tushare",
            target_table=self._spec.target_table,
            sync_priority=self._spec.sync_priority,
            requires_permission=self._spec.requires_permission,
            enabled_by_default=False,
            description=f"Tushare catalog interface: {self._spec.api_name}",
        )

    def get_ddl(self) -> str:
        return ddl.get_catalog_ddl(self.info.target_table)

    def supports_scheduled_sync(self) -> bool:
        return self._spec.interface_key not in _RUNTIME_UNSUPPORTED_INTERFACE_KEYS

    def should_ensure_table_before_sync(self) -> bool:
        return not ddl.uses_sample_inferred_schema(self.info.target_table)

    def supports_backfill(self) -> bool:
        if not self.supports_scheduled_sync():
            return False
        return self._date_param() is not None or self._range_params() is not None

    def backfill_mode(self) -> str:
        if self._range_params() is not None:
            return "range"
        return "date"

    def requires_nonempty_trading_day_data(self) -> bool:
        return self._spec.api_name in _NONEMPTY_TRADING_DAY_APIS

    def sync_date(self, trade_date: date) -> SyncResult:
        params = self._build_params(trade_date, trade_date)
        return self._sync_with_params(params)

    def sync_range(self, start: date, end: date) -> SyncResult:
        range_params = self._range_params()
        if not self.supports_backfill():
            return self.sync_date(end)
        if range_params is None:
            return super().sync_range(start, end)
        return self._sync_with_params(self._build_params(start, end))

    def _date_param(self) -> str | None:
        api_name = self._spec.api_name
        override = _REQUEST_DATE_OVERRIDES.get(api_name)
        if override is not None:
            return override
        if api_name in _TRADE_DATE_APIS:
            return "trade_date"
        if api_name in _ANN_DATE_APIS:
            return "ann_date"
        if api_name in _END_DATE_APIS:
            return "end_date"
        return None

    def _schema_date_column(self) -> str | None:
        return _KEY_DATE_OVERRIDES.get(self._spec.api_name) or self._date_param()

    def _range_params(self) -> tuple[str, str] | None:
        if self._spec.api_name in _RANGE_APIS:
            return ("start_date", "end_date")
        return None

    def _build_params(self, start: date, end: date) -> dict[str, object]:
        params: dict[str, object] = dict(_DEFAULT_API_PARAMS.get(self._spec.api_name, {}))
        range_params = self._range_params()
        if range_params is not None:
            start_param, end_param = range_params
            params[start_param] = start.strftime("%Y%m%d")
            params[end_param] = end.strftime("%Y%m%d")
            return params

        date_param = self._date_param()
        if date_param is not None:
            params[date_param] = end.strftime("%Y%m%d")
        return params

    def _payload_key_fields(self) -> tuple[str, ...]:
        date_param = self._schema_date_column()
        fields = ["ts_code"]
        if date_param is not None:
            fields.append(date_param)
        fields.extend(["symbol", "code", "exchange", "market", "name"])
        return tuple(dict.fromkeys(fields))

    def _sync_with_params(self, params: dict[str, object]) -> SyncResult:
        from app.datasync.service.tushare_ingest import call_pro
        from app.domains.extdata.dao.tushare_dao import insert_catalog_rows

        try:
            df = call_pro(self._spec.api_name, **params)
            if df is None or getattr(df, "empty", True):
                return SyncResult(SyncStatus.SUCCESS, 0)
            inferred_schema = self._ensure_inferred_table(df)
            rows = insert_catalog_rows(
                self.info.target_table,
                df,
                key_fields=self._payload_key_fields(),
                column_specs=None if inferred_schema is None else list(inferred_schema["column_specs"]),
                key_columns=None if inferred_schema is None else tuple(inferred_schema["key_columns"]),
            )
            return SyncResult(SyncStatus.SUCCESS, rows)
        except Exception as exc:
            return handle_tushare_sync_exception(
                logger,
                f"catalog sync for {self._spec.interface_key} via {self._spec.api_name} with {params}",
                exc,
                allow_permission_partial=True,
            )

    def _ensure_inferred_table(self, rows) -> dict[str, object] | None:
        if self.should_ensure_table_before_sync():
            return None
        if rows is None or getattr(rows, "empty", True):
            return None

        from app.datasync.table_manager import ensure_inferred_table

        schema = ddl.infer_dynamic_table_schema(
            self.info.target_table,
            rows,
            preferred_date_column=self._schema_date_column(),
            preferred_key_fields=self._payload_key_fields(),
        )
        ensure_inferred_table(self.info.target_database, self.info.target_table, schema)
        return schema


def _normalize_permission(raw: str | None) -> str | None:
    if raw in {None, "NULL", ""}:
        return "0"

    normalized = str(raw).strip().strip("'").lower()
    if normalized == "paid":
        return "1"
    if normalized in {"1", "true", "yes", "on", "y"}:
        return "1"
    return "0"


def _is_unknown_data_source_items_column_error(exc: Exception) -> bool:
    return "unknown column" in str(exc or "").lower()


def _build_specs(rows: list[tuple]) -> tuple[TushareCatalogSpec, ...]:
    return tuple(
        TushareCatalogSpec(
            interface_key=item_key,
            display_name=display_name,
            api_name=api_name,
            target_table=target_table,
            sync_priority=sync_priority,
            requires_permission=_normalize_permission(requires_permission),
        )
        for item_key, display_name, api_name, target_table, sync_priority, requires_permission in rows
        if str(item_key or "").strip()
    )


def _fetch_catalog_rows() -> list[tuple]:
    engine = get_quantmate_engine()
    with engine.connect() as conn:
        try:
            result = conn.execute(text(_TUSHARE_CATALOG_SQL)).fetchall()
        except Exception as exc:
            if not _is_unknown_data_source_items_column_error(exc):
                raise
            logger.warning("Falling back to legacy Tushare catalog query: %s", exc)
            result = conn.execute(text(_TUSHARE_CATALOG_LEGACY_SQL)).fetchall()
    return [
        (
            row[0],
            row[1],
            row[2],
            row[3],
            int(row[4] or 100),
            row[5],
        )
        for row in result
    ]


def _load_catalog_specs() -> tuple[TushareCatalogSpec, ...]:
    try:
        specs = _build_specs(_fetch_catalog_rows())
        if specs:
            return specs
        logger.warning("Tushare catalog query returned no rows; using bundled fallback catalog")
    except Exception:
        logger.exception("Failed to load Tushare catalog rows from data_source_items")
    return _build_specs(list(_FALLBACK_CATALOG_ROWS))


def build_catalog_interfaces(existing_keys: set[str] | None = None) -> list[BaseIngestInterface]:
    seen = existing_keys or set()
    interfaces: list[BaseIngestInterface] = []
    for spec in _load_catalog_specs():
        if spec.interface_key in seen:
            continue
        if spec.interface_key == "cyq_chips":
            from app.datasync.sources.tushare.interfaces import TushareCyqChipsInterface

            interfaces.append(TushareCyqChipsInterface())
            continue
        interfaces.append(TushareCatalogInterface(spec))
    return interfaces
