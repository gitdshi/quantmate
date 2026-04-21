"""Static catalog-backed Tushare interfaces.

This module keeps interface registration code-defined rather than profile-driven.
Missing catalog items are registered through static specs and share a generic
sync implementation backed by code-owned DDL and DAO insert helpers.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date
import logging

from app.datasync.base import BaseIngestInterface, InterfaceInfo, SyncResult, SyncStatus
from app.datasync.sources.tushare.catalog_manifest import CATALOG_ROWS
from app.datasync.sources.tushare import ddl
from app.datasync.sources.tushare.sync_error_handling import handle_tushare_sync_exception

logger = logging.getLogger(__name__)

_TRADE_DATE_APIS = {
    "hsgt_top10",
    "hsgt_stk_hold",
    "hsgt_cash_flow",
    "daily_basic",
    "index_dailybasic",
    "moneyflow",
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

_NONEMPTY_TRADING_DAY_APIS = {
    "hsgt_top10",
    "hsgt_stk_hold",
    "hsgt_cash_flow",
    "daily_basic",
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
    "cyq_chips",
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
        if api_name in _TRADE_DATE_APIS:
            return "trade_date"
        if api_name in _ANN_DATE_APIS:
            return "ann_date"
        if api_name in _END_DATE_APIS:
            return "end_date"
        return None

    def _range_params(self) -> tuple[str, str] | None:
        if self._spec.api_name in _RANGE_APIS:
            return ("start_date", "end_date")
        return None

    def _build_params(self, start: date, end: date) -> dict[str, object]:
        params: dict[str, object] = {}
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
        date_param = self._date_param()
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
            rows = insert_catalog_rows(
                self.info.target_table,
                df,
                key_fields=self._payload_key_fields(),
            )
            return SyncResult(SyncStatus.SUCCESS, rows)
        except Exception as exc:
            return handle_tushare_sync_exception(
                logger,
                f"catalog sync for {self._spec.interface_key} via {self._spec.api_name} with {params}",
                exc,
                allow_permission_partial=True,
            )


def _normalize_permission(raw: str | None) -> str | None:
    if raw in {None, "NULL", ""}:
        return "0"

    normalized = str(raw).strip().strip("'").lower()
    if normalized == "paid":
        return "1"
    if normalized in {"1", "true", "yes", "on", "y"}:
        return "1"
    return "0"


def _build_specs() -> tuple[TushareCatalogSpec, ...]:
    return tuple(
        TushareCatalogSpec(
            interface_key=item_key,
            display_name=display_name,
            api_name=api_name,
            target_table=target_table,
            sync_priority=sync_priority,
            requires_permission=_normalize_permission(requires_permission),
        )
        for item_key, display_name, api_name, target_table, sync_priority, requires_permission in CATALOG_ROWS
    )


def build_catalog_interfaces(existing_keys: set[str] | None = None) -> list[BaseIngestInterface]:
    seen = existing_keys or set()
    return [TushareCatalogInterface(spec) for spec in TUSHARE_CATALOG_SPECS if spec.interface_key not in seen]

TUSHARE_CATALOG_SPECS = _build_specs()