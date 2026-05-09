"""Tushare ingest interface implementations.

Each interface wraps existing ingest functions from tushare_ingest.py,
adapting them to the BaseIngestInterface contract.
"""

from __future__ import annotations

import logging
from datetime import date

import pandas as pd
from sqlalchemy import text

from app.datasync.base import BaseIngestInterface, InterfaceInfo, SyncResult, SyncStatus
from app.datasync.sources.tushare.sync_error_handling import handle_tushare_sync_exception, is_permission_error
from app.datasync.sources.tushare.catalog_interfaces import _DEFAULT_API_PARAMS, TushareCatalogInterface, TushareCatalogSpec
from app.datasync.sources.tushare import ddl
from app.infrastructure.db.connections import get_tushare_engine

logger = logging.getLogger(__name__)

# Major indices synced by default
INDEX_CODES = ["000001.SH", "399001.SZ", "399006.SZ", "000300.SH", "000905.SH"]


def _load_distinct_table_values(table_name: str, column_names: tuple[str, ...]) -> list[str]:
    engine = get_tushare_engine()
    for column_name in column_names:
        try:
            with engine.connect() as conn:
                rows = conn.execute(
                    text(
                        f"SELECT DISTINCT `{column_name}` FROM `{table_name}` "
                        f"WHERE `{column_name}` IS NOT NULL AND `{column_name}` <> '' "
                        f"ORDER BY `{column_name}`"
                    )
                ).fetchall()
        except Exception:
            continue

        values = [str(row[0]).strip() for row in rows if str(row[0] or "").strip()]
        if values:
            return values
    return []


def _get_fund_codes() -> list[str]:
    cached_codes = _load_distinct_table_values("fund_basic", ("ts_code", "fund_code"))
    if cached_codes:
        return cached_codes

    from app.datasync.service.tushare_ingest import call_pro

    df = call_pro("fund_basic")
    if df is None or getattr(df, "empty", True):
        return []

    for column_name in ("ts_code", "fund_code"):
        if column_name in df.columns:
            return sorted({str(value).strip() for value in df[column_name].tolist() if str(value or "").strip()})
    return []


def _get_hk_codes() -> list[str]:
    cached_codes = _load_distinct_table_values("hk_basic", ("ts_code",))
    if cached_codes:
        return cached_codes

    from app.datasync.service.tushare_ingest import call_pro

    df = call_pro("hk_basic", list_status="L")
    if df is None or getattr(df, "empty", True):
        return []

    if "ts_code" in df.columns:
        return sorted({str(value).strip() for value in df["ts_code"].tolist() if str(value or "").strip()})
    return []


def _get_fut_codes() -> list[str]:
    cached_codes = _load_distinct_table_values("fut_basic", ("ts_code",))
    if cached_codes:
        return cached_codes

    from app.datasync.service.tushare_ingest import call_pro

    exchanges = ("CFFEX", "DCE", "CZCE", "SHFE", "INE", "GFEX")
    codes: set[str] = set()
    for exchange in exchanges:
        df = call_pro("fut_basic", exchange=exchange)
        if df is None or getattr(df, "empty", True) or "ts_code" not in df.columns:
            continue
        codes.update(str(value).strip() for value in df["ts_code"].tolist() if str(value or "").strip())
    return sorted(codes)


def _get_index_codes() -> list[str]:
    cached_codes = _load_distinct_table_values("index_basic", ("index_code", "ts_code"))
    if cached_codes:
        return cached_codes
    return list(INDEX_CODES)


def _get_stock_codes() -> list[str]:
    cached_codes = _load_distinct_table_values("stock_basic", ("ts_code",))
    if cached_codes:
        return cached_codes

    from app.datasync.service.tushare_ingest import get_all_ts_codes

    return get_all_ts_codes()


def _parse_metadata_params(raw: str | None) -> set[str]:
    if not raw:
        return set()
    normalized = str(raw).replace(";", ",").replace("|", ",")
    return {
        part.strip().lower()
        for part in normalized.split(",")
        if part and part.strip()
    }


def _resolve_catalog_entity_binding(spec: TushareCatalogSpec):
    input_params = _parse_metadata_params(spec.input_params)
    interface_key = str(spec.interface_key or "").lower()
    api_name = str(spec.api_name or "").lower()

    special_binding = {
        "rt_etf_k": ("ts_code", _get_fund_codes),
        "rt_etf_sz_iopv": ("ts_code", _get_fund_codes),
        "rt_fut_min": ("ts_code", _get_fut_codes),
        "rt_hk_k": ("ts_code", _get_hk_codes),
        "rt_idx_k": ("ts_code", _get_index_codes),
        "rt_idx_min": ("ts_code", _get_index_codes),
        "rt_sw_k": ("ts_code", _get_index_codes),
    }.get(interface_key) or {
        "rt_etf_k": ("ts_code", _get_fund_codes),
        "rt_etf_sz_iopv": ("ts_code", _get_fund_codes),
        "rt_fut_min": ("ts_code", _get_fut_codes),
        "rt_hk_k": ("ts_code", _get_hk_codes),
        "rt_idx_k": ("ts_code", _get_index_codes),
        "rt_idx_min": ("ts_code", _get_index_codes),
        "rt_sw_k": ("ts_code", _get_index_codes),
    }.get(api_name)
    if special_binding is not None:
        return special_binding

    if "index_code" in input_params:
        return "index_code", _get_index_codes
    if "fund_code" in input_params:
        return "fund_code", _get_fund_codes
    if "ts_code" in input_params:
        if interface_key.startswith(("fund_", "etf_")) or api_name.startswith(("fund_", "etf_")):
            return "ts_code", _get_fund_codes
        return "ts_code", _get_stock_codes
    return None


def _resolve_catalog_date_param(spec: TushareCatalogSpec) -> str | None:
    candidates = _parse_metadata_params(spec.analysis_date_params) or _parse_metadata_params(spec.input_params)
    for param_name in ("trade_date", "ann_date", "end_date", "report_date", "date"):
        if param_name in candidates:
            return param_name
    return None


def _resolve_explicit_key_columns(
    column_specs: list[dict[str, object]],
    preferred_key_fields: tuple[str, ...] | list[str] | None = None,
) -> tuple[str, ...]:
    names = {str(spec["name"]) for spec in column_specs}
    by_source = {
        str(spec["source_fields"][0]): str(spec["name"])
        for spec in column_specs
        if spec.get("source_fields")
    }

    resolved: list[str] = []
    for candidate in preferred_key_fields or ():
        if candidate in by_source:
            resolved.append(by_source[candidate])
            continue
        if candidate in names:
            resolved.append(candidate)
    return tuple(dict.fromkeys(resolved))


def _infer_catalog_schema(
    iface: TushareCatalogInterface,
    rows,
    *,
    preferred_date_column: str | None = None,
    preferred_key_fields: tuple[str, ...] | list[str] | None = None,
) -> dict[str, object]:
    schema = ddl.infer_dynamic_table_schema(
        iface.info.target_table,
        rows,
        preferred_date_column=preferred_date_column,
        preferred_key_fields=preferred_key_fields,
    )

    explicit_key_columns = _resolve_explicit_key_columns(schema["column_specs"], preferred_key_fields)
    if explicit_key_columns:
        schema["key_columns"] = explicit_key_columns
        schema["ddl"] = ddl.build_dynamic_table_ddl(
            iface.info.target_table,
            schema["column_specs"],
            explicit_key_columns,
        )
    return schema


def _insert_catalog_dataframe(
    iface: TushareCatalogInterface,
    df: pd.DataFrame,
    inferred_schema: dict[str, object] | None = None,
) -> tuple[dict[str, object] | None, int]:
    from app.domains.extdata.dao.tushare_dao import insert_catalog_rows

    if df is None or getattr(df, "empty", True):
        return inferred_schema, 0

    if inferred_schema is None:
        inferred_schema = iface._ensure_inferred_table(df)

    rows = insert_catalog_rows(
        iface.info.target_table,
        df,
        key_fields=iface._payload_key_fields(),
        column_specs=None if inferred_schema is None else list(inferred_schema["column_specs"]),
        key_columns=None if inferred_schema is None else tuple(inferred_schema["key_columns"]),
    )
    return inferred_schema, rows


def _build_entity_sync_details(
    entities: list[str],
    processed_count: int,
    failed_entities: list[str],
    *,
    quota_exceeded: bool = False,
    retry_after: float | None = None,
    permission_denied: bool = False,
) -> dict[str, object]:
    details: dict[str, object] = {
        "entity_count": len(entities),
        "processed_count": max(processed_count, 0),
    }
    if failed_entities:
        details["failed_entities"] = list(failed_entities[:20])
        details["failure_count"] = len(failed_entities)
    if quota_exceeded:
        details["quota_exceeded"] = True
    if retry_after is not None:
        details["quota_retry_after"] = str(retry_after)
    if permission_denied:
        details["permission_denied"] = True
    return details


def _catalog_api_name(iface: TushareCatalogInterface) -> str:
    spec = getattr(iface, "_spec", None)
    api_name = getattr(spec, "api_name", None)
    if api_name:
        return str(api_name)
    return str(iface.info.interface_key)


def _sync_catalog_once(
    iface: TushareCatalogInterface,
    *,
    params: dict[str, object] | None = None,
) -> SyncResult:
    from app.datasync.service.tushare_ingest import call_pro

    api_name = _catalog_api_name(iface)
    request_params = dict(_DEFAULT_API_PARAMS.get(api_name, {}))
    request_params.update(params or {})
    try:
        df = call_pro(api_name, **request_params)
        _, rows = _insert_catalog_dataframe(iface, df)
        return SyncResult(SyncStatus.SUCCESS, rows)
    except Exception as exc:
        return handle_tushare_sync_exception(
            logger,
            f"{iface.info.interface_key} sync with {request_params}",
            exc,
            allow_permission_partial=True,
        )


def _sync_catalog_by_entities(
    iface: TushareCatalogInterface,
    entities: list[str],
    params_builder,
) -> SyncResult:
    from app.datasync.service.tushare_ingest import TushareQuotaExceededError, call_pro

    ordered_entities = [entity for entity in entities if str(entity or "").strip()]
    if not ordered_entities:
        return SyncResult(SyncStatus.SUCCESS, 0, details={"entity_count": 0, "processed_count": 0})

    api_name = _catalog_api_name(iface)
    inferred_schema: dict[str, object] | None = None
    total_rows = 0
    processed_count = 0
    failed_entities: list[str] = []

    for entity in ordered_entities:
        params = dict(_DEFAULT_API_PARAMS.get(api_name, {}))
        params.update(params_builder(entity))
        try:
            df = call_pro(api_name, **params)
            processed_count += 1
            inferred_schema, rows = _insert_catalog_dataframe(iface, df, inferred_schema)
            total_rows += rows
        except TushareQuotaExceededError as exc:
            return SyncResult(
                SyncStatus.PENDING,
                total_rows,
                str(exc),
                details=_build_entity_sync_details(
                    ordered_entities,
                    processed_count,
                    failed_entities,
                    quota_exceeded=True,
                    retry_after=getattr(exc, "retry_after", None),
                ),
            )
        except Exception as exc:
            if is_permission_error(str(exc)):
                if total_rows == 0:
                    return handle_tushare_sync_exception(
                        logger,
                        f"{iface.info.interface_key} sync via entity loop",
                        exc,
                        allow_permission_partial=True,
                    )
                return SyncResult(
                    SyncStatus.PARTIAL,
                    total_rows,
                    "Permission denied",
                    details=_build_entity_sync_details(
                        ordered_entities,
                        processed_count,
                        failed_entities,
                        permission_denied=True,
                    ),
                )
            failed_entities.append(str(entity))
            logger.warning("%s sync failed for %s: %s", iface.info.interface_key, entity, exc)

    details = _build_entity_sync_details(ordered_entities, processed_count, failed_entities)
    if failed_entities and total_rows == 0:
        return SyncResult(
            SyncStatus.ERROR,
            0,
            f"Failed entities: {', '.join(failed_entities[:5])}",
            details=details,
        )
    if failed_entities:
        return SyncResult(
            SyncStatus.PARTIAL,
            total_rows,
            f"Failed entities: {', '.join(failed_entities[:5])}",
            details=details,
        )
    return SyncResult(SyncStatus.SUCCESS, total_rows, details=details)


def _merge_sync_results_by_variant(results: list[tuple[str, SyncResult]]) -> SyncResult:
    if not results:
        return SyncResult(SyncStatus.SUCCESS, 0, details={"variant_count": 0})

    total_rows = sum(result.rows_synced for _variant, result in results)
    error_messages = [result.error_message for _variant, result in results if result.error_message]
    variant_statuses = {variant: result.status.value for variant, result in results}
    details: dict[str, object] = {
        "variant_count": len(results),
        "variants": [variant for variant, _result in results],
        "variant_statuses": variant_statuses,
    }

    entity_count = 0
    processed_count = 0
    failure_count = 0
    quota_exceeded = False
    permission_denied = False
    quota_retry_after: str | None = None
    failed_entities: list[str] = []

    for _variant, result in results:
        current_details = result.details or {}
        entity_count = max(entity_count, int(current_details.get("entity_count") or 0))
        processed_count += int(current_details.get("processed_count") or 0)
        failure_count += int(current_details.get("failure_count") or 0)
        quota_exceeded = quota_exceeded or bool(current_details.get("quota_exceeded"))
        permission_denied = permission_denied or bool(current_details.get("permission_denied"))
        quota_retry_after = quota_retry_after or (str(current_details.get("quota_retry_after")) if current_details.get("quota_retry_after") is not None else None)
        failed_entities.extend(str(entity) for entity in (current_details.get("failed_entities") or ()))

    if entity_count:
        details["entity_count"] = entity_count
    if processed_count:
        details["processed_count"] = processed_count
    if failure_count:
        details["failure_count"] = failure_count
    if failed_entities:
        details["failed_entities"] = failed_entities[:20]
    if quota_exceeded:
        details["quota_exceeded"] = True
    if quota_retry_after is not None:
        details["quota_retry_after"] = quota_retry_after
    if permission_denied:
        details["permission_denied"] = True

    message = "; ".join(dict.fromkeys(error_messages[:5])) if error_messages else None
    statuses = [result.status for _variant, result in results]
    if any(status == SyncStatus.PENDING for status in statuses):
        return SyncResult(SyncStatus.PENDING, total_rows, message, details=details)
    if any(status == SyncStatus.ERROR for status in statuses):
        if total_rows > 0:
            return SyncResult(SyncStatus.PARTIAL, total_rows, message, details=details)
        return SyncResult(SyncStatus.ERROR, 0, message, details=details)
    if any(status == SyncStatus.PARTIAL for status in statuses):
        return SyncResult(SyncStatus.PARTIAL, total_rows, message, details=details)
    return SyncResult(SyncStatus.SUCCESS, total_rows, message, details=details)


def _get_table_rows_by_date(table_name: str, date_column: str, start: date, end: date) -> dict[date, int]:
    engine = get_tushare_engine()
    with engine.connect() as conn:
        rows = conn.execute(
            text(
                f"SELECT `{date_column}`, COUNT(*) "
                f"FROM `{table_name}` "
                f"WHERE `{date_column}` BETWEEN :start_date AND :end_date "
                f"GROUP BY `{date_column}`"
            ),
            {"start_date": start, "end_date": end},
        ).fetchall()
    return {row[0]: int(row[1]) for row in rows}


def _normalize_trade_cal_rows(df: pd.DataFrame) -> list[dict[str, object]]:
    if df is None or df.empty:
        return []

    calendar_column = "cal_date" if "cal_date" in df.columns else "calendar_date" if "calendar_date" in df.columns else None
    if calendar_column is None:
        raise ValueError("trade_cal response missing cal_date/calendar_date column")

    normalized = df.copy()
    normalized[calendar_column] = pd.to_datetime(normalized[calendar_column], errors="coerce").dt.date
    if "pretrade_date" in normalized.columns:
        normalized["pretrade_date"] = pd.to_datetime(normalized["pretrade_date"], errors="coerce").dt.date
    else:
        normalized["pretrade_date"] = None

    rows: list[dict[str, object]] = []
    for _, row in normalized.iterrows():
        cal_date = row.get(calendar_column)
        if cal_date is None or pd.isna(cal_date):
            continue
        pretrade_date = row.get("pretrade_date")
        if pretrade_date is not None and pd.isna(pretrade_date):
            pretrade_date = None
        rows.append(
            {
                "exchange": str(row.get("exchange") or "SSE").strip() or "SSE",
                "cal_date": cal_date,
                "is_open": int(row.get("is_open") or 0),
                "pretrade_date": pretrade_date,
            }
        )
    return rows


def _upsert_trade_cal_rows(rows: list[dict[str, object]]) -> int:
    if not rows:
        return 0

    engine = get_tushare_engine()
    with engine.begin() as conn:
        conn.execute(
            text(
                "INSERT INTO trade_cal (exchange, cal_date, is_open, pretrade_date) "
                "VALUES (:exchange, :cal_date, :is_open, :pretrade_date) "
                "ON DUPLICATE KEY UPDATE "
                "is_open = VALUES(is_open), pretrade_date = VALUES(pretrade_date)"
            ),
            rows,
        )
    return len(rows)


def _get_trade_cal_counts(start: date, end: date) -> dict[date, int]:
    engine = get_tushare_engine()
    with engine.connect() as conn:
        rows = conn.execute(
            text(
                "SELECT cal_date, COUNT(*) "
                "FROM trade_cal "
                "WHERE cal_date BETWEEN :start_date AND :end_date "
                "GROUP BY cal_date"
            ),
            {"start_date": start, "end_date": end},
        ).fetchall()
    return {row[0]: int(row[1]) for row in rows}


class TushareTradeCalInterface(BaseIngestInterface):
    @property
    def info(self) -> InterfaceInfo:
        return InterfaceInfo(
            interface_key="trade_cal",
            display_name="交易日历",
            source_key="tushare",
            target_database="tushare",
            target_table="trade_cal",
            sync_priority=5,
            enabled_by_default=True,
            description="交易所交易日历",
        )

    def get_ddl(self) -> str:
        return ddl.TRADE_CAL_DDL

    def backfill_mode(self) -> str:
        return "date"

    def sync_date(self, trade_date: date) -> SyncResult:
        return self.sync_range(trade_date, trade_date)

    def sync_range(self, start: date, end: date) -> SyncResult:
        from app.datasync.service.tushare_ingest import call_pro

        try:
            df = call_pro(
                "trade_cal",
                exchange="SSE",
                start_date=start.strftime("%Y%m%d"),
                end_date=end.strftime("%Y%m%d"),
            )
            rows = _normalize_trade_cal_rows(df)
            upserted = _upsert_trade_cal_rows(rows)
            return SyncResult(SyncStatus.SUCCESS, upserted)
        except Exception as exc:
            return handle_tushare_sync_exception(logger, f"trade_cal sync for {start} -> {end}", exc)

    def get_backfill_rows_by_date(self, start: date, end: date) -> dict[date, int]:
        return _get_trade_cal_counts(start, end)


class TushareStockBasicInterface(BaseIngestInterface):
    def supports_backfill(self) -> bool:
        # stock_basic is a latest snapshot, not a per-trade-date historical feed.
        return False

    @property
    def info(self) -> InterfaceInfo:
        return InterfaceInfo(
            interface_key="stock_basic",
            display_name="股票基本信息",
            source_key="tushare",
            target_database="tushare",
            target_table="stock_basic",
            sync_priority=10,
            enabled_by_default=True,
            description="A股基本资料",
        )

    def get_ddl(self) -> str:
        return ddl.STOCK_BASIC_DDL

    def sync_date(self, trade_date: date) -> SyncResult:
        from app.datasync.service.tushare_ingest import ingest_stock_basic
        from app.domains.extdata.dao.data_sync_status_dao import get_stock_basic_count

        try:
            ingest_stock_basic()
            count = get_stock_basic_count()
            return SyncResult(SyncStatus.SUCCESS, count)
        except Exception as exc:
            return handle_tushare_sync_exception(logger, "stock_basic sync", exc)

    def sync_range(self, start: date, end: date) -> SyncResult:
        # stock_basic is a full snapshot, not date-range-based
        return self.sync_date(end)


class TushareStockCompanyInterface(BaseIngestInterface):
    def supports_backfill(self) -> bool:
        # stock_company is a latest snapshot keyed by ts_code, not a dated historical feed.
        return False

    @property
    def info(self) -> InterfaceInfo:
        return InterfaceInfo(
            interface_key="stock_company",
            display_name="公司基本面",
            source_key="tushare",
            target_database="tushare",
            target_table="stock_company",
            sync_priority=15,
            enabled_by_default=True,
            description="上市公司基础信息",
        )

    def get_ddl(self) -> str:
        return ddl.STOCK_COMPANY_DDL

    def sync_date(self, trade_date: date) -> SyncResult:
        from app.datasync.service.tushare_ingest import ingest_stock_company_snapshot

        try:
            ingest_stock_company_snapshot(sleep_between=0)
            with get_tushare_engine().connect() as conn:
                row_count = conn.execute(text("SELECT COUNT(*) FROM stock_company")).scalar()
            return SyncResult(SyncStatus.SUCCESS, int(row_count or 0))
        except Exception as exc:
            return handle_tushare_sync_exception(logger, "stock_company sync", exc)

    def sync_range(self, start: date, end: date) -> SyncResult:
        return self.sync_date(end)


class TushareStockDailyInterface(BaseIngestInterface):
    def requires_nonempty_trading_day_data(self) -> bool:
        return True

    @property
    def info(self) -> InterfaceInfo:
        return InterfaceInfo(
            interface_key="stock_daily",
            display_name="日线行情",
            source_key="tushare",
            target_database="tushare",
            target_table="stock_daily",
            sync_priority=20,
            enabled_by_default=True,
            description="A股日K线",
        )

    def get_ddl(self) -> str:
        return ddl.STOCK_DAILY_DDL

    def sync_date(self, trade_date: date) -> SyncResult:
        from app.datasync.service.tushare_ingest import call_pro, upsert_daily

        target = trade_date.strftime("%Y%m%d")
        try:
            df = call_pro("daily", trade_date=target)
            if df is None or df.empty:
                return SyncResult(SyncStatus.SUCCESS, 0, "No trading data (non-trading day?)")
            rows = upsert_daily(df)
            return SyncResult(SyncStatus.SUCCESS, rows)
        except Exception as exc:
            return handle_tushare_sync_exception(logger, f"stock_daily sync for {trade_date}", exc)


class TushareBakDailyInterface(BaseIngestInterface):
    def requires_nonempty_trading_day_data(self) -> bool:
        return True

    @property
    def info(self) -> InterfaceInfo:
        return InterfaceInfo(
            interface_key="bak_daily",
            display_name="备用行情",
            source_key="tushare",
            target_database="tushare",
            target_table="bak_daily",
            sync_priority=22,
            enabled_by_default=True,
            description="A股备用行情数据(5000积分)",
        )

    def get_ddl(self) -> str:
        return ddl.BAK_DAILY_DDL

    def sync_date(self, trade_date: date) -> SyncResult:
        from app.datasync.service.tushare_ingest import ingest_bak_daily

        target = trade_date.strftime("%Y%m%d")
        try:
            rows = ingest_bak_daily(trade_date=target)
            return SyncResult(SyncStatus.SUCCESS, rows or 0)
        except Exception as exc:
            return handle_tushare_sync_exception(logger, f"bak_daily sync for {trade_date}", exc)


class TushareMoneyflowInterface(BaseIngestInterface):
    def requires_nonempty_trading_day_data(self) -> bool:
        return True

    @property
    def info(self) -> InterfaceInfo:
        return InterfaceInfo(
            interface_key="moneyflow",
            display_name="资金流向",
            source_key="tushare",
            target_database="tushare",
            target_table="moneyflow",
            sync_priority=25,
            enabled_by_default=True,
            description="个股资金流向数据",
        )

    def get_ddl(self) -> str:
        return ddl.STOCK_MONEYFLOW_DDL

    def sync_date(self, trade_date: date) -> SyncResult:
        from app.datasync.service.tushare_ingest import call_pro, upsert_moneyflow

        target = trade_date.strftime("%Y%m%d")
        try:
            df = call_pro("moneyflow", trade_date=target)
            if df is None or df.empty:
                return SyncResult(SyncStatus.SUCCESS, 0)
            rows = upsert_moneyflow(df)
            return SyncResult(SyncStatus.SUCCESS, rows)
        except Exception as exc:
            return handle_tushare_sync_exception(logger, f"moneyflow sync for {trade_date}", exc)


class TushareSuspendDInterface(BaseIngestInterface):
    @property
    def info(self) -> InterfaceInfo:
        return InterfaceInfo(
            interface_key="suspend_d",
            display_name="停复牌当日信息",
            source_key="tushare",
            target_database="tushare",
            target_table="suspend_d",
            sync_priority=23,
            enabled_by_default=True,
            description="停复牌当日状态数据",
        )

    def get_ddl(self) -> str:
        return ddl.SUSPEND_D_DDL

    def sync_date(self, trade_date: date) -> SyncResult:
        from app.datasync.service.tushare_ingest import ingest_suspend_d

        target = trade_date.strftime("%Y%m%d")
        try:
            rows = ingest_suspend_d(trade_date=target)
            return SyncResult(SyncStatus.SUCCESS, rows or 0)
        except Exception as exc:
            return handle_tushare_sync_exception(logger, f"suspend_d sync for {trade_date}", exc)


class TushareSuspendInterface(BaseIngestInterface):
    @property
    def info(self) -> InterfaceInfo:
        return InterfaceInfo(
            interface_key="suspend",
            display_name="停复牌历史",
            source_key="tushare",
            target_database="tushare",
            target_table="suspend",
            sync_priority=24,
            enabled_by_default=True,
            description="停复牌历史数据",
        )

    def get_ddl(self) -> str:
        return ddl.SUSPEND_HISTORY_DDL

    def sync_date(self, trade_date: date) -> SyncResult:
        from app.datasync.service.tushare_ingest import ingest_suspend

        target = trade_date.strftime("%Y%m%d")
        try:
            rows = ingest_suspend(suspend_date=target)
            return SyncResult(SyncStatus.SUCCESS, rows or 0)
        except Exception as exc:
            return handle_tushare_sync_exception(logger, f"suspend sync for {trade_date}", exc)


class TushareAdjFactorInterface(BaseIngestInterface):
    def requires_nonempty_trading_day_data(self) -> bool:
        return True

    @property
    def info(self) -> InterfaceInfo:
        return InterfaceInfo(
            interface_key="adj_factor",
            display_name="复权因子",
            source_key="tushare",
            target_database="tushare",
            target_table="adj_factor",
            sync_priority=30,
            enabled_by_default=True,
            description="前复权因子",
        )

    def get_ddl(self) -> str:
        return ddl.ADJ_FACTOR_DDL

    def sync_date(self, trade_date: date) -> SyncResult:
        from app.datasync.service.tushare_ingest import ingest_adj_factor
        from app.domains.extdata.dao.data_sync_status_dao import get_adj_factor_count_for_date

        target = trade_date.strftime("%Y%m%d")
        try:
            ingest_adj_factor(trade_date=target)
            count = get_adj_factor_count_for_date(trade_date)
            return SyncResult(SyncStatus.SUCCESS, count)
        except Exception as exc:
            return handle_tushare_sync_exception(logger, f"adj_factor sync for {trade_date}", exc)


class TushareDividendInterface(BaseIngestInterface):
    @property
    def info(self) -> InterfaceInfo:
        return InterfaceInfo(
            interface_key="dividend",
            display_name="分红送股",
            source_key="tushare",
            target_database="tushare",
            target_table="dividend",
            sync_priority=50,
            requires_permission="0",
            enabled_by_default=False,
            description="分红送股数据(需高级权限)",
        )

    def get_ddl(self) -> str:
        return ddl.STOCK_DIVIDEND_DDL

    def backfill_mode(self) -> str:
        return "range"

    def sync_date(self, trade_date: date) -> SyncResult:
        from app.datasync.service.tushare_ingest import call_pro
        from app.domains.extdata.dao.tushare_dao import upsert_dividend_df

        target = trade_date.strftime("%Y%m%d")
        try:
            df = call_pro("dividend", ann_date=target)
            if df is None or df.empty:
                return SyncResult(SyncStatus.SUCCESS, 0)
            rows = upsert_dividend_df(df)
            return SyncResult(SyncStatus.SUCCESS, rows)
        except Exception as exc:
            return handle_tushare_sync_exception(
                logger,
                f"dividend sync for {trade_date}",
                exc,
                allow_permission_partial=True,
            )

    def sync_range(self, start: date, end: date) -> SyncResult:
        from app.datasync.service.tushare_ingest import ingest_dividend_by_ann_date_range

        try:
            rows = ingest_dividend_by_ann_date_range(start.isoformat(), end.isoformat())
            return SyncResult(SyncStatus.SUCCESS, rows or 0)
        except Exception as exc:
            return handle_tushare_sync_exception(
                logger,
                f"dividend range backfill for {start} -> {end}",
                exc,
                allow_permission_partial=True,
            )

    def get_backfill_rows_by_date(self, start: date, end: date) -> dict[date, int]:
        from app.domains.extdata.dao.data_sync_status_dao import get_dividend_counts

        return get_dividend_counts(start, end)


class TushareTop10HoldersInterface(BaseIngestInterface):
    @property
    def info(self) -> InterfaceInfo:
        return InterfaceInfo(
            interface_key="top10_holders",
            display_name="十大股东",
            source_key="tushare",
            target_database="tushare",
            target_table="top10_holders",
            sync_priority=57,
            requires_permission="0",
            enabled_by_default=False,
            description="十大股东数据(需高级权限)",
        )

    def get_ddl(self) -> str:
        return ddl.TOP10_HOLDERS_DDL

    def backfill_mode(self) -> str:
        return "range"

    def sync_date(self, trade_date: date) -> SyncResult:
        from app.datasync.service.tushare_ingest import ingest_top10_holders, get_all_ts_codes

        try:
            ts_codes = get_all_ts_codes()
            total = min(50, len(ts_codes))
            success = 0
            for code in ts_codes[:total]:
                try:
                    ingest_top10_holders(ts_code=code)
                    success += 1
                except Exception:
                    pass
            if success > 0:
                return SyncResult(
                    SyncStatus.SUCCESS,
                    success,
                    f"Sampled {success}/{total}",
                    details={"sample_symbols": ts_codes[: min(5, total)], "sampled_count": total},
                )
            return SyncResult(
                SyncStatus.PARTIAL,
                0,
                "No holder data fetched",
                details={"sample_symbols": ts_codes[: min(5, total)], "sampled_count": total},
            )
        except Exception as exc:
            return handle_tushare_sync_exception(logger, "top10_holders sync", exc)

    def sync_range(self, start: date, end: date) -> SyncResult:
        from app.datasync.service.tushare_ingest import ingest_top10_holders_marketwide_by_date_range

        try:
            rows = ingest_top10_holders_marketwide_by_date_range(start.isoformat(), end.isoformat())
            return SyncResult(SyncStatus.SUCCESS, rows or 0)
        except Exception as exc:
            return handle_tushare_sync_exception(
                logger,
                f"top10_holders range backfill for {start} -> {end}",
                exc,
                allow_permission_partial=True,
            )

    def get_backfill_rows_by_date(self, start: date, end: date) -> dict[date, int]:
        from app.domains.extdata.dao.data_sync_status_dao import get_top10_holders_counts

        return get_top10_holders_counts(start, end)


class _ExplicitKeyCatalogInterface(TushareCatalogInterface):
    def supports_scheduled_sync(self) -> bool:
        return True

    def _ensure_inferred_table(self, rows) -> dict[str, object] | None:
        if self.should_ensure_table_before_sync():
            return None
        if rows is None or getattr(rows, "empty", True):
            return None

        from app.datasync.table_manager import ensure_inferred_table

        schema = _infer_catalog_schema(
            self,
            rows,
            preferred_date_column=self._schema_date_column(),
            preferred_key_fields=self._payload_key_fields(),
        )
        ensure_inferred_table(self.info.target_database, self.info.target_table, schema)
        return schema


class _LatestOnlyCatalogInterface(_ExplicitKeyCatalogInterface):
    def supports_backfill(self) -> bool:
        return False


class _OneShotDateCatalogInterface(_ExplicitKeyCatalogInterface):
    def __init__(self, spec: TushareCatalogSpec, *, request_date_param: str, schema_date_column: str | None = None):
        super().__init__(spec)
        self._request_date_param = request_date_param
        self._schema_date_key = schema_date_column or request_date_param

    def supports_backfill(self) -> bool:
        return True

    def _schema_date_column(self) -> str | None:
        return self._schema_date_key

    def sync_date(self, trade_date: date) -> SyncResult:
        target = trade_date.strftime("%Y%m%d")
        return _sync_catalog_once(self, params={self._request_date_param: target})


class TusharePerSymbolDateCatalogInterface(_ExplicitKeyCatalogInterface):
    def __init__(
        self,
        spec: TushareCatalogSpec,
        *,
        request_date_param: str,
        extra_key_fields: tuple[str, ...] = (),
        supports_range: bool = True,
        entity_param_name: str = "ts_code",
        entity_loader=None,
        backfill_mode: str | None = None,
    ):
        super().__init__(spec)
        self._request_date_param = request_date_param
        self._extra_key_fields = tuple(field for field in extra_key_fields if field)
        self._supports_range = supports_range
        self._entity_param_name = entity_param_name
        self._entity_loader = entity_loader or _get_stock_codes
        self._backfill_mode = backfill_mode or ("range" if supports_range else "date")

    def supports_backfill(self) -> bool:
        return True

    def backfill_mode(self) -> str:
        return self._backfill_mode

    def _schema_date_column(self) -> str | None:
        return self._request_date_param

    def _payload_key_fields(self) -> tuple[str, ...]:
        fields = [
            self._entity_param_name,
            self._request_date_param,
            *self._extra_key_fields,
            "symbol",
            "code",
            "exchange",
            "market",
            "name",
        ]
        return tuple(dict.fromkeys(field for field in fields if field))

    def sync_date(self, trade_date: date) -> SyncResult:
        try:
            entities = self._entity_loader()
        except Exception as exc:
            return handle_tushare_sync_exception(logger, f"{self.info.interface_key} symbol load", exc)

        target = trade_date.strftime("%Y%m%d")
        return _sync_catalog_by_entities(
            self,
            entities,
            lambda entity: {self._entity_param_name: entity, self._request_date_param: target},
        )

    def sync_range(self, start: date, end: date) -> SyncResult:
        """Fetch a full date range per symbol.  This reduces API calls from
        (dates × symbols) to (symbols) by passing start_date/end_date to
        the Tushare endpoint for each symbol."""
        try:
            entities = self._entity_loader()
        except Exception as exc:
            return handle_tushare_sync_exception(logger, f"{self.info.interface_key} symbol load", exc)

        start_str = start.strftime("%Y%m%d")
        end_str = end.strftime("%Y%m%d")

        return _sync_catalog_by_entities(
            self,
            entities,
            lambda entity: {
                self._entity_param_name: entity,
                "start_date": start_str,
                "end_date": end_str,
            },
        )


class TusharePerSymbolMultiFreqDateCatalogInterface(TusharePerSymbolDateCatalogInterface):
    def __init__(
        self,
        spec: TushareCatalogSpec,
        *,
        request_date_param: str,
        frequencies: tuple[str, ...] = ("week", "month"),
        extra_key_fields: tuple[str, ...] = (),
        supports_range: bool = True,
        entity_param_name: str = "ts_code",
        entity_loader=None,
        backfill_mode: str | None = None,
    ):
        super().__init__(
            spec,
            request_date_param=request_date_param,
            extra_key_fields=extra_key_fields,
            supports_range=supports_range,
            entity_param_name=entity_param_name,
            entity_loader=entity_loader,
            backfill_mode=backfill_mode,
        )
        self._frequencies = tuple(dict.fromkeys(str(freq).strip() for freq in frequencies if str(freq).strip()))

    def sync_date(self, trade_date: date) -> SyncResult:
        try:
            entities = self._entity_loader()
        except Exception as exc:
            return handle_tushare_sync_exception(logger, f"{self.info.interface_key} symbol load", exc)

        target = trade_date.strftime("%Y%m%d")
        return self._sync_for_frequencies(
            entities,
            lambda entity, freq: {
                self._entity_param_name: entity,
                self._request_date_param: target,
                "freq": freq,
            },
        )

    def sync_range(self, start: date, end: date) -> SyncResult:
        try:
            entities = self._entity_loader()
        except Exception as exc:
            return handle_tushare_sync_exception(logger, f"{self.info.interface_key} symbol load", exc)

        start_str = start.strftime("%Y%m%d")
        end_str = end.strftime("%Y%m%d")
        return self._sync_for_frequencies(
            entities,
            lambda entity, freq: {
                self._entity_param_name: entity,
                "start_date": start_str,
                "end_date": end_str,
                "freq": freq,
            },
        )

    def _sync_for_frequencies(self, entities: list[str], params_builder) -> SyncResult:
        results: list[tuple[str, SyncResult]] = []
        for freq in self._frequencies:
            result = _sync_catalog_by_entities(
                self,
                entities,
                lambda entity, _freq=freq: params_builder(entity, _freq),
            )
            results.append((freq, result))
            if result.status in {SyncStatus.PENDING, SyncStatus.ERROR}:
                break
        return _merge_sync_results_by_variant(results)


class TusharePerSymbolLatestCatalogInterface(_LatestOnlyCatalogInterface):
    def __init__(
        self,
        spec: TushareCatalogSpec,
        *,
        extra_key_fields: tuple[str, ...] = (),
        entity_param_name: str = "ts_code",
        entity_loader=None,
    ):
        super().__init__(spec)
        self._extra_key_fields = tuple(field for field in extra_key_fields if field)
        self._entity_param_name = entity_param_name
        self._entity_loader = entity_loader or _get_stock_codes

    def _payload_key_fields(self) -> tuple[str, ...]:
        fields = [
            self._entity_param_name,
            *self._extra_key_fields,
            "symbol",
            "code",
            "exchange",
            "market",
            "name",
        ]
        return tuple(dict.fromkeys(field for field in fields if field))

    def sync_date(self, trade_date: date) -> SyncResult:
        try:
            entities = self._entity_loader()
        except Exception as exc:
            return handle_tushare_sync_exception(logger, f"{self.info.interface_key} symbol load", exc)

        return _sync_catalog_by_entities(self, entities, lambda entity: {self._entity_param_name: entity})


class TusharePerSymbolCodeCatalogInterface(TusharePerSymbolLatestCatalogInterface):
    def supports_backfill(self) -> bool:
        return True

    def backfill_mode(self) -> str:
        return "code"

    def sync_code(self, anchor_date: date) -> SyncResult:
        return self.sync_date(anchor_date)


class TushareCyqChipsInterface(_ExplicitKeyCatalogInterface):
    def __init__(self):
        super().__init__(
            TushareCatalogSpec(
                interface_key="cyq_chips",
                display_name="筹码分布",
                api_name="cyq_chips",
                target_table="cyq_chips",
                sync_priority=310,
                requires_permission="0",
            )
        )

    def supports_backfill(self) -> bool:
        return True

    def backfill_mode(self) -> str:
        return "date"

    def requires_nonempty_trading_day_data(self) -> bool:
        return True

    def _schema_date_column(self) -> str | None:
        return "trade_date"

    def _payload_key_fields(self) -> tuple[str, ...]:
        return tuple(dict.fromkeys(("ts_code", "trade_date", "price", *super()._payload_key_fields())))

    def sync_date(self, trade_date: date) -> SyncResult:
        from app.datasync.service.tushare_ingest import TushareQuotaExceededError, call_pro, get_all_ts_codes
        from app.domains.extdata.dao.tushare_dao import insert_catalog_rows

        target = trade_date.strftime("%Y%m%d")
        try:
            ts_codes = get_all_ts_codes()
        except Exception as exc:
            return handle_tushare_sync_exception(logger, f"cyq_chips symbol load for {trade_date}", exc)

        if not ts_codes:
            return SyncResult(SyncStatus.SUCCESS, 0, details={"symbol_count": 0, "processed_count": 0})

        inferred_schema: dict[str, object] | None = None
        total_rows = 0
        processed_count = 0
        failed_symbols: list[str] = []

        for ts_code in ts_codes:
            try:
                df = call_pro(self.info.interface_key, ts_code=ts_code, trade_date=target)
                processed_count += 1
                if df is None or getattr(df, "empty", True):
                    continue
                if inferred_schema is None:
                    inferred_schema = self._ensure_inferred_table(df)
                rows = insert_catalog_rows(
                    self.info.target_table,
                    df,
                    key_fields=self._payload_key_fields(),
                    column_specs=None if inferred_schema is None else list(inferred_schema["column_specs"]),
                    key_columns=None if inferred_schema is None else tuple(inferred_schema["key_columns"]),
                )
                total_rows += rows
            except TushareQuotaExceededError as exc:
                return SyncResult(
                    SyncStatus.PENDING,
                    total_rows,
                    str(exc),
                    details=self._build_symbol_sync_details(
                        ts_codes,
                        processed_count,
                        failed_symbols,
                        quota_exceeded=True,
                        retry_after=getattr(exc, "retry_after", None),
                    ),
                )
            except Exception as exc:
                if is_permission_error(str(exc)):
                    if total_rows == 0:
                        return handle_tushare_sync_exception(
                            logger,
                            f"cyq_chips sync for {trade_date}",
                            exc,
                            allow_permission_partial=True,
                        )
                    return SyncResult(
                        SyncStatus.PARTIAL,
                        total_rows,
                        "Permission denied",
                        details=self._build_symbol_sync_details(
                            ts_codes,
                            processed_count,
                            failed_symbols,
                            permission_denied=True,
                        ),
                    )
                failed_symbols.append(ts_code)
                logger.warning("cyq_chips sync failed for %s on %s: %s", ts_code, target, exc)

        details = self._build_symbol_sync_details(ts_codes, processed_count, failed_symbols)
        if failed_symbols and total_rows == 0:
            return SyncResult(
                SyncStatus.ERROR,
                0,
                f"Failed symbols: {', '.join(failed_symbols[:5])}",
                details=details,
            )
        if failed_symbols:
            return SyncResult(
                SyncStatus.PARTIAL,
                total_rows,
                f"Failed symbols: {', '.join(failed_symbols[:5])}",
                details=details,
            )
        return SyncResult(SyncStatus.SUCCESS, total_rows, details=details)

    @staticmethod
    def _build_symbol_sync_details(
        ts_codes: list[str],
        processed_count: int,
        failed_symbols: list[str],
        *,
        quota_exceeded: bool = False,
        retry_after: float | None = None,
        permission_denied: bool = False,
    ) -> dict[str, object]:
        details: dict[str, object] = {
            "symbol_count": len(ts_codes),
            "processed_count": max(processed_count, 0),
        }
        if failed_symbols:
            details["failed_symbols"] = list(failed_symbols[:20])
            details["failure_count"] = len(failed_symbols)
        if quota_exceeded:
            details["quota_exceeded"] = True
        if retry_after is not None:
            details["quota_retry_after"] = str(retry_after)
        if permission_denied:
            details["permission_denied"] = True
        return details


def _sync_catalog_by_fund_codes(
    iface: TushareCatalogInterface,
    trade_date: date,
    *,
    request_date_param: str,
) -> SyncResult:
    try:
        fund_codes = _get_fund_codes()
    except Exception as exc:
        return handle_tushare_sync_exception(logger, f"{iface.info.interface_key} code load", exc)

    target = trade_date.strftime("%Y%m%d")
    return _sync_catalog_by_entities(
        iface,
        fund_codes,
        lambda fund_code: {"ts_code": fund_code, request_date_param: target},
    )


class TushareBoxOfficeMonthlyInterface(_OneShotDateCatalogInterface):
    def __init__(self):
        super().__init__(
            TushareCatalogSpec(
                interface_key="bo_monthly",
                display_name="电影月度票房",
                api_name="bo_monthly",
                target_table="bo_monthly",
                sync_priority=820,
                requires_permission="1",
            ),
            request_date_param="date",
        )

    def _payload_key_fields(self) -> tuple[str, ...]:
        return tuple(dict.fromkeys(("date", "month", "movie_name", "name", "rank", *super()._payload_key_fields())))


class TushareBoxOfficeWeeklyInterface(_OneShotDateCatalogInterface):
    def __init__(self):
        super().__init__(
            TushareCatalogSpec(
                interface_key="bo_weekly",
                display_name="电影周票房",
                api_name="bo_weekly",
                target_table="bo_weekly",
                sync_priority=821,
                requires_permission="1",
            ),
            request_date_param="date",
        )

    def _payload_key_fields(self) -> tuple[str, ...]:
        return tuple(
            dict.fromkeys(("date", "week", "week_date", "movie_name", "name", "rank", *super()._payload_key_fields()))
        )


class TushareFundDivInterface(_ExplicitKeyCatalogInterface):
    def __init__(self):
        super().__init__(
            TushareCatalogSpec(
                interface_key="fund_div",
                display_name="公募基金分红",
                api_name="fund_div",
                target_table="fund_div",
                sync_priority=514,
                requires_permission="0",
            )
        )

    def _payload_key_fields(self) -> tuple[str, ...]:
        return tuple(
            dict.fromkeys(
                (
                    "ts_code",
                    "fund_code",
                    "ann_date",
                    "record_date",
                    "ex_date",
                    "pay_date",
                    "div_proc",
                    *super()._payload_key_fields(),
                )
            )
        )

    def _schema_date_column(self) -> str | None:
        return "ann_date"

    def supports_backfill(self) -> bool:
        return True

    def backfill_mode(self) -> str:
        return "range"

    def sync_date(self, trade_date: date) -> SyncResult:
        return _sync_catalog_by_fund_codes(self, trade_date, request_date_param="ann_date")

    def sync_range(self, start: date, end: date) -> SyncResult:
        try:
            fund_codes = _get_fund_codes()
        except Exception as exc:
            return handle_tushare_sync_exception(logger, f"{self.info.interface_key} code load", exc)
        start_str = start.strftime("%Y%m%d")
        end_str = end.strftime("%Y%m%d")
        return _sync_catalog_by_entities(
            self, fund_codes,
            lambda code: {"ts_code": code, "start_date": start_str, "end_date": end_str},
        )


class TushareFundNavInterface(_ExplicitKeyCatalogInterface):
    def __init__(self):
        super().__init__(
            TushareCatalogSpec(
                interface_key="fund_nav",
                display_name="公募基金净值",
                api_name="fund_nav",
                target_table="fund_nav",
                sync_priority=512,
                requires_permission="0",
            )
        )

    def _payload_key_fields(self) -> tuple[str, ...]:
        return tuple(dict.fromkeys(("ts_code", "fund_code", "nav_date", "ann_date", *super()._payload_key_fields())))

    def _schema_date_column(self) -> str | None:
        return "nav_date"

    def supports_backfill(self) -> bool:
        return True

    def backfill_mode(self) -> str:
        return "range"

    def sync_date(self, trade_date: date) -> SyncResult:
        return _sync_catalog_by_fund_codes(self, trade_date, request_date_param="nav_date")

    def sync_range(self, start: date, end: date) -> SyncResult:
        try:
            fund_codes = _get_fund_codes()
        except Exception as exc:
            return handle_tushare_sync_exception(logger, f"{self.info.interface_key} code load", exc)
        start_str = start.strftime("%Y%m%d")
        end_str = end.strftime("%Y%m%d")
        return _sync_catalog_by_entities(
            self, fund_codes,
            lambda code: {"ts_code": code, "start_date": start_str, "end_date": end_str},
        )


class TushareFundPortfolioInterface(_ExplicitKeyCatalogInterface):
    def __init__(self):
        super().__init__(
            TushareCatalogSpec(
                interface_key="fund_portfolio",
                display_name="公募基金持仓",
                api_name="fund_portfolio",
                target_table="fund_portfolio",
                sync_priority=515,
                requires_permission="0",
            )
        )

    def _payload_key_fields(self) -> tuple[str, ...]:
        return tuple(
            dict.fromkeys(
                (
                    "ts_code",
                    "fund_code",
                    "end_date",
                    "ann_date",
                    "symbol",
                    "stock_code",
                    "con_code",
                    *super()._payload_key_fields(),
                )
            )
        )

    def _schema_date_column(self) -> str | None:
        return "ann_date"

    def supports_backfill(self) -> bool:
        return True

    def backfill_mode(self) -> str:
        return "range"

    def sync_date(self, trade_date: date) -> SyncResult:
        return _sync_catalog_by_fund_codes(self, trade_date, request_date_param="ann_date")

    def sync_range(self, start: date, end: date) -> SyncResult:
        try:
            fund_codes = _get_fund_codes()
        except Exception as exc:
            return handle_tushare_sync_exception(logger, f"{self.info.interface_key} code load", exc)
        start_str = start.strftime("%Y%m%d")
        end_str = end.strftime("%Y%m%d")
        return _sync_catalog_by_entities(
            self, fund_codes,
            lambda code: {"ts_code": code, "start_date": start_str, "end_date": end_str},
        )


class TushareIndexWeightInterface(_ExplicitKeyCatalogInterface):
    def __init__(self):
        super().__init__(
            TushareCatalogSpec(
                interface_key="index_weight",
                display_name="指数成分和权重",
                api_name="index_weight",
                target_table="index_weight",
                sync_priority=402,
                requires_permission="0",
            )
        )

    def _schema_date_column(self) -> str | None:
        return "trade_date"

    def _payload_key_fields(self) -> tuple[str, ...]:
        return tuple(dict.fromkeys(("index_code", "trade_date", "con_code", *super()._payload_key_fields())))

    def supports_backfill(self) -> bool:
        return True

    def backfill_mode(self) -> str:
        return "range"

    def sync_date(self, trade_date: date) -> SyncResult:
        return self.sync_range(trade_date, trade_date)

    def sync_range(self, start: date, end: date) -> SyncResult:
        try:
            index_codes = _get_index_codes()
        except Exception as exc:
            return handle_tushare_sync_exception(logger, "index_weight code load", exc)
        return _sync_catalog_by_entities(
            self,
            index_codes,
            lambda index_code: {
                "index_code": index_code,
                "start_date": start.strftime("%Y%m%d"),
                "end_date": end.strftime("%Y%m%d"),
            },
        )

    def get_backfill_rows_by_date(self, start: date, end: date) -> dict[date, int]:
        try:
            return _get_table_rows_by_date(self.info.target_table, "trade_date", start, end)
        except Exception:
            logger.exception("Failed to count index_weight rows by trade_date for %s -> %s", start, end)
            return {}


class TusharePledgeDetailInterface(_LatestOnlyCatalogInterface):
    def __init__(self):
        super().__init__(
            TushareCatalogSpec(
                interface_key="pledge_detail",
                display_name="股权质押明细",
                api_name="pledge_detail",
                target_table="pledge_detail",
                sync_priority=302,
                requires_permission="0",
            )
        )

    def _payload_key_fields(self) -> tuple[str, ...]:
        return tuple(
            dict.fromkeys(
                (
                    "ts_code",
                    "ann_date",
                    "holder_name",
                    "start_date",
                    "end_date",
                    "release_date",
                    *super()._payload_key_fields(),
                )
            )
        )

    def _schema_date_column(self) -> str | None:
        return "ann_date"

    def sync_date(self, trade_date: date) -> SyncResult:
        try:
            ts_codes = _load_distinct_table_values("stock_basic", ("ts_code",))
            if not ts_codes:
                from app.datasync.service.tushare_ingest import get_all_ts_codes

                ts_codes = get_all_ts_codes()
        except Exception as exc:
            return handle_tushare_sync_exception(logger, "pledge_detail symbol load", exc)
        return _sync_catalog_by_entities(self, ts_codes, lambda ts_code: {"ts_code": ts_code})


class TushareStockWeeklyInterface(BaseIngestInterface):
    @property
    def info(self) -> InterfaceInfo:
        return InterfaceInfo(
            interface_key="stock_weekly",
            display_name="周线行情",
            source_key="tushare",
            target_database="tushare",
            target_table="stock_weekly",
            sync_priority=25,
            enabled_by_default=True,
            description="A股周K线",
        )

    def get_ddl(self) -> str:
        return ddl.STOCK_WEEKLY_DDL

    def sync_date(self, trade_date: date) -> SyncResult:
        from app.datasync.service.tushare_ingest import ingest_weekly

        target = trade_date.strftime("%Y%m%d")
        try:
            rows = ingest_weekly(trade_date=target)
            return SyncResult(SyncStatus.SUCCESS, rows or 0)
        except Exception as exc:
            return handle_tushare_sync_exception(logger, f"stock_weekly sync for {trade_date}", exc)


class TushareStockMonthlyInterface(BaseIngestInterface):
    @property
    def info(self) -> InterfaceInfo:
        return InterfaceInfo(
            interface_key="stock_monthly",
            display_name="月线行情",
            source_key="tushare",
            target_database="tushare",
            target_table="stock_monthly",
            sync_priority=26,
            enabled_by_default=True,
            description="A股月K线",
        )

    def get_ddl(self) -> str:
        return ddl.STOCK_MONTHLY_DDL

    def sync_date(self, trade_date: date) -> SyncResult:
        from app.datasync.service.tushare_ingest import ingest_monthly

        target = trade_date.strftime("%Y%m%d")
        try:
            rows = ingest_monthly(trade_date=target)
            return SyncResult(SyncStatus.SUCCESS, rows or 0)
        except Exception as exc:
            return handle_tushare_sync_exception(logger, f"stock_monthly sync for {trade_date}", exc)


class TushareIndexDailyInterface(BaseIngestInterface):
    def requires_nonempty_trading_day_data(self) -> bool:
        return True

    @property
    def info(self) -> InterfaceInfo:
        return InterfaceInfo(
            interface_key="index_daily",
            display_name="指数日线",
            source_key="tushare",
            target_database="tushare",
            target_table="index_daily",
            sync_priority=27,
            enabled_by_default=True,
            description="指数日K线",
        )

    def get_ddl(self) -> str:
        return ddl.INDEX_DAILY_DDL

    def sync_date(self, trade_date: date) -> SyncResult:
        from app.datasync.service.tushare_ingest import ingest_index_daily

        target = trade_date.strftime("%Y%m%d")
        total_rows = 0
        failures = []
        for code in INDEX_CODES:
            try:
                rows = ingest_index_daily(ts_code=code, start_date=target, end_date=target)
                total_rows += rows or 0
            except Exception as e:
                logger.warning("index_daily %s failed: %s", code, e)
                failures.append(code)

        if failures and total_rows == 0:
            return SyncResult(
                SyncStatus.ERROR,
                0,
                f"Failed: {','.join(failures)}",
                details={"symbols": list(INDEX_CODES), "failed_symbols": failures},
            )
        if failures:
            return SyncResult(
                SyncStatus.PARTIAL,
                total_rows,
                f"Failed: {','.join(failures)}",
                details={"symbols": list(INDEX_CODES), "failed_symbols": failures},
            )
        return SyncResult(SyncStatus.SUCCESS, total_rows, details={"symbols": list(INDEX_CODES)})


class TushareIndexWeeklyInterface(BaseIngestInterface):
    @property
    def info(self) -> InterfaceInfo:
        return InterfaceInfo(
            interface_key="index_weekly",
            display_name="指数周线",
            source_key="tushare",
            target_database="tushare",
            target_table="index_weekly",
            sync_priority=28,
            enabled_by_default=True,
            description="指数周K线",
        )

    def get_ddl(self) -> str:
        return ddl.INDEX_WEEKLY_DDL

    def sync_date(self, trade_date: date) -> SyncResult:
        from app.datasync.service.tushare_ingest import ingest_index_weekly

        target = trade_date.strftime("%Y%m%d")
        total_rows = 0
        failures = []
        for code in INDEX_CODES:
            try:
                rows = ingest_index_weekly(ts_code=code, start_date=target, end_date=target)
                total_rows += rows or 0
            except Exception as e:
                logger.warning("index_weekly %s failed: %s", code, e)
                failures.append(code)

        if failures and total_rows == 0:
            return SyncResult(
                SyncStatus.ERROR,
                0,
                f"Failed: {','.join(failures)}",
                details={"symbols": list(INDEX_CODES), "failed_symbols": failures},
            )
        if failures:
            return SyncResult(
                SyncStatus.PARTIAL,
                total_rows,
                f"Failed: {','.join(failures)}",
                details={"symbols": list(INDEX_CODES), "failed_symbols": failures},
            )
        return SyncResult(SyncStatus.SUCCESS, total_rows, details={"symbols": list(INDEX_CODES)})


_PER_SYMBOL_DATE_CATALOG_CONFIG: dict[str, dict[str, tuple[str, ...] | str | bool]] = {
    # Financial statements — Tushare supports start_date/end_date range queries.
    "balancesheet": {"request_date_param": "ann_date", "extra_key_fields": ("end_date", "report_type", "comp_type"), "supports_range": True},
    "balancesheet_vip": {"request_date_param": "ann_date", "extra_key_fields": ("end_date", "report_type", "comp_type"), "supports_range": True},
    "cashflow": {"request_date_param": "ann_date", "extra_key_fields": ("end_date", "report_type", "comp_type"), "supports_range": True},
    "cashflow_vip": {"request_date_param": "ann_date", "extra_key_fields": ("end_date", "report_type", "comp_type"), "supports_range": True},
    "fina_audit": {"request_date_param": "ann_date", "extra_key_fields": ("end_date", "audit_agency", "audit_sign"), "supports_range": True},
    "fina_indicator": {"request_date_param": "ann_date", "extra_key_fields": ("end_date",), "supports_range": True},
    "fina_indicator_vip": {"request_date_param": "ann_date", "extra_key_fields": ("end_date",), "supports_range": True},
    "fina_mainbz": {"request_date_param": "end_date", "extra_key_fields": ("ann_date", "bz_item", "curr_type"), "supports_range": True},
    "fina_mainbz_vip": {"request_date_param": "end_date", "extra_key_fields": ("ann_date", "bz_item", "curr_type"), "supports_range": True},
    "income": {"request_date_param": "ann_date", "extra_key_fields": ("end_date", "report_type", "comp_type"), "supports_range": True},
    "income_vip": {"request_date_param": "ann_date", "extra_key_fields": ("end_date", "report_type", "comp_type"), "supports_range": True},
    # Non-financial per-symbol — Tushare supports start_date/end_date range queries.
    "stk_managers": {"request_date_param": "ann_date", "extra_key_fields": ("name", "title", "lev", "begin_date"), "supports_range": True},
    "stk_rewards": {"request_date_param": "end_date", "extra_key_fields": ("ann_date", "name"), "supports_range": True},
    "cb_rating": {"request_date_param": "ann_date", "extra_key_fields": ("end_date", "rating_agency"), "supports_range": True},
    "idx_factor_pro": {"request_date_param": "trade_date", "extra_key_fields": (), "supports_range": True},
}

# Catalog interfaces that need a single date parameter (not per-symbol)
# Maps interface_key → date parameter name for Tushare API
_ONE_SHOT_DATE_CATALOG_KEYS: dict[str, str] = {
    "top_list": "trade_date",
    "top_inst": "trade_date",
    "moneyflow_hsgt": "trade_date",
    "ggt_top10": "trade_date",
}

# Catalog interfaces that are permanently unavailable with free/low-point tokens.
# These are valid Tushare APIs but require higher permission levels than the
# current staging token provides. Marked as requiring permission so the sync
# engine treats failures as non-retryable PARTIAL instead of ERROR.
_PERMISSION_REQUIRED_CATALOG_KEYS: frozenset[str] = frozenset({
    "bo_weekly",
    "bo_monthly",
    "hk_mins",
    "realtime_list",
    "realtime_quote",
    "realtime_tick",
    "film_record",
    "stk_premarket",
    # These require non-standard params (freq, item) that need custom implementations
    "stk_weekly_monthly",
    "stk_week_month_adj",
    "fut_weekly_monthly",
    "tmt_twincome",
    "tmt_twincomedetail",
})

_MULTI_FREQ_DATE_CATALOG_CONFIG: dict[str, dict[str, object]] = {
    "fut_weekly_monthly": {
        "request_date_param": "trade_date",
        "frequencies": ("week", "month"),
        "entity_param_name": "ts_code",
        "entity_loader": _get_fut_codes,
        "supports_range": True,
    },
    "stk_week_month_adj": {
        "request_date_param": "trade_date",
        "frequencies": ("week", "month"),
        "entity_param_name": "ts_code",
        "entity_loader": _get_stock_codes,
        "supports_range": True,
    },
    "stk_weekly_monthly": {
        "request_date_param": "trade_date",
        "frequencies": ("week", "month"),
        "entity_param_name": "ts_code",
        "entity_loader": _get_stock_codes,
        "supports_range": True,
    },
}

_PER_SYMBOL_LATEST_CATALOG_CONFIG: dict[str, tuple[str, ...]] = {
}


def build_specialized_catalog_interface(spec: TushareCatalogSpec) -> BaseIngestInterface | None:
    key = spec.interface_key

    if key == "bo_monthly":
        return TushareBoxOfficeMonthlyInterface()
    if key == "bo_weekly":
        return TushareBoxOfficeWeeklyInterface()
    if key == "cyq_chips":
        return TushareCyqChipsInterface()
    if key == "fund_div":
        return TushareFundDivInterface()
    if key == "fund_nav":
        return TushareFundNavInterface()
    if key == "fund_portfolio":
        return TushareFundPortfolioInterface()
    if key == "index_weight":
        return TushareIndexWeightInterface()
    if key == "pledge_detail":
        return TusharePledgeDetailInterface()

    one_shot_date_param = _ONE_SHOT_DATE_CATALOG_KEYS.get(key)
    if one_shot_date_param is not None:
        return _OneShotDateCatalogInterface(spec, request_date_param=one_shot_date_param)

    multi_freq_config = _MULTI_FREQ_DATE_CATALOG_CONFIG.get(key)
    if multi_freq_config is not None:
        return TusharePerSymbolMultiFreqDateCatalogInterface(
            spec,
            request_date_param=str(multi_freq_config["request_date_param"]),
            frequencies=tuple(str(freq) for freq in (multi_freq_config.get("frequencies") or ())),
            extra_key_fields=tuple(multi_freq_config.get("extra_key_fields", ())),
            supports_range=bool(multi_freq_config.get("supports_range", False)),
            entity_param_name=str(multi_freq_config.get("entity_param_name") or "ts_code"),
            entity_loader=multi_freq_config.get("entity_loader"),
            backfill_mode=spec.backfill_mode,
        )

    date_config = _PER_SYMBOL_DATE_CATALOG_CONFIG.get(key)
    if date_config is not None:
        return TusharePerSymbolDateCatalogInterface(
            spec,
            request_date_param=str(date_config["request_date_param"]),
            extra_key_fields=tuple(date_config.get("extra_key_fields", ())),
            supports_range=bool(date_config.get("supports_range", False)),
        )

    latest_key_fields = _PER_SYMBOL_LATEST_CATALOG_CONFIG.get(key)
    if latest_key_fields is not None:
        return TusharePerSymbolLatestCatalogInterface(spec, extra_key_fields=latest_key_fields)

    entity_binding = _resolve_catalog_entity_binding(spec)
    date_param = _resolve_catalog_date_param(spec)

    if spec.backfill_mode == "code" and entity_binding is not None:
        entity_param_name, entity_loader = entity_binding
        return TusharePerSymbolCodeCatalogInterface(
            spec,
            entity_param_name=entity_param_name,
            entity_loader=entity_loader,
        )

    if spec.backfill_mode in {"range", "code_date"} and entity_binding is not None and date_param is not None:
        entity_param_name, entity_loader = entity_binding
        return TusharePerSymbolDateCatalogInterface(
            spec,
            request_date_param=date_param,
            extra_key_fields=(),
            supports_range=spec.backfill_mode == "range",
            entity_param_name=entity_param_name,
            entity_loader=entity_loader,
            backfill_mode=spec.backfill_mode,
        )

    return None
