"""Tushare ingest interface implementations.

Each interface wraps existing ingest functions from tushare_ingest.py,
adapting them to the BaseIngestInterface contract.
"""

from __future__ import annotations

import logging
from datetime import date

from app.datasync.base import BaseIngestInterface, InterfaceInfo, SyncResult, SyncStatus
from app.datasync.sources.tushare import ddl

logger = logging.getLogger(__name__)

# Major indices synced by default
INDEX_CODES = ["000001.SH", "399001.SZ", "399006.SZ", "000300.SH", "000905.SH"]


class TushareStockBasicInterface(BaseIngestInterface):
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
        except Exception as e:
            logger.exception("stock_basic sync failed: %s", e)
            return SyncResult(SyncStatus.ERROR, 0, str(e))

    def sync_range(self, start: date, end: date) -> SyncResult:
        # stock_basic is a full snapshot, not date-range-based
        return self.sync_date(end)


class TushareStockDailyInterface(BaseIngestInterface):
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
        except Exception as e:
            logger.exception("stock_daily sync failed for %s: %s", trade_date, e)
            return SyncResult(SyncStatus.ERROR, 0, str(e))


class TushareBakDailyInterface(BaseIngestInterface):
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
        except Exception as e:
            logger.exception("bak_daily sync failed for %s: %s", trade_date, e)
            return SyncResult(SyncStatus.ERROR, 0, str(e))


class TushareMoneyflowInterface(BaseIngestInterface):
    @property
    def info(self) -> InterfaceInfo:
        return InterfaceInfo(
            interface_key="moneyflow",
            display_name="资金流向",
            source_key="tushare",
            target_database="tushare",
            target_table="stock_moneyflow",
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
        except Exception as e:
            logger.exception("moneyflow sync failed for %s: %s", trade_date, e)
            return SyncResult(SyncStatus.ERROR, 0, str(e))


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
        except Exception as e:
            logger.exception("suspend_d sync failed for %s: %s", trade_date, e)
            return SyncResult(SyncStatus.ERROR, 0, str(e))


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
        return ddl.SUSPEND_DAILY_DDL

    def sync_date(self, trade_date: date) -> SyncResult:
        from app.datasync.service.tushare_ingest import ingest_suspend

        target = trade_date.strftime("%Y%m%d")
        try:
            rows = ingest_suspend(suspend_date=target)
            return SyncResult(SyncStatus.SUCCESS, rows or 0)
        except Exception as e:
            logger.exception("suspend sync failed for %s: %s", trade_date, e)
            return SyncResult(SyncStatus.ERROR, 0, str(e))


class TushareAdjFactorInterface(BaseIngestInterface):
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
        except Exception as e:
            logger.exception("adj_factor sync failed for %s: %s", trade_date, e)
            return SyncResult(SyncStatus.ERROR, 0, str(e))


class TushareDividendInterface(BaseIngestInterface):
    @property
    def info(self) -> InterfaceInfo:
        return InterfaceInfo(
            interface_key="dividend",
            display_name="分红送股",
            source_key="tushare",
            target_database="tushare",
            target_table="stock_dividend",
            sync_priority=50,
            requires_permission="1",
            enabled_by_default=False,
            description="分红送股数据(需高级权限)",
        )

    def get_ddl(self) -> str:
        return ddl.STOCK_DIVIDEND_DDL

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
        except Exception as e:
            err_msg = str(e)
            if "没有接口访问权限" in err_msg or "permission" in err_msg.lower():
                return SyncResult(SyncStatus.PARTIAL, 0, "Permission denied")
            logger.exception("dividend sync failed for %s: %s", trade_date, e)
            return SyncResult(SyncStatus.ERROR, 0, err_msg)


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
            requires_permission="1",
            enabled_by_default=False,
            description="十大股东数据(需高级权限)",
        )

    def get_ddl(self) -> str:
        return ddl.TOP10_HOLDERS_DDL

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
        except Exception as e:
            logger.exception("top10_holders sync failed: %s", e)
            return SyncResult(SyncStatus.ERROR, 0, str(e))


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
        except Exception as e:
            logger.exception("stock_weekly sync failed for %s: %s", trade_date, e)
            return SyncResult(SyncStatus.ERROR, 0, str(e))


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
        except Exception as e:
            logger.exception("stock_monthly sync failed for %s: %s", trade_date, e)
            return SyncResult(SyncStatus.ERROR, 0, str(e))


class TushareIndexDailyInterface(BaseIngestInterface):
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
