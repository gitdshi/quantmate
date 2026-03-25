"""AkShare ingest interface implementations."""

from __future__ import annotations

import logging
from datetime import date

from app.datasync.base import BaseIngestInterface, InterfaceInfo, SyncResult, SyncStatus
from app.datasync.sources.akshare import ddl

logger = logging.getLogger(__name__)


class AkShareIndexDailyInterface(BaseIngestInterface):
    @property
    def info(self) -> InterfaceInfo:
        return InterfaceInfo(
            interface_key="index_daily",
            display_name="指数日线",
            source_key="akshare",
            target_database="akshare",
            target_table="index_daily",
            sync_priority=41,
            enabled_by_default=True,
            description="AkShare指数日K线(沪深300等)",
        )

    def get_ddl(self) -> str:
        return ddl.INDEX_DAILY_DDL

    def sync_date(self, trade_date: date) -> SyncResult:
        from app.datasync.service.akshare_ingest import ingest_index_daily, INDEX_MAPPING

        target_date = trade_date.strftime("%Y-%m-%d")
        total_success = 0
        failures = []
        for symbol in INDEX_MAPPING.keys():
            try:
                rows = ingest_index_daily(symbol=symbol, start_date=target_date)
                total_success += rows
            except Exception as e:
                logger.warning("AkShare index %s failed: %s", symbol, e)
                failures.append(symbol)

        if failures:
            err_msg = f"Failed symbols: {','.join(failures)}"
            if total_success > 0:
                return SyncResult(SyncStatus.PARTIAL, total_success, err_msg)
            return SyncResult(SyncStatus.ERROR, 0, err_msg)
        return SyncResult(SyncStatus.SUCCESS, total_success)


class AkShareIndexSpotInterface(BaseIngestInterface):
    @property
    def info(self) -> InterfaceInfo:
        return InterfaceInfo(
            interface_key="stock_zh_index_spot",
            display_name="指数实时行情",
            source_key="akshare",
            target_database="akshare",
            target_table="stock_zh_index_spot",
            sync_priority=40,
            enabled_by_default=True,
            description="A股指数实时报价",
        )

    def get_ddl(self) -> str:
        return ddl.INDEX_SPOT_DDL

    def sync_date(self, trade_date: date) -> SyncResult:
        try:
            import akshare as ak
            from app.infrastructure.db.connections import get_akshare_engine
            from sqlalchemy import text

            df = ak.stock_zh_index_spot_em()
            if df is None or df.empty:
                return SyncResult(SyncStatus.SUCCESS, 0, "No data")

            engine = get_akshare_engine()
            rows = 0
            with engine.begin() as conn:
                for _, row in df.iterrows():
                    conn.execute(
                        text(
                            "INSERT INTO stock_zh_index_spot (symbol, name, latest_price, change_pct, volume, amount, high, low, open, prev_close) "
                            "VALUES (:sym, :name, :price, :pct, :vol, :amt, :hi, :lo, :op, :pc) "
                            "ON DUPLICATE KEY UPDATE name=VALUES(name), latest_price=VALUES(latest_price), "
                            "change_pct=VALUES(change_pct), volume=VALUES(volume), amount=VALUES(amount), "
                            "high=VALUES(high), low=VALUES(low), open=VALUES(open), prev_close=VALUES(prev_close)"
                        ),
                        {
                            "sym": str(row.get("代码", "")),
                            "name": str(row.get("名称", "")),
                            "price": row.get("最新价"),
                            "pct": row.get("涨跌幅"),
                            "vol": row.get("成交量"),
                            "amt": row.get("成交额"),
                            "hi": row.get("最高"),
                            "lo": row.get("最低"),
                            "op": row.get("今开"),
                            "pc": row.get("昨收"),
                        },
                    )
                    rows += 1
            return SyncResult(SyncStatus.SUCCESS, rows)
        except Exception as e:
            logger.exception("stock_zh_index_spot sync failed: %s", e)
            return SyncResult(SyncStatus.ERROR, 0, str(e))


class AkShareETFDailyInterface(BaseIngestInterface):
    """ETF daily K-line data from AkShare."""

    # Popular ETFs to sync
    ETF_SYMBOLS = ["159919", "510300", "510050", "510500", "159915"]

    @property
    def info(self) -> InterfaceInfo:
        return InterfaceInfo(
            interface_key="fund_etf_daily",
            display_name="ETF日线",
            source_key="akshare",
            target_database="akshare",
            target_table="fund_etf_daily",
            sync_priority=45,
            enabled_by_default=True,
            description="ETF基金日K线数据",
        )

    def get_ddl(self) -> str:
        return ddl.ETF_DAILY_DDL

    def sync_date(self, trade_date: date) -> SyncResult:
        try:
            import akshare as ak
            from app.datasync.service.akshare_ingest import call_ak
            from app.infrastructure.db.connections import get_akshare_engine
            from sqlalchemy import text

            engine = get_akshare_engine()
            total_rows = 0
            failures = []

            for symbol in self.ETF_SYMBOLS:
                try:
                    df = call_ak(
                        "fund_etf_hist_em",
                        ak.fund_etf_hist_em,
                        symbol=symbol,
                        period="daily",
                        start_date=trade_date.strftime("%Y%m%d"),
                        end_date=trade_date.strftime("%Y%m%d"),
                    )
                    if df is None or df.empty:
                        continue
                    with engine.begin() as conn:
                        for _, row in df.iterrows():
                            conn.execute(
                                text(
                                    "INSERT INTO fund_etf_daily (symbol, trade_date, open, high, low, close, volume, amount) "
                                    "VALUES (:sym, :td, :o, :h, :l, :c, :v, :a) "
                                    "ON DUPLICATE KEY UPDATE open=VALUES(open), high=VALUES(high), "
                                    "low=VALUES(low), close=VALUES(close), volume=VALUES(volume), amount=VALUES(amount)"
                                ),
                                {
                                    "sym": symbol,
                                    "td": str(row.get("日期", trade_date))[:10],
                                    "o": row.get("开盘"),
                                    "h": row.get("最高"),
                                    "l": row.get("最低"),
                                    "c": row.get("收盘"),
                                    "v": row.get("成交量"),
                                    "a": row.get("成交额"),
                                },
                            )
                            total_rows += 1
                except Exception as e:
                    logger.warning("ETF %s failed: %s", symbol, e)
                    failures.append(symbol)

            if failures and total_rows == 0:
                return SyncResult(SyncStatus.ERROR, 0, f"Failed: {','.join(failures)}")
            if failures:
                return SyncResult(SyncStatus.PARTIAL, total_rows, f"Failed: {','.join(failures)}")
            return SyncResult(SyncStatus.SUCCESS, total_rows)
        except Exception as e:
            logger.exception("fund_etf_daily sync failed: %s", e)
            return SyncResult(SyncStatus.ERROR, 0, str(e))
