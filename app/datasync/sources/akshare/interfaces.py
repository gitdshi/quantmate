"""AkShare ingest interface implementations."""

from __future__ import annotations

import logging
from datetime import date

import pandas as pd

from app.datasync.base import BaseIngestInterface, InterfaceInfo, SyncResult, SyncStatus
from app.datasync.sources.akshare import ddl

logger = logging.getLogger(__name__)


class AkShareIndexDailyInterface(BaseIngestInterface):
    def requires_nonempty_trading_day_data(self) -> bool:
        return True

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
                return SyncResult(
                    SyncStatus.PARTIAL,
                    total_success,
                    err_msg,
                    details={"symbols": list(INDEX_MAPPING.keys()), "failed_symbols": failures},
                )
            return SyncResult(
                SyncStatus.ERROR,
                0,
                err_msg,
                details={"symbols": list(INDEX_MAPPING.keys()), "failed_symbols": failures},
            )
        return SyncResult(SyncStatus.SUCCESS, total_success, details={"symbols": list(INDEX_MAPPING.keys())})


class AkShareIndexSpotInterface(BaseIngestInterface):
    def supports_backfill(self) -> bool:
        return False

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
            from app.datasync.service.akshare_ingest import call_ak
            from app.infrastructure.db.connections import get_akshare_engine
            from sqlalchemy import text

            df = call_ak("stock_zh_index_spot_sina", ak.stock_zh_index_spot_sina)
            if df is None or df.empty:
                return SyncResult(SyncStatus.SUCCESS, 0, "No data")

            engine = get_akshare_engine()
            rows = 0
            with engine.begin() as conn:
                for _, row in df.iterrows():
                    conn.execute(
                        text(
                            "INSERT INTO stock_zh_index_spot (symbol, name, latest_price, change_pct, change_amount, volume, amount, high, low, open, prev_close) "
                            "VALUES (:sym, :name, :price, :pct, :chg, :vol, :amt, :hi, :lo, :op, :pc) "
                            "ON DUPLICATE KEY UPDATE name=VALUES(name), latest_price=VALUES(latest_price), "
                            "change_pct=VALUES(change_pct), change_amount=VALUES(change_amount), volume=VALUES(volume), amount=VALUES(amount), "
                            "high=VALUES(high), low=VALUES(low), open=VALUES(open), prev_close=VALUES(prev_close)"
                        ),
                        {
                            "sym": str(row.get("代码", "")),
                            "name": str(row.get("名称", "")),
                            "price": row.get("最新价"),
                            "pct": row.get("涨跌幅"),
                            "chg": row.get("涨跌额"),
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

    def requires_nonempty_trading_day_data(self) -> bool:
        return True

    def _sina_symbol(self, symbol: str) -> str:
        if symbol.startswith(("sh", "sz")):
            return symbol
        prefix = "sz" if symbol.startswith(("15", "16", "18")) else "sh"
        return f"{prefix}{symbol}"

    def _history_cache(self) -> dict[str, pd.DataFrame]:
        cache = getattr(self, "_etf_history_cache", None)
        if cache is None:
            cache = {}
            self._etf_history_cache = cache
        return cache

    def _load_symbol_history(self, symbol: str) -> pd.DataFrame:
        cache = self._history_cache()
        if symbol in cache:
            return cache[symbol]

        import akshare as ak
        from app.datasync.service.akshare_ingest import call_ak

        df = call_ak("fund_etf_hist_sina", ak.fund_etf_hist_sina, symbol=self._sina_symbol(symbol))
        if df is None or df.empty:
            cache[symbol] = pd.DataFrame(columns=["date", "open", "high", "low", "close", "volume", "amount"])
            return cache[symbol]

        df = df.rename(
            columns={
                "日期": "date",
                "开盘": "open",
                "最高": "high",
                "最低": "low",
                "收盘": "close",
                "成交量": "volume",
                "成交额": "amount",
            }
        ).copy()
        df["date"] = pd.to_datetime(df["date"], errors="coerce").dt.date
        cache[symbol] = df
        return df

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
            from app.infrastructure.db.connections import get_akshare_engine
            from sqlalchemy import text

            engine = get_akshare_engine()
            total_rows = 0
            failures = []

            for symbol in self.ETF_SYMBOLS:
                try:
                    df = self._load_symbol_history(symbol)
                    if df is None or df.empty:
                        continue
                    day_df = df[df["date"] == trade_date]
                    if day_df.empty:
                        continue
                    with engine.begin() as conn:
                        for _, row in day_df.iterrows():
                            conn.execute(
                                text(
                                    "INSERT INTO fund_etf_daily (symbol, trade_date, open, high, low, close, volume, amount) "
                                    "VALUES (:sym, :td, :o, :h, :l, :c, :v, :a) "
                                    "ON DUPLICATE KEY UPDATE open=VALUES(open), high=VALUES(high), "
                                    "low=VALUES(low), close=VALUES(close), volume=VALUES(volume), amount=VALUES(amount)"
                                ),
                                {
                                    "sym": symbol,
                                    "td": trade_date.isoformat(),
                                    "o": row.get("open"),
                                    "h": row.get("high"),
                                    "l": row.get("low"),
                                    "c": row.get("close"),
                                    "v": row.get("volume"),
                                    "a": row.get("amount"),
                                },
                            )
                            total_rows += 1
                except Exception as e:
                    logger.warning("ETF %s failed: %s", symbol, e)
                    failures.append(symbol)

            if failures and total_rows == 0:
                return SyncResult(
                    SyncStatus.ERROR,
                    0,
                    f"Failed: {','.join(failures)}",
                    details={"symbols": list(self.ETF_SYMBOLS), "failed_symbols": failures},
                )
            if failures:
                return SyncResult(
                    SyncStatus.PARTIAL,
                    total_rows,
                    f"Failed: {','.join(failures)}",
                    details={"symbols": list(self.ETF_SYMBOLS), "failed_symbols": failures},
                )
            return SyncResult(SyncStatus.SUCCESS, total_rows, details={"symbols": list(self.ETF_SYMBOLS)})
        except Exception as e:
            logger.exception("fund_etf_daily sync failed: %s", e)
            return SyncResult(SyncStatus.ERROR, 0, str(e))
