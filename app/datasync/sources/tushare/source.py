"""Tushare data source implementation."""

from __future__ import annotations

import logging

from app.datasync.base import BaseDataSource, BaseIngestInterface

logger = logging.getLogger(__name__)


class TushareDataSource(BaseDataSource):
    @property
    def source_key(self) -> str:
        return "tushare"

    @property
    def display_name(self) -> str:
        return "Tushare Pro"

    @property
    def requires_token(self) -> bool:
        return True

    def get_interfaces(self) -> list[BaseIngestInterface]:
        from app.datasync.sources.tushare.interfaces import (
            TushareStockBasicInterface,
            TushareStockDailyInterface,
            TushareBakDailyInterface,
            TushareMoneyflowInterface,
            TushareSuspendDInterface,
            TushareSuspendInterface,
            TushareAdjFactorInterface,
            TushareDividendInterface,
            TushareTop10HoldersInterface,
            TushareStockWeeklyInterface,
            TushareStockMonthlyInterface,
            TushareIndexDailyInterface,
            TushareIndexWeeklyInterface,
        )

        return [
            TushareStockBasicInterface(),
            TushareStockDailyInterface(),
            TushareBakDailyInterface(),
            TushareMoneyflowInterface(),
            TushareSuspendDInterface(),
            TushareSuspendInterface(),
            TushareAdjFactorInterface(),
            TushareDividendInterface(),
            TushareTop10HoldersInterface(),
            TushareStockWeeklyInterface(),
            TushareStockMonthlyInterface(),
            TushareIndexDailyInterface(),
            TushareIndexWeeklyInterface(),
        ]

    def test_connection(self) -> bool:
        try:
            from app.infrastructure.config import get_settings
            import tushare as ts

            settings = get_settings()
            pro = ts.pro_api(settings.tushare_token)
            pro.trade_cal(exchange="SSE", start_date="20250101", end_date="20250102")
            return True
        except Exception as e:
            logger.warning("Tushare connection test failed: %s", e)
            return False
