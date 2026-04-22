"""Tushare data source implementation."""

from __future__ import annotations

import logging

from app.datasync.base import BaseDataSource, BaseIngestInterface

logger = logging.getLogger(__name__)


class TushareDataSource(BaseDataSource):
    def __init__(self) -> None:
        self._interfaces_cache: list[BaseIngestInterface] | None = None

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
        if self._interfaces_cache is not None:
            return list(self._interfaces_cache)

        from app.datasync.sources.tushare.catalog_interfaces import build_catalog_interfaces
        from app.datasync.sources.tushare.interfaces import (
            TushareBoxOfficeMonthlyInterface,
            TushareBoxOfficeWeeklyInterface,
            TushareTradeCalInterface,
            TushareCyqChipsInterface,
            TushareStockCompanyInterface,
            TushareDividendInterface,
            TushareFundDivInterface,
            TushareFundNavInterface,
            TushareFundPortfolioInterface,
            TushareTop10HoldersInterface,
            TushareIndexDailyInterface,
            TushareIndexWeeklyInterface,
            TushareIndexWeightInterface,
            TusharePledgeDetailInterface,
        )

        interfaces: list[BaseIngestInterface] = [
            TushareBoxOfficeMonthlyInterface(),
            TushareBoxOfficeWeeklyInterface(),
            TushareTradeCalInterface(),
            TushareCyqChipsInterface(),
            TushareStockCompanyInterface(),
            TushareDividendInterface(),
            TushareFundDivInterface(),
            TushareFundNavInterface(),
            TushareFundPortfolioInterface(),
            TushareTop10HoldersInterface(),
            TushareIndexDailyInterface(),
            TushareIndexWeeklyInterface(),
            TushareIndexWeightInterface(),
            TusharePledgeDetailInterface(),
        ]

        existing_keys = {iface.info.interface_key for iface in interfaces}
        interfaces.extend(build_catalog_interfaces(existing_keys))

        self._interfaces_cache = list(interfaces)
        return list(self._interfaces_cache)

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
