"""AkShare data source implementation."""

from __future__ import annotations

import logging

from app.datasync.base import BaseDataSource, BaseIngestInterface

logger = logging.getLogger(__name__)


class AkShareDataSource(BaseDataSource):
    @property
    def source_key(self) -> str:
        return "akshare"

    @property
    def display_name(self) -> str:
        return "AkShare"

    @property
    def requires_token(self) -> bool:
        return False

    def get_interfaces(self) -> list[BaseIngestInterface]:
        from app.datasync.sources.akshare.interfaces import (
            AkShareIndexDailyInterface,
            AkShareIndexSpotInterface,
            AkShareETFDailyInterface,
        )

        return [
            AkShareIndexDailyInterface(),
            AkShareIndexSpotInterface(),
            AkShareETFDailyInterface(),
        ]

    def test_connection(self) -> bool:
        try:
            import akshare as ak

            ak.stock_zh_index_spot_em()
            return True
        except Exception as e:
            logger.warning("AkShare connection test failed: %s", e)
            return False
