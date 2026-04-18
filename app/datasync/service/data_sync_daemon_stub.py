"""Stub for DataSyncDaemon to allow API to start.

This is a temporary workaround until the full DataSyncDaemon class is implemented.
"""

from typing import List, Optional
from datetime import date


class DataSyncDaemon:
    """Minimal implementation to satisfy SyncStatusService dependency."""

    @staticmethod
    def find_missing_trade_dates(
        start_date: Optional[date] = None,
        end_date: Optional[date] = None,
        **_compat,
    ) -> List[date]:
        """
        Return missing trade dates for backfill.

        TODO: Implement actual missing date detection based on sync logs.
        For now, return empty list (no missing dates).
        """
        _ = (start_date, end_date)
        # TODO: Calculate actual missing dates
        return []
