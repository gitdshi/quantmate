"""VNPy sync service — independent job to convert tushare data to VNPy format.

Wraps the existing vnpy_ingest.sync_date_to_vnpy function.
"""

from __future__ import annotations

import logging
from datetime import date
from typing import Optional

from app.datasync.base import SyncResult, SyncStatus

logger = logging.getLogger(__name__)


def sync_vnpy_for_date(sync_date: date) -> SyncResult:
    """Sync tushare stock_daily data to vnpy.dbbardata for a given date."""
    try:
        from app.datasync.service.vnpy_ingest import sync_date_to_vnpy

        total_symbols, total_bars = sync_date_to_vnpy(sync_date)
        if total_symbols > 0:
            return SyncResult(SyncStatus.SUCCESS, total_bars)
        return SyncResult(SyncStatus.PARTIAL, 0, "No symbols synced")
    except Exception as e:
        logger.exception("VNPy sync failed for %s: %s", sync_date, e)
        return SyncResult(SyncStatus.ERROR, 0, str(e))


def run_vnpy_sync_job(sync_date: Optional[date] = None) -> SyncResult:
    """Run the VNPy sync job.

    If sync_date is None, uses the latest sync'd tushare stock_daily date.
    Records status in data_sync_status under source='vnpy', interface_key='vnpy_sync'.
    """
    from app.datasync.service.sync_engine import (
        _write_status,
        _get_status,
        get_previous_trade_date,
    )

    if sync_date is None:
        sync_date = get_previous_trade_date()

    source = "vnpy"
    iface_key = "vnpy_sync"

    # Skip if already done
    existing = _get_status(sync_date, source, iface_key)
    if existing == SyncStatus.SUCCESS.value:
        logger.info("VNPy sync for %s already done, skipping", sync_date)
        return SyncResult(SyncStatus.SUCCESS, 0)

    _write_status(sync_date, source, iface_key, SyncStatus.RUNNING.value)

    result = sync_vnpy_for_date(sync_date)
    _write_status(
        sync_date,
        source,
        iface_key,
        result.status.value,
        result.rows_synced,
        result.error_message,
    )

    logger.info("VNPy sync for %s: %s (%d rows)", sync_date, result.status.value, result.rows_synced)
    return result
