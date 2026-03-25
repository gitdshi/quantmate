"""Abstract base classes for data source plugins."""

from __future__ import annotations

import logging
from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import date
from enum import Enum
from typing import Optional

logger = logging.getLogger(__name__)


class SyncStatus(str, Enum):
    PENDING = "pending"
    RUNNING = "running"
    SUCCESS = "success"
    PARTIAL = "partial"
    ERROR = "error"


@dataclass
class SyncResult:
    status: SyncStatus
    rows_synced: int = 0
    error_message: Optional[str] = None


@dataclass
class InterfaceInfo:
    """Static metadata about a data source interface (for seed data / registration)."""

    interface_key: str
    display_name: str
    source_key: str
    target_database: str
    target_table: str
    sync_priority: int = 100
    requires_permission: Optional[str] = None
    description: str = ""
    enabled_by_default: bool = False


class BaseIngestInterface(ABC):
    """One syncable data interface within a data source (e.g. stock_daily under tushare)."""

    @property
    @abstractmethod
    def info(self) -> InterfaceInfo:
        """Return static metadata for this interface."""

    @abstractmethod
    def get_ddl(self) -> str:
        """Return CREATE TABLE IF NOT EXISTS DDL for the target table."""

    @abstractmethod
    def sync_date(self, trade_date: date) -> SyncResult:
        """Sync data for a single trading day."""

    def sync_range(self, start: date, end: date) -> SyncResult:
        """Sync data for a date range (default: iterate sync_date).

        Override for APIs that support batch date-range fetches.
        """
        total_rows = 0
        errors = []
        cur = start
        from datetime import timedelta

        while cur <= end:
            result = self.sync_date(cur)
            total_rows += result.rows_synced
            if result.status == SyncStatus.ERROR:
                errors.append(f"{cur}: {result.error_message}")
            cur += timedelta(days=1)

        if errors:
            if total_rows > 0:
                return SyncResult(SyncStatus.PARTIAL, total_rows, "; ".join(errors[:5]))
            return SyncResult(SyncStatus.ERROR, 0, "; ".join(errors[:5]))
        return SyncResult(SyncStatus.SUCCESS, total_rows)


class BaseDataSource(ABC):
    """A data source plugin (e.g. Tushare, AkShare)."""

    @property
    @abstractmethod
    def source_key(self) -> str:
        """Unique key for this data source (e.g. 'tushare')."""

    @property
    @abstractmethod
    def display_name(self) -> str:
        """Human-readable name."""

    @property
    @abstractmethod
    def requires_token(self) -> bool:
        """Whether this source requires an API token."""

    @abstractmethod
    def get_interfaces(self) -> list[BaseIngestInterface]:
        """Return all interfaces provided by this source."""

    @abstractmethod
    def test_connection(self) -> bool:
        """Test whether the source is reachable and configured."""

    def get_interface(self, interface_key: str) -> Optional[BaseIngestInterface]:
        """Look up an interface by key."""
        for iface in self.get_interfaces():
            if iface.info.interface_key == interface_key:
                return iface
        return None
