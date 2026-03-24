"""Data source plugin registry — discovers and manages all data source plugins."""

from __future__ import annotations

import logging
from typing import Optional

from app.datasync.base import BaseDataSource, BaseIngestInterface

logger = logging.getLogger(__name__)


class DataSourceRegistry:
    """Central registry for all data source plugins."""

    _sources: dict[str, BaseDataSource]

    def __init__(self) -> None:
        self._sources = {}

    def register(self, source: BaseDataSource) -> None:
        key = source.source_key
        if key in self._sources:
            logger.warning("Data source '%s' already registered, overwriting", key)
        self._sources[key] = source
        logger.info("Registered data source: %s (%s)", key, source.display_name)

    def get_source(self, source_key: str) -> Optional[BaseDataSource]:
        return self._sources.get(source_key)

    def all_sources(self) -> list[BaseDataSource]:
        return list(self._sources.values())

    def get_interface(self, source_key: str, interface_key: str) -> Optional[BaseIngestInterface]:
        source = self.get_source(source_key)
        if source is None:
            return None
        return source.get_interface(interface_key)

    def all_interfaces(self) -> list[BaseIngestInterface]:
        result = []
        for source in self._sources.values():
            result.extend(source.get_interfaces())
        return result


def build_default_registry() -> DataSourceRegistry:
    """Construct the registry with all built-in data sources."""
    registry = DataSourceRegistry()

    # Import and register Tushare
    try:
        from app.datasync.sources.tushare.source import TushareDataSource

        registry.register(TushareDataSource())
    except Exception:
        logger.exception("Failed to register Tushare data source")

    # Import and register AkShare
    try:
        from app.datasync.sources.akshare.source import AkShareDataSource

        registry.register(AkShareDataSource())
    except Exception:
        logger.exception("Failed to register AkShare data source")

    return registry
