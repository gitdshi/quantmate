"""Data sync package for QuantMate.

Contains multi-source plugin architecture, sync engine, and legacy ingest modules.
"""

__all__ = [
    # New plugin architecture
    "base",
    "registry",
    "table_manager",
    "scheduler",
    # Legacy modules (backward compatibility)
    "data_sync_daemon",
    "tushare_ingest",
    "akshare_ingest",
]
