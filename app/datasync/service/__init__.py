"""Datasync service subpackage.

Contains both the new plugin-based sync engine and legacy modules.
"""

from importlib import import_module

__all__ = [
    # New sync engine
    "sync_engine",
    "vnpy_sync",
    "init_service",
    # Legacy modules (backward compatibility)
    "akshare_ingest",
    "tushare_ingest",
    "data_sync_daemon",
    "tushare_sync_daemon",
]


def __getattr__(name: str):
    """Lazily expose legacy submodules for patch/import compatibility."""
    if name in __all__:
        module = import_module(f"{__name__}.{name}")
        globals()[name] = module
        return module
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
