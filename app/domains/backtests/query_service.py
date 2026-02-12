"""Compatibility shim for backtests query service.

This module existed previously; create a thin shim that re-exports the
`BulkBacktestQueryService` implementation from `service.py` so imports
elsewhere continue to work.
"""
from .service import BulkBacktestQueryService

__all__ = ["BulkBacktestQueryService"]
