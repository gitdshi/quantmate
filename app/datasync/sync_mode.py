"""Helpers for DB-backed datasync sync mode metadata."""

from __future__ import annotations

from typing import Any

SYNC_MODE_BACKFILL = "backfill"
SYNC_MODE_LATEST_ONLY = "latest_only"


def normalize_sync_mode(value: Any, default: str = SYNC_MODE_BACKFILL) -> str:
    text = str(value or "").strip().lower()
    if text in {SYNC_MODE_BACKFILL, "historical"}:
        return SYNC_MODE_BACKFILL
    if text in {SYNC_MODE_LATEST_ONLY, "latest-only", "latest", "snapshot", "snapshot_only"}:
        return SYNC_MODE_LATEST_ONLY
    return default


def sync_mode_supports_backfill(value: Any) -> bool:
    return normalize_sync_mode(value) == SYNC_MODE_BACKFILL


def infer_sync_mode_from_interface(iface: object) -> str:
    method = getattr(iface, "supports_backfill", None)
    if not callable(method):
        return SYNC_MODE_BACKFILL
    try:
        return SYNC_MODE_BACKFILL if bool(method()) else SYNC_MODE_LATEST_ONLY
    except Exception:
        return SYNC_MODE_BACKFILL