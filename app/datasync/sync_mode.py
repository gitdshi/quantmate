"""Helpers for DB-backed datasync sync mode metadata."""

from __future__ import annotations

from typing import Any

SYNC_MODE_BACKFILL = "backfill"
SYNC_MODE_LATEST_ONLY = "latest_only"

BACKFILL_MODE_RANGE = "range"
BACKFILL_MODE_DATE = "date"
BACKFILL_MODE_CODE = "code"
BACKFILL_MODE_CODE_DATE = "code_date"
BACKFILL_MODE_OTHER = "other"

_VALID_BACKFILL_MODES = {
    BACKFILL_MODE_RANGE,
    BACKFILL_MODE_DATE,
    BACKFILL_MODE_CODE,
    BACKFILL_MODE_CODE_DATE,
    BACKFILL_MODE_OTHER,
}


def normalize_sync_mode(value: Any, default: str = SYNC_MODE_BACKFILL) -> str:
    text = str(value or "").strip().lower()
    if text in {SYNC_MODE_BACKFILL, "historical"}:
        return SYNC_MODE_BACKFILL
    if text in {SYNC_MODE_LATEST_ONLY, "latest-only", "latest", "snapshot", "snapshot_only"}:
        return SYNC_MODE_LATEST_ONLY
    return default


def sync_mode_supports_backfill(value: Any) -> bool:
    return normalize_sync_mode(value) == SYNC_MODE_BACKFILL


def normalize_backfill_mode(value: Any, default: str = BACKFILL_MODE_DATE) -> str:
    text = str(value or "").strip().lower()
    if text in _VALID_BACKFILL_MODES:
        return text
    return default


def backfill_mode_uses_trade_calendar(value: Any) -> bool:
    return normalize_backfill_mode(value) in {
        BACKFILL_MODE_RANGE,
        BACKFILL_MODE_DATE,
        BACKFILL_MODE_CODE_DATE,
    }


def infer_backfill_mode_from_interface(iface: object) -> str:
    method = getattr(iface, "backfill_mode", None)
    if not callable(method):
        return BACKFILL_MODE_DATE
    try:
        return normalize_backfill_mode(method())
    except Exception:
        return BACKFILL_MODE_DATE


def infer_sync_mode_from_interface(iface: object) -> str:
    method = getattr(iface, "supports_backfill", None)
    if not callable(method):
        return SYNC_MODE_BACKFILL
    try:
        return SYNC_MODE_BACKFILL if bool(method()) else SYNC_MODE_LATEST_ONLY
    except Exception:
        return SYNC_MODE_BACKFILL