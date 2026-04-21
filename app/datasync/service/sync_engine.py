"""Sync engine — dynamic, plugin-driven replacement for data_sync_daemon.py.

Reads enabled interfaces from DB, dispatches to registered plugins,
tracks per-interface per-trading-day status.

Supports parallel execution via ThreadPoolExecutor, with separate
per-source semaphores for daily sync and backfill so they can be tuned
independently per provider.
"""

from __future__ import annotations

import logging
from collections import deque
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from functools import lru_cache
from threading import Semaphore
from typing import Optional

from sqlalchemy import text

from app.datasync.base import SyncResult, SyncStatus
from app.datasync.registry import DataSourceRegistry
from app.datasync.sources.tushare.sync_error_handling import final_retry_count_for_result, is_quota_pause_result
from app.datasync.table_manager import ensure_table
from app.infrastructure.config import get_runtime_config, get_runtime_int
from app.infrastructure.db.connections import get_quantmate_engine

logger = logging.getLogger(__name__)

MAX_RETRIES = get_runtime_int(env_keys="MAX_RETRIES", db_key="datasync.max_retries", default=3)
PARALLEL_WORKERS = get_runtime_int(
    env_keys="SYNC_PARALLEL_WORKERS",
    db_key="datasync.sync_parallel_workers",
    default=4,
)
BACKFILL_WORKERS = get_runtime_int(
    env_keys="BACKFILL_WORKERS",
    db_key="datasync.backfill_workers",
    default=10,
)

_FORCE_RETRY_ERROR_SIGNATURES: dict[tuple[str, str], tuple[str, ...]] = {
    ("tushare", "trade_cal"): ("unknown column", "updated_at"),
    ("tushare", "block_trade"): ("unknown column", "symbol"),
}

# AkShare backfill can exercise py_mini_racer-backed endpoints (for example
# ETF history via Sina). On macOS, concurrent V8 initialization in those paths
# can abort the interpreter, so keep AkShare backfill serial unless explicitly
# overridden by env/DB config.
_BACKFILL_SOURCE_SAFE_DEFAULTS: dict[str, int] = {
    "akshare": 1,
}

# Maximum concurrent API calls per data source (respects Tushare rate limits)
SOURCE_CONCURRENCY: dict[str, int] = {
    "tushare": get_runtime_int(
        env_keys="TUSHARE_CONCURRENCY",
        db_key="datasync.source_concurrency.tushare",
        default=3,
    ),
}

_source_semaphores: dict[str, Semaphore] = {}
_backfill_source_semaphores: dict[str, Semaphore] = {}


@dataclass
class _BackfillTask:
    source: str
    iface_key: str
    iface: object
    dates: list[date]
    retry_counts: dict[date, int]
    mode: str

    @property
    def start_date(self) -> date:
        return self.dates[0]

    @property
    def end_date(self) -> date:
        return self.dates[-1]


def _get_source_env_key(source: str) -> str:
    return "".join(char if char.isalnum() else "_" for char in source.upper())


def _get_source_concurrency_limit(source: str) -> int | None:
    return SOURCE_CONCURRENCY.get(source)


def _get_backfill_source_concurrency_limit(source: str) -> int | None:
    default_limit = _BACKFILL_SOURCE_SAFE_DEFAULTS.get(source, _get_source_concurrency_limit(source))
    override = get_runtime_config(
        env_keys=f"BACKFILL_{_get_source_env_key(source)}_CONCURRENCY",
        db_key=f"datasync.backfill_source_concurrency.{source}",
        default=default_limit,
        parser=int,
    )
    if override is None:
        return None
    return max(1, int(override))


def _get_semaphore(cache: dict[str, Semaphore], source: str, limit: int | None) -> Semaphore | None:
    if limit is None:
        return None
    if source not in cache:
        cache[source] = Semaphore(limit)
    return cache[source]


def _get_source_semaphore(source: str) -> Semaphore | None:
    """Return the daily sync semaphore for the given source, or None for unlimited."""
    return _get_semaphore(_source_semaphores, source, _get_source_concurrency_limit(source))


def _get_backfill_source_semaphore(source: str) -> Semaphore | None:
    """Return the backfill semaphore for the given source, or None for unlimited."""
    return _get_semaphore(
        _backfill_source_semaphores,
        source,
        _get_backfill_source_concurrency_limit(source),
    )


# ---------------------------------------------------------------------------
# DAO helpers (operate on the new data_sync_status schema)
# ---------------------------------------------------------------------------


def _write_status(
    sync_date: date,
    source: str,
    interface_key: str,
    status: str,
    rows_synced: int = 0,
    error_message: Optional[str] = None,
    retry_count: int = 0,
) -> None:
    engine = get_quantmate_engine()
    with engine.begin() as conn:
        conn.execute(
            text(
                "INSERT INTO data_sync_status "
                "(sync_date, source, interface_key, status, rows_synced, error_message, retry_count, started_at, finished_at) "
                "VALUES (:sd, :src, :ik, :st, :rows, :err, :rc, NOW(), NOW()) "
                "ON DUPLICATE KEY UPDATE "
                "status=VALUES(status), rows_synced=VALUES(rows_synced), error_message=VALUES(error_message), "
                "retry_count=VALUES(retry_count), finished_at=NOW(), updated_at=CURRENT_TIMESTAMP"
            ),
            {
                "sd": sync_date,
                "src": source,
                "ik": interface_key,
                "st": status,
                "rows": rows_synced,
                "err": error_message,
                "rc": retry_count,
            },
        )


def _get_status(sync_date: date, source: str, interface_key: str) -> Optional[str]:
    status, _ = _get_status_snapshot(sync_date, source, interface_key)
    return status


def _get_status_snapshot(sync_date: date, source: str, interface_key: str) -> tuple[Optional[str], int]:
    engine = get_quantmate_engine()
    with engine.connect() as conn:
        row = conn.execute(
            text(
                "SELECT status, COALESCE(rows_synced, 0) "
                "FROM data_sync_status WHERE sync_date = :sd AND source = :src AND interface_key = :ik"
            ),
            {"sd": sync_date, "src": source, "ik": interface_key},
        ).fetchone()
        if row is None:
            return None, 0
        try:
            rows_synced = row[1]
        except Exception:
            rows_synced = 0

        try:
            normalized_rows = int(rows_synced or 0)
        except (TypeError, ValueError):
            normalized_rows = 0

        return row[0], normalized_rows


def _get_failed_records(
    start_date: date | None = None,
    end_date: date | None = None,
) -> list[tuple[date, str, str, int, str | None]]:
    """Return retryable records with enough context to decide whether they should be reopened."""
    end = end_date or (date.today() - timedelta(days=1))
    stale_hours = get_runtime_int(
        env_keys=("SYNC_STATUS_RUNNING_STALE_HOURS", "BACKFILL_LOCK_STALE_HOURS"),
        db_key="datasync.sync_status_running_stale_hours",
        default=get_runtime_int(
            env_keys="BACKFILL_LOCK_STALE_HOURS",
            db_key="datasync.backfill_lock_stale_hours",
            default=6,
        ),
    )
    stale_seconds = max(stale_hours, 0) * 3600
    where_clauses = [
        "sync_date <= :e",
        "("
        "status IN ('error', 'partial', 'pending') "
        "OR ("
        "status = 'running' "
        "AND TIMESTAMPDIFF(SECOND, COALESCE(updated_at, started_at, created_at), CURRENT_TIMESTAMP) >= :stale_seconds"
        ") "
        "OR (status = 'success' AND COALESCE(rows_synced, 0) = 0)"
        ")",
    ]
    params: dict[str, object] = {"e": end, "stale_seconds": stale_seconds}

    if start_date is not None:
        where_clauses.insert(0, "sync_date >= :s")
        params["s"] = start_date

    engine = get_quantmate_engine()
    with engine.connect() as conn:
        rows = conn.execute(
            text(
                "SELECT sync_date, source, interface_key, retry_count, error_message, status, COALESCE(rows_synced, 0), updated_at "
                "FROM data_sync_status "
                f"WHERE {' AND '.join(where_clauses)} "
                "ORDER BY sync_date DESC, source, interface_key"
            ),
            params,
        ).fetchall()
        records: list[tuple[date, str, str, int, str | None]] = []
        for row in rows:
            error_message = row[4] if len(row) > 4 else None
            status = row[5] if len(row) > 5 else None
            rows_synced = row[6] if len(row) > 6 else 0
            updated_at = row[7] if len(row) > 7 else None
            records.append((row[0], row[1], row[2], row[3], error_message, status, int(rows_synced or 0), updated_at))
        return records


def _parse_quota_retry_after_seconds(error_message: str | None) -> float | None:
    if not error_message:
        return None

    try:
        from app.datasync.service.tushare_ingest import parse_retry_after

        retry_after = parse_retry_after(error_message)
    except Exception:
        logger.exception("Failed to parse quota retry_after from error message: %s", error_message)
        return None

    if retry_after is None:
        return None

    try:
        return max(0.0, float(retry_after))
    except (TypeError, ValueError):
        return None


def _is_quota_cooldown_record(record: tuple[object, ...], now: datetime | None = None) -> bool:
    status = record[5] if len(record) > 5 else None
    error_message = record[4] if len(record) > 4 else None
    updated_at = record[7] if len(record) > 7 else None

    if status != SyncStatus.PENDING.value or not error_message or updated_at is None:
        return False

    retry_after_seconds = _parse_quota_retry_after_seconds(error_message)
    if retry_after_seconds is None or retry_after_seconds <= 0:
        return False

    if not isinstance(updated_at, datetime):
        return False

    current_time = now or datetime.now(updated_at.tzinfo)
    return updated_at + timedelta(seconds=retry_after_seconds) > current_time


def _is_quota_pending_record(record: tuple[object, ...]) -> bool:
    status = record[5] if len(record) > 5 else None
    error_message = record[4] if len(record) > 4 else None
    return status == SyncStatus.PENDING.value and _parse_quota_retry_after_seconds(error_message) is not None


def _filter_backfill_retry_records(records: list[tuple[object, ...]], now: datetime | None = None) -> list[tuple[object, ...]]:
    filtered: list[tuple[object, ...]] = []
    current_time = now or datetime.now()
    seen_quota_interfaces: set[tuple[str, str]] = set()

    for record in records:
        if _is_quota_cooldown_record(record, now=current_time):
            sync_date, source, iface_key = record[:3]
            logger.info(
                "Skipping backfill retry for quota-cooled record %s/%s@%s until cooldown expires",
                source,
                iface_key,
                sync_date,
            )
            continue

        if _is_quota_pending_record(record):
            source = str(record[1])
            iface_key = str(record[2])
            quota_key = (source, iface_key)
            if quota_key in seen_quota_interfaces:
                logger.info(
                    "Skipping additional quota-limited retry for %s/%s in this backfill pass",
                    source,
                    iface_key,
                )
                continue
            seen_quota_interfaces.add(quota_key)

        filtered.append(record)

    return filtered


def _can_force_retry_terminal_error(source: str, iface_key: str, error_message: str | None) -> bool:
    if not error_message:
        return False

    signatures = _FORCE_RETRY_ERROR_SIGNATURES.get((source, iface_key))
    if not signatures:
        return False

    normalized_error = error_message.lower()
    return all(signature in normalized_error for signature in signatures)


def _effective_retry_count(record: tuple[date, str, str, int] | tuple[date, str, str, int, str | None]) -> int | None:
    sync_date, source, iface_key, retry_count = record[:4]
    error_message = record[4] if len(record) > 4 else None

    if retry_count < MAX_RETRIES:
        return retry_count

    if _can_force_retry_terminal_error(source, iface_key, error_message):
        logger.info(
            "Backfill reopening %s/%s@%s after recoverable terminal error at retry_count=%d",
            source,
            iface_key,
            sync_date,
            retry_count,
        )
        return 0

    return None


def _is_zero_row_success_record(record: tuple[object, ...]) -> bool:
    status = record[5] if len(record) > 5 else None
    rows_synced = record[6] if len(record) > 6 else None
    return status == SyncStatus.SUCCESS.value and int(rows_synced or 0) == 0


def _group_backfill_records_by_date(
    records: list[tuple[date, str, str, int]],
) -> list[tuple[date, list[tuple[date, str, str, int]]]]:
    grouped: list[tuple[date, list[tuple[date, str, str, int]]]] = []
    current_date: date | None = None
    current_group: list[tuple[date, str, str, int]] = []

    for record in records:
        sync_date = record[0]
        if current_date != sync_date:
            if current_group:
                grouped.append((current_date, current_group))
            current_date = sync_date
            current_group = [record]
        else:
            current_group.append(record)

    if current_group:
        grouped.append((current_date, current_group))

    return grouped


def _supports_backfill(iface, source: str, iface_key: str) -> bool:
    supports_backfill = True
    method = getattr(iface, "supports_backfill", None)
    if callable(method):
        try:
            supports_backfill = bool(method())
        except Exception:
            logger.exception("Failed to inspect backfill support for %s/%s", source, iface_key)
    return supports_backfill


def _supports_scheduled_sync(iface, source: str, iface_key: str) -> bool:
    supports_scheduled_sync = True
    method = getattr(iface, "supports_scheduled_sync", None)
    if callable(method):
        try:
            supports_scheduled_sync = bool(method())
        except Exception:
            logger.exception("Failed to inspect scheduled-sync support for %s/%s", source, iface_key)
            supports_scheduled_sync = False
    return supports_scheduled_sync


def _get_backfill_mode(iface, source: str, iface_key: str) -> str:
    mode = "date"
    method = getattr(iface, "backfill_mode", None)
    if callable(method):
        try:
            candidate = method()
            if candidate in {"date", "range"}:
                mode = candidate
            elif candidate is not None:
                logger.warning(
                    "Invalid backfill_mode=%r for %s/%s, falling back to date mode",
                    candidate,
                    source,
                    iface_key,
                )
        except Exception:
            logger.exception("Failed to inspect backfill mode for %s/%s", source, iface_key)
    return mode


def _group_contiguous_trade_dates(dates: list[date]) -> list[list[date]]:
    ordered_dates = sorted(set(dates))
    if not ordered_dates:
        return []
    if len(ordered_dates) == 1:
        return [ordered_dates]

    calendar_positions: dict[date, int] = {}
    try:
        trade_calendar = get_trade_calendar(ordered_dates[0], ordered_dates[-1])
        calendar_positions = {trade_date: index for index, trade_date in enumerate(trade_calendar)}
    except Exception:
        logger.exception(
            "Failed to load trade calendar for backfill range grouping %s -> %s",
            ordered_dates[0],
            ordered_dates[-1],
        )

    groups: list[list[date]] = []
    current_group = [ordered_dates[0]]
    previous_date = ordered_dates[0]
    previous_pos = calendar_positions.get(previous_date)

    for current_date in ordered_dates[1:]:
        current_pos = calendar_positions.get(current_date)
        is_contiguous = previous_pos is not None and current_pos is not None and current_pos == previous_pos + 1
        if not is_contiguous and (current_date - previous_date).days == 1:
            is_contiguous = True

        if is_contiguous:
            current_group.append(current_date)
        else:
            groups.append(current_group)
            current_group = [current_date]

        previous_date = current_date
        previous_pos = current_pos

    groups.append(current_group)
    return groups


def _normalize_log_value(value) -> str:
    if value is None:
        return "-"
    if isinstance(value, (list, tuple, set)):
        return ",".join(str(item) for item in value) if value else "-"
    if isinstance(value, dict):
        if not value:
            return "-"
        return ",".join(f"{key}={value[key]}" for key in sorted(value))
    text = str(value).strip()
    return text if text else "-"


def _build_backfill_log_context(
    sync_date: date,
    source: str,
    iface_key: str,
    result: SyncResult,
    iface=None,
) -> dict[str, str]:
    details = result.details or {}
    symbols = details.get("symbols")
    if symbols is None and iface is not None:
        symbols = getattr(iface, "SYMBOLS", None)
    if symbols is None and iface is not None:
        symbols = getattr(iface, "ETF_SYMBOLS", None)

    failed_symbols = details.get("failed_symbols")
    if failed_symbols is None and result.error_message and result.error_message.startswith("Failed:"):
        failed_symbols = result.error_message.split(":", 1)[1].strip()

    context = {
        "date": sync_date.isoformat(),
        "interface": f"{source}/{iface_key}",
        "status": result.status.value,
        "rows": str(result.rows_synced),
        "symbols": _normalize_log_value(symbols),
        "failed_symbols": _normalize_log_value(failed_symbols),
        "message": _normalize_log_value(result.error_message),
    }

    for key, value in sorted(details.items()):
        if key in {"symbols", "failed_symbols"}:
            continue
        context[key] = _normalize_log_value(value)

    return context


def _log_backfill_result(
    sync_date: date,
    source: str,
    iface_key: str,
    result: SyncResult,
    iface=None,
) -> None:
    context = _build_backfill_log_context(sync_date, source, iface_key, result, iface)
    extra_fields = " ".join(
        f"{key}={value}"
        for key, value in context.items()
        if key not in {"date", "interface", "status", "rows", "symbols", "failed_symbols", "message"}
    )
    logger.info(
        "Backfill result date=%s interface=%s status=%s rows=%s symbols=%s failed_symbols=%s message=%s%s",
        context["date"],
        context["interface"],
        context["status"],
        context["rows"],
        context["symbols"],
        context["failed_symbols"],
        context["message"],
        f" {extra_fields}" if extra_fields else "",
    )


def _get_backfill_rows_by_date(task: _BackfillTask, result: SyncResult) -> dict[date, int]:
    if task.mode != "range":
        return {task.start_date: result.rows_synced}

    method = getattr(task.iface, "get_backfill_rows_by_date", None)
    if callable(method):
        try:
            rows_by_date = method(task.start_date, task.end_date) or {}
            normalized: dict[date, int] = {}
            for raw_date, raw_rows in rows_by_date.items():
                sync_date = raw_date
                if isinstance(raw_date, str):
                    sync_date = date.fromisoformat(raw_date)
                normalized[sync_date] = int(raw_rows)
            return normalized
        except Exception:
            logger.exception(
                "Failed to load per-date backfill counts for %s/%s %s -> %s",
                task.source,
                task.iface_key,
                task.start_date,
                task.end_date,
            )

    if len(task.dates) == 1:
        return {task.start_date: result.rows_synced}
    return {}


def _build_backfill_log_result(task: _BackfillTask, result: SyncResult) -> SyncResult:
    details = dict(result.details or {})
    details.setdefault("backfill_mode", task.mode)
    if task.mode == "range":
        details.setdefault("range_start", task.start_date.isoformat())
        details.setdefault("range_end", task.end_date.isoformat())
        details.setdefault("date_count", len(task.dates))
    return SyncResult(result.status, result.rows_synced, result.error_message, details=details)


def _submit_backfill_task(
    executor: ThreadPoolExecutor,
    future_map: dict,
    active_interfaces: set[tuple[str, str]],
    task: _BackfillTask,
) -> None:
    interface_key = (task.source, task.iface_key)
    active_interfaces.add(interface_key)
    for sync_date in task.dates:
        attempt_retry_count = task.retry_counts[sync_date]
        _write_status(
            sync_date,
            task.source,
            task.iface_key,
            SyncStatus.RUNNING.value,
            retry_count=attempt_retry_count,
        )
    logger.info(
        "Backfill submit mode=%s range=%s->%s interface=%s retry_dates=%d",
        task.mode,
        task.start_date,
        task.end_date,
        f"{task.source}/{task.iface_key}",
        len(task.dates),
    )
    future = executor.submit(_execute_backfill_task, task)
    future_map[future] = task


def _pop_next_backfill_task(
    pending_tasks: deque[_BackfillTask],
    active_interfaces: set[tuple[str, str]],
    blocked_interfaces: set[tuple[str, str]],
) -> _BackfillTask | None:
    total_pending = len(pending_tasks)
    for _ in range(total_pending):
        task = pending_tasks.popleft()
        interface_key = (task.source, task.iface_key)
        if interface_key in blocked_interfaces or interface_key in active_interfaces:
            pending_tasks.append(task)
            continue
        return task
    return None


def _execute_backfill_task(task: _BackfillTask) -> SyncResult:
    sem = _get_backfill_source_semaphore(task.source)
    try:
        if sem:
            sem.acquire()
        info = getattr(task.iface, "info", None)
        target_database = getattr(info, "target_database", None)
        target_table = getattr(info, "target_table", None)
        if isinstance(target_database, str) and target_database and isinstance(target_table, str) and target_table:
            ensure_table(target_database, target_table, task.iface.get_ddl())
        if task.mode == "range":
            return task.iface.sync_range(task.start_date, task.end_date)
        return task.iface.sync_date(task.start_date)
    finally:
        if sem:
            sem.release()


def _get_enabled_items() -> list[dict]:
    """Return enabled data_source_items ordered by sync_priority."""
    engine = get_quantmate_engine()
    with engine.connect() as conn:
        rows = conn.execute(
            text(
                "SELECT dsi.source, dsi.item_key, dsi.target_database, dsi.target_table, "
                "dsi.table_created, dsi.sync_priority "
                "FROM data_source_items dsi "
                "JOIN data_source_configs dsc ON dsi.source = dsc.source_key AND dsc.enabled = 1 "
                "WHERE dsi.enabled = 1 "
                "ORDER BY dsi.sync_priority ASC"
            )
        ).fetchall()
        return [
            {
                "source": r[0],
                "item_key": r[1],
                "target_database": r[2],
                "target_table": r[3],
                "table_created": r[4],
                "sync_priority": r[5],
            }
            for r in rows
        ]


def _get_enabled_backfill_keys() -> set[tuple[str, str]]:
    """Return the set of enabled source/interface pairs managed by the registry backfill."""
    return {(item["source"], item["item_key"]) for item in _get_enabled_items()}


# ---------------------------------------------------------------------------
# Trade calendar
# ---------------------------------------------------------------------------


def get_trade_calendar(start_date: date, end_date: date) -> list[date]:
    """Re-use existing trade calendar logic from the old daemon."""
    from app.datasync.service.data_sync_daemon import get_trade_calendar as _get_cal

    return _get_cal(start_date, end_date)


def get_previous_trade_date(offset: int = 1) -> date:
    from app.datasync.service.data_sync_daemon import get_previous_trade_date as _get_prev

    return _get_prev(offset)


@lru_cache(maxsize=512)
def _is_trading_day(sync_date: date) -> bool:
    try:
        return sync_date in get_trade_calendar(sync_date, sync_date)
    except Exception:
        logger.exception("Failed to determine trading-day status for %s", sync_date)
        return sync_date.weekday() < 5


def _get_latest_completed_trade_date(today: date | None = None) -> date:
    current_date = today or date.today()
    window_start = current_date - timedelta(days=30)

    try:
        trade_days = get_trade_calendar(window_start, current_date)
    except Exception:
        logger.exception("Failed to load trade calendar for default daily target date")
        trade_days = []

    if not trade_days:
        return current_date - timedelta(days=1)

    latest_trade_day = trade_days[-1]
    if latest_trade_day == current_date:
        if len(trade_days) >= 2:
            return trade_days[-2]
        return current_date - timedelta(days=1)
    return latest_trade_day


def _resolve_backfill_window() -> tuple[date | None, date]:
    default_end_date = date.today() - timedelta(days=1)

    try:
        from app.datasync.service.init_service import get_coverage_window

        coverage_window = get_coverage_window(target_end_date=default_end_date)
        return coverage_window["start_date"], coverage_window["end_date"]
    except Exception:
        logger.exception("Failed to resolve default backfill window from coverage settings")
        return None, default_end_date


def _requires_nonempty_trading_day_data(iface, source: str, iface_key: str) -> bool:
    method = getattr(iface, "requires_nonempty_trading_day_data", None)
    if not callable(method):
        return False
    try:
        return bool(method())
    except Exception:
        logger.exception(
            "Failed to inspect nonempty-trading-day policy for %s/%s",
            source,
            iface_key,
        )
        return False


def _should_retry_zero_row_success(iface, sync_date: date, source: str, iface_key: str) -> bool:
    return _requires_nonempty_trading_day_data(iface, source, iface_key) and _is_trading_day(sync_date)


def _normalize_zero_row_success(
    iface,
    sync_date: date,
    source: str,
    iface_key: str,
    result: SyncResult,
) -> SyncResult:
    if result.status != SyncStatus.SUCCESS or result.rows_synced != 0:
        return result

    if not _should_retry_zero_row_success(iface, sync_date, source, iface_key):
        return result

    return SyncResult(
        SyncStatus.PENDING,
        0,
        result.error_message or "No rows synced for trading day; scheduled for retry",
        details=result.details,
    )


def _final_retry_count_for_result(result: SyncResult, attempt_retry_count: int) -> int:
    return final_retry_count_for_result(result, attempt_retry_count)


def _is_quota_pause_result(result: SyncResult) -> bool:
    return is_quota_pause_result(result)


# ---------------------------------------------------------------------------
# Core sync functions
# ---------------------------------------------------------------------------


def _sync_one_item(
    registry: DataSourceRegistry,
    item: dict,
    target_date: date,
    idx: int,
    total: int,
) -> tuple[str, dict]:
    """Sync a single item. Called from daily_sync threads."""
    source = item["source"]
    item_key = item["item_key"]
    label = f"{source}/{item_key}"

    sem = _get_source_semaphore(source)

    try:
        if sem:
            sem.acquire()

        iface = registry.get_interface(source, item_key)
        if iface is None:
            logger.warning("No interface registered for %s, skipping", label)
            return label, {"status": "skipped", "reason": "no plugin"}

        if not _supports_scheduled_sync(iface, source, item_key):
            message = "Skipped scheduled sync for runtime-unsupported interface"
            logger.info("[%d/%d] %s runtime scheduler disabled, skipping", idx, total, label)
            _write_status(target_date, source, item_key, SyncStatus.SUCCESS.value, 0, message)
            return label, {
                "status": SyncStatus.SUCCESS.value,
                "rows": 0,
                "skipped": True,
                "reason": "scheduled sync unsupported",
            }

        existing_status, existing_rows = _get_status_snapshot(target_date, source, item_key)
        if existing_status == SyncStatus.SUCCESS.value and not (
            existing_rows == 0 and _should_retry_zero_row_success(iface, target_date, source, item_key)
        ):
            logger.info("[%d/%d] %s already synced, skipping", idx, total, label)
            return label, {"status": "success", "skipped": True}

        if existing_status == SyncStatus.SUCCESS.value and existing_rows == 0:
            logger.info(
                "[%d/%d] %s reopening prior zero-row success for %s",
                idx,
                total,
                label,
                target_date,
            )

        # Always verify the physical table exists because catalog.table_created can drift.
        try:
            ensure_table(item["target_database"], item["target_table"], iface.get_ddl())
        except Exception as e:
            logger.exception("Failed to create table for %s: %s", label, e)
            _write_status(target_date, source, item_key, SyncStatus.ERROR.value, 0, f"DDL failed: {e}")
            return label, {"status": "error", "error": f"DDL failed: {e}"}

        _write_status(target_date, source, item_key, SyncStatus.RUNNING.value)

        try:
            result: SyncResult = iface.sync_date(target_date)
            result = _normalize_zero_row_success(iface, target_date, source, item_key, result)
            _write_status(
                target_date, source, item_key,
                result.status.value, result.rows_synced, result.error_message,
            )
            logger.info("[%d/%d] %s: %s (%d rows)", idx, total, label, result.status.value, result.rows_synced)
            return label, {"status": result.status.value, "rows": result.rows_synced, "error": result.error_message}
        except Exception as e:
            logger.exception("[%d/%d] %s failed: %s", idx, total, label, e)
            _write_status(target_date, source, item_key, SyncStatus.ERROR.value, 0, str(e))
            return label, {"status": "error", "rows": 0, "error": str(e)}
    finally:
        if sem:
            sem.release()


def daily_sync(
    registry: DataSourceRegistry,
    target_date: Optional[date] = None,
    continue_on_error: bool = True,
    max_workers: int | None = None,
) -> dict[str, dict]:
    """Run daily sync for all enabled interfaces on a given trading day.

    Uses a thread pool with per-source semaphores to respect rate limits
    while maximizing throughput across sources.
    """
    if target_date is None:
        target_date = _get_latest_completed_trade_date()

    if max_workers is None:
        max_workers = PARALLEL_WORKERS

    logger.info("=" * 80)
    logger.info("Daily sync starting for %s (workers=%d)", target_date, max_workers)
    logger.info("=" * 80)

    enabled_items = _get_enabled_items()
    results: dict[str, dict] = {}
    total = len(enabled_items)

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {
            executor.submit(_sync_one_item, registry, item, target_date, idx, total): f"{item['source']}/{item['item_key']}"
            for idx, item in enumerate(enabled_items, 1)
        }
        for future in as_completed(futures):
            label = futures[future]
            try:
                lbl, res = future.result()
                results[lbl] = res
            except Exception as e:
                logger.exception("Unexpected error in sync thread for %s: %s", label, e)
                results[label] = {"status": "error", "error": str(e)}
                if not continue_on_error:
                    # Cancel remaining futures
                    for f in futures:
                        f.cancel()
                    break

    logger.info("=" * 80)
    logger.info("Daily sync finished for %s", target_date)
    logger.info("=" * 80)

    return results


def backfill_retry(
    registry: DataSourceRegistry,
    max_workers: int | None = None,
    **_compat,
) -> dict[str, dict]:
    """Retry failed/pending syncs within the configured coverage window.

    Uses DB lock from the old daemon to prevent concurrent runs.
    """
    if max_workers is None:
        max_workers = BACKFILL_WORKERS
    max_workers = max(1, max_workers)
    window_start_date, window_end_date = _resolve_backfill_window()

    from app.domains.extdata.dao.data_sync_status_dao import (
        acquire_backfill_lock,
        release_backfill_lock,
        is_backfill_locked,
    )

    if is_backfill_locked():
        logger.warning("Backfill already running (DB locked), skipping")
        return {}

    try:
        acquire_backfill_lock()
    except Exception as e:
        logger.warning("Failed to acquire backfill lock: %s", e)
        return {}

    results: dict[str, dict] = {}
    try:
        enabled_backfill_keys = _get_enabled_backfill_keys()
        failed = [
            record
            for record in _filter_backfill_retry_records(_get_failed_records(window_start_date, window_end_date))
            if (record[1], record[2]) in enabled_backfill_keys
        ]
        pending_range_records: dict[tuple[str, str], dict[str, object]] = {}
        tasks: list[_BackfillTask] = []

        for record in failed:
            sync_date, source, iface_key, retry_count = record[:4]
            label = f"{source}/{iface_key}@{sync_date}"
            effective_retry_count = _effective_retry_count(record)

            if effective_retry_count is None:
                logger.debug("Skipping %s: max retries reached", label)
                continue

            iface = registry.get_interface(source, iface_key)
            if iface is None:
                logger.warning("Backfill skip date=%s interface=%s: no plugin registered", sync_date, f"{source}/{iface_key}")
                results[label] = {"status": "skipped", "reason": "no plugin"}
                continue

            if not _supports_scheduled_sync(iface, source, iface_key):
                logger.info("Skipping runtime-unsupported interface %s during backfill", label)
                _write_status(
                    sync_date,
                    source,
                    iface_key,
                    SyncStatus.SUCCESS.value,
                    0,
                    "Skipped scheduled sync for runtime-unsupported interface",
                    retry_count=effective_retry_count,
                )
                results[label] = {
                    "status": SyncStatus.SUCCESS.value,
                    "rows": 0,
                    "skipped": True,
                    "reason": "scheduled sync unsupported",
                }
                continue

            if _is_zero_row_success_record(record) and not _should_retry_zero_row_success(
                iface,
                sync_date,
                source,
                iface_key,
            ):
                logger.debug(
                    "Skipping zero-row success reopen for %s: empty result is allowed on %s",
                    label,
                    sync_date,
                )
                continue

            if not _supports_backfill(iface, source, iface_key):
                logger.info("Skipping historical backfill for non-historical interface %s", label)
                _write_status(
                    sync_date,
                    source,
                    iface_key,
                    SyncStatus.SUCCESS.value,
                    0,
                    "Skipped historical backfill for non-historical interface",
                    retry_count=effective_retry_count,
                )
                skipped_result = SyncResult(
                    SyncStatus.SUCCESS,
                    0,
                    "Skipped historical backfill for non-historical interface",
                )
                _log_backfill_result(sync_date, source, iface_key, skipped_result, iface)
                results[label] = {"status": SyncStatus.SUCCESS.value, "rows": 0, "skipped": True}
                continue

            mode = _get_backfill_mode(iface, source, iface_key)
            if mode == "range":
                task_key = (source, iface_key)
                bucket = pending_range_records.setdefault(
                    task_key,
                    {"iface": iface, "records": []},
                )
                bucket["records"].append((sync_date, effective_retry_count))
                continue

            tasks.append(
                _BackfillTask(
                    source=source,
                    iface_key=iface_key,
                    iface=iface,
                    dates=[sync_date],
                    retry_counts={sync_date: effective_retry_count + 1},
                    mode="date",
                )
            )

        for (source, iface_key), bucket in pending_range_records.items():
            records = bucket["records"]
            iface = bucket["iface"]
            retry_by_date = {sync_date: retry_count + 1 for sync_date, retry_count in records}
            for grouped_dates in _group_contiguous_trade_dates([sync_date for sync_date, _ in records]):
                tasks.append(
                    _BackfillTask(
                        source=source,
                        iface_key=iface_key,
                        iface=iface,
                        dates=grouped_dates,
                        retry_counts={sync_date: retry_by_date[sync_date] for sync_date in grouped_dates},
                        mode="range",
                    )
                )

        tasks.sort(
            key=lambda task: (
                -task.end_date.toordinal(),
                -task.start_date.toordinal(),
                task.source,
                task.iface_key,
            )
        )
        logger.info(
            "Backfill starting: records=%d tasks=%d workers=%d start_date=%s end_date=%s",
            len(failed),
            len(tasks),
            max_workers,
            window_start_date if window_start_date is not None else "-",
            window_end_date,
        )

        with ThreadPoolExecutor(max_workers=max_workers, thread_name_prefix="datasync-backfill") as executor:
            future_map = {}
            pending_tasks = deque(tasks)
            active_interfaces: set[tuple[str, str]] = set()
            blocked_interfaces: set[tuple[str, str]] = set()

            while pending_tasks or future_map:
                while len(future_map) < max_workers:
                    next_task = _pop_next_backfill_task(
                        pending_tasks,
                        active_interfaces,
                        blocked_interfaces,
                    )
                    if next_task is None:
                        break
                    _submit_backfill_task(executor, future_map, active_interfaces, next_task)

                if not future_map:
                    break

                future = next(as_completed(tuple(future_map)))
                task = future_map.pop(future)
                active_interfaces.discard((task.source, task.iface_key))
                log_label = f"{task.source}/{task.iface_key}@{task.start_date}"
                if task.mode == "range" and task.end_date != task.start_date:
                    log_label = f"{task.source}/{task.iface_key}@{task.start_date}->{task.end_date}"
                try:
                    result = future.result()
                    if _is_quota_pause_result(result):
                        interface_key = (task.source, task.iface_key)
                        blocked_interfaces.add(interface_key)
                        deferred_count = sum(
                            1
                            for pending_task in pending_tasks
                            if (pending_task.source, pending_task.iface_key) == interface_key
                        )
                        if deferred_count:
                            logger.info(
                                "Deferring remaining %d backfill tasks for quota-limited interface %s/%s until the next pass",
                                deferred_count,
                                task.source,
                                task.iface_key,
                            )

                    rows_by_date = _get_backfill_rows_by_date(task, result)
                    log_result = _build_backfill_log_result(task, result)
                    if task.mode == "date" and len(task.dates) == 1:
                        log_result = _normalize_zero_row_success(
                            task.iface,
                            task.start_date,
                            task.source,
                            task.iface_key,
                            log_result,
                        )
                    _log_backfill_result(task.end_date, task.source, task.iface_key, log_result, task.iface)

                    for sync_date in task.dates:
                        normalized_result = _normalize_zero_row_success(
                            task.iface,
                            sync_date,
                            task.source,
                            task.iface_key,
                            SyncResult(
                                result.status,
                                rows_by_date.get(sync_date, 0),
                                result.error_message,
                                details=result.details,
                            ),
                        )
                        final_retry_count = _final_retry_count_for_result(
                            normalized_result,
                            task.retry_counts[sync_date],
                        )
                        _write_status(
                            sync_date,
                            task.source,
                            task.iface_key,
                            normalized_result.status.value,
                            normalized_result.rows_synced,
                            normalized_result.error_message,
                            retry_count=final_retry_count,
                        )
                        results[f"{task.source}/{task.iface_key}@{sync_date}"] = {
                            "status": normalized_result.status.value,
                            "rows": normalized_result.rows_synced,
                            "error": normalized_result.error_message,
                            "backfill_mode": task.mode,
                            "range_start": task.start_date.isoformat(),
                            "range_end": task.end_date.isoformat(),
                        }
                except Exception as e:
                    logger.exception("Backfill %s failed: %s", log_label, e)
                    error_result = SyncResult(
                        SyncStatus.ERROR,
                        0,
                        str(e),
                        details={
                            "backfill_mode": task.mode,
                            "range_start": task.start_date.isoformat(),
                            "range_end": task.end_date.isoformat(),
                            "date_count": len(task.dates),
                        },
                    )
                    _log_backfill_result(task.end_date, task.source, task.iface_key, error_result, task.iface)
                    for sync_date in task.dates:
                        _write_status(
                            sync_date,
                            task.source,
                            task.iface_key,
                            SyncStatus.ERROR.value,
                            0,
                            str(e),
                            retry_count=task.retry_counts[sync_date],
                        )
                        results[f"{task.source}/{task.iface_key}@{sync_date}"] = {
                            "status": SyncStatus.ERROR.value,
                            "rows": 0,
                            "error": str(e),
                            "backfill_mode": task.mode,
                            "range_start": task.start_date.isoformat(),
                            "range_end": task.end_date.isoformat(),
                        }
    finally:
        try:
            release_backfill_lock()
        except Exception:
            logger.exception("Failed to release backfill lock")

    return results
