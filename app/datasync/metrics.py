"""Prometheus metrics for DataSync operations."""

from collections import defaultdict
from typing import Optional
from prometheus_client import Counter, Gauge, generate_latest, REGISTRY

# Define metrics according to monitoring/datasync.rules.yml

# API call counters
datasync_api_calls_total = Counter("datasync_api_calls_total", "Total number of Tushare API calls", ["api_name"])

datasync_api_errors_total = Counter("datasync_api_errors_total", "Total number of Tushare API errors", ["api_name"])

# Rate limit hits counter
datasync_rate_limit_hits_total = Counter(
    "datasync_rate_limit_hits_total", "Number of times rate limit was hit", ["api_name"]
)

# Rows ingested counter (by table name)
datasync_rows_ingested_total = Counter(
    "datasync_rows_ingested_total", "Total number of rows successfully ingested into database tables", ["table"]
)

# Failed steps counter (by step/ingest function name)
datasync_failed_steps_total = Counter("datasync_failed_steps_total", "Total number of failed ingestion steps", ["step"])

# Backfill lock status gauge (0 = down/unavailable, 1 = up/available)
datasync_backfill_lock_status = Gauge(
    "datasync_backfill_lock_status", "Status of datasync backfill lock (0 = cannot acquire, 1 = healthy)"
)
# Avoid false-red on fresh process before the first lock check runs.
datasync_backfill_lock_status.set(1)
# Default to healthy/idle to avoid false-red alerts before the first lock check runs.
datasync_backfill_lock_status.set(1)


_KNOWN_API_NAMES = [
    "index_daily",
    "stock_basic",
    "stock_daily",
    "adj_factor",
    "dividend",
    "top10_holders",
    "stock_weekly",
    "stock_monthly",
    "index_weekly",
    "vnpy_sync",
]


def _set_counter_value(counter: Counter, value: float, **labels) -> None:
    """Force a deterministic sample value for a Prometheus counter child."""
    child = counter.labels(**labels)
    child._value.set(float(value))


def _hydrate_metrics_from_db() -> None:
    """Populate exported metric samples from persisted sync-status data.

    The `/metrics` endpoint runs in the API process, while datasync work runs in
    separate worker/daemon containers. Because Prometheus counters are process-
    local, we derive stable exported values from the database so the API process
    can expose real samples for DataSync observability.
    """
    from sqlalchemy import text

    from app.infrastructure.db.connections import get_quantmate_engine

    engine = get_quantmate_engine()
    rows_by_interface = defaultdict(lambda: {"runs": 0, "errors": 0, "rate_limits": 0, "rows": 0})
    failed_by_interface = defaultdict(int)

    with engine.connect() as conn:
        rows = conn.execute(
            text(
                """
                SELECT source, interface_key, status, rows_synced, error_message
                FROM data_sync_status
                """
            )
        ).fetchall()

    for source, interface_key, status, rows_synced, error_message in rows:
        api_name = interface_key or f"{source}_unknown"
        rows_by_interface[api_name]["runs"] += 1
        rows_by_interface[api_name]["rows"] += int(rows_synced or 0)

        if status == "error":
            rows_by_interface[api_name]["errors"] += 1
        if status in {"error", "partial"}:
            failed_by_interface[api_name] += 1

        msg = (error_message or "").lower()
        if any(token in msg for token in ["rate limit", "too many requests", "每分钟最多访问", "接口访问太频繁", "后重试", "频率"]):
            rows_by_interface[api_name]["rate_limits"] += 1

    for api_name in set(_KNOWN_API_NAMES) | set(rows_by_interface.keys()) | set(failed_by_interface.keys()):
        stats = rows_by_interface[api_name]
        _set_counter_value(datasync_api_calls_total, stats["runs"], api_name=api_name)
        _set_counter_value(datasync_api_errors_total, stats["errors"], api_name=api_name)
        _set_counter_value(datasync_rate_limit_hits_total, stats["rate_limits"], api_name=api_name)
        _set_counter_value(datasync_rows_ingested_total, stats["rows"], table=api_name)
        _set_counter_value(datasync_failed_steps_total, failed_by_interface[api_name], step=api_name)


# Metrics hook for call_pro integration
def metrics_hook(api_name: str, success: bool, duration: float, rows: int, error: Optional[str] = None):
    """Hook called by call_pro to record metrics.

    Args:
        api_name: Name of the Tushare API called
        success: Whether the call succeeded
        duration: Call duration in seconds (currently unused but reserved)
        rows: Number of rows returned/ingested
        error: Error message if failed
    """
    # Always record API call
    datasync_api_calls_total.labels(api_name=api_name).inc()

    if not success:
        datasync_api_errors_total.labels(api_name=api_name).inc()
        # Check if this is a rate limit error
        if error and any(
            token in error.lower()
            for token in ["rate limit", "too many requests", "每分钟最多访问", "接口访问太频繁", "后重试", "频率"]
        ):
            datasync_rate_limit_hits_total.labels(api_name=api_name).inc()
    else:
        # On success, record rows ingested if available
        if rows and rows > 0:
            datasync_rows_ingested_total.labels(table=api_name).inc()


def init_metrics():
    """Initialize metrics for datasync.

    Note: Prometheus Gauges default to 0. If we don't set an initial value,
    alert rules like `datasync_backfill_lock_status == 0` can fire even before
    any lock check runs.
    """
    from app.datasync.service.tushare_ingest import set_metrics_hook

    set_metrics_hook(metrics_hook)

    # Mark as healthy by default; subsequent lock operations should flip this to 0 on real failures.
    set_backfill_lock_status(True)


def set_backfill_lock_status(healthy: bool):
    """Update the backfill lock status gauge (1 = healthy/lock available, 0 = unhealthy/cannot acquire)."""
    datasync_backfill_lock_status.set(1 if healthy else 0)


# Expose function for FastAPI endpoint
def get_metrics():
    """Return metrics in Prometheus text format."""
    _hydrate_metrics_from_db()
    return generate_latest(REGISTRY)
