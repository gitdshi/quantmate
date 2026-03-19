"""Prometheus metrics for DataSync operations."""

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
    return generate_latest(REGISTRY)
