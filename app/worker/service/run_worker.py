"""Worker entry point (moved under app.worker.service).

This module is functionally identical to the previous `app.worker.run_worker`
but lives under `app.worker.service` so the package root (`app.worker`) can
remain minimal with only `main.py`.
"""

import os
# Qlib's contrib modules use MLflow for experiment tracking. Newer MLflow
# versions disable the filesystem backend by default; opt in early (before any
# mlflow import) so Qlib's internal recorder works without a DB backend.
os.environ.setdefault("MLFLOW_ALLOW_FILE_STORE", "true")

import sys
from pathlib import Path

# Ensure project root is importable
ROOT = Path(__file__).resolve().parents[3]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

# Change to project root
os.chdir(ROOT)

from dotenv import load_dotenv

load_dotenv()

from app.infrastructure.config import get_runtime_csv
from app.infrastructure.logging import configure_logging, get_logger  # noqa: E402

configure_logging()
logger = get_logger(__name__)

# Import to register tasks
from app.worker.service import tasks  # noqa
from app.worker.service.config import get_queues, redis_conn

QUEUES = None


def get_default_queue_names() -> list[str]:
    """Resolve the queue list for this worker.

    SPEC-OPS-007: ``WORKER_EXCLUDE_QUEUES`` lets a deployment split slow
    queues (e.g. rdagent, whose jobs can run for hours) onto a dedicated
    worker while keeping the default behavior identical for everyone else.
    """
    queue_names = get_runtime_csv(
        env_keys="WORKER_DEFAULT_QUEUE_NAMES",
        db_key="worker.default_queue_names",
        default=["backtest", "optimization", "default", "low", "rdagent"],
    )
    if "rdagent" not in queue_names:
        queue_names.append("rdagent")

    excluded = get_runtime_csv(
        env_keys="WORKER_EXCLUDE_QUEUES",
        db_key="worker.exclude_queue_names",
        default=[],
    )
    if excluded:
        queue_names = [name for name in queue_names if name not in excluded]
    return queue_names


def _reap_stale_rdagent_runs() -> None:
    """Best-effort zombie-run cleanup at worker startup (SPEC-OPS-006)."""
    try:
        from app.domains.factors.rdagent_service import reap_stale_rdagent_runs

        reaped = reap_stale_rdagent_runs()
        if reaped:
            logger.info("Reaped %d stale rdagent runs at startup", reaped)
    except Exception:
        logger.warning("rdagent stale-run reaper failed at startup", exc_info=True)


def main():
    from rq import Worker

    _reap_stale_rdagent_runs()
    queue_names = sys.argv[1:] if len(sys.argv) > 1 else get_default_queue_names()
    available_queues = QUEUES if isinstance(QUEUES, dict) else get_queues()
    queues = [available_queues[name] for name in queue_names if name in available_queues]
    if not queues:
        logger.error("No valid queues specified. Available queues: %s", list(available_queues.keys()))
        sys.exit(1)

    logger.info("Starting worker for queues: %s", queue_names)
    worker = Worker(queues, connection=redis_conn)
    worker.work(with_scheduler=True)


if __name__ == "__main__":
    main()
