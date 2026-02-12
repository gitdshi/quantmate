"""Worker entry point (moved under app.worker.service).

This module is functionally identical to the previous `app.worker.run_worker`
but lives under `app.worker.service` so the package root (`app.worker`) can
remain minimal with only `main.py`.
"""
import os
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

from app.infrastructure.logging import configure_logging, get_logger  # noqa: E402
configure_logging()
logger = get_logger(__name__)

# Import to register tasks
from app.worker.service import tasks  # noqa
from app.worker.service.config import redis_conn, QUEUES

def main():
    from rq import Worker
    queue_names = sys.argv[1:] if len(sys.argv) > 1 else ['backtest', 'optimization', 'default']
    queues = [QUEUES[name] for name in queue_names if name in QUEUES]
    if not queues:
        logger.error("No valid queues specified. Available queues: %s", list(QUEUES.keys()))
        sys.exit(1)

    logger.info("Starting worker for queues: %s", queue_names)
    worker = Worker(queues, connection=redis_conn)
    worker.work(with_scheduler=True)


if __name__ == "__main__":
    main()
