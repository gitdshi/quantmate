"""Worker and Queue Configuration."""

from redis import Redis
from rq import Queue
from app.infrastructure.config import get_runtime_int, get_settings

settings = get_settings()

# Redis connection
redis_conn = Redis(
    host=settings.redis_host,
    port=settings.redis_port,
    db=settings.redis_db,
    decode_responses=False,  # Keep bytes for job data
)

_QUEUE_SPECS = {
    "high": ("QUEUE_HIGH_DEFAULT_TIMEOUT_SECONDS", "worker.queue_timeout.high", 600),
    "default": ("QUEUE_DEFAULT_DEFAULT_TIMEOUT_SECONDS", "worker.queue_timeout.default", 1800),
    "low": ("QUEUE_LOW_DEFAULT_TIMEOUT_SECONDS", "worker.queue_timeout.low", 3600),
    "backtest": ("QUEUE_BACKTEST_DEFAULT_TIMEOUT_SECONDS", "worker.queue_timeout.backtest", 3600),
    "optimization": ("QUEUE_OPTIMIZATION_DEFAULT_TIMEOUT_SECONDS", "worker.queue_timeout.optimization", 7200),
    "rdagent": ("QUEUE_RDAGENT_DEFAULT_TIMEOUT_SECONDS", "worker.queue_timeout.rdagent", 14400),
}

QUEUES = None


def get_queues() -> dict[str, Queue]:
    if isinstance(QUEUES, dict):
        return QUEUES
    queues: dict[str, Queue] = {}
    for name, (env_key, db_key, default_timeout) in _QUEUE_SPECS.items():
        queues[name] = Queue(
            name,
            connection=redis_conn,
            default_timeout=get_runtime_int(
                env_keys=env_key,
                db_key=db_key,
                default=default_timeout,
            ),
        )
    return queues


def get_queue(name: str = "default") -> Queue:
    """Get queue by name."""
    queues = get_queues()
    return queues.get(name, queues["default"])
