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

# Define queues with priorities
QUEUE_HIGH = Queue(
    "high",
    connection=redis_conn,
    default_timeout=get_runtime_int(
        env_keys="QUEUE_HIGH_DEFAULT_TIMEOUT_SECONDS",
        db_key="worker.queue_timeout.high",
        default=600,
    ),
)
QUEUE_DEFAULT = Queue(
    "default",
    connection=redis_conn,
    default_timeout=get_runtime_int(
        env_keys="QUEUE_DEFAULT_DEFAULT_TIMEOUT_SECONDS",
        db_key="worker.queue_timeout.default",
        default=1800,
    ),
)
QUEUE_LOW = Queue(
    "low",
    connection=redis_conn,
    default_timeout=get_runtime_int(
        env_keys="QUEUE_LOW_DEFAULT_TIMEOUT_SECONDS",
        db_key="worker.queue_timeout.low",
        default=3600,
    ),
)
QUEUE_BACKTEST = Queue(
    "backtest",
    connection=redis_conn,
    default_timeout=get_runtime_int(
        env_keys="QUEUE_BACKTEST_DEFAULT_TIMEOUT_SECONDS",
        db_key="worker.queue_timeout.backtest",
        default=3600,
    ),
)
QUEUE_OPTIMIZATION = Queue(
    "optimization",
    connection=redis_conn,
    default_timeout=get_runtime_int(
        env_keys="QUEUE_OPTIMIZATION_DEFAULT_TIMEOUT_SECONDS",
        db_key="worker.queue_timeout.optimization",
        default=7200,
    ),
)
QUEUE_RDAGENT = Queue(
    "rdagent",
    connection=redis_conn,
    default_timeout=get_runtime_int(
        env_keys="QUEUE_RDAGENT_DEFAULT_TIMEOUT_SECONDS",
        db_key="worker.queue_timeout.rdagent",
        default=14400,
    ),
)

# Queue registry
QUEUES = {
    "high": QUEUE_HIGH,
    "default": QUEUE_DEFAULT,
    "low": QUEUE_LOW,
    "backtest": QUEUE_BACKTEST,
    "optimization": QUEUE_OPTIMIZATION,
    "rdagent": QUEUE_RDAGENT,
}


def get_queue(name: str = "default") -> Queue:
    """Get queue by name."""
    return QUEUES.get(name, QUEUE_DEFAULT)
