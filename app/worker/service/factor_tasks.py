"""Factor mining pipeline worker tasks (TASK-004).

Wraps :func:`run_factor_mining_pipeline` so it can run in an RQ worker.
"""

from __future__ import annotations

import logging
import traceback
from datetime import date, datetime
from typing import Any, Dict, Optional

logger = logging.getLogger(__name__)


def run_factor_pipeline_task(
    user_id: int,
    source: str = "alpha158",
    universe: str = "csi300",
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    ic_threshold: float = 0.03,
    ir_threshold: float = 0.5,
    top_n: int = 20,
    auto_promote: bool = False,
) -> Dict[str, Any]:
    """Background task: run the factor mining pipeline."""
    try:
        from app.domains.factors.pipeline import run_factor_mining_pipeline

        sd = datetime.strptime(start_date, "%Y-%m-%d").date() if start_date else None
        ed = datetime.strptime(end_date, "%Y-%m-%d").date() if end_date else None

        logger.info(
            "[factor-worker] Starting pipeline: user=%d source=%s universe=%s",
            user_id,
            source,
            universe,
        )

        result = run_factor_mining_pipeline(
            user_id=user_id,
            source=source,
            universe=universe,
            start_date=sd,
            end_date=ed,
            ic_threshold=ic_threshold,
            ir_threshold=ir_threshold,
            top_n=top_n,
            auto_promote=auto_promote,
        )

        logger.info("[factor-worker] Pipeline done: %s", result)
        return result

    except Exception as e:
        logger.exception("[factor-worker] Pipeline failed: %s", e)
        return {
            "status": "failed",
            "error": str(e),
            "traceback": traceback.format_exc(),
        }


def enqueue_factor_pipeline(
    user_id: int,
    source: str = "alpha158",
    universe: str = "csi300",
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    auto_promote: bool = False,
    ic_threshold: float = 0.03,
    ir_threshold: float = 0.5,
    top_n: int = 20,
) -> Optional[str]:
    """Enqueue the factor pipeline on the default queue.

    Returns the RQ job id, or None if enqueue failed.
    """
    try:
        from app.worker.service.config import get_queue

        queue = get_queue("default")
        job = queue.enqueue(
            run_factor_pipeline_task,
            user_id=user_id,
            source=source,
            universe=universe,
            start_date=start_date,
            end_date=end_date,
            auto_promote=auto_promote,
            ic_threshold=ic_threshold,
            ir_threshold=ir_threshold,
            top_n=top_n,
            job_timeout=1800,
        )
        return job.id
    except Exception:
        logger.warning("[factor-worker] Failed to enqueue pipeline", exc_info=True)
        return None
