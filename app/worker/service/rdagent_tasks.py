"""RD-Agent background worker tasks — autonomous factor mining via RQ.

Follows the same pattern as qlib_tasks.py: lazy imports, try/except wrapping,
dict return with status.
"""

from __future__ import annotations

import logging
import traceback
from datetime import datetime
from typing import Any, Dict, Optional

logger = logging.getLogger(__name__)

# Lazy-loaded references
_RDAgentService = None
_feature_descriptor = None


def _get_rdagent_service():
    global _RDAgentService
    if _RDAgentService is None:
        from app.domains.factors.rdagent_service import RDAgentService

        _RDAgentService = RDAgentService
    return _RDAgentService


def _get_feature_descriptor():
    global _feature_descriptor
    if _feature_descriptor is None:
        from app.domains.factors import feature_descriptor

        _feature_descriptor = feature_descriptor
    return _feature_descriptor


def run_rdagent_mining_task(
    user_id: int,
    run_id: str,
    config_dict: Dict[str, Any],
) -> Dict[str, Any]:
    """Execute an RD-Agent factor mining run in a background worker.

    This task:
    1. Updates run status to 'running'
    2. Calls the RD-Agent sidecar API to start the mining loop
    3. Polls / streams iteration results back to the database
    4. Marks the run as completed or failed
    """
    try:
        logger.info("[rdagent-worker] Starting mining run %s for user %d", run_id, user_id)

        from app.domains.factors.rdagent_service import (
            _update_run_status,
            save_iteration,
            save_discovered_factor,
        )

        _update_run_status(run_id, "running")

        # Build feature context for the sidecar
        fd = _get_feature_descriptor()
        prompt_context = fd.build_prompt_context()

        # Call sidecar
        result = _call_sidecar_mining(
            run_id=run_id,
            config=config_dict,
            prompt_context=prompt_context,
        )

        if result.get("status") == "failed":
            _update_run_status(run_id, "failed", result.get("error"))
            return {
                "run_id": run_id,
                "status": "failed",
                "error": result.get("error"),
            }

        # Save iterations
        iterations = result.get("iterations", [])
        for it in iterations:
            save_iteration(
                run_id=run_id,
                iteration_number=it.get("iteration", 0),
                hypothesis=it.get("hypothesis"),
                experiment_code=it.get("code"),
                metrics=_serialize(it.get("metrics")),
                feedback=it.get("feedback"),
                status=it.get("status", "completed"),
            )

        # Save discovered factors
        factors = result.get("discovered_factors", [])
        for f in factors:
            save_discovered_factor(
                run_id=run_id,
                factor_name=f.get("name", "unnamed"),
                expression=f.get("expression", ""),
                description=f.get("description"),
                ic_mean=f.get("ic_mean"),
                icir=f.get("icir"),
                sharpe=f.get("sharpe"),
            )

        _update_run_status(run_id, "completed")

        return {
            "run_id": run_id,
            "status": "completed",
            "iterations": len(iterations),
            "discovered_factors": len(factors),
            "completed_at": datetime.utcnow().isoformat(),
        }

    except Exception as e:
        logger.exception("[rdagent-worker] Mining run %s failed: %s", run_id, e)
        try:
            from app.domains.factors.rdagent_service import _update_run_status

            _update_run_status(run_id, "failed", str(e))
        except Exception:
            logger.exception("[rdagent-worker] Failed to update run status")
        return {
            "run_id": run_id,
            "status": "failed",
            "error": str(e),
            "traceback": traceback.format_exc(),
        }


def _call_sidecar_mining(
    run_id: str,
    config: Dict[str, Any],
    prompt_context: str,
) -> Dict[str, Any]:
    """Call the RD-Agent sidecar container to run the mining loop.

    In production this makes HTTP requests to the sidecar.
    Returns a result dict with iterations and discovered_factors.
    """
    import httpx

    from app.infrastructure.config import get_settings

    settings = get_settings()
    sidecar_url = getattr(settings, "rdagent_sidecar_url", "http://rdagent-service:8001")

    payload = {
        "run_id": run_id,
        "config": config,
        "prompt_context": prompt_context,
    }

    try:
        with httpx.Client(timeout=httpx.Timeout(timeout=14400.0)) as client:
            resp = client.post(f"{sidecar_url}/mine", json=payload)
            resp.raise_for_status()
            return resp.json()
    except httpx.HTTPStatusError as exc:
        return {
            "status": "failed",
            "error": f"Sidecar returned {exc.response.status_code}: {exc.response.text[:500]}",
        }
    except httpx.ConnectError:
        return {
            "status": "failed",
            "error": "Cannot connect to RD-Agent sidecar. Is the service running?",
        }
    except Exception as exc:
        return {
            "status": "failed",
            "error": f"Sidecar call failed: {exc}",
        }


def _serialize(obj: Any) -> Optional[str]:
    if obj is None:
        return None
    import json

    return json.dumps(obj, ensure_ascii=False, default=str)
