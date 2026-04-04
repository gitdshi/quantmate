"""Optimization task routes (P2 Issue: Parameter Optimization Enhancement)."""

import logging

from fastapi import APIRouter, Depends, Query, status
from pydantic import BaseModel

from app.api.dependencies.permissions import require_permission
from app.api.services.auth_service import get_current_user
from app.api.models.user import TokenData
from app.api.errors import ErrorCode
from app.api.exception_handlers import APIError
from app.domains.system.dao.optimization_dao import OptimizationTaskDao
from app.worker.service.config import get_queue

router = APIRouter(prefix="/optimization", tags=["Optimization"])
logger = logging.getLogger(__name__)


class OptimizationCreateRequest(BaseModel):
    strategy_id: int
    search_method: str  # grid / random / bayesian
    param_space: dict
    objective_metric: str = "sharpe_ratio"


@router.get("/tasks", dependencies=[require_permission("backtests", "read")])
async def list_optimization_tasks(
    page: int = Query(1, ge=1),
    page_size: int = Query(20, ge=1, le=100),
    current_user: TokenData = Depends(get_current_user),
):
    """List optimization tasks for the current user."""
    dao = OptimizationTaskDao()
    tasks, total = dao.list_by_user(current_user.user_id, page=page, page_size=page_size)
    return {
        "data": tasks,
        "meta": {"page": page, "page_size": page_size, "total": total},
    }


@router.get("/tasks/{task_id}", dependencies=[require_permission("backtests", "read")])
async def get_optimization_task(task_id: int, current_user: TokenData = Depends(get_current_user)):
    """Get optimization task detail."""
    dao = OptimizationTaskDao()
    task = dao.get_by_id(task_id, current_user.user_id)
    if not task:
        raise APIError(status_code=404, code=ErrorCode.NOT_FOUND, message="Task not found")
    return task


@router.post("/tasks", status_code=status.HTTP_201_CREATED, dependencies=[require_permission("backtests", "write")])
async def create_optimization_task(req: OptimizationCreateRequest, current_user: TokenData = Depends(get_current_user)):
    """Create a new optimization task."""
    valid_methods = ("grid", "random", "bayesian")
    if req.search_method not in valid_methods:
        raise APIError(status_code=400, code=ErrorCode.VALIDATION_ERROR, message="Invalid search method")
    dao = OptimizationTaskDao()
    task_id = dao.create(
        user_id=current_user.user_id,
        strategy_id=req.strategy_id,
        search_method=req.search_method,
        param_space=req.param_space,
        objective_metric=req.objective_metric,
    )

    try:
        queue = get_queue("optimization")
        queue.enqueue(
            "app.worker.service.tasks.run_optimization_record_task",
            kwargs={"task_id": task_id},
            job_id=f"opt_task_{task_id}",
            job_timeout=14400,
            result_ttl=86400 * 3,
        )
    except Exception:
        logger.exception("Failed to enqueue optimization task %s", task_id)
        dao.update_status(task_id, "failed")
        raise APIError(
            status_code=500,
            code=ErrorCode.INTERNAL_ERROR,
            message="Failed to enqueue optimization task",
        )

    return {"id": task_id, "message": "Optimization task created"}


@router.delete("/tasks/{task_id}", status_code=status.HTTP_204_NO_CONTENT, dependencies=[require_permission("backtests", "write")])
async def delete_optimization_task(task_id: int, current_user: TokenData = Depends(get_current_user)):
    """Delete an optimization task and its result rows for the current user."""
    dao = OptimizationTaskDao()
    deleted = dao.delete_by_id(task_id, current_user.user_id)
    if not deleted:
        raise APIError(status_code=404, code=ErrorCode.NOT_FOUND, message="Task not found")
    return None


@router.get("/tasks/{task_id}/results", dependencies=[require_permission("backtests", "read")])
async def get_optimization_results(task_id: int, current_user: TokenData = Depends(get_current_user)):
    """Get results for an optimization task."""
    dao = OptimizationTaskDao()
    results = dao.get_results(task_id, current_user.user_id)
    return {"results": results}
