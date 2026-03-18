"""Optimization task routes (P2 Issue: Parameter Optimization Enhancement)."""

from fastapi import APIRouter, Depends, Query, status
from pydantic import BaseModel

from app.api.services.auth_service import get_current_user
from app.api.errors import ErrorCode
from app.api.exception_handlers import APIError
from app.domains.system.dao.optimization_dao import OptimizationTaskDao

router = APIRouter(prefix="/optimization", tags=["Optimization"])


class OptimizationCreateRequest(BaseModel):
    strategy_id: int
    search_method: str  # grid / random / bayesian
    param_space: dict
    objective_metric: str = "sharpe_ratio"


@router.get("/tasks")
async def list_optimization_tasks(
    page: int = Query(1, ge=1),
    page_size: int = Query(20, ge=1, le=100),
    current_user: dict = Depends(get_current_user),
):
    """List optimization tasks for the current user."""
    dao = OptimizationTaskDao()
    tasks, total = dao.list_by_user(current_user["id"], page=page, page_size=page_size)
    return {
        "data": tasks,
        "meta": {"page": page, "page_size": page_size, "total": total},
    }


@router.get("/tasks/{task_id}")
async def get_optimization_task(task_id: int, current_user: dict = Depends(get_current_user)):
    """Get optimization task detail."""
    dao = OptimizationTaskDao()
    task = dao.get_by_id(task_id, current_user["id"])
    if not task:
        raise APIError(status_code=404, code=ErrorCode.NOT_FOUND, message="Task not found")
    return task


@router.post("/tasks", status_code=status.HTTP_201_CREATED)
async def create_optimization_task(req: OptimizationCreateRequest, current_user: dict = Depends(get_current_user)):
    """Create a new optimization task."""
    valid_methods = ("grid", "random", "bayesian")
    if req.search_method not in valid_methods:
        raise APIError(status_code=400, code=ErrorCode.VALIDATION_ERROR, message="Invalid search method")
    dao = OptimizationTaskDao()
    task_id = dao.create(
        user_id=current_user["id"],
        strategy_id=req.strategy_id,
        search_method=req.search_method,
        param_space=req.param_space,
        objective_metric=req.objective_metric,
    )
    return {"id": task_id, "message": "Optimization task created"}


@router.get("/tasks/{task_id}/results")
async def get_optimization_results(task_id: int, current_user: dict = Depends(get_current_user)):
    """Get results for an optimization task."""
    dao = OptimizationTaskDao()
    results = dao.get_results(task_id, current_user["id"])
    return {"results": results}
