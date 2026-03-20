"""AI Model (Qlib) routes — train, predict, and manage ML models."""

from __future__ import annotations

from typing import Any, Dict, List, Optional

from fastapi import APIRouter, BackgroundTasks, Depends, Query
from pydantic import BaseModel, Field

from app.api.errors import ErrorCode
from app.api.exception_handlers import APIError
from app.api.models.user import TokenData
from app.api.services.auth_service import get_current_user
from app.infrastructure.qlib.qlib_config import is_qlib_available

router = APIRouter(prefix="/ai/qlib", tags=["AI Qlib Models"])


# ── Request / Response models ───────────────────────────────────────────


class TrainModelRequest(BaseModel):
    model_type: str = Field(default="LightGBM", description="Model from Qlib zoo")
    factor_set: str = Field(default="Alpha158", description="Alpha158 or Alpha360")
    universe: str = Field(default="csi300", description="csi300, csi500, all_a")
    train_start: str = "2018-01-01"
    train_end: str = "2022-12-31"
    valid_start: str = "2023-01-01"
    valid_end: str = "2023-06-30"
    test_start: str = "2023-07-01"
    test_end: str = "2024-12-31"
    hyperparams: Optional[Dict[str, Any]] = None


class DataConvertRequest(BaseModel):
    start_date: Optional[str] = None
    end_date: Optional[str] = None
    use_akshare_supplement: bool = False


# ── Status check ────────────────────────────────────────────────────────


@router.get("/status")
async def qlib_status(current_user: TokenData = Depends(get_current_user)):
    """Check if Qlib is available and configured."""
    available = is_qlib_available()
    return {"available": available, "message": "Qlib is ready" if available else "pyqlib not installed"}


# ── Supported models / datasets ─────────────────────────────────────────


@router.get("/supported-models")
async def list_supported_models(current_user: TokenData = Depends(get_current_user)):
    """List available Qlib model types."""
    from app.domains.ai.qlib_model_service import QlibModelService

    return QlibModelService().list_supported_models()


@router.get("/supported-datasets")
async def list_supported_datasets(current_user: TokenData = Depends(get_current_user)):
    """List available factor datasets."""
    from app.domains.ai.qlib_model_service import QlibModelService

    return QlibModelService().list_supported_datasets()


# ── Training runs ────────────────────────────────────────────────────────


@router.post("/train")
async def train_model(
    req: TrainModelRequest,
    background_tasks: BackgroundTasks,
    current_user: TokenData = Depends(get_current_user),
):
    """Submit a Qlib model training job (runs in background)."""
    if not is_qlib_available():
        raise APIError(status_code=503, code=ErrorCode.SERVICE_UNAVAILABLE, message="Qlib is not installed")

    from app.worker.service.qlib_tasks import run_qlib_training_task

    background_tasks.add_task(
        run_qlib_training_task,
        user_id=current_user.user_id,
        model_type=req.model_type,
        factor_set=req.factor_set,
        universe=req.universe,
        train_start=req.train_start,
        train_end=req.train_end,
        valid_start=req.valid_start,
        valid_end=req.valid_end,
        test_start=req.test_start,
        test_end=req.test_end,
        hyperparams=req.hyperparams,
    )

    return {"status": "queued", "message": f"Training {req.model_type} model submitted"}


@router.get("/training-runs")
async def list_training_runs(
    status: Optional[str] = None,
    limit: int = Query(20, ge=1, le=100),
    offset: int = Query(0, ge=0),
    current_user: TokenData = Depends(get_current_user),
):
    """List user's Qlib training runs."""
    from app.domains.ai.qlib_model_service import QlibModelService

    return QlibModelService().list_training_runs(
        user_id=current_user.user_id,
        status=status,
        limit=limit,
        offset=offset,
    )


@router.get("/training-runs/{run_id}")
async def get_training_run(run_id: int, current_user: TokenData = Depends(get_current_user)):
    """Get details of a training run."""
    from app.domains.ai.qlib_model_service import QlibModelService

    run = QlibModelService().get_training_run(run_id)
    if not run:
        raise APIError(status_code=404, code=ErrorCode.NOT_FOUND, message="Training run not found")
    return run


@router.get("/training-runs/{run_id}/predictions")
async def get_predictions(
    run_id: int,
    trade_date: Optional[str] = None,
    top_n: int = Query(50, ge=1, le=500),
    current_user: TokenData = Depends(get_current_user),
):
    """Get model predictions for a training run."""
    from app.domains.ai.qlib_model_service import QlibModelService

    return QlibModelService().get_predictions(
        training_run_id=run_id,
        trade_date=trade_date,
        top_n=top_n,
    )


# ── Data conversion ─────────────────────────────────────────────────────


@router.post("/data/convert")
async def convert_data(
    req: DataConvertRequest,
    background_tasks: BackgroundTasks,
    current_user: TokenData = Depends(get_current_user),
):
    """Convert tushare/akshare data to Qlib format (runs in background)."""
    from app.worker.service.qlib_tasks import run_data_conversion_task

    background_tasks.add_task(
        run_data_conversion_task,
        start_date=req.start_date,
        end_date=req.end_date,
        use_akshare_supplement=req.use_akshare_supplement,
    )

    return {"status": "queued", "message": "Data conversion job submitted"}
