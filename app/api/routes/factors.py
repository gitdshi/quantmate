"""Factor Lab routes — factor definitions and evaluations."""

from typing import Optional

from fastapi import APIRouter, Depends, status
from pydantic import BaseModel

from app.api.services.auth_service import get_current_user
from app.api.models.user import TokenData
from app.api.errors import ErrorCode
from app.api.exception_handlers import APIError
from app.api.pagination import PaginationParams, paginate
from app.domains.factors.service import FactorService

router = APIRouter(prefix="/factors", tags=["Factor Lab"])


class FactorCreate(BaseModel):
    name: str
    expression: str
    category: Optional[str] = None
    description: Optional[str] = None
    params: Optional[dict] = None


class FactorUpdate(BaseModel):
    name: Optional[str] = None
    expression: Optional[str] = None
    category: Optional[str] = None
    description: Optional[str] = None
    status: Optional[str] = None
    params: Optional[dict] = None


class EvaluationRun(BaseModel):
    start_date: str
    end_date: str


@router.get("")
async def list_factors(
    category: Optional[str] = None,
    pagination: PaginationParams = Depends(),
    current_user: TokenData = Depends(get_current_user),
):
    service = FactorService()
    total = service.count_factors(current_user.user_id)
    rows = service.list_factors(
        current_user.user_id,
        category=category,
        limit=pagination.page_size,
        offset=pagination.offset,
    )
    return paginate(rows, total, pagination)


@router.post("", status_code=status.HTTP_201_CREATED)
async def create_factor(req: FactorCreate, current_user: TokenData = Depends(get_current_user)):
    service = FactorService()
    return service.create_factor(
        current_user.user_id,
        name=req.name,
        expression=req.expression,
        category=req.category,
        description=req.description,
        params=req.params,
    )


@router.get("/{factor_id}")
async def get_factor(factor_id: int, current_user: TokenData = Depends(get_current_user)):
    service = FactorService()
    try:
        return service.get_factor(current_user.user_id, factor_id)
    except KeyError:
        raise APIError(status_code=404, code=ErrorCode.NOT_FOUND, message="Factor not found")


@router.put("/{factor_id}")
async def update_factor(factor_id: int, req: FactorUpdate, current_user: TokenData = Depends(get_current_user)):
    service = FactorService()
    try:
        return service.update_factor(current_user.user_id, factor_id, **req.model_dump(exclude_none=True))
    except KeyError:
        raise APIError(status_code=404, code=ErrorCode.NOT_FOUND, message="Factor not found")


@router.delete("/{factor_id}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_factor(factor_id: int, current_user: TokenData = Depends(get_current_user)):
    service = FactorService()
    try:
        service.delete_factor(current_user.user_id, factor_id)
    except KeyError:
        raise APIError(status_code=404, code=ErrorCode.NOT_FOUND, message="Factor not found")


# --- Evaluations ---


@router.get("/{factor_id}/evaluations")
async def list_evaluations(factor_id: int, current_user: TokenData = Depends(get_current_user)):
    service = FactorService()
    try:
        return service.list_evaluations(current_user.user_id, factor_id)
    except KeyError:
        raise APIError(status_code=404, code=ErrorCode.NOT_FOUND, message="Factor not found")


@router.post("/{factor_id}/evaluations", status_code=status.HTTP_201_CREATED)
async def run_evaluation(factor_id: int, req: EvaluationRun, current_user: TokenData = Depends(get_current_user)):
    service = FactorService()
    try:
        return service.run_evaluation(current_user.user_id, factor_id, req.start_date, req.end_date)
    except KeyError:
        raise APIError(status_code=404, code=ErrorCode.NOT_FOUND, message="Factor not found")


@router.delete("/{factor_id}/evaluations/{eval_id}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_evaluation(factor_id: int, eval_id: int, current_user: TokenData = Depends(get_current_user)):
    service = FactorService()
    try:
        service.delete_evaluation(current_user.user_id, factor_id, eval_id)
    except KeyError as e:
        raise APIError(status_code=404, code=ErrorCode.NOT_FOUND, message=str(e).strip("'"))


# --- Qlib Factor Sets ---


class QlibFactorRequest(BaseModel):
    factor_set: str = "Alpha158"
    instruments: str = "csi300"
    start_date: str = "2023-01-01"
    end_date: str = "2024-12-31"


@router.get("/qlib/factor-sets")
async def list_qlib_factor_sets(current_user: TokenData = Depends(get_current_user)):
    """List available Qlib factor sets (Alpha158, Alpha360)."""
    from app.infrastructure.qlib.qlib_config import SUPPORTED_DATASETS

    return [{"name": k, "class": v} for k, v in SUPPORTED_DATASETS.items()]


@router.post("/qlib/compute")
async def compute_qlib_factors(
    req: QlibFactorRequest,
    current_user: TokenData = Depends(get_current_user),
):
    """Compute Qlib factor set values for given instruments and date range.

    Data source: tushare/akshare via Qlib binary data (NOT vnpy DB).
    Returns a summary; actual factor values are stored in the qlib database.
    """
    from app.infrastructure.qlib.qlib_config import (
        SUPPORTED_DATASETS,
        ensure_qlib_initialized,
        is_qlib_available,
    )

    if not is_qlib_available():
        raise APIError(status_code=503, code=ErrorCode.SERVICE_UNAVAILABLE, message="Qlib is not installed")

    if req.factor_set not in SUPPORTED_DATASETS:
        raise APIError(
            status_code=400,
            code=ErrorCode.VALIDATION_ERROR,
            message=f"Unsupported factor set. Supported: {list(SUPPORTED_DATASETS.keys())}",
        )

    try:
        ensure_qlib_initialized()

        from qlib.utils import init_instance_by_config

        handler_class = SUPPORTED_DATASETS[req.factor_set]
        handler_config = {
            "class": handler_class.split(".")[-1],
            "module_path": handler_class.rsplit(".", 1)[0],
            "kwargs": {
                "instruments": req.instruments,
                "start_time": req.start_date,
                "end_time": req.end_date,
            },
        }

        handler = init_instance_by_config(handler_config)
        df = handler.fetch()

        if df is None or df.empty:
            return {"status": "empty", "message": "No factor data computed"}

        return {
            "status": "completed",
            "factor_set": req.factor_set,
            "instruments": req.instruments,
            "date_range": {"start": req.start_date, "end": req.end_date},
            "shape": {"rows": df.shape[0], "columns": df.shape[1]},
            "factor_names": list(df.columns)[:20],
            "sample_instruments": list(df.index.get_level_values(0).unique()[:10])
            if hasattr(df.index, "get_level_values")
            else [],
        }

    except Exception as e:
        raise APIError(status_code=500, code=ErrorCode.INTERNAL_ERROR, message=f"Factor computation failed: {str(e)}")
