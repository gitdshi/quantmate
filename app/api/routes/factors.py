"""Factor Lab routes — factor definitions and evaluations."""
from typing import Optional

from fastapi import APIRouter, Depends, Query, status
from pydantic import BaseModel

from app.api.services.auth_service import get_current_user
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
    current_user: dict = Depends(get_current_user),
):
    service = FactorService()
    total = service.count_factors(current_user["id"])
    rows = service.list_factors(
        current_user["id"], category=category,
        limit=pagination.page_size, offset=pagination.offset,
    )
    return paginate(rows, total, pagination)


@router.post("", status_code=status.HTTP_201_CREATED)
async def create_factor(req: FactorCreate, current_user: dict = Depends(get_current_user)):
    service = FactorService()
    return service.create_factor(
        current_user["id"], name=req.name, expression=req.expression,
        category=req.category, description=req.description, params=req.params,
    )


@router.get("/{factor_id}")
async def get_factor(factor_id: int, current_user: dict = Depends(get_current_user)):
    service = FactorService()
    try:
        return service.get_factor(current_user["id"], factor_id)
    except KeyError:
        raise APIError(status_code=404, code=ErrorCode.NOT_FOUND, message="Factor not found")


@router.put("/{factor_id}")
async def update_factor(factor_id: int, req: FactorUpdate, current_user: dict = Depends(get_current_user)):
    service = FactorService()
    try:
        return service.update_factor(current_user["id"], factor_id, **req.model_dump(exclude_none=True))
    except KeyError:
        raise APIError(status_code=404, code=ErrorCode.NOT_FOUND, message="Factor not found")


@router.delete("/{factor_id}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_factor(factor_id: int, current_user: dict = Depends(get_current_user)):
    service = FactorService()
    try:
        service.delete_factor(current_user["id"], factor_id)
    except KeyError:
        raise APIError(status_code=404, code=ErrorCode.NOT_FOUND, message="Factor not found")


# --- Evaluations ---

@router.get("/{factor_id}/evaluations")
async def list_evaluations(factor_id: int, current_user: dict = Depends(get_current_user)):
    service = FactorService()
    try:
        return service.list_evaluations(current_user["id"], factor_id)
    except KeyError:
        raise APIError(status_code=404, code=ErrorCode.NOT_FOUND, message="Factor not found")


@router.post("/{factor_id}/evaluations", status_code=status.HTTP_201_CREATED)
async def run_evaluation(factor_id: int, req: EvaluationRun, current_user: dict = Depends(get_current_user)):
    service = FactorService()
    try:
        return service.run_evaluation(current_user["id"], factor_id, req.start_date, req.end_date)
    except KeyError:
        raise APIError(status_code=404, code=ErrorCode.NOT_FOUND, message="Factor not found")


@router.delete("/{factor_id}/evaluations/{eval_id}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_evaluation(factor_id: int, eval_id: int, current_user: dict = Depends(get_current_user)):
    service = FactorService()
    try:
        service.delete_evaluation(current_user["id"], factor_id, eval_id)
    except KeyError as e:
        raise APIError(status_code=404, code=ErrorCode.NOT_FOUND, message=str(e).strip("'"))
