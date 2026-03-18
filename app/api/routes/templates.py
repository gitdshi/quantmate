"""Strategy template & marketplace routes."""

from typing import Optional

from fastapi import APIRouter, Depends, status
from pydantic import BaseModel, Field

from app.api.services.auth_service import get_current_user
from app.api.errors import ErrorCode
from app.api.exception_handlers import APIError
from app.api.pagination import PaginationParams, paginate
from app.domains.templates.service import TemplateService

router = APIRouter(prefix="/templates", tags=["Strategy Templates"])


class TemplateCreate(BaseModel):
    name: str
    code: str
    category: Optional[str] = None
    description: Optional[str] = None
    params_schema: Optional[dict] = None
    default_params: Optional[dict] = None
    visibility: str = "private"


class TemplateUpdate(BaseModel):
    name: Optional[str] = None
    code: Optional[str] = None
    category: Optional[str] = None
    description: Optional[str] = None
    visibility: Optional[str] = None
    params_schema: Optional[dict] = None
    default_params: Optional[dict] = None


class CommentCreate(BaseModel):
    content: str
    parent_id: Optional[int] = None


class RatingCreate(BaseModel):
    rating: int = Field(ge=1, le=5)
    review: Optional[str] = None


# --- Marketplace (public) ---


@router.get("/marketplace")
async def list_marketplace(
    category: Optional[str] = None,
    pagination: PaginationParams = Depends(),
    current_user: dict = Depends(get_current_user),
):
    service = TemplateService()
    total = service.count_marketplace(category=category)
    rows = service.list_marketplace(category=category, limit=pagination.page_size, offset=pagination.offset)
    return paginate(rows, total, pagination)


# --- My templates ---


@router.get("/mine")
async def list_my_templates(
    pagination: PaginationParams = Depends(),
    current_user: dict = Depends(get_current_user),
):
    service = TemplateService()
    total = service.count_my_templates(current_user["id"])
    rows = service.list_my_templates(current_user["id"], limit=pagination.page_size, offset=pagination.offset)
    return paginate(rows, total, pagination)


@router.post("", status_code=status.HTTP_201_CREATED)
async def create_template(req: TemplateCreate, current_user: dict = Depends(get_current_user)):
    service = TemplateService()
    return service.create_template(
        current_user["id"],
        name=req.name,
        code=req.code,
        category=req.category,
        description=req.description,
        params_schema=req.params_schema,
        default_params=req.default_params,
        visibility=req.visibility,
    )


@router.get("/{template_id}")
async def get_template(template_id: int, current_user: dict = Depends(get_current_user)):
    service = TemplateService()
    try:
        return service.get_template(template_id)
    except KeyError:
        raise APIError(status_code=404, code=ErrorCode.NOT_FOUND, message="Template not found")


@router.put("/{template_id}")
async def update_template(template_id: int, req: TemplateUpdate, current_user: dict = Depends(get_current_user)):
    service = TemplateService()
    try:
        return service.update_template(current_user["id"], template_id, **req.model_dump(exclude_none=True))
    except KeyError:
        raise APIError(status_code=404, code=ErrorCode.NOT_FOUND, message="Template not found")


@router.delete("/{template_id}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_template(template_id: int, current_user: dict = Depends(get_current_user)):
    service = TemplateService()
    try:
        service.delete_template(current_user["id"], template_id)
    except KeyError:
        raise APIError(status_code=404, code=ErrorCode.NOT_FOUND, message="Template not found")


@router.post("/{template_id}/clone", status_code=status.HTTP_201_CREATED)
async def clone_template(template_id: int, current_user: dict = Depends(get_current_user)):
    service = TemplateService()
    try:
        return service.clone_template(current_user["id"], template_id)
    except KeyError:
        raise APIError(status_code=404, code=ErrorCode.NOT_FOUND, message="Template not found")


# --- Comments ---


@router.get("/{template_id}/comments")
async def list_comments(template_id: int, current_user: dict = Depends(get_current_user)):
    service = TemplateService()
    return service.list_comments(template_id)


@router.post("/{template_id}/comments", status_code=status.HTTP_201_CREATED)
async def add_comment(template_id: int, req: CommentCreate, current_user: dict = Depends(get_current_user)):
    service = TemplateService()
    try:
        comment_id = service.add_comment(template_id, current_user["id"], req.content, req.parent_id)
        return {"id": comment_id, "message": "Comment added"}
    except KeyError:
        raise APIError(status_code=404, code=ErrorCode.NOT_FOUND, message="Template not found")


@router.delete("/{template_id}/comments/{comment_id}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_comment(template_id: int, comment_id: int, current_user: dict = Depends(get_current_user)):
    service = TemplateService()
    try:
        service.delete_comment(comment_id, current_user["id"])
    except KeyError:
        raise APIError(status_code=404, code=ErrorCode.NOT_FOUND, message="Comment not found")


# --- Ratings ---


@router.get("/{template_id}/ratings")
async def get_ratings(template_id: int, current_user: dict = Depends(get_current_user)):
    service = TemplateService()
    summary = service.get_ratings(template_id)
    reviews = service.list_reviews(template_id)
    return {"summary": summary, "reviews": reviews}


@router.post("/{template_id}/ratings")
async def rate_template(template_id: int, req: RatingCreate, current_user: dict = Depends(get_current_user)):
    service = TemplateService()
    try:
        return service.rate_template(template_id, current_user["id"], req.rating, req.review)
    except KeyError:
        raise APIError(status_code=404, code=ErrorCode.NOT_FOUND, message="Template not found")
    except ValueError as e:
        raise APIError(status_code=400, code=ErrorCode.VALIDATION_ERROR, message=str(e))
