"""AI Assistant routes — conversations, messages, model configs."""

from typing import Optional

from fastapi import APIRouter, Depends, Query, status
from pydantic import BaseModel

from app.api.services.auth_service import get_current_user
from app.api.models.user import TokenData
from app.api.errors import ErrorCode
from app.api.exception_handlers import APIError
from app.api.pagination import PaginationParams, paginate
from app.domains.ai.service import AIService

router = APIRouter(prefix="/ai", tags=["AI Assistant"])


# --- Request models ---


class ConversationCreate(BaseModel):
    title: Optional[str] = None
    model: Optional[str] = None


class ConversationUpdate(BaseModel):
    title: Optional[str] = None
    status: Optional[str] = None


class MessageSend(BaseModel):
    content: str


class ModelConfigCreate(BaseModel):
    model_name: str
    provider: str
    endpoint: Optional[str] = None
    temperature: float = 0.7
    max_tokens: int = 4096


class ModelConfigUpdate(BaseModel):
    model_name: Optional[str] = None
    provider: Optional[str] = None
    endpoint: Optional[str] = None
    temperature: Optional[float] = None
    max_tokens: Optional[int] = None
    enabled: Optional[bool] = None


# --- Conversation endpoints ---


@router.get("/conversations")
async def list_conversations(
    conv_status: Optional[str] = Query(None, alias="status"),
    pagination: PaginationParams = Depends(),
    current_user: TokenData = Depends(get_current_user),
):
    service = AIService()
    total = service.count_conversations(current_user.user_id)
    rows = service.list_conversations(
        current_user.user_id,
        status=conv_status,
        limit=pagination.page_size,
        offset=pagination.offset,
    )
    return paginate(rows, total, pagination)


@router.post("/conversations", status_code=status.HTTP_201_CREATED)
async def create_conversation(
    req: ConversationCreate,
    current_user: TokenData = Depends(get_current_user),
):
    service = AIService()
    conv = service.create_conversation(current_user.user_id, title=req.title, model=req.model)
    return conv


@router.get("/conversations/{conversation_id}")
async def get_conversation(conversation_id: int, current_user: TokenData = Depends(get_current_user)):
    service = AIService()
    try:
        return service.get_conversation(current_user.user_id, conversation_id)
    except KeyError:
        raise APIError(status_code=404, code=ErrorCode.NOT_FOUND, message="Conversation not found")


@router.put("/conversations/{conversation_id}")
async def update_conversation(
    conversation_id: int,
    req: ConversationUpdate,
    current_user: TokenData = Depends(get_current_user),
):
    service = AIService()
    try:
        return service.update_conversation(current_user.user_id, conversation_id, **req.model_dump(exclude_none=True))
    except KeyError:
        raise APIError(status_code=404, code=ErrorCode.NOT_FOUND, message="Conversation not found")


@router.delete("/conversations/{conversation_id}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_conversation(conversation_id: int, current_user: TokenData = Depends(get_current_user)):
    service = AIService()
    try:
        service.delete_conversation(current_user.user_id, conversation_id)
    except KeyError:
        raise APIError(status_code=404, code=ErrorCode.NOT_FOUND, message="Conversation not found")


# --- Message endpoints ---


@router.get("/conversations/{conversation_id}/messages")
async def list_messages(conversation_id: int, current_user: TokenData = Depends(get_current_user)):
    service = AIService()
    try:
        return service.list_messages(current_user.user_id, conversation_id)
    except KeyError:
        raise APIError(status_code=404, code=ErrorCode.NOT_FOUND, message="Conversation not found")


@router.post("/conversations/{conversation_id}/messages")
async def send_message(
    conversation_id: int,
    req: MessageSend,
    current_user: TokenData = Depends(get_current_user),
):
    service = AIService()
    try:
        return service.send_message(current_user.user_id, conversation_id, req.content)
    except KeyError:
        raise APIError(status_code=404, code=ErrorCode.NOT_FOUND, message="Conversation not found")


# --- Model config endpoints ---


@router.get("/models")
async def list_models(
    enabled_only: bool = Query(False),
    current_user: TokenData = Depends(get_current_user),
):
    service = AIService()
    return service.list_models(enabled_only=enabled_only)


@router.get("/models/{model_id}")
async def get_model(model_id: int, current_user: TokenData = Depends(get_current_user)):
    service = AIService()
    try:
        return service.get_model(model_id)
    except KeyError:
        raise APIError(status_code=404, code=ErrorCode.NOT_FOUND, message="Model config not found")


@router.post("/models", status_code=status.HTTP_201_CREATED)
async def create_model(req: ModelConfigCreate, current_user: TokenData = Depends(get_current_user)):
    service = AIService()
    try:
        return service.create_model(
            model_name=req.model_name,
            provider=req.provider,
            endpoint=req.endpoint,
            temperature=req.temperature,
            max_tokens=req.max_tokens,
        )
    except ValueError as e:
        raise APIError(status_code=400, code=ErrorCode.VALIDATION_ERROR, message=str(e))


@router.put("/models/{model_id}")
async def update_model(model_id: int, req: ModelConfigUpdate, current_user: TokenData = Depends(get_current_user)):
    service = AIService()
    try:
        return service.update_model(model_id, **req.model_dump(exclude_none=True))
    except KeyError:
        raise APIError(status_code=404, code=ErrorCode.NOT_FOUND, message="Model config not found")


@router.delete("/models/{model_id}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_model(model_id: int, current_user: TokenData = Depends(get_current_user)):
    service = AIService()
    try:
        service.delete_model(model_id)
    except KeyError:
        raise APIError(status_code=404, code=ErrorCode.NOT_FOUND, message="Model config not found")
