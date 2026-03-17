"""AI domain service."""
from __future__ import annotations

import uuid
from typing import Any, Optional

from app.domains.ai.dao.ai_dao import AIConversationDao, AIModelConfigDao


class AIService:
    def __init__(self) -> None:
        self._conv_dao = AIConversationDao()
        self._model_dao = AIModelConfigDao()

    # --- Conversations ---

    def list_conversations(self, user_id: int, status: Optional[str] = None,
                           limit: int = 50, offset: int = 0) -> list[dict[str, Any]]:
        return self._conv_dao.list_for_user(user_id, status=status, limit=limit, offset=offset)

    def count_conversations(self, user_id: int) -> int:
        return self._conv_dao.count_for_user(user_id)

    def create_conversation(self, user_id: int, title: Optional[str] = None,
                            model: Optional[str] = None) -> dict[str, Any]:
        session_id = str(uuid.uuid4())
        conv_id = self._conv_dao.create(user_id, session_id, title=title, model_used=model)
        return self.get_conversation(user_id, conv_id)

    def get_conversation(self, user_id: int, conversation_id: int) -> dict[str, Any]:
        row = self._conv_dao.get(conversation_id, user_id)
        if not row:
            raise KeyError("Conversation not found")
        return row

    def update_conversation(self, user_id: int, conversation_id: int, **fields) -> dict[str, Any]:
        existing = self._conv_dao.get(conversation_id, user_id)
        if not existing:
            raise KeyError("Conversation not found")
        self._conv_dao.update(conversation_id, user_id, **fields)
        return self.get_conversation(user_id, conversation_id)

    def delete_conversation(self, user_id: int, conversation_id: int) -> None:
        if not self._conv_dao.delete(conversation_id, user_id):
            raise KeyError("Conversation not found")

    # --- Messages ---

    def list_messages(self, user_id: int, conversation_id: int) -> list[dict[str, Any]]:
        # Ownership check
        self.get_conversation(user_id, conversation_id)
        return self._conv_dao.list_messages(conversation_id)

    def send_message(self, user_id: int, conversation_id: int, content: str) -> dict[str, Any]:
        """Add a user message and generate AI response (stub)."""
        self.get_conversation(user_id, conversation_id)
        # Save user message
        self._conv_dao.add_message(conversation_id, "user", content, tokens=len(content) // 4)
        # AI response stub — in production this would call the AI model
        response_content = f"[AI Response] Received: {content[:100]}"
        msg_id = self._conv_dao.add_message(
            conversation_id, "assistant", response_content,
            tokens=len(response_content) // 4,
            metadata={"model": "stub", "finish_reason": "stop"},
        )
        return {"message_id": msg_id, "role": "assistant", "content": response_content}

    # --- Model Configs ---

    def list_models(self, enabled_only: bool = False) -> list[dict[str, Any]]:
        return self._model_dao.list_all(enabled_only=enabled_only)

    def get_model(self, model_id: int) -> dict[str, Any]:
        row = self._model_dao.get(model_id)
        if not row:
            raise KeyError("Model config not found")
        return row

    def create_model(self, model_name: str, provider: str, **kwargs) -> dict[str, Any]:
        if self._model_dao.get_by_name(model_name):
            raise ValueError(f"Model '{model_name}' already exists")
        model_id = self._model_dao.create(model_name, provider, **kwargs)
        return self.get_model(model_id)

    def update_model(self, model_id: int, **fields) -> dict[str, Any]:
        existing = self._model_dao.get(model_id)
        if not existing:
            raise KeyError("Model config not found")
        self._model_dao.update(model_id, **fields)
        return self.get_model(model_id)

    def delete_model(self, model_id: int) -> None:
        if not self._model_dao.delete(model_id):
            raise KeyError("Model config not found")
