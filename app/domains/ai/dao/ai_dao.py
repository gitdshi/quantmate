"""AI domain DAO — conversations and messages."""
from __future__ import annotations

import json
from typing import Any, Optional

from sqlalchemy import text

from app.infrastructure.db.connections import connection


class AIConversationDao:
    """CRUD for ai_conversations and ai_messages."""

    # --- Conversations ---

    def list_for_user(self, user_id: int, status: Optional[str] = None, limit: int = 50, offset: int = 0) -> list[dict[str, Any]]:
        query = "SELECT * FROM ai_conversations WHERE user_id = :uid"
        params: dict[str, Any] = {"uid": user_id, "limit": limit, "offset": offset}
        if status:
            query += " AND status = :status"
            params["status"] = status
        query += " ORDER BY updated_at DESC LIMIT :limit OFFSET :offset"
        with connection("quantmate") as conn:
            rows = conn.execute(text(query), params).fetchall()
            return [dict(r._mapping) for r in rows]

    def count_for_user(self, user_id: int) -> int:
        with connection("quantmate") as conn:
            row = conn.execute(
                text("SELECT COUNT(*) AS cnt FROM ai_conversations WHERE user_id = :uid"),
                {"uid": user_id},
            ).fetchone()
            return row._mapping["cnt"] if row else 0

    def get(self, conversation_id: int, user_id: int) -> Optional[dict[str, Any]]:
        with connection("quantmate") as conn:
            row = conn.execute(
                text("SELECT * FROM ai_conversations WHERE id = :cid AND user_id = :uid"),
                {"cid": conversation_id, "uid": user_id},
            ).fetchone()
            return dict(row._mapping) if row else None

    def create(self, user_id: int, session_id: str, title: Optional[str] = None, model_used: Optional[str] = None) -> int:
        with connection("quantmate") as conn:
            result = conn.execute(
                text(
                    "INSERT INTO ai_conversations (user_id, session_id, title, model_used) "
                    "VALUES (:uid, :sid, :title, :model)"
                ),
                {"uid": user_id, "sid": session_id, "title": title, "model": model_used},
            )
            conn.commit()
            return result.lastrowid  # type: ignore[return-value]

    def update(self, conversation_id: int, user_id: int, **fields) -> None:
        allowed = {"title", "status", "model_used"}
        data = {k: v for k, v in fields.items() if k in allowed and v is not None}
        if not data:
            return
        set_clause = ", ".join(f"{k} = :{k}" for k in data)
        with connection("quantmate") as conn:
            conn.execute(
                text(f"UPDATE ai_conversations SET {set_clause} WHERE id = :cid AND user_id = :uid"),
                {**data, "cid": conversation_id, "uid": user_id},
            )
            conn.commit()

    def delete(self, conversation_id: int, user_id: int) -> bool:
        with connection("quantmate") as conn:
            result = conn.execute(
                text("DELETE FROM ai_conversations WHERE id = :cid AND user_id = :uid"),
                {"cid": conversation_id, "uid": user_id},
            )
            conn.commit()
            return result.rowcount > 0  # type: ignore[union-attr]

    # --- Messages ---

    def list_messages(self, conversation_id: int) -> list[dict[str, Any]]:
        with connection("quantmate") as conn:
            rows = conn.execute(
                text("SELECT * FROM ai_messages WHERE conversation_id = :cid ORDER BY created_at ASC"),
                {"cid": conversation_id},
            ).fetchall()
            return [dict(r._mapping) for r in rows]

    def add_message(self, conversation_id: int, role: str, content: str, tokens: int = 0, metadata: Optional[dict] = None) -> int:
        with connection("quantmate") as conn:
            result = conn.execute(
                text(
                    "INSERT INTO ai_messages (conversation_id, role, content, tokens, metadata) "
                    "VALUES (:cid, :role, :content, :tokens, :meta)"
                ),
                {
                    "cid": conversation_id,
                    "role": role,
                    "content": content,
                    "tokens": tokens,
                    "meta": json.dumps(metadata) if metadata else None,
                },
            )
            # Update conversation token count
            conn.execute(
                text("UPDATE ai_conversations SET total_tokens = total_tokens + :tokens, updated_at = NOW() WHERE id = :cid"),
                {"tokens": tokens, "cid": conversation_id},
            )
            conn.commit()
            return result.lastrowid  # type: ignore[return-value]


class AIModelConfigDao:
    """CRUD for ai_model_configs."""

    def list_all(self, enabled_only: bool = False) -> list[dict[str, Any]]:
        query = "SELECT id, model_name, provider, endpoint, temperature, max_tokens, enabled, created_at FROM ai_model_configs"
        if enabled_only:
            query += " WHERE enabled = 1"
        query += " ORDER BY model_name"
        with connection("quantmate") as conn:
            rows = conn.execute(text(query)).fetchall()
            return [dict(r._mapping) for r in rows]

    def get(self, model_id: int) -> Optional[dict[str, Any]]:
        with connection("quantmate") as conn:
            row = conn.execute(
                text("SELECT * FROM ai_model_configs WHERE id = :mid"),
                {"mid": model_id},
            ).fetchone()
            return dict(row._mapping) if row else None

    def get_by_name(self, model_name: str) -> Optional[dict[str, Any]]:
        with connection("quantmate") as conn:
            row = conn.execute(
                text("SELECT * FROM ai_model_configs WHERE model_name = :name"),
                {"name": model_name},
            ).fetchone()
            return dict(row._mapping) if row else None

    def create(self, model_name: str, provider: str, endpoint: Optional[str] = None,
               temperature: float = 0.7, max_tokens: int = 4096) -> int:
        with connection("quantmate") as conn:
            result = conn.execute(
                text(
                    "INSERT INTO ai_model_configs (model_name, provider, endpoint, temperature, max_tokens) "
                    "VALUES (:name, :provider, :endpoint, :temp, :max_tokens)"
                ),
                {"name": model_name, "provider": provider, "endpoint": endpoint,
                 "temp": temperature, "max_tokens": max_tokens},
            )
            conn.commit()
            return result.lastrowid  # type: ignore[return-value]

    def update(self, model_id: int, **fields) -> None:
        allowed = {"model_name", "provider", "endpoint", "temperature", "max_tokens", "enabled"}
        data = {k: v for k, v in fields.items() if k in allowed and v is not None}
        if not data:
            return
        set_clause = ", ".join(f"{k} = :{k}" for k in data)
        with connection("quantmate") as conn:
            conn.execute(
                text(f"UPDATE ai_model_configs SET {set_clause}, updated_at = NOW() WHERE id = :mid"),
                {**data, "mid": model_id},
            )
            conn.commit()

    def delete(self, model_id: int) -> bool:
        with connection("quantmate") as conn:
            result = conn.execute(
                text("DELETE FROM ai_model_configs WHERE id = :mid"),
                {"mid": model_id},
            )
            conn.commit()
            return result.rowcount > 0  # type: ignore[union-attr]
