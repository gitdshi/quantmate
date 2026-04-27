"""Tests for AI domain routes."""
import pytest
from unittest.mock import patch
from fastapi import FastAPI
from fastapi.testclient import TestClient

from app.api.routes import ai
from app.api.exception_handlers import register_exception_handlers


@pytest.fixture
def mock_user():
    return type("User", (), {"id": 1, "user_id": 1, "username": "testuser"})()


@pytest.fixture
def client(mock_user):
    test_app = FastAPI()
    register_exception_handlers(test_app)
    test_app.include_router(ai.router, prefix="/api/v1")

    async def override_auth():
        return mock_user

    test_app.dependency_overrides[ai.get_current_user] = override_auth
    return TestClient(test_app)


class TestAIConversationRoutes:

    @patch("app.domains.ai.service.AIConversationDao")
    def test_list_conversations(self, MockDao, client):
        instance = MockDao.return_value
        instance.list_for_user.return_value = [
            {"id": 1, "user_id": 1, "session_id": "abc", "title": "Test", "status": "active"}
        ]
        instance.count_for_user.return_value = 1
        resp = client.get("/api/v1/ai/conversations")
        assert resp.status_code == 200
        body = resp.json()
        assert "data" in body

    @patch("app.domains.ai.service.AIConversationDao")
    def test_create_conversation(self, MockDao, client):
        instance = MockDao.return_value
        instance.create.return_value = 1
        instance.get.return_value = {
            "id": 1, "user_id": 1, "session_id": "abc-123",
            "title": "New Chat", "model_used": "gpt-4", "status": "active",
        }
        resp = client.post("/api/v1/ai/conversations", json={"title": "New Chat", "model": "gpt-4"})
        assert resp.status_code == 201
        assert resp.json()["title"] == "New Chat"

    @patch("app.domains.ai.service.AIConversationDao")
    def test_get_conversation(self, MockDao, client):
        instance = MockDao.return_value
        instance.get.return_value = {"id": 1, "user_id": 1, "title": "Test"}
        resp = client.get("/api/v1/ai/conversations/1")
        assert resp.status_code == 200

    @patch("app.domains.ai.service.AIConversationDao")
    def test_get_conversation_not_found(self, MockDao, client):
        instance = MockDao.return_value
        instance.get.return_value = None
        resp = client.get("/api/v1/ai/conversations/999")
        assert resp.status_code == 404

    @patch("app.domains.ai.service.AIConversationDao")
    def test_update_conversation(self, MockDao, client):
        instance = MockDao.return_value
        instance.get.return_value = {"id": 1, "user_id": 1, "title": "Updated"}
        instance.update.return_value = None
        resp = client.put("/api/v1/ai/conversations/1", json={"title": "Updated"})
        assert resp.status_code == 200

    @patch("app.domains.ai.service.AIConversationDao")
    def test_delete_conversation(self, MockDao, client):
        instance = MockDao.return_value
        instance.delete.return_value = True
        resp = client.delete("/api/v1/ai/conversations/1")
        assert resp.status_code == 204

    @patch("app.domains.ai.service.AIConversationDao")
    def test_delete_conversation_not_found(self, MockDao, client):
        instance = MockDao.return_value
        instance.delete.return_value = False
        resp = client.delete("/api/v1/ai/conversations/999")
        assert resp.status_code == 404


class TestAIMessageRoutes:

    @patch("app.domains.ai.service.AIConversationDao")
    def test_list_messages(self, MockDao, client):
        instance = MockDao.return_value
        instance.get.return_value = {"id": 1, "user_id": 1}
        instance.list_messages.return_value = [
            {"id": 1, "conversation_id": 1, "role": "user", "content": "Hello"}
        ]
        resp = client.get("/api/v1/ai/conversations/1/messages")
        assert resp.status_code == 200

    @patch("app.domains.ai.service.AIConversationDao")
    def test_send_message(self, MockDao, client):
        instance = MockDao.return_value
        instance.get.return_value = {"id": 1, "user_id": 1}
        instance.add_message.return_value = 2
        resp = client.post("/api/v1/ai/conversations/1/messages", json={"content": "Hello AI"})
        assert resp.status_code == 200
        assert "content" in resp.json()


class TestAIModelRoutes:

    @patch("app.domains.ai.service.AIModelConfigDao")
    def test_list_models(self, MockDao, client):
        instance = MockDao.return_value
        instance.list_all.return_value = [
            {"id": 1, "model_name": "gpt-4", "provider": "openai", "enabled": True}
        ]
        resp = client.get("/api/v1/ai/models")
        assert resp.status_code == 200

    @patch("app.domains.ai.service.AIModelConfigDao")
    def test_create_model(self, MockDao, client):
        instance = MockDao.return_value
        instance.get_by_name.return_value = None
        instance.create.return_value = 1
        instance.get.return_value = {
            "id": 1, "model_name": "gpt-4", "provider": "openai",
            "temperature": 0.7, "max_tokens": 4096, "enabled": True,
        }
        resp = client.post("/api/v1/ai/models", json={"model_name": "gpt-4", "provider": "openai"})
        assert resp.status_code == 201

    @patch("app.domains.ai.service.AIModelConfigDao")
    def test_create_duplicate_model(self, MockDao, client):
        instance = MockDao.return_value
        instance.get_by_name.return_value = {"id": 1, "model_name": "gpt-4"}
        resp = client.post("/api/v1/ai/models", json={"model_name": "gpt-4", "provider": "openai"})
        assert resp.status_code == 400

    @patch("app.domains.ai.service.AIModelConfigDao")
    def test_update_model(self, MockDao, client):
        instance = MockDao.return_value
        instance.get.return_value = {"id": 1, "model_name": "gpt-4", "provider": "openai"}
        instance.update.return_value = None
        resp = client.put("/api/v1/ai/models/1", json={"temperature": 0.9})
        assert resp.status_code == 200

    @patch("app.domains.ai.service.AIModelConfigDao")
    def test_delete_model(self, MockDao, client):
        instance = MockDao.return_value
        instance.get.return_value = {"id": 1}
        instance.delete.return_value = True
        resp = client.delete("/api/v1/ai/models/1")
        assert resp.status_code == 204

    @patch("app.domains.ai.service.AIModelConfigDao")
    def test_delete_model_not_found(self, MockDao, client):
        instance = MockDao.return_value
        instance.delete.return_value = False
        resp = client.delete("/api/v1/ai/models/999")
        assert resp.status_code == 404

