"""Tests for Strategy Template routes."""
import pytest
from unittest.mock import patch
from fastapi import FastAPI
from fastapi.testclient import TestClient

from app.api.routes import templates
from app.api.exception_handlers import register_exception_handlers


@pytest.fixture
def mock_user():
    return type("User", (), {"id": 1, "user_id": 1, "username": "testuser"})()


@pytest.fixture
def client(mock_user):
    test_app = FastAPI()
    register_exception_handlers(test_app)
    test_app.include_router(templates.router, prefix="/api/v1")

    async def override_auth():
        return mock_user

    test_app.dependency_overrides[templates.get_current_user] = override_auth
    return TestClient(test_app)


SAMPLE_TEMPLATE = {
    "id": 1, "author_id": 1, "name": "MA Cross",
    "category": "trend", "description": "Moving average crossover",
    "code": "class MACross: pass", "visibility": "private",
    "downloads": 0, "version": "1.0.0",
}


class TestTemplateRoutes:

    @patch("app.domains.templates.service.StrategyTemplateDao")
    def test_list_marketplace(self, MockDao, client):
        instance = MockDao.return_value
        instance.list_public.return_value = [SAMPLE_TEMPLATE]
        instance.count_public.return_value = 1
        resp = client.get("/api/v1/templates/marketplace")
        assert resp.status_code == 200
        assert "data" in resp.json()

    @patch("app.domains.templates.service.StrategyTemplateDao")
    def test_list_my_templates(self, MockDao, client):
        instance = MockDao.return_value
        instance.list_for_user.return_value = [SAMPLE_TEMPLATE]
        instance.count_for_user.return_value = 1
        resp = client.get("/api/v1/templates/mine")
        assert resp.status_code == 200

    @patch("app.domains.templates.service.StrategyTemplateDao")
    def test_create_template(self, MockDao, client):
        instance = MockDao.return_value
        instance.create.return_value = 1
        instance.get.return_value = SAMPLE_TEMPLATE
        resp = client.post("/api/v1/templates", json={
            "name": "MA Cross", "code": "class MACross: pass",
            "category": "trend", "visibility": "private",
        })
        assert resp.status_code == 201

    @patch("app.domains.templates.service.StrategyTemplateDao")
    def test_get_template(self, MockDao, client):
        instance = MockDao.return_value
        instance.get.return_value = SAMPLE_TEMPLATE
        resp = client.get("/api/v1/templates/1")
        assert resp.status_code == 200

    @patch("app.domains.templates.service.StrategyTemplateDao")
    def test_get_template_not_found(self, MockDao, client):
        instance = MockDao.return_value
        instance.get.return_value = None
        resp = client.get("/api/v1/templates/999")
        assert resp.status_code == 404

    @patch("app.domains.templates.service.StrategyTemplateDao")
    def test_update_template(self, MockDao, client):
        instance = MockDao.return_value
        instance.get.return_value = SAMPLE_TEMPLATE
        instance.update.return_value = None
        resp = client.put("/api/v1/templates/1", json={"name": "Updated"})
        assert resp.status_code == 200

    @patch("app.domains.templates.service.StrategyTemplateDao")
    def test_update_template_not_owner(self, MockDao, client):
        instance = MockDao.return_value
        other_tpl = {**SAMPLE_TEMPLATE, "author_id": 99}
        instance.get.return_value = other_tpl
        resp = client.put("/api/v1/templates/1", json={"name": "Hack"})
        assert resp.status_code == 404

    @patch("app.domains.templates.service.StrategyTemplateDao")
    def test_delete_template(self, MockDao, client):
        instance = MockDao.return_value
        instance.delete.return_value = True
        resp = client.delete("/api/v1/templates/1")
        assert resp.status_code == 204

    @patch("app.domains.templates.service.StrategyTemplateDao")
    def test_clone_template(self, MockDao, client):
        instance = MockDao.return_value
        instance.get.return_value = SAMPLE_TEMPLATE
        instance.increment_downloads.return_value = None
        instance.create.return_value = 2
        cloned = {**SAMPLE_TEMPLATE, "id": 2, "name": "MA Cross (copy)", "visibility": "private"}
        # Second get call for newly created template
        instance.get.side_effect = [SAMPLE_TEMPLATE, cloned]
        resp = client.post("/api/v1/templates/1/clone")
        assert resp.status_code == 201


class TestTemplateCommentRoutes:

    @patch("app.domains.templates.service.StrategyCommentDao")
    @patch("app.domains.templates.service.StrategyTemplateDao")
    def test_list_comments(self, MockTplDao, MockCommentDao, client):
        MockCommentDao.return_value.list_for_template.return_value = [
            {"id": 1, "template_id": 1, "user_id": 1, "content": "Great template!"}
        ]
        resp = client.get("/api/v1/templates/1/comments")
        assert resp.status_code == 200

    @patch("app.domains.templates.service.StrategyCommentDao")
    @patch("app.domains.templates.service.StrategyTemplateDao")
    def test_add_comment(self, MockTplDao, MockCommentDao, client):
        MockTplDao.return_value.get.return_value = SAMPLE_TEMPLATE
        MockCommentDao.return_value.create.return_value = 1
        resp = client.post("/api/v1/templates/1/comments", json={"content": "Nice!"})
        assert resp.status_code == 201

    @patch("app.domains.templates.service.StrategyCommentDao")
    @patch("app.domains.templates.service.StrategyTemplateDao")
    def test_delete_comment(self, MockTplDao, MockCommentDao, client):
        MockCommentDao.return_value.delete.return_value = True
        resp = client.delete("/api/v1/templates/1/comments/1")
        assert resp.status_code == 204


class TestTemplateRatingRoutes:

    @patch("app.domains.templates.service.StrategyRatingDao")
    @patch("app.domains.templates.service.StrategyTemplateDao")
    def test_get_ratings(self, MockTplDao, MockRatingDao, client):
        MockRatingDao.return_value.get_for_template.return_value = {"avg_rating": 4.5, "count": 10}
        MockRatingDao.return_value.list_for_template.return_value = []
        resp = client.get("/api/v1/templates/1/ratings")
        assert resp.status_code == 200
        assert "summary" in resp.json()

    @patch("app.domains.templates.service.StrategyRatingDao")
    @patch("app.domains.templates.service.StrategyTemplateDao")
    def test_rate_template(self, MockTplDao, MockRatingDao, client):
        MockTplDao.return_value.get.return_value = SAMPLE_TEMPLATE
        MockRatingDao.return_value.upsert.return_value = None
        MockRatingDao.return_value.get_for_template.return_value = {"avg_rating": 5.0, "count": 1}
        resp = client.post("/api/v1/templates/1/ratings", json={"rating": 5, "review": "Excellent!"})
        assert resp.status_code == 200

    @patch("app.domains.templates.service.StrategyRatingDao")
    @patch("app.domains.templates.service.StrategyTemplateDao")
    def test_rate_template_invalid(self, MockTplDao, MockRatingDao, client):
        resp = client.post("/api/v1/templates/1/ratings", json={"rating": 0})
        assert resp.status_code == 422  # Pydantic validation

