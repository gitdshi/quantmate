"""Tests for Issue #9: KYC Verification."""
import pytest
from datetime import datetime, timedelta
from unittest.mock import MagicMock, patch
from fastapi import FastAPI, Depends
from fastapi.testclient import TestClient

from app.api.routes.kyc import router, _require_admin, _mask_name
from app.api.services.auth_service import get_current_user
from app.api.models.user import TokenData
from app.api.exception_handlers import register_exception_handlers


future_exp = datetime.utcnow() + timedelta(hours=1)
TEST_USER = TokenData(user_id=10, username="alice", exp=future_exp)
ADMIN_USER = TokenData(user_id=1, username="admin", exp=future_exp)


@pytest.fixture
def user_client():
    app = FastAPI()
    register_exception_handlers(app)
    app.dependency_overrides[get_current_user] = lambda: TEST_USER
    app.include_router(router)
    return TestClient(app, raise_server_exceptions=False)


@pytest.fixture
def admin_client():
    app = FastAPI()
    register_exception_handlers(app)
    app.dependency_overrides[get_current_user] = lambda: ADMIN_USER
    app.dependency_overrides[_require_admin] = lambda: ADMIN_USER
    app.include_router(router)
    return TestClient(app, raise_server_exceptions=False)


class TestMaskName:
    def test_two_chars(self):
        assert _mask_name("张三") == "张*"

    def test_three_chars(self):
        assert _mask_name("张三丰") == "张*丰"

    def test_single_char(self):
        assert _mask_name("张") == "张"

    def test_long_name(self):
        assert _mask_name("欧阳修远") == "欧**远"


class TestSubmit:
    def test_submit_success(self, user_client):
        with patch("app.domains.auth.dao.kyc_dao.KycDao") as MockDao:
            MockDao.return_value.get_latest.return_value = None
            MockDao.return_value.insert.return_value = 1
            resp = user_client.post("/kyc/submit", json={
                "real_name": "张三",
                "id_number": "110101199001011234",
                "id_type": "mainland_id",
                "id_front_path": "/uploads/front.jpg",
                "id_back_path": "/uploads/back.jpg",
            })
        assert resp.status_code == 200
        assert resp.json()["status"] == "pending"

    def test_already_approved_rejected(self, user_client):
        with patch("app.domains.auth.dao.kyc_dao.KycDao") as MockDao:
            MockDao.return_value.get_latest.return_value = {"status": "approved"}
            resp = user_client.post("/kyc/submit", json={
                "real_name": "张三",
                "id_number": "110101199001011234",
                "id_type": "mainland_id",
                "id_front_path": "/uploads/front.jpg",
                "id_back_path": "/uploads/back.jpg",
            })
        assert resp.status_code == 400
        assert "already approved" in resp.json()["error"]["message"]

    def test_pending_resubmission_rejected(self, user_client):
        with patch("app.domains.auth.dao.kyc_dao.KycDao") as MockDao:
            MockDao.return_value.get_latest.return_value = {"status": "pending"}
            resp = user_client.post("/kyc/submit", json={
                "real_name": "张三",
                "id_number": "110101199001011234",
                "id_type": "mainland_id",
                "id_front_path": "/uploads/front.jpg",
                "id_back_path": "/uploads/back.jpg",
            })
        assert resp.status_code == 400

    def test_invalid_id_type(self, user_client):
        resp = user_client.post("/kyc/submit", json={
            "real_name": "张三",
            "id_number": "110101199001011234",
            "id_type": "alien_card",
            "id_front_path": "/uploads/front.jpg",
            "id_back_path": "/uploads/back.jpg",
        })
        assert resp.status_code == 400


class TestStatus:
    def test_not_submitted(self, user_client):
        with patch("app.domains.auth.dao.kyc_dao.KycDao") as MockDao:
            MockDao.return_value.get_latest.return_value = None
            resp = user_client.get("/kyc/status")
        assert resp.status_code == 200
        assert resp.json()["status"] == "not_submitted"

    def test_approved_masked(self, user_client):
        with patch("app.domains.auth.dao.kyc_dao.KycDao") as MockDao:
            MockDao.return_value.get_latest.return_value = {
                "status": "approved",
                "real_name": "张三丰",
                "id_type": "mainland_id",
                "created_at": datetime(2025, 3, 1),
                "reviewed_at": datetime(2025, 3, 2),
                "review_notes": None,
            }
            resp = user_client.get("/kyc/status")
        assert resp.status_code == 200
        body = resp.json()
        assert body["status"] == "approved"
        assert body["real_name_masked"] == "张*丰"


class TestAdminReview:
    def test_list_pending(self, admin_client):
        with patch("app.domains.auth.dao.kyc_dao.KycDao") as MockDao:
            MockDao.return_value.count_pending.return_value = 1
            MockDao.return_value.list_pending.return_value = [
                {"id": 1, "user_id": 10, "status": "pending", "real_name": "张三", "id_type": "mainland_id", "created_at": datetime(2025, 3, 1)}
            ]
            resp = admin_client.get("/kyc/pending")
        assert resp.status_code == 200
        assert resp.json()["meta"]["total"] == 1

    def test_approve(self, admin_client):
        with patch("app.domains.auth.dao.kyc_dao.KycDao") as MockDao:
            resp = admin_client.post("/kyc/1/review", json={"action": "approved"})
        assert resp.status_code == 200
        assert resp.json()["status"] == "approved"

    def test_reject_with_notes(self, admin_client):
        with patch("app.domains.auth.dao.kyc_dao.KycDao") as MockDao:
            resp = admin_client.post("/kyc/1/review", json={
                "action": "rejected",
                "review_notes": "Photo too blurry",
            })
        assert resp.status_code == 200
        assert resp.json()["status"] == "rejected"

    def test_invalid_action(self, admin_client):
        resp = admin_client.post("/kyc/1/review", json={"action": "maybe"})
        assert resp.status_code == 422
