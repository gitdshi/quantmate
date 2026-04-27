from datetime import datetime, timedelta

from fastapi import FastAPI
from fastapi.testclient import TestClient

from app.api.exception_handlers import register_exception_handlers
from app.api.routes import strategies
from app.api.models.user import TokenData


def _override_auth():
    return TokenData(user_id=9, username="tester", exp=datetime.utcnow() + timedelta(minutes=5))


def _build_client():
    app = FastAPI()
    register_exception_handlers(app)
    app.include_router(strategies.router, prefix="/api/v1")
    app.dependency_overrides[strategies.get_current_user] = _override_auth
    return TestClient(app, raise_server_exceptions=False)


class TestStrategiesRoutes:
    def test_list_strategies_returns_paginated_items(self, monkeypatch):
        client = _build_client()

        class FakeService:
            def count_strategies(self, user_id):
                return 1

            def list_strategies_paginated(self, user_id, limit, offset):
                return [{
                    "id": 1,
                    "name": "S1",
                    "class_name": "Cls",
                    "description": "desc",
                    "version": 2,
                    "is_active": True,
                    "created_at": datetime.utcnow(),
                    "updated_at": datetime.utcnow(),
                }]

        monkeypatch.setattr(strategies, "StrategiesService", lambda: FakeService())
        resp = client.get("/api/v1/strategies")
        assert resp.status_code == 200
        assert resp.json()["data"][0]["name"] == "S1"

    def test_create_get_update_delete_strategy_paths(self, monkeypatch):
        client = _build_client()
        now = datetime.utcnow()

        class FakeService:
            def create_strategy(self, **kwargs):
                return {"id": 1, "user_id": 9, "name": kwargs["name"], "class_name": kwargs["class_name"], "description": kwargs["description"], "parameters": kwargs["parameters"], "code": kwargs["code"], "version": 1, "is_active": True, "created_at": now, "updated_at": now}

            def get_strategy(self, user_id, strategy_id):
                return {"id": strategy_id, "user_id": 9, "name": "S1", "class_name": "Cls", "description": "desc", "parameters": {}, "code": "code", "version": 1, "is_active": True, "created_at": now, "updated_at": now}

            def update_strategy(self, user_id, strategy_id, **kwargs):
                return {"id": strategy_id, "user_id": 9, "name": kwargs["name"] or "S1", "class_name": kwargs["class_name"] or "Cls", "description": kwargs["description"] or "desc", "parameters": kwargs["parameters"] or {}, "code": kwargs["code"] or "code", "version": 2, "is_active": kwargs["is_active"], "created_at": now, "updated_at": now}

            def delete_strategy(self, user_id, strategy_id):
                return None

        monkeypatch.setattr(strategies, "StrategiesService", lambda: FakeService())

        create_resp = client.post("/api/v1/strategies", json={"name": "S1", "class_name": "Cls", "description": "desc", "parameters": {}, "code": "print(1)"})
        assert create_resp.status_code == 201

        get_resp = client.get("/api/v1/strategies/1")
        assert get_resp.status_code == 200
        assert get_resp.json()["id"] == 1

        update_resp = client.put("/api/v1/strategies/1", json={"name": "S2", "is_active": False})
        assert update_resp.status_code == 200
        assert update_resp.json()["name"] == "S2"
        assert update_resp.json()["is_active"] is False

        delete_resp = client.delete("/api/v1/strategies/1")
        assert delete_resp.status_code == 204

    def test_create_and_update_convert_value_errors_to_400(self, monkeypatch):
        client = _build_client()

        class FakeService:
            def create_strategy(self, **kwargs):
                raise ValueError("bad create")

            def update_strategy(self, *args, **kwargs):
                raise ValueError("bad update")

        monkeypatch.setattr(strategies, "StrategiesService", lambda: FakeService())
        valid_create = {"name": "S1", "class_name": "Cls", "description": "desc", "parameters": {}, "code": "print(1)"}
        assert client.post("/api/v1/strategies", json=valid_create).status_code == 400
        assert client.put("/api/v1/strategies/1", json={"name": "S1", "class_name": "Cls"}).status_code == 400

    def test_get_update_delete_and_history_key_errors_map_to_404(self, monkeypatch):
        client = _build_client()

        class FakeService:
            def get_strategy(self, *args, **kwargs):
                raise KeyError("missing")

            def update_strategy(self, *args, **kwargs):
                raise KeyError("missing")

            def delete_strategy(self, *args, **kwargs):
                raise KeyError("missing")

            def list_code_history(self, *args, **kwargs):
                raise KeyError("missing")

            def get_code_history(self, *args, **kwargs):
                raise KeyError("History missing")

            def restore_code_history(self, *args, **kwargs):
                raise KeyError("History missing")

        monkeypatch.setattr(strategies, "StrategiesService", lambda: FakeService())
        assert client.get("/api/v1/strategies/1").status_code == 404
        assert client.put("/api/v1/strategies/1", json={"name": "S1"}).status_code == 404
        assert client.delete("/api/v1/strategies/1").status_code == 404
        assert client.get("/api/v1/strategies/1/code-history").status_code == 404
        assert client.get("/api/v1/strategies/1/code-history/2").status_code == 404
        assert client.post("/api/v1/strategies/1/code-history/2/restore").status_code == 404

    def test_validate_and_history_success_paths(self, monkeypatch):
        client = _build_client()
        now = datetime.utcnow()

        class FakeService:
            def get_strategy(self, user_id, strategy_id):
                return {"id": strategy_id, "user_id": 9, "name": "S1", "class_name": "Cls", "description": "desc", "parameters": {}, "code": "code", "version": 1, "is_active": True, "created_at": now, "updated_at": now}

            def list_code_history(self, user_id, strategy_id):
                return [{"id": 1}]

            def get_code_history(self, user_id, strategy_id, history_id):
                return {"id": history_id}

            def restore_code_history(self, user_id, strategy_id, history_id):
                return None

        monkeypatch.setattr(strategies, "StrategiesService", lambda: FakeService())
        monkeypatch.setattr(strategies, "validate_strategy_code", lambda code, class_name: {"valid": True, "errors": [], "warnings": []})

        validate_resp = client.post("/api/v1/strategies/1/validate")
        assert validate_resp.status_code == 200
        assert validate_resp.json()["valid"] is True

        assert client.get("/api/v1/strategies/1/code-history").json() == [{"id": 1}]
        assert client.get("/api/v1/strategies/1/code-history/2").json() == {"id": 2}
        restore_resp = client.post("/api/v1/strategies/1/code-history/2/restore")
        assert restore_resp.status_code == 200
        assert restore_resp.json()["history_id"] == 2
