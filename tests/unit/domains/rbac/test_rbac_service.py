from sqlalchemy.exc import SQLAlchemyError

from app.domains.rbac.service.rbac_service import (
    ALL_SYSTEM_PERMISSIONS,
    DEFAULT_ROLE_PERMISSIONS,
    RbacService,
    _permission_set,
)


def test_permission_set_cross_product():
    assert _permission_set(("a", "b"), ("read", "write")) == {"a.read", "a.write", "b.read", "b.write"}


class TestRbacService:
    def test_default_permissions_and_admin_username(self, monkeypatch):
        svc = RbacService()
        monkeypatch.setenv("ADMIN_USERNAME", "root")
        from app.domains.rbac.service import rbac_service
        rbac_service.get_default_admin_username.cache_clear()

        assert svc.get_default_permissions("admin") == ALL_SYSTEM_PERMISSIONS
        assert svc.get_default_permissions("viewer") == DEFAULT_ROLE_PERMISSIONS["viewer"]
        assert svc.get_default_permissions("unknown") == DEFAULT_ROLE_PERMISSIONS["trader"]

    def test_get_user_roles_prefers_db_then_admin_then_default(self, monkeypatch):
        svc = RbacService()
        monkeypatch.setattr(svc.user_roles, "list_user_roles", lambda user_id: [{"name": "viewer"}, {"name": "researcher"}])
        assert svc.get_user_roles(1, "alice") == ["viewer", "researcher"]

        monkeypatch.setattr(svc.user_roles, "list_user_roles", lambda user_id: [])
        monkeypatch.setenv("ADMIN_USERNAME", "root")
        from app.domains.rbac.service import rbac_service
        rbac_service.get_default_admin_username.cache_clear()
        assert svc.get_user_roles(1, "root") == ["admin"]
        assert svc.get_user_roles(1, "alice") == ["trader"]

    def test_get_user_roles_handles_sqlalchemy_error(self, monkeypatch):
        svc = RbacService()
        monkeypatch.setattr(svc.user_roles, "list_user_roles", lambda user_id: (_ for _ in ()).throw(SQLAlchemyError("boom")))
        assert svc.get_user_roles(1, "alice") == ["trader"]

    def test_get_primary_role_and_permissions(self, monkeypatch):
        svc = RbacService()
        monkeypatch.setattr(svc, "get_user_roles", lambda user_id, username=None: ["viewer", "researcher"])
        assert svc.get_primary_role(1) == "researcher"

        monkeypatch.setattr(svc.user_roles, "list_user_permissions", lambda user_id: {"data.read"})
        assert svc.get_user_permissions(1) == {"data.read"}

        monkeypatch.setattr(svc.user_roles, "list_user_permissions", lambda user_id: set())
        perms = svc.get_user_permissions(1)
        assert "data.read" in perms and "strategies.read" in perms

    def test_get_user_permissions_handles_sqlalchemy_error(self, monkeypatch):
        svc = RbacService()
        monkeypatch.setattr(svc.user_roles, "list_user_permissions", lambda user_id: (_ for _ in ()).throw(SQLAlchemyError("boom")))
        monkeypatch.setattr(svc, "get_user_roles", lambda user_id, username=None: ["viewer"])
        perms = svc.get_user_permissions(1)
        assert perms == DEFAULT_ROLE_PERMISSIONS["viewer"]
