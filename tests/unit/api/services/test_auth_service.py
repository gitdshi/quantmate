from datetime import timedelta

import jwt
import pytest
from fastapi.security import HTTPAuthorizationCredentials

from app.api.exception_handlers import APIError
from app.api.services import auth_service


class TestAuthService:
    def test_password_hash_and_verify_roundtrip(self):
        hashed = auth_service.get_password_hash("secret")

        assert hashed != "secret"
        assert auth_service.verify_password("secret", hashed) is True
        assert auth_service.verify_password("wrong", hashed) is False

    def test_access_and_refresh_token_encode_expected_fields(self):
        access = auth_service.create_access_token(1, "alice", timedelta(minutes=5), must_change_password=True)
        refresh = auth_service.create_refresh_token(1, "alice", must_change_password=True)

        access_payload = jwt.decode(access, auth_service.settings.secret_key, algorithms=[auth_service.settings.algorithm])
        refresh_payload = jwt.decode(refresh, auth_service.settings.secret_key, algorithms=[auth_service.settings.algorithm])

        assert access_payload["type"] == "access"
        assert refresh_payload["type"] == "refresh"
        assert access_payload["must_change_password"] is True
        assert refresh_payload["must_change_password"] is True

    def test_decode_token_returns_none_for_invalid_or_expired_token(self):
        expired = auth_service.create_access_token(1, "alice", timedelta(seconds=-1))

        assert auth_service.decode_token("bad-token") is None
        assert auth_service.decode_token(expired) is None

    @pytest.mark.anyio
    async def test_get_current_user_returns_token_data(self):
        token = auth_service.create_access_token(2, "bob")
        creds = HTTPAuthorizationCredentials(scheme="Bearer", credentials=token)

        result = await auth_service.get_current_user(creds)

        assert result.user_id == 2
        assert result.username == "bob"

    @pytest.mark.anyio
    async def test_get_current_user_raises_for_invalid_token(self):
        creds = HTTPAuthorizationCredentials(scheme="Bearer", credentials="bad-token")

        with pytest.raises(APIError) as exc:
            await auth_service.get_current_user(creds)

        assert exc.value.status_code == 401

    @pytest.mark.anyio
    async def test_get_current_user_optional_handles_missing_and_invalid_tokens(self):
        assert await auth_service.get_current_user_optional(None) is None

        bad = HTTPAuthorizationCredentials(scheme="Bearer", credentials="bad-token")
        assert await auth_service.get_current_user_optional(bad) is None
