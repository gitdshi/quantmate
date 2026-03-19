"""MFA routes (P2 Issue: Multi-Factor Authentication)."""

import hashlib
import secrets

from fastapi import APIRouter, Depends, status
from pydantic import BaseModel

from app.api.services.auth_service import get_current_user
from app.api.models.user import TokenData
from app.api.errors import ErrorCode
from app.api.exception_handlers import APIError
from app.domains.auth.dao.mfa_dao import MfaDao

router = APIRouter(prefix="/auth/mfa", tags=["MFA"])


class MfaSetupResponse(BaseModel):
    secret: str
    provisioning_uri: str
    recovery_codes: list[str]


class MfaVerifyRequest(BaseModel):
    code: str


class MfaDisableRequest(BaseModel):
    code: str


class MfaRecoveryRequest(BaseModel):
    recovery_code: str


def _generate_totp_secret() -> str:
    """Generate a base32-encoded TOTP secret."""
    return secrets.token_hex(20)


def _generate_recovery_codes(count: int = 8) -> list[str]:
    """Generate one-time recovery codes."""
    return [secrets.token_hex(4).upper() for _ in range(count)]


def _hash_recovery_codes(codes: list[str]) -> str:
    """Hash recovery codes for storage."""
    import json

    hashed = [hashlib.sha256(c.encode()).hexdigest() for c in codes]
    return json.dumps(hashed)


def _verify_totp_code(secret: str, code: str) -> bool:
    """Verify a TOTP code against the secret.

    Uses HMAC-based verification with a 30-second time step.
    In production, use pyotp library. This is a simplified implementation.
    """
    import hmac
    import struct
    import time

    # Simple TOTP verification: accept current and adjacent time steps
    time_step = 30
    current_time = int(time.time()) // time_step

    for offset in range(-1, 2):
        t = current_time + offset
        t_bytes = struct.pack(">Q", t)
        secret_bytes = bytes.fromhex(secret)
        h = hmac.new(secret_bytes, t_bytes, hashlib.sha1).digest()
        o = h[-1] & 0x0F
        truncated = struct.unpack(">I", h[o : o + 4])[0] & 0x7FFFFFFF
        otp = str(truncated % 1000000).zfill(6)
        if hmac.compare_digest(otp, code):
            return True
    return False


@router.post("/setup", response_model=MfaSetupResponse)
async def mfa_setup(current_user: TokenData = Depends(get_current_user)):
    """Set up MFA for the current user. Returns secret and recovery codes."""
    secret = _generate_totp_secret()
    recovery_codes = _generate_recovery_codes()
    codes_hash = _hash_recovery_codes(recovery_codes)

    dao = MfaDao()
    dao.upsert(
        user_id=current_user.user_id,
        mfa_type="totp",
        secret_encrypted=secret,
        recovery_codes_hash=codes_hash,
    )

    # Build provisioning URI for QR code
    username = getattr(current_user, "username", None)
    if not username and isinstance(current_user, dict):
        username = current_user.get("username")
    if not username:
        username = "user"
    uri = f"otpauth://totp/QuantMate:{username}?secret={secret}&issuer=QuantMate"

    return MfaSetupResponse(
        secret=secret,
        provisioning_uri=uri,
        recovery_codes=recovery_codes,
    )


@router.post("/verify")
async def mfa_verify(req: MfaVerifyRequest, current_user: TokenData = Depends(get_current_user)):
    """Verify MFA code and enable MFA."""
    dao = MfaDao()
    mfa = dao.get_by_user_id(current_user.user_id)
    if not mfa:
        raise APIError(
            status_code=status.HTTP_400_BAD_REQUEST,
            code=ErrorCode.VALIDATION_ERROR,
            message="MFA not set up. Call /auth/mfa/setup first.",
        )

    if not _verify_totp_code(mfa["secret_encrypted"], req.code):
        raise APIError(
            status_code=status.HTTP_400_BAD_REQUEST,
            code=ErrorCode.AUTH_INVALID_CREDENTIALS,
            message="Invalid MFA code",
        )

    dao.enable(current_user.user_id)
    return {"message": "MFA enabled successfully"}


@router.post("/disable")
async def mfa_disable(req: MfaDisableRequest, current_user: TokenData = Depends(get_current_user)):
    """Disable MFA for the current user."""
    dao = MfaDao()
    mfa = dao.get_by_user_id(current_user.user_id)
    if not mfa or not mfa["is_enabled"]:
        raise APIError(
            status_code=status.HTTP_400_BAD_REQUEST,
            code=ErrorCode.VALIDATION_ERROR,
            message="MFA is not enabled",
        )

    if not _verify_totp_code(mfa["secret_encrypted"], req.code):
        raise APIError(
            status_code=status.HTTP_400_BAD_REQUEST,
            code=ErrorCode.AUTH_INVALID_CREDENTIALS,
            message="Invalid MFA code",
        )

    dao.disable(current_user.user_id)
    return {"message": "MFA disabled successfully"}


@router.post("/recovery")
async def mfa_recovery(req: MfaRecoveryRequest, current_user: TokenData = Depends(get_current_user)):
    """Use a recovery code to bypass MFA."""
    import json

    dao = MfaDao()
    mfa = dao.get_by_user_id(current_user.user_id)
    if not mfa or not mfa["is_enabled"]:
        raise APIError(
            status_code=status.HTTP_400_BAD_REQUEST,
            code=ErrorCode.VALIDATION_ERROR,
            message="MFA is not enabled",
        )

    code_hash = hashlib.sha256(req.recovery_code.encode()).hexdigest()
    stored_hashes = json.loads(mfa["recovery_codes_hash"]) if mfa["recovery_codes_hash"] else []

    if code_hash not in stored_hashes:
        raise APIError(
            status_code=status.HTTP_400_BAD_REQUEST,
            code=ErrorCode.AUTH_INVALID_CREDENTIALS,
            message="Invalid recovery code",
        )

    # Remove used code
    stored_hashes.remove(code_hash)
    dao.upsert(
        user_id=current_user.user_id,
        mfa_type=mfa["mfa_type"],
        secret_encrypted=mfa["secret_encrypted"],
        recovery_codes_hash=json.dumps(stored_hashes),
    )
    dao.enable(current_user.user_id)

    return {"message": "Recovery code accepted", "remaining_codes": len(stored_hashes)}
