"""User models for authentication."""

from datetime import datetime
from typing import Optional
from pydantic import BaseModel, EmailStr, Field


class UserBase(BaseModel):
    """Base user model."""

    username: str = Field(..., min_length=3, max_length=50)
    # Keep responses lenient to avoid failing on reserved/local domains.
    email: Optional[str] = None


class UserCreate(UserBase):
    """User creation model."""

    email: Optional[EmailStr] = None
    password: str = Field(..., min_length=6, max_length=100)


class UserLogin(BaseModel):
    """User login model (username or email)."""

    username: Optional[str] = None
    email: Optional[EmailStr] = None
    password: str


class User(UserBase):
    """User response model."""

    id: int
    is_active: bool = True
    created_at: datetime
    role: Optional[str] = None
    primary_role: Optional[str] = None
    permissions: list[str] = Field(default_factory=list)

    class Config:
        from_attributes = True


class UserInDB(User):
    """User model with hashed password (internal use)."""

    hashed_password: str


class Token(BaseModel):
    """JWT token response."""

    access_token: str
    refresh_token: str
    token_type: str = "bearer"
    expires_in: int  # seconds
    must_change_password: bool = False
    user: Optional[User] = None


class TokenData(BaseModel):
    """Token payload data."""

    user_id: int
    username: str
    exp: datetime
    must_change_password: bool = False


class PasswordChangeRequest(BaseModel):
    """Request to change password."""

    current_password: str = Field(..., min_length=1, max_length=100)
    new_password: str = Field(..., min_length=6, max_length=100)


# --- User Profile (Issue #8) ---

VALID_TIMEZONES = {
    "Asia/Shanghai",
    "Asia/Hong_Kong",
    "Asia/Tokyo",
    "Asia/Singapore",
    "US/Eastern",
    "US/Pacific",
    "US/Central",
    "Europe/London",
    "Europe/Berlin",
    "UTC",
}

VALID_LANGUAGES = {"zh-CN", "zh-TW", "en-US", "en-GB", "ja-JP"}


class UserProfileUpdate(BaseModel):
    """Fields that can be updated on the user profile."""

    display_name: Optional[str] = Field(None, max_length=100)
    avatar_url: Optional[str] = Field(None, max_length=500)
    phone: Optional[str] = Field(None, max_length=30, pattern=r"^\+?[\d\-\s]{5,20}$")
    timezone: Optional[str] = None
    language: Optional[str] = None
    bio: Optional[str] = Field(None, max_length=500)


class UserProfileResponse(BaseModel):
    """User profile response."""

    user_id: int
    display_name: Optional[str] = None
    avatar_url: Optional[str] = None
    phone: Optional[str] = None
    timezone: str = "Asia/Shanghai"
    language: str = "zh-CN"
    bio: Optional[str] = None
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None

    class Config:
        from_attributes = True
