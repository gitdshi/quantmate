"""User models for authentication."""
from datetime import datetime
from typing import Optional
from pydantic import BaseModel, EmailStr, Field


class UserBase(BaseModel):
    """Base user model."""
    username: str = Field(..., min_length=3, max_length=50)
    email: Optional[EmailStr] = None


class UserCreate(UserBase):
    """User creation model."""
    password: str = Field(..., min_length=6, max_length=100)


class UserLogin(BaseModel):
    """User login model."""
    username: str
    password: str


class User(UserBase):
    """User response model."""
    id: int
    is_active: bool = True
    created_at: datetime
    
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


class TokenData(BaseModel):
    """Token payload data."""
    user_id: int
    username: str
    exp: datetime
