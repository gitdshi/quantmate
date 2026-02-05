"""Authentication routes."""
from datetime import datetime
from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy import text

from app.api.config import get_settings
from app.api.models.user import UserCreate, UserLogin, User, Token, TokenData
from app.api.middleware.auth import (
    get_password_hash,
    verify_password,
    create_access_token,
    create_refresh_token,
    decode_token,
    get_current_user,
)
from app.api.services.db import get_db_connection

router = APIRouter(prefix="/auth", tags=["Authentication"])
settings = get_settings()


@router.post("/register", response_model=User, status_code=status.HTTP_201_CREATED)
async def register(user_data: UserCreate):
    """Register a new user."""
    conn = get_db_connection()
    
    try:
        # Check if username exists
        result = conn.execute(
            text("SELECT id FROM users WHERE username = :username"),
            {"username": user_data.username}
        )
        if result.fetchone():
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Username already registered"
            )
        
        # Check if email exists
        if user_data.email:
            result = conn.execute(
                text("SELECT id FROM users WHERE email = :email"),
                {"email": user_data.email}
            )
            if result.fetchone():
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail="Email already registered"
                )
        
        # Create user
        hashed_password = get_password_hash(user_data.password)
        now = datetime.utcnow()
        
        result = conn.execute(
            text("""
                INSERT INTO users (username, email, hashed_password, is_active, created_at)
                VALUES (:username, :email, :hashed_password, 1, :created_at)
            """),
            {
                "username": user_data.username,
                "email": user_data.email,
                "hashed_password": hashed_password,
                "created_at": now
            }
        )
        conn.commit()
        
        user_id = result.lastrowid
        
        return User(
            id=user_id,
            username=user_data.username,
            email=user_data.email,
            is_active=True,
            created_at=now
        )
    finally:
        conn.close()


@router.post("/login", response_model=Token)
async def login(credentials: UserLogin):
    """Login and get access token."""
    conn = get_db_connection()
    
    try:
        result = conn.execute(
            text("SELECT id, username, hashed_password, is_active FROM users WHERE username = :username"),
            {"username": credentials.username}
        )
        user = result.fetchone()
        
        if not user:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Incorrect username or password"
            )
        
        if not verify_password(credentials.password, user.hashed_password):
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Incorrect username or password"
            )
        
        if not user.is_active:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="User account is disabled"
            )
        
        access_token = create_access_token(user.id, user.username)
        refresh_token = create_refresh_token(user.id, user.username)
        
        return Token(
            access_token=access_token,
            refresh_token=refresh_token,
            token_type="bearer",
            expires_in=settings.access_token_expire_minutes * 60
        )
    finally:
        conn.close()


@router.post("/refresh", response_model=Token)
async def refresh_token(refresh_token: str):
    """Refresh access token using refresh token."""
    token_data = decode_token(refresh_token)
    
    if token_data is None:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid or expired refresh token"
        )
    
    # Verify user still exists and is active
    conn = get_db_connection()
    try:
        result = conn.execute(
            text("SELECT id, username, is_active FROM users WHERE id = :user_id"),
            {"user_id": token_data.user_id}
        )
        user = result.fetchone()
        
        if not user or not user.is_active:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="User not found or inactive"
            )
        
        new_access_token = create_access_token(user.id, user.username)
        new_refresh_token = create_refresh_token(user.id, user.username)
        
        return Token(
            access_token=new_access_token,
            refresh_token=new_refresh_token,
            token_type="bearer",
            expires_in=settings.access_token_expire_minutes * 60
        )
    finally:
        conn.close()


@router.get("/me", response_model=User)
async def get_me(current_user: TokenData = Depends(get_current_user)):
    """Get current user info."""
    conn = get_db_connection()
    
    try:
        result = conn.execute(
            text("SELECT id, username, email, is_active, created_at FROM users WHERE id = :user_id"),
            {"user_id": current_user.user_id}
        )
        user = result.fetchone()
        
        if not user:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="User not found"
            )
        
        return User(
            id=user.id,
            username=user.username,
            email=user.email,
            is_active=user.is_active,
            created_at=user.created_at
        )
    finally:
        conn.close()
