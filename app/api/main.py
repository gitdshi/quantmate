"""QuantMate API - FastAPI Application."""

import sys
import os
import secrets
from pathlib import Path
from contextlib import asynccontextmanager
from datetime import datetime, timezone

from dotenv import load_dotenv

load_dotenv()

# Ensure project root is importable
ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

# Configure logging (ensure timestamps are present in logs)
from app.infrastructure.logging import configure_logging, get_logger  # noqa: E402

configure_logging()
logger = get_logger(__name__)

from fastapi import FastAPI, Request, status, Depends
from fastapi.middleware.cors import CORSMiddleware
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from fastapi.responses import RedirectResponse
from typing import Optional

from app.infrastructure.config import get_settings

# Note: schema creation/migrations are handled outside the running app.
from app.api.routes import auth, strategies, data, backtest, queue
from app.api.routes import system
from app.api.routes import strategy_code
from app.api.routes import audit
from app.api.routes import kyc
from app.api.routes import settings as settings_routes
from app.api.routes import watchlist
from app.api.routes import portfolio
from app.api.routes import analytics
from app.api.routes import trade_log
from app.api.routes import mfa
from app.api.routes import api_keys
from app.api.routes import sessions
from app.api.routes import trading
from app.api.routes import paper_trading
from app.api.routes import risk
from app.api.routes import alerts
from app.api.routes import reports
from app.api.routes import broker
from app.api.routes import system_config
from app.api.routes import indicators
from app.api.routes import optimization
from app.api.routes import websocket
from app.api.routes import ai as ai_routes
from app.api.routes import factors as factor_routes
from app.api.routes import templates as template_routes
from app.api.routes import teams as team_routes
from app.api.routes import multi_market
from app.api.routes import ai_model as ai_model_routes
from app.api.exception_handlers import register_exception_handlers, APIError
from app.api.errors import ErrorCode

from app.domains.auth.dao.user_dao import UserDao
from app.api.services.auth_service import get_password_hash

settings = get_settings()

security = HTTPBearer(auto_error=False)


async def ensure_password_changed(
    request: Request, credentials: Optional[HTTPAuthorizationCredentials] = Depends(security)
):
    """Global dependency to enforce password change on first login for admin."""
    exempt_paths = [
        "/api/v1/auth/login",
        "/api/v1/auth/register",
        "/api/v1/auth/refresh",
        "/api/v1/auth/change-password",
        "/docs",
        "/redoc",
        "/openapi.json",
    ]
    path = request.url.path
    if any(path.startswith(p) for p in exempt_paths):
        return
    if credentials is None:
        return  # No credentials; let route's own dependency handle it if needed
    try:
        from app.api.services.auth_service import decode_token

        token_data = decode_token(credentials.credentials)
    except Exception:
        raise APIError(
            status_code=status.HTTP_401_UNAUTHORIZED,
            code=ErrorCode.AUTH_INVALID_TOKEN,
            message="Invalid or expired token",
        )
    if token_data is None:
        raise APIError(
            status_code=status.HTTP_401_UNAUTHORIZED,
            code=ErrorCode.AUTH_INVALID_TOKEN,
            message="Invalid or expired token",
        )
    if token_data.must_change_password:
        raise APIError(
            status_code=status.HTTP_403_FORBIDDEN,
            code=ErrorCode.AUTH_PASSWORD_CHANGE_REQUIRED,
            message="Password change required. Please change your password first.",
        )


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Application lifespan events."""
    # Startup
    logger.info("Starting QuantMate API...")
    logger.info("Database migrations should be applied during runtime init")

    # Admin user initialization
    try:
        admin_username = os.getenv("ADMIN_USERNAME", "admin")
        admin_email = os.getenv("ADMIN_EMAIL", "admin@quantmate.local")
        admin_password = os.getenv("ADMIN_PASSWORD")
        is_production = not settings.debug

        user_dao = UserDao()
        # Check if admin exists
        admin_user = user_dao.get_user_for_login(admin_username)

        # Ensure admin credentials are consistent with env on startup.
        # This keeps staging recoverable when DB already has an "admin" user from older deployments.
        if is_production and not admin_password:
            raise RuntimeError("ADMIN_PASSWORD environment variable must be set in production mode")

        if admin_user is None:
            # Admin user does not exist, create it
            if not admin_password:
                # Generate a secure random password (20+ chars)
                admin_password = secrets.token_urlsafe(32)
                logger.info(f"Generated random admin password: {admin_password}")
            hashed = get_password_hash(admin_password)
            now = datetime.utcnow()
            user_dao.insert_user(
                username=admin_username,
                email=admin_email,
                hashed_password=hashed,
                created_at=now,
                must_change_password=True,
            )
            logger.info(f"Admin user '{admin_username}' created. First login will require password change.")
        else:
            # Existing admin: if ADMIN_PASSWORD is explicitly provided, enforce it.
            if admin_password:
                user_dao.update_user_password(
                    admin_user["id"], get_password_hash(admin_password), must_change_password=False
                )
                logger.info("Updated existing admin password from ADMIN_PASSWORD without forcing password change.")

            # Existing admin: upgrade if necessary
            DEFAULT_ADMIN_HASH = "$2b$12$LQv3c1yqBWVHxkd0LHAkCOYz6TtxMQJqhN8/LewY5GyYqVvmvhxKe"
            if admin_user["hashed_password"] == DEFAULT_ADMIN_HASH:
                if admin_user.get("must_change_password") is False:
                    user_dao.update_user_password(
                        admin_user["id"], admin_user["hashed_password"], must_change_password=False
                    )
                    logger.info("Kept existing default admin user without forcing password change.")
            # Ensure column exists: could attempt to ALTER TABLE if needed, but skip for now
    except Exception as e:
        logger.error(f"Admin initialization failed: {e}")
        if is_production:
            raise
        # In development, continue even if admin init fails (e.g., DB not ready)

    yield

    # Shutdown
    logger.info("Shutting down QuantMate API...")


app = FastAPI(
    title=settings.app_name,
    version=settings.app_version,
    description="QuantMate Trading Platform API - Strategy Management, Backtesting, and Market Research",
    docs_url="/docs",
    redoc_url="/redoc",
    lifespan=lifespan,
    dependencies=[Depends(ensure_password_changed)],
)

# Register standardized exception handlers (Issue #16)
register_exception_handlers(app)

# CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.cors_origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Rate limiting middleware (Issue #14)
from app.api.rate_limit import RateLimitMiddleware  # noqa: E402

app.add_middleware(RateLimitMiddleware)

# Audit logging middleware (Issue #2)
from app.api.audit_middleware import AuditMiddleware  # noqa: E402

app.add_middleware(AuditMiddleware)

# Include routers under /api/v1 (Issue #13: API versioning)
app.include_router(auth.router, prefix="/api/v1")
app.include_router(strategies.router, prefix="/api/v1")
app.include_router(data.router, prefix="/api/v1")
app.include_router(backtest.router, prefix="/api/v1")
app.include_router(queue.router, prefix="/api/v1")
app.include_router(system.router, prefix="/api/v1")
app.include_router(strategy_code.router, prefix="/api/v1")
app.include_router(audit.router, prefix="/api/v1")
app.include_router(kyc.router, prefix="/api/v1")
app.include_router(settings_routes.router, prefix="/api/v1")
app.include_router(watchlist.router, prefix="/api/v1")
app.include_router(portfolio.router, prefix="/api/v1")
app.include_router(analytics.router, prefix="/api/v1")
app.include_router(trade_log.router, prefix="/api/v1")
app.include_router(mfa.router, prefix="/api/v1")
app.include_router(api_keys.router, prefix="/api/v1")
app.include_router(sessions.router, prefix="/api/v1")
app.include_router(trading.router, prefix="/api/v1")
app.include_router(paper_trading.router, prefix="/api/v1")
app.include_router(risk.router, prefix="/api/v1")
app.include_router(alerts.router, prefix="/api/v1")
app.include_router(reports.router, prefix="/api/v1")
app.include_router(broker.router, prefix="/api/v1")
app.include_router(system_config.router, prefix="/api/v1")
app.include_router(indicators.router, prefix="/api/v1")
app.include_router(optimization.router, prefix="/api/v1")
app.include_router(websocket.router, prefix="/api/v1")
app.include_router(ai_routes.router, prefix="/api/v1")
app.include_router(factor_routes.router, prefix="/api/v1")
app.include_router(template_routes.router, prefix="/api/v1")
app.include_router(team_routes.router, prefix="/api/v1")
app.include_router(multi_market.router, prefix="/api/v1")
app.include_router(ai_model_routes.router, prefix="/api/v1")


# Legacy /api/* → /api/v1/* redirect (Issue #13: transition period)
@app.middleware("http")
async def legacy_api_redirect(request: Request, call_next):
    """Redirect old /api/<route> paths to /api/v1/<route> with 307 (preserves HTTP method)."""
    path = request.url.path
    if path.startswith("/api/") and not path.startswith("/api/v1/"):
        new_path = "/api/v1/" + path[len("/api/") :]
        query = str(request.url.query)
        url = new_path + ("?" + query if query else "")
        return RedirectResponse(url=url, status_code=307)
    return await call_next(request)


@app.get("/", dependencies=[])
async def root():
    """Root endpoint."""
    return {"name": settings.app_name, "version": settings.app_version, "docs": "/docs", "status": "running"}


@app.get("/health", dependencies=[])
async def health():
    """Health check endpoint with database and Redis connectivity checks."""
    from sqlalchemy import text
    import redis

    health_status = {
        "status": "healthy",
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "service": "quantmate",
        "dependencies": {},
    }

    # Check MySQL connection
    try:
        from app.infrastructure.db.connections import get_quantmate_engine

        engine = get_quantmate_engine()
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        health_status["dependencies"]["mysql"] = {"status": "healthy"}
    except Exception as e:
        health_status["status"] = "unhealthy"
        health_status["dependencies"]["mysql"] = {"status": "unhealthy", "error": str(e)}
        logger.error(f"MySQL health check failed: {e}")

    # Check Redis connection
    try:
        r = redis.Redis.from_url(settings.redis_url)
        r.ping()
        health_status["dependencies"]["redis"] = {"status": "healthy"}
    except Exception as e:
        health_status["status"] = "unhealthy"
        health_status["dependencies"]["redis"] = {"status": "unhealthy", "error": str(e)}
        logger.error(f"Redis health check failed: {e}")

    # Return 503 if unhealthy
    from fastapi.responses import JSONResponse

    if health_status["status"] != "healthy":
        return JSONResponse(status_code=503, content=health_status)

    return health_status


@app.get("/api")
async def api_info():
    """API information."""
    return {
        "version": settings.app_version,
        "endpoints": {
            "auth": "/api/auth",
            "strategies": "/api/strategies",
            "backtest": "/api/backtest",
            "data": "/api/data",
        },
    }


@app.get("/metrics")
async def metrics():
    """Prometheus metrics endpoint."""
    from app.datasync.metrics import get_metrics
    from fastapi.responses import PlainTextResponse

    return PlainTextResponse(content=get_metrics(), media_type="text/plain; version=0.0.4")


if __name__ == "__main__":
    import uvicorn

    uvicorn.run("app.api.main:app", host="0.0.0.0", port=8000, reload=settings.debug)
