"""Unit tests for app.api.audit_middleware."""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from app.api.audit_middleware import (
    _classify_request,
    _extract_resource_id,
    _extract_user_info,
    AuditMiddleware,
)


# ── _classify_request ─────────────────────────────────────────────

def test_classify_login():
    op, res = _classify_request("/api/v1/auth/login", "POST")
    assert op == "AUTH_LOGIN"
    assert res == "user"


def test_classify_register():
    op, res = _classify_request("/api/v1/auth/register", "POST")
    assert op == "AUTH_REGISTER"


def test_classify_strategy_create():
    op, res = _classify_request("/api/v1/strategies", "POST")
    assert op == "STRATEGY_CREATE"
    assert res == "strategy"


def test_classify_strategy_get():
    op, res = _classify_request("/api/v1/strategies/1", "GET")
    assert op == "STRATEGY_VIEW"


def test_classify_strategy_put():
    op, res = _classify_request("/api/v1/strategies/1", "PUT")
    assert op == "STRATEGY_UPDATE"


def test_classify_strategy_delete():
    op, res = _classify_request("/api/v1/strategies/1", "DELETE")
    assert op == "STRATEGY_DELETE"


def test_classify_backtest_submit():
    op, res = _classify_request("/api/v1/backtest", "POST")
    assert op == "BACKTEST_SUBMIT"
    assert res == "backtest"


def test_classify_order_create():
    op, res = _classify_request("/api/v1/trade/orders", "POST")
    assert op == "TRADING_ORDER_CREATE"
    assert res == "order"


def test_classify_order_cancel():
    op, res = _classify_request("/api/v1/trade/orders/123/cancel", "POST")
    assert op == "TRADING_ORDER_CANCEL"
    assert res == "order"


def test_classify_portfolio_close():
    op, res = _classify_request("/api/v1/portfolio/close", "POST")
    assert op == "PORTFOLIO_UPDATE"


def test_classify_config_update():
    op, res = _classify_request("/api/v1/system/configs", "PUT")
    assert op == "CONFIG_UPDATE"
    assert res == "system_config"


def test_classify_data_source_update():
    op, res = _classify_request("/api/v1/system/data-sources", "PUT")
    assert op == "DATA_SOURCE_UPDATE"


def test_classify_paper_trade_deploy():
    op, res = _classify_request("/api/v1/paper-trade/deploy", "POST")
    assert op == "PAPER_TRADE_START"


def test_classify_paper_trade_stop():
    op, res = _classify_request("/api/v1/paper-trade/deployments/5/stop", "POST")
    assert op == "PAPER_TRADE_STOP"


def test_classify_unknown():
    op, res = _classify_request("/api/v1/unknown", "GET")
    assert op == "API_GET"
    assert res is None


def test_classify_data_access():
    op, res = _classify_request("/api/v1/data/stocks", "GET")
    assert op == "DATA_ACCESS"
    assert res == "data"


# ── _extract_resource_id ─────────────────────────────────────────

def test_extract_resource_id_numeric():
    assert _extract_resource_id("/api/v1/strategies/123") == "123"


def test_extract_resource_id_uuid():
    assert _extract_resource_id("/api/v1/jobs/abc-def-123-456") == "abc-def-123-456"


def test_extract_resource_id_none():
    assert _extract_resource_id("/api/v1/strategies") is None


# ── _extract_user_info ────────────────────────────────────────────

def test_extract_user_info_no_auth():
    req = MagicMock()
    req.headers.get.return_value = ""
    uid, uname = _extract_user_info(req)
    assert uid is None and uname is None


def test_extract_user_info_non_bearer():
    req = MagicMock()
    req.headers.get.return_value = "Basic abc"
    uid, uname = _extract_user_info(req)
    assert uid is None


def test_extract_user_info_valid_bearer():
    from app.api.services.auth_service import create_access_token

    tok = create_access_token(42, "alice")
    req = MagicMock()
    req.headers.get.return_value = f"Bearer {tok}"
    uid, uname = _extract_user_info(req)
    assert uid == 42
    assert uname == "alice"


def test_extract_user_info_invalid_bearer():
    req = MagicMock()
    req.headers.get.return_value = "Bearer invalid.token.here"
    uid, uname = _extract_user_info(req)
    assert uid is None


# ── AuditMiddleware dispatch ──────────────────────────────────────

@pytest.mark.asyncio
async def test_middleware_skips_non_api_paths():
    app = MagicMock()
    mw = AuditMiddleware(app)
    req = MagicMock()
    req.url.path = "/health"
    mock_next = AsyncMock(return_value=MagicMock(status_code=200))
    response = await mw.dispatch(req, mock_next)
    mock_next.assert_awaited_once()


@pytest.mark.asyncio
async def test_middleware_skips_docs():
    app = MagicMock()
    mw = AuditMiddleware(app)
    req = MagicMock()
    req.url.path = "/docs"
    mock_next = AsyncMock(return_value=MagicMock(status_code=200))
    response = await mw.dispatch(req, mock_next)
    mock_next.assert_awaited_once()


@pytest.mark.asyncio
async def test_middleware_logs_api_request():
    app = MagicMock()
    mw = AuditMiddleware(app)
    req = MagicMock()
    req.url.path = "/api/v1/strategies"
    req.method = "GET"
    req.headers.get.side_effect = lambda k, d="": {
        "authorization": "",
        "x-forwarded-for": "1.2.3.4",
        "user-agent": "test-agent",
    }.get(k, d)
    req.client.host = "127.0.0.1"
    mock_response = MagicMock(status_code=200)
    mock_next = AsyncMock(return_value=mock_response)

    with patch("app.domains.audit.dao.audit_log_dao.AuditLogDao") as DaoCls:
        dao = DaoCls.return_value
        response = await mw.dispatch(req, mock_next)
    assert response.status_code == 200
    dao.insert.assert_called_once()


@pytest.mark.asyncio
async def test_middleware_handles_audit_failure_gracefully():
    app = MagicMock()
    mw = AuditMiddleware(app)
    req = MagicMock()
    req.url.path = "/api/v1/strategies"
    req.method = "GET"
    req.headers.get.return_value = ""
    req.client.host = "127.0.0.1"
    mock_response = MagicMock(status_code=200)
    mock_next = AsyncMock(return_value=mock_response)

    with patch("app.domains.audit.dao.audit_log_dao.AuditLogDao", side_effect=Exception("db down")):
        response = await mw.dispatch(req, mock_next)
    # Should not break the response
    assert response.status_code == 200
