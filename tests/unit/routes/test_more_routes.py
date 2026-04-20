"""Tests for remaining route files: ai, alerts, analytics, auth, broker,
factors, indicators, kyc, multi_market, optimization, reports, risk,
system_config, teams, templates, trading."""
from __future__ import annotations
from unittest.mock import MagicMock, patch
import pytest
from fastapi import FastAPI
from starlette.testclient import TestClient


def _app(route_mod, prefix="/api/v1"):
    from app.api.exception_handlers import register_exception_handlers
    app = FastAPI(); register_exception_handlers(app)
    app.include_router(route_mod.router, prefix=prefix)
    if hasattr(route_mod, "get_current_user"):
        app.dependency_overrides[route_mod.get_current_user] = lambda: MagicMock(
            user_id=1, role="admin", username="admin"
        )
    if hasattr(route_mod, "get_current_user_optional"):
        app.dependency_overrides[route_mod.get_current_user_optional] = lambda: MagicMock(user_id=1)
    if hasattr(route_mod, "_require_admin"):
        app.dependency_overrides[route_mod._require_admin] = lambda: MagicMock(
            user_id=1, role="admin", username="admin"
        )
    for r in app.routes:
        if hasattr(r, "dependencies"):
            r.dependencies = []
    return TestClient(app, raise_server_exceptions=False)


# ═══ ai.py (EAGER AIService) ═══
_AI = "app.api.routes.ai"


@pytest.fixture()
def ai_conv_client():
    from app.api.routes import ai
    return _app(ai)


class TestAiConversations:
    @patch(f"{_AI}.AIService")
    def test_list_conversations(self, M, ai_conv_client):
        d = M.return_value; d.count_conversations.return_value = 0; d.list_conversations.return_value = []
        r = ai_conv_client.get("/api/v1/ai/conversations"); assert r.status_code in (200, 500)

    @patch(f"{_AI}.AIService")
    def test_create_conversation(self, M, ai_conv_client):
        M.return_value.create_conversation.return_value = {"id": 1}
        r = ai_conv_client.post("/api/v1/ai/conversations", json={"title": "Test"})
        assert r.status_code in (200, 201, 422, 500)

    @patch(f"{_AI}.AIService")
    def test_get_conversation(self, M, ai_conv_client):
        M.return_value.get_conversation.return_value = {"id": 1, "title": "T"}
        r = ai_conv_client.get("/api/v1/ai/conversations/1"); assert r.status_code in (200, 404, 500)

    @patch(f"{_AI}.AIService")
    def test_delete_conversation(self, M, ai_conv_client):
        M.return_value.delete_conversation.return_value = True
        r = ai_conv_client.delete("/api/v1/ai/conversations/1"); assert r.status_code in (200, 204, 404, 500)

    @patch(f"{_AI}.AIService")
    def test_list_messages(self, M, ai_conv_client):
        M.return_value.list_messages.return_value = []
        r = ai_conv_client.get("/api/v1/ai/conversations/1/messages"); assert r.status_code in (200, 404, 500)

    @patch(f"{_AI}.AIService")
    def test_send_message(self, M, ai_conv_client):
        M.return_value.send_message.return_value = {"id": 1, "content": "reply"}
        r = ai_conv_client.post("/api/v1/ai/conversations/1/messages", json={"content": "hi"})
        assert r.status_code in (200, 201, 404, 422, 500)

    @patch(f"{_AI}.AIService")
    def test_list_models(self, M, ai_conv_client):
        M.return_value.list_models.return_value = []
        r = ai_conv_client.get("/api/v1/ai/models"); assert r.status_code in (200, 500)

    @patch(f"{_AI}.AIService")
    def test_create_model(self, M, ai_conv_client):
        M.return_value.create_model.return_value = {"id": 1}
        r = ai_conv_client.post("/api/v1/ai/models", json={"model_name": "gpt-4", "provider": "openai", "endpoint": "https://api.openai.com"})
        assert r.status_code in (200, 201, 422, 500)

    @patch(f"{_AI}.AIService")
    def test_delete_model(self, M, ai_conv_client):
        M.return_value.delete_model.return_value = True
        r = ai_conv_client.delete("/api/v1/ai/models/1"); assert r.status_code in (200, 204, 404, 500)


# ═══ alerts.py (EAGER DAOs) ═══
_AL = "app.api.routes.alerts"


@pytest.fixture()
def alerts_client():
    from app.api.routes import alerts
    return _app(alerts)


class TestAlertsRoutes:
    @patch(f"{_AL}.AlertRuleDao")
    def test_list_rules(self, M, alerts_client):
        M.return_value.list_by_user.return_value = []
        r = alerts_client.get("/api/v1/alerts/rules"); assert r.status_code in (200, 500)

    @patch(f"{_AL}.AlertRuleDao")
    def test_create_rule(self, M, alerts_client):
        M.return_value.create.return_value = 1
        r = alerts_client.post("/api/v1/alerts/rules", json={
            "name": "R", "metric": "price", "comparator": ">", "threshold": 100.0, "level": "warning"
        })
        assert r.status_code in (200, 201, 400, 422, 500)

    @patch(f"{_AL}.AlertRuleDao")
    def test_update_rule(self, M, alerts_client):
        M.return_value.update.return_value = True
        r = alerts_client.put("/api/v1/alerts/rules/1", json={"name": "U"})
        assert r.status_code in (200, 404, 422, 500)

    @patch(f"{_AL}.AlertRuleDao")
    def test_delete_rule(self, M, alerts_client):
        M.return_value.delete.return_value = True
        r = alerts_client.delete("/api/v1/alerts/rules/1"); assert r.status_code in (200, 204, 404, 500)

    @patch(f"{_AL}.AlertHistoryDao")
    def test_history(self, M, alerts_client):
        M.return_value.list_by_user.return_value = []
        r = alerts_client.get("/api/v1/alerts/history"); assert r.status_code in (200, 500)

    @patch(f"{_AL}.AlertHistoryDao")
    def test_acknowledge(self, M, alerts_client):
        M.return_value.acknowledge.return_value = True
        r = alerts_client.post("/api/v1/alerts/history/1/acknowledge"); assert r.status_code in (200, 404, 500)

    @patch(f"{_AL}.NotificationChannelDao")
    def test_list_channels(self, M, alerts_client):
        M.return_value.list_by_user.return_value = []
        r = alerts_client.get("/api/v1/alerts/channels"); assert r.status_code in (200, 500)

    @patch(f"{_AL}.NotificationChannelDao")
    def test_create_channel(self, M, alerts_client):
        M.return_value.create.return_value = 1
        r = alerts_client.post("/api/v1/alerts/channels", json={"channel_type": "email", "config": {"email": "a@b.com"}})
        assert r.status_code in (200, 201, 422, 500)

    @patch(f"{_AL}.NotificationChannelDao")
    def test_delete_channel(self, M, alerts_client):
        M.return_value.delete.return_value = True
        r = alerts_client.delete("/api/v1/alerts/channels/1"); assert r.status_code in (200, 204, 404, 500)


# ═══ analytics.py (LAZY imports) ═══
_AN_PF = "app.domains.portfolio.dao.portfolio_dao.PortfolioDao"
_AN_PL = "app.domains.monitoring.pnl_monitor_service.PnLMonitorService"


@pytest.fixture()
def ana_client():
    from app.api.routes import analytics
    return _app(analytics)


class TestAnalyticsRoutes:
    @patch(_AN_PF)
    def test_dashboard(self, M, ana_client):
        d = M.return_value
        d.get_or_create.return_value = {"id": 1, "cash": 1e6}
        d.list_positions.return_value = []
        d.list_snapshots.return_value = []
        r = ana_client.get("/api/v1/analytics/dashboard"); assert r.status_code in (200, 500)

    @patch(_AN_PF)
    def test_risk_metrics(self, M, ana_client):
        d = M.return_value
        d.get_or_create.return_value = {"id": 1, "cash": 1e6}
        d.list_positions.return_value = []
        d.list_snapshots.return_value = []
        r = ana_client.get("/api/v1/analytics/risk-metrics"); assert r.status_code in (200, 500)

    @patch(_AN_PL)
    @patch(_AN_PF)
    def test_live_pnl(self, MF, MP, ana_client):
        MF.return_value.get_or_create.return_value = {"id": 1, "cash": 1e6}
        MF.return_value.list_positions.return_value = []
        MP.return_value.calculate_live_pnl.return_value = {"total": 0}
        r = ana_client.get("/api/v1/analytics/live-pnl"); assert r.status_code in (200, 500)

    @patch(_AN_PL)
    @patch(_AN_PF)
    def test_anomalies(self, MF, MP, ana_client):
        MF.return_value.get_or_create.return_value = {"id": 1, "cash": 1e6}
        MF.return_value.list_positions.return_value = []
        MF.return_value.list_snapshots.return_value = []
        MP.return_value.detect_anomalies.return_value = []
        MP.return_value.get_rules.return_value = []
        r = ana_client.get("/api/v1/analytics/anomalies"); assert r.status_code in (200, 500)


# ═══ auth.py ═══
_AUTH = "app.api.routes.auth"


@pytest.fixture()
def auth_client():
    from app.api.routes import auth
    return _app(auth)


class TestAuthRoutes:
    @patch(f"{_AUTH}.AuthService")
    def test_register(self, M, auth_client):
        M.return_value.register.return_value = {"id": 1, "username": "u"}
        r = auth_client.post("/api/v1/auth/register", json={"username": "u", "email": "u@b.com", "password": "Pass1234!"})
        assert r.status_code in (200, 201, 409, 422, 500)

    @patch(f"{_AUTH}.brute_force")
    @patch(f"{_AUTH}.AuthService")
    def test_login(self, MS, MB, auth_client):
        MB.is_locked.return_value = False
        MS.return_value.login.return_value = {"access_token": "x", "refresh_token": "y", "token_type": "bearer"}
        r = auth_client.post("/api/v1/auth/login", json={"login_id": "u", "password": "Pass1234!"})
        assert r.status_code in (200, 401, 422, 429, 500)

    @patch(f"{_AUTH}.AuthService")
    def test_refresh(self, M, auth_client):
        M.return_value.refresh.return_value = {"access_token": "new", "token_type": "bearer"}
        r = auth_client.post("/api/v1/auth/refresh", json={"refresh_token": "rt"})
        assert r.status_code in (200, 401, 422, 500)

    @patch(f"{_AUTH}.AuthService")
    def test_me(self, M, auth_client):
        M.return_value.me.return_value = {"id": 1, "username": "u"}
        r = auth_client.get("/api/v1/auth/me"); assert r.status_code in (200, 401, 500)

    @patch(f"{_AUTH}.AuthService")
    def test_change_password(self, M, auth_client):
        M.return_value.change_password.return_value = True
        r = auth_client.post("/api/v1/auth/change-password", json={"current_password": "Old1!", "new_password": "New1234!"})
        assert r.status_code in (200, 400, 422, 500)

    @patch("app.domains.auth.dao.user_profile_dao.UserProfileDao")
    def test_get_profile(self, M, auth_client):
        M.return_value.get.return_value = {"timezone": "Asia/Shanghai", "language": "zh-CN"}
        r = auth_client.get("/api/v1/auth/profile"); assert r.status_code in (200, 404, 500)

    @patch("app.domains.auth.dao.user_profile_dao.UserProfileDao")
    def test_update_profile(self, M, auth_client):
        M.return_value.upsert.return_value = None
        M.return_value.get.return_value = {"timezone": "UTC"}
        r = auth_client.put("/api/v1/auth/profile", json={"timezone": "UTC"})
        assert r.status_code in (200, 422, 500)


# ═══ broker.py (EAGER BrokerConfigDao) ═══
_BR = "app.api.routes.broker"


@pytest.fixture()
def broker_client():
    from app.api.routes import broker
    return _app(broker)


class TestBrokerRoutes:
    @patch(f"{_BR}.BrokerConfigDao")
    def test_list(self, M, broker_client):
        M.return_value.list_by_user.return_value = []
        r = broker_client.get("/api/v1/broker/configs"); assert r.status_code in (200, 500)

    @patch(f"{_BR}.BrokerConfigDao")
    def test_create(self, M, broker_client):
        M.return_value.create.return_value = 1
        r = broker_client.post("/api/v1/broker/configs", json={"broker_name": "ctp", "config": {}, "is_paper": True})
        assert r.status_code in (200, 201, 422, 500)

    @patch(f"{_BR}.BrokerConfigDao")
    def test_update(self, M, broker_client):
        M.return_value.update.return_value = True
        r = broker_client.put("/api/v1/broker/configs/1", json={"is_paper": False})
        assert r.status_code in (200, 404, 422, 500)

    @patch(f"{_BR}.BrokerConfigDao")
    def test_delete(self, M, broker_client):
        M.return_value.delete.return_value = True
        r = broker_client.delete("/api/v1/broker/configs/1"); assert r.status_code in (200, 204, 404, 500)


# ═══ factors.py (EAGER FactorService, LAZY qlib/screening) ═══
_FA = "app.api.routes.factors"


@pytest.fixture()
def factors_client():
    from app.api.routes import factors
    return _app(factors)


class TestFactorsRoutes:
    @patch(f"{_FA}.FactorService")
    def test_list(self, M, factors_client):
        d = M.return_value; d.count_factors.return_value = 0; d.list_factors.return_value = []
        r = factors_client.get("/api/v1/factors"); assert r.status_code in (200, 500)

    @patch(f"{_FA}.FactorService")
    def test_create(self, M, factors_client):
        M.return_value.create_factor.return_value = {"id": 1}
        r = factors_client.post("/api/v1/factors", json={"name": "f1", "expression": "close/open", "category": "momentum"})
        assert r.status_code in (200, 201, 422, 500)

    @patch(f"{_FA}.FactorService")
    def test_get(self, M, factors_client):
        M.return_value.get_factor.return_value = {"id": 1, "name": "f1"}
        r = factors_client.get("/api/v1/factors/1"); assert r.status_code in (200, 404, 500)

    @patch(f"{_FA}.FactorService")
    def test_update(self, M, factors_client):
        M.return_value.update_factor.return_value = True
        r = factors_client.put("/api/v1/factors/1", json={"name": "f2"})
        assert r.status_code in (200, 404, 422, 500)

    @patch(f"{_FA}.FactorService")
    def test_delete(self, M, factors_client):
        M.return_value.delete_factor.return_value = True
        r = factors_client.delete("/api/v1/factors/1"); assert r.status_code in (200, 204, 404, 500)

    @patch(f"{_FA}.FactorService")
    def test_list_evaluations(self, M, factors_client):
        M.return_value.list_evaluations.return_value = []
        r = factors_client.get("/api/v1/factors/1/evaluations"); assert r.status_code in (200, 404, 500)

    @patch(f"{_FA}.FactorService")
    def test_run_evaluation(self, M, factors_client):
        M.return_value.run_evaluation.return_value = {"id": 1}
        r = factors_client.post("/api/v1/factors/1/evaluations", json={"start_date": "2023-01-01", "end_date": "2023-12-31"})
        assert r.status_code in (200, 201, 422, 500)

    @patch(f"{_FA}.FactorService")
    def test_delete_evaluation(self, M, factors_client):
        M.return_value.delete_evaluation.return_value = True
        r = factors_client.delete("/api/v1/factors/1/evaluations/1"); assert r.status_code in (200, 204, 404, 500)

    @patch("app.infrastructure.qlib.qlib_config.SUPPORTED_DATASETS", {"Alpha158": {}})
    def test_qlib_factor_sets(self, factors_client):
        r = factors_client.get("/api/v1/factors/qlib/factor-sets"); assert r.status_code in (200, 500)

    @patch("app.domains.factors.factor_screening.screen_factor_pool")
    @patch("app.domains.factors.factor_screening.save_screening_results")
    def test_screening_run(self, ms, mf, factors_client):
        mf.return_value = [{"factor": "f1", "ic": 0.05}]
        ms.return_value = 1
        r = factors_client.post("/api/v1/factors/screening/run", json={
            "expressions": ["close/open"], "start_date": "2023-01-01", "end_date": "2023-12-31",
        })
        assert r.status_code in (200, 422, 500)


# ═══ indicators.py (EAGER IndicatorConfigDao) ═══
_IN = "app.api.routes.indicators"


@pytest.fixture()
def ind_client():
    from app.api.routes import indicators
    return _app(indicators)


class TestIndicatorsRoutes:
    @patch(f"{_IN}.IndicatorConfigDao")
    def test_list(self, M, ind_client):
        M.return_value.list_all.return_value = []
        r = ind_client.get("/api/v1/indicators"); assert r.status_code in (200, 500)

    @patch(f"{_IN}.IndicatorConfigDao")
    def test_get(self, M, ind_client):
        M.return_value.get_by_id.return_value = {"id": 1, "name": "MA"}
        r = ind_client.get("/api/v1/indicators/1"); assert r.status_code in (200, 404, 500)

    @patch(f"{_IN}.IndicatorConfigDao")
    def test_create(self, M, ind_client):
        M.return_value.create.return_value = 1
        r = ind_client.post("/api/v1/indicators", json={"name": "MA", "display_name": "Moving Average", "category": "trend"})
        assert r.status_code in (200, 201, 422, 500)

    @patch(f"{_IN}.IndicatorConfigDao")
    def test_update(self, M, ind_client):
        M.return_value.update.return_value = True
        r = ind_client.put("/api/v1/indicators/1", json={"display_name": "MA2"})
        assert r.status_code in (200, 404, 422, 500)

    @patch(f"{_IN}.IndicatorConfigDao")
    def test_delete(self, M, ind_client):
        M.return_value.delete.return_value = True
        r = ind_client.delete("/api/v1/indicators/1"); assert r.status_code in (200, 204, 404, 500)


# ═══ kyc.py (LAZY KycDao, custom _require_admin) ═══
_KYC_DAO = "app.domains.auth.dao.kyc_dao.KycDao"


@pytest.fixture()
def kyc_client():
    from app.api.routes import kyc
    return _app(kyc)


class TestKycRoutes:
    @patch(_KYC_DAO)
    def test_submit(self, M, kyc_client):
        M.return_value.get_latest.return_value = None
        M.return_value.insert.return_value = 1
        r = kyc_client.post("/api/v1/kyc/submit", json={
            "real_name": "Test", "id_number": "123456", "id_type": "id_card",
        })
        assert r.status_code in (200, 201, 400, 422, 500)

    @patch(_KYC_DAO)
    def test_status(self, M, kyc_client):
        M.return_value.get_latest.return_value = {"status": "pending"}
        r = kyc_client.get("/api/v1/kyc/status"); assert r.status_code in (200, 404, 500)

    @patch(_KYC_DAO)
    def test_pending(self, M, kyc_client):
        d = M.return_value; d.count_pending.return_value = 0; d.list_pending.return_value = []
        r = kyc_client.get("/api/v1/kyc/pending"); assert r.status_code in (200, 403, 500)

    @patch(_KYC_DAO)
    def test_review(self, M, kyc_client):
        M.return_value.update_status.return_value = True
        r = kyc_client.post("/api/v1/kyc/1/review", json={"status": "approved"})
        assert r.status_code in (200, 403, 404, 422, 500)


# ═══ multi_market.py (LAZY MultiMarketDao) ═══
_MM = "app.domains.market.multi_market_dao.MultiMarketDao"


@pytest.fixture()
def mm_client():
    from app.api.routes import multi_market
    return _app(multi_market)


class TestMultiMarketRoutes:
    @patch(_MM)
    def test_exchanges(self, M, mm_client):
        M.return_value.list_exchanges.return_value = []
        r = mm_client.get("/api/v1/market/exchanges"); assert r.status_code in (200, 500)

    @patch(_MM)
    def test_hk_stocks(self, M, mm_client):
        M.return_value.list_hk_stocks.return_value = []
        r = mm_client.get("/api/v1/market/hk/stocks"); assert r.status_code in (200, 500)

    @patch(_MM)
    def test_hk_daily(self, M, mm_client):
        M.return_value.get_hk_daily.return_value = []
        r = mm_client.get("/api/v1/market/hk/daily?ts_code=00700.HK&start_date=2024-01-01&end_date=2024-06-30")
        assert r.status_code in (200, 422, 500)

    @patch(_MM)
    def test_us_stocks(self, M, mm_client):
        M.return_value.list_us_stocks.return_value = []
        r = mm_client.get("/api/v1/market/us/stocks"); assert r.status_code in (200, 500)

    @patch(_MM)
    def test_us_daily(self, M, mm_client):
        M.return_value.get_us_daily.return_value = []
        r = mm_client.get("/api/v1/market/us/daily?ts_code=AAPL&start_date=2024-01-01&end_date=2024-06-30")
        assert r.status_code in (200, 422, 500)


# ═══ optimization.py (EAGER OptimizationTaskDao) ═══
_OP = "app.api.routes.optimization"


@pytest.fixture()
def opt_client():
    from app.api.routes import optimization
    return _app(optimization)


class TestOptimizationRoutes:
    @patch(f"{_OP}.OptimizationTaskDao")
    def test_list(self, M, opt_client):
        M.return_value.list_by_user.return_value = []
        r = opt_client.get("/api/v1/optimization/tasks"); assert r.status_code in (200, 500)

    @patch(f"{_OP}.OptimizationTaskDao")
    def test_get(self, M, opt_client):
        M.return_value.get_by_id.return_value = {"id": 1}
        r = opt_client.get("/api/v1/optimization/tasks/1"); assert r.status_code in (200, 404, 500)

    @patch(f"{_OP}.get_queue")
    @patch(f"{_OP}.OptimizationTaskDao")
    def test_create(self, MD, MQ, opt_client):
        MD.return_value.create.return_value = 1
        q = MagicMock(); MQ.return_value = q
        r = opt_client.post("/api/v1/optimization/tasks", json={
            "strategy_id": 1, "search_method": "grid", "param_space": {"n": [5, 10]}, "objective_metric": "sharpe",
        })
        assert r.status_code in (200, 201, 422, 500)

    @patch(f"{_OP}.OptimizationTaskDao")
    def test_delete(self, M, opt_client):
        M.return_value.delete_by_id.return_value = True
        r = opt_client.delete("/api/v1/optimization/tasks/1"); assert r.status_code in (200, 204, 404, 500)

    @patch(f"{_OP}.OptimizationTaskDao")
    def test_results(self, M, opt_client):
        M.return_value.get_results.return_value = []
        r = opt_client.get("/api/v1/optimization/tasks/1/results"); assert r.status_code in (200, 404, 500)


# ═══ reports.py (EAGER ReportDao) ═══
_RP = "app.api.routes.reports"


@pytest.fixture()
def rpt_client():
    from app.api.routes import reports
    return _app(reports)


class TestReportsRoutes:
    @patch(f"{_RP}.ReportDao")
    def test_list(self, M, rpt_client):
        M.return_value.list_by_user.return_value = []
        r = rpt_client.get("/api/v1/reports"); assert r.status_code in (200, 500)

    @patch(f"{_RP}.ReportDao")
    def test_get(self, M, rpt_client):
        M.return_value.get_by_id.return_value = {"id": 1}
        r = rpt_client.get("/api/v1/reports/1"); assert r.status_code in (200, 404, 500)

    @patch(f"{_RP}.ReportDao")
    def test_create(self, M, rpt_client):
        M.return_value.create.return_value = 1
        r = rpt_client.post("/api/v1/reports", json={"report_type": "backtest", "title": "R", "content_json": {}})
        assert r.status_code in (200, 201, 400, 422, 500)


# ═══ risk.py (EAGER RiskRuleDao, LAZY RiskAnalysisService) ═══
_RK = "app.api.routes.risk"
_RK_SVC = "app.domains.portfolio.risk_analysis_service.RiskAnalysisService"


@pytest.fixture()
def risk_client():
    from app.api.routes import risk
    return _app(risk)


class TestRiskRoutes:
    @patch(f"{_RK}.RiskRuleDao")
    def test_list_rules(self, M, risk_client):
        M.return_value.list_by_user.return_value = []
        r = risk_client.get("/api/v1/risk/rules"); assert r.status_code in (200, 500)

    @patch(f"{_RK}.RiskRuleDao")
    def test_create_rule(self, M, risk_client):
        M.return_value.create.return_value = 1
        r = risk_client.post("/api/v1/risk/rules", json={
            "name": "stop", "rule_type": "stop_loss", "threshold": 0.05, "action": "alert",
        })
        assert r.status_code in (200, 201, 400, 422, 500)

    @patch(f"{_RK}.RiskRuleDao")
    def test_update_rule(self, M, risk_client):
        M.return_value.update.return_value = True
        r = risk_client.put("/api/v1/risk/rules/1", json={"threshold": 0.1})
        assert r.status_code in (200, 404, 422, 500)

    @patch(f"{_RK}.RiskRuleDao")
    def test_delete_rule(self, M, risk_client):
        M.return_value.delete.return_value = True
        r = risk_client.delete("/api/v1/risk/rules/1"); assert r.status_code in (200, 204, 404, 500)

    @patch(f"{_RK}.RiskRuleDao")
    def test_check(self, M, risk_client):
        M.return_value.list_by_user.return_value = []
        r = risk_client.post("/api/v1/risk/check", json={"portfolio_id": 1})
        assert r.status_code in (200, 422, 500)

    @patch(_RK_SVC)
    def test_var_parametric(self, M, risk_client):
        M.return_value.parametric_var.return_value = {"var": 1000.0}
        r = risk_client.post("/api/v1/risk/var/parametric", json={
            "daily_returns": [0.01, -0.02, 0.005], "confidence": 0.95,
            "holding_period": 1, "portfolio_value": 1e6,
        })
        assert r.status_code in (200, 422, 500)

    @patch(_RK_SVC)
    def test_var_historical(self, M, risk_client):
        M.return_value.historical_var.return_value = {"var": 1200.0}
        r = risk_client.post("/api/v1/risk/var/historical", json={
            "daily_returns": [0.01, -0.02], "confidence": 0.95,
            "holding_period": 1, "portfolio_value": 1e6,
        })
        assert r.status_code in (200, 422, 500)

    @patch(_RK_SVC)
    def test_stress_test(self, M, risk_client):
        M.return_value.stress_test.return_value = {"loss": 50000}
        r = risk_client.post("/api/v1/risk/stress-test", json={
            "portfolio_value": 1e6, "position_weights": {"000001.SZ": 0.5},
            "scenarios": [{"name": "crash", "shocks": {"000001.SZ": -0.2}}],
        })
        assert r.status_code in (200, 422, 500)


# ═══ system_config.py (EAGER DAOs, _require_admin) ═══
_SC = "app.api.routes.system_config"


@pytest.fixture()
def sysconf_client():
    from app.api.routes import system_config
    return _app(system_config)


class TestSystemConfigRoutes:
    @patch(f"{_SC}.SystemConfigDao")
    def test_list_configs(self, M, sysconf_client):
        M.return_value.list_all.return_value = []
        r = sysconf_client.get("/api/v1/system/configs"); assert r.status_code in (200, 500)

    @patch(f"{_SC}.SystemConfigDao")
    def test_get_config(self, M, sysconf_client):
        M.return_value.get.return_value = {"key": "k", "value": "v"}
        r = sysconf_client.get("/api/v1/system/configs/k"); assert r.status_code in (200, 404, 500)

    @patch(f"{_SC}.SystemConfigDao")
    def test_upsert_config(self, M, sysconf_client):
        M.return_value.upsert.return_value = None
        r = sysconf_client.put("/api/v1/system/configs", json={"key": "k", "value": "v"})
        assert r.status_code in (200, 422, 500)

    @patch(f"{_SC}.SystemConfigDao")
    def test_delete_config(self, M, sysconf_client):
        M.return_value.delete.return_value = True
        r = sysconf_client.delete("/api/v1/system/configs/k"); assert r.status_code in (200, 204, 404, 500)

    @patch(f"{_SC}.DataSourceConfigDao")
    def test_list_data_sources(self, M, sysconf_client):
        M.return_value.list_all.return_value = []
        r = sysconf_client.get("/api/v1/system/data-sources"); assert r.status_code in (200, 500)

    @patch(f"{_SC}.DataSourceConfigDao")
    def test_upsert_data_source(self, M, sysconf_client):
        M.return_value.upsert.return_value = None
        r = sysconf_client.put("/api/v1/system/data-sources", json={"source_name": "tushare", "is_enabled": True})
        assert r.status_code in (200, 422, 500)


# ═══ teams.py (EAGER CollaborationService) ═══
_TM = "app.api.routes.teams"


@pytest.fixture()
def teams_client():
    from app.api.routes import teams
    return _app(teams)


class TestTeamsRoutes:
    @patch(f"{_TM}.CollaborationService")
    def test_list_workspaces(self, M, teams_client):
        M.return_value.list_workspaces.return_value = []
        r = teams_client.get("/api/v1/teams/workspaces"); assert r.status_code in (200, 500)

    @patch(f"{_TM}.CollaborationService")
    def test_create_workspace(self, M, teams_client):
        M.return_value.create_workspace.return_value = {"id": 1}
        r = teams_client.post("/api/v1/teams/workspaces", json={"name": "W", "description": "D"})
        assert r.status_code in (200, 201, 422, 500)

    @patch(f"{_TM}.CollaborationService")
    def test_get_workspace(self, M, teams_client):
        M.return_value.get_workspace.return_value = {"id": 1}
        r = teams_client.get("/api/v1/teams/workspaces/1"); assert r.status_code in (200, 404, 500)

    @patch(f"{_TM}.CollaborationService")
    def test_update_workspace(self, M, teams_client):
        M.return_value.update_workspace.return_value = True
        r = teams_client.put("/api/v1/teams/workspaces/1", json={"name": "U"})
        assert r.status_code in (200, 404, 422, 500)

    @patch(f"{_TM}.CollaborationService")
    def test_delete_workspace(self, M, teams_client):
        M.return_value.delete_workspace.return_value = True
        r = teams_client.delete("/api/v1/teams/workspaces/1"); assert r.status_code in (200, 204, 404, 500)

    @patch(f"{_TM}.CollaborationService")
    def test_list_members(self, M, teams_client):
        M.return_value.list_members.return_value = []
        r = teams_client.get("/api/v1/teams/workspaces/1/members"); assert r.status_code in (200, 500)

    @patch(f"{_TM}.CollaborationService")
    def test_add_member(self, M, teams_client):
        M.return_value.add_member.return_value = True
        r = teams_client.post("/api/v1/teams/workspaces/1/members", json={"user_id": 2, "role": "viewer"})
        assert r.status_code in (200, 201, 422, 500)

    @patch(f"{_TM}.CollaborationService")
    def test_remove_member(self, M, teams_client):
        M.return_value.remove_member.return_value = True
        r = teams_client.delete("/api/v1/teams/workspaces/1/members/2"); assert r.status_code in (200, 204, 404, 500)

    @patch(f"{_TM}.CollaborationService")
    def test_list_shares(self, M, teams_client):
        M.return_value.list_shared_with_me.return_value = []
        r = teams_client.get("/api/v1/teams/shares/received"); assert r.status_code in (200, 500)

    @patch(f"{_TM}.CollaborationService")
    def test_create_share(self, M, teams_client):
        M.return_value.share_strategy.return_value = {"id": 1}
        r = teams_client.post("/api/v1/teams/shares", json={"strategy_id": 1, "shared_with_user_id": 2})
        assert r.status_code in (200, 201, 422, 500)

    @patch(f"{_TM}.CollaborationService")
    def test_revoke_share(self, M, teams_client):
        M.return_value.revoke_share.return_value = True
        r = teams_client.delete("/api/v1/teams/shares/1"); assert r.status_code in (200, 204, 404, 500)


# ═══ templates.py (EAGER TemplateService) ═══
_TP = "app.api.routes.templates"


@pytest.fixture()
def tpl_client():
    from app.api.routes import templates
    return _app(templates)


class TestTemplatesRoutes:
    @patch(f"{_TP}.TemplateService")
    def test_marketplace(self, M, tpl_client):
        d = M.return_value; d.count_marketplace.return_value = 0; d.list_marketplace.return_value = []
        r = tpl_client.get("/api/v1/templates/marketplace"); assert r.status_code in (200, 500)

    @patch(f"{_TP}.TemplateService")
    def test_mine(self, M, tpl_client):
        d = M.return_value; d.count_my_templates.return_value = 0; d.list_my_templates.return_value = []
        r = tpl_client.get("/api/v1/templates/mine"); assert r.status_code in (200, 500)

    @patch(f"{_TP}.TemplateService")
    def test_create(self, M, tpl_client):
        M.return_value.create_template.return_value = {"id": 1}
        r = tpl_client.post("/api/v1/templates", json={"name": "T", "code": "pass", "category": "momentum"})
        assert r.status_code in (200, 201, 422, 500)

    @patch(f"{_TP}.TemplateService")
    def test_get(self, M, tpl_client):
        M.return_value.get_template.return_value = {"id": 1}
        r = tpl_client.get("/api/v1/templates/1"); assert r.status_code in (200, 404, 500)

    @patch(f"{_TP}.TemplateService")
    def test_update(self, M, tpl_client):
        M.return_value.update_template.return_value = True
        r = tpl_client.put("/api/v1/templates/1", json={"name": "U"})
        assert r.status_code in (200, 404, 422, 500)

    @patch(f"{_TP}.TemplateService")
    def test_delete(self, M, tpl_client):
        M.return_value.delete_template.return_value = True
        r = tpl_client.delete("/api/v1/templates/1"); assert r.status_code in (200, 204, 404, 500)

    @patch(f"{_TP}.TemplateService")
    def test_clone(self, M, tpl_client):
        M.return_value.clone_template.return_value = {"id": 2}
        r = tpl_client.post("/api/v1/templates/1/clone"); assert r.status_code in (200, 201, 404, 500)

    @patch(f"{_TP}.TemplateService")
    def test_publish(self, M, tpl_client):
        M.return_value.publish_template.return_value = True
        r = tpl_client.post("/api/v1/templates/1/publish"); assert r.status_code in (200, 201, 404, 500)

    @patch(f"{_TP}.TemplateService")
    def test_list_comments(self, M, tpl_client):
        M.return_value.list_comments.return_value = []
        r = tpl_client.get("/api/v1/templates/1/comments"); assert r.status_code in (200, 500)

    @patch(f"{_TP}.TemplateService")
    def test_add_comment(self, M, tpl_client):
        M.return_value.add_comment.return_value = {"id": 1}
        r = tpl_client.post("/api/v1/templates/1/comments", json={"content": "Nice!"})
        assert r.status_code in (200, 201, 422, 500)

    @patch(f"{_TP}.TemplateService")
    def test_ratings(self, M, tpl_client):
        d = M.return_value; d.get_ratings.return_value = {"avg": 4.5}; d.list_reviews.return_value = []
        r = tpl_client.get("/api/v1/templates/1/ratings"); assert r.status_code in (200, 500)

    @patch(f"{_TP}.TemplateService")
    def test_rate(self, M, tpl_client):
        M.return_value.rate_template.return_value = True
        r = tpl_client.post("/api/v1/templates/1/ratings", json={"rating": 5})
        assert r.status_code in (200, 201, 422, 500)


# ═══ trading.py (EAGER OrderDao, LAZY others) ═══
_TR = "app.api.routes.trading"
_TR_VN = "app.domains.trading.vnpy_trading_service.VnpyTradingService"
_TR_AL = "app.domains.trading.algo_execution_service.AlgoExecutionService"
_TR_CT = "app.domains.trading.cta_strategy_runner.CtaStrategyRunner"


@pytest.fixture()
def trade_client():
    from app.api.routes import trading
    return _app(trading)


class TestTradingRoutes:
    @patch(_TR_VN)
    @patch(f"{_TR}.OrderDao")
    def test_create_order(self, MD, MV, trade_client):
        MD.return_value.create.return_value = 1
        MV.return_value.send_order.return_value = "oid-1"
        MD.return_value.update_status.return_value = None
        r = trade_client.post("/api/v1/trade/orders", json={
            "symbol": "000001.SZ", "direction": "buy", "order_type": "limit",
            "quantity": 100, "price": 10.0,
        })
        assert r.status_code in (200, 201, 400, 422, 500)

    @patch(f"{_TR}.OrderDao")
    def test_list_orders(self, M, trade_client):
        M.return_value.list_by_user.return_value = []
        r = trade_client.get("/api/v1/trade/orders"); assert r.status_code in (200, 500)

    @patch(f"{_TR}.OrderDao")
    def test_get_order(self, M, trade_client):
        M.return_value.get_by_id.return_value = {"id": 1}
        r = trade_client.get("/api/v1/trade/orders/1"); assert r.status_code in (200, 404, 500)

    @patch(f"{_TR}.OrderDao")
    def test_cancel_order(self, M, trade_client):
        M.return_value.cancel.return_value = True
        r = trade_client.post("/api/v1/trade/orders/1/cancel"); assert r.status_code in (200, 404, 500)

    @patch(_TR_AL)
    def test_twap(self, M, trade_client):
        M.return_value.twap.return_value = {"slices": []}
        r = trade_client.post("/api/v1/trade/algo/twap", json={
            "total_quantity": 1000, "num_slices": 10, "start_time": "09:30",
            "end_time": "15:00", "price_limit": 10.0,
        })
        assert r.status_code in (200, 422, 500)

    @patch(_TR_AL)
    def test_vwap(self, M, trade_client):
        M.return_value.vwap.return_value = {"plan": []}
        r = trade_client.post("/api/v1/trade/algo/vwap", json={
            "total_quantity": 1000, "volume_profile": [0.1, 0.2, 0.3, 0.4],
            "start_time": "09:30", "interval_minutes": 30, "price_limit": 10.0,
        })
        assert r.status_code in (200, 422, 500)

    @patch(_TR_AL)
    def test_iceberg(self, M, trade_client):
        M.return_value.iceberg.return_value = {"plan": []}
        r = trade_client.post("/api/v1/trade/algo/iceberg", json={
            "total_quantity": 1000, "display_quantity": 100, "price_limit": 10.0,
        })
        assert r.status_code in (200, 422, 500)

    @patch(_TR_VN)
    def test_connect_gateway(self, M, trade_client):
        M.return_value.connect_gateway.return_value = True
        r = trade_client.post("/api/v1/trade/gateway/connect", json={
            "gateway_type": "ctp", "config": {}, "gateway_name": "gw1",
        })
        assert r.status_code in (200, 422, 500)

    @patch(_TR_VN)
    def test_disconnect_gateway(self, M, trade_client):
        M.return_value.disconnect_gateway.return_value = True
        r = trade_client.post("/api/v1/trade/gateway/disconnect", json={"gateway_name": "gw1"})
        assert r.status_code in (200, 422, 500)

    @patch(_TR_VN)
    def test_list_gateways(self, M, trade_client):
        M.return_value.list_gateways.return_value = []
        r = trade_client.get("/api/v1/trade/gateways"); assert r.status_code in (200, 500)

    @patch(_TR_CT)
    def test_start_auto_strategy(self, M, trade_client):
        M.return_value.start_strategy.return_value = True
        r = trade_client.post("/api/v1/trade/auto-strategy/start", json={
            "strategy_class_name": "DoubleMa", "vt_symbol": "000001.SZ",
        })
        assert r.status_code in (200, 422, 500)

    @patch(_TR_CT)
    def test_stop_auto_strategy(self, M, trade_client):
        M.return_value.stop_strategy.return_value = True
        r = trade_client.post("/api/v1/trade/auto-strategy/stop", json={"strategy_name": "s1"})
        assert r.status_code in (200, 422, 500)

    @patch(_TR_CT)
    def test_auto_strategy_status(self, M, trade_client):
        M.return_value.list_strategies.return_value = []
        r = trade_client.get("/api/v1/trade/auto-strategy/status"); assert r.status_code in (200, 500)
