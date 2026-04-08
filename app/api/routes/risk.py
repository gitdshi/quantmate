"""Risk rule routes (P2 Issue: Risk Budget, Pre-trade Risk Check)."""

from datetime import datetime
from typing import Optional

from fastapi import APIRouter, Depends, status
from pydantic import BaseModel

from app.api.dependencies.permissions import require_permission
from app.api.services.auth_service import get_current_user
from app.api.models.user import TokenData
from app.api.errors import ErrorCode
from app.api.exception_handlers import APIError
from app.domains.trading.dao.risk_rule_dao import RiskRuleDao

router = APIRouter(prefix="/risk", tags=["Risk Management"])


class RiskRuleCreateRequest(BaseModel):
    name: str
    rule_type: str
    threshold: float
    action: str = "warn"
    condition_expr: Optional[str] = None


class RiskRuleUpdateRequest(BaseModel):
    name: Optional[str] = None
    rule_type: Optional[str] = None
    threshold: Optional[float] = None
    action: Optional[str] = None
    condition_expr: Optional[str] = None
    is_active: Optional[bool] = None


class RiskCheckRequest(BaseModel):
    scope_type: Optional[str] = None
    scope_id: Optional[int] = None
    strategy_id: Optional[int] = None
    version_id: Optional[int] = None
    projected_action: Optional[str] = None
    symbol: Optional[str] = None
    direction: Optional[str] = None
    quantity: Optional[int] = None


@router.get("/rules", dependencies=[require_permission("portfolios", "read")])
async def list_risk_rules(current_user: TokenData = Depends(get_current_user)):
    """List all risk rules for the current user."""
    dao = RiskRuleDao()
    rules = dao.list_by_user(current_user.user_id)
    return {"rules": rules}


@router.post("/rules", status_code=status.HTTP_201_CREATED, dependencies=[require_permission("portfolios", "write")])
async def create_risk_rule(req: RiskRuleCreateRequest, current_user: TokenData = Depends(get_current_user)):
    """Create a new risk rule."""
    valid_types = ("position_limit", "drawdown", "concentration", "frequency", "custom")
    if req.rule_type not in valid_types:
        raise APIError(
            status_code=400,
            code=ErrorCode.VALIDATION_ERROR,
            message=f"Invalid rule_type. Must be one of: {', '.join(valid_types)}",
        )
    if req.action not in ("block", "reduce", "warn"):
        raise APIError(status_code=400, code=ErrorCode.VALIDATION_ERROR, message="Invalid action")

    dao = RiskRuleDao()
    rule_id = dao.create(
        user_id=current_user.user_id,
        name=req.name,
        rule_type=req.rule_type,
        threshold=req.threshold,
        action=req.action,
        condition_expr=req.condition_expr,
    )
    return {"id": rule_id, "message": "Risk rule created"}


@router.put("/rules/{rule_id}", dependencies=[require_permission("portfolios", "write")])
async def update_risk_rule(
    rule_id: int, req: RiskRuleUpdateRequest, current_user: TokenData = Depends(get_current_user)
):
    """Update a risk rule."""
    dao = RiskRuleDao()
    updates = {k: v for k, v in req.model_dump().items() if v is not None}
    if not updates:
        raise APIError(status_code=400, code=ErrorCode.VALIDATION_ERROR, message="No fields to update")
    if not dao.update(rule_id, current_user.user_id, **updates):
        raise APIError(status_code=404, code=ErrorCode.NOT_FOUND, message="Risk rule not found")
    return {"message": "Risk rule updated"}


@router.delete("/rules/{rule_id}", dependencies=[require_permission("portfolios", "write")])
async def delete_risk_rule(rule_id: int, current_user: TokenData = Depends(get_current_user)):
    """Delete a risk rule."""
    dao = RiskRuleDao()
    if not dao.delete(rule_id, current_user.user_id):
        raise APIError(status_code=404, code=ErrorCode.NOT_FOUND, message="Risk rule not found")
    return {"message": "Risk rule deleted"}


@router.post("/check", dependencies=[require_permission("portfolios", "write")])
async def pre_trade_risk_check(
    body: Optional[RiskCheckRequest] = None,
    symbol: Optional[str] = None,
    direction: Optional[str] = None,
    quantity: Optional[int] = None,
    current_user: TokenData = Depends(get_current_user),
):
    """Run pre-trade / pre-deployment risk checks.

    Supports both the legacy query-param form and the new structured request body.
    Returns a unified `pass|warn|block` result envelope for deployment workflows.
    """
    payload = body or RiskCheckRequest()
    effective_symbol = payload.symbol or symbol
    effective_direction = payload.direction or direction
    effective_quantity = payload.quantity if payload.quantity is not None else quantity

    dao = RiskRuleDao()
    active_rules = dao.list_by_user(current_user.user_id, active_only=True)

    triggered_rules = []
    overall = "pass"

    for rule in active_rules:
        threshold = float(rule.get("threshold") or 0)
        rule_result = "pass"
        message = "Rule passed"

        if rule.get("rule_type") == "position_limit" and effective_quantity is not None and threshold > 0 and effective_quantity > threshold:
            rule_result = "block" if rule.get("action") == "block" else "warn"
            message = f"Projected quantity {effective_quantity} exceeds threshold {threshold:g}"
        elif rule.get("rule_type") == "frequency" and payload.projected_action == "prepare_live_upgrade":
            rule_result = "warn"
            message = "Live upgrade preparation requires manual confirmation"

        if rule_result != "pass":
            triggered_rules.append(
                {
                    "rule_id": rule["id"],
                    "rule_name": rule["name"],
                    "rule_type": rule["rule_type"],
                    "severity": rule_result,
                    "message": message,
                }
            )

        if rule_result == "block":
            overall = "block"
        elif rule_result == "warn" and overall != "block":
            overall = "warn"

    summary = {
        "pass": "No blocking risk rules triggered",
        "warn": "Risk check completed with warnings",
        "block": "Risk check blocked by active rules",
    }[overall]

    # Audit: log risk check for pre-live upgrade scenarios
    if payload.projected_action == "prepare_live_upgrade":
        from app.domains.audit.service import get_audit_service
        audit_svc = get_audit_service()
        audit_svc.log_risk_check(
            user_id=current_user.user_id,
            username=current_user.username or "",
            result=overall,
            strategy_id=payload.strategy_id,
            version_id=payload.version_id,
            deployment_id=payload.scope_id,
            triggered_rules=triggered_rules,
        )

    return {
        "result": overall,
        "overall": overall,
        "summary": summary,
        "triggered_rules": triggered_rules,
        "checked_at": datetime.utcnow().isoformat() + "Z",
        "context": {
            "scope_type": payload.scope_type,
            "scope_id": payload.scope_id,
            "strategy_id": payload.strategy_id,
            "version_id": payload.version_id,
            "projected_action": payload.projected_action,
            "symbol": effective_symbol,
            "direction": effective_direction,
            "quantity": effective_quantity,
        },
        "checks": triggered_rules,
    }


# ── VaR & Stress Testing endpoints ───────────────────────────────────


class VaRRequest(BaseModel):
    daily_returns: list[float]
    confidence: float = 0.95
    holding_period: int = 1
    portfolio_value: float = 1_000_000


class StressTestRequest(BaseModel):
    portfolio_value: float = 1_000_000
    position_weights: dict[str, float]
    scenarios: list[dict] | None = None


@router.post("/var/parametric", dependencies=[require_permission("reports", "read")])
async def compute_parametric_var(req: VaRRequest, current_user: TokenData = Depends(get_current_user)):
    """Compute parametric (Gaussian) Value-at-Risk."""
    from app.domains.portfolio.risk_analysis_service import RiskAnalysisService

    svc = RiskAnalysisService()
    return svc.parametric_var(req.daily_returns, req.confidence, req.holding_period, req.portfolio_value)


@router.post("/var/historical", dependencies=[require_permission("reports", "read")])
async def compute_historical_var(req: VaRRequest, current_user: TokenData = Depends(get_current_user)):
    """Compute historical Value-at-Risk and CVaR."""
    from app.domains.portfolio.risk_analysis_service import RiskAnalysisService

    svc = RiskAnalysisService()
    return svc.historical_var(req.daily_returns, req.confidence, req.holding_period, req.portfolio_value)


@router.post("/stress-test", dependencies=[require_permission("reports", "read")])
async def run_stress_test(req: StressTestRequest, current_user: TokenData = Depends(get_current_user)):
    """Run stress test scenarios against portfolio weights."""
    from app.domains.portfolio.risk_analysis_service import RiskAnalysisService

    svc = RiskAnalysisService()
    return {"scenarios": svc.stress_test(req.portfolio_value, req.position_weights, req.scenarios)}
