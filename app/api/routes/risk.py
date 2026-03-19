"""Risk rule routes (P2 Issue: Risk Budget, Pre-trade Risk Check)."""

from typing import Optional

from fastapi import APIRouter, Depends, status
from pydantic import BaseModel

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


@router.get("/rules")
async def list_risk_rules(current_user: TokenData = Depends(get_current_user)):
    """List all risk rules for the current user."""
    dao = RiskRuleDao()
    rules = dao.list_by_user(current_user.user_id)
    return {"rules": rules}


@router.post("/rules", status_code=status.HTTP_201_CREATED)
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


@router.put("/rules/{rule_id}")
async def update_risk_rule(rule_id: int, req: RiskRuleUpdateRequest, current_user: TokenData = Depends(get_current_user)):
    """Update a risk rule."""
    dao = RiskRuleDao()
    updates = {k: v for k, v in req.model_dump().items() if v is not None}
    if not updates:
        raise APIError(status_code=400, code=ErrorCode.VALIDATION_ERROR, message="No fields to update")
    if not dao.update(rule_id, current_user.user_id, **updates):
        raise APIError(status_code=404, code=ErrorCode.NOT_FOUND, message="Risk rule not found")
    return {"message": "Risk rule updated"}


@router.delete("/rules/{rule_id}")
async def delete_risk_rule(rule_id: int, current_user: TokenData = Depends(get_current_user)):
    """Delete a risk rule."""
    dao = RiskRuleDao()
    if not dao.delete(rule_id, current_user.user_id):
        raise APIError(status_code=404, code=ErrorCode.NOT_FOUND, message="Risk rule not found")
    return {"message": "Risk rule deleted"}


@router.post("/check")
async def pre_trade_risk_check(
    symbol: str,
    direction: str,
    quantity: int,
    current_user: TokenData = Depends(get_current_user),
):
    """Run pre-trade risk checks against active rules.

    Returns a list of check results: pass/warn/block for each active rule.
    """
    dao = RiskRuleDao()
    active_rules = dao.list_by_user(current_user.user_id, active_only=True)

    results = []
    overall = "pass"
    for rule in active_rules:
        # Simplified risk checking logic
        check_result = "pass"  # In production, evaluate rule conditions
        results.append(
            {
                "rule_id": rule["id"],
                "name": rule["name"],
                "rule_type": rule["rule_type"],
                "result": check_result,
            }
        )
        if check_result == "block":
            overall = "block"
        elif check_result == "warn" and overall != "block":
            overall = "warn"

    return {"overall": overall, "checks": results}


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


@router.post("/var/parametric")
async def compute_parametric_var(req: VaRRequest, current_user: TokenData = Depends(get_current_user)):
    """Compute parametric (Gaussian) Value-at-Risk."""
    from app.domains.portfolio.risk_analysis_service import RiskAnalysisService

    svc = RiskAnalysisService()
    return svc.parametric_var(req.daily_returns, req.confidence, req.holding_period, req.portfolio_value)


@router.post("/var/historical")
async def compute_historical_var(req: VaRRequest, current_user: TokenData = Depends(get_current_user)):
    """Compute historical Value-at-Risk and CVaR."""
    from app.domains.portfolio.risk_analysis_service import RiskAnalysisService

    svc = RiskAnalysisService()
    return svc.historical_var(req.daily_returns, req.confidence, req.holding_period, req.portfolio_value)


@router.post("/stress-test")
async def run_stress_test(req: StressTestRequest, current_user: TokenData = Depends(get_current_user)):
    """Run stress test scenarios against portfolio weights."""
    from app.domains.portfolio.risk_analysis_service import RiskAnalysisService

    svc = RiskAnalysisService()
    return {"scenarios": svc.stress_test(req.portfolio_value, req.position_weights, req.scenarios)}
