"""RD-Agent Auto Pilot routes — autonomous factor mining via RD-Agent."""

from fastapi import APIRouter, Depends, status
from pydantic import BaseModel, Field

from app.api.dependencies.permissions import require_permission
from app.api.services.auth_service import get_current_user
from app.api.models.user import TokenData
from app.api.errors import ErrorCode
from app.api.exception_handlers import APIError
from app.domains.factors.rdagent_service import RDAgentService, RDAgentMiningConfig

router = APIRouter(prefix="/rdagent", tags=["RD-Agent Auto Pilot"])


# ── Request / Response models ────────────────────────────────────────


class MiningStartRequest(BaseModel):
    scenario: str = Field("fin_factor", pattern="^(fin_factor|fin_model|fin_quant)$")
    max_iterations: int = Field(10, ge=1, le=100)
    llm_model: str = "gpt-4o-mini"
    universe: str = "csi300"
    feature_columns: list[str] = Field(default_factory=list)
    start_date: str = "2018-01-01"
    end_date: str = "2024-12-31"


class ImportFactorRequest(BaseModel):
    discovered_factor_id: int


# ── Routes ───────────────────────────────────────────────────────────


@router.post("/runs", status_code=status.HTTP_201_CREATED, dependencies=[require_permission("strategies", "write")])
async def start_mining(
    body: MiningStartRequest,
    current_user: TokenData = Depends(get_current_user),
):
    """Start a new RD-Agent autonomous factor mining run."""
    config = RDAgentMiningConfig(
        scenario=body.scenario,
        max_iterations=body.max_iterations,
        llm_model=body.llm_model,
        universe=body.universe,
        feature_columns=body.feature_columns,
        start_date=body.start_date,
        end_date=body.end_date,
    )

    svc = RDAgentService()
    result = svc.start_mining(current_user.user_id, config)

    # Enqueue background task
    from app.worker.service.config import get_queue

    q = get_queue("rdagent")
    from app.worker.service.rdagent_tasks import run_rdagent_mining_task

    q.enqueue(
        run_rdagent_mining_task,
        user_id=current_user.user_id,
        run_id=result["run_id"],
        config_dict=config.to_dict(),
        job_id=f"rdagent-{result['run_id']}",
    )

    return result


@router.get("/runs", dependencies=[require_permission("strategies", "read")])
async def list_runs(
    limit: int = 20,
    offset: int = 0,
    current_user: TokenData = Depends(get_current_user),
):
    """List mining runs for the current user."""
    svc = RDAgentService()
    return svc.list_runs(current_user.user_id, limit=limit, offset=offset)


@router.get("/runs/{run_id}", dependencies=[require_permission("strategies", "read")])
async def get_run(
    run_id: str,
    current_user: TokenData = Depends(get_current_user),
):
    """Get details of a specific mining run."""
    svc = RDAgentService()
    run = svc.get_run(current_user.user_id, run_id)
    if not run:
        raise APIError(
            status_code=404,
            code=ErrorCode.NOT_FOUND,
            message="Mining run not found",
        )
    return run


@router.delete("/runs/{run_id}", dependencies=[require_permission("strategies", "write")])
async def cancel_run(
    run_id: str,
    current_user: TokenData = Depends(get_current_user),
):
    """Cancel a queued or running mining run."""
    svc = RDAgentService()
    try:
        return svc.cancel_run(current_user.user_id, run_id)
    except KeyError:
        raise APIError(
            status_code=404,
            code=ErrorCode.NOT_FOUND,
            message="Mining run not found",
        )
    except ValueError as e:
        raise APIError(
            status_code=400,
            code=ErrorCode.VALIDATION_ERROR,
            message=str(e),
        )


@router.get("/runs/{run_id}/iterations", dependencies=[require_permission("strategies", "read")])
async def get_iterations(
    run_id: str,
    current_user: TokenData = Depends(get_current_user),
):
    """Get all iterations for a mining run."""
    svc = RDAgentService()
    try:
        return svc.get_iterations(current_user.user_id, run_id)
    except KeyError:
        raise APIError(
            status_code=404,
            code=ErrorCode.NOT_FOUND,
            message="Mining run not found",
        )


@router.get("/runs/{run_id}/factors", dependencies=[require_permission("strategies", "read")])
async def get_discovered_factors(
    run_id: str,
    current_user: TokenData = Depends(get_current_user),
):
    """Get factors discovered during a mining run."""
    svc = RDAgentService()
    try:
        return svc.get_discovered_factors(current_user.user_id, run_id)
    except KeyError:
        raise APIError(
            status_code=404,
            code=ErrorCode.NOT_FOUND,
            message="Mining run not found",
        )


@router.post("/runs/{run_id}/import", dependencies=[require_permission("strategies", "write")])
async def import_factor(
    run_id: str,
    body: ImportFactorRequest,
    current_user: TokenData = Depends(get_current_user),
):
    """Import a discovered factor into the user's Factor Lab."""
    svc = RDAgentService()
    try:
        return svc.import_factor(
            current_user.user_id, run_id, body.discovered_factor_id
        )
    except KeyError as e:
        raise APIError(
            status_code=404,
            code=ErrorCode.NOT_FOUND,
            message=str(e),
        )


@router.get("/data-catalog", dependencies=[require_permission("strategies", "read")])
async def get_data_catalog(
    current_user: TokenData = Depends(get_current_user),
):
    """Get the available data catalog for factor mining."""
    from app.domains.factors.data_catalog import get_catalog_summary

    return get_catalog_summary()


@router.get("/feature-descriptor", dependencies=[require_permission("strategies", "read")])
async def get_feature_descriptor(
    current_user: TokenData = Depends(get_current_user),
):
    """Get structured feature descriptions for LLM prompting."""
    from app.domains.factors.feature_descriptor import build_feature_descriptor

    return build_feature_descriptor()
