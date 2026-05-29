"""Backtest routes."""

from datetime import datetime
import uuid
import json
from fastapi import APIRouter, Depends, BackgroundTasks
from pydantic import BaseModel

from app.api.dependencies.permissions import require_permission
from app.api.models.user import TokenData
from app.api.models.backtest import (
    BacktestRequest,
    BatchBacktestRequest,
    BacktestJob,
    BatchBacktestJob,
    BacktestStatus,
    BacktestRunRequest,
    BacktestRunSubmitResponse,
    BacktestRunListItem,
    BacktestRunDetail,
    BacktestSubjectType,
)
from app.api.models.backtest_ai_report import BacktestAIReport, BacktestAIReportCreateResponse
from app.api.services.auth_service import get_current_user
from app.api.services.backtest_service import BacktestService
from app.worker.service.tasks import save_backtest_to_db
from app.api.services.job_storage_service import get_job_storage
from app.api.errors import ErrorCode
from app.api.exception_handlers import APIError
from app.api.pagination import PaginationParams, paginate

from app.domains.backtests.dao.backtest_history_dao import BacktestHistoryDao
from app.domains.ai.backtest_report_service import BacktestReportService
from app.domains.composite.service import CompositeStrategyService
from app.domains.factors.backtest_task import run_factor_backtest_task
from app.domains.factors.service import FactorService

router = APIRouter(prefix="/backtest", tags=["Backtest"])

# In-memory job store (replace with Redis in production)
_jobs: dict[str, BacktestJob] = {}
_batch_jobs: dict[str, BatchBacktestJob] = {}


class BacktestSubmitResponse(BaseModel):
    """Response after submitting a backtest."""

    job_id: str
    status: BacktestStatus
    message: str


def _json_or_empty(value):
    if value is None:
        return {}
    if isinstance(value, dict):
        return value
    try:
        parsed = json.loads(value) if isinstance(value, str) else value
    except Exception:
        return {}
    return parsed if isinstance(parsed, dict) else {}


def _infer_subject_type(row: dict) -> BacktestSubjectType | None:
    raw = row.get("subject_type")
    if raw:
        try:
            return BacktestSubjectType(raw)
        except ValueError:
            return None
    if row.get("strategy_id") or row.get("strategy_class") or row.get("vt_symbol"):
        return BacktestSubjectType.STRATEGY
    return None


def _derive_summary(row: dict) -> dict:
    explicit = _json_or_empty(row.get("summary_json"))
    if explicit:
        return explicit

    result_data = _json_or_empty(row.get("result"))
    if isinstance(result_data.get("statistics"), dict):
        return result_data["statistics"]

    keys = [
        "total_return",
        "annual_return",
        "max_drawdown",
        "max_drawdown_percent",
        "sharpe_ratio",
        "alpha",
        "beta",
        "benchmark_return",
        "total_trades",
        "win_rate",
        "profit_factor",
    ]
    return {key: result_data[key] for key in keys if key in result_data and result_data[key] is not None}


def _serialize_run_item(row: dict) -> BacktestRunListItem:
    subject_type = _infer_subject_type(row)
    return BacktestRunListItem(
        id=row.get("id"),
        job_id=row.get("job_id"),
        subject_type=subject_type,
        subject_id=row.get("subject_id") if row.get("subject_id") is not None else row.get("strategy_id"),
        subject_name=row.get("subject_name") or row.get("strategy_class"),
        engine_type=row.get("engine_type") or ("vnpy" if row.get("vt_symbol") else None),
        scope_type=row.get("scope_type") or ("single_symbol" if row.get("vt_symbol") else None),
        status=row.get("status"),
        start_date=str(row.get("start_date")) if row.get("start_date") else None,
        end_date=str(row.get("end_date")) if row.get("end_date") else None,
        summary=_derive_summary(row),
        created_at=row.get("created_at"),
        completed_at=row.get("completed_at"),
    )


def _serialize_run_detail(row: dict) -> BacktestRunDetail:
    item = _serialize_run_item(row)
    request_payload = _json_or_empty(row.get("request_payload"))
    result_payload = _json_or_empty(row.get("result"))
    return BacktestRunDetail(
        **item.model_dump(),
        request=request_payload,
        result=result_payload,
        artifacts=_json_or_empty(row.get("artifacts_json")),
        diagnostics=_json_or_empty(row.get("diagnostics_json")),
        extensions=_json_or_empty(row.get("extensions_json")),
        error=row.get("error"),
    )


def _build_strategy_backtest_request(request: BacktestRunRequest) -> BacktestRequest:
    profile = request.profile or {}
    vt_symbol = str(profile.get("vt_symbol") or "").strip()
    if not vt_symbol:
        raise APIError(status_code=400, code=ErrorCode.BAD_REQUEST, message="strategy profile.vt_symbol is required")

    payload = {
        "strategy_id": request.subject_id or profile.get("strategy_id"),
        "strategy_class": profile.get("strategy_class") or request.subject_name,
        "vt_symbol": vt_symbol,
        "start_date": request.start_date,
        "end_date": request.end_date,
        "parameters": profile.get("parameters") or {},
        "capital": request.initial_capital,
        "benchmark": request.benchmark,
        "period": profile.get("period", "daily"),
    }
    if request.costs.get("commission_rate") is not None:
        payload["rate"] = request.costs.get("commission_rate")
    if request.costs.get("slippage") is not None:
        payload["slippage"] = request.costs.get("slippage")
    if (profile.get("size") or request.costs.get("size")) is not None:
        payload["size"] = profile.get("size") or request.costs.get("size")
    return BacktestRequest(**payload)


@router.post("/runs", response_model=BacktestRunSubmitResponse, dependencies=[require_permission("backtests", "write")])
async def submit_backtest_run(
    request: BacktestRunRequest,
    background_tasks: BackgroundTasks,
    current_user: TokenData = Depends(get_current_user),
):
    """Submit a unified backtest run."""
    if request.subject_type == BacktestSubjectType.FACTOR:
        profile = request.profile or {}
        factor_id = request.subject_id or profile.get("factor_id")
        factor = None
        if factor_id is not None:
            try:
                factor = FactorService().get_factor(current_user.user_id, int(factor_id))
            except KeyError:
                raise APIError(status_code=404, code=ErrorCode.NOT_FOUND, message="Factor not found")

        expression = str(profile.get("expression") or (factor or {}).get("expression") or "").strip()
        if not expression:
            raise APIError(
                status_code=400,
                code=ErrorCode.BAD_REQUEST,
                message="factor subject_id or profile.expression is required",
            )

        job_id = str(uuid.uuid4())
        created_at = datetime.utcnow()
        subject_id = int(factor_id) if factor_id is not None else None
        subject_name = request.subject_name or (factor or {}).get("name") or "Custom Factor"
        task_payload = request.model_dump(mode="json")
        task_payload["subject_id"] = subject_id
        task_payload["subject_name"] = subject_name

        dao = BacktestHistoryDao()
        dao.upsert_history(
            user_id=current_user.user_id,
            job_id=job_id,
            strategy_id=None,
            strategy_class=None,
            strategy_version=None,
            source="runs_api",
            vt_symbol="",
            start_date=str(request.start_date),
            end_date=str(request.end_date),
            parameters=profile,
            status=BacktestStatus.PENDING.value,
            result=None,
            error=None,
            created_at=created_at,
            completed_at=None,
            subject_type=request.subject_type.value,
            subject_id=subject_id,
            subject_name=subject_name,
            engine_type="portfolio_daily",
            scope_type="cross_sectional_portfolio",
            request_payload=task_payload,
            result_schema_version=2,
        )

        job = BacktestJob(
            job_id=job_id,
            status=BacktestStatus.PENDING,
            progress=0.0,
            message="Queued for execution",
            created_at=created_at,
        )
        _jobs[job_id] = job
        background_tasks.add_task(run_factor_backtest_task, job_id, current_user.user_id, task_payload)

        return BacktestRunSubmitResponse(
            job_id=job_id,
            status=BacktestStatus.PENDING,
            subject_type=request.subject_type,
            message="Factor backtest run queued successfully",
        )

    if request.subject_type == BacktestSubjectType.COMPOSITE:
        composite_id = request.subject_id or request.profile.get("composite_strategy_id")
        if composite_id is None:
            raise APIError(
                status_code=400,
                code=ErrorCode.BAD_REQUEST,
                message="composite subject_id or profile.composite_strategy_id is required",
            )
        try:
            row = CompositeStrategyService().submit_backtest(
                user_id=current_user.user_id,
                composite_strategy_id=int(composite_id),
                start_date=str(request.start_date),
                end_date=str(request.end_date),
                initial_capital=request.initial_capital,
                benchmark=request.benchmark or "000300.SH",
            )
        except KeyError:
            raise APIError(status_code=404, code=ErrorCode.COMPOSITE_NOT_FOUND, message="Composite strategy not found")
        except ValueError as exc:
            raise APIError(status_code=400, code=ErrorCode.COMPOSITE_VALIDATION_FAILED, message=str(exc))

        return BacktestRunSubmitResponse(
            job_id=row["job_id"],
            status=BacktestStatus.PENDING,
            subject_type=request.subject_type,
            message="Composite backtest run queued successfully",
        )

    if request.subject_type != BacktestSubjectType.STRATEGY:
        raise APIError(
            status_code=501,
            code=ErrorCode.BAD_REQUEST,
            message=f"Unified {request.subject_type.value} backtest submission is not implemented yet",
        )

    strategy_request = _build_strategy_backtest_request(request)
    job_id = str(uuid.uuid4())
    created_at = datetime.utcnow()
    dao = BacktestHistoryDao()
    dao.upsert_history(
        user_id=current_user.user_id,
        job_id=job_id,
        strategy_id=strategy_request.strategy_id,
        strategy_class=strategy_request.strategy_class,
        strategy_version=request.profile.get("strategy_version"),
        source="runs_api",
        vt_symbol=strategy_request.vt_symbol,
        start_date=str(request.start_date),
        end_date=str(request.end_date),
        parameters=strategy_request.parameters,
        status=BacktestStatus.PENDING.value,
        result=None,
        error=None,
        created_at=created_at,
        completed_at=None,
        subject_type=request.subject_type.value,
        subject_id=request.subject_id or strategy_request.strategy_id,
        subject_name=request.subject_name or strategy_request.strategy_class,
        engine_type="vnpy",
        scope_type="single_symbol",
        request_payload=request.model_dump(mode="json"),
        result_schema_version=2,
    )

    job = BacktestJob(
        job_id=job_id,
        status=BacktestStatus.PENDING,
        progress=0.0,
        message="Queued for execution",
        created_at=created_at,
    )
    _jobs[job_id] = job
    background_tasks.add_task(run_backtest_task, job_id, strategy_request, current_user.user_id)

    return BacktestRunSubmitResponse(
        job_id=job_id,
        status=BacktestStatus.PENDING,
        subject_type=request.subject_type,
        message="Backtest run queued successfully",
    )


@router.get("/runs", dependencies=[require_permission("backtests", "read")])
async def list_backtest_runs(
    subject_type: BacktestSubjectType | None = None,
    pagination: PaginationParams = Depends(),
    current_user: TokenData = Depends(get_current_user),
):
    """List unified backtest runs for current user."""
    dao = BacktestHistoryDao()
    total = dao.count_runs_for_user(current_user.user_id, subject_type.value if subject_type else None)
    rows = dao.list_runs_for_user(
        user_id=current_user.user_id,
        limit=pagination.limit,
        offset=pagination.offset,
        subject_type=subject_type.value if subject_type else None,
    )
    return paginate([_serialize_run_item(row).model_dump(mode="json") for row in rows], total, pagination)


@router.get("/runs/{job_id}", response_model=BacktestRunDetail, dependencies=[require_permission("backtests", "read")])
async def get_backtest_run(job_id: str, current_user: TokenData = Depends(get_current_user)):
    """Get unified backtest run detail."""
    dao = BacktestHistoryDao()
    row = dao.get_run_detail_for_user(job_id=job_id, user_id=current_user.user_id)
    if not row:
        raise APIError(status_code=404, code=ErrorCode.BACKTEST_NOT_FOUND, message="Backtest run not found")
    return _serialize_run_detail(row)


@router.post("", response_model=BacktestSubmitResponse, dependencies=[require_permission("backtests", "write")])
async def submit_backtest(
    request: BacktestRequest, background_tasks: BackgroundTasks, current_user: TokenData = Depends(get_current_user)
):
    """Submit a single backtest job."""
    job_id = str(uuid.uuid4())

    job = BacktestJob(
        job_id=job_id,
        status=BacktestStatus.PENDING,
        progress=0.0,
        message="Queued for execution",
        created_at=datetime.utcnow(),
    )
    _jobs[job_id] = job

    # Run backtest in background
    background_tasks.add_task(run_backtest_task, job_id, request, current_user.user_id)

    return BacktestSubmitResponse(job_id=job_id, status=BacktestStatus.PENDING, message="Backtest queued successfully")


@router.post(
    "/batch",
    response_model=BacktestSubmitResponse,
    dependencies=[require_permission("backtests", "write")],
)
async def submit_batch_backtest(
    request: BatchBacktestRequest,
    background_tasks: BackgroundTasks,
    current_user: TokenData = Depends(get_current_user),
):
    """Submit a batch backtest job."""
    job_id = str(uuid.uuid4())

    job = BatchBacktestJob(
        job_id=job_id,
        status=BacktestStatus.PENDING,
        total_symbols=len(request.symbols),
        completed_symbols=0,
        progress=0.0,
        created_at=datetime.utcnow(),
    )
    _batch_jobs[job_id] = job

    background_tasks.add_task(run_batch_backtest_task, job_id, request, current_user.user_id)

    return BacktestSubmitResponse(
        job_id=job_id,
        status=BacktestStatus.PENDING,
        message=f"Batch backtest queued for {len(request.symbols)} symbols",
    )


@router.get("/{job_id}", response_model=BacktestJob, dependencies=[require_permission("backtests", "read")])
async def get_backtest_status(job_id: str, current_user: TokenData = Depends(get_current_user)):
    """Get backtest job status and results."""
    if job_id in _jobs:
        return _jobs[job_id]

    raise APIError(status_code=404, code=ErrorCode.JOB_NOT_FOUND, message="Job not found")


@router.get("/{job_id}/ai-report", response_model=BacktestAIReport, dependencies=[require_permission("backtests", "read")])
async def get_backtest_ai_report(job_id: str, current_user: TokenData = Depends(get_current_user)):
    """Get a stored AI interpretation report for a completed backtest."""
    service = BacktestReportService()
    report = service.get_report(current_user.user_id, job_id)
    if not report:
        raise APIError(status_code=404, code=ErrorCode.NOT_FOUND, message="AI backtest report not found")
    return BacktestAIReport(**report)


@router.post("/{job_id}/ai-report", response_model=BacktestAIReportCreateResponse, dependencies=[require_permission("backtests", "read")])
async def create_backtest_ai_report(job_id: str, current_user: TokenData = Depends(get_current_user)):
    """Generate and store an AI interpretation report for a completed backtest."""
    service = BacktestReportService()
    try:
        service.generate_report(current_user.user_id, job_id)
    except KeyError:
        raise APIError(status_code=404, code=ErrorCode.JOB_NOT_FOUND, message="Backtest job not found")
    except ValueError as exc:
        raise APIError(status_code=400, code=ErrorCode.BAD_REQUEST, message=str(exc))
    return BacktestAIReportCreateResponse(
        job_id=job_id,
        status="completed",
        message="AI backtest report generated",
    )


@router.get(
    "/batch/{job_id}",
    response_model=BatchBacktestJob,
    dependencies=[require_permission("backtests", "read")],
)
async def get_batch_backtest_status(job_id: str, current_user: TokenData = Depends(get_current_user)):
    """Get batch backtest job status and results."""
    if job_id in _batch_jobs:
        return _batch_jobs[job_id]

    raise APIError(status_code=404, code=ErrorCode.JOB_NOT_FOUND, message="Batch job not found")


@router.get("/history/list", dependencies=[require_permission("backtests", "read")])
async def list_backtest_history(
    pagination: PaginationParams = Depends(), current_user: TokenData = Depends(get_current_user)
):
    """List past backtest runs for current user from database."""
    dao = BacktestHistoryDao()
    total = dao.count_for_user(current_user.user_id)
    rows = dao.list_for_user(user_id=current_user.user_id, limit=pagination.limit, offset=pagination.offset)

    history = []
    for row in rows:
        total_return = None
        sharpe_ratio = None
        if row.get("result"):
            try:
                result_data = json.loads(row["result"]) if isinstance(row["result"], str) else row["result"]
                stats = result_data.get("statistics", {}) if isinstance(result_data, dict) else {}
                total_return = stats.get("total_return")
                sharpe_ratio = stats.get("sharpe_ratio")
            except Exception:
                pass

        history.append(
            {
                "id": row.get("id"),
                "job_id": row.get("job_id"),
                "strategy_id": row.get("strategy_id"),
                "strategy_class": row.get("strategy_class"),
                "strategy_version": row.get("strategy_version"),
                "vt_symbol": row.get("vt_symbol"),
                "start_date": str(row.get("start_date")) if row.get("start_date") else None,
                "end_date": str(row.get("end_date")) if row.get("end_date") else None,
                "status": row.get("status"),
                "total_return": total_return,
                "sharpe_ratio": sharpe_ratio,
                "created_at": row.get("created_at").isoformat() if row.get("created_at") else None,
                "completed_at": row.get("completed_at").isoformat() if row.get("completed_at") else None,
            }
        )

    return paginate(history, total, pagination)


@router.get("/history/{job_id}", dependencies=[require_permission("backtests", "read")])
async def get_backtest_history_detail(job_id: str, current_user: TokenData = Depends(get_current_user)):
    """Get detailed backtest result from database by job_id."""
    dao = BacktestHistoryDao()
    row = dao.get_detail_for_user(job_id=job_id, user_id=current_user.user_id)
    if not row:
        raise APIError(status_code=404, code=ErrorCode.BACKTEST_NOT_FOUND, message="Backtest not found")

    result_data = None
    if row.get("result"):
        try:
            result_data = json.loads(row["result"]) if isinstance(row["result"], str) else row["result"]
        except Exception:
            result_data = None

    params = {}
    if row.get("parameters"):
        try:
            params = json.loads(row["parameters"]) if isinstance(row["parameters"], str) else row["parameters"]
        except Exception:
            params = {}

    return {
        "id": row.get("id"),
        "job_id": row.get("job_id"),
        "strategy_id": row.get("strategy_id"),
        "strategy_class": row.get("strategy_class"),
        "strategy_version": row.get("strategy_version"),
        "vt_symbol": row.get("vt_symbol"),
        "start_date": str(row.get("start_date")) if row.get("start_date") else None,
        "end_date": str(row.get("end_date")) if row.get("end_date") else None,
        "parameters": params,
        "status": row.get("status"),
        "result": result_data,
        "error": row.get("error"),
        "created_at": row.get("created_at").isoformat() if row.get("created_at") else None,
        "completed_at": row.get("completed_at").isoformat() if row.get("completed_at") else None,
    }


@router.delete("/{job_id}", dependencies=[require_permission("backtests", "write")])
async def cancel_backtest(job_id: str, current_user: TokenData = Depends(get_current_user)):
    """Cancel a pending or running backtest."""
    if job_id in _jobs:
        job = _jobs[job_id]
        if job.status in (BacktestStatus.PENDING, BacktestStatus.RUNNING):
            job.status = BacktestStatus.CANCELLED
            job.message = "Cancelled by user"
            return {"message": "Job cancelled"}
        raise APIError(status_code=400, code=ErrorCode.JOB_CANCEL_FAILED, message="Job cannot be cancelled")

    raise APIError(status_code=404, code=ErrorCode.JOB_NOT_FOUND, message="Job not found")


# Background task functions
async def run_backtest_task(job_id: str, request: BacktestRequest, user_id: int):
    """Run a single backtest in background."""
    job = _jobs[job_id]
    job.status = BacktestStatus.RUNNING
    job.started_at = datetime.utcnow()
    job.message = "Running backtest..."

    try:
        service = BacktestService()
        result = service.run_single_backtest(
            strategy_id=request.strategy_id,
            strategy_class=request.strategy_class,
            vt_symbol=request.vt_symbol,
            start_date=request.start_date,
            end_date=request.end_date,
            parameters=request.parameters,
            capital=request.capital,
            rate=request.rate,
            slippage=request.slippage,
            size=request.size,
            benchmark=getattr(request, "benchmark", None),
            period=getattr(request, "period", "daily"),
        )

        job.status = BacktestStatus.COMPLETED
        job.completed_at = datetime.utcnow()
        job.progress = 100.0
        job.result = result
        job.message = "Backtest completed successfully"
        # Persist to database and job storage for history and UI
        try:
            # Convert result to serializable dict
            res_dict = result.dict() if hasattr(result, "dict") else result
        except Exception:
            res_dict = None

        try:
            save_backtest_to_db(
                job_id=job_id,
                user_id=user_id,
                strategy_id=request.strategy_id,
                strategy_class=request.strategy_class,
                symbol=request.vt_symbol,
                start_date=str(request.start_date),
                end_date=str(request.end_date),
                parameters=(
                    res_dict.get("parameters")
                    if isinstance(res_dict, dict) and res_dict.get("parameters") is not None
                    else request.parameters
                ),
                status="completed",
                result=res_dict,
            )
        except Exception:
            # Don't block the response on DB persistence
            pass

        try:
            # Save result to Redis job storage for UI consistency
            js = get_job_storage()
            js.save_job_metadata(
                job_id,
                {
                    "job_id": job_id,
                    "user_id": user_id,
                    "type": "backtest",
                    "status": "finished",
                    "created_at": datetime.utcnow().isoformat(),
                    "updated_at": datetime.utcnow().isoformat(),
                    "parameters": res_dict.get("parameters") if isinstance(res_dict, dict) else request.parameters,
                },
            )
            js.save_result(job_id, res_dict or {})
        except Exception:
            pass

    except Exception as e:
        job.status = BacktestStatus.FAILED
        job.completed_at = datetime.utcnow()
        job.error = str(e)
        job.message = f"Backtest failed: {str(e)}"
        try:
            save_backtest_to_db(
                job_id=job_id,
                user_id=user_id,
                strategy_id=request.strategy_id,
                strategy_class=request.strategy_class,
                symbol=request.vt_symbol,
                start_date=str(request.start_date),
                end_date=str(request.end_date),
                parameters=request.parameters,
                status="failed",
                result=None,
                error=str(e),
            )
        except Exception:
            pass


async def run_batch_backtest_task(job_id: str, request: BatchBacktestRequest, user_id: int):
    """Run batch backtest in background."""
    job = _batch_jobs[job_id]
    job.status = BacktestStatus.RUNNING

    try:
        service = BacktestService()
        results = []
        errors = []

        for i, symbol in enumerate(request.symbols):
            if job.status == BacktestStatus.CANCELLED:
                break

            try:
                result = service.run_single_backtest(
                    strategy_id=request.strategy_id,
                    strategy_class=request.strategy_class,
                    vt_symbol=symbol,
                    start_date=request.start_date,
                    end_date=request.end_date,
                    parameters=request.parameters,
                    capital=request.capital,
                    rate=request.rate,
                    slippage=request.slippage,
                    size=request.size,
                    benchmark=getattr(request, "benchmark", None),
                    period=getattr(request, "period", "daily"),
                )
                if result:
                    results.append(result)
                    # Persist child result into backtest_history for batch runs
                    try:
                        child_job_id = f"{job_id}__{symbol}"
                        res_dict = result.dict() if hasattr(result, "dict") else result
                        save_backtest_to_db(
                            job_id=child_job_id,
                            user_id=user_id,
                            strategy_id=request.strategy_id,
                            strategy_class=request.strategy_class,
                            symbol=symbol,
                            start_date=str(request.start_date),
                            end_date=str(request.end_date),
                            parameters=(
                                res_dict.get("parameters")
                                if isinstance(res_dict, dict) and res_dict.get("parameters") is not None
                                else request.parameters
                            ),
                            status="completed",
                            result=res_dict,
                        )
                    except Exception:
                        pass
            except Exception as e:
                errors.append({"symbol": symbol, "error": str(e)})

            job.completed_symbols = i + 1
            job.progress = (i + 1) / len(request.symbols) * 100

        # Sort by total_return and keep top N
        results.sort(key=lambda r: r.total_return, reverse=True)
        job.results = results[: request.top_n]
        job.errors = errors
        job.status = BacktestStatus.COMPLETED
        job.completed_at = datetime.utcnow()

    except Exception:
        job.status = BacktestStatus.FAILED
        job.completed_at = datetime.utcnow()


# ── Export & Analysis endpoints ──────────────────────────────────────────


class ExportRequest(BaseModel):
    format: str = "csv"  # csv, html, json


class WalkForwardRequest(BaseModel):
    total_bars: int
    in_sample_pct: float = 0.7
    num_windows: int = 5


class MonteCarloRequest(BaseModel):
    trade_returns: list[float]
    num_simulations: int = 1000
    initial_capital: float = 1000000


@router.post("/{job_id}/export")
async def export_backtest(
    job_id: str,
    req: ExportRequest,
    current_user: TokenData = Depends(get_current_user),
):
    """Export a backtest result to CSV/HTML/JSON."""
    from app.domains.backtests.export_service import BacktestExportService
    from fastapi.responses import PlainTextResponse

    job = _jobs.get(job_id)
    if not job or not job.result:
        raise APIError(status_code=404, code=ErrorCode.JOB_NOT_FOUND, message="Job not found or no result")

    result_dict = job.result.dict() if hasattr(job.result, "dict") else job.result
    svc = BacktestExportService()

    if req.format == "csv":
        content = svc.to_csv(result_dict)
        return PlainTextResponse(
            content,
            media_type="text/csv",
            headers={"Content-Disposition": f"attachment; filename=backtest_{job_id}.csv"},
        )
    elif req.format == "html":
        content = svc.to_html(result_dict)
        return PlainTextResponse(content, media_type="text/html")
    else:
        content = svc.to_json(result_dict)
        return PlainTextResponse(content, media_type="application/json")


@router.post("/analysis/walk-forward")
async def walk_forward_analysis(
    req: WalkForwardRequest,
    current_user: TokenData = Depends(get_current_user),
):
    """Run Walk-Forward analysis."""
    from app.domains.backtests.analysis_service import WalkForwardService

    svc = WalkForwardService()
    return svc.run(
        total_bars=req.total_bars,
        in_sample_pct=req.in_sample_pct,
        num_windows=req.num_windows,
    )


@router.post("/analysis/monte-carlo")
async def monte_carlo_analysis(
    req: MonteCarloRequest,
    current_user: TokenData = Depends(get_current_user),
):
    """Run Monte Carlo simulation."""
    from app.domains.backtests.analysis_service import MonteCarloService

    svc = MonteCarloService()
    return svc.run(
        trade_returns=req.trade_returns,
        num_simulations=req.num_simulations,
        initial_capital=req.initial_capital,
    )
