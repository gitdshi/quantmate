"""Autopilot orchestrator: daily DAG state machine + CLI entry point.

Wires the existing atomic capabilities (Qlib ingest, factor mining/backtest,
decision engine, deploy bridge, settlement/analytics) into a single ordered
loop, persisted through :class:`AutopilotDao`.

Usage::

    python -m app.domains.autopilot.orchestrator --once          # run full DAG now
    python -m app.domains.autopilot.orchestrator --daemon        # scheduled loop
    python -m app.domains.autopilot.orchestrator --stage e1_settlement
    python -m app.domains.autopilot.orchestrator --status

Idempotency: a stage already ``success`` is skipped on re-run, so repeated
``--once`` invocations do not double-deploy. Heavy research steps (factor
mining / factor backtest) are dispatched to the existing RQ worker queues and
polled until completion.
"""

from __future__ import annotations

import argparse
import json
import logging
import time
from datetime import datetime
from typing import Any, Optional

from app.domains.autopilot.dao.autopilot_dao import AutopilotDao
from app.domains.autopilot.decision_engine import evaluate_performance, select_factors
from app.domains.autopilot.deploy_bridge import (
    build_composite_strategy,
    build_combined_expression,
    deploy,
)
from app.domains.autopilot.guardrails import data_quality_gate, is_kill_switch_active
from app.domains.autopilot.policies import Policies
from app.domains.autopilot.resolvers import (
    get_paper_account,
    resolve_paper_account,
    resolve_user_id,
)
from app.domains.autopilot.schedule import (
    business_date_cn,
    is_trading_day,
    now_cn,
    stage_trigger_time,
)
from app.domains.autopilot.state import (
    STAGE_ORDER,
    TRADING_STAGES,
    RunStatus,
    Stage,
    StageStatus,
    previous_stage,
)
from app.infrastructure.db.connections import connection
from sqlalchemy import text

logger = logging.getLogger(__name__)

_POLL_INTERVAL_SECONDS = 10
_JOB_TIMEOUT_SECONDS = 1800

# Research stages that must pass the data-quality gate before running.
_DATA_GATE_STAGES: frozenset[Stage] = frozenset(
    {
        Stage.FACTOR_MINING,
        Stage.FACTOR_BACKTEST,
        Stage.FACTOR_SELECTION,
    }
)


class AutopilotOrchestrator:
    """Executes autopilot stages in dependency order and persists runtime state."""

    def __init__(self, dao: Optional[AutopilotDao] = None) -> None:
        self.dao = dao or AutopilotDao()

    # ── Public entry points ─────────────────────────────────────────────

    def run_once(self, start_at: Optional[Stage] = None) -> dict[str, Any]:
        """Run the full DAG (or a suffix) sequentially, ignoring the clock."""
        self.dao.reset_running_runs_to_pending()
        policies = Policies.load()
        if not policies.enabled:
            return {"status": "disabled", "reason": "autopilot.enabled is false"}

        run = self._ensure_run()
        run_id = run["run_id"]
        start_idx = STAGE_ORDER.index(start_at) if start_at else 0

        for stage in STAGE_ORDER[start_idx:]:
            self._execute_stage(run_id, stage, policies)

        self._sync_run_status(run_id)
        return {"status": "done", "run_id": run_id, "stages": self.dao.list_stages(run_id)}

    def run_single(self, stage: Stage) -> dict[str, Any]:
        """Run one stage in isolation (dependency ignored)."""
        self.dao.reset_running_runs_to_pending()
        policies = Policies.load()
        run = self._ensure_run()
        self._execute_stage(run["run_id"], stage, policies)
        self._sync_run_status(run["run_id"])
        return {"status": "done", "run_id": run["run_id"]}

    def run_daemon(self, poll_interval: int = 30) -> None:
        """Scheduled loop: trigger stages once their trigger time passes."""
        self.dao.reset_running_runs_to_pending()
        logger.info("Autopilot orchestrator starting (daemon, poll=%ss)", poll_interval)
        while True:
            try:
                self._tick()
            except Exception:
                logger.exception("Autopilot daemon tick failed")
            time.sleep(poll_interval)

    # ── Scheduling ──────────────────────────────────────────────────────

    def _tick(self) -> None:
        policies = Policies.load()
        if not policies.enabled:
            return
        business_date = business_date_cn()
        run = self.dao.get_run_for_date(business_date) or self.dao.create_run(business_date)
        run_id = run["run_id"]
        now = now_cn()

        for stage in STAGE_ORDER:
            if now < stage_trigger_time(stage, business_date):
                continue

            if stage in TRADING_STAGES and not is_trading_day(business_date):
                self._ensure_stage_skipped(run_id, stage, "non-trading day")
                continue

            current = self.dao.get_stage(run_id, stage.value)
            if current and current["status"] in {StageStatus.SUCCESS.value, StageStatus.SKIPPED.value}:
                continue

            prev = previous_stage(stage)
            if prev:
                prev_row = self.dao.get_stage(run_id, prev.value)
                if not prev_row or prev_row["status"] != StageStatus.SUCCESS.value:
                    continue  # wait for dependency

            self._execute_stage(run_id, stage, policies)

        self._sync_run_status(run_id)

    # ── Stage execution ─────────────────────────────────────────────────

    def _execute_stage(self, run_id: str, stage: Stage, policies: Policies) -> None:
        current = self.dao.get_stage(run_id, stage.value)
        if current and current["status"] == StageStatus.SUCCESS.value:
            logger.info("[autopilot %s] stage %s already succeeded; skip", run_id, stage.value)
            return

        if is_kill_switch_active(policies):
            self._ensure_stage_skipped(run_id, stage, "global kill-switch active")
            return

        if stage in _DATA_GATE_STAGES:
            gate = data_quality_gate(business_date_cn(), policies)
            if not gate["ok"]:
                self._ensure_stage_skipped(run_id, stage, gate["reason"])
                return

        try:
            self.dao.start_stage(run_id, stage.value)
        except Exception as exc:
            logger.exception("[autopilot %s] failed to start stage %s", run_id, stage.value)
            self.dao.finish_stage(run_id, stage.value, StageStatus.FAILED, error=str(exc))
            return

        try:
            result = self._dispatch(run_id, stage, policies)
            self.dao.finish_stage(run_id, stage.value, StageStatus.SUCCESS, result=result)
            logger.info("[autopilot %s] stage %s success", run_id, stage.value)
        except Exception as exc:
            logger.exception("[autopilot %s] stage %s failed", run_id, stage.value)
            self.dao.finish_stage(run_id, stage.value, StageStatus.FAILED, error=str(exc))

    def _ensure_stage_skipped(self, run_id: str, stage: Stage, reason: str) -> None:
        existing = self.dao.get_stage(run_id, stage.value)
        if not existing:
            self.dao.get_or_create_stage(run_id, stage.value)
        self.dao.finish_stage(run_id, stage.value, StageStatus.SKIPPED, error=reason)

    # ── Stage handlers ──────────────────────────────────────────────────

    def _dispatch(self, run_id: str, stage: Stage, policies: Policies) -> dict[str, Any]:
        handlers = {
            Stage.QLIB_INGEST: self._run_qlib_ingest,
            Stage.FACTOR_MINING: self._run_factor_mining,
            Stage.FACTOR_BACKTEST: self._run_factor_backtest,
            Stage.FACTOR_SELECTION: self._run_factor_selection,
            Stage.STRATEGY_DEPLOY: self._run_strategy_deploy,
            Stage.PREMARKET_CHECK: self._run_premarket_check,
            Stage.SETTLEMENT: self._run_settlement,
            Stage.ANALYSIS: self._run_analysis,
            Stage.STRATEGY_ADJUST: self._run_strategy_adjust,
        }
        return handlers[stage](run_id, policies)

    def _run_qlib_ingest(self, run_id: str, policies: Policies) -> dict[str, Any]:
        from app.infrastructure.qlib.qlib_config import get_qlib_data_range

        data_range = get_qlib_data_range()
        if not data_range:
            raise RuntimeError(
                "Qlib data not populated; run data conversion (qlib ingest) first"
            )
        return {"data_range": list(data_range), "note": "Qlib data available"}

    def _run_factor_mining(self, run_id: str, policies: Policies) -> dict[str, Any]:
        from app.worker.service.factor_tasks import enqueue_factor_pipeline

        user_id = resolve_user_id(policies)
        start_date, end_date = self._research_window(policies)

        job_id = enqueue_factor_pipeline(
            user_id=user_id,
            source="alpha158",
            universe=policies.universe,
            start_date=start_date,
            end_date=end_date,
            auto_promote=False,
            ic_threshold=policies.ic_threshold,
            ir_threshold=policies.ir_threshold,
            top_n=policies.top_n_factor,
        )
        if not job_id:
            raise RuntimeError("Failed to enqueue factor mining pipeline")

        result = self._poll_job(job_id, _JOB_TIMEOUT_SECONDS)
        if result.get("status") == "failed":
            raise RuntimeError(f"Factor mining failed: {result.get('error')}")

        result["job_id"] = job_id
        result["user_id"] = user_id
        return result

    def _run_factor_backtest(self, run_id: str, policies: Policies) -> dict[str, Any]:
        user_id = resolve_user_id(policies)
        candidates = self._load_candidate_factors(user_id, policies.universe)
        if not candidates:
            raise RuntimeError("No candidate factors found; run factor mining first")

        expression, factors = self._candidate_expression(candidates, policies.top_n_factor)
        start_date, end_date = self._research_window(policies)

        request_payload: dict[str, Any] = {
            "subject_type": "factor",
            "subject_id": None,
            "subject_name": "Autopilot Candidate Portfolio",
            "start_date": start_date,
            "end_date": end_date,
            "benchmark": "000300.SH",
            "initial_capital": 1_000_000.0,
            "costs": {},
            "profile": {
                "expression": expression,
                "universe": {"preset": policies.universe},
                "top_n": policies.top_n_factor,
                "forward_periods": 1,
                "market_constraints": {
                    "t_plus_one": True,
                    "limit_up_down": True,
                    "lot_size": 100,
                },
            },
        }

        job_id = self._enqueue_factor_backtest(user_id, request_payload)
        if not job_id:
            raise RuntimeError("Failed to enqueue factor backtest")

        result = self._poll_job(job_id, _JOB_TIMEOUT_SECONDS)
        if result.get("status") == "failed":
            raise RuntimeError(f"Factor backtest failed: {result.get('error')}")

        result["job_id"] = job_id
        result["candidate_factors"] = [f["name"] for f in factors]
        result["expression"] = expression
        return result

    def _run_factor_selection(self, run_id: str, policies: Policies) -> dict[str, Any]:
        user_id = resolve_user_id(policies)
        candidates = self._load_candidate_factors(user_id, policies.universe)
        if not candidates:
            raise RuntimeError("No candidate factors found; run factor mining first")

        previous_names = self._previous_selected_factors()
        result = select_factors(candidates, policies, previous_factors=previous_names)

        self.dao.record_decision(
            run_id,
            decision_type="factor_selection",
            action="select",
            subject_type="factor",
            input_summary={
                "selected": [f["name"] for f in result["factors"]],
                "rejected_count": len(result["rejected"]),
            },
            reason=f"selected {len(result['factors'])} factors from {len(candidates)} candidates",
        )
        return result

    def _run_strategy_deploy(self, run_id: str, policies: Policies) -> dict[str, Any]:
        selection = self.dao.get_stage_result(run_id, Stage.FACTOR_SELECTION.value) or {}
        factors = selection.get("factors") or []
        if not factors:
            raise RuntimeError("No selected factors; cannot build deployment")

        user_id = resolve_user_id(policies)
        account_id = resolve_paper_account(user_id, policies)

        universe_symbols = self._resolve_universe(policies, business_date_cn())

        backtest = self.dao.get_stage_result(run_id, Stage.FACTOR_BACKTEST.value) or {}
        composite = build_composite_strategy(user_id, factors, universe_symbols, policies)

        deployment = deploy(
            user_id=user_id,
            composite_strategy_id=composite["id"],
            paper_account_id=account_id,
            selected_factors=factors,
            policies=policies,
            source_backtest_job_id=backtest.get("job_id"),
        )

        approval = deployment.get("approval_status", "auto")
        self.dao.record_decision(
            run_id,
            decision_type="deploy",
            action="deploy" if deployment.get("success") else "hold",
            subject_type="composite_strategy",
            subject_id=str(composite["id"]),
            input_summary={
                "composite_strategy_id": composite["id"],
                "paper_account_id": account_id,
                "factors": [f["name"] for f in factors],
            },
            reason=deployment.get("error")
            or f"deployed (deployment_id={deployment.get('deployment_id')})",
            approval_status=approval,
        )
        return {"composite_strategy_id": composite["id"], "deployment": deployment}

    def _run_premarket_check(self, run_id: str, policies: Policies) -> dict[str, Any]:
        if is_kill_switch_active(policies):
            raise RuntimeError("global kill-switch active; cannot trade")

        user_id = resolve_user_id(policies)
        account_id = resolve_paper_account(user_id, policies)
        account = get_paper_account(user_id, account_id)
        if not account:
            raise RuntimeError(f"Paper account {account_id} not found")

        return {
            "account_id": account_id,
            "balance": account.get("balance"),
            "market_value": account.get("market_value"),
            "kill_switch": False,
            "ready": True,
        }

    def _run_settlement(self, run_id: str, policies: Policies) -> dict[str, Any]:
        from app.domains.trading.paper_settlement_service import PaperSettlementService

        return PaperSettlementService().settle_all(business_date_cn())

    def _run_analysis(self, run_id: str, policies: Policies) -> dict[str, Any]:
        from app.domains.trading.paper_analytics_service import PaperAnalyticsService

        user_id = resolve_user_id(policies)
        account_id = resolve_paper_account(user_id, policies)
        analytics = PaperAnalyticsService().get_analytics(account_id, user_id)
        return {"analytics": analytics}

    def _run_strategy_adjust(self, run_id: str, policies: Policies) -> dict[str, Any]:
        analysis_result = self.dao.get_stage_result(run_id, Stage.ANALYSIS.value) or {}
        analytics = analysis_result.get("analytics", analysis_result)

        history = self._load_performance_history(policies)
        action = evaluate_performance(analytics, policies, history=history)

        self.dao.record_decision(
            run_id,
            decision_type="adjust",
            action=action["action"],
            subject_type="deployment",
            input_summary={
                "sharpe_ratio": analytics.get("sharpe_ratio"),
                "max_drawdown_pct": analytics.get("max_drawdown_pct"),
            },
            reason=action["reason"],
        )
        return action

    # ── Helpers ─────────────────────────────────────────────────────────

    def _ensure_run(self) -> dict[str, Any]:
        business_date = business_date_cn()
        return self.dao.get_run_for_date(business_date) or self.dao.create_run(business_date)

    def _research_window(self, policies: Policies) -> tuple[str, str]:
        from app.infrastructure.qlib.qlib_config import get_qlib_data_range

        data_range = get_qlib_data_range()
        if data_range:
            return data_range[0], data_range[1]

        from datetime import timedelta

        end = business_date_cn()
        start = end - timedelta(days=365 * policies.init_backtest_years)
        return start.isoformat(), end.isoformat()

    def _enqueue_factor_backtest(self, user_id: int, request_payload: dict[str, Any]) -> Optional[str]:
        from app.worker.service.config import get_queue

        job_id = f"autopilot-bt-{int(time.time())}"
        job = get_queue("backtest").enqueue(
            "app.domains.factors.backtest_task.run_factor_backtest_task",
            kwargs={
                "job_id": job_id,
                "user_id": user_id,
                "request_payload": request_payload,
            },
            job_id=job_id,
            job_timeout=_JOB_TIMEOUT_SECONDS,
        )
        return job.id if job else None

    def _poll_job(self, job_id: str, timeout: int) -> dict[str, Any]:
        from rq.job import Job

        from app.worker.service.config import redis_conn

        deadline = time.time() + timeout
        while time.time() < deadline:
            try:
                job = Job.fetch(job_id, connection=redis_conn)
            except Exception:
                time.sleep(_POLL_INTERVAL_SECONDS)
                continue

            if job is None:
                time.sleep(_POLL_INTERVAL_SECONDS)
                continue
            if job.is_finished:
                return job.result or {}
            if job.is_failed:
                return {"status": "failed", "error": str(job.exc_info)}
            time.sleep(_POLL_INTERVAL_SECONDS)

        return {"status": "failed", "error": f"job {job_id} timed out after {timeout}s"}

    def _load_candidate_factors(self, user_id: int, universe: str) -> list[dict[str, Any]]:
        """Read the most recent screening run's per-factor metrics."""
        with connection("quantmate") as conn:
            run_row = conn.execute(
                text(
                    "SELECT id FROM factor_screening_results WHERE user_id = :uid "
                    "ORDER BY id DESC LIMIT 1"
                ),
                {"uid": user_id},
            ).fetchone()
            if not run_row:
                return []
            rows = conn.execute(
                text(
                    "SELECT factor_name, ic_mean, rank_ir, expression, metrics "
                    "FROM factor_screening_details WHERE run_id = :rid ORDER BY rank_order ASC"
                ),
                {"rid": run_row[0]},
            ).fetchall()

        candidates: list[dict[str, Any]] = []
        for row in rows:
            metrics = json.loads(row.metrics) if row.metrics else {}
            metrics.setdefault("factor_name", row.factor_name)
            metrics.setdefault("ic_mean", float(row.ic_mean or 0))
            metrics.setdefault("rank_ir", float(row.rank_ir or 0))
            metrics.setdefault("expression", row.expression or "")
            candidates.append(metrics)
        return candidates

    def _candidate_expression(
        self, candidates: list[dict[str, Any]], top_n: int
    ) -> tuple[str, list[dict[str, Any]]]:
        ranked = sorted(
            candidates,
            key=lambda c: abs(float(c.get("rank_ir") or c.get("ic_ir") or 0)),
            reverse=True,
        )[: top_n]

        factors = []
        for c in ranked:
            ic_mean = float(c.get("ic_mean") or 0)
            factors.append(
                {
                    "name": c.get("factor_name") or c.get("name"),
                    "expression": c.get("expression") or "",
                    "direction": 1.0 if ic_mean >= 0 else -1.0,
                    "weight": 1.0,
                }
            )
        return build_combined_expression(factors), factors

    def _resolve_universe(self, policies: Policies, business_date: Any) -> list[str]:
        from app.domains.factors.backtest_task import _resolve_index_universe

        return _resolve_index_universe(policies.universe, business_date)

    def _previous_selected_factors(self) -> list[str]:
        # Best-effort: reuse the factors selected in a prior completed run.
        with connection("quantmate") as conn:
            rows = conn.execute(
                text(
                    "SELECT result FROM autopilot_stages "
                    "WHERE stage = :s AND status = 'success' ORDER BY id DESC LIMIT 1"
                ),
                {"s": Stage.FACTOR_SELECTION.value},
            ).fetchall()
        if not rows or not rows[0][0]:
            return []
        try:
            result = json.loads(rows[0][0])
            return [f.get("name") for f in result.get("factors", []) if f.get("name")]
        except Exception:
            return []

    def _load_performance_history(self, policies: Policies) -> list[dict[str, Any]]:
        with connection("quantmate") as conn:
            rows = conn.execute(
                text(
                    "SELECT result FROM autopilot_stages "
                    "WHERE stage = :s AND status = 'success' ORDER BY id ASC"
                ),
                {"s": Stage.ANALYSIS.value},
            ).fetchall()
        history: list[dict[str, Any]] = []
        for row in rows:
            try:
                if not row[0]:
                    continue
                result = json.loads(row[0])
                analytics = result.get("analytics", result)
                history.append(analytics)
            except Exception:
                continue
        return history

    def _sync_run_status(self, run_id: str) -> None:
        stages = self.dao.list_stages(run_id)
        if not stages:
            return
        statuses = [s["status"] for s in stages]
        if StageStatus.FAILED.value in statuses:
            self.dao.update_run_status(run_id, RunStatus.FAILED)
        elif len(stages) == len(STAGE_ORDER) and all(
            s in {StageStatus.SUCCESS.value, StageStatus.SKIPPED.value} for s in statuses
        ):
            self.dao.update_run_status(run_id, RunStatus.SUCCESS)

    # ── Status reporting ────────────────────────────────────────────────

    def status_report(self) -> dict[str, Any]:
        policies = Policies.load()
        runs = self.dao.list_runs(limit=10)
        business_date = business_date_cn()
        run = self.dao.get_run_for_date(business_date)
        return {
            "enabled": policies.enabled,
            "kill_switch": policies.kill_switch,
            "business_date": business_date.isoformat(),
            "current_run": run,
            "recent_runs": runs,
        }


# ── CLI ───────────────────────────────────────────────────────────────────


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Autopilot closed-loop orchestrator")
    group = parser.add_mutually_exclusive_group()
    group.add_argument("--once", action="store_true", help="run the full DAG now")
    group.add_argument("--daemon", action="store_true", help="run the scheduled loop")
    group.add_argument("--stage", type=str, help="run a single stage (e.g. e1_settlement)")
    group.add_argument("--status", action="store_true", help="print current autopilot status")
    parser.add_argument("--start-at", type=str, help="(with --once) begin at a stage value")
    parser.add_argument("--poll-interval", type=int, default=30, help="daemon poll interval seconds")
    return parser


def main(argv: Optional[list[str]] = None) -> int:
    args = _build_parser().parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")

    orchestrator = AutopilotOrchestrator()

    if args.status:
        import pprint

        pprint.pprint(orchestrator.status_report())
        return 0

    if args.stage:
        try:
            stage = Stage(args.stage)
        except ValueError:
            print(f"Unknown stage: {args.stage!r}. Valid: {[s.value for s in STAGE_ORDER]}")
            return 2
        result = orchestrator.run_single(stage)
        print(result)
        return 0

    if args.once:
        start_at = Stage(args.start_at) if args.start_at else None
        result = orchestrator.run_once(start_at=start_at)
        print(result)
        return 0

    if args.daemon:
        orchestrator.run_daemon(poll_interval=args.poll_interval)
        return 0

    _build_parser().print_help()
    return 1


if __name__ == "__main__":
    raise SystemExit(main())