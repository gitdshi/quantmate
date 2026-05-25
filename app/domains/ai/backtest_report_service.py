"""Backtest AI report service."""

from __future__ import annotations

import json
from datetime import datetime
from typing import Any, Optional

from sqlalchemy import text

from app.api.services.backtest_service import get_backtest_service
from app.infrastructure.db.connections import connection


def _loads_report(value: Any) -> dict[str, Any]:
    if isinstance(value, dict):
        return value
    if isinstance(value, str) and value:
        try:
            parsed = json.loads(value)
            return parsed if isinstance(parsed, dict) else {}
        except json.JSONDecodeError:
            return {}
    return {}


class BacktestReportService:
    """Generates and stores structured report payloads for completed backtests."""

    def get_report(self, user_id: int, job_id: str) -> Optional[dict[str, Any]]:
        job = get_backtest_service().get_job_status(job_id, user_id)
        if not job:
            return None

        with connection("quantmate") as conn:
            row = conn.execute(
                text(
                    """
                    SELECT job_id, status, report_json, created_at, completed_at
                    FROM ai_backtest_reports
                    WHERE user_id = :user_id AND job_id = :job_id
                    """
                ),
                {"user_id": user_id, "job_id": job_id},
            ).fetchone()

        if not row:
            return None

        return {
            "job_id": row.job_id,
            "status": row.status,
            "report_json": _loads_report(row.report_json),
            "created_at": row.created_at,
            "completed_at": row.completed_at,
        }

    def generate_report(self, user_id: int, job_id: str) -> dict[str, Any]:
        job = get_backtest_service().get_job_status(job_id, user_id)
        if not job:
            raise KeyError(job_id)

        status = str(job.get("status") or "").lower()
        if status not in {"completed", "finished"}:
            raise ValueError("Backtest must be completed before generating an AI report")

        report = self._build_report_payload(job)
        now = datetime.utcnow()

        with connection("quantmate") as conn:
            conn.execute(
                text(
                    """
                    INSERT INTO ai_backtest_reports (user_id, job_id, status, report_json, created_at, completed_at)
                    VALUES (:user_id, :job_id, 'completed', CAST(:report_json AS JSON), :created_at, :completed_at)
                    ON DUPLICATE KEY UPDATE
                        status = VALUES(status),
                        report_json = VALUES(report_json),
                        completed_at = VALUES(completed_at)
                    """
                ),
                {
                    "user_id": user_id,
                    "job_id": job_id,
                    "report_json": json.dumps(report, ensure_ascii=True),
                    "created_at": now,
                    "completed_at": now,
                },
            )
            conn.commit()

        return {
            "job_id": job_id,
            "status": "completed",
            "report_json": report,
            "created_at": now,
            "completed_at": now,
        }

    def _build_report_payload(self, job: dict[str, Any]) -> dict[str, Any]:
        result = job.get("result") or {}
        stats = result.get("statistics") if isinstance(result, dict) else {}
        stats = stats if isinstance(stats, dict) else {}

        total_return = float(stats.get("total_return") or 0)
        annual_return = float(stats.get("annual_return") or 0)
        sharpe = float(stats.get("sharpe_ratio") or 0)
        max_drawdown = float(stats.get("max_drawdown_percent") or stats.get("max_drawdown") or 0)
        win_rate = float(stats.get("win_rate") or 0)

        quality = "strong"
        if sharpe < 1 or total_return <= 0:
            quality = "weak"
        elif sharpe < 1.5 or max_drawdown > 20:
            quality = "mixed"

        risk_level = "low"
        if max_drawdown > 25:
            risk_level = "high"
        elif max_drawdown > 15:
            risk_level = "medium"

        overfit_risk = "low"
        if sharpe > 3 and win_rate > 75:
            overfit_risk = "high"
        elif sharpe > 2.2 or win_rate > 68:
            overfit_risk = "medium"

        actions: list[dict[str, Any]] = []
        if max_drawdown > 15:
            actions.append({
                "action": "tighten_risk_control",
                "label": "Add or tighten stop-loss and position sizing controls",
            })
        if sharpe < 1:
            actions.append({
                "action": "review_signal_quality",
                "label": "Review factor weights or add signal filters before redeploying",
            })
        if total_return > 0 and sharpe >= 1 and max_drawdown <= 20:
            actions.append({
                "action": "promote_to_paper",
                "label": "Promote this strategy to paper trading with guardrails enabled",
            })

        overall = (
            "The backtest shows a strong balance of return and risk and is a reasonable candidate for paper trading."
            if quality == "strong"
            else "The backtest is directionally promising but still needs tuning before deployment."
            if quality == "mixed"
            else "The backtest is not yet strong enough for deployment and should stay in research mode."
        )

        return {
            "summary": {
                "quality": quality,
                "risk_level": risk_level,
                "overfit_risk": overfit_risk,
            },
            "sections": {
                "overall_assessment": {
                    "title": "Overall Assessment",
                    "content": overall,
                },
                "risk_analysis": {
                    "title": "Risk Analysis",
                    "content": (
                        f"Max drawdown is {max_drawdown:.2f}% and win rate is {win_rate:.2f}%. "
                        f"This profile is classified as {risk_level} risk for the current validation window."
                    ),
                },
                "overfitting_assessment": {
                    "title": "Overfitting Assessment",
                    "content": (
                        f"Sharpe ratio is {sharpe:.2f}. Combined with the observed win rate, "
                        f"the heuristic overfitting risk is {overfit_risk}."
                    ),
                },
                "trading_behavior": {
                    "title": "Trading Behavior Analysis",
                    "content": (
                        f"Annual return is {annual_return:.2f}% and total return is {total_return:.2f}%. "
                        "Use trade logs and concentration checks before promoting to broader paper exposure."
                    ),
                },
                "optimization_suggestions": {
                    "title": "Optimization Suggestions",
                    "content": "Apply guardrails, signal filters, or factor-weight tuning based on the report summary.",
                    "actions": actions,
                },
                "market_suitability": {
                    "title": "Market Suitability",
                    "content": (
                        "This run should be rechecked across adjacent market regimes before live promotion. "
                        "Use paper trading to confirm stability under current market conditions."
                    ),
                },
            },
        }