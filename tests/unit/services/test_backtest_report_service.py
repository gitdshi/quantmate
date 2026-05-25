"""Unit tests for backtest AI report generation."""

from __future__ import annotations

import pytest

from app.domains.ai.backtest_report_service import BacktestReportService


@pytest.mark.unit
class TestBacktestReportService:
    def test_build_report_payload_marks_strong_candidate_for_paper(self):
        service = BacktestReportService()

        report = service._build_report_payload(
            {
                "status": "completed",
                "result": {
                    "statistics": {
                        "total_return": 18.5,
                        "annual_return": 12.2,
                        "sharpe_ratio": 1.8,
                        "max_drawdown_percent": 9.5,
                        "win_rate": 61.0,
                    }
                },
            }
        )

        assert report["summary"]["quality"] == "strong"
        assert report["summary"]["risk_level"] == "low"
        actions = report["sections"]["optimization_suggestions"]["actions"]
        assert any(action["action"] == "promote_to_paper" for action in actions)

    def test_build_report_payload_marks_weak_profile_and_adds_repairs(self):
        service = BacktestReportService()

        report = service._build_report_payload(
            {
                "status": "completed",
                "result": {
                    "statistics": {
                        "total_return": -4.2,
                        "annual_return": -2.1,
                        "sharpe_ratio": 0.4,
                        "max_drawdown_percent": 28.0,
                        "win_rate": 42.0,
                    }
                },
            }
        )

        assert report["summary"]["quality"] == "weak"
        assert report["summary"]["risk_level"] == "high"
        actions = {action["action"] for action in report["sections"]["optimization_suggestions"]["actions"]}
        assert "tighten_risk_control" in actions
        assert "review_signal_quality" in actions