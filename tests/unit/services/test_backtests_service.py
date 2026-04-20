"""Unit tests for app.domains.backtests.service — BulkBacktestQueryService."""

from __future__ import annotations

import json
from datetime import datetime
from unittest.mock import patch

import pytest

_SVC_MOD = "app.domains.backtests.service"


def _make_svc():
    with patch(f"{_SVC_MOD}.BulkBacktestDao") as BulkDao, \
         patch(f"{_SVC_MOD}.BulkResultsDao") as ResDao, \
         patch(f"{_SVC_MOD}.MarketService") as MktSvc:
        from app.domains.backtests.service import BulkBacktestQueryService
        svc = BulkBacktestQueryService()
        return svc, svc._bulk, svc._results, svc._market


class TestGetResultsPage:
    def test_basic(self):
        svc, _, results, market = _make_svc()
        results.count_children.return_value = 1
        results.list_children_page.return_value = [
            {
                "job_id": "j1",
                "vt_symbol": "000001.XSHE",
                "status": "completed",
                "error": None,
                "created_at": datetime(2024, 1, 1),
                "completed_at": datetime(2024, 1, 2),
                "result": json.dumps({
                    "statistics": {"total_return": 10.0},
                    "symbol_name": "平安银行",
                    "parameters": {"fast": 5},
                }),
                "parameters": None,
            }
        ]
        page = svc.get_results_page(bulk_job_id="bj1", user_id=1, page=1, page_size=10, sort_order="desc")
        assert page["total"] == 1
        assert page["results"][0]["symbol_name"] == "平安银行"
        assert page["results"][0]["statistics"]["total_return"] == 10.0
        assert page["results"][0]["parameters"] == {"fast": 5}

    def test_no_result_json(self):
        svc, _, results, market = _make_svc()
        results.count_children.return_value = 1
        results.list_children_page.return_value = [
            {
                "job_id": "j2",
                "vt_symbol": "000002.XSHE",
                "status": "queued",
                "error": None,
                "created_at": None,
                "completed_at": None,
                "result": None,
                "parameters": '{"slow": 20}',
            }
        ]
        market.resolve_symbol_name.return_value = "万科A"
        page = svc.get_results_page(bulk_job_id="bj1", user_id=1, page=1, page_size=10, sort_order="asc")
        r = page["results"][0]
        assert r["symbol_name"] == "万科A"
        assert r["parameters"] == {"slow": 20}
        assert r["statistics"] is None

    def test_bad_result_json(self):
        svc, _, results, market = _make_svc()
        results.count_children.return_value = 1
        results.list_children_page.return_value = [
            {
                "job_id": "j3",
                "vt_symbol": "000003.XSHE",
                "status": "completed",
                "error": None,
                "created_at": None,
                "completed_at": None,
                "result": "not valid json {{{",
                "parameters": "also invalid",
            }
        ]
        market.resolve_symbol_name.return_value = ""
        page = svc.get_results_page(bulk_job_id="bj1", user_id=1, page=1, page_size=10, sort_order="desc")
        assert page["results"][0]["statistics"] is None
        assert page["results"][0]["parameters"] == {}

    def test_result_is_dict(self):
        svc, _, results, market = _make_svc()
        results.count_children.return_value = 1
        results.list_children_page.return_value = [
            {
                "job_id": "j4",
                "vt_symbol": "000004.XSHE",
                "status": "completed",
                "error": None,
                "created_at": datetime(2024, 1, 1),
                "completed_at": datetime(2024, 1, 2),
                "result": {"statistics": {"sharpe_ratio": 1.5}, "parameters": {"p": 1}},
                "parameters": None,
            }
        ]
        market.resolve_symbol_name.return_value = ""
        page = svc.get_results_page(bulk_job_id="bj1", user_id=1, page=1, page_size=10, sort_order="desc")
        assert page["results"][0]["parameters"] == {"p": 1}


class TestGetSummary:
    def test_not_owner(self):
        svc, bulk, _, _ = _make_svc()
        bulk.get_owner_user_id.return_value = 2
        with pytest.raises(KeyError, match="Job not found"):
            svc.get_summary(bulk_job_id="bj1", user_id=1)

    def test_empty(self):
        svc, bulk, results, _ = _make_svc()
        bulk.get_owner_user_id.return_value = 1
        results.list_all_children.return_value = []
        summary = svc.get_summary(bulk_job_id="bj1", user_id=1)
        assert summary["total_symbols"] == 0
        assert summary["completed_count"] == 0
        assert summary["win_rate"] == 0

    def test_full_summary(self):
        svc, bulk, results, market = _make_svc()
        bulk.get_owner_user_id.return_value = 1
        market.resolve_symbol_name.return_value = ""

        rows = []
        # 3 completed: 2 winning, 1 losing
        for i, ret in enumerate([15.0, -5.0, 25.0]):
            rows.append({
                "vt_symbol": f"0000{i}.XSHE",
                "status": "completed",
                "result": json.dumps({
                    "statistics": {
                        "total_return": ret,
                        "annual_return": ret * 0.8,
                        "sharpe_ratio": 1.0 if ret > 0 else -0.5,
                        "max_drawdown_percent": abs(ret / 2),
                        "total_trades": 10,
                        "winning_rate": 0.6 if ret > 0 else 0.3,
                        "profit_factor": 2.0 if ret > 0 else 0.5,
                    },
                    "symbol_name": f"Stock{i}",
                }),
                "error": None,
            })
        # 1 failed
        rows.append({
            "vt_symbol": "000099.XSHE",
            "status": "failed",
            "result": None,
            "error": "timeout",
        })
        results.list_all_children.return_value = rows

        summary = svc.get_summary(bulk_job_id="bj1", user_id=1)
        assert summary["total_symbols"] == 4
        assert summary["completed_count"] == 3
        assert summary["failed_count"] == 1
        assert summary["winning_count"] == 2
        assert summary["losing_count"] == 1
        assert summary["win_rate"] == pytest.approx(66.666, abs=0.01)
        assert summary["avg_metrics"]["total_return"] is not None
        assert len(summary["top10"]) == 3
        assert len(summary["failed_symbols"]) == 1

    def test_distribution_buckets(self):
        svc, bulk, results, market = _make_svc()
        bulk.get_owner_user_id.return_value = 1
        market.resolve_symbol_name.return_value = ""

        returns = [-25.0, -15.0, -5.0, 5.0, 15.0, 25.0]
        rows = []
        for i, ret in enumerate(returns):
            rows.append({
                "vt_symbol": f"0000{i}.XSHE",
                "status": "completed",
                "result": json.dumps({
                    "statistics": {"total_return": ret},
                    "symbol_name": f"S{i}",
                }),
                "error": None,
            })
        results.list_all_children.return_value = rows

        summary = svc.get_summary(bulk_job_id="bj1", user_id=1)
        dist = summary["return_distribution"]
        assert dist["<-20%"] == 1
        assert dist["-20%~-10%"] == 1
        assert dist["-10%~0%"] == 1
        assert dist["0%~10%"] == 1
        assert dist["10%~20%"] == 1
        assert dist[">20%"] == 1

    def test_failed_with_result_json(self):
        svc, bulk, results, market = _make_svc()
        bulk.get_owner_user_id.return_value = 1
        market.resolve_symbol_name.return_value = "Fallback"

        rows = [{
            "vt_symbol": "000001.XSHE",
            "status": "failed",
            "result": json.dumps({"symbol_name": "TestName"}),
            "error": "connection error",
        }]
        results.list_all_children.return_value = rows

        summary = svc.get_summary(bulk_job_id="bj1", user_id=1)
        assert summary["failed_symbols"][0]["symbol_name"] == "TestName"
        assert summary["failed_symbols"][0]["error"] == "connection error"

    def test_parse_error_in_completed(self):
        svc, bulk, results, market = _make_svc()
        bulk.get_owner_user_id.return_value = 1
        market.resolve_symbol_name.return_value = ""

        rows = [{
            "vt_symbol": "000001.XSHE",
            "status": "completed",
            "result": "bad json {{{",
            "error": None,
        }]
        results.list_all_children.return_value = rows

        summary = svc.get_summary(bulk_job_id="bj1", user_id=1)
        assert summary["completed_count"] == 0
        assert summary["failed_count"] == 1
        assert summary["failed_symbols"][0]["error"] == "Parse error"
