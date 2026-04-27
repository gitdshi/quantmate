from app.domains.trading.paper_trading_service import PaperTradingService


def test_paper_trading_service_lists_source_traceability_fields(monkeypatch):
    row = type(
        "Row",
        (),
        {
            "id": 1,
            "strategy_id": 2,
            "strategy_name": "S1",
            "vt_symbol": "000001.SZSE",
            "parameters": "{}",
            "status": "running",
            "started_at": None,
            "stopped_at": None,
            "source_backtest_job_id": "job-1",
            "source_version_id": 9,
            "risk_check_status": "warn",
            "risk_check_summary": '{"summary":"check"}',
        },
    )()

    class FakeConn:
        def execute(self, *args, **kwargs):
            class Result:
                def fetchall(self_inner):
                    return [row]
            return Result()

    class FakeCtx:
        def __enter__(self):
            return FakeConn()
        def __exit__(self, exc_type, exc, tb):
            return False

    import app.domains.trading.paper_trading_service as mod
    monkeypatch.setattr(mod, "connection", lambda name: FakeCtx())

    result = PaperTradingService().list_deployments(1)

    assert result[0]["source_backtest_job_id"] == "job-1"
    assert result[0]["source_version_id"] == 9
    assert result[0]["risk_check_status"] == "warn"
    assert result[0]["risk_check_summary"] == {"summary": "check"}
