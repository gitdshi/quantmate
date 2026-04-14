"""Coverage batch for vnpy_ingest, akshare_ingest, api/main, datasync/scheduler, backtest_service extra.

Targets ~230 miss across:
  - vnpy_ingest.py  48 miss
  - akshare_ingest.py  35 miss
  - api/main.py  54 miss
  - datasync/scheduler.py  32 miss
  - backtest_service.py extra  70 miss
"""
from __future__ import annotations

import json
import os
from datetime import date, datetime, timedelta
from types import SimpleNamespace
from unittest.mock import MagicMock, patch, PropertyMock

import pytest


# ═══════════════════════════════════════════════════════════════════════
# vnpy_ingest.py  (48 miss → ~0)
# ═══════════════════════════════════════════════════════════════════════

class TestVnpyIngest:
    def test_get_symbol(self):
        from app.datasync.service.vnpy_ingest import get_symbol
        assert get_symbol("000001.SZ") == "000001"
        assert get_symbol("AAPL") == "AAPL"

    def test_map_exchange(self):
        from app.datasync.service.vnpy_ingest import map_exchange
        assert map_exchange("000001.SZ") == "SZSE"
        assert map_exchange("600000.SH") == "SSE"
        assert map_exchange("830001.BJ") == "BSE"
        assert map_exchange("AAPL") == "SZSE"  # default

    @patch("app.datasync.service.vnpy_ingest.update_sync_status")
    @patch("app.datasync.service.vnpy_ingest.bulk_upsert_dbbardata")
    @patch("app.datasync.service.vnpy_ingest.fetch_stock_daily_rows")
    @patch("app.datasync.service.vnpy_ingest.get_last_sync_date")
    def test_sync_symbol_no_data(self, last_sync, fetch, upsert, upd):
        from app.datasync.service.vnpy_ingest import sync_symbol_to_vnpy
        last_sync.return_value = None
        fetch.return_value = []
        assert sync_symbol_to_vnpy("000001.SZ") == 0

    @patch("app.datasync.service.vnpy_ingest.update_sync_status")
    @patch("app.datasync.service.vnpy_ingest.bulk_upsert_dbbardata")
    @patch("app.datasync.service.vnpy_ingest.fetch_stock_daily_rows")
    @patch("app.datasync.service.vnpy_ingest.get_last_sync_date")
    def test_sync_symbol_with_data(self, last_sync, fetch, upsert, upd):
        from app.datasync.service.vnpy_ingest import sync_symbol_to_vnpy
        last_sync.return_value = date(2024, 1, 1)
        fetch.return_value = [
            ("2024-01-02", 10.0, 11.0, 9.0, 10.5, 1000, 10000),
        ]
        upsert.return_value = 1
        assert sync_symbol_to_vnpy("000001.SZ") == 1
        upd.assert_called_once()

    @patch("app.datasync.service.vnpy_ingest.update_sync_status")
    @patch("app.datasync.service.vnpy_ingest.bulk_upsert_dbbardata")
    @patch("app.datasync.service.vnpy_ingest.fetch_stock_daily_rows")
    @patch("app.datasync.service.vnpy_ingest.get_last_sync_date")
    def test_sync_symbol_date_object(self, last_sync, fetch, upsert, upd):
        from app.datasync.service.vnpy_ingest import sync_symbol_to_vnpy
        last_sync.return_value = None
        fetch.return_value = [
            (date(2024, 1, 2), 10.0, 11.0, 9.0, 10.5, None, None),
        ]
        upsert.return_value = 1
        assert sync_symbol_to_vnpy("000001.SZ", start_date=date(2024, 1, 1)) == 1

    @patch("app.datasync.service.vnpy_ingest.upsert_dbbaroverview")
    @patch("app.datasync.service.vnpy_ingest.get_bar_stats")
    def test_update_bar_overview(self, stats, upsert):
        from app.datasync.service.vnpy_ingest import update_bar_overview
        stats.return_value = (100, datetime(2024, 1, 1), datetime(2024, 6, 1))
        update_bar_overview("000001", "SZSE")
        upsert.assert_called_once()

    @patch("app.datasync.service.vnpy_ingest.upsert_dbbaroverview")
    @patch("app.datasync.service.vnpy_ingest.get_bar_stats")
    def test_update_bar_overview_empty(self, stats, upsert):
        from app.datasync.service.vnpy_ingest import update_bar_overview
        stats.return_value = (0, None, None)
        update_bar_overview("000001", "SZSE")
        upsert.assert_not_called()

    @patch("app.datasync.service.vnpy_ingest.update_bar_overview")
    @patch("app.datasync.service.vnpy_ingest.sync_symbol_to_vnpy")
    @patch("app.datasync.service.vnpy_ingest.get_stock_daily_ts_codes_for_date")
    def test_sync_date_to_vnpy(self, get_codes, sync_sym, upd_overview):
        from app.datasync.service.vnpy_ingest import sync_date_to_vnpy
        get_codes.return_value = ["000001.SZ", "000002.SZ"]
        sync_sym.side_effect = [5, 3]
        symbols, bars = sync_date_to_vnpy(date(2024, 1, 15))
        assert symbols == 2
        assert bars == 8

    @patch("app.datasync.service.vnpy_ingest.get_stock_daily_ts_codes_for_date")
    def test_sync_date_no_data(self, get_codes):
        from app.datasync.service.vnpy_ingest import sync_date_to_vnpy
        get_codes.return_value = []
        s, b = sync_date_to_vnpy(date(2024, 1, 15))
        assert s == 0 and b == 0

    @patch("app.datasync.service.vnpy_ingest.update_bar_overview")
    @patch("app.datasync.service.vnpy_ingest.sync_symbol_to_vnpy")
    @patch("app.datasync.service.vnpy_ingest.get_stock_daily_ts_codes_for_date")
    def test_sync_date_single_failure(self, get_codes, sync_sym, upd):
        from app.datasync.service.vnpy_ingest import sync_date_to_vnpy
        get_codes.return_value = ["000001.SZ"]
        sync_sym.side_effect = RuntimeError("fail")
        s, b = sync_date_to_vnpy(date(2024, 1, 15))
        assert s == 0 and b == 0

    @patch("app.datasync.service.vnpy_ingest.update_bar_overview")
    @patch("app.datasync.service.vnpy_ingest.sync_symbol_to_vnpy")
    @patch("app.datasync.service.vnpy_ingest.get_all_ts_codes")
    def test_sync_all_to_vnpy(self, all_codes, sync_sym, upd):
        from app.datasync.service.vnpy_ingest import sync_all_to_vnpy
        all_codes.return_value = ["000001.SZ"]
        sync_sym.return_value = 10
        s, b = sync_all_to_vnpy()
        assert s == 1 and b == 10

    @patch("app.datasync.service.vnpy_ingest.get_all_ts_codes")
    def test_sync_all_no_codes(self, all_codes):
        from app.datasync.service.vnpy_ingest import sync_all_to_vnpy
        all_codes.return_value = []
        s, b = sync_all_to_vnpy()
        assert s == 0 and b == 0

    @patch("app.datasync.service.vnpy_ingest.update_bar_overview")
    @patch("app.datasync.service.vnpy_ingest.sync_symbol_to_vnpy")
    def test_sync_all_explicit_codes(self, sync_sym, upd):
        from app.datasync.service.vnpy_ingest import sync_all_to_vnpy
        sync_sym.return_value = 5
        s, b = sync_all_to_vnpy(ts_codes=["000001.SZ", "000002.SZ"])
        assert s == 2 and b == 10

    @patch("app.datasync.service.vnpy_ingest.update_bar_overview")
    @patch("app.datasync.service.vnpy_ingest.sync_symbol_to_vnpy")
    def test_sync_all_with_error(self, sync_sym, upd):
        from app.datasync.service.vnpy_ingest import sync_all_to_vnpy
        sync_sym.side_effect = [5, RuntimeError("fail")]
        s, b = sync_all_to_vnpy(ts_codes=["000001.SZ", "000002.SZ"])
        assert s == 1 and b == 5


# ═══════════════════════════════════════════════════════════════════════
# akshare_ingest.py  (35 miss → ~0)
# ═══════════════════════════════════════════════════════════════════════

class TestAkshareIngest:
    def test_env_rate_default(self):
        from app.datasync.service.akshare_ingest import _env_rate
        with patch.dict(os.environ, {}, clear=False):
            r = _env_rate("test_api", 30)
            assert r == 30

    def test_env_rate_custom(self):
        from app.datasync.service.akshare_ingest import _env_rate
        with patch.dict(os.environ, {"AKSHARE_RATE_test": "100"}, clear=False):
            r = _env_rate("test", 30)
            assert r == 100

    def test_env_rate_invalid(self):
        from app.datasync.service.akshare_ingest import _env_rate
        with patch.dict(os.environ, {"AKSHARE_RATE_bad": "abc"}, clear=False):
            r = _env_rate("bad", 30)
            assert r == 30

    def test_min_interval(self):
        from app.datasync.service.akshare_ingest import _min_interval_for
        iv = _min_interval_for("stock_zh_index_daily")
        assert iv > 0

    @patch("app.datasync.service.akshare_ingest.audit_finish")
    @patch("app.datasync.service.akshare_ingest.audit_start", return_value=1)
    @patch("app.datasync.service.akshare_ingest.upsert_index_daily_rows")
    @patch("app.datasync.service.akshare_ingest.call_ak")
    def test_ingest_index_daily_success(self, call_ak_fn, upsert, a_start, a_finish):
        from app.datasync.service.akshare_ingest import ingest_index_daily
        import pandas as pd
        call_ak_fn.return_value = pd.DataFrame({
            "date": ["2024-01-01", "2024-01-02"],
            "open": [3000, 3010], "high": [3050, 3060],
            "low": [2990, 3000], "close": [3020, 3030],
            "volume": [1000000, 1100000],
        })
        upsert.return_value = 2
        r = ingest_index_daily("sh000300")
        assert r == 2

    @patch("app.datasync.service.akshare_ingest.audit_finish")
    @patch("app.datasync.service.akshare_ingest.audit_start", return_value=1)
    @patch("app.datasync.service.akshare_ingest.call_ak")
    def test_ingest_index_daily_empty(self, call_ak_fn, a_start, a_finish):
        from app.datasync.service.akshare_ingest import ingest_index_daily
        import pandas as pd
        call_ak_fn.return_value = pd.DataFrame()
        r = ingest_index_daily("sh000300")
        assert r == 0

    @patch("app.datasync.service.akshare_ingest.audit_finish")
    @patch("app.datasync.service.akshare_ingest.audit_start", return_value=1)
    @patch("app.datasync.service.akshare_ingest.call_ak")
    def test_ingest_index_daily_error(self, call_ak_fn, a_start, a_finish):
        from app.datasync.service.akshare_ingest import ingest_index_daily
        call_ak_fn.side_effect = RuntimeError("API down")
        with pytest.raises(RuntimeError):
            ingest_index_daily("sh000300")
        a_finish.assert_called_once()

    @patch("app.datasync.service.akshare_ingest.ingest_index_daily")
    def test_ingest_all_indexes(self, ingest):
        from app.datasync.service.akshare_ingest import ingest_all_indexes
        ingest.side_effect = [10, 20, RuntimeError("fail"), 30, 40, 50]
        r = ingest_all_indexes()
        assert any(v["status"] == "error" for v in r.values())
        assert any(v["status"] == "success" for v in r.values())

    def test_call_ak_success(self):
        from app.datasync.service.akshare_ingest import call_ak
        fn = MagicMock(return_value="data")
        r = call_ak("test_api", fn)
        assert r == "data"

    def test_call_ak_retry_on_rate_limit(self):
        from app.datasync.service.akshare_ingest import call_ak
        fn = MagicMock(side_effect=[Exception("429 rate limit"), "data"])
        r = call_ak("test_api", fn, max_retries=2, backoff_base=0)
        assert r == "data"

    def test_call_ak_exhausted_retries(self):
        from app.datasync.service.akshare_ingest import call_ak
        fn = MagicMock(side_effect=Exception("persistent error"))
        with pytest.raises(Exception, match="persistent error"):
            call_ak("test_api", fn, max_retries=1, backoff_base=0)

    def test_set_metrics_hook(self):
        from app.datasync.service.akshare_ingest import set_metrics_hook, call_ak
        hook = MagicMock()
        set_metrics_hook(hook)
        fn = MagicMock(return_value="ok")
        call_ak("test_api", fn)
        hook.assert_called_once()
        # cleanup
        set_metrics_hook(None)

    @patch("app.datasync.service.akshare_ingest.audit_finish")
    @patch("app.datasync.service.akshare_ingest.audit_start", return_value=1)
    @patch("app.datasync.service.akshare_ingest.upsert_index_daily_rows")
    @patch("app.datasync.service.akshare_ingest.call_ak")
    def test_ingest_index_with_start_date(self, call_ak_fn, upsert, a_start, a_finish):
        from app.datasync.service.akshare_ingest import ingest_index_daily
        import pandas as pd
        call_ak_fn.return_value = pd.DataFrame({
            "date": ["2024-01-01", "2024-06-01"],
            "open": [3000, 3100], "high": [3050, 3150],
            "low": [2990, 3090], "close": [3020, 3120],
            "volume": [1000000, 1200000],
        })
        upsert.return_value = 1
        r = ingest_index_daily("sh000300", start_date="2024-05-01")
        assert r == 1  # filter by start_date


# ═══════════════════════════════════════════════════════════════════════
# api/main.py  (54 miss → ~0)
# ═══════════════════════════════════════════════════════════════════════

class TestApiMain:
    def _client(self):
        from app.api.main import app
        from app.api.services.auth_service import get_current_user
        from app.api.models.user import TokenData
        app.dependency_overrides[get_current_user] = lambda: TokenData(
            user_id=1, username="u", exp=datetime(2099, 1, 1))
        from fastapi.testclient import TestClient
        return TestClient(app, raise_server_exceptions=False)

    def test_root(self):
        c = self._client()
        r = c.get("/")
        assert r.status_code == 200
        assert "status" in r.json()

    def test_api_info(self):
        c = self._client()
        r = c.get("/api")
        # /api should redirect to /api/v1/
        assert r.status_code in (200, 307)

    def test_health(self):
        c = self._client()
        with patch("app.api.main.get_quantmate_engine", create=True) as eng_mock:
            # Health endpoint checks mysql and redis
            r = c.get("/health")
            assert r.status_code in (200, 503)

    def test_metrics(self):
        # Mock prometheus_client before importing metrics module
        import sys
        fake_prom = MagicMock()
        with patch.dict(sys.modules, {"prometheus_client": fake_prom}):
            # Remove cached module if present
            sys.modules.pop("app.datasync.metrics", None)
            import app.datasync.metrics  # noqa: F401
            with patch.object(app.datasync.metrics, "get_metrics", return_value="# HELP test\n"):
                c = self._client()
                r = c.get("/metrics")
                assert r.status_code == 200

    def test_legacy_redirect(self):
        c = self._client()
        r = c.get("/api/strategies", follow_redirects=False)
        assert r.status_code == 307
        assert "/api/v1/" in r.headers.get("location", "")

    def test_ensure_password_changed_exempt(self):
        from app.api.main import ensure_password_changed
        from unittest.mock import AsyncMock
        import asyncio
        req = MagicMock()
        req.url.path = "/api/v1/auth/login"
        # Should not raise
        asyncio.run(ensure_password_changed(req, None))

    def test_ensure_password_changed_no_creds(self):
        from app.api.main import ensure_password_changed
        import asyncio
        req = MagicMock()
        req.url.path = "/api/v1/strategies"
        asyncio.run(ensure_password_changed(req, None))

    def test_ensure_password_changed_bad_token(self):
        from app.api.main import ensure_password_changed
        from app.api.exception_handlers import APIError
        import asyncio
        req = MagicMock()
        req.url.path = "/api/v1/strategies"
        creds = MagicMock()
        creds.credentials = "invalid_token"
        with pytest.raises(APIError):
            asyncio.run(ensure_password_changed(req, creds))

    def test_ensure_password_changed_must_change(self):
        from app.api.main import ensure_password_changed
        from app.api.exception_handlers import APIError
        from app.api.models.user import TokenData
        import asyncio
        req = MagicMock()
        req.url.path = "/api/v1/strategies"
        creds = MagicMock()
        creds.credentials = "valid"
        td = TokenData(user_id=1, username="newuser", exp=datetime(2099, 1, 1),
                      must_change_password=True)
        with patch("app.api.services.auth_service.decode_token", return_value=td):
            with pytest.raises(APIError) as exc_info:
                asyncio.run(ensure_password_changed(req, creds))
            assert "Password change required" in str(exc_info.value.message)

    def test_ensure_password_changed_admin_exempt_from_change(self):
        from app.api.main import ensure_password_changed
        from app.api.models.user import TokenData
        import asyncio
        req = MagicMock()
        req.url.path = "/api/v1/strategies"
        creds = MagicMock()
        creds.credentials = "valid"
        td = TokenData(user_id=1, username="admin", exp=datetime(2099, 1, 1),
                      must_change_password=True)
        with patch("app.api.services.auth_service.decode_token", return_value=td), \
             patch.dict(os.environ, {"ADMIN_USERNAME": "admin"}):
            # Should not raise — admin is exempt
            asyncio.run(ensure_password_changed(req, creds))


# ═══════════════════════════════════════════════════════════════════════
# backtest_service.py extra  (70 miss → ~0)
# ═══════════════════════════════════════════════════════════════════════

class TestBacktestServiceExtra:
    """Additional coverage for BacktestService methods not covered previously."""

    def test_convert_to_tushare_symbol(self):
        from app.api.services.backtest_service import convert_to_tushare_symbol
        assert convert_to_tushare_symbol("000001.SZSE") == "000001.SZ"
        assert convert_to_tushare_symbol("600000.SSE") == "600000.SH"
        assert convert_to_tushare_symbol("830001.BSE") == "830001.BJ"
        assert convert_to_tushare_symbol("AAPL") == "AAPL"  # no dot
        assert convert_to_tushare_symbol("") == ""
        assert convert_to_tushare_symbol("000001.SZ") == "000001.SZ"

    def test_calculate_alpha_beta_short(self):
        from app.api.services.backtest_service import calculate_alpha_beta
        import numpy as np
        a, b = calculate_alpha_beta(np.array([0.01]), np.array([0.02]))
        assert a is None and b is None

    def test_calculate_alpha_beta_ok(self):
        from app.api.services.backtest_service import calculate_alpha_beta
        import numpy as np
        sr = np.array([0.01, 0.02, 0.03, -0.01, 0.005])
        br = np.array([0.005, 0.015, 0.02, -0.005, 0.003])
        a, b = calculate_alpha_beta(sr, br)
        assert a is not None and b is not None

    def test_calculate_alpha_beta_nan(self):
        from app.api.services.backtest_service import calculate_alpha_beta
        import numpy as np
        sr = np.array([float('nan'), float('nan')])
        br = np.array([0.01, 0.02])
        a, b = calculate_alpha_beta(sr, br)
        assert a is None and b is None

    @patch("app.api.services.backtest_service.AkshareBenchmarkDao")
    def test_get_benchmark_data_ok(self, dao_cls):
        from app.api.services.backtest_service import get_benchmark_data
        dao_cls.return_value.get_benchmark_data.return_value = {"returns": [], "total_return": 0.1}
        r = get_benchmark_data(date(2024, 1, 1), date(2024, 6, 1))
        assert r is not None

    @patch("app.api.services.backtest_service.AkshareBenchmarkDao")
    def test_get_benchmark_data_error(self, dao_cls):
        from app.api.services.backtest_service import get_benchmark_data
        dao_cls.return_value.get_benchmark_data.side_effect = RuntimeError("fail")
        r = get_benchmark_data(date(2024, 1, 1), date(2024, 6, 1))
        assert r is None

    @patch("app.api.services.backtest_service.MarketService")
    def test_get_stock_name_ok(self, ms):
        from app.api.services.backtest_service import get_stock_name
        ms.return_value.resolve_symbol_name.return_value = "平安银行"
        assert get_stock_name("000001.SZ") == "平安银行"

    @patch("app.api.services.backtest_service.MarketService")
    def test_get_stock_name_error(self, ms):
        from app.api.services.backtest_service import get_stock_name
        ms.return_value.resolve_symbol_name.side_effect = RuntimeError("fail")
        assert get_stock_name("000001.SZ") is None

    @patch("app.api.services.backtest_service.update_bar_overview")
    @patch("app.api.services.backtest_service.sync_symbol_to_vnpy")
    def test_ensure_vnpy_history_synced(self, sync, upd):
        from app.api.services.backtest_service import ensure_vnpy_history_data
        sync.return_value = 10
        r = ensure_vnpy_history_data("000001.SZSE", date(2024, 1, 1))
        assert r == 10
        upd.assert_called_once()

    @patch("app.api.services.backtest_service.sync_symbol_to_vnpy")
    def test_ensure_vnpy_history_no_data(self, sync):
        from app.api.services.backtest_service import ensure_vnpy_history_data
        sync.return_value = 0
        r = ensure_vnpy_history_data("000001.SZSE", date(2024, 1, 1))
        assert r == 0

    def test_ensure_vnpy_history_invalid_symbol(self):
        from app.api.services.backtest_service import ensure_vnpy_history_data
        r = ensure_vnpy_history_data("INVALID", date(2024, 1, 1))
        assert r == 0

    @patch("app.api.services.backtest_service.get_job_storage")
    def test_service_v2_cancel_job_wrong_user(self, storage):
        from app.api.services.backtest_service import BacktestServiceV2
        svc = BacktestServiceV2.__new__(BacktestServiceV2)
        svc.job_storage = storage.return_value
        svc.job_storage.get_job_metadata.return_value = {"user_id": 99}
        assert svc.cancel_job("bt_123", 1) is False

    @patch("app.api.services.backtest_service.get_job_storage")
    def test_service_v2_cancel_job_not_found(self, storage):
        from app.api.services.backtest_service import BacktestServiceV2
        svc = BacktestServiceV2.__new__(BacktestServiceV2)
        svc.job_storage = storage.return_value
        svc.job_storage.get_job_metadata.return_value = None
        assert svc.cancel_job("bt_123", 1) is False

    @patch("app.api.services.backtest_service.get_queue")
    @patch("app.api.services.backtest_service.get_job_storage")
    def test_service_v2_cancel_optimization(self, storage, get_q):
        from app.api.services.backtest_service import BacktestServiceV2
        svc = BacktestServiceV2.__new__(BacktestServiceV2)
        svc.job_storage = storage.return_value
        svc.job_storage.get_job_metadata.return_value = {"user_id": 1, "type": "optimization"}
        svc.job_storage.cancel_job.return_value = True
        assert svc.cancel_job("opt_123", 1) is True
        get_q.assert_called_with("optimization")

    def test_get_strategy_from_db_not_found(self):
        from app.api.services.backtest_service import BacktestServiceV2
        svc = BacktestServiceV2.__new__(BacktestServiceV2)
        with patch("app.api.services.backtest_service.StrategySourceDao") as Dao:
            Dao.return_value.get_strategy_source_for_user.side_effect = KeyError("nope")
            with pytest.raises(ValueError, match="not found"):
                svc._get_strategy_from_db(999, 1)
