"""Unit tests for app.datasync.service.akshare_ingest."""

from __future__ import annotations

from unittest.mock import MagicMock, patch
import sys

import pytest

_MOD = "app.datasync.service.akshare_ingest"


@pytest.fixture(autouse=True)
def _stub_akshare():
    stubs = {}
    if "akshare" not in sys.modules:
        stubs["akshare"] = sys.modules["akshare"] = MagicMock()
    yield
    for name in stubs:
        sys.modules.pop(name, None)


# ── Pure helpers ─────────────────────────────────────────────────

class TestMinInterval:
    def test_known_endpoint(self):
        from app.datasync.service.akshare_ingest import _min_interval_for
        interval = _min_interval_for("stock_zh_index_daily")
        assert interval > 0

    def test_unknown_endpoint(self):
        from app.datasync.service.akshare_ingest import _min_interval_for
        interval = _min_interval_for("nonexistent_api")
        assert interval > 0  # falls back to default


class TestIndexMapping:
    def test_has_codes(self):
        from app.datasync.service.akshare_ingest import INDEX_MAPPING
        assert "sh000300" in INDEX_MAPPING or len(INDEX_MAPPING) > 0


class TestSetMetricsHook:
    def test_sets_hook(self):
        from app.datasync.service.akshare_ingest import set_metrics_hook, call_ak
        mock_hook = MagicMock()
        set_metrics_hook(mock_hook)
        assert call_ak._metrics_hook is mock_hook
        # Clean up
        call_ak._metrics_hook = None


# ── call_ak ──────────────────────────────────────────────────────

class TestCallAk:
    def test_success(self):
        import pandas as pd
        from app.datasync.service.akshare_ingest import call_ak
        df = pd.DataFrame({"close": [100.0]})
        mock_fn = MagicMock(return_value=df)
        result = call_ak("test_api", mock_fn)
        assert len(result) == 1
        mock_fn.assert_called_once()

    def test_retries_on_rate_limit(self):
        import pandas as pd
        from app.datasync.service.akshare_ingest import call_ak
        df = pd.DataFrame({"close": [100.0]})
        mock_fn = MagicMock(side_effect=[
            Exception("429 Too Many Requests"),
            df,
        ])
        with patch(f"{_MOD}.time.sleep"):
            result = call_ak("test_api", mock_fn, max_retries=3, backoff_base=0)
        assert len(result) == 1

    def test_max_retries_exceeded(self):
        from app.datasync.service.akshare_ingest import call_ak
        mock_fn = MagicMock(side_effect=Exception("connection timeout"))
        with patch(f"{_MOD}.time.sleep"), pytest.raises(Exception, match="connection timeout"):
            call_ak("test_api", mock_fn, max_retries=2, backoff_base=0)

    def test_metrics_hook_called(self):
        import pandas as pd
        from app.datasync.service.akshare_ingest import call_ak
        df = pd.DataFrame({"close": [100.0]})
        mock_fn = MagicMock(return_value=df)
        hook = MagicMock()
        call_ak._metrics_hook = hook
        try:
            call_ak("test_api", mock_fn)
            hook.assert_called_once()
        finally:
            call_ak._metrics_hook = None


# ── ingest_index_daily ───────────────────────────────────────────

class TestIngestIndexDaily:
    def test_success(self):
        import pandas as pd
        from app.datasync.service.akshare_ingest import ingest_index_daily
        df = pd.DataFrame({
            "date": pd.to_datetime(["2024-01-05"]),
            "open": [3000.0], "high": [3100.0], "low": [2900.0], "close": [3050.0],
            "volume": [500000.0],
        })
        with patch(f"{_MOD}.call_ak", return_value=df), \
             patch(f"{_MOD}.audit_start", return_value=1), \
             patch(f"{_MOD}.audit_finish"), \
             patch(f"{_MOD}.upsert_index_daily_rows", return_value=1) as mock_upsert:
            result = ingest_index_daily("sh000300", start_date="2024-01-01")
        assert result >= 0

    def test_empty_df(self):
        import pandas as pd
        from app.datasync.service.akshare_ingest import ingest_index_daily
        with patch(f"{_MOD}.call_ak", return_value=pd.DataFrame()), \
             patch(f"{_MOD}.audit_start", return_value=1), \
             patch(f"{_MOD}.audit_finish"):
            result = ingest_index_daily("sh000300")
        assert result == 0


# ── ingest_all_indexes ───────────────────────────────────────────

class TestIngestAllIndexes:
    def test_calls_per_symbol(self):
        from app.datasync.service.akshare_ingest import ingest_all_indexes, INDEX_MAPPING
        with patch(f"{_MOD}.ingest_index_daily", return_value=100) as mock_ingest, \
             patch(f"{_MOD}.time.sleep"):
            result = ingest_all_indexes()
        assert mock_ingest.call_count == len(INDEX_MAPPING)
        assert isinstance(result, dict)

    def test_passes_start_date_to_each_symbol(self):
        from app.datasync.service.akshare_ingest import ingest_all_indexes, INDEX_MAPPING

        with patch(f"{_MOD}.ingest_index_daily", return_value=100) as mock_ingest, \
             patch(f"{_MOD}.time.sleep"):
            result = ingest_all_indexes(start_date="2024-01-01")

        assert isinstance(result, dict)
        assert mock_ingest.call_count == len(INDEX_MAPPING)
        for call in mock_ingest.call_args_list:
            assert call.kwargs["start_date"] == "2024-01-01"
