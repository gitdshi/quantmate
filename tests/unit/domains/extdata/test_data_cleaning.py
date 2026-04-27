"""Tests for DataCleaningService."""
import pytest
from unittest.mock import patch, MagicMock
from datetime import date

from app.domains.extdata.data_cleaning_service import DataCleaningService, _safe_table, _quality_score


class TestDataCleaningService:
    """Tests for data_cleaning_service."""

    @patch("app.domains.extdata.data_cleaning_service.connection")
    def test_detect_missing_dates_all_present(self, mock_conn_ctx):
        conn = MagicMock()
        mock_conn_ctx.return_value.__enter__ = MagicMock(return_value=conn)
        mock_conn_ctx.return_value.__exit__ = MagicMock(return_value=False)
        # Calendar has 3 trading days
        conn.execute.side_effect = [
            MagicMock(fetchall=MagicMock(return_value=[("20240101",), ("20240102",), ("20240103",)])),
            MagicMock(fetchall=MagicMock(return_value=[("20240101",), ("20240102",), ("20240103",)])),
        ]
        svc = DataCleaningService()
        result = svc.detect_missing_dates("000001.SZ", date(2024, 1, 1), date(2024, 1, 3))
        assert result["missing_days"] == 0
        assert result["completeness"] == 1.0

    @patch("app.domains.extdata.data_cleaning_service.connection")
    def test_detect_missing_dates_with_gaps(self, mock_conn_ctx):
        conn = MagicMock()
        mock_conn_ctx.return_value.__enter__ = MagicMock(return_value=conn)
        mock_conn_ctx.return_value.__exit__ = MagicMock(return_value=False)
        conn.execute.side_effect = [
            MagicMock(fetchall=MagicMock(return_value=[("20240101",), ("20240102",), ("20240103",)])),
            MagicMock(fetchall=MagicMock(return_value=[("20240101",)])),
        ]
        svc = DataCleaningService()
        result = svc.detect_missing_dates("000001.SZ", date(2024, 1, 1), date(2024, 1, 3))
        assert result["missing_days"] == 2
        assert len(result["missing_dates"]) == 2

    @patch("app.domains.extdata.data_cleaning_service.connection")
    def test_detect_price_anomalies(self, mock_conn_ctx):
        conn = MagicMock()
        mock_conn_ctx.return_value.__enter__ = MagicMock(return_value=conn)
        mock_conn_ctx.return_value.__exit__ = MagicMock(return_value=False)
        Row = type("Row", (), {"_mapping": None})
        rows = []
        for d, pct in [("20240101", 1.5), ("20240102", 25.0), ("20240103", -22.0)]:
            r = Row()
            r._mapping = {"trade_date": d, "open": 10, "high": 11, "low": 9, "close": 10, "pct_chg": pct}
            rows.append(r)
        conn.execute.return_value.fetchall.return_value = rows
        svc = DataCleaningService()
        result = svc.detect_price_anomalies("000001.SZ", threshold_pct=20.0)
        assert result["anomaly_count"] == 2

    @patch("app.domains.extdata.data_cleaning_service.connection")
    def test_check_ohlc_consistency(self, mock_conn_ctx):
        conn = MagicMock()
        mock_conn_ctx.return_value.__enter__ = MagicMock(return_value=conn)
        mock_conn_ctx.return_value.__exit__ = MagicMock(return_value=False)
        Row = type("Row", (), {"_mapping": None})
        # Valid row
        r1 = Row(); r1._mapping = {"trade_date": "20240101", "open": 10, "high": 11, "low": 9, "close": 10.5}
        # Invalid: high < close
        r2 = Row(); r2._mapping = {"trade_date": "20240102", "open": 10, "high": 9, "low": 8, "close": 10.5}
        conn.execute.return_value.fetchall.return_value = [r1, r2]
        svc = DataCleaningService()
        result = svc.check_ohlc_consistency("000001.SZ")
        assert result["violation_count"] == 1

    def test_safe_table_valid(self):
        assert _safe_table("stock_daily") == "stock_daily"

    def test_safe_table_invalid(self):
        with pytest.raises(ValueError):
            _safe_table("malicious_table; DROP TABLE")

    def test_quality_score(self):
        missing = {"completeness": 1.0}
        anomalies = {"anomaly_count": 0}
        ohlc = {"violation_count": 0}
        assert _quality_score(missing, anomalies, ohlc) == 100.0

    def test_quality_score_with_issues(self):
        missing = {"completeness": 0.8}
        anomalies = {"anomaly_count": 2}
        ohlc = {"violation_count": 1}
        score = _quality_score(missing, anomalies, ohlc)
        assert 0 < score < 100
