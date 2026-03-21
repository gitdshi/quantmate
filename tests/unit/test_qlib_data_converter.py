"""Tests for Qlib data converter (tushare/akshare → Qlib binary format)."""
import pytest
import os
import struct
import tempfile
from unittest.mock import patch, MagicMock
from datetime import date

import pandas as pd
import numpy as np


# Pre-mock qlib before import only when pyqlib is unavailable.
import sys
try:
    import qlib  # noqa: F401
except ImportError:
    sys.modules["qlib"] = MagicMock()
    sys.modules["qlib.config"] = MagicMock()
    sys.modules["qlib.data"] = MagicMock()

from app.infrastructure.qlib.data_converter import (
    _ts_code_to_qlib_instrument,
    fetch_tushare_daily,
    fetch_akshare_daily,
    convert_to_qlib_format,
)


class TestTsCodeMapping:
    """Test tushare ts_code → Qlib instrument mapping."""

    def test_sz_stock(self):
        assert _ts_code_to_qlib_instrument("000001.SZ") == "SZ000001"

    def test_sh_stock(self):
        assert _ts_code_to_qlib_instrument("600000.SH") == "SH600000"

    def test_bj_stock(self):
        assert _ts_code_to_qlib_instrument("430047.BJ") == "BJ430047"

    def test_no_suffix_passthrough(self):
        # If there's no dot, just return as-is
        result = _ts_code_to_qlib_instrument("NODOT")
        assert result == "NODOT"


class TestFetchTushareDaily:
    """Test tushare data fetching."""

    @patch("app.infrastructure.qlib.data_converter.connection")
    def test_fetch_returns_dataframe(self, mock_conn_ctx):
        conn = MagicMock()
        mock_conn_ctx.return_value.__enter__ = MagicMock(return_value=conn)
        mock_conn_ctx.return_value.__exit__ = MagicMock(return_value=False)

        mock_df = pd.DataFrame({
            "ts_code": ["000001.SZ", "000001.SZ"],
            "trade_date": ["20240101", "20240102"],
            "open": [10.0, 10.5],
            "high": [11.0, 11.5],
            "low": [9.5, 10.0],
            "close": [10.5, 11.0],
            "vol": [100000, 120000],
            "amount": [1050000, 1320000],
            "adj_factor": [1.0, 1.0],
        })

        with patch("app.infrastructure.qlib.data_converter.pd.read_sql", return_value=mock_df):
            result = fetch_tushare_daily("2024-01-01", "2024-01-02")
            assert isinstance(result, pd.DataFrame)
            assert len(result) == 2


class TestFetchAkshareDaily:
    """Test akshare data fetching."""

    @patch("app.infrastructure.qlib.data_converter.connection")
    def test_fetch_returns_dataframe(self, mock_conn_ctx):
        conn = MagicMock()
        mock_conn_ctx.return_value.__enter__ = MagicMock(return_value=conn)
        mock_conn_ctx.return_value.__exit__ = MagicMock(return_value=False)

        mock_df = pd.DataFrame({
            "symbol": ["000001", "000002"],
            "trade_date": ["20240101", "20240101"],
            "open": [10.0, 20.0],
            "high": [11.0, 21.0],
            "low": [9.5, 19.5],
            "close": [10.5, 20.5],
            "volume": [100000, 200000],
            "amount": [1050000, 4100000],
        })

        with patch("app.infrastructure.qlib.data_converter.pd.read_sql", return_value=mock_df):
            result = fetch_akshare_daily("2024-01-01", "2024-01-02")
            assert isinstance(result, pd.DataFrame)
            assert len(result) == 2


class TestConvertToQlibFormat:
    """Test binary conversion output."""

    @patch("app.infrastructure.qlib.data_converter._log_conversion")
    @patch("app.infrastructure.qlib.data_converter.fetch_akshare_daily")
    @patch("app.infrastructure.qlib.data_converter.fetch_tushare_daily")
    def test_convert_creates_files(self, mock_tushare, mock_akshare, mock_log):
        """Verify conversion creates correct directory structure."""
        mock_tushare.return_value = pd.DataFrame({
            "ts_code": ["000001.SZ"] * 3,
            "trade_date": ["20240101", "20240102", "20240103"],
            "open": [10.0, 10.5, 11.0],
            "high": [11.0, 11.5, 12.0],
            "low": [9.5, 10.0, 10.5],
            "close": [10.5, 11.0, 11.5],
            "vol": [100000, 120000, 110000],
            "amount": [1050000, 1320000, 1265000],
            "adj_factor": [1.0, 1.0, 1.0],
        })
        mock_akshare.return_value = pd.DataFrame()

        with tempfile.TemporaryDirectory() as tmpdir:
            with patch("app.infrastructure.qlib.data_converter.QLIB_DATA_DIR", tmpdir):
                result = convert_to_qlib_format(
                    start_date="2024-01-01",
                    end_date="2024-01-03",
                )

            assert result["status"] == "completed"
            assert result["instrument_count"] >= 1

            # Check directory structure
            features_dir = os.path.join(tmpdir, "features")
            calendars_dir = os.path.join(tmpdir, "calendars")
            instruments_dir = os.path.join(tmpdir, "instruments")

            assert os.path.isdir(features_dir)
            assert os.path.isdir(calendars_dir)
            assert os.path.isdir(instruments_dir)

            # Check instrument directory exists
            assert os.path.isdir(os.path.join(features_dir, "SZ000001"))

            # Check calendar file
            cal_path = os.path.join(calendars_dir, "day.txt")
            assert os.path.isfile(cal_path)

            # Check instruments file
            inst_path = os.path.join(instruments_dir, "all.txt")
            assert os.path.isfile(inst_path)

    @patch("app.infrastructure.qlib.data_converter._log_conversion")
    @patch("app.infrastructure.qlib.data_converter.fetch_akshare_daily")
    @patch("app.infrastructure.qlib.data_converter.fetch_tushare_daily")
    def test_convert_empty_data(self, mock_tushare, mock_akshare, mock_log):
        """Verify graceful handling when no data is available."""
        mock_tushare.return_value = pd.DataFrame()
        mock_akshare.return_value = pd.DataFrame()

        with tempfile.TemporaryDirectory() as tmpdir:
            with patch("app.infrastructure.qlib.data_converter.QLIB_DATA_DIR", tmpdir):
                result = convert_to_qlib_format()

            assert result["status"] in ("completed", "empty")
