"""Unit tests for factor unified backtest task."""

from __future__ import annotations

from unittest.mock import patch

import pandas as pd

from app.domains.factors.backtest_task import run_factor_backtest_task


def _sample_ohlcv() -> pd.DataFrame:
    index = pd.MultiIndex.from_product(
        [
            ["000001.SZ", "000002.SZ"],
            pd.to_datetime(["2024-01-02", "2024-01-03", "2024-01-04"]),
        ],
        names=["instrument", "date"],
    )
    return pd.DataFrame(
        {
            "open": [10.0, 11.0, 12.0, 20.0, 19.0, 18.0],
            "high": [10.5, 11.5, 12.5, 20.5, 19.5, 18.5],
            "low": [9.5, 10.5, 11.5, 19.5, 18.5, 17.5],
            "close": [10.0, 11.0, 12.0, 20.0, 19.0, 18.0],
            "volume": [1000.0, 1100.0, 1200.0, 2000.0, 1900.0, 1800.0],
            "amount": [10000.0, 12100.0, 14400.0, 40000.0, 36100.0, 32400.0],
            "factor": [1.0, 1.0, 1.0, 1.0, 1.0, 1.0],
        },
        index=index,
    )


def _sample_market_data() -> dict[str, dict[str, dict[str, float]]]:
    return {
        "2024-01-02": {
            "000001.SZ": {"open": 10.0, "high": 10.5, "low": 9.5, "close": 10.0, "volume": 1000.0, "prev_close": 10.0},
            "000002.SZ": {"open": 20.0, "high": 20.5, "low": 19.5, "close": 20.0, "volume": 2000.0, "prev_close": 20.0},
        },
        "2024-01-03": {
            "000001.SZ": {"open": 11.0, "high": 11.5, "low": 10.5, "close": 11.0, "volume": 1100.0, "prev_close": 10.0},
            "000002.SZ": {"open": 19.0, "high": 19.5, "low": 18.5, "close": 19.0, "volume": 1900.0, "prev_close": 20.0},
        },
        "2024-01-04": {
            "000001.SZ": {"open": 12.0, "high": 12.5, "low": 11.5, "close": 12.0, "volume": 1200.0, "prev_close": 11.0},
            "000002.SZ": {"open": 18.0, "high": 18.5, "low": 17.5, "close": 18.0, "volume": 1800.0, "prev_close": 19.0},
        },
    }


@patch("app.domains.factors.backtest_task.BacktestHistoryDao")
@patch("app.domains.composite.tasks._load_benchmark_data")
@patch("app.domains.composite.tasks._load_market_data")
@patch("app.domains.factors.backtest_task.fetch_ohlcv")
@patch("app.domains.factors.backtest_task.FactorService")
def test_run_factor_backtest_task_success(
    MockFactorService,
    mock_fetch_ohlcv,
    mock_load_market_data,
    mock_load_benchmark_data,
    MockHistoryDao,
):
    MockFactorService.return_value.get_factor.return_value = {
        "id": 7,
        "name": "Quality",
        "expression": "close",
    }
    mock_fetch_ohlcv.return_value = _sample_ohlcv()
    mock_load_market_data.return_value = _sample_market_data()
    mock_load_benchmark_data.return_value = {
        "2024-01-02": 100.0,
        "2024-01-03": 101.0,
        "2024-01-04": 102.0,
    }

    result = run_factor_backtest_task(
        "factor-job-1",
        1,
        {
            "subject_type": "factor",
            "subject_id": 7,
            "subject_name": "Quality",
            "start_date": "2024-01-02",
            "end_date": "2024-01-04",
            "benchmark": "000300.SH",
            "initial_capital": 100000.0,
            "costs": {},
            "profile": {
                "instruments": ["000001.SZ", "000002.SZ"],
                "top_n": 1,
                "max_position_pct": 1.0,
            },
        },
    )

    assert result["status"] == "completed"
    assert MockHistoryDao.return_value.upsert_history.call_count == 2
    assert MockHistoryDao.return_value.upsert_history.call_args_list[-1].kwargs["status"] == "completed"