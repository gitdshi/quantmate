"""Unit tests for worker/service/tasks.py utility functions.

Only tests pure utility functions that don't depend on vnpy/rq.
The entire tasks module imports vnpy at module level, so we must mock it.
"""

from __future__ import annotations

from unittest.mock import MagicMock
import sys

import pytest
import numpy as np


# ── Provide heavy mocks before importing ──────────────────────────
# tasks.py imports vnpy, rq, etc. at module level.
# We stub them out so we can test utility functions.

_vnpy_mocks = {
    "rq": MagicMock(),
    "vnpy": MagicMock(),
    "vnpy.trader": MagicMock(),
    "vnpy.trader.constant": MagicMock(),
    "vnpy.trader.optimize": MagicMock(),
    "vnpy.trader.setting": MagicMock(SETTINGS={}),
    "vnpy_ctastrategy": MagicMock(),
    "vnpy_ctastrategy.backtesting": MagicMock(),
}


@pytest.fixture(autouse=True)
def _mock_vnpy_imports(monkeypatch):
    """Inject stubs for vnpy/rq before tasks module loads."""
    for mod_name, mock_obj in _vnpy_mocks.items():
        monkeypatch.setitem(sys.modules, mod_name, mock_obj)
    # Also mock some app-level imports that tasks.py uses
    monkeypatch.setitem(sys.modules, "app.datasync.service.vnpy_ingest", MagicMock())
    monkeypatch.setitem(sys.modules, "app.api.services.job_storage_service", MagicMock())
    monkeypatch.setitem(sys.modules, "app.domains.market.service", MagicMock())
    yield


def _import_tasks():
    """Import tasks module after mocks are in place."""
    import importlib
    if "app.worker.service.tasks" in sys.modules:
        importlib.reload(sys.modules["app.worker.service.tasks"])
    import app.worker.service.tasks as tasks
    return tasks


# ── convert_to_vnpy_symbol ────────────────────────────────────────

def test_convert_to_vnpy_symbol_sz():
    tasks = _import_tasks()
    assert tasks.convert_to_vnpy_symbol("000001.SZ") == "000001.SZSE"


def test_convert_to_vnpy_symbol_sh():
    tasks = _import_tasks()
    assert tasks.convert_to_vnpy_symbol("600000.SH") == "600000.SSE"


def test_convert_to_vnpy_symbol_bj():
    tasks = _import_tasks()
    assert tasks.convert_to_vnpy_symbol("430047.BJ") == "430047.BSE"


def test_convert_to_vnpy_symbol_already_vnpy():
    tasks = _import_tasks()
    assert tasks.convert_to_vnpy_symbol("000001.SZSE") == "000001.SZSE"


def test_convert_to_vnpy_symbol_no_dot():
    tasks = _import_tasks()
    assert tasks.convert_to_vnpy_symbol("000001") == "000001"


def test_convert_to_vnpy_symbol_empty():
    tasks = _import_tasks()
    assert tasks.convert_to_vnpy_symbol("") == ""


def test_convert_to_vnpy_symbol_none():
    tasks = _import_tasks()
    assert tasks.convert_to_vnpy_symbol(None) is None


# ── convert_to_tushare_symbol ─────────────────────────────────────

def test_convert_to_tushare_symbol_szse():
    tasks = _import_tasks()
    assert tasks.convert_to_tushare_symbol("000001.SZSE") == "000001.SZ"


def test_convert_to_tushare_symbol_sse():
    tasks = _import_tasks()
    assert tasks.convert_to_tushare_symbol("600000.SSE") == "600000.SH"


def test_convert_to_tushare_symbol_bse():
    tasks = _import_tasks()
    assert tasks.convert_to_tushare_symbol("430047.BSE") == "430047.BJ"


def test_convert_to_tushare_symbol_already_tushare():
    tasks = _import_tasks()
    assert tasks.convert_to_tushare_symbol("000001.SZ") == "000001.SZ"


def test_convert_to_tushare_symbol_no_dot():
    tasks = _import_tasks()
    assert tasks.convert_to_tushare_symbol("000001") == "000001"


# ── calculate_alpha_beta_for_worker ───────────────────────────────

def test_alpha_beta_normal():
    tasks = _import_tasks()
    strat = np.array([0.01, 0.02, 0.03, -0.01, 0.015])
    bench = np.array([0.005, 0.01, 0.02, -0.005, 0.01])
    alpha, beta = tasks.calculate_alpha_beta_for_worker(strat, bench)
    assert alpha is not None
    assert beta is not None
    assert isinstance(alpha, float)
    assert isinstance(beta, float)


def test_alpha_beta_short_arrays():
    tasks = _import_tasks()
    strat = np.array([0.01])
    bench = np.array([0.005])
    alpha, beta = tasks.calculate_alpha_beta_for_worker(strat, bench)
    assert alpha is None
    assert beta is None


def test_alpha_beta_with_nans():
    tasks = _import_tasks()
    strat = np.array([0.01, np.nan, 0.03])
    bench = np.array([0.005, 0.01, np.nan])
    alpha, beta = tasks.calculate_alpha_beta_for_worker(strat, bench)
    # Only 1 valid pair after NaN removal → too short
    assert alpha is None
    assert beta is None


def test_alpha_beta_different_lengths():
    tasks = _import_tasks()
    strat = np.array([0.01, 0.02, 0.03, 0.04])
    bench = np.array([0.005, 0.01, 0.02])
    alpha, beta = tasks.calculate_alpha_beta_for_worker(strat, bench)
    assert alpha is not None


# ── _normalize_optimization_results ───────────────────────────────

def test_normalize_optimization_results():
    tasks = _import_tasks()
    raw = [
        (
            {"fast_window": 5, "slow_window": 20},
            1.5,
            {"sharpe_ratio": 1.5, "total_return": 0.15, "annual_return": 0.12,
             "max_drawdown": -0.05, "max_ddpercent": -5.0, "calmar_ratio": 2.4},
        ),
    ]
    result = tasks._normalize_optimization_results(raw, "sharpe_ratio")
    assert len(result) == 1
    assert result[0]["rank_order"] == 1
    assert result[0]["parameters"]["fast_window"] == 5
    assert result[0]["statistics"]["sharpe_ratio"] == 1.5


def test_normalize_optimization_results_empty():
    tasks = _import_tasks()
    assert tasks._normalize_optimization_results([], "sharpe_ratio") == []


def test_normalize_optimization_results_bad_rows():
    tasks = _import_tasks()
    raw = [
        "not a tuple",
        (1, 2),  # too short
        ({"a": 1}, 1.0, {"sharpe_ratio": 1.0}),  # valid
    ]
    result = tasks._normalize_optimization_results(raw, "sharpe_ratio")
    assert len(result) == 1


def test_normalize_optimization_results_non_dict_params():
    tasks = _import_tasks()
    raw = [("not_a_dict", 1.0, {"sharpe_ratio": 1.0})]
    result = tasks._normalize_optimization_results(raw, "sharpe_ratio")
    assert result[0]["parameters"] == {}


def test_normalize_optimization_results_non_dict_stats():
    tasks = _import_tasks()
    raw = [({"a": 1}, 1.0, "not_dict")]
    result = tasks._normalize_optimization_results(raw, "sharpe_ratio")
    # When stats is not a dict → becomes {}, so get(objective_metric, target_value) falls back to target_value
    assert result[0]["statistics"]["target_value"] == 1.0
