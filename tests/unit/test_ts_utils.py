"""Unit tests for app.utils.ts_utils."""

from __future__ import annotations


import pytest

import app.utils.ts_utils as _mod


class TestMovingAverage:
    def test_basic(self):
        result = _mod.moving_average([1, 2, 3, 4, 5], 3)
        assert len(result) == 5
        # First 2 should be NaN or None, 3rd should be 2.0
        assert result[2] == pytest.approx(2.0)

    def test_window_1(self):
        vals = [10, 20, 30]
        result = _mod.moving_average(vals, 1)
        assert result[0] == pytest.approx(10)
        assert result[2] == pytest.approx(30)

    def test_window_equals_length(self):
        vals = [1, 2, 3]
        result = _mod.moving_average(vals, 3)
        assert result[-1] == pytest.approx(2.0)

    def test_empty_input(self):
        result = _mod.moving_average([], 3)
        assert len(result) == 0


class TestPctChange:
    def test_basic(self):
        result = _mod.pct_change([100, 110, 99], 1)
        assert len(result) == 3
        # First should be NaN or None
        assert result[1] == pytest.approx(0.1)
        assert result[2] == pytest.approx(-0.1, abs=0.01)

    def test_periods_2(self):
        result = _mod.pct_change([100, 110, 121], 2)
        assert len(result) == 3
        assert result[2] == pytest.approx(0.21)

    def test_empty(self):
        result = _mod.pct_change([], 1)
        assert len(result) == 0

    def test_single_value(self):
        result = _mod.pct_change([100], 1)
        assert len(result) == 1


class TestWithPandas:
    """Test that pandas Series is handled if available."""

    def test_moving_average_with_series(self):
        try:
            import pandas as pd
            s = pd.Series([1.0, 2.0, 3.0, 4.0, 5.0])
            result = _mod.moving_average(s, 3)
            assert len(result) == 5
        except ImportError:
            pytest.skip("pandas not installed")

    def test_pct_change_with_series(self):
        try:
            import pandas as pd
            s = pd.Series([100.0, 110.0, 121.0])
            result = _mod.pct_change(s, 1)
            assert len(result) == 3
        except ImportError:
            pytest.skip("pandas not installed")
