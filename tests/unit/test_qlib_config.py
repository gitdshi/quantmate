"""Tests for Qlib configuration and initialization helpers."""
import sys
from unittest.mock import patch, MagicMock


# Pre-mock qlib only when the dependency is unavailable.
try:
    import qlib  # noqa: F401
except ImportError:
    sys.modules["qlib"] = MagicMock()
    sys.modules["qlib.config"] = MagicMock()
    sys.modules["qlib.data"] = MagicMock()

from app.infrastructure.qlib.qlib_config import (
    SUPPORTED_MODELS,
    SUPPORTED_DATASETS,
    SUPPORTED_STRATEGIES,
    is_qlib_available,
)


class TestSupportedRegistries:
    """Test that model/dataset/strategy registries are non-empty and well-formed."""

    def test_supported_models_not_empty(self):
        assert len(SUPPORTED_MODELS) >= 1

    def test_supported_models_has_lightgbm(self):
        assert "LightGBM" in SUPPORTED_MODELS

    def test_supported_models_values_are_strings(self):
        for name, cls_path in SUPPORTED_MODELS.items():
            assert isinstance(name, str)
            assert isinstance(cls_path, str)
            assert "." in cls_path  # should be a dotted import path

    def test_supported_datasets_not_empty(self):
        assert len(SUPPORTED_DATASETS) >= 1

    def test_supported_datasets_has_alpha158(self):
        assert "Alpha158" in SUPPORTED_DATASETS

    def test_supported_strategies_not_empty(self):
        assert len(SUPPORTED_STRATEGIES) >= 1


class TestQlibAvailability:

    @patch.dict(sys.modules, {"qlib": MagicMock()})
    def test_qlib_available_when_imported(self):
        assert is_qlib_available() is True

    def test_qlib_unavailable_when_import_fails(self):
        with patch.dict(sys.modules, {"qlib": None}):
            with patch("builtins.__import__", side_effect=ImportError):
                assert is_qlib_available() is False
