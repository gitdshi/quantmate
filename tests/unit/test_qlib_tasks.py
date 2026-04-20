"""Tests for Qlib background worker tasks."""
import sys
from datetime import date
from unittest.mock import patch, MagicMock

# Pre-mock qlib only when the dependency is unavailable.
try:
    import qlib  # noqa: F401
except ImportError:
    sys.modules["qlib"] = MagicMock()
    sys.modules["qlib.config"] = MagicMock()
    sys.modules["qlib.data"] = MagicMock()
    sys.modules["qlib.data.dataset"] = MagicMock()
    sys.modules["qlib.utils"] = MagicMock()
    sys.modules["scipy"] = MagicMock()
    sys.modules["scipy.stats"] = MagicMock()

from app.worker.service.qlib_tasks import (
    run_qlib_training_task,
    run_data_conversion_task,
)


class TestRunQlibTrainingTask:

    @patch("app.worker.service.qlib_tasks.QlibModelService")
    def test_delegates_to_service(self, MockService):
        MockService.return_value.train_model.return_value = {
            "run_id": 1, "status": "completed",
        }
        result = run_qlib_training_task(
            user_id=1,
            model_type="LightGBM",
            factor_set="Alpha158",
            universe="csi300",
            train_start="2020-01-01",
            train_end="2023-12-31",
        )
        MockService.return_value.train_model.assert_called_once()
        assert result["status"] == "completed"

    @patch("app.worker.service.qlib_tasks.QlibModelService")
    def test_handles_exception(self, MockService):
        MockService.return_value.train_model.side_effect = Exception("Training failed")
        result = run_qlib_training_task(
            user_id=1, model_type="LightGBM", factor_set="Alpha158",
        )
        # Should not raise, should return error info
        assert "error" in result or result.get("status") == "failed"


class TestRunDataConversionTask:

    @patch("app.worker.service.qlib_tasks.convert_to_qlib_format")
    def test_delegates_to_converter(self, mock_convert):
        mock_convert.return_value = {
            "status": "completed", "instrument_count": 100,
        }
        result = run_data_conversion_task(
            start_date="2023-01-01",
            end_date="2024-12-31",
        )
        mock_convert.assert_called_once_with(
            start_date=date(2023, 1, 1),
            end_date=date(2024, 12, 31),
            use_akshare_supplement=False,
        )
        assert result["status"] == "completed"

    @patch("app.worker.service.qlib_tasks.convert_to_qlib_format")
    def test_with_akshare_supplement(self, mock_convert):
        mock_convert.return_value = {"status": "completed", "instrument_count": 200}
        result = run_data_conversion_task(use_akshare_supplement=True)
        mock_convert.assert_called_once()
        call_kwargs = mock_convert.call_args
        assert call_kwargs[1].get("use_akshare_supplement") is True or \
               (len(call_kwargs[0]) > 0 and True in call_kwargs[0])
