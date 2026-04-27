"""Unit tests for app.worker.service.run_worker."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest


class TestRunWorker:
    def test_main_creates_worker(self):
        """Verify main() creates a Worker and calls work()."""
        import app.worker.service.run_worker as _mod
        fake_queue = MagicMock()
        worker_instance = MagicMock()

        # We must also patch the 'from rq import Worker' that main() does lazily
        mock_worker_cls = MagicMock(return_value=worker_instance)
        with patch.object(_mod, "QUEUES", {"default": fake_queue, "backtest": fake_queue}), \
             patch("sys.argv", ["run_worker", "default"]), \
             patch.dict("sys.modules", {"rq": MagicMock(Worker=mock_worker_cls)}):
            # Re-run main which does `from rq import Worker` inside
            _mod.main()
            mock_worker_cls.assert_called_once()
            worker_instance.work.assert_called_once()

    def test_main_uses_rdagent_queue_by_default(self):
        """Verify the default worker queue set includes the RD-Agent queue."""
        import app.worker.service.run_worker as _mod

        queue_map = {
            "backtest": MagicMock(name="backtest_queue"),
            "optimization": MagicMock(name="optimization_queue"),
            "default": MagicMock(name="default_queue"),
            "low": MagicMock(name="low_queue"),
            "rdagent": MagicMock(name="rdagent_queue"),
        }
        worker_instance = MagicMock()
        mock_worker_cls = MagicMock(return_value=worker_instance)

        with patch.object(_mod, "QUEUES", queue_map), \
             patch("sys.argv", ["run_worker"]), \
             patch.dict("sys.modules", {"rq": MagicMock(Worker=mock_worker_cls)}):
            _mod.main()

        args, kwargs = mock_worker_cls.call_args
        assert args[0] == [
            queue_map["backtest"],
            queue_map["optimization"],
            queue_map["default"],
            queue_map["low"],
            queue_map["rdagent"],
        ]
        assert kwargs["connection"] is _mod.redis_conn
        worker_instance.work.assert_called_once_with(with_scheduler=True)

    def test_module_imports(self):
        try:
            import app.worker.service.run_worker as _mod
            assert hasattr(_mod, "main")
        except ImportError:
            pytest.skip("run_worker not importable")
