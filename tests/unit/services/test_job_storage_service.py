"""Unit tests for app.api.services.job_storage_service — JobStorage."""

from __future__ import annotations

import json
from datetime import datetime, timedelta
from unittest.mock import MagicMock, patch, PropertyMock

import pytest

_MOD = "app.api.services.job_storage_service"


def _make_storage():
    with patch(f"{_MOD}.Redis") as RedisCls, \
         patch(f"{_MOD}.get_settings") as gs:
        gs.return_value = MagicMock(redis_host="localhost", redis_port=6379, redis_db=0)
        from app.api.services.job_storage_service import JobStorage
        storage = JobStorage()
        mock_redis = storage.redis
        return storage, mock_redis


class TestSaveJobMetadata:
    def test_saves(self):
        s, r = _make_storage()
        s.save_job_metadata("j1", {"user_id": 1, "status": "queued"})
        r.setex.assert_called_once()
        key, ttl, data = r.setex.call_args[0]
        assert key == "quantmate:job:j1"
        assert ttl == 86400 * 7
        loaded = json.loads(data)
        assert loaded["status"] == "queued"
        assert "updated_at" in loaded


class TestGetJobMetadata:
    def test_found(self):
        s, r = _make_storage()
        r.get.return_value = json.dumps({"job_id": "j1", "status": "queued"})
        result = s.get_job_metadata("j1")
        assert result["status"] == "queued"

    def test_not_found(self):
        s, r = _make_storage()
        r.get.return_value = None
        assert s.get_job_metadata("xxx") is None


class TestSaveResult:
    def test_saves_with_params_from_meta(self):
        s, r = _make_storage()
        r.get.return_value = json.dumps({"parameters": {"fast": 5}})
        s.save_result("j1", {"total_return": 10.0})
        r.setex.assert_called()
        data = json.loads(r.setex.call_args[0][2])
        assert data["parameters"] == {"fast": 5}
        assert data["total_return"] == 10.0

    def test_saves_without_meta(self):
        s, r = _make_storage()
        r.get.return_value = None
        s.save_result("j1", {"total_return": 10.0})
        r.setex.assert_called()

    def test_result_already_has_params(self):
        s, r = _make_storage()
        r.get.return_value = json.dumps({"parameters": {"fast": 5}})
        s.save_result("j1", {"total_return": 10.0, "parameters": {"slow": 20}})
        data = json.loads(r.setex.call_args[0][2])
        assert data["parameters"] == {"slow": 20}


class TestGetResult:
    def test_found(self):
        s, r = _make_storage()
        r.get.return_value = json.dumps({"total_return": 10.0})
        result = s.get_result("j1")
        assert result["total_return"] == 10.0

    def test_not_found(self):
        s, r = _make_storage()
        r.get.return_value = None
        assert s.get_result("xxx") is None


class TestUpdateJobStatus:
    def test_updates(self):
        s, r = _make_storage()
        r.get.return_value = json.dumps({"job_id": "j1", "status": "queued"})
        s.update_job_status("j1", "completed", error=None)
        assert r.setex.called

    def test_no_metadata(self):
        s, r = _make_storage()
        r.get.return_value = None
        s.update_job_status("j1", "completed")
        assert not r.setex.called


class TestUpdateProgress:
    def test_updates(self):
        s, r = _make_storage()
        r.get.return_value = json.dumps({"job_id": "j1", "status": "running"})
        s.update_progress("j1", 0.5, "halfway")
        data = json.loads(r.setex.call_args[0][2])
        assert data["progress"] == 0.5
        assert data["progress_message"] == "halfway"

    def test_no_message(self):
        s, r = _make_storage()
        r.get.return_value = json.dumps({"job_id": "j1"})
        s.update_progress("j1", 0.8)
        data = json.loads(r.setex.call_args[0][2])
        assert data["progress"] == 0.8
        assert "progress_message" not in data


class TestListUserJobs:
    def test_filters_by_user(self):
        s, r = _make_storage()
        r.scan_iter.return_value = ["quantmate:job:j1", "quantmate:job:j2"]
        r.get.side_effect = [
            json.dumps({"user_id": 1, "status": "completed", "created_at": "2024-01-01"}),
            json.dumps({"user_id": 2, "status": "queued", "created_at": "2024-01-02"}),
        ]
        result = s.list_user_jobs(1)
        assert len(result) == 1
        assert result[0]["user_id"] == 1

    def test_filters_by_status(self):
        s, r = _make_storage()
        r.scan_iter.return_value = ["quantmate:job:j1", "quantmate:job:j2"]
        r.get.side_effect = [
            json.dumps({"user_id": 1, "status": "completed", "created_at": "2024-01-01"}),
            json.dumps({"user_id": 1, "status": "queued", "created_at": "2024-01-02"}),
        ]
        result = s.list_user_jobs(1, status="queued")
        assert len(result) == 1
        assert result[0]["status"] == "queued"

    def test_respects_limit(self):
        s, r = _make_storage()
        r.scan_iter.return_value = [f"quantmate:job:j{i}" for i in range(10)]
        r.get.side_effect = [
            json.dumps({"user_id": 1, "status": "completed", "created_at": f"2024-01-{i+1:02d}"})
            for i in range(10)
        ]
        result = s.list_user_jobs(1, limit=3)
        assert len(result) == 3


class TestDeleteJob:
    def test_deleted(self):
        s, r = _make_storage()
        r.delete.return_value = 2
        assert s.delete_job("j1") is True

    def test_not_found(self):
        s, r = _make_storage()
        r.delete.return_value = 0
        assert s.delete_job("xxx") is False


class TestCancelJob:
    def test_cancels_queued(self):
        s, r = _make_storage()
        mock_queue = MagicMock()
        mock_job = MagicMock()
        mock_job.get_status.return_value = "queued"
        with patch(f"{_MOD}.Job") as JobCls:
            JobCls.fetch.return_value = mock_job
            r.get.return_value = json.dumps({"job_id": "j1", "status": "queued"})
            result = s.cancel_job("j1", mock_queue)
        assert result is True
        mock_job.cancel.assert_called_once()

    def test_cannot_cancel_finished(self):
        s, r = _make_storage()
        mock_job = MagicMock()
        mock_job.get_status.return_value = "finished"
        with patch(f"{_MOD}.Job") as JobCls:
            JobCls.fetch.return_value = mock_job
            result = s.cancel_job("j1", MagicMock())
        assert result is False

    def test_cancel_error(self):
        s, r = _make_storage()
        with patch(f"{_MOD}.Job") as JobCls:
            JobCls.fetch.side_effect = Exception("connection error")
            result = s.cancel_job("j1", MagicMock())
        assert result is False


class TestCleanupOldJobs:
    def test_cleans_old(self):
        s, r = _make_storage()
        old_date = (datetime.now() - timedelta(days=10)).isoformat()
        r.scan_iter.return_value = ["quantmate:job:j1"]
        r.get.side_effect = [
            json.dumps({"job_id": "j1", "created_at": old_date}),
            None,  # get_job_metadata in delete_job
        ]
        r.delete.return_value = 1
        count = s.cleanup_old_jobs(days=7)
        assert count == 1


class TestGetQueueStats:
    def test_stats(self):
        s, r = _make_storage()
        mock_queue = MagicMock()
        mock_queue.__len__ = lambda self: 5
        mock_queue.failed_job_registry.count = 1
        mock_queue.finished_job_registry.count = 10
        mock_queue.started_job_registry.count = 2
        with patch("app.worker.service.config.QUEUES", {"default": mock_queue}):
            stats = s.get_queue_stats()
        assert stats["default"]["queued"] == 5
        assert stats["default"]["failed"] == 1


class TestGetJobStorage:
    def test_singleton(self):
        import app.api.services.job_storage_service as mod
        mod._job_storage = None
        with patch(f"{_MOD}.Redis"), patch(f"{_MOD}.get_settings") as gs:
            gs.return_value = MagicMock(redis_host="localhost", redis_port=6379, redis_db=0)
            s1 = mod.get_job_storage()
            s2 = mod.get_job_storage()
        assert s1 is s2
        mod._job_storage = None  # cleanup
