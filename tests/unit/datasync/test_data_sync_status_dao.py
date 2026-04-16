from __future__ import annotations

from unittest.mock import MagicMock, patch


def _engine_ctx():
    engine = MagicMock()
    conn = MagicMock()
    ctx = MagicMock()
    ctx.__enter__ = MagicMock(return_value=conn)
    ctx.__exit__ = MagicMock(return_value=False)
    engine.begin.return_value = ctx
    return engine, conn


class TestReleaseStaleBackfillLock:
    def test_uses_database_time_to_release_stale_lock(self):
        from app.domains.extdata.dao.data_sync_status_dao import release_stale_backfill_lock

        engine, conn = _engine_ctx()
        conn.execute.return_value = MagicMock(rowcount=1)

        with patch("app.domains.extdata.dao.data_sync_status_dao.ensure_backfill_lock_table"), \
             patch("app.domains.extdata.dao.data_sync_status_dao.engine_tm", engine):
            released = release_stale_backfill_lock(max_age_hours=6)

        assert released is True
        sql = conn.execute.call_args.args[0].text
        params = conn.execute.call_args.args[1]
        assert "TIMESTAMPDIFF(SECOND, locked_at, CURRENT_TIMESTAMP)" in sql
        assert params["max_age_seconds"] == 6 * 3600

    def test_returns_false_when_lock_is_not_stale(self):
        from app.domains.extdata.dao.data_sync_status_dao import release_stale_backfill_lock

        engine, conn = _engine_ctx()
        conn.execute.return_value = MagicMock(rowcount=0)

        with patch("app.domains.extdata.dao.data_sync_status_dao.ensure_backfill_lock_table"), \
             patch("app.domains.extdata.dao.data_sync_status_dao.engine_tm", engine):
            released = release_stale_backfill_lock(max_age_hours=6)

        assert released is False


class TestReleaseOrphanedBackfillLock:
    def test_releases_local_orphaned_lock(self):
        from app.domains.extdata.dao.data_sync_status_dao import release_orphaned_backfill_lock

        engine, conn = _engine_ctx()
        conn.execute.side_effect = [
            MagicMock(fetchone=MagicMock(return_value=(1, "test-host"))),
            MagicMock(rowcount=1),
        ]

        with patch("app.domains.extdata.dao.data_sync_status_dao.ensure_backfill_lock_table"), \
             patch("app.domains.extdata.dao.data_sync_status_dao.engine_tm", engine), \
             patch("app.domains.extdata.dao.data_sync_status_dao.socket.gethostname", return_value="test-host"), \
             patch("app.domains.extdata.dao.data_sync_status_dao._local_backfill_process_running", return_value=False):
            released = release_orphaned_backfill_lock()

        assert released is True

    def test_keeps_foreign_host_lock(self):
        from app.domains.extdata.dao.data_sync_status_dao import release_orphaned_backfill_lock

        engine, conn = _engine_ctx()
        conn.execute.return_value = MagicMock(fetchone=MagicMock(return_value=(1, "other-host:1234")))

        with patch("app.domains.extdata.dao.data_sync_status_dao.ensure_backfill_lock_table"), \
             patch("app.domains.extdata.dao.data_sync_status_dao.engine_tm", engine), \
             patch("app.domains.extdata.dao.data_sync_status_dao.socket.gethostname", return_value="test-host"):
            released = release_orphaned_backfill_lock()

        assert released is False