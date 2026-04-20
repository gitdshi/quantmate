"""Unit tests for app.domains.system.dao.optimization_dao."""

from __future__ import annotations

from unittest.mock import MagicMock, patch


_MOD = "app.domains.system.dao.optimization_dao"


def _row(**kw):
    m = MagicMock()
    m._mapping = kw
    return m


def _make_dao():
    """Create OptimizationTaskDao with mocked engine."""
    with patch(f"{_MOD}.get_quantmate_engine") as mock_eng:
        engine = MagicMock()
        mock_eng.return_value = engine
        # _load_columns needs connect → fetchall
        conn = MagicMock()
        ctx = MagicMock()
        ctx.__enter__ = MagicMock(return_value=conn)
        ctx.__exit__ = MagicMock(return_value=False)
        engine.connect.return_value = ctx
        engine.begin.return_value = ctx
        # Return empty column list for INFORMATION_SCHEMA queries
        conn.execute.return_value = MagicMock(
            fetchall=MagicMock(return_value=[])
        )
        from app.domains.system.dao.optimization_dao import OptimizationTaskDao
        dao = OptimizationTaskDao()
        dao.engine = engine
        return dao, engine, conn, ctx


class TestOptimizationTaskDao:
    def test_list_by_user(self):
        dao, engine, conn, ctx = _make_dao()
        engine.connect.return_value = ctx
        row = MagicMock()
        row._mapping = {"id": 1, "user_id": 1, "status": "completed"}
        # scalar() returns plain int, mappings().all() returns list of mapping rows
        scalar_result = MagicMock(scalar=MagicMock(return_value=5))
        mappings_result = MagicMock()
        mappings_result.mappings.return_value = MagicMock(all=MagicMock(return_value=[{"id": 1, "status": "completed"}]))
        conn.execute.side_effect = [scalar_result, mappings_result]
        items, total = dao.list_by_user(user_id=1)
        assert isinstance(items, list)
        assert total == 5

    def test_get_by_id_found(self):
        dao, engine, conn, ctx = _make_dao()
        engine.connect.return_value = ctx
        mappings_result = MagicMock()
        mappings_result.mappings.return_value = MagicMock(
            first=MagicMock(return_value={"id": 1, "user_id": 1, "status": "completed", "param_space": '{}'})
        )
        conn.execute.return_value = mappings_result
        result = dao.get_by_id(task_id=1, user_id=1)
        assert result is not None

    def test_get_by_id_not_found(self):
        dao, engine, conn, ctx = _make_dao()
        engine.connect.return_value = ctx
        mappings_result = MagicMock()
        mappings_result.mappings.return_value = MagicMock(
            first=MagicMock(return_value=None)
        )
        conn.execute.return_value = mappings_result
        result = dao.get_by_id(task_id=999, user_id=1)
        assert result is None

    def test_create(self):
        dao, engine, conn, ctx = _make_dao()
        engine.begin.return_value = ctx
        conn.execute.return_value = MagicMock(lastrowid=42)
        result = dao.create(
            user_id=1, strategy_id=10,
            search_method="grid", param_space={"x": [1, 2, 3]},
            objective_metric="sharpe"
        )
        assert result == 42

    def test_update_status(self):
        dao, engine, conn, ctx = _make_dao()
        engine.begin.return_value = ctx
        conn.execute.return_value = MagicMock(rowcount=1)
        result = dao.update_status(
            task_id=1, status="completed",
            best_params={"x": 2}, best_metrics={"sharpe": 1.5},
            total_iterations=10
        )
        assert result is True

    def test_delete_by_id(self):
        dao, engine, conn, ctx = _make_dao()
        engine.begin.return_value = ctx
        conn.execute.return_value = MagicMock(rowcount=1)
        result = dao.delete_by_id(task_id=1, user_id=1)
        assert result is True

    def test_replace_results(self):
        dao, engine, conn, ctx = _make_dao()
        engine.begin.return_value = ctx
        results = [{"params": {"x": 1}, "metric": 0.5}]
        dao.replace_results(task_id=1, results=results)
        assert conn.execute.called

    def test_get_results(self):
        dao, engine, conn, ctx = _make_dao()
        engine.connect.return_value = ctx
        row = MagicMock()
        row._mapping = {"id": 1, "params": '{"x": 1}'}
        conn.execute.return_value = MagicMock(
            fetchall=MagicMock(return_value=[row])
        )
        result = dao.get_results(task_id=1, user_id=1)
        assert isinstance(result, list)

    def test_get_task_for_worker(self):
        dao, engine, conn, ctx = _make_dao()
        engine.connect.return_value = ctx
        mappings_result = MagicMock()
        mappings_result.mappings.return_value = MagicMock(
            first=MagicMock(return_value={"id": 1, "status": "pending", "param_space": '{}'})
        )
        conn.execute.return_value = mappings_result
        result = dao.get_task_for_worker(task_id=1)
        assert result is not None
