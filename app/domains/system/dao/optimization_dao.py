"""Optimization task DAO."""

import json
from typing import Any

from sqlalchemy import text
from app.infrastructure.db.connections import get_quantmate_engine


class OptimizationTaskDao:
    """Data access for optimization_tasks and optimization_task_results tables."""

    def __init__(self):
        self.engine = get_quantmate_engine()
        self._task_columns = self._load_columns("optimization_tasks")
        self._result_columns = self._load_columns("optimization_task_results")

    def _load_columns(self, table_name: str) -> set[str]:
        with self.engine.connect() as conn:
            rows = conn.execute(text(f"SHOW COLUMNS FROM {table_name}")).fetchall()
        return {str(row[0]) for row in rows}

    def _has_task_column(self, column: str) -> bool:
        return column in self._task_columns

    def _has_result_column(self, column: str) -> bool:
        return column in self._result_columns

    @staticmethod
    def _json_load_if_needed(payload: dict[str, Any], field: str) -> None:
        if isinstance(payload.get(field), str):
            payload[field] = json.loads(payload[field])

    def _normalize_task_row(self, row: dict[str, Any]) -> dict[str, Any]:
        self._json_load_if_needed(row, "param_space")
        self._json_load_if_needed(row, "param_ranges")
        self._json_load_if_needed(row, "best_params")
        self._json_load_if_needed(row, "best_metrics")

        if "param_space" not in row and "param_ranges" in row:
            row["param_space"] = row.get("param_ranges")
        if "objective_metric" not in row and "objective" in row:
            row["objective_metric"] = row.get("objective")
        return row

    def _normalize_result_row(self, row: dict[str, Any]) -> dict[str, Any]:
        self._json_load_if_needed(row, "params")
        self._json_load_if_needed(row, "metrics")
        if "rank_order" not in row and "rank_num" in row:
            row["rank_order"] = row.get("rank_num")
        return row

    def list_by_user(self, user_id: int, page: int = 1, page_size: int = 20) -> tuple[list, int]:
        offset = (page - 1) * page_size
        with self.engine.connect() as conn:
            total = conn.execute(
                text("SELECT COUNT(*) FROM optimization_tasks WHERE user_id = :uid"),
                {"uid": user_id},
            ).scalar()
            rows = (
                conn.execute(
                    text("""
                SELECT * FROM optimization_tasks WHERE user_id = :uid
                ORDER BY created_at DESC LIMIT :limit OFFSET :offset
            """),
                    {"uid": user_id, "limit": page_size, "offset": offset},
                )
                .mappings()
                .all()
            )
            result = []
            for r in rows:
                result.append(self._normalize_task_row(dict(r)))
            return result, total

    def get_by_id(self, task_id: int, user_id: int) -> dict | None:
        with self.engine.connect() as conn:
            row = (
                conn.execute(
                    text("SELECT * FROM optimization_tasks WHERE id = :id AND user_id = :uid"),
                    {"id": task_id, "uid": user_id},
                )
                .mappings()
                .first()
            )
            if row:
                return self._normalize_task_row(dict(row))
            return None

    def get_task_for_worker(self, task_id: int) -> dict | None:
        """Load one task row without user filter (used by worker)."""
        with self.engine.connect() as conn:
            row = (
                conn.execute(
                    text("SELECT * FROM optimization_tasks WHERE id = :id"),
                    {"id": task_id},
                )
                .mappings()
                .first()
            )
            if row:
                return self._normalize_task_row(dict(row))
            return None

    def delete_by_id(self, task_id: int, user_id: int) -> bool:
        with self.engine.begin() as conn:
            result = conn.execute(
                text("DELETE FROM optimization_tasks WHERE id = :id AND user_id = :uid"),
                {"id": task_id, "uid": user_id},
            )
            return result.rowcount > 0

    def create(
        self,
        user_id: int,
        strategy_id: int,
        search_method: str,
        param_space: dict,
        objective_metric: str = "sharpe_ratio",
    ) -> int:
        param_field = "param_space" if self._has_task_column("param_space") else "param_ranges"
        objective_field = "objective_metric" if self._has_task_column("objective_metric") else "objective"
        with self.engine.begin() as conn:
            statement = text(
                f"""
                INSERT INTO optimization_tasks
                    (user_id, strategy_id, search_method, {param_field}, {objective_field}, status)
                VALUES (:uid, :sid, :method, :space, :metric, 'pending')
                """
            )
            result = conn.execute(
                statement,
                {
                    "uid": user_id,
                    "sid": strategy_id,
                    "method": search_method,
                    "space": json.dumps(param_space),
                    "metric": objective_metric,
                },
            )
            task_id = result.lastrowid
            if task_id is None:
                task_id = conn.execute(text("SELECT LAST_INSERT_ID()")).scalar()
            return int(task_id)

    def update_status(
        self,
        task_id: int,
        status: str,
        best_params: dict = None,
        best_metrics: dict = None,
        total_iterations: int = None,
    ) -> bool:
        sets = ["status = :status"]
        params = {"id": task_id, "status": status}
        if best_params is not None and self._has_task_column("best_params"):
            sets.append("best_params = :bp")
            params["bp"] = json.dumps(best_params)
        if best_metrics is not None and self._has_task_column("best_metrics"):
            sets.append("best_metrics = :bm")
            params["bm"] = json.dumps(best_metrics)
        if total_iterations is not None and self._has_task_column("total_iterations"):
            sets.append("total_iterations = :ti")
            params["ti"] = total_iterations
        if self._has_task_column("completed_at"):
            if status in ("completed", "failed", "cancelled"):
                sets.append("completed_at = NOW()")
            elif status in ("pending", "running"):
                sets.append("completed_at = NULL")
        with self.engine.begin() as conn:
            result = conn.execute(
                text(f"UPDATE optimization_tasks SET {', '.join(sets)} WHERE id = :id"),
                params,
            )
            return result.rowcount > 0

    def replace_results(self, task_id: int, results: list[dict[str, Any]]) -> None:
        """Replace all result rows for a task (worker write path)."""
        rank_field = "rank_order" if self._has_result_column("rank_order") else "rank_num"
        with self.engine.begin() as conn:
            conn.execute(text("DELETE FROM optimization_task_results WHERE task_id = :id"), {"id": task_id})
            for index, item in enumerate(results, start=1):
                conn.execute(
                    text(
                        f"""
                        INSERT INTO optimization_task_results (task_id, params, metrics, {rank_field})
                        VALUES (:task_id, :params, :metrics, :rank_order)
                        """
                    ),
                    {
                        "task_id": task_id,
                        "params": json.dumps(item.get("params") or {}),
                        "metrics": json.dumps(item.get("metrics") or {}),
                        "rank_order": int(item.get("rank_order") or index),
                    },
                )

    def get_results(self, task_id: int, user_id: int) -> list:
        with self.engine.connect() as conn:
            # Verify ownership
            owner = conn.execute(
                text("SELECT id FROM optimization_tasks WHERE id = :id AND user_id = :uid"),
                {"id": task_id, "uid": user_id},
            ).first()
            if not owner:
                return []
            order_field = "rank_order" if self._has_result_column("rank_order") else "rank_num"
            rows = (
                conn.execute(
                    text(
                        f"""
                        SELECT * FROM optimization_task_results
                        WHERE task_id = :id
                        ORDER BY {order_field}
                        """
                    ),
                    {"id": task_id},
                )
                .mappings()
                .all()
            )
            result = []
            for r in rows:
                result.append(self._normalize_result_row(dict(r)))
            return result
