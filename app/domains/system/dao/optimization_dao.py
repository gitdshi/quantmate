"""Optimization task DAO."""

import json
from sqlalchemy import text
from app.infrastructure.db.connections import get_quantmate_engine


class OptimizationTaskDao:
    """Data access for optimization_tasks and optimization_task_results tables."""

    def __init__(self):
        self.engine = get_quantmate_engine()

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
                d = dict(r)
                for field in ("param_space", "best_params", "best_metrics"):
                    if isinstance(d.get(field), str):
                        d[field] = json.loads(d[field])
                result.append(d)
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
                d = dict(row)
                for field in ("param_space", "best_params", "best_metrics"):
                    if isinstance(d.get(field), str):
                        d[field] = json.loads(d[field])
                return d
            return None

    def create(
        self,
        user_id: int,
        strategy_id: int,
        search_method: str,
        param_space: dict,
        objective_metric: str = "sharpe_ratio",
    ) -> int:
        with self.engine.begin() as conn:
            result = conn.execute(
                text("""
                INSERT INTO optimization_tasks
                    (user_id, strategy_id, search_method, param_space, objective_metric, status)
                VALUES (:uid, :sid, :method, :space, :metric, 'pending')
            """),
                {
                    "uid": user_id,
                    "sid": strategy_id,
                    "method": search_method,
                    "space": json.dumps(param_space),
                    "metric": objective_metric,
                },
            )
            return result.lastrowid

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
        if best_params is not None:
            sets.append("best_params = :bp")
            params["bp"] = json.dumps(best_params)
        if best_metrics is not None:
            sets.append("best_metrics = :bm")
            params["bm"] = json.dumps(best_metrics)
        if total_iterations is not None:
            sets.append("total_iterations = :ti")
            params["ti"] = total_iterations
        with self.engine.begin() as conn:
            result = conn.execute(
                text(f"UPDATE optimization_tasks SET {', '.join(sets)} WHERE id = :id"),
                params,
            )
            return result.rowcount > 0

    def get_results(self, task_id: int, user_id: int) -> list:
        with self.engine.connect() as conn:
            # Verify ownership
            owner = conn.execute(
                text("SELECT id FROM optimization_tasks WHERE id = :id AND user_id = :uid"),
                {"id": task_id, "uid": user_id},
            ).first()
            if not owner:
                return []
            rows = (
                conn.execute(
                    text("""
                SELECT * FROM optimization_task_results WHERE task_id = :id ORDER BY rank_order
            """),
                    {"id": task_id},
                )
                .mappings()
                .all()
            )
            result = []
            for r in rows:
                d = dict(r)
                for field in ("params", "metrics"):
                    if isinstance(d.get(field), str):
                        d[field] = json.loads(d[field])
                result.append(d)
            return result
