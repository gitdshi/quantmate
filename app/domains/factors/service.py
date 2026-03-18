"""Factor Lab domain service."""

from __future__ import annotations

from typing import Any, Optional

from app.domains.factors.dao.factor_dao import FactorDefinitionDao, FactorEvaluationDao


class FactorService:
    def __init__(self) -> None:
        self._factor_dao = FactorDefinitionDao()
        self._eval_dao = FactorEvaluationDao()

    # --- Factor definitions ---

    def list_factors(
        self, user_id: int, category: Optional[str] = None, limit: int = 50, offset: int = 0
    ) -> list[dict[str, Any]]:
        return self._factor_dao.list_for_user(user_id, category=category, limit=limit, offset=offset)

    def count_factors(self, user_id: int) -> int:
        return self._factor_dao.count_for_user(user_id)

    def create_factor(self, user_id: int, name: str, expression: str, **kwargs) -> dict[str, Any]:
        factor_id = self._factor_dao.create(user_id, name, expression, **kwargs)
        return self.get_factor(user_id, factor_id)

    def get_factor(self, user_id: int, factor_id: int) -> dict[str, Any]:
        row = self._factor_dao.get(factor_id, user_id)
        if not row:
            raise KeyError("Factor not found")
        return row

    def update_factor(self, user_id: int, factor_id: int, **fields) -> dict[str, Any]:
        existing = self._factor_dao.get(factor_id, user_id)
        if not existing:
            raise KeyError("Factor not found")
        self._factor_dao.update(factor_id, user_id, **fields)
        return self.get_factor(user_id, factor_id)

    def delete_factor(self, user_id: int, factor_id: int) -> None:
        if not self._factor_dao.delete(factor_id, user_id):
            raise KeyError("Factor not found")

    # --- Evaluations ---

    def list_evaluations(self, user_id: int, factor_id: int) -> list[dict[str, Any]]:
        self.get_factor(user_id, factor_id)  # ownership check
        return self._eval_dao.list_for_factor(factor_id)

    def run_evaluation(self, user_id: int, factor_id: int, start_date: str, end_date: str) -> dict[str, Any]:
        """Run a factor evaluation (stub — returns simulated metrics)."""
        self.get_factor(user_id, factor_id)
        # In production, this would compute IC, returns, etc.
        stub_metrics = {
            "ic_mean": 0.035,
            "ic_ir": 0.42,
            "turnover": 0.15,
            "long_ret": 0.12,
            "short_ret": -0.03,
            "long_short_ret": 0.15,
        }
        eval_id = self._eval_dao.create(
            factor_id,
            start_date,
            end_date,
            metrics=stub_metrics,
            **stub_metrics,
        )
        return self._eval_dao.get(eval_id) or {"id": eval_id}

    def delete_evaluation(self, user_id: int, factor_id: int, eval_id: int) -> None:
        self.get_factor(user_id, factor_id)
        if not self._eval_dao.delete(eval_id):
            raise KeyError("Evaluation not found")
