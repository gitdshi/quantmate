"""Composite strategy domain service.

Orchestrates CRUD for strategy components and composite strategies,
and manages composite backtest job submission.
"""

from __future__ import annotations

from datetime import datetime
from typing import Any, Optional
import json
import uuid

from app.domains.composite.dao.strategy_component_dao import StrategyComponentDao
from app.domains.composite.dao.composite_strategy_dao import CompositeStrategyDao
from app.domains.composite.dao.composite_backtest_dao import CompositeBacktestDao
from app.infrastructure.config import get_runtime_int


class CompositeStrategyService:

    def __init__(self) -> None:
        self._component_dao = StrategyComponentDao()
        self._composite_dao = CompositeStrategyDao()
        self._backtest_dao = CompositeBacktestDao()

    # ── Strategy Components ──────────────────────────────────────────────

    def list_components(
        self, user_id: int, layer: Optional[str] = None
    ) -> list[dict[str, Any]]:
        return self._component_dao.list_for_user(user_id, layer=layer)

    def count_components(self, user_id: int, layer: Optional[str] = None) -> int:
        return self._component_dao.count_for_user(user_id, layer=layer)

    def list_components_paginated(
        self, user_id: int, limit: int, offset: int, layer: Optional[str] = None
    ) -> list[dict[str, Any]]:
        return self._component_dao.list_for_user_paginated(user_id, limit, offset, layer=layer)

    def create_component(
        self,
        user_id: int,
        name: str,
        layer: str,
        sub_type: str,
        description: Optional[str],
        code: Optional[str],
        config: Optional[dict[str, Any]],
        parameters: Optional[dict[str, Any]],
    ) -> dict[str, Any]:
        if self._component_dao.name_exists_for_user(user_id, name, layer):
            raise ValueError(f"Component '{name}' already exists in {layer} layer")

        now = datetime.utcnow()
        component_id = self._component_dao.insert(
            user_id=user_id,
            name=name,
            layer=layer,
            sub_type=sub_type,
            description=description,
            code=code,
            config_json=json.dumps(config) if config else None,
            parameters_json=json.dumps(parameters) if parameters else None,
            created_at=now,
            updated_at=now,
        )
        return self.get_component(user_id, component_id)

    def get_component(self, user_id: int, component_id: int) -> dict[str, Any]:
        row = self._component_dao.get_for_user(component_id, user_id)
        if not row:
            raise KeyError("Strategy component not found")
        row["config"] = _parse_json(row.get("config"))
        row["parameters"] = _parse_json(row.get("parameters"))
        return row

    def update_component(
        self,
        user_id: int,
        component_id: int,
        *,
        name: Optional[str] = None,
        sub_type: Optional[str] = None,
        description: Optional[str] = None,
        code: Optional[str] = None,
        config: Optional[dict[str, Any]] = None,
        parameters: Optional[dict[str, Any]] = None,
        is_active: Optional[bool] = None,
    ) -> dict[str, Any]:
        existing = self._component_dao.get_for_user(component_id, user_id)
        if not existing:
            raise KeyError("Strategy component not found")

        updates: list[str] = []
        params: dict[str, Any] = {}
        version_bump = False

        if name is not None:
            updates.append("name = :name")
            params["name"] = name
            if name != existing.get("name"):
                version_bump = True
        if sub_type is not None:
            updates.append("sub_type = :sub_type")
            params["sub_type"] = sub_type
            if sub_type != existing.get("sub_type"):
                version_bump = True
        if description is not None:
            updates.append("description = :description")
            params["description"] = description
        if code is not None:
            updates.append("code = :code")
            params["code"] = code
            if code != existing.get("code"):
                version_bump = True
        if config is not None:
            updates.append("config = :config")
            params["config"] = json.dumps(config)
            version_bump = True
        if parameters is not None:
            updates.append("parameters = :parameters")
            params["parameters"] = json.dumps(parameters)
            version_bump = True
        if is_active is not None:
            updates.append("is_active = :is_active")
            params["is_active"] = is_active

        if not updates:
            return self.get_component(user_id, component_id)

        if version_bump:
            updates.append("version = version + 1")
        updates.append("updated_at = :updated_at")
        params["updated_at"] = datetime.utcnow()

        self._component_dao.update(component_id, user_id, ", ".join(updates), params)
        return self.get_component(user_id, component_id)

    def delete_component(self, user_id: int, component_id: int) -> None:
        if not self._component_dao.delete_for_user(component_id, user_id):
            raise KeyError("Strategy component not found")

    # ── Composite Strategies ─────────────────────────────────────────────

    def list_composites(self, user_id: int) -> list[dict[str, Any]]:
        return self._composite_dao.list_for_user(user_id)

    def count_composites(self, user_id: int) -> int:
        return self._composite_dao.count_for_user(user_id)

    def list_composites_paginated(
        self, user_id: int, limit: int, offset: int
    ) -> list[dict[str, Any]]:
        return self._composite_dao.list_for_user_paginated(user_id, limit, offset)

    def create_composite(
        self,
        user_id: int,
        name: str,
        description: Optional[str],
        portfolio_config: Optional[dict[str, Any]],
        market_constraints: Optional[dict[str, Any]],
        execution_mode: str,
        bindings: list[dict[str, Any]],
    ) -> dict[str, Any]:
        if self._composite_dao.name_exists_for_user(user_id, name):
            raise ValueError(f"Composite strategy '{name}' already exists")

        # Validate all referenced component IDs belong to this user
        component_ids = [b["component_id"] for b in bindings]
        if component_ids:
            valid_ids = self._component_dao.get_ids_for_user(component_ids, user_id)
            invalid = set(component_ids) - set(valid_ids)
            if invalid:
                raise ValueError(f"Component IDs not found: {sorted(invalid)}")

        now = datetime.utcnow()
        composite_id = self._composite_dao.insert(
            user_id=user_id,
            name=name,
            description=description,
            portfolio_config_json=json.dumps(portfolio_config) if portfolio_config else None,
            market_constraints_json=json.dumps(market_constraints) if market_constraints else None,
            execution_mode=execution_mode,
            created_at=now,
            updated_at=now,
        )

        if bindings:
            binding_dicts = [
                {
                    "component_id": b["component_id"],
                    "layer": b["layer"],
                    "ordinal": b.get("ordinal", 0),
                    "weight": b.get("weight", 1.0),
                    "config_override": json.dumps(b["config_override"]) if b.get("config_override") else None,
                }
                for b in bindings
            ]
            self._composite_dao.replace_bindings(composite_id, binding_dicts)

        return self.get_composite_detail(user_id, composite_id)

    def get_composite(self, user_id: int, composite_id: int) -> dict[str, Any]:
        row = self._composite_dao.get_for_user(composite_id, user_id)
        if not row:
            raise KeyError("Composite strategy not found")
        row["portfolio_config"] = _parse_json(row.get("portfolio_config"))
        row["market_constraints"] = _parse_json(row.get("market_constraints"))
        return row

    def get_composite_detail(self, user_id: int, composite_id: int) -> dict[str, Any]:
        row = self.get_composite(user_id, composite_id)
        bindings = self._composite_dao.get_bindings(composite_id)
        for b in bindings:
            b["config_override"] = _parse_json(b.get("config_override"))
        row["bindings"] = bindings
        return row

    def update_composite(
        self,
        user_id: int,
        composite_id: int,
        *,
        name: Optional[str] = None,
        description: Optional[str] = None,
        portfolio_config: Optional[dict[str, Any]] = None,
        market_constraints: Optional[dict[str, Any]] = None,
        execution_mode: Optional[str] = None,
        is_active: Optional[bool] = None,
    ) -> dict[str, Any]:
        existing = self._composite_dao.get_for_user(composite_id, user_id)
        if not existing:
            raise KeyError("Composite strategy not found")

        updates: list[str] = []
        params: dict[str, Any] = {}

        if name is not None:
            updates.append("name = :name")
            params["name"] = name
        if description is not None:
            updates.append("description = :description")
            params["description"] = description
        if portfolio_config is not None:
            updates.append("portfolio_config = :portfolio_config")
            params["portfolio_config"] = json.dumps(portfolio_config)
        if market_constraints is not None:
            updates.append("market_constraints = :market_constraints")
            params["market_constraints"] = json.dumps(market_constraints)
        if execution_mode is not None:
            updates.append("execution_mode = :execution_mode")
            params["execution_mode"] = execution_mode
        if is_active is not None:
            updates.append("is_active = :is_active")
            params["is_active"] = is_active

        if not updates:
            return self.get_composite_detail(user_id, composite_id)

        updates.append("updated_at = :updated_at")
        params["updated_at"] = datetime.utcnow()

        self._composite_dao.update(composite_id, user_id, ", ".join(updates), params)
        return self.get_composite_detail(user_id, composite_id)

    def replace_bindings(
        self,
        user_id: int,
        composite_id: int,
        bindings: list[dict[str, Any]],
    ) -> list[dict[str, Any]]:
        """Replace all bindings for a composite strategy."""
        existing = self._composite_dao.get_for_user(composite_id, user_id)
        if not existing:
            raise KeyError("Composite strategy not found")

        component_ids = [b["component_id"] for b in bindings]
        if component_ids:
            valid_ids = self._component_dao.get_ids_for_user(component_ids, user_id)
            invalid = set(component_ids) - set(valid_ids)
            if invalid:
                raise ValueError(f"Component IDs not found: {sorted(invalid)}")

        binding_dicts = [
            {
                "component_id": b["component_id"],
                "layer": b["layer"],
                "ordinal": b.get("ordinal", 0),
                "weight": b.get("weight", 1.0),
                "config_override": json.dumps(b["config_override"]) if b.get("config_override") else None,
            }
            for b in bindings
        ]
        self._composite_dao.replace_bindings(composite_id, binding_dicts)

        # Touch updated_at
        self._composite_dao.update(
            composite_id, user_id,
            "updated_at = :updated_at",
            {"updated_at": datetime.utcnow()},
        )

        result = self._composite_dao.get_bindings(composite_id)
        for b in result:
            b["config_override"] = _parse_json(b.get("config_override"))
        return result

    def delete_composite(self, user_id: int, composite_id: int) -> None:
        if not self._composite_dao.delete_for_user(composite_id, user_id):
            raise KeyError("Composite strategy not found")

    # ── Composite Backtests ──────────────────────────────────────────────

    def submit_backtest(
        self,
        user_id: int,
        composite_strategy_id: int,
        start_date: str,
        end_date: str,
        initial_capital: float,
        benchmark: str,
    ) -> dict[str, Any]:
        """Submit a composite backtest job to the RQ backtest queue."""
        # Validate the composite strategy exists and belongs to user
        existing = self._composite_dao.get_for_user(composite_strategy_id, user_id)
        if not existing:
            raise KeyError("Composite strategy not found")

        from app.worker.service.config import get_queue

        job_id = f"cbt_{uuid.uuid4().hex[:16]}"
        self._backtest_dao.insert(
            job_id=job_id,
            user_id=user_id,
            composite_strategy_id=composite_strategy_id,
            start_date=start_date,
            end_date=end_date,
            initial_capital=initial_capital,
            benchmark=benchmark,
        )

        queue = get_queue("backtest")
        queue.enqueue(
            "app.domains.composite.tasks.run_composite_backtest_task",
            kwargs={"job_id": job_id},
            job_id=job_id,
            job_timeout=get_runtime_int(
                env_keys="BACKTEST_JOB_TIMEOUT_SECONDS",
                db_key="backtest.job_timeout_seconds",
                default=3600,
            ),
            result_ttl=get_runtime_int(
                env_keys="BACKTEST_RESULT_TTL_SECONDS",
                db_key="backtest.result_ttl_seconds",
                default=86400 * 7,
            ),
        )

        return self._backtest_dao.get_by_job_id(job_id)

    def list_backtests(
        self,
        user_id: int,
        composite_strategy_id: Optional[int] = None,
    ) -> list[dict[str, Any]]:
        return self._backtest_dao.list_for_user(
            user_id, composite_strategy_id=composite_strategy_id
        )

    def get_backtest(self, user_id: int, job_id: str) -> dict[str, Any]:
        row = self._backtest_dao.get_by_job_id(job_id)
        if not row or row.get("user_id") != user_id:
            raise KeyError("Composite backtest not found")
        row["result"] = _parse_json(row.get("result"))
        row["attribution"] = _parse_json(row.get("attribution"))
        return row

    def delete_backtest(self, user_id: int, job_id: str) -> None:
        row = self._backtest_dao.get_by_job_id(job_id)
        if not row or row.get("user_id") != user_id:
            raise KeyError("Composite backtest not found")
        self._backtest_dao.delete_for_user(row["id"], user_id)


def _parse_json(val: Any) -> Any:
    """Parse a JSON string to dict, or return None."""
    if val is None:
        return None
    if isinstance(val, (dict, list)):
        return val
    try:
        return json.loads(val)
    except Exception:
        return None
