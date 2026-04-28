"""RD-Agent orchestration service — manage autonomous factor mining jobs.

Coordinates between the QuantMate backend and the RD-Agent sidecar container.
Each mining job runs an R&D loop: HypothesisGen → Experiment → CoSTEER → Feedback.
"""

from __future__ import annotations

import logging
import uuid
from dataclasses import dataclass, field, asdict
from datetime import datetime
from enum import Enum
from typing import Any, Optional

from sqlalchemy import text

from app.infrastructure.config import get_runtime_str
from app.infrastructure.db.connections import connection

logger = logging.getLogger(__name__)

_RDAGENT_DB = "qlib"
_RDAGENT_SCHEMA_READY = False

_RDAGENT_SCHEMA_STATEMENTS = (
    """
    CREATE TABLE IF NOT EXISTS `qlib`.`rdagent_runs` (
        id BIGINT AUTO_INCREMENT PRIMARY KEY,
        run_id VARCHAR(36) NOT NULL UNIQUE,
        user_id BIGINT NOT NULL,
        scenario VARCHAR(32) NOT NULL DEFAULT 'fin_factor',
        config JSON,
        status VARCHAR(20) NOT NULL DEFAULT 'queued',
        current_iteration INT DEFAULT 0,
        total_iterations INT DEFAULT 0,
        error_message TEXT,
        created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
        started_at DATETIME,
        completed_at DATETIME,
        INDEX idx_rdagent_runs_user (user_id),
        INDEX idx_rdagent_runs_status (status)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
    """,
    """
    CREATE TABLE IF NOT EXISTS `qlib`.`rdagent_iterations` (
        id BIGINT AUTO_INCREMENT PRIMARY KEY,
        run_id VARCHAR(36) NOT NULL,
        iteration_number INT NOT NULL,
        hypothesis TEXT,
        experiment_code MEDIUMTEXT,
        metrics JSON,
        feedback TEXT,
        status VARCHAR(20) NOT NULL DEFAULT 'pending',
        created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
        INDEX idx_rdagent_iter_run (run_id),
        CONSTRAINT fk_rdagent_iter_run FOREIGN KEY (run_id) REFERENCES `rdagent_runs`(run_id) ON DELETE CASCADE
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
    """,
    """
    CREATE TABLE IF NOT EXISTS `qlib`.`rdagent_discovered_factors` (
        id BIGINT AUTO_INCREMENT PRIMARY KEY,
        run_id VARCHAR(36) NOT NULL,
        factor_name VARCHAR(128) NOT NULL,
        expression TEXT NOT NULL,
        description TEXT,
        ic_mean DOUBLE,
        icir DOUBLE,
        sharpe DOUBLE,
        status VARCHAR(20) NOT NULL DEFAULT 'discovered',
        created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
        INDEX idx_rdagent_factors_run (run_id),
        CONSTRAINT fk_rdagent_factors_run FOREIGN KEY (run_id) REFERENCES `qlib`.`rdagent_runs`(run_id) ON DELETE CASCADE
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
    """,
    """
    CREATE TABLE IF NOT EXISTS `qlib`.`data_catalog` (
        id BIGINT AUTO_INCREMENT PRIMARY KEY,
        source VARCHAR(32) NOT NULL,
        table_name VARCHAR(128) NOT NULL,
        column_name VARCHAR(128) NOT NULL,
        data_type VARCHAR(64) NOT NULL,
        category VARCHAR(32),
        is_numeric TINYINT(1) NOT NULL DEFAULT 0,
        scanned_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
        UNIQUE KEY uq_catalog_col (source, table_name, column_name),
        INDEX idx_catalog_category (category)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
    """,
)


class RunStatus(str, Enum):
    QUEUED = "queued"
    RUNNING = "running"
    PAUSED = "paused"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"


def get_default_rdagent_llm_model() -> str:
    return get_runtime_str(
        env_keys="RDAGENT_LLM_MODEL",
        db_key="rdagent.llm_model",
        default="gpt-4o-mini",
    )


@dataclass
class RDAgentMiningConfig:
    """Configuration for an RD-Agent mining run."""

    scenario: str = "fin_factor"
    max_iterations: int = 10
    llm_model: str = field(default_factory=get_default_rdagent_llm_model)
    universe: str = "csi300"
    feature_columns: list[str] = field(default_factory=list)
    start_date: str = "2018-01-01"
    end_date: str = "2024-12-31"
    hypothesis_type: str = "factor"  # factor | model | joint

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


class RDAgentService:
    """High-level orchestration for RD-Agent mining jobs."""

    def start_mining(
        self,
        user_id: int,
        config: RDAgentMiningConfig,
    ) -> dict[str, Any]:
        """Create a new mining run and enqueue it for background execution."""
        run_id = str(uuid.uuid4())
        now = datetime.utcnow().isoformat()

        with connection(_RDAGENT_DB) as conn:
            _ensure_rdagent_schema(conn)
            conn.execute(
                text(
                    "INSERT INTO rdagent_runs "
                    "(run_id, user_id, scenario, config, status, created_at) "
                    "VALUES (:rid, :uid, :scenario, :config, :status, :created_at)"
                ),
                {
                    "rid": run_id,
                    "uid": user_id,
                    "scenario": config.scenario,
                    "config": _serialize_json(config.to_dict()),
                    "status": RunStatus.QUEUED.value,
                    "created_at": now,
                },
            )
            conn.commit()

        logger.info("[rdagent] Created run %s for user %d", run_id, user_id)
        return {"run_id": run_id, "status": RunStatus.QUEUED.value}

    def get_run(self, user_id: int, run_id: str) -> Optional[dict[str, Any]]:
        """Get a single mining run."""
        with connection(_RDAGENT_DB) as conn:
            _ensure_rdagent_schema(conn)
            result = conn.execute(
                text(
                    "SELECT run_id, user_id, scenario, config, status, "
                    "current_iteration, total_iterations, error_message, "
                    "created_at, started_at, completed_at "
                    "FROM rdagent_runs WHERE run_id = :rid AND user_id = :uid"
                ),
                {"rid": run_id, "uid": user_id},
            )
            row = result.mappings().first()
            return dict(row) if row else None

    def list_runs(
        self, user_id: int, limit: int = 20, offset: int = 0
    ) -> list[dict[str, Any]]:
        """List mining runs for a user."""
        with connection(_RDAGENT_DB) as conn:
            _ensure_rdagent_schema(conn)
            result = conn.execute(
                text(
                    "SELECT run_id, scenario, status, current_iteration, "
                    "total_iterations, created_at, completed_at "
                    "FROM rdagent_runs WHERE user_id = :uid "
                    "ORDER BY created_at DESC LIMIT :lim OFFSET :off"
                ),
                {"uid": user_id, "lim": limit, "off": offset},
            )
            return [dict(r) for r in result.mappings().all()]

    def cancel_run(self, user_id: int, run_id: str) -> dict[str, Any]:
        """Cancel a running or queued mining run."""
        run = self.get_run(user_id, run_id)
        if not run:
            raise KeyError("Run not found")
        if run["status"] not in (RunStatus.QUEUED.value, RunStatus.RUNNING.value):
            raise ValueError(f"Cannot cancel run in status: {run['status']}")

        _update_run_status(run_id, RunStatus.CANCELLED.value)
        logger.info("[rdagent] Cancelled run %s", run_id)
        return {"run_id": run_id, "status": RunStatus.CANCELLED.value}

    def get_iterations(self, user_id: int, run_id: str) -> list[dict[str, Any]]:
        """Get iterations for a mining run."""
        # Ownership check
        run = self.get_run(user_id, run_id)
        if not run:
            raise KeyError("Run not found")

        with connection(_RDAGENT_DB) as conn:
            _ensure_rdagent_schema(conn)
            result = conn.execute(
                text(
                    "SELECT id, run_id, iteration_number, hypothesis, "
                    "experiment_code, metrics, feedback, status, created_at "
                    "FROM rdagent_iterations WHERE run_id = :rid "
                    "ORDER BY iteration_number"
                ),
                {"rid": run_id},
            )
            return [dict(r) for r in result.mappings().all()]

    def get_discovered_factors(
        self, user_id: int, run_id: str
    ) -> list[dict[str, Any]]:
        """Get factors discovered by a mining run."""
        run = self.get_run(user_id, run_id)
        if not run:
            raise KeyError("Run not found")

        with connection(_RDAGENT_DB) as conn:
            _ensure_rdagent_schema(conn)
            result = conn.execute(
                text(
                    "SELECT id, run_id, factor_name, expression, description, "
                    "ic_mean, icir, sharpe, status, created_at "
                    "FROM rdagent_discovered_factors WHERE run_id = :rid "
                    "ORDER BY icir DESC"
                ),
                {"rid": run_id},
            )
            return [dict(r) for r in result.mappings().all()]

    def import_factor(
        self, user_id: int, run_id: str, factor_id: int
    ) -> dict[str, Any]:
        """Import a discovered factor into the user's Factor Lab."""
        from app.domains.factors.service import FactorService

        run = self.get_run(user_id, run_id)
        if not run:
            raise KeyError("Run not found")

        # Get the discovered factor
        with connection(_RDAGENT_DB) as conn:
            _ensure_rdagent_schema(conn)
            result = conn.execute(
                text(
                    "SELECT id, factor_name, expression, description "
                    "FROM rdagent_discovered_factors "
                    "WHERE id = :fid AND run_id = :rid"
                ),
                {"fid": factor_id, "rid": run_id},
            )
            row = result.mappings().first()
            if not row:
                raise KeyError("Discovered factor not found")

        factor_data = dict(row)
        svc = FactorService()
        created = svc.create_factor(
            user_id=user_id,
            name=factor_data["factor_name"],
            expression=factor_data["expression"],
            description=factor_data.get("description"),
            category="rdagent",
        )

        # Mark as imported
        with connection(_RDAGENT_DB) as conn:
            _ensure_rdagent_schema(conn)
            conn.execute(
                text(
                    "UPDATE rdagent_discovered_factors "
                    "SET status = 'imported' WHERE id = :fid"
                ),
                {"fid": factor_id},
            )
            conn.commit()

        logger.info("[rdagent] Imported factor %d from run %s", factor_id, run_id)
        return created


# ── DB helpers ───────────────────────────────────────────────────────


def _ensure_rdagent_schema(conn) -> None:
    """Create the RD-Agent qlib tables on demand for drifted environments."""
    global _RDAGENT_SCHEMA_READY

    if _RDAGENT_SCHEMA_READY:
        return

    for ddl in _RDAGENT_SCHEMA_STATEMENTS:
        conn.execute(text(ddl))

    conn.commit()
    _RDAGENT_SCHEMA_READY = True


def _update_run_status(
    run_id: str,
    status: str,
    error_message: Optional[str] = None,
) -> None:
    """Update the status of a mining run."""
    with connection(_RDAGENT_DB) as conn:
        _ensure_rdagent_schema(conn)
        conn.execute(
            text(
                "UPDATE rdagent_runs SET status = :status, "
                "error_message = :err WHERE run_id = :rid"
            ),
            {"rid": run_id, "status": status, "err": error_message},
        )
        conn.commit()


def save_iteration(
    run_id: str,
    iteration_number: int,
    hypothesis: Optional[str] = None,
    experiment_code: Optional[str] = None,
    metrics: Optional[str] = None,
    feedback: Optional[str] = None,
    status: str = "completed",
) -> int:
    """Save an iteration result to the database."""
    with connection(_RDAGENT_DB) as conn:
        _ensure_rdagent_schema(conn)
        result = conn.execute(
            text(
                "INSERT INTO rdagent_iterations "
                "(run_id, iteration_number, hypothesis, experiment_code, "
                "metrics, feedback, status) "
                "VALUES (:rid, :num, :hyp, :code, :met, :fb, :st)"
            ),
            {
                "rid": run_id,
                "num": iteration_number,
                "hyp": hypothesis,
                "code": experiment_code,
                "met": metrics,
                "fb": feedback,
                "st": status,
            },
        )
        conn.commit()
        return result.lastrowid  # type: ignore[return-value]


def save_discovered_factor(
    run_id: str,
    factor_name: str,
    expression: str,
    description: Optional[str] = None,
    ic_mean: Optional[float] = None,
    icir: Optional[float] = None,
    sharpe: Optional[float] = None,
) -> int:
    """Save a discovered factor to the database."""
    with connection(_RDAGENT_DB) as conn:
        _ensure_rdagent_schema(conn)
        result = conn.execute(
            text(
                "INSERT INTO rdagent_discovered_factors "
                "(run_id, factor_name, expression, description, "
                "ic_mean, icir, sharpe, status) "
                "VALUES (:rid, :name, :expr, :desc, :ic, :icir, :sharpe, 'discovered')"
            ),
            {
                "rid": run_id,
                "name": factor_name,
                "expr": expression,
                "desc": description,
                "ic": ic_mean,
                "icir": icir,
                "sharpe": sharpe,
            },
        )
        conn.commit()
        return result.lastrowid  # type: ignore[return-value]


def _serialize_json(obj: Any) -> str:
    """Serialize a dict/list to JSON string for storage."""
    import json

    return json.dumps(obj, ensure_ascii=False, default=str)
