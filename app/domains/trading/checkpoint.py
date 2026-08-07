"""Paper deployment checkpoint manager (TASK-011).

Improvements over the previous in-memory + DB pickle approach:
  - Single source of truth: only the DB row holds state (no in-memory shadow).
  - JSON serialization instead of pickle for forward/backward compatibility.
  - Atomic UPSERT so partial writes are impossible.
"""

from __future__ import annotations

import json
import logging
from datetime import datetime
from typing import Any, Dict, Optional

from sqlalchemy import text

from app.infrastructure.db.connections import connection

logger = logging.getLogger(__name__)


class CheckpointManager:
    """Persist and restore paper deployment state."""

    def __init__(self, deployment_id: int | str) -> None:
        self.deployment_id = str(deployment_id)

    def save(self, state: Dict[str, Any]) -> None:
        """Persist ``state`` for the deployment (atomic UPSERT)."""
        payload = json.dumps(
            {
                "deployment_id": self.deployment_id,
                "timestamp": datetime.utcnow().isoformat(),
                "state": state,
            },
            default=str,
        )
        try:
            with connection("quantmate") as conn:
                conn.execute(
                    text(
                        """
                        INSERT INTO paper_deployment_checkpoints
                            (deployment_id, state_json, updated_at)
                        VALUES (:id, :state, CURRENT_TIMESTAMP)
                        ON DUPLICATE KEY UPDATE
                            state_json = VALUES(state_json),
                            updated_at = CURRENT_TIMESTAMP
                        """
                    ),
                    {"id": self.deployment_id, "state": payload},
                )
                conn.commit()
        except Exception:
            logger.exception(
                "[checkpoint] Failed to save state for deployment %s", self.deployment_id
            )

    def load(self) -> Optional[Dict[str, Any]]:
        """Load the most recent checkpoint state, or None if absent."""
        try:
            with connection("quantmate") as conn:
                row = conn.execute(
                    text(
                        "SELECT state_json FROM paper_deployment_checkpoints "
                        "WHERE deployment_id = :id"
                    ),
                    {"id": self.deployment_id},
                ).fetchone()
        except Exception:
            logger.exception(
                "[checkpoint] Failed to load state for deployment %s", self.deployment_id
            )
            return None

        if not row:
            return None

        try:
            data = json.loads(row.state_json if hasattr(row, "state_json") else row[0])
            return data.get("state")
        except (json.JSONDecodeError, TypeError):
            logger.warning(
                "[checkpoint] Corrupt checkpoint for deployment %s, ignoring", self.deployment_id
            )
            return None

    def clear(self) -> None:
        """Delete the checkpoint row (used after a clean shutdown or reset)."""
        try:
            with connection("quantmate") as conn:
                conn.execute(
                    text(
                        "DELETE FROM paper_deployment_checkpoints WHERE deployment_id = :id"
                    ),
                    {"id": self.deployment_id},
                )
                conn.commit()
        except Exception:
            logger.exception(
                "[checkpoint] Failed to clear state for deployment %s", self.deployment_id
            )
