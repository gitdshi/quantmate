"""Paper runtime daemon.

This daemon reconciles desired deployment state stored in the database with the
actual in-process paper runtime sessions owned by PaperRuntimeService.
"""

from __future__ import annotations

import argparse
from datetime import datetime
import json
import logging
import socket
import time
from typing import Any, Dict

from sqlalchemy import text

from app.domains.trading.paper_runtime_service import PaperRuntimeService
from app.infrastructure.config import get_runtime_float
from app.infrastructure.db.connections import connection

logger = logging.getLogger(__name__)


def _poll_interval_seconds() -> float:
    return get_runtime_float(
        env_keys="PAPER_RUNTIME_DAEMON_POLL_INTERVAL_SECONDS",
        db_key="paper_runtime.daemon_poll_interval_seconds",
        default=3.0,
    )


class PaperRuntimeDaemon:
    def __init__(self, runtime_service: PaperRuntimeService | None = None, worker_id: str | None = None) -> None:
        self.runtime_service = runtime_service or PaperRuntimeService()
        self.worker_id = worker_id or socket.gethostname()

    def run_forever(self) -> None:
        logger.info("[paper-runtime-daemon] started worker_id=%s", self.worker_id)
        while True:
            self.run_once()
            time.sleep(_poll_interval_seconds())

    def run_once(self) -> None:
        deployments = self._fetch_deployments()
        desired_running: set[int] = set()
        handled: set[int] = set()

        for deployment in deployments:
            deployment_id = deployment["id"]
            handled.add(deployment_id)
            if deployment["desired_status"] == "running":
                desired_running.add(deployment_id)
                self._ensure_running(deployment)
            else:
                self._ensure_stopped(deployment_id)

        for deployment_id in list(self.runtime_service._sessions.keys()):
            if deployment_id not in desired_running and deployment_id not in handled:
                self._ensure_stopped(deployment_id)

    def _ensure_running(self, deployment: Dict[str, Any]) -> None:
        deployment_id = deployment["id"]
        runtime = self.runtime_service.get_runtime(deployment_id)
        if runtime and runtime.get("status") == "running":
            self._touch_heartbeat(deployment_id, runtime)
            return

        result = self.runtime_service.start_deployment(
            deployment_id=deployment_id,
            paper_account_id=deployment["paper_account_id"],
            user_id=deployment["user_id"],
            strategy_id=deployment.get("strategy_id"),
            composite_strategy_id=deployment.get("composite_strategy_id"),
            strategy_source_type=deployment.get("strategy_source_type") or "strategy",
            strategy_name=deployment["strategy_name"],
            vt_symbol=deployment["vt_symbol"],
            parameters=deployment.get("parameters") or {},
            execution_mode=deployment.get("execution_mode") or "auto",
        )
        runtime = result.get("runtime") or self.runtime_service.preview_runtime(
            deployment_id=deployment_id,
            paper_account_id=deployment["paper_account_id"],
            user_id=deployment["user_id"],
            strategy_id=deployment.get("strategy_id"),
            composite_strategy_id=deployment.get("composite_strategy_id"),
            strategy_source_type=deployment.get("strategy_source_type") or "strategy",
            strategy_name=deployment["strategy_name"],
            vt_symbol=deployment["vt_symbol"],
            parameters=deployment.get("parameters") or {},
        )

        if result.get("success"):
            self._touch_heartbeat(deployment_id, runtime)
            return

        self._update_runtime_status(
            deployment_id=deployment_id,
            runtime_status="error",
            runtime_mode=runtime.get("runtime_mode", "native_cta_runtime"),
            strategy_kind=runtime.get("strategy_kind", "cta"),
            gateway_name=runtime.get("gateway_name"),
            message=result.get("error"),
        )

    def _ensure_stopped(self, deployment_id: int) -> None:
        result = self.runtime_service.stop_deployment(deployment_id)
        runtime = result.get("runtime")
        runtime_mode = runtime.get("runtime_mode") if runtime else "native_cta_runtime"
        strategy_kind = runtime.get("strategy_kind") if runtime else "cta"
        gateway_name = runtime.get("gateway_name") if runtime else None
        self._update_runtime_status(
            deployment_id=deployment_id,
            runtime_status="stopped",
            runtime_mode=runtime_mode,
            strategy_kind=strategy_kind,
            gateway_name=gateway_name,
            message=None,
        )

    def _touch_heartbeat(self, deployment_id: int, runtime: Dict[str, Any]) -> None:
        warning_text = "\n".join(runtime.get("warnings") or []) or None
        self._update_runtime_status(
            deployment_id=deployment_id,
            runtime_status=runtime.get("status", "running"),
            runtime_mode=runtime.get("runtime_mode", "native_cta_runtime"),
            strategy_kind=runtime.get("strategy_kind", "cta"),
            gateway_name=runtime.get("gateway_name"),
            message=warning_text,
        )

    def _fetch_deployments(self) -> list[Dict[str, Any]]:
        with connection("quantmate") as conn:
            rows = conn.execute(
                text(
                    """
                      SELECT id, user_id, paper_account_id, strategy_id, composite_strategy_id,
                          strategy_source_type, strategy_name, vt_symbol,
                          parameters, execution_mode, desired_status
                    FROM paper_deployments
                    WHERE paper_account_id IS NOT NULL
                      AND desired_status IN ('running', 'stopped')
                    ORDER BY id ASC
                    """
                )
            ).fetchall()

        deployments: list[Dict[str, Any]] = []
        for row in rows:
            raw_parameters = row.parameters if hasattr(row, "parameters") else None
            if isinstance(raw_parameters, str):
                parameters = json.loads(raw_parameters) if raw_parameters else {}
            else:
                parameters = raw_parameters or {}
            deployments.append(
                {
                    "id": row.id,
                    "user_id": row.user_id,
                    "paper_account_id": row.paper_account_id,
                    "strategy_id": getattr(row, "strategy_id", None),
                    "composite_strategy_id": getattr(row, "composite_strategy_id", None),
                    "strategy_source_type": getattr(row, "strategy_source_type", "strategy"),
                    "strategy_name": row.strategy_name,
                    "vt_symbol": row.vt_symbol,
                    "parameters": parameters,
                    "execution_mode": getattr(row, "execution_mode", "auto"),
                    "desired_status": getattr(row, "desired_status", "running"),
                }
            )
        return deployments

    def _update_runtime_status(
        self,
        *,
        deployment_id: int,
        runtime_status: str,
        runtime_mode: str,
        strategy_kind: str,
        gateway_name: str | None,
        message: str | None,
    ) -> None:
        now = datetime.utcnow()
        with connection("quantmate") as conn:
            conn.execute(
                text(
                    """
                    UPDATE paper_deployments
                    SET runtime_status = :runtime_status,
                        runtime_worker_id = :worker_id,
                        runtime_heartbeat_at = :heartbeat_at,
                        runtime_error = CASE WHEN :runtime_status = 'error' THEN :message ELSE NULL END,
                        runtime_warning = CASE WHEN :runtime_status <> 'error' THEN :message ELSE runtime_warning END
                    WHERE id = :deployment_id
                    """
                ),
                {
                    "deployment_id": deployment_id,
                    "runtime_status": runtime_status,
                    "worker_id": self.worker_id,
                    "heartbeat_at": now,
                    "message": message,
                },
            )
            conn.execute(
                text(
                    """
                    INSERT INTO paper_runtime_heartbeats (
                        deployment_id, worker_id, runtime_status, runtime_mode,
                        strategy_kind, gateway_name, message, heartbeat_at
                    ) VALUES (
                        :deployment_id, :worker_id, :runtime_status, :runtime_mode,
                        :strategy_kind, :gateway_name, :message, :heartbeat_at
                    )
                    ON DUPLICATE KEY UPDATE
                        worker_id = VALUES(worker_id),
                        runtime_status = VALUES(runtime_status),
                        runtime_mode = VALUES(runtime_mode),
                        strategy_kind = VALUES(strategy_kind),
                        gateway_name = VALUES(gateway_name),
                        message = VALUES(message),
                        heartbeat_at = VALUES(heartbeat_at)
                    """
                ),
                {
                    "deployment_id": deployment_id,
                    "worker_id": self.worker_id,
                    "runtime_status": runtime_status,
                    "runtime_mode": runtime_mode,
                    "strategy_kind": strategy_kind,
                    "gateway_name": gateway_name,
                    "message": message,
                    "heartbeat_at": now,
                },
            )
            conn.commit()


def main() -> None:
    parser = argparse.ArgumentParser(description="Run the paper runtime daemon")
    parser.add_argument("--once", action="store_true", help="Run a single reconciliation pass and exit")
    args = parser.parse_args()

    daemon = PaperRuntimeDaemon()
    if args.once:
        daemon.run_once()
        return
    daemon.run_forever()


if __name__ == "__main__":
    main()