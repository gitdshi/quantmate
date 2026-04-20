"""System status routes."""

import json
import logging
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict, Iterator

from fastapi import APIRouter, Depends, Query
from fastapi.responses import StreamingResponse

from app.api.dependencies.permissions import require_permission
from app.api.errors import ErrorCode
from app.api.exception_handlers import APIError
from app.api.services.auth_service import get_current_user
from app.api.models.user import TokenData
from app.infrastructure.config import get_runtime_str, get_settings

from app.domains.extdata.service import SyncStatusService

router = APIRouter(prefix="/system", tags=["system"])
settings = get_settings()
DEFAULT_BUILD_TIME = datetime.now(timezone.utc).isoformat()
logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class LogModuleSpec:
    key: str
    label: str
    service_names: tuple[str, ...]
    name_fragments: tuple[str, ...]
    include_terms: tuple[str, ...] = ()


LOG_MODULE_SPECS: dict[str, LogModuleSpec] = {
    "api": LogModuleSpec(
        key="api",
        label="API",
        service_names=("api",),
        name_fragments=("quantmate_api", "quantmate-api"),
    ),
    "datasync": LogModuleSpec(
        key="datasync",
        label="DataSync",
        service_names=("datasync",),
        name_fragments=("quantmate_datasync", "quantmate-datasync"),
    ),
    "datasync-init": LogModuleSpec(
        key="datasync-init",
        label="DataSync Init",
        service_names=("datasync",),
        name_fragments=("quantmate_datasync", "quantmate-datasync"),
        include_terms=("init", "initializ", "bootstrap", "reconcile"),
    ),
    "datasync-backfill": LogModuleSpec(
        key="datasync-backfill",
        label="DataSync Backfill",
        service_names=("datasync-backfill", "worker"),
        name_fragments=(
            "quantmate_datasync_backfill",
            "quantmate-datasync-backfill",
            "quantmate_worker",
            "quantmate-worker",
        ),
        include_terms=("backfill",),
    ),
    "worker": LogModuleSpec(
        key="worker",
        label="Worker",
        service_names=("worker",),
        name_fragments=("quantmate_worker", "quantmate-worker"),
    ),
    "rdagent": LogModuleSpec(
        key="rdagent",
        label="RDAgent",
        service_names=("rdagent-service", "rdagent"),
        name_fragments=("quantmate_rdagent", "quantmate-rdagent", "rdagent"),
    ),
    "portal": LogModuleSpec(
        key="portal",
        label="Portal",
        service_names=("portal",),
        name_fragments=("quantmate_portal", "quantmate-portal"),
    ),
}

LOG_STREAM_HEADERS = {
    "Cache-Control": "no-cache",
    "Connection": "keep-alive",
    "X-Accel-Buffering": "no",
}


@router.get("/sync-status", dependencies=[require_permission("system", "read")])
async def get_sync_status(current_user: TokenData = Depends(get_current_user)) -> Dict[str, Any]:
    return SyncStatusService().get_sync_status()


@router.get("/version")
async def get_version_info() -> Dict[str, str]:
    return {
        "name": settings.app_name,
        "version": settings.app_version,
        "build_time": get_runtime_str(
            env_keys="APP_BUILD_TIME",
            db_key="app.build_time",
            default=DEFAULT_BUILD_TIME,
        ),
        "environment": settings.environment,
    }


def _normalize_log_module(module: str) -> str:
    return str(module or "").strip().lower().replace("_", "-")


def _get_log_module_spec(module: str) -> LogModuleSpec:
    normalized = _normalize_log_module(module)
    spec = LOG_MODULE_SPECS.get(normalized)
    if spec is None:
        raise APIError(
            status_code=400,
            code=ErrorCode.BAD_REQUEST,
            message=f"Unsupported log module: {module}",
        )
    return spec


def _get_docker_client():
    try:
        import docker

        return docker.from_env()
    except Exception as exc:
        raise APIError(
            status_code=503,
            code=ErrorCode.INTERNAL_ERROR,
            message=f"Docker log streaming is unavailable: {exc}",
        ) from exc


def _container_matches_spec(container: Any, spec: LogModuleSpec) -> bool:
    labels = getattr(container, "labels", {}) or {}
    service_name = str(labels.get("com.docker.compose.service") or "").strip().lower()
    if service_name in spec.service_names:
        return True

    container_name = str(getattr(container, "name", "") or "").strip().lower()
    return any(fragment in container_name for fragment in spec.name_fragments)


def _container_match_priority(container: Any, spec: LogModuleSpec) -> tuple[int, str]:
    labels = getattr(container, "labels", {}) or {}
    service_name = str(labels.get("com.docker.compose.service") or "").strip().lower()
    if service_name in spec.service_names:
        return spec.service_names.index(service_name), str(getattr(container, "name", "") or "")

    container_name = str(getattr(container, "name", "") or "").strip().lower()
    for index, fragment in enumerate(spec.name_fragments):
        if fragment in container_name:
            return len(spec.service_names) + index, container_name

    return len(spec.service_names) + len(spec.name_fragments), container_name


def _resolve_log_container(client: Any, spec: LogModuleSpec) -> Any:
    try:
        containers = client.containers.list(all=True)
    except Exception as exc:
        raise APIError(
            status_code=503,
            code=ErrorCode.INTERNAL_ERROR,
            message=f"Failed to enumerate Docker containers: {exc}",
        ) from exc

    matched = [container for container in containers if _container_matches_spec(container, spec)]
    if not matched:
        raise APIError(
            status_code=404,
            code=ErrorCode.NOT_FOUND,
            message=f"No container found for log module: {spec.key}",
        )

    running = [container for container in matched if str(getattr(container, "status", "")).lower() == "running"]
    candidates = running or matched
    candidates.sort(key=lambda container: _container_match_priority(container, spec))
    return candidates[0]


def _log_line_matches_spec(spec: LogModuleSpec, line: str) -> bool:
    if not spec.include_terms:
        return True
    lowered = line.lower()
    return any(term in lowered for term in spec.include_terms)


def _format_sse_event(event: str, payload: dict[str, Any]) -> str:
    return f"event: {event}\ndata: {json.dumps(payload)}\n\n"


def create_log_stream(module: str, tail: int = 200) -> Iterator[str]:
    spec = _get_log_module_spec(module)
    client = _get_docker_client()
    container = _resolve_log_container(client, spec)

    def event_stream() -> Iterator[str]:
        yield _format_sse_event(
            "meta",
            {
                "type": "meta",
                "module": spec.key,
                "container": str(getattr(container, "name", "")),
                "tail": tail,
            },
        )

        try:
            for raw_line in container.logs(
                stream=True,
                follow=True,
                tail=tail,
                timestamps=True,
                stdout=True,
                stderr=True,
            ):
                line = raw_line.decode("utf-8", errors="replace").rstrip()
                if not line or not _log_line_matches_spec(spec, line):
                    continue
                yield _format_sse_event(
                    "log",
                    {
                        "type": "log",
                        "module": spec.key,
                        "container": str(getattr(container, "name", "")),
                        "line": line,
                    },
                )
        except Exception as exc:
            logger.warning("Log streaming for %s failed: %s", spec.key, exc, exc_info=True)
            yield _format_sse_event(
                "error",
                {
                    "type": "error",
                    "module": spec.key,
                    "container": str(getattr(container, "name", "")),
                    "message": str(exc),
                },
            )
        finally:
            try:
                client.close()
            except Exception:
                logger.debug("Failed to close Docker client for log stream", exc_info=True)

    return event_stream()


@router.get("/logs/modules", dependencies=[require_permission("system", "read")])
async def list_log_modules(current_user: TokenData = Depends(get_current_user)) -> Dict[str, list[Dict[str, str]]]:
    return {
        "data": [
            {"key": spec.key, "label": spec.label}
            for spec in LOG_MODULE_SPECS.values()
        ]
    }


@router.get("/logs/stream", dependencies=[require_permission("system", "read")])
async def stream_system_logs(
    module: str = Query("api", description="Log module key"),
    tail: int = Query(200, ge=1, le=2000, description="Initial number of lines to tail"),
    current_user: TokenData = Depends(get_current_user),
) -> StreamingResponse:
    return StreamingResponse(
        create_log_stream(module, tail=tail),
        media_type="text/event-stream",
        headers=LOG_STREAM_HEADERS,
    )
