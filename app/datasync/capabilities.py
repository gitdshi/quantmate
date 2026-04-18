"""Capability evaluation for registry-backed data source interfaces."""

from __future__ import annotations

import json
import logging
from typing import Any, Iterable, Mapping

from app.datasync.registry import DataSourceRegistry
from app.infrastructure.config import get_runtime_csv, get_runtime_int

_TRUE_VALUES = {"1", "true", "yes", "on", "y", "paid"}
_DEFAULT_TUSHARE_TOKEN_POINTS = 2000

logger = logging.getLogger(__name__)


def load_source_config_map(source: str | None = None) -> dict[str, dict[str, Any]]:
    from app.domains.market.dao.data_source_item_dao import DataSourceConfigDao

    try:
        rows = DataSourceConfigDao().list_all()
    except Exception:
        logger.warning("Falling back to empty source capability config for %s", source or "all sources", exc_info=True)
        return {}

    result: dict[str, dict[str, Any]] = {}
    for row in rows:
        source_key = str(row.get("source_key") or "").strip()
        if not source_key:
            continue
        if source is not None and source_key != source:
            continue
        result[source_key] = dict(row)
    return result


def build_supported_item_keys(
    registry: DataSourceRegistry,
    items: Iterable[Mapping[str, Any]],
    *,
    source_configs: Mapping[str, Mapping[str, Any]] | None = None,
) -> set[tuple[str, str]]:
    supported: set[tuple[str, str]] = set()
    for item in items:
        if is_item_sync_supported(registry, item, source_configs=source_configs):
            source = str(item.get("source") or "").strip()
            item_key = str(item.get("item_key") or "").strip()
            if source and item_key:
                supported.add((source, item_key))
    return supported


def is_item_sync_supported(
    registry: DataSourceRegistry,
    item: Mapping[str, Any],
    *,
    source_configs: Mapping[str, Mapping[str, Any]] | None = None,
) -> bool:
    source = str(item.get("source") or "").strip()
    item_key = str(item.get("item_key") or "").strip()
    if not source or not item_key:
        return False

    iface = registry.get_interface(source, item_key)
    if iface is None:
        return False

    if source != "tushare":
        return True

    source_config = (source_configs or {}).get(source)
    return _is_tushare_item_supported(item, source_config=source_config)


def _is_tushare_item_supported(item: Mapping[str, Any], *, source_config: Mapping[str, Any] | None = None) -> bool:
    config = _parse_source_config_json(source_config)
    item_key = str(item.get("item_key") or "").strip()
    api_name = str(item.get("api_name") or item_key).strip()
    permission_points = _parse_required_points(item.get("permission_points"))
    requires_permission = str(item.get("requires_permission") or "").strip().lower()
    requires_explicit_grant = _requires_explicit_grant(requires_permission)

    granted_names = {
        name.lower()
        for name in get_runtime_csv(
            env_keys=("TUSHARE_GRANTED_API_NAMES", "DATASYNC_TUSHARE_GRANTED_API_NAMES"),
            db_key="datasync.tushare.granted_api_names",
            default=_config_list(
                config,
                (
                    "granted_api_names",
                    "allowed_api_names",
                    "supported_api_names",
                    "granted_interfaces",
                    "allowed_interfaces",
                    "supported_interfaces",
                ),
            ),
        )
    }
    has_explicit_grant = item_key.lower() in granted_names or api_name.lower() in granted_names

    if requires_explicit_grant:
        return has_explicit_grant

    if permission_points > 0:
        token_points = get_runtime_int(
            env_keys=("TUSHARE_TOKEN_POINTS", "DATASYNC_TUSHARE_TOKEN_POINTS"),
            db_key="datasync.tushare.token_points",
            default=_config_int(config, ("token_points", "points", "token_permission_points"), _DEFAULT_TUSHARE_TOKEN_POINTS),
        )
        return token_points >= permission_points

    return True


def _parse_source_config_json(source_config: Mapping[str, Any] | None) -> dict[str, Any]:
    if not source_config:
        return {}

    raw = source_config.get("config_json")
    if raw is None:
        return {}
    if isinstance(raw, dict):
        return dict(raw)
    if isinstance(raw, str):
        try:
            parsed = json.loads(raw)
        except Exception:
            return {}
        if isinstance(parsed, dict):
            return parsed
    return {}


def _config_list(config: Mapping[str, Any], keys: tuple[str, ...]) -> list[str]:
    for key in keys:
        value = config.get(key)
        if isinstance(value, list):
            return [str(item).strip() for item in value if str(item).strip()]
        if isinstance(value, str) and value.strip():
            try:
                decoded = json.loads(value)
            except Exception:
                decoded = None
            if isinstance(decoded, list):
                return [str(item).strip() for item in decoded if str(item).strip()]
            return [part.strip() for part in value.split(",") if part.strip()]
    return []


def _config_int(config: Mapping[str, Any], keys: tuple[str, ...], default: int) -> int:
    for key in keys:
        value = config.get(key)
        if value in {None, ""}:
            continue
        try:
            return int(value)
        except Exception:
            continue
    return default


def _parse_required_points(permission_points: object) -> int:
    if permission_points in {None, ""}:
        return 0
    try:
        return max(0, int(permission_points))
    except Exception:
        return 0


def _requires_explicit_grant(requires_permission: str) -> bool:
    return requires_permission in _TRUE_VALUES