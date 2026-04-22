from __future__ import annotations

import json
import os
import time
from typing import Any, Callable, Sequence, TypeVar

from sqlalchemy import text
from app.infrastructure.config.system_config_registry import (
    is_db_system_config_key,
    is_env_only_system_config_key,
)

T = TypeVar("T")

_CACHE_TTL_SECONDS = 30.0
_SYSTEM_CONFIG_CACHE: dict[str, tuple[float, str | None]] = {}

_TRUE_VALUES = {"1", "true", "yes", "on", "y"}
_FALSE_VALUES = {"0", "false", "no", "off", "n"}


def clear_runtime_config_cache() -> None:
    _SYSTEM_CONFIG_CACHE.clear()


def _normalize_env_keys(env_keys: str | Sequence[str] | None) -> list[str]:
    if env_keys is None:
        return []
    if isinstance(env_keys, str):
        return [env_keys]
    return [key for key in env_keys if key]


def _read_env_value(env_keys: str | Sequence[str] | None) -> str | None:
    for key in _normalize_env_keys(env_keys):
        value = os.getenv(key)
        if value not in {None, ""}:
            return value
    return None


def _load_system_config_value(config_key: str) -> str | None:
    try:
        from app.infrastructure.db.connections import get_quantmate_engine

        engine = get_quantmate_engine()
        with engine.connect() as conn:
            row = conn.execute(
                text("SELECT config_value FROM system_configs WHERE config_key = :key"),
                {"key": config_key},
            ).fetchone()
        return row[0] if row else None
    except Exception:
        return None


def _read_system_config_value(config_key: str | None) -> str | None:
    if not config_key:
        return None

    now = time.monotonic()
    cached = _SYSTEM_CONFIG_CACHE.get(config_key)
    if cached is not None and (now - cached[0]) < _CACHE_TTL_SECONDS:
        return cached[1]

    value = _load_system_config_value(config_key)
    _SYSTEM_CONFIG_CACHE[config_key] = (now, value)
    return value


def resolve_runtime_config_value(
    *,
    env_keys: str | Sequence[str] | None = None,
    db_key: str | None = None,
    default: str | None = None,
) -> tuple[str | None, str]:
    if is_db_system_config_key(db_key):
        candidate_values = (
            ("db", _read_system_config_value(db_key)),
            ("legacy_env", _read_env_value(env_keys)),
        )
    elif is_env_only_system_config_key(db_key):
        candidate_values = (("env", _read_env_value(env_keys)),)
    else:
        candidate_values = (
            ("env", _read_env_value(env_keys)),
            ("db", _read_system_config_value(db_key)),
        )

    for source, raw_value in candidate_values:
        if raw_value not in {None, ""}:
            return raw_value, source
    return default, "default"


def _parse_bool(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    normalized = str(value).strip().lower()
    if normalized in _TRUE_VALUES:
        return True
    if normalized in _FALSE_VALUES:
        return False
    raise ValueError(f"Cannot parse boolean from {value!r}")


def get_runtime_config(
    *,
    env_keys: str | Sequence[str] | None = None,
    db_key: str | None = None,
    default: T,
    parser: Callable[[Any], T] | None = None,
) -> T:
    parse = parser or (lambda value: value)
    raw_value, _ = resolve_runtime_config_value(env_keys=env_keys, db_key=db_key, default=None)
    if raw_value not in {None, ""}:
        try:
            return parse(raw_value)
        except Exception:
            pass
    return default


def get_runtime_str(
    *,
    env_keys: str | Sequence[str] | None = None,
    db_key: str | None = None,
    default: str,
) -> str:
    return get_runtime_config(env_keys=env_keys, db_key=db_key, default=default, parser=str)


def get_runtime_int(
    *,
    env_keys: str | Sequence[str] | None = None,
    db_key: str | None = None,
    default: int,
) -> int:
    return get_runtime_config(env_keys=env_keys, db_key=db_key, default=default, parser=int)


def get_runtime_float(
    *,
    env_keys: str | Sequence[str] | None = None,
    db_key: str | None = None,
    default: float,
) -> float:
    return get_runtime_config(env_keys=env_keys, db_key=db_key, default=default, parser=float)


def get_runtime_bool(
    *,
    env_keys: str | Sequence[str] | None = None,
    db_key: str | None = None,
    default: bool,
) -> bool:
    return get_runtime_config(env_keys=env_keys, db_key=db_key, default=default, parser=_parse_bool)


def get_runtime_json(
    *,
    env_keys: str | Sequence[str] | None = None,
    db_key: str | None = None,
    default: T,
) -> T:
    return get_runtime_config(env_keys=env_keys, db_key=db_key, default=default, parser=json.loads)


def get_runtime_csv(
    *,
    env_keys: str | Sequence[str] | None = None,
    db_key: str | None = None,
    default: list[str] | tuple[str, ...],
) -> list[str]:
    def _parse(value: Any) -> list[str]:
        if isinstance(value, list):
            return [str(item).strip() for item in value if str(item).strip()]
        text_value = str(value).strip()
        if text_value.startswith("["):
            try:
                decoded = json.loads(text_value)
            except Exception:
                decoded = None
            if isinstance(decoded, list):
                return [str(item).strip() for item in decoded if str(item).strip()]
        return [item.strip() for item in text_value.split(",") if item.strip()]

    return get_runtime_config(env_keys=env_keys, db_key=db_key, default=list(default), parser=_parse)
