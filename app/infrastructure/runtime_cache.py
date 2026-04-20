from __future__ import annotations

from collections import OrderedDict
from dataclasses import dataclass
from threading import RLock
from time import monotonic
from typing import Any, Callable


_MISSING = object()


@dataclass(slots=True)
class _CacheEntry:
    value: Any
    expires_at: float
    updated_at: float


class ExpiringCache:
    """Small in-process TTL cache for short-lived API snapshots."""

    def __init__(self, *, name: str, maxsize: int = 128) -> None:
        self.name = name
        self.maxsize = maxsize
        self._entries: OrderedDict[Any, _CacheEntry] = OrderedDict()
        self._lock = RLock()

    def clear(self) -> None:
        with self._lock:
            self._entries.clear()

    def get(self, key: Any) -> Any | None:
        now = monotonic()
        with self._lock:
            entry = self._entries.get(key)
            if entry is None:
                return None
            if entry.expires_at <= now:
                return None
            self._entries.move_to_end(key)
            return entry.value

    def set(self, key: Any, value: Any, *, ttl_seconds: int) -> Any:
        now = monotonic()
        entry = _CacheEntry(value=value, expires_at=now + max(ttl_seconds, 1), updated_at=now)
        with self._lock:
            self._entries[key] = entry
            self._entries.move_to_end(key)
            while len(self._entries) > self.maxsize:
                self._entries.popitem(last=False)
        return value

    def get_or_load(
        self,
        key: Any,
        loader: Callable[[], Any],
        *,
        ttl_seconds: int,
        stale_if_error: bool = False,
    ) -> Any:
        now = monotonic()
        stale_value = _MISSING

        with self._lock:
            entry = self._entries.get(key)
            if entry is not None:
                stale_value = entry.value
                if entry.expires_at > now:
                    self._entries.move_to_end(key)
                    return entry.value

        try:
            value = loader()
        except Exception:
            if stale_if_error and stale_value is not _MISSING:
                return stale_value
            raise

        return self.set(key, value, ttl_seconds=ttl_seconds)