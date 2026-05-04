from __future__ import annotations

from collections import OrderedDict
from dataclasses import dataclass
from threading import RLock
from time import monotonic
from typing import Any, Optional


@dataclass(frozen=True)
class CacheEntry:
    value: Any
    expires_at: Optional[float] = None


class CacheStore:
    """Thread-safe in-memory cache with TTL and LRU eviction."""

    def __init__(self, max_items: int = 1024) -> None:
        if max_items <= 0:
            raise ValueError("max_items must be positive")
        self._max_items = max_items
        self._entries: OrderedDict[str, CacheEntry] = OrderedDict()
        self._lock = RLock()

    def put(self, key: str, value: Any, ttl_seconds: float | None = None) -> None:
        expires_at = None
        if ttl_seconds is not None:
            if ttl_seconds <= 0:
                self.delete(key)
                return
            expires_at = monotonic() + ttl_seconds

        with self._lock:
            if key in self._entries:
                self._entries.pop(key)
            self._entries[key] = CacheEntry(value=value, expires_at=expires_at)
            self._evict_expired_locked()
            self._evict_lru_locked()

    def get(self, key: str) -> Any | None:
        with self._lock:
            entry = self._entries.get(key)
            if entry is None:
                return None
            if self._is_expired(entry):
                self._entries.pop(key, None)
                return None
            self._entries.move_to_end(key)
            return entry.value

    def delete(self, key: str) -> bool:
        with self._lock:
            return self._entries.pop(key, None) is not None

    def clear(self) -> None:
        with self._lock:
            self._entries.clear()

    def size(self) -> int:
        with self._lock:
            self._evict_expired_locked()
            return len(self._entries)

    def items(self) -> dict[str, Any]:
        with self._lock:
            self._evict_expired_locked()
            return {key: entry.value for key, entry in self._entries.items()}

    def snapshot(self) -> dict[str, dict[str, Any | None]]:
        with self._lock:
            self._evict_expired_locked()
            return {
                key: {"value": entry.value, "expires_at": entry.expires_at}
                for key, entry in self._entries.items()
            }

    def _is_expired(self, entry: CacheEntry) -> bool:
        return entry.expires_at is not None and monotonic() >= entry.expires_at

    def _evict_expired_locked(self) -> None:
        expired_keys = [key for key, entry in self._entries.items() if self._is_expired(entry)]
        for key in expired_keys:
            self._entries.pop(key, None)

    def _evict_lru_locked(self) -> None:
        while len(self._entries) > self._max_items:
            self._entries.popitem(last=False)
