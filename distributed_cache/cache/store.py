from __future__ import annotations

import heapq
from collections import OrderedDict
from dataclasses import dataclass
from threading import RLock, Thread, Event
from time import monotonic, sleep
from typing import Any, Optional


@dataclass(frozen=True)
class CacheEntry:
    value: Any
    expires_at: Optional[float] = None


class CacheStore:
    """Thread-safe in-memory cache with TTL and LRU eviction."""

    def __init__(self, max_items: int = 1024, cleanup_interval: float = 5.0) -> None:
        if max_items <= 0:
            raise ValueError("max_items must be positive")
        self._max_items = max_items
        self._entries: OrderedDict[str, CacheEntry] = OrderedDict()
        self._lock = RLock()
        
        # Min-heap for expiration tracking (time, key)
        self._expiration_heap: list[tuple[float, str]] = []
        
        # Background cleanup thread
        self._cleanup_interval = cleanup_interval
        self._stop_cleanup = Event()
        self._cleanup_thread = Thread(target=self._cleanup_worker, daemon=True)
        self._cleanup_thread.start()

    def put(self, key: str, value: Any, ttl_seconds: float | None = None) -> None:
        expires_at = None
        if ttl_seconds is not None:
            if ttl_seconds <= 0:
                self.delete(key)
                return
            expires_at = monotonic() + ttl_seconds

        with self._lock:
            # Remove old entry if exists
            if key in self._entries:
                self._entries.pop(key)
            
            # Add new entry
            self._entries[key] = CacheEntry(value=value, expires_at=expires_at)
            
            # Add to expiration heap if has TTL
            if expires_at is not None:
                heapq.heappush(self._expiration_heap, (expires_at, key))
            
            # LRU eviction (no expiration scan)
            self._evict_lru_locked()

    def get(self, key: str) -> Any | None:
        with self._lock:
            entry = self._entries.get(key)
            if entry is None:
                return None
            # Lazy expiration: only check this specific entry
            if self._is_expired(entry):
                self._entries.pop(key, None)
                return None
            # Mark as recently used
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
            # No full scan; background thread handles expiration
            return len(self._entries)

    def items(self) -> dict[str, Any]:
        with self._lock:
            # Lazy expiration: only remove expired items on-the-fly
            result = {}
            for key, entry in list(self._entries.items()):
                if not self._is_expired(entry):
                    result[key] = entry.value
                else:
                    self._entries.pop(key, None)
            return result

    def snapshot(self) -> dict[str, dict[str, Any | None]]:
        with self._lock:
            # Lazy expiration: skip expired items
            result = {}
            for key, entry in list(self._entries.items()):
                if not self._is_expired(entry):
                    result[key] = {"value": entry.value, "expires_at": entry.expires_at}
                else:
                    self._entries.pop(key, None)
            return result

    def _is_expired(self, entry: CacheEntry) -> bool:
        return entry.expires_at is not None and monotonic() >= entry.expires_at

    def _cleanup_worker(self) -> None:
        """Background thread that periodically removes expired entries from heap and cache."""
        while not self._stop_cleanup.is_set():
            try:
                with self._lock:
                    # Process heap: remove expired items until we hit a valid one
                    while self._expiration_heap:
                        expires_at, key = self._expiration_heap[0]
                        
                        # If this item hasn't expired yet, stop
                        if monotonic() < expires_at:
                            break
                        
                        # Remove from heap
                        heapq.heappop(self._expiration_heap)
                        
                        # Check if still in cache and verify it's really expired
                        if key in self._entries:
                            entry = self._entries[key]
                            if self._is_expired(entry):
                                self._entries.pop(key, None)
                
                # Sleep until next cleanup cycle
                self._stop_cleanup.wait(self._cleanup_interval)
            except Exception:
                # Continue cleanup even if error occurs
                pass

    def shutdown(self) -> None:
        """Stop the background cleanup thread."""
        self._stop_cleanup.set()
        self._cleanup_thread.join(timeout=2.0)

    def _evict_lru_locked(self) -> None:
        """Evict oldest (least recently used) items when cache exceeds max_items."""
        while len(self._entries) > self._max_items:
            self._entries.popitem(last=False)
