import time

from distributed_cache.cache.store import CacheStore


def test_put_get_and_delete() -> None:
    store = CacheStore(max_items=10)

    store.put("alpha", "one")

    assert store.get("alpha") == "one"
    assert store.delete("alpha") is True
    assert store.get("alpha") is None


def test_ttl_expires_entry() -> None:
    store = CacheStore(max_items=10)

    store.put("temp", "value", ttl_seconds=0.01)

    assert store.get("temp") == "value"
    time.sleep(0.02)
    assert store.get("temp") is None


def test_lru_eviction_removes_least_recently_used() -> None:
    store = CacheStore(max_items=2)

    store.put("a", 1)
    store.put("b", 2)
    assert store.get("a") == 1
    store.put("c", 3)

    assert store.get("b") is None
    assert store.get("a") == 1
    assert store.get("c") == 3
