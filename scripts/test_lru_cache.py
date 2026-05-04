#!/usr/bin/env python3

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))

from distributed_cache.cache.store import CacheStore
from time import sleep


def print_cache(cache, title=""):
    if title:
        print(f"\n{'='*60}")
        print(f"{title}")
        print(f"{'='*60}")
    
    snapshot = cache.snapshot()
    print(f"Cache size: {len(snapshot)}/{cache._max_items}")
    print(f"Order (LRU → MRU): {list(snapshot.keys())}")
    for i, (key, entry) in enumerate(snapshot.items(), 1):
        print(f"  [{i}] {key}: {entry['value']}")


def main():
    cache = CacheStore(max_items=5)
    
    print("\n" + "="*60)
    print("LRU CACHE TEST")
    print("="*60)
    
    print("\n1. INSERTING 5 ITEMS")
    items = [("key1", "Alice"), ("key2", "Bob"), ("key3", "Charlie"), 
             ("key4", "Diana"), ("key5", "Eve")]
    for key, val in items:
        cache.put(key, val)
        print(f"   Inserted {key}={val}")
    
    print_cache(cache, "After inserting 5 items:")
    
    print("\n2. ACCESSING SOME ITEMS")
    print("   Accessing key2")
    cache.get("key2")
    print_cache(cache, "After accessing key2:")
    
    print("\n   Accessing key4")
    cache.get("key4")
    print_cache(cache, "After accessing key4:")
    
    print("\n3. INSERTING 6TH ITEM (SIZE CHECK & EVICTION)")
    print("   Inserting key6=Frank")
    cache.put("key6", "Frank")
    print_cache(cache, "After inserting key6 (LRU evicted):")
    print("   → key1 was evicted (Least Recently Used)")
    
    print("\n4. ADDING ITEMS WITH 2.5s TTL")
    cache.put("ttl_key1", "TTL_VALUE_1", ttl_seconds=2.5)
    cache.put("ttl_key2", "TTL_VALUE_2", ttl_seconds=2.5)
    print("   Added ttl_key1 and ttl_key2 with 2.5s TTL")
    print_cache(cache, "After adding TTL items:")
    
    print("\n5. SLEEP 1 SECOND")
    print("   Sleeping for 1 second...")
    sleep(1)
    print_cache(cache, "After 1 second:")
    print("   → TTL items still present (1 sec < 2.5 sec)")
    
    print("\n6. SLEEP 2 MORE SECOND (TOTAL 3 SECONDS)")
    print("   Sleeping for 2 more seconds...")
    sleep(2)
    print_cache(cache, "After 3 seconds total:")
    print("   → TTL expired (3 sec >= 2.5 sec)")
    print("   Accessing ttl_key1...")
    val = cache.get("ttl_key1")
    print(f"   Retrieved: {val} (None = expired & removed)")
    print_cache(cache, "Final cache after accessing expired key:")


if __name__ == "__main__":
    main()
