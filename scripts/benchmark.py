from __future__ import annotations

import asyncio
from statistics import mean
from time import perf_counter

from distributed_cache.entrypoint import build_local_cluster


async def benchmark(rounds: int = 1000) -> dict[str, float]:
    cluster, _ = build_local_cluster()

    put_latencies: list[float] = []
    get_latencies: list[float] = []

    for index in range(rounds):
        key = f"bench:{index}"

        started = perf_counter()
        await cluster.put(key, index)
        put_latencies.append(perf_counter() - started)

        started = perf_counter()
        await cluster.get(key)
        get_latencies.append(perf_counter() - started)

    return {
        "put_avg_ms": mean(put_latencies) * 1000,
        "get_avg_ms": mean(get_latencies) * 1000,
        "ops_per_second": rounds / sum(put_latencies + get_latencies),
    }


if __name__ == "__main__":
    print(asyncio.run(benchmark()))
