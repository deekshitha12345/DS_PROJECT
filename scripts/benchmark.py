from __future__ import annotations

import asyncio
import argparse
import math
import subprocess
import tempfile
import sys
from collections import Counter
from pathlib import Path
from statistics import mean
from time import perf_counter
from xml.sax.saxutils import escape

project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from distributed_cache.cluster.consistent_hash import ConsistentHashRing
from distributed_cache.cluster.models import NodeConfig
from distributed_cache.entrypoint import build_local_cluster


NODE_COLORS = [
    "#0f766e",
    "#2563eb",
    "#dc2626",
    "#7c3aed",
    "#ca8a04",
    "#16a34a",
    "#ea580c",
    "#be185d",
    "#0891b2",
    "#4f46e5",
]


def _build_nodes(count: int) -> list[NodeConfig]:
    return [NodeConfig(f"node-{index + 1}", "127.0.0.1", 8001 + index) for index in range(count)]


def _build_ring(node_count: int, replicas: int) -> ConsistentHashRing:
    ring = ConsistentHashRing(replicas=replicas)
    for node in _build_nodes(node_count):
        ring.add_node(node)
    return ring


def _sample_keys(count: int) -> list[str]:
    return [f"sample:{index}" for index in range(count)]


def _ownership_counts(ring: ConsistentHashRing, sample_keys: list[str]) -> list[tuple[str, int]]:
    counts = Counter()
    for key in sample_keys:
        owner = ring.get_node(key)
        if owner is not None:
            counts[owner.node_id] += 1

    ordered_nodes = ring.get_nodes()
    return [(node.node_id, counts.get(node.node_id, 0)) for node in ordered_nodes]


def _html_color(node_index: int) -> str:
    return NODE_COLORS[node_index % len(NODE_COLORS)]


def _frame_spread(counts: list[tuple[str, int]]) -> float:
    total = sum(value for _, value in counts)
    if total == 0 or not counts:
        return 0.0
    shares = [value / total for _, value in counts]
    return (max(shares) - min(shares)) * 100


def _ring_svg(node_count: int, replicas: int, sample_keys: list[str]) -> str:
    ring = _build_ring(node_count, replicas)
    counts = _ownership_counts(ring, sample_keys)
    spread = _frame_spread(counts)

    width = 1400
    height = 840
    center_x = 350
    center_y = 400
    radius = 240
    ring_radius = 285

    node_colors = {
        node.node_id: _html_color(index)
        for index, node in enumerate(ring.get_nodes())
    }
    node_order = [node_id for node_id, _ in counts]
    max_count = max((count for _, count in counts), default=1)

    svg: list[str] = [
        f'<svg xmlns="http://www.w3.org/2000/svg" width="{width}" height="{height}" viewBox="0 0 {width} {height}">',
        '<rect width="100%" height="100%" fill="#f8fafc"/>',
        f'<text x="56" y="58" font-family="DejaVu Sans, Arial, sans-serif" font-size="28" font-weight="700" fill="#0f172a">Consistent hash ring after adding {node_count} server(s)</text>',
        f'<text x="56" y="92" font-family="DejaVu Sans, Arial, sans-serif" font-size="18" fill="#334155">{replicas} virtual nodes per server · sample ownership spread {spread:.1f}%</text>',
        f'<text x="56" y="140" font-family="DejaVu Sans Mono, monospace" font-size="14" fill="#475569">Ring view</text>',
        f'<circle cx="{center_x}" cy="{center_y}" r="{radius}" fill="#ffffff" stroke="#cbd5e1" stroke-width="3"/>',
    ]

    for angle in range(0, 360, 30):
        radians = math.radians(angle)
        x = center_x + math.cos(radians) * radius
        y = center_y + math.sin(radians) * radius
        svg.append(
            f'<line x1="{center_x}" y1="{center_y}" x2="{x:.2f}" y2="{y:.2f}" stroke="#e2e8f0" stroke-width="1"/>'
        )

    svg.append(f'<circle cx="{center_x}" cy="{center_y}" r="4" fill="#0f172a"/>')

    for ring_position, node in ring.virtual_nodes():
        angle = (ring_position / (1 << 256)) * (2 * math.pi)
        x = center_x + math.cos(angle) * ring_radius
        y = center_y + math.sin(angle) * ring_radius
        color = node_colors[node.node_id]
        svg.append(
            f'<circle cx="{x:.2f}" cy="{y:.2f}" r="8" fill="{color}" stroke="#ffffff" stroke-width="2"/>'
        )

    svg.append(f'<text x="56" y="712" font-family="DejaVu Sans Mono, monospace" font-size="14" fill="#475569">Node ownership distribution</text>')

    chart_x = 620
    chart_y = 170
    chart_width = 650
    chart_height = 460
    bar_gap = 18
    bar_width = max(18, int((chart_width - (len(counts) - 1) * bar_gap) / max(1, len(counts))))
    baseline = chart_y + chart_height

    svg.append(f'<rect x="{chart_x}" y="{chart_y}" width="{chart_width}" height="{chart_height}" rx="20" fill="#ffffff" stroke="#cbd5e1"/>')
    svg.append(f'<line x1="{chart_x + 40}" y1="{baseline}" x2="{chart_x + chart_width - 24}" y2="{baseline}" stroke="#94a3b8" stroke-width="2"/>')

    for tick in range(0, 6):
        tick_value = max_count * tick / 5
        tick_y = baseline - (chart_height - 70) * tick / 5
        svg.append(f'<line x1="{chart_x + 34}" y1="{tick_y:.2f}" x2="{chart_x + chart_width - 20}" y2="{tick_y:.2f}" stroke="#e2e8f0" stroke-width="1"/>')
        svg.append(
            f'<text x="{chart_x + 8}" y="{tick_y + 5:.2f}" font-family="DejaVu Sans Mono, monospace" font-size="12" fill="#64748b">{int(round(tick_value))}</text>'
        )

    for index, (node_id, count) in enumerate(counts):
        if node_id not in node_colors:
            continue
        color = node_colors[node_id]
        bar_height = 0 if max_count == 0 else (count / max_count) * (chart_height - 70)
        x = chart_x + 40 + index * (bar_width + bar_gap)
        y = baseline - bar_height
        svg.append(f'<rect x="{x}" y="{y:.2f}" width="{bar_width}" height="{bar_height:.2f}" rx="8" fill="{color}"/>')
        svg.append(
            f'<text x="{x + bar_width / 2:.2f}" y="{y - 10:.2f}" text-anchor="middle" font-family="DejaVu Sans Mono, monospace" font-size="12" fill="#0f172a">{count}</text>'
        )
        svg.append(
            f'<text x="{x + bar_width / 2:.2f}" y="{baseline + 22:.2f}" text-anchor="middle" font-family="DejaVu Sans Mono, monospace" font-size="12" fill="#334155">{escape(node_id)}</text>'
        )

    legend_y = 740
    svg.append(f'<text x="56" y="{legend_y}" font-family="DejaVu Sans Mono, monospace" font-size="14" fill="#475569">Legend</text>')
    for index, node_id in enumerate(node_order):
        legend_x = 56 + (index % 3) * 190
        legend_row = legend_y + 26 + (index // 3) * 28
        color = node_colors[node_id]
        svg.append(f'<rect x="{legend_x}" y="{legend_row - 12}" width="14" height="14" rx="4" fill="{color}"/>')
        svg.append(
            f'<text x="{legend_x + 20}" y="{legend_row}" font-family="DejaVu Sans Mono, monospace" font-size="12" fill="#0f172a">{escape(node_id)}</text>'
        )

    svg.append("</svg>")
    return "\n".join(svg)


def _count_changed_keys(before_ring: ConsistentHashRing, after_ring: ConsistentHashRing, sample_keys: list[str]) -> tuple[int, list[str]]:
    changed_keys: list[str] = []
    for key in sample_keys:
        before_owner = before_ring.get_node(key)
        after_owner = after_ring.get_node(key)
        before_id = before_owner.node_id if before_owner is not None else "NONE"
        after_id = after_owner.node_id if after_owner is not None else "NONE"
        if before_id != after_id:
            changed_keys.append(key)
    return len(changed_keys), changed_keys[:8]


def _render_distribution_panel(
    *,
    svg: list[str],
    x: int,
    y: int,
    width: int,
    height: int,
    title: str,
    counts: list[tuple[str, int]],
    node_colors: dict[str, str],
    baseline_label: str,
    reference_counts: dict[str, int] | None = None,
    show_delta: bool = False,
) -> None:
    svg.append(f'<rect x="{x}" y="{y}" width="{width}" height="{height}" rx="24" fill="#ffffff" stroke="#cbd5e1"/>')
    svg.append(f'<text x="{x + 26}" y="{y + 34}" font-family="DejaVu Sans, Arial, sans-serif" font-size="20" font-weight="700" fill="#0f172a">{escape(title)}</text>')
    svg.append(f'<text x="{x + 26}" y="{y + 60}" font-family="DejaVu Sans Mono, monospace" font-size="13" fill="#64748b">{escape(baseline_label)}</text>')

    chart_x = x + 34
    chart_y = y + 90
    chart_width = width - 68
    chart_height = height - 170
    baseline = chart_y + chart_height
    max_count = max((count for _, count in counts), default=1)
    bar_gap = 14
    bar_width = max(16, int((chart_width - (len(counts) - 1) * bar_gap) / max(1, len(counts))))

    svg.append(f'<line x1="{chart_x}" y1="{baseline}" x2="{chart_x + chart_width}" y2="{baseline}" stroke="#94a3b8" stroke-width="2"/>')

    for tick in range(0, 6):
        tick_value = max_count * tick / 5
        tick_y = baseline - (chart_height - 40) * tick / 5
        svg.append(f'<line x1="{chart_x}" y1="{tick_y:.2f}" x2="{chart_x + chart_width}" y2="{tick_y:.2f}" stroke="#e2e8f0" stroke-width="1"/>')
        svg.append(
            f'<text x="{chart_x - 6}" y="{tick_y + 4:.2f}" text-anchor="end" font-family="DejaVu Sans Mono, monospace" font-size="11" fill="#64748b">{int(round(tick_value))}</text>'
        )

    for index, (node_id, count) in enumerate(counts):
        color = node_colors[node_id]
        bar_height = 0 if max_count == 0 else (count / max_count) * (chart_height - 40)
        bar_x = chart_x + index * (bar_width + bar_gap)
        bar_y = baseline - bar_height
        svg.append(f'<rect x="{bar_x}" y="{bar_y:.2f}" width="{bar_width}" height="{bar_height:.2f}" rx="8" fill="{color}"/>')
        svg.append(
            f'<text x="{bar_x + bar_width / 2:.2f}" y="{bar_y - 10:.2f}" text-anchor="middle" font-family="DejaVu Sans Mono, monospace" font-size="12" fill="#0f172a">{count}</text>'
        )

        delta_label = ""
        if show_delta and reference_counts is not None:
            delta = count - reference_counts.get(node_id, 0)
            delta_label = f"{delta:+d}"
        if delta_label:
            svg.append(
                f'<text x="{bar_x + bar_width / 2:.2f}" y="{bar_y - 26:.2f}" text-anchor="middle" font-family="DejaVu Sans Mono, monospace" font-size="11" fill="#334155">{delta_label}</text>'
            )

        label_y = baseline + 20
        svg.append(
            f'<text x="{bar_x + bar_width / 2:.2f}" y="{label_y:.2f}" text-anchor="middle" font-family="DejaVu Sans Mono, monospace" font-size="11" fill="#334155">{escape(node_id)}</text>'
        )


def _rebalance_svg(
    before_count: int,
    after_count: int,
    replicas: int,
    sample_keys: list[str],
    action: str,
    stage_label: str,
) -> str:
    before_ring = _build_ring(before_count, replicas)
    after_ring = _build_ring(after_count, replicas)
    before_counts = _ownership_counts(before_ring, sample_keys)
    after_counts = _ownership_counts(after_ring, sample_keys)
    changed_count, changed_examples = _count_changed_keys(before_ring, after_ring, sample_keys)

    width = 1600
    height = 940
    left_x = 48
    top_y = 140
    panel_width = 720
    panel_height = 620
    right_x = 832

    before_map = dict(before_counts)
    after_map = dict(after_counts)
    before_colors = {node.node_id: _html_color(index) for index, node in enumerate(before_ring.get_nodes())}
    after_colors = {node.node_id: _html_color(index) for index, node in enumerate(after_ring.get_nodes())}

    moved_percent = (changed_count / len(sample_keys) * 100) if sample_keys else 0.0
    example_text = ", ".join(changed_examples) if changed_examples else "none"

    svg: list[str] = [
        f'<svg xmlns="http://www.w3.org/2000/svg" width="{width}" height="{height}" viewBox="0 0 {width} {height}">',
        '<rect width="100%" height="100%" fill="#f8fafc"/>',
        f'<text x="56" y="58" font-family="DejaVu Sans, Arial, sans-serif" font-size="28" font-weight="700" fill="#0f172a">Key rebalancing after {escape(action)}</text>',
        f'<text x="56" y="92" font-family="DejaVu Sans, Arial, sans-serif" font-size="18" fill="#334155">{escape(stage_label)} · sampled keys changed: {changed_count} / {len(sample_keys)} ({moved_percent:.1f}%)</text>',
        f'<text x="56" y="118" font-family="DejaVu Sans Mono, monospace" font-size="13" fill="#64748b">Example moved keys: {escape(example_text)}</text>',
        f'<rect x="56" y="132" width="300" height="54" rx="16" fill="#0f172a"/>',
        f'<text x="74" y="166" font-family="DejaVu Sans Mono, monospace" font-size="18" font-weight="700" fill="#f8fafc">Keys changed: {changed_count}</text>',
    ]

    _render_distribution_panel(
        svg=svg,
        x=left_x,
        y=top_y,
        width=panel_width,
        height=panel_height,
        title=f"Before change: {before_count} server(s)",
        counts=before_counts,
        node_colors=before_colors,
        baseline_label="Ownership before rebalancing",
    )

    _render_distribution_panel(
        svg=svg,
        x=right_x,
        y=top_y,
        width=panel_width,
        height=panel_height,
        title=f"After change: {after_count} server(s)",
        counts=after_counts,
        node_colors=after_colors,
        baseline_label="Ownership after rebalancing",
        reference_counts=before_map,
        show_delta=True,
    )

    svg.append(f'<text x="56" y="832" font-family="DejaVu Sans Mono, monospace" font-size="14" fill="#475569">How to read this frame</text>')
    svg.append(f'<text x="56" y="858" font-family="DejaVu Sans Mono, monospace" font-size="13" fill="#64748b">Each bar is the sampled key count owned by a node. The delta above the after-frame bars shows how much each node gained or lost.</text>')
    svg.append("</svg>")
    return "\n".join(svg)


def create_rebalance_gif(
    output_path: Path,
    max_nodes: int = 6,
    replicas: int = 32,
    sample_key_count: int = 4000,
    frame_duration_ms: int = 900,
) -> Path:
    if max_nodes < 2:
        raise ValueError("max_nodes must be at least 2")
    if replicas <= 0:
        raise ValueError("replicas must be positive")
    if sample_key_count <= 0:
        raise ValueError("sample_key_count must be positive")
    if frame_duration_ms <= 0:
        raise ValueError("frame_duration_ms must be positive")

    sample_keys = _sample_keys(sample_key_count)
    output_path = output_path.resolve()
    output_path.parent.mkdir(parents=True, exist_ok=True)

    with tempfile.TemporaryDirectory(prefix="rebalance_gif_") as temp_dir_name:
        temp_dir = Path(temp_dir_name)
        frame_paths: list[Path] = []

        for node_count in range(2, max_nodes + 1):
            frame_path = temp_dir / f"add_{node_count:03d}.svg"
            svg = _rebalance_svg(
                before_count=node_count - 1,
                after_count=node_count,
                replicas=replicas,
                sample_keys=sample_keys,
                action="adding a server",
                stage_label=f"Growing from {node_count - 1} to {node_count} server(s)",
            )
            frame_path.write_text(svg, encoding="utf-8")
            frame_paths.append(frame_path)

        for node_count in range(max_nodes, 1, -1):
            frame_path = temp_dir / f"remove_{node_count:03d}.svg"
            svg = _rebalance_svg(
                before_count=node_count,
                after_count=node_count - 1,
                replicas=replicas,
                sample_keys=sample_keys,
                action="removing a server",
                stage_label=f"Shrinking from {node_count} to {node_count - 1} server(s)",
            )
            frame_path.write_text(svg, encoding="utf-8")
            frame_paths.append(frame_path)

        delay = max(1, frame_duration_ms // 10)
        command = ["convert", "-delay", str(delay), "-loop", "0", *map(str, frame_paths), str(output_path)]
        completed = subprocess.run(command, check=False, capture_output=True, text=True)
        if completed.returncode != 0:
            raise RuntimeError(
                "GIF generation failed using convert. "
                f"stdout: {completed.stdout.strip()} stderr: {completed.stderr.strip()}"
            )

    return output_path


def create_ring_gif(
    output_path: Path,
    max_nodes: int = 6,
    replicas: int = 32,
    sample_key_count: int = 4000,
    frame_duration_ms: int = 900,
) -> Path:
    if max_nodes <= 0:
        raise ValueError("max_nodes must be positive")
    if replicas <= 0:
        raise ValueError("replicas must be positive")
    if sample_key_count <= 0:
        raise ValueError("sample_key_count must be positive")
    if frame_duration_ms <= 0:
        raise ValueError("frame_duration_ms must be positive")

    sample_keys = _sample_keys(sample_key_count)
    output_path = output_path.resolve()
    output_path.parent.mkdir(parents=True, exist_ok=True)

    with tempfile.TemporaryDirectory(prefix="ring_gif_") as temp_dir_name:
        temp_dir = Path(temp_dir_name)
        frame_paths: list[Path] = []
        for node_count in range(1, max_nodes + 1):
            frame_path = temp_dir / f"frame_{node_count:03d}.svg"
            frame_path.write_text(_ring_svg(node_count, replicas, sample_keys), encoding="utf-8")
            frame_paths.append(frame_path)

        delay = max(1, frame_duration_ms // 10)
        command = ["convert", "-delay", str(delay), "-loop", "0", *map(str, frame_paths), str(output_path)]
        completed = subprocess.run(command, check=False, capture_output=True, text=True)
        if completed.returncode != 0:
            raise RuntimeError(
                "GIF generation failed using convert. "
                f"stdout: {completed.stdout.strip()} stderr: {completed.stderr.strip()}"
            )

    return output_path


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


def main() -> None:
    parser = argparse.ArgumentParser(description="Benchmark the local cluster or generate a GIF of ring growth.")
    parser.add_argument("--rounds", type=int, default=1000, help="Number of benchmark rounds to run.")
    parser.add_argument("--ring-gif", type=Path, help="Write a GIF showing the ring after each added server.")
    parser.add_argument("--rebalance-gif", type=Path, help="Write a GIF showing key rebalancing when servers are added and removed.")
    parser.add_argument("--max-nodes", type=int, default=6, help="Maximum number of servers to include in the GIF.")
    parser.add_argument("--replicas", type=int, default=32, help="Virtual nodes per server.")
    parser.add_argument("--sample-key-count", type=int, default=4000, help="Number of keys used to estimate distribution.")
    parser.add_argument("--frame-duration-ms", type=int, default=900, help="Duration of each GIF frame in milliseconds.")
    args = parser.parse_args()

    if args.ring_gif is not None and args.rebalance_gif is not None:
        raise ValueError("Choose only one of --ring-gif or --rebalance-gif")

    if args.ring_gif is not None:
        output_path = create_ring_gif(
            args.ring_gif,
            max_nodes=args.max_nodes,
            replicas=args.replicas,
            sample_key_count=args.sample_key_count,
            frame_duration_ms=args.frame_duration_ms,
        )
        print(f"Wrote GIF to {output_path}")
        return

    if args.rebalance_gif is not None:
        output_path = create_rebalance_gif(
            args.rebalance_gif,
            max_nodes=args.max_nodes,
            replicas=args.replicas,
            sample_key_count=args.sample_key_count,
            frame_duration_ms=args.frame_duration_ms,
        )
        print(f"Wrote GIF to {output_path}")
        return

    print(asyncio.run(benchmark(args.rounds)))


if __name__ == "__main__":
    main()
