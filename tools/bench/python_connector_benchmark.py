#!/usr/bin/env python3
"""OverGraph Python connector benchmark harness.

Emits machine-readable JSON with shared scenario IDs and parity metadata.
"""

from __future__ import annotations

import argparse
import json
import math
import shutil
import struct
import sys
import tempfile
import time
from pathlib import Path
from typing import Any, Callable


REPO_ROOT = Path(__file__).resolve().parents[2]
PROFILE_PATH = REPO_ROOT / "docs/04-quality/workloads/profiles.json"
SCENARIO_CONTRACT_PATH = REPO_ROOT / "docs/04-quality/workloads/scenario-contract.json"
PY_BINDING_ROOT = REPO_ROOT / "overgraph-python/python"
if any(PY_BINDING_ROOT.glob("overgraph/overgraph.*")):
    # Prefer in-repo extension module when present (local dev loop).
    sys.path.insert(0, str(PY_BINDING_ROOT))

from overgraph import OverGraph  # noqa: E402


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Python connector benchmark harness")
    parser.add_argument("--profile", default="small", choices=["small", "medium", "large", "xlarge"])
    parser.add_argument("--warmup", type=int, default=20)
    parser.add_argument("--iters", type=int, default=80)
    parser.add_argument("--scenario-set", default="all", choices=["all", "query"])
    return parser.parse_args()


def percentile(sorted_values: list[float], p: float) -> float:
    idx = math.ceil((p / 100.0) * len(sorted_values)) - 1
    idx = max(0, idx)
    return sorted_values[idx]


def stats(samples_us: list[float]) -> dict[str, float]:
    sorted_values = sorted(samples_us)
    mean = sum(samples_us) / len(samples_us)
    return {
        "p50_us": percentile(sorted_values, 50),
        "p95_us": percentile(sorted_values, 95),
        "p99_us": percentile(sorted_values, 99),
        "min_us": sorted_values[0],
        "max_us": sorted_values[-1],
        "mean_us": mean,
    }


def run_bench(
    fn: Callable[[int], Any], warmup: int, iters: int, *, growth: bool = False
) -> dict[str, float]:
    for i in range(warmup):
        fn(i)
    samples_us: list[float] = []
    for i in range(iters):
        t0 = time.perf_counter_ns()
        fn(warmup + i)
        t1 = time.perf_counter_ns()
        samples_us.append((t1 - t0) / 1000.0)
    s = stats(samples_us)
    if growth and len(samples_us) >= 4:
        mid = len(samples_us) // 2
        early_p95 = percentile(sorted(samples_us[:mid]), 95)
        late_p95 = percentile(sorted(samples_us[mid:]), 95)
        s["early_p95_us"] = early_p95
        s["late_p95_us"] = late_p95
        s["drift_ratio"] = late_p95 / early_p95 if early_p95 > 0 else None
    return s


def throughput_ops_per_sec(mean_us: float, ops_per_iter: int) -> float | None:
    if mean_us <= 0:
        return None
    return (ops_per_iter * 1_000_000.0) / mean_us


def scenario(
    scenario_id: str,
    name: str,
    category: str,
    stats_obj: dict[str, float],
    iter_cfg: dict[str, int],
    scenario_params: dict[str, Any],
    comparability: dict[str, Any],
    ops_per_iter: int = 1,
    notes: str | None = None,
) -> dict[str, Any]:
    return {
        "scenario_id": scenario_id,
        "name": name,
        "category": category,
        "warmup_iterations": iter_cfg["warmup"],
        "benchmark_iterations": iter_cfg["iters"],
        "ops_per_iteration": ops_per_iter,
        "throughput_ops_per_sec": throughput_ops_per_sec(stats_obj["mean_us"], ops_per_iter),
        "stats": stats_obj,
        "scenario_params": scenario_params,
        "comparability": comparability,
        "notes": notes,
    }


def load_contracts(profile_name: str) -> tuple[dict[str, Any], dict[str, Any], dict[str, Any]]:
    profile_payload = json.loads(PROFILE_PATH.read_text(encoding="utf-8"))
    profile = profile_payload["profiles"].get(profile_name)
    if profile is None:
        raise ValueError(f"unknown profile: {profile_name}")
    scenario_contract = json.loads(SCENARIO_CONTRACT_PATH.read_text(encoding="utf-8"))
    return profile_payload, profile, scenario_contract


def scenario_iterations(
    warmup: int,
    iters: int,
    scenario_contract: dict[str, Any],
    scenario_id: str,
) -> dict[str, int]:
    policy_map = scenario_contract["scenario_iteration_policy"]
    default_policy = policy_map["default"]
    policy = policy_map.get(scenario_id, default_policy)

    warmup_divisor = max(1, int(policy.get("warmup_divisor", default_policy.get("warmup_divisor", 1))))
    warmup_min = max(1, int(policy.get("warmup_min", default_policy.get("warmup_min", 1))))
    iters_divisor = max(1, int(policy.get("iters_divisor", default_policy.get("iters_divisor", 1))))
    iters_min = max(1, int(policy.get("iters_min", default_policy.get("iters_min", 1))))
    iters_multiplier = max(1, int(policy.get("iters_multiplier", default_policy.get("iters_multiplier", 1))))

    return {
        "warmup": max(warmup_min, warmup // warmup_divisor),
        "iters": max(iters_min, iters // iters_divisor) * iters_multiplier,
    }


def scenario_comparability(scenario_contract: dict[str, Any], scenario_id: str) -> dict[str, Any]:
    entry = scenario_contract["comparability"].get(
        scenario_id,
        {
            "status": "non_comparable",
            "reason": "Missing comparability contract entry",
        },
    )
    return {
        "status": entry.get("status", "non_comparable"),
        "reason": entry.get("reason"),
    }


def effective_config(profile: dict[str, Any], scenario_contract: dict[str, Any]) -> dict[str, Any]:
    cfg = scenario_contract["effective_config"]
    nodes = max(int(cfg["nodes_min"]), int(profile["nodes"]) // int(cfg["nodes_divisor"]))
    edges = max(int(cfg["edges_min"]), int(profile["edges"]) // int(cfg["edges_divisor"]))

    fanout = min(
        int(cfg["fanout_max"]),
        max(int(cfg["fanout_min"]), int(profile["average_degree_target"]) * int(cfg["fanout_degree_multiplier"])),
    )

    batch_nodes = max(int(cfg["batch_nodes_min"]), int(profile["batch_sizes"]["nodes"]))
    batch_edges = max(int(cfg["batch_edges_min"]), int(profile["batch_sizes"]["edges"]))
    two_hop_mid = max(int(cfg["two_hop_mid_min"]), fanout)

    return {
        "nodes": nodes,
        "edges": edges,
        "fanout": fanout,
        "batch_nodes": batch_nodes,
        "batch_edges": batch_edges,
        "two_hop_mid": two_hop_mid,
        "two_hop_leaves_per_mid": int(cfg["two_hop_leaves_per_mid"]),
        "top_k_candidates": max(int(cfg["top_k_candidates_min"]), nodes // int(cfg["top_k_candidates_divisor"])),
        "ppr_nodes": max(int(cfg["ppr_nodes_min"]), nodes // int(cfg["ppr_nodes_divisor"])),
        "get_node_nodes": min(nodes, int(cfg["time_range_nodes_cap"])),
        "time_range_nodes": min(nodes, int(cfg["time_range_nodes_cap"])),
        "export_nodes": min(nodes, int(cfg["export_nodes_cap"])),
        "export_edges": min(edges, int(cfg["export_edges_cap"])),
        "flush_nodes_per_iter": min(batch_nodes, int(cfg["flush_node_batch_cap"])),
        "flush_edges_per_iter_cap": int(cfg["flush_edge_chain_cap"]),
        "ppr_max_iterations": int(cfg["ppr_max_iterations"]),
        "ppr_max_results": int(cfg["ppr_max_results"]),
        "ppr_seed_count": int(cfg["ppr_seed_count"]),
        "ppr_edge_offsets": [int(v) for v in cfg["ppr_edge_offsets"]],
        "top_k_limit": int(cfg["top_k_limit"]),
        "time_range_from_ms": int(cfg["time_range_from_ms"]),
        "time_range_window_ms": int(cfg["time_range_window_ms"]),
        "include_weights_on_export": bool(cfg["include_weights_on_export"]),
        "shortest_path_nodes": max(int(cfg["shortest_path_nodes_min"]), nodes // int(cfg["shortest_path_nodes_divisor"])),
        "shortest_path_edge_offsets": [int(v) for v in cfg["shortest_path_edge_offsets"]],
        "vector_nodes": max(int(cfg["vector_nodes_min"]), int(profile["nodes"]) // int(cfg["vector_nodes_divisor"])),
        "vector_dim": int(cfg["vector_dim"]),
        "vector_nnz": int(cfg["vector_nnz"]),
        "vector_sparse_dims": int(cfg["vector_sparse_dims"]),
        "vector_k": int(cfg["vector_k"]),
    }


def traverse_deep_branching(fanout: int) -> tuple[int, int, int]:
    return (max(8, min(24, fanout // 4)), 4, 4)


def build_depth_two_traversal_graph(db: OverGraph, cfg: dict[str, Any]) -> int:
    two_hop_nodes = [{"type_id": 1, "key": "root"}]
    for i in range(cfg["two_hop_mid"]):
        two_hop_nodes.append({"type_id": 1, "key": f"m-{i}"})
        for j in range(cfg["two_hop_leaves_per_mid"]):
            two_hop_nodes.append({"type_id": 1, "key": f"l-{i}-{j}"})
    two_hop_ids = db.batch_upsert_nodes(two_hop_nodes)
    root = two_hop_ids[0]
    mid_stride = 1 + cfg["two_hop_leaves_per_mid"]
    two_hop_edges = []
    for i in range(cfg["two_hop_mid"]):
        mid_id = two_hop_ids[1 + i * mid_stride]
        two_hop_edges.append({"from_id": root, "to_id": mid_id, "type_id": 1, "weight": 1.0})
        for j in range(cfg["two_hop_leaves_per_mid"]):
            leaf_id = two_hop_ids[1 + i * mid_stride + 1 + j]
            two_hop_edges.append({"from_id": mid_id, "to_id": leaf_id, "type_id": 1, "weight": 1.0})
    db.batch_upsert_edges(two_hop_edges)
    return root


def build_deep_traversal_graph(db: OverGraph, fanout: int) -> tuple[int, tuple[int, int, int]]:
    level1, level2, level3 = traverse_deep_branching(fanout)
    nodes = [{"type_id": 1, "key": "root"}]
    for i in range(level1):
        nodes.append({"type_id": 11, "key": f"lvl1-{i}"})
    for i in range(level1):
        for j in range(level2):
            nodes.append({"type_id": 2 if (i + j) % 2 == 0 else 3, "key": f"lvl2-{i}-{j}"})
    for i in range(level1):
        for j in range(level2):
            for k in range(level3):
                nodes.append({"type_id": 2 if (i + j + k) % 2 == 0 else 3, "key": f"lvl3-{i}-{j}-{k}"})
    ids = db.batch_upsert_nodes(nodes)
    root = ids[0]
    level1_offset = 1
    level2_offset = level1_offset + level1
    level3_offset = level2_offset + level1 * level2
    edges = []
    for i in range(level1):
        lvl1_id = ids[level1_offset + i]
        edges.append({"from_id": root, "to_id": lvl1_id, "type_id": 1, "weight": 1.0})
        for j in range(level2):
            lvl2_idx = i * level2 + j
            lvl2_id = ids[level2_offset + lvl2_idx]
            edges.append({"from_id": lvl1_id, "to_id": lvl2_id, "type_id": 1, "weight": 1.0})
            for k in range(level3):
                lvl3_idx = lvl2_idx * level3 + k
                edges.append({"from_id": lvl2_id, "to_id": ids[level3_offset + lvl3_idx], "type_id": 1, "weight": 1.0})
    db.batch_upsert_edges(edges)
    return root, (level1, level2, level3)


def bench_splitmix64(x: int) -> int:
    x = (x + 0x9E3779B97F4A7C15) & 0xFFFFFFFFFFFFFFFF
    z = x
    z = ((z ^ (z >> 30)) * 0xBF58476D1CE4E5B9) & 0xFFFFFFFFFFFFFFFF
    z = ((z ^ (z >> 27)) * 0x94D049BB133111EB) & 0xFFFFFFFFFFFFFFFF
    return (z ^ (z >> 31)) & 0xFFFFFFFFFFFFFFFF


def bench_dense_vector(dim: int, seed: int) -> list[float]:
    values: list[float] = []
    state = seed & 0xFFFFFFFFFFFFFFFF
    for _ in range(dim):
        state = bench_splitmix64(state)
        values.append((state >> 40) / 16_777_215.0 * 2.0 - 1.0)
    norm = sum(v * v for v in values) ** 0.5
    if norm > 0:
        values = [v / norm for v in values]
    return values


def bench_sparse_vector(dim_count: int, nnz: int, seed: int) -> list[tuple[int, float]]:
    dims: list[int] = []
    state = seed & 0xFFFFFFFFFFFFFFFF
    while len(dims) < nnz:
        state = bench_splitmix64(state)
        d = int(state % dim_count)
        if d not in dims:
            dims.append(d)
    dims.sort()
    return [(d, 1.0 - i * 0.05) for i, d in enumerate(dims)]


def pack_node_batch(nodes: list[dict[str, Any]]) -> bytes:
    """Pack node dicts using the Python connector binary wire format."""
    buf = bytearray(struct.pack("<I", len(nodes)))
    for node in nodes:
        type_id = int(node.get("type_id", 0))
        weight = float(node.get("weight", 1.0))
        key = str(node.get("key", "")).encode("utf-8")
        props = node.get("props", {})
        props_json = json.dumps(props).encode("utf-8") if props else b""
        buf.extend(struct.pack("<IfH", type_id, weight, len(key)))
        buf.extend(key)
        buf.extend(struct.pack("<I", len(props_json)))
        buf.extend(props_json)
    return bytes(buf)


def query_bench_props(i: int) -> dict[str, Any]:
    return {
        "status": "active" if i % 10 == 0 else "inactive",
        "tier": "gold" if i % 20 == 0 else "standard",
        "score": i % 100,
    }


def wait_for_property_index_ready(db: OverGraph, index_id: int) -> None:
    deadline = time.monotonic() + 10.0
    while time.monotonic() < deadline:
        if any(info.index_id == index_id and info.state == "ready" for info in db.list_node_property_indexes()):
            return
        time.sleep(0.01)
    raise RuntimeError(f"timed out waiting for property index {index_id} to become ready")


def query_benchmark_layout(preload_nodes: int) -> dict[str, int]:
    segments = 1 if preload_nodes >= 2 else 0
    segment_nodes = 0 if segments == 0 else max(1, preload_nodes // (segments + 1))
    return {
        "segments": segments,
        "segment_nodes": segment_nodes,
        "memtable_tail_nodes": max(0, preload_nodes - segments * segment_nodes),
    }


def query_bench_nodes(start: int, count: int) -> list[dict[str, Any]]:
    return [
        {
            "type_id": 1,
            "key": f"q-{i}",
            "props": query_bench_props(i),
        }
        for i in range(start, start + count)
    ]


def build_query_benchmark_db(path: Path, preload_nodes: int) -> tuple[OverGraph, dict[str, int]]:
    db = OverGraph.open(str(path))
    status = db.ensure_node_property_index(1, "status", "equality")
    wait_for_property_index_ready(db, status.index_id)
    tier = db.ensure_node_property_index(1, "tier", "equality")
    wait_for_property_index_ready(db, tier.index_id)
    score = db.ensure_node_property_index(1, "score", "range", domain="int")
    wait_for_property_index_ready(db, score.index_id)

    layout = query_benchmark_layout(preload_nodes)
    for segment in range(layout["segments"]):
        start = segment * layout["segment_nodes"]
        db.batch_upsert_nodes(query_bench_nodes(start, layout["segment_nodes"]))
        db.flush()
    tail_start = layout["segments"] * layout["segment_nodes"]
    db.batch_upsert_nodes(query_bench_nodes(tail_start, layout["memtable_tail_nodes"]))
    return db, layout


def push_query_scenarios(
    args: argparse.Namespace,
    scenario_contract: dict[str, Any],
    cfg: dict[str, Any],
    tmp_root: Path,
    scenarios: list[dict[str, Any]],
) -> None:
    preload_nodes = cfg["time_range_nodes"]
    limit = 100

    scenario_id = "S-QUERY-001"
    iter_cfg = scenario_iterations(args.warmup, args.iters, scenario_contract, scenario_id)
    db, layout = build_query_benchmark_db(tmp_root / "query-node-ids-intersected", preload_nodes)
    s = run_bench(
        lambda _i: db.query_node_ids(
            {
                "type_id": 1,
                "where": {
                    "status": {"eq": "active"},
                    "tier": {"eq": "gold"},
                },
                "limit": limit,
            }
        ),
        iter_cfg["warmup"],
        iter_cfg["iters"],
    )
    scenarios.append(
        scenario(
            scenario_id,
            "query_node_ids_intersected_predicates",
            "query",
            s,
            iter_cfg,
            {
                "type_id": 1,
                "preload_nodes": preload_nodes,
                "segments": layout["segments"],
                "segment_nodes": layout["segment_nodes"],
                "memtable_tail_nodes": layout["memtable_tail_nodes"],
                "predicates": ["status_eq_active", "tier_eq_gold"],
                "limit": limit,
            },
            scenario_comparability(scenario_contract, scenario_id),
        )
    )
    db.close()

    scenario_id = "S-QUERY-002"
    iter_cfg = scenario_iterations(args.warmup, args.iters, scenario_contract, scenario_id)
    db, layout = build_query_benchmark_db(tmp_root / "query-nodes-hydrated-intersected", preload_nodes)
    s = run_bench(
        lambda _i: db.query_nodes(
            {
                "type_id": 1,
                "where": {
                    "status": {"eq": "active"},
                    "score": {"gte": 50},
                },
                "limit": limit,
            }
        ),
        iter_cfg["warmup"],
        iter_cfg["iters"],
    )
    scenarios.append(
        scenario(
            scenario_id,
            "query_nodes_intersected_predicates_hydrated",
            "query",
            s,
            iter_cfg,
            {
                "type_id": 1,
                "preload_nodes": preload_nodes,
                "segments": layout["segments"],
                "segment_nodes": layout["segment_nodes"],
                "memtable_tail_nodes": layout["memtable_tail_nodes"],
                "predicates": ["status_eq_active", "score_gte_50"],
                "limit": limit,
            },
            scenario_comparability(scenario_contract, scenario_id),
        )
    )
    db.close()


def main() -> int:
    args = parse_args()
    profile_payload, profile, scenario_contract = load_contracts(args.profile)
    cfg = effective_config(profile, scenario_contract)
    tmp_root = Path(tempfile.mkdtemp(prefix=f"overgraph-py-bench-{args.profile}-"))
    scenarios: list[dict[str, Any]] = []

    try:
        push_query_scenarios(args, scenario_contract, cfg, tmp_root, scenarios)

        if args.scenario_set == "query":
            output = {
                "schema_version": 1,
                "language": "python",
                "harness_stage": "connector-benchmark-v2-parity",
                "profile_name": args.profile,
                "generated_at_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
                "profile_source": str(PROFILE_PATH),
                "scenario_contract_source": str(SCENARIO_CONTRACT_PATH),
                "percentile_method": scenario_contract["percentile_method"],
                "profile_contract": {
                    "determinism": profile_payload["determinism"],
                    "profile": profile,
                    "effective_config": cfg,
                    "scenario_contract_schema_version": scenario_contract["schema_version"],
                },
                "scenarios": scenarios,
            }
            print(json.dumps(output, indent=2))
            return 0

        # S-CRUD-001 (growth)
        scenario_id = "S-CRUD-001"
        iter_cfg = scenario_iterations(args.warmup, args.iters, scenario_contract, scenario_id)
        db = OverGraph.open(str(tmp_root / "crud-upsert-node"))
        s = run_bench(
            lambda i: db.upsert_node(1, f"node-{i}", props={"idx": i}, weight=1.0),
            iter_cfg["warmup"],
            iter_cfg["iters"],
            growth=True,
        )
        scenarios.append(
            scenario(
                scenario_id,
                "upsert_node",
                "crud",
                s,
                iter_cfg,
                {"type_id": 1, "with_props": True, "weight": 1.0},
                scenario_comparability(scenario_contract, scenario_id),
            )
        )
        db.close()

        # S-CRUD-002 (growth)
        scenario_id = "S-CRUD-002"
        iter_cfg = scenario_iterations(args.warmup, args.iters, scenario_contract, scenario_id)
        db = OverGraph.open(str(tmp_root / "crud-upsert-edge"))
        node_ids = db.batch_upsert_nodes(
            [{"type_id": 1, "key": f"e-{i}"} for i in range(iter_cfg["warmup"] + iter_cfg["iters"] + 1)]
        )
        s = run_bench(
            lambda i: db.upsert_edge(node_ids[i], node_ids[i + 1], 1, weight=1.0),
            iter_cfg["warmup"],
            iter_cfg["iters"],
            growth=True,
        )
        scenarios.append(
            scenario(
                scenario_id,
                "upsert_edge",
                "crud",
                s,
                iter_cfg,
                {"edge_type_id": 1, "weight": 1.0},
                scenario_comparability(scenario_contract, scenario_id),
            )
        )
        db.close()

        # S-BATCH-001
        scenario_id = "S-BATCH-001"
        iter_cfg = scenario_iterations(args.warmup, args.iters, scenario_contract, scenario_id)
        db = OverGraph.open(str(tmp_root / "batch-nodes-json"))
        s = run_bench(
            lambda i: db.batch_upsert_nodes(
                [
                    {"type_id": 1, "key": f"bn-{i}-{j}", "props": {"idx": j}, "weight": 1.0}
                    for j in range(cfg["batch_nodes"])
                ]
            ),
            iter_cfg["warmup"],
            iter_cfg["iters"],
        )
        scenarios.append(
            scenario(
                scenario_id,
                "batch_upsert_nodes_json",
                "batch",
                s,
                iter_cfg,
                {"batch_nodes": cfg["batch_nodes"], "type_id": 1, "with_props": True},
                scenario_comparability(scenario_contract, scenario_id),
                cfg["batch_nodes"],
            )
        )
        db.close()

        # S-BATCH-002
        scenario_id = "S-BATCH-002"
        iter_cfg = scenario_iterations(args.warmup, args.iters, scenario_contract, scenario_id)
        db = OverGraph.open(str(tmp_root / "batch-nodes-binary"))

        def run_batch_binary(i: int) -> None:
            nodes = [
                {"type_id": 1, "key": f"bb-{i}-{j}", "props": {"idx": j}, "weight": 1.0}
                for j in range(cfg["batch_nodes"])
            ]
            db.batch_upsert_nodes_binary(pack_node_batch(nodes))

        s = run_bench(run_batch_binary, iter_cfg["warmup"], iter_cfg["iters"])
        scenarios.append(
            scenario(
                scenario_id,
                "batch_upsert_nodes_binary",
                "batch",
                s,
                iter_cfg,
                {"batch_nodes": cfg["batch_nodes"], "encoding": "binary-pack-node-batch"},
                scenario_comparability(scenario_contract, scenario_id),
                cfg["batch_nodes"],
            )
        )
        db.close()

        # S-CRUD-003
        scenario_id = "S-CRUD-003"
        iter_cfg = scenario_iterations(args.warmup, args.iters, scenario_contract, scenario_id)
        db = OverGraph.open(str(tmp_root / "crud-get-node"))
        ids = db.batch_upsert_nodes(
            [{"type_id": 1, "key": f"gn-{i}", "props": {"idx": i}} for i in range(cfg["get_node_nodes"])]
        )
        s = run_bench(lambda i: db.get_node(ids[i % len(ids)]), iter_cfg["warmup"], iter_cfg["iters"])
        scenarios.append(
            scenario(
                scenario_id,
                "get_node",
                "crud",
                s,
                iter_cfg,
                {"preload_nodes": cfg["get_node_nodes"]},
                scenario_comparability(scenario_contract, scenario_id),
            )
        )
        db.close()

        # S-CRUD-004: upsert_node_fixed_key
        scenario_id = "S-CRUD-004"
        iter_cfg = scenario_iterations(args.warmup, args.iters, scenario_contract, scenario_id)
        db = OverGraph.open(str(tmp_root / "crud-upsert-node-fixed"))
        db.upsert_node(1, "fixed-node", props={"idx": 0}, weight=1.0)
        s = run_bench(
            lambda i: db.upsert_node(1, "fixed-node", props={"idx": i}, weight=1.0),
            iter_cfg["warmup"],
            iter_cfg["iters"],
        )
        scenarios.append(
            scenario(
                scenario_id,
                "upsert_node_fixed_key",
                "crud",
                s,
                iter_cfg,
                {"type_id": 1, "with_props": True, "weight": 1.0, "fixed_key": True},
                scenario_comparability(scenario_contract, scenario_id),
            )
        )
        db.close()

        # S-CRUD-005: upsert_edge_fixed_triple
        scenario_id = "S-CRUD-005"
        iter_cfg = scenario_iterations(args.warmup, args.iters, scenario_contract, scenario_id)
        db = OverGraph.open(str(tmp_root / "crud-upsert-edge-fixed"), edge_uniqueness=True)
        node_a = db.upsert_node(1, "fixed-a")
        node_b = db.upsert_node(1, "fixed-b")
        s = run_bench(
            lambda _i: db.upsert_edge(node_a, node_b, 1, weight=1.0),
            iter_cfg["warmup"],
            iter_cfg["iters"],
        )
        scenarios.append(
            scenario(
                scenario_id,
                "upsert_edge_fixed_triple",
                "crud",
                s,
                iter_cfg,
                {"edge_type_id": 1, "weight": 1.0, "edge_uniqueness": True, "fixed_triple": True},
                scenario_comparability(scenario_contract, scenario_id),
            )
        )
        db.close()

        # S-TRAV-001
        scenario_id = "S-TRAV-001"
        iter_cfg = scenario_iterations(args.warmup, args.iters, scenario_contract, scenario_id)
        db = OverGraph.open(str(tmp_root / "trav-neighbors"))
        nb_node_ids = db.batch_upsert_nodes(
            [{"type_id": 1, "key": "hub"}]
            + [{"type_id": 1, "key": f"n-{i}"} for i in range(cfg["fanout"])]
        )
        hub = nb_node_ids[0]
        db.batch_upsert_edges([
            {"from_id": hub, "to_id": nb_node_ids[1 + i], "type_id": 1, "weight": 1.0}
            for i in range(cfg["fanout"])
        ])
        s = run_bench(lambda _i: db.neighbors(hub, direction="outgoing"), iter_cfg["warmup"], iter_cfg["iters"])
        scenarios.append(
            scenario(
                scenario_id,
                "neighbors",
                "traversal",
                s,
                iter_cfg,
                {"fanout": cfg["fanout"], "direction": "outgoing"},
                scenario_comparability(scenario_contract, scenario_id),
            )
        )
        db.close()

        # S-TRAV-002
        scenario_id = "S-TRAV-002"
        iter_cfg = scenario_iterations(args.warmup, args.iters, scenario_contract, scenario_id)
        db = OverGraph.open(str(tmp_root / "trav-neighbors-2hop"))
        root = build_depth_two_traversal_graph(db, cfg)
        s = run_bench(
            lambda _i: db.traverse(root, min_depth=2, max_depth=2, direction="outgoing"),
            iter_cfg["warmup"],
            iter_cfg["iters"],
        )
        scenarios.append(
            scenario(
                scenario_id,
                "traverse_depth_2",
                "traversal",
                s,
                iter_cfg,
                {
                    "direction": "outgoing",
                    "min_depth": 2,
                    "max_depth": 2,
                    "mid_nodes": cfg["two_hop_mid"],
                    "leaves_per_mid": cfg["two_hop_leaves_per_mid"],
                },
                scenario_comparability(scenario_contract, scenario_id),
            )
        )
        db.close()

        # S-TRAV-007
        scenario_id = "S-TRAV-007"
        iter_cfg = scenario_iterations(args.warmup, args.iters, scenario_contract, scenario_id)
        db = OverGraph.open(str(tmp_root / "trav-depth13-memtable"))
        root, branching = build_deep_traversal_graph(db, cfg["fanout"])
        s = run_bench(
            lambda _i: db.traverse(root, min_depth=1, max_depth=3, direction="outgoing"),
            iter_cfg["warmup"],
            iter_cfg["iters"],
        )
        scenarios.append(
            scenario(
                scenario_id,
                "traverse_depth_1_to_3",
                "traversal",
                s,
                iter_cfg,
                {
                    "direction": "outgoing",
                    "layout": "memtable",
                    "min_depth": 1,
                    "max_depth": 3,
                    "node_type_filter": None,
                    "branching": list(branching),
                },
                scenario_comparability(scenario_contract, scenario_id),
            )
        )
        db.close()

        # S-TRAV-008
        scenario_id = "S-TRAV-008"
        iter_cfg = scenario_iterations(args.warmup, args.iters, scenario_contract, scenario_id)
        db = OverGraph.open(str(tmp_root / "trav-depth13-segment"))
        root, branching = build_deep_traversal_graph(db, cfg["fanout"])
        db.flush()
        s = run_bench(
            lambda _i: db.traverse(root, min_depth=1, max_depth=3, direction="outgoing"),
            iter_cfg["warmup"],
            iter_cfg["iters"],
        )
        scenarios.append(
            scenario(
                scenario_id,
                "traverse_depth_1_to_3_segment",
                "traversal",
                s,
                iter_cfg,
                {
                    "direction": "outgoing",
                    "layout": "segment",
                    "min_depth": 1,
                    "max_depth": 3,
                    "node_type_filter": None,
                    "branching": list(branching),
                },
                scenario_comparability(scenario_contract, scenario_id),
            )
        )
        db.close()

        # S-TRAV-009: deeper traverse, memtable, emission-filtered path
        scenario_id = "S-TRAV-009"
        iter_cfg = scenario_iterations(args.warmup, args.iters, scenario_contract, scenario_id)
        db = OverGraph.open(str(tmp_root / "trav-depth13-filtered-memtable"))
        root, branching = build_deep_traversal_graph(db, cfg["fanout"])
        s = run_bench(
            lambda _i: db.traverse(
                root,
                min_depth=1,
                max_depth=3,
                direction="outgoing",
                node_type_filter=[2],
            ),
            iter_cfg["warmup"],
            iter_cfg["iters"],
        )
        scenarios.append(
            scenario(
                scenario_id,
                "traverse_depth_1_to_3_filtered",
                "traversal",
                s,
                iter_cfg,
                {
                    "direction": "outgoing",
                    "layout": "memtable",
                    "min_depth": 1,
                    "max_depth": 3,
                    "node_type_filter": [2],
                    "branching": list(branching),
                },
                scenario_comparability(scenario_contract, scenario_id),
            )
        )
        db.close()

        # S-TRAV-010: deeper traverse, segmented, emission-filtered path
        scenario_id = "S-TRAV-010"
        iter_cfg = scenario_iterations(args.warmup, args.iters, scenario_contract, scenario_id)
        db = OverGraph.open(str(tmp_root / "trav-depth13-filtered-segment"))
        root, branching = build_deep_traversal_graph(db, cfg["fanout"])
        db.flush()
        s = run_bench(
            lambda _i: db.traverse(
                root,
                min_depth=1,
                max_depth=3,
                direction="outgoing",
                node_type_filter=[2],
            ),
            iter_cfg["warmup"],
            iter_cfg["iters"],
        )
        scenarios.append(
            scenario(
                scenario_id,
                "traverse_depth_1_to_3_filtered_segment",
                "traversal",
                s,
                iter_cfg,
                {
                    "direction": "outgoing",
                    "layout": "segment",
                    "min_depth": 1,
                    "max_depth": 3,
                    "node_type_filter": [2],
                    "branching": list(branching),
                },
                scenario_comparability(scenario_contract, scenario_id),
            )
        )
        db.close()

        # S-TRAV-003: degree (scalar)
        scenario_id = "S-TRAV-003"
        iter_cfg = scenario_iterations(args.warmup, args.iters, scenario_contract, scenario_id)
        db = OverGraph.open(str(tmp_root / "trav-degree"))
        deg_node_ids = db.batch_upsert_nodes(
            [{"type_id": 1, "key": "hub"}]
            + [{"type_id": 1, "key": f"d-{i}"} for i in range(cfg["fanout"])]
        )
        hub = deg_node_ids[0]
        db.batch_upsert_edges([
            {"from_id": hub, "to_id": deg_node_ids[1 + i], "type_id": 1, "weight": 1.0}
            for i in range(cfg["fanout"])
        ])
        s = run_bench(lambda _i: db.degree(hub, direction="outgoing"), iter_cfg["warmup"], iter_cfg["iters"])
        scenarios.append(
            scenario(
                scenario_id,
                "degree",
                "traversal",
                s,
                iter_cfg,
                {"fanout": cfg["fanout"], "direction": "outgoing"},
                scenario_comparability(scenario_contract, scenario_id),
            )
        )
        db.close()

        # S-TRAV-004: degrees (batch)
        scenario_id = "S-TRAV-004"
        iter_cfg = scenario_iterations(args.warmup, args.iters, scenario_contract, scenario_id)
        db = OverGraph.open(str(tmp_root / "trav-degrees"))
        # Batch all nodes: hubs first, then fanout nodes for each hub
        all_degree_nodes = [{"type_id": 1, "key": f"hub-{h}"} for h in range(cfg["batch_nodes"])]
        for h in range(cfg["batch_nodes"]):
            for i in range(cfg["fanout"]):
                all_degree_nodes.append({"type_id": 1, "key": f"dt-{h}-{i}"})
        all_degree_ids = db.batch_upsert_nodes(all_degree_nodes)
        hub_ids = all_degree_ids[: cfg["batch_nodes"]]
        # Batch all edges: each hub connects to its fanout nodes
        degree_edges = []
        for h in range(cfg["batch_nodes"]):
            hub_id = hub_ids[h]
            fanout_start = cfg["batch_nodes"] + h * cfg["fanout"]
            for i in range(cfg["fanout"]):
                degree_edges.append({
                    "from_id": hub_id,
                    "to_id": all_degree_ids[fanout_start + i],
                    "type_id": 1,
                    "weight": 1.0,
                })
        db.batch_upsert_edges(degree_edges)
        s = run_bench(
            lambda _i: db.degrees(hub_ids, direction="outgoing"), iter_cfg["warmup"], iter_cfg["iters"]
        )
        scenarios.append(
            scenario(
                scenario_id,
                "degrees",
                "traversal",
                s,
                iter_cfg,
                {"batch_nodes": cfg["batch_nodes"], "fanout": cfg["fanout"], "direction": "outgoing"},
                scenario_comparability(scenario_contract, scenario_id),
                cfg["batch_nodes"],
            )
        )
        db.close()

        # S-TRAV-005: shortest_path (BFS)
        scenario_id = "S-TRAV-005"
        iter_cfg = scenario_iterations(args.warmup, args.iters, scenario_contract, scenario_id)
        db = OverGraph.open(str(tmp_root / "trav-shortest-path"))
        sp_nodes = [
            {"type_id": 1, "key": f"sp-{i}", "weight": 1.0} for i in range(cfg["shortest_path_nodes"])
        ]
        sp_ids = db.batch_upsert_nodes(sp_nodes)
        offsets = cfg["shortest_path_edge_offsets"]
        sp_edges = []
        for i in range(len(sp_ids)):
            from_id = sp_ids[i]
            to1 = sp_ids[(i + offsets[0]) % len(sp_ids)]
            to2 = sp_ids[(i + offsets[1]) % len(sp_ids)]
            sp_edges.append({"from_id": from_id, "to_id": to1, "type_id": 1, "weight": 1.0})
            sp_edges.append({"from_id": from_id, "to_id": to2, "type_id": 1, "weight": 1.0})
        db.batch_upsert_edges(sp_edges)
        sp_from = sp_ids[0]
        sp_to = sp_ids[len(sp_ids) // 2]
        s = run_bench(
            lambda _i: db.shortest_path(sp_from, sp_to, direction="outgoing"),
            iter_cfg["warmup"],
            iter_cfg["iters"],
        )
        scenarios.append(
            scenario(
                scenario_id,
                "shortest_path",
                "traversal",
                s,
                iter_cfg,
                {
                    "shortest_path_nodes": cfg["shortest_path_nodes"],
                    "edge_offsets": cfg["shortest_path_edge_offsets"],
                    "direction": "outgoing",
                    "weight_field": None,
                },
                scenario_comparability(scenario_contract, scenario_id),
            )
        )
        db.close()

        # S-TRAV-006: is_connected
        scenario_id = "S-TRAV-006"
        iter_cfg = scenario_iterations(args.warmup, args.iters, scenario_contract, scenario_id)
        db = OverGraph.open(str(tmp_root / "trav-is-connected"))
        ic_nodes = [
            {"type_id": 1, "key": f"ic-{i}", "weight": 1.0} for i in range(cfg["shortest_path_nodes"])
        ]
        ic_ids = db.batch_upsert_nodes(ic_nodes)
        offsets = cfg["shortest_path_edge_offsets"]
        ic_edges = []
        for i in range(len(ic_ids)):
            from_id = ic_ids[i]
            to1 = ic_ids[(i + offsets[0]) % len(ic_ids)]
            to2 = ic_ids[(i + offsets[1]) % len(ic_ids)]
            ic_edges.append({"from_id": from_id, "to_id": to1, "type_id": 1, "weight": 1.0})
            ic_edges.append({"from_id": from_id, "to_id": to2, "type_id": 1, "weight": 1.0})
        db.batch_upsert_edges(ic_edges)
        ic_from = ic_ids[0]
        ic_to = ic_ids[len(ic_ids) // 2]
        s = run_bench(
            lambda _i: db.is_connected(ic_from, ic_to, direction="outgoing"),
            iter_cfg["warmup"],
            iter_cfg["iters"],
        )
        scenarios.append(
            scenario(
                scenario_id,
                "is_connected",
                "traversal",
                s,
                iter_cfg,
                {
                    "shortest_path_nodes": cfg["shortest_path_nodes"],
                    "edge_offsets": cfg["shortest_path_edge_offsets"],
                    "direction": "outgoing",
                },
                scenario_comparability(scenario_contract, scenario_id),
            )
        )
        db.close()

        # S-ADV-001
        scenario_id = "S-ADV-001"
        iter_cfg = scenario_iterations(args.warmup, args.iters, scenario_contract, scenario_id)
        db = OverGraph.open(str(tmp_root / "adv-top-k"))
        tk_node_ids = db.batch_upsert_nodes(
            [{"type_id": 1, "key": "hub"}]
            + [{"type_id": 1, "key": f"tk-{i}"} for i in range(cfg["top_k_candidates"])]
        )
        hub = tk_node_ids[0]
        tk_candidate_ids = tk_node_ids[1:]
        db.batch_upsert_edges([
            {"from_id": hub, "to_id": tk_candidate_ids[i], "type_id": 1, "weight": 1.0 + ((i % 100) / 10.0)}
            for i in range(cfg["top_k_candidates"])
        ])
        s = run_bench(
            lambda _i: db.top_k_neighbors(hub, cfg["top_k_limit"], direction="outgoing", scoring="weight"),
            iter_cfg["warmup"],
            iter_cfg["iters"],
        )
        scenarios.append(
            scenario(
                scenario_id,
                "top_k_neighbors",
                "advanced",
                s,
                iter_cfg,
                {
                    "direction": "outgoing",
                    "k": cfg["top_k_limit"],
                    "scoring": "weight",
                    "candidate_nodes": cfg["top_k_candidates"],
                },
                scenario_comparability(scenario_contract, scenario_id),
            )
        )
        db.close()

        # S-ADV-003
        scenario_id = "S-ADV-003"
        iter_cfg = scenario_iterations(args.warmup, args.iters, scenario_contract, scenario_id)
        db = OverGraph.open(str(tmp_root / "adv-time-range"))
        db.batch_upsert_nodes([
            {"type_id": 1, "key": f"tr-{i}", "props": {"idx": i}, "weight": 1.0}
            for i in range(cfg["time_range_nodes"])
        ])
        from_ms = cfg["time_range_from_ms"]
        to_ms = int(time.time() * 1000) + cfg["time_range_window_ms"]
        s = run_bench(
            lambda _i: db.find_nodes_by_time_range(1, from_ms, to_ms),
            iter_cfg["warmup"],
            iter_cfg["iters"],
        )
        scenarios.append(
            scenario(
                scenario_id,
                "find_nodes_by_time_range",
                "advanced",
                s,
                iter_cfg,
                {
                    "type_id": 1,
                    "preload_nodes": cfg["time_range_nodes"],
                    "from_ms": cfg["time_range_from_ms"],
                    "to_ms_window": cfg["time_range_window_ms"],
                },
                scenario_comparability(scenario_contract, scenario_id),
            )
        )
        db.close()

        # S-ADV-004
        scenario_id = "S-ADV-004"
        iter_cfg = scenario_iterations(args.warmup, args.iters, scenario_contract, scenario_id)
        db = OverGraph.open(str(tmp_root / "adv-ppr"))
        ids = db.batch_upsert_nodes([{"type_id": 1, "key": f"ppr-{i}"} for i in range(cfg["ppr_nodes"])])
        ppr_edges = []
        for i, from_id in enumerate(ids):
            to1 = ids[(i + cfg["ppr_edge_offsets"][0]) % len(ids)]
            to2 = ids[(i + cfg["ppr_edge_offsets"][1]) % len(ids)]
            ppr_edges.append({"from_id": from_id, "to_id": to1, "type_id": 1, "weight": 1.0})
            ppr_edges.append({"from_id": from_id, "to_id": to2, "type_id": 1, "weight": 0.7})
        db.batch_upsert_edges(ppr_edges)
        seed = ids[0]
        s = run_bench(
            lambda _i: db.personalized_pagerank(
                [seed],
                max_iterations=cfg["ppr_max_iterations"],
                max_results=cfg["ppr_max_results"],
            ),
            iter_cfg["warmup"],
            iter_cfg["iters"],
        )
        scenarios.append(
            scenario(
                scenario_id,
                "personalized_pagerank",
                "advanced",
                s,
                iter_cfg,
                {
                    "ppr_nodes": cfg["ppr_nodes"],
                    "seed_strategy": "first_node_id",
                    "seed_count": cfg["ppr_seed_count"],
                    "edge_offsets": cfg["ppr_edge_offsets"],
                    "max_iterations": cfg["ppr_max_iterations"],
                    "max_results": cfg["ppr_max_results"],
                },
                scenario_comparability(scenario_contract, scenario_id),
            )
        )
        db.close()

        # S-ADV-005
        scenario_id = "S-ADV-005"
        iter_cfg = scenario_iterations(args.warmup, args.iters, scenario_contract, scenario_id)
        db = OverGraph.open(str(tmp_root / "adv-export"))
        ids = db.batch_upsert_nodes(
            [{"type_id": 1, "key": f"ex-{i}"} for i in range(cfg["export_nodes"])]
        )
        export_edges = []
        for i in range(cfg["export_edges"]):
            from_id = ids[i % len(ids)]
            to_id = ids[(i * 13 + 7) % len(ids)]
            if from_id != to_id:
                export_edges.append({"from_id": from_id, "to_id": to_id, "type_id": 1, "weight": 1.0})
        db.batch_upsert_edges(export_edges)
        s = run_bench(
            lambda _i: db.export_adjacency(include_weights=cfg["include_weights_on_export"]),
            iter_cfg["warmup"],
            iter_cfg["iters"],
        )
        scenarios.append(
            scenario(
                scenario_id,
                "export_adjacency",
                "advanced",
                s,
                iter_cfg,
                {
                    "preload_nodes": cfg["export_nodes"],
                    "preload_edges": cfg["export_edges"],
                    "include_weights": cfg["include_weights_on_export"],
                },
                scenario_comparability(scenario_contract, scenario_id),
            )
        )
        db.close()

        # S-MAIN-001
        scenario_id = "S-MAIN-001"
        iter_cfg = scenario_iterations(args.warmup, args.iters, scenario_contract, scenario_id)
        db = OverGraph.open(str(tmp_root / "maint-flush"))

        def run_flush(i: int) -> None:
            nodes = [
                {"type_id": 1, "key": f"fl-{i}-{j}", "props": {"idx": j}, "weight": 1.0}
                for j in range(cfg["flush_nodes_per_iter"])
            ]
            node_ids = db.batch_upsert_nodes(nodes)
            edges = []
            for j in range(min(cfg["flush_edges_per_iter_cap"], len(node_ids) - 1)):
                edges.append(
                    {
                        "from_id": node_ids[j],
                        "to_id": node_ids[j + 1],
                        "type_id": 1,
                        "weight": 1.0,
                    }
                )
            db.batch_upsert_edges(edges)
            db.flush()

        s = run_bench(run_flush, iter_cfg["warmup"], iter_cfg["iters"])
        scenarios.append(
            scenario(
                scenario_id,
                "flush",
                "maintenance",
                s,
                iter_cfg,
                {
                    "nodes_per_iter": cfg["flush_nodes_per_iter"],
                    "edge_chain_cap": cfg["flush_edges_per_iter_cap"],
                },
                scenario_comparability(scenario_contract, scenario_id),
            )
        )
        db.close()

        # S-VEC-001: hybrid_vector_search
        scenario_id = "S-VEC-001"
        iter_cfg = scenario_iterations(args.warmup, args.iters, scenario_contract, scenario_id)
        db = OverGraph.open(
            str(tmp_root / "vec-hybrid"),
            dense_vector_dimension=cfg["vector_dim"],
            dense_vector_metric="cosine",
        )

        vec_nodes = []
        for i in range(cfg["vector_nodes"]):
            seed = 1729 * (i + 1)
            vec_nodes.append(
                {
                    "type_id": 1,
                    "key": f"v-{i}",
                    "dense_vector": bench_dense_vector(cfg["vector_dim"], seed),
                    "sparse_vector": bench_sparse_vector(
                        cfg["vector_sparse_dims"], cfg["vector_nnz"], seed + 0xCAFE
                    ),
                }
            )
        db.batch_upsert_nodes(vec_nodes)
        db.flush()

        query_seed = 0xDEADBEEF
        dense_q = bench_dense_vector(cfg["vector_dim"], query_seed)
        sparse_q = bench_sparse_vector(
            cfg["vector_sparse_dims"], cfg["vector_nnz"], query_seed + 0xCAFE
        )

        s = run_bench(
            lambda _i: db.vector_search(
                "hybrid", cfg["vector_k"], dense_query=dense_q, sparse_query=sparse_q
            ),
            iter_cfg["warmup"],
            iter_cfg["iters"],
        )
        scenarios.append(
            scenario(
                scenario_id,
                "hybrid_vector_search",
                "vector",
                s,
                iter_cfg,
                {
                    "vector_nodes": cfg["vector_nodes"],
                    "vector_dim": cfg["vector_dim"],
                    "vector_nnz": cfg["vector_nnz"],
                    "vector_sparse_dims": cfg["vector_sparse_dims"],
                    "vector_k": cfg["vector_k"],
                    "mode": "hybrid",
                    "fusion_mode": "weighted_rank",
                },
                scenario_comparability(scenario_contract, scenario_id),
            )
        )
        db.close()

        output = {
            "schema_version": 1,
            "language": "python",
            "harness_stage": "connector-benchmark-v2-parity",
            "profile_name": args.profile,
            "generated_at_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
            "profile_source": str(PROFILE_PATH),
            "scenario_contract_source": str(SCENARIO_CONTRACT_PATH),
            "percentile_method": scenario_contract["percentile_method"],
            "profile_contract": {
                "determinism": profile_payload["determinism"],
                "profile": profile,
                "effective_config": cfg,
                "scenario_contract_schema_version": scenario_contract["schema_version"],
            },
            "scenarios": scenarios,
        }
        print(json.dumps(output, indent=2))
        return 0
    finally:
        shutil.rmtree(tmp_root, ignore_errors=True)


if __name__ == "__main__":
    raise SystemExit(main())
