#!/usr/bin/env python3
"""Focused Phase 20b connector transaction benchmark."""

from __future__ import annotations

import argparse
import json
import statistics
import tempfile
import time

from overgraph import OverGraph, OverGraphError


def percentile(sorted_samples: list[float], p: int) -> float:
    return sorted_samples[max(0, -(-p * len(sorted_samples) // 100) - 1)]


def stats(samples: list[float]) -> dict[str, float]:
    sorted_samples = sorted(samples)
    return {
        "p50_us": percentile(sorted_samples, 50),
        "p95_us": percentile(sorted_samples, 95),
        "min_us": sorted_samples[0],
        "max_us": sorted_samples[-1],
        "mean_us": statistics.fmean(samples),
    }


def run_bench(fn, warmup: int, iters: int) -> dict[str, float]:
    for i in range(warmup):
        fn(i)
    samples = []
    for i in range(iters):
        start = time.perf_counter()
        fn(warmup + i)
        samples.append((time.perf_counter() - start) * 1_000_000.0)
    return stats(samples)


def txn_ops(batch: int, node_count: int, edge_count: int) -> list[dict]:
    ops = [
        {"op": "upsert_node", "alias": f"n{i}", "type_id": 1, "key": f"txn:{batch}:n:{i}"}
        for i in range(node_count)
    ]
    ops.extend(
        {
            "op": "upsert_edge",
            "alias": f"e{i}",
            "from": {"local": f"n{i % node_count}"},
            "to": {"local": f"n{(i + 1) % node_count}"},
            "type_id": 7,
        }
        for i in range(edge_count)
    )
    return ops


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--warmup", type=int, default=5)
    parser.add_argument("--iters", type=int, default=20)
    args = parser.parse_args()

    scenarios = []
    with tempfile.TemporaryDirectory(prefix="overgraph-txn-python-bench-") as tmp:
        db = OverGraph.open(f"{tmp}/db")
        try:
            for name, nodes, edges in [
                ("S-TXN-001-4", 2, 2),
                ("S-TXN-001-16", 8, 8),
                ("S-TXN-001-64", 32, 32),
            ]:
                scenarios.append(
                    {
                        "scenario_id": name,
                        "stats": run_bench(
                            lambda i, nodes=nodes, edges=edges: commit_ops(db, i, nodes, edges),
                            args.warmup,
                            args.iters,
                        ),
                    }
                )

            scenarios.append(
                {
                    "scenario_id": "S-TXN-002",
                    "stats": run_bench(lambda i: conflict(db, i), args.warmup, args.iters),
                }
            )
            scenarios.append(
                {
                    "scenario_id": "S-TXN-003",
                    "stats": run_bench(lambda i: stage_only(db, i), args.warmup, args.iters),
                }
            )
        finally:
            db.close()

    print(json.dumps({"language": "python", "warmup": args.warmup, "iters": args.iters, "scenarios": scenarios}, indent=2))


def commit_ops(db: OverGraph, i: int, nodes: int, edges: int) -> None:
    txn = db.begin_write_txn()
    txn.stage(txn_ops(i, nodes, edges))
    txn.commit()


def conflict(db: OverGraph, i: int) -> None:
    db.upsert_node(9, "conflict", props={"i": i})
    txn = db.begin_write_txn()
    txn.upsert_node(9, "conflict", props={"txn": i})
    db.upsert_node(9, "conflict", props={"other": i})
    try:
        txn.commit()
        raise AssertionError("expected conflict")
    except OverGraphError as exc:
        if "transaction conflict" not in str(exc):
            raise


def stage_only(db: OverGraph, i: int) -> None:
    txn = db.begin_write_txn()
    txn.stage(txn_ops(i, 8, 8))
    txn.rollback()


if __name__ == "__main__":
    main()
