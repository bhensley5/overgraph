# Phase 20b Transaction Benchmark Proof

Captured on 2026-04-24 as the CP3 transaction proof gate.

## Commands

- Rust: `cargo bench --bench core_ops write_txn`
- Rust smoke: `cargo bench --bench core_ops write_txn -- --test`
- Node.js: `node tools/bench/txn_node_benchmark.mjs --warmup 5 --iters 20`
- Python: `PYTHONPATH=/tmp/overgraph-pytest4 python3 tools/bench/txn_python_benchmark.py --warmup 5 --iters 20`

## Rust Criterion

| Scenario | Estimate |
|---|---:|
| `write_txn/explicit_4_intents` | 12.835 us |
| `write_txn/explicit_16_intents` | 38.091 us |
| `write_txn/explicit_64_intents` | 129.08 us |
| `write_txn/explicit_existing_mixed_16_intents` | 60.012 us |
| `write_txn/implicit_graph_patch_equivalent_16_intents` | 46.011 us |
| `write_txn/explicit_existing_edges_16_intents` | 58.226 us |
| `write_txn/implicit_graph_patch_edges_16_intents` | 44.904 us |
| `write_txn/conflict_update_same_key` | 19.611 us |

The transaction-only create/connect shape (`explicit_16_intents`) lands at 38.145 us, but
`graph_patch` cannot express local-ref create-and-connect in one call. The existing-target
comparators provide the closest one-shot substrate comparison and miss the Phase 20b
envelope: mixed node+edge explicit txn is 30% slower than `graph_patch`, and edge-only
explicit txn is 30% slower than `graph_patch`.

Accepted tradeoff: on 2026-04-24, the user explicitly accepted the measured gap because
explicit transactions provide ordered staging, local references, read-own-writes, rollback,
and conflict detection that the one-shot batch path does not provide.

## Connector Focus

| Scenario | Node p50 | Node p95 | Python p50 | Python p95 |
|---|---:|---:|---:|---:|
| `S-TXN-001-4` | 90.334 us | 129.875 us | 71.708 us | 104.291 us |
| `S-TXN-001-16` | 304.958 us | 377.292 us | 274.750 us | 319.333 us |
| `S-TXN-001-64` | 1200.708 us | 3739.166 us | 1050.958 us | 1218.458 us |
| `S-TXN-002` | 68.000 us | 90.083 us | 85.084 us | 107.125 us |
| `S-TXN-003` | 111.666 us | 121.333 us | 90.208 us | 106.250 us |

`S-TXN-003` isolates connector ordered `stage(operations)` parsing plus overlay staging and
rollback for a 16-operation payload. It validates the CP3 connector bulk-staging path exists
and does not reparse at commit.
