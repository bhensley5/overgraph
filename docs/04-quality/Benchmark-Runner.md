# Benchmark Runner Contract

This document defines the Phase 17 benchmark runner and regression-check contract.

## Commands
- Rust: `scripts/bench/run-rust.sh --profile small --warmup 20 --iters 80`
- Node.js: `scripts/bench/run-node.sh --profile small --warmup 20 --iters 80`
- Python: `scripts/bench/run-python.sh --profile small --warmup 20 --iters 80`

Common optional flags:
- `--output-root <path>` (default: `docs/04-quality/reports`)
- `--run-id <id>`
- `--dry-run`
- `--timeout-seconds <n>`
- `--warmup <n>`
- `--iters <n>`
- `--scenario-set <all|query>` (default: `all`; `query` runs only Phase 23 query scenarios)
- `--allow-legacy-rust-criterion-parse` (opt-in fallback for old Criterion text output)

## Output Layout

Each command creates one run directory:

`docs/04-quality/reports/<timestamp>-<platform>-<lang>-<profile>/`

Files:
- `manifest.json`: machine and runtime metadata, profile details, run identity
- `<lang>.json`: language run result metadata, status, command, duration
- `<lang>.stdout.log`: raw command stdout
- `<lang>.stderr.log`: raw command stderr
- `summary.md`: human-readable summary

## Current Harness Stage
- Rust: `core-benchmark-v1-parity` (runs `src/bin/benchmark_harness.rs` with shared scenario contract)
- Node.js: `connector-benchmark-v3-parity` (shared-profile + shared scenario-contract harness)
- Python: `connector-benchmark-v2-parity` (shared-profile + shared scenario-contract harness)
- Phase 20b adds Criterion `write_txn/*` microbenches for explicit 4/16/64-intent commits, a 16-intent implicit batch comparator, and a same-key conflict-heavy workload. Connector benchmark harnesses should mirror this with ordered `stage(operations)` arrays for Node.js and Python.
- Phase 23 adds `query_ops` Criterion microbenchmarks and shared query-only parity scenarios.
  Phase 26 extends the same query set with direct edge query parity, and Phase 32 extends it with graph-row/GQL substrate parity:
  - `S-QUERY-001` / `query_node_ids_intersected_predicates`
  - `S-QUERY-002` / `query_nodes_intersected_predicates_hydrated`
  - `S-QUERY-003` / `query_edge_ids_endpoint_metadata`
  - `S-QUERY-004` / `query_edges_endpoint_property_hydrated`
  - `S-QUERY-005` / `query_edge_ids_property_indexed_equality`
  - `S-QUERY-006` / `query_edge_ids_property_indexed_range`
  - `S-QUERY-007` / `query_graph_rows_optional_edge_traversal`
  - `S-GQL-006` / `execute_gql_optional_edge_traversal_graph_rows`
  Connector-only GQL baselines `S-GQL-001` through `S-GQL-005` are emitted by Node.js/Python for local connector tracking and are marked non-comparable in the shared contract.
  Run only the cross-language query matrix with:
  `scripts/bench/run-rust.sh --scenario-set query --profile small --warmup 20 --iters 80`
  plus the matching Node.js and Python wrapper commands.
  For focused final-proof runs, pass repeated scenario filters through the harnesses or runner, for example:
  `python3 tools/bench/run_suite.py --lang rust --scenario-set query --scenario-id S-QUERY-007 --scenario-id S-GQL-006 --profile small --warmup 1 --iters 1 --output-root /tmp/overgraph-cp32-bench`

Shared parity contract:
- `docs/04-quality/workloads/scenario-contract.json`

Cross-language parity validator:
- `python3 tools/bench/validate_parity.py --rust <rust.json> --node <node.json> --python <python.json>`

Rust Criterion graph-row/GQL microbenches live in `benches/query_ops.rs`:
- `graph_row_query/graph_row_fixed_connected_query`
- `graph_row_query/graph_row_optional_edge_traversal_query`
- `execute_gql/gql_graph_row_fixed_connected_query`
- `execute_gql/gql_graph_row_optional_edge_traversal_query`

Phase 33 adds focused Criterion smoke/proof benches for the unified GQL mutation path. These are
compile-and-execute checks for the final docs checkpoint, not release-grade published performance
claims:
- `execute_gql/gql_mutation_create_smoke`
- `execute_gql/gql_mutation_match_set_smoke`
- `execute_gql/gql_mutation_detach_delete_smoke`
- `execute_gql/gql_mutation_return_smoke`

## Baseline Comparison Command

```bash
python3 tools/bench/compare_baseline.py \
  --baseline <baseline-json-or-run-dir> \
  --candidate <candidate-json-or-run-dir> \
  --allowlist docs/04-quality/regression-allowlist.json \
  --warn-threshold-pct 10 \
  --fail-threshold-pct 20 \
  --report-md /tmp/compare.md \
  --report-json /tmp/compare.json
```

Guardrails enforced by `compare_baseline.py`:
- baseline/candidate language must match
- baseline/candidate profile must match
- baseline/candidate harness stage must match
- at least one shared scenario ID must exist

## Notes
- CI automation is defined in `.github/workflows/benchmarks.yml` (manual + nightly).
- Runtime code is frozen for this offline phase window; runner and benchmark assets are allowed.
