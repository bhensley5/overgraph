# Benchmark FAQ

## Why benchmark Node.js and Python if the engine is Rust?
The Rust engine is the core, but users call it through Rust, Node.js, and Python APIs. Public performance claims need all three paths so connector overhead is transparent.

## Are Rust/Node/Python numbers directly comparable today?
Yes, for scenarios marked `comparable` in `docs/04-quality/workloads/scenario-contract.json`.
The benchmark workflow now enforces parity via `tools/bench/validate_parity.py` (effective config, scenario params, and iteration semantics).
Connector-overhead claims should only use scenarios that pass this comparability gate.

## What metrics should we quote publicly?
At minimum:
- p50 and p95 latency for CRUD, traversal, and advanced queries.
- throughput for batch-heavy operations.
- maintenance and recovery timings.
- hardware/runtime context and durability mode.

## What should fail CI?
Default policy in `tools/bench/compare_baseline.py`:
- Warn: >10% p95 regression.
- Fail: >20% p95 regression.
- Explicit allowlist can exempt known expected regressions.

## Why is flush often noisy?
`flush` is highly sensitive to disk behavior and shared-runner contention. It is included for operational visibility, but may need allowlist bounds to avoid noisy failures.

## How should we present durability mode?
Always separate results by mode:
- `immediate`
- `group-commit` / `group_commit`

Never merge or average those modes in public summary tables.

## Where are benchmark artifacts stored?
Generated runs:
- `docs/04-quality/reports/<run-id>/...`

Versioned launch snapshot:
- `docs/04-quality/reports/2026-03-04-launch-pack-parity/`

Baselines:
- `docs/04-quality/reports/baselines/...`
