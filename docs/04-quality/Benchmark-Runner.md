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

Shared parity contract:
- `docs/04-quality/workloads/scenario-contract.json`

Cross-language parity validator:
- `python3 tools/bench/validate_parity.py --rust <rust.json> --node <node.json> --python <python.json>`

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
