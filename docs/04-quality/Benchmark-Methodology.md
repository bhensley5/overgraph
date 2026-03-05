# Benchmark Methodology

This document is the reproducibility guide for OverGraph benchmark results.

## Scope
- Rust core benchmarks: `src/bin/benchmark_harness.rs`.
- Node.js connector benchmarks: `overgraph-node/__test__/benchmark-v2.mjs`.
- Python connector benchmarks: `tools/bench/python_connector_benchmark.py`.
- Shared profile contract: `docs/04-quality/workloads/profiles.json`.
- Shared scenario contract: `docs/04-quality/workloads/scenario-contract.json`.

## Prerequisites
- Rust toolchain installed (`cargo`, `rustc`).
- Node.js available.
- Python 3.9+ available.

Build connector dependencies before running Node/Python benchmarks:

```bash
npm ci --prefix overgraph-node
npm --prefix overgraph-node run build
python3 -m pip install --upgrade pip maturin
python3 -m pip install -e overgraph-python
```

## Run Benchmarks Locally

Run one language:

```bash
scripts/bench/run-rust.sh --profile small --warmup 20 --iters 80
scripts/bench/run-node.sh --profile small --warmup 20 --iters 80
scripts/bench/run-python.sh --profile small --warmup 20 --iters 80
```

Useful flags:
- `--profile {small|medium|large|xlarge}`
- `--warmup <n>`
- `--iters <n>`
- `--output-root <path>`
- `--run-id <id>`

Artifacts are written under `docs/04-quality/reports/<run-id>/`.

## Baseline Regression Checks

Compare a candidate run to a baseline:

```bash
python3 tools/bench/compare_baseline.py \
  --baseline docs/04-quality/reports/baselines/examples/small/node.json \
  --candidate <candidate-run-dir>/node.json \
  --allowlist docs/04-quality/regression-allowlist.json \
  --warn-threshold-pct 10 \
  --fail-threshold-pct 20 \
  --report-md /tmp/node-compare.md \
  --report-json /tmp/node-compare.json
```

Policy defaults:
- Warning: regression >10% p95 latency.
- Fail: regression >20% p95 latency.
- Allowlist entries can exempt expected regressions with a reason.

## CI Workflow

Workflow file: `.github/workflows/benchmarks.yml`

Triggers:
- Manual: `workflow_dispatch` with profile selection.
- Nightly: scheduled run at `07:00 UTC`.

Workflow behavior:
- Builds Node and Python connectors.
- Runs Rust/Node/Python benchmark wrappers.
- Runs cross-language parity validation (`tools/bench/validate_parity.py`).
- Uploads benchmark artifacts.
- Runs baseline comparisons when baseline files are present.

## Durability Disclosure Rules

Every public benchmark report must clearly state durability mode.

Accepted labels:
- Rust/Node: `immediate` or `group-commit`
- Python: `immediate` or `group_commit`

Current launch-pack defaults:
- Rust core benchmark default mode: `group-commit`.
- Node benchmark default mode: `group-commit`.
- Python benchmark default mode: `group_commit`.

Rust also includes explicit immediate vs group-commit microbench scenarios in `group_commit/*` entries.
