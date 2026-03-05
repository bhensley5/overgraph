# Benchmark Plan - Public Readiness

## Status
- Phase: 17 - Benchmarking and Public Performance Readiness
- Last Updated: 2026-03-04
- Owner: Codex
- Scope: Rust core engine + Node.js connector + Python connector

## Purpose
This plan defines what must be benchmarked and how results are collected so OverGraph can make defensible public performance claims during open-source launch.

## Launch Questions We Must Answer
1. How fast are core writes and reads at realistic sizes?
2. What are p50/p95/p99 latencies for traversal and advanced query APIs?
3. How do durability settings (`Immediate` vs `GroupCommit`) change latency/throughput?
4. How do maintenance operations (flush/compact) impact runtime?
5. How fast is startup/recovery as data grows?
6. What overhead do Node.js and Python bindings add vs Rust core?
7. Are results reproducible by external users from documented commands?

## Deliverables
- Shared workload profiles used by Rust, Node.js, and Python benchmark suites.
- Repeatable benchmark commands (local + CI).
- Machine-readable benchmark artifacts (JSON) plus readable summaries (Markdown).
- Regression comparison tooling against stored baseline artifacts.
- Public benchmark summary and methodology documentation.

Current scaffold assets (CP1):
- Workload profiles: `docs/04-quality/workloads/profiles.json`
- Scenario parity contract: `docs/04-quality/workloads/scenario-contract.json`
- Runner wrappers: `scripts/bench/run-rust.sh`, `scripts/bench/run-node.sh`, `scripts/bench/run-python.sh`
- Shared runner implementation: `tools/bench/run_suite.py`
- Node harness: `overgraph-node/__test__/benchmark-v2.mjs`
- Python harness: `tools/bench/python_connector_benchmark.py`
- Cross-language parity validator: `tools/bench/validate_parity.py`
- Baseline comparator: `tools/bench/compare_baseline.py`
- Regression allowlist: `docs/04-quality/regression-allowlist.json`
- CI workflow: `.github/workflows/benchmarks.yml`
- Public methodology docs: `docs/04-quality/Benchmark-Methodology.md`, `docs/04-quality/Benchmark-FAQ.md`
- Launch report packs:
  - `docs/04-quality/reports/2026-03-04-launch-pack/` (initial snapshot)
  - `docs/04-quality/reports/2026-03-04-launch-pack-parity/` (parity-hardened refresh)

## Benchmark Categories

| Category | Why It Matters | Required Metrics |
|---|---|---|
| Core CRUD | Baseline database speed | p50/p95/p99 latency, throughput (ops/s) |
| Traversal | Primary graph query workload | p50/p95/p99 latency vs fan-out/hops |
| Advanced Queries | Public API differentiators | latency, throughput, result cardinality |
| Durability Modes | Honest durability/speed tradeoff | side-by-side latency/throughput (`Immediate`, `GroupCommit`) |
| Maintenance | Operational behavior | flush/compact time, tail latency impact |
| Recovery | Reliability and restart UX | open/replay/recover time by dataset size |
| Cross-Language | Connector overhead transparency | relative latency delta vs Rust |
| Scale | Behavior as data grows | throughput curve, latency curve, storage growth |

## Workload Profiles (Shared Across Languages)

| Profile | Nodes | Edges | Avg Degree | Property Size | Use |
|---|---:|---:|---:|---|---|
| `small` | 10,000 | 50,000 | 5 | light | CI smoke + local dev |
| `medium` | 100,000 | 500,000 | 5 | mixed | default baseline profile |
| `large` | 1,000,000 | 5,000,000 | 5 | mixed/heavy | full benchmark runs |
| `xlarge` | 3,000,000+ | 15,000,000+ | variable | mixed/heavy | optional stress/nightly |

Graph shapes for each profile:
- chain (path behavior)
- hub-and-spoke (high fan-out neighbors)
- power-law-ish (realistic graph hot spots)
- temporal edges (PIT query coverage)

## Scenario Matrix

| Scenario ID | API Focus | Rust | Node.js | Python |
|---|---|---|---|---|
| `S-CRUD-001` | `upsert_node` | required | required | required |
| `S-CRUD-002` | `upsert_edge` | required | required | required |
| `S-CRUD-003` | `get_node` / `get_nodes` | required | required | required |
| `S-TRAV-001` | `neighbors` fan-out 10/100/1000 | required | required | required |
| `S-TRAV-002` | `neighbors_2hop` | required | required | required |
| `S-TRAV-003` | `neighbors_batch` | required | required | required |
| `S-ADV-001` | `top_k_neighbors` | required | required | required |
| `S-ADV-002` | `find_nodes` / `find_nodes_paged` | required | required | required |
| `S-ADV-003` | `find_nodes_by_time_range(_paged)` | required | required | required |
| `S-ADV-004` | `personalized_pagerank` | required | required | required |
| `S-ADV-005` | `export_adjacency` | required | required | required |
| `S-MAIN-001` | `flush` | required | required | required |
| `S-MAIN-002` | `compact` (clean/overlap/dirty) | required | required | required |
| `S-REC-001` | open/reopen/recovery | required | required | required |
| `S-DUR-001` | durability mode matrix | required | required | required |

## Measurement Policy

### Timing
- Record p50/p95/p99/min/max/mean latencies.
- Record throughput for batch and sustained workloads.
- Use microseconds for low-latency ops, milliseconds otherwise.

### Run Structure
- Warmup runs before measurement.
- At least 3 measured repetitions per scenario/profile.
- Report median across repetitions and include raw per-run data.

### Environment Metadata (always captured)
- OS and kernel
- CPU model and core count
- RAM
- Rust/Node/Python versions
- build mode (`release`/`debug`)
- WAL mode and DB options
- git commit SHA

### Durability Disclosure
Every report must include separate result tables for:
- `WalSyncMode::Immediate`
- `WalSyncMode::GroupCommit`

No mixed/combined numbers in public summary tables.

## Artifact Layout

All generated benchmark outputs live under:

`docs/04-quality/reports/<timestamp>-<platform>-<lang>-<profile>/`

Required files per language run:
- `manifest.json` - metadata, suite versions, commit SHA
- `<lang>.json` - language-specific benchmark results
- `summary.md` - human-readable rollup
- `<lang>.stdout.log` - raw standard output capture
- `<lang>.stderr.log` - raw standard error capture
- comparison markdown/json files (optional, when baseline exists)

## Regression Policy

Default thresholds (can be tuned with evidence):
- Fail: >20% p95 regression on `small` or `medium` profile for stable scenarios.
- Warn: 10-20% p95 regression.
- Ignore list: scenario IDs explicitly marked as expected change with reason.

Guardrails must compare same:
- profile
- scenario ID
- durability mode
- runtime/language

## CI Strategy

### PR (smoke)
- Run a reduced benchmark subset on `small`.
- Goal: catch obvious regressions quickly.
- Workflow entrypoint: manual run in `.github/workflows/benchmarks.yml`

### Nightly/manual full
- Run full suite on `medium` (and optionally `large`).
- Upload artifacts for trend history.
- Nightly schedule currently configured at `07:00 UTC`.

### Release candidate
- Run launch benchmark pack and freeze baseline for publication.

## Reporting for Public Launch

Public benchmark summary must include:
- top-line numbers for core write/read/traversal
- advanced query results (2-hop, top-k, time-range, PPR, export)
- maintenance/recovery behavior
- explicit hardware/runtime context
- command list to reproduce
- known limitations and what is not benchmarked yet

## Phase 17 Execution Order

| Backlog Item | Execution Intent |
|---|---|
| P17-001 | Define benchmark scope and launch bar |
| P17-002 | Standardize workload profiles and generation |
| P17-003 | Standardize runners and artifact format |
| P17-004 | Expand Rust coverage |
| P17-005 | Upgrade Node benchmark harness |
| P17-006 | Add Python benchmark harness |
| P17-007 | Automate benchmark CI runs |
| P17-008 | Add baseline compare + thresholds |
| P17-009 | Publish benchmark methodology/docs |
| P17-010 | Produce first launch benchmark report |

## Initial Command Targets

Rust:
- `scripts/bench/run-rust.sh --profile small --warmup 20 --iters 80`

Node.js:
- `scripts/bench/run-node.sh --profile small --warmup 20 --iters 80`

Python:
- `scripts/bench/run-python.sh --profile small --warmup 20 --iters 80`

Baseline compare:
- `python3 tools/bench/compare_baseline.py --baseline <path> --candidate <path> --allowlist docs/04-quality/regression-allowlist.json --warn-threshold-pct 10 --fail-threshold-pct 20`

## Definition of Done for Benchmark Program
- All required scenario IDs implemented across Rust/Node.js/Python.
- CI can run smoke and full suites with artifact upload.
- Regression checks operational with documented thresholds.
- README/public docs contain reproducible benchmark methodology and current baseline summary.
