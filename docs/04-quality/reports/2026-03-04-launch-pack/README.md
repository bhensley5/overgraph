# Launch Benchmark Pack (2026-03-04)

> Superseded by the parity-hardened pack: `docs/04-quality/reports/2026-03-04-launch-pack-parity/README.md`

## Scope
- Profile: `small`
- Durability default: `group-commit` (Rust/Node) and `group_commit` (Python)
- Languages: Rust core, Node connector, Python connector

## Environment Snapshot
- OS: macOS 26.3 (arm64)
- CPU (logical): 14
- Memory: 36 GiB
- Rust: `rustc 1.92.0`
- Node: `v25.6.0`
- Python: `3.11.5`

## Top-line Results (p95 latency)

### Rust (`criterion-quick-core-advanced-maintenance-recovery`)
| Scenario | p95 (us) |
|---|---:|
| upsert_node | 1.133 |
| get_node | 0.073 |
| neighbors_2hop_100x10 | 120.820 |
| top_k_neighbors_weight_k20_1000 | 16.917 |
| find_nodes_by_time_range_10000 | 485.790 |
| personalized_pagerank_2000_nodes | 904.350 |
| export_adjacency_5000n_20000e | 1390.900 |
| flush_100_nodes_20_edges | 107520.000 |

### Node (`connector-benchmark-v2`)
| Scenario | p95 (us) | Throughput (ops/s) |
|---|---:|---:|
| upsert_node | 3.917 | 486564.73 |
| get_node | 2.209 | 474543.70 |
| neighbors_2hop | 396.333 | 3175.86 |
| top_k_neighbors | 23.541 | 46760.85 |
| find_nodes_by_time_range | 181.708 | 6098.84 |
| personalized_pagerank | 319.875 | 3350.94 |
| export_adjacency | 1090.833 | 958.96 |
| flush | 274416.625 | 4.79 |

### Python (`connector-benchmark-v1`)
| Scenario | p95 (us) | Throughput (ops/s) |
|---|---:|---:|
| upsert_node | 2.292 | 691897.88 |
| get_node | 0.709 | 1717106.68 |
| neighbors_2hop | 254.792 | 4718.88 |
| top_k_neighbors | 14.084 | 72183.21 |
| find_nodes_by_time_range | 242.125 | 4430.00 |
| personalized_pagerank | 380.834 | 2872.84 |
| export_adjacency | 561.250 | 1897.60 |
| flush | 256041.166 | 4.80 |

## Durability Disclosure
This pack includes Rust durability microbench scenarios:
- `group_commit/upsert_node_immediate`: p95 `4508.9 us`
- `group_commit/upsert_node_group_commit`: p95 `156.59 us`

Observed delta in this environment: immediate mode is about `28.79x` slower on that microbench scenario.

## Known Limits
- Rust quick-mode parsing uses Criterion `[low median high]` intervals; p95/p99 are conservative high-bound proxies.
- Cross-language harness internals are aligned by scenario labels but not yet perfectly workload-identical for strict connector-overhead claims.
- Results are single-machine snapshots and should not be generalized without CI trend data.
- Baseline regression checks are environment-specific.

## Artifacts
- Structured summary: `results-small-group-commit.json`
- Baseline examples:
  - `docs/04-quality/reports/baselines/examples/small/rust.json`
  - `docs/04-quality/reports/baselines/examples/small/node.json`
  - `docs/04-quality/reports/baselines/examples/small/python.json`

To regenerate raw run artifacts locally, run:

```bash
scripts/bench/run-rust.sh --profile small --warmup 20 --iters 80
scripts/bench/run-node.sh --profile small --warmup 20 --iters 80
scripts/bench/run-python.sh --profile small --warmup 20 --iters 80
```
