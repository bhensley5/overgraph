# Launch Benchmark Pack (2026-03-04)

Historical naming note: this pack predates the API rename. `traverse_depth_2`
was previously labeled `neighbors_2hop` (`traverse(start, min_depth=2, max_depth=2, ...)`).

## Scope
- Profiles: `small` (10K nodes / 50K edges), `medium` (100K / 500K), `large` (1M / 5M)
- Languages: Rust core, Node connector, Python connector
- Parity status: `pass` (see `parity-report.md`)
- Comparable scenarios per profile: `13`
- Excluded scenario: `S-BATCH-002` (connector-specific binary batch path)
- Growth scenarios (S-CRUD-001, S-CRUD-002) include early/late p95 drift metadata
- Fixed-state scenarios (S-CRUD-004, S-CRUD-005) measure clean write latency without memtable growth
- Durability mode: `group-commit` for all languages
- Both Node.js and Python record types use eager primitives + lazy `props`/container getters

---

## Small Profile (10K nodes / 50K edges)

### Rust (`core-benchmark-v1-parity`)
| Scenario ID | Scenario | p95 (us) | Throughput (ops/s) |
|---|---|---:|---:|
| S-ADV-001 | top_k_neighbors | 17.541 | 81,239 |
| S-ADV-003 | find_nodes_by_time_range | 134.667 | 8,298 |
| S-ADV-004 | personalized_pagerank | 254.500 | 4,256 |
| S-ADV-005 | export_adjacency | 517.667 | 2,122 |
| S-BATCH-001 | batch_upsert_nodes_json | 236.000 | 624,605 |
| S-CRUD-001 | upsert_node | 2.209 | 807,436 |
| S-CRUD-002 | upsert_edge | 2.583 | 668,717 |
| S-CRUD-003 | get_node | 0.209 | 5,502,063 |
| S-CRUD-004 | upsert_node_fixed_key | 1.292 | 961,019 |
| S-CRUD-005 | upsert_edge_fixed_triple | 0.625 | 2,310,670 |
| S-MAIN-001 | flush | 183,861.625 | 9 |
| S-TRAV-001 | neighbors | 2.125 | 541,287 |
| S-TRAV-002 | traverse_depth_2 | 175.958 | 6,885 |

### Node (`connector-benchmark-v3-parity`)
| Scenario ID | Scenario | p95 (us) | Throughput (ops/s) |
|---|---|---:|---:|
| S-ADV-001 | top_k_neighbors | 18.542 | 62,277 |
| S-ADV-003 | find_nodes_by_time_range | 141.416 | 7,427 |
| S-ADV-004 | personalized_pagerank | 250.042 | 4,333 |
| S-ADV-005 | export_adjacency | 560.375 | 1,946 |
| S-BATCH-001 | batch_upsert_nodes_json | 366.333 | 423,387 |
| S-CRUD-001 | upsert_node | 3.375 | 477,603 |
| S-CRUD-002 | upsert_edge | 2.584 | 628,082 |
| S-CRUD-003 | get_node | 1.000 | 1,345,510 |
| S-CRUD-004 | upsert_node_fixed_key | 1.792 | 565,048 |
| S-CRUD-005 | upsert_edge_fixed_triple | 0.709 | 1,549,607 |
| S-MAIN-001 | flush | 164,769.458 | 9 |
| S-TRAV-001 | neighbors | 5.667 | 324,488 |
| S-TRAV-002 | traverse_depth_2 | 153.458 | 7,121 |

### Python (`connector-benchmark-v2-parity`)
| Scenario ID | Scenario | p95 (us) | Throughput (ops/s) |
|---|---|---:|---:|
| S-ADV-001 | top_k_neighbors | 11.584 | 89,153 |
| S-ADV-003 | find_nodes_by_time_range | 187.792 | 5,611 |
| S-ADV-004 | personalized_pagerank | 301.584 | 3,486 |
| S-ADV-005 | export_adjacency | 522.292 | 2,049 |
| S-BATCH-001 | batch_upsert_nodes_json | 180.291 | 646,152 |
| S-CRUD-001 | upsert_node | 2.458 | 641,735 |
| S-CRUD-002 | upsert_edge | 2.667 | 631,149 |
| S-CRUD-003 | get_node | 0.458 | 2,770,851 |
| S-CRUD-004 | upsert_node_fixed_key | 1.500 | 713,203 |
| S-CRUD-005 | upsert_edge_fixed_triple | 0.667 | 1,477,023 |
| S-MAIN-001 | flush | 162,101.791 | 9 |
| S-TRAV-001 | neighbors | 6.042 | 204,515 |
| S-TRAV-002 | traverse_depth_2 | 167.917 | 6,767 |

### Connector Overhead (small)
| Scenario | Rust p95 (us) | Node p95 (us) | Node/Rust | Python p95 (us) | Py/Rust |
|---|---:|---:|---:|---:|---:|
| get_node | 0.209 | 1.000 | 4.78x | 0.458 | 2.19x |
| upsert_node_fixed_key | 1.292 | 1.792 | 1.39x | 1.500 | 1.16x |
| upsert_edge_fixed_triple | 0.625 | 0.709 | 1.13x | 0.667 | 1.07x |
| upsert_node | 2.209 | 3.375 | 1.53x | 2.458 | 1.11x |
| upsert_edge | 2.583 | 2.584 | 1.00x | 2.667 | 1.03x |
| neighbors | 2.125 | 5.667 | 2.67x | 6.042 | 2.84x |
| traverse_depth_2 | 175.958 | 153.458 | 0.87x | 167.917 | 0.95x |
| top_k_neighbors | 17.541 | 18.542 | 1.06x | 11.584 | 0.66x |
| find_nodes_by_time_range | 134.667 | 141.416 | 1.05x | 187.792 | 1.39x |
| personalized_pagerank | 254.500 | 250.042 | 0.98x | 301.584 | 1.19x |
| export_adjacency | 517.667 | 560.375 | 1.08x | 522.292 | 1.01x |
| batch_upsert_nodes_json | 236.000 | 366.333 | 1.55x | 180.291 | 0.76x |
| flush | 183,861.625 | 164,769.458 | 0.90x | 162,101.791 | 0.88x |

---

## Medium Profile (100K nodes / 500K edges)

### Rust (`core-benchmark-v1-parity`)
| Scenario ID | Scenario | p95 (us) | Throughput (ops/s) |
|---|---|---:|---:|
| S-ADV-001 | top_k_neighbors | 35.208 | 37,527 |
| S-ADV-003 | find_nodes_by_time_range | 257.250 | 4,284 |
| S-ADV-004 | personalized_pagerank | 420.333 | 2,507 |
| S-ADV-005 | export_adjacency | 997.458 | 1,103 |
| S-BATCH-001 | batch_upsert_nodes_json | 2,177.083 | 527,080 |
| S-CRUD-001 | upsert_node | 2.250 | 839,895 |
| S-CRUD-002 | upsert_edge | 2.791 | 628,689 |
| S-CRUD-003 | get_node | 0.208 | 7,328,021 |
| S-CRUD-004 | upsert_node_fixed_key | 1.042 | 1,135,444 |
| S-CRUD-005 | upsert_edge_fixed_triple | 0.375 | 2,857,449 |
| S-MAIN-001 | flush | 176,218.875 | 9 |
| S-TRAV-001 | neighbors | 3.667 | 313,567 |
| S-TRAV-002 | traverse_depth_2 | 151.042 | 7,513 |

### Node (`connector-benchmark-v3-parity`)
| Scenario ID | Scenario | p95 (us) | Throughput (ops/s) |
|---|---|---:|---:|
| S-ADV-001 | top_k_neighbors | 32.541 | 37,314 |
| S-ADV-003 | find_nodes_by_time_range | 257.250 | 4,223 |
| S-ADV-004 | personalized_pagerank | 432.041 | 2,420 |
| S-ADV-005 | export_adjacency | 1,103.042 | 1,012 |
| S-BATCH-001 | batch_upsert_nodes_json | 4,996.333 | 372,450 |
| S-CRUD-001 | upsert_node | 3.458 | 470,577 |
| S-CRUD-002 | upsert_edge | 2.666 | 563,889 |
| S-CRUD-003 | get_node | 0.875 | 1,558,452 |
| S-CRUD-004 | upsert_node_fixed_key | 1.750 | 618,200 |
| S-CRUD-005 | upsert_edge_fixed_triple | 0.584 | 1,797,268 |
| S-MAIN-001 | flush | 167,777.625 | 9 |
| S-TRAV-001 | neighbors | 5.375 | 283,397 |
| S-TRAV-002 | traverse_depth_2 | 144.792 | 7,659 |

### Python (`connector-benchmark-v2-parity`)
| Scenario ID | Scenario | p95 (us) | Throughput (ops/s) |
|---|---|---:|---:|
| S-ADV-001 | top_k_neighbors | 24.750 | 45,141 |
| S-ADV-003 | find_nodes_by_time_range | 378.125 | 2,795 |
| S-ADV-004 | personalized_pagerank | 537.542 | 1,976 |
| S-ADV-005 | export_adjacency | 1,040.166 | 1,023 |
| S-BATCH-001 | batch_upsert_nodes_json | 1,895.333 | 524,780 |
| S-CRUD-001 | upsert_node | 2.583 | 639,146 |
| S-CRUD-002 | upsert_edge | 1.416 | 686,424 |
| S-CRUD-003 | get_node | 0.500 | 2,865,330 |
| S-CRUD-004 | upsert_node_fixed_key | 1.417 | 774,833 |
| S-CRUD-005 | upsert_edge_fixed_triple | 0.542 | 2,102,828 |
| S-MAIN-001 | flush | 161,597.041 | 9 |
| S-TRAV-001 | neighbors | 4.750 | 225,407 |
| S-TRAV-002 | traverse_depth_2 | 176.208 | 6,646 |

### Connector Overhead (medium)
| Scenario | Rust p95 (us) | Node p95 (us) | Node/Rust | Python p95 (us) | Py/Rust |
|---|---:|---:|---:|---:|---:|
| get_node | 0.208 | 0.875 | 4.21x | 0.500 | 2.40x |
| upsert_node_fixed_key | 1.042 | 1.750 | 1.68x | 1.417 | 1.36x |
| upsert_edge_fixed_triple | 0.375 | 0.584 | 1.56x | 0.542 | 1.45x |
| upsert_node | 2.250 | 3.458 | 1.54x | 2.583 | 1.15x |
| upsert_edge | 2.791 | 2.666 | 0.96x | 1.416 | 0.51x |
| neighbors | 3.667 | 5.375 | 1.47x | 4.750 | 1.30x |
| traverse_depth_2 | 151.042 | 144.792 | 0.96x | 176.208 | 1.17x |
| top_k_neighbors | 35.208 | 32.541 | 0.92x | 24.750 | 0.70x |
| find_nodes_by_time_range | 257.250 | 257.250 | 1.00x | 378.125 | 1.47x |
| personalized_pagerank | 420.333 | 432.041 | 1.03x | 537.542 | 1.28x |
| export_adjacency | 997.458 | 1,103.042 | 1.11x | 1,040.166 | 1.04x |
| batch_upsert_nodes_json | 2,177.083 | 4,996.333 | 2.29x | 1,895.333 | 0.87x |
| flush | 176,218.875 | 167,777.625 | 0.95x | 161,597.041 | 0.92x |

---

## Large Profile (1M nodes / 5M edges)

### Rust (`core-benchmark-v1-parity`)
| Scenario ID | Scenario | p95 (us) | Throughput (ops/s) |
|---|---|---:|---:|
| S-ADV-001 | top_k_neighbors | 265.333 | 4,344 |
| S-ADV-003 | find_nodes_by_time_range | 608.834 | 1,908 |
| S-ADV-004 | personalized_pagerank | 4,321.250 | 256 |
| S-ADV-005 | export_adjacency | 2,109.959 | 509 |
| S-BATCH-001 | batch_upsert_nodes_json | 5,364.416 | 576,023 |
| S-CRUD-001 | upsert_node | 2.000 | 786,194 |
| S-CRUD-002 | upsert_edge | 2.125 | 703,062 |
| S-CRUD-003 | get_node | 0.208 | 5,926,804 |
| S-CRUD-004 | upsert_node_fixed_key | 0.833 | 1,281,784 |
| S-CRUD-005 | upsert_edge_fixed_triple | 0.417 | 2,659,840 |
| S-MAIN-001 | flush | 175,853.541 | 9 |
| S-TRAV-001 | neighbors | 2.458 | 465,918 |
| S-TRAV-002 | traverse_depth_2 | 141.625 | 7,879 |

### Node (`connector-benchmark-v3-parity`)
| Scenario ID | Scenario | p95 (us) | Throughput (ops/s) |
|---|---|---:|---:|
| S-ADV-001 | top_k_neighbors | 263.334 | 4,258 |
| S-ADV-003 | find_nodes_by_time_range | 630.667 | 1,748 |
| S-ADV-004 | personalized_pagerank | 3,987.208 | 269 |
| S-ADV-005 | export_adjacency | 2,256.875 | 474 |
| S-BATCH-001 | batch_upsert_nodes_json | 5,275.542 | 428,306 |
| S-CRUD-001 | upsert_node | 4.000 | 512,410 |
| S-CRUD-002 | upsert_edge | 3.000 | 638,488 |
| S-CRUD-003 | get_node | 0.917 | 1,585,383 |
| S-CRUD-004 | upsert_node_fixed_key | 1.500 | 697,143 |
| S-CRUD-005 | upsert_edge_fixed_triple | 0.542 | 1,889,823 |
| S-MAIN-001 | flush | 179,141.042 | 9 |
| S-TRAV-001 | neighbors | 4.542 | 297,121 |
| S-TRAV-002 | traverse_depth_2 | 150.125 | 7,463 |

### Python (`connector-benchmark-v2-parity`)
| Scenario ID | Scenario | p95 (us) | Throughput (ops/s) |
|---|---|---:|---:|
| S-ADV-001 | top_k_neighbors | 237.666 | 6,481 |
| S-ADV-003 | find_nodes_by_time_range | 1,234.792 | 991 |
| S-ADV-004 | personalized_pagerank | 5,472.542 | 203 |
| S-ADV-005 | export_adjacency | 2,512.541 | 458 |
| S-BATCH-001 | batch_upsert_nodes_json | 4,892.917 | 518,575 |
| S-CRUD-001 | upsert_node | 2.583 | 704,846 |
| S-CRUD-002 | upsert_edge | 1.333 | 743,612 |
| S-CRUD-003 | get_node | 0.459 | 2,774,887 |
| S-CRUD-004 | upsert_node_fixed_key | 1.250 | 864,482 |
| S-CRUD-005 | upsert_edge_fixed_triple | 0.500 | 2,261,740 |
| S-MAIN-001 | flush | 162,210.750 | 9 |
| S-TRAV-001 | neighbors | 4.584 | 230,934 |
| S-TRAV-002 | traverse_depth_2 | 152.417 | 7,207 |

### Connector Overhead (large)
| Scenario | Rust p95 (us) | Node p95 (us) | Node/Rust | Python p95 (us) | Py/Rust |
|---|---:|---:|---:|---:|---:|
| get_node | 0.208 | 0.917 | 4.41x | 0.459 | 2.21x |
| upsert_node_fixed_key | 0.833 | 1.500 | 1.80x | 1.250 | 1.50x |
| upsert_edge_fixed_triple | 0.417 | 0.542 | 1.30x | 0.500 | 1.20x |
| upsert_node | 2.000 | 4.000 | 2.00x | 2.583 | 1.29x |
| upsert_edge | 2.125 | 3.000 | 1.41x | 1.333 | 0.63x |
| neighbors | 2.458 | 4.542 | 1.85x | 4.584 | 1.86x |
| traverse_depth_2 | 141.625 | 150.125 | 1.06x | 152.417 | 1.08x |
| top_k_neighbors | 265.333 | 263.334 | 0.99x | 237.666 | 0.90x |
| find_nodes_by_time_range | 608.834 | 630.667 | 1.04x | 1,234.792 | 2.03x |
| personalized_pagerank | 4,321.250 | 3,987.208 | 0.92x | 5,472.542 | 1.27x |
| export_adjacency | 2,109.959 | 2,256.875 | 1.07x | 2,512.541 | 1.19x |
| batch_upsert_nodes_json | 5,364.416 | 5,275.542 | 0.98x | 4,892.917 | 0.91x |
| flush | 175,853.541 | 179,141.042 | 1.02x | 162,210.750 | 0.92x |

---

## Artifacts
- `run-data/rust-{small,medium,large}.json`
- `run-data/node-{small,medium,large}.json`
- `run-data/python-{small,medium,large}.json`
- `parity-report.md`
- `results-small-group-commit-parity.json`
