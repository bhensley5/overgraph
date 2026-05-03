# Shared Workload Profiles

This directory defines the cross-language benchmark profile contract for Phase 17.

## Files
- `profiles.json`: canonical profile definitions (`small`, `medium`, `large`, `xlarge`) and deterministic generation rules.
- `scenario-contract.json`: shared effective-config scaling, scenario iteration policy, comparability flags, and percentile method contract.

## Determinism Rules
- Seed: `1729`
- Node key format: `node:{i}`
- Type IDs cycle through `[1, 2, 3, 4, 5]`
- Weight formula: `0.5 + ((i mod 500) / 1000.0)`
- Temporal edge fields use:
  - `valid_from = 1700000000000 + i`
  - `valid_to = valid_from + 604800000` (7 days)

## Shape Mix Contract
Each profile defines percentages for:
- `chain_pct`
- `hub_pct`
- `powerlaw_pct`
- `temporal_pct`

The percentages must sum to `100`.

Current status:
- `shape_mix` is tracked as workload metadata and parity contract input.
- Phase 17 harness scenario scaling does not yet synthesize mixed-topology graphs directly from these percentages.
- Public reports should not claim shape-mix-driven graph synthesis until a dedicated mixed-shape generator is added.

## Profile Contract
Each profile entry includes:
- `nodes`
- `edges`
- `average_degree_target`
- `property_payload`
- `shape_mix`
- `batch_sizes`

All benchmark harnesses (Rust, Node.js, Python) must load these fields consistently. Active scaling currently uses `nodes`, `edges`, `average_degree_target`, and `batch_sizes`; `property_payload`/`shape_mix` are carried as contract metadata.

## Scenario Contract
`scenario-contract.json` is the cross-language parity source of truth for:
- effective workload scaling derived from profile values
- scenario-specific warmup/measurement iteration policy
- per-scenario comparability (`comparable` vs `non_comparable` + reason)
- percentile computation method declaration

## Query Scenario Set
Phase 23 adds a query-only scenario set for native planner parity:

- `S-QUERY-001`: `query_node_ids_intersected_predicates`
- `S-QUERY-002`: `query_nodes_intersected_predicates_hydrated`

These scenarios benchmark planner intersection over existing single-source property indexes. They
must not be described as compound-index benchmarks; maintained compound/composite indexes remain a
separate follow-up.
