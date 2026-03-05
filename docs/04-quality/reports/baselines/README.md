# Benchmark Baselines

This directory stores accepted baseline files for regression checks.

Expected layout:

`docs/04-quality/reports/baselines/<environment>/<profile>/<language>.json`

Example baseline snapshots (sanitized, reproducible):
- `docs/04-quality/reports/baselines/examples/small/{rust,node,python}.json`
- `docs/04-quality/reports/baselines/examples/medium/{rust,node,python}.json`
- `docs/04-quality/reports/baselines/examples/large/{rust,node,python}.json`

All example sets are aligned with parity harness stages (`*-parity`) and scenario IDs (`S-*`).

Notes:
- Baselines are environment-specific. Do not compare macOS-local and Linux-CI baselines directly.
- CI workflow uses `docs/04-quality/reports/baselines/gha-ubuntu-22.04/<profile>/` by default.
- CI now treats missing baseline files as an error (instead of silently skipping regression checks).
- Small-profile baselines refreshed 2026-03-04 after BigInt→Number Node.js migration (commit 3789864).
