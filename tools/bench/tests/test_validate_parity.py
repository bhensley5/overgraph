from __future__ import annotations

import unittest
from copy import deepcopy
from pathlib import Path
import sys


REPO_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(REPO_ROOT))

from tools.bench.validate_parity import validate_parity  # noqa: E402


def base_payload(language: str) -> dict:
    return {
        "schema_version": 1,
        "language": language,
        "profile_name": "small",
        "percentile_method": {
            "name": "nearest_rank",
            "index_formula": "ceil((p / 100) * n) - 1",
            "interpolation": "none",
            "sorted_order": "ascending",
            "sample_unit": "microseconds",
        },
        "profile_contract": {
            "scenario_contract_schema_version": 1,
            "effective_config": {
                "nodes": 1000,
                "edges": 5000,
                "ppr_nodes": 500,
            }
        },
        "scenarios": [
            {
                "scenario_id": "S-ADV-004",
                "warmup_iterations": 10,
                "benchmark_iterations": 30,
                "scenario_params": {
                    "ppr_nodes": 500,
                    "max_iterations": 20,
                    "max_results": 50,
                },
                "comparability": {
                    "status": "comparable",
                    "reason": None,
                },
                "stats": {
                    "p50_us": 1.0,
                    "p95_us": 2.0,
                    "p99_us": 3.0,
                },
            }
        ],
    }


class ValidateParityTests(unittest.TestCase):
    def test_validate_parity_passes_for_matching_payloads(self) -> None:
        rust = base_payload("rust")
        node = base_payload("node")
        python = base_payload("python")

        # Add a connector-specific non-comparable scenario for node/python only.
        node["scenarios"].append(
            {
                "scenario_id": "S-BATCH-002",
                "warmup_iterations": 5,
                "benchmark_iterations": 20,
                "scenario_params": {"batch_nodes": 100, "encoding": "binary-pack-node-batch"},
                "comparability": {
                    "status": "non_comparable",
                    "reason": "connector-specific",
                },
                "stats": {
                    "p50_us": 5.0,
                    "p95_us": 6.0,
                    "p99_us": 7.0,
                },
            }
        )
        python["scenarios"].append(deepcopy(node["scenarios"][-1]))

        report = validate_parity(
            {
                "rust": rust,
                "node": node,
                "python": python,
            }
        )
        self.assertEqual(report["status"], "pass")
        self.assertIn("S-ADV-004", report["comparable_scenarios"])

    def test_validate_parity_fails_on_effective_config_mismatch(self) -> None:
        rust = base_payload("rust")
        node = base_payload("node")
        python = base_payload("python")

        python["profile_contract"]["effective_config"]["ppr_nodes"] = 600

        report = validate_parity(
            {
                "rust": rust,
                "node": node,
                "python": python,
            }
        )
        self.assertEqual(report["status"], "fail")
        self.assertTrue(any("effective_config mismatch" in e for e in report["errors"]))

    def test_validate_parity_fails_on_scenario_params_mismatch(self) -> None:
        rust = base_payload("rust")
        node = base_payload("node")
        python = base_payload("python")

        node["scenarios"][0]["scenario_params"]["max_results"] = 40

        report = validate_parity(
            {
                "rust": rust,
                "node": node,
                "python": python,
            }
        )
        self.assertEqual(report["status"], "fail")
        self.assertTrue(any("scenario_params mismatch" in e for e in report["errors"]))

    def test_validate_parity_fails_on_schema_version_mismatch(self) -> None:
        rust = base_payload("rust")
        node = base_payload("node")
        python = base_payload("python")

        python["schema_version"] = 2

        report = validate_parity(
            {
                "rust": rust,
                "node": node,
                "python": python,
            }
        )
        self.assertEqual(report["status"], "fail")
        self.assertTrue(any("schema_version mismatch" in e for e in report["errors"]))


if __name__ == "__main__":
    unittest.main()
