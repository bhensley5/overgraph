from __future__ import annotations

import unittest
from pathlib import Path
import sys


REPO_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(REPO_ROOT))

from tools.bench.run_suite import parse_rust_criterion_stdout  # noqa: E402


class RustParseTests(unittest.TestCase):
    def test_parse_rust_criterion_stdout_extracts_scenarios(self) -> None:
        sample = """
upsert_node             time:   [1.2448 µs 1.2583 µs 1.2617 µs]
group_commit/upsert_node_immediate
                        time:   [4.0066 ms 4.0068 ms 4.0076 ms]
"""
        parsed = parse_rust_criterion_stdout(
            sample,
            profile_name="small",
            harness_stage="criterion-quick-core-advanced-maintenance-recovery",
        )
        self.assertIsNotNone(parsed)
        assert parsed is not None
        self.assertEqual(parsed["language"], "rust")
        scenarios = parsed["scenarios"]
        self.assertEqual(len(scenarios), 2)

        self.assertEqual(scenarios[0]["name"], "upsert_node")
        self.assertEqual(scenarios[0]["category"], "core")
        self.assertGreater(scenarios[0]["stats"]["p95_us"], 1.0)

        self.assertEqual(scenarios[1]["name"], "upsert_node_immediate")
        self.assertEqual(scenarios[1]["category"], "group_commit")
        # ms -> us conversion
        self.assertGreater(scenarios[1]["stats"]["p50_us"], 4000.0)


if __name__ == "__main__":
    unittest.main()
