from __future__ import annotations

import json
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[3]
COMPARE = REPO_ROOT / "tools/bench/compare_baseline.py"


def make_run_payload(
    *,
    language: str,
    profile_name: str,
    harness_stage: str,
    scenarios: list[dict[str, object]],
) -> dict[str, object]:
    return {
        "schema_version": 1,
        "language": language,
        "profile_name": profile_name,
        "harness_stage": harness_stage,
        "status": "ok",
        "parsed_stdout_json": {
            "schema_version": 1,
            "language": language,
            "profile_name": profile_name,
            "harness_stage": harness_stage,
            "scenarios": scenarios,
        },
    }


class CompareBaselineTests(unittest.TestCase):
    def test_compare_passes_for_small_delta(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            tmpdir = Path(tmp)
            baseline = tmpdir / "baseline.json"
            candidate = tmpdir / "candidate.json"
            report = tmpdir / "report.json"

            baseline.write_text(
                json.dumps(
                    make_run_payload(
                        language="python",
                        profile_name="small",
                        harness_stage="connector-benchmark-v1",
                        scenarios=[
                            {"scenario_id": "S-CRUD-001", "name": "upsert_node", "category": "crud", "stats": {"p95_us": 100.0}},
                            {"scenario_id": "S-CRUD-002", "name": "upsert_edge", "category": "crud", "stats": {"p95_us": 200.0}},
                        ],
                    )
                ),
                encoding="utf-8",
            )
            candidate.write_text(
                json.dumps(
                    make_run_payload(
                        language="python",
                        profile_name="small",
                        harness_stage="connector-benchmark-v1",
                        scenarios=[
                            {"scenario_id": "S-CRUD-001", "name": "upsert_node", "category": "crud", "stats": {"p95_us": 104.0}},
                            {"scenario_id": "S-CRUD-002", "name": "upsert_edge", "category": "crud", "stats": {"p95_us": 206.0}},
                        ],
                    )
                ),
                encoding="utf-8",
            )

            proc = subprocess.run(
                [
                    sys.executable,
                    str(COMPARE),
                    "--baseline",
                    str(baseline),
                    "--candidate",
                    str(candidate),
                    "--report-json",
                    str(report),
                ],
                cwd=REPO_ROOT,
                capture_output=True,
                text=True,
                check=False,
            )
            self.assertEqual(proc.returncode, 0, msg=proc.stderr)
            payload = json.loads(report.read_text(encoding="utf-8"))
            self.assertEqual(payload["summary"]["failure_count"], 0)
            self.assertEqual(payload["summary"]["warning_count"], 0)

    def test_compare_fails_when_regression_exceeds_fail_threshold(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            tmpdir = Path(tmp)
            baseline = tmpdir / "baseline.json"
            candidate = tmpdir / "candidate.json"
            report_md = tmpdir / "report.md"

            baseline.write_text(
                json.dumps(
                    make_run_payload(
                        language="node",
                        profile_name="small",
                        harness_stage="connector-benchmark-v2",
                        scenarios=[
                            {"scenario_id": "S-MAIN-001", "name": "flush", "category": "maintenance", "stats": {"p95_us": 1000.0}},
                        ],
                    )
                ),
                encoding="utf-8",
            )
            candidate.write_text(
                json.dumps(
                    make_run_payload(
                        language="node",
                        profile_name="small",
                        harness_stage="connector-benchmark-v2",
                        scenarios=[
                            {"scenario_id": "S-MAIN-001", "name": "flush", "category": "maintenance", "stats": {"p95_us": 1400.0}},
                        ],
                    )
                ),
                encoding="utf-8",
            )

            proc = subprocess.run(
                [
                    sys.executable,
                    str(COMPARE),
                    "--baseline",
                    str(baseline),
                    "--candidate",
                    str(candidate),
                    "--warn-threshold-pct",
                    "10",
                    "--fail-threshold-pct",
                    "20",
                    "--report-md",
                    str(report_md),
                ],
                cwd=REPO_ROOT,
                capture_output=True,
                text=True,
                check=False,
            )
            self.assertEqual(proc.returncode, 2)
            report_text = report_md.read_text(encoding="utf-8")
            self.assertIn("fail", report_text)
            self.assertIn("+40.00%", report_text)

    def test_allowlist_suppresses_failure(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            tmpdir = Path(tmp)
            baseline = tmpdir / "baseline.json"
            candidate = tmpdir / "candidate.json"
            allowlist = tmpdir / "allowlist.json"
            report = tmpdir / "report.json"

            baseline.write_text(
                json.dumps(
                    make_run_payload(
                        language="rust",
                        profile_name="small",
                        harness_stage="criterion-quick-core-advanced-maintenance-recovery",
                        scenarios=[
                            {"scenario_id": "S-MAIN-001", "name": "flush", "category": "maintenance", "stats": {"p95_us": 100.0}},
                        ],
                    )
                ),
                encoding="utf-8",
            )
            candidate.write_text(
                json.dumps(
                    make_run_payload(
                        language="rust",
                        profile_name="small",
                        harness_stage="criterion-quick-core-advanced-maintenance-recovery",
                        scenarios=[
                            {"scenario_id": "S-MAIN-001", "name": "flush", "category": "maintenance", "stats": {"p95_us": 180.0}},
                        ],
                    )
                ),
                encoding="utf-8",
            )
            allowlist.write_text(
                json.dumps(
                    {
                        "schema_version": 1,
                        "entries": [
                            {
                                "scenario_id": "S-MAIN-001",
                                "language": "rust",
                                "profile_name": "small",
                                "harness_stage": "criterion-quick-core-advanced-maintenance-recovery",
                                "max_regression_pct": 100,
                                "reason": "expected maintenance variance in shared runner",
                            }
                        ],
                    }
                ),
                encoding="utf-8",
            )

            proc = subprocess.run(
                [
                    sys.executable,
                    str(COMPARE),
                    "--baseline",
                    str(baseline),
                    "--candidate",
                    str(candidate),
                    "--allowlist",
                    str(allowlist),
                    "--report-json",
                    str(report),
                ],
                cwd=REPO_ROOT,
                capture_output=True,
                text=True,
                check=False,
            )
            self.assertEqual(proc.returncode, 0, msg=proc.stderr)
            payload = json.loads(report.read_text(encoding="utf-8"))
            self.assertEqual(payload["summary"]["failure_count"], 0)
            self.assertEqual(payload["summary"]["allowlisted_count"], 1)

    def test_profile_mismatch_fails_fast(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            tmpdir = Path(tmp)
            baseline = tmpdir / "baseline.json"
            candidate = tmpdir / "candidate.json"

            baseline.write_text(
                json.dumps(
                    make_run_payload(
                        language="rust",
                        profile_name="small",
                        harness_stage="core-benchmark-v1-parity",
                        scenarios=[
                            {"scenario_id": "S-CRUD-001", "name": "upsert_node", "category": "crud", "stats": {"p95_us": 100.0}},
                        ],
                    )
                ),
                encoding="utf-8",
            )
            candidate.write_text(
                json.dumps(
                    make_run_payload(
                        language="rust",
                        profile_name="large",
                        harness_stage="core-benchmark-v1-parity",
                        scenarios=[
                            {"scenario_id": "S-CRUD-001", "name": "upsert_node", "category": "crud", "stats": {"p95_us": 101.0}},
                        ],
                    )
                ),
                encoding="utf-8",
            )

            proc = subprocess.run(
                [
                    sys.executable,
                    str(COMPARE),
                    "--baseline",
                    str(baseline),
                    "--candidate",
                    str(candidate),
                ],
                cwd=REPO_ROOT,
                capture_output=True,
                text=True,
                check=False,
            )
            self.assertNotEqual(proc.returncode, 0)
            self.assertIn("Profile mismatch", proc.stderr + proc.stdout)

    def test_harness_stage_mismatch_fails_fast(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            tmpdir = Path(tmp)
            baseline = tmpdir / "baseline.json"
            candidate = tmpdir / "candidate.json"

            baseline.write_text(
                json.dumps(
                    make_run_payload(
                        language="node",
                        profile_name="small",
                        harness_stage="connector-benchmark-v2",
                        scenarios=[
                            {"scenario_id": "S-CRUD-001", "name": "upsert_node", "category": "crud", "stats": {"p95_us": 100.0}},
                        ],
                    )
                ),
                encoding="utf-8",
            )
            candidate.write_text(
                json.dumps(
                    make_run_payload(
                        language="node",
                        profile_name="small",
                        harness_stage="connector-benchmark-v3-parity",
                        scenarios=[
                            {"scenario_id": "S-CRUD-001", "name": "upsert_node", "category": "crud", "stats": {"p95_us": 101.0}},
                        ],
                    )
                ),
                encoding="utf-8",
            )

            proc = subprocess.run(
                [
                    sys.executable,
                    str(COMPARE),
                    "--baseline",
                    str(baseline),
                    "--candidate",
                    str(candidate),
                ],
                cwd=REPO_ROOT,
                capture_output=True,
                text=True,
                check=False,
            )
            self.assertNotEqual(proc.returncode, 0)
            self.assertIn("Harness stage mismatch", proc.stderr + proc.stdout)

    def test_no_overlapping_scenarios_fails_fast(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            tmpdir = Path(tmp)
            baseline = tmpdir / "baseline.json"
            candidate = tmpdir / "candidate.json"

            baseline.write_text(
                json.dumps(
                    make_run_payload(
                        language="python",
                        profile_name="small",
                        harness_stage="connector-benchmark-v2-parity",
                        scenarios=[
                            {"scenario_id": "S-CRUD-001", "name": "upsert_node", "category": "crud", "stats": {"p95_us": 100.0}},
                        ],
                    )
                ),
                encoding="utf-8",
            )
            candidate.write_text(
                json.dumps(
                    make_run_payload(
                        language="python",
                        profile_name="small",
                        harness_stage="connector-benchmark-v2-parity",
                        scenarios=[
                            {"scenario_id": "S-ADV-005", "name": "export_adjacency", "category": "advanced", "stats": {"p95_us": 101.0}},
                        ],
                    )
                ),
                encoding="utf-8",
            )

            proc = subprocess.run(
                [
                    sys.executable,
                    str(COMPARE),
                    "--baseline",
                    str(baseline),
                    "--candidate",
                    str(candidate),
                ],
                cwd=REPO_ROOT,
                capture_output=True,
                text=True,
                check=False,
            )
            self.assertNotEqual(proc.returncode, 0)
            self.assertIn("No overlapping scenario IDs", proc.stderr + proc.stdout)

    def test_fail_on_warning_returns_exit_code_one(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            tmpdir = Path(tmp)
            baseline = tmpdir / "baseline.json"
            candidate = tmpdir / "candidate.json"

            baseline.write_text(
                json.dumps(
                    make_run_payload(
                        language="rust",
                        profile_name="small",
                        harness_stage="core-benchmark-v1-parity",
                        scenarios=[
                            {"scenario_id": "S-CRUD-001", "name": "upsert_node", "category": "crud", "stats": {"p95_us": 100.0}},
                        ],
                    )
                ),
                encoding="utf-8",
            )
            candidate.write_text(
                json.dumps(
                    make_run_payload(
                        language="rust",
                        profile_name="small",
                        harness_stage="core-benchmark-v1-parity",
                        scenarios=[
                            {"scenario_id": "S-CRUD-001", "name": "upsert_node", "category": "crud", "stats": {"p95_us": 112.0}},
                        ],
                    )
                ),
                encoding="utf-8",
            )

            proc = subprocess.run(
                [
                    sys.executable,
                    str(COMPARE),
                    "--baseline",
                    str(baseline),
                    "--candidate",
                    str(candidate),
                    "--warn-threshold-pct",
                    "10",
                    "--fail-threshold-pct",
                    "20",
                    "--fail-on-warning",
                ],
                cwd=REPO_ROOT,
                capture_output=True,
                text=True,
                check=False,
            )
            self.assertEqual(proc.returncode, 1)
            self.assertIn("warnings=1", proc.stdout)


if __name__ == "__main__":
    unittest.main()
