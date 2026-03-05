from __future__ import annotations

import json
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[3]
RUNNER = REPO_ROOT / "tools/bench/run_suite.py"
PROFILES = REPO_ROOT / "docs/04-quality/workloads/profiles.json"


class RunSuiteTests(unittest.TestCase):
    def test_profiles_contract_shape_mix_sums_to_100(self) -> None:
        payload = json.loads(PROFILES.read_text(encoding="utf-8"))
        profiles = payload["profiles"]
        self.assertIn("small", profiles)
        self.assertIn("medium", profiles)
        self.assertIn("large", profiles)
        self.assertIn("xlarge", profiles)

        for name, profile in profiles.items():
            shape_mix = profile["shape_mix"]
            total = sum(int(v) for v in shape_mix.values())
            self.assertEqual(
                total,
                100,
                msg=f"shape_mix for profile '{name}' must sum to 100",
            )

    def test_runner_generates_artifacts_in_dry_run(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            proc = subprocess.run(
                [
                    sys.executable,
                    str(RUNNER),
                    "--lang",
                    "rust",
                    "--profile",
                    "small",
                    "--dry-run",
                    "--output-root",
                    tmp,
                ],
                cwd=REPO_ROOT,
                capture_output=True,
                text=True,
                check=False,
            )
            self.assertEqual(proc.returncode, 0, msg=proc.stderr)

            run_dirs = [p for p in Path(tmp).iterdir() if p.is_dir()]
            self.assertEqual(len(run_dirs), 1)
            run_dir = run_dirs[0]

            self.assertTrue((run_dir / "manifest.json").exists())
            self.assertTrue((run_dir / "rust.json").exists())
            self.assertTrue((run_dir / "summary.md").exists())
            self.assertTrue((run_dir / "rust.stdout.log").exists())
            self.assertTrue((run_dir / "rust.stderr.log").exists())

            result = json.loads((run_dir / "rust.json").read_text(encoding="utf-8"))
            self.assertEqual(result["status"], "dry-run")

    def test_python_scaffold_run_parses_stdout_json(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            proc = subprocess.run(
                [
                    sys.executable,
                    str(RUNNER),
                    "--lang",
                    "python",
                    "--profile",
                    "small",
                    "--warmup",
                    "2",
                    "--iters",
                    "6",
                    "--output-root",
                    tmp,
                ],
                cwd=REPO_ROOT,
                capture_output=True,
                text=True,
                check=False,
            )
            self.assertEqual(proc.returncode, 0, msg=proc.stderr)

            run_dirs = [p for p in Path(tmp).iterdir() if p.is_dir()]
            self.assertEqual(len(run_dirs), 1)
            run_dir = run_dirs[0]
            result = json.loads((run_dir / "python.json").read_text(encoding="utf-8"))

            self.assertEqual(result["status"], "ok")
            self.assertIsInstance(result["parsed_stdout_json"], dict)
            self.assertEqual(result["parsed_stdout_json"]["language"], "python")
            self.assertEqual(
                result["parsed_stdout_json"]["harness_stage"],
                "connector-benchmark-v2-parity",
            )


if __name__ == "__main__":
    unittest.main()
