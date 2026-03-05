#!/usr/bin/env python3
"""Phase 17 benchmark runner scaffold.

Generates standardized benchmark artifacts:
  - manifest.json
  - <lang>.json
  - summary.md
  - raw stdout/stderr logs

This CP1 runner focuses on schema/report scaffolding and metadata capture.
"""

from __future__ import annotations

import argparse
import json
import os
import platform
import re
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


REPO_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_PROFILES_FILE = REPO_ROOT / "docs/04-quality/workloads/profiles.json"
DEFAULT_OUTPUT_ROOT = REPO_ROOT / "docs/04-quality/reports"
CRITERION_TIME_RE = re.compile(
    r"time:\s+\[\s*([0-9]+(?:\.[0-9]+)?)\s*([a-zA-Zµμ]+)\s+"
    r"([0-9]+(?:\.[0-9]+)?)\s*([a-zA-Zµμ]+)\s+"
    r"([0-9]+(?:\.[0-9]+)?)\s*([a-zA-Zµμ]+)\s*\]"
)


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def timestamp_tag() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def sanitize_tag(text: str) -> str:
    lowered = text.strip().lower()
    return re.sub(r"[^a-z0-9._-]+", "-", lowered).strip("-")


def run_capture(cmd: list[str], cwd: Path | None = None, timeout_seconds: int = 0) -> dict[str, Any]:
    started = time.time()
    try:
        proc = subprocess.run(
            cmd,
            cwd=str(cwd) if cwd else None,
            capture_output=True,
            text=True,
            timeout=timeout_seconds if timeout_seconds > 0 else None,
            check=False,
        )
        timed_out = False
    except subprocess.TimeoutExpired as exc:
        proc = None
        timed_out = True
        stdout = exc.stdout or ""
        stderr = exc.stderr or ""
        return {
            "ok": False,
            "return_code": None,
            "timed_out": True,
            "duration_ms": round((time.time() - started) * 1000.0, 3),
            "stdout": stdout,
            "stderr": stderr,
        }
    except FileNotFoundError as exc:
        return {
            "ok": False,
            "return_code": None,
            "timed_out": False,
            "duration_ms": round((time.time() - started) * 1000.0, 3),
            "stdout": "",
            "stderr": str(exc),
        }

    return {
        "ok": proc.returncode == 0 and not timed_out,
        "return_code": proc.returncode,
        "timed_out": timed_out,
        "duration_ms": round((time.time() - started) * 1000.0, 3),
        "stdout": proc.stdout,
        "stderr": proc.stderr,
    }


def command_output(cmd: list[str]) -> str | None:
    result = run_capture(cmd, cwd=REPO_ROOT, timeout_seconds=10)
    if result["ok"]:
        return (result["stdout"] or "").strip() or None
    return None


def detect_memory_bytes() -> int | None:
    system = platform.system().lower()
    if system == "darwin":
        out = command_output(["sysctl", "-n", "hw.memsize"])
        if out and out.isdigit():
            return int(out)
    if system == "linux":
        try:
            meminfo = Path("/proc/meminfo").read_text(encoding="utf-8")
            for line in meminfo.splitlines():
                if line.startswith("MemTotal:"):
                    parts = line.split()
                    if len(parts) >= 2 and parts[1].isdigit():
                        return int(parts[1]) * 1024
        except OSError:
            pass
    # Cross-platform fallback when platform-specific probes are unavailable.
    try:
        phys_pages = os.sysconf("SC_PHYS_PAGES")
        page_size = os.sysconf("SC_PAGE_SIZE")
        if isinstance(phys_pages, int) and isinstance(page_size, int):
            if phys_pages > 0 and page_size > 0:
                return phys_pages * page_size
    except (ValueError, OSError, AttributeError):
        pass
    return None


def collect_metadata(profile_name: str, profile: dict[str, Any]) -> dict[str, Any]:
    return {
        "captured_at_utc": utc_now_iso(),
        "hostname": platform.node(),
        "system": platform.system(),
        "release": platform.release(),
        "platform": platform.platform(),
        "machine": platform.machine(),
        "processor": platform.processor() or None,
        "cpu_count_logical": os.cpu_count(),
        "memory_bytes": detect_memory_bytes(),
        "python_version": platform.python_version(),
        "node_version": command_output(["node", "--version"]),
        "rustc_version": command_output(["rustc", "--version"]),
        "cargo_version": command_output(["cargo", "--version"]),
        "git_sha": command_output(["git", "rev-parse", "HEAD"]),
        "profile_name": profile_name,
        "profile": profile,
    }


def load_profiles(path: Path) -> dict[str, Any]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if "profiles" not in payload or not isinstance(payload["profiles"], dict):
        raise ValueError("profiles.json missing 'profiles' object")

    required_profile_fields = {
        "description",
        "nodes",
        "edges",
        "average_degree_target",
        "property_payload",
        "shape_mix",
        "batch_sizes",
    }
    for name, profile in payload["profiles"].items():
        missing = required_profile_fields - set(profile.keys())
        if missing:
            raise ValueError(f"profile '{name}' missing fields: {sorted(missing)}")
        shape_mix = profile["shape_mix"]
        if not isinstance(shape_mix, dict):
            raise ValueError(f"profile '{name}' shape_mix must be an object")
        total = sum(int(v) for v in shape_mix.values())
        if total != 100:
            raise ValueError(f"profile '{name}' shape_mix must sum to 100 (got {total})")
    return payload


def build_command(lang: str, profile: str, warmup: int, iters: int) -> tuple[list[str], str]:
    if lang == "rust":
        return (
            [
                "cargo",
                "run",
                "--release",
                "--bin",
                "benchmark_harness",
                "--",
                "--profile",
                profile,
                "--warmup",
                str(warmup),
                "--iters",
                str(iters),
            ],
            "core-benchmark-v1-parity",
        )
    if lang == "node":
        return (
            [
                "node",
                "overgraph-node/__test__/benchmark-v2.mjs",
                "--profile",
                profile,
                "--warmup",
                str(warmup),
                "--iters",
                str(iters),
            ],
            "connector-benchmark-v3-parity",
        )
    if lang == "python":
        return (
            [
                sys.executable,
                str(REPO_ROOT / "tools/bench/python_connector_benchmark.py"),
                "--profile",
                profile,
                "--warmup",
                str(warmup),
                "--iters",
                str(iters),
            ],
            "connector-benchmark-v2-parity",
        )
    raise ValueError(f"unsupported language: {lang}")


def ensure_unique_run_dir(root: Path, run_id: str) -> Path:
    candidate = root / run_id
    if not candidate.exists():
        candidate.mkdir(parents=True, exist_ok=False)
        return candidate
    suffix = 1
    while suffix <= 10_000:
        next_candidate = root / f"{run_id}-{suffix}"
        if not next_candidate.exists():
            next_candidate.mkdir(parents=True, exist_ok=False)
            return next_candidate
        suffix += 1
    raise RuntimeError(f"unable to allocate unique run dir for run_id='{run_id}' after 10000 attempts")


def write_json(path: Path, payload: dict[str, Any]) -> None:
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def write_summary(path: Path, manifest: dict[str, Any], result: dict[str, Any]) -> None:
    lines = [
        "# Benchmark Run Summary",
        "",
        f"- Run ID: `{manifest['run_id']}`",
        f"- Language: `{manifest['language']}`",
        f"- Profile: `{manifest['profile_name']}`",
        f"- Created (UTC): `{manifest['created_at_utc']}`",
        f"- Status: `{result['status']}`",
        f"- Command: `{result['command_display']}`",
        f"- Duration (ms): `{result.get('duration_ms')}`",
        f"- Return code: `{result.get('return_code')}`",
        "",
        "## Metadata",
        "",
        f"- Hostname: `{manifest['metadata'].get('hostname')}`",
        f"- Platform: `{manifest['metadata'].get('platform')}`",
        f"- Machine: `{manifest['metadata'].get('machine')}`",
        f"- CPU (logical): `{manifest['metadata'].get('cpu_count_logical')}`",
        f"- Memory bytes: `{manifest['metadata'].get('memory_bytes')}`",
        f"- Python: `{manifest['metadata'].get('python_version')}`",
        f"- Node: `{manifest['metadata'].get('node_version')}`",
        f"- Rustc: `{manifest['metadata'].get('rustc_version')}`",
        f"- Cargo: `{manifest['metadata'].get('cargo_version')}`",
        f"- Git SHA: `{manifest['metadata'].get('git_sha')}`",
        "",
        "## Artifacts",
        "",
        f"- `manifest.json`",
        f"- `{manifest['language']}.json`",
        f"- `{manifest['language']}.stdout.log`",
        f"- `{manifest['language']}.stderr.log`",
    ]
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def duration_to_us(value: float, unit: str) -> float:
    normalized = unit.strip().lower()
    if normalized in {"ns"}:
        return value / 1000.0
    if normalized in {"us", "µs", "μs"}:
        return value
    if normalized in {"ms"}:
        return value * 1000.0
    if normalized in {"s", "sec", "secs", "second", "seconds"}:
        return value * 1_000_000.0
    raise ValueError(f"unsupported duration unit: {unit}")


def rust_scenario_id(label: str) -> str:
    token = re.sub(r"[^a-zA-Z0-9]+", "-", label.strip()).strip("-").upper()
    return f"RUST-{token}" if token else "RUST-UNKNOWN"


def parse_rust_criterion_stdout(stdout: str, profile_name: str, harness_stage: str) -> dict[str, Any] | None:
    scenarios: list[dict[str, Any]] = []
    current_label: str | None = None

    for raw_line in stdout.splitlines():
        line = raw_line.rstrip()
        stripped = line.strip()
        if not stripped:
            continue

        if "time:" in stripped:
            match = CRITERION_TIME_RE.search(stripped)
            if not match:
                continue

            label_prefix = stripped.split("time:", 1)[0].strip()
            label = label_prefix or current_label
            if not label:
                continue

            low = duration_to_us(float(match.group(1)), match.group(2))
            median = duration_to_us(float(match.group(3)), match.group(4))
            high = duration_to_us(float(match.group(5)), match.group(6))

            if "/" in label:
                category = label.split("/", 1)[0]
                name = label.split("/", 1)[1]
            else:
                category = "core"
                name = label

            scenarios.append(
                {
                    "scenario_id": rust_scenario_id(label),
                    "name": name,
                    "category": category,
                    "warmup_iterations": None,
                    "benchmark_iterations": None,
                    "ops_per_iteration": 1,
                    "throughput_ops_per_sec": None,
                    "stats": {
                        "p50_us": median,
                        # Criterion quick output exposes low/median/high bounds.
                        # Use high bound as a conservative p95/p99 proxy for regression checks.
                        "p95_us": high,
                        "p99_us": high,
                        "min_us": low,
                        "max_us": high,
                        "mean_us": median,
                    },
                    "notes": "criterion quick estimate from [low median high] interval",
                }
            )
            continue

        # Track the most recent benchmark label for two-line criterion entries.
        if not stripped.startswith("change:") and not stripped.startswith("No change") and not stripped.startswith("Performance has"):
            current_label = stripped

    if not scenarios:
        return None
    return {
        "schema_version": 1,
        "language": "rust",
        "harness_stage": harness_stage,
        "profile_name": profile_name,
        "generated_at_utc": utc_now_iso(),
        "scenarios": scenarios,
    }


def parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run benchmark scaffold for one language.")
    parser.add_argument("--lang", required=True, choices=["rust", "node", "python"])
    parser.add_argument("--profile", default="small", choices=["small", "medium", "large", "xlarge"])
    parser.add_argument("--profiles-file", default=str(DEFAULT_PROFILES_FILE))
    parser.add_argument("--output-root", default=str(DEFAULT_OUTPUT_ROOT))
    parser.add_argument("--run-id", default=None)
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--timeout-seconds", type=int, default=0)
    parser.add_argument("--warmup", type=int, default=20)
    parser.add_argument("--iters", type=int, default=80)
    parser.add_argument(
        "--allow-legacy-rust-criterion-parse",
        action="store_true",
        help="Allow fallback parsing of non-JSON Rust Criterion output.",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv or sys.argv[1:])
    profiles_path = Path(args.profiles_file)
    output_root = Path(args.output_root)

    profiles_payload = load_profiles(profiles_path)
    profile = profiles_payload["profiles"][args.profile]

    platform_tag = sanitize_tag(f"{platform.system()}-{platform.machine()}")
    run_id = args.run_id or f"{timestamp_tag()}-{platform_tag}-{args.lang}-{args.profile}"
    output_root.mkdir(parents=True, exist_ok=True)
    run_dir = ensure_unique_run_dir(output_root, run_id)

    metadata = collect_metadata(args.profile, profile)
    command, harness_stage = build_command(args.lang, args.profile, args.warmup, args.iters)
    command_display = " ".join(command)

    stdout_log = run_dir / f"{args.lang}.stdout.log"
    stderr_log = run_dir / f"{args.lang}.stderr.log"

    if args.dry_run:
        execution = {
            "ok": True,
            "return_code": 0,
            "timed_out": False,
            "duration_ms": 0.0,
            "stdout": "",
            "stderr": "",
        }
        status = "dry-run"
    else:
        execution = run_capture(command, cwd=REPO_ROOT, timeout_seconds=args.timeout_seconds)
        status = "ok" if execution["ok"] else "failed"

    stdout_log.write_text(execution["stdout"] or "", encoding="utf-8")
    stderr_log.write_text(execution["stderr"] or "", encoding="utf-8")

    parsed_stdout_json = None
    raw_stdout = (execution["stdout"] or "").strip()
    if raw_stdout:
        try:
            parsed_stdout_json = json.loads(raw_stdout)
        except json.JSONDecodeError:
            if args.lang == "rust" and args.allow_legacy_rust_criterion_parse:
                parsed_stdout_json = parse_rust_criterion_stdout(raw_stdout, args.profile, harness_stage)
            else:
                parsed_stdout_json = None

    manifest = {
        "schema_version": 1,
        "run_id": run_id,
        "created_at_utc": utc_now_iso(),
        "language": args.lang,
        "profile_name": args.profile,
        "profiles_file": str(profiles_path.resolve()),
        "output_root": str(output_root.resolve()),
        "run_dir": str(run_dir.resolve()),
        "harness_stage": harness_stage,
        "metadata": metadata,
    }

    result = {
        "schema_version": 1,
        "run_id": run_id,
        "language": args.lang,
        "profile_name": args.profile,
        "status": status,
        "dry_run": args.dry_run,
        "command": command,
        "command_display": command_display,
        "harness_stage": harness_stage,
        "duration_ms": execution["duration_ms"],
        "return_code": execution["return_code"],
        "timed_out": execution["timed_out"],
        "stdout_log": stdout_log.name,
        "stderr_log": stderr_log.name,
        "parsed_stdout_json": parsed_stdout_json,
    }

    write_json(run_dir / "manifest.json", manifest)
    write_json(run_dir / f"{args.lang}.json", result)
    write_summary(run_dir / "summary.md", manifest, result)

    print(f"run_dir={run_dir}")
    print(f"status={status}")
    return 0 if status in {"ok", "dry-run"} else 1


if __name__ == "__main__":
    raise SystemExit(main())
