#!/usr/bin/env python3
"""Compare benchmark scenario latencies against a baseline.

This tool compares p95 latency per scenario and produces:
  - JSON summary
  - Markdown diff report

Exit codes:
  0 = no failing regressions
  1 = warnings only and --fail-on-warning was set
  2 = at least one failing regression
"""

from __future__ import annotations

import argparse
import json
import math
from dataclasses import dataclass
from pathlib import Path
from typing import Any


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Compare benchmark run output to baseline.")
    parser.add_argument("--baseline", required=True, help="Baseline run dir or JSON file path.")
    parser.add_argument("--candidate", required=True, help="Candidate run dir or JSON file path.")
    parser.add_argument("--allowlist", default=None, help="Optional regression allowlist JSON file.")
    parser.add_argument("--warn-threshold-pct", type=float, default=10.0)
    parser.add_argument("--fail-threshold-pct", type=float, default=20.0)
    parser.add_argument("--report-md", default=None, help="Optional markdown report output path.")
    parser.add_argument("--report-json", default=None, help="Optional JSON report output path.")
    parser.add_argument("--fail-on-warning", action="store_true")
    return parser.parse_args()


def load_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def resolve_run_json(path_str: str) -> tuple[Path, dict[str, Any]]:
    path = Path(path_str)
    if path.is_file():
        payload = load_json(path)
        return path, payload

    if not path.is_dir():
        raise FileNotFoundError(f"Path not found: {path}")

    json_candidates = sorted(path.glob("*.json"))
    lang_files = [p for p in json_candidates if p.name not in {"manifest.json"}]
    if len(lang_files) != 1:
        raise ValueError(
            f"Expected exactly one language json file in run dir {path}, found {len(lang_files)}"
        )
    payload = load_json(lang_files[0])
    return lang_files[0], payload


def extract_run_info(payload: dict[str, Any]) -> tuple[str, str | None, str | None, list[dict[str, Any]]]:
    language = payload.get("language")
    profile_name = payload.get("profile_name")
    harness_stage = payload.get("harness_stage")

    parsed = payload.get("parsed_stdout_json")
    if isinstance(parsed, dict) and isinstance(parsed.get("scenarios"), list):
        if parsed.get("language"):
            language = parsed["language"]
        if parsed.get("profile_name"):
            profile_name = parsed["profile_name"]
        if parsed.get("harness_stage"):
            harness_stage = parsed["harness_stage"]
        return language, profile_name, harness_stage, parsed["scenarios"]

    if isinstance(payload.get("scenarios"), list):
        if payload.get("language"):
            language = payload["language"]
        if payload.get("profile_name"):
            profile_name = payload["profile_name"]
        if payload.get("harness_stage"):
            harness_stage = payload["harness_stage"]
        return language, profile_name, harness_stage, payload["scenarios"]

    raise ValueError("Benchmark JSON does not contain scenarios.")


def map_scenarios(scenarios: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    mapped: dict[str, dict[str, Any]] = {}
    for scenario in scenarios:
        scenario_id = scenario.get("scenario_id")
        if not isinstance(scenario_id, str):
            continue
        mapped[scenario_id] = scenario
    return mapped


def load_allowlist(path: Path | None) -> list[dict[str, Any]]:
    if path is None:
        return []
    payload = load_json(path)
    entries = payload.get("entries", [])
    if not isinstance(entries, list):
        raise ValueError("allowlist entries must be a list")
    return [e for e in entries if isinstance(e, dict)]


def allowlist_match(
    entry: dict[str, Any],
    *,
    scenario_id: str,
    language: str,
    profile_name: str | None,
    harness_stage: str | None,
    regression_pct: float,
) -> bool:
    sid = entry.get("scenario_id")
    if sid not in {None, "*", scenario_id}:
        return False

    lang = entry.get("language")
    if lang not in {None, "*", language}:
        return False

    profile = entry.get("profile_name")
    if profile not in {None, "*", profile_name}:
        return False

    stage = entry.get("harness_stage")
    if stage not in {None, "*", harness_stage}:
        return False

    max_regression_pct = entry.get("max_regression_pct")
    if isinstance(max_regression_pct, (int, float)):
        if regression_pct > float(max_regression_pct):
            return False
    return True


@dataclass
class ScenarioDiff:
    scenario_id: str
    name: str
    category: str
    baseline_p95_us: float
    candidate_p95_us: float
    delta_pct: float
    severity: str
    allowlisted: bool
    allowlist_reason: str | None


def severity_for_delta(delta_pct: float, warn: float, fail: float) -> str:
    if delta_pct > fail:
        return "fail"
    if delta_pct > warn:
        return "warn"
    return "ok"


def to_float(value: Any) -> float | None:
    if isinstance(value, (int, float)):
        numeric = float(value)
        if math.isfinite(numeric):
            return numeric
    return None


def compare(
    baseline_scenarios: dict[str, dict[str, Any]],
    candidate_scenarios: dict[str, dict[str, Any]],
    *,
    language: str,
    profile_name: str | None,
    harness_stage: str | None,
    warn_threshold_pct: float,
    fail_threshold_pct: float,
    allowlist_entries: list[dict[str, Any]],
) -> tuple[list[ScenarioDiff], list[str]]:
    diffs: list[ScenarioDiff] = []
    notes: list[str] = []

    missing_in_candidate = sorted(set(baseline_scenarios) - set(candidate_scenarios))
    missing_in_baseline = sorted(set(candidate_scenarios) - set(baseline_scenarios))
    if missing_in_candidate:
        notes.append(f"Missing candidate scenarios: {', '.join(missing_in_candidate)}")
    if missing_in_baseline:
        notes.append(f"New candidate scenarios (not in baseline): {', '.join(missing_in_baseline)}")

    for scenario_id in sorted(set(baseline_scenarios) & set(candidate_scenarios)):
        base = baseline_scenarios[scenario_id]
        cand = candidate_scenarios[scenario_id]

        base_p95 = to_float((base.get("stats") or {}).get("p95_us"))
        cand_p95 = to_float((cand.get("stats") or {}).get("p95_us"))
        if base_p95 is None or cand_p95 is None:
            notes.append(f"Skipped scenario {scenario_id}: missing p95_us")
            continue
        if base_p95 <= 0:
            notes.append(f"Skipped scenario {scenario_id}: non-positive baseline p95_us")
            continue

        delta_pct = ((cand_p95 - base_p95) / base_p95) * 100.0
        severity = severity_for_delta(delta_pct, warn_threshold_pct, fail_threshold_pct)
        allowlisted = False
        allowlist_reason = None
        if severity in {"warn", "fail"}:
            for entry in allowlist_entries:
                if allowlist_match(
                    entry,
                    scenario_id=scenario_id,
                    language=language,
                    profile_name=profile_name,
                    harness_stage=harness_stage,
                    regression_pct=delta_pct,
                ):
                    allowlisted = True
                    allowlist_reason = entry.get("reason")
                    break

        diff = ScenarioDiff(
            scenario_id=scenario_id,
            name=str(cand.get("name") or base.get("name") or scenario_id),
            category=str(cand.get("category") or base.get("category") or "unknown"),
            baseline_p95_us=base_p95,
            candidate_p95_us=cand_p95,
            delta_pct=delta_pct,
            severity=severity,
            allowlisted=allowlisted,
            allowlist_reason=str(allowlist_reason) if allowlist_reason else None,
        )
        diffs.append(diff)

    return diffs, notes


def render_markdown(
    *,
    language: str,
    profile_name: str | None,
    harness_stage: str | None,
    baseline_path: Path,
    candidate_path: Path,
    warn_threshold_pct: float,
    fail_threshold_pct: float,
    diffs: list[ScenarioDiff],
    notes: list[str],
) -> str:
    fail_count = sum(1 for d in diffs if d.severity == "fail" and not d.allowlisted)
    warn_count = sum(1 for d in diffs if d.severity == "warn" and not d.allowlisted)
    allowlisted_count = sum(1 for d in diffs if d.allowlisted)
    ok_count = len(diffs) - fail_count - warn_count - allowlisted_count

    lines = [
        "# Benchmark Baseline Comparison",
        "",
        f"- Language: `{language}`",
        f"- Profile: `{profile_name}`",
        f"- Harness stage: `{harness_stage}`",
        f"- Baseline: `{baseline_path}`",
        f"- Candidate: `{candidate_path}`",
        f"- Warn threshold: `{warn_threshold_pct:.2f}%`",
        f"- Fail threshold: `{fail_threshold_pct:.2f}%`",
        "",
        "## Summary",
        "",
        f"- Compared scenarios: `{len(diffs)}`",
        f"- OK: `{ok_count}`",
        f"- Warnings: `{warn_count}`",
        f"- Fails: `{fail_count}`",
        f"- Allowlisted regressions: `{allowlisted_count}`",
        "",
        "## Scenario Diffs (p95 latency)",
        "",
        "| Scenario | Category | Baseline p95 (us) | Candidate p95 (us) | Delta | Status |",
        "|---|---|---:|---:|---:|---|",
    ]

    for diff in diffs:
        if diff.allowlisted and diff.severity in {"warn", "fail"}:
            status = "allowlisted"
            if diff.allowlist_reason:
                status = f"allowlisted ({diff.allowlist_reason})"
        else:
            status = diff.severity
        lines.append(
            "| "
            f"`{diff.scenario_id}` ({diff.name}) | "
            f"{diff.category} | "
            f"{diff.baseline_p95_us:.3f} | "
            f"{diff.candidate_p95_us:.3f} | "
            f"{diff.delta_pct:+.2f}% | "
            f"{status} |"
        )

    if notes:
        lines.extend(["", "## Notes", ""])
        for note in notes:
            lines.append(f"- {note}")

    return "\n".join(lines) + "\n"


def main() -> int:
    args = parse_args()
    if args.fail_threshold_pct <= args.warn_threshold_pct:
        raise ValueError("--fail-threshold-pct must be greater than --warn-threshold-pct")

    baseline_path, baseline_payload = resolve_run_json(args.baseline)
    candidate_path, candidate_payload = resolve_run_json(args.candidate)

    base_language, base_profile, base_stage, base_scenarios = extract_run_info(baseline_payload)
    cand_language, cand_profile, cand_stage, cand_scenarios = extract_run_info(candidate_payload)

    if base_language != cand_language:
        raise ValueError(
            f"Language mismatch: baseline={base_language!r} candidate={cand_language!r}"
        )
    if base_profile != cand_profile:
        raise ValueError(
            f"Profile mismatch: baseline={base_profile!r} candidate={cand_profile!r}"
        )
    if base_stage != cand_stage:
        raise ValueError(
            f"Harness stage mismatch: baseline={base_stage!r} candidate={cand_stage!r}"
        )

    baseline_map = map_scenarios(base_scenarios)
    candidate_map = map_scenarios(cand_scenarios)
    shared_scenarios = sorted(set(baseline_map) & set(candidate_map))
    if not shared_scenarios:
        raise ValueError(
            "No overlapping scenario IDs between baseline and candidate; refusing comparison."
        )

    allowlist_entries = load_allowlist(Path(args.allowlist) if args.allowlist else None)
    diffs, notes = compare(
        baseline_map,
        candidate_map,
        language=cand_language,
        profile_name=cand_profile,
        harness_stage=cand_stage,
        warn_threshold_pct=args.warn_threshold_pct,
        fail_threshold_pct=args.fail_threshold_pct,
        allowlist_entries=allowlist_entries,
    )
    if not diffs:
        raise ValueError("No comparable scenario diffs produced (missing p95 data or invalid inputs).")

    fail_count = sum(1 for d in diffs if d.severity == "fail" and not d.allowlisted)
    warn_count = sum(1 for d in diffs if d.severity == "warn" and not d.allowlisted)

    report = {
        "schema_version": 1,
        "language": cand_language,
        "profile_name": cand_profile,
        "harness_stage": cand_stage,
        "baseline_path": str(baseline_path),
        "candidate_path": str(candidate_path),
        "warn_threshold_pct": args.warn_threshold_pct,
        "fail_threshold_pct": args.fail_threshold_pct,
        "summary": {
            "compared_scenarios": len(diffs),
            "warning_count": warn_count,
            "failure_count": fail_count,
            "allowlisted_count": sum(1 for d in diffs if d.allowlisted),
        },
        "scenario_diffs": [
            {
                "scenario_id": d.scenario_id,
                "name": d.name,
                "category": d.category,
                "baseline_p95_us": d.baseline_p95_us,
                "candidate_p95_us": d.candidate_p95_us,
                "delta_pct": d.delta_pct,
                "severity": d.severity,
                "allowlisted": d.allowlisted,
                "allowlist_reason": d.allowlist_reason,
            }
            for d in diffs
        ],
        "notes": notes,
        "baseline_profile_name": base_profile,
        "baseline_harness_stage": base_stage,
    }

    markdown = render_markdown(
        language=cand_language,
        profile_name=cand_profile,
        harness_stage=cand_stage,
        baseline_path=baseline_path,
        candidate_path=candidate_path,
        warn_threshold_pct=args.warn_threshold_pct,
        fail_threshold_pct=args.fail_threshold_pct,
        diffs=diffs,
        notes=notes,
    )

    if args.report_json:
        report_json_path = Path(args.report_json)
        report_json_path.parent.mkdir(parents=True, exist_ok=True)
        report_json_path.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8")

    if args.report_md:
        report_md_path = Path(args.report_md)
        report_md_path.parent.mkdir(parents=True, exist_ok=True)
        report_md_path.write_text(markdown, encoding="utf-8")

    print(
        f"compared={report['summary']['compared_scenarios']} "
        f"warnings={warn_count} fails={fail_count} allowlisted={report['summary']['allowlisted_count']}"
    )

    if fail_count > 0:
        return 2
    if warn_count > 0 and args.fail_on_warning:
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
