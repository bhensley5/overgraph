#!/usr/bin/env python3
"""Validate cross-language benchmark parity contract.

Checks that Rust/Node/Python benchmark artifacts share:
- profile + effective config
- percentile method declaration
- comparable scenario params + iteration semantics
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Validate cross-language benchmark parity.")
    parser.add_argument("--rust", required=True, help="Rust run JSON path (run_suite output or harness output)")
    parser.add_argument("--node", required=True, help="Node run JSON path (run_suite output or harness output)")
    parser.add_argument("--python", required=True, help="Python run JSON path (run_suite output or harness output)")
    parser.add_argument("--report-json", default=None, help="Optional output path for JSON report")
    parser.add_argument("--report-md", default=None, help="Optional output path for markdown report")
    return parser.parse_args()


def load_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def extract_harness_payload(payload: dict[str, Any]) -> dict[str, Any]:
    parsed = payload.get("parsed_stdout_json")
    if isinstance(parsed, dict):
        return parsed
    return payload


def json_equal(left: Any, right: Any) -> bool:
    if isinstance(left, bool) or isinstance(right, bool):
        return isinstance(left, bool) and isinstance(right, bool) and left == right

    if isinstance(left, (int, float)) and isinstance(right, (int, float)):
        return float(left) == float(right)

    if isinstance(left, dict) and isinstance(right, dict):
        if set(left.keys()) != set(right.keys()):
            return False
        return all(json_equal(left[key], right[key]) for key in left.keys())

    if isinstance(left, list) and isinstance(right, list):
        if len(left) != len(right):
            return False
        return all(json_equal(a, b) for a, b in zip(left, right))

    return left == right


def scenario_map(payload: dict[str, Any]) -> dict[str, dict[str, Any]]:
    mapped: dict[str, dict[str, Any]] = {}
    scenarios = payload.get("scenarios")
    if not isinstance(scenarios, list):
        return mapped
    for scenario in scenarios:
        if not isinstance(scenario, dict):
            continue
        sid = scenario.get("scenario_id")
        if isinstance(sid, str):
            mapped[sid] = scenario
    return mapped


def scenario_comparability(scenario: dict[str, Any]) -> dict[str, Any]:
    comp = scenario.get("comparability")
    if isinstance(comp, dict):
        status = comp.get("status")
        if isinstance(status, str):
            return {
                "status": status,
                "reason": comp.get("reason"),
            }
    return {
        "status": "non_comparable",
        "reason": "missing comparability metadata",
    }


def stats_have_required_percentiles(scenario: dict[str, Any]) -> bool:
    stats = scenario.get("stats")
    if not isinstance(stats, dict):
        return False
    for key in ("p50_us", "p95_us", "p99_us"):
        value = stats.get(key)
        if not isinstance(value, (int, float)):
            return False
    return True


def validate_parity(payloads: dict[str, dict[str, Any]]) -> dict[str, Any]:
    errors: list[str] = []
    warnings: list[str] = []
    infos: list[str] = []

    languages = ["rust", "node", "python"]

    # Schema version parity.
    schema_versions = {lang: payloads[lang].get("schema_version") for lang in languages}
    for lang in languages:
        if not isinstance(schema_versions[lang], int):
            errors.append(f"{lang}: missing or invalid schema_version")
    if all(isinstance(v, int) for v in schema_versions.values()):
        if len(set(schema_versions.values())) != 1:
            errors.append(f"schema_version mismatch across languages: {schema_versions}")

    # Validate language identity.
    for lang in languages:
        reported = payloads[lang].get("language")
        if reported != lang:
            errors.append(f"{lang}: language field mismatch (found {reported!r})")

    # Profile parity.
    profile_names = {lang: payloads[lang].get("profile_name") for lang in languages}
    if len(set(profile_names.values())) != 1:
        errors.append(f"profile_name mismatch across languages: {profile_names}")
    profile_name = next(iter(profile_names.values())) if profile_names else None

    # Effective config parity.
    effective_configs: dict[str, Any] = {}
    scenario_contract_versions: dict[str, int] = {}
    for lang in languages:
        profile_contract = payloads[lang].get("profile_contract")
        if not isinstance(profile_contract, dict):
            errors.append(f"{lang}: missing profile_contract object")
            continue
        effective = profile_contract.get("effective_config")
        if effective is None:
            errors.append(f"{lang}: missing profile_contract.effective_config")
            continue
        effective_configs[lang] = effective

        contract_version = profile_contract.get("scenario_contract_schema_version")
        if not isinstance(contract_version, int):
            errors.append(f"{lang}: missing or invalid profile_contract.scenario_contract_schema_version")
        else:
            scenario_contract_versions[lang] = contract_version

    if len(effective_configs) == 3:
        base = effective_configs["rust"]
        for lang in ("node", "python"):
            if not json_equal(effective_configs[lang], base):
                errors.append(f"effective_config mismatch: rust vs {lang}")

    if len(scenario_contract_versions) == 3:
        if len(set(scenario_contract_versions.values())) != 1:
            errors.append(
                "scenario_contract_schema_version mismatch across languages: "
                f"{scenario_contract_versions}"
            )

    # Percentile method parity.
    percentile_methods: dict[str, Any] = {}
    for lang in languages:
        method = payloads[lang].get("percentile_method")
        if method is None:
            errors.append(f"{lang}: missing percentile_method")
            continue
        percentile_methods[lang] = method

    if len(percentile_methods) == 3:
        base = percentile_methods["rust"]
        for lang in ("node", "python"):
            if not json_equal(percentile_methods[lang], base):
                errors.append(f"percentile_method mismatch: rust vs {lang}")

    scenario_maps = {lang: scenario_map(payloads[lang]) for lang in languages}
    all_scenario_ids: set[str] = set()
    for scenarios in scenario_maps.values():
        all_scenario_ids.update(scenarios.keys())

    comparable_scenarios: list[str] = []
    non_comparable: dict[str, dict[str, Any]] = {}

    for scenario_id in sorted(all_scenario_ids):
        present_langs = [lang for lang in languages if scenario_id in scenario_maps[lang]]
        comp_by_lang: dict[str, dict[str, Any]] = {}

        for lang in present_langs:
            comp = scenario_comparability(scenario_maps[lang][scenario_id])
            comp_by_lang[lang] = comp
            if comp.get("status") == "non_comparable" and not comp.get("reason"):
                errors.append(f"{scenario_id}: {lang} non_comparable status missing reason")

        statuses = {c.get("status") for c in comp_by_lang.values()}
        if len(statuses) > 1:
            errors.append(
                f"{scenario_id}: comparability status mismatch across present languages: {comp_by_lang}"
            )
            continue

        status = next(iter(statuses)) if statuses else "non_comparable"

        if status == "comparable":
            missing_langs = [lang for lang in languages if lang not in present_langs]
            if missing_langs:
                errors.append(
                    f"{scenario_id}: marked comparable but missing in languages: {', '.join(missing_langs)}"
                )
                continue

            # Params parity
            rust_params = scenario_maps["rust"][scenario_id].get("scenario_params")
            for lang in ("node", "python"):
                params = scenario_maps[lang][scenario_id].get("scenario_params")
                if not json_equal(params, rust_params):
                    errors.append(f"{scenario_id}: scenario_params mismatch rust vs {lang}")

            # Iteration parity
            warmups = {
                lang: scenario_maps[lang][scenario_id].get("warmup_iterations")
                for lang in languages
            }
            iters = {
                lang: scenario_maps[lang][scenario_id].get("benchmark_iterations")
                for lang in languages
            }
            if len(set(warmups.values())) != 1:
                errors.append(f"{scenario_id}: warmup_iterations mismatch {warmups}")
            if len(set(iters.values())) != 1:
                errors.append(f"{scenario_id}: benchmark_iterations mismatch {iters}")

            # Percentiles present.
            for lang in languages:
                if not stats_have_required_percentiles(scenario_maps[lang][scenario_id]):
                    errors.append(f"{scenario_id}: {lang} missing required percentile stats")

            comparable_scenarios.append(scenario_id)
        else:
            reasons = {
                lang: comp_by_lang[lang].get("reason")
                for lang in present_langs
            }
            non_comparable[scenario_id] = {
                "present_languages": present_langs,
                "reasons": reasons,
            }

    if not comparable_scenarios:
        warnings.append("No scenarios marked comparable across all three languages.")

    infos.append(f"profile={profile_name}")
    infos.append(f"comparable_scenarios={len(comparable_scenarios)}")
    infos.append(f"non_comparable_scenarios={len(non_comparable)}")

    return {
        "schema_version": 1,
        "status": "pass" if not errors else "fail",
        "profile_name": profile_name,
        "errors": errors,
        "warnings": warnings,
        "info": infos,
        "comparable_scenarios": comparable_scenarios,
        "non_comparable_scenarios": non_comparable,
    }


def render_markdown(report: dict[str, Any]) -> str:
    lines = [
        "# Cross-Language Benchmark Parity Report",
        "",
        f"- Status: `{report['status']}`",
        f"- Profile: `{report.get('profile_name')}`",
        f"- Comparable scenarios: `{len(report.get('comparable_scenarios', []))}`",
        f"- Non-comparable scenarios: `{len(report.get('non_comparable_scenarios', {}))}`",
        "",
    ]

    errors = report.get("errors", [])
    warnings = report.get("warnings", [])

    lines.append("## Errors")
    lines.append("")
    if errors:
        for error in errors:
            lines.append(f"- {error}")
    else:
        lines.append("- None")

    lines.append("")
    lines.append("## Warnings")
    lines.append("")
    if warnings:
        for warning in warnings:
            lines.append(f"- {warning}")
    else:
        lines.append("- None")

    lines.append("")
    lines.append("## Comparable Scenarios")
    lines.append("")
    comparable = report.get("comparable_scenarios", [])
    if comparable:
        for scenario_id in comparable:
            lines.append(f"- `{scenario_id}`")
    else:
        lines.append("- None")

    non_comparable = report.get("non_comparable_scenarios", {})
    lines.append("")
    lines.append("## Non-Comparable Scenarios")
    lines.append("")
    if non_comparable:
        for scenario_id, detail in non_comparable.items():
            lines.append(
                f"- `{scenario_id}`: present={detail.get('present_languages')} reasons={detail.get('reasons')}"
            )
    else:
        lines.append("- None")

    return "\n".join(lines) + "\n"


def main() -> int:
    args = parse_args()

    payloads = {
        "rust": extract_harness_payload(load_json(Path(args.rust))),
        "node": extract_harness_payload(load_json(Path(args.node))),
        "python": extract_harness_payload(load_json(Path(args.python))),
    }

    report = validate_parity(payloads)

    if args.report_json:
        Path(args.report_json).write_text(json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8")

    if args.report_md:
        Path(args.report_md).write_text(render_markdown(report), encoding="utf-8")

    print(json.dumps(report, indent=2, sort_keys=True))
    return 0 if report["status"] == "pass" else 1


if __name__ == "__main__":
    raise SystemExit(main())
