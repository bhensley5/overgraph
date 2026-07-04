use overgraph::{
    ComponentScrubFinding, DatabaseScrubFinding, DatabaseScrubFindingType, DatabaseScrubReport,
    DatabaseScrubSeverity, ManifestScrubSource, ManifestScrubSummary, OrphanSegmentScrubResult,
    ScrubPathOptions, SegmentScrubResult, WalScrubResult, WalScrubRole,
};
use serde_json::json;
use std::env;
use std::path::{Path, PathBuf};
use std::process;

const USAGE: &str =
    "Usage: overgraph-scrub [--json] [--include-orphan-segments] [--no-wal] [--no-stability-check] <db-path>";

#[derive(Debug)]
struct CliArgs {
    json_mode: bool,
    db_path: PathBuf,
    options: ScrubPathOptions,
}

fn main() {
    let args: Vec<String> = env::args().skip(1).collect();
    let exit_code = match run(args) {
        Ok(code) => code,
        Err(message) => {
            eprintln!("{message}");
            2
        }
    };
    process::exit(exit_code);
}

fn run(args: Vec<String>) -> Result<i32, String> {
    let args = parse_args(args)?;
    let report = overgraph::scrub_path_with_options(&args.db_path, &args.options)
        .map_err(|error| format!("Error: {error}"))?;
    let has_errors = report_has_errors(&report);

    if args.json_mode {
        print_json_report(&report)?;
    } else {
        print_text_report(&args.db_path, &report);
    }

    Ok(if has_errors { 1 } else { 0 })
}

fn parse_args(args: Vec<String>) -> Result<CliArgs, String> {
    let mut json_mode = false;
    let mut db_path = None;
    let mut options = ScrubPathOptions::default();

    for arg in args {
        match arg.as_str() {
            "--json" => json_mode = true,
            "--include-orphan-segments" => options.include_orphan_segments = true,
            "--no-wal" => options.validate_wal = false,
            "--no-stability-check" => options.check_manifest_stability = false,
            _ if arg.starts_with('-') => {
                return Err(format!("Unknown argument: {arg}\n{USAGE}"));
            }
            _ => {
                if db_path.is_some() {
                    return Err(format!("Duplicate database path: {arg}\n{USAGE}"));
                }
                db_path = Some(PathBuf::from(arg));
            }
        }
    }

    let db_path = db_path.ok_or_else(|| USAGE.to_string())?;
    Ok(CliArgs {
        json_mode,
        db_path,
        options,
    })
}

fn report_has_errors(report: &DatabaseScrubReport) -> bool {
    report
        .findings
        .iter()
        .any(|finding| finding.severity == DatabaseScrubSeverity::Error)
        || report.total_components_failed > 0
}

fn report_has_non_error_findings(report: &DatabaseScrubReport) -> bool {
    report
        .findings
        .iter()
        .any(|finding| finding.severity != DatabaseScrubSeverity::Error)
}

fn report_status(report: &DatabaseScrubReport) -> &'static str {
    if report_has_errors(report) {
        "errors found"
    } else if report_has_non_error_findings(report) {
        "warnings found"
    } else {
        "clean"
    }
}

fn print_json_report(report: &DatabaseScrubReport) -> Result<(), String> {
    let output = database_report_to_json(report);
    let encoded = serde_json::to_string_pretty(&output)
        .map_err(|error| format!("Error: failed to serialize scrub report: {error}"))?;
    println!("{encoded}");
    Ok(())
}

fn print_text_report(db_path: &Path, report: &DatabaseScrubReport) {
    println!("OverGraph Scrub: {}", db_path.display());
    println!("Status: {}", report_status(report));
    println!("Duration: {} ms", report.duration_ms);
    println!();

    println!("Manifest");
    match &report.manifest {
        Some(manifest) => {
            println!(
                "  Source:                 {}",
                manifest_source_name(manifest.source)
            );
            println!("  Segments:               {}", manifest.segment_count);
            println!("  Pending flush epochs:   {}", manifest.pending_flush_count);
            println!(
                "  Active WAL generation:  {}",
                manifest.active_wal_generation_id
            );
            println!(
                "  Next WAL generation:    {}",
                manifest.next_wal_generation_id
            );
        }
        None => {
            println!("  No valid manifest selected");
        }
    }
    println!();

    println!("Segments");
    println!("  Published segments:     {}", report.segments.len());
    println!(
        "  Components checked:     {}",
        report.total_components_checked
    );
    println!("  Components ok:          {}", report.total_components_ok);
    println!(
        "  Components failed:      {}",
        report.total_components_failed
    );
    println!("  Bytes digested:         {}", report.total_bytes_digested);
    for segment in &report.segments {
        println!(
            "  seg_{:04}: ok={}, failed={}, bytes={}",
            segment.segment_id,
            segment.components_ok,
            failed_component_count(&segment.findings),
            segment.bytes_digested
        );
    }
    println!();

    println!("WAL");
    println!("  Generations checked:    {}", report.wal_generations.len());
    println!(
        "  Records checked:        {}",
        report.total_wal_records_checked
    );
    println!(
        "  Bytes checked:          {}",
        report.total_wal_bytes_checked
    );
    for wal in &report.wal_generations {
        println!(
            "  wal_{}.wal [{}]: file={}, durable={}, trailing={}, records={}, findings={}",
            wal.generation_id,
            wal_role_name(wal.role),
            wal.file_len,
            wal.durable_len,
            wal.trailing_bytes,
            wal.records_checked,
            wal.findings.len()
        );
    }
    println!();

    println!("Orphans");
    let orphan_segment_findings = report
        .findings
        .iter()
        .filter(|finding| finding.finding_type == DatabaseScrubFindingType::OrphanSegment)
        .count();
    let orphan_wal_findings = report
        .findings
        .iter()
        .filter(|finding| finding.finding_type == DatabaseScrubFindingType::OrphanWal)
        .count();
    println!("  Orphan segment findings: {}", orphan_segment_findings);
    println!("  Orphan WAL findings:     {}", orphan_wal_findings);
    println!(
        "  Orphan segments scrubbed: {}",
        report.orphan_segments.len()
    );
    println!();

    if report.findings.is_empty()
        && report
            .segments
            .iter()
            .all(|segment| segment.findings.is_empty())
        && report
            .orphan_segments
            .iter()
            .all(|orphan| orphan.findings.is_empty())
    {
        println!("Findings: none");
        return;
    }

    println!("Findings");
    for finding in &report.findings {
        println!(
            "  [{}] {} {}: {}",
            severity_name(finding.severity),
            finding.component_kind,
            database_finding_type_name(finding.finding_type),
            finding.detail
        );
    }
    for segment in &report.segments {
        for finding in &segment.findings {
            println!(
                "  [component] seg_{:04} {} {}: {}",
                segment.segment_id,
                finding.component_kind,
                component_finding_type_name(finding),
                finding.detail
            );
        }
    }
    for orphan in &report.orphan_segments {
        for finding in &orphan.findings {
            println!(
                "  [orphan component] {} {} {}: {}",
                orphan.path,
                finding.component_kind,
                component_finding_type_name(finding),
                finding.detail
            );
        }
    }
}

fn failed_component_count(findings: &[ComponentScrubFinding]) -> usize {
    let mut component_kinds = Vec::new();
    for finding in findings {
        if !component_kinds.contains(&finding.component_kind) {
            component_kinds.push(finding.component_kind.clone());
        }
    }
    component_kinds.len()
}

fn database_report_to_json(report: &DatabaseScrubReport) -> serde_json::Value {
    json!({
        "manifest": report.manifest.as_ref().map(manifest_summary_to_json),
        "findings": report.findings.iter().map(database_finding_to_json).collect::<Vec<_>>(),
        "segments": report.segments.iter().map(segment_result_to_json).collect::<Vec<_>>(),
        "walGenerations": report.wal_generations.iter().map(wal_result_to_json).collect::<Vec<_>>(),
        "orphanSegments": report.orphan_segments.iter().map(orphan_segment_result_to_json).collect::<Vec<_>>(),
        "totalComponentsChecked": report.total_components_checked,
        "totalComponentsOk": report.total_components_ok,
        "totalComponentsFailed": report.total_components_failed,
        "totalBytesDigested": report.total_bytes_digested,
        "totalWalRecordsChecked": report.total_wal_records_checked,
        "totalWalBytesChecked": report.total_wal_bytes_checked,
        "durationMs": report.duration_ms,
    })
}

fn manifest_summary_to_json(summary: &ManifestScrubSummary) -> serde_json::Value {
    json!({
        "source": manifest_source_name(summary.source),
        "segmentCount": summary.segment_count,
        "pendingFlushCount": summary.pending_flush_count,
        "activeWalGenerationId": summary.active_wal_generation_id,
        "nextWalGenerationId": summary.next_wal_generation_id,
    })
}

fn segment_result_to_json(segment: &SegmentScrubResult) -> serde_json::Value {
    json!({
        "segmentId": segment.segment_id,
        "findings": segment.findings.iter().map(component_finding_to_json).collect::<Vec<_>>(),
        "componentsOk": segment.components_ok,
        "bytesDigested": segment.bytes_digested,
    })
}

fn component_finding_to_json(finding: &ComponentScrubFinding) -> serde_json::Value {
    json!({
        "componentKind": finding.component_kind,
        "findingType": component_finding_type_name(finding),
        "detail": finding.detail,
    })
}

fn database_finding_to_json(finding: &DatabaseScrubFinding) -> serde_json::Value {
    json!({
        "componentKind": finding.component_kind,
        "findingType": database_finding_type_name(finding.finding_type),
        "severity": severity_name(finding.severity),
        "detail": finding.detail,
    })
}

fn wal_result_to_json(wal: &WalScrubResult) -> serde_json::Value {
    json!({
        "generationId": wal.generation_id,
        "role": wal_role_name(wal.role),
        "epochId": wal.epoch_id,
        "segmentId": wal.segment_id,
        "fileLen": wal.file_len,
        "durableLen": wal.durable_len,
        "trailingBytes": wal.trailing_bytes,
        "recordsChecked": wal.records_checked,
        "bytesChecked": wal.bytes_checked,
        "findings": wal.findings.iter().map(database_finding_to_json).collect::<Vec<_>>(),
    })
}

fn orphan_segment_result_to_json(orphan: &OrphanSegmentScrubResult) -> serde_json::Value {
    json!({
        "segmentId": orphan.segment_id,
        "path": orphan.path,
        "findings": orphan.findings.iter().map(component_finding_to_json).collect::<Vec<_>>(),
        "componentsOk": orphan.components_ok,
        "bytesDigested": orphan.bytes_digested,
        "semanticChecksSkipped": orphan.semantic_checks_skipped,
    })
}

fn manifest_source_name(source: ManifestScrubSource) -> &'static str {
    match source {
        ManifestScrubSource::Current => "Current",
        ManifestScrubSource::Tmp => "Tmp",
        ManifestScrubSource::Prev => "Prev",
    }
}

fn database_finding_type_name(finding_type: DatabaseScrubFindingType) -> &'static str {
    match finding_type {
        DatabaseScrubFindingType::ManifestMissing => "ManifestMissing",
        DatabaseScrubFindingType::ManifestFileInvalid => "ManifestFileInvalid",
        DatabaseScrubFindingType::ManifestFallbackUsed => "ManifestFallbackUsed",
        DatabaseScrubFindingType::ManifestChangedDuringScrub => "ManifestChangedDuringScrub",
        DatabaseScrubFindingType::WalMissing => "WalMissing",
        DatabaseScrubFindingType::WalCorrupt => "WalCorrupt",
        DatabaseScrubFindingType::WalTrailingBytes => "WalTrailingBytes",
        DatabaseScrubFindingType::OrphanSegment => "OrphanSegment",
        DatabaseScrubFindingType::OrphanWal => "OrphanWal",
        DatabaseScrubFindingType::OrphanSegmentUnpinned => "OrphanSegmentUnpinned",
        DatabaseScrubFindingType::OrphanSegmentScrubSkipped => "OrphanSegmentScrubSkipped",
    }
}

fn severity_name(severity: DatabaseScrubSeverity) -> &'static str {
    match severity {
        DatabaseScrubSeverity::Info => "info",
        DatabaseScrubSeverity::Warning => "warning",
        DatabaseScrubSeverity::Error => "error",
    }
}

fn wal_role_name(role: WalScrubRole) -> &'static str {
    match role {
        WalScrubRole::Active => "Active",
        WalScrubRole::FrozenPendingFlush => "FrozenPendingFlush",
        WalScrubRole::PublishedPendingRetire => "PublishedPendingRetire",
    }
}

fn component_finding_type_name(finding: &ComponentScrubFinding) -> String {
    format!("{:?}", finding.finding_type)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn empty_report() -> DatabaseScrubReport {
        DatabaseScrubReport {
            manifest: None,
            findings: Vec::new(),
            segments: Vec::new(),
            wal_generations: Vec::new(),
            orphan_segments: Vec::new(),
            total_components_checked: 0,
            total_components_ok: 0,
            total_components_failed: 0,
            total_bytes_digested: 0,
            total_wal_records_checked: 0,
            total_wal_bytes_checked: 0,
            duration_ms: 0,
        }
    }

    #[test]
    fn report_has_errors_ignores_warning_only_findings() {
        let mut report = empty_report();
        report.findings.push(DatabaseScrubFinding {
            component_kind: "wal".to_string(),
            finding_type: DatabaseScrubFindingType::OrphanWal,
            severity: DatabaseScrubSeverity::Warning,
            detail: "orphan".to_string(),
        });

        assert!(!report_has_errors(&report));
    }

    #[test]
    fn report_has_errors_detects_error_severity_findings() {
        let mut report = empty_report();
        report.findings.push(DatabaseScrubFinding {
            component_kind: "manifest".to_string(),
            finding_type: DatabaseScrubFindingType::ManifestMissing,
            severity: DatabaseScrubSeverity::Error,
            detail: "missing".to_string(),
        });

        assert!(report_has_errors(&report));
    }

    #[test]
    fn report_has_errors_detects_component_failures() {
        let mut report = empty_report();
        report.total_components_failed = 1;

        assert!(report_has_errors(&report));
    }

    #[test]
    fn parse_args_accepts_flags_before_and_after_path() {
        let parsed = parse_args(vec![
            "--json".to_string(),
            "db".to_string(),
            "--include-orphan-segments".to_string(),
            "--no-wal".to_string(),
            "--no-stability-check".to_string(),
        ])
        .unwrap();

        assert!(parsed.json_mode);
        assert_eq!(parsed.db_path, PathBuf::from("db"));
        assert!(!parsed.options.validate_wal);
        assert!(parsed.options.include_orphan_segments);
        assert!(!parsed.options.check_manifest_stability);
    }
}
