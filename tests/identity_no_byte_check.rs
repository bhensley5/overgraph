use std::fs;
use std::path::{Path, PathBuf};

const PROHIBITED_PRODUCTION_SYMBOLS: &[&str] = &[
    "ComponentChecksumVerifier",
    "CheckedComponentRange",
    "ComponentBlockChecksumSpec",
    "COMPONENT_CHECKSUM_BLOCK_SIZE",
    "checked_slice",
    "checked_all",
    "block_checksum",
    "checksum_table",
    "verify_block",
    "OGCHK",
    "read_path_counters",
];

const PROHIBITED_STANDALONE_CORE_FILENAMES: &[&str] = &[
    "nodes.dat",
    "edges.dat",
    "node_meta.dat",
    "edge_meta.dat",
    "tombstones.dat",
    "key_index.dat",
    "node_label_index.dat",
    "edge_label_index.dat",
    "edge_triple_index.dat",
    "adj_out.idx",
    "adj_out.dat",
    "adj_in.idx",
    "adj_in.dat",
    "timestamp_index.dat",
    "node_vector_meta.dat",
    "node_dense_vectors.dat",
    "node_sparse_vectors.dat",
    "edge_weight_index.dat",
    "edge_updated_at_index.dat",
    "edge_valid_from_index.dat",
    "edge_valid_to_index.dat",
];

#[test]
fn production_sources_do_not_contain_component_byte_checking() {
    let failures = scan_production_sources_for(PROHIBITED_PRODUCTION_SYMBOLS);

    assert!(
        failures.is_empty(),
        "prohibited checked-read/component checksum symbols found in production sources:\n{}",
        failures.join("\n")
    );
}

#[test]
fn production_sources_do_not_create_standalone_core_component_files() {
    let failures = scan_production_sources_for(PROHIBITED_STANDALONE_CORE_FILENAMES);

    assert!(
        failures.is_empty(),
        "prohibited standalone packed-core component filenames found in production sources:\n{}",
        failures.join("\n")
    );
}

fn scan_production_sources_for(needles: &[&str]) -> Vec<String> {
    let repo = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let src = repo.join("src");
    let mut failures = Vec::new();
    scan_dir(&src, needles, &mut failures);
    failures
}

fn scan_dir(dir: &Path, needles: &[&str], failures: &mut Vec<String>) {
    for entry in fs::read_dir(dir).expect("read source directory") {
        let entry = entry.expect("read source entry");
        let path = entry.path();
        if path.is_dir() {
            if path.ends_with(Path::new("src/engine/tests")) {
                continue;
            }
            scan_dir(&path, needles, failures);
            continue;
        }

        if path.extension().and_then(|ext| ext.to_str()) == Some("rs") {
            scan_file(&path, needles, failures);
        }
    }
}

fn scan_file(path: &Path, needles: &[&str], failures: &mut Vec<String>) {
    let content = fs::read_to_string(path).expect("read source file");
    for (line_no, line) in production_lines(&content).enumerate() {
        for needle in needles {
            if line.contains(needle) {
                failures.push(format!(
                    "{}:{} contains `{}`",
                    path.display(),
                    line_no + 1,
                    needle
                ));
            }
        }
    }
}

#[test]
fn scrub_not_called_from_normal_open_read_query_paths() {
    let repo = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let src = repo.join("src");
    let scrub_call_sites = &["scrub_database", "crate::scrub::"];

    let mut failures = Vec::new();
    scan_dir_for_scrub(&src, scrub_call_sites, &mut failures);

    assert!(
        failures.is_empty(),
        "scrub functions must not be called from normal engine paths:\n{}",
        failures.join("\n")
    );
}

fn scan_dir_for_scrub(dir: &Path, needles: &[&str], failures: &mut Vec<String>) {
    for entry in fs::read_dir(dir).expect("read source directory") {
        let entry = entry.expect("read source entry");
        let path = entry.path();
        if path.is_dir() {
            scan_dir_for_scrub(&path, needles, failures);
            continue;
        }

        if path.extension().and_then(|ext| ext.to_str()) != Some("rs") {
            continue;
        }

        let path_str = path.to_string_lossy();
        if path_str.ends_with("scrub.rs") {
            continue;
        }

        let content = fs::read_to_string(&path).expect("read source file");
        let in_scrub_method = std::cell::Cell::new(false);
        for (line_no, line) in production_lines(&content).enumerate() {
            let trimmed = line.trim();
            if trimmed.starts_with("pub fn scrub(") {
                in_scrub_method.set(true);
            } else if in_scrub_method.get() && trimmed == "}" {
                in_scrub_method.set(false);
                continue;
            }
            if in_scrub_method.get() {
                continue;
            }
            for needle in needles {
                if line.contains(needle) {
                    failures.push(format!(
                        "{}:{} contains `{}`",
                        path.display(),
                        line_no + 1,
                        needle,
                    ));
                }
            }
        }
    }
}

fn production_lines(content: &str) -> impl Iterator<Item = &str> {
    let mut in_test_tail = false;
    content.lines().filter(move |line| {
        if line.trim_start().starts_with("#[cfg(test)]") {
            in_test_tail = true;
        }
        !in_test_tail
    })
}
