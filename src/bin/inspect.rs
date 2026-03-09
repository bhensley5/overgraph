use overgraph::manifest::load_manifest_readonly;
use serde_json::json;
use std::path::{Path, PathBuf};
use std::{env, fs, process};

fn main() {
    let args: Vec<String> = env::args().collect();

    let mut json_mode = false;
    let mut db_path_arg: Option<&str> = None;

    for arg in &args[1..] {
        match arg.as_str() {
            "--json" => json_mode = true,
            _ if db_path_arg.is_none() => db_path_arg = Some(arg),
            _ => {
                eprintln!("Usage: overgraph-inspect [--json] <db-path>");
                process::exit(1);
            }
        }
    }

    let db_path = match db_path_arg {
        Some(p) => PathBuf::from(p),
        None => {
            eprintln!("Usage: overgraph-inspect [--json] <db-path>");
            process::exit(1);
        }
    };

    if !db_path.is_dir() {
        eprintln!("Error: '{}' is not a directory", db_path.display());
        process::exit(1);
    }

    let result = if json_mode {
        inspect_json(&db_path)
    } else {
        inspect_text(&db_path)
    };

    if let Err(e) = result {
        eprintln!("Error: {}", e);
        process::exit(1);
    }
}

fn inspect_json(db_path: &Path) -> Result<(), Box<dyn std::error::Error>> {
    let manifest = load_manifest_readonly(db_path)?;

    let wal_bytes = wal_size(db_path);

    let output = match manifest {
        Some(m) => {
            let segments: Vec<serde_json::Value> = m
                .segments
                .iter()
                .map(|seg| {
                    let seg_name = format!("seg_{:04}", seg.id);
                    let size = dir_size(&db_path.join("segments").join(&seg_name));
                    json!({
                        "id": seg.id,
                        "node_count": seg.node_count,
                        "edge_count": seg.edge_count,
                        "size_bytes": size,
                    })
                })
                .collect();

            let policies: serde_json::Map<String, serde_json::Value> = m
                .prune_policies
                .iter()
                .map(|(name, policy)| {
                    (
                        name.clone(),
                        json!({
                            "max_age_ms": policy.max_age_ms,
                            "max_weight": policy.max_weight,
                            "type_id": policy.type_id,
                        }),
                    )
                })
                .collect();

            json!({
                "path": db_path.display().to_string(),
                "manifest_version": m.version,
                "next_node_id": m.next_node_id,
                "next_edge_id": m.next_edge_id,
                "segment_count": m.segments.len(),
                "total_nodes": m.segments.iter().map(|s| s.node_count).sum::<u64>(),
                "total_edges": m.segments.iter().map(|s| s.edge_count).sum::<u64>(),
                "segments": segments,
                "wal_bytes": wal_bytes,
                "prune_policies": policies,
            })
        }
        None => {
            json!({
                "path": db_path.display().to_string(),
                "initialized": false,
                "wal_bytes": wal_bytes,
            })
        }
    };

    println!("{}", serde_json::to_string_pretty(&output)?);
    Ok(())
}

fn inspect_text(db_path: &Path) -> Result<(), Box<dyn std::error::Error>> {
    println!("OverGraph Database: {}", db_path.display());
    println!();

    // Read-only load. Never writes to the database directory
    let manifest = load_manifest_readonly(db_path)?;
    let manifest = match manifest {
        Some(m) => m,
        None => {
            println!("  No manifest found. Database may be empty or uninitialized.");
            print_wal_info(db_path);
            return Ok(());
        }
    };

    // Manifest info
    println!("Manifest");
    println!("  Version:       {}", manifest.version);
    println!("  Next node ID:  {}", manifest.next_node_id);
    println!("  Next edge ID:  {}", manifest.next_edge_id);
    println!();

    // Segments
    let total_nodes: u64 = manifest.segments.iter().map(|s| s.node_count).sum();
    let total_edges: u64 = manifest.segments.iter().map(|s| s.edge_count).sum();

    println!("Segments: {}", manifest.segments.len());
    if !manifest.segments.is_empty() {
        println!("  {:>6}  {:>8}  {:>8}", "ID", "Nodes", "Edges");
        for seg in &manifest.segments {
            println!(
                "  {:>6}  {:>8}  {:>8}",
                seg.id, seg.node_count, seg.edge_count
            );
        }
        println!("  {:>6}  {:>8}  {:>8}", "Total", total_nodes, total_edges);
    }
    println!();

    // Segment directory sizes
    if !manifest.segments.is_empty() {
        println!("Segment Sizes");
        let mut total_size = 0u64;
        for seg in &manifest.segments {
            let seg_name = format!("seg_{:04}", seg.id);
            let seg_dir = db_path.join("segments").join(&seg_name);
            if seg_dir.is_dir() {
                let size = dir_size(&seg_dir);
                total_size += size;
                println!("  {}: {}", seg_name, format_bytes(size));
            } else {
                println!("  {}: <missing>", seg_name);
            }
        }
        if manifest.segments.len() > 1 {
            println!("  Total: {}", format_bytes(total_size));
        }
        println!();
    }

    // WAL
    print_wal_info(db_path);

    // Prune policies
    if !manifest.prune_policies.is_empty() {
        println!("Prune Policies: {}", manifest.prune_policies.len());
        for (name, policy) in &manifest.prune_policies {
            let mut criteria = Vec::new();
            if let Some(age) = policy.max_age_ms {
                criteria.push(format!("max_age={}ms", age));
            }
            if let Some(w) = policy.max_weight {
                criteria.push(format!("max_weight={}", w));
            }
            if let Some(t) = policy.type_id {
                criteria.push(format!("type_id={}", t));
            }
            println!("  {}: {}", name, criteria.join(", "));
        }
        println!();
    }

    Ok(())
}

fn print_wal_info(db_path: &Path) {
    let size = wal_size(db_path);
    match size {
        Some(bytes) => println!("WAL: data.wal ({})", format_bytes(bytes)),
        None => println!("WAL: not present"),
    }
    println!();
}

fn wal_size(db_path: &Path) -> Option<u64> {
    fs::metadata(db_path.join("data.wal")).ok().map(|m| m.len())
}

// Segment directories are flat (no subdirectories). Only sum top-level files.
fn dir_size(path: &Path) -> u64 {
    let mut total = 0u64;
    if let Ok(entries) = fs::read_dir(path) {
        for entry in entries.flatten() {
            if let Ok(meta) = entry.metadata() {
                if meta.is_file() {
                    total += meta.len();
                }
            }
        }
    }
    total
}

fn format_bytes(bytes: u64) -> String {
    const KB: u64 = 1024;
    const MB: u64 = 1024 * KB;
    const GB: u64 = 1024 * MB;

    if bytes >= GB {
        format!("{:.2} GB", bytes as f64 / GB as f64)
    } else if bytes >= MB {
        format!("{:.2} MB", bytes as f64 / MB as f64)
    } else if bytes >= KB {
        format!("{:.2} KB", bytes as f64 / KB as f64)
    } else {
        format!("{} B", bytes)
    }
}
