use crate::error::EngineError;
use crate::schema::{normalize_schema_manifest, validate_schema_manifest};
use crate::segment_writer::SEGMENT_FORMAT_VERSION;
use crate::types::{
    secondary_index_logical_identity, validate_label_token_name, validate_secondary_index_target,
    ManifestState, SecondaryIndexTarget, LABEL_TOKEN_SCHEMA_VERSION, SCHEMA_CATALOG_VERSION,
};
use std::fs;
use std::io::Write;
use std::path::Path;

const MANIFEST_CURRENT: &str = "manifest.current";
const MANIFEST_TMP: &str = "manifest.tmp";
const MANIFEST_PREV: &str = "manifest.prev";

/// Persist a ManifestState atomically:
/// 1. Write to manifest.tmp + fsync file
/// 2. If manifest.current exists, rename to manifest.prev
/// 3. Rename manifest.tmp → manifest.current
/// 4. fsync the directory to make the rename durable
pub(crate) fn write_manifest(db_dir: &Path, state: &ManifestState) -> Result<(), EngineError> {
    let tmp_path = db_dir.join(MANIFEST_TMP);
    let current_path = db_dir.join(MANIFEST_CURRENT);
    let prev_path = db_dir.join(MANIFEST_PREV);

    // 1. Write to tmp + fsync
    let json = serde_json::to_string_pretty(state)
        .map_err(|e| EngineError::ManifestError(format!("serialize: {}", e)))?;
    let mut file = fs::File::create(&tmp_path)?;
    file.write_all(json.as_bytes())?;
    file.sync_all()?;
    drop(file);

    // 2. If current exists, rename to prev (for rollback safety)
    if current_path.exists() {
        if prev_path.exists() {
            fs::remove_file(&prev_path)?;
        }
        fs::rename(&current_path, &prev_path)?;
    }

    // 3. Atomic rename tmp → current
    fs::rename(&tmp_path, &current_path)?;

    // 4. fsync the directory to make the rename durable
    fsync_dir(db_dir)?;

    Ok(())
}

/// Load a ManifestState from disk. Recovery priority:
/// 1. manifest.current (normal path)
/// 2. manifest.tmp (crash between rename steps; tmp has the newest state)
/// 3. manifest.prev (fallback if current is corrupt)
pub(crate) fn load_manifest(db_dir: &Path) -> Result<Option<ManifestState>, EngineError> {
    let current_path = db_dir.join(MANIFEST_CURRENT);
    let tmp_path = db_dir.join(MANIFEST_TMP);
    let prev_path = db_dir.join(MANIFEST_PREV);

    // Try current
    if let Some((state, dirty)) = try_load_manifest_file_for_write(&current_path)? {
        if dirty {
            write_manifest(db_dir, &state)?;
        }
        return Ok(Some(state));
    }

    // Try tmp (crash between step 2 and step 3 in write_manifest)
    if let Some((state, dirty)) = try_load_manifest_file_for_write(&tmp_path)? {
        // Promote tmp to current
        if dirty {
            write_manifest(db_dir, &state)?;
        } else {
            fs::rename(&tmp_path, &current_path)?;
            fsync_dir(db_dir)?;
        }
        return Ok(Some(state));
    }

    // Fall back to prev
    if let Some((state, _dirty)) = try_load_manifest_file_for_write(&prev_path)? {
        // Promote prev to current for consistency
        write_manifest(db_dir, &state)?;
        return Ok(Some(state));
    }

    Ok(None)
}

fn try_load_manifest_file(path: &Path) -> Result<Option<ManifestState>, EngineError> {
    Ok(try_load_manifest_file_for_write(path)?.map(|(state, _dirty)| state))
}

fn try_load_manifest_file_for_write(
    path: &Path,
) -> Result<Option<(ManifestState, bool)>, EngineError> {
    if !path.exists() {
        return Ok(None);
    }

    let content = fs::read_to_string(path)?;
    match serde_json::from_str::<ManifestState>(&content) {
        Ok(mut state) => {
            let dirty = normalize_schema_manifest(&mut state)?;
            validate_manifest_identity(&state)?;
            Ok(Some((state, dirty)))
        }
        Err(e) => {
            eprintln!("warning: corrupt manifest at {}: {}", path.display(), e);
            Ok(None)
        }
    }
}

fn validate_manifest_identity(state: &ManifestState) -> Result<(), EngineError> {
    validate_label_token_manifest(state)?;
    validate_schema_manifest(state)?;
    validate_secondary_index_manifest(state)?;
    for segment in &state.segments {
        if segment.segment_format_version != SEGMENT_FORMAT_VERSION
            || segment.segment_data_id == [0; 32]
        {
            return Err(EngineError::ManifestError(format!(
                "unsupported segment manifest entry for segment {}; rebuild the database",
                segment.id
            )));
        }
    }
    Ok(())
}

fn validate_secondary_index_manifest(state: &ManifestState) -> Result<(), EngineError> {
    let mut seen_ids = std::collections::HashSet::new();
    let mut seen_identities = std::collections::HashSet::new();
    for entry in &state.secondary_indexes {
        if !seen_ids.insert(entry.index_id) {
            return Err(EngineError::ManifestError(format!(
                "duplicate secondary index id {} in manifest",
                entry.index_id
            )));
        }
        validate_secondary_index_target(&entry.target)?;
        match &entry.target {
            SecondaryIndexTarget::NodeProperty { label_id, .. }
            | SecondaryIndexTarget::NodeFieldIndex { label_id, .. } => {
                if !state.node_label_tokens.values().any(|id| id == label_id) {
                    return Err(EngineError::ManifestError(format!(
                        "secondary index {} references missing node label label_id {}",
                        entry.index_id, label_id
                    )));
                }
            }
            SecondaryIndexTarget::EdgeProperty { label_id, .. }
            | SecondaryIndexTarget::EdgeFieldIndex { label_id, .. } => {
                if !state.edge_label_tokens.values().any(|id| id == label_id) {
                    return Err(EngineError::ManifestError(format!(
                        "secondary index {} references missing edge-label label_id {}",
                        entry.index_id, label_id
                    )));
                }
            }
        }
        if !seen_identities.insert(secondary_index_logical_identity(entry)?) {
            return Err(EngineError::ManifestError(format!(
                "duplicate secondary index declaration for {:?}",
                entry.target
            )));
        }
    }
    Ok(())
}

fn validate_label_token_manifest(state: &ManifestState) -> Result<(), EngineError> {
    if state.label_token_schema_version == 0 {
        return Err(EngineError::ManifestError(
            "database manifest is missing label token schema; recreate the database".to_string(),
        ));
    }
    if state.label_token_schema_version != LABEL_TOKEN_SCHEMA_VERSION {
        return Err(EngineError::ManifestError(format!(
            "unsupported label token schema version: expected {}, got {}",
            LABEL_TOKEN_SCHEMA_VERSION, state.label_token_schema_version
        )));
    }
    validate_token_namespace(
        "node label",
        &state.node_label_tokens,
        state.next_node_label_id,
    )?;
    validate_token_namespace(
        "edge label",
        &state.edge_label_tokens,
        state.next_edge_label_id,
    )?;
    Ok(())
}

fn validate_token_namespace(
    namespace: &str,
    tokens: &std::collections::BTreeMap<String, u32>,
    next_id: u32,
) -> Result<(), EngineError> {
    let mut ids = std::collections::BTreeMap::new();
    let mut max_id = 0u32;
    for (name, &label_id) in tokens {
        if let Err(error) = validate_label_token_name(name) {
            return Err(EngineError::ManifestError(format!(
                "{namespace} token name '{name}' is invalid: {error}"
            )));
        }
        if label_id == 0 {
            return Err(EngineError::ManifestError(format!(
                "{namespace} token '{name}' uses reserved label_id 0"
            )));
        }
        if let Some(existing_name) = ids.insert(label_id, name) {
            return Err(EngineError::ManifestError(format!(
                "{namespace} token conflict: label_id {label_id} is assigned to both '{existing_name}' and '{name}'"
            )));
        }
        max_id = max_id.max(label_id);
    }
    if next_id <= max_id {
        return Err(EngineError::ManifestError(format!(
            "{namespace} next token id {next_id} must be greater than max assigned id {max_id}"
        )));
    }
    Ok(())
}

/// fsync a directory to make rename operations durable.
/// No-op on Windows. NTFS doesn't support directory fsync via File::open().
fn fsync_dir(dir: &Path) -> Result<(), EngineError> {
    #[cfg(not(target_os = "windows"))]
    {
        let d = fs::File::open(dir)?;
        d.sync_all()?;
    }
    #[cfg(target_os = "windows")]
    let _ = dir;
    Ok(())
}

/// Diagnostic read-only manifest load.
///
/// This uses the same priority chain as `load_manifest` but never writes to
/// disk, so diagnostic tooling can inspect a live or crashed database without
/// side effects. The returned `ManifestState` is a raw manifest view and may
/// include internal numeric token IDs; ordinary graph APIs use names instead.
pub fn load_manifest_readonly(db_dir: &Path) -> Result<Option<ManifestState>, EngineError> {
    let current_path = db_dir.join(MANIFEST_CURRENT);
    let tmp_path = db_dir.join(MANIFEST_TMP);
    let prev_path = db_dir.join(MANIFEST_PREV);

    if let Some(state) = try_load_manifest_file(&current_path)? {
        return Ok(Some(state));
    }
    if let Some(state) = try_load_manifest_file(&tmp_path)? {
        return Ok(Some(state));
    }
    if let Some(state) = try_load_manifest_file(&prev_path)? {
        return Ok(Some(state));
    }
    Ok(None)
}

/// Create a fresh default manifest state.
pub(crate) fn default_manifest() -> ManifestState {
    ManifestState {
        version: 1,
        label_token_schema_version: LABEL_TOKEN_SCHEMA_VERSION,
        node_label_tokens: std::collections::BTreeMap::new(),
        edge_label_tokens: std::collections::BTreeMap::new(),
        next_node_label_id: 1,
        next_edge_label_id: 1,
        segments: Vec::new(),
        next_node_id: 1,
        next_edge_id: 1,
        dense_vector: None,
        prune_policies: std::collections::BTreeMap::new(),
        next_engine_seq: 0,
        next_wal_generation_id: 0,
        active_wal_generation_id: 0,
        pending_flush_epochs: Vec::new(),
        secondary_indexes: Vec::new(),
        next_secondary_index_id: 1,
        schema_catalog_version: SCHEMA_CATALOG_VERSION,
        next_schema_id: 1,
        node_schemas: Vec::new(),
        edge_schemas: Vec::new(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{
        DenseVectorSchemaManifestRule, EdgeSchemaManifestEntry, EdgeValiditySchemaManifestRule,
        EndpointLabelManifestRule, NodeLabelConstraintManifestRule, NodeSchemaManifestEntry,
        PropValue, PropertySchemaManifestRule, SchemaAdditionalPropertiesManifest,
        SchemaNumericBoundManifest, SchemaValueTypeManifest, SchemaVectorPresenceManifest,
        SegmentInfo,
    };
    use std::collections::BTreeMap;
    use tempfile::TempDir;

    fn schema_manifest_with_labels() -> ManifestState {
        let mut state = default_manifest();
        state.node_label_tokens.insert("Person".to_string(), 1);
        state.node_label_tokens.insert("Company".to_string(), 2);
        state.next_node_label_id = 3;
        state.edge_label_tokens.insert("KNOWS".to_string(), 1);
        state.next_edge_label_id = 2;
        state
    }

    fn node_schema_entry(schema_id: u64, label_id: u32) -> NodeSchemaManifestEntry {
        NodeSchemaManifestEntry {
            schema_id,
            revision: 1,
            label_id,
            created_at_ms: 100,
            updated_at_ms: 100,
            additional_properties: SchemaAdditionalPropertiesManifest::Allow,
            properties: BTreeMap::new(),
            key: None,
            label_constraints: None,
            weight: None,
            dense_vector: None,
            sparse_vector: None,
        }
    }

    fn edge_schema_entry(schema_id: u64, label_id: u32) -> EdgeSchemaManifestEntry {
        EdgeSchemaManifestEntry {
            schema_id,
            revision: 1,
            label_id,
            created_at_ms: 100,
            updated_at_ms: 100,
            additional_properties: SchemaAdditionalPropertiesManifest::Allow,
            properties: BTreeMap::new(),
            from: None,
            to: None,
            allow_self_loops: true,
            weight: None,
            validity: None,
        }
    }

    fn load_manifest_error_for_state(state: ManifestState) -> String {
        let dir = TempDir::new().unwrap();
        let json = serde_json::to_string_pretty(&state).unwrap();
        fs::write(dir.path().join(MANIFEST_CURRENT), json).unwrap();
        load_manifest(dir.path()).unwrap_err().to_string()
    }

    #[test]
    fn test_write_and_load_manifest() {
        let dir = TempDir::new().unwrap();
        let state = ManifestState {
            version: 1,
            segments: vec![SegmentInfo {
                id: 1,
                node_count: 100,
                edge_count: 200,
                segment_format_version: 10,
                segment_data_id: [1; 32],
            }],
            next_node_id: 101,
            next_edge_id: 201,
            ..default_manifest()
        };

        write_manifest(dir.path(), &state).unwrap();
        let loaded = load_manifest(dir.path()).unwrap().unwrap();

        assert_eq!(loaded.version, 1);
        assert_eq!(loaded.segments.len(), 1);
        assert_eq!(loaded.segments[0].id, 1);
        assert_eq!(loaded.next_node_id, 101);
        assert_eq!(loaded.next_edge_id, 201);
    }

    #[test]
    fn test_load_nonexistent_manifest() {
        let dir = TempDir::new().unwrap();
        let result = load_manifest(dir.path()).unwrap();
        assert!(result.is_none());
    }

    #[test]
    fn test_manifest_preserves_prev() {
        let dir = TempDir::new().unwrap();

        let state1 = ManifestState {
            next_node_id: 10,
            next_edge_id: 20,
            ..default_manifest()
        };
        write_manifest(dir.path(), &state1).unwrap();

        let state2 = ManifestState {
            next_node_id: 50,
            next_edge_id: 100,
            ..default_manifest()
        };
        write_manifest(dir.path(), &state2).unwrap();

        let loaded = load_manifest(dir.path()).unwrap().unwrap();
        assert_eq!(loaded.next_node_id, 50);
        assert!(dir.path().join(MANIFEST_PREV).exists());
    }

    #[test]
    fn test_manifest_fallback_to_prev() {
        let dir = TempDir::new().unwrap();

        let state = ManifestState {
            next_node_id: 42,
            next_edge_id: 84,
            ..default_manifest()
        };
        write_manifest(dir.path(), &state).unwrap();

        let state2 = ManifestState {
            next_node_id: 99,
            next_edge_id: 199,
            ..default_manifest()
        };
        write_manifest(dir.path(), &state2).unwrap();

        // Corrupt current
        let current_path = dir.path().join(MANIFEST_CURRENT);
        fs::write(&current_path, "NOT VALID JSON {{{").unwrap();

        let loaded = load_manifest(dir.path()).unwrap().unwrap();
        assert_eq!(loaded.next_node_id, 42);
    }

    #[test]
    fn test_manifest_recovery_from_tmp() {
        let dir = TempDir::new().unwrap();

        // Simulate crash: only manifest.tmp exists (crash between rename steps)
        let state = ManifestState {
            next_node_id: 77,
            next_edge_id: 88,
            ..default_manifest()
        };
        let json = serde_json::to_string_pretty(&state).unwrap();
        let tmp_path = dir.path().join(MANIFEST_TMP);
        fs::write(&tmp_path, &json).unwrap();

        let loaded = load_manifest(dir.path()).unwrap().unwrap();
        assert_eq!(loaded.next_node_id, 77);

        // tmp should be promoted to current
        assert!(dir.path().join(MANIFEST_CURRENT).exists());
        assert!(!dir.path().join(MANIFEST_TMP).exists());
    }

    #[test]
    fn test_default_manifest() {
        let m = default_manifest();
        assert_eq!(m.version, 1);
        assert_eq!(m.label_token_schema_version, LABEL_TOKEN_SCHEMA_VERSION);
        assert!(m.node_label_tokens.is_empty());
        assert!(m.edge_label_tokens.is_empty());
        assert_eq!(m.next_node_label_id, 1);
        assert_eq!(m.next_edge_label_id, 1);
        assert!(m.segments.is_empty());
        assert_eq!(m.next_node_id, 1);
        assert_eq!(m.next_edge_id, 1);
        assert!(m.dense_vector.is_none());
        assert!(m.prune_policies.is_empty());
        assert!(m.secondary_indexes.is_empty());
        assert_eq!(m.next_secondary_index_id, 1);
        assert_eq!(m.schema_catalog_version, SCHEMA_CATALOG_VERSION);
        assert_eq!(m.next_schema_id, 1);
        assert!(m.node_schemas.is_empty());
        assert!(m.edge_schemas.is_empty());
    }

    #[test]
    fn test_load_manifest_missing_dense_vector_defaults_to_none() {
        let dir = TempDir::new().unwrap();
        let legacy_json = r#"{
  "version": 1,
  "label_token_schema_version": 1,
  "node_label_tokens": {},
  "edge_label_tokens": {},
  "next_node_label_id": 1,
  "next_edge_label_id": 1,
  "segments": [],
  "next_node_id": 10,
  "next_edge_id": 20,
  "prune_policies": {}
}"#;
        fs::write(dir.path().join(MANIFEST_CURRENT), legacy_json).unwrap();

        let loaded = load_manifest(dir.path()).unwrap().unwrap();
        assert_eq!(loaded.next_node_id, 10);
        assert!(loaded.dense_vector.is_none());
    }

    #[test]
    fn test_load_manifest_missing_schema_fields_normalizes_and_writes() {
        let dir = TempDir::new().unwrap();
        let legacy_json = r#"{
  "version": 1,
  "label_token_schema_version": 1,
  "node_label_tokens": {},
  "edge_label_tokens": {},
  "next_node_label_id": 1,
  "next_edge_label_id": 1,
  "segments": [],
  "next_node_id": 10,
  "next_edge_id": 20,
  "prune_policies": {},
  "next_engine_seq": 0,
  "next_wal_generation_id": 0,
  "active_wal_generation_id": 0,
  "pending_flush_epochs": [],
  "secondary_indexes": [],
  "next_secondary_index_id": 1
}"#;
        fs::write(dir.path().join(MANIFEST_CURRENT), legacy_json).unwrap();

        let loaded = load_manifest(dir.path()).unwrap().unwrap();
        assert_eq!(loaded.schema_catalog_version, SCHEMA_CATALOG_VERSION);
        assert_eq!(loaded.next_schema_id, 1);
        assert!(loaded.node_schemas.is_empty());
        assert!(loaded.edge_schemas.is_empty());

        let raw_manifest = fs::read_to_string(dir.path().join(MANIFEST_CURRENT)).unwrap();
        assert!(raw_manifest.contains("\"schema_catalog_version\": 1"));
        assert!(raw_manifest.contains("\"next_schema_id\": 1"));
        assert!(raw_manifest.contains("\"node_schemas\": []"));
        assert!(raw_manifest.contains("\"edge_schemas\": []"));
    }

    #[test]
    fn test_load_manifest_readonly_missing_schema_fields_does_not_write() {
        let dir = TempDir::new().unwrap();
        let legacy_json = r#"{
  "version": 1,
  "label_token_schema_version": 1,
  "node_label_tokens": {},
  "edge_label_tokens": {},
  "next_node_label_id": 1,
  "next_edge_label_id": 1,
  "segments": [],
  "next_node_id": 10,
  "next_edge_id": 20,
  "prune_policies": {},
  "next_engine_seq": 0,
  "next_wal_generation_id": 0,
  "active_wal_generation_id": 0,
  "pending_flush_epochs": [],
  "secondary_indexes": [],
  "next_secondary_index_id": 1
}"#;
        fs::write(dir.path().join(MANIFEST_CURRENT), legacy_json).unwrap();

        let loaded = load_manifest_readonly(dir.path()).unwrap().unwrap();
        assert_eq!(loaded.schema_catalog_version, SCHEMA_CATALOG_VERSION);
        assert_eq!(loaded.next_schema_id, 1);
        assert!(loaded.node_schemas.is_empty());
        assert!(loaded.edge_schemas.is_empty());

        let raw_manifest = fs::read_to_string(dir.path().join(MANIFEST_CURRENT)).unwrap();
        assert!(!raw_manifest.contains("schema_catalog_version"));
        assert!(!raw_manifest.contains("next_schema_id"));
    }

    #[test]
    fn test_load_manifest_readonly_does_not_write() {
        use super::load_manifest_readonly;

        let dir = TempDir::new().unwrap();

        // Simulate crash: only manifest.tmp exists
        let state = ManifestState {
            next_node_id: 77,
            next_edge_id: 88,
            ..default_manifest()
        };
        let json = serde_json::to_string_pretty(&state).unwrap();
        let tmp_path = dir.path().join(MANIFEST_TMP);
        fs::write(&tmp_path, &json).unwrap();

        // Read-only load should find it via tmp
        let loaded = load_manifest_readonly(dir.path()).unwrap().unwrap();
        assert_eq!(loaded.next_node_id, 77);

        // But should NOT have promoted tmp to current
        assert!(dir.path().join(MANIFEST_TMP).exists());
        assert!(!dir.path().join(MANIFEST_CURRENT).exists());
    }

    #[test]
    fn test_manifest_atomic_no_partial_write() {
        let dir = TempDir::new().unwrap();
        let state = ManifestState {
            next_node_id: 5,
            next_edge_id: 10,
            ..default_manifest()
        };
        write_manifest(dir.path(), &state).unwrap();

        assert!(!dir.path().join(MANIFEST_TMP).exists());
        assert!(dir.path().join(MANIFEST_CURRENT).exists());
    }

    #[test]
    fn test_load_manifest_missing_secondary_index_fields_defaults_cleanly() {
        let dir = TempDir::new().unwrap();
        let legacy_json = r#"{
  "version": 1,
  "label_token_schema_version": 1,
  "node_label_tokens": {},
  "edge_label_tokens": {},
  "next_node_label_id": 1,
  "next_edge_label_id": 1,
  "segments": [],
  "next_node_id": 10,
  "next_edge_id": 20,
  "prune_policies": {},
  "next_engine_seq": 0,
  "next_wal_generation_id": 0,
  "active_wal_generation_id": 0,
  "pending_flush_epochs": []
}"#;
        fs::write(dir.path().join(MANIFEST_CURRENT), legacy_json).unwrap();

        let loaded = load_manifest(dir.path()).unwrap().unwrap();
        assert!(loaded.secondary_indexes.is_empty());
        assert_eq!(loaded.next_secondary_index_id, 0);
    }

    #[test]
    fn test_load_manifest_missing_label_token_schema_rejected() {
        let dir = TempDir::new().unwrap();
        let legacy_json = r#"{
  "version": 1,
  "segments": [],
  "next_node_id": 10,
  "next_edge_id": 20,
  "prune_policies": {}
}"#;
        fs::write(dir.path().join(MANIFEST_CURRENT), legacy_json).unwrap();

        let err = load_manifest(dir.path()).unwrap_err();
        assert!(err.to_string().contains("missing label token schema"));
    }

    #[test]
    fn test_load_manifest_rejects_label_token_reverse_conflict() {
        let dir = TempDir::new().unwrap();
        let mut state = default_manifest();
        state.node_label_tokens.insert("Person".to_string(), 1);
        state.node_label_tokens.insert("Company".to_string(), 1);
        state.next_node_label_id = 2;
        write_manifest(dir.path(), &state).unwrap();

        let current_path = dir.path().join(MANIFEST_CURRENT);
        let err = try_load_manifest_file(&current_path).unwrap_err();
        assert!(err.to_string().contains("token conflict"));
    }

    #[test]
    fn test_load_manifest_rejects_label_token_next_id_not_above_max() {
        let dir = TempDir::new().unwrap();
        let mut state = default_manifest();
        state.edge_label_tokens.insert("KNOWS".to_string(), 3);
        state.next_edge_label_id = 3;
        write_manifest(dir.path(), &state).unwrap();

        let current_path = dir.path().join(MANIFEST_CURRENT);
        let err = try_load_manifest_file(&current_path).unwrap_err();
        assert!(err
            .to_string()
            .contains("must be greater than max assigned"));
    }

    #[test]
    fn test_load_manifest_rejects_invalid_label_token_names() {
        let dir = TempDir::new().unwrap();
        let mut state = default_manifest();
        state.node_label_tokens.insert(" Person".to_string(), 1);
        state.next_node_label_id = 2;
        write_manifest(dir.path(), &state).unwrap();

        let current_path = dir.path().join(MANIFEST_CURRENT);
        let err = try_load_manifest_file(&current_path).unwrap_err();
        assert!(err.to_string().contains("token name"));
        assert!(err.to_string().contains("invalid"));
    }

    #[test]
    fn test_manifest_round_trip_node_and_edge_schemas_use_numeric_ids() {
        let dir = TempDir::new().unwrap();
        let mut node_properties = BTreeMap::new();
        node_properties.insert(
            "age".to_string(),
            PropertySchemaManifestRule {
                required: true,
                nullable: false,
                types: vec![SchemaValueTypeManifest::UInt],
                numeric_min: Some(SchemaNumericBoundManifest {
                    value: PropValue::UInt(0),
                    inclusive: true,
                }),
                numeric_max: None,
                string_min_bytes: None,
                string_max_bytes: None,
                bytes_min_len: None,
                bytes_max_len: None,
                array_min_items: None,
                array_max_items: None,
                map_min_entries: None,
                map_max_entries: None,
                enum_values: Vec::new(),
            },
        );
        let mut state = schema_manifest_with_labels();
        state.next_schema_id = 3;
        state.node_schemas = vec![NodeSchemaManifestEntry {
            schema_id: 1,
            revision: 1,
            label_id: 1,
            created_at_ms: 100,
            updated_at_ms: 100,
            additional_properties: SchemaAdditionalPropertiesManifest::Reject,
            properties: node_properties,
            key: None,
            label_constraints: Some(NodeLabelConstraintManifestRule {
                all_of: vec![2],
                any_of: Vec::new(),
                none_of: Vec::new(),
            }),
            weight: None,
            dense_vector: Some(DenseVectorSchemaManifestRule {
                presence: SchemaVectorPresenceManifest::Required,
                dimension: Some(384),
            }),
            sparse_vector: None,
        }];
        state.edge_schemas = vec![EdgeSchemaManifestEntry {
            schema_id: 2,
            revision: 1,
            label_id: 1,
            created_at_ms: 100,
            updated_at_ms: 100,
            additional_properties: SchemaAdditionalPropertiesManifest::Allow,
            properties: BTreeMap::new(),
            from: Some(EndpointLabelManifestRule {
                all_of: vec![1],
                any_of: Vec::new(),
                none_of: Vec::new(),
            }),
            to: Some(EndpointLabelManifestRule {
                all_of: Vec::new(),
                any_of: vec![2],
                none_of: Vec::new(),
            }),
            allow_self_loops: false,
            weight: None,
            validity: Some(EdgeValiditySchemaManifestRule {
                require_valid_from_before_valid_to: true,
                valid_from_min: Some(0),
                valid_from_max: None,
                valid_to_min: None,
                valid_to_max: None,
                allow_open_ended_valid_to: false,
            }),
        }];
        write_manifest(dir.path(), &state).unwrap();

        let raw_manifest = fs::read_to_string(dir.path().join(MANIFEST_CURRENT)).unwrap();
        assert!(raw_manifest.contains("\"label_id\""));
        assert!(raw_manifest.contains("\"Reject\""));
        assert!(raw_manifest.contains("\"UInt\""));
        assert!(raw_manifest.contains("\"Required\""));
        assert!(!raw_manifest.contains("\"label\""));

        let loaded = load_manifest(dir.path()).unwrap().unwrap();
        assert_eq!(loaded.next_schema_id, 3);
        assert_eq!(loaded.node_schemas.len(), 1);
        assert_eq!(loaded.edge_schemas.len(), 1);
        assert_eq!(loaded.node_schemas[0].label_id, 1);
        assert_eq!(loaded.edge_schemas[0].label_id, 1);
        assert_eq!(
            loaded.node_schemas[0].additional_properties,
            SchemaAdditionalPropertiesManifest::Reject
        );
        assert_eq!(
            loaded.node_schemas[0]
                .dense_vector
                .as_ref()
                .unwrap()
                .presence,
            SchemaVectorPresenceManifest::Required
        );
        assert!(!loaded.edge_schemas[0].allow_self_loops);
    }

    #[test]
    fn test_load_manifest_rejects_schema_entries_with_version_zero() {
        let mut state = schema_manifest_with_labels();
        state.schema_catalog_version = 0;
        state.node_schemas.push(node_schema_entry(1, 1));

        let error = load_manifest_error_for_state(state);
        assert!(error.contains("schema_catalog_version 0"));
    }

    #[test]
    fn test_load_manifest_rejects_unsupported_schema_version() {
        let mut state = schema_manifest_with_labels();
        state.schema_catalog_version = 99;

        let error = load_manifest_error_for_state(state);
        assert!(error.contains("unsupported schema catalog version 99"));
    }

    #[test]
    fn test_load_manifest_rejects_duplicate_node_schema_targets() {
        let mut state = schema_manifest_with_labels();
        state.node_schemas.push(node_schema_entry(1, 1));
        state.node_schemas.push(node_schema_entry(2, 1));
        state.next_schema_id = 3;

        let error = load_manifest_error_for_state(state);
        assert!(error.contains("duplicate node schema target label_id 1"));
    }

    #[test]
    fn test_load_manifest_rejects_duplicate_edge_schema_targets() {
        let mut state = schema_manifest_with_labels();
        state.edge_schemas.push(edge_schema_entry(1, 1));
        state.edge_schemas.push(edge_schema_entry(2, 1));
        state.next_schema_id = 3;

        let error = load_manifest_error_for_state(state);
        assert!(error.contains("duplicate edge schema target label_id 1"));
    }

    #[test]
    fn test_load_manifest_rejects_duplicate_schema_ids() {
        let mut state = schema_manifest_with_labels();
        state.node_schemas.push(node_schema_entry(1, 1));
        state.edge_schemas.push(edge_schema_entry(1, 1));
        state.next_schema_id = 2;

        let error = load_manifest_error_for_state(state);
        assert!(error.contains("duplicate schema id 1"));
    }

    #[test]
    fn test_load_manifest_rejects_missing_node_schema_target_label() {
        let mut state = schema_manifest_with_labels();
        state.node_schemas.push(node_schema_entry(1, 99));
        state.next_schema_id = 2;

        let error = load_manifest_error_for_state(state);
        assert!(error.contains("node schema target label_id 99"));
        assert!(error.contains("node_label_tokens"));
    }

    #[test]
    fn test_load_manifest_rejects_missing_edge_schema_target_label() {
        let mut state = schema_manifest_with_labels();
        state.edge_schemas.push(edge_schema_entry(1, 99));
        state.next_schema_id = 2;

        let error = load_manifest_error_for_state(state);
        assert!(error.contains("edge schema target label_id 99"));
        assert!(error.contains("edge_label_tokens"));
    }

    #[test]
    fn test_load_manifest_rejects_missing_node_label_constraint_reference() {
        let mut state = schema_manifest_with_labels();
        let mut entry = node_schema_entry(1, 1);
        entry.label_constraints = Some(NodeLabelConstraintManifestRule {
            all_of: vec![99],
            any_of: Vec::new(),
            none_of: Vec::new(),
        });
        state.node_schemas.push(entry);
        state.next_schema_id = 2;

        let error = load_manifest_error_for_state(state);
        assert!(error.contains("references missing node label_id 99"));
    }

    #[test]
    fn test_load_manifest_rejects_missing_endpoint_label_reference() {
        let mut state = schema_manifest_with_labels();
        let mut entry = edge_schema_entry(1, 1);
        entry.from = Some(EndpointLabelManifestRule {
            all_of: vec![99],
            any_of: Vec::new(),
            none_of: Vec::new(),
        });
        state.edge_schemas.push(entry);
        state.next_schema_id = 2;

        let error = load_manifest_error_for_state(state);
        assert!(error.contains("references missing node label_id 99"));
    }

    #[test]
    fn test_load_manifest_rejects_reserved_label_ids_in_schema() {
        let mut state = schema_manifest_with_labels();
        state.node_schemas.push(node_schema_entry(1, 0));
        state.next_schema_id = 2;

        let error = load_manifest_error_for_state(state);
        assert!(error.contains("reserved label_id 0"));

        let mut state = schema_manifest_with_labels();
        let mut entry = edge_schema_entry(1, 1);
        entry.to = Some(EndpointLabelManifestRule {
            all_of: vec![0],
            any_of: Vec::new(),
            none_of: Vec::new(),
        });
        state.edge_schemas.push(entry);
        state.next_schema_id = 2;
        let error = load_manifest_error_for_state(state);
        assert!(error.contains("reserved label_id 0"));
    }

    #[test]
    fn test_load_manifest_normalizes_schema_counter_and_label_constraints() {
        let dir = TempDir::new().unwrap();
        let mut state = schema_manifest_with_labels();
        state.next_schema_id = 0;
        let mut node = node_schema_entry(5, 1);
        node.label_constraints = Some(NodeLabelConstraintManifestRule {
            all_of: vec![2, 1, 2],
            any_of: Vec::new(),
            none_of: Vec::new(),
        });
        let mut edge = edge_schema_entry(6, 1);
        edge.from = Some(EndpointLabelManifestRule {
            all_of: Vec::new(),
            any_of: Vec::new(),
            none_of: Vec::new(),
        });
        state.node_schemas.push(node);
        state.edge_schemas.push(edge);
        fs::write(
            dir.path().join(MANIFEST_CURRENT),
            serde_json::to_string_pretty(&state).unwrap(),
        )
        .unwrap();

        let loaded = load_manifest(dir.path()).unwrap().unwrap();
        assert_eq!(loaded.next_schema_id, 7);
        assert_eq!(
            loaded.node_schemas[0]
                .label_constraints
                .as_ref()
                .unwrap()
                .all_of,
            vec![1, 2]
        );
        assert!(loaded.edge_schemas[0].from.is_none());
    }

    #[test]
    fn test_load_manifest_rejects_impossible_schema_shapes() {
        let mut state = schema_manifest_with_labels();
        let mut properties = BTreeMap::new();
        properties.insert(
            "score".to_string(),
            PropertySchemaManifestRule {
                required: false,
                nullable: true,
                types: Vec::new(),
                numeric_min: Some(SchemaNumericBoundManifest {
                    value: PropValue::Int(10),
                    inclusive: true,
                }),
                numeric_max: Some(SchemaNumericBoundManifest {
                    value: PropValue::Int(10),
                    inclusive: false,
                }),
                string_min_bytes: Some(5),
                string_max_bytes: Some(3),
                bytes_min_len: None,
                bytes_max_len: None,
                array_min_items: None,
                array_max_items: None,
                map_min_entries: None,
                map_max_entries: None,
                enum_values: Vec::new(),
            },
        );
        let mut entry = node_schema_entry(1, 1);
        entry.properties = properties;
        state.node_schemas.push(entry);
        state.next_schema_id = 2;

        let error = load_manifest_error_for_state(state);
        assert!(error.contains("numeric bounds define an empty range"));
    }

    #[test]
    fn test_manifest_schema_serde_defaults_are_locked() {
        let dir = TempDir::new().unwrap();
        let json = r#"{
  "version": 1,
  "label_token_schema_version": 1,
  "node_label_tokens": { "Person": 1 },
  "edge_label_tokens": { "KNOWS": 1 },
  "next_node_label_id": 2,
  "next_edge_label_id": 2,
  "segments": [],
  "next_node_id": 1,
  "next_edge_id": 1,
  "prune_policies": {},
  "next_engine_seq": 0,
  "next_wal_generation_id": 0,
  "active_wal_generation_id": 0,
  "pending_flush_epochs": [],
  "secondary_indexes": [],
  "next_secondary_index_id": 1,
  "schema_catalog_version": 1,
  "next_schema_id": 3,
  "node_schemas": [
    {
      "schema_id": 1,
      "revision": 1,
      "label_id": 1,
      "created_at_ms": 100,
      "updated_at_ms": 100,
      "properties": {
        "score": {
          "numeric_min": { "value": { "Int": 1 } }
        }
      },
      "dense_vector": {}
    }
  ],
  "edge_schemas": [
    {
      "schema_id": 2,
      "revision": 1,
      "label_id": 1,
      "created_at_ms": 100,
      "updated_at_ms": 100,
      "weight": {},
      "validity": {}
    }
  ]
}"#;
        fs::write(dir.path().join(MANIFEST_CURRENT), json).unwrap();

        let loaded = load_manifest(dir.path()).unwrap().unwrap();
        let node = &loaded.node_schemas[0];
        assert_eq!(
            node.additional_properties,
            SchemaAdditionalPropertiesManifest::Allow
        );
        let score = node.properties.get("score").unwrap();
        assert!(score.nullable);
        assert!(score.numeric_min.as_ref().unwrap().inclusive);
        assert_eq!(
            node.dense_vector.as_ref().unwrap().presence,
            SchemaVectorPresenceManifest::Optional
        );
        let edge = &loaded.edge_schemas[0];
        assert!(edge.allow_self_loops);
        assert!(edge.weight.as_ref().unwrap().finite);
        assert!(edge.validity.as_ref().unwrap().allow_open_ended_valid_to);
    }

    #[test]
    fn test_manifest_round_trip_node_and_edge_secondary_indexes() {
        let dir = TempDir::new().unwrap();
        let state = ManifestState {
            node_label_tokens: BTreeMap::from([("Person".to_string(), 1)]),
            next_node_label_id: 2,
            edge_label_tokens: BTreeMap::from([
                ("KNOWS".to_string(), 1),
                ("WORKS_AT".to_string(), 2),
            ]),
            next_edge_label_id: 3,
            next_secondary_index_id: 5,
            secondary_indexes: vec![
                crate::types::SecondaryIndexManifestEntry {
                    index_id: 0,
                    target: crate::types::SecondaryIndexTarget::NodeProperty {
                        label_id: 1,
                        prop_key: "color".to_string(),
                    },
                    kind: crate::types::SecondaryIndexKind::Equality,
                    state: crate::types::SecondaryIndexState::Building,
                    last_error: None,
                },
                crate::types::SecondaryIndexManifestEntry {
                    index_id: 1,
                    target: crate::types::SecondaryIndexTarget::EdgeProperty {
                        label_id: 2,
                        prop_key: "weight".to_string(),
                    },
                    kind: crate::types::SecondaryIndexKind::Range,
                    state: crate::types::SecondaryIndexState::Building,
                    last_error: None,
                },
                crate::types::SecondaryIndexManifestEntry {
                    index_id: 2,
                    target: crate::types::SecondaryIndexTarget::EdgeProperty {
                        label_id: 1,
                        prop_key: "label".to_string(),
                    },
                    kind: crate::types::SecondaryIndexKind::Equality,
                    state: crate::types::SecondaryIndexState::Ready,
                    last_error: None,
                },
                crate::types::SecondaryIndexManifestEntry {
                    index_id: 3,
                    target: crate::types::SecondaryIndexTarget::NodeFieldIndex {
                        label_id: 1,
                        fields: vec![
                            crate::types::SecondaryIndexFieldManifest::Property {
                                key: "status".to_string(),
                            },
                            crate::types::SecondaryIndexFieldManifest::NodeMetadata {
                                field: crate::types::NodeMetadataIndexFieldManifest::UpdatedAt,
                            },
                        ],
                    },
                    kind: crate::types::SecondaryIndexKind::Equality,
                    state: crate::types::SecondaryIndexState::Building,
                    last_error: None,
                },
            ],
            ..default_manifest()
        };
        write_manifest(dir.path(), &state).unwrap();

        let raw_manifest = fs::read_to_string(dir.path().join(MANIFEST_CURRENT)).unwrap();
        assert!(raw_manifest.contains("\"NodeProperty\""));
        assert!(raw_manifest.contains("\"EdgeProperty\""));
        assert!(raw_manifest.contains("\"NodeFieldIndex\""));
        assert!(raw_manifest.contains("\"label_id\""));
        assert!(!raw_manifest.contains(concat!("\"", "type", "_id\"")));

        let loaded = load_manifest(dir.path()).unwrap().unwrap();
        assert_eq!(loaded.secondary_indexes.len(), 4);
        assert_eq!(loaded.next_secondary_index_id, 5);
        assert!(matches!(
            &loaded.secondary_indexes[0].target,
            crate::types::SecondaryIndexTarget::NodeProperty { label_id: 1, .. }
        ));
        assert!(matches!(
            &loaded.secondary_indexes[1].target,
            crate::types::SecondaryIndexTarget::EdgeProperty { label_id: 2, .. }
        ));
        assert_eq!(loaded.secondary_indexes[1].index_id, 1);
        assert!(matches!(
            loaded.secondary_indexes[1].kind,
            crate::types::SecondaryIndexKind::Range
        ));
        assert!(matches!(
            &loaded.secondary_indexes[2].target,
            crate::types::SecondaryIndexTarget::EdgeProperty { label_id: 1, .. }
        ));
        assert_eq!(
            loaded.secondary_indexes[2].state,
            crate::types::SecondaryIndexState::Ready
        );
        assert!(matches!(
            &loaded.secondary_indexes[3].target,
            crate::types::SecondaryIndexTarget::NodeFieldIndex { label_id: 1, fields }
                if fields.len() == 2
        ));
    }

    #[test]
    fn test_load_manifest_rejects_duplicate_secondary_index_logical_identity() {
        let dir = TempDir::new().unwrap();
        let state = ManifestState {
            node_label_tokens: BTreeMap::from([("Person".to_string(), 1)]),
            next_node_label_id: 2,
            next_secondary_index_id: 3,
            secondary_indexes: vec![
                crate::types::SecondaryIndexManifestEntry {
                    index_id: 1,
                    target: crate::types::SecondaryIndexTarget::NodeProperty {
                        label_id: 1,
                        prop_key: "status".to_string(),
                    },
                    kind: crate::types::SecondaryIndexKind::Equality,
                    state: crate::types::SecondaryIndexState::Building,
                    last_error: None,
                },
                crate::types::SecondaryIndexManifestEntry {
                    index_id: 2,
                    target: crate::types::SecondaryIndexTarget::NodeFieldIndex {
                        label_id: 1,
                        fields: vec![crate::types::SecondaryIndexFieldManifest::Property {
                            key: "status".to_string(),
                        }],
                    },
                    kind: crate::types::SecondaryIndexKind::Equality,
                    state: crate::types::SecondaryIndexState::Building,
                    last_error: None,
                },
            ],
            ..default_manifest()
        };
        fs::write(
            dir.path().join(MANIFEST_CURRENT),
            serde_json::to_string_pretty(&state).unwrap(),
        )
        .unwrap();

        let err = load_manifest(dir.path()).expect_err("duplicate logical identity must fail");
        assert!(
            err.to_string()
                .contains("duplicate secondary index declaration"),
            "{err}"
        );
    }

    #[test]
    fn test_load_manifest_rejects_legacy_domainful_range_index_kind() {
        let dir = TempDir::new().unwrap();
        let state = ManifestState {
            node_label_tokens: BTreeMap::from([("Person".to_string(), 1)]),
            next_node_label_id: 2,
            next_secondary_index_id: 2,
            secondary_indexes: vec![crate::types::SecondaryIndexManifestEntry {
                index_id: 1,
                target: crate::types::SecondaryIndexTarget::NodeProperty {
                    label_id: 1,
                    prop_key: "score".to_string(),
                },
                kind: crate::types::SecondaryIndexKind::Range,
                state: crate::types::SecondaryIndexState::Ready,
                last_error: None,
            }],
            ..default_manifest()
        };
        let json = serde_json::to_string_pretty(&state).unwrap();
        let legacy_json = json.replace(
            "\"kind\": \"Range\"",
            "\"kind\": { \"Range\": { \"domain\": \"Int\" } }",
        );
        assert_ne!(json, legacy_json);
        fs::write(dir.path().join(MANIFEST_CURRENT), legacy_json).unwrap();

        assert!(try_load_manifest_file(&dir.path().join(MANIFEST_CURRENT))
            .unwrap()
            .is_none());
    }
}
