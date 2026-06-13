use crate::error::EngineError;
use crate::property_value_semantics::{
    compare_numeric_keys, compare_numeric_prop_values, numeric_key,
    numeric_range_sort_key_for_value, semantic_property_eq, NumericScalarKey,
};
use crate::types::{
    validate_dense_vector, validate_label_token_name, DenseVectorConfig,
    DenseVectorSchemaManifestRule, EdgeRecord, EdgeSchemaManifestEntry,
    EdgeValiditySchemaManifestRule, EndpointLabelManifestRule, ManifestState,
    NodeLabelConstraintManifestRule, NodeLabelSet, NodeRecord, NodeSchemaManifestEntry,
    NumericFieldSchemaManifestRule, PropValue, PropertySchemaManifestRule,
    SchemaAdditionalPropertiesManifest, SchemaNumericBoundManifest, SchemaValueTypeManifest,
    SchemaVectorPresenceManifest, SparseVectorSchemaManifestRule, StringFieldSchemaManifestRule,
    MAX_NODE_LABELS_PER_NODE, SCHEMA_CATALOG_VERSION,
};
use serde::{Deserialize, Serialize};
use std::cmp::Ordering;
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::sync::Arc;

pub(crate) const MAX_SCHEMA_PROPERTIES_PER_LABEL: usize = 1024;
pub(crate) const MAX_SCHEMA_PROPERTY_KEY_BYTES: usize = 1024;
pub(crate) const MAX_SCHEMA_ENUM_VALUES_PER_PROPERTY: usize = 1024;
pub(crate) const MAX_SCHEMA_ENUM_LITERAL_BYTES_PER_PROPERTY: usize = 64 * 1024;
pub(crate) const MAX_SCHEMA_REFERENCED_LABELS_PER_RULE: usize = 64;
pub(crate) const MAX_SCHEMA_MANIFEST_BYTES_PER_ENTRY: usize = 1024 * 1024;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
pub enum SchemaAdditionalProperties {
    #[default]
    Allow,
    Reject,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum SchemaValueType {
    Bool,
    Int,
    UInt,
    Float,
    Number,
    String,
    Bytes,
    Array,
    Map,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
pub enum SchemaVectorPresence {
    #[default]
    Optional,
    Required,
    Forbidden,
}

fn default_true() -> bool {
    true
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SchemaNumericBound {
    pub value: PropValue,
    #[serde(default = "default_true")]
    pub inclusive: bool,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PropertySchema {
    #[serde(default)]
    pub required: bool,
    #[serde(default = "default_true")]
    pub nullable: bool,
    #[serde(default)]
    pub types: Vec<SchemaValueType>,
    #[serde(default)]
    pub numeric_min: Option<SchemaNumericBound>,
    #[serde(default)]
    pub numeric_max: Option<SchemaNumericBound>,
    #[serde(default)]
    pub string_min_bytes: Option<usize>,
    #[serde(default)]
    pub string_max_bytes: Option<usize>,
    #[serde(default)]
    pub bytes_min_len: Option<usize>,
    #[serde(default)]
    pub bytes_max_len: Option<usize>,
    #[serde(default)]
    pub array_min_items: Option<usize>,
    #[serde(default)]
    pub array_max_items: Option<usize>,
    #[serde(default)]
    pub map_min_entries: Option<usize>,
    #[serde(default)]
    pub map_max_entries: Option<usize>,
    #[serde(default)]
    pub enum_values: Vec<PropValue>,
}

impl Default for PropertySchema {
    fn default() -> Self {
        Self {
            required: false,
            nullable: true,
            types: Vec::new(),
            numeric_min: None,
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
        }
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct StringFieldSchema {
    #[serde(default)]
    pub min_bytes: Option<usize>,
    #[serde(default)]
    pub max_bytes: Option<usize>,
    #[serde(default)]
    pub enum_values: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct NumericFieldSchema {
    #[serde(default)]
    pub min: Option<SchemaNumericBound>,
    #[serde(default)]
    pub max: Option<SchemaNumericBound>,
    #[serde(default = "default_true")]
    pub finite: bool,
}

impl Default for NumericFieldSchema {
    fn default() -> Self {
        Self {
            min: None,
            max: None,
            finite: true,
        }
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct NodeLabelConstraintSchema {
    #[serde(default)]
    pub all_of: Vec<String>,
    #[serde(default)]
    pub any_of: Vec<String>,
    #[serde(default)]
    pub none_of: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DenseVectorSchema {
    #[serde(default)]
    pub presence: SchemaVectorPresence,
    #[serde(default)]
    pub dimension: Option<usize>,
}

impl Default for DenseVectorSchema {
    fn default() -> Self {
        Self {
            presence: SchemaVectorPresence::Optional,
            dimension: None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SparseVectorSchema {
    #[serde(default)]
    pub presence: SchemaVectorPresence,
    #[serde(default)]
    pub min_entries: Option<usize>,
    #[serde(default)]
    pub max_entries: Option<usize>,
    #[serde(default)]
    pub max_dimension_id: Option<u32>,
}

impl Default for SparseVectorSchema {
    fn default() -> Self {
        Self {
            presence: SchemaVectorPresence::Optional,
            min_entries: None,
            max_entries: None,
            max_dimension_id: None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct NodeSchema {
    #[serde(default)]
    pub additional_properties: SchemaAdditionalProperties,
    #[serde(default)]
    pub properties: BTreeMap<String, PropertySchema>,
    #[serde(default)]
    pub key: Option<StringFieldSchema>,
    #[serde(default)]
    pub label_constraints: Option<NodeLabelConstraintSchema>,
    #[serde(default)]
    pub weight: Option<NumericFieldSchema>,
    #[serde(default)]
    pub dense_vector: Option<DenseVectorSchema>,
    #[serde(default)]
    pub sparse_vector: Option<SparseVectorSchema>,
}

impl Default for NodeSchema {
    fn default() -> Self {
        Self {
            additional_properties: SchemaAdditionalProperties::Allow,
            properties: BTreeMap::new(),
            key: None,
            label_constraints: None,
            weight: None,
            dense_vector: None,
            sparse_vector: None,
        }
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct EndpointLabelSchema {
    #[serde(default)]
    pub all_of: Vec<String>,
    #[serde(default)]
    pub any_of: Vec<String>,
    #[serde(default)]
    pub none_of: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EdgeValiditySchema {
    #[serde(default)]
    pub require_valid_from_before_valid_to: bool,
    #[serde(default)]
    pub valid_from_min: Option<i64>,
    #[serde(default)]
    pub valid_from_max: Option<i64>,
    #[serde(default)]
    pub valid_to_min: Option<i64>,
    #[serde(default)]
    pub valid_to_max: Option<i64>,
    #[serde(default = "default_true")]
    pub allow_open_ended_valid_to: bool,
}

impl Default for EdgeValiditySchema {
    fn default() -> Self {
        Self {
            require_valid_from_before_valid_to: false,
            valid_from_min: None,
            valid_from_max: None,
            valid_to_min: None,
            valid_to_max: None,
            allow_open_ended_valid_to: true,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct EdgeSchema {
    #[serde(default)]
    pub additional_properties: SchemaAdditionalProperties,
    #[serde(default)]
    pub properties: BTreeMap<String, PropertySchema>,
    #[serde(default)]
    pub from: Option<EndpointLabelSchema>,
    #[serde(default)]
    pub to: Option<EndpointLabelSchema>,
    #[serde(default = "default_true")]
    pub allow_self_loops: bool,
    #[serde(default)]
    pub weight: Option<NumericFieldSchema>,
    #[serde(default)]
    pub validity: Option<EdgeValiditySchema>,
}

impl Default for EdgeSchema {
    fn default() -> Self {
        Self {
            additional_properties: SchemaAdditionalProperties::Allow,
            properties: BTreeMap::new(),
            from: None,
            to: None,
            allow_self_loops: true,
            weight: None,
            validity: None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct NodeSchemaInfo {
    pub label: String,
    pub schema: NodeSchema,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct EdgeSchemaInfo {
    pub label: String,
    pub schema: EdgeSchema,
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct GraphSchema {
    #[serde(default)]
    pub node_schemas: Vec<NodeSchemaInfo>,
    #[serde(default)]
    pub edge_schemas: Vec<EdgeSchemaInfo>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum SchemaTargetKind {
    Node,
    Edge,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum GraphSchemaOperationKind {
    Add,
    Set,
    Drop,
    DropAll,
    CheckAdd,
    CheckSet,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum GraphSchemaOperation {
    SetNode { label: String, schema: NodeSchema },
    SetEdge { label: String, schema: EdgeSchema },
    DropNode { label: String },
    DropEdge { label: String },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum GraphSchemaDropAction {
    Dropped,
    NotFound,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct GraphSchemaDropTargetResult {
    pub target_kind: SchemaTargetKind,
    pub label: String,
    pub action: GraphSchemaDropAction,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct GraphSchemaSetOptions {
    pub max_violations: usize,
    pub chunk_size: usize,
    pub scan_limit: Option<u64>,
}

impl Default for GraphSchemaSetOptions {
    fn default() -> Self {
        Self {
            max_violations: 1,
            chunk_size: 4096,
            scan_limit: None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct GraphSchemaCheckOptions {
    pub max_violations: usize,
    pub chunk_size: usize,
    pub scan_limit: Option<u64>,
}

impl Default for GraphSchemaCheckOptions {
    fn default() -> Self {
        Self {
            max_violations: 100,
            chunk_size: 4096,
            scan_limit: None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct GraphSchemaValidationReportEntry {
    pub target_kind: SchemaTargetKind,
    pub label: String,
    pub report: SchemaValidationReport,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct GraphSchemaCheckReport {
    pub operation: GraphSchemaOperationKind,
    pub entries: Vec<GraphSchemaValidationReportEntry>,
    pub checked_records: u64,
    pub violation_count: u64,
    pub truncated: bool,
    pub scan_limit_hit: bool,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct GraphSchemaPublishResult {
    pub operation: GraphSchemaOperationKind,
    pub node_schemas: Vec<NodeSchemaInfo>,
    pub edge_schemas: Vec<EdgeSchemaInfo>,
    pub validation: GraphSchemaCheckReport,
    pub targets_published: usize,
    pub targets_dropped: usize,
    #[serde(default)]
    pub drop_targets: Vec<GraphSchemaDropTargetResult>,
    #[serde(default)]
    pub node_schemas_dropped: usize,
    #[serde(default)]
    pub edge_schemas_dropped: usize,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SchemaCheckOptions {
    pub max_violations: usize,
    pub chunk_size: usize,
    pub scan_limit: Option<u64>,
}

impl Default for SchemaCheckOptions {
    fn default() -> Self {
        Self {
            max_violations: 100,
            chunk_size: 4096,
            scan_limit: None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SchemaSetOptions {
    pub max_violations: usize,
    pub chunk_size: usize,
    pub scan_limit: Option<u64>,
}

impl Default for SchemaSetOptions {
    fn default() -> Self {
        Self {
            max_violations: 1,
            chunk_size: 4096,
            scan_limit: None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SchemaValidationReport {
    pub checked_records: u64,
    pub violation_count: u64,
    pub violations: Vec<SchemaViolation>,
    pub truncated: bool,
    pub scan_limit_hit: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SchemaViolation {
    pub target: SchemaViolationTarget,
    pub path: String,
    pub message: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum SchemaViolationTarget {
    Node {
        id: u64,
        labels: Vec<String>,
        key: String,
    },
    Edge {
        id: u64,
        label: String,
        from: u64,
        to: u64,
    },
}

#[derive(Debug, Clone)]
pub(crate) struct SchemaValidationFailure {
    pub(crate) path: String,
    pub(crate) message: String,
}

impl SchemaValidationFailure {
    fn into_engine_error(self) -> EngineError {
        EngineError::InvalidOperation(self.message)
    }
}

#[allow(dead_code)]
pub(crate) fn validate_node_schema_shape(schema: &NodeSchema) -> Result<(), EngineError> {
    validate_public_properties("node schema", &schema.properties)?;
    if let Some(key) = &schema.key {
        validate_public_string_field("node schema key", key)?;
    }
    if let Some(constraints) = &schema.label_constraints {
        validate_public_label_constraint("node schema label constraint", constraints)?;
    }
    if let Some(weight) = &schema.weight {
        validate_public_numeric_field("node schema weight", weight)?;
    }
    if let Some(vector) = &schema.dense_vector {
        validate_nonzero_option_usize(vector.dimension, "node dense vector dimension")?;
        if vector
            .dimension
            .is_some_and(|dimension| dimension > u32::MAX as usize)
        {
            return Err(EngineError::InvalidOperation(
                "invalid schema: node dense vector dimension exceeds u32::MAX".to_string(),
            ));
        }
    }
    if let Some(vector) = &schema.sparse_vector {
        validate_nonzero_option_usize(vector.max_entries, "node sparse vector max_entries")?;
        validate_min_max(
            vector.min_entries,
            vector.max_entries,
            "node sparse vector entries",
        )?;
    }
    Ok(())
}

pub(crate) fn validate_node_schema_dense_vector_config(
    schema: &NodeSchema,
    dense_config: Option<&DenseVectorConfig>,
) -> Result<(), EngineError> {
    let Some(rule) = schema.dense_vector.as_ref() else {
        return Ok(());
    };
    if rule.presence == SchemaVectorPresence::Required && dense_config.is_none() {
        return Err(EngineError::InvalidOperation(
            "invalid schema: node dense vector requires DbOptions::dense_vector to be configured"
                .to_string(),
        ));
    }
    if let (Some(schema_dimension), Some(config)) = (rule.dimension, dense_config) {
        if schema_dimension != config.dimension as usize {
            return Err(EngineError::InvalidOperation(format!(
                "invalid schema: node dense vector dimension {schema_dimension} does not match DB dimension {}",
                config.dimension
            )));
        }
    }
    Ok(())
}

#[allow(dead_code)]
pub(crate) fn validate_edge_schema_shape(schema: &EdgeSchema) -> Result<(), EngineError> {
    validate_public_properties("edge schema", &schema.properties)?;
    if let Some(from) = &schema.from {
        validate_public_endpoint_label_constraint("edge schema from endpoint", from)?;
    }
    if let Some(to) = &schema.to {
        validate_public_endpoint_label_constraint("edge schema to endpoint", to)?;
    }
    if let Some(weight) = &schema.weight {
        validate_public_numeric_field("edge schema weight", weight)?;
    }
    if let Some(validity) = &schema.validity {
        validate_public_edge_validity("edge schema validity", validity)?;
    }
    Ok(())
}

pub(crate) fn normalize_schema_manifest(manifest: &mut ManifestState) -> Result<bool, EngineError> {
    let mut dirty = false;
    let has_schema_entries = !manifest.node_schemas.is_empty() || !manifest.edge_schemas.is_empty();
    match manifest.schema_catalog_version {
        0 if has_schema_entries => {
            return Err(EngineError::ManifestError(
                "invalid schema manifest: schema_catalog_version 0 cannot contain schema entries"
                    .to_string(),
            ));
        }
        0 => {
            manifest.schema_catalog_version = SCHEMA_CATALOG_VERSION;
            dirty = true;
        }
        SCHEMA_CATALOG_VERSION => {}
        version => {
            return Err(EngineError::ManifestError(format!(
                "invalid schema manifest: unsupported schema catalog version {version}"
            )));
        }
    }

    let max_schema_id = manifest
        .node_schemas
        .iter()
        .map(|entry| entry.schema_id)
        .chain(manifest.edge_schemas.iter().map(|entry| entry.schema_id))
        .max()
        .unwrap_or(0);
    let normalized_next_schema_id = if max_schema_id == 0 {
        1
    } else {
        max_schema_id.checked_add(1).ok_or_else(|| {
            EngineError::ManifestError(
                "invalid schema manifest: next_schema_id would overflow".to_string(),
            )
        })?
    };
    if manifest.next_schema_id != normalized_next_schema_id {
        manifest.next_schema_id = normalized_next_schema_id;
        dirty = true;
    }

    for entry in &mut manifest.node_schemas {
        dirty |= normalize_node_label_constraint_option(&mut entry.label_constraints);
    }
    for entry in &mut manifest.edge_schemas {
        dirty |= normalize_endpoint_label_constraint_option(&mut entry.from);
        dirty |= normalize_endpoint_label_constraint_option(&mut entry.to);
    }

    Ok(dirty)
}

pub(crate) fn validate_schema_manifest(manifest: &ManifestState) -> Result<(), EngineError> {
    let has_schema_entries = !manifest.node_schemas.is_empty() || !manifest.edge_schemas.is_empty();
    match manifest.schema_catalog_version {
        0 if !has_schema_entries => return Ok(()),
        0 => {
            return Err(EngineError::ManifestError(
                "invalid schema manifest: schema_catalog_version 0 cannot contain schema entries"
                    .to_string(),
            ));
        }
        SCHEMA_CATALOG_VERSION => {}
        version => {
            return Err(EngineError::ManifestError(format!(
                "invalid schema manifest: unsupported schema catalog version {version}"
            )));
        }
    }

    let node_label_ids: HashSet<u32> = manifest.node_label_tokens.values().copied().collect();
    let edge_label_ids: HashSet<u32> = manifest.edge_label_tokens.values().copied().collect();
    let mut schema_ids = HashSet::new();
    let mut node_targets = HashSet::new();
    let mut edge_targets = HashSet::new();

    for entry in &manifest.node_schemas {
        validate_manifest_entry_size("node schema", entry)?;
        if !schema_ids.insert(entry.schema_id) {
            return Err(EngineError::ManifestError(format!(
                "invalid schema manifest: duplicate schema id {}",
                entry.schema_id
            )));
        }
        validate_target_label_id(
            "node schema target",
            entry.label_id,
            &node_label_ids,
            "node_label_tokens",
        )?;
        if !node_targets.insert(entry.label_id) {
            return Err(EngineError::ManifestError(format!(
                "invalid schema manifest: duplicate node schema target label_id {}",
                entry.label_id
            )));
        }
        validate_manifest_properties("node schema", &entry.properties)?;
        if let Some(key) = &entry.key {
            validate_manifest_string_field("node schema key", key)?;
        }
        if let Some(constraints) = &entry.label_constraints {
            validate_node_label_constraint_manifest(
                "node schema label_constraints",
                constraints,
                &node_label_ids,
            )?;
        }
        if let Some(weight) = &entry.weight {
            validate_manifest_numeric_bounds(
                "node schema weight",
                weight.min.as_ref(),
                weight.max.as_ref(),
            )?;
        }
        if let Some(vector) = &entry.dense_vector {
            if vector.dimension == Some(0) {
                return Err(EngineError::ManifestError(
                    "invalid schema manifest: node dense vector dimension must be nonzero"
                        .to_string(),
                ));
            }
        }
        if let Some(vector) = &entry.sparse_vector {
            if vector.max_entries == Some(0) {
                return Err(EngineError::ManifestError(
                    "invalid schema manifest: node sparse vector max_entries must be nonzero"
                        .to_string(),
                ));
            }
            validate_min_max_manifest(
                vector.min_entries,
                vector.max_entries,
                "node sparse vector entries",
            )?;
        }
    }

    for entry in &manifest.edge_schemas {
        validate_manifest_entry_size("edge schema", entry)?;
        if !schema_ids.insert(entry.schema_id) {
            return Err(EngineError::ManifestError(format!(
                "invalid schema manifest: duplicate schema id {}",
                entry.schema_id
            )));
        }
        validate_target_label_id(
            "edge schema target",
            entry.label_id,
            &edge_label_ids,
            "edge_label_tokens",
        )?;
        if !edge_targets.insert(entry.label_id) {
            return Err(EngineError::ManifestError(format!(
                "invalid schema manifest: duplicate edge schema target label_id {}",
                entry.label_id
            )));
        }
        validate_manifest_properties("edge schema", &entry.properties)?;
        if let Some(from) = &entry.from {
            validate_endpoint_label_manifest("edge schema from endpoint", from, &node_label_ids)?;
        }
        if let Some(to) = &entry.to {
            validate_endpoint_label_manifest("edge schema to endpoint", to, &node_label_ids)?;
        }
        if let Some(weight) = &entry.weight {
            validate_manifest_numeric_bounds(
                "edge schema weight",
                weight.min.as_ref(),
                weight.max.as_ref(),
            )?;
        }
        if let Some(validity) = &entry.validity {
            validate_manifest_edge_validity("edge schema validity", validity)?;
        }
    }

    Ok(())
}

#[derive(Debug, Clone)]
#[allow(dead_code)]
pub(crate) struct CompiledNodeSchema {
    pub(crate) schema_id: u64,
    pub(crate) revision: u64,
    pub(crate) label_id: u32,
    pub(crate) label_name: Option<String>,
    pub(crate) additional_properties: SchemaAdditionalPropertiesManifest,
    pub(crate) properties: Vec<CompiledPropertyRule>,
    pub(crate) property_keys: Vec<String>,
    pub(crate) key: Option<CompiledStringFieldRule>,
    pub(crate) label_constraints: Option<CompiledLabelRule>,
    pub(crate) weight: Option<CompiledNumericFieldRule>,
    pub(crate) dense_vector: Option<CompiledDenseVectorRule>,
    pub(crate) sparse_vector: Option<CompiledSparseVectorRule>,
}

struct ApplicableNodeSchemas<'a> {
    schemas: [Option<&'a CompiledNodeSchema>; MAX_NODE_LABELS_PER_NODE],
    len: usize,
}

impl<'a> ApplicableNodeSchemas<'a> {
    fn new() -> Self {
        Self {
            schemas: [None; MAX_NODE_LABELS_PER_NODE],
            len: 0,
        }
    }

    fn push(&mut self, schema: &'a CompiledNodeSchema) {
        if self.len < MAX_NODE_LABELS_PER_NODE {
            self.schemas[self.len] = Some(schema);
            self.len += 1;
        }
    }

    fn is_empty(&self) -> bool {
        self.len == 0
    }

    fn iter(&self) -> impl Iterator<Item = &'a CompiledNodeSchema> + '_ {
        self.schemas[..self.len].iter().filter_map(|schema| *schema)
    }
}

#[derive(Debug, Clone)]
#[allow(dead_code)]
pub(crate) struct CompiledEdgeSchema {
    pub(crate) schema_id: u64,
    pub(crate) revision: u64,
    pub(crate) label_id: u32,
    pub(crate) label_name: Option<String>,
    pub(crate) additional_properties: SchemaAdditionalPropertiesManifest,
    pub(crate) properties: Vec<CompiledPropertyRule>,
    pub(crate) property_keys: Vec<String>,
    pub(crate) from: Option<CompiledLabelRule>,
    pub(crate) to: Option<CompiledLabelRule>,
    pub(crate) allow_self_loops: bool,
    pub(crate) weight: Option<CompiledNumericFieldRule>,
    pub(crate) validity: Option<EdgeValiditySchemaManifestRule>,
}

#[derive(Debug, Clone)]
#[allow(dead_code)]
pub(crate) struct CompiledPropertyRule {
    key: String,
    required: bool,
    nullable: bool,
    types: Vec<SchemaValueTypeManifest>,
    numeric_min: Option<CompiledNumericBound>,
    numeric_max: Option<CompiledNumericBound>,
    string_min_bytes: Option<usize>,
    string_max_bytes: Option<usize>,
    bytes_min_len: Option<usize>,
    bytes_max_len: Option<usize>,
    array_min_items: Option<usize>,
    array_max_items: Option<usize>,
    map_min_entries: Option<usize>,
    map_max_entries: Option<usize>,
    enum_values: Vec<CompiledEnumValue>,
}

#[derive(Debug, Clone)]
#[allow(dead_code)]
pub(crate) struct CompiledStringFieldRule {
    min_bytes: Option<usize>,
    max_bytes: Option<usize>,
    enum_values: Vec<String>,
}

#[derive(Debug, Clone)]
#[allow(dead_code)]
pub(crate) struct CompiledNumericFieldRule {
    min: Option<CompiledNumericBound>,
    max: Option<CompiledNumericBound>,
    finite: bool,
}

#[derive(Debug, Clone)]
#[allow(dead_code)]
pub(crate) struct CompiledNumericBound {
    value: PropValue,
    key: NumericScalarKey,
    inclusive: bool,
}

#[derive(Debug, Clone)]
#[allow(dead_code)]
pub(crate) struct CompiledEnumValue {
    value: PropValue,
    numeric_key: Option<NumericScalarKey>,
}

#[derive(Debug, Clone)]
#[allow(dead_code)]
pub(crate) struct CompiledLabelRule {
    all_of: Vec<u32>,
    any_of: Vec<u32>,
    none_of: Vec<u32>,
}

#[derive(Debug, Clone)]
#[allow(dead_code)]
pub(crate) struct CompiledDenseVectorRule {
    presence: SchemaVectorPresenceManifest,
    dimension: Option<u32>,
}

#[derive(Debug, Clone)]
#[allow(dead_code)]
pub(crate) struct CompiledSparseVectorRule {
    presence: SchemaVectorPresenceManifest,
    min_entries: Option<usize>,
    max_entries: Option<usize>,
    max_dimension_id: Option<u32>,
}

#[derive(Debug, Clone)]
#[allow(dead_code)]
pub(crate) struct RuntimeSchemaCatalog {
    pub(crate) node_by_label_id: HashMap<u32, Arc<CompiledNodeSchema>>,
    pub(crate) edge_by_label_id: HashMap<u32, Arc<CompiledEdgeSchema>>,
    pub(crate) node_schema_label_ids: HashSet<u32>,
    pub(crate) edge_schema_label_ids: HashSet<u32>,
    pub(crate) endpoint_relevant_node_label_ids: HashSet<u32>,
    pub(crate) endpoint_constrained_edge_label_ids: Vec<u32>,
    pub(crate) is_empty: bool,
    pub(crate) has_node_schemas: bool,
    pub(crate) has_edge_schemas: bool,
    pub(crate) has_edge_endpoint_constraints: bool,
    pub(crate) has_node_label_constraints: bool,
    pub(crate) has_closed_node_schema: bool,
    pub(crate) has_closed_edge_schema: bool,
}

#[allow(dead_code)]
impl RuntimeSchemaCatalog {
    pub(crate) fn empty() -> Self {
        Self {
            node_by_label_id: HashMap::new(),
            edge_by_label_id: HashMap::new(),
            node_schema_label_ids: HashSet::new(),
            edge_schema_label_ids: HashSet::new(),
            endpoint_relevant_node_label_ids: HashSet::new(),
            endpoint_constrained_edge_label_ids: Vec::new(),
            is_empty: true,
            has_node_schemas: false,
            has_edge_schemas: false,
            has_edge_endpoint_constraints: false,
            has_node_label_constraints: false,
            has_closed_node_schema: false,
            has_closed_edge_schema: false,
        }
    }

    pub(crate) fn from_manifest(manifest: &ManifestState) -> Result<Self, EngineError> {
        validate_schema_manifest(manifest)?;
        if manifest.node_schemas.is_empty() && manifest.edge_schemas.is_empty() {
            return Ok(Self::empty());
        }

        let mut catalog = Self {
            node_by_label_id: HashMap::with_capacity(manifest.node_schemas.len()),
            edge_by_label_id: HashMap::with_capacity(manifest.edge_schemas.len()),
            node_schema_label_ids: HashSet::with_capacity(manifest.node_schemas.len()),
            edge_schema_label_ids: HashSet::with_capacity(manifest.edge_schemas.len()),
            endpoint_relevant_node_label_ids: HashSet::new(),
            endpoint_constrained_edge_label_ids: Vec::new(),
            is_empty: false,
            has_node_schemas: !manifest.node_schemas.is_empty(),
            has_edge_schemas: !manifest.edge_schemas.is_empty(),
            has_edge_endpoint_constraints: false,
            has_node_label_constraints: false,
            has_closed_node_schema: false,
            has_closed_edge_schema: false,
        };
        let node_label_names = compile_label_names_by_id(&manifest.node_label_tokens);
        let edge_label_names = compile_label_names_by_id(&manifest.edge_label_tokens);

        for entry in &manifest.node_schemas {
            catalog.node_schema_label_ids.insert(entry.label_id);
            catalog.has_node_label_constraints |= entry.label_constraints.is_some();
            catalog.has_closed_node_schema |=
                entry.additional_properties == SchemaAdditionalPropertiesManifest::Reject;
            catalog.node_by_label_id.insert(
                entry.label_id,
                Arc::new(CompiledNodeSchema {
                    schema_id: entry.schema_id,
                    revision: entry.revision,
                    label_id: entry.label_id,
                    label_name: node_label_names.get(&entry.label_id).cloned(),
                    additional_properties: entry.additional_properties,
                    properties: compile_property_rules(&entry.properties)?,
                    property_keys: entry.properties.keys().cloned().collect(),
                    key: entry.key.as_ref().map(compile_string_field_rule),
                    label_constraints: entry
                        .label_constraints
                        .as_ref()
                        .map(compile_node_label_rule),
                    weight: entry
                        .weight
                        .as_ref()
                        .map(compile_numeric_field_rule)
                        .transpose()?,
                    dense_vector: entry.dense_vector.as_ref().map(compile_dense_vector_rule),
                    sparse_vector: entry.sparse_vector.as_ref().map(compile_sparse_vector_rule),
                }),
            );
        }

        for entry in &manifest.edge_schemas {
            catalog.edge_schema_label_ids.insert(entry.label_id);
            catalog.has_closed_edge_schema |=
                entry.additional_properties == SchemaAdditionalPropertiesManifest::Reject;
            if let Some(from) = &entry.from {
                catalog.has_edge_endpoint_constraints = true;
                catalog
                    .endpoint_constrained_edge_label_ids
                    .push(entry.label_id);
                insert_endpoint_relevant_labels(
                    from,
                    &mut catalog.endpoint_relevant_node_label_ids,
                );
            }
            if let Some(to) = &entry.to {
                catalog.has_edge_endpoint_constraints = true;
                catalog
                    .endpoint_constrained_edge_label_ids
                    .push(entry.label_id);
                insert_endpoint_relevant_labels(to, &mut catalog.endpoint_relevant_node_label_ids);
            }
            catalog.edge_by_label_id.insert(
                entry.label_id,
                Arc::new(CompiledEdgeSchema {
                    schema_id: entry.schema_id,
                    revision: entry.revision,
                    label_id: entry.label_id,
                    label_name: edge_label_names.get(&entry.label_id).cloned(),
                    additional_properties: entry.additional_properties,
                    properties: compile_property_rules(&entry.properties)?,
                    property_keys: entry.properties.keys().cloned().collect(),
                    from: entry.from.as_ref().map(compile_endpoint_label_rule),
                    to: entry.to.as_ref().map(compile_endpoint_label_rule),
                    allow_self_loops: entry.allow_self_loops,
                    weight: entry
                        .weight
                        .as_ref()
                        .map(compile_numeric_field_rule)
                        .transpose()?,
                    validity: entry.validity.clone(),
                }),
            );
        }
        catalog.endpoint_constrained_edge_label_ids.sort_unstable();
        catalog.endpoint_constrained_edge_label_ids.dedup();

        Ok(catalog)
    }

    #[inline]
    pub(crate) fn is_empty(&self) -> bool {
        self.is_empty
    }

    #[inline]
    pub(crate) fn node_has_applicable_schema(&self, labels: &NodeLabelSet) -> bool {
        labels
            .as_slice()
            .iter()
            .any(|label_id| self.node_schema_label_ids.contains(label_id))
    }

    fn applicable_node_schemas<'a>(&'a self, labels: &NodeLabelSet) -> ApplicableNodeSchemas<'a> {
        let mut applicable = ApplicableNodeSchemas::new();
        for label_id in labels.as_slice() {
            if let Some(schema) = self.node_by_label_id.get(label_id) {
                applicable.push(schema.as_ref());
            }
        }
        applicable
    }

    #[inline]
    pub(crate) fn edge_has_applicable_schema(&self, label_id: u32) -> bool {
        self.edge_schema_label_ids.contains(&label_id)
    }

    #[inline]
    pub(crate) fn edge_has_wal_validation_rules(&self, label_id: u32) -> bool {
        self.edge_by_label_id.get(&label_id).is_some_and(|schema| {
            !schema.properties.is_empty()
                || schema.additional_properties == SchemaAdditionalPropertiesManifest::Reject
                || schema.weight.is_some()
                || schema.validity.is_some()
                || schema.from.is_some()
                || schema.to.is_some()
                || !schema.allow_self_loops
        })
    }

    pub(crate) fn label_change_may_affect_endpoint_rules(
        &self,
        old_labels: Option<&NodeLabelSet>,
        final_labels: Option<&NodeLabelSet>,
        deleted: bool,
    ) -> bool {
        if !self.has_edge_endpoint_constraints {
            return false;
        }
        if deleted {
            return true;
        }
        self.endpoint_relevant_node_label_ids
            .iter()
            .any(|label_id| {
                let had = old_labels.is_some_and(|labels| labels.contains(*label_id));
                let has = final_labels.is_some_and(|labels| labels.contains(*label_id));
                had != has
            })
    }

    pub(crate) fn validate_node_record(
        &self,
        node: &NodeRecord,
        dense_config: Option<&DenseVectorConfig>,
    ) -> Result<(), EngineError> {
        self.validate_node_record_detailed(node, dense_config)
            .map_err(SchemaValidationFailure::into_engine_error)
    }

    pub(crate) fn validate_node_record_detailed(
        &self,
        node: &NodeRecord,
        dense_config: Option<&DenseVectorConfig>,
    ) -> Result<(), SchemaValidationFailure> {
        if self.is_empty() || !self.node_has_applicable_schema(&node.label_ids) {
            return Ok(());
        }

        let applicable = self.applicable_node_schemas(&node.label_ids);
        if applicable.is_empty() {
            return Ok(());
        }

        for schema in applicable.iter() {
            if let Some(rule) = &schema.label_constraints {
                validate_compiled_label_rule(rule, &node.label_ids, "labels", |path, expected| {
                    node_schema_violation(
                        node,
                        schema.label_id,
                        schema.label_name.as_deref(),
                        &path,
                        expected,
                        format!("labels {:?}", node.label_ids.as_slice()),
                    )
                })?;
            }
        }
        for schema in applicable.iter() {
            if let Some(rule) = &schema.key {
                validate_string_field_rule(&node.key, rule, |expected, actual| {
                    node_schema_violation(
                        node,
                        schema.label_id,
                        schema.label_name.as_deref(),
                        "key",
                        expected,
                        actual,
                    )
                })?;
            }
        }
        for schema in applicable.iter() {
            if let Some(rule) = &schema.weight {
                validate_weight_rule(node.weight, rule, |expected, actual| {
                    node_schema_violation(
                        node,
                        schema.label_id,
                        schema.label_name.as_deref(),
                        "weight",
                        expected,
                        actual,
                    )
                })?;
            }
        }
        for schema in applicable.iter() {
            if let Some(rule) = &schema.dense_vector {
                validate_dense_vector_rule(node, schema, rule, dense_config)?;
            }
            if let Some(rule) = &schema.sparse_vector {
                validate_sparse_vector_rule(node, schema, rule)?;
            }
        }
        for schema in applicable.iter() {
            for rule in &schema.properties {
                validate_property_rule(
                    rule,
                    node.props.get(&rule.key),
                    |path, expected, actual| {
                        node_schema_violation(
                            node,
                            schema.label_id,
                            schema.label_name.as_deref(),
                            path,
                            expected,
                            actual,
                        )
                    },
                )?;
            }
        }
        validate_node_closed_properties(node, &applicable)
    }

    pub(crate) fn validate_edge_record(&self, edge: &EdgeRecord) -> Result<(), EngineError> {
        self.validate_edge_record_detailed(edge)
            .map_err(SchemaValidationFailure::into_engine_error)
    }

    pub(crate) fn validate_edge_record_detailed(
        &self,
        edge: &EdgeRecord,
    ) -> Result<(), SchemaValidationFailure> {
        if self.is_empty() || !self.edge_has_applicable_schema(edge.label_id) {
            return Ok(());
        }
        let Some(schema) = self.edge_by_label_id.get(&edge.label_id) else {
            return Ok(());
        };
        let schema = schema.as_ref();

        if let Some(rule) = &schema.weight {
            validate_weight_rule(edge.weight, rule, |expected, actual| {
                edge_schema_violation(
                    edge,
                    schema.label_name.as_deref(),
                    "weight",
                    expected,
                    actual,
                )
            })?;
        }
        if let Some(rule) = &schema.validity {
            validate_edge_validity_rule(edge, schema.label_name.as_deref(), rule)?;
        }
        for rule in &schema.properties {
            validate_property_rule(rule, edge.props.get(&rule.key), |path, expected, actual| {
                edge_schema_violation(edge, schema.label_name.as_deref(), path, expected, actual)
            })?;
        }
        validate_edge_closed_properties(edge, schema)?;
        if !schema.allow_self_loops && edge.from == edge.to {
            return Err(edge_schema_violation(
                edge,
                schema.label_name.as_deref(),
                "self_loop",
                "from and to to differ".to_string(),
                format!("from {} to {}", edge.from, edge.to),
            ));
        }
        Ok(())
    }

    #[inline]
    pub(crate) fn edge_schema_has_endpoint_rules(&self, label_id: u32) -> bool {
        self.edge_by_label_id
            .get(&label_id)
            .is_some_and(|schema| schema.from.is_some() || schema.to.is_some())
    }

    pub(crate) fn validate_edge_endpoint_labels(
        &self,
        edge: &EdgeRecord,
        from_labels: Option<&NodeLabelSet>,
        to_labels: Option<&NodeLabelSet>,
    ) -> Result<(), EngineError> {
        self.validate_edge_endpoint_labels_detailed(edge, from_labels, to_labels)
            .map_err(SchemaValidationFailure::into_engine_error)
    }

    pub(crate) fn validate_edge_endpoint_labels_detailed(
        &self,
        edge: &EdgeRecord,
        from_labels: Option<&NodeLabelSet>,
        to_labels: Option<&NodeLabelSet>,
    ) -> Result<(), SchemaValidationFailure> {
        if self.is_empty() || !self.edge_has_applicable_schema(edge.label_id) {
            return Ok(());
        }
        let Some(schema) = self.edge_by_label_id.get(&edge.label_id) else {
            return Ok(());
        };
        let schema = schema.as_ref();
        if let Some(rule) = &schema.from {
            validate_edge_endpoint_label_rule(
                edge,
                schema.label_name.as_deref(),
                "from",
                rule,
                from_labels,
            )?;
        }
        if let Some(rule) = &schema.to {
            validate_edge_endpoint_label_rule(
                edge,
                schema.label_name.as_deref(),
                "to",
                rule,
                to_labels,
            )?;
        }
        Ok(())
    }
}

fn validate_edge_endpoint_label_rule(
    edge: &EdgeRecord,
    schema_label_name: Option<&str>,
    endpoint: &str,
    rule: &CompiledLabelRule,
    labels: Option<&NodeLabelSet>,
) -> Result<(), SchemaValidationFailure> {
    let endpoint_node_id = match endpoint {
        "from" => edge.from,
        "to" => edge.to,
        _ => 0,
    };
    let Some(labels) = labels else {
        return Err(edge_schema_violation(
            edge,
            schema_label_name,
            &missing_endpoint_rule_path(endpoint, rule),
            "existing endpoint with labels satisfying endpoint rule".to_string(),
            format!("{endpoint} endpoint node id {endpoint_node_id} missing endpoint"),
        ));
    };
    validate_compiled_label_rule(
        rule,
        labels,
        &format!("{endpoint}.labels"),
        |path, expected| {
            edge_schema_violation(
                edge,
                schema_label_name,
                &path,
                expected,
                format!(
                    "{endpoint} endpoint node id {endpoint_node_id} labels {:?}",
                    labels.as_slice()
                ),
            )
        },
    )
}

fn missing_endpoint_rule_path(endpoint: &str, rule: &CompiledLabelRule) -> String {
    if !rule.all_of.is_empty() {
        format!("{endpoint}.labels.all_of")
    } else if !rule.any_of.is_empty() {
        format!("{endpoint}.labels.any_of")
    } else {
        format!("{endpoint}.labels.none_of")
    }
}

#[allow(dead_code)]
pub(crate) fn validate_endpoint_label_rule_against_labels(
    rule: &EndpointLabelManifestRule,
    labels: &NodeLabelSet,
) -> Result<(), EngineError> {
    let rule = compile_endpoint_label_rule(rule);
    validate_compiled_label_rule(&rule, labels, "labels", |path, expected| {
        EngineError::InvalidOperation(format!(
            "schema violation: edge endpoint path {path} expected {expected}; actual labels {:?}",
            labels.as_slice()
        ))
    })
}

fn validate_compiled_label_rule<E, F>(
    rule: &CompiledLabelRule,
    labels: &NodeLabelSet,
    path_base: &str,
    mut make_error: F,
) -> Result<(), E>
where
    F: FnMut(String, String) -> E,
{
    for label_id in &rule.all_of {
        if !labels.contains(*label_id) {
            return Err(make_error(
                format!("{path_base}.all_of"),
                format!("label_id {label_id} to be present"),
            ));
        }
    }
    if !rule.any_of.is_empty()
        && !rule
            .any_of
            .iter()
            .any(|label_id| labels.contains(*label_id))
    {
        return Err(make_error(
            format!("{path_base}.any_of"),
            format!("one of label_ids {:?}", rule.any_of),
        ));
    }
    for label_id in &rule.none_of {
        if labels.contains(*label_id) {
            return Err(make_error(
                format!("{path_base}.none_of"),
                format!("label_id {label_id} to be absent"),
            ));
        }
    }
    Ok(())
}

fn validate_property_rule<E, F>(
    rule: &CompiledPropertyRule,
    value: Option<&PropValue>,
    make_error: F,
) -> Result<(), E>
where
    F: Fn(&str, String, String) -> E,
{
    let path = format!("properties.{}", rule.key);
    let Some(value) = value else {
        return if rule.required {
            Err(make_error(
                &path,
                "required property".to_string(),
                "missing".to_string(),
            ))
        } else {
            Ok(())
        };
    };

    if matches!(value, PropValue::Null) {
        return if rule.nullable {
            Ok(())
        } else {
            Err(make_error(
                &path,
                "non-null value".to_string(),
                "null".to_string(),
            ))
        };
    }

    if !rule.types.is_empty()
        && !rule
            .types
            .iter()
            .any(|schema_type| schema_value_type_matches(*schema_type, value))
    {
        return Err(make_error(
            &path,
            format!("type in {:?}", rule.types),
            prop_value_type_name(value).to_string(),
        ));
    }

    validate_property_numeric_bounds(rule, value, &path, &make_error)?;
    validate_type_specific_property_bounds(rule, value, &path, &make_error)?;

    if !rule.enum_values.is_empty()
        && !rule
            .enum_values
            .iter()
            .any(|candidate| compiled_enum_value_matches(candidate, value))
    {
        return Err(make_error(
            &path,
            format!("one of {} enum values", rule.enum_values.len()),
            describe_prop_value(value),
        ));
    }

    Ok(())
}

fn validate_property_numeric_bounds<E, F>(
    rule: &CompiledPropertyRule,
    value: &PropValue,
    path: &str,
    make_error: &F,
) -> Result<(), E>
where
    F: Fn(&str, String, String) -> E,
{
    if rule.numeric_min.is_none() && rule.numeric_max.is_none() {
        return Ok(());
    }
    let Some(value_key) = numeric_key(value) else {
        return Err(make_error(
            path,
            "finite numeric value".to_string(),
            describe_prop_value(value),
        ));
    };
    validate_numeric_bound_pair(
        value_key,
        rule.numeric_min.as_ref(),
        rule.numeric_max.as_ref(),
        path,
        describe_prop_value(value),
        make_error,
    )
}

fn validate_numeric_bound_pair<E, F>(
    value_key: NumericScalarKey,
    min: Option<&CompiledNumericBound>,
    max: Option<&CompiledNumericBound>,
    path: &str,
    actual: String,
    make_error: &F,
) -> Result<(), E>
where
    F: Fn(&str, String, String) -> E,
{
    if let Some(bound) = min {
        let ordering = compare_numeric_keys(value_key, bound.key);
        if ordering == Ordering::Less || (ordering == Ordering::Equal && !bound.inclusive) {
            return Err(make_error(
                path,
                format!(
                    "numeric value {} {}",
                    if bound.inclusive { ">=" } else { ">" },
                    describe_prop_value(&bound.value)
                ),
                actual,
            ));
        }
    }
    if let Some(bound) = max {
        let ordering = compare_numeric_keys(value_key, bound.key);
        if ordering == Ordering::Greater || (ordering == Ordering::Equal && !bound.inclusive) {
            return Err(make_error(
                path,
                format!(
                    "numeric value {} {}",
                    if bound.inclusive { "<=" } else { "<" },
                    describe_prop_value(&bound.value)
                ),
                actual,
            ));
        }
    }
    Ok(())
}

fn validate_type_specific_property_bounds<E, F>(
    rule: &CompiledPropertyRule,
    value: &PropValue,
    path: &str,
    make_error: &F,
) -> Result<(), E>
where
    F: Fn(&str, String, String) -> E,
{
    match value {
        PropValue::String(value) => validate_len_bounds(
            value.len(),
            rule.string_min_bytes,
            rule.string_max_bytes,
            "UTF-8 byte length",
            path,
            make_error,
        ),
        PropValue::Bytes(value) => validate_len_bounds(
            value.len(),
            rule.bytes_min_len,
            rule.bytes_max_len,
            "byte length",
            path,
            make_error,
        ),
        PropValue::Array(value) => validate_len_bounds(
            value.len(),
            rule.array_min_items,
            rule.array_max_items,
            "array item count",
            path,
            make_error,
        ),
        PropValue::Map(value) => validate_len_bounds(
            value.len(),
            rule.map_min_entries,
            rule.map_max_entries,
            "map entry count",
            path,
            make_error,
        ),
        PropValue::Null
        | PropValue::Bool(_)
        | PropValue::Int(_)
        | PropValue::UInt(_)
        | PropValue::Float(_) => Ok(()),
    }
}

fn validate_len_bounds<E, F>(
    len: usize,
    min: Option<usize>,
    max: Option<usize>,
    summary: &str,
    path: &str,
    make_error: &F,
) -> Result<(), E>
where
    F: Fn(&str, String, String) -> E,
{
    if let Some(min) = min {
        if len < min {
            return Err(make_error(
                path,
                format!("{summary} >= {min}"),
                len.to_string(),
            ));
        }
    }
    if let Some(max) = max {
        if len > max {
            return Err(make_error(
                path,
                format!("{summary} <= {max}"),
                len.to_string(),
            ));
        }
    }
    Ok(())
}

fn schema_value_type_matches(schema_type: SchemaValueTypeManifest, value: &PropValue) -> bool {
    match schema_type {
        SchemaValueTypeManifest::Bool => matches!(value, PropValue::Bool(_)),
        SchemaValueTypeManifest::Int => matches!(value, PropValue::Int(_)),
        SchemaValueTypeManifest::UInt => matches!(value, PropValue::UInt(_)),
        SchemaValueTypeManifest::Float => matches!(value, PropValue::Float(_)),
        SchemaValueTypeManifest::Number => numeric_key(value).is_some(),
        SchemaValueTypeManifest::String => matches!(value, PropValue::String(_)),
        SchemaValueTypeManifest::Bytes => matches!(value, PropValue::Bytes(_)),
        SchemaValueTypeManifest::Array => matches!(value, PropValue::Array(_)),
        SchemaValueTypeManifest::Map => matches!(value, PropValue::Map(_)),
    }
}

fn compiled_enum_value_matches(candidate: &CompiledEnumValue, value: &PropValue) -> bool {
    if let (Some(candidate_key), Some(value_key)) = (candidate.numeric_key, numeric_key(value)) {
        candidate_key == value_key
    } else {
        semantic_property_eq(value, &candidate.value)
    }
}

fn validate_string_field_rule<E, F>(
    value: &str,
    rule: &CompiledStringFieldRule,
    make_error: F,
) -> Result<(), E>
where
    F: Fn(String, String) -> E,
{
    let len = value.len();
    if let Some(min) = rule.min_bytes {
        if len < min {
            return Err(make_error(
                format!("UTF-8 byte length >= {min}"),
                len.to_string(),
            ));
        }
    }
    if let Some(max) = rule.max_bytes {
        if len > max {
            return Err(make_error(
                format!("UTF-8 byte length <= {max}"),
                len.to_string(),
            ));
        }
    }
    if !rule.enum_values.is_empty() && !rule.enum_values.iter().any(|candidate| candidate == value)
    {
        return Err(make_error(
            format!("one of {} enum values", rule.enum_values.len()),
            value.to_string(),
        ));
    }
    Ok(())
}

fn validate_weight_rule<E, F>(
    weight: f32,
    rule: &CompiledNumericFieldRule,
    make_error: F,
) -> Result<(), E>
where
    F: Fn(String, String) -> E,
{
    if rule.finite && !weight.is_finite() {
        return Err(make_error(
            "finite weight".to_string(),
            format!("{weight:?}"),
        ));
    }
    if rule.min.is_none() && rule.max.is_none() {
        return Ok(());
    }
    let value = PropValue::Float(weight as f64);
    let Some(value_key) = numeric_key(&value) else {
        return Err(make_error(
            "finite numeric weight within bounds".to_string(),
            format!("{weight:?}"),
        ));
    };
    validate_numeric_bound_pair(
        value_key,
        rule.min.as_ref(),
        rule.max.as_ref(),
        "weight",
        format!("{weight:?}"),
        &|_, expected, actual| make_error(expected, actual),
    )
}

fn validate_dense_vector_rule(
    node: &NodeRecord,
    schema: &CompiledNodeSchema,
    rule: &CompiledDenseVectorRule,
    dense_config: Option<&DenseVectorConfig>,
) -> Result<(), SchemaValidationFailure> {
    if let (Some(schema_dimension), Some(config)) = (rule.dimension, dense_config) {
        if schema_dimension != config.dimension {
            return Err(node_schema_violation(
                node,
                schema.label_id,
                schema.label_name.as_deref(),
                "dense_vector",
                format!(
                    "schema dimension {schema_dimension} to match DB dimension {}",
                    config.dimension
                ),
                "dimension mismatch".to_string(),
            ));
        }
    }
    match (rule.presence, node.dense_vector.as_ref()) {
        (SchemaVectorPresenceManifest::Required, None) => {
            return Err(node_schema_violation(
                node,
                schema.label_id,
                schema.label_name.as_deref(),
                "dense_vector",
                "present dense vector".to_string(),
                "absent".to_string(),
            ));
        }
        (SchemaVectorPresenceManifest::Forbidden, Some(_)) => {
            return Err(node_schema_violation(
                node,
                schema.label_id,
                schema.label_name.as_deref(),
                "dense_vector",
                "absent dense vector".to_string(),
                "present".to_string(),
            ));
        }
        (SchemaVectorPresenceManifest::Optional, _)
        | (SchemaVectorPresenceManifest::Required, Some(_))
        | (SchemaVectorPresenceManifest::Forbidden, None) => {}
    }
    if let (Some(values), Some(config)) = (node.dense_vector.as_ref(), dense_config) {
        validate_dense_vector(values, config).map_err(|error| {
            node_schema_violation(
                node,
                schema.label_id,
                schema.label_name.as_deref(),
                "dense_vector",
                "valid dense vector for DB config".to_string(),
                error.to_string(),
            )
        })?;
    }
    Ok(())
}

fn validate_sparse_vector_rule(
    node: &NodeRecord,
    schema: &CompiledNodeSchema,
    rule: &CompiledSparseVectorRule,
) -> Result<(), SchemaValidationFailure> {
    let vector = node.sparse_vector.as_ref();
    match (rule.presence, vector) {
        (SchemaVectorPresenceManifest::Required, None) => {
            return Err(node_schema_violation(
                node,
                schema.label_id,
                schema.label_name.as_deref(),
                "sparse_vector",
                "present sparse vector".to_string(),
                "absent".to_string(),
            ));
        }
        (SchemaVectorPresenceManifest::Forbidden, Some(_)) => {
            return Err(node_schema_violation(
                node,
                schema.label_id,
                schema.label_name.as_deref(),
                "sparse_vector",
                "absent sparse vector".to_string(),
                "present".to_string(),
            ));
        }
        (SchemaVectorPresenceManifest::Optional, _)
        | (SchemaVectorPresenceManifest::Required, Some(_))
        | (SchemaVectorPresenceManifest::Forbidden, None) => {}
    }

    let Some(vector) = vector else {
        return Ok(());
    };
    if let Some(min) = rule.min_entries {
        if vector.len() < min {
            return Err(node_schema_violation(
                node,
                schema.label_id,
                schema.label_name.as_deref(),
                "sparse_vector",
                format!("entry count >= {min}"),
                vector.len().to_string(),
            ));
        }
    }
    if let Some(max) = rule.max_entries {
        if vector.len() > max {
            return Err(node_schema_violation(
                node,
                schema.label_id,
                schema.label_name.as_deref(),
                "sparse_vector",
                format!("entry count <= {max}"),
                vector.len().to_string(),
            ));
        }
    }
    if let Some(max_dimension_id) = rule.max_dimension_id {
        if let Some((dimension_id, _)) = vector
            .iter()
            .find(|(dimension_id, _)| *dimension_id > max_dimension_id)
        {
            return Err(node_schema_violation(
                node,
                schema.label_id,
                schema.label_name.as_deref(),
                "sparse_vector",
                format!("dimension_id <= {max_dimension_id}"),
                dimension_id.to_string(),
            ));
        }
    }
    Ok(())
}

fn validate_node_closed_properties(
    node: &NodeRecord,
    applicable: &ApplicableNodeSchemas<'_>,
) -> Result<(), SchemaValidationFailure> {
    if !applicable
        .iter()
        .any(|schema| schema.additional_properties == SchemaAdditionalPropertiesManifest::Reject)
    {
        return Ok(());
    }
    let mut allowed = BTreeSet::new();
    for schema in applicable.iter() {
        for key in &schema.property_keys {
            allowed.insert(key.as_str());
        }
    }
    for key in node.props.keys() {
        if !allowed.contains(key.as_str()) {
            let schema = applicable.iter().find(|schema| {
                schema.additional_properties == SchemaAdditionalPropertiesManifest::Reject
            });
            let label_id = schema.map(|schema| schema.label_id).unwrap_or(0);
            let label_name = schema.and_then(|schema| schema.label_name.as_deref());
            return Err(node_schema_violation(
                node,
                label_id,
                label_name,
                &format!("properties.{key}"),
                "property declared by applicable node schema".to_string(),
                "undeclared property".to_string(),
            ));
        }
    }
    Ok(())
}

fn validate_edge_closed_properties(
    edge: &EdgeRecord,
    schema: &CompiledEdgeSchema,
) -> Result<(), SchemaValidationFailure> {
    if schema.additional_properties != SchemaAdditionalPropertiesManifest::Reject {
        return Ok(());
    }
    for key in edge.props.keys() {
        if schema.property_keys.binary_search(key).is_err() {
            return Err(edge_schema_violation(
                edge,
                schema.label_name.as_deref(),
                &format!("properties.{key}"),
                "property declared by edge schema".to_string(),
                "undeclared property".to_string(),
            ));
        }
    }
    Ok(())
}

fn validate_edge_validity_rule(
    edge: &EdgeRecord,
    label_name: Option<&str>,
    rule: &EdgeValiditySchemaManifestRule,
) -> Result<(), SchemaValidationFailure> {
    if rule.require_valid_from_before_valid_to && edge.valid_from >= edge.valid_to {
        return Err(edge_schema_violation(
            edge,
            label_name,
            "validity.valid_from",
            "valid_from < valid_to".to_string(),
            format!("valid_from {} valid_to {}", edge.valid_from, edge.valid_to),
        ));
    }
    if let Some(min) = rule.valid_from_min {
        if edge.valid_from < min {
            return Err(edge_schema_violation(
                edge,
                label_name,
                "validity.valid_from",
                format!("valid_from >= {min}"),
                edge.valid_from.to_string(),
            ));
        }
    }
    if let Some(max) = rule.valid_from_max {
        if edge.valid_from > max {
            return Err(edge_schema_violation(
                edge,
                label_name,
                "validity.valid_from",
                format!("valid_from <= {max}"),
                edge.valid_from.to_string(),
            ));
        }
    }
    if let Some(min) = rule.valid_to_min {
        if edge.valid_to < min {
            return Err(edge_schema_violation(
                edge,
                label_name,
                "validity.valid_to",
                format!("valid_to >= {min}"),
                edge.valid_to.to_string(),
            ));
        }
    }
    if let Some(max) = rule.valid_to_max {
        if edge.valid_to > max {
            return Err(edge_schema_violation(
                edge,
                label_name,
                "validity.valid_to",
                format!("valid_to <= {max}"),
                edge.valid_to.to_string(),
            ));
        }
    }
    if !rule.allow_open_ended_valid_to && edge.valid_to == i64::MAX {
        return Err(edge_schema_violation(
            edge,
            label_name,
            "validity.valid_to",
            "finite valid_to".to_string(),
            "open-ended i64::MAX".to_string(),
        ));
    }
    Ok(())
}

fn node_schema_violation(
    node: &NodeRecord,
    schema_label_id: u32,
    schema_label_name: Option<&str>,
    path: &str,
    expected: String,
    actual: String,
) -> SchemaValidationFailure {
    let label = schema_label_display(schema_label_id, schema_label_name);
    SchemaValidationFailure {
        path: path.to_string(),
        message: format!(
            "schema violation: node id {} label {label} path {path} expected {expected}; actual {actual}",
            node.id
        ),
    }
}

fn edge_schema_violation(
    edge: &EdgeRecord,
    schema_label_name: Option<&str>,
    path: &str,
    expected: String,
    actual: String,
) -> SchemaValidationFailure {
    let label = schema_label_display(edge.label_id, schema_label_name);
    SchemaValidationFailure {
        path: path.to_string(),
        message: format!(
            "schema violation: edge id {} label {label} path {path} expected {expected}; actual {actual}",
            edge.id
        ),
    }
}

fn schema_label_display(label_id: u32, label_name: Option<&str>) -> String {
    match label_name {
        Some(label_name) => format!("'{label_name}' (id {label_id})"),
        None => format!("id {label_id}"),
    }
}

fn prop_value_type_name(value: &PropValue) -> &'static str {
    match value {
        PropValue::Null => "null",
        PropValue::Bool(_) => "bool",
        PropValue::Int(_) => "int",
        PropValue::UInt(_) => "uint",
        PropValue::Float(_) => "float",
        PropValue::String(_) => "string",
        PropValue::Bytes(_) => "bytes",
        PropValue::Array(_) => "array",
        PropValue::Map(_) => "map",
    }
}

fn describe_prop_value(value: &PropValue) -> String {
    format!("{value:?}")
}

fn compile_label_names_by_id(tokens: &BTreeMap<String, u32>) -> HashMap<u32, String> {
    tokens
        .iter()
        .map(|(label, label_id)| (*label_id, label.clone()))
        .collect()
}

fn compile_property_rules(
    properties: &BTreeMap<String, PropertySchemaManifestRule>,
) -> Result<Vec<CompiledPropertyRule>, EngineError> {
    properties
        .iter()
        .map(|(key, rule)| {
            Ok(CompiledPropertyRule {
                key: key.clone(),
                required: rule.required,
                nullable: rule.nullable,
                types: rule.types.clone(),
                numeric_min: rule
                    .numeric_min
                    .as_ref()
                    .map(|bound| compile_numeric_bound("schema property numeric_min", bound))
                    .transpose()?,
                numeric_max: rule
                    .numeric_max
                    .as_ref()
                    .map(|bound| compile_numeric_bound("schema property numeric_max", bound))
                    .transpose()?,
                string_min_bytes: rule.string_min_bytes,
                string_max_bytes: rule.string_max_bytes,
                bytes_min_len: rule.bytes_min_len,
                bytes_max_len: rule.bytes_max_len,
                array_min_items: rule.array_min_items,
                array_max_items: rule.array_max_items,
                map_min_entries: rule.map_min_entries,
                map_max_entries: rule.map_max_entries,
                enum_values: rule
                    .enum_values
                    .iter()
                    .cloned()
                    .map(|value| CompiledEnumValue {
                        numeric_key: numeric_key(&value),
                        value,
                    })
                    .collect(),
            })
        })
        .collect()
}

fn compile_string_field_rule(rule: &StringFieldSchemaManifestRule) -> CompiledStringFieldRule {
    CompiledStringFieldRule {
        min_bytes: rule.min_bytes,
        max_bytes: rule.max_bytes,
        enum_values: rule.enum_values.clone(),
    }
}

fn compile_numeric_field_rule(
    rule: &NumericFieldSchemaManifestRule,
) -> Result<CompiledNumericFieldRule, EngineError> {
    Ok(CompiledNumericFieldRule {
        min: rule
            .min
            .as_ref()
            .map(|bound| compile_numeric_bound("schema numeric field min", bound))
            .transpose()?,
        max: rule
            .max
            .as_ref()
            .map(|bound| compile_numeric_bound("schema numeric field max", bound))
            .transpose()?,
        finite: rule.finite,
    })
}

fn compile_numeric_bound(
    context: &str,
    bound: &SchemaNumericBoundManifest,
) -> Result<CompiledNumericBound, EngineError> {
    let key = numeric_key(&bound.value).ok_or_else(|| {
        EngineError::ManifestError(format!(
            "invalid schema manifest: {context} must be a finite numeric scalar"
        ))
    })?;
    Ok(CompiledNumericBound {
        value: bound.value.clone(),
        key,
        inclusive: bound.inclusive,
    })
}

fn compile_node_label_rule(rule: &NodeLabelConstraintManifestRule) -> CompiledLabelRule {
    compile_label_rule_parts(&rule.all_of, &rule.any_of, &rule.none_of)
}

fn compile_endpoint_label_rule(rule: &EndpointLabelManifestRule) -> CompiledLabelRule {
    compile_label_rule_parts(&rule.all_of, &rule.any_of, &rule.none_of)
}

fn compile_label_rule_parts(all_of: &[u32], any_of: &[u32], none_of: &[u32]) -> CompiledLabelRule {
    CompiledLabelRule {
        all_of: sorted_deduped_label_ids(all_of),
        any_of: sorted_deduped_label_ids(any_of),
        none_of: sorted_deduped_label_ids(none_of),
    }
}

fn sorted_deduped_label_ids(label_ids: &[u32]) -> Vec<u32> {
    let mut ids = label_ids.to_vec();
    ids.sort_unstable();
    ids.dedup();
    ids
}

fn compile_dense_vector_rule(rule: &DenseVectorSchemaManifestRule) -> CompiledDenseVectorRule {
    CompiledDenseVectorRule {
        presence: rule.presence,
        dimension: rule.dimension,
    }
}

fn compile_sparse_vector_rule(rule: &SparseVectorSchemaManifestRule) -> CompiledSparseVectorRule {
    CompiledSparseVectorRule {
        presence: rule.presence,
        min_entries: rule.min_entries,
        max_entries: rule.max_entries,
        max_dimension_id: rule.max_dimension_id,
    }
}

fn validate_public_properties(
    context: &str,
    properties: &BTreeMap<String, PropertySchema>,
) -> Result<(), EngineError> {
    if properties.len() > MAX_SCHEMA_PROPERTIES_PER_LABEL {
        return Err(EngineError::InvalidOperation(format!(
            "invalid schema: {context} has too many properties"
        )));
    }
    for (key, rule) in properties {
        validate_property_key(context, key)?;
        validate_public_property_rule(context, rule)?;
    }
    Ok(())
}

fn validate_public_property_rule(context: &str, rule: &PropertySchema) -> Result<(), EngineError> {
    let min = rule.numeric_min.as_ref().map(public_bound_to_manifest);
    let max = rule.numeric_max.as_ref().map(public_bound_to_manifest);
    validate_manifest_numeric_bounds(context, min.as_ref(), max.as_ref())
        .map_err(manifest_error_to_invalid_operation)?;
    validate_min_max(rule.string_min_bytes, rule.string_max_bytes, context)?;
    validate_min_max(rule.bytes_min_len, rule.bytes_max_len, context)?;
    validate_min_max(rule.array_min_items, rule.array_max_items, context)?;
    validate_min_max(rule.map_min_entries, rule.map_max_entries, context)?;
    validate_enum_literals(context, &rule.enum_values).map_err(manifest_error_to_invalid_operation)
}

fn validate_public_string_field(
    context: &str,
    field: &StringFieldSchema,
) -> Result<(), EngineError> {
    validate_min_max(field.min_bytes, field.max_bytes, context)?;
    validate_public_string_enum_literals(context, &field.enum_values)
}

fn validate_public_numeric_field(
    context: &str,
    field: &NumericFieldSchema,
) -> Result<(), EngineError> {
    let min = field.min.as_ref().map(public_bound_to_manifest);
    let max = field.max.as_ref().map(public_bound_to_manifest);
    validate_manifest_numeric_bounds(context, min.as_ref(), max.as_ref())
        .map_err(manifest_error_to_invalid_operation)
}

fn validate_public_edge_validity(
    context: &str,
    validity: &EdgeValiditySchema,
) -> Result<(), EngineError> {
    validate_i64_min_max(
        validity.valid_from_min,
        validity.valid_from_max,
        "edge validity valid_from",
    )?;
    validate_i64_min_max(
        validity.valid_to_min,
        validity.valid_to_max,
        "edge validity valid_to",
    )?;
    if validity.require_valid_from_before_valid_to
        && strict_validity_ordering_is_impossible(validity.valid_from_min, validity.valid_to_max)
    {
        return Err(EngineError::InvalidOperation(format!(
            "invalid schema: {context} requires valid_from before valid_to but valid_from_min is not less than valid_to_max"
        )));
    }
    if !validity.allow_open_ended_valid_to && validity.valid_to_min == Some(i64::MAX) {
        return Err(EngineError::InvalidOperation(format!(
            "invalid schema: {context} forbids open-ended valid_to but valid_to_min is i64::MAX"
        )));
    }
    Ok(())
}

fn validate_public_label_constraint(
    context: &str,
    constraints: &NodeLabelConstraintSchema,
) -> Result<(), EngineError> {
    validate_public_label_names(context, &constraints.all_of)?;
    validate_public_label_names(context, &constraints.any_of)?;
    validate_public_label_names(context, &constraints.none_of)?;
    validate_public_required_forbidden_disjoint(context, &constraints.all_of, &constraints.none_of)
}

fn validate_public_endpoint_label_constraint(
    context: &str,
    constraints: &EndpointLabelSchema,
) -> Result<(), EngineError> {
    validate_public_label_names(context, &constraints.all_of)?;
    validate_public_label_names(context, &constraints.any_of)?;
    validate_public_label_names(context, &constraints.none_of)?;
    validate_public_required_forbidden_disjoint(context, &constraints.all_of, &constraints.none_of)
}

fn validate_public_label_names(context: &str, labels: &[String]) -> Result<(), EngineError> {
    if labels.len() > MAX_SCHEMA_REFERENCED_LABELS_PER_RULE {
        return Err(EngineError::InvalidOperation(format!(
            "invalid schema: {context} references too many labels"
        )));
    }
    let mut seen = HashSet::new();
    for label in labels {
        validate_label_token_name(label).map_err(|error| {
            EngineError::InvalidOperation(format!("invalid schema: {context}: {error}"))
        })?;
        if !seen.insert(label) {
            return Err(EngineError::InvalidOperation(format!(
                "invalid schema: {context} contains duplicate label '{label}'"
            )));
        }
    }
    Ok(())
}

fn validate_public_required_forbidden_disjoint(
    context: &str,
    all_of: &[String],
    none_of: &[String],
) -> Result<(), EngineError> {
    let required: HashSet<&str> = all_of.iter().map(String::as_str).collect();
    for label in none_of {
        if required.contains(label.as_str()) {
            return Err(EngineError::InvalidOperation(format!(
                "invalid schema: {context} requires and forbids label '{label}'"
            )));
        }
    }
    Ok(())
}

fn public_bound_to_manifest(bound: &SchemaNumericBound) -> SchemaNumericBoundManifest {
    SchemaNumericBoundManifest {
        value: bound.value.clone(),
        inclusive: bound.inclusive,
    }
}

pub(crate) fn node_schema_manifest_entry_from_public<F>(
    label_id: u32,
    schema_id: u64,
    revision: u64,
    created_at_ms: i64,
    updated_at_ms: i64,
    schema: &NodeSchema,
    mut resolve_node_label: F,
) -> Result<NodeSchemaManifestEntry, EngineError>
where
    F: FnMut(&str) -> Result<u32, EngineError>,
{
    validate_node_schema_shape(schema)?;
    let mut entry = NodeSchemaManifestEntry {
        schema_id,
        revision,
        label_id,
        created_at_ms,
        updated_at_ms,
        additional_properties: match schema.additional_properties {
            SchemaAdditionalProperties::Allow => SchemaAdditionalPropertiesManifest::Allow,
            SchemaAdditionalProperties::Reject => SchemaAdditionalPropertiesManifest::Reject,
        },
        properties: public_properties_to_manifest(&schema.properties),
        key: schema.key.as_ref().map(public_string_field_to_manifest),
        label_constraints: schema
            .label_constraints
            .as_ref()
            .map(|rule| public_node_label_constraint_to_manifest(rule, &mut resolve_node_label))
            .transpose()?,
        weight: schema.weight.as_ref().map(public_numeric_field_to_manifest),
        dense_vector: schema
            .dense_vector
            .as_ref()
            .map(public_dense_vector_to_manifest),
        sparse_vector: schema
            .sparse_vector
            .as_ref()
            .map(public_sparse_vector_to_manifest),
    };
    normalize_node_label_constraint_option(&mut entry.label_constraints);
    Ok(entry)
}

pub(crate) fn edge_schema_manifest_entry_from_public<F>(
    label_id: u32,
    schema_id: u64,
    revision: u64,
    created_at_ms: i64,
    updated_at_ms: i64,
    schema: &EdgeSchema,
    mut resolve_node_label: F,
) -> Result<EdgeSchemaManifestEntry, EngineError>
where
    F: FnMut(&str) -> Result<u32, EngineError>,
{
    validate_edge_schema_shape(schema)?;
    let mut entry = EdgeSchemaManifestEntry {
        schema_id,
        revision,
        label_id,
        created_at_ms,
        updated_at_ms,
        additional_properties: match schema.additional_properties {
            SchemaAdditionalProperties::Allow => SchemaAdditionalPropertiesManifest::Allow,
            SchemaAdditionalProperties::Reject => SchemaAdditionalPropertiesManifest::Reject,
        },
        properties: public_properties_to_manifest(&schema.properties),
        from: schema
            .from
            .as_ref()
            .map(|rule| public_endpoint_label_constraint_to_manifest(rule, &mut resolve_node_label))
            .transpose()?,
        to: schema
            .to
            .as_ref()
            .map(|rule| public_endpoint_label_constraint_to_manifest(rule, &mut resolve_node_label))
            .transpose()?,
        allow_self_loops: schema.allow_self_loops,
        weight: schema.weight.as_ref().map(public_numeric_field_to_manifest),
        validity: schema
            .validity
            .as_ref()
            .map(public_edge_validity_to_manifest),
    };
    normalize_endpoint_label_constraint_option(&mut entry.from);
    normalize_endpoint_label_constraint_option(&mut entry.to);
    Ok(entry)
}

pub(crate) fn node_schema_info_from_manifest<F>(
    entry: &NodeSchemaManifestEntry,
    mut node_label_name: F,
) -> Result<NodeSchemaInfo, EngineError>
where
    F: FnMut(u32) -> Option<String>,
{
    let label = node_label_name(entry.label_id).ok_or_else(|| {
        EngineError::ManifestError(format!(
            "node schema {} references missing node label label_id {}",
            entry.schema_id, entry.label_id
        ))
    })?;
    Ok(NodeSchemaInfo {
        label,
        schema: NodeSchema {
            additional_properties: match entry.additional_properties {
                SchemaAdditionalPropertiesManifest::Allow => SchemaAdditionalProperties::Allow,
                SchemaAdditionalPropertiesManifest::Reject => SchemaAdditionalProperties::Reject,
            },
            properties: manifest_properties_to_public(&entry.properties),
            key: entry.key.as_ref().map(manifest_string_field_to_public),
            label_constraints: entry
                .label_constraints
                .as_ref()
                .map(|rule| manifest_node_label_constraint_to_public(rule, &mut node_label_name))
                .transpose()?,
            weight: entry.weight.as_ref().map(manifest_numeric_field_to_public),
            dense_vector: entry
                .dense_vector
                .as_ref()
                .map(manifest_dense_vector_to_public),
            sparse_vector: entry
                .sparse_vector
                .as_ref()
                .map(manifest_sparse_vector_to_public),
        },
    })
}

pub(crate) fn edge_schema_info_from_manifest<N, E>(
    entry: &EdgeSchemaManifestEntry,
    mut node_label_name: N,
    mut edge_label_name: E,
) -> Result<EdgeSchemaInfo, EngineError>
where
    N: FnMut(u32) -> Option<String>,
    E: FnMut(u32) -> Option<String>,
{
    let label = edge_label_name(entry.label_id).ok_or_else(|| {
        EngineError::ManifestError(format!(
            "edge schema {} references missing edge label label_id {}",
            entry.schema_id, entry.label_id
        ))
    })?;
    Ok(EdgeSchemaInfo {
        label,
        schema: EdgeSchema {
            additional_properties: match entry.additional_properties {
                SchemaAdditionalPropertiesManifest::Allow => SchemaAdditionalProperties::Allow,
                SchemaAdditionalPropertiesManifest::Reject => SchemaAdditionalProperties::Reject,
            },
            properties: manifest_properties_to_public(&entry.properties),
            from: entry
                .from
                .as_ref()
                .map(|rule| {
                    manifest_endpoint_label_constraint_to_public(rule, &mut node_label_name)
                })
                .transpose()?,
            to: entry
                .to
                .as_ref()
                .map(|rule| {
                    manifest_endpoint_label_constraint_to_public(rule, &mut node_label_name)
                })
                .transpose()?,
            allow_self_loops: entry.allow_self_loops,
            weight: entry.weight.as_ref().map(manifest_numeric_field_to_public),
            validity: entry
                .validity
                .as_ref()
                .map(manifest_edge_validity_to_public),
        },
    })
}

fn public_properties_to_manifest(
    properties: &BTreeMap<String, PropertySchema>,
) -> BTreeMap<String, PropertySchemaManifestRule> {
    properties
        .iter()
        .map(|(key, rule)| {
            (
                key.clone(),
                PropertySchemaManifestRule {
                    required: rule.required,
                    nullable: rule.nullable,
                    types: rule
                        .types
                        .iter()
                        .map(|schema_type| public_value_type_to_manifest(*schema_type))
                        .collect(),
                    numeric_min: rule.numeric_min.as_ref().map(public_bound_to_manifest),
                    numeric_max: rule.numeric_max.as_ref().map(public_bound_to_manifest),
                    string_min_bytes: rule.string_min_bytes,
                    string_max_bytes: rule.string_max_bytes,
                    bytes_min_len: rule.bytes_min_len,
                    bytes_max_len: rule.bytes_max_len,
                    array_min_items: rule.array_min_items,
                    array_max_items: rule.array_max_items,
                    map_min_entries: rule.map_min_entries,
                    map_max_entries: rule.map_max_entries,
                    enum_values: rule.enum_values.clone(),
                },
            )
        })
        .collect()
}

fn manifest_properties_to_public(
    properties: &BTreeMap<String, PropertySchemaManifestRule>,
) -> BTreeMap<String, PropertySchema> {
    properties
        .iter()
        .map(|(key, rule)| {
            (
                key.clone(),
                PropertySchema {
                    required: rule.required,
                    nullable: rule.nullable,
                    types: rule
                        .types
                        .iter()
                        .map(|schema_type| manifest_value_type_to_public(*schema_type))
                        .collect(),
                    numeric_min: rule.numeric_min.as_ref().map(manifest_bound_to_public),
                    numeric_max: rule.numeric_max.as_ref().map(manifest_bound_to_public),
                    string_min_bytes: rule.string_min_bytes,
                    string_max_bytes: rule.string_max_bytes,
                    bytes_min_len: rule.bytes_min_len,
                    bytes_max_len: rule.bytes_max_len,
                    array_min_items: rule.array_min_items,
                    array_max_items: rule.array_max_items,
                    map_min_entries: rule.map_min_entries,
                    map_max_entries: rule.map_max_entries,
                    enum_values: rule.enum_values.clone(),
                },
            )
        })
        .collect()
}

fn public_value_type_to_manifest(schema_type: SchemaValueType) -> SchemaValueTypeManifest {
    match schema_type {
        SchemaValueType::Bool => SchemaValueTypeManifest::Bool,
        SchemaValueType::Int => SchemaValueTypeManifest::Int,
        SchemaValueType::UInt => SchemaValueTypeManifest::UInt,
        SchemaValueType::Float => SchemaValueTypeManifest::Float,
        SchemaValueType::Number => SchemaValueTypeManifest::Number,
        SchemaValueType::String => SchemaValueTypeManifest::String,
        SchemaValueType::Bytes => SchemaValueTypeManifest::Bytes,
        SchemaValueType::Array => SchemaValueTypeManifest::Array,
        SchemaValueType::Map => SchemaValueTypeManifest::Map,
    }
}

fn manifest_value_type_to_public(schema_type: SchemaValueTypeManifest) -> SchemaValueType {
    match schema_type {
        SchemaValueTypeManifest::Bool => SchemaValueType::Bool,
        SchemaValueTypeManifest::Int => SchemaValueType::Int,
        SchemaValueTypeManifest::UInt => SchemaValueType::UInt,
        SchemaValueTypeManifest::Float => SchemaValueType::Float,
        SchemaValueTypeManifest::Number => SchemaValueType::Number,
        SchemaValueTypeManifest::String => SchemaValueType::String,
        SchemaValueTypeManifest::Bytes => SchemaValueType::Bytes,
        SchemaValueTypeManifest::Array => SchemaValueType::Array,
        SchemaValueTypeManifest::Map => SchemaValueType::Map,
    }
}

fn manifest_bound_to_public(bound: &SchemaNumericBoundManifest) -> SchemaNumericBound {
    SchemaNumericBound {
        value: bound.value.clone(),
        inclusive: bound.inclusive,
    }
}

fn public_string_field_to_manifest(rule: &StringFieldSchema) -> StringFieldSchemaManifestRule {
    StringFieldSchemaManifestRule {
        min_bytes: rule.min_bytes,
        max_bytes: rule.max_bytes,
        enum_values: rule.enum_values.clone(),
    }
}

fn manifest_string_field_to_public(rule: &StringFieldSchemaManifestRule) -> StringFieldSchema {
    StringFieldSchema {
        min_bytes: rule.min_bytes,
        max_bytes: rule.max_bytes,
        enum_values: rule.enum_values.clone(),
    }
}

fn public_numeric_field_to_manifest(rule: &NumericFieldSchema) -> NumericFieldSchemaManifestRule {
    NumericFieldSchemaManifestRule {
        min: rule.min.as_ref().map(public_bound_to_manifest),
        max: rule.max.as_ref().map(public_bound_to_manifest),
        finite: rule.finite,
    }
}

fn manifest_numeric_field_to_public(rule: &NumericFieldSchemaManifestRule) -> NumericFieldSchema {
    NumericFieldSchema {
        min: rule.min.as_ref().map(manifest_bound_to_public),
        max: rule.max.as_ref().map(manifest_bound_to_public),
        finite: rule.finite,
    }
}

fn public_node_label_constraint_to_manifest<F>(
    rule: &NodeLabelConstraintSchema,
    resolve_node_label: &mut F,
) -> Result<NodeLabelConstraintManifestRule, EngineError>
where
    F: FnMut(&str) -> Result<u32, EngineError>,
{
    Ok(NodeLabelConstraintManifestRule {
        all_of: resolve_label_names(&rule.all_of, resolve_node_label)?,
        any_of: resolve_label_names(&rule.any_of, resolve_node_label)?,
        none_of: resolve_label_names(&rule.none_of, resolve_node_label)?,
    })
}

fn public_endpoint_label_constraint_to_manifest<F>(
    rule: &EndpointLabelSchema,
    resolve_node_label: &mut F,
) -> Result<EndpointLabelManifestRule, EngineError>
where
    F: FnMut(&str) -> Result<u32, EngineError>,
{
    Ok(EndpointLabelManifestRule {
        all_of: resolve_label_names(&rule.all_of, resolve_node_label)?,
        any_of: resolve_label_names(&rule.any_of, resolve_node_label)?,
        none_of: resolve_label_names(&rule.none_of, resolve_node_label)?,
    })
}

fn resolve_label_names<F>(labels: &[String], resolve_label: &mut F) -> Result<Vec<u32>, EngineError>
where
    F: FnMut(&str) -> Result<u32, EngineError>,
{
    labels
        .iter()
        .map(|label| resolve_label(label.as_str()))
        .collect()
}

fn manifest_node_label_constraint_to_public<F>(
    rule: &NodeLabelConstraintManifestRule,
    node_label_name: &mut F,
) -> Result<NodeLabelConstraintSchema, EngineError>
where
    F: FnMut(u32) -> Option<String>,
{
    Ok(NodeLabelConstraintSchema {
        all_of: manifest_label_ids_to_names(&rule.all_of, node_label_name)?,
        any_of: manifest_label_ids_to_names(&rule.any_of, node_label_name)?,
        none_of: manifest_label_ids_to_names(&rule.none_of, node_label_name)?,
    })
}

fn manifest_endpoint_label_constraint_to_public<F>(
    rule: &EndpointLabelManifestRule,
    node_label_name: &mut F,
) -> Result<EndpointLabelSchema, EngineError>
where
    F: FnMut(u32) -> Option<String>,
{
    Ok(EndpointLabelSchema {
        all_of: manifest_label_ids_to_names(&rule.all_of, node_label_name)?,
        any_of: manifest_label_ids_to_names(&rule.any_of, node_label_name)?,
        none_of: manifest_label_ids_to_names(&rule.none_of, node_label_name)?,
    })
}

fn manifest_label_ids_to_names<F>(
    label_ids: &[u32],
    label_name: &mut F,
) -> Result<Vec<String>, EngineError>
where
    F: FnMut(u32) -> Option<String>,
{
    label_ids
        .iter()
        .map(|&label_id| {
            label_name(label_id).ok_or_else(|| {
                EngineError::ManifestError(format!(
                    "schema references missing node label label_id {label_id}"
                ))
            })
        })
        .collect()
}

fn public_dense_vector_to_manifest(rule: &DenseVectorSchema) -> DenseVectorSchemaManifestRule {
    DenseVectorSchemaManifestRule {
        presence: public_vector_presence_to_manifest(rule.presence),
        dimension: rule.dimension.map(|dimension| dimension as u32),
    }
}

fn manifest_dense_vector_to_public(rule: &DenseVectorSchemaManifestRule) -> DenseVectorSchema {
    DenseVectorSchema {
        presence: manifest_vector_presence_to_public(rule.presence),
        dimension: rule.dimension.map(|dimension| dimension as usize),
    }
}

fn public_sparse_vector_to_manifest(rule: &SparseVectorSchema) -> SparseVectorSchemaManifestRule {
    SparseVectorSchemaManifestRule {
        presence: public_vector_presence_to_manifest(rule.presence),
        min_entries: rule.min_entries,
        max_entries: rule.max_entries,
        max_dimension_id: rule.max_dimension_id,
    }
}

fn manifest_sparse_vector_to_public(rule: &SparseVectorSchemaManifestRule) -> SparseVectorSchema {
    SparseVectorSchema {
        presence: manifest_vector_presence_to_public(rule.presence),
        min_entries: rule.min_entries,
        max_entries: rule.max_entries,
        max_dimension_id: rule.max_dimension_id,
    }
}

fn public_vector_presence_to_manifest(
    presence: SchemaVectorPresence,
) -> SchemaVectorPresenceManifest {
    match presence {
        SchemaVectorPresence::Optional => SchemaVectorPresenceManifest::Optional,
        SchemaVectorPresence::Required => SchemaVectorPresenceManifest::Required,
        SchemaVectorPresence::Forbidden => SchemaVectorPresenceManifest::Forbidden,
    }
}

fn manifest_vector_presence_to_public(
    presence: SchemaVectorPresenceManifest,
) -> SchemaVectorPresence {
    match presence {
        SchemaVectorPresenceManifest::Optional => SchemaVectorPresence::Optional,
        SchemaVectorPresenceManifest::Required => SchemaVectorPresence::Required,
        SchemaVectorPresenceManifest::Forbidden => SchemaVectorPresence::Forbidden,
    }
}

fn public_edge_validity_to_manifest(rule: &EdgeValiditySchema) -> EdgeValiditySchemaManifestRule {
    EdgeValiditySchemaManifestRule {
        require_valid_from_before_valid_to: rule.require_valid_from_before_valid_to,
        valid_from_min: rule.valid_from_min,
        valid_from_max: rule.valid_from_max,
        valid_to_min: rule.valid_to_min,
        valid_to_max: rule.valid_to_max,
        allow_open_ended_valid_to: rule.allow_open_ended_valid_to,
    }
}

fn manifest_edge_validity_to_public(rule: &EdgeValiditySchemaManifestRule) -> EdgeValiditySchema {
    EdgeValiditySchema {
        require_valid_from_before_valid_to: rule.require_valid_from_before_valid_to,
        valid_from_min: rule.valid_from_min,
        valid_from_max: rule.valid_from_max,
        valid_to_min: rule.valid_to_min,
        valid_to_max: rule.valid_to_max,
        allow_open_ended_valid_to: rule.allow_open_ended_valid_to,
    }
}

fn manifest_error_to_invalid_operation(error: EngineError) -> EngineError {
    match error {
        EngineError::ManifestError(message) => {
            let public_message =
                if let Some(rest) = message.strip_prefix("invalid schema manifest: ") {
                    format!("invalid schema: {rest}")
                } else {
                    message
                };
            EngineError::InvalidOperation(public_message)
        }
        other => other,
    }
}

fn normalize_node_label_constraint_option(
    constraint: &mut Option<NodeLabelConstraintManifestRule>,
) -> bool {
    let Some(rule) = constraint else {
        return false;
    };
    let mut dirty = false;
    dirty |= sort_dedup(&mut rule.all_of);
    dirty |= sort_dedup(&mut rule.any_of);
    dirty |= sort_dedup(&mut rule.none_of);
    if rule.all_of.is_empty() && rule.any_of.is_empty() && rule.none_of.is_empty() {
        *constraint = None;
        dirty = true;
    }
    dirty
}

fn normalize_endpoint_label_constraint_option(
    constraint: &mut Option<EndpointLabelManifestRule>,
) -> bool {
    let Some(rule) = constraint else {
        return false;
    };
    let mut dirty = false;
    dirty |= sort_dedup(&mut rule.all_of);
    dirty |= sort_dedup(&mut rule.any_of);
    dirty |= sort_dedup(&mut rule.none_of);
    if rule.all_of.is_empty() && rule.any_of.is_empty() && rule.none_of.is_empty() {
        *constraint = None;
        dirty = true;
    }
    dirty
}

fn sort_dedup(values: &mut Vec<u32>) -> bool {
    let before = values.clone();
    values.sort_unstable();
    values.dedup();
    *values != before
}

fn validate_manifest_entry_size<T: Serialize>(context: &str, entry: &T) -> Result<(), EngineError> {
    let encoded = serde_json::to_vec(entry).map_err(|error| {
        EngineError::ManifestError(format!(
            "invalid schema manifest: {context} entry cannot serialize: {error}"
        ))
    })?;
    if encoded.len() > MAX_SCHEMA_MANIFEST_BYTES_PER_ENTRY {
        return Err(EngineError::ManifestError(format!(
            "invalid schema manifest: {context} entry exceeds {} bytes",
            MAX_SCHEMA_MANIFEST_BYTES_PER_ENTRY
        )));
    }
    Ok(())
}

fn validate_target_label_id(
    context: &str,
    label_id: u32,
    valid_ids: &HashSet<u32>,
    catalog_name: &str,
) -> Result<(), EngineError> {
    if label_id == 0 {
        return Err(EngineError::ManifestError(format!(
            "invalid schema manifest: {context} uses reserved label_id 0"
        )));
    }
    if !valid_ids.contains(&label_id) {
        return Err(EngineError::ManifestError(format!(
            "invalid schema manifest: {context} label_id {label_id} is missing from {catalog_name}"
        )));
    }
    Ok(())
}

fn validate_manifest_properties(
    context: &str,
    properties: &BTreeMap<String, PropertySchemaManifestRule>,
) -> Result<(), EngineError> {
    if properties.len() > MAX_SCHEMA_PROPERTIES_PER_LABEL {
        return Err(EngineError::ManifestError(format!(
            "invalid schema manifest: {context} has too many properties"
        )));
    }
    for (key, rule) in properties {
        validate_property_key(context, key).map_err(manifest_error_to_manifest_error)?;
        validate_manifest_property_rule(context, rule)?;
    }
    Ok(())
}

fn validate_property_key(context: &str, key: &str) -> Result<(), EngineError> {
    if key.is_empty() {
        return Err(EngineError::InvalidOperation(format!(
            "invalid schema: {context} property key must not be empty"
        )));
    }
    if key.len() > MAX_SCHEMA_PROPERTY_KEY_BYTES {
        return Err(EngineError::InvalidOperation(format!(
            "invalid schema: {context} property key '{key}' exceeds {} UTF-8 bytes",
            MAX_SCHEMA_PROPERTY_KEY_BYTES
        )));
    }
    Ok(())
}

fn manifest_error_to_manifest_error(error: EngineError) -> EngineError {
    match error {
        EngineError::InvalidOperation(message) => EngineError::ManifestError(message),
        other => other,
    }
}

fn validate_manifest_property_rule(
    context: &str,
    rule: &PropertySchemaManifestRule,
) -> Result<(), EngineError> {
    validate_manifest_numeric_bounds(
        context,
        rule.numeric_min.as_ref(),
        rule.numeric_max.as_ref(),
    )?;
    validate_min_max_manifest(rule.string_min_bytes, rule.string_max_bytes, context)?;
    validate_min_max_manifest(rule.bytes_min_len, rule.bytes_max_len, context)?;
    validate_min_max_manifest(rule.array_min_items, rule.array_max_items, context)?;
    validate_min_max_manifest(rule.map_min_entries, rule.map_max_entries, context)?;
    validate_enum_literals(context, &rule.enum_values)
}

fn validate_manifest_string_field(
    context: &str,
    field: &StringFieldSchemaManifestRule,
) -> Result<(), EngineError> {
    validate_min_max_manifest(field.min_bytes, field.max_bytes, context)?;
    validate_manifest_string_enum_literals(context, &field.enum_values)
}

fn validate_manifest_numeric_bounds(
    context: &str,
    min: Option<&SchemaNumericBoundManifest>,
    max: Option<&SchemaNumericBoundManifest>,
) -> Result<(), EngineError> {
    if let Some(bound) = min {
        validate_numeric_bound(context, bound)?;
    }
    if let Some(bound) = max {
        validate_numeric_bound(context, bound)?;
    }
    let Some(min) = min else {
        return Ok(());
    };
    let Some(max) = max else {
        return Ok(());
    };
    let ordering = compare_numeric_prop_values(&min.value, &max.value).ok_or_else(|| {
        EngineError::ManifestError(format!(
            "invalid schema manifest: {context} numeric bounds must be finite numeric values"
        ))
    })?;
    if ordering.is_gt() || (ordering.is_eq() && !(min.inclusive && max.inclusive)) {
        return Err(EngineError::ManifestError(format!(
            "invalid schema manifest: {context} numeric bounds define an empty range"
        )));
    }
    Ok(())
}

fn validate_numeric_bound(
    context: &str,
    bound: &SchemaNumericBoundManifest,
) -> Result<(), EngineError> {
    if prop_value_contains_nonfinite_float(&bound.value)
        || numeric_range_sort_key_for_value(&bound.value).is_none()
    {
        return Err(EngineError::ManifestError(format!(
            "invalid schema manifest: {context} numeric bound must be a finite numeric scalar"
        )));
    }
    Ok(())
}

fn validate_enum_literals(context: &str, values: &[PropValue]) -> Result<(), EngineError> {
    if values.len() > MAX_SCHEMA_ENUM_VALUES_PER_PROPERTY {
        return Err(EngineError::ManifestError(format!(
            "invalid schema manifest: {context} enum has too many values"
        )));
    }
    let mut total_bytes = 0usize;
    for value in values {
        if prop_value_contains_nonfinite_float(value) {
            return Err(EngineError::ManifestError(format!(
                "invalid schema manifest: {context} enum contains a non-finite float"
            )));
        }
        let encoded = serde_json::to_vec(value).map_err(|error| {
            EngineError::ManifestError(format!(
                "invalid schema manifest: {context} enum value cannot serialize: {error}"
            ))
        })?;
        total_bytes = total_bytes.saturating_add(encoded.len());
    }
    if total_bytes > MAX_SCHEMA_ENUM_LITERAL_BYTES_PER_PROPERTY {
        return Err(EngineError::ManifestError(format!(
            "invalid schema manifest: {context} enum literals exceed {} bytes",
            MAX_SCHEMA_ENUM_LITERAL_BYTES_PER_PROPERTY
        )));
    }
    Ok(())
}

fn validate_manifest_string_enum_literals(
    context: &str,
    values: &[String],
) -> Result<(), EngineError> {
    if values.len() > MAX_SCHEMA_ENUM_VALUES_PER_PROPERTY {
        return Err(EngineError::ManifestError(format!(
            "invalid schema manifest: {context} enum has too many values"
        )));
    }
    let mut total_bytes = 0usize;
    for value in values {
        let encoded = serde_json::to_vec(value).map_err(|error| {
            EngineError::ManifestError(format!(
                "invalid schema manifest: {context} enum value cannot serialize: {error}"
            ))
        })?;
        total_bytes = total_bytes.saturating_add(encoded.len());
    }
    if total_bytes > MAX_SCHEMA_ENUM_LITERAL_BYTES_PER_PROPERTY {
        return Err(EngineError::ManifestError(format!(
            "invalid schema manifest: {context} enum literals exceed {} bytes",
            MAX_SCHEMA_ENUM_LITERAL_BYTES_PER_PROPERTY
        )));
    }
    Ok(())
}

fn validate_public_string_enum_literals(
    context: &str,
    values: &[String],
) -> Result<(), EngineError> {
    if values.len() > MAX_SCHEMA_ENUM_VALUES_PER_PROPERTY {
        return Err(EngineError::InvalidOperation(format!(
            "invalid schema: {context} enum has too many values"
        )));
    }
    let mut total_bytes = 0usize;
    for value in values {
        let encoded = serde_json::to_vec(value).map_err(|error| {
            EngineError::InvalidOperation(format!(
                "invalid schema: {context} enum value cannot serialize: {error}"
            ))
        })?;
        total_bytes = total_bytes.saturating_add(encoded.len());
    }
    if total_bytes > MAX_SCHEMA_ENUM_LITERAL_BYTES_PER_PROPERTY {
        return Err(EngineError::InvalidOperation(format!(
            "invalid schema: {context} enum literals exceed {} bytes",
            MAX_SCHEMA_ENUM_LITERAL_BYTES_PER_PROPERTY
        )));
    }
    Ok(())
}

fn validate_manifest_edge_validity(
    context: &str,
    validity: &EdgeValiditySchemaManifestRule,
) -> Result<(), EngineError> {
    validate_i64_min_max_manifest(
        validity.valid_from_min,
        validity.valid_from_max,
        "edge validity valid_from",
    )?;
    validate_i64_min_max_manifest(
        validity.valid_to_min,
        validity.valid_to_max,
        "edge validity valid_to",
    )?;
    if validity.require_valid_from_before_valid_to
        && strict_validity_ordering_is_impossible(validity.valid_from_min, validity.valid_to_max)
    {
        return Err(EngineError::ManifestError(format!(
            "invalid schema manifest: {context} requires valid_from before valid_to but valid_from_min is not less than valid_to_max"
        )));
    }
    if !validity.allow_open_ended_valid_to && validity.valid_to_min == Some(i64::MAX) {
        return Err(EngineError::ManifestError(format!(
            "invalid schema manifest: {context} forbids open-ended valid_to but valid_to_min is i64::MAX"
        )));
    }
    Ok(())
}

fn strict_validity_ordering_is_impossible(
    valid_from_min: Option<i64>,
    valid_to_max: Option<i64>,
) -> bool {
    valid_from_min.unwrap_or(i64::MIN) >= valid_to_max.unwrap_or(i64::MAX)
}

fn prop_value_contains_nonfinite_float(value: &PropValue) -> bool {
    match value {
        PropValue::Float(value) => !value.is_finite(),
        PropValue::Array(values) => values.iter().any(prop_value_contains_nonfinite_float),
        PropValue::Map(values) => values.values().any(prop_value_contains_nonfinite_float),
        PropValue::Null
        | PropValue::Bool(_)
        | PropValue::Int(_)
        | PropValue::UInt(_)
        | PropValue::String(_)
        | PropValue::Bytes(_) => false,
    }
}

fn validate_node_label_constraint_manifest(
    context: &str,
    constraints: &NodeLabelConstraintManifestRule,
    node_label_ids: &HashSet<u32>,
) -> Result<(), EngineError> {
    validate_manifest_label_refs(context, "all_of", &constraints.all_of, node_label_ids)?;
    validate_manifest_label_refs(context, "any_of", &constraints.any_of, node_label_ids)?;
    validate_manifest_label_refs(context, "none_of", &constraints.none_of, node_label_ids)?;
    validate_manifest_required_forbidden_disjoint(
        context,
        &constraints.all_of,
        &constraints.none_of,
    )
}

fn validate_endpoint_label_manifest(
    context: &str,
    constraints: &EndpointLabelManifestRule,
    node_label_ids: &HashSet<u32>,
) -> Result<(), EngineError> {
    validate_manifest_label_refs(context, "all_of", &constraints.all_of, node_label_ids)?;
    validate_manifest_label_refs(context, "any_of", &constraints.any_of, node_label_ids)?;
    validate_manifest_label_refs(context, "none_of", &constraints.none_of, node_label_ids)?;
    validate_manifest_required_forbidden_disjoint(
        context,
        &constraints.all_of,
        &constraints.none_of,
    )
}

fn validate_manifest_label_refs(
    context: &str,
    field: &str,
    label_ids: &[u32],
    valid_ids: &HashSet<u32>,
) -> Result<(), EngineError> {
    if label_ids.len() > MAX_SCHEMA_REFERENCED_LABELS_PER_RULE {
        return Err(EngineError::ManifestError(format!(
            "invalid schema manifest: {context}.{field} references too many labels"
        )));
    }
    for &label_id in label_ids {
        if label_id == 0 {
            return Err(EngineError::ManifestError(format!(
                "invalid schema manifest: {context}.{field} uses reserved label_id 0"
            )));
        }
        if !valid_ids.contains(&label_id) {
            return Err(EngineError::ManifestError(format!(
                "invalid schema manifest: {context}.{field} references missing node label_id {label_id}"
            )));
        }
    }
    Ok(())
}

fn validate_manifest_required_forbidden_disjoint(
    context: &str,
    all_of: &[u32],
    none_of: &[u32],
) -> Result<(), EngineError> {
    let required: HashSet<u32> = all_of.iter().copied().collect();
    for &label_id in none_of {
        if required.contains(&label_id) {
            return Err(EngineError::ManifestError(format!(
                "invalid schema manifest: {context} requires and forbids label_id {label_id}"
            )));
        }
    }
    Ok(())
}

fn validate_min_max(
    min: Option<usize>,
    max: Option<usize>,
    context: &str,
) -> Result<(), EngineError> {
    if min.zip(max).is_some_and(|(min, max)| min > max) {
        return Err(EngineError::InvalidOperation(format!(
            "invalid schema: {context} min exceeds max"
        )));
    }
    Ok(())
}

fn validate_min_max_manifest(
    min: Option<usize>,
    max: Option<usize>,
    context: &str,
) -> Result<(), EngineError> {
    if min.zip(max).is_some_and(|(min, max)| min > max) {
        return Err(EngineError::ManifestError(format!(
            "invalid schema manifest: {context} min exceeds max"
        )));
    }
    Ok(())
}

fn validate_i64_min_max(
    min: Option<i64>,
    max: Option<i64>,
    context: &str,
) -> Result<(), EngineError> {
    if min.zip(max).is_some_and(|(min, max)| min > max) {
        return Err(EngineError::InvalidOperation(format!(
            "invalid schema: {context} min exceeds max"
        )));
    }
    Ok(())
}

fn validate_i64_min_max_manifest(
    min: Option<i64>,
    max: Option<i64>,
    context: &str,
) -> Result<(), EngineError> {
    if min.zip(max).is_some_and(|(min, max)| min > max) {
        return Err(EngineError::ManifestError(format!(
            "invalid schema manifest: {context} min exceeds max"
        )));
    }
    Ok(())
}

fn validate_nonzero_option_usize(value: Option<usize>, context: &str) -> Result<(), EngineError> {
    if value == Some(0) {
        return Err(EngineError::InvalidOperation(format!(
            "invalid schema: {context} must be nonzero"
        )));
    }
    Ok(())
}

fn insert_endpoint_relevant_labels(rule: &EndpointLabelManifestRule, target: &mut HashSet<u32>) {
    target.extend(rule.all_of.iter().copied());
    target.extend(rule.any_of.iter().copied());
    target.extend(rule.none_of.iter().copied());
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::manifest::default_manifest;
    use crate::types::{
        DenseMetric, EdgeSchemaManifestEntry, EdgeValiditySchemaManifestRule,
        EndpointLabelManifestRule, HnswConfig, NodeLabelConstraintManifestRule,
        NodeSchemaManifestEntry, SchemaAdditionalPropertiesManifest, StringFieldSchemaManifestRule,
    };

    fn manifest_with_labels() -> ManifestState {
        let mut manifest = default_manifest();
        manifest.node_label_tokens.insert("Person".to_string(), 1);
        manifest.node_label_tokens.insert("Team".to_string(), 2);
        manifest.node_label_tokens.insert("Admin".to_string(), 3);
        manifest.node_label_tokens.insert("Org".to_string(), 4);
        manifest.node_label_tokens.insert("Blocked".to_string(), 5);
        manifest.next_node_label_id = 6;
        manifest.edge_label_tokens.insert("KNOWS".to_string(), 1);
        manifest.edge_label_tokens.insert("LIKES".to_string(), 2);
        manifest.next_edge_label_id = 3;
        manifest
    }

    fn node_schema(label_id: u32) -> NodeSchemaManifestEntry {
        NodeSchemaManifestEntry {
            schema_id: u64::from(label_id),
            revision: 1,
            label_id,
            created_at_ms: 10,
            updated_at_ms: 10,
            additional_properties: SchemaAdditionalPropertiesManifest::Allow,
            properties: BTreeMap::new(),
            key: None,
            label_constraints: None,
            weight: None,
            dense_vector: None,
            sparse_vector: None,
        }
    }

    fn edge_schema(label_id: u32) -> EdgeSchemaManifestEntry {
        EdgeSchemaManifestEntry {
            schema_id: 100 + u64::from(label_id),
            revision: 1,
            label_id,
            created_at_ms: 10,
            updated_at_ms: 10,
            additional_properties: SchemaAdditionalPropertiesManifest::Allow,
            properties: BTreeMap::new(),
            from: None,
            to: None,
            allow_self_loops: true,
            weight: None,
            validity: None,
        }
    }

    fn property_rule() -> PropertySchemaManifestRule {
        PropertySchemaManifestRule {
            required: false,
            nullable: true,
            types: Vec::new(),
            numeric_min: None,
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
        }
    }

    fn numeric_bound(value: PropValue, inclusive: bool) -> SchemaNumericBoundManifest {
        SchemaNumericBoundManifest { value, inclusive }
    }

    fn node_catalog_with(mut schemas: Vec<NodeSchemaManifestEntry>) -> RuntimeSchemaCatalog {
        let mut manifest = manifest_with_labels();
        manifest.next_schema_id = schemas
            .iter()
            .map(|schema| schema.schema_id)
            .max()
            .unwrap_or(0)
            + 1;
        manifest.node_schemas.append(&mut schemas);
        RuntimeSchemaCatalog::from_manifest(&manifest).unwrap()
    }

    fn edge_catalog_with(mut schemas: Vec<EdgeSchemaManifestEntry>) -> RuntimeSchemaCatalog {
        let mut manifest = manifest_with_labels();
        manifest.next_schema_id = schemas
            .iter()
            .map(|schema| schema.schema_id)
            .max()
            .unwrap_or(0)
            + 1;
        manifest.edge_schemas.append(&mut schemas);
        RuntimeSchemaCatalog::from_manifest(&manifest).unwrap()
    }

    fn empty_schema_report() -> SchemaValidationReport {
        SchemaValidationReport {
            checked_records: 0,
            violation_count: 0,
            violations: Vec::new(),
            truncated: false,
            scan_limit_hit: false,
        }
    }

    #[test]
    fn graph_schema_set_options_default_matches_single_target_publish_defaults() {
        assert_eq!(
            GraphSchemaSetOptions::default(),
            GraphSchemaSetOptions {
                max_violations: 1,
                chunk_size: 4096,
                scan_limit: None,
            }
        );
    }

    #[test]
    fn graph_schema_check_options_default_matches_single_target_check_defaults() {
        assert_eq!(
            GraphSchemaCheckOptions::default(),
            GraphSchemaCheckOptions {
                max_violations: 100,
                chunk_size: 4096,
                scan_limit: None,
            }
        );
    }

    #[test]
    fn graph_schema_deserializes_missing_schema_lists_as_empty() {
        let graph_schema: GraphSchema = serde_json::from_str("{}").unwrap();
        assert_eq!(graph_schema, GraphSchema::default());
    }

    #[test]
    fn graph_schema_public_dtos_construct_node_and_edge_schema_info() {
        let node_info = NodeSchemaInfo {
            label: "Person".to_string(),
            schema: NodeSchema {
                properties: BTreeMap::from([("name".to_string(), PropertySchema::default())]),
                ..Default::default()
            },
        };
        let edge_info = EdgeSchemaInfo {
            label: "KNOWS".to_string(),
            schema: EdgeSchema {
                properties: BTreeMap::from([("since".to_string(), PropertySchema::default())]),
                ..Default::default()
            },
        };
        let graph_schema = GraphSchema {
            node_schemas: vec![node_info.clone()],
            edge_schemas: vec![edge_info.clone()],
        };
        assert_eq!(graph_schema.node_schemas, vec![node_info.clone()]);
        assert_eq!(graph_schema.edge_schemas, vec![edge_info.clone()]);

        let entry = GraphSchemaValidationReportEntry {
            target_kind: SchemaTargetKind::Node,
            label: node_info.label.clone(),
            report: empty_schema_report(),
        };
        let validation = GraphSchemaCheckReport {
            operation: GraphSchemaOperationKind::CheckSet,
            entries: vec![entry],
            checked_records: 0,
            violation_count: 0,
            truncated: false,
            scan_limit_hit: false,
        };
        let publish_result = GraphSchemaPublishResult {
            operation: GraphSchemaOperationKind::Set,
            node_schemas: vec![node_info],
            edge_schemas: vec![edge_info],
            validation,
            targets_published: 2,
            targets_dropped: 0,
            drop_targets: Vec::new(),
            node_schemas_dropped: 0,
            edge_schemas_dropped: 0,
        };
        assert_eq!(publish_result.operation, GraphSchemaOperationKind::Set);
        assert_eq!(publish_result.targets_published, 2);
    }

    #[test]
    fn graph_schema_operation_supports_node_and_edge_set_and_drop() {
        let operations = [
            GraphSchemaOperation::SetNode {
                label: "Person".to_string(),
                schema: NodeSchema::default(),
            },
            GraphSchemaOperation::SetEdge {
                label: "KNOWS".to_string(),
                schema: EdgeSchema::default(),
            },
            GraphSchemaOperation::DropNode {
                label: "Archived".to_string(),
            },
            GraphSchemaOperation::DropEdge {
                label: "OLD_EDGE".to_string(),
            },
        ];

        assert!(matches!(
            &operations[0],
            GraphSchemaOperation::SetNode { label, .. } if label == "Person"
        ));
        assert!(matches!(
            &operations[1],
            GraphSchemaOperation::SetEdge { label, .. } if label == "KNOWS"
        ));
        assert!(matches!(
            &operations[2],
            GraphSchemaOperation::DropNode { label } if label == "Archived"
        ));
        assert!(matches!(
            &operations[3],
            GraphSchemaOperation::DropEdge { label } if label == "OLD_EDGE"
        ));
    }

    fn node_with_labels(label_ids: &[u32]) -> NodeRecord {
        NodeRecord {
            id: 10,
            label_ids: NodeLabelSet::from_canonical_ids(label_ids).unwrap(),
            key: "node-key".to_string(),
            props: BTreeMap::new(),
            created_at: 1,
            updated_at: 1,
            weight: 1.0,
            dense_vector: None,
            sparse_vector: None,
            last_write_seq: 1,
        }
    }

    fn edge_with_label(label_id: u32) -> EdgeRecord {
        EdgeRecord {
            id: 20,
            from: 1,
            to: 2,
            label_id,
            props: BTreeMap::new(),
            created_at: 1,
            updated_at: 1,
            weight: 1.0,
            valid_from: 0,
            valid_to: i64::MAX,
            last_write_seq: 1,
        }
    }

    fn props(entries: Vec<(&str, PropValue)>) -> BTreeMap<String, PropValue> {
        entries
            .into_iter()
            .map(|(key, value)| (key.to_string(), value))
            .collect()
    }

    fn dense_config(dimension: u32) -> DenseVectorConfig {
        DenseVectorConfig {
            dimension,
            metric: DenseMetric::Cosine,
            hnsw: HnswConfig::default(),
        }
    }

    fn assert_schema_error(result: Result<(), EngineError>, needle: &str) {
        let error = result.unwrap_err().to_string();
        assert!(
            error.contains(needle),
            "expected error to contain {needle:?}, got {error:?}"
        );
    }

    fn assert_node_property_ok(rule: PropertySchemaManifestRule, value: Option<PropValue>) {
        let mut schema = node_schema(1);
        schema.properties.insert("p".to_string(), rule);
        let catalog = node_catalog_with(vec![schema]);
        let mut node = node_with_labels(&[1]);
        if let Some(value) = value {
            node.props = props(vec![("p", value)]);
        }
        catalog.validate_node_record(&node, None).unwrap();
    }

    fn assert_node_property_err(
        rule: PropertySchemaManifestRule,
        value: Option<PropValue>,
        needle: &str,
    ) {
        let mut schema = node_schema(1);
        schema.properties.insert("p".to_string(), rule);
        let catalog = node_catalog_with(vec![schema]);
        let mut node = node_with_labels(&[1]);
        if let Some(value) = value {
            node.props = props(vec![("p", value)]);
        }
        assert_schema_error(catalog.validate_node_record(&node, None), needle);
    }

    #[test]
    fn schema_public_dto_defaults_match_spec() {
        assert_eq!(
            SchemaAdditionalProperties::default(),
            SchemaAdditionalProperties::Allow
        );
        assert_eq!(
            SchemaVectorPresence::default(),
            SchemaVectorPresence::Optional
        );

        let property = PropertySchema::default();
        assert!(!property.required);
        assert!(property.nullable);
        assert!(property.types.is_empty());
        assert!(property.enum_values.is_empty());

        assert!(NumericFieldSchema::default().finite);
        assert_eq!(
            EdgeValiditySchema::default(),
            EdgeValiditySchema {
                require_valid_from_before_valid_to: false,
                valid_from_min: None,
                valid_from_max: None,
                valid_to_min: None,
                valid_to_max: None,
                allow_open_ended_valid_to: true,
            }
        );
        assert!(EdgeSchema::default().allow_self_loops);
    }

    #[test]
    fn property_required_optional_and_null_semantics_are_validated() {
        let mut required = property_rule();
        required.required = true;
        assert_node_property_err(
            required,
            None,
            "path properties.p expected required property",
        );

        let mut optional = property_rule();
        optional.types = vec![SchemaValueTypeManifest::String];
        optional.enum_values = vec![PropValue::String("allowed".to_string())];
        optional.numeric_min = Some(numeric_bound(PropValue::Int(5), true));
        assert_node_property_ok(optional, None);

        let mut nullable = property_rule();
        nullable.types = vec![SchemaValueTypeManifest::String];
        nullable.enum_values = vec![PropValue::String("allowed".to_string())];
        nullable.numeric_min = Some(numeric_bound(PropValue::Int(5), true));
        assert_node_property_ok(nullable, Some(PropValue::Null));

        let mut non_nullable = property_rule();
        non_nullable.nullable = false;
        non_nullable.enum_values = vec![PropValue::Null];
        assert_node_property_err(
            non_nullable,
            Some(PropValue::Null),
            "expected non-null value",
        );
    }

    #[test]
    fn property_schema_value_types_match_expected_prop_variants() {
        let matching = [
            (SchemaValueTypeManifest::Bool, PropValue::Bool(true)),
            (SchemaValueTypeManifest::Int, PropValue::Int(-1)),
            (SchemaValueTypeManifest::UInt, PropValue::UInt(1)),
            (
                SchemaValueTypeManifest::Float,
                PropValue::Float(f64::INFINITY),
            ),
            (SchemaValueTypeManifest::Number, PropValue::Float(1.5)),
            (
                SchemaValueTypeManifest::String,
                PropValue::String("x".to_string()),
            ),
            (SchemaValueTypeManifest::Bytes, PropValue::Bytes(vec![1, 2])),
            (
                SchemaValueTypeManifest::Array,
                PropValue::Array(vec![PropValue::Int(1)]),
            ),
            (
                SchemaValueTypeManifest::Map,
                PropValue::Map(props(vec![("k", PropValue::Bool(true))])),
            ),
        ];
        for (schema_type, value) in matching {
            let mut rule = property_rule();
            rule.types = vec![schema_type];
            assert_node_property_ok(rule, Some(value));
        }

        let mut number = property_rule();
        number.types = vec![SchemaValueTypeManifest::Number];
        assert_node_property_err(
            number,
            Some(PropValue::Float(f64::INFINITY)),
            "expected type in [Number]",
        );

        let mut float = property_rule();
        float.types = vec![SchemaValueTypeManifest::Float];
        assert_node_property_ok(float, Some(PropValue::Float(f64::NAN)));
    }

    #[test]
    fn property_numeric_bounds_use_existing_numeric_semantics() {
        let mut inclusive = property_rule();
        inclusive.numeric_min = Some(numeric_bound(PropValue::Int(1), true));
        inclusive.numeric_max = Some(numeric_bound(PropValue::UInt(10), true));
        assert_node_property_ok(inclusive.clone(), Some(PropValue::Int(1)));
        assert_node_property_ok(inclusive.clone(), Some(PropValue::UInt(10)));
        assert_node_property_ok(inclusive.clone(), Some(PropValue::Float(5.5)));
        assert_node_property_err(
            inclusive.clone(),
            Some(PropValue::Int(0)),
            "numeric value >=",
        );
        assert_node_property_err(inclusive, Some(PropValue::UInt(11)), "numeric value <=");

        let mut exclusive = property_rule();
        exclusive.numeric_min = Some(numeric_bound(PropValue::Int(1), false));
        exclusive.numeric_max = Some(numeric_bound(PropValue::Int(2), false));
        assert_node_property_ok(exclusive.clone(), Some(PropValue::Float(1.5)));
        assert_node_property_err(
            exclusive.clone(),
            Some(PropValue::UInt(1)),
            "numeric value >",
        );
        assert_node_property_err(exclusive, Some(PropValue::Float(2.0)), "numeric value <");

        let mut bounded = property_rule();
        bounded.numeric_min = Some(numeric_bound(PropValue::Int(0), true));
        assert_node_property_err(
            bounded,
            Some(PropValue::String("not numeric".to_string())),
            "expected finite numeric value",
        );
    }

    #[test]
    fn property_enum_matching_uses_numeric_semantics_and_structural_values() {
        let mut numeric_enum = property_rule();
        numeric_enum.enum_values = vec![PropValue::Int(1)];
        assert_node_property_ok(numeric_enum.clone(), Some(PropValue::UInt(1)));
        assert_node_property_ok(numeric_enum.clone(), Some(PropValue::Float(1.0)));
        assert_node_property_err(
            numeric_enum,
            Some(PropValue::Float(1.25)),
            "expected one of 1 enum values",
        );

        let mut map = BTreeMap::new();
        map.insert(
            "k".to_string(),
            PropValue::Array(vec![PropValue::Bool(true)]),
        );
        let structural_values = vec![
            PropValue::String("x".to_string()),
            PropValue::Bool(false),
            PropValue::Bytes(vec![1, 2, 3]),
            PropValue::Array(vec![PropValue::UInt(7)]),
            PropValue::Map(map),
        ];
        for value in structural_values {
            let mut rule = property_rule();
            rule.enum_values = vec![value.clone()];
            assert_node_property_ok(rule, Some(value));
        }

        let mut nested_array_zero = property_rule();
        nested_array_zero.enum_values = vec![PropValue::Array(vec![PropValue::Float(0.0)])];
        assert_node_property_ok(
            nested_array_zero.clone(),
            Some(PropValue::Array(vec![PropValue::Float(0.0)])),
        );
        assert_node_property_err(
            nested_array_zero,
            Some(PropValue::Array(vec![PropValue::Float(-0.0)])),
            "expected one of 1 enum values",
        );

        let mut zero_map = BTreeMap::new();
        zero_map.insert("z".to_string(), PropValue::Float(0.0));
        let mut negative_zero_map = BTreeMap::new();
        negative_zero_map.insert("z".to_string(), PropValue::Float(-0.0));
        let mut nested_map_zero = property_rule();
        nested_map_zero.enum_values = vec![PropValue::Map(zero_map.clone())];
        assert_node_property_ok(nested_map_zero.clone(), Some(PropValue::Map(zero_map)));
        assert_node_property_err(
            nested_map_zero,
            Some(PropValue::Map(negative_zero_map)),
            "expected one of 1 enum values",
        );
    }

    #[test]
    fn property_top_level_type_specific_bounds_are_validated() {
        let mut strings = property_rule();
        strings.string_min_bytes = Some(2);
        strings.string_max_bytes = Some(4);
        assert_node_property_ok(strings.clone(), Some(PropValue::String("é".to_string())));
        assert_node_property_err(
            strings.clone(),
            Some(PropValue::String("a".to_string())),
            "UTF-8 byte length >=",
        );
        assert_node_property_err(
            strings,
            Some(PropValue::String("abcde".to_string())),
            "UTF-8 byte length <=",
        );

        let mut bytes = property_rule();
        bytes.bytes_min_len = Some(2);
        bytes.bytes_max_len = Some(3);
        assert_node_property_ok(bytes.clone(), Some(PropValue::Bytes(vec![1, 2])));
        assert_node_property_err(
            bytes,
            Some(PropValue::Bytes(vec![1, 2, 3, 4])),
            "byte length <=",
        );

        let mut arrays = property_rule();
        arrays.array_min_items = Some(1);
        arrays.array_max_items = Some(2);
        assert_node_property_ok(
            arrays.clone(),
            Some(PropValue::Array(vec![PropValue::Int(1)])),
        );
        assert_node_property_err(
            arrays,
            Some(PropValue::Array(Vec::new())),
            "array item count >=",
        );

        let mut maps = property_rule();
        maps.map_min_entries = Some(1);
        maps.map_max_entries = Some(1);
        assert_node_property_ok(
            maps.clone(),
            Some(PropValue::Map(props(vec![("a", PropValue::Int(1))]))),
        );
        assert_node_property_err(
            maps,
            Some(PropValue::Map(props(vec![
                ("a", PropValue::Int(1)),
                ("b", PropValue::Int(2)),
            ]))),
            "map entry count <=",
        );
    }

    #[test]
    fn runtime_schema_catalog_empty_manifest_is_empty() {
        let manifest = default_manifest();
        let catalog = RuntimeSchemaCatalog::from_manifest(&manifest).unwrap();
        assert!(catalog.is_empty());
        assert!(!catalog.has_node_schemas);
        assert!(!catalog.has_edge_schemas);
        assert!(!catalog.node_has_applicable_schema(&NodeLabelSet::single(1).unwrap()));
        assert!(!catalog.edge_has_applicable_schema(1));
        assert!(!catalog.label_change_may_affect_endpoint_rules(
            Some(&NodeLabelSet::single(1).unwrap()),
            Some(&NodeLabelSet::single(2).unwrap()),
            false
        ));
    }

    #[test]
    fn runtime_schema_catalog_tracks_node_schemas_by_numeric_label_id() {
        let mut manifest = manifest_with_labels();
        manifest.node_schemas.push(node_schema(1));
        manifest.next_schema_id = 2;

        let catalog = RuntimeSchemaCatalog::from_manifest(&manifest).unwrap();
        assert!(!catalog.is_empty());
        assert!(catalog.has_node_schemas);
        assert!(catalog.node_schema_label_ids.contains(&1));
        assert!(catalog.node_by_label_id.contains_key(&1));
        assert!(
            catalog.node_has_applicable_schema(&NodeLabelSet::from_canonical_ids(&[1, 2]).unwrap())
        );
        assert!(!catalog.node_has_applicable_schema(&NodeLabelSet::single(2).unwrap()));
    }

    #[test]
    fn runtime_schema_catalog_tracks_edge_schemas_by_numeric_label_id() {
        let mut manifest = manifest_with_labels();
        manifest.edge_schemas.push(edge_schema(1));
        manifest.next_schema_id = 102;

        let catalog = RuntimeSchemaCatalog::from_manifest(&manifest).unwrap();
        assert!(!catalog.is_empty());
        assert!(catalog.has_edge_schemas);
        assert!(catalog.edge_schema_label_ids.contains(&1));
        assert!(catalog.edge_by_label_id.contains_key(&1));
        assert!(catalog.edge_has_applicable_schema(1));
        assert!(!catalog.edge_has_applicable_schema(2));
    }

    #[test]
    fn runtime_schema_catalog_summarizes_endpoint_and_closed_rules() {
        let mut manifest = manifest_with_labels();
        let mut node = node_schema(1);
        node.additional_properties = SchemaAdditionalPropertiesManifest::Reject;
        node.label_constraints = Some(NodeLabelConstraintManifestRule {
            all_of: vec![2],
            any_of: Vec::new(),
            none_of: Vec::new(),
        });
        let mut edge = edge_schema(1);
        edge.additional_properties = SchemaAdditionalPropertiesManifest::Reject;
        edge.from = Some(EndpointLabelManifestRule {
            all_of: vec![1],
            any_of: Vec::new(),
            none_of: vec![2],
        });
        manifest.node_schemas.push(node);
        manifest.edge_schemas.push(edge);
        manifest.next_schema_id = 102;

        let catalog = RuntimeSchemaCatalog::from_manifest(&manifest).unwrap();
        assert!(catalog.has_node_label_constraints);
        assert!(catalog.has_edge_endpoint_constraints);
        assert!(catalog.has_closed_node_schema);
        assert!(catalog.has_closed_edge_schema);
        assert!(catalog.endpoint_relevant_node_label_ids.contains(&1));
        assert!(catalog.endpoint_relevant_node_label_ids.contains(&2));
        assert!(catalog.label_change_may_affect_endpoint_rules(
            Some(&NodeLabelSet::single(1).unwrap()),
            Some(&NodeLabelSet::single(2).unwrap()),
            false
        ));
        assert!(!catalog.label_change_may_affect_endpoint_rules(
            Some(&NodeLabelSet::single(1).unwrap()),
            Some(&NodeLabelSet::single(1).unwrap()),
            false
        ));
    }

    #[test]
    fn compiled_property_rules_are_deterministic_by_key() {
        let mut schema = node_schema(1);
        schema.properties.insert("z".to_string(), property_rule());
        schema.properties.insert("a".to_string(), property_rule());
        schema.properties.insert("m".to_string(), property_rule());
        let catalog = node_catalog_with(vec![schema]);
        let compiled = catalog.node_by_label_id.get(&1).unwrap();
        let keys: Vec<&str> = compiled
            .properties
            .iter()
            .map(|rule| rule.key.as_str())
            .collect();
        assert_eq!(keys, vec!["a", "m", "z"]);
        assert_eq!(compiled.property_keys, vec!["a", "m", "z"]);
    }

    #[test]
    fn schema_violation_errors_include_public_label_names_when_available() {
        let mut node_rule = property_rule();
        node_rule.required = true;
        let mut node_schema = node_schema(1);
        node_schema.properties.insert("p".to_string(), node_rule);
        let node_catalog = node_catalog_with(vec![node_schema]);
        let node = node_with_labels(&[1]);
        assert_schema_error(
            node_catalog.validate_node_record(&node, None),
            "label 'Person' (id 1)",
        );

        let mut edge_rule = property_rule();
        edge_rule.required = true;
        let mut edge_schema = edge_schema(1);
        edge_schema.properties.insert("p".to_string(), edge_rule);
        let edge_catalog = edge_catalog_with(vec![edge_schema]);
        let edge = edge_with_label(1);
        assert_schema_error(
            edge_catalog.validate_edge_record(&edge),
            "label 'KNOWS' (id 1)",
        );
    }

    #[test]
    fn endpoint_relevant_delete_is_conservative() {
        let mut manifest = manifest_with_labels();
        let mut edge = edge_schema(1);
        edge.from = Some(EndpointLabelManifestRule {
            all_of: vec![1],
            any_of: Vec::new(),
            none_of: Vec::new(),
        });
        manifest.edge_schemas.push(edge);
        manifest.next_schema_id = 102;

        let catalog = RuntimeSchemaCatalog::from_manifest(&manifest).unwrap();
        assert!(catalog.has_edge_endpoint_constraints);
        assert!(catalog.label_change_may_affect_endpoint_rules(
            Some(&NodeLabelSet::single(2).unwrap()),
            None,
            true
        ));
        assert!(!catalog.label_change_may_affect_endpoint_rules(
            Some(&NodeLabelSet::single(2).unwrap()),
            Some(&NodeLabelSet::single(2).unwrap()),
            false
        ));
    }

    #[test]
    fn node_closed_property_union_and_open_schema_behavior_are_validated() {
        let mut closed = node_schema(1);
        closed.additional_properties = SchemaAdditionalPropertiesManifest::Reject;
        closed.properties.insert("a".to_string(), property_rule());
        let mut open = node_schema(2);
        open.properties.insert("b".to_string(), property_rule());
        let catalog = node_catalog_with(vec![closed, open]);

        let mut node = node_with_labels(&[1, 2]);
        node.props = props(vec![("a", PropValue::Int(1)), ("b", PropValue::Int(2))]);
        catalog.validate_node_record(&node, None).unwrap();

        node.props
            .insert("extra".to_string(), PropValue::Bool(true));
        assert_schema_error(
            catalog.validate_node_record(&node, None),
            "path properties.extra",
        );

        let mut open_only = node_schema(1);
        open_only.additional_properties = SchemaAdditionalPropertiesManifest::Allow;
        let catalog = node_catalog_with(vec![open_only]);
        let mut node = node_with_labels(&[1]);
        node.props = props(vec![("extra", PropValue::Bool(true))]);
        catalog.validate_node_record(&node, None).unwrap();
    }

    #[test]
    fn node_multi_label_property_composition_applies_every_rule() {
        let mut lower_rule = property_rule();
        lower_rule.numeric_min = Some(numeric_bound(PropValue::Int(0), true));
        let mut upper_rule = property_rule();
        upper_rule.numeric_max = Some(numeric_bound(PropValue::Int(10), true));
        let mut strict_null = property_rule();
        strict_null.nullable = false;

        let mut lower = node_schema(1);
        lower.properties.insert("p".to_string(), lower_rule);
        let mut upper = node_schema(2);
        upper.properties.insert("p".to_string(), upper_rule);
        upper.properties.insert("strict".to_string(), strict_null);
        let catalog = node_catalog_with(vec![lower, upper]);

        let mut node = node_with_labels(&[1, 2]);
        node.props = props(vec![
            ("p", PropValue::Int(5)),
            ("strict", PropValue::Bool(true)),
        ]);
        catalog.validate_node_record(&node, None).unwrap();

        node.props.insert("p".to_string(), PropValue::Int(-1));
        assert_schema_error(
            catalog.validate_node_record(&node, None),
            "numeric value >=",
        );

        node.props.insert("p".to_string(), PropValue::Int(11));
        assert_schema_error(
            catalog.validate_node_record(&node, None),
            "numeric value <=",
        );

        node.props.insert("p".to_string(), PropValue::Int(5));
        node.props.insert("strict".to_string(), PropValue::Null);
        assert_schema_error(
            catalog.validate_node_record(&node, None),
            "expected non-null value",
        );
    }

    #[test]
    fn node_key_and_label_constraints_are_validated() {
        let mut schema = node_schema(1);
        schema.key = Some(StringFieldSchemaManifestRule {
            min_bytes: Some(2),
            max_bytes: Some(4),
            enum_values: vec!["abcd".to_string()],
        });
        schema.label_constraints = Some(NodeLabelConstraintManifestRule {
            all_of: vec![2],
            any_of: vec![3, 4],
            none_of: vec![5],
        });
        let catalog = node_catalog_with(vec![schema]);

        let mut node = node_with_labels(&[1, 2, 3]);
        node.key = "abcd".to_string();
        catalog.validate_node_record(&node, None).unwrap();

        node.key = "a".to_string();
        assert_schema_error(catalog.validate_node_record(&node, None), "path key");

        node.key = "wxyz".to_string();
        assert_schema_error(
            catalog.validate_node_record(&node, None),
            "one of 1 enum values",
        );

        let mut missing_all = node_with_labels(&[1, 3]);
        missing_all.key = "abcd".to_string();
        assert_schema_error(
            catalog.validate_node_record(&missing_all, None),
            "labels.all_of",
        );

        let mut missing_any = node_with_labels(&[1, 2]);
        missing_any.key = "abcd".to_string();
        assert_schema_error(
            catalog.validate_node_record(&missing_any, None),
            "labels.any_of",
        );

        let mut forbidden = node_with_labels(&[1, 2, 3, 5]);
        forbidden.key = "abcd".to_string();
        assert_schema_error(
            catalog.validate_node_record(&forbidden, None),
            "labels.none_of",
        );
    }

    #[test]
    fn node_dense_vector_presence_and_dimension_are_validated() {
        let mut required = node_schema(1);
        required.dense_vector = Some(DenseVectorSchemaManifestRule {
            presence: SchemaVectorPresenceManifest::Required,
            dimension: Some(2),
        });
        let catalog = node_catalog_with(vec![required]);
        let config = dense_config(2);

        let mut node = node_with_labels(&[1]);
        assert_schema_error(
            catalog.validate_node_record(&node, Some(&config)),
            "path dense_vector",
        );

        node.dense_vector = Some(vec![0.25, 0.75]);
        catalog.validate_node_record(&node, Some(&config)).unwrap();

        node.dense_vector = Some(vec![0.25]);
        assert_schema_error(
            catalog.validate_node_record(&node, Some(&config)),
            "valid dense vector for DB config",
        );

        let mut forbidden = node_schema(1);
        forbidden.dense_vector = Some(DenseVectorSchemaManifestRule {
            presence: SchemaVectorPresenceManifest::Forbidden,
            dimension: None,
        });
        let catalog = node_catalog_with(vec![forbidden]);
        let mut node = node_with_labels(&[1]);
        node.dense_vector = Some(vec![0.25, 0.75]);
        assert_schema_error(
            catalog.validate_node_record(&node, Some(&config)),
            "absent dense vector",
        );

        let mut mismatched = node_schema(1);
        mismatched.dense_vector = Some(DenseVectorSchemaManifestRule {
            presence: SchemaVectorPresenceManifest::Optional,
            dimension: Some(3),
        });
        let catalog = node_catalog_with(vec![mismatched]);
        let node = node_with_labels(&[1]);
        assert_schema_error(
            catalog.validate_node_record(&node, Some(&config)),
            "dimension mismatch",
        );
    }

    #[test]
    fn node_sparse_vector_presence_bounds_and_caps_are_validated() {
        let mut required = node_schema(1);
        required.sparse_vector = Some(SparseVectorSchemaManifestRule {
            presence: SchemaVectorPresenceManifest::Required,
            min_entries: Some(2),
            max_entries: Some(3),
            max_dimension_id: Some(10),
        });
        let catalog = node_catalog_with(vec![required]);
        let mut node = node_with_labels(&[1]);
        assert_schema_error(
            catalog.validate_node_record(&node, None),
            "present sparse vector",
        );

        node.sparse_vector = Some(vec![(1, 0.5), (3, 0.25)]);
        catalog.validate_node_record(&node, None).unwrap();

        node.sparse_vector = Some(vec![(1, 0.5)]);
        assert_schema_error(catalog.validate_node_record(&node, None), "entry count >=");

        node.sparse_vector = Some(vec![(1, 0.5), (2, 0.5), (3, 0.5), (4, 0.5)]);
        assert_schema_error(catalog.validate_node_record(&node, None), "entry count <=");

        node.sparse_vector = Some(vec![(11, 0.5), (12, 0.5)]);
        assert_schema_error(catalog.validate_node_record(&node, None), "dimension_id <=");

        let mut forbidden = node_schema(1);
        forbidden.sparse_vector = Some(SparseVectorSchemaManifestRule {
            presence: SchemaVectorPresenceManifest::Forbidden,
            min_entries: None,
            max_entries: None,
            max_dimension_id: None,
        });
        let catalog = node_catalog_with(vec![forbidden]);
        assert_schema_error(
            catalog.validate_node_record(&node, None),
            "absent sparse vector",
        );
    }

    #[test]
    fn node_weight_finite_and_bounds_are_validated() {
        let mut schema = node_schema(1);
        schema.weight = Some(NumericFieldSchemaManifestRule {
            min: Some(numeric_bound(PropValue::Float(0.0), true)),
            max: Some(numeric_bound(PropValue::Float(2.0), true)),
            finite: true,
        });
        let catalog = node_catalog_with(vec![schema]);

        let mut node = node_with_labels(&[1]);
        node.weight = 1.0;
        catalog.validate_node_record(&node, None).unwrap();

        node.weight = f32::INFINITY;
        assert_schema_error(catalog.validate_node_record(&node, None), "finite weight");

        node.weight = -1.0;
        assert_schema_error(
            catalog.validate_node_record(&node, None),
            "numeric value >=",
        );

        node.weight = 3.0;
        assert_schema_error(
            catalog.validate_node_record(&node, None),
            "numeric value <=",
        );
    }

    #[test]
    fn edge_validity_shape_rejects_impossible_ranges() {
        let mut public_schema = EdgeSchema {
            validity: Some(EdgeValiditySchema {
                require_valid_from_before_valid_to: true,
                valid_from_min: Some(10),
                valid_from_max: None,
                valid_to_min: None,
                valid_to_max: Some(10),
                allow_open_ended_valid_to: true,
            }),
            ..Default::default()
        };
        assert!(matches!(
            validate_edge_schema_shape(&public_schema),
            Err(EngineError::InvalidOperation(message))
                if message.contains("valid_from_min is not less than valid_to_max")
        ));

        public_schema.validity = Some(EdgeValiditySchema {
            require_valid_from_before_valid_to: true,
            valid_from_min: Some(i64::MAX),
            valid_from_max: None,
            valid_to_min: None,
            valid_to_max: None,
            allow_open_ended_valid_to: true,
        });
        assert!(matches!(
            validate_edge_schema_shape(&public_schema),
            Err(EngineError::InvalidOperation(message))
                if message.contains("valid_from_min is not less than valid_to_max")
        ));

        let mut manifest = manifest_with_labels();
        let mut edge = edge_schema(1);
        edge.validity = Some(EdgeValiditySchemaManifestRule {
            require_valid_from_before_valid_to: true,
            valid_from_min: Some(10),
            valid_from_max: None,
            valid_to_min: None,
            valid_to_max: Some(10),
            allow_open_ended_valid_to: true,
        });
        manifest.edge_schemas.push(edge);
        manifest.next_schema_id = 102;
        assert!(matches!(
            validate_schema_manifest(&manifest),
            Err(EngineError::ManifestError(message))
                if message.contains("valid_from_min is not less than valid_to_max")
        ));

        let mut manifest = manifest_with_labels();
        let mut edge = edge_schema(1);
        edge.validity = Some(EdgeValiditySchemaManifestRule {
            require_valid_from_before_valid_to: true,
            valid_from_min: None,
            valid_from_max: None,
            valid_to_min: None,
            valid_to_max: Some(i64::MIN),
            allow_open_ended_valid_to: true,
        });
        manifest.edge_schemas.push(edge);
        manifest.next_schema_id = 102;
        assert!(matches!(
            validate_schema_manifest(&manifest),
            Err(EngineError::ManifestError(message))
                if message.contains("valid_from_min is not less than valid_to_max")
        ));

        let mut manifest = manifest_with_labels();
        let mut edge = edge_schema(1);
        edge.validity = Some(EdgeValiditySchemaManifestRule {
            require_valid_from_before_valid_to: false,
            valid_from_min: None,
            valid_from_max: None,
            valid_to_min: Some(i64::MAX),
            valid_to_max: None,
            allow_open_ended_valid_to: false,
        });
        manifest.edge_schemas.push(edge);
        manifest.next_schema_id = 102;
        assert!(matches!(
            validate_schema_manifest(&manifest),
            Err(EngineError::ManifestError(message))
                if message.contains("forbids open-ended valid_to")
        ));
    }

    #[test]
    fn edge_property_rules_closed_properties_and_label_scope_are_validated() {
        let mut required_string = property_rule();
        required_string.required = true;
        required_string.types = vec![SchemaValueTypeManifest::String];
        let mut schema = edge_schema(1);
        schema.additional_properties = SchemaAdditionalPropertiesManifest::Reject;
        schema.properties.insert("p".to_string(), required_string);
        let catalog = edge_catalog_with(vec![schema]);

        let mut edge = edge_with_label(1);
        edge.props = props(vec![("p", PropValue::String("ok".to_string()))]);
        catalog.validate_edge_record(&edge).unwrap();

        edge.props.clear();
        assert_schema_error(catalog.validate_edge_record(&edge), "required property");

        edge.props = props(vec![
            ("p", PropValue::String("ok".to_string())),
            ("extra", PropValue::Bool(true)),
        ]);
        assert_schema_error(catalog.validate_edge_record(&edge), "path properties.extra");

        let mut unrelated_edge = edge_with_label(2);
        unrelated_edge.props.clear();
        catalog.validate_edge_record(&unrelated_edge).unwrap();
    }

    #[test]
    fn edge_self_loop_weight_and_validity_rules_are_validated() {
        let mut schema = edge_schema(1);
        schema.allow_self_loops = false;
        schema.weight = Some(NumericFieldSchemaManifestRule {
            min: Some(numeric_bound(PropValue::Float(0.0), true)),
            max: Some(numeric_bound(PropValue::Float(2.0), true)),
            finite: true,
        });
        schema.validity = Some(EdgeValiditySchemaManifestRule {
            require_valid_from_before_valid_to: true,
            valid_from_min: Some(10),
            valid_from_max: Some(20),
            valid_to_min: Some(30),
            valid_to_max: Some(40),
            allow_open_ended_valid_to: true,
        });
        let catalog = edge_catalog_with(vec![schema]);

        let mut edge = edge_with_label(1);
        edge.weight = 1.0;
        edge.valid_from = 15;
        edge.valid_to = 35;
        catalog.validate_edge_record(&edge).unwrap();

        edge.from = 7;
        edge.to = 7;
        assert_schema_error(catalog.validate_edge_record(&edge), "path self_loop");
        edge.to = 8;

        edge.weight = f32::NAN;
        assert_schema_error(catalog.validate_edge_record(&edge), "finite weight");
        edge.weight = -1.0;
        assert_schema_error(catalog.validate_edge_record(&edge), "numeric value >=");
        edge.weight = 3.0;
        assert_schema_error(catalog.validate_edge_record(&edge), "numeric value <=");
        edge.weight = 1.0;

        edge.valid_from = 35;
        edge.valid_to = 35;
        assert_schema_error(catalog.validate_edge_record(&edge), "valid_from < valid_to");

        edge.valid_from = 9;
        edge.valid_to = 35;
        assert_schema_error(catalog.validate_edge_record(&edge), "valid_from >=");
        edge.valid_from = 21;
        assert_schema_error(catalog.validate_edge_record(&edge), "valid_from <=");
        edge.valid_from = 15;
        edge.valid_to = 29;
        assert_schema_error(catalog.validate_edge_record(&edge), "valid_to >=");
        edge.valid_to = 41;
        assert_schema_error(catalog.validate_edge_record(&edge), "valid_to <=");

        let mut open_ended_schema = edge_schema(1);
        open_ended_schema.validity = Some(EdgeValiditySchemaManifestRule {
            require_valid_from_before_valid_to: false,
            valid_from_min: None,
            valid_from_max: None,
            valid_to_min: None,
            valid_to_max: None,
            allow_open_ended_valid_to: false,
        });
        let catalog = edge_catalog_with(vec![open_ended_schema]);
        let mut edge = edge_with_label(1);
        edge.valid_to = i64::MAX;
        assert_schema_error(catalog.validate_edge_record(&edge), "open-ended i64::MAX");
    }

    #[test]
    fn endpoint_label_helper_validates_all_any_none_against_supplied_labels() {
        let rule = EndpointLabelManifestRule {
            all_of: vec![1],
            any_of: vec![2, 3],
            none_of: vec![4],
        };
        let labels = NodeLabelSet::from_canonical_ids(&[1, 2]).unwrap();
        validate_endpoint_label_rule_against_labels(&rule, &labels).unwrap();

        let labels = NodeLabelSet::from_canonical_ids(&[2]).unwrap();
        assert_schema_error(
            validate_endpoint_label_rule_against_labels(&rule, &labels),
            "labels.all_of",
        );

        let labels = NodeLabelSet::from_canonical_ids(&[1]).unwrap();
        assert_schema_error(
            validate_endpoint_label_rule_against_labels(&rule, &labels),
            "labels.any_of",
        );

        let labels = NodeLabelSet::from_canonical_ids(&[1, 2, 4]).unwrap();
        assert_schema_error(
            validate_endpoint_label_rule_against_labels(&rule, &labels),
            "labels.none_of",
        );
    }

    #[test]
    fn string_field_enum_shape_limits_are_validated() {
        let public_schema = NodeSchema {
            key: Some(StringFieldSchema {
                min_bytes: None,
                max_bytes: None,
                enum_values: vec!["x".to_string(); MAX_SCHEMA_ENUM_VALUES_PER_PROPERTY + 1],
            }),
            ..Default::default()
        };
        assert!(matches!(
            validate_node_schema_shape(&public_schema),
            Err(EngineError::InvalidOperation(message))
                if message.contains("enum has too many values")
        ));

        let mut manifest = manifest_with_labels();
        let mut node = node_schema(1);
        node.key = Some(StringFieldSchemaManifestRule {
            min_bytes: None,
            max_bytes: None,
            enum_values: vec!["x".to_string(); MAX_SCHEMA_ENUM_VALUES_PER_PROPERTY + 1],
        });
        manifest.node_schemas.push(node);
        manifest.next_schema_id = 2;
        assert!(matches!(
            validate_schema_manifest(&manifest),
            Err(EngineError::ManifestError(message))
                if message.contains("enum has too many values")
        ));

        let mut manifest = manifest_with_labels();
        let mut node = node_schema(1);
        node.key = Some(StringFieldSchemaManifestRule {
            min_bytes: None,
            max_bytes: None,
            enum_values: vec!["x".repeat(MAX_SCHEMA_ENUM_LITERAL_BYTES_PER_PROPERTY + 1)],
        });
        manifest.node_schemas.push(node);
        manifest.next_schema_id = 2;
        assert!(matches!(
            validate_schema_manifest(&manifest),
            Err(EngineError::ManifestError(message))
                if message.contains("enum literals exceed")
        ));
    }

    #[test]
    fn public_schema_shape_errors_use_public_prefix() {
        let mut schema = NodeSchema::default();
        schema.properties.insert(
            "score".to_string(),
            PropertySchema {
                numeric_min: Some(SchemaNumericBound {
                    value: PropValue::Float(f64::NAN),
                    inclusive: true,
                }),
                ..PropertySchema::default()
            },
        );

        let error = validate_node_schema_shape(&schema).unwrap_err().to_string();
        assert!(error.contains("invalid schema:"));
        assert!(!error.contains("invalid schema manifest:"));
    }
}
