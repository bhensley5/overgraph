// Schema management APIs and existing-data validation.
// This file is include!()'d into mod.rs. All items share the engine module scope.

#[derive(Clone, Copy)]
struct SchemaScanOptions {
    max_violations: usize,
    chunk_size: usize,
    scan_limit: Option<u64>,
}

fn unexpected_bulk_graph_schema_reply(method: &str) -> EngineError {
    EngineError::InvalidOperation(format!("{method} returned unexpected core write reply"))
}

impl SchemaScanOptions {
    fn from_check(options: &SchemaCheckOptions) -> Result<Self, EngineError> {
        validate_schema_scan_options(options.max_violations, options.chunk_size, options.scan_limit)
    }

    fn from_set(options: &SchemaSetOptions) -> Result<Self, EngineError> {
        validate_schema_scan_options(options.max_violations, options.chunk_size, options.scan_limit)
    }

    fn from_graph_check(options: &GraphSchemaCheckOptions) -> Result<Self, EngineError> {
        validate_graph_schema_scan_options(
            options.max_violations,
            options.chunk_size,
            options.scan_limit,
        )
    }

    fn from_graph_set(options: &GraphSchemaSetOptions) -> Result<Self, EngineError> {
        validate_graph_schema_scan_options(
            options.max_violations,
            options.chunk_size,
            options.scan_limit,
        )
    }
}

fn validate_schema_scan_options(
    max_violations: usize,
    chunk_size: usize,
    scan_limit: Option<u64>,
) -> Result<SchemaScanOptions, EngineError> {
    if chunk_size == 0 {
        return Err(EngineError::InvalidOperation(
            "invalid schema: options.chunk_size must be positive".to_string(),
        ));
    }
    if scan_limit == Some(0) {
        return Err(EngineError::InvalidOperation(
            "invalid schema: options.scan_limit must be positive when set".to_string(),
        ));
    }
    Ok(SchemaScanOptions {
        max_violations,
        chunk_size,
        scan_limit,
    })
}

fn validate_graph_schema_scan_options(
    max_violations: usize,
    chunk_size: usize,
    scan_limit: Option<u64>,
) -> Result<SchemaScanOptions, EngineError> {
    if chunk_size == 0 {
        return Err(EngineError::InvalidOperation(
            "invalid graph schema: options.chunk_size must be positive".to_string(),
        ));
    }
    Ok(SchemaScanOptions {
        max_violations,
        chunk_size,
        scan_limit,
    })
}

struct SchemaReportBuilder {
    report: SchemaValidationReport,
    max_violations: usize,
}

impl SchemaReportBuilder {
    fn new(options: SchemaScanOptions) -> Self {
        Self {
            report: SchemaValidationReport {
                checked_records: 0,
                violation_count: 0,
                violations: Vec::with_capacity(options.max_violations.min(16)),
                truncated: false,
                scan_limit_hit: false,
            },
            max_violations: options.max_violations,
        }
    }

    fn should_validate_next(&mut self, scan_limit: Option<u64>) -> bool {
        if scan_limit.is_some_and(|limit| self.report.checked_records >= limit) {
            self.report.scan_limit_hit = true;
            return false;
        }
        self.report.checked_records += 1;
        true
    }

    fn push_violation(&mut self, violation: SchemaViolation) {
        self.report.violation_count += 1;
        if self.report.violations.len() < self.max_violations {
            self.report.violations.push(violation);
        } else {
            self.report.truncated = true;
        }
    }

    fn finish(self) -> SchemaValidationReport {
        self.report
    }
}

fn schema_report_publication_error(report: &SchemaValidationReport) -> Option<EngineError> {
    if report.scan_limit_hit {
        return Some(EngineError::InvalidOperation(
            "schema publication rejected: existing data scan limit exceeded".to_string(),
        ));
    }
    if report.violation_count > 0 {
        let detail = report
            .violations
            .first()
            .map(|violation| violation.message.as_str())
            .unwrap_or("existing data violates schema");
        return Some(EngineError::InvalidOperation(format!(
            "schema publication rejected: {detail}"
        )));
    }
    None
}

#[derive(Clone)]
enum GraphSchemaValidationTarget {
    Node { label: String, label_id: u32 },
    Edge { label: String, label_id: u32 },
}

enum GraphSchemaAlterPlan {
    Add(GraphSchema),
    Drop(Vec<GraphSchemaDropTargetResult>),
}

fn graph_schema_is_empty(schema: &GraphSchema) -> bool {
    schema.node_schemas.is_empty() && schema.edge_schemas.is_empty()
}

fn reject_duplicate_graph_schema_target(
    seen: &mut BTreeSet<String>,
    label: &str,
    target_kind: &str,
    context: &str,
) -> Result<(), EngineError> {
    if !seen.insert(label.to_string()) {
        return Err(EngineError::InvalidOperation(format!(
            "invalid graph schema: duplicate {target_kind} target '{label}' in {context}"
        )));
    }
    Ok(())
}

fn validate_graph_schema_set_targets(
    schema: &GraphSchema,
    context: &str,
) -> Result<(), EngineError> {
    let mut node_labels = BTreeSet::new();
    for info in &schema.node_schemas {
        reject_duplicate_graph_schema_target(&mut node_labels, &info.label, "node", context)?;
    }
    let mut edge_labels = BTreeSet::new();
    for info in &schema.edge_schemas {
        reject_duplicate_graph_schema_target(&mut edge_labels, &info.label, "edge", context)?;
    }
    Ok(())
}

fn classify_graph_schema_operations(
    operations: Vec<GraphSchemaOperation>,
) -> Result<GraphSchemaAlterPlan, EngineError> {
    if operations.is_empty() {
        return Err(EngineError::InvalidOperation(
            "invalid graph schema: alter_graph_schema requires at least one operation".to_string(),
        ));
    }

    let mut saw_add = false;
    let mut saw_drop = false;
    let mut node_set_labels = BTreeSet::new();
    let mut edge_set_labels = BTreeSet::new();
    let mut node_drop_labels_seen = BTreeSet::new();
    let mut edge_drop_labels_seen = BTreeSet::new();
    let mut node_schemas = Vec::new();
    let mut edge_schemas = Vec::new();
    let mut drop_targets = Vec::new();

    for operation in operations {
        match operation {
            GraphSchemaOperation::SetNode { label, schema } => {
                saw_add = true;
                reject_duplicate_graph_schema_target(
                    &mut node_set_labels,
                    &label,
                    "node set",
                    "alter_graph_schema",
                )?;
                node_schemas.push(NodeSchemaInfo { label, schema });
            }
            GraphSchemaOperation::SetEdge { label, schema } => {
                saw_add = true;
                reject_duplicate_graph_schema_target(
                    &mut edge_set_labels,
                    &label,
                    "edge set",
                    "alter_graph_schema",
                )?;
                edge_schemas.push(EdgeSchemaInfo { label, schema });
            }
            GraphSchemaOperation::DropNode { label } => {
                saw_drop = true;
                reject_duplicate_graph_schema_target(
                    &mut node_drop_labels_seen,
                    &label,
                    "node drop",
                    "alter_graph_schema",
                )?;
                drop_targets.push(GraphSchemaDropTargetResult {
                    target_kind: SchemaTargetKind::Node,
                    label,
                    action: GraphSchemaDropAction::NotFound,
                });
            }
            GraphSchemaOperation::DropEdge { label } => {
                saw_drop = true;
                reject_duplicate_graph_schema_target(
                    &mut edge_drop_labels_seen,
                    &label,
                    "edge drop",
                    "alter_graph_schema",
                )?;
                drop_targets.push(GraphSchemaDropTargetResult {
                    target_kind: SchemaTargetKind::Edge,
                    label,
                    action: GraphSchemaDropAction::NotFound,
                });
            }
        }
    }

    if saw_add && saw_drop {
        return Err(EngineError::InvalidOperation(
            "invalid graph schema: alter_graph_schema cannot mix set and drop operations"
                .to_string(),
        ));
    }
    if saw_add {
        Ok(GraphSchemaAlterPlan::Add(GraphSchema {
            node_schemas,
            edge_schemas,
        }))
    } else {
        Ok(GraphSchemaAlterPlan::Drop(drop_targets))
    }
}

fn aggregate_graph_schema_report(
    operation: GraphSchemaOperationKind,
    entries: Vec<GraphSchemaValidationReportEntry>,
) -> GraphSchemaCheckReport {
    let checked_records = entries
        .iter()
        .map(|entry| entry.report.checked_records)
        .sum();
    let violation_count = entries
        .iter()
        .map(|entry| entry.report.violation_count)
        .sum();
    let truncated = entries.iter().any(|entry| entry.report.truncated);
    let scan_limit_hit = entries.iter().any(|entry| entry.report.scan_limit_hit);
    GraphSchemaCheckReport {
        operation,
        entries,
        checked_records,
        violation_count,
        truncated,
        scan_limit_hit,
    }
}

fn empty_graph_schema_report(operation: GraphSchemaOperationKind) -> GraphSchemaCheckReport {
    aggregate_graph_schema_report(operation, Vec::new())
}

fn graph_schema_publication_error(report: &GraphSchemaCheckReport) -> Option<EngineError> {
    if report.scan_limit_hit {
        return Some(EngineError::InvalidOperation(
            "schema publication rejected: existing data scan limit exceeded".to_string(),
        ));
    }
    if report.violation_count > 0 {
        let detail = report
            .entries
            .iter()
            .flat_map(|entry| entry.report.violations.iter())
            .next()
            .map(|violation| violation.message.as_str())
            .unwrap_or("existing data violates schema");
        return Some(EngineError::InvalidOperation(format!(
            "schema publication rejected: {detail}"
        )));
    }
    None
}

fn graph_schema_publish_impact(node_labels: &[(String, u32)], edge_labels: &[(String, u32)]) -> PublishImpact {
    if node_labels.is_empty() && edge_labels.is_empty() {
        PublishImpact::SnapshotOnly
    } else {
        PublishImpact::SnapshotWithLabelCatalog
    }
}

fn node_violation_target(
    node: &NodeRecord,
    catalog: &impl LabelCatalogLookup,
) -> Result<SchemaViolationTarget, EngineError> {
    let mut labels = Vec::with_capacity(node.label_ids.len());
    for &label_id in node.label_ids.as_slice() {
        labels.push(
            catalog
                .node_label(label_id)
                .ok_or_else(|| {
                    EngineError::ManifestError(format!(
                        "node record {} references missing node label_id {}",
                        node.id, label_id
                    ))
                })?
                .to_string(),
        );
    }
    Ok(SchemaViolationTarget::Node {
        id: node.id,
        labels,
        key: node.key.clone(),
    })
}

fn edge_violation_target(
    edge: &EdgeRecord,
    catalog: &impl LabelCatalogLookup,
) -> Result<SchemaViolationTarget, EngineError> {
    let label = catalog
        .edge_label(edge.label_id)
        .ok_or_else(|| {
            EngineError::ManifestError(format!(
                "edge record {} references missing edge label_id {}",
                edge.id, edge.label_id
            ))
        })?
        .to_string();
    Ok(SchemaViolationTarget::Edge {
        id: edge.id,
        label,
        from: edge.from,
        to: edge.to,
    })
}

fn schema_violation_from_failure(
    target: SchemaViolationTarget,
    failure: SchemaValidationFailure,
) -> SchemaViolation {
    SchemaViolation {
        target,
        path: failure.path,
        message: failure.message,
    }
}

fn schema_snapshot_manifest(
    schema_snapshot: &PublishedSchemaCatalogSnapshot,
    label_catalog: &ReadLabelCatalogSnapshot,
) -> ManifestState {
    let mut manifest = default_manifest();
    manifest.node_label_tokens = label_catalog
        .node_label_to_id
        .iter()
        .map(|(label, &label_id)| (label.clone(), label_id))
        .collect();
    manifest.edge_label_tokens = label_catalog
        .edge_label_to_id
        .iter()
        .map(|(label, &label_id)| (label.clone(), label_id))
        .collect();
    manifest.next_node_label_id = manifest
        .node_label_tokens
        .values()
        .copied()
        .max()
        .unwrap_or(0)
        .saturating_add(1);
    manifest.next_edge_label_id = manifest
        .edge_label_tokens
        .values()
        .copied()
        .max()
        .unwrap_or(0)
        .saturating_add(1);
    manifest.next_schema_id = schema_snapshot.next_schema_id;
    manifest.node_schemas = schema_snapshot.node_schemas.clone();
    manifest.edge_schemas = schema_snapshot.edge_schemas.clone();
    manifest
}

fn reserve_temp_node_label(manifest: &mut ManifestState, label: &str) -> Result<u32, EngineError> {
    if let Some(&label_id) = manifest.node_label_tokens.get(label) {
        return Ok(label_id);
    }
    validate_label_token_name(label)?;
    if manifest.next_node_label_id == u32::MAX {
        return Err(EngineError::InvalidOperation(
            "node label token ID space exhausted".to_string(),
        ));
    }
    let label_id = manifest.next_node_label_id;
    manifest.next_node_label_id += 1;
    manifest.node_label_tokens.insert(label.to_string(), label_id);
    Ok(label_id)
}

fn reserve_temp_edge_label(manifest: &mut ManifestState, label: &str) -> Result<u32, EngineError> {
    if let Some(&label_id) = manifest.edge_label_tokens.get(label) {
        return Ok(label_id);
    }
    validate_label_token_name(label)?;
    if manifest.next_edge_label_id == u32::MAX {
        return Err(EngineError::InvalidOperation(
            "edge-label token ID space exhausted".to_string(),
        ));
    }
    let label_id = manifest.next_edge_label_id;
    manifest.next_edge_label_id += 1;
    manifest.edge_label_tokens.insert(label.to_string(), label_id);
    Ok(label_id)
}

fn next_node_schema_entry_metadata(
    manifest: &ManifestState,
    label_id: u32,
    now_ms: i64,
) -> Result<(u64, u64, i64), EngineError> {
    if let Some(existing) = manifest
        .node_schemas
        .iter()
        .find(|entry| entry.label_id == label_id)
    {
        let revision = existing.revision.checked_add(1).ok_or_else(|| {
            EngineError::InvalidOperation(
                "invalid schema: node schema revision would overflow".to_string(),
            )
        })?;
        return Ok((existing.schema_id, revision, existing.created_at_ms));
    }
    if manifest.next_schema_id == u64::MAX {
        return Err(EngineError::InvalidOperation(
            "invalid schema: schema ID space exhausted".to_string(),
        ));
    }
    Ok((manifest.next_schema_id, 1, now_ms))
}

fn next_edge_schema_entry_metadata(
    manifest: &ManifestState,
    label_id: u32,
    now_ms: i64,
) -> Result<(u64, u64, i64), EngineError> {
    if let Some(existing) = manifest
        .edge_schemas
        .iter()
        .find(|entry| entry.label_id == label_id)
    {
        let revision = existing.revision.checked_add(1).ok_or_else(|| {
            EngineError::InvalidOperation(
                "invalid schema: edge schema revision would overflow".to_string(),
            )
        })?;
        return Ok((existing.schema_id, revision, existing.created_at_ms));
    }
    if manifest.next_schema_id == u64::MAX {
        return Err(EngineError::InvalidOperation(
            "invalid schema: schema ID space exhausted".to_string(),
        ));
    }
    Ok((manifest.next_schema_id, 1, now_ms))
}

fn upsert_node_schema_entry(
    manifest: &mut ManifestState,
    entry: NodeSchemaManifestEntry,
) -> Result<(), EngineError> {
    let is_new = manifest
        .node_schemas
        .iter()
        .all(|existing| existing.label_id != entry.label_id);
    manifest
        .node_schemas
        .retain(|existing| existing.label_id != entry.label_id);
    if is_new {
        manifest.next_schema_id = manifest.next_schema_id.max(entry.schema_id.checked_add(1).ok_or_else(|| {
            EngineError::InvalidOperation("invalid schema: next schema ID would overflow".to_string())
        })?);
    }
    manifest.node_schemas.push(entry);
    manifest
        .node_schemas
        .sort_unstable_by_key(|entry| entry.label_id);
    Ok(())
}

fn upsert_edge_schema_entry(
    manifest: &mut ManifestState,
    entry: EdgeSchemaManifestEntry,
) -> Result<(), EngineError> {
    let is_new = manifest
        .edge_schemas
        .iter()
        .all(|existing| existing.label_id != entry.label_id);
    manifest
        .edge_schemas
        .retain(|existing| existing.label_id != entry.label_id);
    if is_new {
        manifest.next_schema_id = manifest.next_schema_id.max(entry.schema_id.checked_add(1).ok_or_else(|| {
            EngineError::InvalidOperation("invalid schema: next schema ID would overflow".to_string())
        })?);
    }
    manifest.edge_schemas.push(entry);
    manifest
        .edge_schemas
        .sort_unstable_by_key(|entry| entry.label_id);
    Ok(())
}

fn validate_existing_nodes_for_schema(
    view: &ReadView,
    label_catalog: &impl LabelCatalogLookup,
    target_label_id: u32,
    catalog: &RuntimeSchemaCatalog,
    options: SchemaScanOptions,
) -> Result<SchemaValidationReport, EngineError> {
    let mut builder = SchemaReportBuilder::new(options);
    let dense_config = view.sources.manifest.dense_vector.as_ref();

    view.scan_raw_node_label_candidates(&[target_label_id], None, options.chunk_size, |chunk| {
        let hydrated = view.get_nodes_raw(chunk)?;
        for node in hydrated.into_iter().flatten() {
            if !node.label_ids.contains(target_label_id) {
                continue;
            }
            if !builder.should_validate_next(options.scan_limit) {
                return Ok(ControlFlow::Break(()));
            }
            if let Err(failure) = catalog.validate_node_record_detailed(&node, dense_config) {
                let target = node_violation_target(&node, label_catalog)?;
                builder.push_violation(schema_violation_from_failure(target, failure));
            }
        }
        Ok(ControlFlow::Continue(()))
    })?;

    Ok(builder.finish())
}

fn validate_existing_edges_for_schema(
    view: &ReadView,
    label_catalog: &impl LabelCatalogLookup,
    target_label_id: u32,
    catalog: &RuntimeSchemaCatalog,
    options: SchemaScanOptions,
) -> Result<SchemaValidationReport, EngineError> {
    let mut builder = SchemaReportBuilder::new(options);
    let hydrate_endpoints = catalog.edge_schema_has_endpoint_rules(target_label_id);

    view.scan_raw_edge_label_candidates(target_label_id, None, options.chunk_size, |chunk| {
        let hydrated = view.get_edges(chunk)?;
        let mut edges = Vec::with_capacity(hydrated.len());
        for edge in hydrated.into_iter().flatten() {
            if edge.label_id == target_label_id {
                edges.push(edge);
            }
        }
        if edges.is_empty() {
            return Ok(ControlFlow::Continue(()));
        }

        let endpoint_labels = if hydrate_endpoints {
            let mut endpoint_ids = Vec::with_capacity(edges.len().saturating_mul(2));
            for edge in &edges {
                endpoint_ids.push(edge.from);
                endpoint_ids.push(edge.to);
            }
            endpoint_ids.sort_unstable();
            endpoint_ids.dedup();
            let endpoint_states = view.sources().find_node_visibility_meta(&endpoint_ids)?;
            let mut labels_by_id: NodeIdMap<NodeLabelSet> = NodeIdMap::default();
            for (node_id, state) in endpoint_ids.into_iter().zip(endpoint_states) {
                if let NodeVisibilityState::Live(meta) = state {
                    labels_by_id.insert(node_id, meta.label_ids);
                }
            }
            Some(labels_by_id)
        } else {
            None
        };

        for edge in edges {
            if !builder.should_validate_next(options.scan_limit) {
                return Ok(ControlFlow::Break(()));
            }
            let mut validation = catalog.validate_edge_record_detailed(&edge);
            if validation.is_ok() {
                if let Some(endpoint_labels) = endpoint_labels.as_ref() {
                    validation = catalog.validate_edge_endpoint_labels_detailed(
                        &edge,
                        endpoint_labels.get(&edge.from),
                        endpoint_labels.get(&edge.to),
                    );
                }
            }
            if let Err(failure) = validation {
                let target = edge_violation_target(&edge, label_catalog)?;
                builder.push_violation(schema_violation_from_failure(target, failure));
            }
        }
        Ok(ControlFlow::Continue(()))
    })?;

    Ok(builder.finish())
}

fn validate_graph_schema_targets(
    view: &ReadView,
    label_catalog: &impl LabelCatalogLookup,
    targets: &[GraphSchemaValidationTarget],
    catalog: &RuntimeSchemaCatalog,
    options: SchemaScanOptions,
    operation: GraphSchemaOperationKind,
) -> Result<GraphSchemaCheckReport, EngineError> {
    let mut entries = Vec::with_capacity(targets.len());
    for target in targets {
        match target {
            GraphSchemaValidationTarget::Node { label, label_id } => {
                let report = validate_existing_nodes_for_schema(
                    view,
                    label_catalog,
                    *label_id,
                    catalog,
                    options,
                )?;
                entries.push(GraphSchemaValidationReportEntry {
                    target_kind: SchemaTargetKind::Node,
                    label: label.clone(),
                    report,
                });
            }
            GraphSchemaValidationTarget::Edge { label, label_id } => {
                let report = validate_existing_edges_for_schema(
                    view,
                    label_catalog,
                    *label_id,
                    catalog,
                    options,
                )?;
                entries.push(GraphSchemaValidationReportEntry {
                    target_kind: SchemaTargetKind::Edge,
                    label: label.clone(),
                    report,
                });
            }
        }
    }
    Ok(aggregate_graph_schema_report(operation, entries))
}

fn build_node_schema_entry_with_plan(
    manifest: &mut ManifestState,
    label: &str,
    schema: &NodeSchema,
    dense_config: Option<&DenseVectorConfig>,
    label_plan: &mut LabelResolutionPlan<'_>,
    now_ms: i64,
) -> Result<(NodeSchemaManifestEntry, u32), EngineError> {
    validate_node_schema_dense_vector_config(schema, dense_config)?;
    let label_id = label_plan.resolve_node_label_for_write(label)?;
    let (schema_id, revision, created_at_ms) =
        next_node_schema_entry_metadata(manifest, label_id, now_ms)?;
    let entry = node_schema_manifest_entry_from_public(
        label_id,
        schema_id,
        revision,
        created_at_ms,
        now_ms,
        schema,
        |label| label_plan.resolve_node_label_for_write(label),
    )?;
    upsert_node_schema_entry(manifest, entry.clone())?;
    Ok((entry, label_id))
}

fn build_edge_schema_entry_with_plan(
    manifest: &mut ManifestState,
    label: &str,
    schema: &EdgeSchema,
    label_plan: &mut LabelResolutionPlan<'_>,
    now_ms: i64,
) -> Result<(EdgeSchemaManifestEntry, u32), EngineError> {
    let label_id = label_plan.resolve_edge_label_for_write(label)?;
    let (schema_id, revision, created_at_ms) =
        next_edge_schema_entry_metadata(manifest, label_id, now_ms)?;
    let entry = edge_schema_manifest_entry_from_public(
        label_id,
        schema_id,
        revision,
        created_at_ms,
        now_ms,
        schema,
        |label| label_plan.resolve_node_label_for_write(label),
    )?;
    upsert_edge_schema_entry(manifest, entry.clone())?;
    Ok((entry, label_id))
}

fn build_node_schema_entry_with_temp_labels(
    manifest: &mut ManifestState,
    label: &str,
    schema: &NodeSchema,
    dense_config: Option<&DenseVectorConfig>,
    now_ms: i64,
) -> Result<(NodeSchemaManifestEntry, u32), EngineError> {
    validate_node_schema_dense_vector_config(schema, dense_config)?;
    let label_id = reserve_temp_node_label(manifest, label)?;
    let (schema_id, revision, created_at_ms) =
        next_node_schema_entry_metadata(manifest, label_id, now_ms)?;
    let entry = node_schema_manifest_entry_from_public(
        label_id,
        schema_id,
        revision,
        created_at_ms,
        now_ms,
        schema,
        |label| reserve_temp_node_label(manifest, label),
    )?;
    upsert_node_schema_entry(manifest, entry.clone())?;
    Ok((entry, label_id))
}

fn build_edge_schema_entry_with_temp_labels(
    manifest: &mut ManifestState,
    label: &str,
    schema: &EdgeSchema,
    now_ms: i64,
) -> Result<(EdgeSchemaManifestEntry, u32), EngineError> {
    let label_id = reserve_temp_edge_label(manifest, label)?;
    let (schema_id, revision, created_at_ms) =
        next_edge_schema_entry_metadata(manifest, label_id, now_ms)?;
    let entry = edge_schema_manifest_entry_from_public(
        label_id,
        schema_id,
        revision,
        created_at_ms,
        now_ms,
        schema,
        |label| reserve_temp_node_label(manifest, label),
    )?;
    upsert_edge_schema_entry(manifest, entry.clone())?;
    Ok((entry, label_id))
}

fn apply_graph_schema_add_with_plan(
    manifest: &mut ManifestState,
    schema: &GraphSchema,
    dense_config: Option<&DenseVectorConfig>,
    label_plan: &mut LabelResolutionPlan<'_>,
    now_ms: i64,
) -> Result<Vec<GraphSchemaValidationTarget>, EngineError> {
    let mut targets = Vec::with_capacity(schema.node_schemas.len() + schema.edge_schemas.len());
    for info in &schema.node_schemas {
        let (_entry, label_id) = build_node_schema_entry_with_plan(
            manifest,
            &info.label,
            &info.schema,
            dense_config,
            label_plan,
            now_ms,
        )?;
        targets.push(GraphSchemaValidationTarget::Node {
            label: info.label.clone(),
            label_id,
        });
    }
    for info in &schema.edge_schemas {
        let (_entry, label_id) = build_edge_schema_entry_with_plan(
            manifest,
            &info.label,
            &info.schema,
            label_plan,
            now_ms,
        )?;
        targets.push(GraphSchemaValidationTarget::Edge {
            label: info.label.clone(),
            label_id,
        });
    }
    Ok(targets)
}

fn apply_graph_schema_set_with_plan(
    manifest: &mut ManifestState,
    schema: &GraphSchema,
    dense_config: Option<&DenseVectorConfig>,
    label_plan: &mut LabelResolutionPlan<'_>,
    now_ms: i64,
) -> Result<Vec<GraphSchemaValidationTarget>, EngineError> {
    let mut metadata_manifest = manifest.clone();
    let mut node_entries = Vec::with_capacity(schema.node_schemas.len());
    let mut edge_entries = Vec::with_capacity(schema.edge_schemas.len());
    let mut targets = Vec::with_capacity(schema.node_schemas.len() + schema.edge_schemas.len());

    for info in &schema.node_schemas {
        let (entry, label_id) = build_node_schema_entry_with_plan(
            &mut metadata_manifest,
            &info.label,
            &info.schema,
            dense_config,
            label_plan,
            now_ms,
        )?;
        node_entries.push(entry);
        targets.push(GraphSchemaValidationTarget::Node {
            label: info.label.clone(),
            label_id,
        });
    }
    for info in &schema.edge_schemas {
        let (entry, label_id) = build_edge_schema_entry_with_plan(
            &mut metadata_manifest,
            &info.label,
            &info.schema,
            label_plan,
            now_ms,
        )?;
        edge_entries.push(entry);
        targets.push(GraphSchemaValidationTarget::Edge {
            label: info.label.clone(),
            label_id,
        });
    }

    node_entries.sort_unstable_by_key(|entry| entry.label_id);
    edge_entries.sort_unstable_by_key(|entry| entry.label_id);
    metadata_manifest.node_schemas = node_entries;
    metadata_manifest.edge_schemas = edge_entries;
    *manifest = metadata_manifest;
    Ok(targets)
}

fn apply_graph_schema_add_with_temp_labels(
    manifest: &mut ManifestState,
    schema: &GraphSchema,
    dense_config: Option<&DenseVectorConfig>,
    now_ms: i64,
) -> Result<Vec<GraphSchemaValidationTarget>, EngineError> {
    let mut targets = Vec::with_capacity(schema.node_schemas.len() + schema.edge_schemas.len());
    for info in &schema.node_schemas {
        let (_entry, label_id) = build_node_schema_entry_with_temp_labels(
            manifest,
            &info.label,
            &info.schema,
            dense_config,
            now_ms,
        )?;
        targets.push(GraphSchemaValidationTarget::Node {
            label: info.label.clone(),
            label_id,
        });
    }
    for info in &schema.edge_schemas {
        let (_entry, label_id) = build_edge_schema_entry_with_temp_labels(
            manifest,
            &info.label,
            &info.schema,
            now_ms,
        )?;
        targets.push(GraphSchemaValidationTarget::Edge {
            label: info.label.clone(),
            label_id,
        });
    }
    Ok(targets)
}

fn apply_graph_schema_set_with_temp_labels(
    manifest: &mut ManifestState,
    schema: &GraphSchema,
    dense_config: Option<&DenseVectorConfig>,
    now_ms: i64,
) -> Result<Vec<GraphSchemaValidationTarget>, EngineError> {
    let mut metadata_manifest = manifest.clone();
    let mut node_entries = Vec::with_capacity(schema.node_schemas.len());
    let mut edge_entries = Vec::with_capacity(schema.edge_schemas.len());
    let mut targets = Vec::with_capacity(schema.node_schemas.len() + schema.edge_schemas.len());

    for info in &schema.node_schemas {
        let (entry, label_id) = build_node_schema_entry_with_temp_labels(
            &mut metadata_manifest,
            &info.label,
            &info.schema,
            dense_config,
            now_ms,
        )?;
        node_entries.push(entry);
        targets.push(GraphSchemaValidationTarget::Node {
            label: info.label.clone(),
            label_id,
        });
    }
    for info in &schema.edge_schemas {
        let (entry, label_id) = build_edge_schema_entry_with_temp_labels(
            &mut metadata_manifest,
            &info.label,
            &info.schema,
            now_ms,
        )?;
        edge_entries.push(entry);
        targets.push(GraphSchemaValidationTarget::Edge {
            label: info.label.clone(),
            label_id,
        });
    }

    node_entries.sort_unstable_by_key(|entry| entry.label_id);
    edge_entries.sort_unstable_by_key(|entry| entry.label_id);
    metadata_manifest.node_schemas = node_entries;
    metadata_manifest.edge_schemas = edge_entries;
    *manifest = metadata_manifest;
    Ok(targets)
}

fn node_schema_info_from_entry_with_catalog(
    entry: &NodeSchemaManifestEntry,
    catalog: &impl LabelCatalogLookup,
) -> Result<NodeSchemaInfo, EngineError> {
    node_schema_info_from_manifest(entry, |label_id| {
        catalog.node_label(label_id).map(str::to_string)
    })
}

fn edge_schema_info_from_entry_with_catalog(
    entry: &EdgeSchemaManifestEntry,
    catalog: &impl LabelCatalogLookup,
) -> Result<EdgeSchemaInfo, EngineError> {
    edge_schema_info_from_manifest(
        entry,
        |label_id| catalog.node_label(label_id).map(str::to_string),
        |label_id| catalog.edge_label(label_id).map(str::to_string),
    )
}

impl EngineCore {
    pub fn set_node_schema(
        &mut self,
        label: &str,
        schema: NodeSchema,
        options: SchemaSetOptions,
    ) -> Result<(NodeSchemaInfo, PublishImpact), EngineError> {
        let scan_options = SchemaScanOptions::from_set(&options)?;
        validate_node_schema_dense_vector_config(&schema, self.manifest.dense_vector.as_ref())?;
        let validation_ms = now_millis();
        let catalog_guard = self.label_catalog.read().unwrap();
        let mut label_plan = LabelResolutionPlan::from_catalog(&catalog_guard);
        let label_id = label_plan.resolve_node_label_for_write(label)?;
        let (schema_id, revision, created_at_ms) =
            next_node_schema_entry_metadata(&self.manifest, label_id, validation_ms)?;
        let entry = node_schema_manifest_entry_from_public(
            label_id,
            schema_id,
            revision,
            created_at_ms,
            validation_ms,
            &schema,
            |label| label_plan.resolve_node_label_for_write(label),
        )?;
        let (node_labels_to_create, edge_labels_to_create) = label_plan.token_creations();
        drop(label_plan);
        drop(catalog_guard);

        let mut proposed_manifest = self.manifest.clone();
        merge_runtime_label_catalog_into_manifest(&mut proposed_manifest, &self.label_catalog);
        stage_label_tokens_in_manifest(
            &mut proposed_manifest,
            &node_labels_to_create,
            &edge_labels_to_create,
        )?;
        upsert_node_schema_entry(&mut proposed_manifest, entry.clone())?;
        let proposed_catalog = RuntimeSchemaCatalog::from_manifest(&proposed_manifest)?;
        #[cfg(test)]
        self.pause_schema_validation_for_test();
        let report = validate_existing_nodes_for_schema(
            &self.read_view(),
            &proposed_manifest_runtime_label_catalog(&proposed_manifest)?,
            label_id,
            &proposed_catalog,
            scan_options,
        )?;
        if let Some(error) = schema_report_publication_error(&report) {
            return Err(error);
        }

        let publish_ms = now_millis();
        let mut published_entry = entry.clone();
        if revision == 1 {
            published_entry.created_at_ms = publish_ms;
        }
        published_entry.updated_at_ms = publish_ms;

        let entry_for_write = published_entry.clone();
        let label_catalog = Arc::clone(&self.label_catalog);
        self.with_runtime_manifest_write(|manifest| {
            merge_runtime_label_catalog_into_manifest(manifest, &label_catalog);
            stage_label_tokens_in_manifest(manifest, &node_labels_to_create, &edge_labels_to_create)?;
            upsert_node_schema_entry(manifest, entry_for_write)
        })?;
        self.apply_manifest_token_creations(&node_labels_to_create, &edge_labels_to_create)?;
        self.runtime_schema_catalog = RuntimeSchemaCatalog::from_manifest(&self.manifest)?;

        let catalog = self.label_catalog.read().unwrap();
        let info = node_schema_info_from_entry_with_catalog(&published_entry, &*catalog)?;
        Ok((info, PublishImpact::SnapshotWithLabelCatalog))
    }

    pub fn drop_node_schema(
        &mut self,
        label: &str,
    ) -> Result<(bool, PublishImpact), EngineError> {
        let label_id = {
            let catalog = self.label_catalog.read().unwrap();
            let Some(label_id) = resolve_node_label_for_read(&catalog, label)? else {
                return Ok((false, PublishImpact::NoPublish));
            };
            label_id
        };
        if !self
            .manifest
            .node_schemas
            .iter()
            .any(|entry| entry.label_id == label_id)
        {
            return Ok((false, PublishImpact::NoPublish));
        }
        let removed = self.with_runtime_manifest_write(|manifest| {
            let before = manifest.node_schemas.len();
            manifest
                .node_schemas
                .retain(|entry| entry.label_id != label_id);
            Ok(before != manifest.node_schemas.len())
        })?;
        if removed {
            self.runtime_schema_catalog = RuntimeSchemaCatalog::from_manifest(&self.manifest)?;
            Ok((true, PublishImpact::SnapshotOnly))
        } else {
            Ok((false, PublishImpact::NoPublish))
        }
    }

    pub fn set_edge_schema(
        &mut self,
        label: &str,
        schema: EdgeSchema,
        options: SchemaSetOptions,
    ) -> Result<(EdgeSchemaInfo, PublishImpact), EngineError> {
        let scan_options = SchemaScanOptions::from_set(&options)?;
        let validation_ms = now_millis();
        let catalog_guard = self.label_catalog.read().unwrap();
        let mut label_plan = LabelResolutionPlan::from_catalog(&catalog_guard);
        let label_id = label_plan.resolve_edge_label_for_write(label)?;
        let (schema_id, revision, created_at_ms) =
            next_edge_schema_entry_metadata(&self.manifest, label_id, validation_ms)?;
        let entry = edge_schema_manifest_entry_from_public(
            label_id,
            schema_id,
            revision,
            created_at_ms,
            validation_ms,
            &schema,
            |label| label_plan.resolve_node_label_for_write(label),
        )?;
        let (node_labels_to_create, edge_labels_to_create) = label_plan.token_creations();
        drop(label_plan);
        drop(catalog_guard);

        let mut proposed_manifest = self.manifest.clone();
        merge_runtime_label_catalog_into_manifest(&mut proposed_manifest, &self.label_catalog);
        stage_label_tokens_in_manifest(
            &mut proposed_manifest,
            &node_labels_to_create,
            &edge_labels_to_create,
        )?;
        upsert_edge_schema_entry(&mut proposed_manifest, entry.clone())?;
        let proposed_catalog = RuntimeSchemaCatalog::from_manifest(&proposed_manifest)?;
        #[cfg(test)]
        self.pause_schema_validation_for_test();
        let report = validate_existing_edges_for_schema(
            &self.read_view(),
            &proposed_manifest_runtime_label_catalog(&proposed_manifest)?,
            label_id,
            &proposed_catalog,
            scan_options,
        )?;
        if let Some(error) = schema_report_publication_error(&report) {
            return Err(error);
        }

        let publish_ms = now_millis();
        let mut published_entry = entry.clone();
        if revision == 1 {
            published_entry.created_at_ms = publish_ms;
        }
        published_entry.updated_at_ms = publish_ms;

        let entry_for_write = published_entry.clone();
        let label_catalog = Arc::clone(&self.label_catalog);
        self.with_runtime_manifest_write(|manifest| {
            merge_runtime_label_catalog_into_manifest(manifest, &label_catalog);
            stage_label_tokens_in_manifest(manifest, &node_labels_to_create, &edge_labels_to_create)?;
            upsert_edge_schema_entry(manifest, entry_for_write)
        })?;
        self.apply_manifest_token_creations(&node_labels_to_create, &edge_labels_to_create)?;
        self.runtime_schema_catalog = RuntimeSchemaCatalog::from_manifest(&self.manifest)?;

        let catalog = self.label_catalog.read().unwrap();
        let info = edge_schema_info_from_entry_with_catalog(&published_entry, &*catalog)?;
        Ok((info, PublishImpact::SnapshotWithLabelCatalog))
    }

    pub fn drop_edge_schema(
        &mut self,
        label: &str,
    ) -> Result<(bool, PublishImpact), EngineError> {
        let label_id = {
            let catalog = self.label_catalog.read().unwrap();
            let Some(label_id) = resolve_edge_label_for_read(&catalog, label)? else {
                return Ok((false, PublishImpact::NoPublish));
            };
            label_id
        };
        if !self
            .manifest
            .edge_schemas
            .iter()
            .any(|entry| entry.label_id == label_id)
        {
            return Ok((false, PublishImpact::NoPublish));
        }
        let removed = self.with_runtime_manifest_write(|manifest| {
            let before = manifest.edge_schemas.len();
            manifest
                .edge_schemas
                .retain(|entry| entry.label_id != label_id);
            Ok(before != manifest.edge_schemas.len())
        })?;
        if removed {
            self.runtime_schema_catalog = RuntimeSchemaCatalog::from_manifest(&self.manifest)?;
            Ok((true, PublishImpact::SnapshotOnly))
        } else {
            Ok((false, PublishImpact::NoPublish))
        }
    }

    pub fn set_graph_schema(
        &mut self,
        schema: GraphSchema,
        options: GraphSchemaSetOptions,
    ) -> Result<(GraphSchemaPublishResult, PublishImpact), EngineError> {
        validate_graph_schema_set_targets(&schema, "set_graph_schema")?;
        let scan_options = SchemaScanOptions::from_graph_set(&options)?;
        let validation_ms = now_millis();
        let current_node_label_ids: BTreeSet<u32> = self
            .manifest
            .node_schemas
            .iter()
            .map(|entry| entry.label_id)
            .collect();
        let current_edge_label_ids: BTreeSet<u32> = self
            .manifest
            .edge_schemas
            .iter()
            .map(|entry| entry.label_id)
            .collect();

        let catalog_guard = self.label_catalog.read().unwrap();
        let mut label_plan = LabelResolutionPlan::from_catalog(&catalog_guard);
        let mut proposed_manifest = self.manifest.clone();
        merge_runtime_label_catalog_into_manifest(&mut proposed_manifest, &self.label_catalog);
        let targets = apply_graph_schema_set_with_plan(
            &mut proposed_manifest,
            &schema,
            self.manifest.dense_vector.as_ref(),
            &mut label_plan,
            validation_ms,
        )?;
        let (node_labels_to_create, edge_labels_to_create) = label_plan.token_creations();
        drop(label_plan);
        drop(catalog_guard);

        stage_label_tokens_in_manifest(
            &mut proposed_manifest,
            &node_labels_to_create,
            &edge_labels_to_create,
        )?;
        let final_node_label_ids: BTreeSet<u32> = proposed_manifest
            .node_schemas
            .iter()
            .map(|entry| entry.label_id)
            .collect();
        let final_edge_label_ids: BTreeSet<u32> = proposed_manifest
            .edge_schemas
            .iter()
            .map(|entry| entry.label_id)
            .collect();
        let targets_dropped = current_node_label_ids
            .difference(&final_node_label_ids)
            .count()
            + current_edge_label_ids
                .difference(&final_edge_label_ids)
                .count();
        let node_schemas_dropped = current_node_label_ids
            .difference(&final_node_label_ids)
            .count();
        let edge_schemas_dropped = current_edge_label_ids
            .difference(&final_edge_label_ids)
            .count();
        let proposed_catalog = RuntimeSchemaCatalog::from_manifest(&proposed_manifest)?;
        let proposed_label_catalog = proposed_manifest_runtime_label_catalog(&proposed_manifest)?;
        #[cfg(test)]
        self.pause_schema_validation_for_test();
        let validation = validate_graph_schema_targets(
            &self.read_view(),
            &proposed_label_catalog,
            &targets,
            &proposed_catalog,
            scan_options,
            GraphSchemaOperationKind::Set,
        )?;
        if let Some(error) = graph_schema_publication_error(&validation) {
            return Err(error);
        }

        let publish_ms = now_millis();
        refresh_graph_schema_entry_publish_times(&mut proposed_manifest, &targets, publish_ms);
        let published_node_schemas = proposed_manifest.node_schemas.clone();
        let published_edge_schemas = proposed_manifest.edge_schemas.clone();
        let next_schema_id = proposed_manifest.next_schema_id;
        let label_catalog = Arc::clone(&self.label_catalog);
        self.with_runtime_manifest_write(|manifest| {
            merge_runtime_label_catalog_into_manifest(manifest, &label_catalog);
            stage_label_tokens_in_manifest(
                manifest,
                &node_labels_to_create,
                &edge_labels_to_create,
            )?;
            manifest.node_schemas = published_node_schemas;
            manifest.edge_schemas = published_edge_schemas;
            manifest.next_schema_id = next_schema_id;
            Ok(())
        })?;
        self.apply_manifest_token_creations(&node_labels_to_create, &edge_labels_to_create)?;
        self.runtime_schema_catalog = RuntimeSchemaCatalog::from_manifest(&self.manifest)?;

        let (node_schemas, edge_schemas) = self.current_graph_schema_infos()?;
        let result = GraphSchemaPublishResult {
            operation: GraphSchemaOperationKind::Set,
            node_schemas,
            edge_schemas,
            validation,
            targets_published: schema.node_schemas.len() + schema.edge_schemas.len(),
            targets_dropped,
            drop_targets: Vec::new(),
            node_schemas_dropped,
            edge_schemas_dropped,
        };
        Ok((
            result,
            graph_schema_publish_impact(&node_labels_to_create, &edge_labels_to_create),
        ))
    }

    pub fn alter_graph_schema(
        &mut self,
        operations: Vec<GraphSchemaOperation>,
        options: GraphSchemaSetOptions,
    ) -> Result<(GraphSchemaPublishResult, PublishImpact), EngineError> {
        match classify_graph_schema_operations(operations)? {
            GraphSchemaAlterPlan::Add(schema) => self.add_graph_schema(schema, options),
            GraphSchemaAlterPlan::Drop(drop_targets) => self.drop_graph_schema_targets(drop_targets),
        }
    }

    fn add_graph_schema(
        &mut self,
        schema: GraphSchema,
        options: GraphSchemaSetOptions,
    ) -> Result<(GraphSchemaPublishResult, PublishImpact), EngineError> {
        validate_graph_schema_set_targets(&schema, "alter_graph_schema")?;
        let scan_options = SchemaScanOptions::from_graph_set(&options)?;
        let validation_ms = now_millis();
        let catalog_guard = self.label_catalog.read().unwrap();
        let mut label_plan = LabelResolutionPlan::from_catalog(&catalog_guard);
        let mut proposed_manifest = self.manifest.clone();
        merge_runtime_label_catalog_into_manifest(&mut proposed_manifest, &self.label_catalog);
        let targets = apply_graph_schema_add_with_plan(
            &mut proposed_manifest,
            &schema,
            self.manifest.dense_vector.as_ref(),
            &mut label_plan,
            validation_ms,
        )?;
        let (node_labels_to_create, edge_labels_to_create) = label_plan.token_creations();
        drop(label_plan);
        drop(catalog_guard);

        stage_label_tokens_in_manifest(
            &mut proposed_manifest,
            &node_labels_to_create,
            &edge_labels_to_create,
        )?;
        let proposed_catalog = RuntimeSchemaCatalog::from_manifest(&proposed_manifest)?;
        let proposed_label_catalog = proposed_manifest_runtime_label_catalog(&proposed_manifest)?;
        #[cfg(test)]
        self.pause_schema_validation_for_test();
        let validation = validate_graph_schema_targets(
            &self.read_view(),
            &proposed_label_catalog,
            &targets,
            &proposed_catalog,
            scan_options,
            GraphSchemaOperationKind::Add,
        )?;
        if let Some(error) = graph_schema_publication_error(&validation) {
            return Err(error);
        }

        let publish_ms = now_millis();
        refresh_graph_schema_entry_publish_times(&mut proposed_manifest, &targets, publish_ms);
        let published_node_schemas = proposed_manifest.node_schemas.clone();
        let published_edge_schemas = proposed_manifest.edge_schemas.clone();
        let next_schema_id = proposed_manifest.next_schema_id;
        let label_catalog = Arc::clone(&self.label_catalog);
        self.with_runtime_manifest_write(|manifest| {
            merge_runtime_label_catalog_into_manifest(manifest, &label_catalog);
            stage_label_tokens_in_manifest(
                manifest,
                &node_labels_to_create,
                &edge_labels_to_create,
            )?;
            manifest.node_schemas = published_node_schemas;
            manifest.edge_schemas = published_edge_schemas;
            manifest.next_schema_id = next_schema_id;
            Ok(())
        })?;
        self.apply_manifest_token_creations(&node_labels_to_create, &edge_labels_to_create)?;
        self.runtime_schema_catalog = RuntimeSchemaCatalog::from_manifest(&self.manifest)?;

        let (node_schemas, edge_schemas) = self.current_graph_schema_infos()?;
        let result = GraphSchemaPublishResult {
            operation: GraphSchemaOperationKind::Add,
            node_schemas,
            edge_schemas,
            validation,
            targets_published: schema.node_schemas.len() + schema.edge_schemas.len(),
            targets_dropped: 0,
            drop_targets: Vec::new(),
            node_schemas_dropped: 0,
            edge_schemas_dropped: 0,
        };
        Ok((
            result,
            graph_schema_publish_impact(&node_labels_to_create, &edge_labels_to_create),
        ))
    }

    fn drop_graph_schema_targets(
        &mut self,
        mut drop_targets: Vec<GraphSchemaDropTargetResult>,
    ) -> Result<(GraphSchemaPublishResult, PublishImpact), EngineError> {
        let (node_label_ids, edge_label_ids, node_schemas_dropped, edge_schemas_dropped) = {
            let catalog = self.label_catalog.read().unwrap();
            let current_node_label_ids: BTreeSet<u32> = self
                .manifest
                .node_schemas
                .iter()
                .map(|entry| entry.label_id)
                .collect();
            let current_edge_label_ids: BTreeSet<u32> = self
                .manifest
                .edge_schemas
                .iter()
                .map(|entry| entry.label_id)
                .collect();
            let mut node_label_ids = BTreeSet::new();
            let mut edge_label_ids = BTreeSet::new();
            let mut node_schemas_dropped = 0usize;
            let mut edge_schemas_dropped = 0usize;
            for target in &mut drop_targets {
                match target.target_kind {
                    SchemaTargetKind::Node => {
                        if let Some(label_id) =
                            resolve_node_label_for_read(&catalog, &target.label)?
                        {
                            if current_node_label_ids.contains(&label_id) {
                                target.action = GraphSchemaDropAction::Dropped;
                                node_label_ids.insert(label_id);
                                node_schemas_dropped += 1;
                            }
                        }
                    }
                    SchemaTargetKind::Edge => {
                        if let Some(label_id) =
                            resolve_edge_label_for_read(&catalog, &target.label)?
                        {
                            if current_edge_label_ids.contains(&label_id) {
                                target.action = GraphSchemaDropAction::Dropped;
                                edge_label_ids.insert(label_id);
                                edge_schemas_dropped += 1;
                            }
                        }
                    }
                }
            }
            (
                node_label_ids,
                edge_label_ids,
                node_schemas_dropped,
                edge_schemas_dropped,
            )
        };
        let targets_dropped = node_schemas_dropped + edge_schemas_dropped;

        if targets_dropped > 0 {
            self.with_runtime_manifest_write(|manifest| {
                manifest
                    .node_schemas
                    .retain(|entry| !node_label_ids.contains(&entry.label_id));
                manifest
                    .edge_schemas
                    .retain(|entry| !edge_label_ids.contains(&entry.label_id));
                Ok(())
            })?;
            self.runtime_schema_catalog = RuntimeSchemaCatalog::from_manifest(&self.manifest)?;
        }

        let (node_schemas, edge_schemas) = self.current_graph_schema_infos()?;
        let result = GraphSchemaPublishResult {
            operation: GraphSchemaOperationKind::Drop,
            node_schemas,
            edge_schemas,
            validation: empty_graph_schema_report(GraphSchemaOperationKind::Drop),
            targets_published: 0,
            targets_dropped,
            drop_targets,
            node_schemas_dropped,
            edge_schemas_dropped,
        };
        Ok((
            result,
            if targets_dropped > 0 {
                PublishImpact::SnapshotOnly
            } else {
                PublishImpact::NoPublish
            },
        ))
    }

    pub fn drop_graph_schema(
        &mut self,
    ) -> Result<(GraphSchemaPublishResult, PublishImpact), EngineError> {
        let node_schemas_dropped = self.manifest.node_schemas.len();
        let edge_schemas_dropped = self.manifest.edge_schemas.len();
        let targets_dropped = node_schemas_dropped + edge_schemas_dropped;
        self.with_runtime_manifest_write(|manifest| {
            manifest.node_schemas.clear();
            manifest.edge_schemas.clear();
            Ok(())
        })?;
        self.runtime_schema_catalog = RuntimeSchemaCatalog::from_manifest(&self.manifest)?;

        Ok((
            GraphSchemaPublishResult {
                operation: GraphSchemaOperationKind::DropAll,
                node_schemas: Vec::new(),
                edge_schemas: Vec::new(),
                validation: empty_graph_schema_report(GraphSchemaOperationKind::DropAll),
                targets_published: 0,
                targets_dropped,
                drop_targets: Vec::new(),
                node_schemas_dropped,
                edge_schemas_dropped,
            },
            PublishImpact::SnapshotOnly,
        ))
    }

    fn current_graph_schema_infos(
        &self,
    ) -> Result<(Vec<NodeSchemaInfo>, Vec<EdgeSchemaInfo>), EngineError> {
        let catalog = self.label_catalog.read().unwrap();
        graph_schema_infos_from_manifest(&self.manifest, &*catalog)
    }
}

fn proposed_manifest_runtime_label_catalog(
    manifest: &ManifestState,
) -> Result<RuntimeLabelCatalog, EngineError> {
    RuntimeLabelCatalog::from_manifest(manifest)
}

fn refresh_graph_schema_entry_publish_times(
    manifest: &mut ManifestState,
    targets: &[GraphSchemaValidationTarget],
    publish_ms: i64,
) {
    let mut node_label_ids = BTreeSet::new();
    let mut edge_label_ids = BTreeSet::new();
    for target in targets {
        match target {
            GraphSchemaValidationTarget::Node { label_id, .. } => {
                node_label_ids.insert(*label_id);
            }
            GraphSchemaValidationTarget::Edge { label_id, .. } => {
                edge_label_ids.insert(*label_id);
            }
        }
    }
    for entry in &mut manifest.node_schemas {
        if !node_label_ids.contains(&entry.label_id) {
            continue;
        }
        if entry.revision == 1 {
            entry.created_at_ms = publish_ms;
        }
        entry.updated_at_ms = publish_ms;
    }
    for entry in &mut manifest.edge_schemas {
        if !edge_label_ids.contains(&entry.label_id) {
            continue;
        }
        if entry.revision == 1 {
            entry.created_at_ms = publish_ms;
        }
        entry.updated_at_ms = publish_ms;
    }
}

fn graph_schema_infos_from_manifest(
    manifest: &ManifestState,
    catalog: &impl LabelCatalogLookup,
) -> Result<(Vec<NodeSchemaInfo>, Vec<EdgeSchemaInfo>), EngineError> {
    let mut node_schemas = manifest
        .node_schemas
        .iter()
        .map(|entry| node_schema_info_from_entry_with_catalog(entry, catalog))
        .collect::<Result<Vec<_>, _>>()?;
    node_schemas.sort_unstable_by(|left, right| left.label.cmp(&right.label));

    let mut edge_schemas = manifest
        .edge_schemas
        .iter()
        .map(|entry| edge_schema_info_from_entry_with_catalog(entry, catalog))
        .collect::<Result<Vec<_>, _>>()?;
    edge_schemas.sort_unstable_by(|left, right| left.label.cmp(&right.label));

    Ok((node_schemas, edge_schemas))
}

impl DatabaseEngine {
    pub fn set_graph_schema(
        &self,
        schema: GraphSchema,
        options: GraphSchemaSetOptions,
    ) -> Result<GraphSchemaPublishResult, EngineError> {
        match self
            .runtime
            .submit_core_write(CoreWriteRequest::SetGraphSchema { schema, options })?
        {
            CoreWriteReply::GraphSchemaPublishResult(result) => Ok(result),
            _ => Err(unexpected_bulk_graph_schema_reply("set_graph_schema")),
        }
    }

    pub fn alter_graph_schema(
        &self,
        operations: Vec<GraphSchemaOperation>,
        options: GraphSchemaSetOptions,
    ) -> Result<GraphSchemaPublishResult, EngineError> {
        match self
            .runtime
            .submit_core_write(CoreWriteRequest::AlterGraphSchema {
                operations,
                options,
            })?
        {
            CoreWriteReply::GraphSchemaPublishResult(result) => Ok(result),
            _ => Err(unexpected_bulk_graph_schema_reply("alter_graph_schema")),
        }
    }

    pub fn check_graph_schema_set(
        &self,
        schema: GraphSchema,
        options: GraphSchemaCheckOptions,
    ) -> Result<GraphSchemaCheckReport, EngineError> {
        validate_graph_schema_set_targets(&schema, "check_graph_schema_set")?;
        let scan_options = SchemaScanOptions::from_graph_check(&options)?;
        let (_guard, published) = self.runtime.published_snapshot()?;
        let mut manifest =
            schema_snapshot_manifest(&published.schema_catalog, &published.label_catalog);
        let now_ms = now_millis();
        let targets = apply_graph_schema_set_with_temp_labels(
            &mut manifest,
            &schema,
            published.view.sources.manifest.dense_vector.as_ref(),
            now_ms,
        )?;
        let proposed_catalog = RuntimeSchemaCatalog::from_manifest(&manifest)?;
        let label_catalog = RuntimeLabelCatalog::from_manifest(&manifest)?;
        validate_graph_schema_targets(
            published.view.as_ref(),
            &label_catalog,
            &targets,
            &proposed_catalog,
            scan_options,
            GraphSchemaOperationKind::CheckSet,
        )
    }

    pub fn check_graph_schema_add(
        &self,
        schema: GraphSchema,
        options: GraphSchemaCheckOptions,
    ) -> Result<GraphSchemaCheckReport, EngineError> {
        if graph_schema_is_empty(&schema) {
            return Err(EngineError::InvalidOperation(
                "invalid graph schema: check_graph_schema_add requires at least one schema"
                    .to_string(),
            ));
        }
        validate_graph_schema_set_targets(&schema, "check_graph_schema_add")?;
        let scan_options = SchemaScanOptions::from_graph_check(&options)?;
        let (_guard, published) = self.runtime.published_snapshot()?;
        let mut manifest =
            schema_snapshot_manifest(&published.schema_catalog, &published.label_catalog);
        let now_ms = now_millis();
        let targets = apply_graph_schema_add_with_temp_labels(
            &mut manifest,
            &schema,
            published.view.sources.manifest.dense_vector.as_ref(),
            now_ms,
        )?;
        let proposed_catalog = RuntimeSchemaCatalog::from_manifest(&manifest)?;
        let label_catalog = RuntimeLabelCatalog::from_manifest(&manifest)?;
        validate_graph_schema_targets(
            published.view.as_ref(),
            &label_catalog,
            &targets,
            &proposed_catalog,
            scan_options,
            GraphSchemaOperationKind::CheckAdd,
        )
    }

    pub fn drop_graph_schema(&self) -> Result<GraphSchemaPublishResult, EngineError> {
        match self
            .runtime
            .submit_core_write(CoreWriteRequest::DropGraphSchema)?
        {
            CoreWriteReply::GraphSchemaPublishResult(result) => Ok(result),
            _ => Err(unexpected_bulk_graph_schema_reply("drop_graph_schema")),
        }
    }

    pub fn set_node_schema(
        &self,
        label: &str,
        schema: NodeSchema,
    ) -> Result<NodeSchemaInfo, EngineError> {
        self.set_node_schema_with_options(label, schema, SchemaSetOptions::default())
    }

    pub fn set_node_schema_with_options(
        &self,
        label: &str,
        schema: NodeSchema,
        options: SchemaSetOptions,
    ) -> Result<NodeSchemaInfo, EngineError> {
        match self
            .runtime
            .submit_core_write(CoreWriteRequest::SetNodeSchema {
                label: label.to_string(),
                schema,
                options,
            })? {
            CoreWriteReply::NodeSchemaInfo(info) => Ok(info),
            _ => unreachable!("set_node_schema must return node schema info"),
        }
    }

    pub fn check_node_schema(
        &self,
        label: &str,
        schema: NodeSchema,
        options: SchemaCheckOptions,
    ) -> Result<SchemaValidationReport, EngineError> {
        let scan_options = SchemaScanOptions::from_check(&options)?;
        let (_guard, published) = self.runtime.published_snapshot()?;
        validate_node_schema_dense_vector_config(
            &schema,
            published.view.sources.manifest.dense_vector.as_ref(),
        )?;
        let mut manifest =
            schema_snapshot_manifest(&published.schema_catalog, &published.label_catalog);
        let label_id = reserve_temp_node_label(&mut manifest, label)?;
        let now_ms = now_millis();
        let (schema_id, revision, created_at_ms) =
            next_node_schema_entry_metadata(&manifest, label_id, now_ms)?;
        let entry = node_schema_manifest_entry_from_public(
            label_id,
            schema_id,
            revision,
            created_at_ms,
            now_ms,
            &schema,
            |label| reserve_temp_node_label(&mut manifest, label),
        )?;
        upsert_node_schema_entry(&mut manifest, entry)?;
        let proposed_catalog = RuntimeSchemaCatalog::from_manifest(&manifest)?;
        let label_catalog = RuntimeLabelCatalog::from_manifest(&manifest)?;
        validate_existing_nodes_for_schema(
            published.view.as_ref(),
            &label_catalog,
            label_id,
            &proposed_catalog,
            scan_options,
        )
    }

    pub fn drop_node_schema(&self, label: &str) -> Result<bool, EngineError> {
        match self
            .runtime
            .submit_core_write(CoreWriteRequest::DropNodeSchema {
                label: label.to_string(),
            })? {
            CoreWriteReply::Bool(removed) => Ok(removed),
            _ => unreachable!("drop_node_schema must return bool"),
        }
    }

    pub fn get_node_schema(&self, label: &str) -> Result<Option<NodeSchemaInfo>, EngineError> {
        let (_guard, published) = self.runtime.published_snapshot()?;
        let Some(label_id) = published.label_catalog.resolve_node_label_for_read(label)? else {
            return Ok(None);
        };
        let Some(entry) = published
            .schema_catalog
            .node_schemas
            .iter()
            .find(|entry| entry.label_id == label_id)
        else {
            return Ok(None);
        };
        node_schema_info_from_entry_with_catalog(entry, published.label_catalog.as_ref()).map(Some)
    }

    pub fn list_node_schemas(&self) -> Result<Vec<NodeSchemaInfo>, EngineError> {
        let (_guard, published) = self.runtime.published_snapshot()?;
        let mut infos = published
            .schema_catalog
            .node_schemas
            .iter()
            .map(|entry| {
                node_schema_info_from_entry_with_catalog(entry, published.label_catalog.as_ref())
            })
            .collect::<Result<Vec<_>, _>>()?;
        infos.sort_unstable_by(|left, right| left.label.cmp(&right.label));
        Ok(infos)
    }

    pub fn set_edge_schema(
        &self,
        label: &str,
        schema: EdgeSchema,
    ) -> Result<EdgeSchemaInfo, EngineError> {
        self.set_edge_schema_with_options(label, schema, SchemaSetOptions::default())
    }

    pub fn set_edge_schema_with_options(
        &self,
        label: &str,
        schema: EdgeSchema,
        options: SchemaSetOptions,
    ) -> Result<EdgeSchemaInfo, EngineError> {
        match self
            .runtime
            .submit_core_write(CoreWriteRequest::SetEdgeSchema {
                label: label.to_string(),
                schema,
                options,
            })? {
            CoreWriteReply::EdgeSchemaInfo(info) => Ok(info),
            _ => unreachable!("set_edge_schema must return edge schema info"),
        }
    }

    pub fn check_edge_schema(
        &self,
        label: &str,
        schema: EdgeSchema,
        options: SchemaCheckOptions,
    ) -> Result<SchemaValidationReport, EngineError> {
        let scan_options = SchemaScanOptions::from_check(&options)?;
        let (_guard, published) = self.runtime.published_snapshot()?;
        let mut manifest =
            schema_snapshot_manifest(&published.schema_catalog, &published.label_catalog);
        let label_id = reserve_temp_edge_label(&mut manifest, label)?;
        let now_ms = now_millis();
        let (schema_id, revision, created_at_ms) =
            next_edge_schema_entry_metadata(&manifest, label_id, now_ms)?;
        let entry = edge_schema_manifest_entry_from_public(
            label_id,
            schema_id,
            revision,
            created_at_ms,
            now_ms,
            &schema,
            |label| reserve_temp_node_label(&mut manifest, label),
        )?;
        upsert_edge_schema_entry(&mut manifest, entry)?;
        let proposed_catalog = RuntimeSchemaCatalog::from_manifest(&manifest)?;
        let label_catalog = RuntimeLabelCatalog::from_manifest(&manifest)?;
        validate_existing_edges_for_schema(
            published.view.as_ref(),
            &label_catalog,
            label_id,
            &proposed_catalog,
            scan_options,
        )
    }

    pub fn drop_edge_schema(&self, label: &str) -> Result<bool, EngineError> {
        match self
            .runtime
            .submit_core_write(CoreWriteRequest::DropEdgeSchema {
                label: label.to_string(),
            })? {
            CoreWriteReply::Bool(removed) => Ok(removed),
            _ => unreachable!("drop_edge_schema must return bool"),
        }
    }

    pub fn get_edge_schema(&self, label: &str) -> Result<Option<EdgeSchemaInfo>, EngineError> {
        let (_guard, published) = self.runtime.published_snapshot()?;
        let Some(label_id) = published.label_catalog.resolve_edge_label_for_read(label)? else {
            return Ok(None);
        };
        let Some(entry) = published
            .schema_catalog
            .edge_schemas
            .iter()
            .find(|entry| entry.label_id == label_id)
        else {
            return Ok(None);
        };
        edge_schema_info_from_entry_with_catalog(entry, published.label_catalog.as_ref()).map(Some)
    }

    pub fn list_edge_schemas(&self) -> Result<Vec<EdgeSchemaInfo>, EngineError> {
        let (_guard, published) = self.runtime.published_snapshot()?;
        let mut infos = published
            .schema_catalog
            .edge_schemas
            .iter()
            .map(|entry| {
                edge_schema_info_from_entry_with_catalog(entry, published.label_catalog.as_ref())
            })
            .collect::<Result<Vec<_>, _>>()?;
        infos.sort_unstable_by(|left, right| left.label.cmp(&right.label));
        Ok(infos)
    }
}
