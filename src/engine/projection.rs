#[allow(dead_code)]
mod projection_impl {
use super::*;

impl DatabaseEngine {
    pub(crate) fn project_node_id_rows(
        &self,
        ids: &[u64],
        plan: &RowProjectionPlan,
        truncated: bool,
    ) -> Result<ProjectedRows, EngineError> {
        let (_guard, published) = self.runtime.published_snapshot()?;
        published.view.project_node_id_rows(ids, plan, truncated)
    }

    pub(crate) fn project_edge_id_rows(
        &self,
        ids: &[u64],
        plan: &RowProjectionPlan,
        truncated: bool,
    ) -> Result<ProjectedRows, EngineError> {
        let (_guard, published) = self.runtime.published_snapshot()?;
        published.view.project_edge_id_rows(ids, plan, truncated)
    }


}

impl ReadView {
    pub(crate) fn project_node_id_rows(
        &self,
        ids: &[u64],
        plan: &RowProjectionPlan,
        truncated: bool,
    ) -> Result<ProjectedRows, EngineError> {
        validate_direct_node_plan(plan)?;
        let needs = plan.needs.merged_read_needs()?;
        validate_direct_node_needs(&needs)?;
        let node_needs = needs.nodes.get(DIRECT_NODE_ALIAS);
        let selected = if let Some(needs) = node_needs {
            self.read_selected_nodes_by_id(ids, needs)?
        } else {
            NodeIdMap::default()
        };

        let mut rows = Vec::with_capacity(ids.len());
        for &node_id in ids {
            let fields = node_needs
                .map(|_| selected_node_fields_for_id(&selected, node_id))
                .transpose()?;
            let mut values = Vec::with_capacity(plan.columns.len());
            for column in &plan.columns {
                values.push(project_direct_node_column(
                    column,
                    node_id,
                    fields,
                    &self.label_catalog,
                )?);
            }
            rows.push(ProjectedRow { values });
        }

        Ok(ProjectedRows {
            columns: plan.column_names(),
            rows,
            truncated,
        })
    }

    pub(crate) fn project_edge_id_rows(
        &self,
        ids: &[u64],
        plan: &RowProjectionPlan,
        truncated: bool,
    ) -> Result<ProjectedRows, EngineError> {
        validate_direct_edge_plan(plan)?;
        let needs = plan.needs.merged_read_needs()?;
        validate_direct_edge_needs(&needs)?;
        let edge_needs = needs.edges.get(DIRECT_EDGE_ALIAS);
        let selected = if let Some(needs) = edge_needs {
            self.read_selected_edges_by_id(ids, needs)?
        } else {
            NodeIdMap::default()
        };

        let mut rows = Vec::with_capacity(ids.len());
        for &edge_id in ids {
            let fields = edge_needs
                .map(|_| selected_edge_fields_for_id(&selected, edge_id))
                .transpose()?;
            let mut values = Vec::with_capacity(plan.columns.len());
            for column in &plan.columns {
                values.push(project_direct_edge_column(
                    column,
                    edge_id,
                    fields,
                    &self.label_catalog,
                )?);
            }
            rows.push(ProjectedRow { values });
        }

        Ok(ProjectedRows {
            columns: plan.column_names(),
            rows,
            truncated,
        })
    }

    fn read_selected_nodes_by_id(
        &self,
        ids: &[u64],
        needs: &NodeSelectedFieldNeeds,
    ) -> Result<NodeIdMap<SelectedNodeFields>, EngineError> {
        let unique_ids = sorted_unique_ids(ids);
        let selected = self.sources().find_node_projected_fields(&unique_ids, needs)?;
        let mut by_id =
            NodeIdMap::with_capacity_and_hasher(unique_ids.len(), Default::default());
        for (node_id, fields) in unique_ids.into_iter().zip(selected.into_iter()) {
            if let Some(fields) = fields {
                by_id.insert(node_id, fields);
            }
        }
        Ok(by_id)
    }

    fn read_selected_edges_by_id(
        &self,
        ids: &[u64],
        needs: &EdgeSelectedFieldNeeds,
    ) -> Result<NodeIdMap<SelectedEdgeFields>, EngineError> {
        let unique_ids = sorted_unique_ids(ids);
        let selected = self.sources().find_edge_projected_fields(&unique_ids, needs)?;
        let mut by_id =
            NodeIdMap::with_capacity_and_hasher(unique_ids.len(), Default::default());
        for (edge_id, fields) in unique_ids.into_iter().zip(selected.into_iter()) {
            if let Some(fields) = fields {
                by_id.insert(edge_id, fields);
            }
        }
        Ok(by_id)
    }
}

fn sorted_unique_ids(ids: &[u64]) -> Vec<u64> {
    let mut unique = ids.to_vec();
    unique.sort_unstable();
    unique.dedup();
    unique
}

fn validate_direct_node_plan(plan: &RowProjectionPlan) -> Result<(), EngineError> {
    for column in &plan.columns {
        match column {
            ProjectionColumn::NodeAlias { alias, .. }
            | ProjectionColumn::NodeProperty { alias, .. }
            | ProjectionColumn::NodeMetadata { alias, .. } if alias == DIRECT_NODE_ALIAS => {}
            ProjectionColumn::NodeAlias { alias, .. }
            | ProjectionColumn::NodeProperty { alias, .. }
            | ProjectionColumn::NodeMetadata { alias, .. } => {
                return Err(EngineError::InvalidOperation(format!(
                    "direct node projection column uses unsupported alias '{alias}'"
                )));
            }
            ProjectionColumn::EdgeAlias { .. }
            | ProjectionColumn::EdgeProperty { .. }
            | ProjectionColumn::EdgeMetadata { .. } => {
                return Err(EngineError::InvalidOperation(
                    "direct node projection cannot contain edge columns".to_string(),
                ));
            }
        }
    }
    Ok(())
}

fn validate_direct_node_needs(needs: &EntityProjectionNeeds) -> Result<(), EngineError> {
    if !needs.edges.is_empty() {
        return Err(EngineError::InvalidOperation(
            "direct node projection cannot contain edge read needs".to_string(),
        ));
    }
    if !needs.paths.is_empty() {
        return Err(EngineError::InvalidOperation(
            "direct node projection cannot contain path read needs".to_string(),
        ));
    }
    if !needs.hidden_edges.is_empty() || !needs.hidden_paths.is_empty() {
        return Err(EngineError::InvalidOperation(
            "direct node projection cannot contain hidden occurrence read needs".to_string(),
        ));
    }
    for alias in needs.nodes.keys() {
        if alias != DIRECT_NODE_ALIAS {
            return Err(EngineError::InvalidOperation(format!(
                "direct node projection read needs use unsupported alias '{alias}'"
            )));
        }
    }
    Ok(())
}

fn validate_direct_edge_plan(plan: &RowProjectionPlan) -> Result<(), EngineError> {
    for column in &plan.columns {
        match column {
            ProjectionColumn::EdgeAlias { alias, .. }
            | ProjectionColumn::EdgeProperty { alias, .. }
            | ProjectionColumn::EdgeMetadata { alias, .. } if alias == DIRECT_EDGE_ALIAS => {}
            ProjectionColumn::EdgeAlias { alias, .. }
            | ProjectionColumn::EdgeProperty { alias, .. }
            | ProjectionColumn::EdgeMetadata { alias, .. } => {
                return Err(EngineError::InvalidOperation(format!(
                    "direct edge projection column uses unsupported alias '{alias}'"
                )));
            }
            ProjectionColumn::NodeAlias { .. }
            | ProjectionColumn::NodeProperty { .. }
            | ProjectionColumn::NodeMetadata { .. } => {
                return Err(EngineError::InvalidOperation(
                    "direct edge projection cannot contain node columns".to_string(),
                ));
            }
        }
    }
    Ok(())
}

fn validate_direct_edge_needs(needs: &EntityProjectionNeeds) -> Result<(), EngineError> {
    if !needs.nodes.is_empty() {
        return Err(EngineError::InvalidOperation(
            "direct edge projection cannot contain node read needs".to_string(),
        ));
    }
    if !needs.paths.is_empty() {
        return Err(EngineError::InvalidOperation(
            "direct edge projection cannot contain path read needs".to_string(),
        ));
    }
    if !needs.hidden_edges.is_empty() || !needs.hidden_paths.is_empty() {
        return Err(EngineError::InvalidOperation(
            "direct edge projection cannot contain hidden occurrence read needs".to_string(),
        ));
    }
    for alias in needs.edges.keys() {
        if alias != DIRECT_EDGE_ALIAS {
            return Err(EngineError::InvalidOperation(format!(
                "direct edge projection read needs use unsupported alias '{alias}'"
            )));
        }
    }
    Ok(())
}

fn selected_node_fields_for_id(
    selected: &NodeIdMap<SelectedNodeFields>,
    node_id: u64,
) -> Result<&SelectedNodeFields, EngineError> {
    selected.get(&node_id).ok_or_else(|| {
        EngineError::InvalidOperation(format!(
            "projected node row references missing visible node {node_id}"
        ))
    })
}

fn selected_edge_fields_for_id(
    selected: &NodeIdMap<SelectedEdgeFields>,
    edge_id: u64,
) -> Result<&SelectedEdgeFields, EngineError> {
    selected.get(&edge_id).ok_or_else(|| {
        EngineError::InvalidOperation(format!(
            "projected edge row references missing visible edge {edge_id}"
        ))
    })
}

fn project_direct_node_column(
    column: &ProjectionColumn,
    node_id: u64,
    fields: Option<&SelectedNodeFields>,
    catalog: &ReadLabelCatalogSnapshot,
) -> Result<ProjectedValue, EngineError> {
    match column {
        ProjectionColumn::NodeAlias { projection, .. } => {
            Ok(ProjectedValue::Node(projected_node_from_fields(
                node_id,
                projection,
                fields,
                catalog,
            )?))
        }
        ProjectionColumn::NodeProperty { key, .. } => {
            let fields = fields
                .ok_or_else(|| missing_node_selected_fields_error(node_id))?;
            Ok(ProjectedValue::from_optional_prop(fields.props.get(key)))
        }
        ProjectionColumn::NodeMetadata { field, .. } => {
            project_node_metadata_value(node_id, *field, fields, catalog)
        }
        ProjectionColumn::EdgeAlias { .. }
        | ProjectionColumn::EdgeProperty { .. }
        | ProjectionColumn::EdgeMetadata { .. } => unreachable!("direct node plan was validated"),
    }
}

fn project_direct_edge_column(
    column: &ProjectionColumn,
    edge_id: u64,
    fields: Option<&SelectedEdgeFields>,
    catalog: &ReadLabelCatalogSnapshot,
) -> Result<ProjectedValue, EngineError> {
    match column {
        ProjectionColumn::EdgeAlias { projection, .. } => {
            Ok(ProjectedValue::Edge(projected_edge_from_fields(
                edge_id,
                projection,
                fields,
                catalog,
            )?))
        }
        ProjectionColumn::EdgeProperty { key, .. } => {
            let fields = fields
                .ok_or_else(|| missing_edge_selected_fields_error(edge_id))?;
            Ok(ProjectedValue::from_optional_prop(fields.props.get(key)))
        }
        ProjectionColumn::EdgeMetadata { field, .. } => {
            project_edge_metadata_value(edge_id, *field, fields, catalog)
        }
        ProjectionColumn::NodeAlias { .. }
        | ProjectionColumn::NodeProperty { .. }
        | ProjectionColumn::NodeMetadata { .. } => unreachable!("direct edge plan was validated"),
    }
}

fn projected_node_from_fields(
    node_id: u64,
    projection: &NodeOutputProjection,
    fields: Option<&SelectedNodeFields>,
    catalog: &ReadLabelCatalogSnapshot,
) -> Result<ProjectedNode, EngineError> {
    let source = if projection_node_needs_source(projection) {
        Some(fields.ok_or_else(|| missing_node_selected_fields_error(node_id))?)
    } else {
        None
    };
    Ok(ProjectedNode {
        id: projection.id.then_some(node_id),
        labels: if projection.labels {
            Some(resolve_projected_node_labels(
                node_id,
                source.expect("labels require selected fields").meta.label_ids,
                catalog,
            )?)
        } else {
            None
        },
        key: if projection.key {
            Some(
                source
                    .expect("key requires selected fields")
                    .key
                    .as_ref()
                    .ok_or_else(|| missing_node_selected_fields_error(node_id))?
                    .clone(),
            )
        } else {
            None
        },
        props: projected_node_props(node_id, projection, source)?,
        weight: projection
            .weight
            .then(|| source.expect("weight requires selected fields").meta.weight),
        created_at: if projection.created_at {
            Some(
                source
                    .expect("created_at requires selected fields")
                    .created_at
                    .ok_or_else(|| missing_node_selected_fields_error(node_id))?,
            )
        } else {
            None
        },
        updated_at: projection
            .updated_at
            .then(|| source.expect("updated_at requires selected fields").meta.updated_at),
        dense_vector: if projection.vectors.needs_dense() {
            source
                .expect("dense vector requires selected fields")
                .dense_vector
                .clone()
        } else {
            None
        },
        sparse_vector: if projection.vectors.needs_sparse() {
            source
                .expect("sparse vector requires selected fields")
                .sparse_vector
                .clone()
        } else {
            None
        },
    })
}

fn projected_edge_from_fields(
    edge_id: u64,
    projection: &EdgeOutputProjection,
    fields: Option<&SelectedEdgeFields>,
    catalog: &ReadLabelCatalogSnapshot,
) -> Result<ProjectedEdge, EngineError> {
    let source = if projection_edge_needs_source(projection) {
        Some(fields.ok_or_else(|| missing_edge_selected_fields_error(edge_id))?)
    } else {
        None
    };
    Ok(ProjectedEdge {
        id: projection.id.then_some(edge_id),
        from: projection
            .from
            .then(|| source.expect("from requires selected fields").meta.from),
        to: projection
            .to
            .then(|| source.expect("to requires selected fields").meta.to),
        label: if projection.label {
            Some(resolve_projected_edge_label(
                edge_id,
                source.expect("label requires selected fields").meta.label_id,
                catalog,
            )?)
        } else {
            None
        },
        props: projected_edge_props(edge_id, projection, source)?,
        weight: projection
            .weight
            .then(|| source.expect("weight requires selected fields").meta.weight),
        created_at: if projection.created_at {
            Some(
                source
                    .expect("created_at requires selected fields")
                    .created_at
                    .ok_or_else(|| missing_edge_selected_fields_error(edge_id))?,
            )
        } else {
            None
        },
        updated_at: projection
            .updated_at
            .then(|| source.expect("updated_at requires selected fields").meta.updated_at),
        valid_from: projection
            .valid_from
            .then(|| source.expect("valid_from requires selected fields").meta.valid_from),
        valid_to: projection
            .valid_to
            .then(|| source.expect("valid_to requires selected fields").meta.valid_to),
    })
}

fn projection_node_needs_source(projection: &NodeOutputProjection) -> bool {
    projection.labels
        || projection.key
        || !matches!(projection.props, PropertySelection::None)
        || projection.weight
        || projection.created_at
        || projection.updated_at
        || projection.vectors.needs_dense()
        || projection.vectors.needs_sparse()
}

fn projection_edge_needs_source(projection: &EdgeOutputProjection) -> bool {
    projection.from
        || projection.to
        || projection.label
        || !matches!(projection.props, PropertySelection::None)
        || projection.weight
        || projection.created_at
        || projection.updated_at
        || projection.valid_from
        || projection.valid_to
}

fn projected_node_props(
    node_id: u64,
    projection: &NodeOutputProjection,
    source: Option<&SelectedNodeFields>,
) -> Result<Option<BTreeMap<String, ProjectedValue>>, EngineError> {
    match &projection.props {
        PropertySelection::None => Ok(None),
        PropertySelection::Keys(keys) => {
            let source = source.ok_or_else(|| missing_node_selected_fields_error(node_id))?;
            let mut props = BTreeMap::new();
            for key in keys {
                if let Some(value) = source.props.get(key) {
                    props.insert(key.clone(), ProjectedValue::from(value));
                }
            }
            Ok(Some(props))
        }
        PropertySelection::All => {
            let source = source.ok_or_else(|| missing_node_selected_fields_error(node_id))?;
            Ok(Some(
                source
                    .props
                    .iter()
                    .map(|(key, value)| (key.clone(), ProjectedValue::from(value)))
                    .collect(),
            ))
        }
    }
}

fn projected_edge_props(
    edge_id: u64,
    projection: &EdgeOutputProjection,
    source: Option<&SelectedEdgeFields>,
) -> Result<Option<BTreeMap<String, ProjectedValue>>, EngineError> {
    match &projection.props {
        PropertySelection::None => Ok(None),
        PropertySelection::Keys(keys) => {
            let source = source.ok_or_else(|| missing_edge_selected_fields_error(edge_id))?;
            let mut props = BTreeMap::new();
            for key in keys {
                if let Some(value) = source.props.get(key) {
                    props.insert(key.clone(), ProjectedValue::from(value));
                }
            }
            Ok(Some(props))
        }
        PropertySelection::All => {
            let source = source.ok_or_else(|| missing_edge_selected_fields_error(edge_id))?;
            Ok(Some(
                source
                    .props
                    .iter()
                    .map(|(key, value)| (key.clone(), ProjectedValue::from(value)))
                    .collect(),
            ))
        }
    }
}

fn project_node_metadata_value(
    node_id: u64,
    field: NodeProjectionField,
    fields: Option<&SelectedNodeFields>,
    catalog: &ReadLabelCatalogSnapshot,
) -> Result<ProjectedValue, EngineError> {
    match field {
        NodeProjectionField::Id => Ok(ProjectedValue::UInt(node_id)),
        NodeProjectionField::Labels => Ok(ProjectedValue::List(
            resolve_projected_node_labels(
                node_id,
                fields
                    .ok_or_else(|| missing_node_selected_fields_error(node_id))?
                    .meta
                    .label_ids,
                catalog,
            )?
            .into_iter()
            .map(ProjectedValue::String)
            .collect(),
        )),
        NodeProjectionField::Key => Ok(ProjectedValue::String(
            fields
                .ok_or_else(|| missing_node_selected_fields_error(node_id))?
                .key
                .as_ref()
                .ok_or_else(|| missing_node_selected_fields_error(node_id))?
                .clone(),
        )),
        NodeProjectionField::Weight => Ok(ProjectedValue::Float(
            fields
                .ok_or_else(|| missing_node_selected_fields_error(node_id))?
                .meta
                .weight as f64,
        )),
        NodeProjectionField::CreatedAt => Ok(ProjectedValue::Int(
            fields
                .ok_or_else(|| missing_node_selected_fields_error(node_id))?
                .created_at
                .ok_or_else(|| missing_node_selected_fields_error(node_id))?,
        )),
        NodeProjectionField::UpdatedAt => Ok(ProjectedValue::Int(
            fields
                .ok_or_else(|| missing_node_selected_fields_error(node_id))?
                .meta
                .updated_at,
        )),
    }
}

fn project_edge_metadata_value(
    edge_id: u64,
    field: EdgeProjectionField,
    fields: Option<&SelectedEdgeFields>,
    catalog: &ReadLabelCatalogSnapshot,
) -> Result<ProjectedValue, EngineError> {
    match field {
        EdgeProjectionField::Id => Ok(ProjectedValue::UInt(edge_id)),
        EdgeProjectionField::From => Ok(ProjectedValue::UInt(
            fields
                .ok_or_else(|| missing_edge_selected_fields_error(edge_id))?
                .meta
                .from,
        )),
        EdgeProjectionField::To => Ok(ProjectedValue::UInt(
            fields
                .ok_or_else(|| missing_edge_selected_fields_error(edge_id))?
                .meta
                .to,
        )),
        EdgeProjectionField::Label => Ok(ProjectedValue::String(resolve_projected_edge_label(
            edge_id,
            fields
                .ok_or_else(|| missing_edge_selected_fields_error(edge_id))?
                .meta
                .label_id,
            catalog,
        )?)),
        EdgeProjectionField::Weight => Ok(ProjectedValue::Float(
            fields
                .ok_or_else(|| missing_edge_selected_fields_error(edge_id))?
                .meta
                .weight as f64,
        )),
        EdgeProjectionField::CreatedAt => Ok(ProjectedValue::Int(
            fields
                .ok_or_else(|| missing_edge_selected_fields_error(edge_id))?
                .created_at
                .ok_or_else(|| missing_edge_selected_fields_error(edge_id))?,
        )),
        EdgeProjectionField::UpdatedAt => Ok(ProjectedValue::Int(
            fields
                .ok_or_else(|| missing_edge_selected_fields_error(edge_id))?
                .meta
                .updated_at,
        )),
        EdgeProjectionField::ValidFrom => Ok(ProjectedValue::Int(
            fields
                .ok_or_else(|| missing_edge_selected_fields_error(edge_id))?
                .meta
                .valid_from,
        )),
        EdgeProjectionField::ValidTo => Ok(ProjectedValue::Int(
            fields
                .ok_or_else(|| missing_edge_selected_fields_error(edge_id))?
                .meta
                .valid_to,
        )),
    }
}

fn resolve_projected_node_labels(
    node_id: u64,
    label_ids: NodeLabelSet,
    catalog: &ReadLabelCatalogSnapshot,
) -> Result<Vec<String>, EngineError> {
    let mut labels = Vec::with_capacity(label_ids.len());
    for &label_id in label_ids.as_slice() {
        labels.push(
            catalog
                .node_label(label_id)
                .ok_or_else(|| {
                    EngineError::InvalidOperation(format!(
                        "node record {} references missing node label_id {}",
                        node_id, label_id
                    ))
                })?
                .to_string(),
        );
    }
    Ok(labels)
}

fn resolve_projected_edge_label(
    edge_id: u64,
    label_id: u32,
    catalog: &ReadLabelCatalogSnapshot,
) -> Result<String, EngineError> {
    Ok(catalog
        .edge_label(label_id)
        .ok_or_else(|| {
            EngineError::InvalidOperation(format!(
                "edge record {} references missing edge-label label_id {}",
                edge_id, label_id
            ))
        })?
        .to_string())
}

fn missing_node_selected_fields_error(node_id: u64) -> EngineError {
    EngineError::InvalidOperation(format!(
        "projected node row references missing selected fields for node {node_id}"
    ))
}

fn missing_edge_selected_fields_error(edge_id: u64) -> EngineError {
    EngineError::InvalidOperation(format!(
        "projected edge row references missing selected fields for edge {edge_id}"
    ))
}

}
