//! Internal row projection value and needs substrate for future GQL output.
//!
//! This module deliberately stops at plan/value canonicalization. Source-backed
//! selected-field reads and row materialization live in later checkpoints.

#![allow(dead_code)]

use crate::error::EngineError;
use crate::types::PropValue;
use std::collections::{BTreeMap, BTreeSet};

pub(crate) const DIRECT_NODE_ALIAS: &str = "__node";
pub(crate) const DIRECT_EDGE_ALIAS: &str = "__edge";

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub(crate) enum PropertySelection {
    #[default]
    None,
    Keys(Vec<String>),
    All,
}

impl PropertySelection {
    pub(crate) fn canonicalized_for(
        self,
        need_class: ProjectionNeedClass,
    ) -> Result<Self, EngineError> {
        match self {
            Self::None => Ok(Self::None),
            Self::All if need_class.allows_all_properties() => Ok(Self::All),
            Self::All => Err(EngineError::InvalidOperation(format!(
                "PropertySelection::All is only valid for final output needs, not {} needs",
                need_class.name()
            ))),
            Self::Keys(keys) => canonicalize_property_keys(keys).map(Self::Keys),
        }
    }

    pub(crate) fn merge_from(
        &mut self,
        other: &Self,
        need_class: ProjectionNeedClass,
    ) -> Result<(), EngineError> {
        let left = self.clone().canonicalized_for(need_class)?;
        let right = other.clone().canonicalized_for(need_class)?;
        *self = match (left, right) {
            (Self::All, _) | (_, Self::All) => Self::All,
            (Self::None, selection) | (selection, Self::None) => selection,
            (Self::Keys(mut left), Self::Keys(right)) => {
                append_deduped_property_keys(&mut left, right)?;
                Self::Keys(left)
            }
        };
        Ok(())
    }

    pub(crate) fn lookup_keys(&self) -> Option<&[String]> {
        match self {
            Self::Keys(keys) => Some(keys),
            Self::None | Self::All => None,
        }
    }
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) enum VectorSelection {
    #[default]
    None,
    Dense,
    Sparse,
    Both,
}

impl VectorSelection {
    pub(crate) fn union(self, other: Self) -> Self {
        match (
            self.needs_dense() || other.needs_dense(),
            self.needs_sparse() || other.needs_sparse(),
        ) {
            (false, false) => Self::None,
            (true, false) => Self::Dense,
            (false, true) => Self::Sparse,
            (true, true) => Self::Both,
        }
    }

    pub(crate) fn needs_dense(self) -> bool {
        matches!(self, Self::Dense | Self::Both)
    }

    pub(crate) fn needs_sparse(self) -> bool {
        matches!(self, Self::Sparse | Self::Both)
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct NodeOutputProjection {
    pub(crate) id: bool,
    pub(crate) labels: bool,
    pub(crate) key: bool,
    pub(crate) props: PropertySelection,
    pub(crate) weight: bool,
    pub(crate) created_at: bool,
    pub(crate) updated_at: bool,
    pub(crate) vectors: VectorSelection,
}

impl NodeOutputProjection {
    pub(crate) fn id_only() -> Self {
        Self {
            id: true,
            labels: false,
            key: false,
            props: PropertySelection::None,
            weight: false,
            created_at: false,
            updated_at: false,
            vectors: VectorSelection::None,
        }
    }

    pub(crate) fn compact_element() -> Self {
        Self {
            id: true,
            labels: true,
            key: true,
            props: PropertySelection::None,
            weight: false,
            created_at: false,
            updated_at: false,
            vectors: VectorSelection::None,
        }
    }

    pub(crate) fn full_without_vectors() -> Self {
        Self {
            id: true,
            labels: true,
            key: true,
            props: PropertySelection::All,
            weight: true,
            created_at: true,
            updated_at: true,
            vectors: VectorSelection::None,
        }
    }

    pub(crate) fn full_with_vectors() -> Self {
        Self {
            vectors: VectorSelection::Both,
            ..Self::full_without_vectors()
        }
    }

    fn canonicalized(mut self) -> Result<Self, EngineError> {
        self.props = self.props.canonicalized_for(ProjectionNeedClass::Output)?;
        Ok(self)
    }

    fn requires_source_read(&self) -> bool {
        self.labels
            || self.key
            || !matches!(self.props, PropertySelection::None)
            || self.weight
            || self.created_at
            || self.updated_at
            || !matches!(self.vectors, VectorSelection::None)
    }

    fn selected_field_needs(&self) -> NodeSelectedFieldNeeds {
        NodeSelectedFieldNeeds {
            key: self.key,
            created_at: self.created_at,
            props: self.props.clone(),
            vectors: self.vectors,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct EdgeOutputProjection {
    pub(crate) id: bool,
    pub(crate) from: bool,
    pub(crate) to: bool,
    pub(crate) label: bool,
    pub(crate) props: PropertySelection,
    pub(crate) weight: bool,
    pub(crate) created_at: bool,
    pub(crate) updated_at: bool,
    pub(crate) valid_from: bool,
    pub(crate) valid_to: bool,
}

impl EdgeOutputProjection {
    pub(crate) fn id_only() -> Self {
        Self {
            id: true,
            from: false,
            to: false,
            label: false,
            props: PropertySelection::None,
            weight: false,
            created_at: false,
            updated_at: false,
            valid_from: false,
            valid_to: false,
        }
    }

    pub(crate) fn compact_element() -> Self {
        Self {
            id: true,
            from: true,
            to: true,
            label: true,
            props: PropertySelection::None,
            weight: false,
            created_at: false,
            updated_at: false,
            valid_from: false,
            valid_to: false,
        }
    }

    pub(crate) fn full() -> Self {
        Self {
            id: true,
            from: true,
            to: true,
            label: true,
            props: PropertySelection::All,
            weight: true,
            created_at: true,
            updated_at: true,
            valid_from: true,
            valid_to: true,
        }
    }

    fn canonicalized(mut self) -> Result<Self, EngineError> {
        self.props = self.props.canonicalized_for(ProjectionNeedClass::Output)?;
        Ok(self)
    }

    fn requires_source_read(&self) -> bool {
        self.from
            || self.to
            || self.label
            || !matches!(self.props, PropertySelection::None)
            || self.weight
            || self.created_at
            || self.updated_at
            || self.valid_from
            || self.valid_to
    }

    fn selected_field_needs(&self) -> EdgeSelectedFieldNeeds {
        EdgeSelectedFieldNeeds {
            created_at: self.created_at,
            props: self.props.clone(),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum ProjectionColumn {
    NodeAlias {
        alias: String,
        projection: NodeOutputProjection,
        output_name: String,
    },
    EdgeAlias {
        alias: String,
        projection: EdgeOutputProjection,
        output_name: String,
    },
    NodeProperty {
        alias: String,
        key: String,
        output_name: String,
    },
    EdgeProperty {
        alias: String,
        key: String,
        output_name: String,
    },
    NodeMetadata {
        alias: String,
        field: NodeProjectionField,
        output_name: String,
    },
    EdgeMetadata {
        alias: String,
        field: EdgeProjectionField,
        output_name: String,
    },
}

impl ProjectionColumn {
    pub(crate) fn output_name(&self) -> &str {
        match self {
            Self::NodeAlias { output_name, .. }
            | Self::EdgeAlias { output_name, .. }
            | Self::NodeProperty { output_name, .. }
            | Self::EdgeProperty { output_name, .. }
            | Self::NodeMetadata { output_name, .. }
            | Self::EdgeMetadata { output_name, .. } => output_name,
        }
    }

    fn canonicalized(self) -> Result<Self, EngineError> {
        match self {
            Self::NodeAlias {
                alias,
                projection,
                output_name,
            } => Ok(Self::NodeAlias {
                alias,
                projection: projection.canonicalized()?,
                output_name,
            }),
            Self::EdgeAlias {
                alias,
                projection,
                output_name,
            } => Ok(Self::EdgeAlias {
                alias,
                projection: projection.canonicalized()?,
                output_name,
            }),
            Self::NodeProperty {
                alias,
                key,
                output_name,
            } => {
                validate_property_key(&key)?;
                Ok(Self::NodeProperty {
                    alias,
                    key,
                    output_name,
                })
            }
            Self::EdgeProperty {
                alias,
                key,
                output_name,
            } => {
                validate_property_key(&key)?;
                Ok(Self::EdgeProperty {
                    alias,
                    key,
                    output_name,
                })
            }
            Self::NodeMetadata {
                alias,
                field,
                output_name,
            } => Ok(Self::NodeMetadata {
                alias,
                field,
                output_name,
            }),
            Self::EdgeMetadata {
                alias,
                field,
                output_name,
            } => Ok(Self::EdgeMetadata {
                alias,
                field,
                output_name,
            }),
        }
    }

    fn accumulate_needs(
        &self,
        needs: &mut EntityProjectionNeeds,
        need_class: ProjectionNeedClass,
    ) -> Result<(), EngineError> {
        match self {
            Self::NodeAlias {
                alias, projection, ..
            } => {
                if projection.requires_source_read() {
                    needs.merge_node_needs(alias, projection.selected_field_needs(), need_class)?;
                }
            }
            Self::EdgeAlias {
                alias, projection, ..
            } => {
                if projection.requires_source_read() {
                    needs.merge_edge_needs(alias, projection.selected_field_needs(), need_class)?;
                }
            }
            Self::NodeProperty { alias, key, .. } => {
                needs.merge_node_needs(
                    alias,
                    NodeSelectedFieldNeeds {
                        props: PropertySelection::Keys(vec![key.clone()]),
                        ..NodeSelectedFieldNeeds::default()
                    },
                    need_class,
                )?;
            }
            Self::EdgeProperty { alias, key, .. } => {
                needs.merge_edge_needs(
                    alias,
                    EdgeSelectedFieldNeeds {
                        props: PropertySelection::Keys(vec![key.clone()]),
                        ..EdgeSelectedFieldNeeds::default()
                    },
                    need_class,
                )?;
            }
            Self::NodeMetadata { alias, field, .. } => match field {
                NodeProjectionField::Id => {}
                NodeProjectionField::Key => {
                    needs.merge_node_needs(
                        alias,
                        NodeSelectedFieldNeeds {
                            key: true,
                            ..NodeSelectedFieldNeeds::default()
                        },
                        need_class,
                    )?;
                }
                NodeProjectionField::CreatedAt => {
                    needs.merge_node_needs(
                        alias,
                        NodeSelectedFieldNeeds {
                            created_at: true,
                            ..NodeSelectedFieldNeeds::default()
                        },
                        need_class,
                    )?;
                }
                NodeProjectionField::Labels
                | NodeProjectionField::Weight
                | NodeProjectionField::UpdatedAt => {
                    needs.merge_node_needs(alias, NodeSelectedFieldNeeds::default(), need_class)?;
                }
            },
            Self::EdgeMetadata { alias, field, .. } => match field {
                EdgeProjectionField::Id => {}
                EdgeProjectionField::CreatedAt => {
                    needs.merge_edge_needs(
                        alias,
                        EdgeSelectedFieldNeeds {
                            created_at: true,
                            ..EdgeSelectedFieldNeeds::default()
                        },
                        need_class,
                    )?;
                }
                EdgeProjectionField::From
                | EdgeProjectionField::To
                | EdgeProjectionField::Label
                | EdgeProjectionField::Weight
                | EdgeProjectionField::UpdatedAt
                | EdgeProjectionField::ValidFrom
                | EdgeProjectionField::ValidTo => {
                    needs.merge_edge_needs(alias, EdgeSelectedFieldNeeds::default(), need_class)?;
                }
            },
        }
        Ok(())
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) enum NodeProjectionField {
    Id,
    Labels,
    Key,
    Weight,
    CreatedAt,
    UpdatedAt,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) enum EdgeProjectionField {
    Id,
    From,
    To,
    Label,
    Weight,
    CreatedAt,
    UpdatedAt,
    ValidFrom,
    ValidTo,
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) enum ProjectedValue {
    Null,
    Bool(bool),
    Int(i64),
    UInt(u64),
    Float(f64),
    String(String),
    Bytes(Vec<u8>),
    List(Vec<ProjectedValue>),
    Map(BTreeMap<String, ProjectedValue>),
    Node(ProjectedNode),
    Edge(ProjectedEdge),
    Path(ProjectedPath),
}

impl ProjectedValue {
    pub(crate) fn from_optional_prop(value: Option<&PropValue>) -> Self {
        value.map(Self::from).unwrap_or(Self::Null)
    }

    pub(crate) fn is_null(&self) -> bool {
        matches!(self, Self::Null)
    }
}

impl From<PropValue> for ProjectedValue {
    fn from(value: PropValue) -> Self {
        match value {
            PropValue::Null => Self::Null,
            PropValue::Bool(value) => Self::Bool(value),
            PropValue::Int(value) => Self::Int(value),
            PropValue::UInt(value) => Self::UInt(value),
            PropValue::Float(value) => Self::Float(value),
            PropValue::String(value) => Self::String(value),
            PropValue::Bytes(value) => Self::Bytes(value),
            PropValue::Array(values) => {
                Self::List(values.into_iter().map(ProjectedValue::from).collect())
            }
            PropValue::Map(values) => Self::Map(
                values
                    .into_iter()
                    .map(|(key, value)| (key, ProjectedValue::from(value)))
                    .collect(),
            ),
        }
    }
}

impl From<&PropValue> for ProjectedValue {
    fn from(value: &PropValue) -> Self {
        Self::from(value.clone())
    }
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct ProjectedNode {
    pub(crate) id: Option<u64>,
    pub(crate) labels: Option<Vec<String>>,
    pub(crate) key: Option<String>,
    pub(crate) props: Option<BTreeMap<String, ProjectedValue>>,
    pub(crate) weight: Option<f32>,
    pub(crate) created_at: Option<i64>,
    pub(crate) updated_at: Option<i64>,
    pub(crate) dense_vector: Option<Vec<f32>>,
    pub(crate) sparse_vector: Option<Vec<(u32, f32)>>,
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct ProjectedEdge {
    pub(crate) id: Option<u64>,
    pub(crate) from: Option<u64>,
    pub(crate) to: Option<u64>,
    pub(crate) label: Option<String>,
    pub(crate) props: Option<BTreeMap<String, ProjectedValue>>,
    pub(crate) weight: Option<f32>,
    pub(crate) created_at: Option<i64>,
    pub(crate) updated_at: Option<i64>,
    pub(crate) valid_from: Option<i64>,
    pub(crate) valid_to: Option<i64>,
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct ProjectedPath {
    pub(crate) nodes: Vec<ProjectedNode>,
    pub(crate) edges: Vec<ProjectedEdge>,
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct ProjectedRow {
    pub(crate) values: Vec<ProjectedValue>,
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct ProjectedRows {
    pub(crate) columns: Vec<String>,
    pub(crate) rows: Vec<ProjectedRow>,
    pub(crate) truncated: bool,
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct RowProjectionPlan {
    pub(crate) columns: Vec<ProjectionColumn>,
    pub(crate) needs: ProjectionNeeds,
}

impl RowProjectionPlan {
    pub(crate) fn from_columns(columns: Vec<ProjectionColumn>) -> Result<Self, EngineError> {
        Self::from_columns_for_need_class(columns, ProjectionNeedClass::Output)
    }

    pub(crate) fn from_columns_for_need_class(
        columns: Vec<ProjectionColumn>,
        need_class: ProjectionNeedClass,
    ) -> Result<Self, EngineError> {
        Self::with_needs_for_need_class(columns, ProjectionNeeds::default(), need_class)
    }

    pub(crate) fn with_needs(
        columns: Vec<ProjectionColumn>,
        needs: ProjectionNeeds,
    ) -> Result<Self, EngineError> {
        Self::with_needs_for_need_class(columns, needs, ProjectionNeedClass::Output)
    }

    pub(crate) fn with_needs_for_need_class(
        columns: Vec<ProjectionColumn>,
        needs: ProjectionNeeds,
        need_class: ProjectionNeedClass,
    ) -> Result<Self, EngineError> {
        let columns = columns
            .into_iter()
            .map(ProjectionColumn::canonicalized)
            .collect::<Result<Vec<_>, _>>()?;
        let mut needs = needs.canonicalized()?;
        let mut column_needs = EntityProjectionNeeds::default();
        for column in &columns {
            column.accumulate_needs(&mut column_needs, need_class)?;
        }
        needs.merge_class_needs(need_class, &column_needs)?;
        Ok(Self { columns, needs })
    }

    pub(crate) fn with_explicit_needs(
        columns: Vec<ProjectionColumn>,
        needs: ProjectionNeeds,
    ) -> Result<Self, EngineError> {
        let columns = columns
            .into_iter()
            .map(ProjectionColumn::canonicalized)
            .collect::<Result<Vec<_>, _>>()?;
        let needs = needs.canonicalized()?;
        Ok(Self { columns, needs })
    }

    pub(crate) fn column_names(&self) -> Vec<String> {
        self.columns
            .iter()
            .map(|column| column.output_name().to_string())
            .collect()
    }
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub(crate) struct ProjectionNeeds {
    pub(crate) verifier: EntityProjectionNeeds,
    pub(crate) residual: EntityProjectionNeeds,
    pub(crate) order: EntityProjectionNeeds,
    pub(crate) output: EntityProjectionNeeds,
}

impl ProjectionNeeds {
    pub(crate) fn new(
        verifier: EntityProjectionNeeds,
        residual: EntityProjectionNeeds,
        order: EntityProjectionNeeds,
        output: EntityProjectionNeeds,
    ) -> Result<Self, EngineError> {
        Self {
            verifier,
            residual,
            order,
            output,
        }
        .canonicalized()
    }

    pub(crate) fn canonicalized(mut self) -> Result<Self, EngineError> {
        self.verifier = self
            .verifier
            .canonicalized_for(ProjectionNeedClass::Verifier)?;
        self.residual = self
            .residual
            .canonicalized_for(ProjectionNeedClass::Residual)?;
        self.order = self.order.canonicalized_for(ProjectionNeedClass::Order)?;
        self.output = self.output.canonicalized_for(ProjectionNeedClass::Output)?;
        Ok(self)
    }

    pub(crate) fn merged_read_needs(&self) -> Result<EntityProjectionNeeds, EngineError> {
        let mut merged = EntityProjectionNeeds::default();
        merged.merge_from(&self.verifier, ProjectionNeedClass::Verifier)?;
        merged.merge_from(&self.residual, ProjectionNeedClass::Residual)?;
        merged.merge_from(&self.order, ProjectionNeedClass::Order)?;
        merged.merge_from(&self.output, ProjectionNeedClass::Output)?;
        Ok(merged)
    }

    pub(crate) fn merge_class_needs(
        &mut self,
        need_class: ProjectionNeedClass,
        needs: &EntityProjectionNeeds,
    ) -> Result<(), EngineError> {
        match need_class {
            ProjectionNeedClass::Verifier => self.verifier.merge_from(needs, need_class),
            ProjectionNeedClass::Residual => self.residual.merge_from(needs, need_class),
            ProjectionNeedClass::Order => self.order.merge_from(needs, need_class),
            ProjectionNeedClass::Output => self.output.merge_from(needs, need_class),
        }
    }
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub(crate) struct EntityProjectionNeeds {
    pub(crate) nodes: BTreeMap<String, NodeSelectedFieldNeeds>,
    pub(crate) edges: BTreeMap<String, EdgeSelectedFieldNeeds>,
    pub(crate) paths: BTreeMap<String, PathSelectedFieldNeeds>,
    pub(crate) hidden_edges: BTreeMap<usize, EdgeSelectedFieldNeeds>,
    pub(crate) hidden_paths: BTreeMap<usize, PathSelectedFieldNeeds>,
}

impl EntityProjectionNeeds {
    pub(crate) fn canonicalized_for(
        mut self,
        need_class: ProjectionNeedClass,
    ) -> Result<Self, EngineError> {
        for needs in self.nodes.values_mut() {
            needs.canonicalize_for(need_class)?;
        }
        for needs in self.edges.values_mut() {
            needs.canonicalize_for(need_class)?;
        }
        for needs in self.paths.values_mut() {
            needs.canonicalize_for(need_class)?;
        }
        for needs in self.hidden_edges.values_mut() {
            needs.canonicalize_for(need_class)?;
        }
        for needs in self.hidden_paths.values_mut() {
            needs.canonicalize_for(need_class)?;
        }
        Ok(self)
    }

    pub(crate) fn merge_from(
        &mut self,
        other: &Self,
        need_class: ProjectionNeedClass,
    ) -> Result<(), EngineError> {
        let mut nodes = self.nodes.clone();
        let mut edges = self.edges.clone();
        let mut paths = self.paths.clone();
        let mut hidden_edges = self.hidden_edges.clone();
        let mut hidden_paths = self.hidden_paths.clone();
        for (alias, needs) in &other.nodes {
            merge_node_needs_into(&mut nodes, alias, needs.clone(), need_class)?;
        }
        for (alias, needs) in &other.edges {
            merge_edge_needs_into(&mut edges, alias, needs.clone(), need_class)?;
        }
        for (alias, needs) in &other.paths {
            merge_path_needs_into(&mut paths, alias, needs.clone(), need_class)?;
        }
        for (slot_index, needs) in &other.hidden_edges {
            merge_edge_slot_needs_into(&mut hidden_edges, *slot_index, needs.clone(), need_class)?;
        }
        for (slot_index, needs) in &other.hidden_paths {
            merge_path_slot_needs_into(&mut hidden_paths, *slot_index, needs.clone(), need_class)?;
        }
        self.nodes = nodes;
        self.edges = edges;
        self.paths = paths;
        self.hidden_edges = hidden_edges;
        self.hidden_paths = hidden_paths;
        Ok(())
    }

    pub(crate) fn merge_node_needs(
        &mut self,
        alias: &str,
        needs: NodeSelectedFieldNeeds,
        need_class: ProjectionNeedClass,
    ) -> Result<(), EngineError> {
        let mut nodes = self.nodes.clone();
        merge_node_needs_into(&mut nodes, alias, needs, need_class)?;
        self.nodes = nodes;
        Ok(())
    }

    pub(crate) fn merge_edge_needs(
        &mut self,
        alias: &str,
        needs: EdgeSelectedFieldNeeds,
        need_class: ProjectionNeedClass,
    ) -> Result<(), EngineError> {
        let mut edges = self.edges.clone();
        merge_edge_needs_into(&mut edges, alias, needs, need_class)?;
        self.edges = edges;
        Ok(())
    }

    pub(crate) fn merge_path_needs(
        &mut self,
        alias: &str,
        needs: PathSelectedFieldNeeds,
        need_class: ProjectionNeedClass,
    ) -> Result<(), EngineError> {
        let mut paths = self.paths.clone();
        merge_path_needs_into(&mut paths, alias, needs, need_class)?;
        self.paths = paths;
        Ok(())
    }

    pub(crate) fn merge_hidden_edge_needs(
        &mut self,
        slot_index: usize,
        needs: EdgeSelectedFieldNeeds,
        need_class: ProjectionNeedClass,
    ) -> Result<(), EngineError> {
        let mut hidden_edges = self.hidden_edges.clone();
        merge_edge_slot_needs_into(&mut hidden_edges, slot_index, needs, need_class)?;
        self.hidden_edges = hidden_edges;
        Ok(())
    }

    pub(crate) fn merge_hidden_path_needs(
        &mut self,
        slot_index: usize,
        needs: PathSelectedFieldNeeds,
        need_class: ProjectionNeedClass,
    ) -> Result<(), EngineError> {
        let mut hidden_paths = self.hidden_paths.clone();
        merge_path_slot_needs_into(&mut hidden_paths, slot_index, needs, need_class)?;
        self.hidden_paths = hidden_paths;
        Ok(())
    }
}

fn merge_node_needs_into(
    nodes: &mut BTreeMap<String, NodeSelectedFieldNeeds>,
    alias: &str,
    needs: NodeSelectedFieldNeeds,
    need_class: ProjectionNeedClass,
) -> Result<(), EngineError> {
    let mut merged = nodes.get(alias).cloned().unwrap_or_default();
    merged.merge_from(&needs, need_class)?;
    nodes.insert(alias.to_string(), merged);
    Ok(())
}

fn merge_edge_needs_into(
    edges: &mut BTreeMap<String, EdgeSelectedFieldNeeds>,
    alias: &str,
    needs: EdgeSelectedFieldNeeds,
    need_class: ProjectionNeedClass,
) -> Result<(), EngineError> {
    let mut merged = edges.get(alias).cloned().unwrap_or_default();
    merged.merge_from(&needs, need_class)?;
    edges.insert(alias.to_string(), merged);
    Ok(())
}

fn merge_path_needs_into(
    paths: &mut BTreeMap<String, PathSelectedFieldNeeds>,
    alias: &str,
    needs: PathSelectedFieldNeeds,
    need_class: ProjectionNeedClass,
) -> Result<(), EngineError> {
    let mut merged = paths.get(alias).cloned().unwrap_or_default();
    merged.merge_from(&needs, need_class)?;
    paths.insert(alias.to_string(), merged);
    Ok(())
}

fn merge_edge_slot_needs_into(
    edges: &mut BTreeMap<usize, EdgeSelectedFieldNeeds>,
    slot_index: usize,
    needs: EdgeSelectedFieldNeeds,
    need_class: ProjectionNeedClass,
) -> Result<(), EngineError> {
    let mut merged = edges.get(&slot_index).cloned().unwrap_or_default();
    merged.merge_from(&needs, need_class)?;
    edges.insert(slot_index, merged);
    Ok(())
}

fn merge_path_slot_needs_into(
    paths: &mut BTreeMap<usize, PathSelectedFieldNeeds>,
    slot_index: usize,
    needs: PathSelectedFieldNeeds,
    need_class: ProjectionNeedClass,
) -> Result<(), EngineError> {
    let mut merged = paths.get(&slot_index).cloned().unwrap_or_default();
    merged.merge_from(&needs, need_class)?;
    paths.insert(slot_index, merged);
    Ok(())
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub(crate) struct NodeSelectedFieldNeeds {
    pub(crate) key: bool,
    pub(crate) created_at: bool,
    pub(crate) props: PropertySelection,
    pub(crate) vectors: VectorSelection,
}

impl NodeSelectedFieldNeeds {
    fn canonicalize_for(&mut self, need_class: ProjectionNeedClass) -> Result<(), EngineError> {
        self.props = self.props.clone().canonicalized_for(need_class)?;
        Ok(())
    }

    fn merge_from(
        &mut self,
        other: &Self,
        need_class: ProjectionNeedClass,
    ) -> Result<(), EngineError> {
        let mut props = self.props.clone();
        props.merge_from(&other.props, need_class)?;
        self.key |= other.key;
        self.created_at |= other.created_at;
        self.vectors = self.vectors.union(other.vectors);
        self.props = props;
        Ok(())
    }
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub(crate) struct EdgeSelectedFieldNeeds {
    pub(crate) created_at: bool,
    pub(crate) props: PropertySelection,
}

impl EdgeSelectedFieldNeeds {
    fn canonicalize_for(&mut self, need_class: ProjectionNeedClass) -> Result<(), EngineError> {
        self.props = self.props.clone().canonicalized_for(need_class)?;
        Ok(())
    }

    fn merge_from(
        &mut self,
        other: &Self,
        need_class: ProjectionNeedClass,
    ) -> Result<(), EngineError> {
        let mut props = self.props.clone();
        props.merge_from(&other.props, need_class)?;
        self.created_at |= other.created_at;
        self.props = props;
        Ok(())
    }
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub(crate) struct PathSelectedFieldNeeds {
    pub(crate) node_ids: bool,
    pub(crate) edge_ids: bool,
    pub(crate) start_node: Option<NodeSelectedFieldNeeds>,
    pub(crate) end_node: Option<NodeSelectedFieldNeeds>,
    pub(crate) nodes: Option<NodeSelectedFieldNeeds>,
    pub(crate) edges: Option<EdgeSelectedFieldNeeds>,
}

impl PathSelectedFieldNeeds {
    fn canonicalize_for(&mut self, need_class: ProjectionNeedClass) -> Result<(), EngineError> {
        if let Some(nodes) = self.nodes.as_mut() {
            nodes.canonicalize_for(need_class)?;
        }
        if let Some(edges) = self.edges.as_mut() {
            edges.canonicalize_for(need_class)?;
        }
        if let Some(start_node) = self.start_node.as_mut() {
            start_node.canonicalize_for(need_class)?;
        }
        if let Some(end_node) = self.end_node.as_mut() {
            end_node.canonicalize_for(need_class)?;
        }
        Ok(())
    }

    fn merge_from(
        &mut self,
        other: &Self,
        need_class: ProjectionNeedClass,
    ) -> Result<(), EngineError> {
        self.node_ids |= other.node_ids;
        self.edge_ids |= other.edge_ids;
        if let Some(other_start_node) = other.start_node.as_ref() {
            let mut start_node = self.start_node.clone().unwrap_or_default();
            start_node.merge_from(other_start_node, need_class)?;
            self.start_node = Some(start_node);
        }
        if let Some(other_end_node) = other.end_node.as_ref() {
            let mut end_node = self.end_node.clone().unwrap_or_default();
            end_node.merge_from(other_end_node, need_class)?;
            self.end_node = Some(end_node);
        }
        if let Some(other_nodes) = other.nodes.as_ref() {
            let mut nodes = self.nodes.clone().unwrap_or_default();
            nodes.merge_from(other_nodes, need_class)?;
            self.nodes = Some(nodes);
        }
        if let Some(other_edges) = other.edges.as_ref() {
            let mut edges = self.edges.clone().unwrap_or_default();
            edges.merge_from(other_edges, need_class)?;
            self.edges = Some(edges);
        }
        Ok(())
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum ProjectionNeedClass {
    Verifier,
    Residual,
    Order,
    Output,
}

impl ProjectionNeedClass {
    fn allows_all_properties(self) -> bool {
        matches!(self, Self::Output)
    }

    fn name(self) -> &'static str {
        match self {
            Self::Verifier => "verifier",
            Self::Residual => "residual",
            Self::Order => "order",
            Self::Output => "output",
        }
    }
}

fn canonicalize_property_keys(keys: Vec<String>) -> Result<Vec<String>, EngineError> {
    let mut canonical = Vec::with_capacity(keys.len());
    append_deduped_property_keys(&mut canonical, keys)?;
    Ok(canonical)
}

fn append_deduped_property_keys(
    canonical: &mut Vec<String>,
    keys: Vec<String>,
) -> Result<(), EngineError> {
    let mut seen: BTreeSet<String> = canonical.iter().cloned().collect();
    for key in keys {
        validate_property_key(&key)?;
        if seen.insert(key.clone()) {
            canonical.push(key);
        }
    }
    Ok(())
}

fn validate_property_key(key: &str) -> Result<(), EngineError> {
    if key.is_empty() {
        return Err(EngineError::InvalidOperation(
            "projection property key must not be empty".to_string(),
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn node_property_column(alias: &str, key: &str, output_name: &str) -> ProjectionColumn {
        ProjectionColumn::NodeProperty {
            alias: alias.to_string(),
            key: key.to_string(),
            output_name: output_name.to_string(),
        }
    }

    fn edge_property_column(alias: &str, key: &str, output_name: &str) -> ProjectionColumn {
        ProjectionColumn::EdgeProperty {
            alias: alias.to_string(),
            key: key.to_string(),
            output_name: output_name.to_string(),
        }
    }

    fn output_node_props(plan: &RowProjectionPlan, alias: &str) -> PropertySelection {
        plan.needs
            .output
            .nodes
            .get(alias)
            .map(|needs| needs.props.clone())
            .unwrap_or_default()
    }

    #[test]
    fn prop_value_to_projected_value_conversion_covers_all_variants() {
        let mut inner = BTreeMap::new();
        inner.insert("nested".to_string(), PropValue::Bool(false));

        let cases = vec![
            (PropValue::Null, ProjectedValue::Null),
            (PropValue::Bool(true), ProjectedValue::Bool(true)),
            (PropValue::Int(-7), ProjectedValue::Int(-7)),
            (PropValue::UInt(7), ProjectedValue::UInt(7)),
            (PropValue::Float(1.5), ProjectedValue::Float(1.5)),
            (
                PropValue::String("alice".to_string()),
                ProjectedValue::String("alice".to_string()),
            ),
            (
                PropValue::Bytes(vec![1, 2, 3]),
                ProjectedValue::Bytes(vec![1, 2, 3]),
            ),
            (
                PropValue::Array(vec![PropValue::Int(1), PropValue::Null]),
                ProjectedValue::List(vec![ProjectedValue::Int(1), ProjectedValue::Null]),
            ),
            (
                PropValue::Map(inner.clone()),
                ProjectedValue::Map(BTreeMap::from([(
                    "nested".to_string(),
                    ProjectedValue::Bool(false),
                )])),
            ),
        ];

        for (prop, projected) in cases {
            assert_eq!(ProjectedValue::from(prop.clone()), projected);
            assert_eq!(ProjectedValue::from(&prop), projected);
        }
    }

    #[test]
    fn missing_and_null_property_helpers_project_to_null() {
        let mut props = BTreeMap::new();
        props.insert("present_null".to_string(), PropValue::Null);
        props.insert("present_bool".to_string(), PropValue::Bool(true));

        assert!(ProjectedValue::from_optional_prop(props.get("missing")).is_null());
        assert!(ProjectedValue::from_optional_prop(props.get("present_null")).is_null());
        assert_eq!(
            ProjectedValue::from_optional_prop(props.get("present_bool")),
            ProjectedValue::Bool(true)
        );
    }

    #[test]
    fn node_projection_constructor_shapes_are_locked() {
        assert_eq!(
            NodeOutputProjection::id_only(),
            NodeOutputProjection {
                id: true,
                labels: false,
                key: false,
                props: PropertySelection::None,
                weight: false,
                created_at: false,
                updated_at: false,
                vectors: VectorSelection::None,
            }
        );
        assert_eq!(
            NodeOutputProjection::compact_element(),
            NodeOutputProjection {
                id: true,
                labels: true,
                key: true,
                props: PropertySelection::None,
                weight: false,
                created_at: false,
                updated_at: false,
                vectors: VectorSelection::None,
            }
        );
        assert_eq!(
            NodeOutputProjection::full_without_vectors(),
            NodeOutputProjection {
                id: true,
                labels: true,
                key: true,
                props: PropertySelection::All,
                weight: true,
                created_at: true,
                updated_at: true,
                vectors: VectorSelection::None,
            }
        );
        assert_eq!(
            NodeOutputProjection::full_with_vectors(),
            NodeOutputProjection {
                vectors: VectorSelection::Both,
                ..NodeOutputProjection::full_without_vectors()
            }
        );
    }

    #[test]
    fn edge_projection_constructor_shapes_are_locked() {
        assert_eq!(
            EdgeOutputProjection::id_only(),
            EdgeOutputProjection {
                id: true,
                from: false,
                to: false,
                label: false,
                props: PropertySelection::None,
                weight: false,
                created_at: false,
                updated_at: false,
                valid_from: false,
                valid_to: false,
            }
        );
        assert_eq!(
            EdgeOutputProjection::compact_element(),
            EdgeOutputProjection {
                id: true,
                from: true,
                to: true,
                label: true,
                props: PropertySelection::None,
                weight: false,
                created_at: false,
                updated_at: false,
                valid_from: false,
                valid_to: false,
            }
        );
        assert_eq!(
            EdgeOutputProjection::full(),
            EdgeOutputProjection {
                id: true,
                from: true,
                to: true,
                label: true,
                props: PropertySelection::All,
                weight: true,
                created_at: true,
                updated_at: true,
                valid_from: true,
                valid_to: true,
            }
        );
    }

    #[test]
    fn row_projection_plan_rejects_empty_property_keys() {
        assert!(
            RowProjectionPlan::from_columns(vec![node_property_column("n", "", "bad")]).is_err()
        );
        assert!(
            RowProjectionPlan::from_columns(vec![edge_property_column("r", "", "bad")]).is_err()
        );
        assert!(
            RowProjectionPlan::from_columns(vec![ProjectionColumn::NodeAlias {
                alias: "n".to_string(),
                projection: NodeOutputProjection {
                    props: PropertySelection::Keys(vec!["ok".to_string(), String::new()]),
                    ..NodeOutputProjection::compact_element()
                },
                output_name: "n".to_string(),
            }])
            .is_err()
        );

        let mut needs = ProjectionNeeds::default();
        needs.verifier.nodes.insert(
            "n".to_string(),
            NodeSelectedFieldNeeds {
                props: PropertySelection::Keys(vec![String::new()]),
                ..NodeSelectedFieldNeeds::default()
            },
        );
        assert!(RowProjectionPlan::with_needs(Vec::new(), needs).is_err());
    }

    #[test]
    fn duplicate_output_names_are_preserved_for_positional_rows() {
        let plan = RowProjectionPlan::from_columns(vec![
            ProjectionColumn::NodeMetadata {
                alias: "n".to_string(),
                field: NodeProjectionField::Id,
                output_name: "id".to_string(),
            },
            ProjectionColumn::EdgeMetadata {
                alias: "r".to_string(),
                field: EdgeProjectionField::Id,
                output_name: "id".to_string(),
            },
        ])
        .unwrap();

        let rows = ProjectedRows {
            columns: plan.column_names(),
            rows: vec![ProjectedRow {
                values: vec![ProjectedValue::UInt(10), ProjectedValue::UInt(20)],
            }],
            truncated: false,
        };

        assert_eq!(rows.columns, vec!["id".to_string(), "id".to_string()]);
        assert_eq!(
            rows.rows[0].values,
            vec![ProjectedValue::UInt(10), ProjectedValue::UInt(20)]
        );
    }

    #[test]
    fn property_key_canonicalization_preserves_output_order_and_dedupes_lookup_needs() {
        let plan = RowProjectionPlan::from_columns(vec![
            node_property_column("n", "b", "first"),
            node_property_column("n", "a", "second"),
            node_property_column("n", "b", "third"),
        ])
        .unwrap();

        assert_eq!(
            plan.column_names(),
            vec![
                "first".to_string(),
                "second".to_string(),
                "third".to_string()
            ]
        );
        assert_eq!(
            output_node_props(&plan, "n"),
            PropertySelection::Keys(vec!["b".to_string(), "a".to_string()])
        );
    }

    #[test]
    fn row_projection_plan_can_classify_columns_outside_output_needs() {
        let residual_plan = RowProjectionPlan::from_columns_for_need_class(
            vec![node_property_column("n", "status", "status")],
            ProjectionNeedClass::Residual,
        )
        .unwrap();

        assert_eq!(
            residual_plan.needs.residual.nodes["n"].props,
            PropertySelection::Keys(vec!["status".to_string()])
        );
        assert!(residual_plan.needs.output.nodes.is_empty());

        let order_plan = RowProjectionPlan::from_columns_for_need_class(
            vec![edge_property_column("r", "rank", "rank")],
            ProjectionNeedClass::Order,
        )
        .unwrap();

        assert_eq!(
            order_plan.needs.order.edges["r"].props,
            PropertySelection::Keys(vec!["rank".to_string()])
        );
        assert!(order_plan.needs.output.edges.is_empty());
    }

    #[test]
    fn property_selection_all_is_rejected_outside_final_output_needs() {
        for need_class in [
            ProjectionNeedClass::Verifier,
            ProjectionNeedClass::Residual,
            ProjectionNeedClass::Order,
        ] {
            let mut entity_needs = EntityProjectionNeeds::default();
            entity_needs.nodes.insert(
                "n".to_string(),
                NodeSelectedFieldNeeds {
                    props: PropertySelection::All,
                    ..NodeSelectedFieldNeeds::default()
                },
            );
            let result = entity_needs.canonicalized_for(need_class);
            assert!(
                result.is_err(),
                "PropertySelection::All must be rejected for {need_class:?}"
            );
        }

        let plan = RowProjectionPlan::from_columns(vec![
            ProjectionColumn::NodeAlias {
                alias: "n".to_string(),
                projection: NodeOutputProjection::full_without_vectors(),
                output_name: "n".to_string(),
            },
            ProjectionColumn::EdgeAlias {
                alias: "r".to_string(),
                projection: EdgeOutputProjection::full(),
                output_name: "r".to_string(),
            },
        ])
        .unwrap();

        assert_eq!(
            plan.needs.output.nodes.get("n").unwrap().props,
            PropertySelection::All
        );
        assert_eq!(
            plan.needs.output.edges.get("r").unwrap().props,
            PropertySelection::All
        );

        assert!(RowProjectionPlan::from_columns_for_need_class(
            vec![ProjectionColumn::NodeAlias {
                alias: "n".to_string(),
                projection: NodeOutputProjection::full_without_vectors(),
                output_name: "n".to_string(),
            }],
            ProjectionNeedClass::Residual,
        )
        .is_err());
    }

    #[test]
    fn need_classes_stay_separate_and_merge_deterministically_for_reads() {
        let mut needs = ProjectionNeeds::default();
        needs.verifier.nodes.insert(
            "n".to_string(),
            NodeSelectedFieldNeeds {
                props: PropertySelection::Keys(vec!["a".to_string()]),
                ..NodeSelectedFieldNeeds::default()
            },
        );
        needs.residual.nodes.insert(
            "n".to_string(),
            NodeSelectedFieldNeeds {
                props: PropertySelection::Keys(vec!["b".to_string()]),
                ..NodeSelectedFieldNeeds::default()
            },
        );
        needs.order.nodes.insert(
            "n".to_string(),
            NodeSelectedFieldNeeds {
                props: PropertySelection::Keys(vec!["a".to_string()]),
                ..NodeSelectedFieldNeeds::default()
            },
        );

        let plan = RowProjectionPlan::with_needs(vec![node_property_column("n", "c", "c")], needs)
            .unwrap();

        assert_eq!(
            plan.needs.verifier.nodes["n"].props,
            PropertySelection::Keys(vec!["a".to_string()])
        );
        assert_eq!(
            plan.needs.residual.nodes["n"].props,
            PropertySelection::Keys(vec!["b".to_string()])
        );
        assert_eq!(
            plan.needs.order.nodes["n"].props,
            PropertySelection::Keys(vec!["a".to_string()])
        );
        assert_eq!(
            plan.needs.output.nodes["n"].props,
            PropertySelection::Keys(vec!["c".to_string()])
        );

        let merged = plan.needs.merged_read_needs().unwrap();
        assert_eq!(
            merged.nodes["n"].props,
            PropertySelection::Keys(vec!["a".to_string(), "b".to_string(), "c".to_string()])
        );
    }

    #[test]
    fn failed_need_merges_do_not_mutate_existing_or_insert_aliases() {
        let mut needs = EntityProjectionNeeds::default();
        needs.nodes.insert(
            "existing_node".to_string(),
            NodeSelectedFieldNeeds {
                props: PropertySelection::Keys(vec!["a".to_string()]),
                ..NodeSelectedFieldNeeds::default()
            },
        );
        needs.edges.insert(
            "existing_edge".to_string(),
            EdgeSelectedFieldNeeds {
                props: PropertySelection::Keys(vec!["x".to_string()]),
                ..EdgeSelectedFieldNeeds::default()
            },
        );
        let original = needs.clone();

        assert!(needs
            .merge_node_needs(
                "new_node",
                NodeSelectedFieldNeeds {
                    props: PropertySelection::Keys(vec![String::new()]),
                    ..NodeSelectedFieldNeeds::default()
                },
                ProjectionNeedClass::Output,
            )
            .is_err());
        assert_eq!(needs, original);
        assert!(!needs.nodes.contains_key("new_node"));

        assert!(needs
            .merge_edge_needs(
                "new_edge",
                EdgeSelectedFieldNeeds {
                    props: PropertySelection::Keys(vec![String::new()]),
                    ..EdgeSelectedFieldNeeds::default()
                },
                ProjectionNeedClass::Output,
            )
            .is_err());
        assert_eq!(needs, original);
        assert!(!needs.edges.contains_key("new_edge"));

        let mut other = EntityProjectionNeeds::default();
        other.nodes.insert(
            "valid_node".to_string(),
            NodeSelectedFieldNeeds {
                props: PropertySelection::Keys(vec!["b".to_string()]),
                ..NodeSelectedFieldNeeds::default()
            },
        );
        other.edges.insert(
            "bad_edge".to_string(),
            EdgeSelectedFieldNeeds {
                props: PropertySelection::Keys(vec![String::new()]),
                ..EdgeSelectedFieldNeeds::default()
            },
        );
        assert!(needs
            .merge_from(&other, ProjectionNeedClass::Output)
            .is_err());
        assert_eq!(needs, original);
        assert!(!needs.nodes.contains_key("valid_node"));
        assert!(!needs.edges.contains_key("bad_edge"));
    }
}
