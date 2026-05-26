#![allow(dead_code)]

use crate::error::EngineError;
use crate::property_value_semantics::{
    compare_numeric_keys, numeric_key_from_f64, numeric_key_from_i64, numeric_key_from_u64,
    numeric_range_sort_key, NumericRangeSortKey, NumericScalarKey,
};
use crate::row_projection::{
    EdgeSelectedFieldNeeds, EntityProjectionNeeds, NodeSelectedFieldNeeds, PathSelectedFieldNeeds,
    ProjectionNeedClass, ProjectionNeeds, PropertySelection, VectorSelection,
};
use crate::types::{
    EdgeFilterExpr, GraphBinaryOp, GraphEdgeField, GraphEdgeValue, GraphElementProjection,
    GraphExpr, GraphFunction, GraphNodeField, GraphNodeValue, GraphOrderDirection, GraphOrderItem,
    GraphOutputMode, GraphOutputOptions, GraphParamValue, GraphPath, GraphPathField,
    GraphPathValue, GraphPatternPiece, GraphPropertySelection, GraphReturnItem,
    GraphReturnProjection, GraphSelectedEdgeProjection, GraphSelectedNodeProjection,
    GraphSelectedPathProjection, GraphSelectedProjection, GraphUnaryOp, GraphValue,
    GraphVectorSelection, NodeFilterExpr,
};
use std::cmp::Ordering;
use std::collections::{BTreeMap, HashMap};

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) enum GraphBindingSlotKind {
    Node,
    Edge,
    Path,
    Scalar,
    HiddenOccurrence,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) struct GraphBindingSlotRef {
    pub(crate) kind: GraphBindingSlotKind,
    pub(crate) index: usize,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct GraphBindingSlot {
    pub(crate) name: String,
    pub(crate) user_alias: Option<String>,
    pub(crate) kind: GraphBindingSlotKind,
    pub(crate) index: usize,
    pub(crate) nullable: bool,
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub(crate) struct GraphBindingSchema {
    slots: Vec<GraphBindingSlot>,
    alias_to_slot: HashMap<String, GraphBindingSlotRef>,
    node_slot_positions: Vec<usize>,
    edge_slot_positions: Vec<usize>,
    path_slot_positions: Vec<usize>,
    scalar_slot_positions: Vec<usize>,
    hidden_slot_positions: Vec<usize>,
    node_slots: usize,
    edge_slots: usize,
    path_slots: usize,
    scalar_slots: usize,
    hidden_slots: usize,
}

impl GraphBindingSchema {
    pub(crate) fn new() -> Self {
        Self::default()
    }

    pub(crate) fn add_node_alias(
        &mut self,
        alias: impl Into<String>,
        nullable: bool,
    ) -> Result<GraphBindingSlotRef, EngineError> {
        self.add_aliased_slot(alias.into(), GraphBindingSlotKind::Node, nullable)
    }

    pub(crate) fn add_edge_alias(
        &mut self,
        alias: impl Into<String>,
        nullable: bool,
    ) -> Result<GraphBindingSlotRef, EngineError> {
        self.add_aliased_slot(alias.into(), GraphBindingSlotKind::Edge, nullable)
    }

    pub(crate) fn add_path_alias(
        &mut self,
        alias: impl Into<String>,
        nullable: bool,
    ) -> Result<GraphBindingSlotRef, EngineError> {
        self.add_aliased_slot(alias.into(), GraphBindingSlotKind::Path, nullable)
    }

    pub(crate) fn add_scalar_alias(
        &mut self,
        alias: impl Into<String>,
        nullable: bool,
    ) -> Result<GraphBindingSlotRef, EngineError> {
        self.add_aliased_slot(alias.into(), GraphBindingSlotKind::Scalar, nullable)
    }

    pub(crate) fn add_hidden_occurrence(
        &mut self,
        label: impl Into<String>,
    ) -> Result<GraphBindingSlotRef, EngineError> {
        self.add_hidden_occurrence_with_nullability(label, false)
    }

    pub(crate) fn add_hidden_occurrence_with_nullability(
        &mut self,
        label: impl Into<String>,
        nullable: bool,
    ) -> Result<GraphBindingSlotRef, EngineError> {
        let label = label.into();
        if label.is_empty() {
            return Err(EngineError::InvalidOperation(
                "graph row hidden occurrence slot label must be non-empty".to_string(),
            ));
        }
        let slot = GraphBindingSlotRef {
            kind: GraphBindingSlotKind::HiddenOccurrence,
            index: self.hidden_slots,
        };
        self.hidden_slots += 1;
        let slot_position = self.slots.len();
        self.hidden_slot_positions.push(slot_position);
        self.slots.push(GraphBindingSlot {
            name: label,
            user_alias: None,
            kind: slot.kind,
            index: slot.index,
            nullable,
        });
        Ok(slot)
    }

    pub(crate) fn slot_for_alias(&self, alias: &str) -> Option<GraphBindingSlotRef> {
        self.alias_to_slot.get(alias).copied()
    }

    pub(crate) fn slots(&self) -> &[GraphBindingSlot] {
        &self.slots
    }

    pub(crate) fn slot(&self, slot: GraphBindingSlotRef) -> Option<&GraphBindingSlot> {
        let position = match slot.kind {
            GraphBindingSlotKind::Node => self.node_slot_positions.get(slot.index),
            GraphBindingSlotKind::Edge => self.edge_slot_positions.get(slot.index),
            GraphBindingSlotKind::Path => self.path_slot_positions.get(slot.index),
            GraphBindingSlotKind::Scalar => self.scalar_slot_positions.get(slot.index),
            GraphBindingSlotKind::HiddenOccurrence => self.hidden_slot_positions.get(slot.index),
        }?;
        self.slots.get(*position)
    }

    pub(crate) fn empty_row(&self) -> GraphBindingRow {
        GraphBindingRow {
            nodes: vec![GraphSlotState::Unbound; self.node_slots],
            edges: vec![GraphSlotState::Unbound; self.edge_slots],
            paths: vec![GraphSlotState::Unbound; self.path_slots],
            scalars: vec![GraphSlotState::Unbound; self.scalar_slots],
            hidden: vec![None; self.hidden_slots],
        }
    }

    fn add_aliased_slot(
        &mut self,
        alias: String,
        kind: GraphBindingSlotKind,
        nullable: bool,
    ) -> Result<GraphBindingSlotRef, EngineError> {
        if alias.is_empty() {
            return Err(EngineError::InvalidOperation(
                "graph row binding alias must be non-empty".to_string(),
            ));
        }
        if self.alias_to_slot.contains_key(&alias) {
            return Err(EngineError::InvalidOperation(format!(
                "graph row binding alias '{alias}' is already assigned a slot"
            )));
        }
        let index = match kind {
            GraphBindingSlotKind::Node => next_index(&mut self.node_slots),
            GraphBindingSlotKind::Edge => next_index(&mut self.edge_slots),
            GraphBindingSlotKind::Path => next_index(&mut self.path_slots),
            GraphBindingSlotKind::Scalar => next_index(&mut self.scalar_slots),
            GraphBindingSlotKind::HiddenOccurrence => unreachable!("hidden slots are unaliased"),
        };
        let slot = GraphBindingSlotRef { kind, index };
        let slot_position = self.slots.len();
        match kind {
            GraphBindingSlotKind::Node => self.node_slot_positions.push(slot_position),
            GraphBindingSlotKind::Edge => self.edge_slot_positions.push(slot_position),
            GraphBindingSlotKind::Path => self.path_slot_positions.push(slot_position),
            GraphBindingSlotKind::Scalar => self.scalar_slot_positions.push(slot_position),
            GraphBindingSlotKind::HiddenOccurrence => unreachable!("hidden slots are unaliased"),
        }
        self.alias_to_slot.insert(alias.clone(), slot);
        self.slots.push(GraphBindingSlot {
            name: alias.clone(),
            user_alias: Some(alias),
            kind,
            index,
            nullable,
        });
        Ok(slot)
    }
}

fn next_index(value: &mut usize) -> usize {
    let index = *value;
    *value += 1;
    index
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct GraphBindingRow {
    nodes: Vec<GraphSlotState<GraphBoundNode>>,
    edges: Vec<GraphSlotState<GraphBoundEdge>>,
    paths: Vec<GraphSlotState<GraphBoundPath>>,
    scalars: Vec<GraphSlotState<GraphEvalValue>>,
    hidden: Vec<Option<GraphHiddenOccurrence>>,
}

#[derive(Clone, Debug, PartialEq)]
enum GraphSlotState<T> {
    Unbound,
    Null,
    Bound(T),
}

impl GraphBindingRow {
    pub(crate) fn bind_node(
        &mut self,
        slot: GraphBindingSlotRef,
        node: GraphBoundNode,
    ) -> Result<(), EngineError> {
        bind_node_slot(self.node_slot_mut(slot)?, node)
    }

    pub(crate) fn bind_edge(
        &mut self,
        slot: GraphBindingSlotRef,
        edge: GraphBoundEdge,
    ) -> Result<(), EngineError> {
        bind_edge_slot(self.edge_slot_mut(slot)?, edge)
    }

    pub(crate) fn bind_path(
        &mut self,
        slot: GraphBindingSlotRef,
        path: GraphBoundPath,
    ) -> Result<(), EngineError> {
        bind_path_slot(self.path_slot_mut(slot)?, path)
    }

    pub(crate) fn bind_scalar(
        &mut self,
        slot: GraphBindingSlotRef,
        value: GraphEvalValue,
    ) -> Result<(), EngineError> {
        if slot.kind != GraphBindingSlotKind::Scalar {
            return Err(slot_kind_error(slot.kind, GraphBindingSlotKind::Scalar));
        }
        let target = self.scalars.get_mut(slot.index).ok_or_else(|| {
            EngineError::InvalidOperation(format!(
                "graph row scalar slot {} is out of bounds",
                slot.index
            ))
        })?;
        bind_value_slot(target, value, "scalar")
    }

    pub(crate) fn bind_hidden(
        &mut self,
        slot: GraphBindingSlotRef,
        occurrence: GraphHiddenOccurrence,
    ) -> Result<(), EngineError> {
        if slot.kind != GraphBindingSlotKind::HiddenOccurrence {
            return Err(slot_kind_error(
                slot.kind,
                GraphBindingSlotKind::HiddenOccurrence,
            ));
        }
        let target = self.hidden.get_mut(slot.index).ok_or_else(|| {
            EngineError::InvalidOperation(format!(
                "graph row hidden slot {} is out of bounds",
                slot.index
            ))
        })?;
        bind_optional_slot(target, occurrence, "hidden occurrence")
    }

    pub(crate) fn set_null(
        &mut self,
        schema: &GraphBindingSchema,
        slot: GraphBindingSlotRef,
    ) -> Result<(), EngineError> {
        let slot_info = schema.slot(slot).ok_or_else(|| {
            EngineError::InvalidOperation(format!(
                "graph row slot {:?}:{} is not part of the binding schema",
                slot.kind, slot.index
            ))
        })?;
        if !slot_info.nullable {
            return Err(EngineError::InvalidOperation(format!(
                "graph row binding '{}' is not nullable",
                slot_info.name
            )));
        }
        match slot.kind {
            GraphBindingSlotKind::Node => {
                set_slot_null(self.node_slot_mut(slot)?, &slot_info.name)?
            }
            GraphBindingSlotKind::Edge => {
                set_slot_null(self.edge_slot_mut(slot)?, &slot_info.name)?
            }
            GraphBindingSlotKind::Path => {
                set_slot_null(self.path_slot_mut(slot)?, &slot_info.name)?
            }
            GraphBindingSlotKind::Scalar => {
                let target = self.scalars.get_mut(slot.index).ok_or_else(|| {
                    EngineError::InvalidOperation(format!(
                        "graph row scalar slot {} is out of bounds",
                        slot.index
                    ))
                })?;
                set_slot_null(target, &slot_info.name)?;
            }
            GraphBindingSlotKind::HiddenOccurrence => {
                let target = self.hidden.get_mut(slot.index).ok_or_else(|| {
                    EngineError::InvalidOperation(format!(
                        "graph row hidden slot {} is out of bounds",
                        slot.index
                    ))
                })?;
                match target {
                    None | Some(GraphHiddenOccurrence::Null) => {
                        *target = Some(GraphHiddenOccurrence::Null);
                    }
                    Some(_) => {
                        return Err(EngineError::InvalidOperation(format!(
                            "graph row binding '{}' is already bound and cannot be set to null",
                            slot_info.name
                        )));
                    }
                }
            }
        }
        Ok(())
    }

    pub(crate) fn slot_is_null(&self, slot: GraphBindingSlotRef) -> Result<bool, EngineError> {
        Ok(match slot.kind {
            GraphBindingSlotKind::Node => matches!(
                self.nodes
                    .get(slot.index)
                    .ok_or_else(|| slot_bounds_error("node", slot.index))?,
                GraphSlotState::Null
            ),
            GraphBindingSlotKind::Edge => matches!(
                self.edges
                    .get(slot.index)
                    .ok_or_else(|| slot_bounds_error("edge", slot.index))?,
                GraphSlotState::Null
            ),
            GraphBindingSlotKind::Path => matches!(
                self.paths
                    .get(slot.index)
                    .ok_or_else(|| slot_bounds_error("path", slot.index))?,
                GraphSlotState::Null
            ),
            GraphBindingSlotKind::Scalar => matches!(
                self.scalars
                    .get(slot.index)
                    .ok_or_else(|| slot_bounds_error("scalar", slot.index))?,
                GraphSlotState::Null
            ),
            GraphBindingSlotKind::HiddenOccurrence => matches!(
                self.hidden
                    .get(slot.index)
                    .ok_or_else(|| slot_bounds_error("hidden", slot.index))?,
                Some(GraphHiddenOccurrence::Null)
            ),
        })
    }

    pub(crate) fn slot_is_bound(&self, slot: GraphBindingSlotRef) -> Result<bool, EngineError> {
        Ok(match slot.kind {
            GraphBindingSlotKind::Node => matches!(
                self.nodes
                    .get(slot.index)
                    .ok_or_else(|| slot_bounds_error("node", slot.index))?,
                GraphSlotState::Bound(_)
            ),
            GraphBindingSlotKind::Edge => matches!(
                self.edges
                    .get(slot.index)
                    .ok_or_else(|| slot_bounds_error("edge", slot.index))?,
                GraphSlotState::Bound(_)
            ),
            GraphBindingSlotKind::Path => matches!(
                self.paths
                    .get(slot.index)
                    .ok_or_else(|| slot_bounds_error("path", slot.index))?,
                GraphSlotState::Bound(_)
            ),
            GraphBindingSlotKind::Scalar => matches!(
                self.scalars
                    .get(slot.index)
                    .ok_or_else(|| slot_bounds_error("scalar", slot.index))?,
                GraphSlotState::Bound(_)
            ),
            GraphBindingSlotKind::HiddenOccurrence => matches!(
                self.hidden
                    .get(slot.index)
                    .ok_or_else(|| slot_bounds_error("hidden", slot.index))?,
                Some(GraphHiddenOccurrence::Edge(_) | GraphHiddenOccurrence::Path(_))
            ),
        })
    }

    pub(crate) fn copy_slots_from(
        &mut self,
        other: &GraphBindingRow,
        slots: &[GraphBindingSlotRef],
    ) -> Result<(), EngineError> {
        for slot in slots {
            match slot.kind {
                GraphBindingSlotKind::Node => {
                    let value = other
                        .nodes
                        .get(slot.index)
                        .ok_or_else(|| slot_bounds_error("node", slot.index))?
                        .clone();
                    *self.node_slot_mut(*slot)? = value;
                }
                GraphBindingSlotKind::Edge => {
                    let value = other
                        .edges
                        .get(slot.index)
                        .ok_or_else(|| slot_bounds_error("edge", slot.index))?
                        .clone();
                    *self.edge_slot_mut(*slot)? = value;
                }
                GraphBindingSlotKind::Path => {
                    let value = other
                        .paths
                        .get(slot.index)
                        .ok_or_else(|| slot_bounds_error("path", slot.index))?
                        .clone();
                    *self.path_slot_mut(*slot)? = value;
                }
                GraphBindingSlotKind::Scalar => {
                    let value = other
                        .scalars
                        .get(slot.index)
                        .ok_or_else(|| slot_bounds_error("scalar", slot.index))?
                        .clone();
                    let target = self.scalars.get_mut(slot.index).ok_or_else(|| {
                        EngineError::InvalidOperation(format!(
                            "graph row scalar slot {} is out of bounds",
                            slot.index
                        ))
                    })?;
                    *target = value;
                }
                GraphBindingSlotKind::HiddenOccurrence => {
                    let value = other
                        .hidden
                        .get(slot.index)
                        .ok_or_else(|| slot_bounds_error("hidden", slot.index))?
                        .clone();
                    let target = self.hidden.get_mut(slot.index).ok_or_else(|| {
                        EngineError::InvalidOperation(format!(
                            "graph row hidden slot {} is out of bounds",
                            slot.index
                        ))
                    })?;
                    *target = value;
                }
            }
        }
        Ok(())
    }

    pub(crate) fn value_for_alias(
        &self,
        schema: &GraphBindingSchema,
        alias: &str,
    ) -> Result<GraphEvalValue, EngineError> {
        let slot = schema.slot_for_alias(alias).ok_or_else(|| {
            EngineError::InvalidOperation(format!("graph row references unknown binding '{alias}'"))
        })?;
        self.value_for_slot(slot)
    }

    pub(crate) fn value_for_slot(
        &self,
        slot: GraphBindingSlotRef,
    ) -> Result<GraphEvalValue, EngineError> {
        Ok(match slot.kind {
            GraphBindingSlotKind::Node => self
                .nodes
                .get(slot.index)
                .ok_or_else(|| slot_bounds_error("node", slot.index))?
                .eval_value("node", slot.index, GraphEvalValue::Node)?,
            GraphBindingSlotKind::Edge => self
                .edges
                .get(slot.index)
                .ok_or_else(|| slot_bounds_error("edge", slot.index))?
                .eval_value("edge", slot.index, GraphEvalValue::Edge)?,
            GraphBindingSlotKind::Path => self
                .paths
                .get(slot.index)
                .ok_or_else(|| slot_bounds_error("path", slot.index))?
                .eval_value("path", slot.index, GraphEvalValue::Path)?,
            GraphBindingSlotKind::Scalar => self
                .scalars
                .get(slot.index)
                .ok_or_else(|| slot_bounds_error("scalar", slot.index))?
                .eval_value("scalar", slot.index, |value| value)?,
            GraphBindingSlotKind::HiddenOccurrence => {
                return Err(EngineError::InvalidOperation(
                    "graph row hidden occurrence slots are not expression bindings".to_string(),
                ));
            }
        })
    }

    pub(crate) fn node_id_for_slot_if_bound(
        &self,
        slot: GraphBindingSlotRef,
    ) -> Result<Option<u64>, EngineError> {
        if slot.kind != GraphBindingSlotKind::Node {
            return Err(slot_kind_error(slot.kind, GraphBindingSlotKind::Node));
        }
        Ok(
            match self
                .nodes
                .get(slot.index)
                .ok_or_else(|| slot_bounds_error("node", slot.index))?
            {
                GraphSlotState::Unbound | GraphSlotState::Null => None,
                GraphSlotState::Bound(node) => Some(node.id),
            },
        )
    }

    pub(crate) fn edge_id_for_slot_if_bound(
        &self,
        slot: GraphBindingSlotRef,
    ) -> Result<Option<u64>, EngineError> {
        if slot.kind != GraphBindingSlotKind::Edge {
            return Err(slot_kind_error(slot.kind, GraphBindingSlotKind::Edge));
        }
        Ok(
            match self
                .edges
                .get(slot.index)
                .ok_or_else(|| slot_bounds_error("edge", slot.index))?
            {
                GraphSlotState::Unbound | GraphSlotState::Null => None,
                GraphSlotState::Bound(edge) => Some(edge.id),
            },
        )
    }

    pub(crate) fn hidden_edge_id_for_slot_if_bound(
        &self,
        slot: GraphBindingSlotRef,
    ) -> Result<Option<u64>, EngineError> {
        if slot.kind != GraphBindingSlotKind::HiddenOccurrence {
            return Err(slot_kind_error(
                slot.kind,
                GraphBindingSlotKind::HiddenOccurrence,
            ));
        }
        Ok(
            match self
                .hidden
                .get(slot.index)
                .ok_or_else(|| slot_bounds_error("hidden", slot.index))?
            {
                Some(GraphHiddenOccurrence::Edge(edge_id)) => Some(*edge_id),
                Some(GraphHiddenOccurrence::Null) | None => None,
                Some(GraphHiddenOccurrence::Path(_)) => {
                    return Err(EngineError::InvalidOperation(format!(
                    "graph row hidden slot {} contains a path occurrence, not an edge occurrence",
                    slot.index
                )));
                }
            },
        )
    }

    pub(crate) fn path_for_slot_if_bound(
        &self,
        slot: GraphBindingSlotRef,
    ) -> Result<Option<&GraphBoundPath>, EngineError> {
        self.path_for_slot(slot)
    }

    pub(crate) fn logical_sort_key(
        &self,
        schema: &GraphBindingSchema,
    ) -> Result<Vec<GraphSortAtom>, EngineError> {
        let mut key = Vec::with_capacity(schema.slots().len());
        for slot in schema.slots() {
            let slot_ref = GraphBindingSlotRef {
                kind: slot.kind,
                index: slot.index,
            };
            key.push(match slot.kind {
                GraphBindingSlotKind::Node => match self
                    .nodes
                    .get(slot.index)
                    .ok_or_else(|| slot_bounds_error("node", slot.index))?
                {
                    GraphSlotState::Bound(node) => GraphSortAtom::Node(node.id),
                    GraphSlotState::Null => GraphSortAtom::Null,
                    GraphSlotState::Unbound => {
                        return Err(unbound_logical_key_error(&slot.name));
                    }
                },
                GraphBindingSlotKind::Edge => match self
                    .edges
                    .get(slot.index)
                    .ok_or_else(|| slot_bounds_error("edge", slot.index))?
                {
                    GraphSlotState::Bound(edge) => GraphSortAtom::Edge(edge.id),
                    GraphSlotState::Null => GraphSortAtom::Null,
                    GraphSlotState::Unbound => {
                        return Err(unbound_logical_key_error(&slot.name));
                    }
                },
                GraphBindingSlotKind::Path => match self
                    .paths
                    .get(slot.index)
                    .ok_or_else(|| slot_bounds_error("path", slot.index))?
                {
                    GraphSlotState::Bound(path) => GraphSortAtom::Path {
                        hop_count: path.path.edges.len(),
                        nodes: path.path.nodes.clone(),
                        edges: path.path.edges.clone(),
                    },
                    GraphSlotState::Null => GraphSortAtom::Null,
                    GraphSlotState::Unbound => {
                        return Err(unbound_logical_key_error(&slot.name));
                    }
                },
                GraphBindingSlotKind::Scalar => graph_sort_atom_for_value(
                    &self
                        .scalars
                        .get(slot.index)
                        .ok_or_else(|| slot_bounds_error("scalar", slot.index))?
                        .eval_value("scalar", slot.index, |value| value.clone())?,
                )?,
                GraphBindingSlotKind::HiddenOccurrence => match self
                    .hidden
                    .get(slot_ref.index)
                    .ok_or_else(|| slot_bounds_error("hidden", slot_ref.index))?
                {
                    Some(GraphHiddenOccurrence::Edge(edge_id)) => GraphSortAtom::Edge(*edge_id),
                    Some(GraphHiddenOccurrence::Path(path)) => GraphSortAtom::Path {
                        hop_count: path.edges.len(),
                        nodes: path.nodes.clone(),
                        edges: path.edges.clone(),
                    },
                    Some(GraphHiddenOccurrence::Null) => GraphSortAtom::Null,
                    None => return Err(unbound_logical_key_error(&slot.name)),
                },
            });
        }
        Ok(key)
    }

    pub(crate) fn logical_sort_key_for_slots(
        &self,
        schema: &GraphBindingSchema,
        slots: &[GraphBindingSlotRef],
    ) -> Result<Vec<GraphSortAtom>, EngineError> {
        let mut key = Vec::with_capacity(slots.len());
        for slot in slots {
            let slot_info = schema.slot(*slot).ok_or_else(|| {
                EngineError::InvalidOperation(format!(
                    "graph row slot {:?}:{} is not part of the binding schema",
                    slot.kind, slot.index
                ))
            })?;
            key.push(self.sort_atom_for_slot(*slot, &slot_info.name)?);
        }
        Ok(key)
    }

    fn sort_atom_for_slot(
        &self,
        slot: GraphBindingSlotRef,
        name: &str,
    ) -> Result<GraphSortAtom, EngineError> {
        Ok(match slot.kind {
            GraphBindingSlotKind::Node => match self
                .nodes
                .get(slot.index)
                .ok_or_else(|| slot_bounds_error("node", slot.index))?
            {
                GraphSlotState::Bound(node) => GraphSortAtom::Node(node.id),
                GraphSlotState::Null => GraphSortAtom::Null,
                GraphSlotState::Unbound => return Err(unbound_logical_key_error(name)),
            },
            GraphBindingSlotKind::Edge => match self
                .edges
                .get(slot.index)
                .ok_or_else(|| slot_bounds_error("edge", slot.index))?
            {
                GraphSlotState::Bound(edge) => GraphSortAtom::Edge(edge.id),
                GraphSlotState::Null => GraphSortAtom::Null,
                GraphSlotState::Unbound => return Err(unbound_logical_key_error(name)),
            },
            GraphBindingSlotKind::Path => match self
                .paths
                .get(slot.index)
                .ok_or_else(|| slot_bounds_error("path", slot.index))?
            {
                GraphSlotState::Bound(path) => GraphSortAtom::Path {
                    hop_count: path.path.edges.len(),
                    nodes: path.path.nodes.clone(),
                    edges: path.path.edges.clone(),
                },
                GraphSlotState::Null => GraphSortAtom::Null,
                GraphSlotState::Unbound => return Err(unbound_logical_key_error(name)),
            },
            GraphBindingSlotKind::Scalar => graph_sort_atom_for_value(
                &self
                    .scalars
                    .get(slot.index)
                    .ok_or_else(|| slot_bounds_error("scalar", slot.index))?
                    .eval_value("scalar", slot.index, |value| value.clone())?,
            )?,
            GraphBindingSlotKind::HiddenOccurrence => match self
                .hidden
                .get(slot.index)
                .ok_or_else(|| slot_bounds_error("hidden", slot.index))?
            {
                Some(GraphHiddenOccurrence::Edge(edge_id)) => GraphSortAtom::Edge(*edge_id),
                Some(GraphHiddenOccurrence::Path(path)) => GraphSortAtom::Path {
                    hop_count: path.edges.len(),
                    nodes: path.nodes.clone(),
                    edges: path.edges.clone(),
                },
                Some(GraphHiddenOccurrence::Null) => GraphSortAtom::Null,
                None => return Err(unbound_logical_key_error(name)),
            },
        })
    }

    fn node_for_slot(
        &self,
        slot: GraphBindingSlotRef,
    ) -> Result<Option<&GraphBoundNode>, EngineError> {
        if slot.kind != GraphBindingSlotKind::Node {
            return Err(slot_kind_error(slot.kind, GraphBindingSlotKind::Node));
        }
        self.nodes
            .get(slot.index)
            .ok_or_else(|| slot_bounds_error("node", slot.index))?
            .as_ref("node", slot.index)
    }

    fn edge_for_slot(
        &self,
        slot: GraphBindingSlotRef,
    ) -> Result<Option<&GraphBoundEdge>, EngineError> {
        if slot.kind != GraphBindingSlotKind::Edge {
            return Err(slot_kind_error(slot.kind, GraphBindingSlotKind::Edge));
        }
        self.edges
            .get(slot.index)
            .ok_or_else(|| slot_bounds_error("edge", slot.index))?
            .as_ref("edge", slot.index)
    }

    fn path_for_slot(
        &self,
        slot: GraphBindingSlotRef,
    ) -> Result<Option<&GraphBoundPath>, EngineError> {
        if slot.kind != GraphBindingSlotKind::Path {
            return Err(slot_kind_error(slot.kind, GraphBindingSlotKind::Path));
        }
        self.paths
            .get(slot.index)
            .ok_or_else(|| slot_bounds_error("path", slot.index))?
            .as_ref("path", slot.index)
    }

    fn node_slot_mut(
        &mut self,
        slot: GraphBindingSlotRef,
    ) -> Result<&mut GraphSlotState<GraphBoundNode>, EngineError> {
        if slot.kind != GraphBindingSlotKind::Node {
            return Err(slot_kind_error(slot.kind, GraphBindingSlotKind::Node));
        }
        self.nodes
            .get_mut(slot.index)
            .ok_or_else(|| slot_bounds_error("node", slot.index))
    }

    fn edge_slot_mut(
        &mut self,
        slot: GraphBindingSlotRef,
    ) -> Result<&mut GraphSlotState<GraphBoundEdge>, EngineError> {
        if slot.kind != GraphBindingSlotKind::Edge {
            return Err(slot_kind_error(slot.kind, GraphBindingSlotKind::Edge));
        }
        self.edges
            .get_mut(slot.index)
            .ok_or_else(|| slot_bounds_error("edge", slot.index))
    }

    fn path_slot_mut(
        &mut self,
        slot: GraphBindingSlotRef,
    ) -> Result<&mut GraphSlotState<GraphBoundPath>, EngineError> {
        if slot.kind != GraphBindingSlotKind::Path {
            return Err(slot_kind_error(slot.kind, GraphBindingSlotKind::Path));
        }
        self.paths
            .get_mut(slot.index)
            .ok_or_else(|| slot_bounds_error("path", slot.index))
    }
}

impl<T: Clone> GraphSlotState<T> {
    fn eval_value(
        &self,
        kind: &str,
        index: usize,
        wrap: impl FnOnce(T) -> GraphEvalValue,
    ) -> Result<GraphEvalValue, EngineError> {
        match self {
            Self::Unbound => Err(EngineError::InvalidOperation(format!(
                "graph row {kind} slot {index} is unbound"
            ))),
            Self::Null => Ok(GraphEvalValue::Null),
            Self::Bound(value) => Ok(wrap(value.clone())),
        }
    }
}

impl<T> GraphSlotState<T> {
    fn as_ref(&self, kind: &str, index: usize) -> Result<Option<&T>, EngineError> {
        match self {
            Self::Unbound => Err(EngineError::InvalidOperation(format!(
                "graph row {kind} slot {index} is unbound"
            ))),
            Self::Null => Ok(None),
            Self::Bound(value) => Ok(Some(value)),
        }
    }
}

fn set_slot_null<T>(target: &mut GraphSlotState<T>, name: &str) -> Result<(), EngineError> {
    match target {
        GraphSlotState::Unbound | GraphSlotState::Null => {
            *target = GraphSlotState::Null;
            Ok(())
        }
        GraphSlotState::Bound(_) => Err(EngineError::InvalidOperation(format!(
            "graph row binding '{name}' is already bound and cannot be set to null"
        ))),
    }
}

fn bind_value_slot<T: PartialEq>(
    target: &mut GraphSlotState<T>,
    value: T,
    kind: &str,
) -> Result<(), EngineError> {
    match target {
        GraphSlotState::Unbound => {
            *target = GraphSlotState::Bound(value);
            Ok(())
        }
        GraphSlotState::Null => Err(EngineError::InvalidOperation(format!(
            "graph row null {kind} binding cannot be rebound"
        ))),
        GraphSlotState::Bound(existing) if existing == &value => Ok(()),
        GraphSlotState::Bound(_) => Err(EngineError::InvalidOperation(format!(
            "graph row conflicting {kind} binding"
        ))),
    }
}

fn bind_node_slot(
    target: &mut GraphSlotState<GraphBoundNode>,
    mut node: GraphBoundNode,
) -> Result<(), EngineError> {
    node.normalize_element_id()?;
    match target {
        GraphSlotState::Unbound => {
            *target = GraphSlotState::Bound(node);
            Ok(())
        }
        GraphSlotState::Null => Err(EngineError::InvalidOperation(
            "graph row null node binding cannot be rebound".to_string(),
        )),
        GraphSlotState::Bound(existing) if existing.id == node.id => {
            existing.merge_from(node);
            Ok(())
        }
        GraphSlotState::Bound(_) => Err(EngineError::InvalidOperation(
            "graph row conflicting node binding".to_string(),
        )),
    }
}

fn bind_edge_slot(
    target: &mut GraphSlotState<GraphBoundEdge>,
    mut edge: GraphBoundEdge,
) -> Result<(), EngineError> {
    edge.normalize_element_id()?;
    match target {
        GraphSlotState::Unbound => {
            *target = GraphSlotState::Bound(edge);
            Ok(())
        }
        GraphSlotState::Null => Err(EngineError::InvalidOperation(
            "graph row null edge binding cannot be rebound".to_string(),
        )),
        GraphSlotState::Bound(existing) if existing.id == edge.id => {
            existing.merge_from(edge);
            Ok(())
        }
        GraphSlotState::Bound(_) => Err(EngineError::InvalidOperation(
            "graph row conflicting edge binding".to_string(),
        )),
    }
}

fn bind_path_slot(
    target: &mut GraphSlotState<GraphBoundPath>,
    mut path: GraphBoundPath,
) -> Result<(), EngineError> {
    path.normalize_element_ids()?;
    match target {
        GraphSlotState::Unbound => {
            *target = GraphSlotState::Bound(path);
            Ok(())
        }
        GraphSlotState::Null => Err(EngineError::InvalidOperation(
            "graph row null path binding cannot be rebound".to_string(),
        )),
        GraphSlotState::Bound(existing) if existing.path == path.path => {
            existing.merge_from(path);
            Ok(())
        }
        GraphSlotState::Bound(_) => Err(EngineError::InvalidOperation(
            "graph row conflicting path binding".to_string(),
        )),
    }
}

fn bind_optional_slot<T: PartialEq>(
    target: &mut Option<T>,
    value: T,
    kind: &str,
) -> Result<(), EngineError> {
    match target {
        Some(existing) if existing == &value => Ok(()),
        Some(_) => Err(EngineError::InvalidOperation(format!(
            "graph row conflicting {kind} binding"
        ))),
        None => {
            *target = Some(value);
            Ok(())
        }
    }
}

fn slot_kind_error(actual: GraphBindingSlotKind, expected: GraphBindingSlotKind) -> EngineError {
    EngineError::InvalidOperation(format!(
        "graph row binding slot has kind {actual:?}, expected {expected:?}"
    ))
}

fn slot_bounds_error(kind: &str, index: usize) -> EngineError {
    EngineError::InvalidOperation(format!("graph row {kind} slot {index} is out of bounds"))
}

fn unbound_logical_key_error(name: &str) -> EngineError {
    EngineError::InvalidOperation(format!(
        "graph row binding '{name}' is unbound in final logical row key"
    ))
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct GraphBoundNode {
    pub(crate) id: u64,
    pub(crate) element: Option<GraphNodeValue>,
}

impl GraphBoundNode {
    pub(crate) fn id_only(id: u64) -> Self {
        Self { id, element: None }
    }

    pub(crate) fn with_element(id: u64, element: GraphNodeValue) -> Self {
        Self {
            id,
            element: Some(element),
        }
    }

    fn merge_from(&mut self, other: Self) {
        debug_assert_eq!(self.id, other.id);
        merge_optional_node_element(&mut self.element, other.element);
    }

    fn normalize_element_id(&mut self) -> Result<(), EngineError> {
        let Some(element) = self.element.as_mut() else {
            return Ok(());
        };
        match element.id {
            Some(id) if id != self.id => Err(EngineError::InvalidOperation(format!(
                "graph row node element id {id} does not match binding id {}",
                self.id
            ))),
            Some(_) => Ok(()),
            None => {
                element.id = Some(self.id);
                Ok(())
            }
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct GraphBoundEdge {
    pub(crate) id: u64,
    pub(crate) element: Option<GraphEdgeValue>,
}

impl GraphBoundEdge {
    pub(crate) fn id_only(id: u64) -> Self {
        Self { id, element: None }
    }

    pub(crate) fn with_element(id: u64, element: GraphEdgeValue) -> Self {
        Self {
            id,
            element: Some(element),
        }
    }

    fn merge_from(&mut self, other: Self) {
        debug_assert_eq!(self.id, other.id);
        merge_optional_edge_element(&mut self.element, other.element);
    }

    fn normalize_element_id(&mut self) -> Result<(), EngineError> {
        let Some(element) = self.element.as_mut() else {
            return Ok(());
        };
        match element.id {
            Some(id) if id != self.id => Err(EngineError::InvalidOperation(format!(
                "graph row edge element id {id} does not match binding id {}",
                self.id
            ))),
            Some(_) => Ok(()),
            None => {
                element.id = Some(self.id);
                Ok(())
            }
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct GraphBoundPath {
    pub(crate) path: GraphPath,
    pub(crate) nodes: Vec<GraphBoundNode>,
    pub(crate) edges: Vec<GraphBoundEdge>,
}

impl GraphBoundPath {
    pub(crate) fn id_only(path: GraphPath) -> Result<Self, EngineError> {
        validate_graph_path_shape(&path)?;
        let nodes = path
            .nodes
            .iter()
            .copied()
            .map(GraphBoundNode::id_only)
            .collect();
        let edges = path
            .edges
            .iter()
            .copied()
            .map(GraphBoundEdge::id_only)
            .collect();
        Ok(Self { path, nodes, edges })
    }

    pub(crate) fn with_values(
        path: GraphPath,
        nodes: Vec<GraphBoundNode>,
        edges: Vec<GraphBoundEdge>,
    ) -> Result<Self, EngineError> {
        validate_graph_path_shape(&path)?;
        if path.nodes.len() != nodes.len() {
            return Err(EngineError::InvalidOperation(format!(
                "graph row synthetic path has {} node ids but {} node values",
                path.nodes.len(),
                nodes.len()
            )));
        }
        if path.edges.len() != edges.len() {
            return Err(EngineError::InvalidOperation(format!(
                "graph row synthetic path has {} edge ids but {} edge values",
                path.edges.len(),
                edges.len()
            )));
        }
        if path
            .nodes
            .iter()
            .zip(&nodes)
            .any(|(id, node)| *id != node.id)
        {
            return Err(EngineError::InvalidOperation(
                "graph row synthetic path node values must match node id order".to_string(),
            ));
        }
        if path
            .edges
            .iter()
            .zip(&edges)
            .any(|(id, edge)| *id != edge.id)
        {
            return Err(EngineError::InvalidOperation(
                "graph row synthetic path edge values must match edge id order".to_string(),
            ));
        }
        let mut bound_path = Self { path, nodes, edges };
        bound_path.normalize_element_ids()?;
        Ok(bound_path)
    }

    fn merge_from(&mut self, other: Self) {
        debug_assert_eq!(self.path, other.path);
        for (existing, incoming) in self.nodes.iter_mut().zip(other.nodes) {
            existing.merge_from(incoming);
        }
        for (existing, incoming) in self.edges.iter_mut().zip(other.edges) {
            existing.merge_from(incoming);
        }
    }

    fn normalize_element_ids(&mut self) -> Result<(), EngineError> {
        for node in &mut self.nodes {
            node.normalize_element_id()?;
        }
        for edge in &mut self.edges {
            edge.normalize_element_id()?;
        }
        if self
            .path
            .nodes
            .iter()
            .zip(&self.nodes)
            .any(|(id, node)| *id != node.id)
        {
            return Err(EngineError::InvalidOperation(
                "graph row path node values must match node id order".to_string(),
            ));
        }
        if self
            .path
            .edges
            .iter()
            .zip(&self.edges)
            .any(|(id, edge)| *id != edge.id)
        {
            return Err(EngineError::InvalidOperation(
                "graph row path edge values must match edge id order".to_string(),
            ));
        }
        Ok(())
    }
}

fn merge_optional_node_element(
    target: &mut Option<GraphNodeValue>,
    incoming: Option<GraphNodeValue>,
) {
    let Some(incoming) = incoming else {
        return;
    };
    let Some(target) = target.as_mut() else {
        *target = Some(incoming);
        return;
    };
    merge_option(&mut target.id, incoming.id);
    merge_option(&mut target.labels, incoming.labels);
    merge_option(&mut target.key, incoming.key);
    merge_optional_graph_props(&mut target.props, incoming.props);
    merge_option(&mut target.weight, incoming.weight);
    merge_option(&mut target.created_at, incoming.created_at);
    merge_option(&mut target.updated_at, incoming.updated_at);
    merge_option(&mut target.dense_vector, incoming.dense_vector);
    merge_option(&mut target.sparse_vector, incoming.sparse_vector);
}

fn merge_optional_edge_element(
    target: &mut Option<GraphEdgeValue>,
    incoming: Option<GraphEdgeValue>,
) {
    let Some(incoming) = incoming else {
        return;
    };
    let Some(target) = target.as_mut() else {
        *target = Some(incoming);
        return;
    };
    merge_option(&mut target.id, incoming.id);
    merge_option(&mut target.from, incoming.from);
    merge_option(&mut target.to, incoming.to);
    merge_option(&mut target.label, incoming.label);
    merge_optional_graph_props(&mut target.props, incoming.props);
    merge_option(&mut target.weight, incoming.weight);
    merge_option(&mut target.created_at, incoming.created_at);
    merge_option(&mut target.updated_at, incoming.updated_at);
    merge_option(&mut target.valid_from, incoming.valid_from);
    merge_option(&mut target.valid_to, incoming.valid_to);
}

fn merge_option<T>(target: &mut Option<T>, incoming: Option<T>) {
    if target.is_none() {
        *target = incoming;
    }
}

fn merge_optional_graph_props(
    target: &mut Option<BTreeMap<String, GraphValue>>,
    incoming: Option<BTreeMap<String, GraphValue>>,
) {
    match (target.as_mut(), incoming) {
        (Some(target), Some(incoming)) => {
            for (key, value) in incoming {
                target.entry(key).or_insert(value);
            }
        }
        (None, Some(incoming)) => *target = Some(incoming),
        _ => {}
    }
}

fn validate_graph_path_shape(path: &GraphPath) -> Result<(), EngineError> {
    if path.nodes.is_empty() {
        return Err(EngineError::InvalidOperation(
            "graph row path must contain at least one node id".to_string(),
        ));
    }
    if path.nodes.len() != path.edges.len().saturating_add(1) {
        return Err(EngineError::InvalidOperation(format!(
            "graph row path must have exactly one more node id than edge id, got {} node id(s) and {} edge id(s)",
            path.nodes.len(),
            path.edges.len()
        )));
    }
    Ok(())
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) enum GraphHiddenOccurrence {
    Null,
    Edge(u64),
    Path(GraphPath),
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) enum GraphEvalValue {
    Null,
    Bool(bool),
    Int(i64),
    UInt(u64),
    Float(f64),
    String(String),
    Bytes(Vec<u8>),
    List(Vec<GraphEvalValue>),
    Map(BTreeMap<String, GraphEvalValue>),
    Node(GraphBoundNode),
    Edge(GraphBoundEdge),
    Path(GraphBoundPath),
}

impl GraphEvalValue {
    fn is_null(&self) -> bool {
        matches!(self, Self::Null)
    }
}

pub(crate) struct GraphEvalContext<'a> {
    pub(crate) schema: &'a GraphBindingSchema,
    pub(crate) row: &'a GraphBindingRow,
    pub(crate) params: &'a BTreeMap<String, GraphParamValue>,
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) enum BoundGraphExpr {
    Null,
    Bool(bool),
    Int(i64),
    UInt(u64),
    Float(f64),
    String(String),
    Bytes(Vec<u8>),
    List(Vec<BoundGraphExpr>),
    Map(BTreeMap<String, BoundGraphExpr>),
    Binding(GraphBindingSlotRef),
    Property {
        slot: GraphBindingSlotRef,
        key: String,
    },
    NodeField {
        slot: GraphBindingSlotRef,
        field: GraphNodeField,
    },
    EdgeField {
        slot: GraphBindingSlotRef,
        field: GraphEdgeField,
    },
    PathField {
        slot: GraphBindingSlotRef,
        field: GraphPathField,
    },
    Function {
        name: GraphFunction,
        args: Vec<BoundGraphExpr>,
    },
    Unary {
        op: GraphUnaryOp,
        expr: Box<BoundGraphExpr>,
    },
    Binary {
        left: Box<BoundGraphExpr>,
        op: GraphBinaryOp,
        right: Box<BoundGraphExpr>,
    },
    IsNull(Box<BoundGraphExpr>),
    IsNotNull(Box<BoundGraphExpr>),
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct BoundGraphReturnItem {
    pub(crate) expr: BoundGraphExpr,
    pub(crate) projection: GraphReturnProjection,
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct BoundGraphOrderItem {
    pub(crate) expr: BoundGraphExpr,
    pub(crate) direction: GraphOrderDirection,
}

pub(crate) struct BoundGraphEvalContext<'a> {
    pub(crate) row: &'a GraphBindingRow,
}

pub(crate) fn bind_graph_expr(
    schema: &GraphBindingSchema,
    expr: &GraphExpr,
) -> Result<BoundGraphExpr, EngineError> {
    bind_graph_expr_with_params(schema, expr, None)
}

fn bind_graph_expr_with_params(
    schema: &GraphBindingSchema,
    expr: &GraphExpr,
    params: Option<&BTreeMap<String, GraphParamValue>>,
) -> Result<BoundGraphExpr, EngineError> {
    Ok(match expr {
        GraphExpr::Null => BoundGraphExpr::Null,
        GraphExpr::Bool(value) => BoundGraphExpr::Bool(*value),
        GraphExpr::Int(value) => BoundGraphExpr::Int(*value),
        GraphExpr::UInt(value) => BoundGraphExpr::UInt(*value),
        GraphExpr::Float(value) => BoundGraphExpr::Float(*value),
        GraphExpr::String(value) => BoundGraphExpr::String(value.clone()),
        GraphExpr::Bytes(value) => BoundGraphExpr::Bytes(value.clone()),
        GraphExpr::List(items) => BoundGraphExpr::List(
            items
                .iter()
                .map(|item| bind_graph_expr_with_params(schema, item, params))
                .collect::<Result<Vec<_>, _>>()?,
        ),
        GraphExpr::Map(items) => BoundGraphExpr::Map(
            items
                .iter()
                .map(|(key, value)| {
                    Ok((
                        key.clone(),
                        bind_graph_expr_with_params(schema, value, params)?,
                    ))
                })
                .collect::<Result<BTreeMap<_, _>, EngineError>>()?,
        ),
        GraphExpr::Param(name) => {
            let Some(params) = params else {
                return Err(EngineError::InvalidOperation(format!(
                    "graph row param '{name}' must be resolved before expression binding"
                )));
            };
            graph_param_to_bound(params.get(name).ok_or_else(|| {
                EngineError::InvalidOperation(format!(
                    "graph row expression references missing param '{name}'"
                ))
            })?)?
        }
        GraphExpr::Binding(alias) => BoundGraphExpr::Binding(bind_alias_slot(schema, alias)?),
        GraphExpr::Property { alias, key } => {
            let slot = bind_alias_slot(schema, alias)?;
            match slot.kind {
                GraphBindingSlotKind::Node | GraphBindingSlotKind::Edge => {
                    BoundGraphExpr::Property {
                        slot,
                        key: key.clone(),
                    }
                }
                GraphBindingSlotKind::Path => {
                    return Err(EngineError::InvalidOperation(format!(
                        "graph row property expression cannot reference path alias '{alias}'"
                    )));
                }
                GraphBindingSlotKind::Scalar | GraphBindingSlotKind::HiddenOccurrence => {
                    return Err(EngineError::InvalidOperation(format!(
                        "graph row property expression requires a node or edge alias, got '{alias}'"
                    )));
                }
            }
        }
        GraphExpr::NodeField { alias, field } => {
            let slot = bind_alias_slot(schema, alias)?;
            if slot.kind != GraphBindingSlotKind::Node {
                return Err(EngineError::InvalidOperation(format!(
                    "graph row node field references non-node alias '{alias}'"
                )));
            }
            BoundGraphExpr::NodeField {
                slot,
                field: *field,
            }
        }
        GraphExpr::EdgeField { alias, field } => {
            let slot = bind_alias_slot(schema, alias)?;
            if slot.kind != GraphBindingSlotKind::Edge {
                return Err(EngineError::InvalidOperation(format!(
                    "graph row edge field references non-edge alias '{alias}'"
                )));
            }
            BoundGraphExpr::EdgeField {
                slot,
                field: *field,
            }
        }
        GraphExpr::PathField { alias, field } => {
            let slot = bind_alias_slot(schema, alias)?;
            if slot.kind != GraphBindingSlotKind::Path {
                return Err(EngineError::InvalidOperation(format!(
                    "graph row path field references non-path alias '{alias}'"
                )));
            }
            BoundGraphExpr::PathField {
                slot,
                field: *field,
            }
        }
        GraphExpr::Function { name, args } => BoundGraphExpr::Function {
            name: *name,
            args: args
                .iter()
                .map(|arg| bind_graph_expr_with_params(schema, arg, params))
                .collect::<Result<Vec<_>, _>>()?,
        },
        GraphExpr::Unary { op, expr } => BoundGraphExpr::Unary {
            op: *op,
            expr: Box::new(bind_graph_expr_with_params(schema, expr, params)?),
        },
        GraphExpr::Binary { left, op, right } => BoundGraphExpr::Binary {
            left: Box::new(bind_graph_expr_with_params(schema, left, params)?),
            op: *op,
            right: Box::new(bind_graph_expr_with_params(schema, right, params)?),
        },
        GraphExpr::IsNull(expr) => {
            BoundGraphExpr::IsNull(Box::new(bind_graph_expr_with_params(schema, expr, params)?))
        }
        GraphExpr::IsNotNull(expr) => {
            BoundGraphExpr::IsNotNull(Box::new(bind_graph_expr_with_params(schema, expr, params)?))
        }
    })
}

fn graph_param_to_bound(value: &GraphParamValue) -> Result<BoundGraphExpr, EngineError> {
    Ok(match value {
        GraphParamValue::Null => BoundGraphExpr::Null,
        GraphParamValue::Bool(value) => BoundGraphExpr::Bool(*value),
        GraphParamValue::Int(value) => BoundGraphExpr::Int(*value),
        GraphParamValue::UInt(value) => BoundGraphExpr::UInt(*value),
        GraphParamValue::Float(value) => BoundGraphExpr::Float(*value),
        GraphParamValue::String(value) => BoundGraphExpr::String(value.clone()),
        GraphParamValue::Bytes(value) => BoundGraphExpr::Bytes(value.clone()),
        GraphParamValue::List(values) => BoundGraphExpr::List(
            values
                .iter()
                .map(graph_param_to_bound)
                .collect::<Result<Vec<_>, _>>()?,
        ),
        GraphParamValue::Map(values) => BoundGraphExpr::Map(
            values
                .iter()
                .map(|(key, value)| Ok((key.clone(), graph_param_to_bound(value)?)))
                .collect::<Result<BTreeMap<_, _>, EngineError>>()?,
        ),
    })
}

pub(crate) fn bind_graph_return_items(
    schema: &GraphBindingSchema,
    return_items: &[GraphReturnItem],
) -> Result<Vec<BoundGraphReturnItem>, EngineError> {
    return_items
        .iter()
        .map(|item| {
            Ok(BoundGraphReturnItem {
                expr: bind_graph_expr(schema, &item.expr)?,
                projection: item.projection.clone(),
            })
        })
        .collect()
}

pub(crate) fn bind_graph_order_items(
    schema: &GraphBindingSchema,
    order_by: &[GraphOrderItem],
) -> Result<Vec<BoundGraphOrderItem>, EngineError> {
    order_by
        .iter()
        .map(|item| {
            Ok(BoundGraphOrderItem {
                expr: bind_graph_expr(schema, &item.expr)?,
                direction: item.direction,
            })
        })
        .collect()
}

fn bind_alias_slot(
    schema: &GraphBindingSchema,
    alias: &str,
) -> Result<GraphBindingSlotRef, EngineError> {
    schema.slot_for_alias(alias).ok_or_else(|| {
        EngineError::InvalidOperation(format!("graph row references unknown binding '{alias}'"))
    })
}

pub(crate) fn eval_graph_expr(
    expr: &GraphExpr,
    context: &GraphEvalContext<'_>,
) -> Result<GraphEvalValue, EngineError> {
    match expr {
        GraphExpr::Null => Ok(GraphEvalValue::Null),
        GraphExpr::Bool(value) => Ok(GraphEvalValue::Bool(*value)),
        GraphExpr::Int(value) => Ok(GraphEvalValue::Int(*value)),
        GraphExpr::UInt(value) => Ok(GraphEvalValue::UInt(*value)),
        GraphExpr::Float(value) => Ok(GraphEvalValue::Float(*value)),
        GraphExpr::String(value) => Ok(GraphEvalValue::String(value.clone())),
        GraphExpr::Bytes(value) => Ok(GraphEvalValue::Bytes(value.clone())),
        GraphExpr::List(items) => {
            let mut values = Vec::with_capacity(items.len());
            for item in items {
                values.push(eval_graph_expr(item, context)?);
            }
            Ok(GraphEvalValue::List(values))
        }
        GraphExpr::Map(items) => {
            let mut values = BTreeMap::new();
            for (key, value) in items {
                values.insert(key.clone(), eval_graph_expr(value, context)?);
            }
            Ok(GraphEvalValue::Map(values))
        }
        GraphExpr::Param(name) => {
            graph_param_to_eval(context.params.get(name).ok_or_else(|| {
                EngineError::InvalidOperation(format!(
                    "graph row expression references missing param '{name}'"
                ))
            })?)
        }
        GraphExpr::Binding(alias) => context.row.value_for_alias(context.schema, alias),
        GraphExpr::Property { alias, key } => eval_graph_property(alias, key, context),
        GraphExpr::NodeField { alias, field } => eval_graph_node_field(alias, *field, context),
        GraphExpr::EdgeField { alias, field } => eval_graph_edge_field(alias, *field, context),
        GraphExpr::PathField { alias, field } => eval_graph_path_field(alias, *field, context),
        GraphExpr::Function { name, args } => eval_graph_function(*name, args, context),
        GraphExpr::Unary {
            op: GraphUnaryOp::Not,
            expr,
        } => match bool_or_null(&eval_graph_expr(expr, context)?)? {
            Some(value) => Ok(GraphEvalValue::Bool(!value)),
            None => Ok(GraphEvalValue::Null),
        },
        GraphExpr::Binary { left, op, right } => eval_graph_binary(left, *op, right, context),
        GraphExpr::IsNull(expr) => Ok(GraphEvalValue::Bool(
            eval_graph_expr(expr, context)?.is_null(),
        )),
        GraphExpr::IsNotNull(expr) => Ok(GraphEvalValue::Bool(
            !eval_graph_expr(expr, context)?.is_null(),
        )),
    }
}

pub(crate) fn eval_graph_predicate(
    expr: &GraphExpr,
    context: &GraphEvalContext<'_>,
) -> Result<bool, EngineError> {
    match eval_graph_expr(expr, context)? {
        GraphEvalValue::Bool(value) => Ok(value),
        GraphEvalValue::Null => Ok(false),
        _ => Err(EngineError::InvalidOperation(
            "graph row WHERE expressions must evaluate to a boolean or null".to_string(),
        )),
    }
}

pub(crate) fn eval_bound_graph_expr(
    expr: &BoundGraphExpr,
    context: &BoundGraphEvalContext<'_>,
) -> Result<GraphEvalValue, EngineError> {
    match expr {
        BoundGraphExpr::Null => Ok(GraphEvalValue::Null),
        BoundGraphExpr::Bool(value) => Ok(GraphEvalValue::Bool(*value)),
        BoundGraphExpr::Int(value) => Ok(GraphEvalValue::Int(*value)),
        BoundGraphExpr::UInt(value) => Ok(GraphEvalValue::UInt(*value)),
        BoundGraphExpr::Float(value) => Ok(GraphEvalValue::Float(*value)),
        BoundGraphExpr::String(value) => Ok(GraphEvalValue::String(value.clone())),
        BoundGraphExpr::Bytes(value) => Ok(GraphEvalValue::Bytes(value.clone())),
        BoundGraphExpr::List(items) => {
            let mut values = Vec::with_capacity(items.len());
            for item in items {
                values.push(eval_bound_graph_expr(item, context)?);
            }
            Ok(GraphEvalValue::List(values))
        }
        BoundGraphExpr::Map(items) => {
            let mut values = BTreeMap::new();
            for (key, value) in items {
                values.insert(key.clone(), eval_bound_graph_expr(value, context)?);
            }
            Ok(GraphEvalValue::Map(values))
        }
        BoundGraphExpr::Binding(slot) => context.row.value_for_slot(*slot),
        BoundGraphExpr::Property { slot, key } => eval_bound_graph_property(*slot, key, context),
        BoundGraphExpr::NodeField { slot, field } => {
            eval_bound_graph_node_field(*slot, *field, context)
        }
        BoundGraphExpr::EdgeField { slot, field } => {
            eval_bound_graph_edge_field(*slot, *field, context)
        }
        BoundGraphExpr::PathField { slot, field } => {
            eval_bound_graph_path_field(*slot, *field, context)
        }
        BoundGraphExpr::Function { name, args } => eval_bound_graph_function(*name, args, context),
        BoundGraphExpr::Unary {
            op: GraphUnaryOp::Not,
            expr,
        } => match bool_or_null(&eval_bound_graph_expr(expr, context)?)? {
            Some(value) => Ok(GraphEvalValue::Bool(!value)),
            None => Ok(GraphEvalValue::Null),
        },
        BoundGraphExpr::Binary { left, op, right } => {
            eval_bound_graph_binary(left, *op, right, context)
        }
        BoundGraphExpr::IsNull(expr) => Ok(GraphEvalValue::Bool(
            eval_bound_graph_expr(expr, context)?.is_null(),
        )),
        BoundGraphExpr::IsNotNull(expr) => Ok(GraphEvalValue::Bool(
            !eval_bound_graph_expr(expr, context)?.is_null(),
        )),
    }
}

pub(crate) fn eval_bound_graph_predicate(
    expr: &BoundGraphExpr,
    context: &BoundGraphEvalContext<'_>,
) -> Result<bool, EngineError> {
    match eval_bound_graph_expr(expr, context)? {
        GraphEvalValue::Bool(value) => Ok(value),
        GraphEvalValue::Null => Ok(false),
        _ => Err(EngineError::InvalidOperation(
            "graph row WHERE expressions must evaluate to a boolean or null".to_string(),
        )),
    }
}

fn eval_bound_graph_property(
    slot: GraphBindingSlotRef,
    key: &str,
    context: &BoundGraphEvalContext<'_>,
) -> Result<GraphEvalValue, EngineError> {
    match slot.kind {
        GraphBindingSlotKind::Node => {
            let Some(node) = context.row.node_for_slot(slot)? else {
                return Ok(GraphEvalValue::Null);
            };
            let props = node
                .element
                .as_ref()
                .ok_or_else(|| unloaded_node_field_error("bound node", "props"))?
                .props
                .as_ref()
                .ok_or_else(|| unloaded_node_field_error("bound node", "props"))?;
            Ok(props
                .get(key)
                .cloned()
                .map(graph_value_to_eval)
                .transpose()?
                .unwrap_or(GraphEvalValue::Null))
        }
        GraphBindingSlotKind::Edge => {
            let Some(edge) = context.row.edge_for_slot(slot)? else {
                return Ok(GraphEvalValue::Null);
            };
            let props = edge
                .element
                .as_ref()
                .ok_or_else(|| unloaded_edge_field_error("bound edge", "props"))?
                .props
                .as_ref()
                .ok_or_else(|| unloaded_edge_field_error("bound edge", "props"))?;
            Ok(props
                .get(key)
                .cloned()
                .map(graph_value_to_eval)
                .transpose()?
                .unwrap_or(GraphEvalValue::Null))
        }
        GraphBindingSlotKind::Path => Err(EngineError::InvalidOperation(
            "graph row property expression cannot reference path bindings".to_string(),
        )),
        GraphBindingSlotKind::Scalar | GraphBindingSlotKind::HiddenOccurrence => {
            Err(EngineError::InvalidOperation(
                "graph row property expression requires a node or edge binding".to_string(),
            ))
        }
    }
}

fn eval_bound_graph_node_field(
    slot: GraphBindingSlotRef,
    field: GraphNodeField,
    context: &BoundGraphEvalContext<'_>,
) -> Result<GraphEvalValue, EngineError> {
    let Some(node) = context.row.node_for_slot(slot)? else {
        return Ok(GraphEvalValue::Null);
    };
    graph_node_field_value("bound node", node, field)
}

fn eval_bound_graph_edge_field(
    slot: GraphBindingSlotRef,
    field: GraphEdgeField,
    context: &BoundGraphEvalContext<'_>,
) -> Result<GraphEvalValue, EngineError> {
    let Some(edge) = context.row.edge_for_slot(slot)? else {
        return Ok(GraphEvalValue::Null);
    };
    graph_edge_field_value("bound edge", edge, field)
}

fn eval_bound_graph_path_field(
    slot: GraphBindingSlotRef,
    field: GraphPathField,
    context: &BoundGraphEvalContext<'_>,
) -> Result<GraphEvalValue, EngineError> {
    let Some(path) = context.row.path_for_slot(slot)? else {
        return Ok(GraphEvalValue::Null);
    };
    Ok(graph_path_field_value(path, field))
}

fn eval_bound_graph_function(
    name: GraphFunction,
    args: &[BoundGraphExpr],
    context: &BoundGraphEvalContext<'_>,
) -> Result<GraphEvalValue, EngineError> {
    if args.len() != 1 {
        return Err(EngineError::InvalidOperation(format!(
            "graph row function {} expects exactly one argument",
            graph_function_name(name)
        )));
    }

    if let BoundGraphExpr::Binding(slot) = &args[0] {
        return eval_bound_graph_slot_function(name, *slot, context);
    }

    if let Some(value) = eval_bound_path_derived_function(name, &args[0], context)? {
        return Ok(value);
    }

    let value = eval_bound_graph_expr(&args[0], context)?;
    eval_graph_function_value(name, value)
}

fn eval_bound_path_derived_function(
    name: GraphFunction,
    arg: &BoundGraphExpr,
    context: &BoundGraphEvalContext<'_>,
) -> Result<Option<GraphEvalValue>, EngineError> {
    let GraphFunction::Labels = name else {
        return Ok(None);
    };
    let Some((endpoint, path_slot)) = bound_path_endpoint_arg(arg) else {
        return Ok(None);
    };
    let Some(path) = context.row.path_for_slot(path_slot)? else {
        return Ok(Some(GraphEvalValue::Null));
    };
    let node = match endpoint {
        PathEndpoint::Start => path
            .nodes
            .first()
            .ok_or_else(|| invalid_path_shape("start_node"))?,
        PathEndpoint::End => path
            .nodes
            .last()
            .ok_or_else(|| invalid_path_shape("end_node"))?,
    };
    graph_node_field_value(
        "path-derived function argument",
        node,
        GraphNodeField::Labels,
    )
    .map(Some)
}

fn bound_path_endpoint_arg(expr: &BoundGraphExpr) -> Option<(PathEndpoint, GraphBindingSlotRef)> {
    match expr {
        BoundGraphExpr::Function { name, args } => {
            let endpoint = match name {
                GraphFunction::StartNode => PathEndpoint::Start,
                GraphFunction::EndNode => PathEndpoint::End,
                _ => return None,
            };
            match args.as_slice() {
                [BoundGraphExpr::Binding(slot)] if slot.kind == GraphBindingSlotKind::Path => {
                    Some((endpoint, *slot))
                }
                _ => None,
            }
        }
        _ => None,
    }
}

fn eval_bound_graph_slot_function(
    name: GraphFunction,
    slot: GraphBindingSlotRef,
    context: &BoundGraphEvalContext<'_>,
) -> Result<GraphEvalValue, EngineError> {
    match (name, slot.kind) {
        (GraphFunction::Id, GraphBindingSlotKind::Node) => {
            let Some(node) = context.row.node_for_slot(slot)? else {
                return Ok(GraphEvalValue::Null);
            };
            Ok(GraphEvalValue::UInt(node.id))
        }
        (GraphFunction::Id, GraphBindingSlotKind::Edge) => {
            let Some(edge) = context.row.edge_for_slot(slot)? else {
                return Ok(GraphEvalValue::Null);
            };
            Ok(GraphEvalValue::UInt(edge.id))
        }
        (GraphFunction::Labels, GraphBindingSlotKind::Node) => {
            let Some(node) = context.row.node_for_slot(slot)? else {
                return Ok(GraphEvalValue::Null);
            };
            graph_node_field_value("function argument", node, GraphNodeField::Labels)
        }
        (GraphFunction::Type, GraphBindingSlotKind::Edge) => {
            let Some(edge) = context.row.edge_for_slot(slot)? else {
                return Ok(GraphEvalValue::Null);
            };
            graph_edge_field_value("function argument", edge, GraphEdgeField::Label)
        }
        (GraphFunction::Length, GraphBindingSlotKind::Path) => {
            let Some(path) = context.row.path_for_slot(slot)? else {
                return Ok(GraphEvalValue::Null);
            };
            Ok(GraphEvalValue::UInt(path.path.edges.len() as u64))
        }
        (GraphFunction::StartNode, GraphBindingSlotKind::Path) => {
            let Some(path) = context.row.path_for_slot(slot)? else {
                return Ok(GraphEvalValue::Null);
            };
            path.path
                .nodes
                .first()
                .copied()
                .map(GraphBoundNode::id_only)
                .map(GraphEvalValue::Node)
                .ok_or_else(|| invalid_path_shape("start_node"))
        }
        (GraphFunction::EndNode, GraphBindingSlotKind::Path) => {
            let Some(path) = context.row.path_for_slot(slot)? else {
                return Ok(GraphEvalValue::Null);
            };
            path.path
                .nodes
                .last()
                .copied()
                .map(GraphBoundNode::id_only)
                .map(GraphEvalValue::Node)
                .ok_or_else(|| invalid_path_shape("end_node"))
        }
        (GraphFunction::Nodes, GraphBindingSlotKind::Path) => {
            let Some(path) = context.row.path_for_slot(slot)? else {
                return Ok(GraphEvalValue::Null);
            };
            Ok(GraphEvalValue::List(
                path.path
                    .nodes
                    .iter()
                    .copied()
                    .map(GraphBoundNode::id_only)
                    .map(GraphEvalValue::Node)
                    .collect(),
            ))
        }
        (GraphFunction::Relationships, GraphBindingSlotKind::Path) => {
            let Some(path) = context.row.path_for_slot(slot)? else {
                return Ok(GraphEvalValue::Null);
            };
            Ok(GraphEvalValue::List(
                path.path
                    .edges
                    .iter()
                    .copied()
                    .map(GraphBoundEdge::id_only)
                    .map(GraphEvalValue::Edge)
                    .collect(),
            ))
        }
        (GraphFunction::Id, _) => Err(function_input_error(name, "a node or edge")),
        (GraphFunction::Labels, _) => Err(function_input_error(name, "a node")),
        (GraphFunction::Type, _) => Err(function_input_error(name, "an edge")),
        (
            GraphFunction::Length
            | GraphFunction::StartNode
            | GraphFunction::EndNode
            | GraphFunction::Nodes
            | GraphFunction::Relationships,
            _,
        ) => Err(function_input_error(name, "a path")),
    }
}

fn eval_graph_function_value(
    name: GraphFunction,
    value: GraphEvalValue,
) -> Result<GraphEvalValue, EngineError> {
    if value.is_null() {
        return Ok(GraphEvalValue::Null);
    }
    match (name, value) {
        (GraphFunction::Id, GraphEvalValue::Node(node)) => Ok(GraphEvalValue::UInt(node.id)),
        (GraphFunction::Id, GraphEvalValue::Edge(edge)) => Ok(GraphEvalValue::UInt(edge.id)),
        (GraphFunction::Labels, GraphEvalValue::Node(node)) => {
            graph_node_field_value("function argument", &node, GraphNodeField::Labels)
        }
        (GraphFunction::Type, GraphEvalValue::Edge(edge)) => {
            graph_edge_field_value("function argument", &edge, GraphEdgeField::Label)
        }
        (GraphFunction::Length, GraphEvalValue::Path(path)) => {
            Ok(GraphEvalValue::UInt(path.path.edges.len() as u64))
        }
        (GraphFunction::StartNode, GraphEvalValue::Path(path)) => path
            .path
            .nodes
            .first()
            .copied()
            .map(GraphBoundNode::id_only)
            .map(GraphEvalValue::Node)
            .ok_or_else(|| invalid_path_shape("start_node")),
        (GraphFunction::EndNode, GraphEvalValue::Path(path)) => path
            .path
            .nodes
            .last()
            .copied()
            .map(GraphBoundNode::id_only)
            .map(GraphEvalValue::Node)
            .ok_or_else(|| invalid_path_shape("end_node")),
        (GraphFunction::Nodes, GraphEvalValue::Path(path)) => Ok(GraphEvalValue::List(
            path.path
                .nodes
                .iter()
                .copied()
                .map(GraphBoundNode::id_only)
                .map(GraphEvalValue::Node)
                .collect(),
        )),
        (GraphFunction::Relationships, GraphEvalValue::Path(path)) => Ok(GraphEvalValue::List(
            path.path
                .edges
                .iter()
                .copied()
                .map(GraphBoundEdge::id_only)
                .map(GraphEvalValue::Edge)
                .collect(),
        )),
        (GraphFunction::Id, _) => Err(function_input_error(name, "a node or edge")),
        (GraphFunction::Labels, _) => Err(function_input_error(name, "a node")),
        (GraphFunction::Type, _) => Err(function_input_error(name, "an edge")),
        (
            GraphFunction::Length
            | GraphFunction::StartNode
            | GraphFunction::EndNode
            | GraphFunction::Nodes
            | GraphFunction::Relationships,
            _,
        ) => Err(function_input_error(name, "a path")),
    }
}

fn eval_bound_graph_binary(
    left: &BoundGraphExpr,
    op: GraphBinaryOp,
    right: &BoundGraphExpr,
    context: &BoundGraphEvalContext<'_>,
) -> Result<GraphEvalValue, EngineError> {
    match op {
        GraphBinaryOp::And => eval_bound_graph_and(left, right, context),
        GraphBinaryOp::Or => eval_bound_graph_or(left, right, context),
        GraphBinaryOp::Eq
        | GraphBinaryOp::Neq
        | GraphBinaryOp::Lt
        | GraphBinaryOp::Le
        | GraphBinaryOp::Gt
        | GraphBinaryOp::Ge => {
            let left_value = eval_bound_graph_expr(left, context)?;
            let right_value = eval_bound_graph_expr(right, context)?;
            compare_graph_binary_values(op, &left_value, &right_value)
        }
        GraphBinaryOp::In => {
            let left_value = eval_bound_graph_expr(left, context)?;
            let right_value = eval_bound_graph_expr(right, context)?;
            eval_graph_in(&left_value, &right_value)
        }
    }
}

fn eval_bound_graph_and(
    left: &BoundGraphExpr,
    right: &BoundGraphExpr,
    context: &BoundGraphEvalContext<'_>,
) -> Result<GraphEvalValue, EngineError> {
    let left_value = bool_or_null(&eval_bound_graph_expr(left, context)?)?;
    if left_value == Some(false) {
        return Ok(GraphEvalValue::Bool(false));
    }
    let right_value = bool_or_null(&eval_bound_graph_expr(right, context)?)?;
    Ok(graph_and_truth_value(left_value, right_value))
}

fn eval_bound_graph_or(
    left: &BoundGraphExpr,
    right: &BoundGraphExpr,
    context: &BoundGraphEvalContext<'_>,
) -> Result<GraphEvalValue, EngineError> {
    let left_value = bool_or_null(&eval_bound_graph_expr(left, context)?)?;
    if left_value == Some(true) {
        return Ok(GraphEvalValue::Bool(true));
    }
    let right_value = bool_or_null(&eval_bound_graph_expr(right, context)?)?;
    Ok(graph_or_truth_value(left_value, right_value))
}

fn eval_graph_property(
    alias: &str,
    key: &str,
    context: &GraphEvalContext<'_>,
) -> Result<GraphEvalValue, EngineError> {
    match context.row.value_for_alias(context.schema, alias)? {
        GraphEvalValue::Null => Ok(GraphEvalValue::Null),
        GraphEvalValue::Node(node) => {
            let props = node
                .element
                .as_ref()
                .ok_or_else(|| unloaded_node_field_error(alias, "props"))?
                .props
                .as_ref()
                .ok_or_else(|| unloaded_node_field_error(alias, "props"))?;
            Ok(props
                .get(key)
                .cloned()
                .map(graph_value_to_eval)
                .transpose()?
                .unwrap_or(GraphEvalValue::Null))
        }
        GraphEvalValue::Edge(edge) => {
            let props = edge
                .element
                .as_ref()
                .ok_or_else(|| unloaded_edge_field_error(alias, "props"))?
                .props
                .as_ref()
                .ok_or_else(|| unloaded_edge_field_error(alias, "props"))?;
            Ok(props
                .get(key)
                .cloned()
                .map(graph_value_to_eval)
                .transpose()?
                .unwrap_or(GraphEvalValue::Null))
        }
        GraphEvalValue::Path(_) => Err(EngineError::InvalidOperation(format!(
            "graph row property expression cannot reference path alias '{alias}'"
        ))),
        _ => Err(EngineError::InvalidOperation(format!(
            "graph row property expression requires a node or edge alias, got '{alias}'"
        ))),
    }
}

fn eval_graph_node_field(
    alias: &str,
    field: GraphNodeField,
    context: &GraphEvalContext<'_>,
) -> Result<GraphEvalValue, EngineError> {
    match context.row.value_for_alias(context.schema, alias)? {
        GraphEvalValue::Null => Ok(GraphEvalValue::Null),
        GraphEvalValue::Node(node) => graph_node_field_value(alias, &node, field),
        _ => Err(EngineError::InvalidOperation(format!(
            "graph row node field references non-node alias '{alias}'"
        ))),
    }
}

fn eval_graph_edge_field(
    alias: &str,
    field: GraphEdgeField,
    context: &GraphEvalContext<'_>,
) -> Result<GraphEvalValue, EngineError> {
    match context.row.value_for_alias(context.schema, alias)? {
        GraphEvalValue::Null => Ok(GraphEvalValue::Null),
        GraphEvalValue::Edge(edge) => graph_edge_field_value(alias, &edge, field),
        _ => Err(EngineError::InvalidOperation(format!(
            "graph row edge field references non-edge alias '{alias}'"
        ))),
    }
}

fn eval_graph_path_field(
    alias: &str,
    field: GraphPathField,
    context: &GraphEvalContext<'_>,
) -> Result<GraphEvalValue, EngineError> {
    match context.row.value_for_alias(context.schema, alias)? {
        GraphEvalValue::Null => Ok(GraphEvalValue::Null),
        GraphEvalValue::Path(path) => Ok(graph_path_field_value(&path, field)),
        _ => Err(EngineError::InvalidOperation(format!(
            "graph row path field references non-path alias '{alias}'"
        ))),
    }
}

fn eval_graph_function(
    name: GraphFunction,
    args: &[GraphExpr],
    context: &GraphEvalContext<'_>,
) -> Result<GraphEvalValue, EngineError> {
    if args.len() != 1 {
        return Err(EngineError::InvalidOperation(format!(
            "graph row function {} expects exactly one argument",
            graph_function_name(name)
        )));
    }
    if let Some(value) = eval_graph_path_derived_function(name, &args[0], context)? {
        return Ok(value);
    }
    let value = eval_graph_expr(&args[0], context)?;
    eval_graph_function_value(name, value)
}

fn eval_graph_path_derived_function(
    name: GraphFunction,
    arg: &GraphExpr,
    context: &GraphEvalContext<'_>,
) -> Result<Option<GraphEvalValue>, EngineError> {
    let GraphFunction::Labels = name else {
        return Ok(None);
    };
    let Some((endpoint, alias)) = graph_path_endpoint_arg(arg) else {
        return Ok(None);
    };
    let Some(slot) = context.schema.slot_for_alias(alias) else {
        return Ok(None);
    };
    if slot.kind != GraphBindingSlotKind::Path {
        return Ok(None);
    }
    let Some(path) = context.row.path_for_slot(slot)? else {
        return Ok(Some(GraphEvalValue::Null));
    };
    let node = match endpoint {
        PathEndpoint::Start => path
            .nodes
            .first()
            .ok_or_else(|| invalid_path_shape("start_node"))?,
        PathEndpoint::End => path
            .nodes
            .last()
            .ok_or_else(|| invalid_path_shape("end_node"))?,
    };
    graph_node_field_value(
        "path-derived function argument",
        node,
        GraphNodeField::Labels,
    )
    .map(Some)
}

fn graph_path_endpoint_arg(expr: &GraphExpr) -> Option<(PathEndpoint, &str)> {
    match expr {
        GraphExpr::Function { name, args } => {
            let endpoint = match name {
                GraphFunction::StartNode => PathEndpoint::Start,
                GraphFunction::EndNode => PathEndpoint::End,
                _ => return None,
            };
            match args.as_slice() {
                [GraphExpr::Binding(alias)] => Some((endpoint, alias.as_str())),
                _ => None,
            }
        }
        _ => None,
    }
}

fn invalid_path_shape(function: &str) -> EngineError {
    EngineError::InvalidOperation(format!(
        "graph row function {function} received a path with no node ids"
    ))
}

fn function_input_error(name: GraphFunction, expected: &str) -> EngineError {
    EngineError::InvalidOperation(format!(
        "graph row function {} expects {}",
        graph_function_name(name),
        expected
    ))
}

fn eval_graph_binary(
    left: &GraphExpr,
    op: GraphBinaryOp,
    right: &GraphExpr,
    context: &GraphEvalContext<'_>,
) -> Result<GraphEvalValue, EngineError> {
    match op {
        GraphBinaryOp::And => eval_graph_and(left, right, context),
        GraphBinaryOp::Or => eval_graph_or(left, right, context),
        GraphBinaryOp::Eq
        | GraphBinaryOp::Neq
        | GraphBinaryOp::Lt
        | GraphBinaryOp::Le
        | GraphBinaryOp::Gt
        | GraphBinaryOp::Ge => {
            let left_value = eval_graph_expr(left, context)?;
            let right_value = eval_graph_expr(right, context)?;
            compare_graph_binary_values(op, &left_value, &right_value)
        }
        GraphBinaryOp::In => {
            let left_value = eval_graph_expr(left, context)?;
            let right_value = eval_graph_expr(right, context)?;
            eval_graph_in(&left_value, &right_value)
        }
    }
}

fn eval_graph_and(
    left: &GraphExpr,
    right: &GraphExpr,
    context: &GraphEvalContext<'_>,
) -> Result<GraphEvalValue, EngineError> {
    let left_value = bool_or_null(&eval_graph_expr(left, context)?)?;
    if left_value == Some(false) {
        return Ok(GraphEvalValue::Bool(false));
    }
    let right_value = bool_or_null(&eval_graph_expr(right, context)?)?;
    Ok(graph_and_truth_value(left_value, right_value))
}

fn eval_graph_or(
    left: &GraphExpr,
    right: &GraphExpr,
    context: &GraphEvalContext<'_>,
) -> Result<GraphEvalValue, EngineError> {
    let left_value = bool_or_null(&eval_graph_expr(left, context)?)?;
    if left_value == Some(true) {
        return Ok(GraphEvalValue::Bool(true));
    }
    let right_value = bool_or_null(&eval_graph_expr(right, context)?)?;
    Ok(graph_or_truth_value(left_value, right_value))
}

fn graph_and_truth_value(left: Option<bool>, right: Option<bool>) -> GraphEvalValue {
    match (left, right) {
        (_, Some(false)) => GraphEvalValue::Bool(false),
        (Some(true), Some(true)) => GraphEvalValue::Bool(true),
        _ => GraphEvalValue::Null,
    }
}

fn graph_or_truth_value(left: Option<bool>, right: Option<bool>) -> GraphEvalValue {
    match (left, right) {
        (_, Some(true)) => GraphEvalValue::Bool(true),
        (Some(false), Some(false)) => GraphEvalValue::Bool(false),
        _ => GraphEvalValue::Null,
    }
}

fn compare_graph_binary_values(
    op: GraphBinaryOp,
    left: &GraphEvalValue,
    right: &GraphEvalValue,
) -> Result<GraphEvalValue, EngineError> {
    if left.is_null() || right.is_null() {
        return Ok(GraphEvalValue::Null);
    }
    match op {
        GraphBinaryOp::Eq | GraphBinaryOp::Neq => {
            let equal = graph_values_equal(left, right)?;
            Ok(GraphEvalValue::Bool(if op == GraphBinaryOp::Eq {
                equal
            } else {
                !equal
            }))
        }
        GraphBinaryOp::Lt | GraphBinaryOp::Le | GraphBinaryOp::Gt | GraphBinaryOp::Ge => {
            let ordering = partial_cmp_graph_values(left, right)?.ok_or_else(|| {
                EngineError::InvalidOperation(
                    "graph row ordering comparison is not supported for these values".to_string(),
                )
            })?;
            Ok(GraphEvalValue::Bool(match op {
                GraphBinaryOp::Lt => ordering == Ordering::Less,
                GraphBinaryOp::Le => matches!(ordering, Ordering::Less | Ordering::Equal),
                GraphBinaryOp::Gt => ordering == Ordering::Greater,
                GraphBinaryOp::Ge => matches!(ordering, Ordering::Greater | Ordering::Equal),
                _ => unreachable!(),
            }))
        }
        GraphBinaryOp::And | GraphBinaryOp::Or | GraphBinaryOp::In => unreachable!(),
    }
}

fn eval_graph_in(
    left: &GraphEvalValue,
    right: &GraphEvalValue,
) -> Result<GraphEvalValue, EngineError> {
    if left.is_null() || right.is_null() {
        return Ok(GraphEvalValue::Null);
    }
    let GraphEvalValue::List(items) = right else {
        return Err(EngineError::InvalidOperation(
            "graph row IN requires a list right-hand operand".to_string(),
        ));
    };
    let mut saw_null = false;
    for item in items {
        if item.is_null() {
            saw_null = true;
        } else if graph_values_equal(left, item)? {
            return Ok(GraphEvalValue::Bool(true));
        }
    }
    Ok(if saw_null {
        GraphEvalValue::Null
    } else {
        GraphEvalValue::Bool(false)
    })
}

fn bool_or_null(value: &GraphEvalValue) -> Result<Option<bool>, EngineError> {
    match value {
        GraphEvalValue::Bool(value) => Ok(Some(*value)),
        GraphEvalValue::Null => Ok(None),
        _ => Err(EngineError::InvalidOperation(
            "graph row boolean operators require boolean or null operands".to_string(),
        )),
    }
}

fn graph_values_equal(left: &GraphEvalValue, right: &GraphEvalValue) -> Result<bool, EngineError> {
    if let Some(ordering) = partial_cmp_numeric_eval_values(left, right)? {
        return Ok(ordering == Ordering::Equal);
    }
    Ok(match (left, right) {
        (GraphEvalValue::Null, GraphEvalValue::Null) => true,
        (GraphEvalValue::Bool(left), GraphEvalValue::Bool(right)) => left == right,
        (GraphEvalValue::String(left), GraphEvalValue::String(right)) => left == right,
        (GraphEvalValue::Bytes(left), GraphEvalValue::Bytes(right)) => left == right,
        (GraphEvalValue::List(left), GraphEvalValue::List(right)) => {
            if left.len() != right.len() {
                return Ok(false);
            }
            for (left, right) in left.iter().zip(right) {
                if !graph_values_equal(left, right)? {
                    return Ok(false);
                }
            }
            true
        }
        (GraphEvalValue::Map(left), GraphEvalValue::Map(right)) => {
            if left.len() != right.len() {
                return Ok(false);
            }
            for (key, left_value) in left {
                let Some(right_value) = right.get(key) else {
                    return Ok(false);
                };
                if !graph_values_equal(left_value, right_value)? {
                    return Ok(false);
                }
            }
            true
        }
        (GraphEvalValue::Node(left), GraphEvalValue::Node(right)) => left.id == right.id,
        (GraphEvalValue::Edge(left), GraphEvalValue::Edge(right)) => left.id == right.id,
        (GraphEvalValue::Path(left), GraphEvalValue::Path(right)) => left.path == right.path,
        _ => false,
    })
}

fn partial_cmp_graph_values(
    left: &GraphEvalValue,
    right: &GraphEvalValue,
) -> Result<Option<Ordering>, EngineError> {
    if let Some(ordering) = partial_cmp_numeric_eval_values(left, right)? {
        return Ok(Some(ordering));
    }
    Ok(match (left, right) {
        (GraphEvalValue::Bool(left), GraphEvalValue::Bool(right)) => Some(left.cmp(right)),
        (GraphEvalValue::String(left), GraphEvalValue::String(right)) => Some(left.cmp(right)),
        (GraphEvalValue::Bytes(left), GraphEvalValue::Bytes(right)) => Some(left.cmp(right)),
        (GraphEvalValue::List(_), _)
        | (_, GraphEvalValue::List(_))
        | (GraphEvalValue::Map(_), _)
        | (_, GraphEvalValue::Map(_)) => {
            return Err(EngineError::InvalidOperation(
                "graph row list and map values are not orderable".to_string(),
            ));
        }
        (GraphEvalValue::Path(_), _) | (_, GraphEvalValue::Path(_)) => {
            return Err(EngineError::InvalidOperation(
                "graph row path values support equality but not arbitrary comparison".to_string(),
            ));
        }
        _ => None,
    })
}

fn partial_cmp_numeric_eval_values(
    left: &GraphEvalValue,
    right: &GraphEvalValue,
) -> Result<Option<Ordering>, EngineError> {
    match (
        numeric_key_for_eval_value(left)?,
        numeric_key_for_eval_value(right)?,
    ) {
        (Some(left), Some(right)) => Ok(Some(compare_numeric_keys(left, right))),
        _ => Ok(None),
    }
}

fn numeric_key_for_eval_value(
    value: &GraphEvalValue,
) -> Result<Option<NumericScalarKey>, EngineError> {
    Ok(match value {
        GraphEvalValue::Int(value) => Some(numeric_key_from_i64(*value)),
        GraphEvalValue::UInt(value) => Some(numeric_key_from_u64(*value)),
        GraphEvalValue::Float(value) => Some(numeric_key_from_f64(*value).ok_or_else(|| {
            EngineError::InvalidOperation(
                "graph row non-finite floats are not valid in comparison contexts".to_string(),
            )
        })?),
        _ => None,
    })
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) enum GraphSortAtom {
    Null,
    Bool(bool),
    Number(NumericRangeSortKey),
    String(Vec<u8>),
    Bytes(Vec<u8>),
    Node(u64),
    Edge(u64),
    Path {
        hop_count: usize,
        nodes: Vec<u64>,
        edges: Vec<u64>,
    },
}

pub(crate) fn graph_sort_atom_for_value(
    value: &GraphEvalValue,
) -> Result<GraphSortAtom, EngineError> {
    Ok(match value {
        GraphEvalValue::Null => GraphSortAtom::Null,
        GraphEvalValue::Bool(value) => GraphSortAtom::Bool(*value),
        GraphEvalValue::Int(value) => {
            GraphSortAtom::Number(numeric_range_sort_key(numeric_key_from_i64(*value)))
        }
        GraphEvalValue::UInt(value) => {
            GraphSortAtom::Number(numeric_range_sort_key(numeric_key_from_u64(*value)))
        }
        GraphEvalValue::Float(value) => GraphSortAtom::Number(numeric_range_sort_key(
            numeric_key_from_f64(*value).ok_or_else(|| {
                EngineError::InvalidOperation(
                    "graph row non-finite floats are not valid in order contexts".to_string(),
                )
            })?,
        )),
        GraphEvalValue::String(value) => GraphSortAtom::String(value.as_bytes().to_vec()),
        GraphEvalValue::Bytes(value) => GraphSortAtom::Bytes(value.clone()),
        GraphEvalValue::Node(node) => GraphSortAtom::Node(node.id),
        GraphEvalValue::Edge(edge) => GraphSortAtom::Edge(edge.id),
        GraphEvalValue::Path(path) => GraphSortAtom::Path {
            hop_count: path.path.edges.len(),
            nodes: path.path.nodes.clone(),
            edges: path.path.edges.clone(),
        },
        GraphEvalValue::List(_) | GraphEvalValue::Map(_) => {
            return Err(EngineError::InvalidOperation(
                "graph row list and map values are not orderable".to_string(),
            ));
        }
    })
}

pub(crate) fn compare_graph_sort_atoms(left: &GraphSortAtom, right: &GraphSortAtom) -> Ordering {
    match (left, right) {
        (GraphSortAtom::Null, GraphSortAtom::Null) => Ordering::Equal,
        (GraphSortAtom::Null, _) => Ordering::Greater,
        (_, GraphSortAtom::Null) => Ordering::Less,
        _ => graph_sort_atom_rank(left)
            .cmp(&graph_sort_atom_rank(right))
            .then_with(|| graph_sort_atom_payload_cmp(left, right)),
    }
}

fn graph_sort_atom_rank(value: &GraphSortAtom) -> u8 {
    match value {
        GraphSortAtom::Null => 255,
        GraphSortAtom::Bool(_) => 0,
        GraphSortAtom::Number(_) => 1,
        GraphSortAtom::String(_) => 2,
        GraphSortAtom::Bytes(_) => 3,
        GraphSortAtom::Node(_) => 4,
        GraphSortAtom::Edge(_) => 5,
        GraphSortAtom::Path { .. } => 6,
    }
}

fn graph_sort_atom_payload_cmp(left: &GraphSortAtom, right: &GraphSortAtom) -> Ordering {
    match (left, right) {
        (GraphSortAtom::Bool(left), GraphSortAtom::Bool(right)) => left.cmp(right),
        (GraphSortAtom::Number(left), GraphSortAtom::Number(right)) => left.cmp(right),
        (GraphSortAtom::String(left), GraphSortAtom::String(right)) => left.cmp(right),
        (GraphSortAtom::Bytes(left), GraphSortAtom::Bytes(right)) => left.cmp(right),
        (GraphSortAtom::Node(left), GraphSortAtom::Node(right)) => left.cmp(right),
        (GraphSortAtom::Edge(left), GraphSortAtom::Edge(right)) => left.cmp(right),
        (
            GraphSortAtom::Path {
                hop_count: left_hops,
                nodes: left_nodes,
                edges: left_edges,
            },
            GraphSortAtom::Path {
                hop_count: right_hops,
                nodes: right_nodes,
                edges: right_edges,
            },
        ) => left_hops
            .cmp(right_hops)
            .then_with(|| left_nodes.cmp(right_nodes))
            .then_with(|| left_edges.cmp(right_edges)),
        _ => Ordering::Equal,
    }
}

pub(crate) fn project_graph_row_values(
    schema: &GraphBindingSchema,
    row: &GraphBindingRow,
    return_items: &[GraphReturnItem],
    output: &GraphOutputOptions,
    params: &BTreeMap<String, GraphParamValue>,
) -> Result<Vec<GraphValue>, EngineError> {
    let bound_items = return_items
        .iter()
        .map(|item| {
            Ok(BoundGraphReturnItem {
                expr: bind_graph_expr_with_params(schema, &item.expr, Some(params))?,
                projection: item.projection.clone(),
            })
        })
        .collect::<Result<Vec<_>, EngineError>>()?;
    project_bound_graph_row_values(row, &bound_items, output)
}

pub(crate) fn project_bound_graph_row_values(
    row: &GraphBindingRow,
    return_items: &[BoundGraphReturnItem],
    output: &GraphOutputOptions,
) -> Result<Vec<GraphValue>, EngineError> {
    let context = BoundGraphEvalContext { row };
    let mut values = Vec::with_capacity(return_items.len());
    for item in return_items {
        values.push(bound_expr_to_output_value(
            &item.expr,
            &item.projection,
            output,
            &context,
        )?);
    }
    Ok(values)
}

fn bound_expr_to_output_value(
    expr: &BoundGraphExpr,
    projection: &GraphReturnProjection,
    output: &GraphOutputOptions,
    context: &BoundGraphEvalContext<'_>,
) -> Result<GraphValue, EngineError> {
    match expr {
        BoundGraphExpr::Function { name, args } if args.len() == 1 => {
            if let BoundGraphExpr::Binding(slot) = &args[0] {
                if slot.kind == GraphBindingSlotKind::Path {
                    return bound_path_function_to_output_value(
                        *name, *slot, projection, output, context,
                    );
                }
            }
        }
        BoundGraphExpr::List(items) => {
            return Ok(GraphValue::List(
                items
                    .iter()
                    .map(|item| bound_expr_to_output_value(item, projection, output, context))
                    .collect::<Result<Vec<_>, _>>()?,
            ));
        }
        BoundGraphExpr::Map(items) => {
            return Ok(GraphValue::Map(
                items
                    .iter()
                    .map(|(key, value)| {
                        Ok((
                            key.clone(),
                            bound_expr_to_output_value(value, projection, output, context)?,
                        ))
                    })
                    .collect::<Result<BTreeMap<_, _>, EngineError>>()?,
            ));
        }
        _ => {}
    }
    let value = eval_bound_graph_expr(expr, context)?;
    graph_eval_to_output_value(&value, projection, output)
}

fn bound_path_function_to_output_value(
    name: GraphFunction,
    slot: GraphBindingSlotRef,
    projection: &GraphReturnProjection,
    output: &GraphOutputOptions,
    context: &BoundGraphEvalContext<'_>,
) -> Result<GraphValue, EngineError> {
    let Some(path) = context.row.path_for_slot(slot)? else {
        return Ok(GraphValue::Null);
    };
    match name {
        GraphFunction::StartNode => path
            .nodes
            .first()
            .map(|node| graph_bound_node_to_output_value(node, projection, output))
            .transpose()?
            .ok_or_else(|| invalid_path_shape("start_node")),
        GraphFunction::EndNode => path
            .nodes
            .last()
            .map(|node| graph_bound_node_to_output_value(node, projection, output))
            .transpose()?
            .ok_or_else(|| invalid_path_shape("end_node")),
        GraphFunction::Nodes => Ok(GraphValue::List(
            path.nodes
                .iter()
                .map(|node| graph_bound_node_to_output_value(node, projection, output))
                .collect::<Result<Vec<_>, _>>()?,
        )),
        GraphFunction::Relationships => Ok(GraphValue::List(
            path.edges
                .iter()
                .map(|edge| graph_bound_edge_to_output_value(edge, projection, output))
                .collect::<Result<Vec<_>, _>>()?,
        )),
        GraphFunction::Length | GraphFunction::Id | GraphFunction::Labels | GraphFunction::Type => {
            let value = eval_bound_graph_slot_function(name, slot, context)?;
            graph_eval_to_output_value(&value, projection, output)
        }
    }
}

fn graph_bound_node_to_output_value(
    node: &GraphBoundNode,
    projection: &GraphReturnProjection,
    output: &GraphOutputOptions,
) -> Result<GraphValue, EngineError> {
    graph_eval_to_output_value(&GraphEvalValue::Node(node.clone()), projection, output)
}

fn graph_bound_edge_to_output_value(
    edge: &GraphBoundEdge,
    projection: &GraphReturnProjection,
    output: &GraphOutputOptions,
) -> Result<GraphValue, EngineError> {
    graph_eval_to_output_value(&GraphEvalValue::Edge(edge.clone()), projection, output)
}

fn graph_eval_to_output_value(
    value: &GraphEvalValue,
    projection: &GraphReturnProjection,
    output: &GraphOutputOptions,
) -> Result<GraphValue, EngineError> {
    if value.is_null() {
        return Ok(GraphValue::Null);
    }
    match projection {
        GraphReturnProjection::Auto => match output.mode {
            GraphOutputMode::Ids => eval_value_to_ids(value),
            GraphOutputMode::Elements => {
                eval_value_to_element(value, GraphElementProjection::Full, output.include_vectors)
            }
            GraphOutputMode::Projected => Err(EngineError::InvalidOperation(
                "graph row projected output mode requires selected return projections".to_string(),
            )),
        },
        GraphReturnProjection::IdOnly => eval_value_to_ids(value),
        GraphReturnProjection::Element(element) => {
            eval_value_to_element(value, element.clone(), output.include_vectors)
        }
        GraphReturnProjection::Selected(selected) => {
            eval_value_to_selected(value, selected, output.include_vectors)
        }
    }
}

fn eval_value_to_ids(value: &GraphEvalValue) -> Result<GraphValue, EngineError> {
    Ok(match value {
        GraphEvalValue::Null => GraphValue::Null,
        GraphEvalValue::Bool(value) => GraphValue::Bool(*value),
        GraphEvalValue::Int(value) => GraphValue::Int(*value),
        GraphEvalValue::UInt(value) => GraphValue::UInt(*value),
        GraphEvalValue::Float(value) => GraphValue::Float(*value),
        GraphEvalValue::String(value) => GraphValue::String(value.clone()),
        GraphEvalValue::Bytes(value) => GraphValue::Bytes(value.clone()),
        GraphEvalValue::List(values) => GraphValue::List(
            values
                .iter()
                .map(eval_value_to_ids)
                .collect::<Result<Vec<_>, _>>()?,
        ),
        GraphEvalValue::Map(values) => GraphValue::Map(
            values
                .iter()
                .map(|(key, value)| Ok((key.clone(), eval_value_to_ids(value)?)))
                .collect::<Result<BTreeMap<_, _>, EngineError>>()?,
        ),
        GraphEvalValue::Node(node) => GraphValue::NodeId(node.id),
        GraphEvalValue::Edge(edge) => GraphValue::EdgeId(edge.id),
        GraphEvalValue::Path(path) => GraphValue::Path(GraphPathValue {
            node_ids: path.path.nodes.clone(),
            edge_ids: path.path.edges.clone(),
            nodes: None,
            edges: None,
        }),
    })
}

fn eval_value_to_element(
    value: &GraphEvalValue,
    projection: GraphElementProjection,
    include_vectors: bool,
) -> Result<GraphValue, EngineError> {
    Ok(match value {
        GraphEvalValue::Node(node) => {
            GraphValue::Node(project_node_element(node, projection, include_vectors)?)
        }
        GraphEvalValue::Edge(edge) => GraphValue::Edge(project_edge_element(edge, projection)?),
        GraphEvalValue::Path(path) => {
            GraphValue::Path(project_path_element(path, projection, include_vectors)?)
        }
        GraphEvalValue::List(values) => GraphValue::List(
            values
                .iter()
                .map(|value| eval_value_to_element(value, projection.clone(), include_vectors))
                .collect::<Result<Vec<_>, _>>()?,
        ),
        GraphEvalValue::Map(values) => GraphValue::Map(
            values
                .iter()
                .map(|(key, value)| {
                    Ok((
                        key.clone(),
                        eval_value_to_element(value, projection.clone(), include_vectors)?,
                    ))
                })
                .collect::<Result<BTreeMap<_, _>, EngineError>>()?,
        ),
        _ => eval_value_to_ids(value)?,
    })
}

fn eval_value_to_selected(
    value: &GraphEvalValue,
    projection: &GraphSelectedProjection,
    include_vectors: bool,
) -> Result<GraphValue, EngineError> {
    if value.is_null() {
        return Ok(GraphValue::Null);
    }
    match (value, projection) {
        (GraphEvalValue::Node(node), GraphSelectedProjection::Node(selected)) => Ok(
            GraphValue::Node(project_node_selected(node, selected, include_vectors)?),
        ),
        (GraphEvalValue::Edge(edge), GraphSelectedProjection::Edge(selected)) => {
            Ok(GraphValue::Edge(project_edge_selected(edge, selected)?))
        }
        (GraphEvalValue::Path(path), GraphSelectedProjection::Path(selected)) => Ok(
            GraphValue::Path(project_path_selected(path, selected, include_vectors)?),
        ),
        (GraphEvalValue::List(values), _) => Ok(GraphValue::List(
            values
                .iter()
                .map(|value| eval_value_to_selected(value, projection, include_vectors))
                .collect::<Result<Vec<_>, _>>()?,
        )),
        (GraphEvalValue::Map(values), _) => Ok(GraphValue::Map(
            values
                .iter()
                .map(|(key, value)| {
                    Ok((
                        key.clone(),
                        eval_value_to_selected(value, projection, include_vectors)?,
                    ))
                })
                .collect::<Result<BTreeMap<_, _>, EngineError>>()?,
        )),
        _ => Err(EngineError::InvalidOperation(
            "graph row selected projection kind does not match expression value".to_string(),
        )),
    }
}

fn project_node_element(
    node: &GraphBoundNode,
    projection: GraphElementProjection,
    include_vectors: bool,
) -> Result<GraphNodeValue, EngineError> {
    match projection {
        GraphElementProjection::IdOnly => Ok(GraphNodeValue {
            id: Some(node.id),
            labels: None,
            key: None,
            props: None,
            weight: None,
            created_at: None,
            updated_at: None,
            dense_vector: None,
            sparse_vector: None,
        }),
        GraphElementProjection::Compact => {
            let element = loaded_node_element(node, "compact node element")?;
            Ok(GraphNodeValue {
                id: Some(node.id),
                labels: Some(required_loaded_node_value(
                    element.labels.clone(),
                    "compact node element",
                    "labels",
                )?),
                key: Some(required_loaded_node_value(
                    element.key.clone(),
                    "compact node element",
                    "key",
                )?),
                props: None,
                weight: None,
                created_at: None,
                updated_at: None,
                dense_vector: None,
                sparse_vector: None,
            })
        }
        GraphElementProjection::Full => {
            let element = loaded_node_element(node, "full node element")?;
            Ok(GraphNodeValue {
                id: Some(node.id),
                labels: Some(required_loaded_node_value(
                    element.labels.clone(),
                    "full node element",
                    "labels",
                )?),
                key: Some(required_loaded_node_value(
                    element.key.clone(),
                    "full node element",
                    "key",
                )?),
                props: Some(required_loaded_node_value(
                    element.props.clone(),
                    "full node element",
                    "props",
                )?),
                weight: Some(required_loaded_node_value(
                    element.weight,
                    "full node element",
                    "weight",
                )?),
                created_at: Some(required_loaded_node_value(
                    element.created_at,
                    "full node element",
                    "created_at",
                )?),
                updated_at: Some(required_loaded_node_value(
                    element.updated_at,
                    "full node element",
                    "updated_at",
                )?),
                dense_vector: include_vectors
                    .then(|| element.dense_vector.clone())
                    .flatten(),
                sparse_vector: include_vectors
                    .then(|| element.sparse_vector.clone())
                    .flatten(),
            })
        }
    }
}

fn project_edge_element(
    edge: &GraphBoundEdge,
    projection: GraphElementProjection,
) -> Result<GraphEdgeValue, EngineError> {
    match projection {
        GraphElementProjection::IdOnly => Ok(GraphEdgeValue {
            id: Some(edge.id),
            from: None,
            to: None,
            label: None,
            props: None,
            weight: None,
            created_at: None,
            updated_at: None,
            valid_from: None,
            valid_to: None,
        }),
        GraphElementProjection::Compact => {
            let element = loaded_edge_element(edge, "compact edge element")?;
            Ok(GraphEdgeValue {
                id: Some(edge.id),
                from: Some(required_loaded_edge_value(
                    element.from,
                    "compact edge element",
                    "from",
                )?),
                to: Some(required_loaded_edge_value(
                    element.to,
                    "compact edge element",
                    "to",
                )?),
                label: Some(required_loaded_edge_value(
                    element.label.clone(),
                    "compact edge element",
                    "label",
                )?),
                props: None,
                weight: None,
                created_at: None,
                updated_at: None,
                valid_from: None,
                valid_to: None,
            })
        }
        GraphElementProjection::Full => {
            let element = loaded_edge_element(edge, "full edge element")?;
            Ok(GraphEdgeValue {
                id: Some(edge.id),
                from: Some(required_loaded_edge_value(
                    element.from,
                    "full edge element",
                    "from",
                )?),
                to: Some(required_loaded_edge_value(
                    element.to,
                    "full edge element",
                    "to",
                )?),
                label: Some(required_loaded_edge_value(
                    element.label.clone(),
                    "full edge element",
                    "label",
                )?),
                props: Some(required_loaded_edge_value(
                    element.props.clone(),
                    "full edge element",
                    "props",
                )?),
                weight: Some(required_loaded_edge_value(
                    element.weight,
                    "full edge element",
                    "weight",
                )?),
                created_at: Some(required_loaded_edge_value(
                    element.created_at,
                    "full edge element",
                    "created_at",
                )?),
                updated_at: Some(required_loaded_edge_value(
                    element.updated_at,
                    "full edge element",
                    "updated_at",
                )?),
                valid_from: Some(required_loaded_edge_value(
                    element.valid_from,
                    "full edge element",
                    "valid_from",
                )?),
                valid_to: Some(required_loaded_edge_value(
                    element.valid_to,
                    "full edge element",
                    "valid_to",
                )?),
            })
        }
    }
}

fn project_path_element(
    path: &GraphBoundPath,
    projection: GraphElementProjection,
    include_vectors: bool,
) -> Result<GraphPathValue, EngineError> {
    if projection == GraphElementProjection::IdOnly {
        return Ok(GraphPathValue {
            node_ids: path.path.nodes.clone(),
            edge_ids: path.path.edges.clone(),
            nodes: None,
            edges: None,
        });
    }
    Ok(GraphPathValue {
        node_ids: path.path.nodes.clone(),
        edge_ids: path.path.edges.clone(),
        nodes: Some(
            path.nodes
                .iter()
                .map(|node| project_node_element(node, projection.clone(), include_vectors))
                .collect::<Result<Vec<_>, _>>()?,
        ),
        edges: Some(
            path.edges
                .iter()
                .map(|edge| project_edge_element(edge, projection.clone()))
                .collect::<Result<Vec<_>, _>>()?,
        ),
    })
}

fn project_node_selected(
    node: &GraphBoundNode,
    selected: &GraphSelectedNodeProjection,
    include_vectors: bool,
) -> Result<GraphNodeValue, EngineError> {
    let element = node.element.as_ref();
    Ok(GraphNodeValue {
        id: selected.id.then_some(node.id),
        labels: if selected.labels {
            Some(required_loaded_node_value(
                element.and_then(|value| value.labels.clone()),
                "selected node",
                "labels",
            )?)
        } else {
            None
        },
        key: if selected.key {
            Some(required_loaded_node_value(
                element.and_then(|value| value.key.clone()),
                "selected node",
                "key",
            )?)
        } else {
            None
        },
        props: selected_node_props(element, &selected.props)?,
        weight: if selected.weight {
            Some(required_loaded_node_value(
                element.and_then(|value| value.weight),
                "selected node",
                "weight",
            )?)
        } else {
            None
        },
        created_at: if selected.created_at {
            Some(required_loaded_node_value(
                element.and_then(|value| value.created_at),
                "selected node",
                "created_at",
            )?)
        } else {
            None
        },
        updated_at: if selected.updated_at {
            Some(required_loaded_node_value(
                element.and_then(|value| value.updated_at),
                "selected node",
                "updated_at",
            )?)
        } else {
            None
        },
        dense_vector: if include_vectors && selected.vectors.needs_dense() {
            loaded_node_element(node, "selected node")?
                .dense_vector
                .clone()
        } else {
            None
        },
        sparse_vector: if include_vectors && selected.vectors.needs_sparse() {
            loaded_node_element(node, "selected node")?
                .sparse_vector
                .clone()
        } else {
            None
        },
    })
}

fn project_edge_selected(
    edge: &GraphBoundEdge,
    selected: &GraphSelectedEdgeProjection,
) -> Result<GraphEdgeValue, EngineError> {
    let element = edge.element.as_ref();
    Ok(GraphEdgeValue {
        id: selected.id.then_some(edge.id),
        from: if selected.from {
            Some(required_loaded_edge_value(
                element.and_then(|value| value.from),
                "selected edge",
                "from",
            )?)
        } else {
            None
        },
        to: if selected.to {
            Some(required_loaded_edge_value(
                element.and_then(|value| value.to),
                "selected edge",
                "to",
            )?)
        } else {
            None
        },
        label: if selected.label {
            Some(required_loaded_edge_value(
                element.and_then(|value| value.label.clone()),
                "selected edge",
                "label",
            )?)
        } else {
            None
        },
        props: selected_edge_props(element, &selected.props)?,
        weight: if selected.weight {
            Some(required_loaded_edge_value(
                element.and_then(|value| value.weight),
                "selected edge",
                "weight",
            )?)
        } else {
            None
        },
        created_at: if selected.created_at {
            Some(required_loaded_edge_value(
                element.and_then(|value| value.created_at),
                "selected edge",
                "created_at",
            )?)
        } else {
            None
        },
        updated_at: if selected.updated_at {
            Some(required_loaded_edge_value(
                element.and_then(|value| value.updated_at),
                "selected edge",
                "updated_at",
            )?)
        } else {
            None
        },
        valid_from: if selected.valid_from {
            Some(required_loaded_edge_value(
                element.and_then(|value| value.valid_from),
                "selected edge",
                "valid_from",
            )?)
        } else {
            None
        },
        valid_to: if selected.valid_to {
            Some(required_loaded_edge_value(
                element.and_then(|value| value.valid_to),
                "selected edge",
                "valid_to",
            )?)
        } else {
            None
        },
    })
}

fn project_path_selected(
    path: &GraphBoundPath,
    selected: &GraphSelectedPathProjection,
    include_vectors: bool,
) -> Result<GraphPathValue, EngineError> {
    Ok(GraphPathValue {
        node_ids: if selected.node_ids {
            path.path.nodes.clone()
        } else {
            Vec::new()
        },
        edge_ids: if selected.edge_ids {
            path.path.edges.clone()
        } else {
            Vec::new()
        },
        nodes: selected
            .nodes
            .as_ref()
            .map(|node_selection| {
                path.nodes
                    .iter()
                    .map(|node| project_node_selected(node, node_selection, include_vectors))
                    .collect::<Result<Vec<_>, _>>()
            })
            .transpose()?,
        edges: selected
            .edges
            .as_ref()
            .map(|edge_selection| {
                path.edges
                    .iter()
                    .map(|edge| project_edge_selected(edge, edge_selection))
                    .collect::<Result<Vec<_>, _>>()
            })
            .transpose()?,
    })
}

fn selected_node_props(
    element: Option<&GraphNodeValue>,
    selection: &GraphPropertySelection,
) -> Result<Option<BTreeMap<String, GraphValue>>, EngineError> {
    selected_props(
        element.and_then(|value| value.props.as_ref()),
        selection,
        "selected node",
        unloaded_node_field_error,
    )
}

fn selected_edge_props(
    element: Option<&GraphEdgeValue>,
    selection: &GraphPropertySelection,
) -> Result<Option<BTreeMap<String, GraphValue>>, EngineError> {
    selected_props(
        element.and_then(|value| value.props.as_ref()),
        selection,
        "selected edge",
        unloaded_edge_field_error,
    )
}

fn selected_props(
    props: Option<&BTreeMap<String, GraphValue>>,
    selection: &GraphPropertySelection,
    context: &str,
    unloaded_error: fn(&str, &str) -> EngineError,
) -> Result<Option<BTreeMap<String, GraphValue>>, EngineError> {
    match selection {
        GraphPropertySelection::None => Ok(None),
        GraphPropertySelection::Keys(keys) => {
            let props = props.ok_or_else(|| unloaded_error(context, "props"))?;
            let mut selected = BTreeMap::new();
            for key in keys {
                if let Some(value) = props.get(key) {
                    selected.insert(key.clone(), value.clone());
                }
            }
            Ok(Some(selected))
        }
        GraphPropertySelection::All => Ok(Some(
            props
                .cloned()
                .ok_or_else(|| unloaded_error(context, "props"))?,
        )),
    }
}

pub(crate) fn collect_graph_row_projection_needs(
    schema: &GraphBindingSchema,
    nodes: &[(String, Option<NodeFilterExpr>)],
    pieces: &[GraphPatternPiece],
    where_: Option<&GraphExpr>,
    order_by: &[GraphOrderItem],
    return_items: &[GraphReturnItem],
    output: &GraphOutputOptions,
) -> Result<ProjectionNeeds, EngineError> {
    let mut needs = ProjectionNeeds::default();
    let mut verifier = EntityProjectionNeeds::default();
    for (alias, filter) in nodes {
        if let Some(filter) = filter {
            collect_node_filter_needs(alias, filter, &mut verifier)?;
        }
    }
    collect_piece_filter_needs(schema, pieces, &mut verifier)?;
    needs.merge_class_needs(ProjectionNeedClass::Verifier, &verifier)?;

    let mut residual = EntityProjectionNeeds::default();
    if let Some(expr) = where_ {
        collect_expr_projection_needs(expr, schema, &mut residual, ProjectionNeedClass::Residual)?;
    }
    collect_optional_where_needs(pieces, schema, &mut residual)?;
    needs.merge_class_needs(ProjectionNeedClass::Residual, &residual)?;

    let mut order = EntityProjectionNeeds::default();
    for item in order_by {
        collect_expr_projection_needs(&item.expr, schema, &mut order, ProjectionNeedClass::Order)?;
    }
    needs.merge_class_needs(ProjectionNeedClass::Order, &order)?;

    let mut output_needs = EntityProjectionNeeds::default();
    for item in return_items {
        collect_return_projection_needs(schema, item, output, &mut output_needs)?;
    }
    needs.merge_class_needs(ProjectionNeedClass::Output, &output_needs)?;
    needs.canonicalized()
}

pub(crate) fn collect_graph_expr_projection_needs(
    schema: &GraphBindingSchema,
    expr: &GraphExpr,
    need_class: ProjectionNeedClass,
) -> Result<EntityProjectionNeeds, EngineError> {
    let mut needs = EntityProjectionNeeds::default();
    collect_expr_projection_needs(expr, schema, &mut needs, need_class)?;
    needs.canonicalized_for(need_class)
}

fn collect_node_filter_needs(
    alias: &str,
    filter: &NodeFilterExpr,
    needs: &mut EntityProjectionNeeds,
) -> Result<(), EngineError> {
    match filter {
        NodeFilterExpr::PropertyEquals { key, .. }
        | NodeFilterExpr::PropertyIn { key, .. }
        | NodeFilterExpr::PropertyRange { key, .. }
        | NodeFilterExpr::PropertyExists { key }
        | NodeFilterExpr::PropertyMissing { key } => needs.merge_node_needs(
            alias,
            NodeSelectedFieldNeeds {
                props: PropertySelection::Keys(vec![key.clone()]),
                ..NodeSelectedFieldNeeds::default()
            },
            ProjectionNeedClass::Verifier,
        )?,
        NodeFilterExpr::UpdatedAtRange { .. } => needs.merge_node_needs(
            alias,
            NodeSelectedFieldNeeds::default(),
            ProjectionNeedClass::Verifier,
        )?,
        NodeFilterExpr::And(filters) | NodeFilterExpr::Or(filters) => {
            for filter in filters {
                collect_node_filter_needs(alias, filter, needs)?;
            }
        }
        NodeFilterExpr::Not(filter) => collect_node_filter_needs(alias, filter, needs)?,
    }
    Ok(())
}

fn collect_piece_filter_needs(
    schema: &GraphBindingSchema,
    pieces: &[GraphPatternPiece],
    needs: &mut EntityProjectionNeeds,
) -> Result<(), EngineError> {
    collect_piece_filter_needs_scoped(schema, pieces, needs, &mut 0)
}

fn collect_piece_filter_needs_scoped(
    schema: &GraphBindingSchema,
    pieces: &[GraphPatternPiece],
    needs: &mut EntityProjectionNeeds,
    next_hidden_id: &mut usize,
) -> Result<(), EngineError> {
    for piece in pieces {
        match piece {
            GraphPatternPiece::Edge(edge) => {
                if let Some(filter) = edge.filter.as_ref() {
                    if let Some(alias) = edge.alias.as_ref() {
                        collect_edge_filter_needs(alias, filter, needs)?;
                    } else {
                        validate_hidden_occurrence_slot(schema, *next_hidden_id)?;
                        collect_hidden_edge_filter_needs(*next_hidden_id, filter, needs)?;
                    }
                }
                if edge.alias.is_none() {
                    *next_hidden_id += 1;
                }
            }
            GraphPatternPiece::VariableLength(path) => {
                let hidden_index = (path.edge_alias.is_none() && path.path_alias.is_none())
                    .then_some(*next_hidden_id);
                if let Some(filter) = path.filter.as_ref() {
                    if let Some(alias) = path.edge_alias.as_ref() {
                        collect_edge_filter_needs(alias, filter, needs)?;
                    } else if let Some(alias) = path.path_alias.as_ref() {
                        collect_path_edge_filter_needs(alias, filter, needs)?;
                    } else if let Some(slot_index) = hidden_index {
                        validate_hidden_occurrence_slot(schema, slot_index)?;
                        collect_hidden_path_edge_filter_needs(slot_index, filter, needs)?;
                    }
                }
                if hidden_index.is_some() {
                    *next_hidden_id += 1;
                }
            }
            GraphPatternPiece::Optional(group) => {
                collect_piece_filter_needs_scoped(schema, &group.pieces, needs, next_hidden_id)?
            }
        }
    }
    Ok(())
}

fn collect_optional_where_needs(
    pieces: &[GraphPatternPiece],
    schema: &GraphBindingSchema,
    needs: &mut EntityProjectionNeeds,
) -> Result<(), EngineError> {
    for piece in pieces {
        if let GraphPatternPiece::Optional(group) = piece {
            collect_optional_where_needs(&group.pieces, schema, needs)?;
            if let Some(expr) = group.where_.as_ref() {
                collect_expr_projection_needs(expr, schema, needs, ProjectionNeedClass::Residual)?;
            }
        }
    }
    Ok(())
}

fn collect_edge_filter_needs(
    alias: &str,
    filter: &EdgeFilterExpr,
    needs: &mut EntityProjectionNeeds,
) -> Result<(), EngineError> {
    match filter {
        EdgeFilterExpr::PropertyEquals { key, .. }
        | EdgeFilterExpr::PropertyIn { key, .. }
        | EdgeFilterExpr::PropertyRange { key, .. }
        | EdgeFilterExpr::PropertyExists { key }
        | EdgeFilterExpr::PropertyMissing { key } => needs.merge_edge_needs(
            alias,
            EdgeSelectedFieldNeeds {
                props: PropertySelection::Keys(vec![key.clone()]),
                ..EdgeSelectedFieldNeeds::default()
            },
            ProjectionNeedClass::Verifier,
        )?,
        EdgeFilterExpr::WeightRange { .. }
        | EdgeFilterExpr::UpdatedAtRange { .. }
        | EdgeFilterExpr::ValidAt { .. }
        | EdgeFilterExpr::ValidFromRange { .. }
        | EdgeFilterExpr::ValidToRange { .. } => needs.merge_edge_needs(
            alias,
            EdgeSelectedFieldNeeds::default(),
            ProjectionNeedClass::Verifier,
        )?,
        EdgeFilterExpr::And(filters) | EdgeFilterExpr::Or(filters) => {
            for filter in filters {
                collect_edge_filter_needs(alias, filter, needs)?;
            }
        }
        EdgeFilterExpr::Not(filter) => collect_edge_filter_needs(alias, filter, needs)?,
    }
    Ok(())
}

fn collect_hidden_edge_filter_needs(
    slot_index: usize,
    filter: &EdgeFilterExpr,
    needs: &mut EntityProjectionNeeds,
) -> Result<(), EngineError> {
    needs.merge_hidden_edge_needs(
        slot_index,
        edge_filter_selected_needs(filter)?,
        ProjectionNeedClass::Verifier,
    )
}

fn collect_path_edge_filter_needs(
    alias: &str,
    filter: &EdgeFilterExpr,
    needs: &mut EntityProjectionNeeds,
) -> Result<(), EngineError> {
    needs.merge_path_needs(
        alias,
        PathSelectedFieldNeeds {
            edges: Some(edge_filter_selected_needs(filter)?),
            ..PathSelectedFieldNeeds::default()
        },
        ProjectionNeedClass::Verifier,
    )
}

fn collect_hidden_path_edge_filter_needs(
    slot_index: usize,
    filter: &EdgeFilterExpr,
    needs: &mut EntityProjectionNeeds,
) -> Result<(), EngineError> {
    needs.merge_hidden_path_needs(
        slot_index,
        PathSelectedFieldNeeds {
            edge_ids: true,
            edges: Some(edge_filter_selected_needs(filter)?),
            ..PathSelectedFieldNeeds::default()
        },
        ProjectionNeedClass::Verifier,
    )
}

fn validate_hidden_occurrence_slot(
    schema: &GraphBindingSchema,
    slot_index: usize,
) -> Result<(), EngineError> {
    let slot = GraphBindingSlotRef {
        kind: GraphBindingSlotKind::HiddenOccurrence,
        index: slot_index,
    };
    schema.slot(slot).ok_or_else(|| {
        EngineError::InvalidOperation(format!(
            "graph row hidden occurrence slot {slot_index} is missing from binding schema"
        ))
    })?;
    Ok(())
}

fn edge_filter_selected_needs(
    filter: &EdgeFilterExpr,
) -> Result<EdgeSelectedFieldNeeds, EngineError> {
    let mut needs = EntityProjectionNeeds::default();
    collect_edge_filter_needs("__edge_filter_needs", filter, &mut needs)?;
    Ok(needs
        .edges
        .remove("__edge_filter_needs")
        .unwrap_or_default())
}

fn collect_return_projection_needs(
    schema: &GraphBindingSchema,
    item: &GraphReturnItem,
    output: &GraphOutputOptions,
    needs: &mut EntityProjectionNeeds,
) -> Result<(), EngineError> {
    collect_expr_projection_needs(&item.expr, schema, needs, ProjectionNeedClass::Output)?;
    collect_expr_output_needs(schema, &item.expr, &item.projection, output, needs)
}

fn collect_expr_output_needs(
    schema: &GraphBindingSchema,
    expr: &GraphExpr,
    projection: &GraphReturnProjection,
    output: &GraphOutputOptions,
    needs: &mut EntityProjectionNeeds,
) -> Result<(), EngineError> {
    match expr {
        GraphExpr::Binding(alias) => {
            let slot = schema.slot_for_alias(alias).ok_or_else(|| {
                EngineError::InvalidOperation(format!(
                    "graph row return references unknown alias '{alias}'"
                ))
            })?;
            collect_binding_output_needs(alias, slot.kind, projection, output, needs)
        }
        GraphExpr::Function { name, args } => {
            collect_function_output_needs(schema, *name, args, projection, output, needs)
        }
        GraphExpr::List(items) => {
            for item in items {
                collect_expr_output_needs(schema, item, projection, output, needs)?;
            }
            Ok(())
        }
        GraphExpr::Map(items) => {
            for item in items.values() {
                collect_expr_output_needs(schema, item, projection, output, needs)?;
            }
            Ok(())
        }
        GraphExpr::Unary { .. }
        | GraphExpr::Binary { .. }
        | GraphExpr::IsNull(_)
        | GraphExpr::IsNotNull(_)
        | GraphExpr::Property { .. }
        | GraphExpr::NodeField { .. }
        | GraphExpr::EdgeField { .. }
        | GraphExpr::PathField { .. }
        | GraphExpr::Null
        | GraphExpr::Bool(_)
        | GraphExpr::Int(_)
        | GraphExpr::UInt(_)
        | GraphExpr::Float(_)
        | GraphExpr::String(_)
        | GraphExpr::Bytes(_)
        | GraphExpr::Param(_) => Ok(()),
    }
}

fn collect_function_output_needs(
    schema: &GraphBindingSchema,
    name: GraphFunction,
    args: &[GraphExpr],
    projection: &GraphReturnProjection,
    output: &GraphOutputOptions,
    needs: &mut EntityProjectionNeeds,
) -> Result<(), EngineError> {
    let Some(GraphExpr::Binding(alias)) = args.first() else {
        return Ok(());
    };
    let Some(slot) = schema.slot_for_alias(alias) else {
        return Ok(());
    };
    if slot.kind != GraphBindingSlotKind::Path {
        return Ok(());
    }
    match name {
        GraphFunction::StartNode => collect_path_endpoint_node_output_needs(
            alias,
            PathEndpoint::Start,
            projection,
            output,
            needs,
        ),
        GraphFunction::EndNode => collect_path_endpoint_node_output_needs(
            alias,
            PathEndpoint::End,
            projection,
            output,
            needs,
        ),
        GraphFunction::Nodes => {
            collect_path_node_list_output_needs(alias, projection, output, needs)
        }
        GraphFunction::Relationships => {
            collect_path_edge_output_needs(alias, projection, output, needs)
        }
        GraphFunction::Id | GraphFunction::Labels | GraphFunction::Type | GraphFunction::Length => {
            Ok(())
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum PathEndpoint {
    Start,
    End,
}

fn collect_path_endpoint_node_output_needs(
    alias: &str,
    endpoint: PathEndpoint,
    projection: &GraphReturnProjection,
    output: &GraphOutputOptions,
    needs: &mut EntityProjectionNeeds,
) -> Result<(), EngineError> {
    let Some(node_needs) = projected_path_node_source_needs(projection, output) else {
        return Ok(());
    };
    let (start_node, end_node) = match endpoint {
        PathEndpoint::Start => (Some(node_needs), None),
        PathEndpoint::End => (None, Some(node_needs)),
    };
    needs.merge_path_needs(
        alias,
        PathSelectedFieldNeeds {
            node_ids: true,
            start_node,
            end_node,
            ..PathSelectedFieldNeeds::default()
        },
        ProjectionNeedClass::Output,
    )
}

fn collect_path_node_list_output_needs(
    alias: &str,
    projection: &GraphReturnProjection,
    output: &GraphOutputOptions,
    needs: &mut EntityProjectionNeeds,
) -> Result<(), EngineError> {
    let Some(node_needs) = projected_path_node_source_needs(projection, output) else {
        return Ok(());
    };
    needs.merge_path_needs(
        alias,
        PathSelectedFieldNeeds {
            node_ids: true,
            nodes: Some(node_needs),
            ..PathSelectedFieldNeeds::default()
        },
        ProjectionNeedClass::Output,
    )
}

fn projected_path_node_source_needs(
    projection: &GraphReturnProjection,
    output: &GraphOutputOptions,
) -> Option<NodeSelectedFieldNeeds> {
    match projection {
        GraphReturnProjection::IdOnly => None,
        GraphReturnProjection::Auto => match output.mode {
            GraphOutputMode::Ids | GraphOutputMode::Projected => None,
            GraphOutputMode::Elements => {
                node_source_needs_from_element(GraphElementProjection::Full, output.include_vectors)
            }
        },
        GraphReturnProjection::Element(element) => {
            node_source_needs_from_element(element.clone(), output.include_vectors)
        }
        GraphReturnProjection::Selected(GraphSelectedProjection::Node(selected)) => {
            node_source_needs_from_selected(selected)
        }
        GraphReturnProjection::Selected(_) => None,
    }
}

fn collect_path_edge_output_needs(
    alias: &str,
    projection: &GraphReturnProjection,
    output: &GraphOutputOptions,
    needs: &mut EntityProjectionNeeds,
) -> Result<(), EngineError> {
    let edge_needs = match projection {
        GraphReturnProjection::IdOnly => return Ok(()),
        GraphReturnProjection::Auto => match output.mode {
            GraphOutputMode::Ids | GraphOutputMode::Projected => return Ok(()),
            GraphOutputMode::Elements => {
                edge_source_needs_from_element(GraphElementProjection::Full)
            }
        },
        GraphReturnProjection::Element(element) => edge_source_needs_from_element(element.clone()),
        GraphReturnProjection::Selected(GraphSelectedProjection::Edge(selected)) => {
            edge_source_needs_from_selected(selected)
        }
        GraphReturnProjection::Selected(_) => return Ok(()),
    };
    let Some(edge_needs) = edge_needs else {
        return Ok(());
    };
    needs.merge_path_needs(
        alias,
        PathSelectedFieldNeeds {
            edge_ids: true,
            edges: Some(edge_needs),
            ..PathSelectedFieldNeeds::default()
        },
        ProjectionNeedClass::Output,
    )
}

fn collect_binding_output_needs(
    alias: &str,
    kind: GraphBindingSlotKind,
    projection: &GraphReturnProjection,
    output: &GraphOutputOptions,
    needs: &mut EntityProjectionNeeds,
) -> Result<(), EngineError> {
    match projection {
        GraphReturnProjection::IdOnly => Ok(()),
        GraphReturnProjection::Auto => match output.mode {
            GraphOutputMode::Ids => Ok(()),
            GraphOutputMode::Projected => Ok(()),
            GraphOutputMode::Elements => collect_element_output_needs(
                alias,
                kind,
                GraphElementProjection::Full,
                output.include_vectors,
                needs,
            ),
        },
        GraphReturnProjection::Element(element) => collect_element_output_needs(
            alias,
            kind,
            element.clone(),
            output.include_vectors,
            needs,
        ),
        GraphReturnProjection::Selected(GraphSelectedProjection::Node(selected)) => {
            match node_source_needs_from_selected(selected) {
                Some(node_needs) => {
                    needs.merge_node_needs(alias, node_needs, ProjectionNeedClass::Output)
                }
                None => Ok(()),
            }
        }
        GraphReturnProjection::Selected(GraphSelectedProjection::Edge(selected)) => {
            match edge_source_needs_from_selected(selected) {
                Some(edge_needs) => {
                    needs.merge_edge_needs(alias, edge_needs, ProjectionNeedClass::Output)
                }
                None => Ok(()),
            }
        }
        GraphReturnProjection::Selected(GraphSelectedProjection::Path(selected)) => {
            match path_source_needs_from_selected(selected) {
                Some(path_needs) => {
                    needs.merge_path_needs(alias, path_needs, ProjectionNeedClass::Output)
                }
                None => Ok(()),
            }
        }
    }
}

fn collect_element_output_needs(
    alias: &str,
    kind: GraphBindingSlotKind,
    projection: GraphElementProjection,
    include_vectors: bool,
    needs: &mut EntityProjectionNeeds,
) -> Result<(), EngineError> {
    match kind {
        GraphBindingSlotKind::Node => {
            match node_source_needs_from_element(projection, include_vectors) {
                Some(node_needs) => {
                    needs.merge_node_needs(alias, node_needs, ProjectionNeedClass::Output)
                }
                None => Ok(()),
            }
        }
        GraphBindingSlotKind::Edge => match edge_source_needs_from_element(projection) {
            Some(edge_needs) => {
                needs.merge_edge_needs(alias, edge_needs, ProjectionNeedClass::Output)
            }
            None => Ok(()),
        },
        GraphBindingSlotKind::Path => {
            match path_source_needs_from_element(projection, include_vectors) {
                Some(path_needs) => {
                    needs.merge_path_needs(alias, path_needs, ProjectionNeedClass::Output)
                }
                None => Ok(()),
            }
        }
        GraphBindingSlotKind::Scalar | GraphBindingSlotKind::HiddenOccurrence => Ok(()),
    }
}

fn collect_expr_projection_needs(
    expr: &GraphExpr,
    schema: &GraphBindingSchema,
    needs: &mut EntityProjectionNeeds,
    need_class: ProjectionNeedClass,
) -> Result<(), EngineError> {
    match expr {
        GraphExpr::Property { alias, key } => {
            match schema.slot_for_alias(alias).map(|slot| slot.kind) {
                Some(GraphBindingSlotKind::Node) => needs.merge_node_needs(
                    alias,
                    NodeSelectedFieldNeeds {
                        props: PropertySelection::Keys(vec![key.clone()]),
                        ..NodeSelectedFieldNeeds::default()
                    },
                    need_class,
                )?,
                Some(GraphBindingSlotKind::Edge) => needs.merge_edge_needs(
                    alias,
                    EdgeSelectedFieldNeeds {
                        props: PropertySelection::Keys(vec![key.clone()]),
                        ..EdgeSelectedFieldNeeds::default()
                    },
                    need_class,
                )?,
                Some(GraphBindingSlotKind::Path) => {
                    return Err(EngineError::InvalidOperation(format!(
                        "graph row property expression cannot reference path alias '{alias}'"
                    )));
                }
                _ => {}
            }
        }
        GraphExpr::NodeField { alias, field } => {
            merge_node_field_need(alias, *field, needs, need_class)?
        }
        GraphExpr::EdgeField { alias, field } => {
            merge_edge_field_need(alias, *field, needs, need_class)?
        }
        GraphExpr::PathField { alias, field } => {
            merge_path_field_need(alias, *field, needs, need_class)?
        }
        GraphExpr::Function { name, args } => {
            for arg in args {
                collect_expr_projection_needs(arg, schema, needs, need_class)?;
            }
            collect_function_projection_needs(*name, args, needs, need_class)?;
        }
        GraphExpr::Unary { expr, .. } | GraphExpr::IsNull(expr) | GraphExpr::IsNotNull(expr) => {
            collect_expr_projection_needs(expr, schema, needs, need_class)?
        }
        GraphExpr::Binary { left, right, .. } => {
            collect_expr_projection_needs(left, schema, needs, need_class)?;
            collect_expr_projection_needs(right, schema, needs, need_class)?;
        }
        GraphExpr::List(items) => {
            for item in items {
                collect_expr_projection_needs(item, schema, needs, need_class)?;
            }
        }
        GraphExpr::Map(items) => {
            for item in items.values() {
                collect_expr_projection_needs(item, schema, needs, need_class)?;
            }
        }
        GraphExpr::Null
        | GraphExpr::Bool(_)
        | GraphExpr::Int(_)
        | GraphExpr::UInt(_)
        | GraphExpr::Float(_)
        | GraphExpr::String(_)
        | GraphExpr::Bytes(_)
        | GraphExpr::Param(_)
        | GraphExpr::Binding(_) => {}
    }
    Ok(())
}

fn merge_node_field_need(
    alias: &str,
    field: GraphNodeField,
    needs: &mut EntityProjectionNeeds,
    need_class: ProjectionNeedClass,
) -> Result<(), EngineError> {
    let Some(field_needs) = node_field_source_needs(field) else {
        return Ok(());
    };
    needs.merge_node_needs(alias, field_needs, need_class)
}

fn merge_edge_field_need(
    alias: &str,
    field: GraphEdgeField,
    needs: &mut EntityProjectionNeeds,
    need_class: ProjectionNeedClass,
) -> Result<(), EngineError> {
    let Some(field_needs) = edge_field_source_needs(field) else {
        return Ok(());
    };
    needs.merge_edge_needs(alias, field_needs, need_class)
}

fn collect_function_projection_needs(
    name: GraphFunction,
    args: &[GraphExpr],
    needs: &mut EntityProjectionNeeds,
    need_class: ProjectionNeedClass,
) -> Result<(), EngineError> {
    let Some(first_arg) = args.first() else {
        return Ok(());
    };
    if let GraphExpr::Binding(alias) = first_arg {
        match name {
            GraphFunction::Labels => {
                merge_node_field_need(alias, GraphNodeField::Labels, needs, need_class)?
            }
            GraphFunction::Type => {
                merge_edge_field_need(alias, GraphEdgeField::Label, needs, need_class)?
            }
            GraphFunction::StartNode => {
                merge_path_endpoint_node_need(alias, PathEndpoint::Start, None, needs, need_class)?
            }
            GraphFunction::EndNode => {
                merge_path_endpoint_node_need(alias, PathEndpoint::End, None, needs, need_class)?
            }
            GraphFunction::Nodes => merge_path_node_need(alias, None, needs, need_class)?,
            GraphFunction::Relationships => merge_path_edge_need(alias, None, needs, need_class)?,
            GraphFunction::Id | GraphFunction::Length => {}
        }
        return Ok(());
    }

    match name {
        GraphFunction::Labels => {
            if let Some((alias, endpoint)) = path_node_function_alias(first_arg) {
                merge_path_derived_node_need(
                    alias,
                    endpoint,
                    node_field_source_needs(GraphNodeField::Labels),
                    needs,
                    need_class,
                )?;
            }
        }
        GraphFunction::Type => {
            if let Some(alias) = path_edge_function_alias(first_arg) {
                merge_path_edge_need(
                    alias,
                    edge_field_source_needs(GraphEdgeField::Label),
                    needs,
                    need_class,
                )?;
            }
        }
        GraphFunction::Id
        | GraphFunction::Length
        | GraphFunction::StartNode
        | GraphFunction::EndNode
        | GraphFunction::Nodes
        | GraphFunction::Relationships => {}
    }
    Ok(())
}

fn path_node_function_alias(expr: &GraphExpr) -> Option<(&str, Option<PathEndpoint>)> {
    match expr {
        GraphExpr::Function { name, args } => match args.first() {
            Some(GraphExpr::Binding(alias)) => match name {
                GraphFunction::StartNode => Some((alias.as_str(), Some(PathEndpoint::Start))),
                GraphFunction::EndNode => Some((alias.as_str(), Some(PathEndpoint::End))),
                GraphFunction::Nodes => Some((alias.as_str(), None)),
                _ => None,
            },
            _ => None,
        },
        _ => None,
    }
}

fn path_edge_function_alias(expr: &GraphExpr) -> Option<&str> {
    match expr {
        GraphExpr::Function { name, args } if *name == GraphFunction::Relationships => {
            match args.first() {
                Some(GraphExpr::Binding(alias)) => Some(alias.as_str()),
                _ => None,
            }
        }
        _ => None,
    }
}

fn merge_path_node_need(
    alias: &str,
    node_needs: Option<NodeSelectedFieldNeeds>,
    needs: &mut EntityProjectionNeeds,
    need_class: ProjectionNeedClass,
) -> Result<(), EngineError> {
    needs.merge_path_needs(
        alias,
        PathSelectedFieldNeeds {
            node_ids: true,
            nodes: node_needs,
            ..PathSelectedFieldNeeds::default()
        },
        need_class,
    )
}

fn merge_path_derived_node_need(
    alias: &str,
    endpoint: Option<PathEndpoint>,
    node_needs: Option<NodeSelectedFieldNeeds>,
    needs: &mut EntityProjectionNeeds,
    need_class: ProjectionNeedClass,
) -> Result<(), EngineError> {
    match endpoint {
        Some(endpoint) => {
            merge_path_endpoint_node_need(alias, endpoint, node_needs, needs, need_class)
        }
        None => merge_path_node_need(alias, node_needs, needs, need_class),
    }
}

fn merge_path_endpoint_node_need(
    alias: &str,
    endpoint: PathEndpoint,
    node_needs: Option<NodeSelectedFieldNeeds>,
    needs: &mut EntityProjectionNeeds,
    need_class: ProjectionNeedClass,
) -> Result<(), EngineError> {
    let (start_node, end_node) = match endpoint {
        PathEndpoint::Start => (node_needs, None),
        PathEndpoint::End => (None, node_needs),
    };
    needs.merge_path_needs(
        alias,
        PathSelectedFieldNeeds {
            node_ids: true,
            start_node,
            end_node,
            ..PathSelectedFieldNeeds::default()
        },
        need_class,
    )
}

fn merge_path_edge_need(
    alias: &str,
    edge_needs: Option<EdgeSelectedFieldNeeds>,
    needs: &mut EntityProjectionNeeds,
    need_class: ProjectionNeedClass,
) -> Result<(), EngineError> {
    needs.merge_path_needs(
        alias,
        PathSelectedFieldNeeds {
            edge_ids: true,
            edges: edge_needs,
            ..PathSelectedFieldNeeds::default()
        },
        need_class,
    )
}

fn node_field_source_needs(field: GraphNodeField) -> Option<NodeSelectedFieldNeeds> {
    match field {
        GraphNodeField::Id => None,
        GraphNodeField::Key => Some(NodeSelectedFieldNeeds {
            key: true,
            ..NodeSelectedFieldNeeds::default()
        }),
        GraphNodeField::CreatedAt => Some(NodeSelectedFieldNeeds {
            created_at: true,
            ..NodeSelectedFieldNeeds::default()
        }),
        GraphNodeField::Labels | GraphNodeField::Weight | GraphNodeField::UpdatedAt => {
            Some(NodeSelectedFieldNeeds::default())
        }
    }
}

fn edge_field_source_needs(field: GraphEdgeField) -> Option<EdgeSelectedFieldNeeds> {
    match field {
        GraphEdgeField::Id => None,
        GraphEdgeField::CreatedAt => Some(EdgeSelectedFieldNeeds {
            created_at: true,
            ..EdgeSelectedFieldNeeds::default()
        }),
        GraphEdgeField::From
        | GraphEdgeField::To
        | GraphEdgeField::Label
        | GraphEdgeField::Weight
        | GraphEdgeField::UpdatedAt
        | GraphEdgeField::ValidFrom
        | GraphEdgeField::ValidTo => Some(EdgeSelectedFieldNeeds::default()),
    }
}

fn merge_path_field_need(
    alias: &str,
    field: GraphPathField,
    needs: &mut EntityProjectionNeeds,
    need_class: ProjectionNeedClass,
) -> Result<(), EngineError> {
    let path_needs = match field {
        GraphPathField::NodeIds => PathSelectedFieldNeeds {
            node_ids: true,
            ..PathSelectedFieldNeeds::default()
        },
        GraphPathField::EdgeIds => PathSelectedFieldNeeds {
            edge_ids: true,
            ..PathSelectedFieldNeeds::default()
        },
        GraphPathField::Length => return Ok(()),
    };
    needs.merge_path_needs(alias, path_needs, need_class)
}

fn node_needs_from_element(
    projection: GraphElementProjection,
    include_vectors: bool,
) -> NodeSelectedFieldNeeds {
    match projection {
        GraphElementProjection::IdOnly => NodeSelectedFieldNeeds::default(),
        GraphElementProjection::Compact => NodeSelectedFieldNeeds {
            key: true,
            ..NodeSelectedFieldNeeds::default()
        },
        GraphElementProjection::Full => NodeSelectedFieldNeeds {
            key: true,
            created_at: true,
            props: PropertySelection::All,
            vectors: if include_vectors {
                VectorSelection::Both
            } else {
                VectorSelection::None
            },
        },
    }
}

fn edge_needs_from_element(projection: GraphElementProjection) -> EdgeSelectedFieldNeeds {
    match projection {
        GraphElementProjection::IdOnly => EdgeSelectedFieldNeeds::default(),
        GraphElementProjection::Compact => EdgeSelectedFieldNeeds::default(),
        GraphElementProjection::Full => EdgeSelectedFieldNeeds {
            created_at: true,
            props: PropertySelection::All,
        },
    }
}

fn path_needs_from_element(
    projection: GraphElementProjection,
    include_vectors: bool,
) -> PathSelectedFieldNeeds {
    PathSelectedFieldNeeds {
        node_ids: true,
        edge_ids: true,
        nodes: Some(node_needs_from_element(projection.clone(), include_vectors)),
        edges: Some(edge_needs_from_element(projection)),
        ..PathSelectedFieldNeeds::default()
    }
}

fn node_source_needs_from_element(
    projection: GraphElementProjection,
    include_vectors: bool,
) -> Option<NodeSelectedFieldNeeds> {
    match projection {
        GraphElementProjection::IdOnly => None,
        GraphElementProjection::Compact | GraphElementProjection::Full => {
            Some(node_needs_from_element(projection, include_vectors))
        }
    }
}

fn edge_source_needs_from_element(
    projection: GraphElementProjection,
) -> Option<EdgeSelectedFieldNeeds> {
    match projection {
        GraphElementProjection::IdOnly => None,
        GraphElementProjection::Compact | GraphElementProjection::Full => {
            Some(edge_needs_from_element(projection))
        }
    }
}

fn path_source_needs_from_element(
    projection: GraphElementProjection,
    include_vectors: bool,
) -> Option<PathSelectedFieldNeeds> {
    match projection {
        GraphElementProjection::IdOnly => None,
        GraphElementProjection::Compact | GraphElementProjection::Full => {
            Some(path_needs_from_element(projection, include_vectors))
        }
    }
}

fn node_needs_from_selected(selected: &GraphSelectedNodeProjection) -> NodeSelectedFieldNeeds {
    NodeSelectedFieldNeeds {
        key: selected.key,
        created_at: selected.created_at,
        props: property_selection_from_graph(&selected.props),
        vectors: vector_selection_from_graph(selected.vectors),
    }
}

fn node_source_needs_from_selected(
    selected: &GraphSelectedNodeProjection,
) -> Option<NodeSelectedFieldNeeds> {
    if selected.labels
        || selected.key
        || selected.weight
        || selected.created_at
        || selected.updated_at
        || selected.props != GraphPropertySelection::None
        || selected.vectors != GraphVectorSelection::None
    {
        Some(node_needs_from_selected(selected))
    } else {
        None
    }
}

fn edge_needs_from_selected(selected: &GraphSelectedEdgeProjection) -> EdgeSelectedFieldNeeds {
    EdgeSelectedFieldNeeds {
        created_at: selected.created_at,
        props: property_selection_from_graph(&selected.props),
    }
}

fn edge_source_needs_from_selected(
    selected: &GraphSelectedEdgeProjection,
) -> Option<EdgeSelectedFieldNeeds> {
    if selected.from
        || selected.to
        || selected.label
        || selected.weight
        || selected.created_at
        || selected.updated_at
        || selected.valid_from
        || selected.valid_to
        || selected.props != GraphPropertySelection::None
    {
        Some(edge_needs_from_selected(selected))
    } else {
        None
    }
}

fn path_needs_from_selected(selected: &GraphSelectedPathProjection) -> PathSelectedFieldNeeds {
    PathSelectedFieldNeeds {
        node_ids: selected.node_ids,
        edge_ids: selected.edge_ids,
        nodes: selected.nodes.as_ref().map(node_needs_from_selected),
        edges: selected.edges.as_ref().map(edge_needs_from_selected),
        ..PathSelectedFieldNeeds::default()
    }
}

fn path_source_needs_from_selected(
    selected: &GraphSelectedPathProjection,
) -> Option<PathSelectedFieldNeeds> {
    let nodes = selected
        .nodes
        .as_ref()
        .and_then(node_source_needs_from_selected);
    let edges = selected
        .edges
        .as_ref()
        .and_then(edge_source_needs_from_selected);
    if nodes.is_none() && edges.is_none() {
        return None;
    }
    Some(PathSelectedFieldNeeds {
        node_ids: nodes.is_some(),
        edge_ids: edges.is_some(),
        nodes,
        edges,
        ..PathSelectedFieldNeeds::default()
    })
}

fn property_selection_from_graph(selection: &GraphPropertySelection) -> PropertySelection {
    match selection {
        GraphPropertySelection::None => PropertySelection::None,
        GraphPropertySelection::Keys(keys) => PropertySelection::Keys(keys.clone()),
        GraphPropertySelection::All => PropertySelection::All,
    }
}

fn vector_selection_from_graph(selection: GraphVectorSelection) -> VectorSelection {
    match selection {
        GraphVectorSelection::None => VectorSelection::None,
        GraphVectorSelection::Dense => VectorSelection::Dense,
        GraphVectorSelection::Sparse => VectorSelection::Sparse,
        GraphVectorSelection::Both => VectorSelection::Both,
    }
}

fn graph_param_to_eval(value: &GraphParamValue) -> Result<GraphEvalValue, EngineError> {
    Ok(match value {
        GraphParamValue::Null => GraphEvalValue::Null,
        GraphParamValue::Bool(value) => GraphEvalValue::Bool(*value),
        GraphParamValue::Int(value) => GraphEvalValue::Int(*value),
        GraphParamValue::UInt(value) => GraphEvalValue::UInt(*value),
        GraphParamValue::Float(value) => GraphEvalValue::Float(*value),
        GraphParamValue::String(value) => GraphEvalValue::String(value.clone()),
        GraphParamValue::Bytes(value) => GraphEvalValue::Bytes(value.clone()),
        GraphParamValue::List(values) => GraphEvalValue::List(
            values
                .iter()
                .map(graph_param_to_eval)
                .collect::<Result<Vec<_>, _>>()?,
        ),
        GraphParamValue::Map(values) => GraphEvalValue::Map(
            values
                .iter()
                .map(|(key, value)| Ok((key.clone(), graph_param_to_eval(value)?)))
                .collect::<Result<BTreeMap<_, _>, EngineError>>()?,
        ),
    })
}

fn graph_value_to_eval(value: GraphValue) -> Result<GraphEvalValue, EngineError> {
    Ok(match value {
        GraphValue::Null => GraphEvalValue::Null,
        GraphValue::Bool(value) => GraphEvalValue::Bool(value),
        GraphValue::Int(value) => GraphEvalValue::Int(value),
        GraphValue::UInt(value) => GraphEvalValue::UInt(value),
        GraphValue::Float(value) => GraphEvalValue::Float(value),
        GraphValue::String(value) => GraphEvalValue::String(value),
        GraphValue::Bytes(value) => GraphEvalValue::Bytes(value),
        GraphValue::List(values) => GraphEvalValue::List(
            values
                .into_iter()
                .map(graph_value_to_eval)
                .collect::<Result<Vec<_>, _>>()?,
        ),
        GraphValue::Map(values) => GraphEvalValue::Map(
            values
                .into_iter()
                .map(|(key, value)| Ok((key, graph_value_to_eval(value)?)))
                .collect::<Result<BTreeMap<_, _>, EngineError>>()?,
        ),
        GraphValue::NodeId(id) => GraphEvalValue::Node(GraphBoundNode::id_only(id)),
        GraphValue::EdgeId(id) => GraphEvalValue::Edge(GraphBoundEdge::id_only(id)),
        GraphValue::Node(node) => {
            let id = node.id.ok_or_else(|| {
                EngineError::InvalidOperation(
                    "graph row synthetic node values used in expressions must include id"
                        .to_string(),
                )
            })?;
            GraphEvalValue::Node(GraphBoundNode::with_element(id, node))
        }
        GraphValue::Edge(edge) => {
            let id = edge.id.ok_or_else(|| {
                EngineError::InvalidOperation(
                    "graph row synthetic edge values used in expressions must include id"
                        .to_string(),
                )
            })?;
            GraphEvalValue::Edge(GraphBoundEdge::with_element(id, edge))
        }
        GraphValue::Path(path) => GraphEvalValue::Path(GraphBoundPath::id_only(GraphPath {
            nodes: path.node_ids,
            edges: path.edge_ids,
        })?),
    })
}

fn graph_node_field_value(
    alias: &str,
    node: &GraphBoundNode,
    field: GraphNodeField,
) -> Result<GraphEvalValue, EngineError> {
    match field {
        GraphNodeField::Id => Ok(GraphEvalValue::UInt(node.id)),
        GraphNodeField::Labels => Ok(GraphEvalValue::List(
            required_loaded_node_value(
                node.element.as_ref().and_then(|value| value.labels.clone()),
                alias,
                "labels",
            )?
            .into_iter()
            .map(GraphEvalValue::String)
            .collect(),
        )),
        GraphNodeField::Key => Ok(GraphEvalValue::String(required_loaded_node_value(
            node.element.as_ref().and_then(|value| value.key.clone()),
            alias,
            "key",
        )?)),
        GraphNodeField::Weight => Ok(GraphEvalValue::Float(required_loaded_node_value(
            node.element.as_ref().and_then(|value| value.weight),
            alias,
            "weight",
        )? as f64)),
        GraphNodeField::CreatedAt => Ok(GraphEvalValue::Int(required_loaded_node_value(
            node.element.as_ref().and_then(|value| value.created_at),
            alias,
            "created_at",
        )?)),
        GraphNodeField::UpdatedAt => Ok(GraphEvalValue::Int(required_loaded_node_value(
            node.element.as_ref().and_then(|value| value.updated_at),
            alias,
            "updated_at",
        )?)),
    }
}

fn graph_edge_field_value(
    alias: &str,
    edge: &GraphBoundEdge,
    field: GraphEdgeField,
) -> Result<GraphEvalValue, EngineError> {
    match field {
        GraphEdgeField::Id => Ok(GraphEvalValue::UInt(edge.id)),
        GraphEdgeField::From => Ok(GraphEvalValue::UInt(required_loaded_edge_value(
            edge.element.as_ref().and_then(|value| value.from),
            alias,
            "from",
        )?)),
        GraphEdgeField::To => Ok(GraphEvalValue::UInt(required_loaded_edge_value(
            edge.element.as_ref().and_then(|value| value.to),
            alias,
            "to",
        )?)),
        GraphEdgeField::Label => Ok(GraphEvalValue::String(required_loaded_edge_value(
            edge.element.as_ref().and_then(|value| value.label.clone()),
            alias,
            "label",
        )?)),
        GraphEdgeField::Weight => Ok(GraphEvalValue::Float(required_loaded_edge_value(
            edge.element.as_ref().and_then(|value| value.weight),
            alias,
            "weight",
        )? as f64)),
        GraphEdgeField::CreatedAt => Ok(GraphEvalValue::Int(required_loaded_edge_value(
            edge.element.as_ref().and_then(|value| value.created_at),
            alias,
            "created_at",
        )?)),
        GraphEdgeField::UpdatedAt => Ok(GraphEvalValue::Int(required_loaded_edge_value(
            edge.element.as_ref().and_then(|value| value.updated_at),
            alias,
            "updated_at",
        )?)),
        GraphEdgeField::ValidFrom => Ok(GraphEvalValue::Int(required_loaded_edge_value(
            edge.element.as_ref().and_then(|value| value.valid_from),
            alias,
            "valid_from",
        )?)),
        GraphEdgeField::ValidTo => Ok(GraphEvalValue::Int(required_loaded_edge_value(
            edge.element.as_ref().and_then(|value| value.valid_to),
            alias,
            "valid_to",
        )?)),
    }
}

fn loaded_node_element<'a>(
    node: &'a GraphBoundNode,
    context: &str,
) -> Result<&'a GraphNodeValue, EngineError> {
    node.element
        .as_ref()
        .ok_or_else(|| unloaded_node_field_error(context, "element"))
}

fn loaded_edge_element<'a>(
    edge: &'a GraphBoundEdge,
    context: &str,
) -> Result<&'a GraphEdgeValue, EngineError> {
    edge.element
        .as_ref()
        .ok_or_else(|| unloaded_edge_field_error(context, "element"))
}

fn required_loaded_node_value<T>(
    value: Option<T>,
    context: &str,
    field: &str,
) -> Result<T, EngineError> {
    value.ok_or_else(|| unloaded_node_field_error(context, field))
}

fn required_loaded_edge_value<T>(
    value: Option<T>,
    context: &str,
    field: &str,
) -> Result<T, EngineError> {
    value.ok_or_else(|| unloaded_edge_field_error(context, field))
}

fn unloaded_node_field_error(context: &str, field: &str) -> EngineError {
    EngineError::InvalidOperation(format!(
        "graph row node value for {context} is missing loaded field '{field}'"
    ))
}

fn unloaded_edge_field_error(context: &str, field: &str) -> EngineError {
    EngineError::InvalidOperation(format!(
        "graph row edge value for {context} is missing loaded field '{field}'"
    ))
}

fn graph_path_field_value(path: &GraphBoundPath, field: GraphPathField) -> GraphEvalValue {
    match field {
        GraphPathField::NodeIds => GraphEvalValue::List(
            path.path
                .nodes
                .iter()
                .copied()
                .map(GraphEvalValue::UInt)
                .collect(),
        ),
        GraphPathField::EdgeIds => GraphEvalValue::List(
            path.path
                .edges
                .iter()
                .copied()
                .map(GraphEvalValue::UInt)
                .collect(),
        ),
        GraphPathField::Length => GraphEvalValue::UInt(path.path.edges.len() as u64),
    }
}

fn graph_function_name(name: GraphFunction) -> &'static str {
    match name {
        GraphFunction::Id => "id",
        GraphFunction::Labels => "labels",
        GraphFunction::Type => "type",
        GraphFunction::Length => "length",
        GraphFunction::StartNode => "start_node",
        GraphFunction::EndNode => "end_node",
        GraphFunction::Nodes => "nodes",
        GraphFunction::Relationships => "relationships",
    }
}

trait GraphVectorSelectionExt {
    fn needs_dense(self) -> bool;
    fn needs_sparse(self) -> bool;
}

impl GraphVectorSelectionExt for GraphVectorSelection {
    fn needs_dense(self) -> bool {
        matches!(
            self,
            GraphVectorSelection::Dense | GraphVectorSelection::Both
        )
    }

    fn needs_sparse(self) -> bool {
        matches!(
            self,
            GraphVectorSelection::Sparse | GraphVectorSelection::Both
        )
    }
}
