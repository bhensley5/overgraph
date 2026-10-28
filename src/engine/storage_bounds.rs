//! Logical record summaries used by read-only wire validation; no allocation inventory.
use crate::engine::bounds::{add, BoundError};
#[derive(Default, Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct RecordSummary {
    pub(crate) nodes: usize,
    pub(crate) edges: usize,
    pub(crate) node_labels: usize,
    pub(crate) key_bytes: usize,
    pub(crate) key_membership_bytes: usize,
    pub(crate) node_wire: usize,
    pub(crate) edge_wire: usize,
    pub(crate) properties: usize,
    pub(crate) property_values: usize,
    pub(crate) max_containers: usize,
    pub(crate) label_properties: usize,
    pub(crate) property_key_bytes: usize,
    pub(crate) label_property_key_bytes: usize,
    pub(crate) dense_points: usize,
    pub(crate) dense_values: usize,
    pub(crate) sparse_points: usize,
    pub(crate) sparse_entries: usize,
    pub(crate) max_record_wire: usize,
}
impl RecordSummary {
    pub(crate) fn checked_add(self, rhs: Self) -> Result<Self, BoundError> {
        Ok(Self {
            nodes: add(self.nodes, rhs.nodes)?,
            edges: add(self.edges, rhs.edges)?,
            node_labels: add(self.node_labels, rhs.node_labels)?,
            key_bytes: add(self.key_bytes, rhs.key_bytes)?,
            key_membership_bytes: add(self.key_membership_bytes, rhs.key_membership_bytes)?,
            node_wire: add(self.node_wire, rhs.node_wire)?,
            edge_wire: add(self.edge_wire, rhs.edge_wire)?,
            properties: add(self.properties, rhs.properties)?,
            property_values: add(self.property_values, rhs.property_values)?,
            max_containers: self.max_containers.max(rhs.max_containers),
            label_properties: add(self.label_properties, rhs.label_properties)?,
            property_key_bytes: add(self.property_key_bytes, rhs.property_key_bytes)?,
            label_property_key_bytes: add(
                self.label_property_key_bytes,
                rhs.label_property_key_bytes,
            )?,
            dense_points: add(self.dense_points, rhs.dense_points)?,
            dense_values: add(self.dense_values, rhs.dense_values)?,
            sparse_points: add(self.sparse_points, rhs.sparse_points)?,
            sparse_entries: add(self.sparse_entries, rhs.sparse_entries)?,
            max_record_wire: self.max_record_wire.max(rhs.max_record_wire),
        })
    }
}
