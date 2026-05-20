use crate::types::EdgeRecord;

pub(crate) const EDGE_WEIGHT_INDEX_LOGICAL_NAME: &str = "edge_weight_index";
pub(crate) const EDGE_UPDATED_AT_INDEX_LOGICAL_NAME: &str = "edge_updated_at_index";
pub(crate) const EDGE_VALID_FROM_INDEX_LOGICAL_NAME: &str = "edge_valid_from_index";
pub(crate) const EDGE_VALID_TO_INDEX_LOGICAL_NAME: &str = "edge_valid_to_index";

pub(crate) const EDGE_WEIGHT_INDEX_ENTRY_SIZE: usize = 16;
pub(crate) const EDGE_I64_METADATA_INDEX_ENTRY_SIZE: usize = 20;

pub(crate) type EdgeWeightIndexEntry = (u32, u32, u64);
pub(crate) type EdgeI64MetadataIndexEntry = (u32, i64, u64);

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct EdgeMetadataIndexEntries {
    pub(crate) weight: Vec<EdgeWeightIndexEntry>,
    pub(crate) updated_at: Vec<EdgeI64MetadataIndexEntry>,
    pub(crate) valid_from: Vec<EdgeI64MetadataIndexEntry>,
    pub(crate) valid_to: Vec<EdgeI64MetadataIndexEntry>,
}

impl EdgeMetadataIndexEntries {
    pub(crate) fn with_capacity(edge_count: usize) -> Self {
        Self {
            weight: Vec::with_capacity(edge_count),
            updated_at: Vec::with_capacity(edge_count),
            valid_from: Vec::with_capacity(edge_count),
            valid_to: Vec::with_capacity(edge_count),
        }
    }

    pub(crate) fn push(
        &mut self,
        label_id: u32,
        updated_at: i64,
        weight: f32,
        valid_from: i64,
        valid_to: i64,
        edge_id: u64,
    ) {
        if let Some(weight_key) = encode_edge_weight_key(weight) {
            self.weight.push((label_id, weight_key, edge_id));
        }
        self.updated_at.push((label_id, updated_at, edge_id));
        self.valid_from.push((label_id, valid_from, edge_id));
        self.valid_to.push((label_id, valid_to, edge_id));
    }

    pub(crate) fn sort_all(&mut self) {
        self.weight.sort_unstable();
        self.updated_at.sort_unstable();
        self.valid_from.sort_unstable();
        self.valid_to.sort_unstable();
    }
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub(crate) struct EdgeMetadataCandidate {
    pub(crate) edge_id: u64,
    pub(crate) from: u64,
    pub(crate) to: u64,
    pub(crate) label_id: u32,
    pub(crate) updated_at: i64,
    pub(crate) weight: f32,
    pub(crate) valid_from: i64,
    pub(crate) valid_to: i64,
}

impl EdgeMetadataCandidate {
    pub(crate) fn from_edge(edge: &EdgeRecord) -> Self {
        Self {
            edge_id: edge.id,
            from: edge.from,
            to: edge.to,
            label_id: edge.label_id,
            updated_at: edge.updated_at,
            weight: edge.weight,
            valid_from: edge.valid_from,
            valid_to: edge.valid_to,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct RangeBoundFlags<T> {
    pub(crate) lower: Option<T>,
    pub(crate) lower_inclusive: bool,
    pub(crate) upper: Option<T>,
    pub(crate) upper_inclusive: bool,
}

impl<T> RangeBoundFlags<T> {
    pub(crate) fn inclusive(lower: Option<T>, upper: Option<T>) -> Self {
        Self {
            lower,
            lower_inclusive: true,
            upper,
            upper_inclusive: true,
        }
    }
}

pub(crate) fn encode_edge_weight_key(weight: f32) -> Option<u32> {
    if weight.is_nan() {
        return None;
    }
    let normalized = if weight == 0.0 { 0.0 } else { weight };
    let bits = normalized.to_bits();
    Some(if bits & (1u32 << 31) != 0 {
        !bits
    } else {
        bits ^ (1u32 << 31)
    })
}

pub(crate) fn i64_matches_bounds(value: i64, bounds: RangeBoundFlags<i64>) -> bool {
    if let Some(lower) = bounds.lower {
        if bounds.lower_inclusive {
            if value < lower {
                return false;
            }
        } else if value <= lower {
            return false;
        }
    }
    if let Some(upper) = bounds.upper {
        if bounds.upper_inclusive {
            if value > upper {
                return false;
            }
        } else if value >= upper {
            return false;
        }
    }
    true
}

pub(crate) fn weight_matches_bounds(weight: f32, bounds: RangeBoundFlags<f32>) -> bool {
    if weight.is_nan() {
        return false;
    }
    if let Some(lower) = bounds.lower {
        if bounds.lower_inclusive {
            if weight < lower {
                return false;
            }
        } else if weight <= lower {
            return false;
        }
    }
    if let Some(upper) = bounds.upper {
        if bounds.upper_inclusive {
            if weight > upper {
                return false;
            }
        } else if weight >= upper {
            return false;
        }
    }
    true
}
