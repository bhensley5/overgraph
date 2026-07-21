//! Pure deterministic workload definitions shared by the Phase 44b benchmark
//! and its engine-test controls.
//!
//! This module intentionally has no OverGraph or Criterion imports. Callers map
//! the stable shapes below onto the public database types they exercise.

pub const WRITE_CALL_COUNT: usize = 1_000;
pub const WRITE_BATCH_SIZE: usize = 100;
pub const MUTATIONS_PER_CALL: usize = WRITE_BATCH_SIZE;
pub const TOTAL_MUTATION_COUNT: usize = WRITE_CALL_COUNT * MUTATIONS_PER_CALL;

pub const UPSERT_GROUP_COUNT: usize = TOTAL_MUTATION_COUNT;
pub const ENDPOINT_NODE_COUNT: usize = UPSERT_GROUP_COUNT + 1;
pub const TARGET_NODE_INDEX: usize = UPSERT_GROUP_COUNT;

pub const CHURN_GROUP_COUNT: usize = 1_024;
pub const CHURN_LABEL_COUNT: usize = 8;
#[allow(dead_code)]
pub const CHURN_OWNER_COUNT: usize = CHURN_GROUP_COUNT / CHURN_LABEL_COUNT;
pub const CHURN_TRANSIENTS_PER_CALL: usize = MUTATIONS_PER_CALL / 2;
pub const CHURN_SENTINEL_ORDINAL: u64 = u64::MAX;

pub const EDGE_PAYLOAD_KEY: &str = "phase44b_payload";
pub const EDGE_PAYLOAD_LEN: usize = 32;
pub const ENDPOINT_NODE_LABEL: &str = "Phase44bWriteEndpoint";
pub const ENDPOINT_NODE_KEY_PREFIX: &str = "phase44b-write-endpoint";
pub const UPSERT_LABEL: &str = "PHASE44B_UPSERT";
pub const CHURN_LABELS: [&str; CHURN_LABEL_COUNT] = [
    "PHASE44B_CHURN_0",
    "PHASE44B_CHURN_1",
    "PHASE44B_CHURN_2",
    "PHASE44B_CHURN_3",
    "PHASE44B_CHURN_4",
    "PHASE44B_CHURN_5",
    "PHASE44B_CHURN_6",
    "PHASE44B_CHURN_7",
];

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct EdgeShape {
    pub ordinal: u64,
    pub from_node_index: usize,
    pub to_node_index: usize,
    pub label_index: usize,
    pub payload_byte: u8,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ChurnBatchShape {
    pub delete_ordinals: Vec<u64>,
    pub insert_edges: Vec<EdgeShape>,
}

pub fn upsert_batch(call_index: usize) -> Vec<EdgeShape> {
    assert!(
        call_index < WRITE_CALL_COUNT,
        "write call index out of range"
    );
    let first = call_index
        .checked_mul(WRITE_BATCH_SIZE)
        .expect("upsert workload index overflow");
    (first..first + WRITE_BATCH_SIZE)
        .map(|ordinal| EdgeShape {
            ordinal: ordinal as u64,
            from_node_index: ordinal,
            to_node_index: TARGET_NODE_INDEX,
            label_index: 0,
            payload_byte: payload_byte(ordinal as u64),
        })
        .collect()
}

pub fn churn_sentinel_edge() -> EdgeShape {
    edge_shape_for_churn_ordinal(CHURN_SENTINEL_ORDINAL, 0)
}

pub fn churn_initial_transient_edges() -> Vec<EdgeShape> {
    (0..CHURN_TRANSIENTS_PER_CALL)
        .map(|ordinal| edge_shape_for_churn_ordinal(ordinal as u64, ordinal))
        .collect()
}

pub fn churn_batch(call_index: usize) -> ChurnBatchShape {
    assert!(
        call_index < WRITE_CALL_COUNT,
        "write call index out of range"
    );
    let delete_first = call_index
        .checked_mul(CHURN_TRANSIENTS_PER_CALL)
        .expect("churn delete index overflow");
    let insert_first = delete_first
        .checked_add(CHURN_TRANSIENTS_PER_CALL)
        .expect("churn insert index overflow");
    let delete_ordinals = (delete_first..delete_first + CHURN_TRANSIENTS_PER_CALL)
        .map(|ordinal| ordinal as u64)
        .collect::<Vec<_>>();
    let insert_edges = (insert_first..insert_first + CHURN_TRANSIENTS_PER_CALL)
        .map(|ordinal| edge_shape_for_churn_ordinal(ordinal as u64, ordinal))
        .collect::<Vec<_>>();
    assert_eq!(
        delete_ordinals.len() + insert_edges.len(),
        MUTATIONS_PER_CALL
    );
    ChurnBatchShape {
        delete_ordinals,
        insert_edges,
    }
}

fn edge_shape_for_churn_ordinal(ordinal: u64, group_ordinal: usize) -> EdgeShape {
    let group = group_ordinal % CHURN_GROUP_COUNT;
    EdgeShape {
        ordinal,
        from_node_index: group / CHURN_LABEL_COUNT,
        to_node_index: TARGET_NODE_INDEX,
        label_index: group % CHURN_LABEL_COUNT,
        payload_byte: payload_byte(ordinal),
    }
}

fn payload_byte(ordinal: u64) -> u8 {
    ordinal.wrapping_mul(37).wrapping_add(11).to_le_bytes()[0]
}

#[cfg(test)]
#[allow(dead_code, unused_imports)]
mod tests {
    use super::*;
    use std::collections::BTreeSet;

    #[test]
    fn locked_shapes_cover_exact_calls_mutations_and_groups() {
        let mut upsert_groups = BTreeSet::new();
        for call_index in 0..WRITE_CALL_COUNT {
            let batch = upsert_batch(call_index);
            assert_eq!(batch.len(), WRITE_BATCH_SIZE);
            for edge in batch {
                upsert_groups.insert((edge.from_node_index, edge.label_index));
            }
        }
        assert_eq!(upsert_groups.len(), UPSERT_GROUP_COUNT);

        let mut churn_groups = BTreeSet::new();
        let mut mutation_count = 0usize;
        for call_index in 0..WRITE_CALL_COUNT {
            let batch = churn_batch(call_index);
            mutation_count += batch.delete_ordinals.len() + batch.insert_edges.len();
            for edge in batch.insert_edges {
                churn_groups.insert((edge.from_node_index, edge.label_index));
            }
        }
        assert_eq!(mutation_count, TOTAL_MUTATION_COUNT);
        assert_eq!(churn_groups.len(), CHURN_GROUP_COUNT);
    }
}
