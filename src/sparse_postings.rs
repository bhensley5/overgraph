use crate::error::EngineError;
use crate::types::NodeIdMap;
use std::collections::BTreeMap;
use std::fs::File;
use std::io::{BufWriter, Write};
use std::path::Path;

pub const SPARSE_POSTING_INDEX_FILENAME: &str = "sparse_posting_index.dat";
pub const SPARSE_POSTINGS_FILENAME: &str = "sparse_postings.dat";

const SPARSE_POSTING_INDEX_ENTRY_SIZE: usize = 16; // dim_id(4) + offset(8) + count(4)
const SPARSE_POSTING_ENTRY_SIZE: usize = 12; // node_id(8) + weight(4)
const BATCH_RANDOM_ACCESS_PENALTY: usize = 4;

#[derive(Clone, Copy, PartialEq, Eq)]
enum SparsePostingLookupStrategy {
    SeekPerDimension,
    MergeWalk,
}

fn write_u32(w: &mut impl Write, value: u32) -> Result<(), EngineError> {
    w.write_all(&value.to_le_bytes())?;
    Ok(())
}

fn write_u64(w: &mut impl Write, value: u64) -> Result<(), EngineError> {
    w.write_all(&value.to_le_bytes())?;
    Ok(())
}

fn read_u32_at(data: &[u8], offset: usize) -> Result<u32, EngineError> {
    let end = offset
        .checked_add(4)
        .ok_or_else(|| EngineError::CorruptRecord("u32 offset overflow".into()))?;
    let bytes = data.get(offset..end).ok_or_else(|| {
        EngineError::CorruptRecord(format!(
            "u32 read at offset {} exceeds buffer length {}",
            offset,
            data.len()
        ))
    })?;
    Ok(u32::from_le_bytes(bytes.try_into().unwrap()))
}

fn read_u64_at(data: &[u8], offset: usize) -> Result<u64, EngineError> {
    let end = offset
        .checked_add(8)
        .ok_or_else(|| EngineError::CorruptRecord("u64 offset overflow".into()))?;
    let bytes = data.get(offset..end).ok_or_else(|| {
        EngineError::CorruptRecord(format!(
            "u64 read at offset {} exceeds buffer length {}",
            offset,
            data.len()
        ))
    })?;
    Ok(u64::from_le_bytes(bytes.try_into().unwrap()))
}

fn read_f32_at(data: &[u8], offset: usize) -> Result<f32, EngineError> {
    let end = offset
        .checked_add(4)
        .ok_or_else(|| EngineError::CorruptRecord("f32 offset overflow".into()))?;
    let bytes = data.get(offset..end).ok_or_else(|| {
        EngineError::CorruptRecord(format!(
            "f32 read at offset {} exceeds buffer length {}",
            offset,
            data.len()
        ))
    })?;
    Ok(f32::from_le_bytes(bytes.try_into().unwrap()))
}

fn sparse_posting_bytes(
    postings_data: &[u8],
    offset: usize,
    posting_count: usize,
) -> Result<&[u8], EngineError> {
    let bytes = posting_count
        .checked_mul(SPARSE_POSTING_ENTRY_SIZE)
        .ok_or_else(|| EngineError::CorruptRecord("sparse posting byte count overflow".into()))?;
    let end = offset
        .checked_add(bytes)
        .ok_or_else(|| EngineError::CorruptRecord("sparse posting range overflow".into()))?;
    postings_data.get(offset..end).ok_or_else(|| {
        EngineError::CorruptRecord(format!(
            "sparse posting range [{}, {}) exceeds buffer length {}",
            offset,
            end,
            postings_data.len()
        ))
    })
}

fn ceil_log2_usize(n: usize) -> usize {
    if n <= 1 {
        0
    } else {
        usize::BITS as usize - (n - 1).leading_zeros() as usize
    }
}

fn lower_bound_u32_index(
    index_data: &[u8],
    count: usize,
    target: u32,
) -> Result<usize, EngineError> {
    let mut lo = 0usize;
    let mut hi = count;
    while lo < hi {
        let mid = lo + (hi - lo) / 2;
        let off = 8 + mid * SPARSE_POSTING_INDEX_ENTRY_SIZE;
        let key = read_u32_at(index_data, off)?;
        if key < target {
            lo = mid + 1;
        } else {
            hi = mid;
        }
    }
    Ok(lo)
}

fn upper_bound_u32_index(
    index_data: &[u8],
    count: usize,
    target: u32,
) -> Result<usize, EngineError> {
    let mut lo = 0usize;
    let mut hi = count;
    while lo < hi {
        let mid = lo + (hi - lo) / 2;
        let off = 8 + mid * SPARSE_POSTING_INDEX_ENTRY_SIZE;
        let key = read_u32_at(index_data, off)?;
        if key <= target {
            lo = mid + 1;
        } else {
            hi = mid;
        }
    }
    Ok(lo)
}

fn choose_sparse_posting_lookup_strategy(
    index_data: &[u8],
    index_count: usize,
    unique_dims: usize,
    min_dim: u32,
    max_dim: u32,
) -> Result<(SparsePostingLookupStrategy, usize, usize), EngineError> {
    if unique_dims <= 2 || index_count <= 1 {
        return Ok((
            SparsePostingLookupStrategy::SeekPerDimension,
            0,
            index_count,
        ));
    }

    let span_start = lower_bound_u32_index(index_data, index_count, min_dim)?;
    let span_end = upper_bound_u32_index(index_data, index_count, max_dim)?;
    let span = span_end.saturating_sub(span_start).max(unique_dims);
    let seek_cost = unique_dims
        .saturating_mul(ceil_log2_usize(index_count))
        .saturating_mul(BATCH_RANDOM_ACCESS_PENALTY);

    let strategy = if seek_cost <= span {
        SparsePostingLookupStrategy::SeekPerDimension
    } else {
        SparsePostingLookupStrategy::MergeWalk
    };
    Ok((strategy, span_start, span_end))
}

fn find_dimension(
    index_data: &[u8],
    count: usize,
    dimension_id: u32,
) -> Result<Option<(u64, u32)>, EngineError> {
    let mut left = 0usize;
    let mut right = count;
    while left < right {
        let mid = left + (right - left) / 2;
        let base = 8 + mid * SPARSE_POSTING_INDEX_ENTRY_SIZE;
        let current_dimension = read_u32_at(index_data, base)?;
        match current_dimension.cmp(&dimension_id) {
            std::cmp::Ordering::Less => left = mid + 1,
            std::cmp::Ordering::Greater => right = mid,
            std::cmp::Ordering::Equal => {
                let offset = read_u64_at(index_data, base + 4)?;
                let posting_count = read_u32_at(index_data, base + 12)?;
                return Ok(Some((offset, posting_count)));
            }
        }
    }
    Ok(None)
}

pub(crate) fn write_sparse_posting_files(
    seg_dir: &Path,
    groups: &BTreeMap<u32, Vec<(u64, f32)>>,
) -> Result<(), EngineError> {
    if groups.is_empty() {
        return Ok(());
    }

    let index_file = File::create(seg_dir.join(SPARSE_POSTING_INDEX_FILENAME))?;
    let postings_file = File::create(seg_dir.join(SPARSE_POSTINGS_FILENAME))?;
    let mut index_w = BufWriter::new(index_file);
    let mut postings_w = BufWriter::new(postings_file);

    write_u64(&mut index_w, groups.len() as u64)?;

    let mut data_offset = 0u64;
    for (&dimension_id, postings) in groups {
        if postings.is_empty() {
            return Err(EngineError::CorruptRecord(format!(
                "sparse posting dimension {} has no postings",
                dimension_id
            )));
        }

        write_u32(&mut index_w, dimension_id)?;
        write_u64(&mut index_w, data_offset)?;
        write_u32(&mut index_w, postings.len() as u32)?;

        for &(node_id, weight) in postings {
            if !weight.is_finite() {
                return Err(EngineError::CorruptRecord(format!(
                    "sparse posting dimension {} for node {} has non-finite weight",
                    dimension_id, node_id
                )));
            }
            if weight < 0.0 {
                return Err(EngineError::CorruptRecord(format!(
                    "sparse posting dimension {} for node {} has negative weight",
                    dimension_id, node_id
                )));
            }
            write_u64(&mut postings_w, node_id)?;
            postings_w.write_all(&weight.to_le_bytes())?;
        }

        data_offset = data_offset
            .checked_add(postings.len() as u64 * SPARSE_POSTING_ENTRY_SIZE as u64)
            .ok_or_else(|| {
                EngineError::CorruptRecord("sparse posting data offset overflow".into())
            })?;
    }

    index_w.flush()?;
    index_w.get_ref().sync_all()?;
    postings_w.flush()?;
    postings_w.get_ref().sync_all()?;
    Ok(())
}

pub(crate) fn validate_sparse_posting_files(
    index_data: &[u8],
    postings_data: &[u8],
    sparse_vector_count: usize,
    require_for_sparse_vectors: bool,
) -> Result<(), EngineError> {
    if index_data.is_empty() {
        if !postings_data.is_empty() {
            return Err(EngineError::CorruptRecord(
                "sparse postings data exists without sparse posting index".into(),
            ));
        }
        if require_for_sparse_vectors && sparse_vector_count > 0 {
            return Err(EngineError::CorruptRecord(
                "segment has sparse vectors but sparse posting files are missing".into(),
            ));
        }
        return Ok(());
    }

    if postings_data.is_empty() {
        return Err(EngineError::CorruptRecord(
            "sparse posting index exists without sparse postings data".into(),
        ));
    }
    if sparse_vector_count == 0 {
        return Err(EngineError::CorruptRecord(
            "segment has sparse posting files but no sparse vectors".into(),
        ));
    }
    if index_data.len() < 8 {
        return Err(EngineError::CorruptRecord(format!(
            "sparse posting index too short: {} bytes",
            index_data.len()
        )));
    }

    let count = read_u64_at(index_data, 0)? as usize;
    let expected_len = 8usize
        .checked_add(count * SPARSE_POSTING_INDEX_ENTRY_SIZE)
        .ok_or_else(|| EngineError::CorruptRecord("sparse posting index size overflow".into()))?;
    if index_data.len() != expected_len {
        return Err(EngineError::CorruptRecord(format!(
            "sparse posting index size {} does not match expected {}",
            index_data.len(),
            expected_len
        )));
    }

    let mut prev_dimension = None;
    let mut next_offset = 0usize;
    for entry_index in 0..count {
        let base = 8 + entry_index * SPARSE_POSTING_INDEX_ENTRY_SIZE;
        let dimension_id = read_u32_at(index_data, base)?;
        let offset = read_u64_at(index_data, base + 4)? as usize;
        let posting_count = read_u32_at(index_data, base + 12)? as usize;
        if posting_count == 0 {
            return Err(EngineError::CorruptRecord(format!(
                "sparse posting dimension {} has zero posting count",
                dimension_id
            )));
        }
        if prev_dimension.is_some_and(|prev| prev >= dimension_id) {
            return Err(EngineError::CorruptRecord(format!(
                "sparse posting dimensions are not strictly increasing at {}",
                dimension_id
            )));
        }
        if offset != next_offset {
            return Err(EngineError::CorruptRecord(format!(
                "sparse posting dimension {} offset {} does not match expected {}",
                dimension_id, offset, next_offset
            )));
        }

        let postings_len = posting_count
            .checked_mul(SPARSE_POSTING_ENTRY_SIZE)
            .ok_or_else(|| EngineError::CorruptRecord("sparse posting range overflow".into()))?;
        let end = offset
            .checked_add(postings_len)
            .ok_or_else(|| EngineError::CorruptRecord("sparse posting end overflow".into()))?;
        if end > postings_data.len() {
            return Err(EngineError::CorruptRecord(format!(
                "sparse posting dimension {} range [{}, {}) exceeds data length {}",
                dimension_id,
                offset,
                end,
                postings_data.len()
            )));
        }

        let mut prev_node_id = None;
        for posting_index in 0..posting_count {
            let posting_base = offset + posting_index * SPARSE_POSTING_ENTRY_SIZE;
            let node_id = read_u64_at(postings_data, posting_base)?;
            let weight = read_f32_at(postings_data, posting_base + 8)?;
            if !weight.is_finite() {
                return Err(EngineError::CorruptRecord(format!(
                    "sparse posting dimension {} node {} has non-finite weight",
                    dimension_id, node_id
                )));
            }
            if weight < 0.0 {
                return Err(EngineError::CorruptRecord(format!(
                    "sparse posting dimension {} node {} has negative weight",
                    dimension_id, node_id
                )));
            }
            if prev_node_id.is_some_and(|prev| prev >= node_id) {
                return Err(EngineError::CorruptRecord(format!(
                    "sparse posting dimension {} node IDs are not strictly increasing at {}",
                    dimension_id, node_id
                )));
            }
            prev_node_id = Some(node_id);
        }

        prev_dimension = Some(dimension_id);
        next_offset = end;
    }

    if next_offset != postings_data.len() {
        return Err(EngineError::CorruptRecord(format!(
            "sparse postings data has trailing or unreferenced bytes: expected {}, got {}",
            next_offset,
            postings_data.len()
        )));
    }

    Ok(())
}

pub(crate) fn read_sparse_posting_groups(
    index_data: &[u8],
    postings_data: &[u8],
) -> Result<BTreeMap<u32, Vec<(u64, f32)>>, EngineError> {
    let mut groups = BTreeMap::new();
    if index_data.is_empty() {
        return Ok(groups);
    }

    let count = read_u64_at(index_data, 0)? as usize;
    for entry_index in 0..count {
        let base = 8 + entry_index * SPARSE_POSTING_INDEX_ENTRY_SIZE;
        let dimension_id = read_u32_at(index_data, base)?;
        let offset = read_u64_at(index_data, base + 4)? as usize;
        let posting_count = read_u32_at(index_data, base + 12)? as usize;
        let mut postings = Vec::with_capacity(posting_count);
        for posting_index in 0..posting_count {
            let posting_base = offset + posting_index * SPARSE_POSTING_ENTRY_SIZE;
            postings.push((
                read_u64_at(postings_data, posting_base)?,
                read_f32_at(postings_data, posting_base + 8)?,
            ));
        }
        groups.insert(dimension_id, postings);
    }

    Ok(groups)
}

pub(crate) fn accumulate_sparse_posting_scores(
    index_data: &[u8],
    postings_data: &[u8],
    query: &[(u32, f32)],
    scores: &mut NodeIdMap<f32>,
) -> Result<(), EngineError> {
    if index_data.is_empty() || postings_data.is_empty() || query.is_empty() {
        return Ok(());
    }

    let count = read_u64_at(index_data, 0)? as usize;
    let min_dim = query.first().expect("query checked non-empty").0;
    let max_dim = query.last().expect("query checked non-empty").0;
    let (strategy, span_start, span_end) =
        choose_sparse_posting_lookup_strategy(index_data, count, query.len(), min_dim, max_dim)?;

    match strategy {
        SparsePostingLookupStrategy::SeekPerDimension => {
            for &(dimension_id, query_weight) in query {
                if let Some((offset, posting_count)) =
                    find_dimension(index_data, count, dimension_id)?
                {
                    let offset = offset as usize;
                    let posting_bytes =
                        sparse_posting_bytes(postings_data, offset, posting_count as usize)?;
                    let mut cursor = posting_bytes.as_ptr();
                    for _ in 0..posting_count as usize {
                        let (node_id, weight) = unsafe {
                            let node_id =
                                u64::from_le(std::ptr::read_unaligned(cursor as *const u64));
                            let weight_bits =
                                u32::from_le(std::ptr::read_unaligned(cursor.add(8) as *const u32));
                            (node_id, f32::from_bits(weight_bits))
                        };
                        *scores.entry(node_id).or_insert(0.0) += query_weight * weight;
                        cursor = unsafe { cursor.add(SPARSE_POSTING_ENTRY_SIZE) };
                    }
                }
            }
        }
        SparsePostingLookupStrategy::MergeWalk => {
            let mut query_index = 0usize;
            let mut posting_index = span_start;
            while query_index < query.len() && posting_index < span_end {
                let base = 8 + posting_index * SPARSE_POSTING_INDEX_ENTRY_SIZE;
                let index_dimension = read_u32_at(index_data, base)?;
                let query_dimension = query[query_index].0;

                match index_dimension.cmp(&query_dimension) {
                    std::cmp::Ordering::Less => {
                        posting_index += 1;
                    }
                    std::cmp::Ordering::Greater => {
                        query_index += 1;
                    }
                    std::cmp::Ordering::Equal => {
                        let query_weight = query[query_index].1;
                        let offset = read_u64_at(index_data, base + 4)? as usize;
                        let posting_count = read_u32_at(index_data, base + 12)? as usize;
                        let posting_bytes =
                            sparse_posting_bytes(postings_data, offset, posting_count)?;
                        let mut cursor = posting_bytes.as_ptr();
                        for _ in 0..posting_count {
                            let (node_id, weight) = unsafe {
                                let node_id =
                                    u64::from_le(std::ptr::read_unaligned(cursor as *const u64));
                                let weight_bits = u32::from_le(std::ptr::read_unaligned(
                                    cursor.add(8) as *const u32,
                                ));
                                (node_id, f32::from_bits(weight_bits))
                            };
                            *scores.entry(node_id).or_insert(0.0) += query_weight * weight;
                            cursor = unsafe { cursor.add(SPARSE_POSTING_ENTRY_SIZE) };
                        }
                        query_index += 1;
                        posting_index += 1;
                    }
                }
            }
        }
    }
    Ok(())
}

pub(crate) fn sparse_dot_score(query: &[(u32, f32)], values: &[(u32, f32)]) -> f32 {
    let mut score = 0.0f32;
    let mut qi = 0usize;
    let mut vi = 0usize;
    while qi < query.len() && vi < values.len() {
        match query[qi].0.cmp(&values[vi].0) {
            std::cmp::Ordering::Less => qi += 1,
            std::cmp::Ordering::Greater => vi += 1,
            std::cmp::Ordering::Equal => {
                score += query[qi].1 * values[vi].1;
                qi += 1;
                vi += 1;
            }
        }
    }
    score
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    fn valid_sparse_posting_files() -> (Vec<u8>, Vec<u8>) {
        let dir = TempDir::new().unwrap();
        let mut groups = BTreeMap::new();
        groups.insert(2, vec![(3, 1.5), (9, 0.25)]);
        groups.insert(7, vec![(11, 0.75), (17, 2.0)]);
        write_sparse_posting_files(dir.path(), &groups).unwrap();
        (
            std::fs::read(dir.path().join(SPARSE_POSTING_INDEX_FILENAME)).unwrap(),
            std::fs::read(dir.path().join(SPARSE_POSTINGS_FILENAME)).unwrap(),
        )
    }

    #[test]
    fn test_sparse_dot_score_matches_overlap_only() {
        let score = sparse_dot_score(
            &[(1, 2.0), (3, 0.5), (9, -1.0)],
            &[(1, 3.0), (2, 7.0), (9, 4.0)],
        );
        assert!((score - 2.0).abs() < 1e-6);
    }

    #[test]
    fn test_write_validate_and_collect_sparse_postings() {
        let dir = TempDir::new().unwrap();
        let mut groups = BTreeMap::new();
        groups.insert(2, vec![(3, 1.5), (9, 0.25)]);
        groups.insert(7, vec![(3, 0.75)]);
        write_sparse_posting_files(dir.path(), &groups).unwrap();

        let index = std::fs::read(dir.path().join(SPARSE_POSTING_INDEX_FILENAME)).unwrap();
        let data = std::fs::read(dir.path().join(SPARSE_POSTINGS_FILENAME)).unwrap();
        validate_sparse_posting_files(&index, &data, 2, true).unwrap();
        assert_eq!(read_sparse_posting_groups(&index, &data).unwrap(), groups);
    }

    #[test]
    fn test_accumulate_sparse_posting_scores_exact_dot_product() {
        let dir = TempDir::new().unwrap();
        let mut groups = BTreeMap::new();
        groups.insert(2, vec![(3, 1.5), (9, 0.25)]);
        groups.insert(7, vec![(3, 0.75), (5, 2.0)]);
        write_sparse_posting_files(dir.path(), &groups).unwrap();

        let index = std::fs::read(dir.path().join(SPARSE_POSTING_INDEX_FILENAME)).unwrap();
        let data = std::fs::read(dir.path().join(SPARSE_POSTINGS_FILENAME)).unwrap();
        let mut scores = NodeIdMap::default();
        accumulate_sparse_posting_scores(&index, &data, &[(2, 2.0), (7, 1.0)], &mut scores)
            .unwrap();

        assert_eq!(scores.len(), 3);
        assert!((scores[&3] - 3.75).abs() < 1e-6);
        assert!((scores[&5] - 2.0).abs() < 1e-6);
        assert!((scores[&9] - 0.5).abs() < 1e-6);
    }

    #[test]
    fn test_accumulate_sparse_posting_scores_skips_interleaved_missing_dimensions() {
        let dir = TempDir::new().unwrap();
        let mut groups = BTreeMap::new();
        groups.insert(2, vec![(3, 1.5)]);
        groups.insert(7, vec![(5, 2.0)]);
        groups.insert(11, vec![(9, 0.75)]);
        write_sparse_posting_files(dir.path(), &groups).unwrap();

        let index = std::fs::read(dir.path().join(SPARSE_POSTING_INDEX_FILENAME)).unwrap();
        let data = std::fs::read(dir.path().join(SPARSE_POSTINGS_FILENAME)).unwrap();
        let mut scores = NodeIdMap::default();
        accumulate_sparse_posting_scores(
            &index,
            &data,
            &[(1, 5.0), (7, 1.0), (8, 9.0), (11, 2.0)],
            &mut scores,
        )
        .unwrap();

        assert_eq!(scores.len(), 2);
        assert!((scores[&5] - 2.0).abs() < 1e-6);
        assert!((scores[&9] - 1.5).abs() < 1e-6);
    }

    #[test]
    fn test_validate_sparse_postings_rejects_malformed_index_length() {
        let (mut index, data) = valid_sparse_posting_files();
        index.truncate(index.len() - 1);

        match validate_sparse_posting_files(&index, &data, 4, true) {
            Err(EngineError::CorruptRecord(message)) => {
                assert!(message.contains("size"));
            }
            other => panic!("expected malformed index length error, got {:?}", other),
        }
    }

    #[test]
    fn test_validate_sparse_postings_rejects_non_monotonic_dimensions() {
        let (mut index, data) = valid_sparse_posting_files();
        let second_dim_off = 8 + SPARSE_POSTING_INDEX_ENTRY_SIZE;
        index[second_dim_off..second_dim_off + 4].copy_from_slice(&2u32.to_le_bytes());

        match validate_sparse_posting_files(&index, &data, 4, true) {
            Err(EngineError::CorruptRecord(message)) => {
                assert!(message.contains("not strictly increasing"));
            }
            other => panic!("expected non-monotonic dimension error, got {:?}", other),
        }
    }

    #[test]
    fn test_validate_sparse_postings_rejects_offset_gap_or_overlap() {
        let (mut index, data) = valid_sparse_posting_files();
        let second_offset_off = 8 + SPARSE_POSTING_INDEX_ENTRY_SIZE + 4;
        index[second_offset_off..second_offset_off + 8].copy_from_slice(&12u64.to_le_bytes());

        match validate_sparse_posting_files(&index, &data, 4, true) {
            Err(EngineError::CorruptRecord(message)) => {
                assert!(message.contains("offset"));
            }
            other => panic!("expected bad offset error, got {:?}", other),
        }
    }

    #[test]
    fn test_validate_sparse_postings_rejects_truncated_posting_payload() {
        let (index, mut data) = valid_sparse_posting_files();
        data.truncate(data.len() - 1);

        match validate_sparse_posting_files(&index, &data, 4, true) {
            Err(EngineError::CorruptRecord(message)) => {
                assert!(message.contains("exceeds data length"));
            }
            other => panic!("expected truncated posting payload error, got {:?}", other),
        }
    }

    #[test]
    fn test_validate_sparse_postings_rejects_unsorted_node_ids() {
        let (index, mut data) = valid_sparse_posting_files();
        data[12..20].copy_from_slice(&1u64.to_le_bytes());

        match validate_sparse_posting_files(&index, &data, 4, true) {
            Err(EngineError::CorruptRecord(message)) => {
                assert!(message.contains("node IDs are not strictly increasing"));
            }
            other => panic!("expected unsorted node id error, got {:?}", other),
        }
    }

    #[test]
    fn test_validate_sparse_postings_rejects_trailing_posting_bytes() {
        let (index, mut data) = valid_sparse_posting_files();
        data.push(0);

        match validate_sparse_posting_files(&index, &data, 4, true) {
            Err(EngineError::CorruptRecord(message)) => {
                assert!(message.contains("trailing or unreferenced bytes"));
            }
            other => panic!("expected trailing posting bytes error, got {:?}", other),
        }
    }
}
