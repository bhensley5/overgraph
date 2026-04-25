use crate::engine::DegreeEntry;
use crate::error::EngineError;
#[cfg(test)]
use crate::types::NodeIdBuildHasher;
use crate::types::NodeIdMap;
use memmap2::Mmap;
use std::collections::BinaryHeap;
use std::fs::File;
use std::io::{BufWriter, Write};
use std::path::Path;
use std::sync::Arc;

pub(crate) const DEGREE_OVERLAY_SHARD_COUNT: usize = 1024;
pub(crate) const DEGREE_OVERLAY_GROUP_SIZE: usize = 128;
pub(crate) const DEGREE_OVERLAY_GROUP_COUNT: usize = 8;

pub(crate) const DEGREE_DELTA_FILENAME: &str = "degree_delta.dat";
const DEGREE_DELTA_MAGIC: [u8; 8] = *b"OGDDLT01";
const DEGREE_DELTA_FORMAT_VERSION: u32 = 1;
const DEGREE_DELTA_BLOCK_SIZE: u32 = 256;
const HEADER_SIZE: usize = 32;
const BLOCK_INDEX_ENTRY_SIZE: usize = 16;
const DELTA_ENTRY_SIZE: usize = 64;
const CRC_SIZE: usize = 4;

#[derive(Debug, Clone, Copy, Default, PartialEq)]
pub(crate) struct DegreeDelta {
    pub(crate) out_degree: i64,
    pub(crate) in_degree: i64,
    pub(crate) out_weight_sum: f64,
    pub(crate) in_weight_sum: f64,
    pub(crate) self_loop_count: i64,
    pub(crate) self_loop_weight_sum: f64,
    pub(crate) temporal_edge_count: i64,
}

impl DegreeDelta {
    pub(crate) const ZERO: Self = Self {
        out_degree: 0,
        in_degree: 0,
        out_weight_sum: 0.0,
        in_weight_sum: 0.0,
        self_loop_count: 0,
        self_loop_weight_sum: 0.0,
        temporal_edge_count: 0,
    };

    #[inline]
    pub(crate) fn is_zero(self) -> bool {
        self.out_degree == 0
            && self.in_degree == 0
            && self.out_weight_sum == 0.0
            && self.in_weight_sum == 0.0
            && self.self_loop_count == 0
            && self.self_loop_weight_sum == 0.0
            && self.temporal_edge_count == 0
    }

    #[inline]
    pub(crate) fn add_assign_delta(&mut self, other: Self) {
        self.out_degree += other.out_degree;
        self.in_degree += other.in_degree;
        self.out_weight_sum += other.out_weight_sum;
        self.in_weight_sum += other.in_weight_sum;
        self.self_loop_count += other.self_loop_count;
        self.self_loop_weight_sum += other.self_loop_weight_sum;
        self.temporal_edge_count += other.temporal_edge_count;
    }

    #[inline]
    pub(crate) fn add_valid_edge(from: u64, to: u64, weight: f32) -> Self {
        let weight = weight as f64;
        if from == to {
            Self {
                out_degree: 1,
                in_degree: 1,
                out_weight_sum: weight,
                in_weight_sum: weight,
                self_loop_count: 1,
                self_loop_weight_sum: weight,
                temporal_edge_count: 0,
            }
        } else {
            Self {
                out_degree: 1,
                out_weight_sum: weight,
                ..Self::ZERO
            }
        }
    }

    #[inline]
    pub(crate) fn add_valid_edge_incoming(weight: f32) -> Self {
        Self {
            in_degree: 1,
            in_weight_sum: weight as f64,
            ..Self::ZERO
        }
    }

    #[inline]
    pub(crate) fn remove_valid_edge(from: u64, to: u64, weight: f32) -> Self {
        let mut delta = Self::add_valid_edge(from, to, weight);
        delta.negate();
        delta
    }

    #[inline]
    pub(crate) fn remove_valid_edge_incoming(weight: f32) -> Self {
        let mut delta = Self::add_valid_edge_incoming(weight);
        delta.negate();
        delta
    }

    #[inline]
    pub(crate) fn add_temporal_marker() -> Self {
        Self {
            temporal_edge_count: 1,
            ..Self::ZERO
        }
    }

    #[inline]
    pub(crate) fn remove_temporal_marker() -> Self {
        Self {
            temporal_edge_count: -1,
            ..Self::ZERO
        }
    }

    #[inline]
    fn negate(&mut self) {
        self.out_degree = -self.out_degree;
        self.in_degree = -self.in_degree;
        self.out_weight_sum = -self.out_weight_sum;
        self.in_weight_sum = -self.in_weight_sum;
        self.self_loop_count = -self.self_loop_count;
        self.self_loop_weight_sum = -self.self_loop_weight_sum;
        self.temporal_edge_count = -self.temporal_edge_count;
    }
}

impl DegreeEntry {
    pub(crate) fn apply_delta(delta: DegreeDelta) -> Self {
        let out_degree = saturating_i64_to_u32(delta.out_degree);
        let in_degree = saturating_i64_to_u32(delta.in_degree);
        let self_loop_count = saturating_i64_to_u32(delta.self_loop_count);
        DegreeEntry {
            out_degree,
            in_degree,
            out_weight_sum: if out_degree == 0 {
                0.0
            } else {
                delta.out_weight_sum
            },
            in_weight_sum: if in_degree == 0 {
                0.0
            } else {
                delta.in_weight_sum
            },
            self_loop_count,
            self_loop_weight_sum: if self_loop_count == 0 {
                0.0
            } else {
                delta.self_loop_weight_sum
            },
            temporal_edge_count: saturating_i64_to_u32(delta.temporal_edge_count),
        }
    }
}

#[inline]
fn saturating_i64_to_u32(value: i64) -> u32 {
    if value <= 0 {
        0
    } else {
        value.min(u32::MAX as i64) as u32
    }
}

#[derive(Debug, Clone, Default)]
pub(crate) struct DegreeOverlayShard {
    entries: NodeIdMap<DegreeDelta>,
}

#[derive(Debug, Clone)]
pub(crate) struct DegreeOverlayShardGroup {
    shards: Box<[Arc<DegreeOverlayShard>]>,
}

#[derive(Debug, Clone)]
pub(crate) struct DegreeOverlaySnapshot {
    groups: Box<[Arc<DegreeOverlayShardGroup>]>,
}

pub(crate) struct DegreeOverlayEdit {
    groups: Vec<Arc<DegreeOverlayShardGroup>>,
}

impl DegreeOverlaySnapshot {
    pub(crate) fn empty() -> Arc<Self> {
        let mut groups = Vec::with_capacity(DEGREE_OVERLAY_GROUP_COUNT);
        for _ in 0..DEGREE_OVERLAY_GROUP_COUNT {
            let mut shards = Vec::with_capacity(DEGREE_OVERLAY_GROUP_SIZE);
            for _ in 0..DEGREE_OVERLAY_GROUP_SIZE {
                shards.push(Arc::new(DegreeOverlayShard::default()));
            }
            groups.push(Arc::new(DegreeOverlayShardGroup {
                shards: shards.into_boxed_slice(),
            }));
        }
        Arc::new(Self {
            groups: groups.into_boxed_slice(),
        })
    }

    pub(crate) fn from_flat(entries: NodeIdMap<DegreeDelta>) -> Arc<Self> {
        let snapshot = Self::empty();
        if entries.is_empty() {
            return snapshot;
        }
        let mut edit = DegreeOverlayEdit::new(snapshot);
        for (node_id, delta) in entries {
            edit.add_delta(node_id, delta);
        }
        edit.finish()
    }

    #[inline]
    pub(crate) fn get(&self, node_id: u64) -> DegreeDelta {
        let shard_index = degree_overlay_shard_index(node_id);
        let group_index = shard_index / DEGREE_OVERLAY_GROUP_SIZE;
        let local_index = shard_index % DEGREE_OVERLAY_GROUP_SIZE;
        self.groups[group_index].shards[local_index]
            .entries
            .get(&node_id)
            .copied()
            .unwrap_or(DegreeDelta::ZERO)
    }

    pub(crate) fn sorted_entries(&self) -> Vec<(u64, DegreeDelta)> {
        let mut entries = Vec::new();
        for group in self.groups.iter() {
            for shard in group.shards.iter() {
                entries.extend(
                    shard
                        .entries
                        .iter()
                        .filter(|(_, delta)| !delta.is_zero())
                        .map(|(&node_id, &delta)| (node_id, delta)),
                );
            }
        }
        entries.sort_unstable_by_key(|&(node_id, _)| node_id);
        entries
    }

    #[cfg(test)]
    pub(crate) fn group_arc_for_test(&self, index: usize) -> Arc<DegreeOverlayShardGroup> {
        Arc::clone(&self.groups[index])
    }

    #[cfg(test)]
    pub(crate) fn shard_arc_for_test(&self, shard_index: usize) -> Arc<DegreeOverlayShard> {
        let group_index = shard_index / DEGREE_OVERLAY_GROUP_SIZE;
        let local_index = shard_index % DEGREE_OVERLAY_GROUP_SIZE;
        Arc::clone(&self.groups[group_index].shards[local_index])
    }
}

impl DegreeOverlayEdit {
    pub(crate) fn new(base: Arc<DegreeOverlaySnapshot>) -> Self {
        Self {
            groups: base.groups.to_vec(),
        }
    }

    #[allow(dead_code)]
    pub(crate) fn delta_or_default(&self, node_id: u64) -> DegreeDelta {
        let shard_index = degree_overlay_shard_index(node_id);
        let group_index = shard_index / DEGREE_OVERLAY_GROUP_SIZE;
        let local_index = shard_index % DEGREE_OVERLAY_GROUP_SIZE;
        self.groups[group_index].shards[local_index]
            .entries
            .get(&node_id)
            .copied()
            .unwrap_or(DegreeDelta::ZERO)
    }

    pub(crate) fn add_delta(&mut self, node_id: u64, delta: DegreeDelta) {
        if delta.is_zero() {
            return;
        }
        let shard = self.shard_mut(node_id);
        let entry = shard.entries.entry(node_id).or_insert(DegreeDelta::ZERO);
        entry.add_assign_delta(delta);
        if entry.is_zero() {
            shard.entries.remove(&node_id);
        }
    }

    pub(crate) fn finish(mut self) -> Arc<DegreeOverlaySnapshot> {
        for group in &mut self.groups {
            if let Some(group_mut) = Arc::get_mut(group) {
                for shard in group_mut.shards.iter_mut() {
                    if let Some(shard_mut) = Arc::get_mut(shard) {
                        shard_mut.entries.retain(|_, delta| !delta.is_zero());
                    }
                }
            }
        }
        Arc::new(DegreeOverlaySnapshot {
            groups: self.groups.into_boxed_slice(),
        })
    }

    fn shard_mut(&mut self, node_id: u64) -> &mut DegreeOverlayShard {
        let shard_index = degree_overlay_shard_index(node_id);
        let group_index = shard_index / DEGREE_OVERLAY_GROUP_SIZE;
        let local_index = shard_index % DEGREE_OVERLAY_GROUP_SIZE;
        let group = Arc::make_mut(&mut self.groups[group_index]);
        Arc::make_mut(&mut group.shards[local_index])
    }
}

#[inline]
fn degree_overlay_shard_index(node_id: u64) -> usize {
    (node_id as usize) & (DEGREE_OVERLAY_SHARD_COUNT - 1)
}

pub(crate) struct DegreeSidecar {
    data: Mmap,
    entry_count: usize,
    block_count: usize,
}

impl DegreeSidecar {
    pub(crate) fn open(path: &Path) -> Result<Self, EngineError> {
        let file = File::open(path)?;
        if file.metadata()?.len() == 0 {
            return Err(EngineError::CorruptRecord(format!(
                "{} is empty",
                path.display()
            )));
        }
        let data = unsafe { Mmap::map(&file)? };
        validate_degree_sidecar(&data)?;
        let entry_count = read_u64_at(&data, 16)? as usize;
        let block_count = read_u64_at(&data, 24)? as usize;
        Ok(Self {
            data,
            entry_count,
            block_count,
        })
    }

    pub(crate) fn open_optional(path: &Path) -> Option<Self> {
        Self::open(path).ok()
    }

    #[cfg(test)]
    pub(crate) fn entry_count(&self) -> usize {
        self.entry_count
    }

    pub(crate) fn lookup(&self, node_id: u64) -> DegreeDelta {
        if self.entry_count == 0 || self.block_count == 0 {
            return DegreeDelta::ZERO;
        }

        let mut lo = 0usize;
        let mut hi = self.block_count;
        while lo < hi {
            let mid = lo + (hi - lo) / 2;
            let first = self.block_first_node_id(mid);
            if first <= node_id {
                lo = mid + 1;
            } else {
                hi = mid;
            }
        }
        if lo == 0 {
            return DegreeDelta::ZERO;
        }
        let block_index = lo - 1;
        let start = self.block_entry_start(block_index);
        let end = if block_index + 1 < self.block_count {
            self.block_entry_start(block_index + 1)
        } else {
            self.entry_count
        };

        let mut entry_lo = start;
        let mut entry_hi = end;
        while entry_lo < entry_hi {
            let mid = entry_lo + (entry_hi - entry_lo) / 2;
            let mid_node = read_sidecar_entry_node_id(&self.data, self.block_count, mid);
            match mid_node.cmp(&node_id) {
                std::cmp::Ordering::Less => entry_lo = mid + 1,
                std::cmp::Ordering::Greater => entry_hi = mid,
                std::cmp::Ordering::Equal => {
                    return read_sidecar_entry_delta(&self.data, self.block_count, mid);
                }
            }
        }

        DegreeDelta::ZERO
    }

    #[cfg(test)]
    pub(crate) fn entries(&self) -> Vec<(u64, DegreeDelta)> {
        (0..self.entry_count)
            .map(|index| {
                self.entry_at(index)
                    .expect("validated degree sidecar entry index must be in bounds")
            })
            .collect()
    }

    fn entry_at(&self, index: usize) -> Option<(u64, DegreeDelta)> {
        if index >= self.entry_count {
            return None;
        }
        Some((
            read_sidecar_entry_node_id(&self.data, self.block_count, index),
            read_sidecar_entry_delta(&self.data, self.block_count, index),
        ))
    }

    #[inline]
    fn block_first_node_id(&self, block_index: usize) -> u64 {
        let offset = HEADER_SIZE + block_index * BLOCK_INDEX_ENTRY_SIZE;
        u64::from_le_bytes(self.data[offset..offset + 8].try_into().unwrap())
    }

    #[inline]
    fn block_entry_start(&self, block_index: usize) -> usize {
        let offset = HEADER_SIZE + block_index * BLOCK_INDEX_ENTRY_SIZE + 8;
        u64::from_le_bytes(self.data[offset..offset + 8].try_into().unwrap()) as usize
    }
}

#[allow(dead_code)]
pub(crate) fn write_degree_delta_sidecar(
    path: &Path,
    entries: &[(u64, DegreeDelta)],
) -> Result<(), EngineError> {
    let mut sorted = entries
        .iter()
        .copied()
        .filter(|(_, delta)| !delta.is_zero())
        .collect::<Vec<_>>();
    sorted.sort_unstable_by_key(|&(node_id, _)| node_id);

    let mut coalesced: Vec<(u64, DegreeDelta)> = Vec::with_capacity(sorted.len());
    for (node_id, delta) in sorted {
        if let Some((last_id, last_delta)) = coalesced.last_mut() {
            if *last_id == node_id {
                last_delta.add_assign_delta(delta);
                continue;
            }
        }
        coalesced.push((node_id, delta));
    }
    coalesced.retain(|(_, delta)| !delta.is_zero());

    write_sorted_degree_delta_sidecar_unchecked(path, &coalesced)
}

pub(crate) fn write_sorted_degree_delta_sidecar(
    path: &Path,
    entries: &[(u64, DegreeDelta)],
) -> Result<(), EngineError> {
    validate_sorted_degree_delta_entries(entries)?;
    write_sorted_degree_delta_sidecar_unchecked(path, entries)
}

fn validate_sorted_degree_delta_entries(entries: &[(u64, DegreeDelta)]) -> Result<(), EngineError> {
    let mut previous_node_id = None;
    for &(node_id, delta) in entries {
        if delta.is_zero() {
            return Err(EngineError::InvalidOperation(
                "sorted degree sidecar entries must not include zero deltas".into(),
            ));
        }
        if previous_node_id.is_some_and(|previous| previous >= node_id) {
            return Err(EngineError::InvalidOperation(
                "sorted degree sidecar entries must be strictly ordered by node id".into(),
            ));
        }
        previous_node_id = Some(node_id);
    }
    Ok(())
}

fn write_sorted_degree_delta_sidecar_unchecked(
    path: &Path,
    entries: &[(u64, DegreeDelta)],
) -> Result<(), EngineError> {
    let entry_count = entries.len();
    let block_size = DEGREE_DELTA_BLOCK_SIZE as usize;
    let block_count = if entry_count == 0 {
        0
    } else {
        entry_count.div_ceil(block_size)
    };

    let mut bytes = Vec::with_capacity(
        HEADER_SIZE
            + block_count * BLOCK_INDEX_ENTRY_SIZE
            + entry_count * DELTA_ENTRY_SIZE
            + CRC_SIZE,
    );
    bytes.extend_from_slice(&DEGREE_DELTA_MAGIC);
    bytes.extend_from_slice(&DEGREE_DELTA_FORMAT_VERSION.to_le_bytes());
    bytes.extend_from_slice(&DEGREE_DELTA_BLOCK_SIZE.to_le_bytes());
    bytes.extend_from_slice(&(entry_count as u64).to_le_bytes());
    bytes.extend_from_slice(&(block_count as u64).to_le_bytes());

    for block_index in 0..block_count {
        let entry_start = block_index * block_size;
        bytes.extend_from_slice(&entries[entry_start].0.to_le_bytes());
        bytes.extend_from_slice(&(entry_start as u64).to_le_bytes());
    }

    for (node_id, delta) in entries {
        bytes.extend_from_slice(&node_id.to_le_bytes());
        bytes.extend_from_slice(&delta.out_degree.to_le_bytes());
        bytes.extend_from_slice(&delta.in_degree.to_le_bytes());
        bytes.extend_from_slice(&delta.out_weight_sum.to_le_bytes());
        bytes.extend_from_slice(&delta.in_weight_sum.to_le_bytes());
        bytes.extend_from_slice(&delta.self_loop_count.to_le_bytes());
        bytes.extend_from_slice(&delta.self_loop_weight_sum.to_le_bytes());
        bytes.extend_from_slice(&delta.temporal_edge_count.to_le_bytes());
    }

    let crc = crc32fast::hash(&bytes);
    bytes.extend_from_slice(&crc.to_le_bytes());

    let mut file = File::create(path)?;
    file.write_all(&bytes)?;
    file.sync_all()?;
    Ok(())
}

pub(crate) fn write_folded_degree_delta_sidecar_from_sidecars(
    path: &Path,
    sidecars: &[&DegreeSidecar],
) -> Result<(), EngineError> {
    let mut entry_count = 0usize;
    let mut block_index = Vec::new();
    let block_size = DEGREE_DELTA_BLOCK_SIZE as usize;
    for_each_folded_degree_entry(sidecars, |node_id, _| {
        if entry_count.is_multiple_of(block_size) {
            block_index.push((node_id, entry_count as u64));
        }
        entry_count += 1;
        Ok(())
    })?;

    let mut writer = BufWriter::new(File::create(path)?);
    let mut hasher = crc32fast::Hasher::new();
    write_hashed(&mut writer, &mut hasher, &DEGREE_DELTA_MAGIC)?;
    write_hashed(
        &mut writer,
        &mut hasher,
        &DEGREE_DELTA_FORMAT_VERSION.to_le_bytes(),
    )?;
    write_hashed(
        &mut writer,
        &mut hasher,
        &DEGREE_DELTA_BLOCK_SIZE.to_le_bytes(),
    )?;
    write_hashed(
        &mut writer,
        &mut hasher,
        &(entry_count as u64).to_le_bytes(),
    )?;
    write_hashed(
        &mut writer,
        &mut hasher,
        &(block_index.len() as u64).to_le_bytes(),
    )?;

    for &(first_node_id, entry_start) in &block_index {
        write_hashed(&mut writer, &mut hasher, &first_node_id.to_le_bytes())?;
        write_hashed(&mut writer, &mut hasher, &entry_start.to_le_bytes())?;
    }

    let mut emitted = 0usize;
    for_each_folded_degree_entry(sidecars, |node_id, delta| {
        if emitted.is_multiple_of(block_size) {
            debug_assert_eq!(
                block_index.get(emitted / block_size),
                Some(&(node_id, emitted as u64))
            );
        }
        write_degree_delta_entry_hashed(&mut writer, &mut hasher, node_id, delta)?;
        emitted += 1;
        Ok(())
    })?;
    debug_assert_eq!(emitted, entry_count);

    let crc = hasher.finalize();
    writer.write_all(&crc.to_le_bytes())?;
    writer.flush()?;
    writer.get_ref().sync_all()?;
    Ok(())
}

fn write_degree_delta_entry_hashed(
    writer: &mut impl Write,
    hasher: &mut crc32fast::Hasher,
    node_id: u64,
    delta: DegreeDelta,
) -> Result<(), EngineError> {
    write_hashed(writer, hasher, &node_id.to_le_bytes())?;
    write_hashed(writer, hasher, &delta.out_degree.to_le_bytes())?;
    write_hashed(writer, hasher, &delta.in_degree.to_le_bytes())?;
    write_hashed(writer, hasher, &delta.out_weight_sum.to_le_bytes())?;
    write_hashed(writer, hasher, &delta.in_weight_sum.to_le_bytes())?;
    write_hashed(writer, hasher, &delta.self_loop_count.to_le_bytes())?;
    write_hashed(writer, hasher, &delta.self_loop_weight_sum.to_le_bytes())?;
    write_hashed(writer, hasher, &delta.temporal_edge_count.to_le_bytes())?;
    Ok(())
}

fn write_hashed(
    writer: &mut impl Write,
    hasher: &mut crc32fast::Hasher,
    bytes: &[u8],
) -> Result<(), EngineError> {
    writer.write_all(bytes)?;
    hasher.update(bytes);
    Ok(())
}

fn for_each_folded_degree_entry(
    sidecars: &[&DegreeSidecar],
    mut emit: impl FnMut(u64, DegreeDelta) -> Result<(), EngineError>,
) -> Result<(), EngineError> {
    let mut cursors: Vec<_> = sidecars
        .iter()
        .map(|&sidecar| DegreeSidecarCursor { sidecar, index: 0 })
        .collect();
    let mut heap = BinaryHeap::with_capacity(cursors.len());
    for (cursor_index, cursor) in cursors.iter().enumerate() {
        if let Some((node_id, _)) = cursor.current() {
            heap.push(DegreeSidecarHeapEntry {
                node_id,
                cursor_index,
            });
        }
    }

    while let Some(first) = heap.pop() {
        let node_id = first.node_id;
        let mut sum = DegreeDelta::ZERO;
        consume_degree_cursor(
            &mut cursors,
            &mut heap,
            first.cursor_index,
            node_id,
            &mut sum,
        );
        while heap.peek().is_some_and(|entry| entry.node_id == node_id) {
            let entry = heap
                .pop()
                .expect("heap entry must exist after successful peek");
            consume_degree_cursor(
                &mut cursors,
                &mut heap,
                entry.cursor_index,
                node_id,
                &mut sum,
            );
        }
        if !sum.is_zero() {
            emit(node_id, sum)?;
        }
    }

    Ok(())
}

fn consume_degree_cursor(
    cursors: &mut [DegreeSidecarCursor<'_>],
    heap: &mut BinaryHeap<DegreeSidecarHeapEntry>,
    cursor_index: usize,
    expected_node_id: u64,
    sum: &mut DegreeDelta,
) {
    let cursor = &mut cursors[cursor_index];
    let Some((node_id, delta)) = cursor.current() else {
        return;
    };
    debug_assert_eq!(node_id, expected_node_id);
    sum.add_assign_delta(delta);
    cursor.index += 1;
    if let Some((next_node_id, _)) = cursor.current() {
        heap.push(DegreeSidecarHeapEntry {
            node_id: next_node_id,
            cursor_index,
        });
    }
}

struct DegreeSidecarCursor<'a> {
    sidecar: &'a DegreeSidecar,
    index: usize,
}

impl DegreeSidecarCursor<'_> {
    fn current(&self) -> Option<(u64, DegreeDelta)> {
        self.sidecar.entry_at(self.index)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct DegreeSidecarHeapEntry {
    node_id: u64,
    cursor_index: usize,
}

impl Ord for DegreeSidecarHeapEntry {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        other
            .node_id
            .cmp(&self.node_id)
            .then_with(|| other.cursor_index.cmp(&self.cursor_index))
    }
}

impl PartialOrd for DegreeSidecarHeapEntry {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

fn validate_degree_sidecar(data: &[u8]) -> Result<(), EngineError> {
    if data.len() < HEADER_SIZE + CRC_SIZE {
        return Err(EngineError::CorruptRecord(format!(
            "degree sidecar length {} is smaller than header",
            data.len()
        )));
    }
    if data.get(0..8) != Some(&DEGREE_DELTA_MAGIC) {
        return Err(EngineError::CorruptRecord(
            "degree sidecar magic mismatch".into(),
        ));
    }
    let format_version = read_u32_at(data, 8)?;
    if format_version != DEGREE_DELTA_FORMAT_VERSION {
        return Err(EngineError::CorruptRecord(format!(
            "unsupported degree sidecar version {}",
            format_version
        )));
    }
    let block_size = read_u32_at(data, 12)?;
    if block_size != DEGREE_DELTA_BLOCK_SIZE {
        return Err(EngineError::CorruptRecord(format!(
            "unsupported degree sidecar block size {}",
            block_size
        )));
    }
    let entry_count = read_u64_at(data, 16)? as usize;
    let block_count = read_u64_at(data, 24)? as usize;
    let expected_block_count = if entry_count == 0 {
        0
    } else {
        entry_count.div_ceil(DEGREE_DELTA_BLOCK_SIZE as usize)
    };
    if block_count != expected_block_count {
        return Err(EngineError::CorruptRecord(format!(
            "degree sidecar block count {} does not match entry count {}",
            block_count, entry_count
        )));
    }
    let entries_start = HEADER_SIZE
        .checked_add(
            block_count
                .checked_mul(BLOCK_INDEX_ENTRY_SIZE)
                .ok_or_else(|| {
                    EngineError::CorruptRecord("degree sidecar block index length overflow".into())
                })?,
        )
        .ok_or_else(|| {
            EngineError::CorruptRecord("degree sidecar entries offset overflow".into())
        })?;
    let entries_len = entry_count.checked_mul(DELTA_ENTRY_SIZE).ok_or_else(|| {
        EngineError::CorruptRecord("degree sidecar entries length overflow".into())
    })?;
    let expected_len = entries_start
        .checked_add(entries_len)
        .and_then(|len| len.checked_add(CRC_SIZE))
        .ok_or_else(|| EngineError::CorruptRecord("degree sidecar length overflow".into()))?;
    if data.len() != expected_len {
        return Err(EngineError::CorruptRecord(format!(
            "degree sidecar length {} does not match expected {}",
            data.len(),
            expected_len
        )));
    }

    let stored_crc = read_u32_at(data, data.len() - CRC_SIZE)?;
    let actual_crc = crc32fast::hash(&data[..data.len() - CRC_SIZE]);
    if stored_crc != actual_crc {
        return Err(EngineError::CorruptRecord(
            "degree sidecar CRC mismatch".into(),
        ));
    }

    let mut prev_node_id = None;
    for index in 0..entry_count {
        let node_id = read_sidecar_entry_node_id(data, block_count, index);
        if prev_node_id.is_some_and(|prev| prev >= node_id) {
            return Err(EngineError::CorruptRecord(format!(
                "degree sidecar node IDs are not strictly sorted at entry {}",
                index
            )));
        }
        let delta = read_sidecar_entry_delta(data, block_count, index);
        if delta.is_zero() {
            return Err(EngineError::CorruptRecord(format!(
                "degree sidecar contains zero delta at entry {}",
                index
            )));
        }
        prev_node_id = Some(node_id);
    }

    for block_index in 0..block_count {
        let index_offset = HEADER_SIZE + block_index * BLOCK_INDEX_ENTRY_SIZE;
        let first_node_id = read_u64_at(data, index_offset)?;
        let entry_start = read_u64_at(data, index_offset + 8)? as usize;
        let expected_start = block_index * DEGREE_DELTA_BLOCK_SIZE as usize;
        if entry_start != expected_start {
            return Err(EngineError::CorruptRecord(format!(
                "degree sidecar block {} starts at {}, expected {}",
                block_index, entry_start, expected_start
            )));
        }
        if entry_start >= entry_count {
            return Err(EngineError::CorruptRecord(format!(
                "degree sidecar block {} start {} exceeds entry count {}",
                block_index, entry_start, entry_count
            )));
        }
        let actual_first = read_sidecar_entry_node_id(data, block_count, entry_start);
        if first_node_id != actual_first {
            return Err(EngineError::CorruptRecord(format!(
                "degree sidecar block {} first node {} does not match entry {}",
                block_index, first_node_id, actual_first
            )));
        }
    }

    Ok(())
}

#[inline]
fn sidecar_entry_offset(block_count: usize, index: usize) -> usize {
    HEADER_SIZE + block_count * BLOCK_INDEX_ENTRY_SIZE + index * DELTA_ENTRY_SIZE
}

#[inline]
fn read_sidecar_entry_node_id(data: &[u8], block_count: usize, index: usize) -> u64 {
    let offset = sidecar_entry_offset(block_count, index);
    u64::from_le_bytes(data[offset..offset + 8].try_into().unwrap())
}

#[inline]
fn read_sidecar_entry_delta(data: &[u8], block_count: usize, index: usize) -> DegreeDelta {
    let offset = sidecar_entry_offset(block_count, index);
    DegreeDelta {
        out_degree: i64::from_le_bytes(data[offset + 8..offset + 16].try_into().unwrap()),
        in_degree: i64::from_le_bytes(data[offset + 16..offset + 24].try_into().unwrap()),
        out_weight_sum: f64::from_le_bytes(data[offset + 24..offset + 32].try_into().unwrap()),
        in_weight_sum: f64::from_le_bytes(data[offset + 32..offset + 40].try_into().unwrap()),
        self_loop_count: i64::from_le_bytes(data[offset + 40..offset + 48].try_into().unwrap()),
        self_loop_weight_sum: f64::from_le_bytes(
            data[offset + 48..offset + 56].try_into().unwrap(),
        ),
        temporal_edge_count: i64::from_le_bytes(data[offset + 56..offset + 64].try_into().unwrap()),
    }
}

fn read_u32_at(data: &[u8], offset: usize) -> Result<u32, EngineError> {
    let end = offset
        .checked_add(4)
        .ok_or_else(|| EngineError::CorruptRecord("degree sidecar u32 offset overflow".into()))?;
    let slice = data.get(offset..end).ok_or_else(|| {
        EngineError::CorruptRecord(format!(
            "degree sidecar u32 read at offset {} exceeds length {}",
            offset,
            data.len()
        ))
    })?;
    Ok(u32::from_le_bytes(slice.try_into().unwrap()))
}

fn read_u64_at(data: &[u8], offset: usize) -> Result<u64, EngineError> {
    let end = offset
        .checked_add(8)
        .ok_or_else(|| EngineError::CorruptRecord("degree sidecar u64 offset overflow".into()))?;
    let slice = data.get(offset..end).ok_or_else(|| {
        EngineError::CorruptRecord(format!(
            "degree sidecar u64 read at offset {} exceeds length {}",
            offset,
            data.len()
        ))
    })?;
    Ok(u64::from_le_bytes(slice.try_into().unwrap()))
}

#[cfg(test)]
pub(crate) fn fold_degree_entries(
    inputs: impl IntoIterator<Item = (u64, DegreeDelta)>,
) -> Vec<(u64, DegreeDelta)> {
    let mut map: NodeIdMap<DegreeDelta> = NodeIdMap::with_hasher(NodeIdBuildHasher::default());
    for (node_id, delta) in inputs {
        let entry = map.entry(node_id).or_insert(DegreeDelta::ZERO);
        entry.add_assign_delta(delta);
        if entry.is_zero() {
            map.remove(&node_id);
        }
    }
    let mut entries: Vec<_> = map.into_iter().collect();
    entries.sort_unstable_by_key(|&(node_id, _)| node_id);
    entries
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    #[test]
    fn degree_delta_self_loop_and_negative_composition() {
        let mut delta = DegreeDelta::ZERO;
        delta.add_assign_delta(DegreeDelta::add_valid_edge(7, 7, 2.5));
        delta.add_assign_delta(DegreeDelta::add_temporal_marker());
        assert_eq!(delta.out_degree, 1);
        assert_eq!(delta.in_degree, 1);
        assert_eq!(delta.self_loop_count, 1);
        assert_eq!(delta.temporal_edge_count, 1);
        assert!((delta.self_loop_weight_sum - 2.5).abs() < 1e-10);

        delta.add_assign_delta(DegreeDelta::remove_valid_edge(7, 7, 2.5));
        delta.add_assign_delta(DegreeDelta::remove_temporal_marker());
        assert!(delta.is_zero());
    }

    #[test]
    fn overlay_edit_clones_only_touched_group_and_shard() {
        let base = DegreeOverlaySnapshot::empty();
        let untouched_group = base.group_arc_for_test(1);
        let touched_group = base.group_arc_for_test(0);
        let untouched_shard = base.shard_arc_for_test(2);
        let touched_shard = base.shard_arc_for_test(1);

        let mut edit = DegreeOverlayEdit::new(Arc::clone(&base));
        edit.add_delta(1, DegreeDelta::add_valid_edge(1, 2, 1.0));
        let edited = edit.finish();

        assert!(Arc::ptr_eq(&untouched_group, &edited.group_arc_for_test(1)));
        assert!(Arc::ptr_eq(&untouched_shard, &edited.shard_arc_for_test(2)));
        assert!(!Arc::ptr_eq(&touched_group, &edited.group_arc_for_test(0)));
        assert!(!Arc::ptr_eq(&touched_shard, &edited.shard_arc_for_test(1)));
        assert_eq!(edited.get(1).out_degree, 1);
        assert_eq!(base.get(1), DegreeDelta::ZERO);
    }

    #[test]
    fn overlay_zero_delta_elision_and_sorted_iteration() {
        let base = DegreeOverlaySnapshot::empty();
        let mut edit = DegreeOverlayEdit::new(base);
        edit.add_delta(10, DegreeDelta::add_valid_edge(10, 20, 1.0));
        edit.add_delta(2, DegreeDelta::add_valid_edge(2, 20, 1.0));
        edit.add_delta(10, DegreeDelta::remove_valid_edge(10, 20, 1.0));
        let snapshot = edit.finish();
        let entries = snapshot.sorted_entries();
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].0, 2);
    }

    #[test]
    fn sidecar_round_trip_block_lookup_and_zero_entry_file() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join(DEGREE_DELTA_FILENAME);
        let mut entries = Vec::new();
        for node_id in 1..=600u64 {
            entries.push((
                node_id * 2,
                DegreeDelta {
                    out_degree: node_id as i64,
                    out_weight_sum: node_id as f64,
                    ..DegreeDelta::ZERO
                },
            ));
        }
        write_degree_delta_sidecar(&path, &entries).unwrap();
        let sidecar = DegreeSidecar::open(&path).unwrap();
        assert_eq!(sidecar.lookup(2).out_degree, 1);
        assert_eq!(sidecar.lookup(600).out_degree, 300);
        assert_eq!(sidecar.lookup(1200).out_degree, 600);
        assert_eq!(sidecar.lookup(3), DegreeDelta::ZERO);

        let zero_path = dir.path().join("zero.dat");
        write_degree_delta_sidecar(&zero_path, &[]).unwrap();
        let zero = DegreeSidecar::open(&zero_path).unwrap();
        assert_eq!(zero.lookup(1), DegreeDelta::ZERO);
        assert!(zero.entries().is_empty());
    }

    #[test]
    fn sorted_sidecar_writer_matches_defensive_writer_for_sorted_entries() {
        let dir = tempfile::tempdir().unwrap();
        let defensive_path = dir.path().join("defensive_degree_delta.dat");
        let sorted_path = dir.path().join("sorted_degree_delta.dat");
        let mut entries = Vec::new();
        for node_id in 1..=600u64 {
            entries.push((
                node_id * 3,
                DegreeDelta {
                    out_degree: node_id as i64,
                    in_degree: -(node_id as i64),
                    out_weight_sum: node_id as f64 * 1.5,
                    in_weight_sum: -(node_id as f64),
                    self_loop_count: (node_id % 3) as i64,
                    self_loop_weight_sum: node_id as f64 / 2.0,
                    temporal_edge_count: (node_id % 5) as i64,
                },
            ));
        }

        write_degree_delta_sidecar(&defensive_path, &entries).unwrap();
        write_sorted_degree_delta_sidecar(&sorted_path, &entries).unwrap();
        assert_eq!(
            std::fs::read(&defensive_path).unwrap(),
            std::fs::read(&sorted_path).unwrap()
        );

        let sidecar = DegreeSidecar::open(&sorted_path).unwrap();
        assert_eq!(sidecar.entry_count(), entries.len());
        assert_eq!(sidecar.lookup(3), entries[0].1);
        assert_eq!(sidecar.lookup(900), entries[299].1);
        assert_eq!(sidecar.lookup(1800), entries[599].1);
        assert_eq!(sidecar.lookup(4), DegreeDelta::ZERO);
    }

    #[test]
    fn sorted_sidecar_writer_rejects_unsorted_duplicate_or_zero_entries() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join(DEGREE_DELTA_FILENAME);
        let delta = DegreeDelta::add_valid_edge(1, 2, 1.0);

        let err = write_sorted_degree_delta_sidecar(&path, &[(2, delta), (1, delta)]).unwrap_err();
        assert!(matches!(err, EngineError::InvalidOperation(_)));

        let err = write_sorted_degree_delta_sidecar(&path, &[(1, delta), (1, delta)]).unwrap_err();
        assert!(matches!(err, EngineError::InvalidOperation(_)));

        let err = write_sorted_degree_delta_sidecar(&path, &[(1, DegreeDelta::ZERO)]).unwrap_err();
        assert!(matches!(err, EngineError::InvalidOperation(_)));
    }

    #[test]
    fn streaming_folded_sidecar_matches_hash_fold_and_block_lookup() {
        let dir = tempfile::tempdir().unwrap();
        let left_path = dir.path().join("left_degree_delta.dat");
        let right_path = dir.path().join("right_degree_delta.dat");
        let output_path = dir.path().join("folded_degree_delta.dat");

        let mut left_entries = Vec::new();
        for node_id in 1..=300u64 {
            left_entries.push((
                node_id,
                DegreeDelta {
                    out_degree: 1,
                    out_weight_sum: node_id as f64,
                    ..DegreeDelta::ZERO
                },
            ));
        }
        left_entries.push((301, DegreeDelta::add_valid_edge(301, 302, 7.0)));

        let mut right_entries = Vec::new();
        for node_id in (2..=300u64).step_by(2) {
            right_entries.push((
                node_id,
                DegreeDelta {
                    in_degree: 2,
                    in_weight_sum: (node_id * 2) as f64,
                    ..DegreeDelta::ZERO
                },
            ));
        }
        right_entries.push((301, DegreeDelta::remove_valid_edge(301, 302, 7.0)));

        write_degree_delta_sidecar(&left_path, &left_entries).unwrap();
        write_degree_delta_sidecar(&right_path, &right_entries).unwrap();
        let left = DegreeSidecar::open(&left_path).unwrap();
        let right = DegreeSidecar::open(&right_path).unwrap();

        write_folded_degree_delta_sidecar_from_sidecars(&output_path, &[&left, &right]).unwrap();
        let output = DegreeSidecar::open(&output_path).unwrap();
        let expected =
            fold_degree_entries(left_entries.iter().chain(right_entries.iter()).copied());

        assert_eq!(output.entry_count, expected.len());
        assert_eq!(output.block_count, 2);
        for (node_id, delta) in expected {
            assert_eq!(output.lookup(node_id), delta);
        }
        assert_eq!(output.lookup(301), DegreeDelta::ZERO);
        assert_eq!(output.lookup(999), DegreeDelta::ZERO);
    }

    #[test]
    fn streaming_folded_sidecar_writes_valid_empty_file_after_cancellation() {
        let dir = tempfile::tempdir().unwrap();
        let left_path = dir.path().join("left_degree_delta.dat");
        let right_path = dir.path().join("right_degree_delta.dat");
        let output_path = dir.path().join("folded_degree_delta.dat");
        let add = DegreeDelta::add_valid_edge(10, 20, 1.0);
        let remove = DegreeDelta::remove_valid_edge(10, 20, 1.0);

        write_degree_delta_sidecar(&left_path, &[(10, add)]).unwrap();
        write_degree_delta_sidecar(&right_path, &[(10, remove)]).unwrap();
        let left = DegreeSidecar::open(&left_path).unwrap();
        let right = DegreeSidecar::open(&right_path).unwrap();

        write_folded_degree_delta_sidecar_from_sidecars(&output_path, &[&left, &right]).unwrap();
        let output = DegreeSidecar::open(&output_path).unwrap();
        assert_eq!(output.entry_count(), 0);
        assert_eq!(output.lookup(10), DegreeDelta::ZERO);
    }

    #[test]
    fn sidecar_crc_validation_disables_optional_open() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join(DEGREE_DELTA_FILENAME);
        write_degree_delta_sidecar(&path, &[(1, DegreeDelta::add_valid_edge(1, 2, 1.0))]).unwrap();
        let mut bytes = std::fs::read(&path).unwrap();
        let last = bytes.len() - 1;
        bytes[last] ^= 0xff;
        std::fs::write(&path, bytes).unwrap();
        assert!(DegreeSidecar::open(&path).is_err());
        assert!(DegreeSidecar::open_optional(&path).is_none());
    }
}
