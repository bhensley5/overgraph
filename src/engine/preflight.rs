//! Read-only admission before recovery is allowed to mutate a directory.
//!
//! The existing JSON and property codecs validate the supported scan portions.
//! This module counts replay work; it never replays records into a second database.
//! Parse state uses ordinary local ownership on success, refusal and unwind.
//! Wire validation keeps scalar summaries and uses the existing decoders.
//! The completed permit is private; public engine routing belongs to a3.
#![allow(dead_code)] // Actual DatabaseEngine open consumes this substrate in a3.

use super::bounds::{self, BoundError};
use super::payload::{json::JsonJob, messagepack, PayloadError};
use super::reclamation::DomainHandle;
use super::storage_bounds::RecordSummary;
use crate::error::EngineError;
use crate::manifest::{ManifestBodyScratch, MANIFEST_CURRENT, MANIFEST_PREV, MANIFEST_TMP};
use crate::segment_reader::SegmentReaderOpen;
use crate::types::{ConcurrencyOptions, DbOptions, FlushEpochState, ManifestState, OpTag};
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, HashMap};
use std::fs::{self, File};
use std::io::{Read, Seek, SeekFrom};
use std::path::{Path, PathBuf};

#[cfg(test)]
thread_local! {
    static PREFLIGHT_WORK: std::cell::Cell<[usize; 3]> = const { std::cell::Cell::new([0; 3]) };
    static BEFORE_REVALIDATE: std::cell::RefCell<Option<Box<dyn FnOnce()>>> = const { std::cell::RefCell::new(None) };
}
#[cfg(test)]
fn note_preflight_work(index: usize) {
    PREFLIGHT_WORK.with(|work| {
        let mut n = work.get();
        n[index] += 1;
        work.set(n);
    });
}
#[cfg(test)]
// Witnessed files, fingerprint calls and revalidation attempts.
pub(super) fn take_preflight_work_for_test() -> [usize; 3] {
    PREFLIGHT_WORK.with(|work| work.replace([0; 3]))
}
#[cfg(test)]
fn with_revalidation_hook_for_test<R>(hook: impl FnOnce() + 'static, run: impl FnOnce() -> R) -> R {
    struct Reset;
    impl Drop for Reset {
        fn drop(&mut self) {
            BEFORE_REVALIDATE.with(|hook| {
                hook.borrow_mut().take();
            });
        }
    }
    let _reset = Reset;
    BEFORE_REVALIDATE.with(|slot| assert!(slot.borrow_mut().replace(Box::new(hook)).is_none()));
    run()
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum ManifestSource {
    Current,
    Temporary,
    Previous,
    Empty,
}

#[derive(Default, Clone, Copy, Debug)]
pub(super) struct ReplaySummary {
    pub(super) records: RecordSummary,
    /// Number of normal WAL operations represented by this summary.
    pub(super) operations: usize,
    pub(super) node_deletions: usize,
    pub(super) edge_deletions: usize,
    pub(super) label_tokens: usize,
    pub(super) label_name_bytes: usize,
    pub(super) wire_bytes: usize,
    pub(super) max_batch_wire_bytes: usize,
    pub(super) max_batch_operations: usize,
    /// Number of decoded atomic-envelope markers.
    pub(super) markers: usize,
    pub(super) dense_level_slots: usize,
}

#[derive(Clone, Copy, Debug)]
pub(super) struct WalSummary {
    pub(super) generation: u64,
    pub(super) frozen: bool,
    pub(super) durable_len: u64,
    pub(super) file_len: u64,
    pub(super) replay: ReplaySummary,
    /// Includes a decoded incomplete atomic prefix retained by the real reader.
    pub(super) decoded: ReplaySummary,
}

#[derive(Clone, Copy, Debug)]
pub(super) struct PreflightSummary {
    pub(super) source: ManifestSource,
    pub(super) normalized_in_memory: bool,
    pub(super) segments: usize,
    pub(super) frozen_generations: usize,
}

struct FileWitness {
    path: PathBuf,
    file: File,
    len: u64,
    digest: [u8; 32],
}

#[derive(Default)]
struct PreflightJob {
    candidates: Vec<JsonJob>,
    selected: Option<usize>,
    validation: ManifestBodyScratch,
    declarations: HashMap<[u8; 32], Vec<usize>>,
    token_ids: [BTreeMap<u32, String>; 2],
    token_names: [BTreeMap<String, u32>; 2],
    absent: Vec<PathBuf>,
    readers: Vec<SegmentReaderOpen>,
    files: Vec<FileWitness>,
    file_ids: HashMap<PathBuf, usize>,
    wal: Vec<WalSummary>,
    wire: Vec<u8>,
    sparse: Vec<(u32, f32)>,
    components: Vec<crate::segment_components::retained_manifest::Open>,
    identity: Option<super::directory_identity::DirectoryPin>,
    directory: PathBuf,
}

/// The caller retains its canonical DirectorySession/creation gate throughout
/// scan and revalidation. Scan constructs witnessed read-only state, not a
/// mutation authorization. The consuming boundary must call revalidate before
/// startup mutation. Drop releases file and payload ownership normally.
pub(super) struct PreflightPermit {
    owner: PreflightJob,
    summary: PreflightSummary,
    admission: DirectoryAdmission,
}

/// Retains admission through the revalidation-before-first-write boundary.
pub(super) enum DirectoryAdmission {
    Existing(super::completion::DirectorySession),
    Missing {
        parent: File,
        gate: std::sync::MutexGuard<'static, ()>,
    },
}

fn reclaim(error: super::reclamation::ReclaimError) -> EngineError {
    EngineError::from(super::concurrency_lifecycle::Outcome::from(error))
}

impl ReplaySummary {
    fn add(self, rhs: Self) -> Result<Self, EngineError> {
        let plus = |a: usize, b: usize| {
            a.checked_add(b)
                .ok_or_else(|| limit("wire_extent", usize::MAX, usize::MAX))
        };
        Ok(Self {
            records: self
                .records
                .checked_add(rhs.records)
                .map_err(|e| bounded(e, usize::MAX))?,
            operations: plus(self.operations, rhs.operations)?,
            node_deletions: plus(self.node_deletions, rhs.node_deletions)?,
            edge_deletions: plus(self.edge_deletions, rhs.edge_deletions)?,
            label_tokens: plus(self.label_tokens, rhs.label_tokens)?,
            label_name_bytes: plus(self.label_name_bytes, rhs.label_name_bytes)?,
            wire_bytes: plus(self.wire_bytes, rhs.wire_bytes)?,
            max_batch_wire_bytes: self.max_batch_wire_bytes.max(rhs.max_batch_wire_bytes),
            max_batch_operations: self.max_batch_operations.max(rhs.max_batch_operations),
            markers: plus(self.markers, rhs.markers)?,
            dense_level_slots: plus(self.dense_level_slots, rhs.dense_level_slots)?,
        })
    }
}

/// Borrowed primitives mirror encoding::decode_wal_op's envelope. Properties
/// use the existing iterative typed codec; no owned WalOp is constructed here.
struct Cursor<'a> {
    bytes: &'a [u8],
    pos: usize,
}

impl<'a> Cursor<'a> {
    fn take(&mut self, n: usize) -> Result<&'a [u8], EngineError> {
        let end = self
            .pos
            .checked_add(n)
            .ok_or_else(|| corrupt("WAL extent overflow"))?;
        let out = self
            .bytes
            .get(self.pos..end)
            .ok_or_else(|| corrupt("truncated WAL record"))?;
        self.pos = end;
        Ok(out)
    }
    fn u8(&mut self) -> Result<u8, EngineError> {
        Ok(self.take(1)?[0])
    }
    fn u16(&mut self) -> Result<u16, EngineError> {
        Ok(u16::from_le_bytes(self.take(2)?.try_into().unwrap()))
    }
    fn u32(&mut self) -> Result<u32, EngineError> {
        Ok(u32::from_le_bytes(self.take(4)?.try_into().unwrap()))
    }
    fn u64(&mut self) -> Result<u64, EngineError> {
        Ok(u64::from_le_bytes(self.take(8)?.try_into().unwrap()))
    }
    fn string(&mut self) -> Result<&'a str, EngineError> {
        let n = self.u16()? as usize;
        std::str::from_utf8(self.take(n)?).map_err(|_| corrupt("invalid UTF-8 in WAL"))
    }
    fn blob(&mut self) -> Result<&'a [u8], EngineError> {
        let n = self.u32()? as usize;
        self.take(n)
    }
    fn end(&self) -> Result<(), EngineError> {
        if self.pos == self.bytes.len() {
            Ok(())
        } else {
            Err(corrupt(
                "failed to decode WAL record: unexpected trailing bytes",
            ))
        }
    }
}
fn root_properties(bytes: &[u8]) -> Result<usize, EngineError> {
    match bytes.first().copied() {
        Some(marker @ 0x80..=0x8f) => Ok((marker & 15) as usize),
        Some(0xde) => Ok(u16::from_be_bytes(
            bytes
                .get(1..3)
                .ok_or_else(|| corrupt("truncated properties"))?
                .try_into()
                .unwrap(),
        ) as usize),
        Some(0xdf) => Ok(u32::from_be_bytes(
            bytes
                .get(1..5)
                .ok_or_else(|| corrupt("truncated properties"))?
                .try_into()
                .unwrap(),
        ) as usize),
        _ => Err(corrupt("properties are not a map")),
    }
}
enum BorrowedOp<'a> {
    Normal(ReplaySummary),
    Token {
        node: bool,
        name: &'a str,
        id: u32,
        summary: ReplaySummary,
    },
    Begin {
        first: u64,
        count: u32,
    },
    Commit {
        first: u64,
        count: u32,
    },
}
fn inspect_op<'a>(bytes: &'a [u8], dense_m: Option<u16>) -> Result<BorrowedOp<'a>, EngineError> {
    let mut cursor = Cursor { bytes, pos: 0 };
    let tag = OpTag::from_u8(cursor.u8()?).ok_or_else(|| corrupt("unknown WAL operation"))?;
    let mut out = ReplaySummary {
        operations: 1,
        wire_bytes: bytes.len(),
        ..Default::default()
    };
    let node = matches!(tag, OpTag::UpsertNode);
    if matches!(tag, OpTag::UpsertNode | OpTag::UpsertEdge) {
        let id = cursor.u64()?;
        let labels = if node {
            let count = cursor.u8()? as usize;
            if !(1..=10).contains(&count) {
                return Err(corrupt("invalid node label count"));
            }
            let mut previous = 0;
            for _ in 0..count {
                let id = cursor.u32()?;
                if id <= previous {
                    return Err(corrupt("node labels are not canonical"));
                }
                previous = id;
            }
            count
        } else {
            cursor.take(16)?;
            cursor.u32()?;
            1
        };
        let key = if node { cursor.string()? } else { "" };
        cursor.take(20)?;
        let props = cursor.blob()?;
        let inspected = messagepack::inspect_properties(props).map_err(payload)?;
        let properties = root_properties(props)?;
        let mut dense = 0;
        let mut sparse = 0;
        let mut dense_present = false;
        let mut sparse_present = false;
        if node && cursor.pos != bytes.len() {
            let flags = cursor.u8()?;
            if flags & !3 != 0 {
                return Err(corrupt("invalid vector flags"));
            }
            if flags & 1 != 0 {
                dense_present = true;
                dense = cursor.u32()? as usize;
                cursor.take(
                    dense
                        .checked_mul(4)
                        .ok_or_else(|| corrupt("dense vector extent overflow"))?,
                )?;
            }
            if flags & 2 != 0 {
                sparse_present = true;
                sparse = cursor.u32()? as usize;
                cursor.take(
                    sparse
                        .checked_mul(8)
                        .ok_or_else(|| corrupt("sparse vector extent overflow"))?,
                )?;
            }
        } else if !node {
            cursor.take(16)?;
        }
        cursor.end()?;
        out.records = RecordSummary {
            nodes: usize::from(node),
            edges: usize::from(!node),
            node_labels: if node { labels } else { 0 },
            key_bytes: key.len(),
            key_membership_bytes: key
                .len()
                .checked_mul(labels)
                .ok_or_else(|| limit("wire_extent", usize::MAX, usize::MAX))?,
            node_wire: if node { bytes.len() } else { 0 },
            edge_wire: if node { 0 } else { bytes.len() },
            properties,
            property_values: inspected.values,
            max_containers: inspected.max_containers,
            label_properties: properties
                .checked_mul(labels)
                .ok_or_else(|| limit("wire_extent", usize::MAX, usize::MAX))?,
            property_key_bytes: inspected.property_key_bytes,
            label_property_key_bytes: inspected
                .property_key_bytes
                .checked_mul(labels)
                .ok_or_else(|| limit("wire_extent", usize::MAX, usize::MAX))?,
            dense_points: usize::from(dense_present),
            dense_values: dense,
            sparse_points: usize::from(sparse_present),
            sparse_entries: sparse,
            max_record_wire: bytes.len(),
        };
        if dense_present {
            out.dense_level_slots = dense_m.map_or(33, |m| {
                usize::from(crate::dense_hnsw::storage_bound_level(id, m)) + 1
            });
        }
        return Ok(BorrowedOp::Normal(out));
    }
    match tag {
        OpTag::DeleteNode | OpTag::DeleteEdge => {
            cursor.u64()?;
            cursor.take(8)?;
            cursor.end()?;
            out.node_deletions = usize::from(matches!(tag, OpTag::DeleteNode));
            out.edge_deletions = usize::from(matches!(tag, OpTag::DeleteEdge));
        }
        OpTag::EnsureNodeLabel | OpTag::EnsureEdgeLabel => {
            let name = cursor.string()?;
            let id = cursor.u32()?;
            cursor.end()?;
            out.label_tokens = 1;
            out.label_name_bytes = name.len();
            return Ok(BorrowedOp::Token {
                node: matches!(tag, OpTag::EnsureNodeLabel),
                name,
                id,
                summary: out,
            });
        }
        OpTag::BeginAtomicBatch | OpTag::CommitAtomicBatch => {
            let first = cursor.u64()?;
            let count = cursor.u32()?;
            cursor.end()?;
            return Ok(if matches!(tag, OpTag::BeginAtomicBatch) {
                BorrowedOp::Begin { first, count }
            } else {
                BorrowedOp::Commit { first, count }
            });
        }
        _ => unreachable!(),
    }
    Ok(BorrowedOp::Normal(out))
}

struct Batch {
    first: u64,
    count: u32,
    read: u32,
    summary: ReplaySummary,
}
impl PreflightJob {
    fn frame(&mut self, file: usize, position: u64) -> Result<Option<(u64, u64)>, EngineError> {
        let len = self.files[file].len;
        if len.saturating_sub(position) < 8 {
            return Ok(None);
        }
        let mut header = [0u8; 8];
        self.files[file].file.seek(SeekFrom::Start(position))?;
        self.files[file].file.read_exact(&mut header)?;
        let extent = u32::from_le_bytes(header[..4].try_into().unwrap()) as usize;
        if extent == 0 || extent > 64 * 1024 * 1024 {
            return Ok(None);
        }
        let end = position
            .checked_add(8)
            .and_then(|n| n.checked_add(extent as u64))
            .ok_or_else(|| corrupt("WAL frame extent overflow"))?;
        if end > len {
            return Ok(None);
        }

        if extent > self.wire.capacity() {
            self.wire
                .try_reserve_exact(extent.saturating_sub(self.wire.len()))
                .map_err(|_| limit("allocation_capacity", extent, isize::MAX as usize))?;
        }
        self.wire.resize(extent, 0);
        self.files[file].file.read_exact(&mut self.wire)?;
        if crc32fast::hash(&self.wire) != u32::from_le_bytes(header[4..].try_into().unwrap())
            || extent < 8
        {
            return Ok(None);
        }
        let seq = u64::from_le_bytes(self.wire[..8].try_into().unwrap());
        Ok(Some((end, seq)))
    }
    fn scan_wal(
        &mut self,
        file: usize,
        dense_m: Option<u16>,
    ) -> Result<(u64, ReplaySummary, ReplaySummary), EngineError> {
        let len = self.files[file].len;
        if len == 0 {
            return Ok((0, ReplaySummary::default(), ReplaySummary::default()));
        }
        if len < 8 {
            return Err(corrupt("WAL file too small for header"));
        }
        self.files[file].file.seek(SeekFrom::Start(0))?;
        let mut header = [0u8; 8];
        self.files[file].file.read_exact(&mut header)?;
        if &header[..4] != b"OVGR" || u32::from_le_bytes(header[4..].try_into().unwrap()) != 3 {
            return Err(corrupt("invalid WAL version or header"));
        }
        let mut position = 8;
        let mut durable = 8;
        let mut replay = ReplaySummary::default();
        let mut decoded = ReplaySummary::default();
        let mut batch: Option<Batch> = None;
        while let Some((end, seq)) = self.frame(file, position)? {
            let recognized = self.wire.get(8).and_then(|tag| OpTag::from_u8(*tag));
            let op = match inspect_op(&self.wire[8..], dense_m) {
                Ok(op) => op,
                Err(error @ EngineError::ResourceLimit { .. }) => return Err(error),
                Err(_)
                    if matches!(
                        recognized,
                        Some(OpTag::BeginAtomicBatch | OpTag::CommitAtomicBatch)
                    ) || batch.is_some()
                        || recognized.is_none() =>
                {
                    break
                }
                Err(error) => return Err(error),
            };
            match op {
                BorrowedOp::Begin { first, count } => {
                    if batch.is_some() || first == 0 || count < 2 {
                        break;
                    }
                    batch = Some(Batch {
                        first,
                        count,
                        read: 0,
                        summary: ReplaySummary::default(),
                    });
                    decoded.markers = decoded
                        .markers
                        .checked_add(1)
                        .ok_or_else(|| limit("wire_extent", usize::MAX, usize::MAX))?;
                }
                BorrowedOp::Commit { first, count } => {
                    let Some(open) = batch.as_ref() else { break };
                    if open.first != first || open.count != count || open.read != count {
                        break;
                    }
                    let mut summary = batch.take().unwrap().summary;
                    summary.max_batch_wire_bytes = summary.wire_bytes;
                    summary.max_batch_operations = summary.operations;
                    replay = replay.add(summary)?;
                    durable = end;
                    decoded.markers = decoded
                        .markers
                        .checked_add(1)
                        .ok_or_else(|| limit("wire_extent", usize::MAX, usize::MAX))?;
                }
                BorrowedOp::Normal(summary) | BorrowedOp::Token { summary, .. } => {
                    if let Some(open) = batch.as_mut() {
                        if open.read >= open.count
                            || open.first.checked_add(open.read as u64) != Some(seq)
                        {
                            break;
                        }
                        open.read += 1;
                        open.summary = open.summary.add(summary)?;
                    } else {
                        replay = replay.add(summary)?;
                        durable = end;
                    }
                    decoded = decoded.add(summary)?;
                }
            }
            position = end;
        }
        Ok((durable, replay, decoded))
    }
    fn validate_replay(
        &mut self,
        file: usize,
        durable: u64,
        dense: Option<&crate::types::DenseVectorConfig>,
    ) -> Result<(), EngineError> {
        let mut position = 8;
        let mut previous_seq = None;
        while position < durable {
            let Some((end, seq)) = self.frame(file, position)? else {
                return Err(corrupt("WAL changed during bounded preflight"));
            };
            if end > durable {
                return Err(corrupt("WAL durable boundary changed during preflight"));
            }
            let bytes = &self.wire[8..];
            let mut cursor = Cursor { bytes, pos: 0 };
            let tag = OpTag::from_u8(cursor.u8()?)
                .ok_or_else(|| corrupt("unknown durable WAL operation"))?;
            if !matches!(tag, OpTag::BeginAtomicBatch | OpTag::CommitAtomicBatch) {
                if previous_seq.is_some_and(|previous| seq <= previous) {
                    return Err(corrupt("nonmonotonic durable WAL operation sequence"));
                }
                previous_seq = Some(seq);
            }
            match tag {
                OpTag::EnsureNodeLabel | OpTag::EnsureEdgeLabel => {
                    // Token names have the existing <=255B validity boundary;
                    // these two fixed transfer strings stay below64KiB.
                    let borrowed = cursor.string()?;
                    let id = cursor.u32()?;
                    crate::types::validate_label_token_name(borrowed)
                        .map_err(|_| corrupt("invalid replay label token name"))?;
                    let name = borrowed.to_owned();
                    let namespace = usize::from(matches!(tag, OpTag::EnsureEdgeLabel));
                    if id == 0 {
                        return Err(corrupt("reserved replay label token id"));
                    }
                    if let Some(&previous) = self.token_names[namespace].get(&name) {
                        if previous != id {
                            return Err(corrupt("replay label token name conflict"));
                        }
                    } else {
                        if self.token_ids[namespace].contains_key(&id) {
                            return Err(corrupt("replay label token id conflict"));
                        }
                        self.token_names[namespace].insert(name.clone(), id);
                        self.token_ids[namespace].insert(id, name);
                    }
                }
                OpTag::UpsertNode => {
                    cursor.u64()?;
                    let count = cursor.u8()? as usize;
                    let mut missing_label = false;
                    for _ in 0..count {
                        missing_label |= !self.token_ids[0].contains_key(&cursor.u32()?);
                    }
                    cursor.string()?;
                    cursor.take(20)?;
                    cursor.blob()?;
                    if cursor.pos != bytes.len() {
                        let flags = cursor.u8()?;
                        if flags & 1 != 0 {
                            let count = cursor.u32()? as usize;
                            let config = dense.ok_or_else(|| {
                                corrupt("invalid vector payload: dense replay is not configured")
                            })?;
                            if count != config.dimension as usize {
                                return Err(corrupt(
                                    "invalid vector payload: dense replay dimension mismatch",
                                ));
                            }
                            for values in cursor
                                .take(
                                    count
                                        .checked_mul(4)
                                        .ok_or_else(|| corrupt("dense extent overflow"))?,
                                )?
                                .chunks_exact(4)
                            {
                                if !f32::from_le_bytes(values.try_into().unwrap()).is_finite() {
                                    return Err(corrupt(
                                        "invalid vector payload: non-finite dense replay value",
                                    ));
                                }
                            }
                        }
                        if flags & 2 != 0 {
                            let count = cursor.u32()? as usize;
                            let encoded = cursor.take(
                                count
                                    .checked_mul(8)
                                    .ok_or_else(|| corrupt("sparse extent overflow"))?,
                            )?;
                            if count > self.sparse.capacity() {
                                let additional = bounds::array_bytes::<(u32, f32)>(count)
                                    .map_err(|e| bounded(e, isize::MAX as usize))?;
                                self.sparse
                                    .try_reserve_exact(count.saturating_sub(self.sparse.len()))
                                    .map_err(|_| {
                                        limit(
                                            "allocation_capacity",
                                            additional,
                                            isize::MAX as usize,
                                        )
                                    })?;
                            }
                            self.sparse.clear();
                            for entry in encoded.chunks_exact(8) {
                                let id = u32::from_le_bytes(entry[..4].try_into().unwrap());
                                let value = f32::from_le_bytes(entry[4..].try_into().unwrap());
                                if !value.is_finite() || value < 0.0 {
                                    return Err(corrupt(
                                        "invalid vector payload: invalid sparse replay weight",
                                    ));
                                }
                                self.sparse.push((id, value));
                            }
                            // Same unstable dimension sort and same f32 duplicate
                            // addition as canonicalize_sparse_vector_entries.
                            self.sparse.sort_unstable_by_key(|&(id, _)| id);
                            let mut previous = None;
                            let mut total = 0.0f32;
                            for &(id, value) in &self.sparse {
                                if previous == Some(id) {
                                    total += value;
                                } else {
                                    previous = Some(id);
                                    total = value;
                                }
                                if !total.is_finite() {
                                    return Err(corrupt(
                                        "sparse replay duplicate sum is non-finite",
                                    ));
                                }
                            }
                        }
                    }
                    // Released replay validates vector semantics before token
                    // visibility, even when a malformed record violates both.
                    if missing_label {
                        return Err(corrupt("node references missing replay label"));
                    }
                }
                OpTag::UpsertEdge => {
                    cursor.take(24)?;
                    if !self.token_ids[1].contains_key(&cursor.u32()?) {
                        return Err(corrupt("edge references missing replay label"));
                    }
                }
                _ => {}
            }
            position = end;
        }
        Ok(())
    }
}

impl PreflightPermit {
    pub(super) fn into_startup(
        mut self,
    ) -> (
        ManifestState,
        Vec<crate::segment_reader::SegmentReader>,
        DirectoryAdmission,
    ) {
        let index = self.owner.selected.expect("selected manifest");
        let manifest = std::mem::replace(
            &mut self.owner.candidates[index].manifest,
            super::payload::Owned::new(crate::manifest::default_manifest()),
        )
        .into_inner();
        let readers = std::mem::take(&mut self.owner.readers)
            .into_iter()
            .map(SegmentReaderOpen::into_reader)
            .collect();
        (manifest, readers, self.admission)
    }
    pub(super) fn summary(&self) -> PreflightSummary {
        self.summary
    }
    pub(super) fn manifest(&self) -> &ManifestState {
        let job = &self.owner;
        &job.candidates[job.selected.expect("selected manifest")].manifest
    }
    pub(super) fn wal(&self) -> &[WalSummary] {
        &self.owner.wal
    }

    pub(super) fn admission(&self) -> &DirectoryAdmission {
        &self.admission
    }
    /// Reopen each pathname, not just the retained handle: replacement, growth,
    /// removal or a same-length edit invalidates the authoritative selection.
    pub(super) fn revalidate(&self) -> Result<(), EngineError> {
        #[cfg(test)]
        {
            note_preflight_work(2);
            let hook = BEFORE_REVALIDATE.with(|slot| slot.borrow_mut().take());
            if let Some(hook) = hook {
                hook();
            }
        }
        if let Some(identity) = &self.owner.identity {
            let path = &self.owner.directory;
            if super::directory_identity::DirectoryPin::open(path)?.identity()
                != identity.identity()
            {
                return Err(EngineError::InvalidOperation(
                    "preflight directory identity changed before recovery".into(),
                ));
            }
        }
        for path in &self.owner.absent {
            match fs::metadata(path) {
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                Err(error) => return Err(error.into()),
                Ok(_) => {
                    return Err(EngineError::InvalidOperation(
                        "preflight missing source appeared before recovery".into(),
                    ))
                }
            }
        }
        for witness in &self.owner.files {
            let mut file = File::open(&witness.path)?;
            if file.metadata()?.len() != witness.len
                || fingerprint(&mut file, witness.len)? != witness.digest
            {
                return Err(EngineError::InvalidOperation(
                    "preflight source changed before recovery".into(),
                ));
            }
        }
        Ok(())
    }
}

fn limit(resource: &'static str, requested: usize, capacity: usize) -> EngineError {
    EngineError::ResourceLimit {
        resource,
        requested: requested as u64,
        limit: capacity as u64,
        retryable: false,
    }
}
fn corrupt(message: &'static str) -> EngineError {
    EngineError::CorruptWal(message.into())
}
fn bounded(_error: BoundError, capacity: usize) -> EngineError {
    limit("wire_extent", usize::MAX, capacity)
}
fn payload(error: PayloadError) -> EngineError {
    match error {
        PayloadError::Resource {
            resource,
            requested,
            limit: capacity,
            retryable,
        } => EngineError::ResourceLimit {
            resource,
            requested: requested as u64,
            limit: capacity as u64,
            retryable,
        },
        PayloadError::Closed => EngineError::InvalidOperation("closed preflight domain".into()),
        _ => corrupt("invalid property encoding in WAL"),
    }
}
fn fingerprint(file: &mut File, extent: u64) -> Result<[u8; 32], EngineError> {
    #[cfg(test)]
    note_preflight_work(1);
    file.seek(SeekFrom::Start(0))?;
    let mut digest = Sha256::new();
    let mut remaining = extent;
    let mut scratch = [0u8; 8192];
    while remaining != 0 {
        let count = scratch.len().min(remaining as usize);
        file.read_exact(&mut scratch[..count])?;
        digest.update(&scratch[..count]);
        remaining -= count as u64;
    }
    if file.read(&mut [0u8; 1])? != 0 {
        return Err(EngineError::InvalidOperation(
            "preflight extent grew while reading".into(),
        ));
    }
    Ok(digest.finalize().into())
}

impl PreflightJob {
    fn witness(&mut self, path: &Path) -> Result<Option<usize>, EngineError> {
        if let Some(&index) = self.file_ids.get(path) {
            return Ok(Some(index));
        }
        // A path joins four simultaneous bounded constructor buffers in the real
        // component reader. Preserve that existing controlled-transfer boundary.
        if path.as_os_str().len() > crate::segment_reader::RETIREMENT_OUTPUT_PATH_LIMIT {
            return Err(limit(
                "allocation_capacity",
                path.as_os_str().len(),
                crate::segment_reader::RETIREMENT_OUTPUT_PATH_LIMIT,
            ));
        }
        let file = match File::open(path) {
            Ok(file) => file,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                if self.absent.len() == self.absent.capacity() {
                    let next = self
                        .absent
                        .capacity()
                        .max(2)
                        .checked_mul(2)
                        .ok_or_else(|| {
                            limit("allocation_capacity", usize::MAX, isize::MAX as usize)
                        })?;
                    self.absent
                        .try_reserve_exact(next - self.absent.len())
                        .map_err(|_| {
                            limit("allocation_capacity", usize::MAX, isize::MAX as usize)
                        })?;
                }

                self.absent.push(path.to_owned());
                return Ok(None);
            }
            Err(error) => return Err(error.into()),
        };
        let metadata = file.metadata()?;
        if !metadata.is_file() {
            return Err(EngineError::InvalidOperation(
                "preflight component is not a regular file".into(),
            ));
        }
        let len = metadata.len();
        if self.file_ids.len() == self.file_ids.capacity() {
            let next = self
                .file_ids
                .capacity()
                .max(2)
                .checked_mul(2)
                .ok_or_else(|| limit("allocation_capacity", usize::MAX, isize::MAX as usize))?;

            self.file_ids
                .try_reserve(next - self.file_ids.len())
                .map_err(|_| limit("allocation_capacity", usize::MAX, isize::MAX as usize))?;
        }
        if self.files.len() == self.files.capacity() {
            let capacity = self
                .files
                .capacity()
                .max(2)
                .checked_mul(2)
                .ok_or_else(|| limit("allocation_capacity", usize::MAX, isize::MAX as usize))?;
            self.files
                .try_reserve_exact(capacity - self.files.len())
                .map_err(|_| limit("allocation_capacity", usize::MAX, isize::MAX as usize))?;
        }

        let index = self.files.len();
        self.files.push(FileWitness {
            path: path.to_owned(),
            file,
            len,
            digest: [0; 32],
        });
        self.file_ids.insert(path.to_owned(), index);
        let witness = &mut self.files[index];
        witness.digest = fingerprint(&mut witness.file, len)?;
        #[cfg(test)]
        note_preflight_work(0);
        Ok(Some(index))
    }
    fn read_wire(&mut self, witness: usize, maximum: usize) -> Result<(), EngineError> {
        let extent = usize::try_from(self.files[witness].len)
            .map_err(|_| limit("allocation_capacity", usize::MAX, maximum))?;
        if extent > maximum {
            return Err(limit("allocation_capacity", extent, maximum));
        }
        if extent > self.wire.capacity() {
            // Charge the new backing while the old backing is still live.

            self.wire
                .try_reserve_exact(extent.saturating_sub(self.wire.len()))
                .map_err(|_| limit("allocation_capacity", extent, maximum))?;
        }
        self.wire.resize(extent, 0);
        let file = &mut self.files[witness].file;
        file.seek(SeekFrom::Start(0))?;
        file.read_exact(&mut self.wire)?;
        if file.read(&mut [0u8; 1])? != 0 {
            return Err(EngineError::InvalidOperation(
                "preflight extent grew while reading".into(),
            ));
        }
        Ok(())
    }
    fn select(
        &mut self,
        path: &Path,
        options: &DbOptions,
    ) -> Result<(ManifestSource, bool), EngineError> {
        self.candidates
            .try_reserve_exact(4)
            .map_err(|_| limit("allocation_capacity", usize::MAX, isize::MAX as usize))?;
        for (filename, source) in [
            (MANIFEST_CURRENT, ManifestSource::Current),
            (MANIFEST_TMP, ManifestSource::Temporary),
            (MANIFEST_PREV, ManifestSource::Previous),
        ] {
            let Some(file) = self.witness(&path.join(filename))? else {
                continue;
            };
            self.read_wire(file, isize::MAX as usize)?;
            let input = std::mem::take(&mut self.wire);
            self.candidates.push(JsonJob::default());
            let index = self.candidates.len() - 1;
            let result = self.candidates[index].decode_manifest(input);
            match result {
                Err(PayloadError::Wire) => continue, // Same current/tmp/prev grammar fallback.
                Err(PayloadError::Resource { requested, .. }) => {
                    // Report actual addressability/allocation failure.
                    return Err(limit("allocation_capacity", requested, isize::MAX as usize));
                }
                Err(error) => return Err(payload(error)), // Capacity never falls back.
                Ok(()) => {
                    self.selected = Some(index);
                    return Ok((source, self.validate_selected(options)?));
                }
            }
        }
        // Match EngineCore's manifestless-artifact refusal without allocating
        // an unbounded list of filenames or touching recovery artifacts.
        if path.exists() {
            for entry in fs::read_dir(path)? {
                let entry = entry?;
                let name = entry.file_name();
                let name = name.to_string_lossy();
                if (name.starts_with("wal_") && name.ends_with(".wal"))
                    || name == "data.wal"
                    || name == "segments"
                {
                    return Err(EngineError::ManifestError(
                        "database artifacts exist without a valid manifest label-token schema"
                            .into(),
                    ));
                }
            }
        }
        self.candidates.push(JsonJob::default());
        let index = self.candidates.len() - 1;
        *self.candidates[index].manifest = crate::manifest::default_manifest();
        self.selected = Some(index);
        Ok((ManifestSource::Empty, self.validate_selected(options)?))
    }
    fn validate_selected(&mut self, options: &DbOptions) -> Result<bool, EngineError> {
        let index = self.selected.unwrap();
        let state = &self.candidates[index].manifest;
        // Bound input-derived diagnostics before existing validators format an
        // invalid token/key. These are existing validity limits, not new caps.
        for name in state
            .node_label_tokens
            .keys()
            .chain(state.edge_label_tokens.keys())
        {
            if name.len() > 255 {
                return Err(EngineError::ManifestError(
                    "label token name exceeds 255 UTF-8 bytes".into(),
                ));
            }
        }
        for key in state
            .node_schemas
            .iter()
            .flat_map(|s| s.properties.keys())
            .chain(state.edge_schemas.iter().flat_map(|s| s.properties.keys()))
        {
            if key.len() > crate::schema::MAX_SCHEMA_PROPERTY_KEY_BYTES {
                return Err(EngineError::ManifestError(
                    "schema property key exceeds existing UTF-8 byte limit".into(),
                ));
            }
        }
        let state = &mut self.candidates[index].manifest;
        if state.label_token_schema_version != crate::types::LABEL_TOKEN_SCHEMA_VERSION {
            return Err(EngineError::ManifestError(
                "unsupported manifest label token schema".into(),
            ));
        }
        // The same token predicates as manifest::validate_token_namespace, with
        // the ID maps retained in this local parse state.
        for (namespace, (tokens, next_id)) in [
            (&state.node_label_tokens, state.next_node_label_id),
            (&state.edge_label_tokens, state.next_edge_label_id),
        ]
        .into_iter()
        .enumerate()
        {
            let ids = &mut self.token_ids[namespace];
            let names = &mut self.token_names[namespace];
            let mut max_id = 0;
            for (name, &id) in tokens {
                crate::types::validate_label_token_name(name)?;
                if id == 0 || ids.insert(id, name.clone()).is_some() {
                    return Err(EngineError::ManifestError(
                        "reserved or duplicate manifest label token id".into(),
                    ));
                }
                names.insert(name.clone(), id);
                max_id = max_id.max(id);
            }
            if next_id <= max_id {
                return Err(EngineError::ManifestError(
                    "next label token id does not exceed assigned ids".into(),
                ));
            }
        }
        // Suppress only the existing duplicate-target formatter's potentially
        // unbounded input text. The actual validator still checks all identities.
        for (ordinal, entry) in state.secondary_indexes.iter().enumerate() {
            let (fields, len) = field_refs(&entry.target)?;
            for index in 0..len {
                if fields[..index].contains(&fields[index]) {
                    return Err(EngineError::ManifestError(
                        "duplicate secondary index field".into(),
                    ));
                }
            }
            let mut digest = Sha256::new();
            digest.update([
                u8::from(matches!(
                    entry.target,
                    crate::types::SecondaryIndexTarget::EdgeProperty { .. }
                        | crate::types::SecondaryIndexTarget::EdgeFieldIndex { .. }
                )),
                u8::from(matches!(
                    entry.kind,
                    crate::types::SecondaryIndexKind::Range
                )),
            ]);
            digest.update(entry.target.label_id().to_le_bytes());
            for field in fields[..len].iter().flatten() {
                field.digest(&mut digest);
            }
            let key: [u8; 32] = digest.finalize().into();
            let previous = self.declarations.entry(key).or_default();
            for &old in previous.iter() {
                let earlier = &state.secondary_indexes[old];
                let (old_fields, old_len) = field_refs(&earlier.target)?;
                if old_len == len
                    && old_fields[..old_len] == fields[..len]
                    && earlier.kind == entry.kind
                    && earlier.target.target_kind() == entry.target.target_kind()
                    && earlier.target.label_id() == entry.target.label_id()
                {
                    return Err(EngineError::ManifestError(
                        "duplicate secondary index declaration".into(),
                    ));
                }
            }
            previous.push(ordinal);
        }
        let mut normalized = super::reconcile_dense_vector_manifest(state, options)?;
        normalized |= crate::schema::normalize_schema_manifest(state)?;
        crate::manifest::validate_manifest_body_identity_with_scratch(state, &mut self.validation)?;
        // Validation above reused the real scratch-backed declaration validator.
        // The existing secondary normalizer's remaining change is numeric only.
        let next = state
            .secondary_indexes
            .iter()
            .map(|entry| entry.index_id)
            .max()
            .unwrap_or(0)
            .saturating_add(1)
            .max(state.next_secondary_index_id)
            .max(1);
        normalized |= next != state.next_secondary_index_id;
        state.next_secondary_index_id = next;
        let maximum_generation = state
            .pending_flush_epochs
            .iter()
            .map(|epoch| epoch.wal_generation_id)
            .fold(state.active_wal_generation_id, u64::max);
        let next_generation = maximum_generation
            .checked_add(1)
            .ok_or_else(|| {
                EngineError::InvalidOperation(
                    "WAL generation identity exhausted before recovery".into(),
                )
            })?
            .max(state.next_wal_generation_id);
        normalized |= next_generation != state.next_wal_generation_id;
        state.next_wal_generation_id = next_generation;
        Ok(normalized)
    }
}

/// Private a3 entry point. The supplied session or creation gate is consumed;
/// refusal destroys local parse state and releases directory admission.
/// This never calls mutating engine open, promotion, normalization writes or WAL
/// truncation. The consumer must revalidate the witnessed permit immediately
/// before startup mutation; production's sole consumer is finish_candidate_open.
pub(super) fn scan(
    path: &Path,
    database: &DbOptions,
    options: &ConcurrencyOptions,
    domain: &DomainHandle,
    admission: DirectoryAdmission,
) -> Result<PreflightPermit, EngineError> {
    options.validate()?;
    match &admission {
        DirectoryAdmission::Existing(session) => {
            if !session.matches_domain(domain) {
                return Err(EngineError::InvalidOperation(
                    "preflight admission belongs to another domain".into(),
                ));
            }
            if session.identity()
                != Some(super::directory_identity::DirectoryPin::open(path)?.identity())
            {
                return Err(EngineError::InvalidOperation(
                    "preflight path does not match admission".into(),
                ));
            }
        }
        DirectoryAdmission::Missing { parent, .. } => {
            if path.exists() || !parent.metadata()?.is_dir() {
                return Err(EngineError::InvalidOperation(
                    "missing path changed under creation admission".into(),
                ));
            }
            if !database.create_if_missing {
                return Err(EngineError::DatabaseNotFound(
                    "preflight directory does not exist".into(),
                ));
            }
        }
    }
    let mut job = PreflightJob {
        directory: path.to_owned(),
        ..Default::default()
    };
    if path.exists() {
        job.identity = Some(super::directory_identity::DirectoryPin::open(path)?);
    } else {
        job.absent.push(path.to_owned());
    }
    let (source, normalized) = job.select(path, database)?;
    let selected = job.selected.expect("selected manifest");
    let segment_count = job.candidates[selected].manifest.segments.len();
    for ordinal in (0..segment_count).rev() {
        let state = &job.candidates[selected].manifest;
        let info = state.segments[ordinal].clone();
        let directory = crate::segment_writer::segment_dir(path, info.id);
        if !directory.exists() {
            if state.pending_flush_epochs.iter().any(|e| {
                e.state == FlushEpochState::PublishedPendingRetire && e.segment_id == Some(info.id)
            }) {
                return Err(EngineError::InvalidOperation(
                    "published segment missing (published pending-retire segment missing)".into(),
                ));
            }
            job.witness(&directory)?;
            continue;
        }
        let component_path =
            directory.join(crate::segment_components::SEGMENT_COMPONENT_MANIFEST_FILENAME);
        job.witness(&component_path)?.ok_or_else(|| {
            EngineError::InvalidOperation("listed component manifest missing".into())
        })?;
        let mut component = crate::segment_components::retained_manifest::Open::default();
        component.read_bounded(&component_path, None)?;
        for record in &component.manifest().components {
            if let crate::segment_components::ComponentHandleV1::ExternalFile {
                relative_path,
                ..
            } = &record.handle
            {
                crate::segment_components::validate_relative_component_path(relative_path)?;
                job.witness(&directory.join(relative_path))?;
            }
        }
        let index = job.readers.len();
        job.readers.push(SegmentReaderOpen::default());
        let state = &job.candidates[selected].manifest;
        job.readers[index].open(
            &directory,
            &info,
            state.dense_vector.as_ref(),
            &state.secondary_indexes,
        )?;
        let reader = job.readers[index].reader();
        if reader.node_count() != component.manifest().node_count
            || reader.edge_count() != component.manifest().edge_count
        {
            return Err(EngineError::CorruptRecord(
                "record counts disagree with component identity".into(),
            ));
        }
        for entry in &state.secondary_indexes {
            reader.warm_declared_index_runtime_coverage(entry);
        }
        job.components.push(component);
    }
    let state = &job.candidates[selected].manifest;
    for epoch in &state.pending_flush_epochs {
        if epoch.state == FlushEpochState::FrozenPendingFlush {
            job.wal.push(WalSummary {
                generation: epoch.wal_generation_id,
                frozen: true,
                durable_len: 0,
                file_len: 0,
                replay: ReplaySummary::default(),
                decoded: ReplaySummary::default(),
            });
        } else if !state
            .segments
            .iter()
            .any(|info| Some(info.id) == epoch.segment_id)
        {
            return Err(EngineError::InvalidOperation(
                "pending-retire epoch has no listed segment".into(),
            ));
        }
    }
    job.wal.sort_unstable_by_key(|w| w.generation);
    job.wal.push(WalSummary {
        generation: state.active_wal_generation_id,
        frozen: false,
        durable_len: 0,
        file_len: 0,
        replay: ReplaySummary::default(),
        decoded: ReplaySummary::default(),
    });
    let retired: Vec<_> = state
        .pending_flush_epochs
        .iter()
        .filter(|e| e.state == FlushEpochState::PublishedPendingRetire)
        .map(|e| e.wal_generation_id)
        .collect();
    for generation in retired {
        job.witness(&crate::wal::wal_generation_path(path, generation))?;
    }
    let dense = job.candidates[selected].manifest.dense_vector.clone();
    for index in 0..job.wal.len() {
        let generation = job.wal[index].generation;
        let file = job.witness(&crate::wal::wal_generation_path(path, generation))?;
        let (durable, replay, decoded) = if let Some(file) = file {
            let result = job.scan_wal(file, dense.as_ref().map(|c| c.hnsw.m))?;
            job.validate_replay(file, result.0, dense.as_ref())?;
            result
        } else {
            (0, ReplaySummary::default(), ReplaySummary::default())
        };
        job.wal[index].durable_len = durable;
        job.wal[index].file_len = file.map_or(0, |i| job.files[i].len);
        job.wal[index].replay = replay;
        job.wal[index].decoded = decoded;
    }
    let frozen_generations = job.wal.len() - 1;
    let permit = PreflightPermit {
        owner: job,
        summary: PreflightSummary {
            source,
            normalized_in_memory: normalized,
            segments: segment_count,
            frozen_generations,
        },
        admission,
    };
    Ok(permit)
}

fn field_refs(
    target: &crate::types::SecondaryIndexTarget,
) -> Result<([Option<FieldRef<'_>>; 8], usize), EngineError> {
    use crate::types::{SecondaryIndexFieldManifest as F, SecondaryIndexTarget as T};
    let mut refs = [None; 8];
    let count = match target {
        T::NodeProperty { prop_key, .. } | T::EdgeProperty { prop_key, .. } => {
            refs[0] = Some(FieldRef::Property(prop_key));
            1
        }
        T::NodeFieldIndex { fields, .. } | T::EdgeFieldIndex { fields, .. } => {
            if fields.is_empty() || fields.len() > 8 {
                return Err(EngineError::ManifestError(
                    "secondary index field count is outside existing 1..=8 limit".into(),
                ));
            }
            for (out, field) in refs.iter_mut().zip(fields) {
                *out = Some(match field {
                    F::Property { key } => FieldRef::Property(key),
                    F::NodeMetadata { field } => FieldRef::Node((*field).into()),
                    F::EdgeMetadata { field } => FieldRef::Edge((*field).into()),
                });
            }
            fields.len()
        }
    };
    Ok((refs, count))
}
#[derive(Clone, Copy, PartialEq, Eq)]
enum FieldRef<'a> {
    Property(&'a str),
    Node(crate::types::NodeMetadataIndexField),
    Edge(crate::types::EdgeMetadataIndexField),
}
impl FieldRef<'_> {
    fn digest(self, digest: &mut Sha256) {
        match self {
            Self::Property(key) => {
                digest.update([0]);
                digest.update(key.len().to_le_bytes());
                digest.update(key.as_bytes());
            }
            Self::Node(field) => {
                digest.update([1]);
                digest.update(crate::types::node_metadata_index_field_name(field).as_bytes());
            }
            Self::Edge(field) => {
                digest.update([2]);
                digest.update(crate::types::edge_metadata_index_field_name(field).as_bytes());
            }
        }
    }
}

#[cfg(test)]
mod practical_preflight_tests {
    use super::*;
    fn configured(
        path: &Path,
    ) -> (
        super::super::reclamation::ConfiguredDomain,
        DirectoryAdmission,
    ) {
        let configured = super::super::reclamation::ReclaimDomain::configured(
            &ConcurrencyOptions::default(),
            Some(super::super::directory_identity::DirectoryPin::open(path).unwrap()),
        )
        .unwrap();
        let session = configured
            .registration
            .bind_directory(configured.domain.clone(), &ConcurrencyOptions::default())
            .unwrap();
        (configured, DirectoryAdmission::Existing(session))
    }
    fn files(path: &Path) -> Vec<(PathBuf, Vec<u8>)> {
        let mut out = Vec::new();
        for e in fs::read_dir(path).unwrap() {
            let p = e.unwrap().path();
            if p.is_file() {
                out.push((p.clone(), fs::read(p).unwrap()));
            }
        }
        out.sort();
        out
    }
    #[test]
    fn p45a3_preflight_valid_large_manifest_has_no_memory_budget_and_stays_readonly() {
        let dir = tempfile::tempdir().unwrap();
        let mut manifest = crate::manifest::default_manifest();
        for n in 1..=20_000 {
            manifest.node_label_tokens.insert(format!("Node{n:06}"), n);
        }
        manifest.next_node_label_id = 20_001;
        crate::manifest::write_manifest(dir.path(), &manifest).unwrap();
        let before = files(dir.path());
        let (configured, admission) = configured(dir.path());
        let options = ConcurrencyOptions::default().with_max_batch_bytes(1024);
        let permit = scan(
            dir.path(),
            &DbOptions::default(),
            &options,
            &configured.domain,
            admission,
        )
        .unwrap();
        assert_eq!(permit.manifest().node_label_tokens.len(), 20_000);
        assert_eq!(files(dir.path()), before);
        permit.revalidate().unwrap();
        drop(permit);
        configured.domain.close();
        drop(configured);
    }
    #[test]
    fn p45a3_preflight_same_extent_change_and_foreign_domain_refuse_without_writes() {
        let dir = tempfile::tempdir().unwrap();
        crate::manifest::write_manifest(dir.path(), &crate::manifest::default_manifest()).unwrap();
        let (configured, admission) = configured(dir.path());
        let permit = scan(
            dir.path(),
            &DbOptions::default(),
            &ConcurrencyOptions::default(),
            &configured.domain,
            admission,
        )
        .unwrap();
        let file = dir.path().join(MANIFEST_CURRENT);
        let mut changed = fs::read(&file).unwrap();
        let index = changed.iter().position(|b| *b == b'1').unwrap();
        changed[index] = b'2';
        fs::write(&file, &changed).unwrap();
        assert!(permit.revalidate().is_err());
        assert_eq!(fs::read(&file).unwrap(), changed);
        drop(permit);
        configured.domain.close();
        drop(configured);
        let a = tempfile::tempdir().unwrap();
        let b = tempfile::tempdir().unwrap();
        let (one, admission) = self::configured(a.path());
        let (two, other) = self::configured(b.path());
        let result = scan(
            a.path(),
            &DbOptions::default(),
            &ConcurrencyOptions::default(),
            &two.domain,
            admission,
        );
        assert!(matches!(result, Err(EngineError::InvalidOperation(_))));
        drop(other);
        one.domain.close();
        two.domain.close();
        drop(one);
        drop(two);
    }
    #[test]
    fn p45a3_preflight_malformed_sources_preserve_bytes() {
        let dir = tempfile::tempdir().unwrap();
        fs::write(dir.path().join(MANIFEST_CURRENT), b"{malformed").unwrap();
        fs::write(dir.path().join("data.wal"), b"unowned").unwrap();
        let before = files(dir.path());
        let (configured, admission) = configured(dir.path());
        assert!(scan(
            dir.path(),
            &DbOptions::default(),
            &ConcurrencyOptions::default(),
            &configured.domain,
            admission
        )
        .is_err());
        assert_eq!(files(dir.path()), before);
        configured.domain.close();
        drop(configured);
    }
}

#[cfg(test)]
mod review_preflight_safety {
    use super::*;
    use crate::types::{
        DenseMetric, DenseVectorConfig, EdgeRecord, FlushEpochMeta, HnswConfig, NodeLabelSet,
        NodeRecord, PropValue, WalOp,
    };

    fn manifest(path: &Path) -> ManifestState {
        let mut state = crate::manifest::default_manifest();
        state.node_label_tokens.insert("known".into(), 1);
        state.next_node_label_id = 2;
        crate::manifest::write_manifest(path, &state).unwrap();
        state
    }
    fn node() -> WalOp {
        WalOp::UpsertNode(NodeRecord {
            id: 1,
            label_ids: NodeLabelSet::from_canonical_ids(&[1]).unwrap(),
            key: "key".into(),
            props: BTreeMap::new(),
            created_at: 1,
            updated_at: 2,
            weight: 1.0,
            dense_vector: None,
            sparse_vector: None,
            last_write_seq: 0,
        })
    }
    fn attempt(
        path: &Path,
    ) -> (
        super::super::reclamation::ConfiguredDomain,
        Result<PreflightPermit, EngineError>,
    ) {
        let options = ConcurrencyOptions::default();
        let configured = super::super::reclamation::ReclaimDomain::configured(
            &options,
            Some(super::super::directory_identity::DirectoryPin::open(path).unwrap()),
        )
        .unwrap();
        let session = configured
            .registration
            .bind_directory(configured.domain.clone(), &options)
            .unwrap();
        let result = scan(
            path,
            &DbOptions::default(),
            &options,
            &configured.domain,
            DirectoryAdmission::Existing(session),
        );
        (configured, result)
    }
    fn contents(path: &Path) -> BTreeMap<PathBuf, Vec<u8>> {
        fn collect(root: &Path, path: &Path, out: &mut BTreeMap<PathBuf, Vec<u8>>) {
            for entry in fs::read_dir(path).unwrap() {
                let path = entry.unwrap().path();
                if path.is_dir() {
                    out.insert(path.strip_prefix(root).unwrap().to_owned(), Vec::new());
                    collect(root, &path, out);
                } else {
                    out.insert(
                        path.strip_prefix(root).unwrap().to_owned(),
                        fs::read(path).unwrap(),
                    );
                }
            }
        }
        let mut out = BTreeMap::new();
        collect(path, path, &mut out);
        out
    }
    fn wal(path: &Path, operations: &[(u64, WalOp)]) {
        let mut writer = crate::wal::WalWriter::open_generation(path, 0).unwrap();
        writer.append_batch(operations).unwrap();
        writer.sync().unwrap();
    }

    #[test]
    fn p45a3_review_s2_preflight_atomic_torn_offsets_match_real_wal_reader_readonly() {
        let dir = tempfile::tempdir().unwrap();
        let path = crate::wal::wal_generation_path(dir.path(), 0);
        let ops = [
            (
                0,
                WalOp::BeginAtomicBatch {
                    first_seq: 10,
                    op_count: 2,
                },
            ),
            (10, node()),
            (11, node()),
            (
                0,
                WalOp::CommitAtomicBatch {
                    first_seq: 10,
                    op_count: 2,
                },
            ),
        ];
        let mut bytes = b"OVGR\x03\0\0\0".to_vec();
        let mut encoded = Vec::new();
        for (seq, op) in &ops {
            crate::wal::encode_v3_frame_into(&mut encoded, *seq, op, &mut bytes).unwrap();
        }
        for cut in 0..=bytes.len() {
            fs::write(&path, &bytes[..cut]).unwrap();
            let mut job = PreflightJob::default();
            let file = job.witness(&path).unwrap().unwrap();
            let actual = crate::wal::WalReader::read_generation_recoverable(dir.path(), 0);
            let scanned = job.scan_wal(file, None);
            match (actual, scanned) {
                (Err(_), Err(_)) => {}
                (Ok(actual), Ok((durable, replay, decoded))) => {
                    assert_eq!(durable, actual.durable_len, "cut {cut}");
                    let expected = usize::from(cut == bytes.len()) * 2;
                    assert_eq!(
                        replay.records.nodes, expected,
                        "uncommitted batch at cut {cut}"
                    );
                    assert_eq!(replay.operations, expected);
                    assert!(decoded.records.nodes >= replay.records.nodes);
                    if expected != 0 {
                        assert_eq!(replay.max_batch_operations, 2);
                    }
                }
                _ => panic!("preflight / real reader parity mismatch at cut {cut}"),
            }
            assert_eq!(
                fs::read(&path).unwrap(),
                bytes[..cut],
                "scanner mutated cut {cut}"
            );
        }
    }

    #[test]
    fn p45a3_review_s2_preflight_valid_and_malformed_vector_replay_stays_readonly() {
        // Valid duplicates/zero sparse weights, then each actual replay validation boundary.
        for case in 0..8 {
            let dir = tempfile::tempdir().unwrap();
            let mut state = manifest(dir.path());
            if case != 7 {
                state.dense_vector = Some(DenseVectorConfig {
                    dimension: 2,
                    metric: DenseMetric::Euclidean,
                    hnsw: HnswConfig {
                        m: 8,
                        ef_construction: 32,
                    },
                });
            }
            crate::manifest::write_manifest(dir.path(), &state).unwrap();
            let WalOp::UpsertNode(mut row) = node() else {
                unreachable!()
            };
            row.dense_vector = Some(vec![1.0, 2.0]);
            row.sparse_vector = Some(vec![(2, 0.5), (1, 0.0), (2, 0.5)]);
            match case {
                0 | 7 => {}
                1 => row.dense_vector = Some(vec![1.0]),
                2 => row.dense_vector = Some(vec![f32::NAN, 2.0]),
                3 => row.dense_vector = Some(vec![f32::INFINITY, 2.0]),
                4 => row.sparse_vector = Some(vec![(1, -1.0)]),
                5 => row.sparse_vector = Some(vec![(1, f32::NAN)]),
                6 => row.sparse_vector = Some(vec![(1, f32::MAX), (1, f32::MAX)]),
                _ => unreachable!(),
            }
            wal(dir.path(), &[(1, WalOp::UpsertNode(row))]);
            let before = contents(dir.path());
            let (configured, result) = attempt(dir.path());
            if case == 0 {
                let permit = result.unwrap();
                assert_eq!(permit.wal()[0].replay.records.dense_points, 1);
                assert_eq!(permit.wal()[0].replay.records.sparse_entries, 3);
                permit.revalidate().unwrap();
                drop(permit);
            } else {
                assert!(
                    matches!(result, Err(EngineError::CorruptWal(_))),
                    "vector case {case}"
                );
            }
            assert_eq!(contents(dir.path()), before, "vector case {case}");
            configured.domain.close();
            drop(configured);
        }
    }

    #[test]
    fn p45a3_review_s2_preflight_missing_published_segment_keeps_recovery_files() {
        for listed in [false, true] {
            let dir = tempfile::tempdir().unwrap();
            let mut state = manifest(dir.path());
            if listed {
                let segment = crate::segment_writer::segment_dir(dir.path(), 99);
                fs::create_dir_all(&segment).unwrap();
                let info =
                    crate::segment_writer::write_segment_with_degree_overlay_and_secondary_indexes(
                        &segment,
                        99,
                        &crate::memtable::Memtable::new(),
                        None,
                        &crate::degree_cache::DegreeOverlaySnapshot::empty(),
                        &[],
                    )
                    .unwrap();
                state.segments.push(info);
                fs::remove_dir_all(&segment).unwrap();
            }
            state.pending_flush_epochs.push(FlushEpochMeta {
                epoch_id: 1,
                wal_generation_id: 1,
                state: FlushEpochState::PublishedPendingRetire,
                segment_id: Some(99),
            });
            crate::manifest::write_manifest(dir.path(), &state).unwrap();
            fs::write(
                crate::wal::wal_generation_path(dir.path(), 1),
                b"keep retained recovery bytes",
            )
            .unwrap();
            let before = contents(dir.path());
            let (configured, result) = attempt(dir.path());
            let error = result.err().expect("missing published segment accepted");
            assert!(matches!(error, EngineError::InvalidOperation(_)));
            assert!(error.to_string().contains(if listed {
                "published segment missing (published pending-retire segment missing)"
            } else {
                "pending-retire epoch has no listed segment"
            }));
            assert_eq!(contents(dir.path()), before);
            configured.domain.close();
            drop(configured);
        }
    }
    fn full_catalog() -> Vec<crate::types::SecondaryIndexManifestEntry> {
        use crate::types::*;
        (0..8)
            .map(|family| SecondaryIndexManifestEntry {
                index_id: family + 1,
                target: match family {
                    0 | 1 => SecondaryIndexTarget::NodeProperty {
                        label_id: 1,
                        prop_key: "p".into(),
                    },
                    2 | 3 => SecondaryIndexTarget::EdgeProperty {
                        label_id: 3,
                        prop_key: "p".into(),
                    },
                    4 | 5 => SecondaryIndexTarget::NodeFieldIndex {
                        label_id: 1,
                        fields: vec![
                            SecondaryIndexFieldManifest::Property { key: "p".into() },
                            SecondaryIndexFieldManifest::NodeMetadata {
                                field: NodeMetadataIndexFieldManifest::Weight,
                            },
                        ],
                    },
                    _ => SecondaryIndexTarget::EdgeFieldIndex {
                        label_id: 3,
                        fields: vec![
                            SecondaryIndexFieldManifest::Property { key: "p".into() },
                            SecondaryIndexFieldManifest::EdgeMetadata {
                                field: EdgeMetadataIndexFieldManifest::Weight,
                            },
                        ],
                    },
                },
                kind: if family % 2 == 0 {
                    SecondaryIndexKind::Equality
                } else {
                    SecondaryIndexKind::Range
                },
                state: SecondaryIndexState::Ready,
                last_error: None,
            })
            .collect()
    }

    fn indexed_segment(path: &Path) -> (ManifestState, PathBuf) {
        let mut state = manifest(path);
        state.edge_label_tokens.insert("edge".into(), 3);
        state.next_edge_label_id = 4;
        state.secondary_indexes = full_catalog();
        state.next_secondary_index_id = 9;
        let memtable = crate::memtable::Memtable::new();
        for entry in &state.secondary_indexes {
            memtable.register_secondary_index(entry).unwrap();
        }
        let WalOp::UpsertNode(mut row) = node() else {
            unreachable!()
        };
        row.props.insert("p".into(), PropValue::Int(1));
        let edge = EdgeRecord {
            id: 1,
            from: 1,
            to: 1,
            label_id: 3,
            props: row.props.clone(),
            created_at: 1,
            updated_at: 2,
            weight: 1.0,
            valid_from: 0,
            valid_to: i64::MAX,
            last_write_seq: 0,
        };
        memtable.apply_op(&WalOp::UpsertNode(row), 1);
        memtable.apply_op(&WalOp::UpsertEdge(edge), 2);
        let segment = crate::segment_writer::segment_dir(path, 1);
        fs::create_dir_all(&segment).unwrap();
        let info = crate::segment_writer::write_segment_with_degree_overlay_and_secondary_indexes(
            &segment,
            1,
            &memtable,
            None,
            &crate::degree_cache::DegreeOverlaySnapshot::empty(),
            &state.secondary_indexes,
        )
        .unwrap();
        state.segments.push(info);
        crate::manifest::write_manifest(path, &state).unwrap();
        (state, segment)
    }
    #[test]
    fn p45c2_preflight_classifies_optional_repair_and_building_without_mutation() {
        // All eight fixed families succeed; remove and corrupt each family in turn.
        // Building and optional repair states are accepted read-only for startup recovery.
        for case in 0..18 {
            let dir = tempfile::tempdir().unwrap();
            let (mut state, segment) = indexed_segment(dir.path());
            if (1..=16).contains(&case) {
                let mut component = crate::segment_components::retained_manifest::Open::default();
                component
                    .read_bounded(
                        &segment
                            .join(crate::segment_components::SEGMENT_COMPONENT_MANIFEST_FILENAME),
                        None,
                    )
                    .unwrap();
                let kind = crate::segment_components::secondary_index_component_kind_for_entry(
                    &state.secondary_indexes[(case - 1) / 2],
                )
                .unwrap();
                let record = component
                    .manifest()
                    .components
                    .iter()
                    .find(|c| c.kind == kind)
                    .unwrap();
                let crate::segment_components::ComponentHandleV1::ExternalFile {
                    relative_path,
                    ..
                } = &record.handle
                else {
                    panic!("fixture expected external index")
                };
                let path = segment.join(relative_path);
                if case % 2 == 1 {
                    fs::remove_file(path).unwrap();
                } else {
                    let mut bytes = fs::read(&path).unwrap();
                    bytes[0] ^= 0xff;
                    fs::write(path, bytes).unwrap();
                }
            } else if case == 17 {
                state.secondary_indexes[0].state = crate::types::SecondaryIndexState::Building;
                crate::manifest::write_manifest(dir.path(), &state).unwrap();
            }
            let before = contents(dir.path());
            let (configured, result) = attempt(dir.path());
            let permit = result.unwrap();
            assert_eq!(permit.summary().segments, 1);
            let reader = permit.owner.readers[0].reader();
            assert_eq!(reader.get_node(1).unwrap().unwrap().key, "key");
            assert_eq!(reader.edge_count(), 1);
            for (ordinal, entry) in state.secondary_indexes.iter().enumerate() {
                if entry.state != crate::types::SecondaryIndexState::Ready {
                    continue;
                }
                let target = crate::planner_stats::planner_stats_declared_index_target(entry);
                let kind = match entry.kind {
                    crate::types::SecondaryIndexKind::Equality => {
                        crate::planner_stats::PlannerStatsDeclaredIndexKind::Equality
                    }
                    crate::types::SecondaryIndexKind::Range => {
                        crate::planner_stats::PlannerStatsDeclaredIndexKind::Range
                    }
                };
                let coverage = reader.declared_index_runtime_coverage_state_for_target(
                    entry.index_id,
                    target,
                    kind,
                );
                if (1..=16).contains(&case) && ordinal == (case - 1) / 2 {
                    assert_ne!(
                        coverage,
                        crate::planner_stats::DeclaredIndexRuntimeCoverageState::Available,
                        "case {case}"
                    );
                } else {
                    assert_eq!(
                        coverage,
                        crate::planner_stats::DeclaredIndexRuntimeCoverageState::Available,
                        "case {case}, index {ordinal}"
                    );
                }
            }
            permit.revalidate().unwrap();
            drop(permit);
            assert_eq!(contents(dir.path()), before, "coverage case {case}");
            configured.domain.close();
            drop(configured);
        }
    }

    #[cfg(feature = "production-concurrency")]
    #[test]
    fn p45a3_review_s3_large_atomic_replay_needs_no_per_row_transition_allocation() {
        use crate::engine::participant::{allocations, TEST_SERIAL};
        let _serial = TEST_SERIAL.lock().unwrap_or_else(|e| e.into_inner());
        let dir = tempfile::tempdir().unwrap();
        let ops: Vec<_> = (1..=2048)
            .map(|seq| {
                (
                    seq,
                    WalOp::DeleteNode {
                        id: 1,
                        deleted_at: 1,
                    },
                )
            })
            .collect();
        wal(dir.path(), &ops);
        let path = crate::wal::wal_generation_path(dir.path(), 0);
        let bytes = fs::read(&path).unwrap();
        let mut job = PreflightJob::default();
        let file = job.witness(&path).unwrap().unwrap();
        // Warm only the fixed single-frame wire buffer, not an atomic replay pass.
        let normal_position = 16 + u32::from_le_bytes(bytes[8..12].try_into().unwrap()) as u64;
        job.frame(file, normal_position).unwrap().unwrap();
        let ((durable, replay, decoded), observed) =
            allocations::measure(|| job.scan_wal(file, None).unwrap());
        assert_eq!(observed, (0, 0, 0, 0));
        assert_eq!(durable, bytes.len() as u64);
        assert_eq!(replay.operations, ops.len());
        assert_eq!(replay.node_deletions, ops.len());
        assert_eq!(replay.max_batch_operations, ops.len());
        assert_eq!(decoded.operations, ops.len());
        assert_eq!(decoded.markers, 2);
        assert_eq!(fs::read(path).unwrap(), bytes);
    }

    #[cfg(feature = "production-concurrency")]
    #[test]
    fn targeted_o_readonly_scan_and_real_open_revalidate_once_at_consumption() {
        crate::engine::post45_read_tests::ready_participants_for_fixture();
        for flushed in [false, true] {
            let dir = tempfile::tempdir().unwrap();
            manifest(dir.path());
            wal(dir.path(), &[(1, node())]);
            if flushed {
                let db =
                    crate::engine::DatabaseEngine::open(dir.path(), &DbOptions::default()).unwrap();
                db.flush().unwrap();
                crate::engine::p45b1_tests::finish(db);
            }
            let before = contents(dir.path());
            take_preflight_work_for_test();
            let (configured, permit) = attempt(dir.path());
            let permit = permit.unwrap();
            let files = permit.owner.files.len();
            assert!(files > 0);
            assert_eq!(take_preflight_work_for_test(), [files, files, 0]);
            assert_eq!(contents(dir.path()), before);
            permit.revalidate().unwrap();
            assert_eq!(take_preflight_work_for_test(), [0, files, 1]);
            drop(permit);
            configured.domain.close();
            drop(configured);
            // Direct scan admission may still be closing. Use a byte-for-byte
            // copy for real open instead of retrying directory Busy.
            fn copy_tree(from: &Path, to: &Path) {
                fs::create_dir_all(to).unwrap();
                for entry in fs::read_dir(from).unwrap() {
                    let path = entry.unwrap().path();
                    let dest = to.join(path.file_name().unwrap());
                    if path.is_dir() {
                        copy_tree(&path, &dest);
                    } else {
                        fs::copy(path, dest).unwrap();
                    }
                }
            }
            let actual = tempfile::tempdir().unwrap();
            copy_tree(dir.path(), actual.path());
            take_preflight_work_for_test();
            let db =
                crate::engine::DatabaseEngine::open(actual.path(), &DbOptions::default()).unwrap();
            let work = take_preflight_work_for_test();
            assert!(work[0] > 0);
            assert_eq!(work, [work[0], 2 * work[0], 1]);
            assert_eq!(db.get_node(1).unwrap().unwrap().key, "key");
            crate::engine::p45b1_tests::finish(db);
        }
    }

    #[cfg(feature = "production-concurrency")]
    #[test]
    fn targeted_o_final_open_revalidation_rejects_changed_witnesses_before_mutation() {
        use std::cell::{Cell, RefCell};
        use std::rc::Rc;
        crate::engine::post45_read_tests::ready_participants_for_fixture();
        // Same-length edit, growth, removal, newly authoritative absent file,
        // directory replacement, and creation at a witnessed missing path.
        for case in 0..6 {
            let root = tempfile::tempdir().unwrap();
            let path = root.path().join("db");
            let original = if case != 5 {
                fs::create_dir(&path).unwrap();
                manifest(&path);
                let bytes = fs::read(path.join(MANIFEST_CURRENT)).unwrap();
                if case == 3 {
                    fs::rename(path.join(MANIFEST_CURRENT), path.join(MANIFEST_PREV)).unwrap();
                }
                bytes
            } else {
                Vec::new()
            };
            let hook_ran = Rc::new(Cell::new(false));
            let after_hook = Rc::new(RefCell::new(None));
            let (ran, expected) = (hook_ran.clone(), after_hook.clone());
            let hook_path = path.clone();
            let hook_root = root.path().to_owned();
            take_preflight_work_for_test();
            let result = with_revalidation_hook_for_test(
                move || {
                    ran.set(true);
                    let current = hook_path.join(MANIFEST_CURRENT);
                    match case {
                        0 => {
                            let mut bytes = original;
                            let at = bytes.iter().position(|b| *b == b'1').unwrap();
                            bytes[at] = b'2';
                            fs::write(current, bytes).unwrap();
                        }
                        1 => {
                            let mut bytes = original;
                            bytes.push(b' ');
                            fs::write(current, bytes).unwrap();
                        }
                        2 => fs::remove_file(current).unwrap(),
                        3 => fs::write(current, original).unwrap(),
                        4 => {
                            fs::rename(&hook_path, hook_root.join("old")).unwrap();
                            fs::create_dir(&hook_path).unwrap();
                        }
                        5 => fs::create_dir(&hook_path).unwrap(),
                        _ => unreachable!(),
                    }
                    *expected.borrow_mut() = Some(contents(&hook_root));
                },
                || crate::engine::DatabaseEngine::open(&path, &DbOptions::default()),
            );
            assert!(hook_ran.get());
            assert!(
                result.is_err(),
                "case {case} passed final witness validation"
            );
            drop(result);
            assert_eq!(take_preflight_work_for_test()[2], 1);
            assert_eq!(
                contents(root.path()),
                after_hook.borrow_mut().take().unwrap(),
                "startup mutated after refusal, case {case}"
            );
        }
    }

}
