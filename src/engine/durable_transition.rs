//! Registered persistence tickets and verified sealed-WAL retirement capabilities.
//! Actual engine publishers adopt this substrate in a3.
#![allow(dead_code)]
use super::concurrency_lifecycle::{CommitFence, CommitState, Outcome};
use super::reclamation::{OwnerPermit, ReclaimDomain, ResourceClass};
use crate::types::{ManifestState, SegmentInfo, WalOp};
use std::collections::BTreeMap;
use std::io::Read;
use std::path::{Path, PathBuf};

pub(super) struct LocalTicket {
    value: TransitionTicket,
    origin: super::reclamation::DomainIdentity,
}
impl LocalTicket {
    pub(super) fn belongs_to(&self, domain: &ReclaimDomain) -> bool {
        self.origin.belongs_to(domain)
    }
    pub(super) fn mutate<R>(
        &mut self,
        f: impl FnOnce(&mut TransitionTicket) -> R,
    ) -> Result<R, Outcome> {
        Ok(f(&mut self.value))
    }
    pub(super) fn get(&self) -> &TransitionTicket {
        &self.value
    }
}

const METADATA_LIMIT: usize = 48 * 1024;

fn same_segment(a: &SegmentInfo, b: &SegmentInfo) -> bool {
    a.id == b.id
        && a.segment_data_id == b.segment_data_id
        && a.segment_format_version == b.segment_format_version
        && a.node_count == b.node_count
        && a.edge_count == b.edge_count
}

#[derive(Default)]
pub(super) struct TransitionTicket {
    pub(super) captured_catalog_unchanged: bool,
    pub(super) preparation_started: Option<std::time::Instant>,
    expected: Vec<u8>,
    serialized: Vec<u8>,
    pub(super) captured: Option<super::append_slice::Snapshot>,
    manifest: Option<ManifestState>,
    pub(super) inputs: Vec<SegmentInfo>,
    pub(super) outputs: Vec<SegmentInfo>,
    retirements: Vec<RetirementCapability>,
    node_label_ids: std::collections::BTreeSet<u32>,
    edge_label_ids: std::collections::BTreeSet<u32>,
    validation: crate::manifest::ManifestBodyScratch,
    #[cfg(test)]
    drop_notice: Option<std::sync::mpsc::Sender<String>>,
    uncertain: bool,
}
impl TransitionTicket {
    pub(super) fn manifest(&self) -> Option<&ManifestState> {
        self.manifest.as_ref()
    }
    pub(super) fn retires(&self, generation: u64) -> bool {
        self.retirements
            .iter()
            .any(|c| c.seal.get().generation == generation)
    }

    /// Prepared bytes are the immutable catalog's already validated size.
    pub(super) fn metadata_bytes(&self) -> usize {
        self.serialized.len()
    }
}

pub(super) fn metadata_size(state: &ManifestState) -> Result<usize, Outcome> {
    super::payload::validate_manifest_json(state).map_err(|error| match error {
        super::payload::PayloadError::Resource {
            resource,
            requested,
            limit,
            retryable,
        } => Outcome::ResourceLimit(super::concurrency_lifecycle::ResourceLimit {
            resource,
            requested,
            limit,
            retryable,
        }),
        super::payload::PayloadError::Closed => Outcome::Refused("closed payload scratch"),
        super::payload::PayloadError::PersistedRoundTrip => {
            Outcome::Refused("persisted manifest literal incompatibility")
        }
        // Preserve the schema validator's existing nonfinite refusal reason.
        super::payload::PayloadError::Wire => Outcome::Refused("invalid manifest identity"),
    })?;
    struct Counter(usize);
    impl std::io::Write for Counter {
        fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
            self.0 = self
                .0
                .checked_add(bytes.len())
                .ok_or_else(|| std::io::Error::other("metadata overflow"))?;
            Ok(bytes.len())
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }
    let mut counter = Counter(0);
    serde_json::to_writer_pretty(&mut counter, state)
        .map_err(|_| Outcome::Refused("metadata encoding"))?;
    if counter.0 > METADATA_LIMIT {
        return Err(Outcome::Refused("a1 metadata capacity"));
    }
    Ok(counter.0)
}

/// Checked scalar upper bound for a future global manifest. Whole borrowed
/// schema/token/declaration cost was counted by the actual serializer; maximum
/// segment scalar/digest rows and remaining token fragments cover future growth.
pub(super) fn check_future_manifest(
    metadata_bytes: usize,
    extra_token_wire: usize,
    segments: usize,
) -> Result<(), Outcome> {
    struct Count(usize);
    impl std::io::Write for Count {
        fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
            self.0 = self
                .0
                .checked_add(bytes.len())
                .ok_or_else(|| std::io::Error::other("metadata overflow"))?;
            Ok(bytes.len())
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }
    let maximum = SegmentInfo {
        id: u64::MAX,
        node_count: u64::MAX,
        edge_count: u64::MAX,
        segment_format_version: u32::MAX,
        segment_data_id: [255; 32],
    };
    let mut row = Count(0);
    serde_json::to_writer_pretty(&mut row, &maximum)
        .map_err(|_| Outcome::Refused("metadata encoding"))?;
    // SegmentInfo has five fields and a 32-element digest array: <=43 lines.
    // Nesting its standalone pretty representation in manifest.segments adds
    // at most four spaces per line plus comma/newline per row.
    let per_segment = row
        .0
        .checked_add(43 * 4 + 2)
        .ok_or(Outcome::Refused("metadata overflow"))?;
    // All root integer fields can grow to their actual u32/u64 encoded width.
    let scalar_growth = 12 * 20;
    let needed = metadata_bytes
        .checked_add(extra_token_wire)
        .and_then(|n| n.checked_add(segments.checked_mul(per_segment)?))
        .and_then(|n| n.checked_add(scalar_growth))
        .ok_or(Outcome::Refused("metadata overflow"))?;
    if needed > METADATA_LIMIT {
        return Err(Outcome::Refused("private future manifest capacity"));
    }
    Ok(())
}

/// All populated inputs are borrowed. Refusal cannot destroy the caller's catalog.
/// Empty registered ticket precedes clone/serialization growth. These concrete
/// Clone implementations have no user code/error/unwind path; allocation OOM aborts.
pub(super) fn prepare(
    domain: &ReclaimDomain,
    expected: &ManifestState,
    state: &ManifestState,
    inputs: &[SegmentInfo],
    outputs: &[SegmentInfo],
    retirements: &[RetirementCapability],
) -> Result<LocalTicket, Outcome> {
    let preparation_started = std::time::Instant::now();
    metadata_size(state)?;
    metadata_size(expected)?;
    let mut ticket = LocalTicket {
        value: TransitionTicket::default(),
        origin: domain.identity(),
    };
    ticket
        .mutate(|t| {
            t.preparation_started = Some(preparation_started);
            t.captured_catalog_unchanged = expected.secondary_indexes == state.secondary_indexes
                && expected.dense_vector == state.dense_vector
                && expected.node_label_tokens == state.node_label_tokens
                && expected.edge_label_tokens == state.edge_label_tokens
                && expected.node_schemas == state.node_schemas
                && expected.edge_schemas == state.edge_schemas
                && expected.prune_policies == state.prune_policies;
            t.manifest = Some(state.clone());
            for input in inputs {
                if !expected
                    .segments
                    .iter()
                    .any(|s| s.id == input.id && s.segment_data_id == input.segment_data_id)
                {
                    return Err(Outcome::Refused("stale durable ticket"));
                }
            }
            t.inputs.extend_from_slice(inputs);
            t.outputs.extend_from_slice(outputs);
            serde_json::to_writer_pretty(&mut t.expected, expected)
                .map_err(|_| Outcome::Refused("metadata encoding"))?;
            serde_json::to_writer_pretty(&mut t.serialized, state)
                .map_err(|_| Outcome::Refused("metadata encoding"))?;
            let state = t.manifest.as_ref().unwrap();
            if state.label_token_schema_version != crate::types::LABEL_TOKEN_SCHEMA_VERSION {
                return Err(Outcome::Refused("invalid label token schema"));
            }
            for (tokens, next, ids) in [
                (
                    &state.node_label_tokens,
                    state.next_node_label_id,
                    &mut t.node_label_ids,
                ),
                (
                    &state.edge_label_tokens,
                    state.next_edge_label_id,
                    &mut t.edge_label_ids,
                ),
            ] {
                for (name, id) in tokens {
                    crate::types::validate_label_token_name(name)
                        .map_err(|_| Outcome::Refused("invalid token name"))?;
                    if *id == 0 || !ids.insert(*id) || next <= *id {
                        return Err(Outcome::Refused("invalid token identity"));
                    }
                }
            }
            crate::manifest::validate_manifest_body_identity_with_scratch(state, &mut t.validation)
                .map_err(|_| Outcome::Refused("invalid manifest identity"))?;
            for c in retirements {
                if !c.seal.belongs_to(domain) {
                    return Err(Outcome::Refused("foreign retirement capability"));
                }
                let seal = c.seal.get();
                if seal.before != t.expected || seal.after != t.serialized {
                    return Err(Outcome::Refused("retirement catalog mismatch"));
                }
                if t.retires(seal.generation) {
                    return Err(Outcome::Refused("duplicate WAL retirement"));
                }
                t.retirements.push(RetirementCapability {
                    seal: c.seal.fork(),
                });
            }
            Ok(())
        })
        .unwrap()?;
    Ok(ticket)
}

#[cfg(test)]
impl Drop for TransitionTicket {
    fn drop(&mut self) {
        if let Some(notice) = self.drop_notice.take() {
            drop(self.manifest.take());
            drop(std::mem::take(&mut self.expected));
            drop(std::mem::take(&mut self.serialized));
            drop(std::mem::take(&mut self.node_label_ids));
            drop(std::mem::take(&mut self.edge_label_ids));
            drop(std::mem::take(&mut self.validation));
            let _ = notice.send(std::thread::current().name().unwrap_or("unnamed").into());
        }
    }
}

// Exact encoded authority is a stronger, fail-closed revalidation than decoded
// semantic equality. It needs no unregistered catalog or serialization scratch.
// A noncanonical/changed candidate refuses before mutation; recovery selection
// remains the existing engine's responsibility. No stale fallback is overwritten.
fn file_matches(path: &Path, expected: &[u8]) -> Result<bool, Outcome> {
    let mut file =
        std::fs::File::open(path).map_err(|_| Outcome::Refused("missing durable file"))?;
    if file
        .metadata()
        .map_err(|_| Outcome::Refused("durable file metadata"))?
        .len()
        != expected.len() as u64
    {
        return Ok(false);
    }
    let mut buffer = [0u8; 8192];
    for part in expected.chunks(buffer.len()) {
        file.read_exact(&mut buffer[..part.len()])
            .map_err(|_| Outcome::Refused("durable file read"))?;
        if &buffer[..part.len()] != part {
            return Ok(false);
        }
    }
    Ok(true)
}
fn authority_matches(db_dir: &Path, expected: &[u8]) -> Result<bool, Outcome> {
    for name in [
        crate::manifest::MANIFEST_CURRENT,
        crate::manifest::MANIFEST_TMP,
        crate::manifest::MANIFEST_PREV,
    ] {
        let path = db_dir.join(name);
        if path.exists() {
            return file_matches(&path, expected);
        }
    }
    Err(Outcome::Refused("missing durable authority"))
}

pub(super) fn publish(
    db_dir: &Path,
    fence: &CommitFence,
    ticket: &mut LocalTicket,
) -> Result<(), Outcome> {
    let gate = fence.gate()?;
    ticket
        .mutate(|t| {
            if t.uncertain {
                return Err(Outcome::Refused("uncertain ticket"));
            }
            if !authority_matches(db_dir, &t.expected)? {
                return Err(Outcome::Refused("stale durable catalog ticket"));
            }
            // Exact captured authority plus immutable input descriptors establish
            // source/catalog identity; capability validation below checks physical output.
            for c in &t.retirements {
                c.seal.get().revalidate(db_dir)?;
            }
            let persisted = if let Some(output) = t.outputs.first() {
                super::persist_compaction_manifest_encoded(db_dir, &t.serialized, output.id)
            } else {
                crate::manifest::write_manifest_encoded(db_dir, &t.serialized)
            };
            if let Err(error) = persisted {
                t.uncertain = true;
                gate.fence();
                return Err(Outcome::Commit {
                    state: CommitState::Indeterminate,
                    message: error.to_string(),
                });
            }
            for c in &t.retirements {
                if let Err(error) =
                    crate::wal::remove_wal_generation(db_dir, c.seal.get().generation)
                {
                    t.uncertain = true;
                    gate.fence();
                    return Err(Outcome::Commit {
                        state: CommitState::Indeterminate,
                        message: error.to_string(),
                    });
                }
            }
            Ok(())
        })
        .unwrap()
}

struct FileIdentity {
    path: PathBuf,
    len: u64,
    digest: [u8; 32],
}
fn file_identity(path: &Path, limit: u64) -> Result<(u64, [u8; 32]), Outcome> {
    use sha2::{Digest, Sha256};
    let mut file = std::fs::File::open(path).map_err(|_| Outcome::Refused("missing output/WAL"))?;
    let len = file
        .metadata()
        .map_err(|_| Outcome::Refused("file metadata"))?
        .len();
    if len > limit {
        return Err(Outcome::Refused("output inventory capacity"));
    }
    let mut digest = Sha256::new();
    let mut buffer = [0u8; 8192];
    let mut read = 0u64;
    loop {
        let n = file
            .read(&mut buffer)
            .map_err(|_| Outcome::Refused("file read"))?;
        if n == 0 {
            break;
        }
        read = read
            .checked_add(n as u64)
            .ok_or(Outcome::Refused("output inventory capacity"))?;
        if read > len {
            return Err(Outcome::Refused("changed output length"));
        }
        digest.update(&buffer[..n]);
    }
    if read != len {
        return Err(Outcome::Refused("changed output length"));
    }
    Ok((len, digest.finalize().into()))
}

const OUTPUT_PATH_LIMIT: usize = crate::segment_reader::RETIREMENT_OUTPUT_PATH_LIMIT;

#[derive(Clone, Copy, Default, Debug)]
struct OutputInventory {
    files: usize,
    path_bytes: usize,
    output_bytes: usize,
    metadata_bytes: usize,
}
impl OutputInventory {
    fn add(&mut self, path: &PathBuf, len: u64, metadata: bool) -> Result<(), Outcome> {
        let overflow = || Outcome::Refused("output inventory capacity");
        let len = usize::try_from(len).map_err(|_| overflow())?;
        self.files = self.files.checked_add(1).ok_or_else(overflow)?;
        self.path_bytes = self
            .path_bytes
            .checked_add(path.capacity())
            .ok_or_else(overflow)?;
        self.output_bytes = self.output_bytes.checked_add(len).ok_or_else(overflow)?;
        if metadata {
            self.metadata_bytes = self.metadata_bytes.checked_add(len).ok_or_else(overflow)?;
        }
        Ok(())
    }
    fn fits(&self, bound: &Self) -> bool {
        self.files <= bound.files
            && self.path_bytes <= bound.path_bytes
            && self.output_bytes <= bound.output_bytes
            && self.metadata_bytes <= bound.metadata_bytes
    }
}

// Use the same physical inventory for admission and registered identity capture.
// The existing segment layout has root files and one flat secondary-index
// directory. Do not introduce recursive traversal scratch or admit other layouts.
fn visit_output_files(
    dir: &Path,
    mut visit: impl FnMut(PathBuf, u64, bool) -> Result<(), Outcome>,
) -> Result<(), Outcome> {
    fn visit_file(
        entry: std::fs::DirEntry,
        parent: &Path,
        root: bool,
        visit: &mut impl FnMut(PathBuf, u64, bool) -> Result<(), Outcome>,
    ) -> Result<(), Outcome> {
        if !entry
            .file_type()
            .map_err(|_| Outcome::Refused("output metadata"))?
            .is_file()
        {
            return Err(Outcome::Refused("unsupported output entry"));
        }
        let len = entry
            .metadata()
            .map_err(|_| Outcome::Refused("output metadata"))?
            .len();
        let name = entry.file_name();
        let needed = parent
            .as_os_str()
            .len()
            .checked_add(name.len())
            .and_then(|n| n.checked_add(1))
            .filter(|n| *n <= OUTPUT_PATH_LIMIT)
            .ok_or(Outcome::Refused("output path capacity"))?;
        let mut path = PathBuf::with_capacity(needed);
        path.push(parent);
        path.push(&name);
        let metadata =
            root && name == crate::segment_components::SEGMENT_COMPONENT_MANIFEST_FILENAME;
        visit(path, len, metadata)
    }
    if dir.as_os_str().len()
        > OUTPUT_PATH_LIMIT - crate::segment_writer::SECONDARY_INDEX_DIRNAME.len() - 1
    {
        return Err(Outcome::Refused("output path capacity"));
    }
    for entry in std::fs::read_dir(dir).map_err(|_| Outcome::Refused("output directory"))? {
        let entry = entry.map_err(|_| Outcome::Refused("output entry"))?;
        if entry
            .file_type()
            .map_err(|_| Outcome::Refused("output metadata"))?
            .is_dir()
        {
            if entry.file_name() != crate::segment_writer::SECONDARY_INDEX_DIRNAME {
                return Err(Outcome::Refused("unsupported output directory"));
            }
            let child = dir.join(crate::segment_writer::SECONDARY_INDEX_DIRNAME);
            for file in
                std::fs::read_dir(&child).map_err(|_| Outcome::Refused("output directory"))?
            {
                visit_file(
                    file.map_err(|_| Outcome::Refused("output entry"))?,
                    &child,
                    false,
                    &mut visit,
                )?;
            }
        } else {
            visit_file(entry, dir, true, &mut visit)?;
        }
    }
    Ok(())
}

#[derive(Default)]
struct SealedRetirement {
    directory: Option<PathBuf>,
    generation: u64,
    before: Vec<u8>,
    after: Vec<u8>,
    wal: Vec<u8>,
    encode: Vec<u8>,
    batch: Vec<u8>,
    record: Vec<u8>,
    // Registered producing parent owns mappings and final normalized opinions.
    readers: Vec<crate::segment_reader::SegmentReaderOpen>,
    files: Vec<FileIdentity>,
    nodes: BTreeMap<u64, (u64, usize, usize)>,
    edges: BTreeMap<u64, (u64, usize, usize)>,
    #[cfg(test)]
    drop_probe: Option<RetirementDropProbe>,
}
#[cfg(test)]
struct RetirementDropProbe {
    entered: std::sync::mpsc::Sender<(String, (usize, usize, usize, usize))>,
    resume: std::sync::Mutex<std::sync::mpsc::Receiver<()>>,
    finished: std::sync::mpsc::Sender<(String, usize)>,
}
#[cfg(test)]
thread_local! {
    static RETIREMENT_DROP_PROBE: std::cell::RefCell<Option<RetirementDropProbe>> = const { std::cell::RefCell::new(None) };
    static RETIREMENT_INVENTORY_CHANGE: std::cell::RefCell<Option<Box<dyn FnOnce()>>> = const { std::cell::RefCell::new(None) };
    static RETIREMENT_READER_CHANGE: std::cell::RefCell<Option<Box<dyn FnOnce()>>> = const { std::cell::RefCell::new(None) };
}
#[cfg(test)]
impl Drop for SealedRetirement {
    fn drop(&mut self) {
        if let Some(probe) = self.drop_probe.take() {
            let counts = self
                .readers
                .iter()
                .map(|r| r.partial_counts())
                .fold((0, 0, 0, 0), |a, b| {
                    (a.0 + b.0, a.1 + b.1, a.2 + b.2, a.3 + b.3)
                });
            let name = std::thread::current()
                .name()
                .unwrap_or("unnamed")
                .to_owned();
            probe.entered.send((name.clone(), counts)).unwrap();
            probe.resume.lock().unwrap().recv().unwrap();
            let (_, allocation) = super::participant::allocations::measure(|| {
                drop(std::mem::take(&mut self.readers))
            });
            probe.finished.send((name, allocation.3)).unwrap();
        }
    }
}
pub(super) struct RetirementCapability {
    seal: OwnerPermit<SealedRetirement>,
}
impl SealedRetirement {
    fn revalidate(&self, dir: &Path) -> Result<(), Outcome> {
        if self.directory.as_ref()
            != Some(
                &std::fs::canonicalize(dir).map_err(|_| Outcome::Refused("directory identity"))?,
            )
        {
            return Err(Outcome::Refused("foreign retirement capability"));
        }
        if !file_matches(
            &crate::wal::wal_generation_path(dir, self.generation),
            &self.wal,
        )? {
            return Err(Outcome::Refused("changed sealed WAL"));
        }
        for file in &self.files {
            if file_identity(&file.path, file.len)
                .map_err(|_| Outcome::Refused("changed durable output"))?
                != (file.len, file.digest)
            {
                return Err(Outcome::Refused("changed durable output"));
            }
        }
        Ok(())
    }
}

/// A raw generation number has no retirement authority. This capability proves
/// an existing frozen epoch, exact complete V3 batches, final primary/tombstone
/// opinions and all label/sequence/ID effects in the immutable target catalog.
/// All physical outputs are identity-opened and hashed; the gate rechecks files.
#[expect(
    clippy::too_many_arguments,
    reason = "Keep each independently checked retirement authority explicit"
)]
pub(super) fn prepare_retirement(
    domain: &ReclaimDomain,
    db_dir: &Path,
    fence: &CommitFence,
    expected: &ManifestState,
    state: &ManifestState,
    outputs: &[SegmentInfo],
    generation: u64,
    batches: &[&[(u64, WalOp)]],
) -> Result<RetirementCapability, Outcome> {
    // This capability retires one sealed epoch, not unrelated storage/WAL or
    // token authority. Broader adoption jobs have their own later integration.
    if state.active_wal_generation_id != expected.active_wal_generation_id
        || state.next_wal_generation_id != expected.next_wal_generation_id
        || state.next_engine_seq < expected.next_engine_seq
        || state.next_node_id < expected.next_node_id
        || state.next_edge_id < expected.next_edge_id
        || state.next_node_label_id < expected.next_node_label_id
        || state.next_edge_label_id < expected.next_edge_label_id
        || state.next_schema_id < expected.next_schema_id
        || state.next_secondary_index_id < expected.next_secondary_index_id
        || expected
            .node_label_tokens
            .iter()
            .any(|(name, id)| state.node_label_tokens.get(name) != Some(id))
        || expected
            .edge_label_tokens
            .iter()
            .any(|(name, id)| state.edge_label_tokens.get(name) != Some(id))
        || expected
            .segments
            .iter()
            .any(|old| !state.segments.iter().any(|new| same_segment(new, old)))
        || state.segments.iter().enumerate().any(|(index, new)| {
            state.segments[..index]
                .iter()
                .any(|prior| prior.id == new.id)
                || !expected
                    .segments
                    .iter()
                    .chain(outputs)
                    .any(|known| same_segment(new, known))
        })
        || state.pending_flush_epochs.len()
            != expected
                .pending_flush_epochs
                .iter()
                .filter(|e| e.wal_generation_id != generation)
                .count()
        || expected
            .pending_flush_epochs
            .iter()
            .filter(|e| e.wal_generation_id != generation)
            .any(|old| {
                !state.pending_flush_epochs.iter().any(|new| {
                    new.epoch_id == old.epoch_id
                        && new.wal_generation_id == old.wal_generation_id
                        && new.state == old.state
                        && new.segment_id == old.segment_id
                })
            })
    {
        return Err(Outcome::Refused(
            "retirement changed unrelated durable authority",
        ));
    }
    if generation == expected.active_wal_generation_id
        || generation >= expected.next_wal_generation_id
        || !expected.pending_flush_epochs.iter().any(|e| {
            e.wal_generation_id == generation
                && e.state == crate::types::FlushEpochState::FrozenPendingFlush
        })
        || state.active_wal_generation_id == generation
        || state
            .pending_flush_epochs
            .iter()
            .any(|e| e.wal_generation_id == generation)
    {
        return Err(Outcome::Refused("WAL is not sealed for retirement"));
    }
    if !outputs.is_empty()
        && (outputs.len() != 1 || state.segments.iter().any(|s| s.id > outputs[0].id))
    {
        return Err(Outcome::Refused(
            "retirement output is not the newest complete source",
        ));
    }
    let wal_len = std::fs::metadata(crate::wal::wal_generation_path(db_dir, generation))
        .map_err(|_| Outcome::Refused("missing sealed WAL"))?
        .len();
    let wal_len =
        usize::try_from(wal_len).map_err(|_| Outcome::Refused("WAL capacity overflow"))?;
    if crate::wal::v3_batches_encoded_len(batches, wal_len)
        .map_err(|_| Outcome::Refused("supplied WAL exceeds sealed capacity"))?
        != wal_len
    {
        return Err(Outcome::Refused("incomplete sealed WAL coverage"));
    }
    let mut inventory = OutputInventory::default();
    for output in outputs {
        if !state.segments.iter().any(|s| same_segment(s, output)) {
            return Err(Outcome::Refused("output absent from target"));
        }
        visit_output_files(
            &crate::segment_writer::segment_dir(db_dir, output.id),
            |path, len, metadata| inventory.add(&path, len, metadata),
        )?;
    }
    let mut seal = domain
        .build::<SealedRetirement>(ResourceClass::Storage)
        .map_err(Outcome::from)?;
    let _gate = fence.gate()?; // all reservations precede durable authority
    #[cfg(test)]
    RETIREMENT_INVENTORY_CHANGE.with(|hook| {
        if let Some(change) = hook.borrow_mut().take() {
            change();
        }
    });
    seal.mutate(|s| {
        #[cfg(test)]
        {
            s.drop_probe = RETIREMENT_DROP_PROBE.with(|p| p.borrow_mut().take());
        }
        s.directory = Some(
            std::fs::canonicalize(db_dir).map_err(|_| Outcome::Refused("directory identity"))?,
        );
        s.generation = generation;
        serde_json::to_writer_pretty(&mut s.before, expected)
            .map_err(|_| Outcome::Refused("metadata encoding"))?;
        serde_json::to_writer_pretty(&mut s.after, state)
            .map_err(|_| Outcome::Refused("metadata encoding"))?;
        if !authority_matches(db_dir, &s.before)? {
            return Err(Outcome::Refused("stale seal authority"));
        }
        crate::wal::write_wal_header(&mut s.wal).map_err(|_| Outcome::Refused("WAL encoding"))?;
        let mut last_seq = 0;
        for (batch_index, batch) in batches.iter().enumerate() {
            crate::wal::prepare_v3_batch_into(batch, &mut s.encode, &mut s.batch)
                .map_err(|_| Outcome::Refused("WAL encoding"))?;
            s.wal.extend_from_slice(&s.batch);
            for (op_index, (seq, op)) in batch.iter().enumerate() {
                if *seq == 0 || *seq <= last_seq || state.next_engine_seq <= *seq {
                    return Err(Outcome::Refused("uncovered WAL sequence"));
                }
                last_seq = *seq;
                match op {
                    WalOp::EnsureNodeLabel { label, label_id } => {
                        if state.node_label_tokens.get(label) != Some(label_id)
                            || state.next_node_label_id <= *label_id
                        {
                            return Err(Outcome::Refused("uncovered WAL token"));
                        }
                    }
                    WalOp::EnsureEdgeLabel { label, label_id } => {
                        if state.edge_label_tokens.get(label) != Some(label_id)
                            || state.next_edge_label_id <= *label_id
                        {
                            return Err(Outcome::Refused("uncovered WAL token"));
                        }
                    }
                    WalOp::UpsertNode(n) => {
                        if n.dense_vector.is_some() || n.sparse_vector.is_some() {
                            return Err(Outcome::Refused("vector retirement outside slice"));
                        }
                        if state.next_node_id <= n.id
                            || n.label_ids
                                .as_slice()
                                .iter()
                                .any(|id| !state.node_label_tokens.values().any(|v| v == id))
                        {
                            return Err(Outcome::Refused("uncovered WAL identity"));
                        }
                        s.nodes.insert(n.id, (*seq, batch_index, op_index));
                    }
                    WalOp::DeleteNode { id, .. } => {
                        if state.next_node_id <= *id {
                            return Err(Outcome::Refused("uncovered WAL identity"));
                        }
                        s.nodes.insert(*id, (*seq, batch_index, op_index));
                    }
                    WalOp::UpsertEdge(e) => {
                        if state.next_edge_id <= e.id
                            || !state.edge_label_tokens.values().any(|id| *id == e.label_id)
                        {
                            return Err(Outcome::Refused("uncovered WAL identity"));
                        }
                        s.edges.insert(e.id, (*seq, batch_index, op_index));
                    }
                    WalOp::DeleteEdge { id, .. } => {
                        if state.next_edge_id <= *id {
                            return Err(Outcome::Refused("uncovered WAL identity"));
                        }
                        s.edges.insert(*id, (*seq, batch_index, op_index));
                    }
                    _ => return Err(Outcome::Refused("WAL controls supplied as data")),
                }
            }
        }
        if !file_matches(&crate::wal::wal_generation_path(db_dir, generation), &s.wal)? {
            return Err(Outcome::Refused("incomplete sealed WAL coverage"));
        }
        s.files
            .try_reserve_exact(inventory.files)
            .map_err(|_| Outcome::Refused("output identity allocation"))?;
        let mut captured = OutputInventory::default();
        for output in outputs {
            let path = crate::segment_writer::segment_dir(db_dir, output.id);
            let start = s.files.len();
            visit_output_files(&path, |path, len, metadata| {
                captured.add(&path, len, metadata)?;
                if !captured.fits(&inventory) {
                    return Err(Outcome::Refused("output inventory changed"));
                }
                let (actual_len, digest) = file_identity(&path, len)?;
                if actual_len != len {
                    return Err(Outcome::Refused("output inventory changed"));
                }
                // The preallocated vector cannot grow beyond its admitted count.
                s.files.push(FileIdentity { path, len, digest });
                Ok(())
            })?;
            let manifest_path =
                path.join(crate::segment_components::SEGMENT_COMPONENT_MANIFEST_FILENAME);
            let metadata_limit = s.files[start..]
                .iter()
                .find(|f| f.path == manifest_path)
                .map(|f| f.len)
                .ok_or(Outcome::Refused("missing component manifest"))?;
            let mapping_limit = s.files[start..]
                .iter()
                .try_fold(0u64, |sum, file| sum.checked_add(file.len))
                .ok_or(Outcome::Refused("output inventory capacity"))?;
            #[cfg(test)]
            RETIREMENT_READER_CHANGE.with(|hook| {
                if let Some(change) = hook.borrow_mut().take() {
                    change();
                }
            });
            s.readers
                .push(crate::segment_reader::SegmentReaderOpen::default());
            s.readers
                .last_mut()
                .unwrap()
                .open_bounded(
                    &path,
                    output,
                    state.dense_vector.as_ref(),
                    &state.secondary_indexes,
                    metadata_limit,
                    mapping_limit,
                )
                .map_err(|_| Outcome::Refused("invalid durable output"))?;
        }
        for (id, (seq, batch, index)) in &s.nodes {
            let op = &batches[*batch][*index].1;
            let covered = match op {
                WalOp::UpsertNode(n) => {
                    crate::segment_writer::encode_node_record_into(&mut s.record, n)
                        .map_err(|_| Outcome::Refused("record encoding"))?;
                    s.readers.iter().any(|r| {
                        r.reader()
                            .covers_node_bytes(*id, *seq, &s.record)
                            .unwrap_or(false)
                    })
                }
                WalOp::DeleteNode { deleted_at, .. } => s.readers.iter().any(|r| {
                    r.reader()
                        .deleted_node_tombstones()
                        .get(id)
                        .is_some_and(|t| t.last_write_seq == *seq && t.deleted_at == *deleted_at)
                }),
                _ => unreachable!(),
            };
            if !covered {
                return Err(Outcome::Refused("uncovered WAL record"));
            }
        }
        for (id, (seq, batch, index)) in &s.edges {
            let op = &batches[*batch][*index].1;
            let covered = match op {
                WalOp::UpsertEdge(e) => {
                    crate::segment_writer::encode_edge_record_into(&mut s.record, e)
                        .map_err(|_| Outcome::Refused("record encoding"))?;
                    s.readers.iter().any(|r| {
                        r.reader()
                            .covers_edge_bytes(*id, *seq, &s.record)
                            .unwrap_or(false)
                    })
                }
                WalOp::DeleteEdge { deleted_at, .. } => s.readers.iter().any(|r| {
                    r.reader()
                        .deleted_edge_tombstones()
                        .get(id)
                        .is_some_and(|t| t.last_write_seq == *seq && t.deleted_at == *deleted_at)
                }),
                _ => unreachable!(),
            };
            if !covered {
                return Err(Outcome::Refused("uncovered WAL record"));
            }
        }
        s.revalidate(db_dir)?;
        Ok(())
    })
    .unwrap()?;
    Ok(RetirementCapability {
        seal: seal.into_owner().unwrap(),
    })
}

#[cfg(test)]
mod tests {
    use super::super::participant::TEST_SERIAL;
    use super::*;
    use crate::persistence_fault::{self, Boundary};
    use crate::segment_reader::SegmentReader;
    use crate::segment_writer::{
        segment_dir, write_segment_with_degree_overlay_and_secondary_indexes,
    };
    use crate::types::{NodeLabelSet, NodeRecord, WalOp};
    use std::collections::BTreeMap;
    use std::sync::atomic::AtomicBool;
    use std::sync::{mpsc, Arc, Mutex};
    use std::time::{Duration, Instant};
    use tempfile::TempDir;

    fn node(id: u64) -> NodeRecord {
        NodeRecord {
            id,
            label_ids: NodeLabelSet::single(1).unwrap(),
            key: format!("n{id}"),
            props: BTreeMap::from([(
                "data".into(),
                crate::types::PropValue::Bytes(vec![7; 16384]),
            )]),
            created_at: 1,
            updated_at: 2,
            weight: 1.0,
            dense_vector: None,
            sparse_vector: None,
            last_write_seq: id,
        }
    }
    fn segment(path: &Path, id: u64, node_id: u64) -> SegmentInfo {
        let memtable = crate::memtable::Memtable::new();
        memtable.apply_op(&WalOp::UpsertNode(node(node_id)), node_id);
        write_segment_with_degree_overlay_and_secondary_indexes(
            &segment_dir(path, id),
            id,
            &memtable,
            None,
            &crate::degree_cache::DegreeOverlaySnapshot::empty(),
            &[],
        )
        .unwrap()
    }
    fn baseline(path: &Path, segments: Vec<SegmentInfo>) -> ManifestState {
        let mut m = crate::manifest::default_manifest();
        m.node_label_tokens.insert("N".into(), 1);
        m.next_node_label_id = 2;
        m.next_node_id = 100;
        m.next_engine_seq = 100;
        m.segments = segments;
        crate::manifest::write_manifest(path, &m).unwrap();
        crate::manifest::write_manifest(path, &m).unwrap();
        m
    }
    fn wait_exit(stats: &super::super::reclamation::ReclamationStatsHandle) {
        let until = Instant::now() + Duration::from_secs(10);
        while !stats.read().exited {
            assert!(Instant::now() < until, "{:?}", stats.read());
            std::thread::yield_now();
        }
    }

    fn literal_manifest(
        value: crate::types::PropValue,
        edge: bool,
        path: usize,
    ) -> super::super::payload::Owned<ManifestState> {
        use crate::types::{NumericFieldSchemaManifestRule, PropertySchemaManifestRule};
        let mut rule: PropertySchemaManifestRule = serde_json::from_str("{}").unwrap();
        let mut weight: NumericFieldSchemaManifestRule = serde_json::from_str("{}").unwrap();
        let bound = |value| crate::types::SchemaNumericBoundManifest {
            value,
            inclusive: true,
        };
        match path {
            0 => rule.enum_values.push(value),
            1 => rule.numeric_min = Some(bound(value)),
            2 => rule.numeric_max = Some(bound(value)),
            3 => weight.min = Some(bound(value)),
            4 => weight.max = Some(bound(value)),
            _ => unreachable!(),
        }
        let mut state = super::super::payload::Owned::new(crate::manifest::default_manifest());
        state.next_schema_id = 2;
        let schema =
            r#"{"schema_id":1,"revision":1,"label_id":1,"created_at_ms":0,"updated_at_ms":0}"#;
        if edge {
            let mut entry: crate::types::EdgeSchemaManifestEntry =
                serde_json::from_str(schema).unwrap();
            entry.properties.insert("x".into(), rule);
            entry.weight = Some(weight);
            state.edge_schemas.push(entry);
            state.edge_label_tokens.insert("E".into(), 1);
            state.next_edge_label_id = 2;
        } else {
            let mut entry: crate::types::NodeSchemaManifestEntry =
                serde_json::from_str(schema).unwrap();
            entry.properties.insert("x".into(), rule);
            entry.weight = Some(weight);
            state.node_schemas.push(entry);
            state.node_label_tokens.insert("N".into(), 1);
            state.next_node_label_id = 2;
        }
        state
    }

    fn literal_chain(
        mut value: crate::types::PropValue,
        depth: usize,
        map: bool,
    ) -> crate::types::PropValue {
        for _ in 0..depth {
            value = if map {
                crate::types::PropValue::Map(BTreeMap::from([("x".into(), value)]))
            } else {
                crate::types::PropValue::Array(vec![value])
            };
        }
        value
    }

    #[test]
    fn p45a2_manifest_literal_guard_matches_full_json_context_before_ticket_encoding() {
        let _serial = TEST_SERIAL.lock().unwrap_or_else(|e| e.into_inner());
        std::thread::Builder::new()
            .stack_size(2 * 1024 * 1024)
            .spawn(|| {
                use crate::types::PropValue;
                let domain = ReclaimDomain::new();
                let stats = domain.stats();
                let initial = stats.read().occupied;
                for edge in [false, true] {
                    for path in 0..5 {
                        for map in [false, true] {
                            for leaf in
                                [PropValue::Null, PropValue::Int(1), PropValue::Bytes(vec![])]
                            {
                                let accepted = match (&leaf, path >= 3) {
                                    (PropValue::Null, true) => 61,
                                    (PropValue::Bytes(_), false) => 59,
                                    _ => 60,
                                };
                                for depth in [accepted, accepted + 1] {
                                    let state = literal_manifest(
                                        literal_chain(leaf.clone(), depth, map),
                                        edge,
                                        path,
                                    );
                                    let bytes = serde_json::to_vec_pretty(&*state).unwrap();
                                    let old = serde_json::from_slice::<ManifestState>(&bytes)
                                        .map(super::super::payload::Owned::new);
                                    assert_eq!(
                                        old.is_ok(),
                                        depth == accepted,
                                        "edge={edge} path={path} map={map} depth={depth}"
                                    );
                                    let result = metadata_size(&state);
                                    assert_eq!(result.is_ok(), old.is_ok());
                                    if old.is_ok() {
                                        assert_eq!(result.unwrap(), bytes.len());
                                    }
                                    assert_eq!(
                                        stats.read().occupied,
                                        initial,
                                        "retirement slot stays occupied through physical cleanup"
                                    );
                                    // Enum literals are meaningful schemas; nested numeric
                                    // bounds are grammar fixtures, not valid numeric rules.
                                    if path == 0 {
                                        let ticket = prepare(
                                            &domain,
                                            &crate::manifest::default_manifest(),
                                            &state,
                                            &[],
                                            &[],
                                            &[],
                                        );
                                        assert_eq!(ticket.is_ok(), old.is_ok());
                                        if let Ok(mut ticket) = ticket {
                                            assert_eq!(
                                                ticket.mutate(|t| t.metadata_bytes()).unwrap(),
                                                bytes.len()
                                            );
                                            drop(ticket);
                                            let deadline = Instant::now() + Duration::from_secs(5);
                                            while stats.read().occupied != initial {
                                                assert!(Instant::now() < deadline);
                                                std::thread::yield_now();
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
                domain.close();
                wait_exit(&stats);
            })
            .unwrap()
            .join()
            .unwrap();
    }

    #[test]
    fn p45a2_manifest_literal_guard_rejects_deep_inputs_and_preserves_admission() {
        let _serial = TEST_SERIAL.lock().unwrap_or_else(|e| e.into_inner());
        std::thread::Builder::new()
            .stack_size(2 * 1024 * 1024)
            .spawn(|| {
                use super::super::payload::Owned;
                use crate::types::PropValue;
                let domain = ReclaimDomain::new();
                let stats = domain.stats();
                let initial = stats.read().occupied;
                for edge in [false, true] {
                    for path in 0..5 {
                        let state = literal_manifest(
                            literal_chain(PropValue::Int(1), 131_072, path % 2 == 0),
                            edge,
                            path,
                        );
                        let (refusal, measured) =
                            super::super::participant::allocations::measure(|| {
                                metadata_size(&state)
                            });
                        assert!(matches!(
                            refusal,
                            Err(Outcome::Refused(
                                "persisted manifest literal incompatibility"
                            ))
                        ));
                        assert!(
                            measured.2 <= 64 * 1024,
                            "rejecting traversal grows with input: {measured:?}"
                        );
                        assert_eq!(measured.0, measured.1);
                        assert_eq!(stats.read().occupied, initial);

                        let empty = Owned::new(crate::manifest::default_manifest());
                        assert!(prepare(&domain, &empty, &state, &[], &[], &[]).is_err());
                        assert!(prepare(&domain, &state, &empty, &[], &[], &[]).is_err());

                        assert_eq!(stats.read().occupied, initial);
                        let compatible =
                            literal_manifest(literal_chain(PropValue::Int(1), 8, false), edge, 0);
                        assert!(prepare(&domain, &empty, &compatible, &[], &[], &[]).is_ok());
                        let deadline = Instant::now() + Duration::from_secs(5);
                        while stats.read().occupied != initial {
                            assert!(Instant::now() < deadline);
                            std::thread::yield_now();
                        }
                    }
                }
                // A nonfinite scalar must fail compatibility before serde_json can
                // silently turn its Float payload into null.
                for edge in [false, true] {
                    for path in 0..5 {
                        for value in [f64::NAN, f64::INFINITY, f64::NEG_INFINITY] {
                            let state = literal_manifest(PropValue::Float(value), edge, path);
                            assert!(matches!(
                                metadata_size(&state),
                                Err(Outcome::Refused("invalid manifest identity"))
                            ));
                        }
                    }
                }
                domain.close();
                wait_exit(&stats);
            })
            .unwrap()
            .join()
            .unwrap();
    }

    fn inventory_fixture() -> (
        TempDir,
        ManifestState,
        ManifestState,
        SegmentInfo,
        Vec<(u64, WalOp)>,
    ) {
        let dir = TempDir::new().unwrap();
        let mut before = baseline(dir.path(), vec![]);
        before.active_wal_generation_id = 2;
        before.next_wal_generation_id = 3;
        before
            .pending_flush_epochs
            .push(crate::types::FlushEpochMeta {
                epoch_id: 11,
                wal_generation_id: 1,
                state: crate::types::FlushEpochState::FrozenPendingFlush,
                segment_id: None,
            });
        crate::manifest::write_manifest(dir.path(), &before).unwrap();
        let records = vec![(101, WalOp::UpsertNode(node(1)))];
        let mut wal = crate::wal::WalWriter::open_generation(dir.path(), 1).unwrap();
        wal.append_batch(&records).unwrap();
        wal.sync().unwrap();
        drop(wal);
        let memtable = crate::memtable::Memtable::new();
        memtable.apply_op(&records[0].1, 101);
        let output = write_segment_with_degree_overlay_and_secondary_indexes(
            &segment_dir(dir.path(), 3),
            3,
            &memtable,
            None,
            &crate::degree_cache::DegreeOverlaySnapshot::empty(),
            &[],
        )
        .unwrap();
        let mut after = before.clone();
        after.segments.push(output.clone());
        after.next_engine_seq = 102;
        after.pending_flush_epochs.clear();
        (dir, before, after, output, records)
    }
    fn inventory_for(path: &Path) -> OutputInventory {
        let mut inventory = OutputInventory::default();
        visit_output_files(path, |path, len, metadata| {
            inventory.add(&path, len, metadata)
        })
        .unwrap();
        inventory
    }

    // Independent pre-M7 framing, including its two simultaneously owned
    // property-name copies. Keep this allocation mutant outside production.
    fn legacy_compound_fingerprint(entry: &crate::types::SecondaryIndexManifestEntry) -> u64 {
        use crate::types::{SecondaryIndexField, SecondaryIndexKind, SecondaryIndexTarget};
        use sha2::{Digest, Sha256};
        let (target_tag, label_id, fields) = match &entry.target {
            SecondaryIndexTarget::NodeFieldIndex { label_id, fields } => (1, *label_id, fields),
            SecondaryIndexTarget::EdgeFieldIndex { label_id, fields } => (2, *label_id, fields),
            _ => panic!("compound fixture required"),
        };
        let mut hash = Sha256::new();
        hash.update(b"secondary-index-declaration-v2");
        hash.update(entry.index_id.to_le_bytes());
        hash.update([match entry.kind {
            SecondaryIndexKind::Equality => 1,
            SecondaryIndexKind::Range => 2,
        }]);
        hash.update([target_tag]);
        hash.update(label_id.to_le_bytes());
        hash.update((fields.len() as u64).to_le_bytes());
        for field in fields {
            let public = field.to_public();
            let (source, name) = match &public {
                SecondaryIndexField::Property { key } => (1, key.clone()),
                SecondaryIndexField::NodeMetadata(field) => (
                    2,
                    crate::types::node_metadata_index_field_name(*field).to_owned(),
                ),
                SecondaryIndexField::EdgeMetadata(field) => (
                    3,
                    crate::types::edge_metadata_index_field_name(*field).to_owned(),
                ),
            };
            hash.update([source]);
            hash.update((name.len() as u64).to_le_bytes());
            hash.update(name.as_bytes());
        }
        // Frozen v2 schema versions, field/component/tuple limits.
        for value in [1u64, 1, 1, 8, 1024, 4096] {
            hash.update(value.to_le_bytes());
        }
        let digest = hash.finalize();
        u64::from_le_bytes(digest[..8].try_into().unwrap()).max(1)
    }

    fn compound_catalog_entry(key_len: usize) -> crate::types::SecondaryIndexManifestEntry {
        use crate::types::*;
        SecondaryIndexManifestEntry {
            index_id: 7,
            target: SecondaryIndexTarget::NodeFieldIndex {
                label_id: 1,
                fields: vec![
                    SecondaryIndexFieldManifest::Property {
                        key: "k".repeat(key_len),
                    },
                    SecondaryIndexFieldManifest::NodeMetadata {
                        field: NodeMetadataIndexFieldManifest::UpdatedAt,
                    },
                ],
            },
            kind: SecondaryIndexKind::Equality,
            state: SecondaryIndexState::Ready,
            last_error: None,
        }
    }

    #[test]
    fn p45a1_borrowed_fingerprints_long_key_legacy_parity_and_allocations() {
        use crate::segment_components::{
            component_build_fingerprint, component_semantic_fingerprint,
            secondary_index_declaration_fingerprint_for_entry,
        };
        use crate::types::*;
        let _serial = TEST_SERIAL.lock().unwrap_or_else(|e| e.into_inner());
        let mut variants = vec![
            SecondaryIndexFieldManifest::Property {
                key: "updated_at".into(),
            },
            SecondaryIndexFieldManifest::Property {
                key: "α/属性".into(),
            },
        ];
        variants.extend(
            [
                NodeMetadataIndexFieldManifest::Id,
                NodeMetadataIndexFieldManifest::Key,
                NodeMetadataIndexFieldManifest::Weight,
                NodeMetadataIndexFieldManifest::CreatedAt,
                NodeMetadataIndexFieldManifest::UpdatedAt,
            ]
            .map(|field| SecondaryIndexFieldManifest::NodeMetadata { field }),
        );
        variants.extend(
            [
                EdgeMetadataIndexFieldManifest::Id,
                EdgeMetadataIndexFieldManifest::From,
                EdgeMetadataIndexFieldManifest::To,
                EdgeMetadataIndexFieldManifest::Weight,
                EdgeMetadataIndexFieldManifest::CreatedAt,
                EdgeMetadataIndexFieldManifest::UpdatedAt,
                EdgeMetadataIndexFieldManifest::ValidFrom,
                EdgeMetadataIndexFieldManifest::ValidTo,
            ]
            .map(|field| SecondaryIndexFieldManifest::EdgeMetadata { field }),
        );
        let mut checked = 0;
        let mut legacy_bytes = 0;
        for field in variants {
            for kind in [SecondaryIndexKind::Equality, SecondaryIndexKind::Range] {
                for reversed in [false, true] {
                    let mut fields = vec![
                        SecondaryIndexFieldManifest::Property {
                            key: "k".repeat(40 * 1024),
                        },
                        field.clone(),
                    ];
                    if reversed {
                        fields.reverse();
                    }
                    let target =
                        if matches!(field, SecondaryIndexFieldManifest::EdgeMetadata { .. }) {
                            SecondaryIndexTarget::EdgeFieldIndex {
                                label_id: 1,
                                fields,
                            }
                        } else {
                            SecondaryIndexTarget::NodeFieldIndex {
                                label_id: 1,
                                fields,
                            }
                        };
                    let entry = SecondaryIndexManifestEntry {
                        index_id: 7,
                        target,
                        kind: kind.clone(),
                        state: SecondaryIndexState::Ready,
                        last_error: None,
                    };
                    let (expected, old) = super::super::participant::allocations::measure(|| {
                        legacy_compound_fingerprint(&entry)
                    });
                    legacy_bytes = legacy_bytes.max(old.2);
                    assert!(old.2 >= 80 * 1024, "allocation mutant: {old:?}");
                    let (actual, allocations) =
                        super::super::participant::allocations::measure(|| {
                            secondary_index_declaration_fingerprint_for_entry(&entry)
                        });
                    assert_eq!(actual, expected);
                    assert_eq!(allocations, (0, 0, 0, 0));
                    checked += 1;
                }
            }
        }
        let fields: Vec<_> = (0..16384).collect();
        let mut legacy_fields = vec![crate::segment_writer::SEGMENT_FORMAT_VERSION as u64];
        legacy_fields.extend_from_slice(&fields);
        let expected = component_semantic_fingerprint("allocation.fixture", &legacy_fields);
        let (actual, allocations) = super::super::participant::allocations::measure(|| {
            component_build_fingerprint(
                crate::segment_writer::SEGMENT_FORMAT_VERSION,
                "allocation.fixture",
                &fields,
            )
        });
        assert_eq!(actual, expected);
        assert_eq!(allocations, (0, 0, 0, 0));

        let mut ready = compound_catalog_entry(40 * 1024);
        ready.index_id = 9;
        let short = compound_catalog_entry(1);
        let mut building = compound_catalog_entry(40 * 1024);
        building.index_id = 8;
        building.state = SecondaryIndexState::Building;
        let indexes = [ready, building, short]; // sort by ID and exclude Building
        let expected = component_semantic_fingerprint(
            "flush.planner_stats",
            &[
                crate::segment_writer::SEGMENT_FORMAT_VERSION as u64,
                2,
                7,
                legacy_compound_fingerprint(&indexes[2]),
                9,
                legacy_compound_fingerprint(&indexes[0]),
            ],
        );
        let (actual, planner) = super::super::participant::allocations::measure(|| {
            crate::segment_writer::planner_stats_component_fingerprint(&indexes)
        });
        assert_eq!(actual, expected);
        assert!(
            planner.2 < 1024,
            "borrowed ready references only: {planner:?}"
        );
        eprintln!("M7 declaration_cases={checked} legacy_heap_bytes={legacy_bytes} declaration_heap_bytes=0 build_heap_bytes=0 planner_heap_bytes={}", planner.2);
    }

    #[test]
    fn p45a1_manifest_boundaries_real_compaction_cleanup_and_waiting_publisher() {
        let _serial = TEST_SERIAL.lock().unwrap_or_else(|e| e.into_inner());
        for boundary in [
            Boundary::TmpCreate,
            Boundary::TmpWrite,
            Boundary::TmpSync,
            Boundary::PrevRemove,
            Boundary::CurrentToPrev,
            Boundary::TmpToCurrent,
            Boundary::DirSync,
        ] {
            let dir = TempDir::new().unwrap();
            let input1 = segment(dir.path(), 1, 1);
            let input2 = segment(dir.path(), 2, 2);
            let before = baseline(dir.path(), vec![input1.clone(), input2.clone()]);
            let domain = ReclaimDomain::new();
            let stats = domain.stats();
            // Run the actual V3 compaction builder, including metadata-sidecar
            // indexes and the real component manifest, before its acceptance.
            let mut output = domain
                .build::<Option<super::super::BgCompactResult>>(ResourceClass::Storage)
                .unwrap();
            output
                .mutate(|out| {
                    *out = Some(
                        super::super::bg_compact_worker(
                            dir.path().into(),
                            3,
                            vec![
                                (input1.clone(), segment_dir(dir.path(), 1)),
                                (input2.clone(), segment_dir(dir.path(), 2)),
                            ],
                            vec![],
                            None,
                            Default::default(),
                            &AtomicBool::new(false),
                            &Arc::new(Mutex::new(None)),
                        )
                        .unwrap(),
                    )
                })
                .unwrap();
            let info = output
                .mutate(|o| o.as_ref().unwrap().seg_info.clone())
                .unwrap();
            let mut after = before.clone();
            after.segments = vec![info.clone()];
            after.node_label_tokens.insert("Durable".into(), 2);
            after.next_node_label_id = 3;
            let mut ticket = prepare(
                &domain,
                &before,
                &after,
                &before.segments,
                std::slice::from_ref(&info),
                &[],
            )
            .unwrap();
            let mut wal = crate::wal::WalWriter::open_generation(dir.path(), 0).unwrap();
            wal.append_batch(&[(
                101,
                WalOp::EnsureNodeLabel {
                    label: "WAL".into(),
                    label_id: 3,
                },
            )])
            .unwrap();
            wal.sync().unwrap();
            drop(wal);
            let fence = Arc::new(CommitFence::new());
            persistence_fault::set(boundary, false);
            assert!(matches!(
                publish(dir.path(), &fence, &mut ticket),
                Err(Outcome::Commit {
                    state: CommitState::Indeterminate,
                    ..
                })
            ));
            assert!(fence.is_fenced());
            for info in [&input1, &input2, &info] {
                assert!(segment_dir(dir.path(), info.id).exists());
                SegmentReader::open_with_info(&segment_dir(dir.path(), info.id), info, None, &[])
                    .unwrap();
            }
            assert!(crate::wal::wal_generation_path(dir.path(), 0).exists());
            let selected = crate::manifest::load_manifest_readonly(dir.path())
                .unwrap()
                .unwrap();
            let expected_ids: Vec<_> = if matches!(
                boundary,
                Boundary::CurrentToPrev | Boundary::TmpToCurrent | Boundary::DirSync
            ) {
                vec![3]
            } else {
                vec![1, 2]
            };
            assert_eq!(
                selected.segments.iter().map(|s| s.id).collect::<Vec<_>>(),
                expected_ids
            );
            let untouched = std::fs::read(dir.path().join(crate::manifest::MANIFEST_CURRENT)).ok();
            assert!(matches!(
                publish(dir.path(), &fence, &mut ticket),
                Err(Outcome::Refused("fenced"))
            ));
            assert_eq!(
                std::fs::read(dir.path().join(crate::manifest::MANIFEST_CURRENT)).ok(),
                untouched
            );
            drop(output);
            drop(ticket);
            domain.close();
            wait_exit(&stats);
            // Existing read-only selection and real engine reopen retain the
            // authoritative token and both records despite uncertain adoption.
            let db = crate::engine::DatabaseEngine::open(
                dir.path(),
                &crate::types::DbOptions::default(),
            )
            .unwrap();
            assert!(db.get_node(1).unwrap().is_some());
            assert!(db.get_node(2).unwrap().is_some());
            db.close().unwrap();
        }
        let dir = TempDir::new().unwrap();
        let before = baseline(dir.path(), vec![]);
        let domain = ReclaimDomain::new();
        let mut ticket = prepare(&domain, &before, &before, &[], &[], &[]).unwrap();
        // A failure before any recovery-visible mutation is a private refusal;
        // the conservative publisher still fences the uncertain returned call.
        let originals = std::fs::read(dir.path().join(crate::manifest::MANIFEST_CURRENT)).unwrap();
        persistence_fault::set(Boundary::BeforeTmp, false);
        let fence = CommitFence::new();
        assert!(publish(dir.path(), &fence, &mut ticket).is_err());
        assert_eq!(
            originals,
            std::fs::read(dir.path().join(crate::manifest::MANIFEST_CURRENT)).unwrap()
        );
        let gate_fence = Arc::new(CommitFence::new());
        let gate = gate_fence.gate().unwrap();
        let (ready_tx, ready_rx) = mpsc::channel();
        let next = gate_fence.clone();
        let publisher = std::thread::spawn(move || {
            ready_tx.send(()).unwrap();
            next.gate().is_err()
        });
        ready_rx.recv().unwrap();
        gate.fence();
        drop(gate);
        assert!(publisher.join().unwrap());
        drop(ticket);
        domain.close();
    }
    #[test]
    fn p45a1_component_boundaries_preserve_real_output_identity() {
        let _serial = TEST_SERIAL.lock().unwrap_or_else(|e| e.into_inner());
        for boundary in [
            Boundary::ComponentCreate,
            Boundary::ComponentWrite,
            Boundary::ComponentSync,
            Boundary::ComponentRename,
            Boundary::ComponentDirSync,
        ] {
            let dir = TempDir::new().unwrap();
            let memtable = crate::memtable::Memtable::new();
            memtable.apply_op(&WalOp::UpsertNode(node(1)), 1);
            let out = segment_dir(dir.path(), 1);
            let fence = CommitFence::new();
            let gate = fence.gate().unwrap();
            persistence_fault::set(boundary, false);
            assert!(write_segment_with_degree_overlay_and_secondary_indexes(
                &out,
                1,
                &memtable,
                None,
                &crate::degree_cache::DegreeOverlaySnapshot::empty(),
                &[]
            )
            .is_err());
            gate.fence();
            drop(gate);
            assert!(fence.is_fenced());
            assert!(out.exists());
            assert!(out.join("segment.core").exists());
            if matches!(
                boundary,
                Boundary::ComponentRename | Boundary::ComponentDirSync
            ) {
                let reader = SegmentReader::open_unpinned_for_test(&out, 1, None).unwrap();
                assert_eq!(reader.get_node(1).unwrap().unwrap().id, 1);
            }
        }
    }
    #[test]
    fn p45a1_actual_mapping_guardian_drops_only_on_worker() {
        let _serial = TEST_SERIAL.lock().unwrap_or_else(|e| e.into_inner());
        #[derive(Default)]
        struct Storage {
            reader: Option<SegmentReader>,
            done: Option<mpsc::Sender<String>>,
        }
        impl Drop for Storage {
            fn drop(&mut self) {
                drop(self.reader.take());
                if let Some(tx) = self.done.take() {
                    tx.send(std::thread::current().name().unwrap_or("caller").into())
                        .unwrap();
                }
            }
        }
        let dir = TempDir::new().unwrap();
        let info = segment(dir.path(), 1, 1);
        let domain = ReclaimDomain::new();
        let stats = domain.stats();
        let (tx, rx) = mpsc::channel();
        let mut storage = domain.build::<Storage>(ResourceClass::Storage).unwrap();
        storage
            .mutate(|s| {
                s.reader = Some(
                    SegmentReader::open_with_info(&segment_dir(dir.path(), 1), &info, None, &[])
                        .unwrap(),
                );
                s.done = Some(tx);
            })
            .unwrap();
        let owner = storage.into_owner().unwrap();
        let read = owner.read();
        drop(owner);
        domain.close();
        assert_eq!(
            read.get()
                .reader
                .as_ref()
                .unwrap()
                .get_node(1)
                .unwrap()
                .unwrap()
                .id,
            1
        );
        assert!(stats.read().occupied > 0);
        drop(read);
        assert_eq!(
            rx.recv_timeout(Duration::from_secs(5)).unwrap(),
            "overgraph-reclaimer"
        );
        wait_exit(&stats);
    }
    #[test]
    fn p45a1_manifest_named_crash_reopens_complete_authority_child() {
        const CHILD: &str = "OVERGRAPH_P45A1_MANIFEST_CHILD";
        if std::env::var_os(CHILD).is_some() {
            let path =
                std::path::PathBuf::from(std::env::var_os("OVERGRAPH_P45A1_CHILD_DIR").unwrap());
            let before = baseline(&path, vec![]);
            let mut after = before;
            after.node_label_tokens.insert("Durable".into(), 2);
            after.next_node_label_id = 3;
            persistence_fault::set(Boundary::CurrentToPrev, true);
            crate::manifest::write_manifest(&path, &after).unwrap();
            unreachable!();
        }
        let _serial = TEST_SERIAL.lock().unwrap_or_else(|e| e.into_inner());
        let dir = TempDir::new().unwrap();
        let output = std::process::Command::new(std::env::current_exe().unwrap()).args(["--exact", "engine::durable_transition::tests::p45a1_manifest_named_crash_reopens_complete_authority_child", "--nocapture"]).env(CHILD, "1").env("OVERGRAPH_P45A1_CHILD_DIR", dir.path()).output().unwrap();
        assert!(!output.status.success());
        assert!(String::from_utf8_lossy(&output.stderr).contains("named crash"));
        let selected = crate::manifest::load_manifest_readonly(dir.path())
            .unwrap()
            .unwrap();
        assert_eq!(selected.node_label_tokens.get("Durable"), Some(&2));
        let db =
            crate::engine::DatabaseEngine::open(dir.path(), &crate::types::DbOptions::default())
                .unwrap();
        db.close().unwrap();
    }
    #[test]
    fn p45a1_completed_publisher_wal_retirement_and_stale_catalog_rejection() {
        let _serial = TEST_SERIAL.lock().unwrap_or_else(|e| e.into_inner());
        let dir = TempDir::new().unwrap();
        let mut before = baseline(dir.path(), vec![]);
        before.active_wal_generation_id = 2;
        before.next_wal_generation_id = 3;
        before
            .pending_flush_epochs
            .push(crate::types::FlushEpochMeta {
                epoch_id: 1,
                wal_generation_id: 1,
                state: crate::types::FlushEpochState::FrozenPendingFlush,
                segment_id: None,
            });
        crate::manifest::write_manifest(dir.path(), &before).unwrap();
        let domain = ReclaimDomain::new();
        let fence = CommitFence::new();
        let mut wal = crate::wal::WalWriter::open_generation(dir.path(), 1).unwrap();
        wal.append_batch(&[(
            1,
            WalOp::EnsureNodeLabel {
                label: "Durable".into(),
                label_id: 2,
            },
        )])
        .unwrap();
        wal.sync().unwrap();
        drop(wal);
        let mut after = before.clone();
        after.node_label_tokens.insert("Durable".into(), 2);
        after.next_node_label_id = 3;
        after.pending_flush_epochs.clear();
        let records = [(
            1,
            WalOp::EnsureNodeLabel {
                label: "Durable".into(),
                label_id: 2,
            },
        )];
        let capability = prepare_retirement(
            &domain,
            dir.path(),
            &fence,
            &before,
            &after,
            &[],
            1,
            &[&records],
        )
        .unwrap();
        let mut completed = prepare(&domain, &before, &after, &[], &[], &[capability]).unwrap();
        publish(dir.path(), &fence, &mut completed).unwrap();
        assert!(!crate::wal::wal_generation_path(dir.path(), 1).exists());
        let authoritative =
            std::fs::read(dir.path().join(crate::manifest::MANIFEST_CURRENT)).unwrap();
        let mut stale = prepare(&domain, &before, &before, &[], &[], &[]).unwrap();
        assert!(matches!(
            publish(dir.path(), &fence, &mut stale),
            Err(Outcome::Refused("stale durable catalog ticket"))
        ));
        assert!(!fence.is_fenced());
        assert_eq!(
            std::fs::read(dir.path().join(crate::manifest::MANIFEST_CURRENT)).unwrap(),
            authoritative
        );
        fence.establish();
        assert!(matches!(
            publish(dir.path(), &fence, &mut completed),
            Err(Outcome::Refused("fenced"))
        ));
        assert_eq!(
            std::fs::read(dir.path().join(crate::manifest::MANIFEST_CURRENT)).unwrap(),
            authoritative
        );
        drop(stale);
        drop(completed);
        let stats = domain.stats();
        domain.close();
        wait_exit(&stats);
    }
    #[test]
    fn p45a1_retirement_capability_rejects_uncovered_or_changed_recovery_sources() {
        let _serial = TEST_SERIAL.lock().unwrap_or_else(|e| e.into_inner());
        for case in 0..27 {
            let success = matches!(case, 7 | 23 | 24);
            let dir = TempDir::new().unwrap();
            let foreign = TempDir::new().unwrap();
            let old_segments = if case == 12 {
                vec![segment(dir.path(), 0, 42)]
            } else {
                vec![]
            };
            let mut before = baseline(dir.path(), old_segments);
            before.active_wal_generation_id = if case == 0 { 1 } else { 2 };
            before.next_wal_generation_id = 3;
            match case {
                18 => before.next_node_label_id = 100,
                19 => before.next_edge_label_id = 100,
                20 => {
                    before.dense_vector = Some(crate::types::DenseVectorConfig {
                        dimension: 2,
                        metric: crate::types::DenseMetric::Cosine,
                        hnsw: Default::default(),
                    })
                }
                25 => before.next_schema_id = 100,
                26 => before.next_secondary_index_id = 100,
                _ => {}
            }
            before
                .pending_flush_epochs
                .push(crate::types::FlushEpochMeta {
                    epoch_id: 11,
                    wal_generation_id: 1,
                    state: crate::types::FlushEpochState::FrozenPendingFlush,
                    segment_id: None,
                });
            if case == 13 {
                before
                    .pending_flush_epochs
                    .push(crate::types::FlushEpochMeta {
                        epoch_id: 12,
                        wal_generation_id: 0,
                        state: crate::types::FlushEpochState::FrozenPendingFlush,
                        segment_id: None,
                    });
                let mut other_wal = crate::wal::WalWriter::open_generation(dir.path(), 0).unwrap();
                other_wal
                    .append_batch(&[(90, WalOp::UpsertNode(node(42)))])
                    .unwrap();
                other_wal.sync().unwrap();
            }
            crate::manifest::write_manifest(dir.path(), &before).unwrap();
            let records = [
                (
                    101,
                    WalOp::EnsureNodeLabel {
                        label: "Durable".into(),
                        label_id: 2,
                    },
                ),
                (
                    102,
                    match case {
                        21 | 23 => WalOp::DeleteNode {
                            id: 500,
                            deleted_at: 55,
                        },
                        22 | 24 => WalOp::DeleteEdge {
                            id: 500,
                            deleted_at: 55,
                        },
                        _ => WalOp::UpsertNode(node(1)),
                    },
                ),
            ];
            let mut wal = crate::wal::WalWriter::open_generation(dir.path(), 1).unwrap();
            wal.append_batch(&records).unwrap();
            wal.sync().unwrap();
            drop(wal);
            let memtable = crate::memtable::Memtable::new();
            let mut output_node = node(if case == 1 { 2 } else { 1 });
            if case == 17 {
                output_node.sparse_vector = Some(vec![(2, 1.5)]);
            }
            if case == 20 {
                output_node.dense_vector = Some(vec![0.2, 0.3]);
            }
            if matches!(case, 21..=24) {
                memtable.apply_op(&records[1].1, 102);
            } else {
                memtable.apply_op(&WalOp::UpsertNode(output_node), 102);
            }
            let output_path = segment_dir(dir.path(), 3);
            let output = write_segment_with_degree_overlay_and_secondary_indexes(
                &output_path,
                3,
                &memtable,
                before.dense_vector.as_ref(),
                &crate::degree_cache::DegreeOverlaySnapshot::empty(),
                &[],
            )
            .unwrap();
            let mut after = before.clone();
            after.active_wal_generation_id = 2;
            after.pending_flush_epochs.clear();
            after.segments = vec![output.clone()];
            after.next_engine_seq = 103;
            if case != 2 {
                after.node_label_tokens.insert("Durable".into(), 2);
                after.next_node_label_id = 3;
            }
            if case == 10 {
                after.active_wal_generation_id = 3;
            }
            if case == 11 {
                after.node_label_tokens.remove("N");
            }
            if case == 14 {
                after.segments[0].node_count = 0;
            }
            if case == 15 {
                after.segments.push(output.clone());
            }
            if case == 16 {
                let mut unproved = output.clone();
                unproved.id = 2;
                after.segments.push(unproved);
            }
            match case {
                19 => after.next_edge_label_id = 1,
                23 => after.next_node_id = 501,
                24 => after.next_edge_id = 501,
                25 => after.next_schema_id = 1,
                26 => after.next_secondary_index_id = 1,
                _ => {}
            }
            let authority =
                std::fs::read(dir.path().join(crate::manifest::MANIFEST_CURRENT)).unwrap();
            let domain = ReclaimDomain::new();
            let stats = domain.stats();
            let fence = CommitFence::new();
            if case == 3 {
                std::fs::rename(&output_path, dir.path().join("removed-output")).unwrap();
            }
            let seal = prepare_retirement(
                &domain,
                dir.path(),
                &fence,
                &before,
                &after,
                std::slice::from_ref(&output),
                1,
                &[if case == 8 { &records[..1] } else { &records }],
            );
            if case <= 3 || case == 8 || (case >= 10 && !success) {
                assert!(matches!(seal, Err(Outcome::Refused(_))), "case={case}");
            } else {
                let seal = seal.unwrap();
                let mut ticket = prepare(
                    &domain,
                    &before,
                    &after,
                    &[],
                    std::slice::from_ref(&output),
                    &[seal],
                )
                .unwrap();
                match case {
                    4 => {
                        let mut wal =
                            crate::wal::WalWriter::open_generation(dir.path(), 1).unwrap();
                        wal.append_batch(&[(
                            103,
                            WalOp::DeleteNode {
                                id: 1,
                                deleted_at: 55,
                            },
                        )])
                        .unwrap();
                        wal.sync().unwrap();
                    }
                    5 => {
                        let removed = dir.path().join("removed-output");
                        match std::fs::rename(&output_path, &removed) {
                            Ok(()) => {} // Actual change must be refused below.
                            #[cfg(windows)]
                            Err(error) if error.raw_os_error() == Some(5) => {
                                // Windows prevents renaming directories with open
                                // children. That is protection, not an injected
                                // change and not grounds to publish a valid ticket.
                                assert!(output_path.is_dir());
                                assert!(!removed.exists());
                                for file in &ticket.get().retirements[0].seal.get().files {
                                    assert_eq!(file_identity(&file.path, file.len).unwrap(),
                                        (file.len, file.digest));
                                }
                                assert_eq!(std::fs::read(dir.path().join(
                                    crate::manifest::MANIFEST_CURRENT)).unwrap(), authority);
                                assert!(crate::wal::wal_generation_path(dir.path(), 1).exists());
                                drop(ticket);
                                domain.close();
                                wait_exit(&stats);
                                // Normal retirement of the unused ticket must
                                // release the protection without publishing it.
                                std::fs::rename(&output_path, &removed).unwrap();
                                assert!(!output_path.exists());
                                assert!(removed.is_dir());
                                assert_eq!(std::fs::read(dir.path().join(
                                    crate::manifest::MANIFEST_CURRENT)).unwrap(), authority);
                                assert!(crate::wal::wal_generation_path(dir.path(), 1).exists());
                                continue;
                            }
                            Err(error) => panic!("retirement case=5 rename injection: {error:?}"),
                        }
                    }
                    6 => {
                        use std::io::Write;
                        let entry = std::fs::read_dir(&output_path)
                            .unwrap()
                            .next()
                            .unwrap()
                            .unwrap();
                        std::fs::OpenOptions::new()
                            .append(true)
                            .open(entry.path())
                            .unwrap()
                            .write_all(&[7])
                            .unwrap();
                    }
                    9 => {
                        crate::manifest::write_manifest(foreign.path(), &before).unwrap();
                        let mut foreign_wal =
                            crate::wal::WalWriter::open_generation(foreign.path(), 1).unwrap();
                        foreign_wal.append_batch(&records).unwrap();
                        foreign_wal.sync().unwrap();
                    }
                    _ => {}
                }
                let publish_dir = if case == 9 {
                    foreign.path()
                } else {
                    dir.path()
                };
                let result = publish(publish_dir, &fence, &mut ticket);
                if !success {
                    assert!(matches!(result, Err(Outcome::Refused(_))), "case={case}");
                    if case == 9 {
                        assert!(crate::wal::wal_generation_path(foreign.path(), 1).exists());
                        assert_eq!(
                            crate::manifest::load_manifest_readonly(foreign.path())
                                .unwrap()
                                .unwrap()
                                .next_engine_seq,
                            before.next_engine_seq
                        );
                    }
                } else {
                    result.unwrap();
                    assert!(!crate::wal::wal_generation_path(dir.path(), 1).exists());
                    let selected = crate::manifest::load_manifest_readonly(dir.path())
                        .unwrap()
                        .unwrap();
                    let mut oracle = super::super::concurrency_proofs::TraceOracle::default();
                    oracle.adopt(std::slice::from_ref(&output));
                    assert_eq!(oracle.check_adoption(&selected.segments), Ok(()));
                    let mut stale = selected.segments.clone();
                    stale[0].segment_data_id = [88; 32];
                    assert_eq!(oracle.check_adoption(&stale), Err("stale adoption"));
                    let reopened = crate::engine::DatabaseEngine::open(
                        dir.path(),
                        &crate::types::DbOptions::default(),
                    )
                    .unwrap();
                    match case {
                        23 => assert!(reopened.get_node(500).unwrap().is_none()),
                        24 => assert!(reopened.get_edge(500).unwrap().is_none()),
                        _ => assert!(reopened.get_node(1).unwrap().is_some()),
                    }
                    reopened.close().unwrap();
                }
                drop(ticket);
            }
            if !success {
                assert_eq!(
                    std::fs::read(dir.path().join(crate::manifest::MANIFEST_CURRENT)).unwrap(),
                    authority
                );
                assert!(crate::wal::wal_generation_path(dir.path(), 1).exists());
                assert!(!fence.is_fenced());
                if case == 12 {
                    assert!(segment_dir(dir.path(), 0).exists());
                }
                if case == 13 {
                    assert!(crate::wal::wal_generation_path(dir.path(), 0).exists());
                }
            }
            domain.close();
            wait_exit(&stats);
        }
    }

    #[test]
    fn p45a1_retirement_rejects_oversized_supplied_wal_before_growth() {
        let _serial = TEST_SERIAL.lock().unwrap_or_else(|e| e.into_inner());
        // Count/encode parity includes every production tag and both vector forms.
        let mut n = node(1);
        n.dense_vector = Some(vec![0.3, 0.4]);
        n.sparse_vector = Some(vec![(3, 1.0)]);
        let edge = crate::types::EdgeRecord {
            id: 1,
            from: 1,
            to: 2,
            label_id: 1,
            props: n.props.clone(),
            created_at: 1,
            updated_at: 2,
            weight: 1.0,
            valid_from: 0,
            valid_to: 10,
            last_write_seq: 1,
        };
        let ops = [
            WalOp::UpsertNode(n),
            WalOp::UpsertEdge(edge),
            WalOp::DeleteNode {
                id: 1,
                deleted_at: 2,
            },
            WalOp::DeleteEdge {
                id: 1,
                deleted_at: 2,
            },
            WalOp::EnsureNodeLabel {
                label: "N".into(),
                label_id: 1,
            },
            WalOp::EnsureEdgeLabel {
                label: "E".into(),
                label_id: 1,
            },
            WalOp::BeginAtomicBatch {
                first_seq: 1,
                op_count: 2,
            },
            WalOp::CommitAtomicBatch {
                first_seq: 1,
                op_count: 2,
            },
        ];
        for op in &ops {
            let bytes = crate::encoding::encode_wal_op(op).unwrap();
            assert_eq!(
                crate::encoding::wal_op_encoded_len(op, bytes.len()).unwrap(),
                bytes.len()
            );
            assert!(crate::encoding::wal_op_encoded_len(op, bytes.len() - 1).is_err());
        }
        let batch = ops
            .into_iter()
            .enumerate()
            .map(|(i, op)| (i as u64 + 1, op))
            .collect::<Vec<_>>();
        let mut encoded = Vec::new();
        let mut scratch = Vec::new();
        crate::wal::prepare_v3_batch_into(&batch, &mut scratch, &mut encoded).unwrap();
        let length = encoded.len() + 8;
        assert_eq!(
            crate::wal::v3_batches_encoded_len(&[&[], &batch], length).unwrap(),
            length
        );
        assert!(crate::wal::v3_batches_encoded_len(&[&batch], length - 1).is_err());
        let dir = TempDir::new().unwrap();
        let mut before = baseline(dir.path(), vec![]);
        before.active_wal_generation_id = 2;
        before.next_wal_generation_id = 3;
        before
            .pending_flush_epochs
            .push(crate::types::FlushEpochMeta {
                epoch_id: 1,
                wal_generation_id: 1,
                state: crate::types::FlushEpochState::FrozenPendingFlush,
                segment_id: None,
            });
        crate::manifest::write_manifest(dir.path(), &before).unwrap();
        let actual = [(
            1,
            WalOp::EnsureNodeLabel {
                label: "N".into(),
                label_id: 1,
            },
        )];
        let mut wal = crate::wal::WalWriter::open_generation(dir.path(), 1).unwrap();
        wal.append_batch(&actual).unwrap();
        wal.sync().unwrap();
        drop(wal);
        let mut after = before.clone();
        after.pending_flush_epochs.clear();
        let wal_path = crate::wal::wal_generation_path(dir.path(), 1);
        let wal_bytes = std::fs::read(&wal_path).unwrap();
        let authority = std::fs::read(dir.path().join(crate::manifest::MANIFEST_CURRENT)).unwrap();
        let domain = ReclaimDomain::new();
        let stats = domain.stats();
        let initial = stats.read().occupied;
        let fence = CommitFence::new();
        for many in [false, true] {
            let mut supplied = Vec::new();
            if many {
                supplied.extend((1..=100000).map(|seq| {
                    (
                        seq,
                        WalOp::DeleteNode {
                            id: 1,
                            deleted_at: 50,
                        },
                    )
                }));
            } else {
                let mut large = node(1);
                large.props.insert(
                    "data".into(),
                    crate::types::PropValue::Bytes(vec![7; 4 * 1024 * 1024]),
                );
                supplied.push((1, WalOp::UpsertNode(large)));
            }
            let (result, allocations) = super::super::participant::allocations::measure(|| {
                prepare_retirement(
                    &domain,
                    dir.path(),
                    &fence,
                    &before,
                    &after,
                    &[],
                    1,
                    &[&supplied],
                )
            });
            assert!(matches!(
                result,
                Err(Outcome::Refused("supplied WAL exceeds sealed capacity"))
            ));
            assert!(
                allocations.2 < 64 * 1024,
                "oversized trace allocated {allocations:?}"
            );
            assert_eq!(stats.read().occupied, initial);
            assert_eq!(stats.read().occupied, 0);
            assert_eq!(std::fs::read(&wal_path).unwrap(), wal_bytes);
            assert_eq!(
                std::fs::read(dir.path().join(crate::manifest::MANIFEST_CURRENT)).unwrap(),
                authority
            );
            assert!(!fence.is_fenced());
            eprintln!("M6 many={many} supplied_ops={} measured_allocated_bytes={} retained_bytes={} occupied={}", supplied.len(), allocations.2, stats.read().occupied, stats.read().occupied);
        }
        domain.close();
        wait_exit(&stats);
    }
}

#[cfg(all(test, feature = "production-concurrency"))]
mod local_ticket_origin_tests {
    use super::*;
    #[test]
    fn p45a3_local_ticket_rejects_foreign_shared_retirement_before_exposure() {
        let one = ReclaimDomain::new();
        let two = ReclaimDomain::new();
        let manifest = crate::manifest::default_manifest();
        let seal = two
            .build::<SealedRetirement>(ResourceClass::Storage)
            .unwrap()
            .into_owner()
            .unwrap();
        let foreign = RetirementCapability { seal };
        assert!(matches!(
            prepare(&one, &manifest, &manifest, &[], &[], &[foreign]),
            Err(Outcome::Refused("foreign retirement capability"))
        ));
        one.close();
        two.close();
    }
}
