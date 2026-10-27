// Candidate-only immutable secondary bindings and one online build registration.
// The coordinator owns registration/complete append/adoption. The existing index
// worker owns base construction and applied prefixes; the short suffix mutex only
// transfers complete, coordinator-accepted commits and their logical charge.
use crate::memtable::secondary::{FixedCatalog, SecondaryMaps, SecondaryTransition};

const INDEX_BUILD_SUFFIX_LIMIT: usize = 16 * 1024 * 1024;
type PreparedCandidateIndexBuild = (
    CandidateIndexBuild,
    CandidateIndexWork,
    std::sync::mpsc::SyncSender<Result<CandidateIndexBuilt, EngineError>>,
);

#[derive(Default)]
struct CandidateDdl {
    build: Option<CandidateIndexBuild>,
    next_incarnation: u64,
    followups: Arc<CandidateIndexFollowups>,
    cleanup_paths: HashSet<PathBuf>,
    sweep_pending: bool,
    drop_paths: HashMap<PathBuf, u64>,
    last_cleanup: Option<Instant>,
    #[cfg(test)]
    component_fault: Mutex<Option<crate::persistence_fault::Boundary>>,
    #[cfg(test)]
    component_abort: AtomicBool,
    #[cfg(test)]
    target_publish_pause: Mutex<Option<SecondaryIndexBuildPauseHook>>,
}
#[derive(Default)]
struct CandidateIndexFollowups {
    ready: AtomicBool,
    in_flight: AtomicU64,
    pending: Mutex<HashMap<u64, CandidateIndexFollowup>>,
}
type CandidateComponentIdentity = (
    u64,
    [u8; 32],
    u64,
    crate::segment_components::ComponentHandleV1,
);
struct CandidateIndexFollowup {
    incarnation: u64,
    components: Vec<CandidateComponentIdentity>,
    followup: SecondaryIndexReadFollowup,
}
fn candidate_index_components(
    segments: &[SharedArc<SegmentReader>],
    index: u64,
) -> Vec<CandidateComponentIdentity> {
    let mut identities: Vec<_> = segments
        .iter()
        .filter_map(|s| {
            s.secondary_component_identity(index)
                .map(|(id, generation, handle)| (s.segment_id, id, generation, handle))
        })
        .collect();
    identities.sort_unstable_by_key(|v| v.0);
    identities
}
struct CandidateIndexBatch {
    generation: u64,
    frontier: u64,
    transitions: Vec<(u64, SecondaryTransition)>,
    bytes: usize,
}
struct CandidateIndexSuffix {
    accepted_frontier: u64,
    applied_frontier: u64,
    queued: VecDeque<CandidateIndexBatch>,
    // Includes prefixes currently owned by the worker until application finishes.
    bytes: usize,
    peak: usize,
    limit: usize,
}
struct CandidateIndexBuild {
    incarnation: u64,
    entry: SecondaryIndexManifestEntry,
    target: SharedArc<FixedCatalog>,
    generations: Vec<(u64, SharedArc<Memtable>)>,
    segments: Vec<SharedArc<SegmentReader>>,
    suffix: Arc<Mutex<CandidateIndexSuffix>>,
    result: Mutex<std::sync::mpsc::Receiver<Result<CandidateIndexBuilt, EngineError>>>,
    cancel: Arc<AtomicBool>,
    _heavy: CandidateHeavyLease,
}
struct CandidateIndexWork {
    entry: SecondaryIndexManifestEntry,
    frontier: u64,
    generations: Vec<(u64, SharedArc<Memtable>, u64, u64)>,
    segments: Vec<SharedArc<SegmentReader>>,
    suffix: Arc<Mutex<CandidateIndexSuffix>>,
    target: SharedArc<FixedCatalog>,
    domain: reclamation::DomainHandle,
    authority: Arc<ManifestLock>,
    db_dir: PathBuf,
    dense_config: Option<DenseVectorConfig>,
    entries: Vec<SecondaryIndexManifestEntry>,
    cancel: Arc<AtomicBool>,
    #[cfg(test)]
    pause: Option<SecondaryIndexBuildPauseHook>,
    #[cfg(test)]
    component_fault: Option<crate::persistence_fault::Boundary>,
    #[cfg(test)]
    component_abort: bool,
    #[cfg(test)]
    target_publish_pause: Option<SecondaryIndexBuildPauseHook>,
}
struct CandidateIndexBuilt {
    obsolete: Vec<PathBuf>,
    maps: BTreeMap<u64, SecondaryMaps>,
    segments: Vec<SharedArc<SegmentReader>>,
}
struct CandidateBindings {
    catalog: SharedArc<FixedCatalog>,
    active: SharedArc<Memtable>,
    epochs: Vec<ImmutableEpoch>,
    segments: Vec<SharedArc<SegmentReader>>,
    sources: SharedArc<PublishedReadSources>,
}

impl CandidateIndexBatch {
    fn new(
        generation: u64,
        frontier: u64,
        transitions: Vec<(u64, SecondaryTransition)>,
    ) -> Result<Self, EngineError> {
        let mut bytes: usize = if transitions.is_empty() { 0 } else { 24 };
        for (_, transition) in &transitions {
            bytes = bytes
                .checked_add(8)
                .and_then(|n| n.checked_add(transition.encoded_bytes().ok()?))
                .ok_or_else(|| {
                    EngineError::InvalidOperation("index suffix length overflow".into())
                })?;
        }
        Ok(Self {
            generation,
            frontier,
            transitions,
            bytes,
        })
    }
}
impl CandidateIndexSuffix {
    fn admit(&self, bytes: usize) -> Result<(), EngineError> {
        let requested = self.bytes.saturating_add(bytes);
        if requested > self.limit {
            return Err(EngineError::ResourceLimit {
                resource: "index_build_suffix",
                requested: requested as u64,
                limit: self.limit as u64,
                retryable: true,
            });
        }
        Ok(())
    }
    fn append(&mut self, first: u64, batch: CandidateIndexBatch) {
        assert_eq!(
            first,
            self.accepted_frontier + 1,
            "every complete accepted commit enters the registered suffix"
        );
        assert!(batch.frontier >= first);
        self.accepted_frontier = batch.frontier;
        if batch.bytes != 0 {
            self.bytes = self
                .bytes
                .checked_add(batch.bytes)
                .expect("pre-WAL checked suffix");
            assert!(self.bytes <= self.limit);
            self.peak = self.peak.max(self.bytes);
            self.queued.push_back(batch);
        }
    }
    fn take_prefix(&mut self) -> (VecDeque<CandidateIndexBatch>, u64) {
        (std::mem::take(&mut self.queued), self.accepted_frontier)
    }
}

impl EngineCore {
    fn candidate_catalog(
        &self,
        entries: &[SecondaryIndexManifestEntry],
        changed: Option<(u64, u64)>,
    ) -> Result<SharedArc<FixedCatalog>, EngineError> {
        let mut value = FixedCatalog::new(entries)?;
        if let Some(previous) = self.memtable.fixed_secondary_catalog() {
            for (&id, incarnation) in &mut value.incarnations {
                if let Some(old) = previous.incarnations.get(&id) {
                    *incarnation = *old;
                }
            }
        }
        if let Some((id, incarnation)) = changed {
            value.incarnations.insert(id, incarnation);
        }
        let mut value = SharedArc::new(value);
        value.guard(
            &self.candidate.as_ref().unwrap().domain,
            reclamation::ResourceClass::Catalog,
        )?;
        Ok(value)
    }
    fn candidate_bindings(
        &self,
        catalog: SharedArc<FixedCatalog>,
        maps: Option<&BTreeMap<u64, SecondaryMaps>>,
        segments: Vec<SharedArc<SegmentReader>>,
    ) -> Result<CandidateBindings, EngineError> {
        let domain = &self.candidate.as_ref().unwrap().domain;
        let active = self.share(
            self.memtable.rebind_secondary(
                catalog.clone(),
                maps.and_then(|m| m.get(&self.active_wal_generation_id)),
                domain,
            )?,
            reclamation::ResourceClass::Generation,
        );
        let mut epochs = self.immutable_epochs.clone();
        for epoch in &mut epochs {
            epoch.memtable = self.share(
                epoch.memtable.rebind_secondary(
                    catalog.clone(),
                    maps.and_then(|m| m.get(&epoch.wal_generation_id)),
                    domain,
                )?,
                reclamation::ResourceClass::Generation,
            );
        }
        let sources = self.build_published_read_sources_for(
            self.published_read_sources_generation
                .checked_add(1)
                .ok_or_else(|| {
                    EngineError::InvalidOperation("source generation exhausted".into())
                })?,
            &active,
            &epochs,
            &segments,
        )?;
        Ok(CandidateBindings {
            catalog,
            active,
            epochs,
            segments,
            sources,
        })
    }
    fn install_candidate_bindings(&mut self, bindings: CandidateBindings) {
        self.fixed_secondary_catalog = Some(bindings.catalog);
        self.memtable = bindings.active;
        self.immutable_epochs = bindings.epochs;
        self.segments = bindings.segments;
        self.published_read_sources_generation += 1;
        self.published_read_sources = Some(bindings.sources);
        self.refresh_immutable_bytes_total();
    }
    fn prepare_candidate_index_build(
        &self,
        entry: &SecondaryIndexManifestEntry,
        incarnation: u64,
    ) -> Result<PreparedCandidateIndexBuild, EngineError> {
        let candidate = self.candidate.as_ref().unwrap();
        let mut target_entry = entry.clone();
        target_entry.state = SecondaryIndexState::Ready;
        let target =
            self.candidate_catalog(&[target_entry], Some((entry.index_id, incarnation)))?;
        let mut generations = vec![(self.active_wal_generation_id, self.memtable.clone())];
        generations.extend(
            self.immutable_epochs
                .iter()
                .map(|e| (e.wal_generation_id, e.memtable.clone())),
        );
        let suffix = Arc::new(Mutex::new(CandidateIndexSuffix {
            accepted_frontier: self.engine_seq,
            applied_frontier: self.engine_seq,
            queued: VecDeque::new(),
            bytes: 0,
            peak: 0,
            limit: INDEX_BUILD_SUFFIX_LIMIT,
        }));
        let (tx, rx) = std::sync::mpsc::sync_channel(1);
        let cancel = Arc::new(AtomicBool::new(false));
        let work = CandidateIndexWork {
            entry: entry.clone(),
            frontier: self.engine_seq,
            generations: generations
                .iter()
                .map(|(id, m)| (*id, m.clone(), m.max_node_id(), m.max_edge_id()))
                .collect(),
            segments: self.segments.clone(),
            suffix: suffix.clone(),
            target: target.clone(),
            domain: candidate.domain.clone(),
            authority: self.manifest_write_lock.clone(),
            db_dir: self.db_dir.clone(),
            dense_config: self.manifest.dense_vector.clone(),
            entries: self.manifest.secondary_indexes.clone(),
            cancel: cancel.clone(),
            #[cfg(test)]
            pause: self.secondary_index_build_pause.lock().unwrap().take(),
            #[cfg(test)]
            component_fault: candidate.ddl.component_fault.lock().unwrap().take(),
            #[cfg(test)]
            component_abort: candidate.ddl.component_abort.swap(false, Ordering::AcqRel),
            #[cfg(test)]
            target_publish_pause: candidate.ddl.target_publish_pause.lock().unwrap().take(),
        };
        let build = CandidateIndexBuild {
            incarnation,
            entry: entry.clone(),
            target,
            generations,
            segments: self.segments.clone(),
            suffix,
            result: Mutex::new(rx),
            cancel,
            _heavy: self.acquire_heavy_job().expect("candidate heavy lease"),
        };
        Ok((build, work, tx))
    }
    fn dispatch_candidate_build(
        &mut self,
        build: CandidateIndexBuild,
        work: CandidateIndexWork,
        result: std::sync::mpsc::SyncSender<Result<CandidateIndexBuilt, EngineError>>,
    ) -> Result<(), EngineError> {
        self.candidate.as_mut().unwrap().ddl.build = Some(build);
        self.ensure_secondary_index_worker();
        self.secondary_index_bg
            .as_ref()
            .unwrap()
            .job_tx
            .send(SecondaryIndexJob::CandidateBuild { work, result })
            .map_err(|_| EngineError::InvalidOperation("index worker stopped".into()))
    }
    fn candidate_ensure_index(
        &mut self,
        label: &str,
        spec: SecondaryIndexSpec,
        edge: bool,
    ) -> Result<(SecondaryIndexManifestEntry, PublishImpact), EngineError> {
        if edge {
            spec.validate_for_edge()?;
        } else {
            spec.validate_for_node()?;
        }
        let catalog = self.label_catalog.read().unwrap();
        let mut labels = LabelResolutionPlan::from_catalog(&catalog);
        let label_id = if edge {
            labels.resolve_edge_label_for_write(label)?
        } else {
            labels.resolve_node_label_for_write(label)?
        };
        let (nodes, edges) = labels.token_creations();
        drop(labels);
        drop(catalog);
        let target = if edge {
            spec.edge_target(label_id)?
        } else {
            spec.node_target(label_id)?
        };
        let key = SecondaryIndexLookupKey {
            discriminant: secondary_index_target_discriminant(&target),
            target_label_id: label_id,
            fields: target.public_fields(),
            kind: spec.kind.clone(),
        };
        let mut manifest = self.load_current_manifest_for_write()?;
        if let Some(existing) = manifest.secondary_indexes.iter().find(|e| {
            secondary_index_lookup_key(e) == key && e.state != SecondaryIndexState::Failed
        }) {
            return Ok((existing.clone(), PublishImpact::NoPublish));
        }
        let staged = vec![(label.to_owned(), label_id)];
        stage_label_tokens_in_manifest(
            &mut manifest,
            if edge { &[] } else { &staged },
            if edge { &staged } else { &[] },
        )?;
        let entry = if let Some(existing) = manifest
            .secondary_indexes
            .iter_mut()
            .find(|e| secondary_index_lookup_key(e) == key)
        {
            existing.state = SecondaryIndexState::Building;
            existing.last_error = None;
            existing.clone()
        } else {
            let id = manifest.next_secondary_index_id;
            manifest.next_secondary_index_id = id.checked_add(1).ok_or_else(|| {
                EngineError::InvalidOperation("secondary index identity exhausted".into())
            })?;
            let e = SecondaryIndexManifestEntry {
                index_id: id,
                target,
                kind: spec.kind,
                state: SecondaryIndexState::Building,
                last_error: None,
            };
            manifest.secondary_indexes.push(e.clone());
            e
        };
        let incarnation = self
            .candidate
            .as_ref()
            .unwrap()
            .ddl
            .next_incarnation
            .checked_add(1)
            .ok_or_else(|| EngineError::InvalidOperation("index incarnation exhausted".into()))?;
        let fixed = self.candidate_catalog(
            &manifest.secondary_indexes,
            Some((entry.index_id, incarnation)),
        )?;
        let bindings = self.candidate_bindings(
            fixed,
            None,
            self.segments.iter().map(SharedArc::fork_owner).collect(),
        )?;
        let (build, mut work, tx) = self.prepare_candidate_index_build(&entry, incarnation)?;
        work.entries = manifest.secondary_indexes.clone();
        self.ensure_secondary_index_worker();
        self.merge_checkpointed_runtime_manifest_state(&mut manifest);
        {
            let authority = self.manifest_write_lock.clone();
            let gate = authority.gate()?;
            gate.persist(&self.db_dir, &manifest)?;
        }
        self.candidate.as_mut().unwrap().accepted = true;
        self.manifest = manifest;
        self.apply_manifest_token_creations(&nodes, &edges)?;
        self.install_candidate_bindings(bindings);
        self.rebuild_secondary_index_catalog()?;
        self.candidate.as_mut().unwrap().ddl.next_incarnation = incarnation;
        self.dispatch_candidate_build(build, work, tx)?;
        Ok((entry, PublishImpact::SnapshotWithLabelCatalog))
    }
    fn candidate_drop_index(
        &mut self,
        label: &str,
        spec: SecondaryIndexSpec,
        edge: bool,
    ) -> Result<(bool, PublishImpact), EngineError> {
        if edge {
            spec.validate_for_edge()?;
        } else {
            spec.validate_for_node()?;
        }
        let catalog = self.label_catalog.read().unwrap();
        let id = if edge {
            resolve_edge_label_for_read(&catalog, label)?
        } else {
            resolve_node_label_for_read(&catalog, label)?
        };
        drop(catalog);
        let Some(id) = id else {
            return Ok((false, PublishImpact::NoPublish));
        };
        let target = if edge {
            spec.edge_target(id)?
        } else {
            spec.node_target(id)?
        };
        let key = SecondaryIndexLookupKey {
            discriminant: secondary_index_target_discriminant(&target),
            target_label_id: id,
            fields: target.public_fields(),
            kind: spec.kind,
        };
        let mut manifest = self.load_current_manifest_for_write()?;
        let Some(pos) = manifest
            .secondary_indexes
            .iter()
            .position(|e| secondary_index_lookup_key(e) == key)
        else {
            return Ok((false, PublishImpact::NoPublish));
        };
        let entry = manifest.secondary_indexes.remove(pos);
        let fixed = self.candidate_catalog(&manifest.secondary_indexes, None)?;
        let domain = &self.candidate.as_ref().unwrap().domain;
        let mut segments = Vec::new();
        for old in &self.segments {
            let info = self
                .manifest
                .segments
                .iter()
                .find(|s| s.id == old.segment_id)
                .expect("current segment");
            let mut reader = SegmentReader::open_with_info(
                &segment_dir(&self.db_dir, old.segment_id),
                info,
                self.manifest.dense_vector.as_ref(),
                &manifest.secondary_indexes,
            )?;
            reader.retain_secondary_payloads(Some(old));
            reader.discard_secondary_binding(&entry);
            for declared in &manifest.secondary_indexes {
                reader.warm_declared_index_runtime_coverage(declared);
            }
            let mut reader = SharedArc::new(reader);
            reader.guard_storage(domain, reader.segment_id)?;
            reader.pin_components(domain, reader.component_paths())?;
            segments.push(reader);
        }
        let bindings = self.candidate_bindings(fixed, None, segments)?;
        self.merge_checkpointed_runtime_manifest_state(&mut manifest);
        {
            let authority = self.manifest_write_lock.clone();
            let gate = authority.gate()?;
            gate.persist(&self.db_dir, &manifest)?;
        }
        self.candidate.as_mut().unwrap().accepted = true;
        self.manifest = manifest;
        // Component records are retired under the same gate; files wait for exact
        // retained mappings and recoverable component/root authority.
        let authority = self.manifest_write_lock.clone();
        let gate = authority.gate()?;
        for segment in &self.segments {
            match remove_secondary_index_component_records(
                &segment_dir(&self.db_dir, segment.segment_id),
                &entry,
            ) {
                Ok(paths) => {
                    let ddl = &mut self.candidate.as_mut().unwrap().ddl;
                    for path in paths {
                        ddl.drop_paths.insert(path.clone(), entry.index_id);
                        ddl.cleanup_paths.insert(path);
                    }
                }
                Err(error) => {
                    gate.fence();
                    return Err(EngineError::CommitOutcome {
                        state: CommitOutcomeState::Indeterminate,
                        message: format!("component drop publication uncertain: {error}"),
                    });
                }
            }
        }
        drop(gate);
        self.install_candidate_bindings(bindings);
        self.rebuild_secondary_index_catalog()?;
        self.candidate.as_mut().unwrap().ddl.sweep_pending = true;
        self.notify_candidate_component_cleanup();
        Ok((true, PublishImpact::SnapshotWithLabelCatalog))
    }
    fn prepare_candidate_suffix(
        &self,
        prepared: &mut crate::memtable::primary::PreparedPrimary,
        first: u64,
        frontier: u64,
    ) -> Result<Option<CandidateIndexBatch>, EngineError> {
        let Some(build) = self.candidate.as_ref().and_then(|c| c.ddl.build.as_ref()) else {
            return Ok(None);
        };
        let batch = CandidateIndexBatch::new(
            self.active_wal_generation_id,
            frontier,
            prepared.take_build_transitions(),
        )?;
        let suffix = build.suffix.lock().unwrap();
        assert_eq!(suffix.accepted_frontier.checked_add(1), Some(first));
        suffix.admit(batch.bytes)?;
        Ok(Some(batch))
    }
    fn append_candidate_suffix(&self, first: u64, batch: Option<CandidateIndexBatch>) {
        if let Some(batch) = batch {
            self.candidate
                .as_ref()
                .unwrap()
                .ddl
                .build
                .as_ref()
                .expect("registered until adoption")
                .suffix
                .lock()
                .unwrap()
                .append(first, batch);
        }
    }
    fn service_candidate_index_build(&mut self) -> Result<bool, EngineError> {
        let result = self
            .candidate
            .as_ref()
            .and_then(|c| c.ddl.build.as_ref())
            .and_then(|b| match b.result.lock().unwrap().try_recv() {
                Ok(r) => Some(r),
                Err(std::sync::mpsc::TryRecvError::Empty) => None,
                Err(std::sync::mpsc::TryRecvError::Disconnected) => Some(Err(
                    EngineError::InvalidOperation("index worker result lost".into()),
                )),
            });
        if let Some(result) = result {
            let build = self.candidate.as_mut().unwrap().ddl.build.take().unwrap();
            if self.manifest_write_lock.fenced() {
                return Err(EngineError::CommitOutcome {
                    state: CommitOutcomeState::Indeterminate,
                    message: "index build stopped after durable fence".into(),
                });
            }
            let current = self.memtable.fixed_secondary_catalog().unwrap();
            if current.incarnations.get(&build.entry.index_id) != Some(&build.incarnation)
                || !self.manifest.secondary_indexes.iter().any(|e| {
                    e.index_id == build.entry.index_id && e.state == SecondaryIndexState::Building
                })
            {
                // An old result cannot demote or publish over a replacement. Its
                // guarded output retires; the current declaration owns progress.
                return Ok(true);
            }
            let sources_match = self.segments.len() == build.segments.len()
                && self
                    .segments
                    .iter()
                    .zip(&build.segments)
                    .all(|(a, b)| SharedArc::ptr_eq(a, b))
                && build.generations.iter().all(|(id, old)| {
                    let now = if *id == self.active_wal_generation_id {
                        Some(&self.memtable)
                    } else {
                        self.immutable_epochs
                            .iter()
                            .find(|e| e.wal_generation_id == *id)
                            .map(|e| &e.memtable)
                    };
                    now.is_some_and(|m| m.same_generation(old))
                });
            let result = if sources_match {
                result
            } else {
                Err(EngineError::InvalidOperation(
                    "stale index source lineage".into(),
                ))
            };
            match result {
                Ok(mut built) => {
                    let participant = self.memtable.participant_handle().unwrap();
                    let mut suffix = build.suffix.lock().unwrap();
                    if suffix.accepted_frontier != self.engine_seq {
                        return Err(EngineError::InvalidOperation(
                            "incomplete index change frontier".into(),
                        ));
                    }
                    let (tail, frontier) = suffix.take_prefix();
                    let applied = apply_candidate_index_batches(
                        &mut built.maps,
                        tail,
                        &participant,
                        &build.target,
                        &self.candidate.as_ref().unwrap().domain,
                    )?;
                    suffix.bytes = suffix
                        .bytes
                        .checked_sub(applied)
                        .expect("final owned suffix charge");
                    suffix.applied_frontier = frontier;
                    assert_eq!(suffix.bytes, 0);
                    drop(suffix);
                    let mut manifest = self.load_current_manifest_for_write()?;
                    let entry = manifest
                        .secondary_indexes
                        .iter_mut()
                        .find(|e| e.index_id == build.entry.index_id)
                        .ok_or_else(|| {
                            EngineError::InvalidOperation("index declaration lost".into())
                        })?;
                    entry.state = SecondaryIndexState::Ready;
                    entry.last_error = None;
                    let catalog = self.candidate_catalog(&manifest.secondary_indexes, None)?;
                    // Generations born after C with no target changes are valid empty maps.
                    let bindings =
                        self.candidate_bindings(catalog, Some(&built.maps), built.segments)?;
                    self.merge_checkpointed_runtime_manifest_state(&mut manifest);
                    {
                        let authority = self.manifest_write_lock.clone();
                        let gate = authority.gate()?;
                        gate.persist(&self.db_dir, &manifest)?;
                    }
                    self.candidate.as_mut().unwrap().accepted = true;
                    self.manifest = manifest;
                    self.install_candidate_bindings(bindings);
                    self.rebuild_secondary_index_catalog()?;
                    self.candidate
                        .as_mut()
                        .unwrap()
                        .ddl
                        .cleanup_paths
                        .extend(built.obsolete);
                    self.notify_candidate_component_cleanup();
                }
                Err(error) => {
                    let mut manifest = self.load_current_manifest_for_write()?;
                    if let Some(entry) = manifest.secondary_indexes.iter_mut().find(|e| {
                        e.index_id == build.entry.index_id
                            && e.state == SecondaryIndexState::Building
                    }) {
                        entry.state = SecondaryIndexState::Failed;
                        entry.last_error = Some(secondary_index_failure_message_for_entry(
                            entry,
                            error.to_string(),
                        ));
                    }
                    let fixed = self.candidate_catalog(&manifest.secondary_indexes, None)?;
                    let bindings = self.candidate_bindings(
                        fixed,
                        None,
                        self.segments.iter().map(SharedArc::fork_owner).collect(),
                    )?;
                    self.merge_checkpointed_runtime_manifest_state(&mut manifest);
                    {
                        let authority = self.manifest_write_lock.clone();
                        let gate = authority.gate()?;
                        gate.persist(&self.db_dir, &manifest)?;
                    }
                    self.candidate.as_mut().unwrap().accepted = true;
                    self.manifest = manifest;
                    self.install_candidate_bindings(bindings);
                    self.rebuild_secondary_index_catalog()?;
                }
            }
            self.candidate.as_mut().unwrap().ddl.sweep_pending = true;
            self.notify_candidate_component_cleanup();
            drop(build); // Heavy lease survives final catch-up and durable/runtime adoption.
            return Ok(true);
        }
        Ok(false)
    }
    fn pending_candidate_index(&mut self) -> Option<&SecondaryIndexManifestEntry> {
        let entry = self
            .fixed_secondary_catalog
            .as_ref()?
            .first_building_entry();
        #[cfg(test)]
        if entry.is_some() {
            self.post45_work[4] += 1;
        }
        entry
    }
    fn start_pending_candidate_index_build(&mut self) -> Result<bool, EngineError> {
        let Some(candidate) = &self.candidate else {
            return Ok(false);
        };
        if candidate.ddl.build.is_some()
            || candidate.heavy.load(Ordering::Acquire)
            || !self.immutable_epochs.is_empty()
            || self.manifest_write_lock.fenced()
        {
            return Ok(false);
        }
        let Some(entry) = self.pending_candidate_index().cloned() else {
            return Ok(false);
        };
        let incarnation = self
            .memtable
            .fixed_secondary_catalog()
            .unwrap()
            .incarnations[&entry.index_id];
        let (build, work, tx) = self.prepare_candidate_index_build(&entry, incarnation)?;
        self.dispatch_candidate_build(build, work, tx)?;
        Ok(true)
    }
    fn service_candidate_index_followup(&mut self) -> Result<bool, EngineError> {
        if self.manifest_write_lock.fenced()
            || self
                .candidate
                .as_ref()
                .is_some_and(|c| c.heavy.load(Ordering::Acquire))
        {
            return Ok(false);
        }
        let queue = &self.candidate.as_ref().unwrap().ddl.followups;
        if !queue.ready.load(Ordering::Acquire) {
            return Ok(false);
        }
        let queue = queue.clone();
        let pending = {
            let mut pending = queue.pending.lock().unwrap();
            let key = pending.keys().next().copied();
            let value = key.and_then(|id| {
                pending.remove(&id).map(|value| {
                    queue.in_flight.store(id, Ordering::Release);
                    (id, value)
                })
            });
            queue.ready.store(!pending.is_empty(), Ordering::Release);
            value
        };
        let Some((
            id,
            CandidateIndexFollowup {
                incarnation,
                components,
                followup,
            },
        )) = pending
        else {
            return Ok(false);
        };
        let current = self.memtable.fixed_secondary_catalog().unwrap();
        if current.incarnations.get(&id) != Some(&incarnation)
            || candidate_index_components(&self.segments, id) != components
        {
            return Ok(true);
        }
        let mut manifest = self.load_current_manifest_for_write()?;
        let Some(entry) = manifest
            .secondary_indexes
            .iter_mut()
            .find(|e| e.index_id == id && e.state == SecondaryIndexState::Ready)
        else {
            return Ok(true);
        };
        let error = match followup {
            SecondaryIndexReadFollowup::EqualitySidecarFailure { error, .. }
            | SecondaryIndexReadFollowup::RangeSidecarFailure { error, .. }
            | SecondaryIndexReadFollowup::CompoundEqualitySidecarFailure { error, .. }
            | SecondaryIndexReadFollowup::CompoundRangeSidecarFailure { error, .. } => error,
        };
        if let Some(error) = error.filter(|e| !is_not_found_io_error(e)) {
            entry.state = SecondaryIndexState::Failed;
            entry.last_error = Some(secondary_index_failure_message_for_entry(
                entry,
                error.to_string(),
            ));
        } else {
            entry.state = SecondaryIndexState::Building;
            entry.last_error = None;
        }
        let next = self
            .candidate
            .as_ref()
            .unwrap()
            .ddl
            .next_incarnation
            .checked_add(1)
            .ok_or_else(|| EngineError::InvalidOperation("index incarnation exhausted".into()))?;
        let fixed = self.candidate_catalog(&manifest.secondary_indexes, Some((id, next)))?;
        let bindings = self.candidate_bindings(
            fixed,
            None,
            self.segments.iter().map(SharedArc::fork_owner).collect(),
        )?;
        self.merge_checkpointed_runtime_manifest_state(&mut manifest);
        {
            let authority = self.manifest_write_lock.clone();
            let gate = authority.gate()?;
            gate.persist(&self.db_dir, &manifest)?;
        }
        self.candidate.as_mut().unwrap().accepted = true;
        self.candidate.as_mut().unwrap().ddl.next_incarnation = next;
        self.manifest = manifest;
        self.install_candidate_bindings(bindings);
        self.rebuild_secondary_index_catalog()?;
        Ok(true)
    }
    fn seed_candidate_component_cleanup(&mut self) -> Result<(), EngineError> {
        let authority = self.manifest_write_lock.clone();
        let gate = authority.gate()?;
        let references = gate.recovery_references(&self.db_dir)?;
        let pins = self.candidate.as_ref().unwrap().domain.component_pins();
        let mut pending = HashSet::new();
        let mut dropped = HashMap::new();
        for segment in &self.manifest.segments {
            let dir = segment_dir(&self.db_dir, segment.id);
            let current = crate::segment_writer::recoverable_optional_component_paths(&dir)?;
            for &(id, _, ref relative) in &pins {
                if id == segment.id {
                    let path = dir.join(relative);
                    if path.exists() && !current.contains(&path) {
                        pending.insert(path);
                    }
                }
            }
            for entry in references.secondary_indexes.values().filter(|e| {
                !self
                    .manifest
                    .secondary_indexes
                    .iter()
                    .any(|now| now.index_id == e.index_id)
            }) {
                for path in secondary_index_sidecar_paths_for_entry(&dir, entry) {
                    if path.exists() && !current.contains(&path) {
                        dropped.insert(path.clone(), entry.index_id);
                        pending.insert(path);
                    }
                }
            }
        }
        let ddl = &mut self.candidate.as_mut().unwrap().ddl;
        ddl.cleanup_paths.extend(pending);
        ddl.drop_paths.extend(dropped);
        Ok(())
    }
    fn notify_candidate_component_cleanup(&self) {
        if let Some(runtime) = self.runtime.as_ref().and_then(Weak::upgrade) {
            if let Some(candidate) = &runtime.candidate {
                candidate.cleanup_pending.store(true, Ordering::Release);
            }
            runtime.notify_lifecycle_work();
        }
    }
    fn cleanup_candidate_components(&mut self) {
        let Some(candidate) = &self.candidate else {
            return;
        };
        if (candidate.ddl.cleanup_paths.is_empty() && !candidate.ddl.sweep_pending)
            || candidate.heavy.load(Ordering::Acquire)
            || self.manifest_write_lock.fenced()
            || candidate
                .ddl
                .last_cleanup
                .is_some_and(|t| t.elapsed() < std::time::Duration::from_millis(10))
        {
            return;
        }
        self.candidate.as_mut().unwrap().ddl.last_cleanup = Some(Instant::now());
        let authority = self.manifest_write_lock.clone();
        let Ok(gate) = authority.gate() else {
            return;
        };
        let Ok(root_references) = gate.recovery_references(&self.db_dir) else {
            return;
        };
        if self.candidate.as_ref().unwrap().ddl.sweep_pending {
            let mut pending = HashSet::new();
            let mut dropped = HashMap::new();
            for segment in &self.manifest.segments {
                let dir = segment_dir(&self.db_dir, segment.id);
                let Ok(orphans) = crate::segment_writer::optional_orphan_component_paths(&dir)
                else {
                    // Recheck at the next maintenance boundary; never infer an
                    // empty authority from unreadable component metadata.
                    return;
                };
                pending.extend(orphans);
                for entry in root_references.secondary_indexes.values().filter(|entry| {
                    !self
                        .manifest
                        .secondary_indexes
                        .iter()
                        .any(|now| now.index_id == entry.index_id)
                }) {
                    for path in secondary_index_sidecar_paths_for_entry(&dir, entry) {
                        if path.exists() {
                            dropped.insert(path.clone(), entry.index_id);
                            pending.insert(path);
                        }
                    }
                }
            }
            let ddl = &mut self.candidate.as_mut().unwrap().ddl;
            ddl.cleanup_paths.extend(pending);
            ddl.drop_paths.extend(dropped);
            ddl.sweep_pending = false;
        }
        let pins: HashSet<_> = self
            .candidate
            .as_ref()
            .unwrap()
            .domain
            .component_pins()
            .into_iter()
            .map(|(id, _, relative)| segment_dir(&self.db_dir, id).join(relative))
            .collect();
        let mut directories = HashMap::new();
        let mut pending = HashSet::new();
        for path in std::mem::take(&mut self.candidate.as_mut().unwrap().ddl.cleanup_paths) {
            // Optional files may live below secondary/; find their segment root.
            let Some(segment) = path.ancestors().find(|p| {
                p.file_name()
                    .and_then(|s| s.to_str())
                    .is_some_and(|s| s.starts_with("seg_"))
            }) else {
                pending.insert(path);
                continue;
            };
            let root_retained = self
                .candidate
                .as_ref()
                .unwrap()
                .ddl
                .drop_paths
                .get(&path)
                .is_some_and(|id| root_references.secondary_indexes.contains_key(id));
            if root_retained || pins.contains(&path) {
                pending.insert(path);
                continue;
            }
            let references = directories.entry(segment.to_owned()).or_insert_with(|| {
                #[cfg(test)]
                {
                    self.post45_work[1] += 1;
                }
                crate::segment_writer::recoverable_optional_component_paths(segment)
            });
            if references.as_ref().is_err() || references.as_ref().is_ok_and(|r| r.contains(&path))
            {
                pending.insert(path);
                continue;
            }
            match std::fs::remove_file(&path) {
                Ok(()) => {
                    if let Some(parent) = path.parent() {
                        let _ = fsync_dir(parent);
                    }
                }
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
                Err(_) => {
                    pending.insert(path);
                }
            }
        }
        let ddl = &mut self.candidate.as_mut().unwrap().ddl;
        ddl.drop_paths.retain(|path, _| pending.contains(path));
        ddl.cleanup_paths = pending;
        if let Some(runtime) = self.runtime.as_ref().and_then(Weak::upgrade) {
            if let Some(candidate) = &runtime.candidate {
                candidate.cleanup_pending.store(
                    !self.deferred_segment_dir_cleanup.is_empty()
                        || !self
                            .candidate
                            .as_ref()
                            .unwrap()
                            .ddl
                            .cleanup_paths
                            .is_empty(),
                    Ordering::Release,
                );
            }
        }
    }
    fn cancel_candidate_index_build(&mut self) {
        if let Some(build) = self.candidate.as_ref().and_then(|c| c.ddl.build.as_ref()) {
            build.cancel.store(true, Ordering::Release);
        }
        self.shutdown_secondary_index_worker();
        if let Some(candidate) = &mut self.candidate {
            candidate.ddl.build.take();
        }
    }
}

fn apply_candidate_index_batches(
    maps: &mut BTreeMap<u64, SecondaryMaps>,
    batches: VecDeque<CandidateIndexBatch>,
    participants: &participant::ParticipantHandle,
    target: &SharedArc<FixedCatalog>,
    domain: &reclamation::ReclaimDomain,
) -> Result<usize, EngineError> {
    let mut applied = 0usize;
    for batch in batches {
        if let std::collections::btree_map::Entry::Vacant(e) = maps.entry(batch.generation) {
            e.insert(SecondaryMaps::new(target.clone(), domain)?);
        }
        let map = &maps[&batch.generation];
        let session = participants
            .try_acquire()
            .map_err(crate::memtable::primary::participant_error)?;
        for (seq, transition) in batch.transitions {
            map.apply_private(&session, seq, transition)?;
        }
        applied = applied
            .checked_add(batch.bytes)
            .expect("bounded prefix bytes");
    }
    Ok(applied)
}
fn build_candidate_index(work: CandidateIndexWork) -> Result<CandidateIndexBuilt, EngineError> {
    #[cfg(test)]
    if let Some(pause) = work.pause {
        let _ = pause.ready_tx.send(());
        let _ = pause.release_rx.recv();
    }
    #[cfg(test)]
    if let Some(boundary) = work.component_fault {
        crate::persistence_fault::set(boundary, work.component_abort);
    }
    #[cfg(test)]
    let mut target_publish_pause = work.target_publish_pause;
    let mut maps = BTreeMap::new();
    let participants = work.generations[0].1.participant_handle().unwrap();
    for (id, memtable, max_node, max_edge) in &work.generations {
        let map = SecondaryMaps::new(work.target.clone(), &work.domain)?;
        let mut last = None;
        if matches!(
            work.entry.target,
            SecondaryIndexTarget::NodeProperty { .. } | SecondaryIndexTarget::NodeFieldIndex { .. }
        ) {
            loop {
                if work.cancel.load(Ordering::Acquire) {
                    return Err(EngineError::DatabaseClosed);
                }
                let chunk = memtable.export_node_chunk(work.frontier, (last, *max_node))?;
                let session = participants
                    .try_acquire()
                    .map_err(crate::memtable::primary::participant_error)?;
                for node in chunk.records.iter() {
                    map.apply_private(
                        &session,
                        node.last_write_seq,
                        SecondaryTransition::base_node(&work.target, node),
                    )?;
                }
                last = chunk.last;
                if chunk.exhausted {
                    break;
                }
            }
        } else {
            loop {
                if work.cancel.load(Ordering::Acquire) {
                    return Err(EngineError::DatabaseClosed);
                }
                let chunk = memtable.export_edge_chunk(work.frontier, (last, *max_edge))?;
                let session = participants
                    .try_acquire()
                    .map_err(crate::memtable::primary::participant_error)?;
                for edge in chunk.records.iter() {
                    map.apply_private(
                        &session,
                        edge.last_write_seq,
                        SecondaryTransition::base_edge(&work.target, edge),
                    )?;
                }
                last = chunk.last;
                if chunk.exhausted {
                    break;
                }
            }
        }
        maps.insert(*id, map);
    }
    let mut segments = Vec::new();
    let mut obsolete = Vec::new();
    let mut entries = work.entries.clone();
    for e in &mut entries {
        if e.index_id == work.entry.index_id {
            e.state = SecondaryIndexState::Ready;
        }
    }
    for segment in &work.segments {
        if work.cancel.load(Ordering::Acquire) {
            return Err(EngineError::DatabaseClosed);
        }
        let path = segment_dir(&work.db_dir, segment.segment_id);
        let entry = &work.entry;
        let prepared = match (&entry.target, &entry.kind) {
            (
                SecondaryIndexTarget::NodeProperty { label_id, prop_key },
                SecondaryIndexKind::Equality,
            ) => crate::segment_writer::prepare_node_prop_eq_sidecar_component(
                &path,
                entry,
                &build_secondary_eq_groups_for_segment(segment, *label_id, prop_key)?,
            )?,
            (
                SecondaryIndexTarget::NodeProperty { label_id, prop_key },
                SecondaryIndexKind::Range,
            ) => crate::segment_writer::prepare_node_prop_range_sidecar_component(
                &path,
                entry,
                &build_secondary_range_entries_for_segment(segment, *label_id, prop_key)?,
            )?,
            (
                SecondaryIndexTarget::EdgeProperty { label_id, prop_key },
                SecondaryIndexKind::Equality,
            ) => crate::segment_writer::prepare_edge_prop_eq_sidecar_component(
                &path,
                entry,
                &build_edge_secondary_eq_groups_for_segment(segment, *label_id, prop_key)?,
            )?,
            (
                SecondaryIndexTarget::EdgeProperty { label_id, prop_key },
                SecondaryIndexKind::Range,
            ) => crate::segment_writer::prepare_edge_prop_range_sidecar_component(
                &path,
                entry,
                &build_edge_secondary_range_entries_for_segment(segment, *label_id, prop_key)?,
            )?,
            (SecondaryIndexTarget::NodeFieldIndex { label_id, .. }, _) => {
                let meta = compound_node_metas_for_single_segment(segment, *label_id)?;
                let values = build_node_compound_entries_from_metadata(
                    std::slice::from_ref(segment),
                    &meta,
                    entry,
                )?;
                crate::segment_writer::prepare_compound_sidecar_component(&path, entry, &values)?
            }
            (SecondaryIndexTarget::EdgeFieldIndex { label_id, .. }, _) => {
                let meta = compound_edge_metas_for_single_segment(segment, *label_id)?;
                let values = build_edge_compound_entries_from_metadata(
                    std::slice::from_ref(segment),
                    &meta,
                    entry,
                )?;
                crate::segment_writer::prepare_compound_sidecar_component(&path, entry, &values)?
            }
        };
        obsolete.extend(prepared.obsolete_paths());
        {
            let gate = work.authority.gate()?;
            if let Err(error) = prepared.publish() {
                gate.fence();
                return Err(EngineError::CommitOutcome {
                    state: CommitOutcomeState::Indeterminate,
                    message: format!("index component publication uncertain: {error}"),
                });
            }
        }
        #[cfg(test)]
        if let Some(pause) = target_publish_pause.take() {
            let _ = pause.ready_tx.send(());
            let _ = pause.release_rx.recv();
        }
        let info = SegmentInfo {
            id: segment.segment_id,
            node_count: segment.node_count(),
            edge_count: segment.edge_count(),
            segment_format_version: crate::segment_writer::SEGMENT_FORMAT_VERSION,
            segment_data_id: segment.segment_data_id(),
        };
        let mut reader =
            SegmentReader::open_with_info(&path, &info, work.dense_config.as_ref(), &entries)?;
        reader.retain_secondary_payloads(Some(segment));
        for entry in &entries {
            reader.warm_declared_index_runtime_coverage(entry);
        }
        let ready_entry = entries
            .iter()
            .find(|e| e.index_id == work.entry.index_id)
            .expect("captured target");
        // Warming caches records missing/corrupt optional coverage without
        // returning an error. Ready requires the actual retained target bytes,
        // independently of whether advisory statistics can be prepared.
        let valid_target = match &ready_entry.target {
            SecondaryIndexTarget::NodeFieldIndex { .. }
            | SecondaryIndexTarget::EdgeFieldIndex { .. } => {
                reader.validate_compound_sidecar_for_entry(ready_entry)?
            }
            target => {
                let target = match target {
                    SecondaryIndexTarget::NodeProperty { .. } => {
                        PlannerStatsDeclaredIndexTarget::NodeProperty
                    }
                    SecondaryIndexTarget::EdgeProperty { .. } => {
                        PlannerStatsDeclaredIndexTarget::EdgeProperty
                    }
                    _ => unreachable!("compound target validated above"),
                };
                match ready_entry.kind {
                    SecondaryIndexKind::Equality => reader
                        .validate_secondary_eq_sidecar_for_target(ready_entry.index_id, target)?,
                    SecondaryIndexKind::Range => reader
                        .validate_secondary_range_sidecar_for_target(
                            ready_entry.index_id,
                            target,
                        )?,
                }
            }
        };
        if !valid_target {
            return Err(EngineError::CorruptRecord(format!(
                "published index {} target coverage unavailable in segment {}",
                ready_entry.index_id, segment.segment_id
            )));
        }
        // Target coverage is valid and retained. Preparation is advisory, as in
        // the existing refresh path; uncertain statistics publication still fences.
        if let Ok((_, Some(stats))) =
            crate::planner_stats::prepare_targeted_secondary_index_planner_stats_sidecar(
                &path,
                &reader,
                ready_entry,
                &entries,
            )
        {
            obsolete.extend(stats.obsolete_paths());
            {
                let gate = work.authority.gate()?;
                if let Err(error) = stats.publish() {
                    gate.fence();
                    return Err(EngineError::CommitOutcome {
                        state: CommitOutcomeState::Indeterminate,
                        message: format!("index statistics publication uncertain: {error}"),
                    });
                }
            }
            let refreshed =
                SegmentReader::open_with_info(&path, &info, work.dense_config.as_ref(), &entries)?;
            refreshed.retain_secondary_payloads(Some(&reader));
            for entry in &entries {
                refreshed.warm_declared_index_runtime_coverage(entry);
            }
            reader = refreshed;
        }
        let mut reader = SharedArc::new(reader);
        reader.guard_storage(&work.domain, reader.segment_id)?;
        reader.pin_components(&work.domain, reader.component_paths())?;
        segments.push(reader);
    }
    // A bounded number of transfers avoids an endless worker catch-up loop when
    // writers continually arrive. Remaining finite tail is adopted serially.
    for _ in 0..4 {
        if work.cancel.load(Ordering::Acquire) {
            return Err(EngineError::DatabaseClosed);
        }
        let (prefix, frontier) = work.suffix.lock().unwrap().take_prefix();
        if prefix.is_empty() {
            work.suffix.lock().unwrap().applied_frontier = frontier;
            break;
        }
        // Release transfer lock for map allocation/application, but retain charge.
        let applied = apply_candidate_index_batches(
            &mut maps,
            prefix,
            &participants,
            &work.target,
            &work.domain,
        )?;
        let mut suffix = work.suffix.lock().unwrap();
        suffix.bytes = suffix
            .bytes
            .checked_sub(applied)
            .expect("worker prefix charge");
        suffix.applied_frontier = frontier;
    }
    Ok(CandidateIndexBuilt {
        maps,
        segments,
        obsolete,
    })
}

#[cfg(test)]
mod p45c2_tests {
    use super::*;
    use std::time::Duration;
    fn options() -> DbOptions {
        DbOptions {
            memtable_flush_threshold: 0,
            compact_after_n_flushes: 0,
            ..DbOptions::default()
        }
    }
    fn rows(start: usize, count: usize, tenant: &str, score: i64) -> Vec<NodeInput> {
        (start..start + count)
            .map(|i| NodeInput {
                labels: vec!["C2".into()],
                key: format!("n{i}"),
                props: BTreeMap::from([
                    ("tenant".into(), PropValue::String(tenant.into())),
                    ("score".into(), PropValue::Int(score)),
                ]),
                weight: 1.0,
                dense_vector: None,
                sparse_vector: None,
            })
            .collect()
    }
    fn spec() -> SecondaryIndexSpec {
        SecondaryIndexSpec::range(vec![
            SecondaryIndexField::property("tenant"),
            SecondaryIndexField::property("score"),
        ])
    }
    fn query(tenant: &str, score: i64) -> NodeQuery {
        NodeQuery {
            label_filter: Some(NodeLabelFilter {
                labels: vec!["C2".into()],
                mode: LabelMatchMode::All,
            }),
            filter: Some(NodeFilterExpr::And(vec![
                NodeFilterExpr::PropertyEquals {
                    key: "tenant".into(),
                    value: PropValue::String(tenant.into()),
                },
                NodeFilterExpr::PropertyRange {
                    key: "score".into(),
                    lower: Some(PropertyRangeBound::Included(PropValue::Int(score))),
                    upper: None,
                },
            ])),
            ..Default::default()
        }
    }
    fn ready(db: &DatabaseEngine, id: u64) {
        let until = Instant::now() + Duration::from_secs(15);
        loop {
            let state = db
                .list_node_property_indexes()
                .unwrap()
                .into_iter()
                .find(|e| e.index_id == id)
                .unwrap();
            if state.state == SecondaryIndexState::Ready {
                break;
            }
            assert_ne!(state.state, SecondaryIndexState::Failed, "{:?}", state);
            assert!(Instant::now() < until, "build did not become Ready");
            std::thread::sleep(Duration::from_millis(2));
        }
    }
    #[test]
    fn p45c2_review_cleanup_preserves_files_on_reference_read_failure() {
        let dir = tempfile::tempdir().unwrap();
        let db = DatabaseEngine::open(dir.path(), &options()).unwrap();
        db.batch_upsert_nodes(rows(0, 2, "old", 10)).unwrap();
        db.flush().unwrap();
        let index = db.ensure_node_property_index("C2", spec()).unwrap();
        ready(&db, index.index_id);
        let mut manifest = db.manifest().unwrap();
        let entry = manifest.secondary_indexes[0].clone();
        let segment = segment_dir(dir.path(), manifest.segments[0].id);
        db.close_fast().unwrap();
        let until = Instant::now() + Duration::from_secs(10);
        while !matches!(
            db.reclamation_stats().unwrap().close_state,
            ReclamationCloseState::Drained
        ) {
            assert!(Instant::now() < until);
            std::thread::sleep(Duration::from_millis(1));
        }
        drop(db);
        manifest.secondary_indexes.clear();
        crate::manifest::write_manifest(dir.path(), &manifest).unwrap();
        let paths = remove_secondary_index_component_records(&segment, &entry).unwrap();
        assert!(!paths.is_empty());
        let previous = dir.path().join(crate::manifest::MANIFEST_PREV);
        let previous_bytes = std::fs::read(&previous).unwrap();
        std::fs::remove_file(&previous).unwrap();
        std::fs::create_dir(&previous).unwrap();
        assert!(crate::manifest::recovery_file_references(dir.path()).is_err());
        assert!(std::fs::read(dir.path().join(crate::manifest::MANIFEST_CURRENT)).is_ok());
        cleanup_orphan_optional_refresh_files(dir.path(), &manifest, None);
        for path in &paths {
            assert!(
                path.exists(),
                "reference-read error deleted {}",
                path.display()
            );
        }
        std::fs::remove_dir(&previous).unwrap();
        std::fs::write(&previous, previous_bytes).unwrap();
        cleanup_orphan_optional_refresh_files(dir.path(), &manifest, None);
        assert!(
            paths.iter().all(|p| p.exists()),
            "readable fallback lost protection"
        );
        std::fs::remove_file(previous).unwrap();
        cleanup_orphan_optional_refresh_files(dir.path(), &manifest, None);
        assert!(
            paths.iter().all(|p| !p.exists()),
            "eligible files did not retire"
        );
    }

    #[test]
    fn p45c2_review_candidate_adopts_target_when_statistics_preparation_fails() {
        for spec in [
            SecondaryIndexSpec::equality(vec![SecondaryIndexField::property("tenant")]),
            spec(),
        ] {
            let dir = tempfile::tempdir().unwrap();
            let db = DatabaseEngine::open(dir.path(), &options()).unwrap();
            db.batch_upsert_nodes(rows(0, 2, "old", 10)).unwrap();
            db.flush().unwrap();
            let generation = db.segments_for_test()[0].component_manifest_generation();
            db.batch_upsert_nodes(rows(2, 2, "old", 10)).unwrap();
            let (seen, release) = db.set_secondary_index_build_pause();
            *db.runtime
                .core
                .lock()
                .unwrap()
                .as_ref()
                .unwrap()
                .candidate
                .as_ref()
                .unwrap()
                .ddl
                .component_fault
                .lock()
                .unwrap() = Some(crate::persistence_fault::Boundary::PlannerStatsPrepare);
            let index = db.ensure_node_property_index("C2", spec).unwrap();
            seen.recv_timeout(Duration::from_secs(5)).unwrap();
            let building = db.runtime.published.load_full().view.clone();
            db.batch_upsert_nodes(rows(4, 2, "old", 10)).unwrap();
            release.send(()).unwrap();
            ready(&db, index.index_id);
            assert_eq!(
                db.query_node_ids(&query("old", 10)).unwrap().items,
                vec![1, 2, 3, 4, 5, 6]
            );
            assert_eq!(
                building
                    .query_node_ids_outcome(&query("old", 10))
                    .unwrap()
                    .value
                    .items,
                vec![1, 2, 3, 4]
            );
            let reader = db.segments_for_test()[0].clone();
            assert_eq!(
                reader.component_manifest_generation(),
                generation + 1,
                "advisory preparation unexpectedly published statistics"
            );
            assert!(
                format!("{:?}", db.explain_node_query(&query("old", 10)).unwrap())
                    .contains("Index")
            );
            assert!(!db
                .runtime
                .core
                .lock()
                .unwrap()
                .as_ref()
                .unwrap()
                .manifest_write_lock
                .fenced());
            drop(building);
            drop(reader);
            db.close_fast().unwrap();
        }
    }

    #[test]
    fn p45c2_review_missing_or_corrupt_published_target_never_becomes_ready() {
        for compound in [false, true] {
            for corrupt in [false, true] {
                let dir = tempfile::tempdir().unwrap();
                let db = DatabaseEngine::open(dir.path(), &options()).unwrap();
                db.batch_upsert_nodes(rows(0, 2, "old", 10)).unwrap();
                db.flush().unwrap();
                db.batch_upsert_nodes(rows(2, 2, "old", 10)).unwrap();
                let (seen_tx, seen) = std::sync::mpsc::sync_channel(1);
                let (release, release_rx) = std::sync::mpsc::sync_channel(1);
                *db.runtime
                    .core
                    .lock()
                    .unwrap()
                    .as_ref()
                    .unwrap()
                    .candidate
                    .as_ref()
                    .unwrap()
                    .ddl
                    .target_publish_pause
                    .lock()
                    .unwrap() = Some(SecondaryIndexBuildPauseHook {
                    ready_tx: seen_tx,
                    release_rx,
                });
                let target = if compound {
                    spec()
                } else {
                    SecondaryIndexSpec::equality(vec![SecondaryIndexField::property("tenant")])
                };
                let index = db.ensure_node_property_index("C2", target.clone()).unwrap();
                seen.recv_timeout(Duration::from_secs(5)).unwrap();
                let building = db.runtime.published.load_full().view.clone();
                let segment = segment_dir(dir.path(), 1);
                let path = if compound {
                    crate::segment_writer::node_compound_range_sidecar_path(
                        &segment,
                        index.index_id,
                    )
                } else {
                    crate::segment_writer::node_prop_eq_sidecar_path(&segment, index.index_id)
                };
                let mutation = if corrupt {
                    std::fs::write(&path, b"corrupt target")
                } else {
                    std::fs::remove_file(&path)
                };
                db.batch_upsert_nodes(rows(4, 2, "old", 10)).unwrap();
                release.send(()).unwrap();
                mutation.unwrap();
                let until = Instant::now() + Duration::from_secs(10);
                loop {
                    let state = db
                        .list_node_property_indexes()
                        .unwrap()
                        .into_iter()
                        .find(|e| e.index_id == index.index_id)
                        .unwrap();
                    assert_ne!(state.state, SecondaryIndexState::Ready, "invalid retained target became Ready: compound={compound}/corrupt={corrupt}");
                    if state.state == SecondaryIndexState::Failed {
                        assert!(state.last_error.is_some());
                        break;
                    }
                    assert!(Instant::now() < until);
                    std::thread::sleep(Duration::from_millis(2));
                }
                assert_eq!(
                    db.manifest().unwrap().secondary_indexes[0].state,
                    SecondaryIndexState::Failed
                );
                assert_eq!(
                    db.query_node_ids(&query("old", 10)).unwrap().items,
                    vec![1, 2, 3, 4, 5, 6]
                );
                assert_eq!(
                    building
                        .query_node_ids_outcome(&query("old", 10))
                        .unwrap()
                        .value
                        .items,
                    vec![1, 2, 3, 4]
                );
                assert!(!db
                    .runtime
                    .core
                    .lock()
                    .unwrap()
                    .as_ref()
                    .unwrap()
                    .manifest_write_lock
                    .fenced());
                let retry = db.ensure_node_property_index("C2", target).unwrap();
                assert_eq!(retry.index_id, index.index_id);
                ready(&db, retry.index_id);
                assert_eq!(
                    db.query_node_ids(&query("old", 10)).unwrap().items,
                    vec![1, 2, 3, 4, 5, 6]
                );
                drop(building);
                db.close_fast().unwrap();
            }
        }
    }

    #[test]
    fn p45c2_review_uncertain_statistics_publication_still_fences() {
        let dir = tempfile::tempdir().unwrap();
        let db = DatabaseEngine::open(dir.path(), &options()).unwrap();
        db.batch_upsert_nodes(rows(0, 2, "old", 10)).unwrap();
        db.flush().unwrap();
        let (seen_tx, seen) = std::sync::mpsc::sync_channel(1);
        let (release, release_rx) = std::sync::mpsc::sync_channel(1);
        *db.runtime
            .core
            .lock()
            .unwrap()
            .as_ref()
            .unwrap()
            .candidate
            .as_ref()
            .unwrap()
            .ddl
            .target_publish_pause
            .lock()
            .unwrap() = Some(SecondaryIndexBuildPauseHook {
            ready_tx: seen_tx,
            release_rx,
        });
        let index = db.ensure_node_property_index("C2", spec()).unwrap();
        seen.recv_timeout(Duration::from_secs(5)).unwrap();
        let segment = segment_dir(dir.path(), 1);
        let blocked =
            segment.join(crate::segment_components::SEGMENT_COMPONENT_MANIFEST_TMP_FILENAME);
        let mutation = std::fs::create_dir(&blocked);
        release.send(()).unwrap();
        mutation.unwrap();
        wait_fence(&db);
        let target =
            crate::segment_writer::node_compound_range_sidecar_path(&segment, index.index_id);
        assert!(target.exists());
        let authority = root_bytes(dir.path());
        assert!(db.close_fast().is_err());
        drop(db);
        assert_eq!(root_bytes(dir.path()), authority);
        assert!(target.exists());
        std::fs::remove_dir(blocked).unwrap();
        let db = tests::open_after_native_shutdown(dir.path(), &options()).unwrap();
        ready(&db, index.index_id);
        assert_eq!(
            db.query_node_ids(&query("old", 10)).unwrap().items,
            vec![1, 2]
        );
        db.close_fast().unwrap();
    }

    #[test]
    fn p45c2_settled_build_sweeps_unreferenced_optional_generations() {
        let dir = tempfile::TempDir::new().unwrap();
        let options = DbOptions {
            compact_after_n_flushes: 0,
            memtable_flush_threshold: 0,
            ..DbOptions::default()
        };
        let db = DatabaseEngine::open(dir.path(), &options).unwrap();
        db.upsert_node(
            "R",
            "orphan",
            UpsertNodeOptions {
                props: BTreeMap::from([("value".into(), PropValue::Int(7))]),
                ..Default::default()
            },
        )
        .unwrap();
        db.flush().unwrap();
        let segment = segment_dir(dir.path(), db.segments_for_test()[0].segment_id);
        let orphan = segment.join("orphan.g18446744073709551615.dat");
        std::fs::write(&orphan, b"unreferenced private output").unwrap();
        let index = db
            .ensure_node_property_index(
                "R",
                SecondaryIndexSpec::equality(vec![SecondaryIndexField::property("value")]),
            )
            .unwrap();
        ready_all(&db);
        assert_eq!(
            db.list_node_property_indexes().unwrap()[0].index_id,
            index.index_id
        );
        let until = Instant::now() + Duration::from_secs(5);
        while orphan.exists() {
            assert!(
                Instant::now() < until,
                "eligible abandoned output remained after build settlement"
            );
            std::thread::sleep(Duration::from_millis(1));
        }
        assert_eq!(
            db.find_nodes("R", "value", &PropValue::Int(7))
                .unwrap()
                .len(),
            1
        );
        db.close().unwrap();
    }
    #[test]
    fn p45c2_online_compound_base_suffix_rotation_and_old_view() {
        let dir = tempfile::tempdir().unwrap();
        let options = options();
        let limits = ConcurrencyOptions {
            max_batch_operations: 40,
            generation_operation_limit: 40,
            ..ConcurrencyOptions::default()
        };
        let db =
            DatabaseEngine::open_with_concurrency_options(dir.path(), &options, &limits).unwrap();
        let disk = db.batch_upsert_nodes(rows(0, 10, "old", 10)).unwrap();
        db.flush().unwrap();
        let active = db.batch_upsert_nodes(rows(10, 10, "old", 10)).unwrap();
        let old = db.runtime.published.load_full().view.clone();
        let (seen_tx, seen_rx) = std::sync::mpsc::sync_channel(1);
        let (go_tx, go_rx) = std::sync::mpsc::channel();
        *db.runtime
            .core
            .lock()
            .unwrap()
            .as_ref()
            .unwrap()
            .secondary_index_build_pause
            .lock()
            .unwrap() = Some(SecondaryIndexBuildPauseHook {
            ready_tx: seen_tx,
            release_rx: go_rx,
        });
        let index = db.ensure_node_property_index("C2", spec()).unwrap();
        seen_rx.recv_timeout(Duration::from_secs(5)).unwrap();
        for score in 11..46 {
            db.batch_upsert_nodes(rows(10, 1, "new", score)).unwrap();
        }
        assert_eq!(
            db.query_node_ids(&query("new", 45)).unwrap().items,
            vec![active[0]]
        );
        assert_eq!(
            old.query_node_ids_outcome(&query("old", 10))
                .unwrap()
                .value
                .items,
            [disk.clone(), active.clone()].concat()
        );
        go_tx.send(()).unwrap();
        ready(&db, index.index_id);
        assert_eq!(
            db.query_node_ids(&query("new", 45)).unwrap().items,
            vec![active[0]]
        );
        let plan = db.explain_node_query(&query("new", 45)).unwrap();
        assert!(format!("{:?}", plan).contains("Compound"), "{:?}", plan);
        assert_eq!(
            old.query_node_ids_outcome(&query("old", 10))
                .unwrap()
                .value
                .items,
            [disk, active].concat()
        );
        db.close_fast().unwrap();
    }
    #[test]
    fn p45c2_suffix_logical_boundary_keeps_worker_owned_charge() {
        let mut suffix = CandidateIndexSuffix {
            accepted_frontier: 3,
            applied_frontier: 3,
            queued: VecDeque::new(),
            bytes: 0,
            peak: 0,
            limit: 40,
        };
        let batch = CandidateIndexBatch {
            generation: 1,
            frontier: 5,
            transitions: Vec::new(),
            bytes: 40,
        };
        suffix.admit(40).unwrap();
        suffix.append(4, batch);
        let (owned, frontier) = suffix.take_prefix();
        assert_eq!(frontier, 5);
        assert_eq!(owned.len(), 1);
        assert!(suffix.admit(1).is_err());
        suffix.bytes -= 40;
        suffix.applied_frontier = frontier;
        assert!(suffix.admit(40).is_ok());
        assert!(suffix.admit(41).is_err());
    }
    fn family_specs() -> Vec<SecondaryIndexSpec> {
        [
            (SecondaryIndexKind::Equality, vec!["eq"]),
            (SecondaryIndexKind::Range, vec!["range"]),
            (SecondaryIndexKind::Equality, vec!["ceq", "tie"]),
            (SecondaryIndexKind::Range, vec!["crange", "score"]),
        ]
        .into_iter()
        .map(|(kind, keys)| SecondaryIndexSpec {
            kind,
            fields: keys
                .into_iter()
                .map(SecondaryIndexField::property)
                .collect(),
        })
        .collect()
    }
    fn ready_all(db: &DatabaseEngine) {
        let until = Instant::now() + Duration::from_secs(15);
        loop {
            let nodes = db.list_node_property_indexes().unwrap();
            let edges = db.list_edge_property_indexes().unwrap();
            assert!(
                nodes.iter().all(|e| e.state != SecondaryIndexState::Failed),
                "{:?}",
                nodes
            );
            assert!(
                edges.iter().all(|e| e.state != SecondaryIndexState::Failed),
                "{:?}",
                edges
            );
            if nodes.iter().all(|e| e.state == SecondaryIndexState::Ready)
                && edges.iter().all(|e| e.state == SecondaryIndexState::Ready)
            {
                break;
            }
            assert!(Instant::now() < until);
            std::thread::sleep(Duration::from_millis(2));
        }
    }
    #[test]
    fn p45c2_all_eight_cold_old_ready_bindings_survive_drop_recreate_reopen() {
        let dir = tempfile::tempdir().unwrap();
        let opts = options();
        let db = DatabaseEngine::open(dir.path(), &opts).unwrap();
        let nodes: Vec<_> = (0..32)
            .map(|i| NodeInput {
                labels: vec!["R".into()],
                key: format!("r{i}"),
                props: real_route_tests::fixed_family_props(i % 16 < 4),
                weight: 1.0,
                dense_vector: None,
                sparse_vector: None,
            })
            .collect();
        let ids = db.batch_upsert_nodes(nodes[..16].to_vec()).unwrap();
        let edges = |ids: &[u64], start: usize| {
            ids.iter()
                .enumerate()
                .map(|(i, &id)| EdgeInput {
                    from: id,
                    to: id,
                    label: "E".into(),
                    props: real_route_tests::fixed_family_props((i + start) % 16 < 4),
                    weight: 1.0,
                    valid_from: None,
                    valid_to: None,
                })
                .collect()
        };
        let disk_edges = db.batch_upsert_edges(edges(&ids, 0)).unwrap();
        db.flush().unwrap();
        let active = db.batch_upsert_nodes(nodes[16..].to_vec()).unwrap();
        let active_edges = db.batch_upsert_edges(edges(&active, 16)).unwrap();
        let mut original_ids = Vec::new();
        for spec in family_specs() {
            original_ids.push(
                db.ensure_node_property_index("R", spec.clone())
                    .unwrap()
                    .index_id,
            );
            ready_all(&db);
            original_ids.push(db.ensure_edge_property_index("E", spec).unwrap().index_id);
            ready_all(&db);
        }
        // No public sidecar query before taking this view. Compound reads have no
        // query cache to substitute for the retained payload mappings.
        let old = db.runtime.published.load_full().view.clone();
        let paths: Vec<_> = old
            .sources
            .segments
            .iter()
            .flat_map(|s| s.component_paths())
            .filter(|(_, _, p)| p.to_string_lossy().contains("secondary_indexes/"))
            .map(|(id, _, p)| segment_dir(dir.path(), id).join(p))
            .collect();
        assert_eq!(paths.len(), 8);
        for p in &paths {
            assert!(p.exists());
        }
        for spec in family_specs() {
            assert!(db.drop_node_property_index("R", spec.clone()).unwrap());
            let id = db
                .ensure_node_property_index("R", spec.clone())
                .unwrap()
                .index_id;
            assert!(!original_ids.contains(&id));
            ready_all(&db);
            assert!(db.drop_edge_property_index("E", spec.clone()).unwrap());
            let id = db.ensure_edge_property_index("E", spec).unwrap().index_id;
            assert!(!original_ids.contains(&id));
            ready_all(&db);
        }
        db.close_fast().unwrap();
        drop(db);
        let db = tests::open_after_native_shutdown(dir.path(), &opts).unwrap();
        for p in &paths {
            assert!(
                p.exists(),
                "cold mapping lost before first old-view query: {}",
                p.display()
            );
        }
        let expected_nodes = [ids[..4].to_vec(), active[..4].to_vec()].concat();
        let expected_edges = [disk_edges[..4].to_vec(), active_edges[..4].to_vec()].concat();
        for family in 0..4 {
            let (nq, eq) = real_route_tests::fixed_family_queries(family);
            assert_eq!(
                old.query_node_ids_outcome(&nq).unwrap().value.items,
                expected_nodes
            );
            assert_eq!(
                old.query_edge_ids_outcome(&eq).unwrap().value.edge_ids,
                expected_edges
            );
            assert_eq!(db.query_node_ids(&nq).unwrap().items, expected_nodes);
            assert_eq!(db.query_edge_ids(&eq).unwrap().edge_ids, expected_edges);
            let plan = format!("{:?}", old.explain_node_query(&nq).unwrap());
            assert!(plan.contains("Index"), "{plan}");
        }
        drop(old);
        db.set_prune_policy(
            "advance",
            PrunePolicy {
                max_age_ms: None,
                max_weight: Some(0.0),
                label: Some("unused".into()),
            },
        )
        .unwrap();
        db.remove_prune_policy("advance").unwrap();
        let until = Instant::now() + Duration::from_secs(10);
        while paths.iter().any(|p| p.exists()) {
            assert!(
                Instant::now() < until,
                "obsolete cold components did not retire: {:?}",
                paths.iter().filter(|p| p.exists()).collect::<Vec<_>>()
            );
            std::thread::sleep(Duration::from_millis(5));
        }
        db.close_fast().unwrap();
    }
    #[test]
    fn p45c2_real_suffix_pressure_eight_writers_and_lifecycle_progress() {
        let dir = tempfile::tempdir().unwrap();
        let opts = options();
        let limits = ConcurrencyOptions {
            admission_timeout_ms: 20_000,
            ..Default::default()
        };
        let db = DatabaseEngine::open_with_concurrency_options(dir.path(), &opts, &limits).unwrap();
        db.batch_upsert_nodes(rows(0, 14000, "seed", 10)).unwrap();
        let (seen_tx, seen_rx) = std::sync::mpsc::sync_channel(1);
        let (go_tx, go_rx) = std::sync::mpsc::sync_channel(1);
        *db.runtime
            .core
            .lock()
            .unwrap()
            .as_ref()
            .unwrap()
            .secondary_index_build_pause
            .lock()
            .unwrap() = Some(SecondaryIndexBuildPauseHook {
            ready_tx: seen_tx,
            release_rx: go_rx,
        });
        let ix = db.ensure_node_property_index("C2", spec()).unwrap();
        seen_rx.recv_timeout(Duration::from_secs(5)).unwrap();
        let value = "b".repeat(960);
        db.batch_upsert_nodes(rows(0, 14000, &value, 10)).unwrap();
        let before = db.runtime.core.lock().unwrap().as_ref().unwrap().engine_seq;
        let (peak, limit) = {
            let core = db.runtime.core.lock().unwrap();
            let suffix = core
                .as_ref()
                .unwrap()
                .candidate
                .as_ref()
                .unwrap()
                .ddl
                .build
                .as_ref()
                .unwrap()
                .suffix
                .lock()
                .unwrap();
            (suffix.peak, suffix.limit)
        };
        assert_eq!(limit, 16 * 1024 * 1024);
        assert!(
            peak > 12 * 1024 * 1024 && peak <= limit,
            "peak {peak}, limit {limit}"
        );
        let mut writers = Vec::new();
        for i in 0..8 {
            let db = db.clone();
            writers.push(std::thread::spawn(move || {
                db.batch_upsert_nodes(rows(0, 1000, &format!("{i}{}", "c".repeat(960)), 11))
            }));
        }
        let until = Instant::now() + Duration::from_secs(5);
        while db.runtime.candidate.as_ref().unwrap().pressure.occupied() != 8 {
            assert!(Instant::now() < until);
            std::thread::yield_now();
        }
        assert_eq!(
            db.runtime.core.lock().unwrap().as_ref().unwrap().engine_seq,
            before,
            "suffix pressure must precede WAL acceptance"
        );
        go_tx.send(()).unwrap();
        for w in writers {
            assert_eq!(w.join().unwrap().unwrap(), (1..1001).collect::<Vec<_>>());
        }
        ready(&db, ix.index_id);
        let core = db.runtime.core.lock().unwrap();
        assert!(core
            .as_ref()
            .unwrap()
            .candidate
            .as_ref()
            .unwrap()
            .ddl
            .build
            .is_none());
        drop(core);
        assert_eq!(
            db.get_node(1).unwrap().unwrap().props.get("score"),
            Some(&PropValue::Int(11))
        );
        db.close_fast().unwrap();
    }
    #[test]
    fn p45c2_suffix_oversized_request_waits_with_original_deadline_before_wal() {
        let dir = tempfile::tempdir().unwrap();
        let opts = options();
        let limits = ConcurrencyOptions {
            admission_timeout_ms: 80,
            ..Default::default()
        };
        let db = DatabaseEngine::open_with_concurrency_options(dir.path(), &opts, &limits).unwrap();
        db.batch_upsert_nodes(rows(0, 1, "old", 10)).unwrap();
        let (seen_tx, seen_rx) = std::sync::mpsc::sync_channel(1);
        let (go_tx, go_rx) = std::sync::mpsc::sync_channel(1);
        *db.runtime
            .core
            .lock()
            .unwrap()
            .as_ref()
            .unwrap()
            .secondary_index_build_pause
            .lock()
            .unwrap() = Some(SecondaryIndexBuildPauseHook {
            ready_tx: seen_tx,
            release_rx: go_rx,
        });
        let ix = db.ensure_node_property_index("C2", spec()).unwrap();
        seen_rx.recv_timeout(Duration::from_secs(5)).unwrap();
        let before = {
            let core = db.runtime.core.lock().unwrap();
            let core = core.as_ref().unwrap();
            core.candidate
                .as_ref()
                .unwrap()
                .ddl
                .build
                .as_ref()
                .unwrap()
                .suffix
                .lock()
                .unwrap()
                .limit = 32;
            core.engine_seq
        };
        let result = db.batch_upsert_nodes(rows(0, 1, "new", 11));
        assert!(matches!(
            result,
            Err(EngineError::ResourceLimit {
                retryable: true,
                ..
            })
        ));
        assert_eq!(
            db.runtime.core.lock().unwrap().as_ref().unwrap().engine_seq,
            before
        );
        assert_eq!(
            db.get_node(1).unwrap().unwrap().props.get("tenant"),
            Some(&PropValue::String("old".into()))
        );
        go_tx.send(()).unwrap();
        ready(&db, ix.index_id);
        db.batch_upsert_nodes(rows(0, 1, "new", 11)).unwrap();
        db.close_fast().unwrap();
    }

    fn root_bytes(path: &Path) -> [Option<Vec<u8>>; 3] {
        [
            crate::manifest::MANIFEST_CURRENT,
            crate::manifest::MANIFEST_TMP,
            crate::manifest::MANIFEST_PREV,
        ]
        .map(|n| std::fs::read(path.join(n)).ok())
    }
    fn wait_fence(db: &DatabaseEngine) {
        let until = Instant::now() + Duration::from_secs(10);
        while !db
            .runtime
            .core
            .lock()
            .unwrap()
            .as_ref()
            .unwrap()
            .manifest_write_lock
            .fenced()
        {
            assert!(Instant::now() < until, "publisher did not fence");
            std::thread::sleep(Duration::from_millis(2));
        }
    }
    #[test]
    fn p45c2_create_manifest_faults_keep_token_declaration_atomic_all_closes() {
        use crate::persistence_fault::Boundary::*;
        for boundary in [
            BeforeTmp,
            TmpCreate,
            TmpWrite,
            TmpSync,
            PrevRemove,
            CurrentToPrev,
            TmpToCurrent,
            DirSync,
        ] {
            for mode in 0..3 {
                let dir = tempfile::tempdir().unwrap();
                let opts = options();
                let db = DatabaseEngine::open(dir.path(), &opts).unwrap();
                db.batch_upsert_nodes(rows(0, 1, "old", 10)).unwrap();
                db.sync().unwrap();
                let old = db.runtime.published.load_full().view.clone();
                db.set_prune_policy(
                    "prior",
                    PrunePolicy {
                        max_age_ms: None,
                        max_weight: Some(0.0),
                        label: None,
                    },
                )
                .unwrap();
                db.runtime
                    .core
                    .lock()
                    .unwrap()
                    .as_ref()
                    .unwrap()
                    .manifest_write_lock
                    .inject_fault(boundary);
                let result = db.ensure_node_property_index("new_label", spec());
                assert!(
                    matches!(
                        result,
                        Err(EngineError::CommitOutcome {
                            state: CommitOutcomeState::Indeterminate,
                            ..
                        })
                    ),
                    "{boundary:?}: {result:?}"
                );
                let authority = root_bytes(dir.path());
                let recovered = load_manifest_readonly(dir.path()).unwrap().unwrap();
                assert!(recovered.prune_policies.contains_key("prior"));
                let declared = recovered.secondary_indexes.iter().any(|e| {
                    secondary_index_target_label_id(&e.target)
                        == recovered
                            .node_label_tokens
                            .get("new_label")
                            .copied()
                            .unwrap_or(0)
                });
                assert_eq!(
                    recovered.node_label_tokens.contains_key("new_label"),
                    declared,
                    "{boundary:?}"
                );
                assert_eq!(old.get_node(1).unwrap().unwrap().key, "n0");
                if mode == 0 {
                    assert!(db.close().is_err());
                } else if mode == 1 {
                    assert!(db.close_fast().is_err());
                }
                drop(db);
                assert_eq!(root_bytes(dir.path()), authority, "{boundary:?}/{mode}");
                drop(old);
                let db = tests::open_after_native_shutdown(dir.path(), &opts).unwrap();
                assert!(db.get_node(1).unwrap().is_some());
                ready_all(&db);
                db.close_fast().unwrap();
            }
        }
    }
    #[test]
    fn p45c2_component_publication_faults_preserve_files_all_closes() {
        use crate::persistence_fault::Boundary::*;
        for boundary in [
            ComponentCreate,
            ComponentWrite,
            ComponentSync,
            ComponentRename,
            ComponentDirSync,
        ] {
            for mode in 0..3 {
                let dir = tempfile::tempdir().unwrap();
                let opts = options();
                let db = DatabaseEngine::open(dir.path(), &opts).unwrap();
                db.batch_upsert_nodes(rows(0, 2, "old", 10)).unwrap();
                db.flush().unwrap();
                let old = db.runtime.published.load_full().view.clone();
                *db.runtime
                    .core
                    .lock()
                    .unwrap()
                    .as_ref()
                    .unwrap()
                    .candidate
                    .as_ref()
                    .unwrap()
                    .ddl
                    .component_fault
                    .lock()
                    .unwrap() = Some(boundary);
                db.ensure_node_property_index("C2", spec()).unwrap();
                wait_fence(&db);
                let authority = root_bytes(dir.path());
                let paths = crate::segment_writer::recoverable_optional_component_paths(
                    &segment_dir(dir.path(), 1),
                )
                .unwrap();
                for p in &paths {
                    assert!(p.exists(), "{boundary:?}: {}", p.display());
                }
                assert_eq!(old.get_node(1).unwrap().unwrap().key, "n0");
                if mode == 0 {
                    assert!(db.close().is_err());
                } else if mode == 1 {
                    assert!(db.close_fast().is_err());
                }
                drop(db);
                assert_eq!(root_bytes(dir.path()), authority);
                for p in &paths {
                    assert!(p.exists());
                }
                drop(old);
                let db = tests::open_after_native_shutdown(dir.path(), &opts).unwrap();
                assert_eq!(
                    db.query_node_ids(&query("old", 10)).unwrap().items,
                    vec![1, 2]
                );
                ready_all(&db);
                db.close_fast().unwrap();
            }
        }
    }
    #[test]
    fn p45c2_ready_durable_success_then_publication_failure_preserves_new_authority() {
        let dir = tempfile::tempdir().unwrap();
        let opts = options();
        let db = DatabaseEngine::open(dir.path(), &opts).unwrap();
        db.batch_upsert_nodes(rows(0, 2, "old", 10)).unwrap();
        db.flush().unwrap();
        let (seen_tx, seen_rx) = std::sync::mpsc::sync_channel(1);
        let (go_tx, go_rx) = std::sync::mpsc::sync_channel(1);
        *db.runtime
            .core
            .lock()
            .unwrap()
            .as_ref()
            .unwrap()
            .secondary_index_build_pause
            .lock()
            .unwrap() = Some(SecondaryIndexBuildPauseHook {
            ready_tx: seen_tx,
            release_rx: go_rx,
        });
        let ix = db.ensure_node_property_index("C2", spec()).unwrap();
        seen_rx.recv_timeout(Duration::from_secs(5)).unwrap();
        let building = db.runtime.published.load_full().view.clone();
        db.runtime
            .force_publication_error
            .store(true, Ordering::Release);
        go_tx.send(()).unwrap();
        wait_fence(&db);
        let manifest = load_manifest_readonly(dir.path()).unwrap().unwrap();
        assert!(manifest
            .secondary_indexes
            .iter()
            .any(|e| e.index_id == ix.index_id && e.state == SecondaryIndexState::Ready));
        let authority = root_bytes(dir.path());
        assert_eq!(
            building
                .query_node_ids_outcome(&query("old", 10))
                .unwrap()
                .value
                .items,
            vec![1, 2]
        );
        assert!(db.close_fast().is_err());
        drop(db);
        assert_eq!(authority, root_bytes(dir.path()));
        drop(building);
        let db = tests::open_after_native_shutdown(dir.path(), &opts).unwrap();
        assert_eq!(
            db.query_node_ids(&query("old", 10)).unwrap().items,
            vec![1, 2]
        );
        ready_all(&db);
        db.close_fast().unwrap();
    }
    #[test]
    fn p45c2_invalid_declaration_and_identity_overflow_leak_no_token() {
        let dir = tempfile::tempdir().unwrap();
        let opts = options();
        let db = DatabaseEngine::open(dir.path(), &opts).unwrap();
        let before = root_bytes(dir.path());
        assert!(db
            .ensure_node_property_index("bad", SecondaryIndexSpec::range(Vec::new()))
            .is_err());
        assert_eq!(root_bytes(dir.path()), before);
        db.runtime
            .with_core_mut(|core| {
                core.with_runtime_manifest_write(|m| {
                    m.next_secondary_index_id = u64::MAX;
                    Ok(())
                })
            })
            .unwrap();
        let before = root_bytes(dir.path());
        assert!(db.ensure_node_property_index("exhausted", spec()).is_err());
        assert_eq!(root_bytes(dir.path()), before);
        assert!(!db
            .manifest()
            .unwrap()
            .node_label_tokens
            .contains_key("exhausted"));
        db.close_fast().unwrap();
    }

    #[test]
    fn p45c2_retry_incarnation_coalesces_and_rejects_old_view_followups() {
        let dir = tempfile::tempdir().unwrap();
        let opts = options();
        let db = DatabaseEngine::open(dir.path(), &opts).unwrap();
        db.batch_upsert_nodes(rows(0, 2, "old", 10)).unwrap();
        db.flush().unwrap();
        let ix = db.ensure_node_property_index("C2", spec()).unwrap();
        ready_all(&db);
        let old = db.runtime.published.load_full().view.clone();
        db.batch_upsert_nodes(rows(2, 1, "not-query", 0)).unwrap();
        db.flush().unwrap();
        db.compact().unwrap();
        db.runtime.enqueue_secondary_index_read_followup(
            SecondaryIndexReadFollowup::CompoundRangeSidecarFailure {
                index_id: ix.index_id,
                error: Some(EngineError::CorruptRecord(
                    "stale compacted component".into(),
                )),
            },
            &old.sources,
        );
        db.sync().unwrap();
        assert_eq!(
            db.list_node_property_indexes().unwrap()[0].state,
            SecondaryIndexState::Ready
        );
        let active_ready = db.runtime.published.load_full().view.clone();
        db.runtime.enqueue_secondary_index_read_followup(
            SecondaryIndexReadFollowup::CompoundRangeSidecarFailure {
                index_id: ix.index_id,
                error: Some(EngineError::CorruptRecord(
                    "test bad optional payload".into(),
                )),
            },
            &active_ready.sources,
        );
        let until = Instant::now() + Duration::from_secs(5);
        while db.list_node_property_indexes().unwrap()[0].state != SecondaryIndexState::Failed {
            assert!(Instant::now() < until);
            std::thread::sleep(Duration::from_millis(2));
        }
        let failed = db.runtime.published.load_full().view.clone();
        let (seen_tx, seen_rx) = std::sync::mpsc::sync_channel(1);
        let (go_tx, go_rx) = std::sync::mpsc::sync_channel(1);
        *db.runtime
            .core
            .lock()
            .unwrap()
            .as_ref()
            .unwrap()
            .secondary_index_build_pause
            .lock()
            .unwrap() = Some(SecondaryIndexBuildPauseHook {
            ready_tx: seen_tx,
            release_rx: go_rx,
        });
        assert_eq!(
            db.ensure_node_property_index("C2", spec())
                .unwrap()
                .index_id,
            ix.index_id
        );
        seen_rx.recv_timeout(Duration::from_secs(5)).unwrap();
        let building = db.runtime.published.load_full().view.clone();
        db.batch_upsert_nodes(rows(0, 1, "new", 20)).unwrap();
        go_tx.send(()).unwrap();
        ready_all(&db);
        for _ in 0..128 {
            db.runtime.enqueue_secondary_index_read_followup(
                SecondaryIndexReadFollowup::CompoundRangeSidecarFailure {
                    index_id: ix.index_id,
                    error: Some(EngineError::CorruptRecord("late old incarnation".into())),
                },
                &old.sources,
            );
            assert!(
                db.runtime
                    .candidate
                    .as_ref()
                    .unwrap()
                    .index_followups
                    .pending
                    .lock()
                    .unwrap()
                    .len()
                    <= 1
            );
        }
        db.sync().unwrap();
        assert_eq!(
            db.list_node_property_indexes().unwrap()[0].state,
            SecondaryIndexState::Ready
        );
        assert_eq!(db.query_node_ids(&query("new", 20)).unwrap().items, vec![1]);
        for view in [&old, &failed, &building] {
            assert_eq!(
                view.query_node_ids_outcome(&query("old", 10))
                    .unwrap()
                    .value
                    .items,
                vec![1, 2]
            );
        }
        drop(old);
        drop(active_ready);
        drop(failed);
        drop(building);
        db.close_fast().unwrap();
    }
    #[test]
    fn p45c2_building_cancel_native_exit_and_optional_recovery_retry() {
        let dir = tempfile::tempdir().unwrap();
        let opts = options();
        let db = DatabaseEngine::open(dir.path(), &opts).unwrap();
        db.batch_upsert_nodes(rows(0, 2, "old", 10)).unwrap();
        db.flush().unwrap();
        let (seen_tx, seen_rx) = std::sync::mpsc::sync_channel(1);
        let (go_tx, go_rx) = std::sync::mpsc::sync_channel(1);
        *db.runtime
            .core
            .lock()
            .unwrap()
            .as_ref()
            .unwrap()
            .secondary_index_build_pause
            .lock()
            .unwrap() = Some(SecondaryIndexBuildPauseHook {
            ready_tx: seen_tx,
            release_rx: go_rx,
        });
        let ix = db.ensure_node_property_index("C2", spec()).unwrap();
        seen_rx.recv_timeout(Duration::from_secs(5)).unwrap();
        let cancel = db
            .runtime
            .core
            .lock()
            .unwrap()
            .as_ref()
            .unwrap()
            .candidate
            .as_ref()
            .unwrap()
            .ddl
            .build
            .as_ref()
            .unwrap()
            .cancel
            .clone();
        let closing = db.clone();
        let thread = std::thread::spawn(move || closing.close_fast());
        let until = Instant::now() + Duration::from_secs(5);
        while !cancel.load(Ordering::Acquire) {
            assert!(Instant::now() < until);
            std::thread::yield_now();
        }
        go_tx.send(()).unwrap();
        thread.join().unwrap().unwrap();
        assert_eq!(
            load_manifest_readonly(dir.path())
                .unwrap()
                .unwrap()
                .secondary_indexes[0]
                .state,
            SecondaryIndexState::Building
        );
        drop(db);
        let db = tests::open_after_native_shutdown(dir.path(), &opts).unwrap();
        ready_all(&db);
        assert_eq!(
            db.query_node_ids(&query("old", 10)).unwrap().items,
            vec![1, 2]
        );
        let path = {
            let core = db.runtime.core.lock().unwrap();
            let core = core.as_ref().unwrap();
            core.segments
                .iter()
                .flat_map(|s| s.component_paths())
                .find(|(_, _, p)| {
                    p.to_string_lossy()
                        .contains(&format!("node_compound_range_{}.", ix.index_id))
                })
                .map(|(id, _, p)| segment_dir(dir.path(), id).join(p))
                .unwrap()
        };
        super::p45b1_tests::finish(db);
        std::fs::remove_file(&path).unwrap();
        let db = tests::open_after_native_shutdown(dir.path(), &opts).unwrap();
        assert_eq!(
            db.query_node_ids(&query("old", 10)).unwrap().items,
            vec![1, 2]
        );
        ready_all(&db);
        let path = {
            let core = db.runtime.core.lock().unwrap();
            let core = core.as_ref().unwrap();
            core.segments
                .iter()
                .flat_map(|s| s.component_paths())
                .find(|(_, _, p)| {
                    p.to_string_lossy()
                        .contains(&format!("node_compound_range_{}.", ix.index_id))
                })
                .map(|(id, _, p)| segment_dir(dir.path(), id).join(p))
                .unwrap()
        };
        super::p45b1_tests::finish(db);
        let mut bytes = std::fs::read(&path).unwrap();
        bytes[0] ^= 0xff;
        std::fs::write(&path, bytes).unwrap();
        let db = tests::open_after_native_shutdown(dir.path(), &opts).unwrap();
        assert_eq!(
            db.list_node_property_indexes().unwrap()[0].state,
            SecondaryIndexState::Failed
        );
        assert_eq!(
            db.query_node_ids(&query("old", 10)).unwrap().items,
            vec![1, 2]
        );
        assert_eq!(
            db.ensure_node_property_index("C2", spec())
                .unwrap()
                .index_id,
            ix.index_id
        );
        ready_all(&db);
        db.close_fast().unwrap();
    }
    #[test]
    fn p45c2_drop_uncertainty_keeps_old_ready_view_and_recovery_authority() {
        use crate::persistence_fault::Boundary::*;
        for boundary in [CurrentToPrev, TmpToCurrent, DirSync] {
            for mode in 0..3 {
                let dir = tempfile::tempdir().unwrap();
                let opts = options();
                let db = DatabaseEngine::open(dir.path(), &opts).unwrap();
                db.batch_upsert_nodes(rows(0, 2, "old", 10)).unwrap();
                db.flush().unwrap();
                db.ensure_node_property_index("C2", spec()).unwrap();
                ready_all(&db);
                let old = db.runtime.published.load_full().view.clone();
                db.runtime
                    .core
                    .lock()
                    .unwrap()
                    .as_ref()
                    .unwrap()
                    .manifest_write_lock
                    .inject_fault(boundary);
                assert!(matches!(
                    db.drop_node_property_index("C2", spec()),
                    Err(EngineError::CommitOutcome {
                        state: CommitOutcomeState::Indeterminate,
                        ..
                    })
                ));
                let authority = root_bytes(dir.path());
                assert_eq!(
                    old.query_node_ids_outcome(&query("old", 10))
                        .unwrap()
                        .value
                        .items,
                    vec![1, 2]
                );
                if mode == 0 {
                    assert!(db.close().is_err());
                } else if mode == 1 {
                    assert!(db.close_fast().is_err());
                }
                drop(db);
                assert_eq!(authority, root_bytes(dir.path()));
                drop(old);
                let db = tests::open_after_native_shutdown(dir.path(), &opts).unwrap();
                assert_eq!(
                    db.query_node_ids(&query("old", 10)).unwrap().items,
                    vec![1, 2]
                );
                ready_all(&db);
                db.close_fast().unwrap();
            }
        }
    }

    #[test]
    fn p45c2_conflicting_ddl_keeps_capture_catalog_and_original_deadline() {
        let dir = tempfile::tempdir().unwrap();
        let opts = options();
        let limits = ConcurrencyOptions {
            admission_timeout_ms: 100,
            ..Default::default()
        };
        let db = DatabaseEngine::open_with_concurrency_options(dir.path(), &opts, &limits).unwrap();
        db.batch_upsert_nodes(rows(0, 2, "old", 10)).unwrap();
        let (seen_tx, seen_rx) = std::sync::mpsc::sync_channel(1);
        let (go_tx, go_rx) = std::sync::mpsc::sync_channel(1);
        *db.runtime
            .core
            .lock()
            .unwrap()
            .as_ref()
            .unwrap()
            .secondary_index_build_pause
            .lock()
            .unwrap() = Some(SecondaryIndexBuildPauseHook {
            ready_tx: seen_tx,
            release_rx: go_rx,
        });
        let ix = db.ensure_node_property_index("C2", spec()).unwrap();
        seen_rx.recv_timeout(Duration::from_secs(5)).unwrap();
        assert!(matches!(
            db.ensure_node_property_index("later_label", spec()),
            Err(EngineError::ResourceLimit {
                resource: "admission_deadline",
                retryable: true,
                ..
            })
        ));
        let manifest = db.manifest().unwrap();
        assert_eq!(manifest.secondary_indexes.len(), 1);
        assert!(!manifest.node_label_tokens.contains_key("later_label"));
        db.batch_upsert_nodes(rows(0, 1, "new", 20)).unwrap();
        go_tx.send(()).unwrap();
        ready(&db, ix.index_id);
        assert_eq!(db.query_node_ids(&query("new", 20)).unwrap().items, vec![1]);
        let later = db
            .ensure_node_property_index("later_label", spec())
            .unwrap();
        assert!(later.index_id > ix.index_id);
        ready_all(&db);
        db.close_fast().unwrap();
    }

    #[test]
    fn p45c2_captured_storage_jobs_defer_ddl_and_preserve_old_catalog() {
        // Cover flush construction, durable-publisher waiting, and compaction.
        // The released tests retain their late-declaration reconciliation schedule.
        for kind in 0..3 {
            let dir = tempfile::tempdir().unwrap();
            let opts = DbOptions {
                memtable_flush_threshold: 0,
                compact_after_n_flushes: if kind == 2 { 1 } else { 0 },
                ..Default::default()
            };
            let db = DatabaseEngine::open(dir.path(), &opts).unwrap();
            let (seen, release) = if kind == 2 {
                let pause = db.set_bg_compact_pause();
                db.batch_upsert_nodes(rows(0, 1, "old", 10)).unwrap();
                db.flush().unwrap();
                db.batch_upsert_nodes(rows(1, 1, "old", 10)).unwrap();
                db.flush().unwrap();
                pause
            } else {
                db.batch_upsert_nodes(rows(0, 2, "old", 10)).unwrap();
                let pause = if kind == 0 {
                    db.set_flush_pause()
                } else {
                    db.set_flush_publish_pause()
                };
                db.freeze_memtable().unwrap();
                db.enqueue_one_flush().unwrap();
                pause
            };
            seen.recv_timeout(Duration::from_secs(5)).unwrap();
            let old = db.runtime.published.load_full().view.clone();
            let worker = db.clone();
            let ddl = std::thread::spawn(move || worker.ensure_node_property_index("C2", spec()));
            let until = Instant::now() + Duration::from_secs(1);
            while db.runtime.candidate.as_ref().unwrap().pressure.occupied() == 0 {
                assert!(Instant::now() < until);
                std::thread::yield_now();
            }
            assert!(db.manifest().unwrap().secondary_indexes.is_empty());
            assert!(old.sources.secondary_index_entries.is_empty());
            assert!(db.get_node(1).unwrap().is_some());
            release.send(()).unwrap();
            let index = ddl.join().unwrap().unwrap();
            ready(&db, index.index_id);
            assert_eq!(
                db.query_node_ids(&query("old", 10)).unwrap().items,
                vec![1, 2]
            );
            assert_eq!(
                old.query_node_ids_outcome(&query("old", 10))
                    .unwrap()
                    .value
                    .items,
                vec![1, 2]
            );
            assert!(old.sources.secondary_index_entries.is_empty());
            assert_eq!(db.segment_count().unwrap(), 1);
            drop(old);
            db.close_fast().unwrap();
        }
    }

    #[test]
    fn p45c2_component_crash_reopens_building_authority() {
        if let Some(path) = std::env::var_os("OVERGRAPH_C2_COMPONENT_CRASH_DB") {
            let db = DatabaseEngine::open(Path::new(&path), &options()).unwrap();
            db.batch_upsert_nodes(rows(0, 2, "old", 10)).unwrap();
            db.flush().unwrap();
            db.sync().unwrap();
            {
                let core = db.runtime.core.lock().unwrap();
                let ddl = &core.as_ref().unwrap().candidate.as_ref().unwrap().ddl;
                *ddl.component_fault.lock().unwrap() =
                    Some(crate::persistence_fault::Boundary::ComponentRename);
                ddl.component_abort.store(true, Ordering::Release);
            }
            db.ensure_node_property_index("C2", spec()).unwrap();
            std::thread::sleep(Duration::from_secs(5));
            panic!("component crash hook did not fire");
        }
        let dir = tempfile::tempdir().unwrap();
        let output = std::process::Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "engine::p45c2_tests::p45c2_component_crash_reopens_building_authority",
                "--nocapture",
            ])
            .env("OVERGRAPH_C2_COMPONENT_CRASH_DB", dir.path())
            .output()
            .unwrap();
        #[cfg(unix)]
        {
            use std::os::unix::process::ExitStatusExt;
            assert_eq!(output.status.signal(), Some(6), "{:?}", output);
        }
        assert!(!output.status.success());
        assert!(String::from_utf8_lossy(&output.stderr).contains("named crash: ComponentRename"));
        if let Some(path) = std::env::var_os("OVERGRAPH_CP45C2_ARTIFACTS") {
            std::fs::write(
                Path::new(&path).join("component-crash-stderr.log"),
                &output.stderr,
            )
            .unwrap();
        }
        let manifest = load_manifest_readonly(dir.path()).unwrap().unwrap();
        assert_eq!(manifest.secondary_indexes.len(), 1);
        assert_eq!(
            manifest.secondary_indexes[0].state,
            SecondaryIndexState::Building
        );
        let db = DatabaseEngine::open(dir.path(), &options()).unwrap();
        ready_all(&db);
        assert_eq!(
            db.query_node_ids(&query("old", 10)).unwrap().items,
            vec![1, 2]
        );
        assert_eq!(db.next_node_id().unwrap(), 3);
        db.close_fast().unwrap();
    }
}
