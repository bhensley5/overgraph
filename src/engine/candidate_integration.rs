// Candidate routing for the existing locked engine. The memtable representation
// and its reader/writer exclusion remain unchanged through P45a.
thread_local! {
    // Fixed metadata only: these identities cannot retain a DB or payload graph.
    static CANDIDATE_PREPARATION: std::cell::RefCell<Vec<usize>> = const { std::cell::RefCell::new(Vec::new()) };
}
struct CandidatePreparationScope(usize);
impl CandidatePreparationScope {
    fn enter(runtime: &DbRuntime) -> Result<Self, EngineError> {
        let id = runtime as *const DbRuntime as usize;
        CANDIDATE_PREPARATION.with(|stack| {
            let mut stack = stack.borrow_mut();
            if stack.contains(&id) {
                return Err(EngineError::InvalidOperation("self-dependent write from input preparation".into()));
            }
            stack.push(id);
            Ok(Self(id))
        })
    }
    fn active(runtime: &DbRuntime) -> bool {
        let id = runtime as *const DbRuntime as usize;
        CANDIDATE_PREPARATION.with(|stack| stack.borrow().contains(&id))
    }
}
impl Drop for CandidatePreparationScope {
    fn drop(&mut self) {
        CANDIDATE_PREPARATION.with(|stack| assert_eq!(stack.borrow_mut().pop(), Some(self.0)));
    }
}
struct CandidateWriteAdmission {
    position: concurrency_lifecycle::QueuePosition<8>,
    deadline: concurrency_lifecycle::Deadline,
    _operation: concurrency_lifecycle::OperationPermit,
}

impl DatabaseEngine {
    fn open_candidate(
        path: &Path,
        options: &DbOptions,
        limits: &ConcurrencyOptions,
    ) -> Result<Self, EngineError> {
        limits.validate()?;
        if let WalSyncMode::GroupCommit {
            interval_ms,
            soft_trigger_bytes,
            hard_cap_bytes,
        } = options.wal_sync_mode
        {
            if interval_ms == 0 || soft_trigger_bytes == 0 || hard_cap_bytes <= soft_trigger_bytes {
                return Err(EngineError::InvalidOperation(
                    "invalid GroupCommit parameters".into(),
                ));
            }
        }
        let creation = completion::creation_guard();
        let (domain, registration, admission) = if path.exists() {
            let pin = directory_identity::DirectoryPin::open(path)?;
            if let Some(session) =
                completion::DirectorySession::attach(pin, limits).map_err(candidate_start_error)?
            {
                let domain = session.domain().clone();
                let registration = session.registration_handle();
                (
                    domain,
                    registration,
                    preflight::DirectoryAdmission::Existing(session),
                )
            } else {
                let configured = reclamation::ReclaimDomain::configured(
                    limits,
                    Some(directory_identity::DirectoryPin::open(path)?),
                )?;
                let session = configured
                    .registration
                    .bind_directory(configured.domain.clone(), limits)
                    .map_err(candidate_start_error)?;
                (
                    configured.domain,
                    configured.registration,
                    preflight::DirectoryAdmission::Existing(session),
                )
            }
        } else {
            fn existing_parent(path: &Path) -> &Path {
                path.parent()
                    .filter(|parent| !parent.as_os_str().is_empty())
                    .unwrap_or(Path::new("."))
            }
            let mut parent = existing_parent(path);
            while !parent.exists() {
                parent = existing_parent(parent);
            }
            let configured = reclamation::ReclaimDomain::configured(limits, None)?;
            let admission = preflight::DirectoryAdmission::Missing {
                parent: directory_identity::open_directory(parent)?,
                gate: creation,
            };
            return Self::finish_candidate_open(
                path,
                options,
                limits,
                configured.domain,
                configured.registration,
                admission,
            );
        };
        // Existing directories are already pinned by their session. Serializing
        // attach/construction does not authorize a pathname alias to bypass it.
        drop(creation);
        Self::finish_candidate_open(path, options, limits, domain, registration, admission)
    }

    fn finish_candidate_open(
        path: &Path,
        options: &DbOptions,
        limits: &ConcurrencyOptions,
        domain: reclamation::DomainHandle,
        registration: completion::DomainRegistration,
        admission: preflight::DirectoryAdmission,
    ) -> Result<Self, EngineError> {
        let permit = preflight::scan(path, options, limits, &domain, admission)?;
        // scan is read-only; this final witness check authorizes the subsequent
        // promotion/creation/replay boundary. Never move mutation before it.
        permit.revalidate()?;
        let summary = permit.summary();
        let active = permit
            .wal()
            .iter()
            .find(|w| !w.frozen)
            .map(|w| w.replay)
            .unwrap_or_default();
        let (manifest, readers, admission) = permit.into_startup();
        let session = match admission {
            preflight::DirectoryAdmission::Existing(session) => session,
            preflight::DirectoryAdmission::Missing {
                parent: _,
                gate: _gate,
            } => {
                std::fs::create_dir_all(path)?;
                registration
                    .bind_created_identity(directory_identity::DirectoryPin::open(path)?)
                    .map_err(candidate_start_error)?;
                registration
                    .bind_directory(domain.clone(), limits)
                    .map_err(candidate_start_error)?
            }
        };
        if summary.source == preflight::ManifestSource::Empty {
            write_manifest(path, &manifest)?;
        } else {
            // Promote the same validated recovery authority using existing rules.
            // Actual readers and normalized decoded state are transferred below.
            let _ = load_manifest(path)?;
        }
        let mut core =
            EngineCore::open_startup(path, options, Some((manifest, readers)), Some(&domain))?;
        core.candidate = Some(CandidateCore {
            maintenance_error: None,
            domain: domain.clone(),
            options: limits.clone(),
            generation_operations: active.operations,
            generation_bytes: active.wire_bytes,
            prepared_frames: Vec::new(),
            encode_scratch: Vec::new(),
            accepted: false,
            deadline: None,
            heavy: Arc::new(AtomicBool::new(false)),
            ddl: CandidateDdl::default(),
            last_segment_cleanup: None,
            #[cfg(test)]
            segment_cleanup_now: None,
        });
        core.seed_candidate_component_cleanup()?;
        // Startup is private until every actual graph below has a guardian.
        core.published_read_sources.take();
        core.schema_snapshot_dirty = true;
        core.guard_current_graphs()?;
        core.rebuild_published_read_sources()?;
        let lifetime = concurrency_lifecycle::Lifecycle::allocate();
        let client = lifetime.initial_owner();
        let stats = domain.stats();
        let mut runtime = DbRuntime::new(path.to_owned(), core);
        runtime
            .lifecycle
            .get_mut()
            .unwrap()
            .coordinator_queue_capacity = 8;
        let component_cleanup = runtime.core.get_mut().unwrap().as_ref().and_then(|core| core.candidate.as_ref()).is_some_and(|c| !c.ddl.cleanup_paths.is_empty());
        let index_followups = runtime.core.get_mut().unwrap().as_ref().unwrap().candidate.as_ref().unwrap().ddl.followups.clone();
        runtime.candidate = Some(CandidateRuntime {
            index_followups,
            cleanup_pending: AtomicBool::new(component_cleanup),
            session: Mutex::new(Some(session)),
            stats,
            close_result: BindingCloseHandle::new(lifetime.clone()),
            lifetime,
            pressure: Arc::new(concurrency_lifecycle::Pressure::new()),
            options: limits.clone(),
            close_fast: AtomicBool::new(true),
        });
        let runtime = Arc::new(runtime);
        runtime.install_core_runtime_handle();
        Ok(Self {
            runtime,
            client: Some(client),
        })
    }
}

fn candidate_start_error(error: completion::StartError) -> EngineError {
    EngineError::InvalidOperation(format!("candidate directory admission: {error:?}"))
}

impl EngineCore {
    fn guard_current_graphs(&mut self) -> Result<(), EngineError> {
        let Some(candidate) = &self.candidate else {
            return Ok(());
        };
        use reclamation::ResourceClass;
        self.label_catalog.guard(&candidate.domain, ResourceClass::Catalog)?;
        self.secondary_index_catalog.guard(&candidate.domain, ResourceClass::Catalog)?;
        self.secondary_index_entries.guard(&candidate.domain, ResourceClass::Catalog)?;
        if let Some(catalog)=&mut self.fixed_secondary_catalog {catalog.guard(&candidate.domain,ResourceClass::Catalog)?;}
        self.memtable
            .guard(&candidate.domain, ResourceClass::Generation)?;
        self.active_degree_overlay
            .guard(&candidate.domain, ResourceClass::Catalog)?;
        for epoch in &mut self.immutable_epochs {
            epoch
                .memtable
                .guard(&candidate.domain, ResourceClass::Generation)?;
            epoch
                .degree_overlay
                .guard(&candidate.domain, ResourceClass::Catalog)?;
        }
        for reader in &mut self.segments {
            reader.retain_secondary_payloads(None);
            reader.guard_storage(&candidate.domain, reader.segment_id)?;
            reader.pin_components(&candidate.domain, reader.component_paths())?;
        }
        candidate
            .domain
            .stats()
            .set_generations(1, self.immutable_epochs.len() as u64);
        Ok(())
    }
}

impl DbRuntime {
    fn admit_candidate_write(&self) -> Result<Option<CandidateWriteAdmission>, EngineError> {
        let Some(candidate) = &self.candidate else {
            return Ok(None);
        };
        let deadline = concurrency_lifecycle::Deadline::new(candidate.options.admission_timeout_ms)
            .map_err(EngineError::from)?;
        let _operation = candidate.lifetime.admit().map_err(|error| match error {
            concurrency_lifecycle::Outcome::Refused("closed") => EngineError::DatabaseClosed,
            error => EngineError::from(error),
        })?;
        let position = candidate.pressure.try_enter().map_err(EngineError::from)?;
        #[cfg(feature="binding-test-hooks")]
        self.binding_hooks.hit("write");
        Ok(Some(CandidateWriteAdmission {
            position,
            deadline,
            _operation,
        }))
    }

    fn finish_candidate_shutdown(&self) {
        let candidate = self.candidate.as_ref().expect("governed shutdown");
        candidate.lifetime.stop_admission();
        #[cfg(feature="binding-test-hooks")]
        self.binding_hooks.hit("shutdown");
        candidate.pressure.close();
        self.wait_for_reads_to_drain();
        candidate.lifetime.drain_independent();
        candidate.index_followups.pending.lock().unwrap().clear();
        candidate.index_followups.ready.store(false, Ordering::Release);
        candidate.index_followups.in_flight.store(0, Ordering::Release);
        let mut session = candidate
            .session
            .lock()
            .unwrap()
            .take()
            .expect("live session");
        session.begin_shutdown();
        let domain = session.domain().clone();
        let mut core = self.core.lock().unwrap().take().expect("shutdown core");
        let result = if candidate.close_fast.load(Ordering::Acquire) {
            core.close_fast()
        } else {
            core.close()
        };
        core.shutdown_complete = true;
        self.published
            .store(Arc::new(PublishedReadState::closed_sentinel()));
        core.candidate.take(); // No registered child owns its domain/registry.
                               // The completed core's variable fanout and cached graphs are destroyed
                               // by the reclaimer. All persistence/native child shutdown is done above.
        let owner = domain
            .guard_arc(Arc::new(core), reclamation::ResourceClass::Sources)
            .unwrap_or_else(|e| {
                concurrency_lifecycle::fail_stop(&format!("shutdown adoption: {e:?}"))
            });
        drop(owner);
        domain.stats().set_generations(0, 0);
        session.close();
        drop(session);
        drop(domain);
        candidate.close_result.publish(result);
        self.set_open_state(RuntimeOpenState::Closed);
        let mut lifecycle = self.lifecycle.lock().unwrap();
        lifecycle.release_closed_backing();
        lifecycle.closed = true;
        lifecycle.coordinator_stopped = true;
        self.lifecycle_cv.notify_all();
        drop(lifecycle);
        candidate.lifetime.complete_close();
        #[cfg(feature="binding-test-hooks")]
        self.binding_hooks.hit("completed");
        candidate.close_result.notify_completed();
    }
}

impl PublishedReadState {
    fn closed_sentinel() -> Self {
        let label_catalog = SharedArc::new(ReadLabelCatalogSnapshot::from_runtime(
            &RuntimeLabelCatalog::from_manifest(&default_manifest()).expect("empty catalog"),
        ));
        let coverage = SharedArc::new(DeclaredIndexRuntimeCoverage::from_readers(&[], &[]));
        let planner_stats =
            SharedArc::new(PlannerStatsView::build_from_readers(0, &[], &[], &coverage));
        let sources = SharedArc::new(PublishedReadSources {
            manifest: ReadManifestState {
                declared_prune_policies: BTreeMap::new(),
                prune_policies: BTreeMap::new(),
                dense_vector: None,
            },
            memtable: SharedArc::new(Memtable::new()),
            immutable_epochs: Vec::new(),
            segments: Vec::new(),
            secondary_index_catalog: SecondaryIndexCatalog::default().into(),
            secondary_index_entries: Vec::new(),
            declared_index_runtime_coverage: coverage,
            planner_stats,
            #[cfg(test)]
            planning_probe_counters: QueryPlanningProbeCounters::default(),
            #[cfg(test)]
            query_execution_counters: QueryExecutionCounters::default(),
        });
        Self {
            view: Arc::new(ReadView::from_published_sources(
                sources,
                0,
                DegreeOverlaySnapshot::empty(),
                label_catalog.clone(),
            )),
            label_catalog,
            schema_catalog: SharedArc::new(PublishedSchemaCatalogSnapshot {
                next_schema_id: 1,
                node_schemas: Vec::new(),
                edge_schemas: Vec::new(),
            }),
            edge_uniqueness: true,
            #[cfg(test)]
            engine_seq: 0,
            #[cfg(test)]
            active_wal_generation_id: 0,
        }
    }
}

// Bounded observations for one explicitly selected diagnostic cohort. The fixture
// runner drains/prints these records without holding engine or diagnostic locks.
#[cfg(test)]
mod maintenance_diagnostic {
    use super::*;
    const MAX_JOBS: usize = 1_024;
    const MAX_EVENTS: usize = 8_192;
    static ENABLED: AtomicBool = AtomicBool::new(false);
    static TRACE: Mutex<Option<Trace>> = Mutex::new(None);

    struct Job {
        id: u64,
        db: PathBuf,
        output: u64,
        started: Instant,
        stage_started: Instant,
        stage: &'static str,
        // Late stage observations cannot undo the terminal lease release.
        released: bool,
    }
    struct Trace {
        started: Instant,
        filter_db: Option<PathBuf>,
        jobs: Vec<Job>,
        events: Vec<String>,
        recorded: usize,
        omitted: usize,
    }
    impl Trace {
        fn outstanding(&self) -> impl Iterator<Item = &Job> {
            self.jobs.iter().filter(|job| !job.released)
        }
        fn push(&mut self, line: String) {
            if self.recorded < MAX_EVENTS {
                self.events.push(line);
                self.recorded += 1;
            } else {
                self.omitted += 1;
            }
        }
        fn transition(&mut self, index: usize, stage: &'static str) {
            let job = &mut self.jobs[index];
            let prior_stage_us = job.stage_started.elapsed().as_micros();
            job.released |= stage == "heavy_release";
            job.stage = stage;
            job.stage_started = Instant::now();
            let line = format!(
                "maintenance_diagnostic db={:?} job={} output={} stage={} job_us={} prior_stage_us={} cohort_us={} released={}",
                job.db, job.id, job.output, stage, job.started.elapsed().as_micros(),
                prior_stage_us, self.started.elapsed().as_micros(), job.released,
            );
            self.push(line);
        }
    }
    pub(super) fn begin() {
        begin_filtered(None);
    }
    fn begin_filtered(filter_db: Option<PathBuf>) {
        assert!(!ENABLED.swap(true, Ordering::AcqRel));
        *TRACE.lock().unwrap() = Some(Trace {
            started: Instant::now(),
            filter_db,
            jobs: Vec::with_capacity(MAX_JOBS),
            events: Vec::with_capacity(MAX_EVENTS),
            recorded: 0,
            omitted: 0,
        });
    }
    pub(super) struct DatabaseTrace;
    impl Drop for DatabaseTrace {
        fn drop(&mut self) {
            finish();
        }
    }
    pub(super) fn for_database(db: &Path) -> DatabaseTrace {
        begin_filtered(Some(db.to_owned()));
        DatabaseTrace
    }
    pub(super) fn enqueue(db: &Path, output: u64) -> Option<u64> {
        if !ENABLED.load(Ordering::Acquire) {
            return None;
        }
        let mut trace = TRACE.lock().unwrap();
        let trace = trace.as_mut()?;
        if trace.filter_db.as_ref().is_some_and(|selected| selected != db) {
            return None;
        }
        if trace.jobs.len() == MAX_JOBS {
            trace.omitted += 1;
            return None;
        }
        let id = trace.jobs.len() as u64 + 1;
        let now = Instant::now();
        trace.jobs.push(Job {
            id,
            db: db.to_owned(),
            output,
            started: now,
            stage_started: now,
            stage: "heavy_acquired",
            released: false,
        });
        trace.transition(trace.jobs.len() - 1, "heavy_acquired");
        Some(id)
    }
    pub(super) fn stage_id(id: Option<u64>, stage: &'static str) {
        let Some(id) = id else {
            return;
        };
        let mut trace = TRACE.lock().unwrap();
        if let Some(trace) = trace.as_mut() {
            if let Some(index) = trace.jobs.iter().position(|job| job.id == id) {
                trace.transition(index, stage);
            }
        }
    }
    pub(super) fn stage(db: &Path, output: u64, stage: &'static str) {
        if !ENABLED.load(Ordering::Acquire) {
            return;
        }
        let mut trace = TRACE.lock().unwrap();
        if let Some(trace) = trace.as_mut() {
            if let Some(index) = trace
                .jobs
                .iter()
                .rposition(|job| job.db == db && job.output == output && !job.released)
            {
                trace.transition(index, stage);
            }
        }
    }
    pub(super) fn output_stage(tmp: &Path, output: u64, stage_name: &'static str) {
        if let Some(db) = tmp.parent().and_then(Path::parent) {
            stage(db, output, stage_name);
        }
    }
    pub(super) fn admission(db: &Path, stage: &'static str, detail: String) {
        if !ENABLED.load(Ordering::Acquire) {
            // Refusal-only test diagnostics remain available in ordinary CI too.
            eprintln!("admission_diagnostic db={db:?} stage={stage} {detail} trace=disabled");
            return;
        }
        let mut trace = TRACE.lock().unwrap();
        if let Some(trace) = trace.as_mut() {
            if trace.filter_db.as_ref().is_some_and(|selected| selected != db) {
                eprintln!("admission_diagnostic db={db:?} stage={stage} {detail} trace=other_database");
                return;
            }
            let outstanding = trace
                .outstanding()
                .filter(|job| job.db == db)
                .map(|job| {
                    (
                        job.id,
                        job.output,
                        job.stage,
                        job.started.elapsed().as_micros(),
                        job.stage_started.elapsed().as_micros(),
                    )
                })
                .collect::<Vec<_>>();
            trace.push(format!("admission_diagnostic db={db:?} stage={stage} {detail} outstanding_job_output_stage_job_us_stage_us={outstanding:?}"));
        }
    }
    pub(super) fn print_pending() {
        let lines = {
            let mut trace = TRACE.lock().unwrap();
            trace
                .as_mut()
                .map(|trace| trace.events.drain(..).collect::<Vec<_>>())
                .unwrap_or_default()
        };
        // Called only by the outer fixture runner; no engine/trace lock is held.
        for line in lines {
            eprintln!("{line}");
        }
    }
    pub(super) fn outstanding() -> usize {
        TRACE
            .lock()
            .unwrap()
            .as_ref()
            .map_or(0, |trace| trace.outstanding().count())
    }
    pub(super) fn finish() {
        ENABLED.store(false, Ordering::Release);
        print_pending();
        let trace = TRACE.lock().unwrap().take().unwrap();
        eprintln!(
            "maintenance_diagnostic_summary jobs={} events={} omitted={} outstanding={:?}",
            trace.jobs.len(),
            trace.recorded,
            trace.omitted,
            trace
                .outstanding()
                .map(|job| (&job.db, job.id, job.output, job.stage))
                .collect::<Vec<_>>()
        );
    }

    #[cfg(test)]
    mod tests {
        use super::*;

        #[test]
        fn late_completion_observation_cannot_reopen_released_job() {
            let now = Instant::now();
            let mut trace = Trace {
                started: now,
                filter_db: None,
                jobs: (1..=2)
                    .map(|id| Job {
                        id,
                        db: PathBuf::from("trace-state"),
                        output: id,
                        started: now,
                        stage_started: now,
                        stage: "flush_ready_for_adoption",
                        released: false,
                    })
                    .collect(),
                events: Vec::new(),
                recorded: 0,
                omitted: 0,
            };
            assert_eq!(trace.outstanding().count(), 2);
            // Ready was published; publisher pauses before its observation while
            // the coordinator adopts and releases. Then the observation arrives.
            trace.transition(0, "flush_adoption_start");
            trace.transition(0, "heavy_release");
            assert_eq!(trace.outstanding().count(), 1);
            trace.transition(0, "flush_completion_ready");
            assert!(trace.jobs[0].released);
            assert_eq!(trace.jobs[0].stage, "flush_completion_ready");
            assert_eq!(
                trace.outstanding().map(|job| job.id).collect::<Vec<_>>(),
                vec![2]
            );
            assert_eq!(trace.events.len(), 3);
            let late = trace.events.last().unwrap();
            assert!(late.contains("stage=flush_completion_ready"));
            assert!(late.ends_with("released=true"));
        }
    }
}

#[cfg(test)]
pub(crate) fn trace_flush_stage_for_test(path: &Path, segment_id: u64, stage: &'static str) {
    maintenance_diagnostic::output_stage(path, segment_id, stage);
}

#[cfg(test)]
impl EngineCore {
    fn trace_candidate_admission_for_test(
        &self,
        stage: &'static str,
        deadline: Option<concurrency_lifecycle::Deadline>,
    ) {
        let Some(candidate) = &self.candidate else {
            return;
        };
        maintenance_diagnostic::admission(&self.db_dir, stage, format!(
            "age_ms={:?} accepted={} seq={} wal_generation={} heavy={} frozen={:?} flush_ready_applied={:?} compact_complete={:?} segments={}",
            deadline.map(|d| d.started().elapsed().as_millis()), candidate.accepted,
            self.engine_seq, self.active_wal_generation_id, candidate.heavy.load(Ordering::Acquire),
            self.immutable_epochs.iter().map(|e| (e.epoch_id, e.in_flight)).collect::<Vec<_>>(),
            self.bg_flush.as_ref().map(|b| (b.events_ready.load(Ordering::Acquire), b.events_applied)),
            self.bg_compact.as_ref().map(|b| b.completed.load(Ordering::Acquire)), self.segments.len(),
        ));
    }
}

impl EngineCore {
    fn prepare_candidate_frames(
        &mut self,
        ops: &[WalOp],
        prior_ids: IdCounterSnapshot,
    ) -> Result<(), EngineError> {
        let result = self.prepare_candidate_frames_inner(ops, prior_ids);
        if result.is_err() {
            if let Some(candidate) = &mut self.candidate {
                let retained = candidate.options.max_batch_bytes.min(1024 * 1024);
                for buffer in [&mut candidate.encode_scratch, &mut candidate.prepared_frames] {
                    buffer.clear();
                    if buffer.capacity() > retained { *buffer = Vec::new(); }
                }
            }
        }
        result
    }
    fn prepare_candidate_frames_inner(
        &mut self,
        ops: &[WalOp],
        prior_ids: IdCounterSnapshot,
    ) -> Result<(), EngineError> {
        let Some(candidate) = &mut self.candidate else {
            return Ok(());
        };
        let options = candidate.options.clone();
        if ops.len() > options.max_batch_operations {
            return Err(EngineError::ResourceLimit {
                resource: "max_batch_operations",
                requested: ops.len() as u64,
                limit: options.max_batch_operations as u64,
                retryable: false,
            });
        }
        let first = self
            .engine_seq
            .checked_add(1)
            .ok_or_else(|| EngineError::InvalidOperation("sequence exhausted".into()))?;
        crate::wal::prepare_v3_operations_bounded(
            ops,
            first,
            &mut candidate.encode_scratch,
            &mut candidate.prepared_frames,
            options.max_batch_bytes,
        )?;
        let bytes = candidate.prepared_frames.len();
        assert!(bytes <= options.max_batch_bytes, "prepared request exceeds configured limit");
        let rotate = candidate
            .generation_operations
            .checked_add(ops.len())
            .is_none_or(|v| v > options.generation_operation_limit)
            || candidate
                .generation_bytes
                .checked_add(bytes)
                .is_none_or(|v| v > options.generation_wal_bytes);
        if rotate && candidate.generation_operations > 0 {
            // The normalized request owns tentative IDs. A maintenance checkpoint
            // must persist only the preceding committed counters, never those IDs.
            let reserved = IdCounterSnapshot::capture(self);
            prior_ids.restore(self);
            let building = self.candidate.as_ref().is_some_and(|c| c.ddl.build.is_some());
            let result = self.freeze_memtable().and_then(|_| if building { Ok(None) } else { self.flush() });
            reserved.restore(self);
            if !self.manifest_write_lock.fenced() {
                // Successful generation maintenance is distinct from this
                // request's WAL acceptance. A later known refusal must not fence.
                self.candidate.as_mut().unwrap().accepted = false;
            }
            result?;
        }
        if self
            .candidate
            .as_ref()
            .is_some_and(|c| c.deadline.is_some_and(|d| d.expired()))
        {
            #[cfg(test)]
            self.trace_candidate_admission_for_test(
                "frames_after_rotation",
                self.candidate.as_ref().and_then(|c| c.deadline),
            );
            return Err(EngineError::ResourceLimit {
                resource: "admission_deadline",
                requested: 1,
                limit: 0,
                retryable: true,
            });
        }
        Ok(())
    }
}

#[cfg(test)]
mod real_route_tests {
    use super::*;
    use std::time::{Duration, Instant};
    pub(super) fn wait_native(stats: &reclamation::ReclamationStatsHandle) {
        let until = Instant::now() + Duration::from_secs(20);
        while !matches!(
            stats.read_full().unwrap().close_state,
            ReclamationCloseState::Drained
        ) {
            assert!(
                Instant::now() < until,
                "unfinished {:?}",
                stats.read_full().unwrap()
            );
            std::thread::sleep(Duration::from_millis(1));
        }
        assert_eq!(stats.read().occupied, 0);
    }
    fn directory_children(path: &Path) -> Vec<std::ffi::OsString> {
        let mut names = std::fs::read_dir(path)
            .unwrap()
            .map(|entry| entry.unwrap().file_name())
            .collect::<Vec<_>>();
        names.sort();
        names
    }

    #[test]
    fn p45a3_real_default_and_configured_open_use_guarded_sources_and_limits() {
        let dir = tempfile::TempDir::new().unwrap();
        let limits = ConcurrencyOptions::default()
            .with_max_batch_operations(2)
            .with_generation_operation_limit(4);
        let db = DatabaseEngine::open_with_concurrency_options(
            dir.path(),
            &DbOptions::default(),
            &limits,
        )
        .unwrap();
        let (_guard, published) = db.runtime.published_snapshot().unwrap();
        assert!(published.view.sources.is_guarded());
        assert!(published.view.memtable.is_guarded());
        drop(_guard);
        let id = db
            .upsert_node(&["R"], "one", UpsertNodeOptions::default())
            .unwrap();
        assert_eq!(db.get_node(id).unwrap().unwrap().key, "one");
        let inputs = (0..3)
            .map(|i| NodeInput {
                labels: vec!["R".into()],
                key: format!("reject-{i}"),
                props: BTreeMap::new(),
                weight: 1.0,
                dense_vector: None,
                sparse_vector: None,
            })
            .collect();
        assert!(matches!(
            db.batch_upsert_nodes(inputs),
            Err(EngineError::ResourceLimit {
                resource: "max_batch_operations",
                retryable: false,
                ..
            })
        ));
        let stats = db.runtime.candidate.as_ref().unwrap().stats.clone();
        db.close_fast().unwrap();
        assert!(db.reclamation_stats().is_ok());
        assert!(stats.read().occupied > 0); // Actual old sources, never dummy stats.
        assert!(published.view.get_node(id).unwrap().is_none()); // Old frontier predates write.
        drop(published);
        wait_native(&stats);
    }
    #[test]
    fn p45a3_real_generation_rotation_atomicity_and_reopen() {
        const FUNCTIONAL_ADMISSION_TIMEOUT_MS: u64 = 30_000;
        let dir = tempfile::TempDir::new().unwrap();
        let limits = ConcurrencyOptions::default()
            .with_max_batch_operations(2)
            .with_generation_operation_limit(2)
            .with_admission_timeout_ms(FUNCTIONAL_ADMISSION_TIMEOUT_MS);
        let db = DatabaseEngine::open_with_concurrency_options(
            dir.path(),
            &DbOptions::default(),
            &limits,
        )
        .unwrap();
        db.upsert_node(&["R"], "first", UpsertNodeOptions::default())
            .unwrap();
        db.upsert_node(&["R"], "second", UpsertNodeOptions::default())
            .unwrap();
        db.upsert_node(&["R"], "third", UpsertNodeOptions::default())
            .unwrap();
        for id in 1..=3 { assert!(db.get_node(id).unwrap().is_some(), "rotated active node {id} must be visible immediately"); }
        assert!(db
            .runtime
            .with_core_ref(|c| Ok(c.active_wal_generation_id > 0))
            .unwrap());
        let stats = db.runtime.candidate.as_ref().unwrap().stats.clone();
        db.close_fast().unwrap();
        wait_native(&stats);
        let reopened = DatabaseEngine::open(dir.path(), &DbOptions::default()).unwrap();
        assert!(reopened.runtime.candidate.is_some());
        for id in 1..=3 {
            assert!(reopened.get_node(id).unwrap().is_some());
        }
        let stats = reopened.runtime.candidate.as_ref().unwrap().stats.clone();
        reopened.close_fast().unwrap();
        wait_native(&stats);
    }
    #[test]
    fn p45a3_real_transaction_clients_snapshots_results_and_both_drop_orders() {
        for commit in [false, true] {
            let dir = tempfile::TempDir::new().unwrap();
            let db = DatabaseEngine::open(dir.path(), &DbOptions::default()).unwrap();
            let first = db
                .upsert_node("R", "original", UpsertNodeOptions::default())
                .unwrap();
            let stats = db.runtime.candidate.as_ref().unwrap().stats.clone();
            let mut txn = db.begin_write_txn().unwrap();
            let owned = txn.get_node(TxnNodeRef::Id(first)).unwrap().unwrap();
            txn.upsert_node("R", "staged", UpsertNodeOptions::default())
                .unwrap();
            drop(db);
            assert!(!matches!(
                stats.read_full().unwrap().close_state,
                ReclamationCloseState::Drained
            ));
            if commit {
                txn.commit().unwrap();
            } else {
                txn.rollback().unwrap();
            }
            // Keep the closed transaction and owned result alive through native drain.
            wait_native(&stats);
            assert_eq!(owned.key, "original");
            assert!(matches!(
                txn.get_node(TxnNodeRef::Id(first)),
                Err(EngineError::TxnClosed)
            ));
            drop(txn);
            let db = DatabaseEngine::open(dir.path(), &DbOptions::default()).unwrap();
            assert_eq!(db.get_node_by_key("R", "staged").unwrap().is_some(), commit);
            let txn = db.begin_write_txn().unwrap();
            drop(txn); // Reverse order: DB still owns admission.
            db.upsert_node("R", "still-open", UpsertNodeOptions::default())
                .unwrap();
            let stats = db.runtime.candidate.as_ref().unwrap().stats.clone();
            db.close_fast().unwrap();
            wait_native(&stats);
        }
    }
    #[test]
    fn p45a3_real_maintenance_completion_state_is_scoped_and_closed_backing_released() {
        for close_mode in 0..3 {
            let dir = tempfile::TempDir::new().unwrap();
            let options = DbOptions {
                memtable_flush_threshold: 0,
                compact_after_n_flushes: 0,
                ..DbOptions::default()
            };
            let db = DatabaseEngine::open(dir.path(), &options).unwrap();
            for round in 0..6 {
                for item in 0..2 {
                    db.upsert_node(
                        "R",
                        &format!("{round}-{item}"),
                        UpsertNodeOptions::default(),
                    )
                    .unwrap();
                    db.freeze_memtable().unwrap();
                    db.enqueue_one_flush().unwrap();
                    let until = Instant::now() + Duration::from_secs(10);
                    loop {
                        let core = db.runtime.core.lock().unwrap();
                        if core.as_ref().unwrap().immutable_epochs.is_empty() {
                            break;
                        }
                        drop(core);
                        assert!(Instant::now() < until, "flush did not settle");
                        std::thread::sleep(Duration::from_millis(1));
                    }
                    // Unawaited completion goes only to cfg(test) history. The
                    // production slot stays empty regardless of maintenance age.
                    assert!(db.runtime.lifecycle.lock().unwrap().awaited_flush.is_none());
                }
                db.start_bg_compact().unwrap();
                let until = Instant::now() + Duration::from_secs(10);
                loop {
                    let core = db.runtime.core.lock().unwrap();
                    if core.as_ref().unwrap().bg_compact.is_none() {
                        break;
                    }
                    drop(core);
                    assert!(Instant::now() < until, "compaction did not settle");
                    std::thread::sleep(Duration::from_millis(1));
                }
                let lifecycle = db.runtime.lifecycle.lock().unwrap();
                assert!(lifecycle.awaited_flush.is_none());
                assert_eq!(lifecycle.completed_flushes_by_epoch.len(), (round + 1) * 2);
                assert_eq!(lifecycle.completed_bg_compactions.len(), round + 1);
            }
            // Exercise a real production waiter as well as unawaited history.
            db.upsert_node("R", "awaited", UpsertNodeOptions::default())
                .unwrap();
            assert!(db.flush().unwrap().is_some());
            assert!(db.runtime.lifecycle.lock().unwrap().awaited_flush.is_none());
            let runtime = db.runtime.clone();
            let stats = runtime.candidate.as_ref().unwrap().stats.clone();
            let mut retained_wrapper = Some(db);
            match close_mode {
                0 => retained_wrapper.as_ref().unwrap().close().unwrap(),
                1 => retained_wrapper.as_ref().unwrap().close_fast().unwrap(),
                _ => drop(retained_wrapper.take()),
            }
            wait_native(&stats);
            {
                let lifecycle = runtime.lifecycle.lock().unwrap();
                assert!(lifecycle.closed);
                assert!(lifecycle.awaited_flush.is_none());
                assert_eq!(lifecycle.completed_flushes_by_epoch.capacity(), 0);
                assert_eq!(lifecycle.completed_flush_order.capacity(), 0);
                assert_eq!(lifecycle.completed_bg_compactions.capacity(), 0);
                assert_eq!(lifecycle.pending_core_writes.capacity(), 0);
                assert_eq!(lifecycle.pending_secondary_index_followups.capacity(), 0);
            }
            // Closed wrappers/runtime observers may survive actual native exit
            // and a new DB without retaining historical collections or custody.
            let reopened = DatabaseEngine::open(dir.path(), &options).unwrap();
            for round in 0..6 {
                for item in 0..2 {
                    assert!(reopened
                        .get_node_by_key("R", &format!("{round}-{item}"))
                        .unwrap()
                        .is_some());
                }
            }
            assert!(reopened.get_node_by_key("R", "awaited").unwrap().is_some());
            let reopened_stats = reopened.runtime.candidate.as_ref().unwrap().stats.clone();
            reopened.close_fast().unwrap();
            wait_native(&reopened_stats);
            assert!(runtime.lifecycle.lock().unwrap().awaited_flush.is_none());
            drop(retained_wrapper);
        }
    }
    #[test]
    fn p45a3_real_group_commit_hard_cap_progress_and_preparation_self_wait_refusal() {
        let dir = tempfile::TempDir::new().unwrap();
        let options = DbOptions { wal_sync_mode: WalSyncMode::GroupCommit { interval_ms: 1, soft_trigger_bytes: 32, hard_cap_bytes: 64 }, ..DbOptions::default() };
        let db = DatabaseEngine::open(dir.path(), &options).unwrap();
        for index in 0..100 {
            db.upsert_node("R", &format!("cap-{index}"), UpsertNodeOptions::default()).unwrap();
        }
        let nested = db.runtime.submit_core_write_with(|| {
            assert!(matches!(db.close_fast(), Err(EngineError::InvalidOperation(message)) if message.contains("self-dependent")));
            assert!(matches!(db.upsert_node("R", "nested", UpsertNodeOptions::default()), Err(EngineError::InvalidOperation(message)) if message.contains("self-dependent")));
            CoreWriteRequest::UpsertNode { labels: vec!["R".into()], key: "outer".into(), options: UpsertNodeOptions::default() }
        });
        assert!(nested.is_ok());
        db.sync().unwrap();
        let stats = db.runtime.candidate.as_ref().unwrap().stats.clone();
        db.close_fast().unwrap();
        wait_native(&stats);
        let db = DatabaseEngine::open(dir.path(), &options).unwrap();
        assert!(db.get_node_by_key("R", "outer").unwrap().is_some());
        assert!(db.get_node_by_key("R", "nested").unwrap().is_none());
        let stats = db.runtime.candidate.as_ref().unwrap().stats.clone();
        db.close_fast().unwrap();
        wait_native(&stats);
    }
    #[test]
    fn p45a3_real_group_commit_wait_retry_deadline_poison_and_fence() {
        // Stop the timer after an ordinary healthy write. The wait notification
        // observes the actual cap predicate while WAL-state is still locked;
        // acquiring gate -> WAL below therefore exercises its atomic wait release.
        for outcome in ["space", "poison", "sync-fence", "fence", "deadline"] {
            let dir = tempfile::TempDir::new().unwrap();
            let options = DbOptions {
                wal_sync_mode: WalSyncMode::GroupCommit {
                    interval_ms: 1,
                    soft_trigger_bytes: 32,
                    hard_cap_bytes: 64,
                },
                ..DbOptions::default()
            };
            let limits = ConcurrencyOptions {
                admission_timeout_ms: if outcome == "deadline" { 200 } else { 5_000 },
                ..ConcurrencyOptions::default()
            };
            let db =
                DatabaseEngine::open_with_concurrency_options(dir.path(), &options, &limits).unwrap();
            db.ensure_node_label("R").unwrap();
            let (wal, authority) = {
                let mut core = db.runtime.core.lock().unwrap();
                let core = core.as_mut().unwrap();
                let wal = core.wal_state.as_ref().unwrap().clone();
                shutdown_sync_thread(&wal, &mut core.sync_thread).unwrap();
                wal.0.lock().unwrap().shutdown = false;
                (wal, core.manifest_write_lock.clone())
            };
            let mut seed = UpsertNodeOptions::default();
            seed.props
                .insert("bytes".into(), PropValue::Bytes(vec![7; 256]));
            db.upsert_node("R", "seed", seed).unwrap();
            assert!(wal.0.lock().unwrap().buffered_bytes >= 64);
            let (waiting_tx, waiting_rx) = std::sync::mpsc::channel();
            db.runtime
                .core
                .lock()
                .unwrap()
                .as_mut()
                .unwrap()
                .wal_cap_wait_for_test = Some(waiting_tx);
            let (done_tx, done_rx) = std::sync::mpsc::channel();
            let writer = db.clone();
            let writer = std::thread::spawn(move || {
                let result = writer.upsert_node("R", "pending", UpsertNodeOptions::default());
                done_tx.send(result).unwrap();
            });
            waiting_rx
                .recv_timeout(Duration::from_secs(3))
                .expect(outcome);
            let mut sync_thread = None;
            let mut timed_result = None;
            match outcome {
                "space" | "fence" => {
                    let gate = authority.gate().unwrap();
                    let mut state = wal.0.lock().unwrap();
                    crate::wal_sync::sync_pending(&mut state).unwrap();
                    if outcome == "fence" {
                        gate.fence();
                    }
                    wal.1.notify_all();
                }
                "poison" => {
                    let mut state = wal.0.lock().unwrap();
                    state.poisoned = Some("injected cap-wait sync failure".into());
                    wal.1.notify_all();
                }
                "sync-fence" => {
                    wal.0
                        .lock()
                        .unwrap()
                        .wal_writer
                        .inject_fault(crate::wal::WalFault::SyncBeforeFlush);
                    let wal = wal.clone();
                    let authority = authority.clone();
                    sync_thread = Some(std::thread::spawn(move || {
                        durability::sync_thread_loop_fenced(wal, Duration::from_millis(1), authority);
                    }));
                }
                "deadline" => {
                    // Keep sending spurious wakes beyond the original timeout.
                    // Resetting the deadline on a wake would prevent completion.
                    let until = Instant::now() + Duration::from_secs(2);
                    loop {
                        if let Ok(result) = done_rx.try_recv() {
                            timed_result = Some(result);
                            break;
                        }
                        if Instant::now() >= until {
                            // Unblock a broken implementation before asserting.
                            wal.0.lock().unwrap().poisoned =
                                Some("test deadline did not expire".into());
                            wal.1.notify_all();
                            break;
                        }
                        wal.1.notify_all();
                        std::thread::sleep(Duration::from_millis(5));
                    }
                }
                _ => unreachable!(),
            }
            let result = timed_result
                .unwrap_or_else(|| done_rx.recv_timeout(Duration::from_secs(3)).expect(outcome));
            writer.join().unwrap();
            if let Some(thread) = sync_thread {
                thread.join().unwrap();
            }
            match outcome {
                "space" => {
                    assert!(result.is_ok(), "{result:?}");
                }
                "poison" | "sync-fence" => {
                    assert!(
                        matches!(result, Err(EngineError::WalSyncFailed(_))),
                        "{result:?}"
                    );
                }
                "fence" => {
                    assert!(
                        matches!(result, Err(EngineError::InvalidOperation(_))),
                        "{result:?}"
                    );
                }
                "deadline" => {
                    assert!(
                        matches!(
                            result,
                            Err(EngineError::ResourceLimit {
                                resource: "admission_deadline",
                                ..
                            })
                        ),
                        "{result:?}"
                    );
                }
                _ => unreachable!(),
            }
            assert_eq!(
                authority.fenced(),
                matches!(outcome, "sync-fence" | "fence")
            );
            assert_eq!(
                db.get_node_by_key("R", "pending").unwrap().is_some(),
                outcome == "space"
            );
            assert!(
                !db.runtime
                    .core
                    .lock()
                    .unwrap()
                    .as_ref()
                    .unwrap()
                    .candidate
                    .as_ref()
                    .unwrap()
                    .accepted
            );
            drop(wal);
            drop(authority);
            let stats = db.runtime.candidate.as_ref().unwrap().stats.clone();
            let _ = db.close_fast();
            wait_native(&stats);
            drop(db);
            let reopened = DatabaseEngine::open(dir.path(), &options).unwrap();
            assert!(reopened.get_node_by_key("R", "seed").unwrap().is_some());
            assert_eq!(
                reopened.get_node_by_key("R", "pending").unwrap().is_some(),
                outcome == "space"
            );
            let stats = reopened.runtime.candidate.as_ref().unwrap().stats.clone();
            reopened.close_fast().unwrap();
            wait_native(&stats);
        }
    }
    #[test]
    fn p45a3_real_fenced_flush_publisher_settles_flush_and_compact_barriers() {
        use crate::persistence_fault::Boundary;
        for compact in [false, true] {
            for boundary in [Boundary::TmpToCurrent, Boundary::DirSync] {
                for close in 0..3 {
                    let dir = tempfile::TempDir::new().unwrap();
                    let options = DbOptions {
                        memtable_flush_threshold: 0,
                        compact_after_n_flushes: 0,
                        ..DbOptions::default()
                    };
                    let db = DatabaseEngine::open(dir.path(), &options).unwrap();
                    let id = db
                        .upsert_node("R", "retained", UpsertNodeOptions::default())
                        .unwrap();
                    db.sync().unwrap();
                    let authority = db
                        .runtime
                        .with_core_ref(|core| Ok(core.manifest_write_lock.clone()))
                        .unwrap();
                    let (ready, release) = db.set_flush_publish_pause();
                    let worker = db.clone();
                    let (tx, rx) = std::sync::mpsc::channel();
                    let handle = std::thread::spawn(move || {
                        let result = if compact {
                            worker.compact().map(|_| ())
                        } else {
                            worker.flush().map(|_| ())
                        };
                        tx.send(result).unwrap();
                    });
                    ready.recv_timeout(Duration::from_secs(5)).unwrap();
                    let wal = crate::wal::wal_generation_path(dir.path(), 0);
                    assert!(wal.exists());
                    assert_eq!(db.get_node(id).unwrap().unwrap().key, "retained");
                    authority.inject_fault(boundary);
                    release.send(()).unwrap();
                    let result = rx
                        .recv_timeout(Duration::from_secs(5))
                        .expect("fenced maintenance barrier must settle");
                    assert!(
                        matches!(
                            result,
                            Err(EngineError::CommitOutcome {
                                state: CommitOutcomeState::Indeterminate,
                                ..
                            })
                        ),
                        "{compact}/{boundary:?}/{close}: {result:?}"
                    );
                    handle.join().unwrap();
                    {
                        let core = db.runtime.core.lock().unwrap();
                        let core = core.as_ref().unwrap();
                        assert!(core.manifest_write_lock.fenced());
                        assert!(core.flush_pipeline_error.is_some());
                        assert!(core.bg_flush.is_none());
                        assert!(core.immutable_epochs.iter().all(|e| !e.in_flight));
                        assert!(
                            core.segments.is_empty(),
                            "fenced output must not be adopted"
                        );
                    }
                    assert!(db
                        .runtime
                        .lifecycle
                        .lock()
                        .unwrap()
                        .completed_flushes_by_epoch
                        .is_empty());
                    assert!(wal.exists());
                    let saved = [
                        crate::manifest::MANIFEST_CURRENT,
                        crate::manifest::MANIFEST_TMP,
                        crate::manifest::MANIFEST_PREV,
                    ]
                    .map(|name| std::fs::read(dir.path().join(name)).ok());
                    let saved_entries = directory_children(dir.path());
                    let stats = db.runtime.candidate.as_ref().unwrap().stats.clone();
                    if close != 2 {
                        let closer = db.clone();
                        let (tx, rx) = std::sync::mpsc::channel();
                        let handle = std::thread::spawn(move || {
                            tx.send(if close == 0 {
                                closer.close()
                            } else {
                                closer.close_fast()
                            })
                            .unwrap();
                        });
                        assert!(matches!(
                            rx.recv_timeout(Duration::from_secs(5))
                                .expect("close must drain settled maintenance"),
                            Err(EngineError::CommitOutcome {
                                state: CommitOutcomeState::Indeterminate,
                                ..
                            })
                        ));
                        handle.join().unwrap();
                    }
                    drop(db);
                    wait_native(&stats);
                    assert!(wal.exists());
                    assert_eq!(saved_entries, directory_children(dir.path()));
                    assert_eq!(
                        saved,
                        [
                            crate::manifest::MANIFEST_CURRENT,
                            crate::manifest::MANIFEST_TMP,
                            crate::manifest::MANIFEST_PREV
                        ]
                        .map(|name| std::fs::read(dir.path().join(name)).ok())
                    );
                    let db = DatabaseEngine::open(dir.path(), &options).unwrap();
                    assert_eq!(db.get_node(id).unwrap().unwrap().key, "retained");
                    let stats = db.runtime.candidate.as_ref().unwrap().stats.clone();
                    db.close_fast().unwrap();
                    wait_native(&stats);
                }
            }
        }
    }
    #[test]
    fn p45a3_real_fenced_background_compaction_settles_barrier() {
        use crate::persistence_fault::Boundary;
        for boundary in [Boundary::TmpToCurrent, Boundary::DirSync] {
            let dir = tempfile::TempDir::new().unwrap();
            let options = DbOptions {
                memtable_flush_threshold: 0,
                compact_after_n_flushes: 0,
                ..DbOptions::default()
            };
            let db = DatabaseEngine::open(dir.path(), &options).unwrap();
            let first = db
                .upsert_node("R", "first", UpsertNodeOptions::default())
                .unwrap();
            db.flush().unwrap();
            let second = db
                .upsert_node("R", "second", UpsertNodeOptions::default())
                .unwrap();
            db.flush().unwrap();
            let authority = db
                .runtime
                .with_core_ref(|core| Ok(core.manifest_write_lock.clone()))
                .unwrap();
            let (ready, release) = db.set_bg_compact_pause();
            db.start_bg_compact().unwrap();
            ready.recv_timeout(Duration::from_secs(5)).unwrap();
            authority.inject_fault(boundary);
            let worker = db.clone();
            let (tx, rx) = std::sync::mpsc::channel();
            let handle = std::thread::spawn(move || {
                tx.send(worker.compact()).unwrap();
            });
            let until = Instant::now() + Duration::from_secs(5);
            while !db
                .runtime
                .lifecycle
                .lock()
                .unwrap()
                .coordinator_active_command
            {
                assert!(Instant::now() < until, "compact barrier was not admitted");
                std::thread::sleep(Duration::from_millis(1));
            }
            release.send(()).unwrap();
            assert!(matches!(
                rx.recv_timeout(Duration::from_secs(5))
                    .expect("fenced compact barrier must settle"),
                Err(EngineError::CommitOutcome {
                    state: CommitOutcomeState::Indeterminate,
                    ..
                })
            ));
            handle.join().unwrap();
            {
                let core = db.runtime.core.lock().unwrap();
                let core = core.as_ref().unwrap();
                assert!(core.manifest_write_lock.fenced());
                assert!(core.bg_compact.is_none());
                assert!(core.candidate.as_ref().unwrap().maintenance_error.is_some());
                assert!(!core
                    .candidate
                    .as_ref()
                    .unwrap()
                    .heavy
                    .load(Ordering::Acquire));
                assert_eq!(
                    core.segments.len(),
                    2,
                    "fenced output must not replace inputs"
                );
            }
            assert!(db
                .runtime
                .lifecycle
                .lock()
                .unwrap()
                .completed_bg_compactions
                .is_empty());
            let saved = [
                crate::manifest::MANIFEST_CURRENT,
                crate::manifest::MANIFEST_TMP,
                crate::manifest::MANIFEST_PREV,
            ]
            .map(|name| std::fs::read(dir.path().join(name)).ok());
            let before_close = directory_children(dir.path());
            let stats = db.runtime.candidate.as_ref().unwrap().stats.clone();
            assert!(matches!(
                db.close_fast(),
                Err(EngineError::CommitOutcome {
                    state: CommitOutcomeState::Indeterminate,
                    ..
                })
            ));
            drop(db);
            wait_native(&stats);
            assert_eq!(before_close, directory_children(dir.path()));
            assert_eq!(
                saved,
                [
                    crate::manifest::MANIFEST_CURRENT,
                    crate::manifest::MANIFEST_TMP,
                    crate::manifest::MANIFEST_PREV
                ]
                .map(|name| std::fs::read(dir.path().join(name)).ok())
            );
            let db = DatabaseEngine::open(dir.path(), &options).unwrap();
            assert_eq!(db.get_node(first).unwrap().unwrap().key, "first");
            assert_eq!(db.get_node(second).unwrap().unwrap().key, "second");
            let stats = db.runtime.candidate.as_ref().unwrap().stats.clone();
            db.close_fast().unwrap();
            wait_native(&stats);
        }
    }
    #[test]
    fn p45a3_real_relative_directory_creation_and_reopen() {
        // Isolate cwd in a child process: parallel tests never observe a cwd change.
        if std::env::var_os("OVERGRAPH_RELATIVE_DIRECTORY_CHILD").is_none() {
            let dir = tempfile::TempDir::new().unwrap();
            let output = std::process::Command::new(std::env::current_exe().unwrap())
                .current_dir(dir.path())
                .env("OVERGRAPH_RELATIVE_DIRECTORY_CHILD", "1")
                .args([
                    "--exact",
                    "engine::real_route_tests::p45a3_real_relative_directory_creation_and_reopen",
                    "--nocapture",
                ])
                .output()
                .unwrap();
            assert!(
                output.status.success(),
                "{}\n{}",
                String::from_utf8_lossy(&output.stdout),
                String::from_utf8_lossy(&output.stderr)
            );
            return;
        }
        for name in ["new_db", "missing/nested/new_db"] {
            let path = Path::new(name);
            assert!(!path.exists());
            let db = DatabaseEngine::open(path, &DbOptions::default()).unwrap();
            let id = db
                .upsert_node("R", "relative", UpsertNodeOptions::default())
                .unwrap();
            let stats = db.runtime.candidate.as_ref().unwrap().stats.clone();
            db.close_fast().unwrap();
            wait_native(&stats);
            let db = DatabaseEngine::open(path, &DbOptions::default()).unwrap();
            assert_eq!(db.get_node(id).unwrap().unwrap().key, "relative");
            let stats = db.runtime.candidate.as_ref().unwrap().stats.clone();
            db.close_fast().unwrap();
            wait_native(&stats);
        }
    }

    #[test]
    fn p45a3_real_manifest_fault_boundaries_all_close_modes_preserve_authority() {
        use crate::persistence_fault::Boundary;
        for boundary in [
            Boundary::BeforeTmp,
            Boundary::TmpCreate,
            Boundary::TmpWrite,
            Boundary::TmpSync,
            Boundary::PrevRemove,
            Boundary::CurrentToPrev,
            Boundary::TmpToCurrent,
            Boundary::DirSync,
        ] {
            for close in 0..3 {
                let dir = tempfile::TempDir::new().unwrap();
                let db = DatabaseEngine::open(dir.path(), &DbOptions::default()).unwrap();
                db.upsert_node("R", "prior", UpsertNodeOptions::default())
                    .unwrap();
                db.sync().unwrap();
                db.set_prune_policy(
                    "old",
                    PrunePolicy {
                        max_age_ms: Some(999999999),
                        max_weight: None,
                        label: None,
                    },
                )
                .unwrap();
                db.runtime
                    .with_core_ref(|c| {
                        c.manifest_write_lock.inject_fault(boundary);
                        Ok(())
                    })
                    .unwrap();
                assert!(
                    matches!(
                        db.set_prune_policy(
                            "uncertain",
                            PrunePolicy {
                                max_age_ms: Some(888888888),
                                max_weight: None,
                                label: None
                            }
                        ),
                        Err(EngineError::CommitOutcome {
                            state: CommitOutcomeState::Indeterminate,
                            ..
                        })
                    ),
                    "{boundary:?}"
                );
                assert_eq!(db.list_prune_policies().unwrap().len(), 1);
                assert!(db
                    .upsert_node("R", "later", UpsertNodeOptions::default())
                    .is_err());
                let authority = [
                    crate::manifest::MANIFEST_CURRENT,
                    crate::manifest::MANIFEST_TMP,
                    crate::manifest::MANIFEST_PREV,
                ]
                .map(|name| std::fs::read(dir.path().join(name)).ok());
                let recovered = crate::manifest::load_manifest_readonly(dir.path())
                    .unwrap()
                    .unwrap();
                let stats = db.runtime.candidate.as_ref().unwrap().stats.clone();
                if close == 0 {
                    let _ = db.close();
                } else if close == 1 {
                    let _ = db.close_fast();
                }
                drop(db);
                wait_native(&stats);
                assert_eq!(
                    authority,
                    [
                        crate::manifest::MANIFEST_CURRENT,
                        crate::manifest::MANIFEST_TMP,
                        crate::manifest::MANIFEST_PREV
                    ]
                    .map(|name| std::fs::read(dir.path().join(name)).ok()),
                    "{boundary:?} / {close}"
                );
                let db = DatabaseEngine::open(dir.path(), &DbOptions::default()).unwrap();
                assert_eq!(
                    db.list_prune_policies().unwrap().len(),
                    recovered.prune_policies.len()
                );
                assert!(db.get_node_by_key("R", "prior").unwrap().is_some());
                assert!(db.get_node_by_key("R", "later").unwrap().is_none());
                let stats = db.runtime.candidate.as_ref().unwrap().stats.clone();
                db.close_fast().unwrap();
                wait_native(&stats);
            }
        }
    }
    #[test]
    fn p45a3_real_queue_full_preparation_fifo_and_independent_close_lane() {
        let dir = tempfile::TempDir::new().unwrap();
        let db = DatabaseEngine::open(dir.path(), &DbOptions::default()).unwrap();
        db.ensure_node_label("R").unwrap();
        // All eight retained positions below must belong to these eight workers.
        let until = Instant::now() + Duration::from_secs(5);
        while db.runtime.candidate.as_ref().unwrap().pressure.occupied() != 0 {
            assert!(Instant::now() < until, "setup admission did not retire");
            std::thread::yield_now();
        }
        let stats = db.runtime.candidate.as_ref().unwrap().stats.clone();
        let release = Arc::new(std::sync::Barrier::new(9));
        let mut workers = Vec::new();
        for index in 0..8 {
            let worker = db.clone();
            let release = release.clone();
            let (tx, rx) = std::sync::mpsc::channel();
            workers.push(std::thread::spawn(move || {
                worker.runtime.submit_core_write_with(|| {
                    tx.send(()).unwrap();
                    release.wait();
                    CoreWriteRequest::UpsertNode {
                        labels: vec!["R".into()],
                        key: format!("queued-{index}"),
                        options: UpsertNodeOptions::default(),
                    }
                })
            }));
            rx.recv_timeout(Duration::from_secs(10)).unwrap();
        }
        let inspected = AtomicBool::new(false);
        let ninth = db.runtime.submit_core_write_with(|| {
            inspected.store(true, Ordering::Release);
            CoreWriteRequest::Sync
        });
        assert!(matches!(
            ninth,
            Err(EngineError::ResourceLimit {
                resource: "request_queue",
                retryable: true,
                ..
            })
        ));
        assert!(!inspected.load(Ordering::Acquire));
        let closer = db.clone();
        let closing = std::thread::spawn(move || closer.close_fast());
        let until = Instant::now() + Duration::from_secs(10);
        while db.runtime.open_state() != RuntimeOpenState::Closing {
            assert!(Instant::now() < until);
            std::thread::yield_now();
        }
        release.wait();
        for (index, worker) in workers.into_iter().enumerate() {
            assert!(
                matches!(worker.join().unwrap().unwrap(), CoreWriteReply::U64(id) if id == index as u64 + 1)
            );
        }
        closing.join().unwrap().unwrap();
        wait_native(&stats);
        let reopened = DatabaseEngine::open(dir.path(), &DbOptions::default()).unwrap();
        for id in 1..=8 {
            assert!(reopened.get_node(id).unwrap().is_some());
        }
        let stats = reopened.runtime.candidate.as_ref().unwrap().stats.clone();
        reopened.close_fast().unwrap();
        wait_native(&stats);
    }
    #[test]
    fn p45a3_real_wal_uncertainty_fences_publication_all_close_modes_and_reopens() {
        for (fault, complete) in [
            (crate::wal::WalFault::AppendPrefix(3), false),
            (crate::wal::WalFault::SyncBeforeFlush, true),
            (crate::wal::WalFault::SyncAfterFlush, true),
            (crate::wal::WalFault::SyncAfterFsync, true),
        ] {
            for mode in 0..3 {
                let dir = tempfile::TempDir::new().unwrap();
                let options = DbOptions {
                    wal_sync_mode: WalSyncMode::Immediate,
                    ..DbOptions::default()
                };
                let db = DatabaseEngine::open(dir.path(), &options).unwrap();
                db.ensure_node_label("R").unwrap();
                let stats = db.runtime.candidate.as_ref().unwrap().stats.clone();
                db.runtime
                    .core
                    .lock()
                    .unwrap()
                    .as_mut()
                    .unwrap()
                    .wal_writer_immediate
                    .as_mut()
                    .unwrap()
                    .inject_fault(fault);
                let result = db.upsert_node(&["R"], "uncertain", UpsertNodeOptions::default());
                assert!(matches!(
                    result,
                    Err(EngineError::CommitOutcome {
                        state: CommitOutcomeState::Indeterminate,
                        ..
                    })
                ));
                assert!(db.get_node(1).unwrap().is_none());
                assert!(db
                    .upsert_node(&["R"], "later", UpsertNodeOptions::default())
                    .is_err());
                match mode {
                    0 => {
                        assert!(db.close().is_err());
                    }
                    1 => {
                        assert!(db.close_fast().is_err());
                    }
                    _ => {
                        drop(db);
                    }
                }
                wait_native(&stats);
                let reopened = DatabaseEngine::open(dir.path(), &options).unwrap();
                assert_eq!(reopened.get_node(1).unwrap().is_some(), complete);
                assert!(reopened.get_node(2).unwrap().is_none());
                let stats = reopened.runtime.candidate.as_ref().unwrap().stats.clone();
                reopened.close_fast().unwrap();
                wait_native(&stats);
            }
        }
    }
    #[test]
    fn p45a3_real_live_release_attribution_and_final_retired_release_wake() {
        let dir = tempfile::TempDir::new().unwrap();
        let db = DatabaseEngine::open(dir.path(), &DbOptions::default()).unwrap();
        db.ensure_node_label("R").unwrap();
        let until = Instant::now() + Duration::from_secs(10);
        loop {
            let sampled = db.reclamation_stats().unwrap();
            if sampled.retired_objects == 0 && sampled.dropping_objects == 0 {
                break;
            }
            assert!(Instant::now() < until);
            std::thread::sleep(Duration::from_millis(1));
        }
        let stats = db.runtime.candidate.as_ref().unwrap().stats.clone();
        stats.reset_arc_read_observer();
        for i in 0..1000 {
            db.upsert_node(&["R"], &format!("probe-{i}"), UpsertNodeOptions::default())
                .unwrap();
        }
        let (live, notified) = stats.arc_read_observations();
        eprintln!(
            "p45a3_attribution writes=1000 live_releases={live} retired_notifications={notified}"
        );
        assert!(
            live >= 5000,
            "publication releases did not use actual shared bridge"
        );
        assert_eq!(notified, 0, "live current graphs woke the reclaimer");
        let (_, snapshot) = db.runtime.published_snapshot().unwrap();
        db.close_fast().unwrap();
        assert!(stats.read().occupied > 0);
        drop(snapshot);
        wait_native(&stats);
        assert!(stats.arc_read_observations().1 > 0);
    }
    #[test]
    fn p45a3_real_compaction_and_reopen_keep_pinned_reader_files() {
        let dir = tempfile::TempDir::new().unwrap();
        let options = DbOptions {
            memtable_flush_threshold: 0,
            compact_after_n_flushes: 0,
            ..DbOptions::default()
        };
        let db = DatabaseEngine::open(dir.path(), &options).unwrap();
        for i in 0..2 {
            db.upsert_node(
                &["R"],
                &format!("persist-{i}"),
                UpsertNodeOptions::default(),
            )
            .unwrap();
            db.flush().unwrap();
        }
        let (_guard, held) = db.runtime.published_snapshot().unwrap();
        drop(_guard);
        let directories: Vec<_> = held
            .view
            .segments
            .iter()
            .map(|r| segment_dir(dir.path(), r.segment_id))
            .collect();
        assert_eq!(directories.len(), 2);
        db.compact().unwrap().unwrap();
        assert!(directories.iter().all(|p| p.exists()));
        let stats = db.runtime.candidate.as_ref().unwrap().stats.clone();
        db.close_fast().unwrap();
        let until = Instant::now() + Duration::from_secs(10);
        let reopened = loop {
            match DatabaseEngine::open(dir.path(), &options) {
                Ok(db) => break db,
                Err(EngineError::InvalidOperation(message)) if message.contains("Busy") => {
                    assert!(Instant::now() < until);
                    std::thread::sleep(Duration::from_millis(1));
                }
                Err(error) => panic!("reopen: {error}"),
            }
        };
        assert!(directories.iter().all(|p| p.exists()));
        assert!(held.view.get_node(1).unwrap().is_some());
        reopened.close_fast().unwrap();
        drop(held);
        wait_native(&stats);
        let cleanup = DatabaseEngine::open(dir.path(), &options).unwrap();
        assert!(directories.iter().all(|p| !p.exists()));
        let stats = cleanup.runtime.candidate.as_ref().unwrap().stats.clone();
        cleanup.close_fast().unwrap();
        wait_native(&stats);
    }
    #[test]
    fn p45a3_real_oversized_and_closed_deep_inputs_clean_up_locally() {
        let dir = tempfile::TempDir::new().unwrap();
        let options = ConcurrencyOptions::default().with_max_batch_bytes(128);
        let db = DatabaseEngine::open_with_concurrency_options(
            dir.path(),
            &DbOptions::default(),
            &options,
        )
        .unwrap();
        db.ensure_node_label("R").unwrap();
        db.sync().unwrap();
        let sequence = db.runtime.with_core_ref(|c| Ok(c.engine_seq)).unwrap();
        let props = BTreeMap::from([("large".into(), PropValue::Bytes(vec![1; 8192]))]);
        assert!(matches!(
            db.upsert_node(
                &["R"],
                "refused",
                UpsertNodeOptions {
                    props,
                    ..UpsertNodeOptions::default()
                }
            ),
            Err(EngineError::ResourceLimit {
                resource: "max_batch_bytes",
                retryable: false,
                ..
            })
        ));
        assert_eq!(
            db.runtime.with_core_ref(|c| Ok(c.engine_seq)).unwrap(),
            sequence
        );
        let stats = db.runtime.candidate.as_ref().unwrap().stats.clone();
        db.close_fast().unwrap();
        wait_native(&stats);
        let mut deep = PropValue::Null;
        for _ in 0..10000 {
            deep = PropValue::Array(vec![deep]);
        }
        let props = BTreeMap::from([("deep".into(), deep)]);
        std::thread::Builder::new()
            .stack_size(64 * 1024)
            .spawn(move || {
                assert!(db
                    .upsert_node(
                        &["R"],
                        "closed",
                        UpsertNodeOptions {
                            props,
                            ..UpsertNodeOptions::default()
                        }
                    )
                    .is_err());
            })
            .unwrap()
            .join()
            .unwrap();
    }
    #[test]
    fn p45a3_real_transaction_and_graph_patch_rejected_deep_inputs_are_stack_safe() {
        let dir = tempfile::TempDir::new().unwrap();
        let db = DatabaseEngine::open(dir.path(), &DbOptions::default()).unwrap();
        let mut txn = db.begin_write_txn().unwrap();
        fn deep() -> BTreeMap<String, PropValue> {
            let mut value = PropValue::Null;
            for _ in 0..10000 { value = PropValue::Array(vec![value]); }
            BTreeMap::from([("deep".into(), value)])
        }
        let intents = vec![TxnIntent::UpsertNode { alias: None, labels: vec!["R".into()], key: "no-partial-overlay".into(), options: UpsertNodeOptions::default() }, TxnIntent::UpsertNode { alias: None, labels: vec!["R".into()], key: "invalid".into(), options: UpsertNodeOptions { props: deep(), ..UpsertNodeOptions::default() } }];
        assert!(txn.stage_intents(intents).is_err());
        assert!(txn.get_node_by_key("R", "no-partial-overlay").unwrap().is_none());
        txn.rollback().unwrap();
        db.close_fast().unwrap();
        std::thread::Builder::new().stack_size(64 * 1024).spawn(move || {
            assert!(txn.upsert_node("R", "closed", UpsertNodeOptions { props: deep(), ..UpsertNodeOptions::default() }).is_err());
            assert!(txn.stage_intents(vec![TxnIntent::UpsertNode { alias: None, labels: vec!["R".into()], key: "closed".into(), options: UpsertNodeOptions { props: deep(), ..UpsertNodeOptions::default() } }]).is_err());
            assert!(db.graph_patch(GraphPatch { upsert_nodes: vec![NodeInput { labels: vec!["R".into()], key: "closed".into(), props: deep(), weight: 1.0, dense_vector: None, sparse_vector: None }], ..GraphPatch::default() }).is_err());
        }).unwrap().join().unwrap();
    }
    #[test]
    fn p45a3_real_generation_open_error_preserves_authoritative_manifest() {
        let dir = tempfile::TempDir::new().unwrap();
        let db = DatabaseEngine::open(dir.path(), &DbOptions::default()).unwrap();
        db.upsert_node(&["R"], "prior", UpsertNodeOptions::default())
            .unwrap();
        db.runtime
            .core
            .lock()
            .unwrap()
            .as_mut()
            .unwrap()
            .force_next_wal_open_error = true;
        assert!(matches!(
            db.flush(),
            Err(EngineError::CommitOutcome {
                state: CommitOutcomeState::Indeterminate,
                ..
            })
        ));
        let authority = std::fs::read(dir.path().join(crate::manifest::MANIFEST_CURRENT)).unwrap();
        assert!(db
            .upsert_node(&["R"], "later", UpsertNodeOptions::default())
            .is_err());
        let stats = db.runtime.candidate.as_ref().unwrap().stats.clone();
        assert!(db.close().is_err());
        wait_native(&stats);
        assert_eq!(
            std::fs::read(dir.path().join(crate::manifest::MANIFEST_CURRENT)).unwrap(),
            authority
        );
        let reopened = DatabaseEngine::open(dir.path(), &DbOptions::default()).unwrap();
        assert!(reopened.get_node(1).unwrap().is_some());
        assert!(reopened.get_node(2).unwrap().is_none());
        let stats = reopened.runtime.candidate.as_ref().unwrap().stats.clone();
        reopened.close_fast().unwrap();
        wait_native(&stats);
    }
    #[test]
    fn p45a3_real_wal_only_generation_rotation_and_metadata_publication_failure() {
        let dir = tempfile::TempDir::new().unwrap();
        // The one-operation generation boundary is semantic; setup fsync work
        // gets the functional allowance before the publication fault is armed.
        let limits = ConcurrencyOptions::default().with_max_batch_operations(1).with_generation_operation_limit(1)
            .with_admission_timeout_ms(tests::FUNCTIONAL_MAINTENANCE_ADMISSION_TIMEOUT_MS);
        let db = DatabaseEngine::open_with_concurrency_options(dir.path(), &DbOptions::default(), &limits).unwrap();
        db.ensure_node_label("first").unwrap();
        db.ensure_node_label("second").unwrap();
        db.runtime.with_core_ref(|c| {
            assert!(c.active_wal_generation_id > 0);
            assert_eq!(c.candidate.as_ref().unwrap().generation_operations, 1);
            Ok(())
        }).unwrap();
        db.runtime.force_publication_error.store(true, Ordering::Release);
        assert!(matches!(db.set_prune_policy("accepted", PrunePolicy { max_age_ms: Some(10000000), max_weight: None, label: None }), Err(EngineError::CommitOutcome { state: CommitOutcomeState::Indeterminate, .. })));
        assert!(db.list_prune_policies().unwrap().is_empty());
        let stats = db.runtime.candidate.as_ref().unwrap().stats.clone();
        let _ = db.close_fast();
        wait_native(&stats);
        let db = DatabaseEngine::open(dir.path(), &DbOptions::default()).unwrap();
        assert_eq!(db.list_node_labels().unwrap().len(), 2);
        assert_eq!(db.list_prune_policies().unwrap().len(), 1);
        let stats = db.runtime.candidate.as_ref().unwrap().stats.clone();
        db.close_fast().unwrap();
        wait_native(&stats);
    }
    #[test]
    fn p45a3_real_post_wal_publication_error_keeps_old_catalog_and_recovers() {
        let dir = tempfile::TempDir::new().unwrap();
        let db = DatabaseEngine::open(dir.path(), &DbOptions::default()).unwrap();
        db.runtime
            .force_publication_error
            .store(true, Ordering::Release);
        assert!(matches!(
            db.upsert_node(&["New"], "accepted", UpsertNodeOptions::default()),
            Err(EngineError::CommitOutcome {
                state: CommitOutcomeState::Indeterminate,
                ..
            })
        ));
        assert!(db.get_node(1).unwrap().is_none());
        assert!(db.get_node_label_id("New").unwrap().is_none());
        assert!(db.list_node_labels().unwrap().is_empty());
        let stats = db.runtime.candidate.as_ref().unwrap().stats.clone();
        assert!(db.close_fast().is_err());
        wait_native(&stats);
        let reopened = DatabaseEngine::open(dir.path(), &DbOptions::default()).unwrap();
        assert!(reopened.get_node(1).unwrap().is_some());
        assert!(reopened.get_node_label_id("New").unwrap().is_some());
        let stats = reopened.runtime.candidate.as_ref().unwrap().stats.clone();
        reopened.close_fast().unwrap();
        wait_native(&stats);
    }
    #[test]
    fn p45a3_real_close_refuses_its_own_mutating_barrier() {
        let dir = tempfile::TempDir::new().unwrap();
        let db = DatabaseEngine::open(dir.path(), &DbOptions::default()).unwrap();
        let barrier = db.runtime.begin_mutating_barrier().unwrap();
        assert!(
            matches!(db.close_fast(), Err(EngineError::InvalidOperation(message)) if message.contains("self-dependent"))
        );
        drop(barrier);
        let stats = db.runtime.candidate.as_ref().unwrap().stats.clone();
        db.close_fast().unwrap();
        wait_native(&stats);
    }
    #[test]
    fn p45c2_fixed_ready_catalog_crud_and_online_drop_recreate() {
        let dir = tempfile::TempDir::new().unwrap();
        let options = DbOptions {
            memtable_flush_threshold: 0,
            compact_after_n_flushes: 0,
            ..DbOptions::default()
        };
        // Exact legacy constructor is private fixture preparation only. The
        // public candidate never receives a reader-count/DDL bypass.
        let core = EngineCore::open(dir.path(), &options).unwrap();
        let runtime = Arc::new(DbRuntime::new(dir.path().to_owned(), core));
        runtime.install_core_runtime_handle();
        let fixture = DatabaseEngine {
            runtime,
            client: None,
        };
        let red = PropValue::String("red".into());
        fixture
            .upsert_node(
                &["R"],
                "seed",
                UpsertNodeOptions {
                    props: BTreeMap::from([("color".into(), red.clone())]),
                    ..UpsertNodeOptions::default()
                },
            )
            .unwrap();
        fixture.flush().unwrap();
        let spec = SecondaryIndexSpec {
            fields: vec![SecondaryIndexField::Property {
                key: "color".into(),
            }],
            kind: SecondaryIndexKind::Equality,
        };
        let info = fixture
            .ensure_node_property_index("R", spec.clone())
            .unwrap();
        let until = Instant::now() + Duration::from_secs(10);
        loop {
            if fixture
                .list_node_property_indexes()
                .unwrap()
                .iter()
                .any(|entry| {
                    entry.index_id == info.index_id && entry.state == SecondaryIndexState::Ready
                })
            {
                break;
            }
            assert!(Instant::now() < until);
            std::thread::sleep(Duration::from_millis(1));
        }
        fixture.close_fast().unwrap();
        drop(fixture);
        let db = DatabaseEngine::open(dir.path(), &options).unwrap();
        assert_eq!(db.find_nodes("R", "color", &red).unwrap(), vec![1]);
        let (_guard, held) = db.runtime.published_snapshot().unwrap();
        drop(_guard);
        let mut held = Some(held);
        for pinned in [true, false] {
            assert_eq!(db.ensure_node_property_index("R", spec.clone()).unwrap().index_id, info.index_id + u64::from(!pinned));
            assert!(db.drop_node_property_index("R", spec.clone()).unwrap());
            assert_eq!(db.find_nodes("R", "color", &red).unwrap(), vec![1]);
            let recreated = db.ensure_node_property_index("R", spec.clone()).unwrap();
            assert!(recreated.index_id > info.index_id);
            let until = Instant::now() + Duration::from_secs(10);
            while db.list_node_property_indexes().unwrap().iter().any(|e| e.state == SecondaryIndexState::Building) { assert!(Instant::now() < until); std::thread::sleep(Duration::from_millis(1)); }
            if pinned { assert!(held.as_ref().unwrap().view.get_node(1).unwrap().is_some()); drop(held.take()); }
        }
        drop(held);
        let id = db
            .upsert_node(
                &["R"],
                "active",
                UpsertNodeOptions {
                    props: BTreeMap::from([("color".into(), red.clone())]),
                    ..UpsertNodeOptions::default()
                },
            )
            .unwrap();
        assert_eq!(db.find_nodes("R", "color", &red).unwrap(), vec![1, id]);
        db.flush().unwrap();
        db.compact().unwrap();
        assert_eq!(db.find_nodes("R", "color", &red).unwrap(), vec![1, id]);
        let stats = db.runtime.candidate.as_ref().unwrap().stats.clone();
        db.close_fast().unwrap();
        wait_native(&stats);
    }
    pub(super) fn fixed_family_props(matches: bool) -> BTreeMap<String, PropValue> {
        let value = if matches { 1 } else { 99 };
        BTreeMap::from([
            ("eq".into(), PropValue::Int(value)),
            ("range".into(), PropValue::Int(value)),
            ("ceq".into(), PropValue::Int(value)),
            ("tie".into(), PropValue::Int(7)),
            ("crange".into(), PropValue::Int(value)),
            ("score".into(), PropValue::Int(8)),
        ])
    }

    pub(super) fn fixed_family_queries(family: usize) -> (NodeQuery, EdgeQuery) {
        let keys = ["eq", "range", "ceq", "crange"];
        let equals = |key: &str| NodeFilterExpr::PropertyEquals {
            key: key.into(),
            value: PropValue::Int(1),
        };
        let filter = match family {
            0 => equals(keys[family]),
            1 => NodeFilterExpr::PropertyRange {
                key: keys[family].into(),
                lower: Some(PropertyRangeBound::Included(PropValue::Int(1))),
                upper: Some(PropertyRangeBound::Included(PropValue::Int(2))),
            },
            2 => NodeFilterExpr::And(vec![
                equals(keys[family]),
                NodeFilterExpr::PropertyEquals {
                    key: "tie".into(),
                    value: PropValue::Int(7),
                },
            ]),
            3 => NodeFilterExpr::And(vec![
                equals(keys[family]),
                NodeFilterExpr::PropertyRange {
                    key: "score".into(),
                    lower: Some(PropertyRangeBound::Included(PropValue::Int(7))),
                    upper: Some(PropertyRangeBound::Included(PropValue::Int(9))),
                },
            ]),
            _ => unreachable!(),
        };
        fn edge_filter(node: &NodeFilterExpr) -> EdgeFilterExpr {
            match node {
                NodeFilterExpr::PropertyEquals { key, value } => EdgeFilterExpr::PropertyEquals {
                    key: key.clone(),
                    value: value.clone(),
                },
                NodeFilterExpr::PropertyRange { key, lower, upper } => EdgeFilterExpr::PropertyRange {
                    key: key.clone(),
                    lower: lower.clone(),
                    upper: upper.clone(),
                },
                NodeFilterExpr::And(filters) => {
                    EdgeFilterExpr::And(filters.iter().map(edge_filter).collect())
                }
                _ => unreachable!(),
            }
        }
        let edge = EdgeQuery {
            label: Some("E".into()),
            filter: Some(edge_filter(&filter)),
            ..EdgeQuery::default()
        };
        let node = NodeQuery {
            label_filter: Some(NodeLabelFilter {
                labels: vec!["R".into()],
                mode: LabelMatchMode::Any,
            }),
            filter: Some(filter),
            ..NodeQuery::default()
        };
        (node, edge)
    }

    fn fixed_plan_has_index(plan: &QueryPlanNode, family: usize, edge: bool, id: u64) -> bool {
        match plan {
            QueryPlanNode::PropertyEqualityIndex => family == 0 && !edge,
            QueryPlanNode::EdgePropertyEqualityIndex => family == 0 && edge,
            QueryPlanNode::PropertyRangeIndex => family == 1 && !edge,
            QueryPlanNode::EdgePropertyRangeIndex => family == 1 && edge,
            QueryPlanNode::CompoundEqualityIndex { details } => family == 2 && details.index_id == id,
            QueryPlanNode::CompoundRangeIndex { details } => family == 3 && details.index_id == id,
            QueryPlanNode::Intersect { inputs, .. } | QueryPlanNode::Union { inputs, .. } => inputs
                .iter()
                .any(|p| fixed_plan_has_index(p, family, edge, id)),
            QueryPlanNode::StreamedSource { input }
            | QueryPlanNode::BufferedIdSort { input }
            | QueryPlanNode::VerifyNodeFilter { input }
            | QueryPlanNode::VerifyEdgeFilter { input }
            | QueryPlanNode::VerifyEdgePredicates { input } => {
                fixed_plan_has_index(input, family, edge, id)
            }
            _ => false,
        }
    }

    fn assert_fixed_family_results(
        db: &DatabaseEngine,
        nodes: &[u64],
        edges: &[u64],
        index_ids: &[(u64, u64)],
        failed: bool,
        assert_plans: bool,
    ) {
        for (family, &(node_index, edge_index)) in index_ids.iter().enumerate() {
            let (mut nq, mut eq) = fixed_family_queries(family);
            assert_eq!(
                db.query_node_ids(&nq).unwrap().items,
                nodes,
                "node family {family}"
            );
            assert_eq!(
                db.query_edge_ids(&eq).unwrap().edge_ids,
                edges,
                "edge family {family}"
            );
            if assert_plans {
                let np = db.explain_node_query(&nq).unwrap();
                let ep = db.explain_edge_query(&eq).unwrap();
                assert_eq!(
                    fixed_plan_has_index(&np.root, family, false, node_index),
                    !failed,
                    "node family {family}: {:?}",
                    np.root
                );
                assert_eq!(
                    fixed_plan_has_index(&ep.root, family, true, edge_index),
                    !failed,
                    "edge family {family}: {:?}",
                    ep.root
                );
            }
            nq.page.limit = Some(1);
            eq.page.limit = Some(1);
            let mut paged_nodes = Vec::new();
            let mut paged_edges = Vec::new();
            loop {
                let page = db.query_node_ids(&nq).unwrap();
                if let Some(cursor) = page.next_cursor {
                    assert!(nq.page.after.is_none_or(|previous| cursor > previous));
                }
                paged_nodes.extend(page.items);
                if page.next_cursor.is_none() {
                    break;
                }
                nq.page.after = page.next_cursor;
            }
            loop {
                let page = db.query_edge_ids(&eq).unwrap();
                if let Some(cursor) = page.next_cursor {
                    assert!(eq.page.after.is_none_or(|previous| cursor > previous));
                }
                paged_edges.extend(page.edge_ids);
                if page.next_cursor.is_none() {
                    break;
                }
                eq.page.after = page.next_cursor;
            }
            assert_eq!(paged_nodes, nodes, "node cursor family {family}");
            assert_eq!(paged_edges, edges, "edge cursor family {family}");
        }
    }

    fn assert_fixed_cursor_tails(
        db: &DatabaseEngine,
        cursors: &[(u64, u64)],
        nodes: &[u64],
        edges: &[u64],
    ) {
        for (family, &(node_cursor, edge_cursor)) in cursors.iter().enumerate() {
            let (mut nq, mut eq) = fixed_family_queries(family);
            nq.page.after = Some(node_cursor);
            eq.page.after = Some(edge_cursor);
            assert_eq!(
                db.query_node_ids(&nq).unwrap().items,
                &nodes[1..],
                "carried node cursor family {family}"
            );
            assert_eq!(
                db.query_edge_ids(&eq).unwrap().edge_ids,
                &edges[1..],
                "carried edge cursor family {family}"
            );
        }
    }

    #[test]
    fn p45a3_real_all_eight_fixed_ready_and_failed_families_round_trip() {
        for failed in [false, true] {
            let dir = tempfile::TempDir::new().unwrap();
            let options = DbOptions {
                compact_after_n_flushes: 0,
                memtable_flush_threshold: 0,
                ..DbOptions::default()
            };
            // DDL is deliberately a legacy fixture operation; every oracle and
            // mutation below the first reopen uses the actual candidate route.
            let core = EngineCore::open(dir.path(), &options).unwrap();
            let runtime = Arc::new(DbRuntime::new(dir.path().to_owned(), core));
            runtime.install_core_runtime_handle();
            let fixture = DatabaseEngine {
                runtime,
                client: None,
            };
            let ids = fixture
                .batch_upsert_nodes(
                    (0..32)
                        .map(|i| NodeInput {
                            labels: vec!["R".into()],
                            key: format!("row-{i}"),
                            props: fixed_family_props(i < 4),
                            weight: 1.0,
                            dense_vector: None,
                            sparse_vector: None,
                        })
                        .collect(),
                )
                .unwrap();
            let edge_ids = fixture
                .batch_upsert_edges(
                    ids.iter()
                        .enumerate()
                        .map(|(i, &id)| EdgeInput {
                            from: id,
                            to: id,
                            label: "E".into(),
                            props: fixed_family_props(i < 4),
                            weight: 1.0,
                            valid_from: None,
                            valid_to: None,
                        })
                        .collect(),
                )
                .unwrap();
            fixture.flush().unwrap();
            let mut index_ids = Vec::new();
            for (kind, keys) in [
                (SecondaryIndexKind::Equality, vec!["eq"]),
                (SecondaryIndexKind::Range, vec!["range"]),
                (SecondaryIndexKind::Equality, vec!["ceq", "tie"]),
                (SecondaryIndexKind::Range, vec!["crange", "score"]),
            ] {
                let spec = SecondaryIndexSpec {
                    fields: keys
                        .into_iter()
                        .map(SecondaryIndexField::property)
                        .collect(),
                    kind,
                };
                let node = fixture
                    .ensure_node_property_index("R", spec.clone())
                    .unwrap();
                let edge = fixture.ensure_edge_property_index("E", spec).unwrap();
                index_ids.push((node.index_id, edge.index_id));
            }
            let until = Instant::now() + Duration::from_secs(10);
            loop {
                let ready = fixture
                    .runtime
                    .with_core_ref(|core| {
                        Ok(core
                            .secondary_index_entries_snapshot()
                            .iter()
                            .all(|entry| entry.state == SecondaryIndexState::Ready))
                    })
                    .unwrap();
                if ready {
                    break;
                }
                assert!(Instant::now() < until);
                std::thread::sleep(Duration::from_millis(1));
            }
            if failed {
                fixture
                    .with_core_mut(|core| {
                        core.with_runtime_manifest_write(|manifest| {
                            for entry in &mut manifest.secondary_indexes {
                                entry.state = SecondaryIndexState::Failed;
                                entry.last_error = Some("fixed fixture failure".into());
                            }
                            Ok(())
                        })?;
                        core.rebuild_secondary_index_catalog()
                    })
                    .unwrap();
            }
            fixture.close_fast().unwrap();
            drop(fixture);
            let db = DatabaseEngine::open(dir.path(), &options).unwrap();
            let before = db
                .runtime
                .with_core_ref(|c| Ok(c.manifest.secondary_indexes.clone()))
                .unwrap();
            assert_eq!(before.len(), 8);
            assert!(before.iter().all(|e| e.state
                == if failed {
                    SecondaryIndexState::Failed
                } else {
                    SecondaryIndexState::Ready
                }));
            assert_fixed_family_results(&db, &ids[..4], &edge_ids[..4], &index_ids, failed, true);
            let old = db.runtime.published.load_full();
            // Newer property opinions and tombstones must shadow old postings.
            db.upsert_node(
                "R",
                "row-0",
                UpsertNodeOptions {
                    props: fixed_family_props(false),
                    ..UpsertNodeOptions::default()
                },
            )
            .unwrap();
            db.delete_edge(edge_ids[0]).unwrap();
            db.delete_node(ids[1]).unwrap(); // Cascades its self edge.
            let mut txn = db.begin_write_txn().unwrap();
            txn.upsert_node(
                "R",
                "txn",
                UpsertNodeOptions {
                    props: fixed_family_props(true),
                    ..UpsertNodeOptions::default()
                },
            )
            .unwrap();
            let (_, commit_view) = txn.commit_with_gql_return_view().unwrap();
            let txn_id = db.get_node_by_key("R", "txn").unwrap().unwrap().id;
            let mut expected_nodes = vec![ids[2], ids[3], txn_id];
            for family in 0..4 {
                let (nq, _) = fixed_family_queries(family);
                assert_eq!(
                    commit_view.query_node_ids_outcome(&nq).unwrap().value.items,
                    expected_nodes
                );
            }
            drop(commit_view);
            let txn_edge = db
                .upsert_edge(
                    txn_id,
                    txn_id,
                    "E",
                    UpsertEdgeOptions {
                        props: fixed_family_props(true),
                        ..UpsertEdgeOptions::default()
                    },
                )
                .unwrap();
            let fresh = db
                .batch_upsert_nodes(
                    (0..2)
                        .map(|i| NodeInput {
                            labels: vec!["R".into()],
                            key: format!("fresh-{i}"),
                            props: fixed_family_props(true),
                            weight: 1.0,
                            dense_vector: None,
                            sparse_vector: None,
                        })
                        .collect(),
                )
                .unwrap();
            let fresh_edges = db
                .batch_upsert_edges(
                    fresh
                        .iter()
                        .map(|&id| EdgeInput {
                            from: id,
                            to: id,
                            label: "E".into(),
                            props: fixed_family_props(true),
                            weight: 1.0,
                            valid_from: None,
                            valid_to: None,
                        })
                        .collect(),
                )
                .unwrap();
            expected_nodes.extend(fresh);
            let mut expected_edges = vec![edge_ids[2], edge_ids[3], txn_edge];
            expected_edges.extend(fresh_edges);
            // Carry the exact returned cursors across source replacement and reopen.
            let cursors = (0..4)
                .map(|family| {
                    let (mut nq, mut eq) = fixed_family_queries(family);
                    nq.page.limit = Some(1);
                    eq.page.limit = Some(1);
                    (
                        db.query_node_ids(&nq).unwrap().next_cursor.unwrap(),
                        db.query_edge_ids(&eq).unwrap().next_cursor.unwrap(),
                    )
                })
                .collect::<Vec<_>>();
            for stage in 0..4 {
                if stage == 1 {
                    db.flush().unwrap();
                }
                if stage == 2 {
                    // Add another source so compaction exercises both index paths.
                    db.upsert_node(
                        "R",
                        "nonmatch",
                        UpsertNodeOptions {
                            props: fixed_family_props(false),
                            ..UpsertNodeOptions::default()
                        },
                    )
                    .unwrap();
                    db.flush().unwrap();
                }
                if stage == 3 {
                    assert!(db.compact().unwrap().is_some());
                }
                assert_fixed_family_results(
                    &db,
                    &expected_nodes,
                    &expected_edges,
                    &index_ids,
                    failed,
                    true,
                );
                assert_fixed_cursor_tails(&db, &cursors, &expected_nodes, &expected_edges);
                for family in 0..4 {
                    let (nq, eq) = fixed_family_queries(family);
                    assert_eq!(
                        old.view.query_node_ids_outcome(&nq).unwrap().value.items,
                        ids[..4]
                    );
                    assert_eq!(
                        old.view.query_edge_ids_outcome(&eq).unwrap().value.edge_ids,
                        edge_ids[..4]
                    );
                }
                assert_eq!(
                    db.runtime
                        .with_core_ref(|c| Ok(c.manifest.secondary_indexes.clone()))
                        .unwrap(),
                    before
                );
            }
            drop(old);
            let stats = db.runtime.candidate.as_ref().unwrap().stats.clone();
            db.close_fast().unwrap();
            wait_native(&stats);
            let db = DatabaseEngine::open(dir.path(), &options).unwrap();
            assert_fixed_family_results(
                &db,
                &expected_nodes,
                &expected_edges,
                &index_ids,
                failed,
                true,
            );
            assert_fixed_cursor_tails(&db, &cursors, &expected_nodes, &expected_edges);
            assert_eq!(
                db.runtime
                    .with_core_ref(|c| Ok(c.manifest.secondary_indexes.clone()))
                    .unwrap(),
                before
            );
            let stats = db.runtime.candidate.as_ref().unwrap().stats.clone();
            db.close_fast().unwrap();
            wait_native(&stats);
        }
    }

    #[test]
    fn p45a3_real_heavy_guardian_destroys_on_reclaimer_after_passive_release() {
        struct Probe(std::sync::mpsc::Sender<String>);
        impl Drop for Probe {
            fn drop(&mut self) {
                self.0
                    .send(std::thread::current().name().unwrap_or("unnamed").into())
                    .unwrap();
            }
        }
        let domain = reclamation::ReclaimDomain::new();
        let (tx, rx) = std::sync::mpsc::channel();
        let mut owner = SharedArc::new(Probe(tx));
        owner
            .guard(&domain, reclamation::ResourceClass::Generation)
            .unwrap();
        let reader = owner.clone();
        drop(owner);
        assert!(rx.try_recv().is_err());
        drop(reader);
        assert!(rx
            .recv_timeout(Duration::from_secs(10))
            .unwrap()
            .contains("reclaim"));
        domain.close();
    }
}

#[cfg(test)]
mod p45d2_tests {
    use super::p45b1_tests::finish;
    use super::tests::{
        p45d2_assert_order_values, p45d2_order_fixture, p45d2_order_mutations, p45d2_segment_orders,
    };
    use super::*;
    use std::time::Duration;

    #[test]
    fn p45d2_candidate_compaction_lease_queues_newer_flush_until_adoption() {
        let dir = tempfile::TempDir::new().unwrap();
        let options = DbOptions {
            memtable_flush_threshold: 0,
            compact_after_n_flushes: 0,
            wal_sync_mode: WalSyncMode::Immediate,
            ..Default::default()
        };
        let db = DatabaseEngine::open(dir.path(), &options).unwrap();
        assert!(db.published_state().view.memtable.is_candidate());
        let ids = p45d2_order_fixture(&db);
        let original_order = p45d2_segment_orders(&db, dir.path()).0;
        let (ready, release) = db.set_bg_compact_pause();
        db.start_bg_compact().unwrap();
        ready.recv_timeout(Duration::from_secs(5)).unwrap();
        p45d2_order_mutations(&db, ids);
        db.freeze_memtable().unwrap();
        db.enqueue_one_flush().unwrap();
        db.with_core_ref(|core| {
            assert!(core
                .candidate
                .as_ref()
                .unwrap()
                .heavy
                .load(Ordering::Acquire));
            assert!(core.bg_compact.is_some());
            assert_eq!(core.immutable_epochs.len(), 1);
            assert!(!core.immutable_epochs[0].in_flight);
            assert_eq!(core.segments.len(), 2);
            Ok(())
        })
        .unwrap();
        assert_eq!(p45d2_segment_orders(&db, dir.path()).0, original_order);
        p45d2_assert_order_values(&db, ids);
        // Never wait for the queued flush while the real heavy lease is held.
        release.send(()).unwrap();
        assert!(db.wait_for_bg_compact().is_some());
        db.flush().unwrap();
        assert_eq!(db.immutable_epoch_count(), 0);
        assert_eq!(db.in_flight_count(), 0);
        p45d2_assert_order_values(&db, ids);
        let (durable, reverse_live) = p45d2_segment_orders(&db, dir.path());
        assert_eq!(durable, reverse_live);
        finish(db);
        let reopened = DatabaseEngine::open(dir.path(), &options).unwrap();
        p45d2_assert_order_values(&reopened, ids);
        let (durable, reverse_live) = p45d2_segment_orders(&reopened, dir.path());
        assert_eq!(durable, reverse_live);
        finish(reopened);
    }
}

struct CandidateHeavyLease {
    #[cfg(test)]
    diagnostic_id: Option<u64>,
    busy: Arc<AtomicBool>,
    runtime: Option<Weak<DbRuntime>>,
}
impl Drop for CandidateHeavyLease {
    fn drop(&mut self) {
        self.busy.store(false, Ordering::Release);
        #[cfg(test)]
        maintenance_diagnostic::stage_id(self.diagnostic_id, "heavy_release");
        if let Some(runtime) = self.runtime.as_ref().and_then(Weak::upgrade) {
            runtime.notify_lifecycle_work();
        }
    }
}
impl EngineCore {
    fn acquire_heavy_job(&self) -> Option<CandidateHeavyLease> {
        let candidate = self.candidate.as_ref()?;
        candidate
            .heavy
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .unwrap_or_else(|_| concurrency_lifecycle::fail_stop("second heavy job"));
        Some(CandidateHeavyLease {
            #[cfg(test)]
            diagnostic_id: maintenance_diagnostic::enqueue(&self.db_dir, self.next_segment_id),
            busy: candidate.heavy.clone(),
            runtime: self.runtime.clone(),
        })
    }
}

struct InputWireBound {
    bytes: usize,
    maximum: usize,
    exceeded: bool,
}
impl std::io::Write for InputWireBound {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        self.bytes = match self.bytes.checked_add(bytes.len()) {
            Some(n) => n,
            None => {
                self.exceeded = true;
                self.bytes = usize::MAX;
                return Err(std::io::Error::other("request wire overflow"));
            }
        };
        if self.bytes > self.maximum {
            self.exceeded = true;
            return Err(std::io::Error::other("request frame limit"));
        }
        Ok(bytes.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}
fn validate_candidate_input(request: &CoreWriteRequest, maximum: usize) -> Result<(), EngineError> {
    // Raw properties bound input before copies; actual final normalized V3 frames
    // still decide admission after implicit token/schema expansion.
    let mut sink = InputWireBound {
        bytes: 0,
        maximum,
        exceeded: false,
    };
    let mut validate = |key: Option<&str>, props: &BTreeMap<String, PropValue>| {
        // A normalized node opinion retains its key once per input operation.
        // Labels are omitted: resolution deduplicates token names across inputs.
        if let Some(key) = key {
            if std::io::Write::write(&mut sink, key.as_bytes()).is_err() {
                return Err(crate::encoding::wal_byte_limit(sink.bytes as u64, maximum));
            }
        }
        payload::write_properties_checked(props, &mut sink).map_err(|e| {
            if sink.exceeded {
                crate::encoding::wal_byte_limit(sink.bytes as u64, maximum)
            } else {
                EngineError::InvalidOperation(format!("invalid candidate properties: {e:?}"))
            }
        })
    };
    match request {
        CoreWriteRequest::UpsertNode { key, options, .. } => validate(Some(key), &options.props),
        CoreWriteRequest::UpsertEdge { options, .. } => validate(None, &options.props),
        CoreWriteRequest::BatchUpsertNodes { inputs } => {
            for input in inputs { validate(Some(&input.key), &input.props)?; }
            Ok(())
        }
        CoreWriteRequest::BatchUpsertEdges { inputs } => {
            for input in inputs { validate(None, &input.props)?; }
            Ok(())
        }
        CoreWriteRequest::GraphPatch { patch } => {
            for n in &patch.upsert_nodes { validate(Some(&n.key), &n.props)?; }
            for e in &patch.upsert_edges { validate(None, &e.props)?; }
            Ok(())
        }
        #[cfg(test)]
        CoreWriteRequest::WriteOp { op } => match op {
            WalOp::UpsertNode(n) => validate(Some(&n.key), &n.props),
            WalOp::UpsertEdge(e) => validate(None, &e.props),
            _ => Ok(()),
        },
        #[cfg(test)]
        CoreWriteRequest::WriteOpBatch { ops } => {
            for op in ops { match op {
                WalOp::UpsertNode(n) => validate(Some(&n.key), &n.props)?,
                WalOp::UpsertEdge(e) => validate(None, &e.props)?,
                _ => {},
            }}
            Ok(())
        }
        _ => Ok(()),
    }
}

#[cfg(test)]
mod p45b1_tests {
    use super::*;
    use std::time::{Duration, Instant};
    pub(super) fn node(id: u64, label: u32, key: &str) -> NodeRecord {
        NodeRecord {
            id,
            label_ids: NodeLabelSet::single(label).unwrap(),
            key: key.into(),
            props: BTreeMap::new(),
            created_at: 1,
            updated_at: 2,
            weight: 1.0,
            dense_vector: None,
            sparse_vector: None,
            last_write_seq: 0,
        }
    }
    pub(super) fn edge(id: u64, from: u64, to: u64, label: u32) -> EdgeRecord {
        EdgeRecord {
            id,
            from,
            to,
            label_id: label,
            props: BTreeMap::new(),
            created_at: 1,
            updated_at: 2,
            weight: 1.0,
            valid_from: 0,
            valid_to: i64::MAX,
            last_write_seq: 0,
        }
    }
    pub(super) fn finish(db: DatabaseEngine) {
        let stats = db.runtime.candidate.as_ref().unwrap().stats.clone();
        db.close_fast().unwrap();
        drop(db);
        let until = Instant::now() + Duration::from_secs(20);
        while !matches!(
            stats.read_full().unwrap().close_state,
            ReclamationCloseState::Drained
        ) {
            assert!(Instant::now() < until);
            std::thread::sleep(Duration::from_millis(1));
        }
    }
    fn assert_old(view: &ReadView, label: u32, edge_label: u32) {
        assert_eq!(view.get_node_raw(1).unwrap().unwrap().key, "before");
        assert!(view
            .get_node_by_label_key_raw(label, "before")
            .unwrap()
            .is_some());
        assert!(view
            .get_node_by_label_key_raw(label, "after")
            .unwrap()
            .is_none());
        assert_eq!(
            view.sources()
                .find_edge_by_triple(1, 2, edge_label)
                .unwrap()
                .unwrap()
                .id,
            11
        );
        let rows = view.get_nodes_raw(&[1, 99, 1]).unwrap();
        assert!(rows[1].is_none());
        assert_eq!(rows[0].as_ref().unwrap().key, rows[2].as_ref().unwrap().key);
    }
    #[test]
    fn p45b1_actual_primary_identity_and_publication_overlap() {
        for stage in [1, 2, 4] {
            let dir = tempfile::tempdir().unwrap();
            let db = DatabaseEngine::open(dir.path(), &DbOptions::default()).unwrap();
            let n = db.ensure_node_label("N").unwrap();
            let e = db.ensure_edge_label("E").unwrap();
            db.write_op_batch(&[
                WalOp::UpsertNode(node(1, n, "before")),
                WalOp::UpsertNode(node(2, n, "other")),
                WalOp::UpsertEdge(edge(11, 1, 2, e)),
            ])
            .unwrap();
            let old = db.runtime.published.load_full().view.clone();
            assert!(old.memtable.is_candidate());
            assert_eq!(old.memtable.locked_primary_slots_for_test(), [0; 4]);
            let (ready, release) = if stage == 4 {
                db.set_runtime_publish_pause()
            } else {
                old.memtable.primary_pause_for_test(stage)
            };
            let writer = db.clone();
            let job = std::thread::spawn(move || {
                writer.write_op_batch(&[
                    WalOp::UpsertNode(node(1, n, "after")),
                    WalOp::UpsertEdge(edge(11, 2, 1, e)),
                ])
            });
            ready.recv_timeout(Duration::from_secs(10)).unwrap();
            assert_old(&old, n, e);
            // Acquiring a new public read also observes the complete old frontier.
            assert_eq!(db.get_node(1).unwrap().unwrap().key, "before");
            assert!(db.get_node_by_key("N", "before").unwrap().is_some());
            assert!(db.get_node_by_key("N", "after").unwrap().is_none());
            assert!(old.memtable.participant_handle().unwrap().snapshot().0 >= 1);
            release.send(()).unwrap();
            job.join().unwrap().unwrap();
            assert_eq!(db.get_node(1).unwrap().unwrap().key, "after");
            assert_old(&old, n, e);
            drop(old);
            finish(db);
        }
    }
    #[test]
    fn p45b1_admitted_reader_does_not_hold_writer_or_callback_custody() {
        let dir = tempfile::tempdir().unwrap();
        let db = DatabaseEngine::open(dir.path(), &DbOptions::default()).unwrap();
        let n = db.ensure_node_label("N").unwrap();
        db.write_op(&WalOp::UpsertNode(node(1, n, "before")))
            .unwrap();
        let view = db.runtime.published.load_full().view.clone();
        let (ready, release) = view.memtable.primary_pause_for_test(3);
        let reader_view = view.clone();
        let reader = std::thread::spawn(move || reader_view.get_node_raw(1));
        ready.recv_timeout(Duration::from_secs(10)).unwrap();
        let (writer_tx, writer_rx) = std::sync::mpsc::channel();
        let writer = db.clone();
        let writer_job = std::thread::spawn(move || {
            writer_tx
                .send(writer.write_op(&WalOp::UpsertNode(node(1, n, "after"))))
                .unwrap()
        });
        writer_rx
            .recv_timeout(Duration::from_secs(10))
            .unwrap()
            .unwrap();
        release.send(()).unwrap();
        assert_eq!(reader.join().unwrap().unwrap().unwrap().key, "before");
        writer_job.join().unwrap();
        let admission = view.memtable.participant_handle().unwrap();
        let _ = view
            .memtable
            .for_each_visible_node_at(view.snapshot_seq, crate::row_projection::VectorSelection::None, &mut |_| {
                assert_eq!(admission.snapshot().0, 0);
                ControlFlow::Continue(())
            })
            .unwrap();
        drop(view);
        finish(db);
    }
    #[test]
    fn p45b1_sequences_negative_opinions_projection_and_hot_seek() {
        let dir = tempfile::tempdir().unwrap();
        let db = DatabaseEngine::open(
            dir.path(),
            &DbOptions {
                dense_vector: Some(DenseVectorConfig {
                    dimension: 2,
                    metric: DenseMetric::Cosine,
                    hnsw: Default::default(),
                }),
                ..Default::default()
            },
        )
        .unwrap();
        let n = db.ensure_node_label("N").unwrap();
        let base = db.engine_seq_for_test();
        let mut final_node = node(1, n, "c");
        final_node.props.insert("wanted".into(), PropValue::Int(7));
        final_node
            .props
            .insert("ignored".into(), PropValue::String("unused".into()));
        final_node.dense_vector = Some(vec![1.0, 2.0]);
        db.write_op_batch(&[
            WalOp::UpsertNode(node(1, n, "a")),
            WalOp::UpsertNode(node(1, n, "b")),
            WalOp::DeleteNode {
                id: 1,
                deleted_at: 77,
            },
            WalOp::UpsertNode(final_node),
            WalOp::EnsureNodeLabel {
                label: "Unused".into(),
                label_id: n + 1,
            },
            WalOp::UpsertNode(node(3, n, "stable")),
        ])
        .unwrap();
        assert_eq!(db.engine_seq_for_test(), base + 6);
        db.write_op_batch(&[]).unwrap();
        assert_eq!(db.engine_seq_for_test(), base + 6);
        let view = db.runtime.published.load_full().view.clone();
        let mt = &view.memtable;
        assert_eq!(
            mt.node_tombstone_at(1, base + 3)
                .unwrap()
                .unwrap()
                .deleted_at,
            77
        );
        assert_eq!(
            mt.node_tombstone_at(1, base + 3)
                .unwrap()
                .unwrap()
                .last_write_seq,
            base + 3
        );
        assert_eq!(
            mt.key_opinions_at(&[(n, "a"), (n, "b"), (n, "c")], base + 2)
                .unwrap(),
            vec![
                crate::memtable::IdentityOpinion::Removed,
                crate::memtable::IdentityOpinion::Live(1),
                crate::memtable::IdentityOpinion::Missing
            ]
        );
        assert_eq!(
            mt.get_node_at(1, u64::MAX).unwrap().unwrap().last_write_seq,
            base + 4
        );
        let fields = view
            .sources()
            .find_node_projected_fields(
                &[1, 99, 1],
                &crate::row_projection::NodeSelectedFieldNeeds {
                    key: true,
                    props: crate::row_projection::PropertySelection::Keys(vec!["wanted".into()]),
                    ..Default::default()
                },
            )
            .unwrap();
        assert_eq!(fields[0].as_ref().unwrap().props.len(), 1);
        assert!(fields[0].as_ref().unwrap().dense_vector.is_none());
        assert_eq!(
            fields[0].as_ref().unwrap().key,
            fields[2].as_ref().unwrap().key
        );
        assert!(fields[1].is_none());
        let mut ops = Vec::new();
        for i in 0..4096 {
            let mut v = node(1, n, "c");
            v.weight = i as f32;
            ops.push(WalOp::UpsertNode(v));
        }
        ops.push(WalOp::UpsertNode(node(2, n, "future")));
        db.write_op_batch(&ops).unwrap();
        mt.primary_node_seeks_for_test(true);
        assert_eq!(mt.get_node_at(1, base + 2).unwrap().unwrap().key, "b");
        assert_eq!(mt.primary_node_seeks_for_test(false), 1);
        assert_eq!(mt.visible_node_ids_at(base + 6).unwrap(), vec![1, 3]);
        assert_eq!(mt.visible_node_count_at(base + 3).unwrap(), 0);
        assert_eq!(mt.max_node_id(), 3);
        db.write_op(&WalOp::DeleteEdge {
            id: 22,
            deleted_at: 123,
        })
        .unwrap();
        let tomb = mt.edge_tombstone_at(22, u64::MAX).unwrap().unwrap();
        assert_eq!(tomb.deleted_at, 123);
        assert_eq!(tomb.last_write_seq, db.engine_seq_for_test());
        assert!(view.sources().find_edge(22).unwrap().is_none());
        assert_eq!(mt.estimated_size(), mt.estimated_size_full_for_test());
        drop(view);
        finish(db);
    }
    #[test]
    fn p45b1_cross_segment_identity_revalidation_and_parallel_owner_replay() {
        let dir = tempfile::tempdir().unwrap();
        let options = DbOptions {
            edge_uniqueness: false,
            compact_after_n_flushes: 0,
            ..Default::default()
        };
        let db = DatabaseEngine::open(dir.path(), &options).unwrap();
        let n = db.ensure_node_label("N").unwrap();
        let other = db.ensure_node_label("Other").unwrap();
        let e = db.ensure_edge_label("E").unwrap();
        db.write_op_batch(&[
            WalOp::UpsertNode(node(1, n, "old")),
            WalOp::UpsertNode(node(2, n, "two")),
            WalOp::UpsertNode(node(3, n, "three")),
            WalOp::UpsertEdge(edge(11, 1, 2, e)),
            WalOp::UpsertEdge(edge(12, 1, 2, e)),
        ])
        .unwrap();
        let old = db.runtime.published.load_full().view.clone();
        db.flush().unwrap();
        let owner = db
            .runtime
            .published
            .load_full()
            .view
            .sources()
            .find_edge_by_triple(1, 2, e)
            .unwrap()
            .unwrap()
            .id;
        let nonowner = if owner == 11 { 12 } else { 11 };
        db.write_op(&WalOp::UpsertEdge(edge(nonowner, 1, 2, e)))
            .unwrap();
        assert_eq!(
            db.runtime
                .published
                .load_full()
                .view
                .sources()
                .find_edge_by_triple(1, 2, e)
                .unwrap()
                .unwrap()
                .id,
            owner
        );
        db.write_op_batch(&[
            WalOp::UpsertNode(node(1, other, "new")),
            WalOp::DeleteEdge {
                id: nonowner,
                deleted_at: 8,
            },
            WalOp::UpsertEdge(edge(owner, 3, 1, e)),
        ])
        .unwrap();
        let current = db.runtime.published.load_full().view.clone();
        assert!(current
            .sources()
            .find_node_by_label_key(n, "old")
            .unwrap()
            .is_none());
        assert_eq!(
            current
                .sources()
                .find_node_ids_by_label_keys(&[(n, "old"), (other, "new"), (other, "new")])
                .unwrap(),
            vec![None, Some(1), Some(1)]
        );
        assert!(current
            .sources()
            .find_edge_by_triple(1, 2, e)
            .unwrap()
            .is_none());
        let rows = current
            .sources()
            .find_edges_by_triples(&[(1, 2, e), (3, 1, e), (3, 1, e)])
            .unwrap();
        assert!(rows[0].is_none());
        assert_eq!(rows[1].as_ref().unwrap().id, owner);
        assert_eq!(rows[2].as_ref().unwrap().id, owner);
        assert_eq!(
            old.sources()
                .find_node_by_label_key(n, "old")
                .unwrap()
                .unwrap()
                .id,
            1
        );
        assert_eq!(
            old.sources()
                .find_edge_by_triple(1, 2, e)
                .unwrap()
                .unwrap()
                .id,
            12
        );
        drop(current);
        drop(old);
        finish(db);
        let reopened = DatabaseEngine::open(dir.path(), &options).unwrap();
        let view = reopened.runtime.published.load_full().view.clone();
        assert!(view
            .sources()
            .find_edge_by_triple(1, 2, e)
            .unwrap()
            .is_none());
        assert_eq!(
            view.sources()
                .find_edge_by_triple(3, 1, e)
                .unwrap()
                .unwrap()
                .id,
            owner
        );
        assert!(view
            .sources()
            .find_node_by_label_key(n, "old")
            .unwrap()
            .is_none());
        drop(view);
        finish(reopened);
    }
    #[test]
    fn p45b1_participant_pressure_is_explicit_before_wal() {
        const CHILD: &str = "P45B1_PARTICIPANT_CHILD";
        if std::env::var_os(CHILD).is_none() {
            let output = std::process::Command::new(std::env::current_exe().unwrap())
                .args([
                    "--exact",
                    "engine::p45b1_tests::p45b1_participant_pressure_is_explicit_before_wal",
                    "--nocapture",
                ])
                .env(CHILD, "1")
                .output()
                .unwrap();
            assert!(
                output.status.success(),
                "{}\n{}",
                String::from_utf8_lossy(&output.stdout),
                String::from_utf8_lossy(&output.stderr)
            );
            return;
        }
        let dir = tempfile::tempdir().unwrap();
        let db = DatabaseEngine::open(dir.path(), &DbOptions::default()).unwrap();
        let n = db.ensure_node_label("N").unwrap();
        db.write_op(&WalOp::UpsertNode(node(1, n, "before")))
            .unwrap();
        db.sync().unwrap();
        let view = db.runtime.published.load_full().view.clone();
        let handle = view.memtable.participant_handle().unwrap();
        let sessions = (0..256)
            .map(|_| handle.try_acquire().unwrap())
            .collect::<Vec<_>>();
        let seq = db.engine_seq_for_test();
        let wal = std::fs::read_dir(dir.path())
            .unwrap()
            .map(|e| e.unwrap().path())
            .filter(|p| p.extension().is_some_and(|s| s == "wal"))
            .map(|p| {
                let len = std::fs::metadata(&p).unwrap().len();
                (p, len)
            })
            .collect::<Vec<_>>();
        assert!(!wal.is_empty());
        assert!(matches!(
            db.get_node(1),
            Err(EngineError::ResourceLimit {
                resource: "participants",
                ..
            })
        ));
        assert!(matches!(
            db.write_op(&WalOp::UpsertNode(node(99, n, "refused"))),
            Err(EngineError::ResourceLimit {
                resource: "participants",
                ..
            })
        ));
        assert_eq!(db.engine_seq_for_test(), seq);
        for (p, len) in wal {
            assert_eq!(std::fs::metadata(p).unwrap().len(), len);
        }
        drop(sessions);
        assert!(db.get_node(99).unwrap().is_none());
        assert_eq!(
            db.upsert_node("N", "next", UpsertNodeOptions::default())
                .unwrap(),
            2
        );
        drop(view);
        finish(db);
    }
    #[test]
    fn p45b1_mid_install_fault_fences_all_close_modes_and_replays_whole_batch() {
        for immediate in [false, true] {
            for mode in 0..3 {
                let dir = tempfile::tempdir().unwrap();
                let opts = DbOptions {
                    wal_sync_mode: if immediate {
                        WalSyncMode::Immediate
                    } else {
                        WalSyncMode::default()
                    },
                    ..Default::default()
                };
                let db = DatabaseEngine::open(dir.path(), &opts).unwrap();
                let n = db.ensure_node_label("N").unwrap();
                let seq = db.engine_seq_for_test();
                let view = db.runtime.published.load_full().view.clone();
                view.memtable.fail_primary_install_for_test();
                assert!(matches!(
                    db.write_op_batch(&[
                        WalOp::UpsertNode(node(1, n, "one")),
                        WalOp::UpsertNode(node(2, n, "two"))
                    ]),
                    Err(EngineError::CommitOutcome {
                        state: CommitOutcomeState::Indeterminate,
                        ..
                    })
                ));
                assert_eq!(db.runtime.published.load_full().view.snapshot_seq, seq);
                assert!(db.get_node(1).unwrap().is_none());
                assert!(db
                    .upsert_node("N", "later", UpsertNodeOptions::default())
                    .is_err());
                let stats = db.runtime.candidate.as_ref().unwrap().stats.clone();
                drop(view);
                match mode {
                    0 => assert!(db.close().is_err()),
                    1 => assert!(db.close_fast().is_err()),
                    _ => {}
                }
                drop(db);
                let until = Instant::now() + Duration::from_secs(20);
                while !matches!(
                    stats.read_full().unwrap().close_state,
                    ReclamationCloseState::Drained
                ) {
                    assert!(Instant::now() < until);
                    std::thread::sleep(Duration::from_millis(1));
                }
                let reopened = DatabaseEngine::open(dir.path(), &opts).unwrap();
                assert_eq!(reopened.get_node(1).unwrap().unwrap().key, "one");
                assert_eq!(reopened.get_node(2).unwrap().unwrap().key, "two");
                assert!(reopened.get_node_by_key("N", "later").unwrap().is_none());
                finish(reopened);
            }
        }
    }
    #[test]
    fn p45b1_refills_release_sessions_and_continue_past_large_and_future_groups() {
        let dir = tempfile::tempdir().unwrap();
        let db = DatabaseEngine::open(dir.path(), &DbOptions::default()).unwrap();
        let n = db.ensure_node_label("N").unwrap();
        let mut big = node(1, n, "big");
        big.props.insert(
            "payload".into(),
            PropValue::String("x".repeat(1024 * 1024 + 1)),
        );
        db.write_op_batch(&[
            WalOp::UpsertNode(big),
            WalOp::UpsertNode(node(9000, n, "tail")),
        ])
        .unwrap();
        let view = db.runtime.published.load_full().view.clone();
        let handle = view.memtable.participant_handle().unwrap();
        let ops = (2..4100)
            .map(|id| WalOp::UpsertNode(node(id, n, &format!("future{id}"))))
            .collect::<Vec<_>>();
        db.write_op_batch(&ops).unwrap();
        let mut ids = Vec::new();
        let flow = view
            .memtable
            .for_each_visible_node_at(view.snapshot_seq, crate::row_projection::VectorSelection::None, &mut |row| {
                assert_eq!(handle.snapshot().0, 0);
                ids.push(row.id);
                ControlFlow::Continue(())
            })
            .unwrap();
        assert!(flow.is_continue());
        assert_eq!(ids, vec![1, 9000]);
        drop(view);
        finish(db);
    }
    #[test]
    fn p45b1_transaction_return_captures_commit_before_source_only_maintenance() {
        let dir = tempfile::tempdir().unwrap();
        let db = DatabaseEngine::open(
            dir.path(),
            &DbOptions {
                memtable_flush_threshold: 1,
                compact_after_n_flushes: 0,
                ..Default::default()
            },
        )
        .unwrap();
        db.ensure_node_label("N").unwrap();
        let mut txn = db.begin_write_txn().unwrap();
        txn.upsert_node("N", "committed", UpsertNodeOptions::default())
            .unwrap();
        let (_, view) = txn.commit_with_gql_return_view().unwrap();
        let current = db.runtime.published.load_full().view.clone();
        assert_eq!(view.snapshot_seq, current.snapshot_seq);
        assert!(!SharedArc::ptr_eq(&view.memtable, &current.memtable));
        assert_eq!(view.get_node_raw(1).unwrap().unwrap().key, "committed");
        assert_eq!(view.memtable.locked_primary_slots_for_test(), [0; 4]);
        drop(current);
        drop(view);
        drop(txn);
        finish(db);
    }
    #[test]
    fn p45b1_postpublication_maintenance_failure_keeps_published_commit() {
        let dir = tempfile::tempdir().unwrap();
        let opts = DbOptions {
            memtable_flush_threshold: 1,
            compact_after_n_flushes: 0,
            ..Default::default()
        };
        let db = DatabaseEngine::open(dir.path(), &opts).unwrap();
        db.ensure_node_label("N").unwrap();
        db.with_core_mut(|core| {
            core.force_next_wal_open_error = true;
            Ok(())
        })
        .unwrap();
        assert!(matches!(
            db.upsert_node("N", "committed", UpsertNodeOptions::default()),
            Err(EngineError::CommitOutcome {
                state: CommitOutcomeState::Indeterminate,
                ..
            })
        ));
        assert_eq!(db.get_node(1).unwrap().unwrap().key, "committed");
        let stats = db.runtime.candidate.as_ref().unwrap().stats.clone();
        assert!(db.close_fast().is_err());
        drop(db);
        let until = Instant::now() + Duration::from_secs(20);
        while !matches!(
            stats.read_full().unwrap().close_state,
            ReclamationCloseState::Drained
        ) {
            assert!(Instant::now() < until);
            std::thread::sleep(Duration::from_millis(1));
        }
        let reopened = DatabaseEngine::open(dir.path(), &opts).unwrap();
        assert_eq!(reopened.get_node(1).unwrap().unwrap().key, "committed");
        finish(reopened);
    }
    #[test]
    fn p45b1_replay_preserves_zero_sequence_and_refuses_duplicate_versions_read_only() {
        // V3's scalar writer historically accepts zero. Keep that identity rather
        // than deriving a negative base or changing the persisted representation.
        let dir = tempfile::tempdir().unwrap();
        let mut manifest = crate::manifest::default_manifest();
        manifest.node_label_tokens.insert("N".into(), 1);
        manifest.next_node_label_id = 2;
        crate::manifest::write_manifest(dir.path(), &manifest).unwrap();
        let mut wal = crate::wal::WalWriter::open_generation(dir.path(), 0).unwrap();
        wal.append(&WalOp::UpsertNode(node(1, 1, "legacy-zero")), 0)
            .unwrap();
        wal.sync().unwrap();
        drop(wal);
        let db = DatabaseEngine::open(dir.path(), &DbOptions::default()).unwrap();
        assert_eq!(db.get_node(1).unwrap().unwrap().key, "legacy-zero");
        assert_eq!(
            db.runtime
                .published
                .load_full()
                .view
                .sources()
                .find_node(1)
                .unwrap()
                .unwrap()
                .last_write_seq,
            0
        );
        assert_eq!(
            db.upsert_node("N", "next", UpsertNodeOptions::default())
                .unwrap(),
            2
        );
        let last = db.engine_seq_for_test();
        let generation = db.active_wal_generation();
        finish(db);
        // A repeated full operation sequence cannot be installed into AppendMap.
        // Detect it in read-only preflight, before any map or file mutation.
        let path = crate::wal::wal_generation_path(dir.path(), generation);
        let mut wal = crate::wal::WalWriter::open_generation(dir.path(), generation).unwrap();
        wal.append(&WalOp::UpsertNode(node(1, 1, "duplicate")), last)
            .unwrap();
        wal.sync().unwrap();
        drop(wal);
        let before_wal = std::fs::read(&path).unwrap();
        let current_path = dir.path().join(crate::manifest::MANIFEST_CURRENT);
        let before_manifest = std::fs::read(&current_path).unwrap();
        assert!(
            matches!(DatabaseEngine::open(dir.path(),&DbOptions::default()),Err(EngineError::CorruptWal(message)) if message.contains("nonmonotonic"))
        );
        assert_eq!(std::fs::read(path).unwrap(), before_wal);
        assert_eq!(std::fs::read(current_path).unwrap(), before_manifest);
    }
    #[test]
    fn p45b1_candidate_tombstone_scan_is_sparse_and_snapshot_validated() {
        let dir = tempfile::tempdir().unwrap();
        let db = DatabaseEngine::open(dir.path(), &DbOptions::default()).unwrap();
        let n = db.ensure_node_label("N").unwrap();
        let e = db.ensure_edge_label("E").unwrap();
        db.write_op_batch(&[WalOp::UpsertNode(node(1,n,"one")), WalOp::UpsertEdge(edge(1,1,2,e))]).unwrap();
        db.write_op_batch(&[
            WalOp::DeleteNode { id: 1, deleted_at: 3 },
            WalOp::DeleteEdge { id: 1, deleted_at: 4 },
            WalOp::DeleteNode { id: 1, deleted_at: 5 },
            WalOp::DeleteEdge { id: 1, deleted_at: 6 },
        ]).unwrap();
        let old = db.runtime.published.load_full().view.clone();
        db.write_op_batch(&[WalOp::UpsertNode(node(1,n,"resurrected")), WalOp::UpsertEdge(edge(1,1,2,e))]).unwrap();
        let mt = &old.memtable;
        let check = || {
            mt.take_tombstone_scan_work_for_test();
            assert_eq!(mt.collect_deleted_nodes_at(old.snapshot_seq).unwrap(), NodeIdSet::from_iter([1]));
            assert_eq!(mt.collect_deleted_edges_at(old.snapshot_seq).unwrap(), NodeIdSet::from_iter([1]));
            assert_eq!(mt.take_tombstone_scan_work_for_test(), 2); // Repeated deletes retain one marker.
            assert!(mt.collect_deleted_nodes_at(u64::MAX).unwrap().is_empty());
            assert!(mt.collect_deleted_edges_at(u64::MAX).unwrap().is_empty());
            assert_eq!(mt.take_tombstone_scan_work_for_test(), 2);
        };
        check();
        let mut ops = Vec::new();
        for id in 2..1002 {
            ops.push(WalOp::UpsertNode(node(id,n,&format!("live{id}"))));
            ops.push(WalOp::UpsertEdge(edge(id,1,id+1,e)));
        }
        db.write_op_batch(&ops).unwrap();
        check();
        db.write_op_batch(&[
            WalOp::DeleteNode { id: 2, deleted_at: 9000 },
            WalOp::DeleteEdge { id: 2, deleted_at: 9001 },
        ]).unwrap();
        assert_eq!(mt.collect_deleted_nodes_at(old.snapshot_seq).unwrap(), NodeIdSet::from_iter([1]));
        assert_eq!(mt.collect_deleted_edges_at(old.snapshot_seq).unwrap(), NodeIdSet::from_iter([1]));
        assert_eq!(mt.take_tombstone_scan_work_for_test(), 4); // Future markers are revalidated at old S.
        assert_eq!(mt.collect_deleted_nodes_at(u64::MAX).unwrap(), NodeIdSet::from_iter([2]));
        assert_eq!(mt.collect_deleted_edges_at(u64::MAX).unwrap(), NodeIdSet::from_iter([2]));
        assert_eq!(mt.estimated_size(), mt.estimated_size_full_for_test());
        drop(old);
        finish(db);
    }

}

#[cfg(test)]
mod p45b2_tests {
    use super::p45b1_tests::{edge, finish, node};
    use super::*;
    use std::time::Duration;
    fn neighbors(view: &ReadView, id: u64, d: Direction) -> Vec<(u64, u64, u32, f32)> {
        let mut rows = view
            .neighbors_raw(id, d, None, 0, Some(100), None, None)
            .unwrap()
            .into_iter()
            .map(|e| (e.edge_id, e.node_id, e.edge_label_id, e.weight))
            .collect::<Vec<_>>();
        rows.sort_by_key(|e| e.0);
        rows
    }
    fn fixture() -> (tempfile::TempDir, DatabaseEngine, u32, u32, u32) {
        let dir = tempfile::tempdir().unwrap();
        let db = DatabaseEngine::open(dir.path(), &DbOptions::default()).unwrap();
        let n = db.ensure_node_label("N").unwrap();
        let e = db.ensure_edge_label("E").unwrap();
        let f = db.ensure_edge_label("F").unwrap();
        db.write_op_batch(
            &(1..=4)
                .map(|id| WalOp::UpsertNode(node(id, n, &format!("n{id}"))))
                .collect::<Vec<_>>(),
        )
        .unwrap();
        (dir, db, n, e, f)
    }
    fn assert_old(view: &ReadView, e: u32) {
        assert_eq!(
            neighbors(view, 1, Direction::Outgoing),
            vec![(11, 2, e, 1.0)]
        );
        assert_eq!(
            neighbors(view, 2, Direction::Incoming),
            vec![(11, 1, e, 1.0)]
        );
        assert_eq!(
            view.degree_stats(1, Direction::Outgoing, Some(&[e]), 100)
                .unwrap(),
            (1, 1.0)
        );
        assert_eq!(view.edges_by_label_id(e).unwrap(), vec![11]);
    }
    #[test]
    fn p45b2_both_direction_group_label_and_publication_frontiers() {
        for stage in [5, 6, 7, 8, 2, 4] {
            let (_dir, db, n, e, f) = fixture();
            db.write_op(&WalOp::UpsertEdge(edge(11, 1, 2, e))).unwrap();
            let old = db.runtime.published.load_full().view.clone();
            assert_eq!(old.memtable.locked_graph_slots_for_test(), [0; 10]);
            let (ready, release) = if stage == 4 {
                db.set_runtime_publish_pause()
            } else {
                old.memtable.primary_pause_for_test(stage)
            };
            let writer = db.clone();
            let job = std::thread::spawn(move || {
                let mut moved = node(1, n, "new");
                moved.updated_at = 9;
                let edge = WalOp::UpsertEdge(edge(11, 3, 4, f));
                let node = WalOp::UpsertNode(moved);
                if stage == 8 {
                    writer.write_op_batch(&[node, edge])
                } else {
                    writer.write_op_batch(&[edge, node])
                }
            });
            ready.recv_timeout(Duration::from_secs(10)).unwrap();
            // Stage 2 holds the remaining secondary writer lock. These reads must complete.
            assert_old(&old, e);
            let fresh = db.runtime.published.load_full().view.clone();
            assert_old(&fresh, e);
            assert_eq!(
                old.find_nodes_by_time_range(n, 2, 2).unwrap(),
                vec![1, 2, 3, 4]
            );
            release.send(()).unwrap();
            job.join().unwrap().unwrap();
            assert_old(&old, e);
            let current = db.runtime.published.load_full().view.clone();
            assert!(neighbors(&current, 1, Direction::Both).is_empty());
            assert_eq!(
                neighbors(&current, 3, Direction::Outgoing),
                vec![(11, 4, f, 1.0)]
            );
            assert_eq!(
                current.find_nodes_by_time_range(n, 2, 2).unwrap(),
                vec![2, 3, 4]
            );
            assert_eq!(current.memtable.locked_graph_slots_for_test(), [0; 10]);
            drop((old, fresh, current));
            finish(db);
        }
    }
    #[test]
    fn p45b2_reader_refill_and_callback_release_writer() {
        let (_dir, db, _, e, _) = fixture();
        db.write_op(&WalOp::UpsertEdge(edge(11, 1, 2, e))).unwrap();
        let old = db.runtime.published.load_full().view.clone();
        let (ready, release) = old.memtable.primary_pause_for_test(3);
        let reader_view = old.clone();
        let reader = std::thread::spawn(move || neighbors(&reader_view, 1, Direction::Outgoing));
        ready.recv_timeout(Duration::from_secs(10)).unwrap();
        let writer = db.clone();
        let (tx, rx) = std::sync::mpsc::channel();
        let job = std::thread::spawn(move || {
            tx.send(writer.write_op(&WalOp::UpsertEdge(edge(11, 3, 4, e))))
        });
        rx.recv_timeout(Duration::from_secs(10)).unwrap().unwrap();
        release.send(()).unwrap();
        assert_eq!(reader.join().unwrap(), vec![(11, 2, e, 1.0)]);
        job.join().unwrap().unwrap();
        let admission = old.memtable.participant_handle().unwrap();
        let mut visited = 0;
        let _ = old
            .memtable
            .for_each_adj_entry_at(
                1,
                Direction::Both,
                None,
                old.snapshot_seq,
                &mut |_, _, _, _, _| {
                    assert_eq!(admission.snapshot().0, 0);
                    visited += 1;
                    db.write_op(&WalOp::UpsertEdge(edge(12, 1, 4, e))).unwrap();
                    ControlFlow::Break(())
                },
            )
            .unwrap();
        assert_eq!(visited, 1);
        drop(old);
        finish(db);
    }
    fn check_model(view: &ReadView, model: &BTreeMap<u64, EdgeRecord>, e: u32, f: u32) {
        for owner in 1..=4 {
            for d in [Direction::Outgoing, Direction::Incoming, Direction::Both] {
                let expected = model
                    .values()
                    .filter(|x| is_edge_valid_at(x.valid_from, x.valid_to, 100))
                    .filter_map(|x| {
                        let neighbor = match d {
                            Direction::Outgoing if x.from == owner => x.to,
                            Direction::Incoming if x.to == owner => x.from,
                            Direction::Both if x.from == owner => x.to,
                            Direction::Both if x.to == owner => x.from,
                            _ => return None,
                        };
                        Some((x.id, neighbor, x.label_id, x.weight))
                    })
                    .collect::<Vec<_>>();
                assert_eq!(
                    neighbors(view, owner, d),
                    expected,
                    "owner {owner} direction {d:?}"
                );
                for label in [e, f] {
                    let rows = expected.iter().filter(|r| r.2 == label).collect::<Vec<_>>();
                    assert_eq!(
                        view.degree_stats(owner, d, Some(&[label]), 100).unwrap(),
                        (rows.len() as u64, rows.iter().map(|r| r.3 as f64).sum())
                    );
                }
            }
        }
        for label in [e, f] {
            assert_eq!(
                view.edges_by_label_id(label).unwrap(),
                model
                    .values()
                    .filter(|x| x.label_id == label)
                    .map(|x| x.id)
                    .collect::<Vec<_>>()
            );
        }
        assert_eq!(
            view.memtable.estimated_size(),
            view.memtable.estimated_size_full_for_test()
        );
    }
    #[test]
    fn p45b2_same_sequence_model_moves_relabels_self_loops_and_storage() {
        let (dir, db, _, e, f) = fixture();
        let mut model = BTreeMap::new();
        let mut held = Vec::new();
        let batches = vec![
            vec![
                WalOp::UpsertEdge(edge(10, 1, 1, e)),
                WalOp::UpsertEdge(edge(11, 1, 2, e)),
                WalOp::UpsertEdge(edge(12, 1, 2, e)),
            ],
            vec![
                WalOp::UpsertEdge(edge(11, 3, 4, f)),
                WalOp::UpsertEdge(edge(12, 2, 1, f)),
            ],
            vec![
                WalOp::DeleteEdge {
                    id: 10,
                    deleted_at: 3,
                },
                WalOp::UpsertEdge(edge(10, 4, 4, f)),
                WalOp::UpsertEdge(edge(10, 1, 3, e)),
                WalOp::DeleteEdge {
                    id: 12,
                    deleted_at: 3,
                },
            ],
            vec![
                WalOp::UpsertEdge({
                    let mut x = edge(11, 3, 4, f);
                    x.valid_from = 101;
                    x.weight = 9.0;
                    x
                }),
                WalOp::UpsertEdge(edge(12, 2, 2, e)),
            ],
        ];
        for (i, batch) in batches.into_iter().enumerate() {
            db.write_op_batch(&batch).unwrap();
            for op in batch {
                match op {
                    WalOp::UpsertEdge(x) => {
                        model.insert(x.id, x);
                    }
                    WalOp::DeleteEdge { id, .. } => {
                        model.remove(&id);
                    }
                    _ => unreachable!(),
                }
            }
            let view = db.runtime.published.load_full().view.clone();
            check_model(&view, &model, e, f);
            held.push((view, model.clone()));
            if i == 0 {
                db.freeze_memtable().unwrap();
            } else if i == 1 {
                db.flush().unwrap();
            } else if i == 2 {
                db.flush().unwrap();
                db.compact().unwrap();
            }
        }
        for (view, expected) in &held {
            check_model(view, expected, e, f);
        }
        drop(held);
        finish(db);
        let reopened = DatabaseEngine::open(dir.path(), &DbOptions::default()).unwrap();
        let view = reopened.runtime.published.load_full().view.clone();
        check_model(&view, &model, e, f);
        drop(view);
        finish(reopened);
    }
    #[test]
    fn p45b2_updates_touch_constant_keys_without_group_churn() {
        for fanout in [10, 1000] {
            let (_dir, db, _, e, _) = fixture();
            db.write_op_batch(
                &(0..fanout)
                    .map(|id| WalOp::UpsertEdge(edge(id + 10, 1, 2, e)))
                    .collect::<Vec<_>>(),
            )
            .unwrap();
            let view = db.runtime.published.load_full().view.clone();
            let before = view.memtable.graph_versions_for_test();
            let updates = (0..100)
                .map(|i| {
                    let mut x = edge(10, 1, 2, e);
                    x.weight = (i + 1) as f32;
                    WalOp::UpsertEdge(x)
                })
                .collect::<Vec<_>>();
            db.write_op_batch(&updates).unwrap();
            let after = view.memtable.graph_versions_for_test();
            assert_eq!(
                neighbors(&view, 1, Direction::Outgoing).len(),
                fanout as usize
            );
            assert_eq!(
                neighbors(&view, 1, Direction::Outgoing)
                    .iter()
                    .find(|r| r.0 == 10)
                    .unwrap()
                    .3,
                1.0
            );
            assert_eq!(
                view.degree_stats(1, Direction::Outgoing, None, 100)
                    .unwrap(),
                (fanout, fanout as f64)
            );
            assert_eq!(after[0] - before[0], 200);
            assert_eq!(&after[1..], &before[1..]);
            assert_eq!(
                view.memtable.estimated_size(),
                view.memtable.estimated_size_full_for_test()
            );
            drop(view);
            finish(db);
        }
    }
    #[test]
    fn p45b2_future_removed_refills_progress_and_conservative_bounds() {
        let (_dir, db, _, e, _) = fixture();
        db.write_op_batch(&[
            WalOp::UpsertEdge(edge(6001, 6000, 2, e)),
            WalOp::UpsertEdge(edge(6002, 1, 2, e)),
        ])
        .unwrap();
        let old = db.runtime.published.load_full().view.clone();
        let ops = (5..5005)
            .map(|id| WalOp::UpsertEdge(edge(id, id, 2, e)))
            .chain((5..5005).map(|id| WalOp::UpsertEdge(edge(id + 10000, 1, 2, e))))
            .collect::<Vec<_>>();
        db.write_op_batch(&ops).unwrap();
        let r = old
            .memtable
            .refill_ordered_adj_owner_at(
                Direction::Outgoing,
                None,
                old.snapshot_seq,
                std::ops::Bound::Unbounded,
                4096,
            )
            .unwrap();
        assert_eq!(r.owner_head, Some(1));
        let r = old
            .memtable
            .refill_ordered_adj_owner_at(
                Direction::Outgoing,
                None,
                old.snapshot_seq,
                std::ops::Bound::Excluded((1, u32::MAX)),
                4096,
            )
            .unwrap();
        assert!(r.owner_head.is_none());
        assert!(!r.exhausted);
        assert_eq!(r.examined, 4096);
        let r = old
            .memtable
            .refill_ordered_adj_owner_at(
                Direction::Outgoing,
                None,
                old.snapshot_seq,
                std::ops::Bound::Excluded(r.last_group_key_examined.unwrap()),
                4096,
            )
            .unwrap();
        assert_eq!(r.owner_head, Some(6000));
        let r = old
            .memtable
            .refill_ordered_adj_postings_at(
                Direction::Incoming,
                2,
                None,
                old.snapshot_seq,
                std::ops::Bound::Unbounded,
                4096,
                Vec::new(),
            )
            .unwrap();
        assert!(r.entries.is_empty());
        assert!(!r.exhausted);
        assert_eq!(r.examined, 4096);
        let r = old
            .memtable
            .refill_ordered_adj_postings_at(
                Direction::Incoming,
                2,
                None,
                old.snapshot_seq,
                std::ops::Bound::Excluded(r.last_edge_id_examined.unwrap()),
                4096,
                Vec::new(),
            )
            .unwrap();
        assert_eq!(
            r.entries.iter().map(|x| x.edge_id).collect::<Vec<_>>(),
            vec![6001, 6002]
        );
        assert!(
            old.memtable
                .edge_label_posting_count_upper_bound(e)
                .unwrap()
                >= 10002
        );
        db.write_op_batch(
            &(5..5005)
                .map(|id| WalOp::DeleteEdge {
                    id,
                    deleted_at: 101,
                })
                .collect::<Vec<_>>(),
        )
        .unwrap();
        let current = db.runtime.published.load_full().view.clone();
        let r = current
            .memtable
            .refill_ordered_adj_owner_at(
                Direction::Outgoing,
                None,
                current.snapshot_seq,
                std::ops::Bound::Excluded((1, u32::MAX)),
                4096,
            )
            .unwrap();
        assert!(r.owner_head.is_none());
        assert!(!r.exhausted);
        assert_eq!(r.examined, 4096);
        let r = current
            .memtable
            .refill_ordered_adj_owner_at(
                Direction::Outgoing,
                None,
                current.snapshot_seq,
                std::ops::Bound::Excluded(r.last_group_key_examined.unwrap()),
                4096,
            )
            .unwrap();
        assert_eq!(r.owner_head, Some(6000));
        drop(current);
        drop(old);
        finish(db);
    }
    #[test]
    fn p45b2_group_counts_are_checked_and_same_sequence_crossings_coalesce() {
        let (_dir, db, _, _, _) = fixture();
        let view = db.runtime.published.load_full().view.clone();
        view.memtable.verify_group_counts_for_test();
        drop(view);
        finish(db);
    }
    #[test]
    fn p45b2_multi_label_time_count_retractions_and_resurrection() {
        let (dir, db, n, _, _) = fixture();
        let m = db.ensure_node_label("M").unwrap();
        let old = db.runtime.published.load_full().view.clone();
        let mut one = node(1, n, "n1");
        one.label_ids = NodeLabelSet::from_label_ids([n, m]).unwrap();
        one.updated_at = 10;
        let mut two = node(2, m, "n2");
        two.updated_at = 20;
        let mut last = node(1, m, "n1");
        last.updated_at = 30;
        db.write_op_batch(&[
            WalOp::UpsertNode(one),
            WalOp::UpsertNode(two),
            WalOp::UpsertNode(last),
        ])
        .unwrap();
        let moved = db.runtime.published.load_full().view.clone();
        assert_eq!(
            old.memtable
                .visible_nodes_by_label_id_count(n, old.snapshot_seq)
                .unwrap(),
            4
        );
        assert_eq!(
            old.memtable
                .visible_nodes_by_label_id_count(m, old.snapshot_seq)
                .unwrap(),
            0
        );
        assert_eq!(
            moved
                .memtable
                .visible_nodes_by_label_id_count(n, moved.snapshot_seq)
                .unwrap(),
            2
        );
        assert_eq!(
            moved
                .memtable
                .visible_nodes_by_label_id_count(m, moved.snapshot_seq)
                .unwrap(),
            2
        );
        assert_eq!(moved.nodes_by_label_id(n).unwrap(), vec![3, 4]);
        assert_eq!(moved.nodes_by_label_id(m).unwrap(), vec![1, 2]);
        assert_eq!(
            moved.find_nodes_by_time_range(m, 15, 30).unwrap(),
            vec![1, 2]
        );
        db.write_op(&WalOp::DeleteNode {
            id: 1,
            deleted_at: 31,
        })
        .unwrap();
        let mut resurrect = node(1, n, "again");
        resurrect.updated_at = 40;
        db.write_op(&WalOp::UpsertNode(resurrect)).unwrap();
        db.flush().unwrap();
        let current = db.runtime.published.load_full().view.clone();
        assert_eq!(current.nodes_by_label_id(n).unwrap(), vec![1, 3, 4]);
        assert_eq!(current.nodes_by_label_id(m).unwrap(), vec![2]);
        assert_eq!(
            current.find_nodes_by_time_range(n, 40, 40).unwrap(),
            vec![1]
        );
        assert_eq!(
            moved.find_nodes_by_time_range(m, 15, 30).unwrap(),
            vec![1, 2]
        );
        drop((old, moved, current));
        finish(db);
        let reopened = DatabaseEngine::open(dir.path(), &DbOptions::default()).unwrap();
        let view = reopened.runtime.published.load_full().view.clone();
        assert_eq!(view.find_nodes_by_time_range(n, 40, 40).unwrap(), vec![1]);
        assert_eq!(view.nodes_by_label_id(m).unwrap(), vec![2]);
        drop(view);
        finish(reopened);
    }
    #[test]
    fn p45b2_graph_pressure_errors_propagate() {
        const CHILD: &str = "P45B2_GRAPH_PRESSURE_CHILD";
        if std::env::var_os(CHILD).is_none() {
            let output = std::process::Command::new(std::env::current_exe().unwrap())
                .args([
                    "--exact",
                    "engine::p45b2_tests::p45b2_graph_pressure_errors_propagate",
                    "--nocapture",
                ])
                .env(CHILD, "1")
                .output()
                .unwrap();
            assert!(
                output.status.success(),
                "{}\n{}",
                String::from_utf8_lossy(&output.stdout),
                String::from_utf8_lossy(&output.stderr)
            );
            return;
        }

        let (_dir, db, _, e, _) = fixture();
        db.write_op(&WalOp::UpsertEdge(edge(11, 1, 2, e))).unwrap();
        let view = db.runtime.published.load_full().view.clone();
        let admission = view.memtable.participant_handle().unwrap();
        let leases = (0..256)
            .map(|_| admission.try_acquire().unwrap())
            .collect::<Vec<_>>();
        assert!(matches!(
            view.memtable
                .neighbors_at(1, Direction::Outgoing, None, 0, view.snapshot_seq),
            Err(EngineError::ResourceLimit { .. })
        ));
        assert!(view.edges_by_label_id(e).is_err());
        assert!(view
            .degree_stats(1, Direction::Outgoing, Some(&[e]), 100)
            .is_err());
        drop(leases);
        drop(view);
        finish(db);
    }
    #[test]
    fn p45b2_partial_derived_install_is_fenced_and_replayed() {
        for stage in [5, 6, 7] {
            let (dir, db, n, e, f) = fixture();
            db.write_op(&WalOp::UpsertEdge(edge(11, 1, 2, e))).unwrap();
            let old = db.runtime.published.load_full().view.clone();
            old.memtable.fail_graph_install_for_test(stage);
            assert!(matches!(
                db.write_op_batch(&[
                    WalOp::UpsertEdge(edge(11, 3, 4, f)),
                    WalOp::UpsertNode(node(1, n, "after"))
                ]),
                Err(EngineError::CommitOutcome {
                    state: CommitOutcomeState::Indeterminate,
                    ..
                })
            ));
            assert_old(&old, e);
            assert!(db.write_op(&WalOp::UpsertEdge(edge(12, 1, 4, e))).is_err());
            let stats = db.runtime.candidate.as_ref().unwrap().stats.clone();
            drop(old);
            assert!(db.close_fast().is_err());
            drop(db);
            let until = std::time::Instant::now() + Duration::from_secs(20);
            while !matches!(
                stats.read_full().unwrap().close_state,
                ReclamationCloseState::Drained
            ) {
                assert!(std::time::Instant::now() < until);
                std::thread::sleep(Duration::from_millis(1));
            }
            let reopened = DatabaseEngine::open(dir.path(), &DbOptions::default()).unwrap();
            let view = reopened.runtime.published.load_full().view.clone();
            assert!(neighbors(&view, 1, Direction::Outgoing).is_empty());
            assert_eq!(
                neighbors(&view, 3, Direction::Outgoing),
                vec![(11, 4, f, 1.0)]
            );
            assert_eq!(view.get_node_raw(1).unwrap().unwrap().key, "after");
            drop(view);
            finish(reopened);
        }
    }
}

#[cfg(test)]
mod p45b3_tests {
    use super::p45b1_tests::{edge, finish, node};
    use super::real_route_tests::{fixed_family_props, fixed_family_queries};
    use super::*;
    use std::time::{Duration, Instant};
    pub(super) fn fixture() -> (tempfile::TempDir, DatabaseEngine, u32, u32, Vec<(u64, u64)>) {
        let dir = tempfile::tempdir().unwrap();
        let options = DbOptions {
            compact_after_n_flushes: 0,
            ..Default::default()
        };
        let core = EngineCore::open(dir.path(), &options).unwrap();
        let runtime = Arc::new(DbRuntime::new(dir.path().to_owned(), core));
        runtime.install_core_runtime_handle();
        let legacy = DatabaseEngine {
            runtime,
            client: None,
        };
        let n = legacy.ensure_node_label("R").unwrap();
        let e = legacy.ensure_edge_label("E").unwrap();
        let mut ids = Vec::new();
        for (kind, keys) in [
            (SecondaryIndexKind::Equality, vec!["eq"]),
            (SecondaryIndexKind::Range, vec!["range"]),
            (SecondaryIndexKind::Equality, vec!["ceq", "tie"]),
            (SecondaryIndexKind::Range, vec!["crange", "score"]),
        ] {
            let spec = SecondaryIndexSpec {
                fields: keys
                    .into_iter()
                    .map(SecondaryIndexField::property)
                    .collect(),
                kind,
            };
            ids.push((
                legacy
                    .ensure_node_property_index("R", spec.clone())
                    .unwrap()
                    .index_id,
                legacy
                    .ensure_edge_property_index("E", spec)
                    .unwrap()
                    .index_id,
            ));
        }
        let until = Instant::now() + Duration::from_secs(10);
        while !legacy
            .runtime
            .with_core_ref(|c| {
                Ok(c.secondary_index_entries_snapshot()
                    .iter()
                    .all(|e| e.state == SecondaryIndexState::Ready))
            })
            .unwrap()
        {
            assert!(Instant::now() < until);
            std::thread::yield_now();
        }
        legacy
            .batch_upsert_nodes(
                (1..=32)
                    .map(|i| NodeInput {
                        labels: vec!["R".into()],
                        key: format!("n{i}"),
                        props: fixed_family_props(i <= 4),
                        weight: 1.0,
                        dense_vector: None,
                        sparse_vector: None,
                    })
                    .collect(),
            )
            .unwrap();
        legacy
            .batch_upsert_edges(
                (1..=32)
                    .map(|i| EdgeInput {
                        from: i,
                        to: i,
                        label: "E".into(),
                        props: fixed_family_props(i <= 4),
                        weight: 1.0,
                        valid_from: None,
                        valid_to: None,
                    })
                    .collect(),
            )
            .unwrap();
        legacy.flush().unwrap();
        legacy.close_fast().unwrap();
        drop(legacy);
        let db = DatabaseEngine::open(dir.path(), &options).unwrap();
        (dir, db, n, e, ids)
    }
    fn matching_node(id: u64, label: u32, matches: bool) -> NodeRecord {
        let mut n = node(id, label, &format!("n{id}"));
        n.props = fixed_family_props(matches);
        n
    }
    fn matching_edge(id: u64, label: u32, matches: bool) -> EdgeRecord {
        let mut e = edge(id, id, id, label);
        e.props = fixed_family_props(matches);
        e
    }
    fn assert_queries(view: &ReadView, nodes: &[u64], edges: &[u64]) {
        for family in 0..4 {
            let (nq, eq) = fixed_family_queries(family);
            assert_eq!(
                view.query_node_ids_outcome(&nq).unwrap().value.items,
                nodes,
                "node family {family}"
            );
            assert_eq!(
                view.query_edge_ids_outcome(&eq).unwrap().value.edge_ids,
                edges,
                "edge family {family}"
            );
            assert_eq!(
                view.query_nodes_outcome(&nq)
                    .unwrap()
                    .value
                    .items
                    .iter()
                    .map(|n| n.id)
                    .collect::<Vec<_>>(),
                nodes
            );
        }
    }
    #[test]
    fn p45b3_secondary_install_frontiers_and_locked_adapter_independence() {
        for stage in [9, 10, 11, 2, 4] {
            let (_dir, db, n, e, indexes) = fixture();
            db.write_op_batch(&[
                WalOp::UpsertNode(matching_node(100, n, true)),
                WalOp::UpsertEdge(matching_edge(100, e, true)),
            ])
            .unwrap();
            let old = db.runtime.published.load_full().view.clone();
            let catalog = old.memtable.fixed_secondary_catalog().unwrap();
            let (ready, release) = if stage == 4 {
                db.set_runtime_publish_pause()
            } else {
                old.memtable.primary_pause_for_test(stage)
            };
            let writer = db.clone();
            let job = std::thread::spawn(move || {
                writer.write_op_batch(&[
                    WalOp::UpsertNode(matching_node(100, n, false)),
                    WalOp::UpsertEdge(matching_edge(100, e, false)),
                ])
            });
            ready.recv_timeout(Duration::from_secs(10)).unwrap();
            assert_queries(&old, &[1, 2, 3, 4, 100], &[1, 2, 3, 4, 100]);
            let admitted = db.runtime.published.load_full().view.clone();
            assert_queries(&admitted, &[1, 2, 3, 4, 100], &[1, 2, 3, 4, 100]);
            assert_eq!(
                old.memtable
                    .secondary_eq_node_hash_count_at(
                        indexes[0].0,
                        crate::property_value_semantics::hash_prop_equality_key(&PropValue::Int(1)),
                        old.snapshot_seq
                    )
                    .unwrap(),
                1
            );
            release.send(()).unwrap();
            job.join().unwrap().unwrap();
            assert_queries(&old, &[1, 2, 3, 4, 100], &[1, 2, 3, 4, 100]);
            assert_queries(
                &db.runtime.published.load_full().view,
                &[1, 2, 3, 4],
                &[1, 2, 3, 4],
            );
            db.freeze_memtable().unwrap();
            let current = db.runtime.published.load_full().view.clone();
            assert!(SharedArc::ptr_eq(
                &catalog,
                &current.memtable.fixed_secondary_catalog().unwrap()
            ));
            assert_eq!(old.memtable.locked_secondary_slots_for_test(), [0; 4]);
            drop((catalog, old, admitted, current));
            finish(db);
        }
    }
    #[test]
    fn p45b3_refill_and_owned_callback_do_not_block_writer() {
        let (_dir, db, n, e, indexes) = fixture();
        db.write_op_batch(&[
            WalOp::UpsertNode(matching_node(100, n, true)),
            WalOp::UpsertEdge(matching_edge(100, e, true)),
        ])
        .unwrap();
        let old = db.runtime.published.load_full().view.clone();
        let (ready, release) = old.memtable.primary_pause_for_test(12);
        let read = old.clone();
        let index = indexes[0].0;
        let reader = std::thread::spawn(move || {
            read.memtable.refill_secondary_eq_ids_at(
                index,
                crate::property_value_semantics::hash_prop_equality_key(&PropValue::Int(1)),
                read.snapshot_seq,
                std::ops::Bound::Included(0),
                1,
            )
        });
        ready.recv_timeout(Duration::from_secs(10)).unwrap();
        db.write_op(&WalOp::UpsertNode(matching_node(100, n, false)))
            .unwrap();
        release.send(()).unwrap();
        assert_eq!(reader.join().unwrap().unwrap().0, vec![100]);
        let (ready, arrive) = std::sync::mpsc::sync_channel(0);
        let (release, resume) = std::sync::mpsc::sync_channel(0);
        let read = old.clone();
        let index = indexes[1].0;
        let reader = std::thread::spawn(move || {
            let mut values = Vec::new();
            let _ = read
                .memtable
                .for_each_visible_secondary_range_entry_at(
                    index,
                    None,
                    None,
                    None,
                    read.snapshot_seq,
                    &mut |row| {
                        ready.send(()).unwrap();
                        resume.recv().unwrap();
                        values.push(row.1);
                        std::ops::ControlFlow::Continue(())
                    },
                )
                .unwrap();
            values
        });
        arrive.recv_timeout(Duration::from_secs(10)).unwrap();
        db.write_op(&WalOp::UpsertNode(matching_node(101, n, true)))
            .unwrap();
        release.send(()).unwrap();
        assert_eq!(reader.join().unwrap(), vec![100]);
        drop(old);
        finish(db);
    }
    #[test]
    fn p45b3_repeated_operations_keep_sequences_and_secondary_semantics() {
        let (_dir, db, n, e, indexes) = fixture();
        let start = db.engine_seq_for_test() + 1;
        let mut equivalent = matching_node(100, n, true);
        equivalent.props.insert("eq".into(), PropValue::Float(1.0));
        db.write_op_batch(&[
            WalOp::UpsertNode(matching_node(100, n, true)),
            WalOp::UpsertEdge(matching_edge(100, e, true)),
            WalOp::UpsertNode(equivalent),
            WalOp::UpsertNode(matching_node(100, n, false)),
            WalOp::DeleteNode {
                id: 100,
                deleted_at: 3,
            },
            WalOp::UpsertNode(matching_node(100, n, true)),
            WalOp::DeleteEdge {
                id: 100,
                deleted_at: 4,
            },
            WalOp::UpsertEdge(matching_edge(100, e, true)),
        ])
        .unwrap();
        let view = db.runtime.published.load_full().view.clone();
        for (seq, expected) in [
            (start, vec![100]),
            (start + 2, vec![100]),
            (start + 3, vec![]),
            (start + 4, vec![]),
            (start + 5, vec![100]),
        ] {
            assert_eq!(
                view.memtable
                    .find_secondary_eq_nodes_at(indexes[0].0, "eq", &PropValue::Int(1), seq)
                    .unwrap(),
                expected
            );
        }
        let (hashes, ranges, compounds) = view.memtable.secondary_opinions_for_test();
        assert!(hashes > 0 && ranges > 0 && compounds > 0);
        assert_eq!(
            view.memtable.estimated_size(),
            view.memtable.estimated_size_full_for_test()
        );
        assert_queries(&view, &[1, 2, 3, 4, 100], &[1, 2, 3, 4, 100]);
        drop(view);
        finish(db);
    }
    #[test]
    fn p45b3_partial_secondary_install_fences_and_replays() {
        for stage in [9, 10, 11] {
            let (dir, db, n, e, _) = fixture();
            db.write_op_batch(&[
                WalOp::UpsertNode(matching_node(100, n, true)),
                WalOp::UpsertEdge(matching_edge(100, e, true)),
            ])
            .unwrap();
            let old = db.runtime.published.load_full().view.clone();
            old.memtable.fail_graph_install_for_test(stage);
            assert!(matches!(
                db.write_op_batch(&[
                    WalOp::UpsertNode(matching_node(100, n, false)),
                    WalOp::UpsertEdge(matching_edge(100, e, false))
                ]),
                Err(EngineError::CommitOutcome {
                    state: CommitOutcomeState::Indeterminate,
                    ..
                })
            ));
            assert_queries(&old, &[1, 2, 3, 4, 100], &[1, 2, 3, 4, 100]);
            assert!(db
                .write_op(&WalOp::UpsertNode(matching_node(101, n, true)))
                .is_err());
            let stats = db.runtime.candidate.as_ref().unwrap().stats.clone();
            drop(old);
            assert!(db.close_fast().is_err());
            drop(db);
            super::real_route_tests::wait_native(&stats);
            let reopened = DatabaseEngine::open(dir.path(), &DbOptions::default()).unwrap();
            assert_queries(
                &reopened.runtime.published.load_full().view,
                &[1, 2, 3, 4],
                &[1, 2, 3, 4],
            );
            finish(reopened);
        }
    }
    #[test]
    fn p45b3_secondary_pressure_propagates_without_demotion() {
        const CHILD: &str = "P45B3_SECONDARY_PRESSURE_CHILD";
        if std::env::var_os(CHILD).is_none() {
            let out = std::process::Command::new(std::env::current_exe().unwrap())
                .args([
                    "--exact",
                    "engine::p45b3_tests::p45b3_secondary_pressure_propagates_without_demotion",
                    "--nocapture",
                ])
                .env(CHILD, "1")
                .output()
                .unwrap();
            assert!(
                out.status.success(),
                "{}\n{}",
                String::from_utf8_lossy(&out.stdout),
                String::from_utf8_lossy(&out.stderr)
            );
            return;
        }
        let (_dir, db, n, e, indexes) = fixture();
        db.write_op_batch(&[
            WalOp::UpsertNode(matching_node(100, n, true)),
            WalOp::UpsertEdge(matching_edge(100, e, true)),
        ])
        .unwrap();
        let view = db.runtime.published.load_full().view.clone();
        let admission = view.memtable.participant_handle().unwrap();
        let leases = (0..256)
            .map(|_| admission.try_acquire().unwrap())
            .collect::<Vec<_>>();
        for i in 0..4 {
            let (nq, eq) = fixed_family_queries(i);
            assert!(matches!(
                view.query_node_ids_outcome(&nq),
                Err(EngineError::ResourceLimit {
                    resource: "participants",
                    ..
                })
            ));
            assert!(matches!(
                view.query_edge_ids_outcome(&eq),
                Err(EngineError::ResourceLimit {
                    resource: "participants",
                    ..
                })
            ));
        }
        assert!(matches!(
            view.memtable
                .secondary_eq_node_hash_count_at(indexes[0].0, 1, view.snapshot_seq),
            Err(EngineError::ResourceLimit { .. })
        ));
        assert!(view.memtable.secondary_eq_state().is_err());
        drop(leases);
        assert_queries(&view, &[1, 2, 3, 4, 100], &[1, 2, 3, 4, 100]);
        assert!(db
            .list_node_property_indexes()
            .unwrap()
            .iter()
            .all(|i| i.state == SecondaryIndexState::Ready));
        drop(view);
        finish(db);
    }
    #[test]
    fn p45b3_ordered_refills_future_removed_history_and_byte_progress() {
        let (_dir, db, _, _, indexes) = fixture();
        let view = db.runtime.published.load_full().view.clone();
        view.memtable.assert_secondary_cursor_contracts_for_test(
            indexes[0].0,
            indexes[1].0,
            indexes[3].0,
        );
        drop(view);
        finish(db);
    }
    #[test]
    fn p45b3_unchanged_older_memberships_are_complete_in_the_new_generation() {
        let (_dir, db, n, e, indexes) = fixture();
        db.write_op_batch(&[
            WalOp::UpsertNode(matching_node(1, n, true)),
            WalOp::UpsertEdge(matching_edge(1, e, true)),
        ])
        .unwrap();
        let view = db.runtime.published.load_full().view.clone();
        let hash = crate::property_value_semantics::hash_prop_equality_key(&PropValue::Int(1));
        let eq = view.memtable.secondary_eq_state().unwrap();
        let range = view.memtable.secondary_range_state().unwrap();
        let compound = view
            .memtable
            .compound_secondary_state_for_indexes(&[
                indexes[2].0,
                indexes[2].1,
                indexes[3].0,
                indexes[3].1,
            ])
            .unwrap();
        for (index_node, index_edge) in &indexes[..1] {
            for index in [index_node, index_edge] {
                assert!(eq[index][&hash].contains(&1));
            }
        }
        for index in [indexes[1].0, indexes[1].1] {
            assert_eq!(range[&index].len(), 1);
        }
        for rows in compound.values() {
            assert_eq!(rows.len(), 1);
            assert_eq!(rows[0].1, 1);
        }
        assert_queries(&view, &[1, 2, 3, 4], &[1, 2, 3, 4]);
        drop(view);
        db.flush().unwrap();
        db.compact().unwrap();
        assert_queries(
            &db.runtime.published.load_full().view,
            &[1, 2, 3, 4],
            &[1, 2, 3, 4],
        );
        finish(db);
    }
    #[test]
    fn p45b3_null_missing_zero_range_bounds_and_escaped_compound() {
        let (_dir, db, n, e, indexes) = fixture();
        let write = |value: Option<PropValue>| {
            let mut a = matching_node(100, n, true);
            let mut b = matching_edge(100, e, true);
            for p in [&mut a.props, &mut b.props] {
                p.remove("eq");
                p.remove("range");
                if let Some(value) = &value {
                    p.insert("eq".into(), value.clone());
                    p.insert("range".into(), value.clone());
                }
            }
            db.write_op_batch(&[WalOp::UpsertNode(a), WalOp::UpsertEdge(b)])
                .unwrap();
        };
        write(Some(PropValue::Null));
        let held = db.runtime.published.load_full().view.clone();
        write(None);
        let missing = db.runtime.published.load_full().view.clone();
        assert_eq!(
            held.memtable
                .find_secondary_eq_nodes_at(indexes[0].0, "eq", &PropValue::Null, held.snapshot_seq)
                .unwrap(),
            vec![100]
        );
        assert!(missing
            .memtable
            .find_secondary_eq_nodes_at(indexes[0].0, "eq", &PropValue::Null, missing.snapshot_seq)
            .unwrap()
            .is_empty());
        write(Some(PropValue::Int(0)));
        let before = db
            .runtime
            .published
            .load_full()
            .view
            .memtable
            .secondary_opinions_for_test();
        write(Some(PropValue::Float(-0.0)));
        let now = db.runtime.published.load_full().view.clone();
        assert_eq!(before, now.memtable.secondary_opinions_for_test());
        assert_eq!(
            now.memtable
                .find_secondary_eq_nodes_at(
                    indexes[0].0,
                    "eq",
                    &PropValue::Int(0),
                    now.snapshot_seq
                )
                .unwrap(),
            vec![100]
        );
        assert_eq!(
            now.memtable
                .secondary_eq_edge_count_at(
                    indexes[0].1,
                    "eq",
                    &PropValue::Float(0.0),
                    now.snapshot_seq
                )
                .unwrap(),
            1
        );
        let zero =
            crate::property_value_semantics::numeric_range_sort_key_for_value(&PropValue::Int(0))
                .unwrap();
        for index in [indexes[1].0, indexes[1].1] {
            assert_eq!(
                now.memtable
                    .visible_secondary_range_entries_limited(
                        index,
                        Some((zero, true)),
                        Some((zero, true)),
                        None,
                        now.snapshot_seq,
                        Some(1)
                    )
                    .unwrap(),
                vec![(zero, 100)]
            );
            assert!(now
                .memtable
                .visible_secondary_range_entries_limited(
                    index,
                    Some((zero, false)),
                    Some((zero, true)),
                    None,
                    now.snapshot_seq,
                    None
                )
                .unwrap()
                .is_empty());
            assert!(now
                .memtable
                .visible_secondary_range_entries_limited(
                    index,
                    None,
                    Some((zero, true)),
                    Some((zero, 100)),
                    now.snapshot_seq,
                    None
                )
                .unwrap()
                .is_empty());
        }
        let mut a = matching_node(100, n, true);
        a.props
            .insert("crange".into(), PropValue::String("a\0b".into()));
        a.props.insert("score".into(), PropValue::Int(5));
        db.write_op(&WalOp::UpsertNode(a)).unwrap();
        let query = |included| NodeQuery {
            label_filter: Some(NodeLabelFilter {
                labels: vec!["R".into()],
                mode: LabelMatchMode::All,
            }),
            filter: Some(NodeFilterExpr::And(vec![
                NodeFilterExpr::PropertyEquals {
                    key: "crange".into(),
                    value: PropValue::String("a\0b".into()),
                },
                NodeFilterExpr::PropertyRange {
                    key: "score".into(),
                    lower: Some(if included {
                        PropertyRangeBound::Included(PropValue::Int(5))
                    } else {
                        PropertyRangeBound::Excluded(PropValue::Int(5))
                    }),
                    upper: None,
                },
            ])),
            ..Default::default()
        };
        assert_eq!(db.query_node_ids(&query(true)).unwrap().items, vec![100]);
        assert!(db.query_node_ids(&query(false)).unwrap().items.is_empty());
        drop((held, missing, now));
        finish(db);
    }
    #[test]
    fn p45b3_metadata_compound_replacement_and_fixed_output_coverage() {
        let dir = tempfile::tempdir().unwrap();
        let opts = DbOptions {
            compact_after_n_flushes: 0,
            ..Default::default()
        };
        let core = EngineCore::open(dir.path(), &opts).unwrap();
        let runtime = Arc::new(DbRuntime::new(dir.path().to_owned(), core));
        runtime.install_core_runtime_handle();
        let fixture = DatabaseEngine {
            runtime,
            client: None,
        };
        let n = fixture.ensure_node_label("R").unwrap();
        let e = fixture.ensure_edge_label("E").unwrap();
        let ni = fixture
            .ensure_node_property_index(
                "R",
                SecondaryIndexSpec::equality(vec![
                    SecondaryIndexField::node_meta(NodeMetadataIndexField::Key),
                    SecondaryIndexField::property("eq"),
                ]),
            )
            .unwrap();
        let ei = fixture
            .ensure_edge_property_index(
                "E",
                SecondaryIndexSpec::range(vec![
                    SecondaryIndexField::edge_meta(EdgeMetadataIndexField::From),
                    SecondaryIndexField::property("range"),
                ]),
            )
            .unwrap();
        let until = Instant::now() + Duration::from_secs(10);
        while !fixture
            .runtime
            .with_core_ref(|c| {
                Ok(c.secondary_index_entries_snapshot()
                    .iter()
                    .all(|e| e.state == SecondaryIndexState::Ready))
            })
            .unwrap()
        {
            assert!(Instant::now() < until);
            std::thread::yield_now();
        }
        fixture.close_fast().unwrap();
        drop(fixture);
        let db = DatabaseEngine::open(dir.path(), &opts).unwrap();
        let mut a = matching_node(1, n, true);
        a.key = "a\0b".into();
        db.write_op_batch(&[
            WalOp::UpsertNode(a.clone()),
            WalOp::UpsertNode(matching_node(2, n, true)),
            WalOp::UpsertEdge(matching_edge(1, e, true)),
        ])
        .unwrap();
        let old = db.runtime.published.load_full().view.clone();
        let old_tuples = old
            .memtable
            .compound_secondary_state_for_indexes(&[ni.index_id, ei.index_id])
            .unwrap();
        a.key = "new".into();
        let mut b = matching_edge(1, e, false);
        b.from = 2;
        db.write_op_batch(&[WalOp::UpsertNode(a), WalOp::UpsertEdge(b)])
            .unwrap();
        let now = db.runtime.published.load_full().view.clone();
        let tuples = now
            .memtable
            .compound_secondary_state_for_indexes(&[ni.index_id, ei.index_id])
            .unwrap();
        for i in [ni.index_id, ei.index_id] {
            assert_ne!(old_tuples[&i], tuples[&i]);
        }
        let nq = NodeQuery {
            label_filter: Some(NodeLabelFilter {
                labels: vec!["R".into()],
                mode: LabelMatchMode::All,
            }),
            filter: Some(NodeFilterExpr::And(vec![
                NodeFilterExpr::KeyEquals("a\0b".into()),
                NodeFilterExpr::PropertyEquals {
                    key: "eq".into(),
                    value: PropValue::Int(1),
                },
            ])),
            ..Default::default()
        };
        assert_eq!(
            old.query_node_ids_outcome(&nq).unwrap().value.items,
            vec![1]
        );
        assert!(now
            .query_node_ids_outcome(&nq)
            .unwrap()
            .value
            .items
            .is_empty());
        db.flush().unwrap();
        let view = db.runtime.published.load_full().view.clone();
        let mut fake = view.secondary_index_entries[0].clone();
        fake.index_id = 999_999;
        assert!(matches!(
            validate_fixed_ready_output(&view.segments[0], &[fake]),
            Err(EngineError::CorruptRecord(_))
        ));
        drop((old, now, view));
        finish(db);
    }
    #[test]
    fn p45b3_fixed_catalog_producer_survives_generation_retirement() {
        let (_dir, db, n, _, _) = fixture();
        let catalog = db
            .runtime
            .published
            .load_full()
            .view
            .memtable
            .fixed_secondary_catalog()
            .unwrap();
        for id in [100, 101] {
            db.write_op(&WalOp::UpsertNode(matching_node(id, n, true)))
                .unwrap();
            db.flush().unwrap();
            assert!(SharedArc::ptr_eq(
                &catalog,
                &db.runtime
                    .published
                    .load_full()
                    .view
                    .memtable
                    .fixed_secondary_catalog()
                    .unwrap()
            ));
        }
        let stats = db.runtime.candidate.as_ref().unwrap().stats.clone();
        let until = Instant::now() + Duration::from_secs(10);
        loop {
            let s = stats.read_full().unwrap();
            if s.retired_objects == 0 && s.dropping_objects == 0 {
                break;
            }
            assert!(Instant::now() < until);
            std::thread::yield_now();
        }
        stats.reset_arc_read_observer();
        for _ in 0..1000 {
            drop(catalog.clone());
        }
        let (live, notified) = stats.arc_read_observations();
        assert!(live >= 1000);
        assert_eq!(notified, 0, "current catalog must retain producer custody");
        db.close_fast().unwrap();
        drop(db);
        assert!(stats.read().occupied > 0);
        drop(catalog);
        super::real_route_tests::wait_native(&stats);
    }
    #[test]
    fn p45b3_gql_delegated_query_keeps_the_admitted_view() {
        let (_dir, db, n, e, _) = fixture();
        db.write_op_batch(&[
            WalOp::UpsertNode(matching_node(100, n, true)),
            WalOp::UpsertEdge(matching_edge(100, e, true)),
        ])
        .unwrap();
        let view = db.runtime.published.load_full().view.clone();
        let (ready, release) = view.memtable.primary_pause_for_test(14);
        let reader = db.clone();
        let job = std::thread::spawn(move || {
            reader.execute_gql(
                "MATCH (n:R) WHERE n.eq = 1 RETURN id(n)",
                &GqlParams::new(),
                &GqlExecutionOptions::default(),
            )
        });
        ready.recv_timeout(Duration::from_secs(10)).unwrap();
        db.write_op(&WalOp::UpsertNode(matching_node(100, n, false)))
            .unwrap();
        release.send(()).unwrap();
        assert_eq!(job.join().unwrap().unwrap().rows.len(), 5);
        assert_eq!(
            db.execute_gql(
                "MATCH (n:R) WHERE n.eq = 1 RETURN id(n)",
                &GqlParams::new(),
                &GqlExecutionOptions::default()
            )
            .unwrap()
            .rows
            .len(),
            4
        );
        drop(view);
        finish(db);
    }
    #[test]
    fn p45b3_sparse_vector_newest_primary_shadows_frozen_and_segment() {
        let dir = tempfile::tempdir().unwrap();
        let db = DatabaseEngine::open(
            dir.path(),
            &DbOptions {
                compact_after_n_flushes: 0,
                dense_vector: Some(DenseVectorConfig {
                    dimension: 2,
                    metric: DenseMetric::Cosine,
                    hnsw: HnswConfig::default(),
                }),
                ..Default::default()
            },
        )
        .unwrap();
        let n = db.ensure_node_label("R").unwrap();
        let other = db.ensure_node_label("Other").unwrap();
        let mut a = node(1, n, "a");
        a.sparse_vector = Some(vec![(1, 1.0)]);
        a.dense_vector = Some(vec![1.0, 0.0]);
        let mut b = node(2, n, "b");
        b.sparse_vector = Some(vec![(1, 0.5)]);
        b.dense_vector = Some(vec![0.5, 0.5]);
        db.write_op_batch(&[WalOp::UpsertNode(a.clone()), WalOp::UpsertNode(b.clone())])
            .unwrap();
        db.flush().unwrap();
        a.sparse_vector = Some(vec![(1, 2.0)]);
        db.write_op(&WalOp::UpsertNode(a.clone())).unwrap();
        let (ready, release) = db.set_flush_pause();
        db.freeze_memtable().unwrap();
        db.enqueue_one_flush().unwrap();
        ready.recv_timeout(Duration::from_secs(10)).unwrap();
        let held = db.runtime.published.load_full().view.clone();
        let request = VectorSearchRequest {
            mode: VectorSearchMode::Sparse,
            dense_query: None,
            sparse_query: Some(vec![(1, 1.0)]),
            k: 10,
            label_filter: Some(NodeLabelFilter {
                labels: vec!["R".into()],
                mode: LabelMatchMode::Any,
            }),
            ef_search: None,
            scope: None,
            dense_weight: None,
            sparse_weight: None,
            fusion_mode: None,
        };
        a.sparse_vector = None;
        a.dense_vector = None;
        a.label_ids = NodeLabelSet::single(other).unwrap();
        db.write_op(&WalOp::UpsertNode(a)).unwrap();
        let current = db.runtime.published.load_full().view.clone();
        assert_eq!(
            held.vector_search(&request)
                .unwrap()
                .iter()
                .map(|h| h.node_id)
                .collect::<Vec<_>>(),
            vec![1, 2]
        );
        assert_eq!(
            current
                .vector_search(&request)
                .unwrap()
                .iter()
                .map(|h| h.node_id)
                .collect::<Vec<_>>(),
            vec![2]
        );
        for mode in [VectorSearchMode::Dense, VectorSearchMode::Hybrid] {
            let mut r = request.clone();
            r.mode = mode;
            r.dense_query = Some(vec![1.0, 0.0]);
            assert_eq!(
                held.vector_search(&r)
                    .unwrap()
                    .iter()
                    .map(|h| h.node_id)
                    .collect::<Vec<_>>(),
                vec![1, 2]
            );
            assert_eq!(
                current
                    .vector_search(&r)
                    .unwrap()
                    .iter()
                    .map(|h| h.node_id)
                    .collect::<Vec<_>>(),
                vec![2]
            );
        }
        db.write_op(&WalOp::DeleteNode {
            id: 2,
            deleted_at: 3,
        })
        .unwrap();
        db.write_op(&WalOp::UpsertNode(b)).unwrap();
        let resurrected = db.runtime.published.load_full().view.clone();
        assert_eq!(
            resurrected
                .vector_search(&request)
                .unwrap()
                .iter()
                .map(|h| h.node_id)
                .collect::<Vec<_>>(),
            vec![2]
        );
        release.send(()).unwrap();
        drop((held, current, resurrected));
        finish(db);
    }
}

#[cfg(test)]
mod p45b4_tests {
    use super::p45b1_tests::{edge, finish, node};
    use super::*;
    use std::time::{Duration, Instant};
    fn empty_legacy(mt: &Memtable) {
        assert_eq!(mt.locked_primary_slots_for_test(), [0; 4]);
        assert_eq!(mt.locked_graph_slots_for_test(), [0; 10]);
        assert_eq!(mt.locked_secondary_slots_for_test(), [0; 4]);
        assert_eq!(mt.locked_tombstone_slots_for_test(), [0; 2]);
        assert_eq!(mt.estimated_size(), mt.estimated_size_full_for_test());
    }
    #[test]
    fn p45b4_candidate_diagnostics_without_legacy_histories_and_wal_replay() {
        let dir = tempfile::tempdir().unwrap();
        let db = DatabaseEngine::open(dir.path(), &DbOptions::default()).unwrap();
        let n = db.ensure_node_label("N").unwrap();
        let e = db.ensure_edge_label("E").unwrap();
        let legacy = Memtable::new();
        let mut seq = 0;
        for round in 0..4 {
            let ops = vec![
                WalOp::UpsertNode(node(1, n, "one")),
                WalOp::UpsertNode(node(2, n, "two")),
                WalOp::UpsertEdge(edge(1, 1, 2, e)),
                WalOp::DeleteNode {
                    id: 1,
                    deleted_at: round,
                },
                WalOp::DeleteNode {
                    id: 1,
                    deleted_at: round,
                },
                WalOp::DeleteEdge {
                    id: 1,
                    deleted_at: round,
                },
                WalOp::DeleteEdge {
                    id: 1,
                    deleted_at: round,
                },
                WalOp::UpsertNode(node(1, n, "again")),
                WalOp::UpsertEdge(edge(1, 2, 1, e)),
                WalOp::DeleteNode {
                    id: 2,
                    deleted_at: round,
                },
                WalOp::DeleteEdge {
                    id: 99,
                    deleted_at: round,
                },
            ];
            db.write_op_batch(&ops).unwrap();
            for op in &ops {
                seq += 1;
                legacy.apply_op(op, seq);
            }
            let v = db.runtime.published.load_full().view.clone();
            assert_eq!(v.memtable.node_count(), legacy.node_count());
            assert_eq!(v.memtable.edge_count(), legacy.edge_count());
            assert_eq!(
                v.memtable.current_tombstone_counts(),
                legacy.current_tombstone_counts()
            );
            empty_legacy(&v.memtable);
        }
        finish(db); // fast close retains the actual WAL for replay.
        let db = DatabaseEngine::open(dir.path(), &DbOptions::default()).unwrap();
        let v = db.runtime.published.load_full().view.clone();
        assert_eq!(v.memtable.node_count(), legacy.node_count());
        assert_eq!(v.memtable.edge_count(), legacy.edge_count());
        assert_eq!(
            v.memtable.current_tombstone_counts(),
            legacy.current_tombstone_counts()
        );
        empty_legacy(&v.memtable);
        assert_eq!(v.sources().find_node(1).unwrap().unwrap().key, "again");
        assert!(v.sources().find_node(2).unwrap().is_none());
        drop(v);
        db.flush().unwrap();
        let v = db.runtime.published.load_full().view.clone();
        // Deleting an older-source record does not decrement a generation-local live count.
        db.write_op_batch(&[
            WalOp::DeleteNode {
                id: 1,
                deleted_at: 10,
            },
            WalOp::DeleteEdge {
                id: 1,
                deleted_at: 10,
            },
        ])
        .unwrap();
        let current = db.runtime.published.load_full().view.clone();
        assert_eq!(
            (current.memtable.node_count(), current.memtable.edge_count()),
            (0, 0)
        );
        assert_eq!(current.memtable.current_tombstone_counts(), (1, 1));
        empty_legacy(&current.memtable);
        db.write_op_batch(&[
            WalOp::UpsertNode(node(1, n, "restored")),
            WalOp::UpsertEdge(edge(1, 1, 2, e)),
        ])
        .unwrap();
        assert_eq!(
            (current.memtable.node_count(), current.memtable.edge_count()),
            (1, 1)
        );
        assert_eq!(current.memtable.current_tombstone_counts(), (0, 0));
        empty_legacy(&current.memtable);
        drop((v, current));
        finish(db);
    }
    #[test]
    fn p45b4_sequence_zero_private_replay_keeps_only_candidate_opinions() {
        let dir = tempfile::tempdir().unwrap();
        let db = DatabaseEngine::open(dir.path(), &DbOptions::default()).unwrap();
        let v = db.runtime.published.load_full().view.clone();
        for (op, seq) in [
            (
                WalOp::DeleteNode {
                    id: 1,
                    deleted_at: 1,
                },
                0,
            ),
            (WalOp::UpsertNode(node(1, 1, "r")), 1),
            (
                WalOp::DeleteEdge {
                    id: 1,
                    deleted_at: 1,
                },
                2,
            ),
            (WalOp::UpsertEdge(edge(1, 1, 2, 1)), 3),
        ] {
            v.memtable.apply_op_for_test(&op, seq).unwrap();
            empty_legacy(&v.memtable);
        }
        assert_eq!((v.memtable.node_count(), v.memtable.edge_count()), (1, 1));
        assert_eq!(v.memtable.current_tombstone_counts(), (0, 0));
        drop(v);
        finish(db);
    }
    fn assert_closed(tx: &mut WriteTxn) {
        assert!(tx.closed && tx.client.is_none());
        assert!(tx.entries.is_empty() && tx.record_replacements.is_empty());
        assert!(
            tx.gql_return_read_set.node_ids.is_empty()
                && tx.gql_return_read_set.edge_ids.is_empty()
        );
        assert!(tx.graph_op_budget.is_none());
        assert!(tx.snapshot.memtable.is_empty());
        assert!(tx.snapshot.immutable_epochs.is_empty() && tx.snapshot.segments.is_empty());
        let o = &tx.overlay;
        assert!(
            o.node_aliases.is_empty()
                && o.edge_aliases.is_empty()
                && o.nodes_by_local.is_empty()
                && o.node_key_locals.is_empty()
                && o.nodes_by_id.is_empty()
                && o.nodes_by_key.is_empty()
                && o.deleted_node_ids_seen.is_empty()
                && o.edges_by_local.is_empty()
                && o.edge_triple_locals.is_empty()
                && o.edges_by_id.is_empty()
                && o.edges_by_triple.is_empty()
        );
        assert!(matches!(tx.commit(), Err(EngineError::TxnClosed)));
        assert!(matches!(tx.rollback(), Err(EngineError::TxnClosed)));
    }
    #[test]
    fn p45b4_closed_txn_releases_snapshot_after_commit_rollback_and_refusal() {
        let dir = tempfile::tempdir().unwrap();
        let db = DatabaseEngine::open_with_concurrency_options(
            dir.path(),
            &DbOptions::default(),
            &ConcurrencyOptions {
                max_batch_operations: 2,
                ..Default::default()
            },
        )
        .unwrap();
        db.upsert_node("N", "base", UpsertNodeOptions::default())
            .unwrap();
        for mode in 0..4 {
            let mut tx = db.begin_write_txn().unwrap();
            let weak = Arc::downgrade(&tx.snapshot);
            tx.upsert_node("N", "staged", UpsertNodeOptions::default())
                .unwrap();
            // Replace the published outer view; the txn is now the old descriptor's only owner.
            db.upsert_node(
                "N",
                &format!("advance-{mode}"),
                UpsertNodeOptions::default(),
            )
            .unwrap();
            if mode == 0 {
                tx.commit().unwrap();
            } else if mode == 1 {
                tx.rollback().unwrap();
            } else if mode == 2 {
                tx.upsert_node("N", "extra", UpsertNodeOptions::default())
                    .unwrap();
                tx.upsert_node("N", "too-many", UpsertNodeOptions::default())
                    .unwrap();
                assert!(matches!(
                    tx.commit(),
                    Err(EngineError::ResourceLimit {
                        resource: "max_batch_operations",
                        ..
                    })
                ));
            } else {
                tx.get_node(TxnNodeRef::Id(1)).unwrap();
                db.upsert_node(
                    "N",
                    "staged",
                    UpsertNodeOptions {
                        weight: 2.0,
                        ..Default::default()
                    },
                )
                .unwrap();
                assert!(matches!(tx.commit(), Err(EngineError::TxnConflict(_))));
            }
            assert_closed(&mut tx);
            let until = Instant::now() + Duration::from_secs(10);
            while weak.upgrade().is_some() {
                assert!(Instant::now() < until);
                std::thread::yield_now();
            }
        }
        finish(db);
    }
    #[test]
    fn p45b4_exact_return_survives_later_same_id_commit_and_source_publication() {
        let dir = tempfile::tempdir().unwrap();
        let db = DatabaseEngine::open(dir.path(), &DbOptions::default()).unwrap();
        let mut tx = db.begin_write_txn().unwrap();
        tx.upsert_node(
            "N",
            "returned",
            UpsertNodeOptions {
                weight: 3.0,
                ..Default::default()
            },
        )
        .unwrap();
        let (_, view) = tx.commit_with_gql_return_view().unwrap();
        assert_closed(&mut tx);
        let seq = view.snapshot_seq;
        db.upsert_node(
            "N",
            "returned",
            UpsertNodeOptions {
                weight: 7.0,
                ..Default::default()
            },
        )
        .unwrap();
        db.freeze_memtable().unwrap();
        assert!(db.runtime.published.load_full().view.snapshot_seq > seq);
        assert_eq!(view.get_node_raw(1).unwrap().unwrap().weight, 3.0);
        assert_eq!(db.get_node(1).unwrap().unwrap().weight, 7.0);
        drop((tx, view));
        finish(db);
    }
    #[test]
    fn p45b4_public_all_family_reads_complete_during_install() {
        use super::real_route_tests::{fixed_family_props, fixed_family_queries};
        for stage in [1, 5, 7, 9, 10, 11, 2, 4] {
            let (_dir, db, n, e, _) = super::p45b3_tests::fixture();
            let mut a = node(100, n, "n100");
            a.props = fixed_family_props(true);
            a.sparse_vector = Some(vec![(1, 1.0)]);
            let mut b = node(101, n, "n101");
            b.props = fixed_family_props(true);
            let mut x = edge(100, 100, 100, e);
            x.props = fixed_family_props(true);
            db.write_op_batch(&[
                WalOp::UpsertNode(a.clone()),
                WalOp::UpsertNode(b),
                WalOp::UpsertEdge(x.clone()),
            ])
            .unwrap();
            db.freeze_memtable().unwrap();
            let old = db.runtime.published.load_full().view.clone();
            let (ready, release) = if stage == 4 {
                db.set_runtime_publish_pause()
            } else {
                old.memtable.primary_pause_for_test(stage)
            };
            let writer = db.clone();
            a.props = fixed_family_props(false);
            a.sparse_vector = Some(vec![(1, 2.0)]);
            x.to = 101;
            x.props = fixed_family_props(false);
            let job = std::thread::spawn(move || {
                writer.write_op_batch(&[WalOp::UpsertNode(a), WalOp::UpsertEdge(x)])
            });
            ready.recv_timeout(Duration::from_secs(10)).unwrap();
            let reader = db.clone();
            let (tx, rx) = std::sync::mpsc::channel();
            let read = std::thread::spawn(move || {
                assert_eq!(
                    reader.get_node(100).unwrap().unwrap().props,
                    fixed_family_props(true)
                );
                assert_eq!(
                    reader.neighbors(100, &NeighborOptions::default()).unwrap()[0].node_id,
                    100
                );
                let label = NodeQuery {
                    label_filter: Some(NodeLabelFilter {
                        labels: vec!["R".into()],
                        mode: LabelMatchMode::All,
                    }),
                    ..Default::default()
                };
                let mut labels = (1..=32).collect::<Vec<_>>();
                labels.extend([100, 101]);
                assert_eq!(reader.query_node_ids(&label).unwrap().items, labels);
                for family in 0..4 {
                    let (nq, eq) = fixed_family_queries(family);
                    assert_eq!(
                        reader.query_node_ids(&nq).unwrap().items,
                        vec![1, 2, 3, 4, 100, 101]
                    );
                    assert_eq!(
                        reader.query_edge_ids(&eq).unwrap().edge_ids,
                        vec![1, 2, 3, 4, 100]
                    );
                }
                assert_eq!(
                    reader
                        .execute_gql(
                            "MATCH (n:R) WHERE n.eq = 1 RETURN id(n)",
                            &GqlParams::new(),
                            &GqlExecutionOptions::default()
                        )
                        .unwrap()
                        .rows
                        .len(),
                    6
                );
                let request = VectorSearchRequest {
                    mode: VectorSearchMode::Sparse,
                    dense_query: None,
                    sparse_query: Some(vec![(1, 1.0)]),
                    k: 10,
                    label_filter: None,
                    ef_search: None,
                    scope: None,
                    dense_weight: None,
                    sparse_weight: None,
                    fusion_mode: None,
                };
                let hits = reader.vector_search(&request).unwrap();
                assert_eq!(
                    hits.iter().map(|h| h.node_id).collect::<Vec<_>>(),
                    vec![100]
                );
                tx.send(()).unwrap();
            });
            let result = rx.recv_timeout(Duration::from_secs(10));
            release.send(()).unwrap();
            result.expect("public read excluded by install");
            read.join().unwrap();
            job.join().unwrap().unwrap();
            assert_eq!(
                old.get_node_raw(100).unwrap().unwrap().props,
                fixed_family_props(true)
            );
            assert_eq!(
                db.get_node(100).unwrap().unwrap().props,
                fixed_family_props(false)
            );
            empty_legacy(&old.memtable);
            drop(old);
            finish(db);
        }
    }
    #[test]
    fn p45b4_public_reader_pause_and_owned_callback_allow_publication() {
        use super::real_route_tests::{fixed_family_props, fixed_family_queries};
        for role in 0..3 {
            let stage = if role == 0 { 3 } else { 14 };
            let (_dir, db, n, _, _) = super::p45b3_tests::fixture();
            let mut a = node(100, n, "n100");
            a.props = fixed_family_props(true);
            db.write_op(&WalOp::UpsertNode(a.clone())).unwrap();
            let old = db.runtime.published.load_full().view.clone();
            let (ready, release) = old.memtable.primary_pause_for_test(stage);
            let reader = db.clone();
            let job = std::thread::spawn(move || {
                if stage == 3 {
                    assert_eq!(
                        reader.get_node(100).unwrap().unwrap().props,
                        fixed_family_props(true)
                    );
                } else if role == 1 {
                    let (nq, _) = fixed_family_queries(0);
                    assert_eq!(
                        reader.query_node_ids(&nq).unwrap().items,
                        vec![1, 2, 3, 4, 100]
                    );
                } else {
                    assert_eq!(
                        reader
                            .execute_gql(
                                "MATCH (n:R) WHERE n.eq = 1 RETURN id(n)",
                                &GqlParams::new(),
                                &GqlExecutionOptions::default()
                            )
                            .unwrap()
                            .rows
                            .len(),
                        5
                    );
                }
            });
            ready.recv_timeout(Duration::from_secs(10)).unwrap();
            let writer = db.clone();
            a.props = fixed_family_props(false);
            let (tx, rx) = std::sync::mpsc::channel();
            let write = std::thread::spawn(move || {
                tx.send(writer.write_op(&WalOp::UpsertNode(a))).unwrap();
            });
            let result = rx.recv_timeout(Duration::from_secs(10));
            release.send(()).unwrap();
            result.expect("writer excluded by public reader").unwrap();
            job.join().unwrap();
            write.join().unwrap();
            // An owned exported row callback can synchronously submit a write after its session ends.
            let handle = old.memtable.participant_handle().unwrap();
            let mut count = 0;
            let _ = old
                .memtable
                .for_each_visible_node_at(old.snapshot_seq, crate::row_projection::VectorSelection::None, &mut |_| {
                    assert_eq!(handle.snapshot().0, 0);
                    db.upsert_node("R", "callback", UpsertNodeOptions::default())
                        .unwrap();
                    count += 1;
                    ControlFlow::Break(())
                })
                .unwrap();
            assert_eq!(count, 1);
            drop(old);
            finish(db);
        }
    }
    fn take_request(tx: &mut WriteTxn) -> TxnCommitRequest {
        tx.closed = true;
        let request = TxnCommitRequest {
            snapshot: tx.transfer_commit_snapshot(),
            snapshot_seq: tx.snapshot_seq,
            entries: std::mem::take(&mut tx.entries),
            record_replacements: std::mem::take(&mut tx.record_replacements),
            gql_return_read_set: std::mem::take(&mut tx.gql_return_read_set),
            graph_op_budget: tx.graph_op_budget.take(),
        };
        tx.overlay = TxnOverlay::new(tx.edge_uniqueness);
        tx.client.take();
        request
    }
    #[test]
    fn p45b4_dropped_txn_reply_keeps_queued_accepted_custody_through_close() {
        for queued in [true, false] {
            let dir = tempfile::tempdir().unwrap();
            let options = DbOptions::default();
            let db = DatabaseEngine::open_with_concurrency_options(
                dir.path(),
                &options,
                &ConcurrencyOptions {
                    admission_timeout_ms: 10000,
                    ..Default::default()
                },
            )
            .unwrap();
            db.ensure_node_label("N").unwrap();
            db.ensure_edge_label("E").unwrap();
            let mut txn = db.begin_write_txn().unwrap();
            let a = txn
                .upsert_node("N", "a", UpsertNodeOptions::default())
                .unwrap();
            let b = txn
                .upsert_node("N", "b", UpsertNodeOptions::default())
                .unwrap();
            txn.upsert_edge(a, b, "E", UpsertEdgeOptions::default())
                .unwrap();
            let old = Arc::downgrade(&txn.snapshot);
            let captured = db.runtime.published.load_full().view.clone();
            let (ready, release) = captured.memtable.primary_pause_for_test(1);
            let blocker = if queued {
                let d = db.clone();
                let h = std::thread::spawn(move || {
                    d.upsert_node("N", "blocker", UpsertNodeOptions::default())
                });
                ready.recv_timeout(Duration::from_secs(10)).unwrap();
                Some(h)
            } else {
                None
            };
            let request = take_request(&mut txn);
            let admission = db.runtime.admit_candidate_write().unwrap();
            let (reply_tx, reply_rx) = std::sync::mpsc::sync_channel(1);
            drop(reply_rx);
            {
                let mut l = db.runtime.lifecycle.lock().unwrap();
                l.active_non_read_ops += 1;
                l.active_mutating_ops += 1;
                l.pending_core_writes.push_back(QueuedCoreWrite {
                    admission,
                    request: CoreWriteRequest::TxnCommit {
                        request,
                        return_read_view: true,
                    },
                    reply_tx: Some(reply_tx),
                    followup_key: None,
                });
                db.runtime.lifecycle_cv.notify_all();
            }
            if !queued {
                ready.recv_timeout(Duration::from_secs(10)).unwrap();
            }
            assert_closed(&mut txn);
            drop(captured);
            assert!(
                old.upgrade().is_some(),
                "native request must retain its begin view"
            );
            let closer = db.clone();
            let (tx, rx) = std::sync::mpsc::channel();
            let closing = std::thread::spawn(move || tx.send(closer.close_fast()).unwrap());
            let until = Instant::now() + Duration::from_secs(10);
            while db.runtime.open_state() != RuntimeOpenState::Closing {
                assert!(Instant::now() < until);
                std::thread::yield_now();
            }
            assert!(matches!(
                rx.try_recv(),
                Err(std::sync::mpsc::TryRecvError::Empty)
            ));
            release.send(()).unwrap();
            if let Some(h) = blocker {
                h.join().unwrap().unwrap();
            }
            rx.recv_timeout(Duration::from_secs(20)).unwrap().unwrap();
            closing.join().unwrap();
            drop(txn);
            assert!(old.upgrade().is_none());
            let stats = db.runtime.candidate.as_ref().unwrap().stats.clone();
            drop(db);
            super::real_route_tests::wait_native(&stats);
            let db = DatabaseEngine::open(dir.path(), &options).unwrap();
            let a = db.get_node_by_key("N", "a").unwrap().unwrap();
            let b = db.get_node_by_key("N", "b").unwrap().unwrap();
            assert!(db.get_edge_by_triple(a.id, b.id, "E").unwrap().is_some());
            finish(db);
        }
    }
    #[test]
    fn p45b4_sequence_metadata_and_explicit_epoch_keep_distinct_semantics() {
        let dir = tempfile::tempdir().unwrap();
        let db = DatabaseEngine::open(dir.path(), &DbOptions::default()).unwrap();
        let n = db.ensure_node_label("N").unwrap();
        let e = db.ensure_edge_label("E").unwrap();
        let mut x = edge(1, 1, 2, e);
        x.valid_from = 10;
        x.valid_to = 20;
        db.write_op_batch(&[
            WalOp::UpsertNode(node(1, n, "a")),
            WalOp::UpsertNode(node(2, n, "b")),
            WalOp::UpsertEdge(x.clone()),
        ])
        .unwrap();
        let old = db.runtime.published.load_full().view.clone();
        let epoch = NeighborOptions {
            at_epoch: Some(15),
            ..Default::default()
        };
        assert!(old
            .neighbors(1, &NeighborOptions::default())
            .unwrap()
            .is_empty());
        assert_eq!(old.neighbors(1, &epoch).unwrap().len(), 1);
        db.set_prune_policy(
            "age",
            PrunePolicy {
                max_age_ms: Some(1),
                max_weight: None,
                label: Some("N".into()),
            },
        )
        .unwrap();
        let policy = db.runtime.published.load_full().view.clone();
        assert_eq!(policy.snapshot_seq, old.snapshot_seq);
        // Explicit temporal epoch does not rewind the wall-clock node-pruning policy.
        assert!(policy.neighbors(1, &epoch).unwrap().is_empty());
        assert_eq!(old.neighbors(1, &epoch).unwrap().len(), 1);
        db.remove_prune_policy("age").unwrap();
        x.valid_from = 0;
        x.valid_to = i64::MAX;
        db.write_op(&WalOp::UpsertEdge(x)).unwrap();
        assert_eq!(
            db.neighbors(1, &NeighborOptions::default()).unwrap().len(),
            1
        );
        assert!(old
            .neighbors(1, &NeighborOptions::default())
            .unwrap()
            .is_empty());
        assert!(policy.neighbors(1, &epoch).unwrap().is_empty());
        drop((old, policy));
        finish(db);
    }
}

#[cfg(test)]
mod post_p45b_wal_tests {
    use super::p45b1_tests::{finish, node};
    use super::*;
    fn state(db: &DatabaseEngine) -> (u64, u64, u64, Vec<u8>) {
        let core = db.runtime.core.lock().unwrap();
        let core = core.as_ref().unwrap();
        let mut bytes = Vec::new();
        let mut paths = std::fs::read_dir(&core.db_dir)
            .unwrap()
            .map(|e| e.unwrap().path())
            .filter(|p| p.extension().is_some_and(|e| e == "wal"))
            .collect::<Vec<_>>();
        paths.sort();
        for path in paths {
            bytes.extend(std::fs::read(path).unwrap());
        }
        (core.engine_seq, core.next_node_id, core.next_edge_id, bytes)
    }
    #[test]
    fn post_p45b_wal_public_key_excess_and_normalized_overhead_refuse_atomically() {
        for maximum in [16 * 1024 * 1024, 256] {
            let dir = tempfile::tempdir().unwrap();
            let options = DbOptions {
                wal_sync_mode: WalSyncMode::Immediate,
                ..Default::default()
            };
            let db = DatabaseEngine::open_with_concurrency_options(
                dir.path(),
                &options,
                &ConcurrencyOptions {
                    max_batch_bytes: maximum,
                    ..Default::default()
                },
            )
            .unwrap();
            db.ensure_node_label("R").unwrap();
            let before = state(&db);
            let root = db.runtime.published.load_full();
            let labels = db.list_node_labels().unwrap();
            let result = if maximum > 256 {
                let nodes = (0..300)
                    .map(|i| NodeInput {
                        labels: vec!["R".into()],
                        key: format!("{i:03}{}", "x".repeat(64997)),
                        props: BTreeMap::new(),
                        weight: 1.0,
                        dense_vector: None,
                        sparse_vector: None,
                    })
                    .collect();
                db.batch_upsert_nodes(nodes).map(|_| ())
            } else {
                // Raw key/properties fit; normalized token operations and markers do not.
                db.upsert_node(
                    &[
                        "label_one_with_a_long_name",
                        "label_two_with_a_long_name",
                        "label_three_with_a_long_name",
                        "label_four_with_a_long_name",
                        "label_five_with_a_long_name",
                    ][..],
                    "k",
                    UpsertNodeOptions::default(),
                )
                .map(|_| ())
            };
            assert!(
                matches!(result,Err(EngineError::ResourceLimit{resource:"max_batch_bytes",requested,limit,retryable:false})if requested>limit&&limit==maximum as u64)
            );
            assert_eq!(state(&db), before);
            assert!(Arc::ptr_eq(&root, &db.runtime.published.load_full()));
            assert_eq!(db.list_node_labels().unwrap(), labels);
            if maximum == 256 {
                let c = db.runtime.core.lock().unwrap();
                let c = c.as_ref().unwrap().candidate.as_ref().unwrap();
                assert!(c.prepared_frames.is_empty() && c.encode_scratch.is_empty());
                assert!(
                    c.prepared_frames.capacity() <= maximum
                        && c.encode_scratch.capacity() <= maximum
                );
            }
            drop(root);
            let id = db
                .upsert_node("R", "valid-after-refusal", UpsertNodeOptions::default())
                .unwrap();
            assert_eq!(id, before.1);
            finish(db);
            let db = DatabaseEngine::open_with_concurrency_options(
                dir.path(),
                &options,
                &ConcurrencyOptions {
                    max_batch_bytes: maximum,
                    ..Default::default()
                },
            )
            .unwrap();
            assert_eq!(db.get_node(id).unwrap().unwrap().key, "valid-after-refusal");
            finish(db);
        }
    }
    #[test]
    fn post_p45b_wal_failed_preparation_clears_and_releases_exceptional_caches() {
        let dir = tempfile::tempdir().unwrap();
        let db = DatabaseEngine::open(dir.path(), &DbOptions::default()).unwrap();
        let label = db.ensure_node_label("R").unwrap();
        {
            let mut core = db.runtime.core.lock().unwrap();
            let core = core.as_mut().unwrap();
            let prior = IdCounterSnapshot::capture(core);
            for invalid_key in [false, true] {
                let c = core.candidate.as_mut().unwrap();
                c.prepared_frames = vec![0; 2 * 1024 * 1024];
                c.encode_scratch = vec![0; 2 * 1024 * 1024];
                let mut n = node(1, label, "bounded");
                if invalid_key {
                    n.key = "k".repeat(65536);
                } else {
                    n.props.insert(
                        "payload".into(),
                        PropValue::Bytes(vec![5; 17 * 1024 * 1024]),
                    );
                }
                let op = payload::Owned::new(vec![WalOp::UpsertNode(n)]);
                assert!(core.prepare_candidate_frames(&op, prior).is_err());
                let c = core.candidate.as_ref().unwrap();
                assert!(c.prepared_frames.is_empty() && c.encode_scratch.is_empty());
                assert_eq!(
                    (c.prepared_frames.capacity(), c.encode_scratch.capacity()),
                    (0, 0)
                );
            }
            let c = core.candidate.as_mut().unwrap();
            c.prepared_frames = vec![1; 512];
            c.encode_scratch = vec![2; 512];
            let bad = WalOp::EnsureNodeLabel {
                label: "x".repeat(65536),
                label_id: 2,
            };
            assert!(core.prepare_candidate_frames(&[bad], prior).is_err());
            let c = core.candidate.as_ref().unwrap();
            assert!(c.prepared_frames.is_empty() && c.encode_scratch.is_empty());
            assert_eq!(
                (c.prepared_frames.capacity(), c.encode_scratch.capacity()),
                (512, 512)
            );
        }
        db.upsert_node("R", "good", UpsertNodeOptions::default())
            .unwrap();
        finish(db);
    }
    #[test]
    fn post_p45b_wal_raw_lower_bound_covers_node_forms_and_checked_overflow() {
        let input = NodeInput {
            labels: vec!["RepeatedLabel".into(); 10],
            key: "k".repeat(100),
            props: BTreeMap::new(),
            weight: 1.0,
            dense_vector: None,
            sparse_vector: None,
        };
        let batch = CoreWriteRequest::BatchUpsertNodes {
            inputs: vec![input.clone(), input.clone()],
        };
        assert!(validate_candidate_input(&batch, 201).is_err());
        assert!(validate_candidate_input(&batch, 202).is_ok());
        let patch = CoreWriteRequest::GraphPatch {
            patch: GraphPatch {
                upsert_nodes: vec![input.clone(), input],
                ..Default::default()
            },
        };
        assert!(validate_candidate_input(&patch, 201).is_err());
        let op = WalOp::UpsertNode(node(1, 1, &"k".repeat(100)));
        let one = CoreWriteRequest::WriteOp { op: op.clone() };
        assert!(validate_candidate_input(&one, 100).is_err());
        let two = CoreWriteRequest::WriteOpBatch {
            ops: vec![op.clone(), op],
        };
        assert!(validate_candidate_input(&two, 201).is_err());
        let mut sink = InputWireBound {
            bytes: usize::MAX,
            maximum: usize::MAX,
            exceeded: false,
        };
        assert!(std::io::Write::write(&mut sink, &[1]).is_err());
        assert!(sink.exceeded);
    }
    #[test]
    fn checked_property_raw_limits_preserve_all_mutation_routes_and_reopen() {
        super::post45_read_tests::ready_participants_for_fixture();
        for sync in [WalSyncMode::default(), WalSyncMode::Immediate] {
            let dir = tempfile::tempdir().unwrap();
            let options = DbOptions {
                edge_uniqueness: true,
                wal_sync_mode: sync,
                ..Default::default()
            };
            let limits = ConcurrencyOptions::default().with_max_batch_bytes(1024);
            let db = DatabaseEngine::open_with_concurrency_options(dir.path(), &options, &limits)
                .unwrap();
            let a = db
                .upsert_node("R", "a", UpsertNodeOptions::default())
                .unwrap();
            let b = db
                .upsert_node("R", "b", UpsertNodeOptions::default())
                .unwrap();
            db.ensure_edge_label("E").unwrap();
            let oversized = || BTreeMap::from([("blob".into(), PropValue::Bytes(vec![255; 1024]))]);
            let node_input = |key: &str, props| NodeInput {
                labels: vec!["NewRaw".into()],
                key: key.into(),
                props,
                weight: 1.,
                dense_vector: None,
                sparse_vector: None,
            };
            let edge_input = |props| EdgeInput {
                from: a,
                to: b,
                label: "E".into(),
                props,
                weight: 1.,
                valid_from: None,
                valid_to: None,
            };
            for route in 0..8 {
                let before = state(&db);
                let published = db.runtime.published.load_full();
                let labels = db.list_node_labels().unwrap();
                let result: Result<(), EngineError> = match route {
                    0 => db
                        .upsert_node(
                            "NewRaw",
                            "big",
                            UpsertNodeOptions {
                                props: oversized(),
                                ..Default::default()
                            },
                        )
                        .map(|_| ()),
                    1 => db
                        .upsert_edge(
                            a,
                            b,
                            "E",
                            UpsertEdgeOptions {
                                props: oversized(),
                                ..Default::default()
                            },
                        )
                        .map(|_| ()),
                    2 => db
                        .batch_upsert_nodes(vec![
                            node_input("same", oversized()),
                            node_input("same", oversized()),
                        ])
                        .map(|_| ()),
                    3 => db
                        .batch_upsert_edges(vec![edge_input(oversized()), edge_input(oversized())])
                        .map(|_| ()),
                    4 => db
                        .graph_patch(GraphPatch {
                            upsert_nodes: vec![
                                node_input("patch", oversized()),
                                node_input("patch", oversized()),
                            ],
                            ..Default::default()
                        })
                        .map(|_| ()),
                    5 => {
                        let mut tx = db.begin_write_txn().unwrap();
                        tx.upsert_node(
                            "NewRaw",
                            "txn",
                            UpsertNodeOptions {
                                props: oversized(),
                                ..Default::default()
                            },
                        )
                        .map(|_| ())
                    }
                    6 => {
                        let mut tx = db.begin_write_txn().unwrap();
                        tx.upsert_edge(
                            TxnNodeRef::Id(a),
                            TxnNodeRef::Id(b),
                            "E",
                            UpsertEdgeOptions {
                                props: oversized(),
                                ..Default::default()
                            },
                        )
                        .map(|_| ())
                    }
                    _ => db
                        .execute_gql(
                            "CREATE (n:NewRaw {elementKey: 'gql-big', blob: $blob}) RETURN id(n)",
                            &GqlParams::from([(
                                "blob".into(),
                                GqlParamValue::Bytes(vec![255; 1024]),
                            )]),
                            &GqlExecutionOptions::default(),
                        )
                        .map(|_| ()),
                };
                assert!(
                    matches!(
                        result,
                        Err(EngineError::ResourceLimit {
                            resource: "max_batch_bytes",
                            retryable: false,
                            ..
                        })
                    ),
                    "route {route}: {result:?}"
                );
                assert_eq!(state(&db), before, "route {route}");
                assert!(Arc::ptr_eq(&published, &db.runtime.published.load_full()));
                assert_eq!(db.list_node_labels().unwrap(), labels);
            }
            let small = BTreeMap::from([(
                "nested".into(),
                PropValue::Array(vec![
                    PropValue::Int(127),
                    PropValue::UInt(128),
                    PropValue::Float(-0.0),
                    PropValue::Bytes(vec![0, 127, 128, 255]),
                ]),
            )]);
            let ids = db
                .batch_upsert_nodes(vec![
                    node_input("valid", small.clone()),
                    node_input("valid", small.clone()),
                ])
                .unwrap();
            assert_eq!(ids[0], ids[1]);
            db.graph_patch(GraphPatch {
                upsert_nodes: vec![
                    node_input("patch-valid", small.clone()),
                    node_input("patch-valid", small.clone()),
                ],
                ..Default::default()
            })
            .unwrap();
            let edges = db
                .batch_upsert_edges(vec![edge_input(small.clone()), edge_input(small.clone())])
                .unwrap();
            assert_eq!(edges[0], edges[1]);
            let mut tx = db.begin_write_txn().unwrap();
            tx.upsert_node(
                "R",
                "txn-valid",
                UpsertNodeOptions {
                    props: small.clone(),
                    ..Default::default()
                },
            )
            .unwrap();
            tx.commit().unwrap();
            drop(tx);
            db.execute_gql(
                "CREATE (n:NewRaw {elementKey: 'gql-valid', blob: $blob}) RETURN id(n)",
                &GqlParams::from([("blob".into(), GqlParamValue::Bytes(vec![0, 127, 128, 255]))]),
                &GqlExecutionOptions::default(),
            )
            .unwrap();
            db.close().unwrap();
            let stats = db.runtime.candidate.as_ref().unwrap().stats.clone();
            drop(db);
            super::real_route_tests::wait_native(&stats);
            let db = DatabaseEngine::open_with_concurrency_options(dir.path(), &options, &limits)
                .unwrap();
            assert_eq!(db.get_node(ids[0]).unwrap().unwrap().props, small);
            assert_eq!(
                db.get_node_by_key("NewRaw", "patch-valid")
                    .unwrap()
                    .unwrap()
                    .props,
                small
            );
            assert_eq!(
                db.get_node_by_key("R", "txn-valid").unwrap().unwrap().props,
                small
            );
            assert_eq!(db.get_edge(edges[0]).unwrap().unwrap().props, small);
            assert_eq!(
                db.get_node_by_key("NewRaw", "gql-valid")
                    .unwrap()
                    .unwrap()
                    .props
                    .get("blob"),
                Some(&PropValue::Bytes(vec![0, 127, 128, 255]))
            );
            finish(db);
        }
    }
}

#[cfg(test)]
mod post_p45b_preparation_tests {
    use super::p45b1_tests::{edge, finish, node};
    use super::*;
    #[test]
    fn post_p45b_empty_catalog_repeated_borrowed_node_edge_opinions() {
        let dir = tempfile::tempdir().unwrap();
        let db = DatabaseEngine::open(dir.path(), &DbOptions::default()).unwrap();
        let n = db.ensure_node_label("N").unwrap();
        let e = db.ensure_edge_label("E").unwrap();
        db.write_op_batch(&[
            WalOp::UpsertNode(node(1, n, "a")),
            WalOp::UpsertNode(node(2, n, "b")),
            WalOp::UpsertNode(node(3, n, "c")),
            WalOp::UpsertEdge(edge(11, 1, 2, e)),
        ])
        .unwrap();
        let old = db.runtime.published.load_full().view.clone();
        assert!(old
            .memtable
            .fixed_secondary_catalog()
            .unwrap()
            .entries
            .is_empty());
        let mut updated = node(1, n, "new");
        updated.updated_at = 9;
        updated
            .props
            .insert("large".into(), PropValue::String("p".repeat(8192)));
        let mut moved = edge(11, 3, 1, e);
        moved.props = updated.props.clone();
        db.write_op_batch(&[
            WalOp::UpsertNode(updated.clone()),
            WalOp::UpsertNode(updated.clone()),
            WalOp::DeleteNode {
                id: 1,
                deleted_at: 10,
            },
            WalOp::UpsertNode(updated),
            WalOp::UpsertEdge(moved.clone()),
            WalOp::DeleteEdge {
                id: 11,
                deleted_at: 10,
            },
            WalOp::UpsertEdge(moved.clone()),
            WalOp::UpsertEdge(moved),
        ])
        .unwrap();
        assert_eq!(old.get_node_raw(1).unwrap().unwrap().key, "a");
        assert_eq!(old.sources().find_edge(11).unwrap().unwrap().from, 1);
        assert_eq!(db.get_node(1).unwrap().unwrap().key, "new");
        assert_eq!(db.get_edge(11).unwrap().unwrap().from, 3);
        let now = db.runtime.published.load_full().view.clone();
        assert_eq!(now.memtable.participant_handle().unwrap().snapshot().0, 0);
        assert_eq!(now.memtable.current_tombstone_counts(), (0, 0));
        assert_eq!(
            now.memtable.estimated_size(),
            now.memtable.estimated_size_full_for_test()
        );
        assert_eq!(now.memtable.locked_secondary_slots_for_test(), [0; 4]);
        drop((old, now));
        db.flush().unwrap();
        finish(db);
        let db = DatabaseEngine::open(dir.path(), &DbOptions::default()).unwrap();
        assert_eq!(db.get_node(1).unwrap().unwrap().key, "new");
        assert_eq!(
            db.get_edge(11).unwrap().unwrap().props.get("large"),
            Some(&PropValue::String("p".repeat(8192)))
        );
        finish(db);
    }
    #[test]
    fn post_p45b_zero_four_snapshots_across_eight_rotations_release_and_drain() {
        for pins in [0, 4] {
            let dir = tempfile::tempdir().unwrap();
            let db = DatabaseEngine::open(dir.path(), &DbOptions::default()).unwrap();
            let ids = db
                .batch_upsert_nodes(
                    (0..4)
                        .map(|i| NodeInput {
                            labels: vec!["N".into()],
                            key: format!("k{i}"),
                            props: BTreeMap::from([("version".into(), PropValue::Int(0))]),
                            weight: 1.0,
                            dense_vector: None,
                            sparse_vector: None,
                        })
                        .collect(),
                )
                .unwrap();
            db.flush().unwrap();
            let mut held = Vec::new();
            for block in 0..8 {
                if pins == 4 && block % 2 == 0 {
                    held.push((db.begin_write_txn().unwrap(), block * 2));
                }
                for version in [block * 2 + 1, block * 2 + 2] {
                    let rows = db
                        .batch_upsert_nodes(
                            (0..4)
                                .map(|i| NodeInput {
                                    labels: vec!["N".into()],
                                    key: format!("k{i}"),
                                    props: BTreeMap::from([(
                                        "version".into(),
                                        PropValue::Int(version),
                                    )]),
                                    weight: 1.0,
                                    dense_vector: None,
                                    sparse_vector: None,
                                })
                                .collect(),
                        )
                        .unwrap();
                    assert_eq!(rows, ids);
                }
                db.flush().unwrap();
            }
            for (tx, version) in &held {
                for &id in &ids {
                    assert_eq!(
                        tx.get_node(TxnNodeRef::Id(id))
                            .unwrap()
                            .unwrap()
                            .props
                            .get("version"),
                        Some(&PropValue::Int(*version))
                    );
                }
            }
            for (tx, _) in &mut held {
                tx.rollback().unwrap();
            }
            db.close().unwrap();
            let until = std::time::Instant::now() + std::time::Duration::from_secs(10);
            loop {
                let s = db.reclamation_stats().unwrap();
                if matches!(s.close_state, ReclamationCloseState::Drained) {
                    assert_eq!(
                        (s.registered_objects, s.retired_objects, s.dropping_objects),
                        (0, 0, 0)
                    );
                    break;
                }
                assert!(std::time::Instant::now() < until);
                std::thread::yield_now();
            }
            assert_eq!(held.len(), pins);
            drop((held, db));
        }
    }
}

#[cfg(all(test, feature = "production-concurrency"))]
mod p45c1_storage_tests {
    use super::*;
    use crate::segment_writer::{
        write_segment_without_degree_sidecar_for_test,
        write_segment_without_degree_sidecar_with_secondary_indexes_for_test,
    };
    use std::time::Duration;
    fn node(id: u64) -> NodeRecord {
        NodeRecord {
            id,
            label_ids: NodeLabelSet::single(1).unwrap(),
            key: format!("key-{id}"),
            props: BTreeMap::from([("p".into(), PropValue::String("v".repeat(384)))]),
            created_at: 10,
            updated_at: 20,
            weight: 1.,
            dense_vector: None,
            sparse_vector: None,
            last_write_seq: 0,
        }
    }
    fn payload(
        path: &Path,
        record: &crate::segment_components::SegmentComponentRecordV1,
    ) -> Vec<u8> {
        use crate::segment_components::{
            ComponentHandleV1, COMPONENT_IDENTITY_HEADER_LEN, PACKED_CORE_FILENAME,
        };
        match &record.handle {
            ComponentHandleV1::PackedRange { offset, len, .. } => {
                let bytes = std::fs::read(path.join(PACKED_CORE_FILENAME)).unwrap();
                let start = COMPONENT_IDENTITY_HEADER_LEN + *offset as usize;
                bytes[start..start + *len as usize].to_vec()
            }
            ComponentHandleV1::ExternalFile {
                relative_path,
                payload_offset,
                payload_len,
            } => {
                let bytes = std::fs::read(path.join(relative_path)).unwrap();
                bytes[*payload_offset as usize..(*payload_offset + *payload_len) as usize].to_vec()
            }
        }
    }
    #[test]
    fn p45c1_streaming_wire_statistics_and_negative_only_continuation() {
        let root = tempfile::tempdir().unwrap();
        let db = DatabaseEngine::open(
            root.path(),
            &DbOptions {
                memtable_flush_threshold: 0,
                compact_after_n_flushes: 0,
                ..Default::default()
            },
        )
        .unwrap();
        let (_, view) = db.runtime.published_snapshot().unwrap();
        let mt = &view.view.memtable;
        let legacy = Memtable::new();
        let mut seq = 0u64;
        let mut apply = |op: WalOp| {
            seq += 1;
            mt.apply_op_for_test(&op, seq).unwrap();
            legacy.apply_op(&op, seq);
        };
        // Long negative spans precede live data; ID 1 is resurrected. Hot history and
        // resurrection must not restart the cursor or emit an older opinion.
        for id in 1..8200 {
            apply(WalOp::DeleteNode { id, deleted_at: 20 });
        }
        for id in 8200..11000 {
            apply(WalOp::UpsertNode(node(id)));
        }
        for _ in 0..30 {
            apply(WalOp::UpsertNode(node(8200)));
        }
        apply(WalOp::UpsertNode(node(1)));
        let mut big = node(11000);
        big.props.insert(
            "oversized".into(),
            PropValue::String("b".repeat(2 * 1024 * 1024)),
        );
        big.props.insert("null".into(), PropValue::Null);
        big.props.insert(
            "nested".into(),
            PropValue::Array(vec![
                PropValue::Int(7),
                PropValue::Map(BTreeMap::from([("a".into(), PropValue::Bool(true))])),
            ]),
        );
        for i in 0..1030 {
            big.props
                .insert(format!("candidate-{i}"), PropValue::Int(i));
        }
        big.dense_vector = Some(vec![1., 2., 3.]);
        big.sparse_vector = Some(vec![(2, 1.), (7, 0.5)]);
        apply(WalOp::UpsertNode(big));
        for id in 1..20 {
            apply(WalOp::UpsertEdge(EdgeRecord {
                id,
                from: 8200,
                to: if id % 2 == 0 { 8200 } else { 8201 },
                label_id: 1,
                props: BTreeMap::from([(
                    "e".into(),
                    PropValue::String("e".repeat(if id == 1 { 2 * 1024 * 1024 } else { 2048 })),
                )]),
                created_at: 10,
                updated_at: 20,
                weight: 1.,
                valid_from: 0,
                valid_to: i64::MAX,
                last_write_seq: 0,
            }));
        }
        apply(WalOp::DeleteEdge {
            id: 3,
            deleted_at: 21,
        });
        // End the mutable closure borrows before sealing the fixture.
        #[expect(
            clippy::drop_non_drop,
            reason = "Explicitly release closure-captured mutable fixture borrows"
        )]
        drop(apply);
        mt.seal_export(seq);
        let chunk = mt.export_node_chunk(seq, (None, 11000)).unwrap();
        assert!(chunk.records.len() <= 1);
        assert!(!chunk.exhausted);
        assert_eq!(chunk.last, Some(4096));
        drop(chunk);
        let negative = mt.export_node_chunk(seq, (Some(4096), 11000)).unwrap();
        assert!(negative.records.is_empty());
        assert!(!negative.exhausted);
        assert_eq!(negative.last, Some(8192));
        drop(negative);
        let a = root.path().join("legacy");
        let f = root.path().join("stream");
        let ia = write_segment_without_degree_sidecar_for_test(&a, 77, &legacy, None).unwrap();
        let iff = write_segment_without_degree_sidecar_for_test(&f, 77, mt, None).unwrap();
        assert_eq!(ia.segment_data_id, iff.segment_data_id);
        use crate::segment_components::{
            decode_manifest_envelope, SegmentComponentKind, SEGMENT_COMPONENT_MANIFEST_FILENAME,
        };
        let am = decode_manifest_envelope(
            &std::fs::read(a.join(SEGMENT_COMPONENT_MANIFEST_FILENAME)).unwrap(),
        )
        .unwrap();
        let fm = decode_manifest_envelope(
            &std::fs::read(f.join(SEGMENT_COMPONENT_MANIFEST_FILENAME)).unwrap(),
        )
        .unwrap();
        for ar in &am.components {
            if matches!(
                ar.kind,
                SegmentComponentKind::PlannerStats | SegmentComponentKind::PackedSegmentContainer
            ) {
                continue;
            }
            let fr = fm.components.iter().find(|r| r.kind == ar.kind).unwrap();
            assert_eq!(payload(&a, ar), payload(&f, fr), "{:?}", ar.kind);
            assert_eq!(ar.payload_digest, fr.payload_digest, "{:?}", ar.kind);
        }
        let ra = SegmentReader::open_with_info(&a, &ia, None, &[]).unwrap();
        let rf = SegmentReader::open_with_info(&f, &iff, None, &[]).unwrap();
        let mut sa = ra.planner_stats().unwrap().clone();
        let sf = rf.planner_stats().unwrap();
        sa.built_at_ms = sf.built_at_ms;
        assert_eq!(&sa, sf);
        assert!(sf.general_property_stats_complete);
        drop((ra, rf, view));
        let stats = db.runtime.candidate.as_ref().unwrap().stats.clone();
        db.close_fast().unwrap();
        let until = Instant::now() + Duration::from_secs(10);
        while !matches!(
            stats.read_full().unwrap().close_state,
            ReclamationCloseState::Drained
        ) {
            assert!(Instant::now() < until);
            std::thread::sleep(Duration::from_millis(1));
        }
    }
    #[test]
    fn p45c1_empty_node_edge_and_tombstone_only_exports() {
        for mode in 0..4 {
            let root = tempfile::tempdir().unwrap();
            let db = DatabaseEngine::open(root.path(), &DbOptions::default()).unwrap();
            let (_, view) = db.runtime.published_snapshot().unwrap();
            let mt = &view.view.memtable;
            if mode == 1 {
                mt.apply_op_for_test(&WalOp::UpsertNode(node(1)), 1)
                    .unwrap();
            }
            if mode == 2 {
                mt.apply_op_for_test(
                    &WalOp::UpsertEdge(EdgeRecord {
                        id: 1,
                        from: 1,
                        to: 1,
                        label_id: 1,
                        props: BTreeMap::new(),
                        created_at: 0,
                        updated_at: 0,
                        weight: 1.,
                        valid_from: 0,
                        valid_to: i64::MAX,
                        last_write_seq: 0,
                    }),
                    1,
                )
                .unwrap();
            }
            if mode == 3 {
                mt.apply_op_for_test(
                    &WalOp::DeleteNode {
                        id: 1,
                        deleted_at: 1,
                    },
                    1,
                )
                .unwrap();
                mt.apply_op_for_test(
                    &WalOp::DeleteEdge {
                        id: 1,
                        deleted_at: 1,
                    },
                    2,
                )
                .unwrap();
            }
            let unsealed = root.path().join("unsealed");
            assert!(
                write_segment_without_degree_sidecar_for_test(&unsealed, 22, mt, None).is_err()
            );
            assert!(
                !unsealed.exists(),
                "unsealed export must refuse before filesystem mutation"
            );
            mt.seal_export(2);
            let path = root.path().join("output");
            let info = write_segment_without_degree_sidecar_with_secondary_indexes_for_test(
                &path,
                22,
                mt,
                None,
                &[],
            )
            .unwrap();
            let reader = SegmentReader::open_with_info(&path, &info, None, &[]).unwrap();
            assert_eq!(info.node_count, if mode == 1 { 1 } else { 0 });
            assert_eq!(info.edge_count, if mode == 2 { 1 } else { 0 });
            drop((reader, view));
            let stats = db.runtime.candidate.as_ref().unwrap().stats.clone();
            db.close_fast().unwrap();
            let until = Instant::now() + Duration::from_secs(10);
            while !matches!(
                stats.read_full().unwrap().close_state,
                ReclamationCloseState::Drained
            ) {
                assert!(Instant::now() < until);
                std::thread::sleep(Duration::from_millis(1));
            }
        }
    }
    fn copy_tree(from: &Path, to: &Path) {
        std::fs::create_dir_all(to).unwrap();
        for entry in std::fs::read_dir(from).unwrap() {
            let e = entry.unwrap();
            if e.file_type().unwrap().is_dir() {
                copy_tree(&e.path(), &to.join(e.file_name()));
            } else {
                std::fs::copy(e.path(), to.join(e.file_name())).unwrap();
            }
        }
    }
    fn close_native(db: DatabaseEngine) {
        let stats = db.runtime.candidate.as_ref().unwrap().stats.clone();
        db.close_fast().unwrap();
        let until = Instant::now() + Duration::from_secs(10);
        while !matches!(
            stats.read_full().unwrap().close_state,
            ReclamationCloseState::Drained
        ) {
            assert!(Instant::now() < until);
            std::thread::sleep(Duration::from_millis(1));
        }
    }
    #[test]
    fn p45c1_fallback_wal_and_compaction_inputs_survive_real_cleanup() {
        let root = tempfile::tempdir().unwrap();
        let options = DbOptions {
            memtable_flush_threshold: 0,
            compact_after_n_flushes: 0,
            ..Default::default()
        };
        let db = DatabaseEngine::open(root.path(), &options).unwrap();
        db.upsert_node("R", "first", UpsertNodeOptions::default())
            .unwrap();
        db.flush().unwrap();
        // Successful flush must keep the WAL required by the previous manifest.
        let previous: ManifestState = serde_json::from_slice(
            &std::fs::read(root.path().join(crate::manifest::MANIFEST_PREV)).unwrap(),
        )
        .unwrap();
        assert_eq!(previous.pending_flush_epochs.len(), 1);
        let old_wal = wal_generation_path(
            root.path(),
            previous.pending_flush_epochs[0].wal_generation_id,
        );
        assert!(old_wal.exists());
        let fallback = tempfile::tempdir().unwrap();
        copy_tree(root.path(), fallback.path());
        std::fs::write(
            fallback.path().join(crate::manifest::MANIFEST_CURRENT),
            b"corrupt current",
        )
        .unwrap();
        let recovered = DatabaseEngine::open(fallback.path(), &options).unwrap();
        assert!(recovered.get_node_by_key("R", "first").unwrap().is_some());
        close_native(recovered);
        db.upsert_node("R", "second", UpsertNodeOptions::default())
            .unwrap();
        db.flush().unwrap();
        assert!(
            !old_wal.exists(),
            "superseded fallback WAL becomes eligible at the next flush"
        );
        let old: Vec<_> = db
            .runtime
            .with_core_ref(|c| {
                Ok(c.segments
                    .iter()
                    .map(|s| segment_dir(root.path(), s.segment_id))
                    .collect::<Vec<_>>())
            })
            .unwrap();
        assert_eq!(old.len(), 2);
        db.compact().unwrap().unwrap();
        db.runtime
            .with_core_mut(|c| {
                c.retry_deferred_segment_cleanup();
                Ok(())
            })
            .unwrap();
        assert!(
            old.iter().all(|p| p.exists()),
            "manifest.prev protects inputs even without a retained read view"
        );
        let fallback = tempfile::tempdir().unwrap();
        copy_tree(root.path(), fallback.path());
        std::fs::write(
            fallback.path().join(crate::manifest::MANIFEST_CURRENT),
            b"corrupt current",
        )
        .unwrap();
        let recovered = DatabaseEngine::open(fallback.path(), &options).unwrap();
        for key in ["first", "second"] {
            assert!(recovered.get_node_by_key("R", key).unwrap().is_some());
        }
        close_native(recovered);
        // Two manifest advancements release previous input references. Pin-free
        // retirement may lag the reclaimer, so wait for the actual predicate.
        for label in ["AdvanceOne", "AdvanceTwo"] {
            db.set_prune_policy(
                label,
                PrunePolicy {
                    max_age_ms: None,
                    max_weight: Some(0.),
                    label: None,
                },
            )
            .unwrap();
        }
        let until = Instant::now() + Duration::from_secs(10);
        while old.iter().any(|p| p.exists()) {
            db.runtime
                .with_core_mut(|c| {
                    c.retry_deferred_segment_cleanup();
                    Ok(())
                })
                .unwrap();
            assert!(Instant::now() < until);
            std::thread::sleep(Duration::from_millis(1));
        }
        close_native(db);
    }
    #[test]
    fn p45c1_delayed_flush_keeps_later_epochs_tokens_and_sealed_views() {
        let root = tempfile::tempdir().unwrap();
        let options = DbOptions {
            memtable_flush_threshold: 0,
            compact_after_n_flushes: 0,
            ..Default::default()
        };
        let db = DatabaseEngine::open(root.path(), &options).unwrap();
        let id = db
            .upsert_node("R", "first", UpsertNodeOptions::default())
            .unwrap();
        let (_, held) = db.runtime.published_snapshot().unwrap();
        let (ready, release) = db.set_flush_publish_pause();
        // Queue asynchronous work directly: an explicit flush barrier owns
        // the coordinator until completion, so parking it would prevent the
        // intended intervening public writes in this fixture.
        db.runtime
            .with_core_mut(|c| {
                c.freeze_memtable()?;
                c.ensure_bg_flush_worker();
                c.enqueue_flush()?;
                Ok(())
            })
            .unwrap();
        ready.recv_timeout(Duration::from_secs(5)).unwrap();
        let second = db
            .upsert_node("R", "second", UpsertNodeOptions::default())
            .unwrap();
        db.ensure_node_label("WalOnlyLater").unwrap();
        db.runtime
            .with_core_mut(|c| {
                c.freeze_memtable()?;
                c.enqueue_flush()?;
                Ok(())
            })
            .unwrap();
        assert_eq!(
            held.view.memtable.sealed_export().unwrap().seq,
            held.view.snapshot_seq
        );
        assert!(held.view.get_node(id).unwrap().is_some());
        assert!(held.view.get_node(second).unwrap().is_none());
        release.send(()).unwrap();
        db.flush().unwrap();
        assert!(db.get_node(second).unwrap().is_some());
        drop(held);
        close_native(db);
        let reopened = DatabaseEngine::open(root.path(), &options).unwrap();
        for key in ["first", "second"] {
            assert!(reopened.get_node_by_key("R", key).unwrap().is_some());
        }
        assert!(reopened
            .list_node_labels()
            .unwrap()
            .iter()
            .any(|l| l.label == "WalOnlyLater"));
        close_native(reopened);
    }
    #[test]
    fn p45c1_close_fast_settles_late_flush_adoption_before_reset() {
        let root = tempfile::tempdir().unwrap();
        let options = DbOptions {
            memtable_flush_threshold: 0,
            compact_after_n_flushes: 0,
            ..Default::default()
        };
        let db = tests::open_after_native_shutdown(root.path(), &options).unwrap();
        let id = db.upsert_node("R", "late", UpsertNodeOptions { weight: 1.0, ..Default::default() }).unwrap();
        let old = db.runtime.published.load_full().view.clone();
        let (ready, release) = db.set_flush_publish_pause();
        let cancel = db.runtime.with_core_mut(|core| {
            core.freeze_memtable()?;
            core.ensure_bg_flush_worker();
            core.enqueue_flush()?;
            assert!(core.immutable_epochs[0].in_flight);
            Ok(core.bg_flush.as_ref().unwrap().cancel.clone())
        }).unwrap();
        ready.recv_timeout(Duration::from_secs(5)).unwrap();
        let closing = db.clone();
        let closer = std::thread::spawn(move || closing.close_fast());
        let until = Instant::now() + Duration::from_secs(5);
        while !cancel.load(Ordering::Acquire) {
            assert!(Instant::now() < until, "close did not cancel flush worker");
            std::thread::yield_now();
        }
        // The publisher already passed its cancellation check. Its real durable
        // completion must settle before residual in-flight custody is reset.
        release.send(()).unwrap();
        let result = closer.join().unwrap();
        assert!(result.is_ok(), "late completion close_fast failed: {result:?}");
        assert_eq!(old.get_node(id).unwrap().unwrap().weight, 1.0);
        drop(db);
        let reopened = tests::open_after_native_shutdown(root.path(), &options).unwrap();
        assert_eq!(reopened.get_node(id).unwrap().unwrap().weight, 1.0);
        assert_eq!(reopened.segment_count().unwrap(), 1);
        assert_eq!(reopened.upsert_node("R", "late", UpsertNodeOptions { weight: 2.0, ..Default::default() }).unwrap(), id);
        assert_eq!(reopened.get_node(id).unwrap().unwrap().weight, 2.0);
        assert_eq!(old.get_node(id).unwrap().unwrap().weight, 1.0);
        drop(old);
        close_native(reopened);
    }

    #[test]
    fn p45c1_flush_failure_settles_other_late_completion_before_reset() {
        let root = tempfile::tempdir().unwrap();
        let options = DbOptions {
            memtable_flush_threshold: 0,
            compact_after_n_flushes: 0,
            ..Default::default()
        };
        let db = tests::open_after_native_shutdown(root.path(), &options).unwrap();
        let first = db.upsert_node("R", "first", UpsertNodeOptions::default()).unwrap();
        db.freeze_memtable().unwrap();
        let second = db.upsert_node("R", "second", UpsertNodeOptions::default()).unwrap();
        db.freeze_memtable().unwrap();
        super::p45b1_tests::finish(db);
        // The legacy runtime supports multiple admitted epochs. Drive its real
        // lifecycle entry point directly so the Failed event is drained first.
        let core = EngineCore::open(root.path(), &options).unwrap();
        assert!(core.candidate.is_none());
        assert_eq!(core.immutable_epochs.len(), 2);
        let db = DatabaseEngine {
            runtime: Arc::new(DbRuntime::new(root.path().to_path_buf(), core)),
            client: None,
        };
        let old = db.runtime.published.load_full().view.clone();
        let (adopted_epoch, failed_epoch, failed_wal) = {
            let mut guard = db.runtime.core.lock().unwrap();
            let core = guard.as_mut().unwrap();
            let adopted_epoch = core.immutable_epochs[1].epoch_id;
            let failed_epoch = core.immutable_epochs[0].epoch_id;
            let failed_wal = wal_generation_path(root.path(), core.immutable_epochs[0].wal_generation_id);
            let (ready, release) = core.set_flush_publish_pause();
            core.ensure_bg_flush_worker();
            core.enqueue_flush().unwrap();
            ready.recv_timeout(Duration::from_secs(5)).unwrap();
            core.set_flush_force_error();
            core.enqueue_flush().unwrap();
            let until = Instant::now() + Duration::from_secs(5);
            let bg = core.bg_flush.as_ref().unwrap();
            while bg.events_ready.load(Ordering::Acquire) <= bg.events_applied {
                assert!(Instant::now() < until, "second builder did not report failure");
                std::thread::yield_now();
            }
            assert_eq!(core.in_flight_count(), 2);
            // Only the real Failed event is queued; the first publisher is
            // still paused after its cancellation check. Release it for join.
            release.send(()).unwrap();
            (adopted_epoch, failed_epoch, failed_wal)
        };
        let wait = db.runtime.register_flush_wait(adopted_epoch);
        assert!(db.runtime.run_lifecycle_batch_if_open());
        let published = db.runtime.published.load_full();
        assert_eq!(published.view.segments.len(), 1, "nested adoption must rebuild published sources");
        assert_eq!(published.view.immutable_epochs.len(), 1);
        assert_eq!(published.view.immutable_epochs[0].epoch_id, failed_epoch);
        assert!(!published.view.immutable_epochs.iter().any(|epoch| epoch.epoch_id == adopted_epoch));
        let segment_id = published.view.segments[0].segment_id;
        {
            let lifecycle = db.runtime.lifecycle.lock().unwrap();
            assert_eq!(lifecycle.completed_flush_order.iter().filter(|&&epoch| epoch == adopted_epoch).count(), 1);
            assert_eq!(lifecycle.completed_flushes_by_epoch.len(), 1);
            assert_eq!(lifecycle.completed_flushes_by_epoch[&adopted_epoch].id, segment_id);
            assert_eq!(lifecycle.awaited_flush.as_ref().unwrap().completed.as_ref().unwrap().id, segment_id);
        }
        assert_eq!(db.runtime.take_completed_flush_for_epoch(adopted_epoch).unwrap().id, segment_id);
        assert!(db.runtime.take_completed_flush_for_epoch(adopted_epoch).is_none());
        drop(wait);
        assert!(!db.runtime.run_lifecycle_batch_if_open());
        assert_eq!(db.runtime.lifecycle.lock().unwrap().completed_flush_order.len(), 1);
        {
            let guard = db.runtime.core.lock().unwrap();
            let core = guard.as_ref().unwrap();
            assert_eq!(core.segments.len(), 1);
            assert_eq!(core.immutable_epochs.len(), 1);
            assert_eq!(core.in_flight_count(), 0);
            assert!(core.flush_pipeline_error.as_ref().unwrap().message.contains("injected test failure"));
            assert!(!core.manifest_write_lock.fenced());
        }
        assert!(failed_wal.exists());
        assert_eq!(db.get_node(first).unwrap().unwrap().key, "first");
        assert_eq!(db.get_node(second).unwrap().unwrap().key, "second");
        assert_eq!(old.immutable_epochs.len(), 2);
        assert_eq!(old.get_node(first).unwrap().unwrap().key, "first");
        assert_eq!(old.get_node(second).unwrap().unwrap().key, "second");
        assert!(matches!(db.close_fast(),
            Err(EngineError::InvalidOperation(message)) if message.contains("injected test failure")));
        assert!(failed_wal.exists());
        drop(published);
        drop(db);
        let reopened = tests::open_after_native_shutdown(root.path(), &options).unwrap();
        assert_eq!(reopened.get_node(first).unwrap().unwrap().key, "first");
        assert_eq!(reopened.get_node(second).unwrap().unwrap().key, "second");
        assert_eq!(old.get_node(first).unwrap().unwrap().key, "first");
        drop(old);
        close_native(reopened);
    }

    #[test]
    fn p45c1_stale_flush_adoption_still_fences_and_preserves_recovery() {
        for stale in 0..4 {
            let root = tempfile::tempdir().unwrap();
            let options = DbOptions {
                memtable_flush_threshold: 0,
                compact_after_n_flushes: 0,
                ..Default::default()
            };
            let db = tests::open_after_native_shutdown(root.path(), &options).unwrap();
            db.upsert_node("R", "stale", UpsertNodeOptions::default()).unwrap();
            let (ready, release) = db.set_flush_publish_pause();
            db.runtime.with_core_mut(|core| {
                core.freeze_memtable()?;
                core.ensure_bg_flush_worker();
                core.enqueue_flush()?;
                Ok(())
            }).unwrap();
            ready.recv_timeout(Duration::from_secs(5)).unwrap();
            let manifest_before;
            let recovery_wal;
            {
                // Own adoption authority before letting the real publisher finish.
                let mut guard = db.runtime.core.lock().unwrap();
                let core = guard.as_mut().unwrap();
                release.send(()).unwrap();
                let mut event = core.bg_flush.as_ref().unwrap().event_rx.lock().unwrap()
                    .recv_timeout(Duration::from_secs(5)).unwrap();
                core.bg_flush.as_mut().unwrap().events_applied += 1;
                recovery_wal = wal_generation_path(root.path(), core.immutable_epochs[0].wal_generation_id);
                assert!(recovery_wal.exists());
                manifest_before = std::fs::read(root.path().join(crate::manifest::MANIFEST_CURRENT)).unwrap();
                let BgFlushEvent::Adopt(adoption) = &mut event else { panic!("expected real durable adoption") };
                let output = segment_dir(root.path(), adoption.seg_info.id);
                match stale {
                    0 => adoption.epoch_id += 1,
                    1 => adoption.wal_gen_to_retire += 1,
                    2 => adoption.frozen = core.memtable.clone(),
                    3 => core.immutable_epochs[0].in_flight = false,
                    _ => unreachable!(),
                }
                assert!(matches!(core.process_bg_flush_event(event),
                    Err(EngineError::InvalidOperation(message)) if message.contains("stale flush adoption source identity")));
                assert!(core.manifest_write_lock.fenced());
                assert!(core.segments.is_empty());
                assert_eq!(core.immutable_epochs.len(), 1);
                assert!(output.exists());
            }
            let stats = db.runtime.candidate.as_ref().unwrap().stats.clone();
            assert!(matches!(db.close_fast(), Err(EngineError::CommitOutcome {
                state: CommitOutcomeState::Indeterminate, ..
            })));
            assert_eq!(std::fs::read(root.path().join(crate::manifest::MANIFEST_CURRENT)).unwrap(), manifest_before);
            assert!(recovery_wal.exists());
            drop(db);
            let until = Instant::now() + Duration::from_secs(10);
            while !matches!(stats.read_full().unwrap().close_state, ReclamationCloseState::Drained) {
                assert!(Instant::now() < until);
                std::thread::yield_now();
            }
        }
    }

    #[test]
    fn p45c1_retirement_failure_fences_and_keeps_published_output() {
        let root = tempfile::tempdir().unwrap();
        let options = DbOptions {
            memtable_flush_threshold: 0,
            compact_after_n_flushes: 0,
            ..Default::default()
        };
        let db = DatabaseEngine::open(root.path(), &options).unwrap();
        db.upsert_node("R", "first", UpsertNodeOptions::default())
            .unwrap();
        db.flush().unwrap();
        db.upsert_node("R", "second", UpsertNodeOptions::default())
            .unwrap();
        let (ready, release) = db.set_flush_publish_pause();
        let worker = db.clone();
        let handle = std::thread::spawn(move || worker.flush());
        ready.recv_timeout(Duration::from_secs(5)).unwrap();
        db.runtime
            .with_core_ref(|c| {
                c.manifest_write_lock
                    .inject_fault(crate::persistence_fault::Boundary::WalRetire);
                Ok(())
            })
            .unwrap();
        release.send(()).unwrap();
        assert!(matches!(
            handle.join().unwrap(),
            Err(EngineError::CommitOutcome {
                state: CommitOutcomeState::Indeterminate,
                ..
            })
        ));
        let manifest = load_manifest_readonly(root.path()).unwrap().unwrap();
        assert_eq!(manifest.segments.len(), 2);
        assert!(manifest
            .segments
            .iter()
            .all(|s| segment_dir(root.path(), s.id).exists()));
        let stats = db.runtime.candidate.as_ref().unwrap().stats.clone();
        assert!(db.close().is_err());
        drop(db);
        let until = Instant::now() + Duration::from_secs(10);
        while !matches!(
            stats.read_full().unwrap().close_state,
            ReclamationCloseState::Drained
        ) {
            assert!(Instant::now() < until);
            std::thread::sleep(Duration::from_millis(1));
        }
        let db = DatabaseEngine::open(root.path(), &options).unwrap();
        for key in ["first", "second"] {
            assert!(db.get_node_by_key("R", key).unwrap().is_some());
        }
        close_native(db);
    }
    #[test]
    fn p45c1_historical_batch_above_new_write_limits_replays_whole() {
        let root = tempfile::tempdir().unwrap();
        let options = DbOptions {
            memtable_flush_threshold: 0,
            compact_after_n_flushes: 0,
            ..Default::default()
        };
        let core = EngineCore::open(root.path(), &options).unwrap();
        let runtime = Arc::new(DbRuntime::new(root.path().to_owned(), core));
        runtime.install_core_runtime_handle();
        let legacy = DatabaseEngine {
            runtime,
            client: None,
        };
        legacy
            .batch_upsert_nodes(
                (0..4)
                    .map(|i| NodeInput {
                        labels: vec!["R".into()],
                        key: format!("old-{i}"),
                        props: BTreeMap::from([(
                            "payload".into(),
                            PropValue::String("x".repeat(4096)),
                        )]),
                        weight: 1.,
                        dense_vector: None,
                        sparse_vector: None,
                    })
                    .collect(),
            )
            .unwrap();
        legacy.close_fast().unwrap();
        drop(legacy);
        let limits = ConcurrencyOptions::default()
            .with_max_batch_operations(1)
            .with_generation_operation_limit(2)
            .with_max_batch_bytes(1024)
            .with_generation_wal_bytes(2048);
        let db =
            DatabaseEngine::open_with_concurrency_options(root.path(), &options, &limits).unwrap();
        for i in 0..4 {
            assert_eq!(
                db.get_node_by_key("R", &format!("old-{i}"))
                    .unwrap()
                    .unwrap()
                    .props
                    .get("payload"),
                Some(&PropValue::String("x".repeat(4096)))
            );
        }
        db.upsert_node("R", "after", UpsertNodeOptions::default())
            .unwrap();
        db.flush().unwrap();
        close_native(db);
        let db = DatabaseEngine::open(root.path(), &options).unwrap();
        assert!(db.get_node_by_key("R", "after").unwrap().is_some());
        for i in 0..4 {
            assert!(db
                .get_node_by_key("R", &format!("old-{i}"))
                .unwrap()
                .is_some());
        }
        close_native(db);
    }
}

#[cfg(test)]
mod post45_read_tests {
    use super::p45b1_tests::{edge, finish, node};
    use super::*;

    pub(super) fn ready_participants_for_fixture() {
        // These work bounds exercise warm databases. Do not race the existing
        // nonblocking process initializer in four parallel fixture startups.
        static READY: std::sync::Once = std::sync::Once::new();
        READY.call_once(|| {
            let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
            loop {
                match participant::ParticipantAdmission::new(256) {
                    Ok(_) => break,
                    Err(concurrency_lifecycle::Outcome::ResourceLimit(refusal))
                        if refusal.resource == "participants" && refusal.retryable =>
                    {
                        assert!(std::time::Instant::now() < deadline);
                        std::thread::yield_now();
                    }
                    Err(other) => panic!("participant fixture initialization: {other:?}"),
                }
            }
        });
    }

    fn scan_fixture(count: usize) -> (tempfile::TempDir, DatabaseEngine) {
        ready_participants_for_fixture();
        let dir = tempfile::tempdir().unwrap();
        let db = DatabaseEngine::open(
            dir.path(),
            &DbOptions {
                memtable_flush_threshold: 128 * 1024 * 1024,
                compact_after_n_flushes: 0,
                ..Default::default()
            },
        )
        .unwrap();
        let n = db.ensure_node_label("N").unwrap();
        let endpoint = db.ensure_node_label("Endpoint").unwrap();
        let e = db.ensure_edge_label("E").unwrap();
        let mut ops = vec![
            WalOp::UpsertNode(node(1, endpoint, "from")),
            WalOp::UpsertNode(node(2, endpoint, "to")),
        ];
        for i in 0..count as u64 {
            ops.push(WalOp::UpsertNode(node(i + 10, n, &format!("n{i}"))));
            ops.push(WalOp::UpsertEdge(edge(i + 10, 1, 2, e)));
        }
        db.write_op_batch(&ops).unwrap();
        (dir, db)
    }

    #[test]
    fn post45_read_node_label_consumers_retain_chunks() {
        for count in [100, 257, 4205] {
            let (_dir, db) = scan_fixture(count);
            let view = db.runtime.published.load_full().view.clone();
            let query = NodeQuery {
                label_filter: Some(NodeLabelFilter {
                    labels: vec!["N".into()],
                    mode: LabelMatchMode::All,
                }),
                page: PageRequest {
                    limit: Some(count + 1),
                    ..Default::default()
                },
                ..Default::default()
            };
            let expected: Vec<_> = (10..10 + count as u64).collect();
            for hydrate in [false, true] {
                view.memtable.take_read_scan_work_for_test();
                let ids = if hydrate {
                    db.query_nodes(&query)
                        .unwrap()
                        .items
                        .into_iter()
                        .map(|n| n.id)
                        .collect::<Vec<_>>()
                } else {
                    db.query_node_ids(&query).unwrap().items
                };
                assert_eq!(ids, expected);
                let work = view.memtable.take_read_scan_work_for_test()[0];
                eprintln!("post45 node count={count} hydrate={hydrate} enumeration={work:?}");
                assert!(work.0 >= count, "enumeration counter bypassed: {work:?}");
                assert!(work.1 > 0, "refill counter bypassed: {work:?}");
                assert!(
                    work.0 <= count + 128,
                    "repeated node label enumeration: {work:?}"
                );
                assert!(
                    work.1 <= count.div_ceil(128) + 2,
                    "discarded node chunks: {work:?}"
                );
            }
            drop(view);
            finish(db);
        }
    }

    #[test]
    fn post45_read_edge_label_consumers_retain_chunks() {
        for count in [100, 257, 4205] {
            let (_dir, db) = scan_fixture(count);
            let view = db.runtime.published.load_full().view.clone();
            let query = EdgeQuery {
                label: Some("E".into()),
                page: PageRequest {
                    limit: Some(count + 1),
                    ..Default::default()
                },
                ..Default::default()
            };
            let expected: Vec<_> = (10..10 + count as u64).collect();
            for hydrate in [false, true] {
                view.memtable.take_read_scan_work_for_test();
                let ids = if hydrate {
                    db.query_edges(&query)
                        .unwrap()
                        .edges
                        .into_iter()
                        .map(|e| e.id)
                        .collect::<Vec<_>>()
                } else {
                    db.query_edge_ids(&query).unwrap().edge_ids
                };
                assert_eq!(ids, expected);
                let work = view.memtable.take_read_scan_work_for_test()[1];
                eprintln!("post45 edge label count={count} hydrate={hydrate} enumeration={work:?}");
                assert!(work.0 >= count, "enumeration counter bypassed: {work:?}");
                assert!(work.1 > 0, "refill counter bypassed: {work:?}");
                assert!(
                    work.0 <= count + 128,
                    "repeated edge label enumeration: {work:?}"
                );
                assert!(
                    work.1 <= count.div_ceil(128) + 2,
                    "discarded edge chunks: {work:?}"
                );
            }
            drop(view);
            finish(db);
        }
    }

    #[test]
    fn post45_read_full_edge_consumers_retain_chunks() {
        for count in [100, 257, 4205] {
            let (_dir, db) = scan_fixture(count);
            let view = db.runtime.published.load_full().view.clone();
            let query = EdgeQuery {
                allow_full_scan: true,
                page: PageRequest {
                    limit: Some(count + 1),
                    ..Default::default()
                },
                ..Default::default()
            };
            let expected: Vec<_> = (10..10 + count as u64).collect();
            for hydrate in [false, true] {
                view.memtable.take_read_scan_work_for_test();
                let ids = if hydrate {
                    db.query_edges(&query)
                        .unwrap()
                        .edges
                        .into_iter()
                        .map(|e| e.id)
                        .collect::<Vec<_>>()
                } else {
                    db.query_edge_ids(&query).unwrap().edge_ids
                };
                assert_eq!(ids, expected);
                // Primary walks also include legitimate planner/count work.
                let work = view.memtable.take_read_scan_work_for_test()[2].0;
                eprintln!("post45 full edge count={count} hydrate={hydrate} groups={work}");
                assert!(work >= count, "primary walk counter bypassed: {work}");
                assert!(
                    work <= 3 * count + 128,
                    "repeated full-edge enumeration: {work}"
                );
            }
            drop(view);
            finish(db);
        }
    }
    #[test]
    fn post45_read_empty_quanta_pages_old_views_and_source_dedup() {
        let (_dir, db) = scan_fixture(2);
        let n = db.ensure_node_label("N").unwrap();
        let e = db.ensure_edge_label("E").unwrap();
        db.write_op_batch(&[
            WalOp::UpsertNode(node(6000, n, "tail")),
            WalOp::UpsertEdge(edge(6000, 1, 2, e)),
        ])
        .unwrap();
        let old = db.runtime.published.load_full().view.clone();
        // More than 4,096 future-only groups before a visible old tail.
        let ops: Vec<_> = (100..5100)
            .flat_map(|id| {
                [
                    WalOp::UpsertNode(node(id, n, &format!("future{id}"))),
                    WalOp::UpsertEdge(edge(id, 1, 2, e)),
                ]
            })
            .collect();
        db.write_op_batch(&ops).unwrap();
        let nq = NodeQuery {
            label_filter: Some(NodeLabelFilter {
                labels: vec!["N".into()],
                mode: LabelMatchMode::All,
            }),
            page: PageRequest {
                limit: Some(6000),
                ..Default::default()
            },
            ..Default::default()
        };
        let eq = EdgeQuery {
            label: Some("E".into()),
            page: PageRequest {
                limit: Some(6000),
                ..Default::default()
            },
            ..Default::default()
        };
        assert_eq!(old.memtable.next_visible_node_by_label_id_ge(n, old.snapshot_seq, 12).unwrap(), Some(6000));
        assert_eq!(old.memtable.next_visible_edge_by_label_id_ge(e, old.snapshot_seq, 12).unwrap(), Some(6000));
        assert_eq!(old.memtable.next_visible_adj_edge_ge(1, true, Some(e), old.snapshot_seq, 12).unwrap(), Some(6000));
        assert_eq!(old.memtable.next_visible_node_by_label_id_after(n, old.snapshot_seq, Some(u64::MAX)).unwrap(), None);
        assert_eq!(old.memtable.next_visible_edge_from_endpoint_after(1, Some(&[e]), old.snapshot_seq, Some(u64::MAX)).unwrap(), None);

        assert_eq!(
            old.query_node_ids_outcome(&nq).unwrap().value.items,
            vec![10, 11, 6000]
        );
        assert_eq!(
            old.query_edge_ids_outcome(&eq).unwrap().value.edge_ids,
            vec![10, 11, 6000]
        );
        assert_eq!(
            old.query_edge_ids_outcome(&EdgeQuery {
                label: None,
                allow_full_scan: true,
                ..eq.clone()
            })
            .unwrap()
            .value
            .edge_ids,
            vec![10, 11, 6000]
        );
        let deletes: Vec<_> = (100..5100)
            .flat_map(|id| {
                [
                    WalOp::DeleteNode { id, deleted_at: 5 },
                    WalOp::DeleteEdge { id, deleted_at: 5 },
                ]
            })
            .collect();
        db.write_op_batch(&deletes).unwrap();
        assert_eq!(db.query_node_ids(&nq).unwrap().items, vec![10, 11, 6000]);
        assert_eq!(db.query_edge_ids(&eq).unwrap().edge_ids, vec![10, 11, 6000]);
        db.flush().unwrap();
        // Replacements in active must suppress/deduplicate persisted memberships.
        db.write_op_batch(&[
            WalOp::UpsertNode(node(10, n, "replaced")),
            WalOp::UpsertNode(node(100, n, "resurrected")),
            WalOp::UpsertEdge(edge(10, 1, 2, e)),
            WalOp::UpsertEdge(edge(100, 1, 2, e)),
        ])
        .unwrap();
        let mut after = None;
        let mut nodes = Vec::new();
        let mut edges = Vec::new();
        loop {
            let page = db
                .query_node_ids(&NodeQuery {
                    page: PageRequest {
                        limit: Some(1),
                        after,
                    },
                    ..nq.clone()
                })
                .unwrap();
            nodes.extend(page.items);
            after = page.next_cursor;
            if after.is_none() {
                break;
            }
        }
        after = None;
        loop {
            let page = db
                .query_edge_ids(&EdgeQuery {
                    page: PageRequest {
                        limit: Some(1),
                        after,
                    },
                    ..eq.clone()
                })
                .unwrap();
            edges.extend(page.edge_ids);
            after = page.next_cursor;
            if after.is_none() {
                break;
            }
        }
        assert_eq!(nodes, vec![10, 11, 100, 6000]);
        assert_eq!(edges, nodes);
        assert!(db
            .query_node_ids(&NodeQuery {
                page: PageRequest {
                    limit: Some(1),
                    after: Some(u64::MAX)
                },
                ..nq
            })
            .unwrap()
            .items
            .is_empty());
        assert!(db
            .query_edge_ids(&EdgeQuery {
                page: PageRequest {
                    limit: Some(1),
                    after: Some(u64::MAX)
                },
                ..eq.clone()
            })
            .unwrap()
            .edge_ids
            .is_empty());
        assert!(db
            .query_edge_ids(&EdgeQuery {
                label: None,
                allow_full_scan: true,
                page: PageRequest {
                    limit: Some(1),
                    after: Some(u64::MAX)
                },
                ..eq
            })
            .unwrap()
            .edge_ids
            .is_empty());
        assert_eq!(
            old.query_node_ids_outcome(&NodeQuery {
                label_filter: Some(NodeLabelFilter {
                    labels: vec!["N".into()],
                    mode: LabelMatchMode::All,
                }),
                ..Default::default()
            })
            .unwrap()
            .value
            .items,
            vec![10, 11, 6000]
        );
        drop(old);
        finish(db);
    }

    #[test]
    fn post45_read_metadata_one_seek_per_unique_opinion() {
        let (_dir, db) = scan_fixture(2);
        db.write_op_batch(&[
            WalOp::DeleteNode {
                id: 11,
                deleted_at: 8,
            },
            WalOp::DeleteEdge {
                id: 11,
                deleted_at: 8,
            },
        ])
        .unwrap();
        let old = db.runtime.published.load_full().view.clone();
        let n = db.ensure_node_label("N").unwrap();
        let e = db.ensure_edge_label("E").unwrap();
        db.write_op_batch(&[
            WalOp::UpsertNode(node(12, n, "future")),
            WalOp::UpsertEdge(edge(12, 1, 2, e)),
        ])
        .unwrap();
        let ids = [10, 11, 12, 13, 12, 13, 10, 11];
        let lookups: Vec<_> = ids.into_iter().enumerate().collect();
        let mt = &old.memtable;
        mt.primary_node_seeks_for_test(true);
        let mut nodes = vec![NodeVisibilityState::Missing; ids.len()];
        let remaining = mt
            .batch_get_node_visibility_meta_at(&lookups, old.snapshot_seq, &mut nodes)
            .unwrap();
        let node_seeks = mt.primary_node_seeks_for_test(true);
        assert!(matches!(nodes[0], NodeVisibilityState::Live(_)));
        assert_eq!(nodes[1], NodeVisibilityState::Deleted);
        assert_eq!(remaining, vec![(2, 12), (3, 13), (4, 12), (5, 13)]);
        mt.take_edge_seeks_for_test();
        let mut edges = vec![crate::edge_metadata::EdgeMetadataVisibilityState::Missing; ids.len()];
        let edge_remaining = mt
            .batch_get_edge_metadata_at(&lookups, old.snapshot_seq, &mut edges)
            .unwrap();
        let edge_seeks = mt.take_edge_seeks_for_test();
        assert!(matches!(
            edges[0],
            crate::edge_metadata::EdgeMetadataVisibilityState::Live(_)
        ));
        assert_eq!(
            edges[1],
            crate::edge_metadata::EdgeMetadataVisibilityState::Deleted
        );
        assert_eq!(edge_remaining, remaining);
        assert_eq!(
            mt.edge_visibility_state_at(12, old.snapshot_seq).unwrap(),
            EdgeVisibilityState::Missing
        );
        let scalar_seeks = mt.take_edge_seeks_for_test();
        eprintln!(
            "post45 metadata node={node_seeks} edge={edge_seeks} missing_scalar={scalar_seeks}"
        );
        assert_eq!((node_seeks, edge_seeks, scalar_seeks), (4, 4, 1));
        drop(old);
        finish(db);
    }

    #[test]
    fn post45_read_vector_projection_copies_only_selected_payload() {
        ready_participants_for_fixture();
        let dir = tempfile::tempdir().unwrap();
        let db = DatabaseEngine::open(
            dir.path(),
            &DbOptions {
                dense_vector: Some(DenseVectorConfig {
                    dimension: 64,
                    metric: DenseMetric::Cosine,
                    hnsw: HnswConfig::default(),
                }),
                compact_after_n_flushes: 0,
                memtable_flush_threshold: 128 * 1024 * 1024,
                ..Default::default()
            },
        )
        .unwrap();
        let n = db.ensure_node_label("V").unwrap();
        let ops: Vec<_> = (1..=128)
            .map(|id| {
                let mut row = node(id, n, &"key".repeat(100));
                row.props.insert(
                    "unrelated".into(),
                    PropValue::Map(BTreeMap::from([(
                        "nested".into(),
                        PropValue::Array(vec![PropValue::String("x".repeat(4096))]),
                    )])),
                );
                row.dense_vector = Some(vec![1.0; 64]);
                row.sparse_vector = Some((0..8).map(|i| (i, 1.0)).collect());
                WalOp::UpsertNode(row)
            })
            .collect();
        db.write_op_batch(&ops).unwrap();
        let view = db.runtime.published.load_full().view.clone();
        let request = VectorSearchRequest {
            mode: VectorSearchMode::Dense,
            dense_query: Some(vec![1.0; 64]),
            sparse_query: None,
            k: 10,
            label_filter: None,
            ef_search: None,
            scope: None,
            dense_weight: None,
            sparse_weight: None,
            fusion_mode: None,
        };
        view.memtable.take_vector_copy_bytes_for_test();
        let dense = db.vector_search(&request).unwrap();
        assert_eq!(
            dense.iter().map(|h| h.node_id).collect::<Vec<_>>(),
            (1..=10).collect::<Vec<_>>()
        );
        assert!(dense.iter().all(|h| (h.score - 1.0).abs() < 1e-5));
        let dense_bytes = view.memtable.take_vector_copy_bytes_for_test();
        let sparse = db
            .vector_search(&VectorSearchRequest {
                mode: VectorSearchMode::Sparse,
                dense_query: None,
                sparse_query: Some(vec![(0, 1.0)]),
                ..request
            })
            .unwrap();
        assert_eq!(
            sparse.iter().map(|h| h.node_id).collect::<Vec<_>>(),
            (1..=10).collect::<Vec<_>>()
        );
        assert!(sparse.iter().all(|h| (h.score - 1.0).abs() < 1e-5));
        let sparse_bytes = view.memtable.take_vector_copy_bytes_for_test();
        eprintln!("post45 vector dense={dense_bytes:?} sparse={sparse_bytes:?}");
        assert_eq!(dense_bytes, [0, 0, 128 * 64 * 4, 0]);
        assert_eq!(sparse_bytes, [0, 0, 0, 128 * 8 * 8]);
        drop(view);
        finish(db);
    }

    #[test]
    fn post45_read_vector_presence_and_oversized_payload_progress() {
        ready_participants_for_fixture();
        let dir = tempfile::tempdir().unwrap();
        let dimension = 262_161;
        let db = DatabaseEngine::open(
            dir.path(),
            &DbOptions {
                dense_vector: Some(DenseVectorConfig {
                    dimension,
                    metric: DenseMetric::Cosine,
                    hnsw: HnswConfig::default(),
                }),
                memtable_flush_threshold: 128 * 1024 * 1024,
                compact_after_n_flushes: 0,
                ..Default::default()
            },
        )
        .unwrap();
        let n = db.ensure_node_label("V").unwrap();
        let mut first = node(1, n, "first");
        first.dense_vector = Some(vec![1.0; dimension as usize]);
        first.sparse_vector = Some(vec![(1, 1.0)]);
        let mut second = first.clone();
        second.id = 8000;
        second.key = "second".into();
        db.write_op_batch(&[
            WalOp::UpsertNode(first),
            WalOp::UpsertNode(second),
            WalOp::UpsertNode(node(9000, n, "vectorless")),
        ])
        .unwrap();
        let old = db.runtime.published.load_full().view.clone();
        db.write_op_batch(
            &(2..5002)
                .map(|id| WalOp::UpsertNode(node(id, n, &format!("future{id}"))))
                .collect::<Vec<_>>(),
        )
        .unwrap();
        let handle = old.memtable.participant_handle().unwrap();
        let mut seen = Vec::new();
        old.memtable.take_vector_copy_bytes_for_test();
        let _ = old
            .memtable
            .for_each_visible_node_at(
                old.snapshot_seq,
                crate::row_projection::VectorSelection::None,
                &mut |row| {
                    assert_eq!(handle.snapshot().0, 0);
                    assert!(row.dense_vector.is_none() && row.sparse_vector.is_none());
                    assert_eq!(
                        (row.has_dense, row.has_sparse),
                        (row.id != 9000, row.id != 9000)
                    );
                    seen.push(row.id);
                    ControlFlow::Continue(())
                },
            )
            .unwrap();
        assert_eq!(seen, vec![1, 8000, 9000]);
        assert_eq!(old.memtable.take_vector_copy_bytes_for_test(), [0; 4]);
        seen.clear();
        let _ = old
            .memtable
            .for_each_visible_node_at(
                old.snapshot_seq,
                crate::row_projection::VectorSelection::Dense,
                &mut |row| {
                    assert_eq!(handle.snapshot().0, 0);
                    assert!(row.sparse_vector.is_none());
                    if row.id != 9000 {
                        assert_eq!(row.dense_vector.as_ref().unwrap().len(), dimension as usize);
                    }
                    seen.push(row.id);
                    ControlFlow::Continue(())
                },
            )
            .unwrap();
        assert_eq!(seen, vec![1, 8000, 9000]);
        assert_eq!(
            old.memtable.take_vector_copy_bytes_for_test(),
            [0, 0, 2 * dimension as usize * 4, 0]
        );
        seen.clear();
        let mut dense_missing = Vec::new();
        old.memtable
            .visit_nodes_sorted_at(
                &[1, 8000, 9000, 9001],
                old.snapshot_seq,
                crate::row_projection::VectorSelection::Dense,
                &mut dense_missing,
                &mut |id, row| {
                    assert_eq!(handle.snapshot().0, 0);
                    assert!(row.sparse_vector.is_none());
                    if id != 9000 {
                        assert_eq!(row.dense_vector.as_ref().unwrap().len(), dimension as usize);
                    }
                    seen.push(id);
                },
            )
            .unwrap();
        assert_eq!(seen, vec![1, 8000, 9000]);
        assert_eq!(dense_missing, vec![9001]);
        assert_eq!(
            old.memtable.take_vector_copy_bytes_for_test(),
            [0, 0, 2 * dimension as usize * 4, 0]
        );
        seen.clear();
        old.memtable
            .visit_nodes_sorted_at(
                &[],
                old.snapshot_seq,
                crate::row_projection::VectorSelection::Dense,
                &mut Vec::new(),
                &mut |id, _| seen.push(id),
            )
            .unwrap();
        assert!(seen.is_empty());
        old.memtable
            .visit_nodes_sorted_at(
                &[9000],
                old.snapshot_seq,
                crate::row_projection::VectorSelection::Dense,
                &mut Vec::new(),
                &mut |id, row| {
                    assert_eq!(handle.snapshot().0, 0);
                    assert!(row.dense_vector.is_none());
                    seen.push(id);
                },
            )
            .unwrap();
        assert_eq!(seen, vec![9000]);
        assert_eq!(old.memtable.take_vector_copy_bytes_for_test(), [0; 4]);
        seen.clear();
        let mut missing = Vec::new();
        old.memtable
            .visit_nodes_sorted_at(
                &[1, 8000, 9000, 9001],
                old.snapshot_seq,
                crate::row_projection::VectorSelection::Sparse,
                &mut missing,
                &mut |id, row| {
                    assert_eq!(handle.snapshot().0, 0);
                    assert!(row.dense_vector.is_none());
                    if id != 9000 {
                        assert_eq!(row.sparse_vector.as_deref(), Some(&[(1, 1.0)][..]));
                    }
                    seen.push(id);
                },
            )
            .unwrap();
        assert_eq!(seen, vec![1, 8000, 9000]);
        assert_eq!(missing, vec![9001]);
        assert_eq!(
            old.memtable.take_vector_copy_bytes_for_test(),
            [0, 0, 0, 16]
        );
        drop(old);
        finish(db);
    }

    fn vector_transport_options() -> DbOptions {
        DbOptions {
            dense_vector: Some(DenseVectorConfig {
                dimension: 64,
                metric: DenseMetric::Cosine,
                hnsw: HnswConfig::default(),
            }),
            compact_after_n_flushes: 0,
            ..Default::default()
        }
    }

    fn vector_transport_nodes() -> Vec<NodeInput> {
        (1..=3072).map(|id| NodeInput {
            labels: vec!["V".into()],
            key: format!("n{id}"),
            props: BTreeMap::from([("unselected".into(), PropValue::String("x".repeat(512)))]),
            weight: 1.,
            dense_vector: Some(vec![1.; 64]),
            sparse_vector: Some(vec![(1, 1.0); 1]),
        }).collect()
    }

    fn assert_vector_transport_survivors(db: &DatabaseEngine) {
        let mut live = db.get_nodes_by_labels("V").unwrap();
        live.extend(db.get_nodes_by_labels("Other").unwrap());
        live.sort_by_key(|node| node.id);
        assert_eq!(live.iter().map(|node| node.id).collect::<Vec<_>>(), [2900, 3001, 3072]);
        assert_eq!(live.iter().filter(|node| node.dense_vector.is_some()).count(), 2);
        assert!(db.get_node(2999).unwrap().is_none());
    }

    fn assert_large_vector_transport_segment(db: &DatabaseEngine, ids: &[u64]) {
        let header = {
            let segments = db.segments_for_test();
            assert_eq!(segments.len(), 1);
            segments[0].dense_hnsw_header().expect("full-population HNSW available")
        };
        assert_eq!(header.point_count, 3072);
        assert_eq!(header.dimension, 64);
        assert_eq!(header.metric, DenseMetric::Cosine);
        let mut rows = db.get_nodes_by_labels("V").unwrap();
        rows.sort_by_key(|node| node.id);
        assert_eq!(rows.iter().map(|node| node.id).collect::<Vec<_>>(), ids);
        let payload = PropValue::String("x".repeat(512));
        for row in rows {
            assert_eq!(row.key, format!("n{}", row.id));
            assert_eq!(row.weight, 1.);
            assert_eq!(row.props.len(), 1);
            assert_eq!(row.props.get("unselected"), Some(&payload));
            assert_eq!(row.dense_vector.as_deref(), Some(&[1.; 64][..]));
            assert_eq!(row.sparse_vector.as_deref(), Some(&[(1, 1.)][..]));
        }
    }

    // Retain the original full-population HNSW construction and persisted-data
    // coverage without asserting that its CPU work completes within 10s.
    #[test]
    fn vector_transport_full_population_hnsw_flush_reopens() {
        ready_participants_for_fixture();
        let dir = tempfile::tempdir().unwrap();
        let options = vector_transport_options();
        let db = tests::open_functional_maintenance_fixture(dir.path(), &options).unwrap();
        db.ensure_node_label("V").unwrap();
        db.ensure_node_label("Other").unwrap();
        let ids = db.batch_upsert_nodes(vector_transport_nodes()).unwrap();
        assert_eq!(ids, (1..=3072).collect::<Vec<_>>());
        let segment = db.flush().unwrap().expect("full-population flush");
        assert_eq!(segment.node_count, 3072);
        assert_large_vector_transport_segment(&db, &ids);
        finish(db);
        let reopened = DatabaseEngine::open(dir.path(), &options).unwrap();
        assert_large_vector_transport_segment(&reopened, &ids);
        finish(reopened);
    }

    #[test]
    fn vector_transport_reuses_flat_buffers_and_releases_sessions_during_callbacks() {
        ready_participants_for_fixture();
        let dir = tempfile::tempdir().unwrap();
        // The unchanged 10s callback guard is the progress assertion. Give the
        // larger semantic mutation batch the existing functional allowance.
        let db = tests::open_functional_maintenance_fixture(
            dir.path(), &vector_transport_options(),
        ).unwrap();
        let n = db.ensure_node_label("V").unwrap();
        let other = db.ensure_node_label("Other").unwrap();
        let ids = db.batch_upsert_nodes(vector_transport_nodes()).unwrap();
        assert_eq!(ids, (1..=3072).collect::<Vec<_>>());
        let held = db.runtime.published.load_full().view.clone();
        let handle = held.memtable.participant_handle().unwrap();
        let mut seen = Vec::with_capacity(ids.len());
        let mut missing = Vec::with_capacity(ids.len());
        held.memtable.take_vector_copy_bytes_for_test();
        let (_, work) = participant::allocations::measure(|| {
            held.memtable
                .visit_nodes_sorted_at(
                    &ids,
                    held.snapshot_seq,
                    crate::row_projection::VectorSelection::Dense,
                    &mut missing,
                    &mut |id, row| {
                        assert_eq!(handle.snapshot().0, 0);
                        assert!(row.sparse_vector.is_none());
                        assert_eq!(row.dense_vector.as_deref().unwrap(), &[1.; 64]);
                        seen.push(id);
                    },
                )
                .unwrap();
        });
        eprintln!("flat vector transport allocation calls/bytes = {work:?}");
        assert_eq!(seen, ids);
        assert!(missing.is_empty());
        assert!(
            work.0 < 32,
            "transport allocated {} backings for 3072 vectors",
            work.0
        );
        assert!(
            work.2 < 2 * 1024 * 1024,
            "transport cloned unrelated fields or excess backing: {}",
            work.2
        );
        assert_eq!(
            held.memtable.take_vector_copy_bytes_for_test(),
            [0, 0, 3072 * 64 * 4, 0]
        );
        // A writer moves the source graph after the first owned chunk. The next
        // refill must still use the held S, without holding a participant in callbacks.
        let (ready_tx, ready_rx) = std::sync::mpsc::channel();
        let (done_tx, done_rx) = std::sync::mpsc::channel();
        let writer_db = db.clone();
        let writer_ids = ids.clone();
        let writer_phase = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let phase = writer_phase.clone();
        let started = std::time::Instant::now();
        let writer = std::thread::spawn(move || {
            ready_rx
                .recv_timeout(std::time::Duration::from_secs(10))
                .unwrap();
            let mut moved = node(2900, other, "n2900");
            moved.dense_vector = Some(vec![2.; 64]);
            let vectorless = node(3001, n, "n3001");
            let mut updated = node(3072, n, "n3072");
            updated.dense_vector = Some(vec![2.; 64]);
            let result = (|| {
                phase.store(1, Ordering::Release);
                let mut ops = writer_ids.into_iter()
                    .filter(|id| !matches!(id, 2900 | 3000 | 3001 | 3072))
                    .map(|id| WalOp::DeleteNode { id, deleted_at: 9 })
                    .collect::<Vec<_>>();
                ops.extend([
                    WalOp::UpsertNode(moved),
                    WalOp::DeleteNode { id: 3000, deleted_at: 9 },
                    WalOp::UpsertNode(vectorless),
                    WalOp::UpsertNode(updated),
                ]);
                assert_eq!(ops.len(), 3072);
                writer_db.write_op_batch(&ops)?;
                phase.store(2, Ordering::Release);
                eprintln!("vector callback writer batch complete at {:?}", started.elapsed());
                let segment = writer_db.flush()?.expect("real populated flush");
                phase.store(3, Ordering::Release);
                eprintln!("vector callback writer flush complete at {:?}", started.elapsed());
                Ok::<_, EngineError>(segment)
            })();
            done_tx.send(result).unwrap();
        });
        seen.clear();
        let mut paused = false;
        let mut first_owned_prefix = 0;
        let mut later_refill_rows = 0;
        let mut flush_completed = false;
        held.memtable
            .visit_nodes_sorted_at(
                &ids,
                held.snapshot_seq,
                crate::row_projection::VectorSelection::Dense,
                &mut missing,
                &mut |id, row| {
                    assert_eq!(handle.snapshot().0, 0);
                    if !paused {
                        paused = true;
                        let copied = held.memtable.take_vector_copy_bytes_for_test();
                        assert_eq!(copied, [0, 0, copied[2], 0]);
                        assert_eq!(copied[2] % (64 * 4), 0);
                        first_owned_prefix = copied[2] / (64 * 4);
                        assert!(first_owned_prefix > 0 && first_owned_prefix < ids.len());
                        for changed in [2900, 3000, 3001, 3072] {
                            assert!(ids.iter().position(|id| *id == changed).unwrap() >= first_owned_prefix);
                        }
                        ready_tx.send(()).unwrap();
                        let segment = done_rx
                            .recv_timeout(std::time::Duration::from_secs(10))
                            .unwrap_or_else(|error| {
                                panic!(
                                    "vector callback writer wait: {error:?}, phase={} (0=dispatch, 1=batch, 2=flush, 3=done), elapsed={:?}",
                                    writer_phase.load(Ordering::Acquire), started.elapsed(),
                                )
                            })
                            .unwrap();
                        assert_eq!(segment.node_count, 3);
                        let published = db.runtime.published.load_full();
                        assert!(!held.memtable.same_generation(&published.view.memtable));
                        assert!(db.segments_for_test().iter().any(|s| s.segment_id == segment.id));
                        flush_completed = true;
                    }
                    if seen.len() >= first_owned_prefix {
                        assert!(flush_completed);
                        later_refill_rows += 1;
                    }
                    assert_eq!(row.dense_vector.as_deref().unwrap(), &[1.; 64]);
                    seen.push(id);
                },
            )
            .unwrap();
        writer.join().unwrap();
        assert!(flush_completed);
        assert_eq!(later_refill_rows, ids.len() - first_owned_prefix);
        assert!(later_refill_rows > 0);
        eprintln!("owned prefix={first_owned_prefix}, later rows after completed flush={later_refill_rows}");
        assert_eq!(seen, ids);
        assert!(missing.is_empty());
        assert_vector_transport_survivors(&db);
        let selected: NodeIdSet = [2900, 3000, 3001, 3072].into_iter().collect();
        let filter = held
            .label_catalog
            .resolve_node_label_filter_request(Some(&NodeLabelFilter {
                labels: vec!["V".into()],
                mode: LabelMatchMode::Any,
            }))
            .unwrap();
        let current = db.runtime.published.load_full().view.clone();
        let old_hits = held
            .score_dense_candidate_ids(&selected, &[1.; 64], DenseMetric::Cosine, &filter, None)
            .unwrap();
        assert_eq!(
            old_hits.iter().map(|h| h.node_id).collect::<Vec<_>>(),
            vec![2900, 3000, 3001, 3072]
        );
        let new_hits = current
            .score_dense_candidate_ids(&selected, &[1.; 64], DenseMetric::Cosine, &filter, None)
            .unwrap();
        assert_eq!(
            new_hits.iter().map(|h| h.node_id).collect::<Vec<_>>(),
            vec![3072]
        );
        drop((held, current));
        finish(db);
        let reopened = DatabaseEngine::open(dir.path(), &DbOptions::default()).unwrap();
        assert!(reopened.get_node(3000).unwrap().is_none());
        assert!(reopened
            .get_node(3001)
            .unwrap()
            .unwrap()
            .dense_vector
            .is_none());
        assert_eq!(
            reopened.get_node(3072).unwrap().unwrap().dense_vector,
            Some(vec![2.; 64])
        );
        assert_vector_transport_survivors(&reopened);
        finish(reopened);
    }
}

#[cfg(test)]
mod post45_write_tests {
    use super::p45b1_tests::{edge, finish, node};
    use super::post45_read_tests::ready_participants_for_fixture;
    use super::*;

    fn assert_future_id_counts(
        db: &DatabaseEngine,
        distinct: [usize; 2],
        live: [usize; 2],
        tombstones: (usize, usize),
    ) {
        let view = db.runtime.published.load_full().view.clone();
        assert_eq!(view.memtable.primary_distinct_ids_for_test(), distinct);
        assert_eq!(
            [view.memtable.node_count(), view.memtable.edge_count()],
            live
        );
        assert_eq!(view.memtable.current_tombstone_counts(), tombstones);
        assert_eq!(
            view.memtable.estimated_size(),
            view.memtable.estimated_size_full_for_test()
        );
        let stats = db.stats().unwrap();
        assert_eq!(
            (stats.node_tombstone_count, stats.edge_tombstone_count),
            tombstones
        );
    }

    #[test]
    fn post45_write_future_node_tombstones_preserve_allocation_and_reopen_counts() {
        ready_participants_for_fixture();
        for sync in [WalSyncMode::default(), WalSyncMode::Immediate] {
            let dir = tempfile::tempdir().unwrap();
            let options = DbOptions {
                wal_sync_mode: sync,
                compact_after_n_flushes: 0,
                ..Default::default()
            };
            let mut db = DatabaseEngine::open(dir.path(), &options).unwrap();
            db.ensure_node_label("N").unwrap();
            let mut first = 1;
            for target in [5, 9] {
                let old = db.runtime.published.load_full().view.clone();
                old.memtable.take_preparation_seeks_for_test();
                for _ in 0..3 {
                    db.delete_node(target).unwrap();
                    assert!(db.get_node(target).unwrap().is_none());
                    assert_future_id_counts(
                        &db,
                        [first as usize, 0],
                        [(first - 1) as usize, 0],
                        (1, 0),
                    );
                }
                // Only the first delete is above the generation maximum.
                assert_eq!(old.memtable.take_preparation_seeks_for_test(), [2, 0]);
                let mut inputs = (first..=target)
                    .map(|id| NodeInput {
                        labels: vec!["N".into()],
                        key: format!("future-node-{id}"),
                        props: BTreeMap::from([("version".into(), PropValue::Int(1))]),
                        weight: 1.,
                        dense_vector: None,
                        sparse_vector: None,
                    })
                    .collect::<Vec<_>>();
                inputs.push(inputs.last().unwrap().clone());
                assert_eq!(
                    db.batch_upsert_nodes(inputs).unwrap(),
                    (first..=target)
                        .chain(std::iter::once(target))
                        .collect::<Vec<_>>()
                );
                assert_eq!(
                    old.memtable.take_preparation_seeks_for_test(),
                    [(target - first + 1) as usize, 0]
                );
                assert_future_id_counts(&db, [target as usize, 0], [target as usize, 0], (0, 0));
                assert!(old.get_node_raw(target).unwrap().is_none());
                assert_eq!(
                    db.upsert_node(
                        "N",
                        &format!("future-node-{target}"),
                        UpsertNodeOptions {
                            props: BTreeMap::from([("version".into(), PropValue::Int(2))]),
                            ..Default::default()
                        }
                    )
                    .unwrap(),
                    target
                );
                assert_eq!(old.memtable.take_preparation_seeks_for_test(), [1, 0]);
                drop(old);
                finish(db);
                db = DatabaseEngine::open(dir.path(), &options).unwrap();
                assert_future_id_counts(&db, [target as usize, 0], [target as usize, 0], (0, 0));
                for id in 1..=target {
                    assert_eq!(
                        db.get_node_by_key("N", &format!("future-node-{id}"))
                            .unwrap()
                            .unwrap()
                            .id,
                        id
                    );
                }
                assert_eq!(
                    db.get_node(target).unwrap().unwrap().props.get("version"),
                    Some(&PropValue::Int(2))
                );
                first = target + 1;
            }
            // Retain an unallocated tombstone through actual WAL replay too.
            db.delete_node(13).unwrap();
            db.delete_node(13).unwrap();
            assert_future_id_counts(&db, [10, 0], [9, 0], (1, 0));
            finish(db);
            let db = DatabaseEngine::open(dir.path(), &options).unwrap();
            assert_future_id_counts(&db, [10, 0], [9, 0], (1, 0));
            assert!(db.get_node(13).unwrap().is_none());
            finish(db);
        }
    }

    #[test]
    fn post45_write_future_edge_tombstones_preserve_allocation_and_reopen_counts() {
        ready_participants_for_fixture();
        for sync in [WalSyncMode::default(), WalSyncMode::Immediate] {
            let dir = tempfile::tempdir().unwrap();
            let options = DbOptions {
                edge_uniqueness: true,
                wal_sync_mode: sync,
                compact_after_n_flushes: 0,
                ..Default::default()
            };
            let mut db = DatabaseEngine::open(dir.path(), &options).unwrap();
            let nodes = db
                .batch_upsert_nodes(
                    (1..=10)
                        .map(|id| NodeInput {
                            labels: vec!["N".into()],
                            key: format!("endpoint-{id}"),
                            props: BTreeMap::new(),
                            weight: 1.,
                            dense_vector: None,
                            sparse_vector: None,
                        })
                        .collect(),
                )
                .unwrap();
            assert_eq!(nodes, (1..=10).collect::<Vec<_>>());
            db.ensure_edge_label("E").unwrap();
            let mut first = 1;
            for target in [5, 9] {
                let old = db.runtime.published.load_full().view.clone();
                old.memtable.take_preparation_seeks_for_test();
                for _ in 0..3 {
                    db.delete_edge(target).unwrap();
                    assert!(db.get_edge(target).unwrap().is_none());
                    assert_future_id_counts(
                        &db,
                        [10, first as usize],
                        [10, (first - 1) as usize],
                        (0, 1),
                    );
                }
                assert_eq!(old.memtable.take_preparation_seeks_for_test(), [0, 2]);
                let mut inputs = (first..=target)
                    .map(|id| EdgeInput {
                        from: 1,
                        to: id + 1,
                        label: "E".into(),
                        props: BTreeMap::from([("version".into(), PropValue::Int(1))]),
                        weight: 1.,
                        valid_from: None,
                        valid_to: None,
                    })
                    .collect::<Vec<_>>();
                inputs.push(inputs.last().unwrap().clone());
                assert_eq!(
                    db.batch_upsert_edges(inputs).unwrap(),
                    (first..=target)
                        .chain(std::iter::once(target))
                        .collect::<Vec<_>>()
                );
                assert_eq!(
                    old.memtable.take_preparation_seeks_for_test(),
                    [0, (target - first + 1) as usize]
                );
                assert_future_id_counts(&db, [10, target as usize], [10, target as usize], (0, 0));
                assert!(old.sources().find_edge(target).unwrap().is_none());
                assert_eq!(
                    db.upsert_edge(
                        1,
                        target + 1,
                        "E",
                        UpsertEdgeOptions {
                            props: BTreeMap::from([("version".into(), PropValue::Int(2))]),
                            ..Default::default()
                        }
                    )
                    .unwrap(),
                    target
                );
                assert_eq!(old.memtable.take_preparation_seeks_for_test(), [0, 1]);
                drop(old);
                finish(db);
                db = DatabaseEngine::open(dir.path(), &options).unwrap();
                assert_future_id_counts(&db, [10, target as usize], [10, target as usize], (0, 0));
                for id in 1..=target {
                    assert_eq!(
                        db.get_edge_by_triple(1, id + 1, "E").unwrap().unwrap().id,
                        id
                    );
                }
                assert_eq!(
                    db.get_edge(target).unwrap().unwrap().props.get("version"),
                    Some(&PropValue::Int(2))
                );
                first = target + 1;
            }
            db.delete_edge(13).unwrap();
            db.delete_edge(13).unwrap();
            assert_future_id_counts(&db, [10, 10], [10, 9], (0, 1));
            finish(db);
            let db = DatabaseEngine::open(dir.path(), &options).unwrap();
            assert_future_id_counts(&db, [10, 10], [10, 9], (0, 1));
            assert!(db.get_edge(13).unwrap().is_none());
            finish(db);
        }
    }

    fn fixture() -> (tempfile::TempDir, DatabaseEngine, u32, u32, u32, u32) {
        fixture_with_open(DatabaseEngine::open)
    }
    fn fixture_with_open(
        open: fn(&Path, &DbOptions) -> Result<DatabaseEngine, EngineError>,
    ) -> (tempfile::TempDir, DatabaseEngine, u32, u32, u32, u32) {
        ready_participants_for_fixture();
        let dir = tempfile::tempdir().unwrap();
        let db = open(
            dir.path(),
            &DbOptions {
                compact_after_n_flushes: 0,
                edge_uniqueness: false,
                memtable_flush_threshold: 128 * 1024 * 1024,
                ..Default::default()
            },
        )
        .unwrap();
        let n = db.ensure_node_label("Base").unwrap();
        let other = db.ensure_node_label("Other").unwrap();
        let e = db.ensure_edge_label("E").unwrap();
        let other_e = db.ensure_edge_label("OtherE").unwrap();
        let props = BTreeMap::from([
            ("indexed".into(), PropValue::String("a".into())),
            ("payload".into(), PropValue::String("x".repeat(4096))),
        ]);
        let mut ops = Vec::new();
        for id in 1..=3 {
            let mut row = node(id, n, &format!("n{id}"));
            if id == 3 {
                row.label_ids = NodeLabelSet::from_label_ids([n, other]).unwrap();
            }
            row.props = props.clone();
            ops.push(WalOp::UpsertNode(row));
        }
        let mut fourth = node(4, other, "n4");
        fourth.props = props.clone();
        ops.push(WalOp::UpsertNode(fourth));
        for (id, to) in [(11, 2), (12, 2), (13, 3)] {
            let mut row = edge(id, 1, to, e);
            row.props = props.clone();
            ops.push(WalOp::UpsertEdge(row));
        }
        db.write_op_batch(&ops).unwrap();
        db.flush().unwrap();
        (dir, db, n, other, e, other_e)
    }
    fn wait_ready(db: &DatabaseEngine) {
        let until = Instant::now() + std::time::Duration::from_secs(10);
        loop {
            let ready = db
                .list_node_property_indexes()
                .unwrap()
                .iter()
                .all(|i| i.state == SecondaryIndexState::Ready)
                && db
                    .list_edge_property_indexes()
                    .unwrap()
                    .iter()
                    .all(|i| i.state == SecondaryIndexState::Ready);
            if ready {
                break;
            }
            assert!(Instant::now() < until);
            std::thread::yield_now();
        }
    }
    #[test]
    fn post45_write_fresh_ids_skip_primary_seeks_and_keep_batch_identity() {
        ready_participants_for_fixture();
        let dir = tempfile::tempdir().unwrap();
        let options = DbOptions {
            edge_uniqueness: true,
            ..Default::default()
        };
        let db = DatabaseEngine::open(dir.path(), &options).unwrap();
        db.ensure_node_label("N").unwrap();
        db.ensure_edge_label("E").unwrap();
        let old = db.runtime.published.load_full().view.clone();
        old.memtable.take_preparation_seeks_for_test();
        let a = db
            .upsert_node("N", "a", UpsertNodeOptions::default())
            .unwrap();
        let input = |key: &str| NodeInput {
            labels: vec!["N".into()],
            key: key.into(),
            props: BTreeMap::new(),
            weight: 1.,
            dense_vector: None,
            sparse_vector: None,
        };
        let ids = db
            .batch_upsert_nodes(vec![input("b"), input("b"), input("c")])
            .unwrap();
        assert_eq!(ids[0], ids[1]);
        assert_ne!(ids[0], ids[2]);
        let first = db
            .upsert_edge(a, ids[0], "E", UpsertEdgeOptions::default())
            .unwrap();
        let einput = EdgeInput {
            from: a,
            to: ids[2],
            label: "E".into(),
            props: BTreeMap::new(),
            weight: 1.,
            valid_from: None,
            valid_to: None,
        };
        let edges = db.batch_upsert_edges(vec![einput.clone(), einput]).unwrap();
        assert_eq!(edges[0], edges[1]);
        assert_ne!(first, edges[0]);
        assert_eq!(old.memtable.take_preparation_seeks_for_test(), [0, 0]);
        assert!(old.get_node_raw(a).unwrap().is_none());
        assert!(old.sources().find_edge(first).unwrap().is_none());
        let current = db.runtime.published.load_full().view.clone();
        assert_eq!(current.memtable.current_tombstone_counts(), (0, 0));
        assert_eq!(
            current.memtable.estimated_size(),
            current.memtable.estimated_size_full_for_test()
        );
        assert_eq!(db.get_node(ids[2]).unwrap().unwrap().key, "c");
        assert_eq!(
            db.get_edge_by_triple(a, ids[2], "E").unwrap().unwrap().id,
            edges[0]
        );
        drop((old, current));
        finish(db);
        let reopened = DatabaseEngine::open(dir.path(), &options).unwrap();
        assert_eq!(
            reopened.get_node_by_key("N", "b").unwrap().unwrap().id,
            ids[0]
        );
        assert_eq!(
            reopened
                .get_edge_by_triple(a, ids[2], "E")
                .unwrap()
                .unwrap()
                .id,
            edges[0]
        );
        finish(reopened);
    }
    #[test]
    fn post45_write_preparation_resolves_rotated_indexed_sources_again() {
        ready_participants_for_fixture();
        let dir = tempfile::tempdir().unwrap();
        // This fixture proves re-resolution after rotation, not admission latency.
        let limits = ConcurrencyOptions::default()
            .with_admission_timeout_ms(tests::FUNCTIONAL_MAINTENANCE_ADMISSION_TIMEOUT_MS)
            .with_max_batch_operations(2)
            .with_generation_operation_limit(2);
        let db = DatabaseEngine::open_with_concurrency_options(
            dir.path(),
            &DbOptions {
                compact_after_n_flushes: 0,
                ..Default::default()
            },
            &limits,
        )
        .unwrap();
        for label in ["A", "B"] {
            db.ensure_node_label(label).unwrap();
            db.ensure_node_property_index(
                label,
                SecondaryIndexSpec::equality(vec![SecondaryIndexField::property("indexed")]),
            )
            .unwrap();
        }
        wait_ready(&db);
        db.flush().unwrap();
        let props = |v: &str| BTreeMap::from([("indexed".into(), PropValue::String(v.into()))]);
        let id = db
            .upsert_node(
                "A",
                "k",
                UpsertNodeOptions {
                    props: props("a"),
                    ..Default::default()
                },
            )
            .unwrap();
        db.upsert_node(
            "A",
            "k",
            UpsertNodeOptions {
                props: props("b"),
                ..Default::default()
            },
        )
        .unwrap();
        let old = db.runtime.published.load_full().view.clone();
        old.memtable.take_preparation_seeks_for_test();
        let before = db
            .runtime
            .with_core_ref(|c| Ok(c.active_wal_generation_id))
            .unwrap();
        let input = |labels: Vec<String>, value: &str| NodeInput {
            labels,
            key: "k".into(),
            props: props(value),
            weight: 1.,
            dense_vector: None,
            sparse_vector: None,
        };
        assert_eq!(
            db.batch_upsert_nodes(vec![
                input(vec!["A".into(), "B".into()], "c"),
                input(vec!["B".into()], "d")
            ])
            .unwrap(),
            vec![id, id]
        );
        assert!(
            db.runtime
                .with_core_ref(|c| Ok(c.active_wal_generation_id))
                .unwrap()
                > before
        );
        let current = db.runtime.published.load_full().view.clone();
        assert!(!old.memtable.same_generation(&current.memtable));
        assert_eq!(old.memtable.take_preparation_seeks_for_test(), [0, 0]);
        assert_eq!(current.memtable.take_preparation_seeks_for_test(), [1, 0]);
        assert_eq!(
            current.memtable.take_older_preparation_ids_for_test(),
            [1, 0, 1, 0]
        );
        let query = |label: &str, value: &str| NodeQuery {
            label_filter: Some(NodeLabelFilter {
                labels: vec![label.into()],
                mode: LabelMatchMode::All,
            }),
            filter: Some(NodeFilterExpr::PropertyEquals {
                key: "indexed".into(),
                value: PropValue::String(value.into()),
            }),
            ..Default::default()
        };
        assert_eq!(
            old.query_node_ids_outcome(&query("A", "b"))
                .unwrap()
                .value
                .items,
            vec![id]
        );
        for (label, value) in [("A", "a"), ("A", "b"), ("A", "c"), ("B", "c")] {
            assert!(db
                .query_node_ids(&query(label, value))
                .unwrap()
                .items
                .is_empty());
        }
        assert_eq!(db.query_node_ids(&query("B", "d")).unwrap().items, vec![id]);
        assert!(db.get_node_by_key("A", "k").unwrap().is_none());
        assert_eq!(db.get_node_by_key("B", "k").unwrap().unwrap().id, id);
        assert_eq!(
            current.memtable.estimated_size(),
            current.memtable.estimated_size_full_for_test()
        );
        drop((old, current));
        finish(db);
        let reopened = DatabaseEngine::open(dir.path(), &DbOptions::default()).unwrap();
        assert!(reopened
            .query_node_ids(&query("A", "b"))
            .unwrap()
            .items
            .is_empty());
        assert_eq!(
            reopened.query_node_ids(&query("B", "d")).unwrap().items,
            vec![id]
        );
        finish(reopened);
    }
    #[test]
    fn post45_write_active_opinions_skip_older_identity_projection() {
        let (dir, db, n, other, e, _) = fixture();
        let owner = db.get_edge_by_triple(1, 2, "E").unwrap().unwrap().id;
        let nonowner = if owner == 11 { 12 } else { 11 };
        db.write_op_batch(&[
            WalOp::UpsertNode(node(1, n, "hot")),
            WalOp::UpsertEdge(edge(nonowner, 1, 2, e)),
        ])
        .unwrap();
        let old = db.runtime.published.load_full().view.clone();
        old.memtable.take_older_preparation_ids_for_test();
        old.memtable.take_preparation_seeks_for_test();
        db.write_op_batch(&[
            WalOp::UpsertNode(node(1, n, "intermediate")),
            WalOp::DeleteNode {
                id: 1,
                deleted_at: 8,
            },
            WalOp::UpsertNode(node(1, other, "final")),
            WalOp::UpsertEdge(edge(nonowner, 3, 2, e)),
            WalOp::DeleteEdge {
                id: nonowner,
                deleted_at: 8,
            },
            WalOp::UpsertEdge(edge(nonowner, 2, 3, e)),
        ])
        .unwrap();
        let work = old.memtable.take_older_preparation_ids_for_test();
        assert_eq!(old.memtable.take_preparation_seeks_for_test(), [1, 1]);
        assert_eq!(db.get_node(1).unwrap().unwrap().key, "final");
        assert_eq!(old.get_node_raw(1).unwrap().unwrap().key, "hot");
        assert_eq!(
            old.sources()
                .find_edge_by_triple(1, 2, e)
                .unwrap()
                .unwrap()
                .id,
            owner
        );
        assert_eq!(db.get_edge(nonowner).unwrap().unwrap().from, 2);
        assert_eq!(db.get_edge_by_triple(1, 2, "E").unwrap().unwrap().id, owner);
        assert_eq!(db.get_node_by_key("Other", "final").unwrap().unwrap().id, 1);
        eprintln!("post45 active older preparation={work:?}");
        assert_eq!(work, [0; 4]);
        drop(old);
        db.flush().unwrap();
        finish(db);
        let reopened = DatabaseEngine::open(
            dir.path(),
            &DbOptions {
                edge_uniqueness: false,
                ..Default::default()
            },
        )
        .unwrap();
        assert_eq!(reopened.get_node(1).unwrap().unwrap().key, "final");
        assert_eq!(
            reopened.get_edge_by_triple(1, 2, "E").unwrap().unwrap().id,
            owner
        );
        assert_eq!(reopened.get_edge(nonowner).unwrap().unwrap().from, 2);
        finish(reopened);
    }
    #[test]
    fn post45_write_unrelated_ready_indexes_skip_old_full_hydration() {
        // This is a preparation-work proof, not an admission latency test.
        let (dir, db, n, other, e, other_e) = fixture_with_open(tests::open_functional_maintenance_fixture);
        let spec = SecondaryIndexSpec::equality(vec![SecondaryIndexField::property("indexed")]);
        db.ensure_node_property_index("Other", spec.clone())
            .unwrap();
        db.ensure_edge_property_index("OtherE", spec.clone())
            .unwrap();
        wait_ready(&db);
        let old = db.runtime.published.load_full().view.clone();
        old.memtable.take_older_preparation_ids_for_test();
        old.memtable.take_preparation_seeks_for_test();
        let mut row = node(2, other, "moved");
        row.props
            .insert("indexed".into(), PropValue::String("b".into()));
        let mut erow = edge(12, 1, 2, other_e);
        erow.props = row.props.clone();
        db.write_op_batch(&[WalOp::UpsertNode(row), WalOp::UpsertEdge(erow)])
            .unwrap();
        let work = old.memtable.take_older_preparation_ids_for_test();
        assert_eq!(old.memtable.take_preparation_seeks_for_test(), [1, 1]);
        assert_eq!(
            old.get_node_raw(2).unwrap().unwrap().label_ids.as_slice(),
            &[n]
        );
        let q = NodeQuery {
            label_filter: Some(NodeLabelFilter {
                labels: vec!["Other".into()],
                mode: LabelMatchMode::All,
            }),
            filter: Some(NodeFilterExpr::PropertyEquals {
                key: "indexed".into(),
                value: PropValue::String("b".into()),
            }),
            ..Default::default()
        };
        assert_eq!(db.query_node_ids(&q).unwrap().items, vec![2]);
        let eq = EdgeQuery {
            label: Some("OtherE".into()),
            filter: Some(EdgeFilterExpr::PropertyEquals {
                key: "indexed".into(),
                value: PropValue::String("b".into()),
            }),
            ..Default::default()
        };
        assert_eq!(db.query_edge_ids(&eq).unwrap().edge_ids, vec![12]);
        eprintln!("post45 unrelated older preparation={work:?}");
        assert_eq!(work, [1, 1, 0, 0]);
        db.ensure_node_property_index("Base", spec.clone()).unwrap();
        db.ensure_edge_property_index("E", spec).unwrap();
        wait_ready(&db);
        let current = db.runtime.published.load_full().view.clone();
        current.memtable.take_older_preparation_ids_for_test();
        let mut row = node(3, n, "n3");
        row.props
            .insert("indexed".into(), PropValue::String("b".into()));
        let mut erow = edge(13, 2, 3, e);
        erow.props = row.props.clone();
        db.write_op_batch(&[WalOp::UpsertNode(row), WalOp::UpsertEdge(erow)])
            .unwrap();
        assert_eq!(
            current.memtable.take_older_preparation_ids_for_test(),
            [1, 1, 1, 1]
        );
        assert_eq!(
            db.query_node_ids(&NodeQuery {
                label_filter: Some(NodeLabelFilter {
                    labels: vec!["Base".into()],
                    mode: LabelMatchMode::All
                }),
                ..q.clone()
            })
            .unwrap()
            .items,
            vec![3]
        );
        assert_eq!(
            db.query_edge_ids(&EdgeQuery {
                label: Some("E".into()),
                ..eq.clone()
            })
            .unwrap()
            .edge_ids,
            vec![13]
        );
        assert_eq!(
            db.query_node_ids(&NodeQuery {
                filter: Some(NodeFilterExpr::PropertyEquals {
                    key: "indexed".into(),
                    value: PropValue::String("a".into())
                }),
                ..q.clone()
            })
            .unwrap()
            .items,
            vec![4]
        );
        drop(current);
        drop(old);
        db.flush().unwrap();
        finish(db);
        let reopened = DatabaseEngine::open(
            dir.path(),
            &DbOptions {
                edge_uniqueness: false,
                ..Default::default()
            },
        )
        .unwrap();
        assert_eq!(reopened.query_node_ids(&q).unwrap().items, vec![2]);
        assert_eq!(reopened.query_edge_ids(&eq).unwrap().edge_ids, vec![12]);
        assert_eq!(
            reopened.get_node(3).unwrap().unwrap().labels.as_slice(),
            &["Base"]
        );
        assert_eq!(reopened.get_edge(13).unwrap().unwrap().from, 2);
        finish(reopened);
    }
    #[test]
    fn post45_write_build_target_uses_all_old_labels_and_replays_suffix() {
        let (dir, db, n, other, e, _) = fixture();
        let (ready, release) = db.set_secondary_index_build_pause();
        let spec = SecondaryIndexSpec::equality(vec![SecondaryIndexField::property("indexed")]);
        db.ensure_node_property_index("Other", spec).unwrap();
        ready
            .recv_timeout(std::time::Duration::from_secs(10))
            .unwrap();
        let old = db.runtime.published.load_full().view.clone();
        old.memtable.take_older_preparation_ids_for_test();
        let mut remove = node(3, n, "n3");
        remove
            .props
            .insert("indexed".into(), PropValue::String("b".into()));
        let mut add = node(2, other, "moved");
        add.props = remove.props.clone();
        let mut edge_row = edge(13, 1, 3, e);
        edge_row.props = remove.props.clone();
        db.write_op_batch(&[
            WalOp::UpsertNode(remove),
            WalOp::UpsertNode(add),
            WalOp::UpsertEdge(edge_row),
        ])
        .unwrap();
        let work = old.memtable.take_older_preparation_ids_for_test();
        release.send(()).unwrap();
        wait_ready(&db);
        let query = |value: &str| NodeQuery {
            label_filter: Some(NodeLabelFilter {
                labels: vec!["Other".into()],
                mode: LabelMatchMode::All,
            }),
            filter: Some(NodeFilterExpr::PropertyEquals {
                key: "indexed".into(),
                value: PropValue::String(value.into()),
            }),
            ..Default::default()
        };
        assert_eq!(db.query_node_ids(&query("a")).unwrap().items, vec![4]);
        assert_eq!(db.query_node_ids(&query("b")).unwrap().items, vec![2]);
        assert_eq!(
            old.query_node_ids_outcome(&query("a")).unwrap().value.items,
            vec![3, 4]
        );
        assert_eq!(
            db.get_edge(13).unwrap().unwrap().props.get("indexed"),
            Some(&PropValue::String("b".into()))
        );
        eprintln!("post45 build old-label preparation={work:?}");
        assert_eq!(work, [2, 1, 1, 0]);
        drop(old);
        db.flush().unwrap();
        finish(db);
        let reopened = DatabaseEngine::open(
            dir.path(),
            &DbOptions {
                edge_uniqueness: false,
                ..Default::default()
            },
        )
        .unwrap();
        assert_eq!(reopened.query_node_ids(&query("a")).unwrap().items, vec![4]);
        assert_eq!(reopened.query_node_ids(&query("b")).unwrap().items, vec![2]);
        finish(reopened);
    }
    #[test]
    fn post45_write_settled_catalog_needs_no_scheduler_scan() {
        let (_dir, db, _, _, _, _) = fixture();
        for i in 0..24 {
            db.ensure_node_property_index(
                "Base",
                SecondaryIndexSpec::equality(vec![SecondaryIndexField::property(format!(
                    "field{i}"
                ))]),
            )
            .unwrap();
            wait_ready(&db);
        }
        {
            db.runtime
                .core
                .lock()
                .unwrap()
                .as_mut()
                .unwrap()
                .post45_work[4] = 0;
        }
        for i in 0..10 {
            db.upsert_node("Base", &format!("hot{i}"), UpsertNodeOptions::default())
                .unwrap();
        }
        let visits = db
            .runtime
            .core
            .lock()
            .unwrap()
            .as_ref()
            .unwrap()
            .post45_work[4];
        finish(db);
        assert_eq!(
            visits, 0,
            "settled declarations must not be walked by ordinary writes"
        );
    }

    #[test]
    fn post45_write_protected_components_skip_metadata_reads() {
        let (_dir, db, _, _, _, _) = fixture();
        let spec = SecondaryIndexSpec::equality(vec![SecondaryIndexField::property("indexed")]);
        let index = db.ensure_node_property_index("Base", spec.clone()).unwrap();
        wait_ready(&db);
        let old = db.runtime.published.load_full().view.clone();
        let paths: Vec<_> = old
            .sources
            .segments
            .iter()
            .flat_map(|s| s.component_paths())
            .filter(|(_, _, p)| p.to_string_lossy().contains("secondary_indexes/"))
            .map(|(id, _, p)| segment_dir(_dir.path(), id).join(p))
            .collect();
        assert!(!paths.is_empty());
        let root_reads = {
            let mut guard = db.runtime.core.lock().unwrap();
            let c = guard.as_mut().unwrap();
            c.candidate.as_mut().unwrap().ddl.cleanup_paths = paths.iter().cloned().collect();
            c.candidate.as_mut().unwrap().ddl.drop_paths =
                paths.iter().cloned().map(|p| (p, index.index_id)).collect();
            c.candidate.as_mut().unwrap().ddl.sweep_pending = false;
            c.candidate.as_mut().unwrap().ddl.last_cleanup = None;
            c.post45_work[1] = 0;
            c.cleanup_candidate_components();
            c.post45_work[1]
        };
        db.drop_node_property_index("Base", spec).unwrap();
        for label in ["Barrier1", "Barrier2"] {
            db.set_prune_policy(
                label,
                PrunePolicy {
                    max_age_ms: None,
                    max_weight: Some(0.),
                    label: None,
                },
            )
            .unwrap();
        }
        let (pin_reads, unprotected_reads, error_reads) = {
            let mut guard = db.runtime.core.lock().unwrap();
            let c = guard.as_mut().unwrap();
            assert!(!c
                .manifest_write_lock
                .gate()
                .unwrap()
                .recovery_references(&c.db_dir)
                .unwrap()
                .secondary_indexes
                .contains_key(&index.index_id));
            let pins = c.candidate.as_ref().unwrap().domain.component_pins();
            assert!(paths
                .iter()
                .all(|p| pins
                    .iter()
                    .any(|(id, _, relative)| segment_dir(&c.db_dir, *id).join(relative) == *p)));
            c.candidate.as_mut().unwrap().ddl.cleanup_paths = paths.iter().cloned().collect();
            c.candidate.as_mut().unwrap().ddl.sweep_pending = false;
            c.candidate.as_mut().unwrap().ddl.last_cleanup = None;
            c.post45_work[1] = 0;
            c.cleanup_candidate_components();
            let pin_reads = c.post45_work[1];
            let segment = paths[0]
                .ancestors()
                .find(|p| {
                    p.file_name()
                        .and_then(|n| n.to_str())
                        .is_some_and(|n| n.starts_with("seg_"))
                })
                .unwrap();
            let orphan = segment.join("unreferenced-test-file");
            std::fs::write(&orphan, b"orphan").unwrap();
            c.candidate
                .as_mut()
                .unwrap()
                .ddl
                .cleanup_paths
                .insert(orphan.clone());
            c.candidate.as_mut().unwrap().ddl.last_cleanup = None;
            c.post45_work[1] = 0;
            c.cleanup_candidate_components();
            let unprotected_reads = c.post45_work[1];
            assert!(!orphan.exists());
            std::fs::write(&orphan, b"keep on unreadable metadata").unwrap();
            let tmp =
                segment.join(crate::segment_components::SEGMENT_COMPONENT_MANIFEST_TMP_FILENAME);
            assert!(!tmp.exists());
            std::fs::create_dir(&tmp).unwrap();
            c.candidate
                .as_mut()
                .unwrap()
                .ddl
                .cleanup_paths
                .insert(orphan.clone());
            c.candidate.as_mut().unwrap().ddl.last_cleanup = None;
            c.post45_work[1] = 0;
            c.cleanup_candidate_components();
            let error_reads = c.post45_work[1];
            assert!(orphan.exists());
            std::fs::remove_dir(tmp).unwrap();
            (pin_reads, unprotected_reads, error_reads)
        };
        assert!(paths.iter().all(|p| p.exists()));
        drop(old);
        finish(db);
        assert_eq!(
            (root_reads, pin_reads),
            (0, 0),
            "root/pin protected paths must skip metadata reads"
        );
        assert_eq!((unprotected_reads, error_reads), (1, 1));
    }

    #[test]
    fn post45_write_segment_cleanup_coalesces_actual_pin_inventory() {
        let (dir, db, _, _, _, _) = fixture();
        db.upsert_node("Base", "extra", UpsertNodeOptions::default())
            .unwrap();
        db.flush().unwrap();
        let old = db.runtime.published.load_full().view.clone();
        let old_ids: Vec<_> = old.sources.segments.iter().map(|s| s.segment_id).collect();
        assert_eq!(old_ids.len(), 2);
        db.compact().unwrap().unwrap();
        for label in ["CleanupBarrier1", "CleanupBarrier2"] {
            db.set_prune_policy(
                label,
                PrunePolicy {
                    max_age_ms: None,
                    max_weight: Some(0.),
                    label: None,
                },
            )
            .unwrap();
        }
        let (coalesced, advanced, new_work, final_attempt) = {
            let mut guard = db.runtime.core.lock().unwrap();
            let c = guard.as_mut().unwrap();
            let refs = c
                .manifest_write_lock
                .gate()
                .unwrap()
                .recovery_references(&c.db_dir)
                .unwrap();
            assert!(old_ids.iter().all(|id| !refs.segments.contains(id)));
            assert!(old_ids.iter().all(|id| c
                .candidate
                .as_ref()
                .unwrap()
                .domain
                .storage_pins()
                .contains(id)));
            let dirs: Vec<_> = old_ids
                .iter()
                .map(|id| segment_dir(dir.path(), *id))
                .collect();
            c.defer_segment_dir_cleanup(dirs.clone());
            c.post45_work[2] = 0;
            c.post45_work[3] = 0;
            let now = Instant::now();
            c.candidate.as_mut().unwrap().segment_cleanup_now = Some(now);
            for _ in 0..4 {
                db.runtime
                    .publish_locked(c, PublishImpact::SnapshotOnly, false)
                    .unwrap();
                db.runtime
                    .publish_locked(c, PublishImpact::NoPublish, false)
                    .unwrap();
            }
            c.candidate.as_mut().unwrap().segment_cleanup_now = None;
            let coalesced = (c.post45_work[2], c.post45_work[3]);
            assert!(dirs.iter().all(|p| p.exists()));
            c.retry_deferred_segment_cleanup_at(false, || {
                now + std::time::Duration::from_millis(10)
            });
            let advanced = c.post45_work[3];
            c.defer_segment_dir_cleanup(dirs.clone());
            c.retry_deferred_segment_cleanup_at(false, || {
                now + std::time::Duration::from_millis(10)
            });
            let new_work = c.post45_work[3];
            c.retry_deferred_segment_cleanup_at(true, || {
                now + std::time::Duration::from_millis(10)
            });
            let final_attempt = c.post45_work[3];
            // Even an explicit final attempt keeps unknown segment identities.
            let unknown = dir.path().join("seg_unknown");
            std::fs::create_dir(&unknown).unwrap();
            c.defer_segment_dir_cleanup([unknown.clone()]);
            c.retry_deferred_segment_cleanup_at(true, || now);
            assert!(unknown.exists());
            (coalesced, advanced, new_work, final_attempt)
        };
        drop(old);
        let until = Instant::now() + std::time::Duration::from_secs(10);
        while old_ids
            .iter()
            .any(|id| segment_dir(dir.path(), *id).exists())
        {
            assert!(
                Instant::now() < until,
                "lifecycle must retire eligible directories without foreground writes"
            );
            std::thread::yield_now();
        }
        // A fenced final attempt does no inventory/deletion.
        {
            let mut guard = db.runtime.core.lock().unwrap();
            let c = guard.as_mut().unwrap();
            let before = c.post45_work[3];
            c.manifest_write_lock.establish();
            c.retry_deferred_segment_cleanup_at(true, || {
                panic!("fenced cleanup must not read clock")
            });
            assert_eq!(c.post45_work[3], before);
        }
        let stats = db.runtime.candidate.as_ref().unwrap().stats.clone();
        assert!(matches!(
            db.close_fast(),
            Err(EngineError::CommitOutcome {
                state: CommitOutcomeState::Indeterminate,
                ..
            })
        ));
        drop(db);
        let until = Instant::now() + std::time::Duration::from_secs(10);
        while !matches!(
            stats.read_full().unwrap().close_state,
            ReclamationCloseState::Drained
        ) {
            assert!(Instant::now() < until);
            std::thread::yield_now();
        }
        assert_eq!(coalesced, (1, 1));
        assert_eq!((advanced, new_work, final_attempt), (2, 3, 4));
    }

    #[test]
    fn post45_write_removed_active_opinions_shadow_persisted_records() {
        let (_dir, db, n, _, e, _) = fixture();
        db.write_op_batch(&[
            WalOp::DeleteNode {
                id: 1,
                deleted_at: 8,
            },
            WalOp::DeleteEdge {
                id: 13,
                deleted_at: 8,
            },
        ])
        .unwrap();
        let old = db.runtime.published.load_full().view.clone();
        old.memtable.take_older_preparation_ids_for_test();
        db.write_op_batch(&[
            WalOp::DeleteNode {
                id: 1,
                deleted_at: 9,
            },
            WalOp::DeleteEdge {
                id: 13,
                deleted_at: 9,
            },
            WalOp::UpsertNode(node(1, n, "returned")),
            WalOp::UpsertEdge(edge(13, 3, 2, e)),
        ])
        .unwrap();
        let work = old.memtable.take_older_preparation_ids_for_test();
        assert!(old.get_node_raw(1).unwrap().is_none());
        assert!(old.get_edge(13).unwrap().is_none());
        assert_eq!(db.get_node(1).unwrap().unwrap().key, "returned");
        assert_eq!(db.get_edge(13).unwrap().unwrap().from, 3);
        assert!(db.get_node_by_key("Base", "n1").unwrap().is_none());
        assert!(db.get_edge_by_triple(1, 3, "E").unwrap().is_none());
        drop(old);
        finish(db);
        assert_eq!(work, [0; 4]);
    }

    #[test]
    fn post45_write_pending_cache_preserves_order_and_open_retry() {
        let (dir, db, n, _, _, _) = fixture();
        let spec = SecondaryIndexSpec::equality(vec![SecondaryIndexField::property("indexed")]);
        db.ensure_node_property_index("Base", spec.clone()).unwrap();
        wait_ready(&db);
        let mut manifest = db.manifest().unwrap();
        let template = manifest.secondary_indexes[0].clone();
        let mut entries: Vec<_> = (1..=32)
            .map(|id| {
                let mut entry = template.clone();
                entry.index_id = id;
                entry.target = SecondaryIndexTarget::NodeProperty {
                    label_id: n,
                    prop_key: format!("field{id}"),
                };
                entry
            })
            .collect();
        entries[2].state = SecondaryIndexState::Building;
        entries[23].state = SecondaryIndexState::Building;
        entries[6].state = SecondaryIndexState::Failed;
        let fixed = crate::memtable::secondary::FixedCatalog::new(&entries).unwrap();
        assert_eq!(fixed.first_building_entry().unwrap().index_id, 3);
        entries[2].state = SecondaryIndexState::Ready;
        let fixed = crate::memtable::secondary::FixedCatalog::new(&entries).unwrap();
        assert_eq!(fixed.first_building_entry().unwrap().index_id, 24);
        entries.remove(23);
        let fixed = crate::memtable::secondary::FixedCatalog::new(&entries).unwrap();
        assert!(fixed.first_building_entry().is_none());
        entries[6].state = SecondaryIndexState::Building;
        let fixed = crate::memtable::secondary::FixedCatalog::new(&entries).unwrap();
        assert_eq!(fixed.first_building_entry().unwrap().index_id, 7);
        // Real recovery installs one immutable catalog for multiple pending builds.
        finish(db);
        let mut pending: Vec<_> = (1..=3)
            .map(|id| {
                let mut entry = template.clone();
                entry.index_id = id;
                entry.target = SecondaryIndexTarget::NodeProperty {
                    label_id: n,
                    prop_key: format!("pending{id}"),
                };
                entry.state = SecondaryIndexState::Building;
                entry
            })
            .collect();
        pending[0].state = SecondaryIndexState::Failed;
        manifest.secondary_indexes = pending;
        manifest.next_secondary_index_id = 4;
        crate::manifest::write_manifest(dir.path(), &manifest).unwrap();
        let reopened = DatabaseEngine::open(
            dir.path(),
            &DbOptions {
                edge_uniqueness: false,
                compact_after_n_flushes: 0,
                ..Default::default()
            },
        )
        .unwrap();
        let until = Instant::now() + std::time::Duration::from_secs(10);
        loop {
            let entries = reopened.list_node_property_indexes().unwrap();
            if entries[1..]
                .iter()
                .all(|e| e.state == SecondaryIndexState::Ready)
            {
                assert_eq!(entries[0].state, SecondaryIndexState::Failed);
                break;
            }
            assert!(Instant::now() < until);
            std::thread::yield_now();
        }
        let retry = SecondaryIndexSpec::equality(vec![SecondaryIndexField::property("pending1")]);
        assert_eq!(
            reopened
                .ensure_node_property_index("Base", retry)
                .unwrap()
                .index_id,
            1
        );
        wait_ready(&reopened);
        {
            let guard = reopened.runtime.core.lock().unwrap();
            let c = guard.as_ref().unwrap();
            let fixed = c.fixed_secondary_catalog.as_ref().unwrap();
            assert_eq!(fixed.entries, c.manifest.secondary_indexes);
            assert!(fixed.first_building_entry().is_none());
            assert!(SharedArc::ptr_eq(
                fixed,
                &c.memtable.fixed_secondary_catalog().unwrap()
            ));
        }
        finish(reopened);
    }
    #[test]
    fn post45_write_cleanup_retains_unreadable_root_authority() {
        let (dir, db, _, _, _, _) = fixture();
        let mut guard = db.runtime.core.lock().unwrap();
        let c = guard.as_mut().unwrap();
        let segment = segment_dir(dir.path(), c.manifest.segments[0].id);
        let file = segment.join("unreferenced-test-file");
        std::fs::write(&file, b"must survive").unwrap();
        let retired = segment_dir(dir.path(), 999);
        std::fs::create_dir(&retired).unwrap();
        c.candidate
            .as_mut()
            .unwrap()
            .ddl
            .cleanup_paths
            .insert(file.clone());
        c.candidate.as_mut().unwrap().ddl.last_cleanup = None;
        c.candidate.as_mut().unwrap().ddl.sweep_pending = false;
        c.defer_segment_dir_cleanup([retired.clone()]);
        let previous = dir.path().join(crate::manifest::MANIFEST_PREV);
        let bytes = std::fs::read(&previous).unwrap();
        std::fs::remove_file(&previous).unwrap();
        std::fs::create_dir(&previous).unwrap();
        // A fresh authority cache must read, and reject, the unreadable union.
        let original = std::mem::replace(
            &mut c.manifest_write_lock,
            Arc::new(ManifestLock::new(true)),
        );
        c.post45_work = [0; 5];
        c.cleanup_candidate_components();
        c.retry_deferred_segment_cleanup_at(true, Instant::now);
        let work = c.post45_work;
        let retained = file.exists() && retired.exists();
        c.manifest_write_lock = original;
        std::fs::remove_dir(previous.clone()).unwrap();
        std::fs::write(previous, bytes).unwrap();
        drop(guard);
        finish(db);
        assert!(retained);
        assert_eq!((work[1], work[2], work[3]), (0, 1, 0));
    }
}
#[cfg(all(test, feature = "production-concurrency"))]
mod primary_index_tests {
    use super::p45b1_tests::{edge, finish, node};
    use super::*;

    #[test]
    fn primary_index_sparse_model_opinions_order_and_duplicates() {
        let dir = tempfile::tempdir().unwrap();
        let db = DatabaseEngine::open(dir.path(), &DbOptions::default()).unwrap();
        let n = db.ensure_node_label("N").unwrap();
        let e = db.ensure_edge_label("E").unwrap();
        let m = db
            .with_core_ref(|core| {
                Memtable::new_candidate_with_catalog(
                    core.memtable.participant_handle().unwrap(),
                    core.fixed_secondary_catalog.as_ref().unwrap().clone(),
                    &core.candidate.as_ref().unwrap().domain,
                )
            })
            .unwrap();
        let mut seed = 0x0020_2610_251d_u64;
        let mut ids = vec![0, 1, 17, u64::MAX - 1, u64::MAX];
        for _ in 0..14 {
            seed ^= seed << 13;
            seed ^= seed >> 7;
            seed ^= seed << 17;
            ids.push(seed);
        }
        ids.sort_unstable();
        ids.dedup();
        let mut model: BTreeMap<u64, Vec<(u64, bool)>> = BTreeMap::new();
        for i in 0..400_u64 {
            seed ^= seed << 13;
            seed ^= seed >> 7;
            seed ^= seed << 17;
            let id = if i % 4 == 0 {
                1
            } else {
                ids[seed as usize % ids.len()]
            };
            let live = i % 5 != 0;
            let node_op = if live {
                let mut row = node(id, n, &format!("{id}-{i}"));
                row.weight = i as f32;
                WalOp::UpsertNode(row)
            } else {
                WalOp::DeleteNode {
                    id,
                    deleted_at: i as i64,
                }
            };
            let edge_op = if live {
                let mut row = edge(id, 0, 1, e);
                row.weight = i as f32;
                WalOp::UpsertEdge(row)
            } else {
                WalOp::DeleteEdge {
                    id,
                    deleted_at: i as i64,
                }
            };
            // Valid internal fixtures, bypassing only public ID allocation. Every
            // actual install has a unique ascending operation sequence, including 0.
            m.apply_op_for_test(&node_op, 2 * i).unwrap();
            m.apply_op_for_test(&edge_op, 2 * i + 1).unwrap();
            model.entry(id).or_default().push((i, live));
        }
        let mut requests = ids
            .iter()
            .flat_map(|&id| [id, id])
            .chain([3, 3, 42, u64::MAX / 2])
            .collect::<Vec<_>>();
        requests.sort_unstable();
        for s in [0, 1, 15, 63, 127, 399] {
            let view_seq = 2 * s + 1;
            let opinion = |id: u64| {
                model
                    .get(&id)
                    .and_then(|history| history.iter().rev().find(|&&(i, _)| i <= s))
                    .copied()
            };
            let mut expected = Vec::new();
            for &id in &ids {
                let want = opinion(id);
                let nr = m.get_node_at(id, view_seq).unwrap();
                let er = m.get_edge_at(id, view_seq).unwrap();
                assert_eq!(
                    nr.as_ref().map(|r| r.weight),
                    want.filter(|x| x.1).map(|x| x.0 as f32)
                );
                assert_eq!(
                    er.as_ref().map(|r| r.weight),
                    want.filter(|x| x.1).map(|x| x.0 as f32)
                );
                assert_eq!(
                    m.node_tombstone_at(id, view_seq).unwrap().is_some(),
                    want.is_some_and(|x| !x.1)
                );
                assert_eq!(
                    m.edge_tombstone_at(id, view_seq).unwrap().is_some(),
                    want.is_some_and(|x| !x.1)
                );
                if want.is_some_and(|x| x.1) {
                    expected.push(id);
                }
            }
            assert_eq!(m.visible_node_ids_at(view_seq).unwrap(), expected);
            let mut bound = std::ops::Bound::Unbounded;
            let mut edges = Vec::new();
            loop {
                let (part, last, done) = m.refill_edge_ids_at(view_seq, bound).unwrap();
                edges.extend(part);
                if done {
                    break;
                }
                bound = std::ops::Bound::Excluded(last.expect("progress"));
            }
            assert_eq!(edges, expected);
            let mut found = Vec::new();
            let mut missing = Vec::new();
            m.visit_nodes_sorted_at(
                &requests,
                view_seq,
                crate::row_projection::VectorSelection::None,
                &mut missing,
                &mut |id, row| {
                    assert_eq!(m.participant_handle().unwrap().snapshot().0, 0);
                    assert_eq!(row.weight, opinion(id).unwrap().0 as f32);
                    found.push(id);
                },
            )
            .unwrap();
            assert_eq!(
                found,
                requests
                    .iter()
                    .copied()
                    .filter(|&id| opinion(id).is_some_and(|x| x.1))
                    .collect::<Vec<_>>()
            );
            assert_eq!(
                missing,
                requests
                    .iter()
                    .copied()
                    .filter(|&id| opinion(id).is_none())
                    .collect::<Vec<_>>()
            );
        }
        drop(m);
        finish(db);
    }

    #[test]
    fn primary_index_structure_inventory_is_independent_of_charges() {
        let dir = tempfile::tempdir().unwrap();
        let db = DatabaseEngine::open(dir.path(), &DbOptions::default()).unwrap();
        let n = db.ensure_node_label("N").unwrap();
        let e = db.ensure_edge_label("E").unwrap();
        let m = db
            .with_core_ref(|core| {
                Memtable::new_candidate_with_catalog(
                    core.memtable.participant_handle().unwrap(),
                    core.fixed_secondary_catalog.as_ref().unwrap().clone(),
                    &core.candidate.as_ref().unwrap().domain,
                )
            })
            .unwrap();
        let check = |expected| {
            assert_eq!(m.primary_structure_inventory_for_test(), [expected; 2]);
            assert_eq!(m.estimated_size(), m.estimated_size_full_for_test());
        };
        check((0, 0, 0));
        // First versions are allocation-free singleton histories, including 0.
        m.apply_op_for_test(&WalOp::UpsertNode(node(1, n, "one")), 0)
            .unwrap();
        m.apply_op_for_test(&WalOp::UpsertEdge(edge(1, 0, 1, e)), 1)
            .unwrap();
        check((1, 0, 0));
        let mut next_seq = 2;
        for versions in 2..=4097_usize {
            let tombstone = versions == 2 || versions % 5 == 0;
            let node_op = if tombstone {
                WalOp::DeleteNode {
                    id: 1,
                    deleted_at: versions as i64,
                }
            } else {
                WalOp::UpsertNode(node(1, n, "one"))
            };
            let edge_op = if tombstone {
                WalOp::DeleteEdge {
                    id: 1,
                    deleted_at: versions as i64,
                }
            } else {
                WalOp::UpsertEdge(edge(1, 0, 1, e))
            };
            m.apply_op_for_test(&node_op, next_seq).unwrap();
            m.apply_op_for_test(&edge_op, next_seq + 1).unwrap();
            next_seq += 2;
            if [2, 3, 15, 16, 17, 255, 256, 257, 4096, 4097].contains(&versions) {
                let (mut span, mut chunks) = (16, 0);
                loop {
                    chunks += versions.div_ceil(span);
                    if versions <= span {
                        break;
                    }
                    span *= 16;
                }
                check((1, versions - 1, chunks));
            }
        }
        // Sparse fresh live/tombstone IDs add cells, not history containers.
        for id in [0, 17, u64::MAX] {
            let node_op = if id == 0 {
                WalOp::UpsertNode(node(id, n, "fresh"))
            } else {
                WalOp::DeleteNode { id, deleted_at: 7 }
            };
            let edge_op = if id == 0 {
                WalOp::UpsertEdge(edge(id, 0, 1, e))
            } else {
                WalOp::DeleteEdge { id, deleted_at: 7 }
            };
            m.apply_op_for_test(&node_op, next_seq).unwrap();
            m.apply_op_for_test(&edge_op, next_seq + 1).unwrap();
            next_seq += 2;
        }
        check((4, 4096, 277));
        // Promote a first-tombstone cell with another tombstone, then reinsert.
        m.apply_op_for_test(
            &WalOp::DeleteNode {
                id: 17,
                deleted_at: 8,
            },
            next_seq,
        )
        .unwrap();
        m.apply_op_for_test(
            &WalOp::DeleteEdge {
                id: 17,
                deleted_at: 8,
            },
            next_seq + 1,
        )
        .unwrap();
        check((4, 4097, 278));
        m.apply_op_for_test(&WalOp::UpsertNode(node(17, n, "reinserted")), next_seq + 2)
            .unwrap();
        m.apply_op_for_test(&WalOp::UpsertEdge(edge(17, 0, 1, e)), next_seq + 3)
            .unwrap();
        check((4, 4098, 278));
        drop(m);
        finish(db);
    }

    #[test]
    fn primary_index_descending_and_duplicate_replay_stay_read_only() {
        for bad_seq in [8, 9] {
            let dir = tempfile::tempdir().unwrap();
            let mut manifest = crate::manifest::default_manifest();
            manifest.node_label_tokens.insert("N".into(), 1);
            manifest.next_node_label_id = 2;
            crate::manifest::write_manifest(dir.path(), &manifest).unwrap();
            let mut wal = crate::wal::WalWriter::open_generation(dir.path(), 0).unwrap();
            wal.append(&WalOp::UpsertNode(node(1, 1, "first")), 9)
                .unwrap();
            wal.append(&WalOp::UpsertNode(node(1, 1, "invalid")), bad_seq)
                .unwrap();
            wal.sync().unwrap();
            drop(wal);
            let wal_path = crate::wal::wal_generation_path(dir.path(), 0);
            let current = dir.path().join(crate::manifest::MANIFEST_CURRENT);
            let before_wal = std::fs::read(&wal_path).unwrap();
            let before_manifest = std::fs::read(&current).unwrap();
            assert!(
                matches!(DatabaseEngine::open(dir.path(),&DbOptions::default()),Err(EngineError::CorruptWal(message)) if message.contains("nonmonotonic"))
            );
            assert_eq!(std::fs::read(wal_path).unwrap(), before_wal);
            assert_eq!(std::fs::read(current).unwrap(), before_manifest);
        }
    }
}
