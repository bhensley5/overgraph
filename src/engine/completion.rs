#![allow(dead_code)] // Private runtime substrate; public engine routing is a3.

use super::directory_identity::{DirectoryIdentity, DirectoryPin};
use super::reclamation::{DomainHandle, SessionProducer};
use crate::error::EngineError;
use crate::types::ConcurrencyOptions;
use std::collections::LinkedList;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicU8, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, OnceLock};
use std::thread::{self, JoinHandle, Thread};
use std::time::{Duration, Instant};

const IDLE: u8 = 0;
const INITIALIZING: u8 = 1;
const READY: u8 = 2;
const SCAN_RECORDS: usize = 256;
const SCAN_TIME: Duration = Duration::from_millis(1);
const IDLE_RECHECK: Duration = Duration::from_millis(10);

fn invariant(message: &str) -> ! {
    eprintln!("production-concurrency completion invariant: {message}");
    std::process::abort()
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub(super) enum Role {
    Coordinator = 0,
    Reclaimer = 1,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Phase {
    Vacant,
    Starting,
    Running,
    Joining,
}

struct Signal(Option<Arc<SignalInner>>);
struct SignalInner {
    flag: AtomicBool,
}
impl Signal {
    fn new(flag: bool) -> Self {
        Self(Some(Arc::new(SignalInner {
            flag: AtomicBool::new(flag),
        })))
    }
    fn inner(&self) -> &SignalInner {
        self.0.as_ref().expect("live signal")
    }
    fn read(&self) -> bool {
        self.inner().flag.load(Ordering::Acquire)
    }
    fn store(&self, value: bool) {
        self.inner().flag.store(value, Ordering::Release);
    }
}
impl Clone for Signal {
    fn clone(&self) -> Self {
        Self(Some(self.0.as_ref().expect("live signal").clone()))
    }
}
impl Drop for Signal {
    fn drop(&mut self) {
        let _ = Arc::into_inner(self.0.take().expect("signal dropped twice"));
    }
}

struct Record {
    phase: Phase,
    incarnation: u64,
    handle: Option<JoinHandle<()>>,
    wake: Option<WorkerWake>,
    tail: Signal,
}

impl Record {
    fn new(tail: Signal) -> Self {
        Self {
            phase: Phase::Vacant,
            incarnation: 0,
            handle: None,
            wake: None,
            tail,
        }
    }
}

struct Domain {
    id: u64,
    identity: Option<DirectoryPin>,
    attachment: Option<Attachment>,
    records: [Record; 2],
    removing: Signal,
    stats: Option<super::reclamation::ReclamationStatsHandle>,
}

/// The immutable configuration remains represented until native completion and
/// disposal of the node/identity handle. The graph-bearing domain is detached
/// after the reclaimer joins, before testing its separately surviving wake tail.
struct Attachment {
    domain: Option<DomainHandle>,
    options: ConcurrencyOptions,
    active: Option<u64>,
    shutting_down: bool,
    next_session: u64,
}

impl Drop for Domain {
    fn drop(&mut self) {
        for record in &self.records {
            if record.phase != Phase::Vacant || record.handle.is_some() || record.wake.is_some() {
                invariant("destroying occupied worker inventory");
            }
        }
        // pop_front destroyed the containing list allocation before returning
        // this value; native completion retains the final wake tail separately.

        if self
            .attachment
            .as_ref()
            .is_some_and(|attachment| attachment.domain.is_some() || attachment.active.is_some())
        {
            invariant("destroying a directory with retained session authority");
        }
    }
}

struct Inventory {
    domains: LinkedList<Domain>,
    next_id: u64,
    role: Role,
    detached: Option<Detached>,
}

struct Detached {
    id: u64,
    identity: Option<DirectoryIdentity>,
    options: Option<ConcurrencyOptions>,
}

struct Core {
    inventory: Mutex<Inventory>,
    epoch: AtomicU64,
    worker: OnceLock<Thread>,
}

impl Core {
    fn new() -> Self {
        let core = Self {
            inventory: Mutex::new(Inventory {
                domains: LinkedList::new(),
                next_id: 0,
                role: Role::Coordinator,
                detached: None,
            }),
            epoch: AtomicU64::new(1),
            worker: OnceLock::new(),
        };
        drop(
            core.inventory
                .lock()
                .unwrap_or_else(|_| invariant("private inventory poisoned")),
        );
        core
    }
    fn notify(&self) {
        self.epoch
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |n| n.checked_add(1))
            .unwrap_or_else(|_| invariant("wake epoch exhausted"));
        if let Some(worker) = self.worker.get() {
            worker.unpark();
        }
    }
}

/// Armed between successful spawn and transfer, including process-service
/// publication. There is no path that can silently detach a successful worker.
struct PublicationGuard {
    handle: Option<JoinHandle<()>>,
}
impl PublicationGuard {
    fn new(handle: JoinHandle<()>) -> Self {
        Self {
            handle: Some(handle),
        }
    }
    fn transfer(mut self) -> JoinHandle<()> {
        self.handle.take().expect("published once")
    }
}
impl Drop for PublicationGuard {
    fn drop(&mut self) {
        if self.handle.is_some() {
            invariant("successful worker lost before handle publication");
        }
    }
}

struct Service {
    core: Arc<Core>,
    // Permanent process ownership. Static Service is never destroyed or joined.
    _handle: PublicationGuard,
}

struct ServiceCell {
    state: AtomicU8,
    service: OnceLock<Service>,
}

impl ServiceCell {
    const fn new() -> Self {
        Self {
            state: AtomicU8::new(IDLE),
            service: OnceLock::new(),
        }
    }

    fn get(&'static self) -> Result<&'static Service, StartError> {
        self.initialize(|core| {
            super::worker::builder("overgraph-completion").spawn(move || {
                core.worker
                    .set(thread::current())
                    .unwrap_or_else(|_| invariant("second completion thread"));
                // OnceLock publication alone is not readiness. No shared mutex
                // access before the owner has installed permanent handle custody.
                while self.state.load(Ordering::Acquire) != READY {
                    thread::park_timeout(IDLE_RECHECK);
                }
                if std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| service_loop(&core)))
                    .is_err()
                {
                    invariant("completion service panicked");
                }
            })
        })
    }

    fn initialize(
        &'static self,
        spawn: impl FnOnce(Arc<Core>) -> std::io::Result<JoinHandle<()>>,
    ) -> Result<&'static Service, StartError> {
        if self.state.load(Ordering::Acquire) == READY {
            return Ok(self
                .service
                .get()
                .unwrap_or_else(|| invariant("ready service missing")));
        }
        if self
            .state
            .compare_exchange(IDLE, INITIALIZING, Ordering::AcqRel, Ordering::Acquire)
            .is_err()
        {
            return Err(StartError::Busy);
        }
        struct Initialization<'a> {
            cell: &'a ServiceCell,
            successful: bool,
        }
        impl Drop for Initialization<'_> {
            fn drop(&mut self) {
                if self.successful {
                    invariant("successful service startup unwound before readiness");
                }
                self.cell.state.store(IDLE, Ordering::Release);
            }
        }
        let mut initialization = Initialization {
            cell: self,
            successful: false,
        };
        let core = Arc::new(Core::new());
        let handle = match spawn(core.clone()) {
            Ok(handle) => PublicationGuard::new(handle),
            Err(error) => {
                drop(core); // Failed-start closure/packet already destroyed by std.
                drop(initialization); // Only now may another opener retry.
                return Err(StartError::Spawn(error));
            }
        };
        initialization.successful = true;
        if self
            .service
            .set(Service {
                core,
                _handle: handle,
            })
            .is_err()
        {
            invariant("completion service published twice");
        }
        self.state.store(READY, Ordering::Release);
        initialization.successful = false;
        // Disarm without resetting Ready. No allocation or fallible operation
        // intervenes between the successful publication and this store.
        std::mem::forget(initialization);
        let service = self
            .service
            .get()
            .unwrap_or_else(|| invariant("installed service missing"));
        service.core.notify();
        Ok(service)
    }
}

static SERVICE: ServiceCell = ServiceCell::new();
// Absent-path preflight/creation callers use this gate, then resolve their path
// again while holding it. Existing-directory attach and graph drain never need it.
static CREATION: Mutex<()> = Mutex::new(());
static CREATION_READY: AtomicBool = AtomicBool::new(false);

pub(super) fn creation_guard() -> MutexGuard<'static, ()> {
    let guard = CREATION
        .lock()
        .unwrap_or_else(|_| invariant("directory creation gate poisoned"));
    CREATION_READY.store(true, Ordering::Release);
    guard
}

#[derive(Debug)]
pub(super) enum StartError {
    Busy,
    ActiveSession,
    ConfigurationConflict,
    Spawn(std::io::Error),
    Inventory(EngineError),
    WrongDomain,
}

/// One admitted write session. Explicit close releases graph-bearing references;
/// a surviving closed wrapper retains only IDs and a signal.
/// This handle is deliberately not Clone: passive views retain independent data
/// owners and cannot authorize another active session or prolong open authority.
pub(super) struct DirectorySession {
    registration: DomainRegistration,
    session: u64,
    domain: Option<DomainHandle>,
    producer: Option<SessionProducer>,
}

fn same_options(left: &ConcurrencyOptions, right: &ConcurrencyOptions) -> bool {
    left.same_configuration(right)
}

impl DirectorySession {
    pub(super) fn identity(&self) -> Option<DirectoryIdentity> {
        self.registration
            .service
            .core
            .inventory
            .lock()
            .unwrap_or_else(|_| invariant("inventory poisoned"))
            .domains
            .iter()
            .find(|domain| domain.id == self.registration.id)
            .and_then(|domain| domain.identity.as_ref().map(DirectoryPin::identity))
    }

    /// Look up an already opened, handle-pinned directory. None is the sole
    /// permission to construct a fresh domain; a represented-but-exiting domain
    /// returns Busy, preserving its identity until actual final disposal.
    pub(super) fn attach(
        identity: DirectoryPin,
        options: &ConcurrencyOptions,
    ) -> Result<Option<Self>, StartError> {
        let service = SERVICE.get()?;
        let key = identity.identity();
        let mut inventory = service
            .core
            .inventory
            .lock()
            .unwrap_or_else(|_| invariant("inventory poisoned"));
        if let Some(pending) = inventory
            .detached
            .as_ref()
            .filter(|pending| pending.identity == Some(key))
        {
            return Err(
                if pending
                    .options
                    .as_ref()
                    .is_some_and(|old| !same_options(old, options))
                {
                    StartError::ConfigurationConflict
                } else {
                    StartError::Busy
                },
            );
        }
        let Some(entry) = inventory.domains.iter_mut().find(|entry| {
            entry
                .identity
                .as_ref()
                .is_some_and(|pin| pin.identity() == key)
        }) else {
            return Ok(None);
        };
        let Some(attachment) = entry.attachment.as_mut() else {
            return Err(StartError::Busy); // First configured construction owns it.
        };
        if !same_options(&attachment.options, options) {
            return Err(StartError::ConfigurationConflict);
        }
        if attachment.active.is_some() {
            return Err(if attachment.shutting_down {
                StartError::Busy
            } else {
                StartError::ActiveSession
            });
        }
        if entry.records[Role::Coordinator as usize].phase != Phase::Vacant
            || entry.records[Role::Reclaimer as usize].phase != Phase::Running
        {
            return Err(StartError::Busy);
        }
        let domain = attachment.domain.as_ref().ok_or(StartError::Busy)?;
        // Inventory -> registry is the only attach/close ordering. The reclaimer
        // commits exit under the same registry mutex without taking inventory.
        let producer = domain.try_attach_session().map_err(|_| StartError::Busy)?;
        domain.stats().set_sessions(1, 0);
        attachment.next_session = attachment
            .next_session
            .checked_add(1)
            .unwrap_or_else(|| invariant("session identifier exhausted"));
        let session = attachment.next_session;
        attachment.active = Some(session);
        entry.removing.store(false);
        let result = Self {
            registration: DomainRegistration {
                id: entry.id,
                service,
                removing: entry.removing.clone(),
            },
            session,
            domain: Some(domain.clone()),
            producer: Some(producer),
        };
        drop(inventory);
        drop(identity); // Existing owned handle already pins the same identity.
        service.core.notify();
        Ok(Some(result))
    }

    pub(super) fn matches_domain(&self, domain: &DomainHandle) -> bool {
        self.producer.is_some()
            && self
                .domain
                .as_ref()
                .is_some_and(|own| own.same_domain(domain))
    }
    pub(super) fn domain(&self) -> &DomainHandle {
        self.domain.as_ref().expect("closed directory session")
    }

    pub(super) fn registration(&self) -> &DomainRegistration {
        &self.registration
    }

    pub(super) fn registration_handle(&self) -> DomainRegistration {
        DomainRegistration {
            id: self.registration.id,
            service: self.registration.service,
            removing: self.registration.removing.clone(),
        }
    }

    pub(super) fn id(&self) -> u64 {
        self.session
    }

    /// Mark the active session as shutting down while its admitted work settles.
    /// Producer custody remains intact until close detaches the session. Passive
    /// data from prior sessions remains held by independent data owners.
    pub(super) fn begin_shutdown(&mut self) {
        if self.producer.is_none() {
            return;
        }
        let mut inventory = self
            .registration
            .service
            .core
            .inventory
            .lock()
            .unwrap_or_else(|_| invariant("inventory poisoned"));
        let entry = inventory
            .domains
            .iter_mut()
            .find(|entry| entry.id == self.registration.id)
            .unwrap_or_else(|| invariant("shutting directory session missing"));
        let attachment = entry.attachment.as_mut().expect("bound directory");
        if attachment.active != Some(self.session) {
            invariant("stale directory session shutdown");
        }
        attachment.shutting_down = true;
        entry.removing.store(true);
        let domain = self.domain.as_ref().expect("live directory session");
        domain.stats().set_sessions(0, 1);
        // Service admission stops at the lifecycle boundary. Existing queued
        // operations still own construction authority until terminal detach.
        domain.stats().shutdown_requested();
        drop(inventory);
        self.registration.service.core.notify();
    }

    pub(super) fn close(&mut self) {
        let Some(producer) = self.producer.take() else {
            return;
        };
        let domain = self.domain.take();
        let mut inventory = self
            .registration
            .service
            .core
            .inventory
            .lock()
            .unwrap_or_else(|_| invariant("inventory poisoned"));
        let entry = inventory
            .domains
            .iter_mut()
            .find(|entry| entry.id == self.registration.id)
            .unwrap_or_else(|| invariant("active directory session missing"));
        let attachment = entry.attachment.as_mut().expect("bound directory");
        if attachment.active != Some(self.session) {
            invariant("stale directory session close");
        }
        // Closing the producer is fixed metadata work under inventory. A new
        // attach cannot reset closing until this old producer has relinquished
        // its custody; all graph-bearing Arc destruction is outside the lock.
        drop(producer);
        domain
            .as_ref()
            .expect("closing directory domain")
            .stats()
            .set_sessions(0, 0);
        attachment.active = None;
        attachment.shutting_down = false;
        entry.removing.store(true);
        drop(inventory);
        drop(domain);
        self.registration.service.core.notify();
    }
}

impl Drop for DirectorySession {
    fn drop(&mut self) {
        self.close();
    }
}

/// One stable process-domain record, shared by canonical directory attachment.
/// Dropping it requests bounded removal; occupied worker records keep themselves
/// alive independently until native completion and all fixed wake tails settle.
pub(super) struct DomainRegistration {
    id: u64,
    service: &'static Service,
    removing: Signal,
}

impl DomainRegistration {
    pub(super) fn bind_created_identity(&self, pin: DirectoryPin) -> Result<(), StartError> {
        let mut inventory = self
            .service
            .core
            .inventory
            .lock()
            .unwrap_or_else(|_| invariant("inventory poisoned"));
        let identity = pin.identity();
        if inventory.has_identity(identity) {
            return Err(StartError::Busy);
        }
        let entry = inventory
            .domains
            .iter_mut()
            .find(|entry| entry.id == self.id)
            .ok_or(StartError::Busy)?;
        if entry.identity.is_some() {
            return Err(StartError::Busy);
        }
        entry.identity = Some(pin);
        Ok(())
    }
    pub(super) fn bind_stats(&self, stats: super::reclamation::ReclamationStatsHandle) {
        stats.bind_native_completion();
        let mut inventory = self
            .service
            .core
            .inventory
            .lock()
            .unwrap_or_else(|_| invariant("inventory poisoned"));
        let domain = inventory
            .domains
            .iter_mut()
            .find(|domain| domain.id == self.id)
            .unwrap_or_else(|| invariant("stats bound after completion removal"));
        if domain.stats.replace(stats).is_some() {
            invariant("completion stats bound twice");
        }
    }
    pub(super) fn new(identity: Option<DirectoryPin>) -> Result<Self, StartError> {
        Self::new_inner(identity, None)
    }
    pub(super) fn new_configured(
        identity: Option<DirectoryPin>,
        options: &ConcurrencyOptions,
    ) -> Result<Self, StartError> {
        Self::new_inner(identity, Some(options))
    }
    pub(super) fn bind_domain(&self, domain: &DomainHandle) {
        if domain
            .inner_registration_id()
            .compare_exchange(0, self.id, Ordering::AcqRel, Ordering::Acquire)
            .is_err()
        {
            invariant("domain bound twice");
        }
    }
    pub(super) fn same_registration(&self, other: &Self) -> bool {
        self.id == other.id && std::ptr::eq(self.service, other.service)
    }
    fn new_inner(
        identity: Option<DirectoryPin>,
        options: Option<&ConcurrencyOptions>,
    ) -> Result<Self, StartError> {
        let service = SERVICE.get()?; // No governed worker before the singleton.
        let mut inventory = service
            .core
            .inventory
            .lock()
            .unwrap_or_else(|_| invariant("inventory poisoned"));
        if let Some(key) = identity.as_ref().map(DirectoryPin::identity) {
            if inventory.has_identity(key) {
                if let Some(options) = options {
                    let represented = inventory.domains.iter().find(|domain| {
                        domain
                            .identity
                            .as_ref()
                            .is_some_and(|pin| pin.identity() == key)
                    });
                    let retained_options = represented
                        .and_then(|domain| domain.attachment.as_ref())
                        .map(|attachment| &attachment.options)
                        .or_else(|| {
                            inventory
                                .detached
                                .as_ref()
                                .filter(|pending| pending.identity == Some(key))
                                .and_then(|pending| pending.options.as_ref())
                        });
                    if retained_options.is_some_and(|old| !same_options(old, options)) {
                        return Err(StartError::ConfigurationConflict);
                    }
                    if let Some(attachment) = represented
                        .and_then(|domain| domain.attachment.as_ref())
                        .filter(|attachment| attachment.active.is_some())
                    {
                        return Err(if attachment.shutting_down {
                            StartError::Busy
                        } else {
                            StartError::ActiveSession
                        });
                    }
                }
                return Err(StartError::Busy);
            }
        }
        let id = inventory
            .next_id
            .checked_add(1)
            .unwrap_or_else(|| invariant("domain identifier exhausted"));
        inventory.next_id = id;
        let removing = Signal::new(false);
        let attachment = identity.as_ref().and(options).map(|options| Attachment {
            domain: None,
            options: options.clone(),
            active: None,
            shutting_down: false,
            next_session: 0,
        });
        let domain = Domain {
            id,
            identity,
            attachment,
            records: [
                Record::new(Signal::new(true)),
                Record::new(Signal::new(true)),
            ],
            removing: removing.clone(),
            stats: None,
        };
        inventory.domains.push_back(domain);
        drop(inventory);
        service.core.notify();
        Ok(Self {
            id,
            service,
            removing,
        })
    }

    /// Bind the already started reclaimer to its first directory session. The
    /// registration's identity remains pinned even if startup after this point
    /// fails; closing the returned session leaves cleanup to the live reclaimer.
    pub(super) fn bind_directory(
        &self,
        domain: DomainHandle,
        options: &ConcurrencyOptions,
    ) -> Result<DirectorySession, StartError> {
        if domain.inner_registration_id().load(Ordering::Acquire) != self.id {
            return Err(StartError::WrongDomain);
        }
        let mut inventory = self
            .service
            .core
            .inventory
            .lock()
            .unwrap_or_else(|_| invariant("inventory poisoned"));
        let entry = inventory
            .domains
            .iter_mut()
            .find(|entry| entry.id == self.id)
            .ok_or(StartError::Busy)?;
        if let Some(attachment) = &entry.attachment {
            if !same_options(&attachment.options, options) {
                return Err(StartError::ConfigurationConflict);
            }
            if attachment.domain.is_some()
                || attachment.active.is_some()
                || attachment.next_session != 0
            {
                return Err(StartError::Busy);
            }
        }
        if entry.identity.is_none()
            || entry.removing.read()
            || entry.records[Role::Coordinator as usize].phase != Phase::Vacant
            || entry.records[Role::Reclaimer as usize].phase != Phase::Running
        {
            return Err(StartError::Busy);
        }
        let producer = domain.try_attach_session().map_err(StartError::Inventory)?;
        domain.stats().set_sessions(1, 0);
        entry.attachment = Some(Attachment {
            domain: Some(domain.clone()),
            options: options.clone(),
            active: Some(1),
            shutting_down: false,
            next_session: 1,
        });
        let session = DirectorySession {
            registration: DomainRegistration {
                id: self.id,
                service: self.service,
                removing: self.removing.clone(),
            },
            session: 1,
            domain: Some(domain),
            producer: Some(producer),
        };
        drop(inventory);
        self.service.core.notify();
        Ok(session)
    }

    /// Reserve this worker record before spawning; retain native join/wake custody.
    pub(super) fn spawn(
        &self,
        role: Role,
        name: &str,
        body: impl FnOnce(WorkerWake) + Send + 'static,
    ) -> Result<(), StartError> {
        self.spawn_with(role, |wake| {
            super::worker::builder(name).spawn(move || {
                wake.register_current();
                if std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| body(wake))).is_err() {
                    invariant("candidate worker panicked");
                }
                SERVICE
                    .service
                    .get()
                    .unwrap_or_else(|| invariant("worker without service"))
                    .core
                    .notify();
            })
        })
    }

    fn spawn_with(
        &self,
        role: Role,
        spawn: impl FnOnce(WorkerWake) -> std::io::Result<JoinHandle<()>>,
    ) -> Result<(), StartError> {
        let mut inventory = self
            .service
            .core
            .inventory
            .lock()
            .unwrap_or_else(|_| invariant("inventory poisoned"));
        let domain = inventory
            .domains
            .iter_mut()
            .find(|domain| domain.id == self.id)
            .unwrap_or_else(|| invariant("domain missing at spawn"));
        let record = &mut domain.records[role as usize];
        let active_directory = domain
            .attachment
            .as_ref()
            .is_some_and(|attachment| attachment.active.is_some() && !attachment.shutting_down);
        if (domain.removing.read() && !active_directory) || record.phase != Phase::Vacant {
            drop(inventory);
            return Err(StartError::Busy);
        }
        record.incarnation = record
            .incarnation
            .checked_add(1)
            .unwrap_or_else(|| invariant("worker incarnation exhausted"));
        let incarnation = record.incarnation;
        record.tail.store(false);
        let wake = WorkerWake(Some(Arc::new(WakeInner {
            worker: OnceLock::new(),
            epoch: AtomicU64::new(1),
            tail: record.tail.clone(),
            service: self.service,
        })));
        record.wake = Some(wake.clone());
        record.phase = Phase::Starting;
        drop(inventory);
        let handle = match spawn(wake) {
            Ok(handle) => PublicationGuard::new(handle),
            Err(error) => {
                // std has destroyed the failed closure/packet before returning.
                let wake = {
                    let mut inventory = self
                        .service
                        .core
                        .inventory
                        .lock()
                        .unwrap_or_else(|_| invariant("inventory poisoned"));
                    let record =
                        checked_record(&mut inventory, self.id, role, incarnation, Phase::Starting);
                    record.wake.take()
                };
                drop(wake);
                let mut inventory = self
                    .service
                    .core
                    .inventory
                    .lock()
                    .unwrap_or_else(|_| invariant("inventory poisoned"));
                let record =
                    checked_record(&mut inventory, self.id, role, incarnation, Phase::Starting);
                if !record.tail.read() {
                    invariant("failed spawn retained a wake");
                }
                record.phase = Phase::Vacant;
                drop(inventory);
                self.service.core.notify();
                return Err(StartError::Spawn(error));
            }
        };
        let mut inventory = self
            .service
            .core
            .inventory
            .lock()
            .unwrap_or_else(|_| invariant("inventory poisoned"));
        let record = checked_record(&mut inventory, self.id, role, incarnation, Phase::Starting);
        record.handle = Some(handle.transfer());
        record.phase = Phase::Running;
        drop(inventory);
        self.service.core.notify();
        Ok(())
    }

    pub(super) fn vacant(&self, role: Role) -> bool {
        let inventory = self
            .service
            .core
            .inventory
            .lock()
            .unwrap_or_else(|_| invariant("inventory poisoned"));
        inventory
            .domains
            .iter()
            .find(|domain| domain.id == self.id)
            .is_some_and(|domain| domain.records[role as usize].phase == Phase::Vacant)
    }
}

impl Drop for DomainRegistration {
    fn drop(&mut self) {
        self.removing.store(true);
        self.service.core.notify();
    }
}

fn checked_record(
    inventory: &mut Inventory,
    id: u64,
    role: Role,
    incarnation: u64,
    phase: Phase,
) -> &mut Record {
    let record = &mut inventory
        .domains
        .iter_mut()
        .find(|domain| domain.id == id)
        .unwrap_or_else(|| invariant("worker domain missing"))
        .records[role as usize];
    if record.incarnation != incarnation || record.phase != phase {
        invariant("stale worker transition");
    }
    record
}

pub(super) struct WorkerWake(Option<Arc<WakeInner>>);
struct WakeInner {
    worker: OnceLock<Thread>,
    epoch: AtomicU64,
    tail: Signal,
    service: &'static Service,
}

impl WorkerWake {
    fn inner(&self) -> &WakeInner {
        self.0.as_ref().expect("live worker wake")
    }
    fn register_current(&self) {
        self.inner()
            .worker
            .set(thread::current())
            .unwrap_or_else(|_| invariant("worker wake registered twice"));
    }
    pub(super) fn notify(&self) {
        let inner = self.inner();
        inner
            .epoch
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |n| n.checked_add(1))
            .unwrap_or_else(|_| invariant("worker wake epoch exhausted"));
        if let Some(worker) = inner.worker.get() {
            worker.unpark();
        }
    }
    pub(super) fn epoch(&self) -> u64 {
        self.inner().epoch.load(Ordering::Acquire)
    }
}
impl Clone for WorkerWake {
    fn clone(&self) -> Self {
        Self(Some(self.0.as_ref().expect("live wake").clone()))
    }
}
impl Drop for WorkerWake {
    fn drop(&mut self) {
        if let Some(inner) = Arc::into_inner(self.0.take().expect("wake dropped twice")) {
            let WakeInner {
                worker,
                epoch: _,
                tail,
                service,
            } = inner;
            drop(worker); // After native join, including retained Thread/name/Parker.
            tail.store(true);
            service.core.notify();
        }
    }
}

struct Joining {
    id: u64,
    role: Role,
    incarnation: u64,
    handle: JoinHandle<()>,
    wake: Option<WorkerWake>,
}

enum Work {
    Join(Joining),
    Remove(LinkedList<Domain>),
}

impl Inventory {
    fn has_identity(&self, identity: DirectoryIdentity) -> bool {
        self.detached
            .as_ref()
            .is_some_and(|pending| pending.identity == Some(identity))
            || self.domains.iter().any(|domain| {
                domain
                    .identity
                    .as_ref()
                    .is_some_and(|pin| pin.identity() == identity)
            })
    }

    fn advance(&mut self) {
        if self.role == Role::Coordinator {
            self.role = Role::Reclaimer;
        } else {
            self.role = Role::Coordinator;
            let mut head = self.domains.split_off(1);
            std::mem::swap(&mut self.domains, &mut head);
            self.domains.append(&mut head);
        }
    }

    fn detach_head(&mut self) -> LinkedList<Domain> {
        if self.detached.is_some() {
            invariant("second detached identity");
        }
        let domain = self.domains.front().expect("removing head");
        self.detached = Some(Detached {
            id: domain.id,
            identity: domain.identity.as_ref().map(DirectoryPin::identity),
            options: domain
                .attachment
                .as_ref()
                .map(|attachment| attachment.options.clone()),
        });
        let remainder = self.domains.split_off(1);
        self.role = Role::Coordinator;
        std::mem::replace(&mut self.domains, remainder)
    }
}

fn scan(core: &Core) -> Option<Work> {
    let started = Instant::now();
    let mut inventory = core
        .inventory
        .lock()
        .unwrap_or_else(|_| invariant("inventory poisoned"));
    // Snapshot the initial length: no repeated cycles through a small list, and
    // appenders cannot extend this pass. Only the service rotates/removes nodes.
    let checks = inventory.domains.len().saturating_mul(2).min(SCAN_RECORDS);
    for _ in 0..checks {
        let role = inventory.role;
        let domain = inventory.domains.front_mut().expect("nonempty scan");
        let id = domain.id;
        let record = &mut domain.records[role as usize];
        if record.phase == Phase::Joining && record.tail.read() {
            record.phase = Phase::Vacant;
        }
        if record.phase == Phase::Running
            && record
                .handle
                .as_ref()
                .expect("running handle")
                .is_finished()
        {
            record.phase = Phase::Joining;
            return Some(Work::Join(Joining {
                id,
                role,
                incarnation: record.incarnation,
                handle: record.handle.take().expect("eligible handle"),
                wake: record.wake.take(),
            }));
        }
        if domain.removing.read()
            && domain
                .attachment
                .as_ref()
                .is_none_or(|attachment| attachment.active.is_none() && attachment.domain.is_none())
            && domain
                .records
                .iter()
                .all(|record| record.phase == Phase::Vacant)
        {
            return Some(Work::Remove(inventory.detach_head()));
        }
        inventory.advance();
        if started.elapsed() >= SCAN_TIME {
            break;
        }
    }
    None
}

fn finish_join(core: &Core, joining: Joining) {
    let Joining {
        id,
        role,
        incarnation,
        handle,
        wake,
    } = joining;
    if handle.join().is_err() {
        invariant("native worker join failed");
    }
    // A directory node's bound ReclaimDomain retains its governed WorkerWake.
    // Take it only after native join established graph drain, then dispose it
    // outside inventory before waiting for the final wake tail. Keeping it until
    // Vacancy would create a strong cycle through this completion record.
    let detached_domain = if role == Role::Reclaimer {
        let mut inventory = core
            .inventory
            .lock()
            .unwrap_or_else(|_| invariant("inventory poisoned"));
        let domain = inventory
            .domains
            .front_mut()
            .unwrap_or_else(|| invariant("joining head missing"));
        if domain.id != id
            || domain.records[role as usize].incarnation != incarnation
            || domain.records[role as usize].phase != Phase::Joining
        {
            invariant("stale directory detach");
        }
        domain.attachment.as_mut().and_then(|attachment| {
            if attachment.active.is_some() {
                invariant("reclaimer completed with an active directory producer");
            }
            domain.removing.store(true);
            attachment.domain.take()
        })
    } else {
        None
    };
    drop(detached_domain);
    drop(wake);
    let mut inventory = core
        .inventory
        .lock()
        .unwrap_or_else(|_| invariant("inventory poisoned"));
    // The selected head stayed stationary throughout native join. Registrations
    // can only append; their Drop is an atomic removal request, never a removal.
    let domain = inventory
        .domains
        .front_mut()
        .unwrap_or_else(|| invariant("joining head missing"));
    if domain.id != id {
        invariant("joining head moved");
    }
    let record = &mut domain.records[role as usize];
    if record.incarnation != incarnation || record.phase != Phase::Joining {
        invariant("stale head completion");
    }
    if record.tail.read() {
        record.phase = Phase::Vacant;
    }
    inventory.advance();
}

fn remove_drained(core: &Core, mut singleton: LinkedList<Domain>) {
    if singleton.len() != 1 {
        invariant("removal is not a singleton");
    }
    // Pinned pop_front consumes the Box allocation before returning Domain.
    // This and every following destructor run outside all inventory locks.
    let domain = singleton.pop_front().expect("detached domain");
    finish_domain_disposal(core, domain);
}

fn finish_domain_disposal(core: &Core, mut domain: Domain) {
    let marker = (
        domain.id,
        domain.identity.as_ref().map(DirectoryPin::identity),
    );
    let stats = domain.stats.take();
    drop(domain); // Files and signal handles first.
    if let Some(stats) = stats {
        stats.native_completed();
    }
    let mut inventory = core
        .inventory
        .lock()
        .unwrap_or_else(|_| invariant("inventory poisoned"));
    if !inventory
        .detached
        .as_ref()
        .is_some_and(|pending| (pending.id, pending.identity) == marker)
    {
        invariant("detached identity changed");
    }
    inventory.detached = None;
    drop(inventory);
    core.notify();
}

fn service_loop(core: &Core) -> ! {
    loop {
        let epoch = core.epoch.load(Ordering::Acquire);
        match scan(core) {
            Some(Work::Join(joining)) => {
                finish_join(core, joining);
                continue;
            }
            Some(Work::Remove(singleton)) => {
                remove_drained(core, singleton);
                continue;
            }
            None => {}
        }
        if core.epoch.load(Ordering::Acquire) != epoch {
            continue;
        }
        // unpark retains its token across the check/park gap; periodic recheck
        // observes TLS completion without imposing delay on useful ready work.
        thread::park_timeout(IDLE_RECHECK);
    }
}

#[cfg(test)]
mod practical_completion_tests {
    use super::*;
    use crate::engine::reclamation::ReclaimDomain;
    #[test]
    fn p45a3_completion_failed_spawn_restores_record_and_native_tail_blocks_reuse() {
        let registration = DomainRegistration::new(None).unwrap();
        assert!(matches!(
            registration.spawn_with(Role::Coordinator, |_| Err(std::io::Error::other("fixture"))),
            Err(StartError::Spawn(_))
        ));
        assert!(registration.vacant(Role::Coordinator));
        let (send, recv) = std::sync::mpsc::channel();
        registration
            .spawn(
                Role::Coordinator,
                "overgraph-completion-fixture",
                move |wake| {
                    send.send(wake.clone()).unwrap();
                },
            )
            .unwrap();
        let retained = recv.recv_timeout(Duration::from_secs(5)).unwrap();
        let deadline = Instant::now() + Duration::from_secs(5);
        loop {
            let inventory = registration.service.core.inventory.lock().unwrap();
            let phase = inventory
                .domains
                .iter()
                .find(|d| d.id == registration.id)
                .unwrap()
                .records[0]
                .phase;
            if phase == Phase::Joining {
                break;
            }
            drop(inventory);
            assert!(Instant::now() < deadline);
            thread::yield_now();
        }
        assert!(!registration.vacant(Role::Coordinator));
        drop(retained);
        while !registration.vacant(Role::Coordinator) {
            assert!(Instant::now() < deadline);
            thread::yield_now();
        }
        drop(registration);
    }
    #[test]
    fn p45a3_completion_foreign_domain_cannot_bind_directory() {
        let one = tempfile::tempdir().unwrap();
        let two = tempfile::tempdir().unwrap();
        let options = ConcurrencyOptions::default();
        let a = ReclaimDomain::configured(&options, Some(DirectoryPin::open(one.path()).unwrap()))
            .unwrap();
        let b = ReclaimDomain::configured(&options, Some(DirectoryPin::open(two.path()).unwrap()))
            .unwrap();
        assert!(matches!(
            a.registration.bind_directory(b.domain.clone(), &options),
            Err(StartError::WrongDomain)
        ));
        let mut session = a
            .registration
            .bind_directory(a.domain.clone(), &options)
            .unwrap();
        assert!(matches!(
            DirectorySession::attach(DirectoryPin::open(one.path()).unwrap(), &options),
            Err(StartError::ActiveSession)
        ));
        session.begin_shutdown();
        assert!(matches!(
            DirectorySession::attach(DirectoryPin::open(one.path()).unwrap(), &options),
            Err(StartError::Busy)
        ));
        session.close();
        drop(session);
        a.domain.close();
        b.domain.close();
        drop(a);
        drop(b);
    }
}

#[cfg(test)]
mod task_custody_regression {
    use super::*;
    #[test]
    fn p45a3_canceled_delivery_keeps_local_task_payload_until_actual_native_return() {
        struct Payload(std::sync::mpsc::Sender<String>);
        impl Drop for Payload {
            fn drop(&mut self) {
                self.0
                    .send(thread::current().name().unwrap_or("unnamed").into())
                    .unwrap();
            }
        }
        let registration = DomainRegistration::new(None).unwrap();
        let (notice, dropped) = std::sync::mpsc::channel();
        let (entered, entry) = std::sync::mpsc::channel();
        let (resume, resumed) = std::sync::mpsc::channel();
        let (delivery, canceled) = std::sync::mpsc::channel::<()>();
        registration
            .spawn(
                Role::Coordinator,
                "overgraph-captured-local",
                move |_wake| {
                    let payload = Payload(notice);
                    entered.send(()).unwrap();
                    resumed.recv().unwrap();
                    let _ = delivery.send(());
                    drop(payload);
                },
            )
            .unwrap();
        entry.recv_timeout(Duration::from_secs(5)).unwrap();
        drop(canceled);
        drop(registration);
        assert!(dropped.try_recv().is_err());
        resume.send(()).unwrap();
        assert_eq!(
            dropped.recv_timeout(Duration::from_secs(5)).unwrap(),
            "overgraph-captured-local"
        );
    }
}

#[cfg(test)]
mod review_completion_safety {
    use super::*;
    use crate::engine::reclamation::{ConfiguredDomain, ReclaimDomain, ResourceClass};
    use std::cell::RefCell;
    use std::sync::mpsc;

    static REVIEW_SERIAL: Mutex<()> = Mutex::new(());

    fn wait_until(mut predicate: impl FnMut() -> bool) {
        let deadline = Instant::now() + Duration::from_secs(10);
        while !predicate() {
            assert!(Instant::now() < deadline, "completion condition timed out");
            thread::yield_now();
        }
    }
    fn phase(registration: &DomainRegistration, role: Role) -> Phase {
        registration
            .service
            .core
            .inventory
            .lock()
            .unwrap()
            .domains
            .iter()
            .find(|d| d.id == registration.id)
            .unwrap()
            .records[role as usize]
            .phase
    }
    fn identity_removed(identity: DirectoryIdentity) {
        wait_until(|| {
            !SERVICE
                .get()
                .unwrap()
                .core
                .inventory
                .lock()
                .unwrap()
                .has_identity(identity)
        });
    }
    fn directory(
        path: &std::path::Path,
        options: &ConcurrencyOptions,
    ) -> (ConfiguredDomain, DirectorySession) {
        let configured =
            ReclaimDomain::configured(options, Some(DirectoryPin::open(path).unwrap())).unwrap();
        let session = configured
            .registration
            .bind_directory(configured.domain.clone(), options)
            .unwrap();
        (configured, session)
    }
    struct HeldTls {
        entered: mpsc::Sender<()>,
        release: mpsc::Receiver<()>,
    }
    impl Drop for HeldTls {
        fn drop(&mut self) {
            let _ = self.entered.send(());
            let _ = self.release.recv();
        }
    }
    thread_local! { static HELD_TLS: RefCell<Option<HeldTls>> = const { RefCell::new(None) }; }
    // Unblock native cleanup even if a fixture assertion unwinds.
    struct ReleaseOnDrop(mpsc::Sender<()>);
    impl Drop for ReleaseOnDrop {
        fn drop(&mut self) {
            let _ = self.0.send(());
        }
    }

    #[test]
    fn p45a3_review_s1_native_join_retains_record_through_tls_and_releases_inventory() {
        let _serial = REVIEW_SERIAL.lock().unwrap_or_else(|e| e.into_inner());
        let registration = DomainRegistration::new(None).unwrap();
        let (entered, entry) = mpsc::channel();
        let (release, released) = mpsc::channel();
        let go = ReleaseOnDrop(release);
        registration
            .spawn(Role::Coordinator, "overgraph-review-held-tls", move |_| {
                HELD_TLS.with(|tls| {
                    *tls.borrow_mut() = Some(HeldTls {
                        entered,
                        release: released,
                    })
                });
            })
            .unwrap();
        entry.recv_timeout(Duration::from_secs(10)).unwrap();
        wait_until(|| phase(&registration, Role::Coordinator) == Phase::Joining);
        assert!(!registration.vacant(Role::Coordinator));
        assert!(matches!(
            registration.spawn(Role::Coordinator, "occupied-review-role", |_| {}),
            Err(StartError::Busy)
        ));
        let inventory = registration
            .service
            .core
            .inventory
            .try_lock()
            .expect("native join held inventory lock");
        let record = &inventory
            .domains
            .iter()
            .find(|d| d.id == registration.id)
            .unwrap()
            .records[Role::Coordinator as usize];
        assert!(
            record.handle.is_none(),
            "native join owns the exact handle outside inventory"
        );
        drop(inventory);
        // Registration and spawning remain possible while the sole joiner is blocked.
        let appended = DomainRegistration::new(None).unwrap();
        let (ran, running) = mpsc::channel();
        appended
            .spawn(Role::Coordinator, "overgraph-review-appended", move |_| {
                ran.send(()).unwrap();
            })
            .unwrap();
        running.recv_timeout(Duration::from_secs(10)).unwrap();
        assert_eq!(phase(&registration, Role::Coordinator), Phase::Joining);
        drop(go);
        wait_until(|| registration.vacant(Role::Coordinator) && appended.vacant(Role::Coordinator));
    }

    #[test]
    fn p45a3_review_s1_early_return_cannot_skip_handle_publication() {
        let _serial = REVIEW_SERIAL.lock().unwrap_or_else(|e| e.into_inner());
        let registration = DomainRegistration::new(None).unwrap();
        let mut before_publication = None;
        registration
            .spawn_with(Role::Coordinator, |wake| {
                let handle = PublicationGuard::new(
                    super::super::worker::builder("overgraph-review-early")
                        .spawn(move || wake.register_current())?,
                );
                wait_until(|| handle.handle.as_ref().unwrap().is_finished());
                before_publication = Some((
                    phase(&registration, Role::Coordinator),
                    registration.vacant(Role::Coordinator),
                ));
                Ok(handle.transfer())
            })
            .unwrap();
        assert_eq!(before_publication, Some((Phase::Starting, false)));
        wait_until(|| registration.vacant(Role::Coordinator));
    }

    #[test]
    fn p45a3_review_s1_aliases_compare_all_five_options_and_one_active_session() {
        let _serial = REVIEW_SERIAL.lock().unwrap_or_else(|e| e.into_inner());
        let dir = tempfile::tempdir().unwrap();
        let options = ConcurrencyOptions::default();
        let identity = DirectoryPin::open(dir.path()).unwrap().identity();
        let (configured, mut session) = directory(dir.path(), &options);
        let alias = dir.path().join(".");
        assert!(matches!(
            DirectorySession::attach(DirectoryPin::open(&alias).unwrap(), &options),
            Err(StartError::ActiveSession)
        ));
        #[cfg(unix)]
        {
            let other = tempfile::tempdir().unwrap();
            let alias = other.path().join("alias");
            std::os::unix::fs::symlink(dir.path(), &alias).unwrap();
            assert!(matches!(
                DirectorySession::attach(DirectoryPin::open(&alias).unwrap(), &options),
                Err(StartError::ActiveSession)
            ));
        }
        for changed in 0..5 {
            let mut conflict = options.clone();
            match changed {
                0 => conflict.max_batch_operations += 1,
                1 => conflict.max_batch_bytes += 1,
                2 => conflict.generation_operation_limit += 1,
                3 => conflict.generation_wal_bytes += 1,
                4 => conflict.admission_timeout_ms += 1,
                _ => unreachable!(),
            }
            conflict.validate().unwrap();
            assert!(
                matches!(
                    DirectorySession::attach(DirectoryPin::open(&alias).unwrap(), &conflict),
                    Err(StartError::ConfigurationConflict)
                ),
                "option {changed}"
            );
            assert!(
                matches!(
                    DomainRegistration::new_configured(
                        Some(DirectoryPin::open(&alias).unwrap()),
                        &conflict
                    ),
                    Err(StartError::ConfigurationConflict)
                ),
                "registration option {changed}"
            );
            assert_eq!(session.registration().id, configured.registration.id);
            assert!(session.matches_domain(&configured.domain));
        }
        session.close();
        drop(session);
        drop(configured);
        identity_removed(identity);
    }

    #[derive(Default)]
    struct SessionPayload(Vec<u8>);
    #[test]
    fn p45a3_review_s1_reopen_keeps_old_payloads_and_domain_registration_identity() {
        let _serial = REVIEW_SERIAL.lock().unwrap_or_else(|e| e.into_inner());
        let dir = tempfile::tempdir().unwrap();
        let options = ConcurrencyOptions::default();
        let identity = DirectoryPin::open(dir.path()).unwrap().identity();
        let (configured, mut session) = directory(dir.path(), &options);
        let stats = configured.domain.stats();
        let domain = configured.domain.clone();
        let domain_identity = domain.identity();
        let registration_id = configured.registration.id;
        drop(configured); // Original registration Drop cannot close the bound session.
        assert!(matches!(
            DirectorySession::attach(DirectoryPin::open(dir.path()).unwrap(), &options),
            Err(StartError::ActiveSession)
        ));
        let mut pins = Vec::new();
        for index in 0..12 {
            let mut value = session
                .domain()
                .build::<SessionPayload>(ResourceClass::Generation)
                .unwrap();
            value.mutate(|value| value.0.resize(8192, index)).unwrap();
            let owner = value.into_owner().unwrap();
            pins.push(owner.read());
            drop(owner);
            let previous_session = session.id();
            session.close();
            session = DirectorySession::attach(
                DirectoryPin::open(&dir.path().join(".")).unwrap(),
                &options,
            )
            .unwrap()
            .unwrap();
            assert!(session.id() > previous_session);
            assert_eq!(session.registration().id, registration_id);
            assert!(session.domain().same_domain(&domain));
            for (expected, old) in pins.iter().enumerate() {
                assert_eq!(old.get().0.len(), 8192);
                assert!(old.get().0.iter().all(|&value| value == expected as u8));
            }
        }
        session.close();
        let different = options.with_admission_timeout_ms(1001);
        assert!(matches!(
            DirectorySession::attach(DirectoryPin::open(dir.path()).unwrap(), &different),
            Err(StartError::ConfigurationConflict)
        ));
        drop(pins);
        drop(domain);
        identity_removed(identity);
        assert!(stats.read().exited);
        assert!(
            DirectorySession::attach(DirectoryPin::open(dir.path()).unwrap(), &different)
                .unwrap()
                .is_none()
        );
        let (fresh, mut new_session) = directory(dir.path(), &different);
        assert!(!fresh.domain.identity().same_identity(&domain_identity));
        assert_ne!(fresh.registration.id, registration_id);
        // Surviving closed session/status handles cannot remove the new incarnation.
        drop(session);
        drop(stats);
        assert!(matches!(
            DirectorySession::attach(DirectoryPin::open(dir.path()).unwrap(), &different),
            Err(StartError::ActiveSession)
        ));
        new_session.close();
        drop(new_session);
        drop(fresh);
        identity_removed(identity);
    }

    #[test]
    fn p45a3_review_s1_native_tls_completion_blocks_directory_reopen_with_root_free_stats() {
        let _serial = REVIEW_SERIAL.lock().unwrap_or_else(|e| e.into_inner());
        let dir = tempfile::tempdir().unwrap();
        let options = ConcurrencyOptions::default();
        let identity = DirectoryPin::open(dir.path()).unwrap().identity();
        let (configured, mut session) = directory(dir.path(), &options);
        let stats = configured.domain.stats();
        let (entered, entry) = mpsc::channel();
        let (release, released) = mpsc::channel();
        let go = ReleaseOnDrop(release);
        configured
            .registration
            .spawn(
                Role::Coordinator,
                "overgraph-review-directory-tls",
                move |_| {
                    HELD_TLS.with(|tls| {
                        *tls.borrow_mut() = Some(HeldTls {
                            entered,
                            release: released,
                        })
                    });
                },
            )
            .unwrap();
        entry.recv_timeout(Duration::from_secs(10)).unwrap();
        wait_until(|| phase(&configured.registration, Role::Coordinator) == Phase::Joining);
        session.close();
        drop(configured);
        wait_until(|| stats.read().exited);
        assert!(matches!(
            stats.read_full().unwrap().close_state,
            crate::ReclamationCloseState::Draining
        ));
        assert!(matches!(
            DirectorySession::attach(DirectoryPin::open(dir.path()).unwrap(), &options),
            Err(StartError::Busy)
        ));
        let different = options.with_admission_timeout_ms(1001);
        assert!(matches!(
            DirectorySession::attach(DirectoryPin::open(dir.path()).unwrap(), &different),
            Err(StartError::ConfigurationConflict)
        ));
        assert!(SERVICE
            .get()
            .unwrap()
            .core
            .inventory
            .lock()
            .unwrap()
            .has_identity(identity));
        drop(go);
        identity_removed(identity);
        assert!(matches!(
            stats.read_full().unwrap().close_state,
            crate::ReclamationCloseState::Drained
        ));
        assert!(
            DirectorySession::attach(DirectoryPin::open(dir.path()).unwrap(), &different)
                .unwrap()
                .is_none()
        );
        // Closed session and numeric stats have intentionally survived native completion.
        drop(session);
        drop(stats);
    }
}
