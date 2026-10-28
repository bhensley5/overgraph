//! Generation-owned, single-writer searchable ID histories.
//!
//! A stable published cell contains the immutable first version inline. Until
//! promotion its head is null and it owns no allocation. Appends own immutable
//! descriptors and 16-way tree nodes through intrusive, writer-private lists;
//! those lists are ONLY ownership bookkeeping, never a search path. Destruction
//! walks each list iteratively. Tree roots share backing but never own children.
use crate::engine::payload::{Drain, Owned};
use std::cell::UnsafeCell;
use std::mem::size_of;
use std::ptr;
use std::sync::atomic::{AtomicPtr, Ordering};

const FANOUT: usize = 16;

struct Version<T: Drain> {
    seq: u64,
    value: Owned<T>,
}
struct Head<T: Drain> {
    version: Version<T>,
    ordinal: usize,
    root: *mut Chunk<T>,
    height: usize,
    owned_next: *mut Head<T>,
    // Immutable ownership-list anchor for the quiescent test oracle. Readers of
    // this oracle never inspect the writer-private UnsafeCell<Owner>.
    #[cfg(test)]
    inventory_chunks: *mut Chunk<T>,
}
enum Slots<T: Drain> {
    Leaf([AtomicPtr<Version<T>>; FANOUT]),
    Branch([AtomicPtr<Chunk<T>>; FANOUT]),
}
struct Chunk<T: Drain> {
    slots: Slots<T>,
    owned_next: *mut Chunk<T>,
}
struct Owner<T: Drain> {
    heads: *mut Head<T>,
    chunks: *mut Chunk<T>,
}
pub(super) struct IdCell<T: Drain> {
    first: Version<T>,
    latest: AtomicPtr<Head<T>>,
    owner: UnsafeCell<Owner<T>>,
    #[cfg(test)]
    accesses: std::sync::atomic::AtomicUsize,
    #[cfg(test)]
    follows: std::sync::atomic::AtomicUsize,
}

// SAFETY: moving a cell before exposure is allowed: no first-version pointer
// exists until unsafe append is called in stable directory storage. After that
// call its contract prohibits moving the cell. All allocated backing moves with
// its generation owner and is destroyed only through exclusive Drop. T is Send.
unsafe impl<T: Drain + Send> Send for IdCell<T> {}
// SAFETY: readers access immutable first/descriptors/payloads and atomic links.
// The only UnsafeCell access while shared is by the exclusive writer through
// unsafe append; no reader examines Owner. Shared payload references require Sync.
unsafe impl<T: Drain + Send + Sync> Sync for IdCell<T> {}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Stage {
    BeforeSlot,
    AfterSlot,
    AfterHead,
}
#[cfg(test)]
#[derive(Clone, Copy, Debug, Default)]
pub(super) struct StructureInventory {
    pub(super) cells: usize,
    pub(super) descriptors: usize,
    pub(super) chunks: usize,
    pub(super) bytes: usize,
}
#[cfg(test)]
impl StructureInventory {
    pub(super) fn plus(self, other: Self) -> Self {
        Self {
            cells: self.cells + other.cells,
            descriptors: self.descriptors + other.descriptors,
            chunks: self.chunks + other.chunks,
            bytes: self.bytes + other.bytes,
        }
    }
    pub(super) fn extra(self) -> usize {
        self.bytes - 32 * (self.cells + self.descriptors)
    }
}
impl<T: Drain> IdCell<T> {
    pub(super) fn new(seq: u64, value: Owned<T>) -> Self {
        Self {
            first: Version { seq, value },
            latest: AtomicPtr::new(ptr::null_mut()),
            owner: UnsafeCell::new(Owner {
                heads: ptr::null_mut(),
                chunks: ptr::null_mut(),
            }),
            #[cfg(test)]
            accesses: std::sync::atomic::AtomicUsize::new(0),
            #[cfg(test)]
            follows: std::sync::atomic::AtomicUsize::new(0),
        }
    }
    // Approximate structural pressure, separate from the existing payload
    // estimate: the cell header plus the existing 32-byte directory-node estimate.
    pub(super) fn singleton_structure() -> usize {
        size_of::<Self>() - size_of::<Owned<T>>() + 32
    }
    pub(super) fn at(&self, seq: u64) -> Option<&Owned<T>> {
        self.select(self.latest.load(Ordering::Acquire), seq)
    }
    fn select(&self, head: *mut Head<T>, seq: u64) -> Option<&Owned<T>> {
        if head.is_null() {
            return (self.first.seq <= seq).then_some(&self.first.value);
        }
        // SAFETY: acquire-loaded descriptor was initialized before Release and
        // is immutable, owned until exclusive cell Drop. One load captures the
        // sequence, prefix length, root and height coherently.
        let head = unsafe { &*head };
        if head.version.seq <= seq {
            return Some(&head.version.value);
        }
        if seq < self.first.seq {
            return None;
        }
        // A snapshot one record behind needs one ordinal lookup, independent of
        // history depth. Global sequence gaps do not identify that predecessor.
        // Use only this acquired head's initialized prefix, never a newer head
        // or writer-private ownership bookkeeping.
        let previous_ordinal = head.ordinal - 1;
        let previous = self.indexed(head, previous_ordinal);
        if previous.seq <= seq {
            return Some(&previous.value);
        }
        // Both the captured head and its predecessor are newer than seq. Keep
        // the older-snapshot binary search, excluding these two known misses.
        let (mut lo, mut hi) = (0, previous_ordinal);
        while lo < hi {
            let mid = lo + (hi - lo) / 2;
            if self.indexed(head, mid).seq <= seq {
                lo = mid + 1;
            } else {
                hi = mid;
            }
        }
        lo.checked_sub(1)
            .map(|ordinal| &self.indexed(head, ordinal).value)
    }
    fn indexed<'a>(&'a self, prefix: &'a Head<T>, ordinal: usize) -> &'a Version<T> {
        assert!(ordinal <= prefix.ordinal, "captured initialized prefix");
        #[cfg(test)]
        self.accesses.fetch_add(1, Ordering::Relaxed);
        let mut node = prefix.root;
        for level in (0..=prefix.height).rev() {
            #[cfg(test)]
            self.follows.fetch_add(1, Ordering::Relaxed);
            // SAFETY: the acquired head reaches every slot in its initialized
            // prefix. All links are one-time initialized Release/Acquire; future
            // appends can fill unused slots but cannot replace these pointers.
            let chunk = unsafe { &*node };
            let digit = (ordinal >> (level * 4)) & (FANOUT - 1);
            match &chunk.slots {
                Slots::Branch(slots) if level > 0 => {
                    node = slots[digit].load(Ordering::Acquire);
                }
                Slots::Leaf(slots) if level == 0 => {
                    let version = slots[digit].load(Ordering::Acquire);
                    assert!(!version.is_null(), "initialized history slot");
                    // SAFETY: version is the stable inline first version or an
                    // immutable owned descriptor's version. Both outlive self.
                    return unsafe { &*version };
                }
                _ => unreachable!("history tree height"),
            }
            assert!(!node.is_null(), "initialized history branch");
        }
        unreachable!("history leaf")
    }
    /// # Safety
    /// Caller holds this generation's exclusive engine writer custody, including
    /// against other appends to this cell. The cell has reached stable storage
    /// and will not move again before Drop. All readers retain the generation.
    /// The interface is private; no safe general concurrent mutation API exists.
    pub(super) unsafe fn append(&self, seq: u64, value: Owned<T>) -> Result<usize, ()> {
        // SAFETY: exactly the caller's exclusive-writer/stable-storage contract.
        unsafe { self.append_with(seq, value, |_| {}) }
    }
    unsafe fn append_with(
        &self,
        seq: u64,
        value: Owned<T>,
        mut hook: impl FnMut(Stage),
    ) -> Result<usize, ()> {
        let previous = self.latest.load(Ordering::Acquire);
        // SAFETY: generation still owns the immutable prior descriptor.
        let previous = unsafe { previous.as_ref() };
        let last_seq = previous.map_or(self.first.seq, |p| p.version.seq);
        if seq <= last_seq {
            return Err(());
        }
        let ordinal = previous
            .map_or(Some(1), |p| p.ordinal.checked_add(1))
            .ok_or(())?;
        // Check all arithmetic before mutation/allocation. Each level consumes
        // four ordinal bits, so shifts and Drop depth cannot grow unboundedly.
        let mut height = previous.map_or(0, |p| p.height);
        let mut capacity = FANOUT
            .checked_pow(u32::try_from(height + 1).map_err(|_| ())?)
            .ok_or(())?;
        let grow = ordinal >= capacity;
        if grow {
            capacity = capacity.checked_mul(FANOUT).ok_or(())?;
            height += 1;
        }
        debug_assert!(ordinal < capacity);
        // SAFETY: only this writer touches Owner; readers never borrow it.
        let owner = unsafe { &mut *self.owner.get() };
        let mut bytes = 0;
        let mut allocate = |slots| {
            let node = Box::into_raw(Box::new(Chunk {
                slots,
                owned_next: owner.chunks,
            }));
            owner.chunks = node;
            bytes += size_of::<Chunk<T>>();
            node
        };
        let mut root = if let Some(p) = previous {
            p.root
        } else {
            let root = allocate(Slots::Leaf(std::array::from_fn(|_| {
                AtomicPtr::new(ptr::null_mut())
            })));
            // SAFETY: first's address is taken only now, after stable exposure.
            if let Slots::Leaf(slots) = unsafe { &(*root).slots } {
                slots[0].store(ptr::addr_of!(self.first).cast_mut(), Ordering::Release);
            }
            root
        };
        if grow {
            let old = root;
            root = allocate(Slots::Branch(std::array::from_fn(|_| {
                AtomicPtr::new(ptr::null_mut())
            })));
            // SAFETY: private, correctly typed new root; old roots remain owned.
            if let Slots::Branch(slots) = unsafe { &(*root).slots } {
                slots[0].store(old, Ordering::Release);
            }
        }
        let mut leaf = root;
        for level in (1..=height).rev() {
            // SAFETY: initialized root/path; writer alone fills absent slots.
            let slots = match unsafe { &(*leaf).slots } {
                Slots::Branch(s) => s,
                _ => unreachable!(),
            };
            let slot = &slots[(ordinal >> (level * 4)) & (FANOUT - 1)];
            let mut child = slot.load(Ordering::Acquire);
            if child.is_null() {
                child = allocate(if level == 1 {
                    Slots::Leaf(std::array::from_fn(|_| AtomicPtr::new(ptr::null_mut())))
                } else {
                    Slots::Branch(std::array::from_fn(|_| AtomicPtr::new(ptr::null_mut())))
                });
                slot.store(child, Ordering::Release);
            }
            leaf = child;
        }
        let new = Box::into_raw(Box::new(Head {
            version: Version { seq, value },
            ordinal,
            root,
            height,
            owned_next: owner.heads,
            #[cfg(test)]
            inventory_chunks: owner.chunks,
        }));
        owner.heads = new;
        bytes += size_of::<Head<T>>() - size_of::<Owned<T>>();
        // SAFETY: the path reaches a leaf, and this ordinal is unused. The
        // descriptor is already at its permanent address before its field links.
        let slot = match unsafe { &(*leaf).slots } {
            Slots::Leaf(s) => &s[ordinal & (FANOUT - 1)],
            _ => unreachable!(),
        };
        assert!(
            slot.load(Ordering::Relaxed).is_null(),
            "append-only history slot"
        );
        hook(Stage::BeforeSlot);
        slot.store(
            unsafe { ptr::addr_of_mut!((*new).version) },
            Ordering::Release,
        );
        hook(Stage::AfterSlot);
        self.latest.store(new, Ordering::Release);
        hook(Stage::AfterHead);
        Ok(bytes)
    }
    /// Independent allocation-list inventory, not a sum of append's returned
    /// charges. Call after installations are quiescent to inventory all backing.
    /// During an append this safely observes only the acquired completed prefix.
    #[cfg(test)]
    pub(super) fn structure_inventory_for_test(&self) -> StructureInventory {
        let mut head = self.latest.load(Ordering::Acquire);
        let mut inventory = StructureInventory {
            cells: 1,
            ..Default::default()
        };
        let mut chunk = if head.is_null() {
            ptr::null_mut()
        } else {
            // SAFETY: acquire-published immutable anchor; self retains all backing.
            unsafe { (*head).inventory_chunks }
        };
        while !head.is_null() {
            inventory.descriptors += 1;
            // SAFETY: immutable, uniquely owned descriptor-list links; no Owner
            // bookkeeping is read and appending cannot modify existing links.
            head = unsafe { (*head).owned_next };
        }
        while !chunk.is_null() {
            inventory.chunks += 1;
            // SAFETY: same immutable ownership-list/generation-lifetime argument.
            chunk = unsafe { (*chunk).owned_next };
        }
        // Derive byte terms from actual object counts/layout, independently of
        // singleton_structure(), append's byte accumulator and charge counters.
        // Payload estimates and the existing 32-byte directory approximation
        // retain their prior meaning; this is only a structural-pressure oracle.
        inventory.bytes = size_of::<Self>() - size_of::<Owned<T>>()
            + 32
            + inventory.descriptors * (size_of::<Head<T>>() - size_of::<Owned<T>>())
            + inventory.chunks * size_of::<Chunk<T>>();
        inventory
    }
    #[cfg(test)]
    pub(super) fn fold<A>(&self, mut initial: A, mut f: impl FnMut(A, &Owned<T>) -> A) -> A {
        let head = self.latest.load(Ordering::Acquire);
        if head.is_null() {
            return f(initial, &self.first.value);
        }
        // SAFETY: the same acquire/generation-lifetime argument as select.
        let prefix = unsafe { &*head };
        for i in 0..=prefix.ordinal {
            initial = f(initial, &self.indexed(prefix, i).value);
        }
        initial
    }
}
impl<T: Drain> Drop for IdCell<T> {
    fn drop(&mut self) {
        let owner = self.owner.get_mut();
        while !owner.heads.is_null() {
            // SAFETY: exclusive Drop, each allocation belongs to exactly one
            // intrusive owner list. Raw tree slots do not own versions. Owned<T>
            // drains payloads through existing custody, exactly once.
            let head = unsafe { Box::from_raw(owner.heads) };
            owner.heads = head.owned_next;
            drop(head);
        }
        while !owner.chunks.is_null() {
            // SAFETY: same unique-list ownership; no recursive child Drop.
            let chunk = unsafe { Box::from_raw(owner.chunks) };
            owner.chunks = chunk.owned_next;
            drop(chunk);
        }
        // The inline first version is dropped by ordinary field drop glue.
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{atomic::AtomicUsize, mpsc, Arc};
    struct Value(u64, Arc<AtomicUsize>);
    impl Drain for Value {
        fn drain_payload(&mut self) {
            self.1.fetch_add(1, Ordering::Relaxed);
        }
    }
    fn value(seq: u64, drops: &Arc<AtomicUsize>) -> Owned<Value> {
        Owned::new(Value(seq, drops.clone()))
    }
    #[test]
    fn id_history_boundaries_prefix_work_and_iterative_drain() {
        let drops = Arc::new(AtomicUsize::new(0));
        let cell = Box::new(IdCell::new(0, value(0, &drops)));
        assert!(cell.latest.load(Ordering::Acquire).is_null());
        assert!(unsafe { &*cell.owner.get() }.chunks.is_null());
        assert_eq!(cell.at(0).unwrap().0, 0);
        let mut retained = Vec::new();
        for seq in 1..=4096 {
            // SAFETY: one writer, Box provides stable storage, no moving cell.
            unsafe { cell.append(seq, value(seq, &drops)) }.unwrap();
            if [1, 14, 15, 16, 254, 255, 256, 4095, 4096].contains(&seq) {
                retained.push(cell.latest.load(Ordering::Acquire));
                for s in [0, seq / 2, seq, seq + 1] {
                    assert_eq!(cell.at(s).unwrap().0, s.min(seq));
                }
            }
        }
        cell.accesses.store(0, Ordering::Relaxed);
        cell.follows.store(0, Ordering::Relaxed);
        assert_eq!(cell.at(1).unwrap().0, 1);
        assert!(cell.accesses.load(Ordering::Relaxed) <= 15);
        assert!(cell.follows.load(Ordering::Relaxed) <= 60);
        // Ownership/storage grows with versions, independently of numeric IDs.
        // 4097 versions require ceil(H/16)+ceil(H/256)+ceil(H/4096)
        // +ceil(H/65536)=277 tree nodes, retaining shared old roots once.
        let mut chunk = unsafe { &*cell.owner.get() }.chunks;
        let mut chunks = 0;
        while !chunk.is_null() {
            chunks += 1;
            chunk = unsafe { (*chunk).owned_next };
        }
        assert_eq!(chunks, 277);
        for prefix in retained {
            let seq = unsafe { (*prefix).version.seq };
            assert_eq!(cell.select(prefix, seq - 1).unwrap().0, seq - 1);
        }
        assert!(unsafe { cell.append(4096, value(999, &drops)) }.is_err());
        assert!(unsafe { cell.append(1, value(999, &drops)) }.is_err());
        assert_eq!(cell.at(u64::MAX).unwrap().0, 4096);
        drop(cell);
        assert_eq!(drops.load(Ordering::Relaxed), 4099);
    }
    #[test]
    fn id_history_paused_writer_never_blocks_readers() {
        for length in [1, 16, 256, 4096] {
            for stage in [Stage::BeforeSlot, Stage::AfterSlot, Stage::AfterHead] {
                let drops = Arc::new(AtomicUsize::new(0));
                let cell = Arc::new(IdCell::new(0, value(0, &drops)));
                for seq in 1..length {
                    unsafe { cell.append(seq, value(seq, &drops)) }.unwrap();
                }
                let (ready_tx, ready_rx) = mpsc::channel();
                let (release_tx, release_rx) = mpsc::channel();
                let writer = cell.clone();
                let counter = drops.clone();
                let thread = std::thread::spawn(move || {
                    unsafe {
                        writer.append_with(length, value(length, &counter), |at| {
                            if at == stage {
                                ready_tx.send(()).unwrap();
                                release_rx.recv().unwrap();
                            }
                        })
                    }
                    .unwrap()
                });
                ready_rx
                    .recv_timeout(std::time::Duration::from_secs(5))
                    .unwrap();
                let reader = cell.clone();
                let (read_tx, read_rx) = mpsc::channel();
                let read = std::thread::spawn(move || {
                    assert_eq!(reader.at(0).unwrap().0, 0);
                    assert_eq!(reader.at(length - 1).unwrap().0, length - 1);
                    read_tx.send(reader.at(u64::MAX).unwrap().0).unwrap();
                });
                let observed = read_rx
                    .recv_timeout(std::time::Duration::from_secs(5))
                    .expect("paused writer blocked reader");
                assert_eq!(
                    observed,
                    if stage == Stage::AfterHead {
                        length
                    } else {
                        length - 1
                    }
                );
                release_tx.send(()).unwrap();
                thread.join().unwrap();
                read.join().unwrap();
                drop(cell);
                assert_eq!(drops.load(Ordering::Relaxed), length as usize + 1);
            }
        }
    }
    #[test]
    fn id_history_nonzero_first_and_max_sequence() {
        let drops = Arc::new(AtomicUsize::new(0));
        let cell = Box::new(IdCell::new(5, value(5, &drops)));
        assert!(cell.at(0).is_none());
        unsafe { cell.append(u64::MAX, value(u64::MAX, &drops)) }.unwrap();
        assert_eq!(cell.at(u64::MAX - 1).unwrap().0, 5);
        assert_eq!(cell.at(u64::MAX).unwrap().0, u64::MAX);
        drop(cell);
        assert_eq!(drops.load(Ordering::Relaxed), 2);
    }

    #[test]
    fn id_history_penultimate_gaps_tombstones_and_captured_heads() {
        struct Opinion(Option<u64>);
        impl Drain for Opinion {
            fn drain_payload(&mut self) {}
        }
        let cell = Box::new(IdCell::new(5, Owned::new(Opinion(Some(0)))));
        let mut model = vec![(5, Some(0))];
        let mut retained = Vec::new();
        for ordinal in 1..=256 {
            let seq = 5 + 17 * ordinal;
            let opinion = (ordinal % 7 != 3).then_some(ordinal);
            unsafe { cell.append(seq, Owned::new(Opinion(opinion))) }.unwrap();
            model.push((seq, opinion));
            if [1, 2, 15, 16, 255, 256].contains(&ordinal) {
                retained.push((ordinal as usize, cell.latest.load(Ordering::Acquire)));
            }
        }
        for (ordinal, head) in retained {
            assert!(cell.select(head, 4).is_none());
            for &(seq, _) in &model[..=ordinal] {
                for snapshot in [seq - 1, seq, seq + 1] {
                    let expected = model[..=ordinal]
                        .iter()
                        .rev()
                        .find(|(s, _)| *s <= snapshot)
                        .map(|(_, opinion)| *opinion);
                    assert_eq!(cell.select(head, snapshot).map(|v| v.0), expected);
                }
            }
            cell.accesses.store(0, Ordering::Relaxed);
            let (previous_seq, previous_opinion) = model[ordinal - 1];
            assert_eq!(
                cell.select(head, previous_seq).map(|v| v.0),
                Some(previous_opinion)
            );
            assert_eq!(cell.accesses.load(Ordering::Relaxed), 1);
            // A later acquired prefix must not affect a captured head's latest.
            assert_eq!(
                cell.select(head, u64::MAX).map(|v| v.0),
                Some(model[ordinal].1)
            );
        }
    }
}
