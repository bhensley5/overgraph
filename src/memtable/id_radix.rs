//! Compressed nibble directory, under existing generation custody.
//! One writer publishes complete replacements; readers never retry or wait.
//! Every concrete allocation has a small immutable typed header and belongs to
//! one intrusive owner list. Old routes and stable history cells survive to Drop.
use super::id_history::IdCell;
use crate::engine::participant::Session;
use crate::engine::payload::Drain;
use std::cell::UnsafeCell;
use std::marker::PhantomData;
use std::mem::size_of;
use std::ops::{Bound, ControlFlow};
use std::ptr;
use std::sync::atomic::{AtomicPtr, Ordering};

#[repr(u8)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Kind {
    Leaf,
    Four,
    Sixteen,
}
#[repr(C)]
struct Header {
    kind: Kind,
    prefix_len: u8,
    // Absolute high-nibble prefix; a leaf uses all 16 nibbles as its full ID.
    prefix: u64,
    owned_next: *mut Header,
}
#[repr(C)]
struct Leaf<T: Drain> {
    header: Header,
    cell: IdCell<T>,
}
#[repr(C)]
struct Node4 {
    header: Header,
    len: u8,
    digits: [u8; 4],
    children: [AtomicPtr<Header>; 4],
}
#[repr(C)]
struct Node16 {
    header: Header,
    children: [AtomicPtr<Header>; 16],
}

pub(super) struct RadixDirectory<T: Drain> {
    root: AtomicPtr<Header>,
    owner: UnsafeCell<*mut Header>,
    payload: PhantomData<T>,
    #[cfg(test)]
    inventory_head: AtomicPtr<Header>,
    #[cfg(test)]
    scan_groups: std::sync::atomic::AtomicUsize,
    #[cfg(test)]
    sorted_work: [std::sync::atomic::AtomicUsize; 2],
}
// SAFETY: concrete backing moves only as raw ownership, not as allocations. Drop
// is exclusive and drains T through the unchanged cells. T must be Send.
unsafe impl<T: Drain + Send> Send for RadixDirectory<T> {}
// SAFETY: readers touch only immutable headers/Node4 arrays and atomic links.
// Only unsafe insertion accesses writer-private owner bookkeeping. Shared cell
// payloads additionally require Sync. Backing lives for the owning generation.
unsafe impl<T: Drain + Send + Sync> Sync for RadixDirectory<T> {}

fn prefix(id: u64, len: u8) -> u64 {
    if len == 0 {
        0
    } else {
        id & (u64::MAX << (64 - 4 * u32::from(len)))
    }
}
fn digit(id: u64, len: u8) -> u8 {
    debug_assert!(len < 16);
    ((id >> (60 - 4 * u32::from(len))) & 15) as u8
}
fn high(header: &Header) -> u64 {
    if header.prefix_len == 0 {
        u64::MAX
    } else if header.prefix_len == 16 {
        header.prefix
    } else {
        header.prefix | (u64::MAX >> (4 * u32::from(header.prefix_len)))
    }
}

impl<T: Drain> RadixDirectory<T> {
    pub(super) fn new() -> Self {
        Self {
            root: AtomicPtr::new(ptr::null_mut()),
            owner: UnsafeCell::new(ptr::null_mut()),
            payload: PhantomData,
            #[cfg(test)]
            inventory_head: AtomicPtr::new(ptr::null_mut()),
            #[cfg(test)]
            scan_groups: std::sync::atomic::AtomicUsize::new(0),
            #[cfg(test)]
            sorted_work: std::array::from_fn(|_| std::sync::atomic::AtomicUsize::new(0)),
        }
    }
    pub(super) fn get<'a>(&'a self, _session: &'a Session<'_>, id: &u64) -> Option<&'a IdCell<T>> {
        self.find(*id)
    }
    fn find(&self, id: u64) -> Option<&IdCell<T>> {
        let mut at = self.root.load(Ordering::Acquire);
        while !at.is_null() {
            // SAFETY: an acquired route owns initialized immutable typed backing;
            // no route is freed/retyped until exclusive generation destruction.
            let h = unsafe { &*at };
            if prefix(id, h.prefix_len) != h.prefix {
                return None;
            }
            match h.kind {
                Kind::Leaf => return Some(unsafe { &(*at.cast::<Leaf<T>>()).cell }),
                Kind::Four => {
                    let n = unsafe { &*at.cast::<Node4>() };
                    let d = digit(id, h.prefix_len);
                    let i = n.digits[..usize::from(n.len)]
                        .iter()
                        .position(|&x| x == d)?;
                    at = n.children[i].load(Ordering::Acquire);
                }
                Kind::Sixteen => {
                    let n = unsafe { &*at.cast::<Node16>() };
                    at = n.children[usize::from(digit(id, h.prefix_len))].load(Ordering::Acquire);
                }
            }
        }
        None
    }
    // SAFETY for every allocator: sole insertion writer; caller links only the
    // completely initialized Box. repr(C) makes Header its first field. The owner
    // list is never inspected by production readers, and is destroyed iteratively.
    unsafe fn leaf(&self, id: u64, cell: IdCell<T>) -> *mut Header {
        let owner = unsafe { &mut *self.owner.get() };
        let p = Box::into_raw(Box::new(Leaf {
            header: Header {
                kind: Kind::Leaf,
                prefix_len: 16,
                prefix: id,
                owned_next: *owner,
            },
            cell,
        }))
        .cast::<Header>();
        *owner = p;
        p
    }
    unsafe fn four(&self, key: u64, len: u8, pairs: &[(u8, *mut Header)]) -> *mut Header {
        assert!((2..=4).contains(&pairs.len()));
        debug_assert!(pairs.windows(2).all(|p| p[0].0 < p[1].0));
        let owner = unsafe { &mut *self.owner.get() };
        let p = Box::into_raw(Box::new(Node4 {
            header: Header {
                kind: Kind::Four,
                prefix_len: len,
                prefix: prefix(key, len),
                owned_next: *owner,
            },
            len: pairs.len() as u8,
            digits: std::array::from_fn(|i| pairs.get(i).map_or(0, |p| p.0)),
            children: std::array::from_fn(|i| {
                AtomicPtr::new(pairs.get(i).map_or(ptr::null_mut(), |p| p.1))
            }),
        }))
        .cast::<Header>();
        *owner = p;
        p
    }
    unsafe fn sixteen(&self, key: u64, len: u8, pairs: &[(u8, *mut Header)]) -> *mut Header {
        let owner = unsafe { &mut *self.owner.get() };
        let p = Box::into_raw(Box::new(Node16 {
            header: Header {
                kind: Kind::Sixteen,
                prefix_len: len,
                prefix: prefix(key, len),
                owned_next: *owner,
            },
            children: std::array::from_fn(|i| {
                AtomicPtr::new(
                    pairs
                        .iter()
                        .find(|p| usize::from(p.0) == i)
                        .map_or(ptr::null_mut(), |p| p.1),
                )
            }),
        }))
        .cast::<Header>();
        *owner = p;
        p
    }
    /// # Safety
    /// Same sole generation writer custody as primary installation. Unique ID is
    /// a preparation fact. Exposed leaves/cells are never moved or replaced.
    /// Return all new routing/leaf-header bytes, including retired replacements;
    /// the facade separately charges the unchanged cell/history structure.
    pub(super) unsafe fn insert_unique(
        &self,
        _session: &Session<'_>,
        id: u64,
        cell: IdCell<T>,
    ) -> usize {
        unsafe { self.insert_with(id, cell, || {}) }
    }
    unsafe fn insert_with(
        &self,
        id: u64,
        cell: IdCell<T>,
        mut before_publish: impl FnMut(),
    ) -> usize {
        let mut link = &self.root;
        let leaf_bytes = size_of::<Leaf<T>>() - size_of::<IdCell<T>>();
        let bytes = loop {
            let at = link.load(Ordering::Acquire);
            if at.is_null() {
                let new = unsafe { self.leaf(id, cell) };
                before_publish();
                link.store(new, Ordering::Release);
                break leaf_bytes;
            }
            let h = unsafe { &*at };
            let shared = ((h.prefix ^ id).leading_zeros() / 4) as u8;
            if shared < h.prefix_len {
                let new = unsafe { self.leaf(id, cell) };
                let a = (digit(h.prefix, shared), at);
                let b = (digit(id, shared), new);
                let pairs = if a.0 < b.0 { [a, b] } else { [b, a] };
                let branch = unsafe { self.four(id, shared, &pairs) };
                before_publish();
                link.store(branch, Ordering::Release);
                break leaf_bytes + size_of::<Node4>();
            }
            match h.kind {
                Kind::Leaf => panic!("duplicate radix primary ID"),
                Kind::Four => {
                    let n = unsafe { &*at.cast::<Node4>() };
                    let d = digit(id, h.prefix_len);
                    if let Some(i) = n.digits[..usize::from(n.len)].iter().position(|&x| x == d) {
                        link = &n.children[i];
                        continue;
                    }
                    let new = unsafe { self.leaf(id, cell) };
                    let mut pairs = [(0, ptr::null_mut()); 5];
                    for (i, p) in pairs.iter_mut().enumerate().take(usize::from(n.len)) {
                        *p = (n.digits[i], n.children[i].load(Ordering::Acquire));
                    }
                    pairs[usize::from(n.len)] = (d, new);
                    let used = &mut pairs[..usize::from(n.len) + 1];
                    used.sort_unstable_by_key(|p| p.0);
                    let (replacement, bytes) = if n.len == 4 {
                        (
                            unsafe { self.sixteen(id, h.prefix_len, used) },
                            size_of::<Node16>(),
                        )
                    } else {
                        (
                            unsafe { self.four(id, h.prefix_len, used) },
                            size_of::<Node4>(),
                        )
                    };
                    before_publish();
                    link.store(replacement, Ordering::Release);
                    break leaf_bytes + bytes;
                }
                Kind::Sixteen => {
                    let n = unsafe { &*at.cast::<Node16>() };
                    link = &n.children[usize::from(digit(id, h.prefix_len))];
                }
            }
        };
        #[cfg(test)]
        self.inventory_head
            .store(unsafe { *self.owner.get() }, Ordering::Release);
        bytes
    }
    fn cursor(&self, bound: Bound<u64>) -> Cursor<'_, T> {
        Cursor {
            _owner: self,
            pending: self.root.load(Ordering::Acquire),
            stack: [Frame {
                node: ptr::null_mut(),
                next: 0,
            }; 16],
            depth: 0,
            lower: bound,
        }
    }
    pub(super) fn walk_id_cells<'a>(
        &'a self,
        _session: &'a Session<'_>,
        bounds: (Option<u64>, u64),
        max_groups: usize,
        mut copy: impl FnMut(u64, &'a IdCell<T>) -> bool,
    ) -> (Option<u64>, bool) {
        let (mut last, upper) = bounds;
        let mut cursor = self.cursor(last.map_or(Bound::Unbounded, Bound::Excluded));
        for _ in 0..max_groups {
            let Some((id, cell)) = cursor.next() else {
                return (last, true);
            };
            if id > upper {
                return (last, true);
            }
            #[cfg(test)]
            self.scan_groups.fetch_add(1, Ordering::Relaxed);
            if !copy(id, cell) {
                return (last, false);
            }
            last = Some(id);
        }
        (last, cursor.next().is_none_or(|(id, _)| id > upper))
    }
    pub(super) fn copy_sorted_id_cells<'a>(
        &'a self,
        _session: &'a Session<'_>,
        ids: &[u64],
        mut copy: impl FnMut(u64, Option<&'a IdCell<T>>) -> ControlFlow<bool>,
    ) -> usize {
        let mut cursor = None;
        let mut entry: Option<(u64, &'a IdCell<T>)> = None;
        let mut previous = None;
        let mut consumed = 0;
        for &id in ids.iter().take(4096) {
            if previous != Some(id) {
                if entry.is_some_and(|e| e.0 < id) {
                    #[cfg(test)]
                    self.sorted_work[1].fetch_add(1, Ordering::Relaxed);
                    entry = cursor.as_mut().and_then(Cursor::next);
                }
                if entry.is_none_or(|e| e.0 != id) {
                    #[cfg(test)]
                    self.sorted_work[0].fetch_add(1, Ordering::Relaxed);
                    cursor = Some(self.cursor(Bound::Included(id)));
                    entry = cursor.as_mut().and_then(Cursor::next);
                }
            }
            previous = Some(id);
            match copy(id, entry.filter(|e| e.0 == id).map(|e| e.1)) {
                ControlFlow::Continue(()) => consumed += 1,
                ControlFlow::Break(accepted) => {
                    consumed += usize::from(accepted);
                    break;
                }
            }
        }
        consumed
    }
    #[cfg(test)]
    pub(super) fn fold_all_for_test<A>(
        &self,
        _session: &Session<'_>,
        mut initial: A,
        mut fold: impl FnMut(A, &u64, &IdCell<T>) -> A,
    ) -> A {
        let mut cursor = self.cursor(Bound::Unbounded);
        while let Some((id, cell)) = cursor.next() {
            initial = fold(initial, &id, cell);
        }
        initial
    }
    #[cfg(test)]
    pub(super) fn take_scan_groups_for_test(&self) -> usize {
        self.scan_groups.swap(0, Ordering::Relaxed)
    }
    #[cfg(test)]
    pub(super) fn take_sorted_walk_work_for_test(&self) -> [usize; 2] {
        std::array::from_fn(|i| self.sorted_work[i].swap(0, Ordering::Relaxed))
    }
    /// Quiescent inventory of the actual concrete owner list, not insertion's
    /// charge accumulator. The atomic test anchor publishes immutable list links.
    #[cfg(test)]
    pub(super) fn routing_inventory_for_test(&self) -> ([usize; 3], usize) {
        let mut p = self.inventory_head.load(Ordering::Acquire);
        let mut counts = [0; 3];
        let mut bytes = 0;
        while !p.is_null() {
            let h = unsafe { &*p };
            match h.kind {
                Kind::Leaf => {
                    counts[0] += 1;
                    bytes += size_of::<Leaf<T>>() - size_of::<IdCell<T>>();
                }
                Kind::Four => {
                    counts[1] += 1;
                    bytes += size_of::<Node4>();
                }
                Kind::Sixteen => {
                    counts[2] += 1;
                    bytes += size_of::<Node16>();
                }
            }
            p = h.owned_next;
        }
        (counts, bytes)
    }
}

#[derive(Clone, Copy)]
struct Frame {
    node: *mut Header,
    next: u8,
}
struct Cursor<'a, T: Drain> {
    _owner: &'a RadixDirectory<T>,
    pending: *mut Header,
    stack: [Frame; 16],
    depth: usize,
    lower: Bound<u64>,
}
impl<'a, T: Drain> Cursor<'a, T> {
    fn next_child(&mut self) -> *mut Header {
        while self.depth > 0 {
            let frame = &mut self.stack[self.depth - 1];
            // SAFETY: the captured route and every replacement below it retain
            // all prior IDs in the same disjoint digit range until owner Drop.
            let h = unsafe { &*frame.node };
            match h.kind {
                Kind::Four => {
                    let n = unsafe { &*frame.node.cast::<Node4>() };
                    if frame.next < n.len {
                        let i = frame.next;
                        frame.next += 1;
                        return n.children[usize::from(i)].load(Ordering::Acquire);
                    }
                }
                Kind::Sixteen => {
                    let n = unsafe { &*frame.node.cast::<Node16>() };
                    while frame.next < 16 {
                        let i = frame.next;
                        frame.next += 1;
                        let p = n.children[usize::from(i)].load(Ordering::Acquire);
                        if !p.is_null() {
                            return p;
                        }
                    }
                }
                Kind::Leaf => unreachable!("only branches on cursor stack"),
            }
            self.depth -= 1;
        }
        ptr::null_mut()
    }
    fn next(&mut self) -> Option<(u64, &'a IdCell<T>)> {
        loop {
            let p = if !self.pending.is_null() {
                let p = self.pending;
                self.pending = ptr::null_mut();
                p
            } else {
                self.next_child()
            };
            if p.is_null() {
                return None;
            }
            let h = unsafe { &*p };
            let below = match self.lower {
                Bound::Unbounded => false,
                Bound::Included(id) => high(h) < id,
                Bound::Excluded(id) => high(h) <= id,
            };
            if below {
                continue;
            }
            if h.kind == Kind::Leaf {
                self.lower = Bound::Unbounded;
                // SAFETY: kind is immutable, repr(C) preserves header location,
                // and the cursor's generation borrow keeps this stable cell alive.
                return Some((h.prefix, unsafe { &(*p.cast::<Leaf<T>>()).cell }));
            }
            assert!(self.depth < 16, "u64 nibble path depth");
            self.stack[self.depth] = Frame { node: p, next: 0 };
            self.depth += 1;
        }
    }
}
impl<T: Drain> Drop for RadixDirectory<T> {
    fn drop(&mut self) {
        let mut p = *self.owner.get_mut();
        while !p.is_null() {
            // SAFETY: exclusive owner destruction, one list entry per concrete
            // allocation. Child links never own backing. Drop is iterative; the
            // stable leaf invokes the unchanged IdCell/Owned payload drain once.
            let h = unsafe { &*p };
            let next = h.owned_next;
            let kind = h.kind;
            unsafe {
                match kind {
                    Kind::Leaf => drop(Box::from_raw(p.cast::<Leaf<T>>())),
                    Kind::Four => drop(Box::from_raw(p.cast::<Node4>())),
                    Kind::Sixteen => drop(Box::from_raw(p.cast::<Node16>())),
                }
            }
            p = next;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::payload::Owned;
    use std::collections::BTreeSet;
    use std::sync::{atomic::AtomicUsize, mpsc, Arc};
    struct Value(u64, Arc<AtomicUsize>);
    impl Drain for Value {
        fn drain_payload(&mut self) {
            self.1.fetch_add(1, Ordering::Relaxed);
        }
    }
    fn cell(id: u64, drops: &Arc<AtomicUsize>) -> IdCell<Value> {
        IdCell::new(0, Owned::new(Value(id, drops.clone())))
    }
    fn ids(d: &RadixDirectory<Value>, bound: Bound<u64>) -> Vec<u64> {
        let mut c = d.cursor(bound);
        let mut out = Vec::new();
        while let Some((id, v)) = c.next() {
            assert_eq!(v.at(0).unwrap().0, id);
            out.push(id);
        }
        out
    }
    #[test]
    fn id_radix_ordered_model_dense_sparse_bounds_and_pressure() {
        let drops = Arc::new(AtomicUsize::new(0));
        let d = RadixDirectory::new();
        let mut model = BTreeSet::new();
        let mut charge = 0;
        let mut inputs = (0..512).collect::<Vec<u64>>();
        let mut seed = 0x1026_2026_9876_u64;
        for _ in 0..512 {
            seed ^= seed << 13;
            seed ^= seed >> 7;
            seed ^= seed << 17;
            inputs.push(seed);
        }
        inputs.extend([0, u64::MAX, u64::MAX - 1, 1 << 60, 1 << 32]);
        for id in inputs {
            if model.insert(id) {
                unsafe {
                    charge += d.insert_with(id, cell(id, &drops), || {});
                }
            }
        }
        let expected = model.iter().copied().collect::<Vec<_>>();
        assert_eq!(ids(&d, Bound::Unbounded), expected);
        for &id in &expected {
            assert_eq!(d.find(id).unwrap().at(0).unwrap().0, id);
        }
        for start in [0, 1, 17, 511, 512, 1 << 32, 1 << 60, u64::MAX - 1, u64::MAX] {
            assert_eq!(
                ids(&d, Bound::Included(start)),
                model.range(start..).copied().collect::<Vec<_>>()
            );
            assert_eq!(
                ids(&d, Bound::Excluded(start)),
                model
                    .range((Bound::Excluded(start), Bound::Unbounded))
                    .copied()
                    .collect::<Vec<_>>()
            );
        }
        let mut paged = Vec::new();
        let mut bound = Bound::Unbounded;
        loop {
            let mut cursor = d.cursor(bound);
            let page = (0..3)
                .filter_map(|_| cursor.next().map(|x| x.0))
                .collect::<Vec<_>>();
            if page.is_empty() {
                break;
            }
            bound = Bound::Excluded(*page.last().unwrap());
            paged.extend(page);
        }
        assert_eq!(paged, expected);
        let (counts, bytes) = d.routing_inventory_for_test();
        assert_eq!(counts[0], model.len());
        assert_eq!(charge, bytes);
        let count = model.len();
        drop(d);
        assert_eq!(drops.load(Ordering::Relaxed), count);
    }
    #[test]
    fn id_radix_node4_growth_promotion_and_old_routes() {
        let drops = Arc::new(AtomicUsize::new(0));
        let d = RadixDirectory::new();
        let mut charge = 0;
        for digit in [0_u64, 2, 1, 3] {
            unsafe {
                charge += d.insert_with(digit << 60, cell(digit << 60, &drops), || {});
            }
        }
        let root = d.root.load(Ordering::Acquire);
        assert_eq!(unsafe { (*root).kind }, Kind::Four);
        let old = d.cursor(Bound::Unbounded);
        unsafe {
            charge += d.insert_with(4 << 60, cell(4 << 60, &drops), || {});
        }
        assert_eq!(
            unsafe { (*d.root.load(Ordering::Acquire)).kind },
            Kind::Sixteen
        );
        let mut old = old;
        let mut seen = Vec::new();
        while let Some((id, _)) = old.next() {
            seen.push(id);
        }
        assert_eq!(seen, (0..4).map(|x| x << 60).collect::<Vec<_>>());
        for digit in 5..16 {
            unsafe {
                charge += d.insert_with(digit << 60, cell(digit << 60, &drops), || {});
            }
        }
        assert_eq!(
            ids(&d, Bound::Unbounded),
            (0..16).map(|x| x << 60).collect::<Vec<_>>()
        );
        assert_eq!(d.routing_inventory_for_test(), ([16, 3, 1], charge));
        drop(d);
        assert_eq!(drops.load(Ordering::Relaxed), 16);
    }
    #[test]
    fn id_radix_paused_publication_preserves_existing_reads_and_routes() {
        for (existing, new) in [
            (vec![0], u64::MAX),
            (vec![0, 2 << 60], 1 << 60),
            (vec![0, 1 << 60, 2 << 60, 3 << 60], 4 << 60),
            (vec![0, 1 << 60, 2 << 60, 3 << 60, 4 << 60], 5 << 60),
            (vec![0, 1, 2, 3, 4], 1 << 32),
        ] {
            let drops = Arc::new(AtomicUsize::new(0));
            let d = Arc::new(RadixDirectory::new());
            for &id in &existing {
                unsafe {
                    d.insert_with(id, cell(id, &drops), || {});
                }
            }
            let mut old = d.cursor(Bound::Unbounded);
            let writer = d.clone();
            let counter = drops.clone();
            let (ready_tx, ready_rx) = mpsc::channel();
            let (release_tx, release_rx) = mpsc::channel();
            let job = std::thread::spawn(move || unsafe {
                writer.insert_with(new, cell(new, &counter), || {
                    ready_tx.send(()).unwrap();
                    release_rx.recv().unwrap();
                })
            });
            ready_rx
                .recv_timeout(std::time::Duration::from_secs(5))
                .unwrap();
            for &id in &existing {
                assert_eq!(d.find(id).unwrap().at(0).unwrap().0, id);
            }
            assert_eq!(ids(&d, Bound::Unbounded), existing);
            assert!(d.find(new).is_none());
            release_tx.send(()).unwrap();
            job.join().unwrap();
            let mut retained = Vec::new();
            while let Some((id, _)) = old.next() {
                retained.push(id);
            }
            assert!(existing.iter().all(|id| retained.contains(id)));
            assert!(retained.windows(2).all(|w| w[0] < w[1]));
            let mut all = existing.clone();
            all.push(new);
            all.sort_unstable();
            assert_eq!(ids(&d, Bound::Unbounded), all);
            let count = all.len();
            drop(d);
            assert_eq!(drops.load(Ordering::Relaxed), count);
        }
    }
}
