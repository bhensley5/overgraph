#[allow(dead_code)]
const STREAM_UNION_LINEAR_SCAN_MAX: usize = 8;

#[allow(dead_code)]
pub(crate) trait CandidateCursorSeek {
    fn next_ge(&mut self, bound: u64) -> Result<Option<u64>, EngineError>;

    fn append_chunk(
        &mut self,
        mut bound: u64,
        upper_exclusive: Option<u64>,
        max: usize,
        out: &mut Vec<u64>,
    ) -> Result<(), EngineError> {
        let start_len = out.len();
        while out.len().saturating_sub(start_len) < max {
            let Some(candidate) = self.next_ge(bound)? else {
                break;
            };
            if upper_exclusive.is_some_and(|upper| candidate >= upper) {
                break;
            }
            out.push(candidate);
            if candidate == u64::MAX {
                break;
            }
            bound = candidate + 1;
        }
        Ok(())
    }
}

#[allow(dead_code)]
pub(crate) struct StreamSet<C> {
    cursors: Vec<C>,
    heap: Option<BinaryHeap<Reverse<(u64, usize)>>>,
    initialized: bool,
    held: Option<u64>,
    last_seek_bound: Option<u64>,
    exhausted: bool,
}

#[allow(dead_code)]
impl<C: CandidateCursorSeek> StreamSet<C> {
    pub(crate) fn new(cursors: Vec<C>) -> Self {
        let heap = (cursors.len() > STREAM_UNION_LINEAR_SCAN_MAX)
            .then(|| BinaryHeap::with_capacity(cursors.len()));
        Self {
            cursors,
            heap,
            initialized: false,
            held: None,
            last_seek_bound: None,
            exhausted: false,
        }
    }

    pub(crate) fn next_ge(&mut self, bound: u64) -> Result<Option<u64>, EngineError> {
        <Self as CandidateCursorSeek>::next_ge(self, bound)
    }

    pub(crate) fn append_chunk(
        &mut self,
        bound: u64,
        upper_exclusive: Option<u64>,
        max: usize,
        out: &mut Vec<u64>,
    ) -> Result<(), EngineError> {
        if max == 0 {
            return Ok(());
        }
        if self.heap.is_some() {
            self.append_chunk_heap(bound, upper_exclusive, max, out)
        } else {
            self.append_chunk_linear(bound, upper_exclusive, max, out)
        }
    }

    fn append_chunk_linear(
        &mut self,
        mut bound: u64,
        upper_exclusive: Option<u64>,
        max: usize,
        out: &mut Vec<u64>,
    ) -> Result<(), EngineError> {
        let start_len = out.len();
        let mut heads = Vec::with_capacity(self.cursors.len());
        for cursor in &mut self.cursors {
            heads.push(cursor.next_ge(bound)?);
        }
        while out.len().saturating_sub(start_len) < max {
            let mut minimum = None;
            let mut minimum_index = 0usize;
            let mut minimum_count = 0usize;
            for (index, head) in heads.iter().copied().enumerate() {
                let Some(head) = head else {
                    continue;
                };
                match minimum {
                    None => {
                        minimum = Some(head);
                        minimum_index = index;
                        minimum_count = 1;
                    }
                    Some(current) if head < current => {
                        minimum = Some(head);
                        minimum_index = index;
                        minimum_count = 1;
                    }
                    Some(current) if head == current => minimum_count += 1,
                    Some(_) => {}
                }
            }
            let Some(minimum) = minimum else {
                self.held = None;
                self.exhausted = true;
                break;
            };
            if upper_exclusive.is_some_and(|upper| minimum >= upper) {
                self.held = Some(minimum);
                self.exhausted = false;
                break;
            }
            let remaining = max.saturating_sub(out.len().saturating_sub(start_len));
            if minimum_count > 1 {
                out.push(minimum);
                if minimum == u64::MAX {
                    for head in &mut heads {
                        if *head == Some(minimum) {
                            *head = None;
                        }
                    }
                    break;
                }
                let next_bound = minimum + 1;
                for (index, head) in heads.iter_mut().enumerate() {
                    if *head == Some(minimum) {
                        *head = self.cursors[index].next_ge(next_bound)?;
                    }
                }
            } else {
                let second = heads
                    .iter()
                    .copied()
                    .flatten()
                    .filter(|&head| head > minimum)
                    .min();
                let before = out.len();
                let child_upper = match (second, upper_exclusive) {
                    (Some(second), Some(upper)) => Some(second.min(upper)),
                    (Some(second), None) => Some(second),
                    (None, upper) => upper,
                };
                self.cursors[minimum_index].append_chunk(
                    minimum,
                    child_upper,
                    remaining,
                    out,
                )?;
                debug_assert!(out.len() > before, "minimum cursor must append its head");
            }
            let last = *out.last().expect("stream chunk must append its minimum");
            self.held = Some(last);
            self.exhausted = false;
            if last == u64::MAX {
                heads[minimum_index] = None;
                break;
            }
            bound = last + 1;
            if minimum_count == 1 {
                heads[minimum_index] = self.cursors[minimum_index].next_ge(bound)?;
            }
        }
        self.initialized = false;
        self.last_seek_bound = Some(bound);
        Ok(())
    }

    #[inline(never)]
    fn append_chunk_heap(
        &mut self,
        mut bound: u64,
        upper_exclusive: Option<u64>,
        max: usize,
        out: &mut Vec<u64>,
    ) -> Result<(), EngineError> {
        let start_len = out.len();
        if !self.initialized {
            let mut initial_heads = Vec::with_capacity(self.cursors.len());
            for (index, cursor) in self.cursors.iter_mut().enumerate() {
                if let Some(head) = cursor.next_ge(bound)? {
                    debug_assert!(head >= bound);
                    initial_heads.push(Reverse((head, index)));
                }
            }
            let heap = self.heap.as_mut().expect("heap path requires heap storage");
            heap.clear();
            heap.extend(initial_heads);
            self.initialized = true;
        } else {
            loop {
                let stale = self
                    .heap
                    .as_ref()
                    .and_then(|heap| heap.peek().copied())
                    .filter(|Reverse((head, _))| *head < bound);
                let Some(Reverse((_, index))) = stale else {
                    break;
                };
                let _ = self
                    .heap
                    .as_mut()
                    .expect("heap path requires heap storage")
                    .pop();
                if let Some(next) = self.cursors[index].next_ge(bound)? {
                    debug_assert!(next >= bound);
                    self.heap
                        .as_mut()
                        .expect("heap path requires heap storage")
                        .push(Reverse((next, index)));
                }
            }
        }

        let mut tied = Vec::with_capacity(self.cursors.len());
        while out.len().saturating_sub(start_len) < max {
            let minimum = self
                .heap
                .as_ref()
                .and_then(|heap| heap.peek().copied())
                .map(|Reverse((head, _))| head);
            let Some(minimum) = minimum else {
                if out.len() == start_len {
                    self.held = None;
                }
                self.exhausted = true;
                break;
            };
            if upper_exclusive.is_some_and(|upper| minimum >= upper) {
                self.held = Some(minimum);
                self.exhausted = false;
                break;
            }

            tied.clear();
            loop {
                let entry = self
                    .heap
                    .as_ref()
                    .and_then(|heap| heap.peek().copied())
                    .filter(|Reverse((head, _))| *head == minimum);
                let Some(entry) = entry else {
                    break;
                };
                let popped = self
                    .heap
                    .as_mut()
                    .expect("heap path requires heap storage")
                    .pop();
                debug_assert_eq!(popped, Some(entry));
                tied.push(entry);
            }

            let remaining = max.saturating_sub(out.len().saturating_sub(start_len));
            if tied.len() > 1 {
                out.push(minimum);
                if minimum == u64::MAX {
                    self.heap
                        .as_mut()
                        .expect("heap path requires heap storage")
                        .extend(tied.iter().copied());
                    self.held = Some(minimum);
                    self.exhausted = false;
                    break;
                }
                let next_bound = minimum + 1;
                for Reverse((_, index)) in tied.iter().copied() {
                    if let Some(next) = self.cursors[index].next_ge(next_bound)? {
                        debug_assert!(next >= next_bound);
                        self.heap
                            .as_mut()
                            .expect("heap path requires heap storage")
                            .push(Reverse((next, index)));
                    }
                }
            } else {
                let Reverse((_, index)) = tied[0];
                let next_other = self
                    .heap
                    .as_ref()
                    .and_then(|heap| heap.peek().copied())
                    .map(|Reverse((head, _))| head);
                let child_upper = match (next_other, upper_exclusive) {
                    (Some(next), Some(upper)) => Some(next.min(upper)),
                    (Some(next), None) => Some(next),
                    (None, upper) => upper,
                };
                let before = out.len();
                self.cursors[index].append_chunk(
                    minimum,
                    child_upper,
                    remaining,
                    out,
                )?;
                debug_assert!(out.len() > before, "minimum cursor must append its head");
                let emitted = out.len().saturating_sub(before);
                let last = *out.last().expect("heap union must append its minimum");
                if emitted == remaining {
                    let held = self.cursors[index].next_ge(last)?;
                    debug_assert_eq!(held, Some(last));
                    if let Some(held) = held {
                        self.heap
                            .as_mut()
                            .expect("heap path requires heap storage")
                            .push(Reverse((held, index)));
                    }
                } else if let Some(resume_bound) = child_upper {
                    if let Some(next) = self.cursors[index].next_ge(resume_bound)? {
                        debug_assert!(next >= resume_bound);
                        self.heap
                            .as_mut()
                            .expect("heap path requires heap storage")
                            .push(Reverse((next, index)));
                    }
                }
            }

            let last = *out.last().expect("stream chunk must append its minimum");
            self.held = Some(last);
            self.exhausted = false;
            if last == u64::MAX {
                break;
            }
            bound = last + 1;
        }

        if self
            .heap
            .as_ref()
            .is_some_and(std::collections::BinaryHeap::is_empty)
        {
            self.exhausted = true;
        }
        self.last_seek_bound = Some(bound);
        Ok(())
    }

    fn next_ge_linear(&mut self, bound: u64) -> Result<Option<u64>, EngineError> {
        let mut min_head = None;
        for cursor in &mut self.cursors {
            if let Some(head) = cursor.next_ge(bound)? {
                debug_assert!(head >= bound);
                min_head = Some(min_head.map_or(head, |current: u64| current.min(head)));
            }
        }
        Ok(min_head)
    }

    fn next_ge_heap(&mut self, bound: u64) -> Result<Option<u64>, EngineError> {
        let heap = self.heap.as_mut().expect("heap path requires heap storage");
        if !self.initialized {
            heap.clear();
            for (index, cursor) in self.cursors.iter_mut().enumerate() {
                if let Some(head) = cursor.next_ge(bound)? {
                    debug_assert!(head >= bound);
                    heap.push(Reverse((head, index)));
                }
            }
            self.initialized = true;
        } else {
            while let Some(Reverse((head, index))) = heap.peek().copied() {
                if head >= bound {
                    break;
                }
                let _ = heap.pop();
                if let Some(next) = self.cursors[index].next_ge(bound)? {
                    debug_assert!(next >= bound);
                    heap.push(Reverse((next, index)));
                }
            }
        }

        Ok(heap.peek().map(|entry| {
            let Reverse((head, _)) = *entry;
            head
        }))
    }

    pub(crate) fn into_cursors(self) -> Vec<C> {
        self.cursors
    }
}

impl<C: CandidateCursorSeek> CandidateCursorSeek for StreamSet<C> {
    fn next_ge(&mut self, bound: u64) -> Result<Option<u64>, EngineError> {
        if let Some(head) = self.held {
            if bound <= head {
                return Ok(Some(head));
            }
        }
        assert_monotonic_bound(self.last_seek_bound, self.held, bound);
        self.last_seek_bound = Some(bound);
        if self.exhausted {
            return Ok(None);
        }

        let next = if self.heap.is_some() {
            self.next_ge_heap(bound)?
        } else {
            self.next_ge_linear(bound)?
        };
        self.held = next;
        self.exhausted = next.is_none();
        Ok(next)
    }
}

#[allow(dead_code)]
pub(crate) struct LeapfrogIntersection<C> {
    streams: Vec<StreamSet<C>>,
    held: Option<u64>,
    last_seek_bound: Option<u64>,
    exhausted: bool,
}

#[allow(dead_code)]
impl<C: CandidateCursorSeek> LeapfrogIntersection<C> {
    pub(crate) fn new(streams: Vec<StreamSet<C>>) -> Self {
        debug_assert!(
            !streams.is_empty(),
            "leapfrog intersection requires at least one stream"
        );
        Self {
            streams,
            held: None,
            last_seek_bound: None,
            exhausted: false,
        }
    }

    pub(crate) fn next_ge(&mut self, bound: u64) -> Result<Option<u64>, EngineError> {
        <Self as CandidateCursorSeek>::next_ge(self, bound)
    }

    pub(crate) fn append_chunk(
        &mut self,
        mut bound: u64,
        upper_exclusive: Option<u64>,
        max: usize,
        out: &mut Vec<u64>,
    ) -> Result<(), EngineError> {
        if max == 0 {
            return Ok(());
        }
        let start_len = out.len();
        while out.len().saturating_sub(start_len) < max {
            let Some(candidate) = self.next_ge(bound)? else {
                break;
            };
            if upper_exclusive.is_some_and(|upper| candidate >= upper) {
                break;
            }
            out.push(candidate);
            if candidate == u64::MAX {
                break;
            }
            bound = candidate + 1;
        }
        Ok(())
    }
}

impl<C: CandidateCursorSeek> CandidateCursorSeek for LeapfrogIntersection<C> {
    fn next_ge(&mut self, bound: u64) -> Result<Option<u64>, EngineError> {
        if let Some(head) = self.held {
            if bound <= head {
                return Ok(Some(head));
            }
        }
        assert_monotonic_bound(self.last_seek_bound, self.held, bound);
        self.last_seek_bound = Some(bound);
        if self.exhausted || self.streams.is_empty() {
            self.exhausted = true;
            return Ok(None);
        }

        let mut retry_bound = bound;
        'outer: loop {
            let Some(target) = self.streams[0].next_ge(retry_bound)? else {
                self.held = None;
                self.exhausted = true;
                return Ok(None);
            };
            for index in 1..self.streams.len() {
                let Some(got) = self.streams[index].next_ge(target)? else {
                    self.held = None;
                    self.exhausted = true;
                    return Ok(None);
                };
                if got > target {
                    retry_bound = got;
                    continue 'outer;
                }
            }
            self.held = Some(target);
            return Ok(Some(target));
        }
    }
}

#[allow(dead_code)]
fn assert_monotonic_bound(last_seek_bound: Option<u64>, held: Option<u64>, bound: u64) {
    if held.is_some_and(|head| bound <= head) {
        return;
    }
    if let Some(previous) = last_seek_bound {
        debug_assert!(
            bound >= previous,
            "next_ge bounds must be non-decreasing: previous={previous}; held={held:?}; bound={bound}"
        );
    }
}

#[allow(dead_code)]
fn seek_ordered_id_posting_ge<F>(
    mut id_at: F,
    from_pos: usize,
    bound: u64,
) -> Result<Option<(usize, u64)>, EngineError>
where
    F: FnMut(usize) -> Result<Option<u64>, EngineError>,
{
    let Some(from_id) = id_at(from_pos)? else {
        return Ok(None);
    };
    if from_id >= bound {
        return Ok(Some((from_pos, from_id)));
    }

    let mut previous_probe_id = from_id;
    let mut step = 1usize;
    let mut lo = from_pos.saturating_add(1);
    let mut hi = None;
    while let Some(probe) = from_pos.checked_add(step) {
        match id_at(probe)? {
            Some(id) => {
                debug_assert!(
                    id > previous_probe_id,
                    "ordered ID postings must be strictly increasing for seek"
                );
                previous_probe_id = id;
                if id >= bound {
                    hi = Some(probe.saturating_add(1));
                    break;
                }
                lo = probe.saturating_add(1);
            }
            None => {
                hi = Some(probe);
                break;
            }
        }
        step = match step.checked_mul(2) {
            Some(next) => next,
            None => {
                hi = Some(usize::MAX);
                break;
            }
        };
    }

    let mut hi = hi.unwrap_or(usize::MAX);
    while lo < hi {
        let mid = lo + (hi - lo) / 2;
        match id_at(mid)? {
            Some(id) if id < bound => lo = mid + 1,
            Some(_) | None => hi = mid,
        }
    }

    Ok(id_at(lo)?.map(|id| (lo, id)))
}

#[cfg(test)]
mod stream_core_tests {
    use super::*;

    #[derive(Debug)]
    struct TestCursor {
        ids: Vec<u64>,
        pos: usize,
        held: Option<u64>,
        last_seek_bound: Option<u64>,
        exhausted: bool,
    }

    impl TestCursor {
        fn new(ids: &[u64]) -> Self {
            debug_assert!(ids.windows(2).all(|window| window[0] < window[1]));
            Self {
                ids: ids.to_vec(),
                pos: 0,
                held: None,
                last_seek_bound: None,
                exhausted: false,
            }
        }
    }

    impl CandidateCursorSeek for TestCursor {
        fn next_ge(&mut self, bound: u64) -> Result<Option<u64>, EngineError> {
            if let Some(head) = self.held {
                if bound <= head {
                    return Ok(Some(head));
                }
            }
            assert_monotonic_bound(self.last_seek_bound, self.held, bound);
            self.last_seek_bound = Some(bound);
            if self.exhausted {
                return Ok(None);
            }
            let start = if self.held.is_some() {
                self.pos.saturating_add(1)
            } else {
                self.pos
            };
            let relative = self.ids[start..].partition_point(|&id| id < bound);
            let next_pos = start + relative;
            if next_pos >= self.ids.len() {
                self.held = None;
                self.exhausted = true;
                return Ok(None);
            }
            self.pos = next_pos;
            self.held = Some(self.ids[next_pos]);
            Ok(self.held)
        }
    }

    fn cursor(ids: &[u64]) -> TestCursor {
        TestCursor::new(ids)
    }

    fn leaf(ids: &[u64]) -> StreamSet<TestCursor> {
        StreamSet::new(vec![cursor(ids)])
    }

    fn collect_stream_set(mut stream: StreamSet<TestCursor>) -> Vec<u64> {
        let mut out = Vec::new();
        let mut bound = 0u64;
        while let Some(id) = stream.next_ge(bound).unwrap() {
            out.push(id);
            if id == u64::MAX {
                break;
            }
            bound = id + 1;
        }
        out
    }

    fn collect_intersection(mut stream: LeapfrogIntersection<TestCursor>) -> Vec<u64> {
        let mut out = Vec::new();
        let mut bound = 0u64;
        while let Some(id) = stream.next_ge(bound).unwrap() {
            out.push(id);
            if id == u64::MAX {
                break;
            }
            bound = id + 1;
        }
        out
    }

    fn collect_stream_set_chunked(
        mut stream: StreamSet<TestCursor>,
        chunk_size: usize,
    ) -> Vec<u64> {
        let mut out = Vec::new();
        let mut bound = 0u64;
        loop {
            let before = out.len();
            stream
                .append_chunk(bound, None, chunk_size, &mut out)
                .unwrap();
            if out.len() == before {
                break;
            }
            let last = *out.last().unwrap();
            if last == u64::MAX {
                break;
            }
            bound = last + 1;
        }
        out
    }

    fn collect_intersection_chunked(
        mut stream: LeapfrogIntersection<TestCursor>,
        chunk_size: usize,
    ) -> Vec<u64> {
        let mut out = Vec::new();
        let mut bound = 0u64;
        loop {
            let before = out.len();
            stream
                .append_chunk(bound, None, chunk_size, &mut out)
                .unwrap();
            if out.len() == before {
                break;
            }
            let last = *out.last().unwrap();
            if last == u64::MAX {
                break;
            }
            bound = last + 1;
        }
        out
    }

    #[test]
    fn stream_core_sorted_buffer_cursor_contract() {
        let mut empty = cursor(&[]);
        assert_eq!(empty.next_ge(0).unwrap(), None);
        assert_eq!(empty.next_ge(u64::MAX).unwrap(), None);

        let mut single = cursor(&[10]);
        assert_eq!(single.next_ge(0).unwrap(), Some(10));
        assert_eq!(single.next_ge(0).unwrap(), Some(10));
        assert_eq!(single.next_ge(10).unwrap(), Some(10));
        assert_eq!(single.next_ge(11).unwrap(), None);

        let mut cursor = cursor(&[0, 5, 10, 20, u64::MAX]);
        assert_eq!(cursor.next_ge(0).unwrap(), Some(0));
        assert_eq!(cursor.next_ge(1).unwrap(), Some(5));
        assert_eq!(cursor.next_ge(5).unwrap(), Some(5));
        assert_eq!(cursor.next_ge(6).unwrap(), Some(10));
        assert_eq!(cursor.next_ge(20).unwrap(), Some(20));
        assert_eq!(cursor.next_ge(21).unwrap(), Some(u64::MAX));
        assert_eq!(cursor.next_ge(u64::MAX).unwrap(), Some(u64::MAX));
    }

    #[test]
    fn stream_core_union_linear_path_dedups_and_seeks_through() {
        let stream = StreamSet::new(vec![
            cursor(&[1, 4, 7]),
            cursor(&[2, 4, 8]),
            cursor(&[4, 9]),
        ]);

        assert_eq!(collect_stream_set(stream), vec![1, 2, 4, 7, 8, 9]);
    }

    #[test]
    fn stream_core_union_heap_path_dedups_and_seeks_through() {
        let mut cursors = Vec::new();
        for i in 0..9 {
            cursors.push(cursor(&[i, 10, 20 + i]));
        }
        let mut stream = StreamSet::new(cursors);

        assert_eq!(stream.next_ge(0).unwrap(), Some(0));
        assert_eq!(stream.next_ge(5).unwrap(), Some(5));
        assert_eq!(stream.next_ge(10).unwrap(), Some(10));
        assert_eq!(stream.next_ge(10).unwrap(), Some(10));
        assert_eq!(stream.next_ge(11).unwrap(), Some(20));
        assert_eq!(stream.next_ge(30).unwrap(), None);
    }

    #[test]
    fn stream_core_bulk_fill_matches_seek_for_linear_heap_intersection_and_max() {
        for chunk_size in [1, 2, 255, 256] {
            let linear_inputs = [
                vec![1, 4, 7, u64::MAX],
                vec![2, 4, 8, u64::MAX],
                vec![4, 9, u64::MAX],
            ];
            let expected = collect_stream_set(StreamSet::new(
                linear_inputs.iter().map(|ids| cursor(ids)).collect(),
            ));
            let actual = collect_stream_set_chunked(
                StreamSet::new(linear_inputs.iter().map(|ids| cursor(ids)).collect()),
                chunk_size,
            );
            assert_eq!(actual, expected, "linear chunk_size={chunk_size}");

            let heap_inputs = (0..9u64)
                .map(|index| vec![index, 10, 20 + index, u64::MAX])
                .collect::<Vec<_>>();
            let expected = collect_stream_set(StreamSet::new(
                heap_inputs.iter().map(|ids| cursor(ids)).collect(),
            ));
            let actual = collect_stream_set_chunked(
                StreamSet::new(heap_inputs.iter().map(|ids| cursor(ids)).collect()),
                chunk_size,
            );
            assert_eq!(actual, expected, "heap chunk_size={chunk_size}");

            let expected = collect_intersection(LeapfrogIntersection::new(vec![
                leaf(&[1, 3, 5, 8, u64::MAX]),
                leaf(&[2, 3, 8, u64::MAX]),
                leaf(&[3, 7, 8, u64::MAX]),
            ]));
            let actual = collect_intersection_chunked(
                LeapfrogIntersection::new(vec![
                    leaf(&[1, 3, 5, 8, u64::MAX]),
                    leaf(&[2, 3, 8, u64::MAX]),
                    leaf(&[3, 7, 8, u64::MAX]),
                ]),
                chunk_size,
            );
            assert_eq!(actual, expected, "intersection chunk_size={chunk_size}");
        }
    }

    #[test]
    fn stream_core_heap_bulk_fill_retains_heap_for_fully_interleaved_inputs() {
        let inputs = (0..64u64)
            .map(|offset| {
                (0..64u64)
                    .map(|round| round * 64 + offset)
                    .collect::<Vec<_>>()
            })
            .collect::<Vec<_>>();
        let mut stream = StreamSet::new(inputs.iter().map(|ids| cursor(ids)).collect());
        let mut out = Vec::new();
        let mut bound = 0u64;
        while out.len() < 4096 {
            let before = out.len();
            stream.append_chunk(bound, None, 257, &mut out).unwrap();
            assert!(out.len() > before);
            if out.len() < 4096 {
                assert!(stream.initialized, "heap fill must retain initialized state");
                assert_eq!(
                    stream.heap.as_ref().map(BinaryHeap::len),
                    Some(64),
                    "fully interleaved continuation must retain one heap head per cursor"
                );
            }
            bound = out.last().copied().unwrap() + 1;
        }
        assert_eq!(out, (0..4096u64).collect::<Vec<_>>());
    }

    #[test]
    fn stream_core_leapfrog_handles_k_one_two_three_and_sixteen() {
        assert_eq!(
            collect_intersection(LeapfrogIntersection::new(vec![leaf(&[1, 3, 5])])),
            vec![1, 3, 5]
        );
        assert_eq!(
            collect_intersection(LeapfrogIntersection::new(vec![
                leaf(&[1, 3, 5, 8]),
                leaf(&[2, 3, 8, 13]),
            ])),
            vec![3, 8]
        );
        assert_eq!(
            collect_intersection(LeapfrogIntersection::new(vec![
                leaf(&[1, 5, 9, 13]),
                leaf(&[3, 5, 11, 13]),
                leaf(&[5, 7, 13]),
            ])),
            vec![5, 13]
        );

        let mut many = Vec::new();
        for i in 0..16 {
            many.push(leaf(&[i, 50, 100 + i]));
        }
        assert_eq!(
            collect_intersection(LeapfrogIntersection::new(many)),
            vec![50]
        );
    }

    #[test]
    fn stream_core_leapfrog_handles_empty_single_head_and_tail_boundaries() {
        assert_eq!(
            collect_intersection(LeapfrogIntersection::new(vec![leaf(&[1, 2, 3]), leaf(&[])])),
            Vec::<u64>::new()
        );
        assert_eq!(
            collect_intersection(LeapfrogIntersection::new(vec![
                leaf(&[7]),
                leaf(&[1, 7, 9]),
                leaf(&[7, 10]),
            ])),
            vec![7]
        );
        assert_eq!(
            collect_intersection(LeapfrogIntersection::new(vec![
                leaf(&[1, 3, 9]),
                leaf(&[1, 2, 9]),
            ])),
            vec![1, 9]
        );
    }

    #[test]
    fn stream_core_leapfrog_retry_reobserves_held_head() {
        let mut stream = LeapfrogIntersection::new(vec![leaf(&[5, 10]), leaf(&[10])]);
        assert_eq!(stream.next_ge(0).unwrap(), Some(10));
        assert_eq!(stream.next_ge(10).unwrap(), Some(10));
        assert_eq!(stream.next_ge(11).unwrap(), None);
    }

    #[test]
    fn stream_core_ordered_id_posting_seek_matches_binary_search_oracle() {
        let ids: Vec<u64> = (0..128u64)
            .scan(0u64, |next, n| {
                *next += 1 + (n % 7);
                Some(*next)
            })
            .collect();
        let mut seed = 0x9e37_79b9_7f4a_7c15u64;
        let mut from_pos = 0usize;
        for _ in 0..512 {
            seed = seed.wrapping_mul(6_364_136_223_846_793_005).wrapping_add(1);
            let bound = seed % 900;
            let expected = ids[from_pos..]
                .binary_search(&bound)
                .map(|relative| from_pos + relative)
                .unwrap_or_else(|relative| from_pos + relative);
            let got = seek_ordered_id_posting_ge(
                |index| Ok(ids.get(index).copied()),
                from_pos,
                bound,
            )
            .unwrap();
            if expected >= ids.len() {
                assert_eq!(got, None, "from_pos {from_pos}, bound {bound}");
                from_pos = ids.len();
            } else {
                assert_eq!(
                    got,
                    Some((expected, ids[expected])),
                    "from_pos {from_pos}, bound {bound}"
                );
                from_pos = expected;
        }
    }
    }

    #[test]
    fn stream_core_bulk_fill_honors_upper_bound_without_overread_and_zero_is_noop() {
        let mut zero = StreamSet::new(vec![cursor(&[1, 3, 5])]);
        let mut out = Vec::new();
        zero.append_chunk(0, Some(4), 0, &mut out).unwrap();
        assert!(out.is_empty());
        let cursors = zero.into_cursors();
        assert_eq!(cursors[0].held, None);
        assert_eq!(cursors[0].last_seek_bound, None);

        #[derive(Debug)]
        struct FailAfterUpper {
            inner: TestCursor,
            upper: u64,
        }

        impl CandidateCursorSeek for FailAfterUpper {
            fn next_ge(&mut self, bound: u64) -> Result<Option<u64>, EngineError> {
                if bound > self.upper {
                    return Err(EngineError::InvalidOperation(
                        "cursor advanced past the first out-of-range candidate".to_string(),
                    ));
                }
                self.inner.next_ge(bound)
            }
        }

        let mut bounded = StreamSet::new(vec![FailAfterUpper {
            inner: TestCursor::new(&[1, 3, 5, 6, 7]),
            upper: 5,
        }]);
        bounded.append_chunk(0, Some(5), 256, &mut out).unwrap();
        assert_eq!(out, [1, 3]);
        assert_eq!(bounded.next_ge(4).unwrap(), Some(5));
    }

    #[cfg(debug_assertions)]
    #[test]
    #[should_panic(expected = "next_ge bounds must be non-decreasing")]
    fn stream_core_debug_asserts_decreasing_bound_after_terminal_seek() {
        let mut cursor = cursor(&[10, 20]);
        assert_eq!(cursor.next_ge(10).unwrap(), Some(10));
        assert_eq!(cursor.next_ge(25).unwrap(), None);
        let _ = cursor.next_ge(24);
    }
}
