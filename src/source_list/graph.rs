//! Same-view primary verification of derived adjacency from older sources.
use super::*;
type AdjRow = (u64, NeighborRecord);
impl SourceList<'_> {
    pub(super) fn verify_edge_candidate_ids(
        &self,
        mut ids: Vec<u64>,
        filter: impl Fn(EdgeMetadataCandidate) -> bool,
    ) -> Result<Vec<u64>, EngineError> {
        ids.sort_unstable();
        ids.dedup();
        let mut result = Vec::with_capacity(ids.len());
        for chunk in ids.chunks(4096) {
            for (&id, opinion) in chunk
                .iter()
                .zip(self.find_edge_metadata_with_source(chunk)?)
            {
                if let EdgeMetadataSourceOpinion::Live { meta, .. } = opinion {
                    if filter(meta) {
                        result.push(id);
                    }
                }
            }
        }
        Ok(result)
    }

    fn memtable_graph_layer(&self, m: &Memtable) -> EdgeMetadataSourceLayer {
        if std::ptr::eq(m, self.active) {
            return EdgeMetadataSourceLayer::Active;
        }
        EdgeMetadataSourceLayer::Immutable(
            self.immutable
                .iter()
                .position(|e| std::ptr::eq(&*e.memtable, m))
                .expect("captured memtable"),
        )
    }
    fn segment_graph_layer(&self, m: &SegmentReader) -> EdgeMetadataSourceLayer {
        EdgeMetadataSourceLayer::Segment(
            self.segments
                .iter()
                .position(|s| std::ptr::eq(&**s, m))
                .expect("captured segment"),
        )
    }
    fn verify_adj_rows(
        &self,
        layer: EdgeMetadataSourceLayer,
        rows: &mut Vec<AdjRow>,
        direction: Direction,
        labels: Option<&[u32]>,
    ) -> Result<(), EngineError> {
        if layer == EdgeMetadataSourceLayer::Active {
            return Ok(());
        }
        let mut keep = Vec::with_capacity(rows.len());
        for chunk in rows.chunks(4096) {
            let ids = chunk.iter().map(|(_, e)| e.edge_id).collect::<Vec<_>>();
            let opinions = self.find_edge_metadata_with_source(&ids)?;
            for ((owner, entry), opinion) in chunk.iter().zip(opinions) {
                let EdgeMetadataSourceOpinion::Live {
                    layer: newest,
                    meta,
                } = opinion
                else {
                    continue;
                };
                if newest != layer || labels.is_some_and(|ls| !ls.contains(&meta.label_id)) {
                    continue;
                }
                let matches = match direction {
                    Direction::Outgoing => meta.from == *owner && meta.to == entry.node_id,
                    Direction::Incoming => meta.to == *owner && meta.from == entry.node_id,
                    Direction::Both => {
                        (meta.from == *owner && meta.to == entry.node_id)
                            || (meta.to == *owner && meta.from == entry.node_id)
                    }
                };
                if matches {
                    keep.push((
                        *owner,
                        NeighborRecord {
                            node_id: entry.node_id,
                            edge_id: meta.edge_id,
                            edge_label_id: meta.label_id,
                            weight: meta.weight,
                            valid_from: meta.valid_from,
                            valid_to: meta.valid_to,
                        },
                    ));
                }
            }
        }
        *rows = keep;
        Ok(())
    }
    pub(crate) fn graph_neighbors_memtable(
        &self,
        m: &Memtable,
        owner: u64,
        direction: Direction,
        labels: Option<&[u32]>,
        limit: usize,
    ) -> Result<Vec<NeighborRecord>, EngineError> {
        if std::ptr::eq(m, self.active) {
            return m.neighbors_at(owner, direction, labels, limit, self.snapshot_seq);
        }

        let mut rows = m
            .neighbors_at(owner, direction, labels, 0, self.snapshot_seq)?
            .into_iter()
            .map(|e| (owner, e))
            .collect();
        self.verify_adj_rows(self.memtable_graph_layer(m), &mut rows, direction, labels)?;
        let mut out = rows.into_iter().map(|(_, e)| e).collect::<Vec<_>>();
        if limit > 0 {
            out.truncate(limit)
        }
        Ok(out)
    }
    pub(crate) fn graph_neighbors_segment(
        &self,
        m: &SegmentReader,
        owner: u64,
        direction: Direction,
        labels: Option<&[u32]>,
        limit: usize,
    ) -> Result<Vec<NeighborRecord>, EngineError> {
        let mut rows = m
            .neighbors(owner, direction, labels, 0)?
            .into_iter()
            .map(|e| (owner, e))
            .collect();
        self.verify_adj_rows(self.segment_graph_layer(m), &mut rows, direction, labels)?;
        let mut out = rows.into_iter().map(|(_, e)| e).collect::<Vec<_>>();
        if limit > 0 {
            out.truncate(limit)
        }
        Ok(out)
    }
    pub(crate) fn graph_neighbors_batch_memtable(
        &self,
        m: &Memtable,
        owners: &[u64],
        direction: Direction,
        labels: Option<&[u32]>,
    ) -> Result<NodeIdMap<Vec<NeighborRecord>>, EngineError> {
        let batch = m.neighbors_batch_at(owners, direction, labels, self.snapshot_seq)?;
        if std::ptr::eq(m, self.active) {
            return Ok(batch);
        }
        let mut rows = batch
            .into_iter()
            .flat_map(|(owner, es)| es.into_iter().map(move |e| (owner, e)))
            .collect();
        self.verify_adj_rows(self.memtable_graph_layer(m), &mut rows, direction, labels)?;
        let mut out = NodeIdMap::default();
        for (owner, e) in rows {
            out.entry(owner).or_insert_with(Vec::new).push(e);
        }
        Ok(out)
    }
    pub(crate) fn graph_neighbors_batch_segment(
        &self,
        m: &SegmentReader,
        owners: &[u64],
        direction: Direction,
        labels: Option<&[u32]>,
    ) -> Result<NodeIdMap<Vec<NeighborRecord>>, EngineError> {
        let batch = m.neighbors_batch(owners, direction, labels)?;
        let mut rows = batch
            .into_iter()
            .flat_map(|(owner, es)| es.into_iter().map(move |e| (owner, e)))
            .collect();
        self.verify_adj_rows(self.segment_graph_layer(m), &mut rows, direction, labels)?;
        let mut out = NodeIdMap::default();
        for (owner, e) in rows {
            out.entry(owner).or_insert_with(Vec::new).push(e);
        }
        Ok(out)
    }
    fn graph_adj_walk(
        &self,
        layer: EdgeMetadataSourceLayer,
        direction: Direction,
        labels: Option<&[u32]>,
        walk: impl FnOnce(
            &mut dyn FnMut(u64, u64, u64, f32, i64, i64) -> ControlFlow<()>,
        ) -> Result<ControlFlow<()>, EngineError>,
        mut output: impl FnMut(u64, u64, u64, f32, i64, i64) -> ControlFlow<()>,
    ) -> Result<ControlFlow<()>, EngineError> {
        if layer == EdgeMetadataSourceLayer::Active {
            return walk(&mut output);
        }
        let mut rows = Vec::with_capacity(4096);
        let mut error = None;
        let mut flush = |rows: &mut Vec<AdjRow>| -> Result<ControlFlow<()>, EngineError> {
            self.verify_adj_rows(layer, rows, direction, labels)?;
            for (owner, e) in rows.drain(..) {
                if output(
                    owner,
                    e.edge_id,
                    e.node_id,
                    e.weight,
                    e.valid_from,
                    e.valid_to,
                )
                .is_break()
                {
                    return Ok(ControlFlow::Break(()));
                }
            }
            Ok(ControlFlow::Continue(()))
        };
        let stopped = walk(&mut |owner, id, neighbor, weight, from, to| {
            rows.push((
                owner,
                NeighborRecord {
                    node_id: neighbor,
                    edge_id: id,
                    edge_label_id: 0,
                    weight,
                    valid_from: from,
                    valid_to: to,
                },
            ));
            if rows.len() < 4096 {
                return ControlFlow::Continue(());
            }
            match flush(&mut rows) {
                Ok(flow) => flow,
                Err(e) => {
                    error = Some(e);
                    ControlFlow::Break(())
                }
            }
        })?;
        if let Some(e) = error {
            return Err(e);
        }
        if stopped.is_break() {
            return Ok(stopped);
        }
        flush(&mut rows)
    }
    pub(crate) fn graph_adj_memtable<F>(
        &self,
        m: &Memtable,
        owner: u64,
        direction: Direction,
        labels: Option<&[u32]>,
        callback: &mut F,
    ) -> Result<ControlFlow<()>, EngineError>
    where
        F: FnMut(u64, u64, f32, i64, i64) -> ControlFlow<()>,
    {
        self.graph_adj_walk(
            self.memtable_graph_layer(m),
            direction,
            labels,
            |emit| {
                m.for_each_adj_entry_at(
                    owner,
                    direction,
                    labels,
                    self.snapshot_seq,
                    &mut |id, n, w, f, t| emit(owner, id, n, w, f, t),
                )
            },
            |_, id, n, w, f, t| callback(id, n, w, f, t),
        )
    }
    pub(crate) fn graph_adj_segment<F>(
        &self,
        m: &SegmentReader,
        owner: u64,
        direction: Direction,
        labels: Option<&[u32]>,
        callback: &mut F,
    ) -> Result<ControlFlow<()>, EngineError>
    where
        F: FnMut(u64, u64, f32, i64, i64) -> ControlFlow<()>,
    {
        self.graph_adj_walk(
            self.segment_graph_layer(m),
            direction,
            labels,
            |emit| {
                m.for_each_adj_posting(owner, direction, labels, &mut |id, n, w, f, t| {
                    emit(owner, id, n, w, f, t)
                })
            },
            |_, id, n, w, f, t| callback(id, n, w, f, t),
        )
    }
    pub(crate) fn graph_adj_segment_batch<F>(
        &self,
        m: &SegmentReader,
        owners: &[u64],
        direction: Direction,
        labels: Option<&[u32]>,
        callback: &mut F,
    ) -> Result<ControlFlow<()>, EngineError>
    where
        F: FnMut(u64, u64, u64, f32, i64, i64) -> ControlFlow<()>,
    {
        self.graph_adj_walk(
            self.segment_graph_layer(m),
            direction,
            labels,
            |emit| {
                m.for_each_adj_posting_batch(owners, direction, labels, &mut |o, id, n, w, f, t| {
                    emit(o, id, n, w, f, t)
                })
            },
            callback,
        )
    }
}
