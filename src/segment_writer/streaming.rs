//! Candidate-only sealed export. Full records live only in one owned refill and
//! one record-sized encoding/readback buffer. Builder metadata/vector state is
//! intentionally retained; it is not a total-memory bound.
use super::*;
use crate::engine::payload::Owned;
use crate::planner_stats::StreamingFlushStats;
use std::io::Read;

fn overflow() -> EngineError {
    EngineError::CorruptRecord("streaming record count/offset overflow".into())
}
fn table_size(count: u64) -> Result<u64, EngineError> {
    count
        .checked_mul(NODE_INDEX_ENTRY_SIZE)
        .and_then(|n| n.checked_add(8))
        .ok_or_else(overflow)
}

struct RecordExportWriter<'a> {
    writer: &'a mut BufWriter<File>,
    spans: RecordDataSpans,
    offset: u64,
    buffer: Vec<u8>,
    count: u64,
}
impl RecordExportWriter<'_> {
    fn append(
        &mut self,
        id: u64,
        encode: impl FnOnce(&mut Vec<u8>) -> Result<(), EngineError>,
    ) -> Result<RecordDataSpan, EngineError> {
        if self.spans.len() as u64 >= self.count || self.spans.last().is_some_and(|r| r.0 >= id) {
            return Err(EngineError::CorruptRecord(
                "sealed export count/order mismatch".into(),
            ));
        }
        encode(&mut self.buffer)?;
        let len = u32::try_from(self.buffer.len()).map_err(|_| overflow())?;
        let span = (id, self.offset, len);
        self.offset = self
            .offset
            .checked_add(u64::from(len))
            .ok_or_else(overflow)?;
        self.writer.write_all(&self.buffer)?;
        self.spans.push(span);
        Ok(span)
    }
}
impl PackedCoreWriter {
    /// Reserve the existing wire table, encode once, backpatch privately, then
    /// hash finalized bytes in wire order. Readback releases every decoded node
    /// before reading its successor and restores the append position.
    fn write_streamed_records<T>(
        &mut self,
        kind: SegmentComponentKind,
        count: u64,
        emit: impl FnOnce(&mut RecordExportWriter<'_>) -> Result<T, EngineError>,
        mut observe: impl FnMut(&mut T, u64, &[u8]) -> Result<(), EngineError>,
    ) -> Result<(SegmentComponentRecordV1, T), EngineError> {
        let fingerprint = match kind {
            SegmentComponentKind::NodeRecords => "flush.nodes",
            SegmentComponentKind::EdgeRecords => "flush.edges",
            _ => return Err(overflow()),
        };
        let table_len = table_size(count)?;
        let capacity = usize::try_from(count).map_err(|_| overflow())?;
        let mut spans = Vec::new();
        spans.try_reserve_exact(capacity).map_err(|_| overflow())?;
        self.align_next_component()?;
        let start = self.container_payload_len;
        let absolute = (COMPONENT_IDENTITY_HEADER_LEN as u64)
            .checked_add(start)
            .ok_or_else(overflow)?;
        absolute.checked_add(table_len).ok_or_else(overflow)?;
        write_u64(&mut self.writer, count)?;
        let zeros = [0u8; 64 * 1024];
        let mut left = table_len - 8;
        while left > 0 {
            let n = left.min(zeros.len() as u64) as usize;
            self.writer.write_all(&zeros[..n])?;
            left -= n as u64;
        }
        let mut export = RecordExportWriter {
            writer: &mut self.writer,
            spans,
            offset: table_len,
            buffer: Vec::new(),
            count,
        };
        let mut output = emit(&mut export)?;
        if export.spans.len() as u64 != count {
            return Err(EngineError::CorruptRecord(
                "sealed export reserved count mismatch".into(),
            ));
        }
        let len = export.offset;
        let end = absolute.checked_add(len).ok_or_else(overflow)?;
        let spans = export.spans;
        drop(export.buffer);
        self.writer.seek(SeekFrom::Start(absolute + 8))?;
        for &(id, offset, _) in &spans {
            write_u64(&mut self.writer, id)?;
            write_u64(&mut self.writer, offset)?;
        }
        self.writer.flush()?;
        // File::create is write-only. An independent read handle shares no seek
        // cursor with BufWriter, and is bounded to the finalized component.
        let mut reader = std::io::BufReader::with_capacity(64 * 1024, File::open(&self.tmp_path)?);
        reader.seek(SeekFrom::Start(absolute))?;
        let mut digest = Sha256::new();
        let mut scratch = vec![0u8; 64 * 1024];
        let mut remaining = table_len;
        while remaining > 0 {
            let n = remaining.min(scratch.len() as u64) as usize;
            reader.read_exact(&mut scratch[..n])?;
            digest.update(&scratch[..n]);
            self.container_digest.update(&scratch[..n]);
            remaining -= n as u64;
        }
        for &(id, _, record_len) in &spans {
            if kind == SegmentComponentKind::NodeRecords {
                scratch.resize(record_len as usize, 0);
                reader.read_exact(&mut scratch)?;
                digest.update(&scratch);
                self.container_digest.update(&scratch);
                observe(&mut output, id, &scratch)?;
            } else {
                let mut remaining = u64::from(record_len);
                while remaining > 0 {
                    let n = remaining.min(scratch.len() as u64) as usize;
                    reader.read_exact(&mut scratch[..n])?;
                    digest.update(&scratch[..n]);
                    self.container_digest.update(&scratch[..n]);
                    remaining -= n as u64;
                }
            }
        }
        self.writer.seek(SeekFrom::Start(end))?;
        self.container_payload_len = start.checked_add(len).ok_or_else(overflow)?;
        let payload_digest: [u8; 32] = digest.finalize().into();
        let dependencies = Vec::new();
        let dependency_digest = dependency_digest(&dependencies);
        let build_fingerprint = component_fingerprint(fingerprint, &[]);
        let record = SegmentComponentRecordV1 {
            component_id: component_id(
                self.segment_id,
                &kind,
                FLUSH_COMPONENT_LOGICAL_FORMAT_VERSION,
                len,
                Some(&payload_digest),
                &dependency_digest,
                build_fingerprint,
            ),
            kind,
            logical_format_version: FLUSH_COMPONENT_LOGICAL_FORMAT_VERSION,
            created_generation: self.generation,
            requirement: ComponentRequirement::Required,
            trust_class: ComponentTrustClass::PrimaryData,
            handle: ComponentHandleV1::PackedRange {
                container_component_id: ZERO_DIGEST,
                offset: start,
                len,
            },
            payload_len: len,
            payload_digest: Some(payload_digest),
            dependency_digest,
            dependencies,
            build_fingerprint,
        };
        self.records.push(record.clone());
        Ok((record, output))
    }
}

#[derive(Clone, Copy, Debug)]
pub(crate) struct SealedExport {
    pub seq: u64,
    pub nodes: usize,
    pub edges: usize,
    pub max_node: u64,
    pub max_edge: u64,
}
impl SealedExport {
    pub(crate) fn capture(memtable: &Memtable, seq: u64) -> Self {
        Self {
            seq,
            nodes: memtable.node_count(),
            edges: memtable.edge_count(),
            max_node: memtable.max_node_id(),
            max_edge: memtable.max_edge_id(),
        }
    }
}
struct NodeExport {
    metas: Vec<CompactNodeMeta>,
    keys: Vec<String>,
    vectors: NodeVectorSourcePlan,
    sparse: Vec<(u64, Vec<(u32, f32)>)>,
    deleted: NodeIdMap<TombstoneEntry>,
    stats: StreamingFlushStats,
    chunk_bytes: usize,
}
struct EdgeExport {
    metas: Vec<CompactEdgeMeta>,
    deleted: NodeIdMap<TombstoneEntry>,
    chunk_bytes: usize,
}

fn export_nodes(
    memtable: &Memtable,
    sealed: SealedExport,
    w: &mut RecordExportWriter<'_>,
    indexes: &[SecondaryIndexManifestEntry],
) -> Result<NodeExport, EngineError> {
    let mut output = NodeExport {
        metas: Vec::new(),
        keys: Vec::new(),
        vectors: NodeVectorSourcePlan {
            rows: Vec::new(),
            has_dense: false,
            has_sparse: false,
            dense_points: Vec::new(),
        },
        sparse: Vec::new(),
        deleted: NodeIdMap::default(),
        stats: StreamingFlushStats::new(indexes),
        chunk_bytes: 0,
    };
    let (mut after, mut dense_offset, mut sparse_offset) = (None, 0u64, 0u64);
    loop {
        let mut chunk = memtable.export_node_chunk(sealed.seq, (after, sealed.max_node))?;
        output.chunk_bytes = output.chunk_bytes.max(chunk.copied_bytes);
        for node in chunk.records.iter_mut() {
            let (_, offset, len) = w.append(node.id, |buf| encode_node_record_into(buf, node))?;
            output.stats.observe_candidates(node);
            let dense_len = u32::try_from(node.dense_vector.as_ref().map_or(0, Vec::len))
                .map_err(|_| overflow())?;
            let sparse_len = u32::try_from(node.sparse_vector.as_ref().map_or(0, Vec::len))
                .map_err(|_| overflow())?;
            output.metas.push(CompactNodeMeta {
                node_id: node.id,
                new_data_offset: offset,
                data_len: len,
                label_ids: node.label_ids,
                updated_at: node.updated_at,
                weight: node.weight,
                key_len: u16::try_from(node.key.len()).map_err(|_| overflow())?,
                dense_vector_offset: dense_offset,
                dense_vector_len: dense_len,
                sparse_vector_offset: sparse_offset,
                sparse_vector_len: sparse_len,
                src_seg_idx: 0,
                src_data_offset: offset,
                last_write_seq: node.last_write_seq,
            });
            output.keys.push(std::mem::take(&mut node.key));
            let mut flags = 0;
            if let Some(values) = node.dense_vector.take() {
                flags |= NODE_VECTOR_FLAG_DENSE;
                output.vectors.has_dense = true;
                output.vectors.dense_points.push(DensePointInput {
                    node_id: node.id,
                    dense_vector_offset: dense_offset,
                    values,
                });
            }
            if let Some(values) = node.sparse_vector.take() {
                flags |= NODE_VECTOR_FLAG_SPARSE;
                output.vectors.has_sparse = true;
                output.sparse.push((node.id, values));
            }
            output.vectors.rows.push(NodeVectorSourceRow {
                node_id: node.id,
                flags,
                dense_offset: if flags & NODE_VECTOR_FLAG_DENSE != 0 {
                    dense_offset
                } else {
                    0
                },
                dense_len,
                sparse_offset: if flags & NODE_VECTOR_FLAG_SPARSE != 0 {
                    sparse_offset
                } else {
                    0
                },
                sparse_len,
            });
            dense_offset = dense_offset
                .checked_add(u64::from(dense_len) * DENSE_VECTOR_VALUE_SIZE)
                .ok_or_else(overflow)?;
            sparse_offset = sparse_offset
                .checked_add(u64::from(sparse_len) * SPARSE_VECTOR_ENTRY_SIZE)
                .ok_or_else(overflow)?;
        }
        output.deleted.extend(chunk.tombstones);
        if chunk.exhausted {
            break;
        }
        if chunk.last <= after {
            return Err(EngineError::CorruptRecord(
                "sealed node export made no progress".into(),
            ));
        }
        after = chunk.last;
    }
    output.stats.select();
    Ok(output)
}
fn export_edges(
    memtable: &Memtable,
    sealed: SealedExport,
    w: &mut RecordExportWriter<'_>,
) -> Result<EdgeExport, EngineError> {
    let mut output = EdgeExport {
        metas: Vec::new(),
        deleted: NodeIdMap::default(),
        chunk_bytes: 0,
    };
    let mut after = None;
    loop {
        let chunk = memtable.export_edge_chunk(sealed.seq, (after, sealed.max_edge))?;
        output.chunk_bytes = output.chunk_bytes.max(chunk.copied_bytes);
        for edge in chunk.records.iter() {
            let (_, offset, len) = w.append(edge.id, |buf| encode_edge_record_into(buf, edge))?;
            output.metas.push(CompactEdgeMeta {
                edge_id: edge.id,
                new_data_offset: offset,
                data_len: len,
                from: edge.from,
                to: edge.to,
                label_id: edge.label_id,
                updated_at: edge.updated_at,
                weight: edge.weight,
                valid_from: edge.valid_from,
                valid_to: edge.valid_to,
                src_seg_idx: 0,
                src_data_offset: offset,
                last_write_seq: edge.last_write_seq,
            });
        }
        output.deleted.extend(chunk.tombstones);
        if chunk.exhausted {
            break;
        }
        if chunk.last <= after {
            return Err(EngineError::CorruptRecord(
                "sealed edge export made no progress".into(),
            ));
        }
        after = chunk.last;
    }
    Ok(output)
}

fn key_plan<'a>(nodes: &'a NodeExport) -> Result<KeyIndexPayloadPlan<'a>, EngineError> {
    let mut entries = Vec::new();
    for (meta, key) in nodes.metas.iter().zip(&nodes.keys) {
        for &label in meta.label_ids.as_slice() {
            entries.push(KeyIndexEntryPlan {
                label_id: label,
                key: key.as_bytes(),
                node_id: meta.node_id,
                encoded_len: 14 + u64::from(meta.key_len),
            });
        }
    }
    entries.sort_by(|a, b| {
        a.label_id
            .cmp(&b.label_id)
            .then_with(|| a.key.cmp(b.key))
            .then_with(|| a.node_id.cmp(&b.node_id))
    });
    if entries.windows(2).any(|pair| {
        pair[0].label_id == pair[1].label_id
            && pair[0].key == pair[1].key
            && pair[0].node_id != pair[1].node_id
    }) {
        return Err(EngineError::InvalidOperation(
            "duplicate live node key membership".into(),
        ));
    }
    Ok(KeyIndexPayloadPlan { entries })
}
fn vectors(
    writer: &mut PackedCoreWriter,
    nodes: &NodeExport,
    source: [u8; 32],
) -> Result<Vec<SegmentComponentRecordV1>, EngineError> {
    if !nodes.vectors.has_dense && !nodes.vectors.has_sparse {
        return Ok(Vec::new());
    }
    let dep = source_group_dependency(SegmentSourceGroupKind::NodeSource, source);
    let (meta, _) = writer.write_component(
        SegmentComponentKind::NodeVectorMetadata,
        ComponentRequirement::Required,
        ComponentTrustClass::AuxiliaryBlob,
        vec![dep.clone()],
        component_fingerprint("flush.node_vector_meta", &[]),
        |w| write_node_vector_meta_payload(w, &nodes.vectors),
    )?;
    let deps = vec![dep, source_component_dependency(&meta)];
    let mut records = vec![meta];
    if nodes.vectors.has_dense {
        let (r, _) = writer.write_component(
            SegmentComponentKind::NodeDenseVectorBlob,
            ComponentRequirement::Required,
            ComponentTrustClass::AuxiliaryBlob,
            deps.clone(),
            component_fingerprint("flush.node_dense_vectors", &[]),
            |w| {
                for point in &nodes.vectors.dense_points {
                    for v in &point.values {
                        w.write_all(&v.to_le_bytes())?;
                    }
                }
                Ok(())
            },
        )?;
        records.push(r);
    }
    if nodes.vectors.has_sparse {
        let (r, _) = writer.write_component(
            SegmentComponentKind::NodeSparseVectorBlob,
            ComponentRequirement::Required,
            ComponentTrustClass::AuxiliaryBlob,
            deps,
            component_fingerprint("flush.node_sparse_vectors", &[]),
            |w| {
                for (_, values) in &nodes.sparse {
                    for (dimension, weight) in values {
                        write_u32(w, *dimension)?;
                        w.write_all(&weight.to_le_bytes())?;
                    }
                }
                Ok(())
            },
        )?;
        records.push(r);
    }
    Ok(records)
}

pub(super) fn write_segment(
    seg_dir: &Path,
    segment_id: u64,
    memtable: &Memtable,
    dense_config: Option<&DenseVectorConfig>,
    degree_overlay: Option<&DegreeOverlaySnapshot>,
    indexes: &[SecondaryIndexManifestEntry],
    sealed: SealedExport,
) -> Result<SegmentInfo, EngineError> {
    fs::create_dir_all(seg_dir)?;
    let mut core = PackedCoreWriter::create(seg_dir, segment_id, FLUSH_COMPONENT_GENERATION)?;
    let (nr, mut nodes) = core.write_streamed_records(
        SegmentComponentKind::NodeRecords,
        sealed.nodes as u64,
        |w| export_nodes(memtable, sealed, w, indexes),
        |nodes, id, bytes| {
            let (node, end) = crate::segment_reader::decode_node_at_with_end(bytes, 0, id)?;
            let node = Owned::new(node);
            if end != bytes.len() {
                return Err(EngineError::CorruptRecord(
                    "streaming node readback trailing bytes".into(),
                ));
            }
            nodes.stats.observe_values(&node);
            Ok(())
        },
    )?;
    let (er, edges) = core.write_streamed_records(
        SegmentComponentKind::EdgeRecords,
        sealed.edges as u64,
        |w| export_edges(memtable, sealed, w),
        |_, _, _| Ok(()),
    )?;
    let (nm, _) = core.write_component(
        SegmentComponentKind::NodeMetadata,
        ComponentRequirement::Required,
        ComponentTrustClass::PrimaryMetadata,
        Vec::new(),
        component_fingerprint("flush.node_meta", &[]),
        |w| write_compact_node_meta_payload(w, &nodes.metas),
    )?;
    let (em, _) = core.write_component(
        SegmentComponentKind::EdgeMetadata,
        ComponentRequirement::Required,
        ComponentTrustClass::PrimaryMetadata,
        Vec::new(),
        component_fingerprint("flush.edge_meta", &[]),
        |w| write_compact_edge_meta_payload(w, &edges.metas),
    )?;
    let (ts, _) = core.write_component(
        SegmentComponentKind::Tombstones,
        ComponentRequirement::Required,
        ComponentTrustClass::PrimaryMetadata,
        Vec::new(),
        component_fingerprint("flush.tombstones", &[]),
        |w| write_tombstones_payload(w, &nodes.deleted, &edges.deleted),
    )?;
    let initial = segment_source_groups_from_records(
        segment_id,
        sealed.nodes as u64,
        sealed.edges as u64,
        &[nr.clone(), er.clone(), nm.clone(), em.clone(), ts.clone()],
    )?;
    let vr = vectors(&mut core, &nodes, initial.node_source)?;
    let sources = compute_flush_source_groups(
        segment_id,
        sealed.nodes as u64,
        sealed.edges as u64,
        &nr,
        &er,
        &nm,
        &em,
        &ts,
        &vr,
    )?;
    let degree = degree_overlay.map(DegreeOverlaySnapshot::sorted_entries);
    let partitions = partition_secondary_indexes(indexes);
    let compound =
        memtable.compound_secondary_state_for_indexes(&compound_flush_index_ids(&partitions))?;
    let points = std::mem::take(&mut nodes.vectors.dense_points);
    let (((node_output, edge_output), sparse_records), hnsw) = engine_cpu_try_join(
        || {
            engine_cpu_try_join(
                || {
                    engine_cpu_try_join(
                        || {
                            write_flush_node_index_components(
                                seg_dir,
                                segment_id,
                                memtable,
                                key_plan(&nodes)?,
                                &partitions.node_eq,
                                &partitions.node_range,
                                &compound,
                                sources,
                            )
                        },
                        || {
                            write_flush_edge_index_components(
                                seg_dir,
                                segment_id,
                                memtable,
                                (
                                    prepare_edge_triple_index_payload_from_meta(&edges.metas),
                                    prepare_edge_metadata_index_components_from_meta(&edges.metas),
                                ),
                                degree.as_deref(),
                                &partitions.edge_eq,
                                &partitions.edge_range,
                                &compound,
                                sources,
                            )
                        },
                    )
                },
                || {
                    let mut groups: BTreeMap<u32, Vec<(u64, f32)>> = BTreeMap::new();
                    for (id, values) in &nodes.sparse {
                        for (dimension, weight) in values {
                            groups.entry(*dimension).or_default().push((*id, *weight));
                        }
                    }
                    sort_sparse_posting_groups(&mut groups)?;
                    write_flush_sparse_posting_groups(seg_dir, segment_id, &groups, sources)
                },
            )
        },
        || maybe_build_dense_hnsw(points, dense_config),
    )?;
    emit_flush_node_index_components(&mut core, sources, &node_output)?;
    emit_flush_edge_index_components(&mut core, sources, &edge_output)?;
    let mut evidence = node_output.declared_evidence;
    evidence.extend(edge_output.declared_evidence);
    evidence.sort();
    let mut session = FlushComponentBuildSession::create();
    session.extend(node_output.external_records);
    session.extend(edge_output.external_records);
    session.extend(sparse_records);
    session.extend(write_flush_prebuilt_dense_hnsw_components(
        seg_dir,
        segment_id,
        dense_config,
        hnsw,
        sources,
    )?);
    session.extend(core.finish()?);
    let stats = assemble_flush_stats_from_partials(
        segment_id,
        indexes,
        nodes.stats.finish(&edges.metas),
        evidence,
    );
    if let Ok(Some(payload)) = planner_stats_sidecar_payload(stats) {
        if let Ok((record, _)) = write_flush_component(
            seg_dir,
            segment_id,
            PLANNER_STATS_FILENAME,
            SegmentComponentKind::PlannerStats,
            ComponentRequirement::Optional {
                fallback: ComponentFallbackClass::PlannerStatsUnavailable,
            },
            ComponentTrustClass::OptionalAdvisoryStats,
            planner_stats_component_dependencies(sources.segment_data_id, indexes),
            planner_stats_component_fingerprint(indexes),
            |w| {
                w.write_all(&payload)?;
                Ok(())
            },
        ) {
            session.push(record);
        }
    }
    validate_required_components_before_manifest(segment_id, &session.records)?;
    session.fsync_component_parent_dirs(seg_dir)?;
    fsync_dir(seg_dir)?;
    let mut records = session.records;
    sort_component_records_for_manifest(&mut records);
    write_segment_component_manifest(
        seg_dir,
        &SegmentComponentManifestV1 {
            format_version: SEGMENT_COMPONENT_MANIFEST_PAYLOAD_VERSION,
            segment_format_version: SEGMENT_FORMAT_VERSION,
            segment_id,
            generation: FLUSH_COMPONENT_GENERATION,
            built_at_ms: current_time_millis(),
            build_kind: SegmentComponentBuildKind::Flush,
            segment_data_id: sources.segment_data_id,
            node_count: sealed.nodes as u64,
            edge_count: sealed.edges as u64,
            components: records,
            unknown_optional_components: Vec::new(),
        },
    )?;
    fsync_dir(seg_dir)?;
    Ok(SegmentInfo {
        id: segment_id,
        node_count: sealed.nodes as u64,
        edge_count: sealed.edges as u64,
        segment_format_version: SEGMENT_FORMAT_VERSION,
        segment_data_id: sources.segment_data_id,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn p45c1_backpatch_counts_growth_and_partial_output() {
        assert!(table_size(u64::MAX).is_err());
        for count in [0, 2] {
            let dir = tempfile::tempdir().unwrap();
            let mut writer = PackedCoreWriter::create(dir.path(), 1, 1).unwrap();
            let result = writer.write_streamed_records(
                SegmentComponentKind::NodeRecords,
                count,
                |w| {
                    w.append(7, |buf| {
                        buf.clear();
                        buf.push(1);
                        Ok(())
                    })?;
                    Ok(())
                },
                |_, _, _| Ok(()),
            );
            assert!(result.is_err());
            assert!(!dir
                .path()
                .join(SEGMENT_COMPONENT_MANIFEST_FILENAME)
                .exists());
            assert!(!dir.path().join(PACKED_CORE_FILENAME).exists());
        }
        let dir = tempfile::tempdir().unwrap();
        let mut writer = PackedCoreWriter::create(dir.path(), 1, 1).unwrap();
        let mut observed = Vec::new();
        let (record, ()) = writer
            .write_streamed_records(
                SegmentComponentKind::NodeRecords,
                2,
                |w| {
                    for (id, bytes) in [(2, vec![8, 9]), (5, vec![1, 2, 3])] {
                        w.append(id, |buf| {
                            *buf = bytes;
                            Ok(())
                        })?;
                    }
                    Ok(())
                },
                |_, id, bytes| {
                    observed.push((id, bytes.to_vec()));
                    Ok(())
                },
            )
            .unwrap();
        assert_eq!(observed, vec![(2, vec![8, 9]), (5, vec![1, 2, 3])]);
        let expected = [
            2u64.to_le_bytes().as_slice(),
            2u64.to_le_bytes().as_slice(),
            40u64.to_le_bytes().as_slice(),
            5u64.to_le_bytes().as_slice(),
            42u64.to_le_bytes().as_slice(),
            &[8, 9, 1, 2, 3],
        ]
        .concat();
        assert_eq!(
            record.payload_digest,
            Some(Sha256::digest(&expected).into())
        );
        writer
            .write_component(
                SegmentComponentKind::Tombstones,
                ComponentRequirement::Required,
                ComponentTrustClass::PrimaryMetadata,
                Vec::new(),
                0,
                |w| {
                    w.write_all(&[7, 6])?;
                    Ok(())
                },
            )
            .unwrap();
        let records = writer.finish().unwrap();
        let container = records
            .iter()
            .find(|r| r.kind == SegmentComponentKind::PackedSegmentContainer)
            .unwrap();
        let bytes = fs::read(dir.path().join(PACKED_CORE_FILENAME)).unwrap();
        assert_eq!(
            container.payload_digest,
            Some(Sha256::digest(&bytes[COMPONENT_IDENTITY_HEADER_LEN..]).into())
        );
        let mut export = RecordExportWriter {
            writer: &mut BufWriter::new(File::create(dir.path().join("overflow")).unwrap()),
            spans: Vec::new(),
            offset: u64::MAX,
            buffer: Vec::new(),
            count: 1,
        };
        assert!(export
            .append(1, |buf| {
                buf.push(1);
                Ok(())
            })
            .is_err());
    }
}
