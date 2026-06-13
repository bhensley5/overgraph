use crate::error::EngineError;
use crate::secondary_index_key::{
    public_canonical_field_name, public_field_source, COMPOUND_INDEX_KEY_ENCODING_VERSION,
    COMPOUND_INDEX_METADATA_ENUM_VERSION, COMPOUND_INDEX_SENTINEL_ORDERING_VERSION,
    MAX_COMPOUND_COMPONENT_BYTES, MAX_COMPOUND_TUPLE_BYTES,
    MAX_SECONDARY_INDEX_FIELDS as MAX_COMPOUND_SECONDARY_INDEX_FIELDS,
};
use crate::types::{
    SecondaryIndexFieldManifest, SecondaryIndexKind, SecondaryIndexManifestEntry,
    SecondaryIndexTarget,
};
use crc32fast::Hasher as Crc32Hasher;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::HashSet;
use std::fs::File;
use std::io::{BufWriter, Seek, SeekFrom, Write};
use std::path::{Component, Path};

pub(crate) type ComponentDigest32 = [u8; 32];

pub(crate) const SEGMENT_COMPONENT_MANIFEST_FILENAME: &str = "segment_manifest.dat";
pub(crate) const SEGMENT_COMPONENT_MANIFEST_TMP_FILENAME: &str = "segment_manifest.tmp";
pub(crate) const PACKED_CORE_FILENAME: &str = "segment.core";
pub(crate) const PACKED_CORE_TMP_FILENAME: &str = "segment.core.tmp";
pub(crate) const SEGMENT_COMPONENT_MANIFEST_MAGIC: [u8; 8] = *b"OGSID01\0";
pub(crate) const SEGMENT_COMPONENT_MANIFEST_ENVELOPE_VERSION: u32 = 1;
pub(crate) const SEGMENT_COMPONENT_MANIFEST_PAYLOAD_VERSION: u32 = 1;
pub(crate) const SEGMENT_COMPONENT_MANIFEST_ENVELOPE_LEN: usize = 28;
pub(crate) const SEGMENT_COMPONENT_MANIFEST_MAX_PAYLOAD_LEN: usize = 16 * 1024 * 1024;

pub(crate) const COMPONENT_IDENTITY_HEADER_MAGIC: [u8; 8] = *b"OGCID01\0";
pub(crate) const COMPONENT_IDENTITY_HEADER_VERSION: u16 = 1;
pub(crate) const COMPONENT_IDENTITY_HEADER_LEN: usize = 192;
pub(crate) const ZERO_DIGEST: ComponentDigest32 = [0; 32];

const COMPONENT_IDENTITY_DOMAIN: &[u8] = b"overgraph.component.identity.v1";
const DEPENDENCY_DIGEST_DOMAIN: &[u8] = b"overgraph.component.dependencies.v1";
const SOURCE_GROUP_DIGEST_DOMAIN: &[u8] = b"overgraph.component.source_group.v1";
const SEGMENT_DATA_DIGEST_DOMAIN: &[u8] = b"overgraph.segment_data.v1";
const SEMANTIC_FINGERPRINT_DOMAIN: &[u8] = b"overgraph.semantic_fingerprint.v1";
const SECONDARY_DECLARATION_FINGERPRINT_DOMAIN: &[u8] = b"overgraph.secondary_index_declaration.v1";
const SECONDARY_DECLARATION_FINGERPRINT_V2_DOMAIN: &[u8] = b"secondary-index-declaration-v2";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum SegmentComponentBuildKind {
    Flush,
    Compaction,
    OptionalRefresh,
    TestFixture,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct SegmentComponentManifestV1 {
    pub format_version: u32,
    pub segment_format_version: u32,
    pub segment_id: u64,
    pub generation: u64,
    pub built_at_ms: i64,
    pub build_kind: SegmentComponentBuildKind,
    pub segment_data_id: ComponentDigest32,
    pub node_count: u64,
    pub edge_count: u64,
    pub components: Vec<SegmentComponentRecordV1>,
    pub unknown_optional_components: Vec<UnknownOptionalComponentRecordV1>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct SegmentComponentRecordV1 {
    pub component_id: ComponentDigest32,
    pub kind: SegmentComponentKind,
    pub logical_format_version: u32,
    pub created_generation: u64,
    pub requirement: ComponentRequirement,
    pub trust_class: ComponentTrustClass,
    pub handle: ComponentHandleV1,
    pub payload_len: u64,
    pub payload_digest: Option<ComponentDigest32>,
    pub dependency_digest: ComponentDigest32,
    pub dependencies: Vec<ComponentDependencyV1>,
    pub build_fingerprint: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct UnknownOptionalComponentRecordV1 {
    pub wire: SegmentComponentRecordWireV1,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct SegmentComponentManifestWireV1 {
    pub format_version: u32,
    pub segment_format_version: u32,
    pub segment_id: u64,
    pub generation: u64,
    pub built_at_ms: i64,
    pub build_kind_tag: u8,
    pub segment_data_id: ComponentDigest32,
    pub node_count: u64,
    pub edge_count: u64,
    pub components: Vec<SegmentComponentRecordWireV1>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct SegmentComponentRecordWireV1 {
    pub component_id: ComponentDigest32,
    pub kind_tag: u32,
    pub index_id: Option<u64>,
    pub logical_format_version: u32,
    pub created_generation: u64,
    pub requirement_tag: u8,
    pub fallback_tag: u8,
    pub trust_class_tag: u8,
    pub handle: ComponentHandleWireV1,
    pub payload_len: u64,
    pub payload_digest: Option<ComponentDigest32>,
    pub dependency_digest: ComponentDigest32,
    pub dependencies: Vec<ComponentDependencyWireV1>,
    pub build_fingerprint: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct ComponentHandleWireV1 {
    pub handle_tag: u8,
    pub relative_path: Option<String>,
    pub payload_offset: u64,
    pub payload_len: u64,
    pub container_component_id: Option<ComponentDigest32>,
    pub offset: u64,
    pub len: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum ComponentHandleV1 {
    ExternalFile {
        relative_path: String,
        payload_offset: u64,
        payload_len: u64,
    },
    PackedRange {
        container_component_id: ComponentDigest32,
        offset: u64,
        len: u64,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(crate) enum SegmentComponentKind {
    NodeRecords,
    EdgeRecords,
    NodeMetadata,
    EdgeMetadata,
    Tombstones,
    KeyIndex,
    NodeLabelIndex,
    EdgeLabelIndex,
    EdgeTripleIndex,
    AdjOutIndex,
    AdjOutPostings,
    AdjInIndex,
    AdjInPostings,
    TimestampIndex,
    LegacyNodePropertyIndex,
    NodePropertyHashMetadata,
    NodePropertyEqualityIndex { index_id: u64 },
    NodePropertyRangeIndex { index_id: u64 },
    EdgeWeightIndex,
    EdgeUpdatedAtIndex,
    EdgeValidFromIndex,
    EdgeValidToIndex,
    DegreeDelta,
    PlannerStats,
    NodeVectorMetadata,
    NodeDenseVectorBlob,
    NodeSparseVectorBlob,
    DenseHnswMetadata,
    DenseHnswGraph,
    SparsePostingIndex,
    SparsePostings,
    EdgePropertyEqualityIndex { index_id: u64 },
    EdgePropertyRangeIndex { index_id: u64 },
    PackedSegmentContainer,
    NodeCompoundEqualityIndex { index_id: u64 },
    NodeCompoundRangeIndex { index_id: u64 },
    EdgeCompoundEqualityIndex { index_id: u64 },
    EdgeCompoundRangeIndex { index_id: u64 },
}

/// Map a secondary index manifest entry to its sidecar component kind.
///
/// Single source of truth for the (target, kind) → component mapping used by
/// both the writer (flush/compaction sidecar emission) and the reader
/// (sidecar discovery and validation).
pub(crate) fn secondary_index_component_kind_for_entry(
    entry: &SecondaryIndexManifestEntry,
) -> Option<SegmentComponentKind> {
    match (&entry.target, &entry.kind) {
        (SecondaryIndexTarget::NodeProperty { .. }, SecondaryIndexKind::Equality) => {
            Some(SegmentComponentKind::NodePropertyEqualityIndex {
                index_id: entry.index_id,
            })
        }
        (SecondaryIndexTarget::NodeProperty { .. }, SecondaryIndexKind::Range) => {
            Some(SegmentComponentKind::NodePropertyRangeIndex {
                index_id: entry.index_id,
            })
        }
        (SecondaryIndexTarget::EdgeProperty { .. }, SecondaryIndexKind::Equality) => {
            Some(SegmentComponentKind::EdgePropertyEqualityIndex {
                index_id: entry.index_id,
            })
        }
        (SecondaryIndexTarget::EdgeProperty { .. }, SecondaryIndexKind::Range) => {
            Some(SegmentComponentKind::EdgePropertyRangeIndex {
                index_id: entry.index_id,
            })
        }
        (SecondaryIndexTarget::NodeFieldIndex { .. }, SecondaryIndexKind::Equality) => {
            Some(SegmentComponentKind::NodeCompoundEqualityIndex {
                index_id: entry.index_id,
            })
        }
        (SecondaryIndexTarget::NodeFieldIndex { .. }, SecondaryIndexKind::Range) => {
            Some(SegmentComponentKind::NodeCompoundRangeIndex {
                index_id: entry.index_id,
            })
        }
        (SecondaryIndexTarget::EdgeFieldIndex { .. }, SecondaryIndexKind::Equality) => {
            Some(SegmentComponentKind::EdgeCompoundEqualityIndex {
                index_id: entry.index_id,
            })
        }
        (SecondaryIndexTarget::EdgeFieldIndex { .. }, SecondaryIndexKind::Range) => {
            Some(SegmentComponentKind::EdgeCompoundRangeIndex {
                index_id: entry.index_id,
            })
        }
    }
}

/// Like [`secondary_index_component_kind_for_entry`], but only for compound
/// (field-index) declarations; property-target entries map to `None`.
pub(crate) fn compound_component_kind_for_entry(
    entry: &SecondaryIndexManifestEntry,
) -> Option<SegmentComponentKind> {
    match &entry.target {
        SecondaryIndexTarget::NodeFieldIndex { .. }
        | SecondaryIndexTarget::EdgeFieldIndex { .. } => {
            secondary_index_component_kind_for_entry(entry)
        }
        SecondaryIndexTarget::NodeProperty { .. } | SecondaryIndexTarget::EdgeProperty { .. } => {
            None
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum ComponentRequirement {
    Required,
    Optional { fallback: ComponentFallbackClass },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ComponentFallbackClass {
    MetadataScan,
    TypeScan,
    AdjacencyWalk,
    RecordScan,
    ExactVectorScan,
    PlannerStatsUnavailable,
    FeatureUnavailable,
    None,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ComponentTrustClass {
    PrimaryData,
    PrimaryMetadata,
    CoreMaintainedIndex,
    OptionalCandidateIndex,
    OptionalExactAccelerator,
    OptionalAdvisoryStats,
    OptionalApproximateAccelerator,
    AuxiliaryBlob,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum ComponentAvailability {
    Available,
    Missing,
    Incompatible { reason: String },
    CorruptIdentity { reason: String },
    Unsupported { reason: String },
}

impl ComponentAvailability {
    pub(crate) fn is_available(&self) -> bool {
        matches!(self, ComponentAvailability::Available)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct SegmentComponentSourceGroups {
    pub node_source: ComponentDigest32,
    pub edge_source: ComponentDigest32,
    pub node_property_content_source: ComponentDigest32,
    pub node_property_hash_source: ComponentDigest32,
    pub edge_metadata_source: ComponentDigest32,
    pub degree_source: ComponentDigest32,
    pub dense_vector_source: ComponentDigest32,
    pub sparse_vector_source: ComponentDigest32,
    pub segment_data_id: ComponentDigest32,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum ComponentDependencyV1 {
    SourceComponent {
        kind: SegmentComponentKind,
        component_id: ComponentDigest32,
    },
    SourceGroup {
        group: SegmentSourceGroupKind,
        group_id: ComponentDigest32,
    },
    SecondaryIndexDeclaration {
        index_id: u64,
        target_kind: SecondaryIndexTargetKindForComponents,
        kind: SecondaryIndexKindFingerprint,
        fingerprint: u64,
    },
    DenseVectorConfig {
        fingerprint: u64,
    },
    SparseVectorConfig {
        fingerprint: u64,
    },
    WriterBuildParams {
        fingerprint: u64,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct ComponentDependencyWireV1 {
    pub dependency_tag: u8,
    pub component_kind_tag: Option<u32>,
    pub component_index_id: Option<u64>,
    pub component_id: Option<ComponentDigest32>,
    pub group_tag: Option<u8>,
    pub group_id: Option<ComponentDigest32>,
    pub index_id: Option<u64>,
    pub target_kind_tag: Option<u8>,
    pub secondary_index_kind_tag: Option<u8>,
    pub fingerprint: Option<u64>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) enum SegmentSourceGroupKind {
    NodeSource,
    EdgeSource,
    NodePropertyContentSource,
    NodePropertyHashSource,
    EdgeMetadataSource,
    DegreeSource,
    DenseVectorSource,
    SparseVectorSource,
    SegmentData,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) enum SecondaryIndexTargetKindForComponents {
    Node,
    Edge,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) enum SecondaryIndexKindFingerprint {
    Equality,
    Range,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ComponentIdentityHeaderV1 {
    pub segment_format_version: u32,
    pub segment_id: u64,
    pub component_kind: SegmentComponentKind,
    pub logical_format_version: u32,
    pub created_generation: u64,
    pub payload_offset: u64,
    pub payload_len: u64,
    pub component_id: ComponentDigest32,
    pub dependency_digest: ComponentDigest32,
    pub build_fingerprint: u64,
    pub payload_digest: Option<ComponentDigest32>,
}

pub(crate) struct ComponentIdentityWriter {
    segment_format_version: u32,
    segment_id: u64,
    kind: SegmentComponentKind,
    logical_format_version: u32,
    created_generation: u64,
    requirement: ComponentRequirement,
    trust_class: ComponentTrustClass,
    relative_path: String,
    build_fingerprint: u64,
    writer: BufWriter<File>,
    payload_digest: Sha256,
    payload_len: u64,
    writes_identity_header: bool,
}

impl ComponentIdentityWriter {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn create(
        path: &Path,
        relative_path: String,
        segment_format_version: u32,
        segment_id: u64,
        kind: SegmentComponentKind,
        logical_format_version: u32,
        created_generation: u64,
        requirement: ComponentRequirement,
        trust_class: ComponentTrustClass,
        build_fingerprint: u64,
        writes_identity_header: bool,
    ) -> Result<Self, EngineError> {
        validate_relative_component_path(&relative_path)?;
        let mut writer = BufWriter::new(File::create(path)?);
        if writes_identity_header {
            writer.write_all(&[0; COMPONENT_IDENTITY_HEADER_LEN])?;
        }
        Ok(Self {
            segment_format_version,
            segment_id,
            kind,
            logical_format_version,
            created_generation,
            requirement,
            trust_class,
            relative_path,
            build_fingerprint,
            writer,
            payload_digest: Sha256::new(),
            payload_len: 0,
            writes_identity_header,
        })
    }

    pub(crate) fn finish(
        mut self,
        dependencies: Vec<ComponentDependencyV1>,
    ) -> Result<SegmentComponentRecordV1, EngineError> {
        self.writer.flush()?;
        let payload_digest: ComponentDigest32 = self.payload_digest.finalize().into();
        let dependency_digest = dependency_digest(&dependencies);
        let payload_offset = if self.writes_identity_header {
            COMPONENT_IDENTITY_HEADER_LEN as u64
        } else {
            0
        };
        let computed_component_id = component_id(
            self.segment_id,
            &self.kind,
            self.logical_format_version,
            self.payload_len,
            Some(&payload_digest),
            &dependency_digest,
            self.build_fingerprint,
        );
        if self.writes_identity_header {
            let header = ComponentIdentityHeaderV1 {
                segment_format_version: self.segment_format_version,
                segment_id: self.segment_id,
                component_kind: self.kind.clone(),
                logical_format_version: self.logical_format_version,
                created_generation: self.created_generation,
                payload_offset,
                payload_len: self.payload_len,
                component_id: computed_component_id,
                dependency_digest,
                build_fingerprint: self.build_fingerprint,
                payload_digest: Some(payload_digest),
            };
            self.writer.seek(SeekFrom::Start(0))?;
            self.writer.write_all(&encode_identity_header(&header))?;
            self.writer.flush()?;
        }
        self.writer.get_ref().sync_all()?;
        Ok(SegmentComponentRecordV1 {
            component_id: computed_component_id,
            kind: self.kind,
            logical_format_version: self.logical_format_version,
            created_generation: self.created_generation,
            requirement: self.requirement,
            trust_class: self.trust_class,
            handle: ComponentHandleV1::ExternalFile {
                relative_path: self.relative_path,
                payload_offset,
                payload_len: self.payload_len,
            },
            payload_len: self.payload_len,
            payload_digest: Some(payload_digest),
            dependency_digest,
            dependencies,
            build_fingerprint: self.build_fingerprint,
        })
    }
}

impl Write for ComponentIdentityWriter {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        let written = self.writer.write(buf)?;
        self.payload_digest.update(&buf[..written]);
        self.payload_len += written as u64;
        Ok(written)
    }

    fn write_all(&mut self, buf: &[u8]) -> std::io::Result<()> {
        self.writer.write_all(buf)?;
        self.payload_digest.update(buf);
        self.payload_len += buf.len() as u64;
        Ok(())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        self.writer.flush()
    }
}

pub(crate) fn encode_manifest_envelope(
    manifest: &SegmentComponentManifestV1,
) -> Result<Vec<u8>, EngineError> {
    validate_manifest(manifest)?;
    let wire = manifest_to_wire(manifest);
    let payload = rmp_serde::to_vec(&wire)
        .map_err(|error| EngineError::SerializationError(error.to_string()))?;
    if payload.len() > SEGMENT_COMPONENT_MANIFEST_MAX_PAYLOAD_LEN {
        return Err(component_manifest_error(
            "segment component manifest exceeds hard cap",
        ));
    }
    let mut crc = Crc32Hasher::new();
    crc.update(&payload);
    let checksum = crc.finalize();
    let mut data = Vec::with_capacity(SEGMENT_COMPONENT_MANIFEST_ENVELOPE_LEN + payload.len());
    data.extend_from_slice(&SEGMENT_COMPONENT_MANIFEST_MAGIC);
    data.extend_from_slice(&SEGMENT_COMPONENT_MANIFEST_ENVELOPE_VERSION.to_le_bytes());
    data.extend_from_slice(&manifest.segment_format_version.to_le_bytes());
    data.extend_from_slice(&(payload.len() as u64).to_le_bytes());
    data.extend_from_slice(&checksum.to_le_bytes());
    data.extend_from_slice(&payload);
    Ok(data)
}

pub(crate) fn decode_manifest_envelope(
    data: &[u8],
) -> Result<SegmentComponentManifestV1, EngineError> {
    if data.len() < SEGMENT_COMPONENT_MANIFEST_ENVELOPE_LEN {
        return Err(component_manifest_error(
            "segment component manifest is shorter than envelope",
        ));
    }
    if data[0..8] != SEGMENT_COMPONENT_MANIFEST_MAGIC {
        return Err(component_manifest_error(
            "segment component manifest has bad magic",
        ));
    }
    let envelope_version = u32::from_le_bytes(data[8..12].try_into().unwrap());
    if envelope_version != SEGMENT_COMPONENT_MANIFEST_ENVELOPE_VERSION {
        return Err(component_manifest_error(format!(
            "unsupported segment component manifest envelope version {}",
            envelope_version
        )));
    }
    let segment_format_version = u32::from_le_bytes(data[12..16].try_into().unwrap());
    let payload_len = u64::from_le_bytes(data[16..24].try_into().unwrap()) as usize;
    if payload_len > SEGMENT_COMPONENT_MANIFEST_MAX_PAYLOAD_LEN {
        return Err(component_manifest_error(
            "segment component manifest exceeds hard cap",
        ));
    }
    let expected_payload_len = data.len() - SEGMENT_COMPONENT_MANIFEST_ENVELOPE_LEN;
    if payload_len != expected_payload_len {
        return Err(component_manifest_error(format!(
            "segment component manifest payload length mismatch: header={}, actual={}",
            payload_len, expected_payload_len
        )));
    }
    let expected_crc = u32::from_le_bytes(data[24..28].try_into().unwrap());
    let payload = &data[SEGMENT_COMPONENT_MANIFEST_ENVELOPE_LEN..];
    let mut crc = Crc32Hasher::new();
    crc.update(payload);
    if crc.finalize() != expected_crc {
        return Err(component_manifest_error(
            "segment component manifest payload crc mismatch",
        ));
    }
    let wire: SegmentComponentManifestWireV1 = rmp_serde::from_slice(payload)
        .map_err(|error| EngineError::SerializationError(error.to_string()))?;
    let mut manifest = manifest_from_wire(wire)?;
    manifest.segment_format_version = segment_format_version;
    validate_manifest(&manifest)?;
    Ok(manifest)
}

pub(crate) fn encode_identity_header(
    header: &ComponentIdentityHeaderV1,
) -> [u8; COMPONENT_IDENTITY_HEADER_LEN] {
    let mut data = [0u8; COMPONENT_IDENTITY_HEADER_LEN];
    data[0..8].copy_from_slice(&COMPONENT_IDENTITY_HEADER_MAGIC);
    data[8..10].copy_from_slice(&COMPONENT_IDENTITY_HEADER_VERSION.to_le_bytes());
    data[10..12].copy_from_slice(&0u16.to_le_bytes());
    data[12..16].copy_from_slice(&header.segment_format_version.to_le_bytes());
    data[16..24].copy_from_slice(&header.segment_id.to_le_bytes());
    data[24..28].copy_from_slice(&header.component_kind.kind_tag().to_le_bytes());
    data[28..32].copy_from_slice(&header.logical_format_version.to_le_bytes());
    data[32..40].copy_from_slice(&header.created_generation.to_le_bytes());
    data[40..48].copy_from_slice(&header.payload_offset.to_le_bytes());
    data[48..56].copy_from_slice(&header.payload_len.to_le_bytes());
    data[56..88].copy_from_slice(&header.component_id);
    data[88..120].copy_from_slice(&header.dependency_digest);
    data[120..128].copy_from_slice(&header.build_fingerprint.to_le_bytes());
    data[128] = u8::from(header.payload_digest.is_some());
    data[129] = u8::from(header.component_kind.index_id().is_some());
    if let Some(payload_digest) = header.payload_digest {
        data[136..168].copy_from_slice(&payload_digest);
    }
    data[168..176].copy_from_slice(&header.component_kind.index_id().unwrap_or(0).to_le_bytes());
    data
}

pub(crate) fn decode_identity_header(
    data: &[u8],
) -> Result<ComponentIdentityHeaderV1, EngineError> {
    if data.len() < COMPONENT_IDENTITY_HEADER_LEN {
        return Err(component_manifest_error(
            "component identity header is too short",
        ));
    }
    if data[0..8] != COMPONENT_IDENTITY_HEADER_MAGIC {
        return Err(component_manifest_error(
            "component identity header has bad magic",
        ));
    }
    let header_version = u16::from_le_bytes(data[8..10].try_into().unwrap());
    if header_version != COMPONENT_IDENTITY_HEADER_VERSION {
        return Err(component_manifest_error(format!(
            "unsupported component identity header version {}",
            header_version
        )));
    }
    if u16::from_le_bytes(data[10..12].try_into().unwrap()) != 0 {
        return Err(component_manifest_error(
            "component identity header reserved field is nonzero",
        ));
    }
    if data[130..136] != [0; 6]
        || data[176..COMPONENT_IDENTITY_HEADER_LEN]
            .iter()
            .any(|b| *b != 0)
    {
        return Err(component_manifest_error(
            "component identity header reserved bytes are nonzero",
        ));
    }
    let kind_tag = u32::from_le_bytes(data[24..28].try_into().unwrap());
    let payload_digest = match data[128] {
        0 => {
            if data[136..168].iter().any(|b| *b != 0) {
                return Err(component_manifest_error(
                    "component identity header has digest bytes without payload digest flag",
                ));
            }
            None
        }
        1 => Some(data[136..168].try_into().unwrap()),
        _ => {
            return Err(component_manifest_error(
                "component identity header has invalid payload digest flag",
            ));
        }
    };
    let index_id_value = u64::from_le_bytes(data[168..176].try_into().unwrap());
    let index_id = match data[129] {
        0 if index_id_value == 0 => None,
        0 => {
            return Err(component_manifest_error(
                "component identity header has index_id bytes without index flag",
            ));
        }
        1 => Some(index_id_value),
        _ => {
            return Err(component_manifest_error(
                "component identity header has invalid index_id flag",
            ));
        }
    };
    let component_kind =
        SegmentComponentKind::from_tag_and_index(kind_tag, index_id)?.ok_or_else(|| {
            component_manifest_error("component identity header has unknown kind tag")
        })?;
    Ok(ComponentIdentityHeaderV1 {
        segment_format_version: u32::from_le_bytes(data[12..16].try_into().unwrap()),
        segment_id: u64::from_le_bytes(data[16..24].try_into().unwrap()),
        component_kind,
        logical_format_version: u32::from_le_bytes(data[28..32].try_into().unwrap()),
        created_generation: u64::from_le_bytes(data[32..40].try_into().unwrap()),
        payload_offset: u64::from_le_bytes(data[40..48].try_into().unwrap()),
        payload_len: u64::from_le_bytes(data[48..56].try_into().unwrap()),
        component_id: data[56..88].try_into().unwrap(),
        dependency_digest: data[88..120].try_into().unwrap(),
        build_fingerprint: u64::from_le_bytes(data[120..128].try_into().unwrap()),
        payload_digest,
    })
}

pub(crate) fn validate_relative_component_path(relative_path: &str) -> Result<(), EngineError> {
    if relative_path.is_empty() {
        return Err(component_manifest_error("component relative path is empty"));
    }
    if relative_path.ends_with('/') || relative_path.ends_with('\\') {
        return Err(component_manifest_error(
            "component relative path must not end with a separator",
        ));
    }
    if relative_path.contains('\\') {
        return Err(component_manifest_error(
            "component relative path must not contain backslashes",
        ));
    }
    if relative_path.chars().any(char::is_control) {
        return Err(component_manifest_error(
            "component relative path must not contain control characters",
        ));
    }
    let path = Path::new(relative_path);
    if path.is_absolute() {
        return Err(component_manifest_error(
            "component relative path must not be absolute",
        ));
    }
    let mut saw_normal = false;
    for component in path.components() {
        match component {
            Component::Normal(part) if !part.is_empty() => saw_normal = true,
            Component::CurDir | Component::ParentDir => {
                return Err(component_manifest_error(
                    "component relative path must be normalized",
                ));
            }
            _ => {
                return Err(component_manifest_error(
                    "component relative path contains an invalid component",
                ));
            }
        }
    }
    if !saw_normal {
        return Err(component_manifest_error(
            "component relative path must include a file name",
        ));
    }
    Ok(())
}

pub(crate) fn component_id(
    segment_id: u64,
    kind: &SegmentComponentKind,
    logical_format_version: u32,
    payload_len: u64,
    payload_digest: Option<&ComponentDigest32>,
    dependency_digest: &ComponentDigest32,
    build_fingerprint: u64,
) -> ComponentDigest32 {
    let mut hasher = Sha256::new();
    hasher.update(COMPONENT_IDENTITY_DOMAIN);
    put_u64(&mut hasher, segment_id);
    put_u32(&mut hasher, kind.kind_tag());
    put_u64(&mut hasher, kind.index_id().unwrap_or(0));
    put_u32(&mut hasher, logical_format_version);
    put_u64(&mut hasher, payload_len);
    hasher.update(payload_digest.unwrap_or(&ZERO_DIGEST));
    hasher.update(dependency_digest);
    put_u64(&mut hasher, build_fingerprint);
    hasher.finalize().into()
}

pub(crate) fn dependency_digest(dependencies: &[ComponentDependencyV1]) -> ComponentDigest32 {
    let mut wires: Vec<ComponentDependencyWireV1> =
        dependencies.iter().map(dependency_to_wire).collect();
    dependency_digest_from_wire(&mut wires).expect("dependency_to_wire produces valid dependencies")
}

fn dependency_digest_from_wire(
    dependencies: &mut [ComponentDependencyWireV1],
) -> Result<ComponentDigest32, EngineError> {
    for dependency in dependencies.iter() {
        dependency_from_wire(dependency)?;
    }
    let mut canonical: Vec<Vec<u8>> = dependencies
        .iter()
        .map(dependency_canonical_bytes)
        .collect();
    canonical.sort();
    let mut hasher = Sha256::new();
    hasher.update(DEPENDENCY_DIGEST_DOMAIN);
    for dependency in canonical {
        hasher.update(dependency);
    }
    Ok(hasher.finalize().into())
}

pub(crate) fn digest_source_group(
    group: SegmentSourceGroupKind,
    digests: &[ComponentDigest32],
) -> ComponentDigest32 {
    let mut hasher = Sha256::new();
    hasher.update(SOURCE_GROUP_DIGEST_DOMAIN);
    put_u8(&mut hasher, group.tag());
    for digest in digests {
        hasher.update(digest);
    }
    hasher.finalize().into()
}

pub(crate) fn digest_segment_data(
    segment_id: u64,
    node_count: u64,
    edge_count: u64,
    node_source: &ComponentDigest32,
    edge_source: &ComponentDigest32,
    dense_vector_source: &ComponentDigest32,
    sparse_vector_source: &ComponentDigest32,
) -> ComponentDigest32 {
    let mut hasher = Sha256::new();
    hasher.update(SEGMENT_DATA_DIGEST_DOMAIN);
    put_u64(&mut hasher, segment_id);
    put_u64(&mut hasher, node_count);
    put_u64(&mut hasher, edge_count);
    hasher.update(node_source);
    hasher.update(edge_source);
    hasher.update(dense_vector_source);
    hasher.update(sparse_vector_source);
    hasher.finalize().into()
}

pub(crate) fn component_semantic_fingerprint(namespace: &str, fields: &[u64]) -> u64 {
    let mut hasher = Sha256::new();
    hasher.update(SEMANTIC_FINGERPRINT_DOMAIN);
    put_bytes_with_len(&mut hasher, namespace.as_bytes());
    for field in fields {
        put_u64(&mut hasher, *field);
    }
    fingerprint_from_digest(hasher.finalize().into())
}

pub(crate) fn component_build_fingerprint(
    segment_format_version: u32,
    namespace: &str,
    fields: &[u64],
) -> u64 {
    let mut all_fields = Vec::with_capacity(fields.len() + 1);
    all_fields.push(segment_format_version as u64);
    all_fields.extend_from_slice(fields);
    component_semantic_fingerprint(namespace, &all_fields)
}

#[allow(clippy::too_many_arguments)]
pub(crate) fn secondary_index_declaration_fingerprint(
    index_id: u64,
    target_kind: SecondaryIndexTargetKindForComponents,
    target_label_id: u32,
    property_key: &[u8],
    index_kind: SecondaryIndexKindFingerprint,
    range_key_schema: u64,
    declaration_generation: u64,
    value_encoding_version: u64,
) -> u64 {
    let mut hasher = Sha256::new();
    hasher.update(SECONDARY_DECLARATION_FINGERPRINT_DOMAIN);
    put_u64(&mut hasher, index_id);
    put_u8(&mut hasher, target_kind.tag());
    put_u32(&mut hasher, target_label_id);
    put_bytes_with_len(&mut hasher, property_key);
    put_u8(&mut hasher, index_kind.tag());
    put_u64(&mut hasher, range_key_schema);
    put_u64(&mut hasher, declaration_generation);
    put_u64(&mut hasher, value_encoding_version);
    fingerprint_from_digest(hasher.finalize().into())
}

pub(crate) fn secondary_index_declaration_fingerprint_for_entry(
    entry: &SecondaryIndexManifestEntry,
) -> u64 {
    match &entry.target {
        SecondaryIndexTarget::NodeProperty { label_id, prop_key } => {
            secondary_index_declaration_fingerprint_for_single_property(
                entry.index_id,
                SecondaryIndexTargetKindForComponents::Node,
                *label_id,
                prop_key.as_bytes(),
                &entry.kind,
            )
        }
        SecondaryIndexTarget::EdgeProperty { label_id, prop_key } => {
            secondary_index_declaration_fingerprint_for_single_property(
                entry.index_id,
                SecondaryIndexTargetKindForComponents::Edge,
                *label_id,
                prop_key.as_bytes(),
                &entry.kind,
            )
        }
        SecondaryIndexTarget::NodeFieldIndex { label_id, fields } => {
            secondary_index_declaration_fingerprint_v2(
                entry.index_id,
                SecondaryIndexTargetKindForComponents::Node,
                *label_id,
                &entry.kind,
                fields,
            )
        }
        SecondaryIndexTarget::EdgeFieldIndex { label_id, fields } => {
            secondary_index_declaration_fingerprint_v2(
                entry.index_id,
                SecondaryIndexTargetKindForComponents::Edge,
                *label_id,
                &entry.kind,
                fields,
            )
        }
    }
}

fn secondary_index_declaration_fingerprint_for_single_property(
    index_id: u64,
    target_kind: SecondaryIndexTargetKindForComponents,
    target_label_id: u32,
    property_key: &[u8],
    kind: &SecondaryIndexKind,
) -> u64 {
    let (kind, range_key_schema, value_encoding_version) = match kind {
        SecondaryIndexKind::Equality => (SecondaryIndexKindFingerprint::Equality, 0, 2),
        SecondaryIndexKind::Range => (SecondaryIndexKindFingerprint::Range, 0, 2),
    };
    secondary_index_declaration_fingerprint(
        index_id,
        target_kind,
        target_label_id,
        property_key,
        kind,
        range_key_schema,
        1,
        value_encoding_version,
    )
}

fn secondary_index_declaration_fingerprint_v2(
    index_id: u64,
    target_kind: SecondaryIndexTargetKindForComponents,
    target_label_id: u32,
    kind: &SecondaryIndexKind,
    fields: &[SecondaryIndexFieldManifest],
) -> u64 {
    let mut hasher = Sha256::new();
    hasher.update(SECONDARY_DECLARATION_FINGERPRINT_V2_DOMAIN);
    put_u64(&mut hasher, index_id);
    put_u8(
        &mut hasher,
        match kind {
            SecondaryIndexKind::Equality => SecondaryIndexKindFingerprint::Equality.tag(),
            SecondaryIndexKind::Range => SecondaryIndexKindFingerprint::Range.tag(),
        },
    );
    put_u8(&mut hasher, target_kind.tag());
    put_u32(&mut hasher, target_label_id);
    put_u64(&mut hasher, fields.len() as u64);
    for field in fields {
        let public = field.to_public();
        put_u8(&mut hasher, public_field_source(&public).tag());
        put_bytes_with_len(&mut hasher, public_canonical_field_name(&public).as_bytes());
    }
    put_u64(&mut hasher, COMPOUND_INDEX_KEY_ENCODING_VERSION as u64);
    put_u64(&mut hasher, COMPOUND_INDEX_SENTINEL_ORDERING_VERSION as u64);
    put_u64(&mut hasher, COMPOUND_INDEX_METADATA_ENUM_VERSION as u64);
    put_u64(&mut hasher, MAX_COMPOUND_SECONDARY_INDEX_FIELDS as u64);
    put_u64(&mut hasher, MAX_COMPOUND_COMPONENT_BYTES as u64);
    put_u64(&mut hasher, MAX_COMPOUND_TUPLE_BYTES as u64);
    fingerprint_from_digest(hasher.finalize().into())
}

pub(crate) fn secondary_declaration_dependency(
    entry: &SecondaryIndexManifestEntry,
) -> ComponentDependencyV1 {
    let target_kind = match &entry.target {
        SecondaryIndexTarget::NodeProperty { .. } | SecondaryIndexTarget::NodeFieldIndex { .. } => {
            SecondaryIndexTargetKindForComponents::Node
        }
        SecondaryIndexTarget::EdgeProperty { .. } | SecondaryIndexTarget::EdgeFieldIndex { .. } => {
            SecondaryIndexTargetKindForComponents::Edge
        }
    };
    let kind = match entry.kind {
        SecondaryIndexKind::Equality => SecondaryIndexKindFingerprint::Equality,
        SecondaryIndexKind::Range => SecondaryIndexKindFingerprint::Range,
    };
    let fingerprint = secondary_index_declaration_fingerprint_for_entry(entry);
    ComponentDependencyV1::SecondaryIndexDeclaration {
        index_id: entry.index_id,
        target_kind,
        kind,
        fingerprint,
    }
}

pub(crate) fn secondary_index_component_dependencies_for_entry(
    entry: &SecondaryIndexManifestEntry,
    source_groups: &SegmentComponentSourceGroups,
) -> Vec<ComponentDependencyV1> {
    let source_group = match &entry.target {
        SecondaryIndexTarget::NodeProperty { .. } | SecondaryIndexTarget::NodeFieldIndex { .. } => {
            (
                SegmentSourceGroupKind::NodePropertyContentSource,
                source_groups.node_property_content_source,
            )
        }
        SecondaryIndexTarget::EdgeProperty { .. } => (
            SegmentSourceGroupKind::EdgeSource,
            source_groups.edge_source,
        ),
        SecondaryIndexTarget::EdgeFieldIndex { fields, .. } => {
            if fields
                .iter()
                .any(|field| matches!(field, SecondaryIndexFieldManifest::Property { .. }))
            {
                (
                    SegmentSourceGroupKind::EdgeSource,
                    source_groups.edge_source,
                )
            } else {
                (
                    SegmentSourceGroupKind::EdgeMetadataSource,
                    source_groups.edge_metadata_source,
                )
            }
        }
    };
    vec![
        source_group_dependency(source_group.0, source_group.1),
        secondary_declaration_dependency(entry),
    ]
}

pub(crate) fn source_group_dependency(
    group: SegmentSourceGroupKind,
    group_id: ComponentDigest32,
) -> ComponentDependencyV1 {
    ComponentDependencyV1::SourceGroup { group, group_id }
}

pub(crate) fn source_component_dependency(
    record: &SegmentComponentRecordV1,
) -> ComponentDependencyV1 {
    ComponentDependencyV1::SourceComponent {
        kind: record.kind.clone(),
        component_id: record.component_id,
    }
}

pub(crate) fn segment_source_groups_from_records(
    segment_id: u64,
    node_count: u64,
    edge_count: u64,
    records: &[SegmentComponentRecordV1],
) -> Result<SegmentComponentSourceGroups, EngineError> {
    let node_records = required_component_id(records, &SegmentComponentKind::NodeRecords)?;
    let edge_records = required_component_id(records, &SegmentComponentKind::EdgeRecords)?;
    let node_meta = required_component_id(records, &SegmentComponentKind::NodeMetadata)?;
    let edge_meta = required_component_id(records, &SegmentComponentKind::EdgeMetadata)?;
    let tombstones = required_component_id(records, &SegmentComponentKind::Tombstones)?;
    let node_vector_meta =
        optional_component_id(records, &SegmentComponentKind::NodeVectorMetadata);
    let node_dense_vectors =
        optional_component_id(records, &SegmentComponentKind::NodeDenseVectorBlob);
    let node_sparse_vectors =
        optional_component_id(records, &SegmentComponentKind::NodeSparseVectorBlob);
    let node_prop_hashes =
        optional_component_id(records, &SegmentComponentKind::NodePropertyHashMetadata);

    let node_source = digest_source_group(
        SegmentSourceGroupKind::NodeSource,
        &[node_records, node_meta, tombstones],
    );
    let edge_source = digest_source_group(
        SegmentSourceGroupKind::EdgeSource,
        &[edge_records, edge_meta, tombstones],
    );
    let node_property_content_source = digest_source_group(
        SegmentSourceGroupKind::NodePropertyContentSource,
        &[node_source],
    );
    let node_property_hash_source = digest_source_group(
        SegmentSourceGroupKind::NodePropertyHashSource,
        &[node_source, node_prop_hashes],
    );
    let edge_metadata_source = digest_source_group(
        SegmentSourceGroupKind::EdgeMetadataSource,
        &[edge_meta, tombstones],
    );
    let degree_source = digest_source_group(SegmentSourceGroupKind::DegreeSource, &[edge_source]);
    let dense_vector_source = digest_source_group(
        SegmentSourceGroupKind::DenseVectorSource,
        &[node_source, node_vector_meta, node_dense_vectors],
    );
    let sparse_vector_source = digest_source_group(
        SegmentSourceGroupKind::SparseVectorSource,
        &[node_source, node_vector_meta, node_sparse_vectors],
    );
    let segment_data_id = digest_segment_data(
        segment_id,
        node_count,
        edge_count,
        &node_source,
        &edge_source,
        &dense_vector_source,
        &sparse_vector_source,
    );

    Ok(SegmentComponentSourceGroups {
        node_source,
        edge_source,
        node_property_content_source,
        node_property_hash_source,
        edge_metadata_source,
        degree_source,
        dense_vector_source,
        sparse_vector_source,
        segment_data_id,
    })
}

pub(crate) fn is_packed_core_component_kind(kind: &SegmentComponentKind) -> bool {
    matches!(
        kind,
        SegmentComponentKind::NodeRecords
            | SegmentComponentKind::EdgeRecords
            | SegmentComponentKind::NodeMetadata
            | SegmentComponentKind::EdgeMetadata
            | SegmentComponentKind::Tombstones
            | SegmentComponentKind::KeyIndex
            | SegmentComponentKind::NodeLabelIndex
            | SegmentComponentKind::EdgeLabelIndex
            | SegmentComponentKind::EdgeTripleIndex
            | SegmentComponentKind::AdjOutIndex
            | SegmentComponentKind::AdjOutPostings
            | SegmentComponentKind::AdjInIndex
            | SegmentComponentKind::AdjInPostings
            | SegmentComponentKind::TimestampIndex
            | SegmentComponentKind::NodeVectorMetadata
            | SegmentComponentKind::NodeDenseVectorBlob
            | SegmentComponentKind::NodeSparseVectorBlob
            | SegmentComponentKind::EdgeWeightIndex
            | SegmentComponentKind::EdgeUpdatedAtIndex
            | SegmentComponentKind::EdgeValidFromIndex
            | SegmentComponentKind::EdgeValidToIndex
    )
}

pub(crate) fn is_refreshable_external_component_kind(kind: &SegmentComponentKind) -> bool {
    matches!(
        kind,
        SegmentComponentKind::LegacyNodePropertyIndex
            | SegmentComponentKind::NodePropertyHashMetadata
            | SegmentComponentKind::NodePropertyEqualityIndex { .. }
            | SegmentComponentKind::NodePropertyRangeIndex { .. }
            | SegmentComponentKind::DegreeDelta
            | SegmentComponentKind::PlannerStats
            | SegmentComponentKind::DenseHnswMetadata
            | SegmentComponentKind::DenseHnswGraph
            | SegmentComponentKind::SparsePostingIndex
            | SegmentComponentKind::SparsePostings
            | SegmentComponentKind::EdgePropertyEqualityIndex { .. }
            | SegmentComponentKind::EdgePropertyRangeIndex { .. }
            | SegmentComponentKind::NodeCompoundEqualityIndex { .. }
            | SegmentComponentKind::NodeCompoundRangeIndex { .. }
            | SegmentComponentKind::EdgeCompoundEqualityIndex { .. }
            | SegmentComponentKind::EdgeCompoundRangeIndex { .. }
    )
}

pub(crate) fn is_container_component_kind(kind: &SegmentComponentKind) -> bool {
    matches!(kind, SegmentComponentKind::PackedSegmentContainer)
}

pub(crate) fn packed_core_container_record(
    manifest: &SegmentComponentManifestV1,
) -> Result<&SegmentComponentRecordV1, EngineError> {
    manifest
        .components
        .iter()
        .find(|record| is_container_component_kind(&record.kind))
        .ok_or_else(|| {
            component_manifest_error("packed core manifest missing segment.core container")
        })
}

pub(crate) fn patch_packed_range_container_id(
    records: &mut [SegmentComponentRecordV1],
    container_component_id: ComponentDigest32,
) {
    for record in records {
        if let ComponentHandleV1::PackedRange {
            container_component_id: handle_container_id,
            ..
        } = &mut record.handle
        {
            *handle_container_id = container_component_id;
        }
    }
}

#[cfg(test)]
pub(crate) fn validate_packed_core_manifest_contract(
    manifest: &SegmentComponentManifestV1,
) -> Result<(), EngineError> {
    validate_packed_core_records_contract_impl(
        &manifest.components,
        PackedRangeOverlapPolicy::AllValidRanges,
    )
}

pub(crate) fn validate_packed_core_manifest_contract_for_open(
    manifest: &SegmentComponentManifestV1,
) -> Result<(), EngineError> {
    validate_packed_core_records_contract_impl(
        &manifest.components,
        PackedRangeOverlapPolicy::RequiredRangesOnly,
    )
}

pub(crate) fn validate_packed_core_records_contract(
    records: &[SegmentComponentRecordV1],
) -> Result<(), EngineError> {
    validate_packed_core_records_contract_impl(records, PackedRangeOverlapPolicy::AllValidRanges)
}

#[derive(Clone, Copy)]
enum PackedRangeOverlapPolicy {
    AllValidRanges,
    RequiredRangesOnly,
}

fn validate_packed_core_records_contract_impl(
    records: &[SegmentComponentRecordV1],
    overlap_policy: PackedRangeOverlapPolicy,
) -> Result<(), EngineError> {
    for record in records {
        if is_packed_core_component_kind(&record.kind)
            && matches!(record.handle, ComponentHandleV1::ExternalFile { .. })
        {
            return Err(component_manifest_error(format!(
                "packed core component {:?} must use a PackedRange handle",
                record.kind
            )));
        }
    }

    let packed_records: Vec<&SegmentComponentRecordV1> = records
        .iter()
        .filter(|record| matches!(record.handle, ComponentHandleV1::PackedRange { .. }))
        .collect();
    let Some(container_record) = records
        .iter()
        .find(|record| record.kind == SegmentComponentKind::PackedSegmentContainer)
    else {
        if !packed_records
            .iter()
            .any(|record| record.requirement == ComponentRequirement::Required)
        {
            return Ok(());
        }
        return Err(component_manifest_error(
            "packed core manifest has packed ranges without segment.core container",
        ));
    };

    if container_record.requirement != ComponentRequirement::Required {
        return Err(component_manifest_error(
            "packed core container must be required",
        ));
    }
    if container_record.trust_class != ComponentTrustClass::AuxiliaryBlob {
        return Err(component_manifest_error(
            "packed core container must use AuxiliaryBlob trust class",
        ));
    }
    let ComponentHandleV1::ExternalFile {
        relative_path,
        payload_offset,
        payload_len: container_payload_len,
    } = &container_record.handle
    else {
        return Err(component_manifest_error(
            "packed core container must use an external segment.core handle",
        ));
    };
    if relative_path != PACKED_CORE_FILENAME {
        return Err(component_manifest_error(format!(
            "packed core container path must be {PACKED_CORE_FILENAME}"
        )));
    }
    if *payload_offset != COMPONENT_IDENTITY_HEADER_LEN as u64 {
        return Err(component_manifest_error(
            "packed core container payload offset must follow identity header",
        ));
    }

    let mut ranges: Vec<(u64, u64, u32, Option<u64>)> = Vec::new();
    for record in packed_records {
        if record.kind == SegmentComponentKind::PackedSegmentContainer {
            return Err(component_manifest_error(
                "packed core container cannot be a packed range",
            ));
        }
        if !is_packed_core_component_kind(&record.kind) {
            if record.requirement == ComponentRequirement::Required {
                return Err(component_manifest_error(format!(
                    "component {:?} is not allowed in segment.core",
                    record.kind
                )));
            }
            continue;
        }
        let ComponentHandleV1::PackedRange {
            container_component_id,
            offset,
            len,
        } = &record.handle
        else {
            unreachable!("packed_records filtered by handle")
        };
        if record.payload_len != *len {
            if record.requirement == ComponentRequirement::Required {
                return Err(component_manifest_error(format!(
                    "packed component {:?} payload length does not match packed range",
                    record.kind
                )));
            }
            continue;
        }
        if *container_component_id != container_record.component_id {
            if record.requirement == ComponentRequirement::Required {
                return Err(component_manifest_error(format!(
                    "required packed component {:?} points at the wrong container",
                    record.kind
                )));
            }
            continue;
        }
        let Some(end) = offset.checked_add(*len) else {
            if record.requirement == ComponentRequirement::Required {
                return Err(component_manifest_error(format!(
                    "packed component {:?} range overflows",
                    record.kind
                )));
            }
            continue;
        };
        if end > *container_payload_len {
            if record.requirement == ComponentRequirement::Required {
                return Err(component_manifest_error(format!(
                    "packed component {:?} range [{}, {}) exceeds segment.core payload length {}",
                    record.kind, offset, end, container_payload_len
                )));
            }
            continue;
        }
        let include_in_overlap_check = match overlap_policy {
            PackedRangeOverlapPolicy::AllValidRanges => true,
            PackedRangeOverlapPolicy::RequiredRangesOnly => {
                record.requirement == ComponentRequirement::Required
            }
        };
        if *len > 0 && include_in_overlap_check {
            ranges.push((*offset, end, record.kind.kind_tag(), record.kind.index_id()));
        }
    }

    ranges.sort_by(|a, b| {
        a.0.cmp(&b.0)
            .then_with(|| a.1.cmp(&b.1))
            .then_with(|| a.2.cmp(&b.2))
            .then_with(|| a.3.cmp(&b.3))
    });
    for pair in ranges.windows(2) {
        let previous = pair[0];
        let current = pair[1];
        if current.0 < previous.1 {
            return Err(component_manifest_error(format!(
                "packed component ranges overlap: previous=[{}, {}), current=[{}, {})",
                previous.0, previous.1, current.0, current.1
            )));
        }
    }

    Ok(())
}

fn required_component_id(
    records: &[SegmentComponentRecordV1],
    kind: &SegmentComponentKind,
) -> Result<ComponentDigest32, EngineError> {
    records
        .iter()
        .find(|record| &record.kind == kind)
        .map(|record| record.component_id)
        .ok_or_else(|| component_manifest_error(format!("missing source component {:?}", kind)))
}

fn optional_component_id(
    records: &[SegmentComponentRecordV1],
    kind: &SegmentComponentKind,
) -> ComponentDigest32 {
    records
        .iter()
        .find(|record| &record.kind == kind)
        .map(|record| record.component_id)
        .unwrap_or(ZERO_DIGEST)
}

fn fingerprint_from_digest(digest: ComponentDigest32) -> u64 {
    let mut value = u64::from_le_bytes(digest[0..8].try_into().unwrap());
    if value == 0 {
        value = 1;
    }
    value
}

impl SegmentComponentKind {
    pub(crate) fn kind_tag(&self) -> u32 {
        match self {
            SegmentComponentKind::NodeRecords => 1,
            SegmentComponentKind::EdgeRecords => 2,
            SegmentComponentKind::NodeMetadata => 3,
            SegmentComponentKind::EdgeMetadata => 4,
            SegmentComponentKind::Tombstones => 5,
            SegmentComponentKind::KeyIndex => 6,
            SegmentComponentKind::NodeLabelIndex => 7,
            SegmentComponentKind::EdgeLabelIndex => 8,
            SegmentComponentKind::EdgeTripleIndex => 9,
            SegmentComponentKind::AdjOutIndex => 10,
            SegmentComponentKind::AdjOutPostings => 11,
            SegmentComponentKind::AdjInIndex => 12,
            SegmentComponentKind::AdjInPostings => 13,
            SegmentComponentKind::TimestampIndex => 14,
            SegmentComponentKind::LegacyNodePropertyIndex => 15,
            SegmentComponentKind::NodePropertyHashMetadata => 16,
            SegmentComponentKind::NodePropertyEqualityIndex { .. } => 17,
            SegmentComponentKind::NodePropertyRangeIndex { .. } => 18,
            SegmentComponentKind::EdgeWeightIndex => 19,
            SegmentComponentKind::EdgeUpdatedAtIndex => 20,
            SegmentComponentKind::EdgeValidFromIndex => 21,
            SegmentComponentKind::EdgeValidToIndex => 22,
            SegmentComponentKind::DegreeDelta => 23,
            SegmentComponentKind::PlannerStats => 24,
            SegmentComponentKind::NodeVectorMetadata => 25,
            SegmentComponentKind::NodeDenseVectorBlob => 26,
            SegmentComponentKind::NodeSparseVectorBlob => 27,
            SegmentComponentKind::DenseHnswMetadata => 28,
            SegmentComponentKind::DenseHnswGraph => 29,
            SegmentComponentKind::SparsePostingIndex => 30,
            SegmentComponentKind::SparsePostings => 31,
            SegmentComponentKind::EdgePropertyEqualityIndex { .. } => 32,
            SegmentComponentKind::EdgePropertyRangeIndex { .. } => 33,
            SegmentComponentKind::PackedSegmentContainer => 34,
            SegmentComponentKind::NodeCompoundEqualityIndex { .. } => 35,
            SegmentComponentKind::NodeCompoundRangeIndex { .. } => 36,
            SegmentComponentKind::EdgeCompoundEqualityIndex { .. } => 37,
            SegmentComponentKind::EdgeCompoundRangeIndex { .. } => 38,
        }
    }

    pub(crate) fn index_id(&self) -> Option<u64> {
        match self {
            SegmentComponentKind::NodePropertyEqualityIndex { index_id }
            | SegmentComponentKind::NodePropertyRangeIndex { index_id }
            | SegmentComponentKind::EdgePropertyEqualityIndex { index_id }
            | SegmentComponentKind::EdgePropertyRangeIndex { index_id }
            | SegmentComponentKind::NodeCompoundEqualityIndex { index_id }
            | SegmentComponentKind::NodeCompoundRangeIndex { index_id }
            | SegmentComponentKind::EdgeCompoundEqualityIndex { index_id }
            | SegmentComponentKind::EdgeCompoundRangeIndex { index_id } => Some(*index_id),
            _ => None,
        }
    }

    fn from_tag_and_index(
        kind_tag: u32,
        index_id: Option<u64>,
    ) -> Result<Option<Self>, EngineError> {
        let known = match kind_tag {
            1 => require_no_index(kind_tag, index_id, SegmentComponentKind::NodeRecords)?,
            2 => require_no_index(kind_tag, index_id, SegmentComponentKind::EdgeRecords)?,
            3 => require_no_index(kind_tag, index_id, SegmentComponentKind::NodeMetadata)?,
            4 => require_no_index(kind_tag, index_id, SegmentComponentKind::EdgeMetadata)?,
            5 => require_no_index(kind_tag, index_id, SegmentComponentKind::Tombstones)?,
            6 => require_no_index(kind_tag, index_id, SegmentComponentKind::KeyIndex)?,
            7 => require_no_index(kind_tag, index_id, SegmentComponentKind::NodeLabelIndex)?,
            8 => require_no_index(kind_tag, index_id, SegmentComponentKind::EdgeLabelIndex)?,
            9 => require_no_index(kind_tag, index_id, SegmentComponentKind::EdgeTripleIndex)?,
            10 => require_no_index(kind_tag, index_id, SegmentComponentKind::AdjOutIndex)?,
            11 => require_no_index(kind_tag, index_id, SegmentComponentKind::AdjOutPostings)?,
            12 => require_no_index(kind_tag, index_id, SegmentComponentKind::AdjInIndex)?,
            13 => require_no_index(kind_tag, index_id, SegmentComponentKind::AdjInPostings)?,
            14 => require_no_index(kind_tag, index_id, SegmentComponentKind::TimestampIndex)?,
            15 => require_no_index(
                kind_tag,
                index_id,
                SegmentComponentKind::LegacyNodePropertyIndex,
            )?,
            16 => require_no_index(
                kind_tag,
                index_id,
                SegmentComponentKind::NodePropertyHashMetadata,
            )?,
            17 => SegmentComponentKind::NodePropertyEqualityIndex {
                index_id: require_index(kind_tag, index_id)?,
            },
            18 => SegmentComponentKind::NodePropertyRangeIndex {
                index_id: require_index(kind_tag, index_id)?,
            },
            19 => require_no_index(kind_tag, index_id, SegmentComponentKind::EdgeWeightIndex)?,
            20 => require_no_index(kind_tag, index_id, SegmentComponentKind::EdgeUpdatedAtIndex)?,
            21 => require_no_index(kind_tag, index_id, SegmentComponentKind::EdgeValidFromIndex)?,
            22 => require_no_index(kind_tag, index_id, SegmentComponentKind::EdgeValidToIndex)?,
            23 => require_no_index(kind_tag, index_id, SegmentComponentKind::DegreeDelta)?,
            24 => require_no_index(kind_tag, index_id, SegmentComponentKind::PlannerStats)?,
            25 => require_no_index(kind_tag, index_id, SegmentComponentKind::NodeVectorMetadata)?,
            26 => require_no_index(
                kind_tag,
                index_id,
                SegmentComponentKind::NodeDenseVectorBlob,
            )?,
            27 => require_no_index(
                kind_tag,
                index_id,
                SegmentComponentKind::NodeSparseVectorBlob,
            )?,
            28 => require_no_index(kind_tag, index_id, SegmentComponentKind::DenseHnswMetadata)?,
            29 => require_no_index(kind_tag, index_id, SegmentComponentKind::DenseHnswGraph)?,
            30 => require_no_index(kind_tag, index_id, SegmentComponentKind::SparsePostingIndex)?,
            31 => require_no_index(kind_tag, index_id, SegmentComponentKind::SparsePostings)?,
            32 => SegmentComponentKind::EdgePropertyEqualityIndex {
                index_id: require_index(kind_tag, index_id)?,
            },
            33 => SegmentComponentKind::EdgePropertyRangeIndex {
                index_id: require_index(kind_tag, index_id)?,
            },
            34 => require_no_index(
                kind_tag,
                index_id,
                SegmentComponentKind::PackedSegmentContainer,
            )?,
            35 => SegmentComponentKind::NodeCompoundEqualityIndex {
                index_id: require_index(kind_tag, index_id)?,
            },
            36 => SegmentComponentKind::NodeCompoundRangeIndex {
                index_id: require_index(kind_tag, index_id)?,
            },
            37 => SegmentComponentKind::EdgeCompoundEqualityIndex {
                index_id: require_index(kind_tag, index_id)?,
            },
            38 => SegmentComponentKind::EdgeCompoundRangeIndex {
                index_id: require_index(kind_tag, index_id)?,
            },
            _ => return Ok(None),
        };
        Ok(Some(known))
    }
}

fn require_no_index(
    kind_tag: u32,
    index_id: Option<u64>,
    kind: SegmentComponentKind,
) -> Result<SegmentComponentKind, EngineError> {
    if index_id.is_some() {
        return Err(component_manifest_error(format!(
            "component kind tag {} does not accept index_id",
            kind_tag
        )));
    }
    Ok(kind)
}

fn require_index(kind_tag: u32, index_id: Option<u64>) -> Result<u64, EngineError> {
    index_id.ok_or_else(|| {
        component_manifest_error(format!("component kind tag {} requires index_id", kind_tag))
    })
}

fn manifest_to_wire(manifest: &SegmentComponentManifestV1) -> SegmentComponentManifestWireV1 {
    let mut components: Vec<SegmentComponentRecordWireV1> =
        manifest.components.iter().map(record_to_wire).collect();
    components.extend(
        manifest
            .unknown_optional_components
            .iter()
            .map(|record| record.wire.clone()),
    );
    SegmentComponentManifestWireV1 {
        format_version: manifest.format_version,
        segment_format_version: manifest.segment_format_version,
        segment_id: manifest.segment_id,
        generation: manifest.generation,
        built_at_ms: manifest.built_at_ms,
        build_kind_tag: manifest.build_kind.tag(),
        segment_data_id: manifest.segment_data_id,
        node_count: manifest.node_count,
        edge_count: manifest.edge_count,
        components,
    }
}

fn manifest_from_wire(
    wire: SegmentComponentManifestWireV1,
) -> Result<SegmentComponentManifestV1, EngineError> {
    let mut components = Vec::new();
    let mut unknown_optional_components = Vec::new();
    for record in wire.components {
        match SegmentComponentKind::from_tag_and_index(record.kind_tag, record.index_id)? {
            Some(kind) => components.push(record_from_wire(record, kind)?),
            None if requirement_from_wire(record.requirement_tag, record.fallback_tag)?
                .is_optional() =>
            {
                unknown_optional_components.push(UnknownOptionalComponentRecordV1 { wire: record });
            }
            None => {
                return Err(component_manifest_error(format!(
                    "unknown required component kind tag {}",
                    record.kind_tag
                )));
            }
        }
    }
    Ok(SegmentComponentManifestV1 {
        format_version: wire.format_version,
        segment_format_version: wire.segment_format_version,
        segment_id: wire.segment_id,
        generation: wire.generation,
        built_at_ms: wire.built_at_ms,
        build_kind: SegmentComponentBuildKind::from_tag(wire.build_kind_tag)?,
        segment_data_id: wire.segment_data_id,
        node_count: wire.node_count,
        edge_count: wire.edge_count,
        components,
        unknown_optional_components,
    })
}

fn validate_manifest(manifest: &SegmentComponentManifestV1) -> Result<(), EngineError> {
    if manifest.format_version != SEGMENT_COMPONENT_MANIFEST_PAYLOAD_VERSION {
        return Err(component_manifest_error(format!(
            "unsupported segment component manifest payload version {}",
            manifest.format_version
        )));
    }
    let mut component_ids = HashSet::new();
    let mut known_keys = HashSet::new();
    let mut unknown_keys = HashSet::new();
    for record in &manifest.components {
        if !component_ids.insert(record.component_id) {
            return Err(component_manifest_error("duplicate component id"));
        }
        let key = (record.kind.kind_tag(), record.kind.index_id());
        if !known_keys.insert(key) {
            return Err(component_manifest_error("duplicate known component kind"));
        }
        if let ComponentHandleV1::ExternalFile { relative_path, .. } = &record.handle {
            validate_relative_component_path(relative_path)?;
        }
        validate_record_payload_len(record.payload_len, &record.handle)?;
        let expected_dependency_digest = dependency_digest(&record.dependencies);
        if record.dependency_digest != expected_dependency_digest {
            return Err(component_manifest_error(
                "component dependency digest does not match dependency list",
            ));
        }
    }
    for record in &manifest.unknown_optional_components {
        if !component_ids.insert(record.wire.component_id) {
            return Err(component_manifest_error("duplicate component id"));
        }
        if SegmentComponentKind::from_tag_and_index(record.wire.kind_tag, record.wire.index_id)?
            .is_some()
        {
            return Err(component_manifest_error(
                "unknown optional component record uses a known kind tag",
            ));
        }
        if !requirement_from_wire(record.wire.requirement_tag, record.wire.fallback_tag)?
            .is_optional()
        {
            return Err(component_manifest_error(
                "unknown component record must be optional",
            ));
        }
        let key = (record.wire.kind_tag, record.wire.index_id);
        if !unknown_keys.insert(key) {
            return Err(component_manifest_error(
                "duplicate unknown optional component kind",
            ));
        }
        let handle = handle_from_wire(record.wire.handle.clone())?;
        validate_record_payload_len(record.wire.payload_len, &handle)?;
        let mut dependencies = record.wire.dependencies.clone();
        let expected_dependency_digest = dependency_digest_from_wire(&mut dependencies)?;
        if record.wire.dependency_digest != expected_dependency_digest {
            return Err(component_manifest_error(
                "unknown component dependency digest does not match dependency list",
            ));
        }
    }
    Ok(())
}

fn validate_record_payload_len(
    payload_len: u64,
    handle: &ComponentHandleV1,
) -> Result<(), EngineError> {
    let handle_payload_len = match handle {
        ComponentHandleV1::ExternalFile {
            payload_len: handle_payload_len,
            ..
        } => *handle_payload_len,
        ComponentHandleV1::PackedRange { len, .. } => *len,
    };
    if payload_len != handle_payload_len {
        return Err(component_manifest_error(format!(
            "component payload length mismatch: record={}, handle={}",
            payload_len, handle_payload_len
        )));
    }
    Ok(())
}

fn record_to_wire(record: &SegmentComponentRecordV1) -> SegmentComponentRecordWireV1 {
    let (requirement_tag, fallback_tag) = requirement_to_wire(&record.requirement);
    SegmentComponentRecordWireV1 {
        component_id: record.component_id,
        kind_tag: record.kind.kind_tag(),
        index_id: record.kind.index_id(),
        logical_format_version: record.logical_format_version,
        created_generation: record.created_generation,
        requirement_tag,
        fallback_tag,
        trust_class_tag: record.trust_class.tag(),
        handle: handle_to_wire(&record.handle),
        payload_len: record.payload_len,
        payload_digest: record.payload_digest,
        dependency_digest: record.dependency_digest,
        dependencies: record.dependencies.iter().map(dependency_to_wire).collect(),
        build_fingerprint: record.build_fingerprint,
    }
}

fn record_from_wire(
    wire: SegmentComponentRecordWireV1,
    kind: SegmentComponentKind,
) -> Result<SegmentComponentRecordV1, EngineError> {
    Ok(SegmentComponentRecordV1 {
        component_id: wire.component_id,
        kind,
        logical_format_version: wire.logical_format_version,
        created_generation: wire.created_generation,
        requirement: requirement_from_wire(wire.requirement_tag, wire.fallback_tag)?,
        trust_class: ComponentTrustClass::from_tag(wire.trust_class_tag)?,
        handle: handle_from_wire(wire.handle)?,
        payload_len: wire.payload_len,
        payload_digest: wire.payload_digest,
        dependency_digest: wire.dependency_digest,
        dependencies: wire
            .dependencies
            .iter()
            .map(dependency_from_wire)
            .collect::<Result<Vec<_>, _>>()?,
        build_fingerprint: wire.build_fingerprint,
    })
}

fn handle_to_wire(handle: &ComponentHandleV1) -> ComponentHandleWireV1 {
    match handle {
        ComponentHandleV1::ExternalFile {
            relative_path,
            payload_offset,
            payload_len,
        } => ComponentHandleWireV1 {
            handle_tag: 1,
            relative_path: Some(relative_path.clone()),
            payload_offset: *payload_offset,
            payload_len: *payload_len,
            container_component_id: None,
            offset: 0,
            len: 0,
        },
        ComponentHandleV1::PackedRange {
            container_component_id,
            offset,
            len,
        } => ComponentHandleWireV1 {
            handle_tag: 2,
            relative_path: None,
            payload_offset: 0,
            payload_len: 0,
            container_component_id: Some(*container_component_id),
            offset: *offset,
            len: *len,
        },
    }
}

fn handle_from_wire(wire: ComponentHandleWireV1) -> Result<ComponentHandleV1, EngineError> {
    match wire.handle_tag {
        1 => {
            let relative_path = wire.relative_path.ok_or_else(|| {
                component_manifest_error("external component handle missing relative_path")
            })?;
            validate_relative_component_path(&relative_path)?;
            Ok(ComponentHandleV1::ExternalFile {
                relative_path,
                payload_offset: wire.payload_offset,
                payload_len: wire.payload_len,
            })
        }
        2 => Ok(ComponentHandleV1::PackedRange {
            container_component_id: wire.container_component_id.ok_or_else(|| {
                component_manifest_error("packed component handle missing container id")
            })?,
            offset: wire.offset,
            len: wire.len,
        }),
        _ => Err(component_manifest_error(format!(
            "unknown component handle tag {}",
            wire.handle_tag
        ))),
    }
}

fn requirement_to_wire(requirement: &ComponentRequirement) -> (u8, u8) {
    match requirement {
        ComponentRequirement::Required => (1, 0),
        ComponentRequirement::Optional { fallback } => (2, fallback.tag()),
    }
}

fn requirement_from_wire(
    requirement_tag: u8,
    fallback_tag: u8,
) -> Result<ComponentRequirement, EngineError> {
    match requirement_tag {
        1 if fallback_tag == 0 => Ok(ComponentRequirement::Required),
        1 => Err(component_manifest_error(
            "required component must use empty fallback tag",
        )),
        2 => Ok(ComponentRequirement::Optional {
            fallback: ComponentFallbackClass::from_tag(fallback_tag)?,
        }),
        _ => Err(component_manifest_error(format!(
            "unknown component requirement tag {}",
            requirement_tag
        ))),
    }
}

impl ComponentRequirement {
    fn is_optional(&self) -> bool {
        matches!(self, ComponentRequirement::Optional { .. })
    }
}

impl ComponentFallbackClass {
    fn tag(self) -> u8 {
        match self {
            ComponentFallbackClass::None => 0,
            ComponentFallbackClass::MetadataScan => 1,
            ComponentFallbackClass::TypeScan => 2,
            ComponentFallbackClass::AdjacencyWalk => 3,
            ComponentFallbackClass::RecordScan => 4,
            ComponentFallbackClass::ExactVectorScan => 5,
            ComponentFallbackClass::PlannerStatsUnavailable => 6,
            ComponentFallbackClass::FeatureUnavailable => 7,
        }
    }

    fn from_tag(tag: u8) -> Result<Self, EngineError> {
        match tag {
            0 => Ok(ComponentFallbackClass::None),
            1 => Ok(ComponentFallbackClass::MetadataScan),
            2 => Ok(ComponentFallbackClass::TypeScan),
            3 => Ok(ComponentFallbackClass::AdjacencyWalk),
            4 => Ok(ComponentFallbackClass::RecordScan),
            5 => Ok(ComponentFallbackClass::ExactVectorScan),
            6 => Ok(ComponentFallbackClass::PlannerStatsUnavailable),
            7 => Ok(ComponentFallbackClass::FeatureUnavailable),
            _ => Err(component_manifest_error(format!(
                "unknown component fallback tag {}",
                tag
            ))),
        }
    }
}

impl ComponentTrustClass {
    fn tag(self) -> u8 {
        match self {
            ComponentTrustClass::PrimaryData => 1,
            ComponentTrustClass::PrimaryMetadata => 2,
            ComponentTrustClass::CoreMaintainedIndex => 3,
            ComponentTrustClass::OptionalCandidateIndex => 4,
            ComponentTrustClass::OptionalExactAccelerator => 5,
            ComponentTrustClass::OptionalAdvisoryStats => 6,
            ComponentTrustClass::OptionalApproximateAccelerator => 7,
            ComponentTrustClass::AuxiliaryBlob => 8,
        }
    }

    fn from_tag(tag: u8) -> Result<Self, EngineError> {
        match tag {
            1 => Ok(ComponentTrustClass::PrimaryData),
            2 => Ok(ComponentTrustClass::PrimaryMetadata),
            3 => Ok(ComponentTrustClass::CoreMaintainedIndex),
            4 => Ok(ComponentTrustClass::OptionalCandidateIndex),
            5 => Ok(ComponentTrustClass::OptionalExactAccelerator),
            6 => Ok(ComponentTrustClass::OptionalAdvisoryStats),
            7 => Ok(ComponentTrustClass::OptionalApproximateAccelerator),
            8 => Ok(ComponentTrustClass::AuxiliaryBlob),
            _ => Err(component_manifest_error(format!(
                "unknown component trust class tag {}",
                tag
            ))),
        }
    }
}

impl SegmentComponentBuildKind {
    fn tag(self) -> u8 {
        match self {
            SegmentComponentBuildKind::Flush => 1,
            SegmentComponentBuildKind::Compaction => 2,
            SegmentComponentBuildKind::OptionalRefresh => 3,
            SegmentComponentBuildKind::TestFixture => 4,
        }
    }

    fn from_tag(tag: u8) -> Result<Self, EngineError> {
        match tag {
            1 => Ok(SegmentComponentBuildKind::Flush),
            2 => Ok(SegmentComponentBuildKind::Compaction),
            3 => Ok(SegmentComponentBuildKind::OptionalRefresh),
            4 => Ok(SegmentComponentBuildKind::TestFixture),
            _ => Err(component_manifest_error(format!(
                "unknown component manifest build kind tag {}",
                tag
            ))),
        }
    }
}

impl SegmentSourceGroupKind {
    fn tag(self) -> u8 {
        match self {
            SegmentSourceGroupKind::NodeSource => 1,
            SegmentSourceGroupKind::EdgeSource => 2,
            SegmentSourceGroupKind::NodePropertyContentSource => 3,
            SegmentSourceGroupKind::NodePropertyHashSource => 4,
            SegmentSourceGroupKind::EdgeMetadataSource => 5,
            SegmentSourceGroupKind::DegreeSource => 6,
            SegmentSourceGroupKind::DenseVectorSource => 7,
            SegmentSourceGroupKind::SparseVectorSource => 8,
            SegmentSourceGroupKind::SegmentData => 9,
        }
    }

    fn from_tag(tag: u8) -> Result<Self, EngineError> {
        match tag {
            1 => Ok(SegmentSourceGroupKind::NodeSource),
            2 => Ok(SegmentSourceGroupKind::EdgeSource),
            3 => Ok(SegmentSourceGroupKind::NodePropertyContentSource),
            4 => Ok(SegmentSourceGroupKind::NodePropertyHashSource),
            5 => Ok(SegmentSourceGroupKind::EdgeMetadataSource),
            6 => Ok(SegmentSourceGroupKind::DegreeSource),
            7 => Ok(SegmentSourceGroupKind::DenseVectorSource),
            8 => Ok(SegmentSourceGroupKind::SparseVectorSource),
            9 => Ok(SegmentSourceGroupKind::SegmentData),
            _ => Err(component_manifest_error(format!(
                "unknown source group tag {}",
                tag
            ))),
        }
    }
}

impl SecondaryIndexTargetKindForComponents {
    fn tag(self) -> u8 {
        match self {
            SecondaryIndexTargetKindForComponents::Node => 1,
            SecondaryIndexTargetKindForComponents::Edge => 2,
        }
    }

    fn from_tag(tag: u8) -> Result<Self, EngineError> {
        match tag {
            1 => Ok(SecondaryIndexTargetKindForComponents::Node),
            2 => Ok(SecondaryIndexTargetKindForComponents::Edge),
            _ => Err(component_manifest_error(format!(
                "unknown secondary index target tag {}",
                tag
            ))),
        }
    }
}

impl SecondaryIndexKindFingerprint {
    fn tag(self) -> u8 {
        match self {
            SecondaryIndexKindFingerprint::Equality => 1,
            SecondaryIndexKindFingerprint::Range => 2,
        }
    }

    fn from_tag(tag: u8) -> Result<Self, EngineError> {
        match tag {
            1 => Ok(SecondaryIndexKindFingerprint::Equality),
            2 => Ok(SecondaryIndexKindFingerprint::Range),
            _ => Err(component_manifest_error(format!(
                "unknown secondary index kind tag {}",
                tag
            ))),
        }
    }
}

fn dependency_to_wire(dependency: &ComponentDependencyV1) -> ComponentDependencyWireV1 {
    match dependency {
        ComponentDependencyV1::SourceComponent { kind, component_id } => {
            ComponentDependencyWireV1 {
                dependency_tag: 1,
                component_kind_tag: Some(kind.kind_tag()),
                component_index_id: kind.index_id(),
                component_id: Some(*component_id),
                group_tag: None,
                group_id: None,
                index_id: None,
                target_kind_tag: None,
                secondary_index_kind_tag: None,
                fingerprint: None,
            }
        }
        ComponentDependencyV1::SourceGroup { group, group_id } => ComponentDependencyWireV1 {
            dependency_tag: 2,
            component_kind_tag: None,
            component_index_id: None,
            component_id: None,
            group_tag: Some(group.tag()),
            group_id: Some(*group_id),
            index_id: None,
            target_kind_tag: None,
            secondary_index_kind_tag: None,
            fingerprint: None,
        },
        ComponentDependencyV1::SecondaryIndexDeclaration {
            index_id,
            target_kind,
            kind,
            fingerprint,
        } => ComponentDependencyWireV1 {
            dependency_tag: 3,
            component_kind_tag: None,
            component_index_id: None,
            component_id: None,
            group_tag: None,
            group_id: None,
            index_id: Some(*index_id),
            target_kind_tag: Some(target_kind.tag()),
            secondary_index_kind_tag: Some(kind.tag()),
            fingerprint: Some(*fingerprint),
        },
        ComponentDependencyV1::DenseVectorConfig { fingerprint } => ComponentDependencyWireV1 {
            dependency_tag: 4,
            component_kind_tag: None,
            component_index_id: None,
            component_id: None,
            group_tag: None,
            group_id: None,
            index_id: None,
            target_kind_tag: None,
            secondary_index_kind_tag: None,
            fingerprint: Some(*fingerprint),
        },
        ComponentDependencyV1::SparseVectorConfig { fingerprint } => ComponentDependencyWireV1 {
            dependency_tag: 5,
            component_kind_tag: None,
            component_index_id: None,
            component_id: None,
            group_tag: None,
            group_id: None,
            index_id: None,
            target_kind_tag: None,
            secondary_index_kind_tag: None,
            fingerprint: Some(*fingerprint),
        },
        ComponentDependencyV1::WriterBuildParams { fingerprint } => ComponentDependencyWireV1 {
            dependency_tag: 6,
            component_kind_tag: None,
            component_index_id: None,
            component_id: None,
            group_tag: None,
            group_id: None,
            index_id: None,
            target_kind_tag: None,
            secondary_index_kind_tag: None,
            fingerprint: Some(*fingerprint),
        },
    }
}

fn dependency_from_wire(
    wire: &ComponentDependencyWireV1,
) -> Result<ComponentDependencyV1, EngineError> {
    match wire.dependency_tag {
        1 => Ok(ComponentDependencyV1::SourceComponent {
            kind: SegmentComponentKind::from_tag_and_index(
                required_field(wire.component_kind_tag, "dependency component kind tag")?,
                wire.component_index_id,
            )?
            .ok_or_else(|| {
                component_manifest_error("dependency references unknown component kind")
            })?,
            component_id: required_field(wire.component_id, "dependency component id")?,
        }),
        2 => Ok(ComponentDependencyV1::SourceGroup {
            group: SegmentSourceGroupKind::from_tag(required_field(
                wire.group_tag,
                "dependency source group tag",
            )?)?,
            group_id: required_field(wire.group_id, "dependency source group id")?,
        }),
        3 => Ok(ComponentDependencyV1::SecondaryIndexDeclaration {
            index_id: required_field(wire.index_id, "dependency index id")?,
            target_kind: SecondaryIndexTargetKindForComponents::from_tag(required_field(
                wire.target_kind_tag,
                "dependency target kind tag",
            )?)?,
            kind: SecondaryIndexKindFingerprint::from_tag(required_field(
                wire.secondary_index_kind_tag,
                "dependency secondary index kind tag",
            )?)?,
            fingerprint: required_field(wire.fingerprint, "dependency fingerprint")?,
        }),
        4 => Ok(ComponentDependencyV1::DenseVectorConfig {
            fingerprint: required_field(wire.fingerprint, "dependency fingerprint")?,
        }),
        5 => Ok(ComponentDependencyV1::SparseVectorConfig {
            fingerprint: required_field(wire.fingerprint, "dependency fingerprint")?,
        }),
        6 => Ok(ComponentDependencyV1::WriterBuildParams {
            fingerprint: required_field(wire.fingerprint, "dependency fingerprint")?,
        }),
        _ => Err(component_manifest_error(format!(
            "unknown dependency tag {}",
            wire.dependency_tag
        ))),
    }
}

fn dependency_canonical_bytes(dependency: &ComponentDependencyWireV1) -> Vec<u8> {
    let mut bytes = Vec::with_capacity(96);
    push_u8(&mut bytes, dependency.dependency_tag);
    push_u32(&mut bytes, dependency.component_kind_tag.unwrap_or(0));
    push_u64(&mut bytes, dependency.component_index_id.unwrap_or(0));
    bytes.extend_from_slice(&dependency.component_id.unwrap_or(ZERO_DIGEST));
    push_u8(&mut bytes, dependency.group_tag.unwrap_or(0));
    bytes.extend_from_slice(&dependency.group_id.unwrap_or(ZERO_DIGEST));
    push_u64(&mut bytes, dependency.index_id.unwrap_or(0));
    push_u8(&mut bytes, dependency.target_kind_tag.unwrap_or(0));
    push_u8(&mut bytes, dependency.secondary_index_kind_tag.unwrap_or(0));
    push_u64(&mut bytes, dependency.fingerprint.unwrap_or(0));
    bytes
}

fn required_field<T>(value: Option<T>, name: &str) -> Result<T, EngineError> {
    value.ok_or_else(|| component_manifest_error(format!("missing {}", name)))
}

fn put_u8(hasher: &mut Sha256, value: u8) {
    hasher.update([value]);
}

fn put_u32(hasher: &mut Sha256, value: u32) {
    hasher.update(value.to_le_bytes());
}

fn put_u64(hasher: &mut Sha256, value: u64) {
    hasher.update(value.to_le_bytes());
}

fn put_bytes_with_len(hasher: &mut Sha256, bytes: &[u8]) {
    put_u64(hasher, bytes.len() as u64);
    hasher.update(bytes);
}

fn push_u8(bytes: &mut Vec<u8>, value: u8) {
    bytes.push(value);
}

fn push_u32(bytes: &mut Vec<u8>, value: u32) {
    bytes.extend_from_slice(&value.to_le_bytes());
}

fn push_u64(bytes: &mut Vec<u8>, value: u64) {
    bytes.extend_from_slice(&value.to_le_bytes());
}

fn component_manifest_error(message: impl Into<String>) -> EngineError {
    EngineError::ManifestError(message.into())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::segment_writer::SEGMENT_FORMAT_VERSION;
    use std::io::Read;
    use tempfile::tempdir;

    fn digest(seed: u8) -> ComponentDigest32 {
        [seed; 32]
    }

    fn known_component(
        kind: SegmentComponentKind,
        component_id: ComponentDigest32,
    ) -> SegmentComponentRecordV1 {
        let dependencies = vec![ComponentDependencyV1::SourceGroup {
            group: SegmentSourceGroupKind::NodeSource,
            group_id: digest(3),
        }];
        let dependency_digest = dependency_digest(&dependencies);
        SegmentComponentRecordV1 {
            component_id,
            kind,
            logical_format_version: 1,
            created_generation: 1,
            requirement: ComponentRequirement::Required,
            trust_class: ComponentTrustClass::CoreMaintainedIndex,
            handle: ComponentHandleV1::ExternalFile {
                relative_path: "component_payload.dat".to_string(),
                payload_offset: COMPONENT_IDENTITY_HEADER_LEN as u64,
                payload_len: 12,
            },
            payload_len: 12,
            payload_digest: Some(digest(9)),
            dependency_digest,
            dependencies,
            build_fingerprint: 11,
        }
    }

    fn manifest_with_components(
        components: Vec<SegmentComponentRecordV1>,
        unknown_optional_components: Vec<UnknownOptionalComponentRecordV1>,
    ) -> SegmentComponentManifestV1 {
        SegmentComponentManifestV1 {
            format_version: SEGMENT_COMPONENT_MANIFEST_PAYLOAD_VERSION,
            segment_format_version: 10,
            segment_id: 42,
            generation: 1,
            built_at_ms: 123,
            build_kind: SegmentComponentBuildKind::TestFixture,
            segment_data_id: digest(4),
            node_count: 2,
            edge_count: 3,
            components,
            unknown_optional_components,
        }
    }

    fn unknown_optional(
        tag: u32,
        index_id: Option<u64>,
        component_id: ComponentDigest32,
    ) -> UnknownOptionalComponentRecordV1 {
        let dependency_digest = dependency_digest(&[]);
        UnknownOptionalComponentRecordV1 {
            wire: SegmentComponentRecordWireV1 {
                component_id,
                kind_tag: tag,
                index_id,
                logical_format_version: 1,
                created_generation: 1,
                requirement_tag: 2,
                fallback_tag: ComponentFallbackClass::RecordScan.tag(),
                trust_class_tag: ComponentTrustClass::OptionalCandidateIndex.tag(),
                handle: ComponentHandleWireV1 {
                    handle_tag: 1,
                    relative_path: Some("future_sidecar.dat".to_string()),
                    payload_offset: COMPONENT_IDENTITY_HEADER_LEN as u64,
                    payload_len: 8,
                    container_component_id: None,
                    offset: 0,
                    len: 0,
                },
                payload_len: 8,
                payload_digest: Some(digest(8)),
                dependency_digest,
                dependencies: vec![],
                build_fingerprint: 5,
            },
        }
    }

    #[test]
    fn component_kind_tags_match_snapshot_and_are_unique() {
        let snapshot = vec![
            (SegmentComponentKind::NodeRecords, 1, None),
            (SegmentComponentKind::EdgeRecords, 2, None),
            (SegmentComponentKind::NodeMetadata, 3, None),
            (SegmentComponentKind::EdgeMetadata, 4, None),
            (SegmentComponentKind::Tombstones, 5, None),
            (SegmentComponentKind::KeyIndex, 6, None),
            (SegmentComponentKind::NodeLabelIndex, 7, None),
            (SegmentComponentKind::EdgeLabelIndex, 8, None),
            (SegmentComponentKind::EdgeTripleIndex, 9, None),
            (SegmentComponentKind::AdjOutIndex, 10, None),
            (SegmentComponentKind::AdjOutPostings, 11, None),
            (SegmentComponentKind::AdjInIndex, 12, None),
            (SegmentComponentKind::AdjInPostings, 13, None),
            (SegmentComponentKind::TimestampIndex, 14, None),
            (SegmentComponentKind::LegacyNodePropertyIndex, 15, None),
            (SegmentComponentKind::NodePropertyHashMetadata, 16, None),
            (
                SegmentComponentKind::NodePropertyEqualityIndex { index_id: 91 },
                17,
                Some(91),
            ),
            (
                SegmentComponentKind::NodePropertyRangeIndex { index_id: 92 },
                18,
                Some(92),
            ),
            (SegmentComponentKind::EdgeWeightIndex, 19, None),
            (SegmentComponentKind::EdgeUpdatedAtIndex, 20, None),
            (SegmentComponentKind::EdgeValidFromIndex, 21, None),
            (SegmentComponentKind::EdgeValidToIndex, 22, None),
            (SegmentComponentKind::DegreeDelta, 23, None),
            (SegmentComponentKind::PlannerStats, 24, None),
            (SegmentComponentKind::NodeVectorMetadata, 25, None),
            (SegmentComponentKind::NodeDenseVectorBlob, 26, None),
            (SegmentComponentKind::NodeSparseVectorBlob, 27, None),
            (SegmentComponentKind::DenseHnswMetadata, 28, None),
            (SegmentComponentKind::DenseHnswGraph, 29, None),
            (SegmentComponentKind::SparsePostingIndex, 30, None),
            (SegmentComponentKind::SparsePostings, 31, None),
            (
                SegmentComponentKind::EdgePropertyEqualityIndex { index_id: 93 },
                32,
                Some(93),
            ),
            (
                SegmentComponentKind::EdgePropertyRangeIndex { index_id: 94 },
                33,
                Some(94),
            ),
            (SegmentComponentKind::PackedSegmentContainer, 34, None),
            (
                SegmentComponentKind::NodeCompoundEqualityIndex { index_id: 95 },
                35,
                Some(95),
            ),
            (
                SegmentComponentKind::NodeCompoundRangeIndex { index_id: 96 },
                36,
                Some(96),
            ),
            (
                SegmentComponentKind::EdgeCompoundEqualityIndex { index_id: 97 },
                37,
                Some(97),
            ),
            (
                SegmentComponentKind::EdgeCompoundRangeIndex { index_id: 98 },
                38,
                Some(98),
            ),
        ];
        let mut tags = HashSet::new();
        for (kind, expected_tag, expected_index_id) in snapshot {
            assert_eq!(kind.kind_tag(), expected_tag);
            assert_eq!(kind.index_id(), expected_index_id);
            assert!(tags.insert(expected_tag));
            assert_eq!(
                SegmentComponentKind::from_tag_and_index(expected_tag, expected_index_id)
                    .unwrap()
                    .unwrap(),
                kind
            );
        }
    }

    #[test]
    fn indexed_kind_manifest_round_trip_preserves_index_id() {
        let manifest = manifest_with_components(
            vec![
                known_component(
                    SegmentComponentKind::NodePropertyEqualityIndex { index_id: 77 },
                    digest(1),
                ),
                known_component(
                    SegmentComponentKind::NodeCompoundRangeIndex { index_id: 88 },
                    digest(2),
                ),
            ],
            vec![],
        );
        let decoded =
            decode_manifest_envelope(&encode_manifest_envelope(&manifest).unwrap()).unwrap();
        assert_eq!(
            decoded.components[0].kind,
            SegmentComponentKind::NodePropertyEqualityIndex { index_id: 77 }
        );
        assert_eq!(
            decoded.components[1].kind,
            SegmentComponentKind::NodeCompoundRangeIndex { index_id: 88 }
        );
    }

    #[test]
    fn manifest_envelope_round_trips_and_rejects_malformed_inputs() {
        let manifest = manifest_with_components(
            vec![known_component(SegmentComponentKind::KeyIndex, digest(1))],
            vec![],
        );
        let encoded = encode_manifest_envelope(&manifest).unwrap();
        assert_eq!(decode_manifest_envelope(&encoded).unwrap(), manifest);

        let mut bad_magic = encoded.clone();
        bad_magic[0] ^= 1;
        assert!(decode_manifest_envelope(&bad_magic)
            .unwrap_err()
            .to_string()
            .contains("magic"));

        let mut bad_version = encoded.clone();
        bad_version[8..12].copy_from_slice(&99u32.to_le_bytes());
        assert!(decode_manifest_envelope(&bad_version)
            .unwrap_err()
            .to_string()
            .contains("version"));

        let mut bad_crc = encoded.clone();
        let last = bad_crc.len() - 1;
        bad_crc[last] ^= 1;
        assert!(decode_manifest_envelope(&bad_crc)
            .unwrap_err()
            .to_string()
            .contains("crc"));

        let mut bad_len = encoded;
        bad_len[16..24].copy_from_slice(&1u64.to_le_bytes());
        assert!(decode_manifest_envelope(&bad_len)
            .unwrap_err()
            .to_string()
            .contains("length"));
    }

    #[test]
    fn manifest_envelope_future_segment_format_version_decoded_correctly() {
        let mut manifest = manifest_with_components(
            vec![known_component(SegmentComponentKind::KeyIndex, digest(1))],
            vec![],
        );
        manifest.segment_format_version = 99;
        let encoded = encode_manifest_envelope(&manifest).unwrap();
        let version_in_envelope = u32::from_le_bytes(encoded[12..16].try_into().unwrap());
        assert_eq!(version_in_envelope, 99);
        let decoded = decode_manifest_envelope(&encoded).unwrap();
        assert_eq!(decoded.segment_format_version, 99);
    }

    #[test]
    fn manifest_rejects_duplicate_component_ids_and_known_kinds() {
        let duplicate_id = manifest_with_components(
            vec![
                known_component(SegmentComponentKind::KeyIndex, digest(1)),
                known_component(SegmentComponentKind::NodeLabelIndex, digest(1)),
            ],
            vec![],
        );
        assert!(encode_manifest_envelope(&duplicate_id)
            .unwrap_err()
            .to_string()
            .contains("duplicate component id"));

        let duplicate_kind = manifest_with_components(
            vec![
                known_component(SegmentComponentKind::KeyIndex, digest(1)),
                known_component(SegmentComponentKind::KeyIndex, digest(2)),
            ],
            vec![],
        );
        assert!(encode_manifest_envelope(&duplicate_kind)
            .unwrap_err()
            .to_string()
            .contains("duplicate known"));
    }

    #[test]
    fn manifest_rejects_loose_wire_contract_records() {
        let mut bad_version = manifest_with_components(
            vec![known_component(SegmentComponentKind::KeyIndex, digest(1))],
            vec![],
        );
        bad_version.format_version = SEGMENT_COMPONENT_MANIFEST_PAYLOAD_VERSION + 1;
        assert!(encode_manifest_envelope(&bad_version)
            .unwrap_err()
            .to_string()
            .contains("payload version"));

        let mut bad_payload_len = manifest_with_components(
            vec![known_component(SegmentComponentKind::KeyIndex, digest(1))],
            vec![],
        );
        bad_payload_len.components[0].payload_len += 1;
        assert!(encode_manifest_envelope(&bad_payload_len)
            .unwrap_err()
            .to_string()
            .contains("payload length"));

        let mut bad_dependency_digest = manifest_with_components(
            vec![known_component(SegmentComponentKind::KeyIndex, digest(1))],
            vec![],
        );
        bad_dependency_digest.components[0].dependency_digest = digest(99);
        assert!(encode_manifest_envelope(&bad_dependency_digest)
            .unwrap_err()
            .to_string()
            .contains("dependency digest"));

        let mut bad_unknown_path = unknown_optional(5000, Some(6), digest(2));
        bad_unknown_path.wire.handle.relative_path = Some("a/../b".to_string());
        let manifest = manifest_with_components(vec![], vec![bad_unknown_path]);
        assert!(encode_manifest_envelope(&manifest)
            .unwrap_err()
            .to_string()
            .contains("normalized"));

        let mut bad_unknown_dependency = unknown_optional(5001, None, digest(3));
        bad_unknown_dependency.wire.dependency_digest = digest(88);
        let manifest = manifest_with_components(vec![], vec![bad_unknown_dependency]);
        assert!(encode_manifest_envelope(&manifest)
            .unwrap_err()
            .to_string()
            .contains("dependency digest"));
    }

    #[test]
    fn unknown_optional_records_are_preserved_without_known_duplicate_collision() {
        let manifest = manifest_with_components(
            vec![known_component(SegmentComponentKind::KeyIndex, digest(1))],
            vec![unknown_optional(5000, Some(6), digest(2))],
        );
        let decoded =
            decode_manifest_envelope(&encode_manifest_envelope(&manifest).unwrap()).unwrap();
        assert_eq!(decoded.components.len(), 1);
        assert_eq!(decoded.unknown_optional_components.len(), 1);
        assert_eq!(decoded.unknown_optional_components[0].wire.kind_tag, 5000);
        assert_eq!(
            decoded.unknown_optional_components[0].wire.index_id,
            Some(6)
        );
    }

    #[test]
    fn duplicate_unknown_optional_key_is_rejected() {
        let manifest = manifest_with_components(
            vec![],
            vec![
                unknown_optional(5000, Some(6), digest(2)),
                unknown_optional(5000, Some(6), digest(3)),
            ],
        );
        assert!(encode_manifest_envelope(&manifest)
            .unwrap_err()
            .to_string()
            .contains("duplicate unknown"));
    }

    #[test]
    fn unknown_required_record_fails_decode_before_runtime_use() {
        let wire = SegmentComponentManifestWireV1 {
            format_version: 1,
            segment_format_version: 10,
            segment_id: 42,
            generation: 1,
            built_at_ms: 123,
            build_kind_tag: SegmentComponentBuildKind::TestFixture.tag(),
            segment_data_id: digest(4),
            node_count: 2,
            edge_count: 3,
            components: vec![SegmentComponentRecordWireV1 {
                requirement_tag: 1,
                fallback_tag: 0,
                ..unknown_optional(5000, None, digest(1)).wire
            }],
        };
        let payload = rmp_serde::to_vec(&wire).unwrap();
        let mut crc = Crc32Hasher::new();
        crc.update(&payload);
        let mut data = Vec::new();
        data.extend_from_slice(&SEGMENT_COMPONENT_MANIFEST_MAGIC);
        data.extend_from_slice(&SEGMENT_COMPONENT_MANIFEST_ENVELOPE_VERSION.to_le_bytes());
        data.extend_from_slice(&SEGMENT_FORMAT_VERSION.to_le_bytes());
        data.extend_from_slice(&(payload.len() as u64).to_le_bytes());
        data.extend_from_slice(&crc.finalize().to_le_bytes());
        data.extend_from_slice(&payload);
        assert!(decode_manifest_envelope(&data)
            .unwrap_err()
            .to_string()
            .contains("unknown required"));
    }

    #[test]
    fn relative_path_validation_allows_nested_sidecar_and_rejects_unsafe_paths() {
        validate_relative_component_path("secondary_indexes/node_prop_eq_1.dat").unwrap();
        for invalid in [
            "",
            ".",
            "..",
            "a/../b",
            "/absolute",
            "secondary_indexes/",
            "secondary_indexes\\node_prop_eq_1.dat",
            "secondary_indexes/\u{1f}.dat",
        ] {
            assert!(
                validate_relative_component_path(invalid).is_err(),
                "{invalid:?} should be rejected"
            );
        }
    }

    #[test]
    fn identity_header_round_trips_and_rejects_mismatch() {
        let header = ComponentIdentityHeaderV1 {
            segment_format_version: 10,
            segment_id: 42,
            component_kind: SegmentComponentKind::KeyIndex,
            logical_format_version: 1,
            created_generation: 2,
            payload_offset: COMPONENT_IDENTITY_HEADER_LEN as u64,
            payload_len: 12,
            component_id: digest(1),
            dependency_digest: digest(2),
            build_fingerprint: 3,
            payload_digest: Some(digest(4)),
        };
        let encoded = encode_identity_header(&header);
        assert_eq!(decode_identity_header(&encoded).unwrap(), header);
        let mut bad_magic = encoded;
        bad_magic[0] ^= 1;
        assert!(decode_identity_header(&bad_magic)
            .unwrap_err()
            .to_string()
            .contains("magic"));
    }

    #[test]
    fn indexed_identity_header_round_trips_and_rejects_stale_flag_bytes() {
        let indexed_header = ComponentIdentityHeaderV1 {
            segment_format_version: 10,
            segment_id: 42,
            component_kind: SegmentComponentKind::NodePropertyEqualityIndex { index_id: 77 },
            logical_format_version: 1,
            created_generation: 2,
            payload_offset: COMPONENT_IDENTITY_HEADER_LEN as u64,
            payload_len: 12,
            component_id: digest(1),
            dependency_digest: digest(2),
            build_fingerprint: 3,
            payload_digest: Some(digest(4)),
        };
        let encoded = encode_identity_header(&indexed_header);
        assert_eq!(decode_identity_header(&encoded).unwrap(), indexed_header);

        let no_digest_header = ComponentIdentityHeaderV1 {
            payload_digest: None,
            ..indexed_header
        };
        let mut stale_digest_bytes = encode_identity_header(&no_digest_header);
        stale_digest_bytes[136] = 1;
        assert!(decode_identity_header(&stale_digest_bytes)
            .unwrap_err()
            .to_string()
            .contains("digest bytes"));

        let mut stale_index_bytes = encode_identity_header(&ComponentIdentityHeaderV1 {
            component_kind: SegmentComponentKind::KeyIndex,
            payload_digest: None,
            ..no_digest_header
        });
        stale_index_bytes[168..176].copy_from_slice(&77u64.to_le_bytes());
        assert!(decode_identity_header(&stale_index_bytes)
            .unwrap_err()
            .to_string()
            .contains("index_id bytes"));
    }

    #[test]
    fn dependency_digest_sorts_deterministically() {
        let a = ComponentDependencyV1::SourceGroup {
            group: SegmentSourceGroupKind::NodeSource,
            group_id: digest(1),
        };
        let b = ComponentDependencyV1::WriterBuildParams { fingerprint: 9 };
        assert_eq!(
            dependency_digest(&[a.clone(), b.clone()]),
            dependency_digest(&[b, a])
        );

        let same_shape_a = ComponentDependencyV1::SourceGroup {
            group: SegmentSourceGroupKind::NodeSource,
            group_id: digest(11),
        };
        let same_shape_b = ComponentDependencyV1::SourceGroup {
            group: SegmentSourceGroupKind::NodeSource,
            group_id: digest(12),
        };
        assert_eq!(
            dependency_digest(&[same_shape_a.clone(), same_shape_b.clone()]),
            dependency_digest(&[same_shape_b.clone(), same_shape_a.clone()])
        );
        assert_ne!(
            dependency_digest(&[same_shape_a.clone(), same_shape_a]),
            dependency_digest(&[same_shape_b.clone(), same_shape_b])
        );
    }

    #[test]
    fn source_group_digests_are_deterministic() {
        assert_eq!(
            digest_source_group(SegmentSourceGroupKind::NodeSource, &[digest(1), digest(2)]),
            digest_source_group(SegmentSourceGroupKind::NodeSource, &[digest(1), digest(2)])
        );
        assert_ne!(
            digest_source_group(SegmentSourceGroupKind::NodeSource, &[digest(1), digest(2)]),
            digest_source_group(SegmentSourceGroupKind::NodeSource, &[digest(2), digest(1)])
        );
    }

    #[test]
    fn packed_container_is_ignored_by_segment_data_source_groups() {
        let required = vec![
            known_component(SegmentComponentKind::NodeRecords, digest(1)),
            known_component(SegmentComponentKind::EdgeRecords, digest(2)),
            known_component(SegmentComponentKind::NodeMetadata, digest(3)),
            known_component(SegmentComponentKind::EdgeMetadata, digest(4)),
            known_component(SegmentComponentKind::Tombstones, digest(5)),
        ];
        let without_container = segment_source_groups_from_records(42, 2, 3, &required).unwrap();
        let mut with_container_records = required;
        with_container_records.push(known_component(
            SegmentComponentKind::PackedSegmentContainer,
            digest(99),
        ));
        let with_container =
            segment_source_groups_from_records(42, 2, 3, &with_container_records).unwrap();

        assert_eq!(
            without_container.segment_data_id,
            with_container.segment_data_id
        );
        assert_eq!(without_container.node_source, with_container.node_source);
        assert_eq!(without_container.edge_source, with_container.edge_source);
        assert_eq!(
            without_container.dense_vector_source,
            with_container.dense_vector_source
        );
        assert_eq!(
            without_container.sparse_vector_source,
            with_container.sparse_vector_source
        );
    }

    #[test]
    fn component_id_sensitivity_matches_policy() {
        let base = component_id(
            42,
            &SegmentComponentKind::KeyIndex,
            1,
            100,
            Some(&digest(1)),
            &digest(2),
            3,
        );
        assert_ne!(
            base,
            component_id(
                42,
                &SegmentComponentKind::KeyIndex,
                1,
                100,
                Some(&digest(1)),
                &digest(9),
                3,
            )
        );
        assert_ne!(
            base,
            component_id(
                42,
                &SegmentComponentKind::KeyIndex,
                1,
                100,
                Some(&digest(1)),
                &digest(2),
                9,
            )
        );
        assert_ne!(
            base,
            component_id(
                42,
                &SegmentComponentKind::KeyIndex,
                1,
                100,
                Some(&digest(9)),
                &digest(2),
                3,
            )
        );
        assert_eq!(
            base,
            component_id(
                42,
                &SegmentComponentKind::KeyIndex,
                1,
                100,
                Some(&digest(1)),
                &digest(2),
                3,
            )
        );
    }

    #[test]
    fn semantic_fingerprints_are_nonzero_stable_and_field_sensitive() {
        let planner = component_semantic_fingerprint("planner_stats", &[1, 2, 3, 4]);
        let sparse = component_semantic_fingerprint("sparse_postings", &[1, 2, 3, 4]);
        let dense = component_semantic_fingerprint("dense_hnsw", &[64, 1, 16, 200]);
        assert_ne!(planner, 0);
        assert_eq!(
            planner,
            component_semantic_fingerprint("planner_stats", &[1, 2, 3, 4])
        );
        assert_ne!(
            planner,
            component_semantic_fingerprint("planner_stats", &[1, 2, 3, 5])
        );
        assert_ne!(planner, sparse);
        assert_ne!(dense, 0);
    }

    #[test]
    fn declaration_fingerprint_changes_for_declared_index_fields() {
        let base = secondary_index_declaration_fingerprint(
            1,
            SecondaryIndexTargetKindForComponents::Node,
            10,
            b"email",
            SecondaryIndexKindFingerprint::Equality,
            0,
            7,
            3,
        );
        assert_ne!(base, 0);
        assert_eq!(
            base,
            secondary_index_declaration_fingerprint(
                1,
                SecondaryIndexTargetKindForComponents::Node,
                10,
                b"email",
                SecondaryIndexKindFingerprint::Equality,
                0,
                7,
                3,
            )
        );
        assert_ne!(
            base,
            secondary_index_declaration_fingerprint(
                2,
                SecondaryIndexTargetKindForComponents::Node,
                10,
                b"email",
                SecondaryIndexKindFingerprint::Equality,
                0,
                7,
                3,
            )
        );
        assert_ne!(
            base,
            secondary_index_declaration_fingerprint(
                1,
                SecondaryIndexTargetKindForComponents::Edge,
                10,
                b"email",
                SecondaryIndexKindFingerprint::Equality,
                0,
                7,
                3,
            )
        );
        assert_ne!(
            base,
            secondary_index_declaration_fingerprint(
                1,
                SecondaryIndexTargetKindForComponents::Node,
                11,
                b"email",
                SecondaryIndexKindFingerprint::Equality,
                0,
                7,
                3,
            )
        );
        assert_ne!(
            base,
            secondary_index_declaration_fingerprint(
                1,
                SecondaryIndexTargetKindForComponents::Node,
                10,
                b"email\0",
                SecondaryIndexKindFingerprint::Equality,
                0,
                7,
                3,
            )
        );
        assert_ne!(
            base,
            secondary_index_declaration_fingerprint(
                1,
                SecondaryIndexTargetKindForComponents::Node,
                10,
                b"email",
                SecondaryIndexKindFingerprint::Range,
                0,
                7,
                3,
            )
        );
        assert_ne!(
            base,
            secondary_index_declaration_fingerprint(
                1,
                SecondaryIndexTargetKindForComponents::Node,
                10,
                b"email",
                SecondaryIndexKindFingerprint::Equality,
                1,
                7,
                3,
            )
        );
        assert_ne!(
            base,
            secondary_index_declaration_fingerprint(
                1,
                SecondaryIndexTargetKindForComponents::Node,
                10,
                b"email",
                SecondaryIndexKindFingerprint::Equality,
                0,
                8,
                3,
            )
        );
        assert_ne!(
            base,
            secondary_index_declaration_fingerprint(
                1,
                SecondaryIndexTargetKindForComponents::Node,
                10,
                b"email",
                SecondaryIndexKindFingerprint::Equality,
                0,
                7,
                4,
            )
        );
    }

    #[test]
    fn tuple_declaration_fingerprint_uses_v2_field_identity() {
        let property_entry = SecondaryIndexManifestEntry {
            index_id: 7,
            target: SecondaryIndexTarget::NodeProperty {
                label_id: 3,
                prop_key: "updated_at".to_string(),
            },
            kind: SecondaryIndexKind::Equality,
            state: crate::types::SecondaryIndexState::Ready,
            last_error: None,
        };
        let field_entry = SecondaryIndexManifestEntry {
            index_id: 7,
            target: SecondaryIndexTarget::NodeFieldIndex {
                label_id: 3,
                fields: vec![SecondaryIndexFieldManifest::Property {
                    key: "updated_at".to_string(),
                }],
            },
            kind: SecondaryIndexKind::Equality,
            state: crate::types::SecondaryIndexState::Ready,
            last_error: None,
        };
        let metadata_entry = SecondaryIndexManifestEntry {
            index_id: 7,
            target: SecondaryIndexTarget::NodeFieldIndex {
                label_id: 3,
                fields: vec![SecondaryIndexFieldManifest::NodeMetadata {
                    field: crate::types::NodeMetadataIndexFieldManifest::UpdatedAt,
                }],
            },
            kind: SecondaryIndexKind::Equality,
            state: crate::types::SecondaryIndexState::Ready,
            last_error: None,
        };

        let property_fingerprint =
            secondary_index_declaration_fingerprint_for_entry(&property_entry);
        let field_fingerprint = secondary_index_declaration_fingerprint_for_entry(&field_entry);
        let metadata_fingerprint =
            secondary_index_declaration_fingerprint_for_entry(&metadata_entry);
        assert_ne!(property_fingerprint, field_fingerprint);
        assert_ne!(field_fingerprint, metadata_fingerprint);
        assert_eq!(
            field_fingerprint,
            secondary_index_declaration_fingerprint_for_entry(&field_entry)
        );

        let ComponentDependencyV1::SecondaryIndexDeclaration { fingerprint, .. } =
            secondary_declaration_dependency(&field_entry)
        else {
            panic!("secondary declaration dependency expected");
        };
        assert_eq!(fingerprint, field_fingerprint);
    }

    #[test]
    fn tuple_declaration_fingerprint_golden_snapshots() {
        let property_only = SecondaryIndexManifestEntry {
            index_id: 11,
            target: SecondaryIndexTarget::NodeFieldIndex {
                label_id: 3,
                fields: vec![SecondaryIndexFieldManifest::Property {
                    key: "tenant".to_string(),
                }],
            },
            kind: crate::types::SecondaryIndexKind::Equality,
            state: crate::types::SecondaryIndexState::Ready,
            last_error: None,
        };
        let metadata_only = SecondaryIndexManifestEntry {
            index_id: 12,
            target: SecondaryIndexTarget::NodeFieldIndex {
                label_id: 3,
                fields: vec![SecondaryIndexFieldManifest::NodeMetadata {
                    field: crate::types::NodeMetadataIndexFieldManifest::UpdatedAt,
                }],
            },
            kind: crate::types::SecondaryIndexKind::Equality,
            state: crate::types::SecondaryIndexState::Ready,
            last_error: None,
        };
        let mixed_equality = SecondaryIndexManifestEntry {
            index_id: 13,
            target: SecondaryIndexTarget::NodeFieldIndex {
                label_id: 3,
                fields: vec![
                    SecondaryIndexFieldManifest::Property {
                        key: "tenant".to_string(),
                    },
                    SecondaryIndexFieldManifest::NodeMetadata {
                        field: crate::types::NodeMetadataIndexFieldManifest::UpdatedAt,
                    },
                ],
            },
            kind: crate::types::SecondaryIndexKind::Equality,
            state: crate::types::SecondaryIndexState::Ready,
            last_error: None,
        };
        let mixed_range = SecondaryIndexManifestEntry {
            kind: crate::types::SecondaryIndexKind::Range,
            ..mixed_equality.clone()
        };

        let snapshots = [
            (&property_only, 7885699672810247875u64),
            (&metadata_only, 16445049732461440943u64),
            (&mixed_equality, 8860785994094260266u64),
            (&mixed_range, 6677797123317768889u64),
        ];
        for (entry, expected) in snapshots {
            assert_eq!(
                secondary_index_declaration_fingerprint_for_entry(entry),
                expected
            );
        }
    }

    #[test]
    fn tuple_component_source_dependencies_match_spec() {
        let source_groups = SegmentComponentSourceGroups {
            node_source: digest(1),
            edge_source: digest(2),
            node_property_content_source: digest(3),
            node_property_hash_source: digest(4),
            edge_metadata_source: digest(5),
            degree_source: digest(6),
            dense_vector_source: digest(7),
            sparse_vector_source: digest(8),
            segment_data_id: digest(9),
        };
        let node_metadata_only = SecondaryIndexManifestEntry {
            index_id: 1,
            target: SecondaryIndexTarget::NodeFieldIndex {
                label_id: 1,
                fields: vec![SecondaryIndexFieldManifest::NodeMetadata {
                    field: crate::types::NodeMetadataIndexFieldManifest::UpdatedAt,
                }],
            },
            kind: SecondaryIndexKind::Range,
            state: crate::types::SecondaryIndexState::Ready,
            last_error: None,
        };
        let edge_metadata_only = SecondaryIndexManifestEntry {
            index_id: 2,
            target: SecondaryIndexTarget::EdgeFieldIndex {
                label_id: 1,
                fields: vec![SecondaryIndexFieldManifest::EdgeMetadata {
                    field: crate::types::EdgeMetadataIndexFieldManifest::UpdatedAt,
                }],
            },
            kind: SecondaryIndexKind::Range,
            state: crate::types::SecondaryIndexState::Ready,
            last_error: None,
        };
        let edge_mixed = SecondaryIndexManifestEntry {
            index_id: 3,
            target: SecondaryIndexTarget::EdgeFieldIndex {
                label_id: 1,
                fields: vec![
                    SecondaryIndexFieldManifest::EdgeMetadata {
                        field: crate::types::EdgeMetadataIndexFieldManifest::UpdatedAt,
                    },
                    SecondaryIndexFieldManifest::Property {
                        key: "status".to_string(),
                    },
                ],
            },
            kind: SecondaryIndexKind::Equality,
            state: crate::types::SecondaryIndexState::Ready,
            last_error: None,
        };

        let node_deps =
            secondary_index_component_dependencies_for_entry(&node_metadata_only, &source_groups);
        assert!(node_deps.contains(&source_group_dependency(
            SegmentSourceGroupKind::NodePropertyContentSource,
            source_groups.node_property_content_source,
        )));

        let edge_meta_deps =
            secondary_index_component_dependencies_for_entry(&edge_metadata_only, &source_groups);
        assert!(edge_meta_deps.contains(&source_group_dependency(
            SegmentSourceGroupKind::EdgeMetadataSource,
            source_groups.edge_metadata_source,
        )));

        let edge_mixed_deps =
            secondary_index_component_dependencies_for_entry(&edge_mixed, &source_groups);
        assert!(edge_mixed_deps.contains(&source_group_dependency(
            SegmentSourceGroupKind::EdgeSource,
            source_groups.edge_source,
        )));
    }

    #[test]
    fn component_identity_writer_is_single_pass_and_unwrapped_from_production_paths() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("planner_stats.dat");
        let mut writer = ComponentIdentityWriter::create(
            &path,
            "planner_stats.dat".to_string(),
            10,
            42,
            SegmentComponentKind::PlannerStats,
            1,
            1,
            ComponentRequirement::Optional {
                fallback: ComponentFallbackClass::PlannerStatsUnavailable,
            },
            ComponentTrustClass::OptionalAdvisoryStats,
            17,
            true,
        )
        .unwrap();
        writer.write_all(b"hello").unwrap();
        writer.write_all(b" world").unwrap();
        let dependencies = vec![ComponentDependencyV1::SourceGroup {
            group: SegmentSourceGroupKind::NodeSource,
            group_id: digest(44),
        }];
        let expected_dependency_digest = dependency_digest(&dependencies);
        let record = writer.finish(dependencies).unwrap();
        assert_eq!(record.payload_len, 11);
        assert_eq!(record.dependency_digest, expected_dependency_digest);
        assert_eq!(
            record.handle,
            ComponentHandleV1::ExternalFile {
                relative_path: "planner_stats.dat".to_string(),
                payload_offset: COMPONENT_IDENTITY_HEADER_LEN as u64,
                payload_len: 11,
            }
        );
        let mut bytes = Vec::new();
        File::open(&path).unwrap().read_to_end(&mut bytes).unwrap();
        assert_eq!(&bytes[COMPONENT_IDENTITY_HEADER_LEN..], b"hello world");
        let header = decode_identity_header(&bytes[..COMPONENT_IDENTITY_HEADER_LEN]).unwrap();
        assert_eq!(header.component_id, record.component_id);
        assert_eq!(header.payload_digest, record.payload_digest);
    }

    fn packed_container_record(payload_len: u64) -> SegmentComponentRecordV1 {
        let dependencies = Vec::new();
        let dependency_digest = dependency_digest(&dependencies);
        let payload_digest = digest(88);
        SegmentComponentRecordV1 {
            component_id: digest(77),
            kind: SegmentComponentKind::PackedSegmentContainer,
            logical_format_version: 1,
            created_generation: 1,
            requirement: ComponentRequirement::Required,
            trust_class: ComponentTrustClass::AuxiliaryBlob,
            handle: ComponentHandleV1::ExternalFile {
                relative_path: PACKED_CORE_FILENAME.to_string(),
                payload_offset: COMPONENT_IDENTITY_HEADER_LEN as u64,
                payload_len,
            },
            payload_len,
            payload_digest: Some(payload_digest),
            dependency_digest,
            dependencies,
            build_fingerprint: 101,
        }
    }

    fn packed_record(
        kind: SegmentComponentKind,
        offset: u64,
        len: u64,
    ) -> SegmentComponentRecordV1 {
        let dependencies = Vec::new();
        let dependency_digest = dependency_digest(&dependencies);
        let payload_digest = digest(kind.kind_tag() as u8);
        let computed_component_id = component_id(
            42,
            &kind,
            1,
            len,
            Some(&payload_digest),
            &dependency_digest,
            11,
        );
        SegmentComponentRecordV1 {
            component_id: computed_component_id,
            kind,
            logical_format_version: 1,
            created_generation: 1,
            requirement: ComponentRequirement::Required,
            trust_class: ComponentTrustClass::CoreMaintainedIndex,
            handle: ComponentHandleV1::PackedRange {
                container_component_id: digest(77),
                offset,
                len,
            },
            payload_len: len,
            payload_digest: Some(payload_digest),
            dependency_digest,
            dependencies,
            build_fingerprint: 11,
        }
    }

    #[test]
    fn packed_range_wire_roundtrip_preserves_fields() {
        let mut record = known_component(SegmentComponentKind::KeyIndex, digest(9));
        record.handle = ComponentHandleV1::PackedRange {
            container_component_id: digest(7),
            offset: 24,
            len: 12,
        };
        record.payload_len = 12;
        let manifest = manifest_with_components(vec![record], vec![]);
        let decoded =
            decode_manifest_envelope(&encode_manifest_envelope(&manifest).unwrap()).unwrap();
        assert_eq!(
            decoded.components[0].handle,
            ComponentHandleV1::PackedRange {
                container_component_id: digest(7),
                offset: 24,
                len: 12,
            }
        );
    }

    #[test]
    fn packed_component_id_ignores_physical_handle() {
        let kind = SegmentComponentKind::NodeRecords;
        let dependencies = Vec::new();
        let dependency_digest = dependency_digest(&dependencies);
        let payload_digest = digest(5);
        let computed_component_id = component_id(
            42,
            &kind,
            1,
            99,
            Some(&payload_digest),
            &dependency_digest,
            11,
        );
        let external = SegmentComponentRecordV1 {
            component_id: computed_component_id,
            kind: kind.clone(),
            logical_format_version: 1,
            created_generation: 1,
            requirement: ComponentRequirement::Required,
            trust_class: ComponentTrustClass::PrimaryData,
            handle: ComponentHandleV1::ExternalFile {
                relative_path: "external_component_payload.dat".to_string(),
                payload_offset: COMPONENT_IDENTITY_HEADER_LEN as u64,
                payload_len: 99,
            },
            payload_len: 99,
            payload_digest: Some(payload_digest),
            dependency_digest,
            dependencies: dependencies.clone(),
            build_fingerprint: 11,
        };
        let mut packed = external.clone();
        packed.handle = ComponentHandleV1::PackedRange {
            container_component_id: digest(7),
            offset: 128,
            len: 99,
        };
        assert_ne!(external.handle, packed.handle);
        assert_eq!(external.component_id, packed.component_id);
        assert_eq!(
            packed.component_id,
            component_id(
                42,
                &packed.kind,
                packed.logical_format_version,
                packed.payload_len,
                packed.payload_digest.as_ref(),
                &packed.dependency_digest,
                packed.build_fingerprint,
            )
        );
    }

    #[test]
    fn packed_manifest_validation_rejects_missing_container_length_and_overlap() {
        let node = packed_record(SegmentComponentKind::NodeRecords, 0, 16);
        let manifest = manifest_with_components(vec![node.clone()], vec![]);
        assert!(validate_packed_core_manifest_contract(&manifest)
            .unwrap_err()
            .to_string()
            .contains("without segment.core container"));

        let mut bad_len = manifest_with_components(vec![packed_container_record(64), node], vec![]);
        bad_len.components[1].payload_len = 15;
        assert!(validate_packed_core_manifest_contract(&bad_len)
            .unwrap_err()
            .to_string()
            .contains("payload length"));

        let overflow = manifest_with_components(
            vec![
                packed_container_record(64),
                packed_record(SegmentComponentKind::NodeRecords, u64::MAX - 1, 8),
            ],
            vec![],
        );
        assert!(validate_packed_core_manifest_contract(&overflow)
            .unwrap_err()
            .to_string()
            .contains("overflows"));

        let overlap = manifest_with_components(
            vec![
                packed_container_record(64),
                packed_record(SegmentComponentKind::NodeRecords, 0, 16),
                packed_record(SegmentComponentKind::EdgeRecords, 8, 16),
            ],
            vec![],
        );
        assert!(validate_packed_core_manifest_contract(&overlap)
            .unwrap_err()
            .to_string()
            .contains("overlap"));
    }

    #[test]
    fn packed_manifest_validation_allows_zero_length_shared_offsets() {
        let manifest = manifest_with_components(
            vec![
                packed_container_record(64),
                packed_record(SegmentComponentKind::NodeRecords, 0, 0),
                packed_record(SegmentComponentKind::EdgeRecords, 0, 0),
            ],
            vec![],
        );
        validate_packed_core_manifest_contract(&manifest).unwrap();
    }

    #[test]
    fn packed_manifest_validation_rejects_optional_range_overlap() {
        let mut optional = packed_record(SegmentComponentKind::EdgeWeightIndex, 8, 16);
        optional.requirement = ComponentRequirement::Optional {
            fallback: ComponentFallbackClass::MetadataScan,
        };
        optional.trust_class = ComponentTrustClass::OptionalCandidateIndex;

        let overlap = manifest_with_components(
            vec![
                packed_container_record(64),
                packed_record(SegmentComponentKind::NodeRecords, 0, 16),
                optional,
            ],
            vec![],
        );

        assert!(validate_packed_core_manifest_contract(&overlap)
            .unwrap_err()
            .to_string()
            .contains("overlap"));
    }
}
