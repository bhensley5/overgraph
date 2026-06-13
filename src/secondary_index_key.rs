use crate::error::EngineError;
use crate::property_value_semantics::{
    hash_semantic_equality_key_bytes, numeric_range_sort_key_for_value,
    semantic_equality_key_bytes, validate_numeric_range_sidecar_key, NUMERIC_RANGE_KEY_BYTES,
};
use crate::types::{
    edge_metadata_index_field_name, fnv1a, node_metadata_index_field_name,
    EdgeMetadataIndexFieldManifest, NodeMetadataIndexFieldManifest, PropValue, SecondaryIndexField,
    SecondaryIndexFieldManifest, SecondaryIndexKind, SecondaryIndexManifestEntry,
    SecondaryIndexTarget,
};
use crc32fast::Hasher as Crc32Hasher;
use std::collections::BTreeMap;
use std::io::Write;

pub(crate) const COMPOUND_INDEX_KEY_ENCODING_VERSION: u16 = 1;
pub(crate) const COMPOUND_INDEX_SENTINEL_ORDERING_VERSION: u16 = 1;
pub(crate) const COMPOUND_INDEX_METADATA_ENUM_VERSION: u16 = 1;
pub(crate) const MAX_SECONDARY_INDEX_FIELDS: usize = 8;
pub(crate) const MAX_COMPOUND_COMPONENT_BYTES: usize = 1024;
pub(crate) const MAX_COMPOUND_TUPLE_BYTES: usize = 4096;
#[allow(dead_code)]
pub(crate) const MAX_COMPOUND_INDEX_IN_EXPANSIONS: usize = 64;

/// Stable failure-message prefix for compound declarations (DEC-37-029).
/// Every failure recorded on a compound declaration carries this prefix
/// exactly once; use the helpers below instead of formatting it inline.
pub(crate) const COMPOUND_SECONDARY_UNAVAILABLE_PREFIX: &str =
    "compound secondary index unavailable:";

pub(crate) fn compound_secondary_failure_message_from_str(message: &str) -> String {
    let message = message.trim();
    if let Some(prefix_pos) = message.rfind(COMPOUND_SECONDARY_UNAVAILABLE_PREFIX) {
        message[prefix_pos..].to_string()
    } else {
        format!("{COMPOUND_SECONDARY_UNAVAILABLE_PREFIX} {message}")
    }
}

pub(crate) fn compound_secondary_failure_message(error: &EngineError) -> String {
    compound_secondary_failure_message_from_str(&error.to_string())
}

pub(crate) const COMPOUND_COMPONENT_CLASS_MISSING: u8 = 0x01;
pub(crate) const COMPOUND_COMPONENT_CLASS_NULL: u8 = 0x02;
pub(crate) const COMPOUND_COMPONENT_CLASS_BOOL_FALSE: u8 = 0x03;
pub(crate) const COMPOUND_COMPONENT_CLASS_BOOL_TRUE: u8 = 0x04;
pub(crate) const COMPOUND_COMPONENT_CLASS_NUMERIC: u8 = 0x10;
pub(crate) const COMPOUND_COMPONENT_CLASS_STRING: u8 = 0x20;
pub(crate) const COMPOUND_COMPONENT_CLASS_BYTES: u8 = 0x30;
pub(crate) const COMPOUND_COMPONENT_CLASS_EQUALITY_HASH: u8 = 0x40;

pub(crate) const COMPOUND_SIDECAR_MAGIC: [u8; 8] = *b"OGCIX01\0";
pub(crate) const COMPOUND_SIDECAR_VERSION: u16 = 1;
pub(crate) const COMPOUND_SIDECAR_HEADER_LEN: usize = 120;

const COMPOUND_EQUALITY_HASH_DOMAIN: &[u8] = b"compound-equality-component-v1";
const COMPOUND_KEY_TABLE_ENTRY_BYTES: usize = 24;
const COMPOUND_SIDECAR_FLAG_HAS_METADATA: u16 = 0b0000_0001;
const COMPOUND_SIDECAR_FLAG_HAS_PROPERTY: u16 = 0b0000_0010;
const HEADER_CRC_OFFSET: usize = 100;
const HEADER_CRC_END: usize = 104;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum CompoundComponentClass {
    Missing,
    Null,
    BoolFalse,
    BoolTrue,
    Numeric,
    String,
    Bytes,
    EqualityHash,
}

impl CompoundComponentClass {
    pub(crate) fn tag(self) -> u8 {
        match self {
            Self::Missing => COMPOUND_COMPONENT_CLASS_MISSING,
            Self::Null => COMPOUND_COMPONENT_CLASS_NULL,
            Self::BoolFalse => COMPOUND_COMPONENT_CLASS_BOOL_FALSE,
            Self::BoolTrue => COMPOUND_COMPONENT_CLASS_BOOL_TRUE,
            Self::Numeric => COMPOUND_COMPONENT_CLASS_NUMERIC,
            Self::String => COMPOUND_COMPONENT_CLASS_STRING,
            Self::Bytes => COMPOUND_COMPONENT_CLASS_BYTES,
            Self::EqualityHash => COMPOUND_COMPONENT_CLASS_EQUALITY_HASH,
        }
    }

    fn from_tag(tag: u8) -> Result<Self, EngineError> {
        match tag {
            COMPOUND_COMPONENT_CLASS_MISSING => Ok(Self::Missing),
            COMPOUND_COMPONENT_CLASS_NULL => Ok(Self::Null),
            COMPOUND_COMPONENT_CLASS_BOOL_FALSE => Ok(Self::BoolFalse),
            COMPOUND_COMPONENT_CLASS_BOOL_TRUE => Ok(Self::BoolTrue),
            COMPOUND_COMPONENT_CLASS_NUMERIC => Ok(Self::Numeric),
            COMPOUND_COMPONENT_CLASS_STRING => Ok(Self::String),
            COMPOUND_COMPONENT_CLASS_BYTES => Ok(Self::Bytes),
            COMPOUND_COMPONENT_CLASS_EQUALITY_HASH => Ok(Self::EqualityHash),
            _ => Err(corrupt_compound_sidecar(format!(
                "compound tuple component has invalid class 0x{tag:02x}"
            ))),
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum CompoundSidecarTargetKind {
    Node,
    Edge,
}

impl CompoundSidecarTargetKind {
    pub(crate) fn tag(self) -> u8 {
        match self {
            Self::Node => 1,
            Self::Edge => 2,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum CompoundSidecarIndexKind {
    Equality,
    Range,
}

impl CompoundSidecarIndexKind {
    pub(crate) fn tag(self) -> u8 {
        match self {
            Self::Equality => 1,
            Self::Range => 2,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum CompoundFieldSource {
    Property,
    NodeMetadata,
    EdgeMetadata,
}

impl CompoundFieldSource {
    pub(crate) fn tag(self) -> u8 {
        match self {
            Self::Property => 1,
            Self::NodeMetadata => 2,
            Self::EdgeMetadata => 3,
        }
    }
}

#[derive(Clone, Copy, Debug)]
pub(crate) enum CompoundFieldValue<'a> {
    Property(Option<&'a PropValue>),
    MetadataString(&'a str),
    MetadataI64(i64),
    MetadataU64(u64),
    MetadataF64(f64),
}

#[derive(Clone, Debug)]
pub(crate) struct CompoundTupleContext<'a> {
    pub(crate) target_kind: CompoundSidecarTargetKind,
    pub(crate) target_label_id: u32,
    pub(crate) fields: &'a [SecondaryIndexFieldManifest],
}

impl<'a> CompoundTupleContext<'a> {
    pub(crate) fn from_manifest_entry(
        entry: &'a SecondaryIndexManifestEntry,
    ) -> Result<Self, EngineError> {
        match &entry.target {
            SecondaryIndexTarget::NodeFieldIndex { label_id, fields } => Ok(Self {
                target_kind: CompoundSidecarTargetKind::Node,
                target_label_id: *label_id,
                fields,
            }),
            SecondaryIndexTarget::EdgeFieldIndex { label_id, fields } => Ok(Self {
                target_kind: CompoundSidecarTargetKind::Edge,
                target_label_id: *label_id,
                fields,
            }),
            SecondaryIndexTarget::NodeProperty { .. } | SecondaryIndexTarget::EdgeProperty { .. } => {
                Err(EngineError::InvalidOperation(
                    "compound secondary index unavailable: single-property declarations use legacy sidecars"
                        .to_string(),
                ))
            }
        }
    }
}

#[derive(Clone, Debug)]
pub(crate) struct CompoundSidecarDeclaration<'a> {
    pub(crate) index_id: u64,
    pub(crate) target_kind: CompoundSidecarTargetKind,
    pub(crate) index_kind: CompoundSidecarIndexKind,
    pub(crate) declaration_fingerprint: u64,
    pub(crate) fields: &'a [SecondaryIndexFieldManifest],
}

impl<'a> CompoundSidecarDeclaration<'a> {
    pub(crate) fn from_manifest_entry(
        entry: &'a SecondaryIndexManifestEntry,
        declaration_fingerprint: u64,
    ) -> Result<Self, EngineError> {
        let (target_kind, fields) = match &entry.target {
            SecondaryIndexTarget::NodeFieldIndex { fields, .. } => {
                (CompoundSidecarTargetKind::Node, fields.as_slice())
            }
            SecondaryIndexTarget::EdgeFieldIndex { fields, .. } => {
                (CompoundSidecarTargetKind::Edge, fields.as_slice())
            }
            SecondaryIndexTarget::NodeProperty { .. }
            | SecondaryIndexTarget::EdgeProperty { .. } => {
                return Err(EngineError::InvalidOperation(
                    "compound secondary index unavailable: single-property declarations use legacy sidecars"
                        .to_string(),
                ));
            }
        };
        let index_kind = match entry.kind {
            SecondaryIndexKind::Equality => CompoundSidecarIndexKind::Equality,
            SecondaryIndexKind::Range => CompoundSidecarIndexKind::Range,
        };
        Ok(Self {
            index_id: entry.index_id,
            target_kind,
            index_kind,
            declaration_fingerprint,
            fields,
        })
    }

    fn flags(&self) -> u16 {
        let mut flags = 0u16;
        for field in self.fields {
            match field_source(field) {
                CompoundFieldSource::Property => flags |= COMPOUND_SIDECAR_FLAG_HAS_PROPERTY,
                CompoundFieldSource::NodeMetadata | CompoundFieldSource::EdgeMetadata => {
                    flags |= COMPOUND_SIDECAR_FLAG_HAS_METADATA
                }
            }
        }
        flags
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct DecodedCompoundComponent<'a> {
    pub(crate) class: CompoundComponentClass,
    pub(crate) payload: &'a [u8],
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct CompoundPrefixBounds {
    pub(crate) lower: Vec<u8>,
    pub(crate) upper_exclusive: Vec<u8>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct CompoundLowerBound {
    pub(crate) key: Vec<u8>,
    pub(crate) exclusive_component_prefix: bool,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct CompoundRangeBounds {
    pub(crate) lower: Option<CompoundLowerBound>,
    pub(crate) upper_exclusive: Vec<u8>,
}

pub(crate) fn encode_compound_tuple_key(
    context: &CompoundTupleContext<'_>,
    values: &[CompoundFieldValue<'_>],
) -> Result<Vec<u8>, EngineError> {
    if values.len() != context.fields.len() {
        return Err(EngineError::InvalidOperation(format!(
            "compound secondary index unavailable: tuple value count {} does not match field count {}",
            values.len(),
            context.fields.len()
        )));
    }
    encode_compound_components(context, values)
}

pub(crate) fn encode_compound_tuple_prefix(
    context: &CompoundTupleContext<'_>,
    values: &[CompoundFieldValue<'_>],
) -> Result<Vec<u8>, EngineError> {
    if values.len() > context.fields.len() {
        return Err(EngineError::InvalidOperation(format!(
            "compound secondary index unavailable: tuple prefix value count {} exceeds field count {}",
            values.len(),
            context.fields.len()
        )));
    }
    encode_compound_components(context, values)
}

pub(crate) fn encode_compound_field_component(
    context: &CompoundTupleContext<'_>,
    ordinal: usize,
    value: CompoundFieldValue<'_>,
) -> Result<Vec<u8>, EngineError> {
    let field = context.fields.get(ordinal).ok_or_else(|| {
        EngineError::InvalidOperation(format!(
            "compound secondary index unavailable: field ordinal {ordinal} exceeds declaration field count {}",
            context.fields.len()
        ))
    })?;
    encode_component(context, ordinal, field, value, false).map(|component| component.bytes)
}

pub(crate) fn decode_compound_tuple_components<'a>(
    data: &'a [u8],
    fields: &[SecondaryIndexFieldManifest],
) -> Result<Vec<DecodedCompoundComponent<'a>>, EngineError> {
    if fields.is_empty() || fields.len() > MAX_SECONDARY_INDEX_FIELDS {
        return Err(corrupt_compound_sidecar(format!(
            "compound tuple field count {} is outside 1..={MAX_SECONDARY_INDEX_FIELDS}",
            fields.len()
        )));
    }
    let mut offset = 0usize;
    let mut components = Vec::with_capacity(fields.len());
    for (ordinal, field) in fields.iter().enumerate() {
        let header = data.get(offset..offset + 3).ok_or_else(|| {
            corrupt_compound_sidecar(format!(
                "compound tuple component {ordinal} header exceeds key length {}",
                data.len()
            ))
        })?;
        let class = CompoundComponentClass::from_tag(header[0])?;
        let payload_len = u16::from_be_bytes([header[1], header[2]]) as usize;
        let payload_start = offset + 3;
        let payload_end = payload_start.checked_add(payload_len).ok_or_else(|| {
            corrupt_compound_sidecar("compound tuple component payload length overflow")
        })?;
        let payload = data.get(payload_start..payload_end).ok_or_else(|| {
            corrupt_compound_sidecar(format!(
                "compound tuple component {ordinal} payload exceeds key length {}",
                data.len()
            ))
        })?;
        validate_component_payload_for_field(class, payload, field, ordinal)?;
        components.push(DecodedCompoundComponent { class, payload });
        offset = payload_end;
    }
    if offset != data.len() {
        return Err(corrupt_compound_sidecar(format!(
            "compound tuple has trailing bytes after {} fields: decoded {}, key length {}",
            fields.len(),
            offset,
            data.len()
        )));
    }
    Ok(components)
}

pub(crate) fn compound_prefix_bounds(encoded_prefix: &[u8]) -> CompoundPrefixBounds {
    let mut lower = Vec::with_capacity(encoded_prefix.len());
    lower.extend_from_slice(encoded_prefix);
    let mut upper_exclusive = Vec::with_capacity(encoded_prefix.len() + 1);
    upper_exclusive.extend_from_slice(encoded_prefix);
    upper_exclusive.push(0xFF);
    CompoundPrefixBounds {
        lower,
        upper_exclusive,
    }
}

pub(crate) fn compound_range_bounds(
    equality_prefix: &[u8],
    lower_component: Option<(&[u8], bool)>,
    upper_component: Option<(&[u8], bool)>,
) -> Result<CompoundRangeBounds, EngineError> {
    let lower = match lower_component {
        Some((component, inclusive)) => {
            validate_numeric_range_bound_component(component, "lower")?;
            let mut key = Vec::with_capacity(equality_prefix.len() + component.len());
            key.extend_from_slice(equality_prefix);
            key.extend_from_slice(component);
            Some(CompoundLowerBound {
                key,
                exclusive_component_prefix: !inclusive,
            })
        }
        None => {
            let mut key = Vec::with_capacity(equality_prefix.len() + 1);
            key.extend_from_slice(equality_prefix);
            key.push(COMPOUND_COMPONENT_CLASS_NUMERIC);
            Some(CompoundLowerBound {
                key,
                exclusive_component_prefix: false,
            })
        }
    };
    let upper_exclusive = match upper_component {
        Some((component, inclusive)) => {
            validate_numeric_range_bound_component(component, "upper")?;
            let mut key = Vec::with_capacity(equality_prefix.len() + component.len() + 1);
            key.extend_from_slice(equality_prefix);
            key.extend_from_slice(component);
            if inclusive {
                key.push(0xFF);
            }
            key
        }
        None => {
            // Range predicates only match Numeric components, so an unbounded
            // upper stops at the end of the numeric class instead of walking
            // String/Bytes/EqualityHash suffixes under the same prefix.
            let mut key = Vec::with_capacity(equality_prefix.len() + 1);
            key.extend_from_slice(equality_prefix);
            key.push(COMPOUND_COMPONENT_CLASS_NUMERIC + 1);
            key
        }
    };
    Ok(CompoundRangeBounds {
        lower,
        upper_exclusive,
    })
}

fn validate_numeric_range_bound_component(
    component: &[u8],
    bound_name: &str,
) -> Result<(), EngineError> {
    if component.len() != 3 + NUMERIC_RANGE_KEY_BYTES {
        return Err(EngineError::InvalidOperation(format!(
            "compound secondary index unavailable: {bound_name} range bound must be exactly one numeric component"
        )));
    }
    if component[0] != COMPOUND_COMPONENT_CLASS_NUMERIC {
        return Err(EngineError::InvalidOperation(format!(
            "compound secondary index unavailable: {bound_name} range bound class 0x{:02x} is not numeric",
            component[0]
        )));
    }
    let payload_len = u16::from_be_bytes([component[1], component[2]]) as usize;
    if payload_len != NUMERIC_RANGE_KEY_BYTES {
        return Err(EngineError::InvalidOperation(format!(
            "compound secondary index unavailable: {bound_name} range bound numeric payload length {payload_len} != {NUMERIC_RANGE_KEY_BYTES}"
        )));
    }
    let bytes: [u8; NUMERIC_RANGE_KEY_BYTES] = component[3..3 + NUMERIC_RANGE_KEY_BYTES]
        .try_into()
        .unwrap();
    validate_numeric_range_sidecar_key(&bytes).map_err(|error| {
        EngineError::InvalidOperation(format!(
            "compound secondary index unavailable: {bound_name} range bound numeric component is invalid: {error}"
        ))
    })
}

pub(crate) fn write_compound_sidecar_payload(
    writer: &mut impl Write,
    declaration: &CompoundSidecarDeclaration<'_>,
    entries: &[(Vec<u8>, u64)],
) -> Result<(), EngineError> {
    let normalized = normalize_compound_sidecar_entries(declaration, entries)?;
    let mut key_bytes = Vec::new();
    let mut postings = Vec::new();
    let mut key_table_entries = Vec::with_capacity(normalized.len());
    for (key, ids) in &normalized {
        let key_offset = key_bytes.len() as u64;
        key_bytes.extend_from_slice(key);
        let postings_offset = postings.len() as u64;
        for &id in ids {
            postings.extend_from_slice(&id.to_le_bytes());
        }
        key_table_entries.push((
            key_offset,
            key.len() as u32,
            postings_offset,
            ids.len() as u32,
        ));
    }

    let key_table_offset = COMPOUND_SIDECAR_HEADER_LEN as u64;
    let key_table_len = (8 + key_table_entries.len() * COMPOUND_KEY_TABLE_ENTRY_BYTES) as u64;
    let key_bytes_offset = key_table_offset + key_table_len;
    let key_bytes_len = key_bytes.len() as u64;
    let postings_offset = align_u64(key_bytes_offset + key_bytes_len, 8)?;
    let postings_padding = (postings_offset - (key_bytes_offset + key_bytes_len)) as usize;
    let postings_len = postings.len() as u64;
    let file_len = postings_offset
        .checked_add(postings_len)
        .ok_or_else(|| corrupt_compound_sidecar("compound sidecar file length overflow"))?;

    let mut data = vec![0u8; COMPOUND_SIDECAR_HEADER_LEN];
    put_header(
        &mut data,
        declaration,
        normalized.len() as u64,
        (postings.len() / 8) as u64,
        key_table_offset,
        key_table_len,
        key_bytes_offset,
        key_bytes_len,
        postings_offset,
        postings_len,
        0,
    )?;
    data.extend_from_slice(&(normalized.len() as u64).to_le_bytes());
    for (key_offset, key_len, postings_offset, postings_count) in &key_table_entries {
        data.extend_from_slice(&key_offset.to_le_bytes());
        data.extend_from_slice(&key_len.to_le_bytes());
        data.extend_from_slice(&postings_offset.to_le_bytes());
        data.extend_from_slice(&postings_count.to_le_bytes());
    }
    data.extend_from_slice(&key_bytes);
    data.extend(std::iter::repeat_n(0, postings_padding));
    data.extend_from_slice(&postings);
    if data.len() as u64 != file_len {
        return Err(corrupt_compound_sidecar(
            "compound sidecar writer produced inconsistent file length",
        ));
    }
    let crc = compound_payload_crc32(&data)?;
    data[HEADER_CRC_OFFSET..HEADER_CRC_END].copy_from_slice(&crc.to_le_bytes());
    writer.write_all(&data)?;
    Ok(())
}

pub(crate) fn validate_compound_sidecar_payload(
    data: &[u8],
    declaration: &CompoundSidecarDeclaration<'_>,
) -> Result<CompoundSidecarLayout, EngineError> {
    validate_compound_sidecar_payload_with_entry_callback(data, declaration, |_, _| Ok(()))
}

fn validate_compound_sidecar_payload_with_entry_callback<F>(
    data: &[u8],
    declaration: &CompoundSidecarDeclaration<'_>,
    mut callback: F,
) -> Result<CompoundSidecarLayout, EngineError>
where
    F: FnMut(&[u8], u64) -> Result<(), EngineError>,
{
    let layout = read_compound_sidecar_layout(data, declaration)?;
    let actual_crc = compound_payload_crc32(data)?;
    if actual_crc != layout.header.payload_crc32 {
        return Err(corrupt_compound_sidecar(format!(
            "compound sidecar CRC mismatch: expected {}, got {}",
            layout.header.payload_crc32, actual_crc
        )));
    }

    let mut previous_key_range: Option<(usize, usize)> = None;
    let mut previous_key_end = 0usize;
    let mut previous_postings_end = 0usize;
    let mut postings_sum = 0u64;
    for index in 0..layout.header.entry_key_count as usize {
        let entry = read_key_table_entry(data, &layout.header, index)?;
        if entry.key_offset != previous_key_end {
            return Err(corrupt_compound_sidecar(format!(
                "compound sidecar key {index} offset {} is not contiguous after {}",
                entry.key_offset, previous_key_end
            )));
        }
        let key_start = checked_add(
            layout.header.key_bytes_offset,
            entry.key_offset,
            "key start",
        )?;
        let key_end = checked_add(key_start, entry.key_len, "key end")?;
        if key_end
            > checked_add(
                layout.header.key_bytes_offset,
                layout.header.key_bytes_len,
                "key bytes end",
            )?
        {
            return Err(corrupt_compound_sidecar(format!(
                "compound sidecar key {index} range [{key_start}, {key_end}) exceeds key bytes region"
            )));
        }
        let key = &data[key_start..key_end];
        if key.is_empty() {
            return Err(corrupt_compound_sidecar(format!(
                "compound sidecar key {index} is empty"
            )));
        }
        decode_compound_tuple_components(key, declaration.fields)?;
        if previous_key_range
            .map(|(start, end)| &data[start..end])
            .is_some_and(|previous| key <= previous)
        {
            return Err(corrupt_compound_sidecar(format!(
                "compound sidecar keys are not strictly increasing at key {index}"
            )));
        }

        if entry.postings_count == 0 {
            return Err(corrupt_compound_sidecar(format!(
                "compound sidecar key {index} has no postings"
            )));
        }
        if entry.postings_offset != previous_postings_end {
            return Err(corrupt_compound_sidecar(format!(
                "compound sidecar postings for key {index} offset {} is not contiguous after {}",
                entry.postings_offset, previous_postings_end
            )));
        }
        let posting_start = checked_add(
            layout.header.postings_offset,
            entry.postings_offset,
            "posting start",
        )?;
        let posting_bytes = checked_mul(entry.postings_count, 8, "posting bytes")?;
        let posting_end = checked_add(posting_start, posting_bytes, "posting end")?;
        if posting_end
            > checked_add(
                layout.header.postings_offset,
                layout.header.postings_len,
                "postings end",
            )?
        {
            return Err(corrupt_compound_sidecar(format!(
                "compound sidecar postings for key {index} exceed postings region"
            )));
        }
        let mut previous_id = None;
        for posting_index in 0..entry.postings_count {
            let offset = posting_start + posting_index * 8;
            let id = read_u64_le_at(data, offset)?;
            if previous_id.is_some_and(|previous| id <= previous) {
                return Err(corrupt_compound_sidecar(format!(
                    "compound sidecar postings for key {index} are not strictly increasing"
                )));
            }
            callback(key, id)?;
            previous_id = Some(id);
        }

        previous_key_range = Some((key_start, key_end));
        previous_key_end = entry.key_offset + entry.key_len;
        previous_postings_end = entry.postings_offset + posting_bytes;
        postings_sum = postings_sum
            .checked_add(entry.postings_count as u64)
            .ok_or_else(|| corrupt_compound_sidecar("compound sidecar posting count overflow"))?;
    }
    if previous_key_end != layout.header.key_bytes_len {
        return Err(corrupt_compound_sidecar(format!(
            "compound sidecar key bytes used {} does not match header {}",
            previous_key_end, layout.header.key_bytes_len
        )));
    }
    if previous_postings_end != layout.header.postings_len {
        return Err(corrupt_compound_sidecar(format!(
            "compound sidecar postings used {} does not match header {}",
            previous_postings_end, layout.header.postings_len
        )));
    }
    if postings_sum != layout.header.posting_count {
        return Err(corrupt_compound_sidecar(format!(
            "compound sidecar posting count sum {} does not match header {}",
            postings_sum, layout.header.posting_count
        )));
    }
    Ok(layout)
}

pub(crate) fn validate_compound_sidecar_header_only(
    data: &[u8],
    declaration: &CompoundSidecarDeclaration<'_>,
) -> Result<(), EngineError> {
    let header = read_compound_sidecar_header(data)?;
    validate_compound_sidecar_header(&header, data, declaration)
}

pub(crate) fn read_compound_sidecar_layout(
    data: &[u8],
    declaration: &CompoundSidecarDeclaration<'_>,
) -> Result<CompoundSidecarLayout, EngineError> {
    let header = read_compound_sidecar_header(data)?;
    validate_compound_sidecar_header(&header, data, declaration)?;
    let key_table_count = read_u64_le_at(data, header.key_table_offset)?;
    if key_table_count != header.entry_key_count {
        return Err(corrupt_compound_sidecar(format!(
            "compound sidecar key table count {} does not match header {}",
            key_table_count, header.entry_key_count
        )));
    }
    Ok(CompoundSidecarLayout { header })
}

#[allow(dead_code)]
pub(crate) fn scan_compound_sidecar_prefix(
    data: &[u8],
    declaration: &CompoundSidecarDeclaration<'_>,
    bounds: &CompoundPrefixBounds,
) -> Result<Vec<u64>, EngineError> {
    let layout = read_compound_sidecar_layout(data, declaration)?;
    scan_compound_key_range(
        data,
        &layout,
        declaration.fields,
        Some(CompoundLowerBound {
            key: bounds.lower.clone(),
            exclusive_component_prefix: false,
        }),
        &bounds.upper_exclusive,
        None,
    )
}

pub(crate) fn scan_compound_sidecar_prefix_limited(
    data: &[u8],
    declaration: &CompoundSidecarDeclaration<'_>,
    bounds: &CompoundPrefixBounds,
    limit: usize,
) -> Result<Vec<u64>, EngineError> {
    let layout = read_compound_sidecar_layout(data, declaration)?;
    scan_compound_key_range(
        data,
        &layout,
        declaration.fields,
        Some(CompoundLowerBound {
            key: bounds.lower.clone(),
            exclusive_component_prefix: false,
        }),
        &bounds.upper_exclusive,
        Some(limit),
    )
}

#[allow(dead_code)]
pub(crate) fn scan_compound_sidecar_range(
    data: &[u8],
    declaration: &CompoundSidecarDeclaration<'_>,
    bounds: &CompoundRangeBounds,
) -> Result<Vec<u64>, EngineError> {
    let layout = read_compound_sidecar_layout(data, declaration)?;
    scan_compound_key_range(
        data,
        &layout,
        declaration.fields,
        bounds.lower.clone(),
        &bounds.upper_exclusive,
        None,
    )
}

pub(crate) fn scan_compound_sidecar_range_limited(
    data: &[u8],
    declaration: &CompoundSidecarDeclaration<'_>,
    bounds: &CompoundRangeBounds,
    limit: usize,
) -> Result<Vec<u64>, EngineError> {
    let layout = read_compound_sidecar_layout(data, declaration)?;
    scan_compound_key_range(
        data,
        &layout,
        declaration.fields,
        bounds.lower.clone(),
        &bounds.upper_exclusive,
        Some(limit),
    )
}

pub(crate) fn count_compound_sidecar_prefix(
    data: &[u8],
    declaration: &CompoundSidecarDeclaration<'_>,
    bounds: &CompoundPrefixBounds,
    cap: u64,
) -> Result<u64, EngineError> {
    let layout = read_compound_sidecar_layout(data, declaration)?;
    count_compound_key_range(
        data,
        &layout,
        declaration.fields,
        Some(CompoundLowerBound {
            key: bounds.lower.clone(),
            exclusive_component_prefix: false,
        }),
        &bounds.upper_exclusive,
        cap,
    )
}

pub(crate) fn count_compound_sidecar_range(
    data: &[u8],
    declaration: &CompoundSidecarDeclaration<'_>,
    bounds: &CompoundRangeBounds,
    cap: u64,
) -> Result<u64, EngineError> {
    let layout = read_compound_sidecar_layout(data, declaration)?;
    count_compound_key_range(
        data,
        &layout,
        declaration.fields,
        bounds.lower.clone(),
        &bounds.upper_exclusive,
        cap,
    )
}

pub(crate) fn for_each_compound_sidecar_entry<F>(
    data: &[u8],
    declaration: &CompoundSidecarDeclaration<'_>,
    callback: F,
) -> Result<(), EngineError>
where
    F: FnMut(&[u8], u64) -> Result<(), EngineError>,
{
    validate_compound_sidecar_payload_with_entry_callback(data, declaration, callback).map(|_| ())
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct CompoundSidecarLayout {
    header: CompoundSidecarHeader,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct ComponentEncoding {
    bytes: Vec<u8>,
    hashable_variable_len: Option<usize>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct CompoundSidecarHeader {
    target_kind: u8,
    index_kind: u8,
    index_id: u64,
    declaration_fingerprint: u64,
    field_count: u16,
    flags: u16,
    entry_key_count: u64,
    posting_count: u64,
    key_table_offset: usize,
    key_table_len: usize,
    key_bytes_offset: usize,
    key_bytes_len: usize,
    postings_offset: usize,
    postings_len: usize,
    payload_crc32: u32,
}

#[derive(Clone, Copy, Debug)]
struct KeyTableEntry {
    key_offset: usize,
    key_len: usize,
    postings_offset: usize,
    postings_count: usize,
}

fn encode_compound_components(
    context: &CompoundTupleContext<'_>,
    values: &[CompoundFieldValue<'_>],
) -> Result<Vec<u8>, EngineError> {
    validate_field_count(context.fields.len())?;
    let mut force_hash = vec![false; values.len()];
    loop {
        let mut encoded = Vec::with_capacity(values.len());
        for (ordinal, (field, value)) in context.fields.iter().zip(values.iter()).enumerate() {
            encoded.push(encode_component(
                context,
                ordinal,
                field,
                *value,
                force_hash[ordinal],
            )?);
        }
        let total_len: usize = encoded.iter().map(|component| component.bytes.len()).sum();
        if total_len <= MAX_COMPOUND_TUPLE_BYTES {
            let mut tuple = Vec::with_capacity(total_len);
            for component in encoded {
                tuple.extend_from_slice(&component.bytes);
            }
            return Ok(tuple);
        }
        let Some((ordinal, _)) = encoded
            .iter()
            .enumerate()
            .filter_map(|(ordinal, component)| {
                component
                    .hashable_variable_len
                    .map(|len| (ordinal, len))
                    .filter(|_| !force_hash[ordinal])
            })
            .max_by_key(|(_, len)| *len)
        else {
            return Err(EngineError::InvalidOperation(format!(
                "compound secondary index unavailable: encoded tuple length {total_len} exceeds {MAX_COMPOUND_TUPLE_BYTES} bytes"
            )));
        };
        force_hash[ordinal] = true;
    }
}

fn encode_component(
    context: &CompoundTupleContext<'_>,
    ordinal: usize,
    field: &SecondaryIndexFieldManifest,
    value: CompoundFieldValue<'_>,
    force_hash: bool,
) -> Result<ComponentEncoding, EngineError> {
    validate_value_matches_field(field, value, ordinal)?;
    match value {
        CompoundFieldValue::Property(None) => Ok(ComponentEncoding {
            bytes: emit_component(CompoundComponentClass::Missing, &[])?,
            hashable_variable_len: None,
        }),
        CompoundFieldValue::Property(Some(value)) => {
            encode_property_component(context, ordinal, field, value, force_hash)
        }
        CompoundFieldValue::MetadataString(value) => {
            if value.len() > MAX_COMPOUND_COMPONENT_BYTES || force_hash {
                return Ok(ComponentEncoding {
                    bytes: emit_component(
                        CompoundComponentClass::EqualityHash,
                        &equality_hash_payload(
                            context,
                            ordinal,
                            field,
                            &metadata_semantic_equality_bytes(CompoundFieldValue::MetadataString(
                                value,
                            )),
                        )?,
                    )?,
                    hashable_variable_len: None,
                });
            }
            Ok(ComponentEncoding {
                bytes: emit_component(CompoundComponentClass::String, value.as_bytes())?,
                hashable_variable_len: Some(value.len()),
            })
        }
        CompoundFieldValue::MetadataI64(value) => {
            let prop = PropValue::Int(value);
            let payload = numeric_range_sort_key_for_value(&prop)
                .expect("finite integer metadata must encode as numeric")
                .as_bytes();
            Ok(ComponentEncoding {
                bytes: emit_component(CompoundComponentClass::Numeric, &payload)?,
                hashable_variable_len: None,
            })
        }
        CompoundFieldValue::MetadataU64(value) => {
            let prop = PropValue::UInt(value);
            let payload = numeric_range_sort_key_for_value(&prop)
                .expect("finite unsigned metadata must encode as numeric")
                .as_bytes();
            Ok(ComponentEncoding {
                bytes: emit_component(CompoundComponentClass::Numeric, &payload)?,
                hashable_variable_len: None,
            })
        }
        CompoundFieldValue::MetadataF64(value) => {
            let prop = PropValue::Float(value);
            if let Some(key) = numeric_range_sort_key_for_value(&prop) {
                Ok(ComponentEncoding {
                    bytes: emit_component(CompoundComponentClass::Numeric, &key.as_bytes())?,
                    hashable_variable_len: None,
                })
            } else {
                Ok(ComponentEncoding {
                    bytes: emit_component(
                        CompoundComponentClass::EqualityHash,
                        &equality_hash_payload(
                            context,
                            ordinal,
                            field,
                            &metadata_semantic_equality_bytes(CompoundFieldValue::MetadataF64(
                                value,
                            )),
                        )?,
                    )?,
                    hashable_variable_len: None,
                })
            }
        }
    }
}

fn encode_property_component(
    context: &CompoundTupleContext<'_>,
    ordinal: usize,
    field: &SecondaryIndexFieldManifest,
    value: &PropValue,
    force_hash: bool,
) -> Result<ComponentEncoding, EngineError> {
    match value {
        PropValue::Null => Ok(ComponentEncoding {
            bytes: emit_component(CompoundComponentClass::Null, &[])?,
            hashable_variable_len: None,
        }),
        PropValue::Bool(false) => Ok(ComponentEncoding {
            bytes: emit_component(CompoundComponentClass::BoolFalse, &[])?,
            hashable_variable_len: None,
        }),
        PropValue::Bool(true) => Ok(ComponentEncoding {
            bytes: emit_component(CompoundComponentClass::BoolTrue, &[])?,
            hashable_variable_len: None,
        }),
        PropValue::Int(_) | PropValue::UInt(_) => {
            let payload = numeric_range_sort_key_for_value(value)
                .expect("finite integer property must encode as numeric")
                .as_bytes();
            Ok(ComponentEncoding {
                bytes: emit_component(CompoundComponentClass::Numeric, &payload)?,
                hashable_variable_len: None,
            })
        }
        PropValue::Float(_) => {
            if let Some(key) = numeric_range_sort_key_for_value(value) {
                Ok(ComponentEncoding {
                    bytes: emit_component(CompoundComponentClass::Numeric, &key.as_bytes())?,
                    hashable_variable_len: None,
                })
            } else {
                Ok(ComponentEncoding {
                    bytes: emit_component(
                        CompoundComponentClass::EqualityHash,
                        &equality_hash_payload(
                            context,
                            ordinal,
                            field,
                            &semantic_equality_key_bytes(value),
                        )?,
                    )?,
                    hashable_variable_len: None,
                })
            }
        }
        PropValue::String(value) => {
            if value.len() > MAX_COMPOUND_COMPONENT_BYTES || force_hash {
                return Ok(ComponentEncoding {
                    bytes: emit_component(
                        CompoundComponentClass::EqualityHash,
                        &equality_hash_payload(
                            context,
                            ordinal,
                            field,
                            &semantic_equality_key_bytes(&PropValue::String(value.clone())),
                        )?,
                    )?,
                    hashable_variable_len: None,
                });
            }
            Ok(ComponentEncoding {
                bytes: emit_component(CompoundComponentClass::String, value.as_bytes())?,
                hashable_variable_len: Some(value.len()),
            })
        }
        PropValue::Bytes(value) => {
            if value.len() > MAX_COMPOUND_COMPONENT_BYTES || force_hash {
                return Ok(ComponentEncoding {
                    bytes: emit_component(
                        CompoundComponentClass::EqualityHash,
                        &equality_hash_payload(
                            context,
                            ordinal,
                            field,
                            &semantic_equality_key_bytes(&PropValue::Bytes(value.clone())),
                        )?,
                    )?,
                    hashable_variable_len: None,
                });
            }
            Ok(ComponentEncoding {
                bytes: emit_component(CompoundComponentClass::Bytes, value)?,
                hashable_variable_len: Some(value.len()),
            })
        }
        PropValue::Array(_) | PropValue::Map(_) => Ok(ComponentEncoding {
            bytes: emit_component(
                CompoundComponentClass::EqualityHash,
                &equality_hash_payload(
                    context,
                    ordinal,
                    field,
                    &semantic_equality_key_bytes(value),
                )?,
            )?,
            hashable_variable_len: None,
        }),
    }
}

fn emit_component(class: CompoundComponentClass, payload: &[u8]) -> Result<Vec<u8>, EngineError> {
    let payload_len = u16::try_from(payload.len()).map_err(|_| {
        EngineError::InvalidOperation(format!(
            "compound secondary index unavailable: component payload length {} exceeds u16",
            payload.len()
        ))
    })?;
    let mut bytes = Vec::with_capacity(3 + payload.len());
    bytes.push(class.tag());
    bytes.extend_from_slice(&payload_len.to_be_bytes());
    bytes.extend_from_slice(payload);
    Ok(bytes)
}

fn equality_hash_payload(
    context: &CompoundTupleContext<'_>,
    ordinal: usize,
    field: &SecondaryIndexFieldManifest,
    semantic_bytes: &[u8],
) -> Result<[u8; 8], EngineError> {
    let mut bytes = Vec::new();
    bytes.extend_from_slice(COMPOUND_EQUALITY_HASH_DOMAIN);
    bytes.push(context.target_kind.tag());
    bytes.extend_from_slice(&context.target_label_id.to_le_bytes());
    bytes.extend_from_slice(&(context.fields.len() as u16).to_le_bytes());
    bytes.extend_from_slice(&(ordinal as u16).to_le_bytes());
    bytes.push(field_source(field).tag());
    put_len_prefixed(&mut bytes, canonical_field_name(field).as_bytes());
    bytes.extend_from_slice(&hash_semantic_equality_key_bytes(semantic_bytes).to_le_bytes());
    put_len_prefixed(&mut bytes, semantic_bytes);
    Ok(fnv1a(&bytes).to_le_bytes())
}

fn metadata_semantic_equality_bytes(value: CompoundFieldValue<'_>) -> Vec<u8> {
    match value {
        CompoundFieldValue::MetadataString(value) => {
            semantic_equality_key_bytes(&PropValue::String(value.to_string()))
        }
        CompoundFieldValue::MetadataI64(value) => {
            semantic_equality_key_bytes(&PropValue::Int(value))
        }
        CompoundFieldValue::MetadataU64(value) => {
            semantic_equality_key_bytes(&PropValue::UInt(value))
        }
        CompoundFieldValue::MetadataF64(value) => {
            semantic_equality_key_bytes(&PropValue::Float(value))
        }
        CompoundFieldValue::Property(_) => unreachable!("metadata equality bytes require metadata"),
    }
}

fn validate_value_matches_field(
    field: &SecondaryIndexFieldManifest,
    value: CompoundFieldValue<'_>,
    ordinal: usize,
) -> Result<(), EngineError> {
    #[allow(clippy::match_like_matches_macro)]
    // The explicit matrix is easier to audit than one large `matches!`.
    let valid = match (field, value) {
        (SecondaryIndexFieldManifest::Property { .. }, CompoundFieldValue::Property(_)) => true,
        (
            SecondaryIndexFieldManifest::NodeMetadata {
                field: NodeMetadataIndexFieldManifest::Id,
            },
            CompoundFieldValue::MetadataU64(_),
        )
        | (
            SecondaryIndexFieldManifest::NodeMetadata {
                field: NodeMetadataIndexFieldManifest::Key,
            },
            CompoundFieldValue::MetadataString(_),
        )
        | (
            SecondaryIndexFieldManifest::NodeMetadata {
                field: NodeMetadataIndexFieldManifest::Weight,
            },
            CompoundFieldValue::MetadataF64(_),
        )
        | (
            SecondaryIndexFieldManifest::NodeMetadata {
                field: NodeMetadataIndexFieldManifest::CreatedAt,
            },
            CompoundFieldValue::MetadataI64(_),
        )
        | (
            SecondaryIndexFieldManifest::NodeMetadata {
                field: NodeMetadataIndexFieldManifest::UpdatedAt,
            },
            CompoundFieldValue::MetadataI64(_),
        )
        | (
            SecondaryIndexFieldManifest::EdgeMetadata {
                field: EdgeMetadataIndexFieldManifest::Id,
            },
            CompoundFieldValue::MetadataU64(_),
        )
        | (
            SecondaryIndexFieldManifest::EdgeMetadata {
                field: EdgeMetadataIndexFieldManifest::From,
            },
            CompoundFieldValue::MetadataU64(_),
        )
        | (
            SecondaryIndexFieldManifest::EdgeMetadata {
                field: EdgeMetadataIndexFieldManifest::To,
            },
            CompoundFieldValue::MetadataU64(_),
        )
        | (
            SecondaryIndexFieldManifest::EdgeMetadata {
                field: EdgeMetadataIndexFieldManifest::Weight,
            },
            CompoundFieldValue::MetadataF64(_),
        )
        | (
            SecondaryIndexFieldManifest::EdgeMetadata {
                field: EdgeMetadataIndexFieldManifest::CreatedAt,
            },
            CompoundFieldValue::MetadataI64(_),
        )
        | (
            SecondaryIndexFieldManifest::EdgeMetadata {
                field: EdgeMetadataIndexFieldManifest::UpdatedAt,
            },
            CompoundFieldValue::MetadataI64(_),
        )
        | (
            SecondaryIndexFieldManifest::EdgeMetadata {
                field: EdgeMetadataIndexFieldManifest::ValidFrom,
            },
            CompoundFieldValue::MetadataI64(_),
        )
        | (
            SecondaryIndexFieldManifest::EdgeMetadata {
                field: EdgeMetadataIndexFieldManifest::ValidTo,
            },
            CompoundFieldValue::MetadataI64(_),
        ) => true,
        _ => false,
    };
    if valid {
        Ok(())
    } else {
        Err(EngineError::InvalidOperation(format!(
            "compound secondary index unavailable: value for field ordinal {ordinal} is incompatible with declaration field"
        )))
    }
}

fn validate_component_payload_for_field(
    class: CompoundComponentClass,
    payload: &[u8],
    field: &SecondaryIndexFieldManifest,
    ordinal: usize,
) -> Result<(), EngineError> {
    match class {
        CompoundComponentClass::Missing
        | CompoundComponentClass::Null
        | CompoundComponentClass::BoolFalse
        | CompoundComponentClass::BoolTrue => {
            if !payload.is_empty() {
                return Err(corrupt_compound_sidecar(format!(
                    "compound tuple component {ordinal} class {:?} must have empty payload",
                    class
                )));
            }
        }
        CompoundComponentClass::Numeric => {
            if payload.len() != NUMERIC_RANGE_KEY_BYTES {
                return Err(corrupt_compound_sidecar(format!(
                    "compound tuple component {ordinal} numeric payload length {} != {NUMERIC_RANGE_KEY_BYTES}",
                    payload.len()
                )));
            }
            let bytes: [u8; NUMERIC_RANGE_KEY_BYTES] = payload.try_into().unwrap();
            validate_numeric_range_sidecar_key(&bytes).map_err(|error| {
                corrupt_compound_sidecar(format!("numeric component is invalid: {error}"))
            })?;
        }
        CompoundComponentClass::String => {
            if payload.len() > MAX_COMPOUND_COMPONENT_BYTES {
                return Err(corrupt_compound_sidecar(format!(
                    "compound tuple component {ordinal} scalar payload length {} exceeds {MAX_COMPOUND_COMPONENT_BYTES}",
                    payload.len()
                )));
            }
            std::str::from_utf8(payload).map_err(|error| {
                corrupt_compound_sidecar(format!(
                    "compound tuple component {ordinal} string payload is not valid UTF-8: {error}"
                ))
            })?;
        }
        CompoundComponentClass::Bytes => {
            if payload.len() > MAX_COMPOUND_COMPONENT_BYTES {
                return Err(corrupt_compound_sidecar(format!(
                    "compound tuple component {ordinal} scalar payload length {} exceeds {MAX_COMPOUND_COMPONENT_BYTES}",
                    payload.len()
                )));
            }
        }
        CompoundComponentClass::EqualityHash => {
            if payload.len() != 8 {
                return Err(corrupt_compound_sidecar(format!(
                    "compound tuple component {ordinal} equality hash payload length {} != 8",
                    payload.len()
                )));
            }
        }
    }
    if !component_class_compatible_with_field(class, field) {
        return Err(corrupt_compound_sidecar(format!(
            "compound tuple component {ordinal} class {:?} is incompatible with declaration field",
            class
        )));
    }
    Ok(())
}

fn component_class_compatible_with_field(
    class: CompoundComponentClass,
    field: &SecondaryIndexFieldManifest,
) -> bool {
    match field {
        SecondaryIndexFieldManifest::Property { .. } => true,
        SecondaryIndexFieldManifest::NodeMetadata { field } => match field {
            NodeMetadataIndexFieldManifest::Id
            | NodeMetadataIndexFieldManifest::CreatedAt
            | NodeMetadataIndexFieldManifest::UpdatedAt => class == CompoundComponentClass::Numeric,
            NodeMetadataIndexFieldManifest::Key => {
                matches!(
                    class,
                    CompoundComponentClass::String | CompoundComponentClass::EqualityHash
                )
            }
            NodeMetadataIndexFieldManifest::Weight => {
                matches!(
                    class,
                    CompoundComponentClass::Numeric | CompoundComponentClass::EqualityHash
                )
            }
        },
        SecondaryIndexFieldManifest::EdgeMetadata { field } => match field {
            EdgeMetadataIndexFieldManifest::Id
            | EdgeMetadataIndexFieldManifest::From
            | EdgeMetadataIndexFieldManifest::To
            | EdgeMetadataIndexFieldManifest::CreatedAt
            | EdgeMetadataIndexFieldManifest::UpdatedAt
            | EdgeMetadataIndexFieldManifest::ValidFrom
            | EdgeMetadataIndexFieldManifest::ValidTo => class == CompoundComponentClass::Numeric,
            EdgeMetadataIndexFieldManifest::Weight => {
                matches!(
                    class,
                    CompoundComponentClass::Numeric | CompoundComponentClass::EqualityHash
                )
            }
        },
    }
}

pub(crate) fn field_source(field: &SecondaryIndexFieldManifest) -> CompoundFieldSource {
    match field {
        SecondaryIndexFieldManifest::Property { .. } => CompoundFieldSource::Property,
        SecondaryIndexFieldManifest::NodeMetadata { .. } => CompoundFieldSource::NodeMetadata,
        SecondaryIndexFieldManifest::EdgeMetadata { .. } => CompoundFieldSource::EdgeMetadata,
    }
}

pub(crate) fn public_field_source(field: &SecondaryIndexField) -> CompoundFieldSource {
    match field {
        SecondaryIndexField::Property { .. } => CompoundFieldSource::Property,
        SecondaryIndexField::NodeMetadata(_) => CompoundFieldSource::NodeMetadata,
        SecondaryIndexField::EdgeMetadata(_) => CompoundFieldSource::EdgeMetadata,
    }
}

pub(crate) fn canonical_field_name(field: &SecondaryIndexFieldManifest) -> String {
    match field {
        SecondaryIndexFieldManifest::Property { key } => key.clone(),
        SecondaryIndexFieldManifest::NodeMetadata { field } => {
            node_metadata_index_field_name((*field).into()).to_string()
        }
        SecondaryIndexFieldManifest::EdgeMetadata { field } => {
            edge_metadata_index_field_name((*field).into()).to_string()
        }
    }
}

pub(crate) fn public_canonical_field_name(field: &SecondaryIndexField) -> String {
    match field {
        SecondaryIndexField::Property { key } => key.clone(),
        SecondaryIndexField::NodeMetadata(field) => {
            node_metadata_index_field_name(*field).to_string()
        }
        SecondaryIndexField::EdgeMetadata(field) => {
            edge_metadata_index_field_name(*field).to_string()
        }
    }
}

type NormalizedCompoundSidecarEntries = Vec<(Vec<u8>, Vec<u64>)>;

fn normalize_compound_sidecar_entries(
    declaration: &CompoundSidecarDeclaration<'_>,
    entries: &[(Vec<u8>, u64)],
) -> Result<NormalizedCompoundSidecarEntries, EngineError> {
    let mut grouped: BTreeMap<Vec<u8>, Vec<u64>> = BTreeMap::new();
    for (key, id) in entries {
        decode_compound_tuple_components(key, declaration.fields)?;
        grouped.entry(key.clone()).or_default().push(*id);
    }
    let mut normalized = Vec::with_capacity(grouped.len());
    for (key, mut ids) in grouped {
        ids.sort_unstable();
        ids.dedup();
        if !ids.is_empty() {
            normalized.push((key, ids));
        }
    }
    Ok(normalized)
}

fn validate_field_count(field_count: usize) -> Result<(), EngineError> {
    if (1..=MAX_SECONDARY_INDEX_FIELDS).contains(&field_count) {
        Ok(())
    } else {
        Err(EngineError::InvalidOperation(format!(
            "compound secondary index unavailable: field count {field_count} is outside 1..={MAX_SECONDARY_INDEX_FIELDS}"
        )))
    }
}

#[allow(clippy::too_many_arguments)] // Header fields are written in fixed on-disk order.
fn put_header(
    data: &mut [u8],
    declaration: &CompoundSidecarDeclaration<'_>,
    entry_key_count: u64,
    posting_count: u64,
    key_table_offset: u64,
    key_table_len: u64,
    key_bytes_offset: u64,
    key_bytes_len: u64,
    postings_offset: u64,
    postings_len: u64,
    payload_crc32: u32,
) -> Result<(), EngineError> {
    if data.len() != COMPOUND_SIDECAR_HEADER_LEN {
        return Err(corrupt_compound_sidecar(
            "compound sidecar writer received wrong header buffer length",
        ));
    }
    validate_field_count(declaration.fields.len())?;
    data[0..8].copy_from_slice(&COMPOUND_SIDECAR_MAGIC);
    data[8..10].copy_from_slice(&COMPOUND_SIDECAR_VERSION.to_le_bytes());
    data[10..12].copy_from_slice(&(COMPOUND_SIDECAR_HEADER_LEN as u16).to_le_bytes());
    data[12] = declaration.target_kind.tag();
    data[13] = declaration.index_kind.tag();
    data[14..16].copy_from_slice(&0u16.to_le_bytes());
    data[16..24].copy_from_slice(&declaration.index_id.to_le_bytes());
    data[24..32].copy_from_slice(&declaration.declaration_fingerprint.to_le_bytes());
    data[32..34].copy_from_slice(&(declaration.fields.len() as u16).to_le_bytes());
    data[34..36].copy_from_slice(&declaration.flags().to_le_bytes());
    data[36..44].copy_from_slice(&entry_key_count.to_le_bytes());
    data[44..52].copy_from_slice(&posting_count.to_le_bytes());
    data[52..60].copy_from_slice(&key_table_offset.to_le_bytes());
    data[60..68].copy_from_slice(&key_table_len.to_le_bytes());
    data[68..76].copy_from_slice(&key_bytes_offset.to_le_bytes());
    data[76..84].copy_from_slice(&key_bytes_len.to_le_bytes());
    data[84..92].copy_from_slice(&postings_offset.to_le_bytes());
    data[92..100].copy_from_slice(&postings_len.to_le_bytes());
    data[HEADER_CRC_OFFSET..HEADER_CRC_END].copy_from_slice(&payload_crc32.to_le_bytes());
    data[104..120].fill(0);
    Ok(())
}

fn read_compound_sidecar_header(data: &[u8]) -> Result<CompoundSidecarHeader, EngineError> {
    if data.len() < COMPOUND_SIDECAR_HEADER_LEN {
        return Err(corrupt_compound_sidecar(format!(
            "compound sidecar length {} is shorter than header length {COMPOUND_SIDECAR_HEADER_LEN}",
            data.len()
        )));
    }
    if data[0..8] != COMPOUND_SIDECAR_MAGIC {
        return Err(corrupt_compound_sidecar(
            "compound sidecar magic does not match OGCIX01",
        ));
    }
    let version = read_u16_le_at(data, 8)?;
    if version != COMPOUND_SIDECAR_VERSION {
        return Err(corrupt_compound_sidecar(format!(
            "compound sidecar version {version} is unsupported"
        )));
    }
    let header_len = read_u16_le_at(data, 10)? as usize;
    if header_len != COMPOUND_SIDECAR_HEADER_LEN {
        return Err(corrupt_compound_sidecar(format!(
            "compound sidecar header length {header_len} != {COMPOUND_SIDECAR_HEADER_LEN}"
        )));
    }
    if read_u16_le_at(data, 14)? != 0 {
        return Err(corrupt_compound_sidecar(
            "compound sidecar reserved0 must be zero",
        ));
    }
    if data[104..120].iter().any(|byte| *byte != 0) {
        return Err(corrupt_compound_sidecar(
            "compound sidecar reserved1 must be zero",
        ));
    }
    Ok(CompoundSidecarHeader {
        target_kind: data[12],
        index_kind: data[13],
        index_id: read_u64_le_at(data, 16)?,
        declaration_fingerprint: read_u64_le_at(data, 24)?,
        field_count: read_u16_le_at(data, 32)?,
        flags: read_u16_le_at(data, 34)?,
        entry_key_count: read_u64_le_at(data, 36)?,
        posting_count: read_u64_le_at(data, 44)?,
        key_table_offset: read_usize_u64_at(data, 52, "key table offset")?,
        key_table_len: read_usize_u64_at(data, 60, "key table length")?,
        key_bytes_offset: read_usize_u64_at(data, 68, "key bytes offset")?,
        key_bytes_len: read_usize_u64_at(data, 76, "key bytes length")?,
        postings_offset: read_usize_u64_at(data, 84, "postings offset")?,
        postings_len: read_usize_u64_at(data, 92, "postings length")?,
        payload_crc32: read_u32_le_at(data, HEADER_CRC_OFFSET)?,
    })
}

fn validate_compound_sidecar_header(
    header: &CompoundSidecarHeader,
    data: &[u8],
    declaration: &CompoundSidecarDeclaration<'_>,
) -> Result<(), EngineError> {
    if header.target_kind != declaration.target_kind.tag() {
        return Err(corrupt_compound_sidecar(format!(
            "compound sidecar target kind {} does not match declaration {}",
            header.target_kind,
            declaration.target_kind.tag()
        )));
    }
    if header.index_kind != declaration.index_kind.tag() {
        return Err(corrupt_compound_sidecar(format!(
            "compound sidecar index kind {} does not match declaration {}",
            header.index_kind,
            declaration.index_kind.tag()
        )));
    }
    if header.index_id != declaration.index_id {
        return Err(corrupt_compound_sidecar(format!(
            "compound sidecar index id {} does not match declaration {}",
            header.index_id, declaration.index_id
        )));
    }
    if header.declaration_fingerprint != declaration.declaration_fingerprint {
        return Err(corrupt_compound_sidecar(format!(
            "compound sidecar declaration fingerprint {} does not match {}",
            header.declaration_fingerprint, declaration.declaration_fingerprint
        )));
    }
    if header.field_count as usize != declaration.fields.len() {
        return Err(corrupt_compound_sidecar(format!(
            "compound sidecar field count {} does not match declaration {}",
            header.field_count,
            declaration.fields.len()
        )));
    }
    if header.flags != declaration.flags() {
        return Err(corrupt_compound_sidecar(format!(
            "compound sidecar flags {} do not match declaration {}",
            header.flags,
            declaration.flags()
        )));
    }
    for (name, offset) in [
        ("key table offset", header.key_table_offset),
        ("key bytes offset", header.key_bytes_offset),
        ("postings offset", header.postings_offset),
    ] {
        if offset % 8 != 0 {
            return Err(corrupt_compound_sidecar(format!(
                "compound sidecar {name} {offset} is not 8-byte aligned"
            )));
        }
    }
    let key_table_expected_len = checked_add(
        8,
        checked_mul(
            header.entry_key_count as usize,
            COMPOUND_KEY_TABLE_ENTRY_BYTES,
            "key table bytes",
        )?,
        "key table length",
    )?;
    if header.key_table_len != key_table_expected_len {
        return Err(corrupt_compound_sidecar(format!(
            "compound sidecar key table length {} does not match expected {}",
            header.key_table_len, key_table_expected_len
        )));
    }
    let postings_expected_len = checked_mul(header.posting_count as usize, 8, "postings bytes")?;
    if header.postings_len != postings_expected_len {
        return Err(corrupt_compound_sidecar(format!(
            "compound sidecar postings length {} does not match expected {}",
            header.postings_len, postings_expected_len
        )));
    }
    let key_table_end = checked_add(
        header.key_table_offset,
        header.key_table_len,
        "key table end",
    )?;
    let key_bytes_end = checked_add(
        header.key_bytes_offset,
        header.key_bytes_len,
        "key bytes end",
    )?;
    let postings_end = checked_add(header.postings_offset, header.postings_len, "postings end")?;
    if header.key_table_offset < COMPOUND_SIDECAR_HEADER_LEN {
        return Err(corrupt_compound_sidecar(
            "compound sidecar key table overlaps header",
        ));
    }
    if key_table_end > header.key_bytes_offset {
        return Err(corrupt_compound_sidecar(
            "compound sidecar key table overlaps key bytes region",
        ));
    }
    if key_bytes_end > header.postings_offset {
        return Err(corrupt_compound_sidecar(
            "compound sidecar key bytes overlap postings region",
        ));
    }
    if postings_end != data.len() {
        return Err(corrupt_compound_sidecar(format!(
            "compound sidecar postings end {} does not match file length {}",
            postings_end,
            data.len()
        )));
    }
    if key_table_end > data.len() || key_bytes_end > data.len() {
        return Err(corrupt_compound_sidecar(
            "compound sidecar region exceeds file length",
        ));
    }
    Ok(())
}

fn read_key_table_entry(
    data: &[u8],
    header: &CompoundSidecarHeader,
    index: usize,
) -> Result<KeyTableEntry, EngineError> {
    let offset = checked_add(
        header.key_table_offset + 8,
        checked_mul(index, COMPOUND_KEY_TABLE_ENTRY_BYTES, "key table entry")?,
        "key table entry offset",
    )?;
    Ok(KeyTableEntry {
        key_offset: read_usize_u64_at(data, offset, "key offset")?,
        key_len: read_usize_u32_at(data, offset + 8, "key length")?,
        postings_offset: read_usize_u64_at(data, offset + 12, "postings offset")?,
        postings_count: read_usize_u32_at(data, offset + 20, "postings count")?,
    })
}

fn scan_compound_key_range(
    data: &[u8],
    layout: &CompoundSidecarLayout,
    fields: &[SecondaryIndexFieldManifest],
    lower: Option<CompoundLowerBound>,
    upper_exclusive: &[u8],
    limit: Option<usize>,
) -> Result<Vec<u64>, EngineError> {
    let start_index = match &lower {
        Some(lower) => compound_key_lower_bound(data, layout, &lower.key)?,
        None => 0,
    };
    let mut ids = Vec::new();
    for index in start_index..layout.header.entry_key_count as usize {
        let key = key_at(data, layout, index)?;
        if key >= upper_exclusive {
            break;
        }
        if lower
            .as_ref()
            .is_some_and(|lower| lower.exclusive_component_prefix && key.starts_with(&lower.key))
        {
            continue;
        }
        decode_compound_tuple_components(key, fields)?;
        // Capped scans must not decode a hot key's full posting list: read only
        // the remaining requested IDs and stop without touching later postings.
        let max_postings = match limit {
            Some(limit) => {
                let remaining = limit.saturating_sub(ids.len());
                if remaining == 0 {
                    break;
                }
                remaining
            }
            None => usize::MAX,
        };
        let postings = postings_prefix_at(data, layout, index, max_postings)?;
        ids.extend(postings);
        if limit.is_some_and(|limit| ids.len() >= limit) {
            break;
        }
    }
    Ok(ids)
}

// Sums postings_count over keys in [lower, upper_exclusive) without decoding
// any posting list. Stops once the running total reaches `cap`: counts at or
// past the cap all drive the same planner decision, so further precision is
// wasted key-table walking.
fn count_compound_key_range(
    data: &[u8],
    layout: &CompoundSidecarLayout,
    fields: &[SecondaryIndexFieldManifest],
    lower: Option<CompoundLowerBound>,
    upper_exclusive: &[u8],
    cap: u64,
) -> Result<u64, EngineError> {
    let start_index = match &lower {
        Some(lower) => compound_key_lower_bound(data, layout, &lower.key)?,
        None => 0,
    };
    let mut total = 0u64;
    for index in start_index..layout.header.entry_key_count as usize {
        let key = key_at(data, layout, index)?;
        if key >= upper_exclusive {
            break;
        }
        if lower
            .as_ref()
            .is_some_and(|lower| lower.exclusive_component_prefix && key.starts_with(&lower.key))
        {
            continue;
        }
        decode_compound_tuple_components(key, fields)?;
        let entry = read_key_table_entry(data, &layout.header, index)?;
        total = total.saturating_add(entry.postings_count as u64);
        if total >= cap {
            break;
        }
    }
    Ok(total)
}

fn compound_key_lower_bound(
    data: &[u8],
    layout: &CompoundSidecarLayout,
    bound: &[u8],
) -> Result<usize, EngineError> {
    let mut lo = 0usize;
    let mut hi = layout.header.entry_key_count as usize;
    while lo < hi {
        let mid = lo + (hi - lo) / 2;
        if key_at(data, layout, mid)? < bound {
            lo = mid + 1;
        } else {
            hi = mid;
        }
    }
    Ok(lo)
}

fn key_at<'a>(
    data: &'a [u8],
    layout: &CompoundSidecarLayout,
    index: usize,
) -> Result<&'a [u8], EngineError> {
    let entry = read_key_table_entry(data, &layout.header, index)?;
    let start = checked_add(
        layout.header.key_bytes_offset,
        entry.key_offset,
        "key start",
    )?;
    let end = checked_add(start, entry.key_len, "key end")?;
    if entry.key_len == 0 {
        return Err(corrupt_compound_sidecar(format!(
            "compound sidecar key {index} is empty"
        )));
    }
    let key_bytes_end = checked_add(
        layout.header.key_bytes_offset,
        layout.header.key_bytes_len,
        "key bytes end",
    )?;
    if end > key_bytes_end {
        return Err(corrupt_compound_sidecar(format!(
            "compound sidecar key {index} exceeds key bytes region"
        )));
    }
    data.get(start..end)
        .ok_or_else(|| corrupt_compound_sidecar("compound sidecar key range out of bounds"))
}

fn postings_prefix_at(
    data: &[u8],
    layout: &CompoundSidecarLayout,
    index: usize,
    max_postings: usize,
) -> Result<Vec<u64>, EngineError> {
    let entry = read_key_table_entry(data, &layout.header, index)?;
    if entry.postings_count == 0 {
        return Err(corrupt_compound_sidecar(format!(
            "compound sidecar key {index} has no postings"
        )));
    }
    let start = checked_add(
        layout.header.postings_offset,
        entry.postings_offset,
        "posting start",
    )?;
    let posting_bytes = checked_mul(entry.postings_count, 8, "posting bytes")?;
    let end = checked_add(start, posting_bytes, "posting end")?;
    let postings_end = checked_add(
        layout.header.postings_offset,
        layout.header.postings_len,
        "postings end",
    )?;
    if end > postings_end {
        return Err(corrupt_compound_sidecar(format!(
            "compound sidecar postings for key {index} exceed postings region"
        )));
    }
    // Strictly-increasing validation only covers decoded postings; bytes past
    // `max_postings` are unvisited and stay subject to scrub/full validation.
    let decode_count = entry.postings_count.min(max_postings);
    let mut ids = Vec::with_capacity(decode_count);
    let mut previous_id = None;
    for posting_index in 0..decode_count {
        let id = read_u64_le_at(data, start + posting_index * 8)?;
        if previous_id.is_some_and(|previous| id <= previous) {
            return Err(corrupt_compound_sidecar(format!(
                "compound sidecar postings for key {index} are not strictly increasing"
            )));
        }
        previous_id = Some(id);
        ids.push(id);
    }
    Ok(ids)
}

fn compound_payload_crc32(data: &[u8]) -> Result<u32, EngineError> {
    if data.len() < HEADER_CRC_END {
        return Err(corrupt_compound_sidecar(
            "compound sidecar is too short for CRC",
        ));
    }
    let mut hasher = Crc32Hasher::new();
    hasher.update(&data[HEADER_CRC_END..]);
    Ok(hasher.finalize())
}

fn put_len_prefixed(target: &mut Vec<u8>, bytes: &[u8]) {
    target.extend_from_slice(&(bytes.len() as u64).to_le_bytes());
    target.extend_from_slice(bytes);
}

fn align_u64(value: u64, alignment: u64) -> Result<u64, EngineError> {
    let remainder = value % alignment;
    if remainder == 0 {
        Ok(value)
    } else {
        value
            .checked_add(alignment - remainder)
            .ok_or_else(|| corrupt_compound_sidecar("compound sidecar alignment overflow"))
    }
}

fn checked_add(left: usize, right: usize, context: &str) -> Result<usize, EngineError> {
    left.checked_add(right)
        .ok_or_else(|| corrupt_compound_sidecar(format!("compound sidecar {context} overflow")))
}

fn checked_mul(left: usize, right: usize, context: &str) -> Result<usize, EngineError> {
    left.checked_mul(right)
        .ok_or_else(|| corrupt_compound_sidecar(format!("compound sidecar {context} overflow")))
}

fn read_u16_le_at(data: &[u8], offset: usize) -> Result<u16, EngineError> {
    let bytes = data
        .get(offset..offset + 2)
        .ok_or_else(|| corrupt_compound_sidecar("compound sidecar u16 read out of bounds"))?;
    Ok(u16::from_le_bytes(bytes.try_into().unwrap()))
}

fn read_u32_le_at(data: &[u8], offset: usize) -> Result<u32, EngineError> {
    let bytes = data
        .get(offset..offset + 4)
        .ok_or_else(|| corrupt_compound_sidecar("compound sidecar u32 read out of bounds"))?;
    Ok(u32::from_le_bytes(bytes.try_into().unwrap()))
}

fn read_u64_le_at(data: &[u8], offset: usize) -> Result<u64, EngineError> {
    let bytes = data
        .get(offset..offset + 8)
        .ok_or_else(|| corrupt_compound_sidecar("compound sidecar u64 read out of bounds"))?;
    Ok(u64::from_le_bytes(bytes.try_into().unwrap()))
}

fn read_usize_u64_at(data: &[u8], offset: usize, context: &str) -> Result<usize, EngineError> {
    usize::try_from(read_u64_le_at(data, offset)?).map_err(|_| {
        corrupt_compound_sidecar(format!("compound sidecar {context} does not fit usize"))
    })
}

fn read_usize_u32_at(data: &[u8], offset: usize, context: &str) -> Result<usize, EngineError> {
    usize::try_from(read_u32_le_at(data, offset)?).map_err(|_| {
        corrupt_compound_sidecar(format!("compound sidecar {context} does not fit usize"))
    })
}

fn corrupt_compound_sidecar(message: impl Into<String>) -> EngineError {
    EngineError::CorruptRecord(format!(
        "compound secondary index unavailable: corrupt sidecar {}",
        message.into()
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{SecondaryIndexState, SecondaryIndexTarget};
    use std::cmp::Ordering;

    fn node_entry(index_id: u64, kind: SecondaryIndexKind) -> SecondaryIndexManifestEntry {
        SecondaryIndexManifestEntry {
            index_id,
            target: SecondaryIndexTarget::NodeFieldIndex {
                label_id: 7,
                fields: vec![
                    SecondaryIndexFieldManifest::Property {
                        key: "tenant".to_string(),
                    },
                    SecondaryIndexFieldManifest::Property {
                        key: "score".to_string(),
                    },
                ],
            },
            kind,
            state: SecondaryIndexState::Ready,
            last_error: None,
        }
    }

    fn mixed_node_entry(index_id: u64) -> SecondaryIndexManifestEntry {
        SecondaryIndexManifestEntry {
            index_id,
            target: SecondaryIndexTarget::NodeFieldIndex {
                label_id: 7,
                fields: vec![
                    SecondaryIndexFieldManifest::Property {
                        key: "tenant".to_string(),
                    },
                    SecondaryIndexFieldManifest::NodeMetadata {
                        field: NodeMetadataIndexFieldManifest::UpdatedAt,
                    },
                ],
            },
            kind: SecondaryIndexKind::Range,
            state: SecondaryIndexState::Ready,
            last_error: None,
        }
    }

    fn declaration<'a>(entry: &'a SecondaryIndexManifestEntry) -> CompoundSidecarDeclaration<'a> {
        CompoundSidecarDeclaration::from_manifest_entry(entry, 55).unwrap()
    }

    fn tuple_context<'a>(entry: &'a SecondaryIndexManifestEntry) -> CompoundTupleContext<'a> {
        let SecondaryIndexTarget::NodeFieldIndex { label_id, fields } = &entry.target else {
            panic!("test entry must be node field index");
        };
        CompoundTupleContext {
            target_kind: CompoundSidecarTargetKind::Node,
            target_label_id: *label_id,
            fields,
        }
    }

    fn class(component: &[u8]) -> u8 {
        component[0]
    }

    #[test]
    fn compound_secondary_failure_message_is_normalized_once() {
        assert_eq!(
            compound_secondary_failure_message_from_str(
                "compound secondary index unavailable: missing sidecar"
            ),
            "compound secondary index unavailable: missing sidecar"
        );
        assert_eq!(
            compound_secondary_failure_message_from_str(
                "corrupt record: compound secondary index unavailable: missing sidecar"
            ),
            "compound secondary index unavailable: missing sidecar"
        );
        assert_eq!(
            compound_secondary_failure_message_from_str(concat!(
                "compound secondary index unavailable: corrupt record: ",
                "compound secondary index unavailable: missing sidecar"
            )),
            "compound secondary index unavailable: missing sidecar"
        );
    }

    fn fix_crc(data: &mut [u8]) {
        data[HEADER_CRC_OFFSET..HEADER_CRC_END].fill(0);
        let crc = compound_payload_crc32(data).unwrap();
        data[HEADER_CRC_OFFSET..HEADER_CRC_END].copy_from_slice(&crc.to_le_bytes());
    }

    fn write_payload(
        declaration: &CompoundSidecarDeclaration<'_>,
        entries: &[(Vec<u8>, u64)],
    ) -> Vec<u8> {
        let mut payload = Vec::new();
        write_compound_sidecar_payload(&mut payload, declaration, entries).unwrap();
        payload
    }

    #[test]
    fn secondary_index_key_numeric_equality_encodes_identically() {
        let entry = node_entry(1, SecondaryIndexKind::Equality);
        let context = tuple_context(&entry);
        let int = encode_compound_field_component(
            &context,
            1,
            CompoundFieldValue::Property(Some(&PropValue::Int(1))),
        )
        .unwrap();
        let uint = encode_compound_field_component(
            &context,
            1,
            CompoundFieldValue::Property(Some(&PropValue::UInt(1))),
        )
        .unwrap();
        let float = encode_compound_field_component(
            &context,
            1,
            CompoundFieldValue::Property(Some(&PropValue::Float(1.0))),
        )
        .unwrap();
        assert_eq!(int, uint);
        assert_eq!(int, float);
    }

    #[test]
    fn secondary_index_key_numeric_sort_order_matches_range_key() {
        let entry = node_entry(1, SecondaryIndexKind::Range);
        let context = tuple_context(&entry);
        let values = [
            PropValue::Int(-2),
            PropValue::Float(-1.5),
            PropValue::Int(0),
            PropValue::UInt(1),
            PropValue::Float(2.5),
        ];
        for pair in values.windows(2) {
            let left = encode_compound_field_component(
                &context,
                1,
                CompoundFieldValue::Property(Some(&pair[0])),
            )
            .unwrap();
            let right = encode_compound_field_component(
                &context,
                1,
                CompoundFieldValue::Property(Some(&pair[1])),
            )
            .unwrap();
            let expected = numeric_range_sort_key_for_value(&pair[0])
                .unwrap()
                .cmp(&numeric_range_sort_key_for_value(&pair[1]).unwrap());
            assert_eq!(left.cmp(&right), expected);
        }
    }

    #[test]
    fn secondary_index_key_scalar_classes_encode_and_validate() {
        let entry = node_entry(1, SecondaryIndexKind::Equality);
        let context = tuple_context(&entry);
        let cases = [
            (None, COMPOUND_COMPONENT_CLASS_MISSING),
            (Some(PropValue::Null), COMPOUND_COMPONENT_CLASS_NULL),
            (
                Some(PropValue::Bool(false)),
                COMPOUND_COMPONENT_CLASS_BOOL_FALSE,
            ),
            (
                Some(PropValue::Bool(true)),
                COMPOUND_COMPONENT_CLASS_BOOL_TRUE,
            ),
            (
                Some(PropValue::String("abc".to_string())),
                COMPOUND_COMPONENT_CLASS_STRING,
            ),
            (
                Some(PropValue::Bytes(vec![1, 2, 3])),
                COMPOUND_COMPONENT_CLASS_BYTES,
            ),
        ];
        for (value, expected_class) in cases {
            let component = encode_compound_field_component(
                &context,
                0,
                CompoundFieldValue::Property(value.as_ref()),
            )
            .unwrap();
            assert_eq!(class(&component), expected_class);
            let key = encode_compound_tuple_key(
                &context,
                &[
                    CompoundFieldValue::Property(value.as_ref()),
                    CompoundFieldValue::Property(Some(&PropValue::Int(1))),
                ],
            )
            .unwrap();
            decode_compound_tuple_components(&key, context.fields).unwrap();
        }
    }

    #[test]
    fn secondary_index_key_complex_and_oversized_values_use_equality_hash() {
        let entry = node_entry(1, SecondaryIndexKind::Equality);
        let context = tuple_context(&entry);
        let mut map = BTreeMap::new();
        map.insert("x".to_string(), PropValue::Int(1));
        let values = [
            PropValue::Array(vec![PropValue::Int(1)]),
            PropValue::Map(map),
            PropValue::Float(f64::NAN),
            PropValue::String("x".repeat(MAX_COMPOUND_COMPONENT_BYTES + 1)),
            PropValue::Bytes(vec![7; MAX_COMPOUND_COMPONENT_BYTES + 1]),
        ];
        for value in values {
            let component = encode_compound_field_component(
                &context,
                0,
                CompoundFieldValue::Property(Some(&value)),
            )
            .unwrap();
            assert_eq!(class(&component), COMPOUND_COMPONENT_CLASS_EQUALITY_HASH);
            assert_eq!(component.len(), 11);
        }
    }

    #[test]
    fn secondary_index_key_hashes_longest_components_until_tuple_fits() {
        let fields = (0..MAX_SECONDARY_INDEX_FIELDS)
            .map(|idx| SecondaryIndexFieldManifest::Property {
                key: format!("p{idx}"),
            })
            .collect::<Vec<_>>();
        let entry = SecondaryIndexManifestEntry {
            index_id: 1,
            target: SecondaryIndexTarget::NodeFieldIndex {
                label_id: 1,
                fields,
            },
            kind: SecondaryIndexKind::Equality,
            state: SecondaryIndexState::Ready,
            last_error: None,
        };
        let context = tuple_context(&entry);
        let values = (0..MAX_SECONDARY_INDEX_FIELDS)
            .map(|_| PropValue::String("x".repeat(MAX_COMPOUND_COMPONENT_BYTES)))
            .collect::<Vec<_>>();
        let field_values = values
            .iter()
            .map(|value| CompoundFieldValue::Property(Some(value)))
            .collect::<Vec<_>>();
        let key = encode_compound_tuple_key(&context, &field_values).unwrap();
        assert!(key.len() <= MAX_COMPOUND_TUPLE_BYTES);
        let decoded = decode_compound_tuple_components(&key, context.fields).unwrap();
        assert!(decoded
            .iter()
            .any(|component| component.class == CompoundComponentClass::EqualityHash));
    }

    #[test]
    fn secondary_index_key_prefix_bounds_include_matching_suffixes() {
        let entry = node_entry(1, SecondaryIndexKind::Equality);
        let context = tuple_context(&entry);
        let prefix = encode_compound_tuple_prefix(
            &context,
            &[CompoundFieldValue::Property(Some(&PropValue::String(
                "acme".to_string(),
            )))],
        )
        .unwrap();
        let bounds = compound_prefix_bounds(&prefix);
        let matching = encode_compound_tuple_key(
            &context,
            &[
                CompoundFieldValue::Property(Some(&PropValue::String("acme".to_string()))),
                CompoundFieldValue::Property(Some(&PropValue::Int(10))),
            ],
        )
        .unwrap();
        let nonmatching = encode_compound_tuple_key(
            &context,
            &[
                CompoundFieldValue::Property(Some(&PropValue::String("beta".to_string()))),
                CompoundFieldValue::Property(Some(&PropValue::Int(10))),
            ],
        )
        .unwrap();
        assert!(bounds.lower < matching);
        assert!(matching < bounds.upper_exclusive);
        assert!(nonmatching >= bounds.upper_exclusive);
    }

    #[test]
    fn secondary_index_key_range_bounds_respect_inclusive_and_exclusive() {
        let entry = node_entry(1, SecondaryIndexKind::Range);
        let context = tuple_context(&entry);
        let prefix = encode_compound_tuple_prefix(
            &context,
            &[CompoundFieldValue::Property(Some(&PropValue::String(
                "acme".to_string(),
            )))],
        )
        .unwrap();
        let lower = encode_compound_field_component(
            &context,
            1,
            CompoundFieldValue::Property(Some(&PropValue::Int(10))),
        )
        .unwrap();
        let upper = encode_compound_field_component(
            &context,
            1,
            CompoundFieldValue::Property(Some(&PropValue::Int(20))),
        )
        .unwrap();
        let inclusive =
            compound_range_bounds(&prefix, Some((&lower, true)), Some((&upper, true))).unwrap();
        assert_eq!(
            inclusive.lower.as_ref().unwrap().key,
            [prefix.as_slice(), lower.as_slice()].concat()
        );
        assert!(!inclusive.lower.as_ref().unwrap().exclusive_component_prefix);
        assert!(inclusive.upper_exclusive.ends_with(&[0xFF]));

        let exclusive =
            compound_range_bounds(&prefix, Some((&lower, false)), Some((&upper, false))).unwrap();
        assert!(exclusive.lower.as_ref().unwrap().exclusive_component_prefix);
        assert!(!exclusive.upper_exclusive.ends_with(&[0xFF]));
    }

    #[test]
    fn secondary_index_key_range_bounds_reject_non_numeric_components() {
        let entry = node_entry(1, SecondaryIndexKind::Range);
        let context = tuple_context(&entry);
        let prefix = encode_compound_tuple_prefix(
            &context,
            &[CompoundFieldValue::Property(Some(&PropValue::String(
                "acme".to_string(),
            )))],
        )
        .unwrap();
        let string_component = encode_compound_field_component(
            &context,
            1,
            CompoundFieldValue::Property(Some(&PropValue::String("score".to_string()))),
        )
        .unwrap();
        let hash_component = encode_compound_field_component(
            &context,
            1,
            CompoundFieldValue::Property(Some(&PropValue::Array(vec![PropValue::Int(1)]))),
        )
        .unwrap();

        assert!(compound_range_bounds(&prefix, Some((&string_component, true)), None).is_err());
        assert!(compound_range_bounds(&prefix, None, Some((&hash_component, true))).is_err());
    }

    #[test]
    fn secondary_index_key_byte_ordering_is_stable() {
        let entry = node_entry(1, SecondaryIndexKind::Equality);
        let context = tuple_context(&entry);
        let missing =
            encode_compound_field_component(&context, 0, CompoundFieldValue::Property(None))
                .unwrap();
        let null = encode_compound_field_component(
            &context,
            0,
            CompoundFieldValue::Property(Some(&PropValue::Null)),
        )
        .unwrap();
        let false_value = encode_compound_field_component(
            &context,
            0,
            CompoundFieldValue::Property(Some(&PropValue::Bool(false))),
        )
        .unwrap();
        let numeric = encode_compound_field_component(
            &context,
            0,
            CompoundFieldValue::Property(Some(&PropValue::Int(0))),
        )
        .unwrap();
        let string = encode_compound_field_component(
            &context,
            0,
            CompoundFieldValue::Property(Some(&PropValue::String("a".to_string()))),
        )
        .unwrap();
        assert_eq!(missing.cmp(&null), Ordering::Less);
        assert_eq!(null.cmp(&false_value), Ordering::Less);
        assert_eq!(false_value.cmp(&numeric), Ordering::Less);
        assert_eq!(numeric.cmp(&string), Ordering::Less);
    }

    #[test]
    fn compound_sidecar_empty_single_and_multi_key_validate_and_scan() {
        let entry = node_entry(9, SecondaryIndexKind::Equality);
        let declaration = declaration(&entry);
        let context = tuple_context(&entry);
        let empty = write_payload(&declaration, &[]);
        validate_compound_sidecar_payload(&empty, &declaration).unwrap();

        let k1 = encode_compound_tuple_key(
            &context,
            &[
                CompoundFieldValue::Property(Some(&PropValue::String("acme".to_string()))),
                CompoundFieldValue::Property(Some(&PropValue::Int(1))),
            ],
        )
        .unwrap();
        let k2 = encode_compound_tuple_key(
            &context,
            &[
                CompoundFieldValue::Property(Some(&PropValue::String("acme".to_string()))),
                CompoundFieldValue::Property(Some(&PropValue::Int(2))),
            ],
        )
        .unwrap();
        let k3 = encode_compound_tuple_key(
            &context,
            &[
                CompoundFieldValue::Property(Some(&PropValue::String("beta".to_string()))),
                CompoundFieldValue::Property(Some(&PropValue::Int(1))),
            ],
        )
        .unwrap();
        let payload = write_payload(
            &declaration,
            &[(k2.clone(), 5), (k1.clone(), 3), (k1.clone(), 1), (k3, 8)],
        );
        validate_compound_sidecar_payload(&payload, &declaration).unwrap();
        let prefix = encode_compound_tuple_prefix(
            &context,
            &[CompoundFieldValue::Property(Some(&PropValue::String(
                "acme".to_string(),
            )))],
        )
        .unwrap();
        assert_eq!(
            scan_compound_sidecar_prefix(&payload, &declaration, &compound_prefix_bounds(&prefix))
                .unwrap(),
            vec![1, 3, 5]
        );
        assert_eq!(
            scan_compound_sidecar_prefix(&payload, &declaration, &compound_prefix_bounds(&k1))
                .unwrap(),
            vec![1, 3]
        );
    }

    #[test]
    fn compound_sidecar_scan_does_not_full_validate_unvisited_keys() {
        let entry = node_entry(9, SecondaryIndexKind::Equality);
        let declaration = declaration(&entry);
        let context = tuple_context(&entry);
        let key = |tenant: &str, score: i64| {
            encode_compound_tuple_key(
                &context,
                &[
                    CompoundFieldValue::Property(Some(&PropValue::String(tenant.to_string()))),
                    CompoundFieldValue::Property(Some(&PropValue::Int(score))),
                ],
            )
            .unwrap()
        };
        let k1 = key("acme", 1);
        let mut payload = write_payload(
            &declaration,
            &[(k1.clone(), 1), (key("acme", 2), 2), (key("beta", 1), 3)],
        );
        let third_entry_offset =
            COMPOUND_SIDECAR_HEADER_LEN + 8 + 2 * COMPOUND_KEY_TABLE_ENTRY_BYTES;
        let third_key_relative = read_u64_le_at(&payload, third_entry_offset).unwrap() as usize;
        let key_bytes_offset = read_u64_le_at(&payload, 68).unwrap() as usize;
        payload[key_bytes_offset + third_key_relative] = 0x7F;
        fix_crc(&mut payload);

        assert!(validate_compound_sidecar_payload(&payload, &declaration).is_err());
        assert_eq!(
            scan_compound_sidecar_prefix(&payload, &declaration, &compound_prefix_bounds(&k1))
                .unwrap(),
            vec![1]
        );
    }

    #[test]
    fn compound_sidecar_count_stops_at_cap_before_unvisited_keys() {
        let entry = node_entry(9, SecondaryIndexKind::Equality);
        let declaration = declaration(&entry);
        let context = tuple_context(&entry);
        let key = |tenant: &str, score: i64| {
            encode_compound_tuple_key(
                &context,
                &[
                    CompoundFieldValue::Property(Some(&PropValue::String(tenant.to_string()))),
                    CompoundFieldValue::Property(Some(&PropValue::Int(score))),
                ],
            )
            .unwrap()
        };
        let k1 = key("acme", 1);
        let prefix = encode_compound_tuple_prefix(
            &context,
            &[CompoundFieldValue::Property(Some(&PropValue::String(
                "acme".to_string(),
            )))],
        )
        .unwrap();
        let mut payload = write_payload(
            &declaration,
            &[(k1, 1), (key("acme", 2), 2), (key("acme", 3), 3)],
        );
        let second_entry_offset = COMPOUND_SIDECAR_HEADER_LEN + 8 + COMPOUND_KEY_TABLE_ENTRY_BYTES;
        let second_key_relative = read_u64_le_at(&payload, second_entry_offset).unwrap() as usize;
        let key_bytes_offset = read_u64_le_at(&payload, 68).unwrap() as usize;
        // Keep the first component prefix intact, but corrupt the second
        // component length so decoding fails if counting walks beyond cap.
        payload[key_bytes_offset + second_key_relative + 9] = 0;
        fix_crc(&mut payload);

        assert!(validate_compound_sidecar_payload(&payload, &declaration).is_err());
        assert_eq!(
            count_compound_sidecar_prefix(
                &payload,
                &declaration,
                &compound_prefix_bounds(&prefix),
                1,
            )
            .unwrap(),
            1
        );
        assert!(count_compound_sidecar_prefix(
            &payload,
            &declaration,
            &compound_prefix_bounds(&prefix),
            2,
        )
        .is_err());
    }

    #[test]
    fn compound_sidecar_full_validation_rejects_invalid_utf8_string_component() {
        let entry = node_entry(9, SecondaryIndexKind::Equality);
        let declaration = declaration(&entry);
        let context = tuple_context(&entry);
        let key = encode_compound_tuple_key(
            &context,
            &[
                CompoundFieldValue::Property(Some(&PropValue::String("acme".to_string()))),
                CompoundFieldValue::Property(Some(&PropValue::Int(1))),
            ],
        )
        .unwrap();
        let mut payload = write_payload(&declaration, &[(key, 1)]);
        let key_bytes_offset = read_u64_le_at(&payload, 68).unwrap() as usize;
        payload[key_bytes_offset + 3] = 0xFF;
        fix_crc(&mut payload);

        let error = validate_compound_sidecar_payload(&payload, &declaration).unwrap_err();
        assert!(error.to_string().contains("not valid UTF-8"));
    }

    #[test]
    fn compound_sidecar_range_scan_respects_bounds() {
        let entry = node_entry(9, SecondaryIndexKind::Range);
        let declaration = declaration(&entry);
        let context = tuple_context(&entry);
        let key = |tenant: &str, score: i64| {
            encode_compound_tuple_key(
                &context,
                &[
                    CompoundFieldValue::Property(Some(&PropValue::String(tenant.to_string()))),
                    CompoundFieldValue::Property(Some(&PropValue::Int(score))),
                ],
            )
            .unwrap()
        };
        let payload = write_payload(
            &declaration,
            &[
                (key("acme", 1), 1),
                (key("acme", 2), 2),
                (key("acme", 3), 3),
                (key("beta", 2), 4),
            ],
        );
        let prefix = encode_compound_tuple_prefix(
            &context,
            &[CompoundFieldValue::Property(Some(&PropValue::String(
                "acme".to_string(),
            )))],
        )
        .unwrap();
        let lower = encode_compound_field_component(
            &context,
            1,
            CompoundFieldValue::Property(Some(&PropValue::Int(1))),
        )
        .unwrap();
        let upper = encode_compound_field_component(
            &context,
            1,
            CompoundFieldValue::Property(Some(&PropValue::Int(3))),
        )
        .unwrap();
        let inclusive =
            compound_range_bounds(&prefix, Some((&lower, true)), Some((&upper, true))).unwrap();
        assert_eq!(
            scan_compound_sidecar_range(&payload, &declaration, &inclusive).unwrap(),
            vec![1, 2, 3]
        );
        let exclusive =
            compound_range_bounds(&prefix, Some((&lower, false)), Some((&upper, false))).unwrap();
        assert_eq!(
            scan_compound_sidecar_range(&payload, &declaration, &exclusive).unwrap(),
            vec![2]
        );
    }

    #[test]
    fn compound_sidecar_unbounded_upper_range_stops_at_numeric_class() {
        let entry = node_entry(9, SecondaryIndexKind::Range);
        let declaration = declaration(&entry);
        let context = tuple_context(&entry);
        let key = |score: &PropValue| {
            encode_compound_tuple_key(
                &context,
                &[
                    CompoundFieldValue::Property(Some(&PropValue::String("acme".to_string()))),
                    CompoundFieldValue::Property(Some(score)),
                ],
            )
            .unwrap()
        };
        let payload = write_payload(
            &declaration,
            &[
                (key(&PropValue::Int(1)), 1),
                (key(&PropValue::Int(2)), 2),
                (key(&PropValue::String("not-numeric".to_string())), 3),
                (key(&PropValue::Array(vec![PropValue::Int(1)])), 4),
            ],
        );
        let prefix = encode_compound_tuple_prefix(
            &context,
            &[CompoundFieldValue::Property(Some(&PropValue::String(
                "acme".to_string(),
            )))],
        )
        .unwrap();
        let lower = encode_compound_field_component(
            &context,
            1,
            CompoundFieldValue::Property(Some(&PropValue::Int(2))),
        )
        .unwrap();

        let lower_only = compound_range_bounds(&prefix, Some((&lower, true)), None).unwrap();
        assert_eq!(
            lower_only.upper_exclusive,
            [prefix.as_slice(), &[COMPOUND_COMPONENT_CLASS_NUMERIC + 1]].concat()
        );
        assert_eq!(
            scan_compound_sidecar_range(&payload, &declaration, &lower_only).unwrap(),
            vec![2]
        );

        let unbounded = compound_range_bounds(&prefix, None, None).unwrap();
        assert_eq!(
            scan_compound_sidecar_range(&payload, &declaration, &unbounded).unwrap(),
            vec![1, 2]
        );
    }

    #[test]
    fn compound_sidecar_limited_scan_does_not_decode_postings_past_cap() {
        let entry = node_entry(9, SecondaryIndexKind::Equality);
        let declaration = declaration(&entry);
        let context = tuple_context(&entry);
        let key = encode_compound_tuple_key(
            &context,
            &[
                CompoundFieldValue::Property(Some(&PropValue::String("acme".to_string()))),
                CompoundFieldValue::Property(Some(&PropValue::Int(1))),
            ],
        )
        .unwrap();
        let entries: Vec<(Vec<u8>, u64)> = (1..=32u64).map(|id| (key.clone(), id)).collect();
        let mut payload = write_payload(&declaration, &entries);
        // Corrupt a posting past the cap position: a capped scan must succeed
        // without decoding it, while an uncapped scan must reject it.
        let postings_offset = read_u64_le_at(&payload, 84).unwrap() as usize;
        let corrupt_at = postings_offset + 8 * 8;
        payload[corrupt_at..corrupt_at + 8].copy_from_slice(&0u64.to_le_bytes());
        fix_crc(&mut payload);

        let bounds = compound_prefix_bounds(&key);
        assert_eq!(
            scan_compound_sidecar_prefix_limited(&payload, &declaration, &bounds, 5).unwrap(),
            vec![1, 2, 3, 4, 5]
        );
        assert!(scan_compound_sidecar_prefix(&payload, &declaration, &bounds).is_err());
    }

    #[test]
    fn compound_sidecar_rejects_bad_header_crc_fingerprint_and_field_count() {
        let entry = node_entry(9, SecondaryIndexKind::Equality);
        let declaration = declaration(&entry);
        let context = tuple_context(&entry);
        let key = encode_compound_tuple_key(
            &context,
            &[
                CompoundFieldValue::Property(Some(&PropValue::String("acme".to_string()))),
                CompoundFieldValue::Property(Some(&PropValue::Int(1))),
            ],
        )
        .unwrap();
        let payload = write_payload(&declaration, &[(key, 1)]);

        let mut bad_magic = payload.clone();
        bad_magic[0] = b'X';
        assert!(validate_compound_sidecar_payload(&bad_magic, &declaration).is_err());

        let mut bad_version = payload.clone();
        bad_version[8..10].copy_from_slice(&2u16.to_le_bytes());
        fix_crc(&mut bad_version);
        assert!(validate_compound_sidecar_payload(&bad_version, &declaration).is_err());

        let mut bad_header_len = payload.clone();
        bad_header_len[10..12].copy_from_slice(&119u16.to_le_bytes());
        fix_crc(&mut bad_header_len);
        assert!(validate_compound_sidecar_payload(&bad_header_len, &declaration).is_err());

        let mut bad_crc = payload.clone();
        let crc = read_u32_le_at(&bad_crc, HEADER_CRC_OFFSET).unwrap();
        bad_crc[HEADER_CRC_OFFSET..HEADER_CRC_END].copy_from_slice(&(crc ^ 1).to_le_bytes());
        assert!(validate_compound_sidecar_payload(&bad_crc, &declaration).is_err());

        let wrong_fingerprint = CompoundSidecarDeclaration {
            declaration_fingerprint: 99,
            ..declaration.clone()
        };
        assert!(validate_compound_sidecar_payload(&payload, &wrong_fingerprint).is_err());

        let mut bad_field_count = payload;
        bad_field_count[32..34].copy_from_slice(&1u16.to_le_bytes());
        fix_crc(&mut bad_field_count);
        assert!(validate_compound_sidecar_payload(&bad_field_count, &declaration).is_err());
    }

    #[test]
    fn compound_sidecar_rejects_order_offsets_alignment_and_classes() {
        let entry = node_entry(9, SecondaryIndexKind::Equality);
        let declaration = declaration(&entry);
        let context = tuple_context(&entry);
        let k1 = encode_compound_tuple_key(
            &context,
            &[
                CompoundFieldValue::Property(Some(&PropValue::String("a".to_string()))),
                CompoundFieldValue::Property(Some(&PropValue::Int(1))),
            ],
        )
        .unwrap();
        let k2 = encode_compound_tuple_key(
            &context,
            &[
                CompoundFieldValue::Property(Some(&PropValue::String("b".to_string()))),
                CompoundFieldValue::Property(Some(&PropValue::Int(1))),
            ],
        )
        .unwrap();
        let payload = write_payload(&declaration, &[(k1, 2), (k2, 3)]);

        let mut duplicate_key = payload.clone();
        let first_key_len =
            read_u32_le_at(&duplicate_key, COMPOUND_SIDECAR_HEADER_LEN + 8 + 8).unwrap() as usize;
        let key_bytes_offset = read_u64_le_at(&duplicate_key, 68).unwrap() as usize;
        let second_key_offset = read_u64_le_at(
            &duplicate_key,
            COMPOUND_SIDECAR_HEADER_LEN + 8 + COMPOUND_KEY_TABLE_ENTRY_BYTES,
        )
        .unwrap() as usize;
        duplicate_key.copy_within(
            key_bytes_offset..key_bytes_offset + first_key_len,
            key_bytes_offset + second_key_offset,
        );
        fix_crc(&mut duplicate_key);
        assert!(validate_compound_sidecar_payload(&duplicate_key, &declaration).is_err());

        let mut bad_alignment = payload.clone();
        bad_alignment[52..60].copy_from_slice(&121u64.to_le_bytes());
        fix_crc(&mut bad_alignment);
        assert!(validate_compound_sidecar_payload(&bad_alignment, &declaration).is_err());

        let mut bad_overlap = payload.clone();
        bad_overlap[68..76].copy_from_slice(&120u64.to_le_bytes());
        fix_crc(&mut bad_overlap);
        assert!(validate_compound_sidecar_payload(&bad_overlap, &declaration).is_err());

        let mut invalid_class = payload;
        let key_bytes_offset = read_u64_le_at(&invalid_class, 68).unwrap() as usize;
        invalid_class[key_bytes_offset] = 0x7F;
        fix_crc(&mut invalid_class);
        assert!(validate_compound_sidecar_payload(&invalid_class, &declaration).is_err());
    }

    #[test]
    fn compound_sidecar_rejects_unsorted_postings_with_valid_crc() {
        let entry = node_entry(9, SecondaryIndexKind::Equality);
        let declaration = declaration(&entry);
        let context = tuple_context(&entry);
        let key = encode_compound_tuple_key(
            &context,
            &[
                CompoundFieldValue::Property(Some(&PropValue::String("a".to_string()))),
                CompoundFieldValue::Property(Some(&PropValue::Int(1))),
            ],
        )
        .unwrap();
        let mut grouped = BTreeMap::new();
        grouped.insert(key, vec![3u64, 2u64]);
        let mut data = Vec::new();
        let normalized = grouped.into_iter().collect::<Vec<_>>();
        let mut key_bytes = normalized[0].0.clone();
        let postings_offset = align_u64(
            COMPOUND_SIDECAR_HEADER_LEN as u64
                + 8
                + COMPOUND_KEY_TABLE_ENTRY_BYTES as u64
                + key_bytes.len() as u64,
            8,
        )
        .unwrap();
        data.resize(COMPOUND_SIDECAR_HEADER_LEN, 0);
        put_header(
            &mut data,
            &declaration,
            1,
            2,
            COMPOUND_SIDECAR_HEADER_LEN as u64,
            (8 + COMPOUND_KEY_TABLE_ENTRY_BYTES) as u64,
            (COMPOUND_SIDECAR_HEADER_LEN + 8 + COMPOUND_KEY_TABLE_ENTRY_BYTES) as u64,
            key_bytes.len() as u64,
            postings_offset,
            16,
            0,
        )
        .unwrap();
        data.extend_from_slice(&1u64.to_le_bytes());
        data.extend_from_slice(&0u64.to_le_bytes());
        data.extend_from_slice(&(key_bytes.len() as u32).to_le_bytes());
        data.extend_from_slice(&0u64.to_le_bytes());
        data.extend_from_slice(&2u32.to_le_bytes());
        data.append(&mut key_bytes);
        while data.len() < postings_offset as usize {
            data.push(0);
        }
        data.extend_from_slice(&3u64.to_le_bytes());
        data.extend_from_slice(&2u64.to_le_bytes());
        fix_crc(&mut data);
        assert!(validate_compound_sidecar_payload(&data, &declaration).is_err());
    }

    #[test]
    fn metadata_components_encode_and_validate_by_source() {
        let entry = mixed_node_entry(11);
        let context = tuple_context(&entry);
        let key = encode_compound_tuple_key(
            &context,
            &[
                CompoundFieldValue::Property(Some(&PropValue::String("acme".to_string()))),
                CompoundFieldValue::MetadataI64(100),
            ],
        )
        .unwrap();
        let decoded = decode_compound_tuple_components(&key, context.fields).unwrap();
        assert_eq!(decoded[1].class, CompoundComponentClass::Numeric);
        let bad = encode_compound_tuple_key(
            &context,
            &[
                CompoundFieldValue::Property(Some(&PropValue::String("acme".to_string()))),
                CompoundFieldValue::MetadataString("wrong"),
            ],
        );
        assert!(bad.is_err());

        let id_entry = SecondaryIndexManifestEntry {
            index_id: 12,
            target: SecondaryIndexTarget::NodeFieldIndex {
                label_id: 7,
                fields: vec![SecondaryIndexFieldManifest::NodeMetadata {
                    field: NodeMetadataIndexFieldManifest::Id,
                }],
            },
            kind: SecondaryIndexKind::Equality,
            state: SecondaryIndexState::Ready,
            last_error: None,
        };
        let id_context = tuple_context(&id_entry);
        let id_key =
            encode_compound_tuple_key(&id_context, &[CompoundFieldValue::MetadataU64(42)]).unwrap();
        assert_eq!(
            decode_compound_tuple_components(&id_key, id_context.fields).unwrap()[0].class,
            CompoundComponentClass::Numeric
        );

        let weight_entry = SecondaryIndexManifestEntry {
            index_id: 13,
            target: SecondaryIndexTarget::NodeFieldIndex {
                label_id: 7,
                fields: vec![SecondaryIndexFieldManifest::NodeMetadata {
                    field: NodeMetadataIndexFieldManifest::Weight,
                }],
            },
            kind: SecondaryIndexKind::Equality,
            state: SecondaryIndexState::Ready,
            last_error: None,
        };
        let weight_context = tuple_context(&weight_entry);
        let finite_weight =
            encode_compound_tuple_key(&weight_context, &[CompoundFieldValue::MetadataF64(0.5)])
                .unwrap();
        assert_eq!(
            decode_compound_tuple_components(&finite_weight, weight_context.fields).unwrap()[0]
                .class,
            CompoundComponentClass::Numeric
        );
        let nonfinite_weight = encode_compound_tuple_key(
            &weight_context,
            &[CompoundFieldValue::MetadataF64(f64::NAN)],
        )
        .unwrap();
        assert_eq!(
            decode_compound_tuple_components(&nonfinite_weight, weight_context.fields).unwrap()[0]
                .class,
            CompoundComponentClass::EqualityHash
        );
    }
}
