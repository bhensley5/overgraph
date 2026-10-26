use crate::error::EngineError;
use crate::types::*;
use std::collections::BTreeMap;
use std::io::{Cursor, Read};

// --- Write helpers (little-endian) ---

fn write_u8(buf: &mut Vec<u8>, v: u8) {
    buf.push(v);
}

fn write_u16(buf: &mut Vec<u8>, v: u16) {
    buf.extend_from_slice(&v.to_le_bytes());
}

fn write_u32(buf: &mut Vec<u8>, v: u32) {
    buf.extend_from_slice(&v.to_le_bytes());
}

fn write_u64(buf: &mut Vec<u8>, v: u64) {
    buf.extend_from_slice(&v.to_le_bytes());
}

fn write_i64(buf: &mut Vec<u8>, v: i64) {
    buf.extend_from_slice(&v.to_le_bytes());
}

fn write_f32(buf: &mut Vec<u8>, v: f32) {
    buf.extend_from_slice(&v.to_le_bytes());
}

fn write_sparse_vector_entry(buf: &mut Vec<u8>, dimension_id: u32, weight: f32) {
    write_u32(buf, dimension_id);
    write_f32(buf, weight);
}

const MAX_BYTES_LEN: usize = 64 * 1024 * 1024; // 64MB safety limit

fn validate_bytes_len(length: usize) -> Result<(), EngineError> {
    if length > MAX_BYTES_LEN {
        return Err(EngineError::SerializationError(format!(
            "bytes payload too large: {} bytes (max {})",
            length, MAX_BYTES_LEN
        )));
    }
    Ok(())
}

#[cfg(test)]
fn write_bytes(buf: &mut Vec<u8>, data: &[u8]) -> Result<(), EngineError> {
    validate_bytes_len(data.len())?;
    write_u32(buf, data.len() as u32);
    buf.extend_from_slice(data);
    Ok(())
}

fn write_str(buf: &mut Vec<u8>, s: &str) -> Result<(), EngineError> {
    let bytes = s.as_bytes();
    if bytes.len() > u16::MAX as usize {
        return Err(EngineError::SerializationError(format!(
            "string too long: {} bytes (max {})",
            bytes.len(),
            u16::MAX
        )));
    }
    write_u16(buf, bytes.len() as u16);
    buf.extend_from_slice(bytes);
    Ok(())
}

// --- Read helpers (little-endian, from cursor) ---

fn read_u8(cursor: &mut Cursor<&[u8]>) -> Result<u8, EngineError> {
    let mut buf = [0u8; 1];
    cursor
        .read_exact(&mut buf)
        .map_err(|_| EngineError::CorruptRecord("unexpected EOF reading u8".into()))?;
    Ok(buf[0])
}

fn read_u16(cursor: &mut Cursor<&[u8]>) -> Result<u16, EngineError> {
    let mut buf = [0u8; 2];
    cursor
        .read_exact(&mut buf)
        .map_err(|_| EngineError::CorruptRecord("unexpected EOF reading u16".into()))?;
    Ok(u16::from_le_bytes(buf))
}

fn read_u32(cursor: &mut Cursor<&[u8]>) -> Result<u32, EngineError> {
    let mut buf = [0u8; 4];
    cursor
        .read_exact(&mut buf)
        .map_err(|_| EngineError::CorruptRecord("unexpected EOF reading u32".into()))?;
    Ok(u32::from_le_bytes(buf))
}

fn read_u64(cursor: &mut Cursor<&[u8]>) -> Result<u64, EngineError> {
    let mut buf = [0u8; 8];
    cursor
        .read_exact(&mut buf)
        .map_err(|_| EngineError::CorruptRecord("unexpected EOF reading u64".into()))?;
    Ok(u64::from_le_bytes(buf))
}

fn read_i64(cursor: &mut Cursor<&[u8]>) -> Result<i64, EngineError> {
    let mut buf = [0u8; 8];
    cursor
        .read_exact(&mut buf)
        .map_err(|_| EngineError::CorruptRecord("unexpected EOF reading i64".into()))?;
    Ok(i64::from_le_bytes(buf))
}

fn read_f32(cursor: &mut Cursor<&[u8]>) -> Result<f32, EngineError> {
    let mut buf = [0u8; 4];
    cursor
        .read_exact(&mut buf)
        .map_err(|_| EngineError::CorruptRecord("unexpected EOF reading f32".into()))?;
    Ok(f32::from_le_bytes(buf))
}

fn remaining_bytes(cursor: &Cursor<&[u8]>) -> usize {
    cursor
        .get_ref()
        .len()
        .saturating_sub(cursor.position() as usize)
}

fn read_bytes(cursor: &mut Cursor<&[u8]>) -> Result<Vec<u8>, EngineError> {
    let len = read_u32(cursor)? as usize;
    if len > MAX_BYTES_LEN {
        return Err(EngineError::CorruptRecord(format!(
            "bytes payload too large: {} (max {})",
            len, MAX_BYTES_LEN
        )));
    }
    if len > remaining_bytes(cursor) {
        return Err(EngineError::CorruptRecord(
            "unexpected EOF reading bytes".into(),
        ));
    }
    let mut buf = vec![0u8; len];
    cursor
        .read_exact(&mut buf)
        .map_err(|_| EngineError::CorruptRecord("unexpected EOF reading bytes".into()))?;
    Ok(buf)
}

fn read_str(cursor: &mut Cursor<&[u8]>) -> Result<String, EngineError> {
    let len = read_u16(cursor)? as usize;
    if len > remaining_bytes(cursor) {
        return Err(EngineError::CorruptRecord(
            "unexpected EOF reading string".into(),
        ));
    }
    let mut buf = vec![0u8; len];
    cursor
        .read_exact(&mut buf)
        .map_err(|_| EngineError::CorruptRecord("unexpected EOF reading string".into()))?;
    String::from_utf8(buf).map_err(|_| EngineError::CorruptRecord("invalid UTF-8 in string".into()))
}

fn read_node_label_set(cursor: &mut Cursor<&[u8]>) -> Result<NodeLabelSet, EngineError> {
    let count = read_u8(cursor)? as usize;
    if count == 0 {
        return Err(EngineError::CorruptRecord(
            "node WAL label_count must be at least 1".into(),
        ));
    }
    if count > MAX_NODE_LABELS_PER_NODE {
        return Err(EngineError::CorruptRecord(format!(
            "node WAL label_count {} exceeds maximum {}",
            count, MAX_NODE_LABELS_PER_NODE
        )));
    }

    let mut ids = [0u32; MAX_NODE_LABELS_PER_NODE];
    for index in 0..count {
        ids[index] = read_u32(cursor)?;
        if index > 0 && ids[index - 1] >= ids[index] {
            return Err(EngineError::CorruptRecord(
                "node WAL label IDs must be sorted ascending and unique".into(),
            ));
        }
    }
    NodeLabelSet::from_canonical_ids(&ids[..count])
        .map_err(|err| EngineError::CorruptRecord(format!("invalid node WAL label set: {err}")))
}

// --- Public API ---

/// Count the production encoding without allocating its property/vector payload.
/// A bounded writer stops an incorrect retirement trace before any encode growth.
#[cfg(feature = "production-concurrency")]
pub(crate) fn wal_op_encoded_len(op: &WalOp, limit: usize) -> Result<usize, EngineError> {
    struct Counter {
        bytes: usize,
        limit: usize,
    }
    impl std::io::Write for Counter {
        fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
            self.bytes = self
                .bytes
                .checked_add(bytes.len())
                .filter(|n| *n <= self.limit)
                .ok_or_else(|| std::io::Error::other("WAL input exceeds admitted bytes"))?;
            Ok(bytes.len())
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }
    fn props_len(props: &BTreeMap<String, PropValue>, limit: usize) -> Result<usize, EngineError> {
        let mut count = Counter { bytes: 0, limit };
        serde::Serialize::serialize(props, &mut rmp_serde::Serializer::new(&mut count))
            .map_err(|e| EngineError::SerializationError(e.to_string()))?;
        validate_bytes_len(count.bytes)?;
        Ok(count.bytes)
    }
    fn string_len(value: &str) -> Result<usize, EngineError> {
        if value.len() > u16::MAX as usize {
            return Err(EngineError::SerializationError(
                "WAL string too long".into(),
            ));
        }
        Ok(2 + value.len())
    }
    fn sum(parts: &[usize]) -> Result<usize, EngineError> {
        parts
            .iter()
            .try_fold(0usize, |n, part| n.checked_add(*part))
            .ok_or_else(|| EngineError::SerializationError("WAL size overflow".into()))
    }
    fn vector_len(length: Option<usize>, width: usize) -> Result<usize, EngineError> {
        length.map_or(Ok(0), |n| {
            n.checked_mul(width)
                .and_then(|n| n.checked_add(4))
                .ok_or_else(|| EngineError::SerializationError("WAL vector size overflow".into()))
        })
    }
    let length = match op {
        WalOp::UpsertNode(n) => sum(&[
            1 + 8 + 1 + 8 + 8 + 4 + 4 + 1,
            n.label_ids.len() * 4,
            string_len(&n.key)?,
            props_len(&n.props, limit)?,
            vector_len(n.dense_vector.as_ref().map(Vec::len), 4)?,
            vector_len(n.sparse_vector.as_ref().map(Vec::len), 8)?,
        ])?,
        WalOp::UpsertEdge(e) => sum(&[
            1 + 8 + 8 + 8 + 4 + 8 + 8 + 4 + 4 + 8 + 8,
            props_len(&e.props, limit)?,
        ])?,
        WalOp::DeleteNode { .. } | WalOp::DeleteEdge { .. } => 17,
        WalOp::EnsureNodeLabel { label, .. } | WalOp::EnsureEdgeLabel { label, .. } => {
            5 + string_len(label)?
        }
        WalOp::BeginAtomicBatch { .. } | WalOp::CommitAtomicBatch { .. } => 13,
    };
    if length > limit {
        return Err(EngineError::SerializationError(
            "WAL input exceeds admitted bytes".into(),
        ));
    }
    Ok(length)
}

/// Encode a WalOp into the provided buffer (clears first, reuses allocation).
pub(crate) fn encode_wal_op_into(op: &WalOp, buf: &mut Vec<u8>) -> Result<(), EngineError> {
    encode_wal_op_with_props(op, buf, write_wal_props)
}

/// The governed route shares every field/tag with the existing encoder, while
/// nested property traversal uses the charged iterative continuation substrate.
#[allow(dead_code)] // Private candidate adapter; public engine wiring remains gated.
pub(crate) fn encode_wal_op_bounded(op: &WalOp, buf: &mut Vec<u8>) -> Result<(), EngineError> {
    encode_wal_op_with_props(op, buf, |buf, props| {
        let prefix = buf.len();
        write_u32(buf, 0);
        crate::engine::payload::write_properties_checked(props, buf).map_err(wal_property_error)?;
        let length = buf.len() - prefix - 4;
        validate_bytes_len(length)?;
        let length = u32::try_from(length).map_err(|_| {
            EngineError::SerializationError("WAL properties exceed u32 length".into())
        })?;
        buf[prefix..prefix + 4].copy_from_slice(&length.to_le_bytes());
        Ok(())
    })
}

/// Encode once with an allowance for this operation's complete payload. Fixed
/// fields (including vector suffixes) are checked before any property growth.
#[cfg(feature = "production-concurrency")]
pub(crate) fn encode_wal_op_to_limit(
    op: &WalOp,
    buf: &mut Vec<u8>,
    maximum: usize,
) -> Result<(), EngineError> {
    buf.clear();
    let fixed = wal_fixed_bytes(op)?;
    if fixed > maximum {
        return Err(wal_byte_limit(fixed as u64, maximum));
    }
    let property_allowance = maximum - fixed;
    encode_wal_op_with_props(op, buf, |buf, props| {
        let prefix = buf.len();
        write_u32(buf, 0);
        let mut writer = LimitedWalProperties {
            buf,
            remaining: property_allowance,
            written: 0,
            exceeded: None,
        };
        let result = crate::engine::payload::write_properties_checked(props, &mut writer);
        if let Some(encountered) = writer.exceeded {
            let requested = (fixed as u64).saturating_add(encountered);
            return Err(wal_byte_limit(requested, maximum));
        }
        result.map_err(wal_property_error)?;
        let length = writer.written;
        validate_bytes_len(length)?;
        let length = u32::try_from(length).map_err(|_| {
            EngineError::SerializationError("WAL properties exceed u32 length".into())
        })?;
        writer.buf[prefix..prefix + 4].copy_from_slice(&length.to_le_bytes());
        Ok(())
    })?;
    assert!(
        buf.len() <= maximum,
        "bounded WAL encoder exceeded its allowance"
    );
    Ok(())
}

#[cfg(feature = "production-concurrency")]
struct LimitedWalProperties<'a> {
    buf: &'a mut Vec<u8>,
    remaining: usize,
    written: usize,
    exceeded: Option<u64>,
}
#[cfg(feature = "production-concurrency")]
impl std::io::Write for LimitedWalProperties<'_> {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        if bytes.len() > self.remaining {
            self.exceeded = Some((self.written as u64).saturating_add(bytes.len() as u64));
            return Err(std::io::Error::other("request frame limit"));
        }
        self.buf.extend_from_slice(bytes);
        self.written += bytes.len();
        self.remaining -= bytes.len();
        Ok(bytes.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

#[cfg(feature = "production-concurrency")]
pub(crate) fn wal_byte_limit(requested: u64, maximum: usize) -> EngineError {
    EngineError::ResourceLimit {
        resource: "max_batch_bytes",
        requested,
        limit: maximum as u64,
        retryable: false,
    }
}

// Raw/fixed fields only: no property traversal, serialization or allocation.
#[cfg(feature = "production-concurrency")]
fn wal_fixed_bytes(op: &WalOp) -> Result<usize, EngineError> {
    fn string_bytes(s: &str) -> Result<usize, EngineError> {
        if s.len() > u16::MAX as usize {
            return Err(EngineError::SerializationError(format!(
                "string too long: {} bytes (max {})",
                s.len(),
                u16::MAX
            )));
        }
        Ok(2 + s.len())
    }
    fn vector_bytes(n: Option<usize>, width: usize) -> Result<usize, EngineError> {
        n.map_or(Ok(0), |n| {
            n.checked_mul(width)
                .and_then(|v| v.checked_add(4))
                .ok_or_else(|| EngineError::SerializationError("WAL vector size overflow".into()))
        })
    }
    let (base, key, dense, sparse) = match op {
        WalOp::UpsertNode(n) => (
            1 + 8 + 1 + n.label_ids.len() * 4 + 8 + 8 + 4 + 4 + 1,
            string_bytes(&n.key)?,
            vector_bytes(n.dense_vector.as_ref().map(Vec::len), 4)?,
            vector_bytes(n.sparse_vector.as_ref().map(Vec::len), 8)?,
        ),
        WalOp::UpsertEdge(_) => (69, 0, 0, 0),
        WalOp::DeleteNode { .. } | WalOp::DeleteEdge { .. } => (17, 0, 0, 0),
        WalOp::EnsureNodeLabel { label, .. } | WalOp::EnsureEdgeLabel { label, .. } => {
            (5, string_bytes(label)?, 0, 0)
        }
        WalOp::BeginAtomicBatch { .. } | WalOp::CommitAtomicBatch { .. } => (13, 0, 0, 0),
    };
    [key, dense, sparse]
        .into_iter()
        .try_fold(base, |n, v| n.checked_add(v))
        .ok_or_else(|| EngineError::SerializationError("WAL fixed size overflow".into()))
}

fn wal_property_error(error: crate::engine::payload::PayloadError) -> EngineError {
    match error {
        crate::engine::payload::PayloadError::Resource {
            resource,
            requested,
            limit,
            retryable,
        } => EngineError::ResourceLimit {
            resource,
            requested: requested as u64,
            limit: limit as u64,
            retryable,
        },
        crate::engine::payload::PayloadError::Closed => {
            EngineError::InvalidOperation("closed property scratch".into())
        }
        _ => EngineError::SerializationError("invalid governed WAL property encoding".into()),
    }
}

fn encode_wal_op_with_props(
    op: &WalOp,
    buf: &mut Vec<u8>,
    mut properties: impl FnMut(&mut Vec<u8>, &BTreeMap<String, PropValue>) -> Result<(), EngineError>,
) -> Result<(), EngineError> {
    buf.clear();

    match op {
        WalOp::UpsertNode(node) => {
            write_u8(buf, OpTag::UpsertNode as u8);
            write_u64(buf, node.id);
            write_u8(buf, node.label_ids.len() as u8);
            for &label_id in node.label_ids.as_slice() {
                write_u32(buf, label_id);
            }
            write_str(buf, &node.key)?;
            write_i64(buf, node.created_at);
            write_i64(buf, node.updated_at);
            write_f32(buf, node.weight);
            properties(buf, &node.props)?;
            let mut flags = 0u8;
            if node.dense_vector.is_some() {
                flags |= 0b0000_0001;
            }
            if node.sparse_vector.is_some() {
                flags |= 0b0000_0010;
            }
            write_u8(buf, flags);
            if let Some(dense) = &node.dense_vector {
                write_u32(buf, dense.len() as u32);
                for &value in dense {
                    write_f32(buf, value);
                }
            }
            if let Some(sparse) = &node.sparse_vector {
                write_u32(buf, sparse.len() as u32);
                for &(dimension_id, weight) in sparse {
                    write_sparse_vector_entry(buf, dimension_id, weight);
                }
            }
        }
        WalOp::UpsertEdge(edge) => {
            write_u8(buf, OpTag::UpsertEdge as u8);
            write_u64(buf, edge.id);
            write_u64(buf, edge.from);
            write_u64(buf, edge.to);
            write_u32(buf, edge.label_id);
            write_i64(buf, edge.created_at);
            write_i64(buf, edge.updated_at);
            write_f32(buf, edge.weight);
            properties(buf, &edge.props)?;
            // Temporal fields are decoded from the trailing payload when present.
            write_i64(buf, edge.valid_from);
            write_i64(buf, edge.valid_to);
        }
        WalOp::DeleteNode { id, deleted_at } => {
            write_u8(buf, OpTag::DeleteNode as u8);
            write_u64(buf, *id);
            write_i64(buf, *deleted_at);
        }
        WalOp::DeleteEdge { id, deleted_at } => {
            write_u8(buf, OpTag::DeleteEdge as u8);
            write_u64(buf, *id);
            write_i64(buf, *deleted_at);
        }
        WalOp::EnsureNodeLabel { label, label_id } => {
            write_u8(buf, OpTag::EnsureNodeLabel as u8);
            write_str(buf, label)?;
            write_u32(buf, *label_id);
        }
        WalOp::EnsureEdgeLabel { label, label_id } => {
            write_u8(buf, OpTag::EnsureEdgeLabel as u8);
            write_str(buf, label)?;
            write_u32(buf, *label_id);
        }
        WalOp::BeginAtomicBatch {
            first_seq,
            op_count,
        } => {
            write_u8(buf, OpTag::BeginAtomicBatch as u8);
            write_u64(buf, *first_seq);
            write_u32(buf, *op_count);
        }
        WalOp::CommitAtomicBatch {
            first_seq,
            op_count,
        } => {
            write_u8(buf, OpTag::CommitAtomicBatch as u8);
            write_u64(buf, *first_seq);
            write_u32(buf, *op_count);
        }
    }

    Ok(())
}

// Same MessagePack serializer/options as rmp_serde::to_vec, written directly
// into the caller's registered encode buffer. No substantial temporary property
// byte vector crosses a fallible encoder operation or foreground scope exit.
fn write_wal_props(
    buf: &mut Vec<u8>,
    props: &std::collections::BTreeMap<String, crate::types::PropValue>,
) -> Result<(), EngineError> {
    let prefix = buf.len();
    write_u32(buf, 0);
    serde::Serialize::serialize(props, &mut rmp_serde::Serializer::new(&mut *buf))
        .map_err(|e| EngineError::SerializationError(e.to_string()))?;
    let length = buf.len() - prefix - 4;
    validate_bytes_len(length)?;
    let length = u32::try_from(length)
        .map_err(|_| EngineError::SerializationError("WAL properties exceed u32 length".into()))?;
    buf[prefix..prefix + 4].copy_from_slice(&length.to_le_bytes());
    Ok(())
}

/// Encode a WalOp into a new buffer. Convenience wrapper around `encode_wal_op_into`.
#[cfg(test)]
pub(crate) fn encode_wal_op(op: &WalOp) -> Result<Vec<u8>, EngineError> {
    let mut buf = Vec::new();
    encode_wal_op_into(op, &mut buf)?;
    Ok(buf)
}

fn reject_trailing_bytes(cursor: &Cursor<&[u8]>, context: &str) -> Result<(), EngineError> {
    if remaining_bytes(cursor) != 0 {
        return Err(EngineError::CorruptRecord(format!(
            "unexpected trailing bytes in {context}"
        )));
    }
    Ok(())
}

/// Decode a binary payload into a WalOp.
pub(crate) fn decode_wal_op(data: &[u8]) -> Result<WalOp, EngineError> {
    let mut cursor = Cursor::new(data);

    let op_tag = read_u8(&mut cursor)?;

    match OpTag::from_u8(op_tag) {
        Some(OpTag::UpsertNode) => {
            let id = read_u64(&mut cursor)?;
            let label_ids = read_node_label_set(&mut cursor)?;
            let key = read_str(&mut cursor)?;
            let created_at = read_i64(&mut cursor)?;
            let updated_at = read_i64(&mut cursor)?;
            let weight = read_f32(&mut cursor)?;
            let props_bytes = read_bytes(&mut cursor)?;
            let props: BTreeMap<String, PropValue> = rmp_serde::from_slice(&props_bytes)
                .map_err(|e| EngineError::SerializationError(e.to_string()))?;
            let (dense_vector, sparse_vector) = if remaining_bytes(&cursor) == 0 {
                (None, None)
            } else {
                let flags = read_u8(&mut cursor)?;
                if flags & !0b0000_0011 != 0 {
                    return Err(EngineError::CorruptRecord(format!(
                        "invalid vector flags on node WAL op: {:#010b}",
                        flags
                    )));
                }

                let dense_vector = if flags & 0b0000_0001 != 0 {
                    let len = read_u32(&mut cursor)? as usize;
                    if len
                        .checked_mul(std::mem::size_of::<f32>())
                        .is_none_or(|bytes| bytes > remaining_bytes(&cursor))
                    {
                        return Err(EngineError::CorruptRecord(
                            "unexpected EOF reading f32".into(),
                        ));
                    }
                    let mut values = Vec::with_capacity(len);
                    for _ in 0..len {
                        values.push(read_f32(&mut cursor)?);
                    }
                    Some(values)
                } else {
                    None
                };

                let sparse_vector = if flags & 0b0000_0010 != 0 {
                    let len = read_u32(&mut cursor)? as usize;
                    let remaining = remaining_bytes(&cursor);
                    if len.checked_mul(8).is_none_or(|bytes| bytes > remaining) {
                        // Preserve which primitive the original loop reaches
                        // first in a partial (u32 dimension, f32 weight) pair.
                        return Err(EngineError::CorruptRecord(
                            if remaining % 8 < 4 {
                                "unexpected EOF reading u32"
                            } else {
                                "unexpected EOF reading f32"
                            }
                            .into(),
                        ));
                    }
                    let mut values = Vec::with_capacity(len);
                    for _ in 0..len {
                        let dimension_id = read_u32(&mut cursor)?;
                        let weight = read_f32(&mut cursor)?;
                        values.push((dimension_id, weight));
                    }
                    Some(values)
                } else {
                    None
                };

                reject_trailing_bytes(&cursor, "node WAL op")?;

                (dense_vector, sparse_vector)
            };

            Ok(WalOp::UpsertNode(NodeRecord {
                id,
                label_ids,
                key,
                props,
                created_at,
                updated_at,
                weight,
                dense_vector,
                sparse_vector,
                last_write_seq: 0,
            }))
        }
        Some(OpTag::UpsertEdge) => {
            let id = read_u64(&mut cursor)?;
            let from = read_u64(&mut cursor)?;
            let to = read_u64(&mut cursor)?;
            let label_id = read_u32(&mut cursor)?;
            let created_at = read_i64(&mut cursor)?;
            let updated_at = read_i64(&mut cursor)?;
            let weight = read_f32(&mut cursor)?;
            let props_bytes = read_bytes(&mut cursor)?;
            let props: BTreeMap<String, PropValue> = rmp_serde::from_slice(&props_bytes)
                .map_err(|e| EngineError::SerializationError(e.to_string()))?;

            let valid_from = read_i64(&mut cursor)?;
            let valid_to = read_i64(&mut cursor)?;
            reject_trailing_bytes(&cursor, "edge WAL op")?;

            Ok(WalOp::UpsertEdge(EdgeRecord {
                id,
                from,
                to,
                label_id,
                props,
                created_at,
                updated_at,
                weight,
                valid_from,
                valid_to,
                last_write_seq: 0,
            }))
        }
        Some(OpTag::DeleteNode) => {
            let id = read_u64(&mut cursor)?;
            let deleted_at = read_i64(&mut cursor)?;
            reject_trailing_bytes(&cursor, "delete-node WAL op")?;
            Ok(WalOp::DeleteNode { id, deleted_at })
        }
        Some(OpTag::DeleteEdge) => {
            let id = read_u64(&mut cursor)?;
            let deleted_at = read_i64(&mut cursor)?;
            reject_trailing_bytes(&cursor, "delete-edge WAL op")?;
            Ok(WalOp::DeleteEdge { id, deleted_at })
        }
        Some(OpTag::EnsureNodeLabel) => {
            let label = read_str(&mut cursor)?;
            let label_id = read_u32(&mut cursor)?;
            reject_trailing_bytes(&cursor, "ensure-node-label WAL op")?;
            Ok(WalOp::EnsureNodeLabel { label, label_id })
        }
        Some(OpTag::EnsureEdgeLabel) => {
            let label = read_str(&mut cursor)?;
            let label_id = read_u32(&mut cursor)?;
            reject_trailing_bytes(&cursor, "ensure-edge-label WAL op")?;
            Ok(WalOp::EnsureEdgeLabel { label, label_id })
        }
        Some(OpTag::BeginAtomicBatch) => {
            let first_seq = read_u64(&mut cursor)?;
            let op_count = read_u32(&mut cursor)?;
            reject_trailing_bytes(&cursor, "begin-atomic-batch WAL op")?;
            Ok(WalOp::BeginAtomicBatch {
                first_seq,
                op_count,
            })
        }
        Some(OpTag::CommitAtomicBatch) => {
            let first_seq = read_u64(&mut cursor)?;
            let op_count = read_u32(&mut cursor)?;
            reject_trailing_bytes(&cursor, "commit-atomic-batch WAL op")?;
            Ok(WalOp::CommitAtomicBatch {
                first_seq,
                op_count,
            })
        }
        None => Err(EngineError::CorruptRecord(format!(
            "unknown op tag: {}",
            op_tag
        ))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeMap;

    fn node_with_untrusted_vector_hint(flags: u8, count: u32, tail: &[u8]) -> Vec<u8> {
        let mut encoded = encode_wal_op(&WalOp::UpsertNode(NodeRecord {
            id: 1,
            label_ids: NodeLabelSet::single(1).unwrap(),
            key: String::new(),
            props: BTreeMap::new(),
            created_at: 0,
            updated_at: 0,
            weight: 1.0,
            dense_vector: None,
            sparse_vector: None,
            last_write_seq: 0,
        }))
        .unwrap();
        assert_eq!(encoded.last(), Some(&0));
        *encoded.last_mut().unwrap() = flags;
        encoded.extend_from_slice(&count.to_le_bytes());
        encoded.extend_from_slice(tail);
        encoded
    }

    fn assert_corrupt_eof<T: std::fmt::Debug>(result: Result<T, EngineError>, expected: &str) {
        match result {
            Err(EngineError::CorruptRecord(message)) => assert_eq!(message, expected),
            other => panic!("expected {expected:?}, got {other:?}"),
        }
    }

    #[test]
    fn p45a2_wal_decode_hints_check_available_extent_and_preserve_eof_errors() {
        let mut encoded = (MAX_BYTES_LEN as u32).to_le_bytes().to_vec();
        encoded.push(0x80);
        assert_corrupt_eof(
            read_bytes(&mut Cursor::new(&encoded)),
            "unexpected EOF reading bytes",
        );
        let encoded = u16::MAX.to_le_bytes();
        assert_corrupt_eof(
            read_str(&mut Cursor::new(&encoded)),
            "unexpected EOF reading string",
        );
        assert_corrupt_eof(
            decode_wal_op(&node_with_untrusted_vector_hint(1, u32::MAX, &[])),
            "unexpected EOF reading f32",
        );
        assert_corrupt_eof(
            decode_wal_op(&node_with_untrusted_vector_hint(2, u32::MAX, &[])),
            "unexpected EOF reading u32",
        );
        // Sparse pairs preserve the first missing primitive for every byte cut.
        let entry = [3_u32.to_le_bytes(), 0.5_f32.to_le_bytes()].concat();
        for present in 0..8 {
            assert_corrupt_eof(
                decode_wal_op(&node_with_untrusted_vector_hint(2, 1, &entry[..present])),
                if present < 4 {
                    "unexpected EOF reading u32"
                } else {
                    "unexpected EOF reading f32"
                },
            );
        }
        let dense = node_with_untrusted_vector_hint(1, 1, &0.5_f32.to_le_bytes());
        assert!(matches!(decode_wal_op(&dense), Ok(WalOp::UpsertNode(node))
            if node.dense_vector == Some(vec![0.5])));
        let sparse = node_with_untrusted_vector_hint(2, 1, &entry);
        assert!(matches!(decode_wal_op(&sparse), Ok(WalOp::UpsertNode(node))
            if node.sparse_vector == Some(vec![(3, 0.5)])));
        for flags in [1, 2] {
            assert!(decode_wal_op(&node_with_untrusted_vector_hint(flags, 0, &[])).is_ok());
        }
    }

    #[test]
    fn test_roundtrip_upsert_node() {
        let mut props = BTreeMap::new();
        props.insert("name".to_string(), PropValue::String("Alice".to_string()));
        props.insert("age".to_string(), PropValue::Int(30));

        let op = WalOp::UpsertNode(NodeRecord {
            id: 42,
            label_ids: NodeLabelSet::single(1).unwrap(),
            key: "user:alice".to_string(),
            props,
            created_at: 1000000,
            updated_at: 1000001,
            weight: 0.95,
            dense_vector: None,
            sparse_vector: None,
            last_write_seq: 0,
        });

        let encoded = encode_wal_op(&op).unwrap();
        assert_eq!(encoded[9], 1);
        assert_eq!(u32::from_le_bytes(encoded[10..14].try_into().unwrap()), 1);
        let decoded = decode_wal_op(&encoded).unwrap();

        match decoded {
            WalOp::UpsertNode(node) => {
                assert_eq!(node.id, 42);
                assert_eq!(node.label_ids.as_slice(), &[1]);
                assert_eq!(node.key, "user:alice");
                assert_eq!(node.created_at, 1000000);
                assert_eq!(node.updated_at, 1000001);
                assert!((node.weight - 0.95).abs() < f32::EPSILON);
                assert_eq!(
                    node.props.get("name"),
                    Some(&PropValue::String("Alice".to_string()))
                );
                assert_eq!(node.props.get("age"), Some(&PropValue::Int(30)));
            }
            _ => panic!("expected UpsertNode"),
        }
    }

    #[test]
    fn test_roundtrip_upsert_node_with_vectors() {
        let op = WalOp::UpsertNode(NodeRecord {
            id: 7,
            label_ids: NodeLabelSet::single(2).unwrap(),
            key: "vector-node".to_string(),
            props: BTreeMap::new(),
            created_at: 10,
            updated_at: 11,
            weight: 0.5,
            dense_vector: Some(vec![0.1, 0.2, 0.3]),
            sparse_vector: Some(vec![(4, 1.5), (9, 2.5)]),
            last_write_seq: 0,
        });

        let encoded = encode_wal_op(&op).unwrap();
        let decoded = decode_wal_op(&encoded).unwrap();

        match decoded {
            WalOp::UpsertNode(node) => {
                assert_eq!(node.dense_vector, Some(vec![0.1, 0.2, 0.3]));
                assert_eq!(node.sparse_vector, Some(vec![(4, 1.5), (9, 2.5)]));
            }
            _ => panic!("expected UpsertNode"),
        }
    }

    #[test]
    fn test_roundtrip_upsert_node_with_multi_label_sets() {
        let cases: &[&[u32]] = &[&[2, 5], &[10, 11, 12, 13, 14, 15, 16, 17, 18, 19]];

        for &label_ids in cases {
            let op = WalOp::UpsertNode(NodeRecord {
                id: 100 + label_ids.len() as u64,
                label_ids: NodeLabelSet::from_canonical_ids(label_ids).unwrap(),
                key: format!("multi-label-{}", label_ids.len()),
                props: BTreeMap::new(),
                created_at: 10,
                updated_at: 11,
                weight: 0.5,
                dense_vector: None,
                sparse_vector: None,
                last_write_seq: 0,
            });

            let encoded = encode_wal_op(&op).unwrap();
            assert_eq!(encoded[9], label_ids.len() as u8);
            let decoded = decode_wal_op(&encoded).unwrap();

            match decoded {
                WalOp::UpsertNode(node) => {
                    assert_eq!(node.label_ids.as_slice(), label_ids);
                    assert_eq!(node.key, format!("multi-label-{}", label_ids.len()));
                }
                _ => panic!("expected UpsertNode"),
            }
        }
    }

    #[test]
    fn test_decode_legacy_upsert_node_without_vector_payload_is_rejected() {
        let mut props = BTreeMap::new();
        props.insert("name".to_string(), PropValue::String("legacy".to_string()));

        let mut encoded = Vec::new();
        write_u8(&mut encoded, OpTag::UpsertNode as u8);
        write_u64(&mut encoded, 42);
        write_u32(&mut encoded, 1);
        write_str(&mut encoded, "legacy").unwrap();
        write_i64(&mut encoded, 100);
        write_i64(&mut encoded, 101);
        write_f32(&mut encoded, 0.75);
        let props_bytes = rmp_serde::to_vec(&props).unwrap();
        write_bytes(&mut encoded, &props_bytes).unwrap();

        let err = decode_wal_op(&encoded).unwrap_err();
        assert!(
            matches!(
                err,
                EngineError::CorruptRecord(_) | EngineError::SerializationError(_)
            ),
            "unexpected legacy WAL error: {err}"
        );
    }

    #[test]
    fn test_decode_upsert_node_rejects_invalid_label_sets() {
        fn encode_with_labels(labels: &[u32]) -> Vec<u8> {
            let mut encoded = Vec::new();
            write_u8(&mut encoded, OpTag::UpsertNode as u8);
            write_u64(&mut encoded, 42);
            write_u8(&mut encoded, labels.len() as u8);
            for &label_id in labels {
                write_u32(&mut encoded, label_id);
            }
            write_str(&mut encoded, "bad").unwrap();
            write_i64(&mut encoded, 1);
            write_i64(&mut encoded, 2);
            write_f32(&mut encoded, 1.0);
            let props_bytes = rmp_serde::to_vec(&BTreeMap::<String, PropValue>::new()).unwrap();
            write_bytes(&mut encoded, &props_bytes).unwrap();
            write_u8(&mut encoded, 0);
            encoded
        }

        for labels in [&[][..], &[2, 1][..], &[1, 1][..], &[0][..]] {
            let err = decode_wal_op(&encode_with_labels(labels)).unwrap_err();
            assert!(
                err.to_string().contains("node WAL"),
                "unexpected error for {labels:?}: {err}"
            );
        }
    }

    #[test]
    fn test_roundtrip_upsert_edge() {
        let mut props = BTreeMap::new();
        props.insert("role".to_string(), PropValue::String("owner".to_string()));

        let op = WalOp::UpsertEdge(EdgeRecord {
            id: 100,
            from: 1,
            to: 2,
            label_id: 10,
            props,
            created_at: 2000000,
            updated_at: 2000001,
            weight: 1.0,
            valid_from: 1500000,
            valid_to: 3000000,
            last_write_seq: 0,
        });

        let encoded = encode_wal_op(&op).unwrap();
        let decoded = decode_wal_op(&encoded).unwrap();

        match decoded {
            WalOp::UpsertEdge(edge) => {
                assert_eq!(edge.id, 100);
                assert_eq!(edge.from, 1);
                assert_eq!(edge.to, 2);
                assert_eq!(edge.label_id, 10);
                assert_eq!(edge.created_at, 2000000);
                assert_eq!(edge.updated_at, 2000001);
                assert!((edge.weight - 1.0).abs() < f32::EPSILON);
                assert_eq!(edge.valid_from, 1500000);
                assert_eq!(edge.valid_to, 3000000);
                assert_eq!(
                    edge.props.get("role"),
                    Some(&PropValue::String("owner".to_string()))
                );
            }
            _ => panic!("expected UpsertEdge"),
        }
    }

    #[test]
    fn test_roundtrip_delete_node() {
        let op = WalOp::DeleteNode {
            id: 42,
            deleted_at: 3000000,
        };
        let encoded = encode_wal_op(&op).unwrap();
        let decoded = decode_wal_op(&encoded).unwrap();

        match decoded {
            WalOp::DeleteNode { id, deleted_at } => {
                assert_eq!(id, 42);
                assert_eq!(deleted_at, 3000000);
            }
            _ => panic!("expected DeleteNode"),
        }
    }

    #[test]
    fn test_roundtrip_delete_edge() {
        let op = WalOp::DeleteEdge {
            id: 99,
            deleted_at: 4000000,
        };
        let encoded = encode_wal_op(&op).unwrap();
        let decoded = decode_wal_op(&encoded).unwrap();

        match decoded {
            WalOp::DeleteEdge { id, deleted_at } => {
                assert_eq!(id, 99);
                assert_eq!(deleted_at, 4000000);
            }
            _ => panic!("expected DeleteEdge"),
        }
    }

    #[test]
    fn test_roundtrip_ensure_node_label() {
        let op = WalOp::EnsureNodeLabel {
            label: "Person".to_string(),
            label_id: 7,
        };
        let encoded = encode_wal_op(&op).unwrap();
        let decoded = decode_wal_op(&encoded).unwrap();

        match decoded {
            WalOp::EnsureNodeLabel { label, label_id } => {
                assert_eq!(label, "Person");
                assert_eq!(label_id, 7);
            }
            _ => panic!("expected EnsureNodeLabel"),
        }
    }

    #[test]
    fn test_roundtrip_ensure_edge_label() {
        let op = WalOp::EnsureEdgeLabel {
            label: "KNOWS".to_string(),
            label_id: 11,
        };
        let encoded = encode_wal_op(&op).unwrap();
        let decoded = decode_wal_op(&encoded).unwrap();

        match decoded {
            WalOp::EnsureEdgeLabel { label, label_id } => {
                assert_eq!(label, "KNOWS");
                assert_eq!(label_id, 11);
            }
            _ => panic!("expected EnsureEdgeLabel"),
        }
    }

    #[test]
    fn test_roundtrip_atomic_batch_markers() {
        let begin = WalOp::BeginAtomicBatch {
            first_seq: 42,
            op_count: 3,
        };
        let encoded = encode_wal_op(&begin).unwrap();
        let decoded = decode_wal_op(&encoded).unwrap();
        match decoded {
            WalOp::BeginAtomicBatch {
                first_seq,
                op_count,
            } => {
                assert_eq!(first_seq, 42);
                assert_eq!(op_count, 3);
            }
            _ => panic!("expected BeginAtomicBatch"),
        }

        let commit = WalOp::CommitAtomicBatch {
            first_seq: 42,
            op_count: 3,
        };
        let encoded = encode_wal_op(&commit).unwrap();
        let decoded = decode_wal_op(&encoded).unwrap();
        match decoded {
            WalOp::CommitAtomicBatch {
                first_seq,
                op_count,
            } => {
                assert_eq!(first_seq, 42);
                assert_eq!(op_count, 3);
            }
            _ => panic!("expected CommitAtomicBatch"),
        }
    }

    #[test]
    fn test_roundtrip_empty_props() {
        let op = WalOp::UpsertNode(NodeRecord {
            id: 1,
            label_ids: NodeLabelSet::single(1).unwrap(),
            key: "test".to_string(),
            props: BTreeMap::new(),
            created_at: 0,
            updated_at: 0,
            weight: 0.0,
            dense_vector: None,
            sparse_vector: None,
            last_write_seq: 0,
        });

        let encoded = encode_wal_op(&op).unwrap();
        let decoded = decode_wal_op(&encoded).unwrap();

        match decoded {
            WalOp::UpsertNode(node) => {
                assert!(node.props.is_empty());
            }
            _ => panic!("expected UpsertNode"),
        }
    }

    #[test]
    #[allow(clippy::approx_constant)]
    fn test_roundtrip_all_prop_types() {
        let mut props = BTreeMap::new();
        props.insert("null_val".to_string(), PropValue::Null);
        props.insert("bool_val".to_string(), PropValue::Bool(true));
        props.insert("int_val".to_string(), PropValue::Int(-42));
        props.insert("uint_val".to_string(), PropValue::UInt(999));
        props.insert("float_val".to_string(), PropValue::Float(3.14));
        props.insert(
            "string_val".to_string(),
            PropValue::String("hello".to_string()),
        );
        props.insert("bytes_val".to_string(), PropValue::Bytes(vec![1, 2, 3]));
        props.insert(
            "array_val".to_string(),
            PropValue::Array(vec![
                PropValue::Int(1),
                PropValue::String("two".to_string()),
            ]),
        );

        let op = WalOp::UpsertNode(NodeRecord {
            id: 1,
            label_ids: NodeLabelSet::single(1).unwrap(),
            key: "test".to_string(),
            props,
            created_at: 0,
            updated_at: 0,
            weight: 1.0,
            dense_vector: None,
            sparse_vector: None,
            last_write_seq: 0,
        });

        let encoded = encode_wal_op(&op).unwrap();
        let decoded = decode_wal_op(&encoded).unwrap();

        match decoded {
            WalOp::UpsertNode(node) => {
                assert_eq!(node.props.len(), 8);
                assert_eq!(node.props.get("null_val"), Some(&PropValue::Null));
                assert_eq!(node.props.get("bool_val"), Some(&PropValue::Bool(true)));
                assert_eq!(node.props.get("int_val"), Some(&PropValue::Int(-42)));
                assert_eq!(node.props.get("uint_val"), Some(&PropValue::UInt(999)));
                if let Some(PropValue::Float(f)) = node.props.get("float_val") {
                    assert!((f - 3.14).abs() < f64::EPSILON);
                } else {
                    panic!("expected Float");
                }
                assert_eq!(
                    node.props.get("string_val"),
                    Some(&PropValue::String("hello".to_string()))
                );
                assert_eq!(
                    node.props.get("bytes_val"),
                    Some(&PropValue::Bytes(vec![1, 2, 3]))
                );
            }
            _ => panic!("expected UpsertNode"),
        }
    }

    #[test]
    fn test_invalid_op_tag() {
        let data = vec![255u8, 0, 0, 0, 0, 0, 0, 0, 0];
        let result = decode_wal_op(&data);
        assert!(result.is_err());
    }

    #[test]
    fn test_truncated_data() {
        let data = vec![1u8, 0, 0]; // UpsertNode tag but truncated
        let result = decode_wal_op(&data);
        assert!(result.is_err());
    }

    #[test]
    fn test_node_encoding_deterministic_size() {
        // A delete op should always be exactly 1 + 8 + 8 = 17 bytes
        let op = WalOp::DeleteNode {
            id: 1,
            deleted_at: 1000,
        };
        let encoded = encode_wal_op(&op).unwrap();
        assert_eq!(encoded.len(), 17);
    }

    // --- Regression tests for M6: overflow protection ---

    #[test]
    fn test_string_key_overflow_rejected() {
        // A key longer than u16::MAX (65535) bytes must return an error, not silently truncate
        let long_key = "x".repeat(65536);
        let op = WalOp::UpsertNode(NodeRecord {
            id: 1,
            label_ids: NodeLabelSet::single(1).unwrap(),
            key: long_key,
            props: BTreeMap::new(),
            created_at: 0,
            updated_at: 0,
            weight: 0.0,
            dense_vector: None,
            sparse_vector: None,
            last_write_seq: 0,
        });
        let result = encode_wal_op(&op);
        assert!(result.is_err());
        let err_msg = format!("{}", result.unwrap_err());
        assert!(err_msg.contains("string too long"));
    }

    #[test]
    fn test_string_key_at_max_length_ok() {
        // Exactly u16::MAX bytes should be fine
        let max_key = "x".repeat(u16::MAX as usize);
        let op = WalOp::UpsertNode(NodeRecord {
            id: 1,
            label_ids: NodeLabelSet::single(1).unwrap(),
            key: max_key.clone(),
            props: BTreeMap::new(),
            created_at: 0,
            updated_at: 0,
            weight: 0.0,
            dense_vector: None,
            sparse_vector: None,
            last_write_seq: 0,
        });
        let encoded = encode_wal_op(&op).unwrap();
        let decoded = decode_wal_op(&encoded).unwrap();
        match decoded {
            WalOp::UpsertNode(node) => assert_eq!(node.key.len(), u16::MAX as usize),
            _ => panic!("expected UpsertNode"),
        }
    }

    #[test]
    fn test_encode_wal_op_into_reuses_buffer() {
        let mut buf = Vec::new();

        let op1 = WalOp::DeleteNode {
            id: 1,
            deleted_at: 1000,
        };
        encode_wal_op_into(&op1, &mut buf).unwrap();
        let ptr1 = buf.as_ptr();
        let cap1 = buf.capacity();

        // Second encode should reuse the same allocation
        let op2 = WalOp::DeleteNode {
            id: 2,
            deleted_at: 2000,
        };
        encode_wal_op_into(&op2, &mut buf).unwrap();

        // If capacity was sufficient, pointer should be the same (allocation reused)
        assert_eq!(buf.as_ptr(), ptr1);
        assert_eq!(buf.capacity(), cap1);

        // Content should be for op2, not op1
        let decoded = decode_wal_op(&buf).unwrap();
        match decoded {
            WalOp::DeleteNode { id, .. } => assert_eq!(id, 2),
            _ => panic!("expected DeleteNode"),
        }
    }

    #[test]
    fn test_wal_props_wire_compatibility_and_size_limit() {
        let props = BTreeMap::from([
            ("bytes".into(), PropValue::Bytes(vec![0, 255])),
            ("integer".into(), PropValue::Int(-42)),
            ("text".into(), PropValue::String("hello".into())),
        ]);
        let legacy = rmp_serde::to_vec(&props).unwrap();
        let mut expected = vec![7, 8];
        write_bytes(&mut expected, &legacy).unwrap();
        let mut direct = vec![7, 8];
        write_wal_props(&mut direct, &props).unwrap();
        assert_eq!(direct, expected);
        assert!(validate_bytes_len(MAX_BYTES_LEN).is_ok());
        let error = validate_bytes_len(MAX_BYTES_LEN + 1).unwrap_err();
        assert!(error.to_string().contains("bytes payload too large"));
    }

    #[test]
    fn test_read_bytes_rejects_oversized_length() {
        // Craft a payload with a u32 length field claiming 128MB
        // followed by insufficient data. Should error, not OOM
        let mut data = Vec::new();
        let huge_len: u32 = 128 * 1024 * 1024;
        data.extend_from_slice(&huge_len.to_le_bytes());
        data.extend_from_slice(&[0u8; 16]); // only 16 bytes of actual data
        let mut cursor = Cursor::new(data.as_slice());
        let result = read_bytes(&mut cursor);
        assert!(result.is_err());
    }
}
