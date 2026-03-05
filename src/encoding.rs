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

const MAX_BYTES_LEN: usize = 64 * 1024 * 1024; // 64MB safety limit

fn write_bytes(buf: &mut Vec<u8>, data: &[u8]) -> Result<(), EngineError> {
    if data.len() > MAX_BYTES_LEN {
        return Err(EngineError::SerializationError(format!(
            "bytes payload too large: {} bytes (max {})",
            data.len(),
            MAX_BYTES_LEN
        )));
    }
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

fn read_bytes(cursor: &mut Cursor<&[u8]>) -> Result<Vec<u8>, EngineError> {
    let len = read_u32(cursor)? as usize;
    if len > MAX_BYTES_LEN {
        return Err(EngineError::CorruptRecord(format!(
            "bytes payload too large: {} (max {})",
            len, MAX_BYTES_LEN
        )));
    }
    let mut buf = vec![0u8; len];
    cursor
        .read_exact(&mut buf)
        .map_err(|_| EngineError::CorruptRecord("unexpected EOF reading bytes".into()))?;
    Ok(buf)
}

fn read_str(cursor: &mut Cursor<&[u8]>) -> Result<String, EngineError> {
    let len = read_u16(cursor)? as usize;
    let mut buf = vec![0u8; len];
    cursor
        .read_exact(&mut buf)
        .map_err(|_| EngineError::CorruptRecord("unexpected EOF reading string".into()))?;
    String::from_utf8(buf)
        .map_err(|_| EngineError::CorruptRecord("invalid UTF-8 in string".into()))
}

// --- Public API ---

/// Encode a WalOp into the provided buffer (clears first, reuses allocation).
pub fn encode_wal_op_into(op: &WalOp, buf: &mut Vec<u8>) -> Result<(), EngineError> {
    buf.clear();

    match op {
        WalOp::UpsertNode(node) => {
            write_u8(buf, OpTag::UpsertNode as u8);
            write_u64(buf, node.id);
            write_u32(buf, node.type_id);
            write_str(buf, &node.key)?;
            write_i64(buf, node.created_at);
            write_i64(buf, node.updated_at);
            write_f32(buf, node.weight);
            let props_bytes = rmp_serde::to_vec(&node.props)
                .map_err(|e| EngineError::SerializationError(e.to_string()))?;
            write_bytes(buf, &props_bytes)?;
        }
        WalOp::UpsertEdge(edge) => {
            write_u8(buf, OpTag::UpsertEdge as u8);
            write_u64(buf, edge.id);
            write_u64(buf, edge.from);
            write_u64(buf, edge.to);
            write_u32(buf, edge.type_id);
            write_i64(buf, edge.created_at);
            write_i64(buf, edge.updated_at);
            write_f32(buf, edge.weight);
            let props_bytes = rmp_serde::to_vec(&edge.props)
                .map_err(|e| EngineError::SerializationError(e.to_string()))?;
            write_bytes(buf, &props_bytes)?;
            // Temporal fields (added in v2, detected by trailing bytes on decode)
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
    }

    Ok(())
}

/// Encode a WalOp into a new buffer. Convenience wrapper around `encode_wal_op_into`.
pub fn encode_wal_op(op: &WalOp) -> Result<Vec<u8>, EngineError> {
    let mut buf = Vec::new();
    encode_wal_op_into(op, &mut buf)?;
    Ok(buf)
}

/// Decode a binary payload into a WalOp.
pub fn decode_wal_op(data: &[u8]) -> Result<WalOp, EngineError> {
    let mut cursor = Cursor::new(data);

    let op_tag = read_u8(&mut cursor)?;

    match OpTag::from_u8(op_tag) {
        Some(OpTag::UpsertNode) => {
            let id = read_u64(&mut cursor)?;
            let type_id = read_u32(&mut cursor)?;
            let key = read_str(&mut cursor)?;
            let created_at = read_i64(&mut cursor)?;
            let updated_at = read_i64(&mut cursor)?;
            let weight = read_f32(&mut cursor)?;
            let props_bytes = read_bytes(&mut cursor)?;
            let props: BTreeMap<String, PropValue> = rmp_serde::from_slice(&props_bytes)
                .map_err(|e| EngineError::SerializationError(e.to_string()))?;

            Ok(WalOp::UpsertNode(NodeRecord {
                id,
                type_id,
                key,
                props,
                created_at,
                updated_at,
                weight,
            }))
        }
        Some(OpTag::UpsertEdge) => {
            let id = read_u64(&mut cursor)?;
            let from = read_u64(&mut cursor)?;
            let to = read_u64(&mut cursor)?;
            let type_id = read_u32(&mut cursor)?;
            let created_at = read_i64(&mut cursor)?;
            let updated_at = read_i64(&mut cursor)?;
            let weight = read_f32(&mut cursor)?;
            let props_bytes = read_bytes(&mut cursor)?;
            let props: BTreeMap<String, PropValue> = rmp_serde::from_slice(&props_bytes)
                .map_err(|e| EngineError::SerializationError(e.to_string()))?;

            let valid_from = read_i64(&mut cursor)?;
            let valid_to = read_i64(&mut cursor)?;

            Ok(WalOp::UpsertEdge(EdgeRecord {
                id,
                from,
                to,
                type_id,
                props,
                created_at,
                updated_at,
                weight,
                valid_from,
                valid_to,
            }))
        }
        Some(OpTag::DeleteNode) => {
            let id = read_u64(&mut cursor)?;
            let deleted_at = read_i64(&mut cursor)?;
            Ok(WalOp::DeleteNode { id, deleted_at })
        }
        Some(OpTag::DeleteEdge) => {
            let id = read_u64(&mut cursor)?;
            let deleted_at = read_i64(&mut cursor)?;
            Ok(WalOp::DeleteEdge { id, deleted_at })
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

    #[test]
    fn test_roundtrip_upsert_node() {
        let mut props = BTreeMap::new();
        props.insert("name".to_string(), PropValue::String("Alice".to_string()));
        props.insert("age".to_string(), PropValue::Int(30));

        let op = WalOp::UpsertNode(NodeRecord {
            id: 42,
            type_id: 1,
            key: "user:alice".to_string(),
            props,
            created_at: 1000000,
            updated_at: 1000001,
            weight: 0.95,
        });

        let encoded = encode_wal_op(&op).unwrap();
        let decoded = decode_wal_op(&encoded).unwrap();

        match decoded {
            WalOp::UpsertNode(node) => {
                assert_eq!(node.id, 42);
                assert_eq!(node.type_id, 1);
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
    fn test_roundtrip_upsert_edge() {
        let mut props = BTreeMap::new();
        props.insert("role".to_string(), PropValue::String("owner".to_string()));

        let op = WalOp::UpsertEdge(EdgeRecord {
            id: 100,
            from: 1,
            to: 2,
            type_id: 10,
            props,
            created_at: 2000000,
            updated_at: 2000001,
            weight: 1.0,
            valid_from: 1500000,
            valid_to: 3000000,
        });

        let encoded = encode_wal_op(&op).unwrap();
        let decoded = decode_wal_op(&encoded).unwrap();

        match decoded {
            WalOp::UpsertEdge(edge) => {
                assert_eq!(edge.id, 100);
                assert_eq!(edge.from, 1);
                assert_eq!(edge.to, 2);
                assert_eq!(edge.type_id, 10);
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
    fn test_roundtrip_empty_props() {
        let op = WalOp::UpsertNode(NodeRecord {
            id: 1,
            type_id: 1,
            key: "test".to_string(),
            props: BTreeMap::new(),
            created_at: 0,
            updated_at: 0,
            weight: 0.0,
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
            type_id: 1,
            key: "test".to_string(),
            props,
            created_at: 0,
            updated_at: 0,
            weight: 1.0,
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
            type_id: 1,
            key: long_key,
            props: BTreeMap::new(),
            created_at: 0,
            updated_at: 0,
            weight: 0.0,
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
            type_id: 1,
            key: max_key.clone(),
            props: BTreeMap::new(),
            created_at: 0,
            updated_at: 0,
            weight: 0.0,
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

        let op1 = WalOp::DeleteNode { id: 1, deleted_at: 1000 };
        encode_wal_op_into(&op1, &mut buf).unwrap();
        let ptr1 = buf.as_ptr();
        let cap1 = buf.capacity();

        // Second encode should reuse the same allocation
        let op2 = WalOp::DeleteNode { id: 2, deleted_at: 2000 };
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
