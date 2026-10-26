//! Stack-safe private ownership for the existing public property representation.
//! This module is normal-compiled; a3 supplies whole-engine entry/result routing.
#![allow(dead_code)]

pub(crate) mod json;
pub(crate) mod messagepack;

use crate::types::{
    EdgeRecord, EdgeSchemaManifestEntry, ManifestState, NodeRecord, NodeSchemaManifestEntry,
    NumericFieldSchemaManifestRule, PropValue, PropertySchemaManifestRule,
    SchemaNumericBoundManifest, WalOp,
};
use std::collections::BTreeMap;
use std::ops::{Deref, DerefMut};

#[derive(Debug, PartialEq, Eq)]
pub(crate) enum PayloadError {
    Resource {
        resource: &'static str,
        requested: usize,
        limit: usize,
        retryable: bool,
    },
    Closed,
    PersistedRoundTrip,
    Wire,
}

/// The only lifetime-bearing workspace. T is verified to have no drop glue:
/// no strings, roots, Arc, callbacks or owned partial trees may enter it.
struct Frames<T> {
    inline: [Option<T>; 8],
    len: usize,
    heap: Vec<T>,
}
impl<T> Frames<T> {
    fn new() -> Self {
        const { assert!(!std::mem::needs_drop::<T>()) };
        Self {
            inline: std::array::from_fn(|_| None),
            len: 0,
            heap: Vec::new(),
        }
    }
    fn push(&mut self, frame: T) -> Result<(), PayloadError> {
        if self.heap.capacity() == 0 && self.len < self.inline.len() {
            self.inline[self.len] = Some(frame);
            self.len += 1;
            return Ok(());
        }
        if self.heap.capacity() == 0 || self.heap.len() == self.heap.capacity() {
            let capacity =
                self.heap
                    .capacity()
                    .max(8)
                    .checked_mul(2)
                    .ok_or(PayloadError::Resource {
                        resource: "allocation_capacity",
                        requested: usize::MAX,
                        limit: usize::MAX,
                        retryable: false,
                    })?;
            let bytes =
                capacity
                    .checked_mul(std::mem::size_of::<T>())
                    .ok_or(PayloadError::Resource {
                        resource: "allocation_capacity",
                        requested: usize::MAX,
                        limit: usize::MAX,
                        retryable: false,
                    })?;
            // Old and new grants coexist until the old allocation is freed.
            let mut next = Vec::new();
            next.try_reserve_exact(capacity)
                .map_err(|_| PayloadError::Resource {
                    resource: "allocation_capacity",
                    requested: bytes,
                    limit: bytes,
                    retryable: false,
                })?;
            assert_eq!(next.capacity(), capacity, "exact RawVec request");
            if self.heap.capacity() == 0 {
                for item in &mut self.inline[..self.len] {
                    next.push(item.take().expect("inline continuation"));
                }
            } else {
                next.append(&mut self.heap);
            }
            let old = std::mem::replace(&mut self.heap, next);
            drop(old);
            // No graph root is involved in this one-backing foreground free.
        }
        self.heap.push(frame);
        self.len += 1;
        Ok(())
    }
    fn last_mut(&mut self) -> Option<&mut T> {
        if self.heap.capacity() != 0 {
            self.heap.last_mut()
        } else {
            self.len
                .checked_sub(1)
                .and_then(|index| self.inline[index].as_mut())
        }
    }
    fn pop(&mut self) -> Option<T> {
        self.len = self.len.checked_sub(1)?;
        if self.heap.capacity() != 0 {
            self.heap.pop()
        } else {
            self.inline[self.len].take()
        }
    }
}
impl<T> Drop for Frames<T> {
    fn drop(&mut self) {
        drop(std::mem::take(&mut self.heap));
    }
}

enum Continuation<'a> {
    Array(std::slice::Iter<'a, PropValue>),
    Map(std::collections::btree_map::Iter<'a, String, PropValue>),
}
const _: () = assert!(!std::mem::needs_drop::<Continuation<'static>>());

enum Visit<'a> {
    Value(&'a PropValue, usize),
    Key(&'a String),
    End,
}
struct Walk<'a> {
    frames: Frames<Continuation<'a>>,
    pending: Option<&'a PropValue>,
}
impl<'a> Walk<'a> {
    fn new(value: &'a PropValue) -> Self {
        Self {
            frames: Frames::new(),
            pending: Some(value),
        }
    }
    fn next(&mut self) -> Result<Option<Visit<'a>>, PayloadError> {
        loop {
            if let Some(value) = self.pending.take() {
                let depth = self.frames.len;
                match value {
                    PropValue::Array(values) => {
                        self.frames.push(Continuation::Array(values.iter()))?
                    }
                    PropValue::Map(values) => self.frames.push(Continuation::Map(values.iter()))?,
                    _ => {}
                }
                return Ok(Some(Visit::Value(value, depth)));
            }
            match self.frames.last_mut() {
                Some(Continuation::Array(values)) => match values.next() {
                    Some(value) => self.pending = Some(value),
                    None => {
                        self.frames.pop();
                        return Ok(Some(Visit::End));
                    }
                },
                Some(Continuation::Map(values)) => match values.next() {
                    Some((key, value)) => {
                        self.pending = Some(value);
                        return Ok(Some(Visit::Key(key)));
                    }
                    None => {
                        self.frames.pop();
                        return Ok(Some(Visit::End));
                    }
                },
                None => return Ok(None),
            }
        }
    }
}

#[derive(Default, Debug, Clone, Copy)]
pub(crate) struct Inspection {
    /// Independent copied backing: String/Vec clone requests live length,
    /// not the source's retained spare capacity. Nonempty BTree clone may keep
    /// its source topology, so it uses the shared mutation-history node bound.
    pub(crate) values: usize,
    /// Canonical MessagePack typed depth, including Bytes sequences. This also
    /// conservatively covers the clone's owned continuation backing.
    pub(crate) max_containers: usize,
    pub(crate) json_depth: usize,
    pub(crate) nonfinite: bool,
    pub(crate) float_zero: bool,
}

fn checked_add(target: &mut usize, bytes: usize) -> Result<(), PayloadError> {
    *target = target.checked_add(bytes).ok_or(PayloadError::Resource {
        resource: "allocation_capacity",
        requested: usize::MAX,
        limit: usize::MAX,
        retryable: false,
    })?;
    Ok(())
}

/// One visit computes physical capacity and compatibility metadata together.
/// Container shells already belong to the parent's allocation. The BTree node
/// Compatibility metadata is checked without retaining allocation inventories.
pub(crate) fn inspect(value: &PropValue) -> Result<Inspection, PayloadError> {
    let mut out = Inspection::default();
    let mut walk = Walk::new(value);
    while let Some(event) = walk.next()? {
        match event {
            Visit::Value(value, depth) => {
                checked_add(&mut out.values, 1)?;
                let json_leaf = match value {
                    PropValue::Null => 0,
                    PropValue::Array(_) | PropValue::Map(_) | PropValue::Bytes(_) => 2,
                    _ => 1,
                };
                out.json_depth = out.json_depth.max(
                    depth
                        .checked_mul(2)
                        .and_then(|n| n.checked_add(json_leaf))
                        .ok_or(PayloadError::PersistedRoundTrip)?,
                );
                match value {
                    PropValue::Array(_) | PropValue::Map(_) | PropValue::Bytes(_) => {
                        out.max_containers = out.max_containers.max(depth + 1);
                    }
                    PropValue::Float(v) => {
                        out.nonfinite |= !v.is_finite();
                        out.float_zero |= *v == 0.0;
                    }
                    _ => {}
                }
            }
            Visit::Key(key) => {
                let _ = key;
            }
            Visit::End => {}
        }
    }
    Ok(out)
}

pub(crate) fn validate_persisted(
    inspection: Inspection,
    outer_containers: usize,
) -> Result<(), PayloadError> {
    // rmp-serde's initial 1024 budget rejects when a typed payload decrements
    // to zero. Enum wrapper maps do not debit it. Canonical record props add 1.
    if inspection
        .max_containers
        .checked_add(outer_containers)
        .is_none_or(|n| n >= 1024)
    {
        Err(PayloadError::PersistedRoundTrip)
    } else {
        Ok(())
    }
}

pub(crate) fn validate_json(
    inspection: Inspection,
    enclosing_depth: usize,
) -> Result<(), PayloadError> {
    if inspection
        .json_depth
        .checked_add(enclosing_depth)
        .is_none_or(|n| n >= 128)
    {
        Err(PayloadError::PersistedRoundTrip)
    } else {
        Ok(())
    }
}

/// Bound the finite JSON encoder before it can recurse into a caller-composed
/// literal. Stop at the first incompatible prefix, so rejected arbitrarily deep
/// inputs need only the existing bounded, root-free continuation workspace.
fn validate_json_value(value: &PropValue, enclosing_depth: usize) -> Result<(), PayloadError> {
    let mut walk = Walk::new(value);
    while let Some(event) = walk.next()? {
        if let Visit::Value(value, depth) = event {
            let leaf_depth = match value {
                PropValue::Null => 0,
                PropValue::Array(_) | PropValue::Map(_) | PropValue::Bytes(_) => 2,
                _ => 1,
            };
            let json_depth = depth
                .checked_mul(2)
                .and_then(|n| n.checked_add(leaf_depth))
                .ok_or(PayloadError::PersistedRoundTrip)?;
            validate_json(
                Inspection {
                    json_depth,
                    ..Inspection::default()
                },
                enclosing_depth,
            )?;
            // serde_json writes a nonfinite float as null, which the unchanged
            // PropValue::Float reader cannot accept.
            if matches!(value, PropValue::Float(v) if !v.is_finite()) {
                return Err(PayloadError::Wire);
            }
        }
    }
    Ok(())
}

pub(crate) fn validate_manifest_json(state: &ManifestState) -> Result<(), PayloadError> {
    fn property(rule: &PropertySchemaManifestRule) -> Result<(), PayloadError> {
        // Manifest root, schema array/entry, properties map/rule, then either
        // enum_values array or numeric bound object: six enclosing containers.
        for value in &rule.enum_values {
            validate_json_value(value, 6)?;
        }
        for bound in [&rule.numeric_min, &rule.numeric_max].into_iter().flatten() {
            validate_json_value(&bound.value, 6)?;
        }
        Ok(())
    }
    fn weight(rule: &Option<NumericFieldSchemaManifestRule>) -> Result<(), PayloadError> {
        if let Some(rule) = rule {
            // Root, schema array/entry, weight rule and numeric bound object.
            for bound in [&rule.min, &rule.max].into_iter().flatten() {
                validate_json_value(&bound.value, 5)?;
            }
        }
        Ok(())
    }
    for schema in &state.node_schemas {
        for rule in schema.properties.values() {
            property(rule)?;
        }
        weight(&schema.weight)?;
    }
    for schema in &state.edge_schemas {
        for rule in schema.properties.values() {
            property(rule)?;
        }
        weight(&schema.weight)?;
    }
    Ok(())
}

/// The same writer is used with count, byte and hash sinks. No nested serde call.
pub(crate) fn write_value<W: std::io::Write>(
    value: &PropValue,
    sink: &mut W,
) -> Result<(), PayloadError> {
    write_value_checked(value, sink, 0)
}

fn write_value_checked<W: std::io::Write>(
    value: &PropValue,
    sink: &mut W,
    enclosing: usize,
) -> Result<(), PayloadError> {
    let mut walk = Walk::new(value);
    while let Some(event) = walk.next()? {
        if enclosing != 0 {
            if let Visit::Value(
                PropValue::Array(_) | PropValue::Map(_) | PropValue::Bytes(_),
                depth,
            ) = &event
            {
                if depth
                    .checked_add(1)
                    .and_then(|n| n.checked_add(enclosing))
                    .is_none_or(|n| n >= 1024)
                {
                    return Err(PayloadError::PersistedRoundTrip);
                }
            }
        }
        let result = match event {
            Visit::Value(PropValue::Null, _) => {
                write_str(sink, "Null").map_err(|_| PayloadError::Wire)
            }
            Visit::Value(value, _) => {
                write_map_len(sink, 1).map_err(|_| PayloadError::Wire)?;
                let tag = match value {
                    PropValue::Bool(_) => "Bool",
                    PropValue::Int(_) => "Int",
                    PropValue::UInt(_) => "UInt",
                    PropValue::Float(_) => "Float",
                    PropValue::String(_) => "String",
                    PropValue::Bytes(_) => "Bytes",
                    PropValue::Array(_) => "Array",
                    PropValue::Map(_) => "Map",
                    PropValue::Null => unreachable!(),
                };
                write_str(sink, tag).map_err(|_| PayloadError::Wire)?;
                match value {
                    PropValue::Bool(v) => write_bool(sink, *v).map_err(|_| PayloadError::Wire),
                    PropValue::Int(v) => write_sint(sink, *v)
                        .map(|_| ())
                        .map_err(|_| PayloadError::Wire),
                    PropValue::UInt(v) => write_uint(sink, *v)
                        .map(|_| ())
                        .map_err(|_| PayloadError::Wire),
                    PropValue::Float(v) => write_f64(sink, *v).map_err(|_| PayloadError::Wire),
                    PropValue::String(v) => write_str(sink, v).map_err(|_| PayloadError::Wire),
                    PropValue::Bytes(v) => {
                        write_array_len(
                            sink,
                            u32::try_from(v.len()).map_err(|_| PayloadError::Wire)?,
                        )
                        .map_err(|_| PayloadError::Wire)?;
                        for byte in v {
                            write_uint(sink, u64::from(*byte)).map_err(|_| PayloadError::Wire)?;
                        }
                        Ok(())
                    }
                    PropValue::Array(v) => write_array_len(
                        sink,
                        u32::try_from(v.len()).map_err(|_| PayloadError::Wire)?,
                    )
                    .map_err(|_| PayloadError::Wire),
                    PropValue::Map(v) => write_map_len(
                        sink,
                        u32::try_from(v.len()).map_err(|_| PayloadError::Wire)?,
                    )
                    .map_err(|_| PayloadError::Wire),
                    PropValue::Null => unreachable!(),
                }
            }
            Visit::Key(key) => write_str(sink, key).map_err(|_| PayloadError::Wire),
            Visit::End => Ok(()),
        };
        result?;
    }
    Ok(())
}

// Pinned rmp-serde scalar calls keep marker/coercion behavior in one backend.
// These inputs cannot recurse. Container headers reproduce rmp 0.8.15
// encode/mod.rs:251–292; no new direct/transitive dependency is introduced.
fn scalar<W: std::io::Write, T: serde::Serialize + ?Sized>(
    sink: &mut W,
    value: &T,
) -> Result<(), PayloadError> {
    value
        .serialize(&mut rmp_serde::Serializer::new(sink))
        .map_err(|_| PayloadError::Wire)
}
fn write_str<W: std::io::Write>(sink: &mut W, value: &str) -> Result<(), PayloadError> {
    scalar(sink, value)
}
fn write_bool<W: std::io::Write>(sink: &mut W, value: bool) -> Result<(), PayloadError> {
    scalar(sink, &value)
}
fn write_sint<W: std::io::Write>(sink: &mut W, value: i64) -> Result<(), PayloadError> {
    scalar(sink, &value)
}
fn write_uint<W: std::io::Write>(sink: &mut W, value: u64) -> Result<(), PayloadError> {
    scalar(sink, &value)
}
fn write_f64<W: std::io::Write>(sink: &mut W, value: f64) -> Result<(), PayloadError> {
    scalar(sink, &value)
}
fn write_array_len<W: std::io::Write>(sink: &mut W, len: u32) -> Result<(), PayloadError> {
    write_len(sink, len, 0x90, 0xdc, 0xdd)
}
fn write_map_len<W: std::io::Write>(sink: &mut W, len: u32) -> Result<(), PayloadError> {
    write_len(sink, len, 0x80, 0xde, 0xdf)
}
fn write_len<W: std::io::Write>(
    sink: &mut W,
    len: u32,
    fix: u8,
    small: u8,
    large: u8,
) -> Result<(), PayloadError> {
    if len < 16 {
        sink.write_all(&[fix | len as u8])
            .map_err(|_| PayloadError::Wire)
    } else if let Ok(len) = u16::try_from(len) {
        sink.write_all(&[small])
            .and_then(|_| sink.write_all(&len.to_be_bytes()))
            .map_err(|_| PayloadError::Wire)
    } else {
        sink.write_all(&[large])
            .and_then(|_| sink.write_all(&len.to_be_bytes()))
            .map_err(|_| PayloadError::Wire)
    }
}

#[derive(Default)]
pub(crate) struct CountSink(pub(crate) usize);
impl std::io::Write for CountSink {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        self.0 = self
            .0
            .checked_add(bytes.len())
            .ok_or_else(|| std::io::Error::other("payload size overflow"))?;
        Ok(bytes.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

pub(crate) struct HashSink(pub(crate) u64);
impl Default for HashSink {
    fn default() -> Self {
        Self(0xcbf29ce484222325)
    }
}

pub(crate) fn semantic_hash(value: &PropValue) -> Result<u64, PayloadError> {
    use std::io::Write;
    let mut sink = HashSink::default();
    if let Some(key) = crate::property_value_semantics::numeric_key(value) {
        sink.write_all(&[1]).map_err(|_| PayloadError::Wire)?;
        crate::property_value_semantics::write_numeric_key_bytes(&mut sink, key)
            .map_err(|_| PayloadError::Wire)?;
    } else {
        sink.write_all(&[2]).map_err(|_| PayloadError::Wire)?;
        write_value(value, &mut sink)?;
    }
    Ok(sink.0)
}

pub(crate) fn semantic_eq(left: &PropValue, right: &PropValue) -> Result<bool, PayloadError> {
    match (
        crate::property_value_semantics::numeric_key(left),
        crate::property_value_semantics::numeric_key(right),
    ) {
        (Some(left), Some(right)) => Ok(left == right),
        _ => compare_values::<true>(left, right),
    }
}

pub(crate) fn write_properties<W: std::io::Write>(
    properties: &BTreeMap<String, PropValue>,
    sink: &mut W,
) -> Result<(), PayloadError> {
    write_properties_with_depth(properties, sink, 0)
}

pub(crate) fn write_properties_checked<W: std::io::Write>(
    properties: &BTreeMap<String, PropValue>,
    sink: &mut W,
) -> Result<(), PayloadError> {
    write_properties_with_depth(properties, sink, 1)
}

fn write_properties_with_depth<W: std::io::Write>(
    properties: &BTreeMap<String, PropValue>,
    sink: &mut W,
    enclosing: usize,
) -> Result<(), PayloadError> {
    write_map_len(
        sink,
        u32::try_from(properties.len()).map_err(|_| PayloadError::Wire)?,
    )?;
    for (key, value) in properties {
        write_str(sink, key)?;
        write_value_checked(value, sink, enclosing)?;
    }
    Ok(())
}
impl std::io::Write for HashSink {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        for byte in bytes {
            self.0 ^= u64::from(*byte);
            self.0 = self.0.wrapping_mul(0x100000001b3);
        }
        Ok(bytes.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

enum Construction {
    Array(Owned<Vec<PropValue>>),
    Map {
        values: Owned<BTreeMap<String, PropValue>>,
        key: String,
    },
}
impl Drain for Construction {
    fn drain_payload(&mut self) {
        match self {
            Self::Array(values) => values.drain_payload(),
            Self::Map { values, .. } => values.drain_payload(),
        }
    }
}

/// Owned partials stay under iterative RAII, unlike borrowed continuation
/// frames. The first eight levels live in its inline shell. All deeper frames,
/// partial children and pending keys remain here across errors and unwinds.
pub(crate) struct CloneState {
    pub(crate) root: Owned<PropValue>,
    inline: [Option<Construction>; 8],
    frames: Owned<Vec<Construction>>,
    displaced: Owned<Vec<PropValue>>,
    len: usize,
}
impl Default for CloneState {
    fn default() -> Self {
        Self {
            root: Owned::default(),
            inline: std::array::from_fn(|_| None),
            frames: Owned::default(),
            displaced: Owned::default(),
            len: 0,
        }
    }
}
impl CloneState {
    fn push(&mut self, frame: Construction) -> Result<(), PayloadError> {
        if self.frames.capacity() == 0 && self.len < 8 {
            self.inline[self.len] = Some(frame);
        } else {
            if self.frames.capacity() == 0 || self.frames.len() == self.frames.capacity() {
                let capacity = self
                    .frames
                    .capacity()
                    .max(8)
                    .checked_mul(2)
                    .ok_or(PayloadError::Wire)?;
                // Charge the new backing in full while old backing coexists.

                let additional = capacity - self.frames.len();
                self.frames
                    .try_reserve_exact(additional)
                    .map_err(|_| PayloadError::Wire)?;
                if self.len <= 8 && self.frames.is_empty() {
                    for item in &mut self.inline[..self.len] {
                        self.frames.push(item.take().expect("inline construction"));
                    }
                }
            }
            self.frames.push(frame);
        }
        self.len += 1;
        Ok(())
    }
    fn last_mut(&mut self) -> Option<&mut Construction> {
        if self.frames.capacity() != 0 {
            self.frames.last_mut()
        } else {
            self.len
                .checked_sub(1)
                .and_then(|index| self.inline[index].as_mut())
        }
    }
    fn pop(&mut self) -> Option<Construction> {
        self.len = self.len.checked_sub(1)?;
        if self.frames.capacity() != 0 {
            self.frames.pop()
        } else {
            self.inline[self.len].take()
        }
    }
    fn attach(&mut self) {
        if self.len == 0 {
            return;
        }
        let child = std::mem::replace(&mut *self.root, PropValue::Null);
        match self.last_mut().expect("construction parent") {
            Construction::Array(values) => values.push(child),
            Construction::Map { values, key } => {
                // Input map keys are unique; no owned root is replaced here.
                let displaced = values.insert(std::mem::take(key), child);
                assert!(displaced.is_none(), "unique cloned map key");
            }
        }
    }
    /// Clone once with checked, fallible growth and guarded partial values.
    pub(crate) fn clone_value(&mut self, value: &PropValue) -> Result<(), PayloadError> {
        assert_eq!(self.len, 0, "single partial construction");
        assert!(matches!(*self.root, PropValue::Null));
        let mut walk = Walk::new(value);
        while let Some(event) = walk.next()? {
            match event {
                Visit::Value(PropValue::Array(values), _) => {
                    self.push(Construction::Array(Owned::default()))?;
                    if let Some(Construction::Array(output)) = self.last_mut() {
                        output
                            .try_reserve_exact(values.len())
                            .map_err(|_| PayloadError::Wire)?;
                    }
                }
                Visit::Value(PropValue::Map(_values), _) => {
                    // A fresh empty clone has no root allocation. The retained
                    // input may still own an empty BTree root after removals.
                    self.push(Construction::Map {
                        values: Owned::default(),
                        key: String::new(),
                    })?;
                }
                Visit::Value(value, _) => {
                    match value {
                        PropValue::String(text) => {
                            *self.root = PropValue::String(String::new());
                            if let PropValue::String(output) = &mut *self.root {
                                output
                                    .try_reserve_exact(text.len())
                                    .map_err(|_| PayloadError::Wire)?;
                                output.push_str(text);
                            }
                        }
                        PropValue::Bytes(bytes) => {
                            *self.root = PropValue::Bytes(Vec::new());
                            if let PropValue::Bytes(output) = &mut *self.root {
                                output
                                    .try_reserve_exact(bytes.len())
                                    .map_err(|_| PayloadError::Wire)?;
                                output.extend_from_slice(bytes);
                            }
                        }
                        PropValue::Null => *self.root = PropValue::Null,
                        PropValue::Bool(v) => *self.root = PropValue::Bool(*v),
                        PropValue::Int(v) => *self.root = PropValue::Int(*v),
                        PropValue::UInt(v) => *self.root = PropValue::UInt(*v),
                        PropValue::Float(v) => *self.root = PropValue::Float(*v),
                        _ => unreachable!(),
                    }
                    self.attach();
                }
                Visit::Key(key) => {
                    if let Some(Construction::Map { key: output, .. }) = self.last_mut() {
                        output
                            .try_reserve_exact(key.len())
                            .map_err(|_| PayloadError::Wire)?;
                        output.push_str(key);
                    } else {
                        unreachable!("map key context")
                    }
                }
                Visit::End => {
                    let frame = self.pop().expect("completed construction");
                    *self.root = match frame {
                        Construction::Array(values) => PropValue::Array(values.into_inner()),
                        Construction::Map { values, key } => {
                            assert!(key.is_empty());
                            PropValue::Map(values.into_inner())
                        }
                    };
                    self.attach();
                }
            }
        }
        Ok(())
    }
}

pub(crate) fn structural_eq(left: &PropValue, right: &PropValue) -> Result<bool, PayloadError> {
    compare_values::<false>(left, right)
}

/// Match the public derived PartialEq, while semantic equality above retains
/// bitwise nested float identity used by persisted equality index keys.
fn compare_values<const RAW_FLOAT: bool>(
    left: &PropValue,
    right: &PropValue,
) -> Result<bool, PayloadError> {
    let mut left = Walk::new(left);
    let mut right = Walk::new(right);
    loop {
        let equal = match (left.next()?, right.next()?) {
            (None, None) => return Ok(true),
            (Some(Visit::End), Some(Visit::End)) => true,
            (Some(Visit::Key(left)), Some(Visit::Key(right))) => left == right,
            (Some(Visit::Value(left, _)), Some(Visit::Value(right, _))) => match (left, right) {
                (PropValue::Null, PropValue::Null) => true,
                (PropValue::Bool(l), PropValue::Bool(r)) => l == r,
                (PropValue::Int(l), PropValue::Int(r)) => l == r,
                (PropValue::UInt(l), PropValue::UInt(r)) => l == r,
                (PropValue::Float(l), PropValue::Float(r)) => {
                    if RAW_FLOAT {
                        !l.is_nan() && !r.is_nan() && l.to_bits() == r.to_bits()
                    } else {
                        l == r
                    }
                }
                (PropValue::String(l), PropValue::String(r)) => l == r,
                (PropValue::Bytes(l), PropValue::Bytes(r)) => l == r,
                (PropValue::Array(l), PropValue::Array(r)) => l.len() == r.len(),
                (PropValue::Map(l), PropValue::Map(r)) => l.len() == r.len(),
                _ => false,
            },
            _ => false,
        };
        if !equal {
            return Ok(false);
        }
    }
}

/// Explicit inventory: implement only for closed payload-bearing internal types.
/// An implementation empties recursive fields before ordinary drop glue runs.
pub(crate) trait Drain {
    fn drain_payload(&mut self);
}

/// Allocation-free ownership transfer. Before admission this drains original
/// caller work; registered instances must be destroyed by the domain worker.
/// It does not confer a reservation or authorize a payload allocation.
pub(crate) struct Owned<T: Drain> {
    value: Option<T>,
}

impl<T: Drain> Owned<T> {
    pub(crate) fn new(value: T) -> Self {
        Self { value: Some(value) }
    }
    /// The receiving guarded/registered owner must exist before this transfer.
    pub(crate) fn into_inner(mut self) -> T {
        self.value.take().expect("owned payload transfer once")
    }
}
impl<T: Drain> Deref for Owned<T> {
    type Target = T;
    fn deref(&self) -> &T {
        self.value.as_ref().expect("live payload guard")
    }
}
impl<T: Drain> DerefMut for Owned<T> {
    fn deref_mut(&mut self) -> &mut T {
        self.value.as_mut().expect("live payload guard")
    }
}
impl<T: Drain> Drop for Owned<T> {
    fn drop(&mut self) {
        if let Some(value) = self.value.as_mut() {
            value.drain_payload();
        }
    }
}
impl<T: Drain> Default for Owned<Vec<T>> {
    fn default() -> Self {
        Self::new(Vec::new())
    }
}
impl<'a, T: Drain> IntoIterator for &'a Owned<Vec<T>> {
    type Item = &'a T;
    type IntoIter = std::slice::Iter<'a, T>;
    fn into_iter(self) -> Self::IntoIter {
        self.iter()
    }
}
impl<T: Drain> Default for Owned<Option<T>> {
    fn default() -> Self {
        Self::new(None)
    }
}
impl Default for Owned<PropValue> {
    fn default() -> Self {
        Self::new(PropValue::Null)
    }
}
impl<T: Drain> Default for Owned<BTreeMap<String, T>> {
    fn default() -> Self {
        Self::new(BTreeMap::new())
    }
}

#[derive(Default, Debug)]
struct DrainWork {
    entered_containers: usize,
    inspected_children: usize,
    continuation_children: usize,
}

fn leaf_or_empty(value: &PropValue) -> bool {
    match value {
        PropValue::Array(values) => values.is_empty(),
        PropValue::Map(values) => values.is_empty(),
        _ => true,
    }
}

/// Use the existing last-child slot as the older-parent link, leaving only
/// current/pending owned roots on the stack. Each slot is removed exactly once.
/// Rust 1.96 alloc/vec/mod.rs:2816–2825 and btree/{map,entry,remove,fix,node}.rs:
/// pop/last_entry/pop_last only move, balance in existing storage, and deallocate.
/// They never allocate. Empty/shallow map drop traverses BTree nodes iteratively.
/// `first_entry` is essential: a resumed parent must never rescan its siblings.
fn drain_value<const TRACK: bool>(mut current: PropValue) -> DrainWork {
    let mut pending = PropValue::Null;
    let mut first_entry = true;
    let mut work = DrainWork::default();
    loop {
        match current {
            PropValue::Array(mut values) => {
                if TRACK && first_entry {
                    work.entered_containers += 1;
                }
                let shallow = first_entry
                    && values.iter().all(|child| {
                        if TRACK {
                            work.inspected_children += 1;
                        }
                        leaf_or_empty(child)
                    });
                if shallow || values.is_empty() {
                    // At most one extra empty-container level reaches drop glue.
                    drop(values);
                } else {
                    let child = std::mem::replace(values.last_mut().unwrap(), pending);
                    pending = PropValue::Array(values);
                    current = child;
                    first_entry = true;
                    if TRACK {
                        work.continuation_children += 1;
                    }
                    continue;
                }
            }
            PropValue::Map(mut values) => {
                if TRACK && first_entry {
                    work.entered_containers += 1;
                }
                let shallow = first_entry
                    && values.values().all(|child| {
                        if TRACK {
                            work.inspected_children += 1;
                        }
                        leaf_or_empty(child)
                    });
                if shallow || values.is_empty() {
                    drop(values);
                } else {
                    let child = std::mem::replace(values.last_entry().unwrap().get_mut(), pending);
                    pending = PropValue::Map(values);
                    current = child;
                    first_entry = true;
                    if TRACK {
                        work.continuation_children += 1;
                    }
                    continue;
                }
            }
            leaf => drop(leaf),
        }
        match pending {
            PropValue::Null => return work,
            PropValue::Array(mut parent) => {
                pending = parent.pop().expect("drain parent continuation");
                current = PropValue::Array(parent);
            }
            PropValue::Map(mut parent) => {
                let (key, older) = parent.pop_last().expect("drain map continuation");
                drop(key);
                pending = older;
                current = PropValue::Map(parent);
            }
            _ => {
                eprintln!("production-concurrency payload invariant: invalid continuation");
                std::process::abort();
            }
        }
        first_entry = false;
    }
}

impl Drain for PropValue {
    fn drain_payload(&mut self) {
        drain_value::<false>(std::mem::replace(self, PropValue::Null));
    }
}
impl<T: Drain> Drain for Option<T> {
    fn drain_payload(&mut self) {
        if let Some(mut value) = self.take() {
            value.drain_payload();
        }
    }
}
impl<T: Drain> Drain for Vec<T> {
    fn drain_payload(&mut self) {
        while let Some(mut value) = self.pop() {
            value.drain_payload();
        }
    }
}
impl<T: Drain> Drain for (u64, T) {
    fn drain_payload(&mut self) {
        self.1.drain_payload();
    }
}
impl<T: Drain> Drain for ((u64, std::cmp::Reverse<u64>), T) {
    fn drain_payload(&mut self) {
        self.1.drain_payload();
    }
}
impl<K, T: Drain, S> Drain for std::collections::HashMap<K, T, S>
where
    K: Eq + std::hash::Hash,
    S: std::hash::BuildHasher,
{
    fn drain_payload(&mut self) {
        for (_, mut value) in self.drain() {
            value.drain_payload();
        }
    }
}
impl<T: Drain> Drain for BTreeMap<String, T> {
    fn drain_payload(&mut self) {
        while let Some((_key, mut value)) = self.pop_last() {
            value.drain_payload();
        }
    }
}
impl<T: Drain> Drain for Owned<T> {
    fn drain_payload(&mut self) {
        if let Some(value) = self.value.as_mut() {
            value.drain_payload();
        }
    }
}
impl Drain for NodeRecord {
    fn drain_payload(&mut self) {
        self.props.drain_payload();
    }
}
impl Drain for EdgeRecord {
    fn drain_payload(&mut self) {
        self.props.drain_payload();
    }
}
impl Drain for WalOp {
    fn drain_payload(&mut self) {
        match self {
            Self::UpsertNode(node) => node.drain_payload(),
            Self::UpsertEdge(edge) => edge.drain_payload(),
            _ => {}
        }
    }
}
impl Drain for SchemaNumericBoundManifest {
    fn drain_payload(&mut self) {
        self.value.drain_payload();
    }
}
impl Drain for PropertySchemaManifestRule {
    fn drain_payload(&mut self) {
        self.numeric_min.drain_payload();
        self.numeric_max.drain_payload();
        self.enum_values.drain_payload();
    }
}
impl Drain for NodeSchemaManifestEntry {
    fn drain_payload(&mut self) {
        self.properties.drain_payload();
        self.weight.drain_payload();
    }
}
impl Drain for EdgeSchemaManifestEntry {
    fn drain_payload(&mut self) {
        self.properties.drain_payload();
        self.weight.drain_payload();
    }
}
impl Drain for NumericFieldSchemaManifestRule {
    fn drain_payload(&mut self) {
        self.min.drain_payload();
        self.max.drain_payload();
    }
}
impl Drain for ManifestState {
    fn drain_payload(&mut self) {
        self.node_schemas.drain_payload();
        self.edge_schemas.drain_payload();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn chain(depth: usize, mixed: bool) -> PropValue {
        let mut root = PropValue::Bytes(vec![1; 4096]);
        for level in 0..depth {
            root = if mixed && level % 2 == 0 {
                PropValue::Map(BTreeMap::from([
                    ("a".into(), PropValue::String("scalar".into())),
                    ("z".into(), root),
                ]))
            } else {
                PropValue::Array(vec![PropValue::Array(Vec::new()), root])
            };
        }
        root
    }

    #[test]
    fn p45a2_payload_drain_deep_mixed_and_spare_capacity_on_two_mib_stack() {
        std::thread::Builder::new()
            .stack_size(2 * 1024 * 1024)
            .spawn(|| {
                for mixed in [false, true] {
                    drop(Owned::new(chain(131_072, mixed)));
                }
                let mut values = Vec::with_capacity(100_000);
                values.extend((0..100_000).map(|_| PropValue::Array(Vec::new())));
                let work = drain_value::<true>(PropValue::Array(values));
                assert_eq!(work.entered_containers, 1);
                assert_eq!(work.inspected_children, 100_000);
                assert_eq!(work.continuation_children, 0);
                let mut wide = BTreeMap::new();
                for index in 0..4096 {
                    wide.insert(index.to_string(), chain(8, false));
                }
                let work = drain_value::<true>(PropValue::Map(wide));
                // The local counters instrument the same loop with no global work.
                assert!(work.inspected_children <= 4096 * 18 + 4096);
                assert!(work.continuation_children <= 4096 * 18 + 4096);
            })
            .unwrap()
            .join()
            .unwrap();
    }

    #[cfg(feature = "production-concurrency")]
    #[test]
    fn p45a2_payload_drain_allocates_nothing_and_frees_complete_graph() {
        use super::super::participant::allocations;
        std::thread::Builder::new()
            .stack_size(2 * 1024 * 1024)
            .spawn(|| {
                let (root, allocated) = allocations::measure(|| chain(16_384, true));
                assert_eq!(allocated.1, 0);
                let (_, freed) = allocations::measure(|| drop(Owned::new(root)));
                assert_eq!(freed.0, 0, "cleanup must not allocate");
                assert_eq!(freed.2, 0);
                assert_eq!(freed.1, allocated.0);
                assert_eq!(freed.3, allocated.2);
            })
            .unwrap()
            .join()
            .unwrap();
    }

    #[test]
    fn p45a2_payload_writer_matches_pinned_bytes_and_semantics() {
        let corpus = Owned::new(vec![
            PropValue::Null,
            PropValue::Bool(false),
            PropValue::Bool(true),
            PropValue::Int(i64::MIN),
            PropValue::Int(-129),
            PropValue::Int(-33),
            PropValue::Int(128),
            PropValue::Int(i64::MAX),
            PropValue::UInt(u64::MAX),
            PropValue::Float(-0.0),
            PropValue::Float(f64::INFINITY),
            PropValue::Float(f64::from_bits(0x7ff8_0000_0000_0042)),
            PropValue::String("\0é😀".into()),
            PropValue::Bytes((0..=255).collect()),
            PropValue::Array(vec![
                PropValue::UInt(1),
                PropValue::Int(1),
                PropValue::Float(-0.0),
            ]),
            PropValue::Map(BTreeMap::from([
                ("a".into(), PropValue::Array(vec![PropValue::Null])),
                ("z".into(), PropValue::Map(BTreeMap::new())),
            ])),
        ]);
        for value in corpus.iter() {
            let expected = rmp_serde::to_vec(value).unwrap();
            let mut bytes = Vec::new();
            write_value(value, &mut bytes).unwrap();
            assert_eq!(bytes, expected);
            let mut count = CountSink::default();
            write_value(value, &mut count).unwrap();
            assert_eq!(count.0, expected.len());
            let mut hash = HashSink::default();
            write_value(value, &mut hash).unwrap();
            assert_eq!(hash.0, crate::types::hash_prop_value(value));
            assert_eq!(
                semantic_hash(value).unwrap(),
                crate::property_value_semantics::hash_prop_equality_key(value)
            );
            let mut cloned = CloneState::default();
            cloned.clone_value(value).unwrap();
            let mut cloned_bytes = Vec::new();
            write_value(&cloned.root, &mut cloned_bytes).unwrap();
            assert_eq!(cloned_bytes, expected);
        }
        let positive = PropValue::Array(vec![PropValue::Float(0.0)]);
        let negative = PropValue::Array(vec![PropValue::Float(-0.0)]);
        assert!(structural_eq(&positive, &negative).unwrap());
        assert!(!semantic_eq(&positive, &negative).unwrap());
        assert!(semantic_eq(&PropValue::Float(-0.0), &PropValue::UInt(0)).unwrap());
        for left in corpus.iter() {
            for right in corpus.iter() {
                assert_eq!(
                    semantic_eq(left, right).unwrap(),
                    crate::property_value_semantics::semantic_property_eq(left, right)
                );
                assert_eq!(structural_eq(left, right).unwrap(), left == right);
            }
        }
    }

    #[cfg(feature = "production-concurrency")]
    #[test]
    fn p45a2_payload_count_clone_encode_hash_deep_on_two_mib_stack() {
        use super::super::reclamation::{ReclaimDomain, ResourceClass};
        std::thread::Builder::new()
            .stack_size(2 * 1024 * 1024)
            .spawn(|| {
                let domain = ReclaimDomain::new();
                for mixed in [false, true] {
                    let root = Owned::new(chain(65_536, mixed));
                    let inspection = inspect(&root).unwrap();
                    assert_eq!(inspection.max_containers, 65_537);
                    assert_eq!(inspection.values, 131_073);
                    assert_eq!(
                        validate_persisted(inspection, 1),
                        Err(PayloadError::PersistedRoundTrip)
                    );
                    let mut clone = domain.build::<CloneState>(ResourceClass::Staging).unwrap();
                    clone
                        .mutate(|state| state.clone_value(&root))
                        .unwrap()
                        .unwrap();
                    let clone = clone.into_owner().unwrap();
                    assert!(structural_eq(&root, &clone.get().root).unwrap());
                    let mut count = CountSink::default();
                    write_value(&root, &mut count).unwrap();
                    assert!(count.0 > 524_288);
                    let mut expected = HashSink::default();
                    write_value(&root, &mut expected).unwrap();
                    let mut actual = HashSink::default();
                    write_value(&clone.get().root, &mut actual).unwrap();
                    assert_eq!(expected.0, actual.0);
                    drop(clone);
                    // Avoid starting the next 128 MiB reservation until actual
                    // worker completion; this test does not depend on scheduling.
                    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
                    while domain.stats().read().occupied != 0 {
                        assert!(std::time::Instant::now() < deadline);
                        std::thread::yield_now();
                    }
                }
                domain.close();
            })
            .unwrap()
            .join()
            .unwrap();
    }

    #[test]
    fn p45a2_review_m1_bytes_leaf_uses_persisted_sequence_depth() {
        use crate::engine::reclamation::ReclaimDomain;
        #[cfg(feature = "production-concurrency")]
        let _serial = crate::engine::participant::TEST_SERIAL
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        let domain = ReclaimDomain::new();
        // A low-depth pinned control distinguishes typed sequences from the
        // binary fast path without invoking the recursive backend at depth 1024.
        let prefix = [
            0x81, 0xa1, b'x', 0x81, 0xa5, b'A', b'r', b'r', b'a', b'y', 0x91, 0x81, 0xa5, b'B',
            b'y', b't', b'e', b's',
        ];
        for (body, accepted) in [
            (vec![0x91, 1], false),
            (vec![0x90], false),
            (vec![0xc4, 1, 1], true),
            (vec![0xc4, 0], true),
        ] {
            let mut bytes = prefix.to_vec();
            bytes.extend(body);
            let mut decoder = rmp_serde::Deserializer::new(&bytes[..]);
            decoder.set_max_depth(3);
            let result =
                <BTreeMap<String, PropValue> as serde::Deserialize>::deserialize(&mut decoder)
                    .map(Owned::new);
            assert_eq!(result.is_ok(), accepted);
        }
        for map in [false, true] {
            for leaf in [vec![], vec![1]] {
                for depth in [1021, 1022] {
                    let mut root = Owned::new(PropValue::Bytes(leaf.clone()));
                    for _ in 0..depth {
                        let child = std::mem::replace(&mut *root, PropValue::Null);
                        *root = if map {
                            PropValue::Map(BTreeMap::from([("x".into(), child)]))
                        } else {
                            PropValue::Array(vec![child])
                        };
                    }
                    let inspection = inspect(&root).unwrap();
                    assert_eq!(inspection.max_containers, depth + 1);
                    assert_eq!(validate_persisted(inspection, 1).is_ok(), depth == 1021);
                    let properties = Owned::new(BTreeMap::from([("x".into(), root.into_inner())]));
                    let mut wire = Vec::new();
                    write_properties(&properties, &mut wire).unwrap();
                    let scanned = messagepack::inspect_properties(&wire);
                    assert_eq!(scanned.is_ok(), depth == 1021);
                    if let Ok(scanned) = scanned {
                        assert_eq!(scanned.max_containers, depth + 2);
                    }
                    let mut decoded = CloneState::default();
                    assert_eq!(decoded.decode_properties(&wire).is_ok(), depth == 1021);
                    // Replace only the leaf's sequence encoding with historical
                    // bin encoding. It preserves payload and consumes no debit.
                    wire.truncate(wire.len() - 1 - leaf.len());
                    wire.extend([0xc4, leaf.len() as u8]);
                    wire.extend(&leaf);
                    assert!(messagepack::inspect_properties(&wire).is_ok());
                    let mut binary = CloneState::default();
                    binary.decode_properties(&wire).unwrap();
                    let expected = Owned::new(PropValue::Map(properties.into_inner()));
                    assert!(structural_eq(&binary.root, &expected).unwrap());
                }
            }
        }
        domain.close();
    }
}

#[cfg(test)]
mod ordinary_unwind_tests {
    use super::*;
    #[test]
    fn p45a3_local_partial_payload_unwind_is_stack_safe_on_two_mib_owner_stack() {
        std::thread::Builder::new()
            .name("overgraph-local-unwind".into())
            .stack_size(2 * 1024 * 1024)
            .spawn(|| {
                let result = std::panic::catch_unwind(|| {
                    let mut partial = CloneState::default();
                    for _ in 0..65_536 {
                        let value = std::mem::replace(&mut *partial.root, PropValue::Null);
                        *partial.root = PropValue::Array(vec![value]);
                    }
                    panic!("fixture partial local unwind");
                });
                assert!(result.is_err());
            })
            .unwrap()
            .join()
            .unwrap();
    }
}

// Real-engine inputs remain local, including refusal before queue admission.
impl Drain for crate::types::UpsertNodeOptions {
    fn drain_payload(&mut self) {
        self.props.drain_payload();
    }
}
impl Drain for crate::types::UpsertEdgeOptions {
    fn drain_payload(&mut self) {
        self.props.drain_payload();
    }
}
impl Drain for crate::types::NodeInput {
    fn drain_payload(&mut self) {
        self.props.drain_payload();
    }
}
impl Drain for crate::types::EdgeInput {
    fn drain_payload(&mut self) {
        self.props.drain_payload();
    }
}

impl Drain for crate::types::GraphPatch {
    fn drain_payload(&mut self) {
        self.upsert_nodes.drain_payload();
        self.upsert_edges.drain_payload();
    }
}
impl Drain for crate::types::TxnIntent {
    fn drain_payload(&mut self) {
        match self {
            Self::UpsertNode { options, .. } => options.drain_payload(),
            Self::UpsertEdge { options, .. } => options.drain_payload(),
            _ => {}
        }
    }
}
impl Drain for crate::types::TxnNodeView {
    fn drain_payload(&mut self) {
        self.props.drain_payload();
    }
}
impl Drain for crate::types::TxnEdgeView {
    fn drain_payload(&mut self) {
        self.props.drain_payload();
    }
}
