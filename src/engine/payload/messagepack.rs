//! The pinned rmp-serde typed PropValue grammar, with borrowed preflight and
//! registered iterative construction. Enum wrappers do not debit typed depth.
use super::{checked_add, CloneState, Construction, Frames, Owned, PayloadError};
use crate::types::PropValue;
use rmp_serde::decode::Error as DecodeError;
use serde::de::{self, DeserializeSeed, EnumAccess, MapAccess, SeqAccess, VariantAccess, Visitor};
use serde::Deserializer;

#[derive(Clone, Copy)]
struct Remaining {
    entries: u32,
    map: bool,
}
const _: () = assert!(!std::mem::needs_drop::<Remaining>());

enum Atom<'a> {
    Null,
    Bool(bool),
    Int(i64),
    UInt(u64),
    Float(f64),
    String(&'a str),
    Bytes {
        encoded: &'a [u8],
        length: usize,
        bin: bool,
    },
    Array {
        entries: u32,
        debits: bool,
    },
    Map(u32),
}
pub(crate) struct Parser<'a> {
    bytes: &'a [u8],
    position: usize,
}
impl<'a> Parser<'a> {
    fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, position: 0 }
    }
    fn take(&mut self, length: usize) -> Result<&'a [u8], PayloadError> {
        let end = self
            .position
            .checked_add(length)
            .ok_or(PayloadError::Wire)?;
        let bytes = self
            .bytes
            .get(self.position..end)
            .ok_or(PayloadError::Wire)?;
        self.position = end;
        Ok(bytes)
    }
    fn marker(&mut self) -> Result<u8, PayloadError> {
        Ok(self.take(1)?[0])
    }
    fn u16(&mut self) -> Result<u16, PayloadError> {
        Ok(u16::from_be_bytes(self.take(2)?.try_into().unwrap()))
    }
    fn u32(&mut self) -> Result<u32, PayloadError> {
        Ok(u32::from_be_bytes(self.take(4)?.try_into().unwrap()))
    }
    fn u64(&mut self) -> Result<u64, PayloadError> {
        Ok(u64::from_be_bytes(self.take(8)?.try_into().unwrap()))
    }
    fn uint_marker(&mut self, marker: u8) -> Result<u64, PayloadError> {
        match marker {
            0x00..=0x7f => Ok(u64::from(marker)),
            0xcc => Ok(u64::from(self.marker()?)),
            0xcd => Ok(u64::from(self.u16()?)),
            0xce => Ok(u64::from(self.u32()?)),
            0xcf => self.u64(),
            _ => Err(PayloadError::Wire),
        }
    }
    fn integer_marker(&mut self, marker: u8) -> Result<Result<u64, i64>, PayloadError> {
        match marker {
            0xe0..=0xff => Ok(Err(i64::from(marker as i8))),
            0xd0 => Ok(Err(i64::from(self.marker()? as i8))),
            0xd1 => Ok(Err(i64::from(self.u16()? as i16))),
            0xd2 => Ok(Err(i64::from(self.u32()? as i32))),
            0xd3 => Ok(Err(self.u64()? as i64)),
            _ => self.uint_marker(marker).map(Ok),
        }
    }
    fn byte(&mut self) -> Result<u8, PayloadError> {
        let marker = self.marker()?;
        match self.integer_marker(marker)? {
            Ok(v) => u8::try_from(v).map_err(|_| PayloadError::Wire),
            Err(v) => u8::try_from(v).map_err(|_| PayloadError::Wire),
        }
    }
    fn string_marker(&mut self, marker: u8) -> Result<&'a str, PayloadError> {
        let length = match marker {
            0xa0..=0xbf => u32::from(marker & 31),
            0xd9 | 0xc4 => u32::from(self.marker()?),
            0xda | 0xc5 => u32::from(self.u16()?),
            0xdb | 0xc6 => self.u32()?,
            _ => return Err(PayloadError::Wire),
        };
        std::str::from_utf8(self.take(length as usize)?).map_err(|_| PayloadError::Wire)
    }
    fn key(&mut self) -> Result<&'a str, PayloadError> {
        let marker = self.marker()?;
        self.string_marker(marker)
    }
    fn map_marker(&mut self, marker: u8) -> Result<u32, PayloadError> {
        match marker {
            0x80..=0x8f => Ok(u32::from(marker & 15)),
            0xde => self.u16().map(u32::from),
            0xdf => self.u32(),
            _ => Err(PayloadError::Wire),
        }
    }
    fn array_marker(&mut self, marker: u8) -> Result<u32, PayloadError> {
        match marker {
            0x90..=0x9f => Ok(u32::from(marker & 15)),
            0xdc => self.u16().map(u32::from),
            0xdd => self.u32(),
            _ => Err(PayloadError::Wire),
        }
    }
    fn bin_marker(&mut self, marker: u8) -> Result<usize, PayloadError> {
        match marker {
            0xc4 => self.marker().map(usize::from),
            0xc5 => self.u16().map(usize::from),
            0xc6 => self.u32().map(|n| n as usize),
            _ => Err(PayloadError::Wire),
        }
    }
    fn tag(&mut self, marker: u8) -> Result<u8, PayloadError> {
        if matches!(marker, 0x00..=0x7f | 0xcc..=0xcf) {
            return u8::try_from(self.uint_marker(marker)?)
                .ok()
                .filter(|v| *v <= 8)
                .ok_or(PayloadError::Wire);
        }
        let tag = self.string_marker(marker)?;
        match tag {
            "Null" => Ok(0),
            "Bool" => Ok(1),
            "Int" => Ok(2),
            "UInt" => Ok(3),
            "Float" => Ok(4),
            "String" => Ok(5),
            "Bytes" => Ok(6),
            "Array" => Ok(7),
            "Map" => Ok(8),
            _ => Err(PayloadError::Wire),
        }
    }
    fn value(&mut self) -> Result<Atom<'a>, PayloadError> {
        let marker = self.marker()?;
        let tagged = matches!(marker, 0x80..=0x8f | 0xde | 0xdf);
        let tag = if tagged {
            if self.map_marker(marker)? != 1 {
                return Err(PayloadError::Wire);
            }
            let marker = self.marker()?;
            self.tag(marker)?
        } else {
            self.tag(marker)?
        };
        if !tagged {
            return if tag == 0 {
                Ok(Atom::Null)
            } else {
                Err(PayloadError::Wire)
            };
        }
        let marker = self.marker()?;
        match tag {
            0 if marker == 0xc0 => Ok(Atom::Null),
            1 => match marker {
                0xc2 => Ok(Atom::Bool(false)),
                0xc3 => Ok(Atom::Bool(true)),
                _ => Err(PayloadError::Wire),
            },
            2 => match self.integer_marker(marker)? {
                Ok(v) => i64::try_from(v)
                    .map(Atom::Int)
                    .map_err(|_| PayloadError::Wire),
                Err(v) => Ok(Atom::Int(v)),
            },
            3 => match self.integer_marker(marker)? {
                Ok(v) => Ok(Atom::UInt(v)),
                Err(v) => u64::try_from(v)
                    .map(Atom::UInt)
                    .map_err(|_| PayloadError::Wire),
            },
            4 => {
                let value = match marker {
                    0xca => {
                        let v = f32::from_bits(self.u32()?);
                        (v as f64).copysign(if v.is_sign_negative() { -1.0 } else { 1.0 })
                    }
                    0xcb => f64::from_bits(self.u64()?),
                    _ => match self.integer_marker(marker)? {
                        Ok(v) => v as f64,
                        Err(v) => v as f64,
                    },
                };
                Ok(Atom::Float(value))
            }
            5 => self.string_marker(marker).map(Atom::String),
            6 => {
                if matches!(marker, 0xc4..=0xc6) {
                    let length = self.bin_marker(marker)?;
                    Ok(Atom::Bytes {
                        encoded: self.take(length)?,
                        length,
                        bin: true,
                    })
                } else {
                    let length = self.array_marker(marker)? as usize;
                    if length > self.bytes.len() - self.position {
                        return Err(PayloadError::Wire);
                    }
                    let start = self.position;
                    for _ in 0..length {
                        self.byte()?;
                    }
                    Ok(Atom::Bytes {
                        encoded: &self.bytes[start..self.position],
                        length,
                        bin: false,
                    })
                }
            }
            7 => {
                if matches!(marker, 0xc4..=0xc6) {
                    if self.bin_marker(marker)? != 0 {
                        return Err(PayloadError::Wire);
                    }
                    Ok(Atom::Array {
                        entries: 0,
                        debits: false,
                    })
                } else {
                    self.array_marker(marker).map(|entries| Atom::Array {
                        entries,
                        debits: true,
                    })
                }
            }
            8 => self.map_marker(marker).map(Atom::Map),
            _ => Err(PayloadError::Wire),
        }
    }
}

#[derive(Default)]
pub(super) struct SkipWorkspace {
    inline: [u64; 8],
    len: usize,
    heap: Vec<u64>,
    pub(super) allocation_failure: Option<usize>,
}
impl SkipWorkspace {
    fn clear(&mut self) {
        self.len = 0;
        self.heap.clear();
    }
    fn push(&mut self, remaining: u64) -> Result<(), DecodeError> {
        if self.heap.capacity() == 0 && self.len < self.inline.len() {
            self.inline[self.len] = remaining;
        } else {
            if self.heap.capacity() == 0 || self.heap.len() == self.heap.capacity() {
                let capacity = self
                    .heap
                    .capacity()
                    .max(8)
                    .checked_mul(2)
                    .ok_or_else(|| sidecar_error("MessagePack workspace capacity overflow"))?;
                let mut next = Vec::new();
                if next.try_reserve_exact(capacity).is_err() {
                    self.allocation_failure = Some(capacity * std::mem::size_of::<u64>());
                    return Err(sidecar_error(
                        "unable to grow admitted MessagePack workspace",
                    ));
                }
                assert_eq!(next.capacity(), capacity, "exact sidecar workspace request");
                if self.heap.capacity() == 0 {
                    next.extend_from_slice(&self.inline[..self.len]);
                } else {
                    next.append(&mut self.heap);
                }
                self.heap = next; // old allocation freed after ordinary checked growth
            }
            self.heap.push(remaining);
        }
        self.len += 1;
        Ok(())
    }
    fn last_mut(&mut self) -> Option<&mut u64> {
        if self.heap.capacity() == 0 {
            self.len.checked_sub(1).map(|index| &mut self.inline[index])
        } else {
            self.heap.last_mut()
        }
    }
    fn pop(&mut self) {
        self.len -= 1;
        if self.heap.capacity() != 0 {
            self.heap.pop();
        }
    }
}

fn sidecar_error(message: &'static str) -> DecodeError {
    // Closed descriptions only: never interpolate input, tags, keys or bytes.
    DecodeError::Syntax(message.to_owned())
}
fn sidecar_eof(marker: bool) -> DecodeError {
    let error = std::io::ErrorKind::UnexpectedEof.into();
    if marker {
        DecodeError::InvalidMarkerRead(error)
    } else {
        DecodeError::InvalidDataRead(error)
    }
}

/// Private borrowed cursor bridge for the two fixed sidecar DTO inventories.
/// Existing registered Build/Shape visitors construct every field. Their known
/// topology is finite and contains no recursive PropValue; arbitrary unknown
/// fields use the iterative skip below. This is not a general Deserialize entry
/// point, a JSON backend, a validation pass or a second owned representation.
pub(super) struct SidecarDecoder<'de, 's> {
    parser: Parser<'de>,
    depth: usize,
    workspace: &'s mut SkipWorkspace,
}
impl<'de, 's> SidecarDecoder<'de, 's> {
    pub(super) fn new(bytes: &'de [u8], workspace: &'s mut SkipWorkspace) -> Self {
        Self {
            parser: Parser::new(bytes),
            depth: 0,
            workspace,
        }
    }
    fn marker(&mut self) -> Result<u8, DecodeError> {
        self.parser.marker().map_err(|_| sidecar_eof(true))
    }
    fn take(&mut self, bytes: usize) -> Result<&'de [u8], DecodeError> {
        self.parser.take(bytes).map_err(|_| sidecar_eof(false))
    }
    fn u16(&mut self) -> Result<u16, DecodeError> {
        self.parser.u16().map_err(|_| sidecar_eof(false))
    }
    fn u32(&mut self) -> Result<u32, DecodeError> {
        self.parser.u32().map_err(|_| sidecar_eof(false))
    }
    fn u64(&mut self) -> Result<u64, DecodeError> {
        self.parser.u64().map_err(|_| sidecar_eof(false))
    }
    fn length(&mut self, marker: u8) -> Result<u32, DecodeError> {
        match marker {
            0x80..=0x9f => Ok(u32::from(marker & 15)),
            0xa0..=0xbf => Ok(u32::from(marker & 31)),
            0xc4 | 0xc7 | 0xd9 => Ok(u32::from(self.take(1)?[0])),
            0xc5 | 0xc8 | 0xda | 0xdc | 0xde => self.u16().map(u32::from),
            0xc6 | 0xc9 | 0xdb | 0xdd | 0xdf => self.u32(),
            0xd4 => Ok(1),
            0xd5 => Ok(2),
            0xd6 => Ok(4),
            0xd7 => Ok(8),
            0xd8 => Ok(16),
            _ => Err(sidecar_error("unexpected guarded MessagePack marker")),
        }
    }
    fn enter(&mut self) -> Result<(), DecodeError> {
        if self.depth + 1 >= 1024 {
            return Err(DecodeError::DepthLimitExceeded);
        }
        self.depth += 1;
        Ok(())
    }
    fn any<V: Visitor<'de>>(
        &mut self,
        visitor: V,
        allow_bytes: bool,
    ) -> Result<V::Value, DecodeError> {
        let marker = self.marker()?;
        match marker {
            0x00..=0x7f => visitor.visit_u8(marker),
            0xe0..=0xff => visitor.visit_i8(marker as i8),
            0xc0 => visitor.visit_unit(),
            0xc2 | 0xc3 => visitor.visit_bool(marker == 0xc3),
            0xcc => visitor.visit_u8(self.take(1)?[0]),
            0xcd => visitor.visit_u16(self.u16()?),
            0xce => visitor.visit_u32(self.u32()?),
            0xcf => visitor.visit_u64(self.u64()?),
            0xd0 => visitor.visit_i8(self.take(1)?[0] as i8),
            0xd1 => visitor.visit_i16(self.u16()? as i16),
            0xd2 => visitor.visit_i32(self.u32()? as i32),
            0xd3 => visitor.visit_i64(self.u64()? as i64),
            0xca => visitor.visit_f32(f32::from_bits(self.u32()?)),
            0xcb => visitor.visit_f64(f64::from_bits(self.u64()?)),
            0xa0..=0xbf | 0xd9..=0xdb => {
                let length = self.length(marker)?;
                let bytes = self.take(length as usize)?;
                match std::str::from_utf8(bytes) {
                    Ok(text) => visitor.visit_borrowed_str(text),
                    Err(error) => match visitor.visit_borrowed_bytes::<DecodeError>(bytes) {
                        Ok(value) => Ok(value),
                        Err(_) => Err(DecodeError::Utf8Error(error)),
                    },
                }
            }
            0x90..=0x9f | 0xdc | 0xdd => {
                let length = self.length(marker)?;
                self.enter()?;
                let mut access = SidecarSequence {
                    decoder: self,
                    left: length,
                };
                let result = visitor.visit_seq(&mut access);
                let left = access.left;
                self.depth -= 1;
                match result {
                    Ok(value) if left == 0 => Ok(value),
                    Ok(_) => Err(DecodeError::LengthMismatch(length - left)),
                    Err(error) => Err(error),
                }
            }
            0x80..=0x8f | 0xde | 0xdf => {
                let length = self.length(marker)?;
                self.enter()?;
                let mut access = SidecarMapping {
                    decoder: self,
                    left: length,
                };
                let result = visitor.visit_map(&mut access);
                let left = access.left;
                self.depth -= 1;
                match result {
                    Ok(value) if left == 0 => Ok(value),
                    Ok(_) => Err(DecodeError::LengthMismatch(length - left)),
                    Err(error) => Err(error),
                }
            }
            0xc4..=0xc6 => {
                let length = self.length(marker)?;
                let bytes = self.take(length as usize)?;
                if allow_bytes {
                    visitor.visit_borrowed_bytes(bytes)
                } else {
                    visitor.visit_seq(de::value::SeqDeserializer::new(bytes.iter().copied()))
                }
            }
            0xc7..=0xc9 | 0xd4..=0xd8 => {
                let _length = self.length(marker)?;
                self.enter()?;
                self.depth -= 1;
                // No visitor in the closed known DTO inventory accepts a
                // newtype-struct event. Ignored extensions are consumed below.
                Err(sidecar_error(
                    "unexpected extension for guarded MessagePack shape",
                ))
            }
            _ => Err(sidecar_error("unexpected guarded MessagePack marker")),
        }
    }
    fn skip(&mut self) -> Result<(), DecodeError> {
        self.workspace.clear();
        loop {
            let marker = self.marker()?;
            match marker {
                0x00..=0x7f | 0xe0..=0xff | 0xc0 | 0xc2 | 0xc3 => {}
                0xcc | 0xd0 => {
                    self.take(1)?;
                }
                0xcd | 0xd1 => {
                    self.take(2)?;
                }
                0xce | 0xd2 | 0xca => {
                    self.take(4)?;
                }
                0xcf | 0xd3 | 0xcb => {
                    self.take(8)?;
                }
                0xa0..=0xbf | 0xc4..=0xc6 | 0xd9..=0xdb => {
                    let length = self.length(marker)?;
                    // IgnoredAny accepts invalid UTF-8 via borrowed bytes.
                    self.take(length as usize)?;
                }
                0x80..=0x9f | 0xdc..=0xdf => {
                    let length = self.length(marker)?;
                    if self.depth + self.workspace.len + 1 >= 1024 {
                        return Err(DecodeError::DepthLimitExceeded);
                    }
                    let map = matches!(marker, 0x80..=0x8f | 0xde | 0xdf);
                    self.workspace
                        .push(u64::from(length) * if map { 2 } else { 1 })?;
                }
                0xc7..=0xc9 | 0xd4..=0xd8 => {
                    let length = self.length(marker)?;
                    if self.depth + self.workspace.len + 1 >= 1024 {
                        return Err(DecodeError::DepthLimitExceeded);
                    }
                    // Extension has one signed type byte plus its opaque data,
                    // and debits one level without a synthetic sequence debit.
                    self.take(1)?;
                    self.take(length as usize)?;
                }
                _ => return Err(sidecar_error("unexpected guarded MessagePack marker")),
            }
            loop {
                match self.workspace.last_mut() {
                    Some(0) => self.workspace.pop(),
                    Some(left) => {
                        *left -= 1;
                        break;
                    }
                    None => return Ok(()),
                }
            }
        }
    }
}

impl<'de> Deserializer<'de> for &mut SidecarDecoder<'de, '_> {
    type Error = DecodeError;
    fn is_human_readable(&self) -> bool {
        false
    }
    fn deserialize_any<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, DecodeError> {
        self.any(visitor, true)
    }
    fn deserialize_seq<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, DecodeError> {
        self.any(visitor, false)
    }
    fn deserialize_tuple<V: Visitor<'de>>(
        self,
        _: usize,
        visitor: V,
    ) -> Result<V::Value, DecodeError> {
        self.any(visitor, false)
    }
    fn deserialize_struct<V: Visitor<'de>>(
        self,
        _: &'static str,
        _: &'static [&'static str],
        visitor: V,
    ) -> Result<V::Value, DecodeError> {
        self.any(visitor, false)
    }
    fn deserialize_tuple_struct<V: Visitor<'de>>(
        self,
        _: &'static str,
        _: usize,
        visitor: V,
    ) -> Result<V::Value, DecodeError> {
        self.any(visitor, false)
    }
    fn deserialize_option<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, DecodeError> {
        if self.parser.bytes.get(self.parser.position) == Some(&0xc0) {
            self.parser.position += 1;
            visitor.visit_none()
        } else {
            visitor.visit_some(self)
        }
    }
    fn deserialize_enum<V: Visitor<'de>>(
        self,
        _: &'static str,
        _: &'static [&'static str],
        visitor: V,
    ) -> Result<V::Value, DecodeError> {
        let position = self.parser.position;
        let marker = self.marker()?;
        let payload = matches!(marker, 0x80..=0x8f | 0xde | 0xdf);
        if payload {
            let length = self.length(marker)?;
            if length != 1 {
                return Err(DecodeError::LengthMismatch(length));
            }
        } else {
            self.parser.position = position;
        }
        // Typed enum wrapper maps deliberately do not debit depth.
        visitor.visit_enum(SidecarVariant {
            decoder: self,
            payload,
        })
    }
    fn deserialize_ignored_any<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, DecodeError> {
        self.skip()?;
        visitor.visit_unit()
    }
    serde::forward_to_deserialize_any! {
        bool i8 i16 i32 i64 i128 u8 u16 u32 u64 u128 f32 f64 char str string
        bytes byte_buf unit unit_struct newtype_struct map identifier
    }
}

struct SidecarSequence<'a, 'de, 's> {
    decoder: &'a mut SidecarDecoder<'de, 's>,
    left: u32,
}
impl<'de> SeqAccess<'de> for &mut SidecarSequence<'_, 'de, '_> {
    type Error = DecodeError;
    fn next_element_seed<T: DeserializeSeed<'de>>(
        &mut self,
        seed: T,
    ) -> Result<Option<T::Value>, DecodeError> {
        if self.left == 0 {
            return Ok(None);
        }
        self.left -= 1;
        seed.deserialize(&mut *self.decoder).map(Some)
    }
    fn size_hint(&self) -> Option<usize> {
        Some(self.left as usize)
    }
}
struct SidecarMapping<'a, 'de, 's> {
    decoder: &'a mut SidecarDecoder<'de, 's>,
    left: u32,
}
impl<'de> MapAccess<'de> for &mut SidecarMapping<'_, 'de, '_> {
    type Error = DecodeError;
    fn next_key_seed<T: DeserializeSeed<'de>>(
        &mut self,
        seed: T,
    ) -> Result<Option<T::Value>, DecodeError> {
        if self.left == 0 {
            return Ok(None);
        }
        self.left -= 1;
        seed.deserialize(&mut *self.decoder).map(Some)
    }
    fn next_value_seed<T: DeserializeSeed<'de>>(
        &mut self,
        seed: T,
    ) -> Result<T::Value, DecodeError> {
        seed.deserialize(&mut *self.decoder)
    }
    fn size_hint(&self) -> Option<usize> {
        Some(self.left as usize)
    }
}
struct SidecarVariant<'a, 'de, 's> {
    decoder: &'a mut SidecarDecoder<'de, 's>,
    payload: bool,
}
impl<'de> EnumAccess<'de> for SidecarVariant<'_, 'de, '_> {
    type Error = DecodeError;
    type Variant = Self;
    fn variant_seed<T: DeserializeSeed<'de>>(
        self,
        seed: T,
    ) -> Result<(T::Value, Self), DecodeError> {
        let tag = seed.deserialize(&mut *self.decoder)?;
        Ok((tag, self))
    }
}
impl<'de> VariantAccess<'de> for SidecarVariant<'_, 'de, '_> {
    type Error = DecodeError;
    fn unit_variant(self) -> Result<(), DecodeError> {
        if !self.payload || self.decoder.marker()? == 0xc0 {
            Ok(())
        } else {
            Err(sidecar_error("expected null for guarded unit variant"))
        }
    }
    fn newtype_variant_seed<T: DeserializeSeed<'de>>(
        self,
        seed: T,
    ) -> Result<T::Value, DecodeError> {
        if self.payload {
            seed.deserialize(self.decoder)
        } else {
            Err(sidecar_error("expected a guarded variant payload"))
        }
    }
    fn tuple_variant<V: Visitor<'de>>(
        self,
        length: usize,
        visitor: V,
    ) -> Result<V::Value, DecodeError> {
        if self.payload {
            self.decoder.deserialize_tuple(length, visitor)
        } else {
            Err(sidecar_error("expected a guarded variant payload"))
        }
    }
    fn struct_variant<V: Visitor<'de>>(
        self,
        fields: &'static [&'static str],
        visitor: V,
    ) -> Result<V::Value, DecodeError> {
        if self.payload {
            self.decoder.deserialize_tuple(fields.len(), visitor)
        } else {
            Err(sidecar_error("expected a guarded variant payload"))
        }
    }
}

#[derive(Debug, Default)]
pub(crate) struct WireInspection {
    pub(crate) consumed: usize,
    pub(crate) values: usize,
    pub(crate) map_entries: usize,
    pub(crate) max_containers: usize,
    /// Bytes of keys in the outer property map, excluding nested map keys and
    /// values. Counted during the existing borrowed walk without a second parse.
    pub(crate) property_key_bytes: usize,
}

/// Bounded hints are checked against wire extent before capacity arithmetic.
/// This does no owned decode. It retains rmp-serde's first-value/trailing-byte
/// behavior and the typed 1024 budget, separate from generic IgnoredAny scanning.
pub(crate) fn inspect_wire(
    bytes: &[u8],
    outer_containers: usize,
) -> Result<WireInspection, PayloadError> {
    inspect_wire_kind(bytes, outer_containers, false)
}

pub(crate) fn inspect_properties(bytes: &[u8]) -> Result<WireInspection, PayloadError> {
    inspect_wire_kind(bytes, 0, true)
}

fn inspect_wire_kind(
    bytes: &[u8],
    outer_containers: usize,
    properties: bool,
) -> Result<WireInspection, PayloadError> {
    let mut parser = Parser::new(bytes);
    let mut frames: Frames<Remaining> = Frames::new();
    let mut out = WireInspection::default();
    let mut root = true;
    loop {
        if !root {
            let Some(frame) = frames.last_mut() else {
                break;
            };
            if frame.entries == 0 {
                frames.pop();
                continue;
            }
            frame.entries -= 1;
            if frame.map {
                let key = parser.key()?;
                if properties && frames.len == 1 {
                    checked_add(&mut out.property_key_bytes, key.len())?;
                }
            }
        }
        let atom = if root && properties {
            let marker = parser.marker()?;
            Atom::Map(parser.map_marker(marker)?)
        } else {
            parser.value()?
        };
        root = false;
        checked_add(&mut out.values, 1)?;
        match atom {
            Atom::String(_) => {}
            Atom::Bytes { length: _, bin, .. } => {
                // Sequence-coded Vec<u8> consumes typed depth; the pinned
                // decoder's binary fast path consumes none. Bytes are parsed
                // as one atom, so their sequence never enters `frames`.
                if !bin {
                    let depth = frames
                        .len
                        .checked_add(outer_containers)
                        .and_then(|n| n.checked_add(1))
                        .ok_or(PayloadError::Wire)?;
                    if depth >= 1024 {
                        return Err(PayloadError::Wire);
                    }
                    out.max_containers = out.max_containers.max(depth - outer_containers);
                }
            }
            Atom::Array { entries, debits } => inspect_container(
                &mut out,
                &mut frames,
                entries,
                false,
                debits,
                parser.bytes.len() - parser.position,
                outer_containers,
            )?,
            Atom::Map(entries) => inspect_container(
                &mut out,
                &mut frames,
                entries,
                true,
                true,
                parser.bytes.len() - parser.position,
                outer_containers,
            )?,
            _ => {}
        }
    }
    out.consumed = parser.position;
    Ok(out)
}

fn inspect_container(
    out: &mut WireInspection,
    frames: &mut Frames<Remaining>,
    entries: u32,
    map: bool,
    debits: bool,
    remaining_wire: usize,
    outer: usize,
) -> Result<(), PayloadError> {
    let depth = frames
        .len
        .checked_add(outer)
        .and_then(|n| n.checked_add(1))
        .ok_or(PayloadError::Wire)?;
    if debits && depth >= 1024 {
        return Err(PayloadError::Wire);
    }
    let minimum = if map { 2 } else { 1 };
    if entries as usize > remaining_wire / minimum {
        return Err(PayloadError::Wire);
    }
    out.max_containers = out.max_containers.max(depth - outer);
    if map {
        checked_add(&mut out.map_entries, entries as usize)?;
    }
    frames.push(Remaining { entries, map })
}

impl CloneState {
    /// All constructor fields/partials and duplicate replacements are registered
    /// before this is called. Its wire preflight precedes any payload growth.
    pub(crate) fn decode_value(
        &mut self,
        bytes: &[u8],
        outer_containers: usize,
    ) -> Result<usize, PayloadError> {
        self.decode_kind(bytes, outer_containers, false)
    }

    pub(crate) fn decode_properties(&mut self, bytes: &[u8]) -> Result<usize, PayloadError> {
        self.decode_kind(bytes, 0, true)
    }

    fn decode_kind(
        &mut self,
        bytes: &[u8],
        outer_containers: usize,
        properties: bool,
    ) -> Result<usize, PayloadError> {
        assert_eq!(self.len, 0);
        assert!(matches!(*self.root, PropValue::Null));
        let inspection = inspect_wire_kind(bytes, outer_containers, properties)?;
        self.displaced
            .try_reserve_exact(inspection.map_entries)
            .map_err(|_| PayloadError::Wire)?;
        let mut parser = Parser::new(bytes);
        let mut frames: Frames<Remaining> = Frames::new();
        let mut root = true;
        loop {
            if !root {
                let Some(frame) = frames.last_mut() else {
                    break;
                };
                if frame.entries == 0 {
                    frames.pop();
                    *self.root = match self.pop().expect("decoder construction") {
                        Construction::Array(values) => PropValue::Array(values.into_inner()),
                        Construction::Map { values, key } => {
                            assert!(key.is_empty());
                            PropValue::Map(values.into_inner())
                        }
                    };
                    self.attach_decoded();
                    continue;
                }
                frame.entries -= 1;
                if frame.map {
                    let key = parser.key()?;

                    if let Some(Construction::Map { key: output, .. }) = self.last_mut() {
                        output
                            .try_reserve_exact(key.len())
                            .map_err(|_| PayloadError::Wire)?;
                        output.push_str(key);
                    } else {
                        unreachable!()
                    }
                }
            }
            let atom = if root && properties {
                let marker = parser.marker()?;
                Atom::Map(parser.map_marker(marker)?)
            } else {
                parser.value()?
            };
            root = false;
            match atom {
                Atom::Array { entries, .. } => {
                    self.push(Construction::Array(Owned::default()))?;
                    if let Some(Construction::Array(values)) = self.last_mut() {
                        values
                            .try_reserve_exact(entries as usize)
                            .map_err(|_| PayloadError::Wire)?;
                    }
                    frames.push(Remaining {
                        entries,
                        map: false,
                    })?;
                }
                Atom::Map(entries) => {
                    self.push(Construction::Map {
                        values: Owned::default(),
                        key: String::new(),
                    })?;
                    frames.push(Remaining { entries, map: true })?;
                }
                atom => {
                    match atom {
                        Atom::Null => *self.root = PropValue::Null,
                        Atom::Bool(v) => *self.root = PropValue::Bool(v),
                        Atom::Int(v) => *self.root = PropValue::Int(v),
                        Atom::UInt(v) => *self.root = PropValue::UInt(v),
                        Atom::Float(v) => *self.root = PropValue::Float(v),
                        Atom::String(v) => {
                            *self.root = PropValue::String(String::new());
                            if let PropValue::String(output) = &mut *self.root {
                                output
                                    .try_reserve_exact(v.len())
                                    .map_err(|_| PayloadError::Wire)?;
                                output.push_str(v);
                            }
                        }
                        Atom::Bytes {
                            encoded,
                            length,
                            bin,
                        } => {
                            *self.root = PropValue::Bytes(Vec::new());
                            if let PropValue::Bytes(output) = &mut *self.root {
                                output
                                    .try_reserve_exact(length)
                                    .map_err(|_| PayloadError::Wire)?;
                                if bin {
                                    output.extend_from_slice(encoded);
                                } else {
                                    let mut bytes = Parser::new(encoded);
                                    for _ in 0..length {
                                        output.push(bytes.byte()?);
                                    }
                                }
                            }
                        }
                        _ => unreachable!(),
                    }
                    self.attach_decoded();
                }
            }
        }
        assert_eq!(parser.position, inspection.consumed);
        Ok(parser.position)
    }
    fn attach_decoded(&mut self) {
        if self.len == 0 {
            return;
        }
        let child = std::mem::replace(&mut *self.root, PropValue::Null);
        let displaced = match self.last_mut().expect("decoded parent") {
            Construction::Array(values) => {
                values.push(child);
                None
            }
            Construction::Map { values, key } => values.insert(std::mem::take(key), child),
        };
        if let Some(displaced) = displaced {
            // Capacity for every wire map entry was reserved before decoding.
            // Thus registering the replacement cannot itself allocate/fail.
            assert!(self.displaced.len() < self.displaced.capacity());
            self.displaced.push(displaced);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::super::write_value;
    use super::*;

    fn differential(bytes: &[u8]) {
        let expected = rmp_serde::from_slice::<PropValue>(bytes).map(Owned::new);
        let mut output = CloneState::default();
        let actual = output.decode_value(bytes, 0);
        assert_eq!(
            actual.is_ok(),
            expected.is_ok(),
            "wire={bytes:02x?}, old={:?}, new={actual:?}",
            expected.as_ref().err()
        );
        if let Ok(expected) = expected {
            let mut old = Vec::new();
            let mut new = Vec::new();
            write_value(&expected, &mut old).unwrap();
            write_value(&output.root, &mut new).unwrap();
            assert_eq!(new, old, "wire={bytes:02x?}");
        }
    }
    fn tagged(tag: &[u8], payload: &[u8]) -> Vec<u8> {
        let mut out = vec![0x81];
        out.extend_from_slice(tag);
        out.extend_from_slice(payload);
        out
    }

    #[test]
    fn p45a2_payload_msgpack_pinned_grammar_differential() {
        let names = [
            "Null", "Bool", "Int", "UInt", "Float", "String", "Bytes", "Array", "Map",
        ];
        let payloads: &[&[u8]] = &[
            &[],
            &[0xc0],
            &[0xc2],
            &[0xc3],
            &[0],
            &[0xff],
            &[0xcc, 255],
            &[0xd0, 0],
            &[0xd0, 255],
            &[0xd1, 0, 1],
            &[0xd2, 0, 0, 0, 1],
            &[0xd3, 0, 0, 0, 0, 0, 0, 0, 1],
            &[0xcf, 255, 255, 255, 255, 255, 255, 255, 255],
            &[0xca, 0xff, 0xc0, 0, 1],
            &[0xcb, 0x7f, 0xf8, 0, 0, 0, 0, 0, 0x42],
            &[0xa0],
            &[0xa1, b'x'],
            &[0xa1, 255],
            &[0xd9, 1, b'x'],
            &[0xda, 0, 1, b'x'],
            &[0xdb, 0, 0, 0, 1, b'x'],
            &[0xc4, 0],
            &[0xc4, 1, 0],
            &[0xc4, 1, b'x'],
            &[0xc5, 0, 1, b'x'],
            &[0xc6, 0, 0, 0, 1, b'x'],
            &[0x90],
            &[0xdc, 0, 0],
            &[0xdd, 0, 0, 0, 0],
            &[0x91, 0],
            &[0x92, 0, 1],
            &[0x92, 0xd0, 0, 0xcc, 255],
            &[0x80],
            &[0xde, 0, 0],
            &[0xdf, 0, 0, 0, 0],
            &[0x81, 0xa1, b'x', 0],
            &[0xc1],
            &[0xd4, 0, 0],
            &[0xd9, 1],
            &[0xc4, 4, 0],
        ];
        for (id, name) in names.iter().enumerate() {
            let mut tags = vec![
                rmp_serde::to_vec(name).unwrap(),
                vec![id as u8],
                vec![0xcc, id as u8],
                vec![0xcd, 0, id as u8],
                vec![0xd0, id as u8],
            ];
            let mut bin_tag = vec![0xc4, name.len() as u8];
            bin_tag.extend_from_slice(name.as_bytes());
            tags.push(bin_tag);
            for tag in &tags {
                differential(tag);
                for payload in payloads {
                    let bytes = tagged(tag, payload);
                    differential(&bytes);
                    let mut trailing = bytes.clone();
                    trailing.extend_from_slice(&[0xc1, 0xff]);
                    differential(&trailing);
                    for end in 0..bytes.len() {
                        differential(&bytes[..end]);
                    }
                }
            }
        }
        for marker in 0..=255 {
            differential(&[marker]);
        }
        // Duplicate map keys retain the last decoded value; earlier ownership
        // stays registered rather than being recursively dropped at insertion.
        let map = tagged(
            &[0xa3, b'M', b'a', b'p'],
            &[0x82, 0xa1, b'x', 0, 0xc4, 1, b'x', 0x81, 2, 7],
        );
        differential(&map);
    }

    #[test]
    fn p45a2_payload_msgpack_outer_properties_and_trailing_bytes() {
        let bytes = [0x82, 0xa1, b'x', 0, 0xa1, b'x', 0x81, 2, 7, 0xc1];
        let expected: std::collections::BTreeMap<String, PropValue> =
            rmp_serde::from_slice(&bytes).unwrap();
        let mut state = CloneState::default();
        assert_eq!(state.decode_properties(&bytes).unwrap(), bytes.len() - 1);
        let PropValue::Map(actual) = &*state.root else {
            panic!("property map")
        };
        assert_eq!(*actual, expected);
        assert_eq!(state.displaced.len(), 1);
        for bytes in [
            &[0x91, 0][..],
            &[0xdf, 255, 255, 255, 255][..],
            &[0x81, 0xa1, b'x'][..],
        ] {
            assert!(inspect_properties(bytes).is_err());
        }
    }
    #[test]
    fn p45a2_preflight_property_key_inventory_excludes_nested_keys_and_values() {
        let props = std::collections::BTreeMap::from([
            (
                "outer".to_owned(),
                PropValue::Map(std::collections::BTreeMap::from([(
                    "nested-long-key".to_owned(),
                    PropValue::String("x".repeat(4096)),
                )])),
            ),
            ("q".to_owned(), PropValue::Int(1)),
        ]);
        let bytes = rmp_serde::to_vec(&props).unwrap();
        let inspected = inspect_properties(&bytes).unwrap();
        assert_eq!(inspected.property_key_bytes, 6);
        assert_eq!(inspected.consumed, bytes.len());
    }

    #[test]
    fn p45a2_sidecar_messagepack_ignored_markers_lengths_and_truncation_differential() {
        fn agrees(bytes: &[u8]) {
            let expected = rmp_serde::from_slice::<de::IgnoredAny>(bytes);
            let mut workspace = SkipWorkspace::default();
            let actual = <de::IgnoredAny as de::Deserialize>::deserialize(
                &mut SidecarDecoder::new(bytes, &mut workspace),
            );
            assert_eq!(actual.is_ok(), expected.is_ok(), "{:?}", bytes);
            if let Err(error) = actual {
                assert!(error.to_string().len() <= 1024);
            }
        }
        for marker in 0u8..=255 {
            let mut bytes = vec![marker];
            match marker {
                0x80..=0x8f => {
                    bytes.extend(std::iter::repeat_n(0xc0, usize::from(marker & 15) * 2))
                }
                0x90..=0x9f => bytes.extend(std::iter::repeat_n(0xc0, usize::from(marker & 15))),
                0xa0..=0xbf => bytes.extend(std::iter::repeat_n(0xff, usize::from(marker & 31))),
                0xc4 | 0xd9 => bytes.extend_from_slice(&[3, 0xff, 0xc1, 0]),
                0xc5 | 0xda => bytes.extend_from_slice(&[0, 3, 0xff, 0xc1, 0]),
                0xc6 | 0xdb => bytes.extend_from_slice(&[0, 0, 0, 3, 0xff, 0xc1, 0]),
                0xc7 => bytes.extend_from_slice(&[3, 0xff, 0xc1, 0xc1, 0xc1]),
                0xc8 => bytes.extend_from_slice(&[0, 3, 0xff, 0xc1, 0xc1, 0xc1]),
                0xc9 => bytes.extend_from_slice(&[0, 0, 0, 3, 0xff, 0xc1, 0xc1, 0xc1]),
                0xca | 0xce | 0xd2 => bytes.extend_from_slice(&[255; 4]),
                0xcb | 0xcf | 0xd3 => bytes.extend_from_slice(&[255; 8]),
                0xcc | 0xd0 => bytes.push(255),
                0xcd | 0xd1 => bytes.extend_from_slice(&[255; 2]),
                0xd4..=0xd8 => {
                    bytes.push(255);
                    bytes.extend(std::iter::repeat_n(0xc1, 1 << (marker - 0xd4)));
                }
                0xdc => bytes.extend_from_slice(&[0, 2, 0x80, 0x90]),
                0xdd => bytes.extend_from_slice(&[0, 0, 0, 2, 0x80, 0x90]),
                0xde => bytes.extend_from_slice(&[0, 1, 0x91, 0xc0, 0x80]),
                0xdf => bytes.extend_from_slice(&[0, 0, 0, 1, 0x91, 0xc0, 0x80]),
                _ => {}
            }
            agrees(&bytes);
            for end in 0..bytes.len() {
                agrees(&bytes[..end]);
            }
            bytes.extend_from_slice(&[0xc1, 0]);
            agrees(&bytes);
        }
        // Wide ignored maps can have containers as keys; signed tags, opaque
        // extension payloads and invalid UTF-8 strings all remain ignorable.
        agrees(&[
            0x83, 0x91, 0xc0, 0xd4, 0xff, 0xc1, 0x80, 0xa1, 0xff, 0xc4, 1, 0xff, 0x90,
        ]);
        for bytes in [
            &[0xdd, 255, 255, 255, 255][..],
            &[0xdf, 255, 255, 255, 255][..],
            &[0xc6, 255, 255, 255, 255][..],
            &[0xc9, 255, 255, 255, 255][..],
        ] {
            agrees(bytes);
        }
        let mut bytes = vec![0x91; 20];
        bytes.extend_from_slice(&[0x81, 0xc0, 0xd4, 255, 0xc1]);
        agrees(&bytes);
    }
}
