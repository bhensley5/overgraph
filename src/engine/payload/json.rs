//! Registered construction using the existing serde_json backend. Its owned
//! reader keeps private parse/escape/ignored-field scratch inside the same job.
//! No recursive owned result or displaced map value is a serde visitor local.
//!
//! The private preflight reuses this bounded registered decoder; public engine
//! open routing remains outside this checkpoint. Closed shape visitors omit input contents from errors
//! before the pinned backend allocates diagnostic text.
use super::{Drain, Owned, PayloadError};
use crate::planner_stats::*;
use crate::segment_components::{
    ComponentDependencyWireV1, ComponentHandleWireV1, SegmentComponentManifestWireV1,
    SegmentComponentRecordWireV1,
};
use crate::types::*;
use serde::de::{self, DeserializeSeed, MapAccess, SeqAccess, Visitor};
use serde::{Deserialize, Deserializer};
use std::collections::btree_map::Entry;
use std::collections::BTreeMap;
use std::fmt;
use std::io::Cursor;

type Parser = serde_json::Deserializer<serde_json::de::IoRead<Cursor<Vec<u8>>>>;

// Closed diagnostic inventory: at most 105 body bytes (largest fixed scalar
// Unexpected + Expected), plus a 57-byte position suffix on 64-bit profiles.
// String grow/shrink peak is <=3*body, and position fixup or a secondary end
// error retains at most two 40-byte ErrorImpl shells. Debug also owns
// code.to_string(): static ASCII has <=2 escaped bytes, two quotes, 25 framing
// bytes and two 20-digit usize values. Outer growth coexists with code capacity
// <=2*body and the retained body/shell. Native pinned DWARF proves shell=40;
// foreign layout and full stack certification remain separate routing gates.
const ERROR_BODY_BYTES: usize = 105;
const ERROR_POSITION_BYTES: usize = 57;
const ERROR_SHELL_BYTES: usize = 40;
const ERROR_DEBUG_BYTES: usize = ERROR_BODY_BYTES + 4 + 25 + 40;
const ERROR_PEAK_BYTES: usize =
    3 * ERROR_DEBUG_BYTES + 2 * ERROR_BODY_BYTES + ERROR_BODY_BYTES + 2 * ERROR_SHELL_BYTES;
const _: () = assert!(ERROR_BODY_BYTES + ERROR_POSITION_BYTES <= 1024);
const _: () = assert!(ERROR_DEBUG_BYTES <= 1024);
const _: () = assert!(ERROR_PEAK_BYTES < 64 * 1024);

#[expect(
    clippy::large_enum_variant,
    reason = "Inline displaced DTOs avoid unaccounted allocations in bounded payload destruction"
)]
enum Displaced {
    Value(PropValue),
    Rule(PropertySchemaManifestRule),
    Policy(PrunePolicy),
}
impl Drain for Displaced {
    fn drain_payload(&mut self) {
        match self {
            Self::Value(value) => value.drain_payload(),
            Self::Rule(rule) => rule.drain_payload(),
            Self::Policy(_) => {} // String is a leaf, freed by this job's worker.
        }
    }
}

#[derive(Default)]
struct Context {
    pending_key: String,
    displaced: Owned<Vec<Displaced>>,
    allocation: Allocation,
    messagepack: bool,
}
#[derive(Default)]
struct Allocation {
    refusal: Option<PayloadError>,
}
impl Allocation {
    fn refuse<E: de::Error>(&mut self, requested: usize) -> E {
        self.refusal = Some(PayloadError::Resource {
            resource: "allocation_capacity",
            requested,
            limit: isize::MAX as usize,
            retryable: false,
        });
        E::custom("construction allocation failed")
    }
    fn grow<T, E: de::Error>(&mut self, values: &mut Vec<T>) -> Result<(), E> {
        if values.len() < values.capacity() {
            return Ok(());
        }
        let capacity = values
            .capacity()
            .max(2)
            .checked_mul(2)
            .ok_or_else(|| self.refuse::<E>(usize::MAX))?;
        values
            .try_reserve_exact(capacity - values.len())
            .map_err(|_| self.refuse::<E>(capacity))?;
        Ok(())
    }
}
impl Context {
    fn refuse<E: de::Error>(&mut self, requested: usize) -> E {
        self.allocation.refuse(requested)
    }
    fn grow<T, E: de::Error>(&mut self, values: &mut Vec<T>) -> Result<(), E> {
        self.allocation.grow(values)
    }
    fn prepare_displaced<E: de::Error>(&mut self) -> Result<(), E> {
        let old = self.displaced.capacity();
        if self.displaced.len() < old {
            return Ok(());
        }
        let Some(capacity) = old.max(2).checked_mul(2) else {
            return Err(self.refuse(usize::MAX));
        };
        let Some(bytes) = capacity.checked_mul(std::mem::size_of::<Displaced>()) else {
            return Err(self.refuse(usize::MAX));
        };

        let additional = capacity - self.displaced.len();
        self.displaced
            .try_reserve_exact(additional)
            .map_err(|_| self.refuse(bytes))?;

        Ok(())
    }
}

/// Closed inventory, deliberately no blanket Deserialize fallback for owned T.
trait Build: Sized {
    fn read_into<'de, D: Deserializer<'de>>(
        &mut self,
        context: &mut Context,
        decoder: D,
    ) -> Result<(), D::Error>;
}
trait Shell: Build {
    fn empty() -> Self;
}
struct Seed<'a, T> {
    target: &'a mut T,
    context: &'a mut Context,
}
impl<'de, T: Build> DeserializeSeed<'de> for Seed<'_, T> {
    type Value = ();
    fn deserialize<D: Deserializer<'de>>(self, decoder: D) -> Result<(), D::Error> {
        self.target.read_into(self.context, decoder)
    }
}

// Fixed scalar events may reuse Serde conversion/range rules. Input strings
// never enter those deserializers or the backend's typed mismatch formatter.
macro_rules! leaf {
    ($ty:ty, $empty:expr) => {
        const _: () = assert!(!std::mem::needs_drop::<$ty>());
        impl<'de> Visitor<'de> for Scalar<'_, $ty> {
            type Value = ();
            fn expecting(&self, f: &mut fmt::Formatter) -> fmt::Result {
                f.write_str(stringify!($ty))
            }
            fn visit_bool<E: de::Error>(self, value: bool) -> Result<(), E> {
                *self.target = <$ty>::deserialize(de::value::BoolDeserializer::<E>::new(value))?;
                Ok(())
            }
            fn visit_i64<E: de::Error>(self, value: i64) -> Result<(), E> {
                *self.target = <$ty>::deserialize(de::value::I64Deserializer::<E>::new(value))?;
                Ok(())
            }
            fn visit_u64<E: de::Error>(self, value: u64) -> Result<(), E> {
                *self.target = <$ty>::deserialize(de::value::U64Deserializer::<E>::new(value))?;
                Ok(())
            }
            fn visit_f64<E: de::Error>(self, value: f64) -> Result<(), E> {
                *self.target = <$ty>::deserialize(de::value::F64Deserializer::<E>::new(value))?;
                Ok(())
            }
            reject_text!();
        }
        impl Build for $ty {
            fn read_into<'de, D: Deserializer<'de>>(
                &mut self,
                _: &mut Context,
                decoder: D,
            ) -> Result<(), D::Error> {
                decoder.deserialize_any(Scalar { target: self })
            }
        }
        impl Shell for $ty {
            fn empty() -> Self {
                $empty
            }
        }
    };
}
struct Scalar<'a, T> {
    target: &'a mut T,
}

// IoRead emits borrowed visit_str, never visit_string/visit_byte_buf. Include
// borrowed text and bytes explicitly so no rejecting visitor can format input.
macro_rules! reject_text {
    () => {
        fn visit_str<E: de::Error>(self, _: &str) -> Result<Self::Value, E> {
            Err(E::custom("unexpected string for guarded JSON shape"))
        }
        fn visit_borrowed_str<E: de::Error>(self, _: &'de str) -> Result<Self::Value, E> {
            Err(E::custom("unexpected string for guarded JSON shape"))
        }
        fn visit_bytes<E: de::Error>(self, _: &[u8]) -> Result<Self::Value, E> {
            Err(E::custom("unexpected bytes for guarded JSON shape"))
        }
        fn visit_borrowed_bytes<E: de::Error>(self, _: &'de [u8]) -> Result<Self::Value, E> {
            Err(E::custom("unexpected bytes for guarded JSON shape"))
        }
    };
}
leaf!(bool, false);
leaf!(u8, 0);
leaf!(u16, 0);
leaf!(u32, 0);
leaf!(u64, 0);
leaf!(usize, 0);
leaf!(i64, 0);
leaf!(f32, 0.0);
leaf!(f64, 0.0);

macro_rules! unit_enum {
    ($ty:ty, $first:ident $(, $other:ident)* $(,)?) => {
        impl Shell for $ty { fn empty() -> Self { Self::$first } }
        impl EnumBuild for $ty {
            const VARIANTS: &'static [&'static str] = &[stringify!($first), $(stringify!($other)),*];
            fn bare(&mut self, tag: usize) -> Result<(), &'static str> {
                *self = [Self::$first, $(Self::$other),*].into_iter().nth(tag).unwrap();
                Ok(())
            }
            fn payload<'de, D: Deserializer<'de>>(&mut self, tag: usize, _: &mut Context, decoder: D) -> Result<(), D::Error> {
                self.bare(tag).unwrap();
                Null.deserialize(decoder)
            }
        }
        impl Build for $ty {
            fn read_into<'de, D: Deserializer<'de>>(&mut self, context: &mut Context, decoder: D) -> Result<(), D::Error> {
                if context.messagepack {
                    decoder.deserialize_enum(stringify!($ty), <Self as EnumBuild>::VARIANTS, EnumObject { target: self, context })
                } else { decoder.deserialize_any(EnumObject { target: self, context }) }
            }
        }
    };
}
unit_enum!(DenseMetric, Cosine, Euclidean, DotProduct);
unit_enum!(
    SchemaValueTypeManifest,
    Bool,
    Int,
    UInt,
    Float,
    Number,
    String,
    Bytes,
    Array,
    Map
);
unit_enum!(SchemaAdditionalPropertiesManifest, Allow, Reject);
unit_enum!(SchemaVectorPresenceManifest, Optional, Required, Forbidden);
unit_enum!(SecondaryIndexKind, Equality, Range);
unit_enum!(SecondaryIndexState, Building, Ready, Failed);
unit_enum!(FlushEpochState, FrozenPendingFlush, PublishedPendingRetire);
unit_enum!(
    NodeMetadataIndexFieldManifest,
    Id,
    Key,
    Weight,
    CreatedAt,
    UpdatedAt
);
unit_enum!(
    EdgeMetadataIndexFieldManifest,
    Id,
    From,
    To,
    Weight,
    CreatedAt,
    UpdatedAt,
    ValidFrom,
    ValidTo
);

impl<const N: usize> Shell for [u8; N] {
    fn empty() -> Self {
        [0; N]
    }
}
struct FixedBytes<'a, const N: usize> {
    target: &'a mut [u8; N],
    context: &'a mut Context,
}
impl<'de, const N: usize> Visitor<'de> for FixedBytes<'_, N> {
    type Value = ();
    fn expecting(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.write_str("a fixed byte array")
    }
    fn visit_seq<A: SeqAccess<'de>>(self, mut access: A) -> Result<(), A::Error> {
        for (index, target) in self.target.iter_mut().enumerate() {
            if access
                .next_element_seed(Seed {
                    target,
                    context: self.context,
                })?
                .is_none()
            {
                return Err(de::Error::invalid_length(index, &"a fixed byte array"));
            }
        }
        Ok(())
    }
    reject_text!();
}
impl<const N: usize> Build for [u8; N] {
    fn read_into<'de, D: Deserializer<'de>>(
        &mut self,
        context: &mut Context,
        decoder: D,
    ) -> Result<(), D::Error> {
        if context.messagepack {
            decoder.deserialize_tuple(
                N,
                FixedBytes {
                    target: self,
                    context,
                },
            )
        } else {
            decoder.deserialize_any(FixedBytes {
                target: self,
                context,
            })
        }
    }
}

impl Shell for String {
    fn empty() -> Self {
        String::new()
    }
}
struct Text<'a> {
    target: &'a mut String,
    allocation: &'a mut Allocation,
}
impl<'de> Visitor<'de> for Text<'_> {
    type Value = ();
    fn expecting(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.write_str("a string")
    }
    fn visit_str<E: de::Error>(self, text: &str) -> Result<(), E> {
        let needed = text.len().saturating_sub(self.target.capacity());
        if needed != 0 {
            // Grow fallibly before writing the borrowed text.
            self.target
                .try_reserve_exact(text.len() - self.target.len())
                .map_err(|_| self.allocation.refuse::<E>(text.len()))?;
        }
        self.target.push_str(text);
        Ok(())
    }
    fn visit_borrowed_str<E: de::Error>(self, text: &'de str) -> Result<(), E> {
        self.visit_str(text)
    }
    fn visit_bytes<E: de::Error>(self, bytes: &[u8]) -> Result<(), E> {
        let text = std::str::from_utf8(bytes)
            .map_err(|_| E::custom("invalid UTF-8 for guarded string"))?;
        self.visit_str(text)
    }
    fn visit_borrowed_bytes<E: de::Error>(self, bytes: &'de [u8]) -> Result<(), E> {
        self.visit_bytes(bytes)
    }
}
impl Build for String {
    fn read_into<'de, D: Deserializer<'de>>(
        &mut self,
        context: &mut Context,
        decoder: D,
    ) -> Result<(), D::Error> {
        debug_assert!(self.is_empty());
        decoder.deserialize_string(Text {
            target: self,
            allocation: &mut context.allocation,
        })
    }
}
impl<T: Shell> Shell for Option<T> {
    fn empty() -> Self {
        None
    }
}
struct Optional<'a, T> {
    target: &'a mut Option<T>,
    context: &'a mut Context,
}
impl<'de, T: Shell> Visitor<'de> for Optional<'_, T> {
    type Value = ();
    fn expecting(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.write_str("option")
    }
    fn visit_none<E: de::Error>(self) -> Result<(), E> {
        Ok(())
    }
    fn visit_unit<E: de::Error>(self) -> Result<(), E> {
        Ok(())
    }
    fn visit_some<D: Deserializer<'de>>(self, decoder: D) -> Result<(), D::Error> {
        *self.target = Some(T::empty());
        Seed {
            target: self.target.as_mut().unwrap(),
            context: self.context,
        }
        .deserialize(decoder)
    }
}
impl<T: Shell> Build for Option<T> {
    fn read_into<'de, D: Deserializer<'de>>(
        &mut self,
        context: &mut Context,
        decoder: D,
    ) -> Result<(), D::Error> {
        debug_assert!(self.is_none());
        decoder.deserialize_option(Optional {
            target: self,
            context,
        })
    }
}

impl<T: Shell> Shell for Vec<T> {
    fn empty() -> Self {
        Vec::new()
    }
}
struct Push<'a, T> {
    target: &'a mut Vec<T>,
    context: &'a mut Context,
}
impl<'de, T: Shell> DeserializeSeed<'de> for Push<'_, T> {
    type Value = ();
    fn deserialize<D: Deserializer<'de>>(self, decoder: D) -> Result<(), D::Error> {
        self.context.grow(self.target)?;
        self.target.push(T::empty()); // actual element exists before any child decode
        Seed {
            target: self.target.last_mut().unwrap(),
            context: self.context,
        }
        .deserialize(decoder)
    }
}
struct Sequence<'a, T> {
    target: &'a mut Vec<T>,
    context: &'a mut Context,
}
impl<'de, T: Shell> Visitor<'de> for Sequence<'_, T> {
    type Value = ();
    fn expecting(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.write_str("a sequence")
    }
    fn visit_seq<A: SeqAccess<'de>>(self, mut access: A) -> Result<(), A::Error> {
        while access
            .next_element_seed(Push {
                target: self.target,
                context: self.context,
            })?
            .is_some()
        {}
        Ok(())
    }
    reject_text!();
}
impl<T: Shell> Build for Vec<T> {
    fn read_into<'de, D: Deserializer<'de>>(
        &mut self,
        context: &mut Context,
        decoder: D,
    ) -> Result<(), D::Error> {
        if context.messagepack {
            decoder.deserialize_seq(Sequence {
                target: self,
                context,
            })
        } else {
            decoder.deserialize_any(Sequence {
                target: self,
                context,
            })
        }
    }
}

trait MapElement: Shell {
    const RETAIN_OLD: bool;
    fn retain_old(self, context: &mut Context);
}
impl MapElement for u32 {
    const RETAIN_OLD: bool = false;
    fn retain_old(self, _: &mut Context) {}
}
impl MapElement for PropValue {
    const RETAIN_OLD: bool = true;
    fn retain_old(self, context: &mut Context) {
        context.displaced.push(Displaced::Value(self));
    }
}
impl MapElement for PropertySchemaManifestRule {
    const RETAIN_OLD: bool = true;
    fn retain_old(self, context: &mut Context) {
        context.displaced.push(Displaced::Rule(self));
    }
}
impl MapElement for PrunePolicy {
    const RETAIN_OLD: bool = true;
    fn retain_old(self, context: &mut Context) {
        context.displaced.push(Displaced::Policy(self));
    }
}
impl<T: MapElement> Shell for BTreeMap<String, T> {
    fn empty() -> Self {
        BTreeMap::new()
    }
}
struct Mapping<'a, T> {
    target: &'a mut BTreeMap<String, T>,
    context: &'a mut Context,
}
impl<'de, T: MapElement> Visitor<'de> for Mapping<'_, T> {
    type Value = ();
    fn expecting(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.write_str("a map")
    }
    fn visit_map<A: MapAccess<'de>>(self, mut access: A) -> Result<(), A::Error> {
        while access
            .next_key_seed(Key {
                context: self.context,
            })?
            .is_some()
        {
            if let Some(destination) = self.target.get_mut(self.context.pending_key.as_str()) {
                if T::RETAIN_OLD {
                    self.context.prepare_displaced::<A::Error>()?;
                }
                let old = std::mem::replace(destination, T::empty());
                old.retain_old(self.context);
                self.context.pending_key.clear(); // retain its allocation in the job
                access.next_value_seed(Seed {
                    target: destination,
                    context: self.context,
                })?;
                continue;
            }

            // No fallible operation occurs between detach and registered insert.
            let key = std::mem::take(&mut self.context.pending_key);
            let Entry::Vacant(entry) = self.target.entry(key) else {
                unreachable!("exclusive unique JSON key")
            };
            let destination = entry.insert(T::empty());
            access.next_value_seed(Seed {
                target: destination,
                context: self.context,
            })?;
        }
        Ok(())
    }
    reject_text!();
}
struct Key<'a> {
    context: &'a mut Context,
}
impl<'de> DeserializeSeed<'de> for Key<'_> {
    type Value = ();
    fn deserialize<D: Deserializer<'de>>(self, decoder: D) -> Result<(), D::Error> {
        debug_assert!(self.context.pending_key.is_empty());
        decoder.deserialize_string(Text {
            target: &mut self.context.pending_key,
            allocation: &mut self.context.allocation,
        })
    }
}
impl<T: MapElement> Build for BTreeMap<String, T> {
    fn read_into<'de, D: Deserializer<'de>>(
        &mut self,
        context: &mut Context,
        decoder: D,
    ) -> Result<(), D::Error> {
        decoder.deserialize_any(Mapping {
            target: self,
            context,
        })
    }
}

trait Shape: Shell {
    fn fields(&self) -> &'static [&'static str];
    fn name(&self) -> &'static str;
    fn map_required(&self) -> u64;
    fn sequence_required(&self) -> u64;
    fn field<'de, D: Deserializer<'de>>(
        &mut self,
        index: usize,
        context: &mut Context,
        decoder: D,
    ) -> Result<(), D::Error>;
}
struct Field<'a, T> {
    target: &'a mut T,
    index: usize,
    context: &'a mut Context,
}
impl<'de, T: Shape> DeserializeSeed<'de> for Field<'_, T> {
    type Value = ();
    fn deserialize<D: Deserializer<'de>>(self, decoder: D) -> Result<(), D::Error> {
        self.target.field(self.index, self.context, decoder)
    }
}
struct Identifier {
    names: &'static [&'static str],
    variant: bool,
    messagepack: bool,
}
impl<'de> Visitor<'de> for Identifier {
    type Value = Option<usize>;
    fn expecting(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.write_str("field identifier")
    }
    fn visit_str<E: de::Error>(self, value: &str) -> Result<Self::Value, E> {
        let found = self.names.iter().position(|name| *name == value);
        if found.is_none() && self.variant {
            return Err(E::custom("unknown guarded JSON enum variant"));
        }
        Ok(found)
    }
    fn visit_borrowed_str<E: de::Error>(self, value: &'de str) -> Result<Self::Value, E> {
        self.visit_str(value)
    }
    fn visit_u64<E: de::Error>(self, value: u64) -> Result<Self::Value, E> {
        if !self.messagepack {
            return Err(E::custom("unexpected numeric guarded identifier"));
        }
        let found = usize::try_from(value)
            .ok()
            .filter(|index| *index < self.names.len());
        if self.variant && found.is_none() {
            return Err(E::custom("unknown guarded enum variant"));
        }
        Ok(found)
    }
    fn visit_bytes<E: de::Error>(self, bytes: &[u8]) -> Result<Self::Value, E> {
        if !self.messagepack {
            return Err(E::custom("unexpected bytes for guarded identifier"));
        }
        match std::str::from_utf8(bytes) {
            Ok(text) => self.visit_str(text),
            Err(_) if !self.variant => Ok(None),
            Err(_) => Err(E::custom("unknown guarded enum variant")),
        }
    }
    fn visit_borrowed_bytes<E: de::Error>(self, bytes: &'de [u8]) -> Result<Self::Value, E> {
        self.visit_bytes(bytes)
    }
}
impl<'de> DeserializeSeed<'de> for Identifier {
    type Value = Option<usize>;
    fn deserialize<D: Deserializer<'de>>(self, decoder: D) -> Result<Self::Value, D::Error> {
        decoder.deserialize_identifier(self)
    }
}
/// The private borrowed sidecar decoder consumes unknown values iteratively.
/// This seed returns no owned input, result or temporary representation.
struct IgnoredMessagePack;
impl<'de> DeserializeSeed<'de> for IgnoredMessagePack {
    type Value = ();
    fn deserialize<D: Deserializer<'de>>(self, decoder: D) -> Result<(), D::Error> {
        decoder.deserialize_ignored_any(self)
    }
}
impl<'de> Visitor<'de> for IgnoredMessagePack {
    type Value = ();
    fn expecting(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.write_str("an iteratively ignored value")
    }
    fn visit_unit<E: de::Error>(self) -> Result<(), E> {
        Ok(())
    }
}

struct Object<'a, T> {
    target: &'a mut T,
    context: &'a mut Context,
}
impl<'de, T: Shape> Visitor<'de> for Object<'_, T> {
    type Value = ();
    fn expecting(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "struct {}", self.target.name())
    }
    fn visit_map<A: MapAccess<'de>>(self, mut access: A) -> Result<(), A::Error> {
        let fields = self.target.fields();
        let mut seen = 0u64;
        while let Some(index) = access.next_key_seed(Identifier {
            names: fields,
            variant: false,
            messagepack: self.context.messagepack,
        })? {
            if let Some(index) = index {
                let bit = 1u64 << index;
                if seen & bit != 0 {
                    return Err(de::Error::duplicate_field(fields[index]));
                }
                seen |= bit;
                access.next_value_seed(Field {
                    target: self.target,
                    index,
                    context: self.context,
                })?;
            } else {
                if self.context.messagepack {
                    access.next_value_seed(IgnoredMessagePack)?;
                } else {
                    access.next_value::<de::IgnoredAny>()?;
                }
            }
        }
        for (index, field) in fields.iter().enumerate() {
            if self.target.map_required() & (1 << index) != 0 && seen & (1 << index) == 0 {
                return Err(de::Error::missing_field(field));
            }
        }
        Ok(())
    }
    fn visit_seq<A: SeqAccess<'de>>(self, mut access: A) -> Result<(), A::Error> {
        for index in 0..self.target.fields().len() {
            if access
                .next_element_seed(Field {
                    target: self.target,
                    index,
                    context: self.context,
                })?
                .is_none()
                && self.target.sequence_required() & (1 << index) != 0
            {
                return Err(de::Error::invalid_length(index, &self));
            }
        }
        Ok(())
    }
    reject_text!();
}

macro_rules! structure {
    ($ty:ty, $empty:expr, [$($field:ident: ($map:expr, $seq:expr)),* $(,)?]) => {
        impl Shell for $ty { fn empty() -> Self { $empty } }
        impl Shape for $ty {
            fn fields(&self) -> &'static [&'static str] { &[$(stringify!($field)),*] }
            fn name(&self) -> &'static str { stringify!($ty) }
            fn map_required(&self) -> u64 { let mut index = 0; let mut mask = 0; $(if $map { mask |= 1 << index; } index += 1;)* let _ = index; mask }
            fn sequence_required(&self) -> u64 { let mut index = 0; let mut mask = 0; $(if $seq { mask |= 1 << index; } index += 1;)* let _ = index; mask }
            fn field<'de, D: Deserializer<'de>>(&mut self, wanted: usize, context: &mut Context, decoder: D) -> Result<(), D::Error> {
                let mut index = 0;
                $(if wanted == index { return Seed { target: &mut self.$field, context }.deserialize(decoder); } index += 1;)*
                let _ = index;
                unreachable!("closed struct field")
            }
        }
        impl Build for $ty {
            fn read_into<'de, D: Deserializer<'de>>(&mut self, context: &mut Context, decoder: D) -> Result<(), D::Error> {
                if context.messagepack {
                    decoder.deserialize_struct(stringify!($ty), &[$(stringify!($field)),*], Object { target: self, context })
                } else { decoder.deserialize_any(Object { target: self, context }) }
            }
        }
    };
}

structure!(SchemaNumericBoundManifest, SchemaNumericBoundManifest { value: PropValue::Null, inclusive: true }, [value: (true, true), inclusive: (false, false)]);
structure!(HnswConfig, HnswConfig::default(), [m: (true, true), ef_construction: (true, true)]);
structure!(DenseVectorConfig, DenseVectorConfig { dimension: 0, metric: DenseMetric::Cosine, hnsw: HnswConfig::default() }, [dimension: (true, true), metric: (true, true), hnsw: (false, false)]);
structure!(SegmentInfo, SegmentInfo { id: 0, node_count: 0, edge_count: 0, segment_format_version: 0, segment_data_id: [0; 32] }, [id: (true, true), node_count: (true, true), edge_count: (true, true), segment_format_version: (false, false), segment_data_id: (false, false)]);
structure!(FlushEpochMeta, FlushEpochMeta { epoch_id: 0, wal_generation_id: 0, state: FlushEpochState::FrozenPendingFlush, segment_id: None }, [epoch_id: (true, true), wal_generation_id: (true, true), state: (true, true), segment_id: (false, true)]);
structure!(DenseVectorSchemaManifestRule, DenseVectorSchemaManifestRule { presence: SchemaVectorPresenceManifest::Optional, dimension: None }, [presence: (false, false), dimension: (false, false)]);
structure!(SparseVectorSchemaManifestRule, SparseVectorSchemaManifestRule { presence: SchemaVectorPresenceManifest::Optional, min_entries: None, max_entries: None, max_dimension_id: None }, [presence: (false, false), min_entries: (false, false), max_entries: (false, false), max_dimension_id: (false, false)]);
structure!(EdgeValiditySchemaManifestRule, EdgeValiditySchemaManifestRule { require_valid_from_before_valid_to: false, valid_from_min: None, valid_from_max: None, valid_to_min: None, valid_to_max: None, allow_open_ended_valid_to: true }, [require_valid_from_before_valid_to: (false, false), valid_from_min: (false, false), valid_from_max: (false, false), valid_to_min: (false, false), valid_to_max: (false, false), allow_open_ended_valid_to: (false, false)]);
structure!(PropertySchemaManifestRule, PropertySchemaManifestRule { required: false, nullable: true, types: Vec::new(), numeric_min: None, numeric_max: None, string_min_bytes: None, string_max_bytes: None, bytes_min_len: None, bytes_max_len: None, array_min_items: None, array_max_items: None, map_min_entries: None, map_max_entries: None, enum_values: Vec::new() }, [required: (false, false), nullable: (false, false), types: (false, false), numeric_min: (false, false), numeric_max: (false, false), string_min_bytes: (false, false), string_max_bytes: (false, false), bytes_min_len: (false, false), bytes_max_len: (false, false), array_min_items: (false, false), array_max_items: (false, false), map_min_entries: (false, false), map_max_entries: (false, false), enum_values: (false, false)]);
structure!(NumericFieldSchemaManifestRule, NumericFieldSchemaManifestRule { min: None, max: None, finite: true }, [min: (false, false), max: (false, false), finite: (false, false)]);
structure!(StringFieldSchemaManifestRule, StringFieldSchemaManifestRule { min_bytes: None, max_bytes: None, enum_values: Vec::new() }, [min_bytes: (false, false), max_bytes: (false, false), enum_values: (false, false)]);
structure!(NodeLabelConstraintManifestRule, NodeLabelConstraintManifestRule { all_of: Vec::new(), any_of: Vec::new(), none_of: Vec::new() }, [all_of: (false, false), any_of: (false, false), none_of: (false, false)]);
structure!(EndpointLabelManifestRule, EndpointLabelManifestRule { all_of: Vec::new(), any_of: Vec::new(), none_of: Vec::new() }, [all_of: (false, false), any_of: (false, false), none_of: (false, false)]);
structure!(PrunePolicy, PrunePolicy { max_age_ms: None, max_weight: None, label: None }, [max_age_ms: (false, true), max_weight: (false, true), label: (false, true)]);
structure!(NodeSchemaManifestEntry, NodeSchemaManifestEntry { schema_id: 0, revision: 0, label_id: 0, created_at_ms: 0, updated_at_ms: 0, additional_properties: SchemaAdditionalPropertiesManifest::Allow, properties: BTreeMap::new(), key: None, label_constraints: None, weight: None, dense_vector: None, sparse_vector: None }, [schema_id: (true, true), revision: (true, true), label_id: (true, true), created_at_ms: (true, true), updated_at_ms: (true, true), additional_properties: (false, false), properties: (false, false), key: (false, false), label_constraints: (false, false), weight: (false, false), dense_vector: (false, false), sparse_vector: (false, false)]);
structure!(EdgeSchemaManifestEntry, EdgeSchemaManifestEntry { schema_id: 0, revision: 0, label_id: 0, created_at_ms: 0, updated_at_ms: 0, additional_properties: SchemaAdditionalPropertiesManifest::Allow, properties: BTreeMap::new(), from: None, to: None, allow_self_loops: true, weight: None, validity: None }, [schema_id: (true, true), revision: (true, true), label_id: (true, true), created_at_ms: (true, true), updated_at_ms: (true, true), additional_properties: (false, false), properties: (false, false), from: (false, false), to: (false, false), allow_self_loops: (false, false), weight: (false, false), validity: (false, false)]);
structure!(SecondaryIndexManifestEntry, SecondaryIndexManifestEntry { index_id: 0, target: SecondaryIndexTarget::NodeProperty { label_id: 0, prop_key: String::new() }, kind: SecondaryIndexKind::Equality, state: SecondaryIndexState::Building, last_error: None }, [index_id: (true, true), target: (true, true), kind: (true, true), state: (true, true), last_error: (false, false)]);
structure!(ManifestState, ManifestState { version: 0, label_token_schema_version: 0, node_label_tokens: BTreeMap::new(), edge_label_tokens: BTreeMap::new(), next_node_label_id: 0, next_edge_label_id: 0, segments: Vec::new(), next_node_id: 0, next_edge_id: 0, dense_vector: None, prune_policies: BTreeMap::new(), next_engine_seq: 0, next_wal_generation_id: 0, active_wal_generation_id: 0, pending_flush_epochs: Vec::new(), secondary_indexes: Vec::new(), next_secondary_index_id: 0, schema_catalog_version: 0, next_schema_id: 0, node_schemas: Vec::new(), edge_schemas: Vec::new() }, [version: (true, true), label_token_schema_version: (false, false), node_label_tokens: (false, false), edge_label_tokens: (false, false), next_node_label_id: (false, false), next_edge_label_id: (false, false), segments: (true, true), next_node_id: (true, true), next_edge_id: (true, true), dense_vector: (false, false), prune_policies: (false, false), next_engine_seq: (false, false), next_wal_generation_id: (false, false), active_wal_generation_id: (false, false), pending_flush_epochs: (false, false), secondary_indexes: (false, false), next_secondary_index_id: (false, false), schema_catalog_version: (false, false), next_schema_id: (false, false), node_schemas: (false, false), edge_schemas: (false, false)]);

impl Shell for PropValue {
    fn empty() -> Self {
        Self::Null
    }
}
// Closed persisted sidecar DTO inventory. No arbitrary Deserialize result may escape.
unit_enum!(
    PlannerStatsBuildKind,
    Flush,
    Compaction,
    SecondaryIndexRefresh
);
unit_enum!(PlannerStatsDeclaredIndexKind, Equality, Range);
unit_enum!(
    PlannerStatsDeclaredIndexTarget,
    NodeProperty,
    EdgeProperty,
    NodeFieldIndex,
    EdgeFieldIndex
);
unit_enum!(
    DeclaredIndexRuntimeCoverageState,
    Available,
    Missing,
    Corrupt,
    Unknown
);
unit_enum!(
    PropertyStatsTrackedReason,
    DeclaredEquality,
    DeclaredRange,
    DeclaredEqualityAndRange,
    GeneralTopProperty
);
unit_enum!(PlannerStatsDirection, Outgoing, Incoming);
structure!(SegmentPlannerStatsV1, SegmentPlannerStatsV1 { format_version: Shell::empty(), segment_id: Shell::empty(), build_kind: Shell::empty(), built_at_ms: Shell::empty(), declaration_fingerprint: Shell::empty(), declared_indexes: Shell::empty(), node_count: Shell::empty(), edge_count: Shell::empty(), truncated: Shell::empty(), general_property_stats_complete: Shell::empty(), general_property_sampled_node_count: Shell::empty(), general_property_sampled_raw_bytes: Shell::empty(), general_property_budget_exhausted: Shell::empty(), node_label_stats: Shell::empty(), timestamp_stats: Shell::empty(), property_stats: Shell::empty(), equality_index_stats: Shell::empty(), range_index_stats: Shell::empty(), adjacency_stats: Shell::empty(), node_id_sample: Shell::empty(), compound_index_stats: Shell::empty() }, [format_version: (true, true), segment_id: (true, true), build_kind: (true, true), built_at_ms: (true, true), declaration_fingerprint: (true, true), declared_indexes: (true, true), node_count: (true, true), edge_count: (true, true), truncated: (true, true), general_property_stats_complete: (true, true), general_property_sampled_node_count: (true, true), general_property_sampled_raw_bytes: (true, true), general_property_budget_exhausted: (true, true), node_label_stats: (true, true), timestamp_stats: (true, true), property_stats: (true, true), equality_index_stats: (true, true), range_index_stats: (true, true), adjacency_stats: (true, true), node_id_sample: (true, true), compound_index_stats: (false, false)]);
structure!(DeclaredIndexStatsFingerprint, DeclaredIndexStatsFingerprint { index_id: Shell::empty(), target: Shell::empty(), kind: Shell::empty(), target_label_id: Shell::empty(), prop_key: Shell::empty(), field_fingerprint: Shell::empty(), field_count: Shell::empty() }, [index_id: (true, true), target: (false, false), kind: (true, true), target_label_id: (true, true), prop_key: (true, true), field_fingerprint: (false, false), field_count: (false, false)]);
structure!(NodeLabelPlannerStats, NodeLabelPlannerStats { label_id: Shell::empty(), node_count: Shell::empty(), min_node_id: Shell::empty(), max_node_id: Shell::empty(), min_updated_at_ms: Shell::empty(), max_updated_at_ms: Shell::empty() }, [label_id: (true, true), node_count: (true, true), min_node_id: (false, true), max_node_id: (false, true), min_updated_at_ms: (false, true), max_updated_at_ms: (false, true)]);
structure!(TimestampPlannerStats, TimestampPlannerStats { label_id: Shell::empty(), count: Shell::empty(), min_ms: Shell::empty(), max_ms: Shell::empty(), buckets: Shell::empty() }, [label_id: (true, true), count: (true, true), min_ms: (true, true), max_ms: (true, true), buckets: (true, true)]);
structure!(TimestampBucket, TimestampBucket { upper_ms: Shell::empty(), count: Shell::empty() }, [upper_ms: (true, true), count: (true, true)]);
structure!(PropertyPlannerStats, PropertyPlannerStats { label_id: Shell::empty(), prop_key: Shell::empty(), tracked_reason: Shell::empty(), present_count: Shell::empty(), null_count: Shell::empty(), value_kind_counts: Shell::empty(), exact_distinct_count: Shell::empty(), distinct_lower_bound: Shell::empty(), top_values: Shell::empty(), numeric_summaries: Shell::empty() }, [label_id: (true, true), prop_key: (true, true), tracked_reason: (true, true), present_count: (true, true), null_count: (true, true), value_kind_counts: (true, true), exact_distinct_count: (false, true), distinct_lower_bound: (false, true), top_values: (true, true), numeric_summaries: (true, true)]);
structure!(ValueKindCounts, ValueKindCounts { null_count: Shell::empty(), bool_count: Shell::empty(), int_count: Shell::empty(), uint_count: Shell::empty(), float_count: Shell::empty(), string_count: Shell::empty(), bytes_count: Shell::empty(), array_count: Shell::empty(), map_count: Shell::empty() }, [null_count: (true, true), bool_count: (true, true), int_count: (true, true), uint_count: (true, true), float_count: (true, true), string_count: (true, true), bytes_count: (true, true), array_count: (true, true), map_count: (true, true)]);
structure!(RangeValueSummary, RangeValueSummary { count: Shell::empty(), min_key: Shell::empty(), max_key: Shell::empty(), buckets: Shell::empty() }, [count: (true, true), min_key: (false, true), max_key: (false, true), buckets: (true, true)]);
structure!(EqualityIndexPlannerStats, EqualityIndexPlannerStats { index_id: Shell::empty(), target_label_id: Shell::empty(), prop_key: Shell::empty(), total_postings: Shell::empty(), value_group_count: Shell::empty(), max_group_postings: Shell::empty(), top_value_hashes: Shell::empty(), sidecar_present_at_build: Shell::empty() }, [index_id: (true, true), target_label_id: (true, true), prop_key: (true, true), total_postings: (true, true), value_group_count: (true, true), max_group_postings: (true, true), top_value_hashes: (true, true), sidecar_present_at_build: (true, true)]);
structure!(RangeIndexPlannerStats, RangeIndexPlannerStats { index_id: Shell::empty(), target_label_id: Shell::empty(), prop_key: Shell::empty(), total_entries: Shell::empty(), min_key: Shell::empty(), max_key: Shell::empty(), buckets: Shell::empty(), sidecar_present_at_build: Shell::empty() }, [index_id: (true, true), target_label_id: (true, true), prop_key: (true, true), total_entries: (true, true), min_key: (false, true), max_key: (false, true), buckets: (true, true), sidecar_present_at_build: (true, true)]);
structure!(CompoundIndexPlannerStats, CompoundIndexPlannerStats { index_id: Shell::empty(), target: Shell::empty(), target_label_id: Shell::empty(), kind: Shell::empty(), field_fingerprint: Shell::empty(), field_count: Shell::empty(), total_postings: Shell::empty(), distinct_full_keys: Shell::empty(), prefix_stats: Shell::empty(), range_stats: Shell::empty(), coverage: Shell::empty() }, [index_id: (true, true), target: (true, true), target_label_id: (true, true), kind: (true, true), field_fingerprint: (true, true), field_count: (true, true), total_postings: (true, true), distinct_full_keys: (true, true), prefix_stats: (true, true), range_stats: (true, true), coverage: (true, true)]);
structure!(CompoundPrefixStats, CompoundPrefixStats { prefix_len: Shell::empty(), distinct_prefixes: Shell::empty(), max_postings_per_prefix: Shell::empty(), exact_prefix_postings: Shell::empty() }, [prefix_len: (true, true), distinct_prefixes: (true, true), max_postings_per_prefix: (true, true), exact_prefix_postings: (true, true)]);
structure!(CompoundExactPrefixStat, CompoundExactPrefixStat { encoded_prefix: Shell::empty(), postings: Shell::empty() }, [encoded_prefix: (true, true), postings: (true, true)]);
structure!(CompoundRangeStats, CompoundRangeStats { equality_prefix_len: Shell::empty(), range_field_ordinal: Shell::empty(), total_numeric_entries: Shell::empty(), min_key: Shell::empty(), max_key: Shell::empty(), buckets: Shell::empty() }, [equality_prefix_len: (true, true), range_field_ordinal: (true, true), total_numeric_entries: (true, true), min_key: (false, true), max_key: (false, true), buckets: (true, true)]);
structure!(RangeBucket, RangeBucket { upper_key: Shell::empty(), count: Shell::empty() }, [upper_key: (true, true), count: (true, true)]);
structure!(AdjacencyPlannerStats, AdjacencyPlannerStats { direction: Shell::empty(), edge_label_id: Shell::empty(), source_node_count: Shell::empty(), total_edges: Shell::empty(), min_fanout: Shell::empty(), max_fanout: Shell::empty(), p50_fanout: Shell::empty(), p90_fanout: Shell::empty(), p99_fanout: Shell::empty(), top_hubs: Shell::empty() }, [direction: (true, true), edge_label_id: (false, true), source_node_count: (true, true), total_edges: (true, true), min_fanout: (true, true), max_fanout: (true, true), p50_fanout: (true, true), p90_fanout: (true, true), p99_fanout: (true, true), top_hubs: (true, true)]);
structure!(ValueFrequency, ValueFrequency { value_hash: Shell::empty(), count: Shell::empty() }, [value_hash: (true, true), count: (true, true)]);
structure!(NodeFanoutFrequency, NodeFanoutFrequency { node_id: Shell::empty(), count: Shell::empty() }, [node_id: (true, true), count: (true, true)]);
structure!(SegmentComponentManifestWireV1, SegmentComponentManifestWireV1 { format_version: Shell::empty(), segment_format_version: Shell::empty(), segment_id: Shell::empty(), generation: Shell::empty(), built_at_ms: Shell::empty(), build_kind_tag: Shell::empty(), segment_data_id: Shell::empty(), node_count: Shell::empty(), edge_count: Shell::empty(), components: Shell::empty() }, [format_version: (true, true), segment_format_version: (true, true), segment_id: (true, true), generation: (true, true), built_at_ms: (true, true), build_kind_tag: (true, true), segment_data_id: (true, true), node_count: (true, true), edge_count: (true, true), components: (true, true)]);
structure!(SegmentComponentRecordWireV1, SegmentComponentRecordWireV1 { component_id: Shell::empty(), kind_tag: Shell::empty(), index_id: Shell::empty(), logical_format_version: Shell::empty(), created_generation: Shell::empty(), requirement_tag: Shell::empty(), fallback_tag: Shell::empty(), trust_class_tag: Shell::empty(), handle: Shell::empty(), payload_len: Shell::empty(), payload_digest: Shell::empty(), dependency_digest: Shell::empty(), dependencies: Shell::empty(), build_fingerprint: Shell::empty() }, [component_id: (true, true), kind_tag: (true, true), index_id: (false, true), logical_format_version: (true, true), created_generation: (true, true), requirement_tag: (true, true), fallback_tag: (true, true), trust_class_tag: (true, true), handle: (true, true), payload_len: (true, true), payload_digest: (false, true), dependency_digest: (true, true), dependencies: (true, true), build_fingerprint: (true, true)]);
structure!(ComponentHandleWireV1, ComponentHandleWireV1 { handle_tag: Shell::empty(), relative_path: Shell::empty(), payload_offset: Shell::empty(), payload_len: Shell::empty(), container_component_id: Shell::empty(), offset: Shell::empty(), len: Shell::empty() }, [handle_tag: (true, true), relative_path: (false, true), payload_offset: (true, true), payload_len: (true, true), container_component_id: (false, true), offset: (true, true), len: (true, true)]);
structure!(ComponentDependencyWireV1, ComponentDependencyWireV1 { dependency_tag: Shell::empty(), component_kind_tag: Shell::empty(), component_index_id: Shell::empty(), component_id: Shell::empty(), group_tag: Shell::empty(), group_id: Shell::empty(), index_id: Shell::empty(), target_kind_tag: Shell::empty(), secondary_index_kind_tag: Shell::empty(), fingerprint: Shell::empty() }, [dependency_tag: (true, true), component_kind_tag: (false, true), component_index_id: (false, true), component_id: (false, true), group_tag: (false, true), group_id: (false, true), index_id: (false, true), target_kind_tag: (false, true), secondary_index_kind_tag: (false, true), fingerprint: (false, true)]);

const PROPERTY_VARIANTS: &[&str] = &[
    "Null", "Bool", "Int", "UInt", "Float", "String", "Bytes", "Array", "Map",
];
/// Closed Serde-level enum bridge; native VariantAccess helpers would re-enter
/// typed unit/sequence/struct decoding before controlled visitors can reject text.
trait EnumBuild: Shell {
    const VARIANTS: &'static [&'static str];
    fn bare(&mut self, tag: usize) -> Result<(), &'static str>;
    fn payload<'de, D: Deserializer<'de>>(
        &mut self,
        tag: usize,
        context: &mut Context,
        decoder: D,
    ) -> Result<(), D::Error>;
}
struct EnumObject<'a, T> {
    target: &'a mut T,
    context: &'a mut Context,
}
struct EnumPayload<'a, T> {
    target: &'a mut T,
    context: &'a mut Context,
    tag: usize,
}
impl<'de, T: EnumBuild> DeserializeSeed<'de> for EnumPayload<'_, T> {
    type Value = ();
    fn deserialize<D: Deserializer<'de>>(self, decoder: D) -> Result<(), D::Error> {
        self.target.payload(self.tag, self.context, decoder)
    }
}
impl<'de, T: EnumBuild> Visitor<'de> for EnumObject<'_, T> {
    type Value = ();
    fn expecting(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.write_str("a guarded JSON enum")
    }
    fn visit_str<E: de::Error>(self, text: &str) -> Result<(), E> {
        let Some(tag) = T::VARIANTS.iter().position(|name| *name == text) else {
            return Err(E::custom("unknown guarded JSON enum variant"));
        };
        self.target.bare(tag).map_err(E::custom)
    }
    fn visit_borrowed_str<E: de::Error>(self, text: &'de str) -> Result<(), E> {
        self.visit_str(text)
    }
    fn visit_enum<A: de::EnumAccess<'de>>(self, access: A) -> Result<(), A::Error> {
        let (tag, variant) = access.variant_seed(Identifier {
            names: T::VARIANTS,
            variant: true,
            messagepack: self.context.messagepack,
        })?;
        let tag = tag.ok_or_else(|| de::Error::custom("unknown guarded enum variant"))?;
        self.target.bare(tag).map_err(de::Error::custom)?;
        de::VariantAccess::unit_variant(variant)
    }
    fn visit_map<A: MapAccess<'de>>(self, mut access: A) -> Result<(), A::Error> {
        let Some(Some(tag)) = access.next_key_seed(Identifier {
            names: T::VARIANTS,
            variant: true,
            messagepack: self.context.messagepack,
        })?
        else {
            return Err(de::Error::custom("expected one guarded JSON enum entry"));
        };
        access.next_value_seed(EnumPayload {
            target: self.target,
            context: self.context,
            tag,
        })?;
        if access
            .next_key_seed(Identifier {
                names: &[],
                variant: false,
                messagepack: self.context.messagepack,
            })?
            .is_some()
        {
            return Err(de::Error::custom("extra guarded JSON enum entry"));
        }
        Ok(())
    }
}
struct Null;
impl<'de> Visitor<'de> for Null {
    type Value = ();
    fn expecting(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.write_str("null")
    }
    fn visit_unit<E: de::Error>(self) -> Result<(), E> {
        Ok(())
    }
    reject_text!();
}
impl<'de> DeserializeSeed<'de> for Null {
    type Value = ();
    fn deserialize<D: Deserializer<'de>>(self, decoder: D) -> Result<(), D::Error> {
        decoder.deserialize_any(self)
    }
}
impl EnumBuild for PropValue {
    const VARIANTS: &'static [&'static str] = PROPERTY_VARIANTS;
    fn bare(&mut self, tag: usize) -> Result<(), &'static str> {
        if tag == 0 {
            Ok(())
        } else {
            Err("expected an enum payload")
        }
    }
    fn payload<'de, D: Deserializer<'de>>(
        &mut self,
        tag: usize,
        context: &mut Context,
        decoder: D,
    ) -> Result<(), D::Error> {
        *self = match tag {
            0 => return Null.deserialize(decoder),
            1 => Self::Bool(false),
            2 => Self::Int(0),
            3 => Self::UInt(0),
            4 => Self::Float(0.0),
            5 => Self::String(String::new()),
            6 => Self::Bytes(Vec::new()),
            7 => Self::Array(Vec::new()),
            8 => Self::Map(BTreeMap::new()),
            _ => unreachable!(),
        };
        match self {
            Self::Bool(target) => Seed { target, context }.deserialize(decoder),
            Self::Int(target) => Seed { target, context }.deserialize(decoder),
            Self::UInt(target) => Seed { target, context }.deserialize(decoder),
            Self::Float(target) => Seed { target, context }.deserialize(decoder),
            Self::String(target) => Seed { target, context }.deserialize(decoder),
            Self::Bytes(target) => Seed { target, context }.deserialize(decoder),
            Self::Array(target) => Seed { target, context }.deserialize(decoder),
            Self::Map(target) => Seed { target, context }.deserialize(decoder),
            Self::Null => unreachable!(),
        }
    }
}
impl Build for PropValue {
    fn read_into<'de, D: Deserializer<'de>>(
        &mut self,
        context: &mut Context,
        decoder: D,
    ) -> Result<(), D::Error> {
        decoder.deserialize_any(EnumObject {
            target: self,
            context,
        })
    }
}

trait Tagged: Shape {
    const VARIANTS: &'static [&'static str];
    const ENUM: &'static str;
    fn select(&mut self, tag: usize);
}
macro_rules! tagged_build {
    ($ty:ty) => {
        impl EnumBuild for $ty {
            const VARIANTS: &'static [&'static str] = <Self as Tagged>::VARIANTS;
            fn bare(&mut self, _: usize) -> Result<(), &'static str> {
                Err("expected a struct variant payload")
            }
            fn payload<'de, D: Deserializer<'de>>(
                &mut self,
                tag: usize,
                context: &mut Context,
                decoder: D,
            ) -> Result<(), D::Error> {
                self.select(tag);
                decoder.deserialize_any(Object {
                    target: self,
                    context,
                })
            }
        }
        impl Build for $ty {
            fn read_into<'de, D: Deserializer<'de>>(
                &mut self,
                context: &mut Context,
                decoder: D,
            ) -> Result<(), D::Error> {
                decoder.deserialize_any(EnumObject {
                    target: self,
                    context,
                })
            }
        }
    };
}

impl Shell for SecondaryIndexTarget {
    fn empty() -> Self {
        Self::NodeProperty {
            label_id: 0,
            prop_key: String::new(),
        }
    }
}
impl Tagged for SecondaryIndexTarget {
    const ENUM: &'static str = "SecondaryIndexTarget";
    const VARIANTS: &'static [&'static str] = &[
        "NodeProperty",
        "EdgeProperty",
        "NodeFieldIndex",
        "EdgeFieldIndex",
    ];
    fn select(&mut self, tag: usize) {
        *self = match tag {
            0 => Self::NodeProperty {
                label_id: 0,
                prop_key: String::new(),
            },
            1 => Self::EdgeProperty {
                label_id: 0,
                prop_key: String::new(),
            },
            2 => Self::NodeFieldIndex {
                label_id: 0,
                fields: Vec::new(),
            },
            3 => Self::EdgeFieldIndex {
                label_id: 0,
                fields: Vec::new(),
            },
            _ => unreachable!(),
        };
    }
}
impl Shape for SecondaryIndexTarget {
    fn fields(&self) -> &'static [&'static str] {
        match self {
            Self::NodeProperty { .. } | Self::EdgeProperty { .. } => &["label_id", "prop_key"],
            _ => &["label_id", "fields"],
        }
    }
    fn name(&self) -> &'static str {
        Self::ENUM
    }
    fn map_required(&self) -> u64 {
        3
    }
    fn sequence_required(&self) -> u64 {
        3
    }
    fn field<'de, D: Deserializer<'de>>(
        &mut self,
        index: usize,
        context: &mut Context,
        decoder: D,
    ) -> Result<(), D::Error> {
        match self {
            Self::NodeProperty { label_id, prop_key }
            | Self::EdgeProperty { label_id, prop_key } => match index {
                0 => Seed {
                    target: label_id,
                    context,
                }
                .deserialize(decoder),
                1 => Seed {
                    target: prop_key,
                    context,
                }
                .deserialize(decoder),
                _ => unreachable!(),
            },
            Self::NodeFieldIndex { label_id, fields }
            | Self::EdgeFieldIndex { label_id, fields } => match index {
                0 => Seed {
                    target: label_id,
                    context,
                }
                .deserialize(decoder),
                1 => Seed {
                    target: fields,
                    context,
                }
                .deserialize(decoder),
                _ => unreachable!(),
            },
        }
    }
}
tagged_build!(SecondaryIndexTarget);
impl Shell for SecondaryIndexFieldManifest {
    fn empty() -> Self {
        Self::Property { key: String::new() }
    }
}
impl Tagged for SecondaryIndexFieldManifest {
    const ENUM: &'static str = "SecondaryIndexFieldManifest";
    const VARIANTS: &'static [&'static str] = &["Property", "NodeMetadata", "EdgeMetadata"];
    fn select(&mut self, tag: usize) {
        *self = match tag {
            0 => Self::Property { key: String::new() },
            1 => Self::NodeMetadata {
                field: NodeMetadataIndexFieldManifest::Id,
            },
            2 => Self::EdgeMetadata {
                field: EdgeMetadataIndexFieldManifest::Id,
            },
            _ => unreachable!(),
        };
    }
}
impl Shape for SecondaryIndexFieldManifest {
    fn fields(&self) -> &'static [&'static str] {
        match self {
            Self::Property { .. } => &["key"],
            _ => &["field"],
        }
    }
    fn name(&self) -> &'static str {
        Self::ENUM
    }
    fn map_required(&self) -> u64 {
        1
    }
    fn sequence_required(&self) -> u64 {
        1
    }
    fn field<'de, D: Deserializer<'de>>(
        &mut self,
        index: usize,
        context: &mut Context,
        decoder: D,
    ) -> Result<(), D::Error> {
        assert_eq!(index, 0);
        match self {
            Self::Property { key } => Seed {
                target: key,
                context,
            }
            .deserialize(decoder),
            Self::NodeMetadata { field } => Seed {
                target: field,
                context,
            }
            .deserialize(decoder),
            Self::EdgeMetadata { field } => Seed {
                target: field,
                context,
            }
            .deserialize(decoder),
        }
    }
}
tagged_build!(SecondaryIndexFieldManifest);

/// Sidecar decode state owned by the registered component/reader job. The only
/// roots admitted here are the fixed internal persisted DTO inventory above.
#[derive(Default)]
pub(crate) struct MessagePackConstruction {
    context: Context,
    error: Option<rmp_serde::decode::Error>,
    skip_workspace: super::messagepack::SkipWorkspace,
    started: bool,
}
impl MessagePackConstruction {
    pub(crate) fn allocation_error(&self, requested: usize) -> crate::error::EngineError {
        crate::error::EngineError::ResourceLimit {
            resource: "allocation_capacity",
            requested: requested as u64,
            limit: isize::MAX as u64,
            retryable: false,
        }
    }

    pub(crate) fn grow<T>(&mut self, values: &mut Vec<T>) -> Result<(), crate::error::EngineError> {
        self.context
            .grow::<T, rmp_serde::decode::Error>(values)
            .map_err(|_| self.resource_error())
    }
    fn resource_error(&mut self) -> crate::error::EngineError {
        match self
            .context
            .allocation
            .refusal
            .take()
            .expect("construction refusal")
        {
            PayloadError::Resource {
                resource,
                requested,
                limit,
                retryable,
            } => crate::error::EngineError::ResourceLimit {
                resource,
                requested: requested as u64,
                limit: limit as u64,
                retryable,
            },
            _ => unreachable!(),
        }
    }
    fn begin(&mut self, _wire_bytes: usize) -> Result<(), crate::error::EngineError> {
        if self.started {
            std::process::abort();
        }
        self.started = true;
        self.context.messagepack = true;
        Ok(())
    }
    fn finish(
        &mut self,
        result: Result<(), rmp_serde::decode::Error>,
    ) -> Result<(), crate::error::EngineError> {
        match result {
            Ok(()) => Ok(()),
            Err(error) => {
                self.error = Some(error);
                if let Some(bytes) = self.skip_workspace.allocation_failure.take() {
                    return Err(self.allocation_error(bytes));
                }
                if self.context.allocation.refusal.is_some() {
                    return Err(self.resource_error());
                }
                Err(crate::error::EngineError::SerializationError(
                    self.error.as_ref().unwrap().to_string(),
                ))
            }
        }
    }
    pub(crate) fn component_manifest(
        &mut self,
        target: &mut SegmentComponentManifestWireV1,
        bytes: &[u8],
    ) -> Result<(), crate::error::EngineError> {
        self.begin(bytes.len())?;
        let result = Seed {
            target,
            context: &mut self.context,
        }
        .deserialize(&mut super::messagepack::SidecarDecoder::new(
            bytes,
            &mut self.skip_workspace,
        ));
        self.finish(result)
    }
    pub(crate) fn planner_stats(
        &mut self,
        target: &mut SegmentPlannerStatsV1,
        bytes: &[u8],
    ) -> Result<(), crate::error::EngineError> {
        self.begin(bytes.len())?;
        let result = Seed {
            target,
            context: &mut self.context,
        }
        .deserialize(&mut super::messagepack::SidecarDecoder::new(
            bytes,
            &mut self.skip_workspace,
        ));
        self.finish(result)
    }
    pub(crate) fn empty_planner_stats() -> SegmentPlannerStatsV1 {
        SegmentPlannerStatsV1::empty()
    }
}

/// Construct only inside an already-registered BuildCell. A3 reuses this job in
/// bounded open; its parser and retained error never become foreground locals.
pub(crate) struct JsonJob {
    pub(crate) manifest: Owned<ManifestState>,
    pub(crate) value: Owned<PropValue>,
    context: Context,
    incoming: Option<Vec<u8>>,
    parser: Option<Parser>,
    error: Option<serde_json::Error>,
    started: bool,
}
impl Default for JsonJob {
    fn default() -> Self {
        Self {
            manifest: Owned::new(ManifestState::empty()),
            value: Owned::default(),
            context: Context::default(),
            incoming: None,
            parser: None,
            error: None,
            started: false,
        }
    }
}
impl JsonJob {
    fn begin(&mut self, bytes: Vec<u8>) -> Result<(), PayloadError> {
        // This is one registered construction, not a reusable decoder. A second
        // input has no reservation under this job's original grant. Fail-stop
        // before replacing any root or permitting a caught invariant unwind.
        if self.started || self.incoming.is_some() {
            eprintln!("guarded JSON invariant: repeated construction");
            std::process::abort();
        }
        // Register the incoming buffer before any fallible construction work.
        self.incoming = Some(bytes);
        self.started = true;
        let bytes = self.incoming.take().unwrap();
        self.parser = Some(serde_json::Deserializer::from_reader(Cursor::new(bytes)));
        Ok(())
    }
    fn finish(&mut self, result: Result<(), serde_json::Error>) -> Result<(), PayloadError> {
        match result {
            Ok(()) => Ok(()),
            Err(error) => {
                self.error = Some(error);
                Err(self
                    .context
                    .allocation
                    .refusal
                    .take()
                    .unwrap_or(PayloadError::Wire))
            }
        }
    }
    pub(crate) fn decode_value(&mut self, bytes: Vec<u8>) -> Result<(), PayloadError> {
        self.begin(bytes)?;
        let parser = self.parser.as_mut().unwrap();
        let result = Seed {
            target: &mut *self.value,
            context: &mut self.context,
        }
        .deserialize(&mut *parser)
        .and_then(|()| parser.end());
        self.finish(result)
    }
    pub(crate) fn decode_manifest(&mut self, bytes: Vec<u8>) -> Result<(), PayloadError> {
        self.begin(bytes)?;
        let parser = self.parser.as_mut().unwrap();
        let result = Seed {
            target: &mut *self.manifest,
            context: &mut self.context,
        }
        .deserialize(&mut *parser)
        .and_then(|()| parser.end());
        self.finish(result)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const LIMIT: usize = 32 * 1024 * 1024;

    // Compatibility oracle only. Production has a closed Build inventory and
    // registered JsonJob ownership, not a generic Deserialize fallback.
    fn closed_differential<T: Shell + serde::de::DeserializeOwned + serde::Serialize>(
        bytes: &[u8],
    ) {
        let expected = serde_json::from_slice::<T>(bytes);
        let mut actual = T::empty();
        let mut context = Context::default();

        let mut parser = serde_json::Deserializer::from_reader(Cursor::new(bytes));
        let result = Seed {
            target: &mut actual,
            context: &mut context,
        }
        .deserialize(&mut parser)
        .and_then(|()| parser.end());
        assert_eq!(
            result.is_ok(),
            expected.is_ok(),
            "type={} input={bytes:?} old={:?} new={result:?}",
            std::any::type_name::<T>(),
            expected.as_ref().err()
        );
        if let Ok(expected) = expected {
            assert_eq!(
                serde_json::to_vec(&actual).unwrap(),
                serde_json::to_vec(&expected).unwrap()
            );
        }
    }

    #[test]
    fn p45a2_payload_json_closed_dtos_enums_and_scalar_conversions() {
        let scalars = [
            "null",
            "true",
            "false",
            "-1",
            "-0",
            "0",
            "255",
            "256",
            "65535",
            "65536",
            "4294967295",
            "4294967296",
            "9223372036854775807",
            "-9223372036854775808",
            "9223372036854775808",
            "-9223372036854775809",
            "18446744073709551615",
            "18446744073709551616",
            "1.0",
            "1e0",
            "1e39",
            "1.0000000596046448",
            "2.2250738585072014e-308",
            "5e-324",
            "1e400",
            "\"x\"",
            "[]",
            "{}",
        ];
        for text in scalars {
            macro_rules! scalar { ($($ty:ty),*) => { $(closed_differential::<$ty>(text.as_bytes());)* } }
            scalar!(bool, u8, u16, u32, u64, usize, i64, f32, f64, String);
            for float in ["-0.0", "1.0000000596046448", "1e39", "5e-324"] {
                let mut context = Context::default();
                let mut single = 0.0f32;
                let mut parser = serde_json::Deserializer::from_str(float);
                Seed {
                    target: &mut single,
                    context: &mut context,
                }
                .deserialize(&mut parser)
                .unwrap();
                assert_eq!(
                    single.to_bits(),
                    serde_json::from_str::<f32>(float).unwrap().to_bits()
                );
                let mut double = 0.0f64;
                let mut parser = serde_json::Deserializer::from_str(float);
                Seed {
                    target: &mut double,
                    context: &mut context,
                }
                .deserialize(&mut parser)
                .unwrap();
                assert_eq!(
                    double.to_bits(),
                    serde_json::from_str::<f64>(float).unwrap().to_bits()
                );
            }
        }
        macro_rules! enums {
            ($ty:ty) => {
                for name in <$ty as EnumBuild>::VARIANTS {
                    closed_differential::<$ty>(format!("\"{name}\"").as_bytes());
                    for payload in ["null", "0", "\"x\"", "[]", "{}"] {
                        closed_differential::<$ty>(format!("{{\"{name}\":{payload}}}").as_bytes());
                    }
                }
                for text in ["\"unknown\"", "null", "0", "[]", "{}"] {
                    closed_differential::<$ty>(text.as_bytes());
                }
            };
        }
        enums!(DenseMetric);
        enums!(SchemaValueTypeManifest);
        enums!(SchemaAdditionalPropertiesManifest);
        enums!(SchemaVectorPresenceManifest);
        enums!(SecondaryIndexKind);
        enums!(SecondaryIndexState);
        enums!(FlushEpochState);
        enums!(NodeMetadataIndexFieldManifest);
        enums!(EdgeMetadataIndexFieldManifest);

        macro_rules! dto {
            ($ty:ty, [$($text:expr),* $(,)?]) => {
                for text in [$($text),*] { closed_differential::<$ty>(text.as_bytes()); }
                for text in ["null", "0", "\"x\"", "{}", "[]"] { closed_differential::<$ty>(text.as_bytes()); }
            }
        }
        dto!(
            HnswConfig,
            [
                r#"{"m":16,"ef_construction":200}"#,
                "[16,200]",
                "[65536,1]",
                "[16]",
                "[16,200,1]"
            ]
        );
        dto!(
            DenseVectorConfig,
            [
                r#"{"dimension":2,"metric":"Cosine"}"#,
                "[2,\"Euclidean\"]",
                "[2,\"DotProduct\",[16,200]]"
            ]
        );
        dto!(
            SegmentInfo,
            [r#"{"id":1,"node_count":2,"edge_count":3}"#, "[1,2,3]"]
        );
        dto!(
            FlushEpochMeta,
            [
                r#"{"epoch_id":1,"wal_generation_id":2,"state":"FrozenPendingFlush"}"#,
                "[1,2,\"FrozenPendingFlush\"]",
                "[1,2,\"PublishedPendingRetire\",null]"
            ]
        );
        dto!(
            DenseVectorSchemaManifestRule,
            [
                r#"{"presence":{"Required":null},"dimension":3}"#,
                "[\"Forbidden\",null]"
            ]
        );
        dto!(
            SparseVectorSchemaManifestRule,
            [
                r#"{"min_entries":2,"max_dimension_id":3}"#,
                "[\"Optional\",null,2,3]"
            ]
        );
        dto!(
            EdgeValiditySchemaManifestRule,
            [
                r#"{"valid_from_min":-1,"allow_open_ended_valid_to":false}"#,
                "[true,-1,0,1,2,false]"
            ]
        );
        dto!(
            SchemaNumericBoundManifest,
            [
                r#"{"value":{"Int":2}}"#,
                "[\"Null\"]",
                "[{\"UInt\":2},false]"
            ]
        );
        dto!(
            PropertySchemaManifestRule,
            [
                r#"{"required":true,"types":["Int",{"Float":null}],"enum_values":["Null"]}"#,
                "[true,false,[],null,null]"
            ]
        );
        dto!(
            NumericFieldSchemaManifestRule,
            [
                r#"{"min":{"value":{"Int":0}},"finite":false}"#,
                "[null,null]"
            ]
        );
        dto!(
            StringFieldSchemaManifestRule,
            [r#"{"min_bytes":1,"enum_values":["x"]}"#, "[1,2,[\"x\"]]"]
        );
        dto!(
            NodeLabelConstraintManifestRule,
            [r#"{"all_of":[1,2]}"#, "[[1],[2],[3]]"]
        );
        dto!(
            EndpointLabelManifestRule,
            [r#"{"none_of":[3]}"#, "[[1],[2],[3]]"]
        );
        dto!(
            PrunePolicy,
            [
                r#"{"max_weight":1.0000000596046448,"label":"x"}"#,
                "[null,1,\"x\"]",
                "[null]"
            ]
        );
        dto!(
            NodeSchemaManifestEntry,
            [
                "[1,2,3,4,5]",
                r#"{"schema_id":1,"revision":2,"label_id":3,"created_at_ms":4,"updated_at_ms":5,"dense_vector":{"presence":"Required"}}"#
            ]
        );
        dto!(
            EdgeSchemaManifestEntry,
            [
                "[1,2,3,4,5]",
                r#"{"schema_id":1,"revision":2,"label_id":3,"created_at_ms":4,"updated_at_ms":5,"allow_self_loops":false}"#
            ]
        );
        dto!(
            SecondaryIndexTarget,
            [
                r#"{"NodeProperty":[1,"x"]}"#,
                r#"{"EdgeProperty":{"label_id":1,"prop_key":"x"}}"#,
                r#"{"NodeFieldIndex":[1,[{"Property":{"key":"x"}}]]}"#,
                r#"{"EdgeFieldIndex":{"label_id":1,"fields":[{"EdgeMetadata":{"field":"From"}}]}}"#
            ]
        );
        dto!(
            SecondaryIndexFieldManifest,
            [
                r#"{"Property":["x"]}"#,
                r#"{"NodeMetadata":["Id"]}"#,
                r#"{"EdgeMetadata":{"field":"ValidTo"}}"#
            ]
        );
        dto!(
            SecondaryIndexManifestEntry,
            [
                r#"{"index_id":1,"target":{"NodeProperty":[1,"x"]},"kind":"Equality","state":"Ready"}"#,
                r#"[1,{"EdgeFieldIndex":[1,[]]},"Range","Failed","error"]"#
            ]
        );
        for length in [0, 31, 32, 33] {
            closed_differential::<[u8; 32]>(
                serde_json::to_string(&vec![255; length])
                    .unwrap()
                    .as_bytes(),
            );
        }
    }

    fn value_differential(bytes: &[u8]) {
        let expected = serde_json::from_slice::<PropValue>(bytes).map(Owned::new);
        let mut actual = JsonJob::default();
        let result = actual.decode_value(bytes.to_vec());
        assert_eq!(
            result.is_ok(),
            expected.is_ok(),
            "input={bytes:?}, old={:?}, new={result:?}",
            expected.as_ref().err()
        );
        if let Ok(expected) = expected {
            assert_eq!(
                serde_json::to_vec(&*actual.value).unwrap(),
                serde_json::to_vec(&*expected).unwrap()
            );
        }
    }

    fn manifest_differential(bytes: &[u8]) {
        let expected = serde_json::from_slice::<ManifestState>(bytes).map(Owned::new);
        let mut actual = JsonJob::default();
        let result = actual.decode_manifest(bytes.to_vec());
        assert_eq!(
            result.is_ok(),
            expected.is_ok(),
            "input={bytes:?}, old={:?}, new={result:?}",
            expected.as_ref().err()
        );
        if let Ok(expected) = expected {
            assert_eq!(
                serde_json::to_vec(&*actual.manifest).unwrap(),
                serde_json::to_vec(&*expected).unwrap()
            );
        }
    }

    #[test]
    fn p45a2_payload_json_pinned_grammar_and_manifest_defaults() {
        let names = [
            "Null", "Bool", "Int", "UInt", "Float", "String", "Bytes", "Array", "Map", "Other",
        ];
        let payloads = [
            "null",
            "false",
            "true",
            "-1",
            "0",
            "18446744073709551615",
            "1.5",
            "-0.0",
            "1e400",
            "\"x\\u0000\\uD834\\uDD1E\"",
            "[]",
            "[0,255]",
            "[256]",
            "[\"Null\"]",
            "{}",
            "{\"x\":\"Null\"}",
        ];
        for name in names {
            value_differential(format!("\"{name}\"").as_bytes());
            for payload in payloads {
                let bytes = format!("{{\"{name}\":{payload}}}").into_bytes();
                value_differential(&bytes);
                for end in 0..bytes.len() {
                    value_differential(&bytes[..end]);
                }
                value_differential(format!("{{\"{name}\":{payload}}} null").as_bytes());
                value_differential(format!("{{\"{name}\":{payload},\"Null\":null}}").as_bytes());
            }
        }
        value_differential(br#"{"Map":{"x":{"Array":[{"String":"old"}]},"x":{"Int":7}}}"#);
        let fields = [
            "", ",\"unknown\":[[[{}]]]", ",\"prune_policies\":{\"x\":{}}", ",\"prune_policies\":{\"x\":[]}",
            ",\"prune_policies\":{\"x\":[null,null,null]}",
            ",\"node_schemas\":[{\"schema_id\":1,\"revision\":2,\"label_id\":3,\"created_at_ms\":4,\"updated_at_ms\":5}]",
            ",\"node_schemas\":[[1,2,3,4,5]]",
            ",\"node_schemas\":[{\"schema_id\":1,\"revision\":2,\"label_id\":3,\"created_at_ms\":4,\"updated_at_ms\":5,\"properties\":{\"x\":{},\"x\":{\"numeric_min\":{\"value\":{\"Int\":1}},\"enum_values\":[\"Null\"]}},\"weight\":{}}]",
            ",\"edge_schemas\":[[1,2,3,4,5]]",
            ",\"secondary_indexes\":[{\"index_id\":1,\"target\":{\"NodeProperty\":{\"label_id\":3,\"prop_key\":\"x\"}},\"kind\":\"Equality\",\"state\":\"Building\"}]",
            ",\"secondary_indexes\":[{\"index_id\":1,\"target\":{\"NodeFieldIndex\":[3,[{\"Property\":[\"x\"]},{\"NodeMetadata\":{\"field\":\"Id\"}}]]},\"kind\":\"Equality\",\"state\":\"Building\"}]",
            ",\"next_node_id\":2", ",\"segments\":null", ",\"node_schemas\":[{}]",
        ];
        for fields in fields {
            let bytes = format!(
                "{{\"version\":1,\"segments\":[],\"next_node_id\":1,\"next_edge_id\":1{fields}}}"
            )
            .into_bytes();
            manifest_differential(&bytes);
            for end in 0..bytes.len() {
                manifest_differential(&bytes[..end]);
            }
        }
        for bytes in [
            b"[]".as_slice(),
            b"[1,0,{},{},0,0,[],1,1]",
            b"[1,0,{},{},0,0,[],1,1,null,{},0,0,0,[],[],0,0,0,[],[]]",
            b"{}",
        ] {
            manifest_differential(bytes);
        }
    }

    fn chain(depth: usize, leaf: &str, map: bool) -> String {
        let mut text = String::new();
        for _ in 0..depth {
            text.push_str(if map {
                "{\"Map\":{\"x\":"
            } else {
                "{\"Array\":["
            });
        }
        text.push_str(leaf);
        for _ in 0..depth {
            text.push_str(if map { "}}" } else { "]}" });
        }
        text
    }

    #[test]
    fn p45a2_payload_json_full_context_depth_and_ignored_values() {
        std::thread::Builder::new().stack_size(2 * 1024 * 1024).spawn(|| {
            for map in [false, true] {
                for (leaf, accepted_depth) in [("\"Null\"", 63), ("{\"Int\":1}", 63), ("{\"Bytes\":[]}", 62)] {
                    for depth in [accepted_depth, accepted_depth + 1] {
                        let bytes = chain(depth, leaf, map).into_bytes();
                        let old = serde_json::from_slice::<PropValue>(&bytes).map(Owned::new);
                        let mut job = JsonJob::default();
                        let result = job.decode_value(bytes);
                        assert_eq!(old.is_ok(), depth == accepted_depth, "leaf={leaf} depth={depth}");
                        assert_eq!(result.is_ok(), old.is_ok());
                    }
                }
                for depth in [60, 61] {
                    let literal = chain(depth, "{\"Int\":1}", map);
                    let bytes = format!("{{\"version\":1,\"segments\":[],\"next_node_id\":1,\"next_edge_id\":1,\"node_schemas\":[{{\"schema_id\":1,\"revision\":1,\"label_id\":1,\"created_at_ms\":0,\"updated_at_ms\":0,\"properties\":{{\"x\":{{\"enum_values\":[{literal}]}}}}}}]}}").into_bytes();
                    let expected = serde_json::from_slice::<ManifestState>(&bytes).map(Owned::new);
                    let mut job = JsonJob::default();
                    let result = job.decode_manifest(bytes);
                    assert_eq!(expected.is_ok(), depth == 60);
                    assert_eq!(result.is_ok(), expected.is_ok());
                }
            }
            // IgnoredAny deliberately has no typed recursion debit in the pinned
            // backend. Its continuation bytes stay in this job's parser scratch.
            let ignored = format!("{}0{}", "[".repeat(65_536), "]".repeat(65_536));
            let bytes = format!("{{\"version\":1,\"segments\":[],\"next_node_id\":1,\"next_edge_id\":1,\"unknown\":{ignored}}}").into_bytes();
            let mut job = JsonJob::default();
            job.decode_manifest(bytes).unwrap();
        }).unwrap().join().unwrap();
    }

    #[cfg(feature = "production-concurrency")]
    #[test]
    fn p45a2_payload_json_registered_error_slot_survives_worker_drop() {
        use super::super::super::reclamation::{ReclaimDomain, ResourceClass};
        use std::sync::{mpsc, Mutex};
        use std::time::{Duration, Instant};

        #[derive(Default)]
        struct PausedJob {
            json: JsonJob,
            pause: Option<Mutex<(mpsc::Sender<String>, mpsc::Receiver<()>)>>,
        }
        impl Drop for PausedJob {
            fn drop(&mut self) {
                if let Some(pause) = self.pause.take() {
                    let (ready, resume) = pause.into_inner().unwrap();
                    ready
                        .send(std::thread::current().name().unwrap().into())
                        .unwrap();
                    resume.recv().unwrap();
                }
            }
        }

        let domain = ReclaimDomain::new();
        let stats = domain.stats();
        let (ready, entered) = mpsc::channel();
        let (resume, wait) = mpsc::channel();
        let bytes = format!(
            "{{\"Map\":{{\"x\":{{\"String\":\"{}\"}},\"x\":{{\"Array\":[{{\"UInt\":-1}}]}}}}}}",
            "z".repeat(256 * 1024)
        )
        .into_bytes();
        let mut job = domain.build::<PausedJob>(ResourceClass::Staging).unwrap();
        let interrupted = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            job.mutate(|job| {
                assert_eq!(job.json.decode_value(bytes), Err(PayloadError::Wire));
                assert_eq!(job.json.context.displaced.len(), 1);
                job.pause = Some(Mutex::new((ready, wait)));
                panic!("named producer interruption after registered JSON failure");
            })
            .unwrap();
        }));
        assert!(interrupted.is_err());
        let owner = job.into_owner().unwrap();
        let occupied = stats.read().occupied;
        drop(owner);
        assert_eq!(
            entered.recv_timeout(Duration::from_secs(10)).unwrap(),
            "overgraph-reclaimer"
        );
        assert_eq!(
            stats.read().occupied,
            occupied,
            "shared slot held during actual Drop"
        );
        assert_eq!(stats.read().occupied, 1);
        resume.send(()).unwrap();
        domain.close();
        let deadline = Instant::now() + Duration::from_secs(10);
        while !stats.read().exited {
            assert!(Instant::now() < deadline);
            std::thread::yield_now();
        }
        assert_eq!(stats.read().occupied, 0);
        assert_eq!(stats.read().occupied, 0);
    }

    #[test]
    fn p45a2_sidecar_messagepack_scalar_binary_identifier_and_enum_grammar() {
        fn same<'de, T: Shell + Deserialize<'de> + std::fmt::Debug + PartialEq>(bytes: &'de [u8]) {
            let expected: T = rmp_serde::from_slice(bytes).unwrap();
            let mut actual = T::empty();
            let mut context = Context {
                messagepack: true,
                ..Context::default()
            };

            let mut workspace = super::super::messagepack::SkipWorkspace::default();

            Seed {
                target: &mut actual,
                context: &mut context,
            }
            .deserialize(&mut super::super::messagepack::SidecarDecoder::new(
                bytes,
                &mut workspace,
            ))
            .unwrap();
            assert_eq!(actual, expected);
        }
        same::<String>(&[0xc4, 3, b'a', b'b', b'c']);
        same::<Vec<u8>>(&[0xc4, 3, 1, 2, 3]);
        same::<[u8; 24]>(&[
            0xc4, 24, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21,
            22, 23,
        ]);
        for bytes in [
            &[0][..],
            &[1][..],
            &[0x81, 0, 0xc0][..],
            &[0xc4, 8, b'E', b'q', b'u', b'a', b'l', b'i', b't', b'y'][..],
        ] {
            same::<PlannerStatsDeclaredIndexKind>(bytes);
        }
        // Integer struct identifiers and ignored invalid-UTF8 unknown names use
        // the backend's original map grammar. Optional label bounds default.
        same::<NodeLabelPlannerStats>(&[0x83, 0, 7, 1, 1, 0xc4, 1, 0xff, 0]);
        // Old five-field fingerprint tuples omit the two serde(default) tails.
        same::<DeclaredIndexStatsFingerprint>(&[0x95, 1, 0, 0, 7, 0xa0]);
        // Fixed-length DTO growth ignores malicious hints and rejects before
        // allocating a result-sized vector from them.
        let mut target = Vec::<u64>::new();
        let mut context = Context {
            messagepack: true,
            ..Context::default()
        };

        let mut workspace = super::super::messagepack::SkipWorkspace::default();
        assert!(Seed {
            target: &mut target,
            context: &mut context
        }
        .deserialize(&mut super::super::messagepack::SidecarDecoder::new(
            &[0xdd, 0xff, 0xff, 0xff, 0xff],
            &mut workspace
        ))
        .is_err());
        assert!(target.capacity() <= 4);
        assert!(target.len() <= 1);
    }

    #[test]
    fn p45a2_sidecar_messagepack_closed_dto_and_malformed_grammar_differential() {
        fn agrees<'de, T: Shell + Deserialize<'de> + std::fmt::Debug + PartialEq>(
            bytes: &'de [u8],
        ) {
            let expected = rmp_serde::from_slice::<T>(bytes);
            let mut actual = T::empty();
            let mut context = Context {
                messagepack: true,
                ..Context::default()
            };

            let mut workspace = super::super::messagepack::SkipWorkspace::default();

            let result = Seed {
                target: &mut actual,
                context: &mut context,
            }
            .deserialize(&mut super::super::messagepack::SidecarDecoder::new(
                bytes,
                &mut workspace,
            ));
            assert_eq!(
                result.is_ok(),
                expected.is_ok(),
                "{}: {:?}",
                std::any::type_name::<T>(),
                bytes
            );
            match (expected, result) {
                (Ok(expected), Ok(())) => assert_eq!(actual, expected),
                (Err(_), Err(error)) => assert!(error.to_string().len() <= 1024),
                _ => unreachable!(),
            }
        }
        macro_rules! inventory {
            ($($ty:ty),* $(,)?) => { $(
                let empty = <$ty>::empty();
                for mut bytes in [rmp_serde::to_vec(&empty).unwrap(), rmp_serde::to_vec_named(&empty).unwrap()] {
                    agrees::<$ty>(&bytes);
                    for end in 0..bytes.len() { agrees::<$ty>(&bytes[..end]); }
                    bytes.extend_from_slice(&[0xc1, 0xff]);
                    agrees::<$ty>(&bytes); // original reader ignores trailing bytes
                }
            )* };
        }
        inventory!(
            SegmentPlannerStatsV1,
            DeclaredIndexStatsFingerprint,
            NodeLabelPlannerStats,
            TimestampPlannerStats,
            TimestampBucket,
            PropertyPlannerStats,
            ValueKindCounts,
            RangeValueSummary,
            EqualityIndexPlannerStats,
            RangeIndexPlannerStats,
            CompoundIndexPlannerStats,
            CompoundPrefixStats,
            CompoundExactPrefixStat,
            CompoundRangeStats,
            RangeBucket,
            AdjacencyPlannerStats,
            ValueFrequency,
            NodeFanoutFrequency,
            SegmentComponentManifestWireV1,
            SegmentComponentRecordWireV1,
            ComponentHandleWireV1,
            ComponentDependencyWireV1,
            PlannerStatsBuildKind,
            PlannerStatsDeclaredIndexKind,
            PlannerStatsDeclaredIndexTarget,
            DeclaredIndexRuntimeCoverageState,
            PropertyStatsTrackedReason,
            PlannerStatsDirection,
            String,
            Vec<u8>,
            Vec<u64>,
            [u8; 24],
            [u8; 32],
            Option<u64>
        );
        let mut scalar_inputs = vec![
            vec![0xc0],
            vec![0xc2],
            vec![0xc3],
            vec![0],
            vec![0x7f],
            vec![0xff],
            vec![0xcc, 255],
            vec![0xcd, 255, 255],
            vec![0xd0, 0x7f],
            vec![0xd0, 0xff],
            vec![0xd1, 0x80, 0],
            vec![0xd4, 0xff, 0],
            vec![0xc4, 1, 0xff],
            vec![0xa1, 0xff],
            vec![0xc1],
        ];
        for value in [0, i64::MAX as u64, u64::MAX] {
            let mut bytes = vec![0xcf];
            bytes.extend_from_slice(&value.to_be_bytes());
            scalar_inputs.push(bytes);
            let mut bytes = vec![0xd3];
            bytes.extend_from_slice(&value.to_be_bytes());
            scalar_inputs.push(bytes);
        }
        for value in [0.0f64, -0.0, 1.5, f64::INFINITY] {
            let mut bytes = vec![0xcb];
            bytes.extend_from_slice(&value.to_bits().to_be_bytes());
            scalar_inputs.push(bytes);
            let mut bytes = vec![0xca];
            bytes.extend_from_slice(&(value as f32).to_bits().to_be_bytes());
            scalar_inputs.push(bytes);
        }
        for bytes in &scalar_inputs {
            agrees::<bool>(bytes);
            agrees::<u8>(bytes);
            agrees::<u16>(bytes);
            agrees::<u32>(bytes);
            agrees::<u64>(bytes);
            agrees::<i64>(bytes);
            agrees::<f32>(bytes);
            agrees::<f64>(bytes);
            agrees::<String>(bytes);
            agrees::<PlannerStatsDeclaredIndexKind>(bytes);
        }
        for bytes in [
            &[0x81, 0, 0xc0][..],
            &[0xde, 0, 1, 0xcc, 1, 0xc0][..],
            &[0xdf, 0, 0, 0, 1, 0xcd, 0, 1, 0xc0][..],
            &[0x81, 0, 0xc2][..],
            &[0x82, 0, 0xc0, 1, 0xc0][..],
            &[0x80][..],
            &[0x91, 0][..],
            &[0xc4, 8, b'E', b'q', b'u', b'a', b'l', b'i', b't', b'y'][..],
        ] {
            agrees::<PlannerStatsDeclaredIndexKind>(bytes);
        }
        // Numeric/nonminimal identifiers, missing/default tails and duplicate
        // required fields agree with the original derived DTO reader.
        for bytes in [
            &[0xde, 0, 2, 0xcc, 0, 7, 0xcd, 0, 1, 1][..],
            &[0x83, 0, 7, 1, 1, 0, 8][..],
            &[0x82, 0, 7, 0xd0, 1, 1][..],
            &[0x82, 0, 7, 1, 1][..],
            &[0x91, 7][..],
            &[0x95, 1, 0, 0, 7, 0xa0][..],
        ] {
            agrees::<NodeLabelPlannerStats>(bytes);
            agrees::<DeclaredIndexStatsFingerprint>(bytes);
        }
        let long = rmp_serde::to_vec(&"unrecognized".repeat(128 * 1024)).unwrap();
        agrees::<PlannerStatsDeclaredIndexKind>(&long);
        agrees::<u64>(&long);
        agrees::<String>(&long);
    }

    #[test]
    fn p45a2_sidecar_messagepack_enclosing_dto_extension_and_map_key_depth() {
        std::thread::Builder::new()
            .stack_size(2 * 1024 * 1024)
            .spawn(|| {
                // The planner root has the same one-debit enclosing context as the
                // component root; its Map16 header and remaining fields are intact.
                let expected = SegmentPlannerStatsV1::empty();
                let encoded = rmp_serde::to_vec_named(&expected).unwrap();
                assert_eq!(&encoded[..3], &[0xde, 0, 21]);
                for depth in [1022, 1023] {
                    let mut bytes = vec![0xde, 0, 22, 0xa1, b'x'];
                    bytes.extend(std::iter::repeat_n(0x91, depth));
                    bytes.push(0xc0);
                    bytes.extend_from_slice(&encoded[3..]);
                    let mut root = SegmentPlannerStatsV1::empty();
                    let mut construction = MessagePackConstruction::default();
                    let result = construction.planner_stats(&mut root, &bytes);
                    if depth == 1022 {
                        result.unwrap();
                        assert_eq!(root, expected);
                    } else {
                        assert!(matches!(
                            construction.error,
                            Some(rmp_serde::decode::Error::DepthLimitExceeded)
                        ));
                    }
                }
                let mut expected = SegmentComponentManifestWireV1::default();
                expected
                    .components
                    .push(SegmentComponentRecordWireV1::default());
                let encoded = rmp_serde::to_vec_named(&expected).unwrap();
                let handle = rmp_serde::to_vec_named(&ComponentHandleWireV1::default()).unwrap();
                assert_eq!(handle[0], 0x87);
                let position = encoded
                    .windows(handle.len())
                    .position(|part| part == handle)
                    .unwrap();
                // root + components array + record + handle = four active debits.
                for depth in [1019, 1020] {
                    let mut bytes = encoded[..position].to_vec();
                    bytes.extend_from_slice(&[0x88, 0xa1, b'x']);
                    bytes.extend(std::iter::repeat_n(0x91, depth));
                    bytes.push(0xc0);
                    bytes.extend_from_slice(&handle[1..]);
                    bytes.extend_from_slice(&encoded[position + handle.len()..]);
                    let mut root = SegmentComponentManifestWireV1::default();
                    let mut construction = MessagePackConstruction::default();
                    let result = construction.component_manifest(&mut root, &bytes);
                    if depth == 1019 {
                        result.unwrap();
                        assert_eq!(root, expected);
                    } else {
                        assert!(matches!(
                            construction.error,
                            Some(rmp_serde::decode::Error::DepthLimitExceeded)
                        ));
                    }
                }
                let expected = SegmentComponentManifestWireV1::default();
                let encoded = rmp_serde::to_vec_named(&expected).unwrap();
                for (shape, depth, rejected) in [
                    ("ext", 1021, false),
                    ("ext", 1022, true),
                    ("key", 1022, false),
                    ("key", 1023, true),
                ] {
                    let mut bytes = vec![0x8b, 0xa1, b'x'];
                    if shape == "ext" {
                        bytes.extend(std::iter::repeat_n(0x91, depth));
                        bytes.extend_from_slice(&[0xd4, 255, 0xc1]);
                    } else {
                        bytes.extend(std::iter::repeat_n(0x81, depth));
                        bytes.push(0xc0);
                        bytes.extend(std::iter::repeat_n(0xc0, depth)); // values follow nested map keys
                    }
                    bytes.extend_from_slice(&encoded[1..]);
                    let mut root = SegmentComponentManifestWireV1::default();
                    let mut construction = MessagePackConstruction::default();
                    let result = construction.component_manifest(&mut root, &bytes);
                    if rejected {
                        assert!(matches!(
                            construction.error,
                            Some(rmp_serde::decode::Error::DepthLimitExceeded)
                        ));
                    } else {
                        result.unwrap();
                        assert_eq!(root, expected);
                    }
                }
            })
            .unwrap()
            .join()
            .unwrap();
    }

    #[test]
    fn p45a2_sidecar_messagepack_ignored_depth_preserves_native_stack_and_backend_limit() {
        const CHILD: &str = "OVERGRAPH_SIDECAR_IGNORED_DEPTH_CHILD";
        if let Some(case) = std::env::var_os(CHILD) {
            let case = case.to_str().unwrap().to_owned();
            std::thread::Builder::new()
                .stack_size(2 * 1024 * 1024)
                .spawn(move || {
                    let (kind, depth) = case.split_once(':').unwrap();
                    let (baseline, shape) = if let Some(shape) = kind.strip_prefix("baseline-") {
                        (true, shape)
                    } else {
                        (false, kind)
                    };
                    let depth = depth.parse::<usize>().unwrap();
                    let expected = SegmentComponentManifestWireV1 {
                        format_version: 1,
                        segment_format_version: crate::segment_writer::SEGMENT_FORMAT_VERSION,
                        segment_id: 1,
                        generation: 1,
                        built_at_ms: 0,
                        build_kind_tag: 1,
                        segment_data_id: [0; 32],
                        node_count: 0,
                        edge_count: 0,
                        components: Vec::new(),
                    };
                    let encoded = rmp_serde::to_vec_named(&expected).unwrap();
                    assert_eq!(encoded[0], 0x8a);
                    let mut bytes = vec![0x8b, 0xa1, b'x'];
                    match shape {
                        "array" => {
                            bytes.extend(std::iter::repeat_n(0x91, depth));
                            bytes.push(0xc0);
                        }
                        "map" => {
                            for _ in 0..depth {
                                bytes.extend_from_slice(&[0x81, 0xc0]);
                            }
                            bytes.push(0xc0);
                        }
                        "ext" => bytes.extend_from_slice(&[0xd4, 0xff, 0xa5]),
                        "utf8" => bytes.extend_from_slice(&[0xa1, 0xff]),
                        "truncated" => bytes.extend_from_slice(&[0xda, 0xff, 0xff]),
                        _ => unreachable!(),
                    }
                    bytes.extend_from_slice(&encoded[1..]);
                    println!("full component root, {}, bytes {}", case, bytes.len());
                    if baseline {
                        let result =
                            rmp_serde::from_slice::<SegmentComponentManifestWireV1>(&bytes);
                        // The enclosing manifest debits one of the backend's
                        // 1,024 levels. It rejects when the counter reaches
                        // zero, so 1,022 nested ignored containers are accepted.
                        if depth >= 1023 {
                            assert!(matches!(
                                result,
                                Err(rmp_serde::decode::Error::DepthLimitExceeded)
                            ));
                        } else if shape == "truncated" {
                            assert!(result.unwrap_err().to_string().len() <= 1024);
                        } else {
                            assert_eq!(result.unwrap(), expected);
                        }
                        return;
                    }
                    let mut root = SegmentComponentManifestWireV1::default();
                    let mut construction = MessagePackConstruction::default();
                    let result = construction.component_manifest(&mut root, &bytes);
                    if depth >= 1023 {
                        assert!(matches!(
                            construction.error,
                            Some(rmp_serde::decode::Error::DepthLimitExceeded)
                        ));
                    } else if shape == "truncated" {
                        assert!(result.unwrap_err().to_string().len() <= 1024);
                    } else {
                        result.unwrap();
                        assert_eq!(root, expected);
                    }
                })
                .unwrap()
                .join()
                .unwrap();
            return;
        }
        let mut failures = Vec::new();
        for case in [
            "array:127",
            "map:127",
            "ext:0",
            "utf8:0",
            "truncated:0",
            "array:1022",
            "map:1022",
            "array:1023",
            "map:1023",
            "array:1024",
            "map:1024",
            // Original maximum-depth stack aborts remain recorded in250.
            // Low-depth oracle cases validate grammar; the source-derived
            // maximum is tested positively through the guarded reader above.
            "baseline-array:127",
            "baseline-map:127",
        ] {
            let output=std::process::Command::new(std::env::current_exe().unwrap()).args(["--exact","engine::payload::json::tests::p45a2_sidecar_messagepack_ignored_depth_preserves_native_stack_and_backend_limit","--nocapture"]).env(CHILD,case).output().unwrap();
            println!("case {}: {}", case, output.status);
            if !output.status.success() {
                println!(
                    "{} {}",
                    String::from_utf8_lossy(&output.stdout),
                    String::from_utf8_lossy(&output.stderr)
                );
                failures.push(case);
            }
        }
        assert!(
            failures.is_empty(),
            "native ignored-field boundaries failed: {:?}",
            failures
        );
    }

    #[test]
    fn p45a2_payload_json_secondary_errors_and_guarded_unwind() {
        use std::panic::{catch_unwind, AssertUnwindSafe};
        let long = "x".repeat(256 * 1024);
        let inputs = [
            format!("{{\"Array\":[{{\"String\":\"{long}\"}},{{\"UInt\":\"wrong\"}} bad]}}"),
            format!(
                "{{\"Map\":{{\"x\":{{\"String\":\"{long}\"}},\"x\":{{\"Bool\":\"wrong\"}} bad}}}}"
            ),
            format!("{{\"Array\":[{{\"String\":\"{long}\"}}] , \"extra\":null}}"),
        ];
        for input in inputs {
            let mut job = JsonJob::default();
            assert_eq!(
                job.decode_value(input.into_bytes()),
                Err(PayloadError::Wire)
            );
            assert!(job.error.as_ref().unwrap().to_string().len() <= 1024);
        }
        let mut job = JsonJob::default();
        let bytes = format!("{{\"Array\":[{{\"String\":\"{long}\"}}]}}").into_bytes();
        assert!(catch_unwind(AssertUnwindSafe(|| {
            job.decode_value(bytes).unwrap();
            panic!("named producer interruption after registered JSON success");
        }))
        .is_err());
        let PropValue::Array(values) = &*job.value else {
            panic!("registered array")
        };
        assert!(matches!(&values[0], PropValue::String(value) if value.len() == long.len()));
        assert!(job.parser.is_some());
    }

    #[test]
    fn p45a2_payload_json_repeated_construction_is_fail_stop() {
        use std::panic::{catch_unwind, AssertUnwindSafe};
        if std::env::var_os("OVERGRAPH_P45A2_JSON_REENTRY_CHILD").is_some() {
            let mut job = JsonJob::default();
            job.decode_value(b"\"Null\"".to_vec()).unwrap();
            let second = vec![b' '; 256 * 1024];
            let _ = catch_unwind(AssertUnwindSafe(|| job.decode_value(second)));
            std::process::exit(0); // A returned/caught second attempt is a failure.
        }
        let output = std::process::Command::new(std::env::current_exe().unwrap())
            .args(["--exact", "engine::payload::json::tests::p45a2_payload_json_repeated_construction_is_fail_stop", "--nocapture"])
            .env("OVERGRAPH_P45A2_JSON_REENTRY_CHILD", "1")
            .output().unwrap();
        assert!(!output.status.success());
        #[cfg(unix)]
        {
            use std::os::unix::process::ExitStatusExt;
            assert_eq!(output.status.signal(), Some(6));
        }
        assert!(String::from_utf8_lossy(&output.stderr).contains("repeated construction"));
    }
}
