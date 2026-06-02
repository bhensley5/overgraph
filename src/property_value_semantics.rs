use crate::error::EngineError;
use crate::types::{fnv1a, PropValue, PropertyRangeBound, PropertyRangeCursor};
use std::cmp::Ordering;

pub(crate) const NUMERIC_RANGE_KEY_BYTES: usize = 24;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub(crate) enum NumericSign {
    Negative,
    Zero,
    Positive,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub(crate) struct NumericScalarKey {
    sign: NumericSign,
    exponent: i32,
    significand: u128,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub(crate) struct NumericRangeSortKey(pub(crate) [u8; NUMERIC_RANGE_KEY_BYTES]);

impl NumericRangeSortKey {
    pub(crate) fn as_bytes(self) -> [u8; NUMERIC_RANGE_KEY_BYTES] {
        self.0
    }

    pub(crate) fn from_sidecar_bytes(
        bytes: [u8; NUMERIC_RANGE_KEY_BYTES],
    ) -> Result<Self, EngineError> {
        validate_numeric_range_sidecar_key(&bytes)?;
        Ok(Self(bytes))
    }
}

pub(crate) fn validate_numeric_range_sidecar_key(
    bytes: &[u8; NUMERIC_RANGE_KEY_BYTES],
) -> Result<(), EngineError> {
    match bytes[0] {
        0 | 2 => {
            if bytes[21..24].iter().any(|byte| *byte != 0) {
                return Err(EngineError::CorruptRecord(
                    "numeric range sidecar key reserved bytes must be zero".to_string(),
                ));
            }
            let (key, rank, bit_length) = decode_nonzero_numeric_range_sidecar_key(bytes)?;
            if !numeric_scalar_key_is_emittable(key, rank, bit_length) {
                return Err(EngineError::CorruptRecord(
                    "numeric range sidecar key is not a canonical finite numeric key".to_string(),
                ));
            }
            if numeric_range_sort_key(key).as_bytes() != *bytes {
                return Err(EngineError::CorruptRecord(
                    "numeric range sidecar key does not round-trip through canonical encoding"
                        .to_string(),
                ));
            }
        }
        1 => {
            if bytes[1..24].iter().any(|byte| *byte != 0) {
                return Err(EngineError::CorruptRecord(
                    "numeric range sidecar zero key must have zero payload".to_string(),
                ));
            }
        }
        _ => {
            return Err(EngineError::CorruptRecord(
                "numeric range sidecar key has invalid class".to_string(),
            ));
        }
    }
    Ok(())
}

fn decode_nonzero_numeric_range_sidecar_key(
    bytes: &[u8; NUMERIC_RANGE_KEY_BYTES],
) -> Result<(NumericScalarKey, i32, u32), EngineError> {
    let sign = match bytes[0] {
        0 => NumericSign::Negative,
        2 => NumericSign::Positive,
        _ => unreachable!("caller validates nonzero numeric range key class"),
    };
    let encoded_rank = u32::from_be_bytes(bytes[1..5].try_into().unwrap());
    let encoded_fraction = u128::from_be_bytes(bytes[5..21].try_into().unwrap());
    let rank_bits = if sign == NumericSign::Negative {
        !encoded_rank
    } else {
        encoded_rank
    };
    let fraction = if sign == NumericSign::Negative {
        !encoded_fraction
    } else {
        encoded_fraction
    };
    if fraction == 0 {
        return Err(EngineError::CorruptRecord(
            "numeric range sidecar key has zero nonzero-fraction payload".to_string(),
        ));
    }
    if fraction.leading_zeros() != 0 {
        return Err(EngineError::CorruptRecord(
            "numeric range sidecar key normalized fraction must set the top bit".to_string(),
        ));
    }

    let rank = (rank_bits ^ 0x8000_0000) as i32;
    let bit_length = 128 - fraction.trailing_zeros();
    let significand = fraction >> (128 - bit_length);
    let rank_offset = bit_length as i32 - 1;
    let exponent = rank.checked_sub(rank_offset).ok_or_else(|| {
        EngineError::CorruptRecord("numeric range sidecar key exponent overflow".to_string())
    })?;
    Ok((
        NumericScalarKey {
            sign,
            exponent,
            significand,
        },
        rank,
        bit_length,
    ))
}

fn numeric_scalar_key_is_emittable(key: NumericScalarKey, rank: i32, bit_length: u32) -> bool {
    debug_assert!(key.sign != NumericSign::Zero);
    if key.significand == 0 || key.significand & 1 == 0 {
        return false;
    }

    let f64_emittable = key.exponent >= -1074 && bit_length <= 53 && rank <= 1023;
    if f64_emittable {
        return true;
    }

    if key.exponent < 0 {
        return false;
    }
    match key.sign {
        NumericSign::Positive => rank <= 63,
        NumericSign::Negative => {
            rank < 63 || (rank == 63 && key.significand == 1 && key.exponent == 63)
        }
        NumericSign::Zero => false,
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct NumericRangeBoundKey {
    pub(crate) key: NumericScalarKey,
    pub(crate) inclusive: bool,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct ValidatedNumericRange {
    pub(crate) lower: Option<NumericRangeBoundKey>,
    pub(crate) upper: Option<NumericRangeBoundKey>,
    pub(crate) is_empty: bool,
}

impl NumericScalarKey {
    fn zero() -> Self {
        Self {
            sign: NumericSign::Zero,
            exponent: 0,
            significand: 0,
        }
    }

    fn nonzero(sign: NumericSign, mut significand: u128, mut exponent: i32) -> Self {
        debug_assert!(sign != NumericSign::Zero);
        debug_assert!(significand != 0);
        let trailing = significand.trailing_zeros() as i32;
        significand >>= trailing;
        exponent += trailing;
        Self {
            sign,
            exponent,
            significand,
        }
    }

    fn bit_length(self) -> u32 {
        debug_assert!(self.significand != 0);
        128 - self.significand.leading_zeros()
    }

    fn magnitude_rank(self) -> i32 {
        self.exponent + self.bit_length() as i32 - 1
    }

    fn normalized_fraction(self) -> u128 {
        self.significand << (128 - self.bit_length())
    }
}

pub(crate) fn numeric_key_from_i64(value: i64) -> NumericScalarKey {
    if value == 0 {
        return NumericScalarKey::zero();
    }
    let sign = if value < 0 {
        NumericSign::Negative
    } else {
        NumericSign::Positive
    };
    NumericScalarKey::nonzero(sign, value.unsigned_abs() as u128, 0)
}

pub(crate) fn numeric_key_from_u64(value: u64) -> NumericScalarKey {
    if value == 0 {
        NumericScalarKey::zero()
    } else {
        NumericScalarKey::nonzero(NumericSign::Positive, value as u128, 0)
    }
}

pub(crate) fn exact_i64_to_f64(value: i64, context: &str) -> Result<f64, EngineError> {
    let converted = value as f64;
    let converted_key = numeric_key_from_f64(converted).ok_or_else(|| {
        EngineError::InvalidOperation(format!("{context} produced a non-finite float"))
    })?;
    let exact_key = numeric_key_from_i64(value);
    if converted_key == exact_key {
        Ok(converted)
    } else {
        Err(EngineError::InvalidOperation(format!(
            "{context} cannot represent integer value exactly as float"
        )))
    }
}

pub(crate) fn exact_u64_to_f64(value: u64, context: &str) -> Result<f64, EngineError> {
    let converted = value as f64;
    let converted_key = numeric_key_from_f64(converted).ok_or_else(|| {
        EngineError::InvalidOperation(format!("{context} produced a non-finite float"))
    })?;
    let exact_key = numeric_key_from_u64(value);
    if converted_key == exact_key {
        Ok(converted)
    } else {
        Err(EngineError::InvalidOperation(format!(
            "{context} cannot represent unsigned integer value exactly as float"
        )))
    }
}

pub(crate) fn numeric_key_from_f64(value: f64) -> Option<NumericScalarKey> {
    if !value.is_finite() {
        return None;
    }
    if value == 0.0 {
        return Some(NumericScalarKey::zero());
    }

    let bits = value.to_bits();
    let sign = if bits >> 63 == 0 {
        NumericSign::Positive
    } else {
        NumericSign::Negative
    };
    let exp_bits = ((bits >> 52) & 0x7ff) as i32;
    let fraction = bits & ((1_u64 << 52) - 1);
    let (significand, exponent) = if exp_bits == 0 {
        (fraction as u128, 1 - 1023 - 52)
    } else {
        (((1_u64 << 52) | fraction) as u128, exp_bits - 1023 - 52)
    };
    if significand == 0 {
        Some(NumericScalarKey::zero())
    } else {
        Some(NumericScalarKey::nonzero(sign, significand, exponent))
    }
}

pub(crate) fn numeric_key(value: &PropValue) -> Option<NumericScalarKey> {
    match value {
        PropValue::Int(value) => Some(numeric_key_from_i64(*value)),
        PropValue::UInt(value) => Some(numeric_key_from_u64(*value)),
        PropValue::Float(value) => numeric_key_from_f64(*value),
        _ => None,
    }
}

pub(crate) fn compare_numeric_keys(left: NumericScalarKey, right: NumericScalarKey) -> Ordering {
    match (left.sign, right.sign) {
        (NumericSign::Negative, NumericSign::Negative) => compare_magnitudes(right, left),
        (NumericSign::Zero, NumericSign::Zero) => Ordering::Equal,
        (NumericSign::Positive, NumericSign::Positive) => compare_magnitudes(left, right),
        (NumericSign::Negative, _) => Ordering::Less,
        (_, NumericSign::Negative) => Ordering::Greater,
        (NumericSign::Zero, NumericSign::Positive) => Ordering::Less,
        (NumericSign::Positive, NumericSign::Zero) => Ordering::Greater,
    }
}

pub(crate) fn compare_numeric_prop_values(left: &PropValue, right: &PropValue) -> Option<Ordering> {
    Some(compare_numeric_keys(
        numeric_key(left)?,
        numeric_key(right)?,
    ))
}

pub(crate) fn semantic_property_eq(left: &PropValue, right: &PropValue) -> bool {
    match (numeric_key(left), numeric_key(right)) {
        (Some(left), Some(right)) => left == right,
        _ => raw_structural_property_eq(left, right),
    }
}

pub(crate) fn semantic_equality_key_bytes(value: &PropValue) -> Vec<u8> {
    let mut bytes = Vec::new();
    if let Some(key) = numeric_key(value) {
        bytes.push(1);
        push_numeric_key_bytes(&mut bytes, key);
    } else {
        bytes.push(2);
        bytes.extend_from_slice(&raw_canonical_prop_value_bytes(value));
    }
    bytes
}

pub(crate) fn hash_prop_equality_key(value: &PropValue) -> u64 {
    hash_semantic_equality_key_bytes(&semantic_equality_key_bytes(value))
}

pub(crate) fn hash_semantic_equality_key_bytes(bytes: &[u8]) -> u64 {
    fnv1a(bytes)
}

pub(crate) fn structural_value_contains_float_zero(value: &PropValue) -> bool {
    match value {
        PropValue::Array(values) => values.iter().any(prop_value_contains_float_zero),
        PropValue::Map(values) => values.values().any(prop_value_contains_float_zero),
        _ => false,
    }
}

pub(crate) fn numeric_range_sort_key(key: NumericScalarKey) -> NumericRangeSortKey {
    let mut bytes = [0_u8; NUMERIC_RANGE_KEY_BYTES];
    match key.sign {
        NumericSign::Zero => {
            bytes[0] = 1;
        }
        NumericSign::Positive | NumericSign::Negative => {
            let rank = key.magnitude_rank();
            let fraction = key.normalized_fraction();
            let mut rank_bits = (rank as u32) ^ 0x8000_0000;
            let mut fraction_bits = fraction;
            if key.sign == NumericSign::Negative {
                bytes[0] = 0;
                rank_bits = !rank_bits;
                fraction_bits = !fraction_bits;
            } else {
                bytes[0] = 2;
            }
            bytes[1..5].copy_from_slice(&rank_bits.to_be_bytes());
            bytes[5..21].copy_from_slice(&fraction_bits.to_be_bytes());
        }
    }
    NumericRangeSortKey(bytes)
}

pub(crate) fn numeric_range_sort_key_for_value(value: &PropValue) -> Option<NumericRangeSortKey> {
    numeric_key(value).map(numeric_range_sort_key)
}

pub(crate) fn semantic_range_bound_key_bytes(bound: &PropertyRangeBound) -> Vec<u8> {
    let mut bytes = Vec::new();
    bytes.push(bound.is_inclusive() as u8);
    push_numeric_key_bytes(
        &mut bytes,
        numeric_key(bound.value()).expect("validated range bound must be numeric"),
    );
    bytes
}

pub(crate) fn validate_numeric_range_bounds(
    lower: Option<&PropertyRangeBound>,
    upper: Option<&PropertyRangeBound>,
    cursor: Option<&PropertyRangeCursor>,
) -> Result<ValidatedNumericRange, EngineError> {
    if lower.is_none() && upper.is_none() {
        return Err(EngineError::InvalidOperation(
            "range query requires at least one finite numeric bound".to_string(),
        ));
    }

    let lower = lower
        .map(|bound| validate_bound(bound, "lower"))
        .transpose()?;
    let upper = upper
        .map(|bound| validate_bound(bound, "upper"))
        .transpose()?;

    if let Some(cursor) = cursor {
        validate_numeric_range_value(&cursor.value, "cursor")?;
    }

    let is_empty = match (lower, upper) {
        (Some(lower), Some(upper)) => range_bounds_are_empty(lower, upper),
        _ => false,
    };

    Ok(ValidatedNumericRange {
        lower,
        upper,
        is_empty,
    })
}

pub(crate) fn numeric_range_value_within_bounds(
    value: &PropValue,
    lower: Option<&PropertyRangeBound>,
    upper: Option<&PropertyRangeBound>,
) -> Option<bool> {
    let value_key = numeric_key(value)?;
    if let Some(lower) = lower {
        let lower_key = numeric_key(lower.value())?;
        let ordering = compare_numeric_keys(value_key, lower_key);
        let in_lower = if lower.is_inclusive() {
            ordering != Ordering::Less
        } else {
            ordering == Ordering::Greater
        };
        if !in_lower {
            return Some(false);
        }
    }
    if let Some(upper) = upper {
        let upper_key = numeric_key(upper.value())?;
        let ordering = compare_numeric_keys(value_key, upper_key);
        let in_upper = if upper.is_inclusive() {
            ordering != Ordering::Greater
        } else {
            ordering == Ordering::Less
        };
        if !in_upper {
            return Some(false);
        }
    }
    Some(true)
}

pub(crate) fn numeric_key_within_validated_range(
    value: NumericScalarKey,
    range: &ValidatedNumericRange,
) -> bool {
    if range.is_empty {
        return false;
    }
    if let Some(lower) = range.lower {
        let ordering = compare_numeric_keys(value, lower.key);
        if if lower.inclusive {
            ordering == Ordering::Less
        } else {
            ordering != Ordering::Greater
        } {
            return false;
        }
    }
    if let Some(upper) = range.upper {
        let ordering = compare_numeric_keys(value, upper.key);
        if if upper.inclusive {
            ordering == Ordering::Greater
        } else {
            ordering != Ordering::Less
        } {
            return false;
        }
    }
    true
}

pub(crate) fn prop_value_within_validated_range(
    value: &PropValue,
    range: &ValidatedNumericRange,
) -> bool {
    numeric_key(value).is_some_and(|key| numeric_key_within_validated_range(key, range))
}

pub(crate) fn intersect_validated_numeric_ranges(
    left: &ValidatedNumericRange,
    right: &ValidatedNumericRange,
) -> ValidatedNumericRange {
    let lower = max_lower_bound(left.lower, right.lower);
    let upper = min_upper_bound(left.upper, right.upper);
    let is_empty = left.is_empty
        || right.is_empty
        || match (lower, upper) {
            (Some(lower), Some(upper)) => range_bounds_are_empty(lower, upper),
            _ => false,
        };
    ValidatedNumericRange {
        lower,
        upper,
        is_empty,
    }
}

fn compare_magnitudes(left: NumericScalarKey, right: NumericScalarKey) -> Ordering {
    debug_assert!(left.sign != NumericSign::Zero);
    debug_assert!(right.sign != NumericSign::Zero);
    match left.magnitude_rank().cmp(&right.magnitude_rank()) {
        Ordering::Equal => left.normalized_fraction().cmp(&right.normalized_fraction()),
        ordering => ordering,
    }
}

fn push_numeric_key_bytes(target: &mut Vec<u8>, key: NumericScalarKey) {
    target.push(match key.sign {
        NumericSign::Negative => 0,
        NumericSign::Zero => 1,
        NumericSign::Positive => 2,
    });
    target.extend_from_slice(&key.exponent.to_be_bytes());
    target.extend_from_slice(&key.significand.to_be_bytes());
}

fn raw_canonical_prop_value_bytes(value: &PropValue) -> Vec<u8> {
    rmp_serde::to_vec(value).expect("PropValue must be serializable")
}

fn raw_structural_property_eq(left: &PropValue, right: &PropValue) -> bool {
    match (left, right) {
        (PropValue::Null, PropValue::Null) => true,
        (PropValue::Bool(left), PropValue::Bool(right)) => left == right,
        (PropValue::Int(left), PropValue::Int(right)) => left == right,
        (PropValue::UInt(left), PropValue::UInt(right)) => left == right,
        (PropValue::Float(left), PropValue::Float(right)) => {
            !left.is_nan() && !right.is_nan() && left.to_bits() == right.to_bits()
        }
        (PropValue::String(left), PropValue::String(right)) => left == right,
        (PropValue::Bytes(left), PropValue::Bytes(right)) => left == right,
        (PropValue::Array(left), PropValue::Array(right)) => {
            left.len() == right.len()
                && left
                    .iter()
                    .zip(right)
                    .all(|(left, right)| raw_structural_property_eq(left, right))
        }
        (PropValue::Map(left), PropValue::Map(right)) => {
            left.len() == right.len()
                && left.iter().all(|(key, left_value)| {
                    right.get(key).is_some_and(|right_value| {
                        raw_structural_property_eq(left_value, right_value)
                    })
                })
        }
        _ => false,
    }
}

fn prop_value_contains_float_zero(value: &PropValue) -> bool {
    match value {
        PropValue::Float(value) => *value == 0.0,
        PropValue::Array(values) => values.iter().any(prop_value_contains_float_zero),
        PropValue::Map(values) => values.values().any(prop_value_contains_float_zero),
        _ => false,
    }
}

fn validate_bound(
    bound: &PropertyRangeBound,
    _name: &str,
) -> Result<NumericRangeBoundKey, EngineError> {
    let key = validate_numeric_range_value(bound.value(), "bound")?;
    Ok(NumericRangeBoundKey {
        key,
        inclusive: bound.is_inclusive(),
    })
}

fn validate_numeric_range_value(
    value: &PropValue,
    context: &str,
) -> Result<NumericScalarKey, EngineError> {
    match value {
        PropValue::Float(value) if !value.is_finite() => Err(EngineError::InvalidOperation(
            "non-finite float is not valid for numeric range bounds".to_string(),
        )),
        PropValue::Int(_) | PropValue::UInt(_) | PropValue::Float(_) => numeric_key(value)
            .ok_or_else(|| {
                EngineError::InvalidOperation(format!("{context} must be a finite numeric scalar"))
            }),
        _ => Err(EngineError::InvalidOperation(
            "range bound must be a finite numeric scalar".to_string(),
        )),
    }
}

fn range_bounds_are_empty(lower: NumericRangeBoundKey, upper: NumericRangeBoundKey) -> bool {
    match compare_numeric_keys(lower.key, upper.key) {
        Ordering::Greater => true,
        Ordering::Equal => !(lower.inclusive && upper.inclusive),
        Ordering::Less => false,
    }
}

fn max_lower_bound(
    left: Option<NumericRangeBoundKey>,
    right: Option<NumericRangeBoundKey>,
) -> Option<NumericRangeBoundKey> {
    match (left, right) {
        (Some(left), Some(right)) => match compare_numeric_keys(left.key, right.key) {
            Ordering::Less => Some(right),
            Ordering::Greater => Some(left),
            Ordering::Equal => Some(NumericRangeBoundKey {
                key: left.key,
                inclusive: left.inclusive && right.inclusive,
            }),
        },
        (Some(bound), None) | (None, Some(bound)) => Some(bound),
        (None, None) => None,
    }
}

fn min_upper_bound(
    left: Option<NumericRangeBoundKey>,
    right: Option<NumericRangeBoundKey>,
) -> Option<NumericRangeBoundKey> {
    match (left, right) {
        (Some(left), Some(right)) => match compare_numeric_keys(left.key, right.key) {
            Ordering::Less => Some(left),
            Ordering::Greater => Some(right),
            Ordering::Equal => Some(NumericRangeBoundKey {
                key: left.key,
                inclusive: left.inclusive && right.inclusive,
            }),
        },
        (Some(bound), None) | (None, Some(bound)) => Some(bound),
        (None, None) => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeMap;

    fn key(value: PropValue) -> NumericScalarKey {
        numeric_key(&value).unwrap()
    }

    fn cmp(left: PropValue, right: PropValue) -> Ordering {
        compare_numeric_prop_values(&left, &right).unwrap()
    }

    fn raw_nonzero_range_key_bytes(class: u8, rank: i32, fraction: u128) -> [u8; 24] {
        let mut bytes = [0_u8; NUMERIC_RANGE_KEY_BYTES];
        bytes[0] = class;
        let mut rank_bits = (rank as u32) ^ 0x8000_0000;
        let mut fraction_bits = fraction;
        if class == 0 {
            rank_bits = !rank_bits;
            fraction_bits = !fraction_bits;
        }
        bytes[1..5].copy_from_slice(&rank_bits.to_be_bytes());
        bytes[5..21].copy_from_slice(&fraction_bits.to_be_bytes());
        bytes
    }

    fn unchecked_range_key_bytes(sign: NumericSign, significand: u128, exponent: i32) -> [u8; 24] {
        numeric_range_sort_key(NumericScalarKey {
            sign,
            exponent,
            significand,
        })
        .as_bytes()
    }

    #[test]
    fn numeric_key_zero_equivalence() {
        let zero = key(PropValue::Int(0));
        assert_eq!(key(PropValue::UInt(0)), zero);
        assert_eq!(key(PropValue::Float(0.0)), zero);
        assert_eq!(key(PropValue::Float(-0.0)), zero);
        assert!(semantic_property_eq(
            &PropValue::Float(-0.0),
            &PropValue::UInt(0)
        ));
    }

    #[test]
    fn numeric_key_integer_float_exact_equivalence() {
        let one = key(PropValue::Int(1));
        assert_eq!(key(PropValue::UInt(1)), one);
        assert_eq!(key(PropValue::Float(1.0)), one);
        assert!(semantic_property_eq(
            &PropValue::Int(1),
            &PropValue::Float(1.0)
        ));
        assert!(!semantic_property_eq(
            &PropValue::Int(1),
            &PropValue::Float(1.5)
        ));
    }

    #[test]
    fn numeric_key_large_integer_not_lost_to_float() {
        let above_safe = 9_007_199_254_740_993_u64;
        assert_ne!(
            key(PropValue::UInt(above_safe)),
            key(PropValue::Float(9_007_199_254_740_992.0))
        );
        assert_eq!(
            cmp(
                PropValue::UInt(above_safe),
                PropValue::Float(9_007_199_254_740_992.0),
            ),
            Ordering::Greater
        );
        assert_ne!(
            key(PropValue::UInt(u64::MAX)),
            key(PropValue::Float(18_446_744_073_709_551_616.0))
        );
        assert_eq!(
            cmp(
                PropValue::UInt(u64::MAX),
                PropValue::Float(18_446_744_073_709_551_616.0),
            ),
            Ordering::Less
        );
    }

    #[test]
    fn exact_integer_to_float_rejects_rounded_values() {
        assert_eq!(
            exact_u64_to_f64(9_007_199_254_740_992, "test").unwrap(),
            9_007_199_254_740_992.0
        );
        assert!(exact_u64_to_f64(9_007_199_254_740_993, "test").is_err());
        assert_eq!(
            exact_i64_to_f64(i64::MIN, "test").unwrap(),
            -9_223_372_036_854_775_808.0
        );
        assert!(exact_i64_to_f64(i64::MAX, "test").is_err());
    }

    #[test]
    fn numeric_key_signed_unsigned_boundaries() {
        assert_eq!(
            key(PropValue::Int(i64::MIN)),
            key(PropValue::Float(-9_223_372_036_854_775_808.0))
        );
        assert_eq!(
            cmp(PropValue::Int(i64::MIN), PropValue::UInt(0)),
            Ordering::Less
        );
        assert_eq!(
            cmp(PropValue::UInt(u64::MAX), PropValue::Int(i64::MAX)),
            Ordering::Greater
        );
    }

    #[test]
    fn numeric_key_subnormal_float() {
        let subnormal = f64::MIN_POSITIVE / 2.0;
        assert!(subnormal.is_subnormal());
        assert_eq!(
            key(PropValue::Float(subnormal)),
            key(PropValue::Float(subnormal))
        );
        assert_eq!(
            cmp(PropValue::Float(subnormal), PropValue::Int(0)),
            Ordering::Greater
        );
        assert_eq!(
            cmp(PropValue::Float(-subnormal), PropValue::Int(0)),
            Ordering::Less
        );
    }

    #[test]
    fn numeric_key_nonfinite_none() {
        assert!(numeric_key(&PropValue::Float(f64::NAN)).is_none());
        assert!(numeric_key(&PropValue::Float(f64::INFINITY)).is_none());
        assert!(numeric_key(&PropValue::Float(f64::NEG_INFINITY)).is_none());
        assert!(!semantic_property_eq(
            &PropValue::Float(f64::NAN),
            &PropValue::Float(f64::NAN),
        ));
        assert!(semantic_property_eq(
            &PropValue::Float(f64::INFINITY),
            &PropValue::Float(f64::INFINITY),
        ));
    }

    #[test]
    fn numeric_compare_cross_domain_matrix() {
        let ordered = [
            PropValue::Int(i64::MIN),
            PropValue::Int(-3),
            PropValue::Float(-2.5),
            PropValue::Float(-0.0),
            PropValue::UInt(0),
            PropValue::Float(f64::MIN_POSITIVE / 2.0),
            PropValue::Float(1.25),
            PropValue::Float(1.5),
            PropValue::Int(2),
            PropValue::UInt(3),
            PropValue::UInt(u64::MAX),
        ];
        for window in ordered.windows(2) {
            assert_ne!(cmp(window[0].clone(), window[1].clone()), Ordering::Greater);
        }
        assert_eq!(
            cmp(PropValue::Int(2), PropValue::Float(2.0)),
            Ordering::Equal
        );
        assert_eq!(
            cmp(PropValue::Float(-3.0), PropValue::Int(-2)),
            Ordering::Less
        );
    }

    #[test]
    fn numeric_range_sort_key_matches_comparator() {
        let values = vec![
            PropValue::Int(i64::MIN),
            PropValue::Float(-9_223_372_036_854_775_808.0),
            PropValue::Int(-3),
            PropValue::Float(-2.5),
            PropValue::Float(-f64::MIN_POSITIVE),
            PropValue::Float(-(f64::MIN_POSITIVE / 2.0)),
            PropValue::Float(-0.0),
            PropValue::Int(0),
            PropValue::Float(f64::MIN_POSITIVE / 2.0),
            PropValue::Float(f64::MIN_POSITIVE),
            PropValue::Float(1.25),
            PropValue::Float(1.5),
            PropValue::Float(2.5),
            PropValue::Int(3),
            PropValue::UInt((1_u64 << 53) - 1),
            PropValue::UInt(1_u64 << 53),
            PropValue::UInt((1_u64 << 53) + 1),
            PropValue::UInt(u64::MAX),
        ];
        for left in &values {
            for right in &values {
                let ordering = compare_numeric_prop_values(left, right).unwrap();
                let left_key = numeric_range_sort_key_for_value(left).unwrap();
                let right_key = numeric_range_sort_key_for_value(right).unwrap();
                assert_eq!(
                    left_key.cmp(&right_key),
                    ordering,
                    "sort-key mismatch for {left:?} and {right:?}",
                );
            }
        }
    }

    #[test]
    fn numeric_range_sidecar_key_validation_accepts_emitted_boundaries() {
        let values = vec![
            PropValue::Int(i64::MIN),
            PropValue::Int(i64::MAX),
            PropValue::UInt(u64::MAX),
            PropValue::UInt((1_u64 << 53) + 1),
            PropValue::Float(f64::from_bits(1)),
            PropValue::Float(-f64::from_bits(1)),
            PropValue::Float(f64::MIN_POSITIVE),
            PropValue::Float(-f64::MIN_POSITIVE),
            PropValue::Float(f64::MAX),
            PropValue::Float(-f64::MAX),
            PropValue::Float(2.0_f64.powi(80)),
            PropValue::Float(-2.0_f64.powi(80)),
            PropValue::Float(0.5),
            PropValue::Float(-0.5),
        ];

        for value in values {
            let bytes = numeric_range_sort_key_for_value(&value).unwrap().as_bytes();
            validate_numeric_range_sidecar_key(&bytes)
                .unwrap_or_else(|error| panic!("rejected emitted key for {value:?}: {error:?}"));
            NumericRangeSortKey::from_sidecar_bytes(bytes).unwrap_or_else(|error| {
                panic!("from_sidecar_bytes rejected emitted key for {value:?}: {error:?}")
            });
        }
    }

    #[test]
    fn numeric_range_sidecar_key_validation_rejects_impossible_payloads() {
        let zero_positive_fraction = raw_nonzero_range_key_bytes(2, 0, 0);
        assert!(validate_numeric_range_sidecar_key(&zero_positive_fraction).is_err());

        let zero_negative_fraction = raw_nonzero_range_key_bytes(0, 0, 0);
        assert!(validate_numeric_range_sidecar_key(&zero_negative_fraction).is_err());

        let missing_top_bit = raw_nonzero_range_key_bytes(2, 0, 1_u128 << 126);
        assert!(validate_numeric_range_sidecar_key(&missing_top_bit).is_err());

        let too_precise_fractional =
            unchecked_range_key_bytes(NumericSign::Positive, (1_u128 << 53) | 1, -1);
        assert!(validate_numeric_range_sidecar_key(&too_precise_fractional).is_err());

        let positive_above_u64_not_f64 =
            unchecked_range_key_bytes(NumericSign::Positive, u64::MAX as u128, 1);
        assert!(validate_numeric_range_sidecar_key(&positive_above_u64_not_f64).is_err());

        let negative_below_i64_not_f64 =
            unchecked_range_key_bytes(NumericSign::Negative, (1_u128 << 63) | 1, 0);
        assert!(validate_numeric_range_sidecar_key(&negative_below_i64_not_f64).is_err());

        assert!(NumericRangeSortKey::from_sidecar_bytes(too_precise_fractional).is_err());
    }

    #[test]
    fn semantic_equality_key_raw_values_do_not_normalize_arrays_maps() {
        assert_ne!(
            semantic_equality_key_bytes(&PropValue::Array(vec![PropValue::Int(1)])),
            semantic_equality_key_bytes(&PropValue::Array(vec![PropValue::Float(1.0)])),
        );
        assert!(!semantic_property_eq(
            &PropValue::Array(vec![PropValue::Float(-0.0)]),
            &PropValue::Array(vec![PropValue::Float(0.0)]),
        ));
        assert!(!semantic_property_eq(
            &PropValue::Array(vec![PropValue::Float(f64::NAN)]),
            &PropValue::Array(vec![PropValue::Float(f64::NAN)]),
        ));
        let mut int_map = BTreeMap::new();
        int_map.insert("x".to_string(), PropValue::Int(1));
        let mut float_map = BTreeMap::new();
        float_map.insert("x".to_string(), PropValue::Float(1.0));
        assert_ne!(
            semantic_equality_key_bytes(&PropValue::Map(int_map)),
            semantic_equality_key_bytes(&PropValue::Map(float_map)),
        );
        let mut neg_zero_map = BTreeMap::new();
        neg_zero_map.insert("x".to_string(), PropValue::Float(-0.0));
        let mut pos_zero_map = BTreeMap::new();
        pos_zero_map.insert("x".to_string(), PropValue::Float(0.0));
        assert!(!semantic_property_eq(
            &PropValue::Map(neg_zero_map),
            &PropValue::Map(pos_zero_map),
        ));
    }

    #[test]
    fn structural_zero_detection_is_limited_to_nested_float_zero() {
        assert!(!structural_value_contains_float_zero(&PropValue::Float(
            -0.0
        )));
        assert!(!structural_value_contains_float_zero(&PropValue::Array(
            vec![PropValue::Int(0)]
        )));
        assert!(structural_value_contains_float_zero(&PropValue::Array(
            vec![PropValue::Float(-0.0)]
        )));
        let mut values = BTreeMap::new();
        values.insert("x".to_string(), PropValue::Float(0.0));
        assert!(structural_value_contains_float_zero(&PropValue::Map(
            values
        )));
    }

    #[test]
    fn semantic_equality_hash_uses_exact_numeric_key() {
        let one_hash = hash_prop_equality_key(&PropValue::Int(1));
        assert_eq!(one_hash, hash_prop_equality_key(&PropValue::UInt(1)));
        assert_eq!(one_hash, hash_prop_equality_key(&PropValue::Float(1.0)));

        let zero_hash = hash_prop_equality_key(&PropValue::Int(0));
        assert_eq!(zero_hash, hash_prop_equality_key(&PropValue::UInt(0)));
        assert_eq!(zero_hash, hash_prop_equality_key(&PropValue::Float(0.0)));
        assert_eq!(zero_hash, hash_prop_equality_key(&PropValue::Float(-0.0)));

        assert_ne!(
            one_hash,
            hash_prop_equality_key(&PropValue::String("1".to_string()))
        );
        assert_ne!(
            one_hash,
            hash_prop_equality_key(&PropValue::Array(vec![PropValue::Int(1)]))
        );
    }

    #[test]
    fn semantic_in_dedupes_numeric_values() {
        let mut values = vec![
            semantic_equality_key_bytes(&PropValue::Int(1)),
            semantic_equality_key_bytes(&PropValue::UInt(1)),
            semantic_equality_key_bytes(&PropValue::Float(1.0)),
            semantic_equality_key_bytes(&PropValue::Float(1.5)),
        ];
        values.sort();
        values.dedup();
        assert_eq!(values.len(), 2);
    }

    #[test]
    fn nonfinite_and_nonnumeric_range_bounds_are_invalid() {
        assert!(validate_numeric_range_bounds(
            Some(&PropertyRangeBound::Included(PropValue::Float(f64::NAN))),
            None,
            None,
        )
        .unwrap_err()
        .to_string()
        .contains("non-finite float is not valid for numeric range bounds"));
        assert!(validate_numeric_range_bounds(
            Some(&PropertyRangeBound::Included(PropValue::String(
                "1".to_string()
            ))),
            None,
            None,
        )
        .unwrap_err()
        .to_string()
        .contains("range bound must be a finite numeric scalar"));
    }

    #[test]
    fn empty_finite_ranges_validate_as_empty() {
        let range = validate_numeric_range_bounds(
            Some(&PropertyRangeBound::Excluded(PropValue::Int(2))),
            Some(&PropertyRangeBound::Included(PropValue::Float(2.0))),
            None,
        )
        .unwrap();
        assert!(range.is_empty);
    }
}
