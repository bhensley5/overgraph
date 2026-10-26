//! Checked addressability helpers for untrusted wire extents.
use std::alloc::Layout;
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Measure {
    Bytes,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum BoundError {
    Overflow(Measure),
    AllocationTooLarge { requested: usize, limit: usize },
}
pub(crate) fn add(a: usize, b: usize) -> Result<usize, BoundError> {
    a.checked_add(b).ok_or(BoundError::Overflow(Measure::Bytes))
}
pub(crate) fn mul(a: usize, b: usize) -> Result<usize, BoundError> {
    a.checked_mul(b).ok_or(BoundError::Overflow(Measure::Bytes))
}
pub(crate) fn array_bytes<T>(capacity: usize) -> Result<usize, BoundError> {
    let bytes = mul(capacity, std::mem::size_of::<T>())?;
    Layout::array::<T>(capacity)
        .map(|layout| layout.size())
        .map_err(|_| BoundError::AllocationTooLarge {
            requested: bytes,
            limit: isize::MAX as usize - (std::mem::align_of::<T>() - 1),
        })
}
