// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

//! Requested-heap bounds for immutable retained query data. Inline storage is
//! charged by its owner. Shared allocations are charged in full per reference.
//! Allocator rounding and request-owned clones are outside this bound. Pinned
//! standard-library layouts require an independent allocation oracle.

use crate::{Value, Vector};
use std::collections::{BTreeMap, BTreeSet};
use std::mem::{align_of, align_of_val, size_of, size_of_val};
use std::sync::Arc;

/// Reachable owned requested heap, excluding the value's inline layout.
/// Return `None` on overflow or when retained ownership/interior growth cannot
/// be bounded. Spare container capacity must be included.
#[doc(hidden)]
pub trait RetainedHeapBytes {
    fn retained_heap_bytes(&self) -> Option<usize>;
}

#[doc(hidden)]
pub fn checked_retained_heap_sum(values: impl IntoIterator<Item = Option<usize>>) -> Option<usize> {
    values
        .into_iter()
        .try_fold(0usize, |total, value| total.checked_add(value?))
}

/// Heap-free leaves must remain Copy when their definition changes.
#[doc(hidden)]
#[macro_export]
macro_rules! impl_retained_heap_for_copy {
    ($($ty:ty),+ $(,)?) => {$(
        impl $crate::RetainedHeapBytes for $ty {
            fn retained_heap_bytes(&self) -> Option<usize> {
                fn require_copy<T: Copy>() {}
                require_copy::<Self>();
                Some(0)
            }
        }
    )+};
}

/// Exhaustive fields make new retained fields require an accounting update.
#[doc(hidden)]
#[macro_export]
macro_rules! impl_retained_heap_for_struct {
    ($ty:ty { $($field:ident),* $(,)? }) => {
        impl $crate::RetainedHeapBytes for $ty {
            fn retained_heap_bytes(&self) -> Option<usize> {
                let Self { $($field),* } = self;
                $crate::checked_retained_heap_sum([
                    $($crate::RetainedHeapBytes::retained_heap_bytes($field)),*
                ])
            }
        }
    };
}

impl_retained_heap_for_copy!(
    (),
    bool,
    char,
    u8,
    u16,
    u32,
    u64,
    u128,
    usize,
    i8,
    i16,
    i32,
    i64,
    i128,
    isize,
    f32,
    f64,
    std::num::NonZeroUsize,
    std::num::NonZeroU64,
);

impl_retained_heap_for_copy!(
    crate::LabelId,
    crate::TableId,
    crate::PropertyId,
    crate::PropertyType,
    crate::TableKind,
    crate::schema::ConstraintSubject,
    crate::VectorDimension,
    crate::ListFunction,
    crate::temporal::NaiveDate,
    crate::temporal::NaiveTime,
    crate::temporal::NaiveDateTime,
    crate::temporal::DateTime<crate::temporal::FixedOffset>,
    crate::temporal::Duration,
    crate::temporal::TemporalFunction,
    crate::ConversionFunction,
);

impl RetainedHeapBytes for String {
    fn retained_heap_bytes(&self) -> Option<usize> {
        Some(self.capacity())
    }
}

impl RetainedHeapBytes for str {
    fn retained_heap_bytes(&self) -> Option<usize> {
        Some(0)
    }
}

impl<T: RetainedHeapBytes> RetainedHeapBytes for Option<T> {
    fn retained_heap_bytes(&self) -> Option<usize> {
        self.as_ref().map_or(Some(0), T::retained_heap_bytes)
    }
}

impl<T: RetainedHeapBytes> RetainedHeapBytes for [T] {
    fn retained_heap_bytes(&self) -> Option<usize> {
        checked_retained_heap_sum(self.iter().map(T::retained_heap_bytes))
    }
}

impl<T: RetainedHeapBytes, const N: usize> RetainedHeapBytes for [T; N] {
    fn retained_heap_bytes(&self) -> Option<usize> {
        self.as_slice().retained_heap_bytes()
    }
}

impl<T: RetainedHeapBytes> RetainedHeapBytes for Vec<T> {
    fn retained_heap_bytes(&self) -> Option<usize> {
        self.capacity()
            .checked_mul(size_of::<T>())?
            .checked_add(self.as_slice().retained_heap_bytes()?)
    }
}

impl<T: RetainedHeapBytes + ?Sized> RetainedHeapBytes for Box<T> {
    fn retained_heap_bytes(&self) -> Option<usize> {
        size_of_val(self.as_ref()).checked_add(self.as_ref().retained_heap_bytes()?)
    }
}

impl<T: RetainedHeapBytes + ?Sized> RetainedHeapBytes for Arc<T> {
    fn retained_heap_bytes(&self) -> Option<usize> {
        checked_retained_heap_sum([
            Some(2 * size_of::<usize>()),
            align_of_val(self.as_ref()).checked_mul(2),
            Some(size_of_val(self.as_ref())),
            self.as_ref().retained_heap_bytes(),
        ])
    }
}

fn tree_storage<K, V>(entries: usize) -> Option<usize> {
    // Pinned std: eleven slots, twelve child pointers, >=5 entries/non-root
    // node. Include a possibly allocated empty root, bound every node as
    // internal, and include alignment padding.
    let nodes = 1usize.checked_add(entries.saturating_sub(1) / 5)?;
    let node = 11usize
        .checked_mul(size_of::<(K, V)>())?
        .checked_add(14usize.checked_mul(size_of::<usize>())?)?
        .checked_add(align_of::<(K, V)>())?;
    nodes.checked_mul(node)
}

impl<K: RetainedHeapBytes, V: RetainedHeapBytes> RetainedHeapBytes for BTreeMap<K, V> {
    fn retained_heap_bytes(&self) -> Option<usize> {
        self.iter()
            .try_fold(tree_storage::<K, V>(self.len())?, |total, (key, value)| {
                total
                    .checked_add(key.retained_heap_bytes()?)?
                    .checked_add(value.retained_heap_bytes()?)
            })
    }
}

impl<T: RetainedHeapBytes> RetainedHeapBytes for BTreeSet<T> {
    fn retained_heap_bytes(&self) -> Option<usize> {
        self.iter()
            .try_fold(tree_storage::<T, ()>(self.len())?, |total, value| {
                total.checked_add(value.retained_heap_bytes()?)
            })
    }
}

impl<A: RetainedHeapBytes, B: RetainedHeapBytes> RetainedHeapBytes for (A, B) {
    fn retained_heap_bytes(&self) -> Option<usize> {
        self.0
            .retained_heap_bytes()?
            .checked_add(self.1.retained_heap_bytes()?)
    }
}

impl<A: RetainedHeapBytes, B: RetainedHeapBytes, C: RetainedHeapBytes> RetainedHeapBytes
    for (A, B, C)
{
    fn retained_heap_bytes(&self) -> Option<usize> {
        checked_retained_heap_sum([
            self.0.retained_heap_bytes(),
            self.1.retained_heap_bytes(),
            self.2.retained_heap_bytes(),
        ])
    }
}

impl RetainedHeapBytes for Vector {
    fn retained_heap_bytes(&self) -> Option<usize> {
        Some(self.retained_capacity())
    }
}

impl RetainedHeapBytes for Value {
    fn retained_heap_bytes(&self) -> Option<usize> {
        match self {
            Self::String(value) => value.retained_heap_bytes(),
            Self::Binary(value) => Some(value.capacity()),
            Self::Vector(value) => value.retained_heap_bytes(),
            Self::List(value) => value.retained_heap_bytes(),
            Self::Map(value) => value.retained_heap_bytes(),
            Self::Date(value) => value.retained_heap_bytes(),
            Self::LocalTime(value) => value.retained_heap_bytes(),
            Self::LocalDateTime(value) => value.retained_heap_bytes(),
            Self::DateTime(value) => value.retained_heap_bytes(),
            Self::Duration(value) => value.retained_heap_bytes(),
            Self::Null | Self::Bool(_) | Self::Int(_) | Self::Float(_) | Self::Uuid(_) => Some(0),
        }
    }
}
