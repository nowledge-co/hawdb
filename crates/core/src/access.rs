// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

//! Language-neutral graph index access requests shared by planning and storage.

use crate::Value;

/// One equality-index branch in a bounded exact-property union seek.
#[derive(Debug, Clone, PartialEq)]
pub struct ExactPropertySeekBranch {
    pub property: String,
    pub values: Vec<Value>,
}

/// Leading equalities followed by one bounded range on an ordered composite index.
#[derive(Debug, Clone, PartialEq)]
pub struct CompositeRangeSeek {
    pub index_properties: Vec<String>,
    pub equality_prefix: Vec<(String, Value)>,
    pub range_property: String,
    pub lower: Option<(Value, bool)>,
    pub upper: Option<(Value, bool)>,
}

/// Ordered traversal of complete non-NULL keys, after an exclusive suffix tuple.
/// Canonical-query planning must prove complete key coverage and comparator compatibility.
#[derive(Debug, Clone, PartialEq)]
pub struct CompositeOrderedSeek {
    pub index_properties: Vec<String>,
    pub equality_prefix: Vec<(String, Value)>,
    pub after: Option<Vec<Value>>,
    pub descending: bool,
}

/// Candidate source used by a fused scalar node projection.
///
/// Keeping the access path beside the required-property set lets storage
/// decode only fields needed by the residual predicate and output projection,
/// without giving up an index seek selected earlier by the optimizer.
#[derive(Debug, Clone, PartialEq)]
pub enum NodeProjectionAccess {
    LabelScan,
    PropertyValues {
        property: String,
        values: Vec<Value>,
    },
    PropertyUnion {
        branches: Vec<ExactPropertySeekBranch>,
    },
    CompositeEquality {
        predicates: Vec<(String, Value)>,
    },
    CompositeRange {
        seek: CompositeRangeSeek,
    },
    CompositeOrdered {
        seek: CompositeOrderedSeek,
    },
    PropertyRange {
        property: String,
        lower: Option<(Value, bool)>,
        upper: Option<(Value, bool)>,
    },
    FullText {
        property: String,
        query: String,
    },
}

impl NodeProjectionAccess {
    pub fn is_label_scan(&self) -> bool {
        matches!(self, Self::LabelScan)
    }

    pub fn kind_name(&self) -> &'static str {
        match self {
            Self::LabelScan => "label_scan",
            Self::PropertyValues { .. } => "property_values",
            Self::PropertyUnion { .. } => "property_union",
            Self::CompositeEquality { .. } => "composite_equality",
            Self::CompositeRange { .. } => "composite_range",
            Self::CompositeOrdered { .. } => "composite_ordered",
            Self::PropertyRange { .. } => "property_range",
            Self::FullText { .. } => "full_text",
        }
    }

    /// Returns the access operator represented inside the fused projection.
    pub fn physical_operator_name(&self) -> &'static str {
        match self {
            Self::LabelScan => "SeqNodeScan",
            Self::PropertyValues { values, .. } if values.len() == 1 => "IndexNodeSeek",
            Self::PropertyValues { .. } => "IndexNodeMultiSeek",
            Self::PropertyUnion { .. } => "IndexNodeUnionSeek",
            Self::CompositeEquality { .. } => "IndexNodeCompositeSeek",
            Self::CompositeRange { .. } => "IndexNodeCompositeRangeSeek",
            Self::CompositeOrdered { .. } => "IndexNodeCompositeOrderedSeek",
            Self::PropertyRange { .. } => "IndexNodeRangeSeek",
            Self::FullText { .. } => "IndexNodeTextSeek",
        }
    }
}

// Retained query ownership: ExactPropertySeekBranch.
crate::impl_retained_heap_for_struct!(ExactPropertySeekBranch { property, values });

// Retained query ownership: CompositeRangeSeek.
crate::impl_retained_heap_for_struct!(CompositeRangeSeek {
    index_properties,
    equality_prefix,
    range_property,
    lower,
    upper
});

// Retained query ownership: CompositeOrderedSeek.
crate::impl_retained_heap_for_struct!(CompositeOrderedSeek {
    index_properties,
    equality_prefix,
    after,
    descending
});

// Retained query ownership: NodeProjectionAccess.
impl crate::RetainedHeapBytes for NodeProjectionAccess {
    fn retained_heap_bytes(&self) -> Option<usize> {
        match self {
            Self::LabelScan => crate::checked_retained_heap_sum([]),
            Self::PropertyValues { property, values } => crate::checked_retained_heap_sum([
                crate::RetainedHeapBytes::retained_heap_bytes(property),
                crate::RetainedHeapBytes::retained_heap_bytes(values),
            ]),
            Self::PropertyUnion { branches } => {
                crate::checked_retained_heap_sum([crate::RetainedHeapBytes::retained_heap_bytes(
                    branches,
                )])
            }
            Self::CompositeEquality { predicates } => {
                crate::checked_retained_heap_sum([crate::RetainedHeapBytes::retained_heap_bytes(
                    predicates,
                )])
            }
            Self::CompositeRange { seek } => {
                crate::checked_retained_heap_sum([crate::RetainedHeapBytes::retained_heap_bytes(
                    seek,
                )])
            }
            Self::CompositeOrdered { seek } => {
                crate::checked_retained_heap_sum([crate::RetainedHeapBytes::retained_heap_bytes(
                    seek,
                )])
            }
            Self::PropertyRange {
                property,
                lower,
                upper,
            } => crate::checked_retained_heap_sum([
                crate::RetainedHeapBytes::retained_heap_bytes(property),
                crate::RetainedHeapBytes::retained_heap_bytes(lower),
                crate::RetainedHeapBytes::retained_heap_bytes(upper),
            ]),
            Self::FullText { property, query } => crate::checked_retained_heap_sum([
                crate::RetainedHeapBytes::retained_heap_bytes(property),
                crate::RetainedHeapBytes::retained_heap_bytes(query),
            ]),
        }
    }
}
