// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

use super::*;

/// Bound graph work for one MATCH clause; its predicate belongs inside the optional boundary.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GraphMatchProgram {
    pub imports: Vec<GraphBindingImport>,
    pub introduced: Vec<String>,
    pub steps: Vec<GraphMatchStep>,
    pub predicate: Option<Predicate>,
    pub optional: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GraphBindingImport {
    pub variable: String,
    pub column: String,
    pub kind: GraphEntityKind,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GraphEntityKind {
    Node,
    Relationship,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GraphMatchNode {
    pub variable: String,
    pub label: String,
    pub properties: BTreeMap<String, Value>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum GraphMatchStep {
    Node(GraphMatchNode),
    Expand {
        source: String,
        relationship: Option<String>,
        rel_type: String,
        properties: BTreeMap<String, Value>,
        direction: RelationshipDirection,
        min_hops: usize,
        max_hops: usize,
        target: GraphMatchNode,
    },
}

// Retained query ownership: GraphMatchProgram.
hawdb_core::impl_retained_heap_for_struct!(GraphMatchProgram {
    imports,
    introduced,
    steps,
    predicate,
    optional
});

// Retained query ownership: GraphMatchNode.
hawdb_core::impl_retained_heap_for_struct!(GraphMatchNode {
    variable,
    label,
    properties
});

// Retained query ownership: GraphBindingImport.
hawdb_core::impl_retained_heap_for_struct!(GraphBindingImport {
    variable,
    column,
    kind
});

// Retained query ownership: GraphEntityKind.
hawdb_core::impl_retained_heap_for_copy!(GraphEntityKind);

// Retained query ownership: GraphMatchStep.
impl hawdb_core::RetainedHeapBytes for GraphMatchStep {
    fn retained_heap_bytes(&self) -> Option<usize> {
        match self {
            Self::Node(value0) => hawdb_core::checked_retained_heap_sum([
                hawdb_core::RetainedHeapBytes::retained_heap_bytes(value0),
            ]),
            Self::Expand {
                source,
                relationship,
                rel_type,
                properties,
                direction,
                min_hops,
                max_hops,
                target,
            } => hawdb_core::checked_retained_heap_sum([
                hawdb_core::RetainedHeapBytes::retained_heap_bytes(source),
                hawdb_core::RetainedHeapBytes::retained_heap_bytes(relationship),
                hawdb_core::RetainedHeapBytes::retained_heap_bytes(rel_type),
                hawdb_core::RetainedHeapBytes::retained_heap_bytes(properties),
                hawdb_core::RetainedHeapBytes::retained_heap_bytes(direction),
                hawdb_core::RetainedHeapBytes::retained_heap_bytes(min_hops),
                hawdb_core::RetainedHeapBytes::retained_heap_bytes(max_hops),
                hawdb_core::RetainedHeapBytes::retained_heap_bytes(target),
            ]),
        }
    }
}
