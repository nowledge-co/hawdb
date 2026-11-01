// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

use super::*;

/// Bound graph work for one MATCH clause; its predicate belongs inside the optional boundary.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GraphMatchProgram {
    pub imports: Vec<GraphBindingImport>,
    pub introduced: Vec<String>,
    /// Node variables bound by this program (introduced or imported) that
    /// every later reader uses only through these properties, its identity
    /// and its labels. The executor reads just those parts of their records;
    /// a variable that is absent is read in full. Lowering fills it once the
    /// whole plan is known; the binder leaves it empty.
    pub projected_nodes: BTreeMap<String, BTreeSet<String>>,
    pub steps: Vec<GraphMatchStep>,
    pub predicate: Option<Predicate>,
    /// Where each top-level conjunct of `predicate` is evaluated: entry `i`
    /// is the number of steps completed before conjunct `i` (a child of
    /// `Predicate::And`, or the whole predicate otherwise) runs, at most
    /// `steps.len()`. Empty evaluates the whole predicate after the last step.
    /// Lowering fills it from the final step order; the binder leaves it empty.
    pub predicate_steps: Vec<usize>,
    pub optional: bool,
}

impl GraphMatchProgram {
    /// The top-level conjuncts that `predicate_steps` places, in order.
    pub fn predicate_conjuncts(&self) -> &[Predicate] {
        match &self.predicate {
            None => &[],
            Some(Predicate::And(conjuncts)) => conjuncts,
            Some(predicate) => std::slice::from_ref(predicate),
        }
    }
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
    /// Binds `path` to the shortest path(s) between two already bound nodes.
    ShortestPath(Box<GraphMatchShortestPath>),
}

/// Upper bound for unbounded shortest-path patterns such as `[:KNOWS*]`, and
/// for explicit maximum hop counts. It matches the bounded-expansion
/// recursion limit. An unbounded search that would need more hops fails
/// instead of silently reporting that no path exists.
pub const MAX_SHORTEST_PATH_HOPS: usize = 256;

/// Path selection for a shortest-path step.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ShortestPathSelection {
    /// `shortestPath`: one shortest path per endpoint pair.
    Any,
    /// `allShortestPaths`: every shortest path per endpoint pair.
    All,
}

/// `p = shortestPath((source)-[:T*min..max]-(target))` over bound endpoints.
///
/// `min_hops` is 0 or 1. With 0, equal endpoints produce a zero-length path;
/// with 1 they produce no path. `max_hops` never exceeds
/// [`MAX_SHORTEST_PATH_HOPS`]; `bounded` is false when the pattern omitted the
/// upper bound, in which case exceeding the cap is an execution error.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GraphMatchShortestPath {
    pub path: String,
    pub source: String,
    pub target: String,
    pub rel_type: String,
    pub direction: RelationshipDirection,
    pub min_hops: usize,
    pub max_hops: usize,
    pub bounded: bool,
    pub selection: ShortestPathSelection,
}

// Retained query ownership: GraphMatchProgram.
hawdb_core::impl_retained_heap_for_struct!(GraphMatchProgram {
    imports,
    introduced,
    projected_nodes,
    steps,
    predicate,
    predicate_steps,
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
            Self::ShortestPath(path) => hawdb_core::RetainedHeapBytes::retained_heap_bytes(path),
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

// Retained query ownership: GraphMatchShortestPath.
hawdb_core::impl_retained_heap_for_struct!(GraphMatchShortestPath {
    path,
    source,
    target,
    rel_type,
    direction,
    min_hops,
    max_hops,
    bounded,
    selection
});

// Retained query ownership: ShortestPathSelection.
hawdb_core::impl_retained_heap_for_copy!(ShortestPathSelection);
