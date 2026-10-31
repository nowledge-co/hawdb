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
