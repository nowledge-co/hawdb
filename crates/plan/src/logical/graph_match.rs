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
