use super::*;

pub type QueryPipeline = AstNode<QueryPipelineKind>;
pub type Clause = AstNode<ClauseKind>;
pub type MatchPattern = AstNode<MatchPatternKind>;
pub type NodePattern = AstNode<NodePatternKind>;
pub type RelationshipPattern = AstNode<RelationshipPatternKind>;
pub type PatternStep = AstNode<PatternStepKind>;
pub type PredicateExpression = AstNode<PropertyPredicate>;
pub type ProcedureCall = AstNode<ProcedureCallKind>;
pub type YieldItem = AstNode<YieldItemKind>;

/// Clauses retain their lexical order; binding determines their scopes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QueryPipelineKind {
    pub clauses: Vec<Clause>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ClauseKind {
    Unwind {
        source: ValueExpression,
        variable: String,
    },
    Match {
        optional: bool,
        patterns: Vec<MatchPattern>,
        predicate: Option<PredicateExpression>,
    },
    With(ProjectionClause),
    Return(ProjectionClause),
    Call {
        procedure: ProcedureCall,
        yields: Vec<YieldItem>,
    },
    Create(Vec<MatchPattern>),
    Merge {
        pattern: MatchPattern,
        on_create: Vec<SetProperty>,
        on_match: Vec<SetProperty>,
    },
    Set(Vec<SetProperty>),
    Delete {
        detach: bool,
        variables: Vec<String>,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ProcedureCallKind {
    VectorSearch(VectorSearch),
    GraphAlgorithm {
        algorithm: GraphAlgorithmKind,
        graph_name: String,
        options: GraphAlgorithmOptions,
    },
    ProjectGraph {
        name: String,
        node_labels: Vec<String>,
        rel_types: Vec<String>,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct YieldItemKind {
    pub name: String,
    pub alias: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProjectionClause {
    pub distinct: bool,
    pub items: Vec<ReturnItem>,
    pub predicate: Option<PredicateExpression>,
    pub order_by: Vec<OrderItem>,
    pub offset: Option<ValueExpression>,
    pub limit: Option<ValueExpression>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MatchPatternKind {
    pub variable: Option<String>,
    pub first: NodePattern,
    pub steps: Vec<PatternStep>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PatternStepKind {
    pub relationship: RelationshipPattern,
    pub target: NodePattern,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NodePatternKind {
    pub variable: String,
    pub anonymous: bool,
    pub label: String,
    pub properties: BTreeMap<String, ValueExpression>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RelationshipPatternKind {
    pub variable: Option<String>,
    pub rel_type: String,
    pub properties: BTreeMap<String, ValueExpression>,
    pub direction: RelationshipDirection,
    pub min_hops: usize,
    pub max_hops: usize,
    pub search: PathSearch,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PathSearch {
    All,
    AllShortest,
}
