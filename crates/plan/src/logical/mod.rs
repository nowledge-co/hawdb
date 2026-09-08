use skein_core::{Result, SkeinError, ValidatedRegex, Value};
use skein_cypher::{
    ComparisonOp as CypherComparisonOp, GraphAlgorithmKind as CypherGraphAlgorithmKind,
    GraphAlgorithmOptions as CypherGraphAlgorithmOptions, MatchReturn,
    OrderDirection as CypherOrderDirection, OrderExpression, OrderItem, PostWithNodeLookup,
    PropertyPredicate, RelationshipDirection, RelationshipExpand as CypherRelationshipExpand,
    ReturnExpression, ReturnItem, ReturnValueExpression, SetProperty, SetValueExpression,
    ShortestPathReturn, ShortestPathReturnExpression, Statement, ValueExpression,
    VectorSearch as CypherVectorSearch, WithAggregateProjection, WithAliasFilter,
    WithAliasFilterExpression, WithAliasFilterOp, WithCollect, WithDistinctProjection,
    WithProjection,
};
pub use skein_ddl::{SchemaObjectState, SchemaPropertyType, SchemaTableKind};
pub use skein_expression::{
    CaseColumnSearchRankProjection, CaseEntitySearchRankProjection,
    CoalesceDifferenceProjectionTerm, ComparisonOp, DatePart, Predicate, ProjectionExpression,
};
use std::collections::{BTreeMap, BTreeSet};

mod binding;
mod projection;
mod statement;
mod with_clause;

use binding::*;
use projection::*;
pub use statement::{plan, plan_with_params};
use with_clause::*;

const MAX_VECTOR_SEEDED_GRAPH_HOPS: usize = 2;

#[derive(Debug, Clone, PartialEq)]
pub enum LogicalPlan {
    CreateNodeLabel {
        label: String,
    },
    CreateRelationshipType {
        rel_type: String,
    },
    CreateNodeTable {
        name: String,
    },
    CreateRelationshipTable {
        name: String,
    },
    CreateProperty {
        table_kind: SchemaTableKind,
        table: String,
        property: String,
        value_type: SchemaPropertyType,
        nullable: bool,
    },
    AlterTableState {
        table_kind: SchemaTableKind,
        table: String,
        state: SchemaObjectState,
    },
    AlterPropertyState {
        table_kind: SchemaTableKind,
        table: String,
        property: String,
        state: SchemaObjectState,
    },
    CreateIndex {
        label: String,
        property: String,
    },
    CreateCompositeIndex {
        label: String,
        properties: Vec<String>,
    },
    CreateRangeIndex {
        label: String,
        property: String,
    },
    CreateFullTextIndex {
        label: String,
        property: String,
    },
    CreateUniqueConstraint {
        label: String,
        property: String,
    },
    CreateNodePropertyExistsConstraint {
        label: String,
        property: String,
    },
    CreateRelationshipUniqueConstraint {
        rel_type: String,
        property: String,
    },
    CreateRelationshipPropertyExistsConstraint {
        rel_type: String,
        property: String,
    },
    ProjectGraph {
        name: String,
        node_labels: Vec<String>,
        rel_types: Vec<String>,
    },
    GraphAlgorithm {
        algorithm: GraphAlgorithmKind,
        graph_name: String,
        options: GraphAlgorithmOptions,
        score_column: String,
        node_visibility_predicate: Option<Predicate>,
    },
    VectorSeed {
        embedding_parameter: String,
        embedding_dimension: usize,
        top_k: usize,
        output_external_id: bool,
    },
    CreateNode {
        label: String,
        properties: BTreeMap<String, Value>,
    },
    MergeNode {
        label: String,
        match_properties: BTreeMap<String, Value>,
        on_create_properties: BTreeMap<String, Value>,
        on_match_assignments: Vec<SetAssignment>,
        post_merge_assignments: Vec<SetAssignment>,
    },
    MergeRelationship {
        source_label: String,
        source_properties: BTreeMap<String, Value>,
        rel_type: String,
        rel_properties: BTreeMap<String, Value>,
        target_label: String,
        target_properties: BTreeMap<String, Value>,
    },
    MergeMatchedRelationship {
        source_label: String,
        source_properties: BTreeMap<String, Value>,
        target_label: String,
        target_properties: BTreeMap<String, Value>,
        rel_type: String,
        rel_match_properties: BTreeMap<String, Value>,
        on_create_properties: BTreeMap<String, Value>,
    },
    MergeRelationshipFromMatchedRelationship {
        source_label: String,
        source_properties: BTreeMap<String, Value>,
        old_rel_variable: Option<String>,
        old_rel_type: String,
        old_rel_properties: BTreeMap<String, Value>,
        target_label: String,
        target_properties: BTreeMap<String, Value>,
        new_rel_type: String,
        new_rel_match_properties: BTreeMap<String, Value>,
        on_create_properties: BTreeMap<String, RelationshipOnCreateValue>,
    },
    MergeRelationshipToMatchedTarget {
        source_label: String,
        source_properties: BTreeMap<String, Value>,
        old_rel_type: String,
        old_rel_properties: BTreeMap<String, Value>,
        old_target_label: String,
        old_target_properties: BTreeMap<String, Value>,
        new_target_label: String,
        new_target_properties: BTreeMap<String, Value>,
        new_rel_type: String,
        new_rel_match_properties: BTreeMap<String, Value>,
        on_create_properties: BTreeMap<String, Value>,
    },
    MergeRelationshipFromMatchedTarget {
        old_source_label: String,
        old_source_properties: BTreeMap<String, Value>,
        old_rel_type: String,
        old_rel_properties: BTreeMap<String, Value>,
        old_target_label: String,
        old_target_properties: BTreeMap<String, Value>,
        new_source_label: String,
        new_source_properties: BTreeMap<String, Value>,
        new_rel_type: String,
        new_rel_match_properties: BTreeMap<String, Value>,
        on_create_properties: BTreeMap<String, Value>,
    },
    SetNodeProperty {
        variable: String,
        label: String,
        predicate: Option<Predicate>,
        property: String,
        value: SetValue,
    },
    SetNodeProperties {
        variable: String,
        label: String,
        predicate: Option<Predicate>,
        assignments: Vec<SetAssignment>,
    },
    SetNodePropertiesReturn {
        variable: String,
        label: String,
        predicate: Option<Predicate>,
        assignments: Vec<SetAssignment>,
        returns: SetNodePropertiesReturnMode,
    },
    SetRelationshipProperty {
        source_variable: String,
        source_label: String,
        predicate: Option<Predicate>,
        rel_variable: String,
        rel_type: String,
        rel_properties: BTreeMap<String, Value>,
        rel_predicate: Option<Predicate>,
        target_variable: String,
        target_label: String,
        target_properties: BTreeMap<String, Value>,
        property: String,
        value: Value,
    },
    SetRelationshipProperties {
        source_variable: String,
        source_label: String,
        predicate: Option<Predicate>,
        rel_variable: String,
        rel_type: String,
        rel_properties: BTreeMap<String, Value>,
        rel_predicate: Option<Predicate>,
        target_variable: String,
        target_label: String,
        target_properties: BTreeMap<String, Value>,
        assignments: Vec<RelationshipSetAssignment>,
    },
    DeleteNode {
        variable: String,
        label: String,
        predicate: Option<Predicate>,
        detach: bool,
    },
    DeleteRelationship {
        source_variable: String,
        source_label: String,
        predicate: Option<Predicate>,
        rel_variable: String,
        rel_type: String,
        rel_properties: BTreeMap<String, Value>,
        rel_predicate: Option<Predicate>,
        target_variable: String,
        target_label: String,
        target_properties: BTreeMap<String, Value>,
    },
    DeleteRelationshipTargetNodes {
        source_variable: String,
        source_label: String,
        source_predicate: Option<Predicate>,
        rel_type: String,
        rel_properties: BTreeMap<String, Value>,
        target_variable: String,
        target_label: String,
        target_properties: BTreeMap<String, Value>,
        detach: bool,
    },
    CreateRelationship {
        source_label: String,
        source_properties: BTreeMap<String, Value>,
        rel_type: String,
        rel_properties: BTreeMap<String, Value>,
        target_label: String,
        target_properties: BTreeMap<String, Value>,
    },
    CreateMatchedRelationship {
        source_label: String,
        source_properties: BTreeMap<String, Value>,
        target_label: String,
        target_properties: BTreeMap<String, Value>,
        rel_type: String,
        rel_properties: BTreeMap<String, Value>,
    },
    NodeScan {
        variable: String,
        label: String,
    },
    NodeCartesianProduct {
        left: Box<LogicalPlan>,
        right: Box<LogicalPlan>,
    },
    NodeColumnLookup {
        variable: String,
        label: String,
        property: String,
        column: String,
        optional: bool,
        input: Box<LogicalPlan>,
    },
    Expand {
        source_variable: String,
        source_label: String,
        rel_variable: Option<String>,
        rel_type: String,
        rel_properties: BTreeMap<String, Value>,
        direction: RelationshipDirection,
        target_variable: String,
        target_label: String,
        min_hops: usize,
        max_hops: usize,
        optional: bool,
        input: Box<LogicalPlan>,
    },
    OptionalDegree {
        source_variable: String,
        rel_type: String,
        rel_properties: BTreeMap<String, Value>,
        direction: RelationshipDirection,
        target_label: String,
        target_properties: BTreeMap<String, Value>,
        alias: String,
        input: Box<LogicalPlan>,
    },
    OptionalRelationshipCountSum {
        variable: String,
        label: String,
        properties: BTreeMap<String, Value>,
        legs: Vec<RelationshipCountLeg>,
        output: String,
    },
    ThreadRepairStats {
        label: String,
        identity_label: String,
        identity_ref_property: String,
        thread_id_property: String,
        message_rel_type: String,
        message_label: String,
        memory_rel_type: String,
        memory_label: String,
    },
    ShortestPath {
        source_variable: String,
        source_label: String,
        source_id: Value,
        source_visibility_predicate: Option<Predicate>,
        rel_type: String,
        direction: RelationshipDirection,
        target_variable: String,
        target_label: String,
        target_id: Value,
        target_visibility_predicate: Option<Predicate>,
        min_hops: usize,
        max_hops: usize,
        returns: Vec<ShortestPathProjection>,
    },
    Filter {
        predicate: Predicate,
        input: Box<LogicalPlan>,
    },
    Project {
        items: Vec<Projection>,
        input: Box<LogicalPlan>,
    },
    Aggregate {
        group_keys: Vec<Projection>,
        items: Vec<Aggregation>,
        input: Box<LogicalPlan>,
    },
    Distinct {
        input: Box<LogicalPlan>,
    },
    Sort {
        items: Vec<SortItem>,
        input: Box<LogicalPlan>,
    },
    Limit {
        offset: usize,
        limit: Option<usize>,
        input: Box<LogicalPlan>,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GraphAlgorithmKind {
    PageRank,
    Louvain,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct GraphAlgorithmOptions {
    pub damping: Option<f64>,
    pub max_iterations: Option<usize>,
    pub max_levels: Option<usize>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Projection {
    pub expression: ProjectionExpression,
    pub name: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SetNodePropertiesReturnMode {
    Project(Vec<Projection>),
    Count { name: String },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ShortestPathProjection {
    pub expression: ShortestPathProjectionExpression,
    pub name: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ShortestPathProjectionExpression {
    NodePropertyList { property: String },
    Length,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SetValue {
    Value(Value),
    Coalesce {
        property: String,
        default: Value,
    },
    AddInt {
        property: String,
        amount: i64,
    },
    DecrementFloorZero {
        property: String,
    },
    PreserveNewerExisting {
        property: String,
        incoming: Value,
        preserve: bool,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SetAssignment {
    pub property: String,
    pub value: SetValue,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RelationshipSetAssignment {
    pub property: String,
    pub value: Value,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RelationshipCountLeg {
    pub rel_type: String,
    pub direction: RelationshipDirection,
    pub distinct: bool,
    pub filter: Option<RelationshipCountFilter>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RelationshipCountFilter {
    PropertyNotEqOrEmpty { property: String, value: Value },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RelationshipOnCreateValue {
    Value(Value),
    MatchedRelationshipProperty { property: String },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Aggregation {
    pub function: AggregateFunction,
    pub target: AggregateTarget,
    pub distinct: bool,
    pub name: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AggregateFunction {
    Count,
    Min,
    Max,
    Avg,
    Collect,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AggregateTarget {
    All,
    Variable(String),
    Property { variable: String, property: String },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SortItem {
    pub key: SortKey,
    pub direction: SortDirection,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SortKey {
    Property { variable: String, property: String },
    Id { variable: String },
    Expression(ProjectionExpression),
    Column(String),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SortDirection {
    Asc,
    Desc,
}
