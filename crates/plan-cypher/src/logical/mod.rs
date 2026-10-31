// Copyright 2026 Nowledge
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use hawdb_core::{HawDBError, Result, ValidatedRegex, Value};
use hawdb_cypher::{
    AggregateExpression, AggregateFunctionName, ComparisonOp as CypherComparisonOp,
    GraphAlgorithmKind as CypherGraphAlgorithmKind,
    GraphAlgorithmOptions as CypherGraphAlgorithmOptions, OrderDirection as CypherOrderDirection,
    OrderExpression, OrderItem, PropertyPredicate, RelationshipDirection, ReturnExpression,
    ReturnItem, ScalarExpression, SetProperty, SetValueExpression, ShortestPathReturnExpression,
    Statement, TextSearch as CypherTextSearch, ValueExpression, VectorSearch as CypherVectorSearch,
};
use hawdb_cypher::{
    AstNode, ReturnExpressionKind, ReturnItemKind, ScalarExpressionKind, ValueExpressionKind,
};
pub use hawdb_ddl::{SchemaObjectState, SchemaPropertyType, SchemaTableKind};
pub use hawdb_expression::{
    CaseColumnSearchRankProjection, CaseEntitySearchRankProjection,
    CoalesceDifferenceProjectionTerm, ComparisonOp, DatePart, ListComprehensionProjection,
    PathFunction, Predicate, ProjectionExpression, ScalarBinaryOp, SortDirection, SortItem,
    SortKey,
};
use std::collections::{BTreeMap, BTreeSet};

mod arithmetic;
mod graph_match;
mod pipeline;
mod visibility;
pub use graph_match::*;
pub use pipeline::{
    plan_normalized_pipeline_query, plan_parsed_pipeline_query, plan_pipeline_query,
    unique_projection_names,
};
pub use visibility::apply_node_visibility_predicates;
mod binding;
mod case;
#[cfg(test)]
mod list_tests;
mod projection;
mod statement;

use binding::*;
use case::*;
use projection::*;
pub use statement::{plan, plan_with_params};

const MAX_RETRIEVER_SEEDED_GRAPH_HOPS: usize = 2;

/// Public projection of the specialized thread-repair summary operator.
pub const THREAD_REPAIR_STATS_COLUMNS: [&str; 7] = [
    "t.id",
    "t.thread_id",
    "CASE WHEN t.space_id IS NULL OR t.space_id = '' THEN 'default' ELSE t.space_id END",
    "COALESCE(t.message_count, 0)",
    "identity_refs",
    "legacy_messages",
    "COUNT(m)",
];

/// Shared logical/physical parameters for explicit schema-bound index access.
/// The attached read domain resolves the index and supplies canonical nodes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VectorIndexScan<P> {
    pub index: String,
    pub embedding_parameter: String,
    pub candidate_limit: usize,
    pub top_k: usize,
    pub variable: String,
    pub distance_column: String,
    pub node_visibility_predicate: Option<Predicate>,
    pub mode: hawdb_core::VectorIndexQueryMode,
    pub eligible_variable: Option<String>,
    pub max_work: usize,
    pub input: Option<Box<P>>,
}

impl<P> VectorIndexScan<P> {
    pub fn map_input<Q>(self, lower: impl FnOnce(P) -> Q) -> VectorIndexScan<Q> {
        VectorIndexScan {
            index: self.index,
            embedding_parameter: self.embedding_parameter,
            candidate_limit: self.candidate_limit,
            top_k: self.top_k,
            variable: self.variable,
            distance_column: self.distance_column,
            node_visibility_predicate: self.node_visibility_predicate,
            mode: self.mode,
            eligible_variable: self.eligible_variable,
            max_work: self.max_work,
            input: self.input.map(|input| Box::new(lower(*input))),
        }
    }
}

/// A mutation property resolved from one bounded `UNWIND` row.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BatchMutationValue {
    Static(Value),
    Row,
    RowProperty(String),
}

/// Two independently matched endpoints, evaluated for every bounded input row.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BatchRelationshipMerge {
    pub source_variable: String,
    pub source_label: String,
    pub source_properties: BTreeMap<String, BatchMutationValue>,
    pub target_variable: String,
    pub target_label: String,
    pub target_properties: BTreeMap<String, BatchMutationValue>,
    pub rel_type: String,
    pub rel_match_properties: BTreeMap<String, BatchMutationValue>,
    pub on_create_properties: BTreeMap<String, BatchMutationValue>,
    pub returns: Option<Vec<Projection>>,
}

/// The mutation operations currently fed by a bounded `UNWIND` row source.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BatchMutationOperation {
    MergeRelationship(Box<BatchRelationshipMerge>),
    CreateNode {
        label: String,
        properties: BTreeMap<String, BatchMutationValue>,
    },
    MergeNode {
        label: String,
        match_properties: BTreeMap<String, BatchMutationValue>,
        on_create_properties: BTreeMap<String, BatchMutationValue>,
    },
}

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
        relationship_predicates: BTreeMap<String, Predicate>,
    },
    GraphAlgorithm {
        algorithm: GraphAlgorithmKind,
        graph_name: String,
        options: GraphAlgorithmOptions,
        score_column: String,
        return_node_identity: bool,
        node_visibility_predicate: Option<Predicate>,
    },
    GraphAlgorithmOutput(Box<GraphAlgorithmOutput>),
    GraphSeed {
        query_parameter: String,
        label: String,
        variable: String,
        score_column: String,
        top_k: usize,
        node_visibility_predicate: Option<Predicate>,
    },
    TextSeed {
        query_parameter: String,
        top_k: usize,
        complete: bool,
        output_external_id: bool,
    },
    VectorSeed {
        embedding_parameter: String,
        embedding_dimension: usize,
        top_k: usize,
        output_external_id: bool,
    },
    VectorIndexSearch(Box<VectorIndexScan<LogicalPlan>>),
    CreateNode {
        label: String,
        properties: BTreeMap<String, Value>,
    },
    Unwind {
        rows: Vec<Value>,
        variable: String,
    },
    UnwindMutation {
        rows: Vec<Value>,
        variable: String,
        operation: BatchMutationOperation,
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
    GraphMatch {
        program: GraphMatchProgram,
        input: Option<Box<LogicalPlan>>,
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
        node_visibility_predicate: Option<Predicate>,
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

/// Explicit result selection, separate from legacy algorithm option literals.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct GraphAlgorithmIdentity {
    pub node_id: bool,
    pub node_label: bool,
    pub node_labels: bool,
}

impl GraphAlgorithmIdentity {
    pub fn legacy(enabled: bool) -> Self {
        Self {
            node_id: enabled,
            node_label: enabled,
            node_labels: false,
        }
    }

    pub fn is_empty(self) -> bool {
        !(self.node_id || self.node_label || self.node_labels)
    }

    pub fn columns(self) -> impl Iterator<Item = &'static str> {
        [
            ("node_id", self.node_id),
            ("node_label", self.node_label),
            ("node_labels", self.node_labels),
        ]
        .into_iter()
        .filter_map(|(name, selected)| selected.then_some(name))
    }
}

/// Algorithm source with an explicit, independently selected identity projection.
#[derive(Debug, Clone, PartialEq)]
pub struct GraphAlgorithmOutput {
    pub algorithm: GraphAlgorithmKind,
    pub graph_name: String,
    pub options: GraphAlgorithmOptions,
    pub score_column: String,
    pub identity: GraphAlgorithmIdentity,
    pub node_visibility_predicate: Option<Predicate>,
}

#[derive(Debug, Clone, Copy, Default, PartialEq)]
pub struct GraphAlgorithmOptions {
    pub damping: Option<f64>,
    pub max_iterations: Option<usize>,
    pub max_phases: Option<usize>,
    pub max_levels: Option<usize>,
    pub tolerance: Option<f64>,
    pub normalize_initial: Option<bool>,
    pub resolution: Option<f64>,
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
    Sum,
    Min,
    Max,
    Avg,
    Collect,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AggregateTarget {
    Ranked {
        document_id: String,
        score: String,
        limit: usize,
    },
    Column(String),
    ColumnProperty {
        column: String,
        property: String,
    },
    All,
    Variable(String),
    Property {
        variable: String,
        property: String,
    },
    /// A general aggregate-free argument evaluated against each input row.
    Expression(ProjectionExpression),
}

impl AggregateFunction {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Count => "count",
            Self::Sum => "sum",
            Self::Min => "min",
            Self::Max => "max",
            Self::Avg => "avg",
            Self::Collect => "collect",
        }
    }
}

// Retained query ownership: GraphAlgorithmKind.
hawdb_core::impl_retained_heap_for_copy!(GraphAlgorithmKind);

// Retained query ownership: GraphAlgorithmOptions.
hawdb_core::impl_retained_heap_for_copy!(GraphAlgorithmOptions);

// Retained query ownership: GraphAlgorithmOutput.
hawdb_core::impl_retained_heap_for_struct!(GraphAlgorithmOutput {
    algorithm,
    graph_name,
    options,
    score_column,
    identity,
    node_visibility_predicate
});

// Retained query ownership: VectorIndexScan.
impl<P: hawdb_core::RetainedHeapBytes> hawdb_core::RetainedHeapBytes for VectorIndexScan<P> {
    fn retained_heap_bytes(&self) -> Option<usize> {
        let Self {
            index,
            embedding_parameter,
            candidate_limit,
            top_k,
            variable,
            distance_column,
            node_visibility_predicate,
            mode,
            eligible_variable,
            max_work,
            input,
        } = self;
        hawdb_core::checked_retained_heap_sum([
            hawdb_core::RetainedHeapBytes::retained_heap_bytes(index),
            hawdb_core::RetainedHeapBytes::retained_heap_bytes(embedding_parameter),
            hawdb_core::RetainedHeapBytes::retained_heap_bytes(candidate_limit),
            hawdb_core::RetainedHeapBytes::retained_heap_bytes(top_k),
            hawdb_core::RetainedHeapBytes::retained_heap_bytes(variable),
            hawdb_core::RetainedHeapBytes::retained_heap_bytes(distance_column),
            hawdb_core::RetainedHeapBytes::retained_heap_bytes(node_visibility_predicate),
            hawdb_core::RetainedHeapBytes::retained_heap_bytes(mode),
            hawdb_core::RetainedHeapBytes::retained_heap_bytes(eligible_variable),
            hawdb_core::RetainedHeapBytes::retained_heap_bytes(max_work),
            hawdb_core::RetainedHeapBytes::retained_heap_bytes(input),
        ])
    }
}

// Retained query ownership: BatchRelationshipMerge.
hawdb_core::impl_retained_heap_for_struct!(BatchRelationshipMerge {
    source_variable,
    source_label,
    source_properties,
    target_variable,
    target_label,
    target_properties,
    rel_type,
    rel_match_properties,
    on_create_properties,
    returns,
});

// Retained query ownership: BatchMutationOperation.
impl hawdb_core::RetainedHeapBytes for BatchMutationOperation {
    fn retained_heap_bytes(&self) -> Option<usize> {
        match self {
            Self::MergeRelationship(merge) => {
                hawdb_core::RetainedHeapBytes::retained_heap_bytes(merge)
            }
            Self::CreateNode { label, properties } => hawdb_core::checked_retained_heap_sum([
                hawdb_core::RetainedHeapBytes::retained_heap_bytes(label),
                hawdb_core::RetainedHeapBytes::retained_heap_bytes(properties),
            ]),
            Self::MergeNode {
                label,
                match_properties,
                on_create_properties,
            } => hawdb_core::checked_retained_heap_sum([
                hawdb_core::RetainedHeapBytes::retained_heap_bytes(label),
                hawdb_core::RetainedHeapBytes::retained_heap_bytes(match_properties),
                hawdb_core::RetainedHeapBytes::retained_heap_bytes(on_create_properties),
            ]),
        }
    }
}

// Retained query ownership: SetAssignment.
hawdb_core::impl_retained_heap_for_struct!(SetAssignment { property, value });

// Retained query ownership: RelationshipOnCreateValue.
impl hawdb_core::RetainedHeapBytes for RelationshipOnCreateValue {
    fn retained_heap_bytes(&self) -> Option<usize> {
        match self {
            Self::Value(value0) => hawdb_core::checked_retained_heap_sum([
                hawdb_core::RetainedHeapBytes::retained_heap_bytes(value0),
            ]),
            Self::MatchedRelationshipProperty { property } => {
                hawdb_core::checked_retained_heap_sum([
                    hawdb_core::RetainedHeapBytes::retained_heap_bytes(property),
                ])
            }
        }
    }
}

// Retained query ownership: SetValue.
impl hawdb_core::RetainedHeapBytes for SetValue {
    fn retained_heap_bytes(&self) -> Option<usize> {
        match self {
            Self::Value(value0) => hawdb_core::checked_retained_heap_sum([
                hawdb_core::RetainedHeapBytes::retained_heap_bytes(value0),
            ]),
            Self::Coalesce { property, default } => hawdb_core::checked_retained_heap_sum([
                hawdb_core::RetainedHeapBytes::retained_heap_bytes(property),
                hawdb_core::RetainedHeapBytes::retained_heap_bytes(default),
            ]),
            Self::AddInt { property, amount } => hawdb_core::checked_retained_heap_sum([
                hawdb_core::RetainedHeapBytes::retained_heap_bytes(property),
                hawdb_core::RetainedHeapBytes::retained_heap_bytes(amount),
            ]),
            Self::DecrementFloorZero { property } => hawdb_core::checked_retained_heap_sum([
                hawdb_core::RetainedHeapBytes::retained_heap_bytes(property),
            ]),
            Self::PreserveNewerExisting {
                property,
                incoming,
                preserve,
            } => hawdb_core::checked_retained_heap_sum([
                hawdb_core::RetainedHeapBytes::retained_heap_bytes(property),
                hawdb_core::RetainedHeapBytes::retained_heap_bytes(incoming),
                hawdb_core::RetainedHeapBytes::retained_heap_bytes(preserve),
            ]),
        }
    }
}

// Retained query ownership: SetNodePropertiesReturnMode.
impl hawdb_core::RetainedHeapBytes for SetNodePropertiesReturnMode {
    fn retained_heap_bytes(&self) -> Option<usize> {
        match self {
            Self::Project(value0) => hawdb_core::checked_retained_heap_sum([
                hawdb_core::RetainedHeapBytes::retained_heap_bytes(value0),
            ]),
            Self::Count { name } => hawdb_core::checked_retained_heap_sum([
                hawdb_core::RetainedHeapBytes::retained_heap_bytes(name),
            ]),
        }
    }
}

// Retained query ownership: RelationshipSetAssignment.
hawdb_core::impl_retained_heap_for_struct!(RelationshipSetAssignment { property, value });

// Retained query ownership: RelationshipCountLeg.
hawdb_core::impl_retained_heap_for_struct!(RelationshipCountLeg {
    rel_type,
    direction,
    distinct,
    filter
});

// Retained query ownership: ShortestPathProjection.
hawdb_core::impl_retained_heap_for_struct!(ShortestPathProjection { expression, name });

// Retained query ownership: Aggregation.
hawdb_core::impl_retained_heap_for_struct!(Aggregation {
    function,
    target,
    distinct,
    name
});

// Retained query ownership: Projection.
hawdb_core::impl_retained_heap_for_struct!(Projection { expression, name });

// Retained query ownership: GraphAlgorithmIdentity.
hawdb_core::impl_retained_heap_for_copy!(GraphAlgorithmIdentity);

// Retained query ownership: BatchMutationValue.
impl hawdb_core::RetainedHeapBytes for BatchMutationValue {
    fn retained_heap_bytes(&self) -> Option<usize> {
        match self {
            Self::Row => Some(0),
            Self::Static(value0) => hawdb_core::checked_retained_heap_sum([
                hawdb_core::RetainedHeapBytes::retained_heap_bytes(value0),
            ]),
            Self::RowProperty(value0) => hawdb_core::checked_retained_heap_sum([
                hawdb_core::RetainedHeapBytes::retained_heap_bytes(value0),
            ]),
        }
    }
}

// Retained query ownership: RelationshipCountFilter.
impl hawdb_core::RetainedHeapBytes for RelationshipCountFilter {
    fn retained_heap_bytes(&self) -> Option<usize> {
        match self {
            Self::PropertyNotEqOrEmpty { property, value } => {
                hawdb_core::checked_retained_heap_sum([
                    hawdb_core::RetainedHeapBytes::retained_heap_bytes(property),
                    hawdb_core::RetainedHeapBytes::retained_heap_bytes(value),
                ])
            }
        }
    }
}

// Retained query ownership: ShortestPathProjectionExpression.
impl hawdb_core::RetainedHeapBytes for ShortestPathProjectionExpression {
    fn retained_heap_bytes(&self) -> Option<usize> {
        match self {
            Self::NodePropertyList { property } => hawdb_core::checked_retained_heap_sum([
                hawdb_core::RetainedHeapBytes::retained_heap_bytes(property),
            ]),
            Self::Length => hawdb_core::checked_retained_heap_sum([]),
        }
    }
}

// Retained query ownership: AggregateFunction.
hawdb_core::impl_retained_heap_for_copy!(AggregateFunction);

// Retained query ownership: AggregateTarget.
impl hawdb_core::RetainedHeapBytes for AggregateTarget {
    fn retained_heap_bytes(&self) -> Option<usize> {
        match self {
            Self::Ranked {
                document_id,
                score,
                limit,
            } => hawdb_core::checked_retained_heap_sum([
                hawdb_core::RetainedHeapBytes::retained_heap_bytes(document_id),
                hawdb_core::RetainedHeapBytes::retained_heap_bytes(score),
                hawdb_core::RetainedHeapBytes::retained_heap_bytes(limit),
            ]),
            Self::Column(value0) => hawdb_core::checked_retained_heap_sum([
                hawdb_core::RetainedHeapBytes::retained_heap_bytes(value0),
            ]),
            Self::ColumnProperty { column, property } => hawdb_core::checked_retained_heap_sum([
                hawdb_core::RetainedHeapBytes::retained_heap_bytes(column),
                hawdb_core::RetainedHeapBytes::retained_heap_bytes(property),
            ]),
            Self::All => hawdb_core::checked_retained_heap_sum([]),
            Self::Variable(value0) => hawdb_core::checked_retained_heap_sum([
                hawdb_core::RetainedHeapBytes::retained_heap_bytes(value0),
            ]),
            Self::Property { variable, property } => hawdb_core::checked_retained_heap_sum([
                hawdb_core::RetainedHeapBytes::retained_heap_bytes(variable),
                hawdb_core::RetainedHeapBytes::retained_heap_bytes(property),
            ]),
            Self::Expression(value0) => hawdb_core::checked_retained_heap_sum([
                hawdb_core::RetainedHeapBytes::retained_heap_bytes(value0),
            ]),
        }
    }
}
