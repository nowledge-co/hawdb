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

mod pipeline;
pub use pipeline::*;

mod source;
pub use source::{AstNode, SourceSpan};

pub use hawdb_core::RelationshipDirection;
use hawdb_core::Value;
pub use hawdb_ddl::{SchemaObjectState, SchemaPropertyType, SchemaTableKind};
use std::collections::BTreeMap;

#[derive(Debug, Clone, PartialEq)]
pub enum Statement {
    BeginTransaction,
    Checkpoint,
    CypherQuery(Box<CypherQuery>),
    Explain(Box<Explain>),
    /// A bounded row source followed by one mutation clause.
    ///
    /// The row source and mutation are planned and admitted as one atomic batch.
    UnwindMutation(Box<QueryPipeline>),
    Commit,
    CreateNodeLabel(String),
    CreateRelationshipType(String),
    CreateNodeTable(String),
    CreateRelationshipTable(String),
    CreateProperty(CreateProperty),
    AlterTableState(AlterTableState),
    AlterPropertyState(AlterPropertyState),
    CreateIndex(CreateIndex),
    CreateCompositeIndex(CreateCompositeIndex),
    CreateRangeIndex(CreateIndex),
    CreateFullTextIndex(CreateIndex),
    CreateUniqueConstraint(CreateIndex),
    CreateNodePropertyExistsConstraint(CreateIndex),
    CreateRelationshipUniqueConstraint(CreateIndex),
    CreateRelationshipPropertyExistsConstraint(CreateIndex),
    ProjectGraph(ProjectGraph),
    GraphAlgorithm(GraphAlgorithm),
    VectorSearch(VectorSearch),
    CreateNode(CreateNode),
    CreateRelationship(CreateRelationship),
    MergeNode(MergeNode),
    MergeRelationship(CreateRelationship),
    /// Ordered MATCH, WITH, RETURN and mutation clauses with their source spans.
    Pipeline(Box<QueryPipeline>),
    SetSystemVariable(SetSystemVariable),
    Rollback,
}

#[derive(Debug, Clone, PartialEq)]
pub struct Explain {
    pub analyze: bool,
    pub statement: Statement,
}

#[derive(Debug, Clone, PartialEq)]
pub struct CypherQuery {
    pub system_variables: Vec<SetSystemVariable>,
    pub statement: Statement,
}

#[derive(Debug, Clone, PartialEq)]
pub struct SetSystemVariable {
    pub name: String,
    pub value: ValueExpression,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreateIndex {
    pub label: String,
    pub property: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreateCompositeIndex {
    pub label: String,
    pub properties: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreateProperty {
    pub table_kind: SchemaTableKind,
    pub table: String,
    pub property: String,
    pub value_type: SchemaPropertyType,
    pub nullable: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AlterTableState {
    pub table_kind: SchemaTableKind,
    pub table: String,
    pub state: SchemaObjectState,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AlterPropertyState {
    pub table_kind: SchemaTableKind,
    pub table: String,
    pub property: String,
    pub state: SchemaObjectState,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ProjectGraph {
    pub name: String,
    pub node_labels: Vec<String>,
    pub rel_types: Vec<String>,
    pub relationship_predicates: BTreeMap<String, PropertyPredicate>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct GraphAlgorithm {
    pub algorithm: GraphAlgorithmKind,
    pub graph_name: String,
    pub options: Box<GraphAlgorithmOptions>,
    pub score_column: String,
    pub return_node_identity: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GraphSeedSearch {
    pub query: ValueExpression,
    pub label: ValueExpression,
    pub top_k: Option<ValueExpression>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TextSearch {
    pub query: ValueExpression,
    pub top_k: Option<ValueExpression>,
    pub complete: Option<ValueExpression>,
    pub max_candidates: Option<ValueExpression>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VectorSearch {
    pub embedding: ValueExpression,
    pub top_k: Option<ValueExpression>,
}

/// Explicit index retrieval semantics, separate from legacy relevance search.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VectorIndexSearch {
    pub index: ValueExpression,
    pub embedding: ValueExpression,
    pub mode: hawdb_core::VectorIndexQueryMode,
    pub eligible: Option<String>,
    pub candidates: Option<ValueExpression>,
    pub max_work: Option<ValueExpression>,
    pub top_k: ValueExpression,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GraphAlgorithmKind {
    PageRank,
    Louvain,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct GraphAlgorithmOptions {
    pub damping: Option<ValueExpression>,
    pub max_iterations: Option<ValueExpression>,
    pub max_phases: Option<ValueExpression>,
    pub max_levels: Option<ValueExpression>,
    pub tolerance: Option<ValueExpression>,
    pub normalize_initial: Option<ValueExpression>,
    pub resolution: Option<ValueExpression>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreateNode {
    pub label: String,
    pub properties: BTreeMap<String, ValueExpression>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreateRelationship {
    pub source: CreateNode,
    pub rel_type: String,
    pub properties: BTreeMap<String, ValueExpression>,
    pub target: CreateNode,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MergeNode {
    pub variable: Option<String>,
    pub label: String,
    pub properties: BTreeMap<String, ValueExpression>,
    pub on_create_sets: Vec<SetProperty>,
    pub on_match_sets: Vec<SetProperty>,
    pub post_merge_sets: Vec<SetProperty>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PostWithNodeLookup {
    pub variable: String,
    pub label: String,
    pub property: String,
    pub column: String,
    pub optional: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ShortestPathReturnExpression {
    NodePropertyList {
        path_variable: String,
        property: String,
    },
    Length {
        path_variable: String,
    },
    Nodes {
        path_variable: String,
    },
    Relationships {
        path_variable: String,
    },
}

impl ShortestPathReturnExpression {
    pub fn path_variable(&self) -> &str {
        match self {
            Self::NodePropertyList { path_variable, .. }
            | Self::Length { path_variable }
            | Self::Nodes { path_variable }
            | Self::Relationships { path_variable } => path_variable,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SetProperty {
    pub variable: String,
    pub property: String,
    pub value: SetValueExpression,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SetValueExpression {
    Value(ValueExpression),
    Property {
        variable: String,
        property: String,
    },
    CoalesceProperty {
        variable: String,
        property: String,
        default: ValueExpression,
    },
    PropertyAdd {
        variable: String,
        property: String,
        value: ValueExpression,
    },
    DecrementFloorZero {
        variable: String,
        property: String,
    },
    PreserveNewerExisting {
        variable: String,
        property: String,
        incoming: ValueExpression,
        preserve: ValueExpression,
    },
    CoalescePropertyAdd {
        variable: String,
        property: String,
        default: ValueExpression,
        value: ValueExpression,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PropertyPredicate {
    And(Vec<PropertyPredicate>),
    Or(Vec<PropertyPredicate>),
    Not(Box<PropertyPredicate>),
    RelationshipExists {
        variable: String,
        rel_type: String,
        direction: RelationshipDirection,
        target_label: String,
    },
    BoundRelationshipExists {
        source_variable: String,
        rel_type: String,
        direction: RelationshipDirection,
        target_variable: String,
    },
    IdEq {
        variable: String,
        value: ValueExpression,
    },
    IdNotEq {
        variable: String,
        value: ValueExpression,
    },
    IdCompare {
        variable: String,
        op: ComparisonOp,
        value: ValueExpression,
    },
    IdIn {
        variable: String,
        values: ValueExpression,
    },
    Eq {
        variable: String,
        property: String,
        value: ValueExpression,
    },
    NotEq {
        variable: String,
        property: String,
        value: ValueExpression,
    },
    Compare {
        variable: String,
        property: String,
        op: ComparisonOp,
        value: ValueExpression,
    },
    ExpressionEq {
        expression: ScalarExpression,
        value: ScalarExpression,
    },
    ExpressionNotEq {
        expression: ScalarExpression,
        value: ScalarExpression,
    },
    ExpressionCompare {
        expression: ScalarExpression,
        op: ComparisonOp,
        value: ScalarExpression,
    },
    ExpressionContains {
        expression: ScalarExpression,
        value: ScalarExpression,
    },
    ExpressionIn {
        expression: ScalarExpression,
        values: ValueExpression,
    },
    ListContains {
        variable: String,
        property: String,
        value: ValueExpression,
    },
    ListContainsLower {
        variable: String,
        property: String,
        value: ValueExpression,
    },
    Contains {
        variable: String,
        property: String,
        value: ValueExpression,
    },
    StartsWith {
        variable: String,
        property: String,
        value: ValueExpression,
    },
    EndsWith {
        variable: String,
        property: String,
        value: ValueExpression,
    },
    RegexMatch {
        variable: String,
        property: String,
        pattern: ValueExpression,
    },
    IsNull {
        variable: String,
        property: String,
    },
    IsNotNull {
        variable: String,
        property: String,
    },
    ParameterIsNull {
        parameter: String,
    },
    ParameterIsNotNull {
        parameter: String,
    },
    ParameterEq {
        left: String,
        right: ValueExpression,
    },
    ParameterNotEq {
        left: String,
        right: ValueExpression,
    },
    In {
        variable: String,
        property: String,
        values: ValueExpression,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ComparisonOp {
    Lt,
    Lte,
    Gt,
    Gte,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ValueExpressionKind {
    Literal(Value),
    Parameter(String),
    List(Vec<ValueExpression>),
    Binding(String),
    BindingProperty {
        variable: String,
        property: String,
    },
    CurrentTimestamp,
    Timestamp(Box<ValueExpression>),
    /// A temporal function over statically bound arguments, such as
    /// `date('2026-10-30')` in a CREATE property map. Evaluated when the
    /// statement is planned; zero-argument constructors read the statement
    /// clock.
    Temporal {
        function: hawdb_core::TemporalFunction,
        arguments: Vec<ValueExpression>,
    },
    /// `{key: value, ...}`, accepted as a temporal function argument.
    Map(BTreeMap<String, ValueExpression>),
}

impl ValueExpressionKind {
    /// True when the value reads a row binding. Scalar contexts parse such
    /// lists as [`ScalarExpressionKind::List`] instead.
    pub fn reads_row_binding(&self) -> bool {
        match self {
            Self::Binding(_) | Self::BindingProperty { .. } => true,
            Self::List(values) => values.iter().any(|value| value.kind.reads_row_binding()),
            Self::Timestamp(value) => value.kind.reads_row_binding(),
            Self::Temporal { arguments, .. } => arguments
                .iter()
                .any(|argument| argument.kind.reads_row_binding()),
            Self::Map(entries) => entries.values().any(|value| value.kind.reads_row_binding()),
            Self::Literal(_) | Self::Parameter(_) | Self::CurrentTimestamp => false,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReturnItemKind {
    pub expression: ReturnExpression,
    pub alias: Option<String>,
}

/// A scalar expression shared by projections, predicates and nested calls.
///
/// Aggregates are return items, not scalar function arguments:
///
/// ```compile_fail
/// use hawdb_cypher::{AggregateExpression, AstNode, ScalarExpressionKind};
///
/// let nested = AstNode::synthetic(ScalarExpressionKind::Coalesce(vec![AggregateExpression::CountAll]));
/// ```
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ScalarExpressionKind {
    Variable(String),
    Property {
        variable: String,
        property: String,
    },
    Id(String),
    RelationshipType(String),
    Value(ValueExpression),
    Coalesce(Vec<ScalarExpression>),
    VectorDistance {
        metric: hawdb_core::VectorDistance,
        left: Box<ScalarExpression>,
        right: Box<ScalarExpression>,
    },
    JsonValue {
        expression: Box<ScalarExpression>,
        path: ValueExpression,
    },
    Left {
        expression: Box<ScalarExpression>,
        length: ValueExpression,
    },
    Lower(Box<ScalarExpression>),
    DatePart {
        part: String,
        variable: String,
        property: String,
    },
    DefaultIfNullOrEq {
        variable: String,
        property: String,
        empty: ValueExpression,
        default: ValueExpression,
    },
    DefaultIfNull {
        variable: String,
        property: String,
        default: ValueExpression,
    },
    CasePropertyNotNullOrEq {
        variable: String,
        property: String,
        empty: Box<ValueExpression>,
        non_empty: Box<ValueExpression>,
        null_or_empty: Box<ValueExpression>,
    },
    CasePropertyEqualsRank {
        variable: String,
        property: String,
        branches: Vec<(ValueExpression, ValueExpression)>,
        default: ValueExpression,
    },
    CaseLowerPropertyDefault {
        variable: String,
        property: String,
        default: ValueExpression,
    },
    CaseCoalesceDifferenceFloorZero {
        variable: String,
        terms: Vec<CoalesceDifferenceTerm>,
    },
    Case {
        operand: Option<Box<ScalarExpression>>,
        branches: Vec<(ScalarExpression, ScalarExpression)>,
        otherwise: Option<Box<ScalarExpression>>,
    },
    Binary {
        left: Box<ScalarExpression>,
        op: ScalarBinaryOp,
        right: Box<ScalarExpression>,
    },
    Not(Box<ScalarExpression>),
    IsNull {
        expression: Box<ScalarExpression>,
        negated: bool,
    },
    /// A list literal with at least one row-bound element, such as
    /// `[n, m.name, $p]`. Lists of constants and parameters stay
    /// [`ScalarExpressionKind::Value`] so they keep value-list fast paths.
    List(Vec<ScalarExpression>),
    /// `size`, `head`, `last`, `tail`, `reverse` or `range`.
    ListFunction {
        function: hawdb_core::ListFunction,
        arguments: Vec<ScalarExpression>,
    },
    /// `[variable IN list WHERE predicate | projection]`.
    ///
    /// `variable` is scoped to `predicate` and `projection`; it must not
    /// shadow a variable or column already visible to the comprehension.
    ListComprehension {
        variable: String,
        list: Box<ScalarExpression>,
        predicate: Option<Box<ScalarExpression>>,
        projection: Option<Box<ScalarExpression>>,
    },
    /// `list[index]`; negative indexes count from the end.
    ListIndex {
        list: Box<ScalarExpression>,
        index: Box<ScalarExpression>,
    },
    /// `list[start..end]`; either bound may be omitted.
    ListSlice {
        list: Box<ScalarExpression>,
        start: Option<Box<ScalarExpression>>,
        end: Option<Box<ScalarExpression>>,
    },
    /// `length(p)`, `nodes(p)`, `relationships(p)` or
    /// `properties(nodes(p), 'key')` over a bound path variable.
    Path(ShortestPathReturnExpression),
    /// An openCypher temporal function: `date(...)`, `datetime(...)`,
    /// `duration.between(a, b)`, `date.truncate(unit, value)`, ...
    TemporalFunction {
        function: hawdb_core::TemporalFunction,
        arguments: Vec<ScalarExpression>,
    },
    /// Postfix `expression.key` over a general expression: a temporal
    /// component (`p.birthday.month`) or a map key.
    PropertyAccess {
        expression: Box<ScalarExpression>,
        key: String,
    },
    /// `{key: expression, ...}` in source order (keys are unique).
    MapLiteral(Vec<(String, ScalarExpression)>),
}

/// A projection or grouping expression, with aggregation explicit in its type.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ReturnExpressionKind {
    Value(ScalarExpression),
    Aggregate(AggregateExpression),
    Arithmetic {
        first: Box<ReturnExpression>,
        rest: Vec<(ArithmeticOp, ReturnExpression)>,
    },
    Path(ShortestPathReturnExpression),
    /// A list function applied to an expression containing an aggregate,
    /// such as `size(collect(x))`. Scalar-only calls use
    /// [`ScalarExpressionKind::ListFunction`].
    ListFunction {
        function: hawdb_core::ListFunction,
        arguments: Vec<ReturnExpression>,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ArithmeticOp {
    Add,
    Subtract,
    Multiply,
    Divide,
    Remainder,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AggregateExpression {
    CountAll,
    MaxVariable {
        variable: String,
    },
    CollectTop {
        document_id: String,
        score: String,
        limit: ValueExpression,
    },
    CountVariable {
        variable: String,
        distinct: bool,
    },
    CountProperty {
        variable: String,
        property: String,
        distinct: bool,
    },
    CollectProperty {
        variable: String,
        property: String,
        distinct: bool,
    },
    CollectVariable {
        variable: String,
        distinct: bool,
    },
    MinProperty {
        variable: String,
        property: String,
    },
    MaxProperty {
        variable: String,
        property: String,
    },
    AvgProperty {
        variable: String,
        property: String,
    },
    /// An aggregate over an arbitrary aggregate-free argument expression.
    ///
    /// The parser keeps the historical fixed shapes above for the forms they
    /// cover, so this variant only carries arguments outside those shapes.
    Expression {
        function: AggregateFunctionName,
        distinct: bool,
        argument: Box<ReturnExpression>,
    },
}

/// The general aggregate functions accepted with an expression argument.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AggregateFunctionName {
    Count,
    Sum,
    Min,
    Max,
    Avg,
    Collect,
}

impl AggregateFunctionName {
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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ScalarBinaryOp {
    Eq,
    NotEq,
    Lt,
    Lte,
    Gt,
    Gte,
    Contains,
    ListContains,
    /// `element IN list` with openCypher three-valued semantics.
    In,
    And,
    Or,
    /// Scalar `+` (numbers, temporal instants and durations).
    Add,
    /// Scalar `-`.
    Subtract,
    /// Scalar `*`.
    Multiply,
    /// Scalar `/`.
    Divide,
    /// Scalar `%`.
    Remainder,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CoalesceDifferenceTerm {
    pub property: String,
    pub default: ValueExpression,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OrderItemKind {
    pub expression: OrderExpression,
    pub direction: OrderDirection,
}

/// An `ORDER BY` key. `Projection` carries keys outside the scalar grammar,
/// such as arithmetic or aggregates, planned like an unaliased projection
/// item; an aggregate key must match an aggregate projected by the same
/// `RETURN` or `WITH`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum OrderExpression {
    Property { variable: String, property: String },
    Id { variable: String },
    Value(ScalarExpression),
    Column(String),
    Projection(ReturnExpression),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OrderDirection {
    Asc,
    Desc,
}

pub type ValueExpression = AstNode<ValueExpressionKind>;

pub type ScalarExpression = AstNode<ScalarExpressionKind>;

pub type ReturnExpression = AstNode<ReturnExpressionKind>;

pub type ReturnItem = AstNode<ReturnItemKind>;

pub type OrderItem = AstNode<OrderItemKind>;

impl ScalarExpressionKind {
    /// Visits immediate scalar children in evaluation order, stopping on false.
    pub fn all_children(&self, mut visit: impl FnMut(&ScalarExpression) -> bool) -> bool {
        match self {
            Self::Case {
                operand,
                branches,
                otherwise,
            } => operand
                .iter()
                .map(Box::as_ref)
                .chain(
                    branches
                        .iter()
                        .flat_map(|(condition, result)| [condition, result]),
                )
                .chain(otherwise.iter().map(Box::as_ref))
                .all(visit),
            Self::Binary { left, right, .. } | Self::VectorDistance { left, right, .. } => {
                visit(left) && visit(right)
            }
            Self::Not(expression)
            | Self::IsNull { expression, .. }
            | Self::Lower(expression)
            | Self::Left { expression, .. }
            | Self::JsonValue { expression, .. }
            | Self::PropertyAccess { expression, .. } => visit(expression),
            Self::Coalesce(expressions)
            | Self::List(expressions)
            | Self::ListFunction {
                arguments: expressions,
                ..
            }
            | Self::TemporalFunction {
                arguments: expressions,
                ..
            } => expressions.iter().all(visit),
            Self::MapLiteral(entries) => entries.iter().all(|(_, value)| visit(value)),
            Self::ListComprehension {
                list,
                predicate,
                projection,
                ..
            } => [Some(list), predicate.as_ref(), projection.as_ref()]
                .into_iter()
                .flatten()
                .map(Box::as_ref)
                .all(visit),
            Self::ListIndex { list, index } => visit(list) && visit(index),
            Self::ListSlice { list, start, end } => [Some(list), start.as_ref(), end.as_ref()]
                .into_iter()
                .flatten()
                .map(Box::as_ref)
                .all(visit),
            _ => true,
        }
    }
}
