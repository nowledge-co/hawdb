// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

mod graph_match;
mod predicate_variables;
pub mod sql;

pub use graph_match::*;
pub use predicate_variables::predicate_variables;

use hawdb_core::{RelationshipDirection, ValidatedRegex, Value};

mod null_rejection;

pub use null_rejection::{
    prove_null_rejecting, BindingId, BindingSet, BoundPredicate, BoundScalarExpression,
    NullRejectionProof, ScalarNullability, TruthSet, TruthValue,
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Predicate {
    And(Vec<Predicate>),
    Or(Vec<Predicate>),
    Not(Box<Predicate>),
    ConstantBool(bool),
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
        value: Value,
    },
    IdNotEq {
        variable: String,
        value: Value,
    },
    IdCompare {
        variable: String,
        op: ComparisonOp,
        value: Value,
    },
    IdIn {
        variable: String,
        values: Vec<Value>,
    },
    PropertyEq {
        variable: String,
        property: String,
        value: Value,
    },
    PropertyNotEq {
        variable: String,
        property: String,
        value: Value,
    },
    PropertyCompare {
        variable: String,
        property: String,
        op: ComparisonOp,
        value: Value,
    },
    ExpressionEq {
        expression: ProjectionExpression,
        value: ProjectionExpression,
    },
    ExpressionNotEq {
        expression: ProjectionExpression,
        value: ProjectionExpression,
    },
    ExpressionCompare {
        expression: ProjectionExpression,
        op: ComparisonOp,
        value: ProjectionExpression,
    },
    ExpressionContains {
        expression: ProjectionExpression,
        value: ProjectionExpression,
    },
    ExpressionRegexMatch {
        expression: ProjectionExpression,
        pattern: ValidatedRegex,
    },
    ExpressionIn {
        expression: ProjectionExpression,
        values: Vec<Value>,
    },
    PropertyListContains {
        variable: String,
        property: String,
        value: Value,
    },
    PropertyListContainsLower {
        variable: String,
        property: String,
        value: String,
    },
    PropertyContains {
        variable: String,
        property: String,
        value: String,
    },
    PropertyStartsWith {
        variable: String,
        property: String,
        value: String,
    },
    PropertyEndsWith {
        variable: String,
        property: String,
        value: String,
    },
    PropertyRegexMatch {
        variable: String,
        property: String,
        pattern: ValidatedRegex,
    },
    PropertyIsNull {
        variable: String,
        property: String,
    },
    PropertyIsNotNull {
        variable: String,
        property: String,
    },
    PropertyIn {
        variable: String,
        property: String,
        values: Vec<Value>,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ComparisonOp {
    Lt,
    Lte,
    Gt,
    Gte,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DatePart {
    Year,
    Month,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ProjectionExpression {
    Variable {
        variable: String,
    },
    Property {
        variable: String,
        property: String,
    },
    Id {
        variable: String,
    },
    RelationshipType {
        variable: String,
    },
    Literal(Value),
    Coalesce(Vec<ProjectionExpression>),
    VectorDistance {
        metric: hawdb_core::VectorDistance,
        left: Box<ProjectionExpression>,
        right: Box<ProjectionExpression>,
    },
    JsonValue {
        expression: Box<ProjectionExpression>,
        path: String,
        null_on_non_scalar: bool,
    },
    Left {
        expression: Box<ProjectionExpression>,
        length: usize,
    },
    Lower(Box<ProjectionExpression>),
    DatePart {
        part: DatePart,
        variable: String,
        property: String,
    },
    DefaultIfNullOrEq {
        variable: String,
        property: String,
        empty: Value,
        default: Value,
    },
    DefaultIfNull {
        variable: String,
        property: String,
        default: Value,
    },
    CasePropertyNotNullOrEq {
        variable: String,
        property: String,
        empty: Value,
        non_empty: Value,
        null_or_empty: Value,
    },
    CasePropertyEqualsRank {
        variable: String,
        property: String,
        branches: Vec<(Value, Value)>,
        default: Value,
    },
    CaseLowerPropertyDefault {
        variable: String,
        property: String,
        default: Value,
    },
    CaseCoalesceDifferenceFloorZero {
        variable: String,
        terms: Vec<CoalesceDifferenceProjectionTerm>,
    },
    Case {
        operand: Option<Box<ProjectionExpression>>,
        branches: Vec<(ProjectionExpression, ProjectionExpression)>,
        otherwise: Option<Box<ProjectionExpression>>,
    },
    Binary {
        left: Box<ProjectionExpression>,
        op: ScalarBinaryOp,
        right: Box<ProjectionExpression>,
    },
    Not(Box<ProjectionExpression>),
    IsNull {
        expression: Box<ProjectionExpression>,
        negated: bool,
    },
    CaseEntitySearchRank(Box<CaseEntitySearchRankProjection>),
    CaseColumnSearchRank(Box<CaseColumnSearchRankProjection>),
    ColumnDefaultIfNullOrEq {
        column: String,
        property: String,
        empty: Value,
        default: Value,
    },
    ColumnValueDefaultIfNull {
        column: String,
        default: Value,
    },
    ColumnValueCasePropertyNotNullOrEq {
        column: String,
        empty: Value,
        non_empty: Value,
        null_or_empty: Value,
    },
    Column(String),
    ColumnProperty {
        column: String,
        property: String,
    },
    /// A list literal whose elements are evaluated per row, in order.
    List(Vec<ProjectionExpression>),
    /// `size`, `head`, `last`, `tail`, `reverse` or `range`.
    /// `toInteger`, `toFloat`, `toBoolean` or `toString`.
    Conversion {
        function: hawdb_core::ConversionFunction,
        argument: Box<ProjectionExpression>,
    },
    ListFunction {
        function: hawdb_core::ListFunction,
        arguments: Vec<ProjectionExpression>,
    },

    StringFunction {
        function: hawdb_core::StringFunction,
        arguments: Vec<ProjectionExpression>,
        /// Invariant literal/parameter pattern, compiled once per execution.
        pattern: Option<Box<ValidatedRegex>>,
    },
    /// `[variable IN list WHERE predicate | projection]`.
    ListComprehension(Box<ListComprehensionProjection>),
    /// The element bound by an enclosing list comprehension, optionally
    /// followed by a map-key lookup (`x.key`). It reads no row column.
    LocalVariable {
        name: String,
        property: Option<String>,
    },
    /// `list[index]`; negative indexes count from the end.
    ListIndex {
        list: Box<ProjectionExpression>,
        index: Box<ProjectionExpression>,
    },
    /// `list[start..end]`; omitted bounds select from the start or to the end.
    ListSlice {
        list: Box<ProjectionExpression>,
        start: Option<Box<ProjectionExpression>>,
        end: Option<Box<ProjectionExpression>>,
    },
    /// A path function over a path value bound by a shortest-path pattern.
    /// A NULL path, such as an unmatched OPTIONAL MATCH, yields NULL.
    Path {
        function: PathFunction,
        column: String,
    },
    /// An openCypher temporal function evaluated per row. Calls that read
    /// the statement clock are bound to literals during planning.
    TemporalFunction {
        function: hawdb_core::TemporalFunction,
        arguments: Vec<ProjectionExpression>,
    },
    /// `expression.key`: a temporal component or a map key.
    PropertyAccess {
        expression: Box<ProjectionExpression>,
        key: String,
    },
    /// `{key: expression, ...}`; keys are unique.
    MapLiteral(Vec<(String, ProjectionExpression)>),
    /// `abs`, `ceil`, `floor`, `round`, `sign` or `sqrt`.
    NumericFunction {
        function: hawdb_core::NumericFunction,
        argument: Box<ProjectionExpression>,
    },
    /// `reduce(accumulator = initial, variable IN list | expression)`.
    Reduce(Box<ReduceProjection>),
    /// `startNode(r)` or `endNode(r)`: the stored source or target node of
    /// the relationship bound to `variable`, as a node value. NULL when the
    /// variable is NULL (an unmatched OPTIONAL MATCH).
    RelationshipEndpoint {
        endpoint: hawdb_core::RelationshipEndpoint,
        variable: String,
        /// The two node variables of the single-hop pattern step that binds
        /// `variable`, in pattern order. The stored source or target is the
        /// one whose id matches; no other row variable is consulted. Planning
        /// resolves them after binding. `None` when no such step binds it,
        /// as for a relationship a pattern expression introduces: the
        /// executor then reads the node from the graph by its id.
        endpoints: Option<[String; 2]>,
    },
    /// `startNode(r)` or `endNode(r)` of a relationship value, such as an
    /// element of `relationships(p)`: the stored source or target of the
    /// stored relationship the value names, read from the graph. A value
    /// that names no stored relationship with the same endpoints fails, so a
    /// map cannot pass for a relationship. NULL for a NULL value.
    RelationshipValueEndpoint {
        endpoint: hawdb_core::RelationshipEndpoint,
        relationship: Box<ProjectionExpression>,
        /// A node visibility policy over [`ENDPOINT_VISIBILITY_VARIABLE`];
        /// an endpoint it does not admit is NULL.
        visibility: Option<Box<Predicate>>,
    },
    /// A pattern predicate or pattern comprehension: a correlated MATCH
    /// evaluated once per evaluation of the expression.
    Pattern(Box<PatternProjection>),
}

/// The variable a [`ProjectionExpression::RelationshipValueEndpoint`]
/// visibility predicate reads; no query can write the name.
pub const ENDPOINT_VISIBILITY_VARIABLE: &str = "#endpoint";

/// A pattern predicate (`projection` is `None`) or a pattern comprehension.
///
/// The executor runs `program` as a correlated MATCH over a row holding only
/// what the pattern and its body read: `graph_variables` copied from the
/// enclosing row and `arguments` evaluated in the enclosing scope. Each match
/// that satisfies `predicate` makes a predicate true, or contributes one
/// `projection` to a comprehension's list. A NULL bound variable matches
/// nothing, so the predicate is false and the list empty.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PatternProjection {
    /// Introduces the variables the pattern binds first; its imports read
    /// the `arguments` columns.
    pub program: GraphMatchProgram,
    /// Node and relationship variables of the enclosing row that the
    /// pattern or its body reads.
    pub graph_variables: Vec<String>,
    /// Enclosing values matched as graph entities, such as a list
    /// comprehension element: each import column of `program` with the
    /// enclosing-scope expression that produces it.
    pub arguments: Vec<(String, ProjectionExpression)>,
    /// Enclosing row columns that `predicate` and `projection` read, as
    /// [`ProjectionExpression::LocalVariable`].
    pub columns: Vec<String>,
    /// Evaluated over each match; only TRUE keeps it.
    pub predicate: Option<ProjectionExpression>,
    /// Evaluated over each kept match of a comprehension.
    pub projection: Option<ProjectionExpression>,
    /// Start values for a node the pattern introduces, when every disjunct
    /// of `predicate` equates it with an enclosing-row expression (#1607).
    pub seeds: Option<PatternSeeds>,
}

/// Seeds for one node variable that `PatternProjection::program`
/// introduces. `predicate` implies the variable equals one of `expressions`,
/// so matching from each distinct node among their values with `program`
/// finds exactly the matches the unseeded program accepts, without scanning.
/// `predicate` still decides every match.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PatternSeeds {
    /// The seeded node variable.
    pub variable: String,
    /// The import column of `program` that carries one seed.
    pub column: String,
    /// Enclosing-scope expressions, evaluated like `arguments`.
    pub expressions: Vec<ProjectionExpression>,
    /// The pattern with `variable` imported from `column`, starting there.
    pub program: GraphMatchProgram,
}

impl PatternProjection {
    /// Expressions in evaluation order: arguments, seeds, predicate,
    /// projection.
    pub fn expressions(&self) -> impl Iterator<Item = &ProjectionExpression> {
        self.arguments
            .iter()
            .map(|(_, expression)| expression)
            .chain(self.seeds.iter().flat_map(|seeds| &seeds.expressions))
            .chain(self.predicate.iter())
            .chain(self.projection.iter())
    }

    fn expressions_mut(&mut self) -> impl Iterator<Item = &mut ProjectionExpression> {
        self.arguments
            .iter_mut()
            .map(|(_, expression)| expression)
            .chain(
                self.seeds
                    .iter_mut()
                    .flat_map(|seeds| seeds.expressions.iter_mut()),
            )
            .chain(self.predicate.iter_mut())
            .chain(self.projection.iter_mut())
    }

    /// The pattern's programs: the unseeded one, then the seeded one.
    pub fn programs(&self) -> impl Iterator<Item = &GraphMatchProgram> {
        std::iter::once(&self.program).chain(self.seeds.iter().map(|seeds| &seeds.program))
    }

    pub fn programs_mut(&mut self) -> impl Iterator<Item = &mut GraphMatchProgram> {
        std::iter::once(&mut self.program)
            .chain(self.seeds.iter_mut().map(|seeds| &mut seeds.program))
    }
}

/// A `reduce` fold. `accumulator` and `variable` are visible only to
/// `expression`, as [`ProjectionExpression::LocalVariable`]; `initial` and
/// `list` read the enclosing scope.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReduceProjection {
    pub accumulator: String,
    pub initial: ProjectionExpression,
    pub variable: String,
    pub list: ProjectionExpression,
    pub expression: ProjectionExpression,
}

/// A list comprehension. `variable` is visible only to `predicate` and
/// `projection`, as [`ProjectionExpression::LocalVariable`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ListComprehensionProjection {
    pub variable: String,
    pub list: ProjectionExpression,
    pub predicate: Option<ProjectionExpression>,
    pub projection: Option<ProjectionExpression>,
}

/// Functions evaluated over a path value.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PathFunction {
    /// `length(p)`: the number of relationships.
    Length,
    /// `nodes(p)`: the ordered node values.
    Nodes,
    /// `relationships(p)`: the ordered relationship values.
    Relationships,
    /// `properties(nodes(p), 'key')`: one property of every node, in order.
    NodeProperties(String),
}

impl PathFunction {
    /// The Cypher spelling used for default projection names.
    pub fn display(&self, column: &str) -> String {
        match self {
            Self::Length => format!("length({column})"),
            Self::Nodes => format!("nodes({column})"),
            Self::Relationships => format!("relationships({column})"),
            Self::NodeProperties(property) => {
                format!("properties(nodes({column}), '{property}')")
            }
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ScalarBinaryOp {
    Add,
    Subtract,
    Multiply,
    Divide,
    Remainder,
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
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CaseEntitySearchRankProjection {
    pub variable: String,
    pub name_property: String,
    pub aliases_property: String,
    pub raw_query: Value,
    pub normalized_query: Value,
    pub raw_input: Value,
    pub exact_rank: Value,
    pub alias_rank: Value,
    pub fallback_rank: Value,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CaseColumnSearchRankProjection {
    pub column: String,
    pub raw_query: Value,
    pub normalized_query: Value,
    pub exact_rank: Value,
    pub contains_rank: Value,
    pub fallback_rank: Value,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CoalesceDifferenceProjectionTerm {
    pub property: String,
    pub default: Value,
}

impl ProjectionExpression {
    /// Visits immediate scalar children in evaluation order, stopping on false.
    pub fn all_children(&self, mut visit: impl FnMut(&ProjectionExpression) -> bool) -> bool {
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
            | Self::PropertyAccess { expression, .. }
            | Self::Conversion {
                argument: expression,
                ..
            }
            | Self::NumericFunction {
                argument: expression,
                ..
            } => visit(expression),
            Self::Reduce(reduce) => {
                visit(&reduce.initial) && visit(&reduce.list) && visit(&reduce.expression)
            }
            Self::Coalesce(expressions)
            | Self::List(expressions)
            | Self::ListFunction {
                arguments: expressions,
                ..
            }
            | Self::StringFunction {
                arguments: expressions,
                ..
            }
            | Self::TemporalFunction {
                arguments: expressions,
                ..
            } => expressions.iter().all(visit),
            Self::MapLiteral(entries) => entries.iter().all(|(_, value)| visit(value)),
            Self::ListComprehension(comprehension) => [
                Some(&comprehension.list),
                comprehension.predicate.as_ref(),
                comprehension.projection.as_ref(),
            ]
            .into_iter()
            .flatten()
            .all(visit),
            Self::ListIndex { list, index } => visit(list) && visit(index),
            Self::ListSlice { list, start, end } => [Some(list), start.as_ref(), end.as_ref()]
                .into_iter()
                .flatten()
                .map(Box::as_ref)
                .all(visit),
            Self::RelationshipValueEndpoint { relationship, .. } => visit(relationship),
            Self::Pattern(pattern) => pattern.expressions().all(visit),
            _ => true,
        }
    }
}

impl ProjectionExpression {
    /// Mutates immediate scalar children in evaluation order; errors are not rolled back.
    pub fn try_for_each_child_mut<E>(
        &mut self,
        mut visit: impl FnMut(&mut Self) -> Result<(), E>,
    ) -> Result<(), E> {
        match self {
            Self::Case {
                operand,
                branches,
                otherwise,
            } => {
                for expression in operand
                    .iter_mut()
                    .map(Box::as_mut)
                    .chain(
                        branches
                            .iter_mut()
                            .flat_map(|(condition, result)| [condition, result]),
                    )
                    .chain(otherwise.iter_mut().map(Box::as_mut))
                {
                    visit(expression)?;
                }
            }
            Self::Binary { left, right, .. } | Self::VectorDistance { left, right, .. } => {
                visit(left)?;
                visit(right)?;
            }
            Self::Not(expression)
            | Self::IsNull { expression, .. }
            | Self::Lower(expression)
            | Self::Left { expression, .. }
            | Self::JsonValue { expression, .. }
            | Self::PropertyAccess { expression, .. }
            | Self::Conversion {
                argument: expression,
                ..
            }
            | Self::NumericFunction {
                argument: expression,
                ..
            } => visit(expression)?,
            Self::Reduce(reduce) => {
                let ReduceProjection {
                    initial,
                    list,
                    expression,
                    ..
                } = reduce.as_mut();
                visit(initial)?;
                visit(list)?;
                visit(expression)?;
            }
            Self::Coalesce(expressions)
            | Self::List(expressions)
            | Self::ListFunction {
                arguments: expressions,
                ..
            }
            | Self::StringFunction {
                arguments: expressions,
                ..
            }
            | Self::TemporalFunction {
                arguments: expressions,
                ..
            } => {
                for expression in expressions {
                    visit(expression)?;
                }
            }
            Self::MapLiteral(entries) => {
                for (_, value) in entries {
                    visit(value)?;
                }
            }
            Self::ListComprehension(comprehension) => {
                let ListComprehensionProjection {
                    list,
                    predicate,
                    projection,
                    ..
                } = comprehension.as_mut();
                visit(list)?;
                if let Some(predicate) = predicate {
                    visit(predicate)?;
                }
                if let Some(projection) = projection {
                    visit(projection)?;
                }
            }
            Self::ListIndex { list, index } => {
                visit(list)?;
                visit(index)?;
            }
            Self::ListSlice { list, start, end } => {
                visit(list)?;
                if let Some(start) = start {
                    visit(start)?;
                }
                if let Some(end) = end {
                    visit(end)?;
                }
            }
            Self::RelationshipValueEndpoint { relationship, .. } => visit(relationship)?,
            Self::Pattern(pattern) => {
                for expression in pattern.expressions_mut() {
                    visit(expression)?;
                }
            }
            _ => {}
        }
        Ok(())
    }
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

impl SortDirection {
    /// Orders two evaluated sort-key values in this direction with openCypher
    /// `ORDER BY` semantics: `NULL` is greater than every other value, so it
    /// sorts last ascending and first descending. Non-null values keep
    /// [`Value`]'s total order.
    ///
    /// This is the ordering contract of every [`SortItem`]; it deliberately
    /// differs from `Value`'s `Ord`, which ranks `NULL` lowest for sets, maps,
    /// grouping and index keys. SQL callers that share the sort operators
    /// encode their `NULLS FIRST`/`NULLS LAST` placement into a non-null
    /// ranked key first, so this placement never applies to them.
    pub fn compare(self, left: &Value, right: &Value) -> std::cmp::Ordering {
        use std::cmp::Ordering;
        let ordering = match (left, right) {
            (Value::Null, Value::Null) => Ordering::Equal,
            (Value::Null, _) => Ordering::Greater,
            (_, Value::Null) => Ordering::Less,
            (left, right) => left.cmp(right),
        };
        match self {
            Self::Asc => ordering,
            Self::Desc => ordering.reverse(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sort_direction_places_null_like_open_cypher() {
        let mut values = vec![Value::Int(2), Value::Null, Value::Int(1), Value::Null];
        values.sort_by(|left, right| SortDirection::Asc.compare(left, right));
        assert_eq!(
            values,
            [Value::Int(1), Value::Int(2), Value::Null, Value::Null]
        );
        values.sort_by(|left, right| SortDirection::Desc.compare(left, right));
        assert_eq!(
            values,
            [Value::Null, Value::Null, Value::Int(2), Value::Int(1)]
        );
        // `Value`'s own order is unchanged: NULL stays the least value.
        assert!(Value::Null < Value::Int(i64::MIN));
    }

    #[test]
    fn expression_ir_composes_boolean_and_scalar_nodes() {
        let predicate = Predicate::ExpressionCompare {
            expression: ProjectionExpression::Property {
                variable: "memory".to_string(),
                property: "importance".to_string(),
            },
            op: ComparisonOp::Gte,
            value: ProjectionExpression::Literal(Value::Float(0.75)),
        };

        assert!(matches!(
            predicate,
            Predicate::ExpressionCompare {
                op: ComparisonOp::Gte,
                ..
            }
        ));
    }
}

// Retained query ownership: Predicate.
impl hawdb_core::RetainedHeapBytes for Predicate {
    fn retained_heap_bytes(&self) -> Option<usize> {
        match self {
            Self::And(value0) => hawdb_core::checked_retained_heap_sum([
                hawdb_core::RetainedHeapBytes::retained_heap_bytes(value0),
            ]),
            Self::Or(value0) => hawdb_core::checked_retained_heap_sum([
                hawdb_core::RetainedHeapBytes::retained_heap_bytes(value0),
            ]),
            Self::Not(value0) => hawdb_core::checked_retained_heap_sum([
                hawdb_core::RetainedHeapBytes::retained_heap_bytes(value0),
            ]),
            Self::ConstantBool(value0) => hawdb_core::checked_retained_heap_sum([
                hawdb_core::RetainedHeapBytes::retained_heap_bytes(value0),
            ]),
            Self::RelationshipExists {
                variable,
                rel_type,
                direction,
                target_label,
            } => hawdb_core::checked_retained_heap_sum([
                hawdb_core::RetainedHeapBytes::retained_heap_bytes(variable),
                hawdb_core::RetainedHeapBytes::retained_heap_bytes(rel_type),
                hawdb_core::RetainedHeapBytes::retained_heap_bytes(direction),
                hawdb_core::RetainedHeapBytes::retained_heap_bytes(target_label),
            ]),
            Self::BoundRelationshipExists {
                source_variable,
                rel_type,
                direction,
                target_variable,
            } => hawdb_core::checked_retained_heap_sum([
                hawdb_core::RetainedHeapBytes::retained_heap_bytes(source_variable),
                hawdb_core::RetainedHeapBytes::retained_heap_bytes(rel_type),
                hawdb_core::RetainedHeapBytes::retained_heap_bytes(direction),
                hawdb_core::RetainedHeapBytes::retained_heap_bytes(target_variable),
            ]),
            Self::IdEq { variable, value } => hawdb_core::checked_retained_heap_sum([
                hawdb_core::RetainedHeapBytes::retained_heap_bytes(variable),
                hawdb_core::RetainedHeapBytes::retained_heap_bytes(value),
            ]),
            Self::IdNotEq { variable, value } => hawdb_core::checked_retained_heap_sum([
                hawdb_core::RetainedHeapBytes::retained_heap_bytes(variable),
                hawdb_core::RetainedHeapBytes::retained_heap_bytes(value),
            ]),
            Self::IdCompare {
                variable,
                op,
                value,
            } => hawdb_core::checked_retained_heap_sum([
                hawdb_core::RetainedHeapBytes::retained_heap_bytes(variable),
                hawdb_core::RetainedHeapBytes::retained_heap_bytes(op),
                hawdb_core::RetainedHeapBytes::retained_heap_bytes(value),
            ]),
            Self::IdIn { variable, values } => hawdb_core::checked_retained_heap_sum([
                hawdb_core::RetainedHeapBytes::retained_heap_bytes(variable),
                hawdb_core::RetainedHeapBytes::retained_heap_bytes(values),
            ]),
            Self::PropertyEq {
                variable,
                property,
                value,
            } => hawdb_core::checked_retained_heap_sum([
                hawdb_core::RetainedHeapBytes::retained_heap_bytes(variable),
                hawdb_core::RetainedHeapBytes::retained_heap_bytes(property),
                hawdb_core::RetainedHeapBytes::retained_heap_bytes(value),
            ]),
            Self::PropertyNotEq {
                variable,
                property,
                value,
            } => hawdb_core::checked_retained_heap_sum([
                hawdb_core::RetainedHeapBytes::retained_heap_bytes(variable),
                hawdb_core::RetainedHeapBytes::retained_heap_bytes(property),
                hawdb_core::RetainedHeapBytes::retained_heap_bytes(value),
            ]),
            Self::PropertyCompare {
                variable,
                property,
                op,
                value,
            } => hawdb_core::checked_retained_heap_sum([
                hawdb_core::RetainedHeapBytes::retained_heap_bytes(variable),
                hawdb_core::RetainedHeapBytes::retained_heap_bytes(property),
                hawdb_core::RetainedHeapBytes::retained_heap_bytes(op),
                hawdb_core::RetainedHeapBytes::retained_heap_bytes(value),
            ]),
            Self::ExpressionEq { expression, value } => hawdb_core::checked_retained_heap_sum([
                hawdb_core::RetainedHeapBytes::retained_heap_bytes(expression),
                hawdb_core::RetainedHeapBytes::retained_heap_bytes(value),
            ]),
            Self::ExpressionNotEq { expression, value } => hawdb_core::checked_retained_heap_sum([
                hawdb_core::RetainedHeapBytes::retained_heap_bytes(expression),
                hawdb_core::RetainedHeapBytes::retained_heap_bytes(value),
            ]),
            Self::ExpressionCompare {
                expression,
                op,
                value,
            } => hawdb_core::checked_retained_heap_sum([
                hawdb_core::RetainedHeapBytes::retained_heap_bytes(expression),
                hawdb_core::RetainedHeapBytes::retained_heap_bytes(op),
                hawdb_core::RetainedHeapBytes::retained_heap_bytes(value),
            ]),
            Self::ExpressionContains { expression, value } => {
                hawdb_core::checked_retained_heap_sum([
                    hawdb_core::RetainedHeapBytes::retained_heap_bytes(expression),
                    hawdb_core::RetainedHeapBytes::retained_heap_bytes(value),
                ])
            }
            Self::ExpressionRegexMatch {
                expression,
                pattern,
            } => hawdb_core::checked_retained_heap_sum([
                hawdb_core::RetainedHeapBytes::retained_heap_bytes(expression),
                hawdb_core::RetainedHeapBytes::retained_heap_bytes(pattern),
            ]),
            Self::ExpressionIn { expression, values } => hawdb_core::checked_retained_heap_sum([
                hawdb_core::RetainedHeapBytes::retained_heap_bytes(expression),
                hawdb_core::RetainedHeapBytes::retained_heap_bytes(values),
            ]),
            Self::PropertyListContains {
                variable,
                property,
                value,
            } => hawdb_core::checked_retained_heap_sum([
                hawdb_core::RetainedHeapBytes::retained_heap_bytes(variable),
                hawdb_core::RetainedHeapBytes::retained_heap_bytes(property),
                hawdb_core::RetainedHeapBytes::retained_heap_bytes(value),
            ]),
            Self::PropertyListContainsLower {
                variable,
                property,
                value,
            } => hawdb_core::checked_retained_heap_sum([
                hawdb_core::RetainedHeapBytes::retained_heap_bytes(variable),
                hawdb_core::RetainedHeapBytes::retained_heap_bytes(property),
                hawdb_core::RetainedHeapBytes::retained_heap_bytes(value),
            ]),
            Self::PropertyContains {
                variable,
                property,
                value,
            } => hawdb_core::checked_retained_heap_sum([
                hawdb_core::RetainedHeapBytes::retained_heap_bytes(variable),
                hawdb_core::RetainedHeapBytes::retained_heap_bytes(property),
                hawdb_core::RetainedHeapBytes::retained_heap_bytes(value),
            ]),
            Self::PropertyStartsWith {
                variable,
                property,
                value,
            } => hawdb_core::checked_retained_heap_sum([
                hawdb_core::RetainedHeapBytes::retained_heap_bytes(variable),
                hawdb_core::RetainedHeapBytes::retained_heap_bytes(property),
                hawdb_core::RetainedHeapBytes::retained_heap_bytes(value),
            ]),
            Self::PropertyEndsWith {
                variable,
                property,
                value,
            } => hawdb_core::checked_retained_heap_sum([
                hawdb_core::RetainedHeapBytes::retained_heap_bytes(variable),
                hawdb_core::RetainedHeapBytes::retained_heap_bytes(property),
                hawdb_core::RetainedHeapBytes::retained_heap_bytes(value),
            ]),
            Self::PropertyRegexMatch {
                variable,
                property,
                pattern,
            } => hawdb_core::checked_retained_heap_sum([
                hawdb_core::RetainedHeapBytes::retained_heap_bytes(variable),
                hawdb_core::RetainedHeapBytes::retained_heap_bytes(property),
                hawdb_core::RetainedHeapBytes::retained_heap_bytes(pattern),
            ]),
            Self::PropertyIsNull { variable, property } => hawdb_core::checked_retained_heap_sum([
                hawdb_core::RetainedHeapBytes::retained_heap_bytes(variable),
                hawdb_core::RetainedHeapBytes::retained_heap_bytes(property),
            ]),
            Self::PropertyIsNotNull { variable, property } => {
                hawdb_core::checked_retained_heap_sum([
                    hawdb_core::RetainedHeapBytes::retained_heap_bytes(variable),
                    hawdb_core::RetainedHeapBytes::retained_heap_bytes(property),
                ])
            }
            Self::PropertyIn {
                variable,
                property,
                values,
            } => hawdb_core::checked_retained_heap_sum([
                hawdb_core::RetainedHeapBytes::retained_heap_bytes(variable),
                hawdb_core::RetainedHeapBytes::retained_heap_bytes(property),
                hawdb_core::RetainedHeapBytes::retained_heap_bytes(values),
            ]),
        }
    }
}

// Retained query ownership: ProjectionExpression.
impl hawdb_core::RetainedHeapBytes for ProjectionExpression {
    fn retained_heap_bytes(&self) -> Option<usize> {
        match self {
            Self::Variable { variable } => hawdb_core::checked_retained_heap_sum([
                hawdb_core::RetainedHeapBytes::retained_heap_bytes(variable),
            ]),
            Self::Property { variable, property } => hawdb_core::checked_retained_heap_sum([
                hawdb_core::RetainedHeapBytes::retained_heap_bytes(variable),
                hawdb_core::RetainedHeapBytes::retained_heap_bytes(property),
            ]),
            Self::Id { variable } => hawdb_core::checked_retained_heap_sum([
                hawdb_core::RetainedHeapBytes::retained_heap_bytes(variable),
            ]),
            Self::RelationshipType { variable } => hawdb_core::checked_retained_heap_sum([
                hawdb_core::RetainedHeapBytes::retained_heap_bytes(variable),
            ]),
            Self::Literal(value0) => hawdb_core::checked_retained_heap_sum([
                hawdb_core::RetainedHeapBytes::retained_heap_bytes(value0),
            ]),
            Self::Coalesce(value0) | Self::List(value0) => hawdb_core::checked_retained_heap_sum([
                hawdb_core::RetainedHeapBytes::retained_heap_bytes(value0),
            ]),
            Self::TemporalFunction {
                function,
                arguments,
            } => hawdb_core::checked_retained_heap_sum([
                hawdb_core::RetainedHeapBytes::retained_heap_bytes(function),
                hawdb_core::RetainedHeapBytes::retained_heap_bytes(arguments),
            ]),
            Self::PropertyAccess { expression, key } => hawdb_core::checked_retained_heap_sum([
                hawdb_core::RetainedHeapBytes::retained_heap_bytes(expression),
                hawdb_core::RetainedHeapBytes::retained_heap_bytes(key),
            ]),
            Self::MapLiteral(entries) => {
                hawdb_core::RetainedHeapBytes::retained_heap_bytes(entries)
            }
            Self::VectorDistance {
                metric,
                left,
                right,
            } => hawdb_core::checked_retained_heap_sum([
                hawdb_core::RetainedHeapBytes::retained_heap_bytes(metric),
                hawdb_core::RetainedHeapBytes::retained_heap_bytes(left),
                hawdb_core::RetainedHeapBytes::retained_heap_bytes(right),
            ]),
            Self::JsonValue {
                expression, path, ..
            } => hawdb_core::checked_retained_heap_sum([
                hawdb_core::RetainedHeapBytes::retained_heap_bytes(expression),
                hawdb_core::RetainedHeapBytes::retained_heap_bytes(path),
            ]),
            Self::Left { expression, length } => hawdb_core::checked_retained_heap_sum([
                hawdb_core::RetainedHeapBytes::retained_heap_bytes(expression),
                hawdb_core::RetainedHeapBytes::retained_heap_bytes(length),
            ]),
            Self::Lower(value0) => hawdb_core::checked_retained_heap_sum([
                hawdb_core::RetainedHeapBytes::retained_heap_bytes(value0),
            ]),
            Self::DatePart {
                part,
                variable,
                property,
            } => hawdb_core::checked_retained_heap_sum([
                hawdb_core::RetainedHeapBytes::retained_heap_bytes(part),
                hawdb_core::RetainedHeapBytes::retained_heap_bytes(variable),
                hawdb_core::RetainedHeapBytes::retained_heap_bytes(property),
            ]),
            Self::DefaultIfNullOrEq {
                variable,
                property,
                empty,
                default,
            } => hawdb_core::checked_retained_heap_sum([
                hawdb_core::RetainedHeapBytes::retained_heap_bytes(variable),
                hawdb_core::RetainedHeapBytes::retained_heap_bytes(property),
                hawdb_core::RetainedHeapBytes::retained_heap_bytes(empty),
                hawdb_core::RetainedHeapBytes::retained_heap_bytes(default),
            ]),
            Self::DefaultIfNull {
                variable,
                property,
                default,
            } => hawdb_core::checked_retained_heap_sum([
                hawdb_core::RetainedHeapBytes::retained_heap_bytes(variable),
                hawdb_core::RetainedHeapBytes::retained_heap_bytes(property),
                hawdb_core::RetainedHeapBytes::retained_heap_bytes(default),
            ]),
            Self::CasePropertyNotNullOrEq {
                variable,
                property,
                empty,
                non_empty,
                null_or_empty,
            } => hawdb_core::checked_retained_heap_sum([
                hawdb_core::RetainedHeapBytes::retained_heap_bytes(variable),
                hawdb_core::RetainedHeapBytes::retained_heap_bytes(property),
                hawdb_core::RetainedHeapBytes::retained_heap_bytes(empty),
                hawdb_core::RetainedHeapBytes::retained_heap_bytes(non_empty),
                hawdb_core::RetainedHeapBytes::retained_heap_bytes(null_or_empty),
            ]),
            Self::CasePropertyEqualsRank {
                variable,
                property,
                branches,
                default,
            } => hawdb_core::checked_retained_heap_sum([
                hawdb_core::RetainedHeapBytes::retained_heap_bytes(variable),
                hawdb_core::RetainedHeapBytes::retained_heap_bytes(property),
                hawdb_core::RetainedHeapBytes::retained_heap_bytes(branches),
                hawdb_core::RetainedHeapBytes::retained_heap_bytes(default),
            ]),
            Self::CaseLowerPropertyDefault {
                variable,
                property,
                default,
            } => hawdb_core::checked_retained_heap_sum([
                hawdb_core::RetainedHeapBytes::retained_heap_bytes(variable),
                hawdb_core::RetainedHeapBytes::retained_heap_bytes(property),
                hawdb_core::RetainedHeapBytes::retained_heap_bytes(default),
            ]),
            Self::CaseCoalesceDifferenceFloorZero { variable, terms } => {
                hawdb_core::checked_retained_heap_sum([
                    hawdb_core::RetainedHeapBytes::retained_heap_bytes(variable),
                    hawdb_core::RetainedHeapBytes::retained_heap_bytes(terms),
                ])
            }
            Self::Case {
                operand,
                branches,
                otherwise,
            } => hawdb_core::checked_retained_heap_sum([
                hawdb_core::RetainedHeapBytes::retained_heap_bytes(operand),
                hawdb_core::RetainedHeapBytes::retained_heap_bytes(branches),
                hawdb_core::RetainedHeapBytes::retained_heap_bytes(otherwise),
            ]),
            Self::Binary { left, op, right } => hawdb_core::checked_retained_heap_sum([
                hawdb_core::RetainedHeapBytes::retained_heap_bytes(left),
                hawdb_core::RetainedHeapBytes::retained_heap_bytes(op),
                hawdb_core::RetainedHeapBytes::retained_heap_bytes(right),
            ]),
            Self::Not(value0) => hawdb_core::checked_retained_heap_sum([
                hawdb_core::RetainedHeapBytes::retained_heap_bytes(value0),
            ]),
            Self::IsNull {
                expression,
                negated,
            } => hawdb_core::checked_retained_heap_sum([
                hawdb_core::RetainedHeapBytes::retained_heap_bytes(expression),
                hawdb_core::RetainedHeapBytes::retained_heap_bytes(negated),
            ]),
            Self::CaseEntitySearchRank(value0) => hawdb_core::checked_retained_heap_sum([
                hawdb_core::RetainedHeapBytes::retained_heap_bytes(value0),
            ]),
            Self::CaseColumnSearchRank(value0) => hawdb_core::checked_retained_heap_sum([
                hawdb_core::RetainedHeapBytes::retained_heap_bytes(value0),
            ]),
            Self::ColumnDefaultIfNullOrEq {
                column,
                property,
                empty,
                default,
            } => hawdb_core::checked_retained_heap_sum([
                hawdb_core::RetainedHeapBytes::retained_heap_bytes(column),
                hawdb_core::RetainedHeapBytes::retained_heap_bytes(property),
                hawdb_core::RetainedHeapBytes::retained_heap_bytes(empty),
                hawdb_core::RetainedHeapBytes::retained_heap_bytes(default),
            ]),
            Self::ColumnValueDefaultIfNull { column, default } => {
                hawdb_core::checked_retained_heap_sum([
                    hawdb_core::RetainedHeapBytes::retained_heap_bytes(column),
                    hawdb_core::RetainedHeapBytes::retained_heap_bytes(default),
                ])
            }
            Self::ColumnValueCasePropertyNotNullOrEq {
                column,
                empty,
                non_empty,
                null_or_empty,
            } => hawdb_core::checked_retained_heap_sum([
                hawdb_core::RetainedHeapBytes::retained_heap_bytes(column),
                hawdb_core::RetainedHeapBytes::retained_heap_bytes(empty),
                hawdb_core::RetainedHeapBytes::retained_heap_bytes(non_empty),
                hawdb_core::RetainedHeapBytes::retained_heap_bytes(null_or_empty),
            ]),
            Self::Column(value0) => hawdb_core::checked_retained_heap_sum([
                hawdb_core::RetainedHeapBytes::retained_heap_bytes(value0),
            ]),
            Self::ColumnProperty { column, property } => hawdb_core::checked_retained_heap_sum([
                hawdb_core::RetainedHeapBytes::retained_heap_bytes(column),
                hawdb_core::RetainedHeapBytes::retained_heap_bytes(property),
            ]),
            Self::ListFunction {
                function,
                arguments,
            } => hawdb_core::checked_retained_heap_sum([
                hawdb_core::RetainedHeapBytes::retained_heap_bytes(function),
                hawdb_core::RetainedHeapBytes::retained_heap_bytes(arguments),
            ]),
            Self::StringFunction {
                function,
                arguments,
                pattern,
            } => hawdb_core::checked_retained_heap_sum([
                hawdb_core::RetainedHeapBytes::retained_heap_bytes(function),
                hawdb_core::RetainedHeapBytes::retained_heap_bytes(arguments),
                hawdb_core::RetainedHeapBytes::retained_heap_bytes(pattern),
            ]),
            Self::ListComprehension(comprehension) => {
                hawdb_core::RetainedHeapBytes::retained_heap_bytes(comprehension)
            }
            Self::LocalVariable { name, property } => hawdb_core::checked_retained_heap_sum([
                hawdb_core::RetainedHeapBytes::retained_heap_bytes(name),
                hawdb_core::RetainedHeapBytes::retained_heap_bytes(property),
            ]),
            Self::ListIndex { list, index } => hawdb_core::checked_retained_heap_sum([
                hawdb_core::RetainedHeapBytes::retained_heap_bytes(list),
                hawdb_core::RetainedHeapBytes::retained_heap_bytes(index),
            ]),
            Self::Conversion { function, argument } => hawdb_core::checked_retained_heap_sum([
                hawdb_core::RetainedHeapBytes::retained_heap_bytes(function),
                hawdb_core::RetainedHeapBytes::retained_heap_bytes(argument),
            ]),
            Self::ListSlice { list, start, end } => hawdb_core::checked_retained_heap_sum([
                hawdb_core::RetainedHeapBytes::retained_heap_bytes(list),
                hawdb_core::RetainedHeapBytes::retained_heap_bytes(start),
                hawdb_core::RetainedHeapBytes::retained_heap_bytes(end),
            ]),
            Self::Path { function, column } => hawdb_core::checked_retained_heap_sum([
                hawdb_core::RetainedHeapBytes::retained_heap_bytes(function),
                hawdb_core::RetainedHeapBytes::retained_heap_bytes(column),
            ]),
            Self::NumericFunction { function, argument } => {
                hawdb_core::checked_retained_heap_sum([
                    hawdb_core::RetainedHeapBytes::retained_heap_bytes(function),
                    hawdb_core::RetainedHeapBytes::retained_heap_bytes(argument),
                ])
            }
            Self::Reduce(reduce) => hawdb_core::RetainedHeapBytes::retained_heap_bytes(reduce),
            Self::RelationshipEndpoint {
                endpoint,
                variable,
                endpoints,
            } => hawdb_core::checked_retained_heap_sum([
                hawdb_core::RetainedHeapBytes::retained_heap_bytes(endpoint),
                hawdb_core::RetainedHeapBytes::retained_heap_bytes(variable),
                hawdb_core::RetainedHeapBytes::retained_heap_bytes(endpoints),
            ]),
            Self::RelationshipValueEndpoint {
                endpoint,
                relationship,
                visibility,
            } => hawdb_core::checked_retained_heap_sum([
                hawdb_core::RetainedHeapBytes::retained_heap_bytes(endpoint),
                hawdb_core::RetainedHeapBytes::retained_heap_bytes(relationship),
                hawdb_core::RetainedHeapBytes::retained_heap_bytes(visibility),
            ]),
            Self::Pattern(pattern) => hawdb_core::RetainedHeapBytes::retained_heap_bytes(pattern),
        }
    }
}

// Retained query ownership: PatternProjection.
hawdb_core::impl_retained_heap_for_struct!(PatternProjection {
    program,
    graph_variables,
    arguments,
    columns,
    predicate,
    projection,
    seeds,
});

// Retained query ownership: PatternSeeds.
hawdb_core::impl_retained_heap_for_struct!(PatternSeeds {
    variable,
    column,
    expressions,
    program,
});

// Retained query ownership: PathFunction.
impl hawdb_core::RetainedHeapBytes for PathFunction {
    fn retained_heap_bytes(&self) -> Option<usize> {
        match self {
            Self::Length | Self::Nodes | Self::Relationships => Some(0),
            Self::NodeProperties(property) => {
                hawdb_core::RetainedHeapBytes::retained_heap_bytes(property)
            }
        }
    }
}

// Retained query ownership: ListComprehensionProjection.
hawdb_core::impl_retained_heap_for_struct!(ListComprehensionProjection {
    variable,
    list,
    predicate,
    projection,
});

// Retained query ownership: ReduceProjection.
hawdb_core::impl_retained_heap_for_struct!(ReduceProjection {
    accumulator,
    initial,
    variable,
    list,
    expression,
});

// Retained query ownership: SortItem.
hawdb_core::impl_retained_heap_for_struct!(SortItem { key, direction });

// Retained query ownership: SortKey.
impl hawdb_core::RetainedHeapBytes for SortKey {
    fn retained_heap_bytes(&self) -> Option<usize> {
        match self {
            Self::Property { variable, property } => hawdb_core::checked_retained_heap_sum([
                hawdb_core::RetainedHeapBytes::retained_heap_bytes(variable),
                hawdb_core::RetainedHeapBytes::retained_heap_bytes(property),
            ]),
            Self::Id { variable } => hawdb_core::checked_retained_heap_sum([
                hawdb_core::RetainedHeapBytes::retained_heap_bytes(variable),
            ]),
            Self::Expression(value0) => hawdb_core::checked_retained_heap_sum([
                hawdb_core::RetainedHeapBytes::retained_heap_bytes(value0),
            ]),
            Self::Column(value0) => hawdb_core::checked_retained_heap_sum([
                hawdb_core::RetainedHeapBytes::retained_heap_bytes(value0),
            ]),
        }
    }
}

// Retained query ownership: SortDirection.
hawdb_core::impl_retained_heap_for_copy!(SortDirection);

// Retained query ownership: ComparisonOp.
hawdb_core::impl_retained_heap_for_copy!(ComparisonOp);

// Retained query ownership: DatePart.
hawdb_core::impl_retained_heap_for_copy!(DatePart);

// Retained query ownership: CoalesceDifferenceProjectionTerm.
hawdb_core::impl_retained_heap_for_struct!(CoalesceDifferenceProjectionTerm { property, default });

// Retained query ownership: ScalarBinaryOp.
hawdb_core::impl_retained_heap_for_copy!(ScalarBinaryOp);

// Retained query ownership: CaseEntitySearchRankProjection.
hawdb_core::impl_retained_heap_for_struct!(CaseEntitySearchRankProjection {
    variable,
    name_property,
    aliases_property,
    raw_query,
    normalized_query,
    raw_input,
    exact_rank,
    alias_rank,
    fallback_rank
});

// Retained query ownership: CaseColumnSearchRankProjection.
hawdb_core::impl_retained_heap_for_struct!(CaseColumnSearchRankProjection {
    column,
    raw_query,
    normalized_query,
    exact_rank,
    contains_rank,
    fallback_rank
});
