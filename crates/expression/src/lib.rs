// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

pub mod sql;

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
            } => visit(expression),
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
            } => visit(expression)?,
            Self::Coalesce(expressions)
            | Self::List(expressions)
            | Self::ListFunction {
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

#[cfg(test)]
mod tests {
    use super::*;

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
            Self::JsonValue { expression, path } => hawdb_core::checked_retained_heap_sum([
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
        }
    }
}

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
