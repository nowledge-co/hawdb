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
            Self::Binary { left, right, .. } => visit(left) && visit(right),
            Self::Not(expression)
            | Self::IsNull { expression, .. }
            | Self::Lower(expression)
            | Self::Left { expression, .. } => visit(expression),
            Self::Coalesce(expressions) => expressions.iter().all(visit),
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
            Self::Binary { left, right, .. } => {
                visit(left)?;
                visit(right)?;
            }
            Self::Not(expression)
            | Self::IsNull { expression, .. }
            | Self::Lower(expression)
            | Self::Left { expression, .. } => visit(expression)?,
            Self::Coalesce(expressions) => {
                for expression in expressions {
                    visit(expression)?;
                }
            }
            _ => {}
        }
        Ok(())
    }
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
