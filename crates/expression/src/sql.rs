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

//! Shared SQL expressions consumed by the frontend and relational optimizer.
//!
//! Syntax acceptance remains the SQL frontend's responsibility. Keeping the
//! representation here lets optimizer analysis avoid depending on the parser.

use hawdb_core::Value;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SqlValue {
    Literal(Value),
    Parameter(usize),
}

/// An owned SQL expression shared by predicates, projections and ordering.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Expr {
    pub kind: ExprKind,
    pub span: SqlSourceSpan,
}

/// Contributing token locations supplied by the frontend, not an exact text slice.
/// Zero locations represent expressions constructed without source information.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct SqlSourceSpan {
    pub start: SqlSourceLocation,
    pub end: SqlSourceLocation,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct SqlSourceLocation {
    pub line: u64,
    pub column: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ExprKind {
    Column(SqlColumnRef),
    Value(SqlValue),
    Function {
        name: String,
        arguments: Vec<SqlFunctionArgument>,
        distinct: bool,
        filter: Option<Box<Expr>>,
    },
    And(Box<Expr>, Box<Expr>),
    Or(Box<Expr>, Box<Expr>),
    Not(Box<Expr>),
    Compare {
        left: Box<Expr>,
        op: SqlComparisonOp,
        right: Box<Expr>,
    },
    InList {
        left: Box<Expr>,
        values: Vec<Expr>,
        negated: bool,
    },
    Like {
        left: Box<Expr>,
        pattern: Box<Expr>,
        case_insensitive: bool,
        negated: bool,
        escape: SqlLikeEscape,
    },
    IsNull {
        expression: Box<Expr>,
        negated: bool,
    },
}

/// Compatibility name for the shared expression tree.
pub type SqlExpression = Expr;
/// Compatibility name for expressions used in predicate positions.
pub type SqlPredicate = Expr;

impl Expr {
    pub fn unspanned(kind: ExprKind) -> Self {
        Self {
            kind,
            span: SqlSourceSpan::default(),
        }
    }

    pub fn column(column: SqlColumnRef) -> Self {
        Self::unspanned(ExprKind::Column(column))
    }

    pub fn value(value: SqlValue) -> Self {
        Self::unspanned(ExprKind::Value(value))
    }

    pub fn as_column(&self) -> Option<&SqlColumnRef> {
        match &self.kind {
            ExprKind::Column(column) => Some(column),
            _ => None,
        }
    }

    pub fn as_value(&self) -> Option<&SqlValue> {
        match &self.kind {
            ExprKind::Value(value) => Some(value),
            _ => None,
        }
    }

    pub fn require_column(&self) -> hawdb_core::Result<&SqlColumnRef> {
        self.as_column().ok_or_else(|| {
            hawdb_core::HawDBError::Semantic(
                "this SQL expression position requires a column reference".to_owned(),
            )
        })
    }

    pub fn require_value(&self) -> hawdb_core::Result<&SqlValue> {
        self.as_value().ok_or_else(|| {
            hawdb_core::HawDBError::Semantic(
                "this SQL expression position requires a literal or parameter".to_owned(),
            )
        })
    }

    /// Rewrites children before their parent while retaining source metadata.
    /// Changes made before a visitor error are not rolled back.
    pub fn try_visit_mut<E>(
        &mut self,
        visitor: &mut impl FnMut(&mut Self) -> std::result::Result<(), E>,
    ) -> std::result::Result<(), E> {
        match &mut self.kind {
            ExprKind::Column(_) | ExprKind::Value(_) => {}
            ExprKind::Function {
                arguments, filter, ..
            } => {
                for argument in arguments {
                    if let SqlFunctionArgument::Expression(expression) = argument {
                        expression.try_visit_mut(visitor)?;
                    }
                }
                if let Some(filter) = filter {
                    filter.try_visit_mut(visitor)?;
                }
            }
            ExprKind::And(left, right)
            | ExprKind::Or(left, right)
            | ExprKind::Compare { left, right, .. } => {
                left.try_visit_mut(visitor)?;
                right.try_visit_mut(visitor)?;
            }
            ExprKind::Not(expression) | ExprKind::IsNull { expression, .. } => {
                expression.try_visit_mut(visitor)?
            }
            ExprKind::InList { left, values, .. } => {
                left.try_visit_mut(visitor)?;
                for value in values {
                    value.try_visit_mut(visitor)?;
                }
            }
            ExprKind::Like { left, pattern, .. } => {
                left.try_visit_mut(visitor)?;
                pattern.try_visit_mut(visitor)?;
            }
        }
        visitor(self)
    }

    /// Visits every expression once in source order, including aggregate FILTER.
    pub fn visit<'a>(&'a self, visitor: &mut impl FnMut(&'a Self)) {
        visitor(self);
        match &self.kind {
            ExprKind::Column(_) | ExprKind::Value(_) => {}
            ExprKind::Function {
                arguments, filter, ..
            } => {
                for argument in arguments {
                    if let SqlFunctionArgument::Expression(expression) = argument {
                        expression.visit(visitor);
                    }
                }
                if let Some(filter) = filter {
                    filter.visit(visitor);
                }
            }
            ExprKind::And(left, right)
            | ExprKind::Or(left, right)
            | ExprKind::Compare { left, right, .. } => {
                left.visit(visitor);
                right.visit(visitor);
            }
            ExprKind::Not(expression) | ExprKind::IsNull { expression, .. } => {
                expression.visit(visitor)
            }
            ExprKind::InList { left, values, .. } => {
                left.visit(visitor);
                for value in values {
                    value.visit(visitor);
                }
            }
            ExprKind::Like { left, pattern, .. } => {
                left.visit(visitor);
                pattern.visit(visitor);
            }
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SqlFunctionArgument {
    Expression(SqlExpression),
    Wildcard,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct SqlColumnRef {
    pub qualifier: Option<String>,
    pub name: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SqlOrderItem {
    pub expression: Expr,
    pub direction: SqlOrderDirection,
    pub nulls: SqlNullOrder,
}

/// A column-only index DDL specification, independent of query order expressions.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SqlIndexColumn {
    pub column: SqlColumnRef,
    pub direction: SqlOrderDirection,
    pub nulls: SqlNullOrder,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SqlOrderDirection {
    Asc,
    Desc,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SqlNullOrder {
    DialectDefault,
    First,
    Last,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SqlLikeEscape {
    Character(char),
    Disabled,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SqlComparisonOp {
    Eq,
    NotEq,
    Lt,
    Lte,
    Gt,
    Gte,
}

impl std::fmt::Display for SqlValue {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Literal(value) => write!(formatter, "{value}"),
            Self::Parameter(position) => write!(formatter, "${position}"),
        }
    }
}
