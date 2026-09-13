//! Shared SQL expressions consumed by the frontend and relational optimizer.
//!
//! Syntax acceptance remains the SQL frontend's responsibility. Keeping the
//! representation here lets optimizer analysis avoid depending on the parser.

use skein_core::Value;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SqlValue {
    Literal(Value),
    Parameter(usize),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SqlExpression {
    Column(SqlColumnRef),
    Value(SqlValue),
    Function {
        name: String,
        arguments: Vec<SqlFunctionArgument>,
        distinct: bool,
        filter: Option<SqlPredicate>,
    },
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

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SqlPredicate {
    And(Box<SqlPredicate>, Box<SqlPredicate>),
    Or(Box<SqlPredicate>, Box<SqlPredicate>),
    Not(Box<SqlPredicate>),
    Compare {
        left: SqlColumnRef,
        op: SqlComparisonOp,
        right: SqlValue,
    },
    CompareColumns {
        left: SqlColumnRef,
        op: SqlComparisonOp,
        right: SqlColumnRef,
    },
    InList {
        left: SqlColumnRef,
        values: Vec<SqlValue>,
        negated: bool,
    },
    Like {
        left: SqlColumnRef,
        pattern: SqlValue,
        case_insensitive: bool,
        negated: bool,
        escape: SqlLikeEscape,
    },
    IsNull {
        column: SqlColumnRef,
        negated: bool,
    },
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
