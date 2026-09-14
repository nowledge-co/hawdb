pub use skein_expression::sql::{
    Expr, ExprKind, SqlColumnRef, SqlComparisonOp, SqlExpression, SqlFunctionArgument,
    SqlIndexColumn, SqlLikeEscape, SqlNullOrder, SqlOrderDirection, SqlOrderItem, SqlPredicate,
    SqlSourceLocation, SqlSourceSpan, SqlValue,
};

use caseless::Caseless;
use skein_core::{LogicalType, Result, SkeinError};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SqlStatement {
    Explain(SqlExplainStatement),
    Select(SelectStatement),
    Insert(InsertStatement),
    Update(UpdateStatement),
    Delete(DeleteStatement),
    CreateTable(CreateTableStatement),
    CreateIndex(CreateIndexStatement),
    AlterTableAddColumn(AlterTableAddColumnStatement),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SqlExplainStatement {
    pub analyze: bool,
    pub statement: Box<SqlStatement>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SelectStatement {
    pub projection: Vec<SelectProjection>,
    pub distinct: bool,
    pub from: SqlTableName,
    pub from_alias: Option<String>,
    pub joins: Vec<SqlJoin>,
    pub selection: Option<SqlPredicate>,
    pub group_by: Vec<SqlColumnRef>,
    pub having: Option<SqlPredicate>,
    pub order_by: Vec<SqlOrderItem>,
    pub limit: Option<SqlBound>,
    pub offset: Option<SqlBound>,
    pub lock_strength: Option<SqlLockStrength>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SqlLockStrength {
    Share,
    Update,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InsertStatement {
    pub table: SqlTableName,
    pub columns: Vec<String>,
    pub rows: Vec<Vec<SqlValue>>,
    pub on_conflict: Option<SqlOnConflict>,
    pub returning: Vec<SqlColumnRef>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UpdateStatement {
    pub table: SqlTableName,
    pub alias: Option<String>,
    pub assignments: Vec<SqlAssignment>,
    pub selection: Option<SqlPredicate>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeleteStatement {
    pub table: SqlTableName,
    pub alias: Option<String>,
    pub selection: Option<SqlPredicate>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SqlAssignment {
    pub column: String,
    pub value: SqlAssignmentValue,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SqlAssignmentValue {
    Value(SqlValue),
    Column(SqlColumnRef),
    Arithmetic {
        left: SqlArithmeticOperand,
        operator: SqlArithmeticOperator,
        right: SqlArithmeticOperand,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SqlArithmeticOperand {
    Value(SqlValue),
    Column(SqlColumnRef),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SqlArithmeticOperator {
    Add,
    Subtract,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SqlOnConflict {
    pub columns: Vec<String>,
    pub action: SqlConflictAction,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SqlConflictAction {
    DoNothing,
    DoUpdate(Vec<SqlAssignment>),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreateTableStatement {
    pub table: SqlTableName,
    pub if_not_exists: bool,
    pub columns: Vec<SqlColumnDefinition>,
    pub constraints: Vec<SqlTableConstraint>,
    pub storage: SqlTableStorage,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub enum SqlTableStorage {
    #[default]
    RowPage,
    StrictAppend {
        partition_key: Vec<String>,
        order_key: Vec<String>,
        generated_order: SqlGeneratedOrder,
    },
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum SqlGeneratedOrder {
    #[default]
    CallerProvided,
    CommitSequence,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SqlColumnDefinition {
    pub name: String,
    pub data_type: SqlDataType,
    pub nullable: bool,
    pub default: Option<SqlColumnDefault>,
    pub primary_key: bool,
    pub unique: bool,
    pub references: Option<SqlForeignKeyReference>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SqlColumnDefault {
    Literal(SqlValue),
    UuidV7,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SqlDataType {
    Boolean,
    BigInt,
    DoublePrecision,
    Text,
    Bytea,
    Uuid,
}

impl SqlDataType {
    pub const fn logical_type(self) -> LogicalType {
        match self {
            Self::Boolean => LogicalType::Boolean,
            Self::BigInt => LogicalType::Int64,
            Self::DoublePrecision => LogicalType::Float64,
            Self::Text => LogicalType::Text,
            Self::Bytea => LogicalType::Binary,
            Self::Uuid => LogicalType::Uuid,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SqlTableConstraint {
    PrimaryKey(Vec<String>),
    Unique(Vec<String>),
    ForeignKey {
        columns: Vec<String>,
        reference: SqlForeignKeyReference,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SqlForeignKeyReference {
    pub table: SqlTableName,
    pub columns: Vec<String>,
    pub on_delete: SqlReferentialAction,
    pub on_update: SqlReferentialAction,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SqlReferentialAction {
    NoAction,
    Restrict,
    Cascade,
    SetNull,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreateIndexStatement {
    pub name: String,
    pub table: SqlTableName,
    pub columns: Vec<SqlIndexColumn>,
    pub unique: bool,
    pub if_not_exists: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AlterTableAddColumnStatement {
    pub table: SqlTableName,
    pub if_not_exists: bool,
    pub column: SqlColumnDefinition,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SqlBound {
    Literal(u64),
    Parameter(usize),
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct SqlTableName {
    pub schema: Option<String>,
    pub name: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SelectProjection {
    Wildcard,
    Expression {
        expression: SqlExpression,
        alias: Option<String>,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SqlJoin {
    pub kind: SqlJoinKind,
    pub table: SqlTableName,
    pub alias: Option<String>,
    pub on: SqlPredicate,
    /// First input visible to ON, with the SELECT base relation at ordinal zero.
    /// The current join's right input is the inclusive end of this scope.
    pub on_scope_start: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SqlJoinKind {
    Inner,
    Left,
}

/// Matches SQL LIKE patterns with locale-independent Unicode default case folding for ILIKE.
pub fn sql_like_matches(
    value: &str,
    pattern: &str,
    escape: SqlLikeEscape,
    case_insensitive: bool,
) -> Result<bool> {
    let tokens = tokenize_like_pattern(pattern, escape, case_insensitive)?;
    let value = value.chars().collect::<Vec<_>>();

    let mut value_index = 0;
    let mut pattern_index = 0;
    let mut wildcard_index = None;
    let mut wildcard_value_index = 0;
    while value_index < value.len() {
        let matched = match tokens.get(pattern_index) {
            Some(LikeToken::Literal(expected)) => {
                if let Some(consumed) =
                    match_like_literal(&value[value_index..], expected, case_insensitive)
                {
                    value_index += consumed;
                    pattern_index += 1;
                    true
                } else {
                    false
                }
            }
            Some(LikeToken::One) => {
                value_index += 1;
                pattern_index += 1;
                true
            }
            Some(LikeToken::Many) => {
                wildcard_index = Some(pattern_index);
                wildcard_value_index = value_index;
                pattern_index += 1;
                true
            }
            None => false,
        };
        if matched {
            continue;
        }
        let Some(wildcard_index) = wildcard_index else {
            return Ok(false);
        };
        wildcard_value_index += 1;
        value_index = wildcard_value_index;
        pattern_index = wildcard_index + 1;
    }
    while matches!(tokens.get(pattern_index), Some(LikeToken::Many)) {
        pattern_index += 1;
    }
    Ok(pattern_index == tokens.len())
}

#[derive(Debug, Clone)]
enum LikeToken {
    Literal(String),
    One,
    Many,
}

fn tokenize_like_pattern(
    pattern: &str,
    escape: SqlLikeEscape,
    case_insensitive: bool,
) -> Result<Vec<LikeToken>> {
    let escape = match escape {
        SqlLikeEscape::Character(character) => Some(character),
        SqlLikeEscape::Disabled => None,
    };
    let mut tokens = Vec::with_capacity(pattern.chars().count());
    let mut characters = pattern.chars();
    while let Some(character) = characters.next() {
        if Some(character) == escape {
            let escaped = characters.next().ok_or_else(|| {
                SkeinError::Semantic("LIKE pattern ends with its escape character".to_string())
            })?;
            push_like_literal(&mut tokens, escaped, case_insensitive);
            continue;
        }
        match character {
            '%' if !matches!(tokens.last(), Some(LikeToken::Many)) => tokens.push(LikeToken::Many),
            '%' => {}
            '_' => tokens.push(LikeToken::One),
            literal => push_like_literal(&mut tokens, literal, case_insensitive),
        }
    }
    Ok(tokens)
}

fn push_like_literal(tokens: &mut Vec<LikeToken>, literal: char, case_insensitive: bool) {
    let literal = if case_insensitive {
        std::iter::once(literal).default_case_fold().collect()
    } else {
        literal.to_string()
    };
    if let Some(LikeToken::Literal(previous)) = tokens.last_mut() {
        previous.push_str(&literal);
    } else {
        tokens.push(LikeToken::Literal(literal));
    }
}

fn match_like_literal(value: &[char], expected: &str, case_insensitive: bool) -> Option<usize> {
    let mut actual = String::new();
    for (index, character) in value.iter().copied().enumerate() {
        if case_insensitive {
            actual.extend(std::iter::once(character).default_case_fold());
        } else {
            actual.push(character);
        }
        if !expected.starts_with(&actual) {
            return None;
        }
        if actual == expected {
            return Some(index + 1);
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn postgres_data_types_use_shared_logical_types() {
        assert_eq!(SqlDataType::Boolean.logical_type(), LogicalType::Boolean);
        assert_eq!(SqlDataType::BigInt.logical_type(), LogicalType::Int64);
        assert_eq!(
            SqlDataType::DoublePrecision.logical_type(),
            LogicalType::Float64
        );
        assert_eq!(SqlDataType::Text.logical_type(), LogicalType::Text);
        assert_eq!(SqlDataType::Bytea.logical_type(), LogicalType::Binary);
    }
}
