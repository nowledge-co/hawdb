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

pub use hawdb_expression::sql::{
    Expr, ExprKind, SqlColumnRef, SqlComparisonOp, SqlExpression, SqlFunctionArgument,
    SqlIndexColumn, SqlLikeEscape, SqlNullOrder, SqlOrderDirection, SqlOrderItem, SqlPredicate,
    SqlSourceLocation, SqlSourceSpan, SqlValue,
};

use caseless::Caseless;
use hawdb_core::{HawDBError, LogicalType, Result};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SqlStatement {
    Explain(SqlExplainStatement),
    Select(SelectStatement),
    Insert(InsertStatement),
    Update(UpdateStatement),
    Delete(DeleteStatement),
    CreateTable(CreateTableStatement),
    CreateIndex(CreateIndexStatement),
    CreateMaterializedView(CreateMaterializedViewStatement),
    DropMaterializedView(DropMaterializedViewStatement),
    AlterTableAddColumn(AlterTableAddColumnStatement),
    /// Project-scoped branch lifecycle commands. These are deliberately
    /// distinct from relational DDL: their executor operates on the durable
    /// branch catalog rather than a selected branch's schema.
    Branch(BranchSqlStatement),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreateMaterializedViewStatement {
    pub name: SqlTableName,
    pub if_not_exists: bool,
    pub query: Box<SelectStatement>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DropMaterializedViewStatement {
    pub name: SqlTableName,
    pub if_exists: bool,
}

/// A branch lifecycle command parsed by the PostgreSQL entrypoint.
///
/// Keeping this as an AST family makes branch operations visible to the query
/// runtime and prevents a host integration from recognizing command strings
/// itself. Lifecycle and selection commands dispatch outside ordinary implicit
/// data transactions through the storage-owned admission/publication kernel.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BranchSqlStatement {
    ShowBranches(ShowBranchesStatement),
    ShowBranch(ShowBranchStatement),
    ShowCurrentBranch,
    UseBranch(BranchSqlSelector),
    CreateBranch(CreateBranchStatement),
    DropBranch(DropBranchStatement),
}

impl BranchSqlStatement {
    pub const fn changes_context_or_catalog(&self) -> bool {
        matches!(
            self,
            Self::UseBranch(_) | Self::CreateBranch(_) | Self::DropBranch(_)
        )
    }

    pub const fn command(&self) -> &'static str {
        match self {
            Self::ShowBranches(_) => "SHOW BRANCHES",
            Self::ShowBranch(_) => "SHOW BRANCH",
            Self::ShowCurrentBranch => "SHOW CURRENT BRANCH",
            Self::UseBranch(_) => "USE BRANCH",
            Self::CreateBranch(_) => "CREATE BRANCH",
            Self::DropBranch(_) => "DROP BRANCH",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreateBranchStatement {
    pub name: Option<BranchSqlValue>,
    pub source: BranchSqlSelector,
    pub expected_source_revision: SqlBound,
    pub request_key: BranchSqlValue,
    pub owner: Option<BranchSqlValue>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DropBranchStatement {
    pub id: BranchSqlValue,
    pub expected_metadata_revision: SqlBound,
}

/// A bounded page over the durable branch catalog.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ShowBranchesStatement {
    pub limit: SqlBound,
    pub offset: Option<SqlBound>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ShowBranchStatement {
    pub selector: BranchSqlSelector,
}

/// A typed selector avoids treating UUID-looking names as identifiers. The
/// runtime validates `Id` values as UUIDs only after parameter binding.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BranchSqlSelector {
    Name(BranchSqlValue),
    Id(BranchSqlValue),
}

/// A branch name, UUID, or opaque selector parameter. Branch commands accept
/// only string literals or PostgreSQL positional parameters, never SQL
/// expression evaluation or interpolated text.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BranchSqlValue {
    Literal(String),
    Parameter(usize),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SqlExplainStatement {
    pub analyze: bool,
    pub statement: Box<SqlStatement>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SqlVectorIndexSearch {
    pub index: SqlValue,
    pub embedding: SqlValue,
    pub mode: hawdb_core::VectorIndexQueryMode,
    pub candidates: Option<SqlValue>,
    pub max_work: Option<SqlValue>,
    pub top_k: SqlValue,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SelectStatement {
    pub vector_index: Option<Box<SqlVectorIndexSearch>>,
    pub projection: Vec<SelectProjection>,
    pub distinct: bool,
    /// `None` for a FROM-less SELECT (e.g. `SELECT version();`): the
    /// projection is evaluated exactly once against an implicit row with no
    /// column bindings, so any column reference in it is rejected the same
    /// way an unresolvable column against a real table would be.
    pub from: Option<SqlTableName>,
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

impl SelectStatement {
    /// The base table, for every code path that only ever runs on a real
    /// table (catalog resolution, access-path selection, join planning, row
    /// scanning). FROM-less selects never reach these paths — they're
    /// handled by a separate, earlier branch — so `None` here is a caller
    /// bug, not a user-facing state.
    pub fn from_table(&self) -> &SqlTableName {
        self.from
            .as_ref()
            .expect("from_table called on a FROM-less SELECT; it must take the fromless path")
    }
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
    Expression(Expr),
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
    Decimal(hawdb_core::DecimalType),
    Text,
    Bytea,
    Uuid,
    Vector(hawdb_core::VectorDimension),
}

impl SqlDataType {
    pub const fn logical_type(self) -> LogicalType {
        match self {
            Self::Boolean => LogicalType::Boolean,
            Self::BigInt => LogicalType::Int64,
            Self::DoublePrecision => LogicalType::Float64,
            Self::Decimal(_) => LogicalType::Decimal,
            Self::Text => LogicalType::Text,
            Self::Bytea => LogicalType::Binary,
            Self::Uuid => LogicalType::Uuid,
            Self::Vector(dimension) => LogicalType::Vector(dimension),
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
    let layout = like_layout(pattern, escape, case_insensitive)?;
    let value_chars = value.chars().count();
    layout.scratch_bytes(value_chars)?;
    let (tokens, literals) = tokenize_like_pattern(pattern, escape, case_insensitive, &layout)?;
    let mut characters = Vec::with_capacity(value_chars);
    characters.extend(value.chars());
    let value = characters;

    let mut value_index = 0;
    let mut pattern_index = 0;
    let mut wildcard_index = None;
    let mut wildcard_value_index = 0;
    while value_index < value.len() {
        let matched = match tokens.get(pattern_index) {
            Some(LikeToken::Literal(expected)) => {
                if let Some(consumed) = match_like_literal(
                    &value[value_index..],
                    &literals[expected.clone()],
                    case_insensitive,
                ) {
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

/// Requested heap bytes used by the LIKE matcher, before allocating its buffers.
/// Escape parsing and Unicode folding share the matcher's exact layout pass.
#[doc(hidden)]
pub fn sql_like_scratch_bytes(
    value: &str,
    pattern: &str,
    escape: SqlLikeEscape,
    case_insensitive: bool,
) -> Result<usize> {
    like_layout(pattern, escape, case_insensitive)?.scratch_bytes(value.chars().count())
}

struct LikeLayout {
    tokens: usize,
    literal_bytes: usize,
}

impl LikeLayout {
    fn scratch_bytes(&self, value_chars: usize) -> Result<usize> {
        self.tokens
            .checked_mul(std::mem::size_of::<LikeToken>())
            .and_then(|bytes| bytes.checked_add(self.literal_bytes))
            .and_then(|bytes| {
                value_chars
                    .checked_mul(std::mem::size_of::<char>())
                    .and_then(|value| bytes.checked_add(value))
            })
            .ok_or_else(like_size_overflow)
    }
}

#[derive(Debug, Clone)]
enum LikeToken {
    Literal(std::ops::Range<usize>),
    One,
    Many,
}

#[derive(Clone, Copy)]
enum LikePart {
    Literal(char),
    One,
    Many,
}

fn like_size_overflow() -> HawDBError {
    HawDBError::Execution("LIKE scratch length overflow".into())
}

fn visit_like_pattern(
    pattern: &str,
    escape: SqlLikeEscape,
    mut visit: impl FnMut(LikePart) -> Result<()>,
) -> Result<()> {
    let escape = match escape {
        SqlLikeEscape::Character(character) => Some(character),
        SqlLikeEscape::Disabled => None,
    };
    let mut characters = pattern.chars();
    while let Some(character) = characters.next() {
        let part = if Some(character) == escape {
            LikePart::Literal(characters.next().ok_or_else(|| {
                HawDBError::Semantic("LIKE pattern ends with its escape character".to_string())
            })?)
        } else {
            match character {
                '%' => LikePart::Many,
                '_' => LikePart::One,
                literal => LikePart::Literal(literal),
            }
        };
        visit(part)?;
    }
    Ok(())
}

fn visit_like_literal(
    literal: char,
    case_insensitive: bool,
    mut visit: impl FnMut(char) -> Result<()>,
) -> Result<()> {
    if case_insensitive {
        for character in std::iter::once(literal).default_case_fold() {
            visit(character)?;
        }
        Ok(())
    } else {
        visit(literal)
    }
}

fn like_layout(pattern: &str, escape: SqlLikeEscape, case_insensitive: bool) -> Result<LikeLayout> {
    let mut layout = LikeLayout {
        tokens: 0,
        literal_bytes: 0,
    };
    let mut previous = None;
    visit_like_pattern(pattern, escape, |part| {
        let merged = matches!(
            (previous, part),
            (Some(LikePart::Many), LikePart::Many)
                | (Some(LikePart::Literal(_)), LikePart::Literal(_))
        );
        if !merged {
            layout.tokens = layout
                .tokens
                .checked_add(1)
                .ok_or_else(like_size_overflow)?;
        }
        if let LikePart::Literal(literal) = part {
            visit_like_literal(literal, case_insensitive, |character| {
                layout.literal_bytes = layout
                    .literal_bytes
                    .checked_add(character.len_utf8())
                    .ok_or_else(like_size_overflow)?;
                Ok(())
            })?;
        }
        previous = Some(part);
        Ok(())
    })?;
    Ok(layout)
}

fn tokenize_like_pattern(
    pattern: &str,
    escape: SqlLikeEscape,
    case_insensitive: bool,
    layout: &LikeLayout,
) -> Result<(Vec<LikeToken>, String)> {
    let mut tokens = Vec::with_capacity(layout.tokens);
    let mut literals = String::with_capacity(layout.literal_bytes);
    visit_like_pattern(pattern, escape, |part| {
        match part {
            LikePart::Literal(literal) => {
                let start = literals.len();
                visit_like_literal(literal, case_insensitive, |character| {
                    literals.push(character);
                    Ok(())
                })?;
                if let Some(LikeToken::Literal(range)) = tokens.last_mut() {
                    range.end = literals.len();
                } else {
                    tokens.push(LikeToken::Literal(start..literals.len()));
                }
            }
            LikePart::Many if !matches!(tokens.last(), Some(LikeToken::Many)) => {
                tokens.push(LikeToken::Many)
            }
            LikePart::Many => {}
            LikePart::One => tokens.push(LikeToken::One),
        }
        Ok(())
    })?;
    debug_assert_eq!(tokens.len(), layout.tokens);
    debug_assert_eq!(literals.len(), layout.literal_bytes);
    Ok((tokens, literals))
}

fn match_like_literal(value: &[char], expected: &str, case_insensitive: bool) -> Option<usize> {
    let mut expected = expected.chars().peekable();
    for (index, character) in value.iter().copied().enumerate() {
        if case_insensitive {
            for folded in std::iter::once(character).default_case_fold() {
                if expected.next() != Some(folded) {
                    return None;
                }
            }
        } else if expected.next() != Some(character) {
            return None;
        }
        // Wildcards and literal boundaries consume complete source characters,
        // even when one character folds into several code points.
        if expected.peek().is_none() {
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
