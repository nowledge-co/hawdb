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

use crate::ast::*;
use hawdb_core::{HawDBError, Result, Value};
use sqlparser::ast::Spanned;
use sqlparser::ast::{
    BinaryOperator, Distinct, DuplicateTreatment, Expr, FunctionArg, FunctionArgExpr,
    FunctionArguments, GroupByExpr, Ident, JoinConstraint, JoinOperator, LimitClause, LockClause,
    LockType, ObjectName, ObjectNamePart, OrderByKind, SelectItem as ParserSelectItem, SetExpr,
    Statement as ParserStatement, TableAlias, TableFactor, Value as ParserValue, ValueWithSpan,
};
use sqlparser::dialect::PostgreSqlDialect;
use sqlparser::parser::Parser;

mod mutation;
mod schema;

#[cfg(test)]
mod clause_tests;

fn reject_unsupported_clauses(
    statement: &'static str,
    clauses: &[(&'static str, bool)],
) -> Result<()> {
    if let Some((clause, _)) = clauses.iter().find(|(_, present)| *present) {
        return Err(HawDBError::Semantic(format!(
            "unsupported PostgreSQL {statement} clause: {clause}"
        )));
    }
    Ok(())
}

use mutation::{lower_delete_statement, lower_insert_statement, lower_update_statement};
use schema::{
    lower_alter_table_statement, lower_create_index_statement, lower_create_table_statement,
};

pub fn parse_postgres_sql(input: &str) -> Result<SqlStatement> {
    let dialect = PostgreSqlDialect {};
    let statements = Parser::parse_sql(&dialect, input)
        .map_err(|error| HawDBError::Parse(format!("failed to parse PostgreSQL SQL: {error}")))?;
    let [statement] = statements.as_slice() else {
        return Err(HawDBError::Parse(
            "expected exactly one PostgreSQL SQL statement".to_string(),
        ));
    };
    lower_statement(statement)
}

fn lower_statement(statement: &ParserStatement) -> Result<SqlStatement> {
    match statement {
        ParserStatement::Explain {
            describe_alias,
            analyze,
            verbose,
            query_plan,
            estimate,
            statement,
            format,
            options,
        } => {
            if !matches!(describe_alias, sqlparser::ast::DescribeAlias::Explain)
                || *verbose
                || *query_plan
                || *estimate
                || format.is_some()
                || options.as_ref().is_some_and(|options| !options.is_empty())
            {
                return Err(HawDBError::Semantic(
                    "unsupported PostgreSQL EXPLAIN option".to_string(),
                ));
            }
            let statement = lower_statement(statement)?;
            if !matches!(statement, SqlStatement::Select(_)) {
                return Err(HawDBError::Semantic(
                    "EXPLAIN only supports relational SELECT".to_string(),
                ));
            }
            Ok(SqlStatement::Explain(SqlExplainStatement {
                analyze: *analyze,
                statement: Box::new(statement),
            }))
        }
        ParserStatement::Query(query) => lower_select_statement(query),
        ParserStatement::Insert(insert) => lower_insert_statement(insert),
        ParserStatement::Update(update) => lower_update_statement(update),
        ParserStatement::Delete(delete) => lower_delete_statement(delete),
        ParserStatement::CreateTable(create) => lower_create_table_statement(create),
        ParserStatement::CreateIndex(create) => lower_create_index_statement(create),
        ParserStatement::AlterTable(alter) => lower_alter_table_statement(alter),
        _ => Err(HawDBError::Semantic(
            "unsupported PostgreSQL statement kind".to_string(),
        )),
    }
}

fn lower_select_statement(query: &sqlparser::ast::Query) -> Result<SqlStatement> {
    if query.with.is_some()
        || query.fetch.is_some()
        || query.for_clause.is_some()
        || query.settings.is_some()
        || query.format_clause.is_some()
        || !query.pipe_operators.is_empty()
    {
        return Err(HawDBError::Semantic(
            "unsupported PostgreSQL SELECT clause".to_string(),
        ));
    }
    let SetExpr::Select(select) = query.body.as_ref() else {
        return Err(HawDBError::Semantic(
            "set operations and nested queries are not supported".to_string(),
        ));
    };
    if select.top.is_some()
        || select.into.is_some()
        || select.prewhere.is_some()
        || !select.lateral_views.is_empty()
        || !select.connect_by.is_empty()
        || !select.cluster_by.is_empty()
        || !select.distribute_by.is_empty()
        || !select.sort_by.is_empty()
        || !select.named_window.is_empty()
        || select.qualify.is_some()
        || select.value_table_mode.is_some()
    {
        return Err(HawDBError::Semantic(
            "unsupported PostgreSQL SELECT feature".to_string(),
        ));
    }
    if select.from.is_empty() {
        return Err(HawDBError::Semantic(
            "PostgreSQL SELECT requires at least one FROM item".to_string(),
        ));
    }
    let from = &select.from[0];
    let (from_name, from_alias) = lower_table_factor(&from.relation)?;
    let projection = lower_projection(&select.projection)?;
    let distinct = lower_distinct(select.distinct.as_ref())?;
    let mut joins = from
        .joins
        .iter()
        .map(|join| lower_join(join, 0))
        .collect::<Result<Vec<_>>>()?;
    for from in &select.from[1..] {
        let on_scope_start = joins.len() + 1;
        let (table, alias) = lower_table_factor(&from.relation)?;
        joins.push(SqlJoin {
            kind: SqlJoinKind::Inner,
            table,
            alias,
            on: crate::Expr::value(SqlValue::Literal(Value::Bool(true))),
            on_scope_start,
        });
        for join in &from.joins {
            joins.push(lower_join(join, on_scope_start)?);
        }
    }

    Ok(SqlStatement::Select(SelectStatement {
        projection,
        distinct,
        from: from_name,
        from_alias,
        joins,
        selection: select
            .selection
            .as_ref()
            .map(|expr| lower_expression(expr, ExpressionPosition::Predicate))
            .transpose()?,
        group_by: lower_group_by(&select.group_by)?,
        having: select
            .having
            .as_ref()
            .map(|expression| lower_expression(expression, ExpressionPosition::Having))
            .transpose()?,
        order_by: lower_order_by(query.order_by.as_ref())?,
        limit: lower_limit(query.limit_clause.as_ref())?,
        offset: lower_offset(query.limit_clause.as_ref())?,
        lock_strength: lower_lock_strength(&query.locks)?,
    }))
}

fn lower_lock_strength(locks: &[LockClause]) -> Result<Option<SqlLockStrength>> {
    let ([] | [_]) = locks else {
        return Err(HawDBError::Semantic(
            "PostgreSQL SELECT supports at most one locking clause".to_string(),
        ));
    };
    let Some(lock) = locks.first() else {
        return Ok(None);
    };
    if lock.of.is_some() || lock.nonblock.is_some() {
        return Err(HawDBError::Semantic(
            "FOR UPDATE/SHARE OF, NOWAIT, and SKIP LOCKED are not supported".to_string(),
        ));
    }
    Ok(Some(match lock.lock_type {
        LockType::Share => SqlLockStrength::Share,
        LockType::Update => SqlLockStrength::Update,
    }))
}

fn lower_projection(items: &[ParserSelectItem]) -> Result<Vec<SelectProjection>> {
    items
        .iter()
        .map(|item| match item {
            ParserSelectItem::Wildcard(_) => Ok(SelectProjection::Wildcard),
            ParserSelectItem::UnnamedExpr(expr) => lower_projection_expression(expr, None),
            ParserSelectItem::ExprWithAlias { expr, alias } => {
                lower_projection_expression(expr, Some(normalize_ident(alias)))
            }
            ParserSelectItem::QualifiedWildcard(_, _) => Err(HawDBError::Semantic(
                "qualified wildcards are not supported".to_string(),
            )),
        })
        .collect()
}

fn lower_projection_expression(expr: &Expr, alias: Option<String>) -> Result<SelectProjection> {
    Ok(SelectProjection::Expression {
        expression: lower_expression(expr, ExpressionPosition::Scalar)?,
        alias,
    })
}

fn lower_order_by(order_by: Option<&sqlparser::ast::OrderBy>) -> Result<Vec<SqlOrderItem>> {
    let Some(order_by) = order_by else {
        return Ok(Vec::new());
    };
    let OrderByKind::Expressions(expressions) = &order_by.kind else {
        return Err(HawDBError::Semantic(
            "ORDER BY ALL is not supported".to_string(),
        ));
    };
    expressions
        .iter()
        .map(|item| {
            Ok(SqlOrderItem {
                expression: lower_expression(&item.expr, ExpressionPosition::Column)?,
                direction: match item.options.asc {
                    Some(false) => SqlOrderDirection::Desc,
                    Some(true) | None => SqlOrderDirection::Asc,
                },
                nulls: match item.options.nulls_first {
                    Some(true) => SqlNullOrder::First,
                    Some(false) => SqlNullOrder::Last,
                    None => SqlNullOrder::DialectDefault,
                },
            })
        })
        .collect()
}

fn lower_limit(limit_clause: Option<&LimitClause>) -> Result<Option<SqlBound>> {
    let Some(limit_clause) = limit_clause else {
        return Ok(None);
    };
    match limit_clause {
        LimitClause::LimitOffset { limit, .. } => limit
            .as_ref()
            .map(lower_nonnegative_integer_expr)
            .transpose(),
        LimitClause::OffsetCommaLimit { limit, .. } => {
            Ok(Some(lower_nonnegative_integer_expr(limit)?))
        }
    }
}

fn lower_offset(limit_clause: Option<&LimitClause>) -> Result<Option<SqlBound>> {
    let Some(limit_clause) = limit_clause else {
        return Ok(None);
    };
    match limit_clause {
        LimitClause::LimitOffset { offset, .. } => offset
            .as_ref()
            .map(|offset| lower_nonnegative_integer_expr(&offset.value))
            .transpose(),
        LimitClause::OffsetCommaLimit { offset, .. } => {
            Ok(Some(lower_nonnegative_integer_expr(offset)?))
        }
    }
}

#[derive(Clone, Copy)]
pub(super) enum ExpressionPosition {
    Predicate,
    Having,
    HavingScalar,
    Scalar,
    Column,
    Value,
}

// These position checks preserve the existing language surface independently of
// the shared representation. New expression shapes require separate semantics.
pub(super) fn lower_expression(expr: &Expr, position: ExpressionPosition) -> Result<crate::Expr> {
    use ExpressionPosition::{Column, Having, HavingScalar, Predicate, Scalar, Value};
    let having = matches!(position, Having | HavingScalar);
    let operand = if having { HavingScalar } else { Column };
    let lower = |expr: &Expr, position| lower_expression(expr, position).map(Box::new);
    let kind = match position {
        Column => ExprKind::Column(lower_column_expr(expr)?),
        Value => ExprKind::Value(lower_literal_expr(expr)?),
        Scalar | HavingScalar => match expr {
            Expr::Nested(inner) if having => lower_expression(inner, HavingScalar)?.kind,
            Expr::Identifier(_) | Expr::CompoundIdentifier(_) => {
                ExprKind::Column(lower_column_expr(expr)?)
            }
            Expr::Value(_) | Expr::Nested(_) | Expr::UnaryOp { .. } => {
                ExprKind::Value(lower_literal_expr(expr)?)
            }
            Expr::Function(function) => lower_function_expression(function, position)?,
            _ => {
                return Err(HawDBError::Semantic(format!(
                    "unsupported PostgreSQL projection expression {expr}"
                )))
            }
        },
        Predicate | Having => match expr {
            Expr::BinaryOp { left, op, right } => match op {
                BinaryOperator::And => {
                    ExprKind::And(lower(left, position)?, lower(right, position)?)
                }
                BinaryOperator::Or => ExprKind::Or(lower(left, position)?, lower(right, position)?),
                BinaryOperator::Eq
                | BinaryOperator::NotEq
                | BinaryOperator::Lt
                | BinaryOperator::LtEq
                | BinaryOperator::Gt
                | BinaryOperator::GtEq => ExprKind::Compare {
                    left: lower(left, operand)?,
                    op: lower_comparison_op(op),
                    right: lower(
                        right,
                        if having {
                            HavingScalar
                        } else {
                            match right.as_ref() {
                                Expr::Identifier(_) | Expr::CompoundIdentifier(_) => Column,
                                _ => Value,
                            }
                        },
                    )?,
                },
                _ => {
                    return Err(HawDBError::Semantic(format!(
                        "unsupported PostgreSQL predicate operator {op}"
                    )))
                }
            },
            Expr::Nested(inner) => lower_expression(inner, position)?.kind,
            Expr::UnaryOp {
                op: sqlparser::ast::UnaryOperator::Not,
                expr,
            } => ExprKind::Not(lower(expr, position)?),
            Expr::InList {
                expr,
                list,
                negated,
            } => ExprKind::InList {
                left: lower(expr, operand)?,
                values: list
                    .iter()
                    .map(|value| lower_expression(value, if having { HavingScalar } else { Value }))
                    .collect::<Result<_>>()?,
                negated: *negated,
            },
            Expr::Like {
                negated,
                any,
                expr,
                pattern,
                escape_char,
            } => lower_like_expression(
                expr,
                pattern,
                *negated,
                *any,
                escape_char.as_ref(),
                false,
                having,
            )?,
            Expr::ILike {
                negated,
                any,
                expr,
                pattern,
                escape_char,
            } => lower_like_expression(
                expr,
                pattern,
                *negated,
                *any,
                escape_char.as_ref(),
                true,
                having,
            )?,
            Expr::IsNull(expr) => ExprKind::IsNull {
                expression: lower(expr, operand)?,
                negated: false,
            },
            Expr::IsNotNull(expr) => ExprKind::IsNull {
                expression: lower(expr, operand)?,
                negated: true,
            },
            _ if having => lower_expression(expr, HavingScalar)?.kind,
            _ => {
                return Err(HawDBError::Semantic(format!(
                    "unsupported PostgreSQL predicate expression {expr}"
                )))
            }
        },
    };
    let span = expr.span();
    Ok(crate::Expr {
        kind,
        span: SqlSourceSpan {
            start: SqlSourceLocation {
                line: span.start.line,
                column: span.start.column,
            },
            end: SqlSourceLocation {
                line: span.end.line,
                column: span.end.column,
            },
        },
    })
}

fn lower_like_expression(
    expr: &Expr,
    pattern: &Expr,
    negated: bool,
    any: bool,
    escape_char: Option<&ParserValue>,
    case_insensitive: bool,
    having: bool,
) -> Result<ExprKind> {
    if any {
        return Err(HawDBError::Semantic(
            "PostgreSQL LIKE ANY is not supported".to_string(),
        ));
    }
    Ok(ExprKind::Like {
        left: Box::new(lower_expression(
            expr,
            if having {
                ExpressionPosition::HavingScalar
            } else {
                ExpressionPosition::Column
            },
        )?),
        pattern: Box::new(lower_expression(
            pattern,
            if having {
                ExpressionPosition::HavingScalar
            } else {
                ExpressionPosition::Value
            },
        )?),
        case_insensitive,
        negated,
        escape: lower_like_escape(escape_char)?,
    })
}

fn lower_like_escape(escape_char: Option<&ParserValue>) -> Result<SqlLikeEscape> {
    let Some(escape_char) = escape_char else {
        return Ok(SqlLikeEscape::Character('\\'));
    };
    let Some(escape) = escape_char.clone().into_string() else {
        return Err(HawDBError::Semantic(
            "LIKE ESCAPE must be a string literal".to_string(),
        ));
    };
    let mut characters = escape.chars();
    let Some(character) = characters.next() else {
        return Ok(SqlLikeEscape::Disabled);
    };
    if characters.next().is_some() {
        return Err(HawDBError::Semantic(
            "LIKE ESCAPE must contain at most one Unicode scalar".to_string(),
        ));
    }
    Ok(SqlLikeEscape::Character(character))
}

fn lower_distinct(distinct: Option<&Distinct>) -> Result<bool> {
    match distinct {
        None | Some(Distinct::All) => Ok(false),
        Some(Distinct::Distinct) => Ok(true),
        Some(Distinct::On(_)) => Err(HawDBError::Semantic(
            "PostgreSQL DISTINCT ON is not supported".to_string(),
        )),
    }
}

fn lower_group_by(group_by: &GroupByExpr) -> Result<Vec<SqlColumnRef>> {
    match group_by {
        GroupByExpr::Expressions(expressions, modifiers) if modifiers.is_empty() => expressions
            .iter()
            .map(lower_column_expr)
            .collect::<Result<Vec<_>>>(),
        GroupByExpr::Expressions(_, _) | GroupByExpr::All(_) => Err(HawDBError::Semantic(
            "PostgreSQL GROUP BY modifiers and GROUP BY ALL are not supported".to_string(),
        )),
    }
}

pub(super) fn lower_table_factor(table: &TableFactor) -> Result<(SqlTableName, Option<String>)> {
    let TableFactor::Table { name, alias, .. } = table else {
        return Err(HawDBError::Semantic(
            "PostgreSQL relational SQL supports base tables only".to_string(),
        ));
    };
    Ok((lower_table_name(name)?, lower_table_alias(alias.as_ref())?))
}

fn lower_table_alias(alias: Option<&TableAlias>) -> Result<Option<String>> {
    let Some(alias) = alias else {
        return Ok(None);
    };
    if !alias.columns.is_empty() {
        return Err(HawDBError::Semantic(
            "PostgreSQL table column aliases are not supported".to_string(),
        ));
    }
    Ok(Some(normalize_ident(&alias.name)))
}

fn lower_join(join: &sqlparser::ast::Join, on_scope_start: usize) -> Result<SqlJoin> {
    let (table, alias) = lower_table_factor(&join.relation)?;
    if matches!(
        join.join_operator,
        JoinOperator::CrossJoin(JoinConstraint::None)
    ) {
        return Ok(SqlJoin {
            kind: SqlJoinKind::Inner,
            table,
            alias,
            on: crate::Expr::value(SqlValue::Literal(Value::Bool(true))),
            on_scope_start,
        });
    }
    let (kind, constraint) = match &join.join_operator {
        JoinOperator::Join(constraint) | JoinOperator::Inner(constraint) => {
            (SqlJoinKind::Inner, constraint)
        }
        JoinOperator::Left(constraint) | JoinOperator::LeftOuter(constraint) => {
            (SqlJoinKind::Left, constraint)
        }
        other => {
            return Err(HawDBError::Semantic(format!(
                "unsupported PostgreSQL join operator {other:?}"
            )));
        }
    };
    let JoinConstraint::On(on) = constraint else {
        return Err(HawDBError::Semantic(
            "PostgreSQL joins require an ON predicate".to_string(),
        ));
    };
    Ok(SqlJoin {
        kind,
        table,
        alias,
        on: lower_expression(on, ExpressionPosition::Predicate)?,
        on_scope_start,
    })
}

fn lower_function_expression(
    function: &sqlparser::ast::Function,
    argument_position: ExpressionPosition,
) -> Result<ExprKind> {
    if function.uses_odbc_syntax
        || !matches!(function.parameters, FunctionArguments::None)
        || function.null_treatment.is_some()
        || function.over.is_some()
        || !function.within_group.is_empty()
    {
        return Err(HawDBError::Semantic(
            "unsupported PostgreSQL function clause".to_string(),
        ));
    }
    let name_parts = object_name_parts(&function.name)?;
    let [name] = name_parts.as_slice() else {
        return Err(HawDBError::Semantic(
            "qualified PostgreSQL function names are not supported".to_string(),
        ));
    };
    let FunctionArguments::List(arguments) = &function.args else {
        return Err(HawDBError::Semantic(
            "PostgreSQL functions require an argument list".to_string(),
        ));
    };
    if !arguments.clauses.is_empty() {
        return Err(HawDBError::Semantic(
            "PostgreSQL function argument clauses are not supported".to_string(),
        ));
    }
    let distinct = match arguments.duplicate_treatment {
        None | Some(DuplicateTreatment::All) => false,
        Some(DuplicateTreatment::Distinct) => true,
    };
    let arguments = arguments
        .args
        .iter()
        .map(|argument| match argument {
            FunctionArg::Unnamed(FunctionArgExpr::Expr(expr)) => {
                lower_expression(expr, argument_position).map(SqlFunctionArgument::Expression)
            }
            FunctionArg::Unnamed(FunctionArgExpr::Wildcard) => Ok(SqlFunctionArgument::Wildcard),
            _ => Err(HawDBError::Semantic(
                "named and qualified-wildcard PostgreSQL function arguments are not supported"
                    .to_string(),
            )),
        })
        .collect::<Result<Vec<_>>>()?;
    let filter = function
        .filter
        .as_deref()
        .map(|expr| lower_expression(expr, ExpressionPosition::Predicate).map(Box::new))
        .transpose()?;
    match name.as_str() {
        "count" | "sum" => Ok(ExprKind::Function {
            name: name.clone(),
            arguments,
            distinct,
            filter,
        }),
        "max" | "coalesce" | "octet_length" | "uuidv7" if filter.is_none() => {
            Ok(ExprKind::Function {
                name: name.clone(),
                arguments,
                distinct,
                filter,
            })
        }
        "max" | "coalesce" | "octet_length" | "uuidv7" => Err(HawDBError::Semantic(
            "FILTER is supported only for COUNT and SUM aggregates".to_string(),
        )),
        _ => Err(HawDBError::Semantic(format!(
            "unsupported PostgreSQL function {name}"
        ))),
    }
}

fn lower_comparison_op(op: &BinaryOperator) -> SqlComparisonOp {
    match op {
        BinaryOperator::Eq => SqlComparisonOp::Eq,
        BinaryOperator::NotEq => SqlComparisonOp::NotEq,
        BinaryOperator::Lt => SqlComparisonOp::Lt,
        BinaryOperator::LtEq => SqlComparisonOp::Lte,
        BinaryOperator::Gt => SqlComparisonOp::Gt,
        BinaryOperator::GtEq => SqlComparisonOp::Gte,
        _ => unreachable!("checked by caller"),
    }
}

pub(super) fn lower_table_name(name: &ObjectName) -> Result<SqlTableName> {
    let parts = object_name_parts(name)?;
    match parts.as_slice() {
        [name] => Ok(SqlTableName {
            schema: None,
            name: name.clone(),
        }),
        [schema, name] => Ok(SqlTableName {
            schema: Some(schema.clone()),
            name: name.clone(),
        }),
        _ => Err(HawDBError::Semantic(
            "PostgreSQL SELECT currently supports one- or two-part table names".to_string(),
        )),
    }
}

pub(super) fn lower_column_expr(expr: &Expr) -> Result<SqlColumnRef> {
    match expr {
        Expr::Identifier(ident) => Ok(SqlColumnRef {
            qualifier: None,
            name: normalize_ident(ident),
        }),
        Expr::CompoundIdentifier(parts) => match parts.as_slice() {
            [qualifier, name] => Ok(SqlColumnRef {
                qualifier: Some(normalize_ident(qualifier)),
                name: normalize_ident(name),
            }),
            _ => Err(HawDBError::Semantic(
                "PostgreSQL SELECT currently supports one- or two-part column names".to_string(),
            )),
        },
        _ => Err(HawDBError::Semantic(format!(
            "expected a column reference, got {expr}"
        ))),
    }
}

pub(super) fn lower_literal_expr(expr: &Expr) -> Result<SqlValue> {
    match expr {
        Expr::Value(value) => lower_value(value),
        Expr::Nested(inner) => lower_literal_expr(inner),
        Expr::UnaryOp {
            op: sqlparser::ast::UnaryOperator::Minus,
            expr,
        } => match lower_literal_expr(expr)? {
            SqlValue::Literal(Value::Int(value)) => Ok(SqlValue::Literal(Value::Int(-value))),
            SqlValue::Literal(Value::Float(value)) => Ok(SqlValue::Literal(Value::Float(-value))),
            value => Err(HawDBError::Semantic(format!(
                "cannot negate literal value {value}"
            ))),
        },
        _ => Err(HawDBError::Semantic(format!(
            "expected a literal value, got {expr}"
        ))),
    }
}

fn lower_value(value: &ValueWithSpan) -> Result<SqlValue> {
    match &value.value {
        ParserValue::Boolean(value) => Ok(SqlValue::Literal(Value::Bool(*value))),
        ParserValue::Null => Ok(SqlValue::Literal(Value::Null)),
        ParserValue::Number(raw, _) => lower_number(raw).map(SqlValue::Literal),
        ParserValue::SingleQuotedString(value)
        | ParserValue::DoubleQuotedString(value)
        | ParserValue::TripleSingleQuotedString(value)
        | ParserValue::TripleDoubleQuotedString(value)
        | ParserValue::EscapedStringLiteral(value)
        | ParserValue::UnicodeStringLiteral(value) => {
            Ok(SqlValue::Literal(Value::String(value.clone())))
        }
        ParserValue::Placeholder(raw) => Ok(SqlValue::Parameter(postgres_parameter_position(raw)?)),
        _ => Err(HawDBError::Semantic(format!(
            "unsupported PostgreSQL literal {value}"
        ))),
    }
}

fn lower_number(raw: &str) -> Result<Value> {
    if raw.contains('.') {
        raw.parse::<f64>().map(Value::Float).map_err(|error| {
            HawDBError::Semantic(format!("invalid PostgreSQL number {raw}: {error}"))
        })
    } else {
        raw.parse::<i64>().map(Value::Int).map_err(|error| {
            HawDBError::Semantic(format!("invalid PostgreSQL integer {raw}: {error}"))
        })
    }
}

fn lower_nonnegative_integer_expr(expr: &Expr) -> Result<SqlBound> {
    let value = lower_literal_expr(expr)?;
    match value {
        SqlValue::Literal(Value::Int(value)) if value >= 0 => Ok(SqlBound::Literal(value as u64)),
        SqlValue::Parameter(position) => Ok(SqlBound::Parameter(position)),
        _ => Err(HawDBError::Semantic(
            "LIMIT/OFFSET must be non-negative integers or PostgreSQL parameters".to_string(),
        )),
    }
}

fn postgres_parameter_position(raw: &str) -> Result<usize> {
    let Some(raw) = raw.strip_prefix('$') else {
        return Err(HawDBError::Semantic(
            "PostgreSQL parameters must use one-based $n syntax".to_string(),
        ));
    };
    let position = raw.parse::<usize>().map_err(|_| {
        HawDBError::Semantic("PostgreSQL parameters must use one-based $n syntax".to_string())
    })?;
    if position == 0 {
        return Err(HawDBError::Semantic(
            "PostgreSQL parameters are one-based".to_string(),
        ));
    }
    Ok(position)
}

pub(super) fn object_name_parts(name: &ObjectName) -> Result<Vec<String>> {
    name.0
        .iter()
        .map(|part| match part {
            ObjectNamePart::Identifier(ident) => Ok(normalize_ident(ident)),
            _ => Err(HawDBError::Semantic(
                "object name functions are not supported".to_string(),
            )),
        })
        .collect()
}

pub(super) fn normalize_ident(ident: &Ident) -> String {
    if ident.quote_style.is_some() {
        ident.value.clone()
    } else {
        ident.value.to_ascii_lowercase()
    }
}
