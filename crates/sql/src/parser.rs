use crate::ast::*;
use skein_core::{Result, SkeinError, Value};
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

use mutation::{lower_delete_statement, lower_insert_statement, lower_update_statement};
use schema::{
    lower_alter_table_statement, lower_create_index_statement, lower_create_table_statement,
};

pub fn parse_postgres_sql(input: &str) -> Result<SqlStatement> {
    let dialect = PostgreSqlDialect {};
    let statements = Parser::parse_sql(&dialect, input)
        .map_err(|error| SkeinError::Parse(format!("failed to parse PostgreSQL SQL: {error}")))?;
    let [statement] = statements.as_slice() else {
        return Err(SkeinError::Parse(
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
                return Err(SkeinError::Semantic(
                    "unsupported PostgreSQL EXPLAIN option".to_string(),
                ));
            }
            let statement = lower_statement(statement)?;
            if !matches!(statement, SqlStatement::Select(_)) {
                return Err(SkeinError::Semantic(
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
        _ => Err(SkeinError::Semantic(
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
        return Err(SkeinError::Semantic(
            "unsupported PostgreSQL SELECT clause".to_string(),
        ));
    }
    let SetExpr::Select(select) = query.body.as_ref() else {
        return Err(SkeinError::Semantic(
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
        || select.having.is_some()
        || !select.named_window.is_empty()
        || select.qualify.is_some()
        || select.value_table_mode.is_some()
    {
        return Err(SkeinError::Semantic(
            "unsupported PostgreSQL SELECT feature".to_string(),
        ));
    }
    if select.from.len() != 1 {
        return Err(SkeinError::Semantic(
            "PostgreSQL SELECT currently supports exactly one FROM item".to_string(),
        ));
    }
    let from = &select.from[0];
    let (from_name, from_alias) = lower_table_factor(&from.relation)?;

    Ok(SqlStatement::Select(SelectStatement {
        projection: lower_projection(&select.projection)?,
        distinct: lower_distinct(select.distinct.as_ref())?,
        from: from_name,
        from_alias,
        joins: from.joins.iter().map(lower_join).collect::<Result<_>>()?,
        selection: select.selection.as_ref().map(lower_predicate).transpose()?,
        group_by: lower_group_by(&select.group_by)?,
        order_by: lower_order_by(query.order_by.as_ref())?,
        limit: lower_limit(query.limit_clause.as_ref())?,
        offset: lower_offset(query.limit_clause.as_ref())?,
        lock_strength: lower_lock_strength(&query.locks)?,
    }))
}

fn lower_lock_strength(locks: &[LockClause]) -> Result<Option<SqlLockStrength>> {
    let ([] | [_]) = locks else {
        return Err(SkeinError::Semantic(
            "PostgreSQL SELECT supports at most one locking clause".to_string(),
        ));
    };
    let Some(lock) = locks.first() else {
        return Ok(None);
    };
    if lock.of.is_some() || lock.nonblock.is_some() {
        return Err(SkeinError::Semantic(
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
            ParserSelectItem::QualifiedWildcard(_, _) => Err(SkeinError::Semantic(
                "qualified wildcards are not supported".to_string(),
            )),
        })
        .collect()
}

fn lower_projection_expression(expr: &Expr, alias: Option<String>) -> Result<SelectProjection> {
    match expr {
        Expr::Identifier(_) | Expr::CompoundIdentifier(_) => Ok(SelectProjection::Column {
            name: lower_column_expr(expr)?,
            alias,
        }),
        _ => Ok(SelectProjection::Expression {
            expression: lower_sql_expression(expr)?,
            alias,
        }),
    }
}

fn lower_order_by(order_by: Option<&sqlparser::ast::OrderBy>) -> Result<Vec<SqlOrderItem>> {
    let Some(order_by) = order_by else {
        return Ok(Vec::new());
    };
    let OrderByKind::Expressions(expressions) = &order_by.kind else {
        return Err(SkeinError::Semantic(
            "ORDER BY ALL is not supported".to_string(),
        ));
    };
    expressions
        .iter()
        .map(|item| {
            Ok(SqlOrderItem {
                column: lower_column_expr(&item.expr)?,
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

pub(super) fn lower_predicate(expr: &Expr) -> Result<SqlPredicate> {
    match expr {
        Expr::BinaryOp { left, op, right } => match op {
            BinaryOperator::And => Ok(SqlPredicate::And(
                Box::new(lower_predicate(left)?),
                Box::new(lower_predicate(right)?),
            )),
            BinaryOperator::Or => Ok(SqlPredicate::Or(
                Box::new(lower_predicate(left)?),
                Box::new(lower_predicate(right)?),
            )),
            BinaryOperator::Eq
            | BinaryOperator::NotEq
            | BinaryOperator::Lt
            | BinaryOperator::LtEq
            | BinaryOperator::Gt
            | BinaryOperator::GtEq => {
                let left = lower_column_expr(left)?;
                let op = lower_comparison_op(op);
                match right.as_ref() {
                    Expr::Identifier(_) | Expr::CompoundIdentifier(_) => {
                        Ok(SqlPredicate::CompareColumns {
                            left,
                            op,
                            right: lower_column_expr(right)?,
                        })
                    }
                    _ => Ok(SqlPredicate::Compare {
                        left,
                        op,
                        right: lower_literal_expr(right)?,
                    }),
                }
            }
            _ => Err(SkeinError::Semantic(format!(
                "unsupported PostgreSQL predicate operator {op}"
            ))),
        },
        Expr::Nested(inner) => lower_predicate(inner),
        Expr::UnaryOp {
            op: sqlparser::ast::UnaryOperator::Not,
            expr,
        } => Ok(SqlPredicate::Not(Box::new(lower_predicate(expr)?))),
        Expr::InList {
            expr,
            list,
            negated,
        } => Ok(SqlPredicate::InList {
            left: lower_column_expr(expr)?,
            values: list
                .iter()
                .map(lower_literal_expr)
                .collect::<Result<Vec<_>>>()?,
            negated: *negated,
        }),
        Expr::Like {
            negated,
            any,
            expr,
            pattern,
            escape_char,
        } => lower_like_predicate(expr, pattern, *negated, *any, escape_char.as_ref(), false),
        Expr::ILike {
            negated,
            any,
            expr,
            pattern,
            escape_char,
        } => lower_like_predicate(expr, pattern, *negated, *any, escape_char.as_ref(), true),
        Expr::IsNull(expr) => Ok(SqlPredicate::IsNull {
            column: lower_column_expr(expr)?,
            negated: false,
        }),
        Expr::IsNotNull(expr) => Ok(SqlPredicate::IsNull {
            column: lower_column_expr(expr)?,
            negated: true,
        }),
        _ => Err(SkeinError::Semantic(format!(
            "unsupported PostgreSQL predicate expression {expr}"
        ))),
    }
}

fn lower_like_predicate(
    expr: &Expr,
    pattern: &Expr,
    negated: bool,
    any: bool,
    escape_char: Option<&ParserValue>,
    case_insensitive: bool,
) -> Result<SqlPredicate> {
    if any {
        return Err(SkeinError::Semantic(
            "PostgreSQL LIKE ANY is not supported".to_string(),
        ));
    }
    Ok(SqlPredicate::Like {
        left: lower_column_expr(expr)?,
        pattern: lower_literal_expr(pattern)?,
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
        return Err(SkeinError::Semantic(
            "LIKE ESCAPE must be a string literal".to_string(),
        ));
    };
    let mut characters = escape.chars();
    let Some(character) = characters.next() else {
        return Ok(SqlLikeEscape::Disabled);
    };
    if characters.next().is_some() {
        return Err(SkeinError::Semantic(
            "LIKE ESCAPE must contain at most one Unicode scalar".to_string(),
        ));
    }
    Ok(SqlLikeEscape::Character(character))
}

fn lower_distinct(distinct: Option<&Distinct>) -> Result<bool> {
    match distinct {
        None | Some(Distinct::All) => Ok(false),
        Some(Distinct::Distinct) => Ok(true),
        Some(Distinct::On(_)) => Err(SkeinError::Semantic(
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
        GroupByExpr::Expressions(_, _) | GroupByExpr::All(_) => Err(SkeinError::Semantic(
            "PostgreSQL GROUP BY modifiers and GROUP BY ALL are not supported".to_string(),
        )),
    }
}

pub(super) fn lower_table_factor(table: &TableFactor) -> Result<(SqlTableName, Option<String>)> {
    let TableFactor::Table { name, alias, .. } = table else {
        return Err(SkeinError::Semantic(
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
        return Err(SkeinError::Semantic(
            "PostgreSQL table column aliases are not supported".to_string(),
        ));
    }
    Ok(Some(normalize_ident(&alias.name)))
}

fn lower_join(join: &sqlparser::ast::Join) -> Result<SqlJoin> {
    let (table, alias) = lower_table_factor(&join.relation)?;
    let (kind, constraint) = match &join.join_operator {
        JoinOperator::Join(constraint) | JoinOperator::Inner(constraint) => {
            (SqlJoinKind::Inner, constraint)
        }
        JoinOperator::Left(constraint) | JoinOperator::LeftOuter(constraint) => {
            (SqlJoinKind::Left, constraint)
        }
        other => {
            return Err(SkeinError::Semantic(format!(
                "unsupported PostgreSQL join operator {other:?}"
            )));
        }
    };
    let JoinConstraint::On(on) = constraint else {
        return Err(SkeinError::Semantic(
            "PostgreSQL joins require an ON predicate".to_string(),
        ));
    };
    Ok(SqlJoin {
        kind,
        table,
        alias,
        on: lower_predicate(on)?,
    })
}

pub(super) fn lower_sql_expression(expr: &Expr) -> Result<SqlExpression> {
    match expr {
        Expr::Identifier(_) | Expr::CompoundIdentifier(_) => {
            Ok(SqlExpression::Column(lower_column_expr(expr)?))
        }
        Expr::Value(_) | Expr::Nested(_) | Expr::UnaryOp { .. } => {
            Ok(SqlExpression::Value(lower_literal_expr(expr)?))
        }
        Expr::Function(function) => lower_function_expression(function),
        _ => Err(SkeinError::Semantic(format!(
            "unsupported PostgreSQL projection expression {expr}"
        ))),
    }
}

fn lower_function_expression(function: &sqlparser::ast::Function) -> Result<SqlExpression> {
    if function.uses_odbc_syntax
        || !matches!(function.parameters, FunctionArguments::None)
        || function.null_treatment.is_some()
        || function.over.is_some()
        || !function.within_group.is_empty()
    {
        return Err(SkeinError::Semantic(
            "unsupported PostgreSQL function clause".to_string(),
        ));
    }
    let name_parts = object_name_parts(&function.name)?;
    let [name] = name_parts.as_slice() else {
        return Err(SkeinError::Semantic(
            "qualified PostgreSQL function names are not supported".to_string(),
        ));
    };
    let FunctionArguments::List(arguments) = &function.args else {
        return Err(SkeinError::Semantic(
            "PostgreSQL functions require an argument list".to_string(),
        ));
    };
    if !arguments.clauses.is_empty() {
        return Err(SkeinError::Semantic(
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
                lower_sql_expression(expr).map(SqlFunctionArgument::Expression)
            }
            FunctionArg::Unnamed(FunctionArgExpr::Wildcard) => Ok(SqlFunctionArgument::Wildcard),
            _ => Err(SkeinError::Semantic(
                "named and qualified-wildcard PostgreSQL function arguments are not supported"
                    .to_string(),
            )),
        })
        .collect::<Result<Vec<_>>>()?;
    let filter = function
        .filter
        .as_deref()
        .map(lower_predicate)
        .transpose()?;
    match name.as_str() {
        "count" | "sum" => Ok(SqlExpression::Function {
            name: name.clone(),
            arguments,
            distinct,
            filter,
        }),
        "max" | "coalesce" | "octet_length" | "uuidv7" if filter.is_none() => {
            Ok(SqlExpression::Function {
                name: name.clone(),
                arguments,
                distinct,
                filter,
            })
        }
        "max" | "coalesce" | "octet_length" | "uuidv7" => Err(SkeinError::Semantic(
            "FILTER is supported only for COUNT and SUM aggregates".to_string(),
        )),
        _ => Err(SkeinError::Semantic(format!(
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
        _ => Err(SkeinError::Semantic(
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
            _ => Err(SkeinError::Semantic(
                "PostgreSQL SELECT currently supports one- or two-part column names".to_string(),
            )),
        },
        _ => Err(SkeinError::Semantic(format!(
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
            value => Err(SkeinError::Semantic(format!(
                "cannot negate literal value {value}"
            ))),
        },
        _ => Err(SkeinError::Semantic(format!(
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
        _ => Err(SkeinError::Semantic(format!(
            "unsupported PostgreSQL literal {value}"
        ))),
    }
}

fn lower_number(raw: &str) -> Result<Value> {
    if raw.contains('.') {
        raw.parse::<f64>().map(Value::Float).map_err(|error| {
            SkeinError::Semantic(format!("invalid PostgreSQL number {raw}: {error}"))
        })
    } else {
        raw.parse::<i64>().map(Value::Int).map_err(|error| {
            SkeinError::Semantic(format!("invalid PostgreSQL integer {raw}: {error}"))
        })
    }
}

fn lower_nonnegative_integer_expr(expr: &Expr) -> Result<SqlBound> {
    let value = lower_literal_expr(expr)?;
    match value {
        SqlValue::Literal(Value::Int(value)) if value >= 0 => Ok(SqlBound::Literal(value as u64)),
        SqlValue::Parameter(position) => Ok(SqlBound::Parameter(position)),
        _ => Err(SkeinError::Semantic(
            "LIMIT/OFFSET must be non-negative integers or PostgreSQL parameters".to_string(),
        )),
    }
}

fn postgres_parameter_position(raw: &str) -> Result<usize> {
    let Some(raw) = raw.strip_prefix('$') else {
        return Err(SkeinError::Semantic(
            "PostgreSQL parameters must use one-based $n syntax".to_string(),
        ));
    };
    let position = raw.parse::<usize>().map_err(|_| {
        SkeinError::Semantic("PostgreSQL parameters must use one-based $n syntax".to_string())
    })?;
    if position == 0 {
        return Err(SkeinError::Semantic(
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
            _ => Err(SkeinError::Semantic(
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
