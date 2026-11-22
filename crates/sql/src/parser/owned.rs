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

//! Lower the owned PEG SELECT AST without reparsing source text.
use super::*;
use hawdb_sql_syntax as syntax;

mod expression;

pub(super) fn parse(input: &str) -> Result<SqlStatement> {
    let first = syntax::first_token(input).map_err(|error| HawDBError::Parse(error.to_string()))?;
    if first.is_keyword(input, "EXPLAIN") {
        let mut offset = first.span.end;
        let mut next = syntax::first_token(&input[offset..])
            .map_err(|error| HawDBError::Parse(error.to_string()))?;
        if next.is_keyword(&input[offset..], "ANALYZE") {
            offset += next.span.end;
            next = syntax::first_token(&input[offset..])
                .map_err(|error| HawDBError::Parse(error.to_string()))?;
        }
        if !next.is_keyword(&input[offset..], "SELECT")
            && !next.is_keyword(&input[offset..], "WITH")
            && !next.is_keyword(&input[offset..], "VALUES")
        {
            return Err(HawDBError::Semantic(
                if ["UPDATE", "DELETE", "INSERT", "CREATE", "ALTER", "DROP"]
                    .iter()
                    .any(|keyword| next.is_keyword(&input[offset..], keyword))
                {
                    "EXPLAIN only supports relational SELECT"
                } else {
                    "unsupported PostgreSQL EXPLAIN option"
                }
                .into(),
            ));
        }
    }
    let statement = syntax::parse_relational_statement(input).map_err(|error| {
        if error.code == syntax::SyntaxErrorCode::InvalidParameter {
            HawDBError::Semantic("PostgreSQL parameters must use one-based $n syntax".into())
        } else {
            HawDBError::Parse(error.to_string())
        }
    })?;
    let lowerer = Lowerer::new(input);
    match statement {
        syntax::RelationalStatementSyntax::Select(select) => lowerer.select(&select),
        syntax::RelationalStatementSyntax::Query(query) => {
            Ok(SqlStatement::Query(lowerer.query(&query)?))
        }
        syntax::RelationalStatementSyntax::Explain { analyze, query } => {
            let query = lowerer.query(&query)?;
            let has_subquery_predicate = matches!(
                &query.body,
                SqlQueryBody::Select(select) if select.subquery_predicate.is_some()
            );
            let statement = if has_subquery_predicate {
                SqlStatement::Query(query)
            } else {
                match query.into_simple_select() {
                    Ok(select) => SqlStatement::Select(select),
                    Err(query) => SqlStatement::Query(*query),
                }
            };
            Ok(SqlStatement::Explain(SqlExplainStatement {
                analyze,
                statement: Box::new(statement),
            }))
        }
    }
}

pub(super) struct Lowerer<'a> {
    input: &'a str,
    lines: Vec<(usize, usize)>,
    checkpoints: Vec<(usize, usize)>,
    statement_tokens: Option<&'a [syntax::Token]>,
}

impl<'a> Lowerer<'a> {
    pub(super) fn new(input: &'a str) -> Self {
        let mut lines = vec![(0, 0)];
        let mut checkpoints = Vec::new();
        if input.is_ascii() {
            lines.extend(
                input
                    .match_indices('\n')
                    .map(|(byte, _)| (byte + 1, byte + 1)),
            );
        } else {
            checkpoints.push((0, 0));
            for (character, (byte, ch)) in input.char_indices().enumerate() {
                if byte - checkpoints.last().expect("initial checkpoint").0 >= 256 {
                    checkpoints.push((byte, character));
                }
                if ch == '\n' {
                    lines.push((byte + 1, character + 1));
                }
            }
        }
        Self {
            input,
            lines,
            checkpoints,
            statement_tokens: None,
        }
    }

    pub(super) fn for_statement(input: &'a str, tokens: &'a [syntax::Token]) -> Self {
        Self {
            statement_tokens: Some(tokens),
            ..Self::new(input)
        }
    }

    fn location(&self, byte: usize) -> SqlSourceLocation {
        let line = self.lines.partition_point(|(start, _)| *start <= byte) - 1;
        let character = if self.checkpoints.is_empty() {
            byte
        } else {
            let checkpoint = self
                .checkpoints
                .partition_point(|(start, _)| *start <= byte)
                - 1;
            let (start, character) = self.checkpoints[checkpoint];
            character + self.input[start..byte].chars().count()
        };
        SqlSourceLocation {
            line: (line + 1) as u64,
            column: (character - self.lines[line].1 + 1) as u64,
        }
    }

    fn span(&self, span: syntax::Span) -> SqlSourceSpan {
        SqlSourceSpan {
            start: self.location(span.start),
            end: self.location(span.end),
        }
    }

    fn relation(
        &self,
        item: &syntax::PostgresFromItemSyntax,
    ) -> Result<(
        SqlTableName,
        Option<String>,
        Option<SqlCommonTableExpression>,
    )> {
        match item {
            syntax::PostgresFromItemSyntax::Relation(relation) => {
                let alias = relation
                    .alias
                    .as_ref()
                    .map(|alias| {
                        if !alias.columns.is_empty() {
                            return Err(HawDBError::Semantic(
                                "PostgreSQL base-table column aliases are not supported".into(),
                            ));
                        }
                        Ok(self.ident(&alias.name))
                    })
                    .transpose()?;
                Ok((self.name(&relation.name)?, alias, None))
            }
            syntax::PostgresFromItemSyntax::Derived(derived) => {
                // Keep derived-table materialization in an internal namespace
                // that no SQL identifier can spell, including a quoted one.
                // The suffix remains source-stable for diagnostics and plans.
                let internal_name = format!(
                    "\0hawdb_derived_{}_{}",
                    derived.span.start, derived.span.end
                );
                let alias = self.ident(&derived.alias.name);
                let columns = derived
                    .alias
                    .columns
                    .iter()
                    .map(|column| self.ident(column))
                    .collect();
                Ok((
                    SqlTableName {
                        schema: None,
                        name: internal_name.clone(),
                    },
                    Some(alias),
                    Some(SqlCommonTableExpression {
                        name: internal_name,
                        columns,
                        body: SqlCommonTableExpressionBody::Query(Box::new(
                            self.query(&derived.query)?,
                        )),
                    }),
                ))
            }
            syntax::PostgresFromItemSyntax::GraphTable(_) => Err(HawDBError::Semantic(
                "GRAPH_TABLE execution is not qualified".into(),
            )),
        }
    }

    pub(super) fn select(&self, select: &syntax::PostgresSelectSyntax) -> Result<SqlStatement> {
        let (select, with) = self.select_statement(select)?;
        if with.is_empty() && select.subquery_predicate.is_none() {
            Ok(SqlStatement::Select(select))
        } else {
            Ok(SqlStatement::Query(SqlQueryStatement {
                with,
                body: SqlQueryBody::Select(Box::new(select)),
                order_by: Vec::new(),
                limit: None,
                offset: None,
                lock_strength: None,
            }))
        }
    }

    fn query(&self, query: &syntax::PostgresQuerySyntax) -> Result<SqlQueryStatement> {
        if query.locking.as_ref().is_some_and(|lock| lock.nonblocking) {
            return Err(HawDBError::Semantic(
                "FOR UPDATE/SHARE OF, NOWAIT, and SKIP LOCKED are not supported".into(),
            ));
        }
        let mut with = query
            .with
            .iter()
            .map(|cte| {
                let name = self.ident(&cte.name);
                let columns = cte
                    .columns
                    .iter()
                    .map(|column| self.ident(column))
                    .collect();
                Ok(SqlCommonTableExpression {
                    name,
                    columns,
                    body: SqlCommonTableExpressionBody::Query(Box::new(self.query(&cte.query)?)),
                })
            })
            .collect::<Result<Vec<_>>>()?;
        let (body, derived) = self.query_body(&query.body)?;
        with.extend(derived);
        Ok(SqlQueryStatement {
            with,
            body,
            order_by: query
                .order_by
                .iter()
                .map(|item| self.order_item(item))
                .collect::<Result<_>>()?,
            limit: query
                .limit
                .as_ref()
                .map(|expr| self.bound(expr))
                .transpose()?,
            offset: query
                .offset
                .as_ref()
                .map(|expr| self.bound(expr))
                .transpose()?,
            lock_strength: query.locking.as_ref().map(|lock| match lock.strength {
                syntax::LockStrengthSyntax::Share => SqlLockStrength::Share,
                syntax::LockStrengthSyntax::Update => SqlLockStrength::Update,
            }),
        })
    }

    fn query_body(
        &self,
        body: &syntax::PostgresQueryBodySyntax,
    ) -> Result<(SqlQueryBody, Vec<SqlCommonTableExpression>)> {
        Ok(match body {
            syntax::PostgresQueryBodySyntax::Select(select) => {
                let (select, derived) = self.select_statement(select)?;
                (SqlQueryBody::Select(Box::new(select)), derived)
            }
            syntax::PostgresQueryBodySyntax::Values { rows, .. } => (
                SqlQueryBody::Values(
                    rows.iter()
                        .map(|row| {
                            row.iter()
                                .map(|expression| {
                                    self.lower(expression, ExpressionPosition::Scalar)
                                })
                                .collect::<Result<Vec<_>>>()
                        })
                        .collect::<Result<Vec<_>>>()?,
                ),
                Vec::new(),
            ),
            syntax::PostgresQueryBodySyntax::Nested(query) => (
                SqlQueryBody::Nested(Box::new(self.query(query)?)),
                Vec::new(),
            ),
            syntax::PostgresQueryBodySyntax::SetOperation {
                left,
                operator,
                all,
                right,
                ..
            } => {
                let (left, mut derived) = self.query_body(left)?;
                let (right, right_derived) = self.query_body(right)?;
                derived.extend(right_derived);
                (
                    SqlQueryBody::SetOperation {
                        left: Box::new(left),
                        operator: match operator {
                            syntax::PostgresSetOperator::Union => SqlSetOperator::Union,
                            syntax::PostgresSetOperator::Intersect => SqlSetOperator::Intersect,
                            syntax::PostgresSetOperator::Except => SqlSetOperator::Except,
                        },
                        all: *all,
                        right: Box::new(right),
                    },
                    derived,
                )
            }
        })
    }

    fn order_item(&self, item: &syntax::OrderBySyntax) -> Result<SqlOrderItem> {
        Ok(SqlOrderItem {
            expression: self.lower(&item.expression, ExpressionPosition::Column)?,
            direction: match item.direction {
                Some(syntax::OrderDirectionSyntax::Descending) => SqlOrderDirection::Desc,
                _ => SqlOrderDirection::Asc,
            },
            nulls: match item.nulls {
                Some(syntax::NullOrderSyntax::First) => SqlNullOrder::First,
                Some(syntax::NullOrderSyntax::Last) => SqlNullOrder::Last,
                None => SqlNullOrder::DialectDefault,
            },
        })
    }

    fn select_statement(
        &self,
        select: &syntax::PostgresSelectSyntax,
    ) -> Result<(SelectStatement, Vec<SqlCommonTableExpression>)> {
        if select.locking.as_ref().is_some_and(|lock| lock.nonblocking) {
            return Err(HawDBError::Semantic(
                "FOR UPDATE/SHARE OF, NOWAIT, and SKIP LOCKED are not supported".into(),
            ));
        }
        let projection = select
            .projection
            .iter()
            .map(|item| {
                if matches!(
                    item.expression.kind,
                    syntax::ExpressionKindSyntax::Wildcard(None)
                ) {
                    return Ok(SelectProjection::Wildcard);
                }
                Ok(SelectProjection::Expression {
                    expression: self.lower(&item.expression, ExpressionPosition::Scalar)?,
                    alias: item.alias.as_ref().map(|alias| self.ident(alias)),
                })
            })
            .collect::<Result<_>>()?;
        let mut table_function = None;
        if let Some(syntax::PostgresFromSyntax {
            relation: syntax::PostgresFromItemSyntax::Relation(relation),
            ..
        }) = select.from.first()
            && let Some(arguments) = relation.arguments.as_ref()
        {
            table_function = Some(Box::new(if self.is_text_search_call(&relation.name)? {
                SqlTableFunction::TextSearch(self.text_search_call(arguments)?)
            } else {
                SqlTableFunction::VectorIndexSearch(self.vector_call(&relation.name, arguments)?)
            }));
        }
        let mut joins = Vec::new();
        let mut base = None;
        let mut derived = Vec::new();
        for (index, from) in select.from.iter().enumerate() {
            if index > 0
                && matches!(&from.relation, syntax::PostgresFromItemSyntax::Relation(relation) if relation.arguments.is_some())
            {
                return Err(HawDBError::Semantic(
                    "PostgreSQL relational SQL supports base tables only".into(),
                ));
            }
            let (table, alias, derived_table) = self.relation(&from.relation)?;
            derived.extend(derived_table);
            let scope = if index == 0 {
                base = Some((table, alias));
                0
            } else {
                let scope = joins.len() + 1;
                joins.push(SqlJoin {
                    kind: SqlJoinKind::Inner,
                    table,
                    alias,
                    on: crate::Expr::value(SqlValue::Literal(Value::Bool(true))),
                    on_scope_start: scope,
                });
                scope
            };
            for join in &from.joins {
                if matches!(&join.relation, syntax::PostgresFromItemSyntax::Relation(relation) if relation.arguments.is_some())
                {
                    return Err(HawDBError::Semantic(
                        "PostgreSQL relational SQL supports base tables only".into(),
                    ));
                }
                let (table, alias, derived_table) = self.relation(&join.relation)?;
                derived.extend(derived_table);
                joins.push(SqlJoin {
                    kind: match join.kind {
                        syntax::PostgresJoinKind::Left => SqlJoinKind::Left,
                        _ => SqlJoinKind::Inner,
                    },
                    table,
                    alias,
                    on: join
                        .condition
                        .as_ref()
                        .map(|expr| self.lower(expr, ExpressionPosition::Predicate))
                        .transpose()?
                        .unwrap_or_else(|| {
                            crate::Expr::value(SqlValue::Literal(Value::Bool(true)))
                        }),
                    on_scope_start: scope,
                });
            }
        }
        let (selection, subquery_predicate) = match select.selection.as_ref() {
            Some(expression) if self.contains_subquery(expression) => {
                let predicate = self.subquery_predicate(expression)?;
                let selection = self.pushdown_predicate(&predicate);
                (selection, Some(Box::new(predicate)))
            }
            Some(expression) => (
                Some(self.lower(expression, ExpressionPosition::Predicate)?),
                None,
            ),
            None => (None, None),
        };
        let statement = SelectStatement {
            table_function,
            projection,
            distinct: select.distinct,
            from: base.as_ref().map(|base| base.0.clone()),
            from_alias: base.and_then(|base| base.1),
            joins,
            selection,
            subquery_predicate,
            group_by: select
                .group_by
                .iter()
                .map(|expr| self.column(expr))
                .collect::<Result<_>>()?,
            having: select
                .having
                .as_ref()
                .map(|expr| self.lower(expr, ExpressionPosition::Having))
                .transpose()?,
            order_by: select
                .order_by
                .iter()
                .map(|item| self.order_item(item))
                .collect::<Result<_>>()?,
            limit: select
                .limit
                .as_ref()
                .map(|expr| self.bound(expr))
                .transpose()?,
            offset: select
                .offset
                .as_ref()
                .map(|expr| self.bound(expr))
                .transpose()?,
            lock_strength: select.locking.as_ref().map(|lock| match lock.strength {
                syntax::LockStrengthSyntax::Share => SqlLockStrength::Share,
                syntax::LockStrengthSyntax::Update => SqlLockStrength::Update,
            }),
        };
        if statement.from.is_none()
            && (statement.selection.is_some()
                || statement.subquery_predicate.is_some()
                || !statement.group_by.is_empty()
                || statement.having.is_some()
                || !statement.order_by.is_empty()
                || statement.limit.is_some()
                || statement.offset.is_some()
                || statement.lock_strength.is_some())
        {
            return Err(HawDBError::Semantic("PostgreSQL SELECT without FROM does not support WHERE, GROUP BY, HAVING, ORDER BY, LIMIT, OFFSET, or locking clauses".into()));
        }
        Ok((statement, derived))
    }

    fn contains_subquery(&self, expression: &syntax::ExpressionSyntax) -> bool {
        use syntax::ExpressionKindSyntax as E;
        match &expression.kind {
            E::Exists { .. } | E::InSubquery { .. } => true,
            E::Unary { expression, .. }
            | E::IsNull { expression, .. }
            | E::Cast { expression, .. }
            | E::Collate { expression, .. }
            | E::Parenthesized(expression) => self.contains_subquery(expression),
            E::Binary { left, right, .. } => {
                self.contains_subquery(left) || self.contains_subquery(right)
            }
            E::Like {
                expression,
                pattern,
                escape,
                ..
            } => {
                self.contains_subquery(expression)
                    || self.contains_subquery(pattern)
                    || escape
                        .as_deref()
                        .is_some_and(|escape| self.contains_subquery(escape))
            }
            E::Filtered {
                function,
                predicate,
            } => self.contains_subquery(function) || self.contains_subquery(predicate),
            E::InList {
                expression, values, ..
            } => {
                self.contains_subquery(expression)
                    || values.iter().any(|value| self.contains_subquery(value))
            }
            E::Row(fields) => fields.iter().any(|field| self.contains_subquery(field)),
            E::Between {
                expression,
                low,
                high,
                ..
            } => {
                self.contains_subquery(expression)
                    || self.contains_subquery(low)
                    || self.contains_subquery(high)
            }
            E::Function { arguments, .. } => arguments
                .iter()
                .any(|argument| self.contains_subquery(argument)),
            E::Column(_)
            | E::Wildcard(_)
            | E::Parameter(_)
            | E::Literal(_)
            | E::TypedString { .. } => false,
        }
    }

    fn subquery_predicate(
        &self,
        expression: &syntax::ExpressionSyntax,
    ) -> Result<SqlSubqueryPredicate> {
        use syntax::{
            BinaryOperatorSyntax as B, ExpressionKindSyntax as E, UnaryOperatorSyntax as U,
        };
        Ok(match &expression.kind {
            E::Parenthesized(inner) => self.subquery_predicate(inner)?,
            E::Unary {
                operator: U::Not,
                expression: inner,
            } if self.contains_subquery(inner) => {
                SqlSubqueryPredicate::Not(Box::new(self.subquery_predicate(inner)?))
            }
            E::Binary {
                left,
                operator: B::And,
                right,
            } => SqlSubqueryPredicate::And(
                Box::new(self.subquery_predicate(left)?),
                Box::new(self.subquery_predicate(right)?),
            ),
            E::Binary {
                left,
                operator: B::Or,
                right,
            } => SqlSubqueryPredicate::Or(
                Box::new(self.subquery_predicate(left)?),
                Box::new(self.subquery_predicate(right)?),
            ),
            E::Exists { query } => SqlSubqueryPredicate::Exists {
                query: Box::new(self.query(query)?),
            },
            E::InSubquery {
                expression: left,
                query,
                negated,
            } => {
                let fields = match &left.kind {
                    E::Row(fields) => fields.as_slice(),
                    E::Parenthesized(inner) => match &inner.kind {
                        E::Row(fields) => fields.as_slice(),
                        _ => std::slice::from_ref(inner.as_ref()),
                    },
                    _ => std::slice::from_ref(left.as_ref()),
                };
                SqlSubqueryPredicate::In {
                    left: fields
                        .iter()
                        .map(|field| self.lower(field, ExpressionPosition::PredicateScalar))
                        .collect::<Result<Vec<_>>>()?,
                    query: Box::new(self.query(query)?),
                    negated: *negated,
                }
            }
            _ if self.contains_subquery(expression) => {
                return Err(HawDBError::Semantic(
                    "subqueries are supported only as EXISTS or IN predicates combined with AND, OR, and NOT"
                        .into(),
                ));
            }
            _ => {
                SqlSubqueryPredicate::Scalar(self.lower(expression, ExpressionPosition::Predicate)?)
            }
        })
    }

    fn pushdown_predicate(&self, predicate: &SqlSubqueryPredicate) -> Option<SqlPredicate> {
        match predicate {
            SqlSubqueryPredicate::Scalar(predicate) => Some(predicate.clone()),
            SqlSubqueryPredicate::And(left, right) => {
                match (
                    self.pushdown_predicate(left),
                    self.pushdown_predicate(right),
                ) {
                    (Some(left), Some(right)) => Some(crate::Expr::unspanned(ExprKind::And(
                        Box::new(left),
                        Box::new(right),
                    ))),
                    (Some(predicate), None) | (None, Some(predicate)) => Some(predicate),
                    (None, None) => None,
                }
            }
            SqlSubqueryPredicate::Or(_, _)
            | SqlSubqueryPredicate::Not(_)
            | SqlSubqueryPredicate::Exists { .. }
            | SqlSubqueryPredicate::In { .. } => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn values_derived_tables_and_subqueries_lower_into_owned_query_ir() {
        let prepared = crate::prepare_postgres_sql(
            "SELECT input.id FROM (VALUES ($1), ($2)) input(id) \
             WHERE input.id IN (SELECT id FROM numbers WHERE n = $3) \
                OR EXISTS (SELECT 1 FROM numbers WHERE id = input.id)",
        )
        .unwrap();
        assert_eq!(
            prepared
                .parameters
                .iter()
                .map(|parameter| parameter.position)
                .collect::<Vec<_>>(),
            vec![1, 2, 3]
        );
        let SqlStatement::Query(query) = prepared.statement else {
            panic!("derived-table subquery must use statement-scoped query IR");
        };
        assert_eq!(query.with.len(), 1);
        assert!(query.with[0].name.starts_with("\0hawdb_derived_"));
        let SqlQueryBody::Select(select) = query.body else {
            panic!("outer query must remain a SELECT");
        };
        assert!(matches!(
            select.subquery_predicate.as_deref(),
            Some(SqlSubqueryPredicate::Or(left, right))
                if matches!(left.as_ref(), SqlSubqueryPredicate::In { .. })
                    && matches!(right.as_ref(), SqlSubqueryPredicate::Exists { .. })
        ));
    }

    #[test]
    fn non_select_statement_bodies_preserve_independent_input_limits() {
        let payload = "x".repeat(syntax::LexerConfig::default().max_input_bytes + 1);
        {
            let input = format!("-- dispatch only\nINSERT INTO items (id) VALUES ('{payload}')");
            let SqlStatement::Insert(insert) = parse_postgres_sql(&input).unwrap() else {
                panic!("large single-literal INSERT must remain accepted");
            };
            assert_eq!(insert.rows.len(), 1);
            assert!(
                matches!(&insert.rows[0][0], SqlValue::Literal(Value::String(value)) if value == &payload)
            );
        }
        let ddl = format!("CREATE TABLE items (id BIGINT) /* {payload} */");
        assert!(matches!(
            parse_postgres_sql(&ddl).unwrap(),
            SqlStatement::CreateTable(_)
        ));
        for prefix in ["SELECT", "EXPLAIN SELECT"] {
            let input = format!("{prefix} '{payload}'");
            let HawDBError::Parse(error) = parse_postgres_sql(&input).unwrap_err() else {
                panic!("owned parser must retain its full-input byte limit");
            };
            assert!(error.contains("InputTooLarge"), "{error}");
        }
    }

    #[test]
    fn default_select_errors_retain_peg_limits_and_do_not_poison_next_parse() {
        let expression = format!("{}id{}", "(".repeat(129), ")".repeat(129));
        for prefix in ["SELECT", "EXPLAIN SELECT"] {
            let input = format!("{prefix} {expression} FROM items");
            let HawDBError::Parse(error) = parse_postgres_sql(&input).unwrap_err() else {
                panic!("language nesting must fail during the selected PEG parse");
            };
            assert!(error.contains("ExpressionNestingLimitExceeded"), "{error}");
        }
        assert!(matches!(
            parse_postgres_sql("SELECT id FROM items").unwrap(),
            SqlStatement::Select(_)
        ));
        // DDL remains explicitly dispatched; a new SELECT lexer extension cannot
        // accidentally become a prerequisite for the other statement families.
        assert!(parse_postgres_sql("CREATE TABLE items (id TEXT DEFAULT E'line\\n')").is_ok());
    }
    #[test]
    #[ignore = "manual parser-only timing diagnostic; use the repository release profile"]
    fn parser_timing_diagnostic() {
        println!("shape,sample,frontend,iterations,nanos");
        for line in include_str!("../../fixtures/frontend_timing_peg_baseline_v1.jsonl").lines() {
            let expected: serde_json::Value = serde_json::from_str(line).unwrap();
            let input = expected["sql"].as_str().unwrap();
            let shape = expected["shape"].as_str().unwrap();
            let iterations = expected["iterations"].as_u64().unwrap();
            super::peg_baseline::assert_case(input, &expected);
            for sample in 0..9 {
                let started = std::time::Instant::now();
                for _ in 0..iterations {
                    drop(std::hint::black_box(parse_postgres_sql(
                        std::hint::black_box(input),
                    )));
                }
                println!(
                    "{shape},{sample},peg,{iterations},{}",
                    started.elapsed().as_nanos()
                );
            }
        }
    }
}

#[cfg(test)]
mod peg_baseline {
    use super::super::parse_postgres_sql;
    use hawdb_core::HawDBError;

    // Captured from main 0f611b6dec. Hash complete Debug IR, including locations.
    #[test]
    fn frozen_peg_baseline_preserves_ast_spans_and_rejection_classes() {
        let cases: std::collections::BTreeMap<_, _> =
            include_str!("../../fixtures/frontend_corpus_v1.jsonl")
                .lines()
                .map(|line| {
                    let case: serde_json::Value = serde_json::from_str(line).unwrap();
                    (
                        case["id"].as_str().unwrap().to_owned(),
                        case["sql"].as_str().unwrap().to_owned(),
                    )
                })
                .collect();
        let mut count = 0;
        for line in include_str!("../../fixtures/frontend_peg_baseline_v1.jsonl").lines() {
            let expected: serde_json::Value = serde_json::from_str(line).unwrap();
            let input = &cases[expected["id"].as_str().unwrap()];
            assert_case(input, &expected);
            count += 1;
        }
        assert_eq!(count, 1009);
    }
    #[test]
    fn frozen_literal_decoding_and_contributing_locations() {
        // Exported from independent main 9c355e9a46, before deleting its
        // tokenizer/AST conversion. Includes ASCII byte escape admission,
        // Unicode, escaped NUL refusal, UTF-8 locations and partial spans.
        let mut count = 0;
        for line in include_str!("../../fixtures/frontend_literals_peg_baseline_v1.jsonl").lines() {
            let expected: serde_json::Value = serde_json::from_str(line).unwrap();
            assert_case(expected["sql"].as_str().unwrap(), &expected);
            count += 1;
        }
        assert_eq!(count, 102);
    }

    #[test]
    fn frozen_generated_select_and_mutation_boundaries() {
        // Main #1474 intentionally accepts INTEGER and implicit INSERT columns.
        // Retain the historical fixture and verify only its six sourced changes.
        let changes: std::collections::BTreeMap<_, _> =
            include_str!("../../fixtures/frontend_main_1474_peg_baseline_v1.jsonl")
                .lines()
                .map(|line| serde_json::from_str::<serde_json::Value>(line).unwrap())
                .filter_map(|case| {
                    case["replaces"]
                        .as_str()
                        .map(str::to_owned)
                        .map(|id| (id, case))
                })
                .collect();
        assert_eq!(changes.len(), 6);
        let mut replacements = 0;
        let mut count = 0;
        for line in include_str!("../../fixtures/frontend_generated_peg_baseline_v1.jsonl").lines()
        {
            let expected: serde_json::Value = serde_json::from_str(line).unwrap();
            let input = expected["sql"].as_str().unwrap();
            if let Some(current) = changes.get(expected["id"].as_str().unwrap()) {
                assert_eq!(current["sql"], expected["sql"]);
                assert_eq!(current["previous_kind"], expected["kind"]);
                assert_eq!(current["previous_ast_sha256"], expected["ast_sha256"]);
                assert_case(input, current);
                replacements += 1;
            } else {
                assert_case(input, &expected);
            }
            count += 1;
        }
        assert_eq!(count, 489);
        assert_eq!(replacements, 6);
    }

    #[test]
    fn frozen_main_1474_compatibility_preserves_complete_ir_and_rejection_classes() {
        let mut count = 0;
        for line in include_str!("../../fixtures/frontend_main_1474_peg_baseline_v1.jsonl").lines()
        {
            let expected: serde_json::Value = serde_json::from_str(line).unwrap();
            assert_eq!(
                expected["source_commit"],
                "f3e5ecd01514b2703bf2a00fffbaeb3d97ba9399"
            );
            assert_case(expected["sql"].as_str().unwrap(), &expected);
            count += 1;
        }
        assert_eq!(count, 522);
    }

    #[test]
    fn frozen_timing_shapes_preserve_complete_ir() {
        let mut count = 0;
        for line in include_str!("../../fixtures/frontend_timing_peg_baseline_v1.jsonl").lines() {
            let expected: serde_json::Value = serde_json::from_str(line).unwrap();
            assert_case(expected["sql"].as_str().unwrap(), &expected);
            count += 1;
        }
        assert_eq!(count, 8);
    }

    #[test]
    fn frozen_main_1514_numeric_contracts_preserve_complete_ir_and_rejection_classes() {
        let mut count = 0;
        for line in
            include_str!("../../fixtures/frontend_main_1514_numeric_peg_baseline_v1.jsonl").lines()
        {
            let expected: serde_json::Value = serde_json::from_str(line).unwrap();
            assert_eq!(
                expected["source_commit"],
                "fd10958dd37b8878151aeb73ff0477d189b5db5e"
            );
            assert_case(expected["sql"].as_str().unwrap(), &expected);
            count += 1;
        }
        assert_eq!(count, 92);
    }

    #[test]
    fn frozen_main_statement_cast_aliases_preserve_ir_and_rejection_boundaries() {
        let mut count = 0;
        for line in
            include_str!("../../fixtures/frontend_main_1514_numeric_alias_peg_baseline_v1.jsonl")
                .lines()
        {
            let expected: serde_json::Value = serde_json::from_str(line).unwrap();
            assert_eq!(
                expected["source_commit"],
                "fd10958dd37b8878151aeb73ff0477d189b5db5e"
            );
            assert_case(expected["sql"].as_str().unwrap(), &expected);
            count += 1;
        }
        assert_eq!(count, 1005);
    }

    #[test]
    fn frozen_main_1873_covering_index_transitions_are_source_bound() {
        let mut count = 0;
        for line in
            include_str!("../../fixtures/frontend_main_1873_covering_index_peg_baseline_v1.jsonl")
                .lines()
        {
            let expected: serde_json::Value = serde_json::from_str(line).unwrap();
            assert_eq!(
                expected["source_commit"],
                "058085fa2d67cad32cdb47116226e7a0bf5a545d"
            );
            assert_case(expected["sql"].as_str().unwrap(), &expected);
            count += 1;
        }
        assert_eq!(count, 15);
    }

    #[test]
    fn frozen_main_1872_partial_index_transitions_are_source_bound() {
        let mut count = 0;
        for line in
            include_str!("../../fixtures/frontend_main_1872_partial_index_peg_baseline_v1.jsonl")
                .lines()
        {
            let expected: serde_json::Value = serde_json::from_str(line).unwrap();
            assert_eq!(
                expected["source_commit"],
                "ebc2c9c4aa2eb737d785876eefed14432e82f0ed"
            );
            assert_case(expected["sql"].as_str().unwrap(), &expected);
            count += 1;
        }
        assert_eq!(count, 16);
    }

    #[test]
    fn frozen_main_1862_mutation_returning_transitions_are_source_bound() {
        let mut count = 0;
        for line in include_str!(
            "../../fixtures/frontend_main_1862_mutation_returning_peg_baseline_v1.jsonl"
        )
        .lines()
        {
            let expected: serde_json::Value = serde_json::from_str(line).unwrap();
            assert_eq!(
                expected["source_commit"],
                "258fd7ac543680ef1909e225383ab796809a244a"
            );
            assert_case(expected["sql"].as_str().unwrap(), &expected);
            count += 1;
        }
        assert_eq!(count, 6);
    }

    pub(super) fn assert_case(input: &str, expected: &serde_json::Value) {
        use std::sync::OnceLock;
        type Transitions = std::collections::BTreeMap<String, Vec<serde_json::Value>>;
        static TRANSITIONS: OnceLock<Transitions> = OnceLock::new();
        let transitions = TRANSITIONS.get_or_init(|| {
            let mut transitions = Transitions::new();
            for line in [
                include_str!("../../fixtures/frontend_main_1514_numeric_peg_baseline_v1.jsonl"),
                include_str!(
                    "../../fixtures/frontend_main_1873_covering_index_peg_baseline_v1.jsonl"
                ),
                include_str!(
                    "../../fixtures/frontend_main_1872_partial_index_peg_baseline_v1.jsonl"
                ),
                include_str!(
                    "../../fixtures/frontend_main_1862_mutation_returning_peg_baseline_v1.jsonl"
                ),
            ]
            .into_iter()
            .flat_map(str::lines)
            {
                let case: serde_json::Value = serde_json::from_str(line).unwrap();
                let input = case["sql"].as_str().unwrap();
                let history = transitions.entry(input.to_owned()).or_default();
                if let Some(prior) = history.last() {
                    assert_eq!(case["previous_kind"], prior["kind"], "{input}");
                    assert_eq!(case["previous_ast_sha256"], prior["ast_sha256"], "{input}");
                }
                history.push(case);
            }
            transitions
        });
        let expected = if let Some(history) = transitions.get(input) {
            assert!(
                history.iter().any(|step| {
                    (step["previous_kind"] == expected["kind"]
                        && step["previous_ast_sha256"] == expected["ast_sha256"])
                        || (step["kind"] == expected["kind"]
                            && step["ast_sha256"] == expected["ast_sha256"])
                }),
                "unrecognized baseline for {input}"
            );
            history.last().unwrap()
        } else {
            expected
        };
        let (kind, digest) = match parse_postgres_sql(input) {
            Ok(ast) => (
                "accept",
                Some(crate::test_support::sha256_hex(
                    format!("{ast:?}").as_bytes(),
                )),
            ),
            Err(HawDBError::Parse(_)) => ("Parse", None),
            Err(HawDBError::Semantic(_)) => ("Semantic", None),
            other => panic!("unexpected baseline result: {other:?}"),
        };
        assert_eq!(
            kind,
            expected["kind"].as_str().unwrap(),
            "{}: {input}",
            expected["id"]
        );
        assert_eq!(
            digest.as_deref(),
            expected["ast_sha256"].as_str(),
            "{}: {input}",
            expected["id"]
        );
    }
}
