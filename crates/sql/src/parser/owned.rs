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
        if !next.is_keyword(&input[offset..], "SELECT") {
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
        syntax::RelationalStatementSyntax::Explain { analyze, select } => {
            Ok(SqlStatement::Explain(SqlExplainStatement {
                analyze,
                statement: Box::new(lowerer.select(&select)?),
            }))
        }
    }
}

pub(super) struct Lowerer<'a> {
    input: &'a str,
    lines: Vec<(usize, usize)>,
    checkpoints: Vec<(usize, usize)>,
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
    ) -> Result<(SqlTableName, Option<String>)> {
        let syntax::PostgresFromItemSyntax::Relation(relation) = item else {
            return Err(HawDBError::Semantic(
                "GRAPH_TABLE execution is not qualified".into(),
            ));
        };
        let alias = relation
            .alias
            .as_ref()
            .map(|alias| {
                if !alias.columns.is_empty() {
                    return Err(HawDBError::Semantic(
                        "PostgreSQL table column aliases are not supported".into(),
                    ));
                }
                Ok(self.ident(&alias.name))
            })
            .transpose()?;
        Ok((self.name(&relation.name)?, alias))
    }

    pub(super) fn select(&self, select: &syntax::PostgresSelectSyntax) -> Result<SqlStatement> {
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
        let vector_index = if let Some(syntax::PostgresFromSyntax {
            relation: syntax::PostgresFromItemSyntax::Relation(relation),
            ..
        }) = select.from.first()
        {
            relation
                .arguments
                .as_ref()
                .map(|arguments| self.vector_call(&relation.name, arguments).map(Box::new))
                .transpose()?
        } else {
            None
        };
        let mut joins = Vec::new();
        let mut base = None;
        for (index, from) in select.from.iter().enumerate() {
            if index > 0
                && matches!(&from.relation, syntax::PostgresFromItemSyntax::Relation(relation) if relation.arguments.is_some())
            {
                return Err(HawDBError::Semantic(
                    "PostgreSQL relational SQL supports base tables only".into(),
                ));
            }
            let relation = self.relation(&from.relation)?;
            let scope = if index == 0 {
                base = Some(relation);
                0
            } else {
                let scope = joins.len() + 1;
                joins.push(SqlJoin {
                    kind: SqlJoinKind::Inner,
                    table: relation.0,
                    alias: relation.1,
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
                let (table, alias) = self.relation(&join.relation)?;
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
        let statement = SelectStatement {
            vector_index,
            projection,
            distinct: select.distinct,
            from: base.as_ref().map(|base| base.0.clone()),
            from_alias: base.and_then(|base| base.1),
            joins,
            selection: select
                .selection
                .as_ref()
                .map(|expr| self.lower(expr, ExpressionPosition::Predicate))
                .transpose()?,
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
                .map(|item| {
                    Ok(SqlOrderItem {
                        expression: self.lower(&item.expression, ExpressionPosition::Column)?,
                        direction: match item.direction {
                            Some(syntax::OrderDirectionSyntax::Descending) => {
                                SqlOrderDirection::Desc
                            }
                            _ => SqlOrderDirection::Asc,
                        },
                        nulls: match item.nulls {
                            Some(syntax::NullOrderSyntax::First) => SqlNullOrder::First,
                            Some(syntax::NullOrderSyntax::Last) => SqlNullOrder::Last,
                            None => SqlNullOrder::DialectDefault,
                        },
                    })
                })
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
                || !statement.group_by.is_empty()
                || statement.having.is_some()
                || !statement.order_by.is_empty()
                || statement.limit.is_some()
                || statement.offset.is_some()
                || statement.lock_strength.is_some())
        {
            return Err(HawDBError::Semantic("PostgreSQL SELECT without FROM does not support WHERE, GROUP BY, HAVING, ORDER BY, LIMIT, OFFSET, or locking clauses".into()));
        }
        Ok(SqlStatement::Select(statement))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
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
        let mut count = 0;
        for line in include_str!("../../fixtures/frontend_generated_peg_baseline_v1.jsonl").lines()
        {
            let expected: serde_json::Value = serde_json::from_str(line).unwrap();
            let input = expected["sql"].as_str().unwrap();
            assert_case(input, &expected);
            count += 1;
        }
        assert_eq!(count, 489);
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

    pub(super) fn assert_case(input: &str, expected: &serde_json::Value) {
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
