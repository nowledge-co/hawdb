// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

//! Lower the owned PEG SELECT AST without reparsing source text.
use super::*;
use hawdb_sql_syntax as syntax;

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

struct Lowerer<'a> {
    input: &'a str,
    lines: Vec<(usize, usize)>,
    checkpoints: Vec<(usize, usize)>,
}

impl<'a> Lowerer<'a> {
    fn new(input: &'a str) -> Self {
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

    fn location(&self, byte: usize) -> sqlparser::tokenizer::Location {
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
        sqlparser::tokenizer::Location::new(
            (line + 1) as u64,
            (character - self.lines[line].1 + 1) as u64,
        )
    }

    fn span(&self, span: syntax::Span) -> sqlparser::tokenizer::Span {
        sqlparser::tokenizer::Span::new(self.location(span.start), self.location(span.end))
    }

    fn ident(&self, ident: &syntax::Identifier) -> Ident {
        let raw = &self.input[ident.span.start..ident.span.end];
        Ident {
            value: if ident.quoted {
                raw[1..raw.len() - 1].replace("\"\"", "\"")
            } else {
                raw.to_owned()
            },
            quote_style: ident.quoted.then_some('"'),
            span: self.span(ident.span),
        }
    }

    fn name(&self, name: &syntax::QualifiedName) -> ObjectName {
        ObjectName(
            name.parts
                .iter()
                .map(|part| ObjectNamePart::Identifier(self.ident(part)))
                .collect(),
        )
    }

    fn expression(&self, expression: &syntax::ExpressionSyntax) -> Result<Expr> {
        use syntax::{ExpressionKindSyntax as E, LiteralSyntax as L};
        let boxed = |e| self.expression(e).map(Box::new);
        Ok(match &expression.kind {
            E::Column(name) => {
                let mut parts = name
                    .parts
                    .iter()
                    .map(|part| self.ident(part))
                    .collect::<Vec<_>>();
                if parts.len() == 1 {
                    Expr::Identifier(parts.remove(0))
                } else {
                    Expr::CompoundIdentifier(parts)
                }
            }
            E::Parameter(position) => Expr::Value(ValueWithSpan {
                value: ParserValue::Placeholder(format!("${position}")),
                span: self.span(expression.span),
            }),
            E::Literal(literal) => Expr::Value(ValueWithSpan {
                value: match literal {
                    L::Null => ParserValue::Null,
                    L::Boolean(value) => ParserValue::Boolean(*value),
                    L::Number(span) => {
                        ParserValue::Number(self.input[span.start..span.end].to_owned(), false)
                    }
                    L::String(span) => {
                        let raw = &self.input[span.start..span.end];
                        if raw.starts_with('$') {
                            return Err(HawDBError::Semantic(
                                "unsupported PostgreSQL dollar-quoted literal".into(),
                            ));
                        }
                        let value = if raw.starts_with(['E', 'e', 'U', 'u']) {
                            // Decode one already matched lexical terminal with the
                            // established PostgreSQL escape policy, not a SQL parser.
                            let tokens =
                                sqlparser::tokenizer::Tokenizer::new(&PostgreSqlDialect {}, raw)
                                    .tokenize()
                                    .map_err(|error| HawDBError::Parse(error.to_string()))?;
                            let [sqlparser::tokenizer::Token::EscapedStringLiteral(value)
                            | sqlparser::tokenizer::Token::UnicodeStringLiteral(value)] =
                                tokens.as_slice()
                            else {
                                return Err(HawDBError::Parse(
                                    "invalid escaped string literal".into(),
                                ));
                            };
                            value.clone()
                        } else {
                            raw[1..raw.len() - 1].replace("''", "'")
                        };
                        ParserValue::SingleQuotedString(value)
                    }
                },
                span: self.span(expression.span),
            }),
            E::Parenthesized(inner) => Expr::Nested(boxed(inner)?),
            E::Unary {
                operator,
                expression,
            } => Expr::UnaryOp {
                op: match operator {
                    syntax::UnaryOperatorSyntax::Not => sqlparser::ast::UnaryOperator::Not,
                    syntax::UnaryOperatorSyntax::Plus => sqlparser::ast::UnaryOperator::Plus,
                    syntax::UnaryOperatorSyntax::Minus => sqlparser::ast::UnaryOperator::Minus,
                },
                expr: boxed(expression)?,
            },
            E::Binary {
                left,
                operator,
                right,
            } => Expr::BinaryOp {
                left: boxed(left)?,
                right: boxed(right)?,
                op: match operator {
                    syntax::BinaryOperatorSyntax::Or => BinaryOperator::Or,
                    syntax::BinaryOperatorSyntax::And => BinaryOperator::And,
                    syntax::BinaryOperatorSyntax::Equal => BinaryOperator::Eq,
                    syntax::BinaryOperatorSyntax::NotEqual => BinaryOperator::NotEq,
                    syntax::BinaryOperatorSyntax::Less => BinaryOperator::Lt,
                    syntax::BinaryOperatorSyntax::LessOrEqual => BinaryOperator::LtEq,
                    syntax::BinaryOperatorSyntax::Greater => BinaryOperator::Gt,
                    syntax::BinaryOperatorSyntax::GreaterOrEqual => BinaryOperator::GtEq,
                    syntax::BinaryOperatorSyntax::Add => BinaryOperator::Plus,
                    syntax::BinaryOperatorSyntax::Subtract => BinaryOperator::Minus,
                    syntax::BinaryOperatorSyntax::Multiply => BinaryOperator::Multiply,
                    syntax::BinaryOperatorSyntax::Divide => BinaryOperator::Divide,
                    syntax::BinaryOperatorSyntax::Modulo => BinaryOperator::Modulo,
                    syntax::BinaryOperatorSyntax::Concat => BinaryOperator::StringConcat,
                },
            },
            E::Filtered {
                function,
                predicate,
            } => {
                let Expr::Function(mut function) = self.expression(function)? else {
                    unreachable!("FILTER requires a function")
                };
                function.filter = Some(boxed(predicate)?);
                Expr::Function(function)
            }
            E::Like {
                expression,
                pattern,
                escape,
                negated,
                case_insensitive,
            } => {
                let expr = boxed(expression)?;
                let pattern = boxed(pattern)?;
                let escape_char = escape.as_deref().map(boxed).transpose()?;
                if *case_insensitive {
                    Expr::ILike {
                        negated: *negated,
                        any: false,
                        expr,
                        pattern,
                        escape_char,
                    }
                } else {
                    Expr::Like {
                        negated: *negated,
                        any: false,
                        expr,
                        pattern,
                        escape_char,
                    }
                }
            }
            E::IsNull {
                expression,
                negated,
            } => {
                if *negated {
                    Expr::IsNotNull(boxed(expression)?)
                } else {
                    Expr::IsNull(boxed(expression)?)
                }
            }
            E::InList {
                expression,
                values,
                negated,
            } => Expr::InList {
                expr: boxed(expression)?,
                list: values
                    .iter()
                    .map(|value| self.expression(value))
                    .collect::<Result<_>>()?,
                negated: *negated,
            },
            E::Between {
                expression,
                low,
                high,
                negated,
            } => Expr::Between {
                expr: boxed(expression)?,
                low: boxed(low)?,
                high: boxed(high)?,
                negated: *negated,
            },
            E::Function {
                name,
                arguments,
                distinct,
            } => Expr::Function(sqlparser::ast::Function {
                name: self.name(name),
                uses_odbc_syntax: false,
                parameters: FunctionArguments::None,
                args: FunctionArguments::List(sqlparser::ast::FunctionArgumentList {
                    duplicate_treatment: distinct.then_some(DuplicateTreatment::Distinct),
                    args: arguments
                        .iter()
                        .map(|arg| {
                            Ok(FunctionArg::Unnamed(match &arg.kind {
                                E::Wildcard(None) => FunctionArgExpr::Wildcard,
                                E::Wildcard(Some(name)) => {
                                    FunctionArgExpr::QualifiedWildcard(self.name(name))
                                }
                                _ => FunctionArgExpr::Expr(self.expression(arg)?),
                            }))
                        })
                        .collect::<Result<_>>()?,
                    clauses: Vec::new(),
                }),
                filter: None,
                null_treatment: None,
                over: None,
                within_group: Vec::new(),
            }),
            _ => {
                return Err(HawDBError::Semantic(
                    "unsupported PostgreSQL expression".into(),
                ))
            }
        })
    }

    fn lower(
        &self,
        expression: &syntax::ExpressionSyntax,
        position: ExpressionPosition,
    ) -> Result<crate::Expr> {
        lower_expression(&self.expression(expression)?, position)
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
                Ok(normalize_ident(&self.ident(&alias.name)))
            })
            .transpose()?;
        Ok((lower_table_name(&self.name(&relation.name))?, alias))
    }

    fn select(&self, select: &syntax::PostgresSelectSyntax) -> Result<SqlStatement> {
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
                    alias: item
                        .alias
                        .as_ref()
                        .map(|alias| normalize_ident(&self.ident(alias))),
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
                .map(|arguments| {
                    let args = sqlparser::ast::TableFunctionArgs {
                        settings: None,
                        args: arguments
                            .iter()
                            .map(|argument| {
                                let arg =
                                    FunctionArgExpr::Expr(self.expression(&argument.expression)?);
                                Ok(match &argument.name {
                                    Some(name) => FunctionArg::Named {
                                        name: self.ident(name),
                                        arg,
                                        operator: sqlparser::ast::FunctionArgOperator::RightArrow,
                                    },
                                    None => FunctionArg::Unnamed(arg),
                                })
                            })
                            .collect::<Result<_>>()?,
                    };
                    vector_index::lower_call(lower_table_name(&self.name(&relation.name))?, &args)
                        .map(Box::new)
                })
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
                .map(|expr| lower_column_expr(&self.expression(expr)?))
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
                .map(|expr| lower_nonnegative_integer_expr(&self.expression(expr)?))
                .transpose()?,
            offset: select
                .offset
                .as_ref()
                .map(|expr| lower_nonnegative_integer_expr(&self.expression(expr)?))
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
    use std::collections::BTreeSet;

    const REVIEWED_ERROR_CLASS_TRANSITIONS: &[&str] = &[
        "case-0091",
        "case-0092",
        "case-0093",
        "case-0094",
        "case-0924",
        "case-0986",
        "case-0987",
        "case-0988",
        "case-0989",
        "case-0990",
        "case-1006",
    ];

    fn assert_rejection_class(
        expected: HawDBError,
        actual: HawDBError,
        reviewed_semantic_to_parse: bool,
        input: &str,
    ) {
        if reviewed_semantic_to_parse {
            assert!(matches!((&expected, &actual), (HawDBError::Semantic(_), HawDBError::Parse(_))),
                "reviewed Semantic-to-Parse transition changed: {input}\nlegacy {expected:?}\nPEG {actual:?}");
        } else {
            assert_eq!(
                std::mem::discriminant(&actual),
                std::mem::discriminant(&expected),
                "unreviewed error-class transition: {input}\nlegacy {expected:?}\nPEG {actual:?}"
            );
        }
    }

    fn reference(input: &str) -> Result<SqlStatement> {
        let statements = Parser::parse_sql(&PostgreSqlDialect {}, input)
            .map_err(|error| HawDBError::Parse(error.to_string()))?;
        let [statement] = statements.as_slice() else {
            return Err(HawDBError::Parse("one statement required".into()));
        };
        lower_statement(statement)
    }

    #[test]
    fn frozen_corpus_preserves_complete_accepted_ir_and_rejection_boundaries() {
        let mut accepted = 0;
        let mut rejected = 0;
        let mut reviewed = BTreeSet::new();
        for line in include_str!("../../fixtures/frontend_corpus_v1.jsonl").lines() {
            let case: serde_json::Value = serde_json::from_str(line).unwrap();
            let input = case["sql"].as_str().unwrap();
            let Ok(first) = syntax::first_token(input) else {
                continue;
            };
            if !first.is_keyword(input, "SELECT") && !first.is_keyword(input, "EXPLAIN") {
                continue;
            }
            let expected = reference(input);
            let actual = parse_postgres_sql(input);
            match (expected, actual) {
                (Ok(expected), Ok(actual)) => {
                    assert_eq!(actual, expected, "{}: {input}", case["id"]);
                    accepted += 1;
                }
                (Err(expected), Err(actual)) => {
                    let id = case["id"].as_str().unwrap();
                    let transition = REVIEWED_ERROR_CLASS_TRANSITIONS.contains(&id);
                    if transition {
                        reviewed.insert(id.to_owned());
                    }
                    assert_rejection_class(expected, actual, transition, input);
                    rejected += 1;
                }
                (expected, actual) => panic!(
                    "{}: {input}\nreference {expected:?}\nPEG {actual:?}",
                    case["id"]
                ),
            }
        }
        assert_eq!(accepted, 66, "frozen accepted statement inventory");
        assert_eq!(
            reviewed,
            REVIEWED_ERROR_CLASS_TRANSITIONS
                .iter()
                .map(|id| (*id).to_owned())
                .collect(),
            "every reviewed transition must still be exercised"
        );
        assert!(
            rejected >= 500,
            "must compare negative cases, got {rejected}"
        );
    }
    #[test]
    fn generated_select_forms_preserve_legacy_ir() {
        let mut inputs = Vec::new();
        for value in [
            "NULL",
            "TRUE",
            "-1",
            "1.25",
            ".5",
            "1.",
            "1_000",
            "1_",
            "1__0",
            "1._5",
            "1.0_0",
            "1.0e1_0",
            "U&'d\\0061t'",
            "$1",
            "'a''b'",
            "E'a\\n'",
            "E'a\\\'b'",
            "$tag$text$tag$",
            "COALESCE(id, $1)",
            "COUNT(ALL id)",
            "SUM(id) FILTER (WHERE id > $1)",
            "\"列\"",
        ] {
            for tail in [
                "",
                " FROM items",
                " AS label FROM items",
                " label FROM items",
            ] {
                inputs.push(format!("SELECT {value}{tail}"));
            }
        }
        for input in [
            "SELECT id FROM items WHERE name LIKE $1 ESCAPE '!'",
            "EXPLAIN ANALYZE SELECT id FROM items WHERE id = $1",
            "SELECT id FROM items OFFSET 1 LIMIT 2",
            "SELECT id FROM items LIMIT ALL",
            "SELECT id FROM items OFFSET 2 ROWS",
            "SELECT ALL id FROM items",
            "SELECT * AS renamed FROM items",
            "SELECT * renamed FROM items",
            "SELECT id FROM items FOR SHARE",
            "SELECT * FROM vector_index_search('items', $1, top_k => 1, candidates => 8)",
            "SELECT * FROM vector_index_search('items', $1, top_k := 1, candidates := 8)",
        ] {
            inputs.push(input.into());
        }
        for input in inputs {
            // This previously reviewed generated negative uses unsupported
            // assignment syntax; the owned grammar rejects it before lowering.
            let transition = input
                == "SELECT * FROM vector_index_search('items', $1, top_k := 1, candidates := 8)";
            for prefix in ["", "-- lead\n", "/* 知識 /* inner */ */\n"] {
                let input = format!("{prefix}{input}");
                match (reference(&input), parse_postgres_sql(&input)) {
                    (Ok(expected), Ok(actual)) => assert_eq!(actual, expected, "{input}"),
                    (Err(expected), Err(actual)) => {
                        assert_rejection_class(expected, actual, transition, &input)
                    }
                    (expected, actual) => panic!("{input}\nreference {expected:?}\nPEG {actual:?}"),
                }
            }
        }
    }
    #[test]
    fn malformed_numeric_separators_are_rejected_by_both_frontends() {
        for number in ["1_", "1__0", "1._5"] {
            let input = format!("SELECT {number} FROM items");
            assert!(reference(&input).is_err(), "legacy accepted {input}");
            assert!(parse_postgres_sql(&input).is_err(), "PEG accepted {input}");
        }
    }

    #[test]
    fn legacy_statement_bodies_do_not_inherit_owned_input_limit() {
        let payload = "x".repeat(syntax::LexerConfig::default().max_input_bytes + 1);
        {
            let input = format!("-- dispatch only\nINSERT INTO items (id) VALUES ('{payload}')");
            let SqlStatement::Insert(insert) = parse_postgres_sql(&input).unwrap() else {
                panic!("large INSERT must stay on the legacy parser path");
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
                panic!("native nesting must fail during the selected PEG parse");
            };
            assert!(error.contains("ExpressionNestingLimitExceeded"), "{error}");
        }
        assert_eq!(
            parse_postgres_sql("SELECT id FROM items").unwrap(),
            reference("SELECT id FROM items").unwrap()
        );
        // DDL remains explicitly dispatched; a new SELECT lexer extension cannot
        // accidentally become a prerequisite for the other statement families.
        assert!(parse_postgres_sql("CREATE TABLE items (id TEXT DEFAULT E'line\\n')").is_ok());
    }
    #[test]
    #[ignore = "manual parser-only timing diagnostic; use the repository release profile"]
    fn parser_timing_diagnostic() {
        let mut cases = vec![
            ("lookup", "SELECT id FROM messages WHERE thread_id = $1 ORDER BY id LIMIT 20".into(), 2_000),
            ("aggregate", "SELECT thread_id, count(*) FILTER (WHERE active = $1) AS n FROM messages GROUP BY thread_id HAVING count(*) > 0".into(), 2_000),
            ("left_join", "SELECT a.id, b.name FROM nodes a LEFT JOIN names b ON a.id = b.id WHERE b.name IS NOT NULL".into(), 2_000),
            ("vector", "SELECT id FROM vector_index_search('items', $1, top_k => 1, candidates => 8) WHERE active = true".into(), 2_000),
            ("constant", "SELECT 1 AS value".into(), 2_000),
            ("rejected", "SELECT id FROM items WHERE".into(), 2_000),
        ];
        for (shape, quoted) in [("wide_ascii", false), ("wide_utf8", true)] {
            let projection = (0..500)
                .map(|index| {
                    if quoted {
                        format!("\"列_{index}\"")
                    } else {
                        format!("column_{index}")
                    }
                })
                .collect::<Vec<_>>()
                .join(", ");
            cases.push((shape, format!("SELECT {projection} FROM items"), 100));
        }
        type ParseFn = fn(&str) -> Result<SqlStatement>;
        println!("shape,sample,frontend,iterations,nanos");
        for (shape, input, iterations) in cases {
            let expected = reference(&input);
            let actual = parse_postgres_sql(&input);
            match (expected, actual) {
                (Ok(expected), Ok(actual)) => assert_eq!(actual, expected),
                (Err(_), Err(_)) => (),
                pair => panic!("{shape}: {pair:?}"),
            }
            for sample in 0..9 {
                let mut parsers: [(&str, ParseFn); 2] =
                    [("legacy", reference), ("peg", parse_postgres_sql)];
                if sample % 2 != 0 {
                    parsers.reverse();
                }
                for (frontend, parser) in parsers {
                    let started = std::time::Instant::now();
                    for _ in 0..iterations {
                        drop(std::hint::black_box(parser(std::hint::black_box(&input))));
                    }
                    println!(
                        "{shape},{sample},{frontend},{iterations},{}",
                        started.elapsed().as_nanos()
                    );
                }
            }
        }
    }
}
