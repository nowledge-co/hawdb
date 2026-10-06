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

use super::*;
use sqlparser::ast::{OptimizerHint, OptimizerHintStyle, OutputClause};

fn upstream(sql: &str) -> ParserStatement {
    Parser::parse_sql(&PostgreSqlDialect {}, sql)
        .unwrap()
        .remove(0)
}

#[test]
fn unlogged_tables_are_rejected_without_weakening_durability() {
    let error = parse_postgres_sql("CREATE UNLOGGED TABLE t (id BIGINT)").unwrap_err();
    assert!(matches!(error, HawDBError::Semantic(_)));
    assert!(error.to_string().contains("UNLOGGED"), "{error}");
}

#[test]
fn table_index_alias_is_not_silently_discarded() {
    let ParserStatement::Query(mut query) = upstream("SELECT id FROM t AS data") else {
        unreachable!()
    };
    let SetExpr::Select(select) = query.body.as_mut() else {
        unreachable!()
    };
    let TableFactor::Table { alias, .. } = &mut select.from[0].relation else {
        unreachable!()
    };
    alias.as_mut().unwrap().at = Some(Ident::new("position"));
    let error = lower_statement(&ParserStatement::Query(query)).unwrap_err();
    assert!(error.to_string().contains("AT aliases"), "{error}");
}

#[test]
fn mutation_hints_and_output_are_not_silently_discarded() {
    for sql in [
        "INSERT INTO t (id) VALUES (1)",
        "UPDATE t SET id = 1",
        "DELETE FROM t",
    ] {
        for output in [false, true] {
            let mut statement = upstream(sql);
            let (hints, clause) = match &mut statement {
                ParserStatement::Insert(insert) => {
                    (&mut insert.optimizer_hints, &mut insert.output)
                }
                ParserStatement::Update(update) => {
                    (&mut update.optimizer_hints, &mut update.output)
                }
                ParserStatement::Delete(delete) => {
                    (&mut delete.optimizer_hints, &mut delete.output)
                }
                _ => unreachable!(),
            };
            if output {
                *clause = Some(OutputClause::Output {
                    output_token: sqlparser::ast::helpers::attached_token::AttachedToken::empty(),
                    select_items: vec![ParserSelectItem::UnnamedExpr(Expr::Identifier(
                        Ident::new("id"),
                    ))],
                    into_table: None,
                });
            } else {
                hints.push(OptimizerHint {
                    prefix: "extension".into(),
                    text: "private_hint".into(),
                    style: OptimizerHintStyle::MultiLine,
                });
            }
            assert!(
                matches!(lower_statement(&statement), Err(HawDBError::Semantic(_))),
                "{sql}"
            );
        }
    }
}

#[test]
fn insert_columns_preserve_quoted_identifiers_and_reject_qualified_paths() {
    let statement = parse_postgres_sql(
        r#"INSERT INTO t ("Mixed", "with.dot", Plain) VALUES (1, 2, 3) RETURNING "Mixed""#,
    )
    .unwrap();
    let SqlStatement::Insert(insert) = statement else {
        panic!("expected INSERT")
    };
    assert_eq!(insert.columns, ["Mixed", "with.dot", "plain"]);
    assert_eq!(insert.returning[0].name, "Mixed");
    let error = parse_postgres_sql("INSERT INTO t (t.id) VALUES (1)").unwrap_err();
    assert!(
        error.to_string().contains("unqualified identifiers"),
        "{error}"
    );
}

#[test]
fn multi_table_insert_clauses_are_not_silently_discarded() {
    use sqlparser::ast::{
        Insert, MultiTableInsertIntoClause, MultiTableInsertType, MultiTableInsertWhenClause,
    };
    let cases: [fn(&mut Insert); 4] = [
        |insert| insert.multi_table_insert_type = Some(MultiTableInsertType::All),
        |insert| {
            insert
                .multi_table_into_clauses
                .push(MultiTableInsertIntoClause {
                    table_name: ObjectName(vec![ObjectNamePart::Identifier(Ident::new("other"))]),
                    columns: vec![],
                    values: None,
                })
        },
        |insert| {
            insert
                .multi_table_when_clauses
                .push(MultiTableInsertWhenClause {
                    condition: Expr::Identifier(Ident::new("condition")),
                    into_clauses: vec![],
                })
        },
        |insert| insert.multi_table_else_clause = Some(vec![]),
    ];
    for modify in cases {
        let ParserStatement::Insert(mut insert) = upstream("INSERT INTO t (id) VALUES (1)") else {
            unreachable!()
        };
        modify(&mut insert);
        let error = lower_statement(&ParserStatement::Insert(insert)).unwrap_err();
        assert!(error.to_string().contains("multi-table"), "{error}");
    }
}

#[test]
fn using_order_is_rejected_for_select_and_index_keys() {
    for sql in [
        "SELECT id FROM t ORDER BY id DESC",
        "CREATE INDEX ix ON t (id DESC)",
    ] {
        let mut statement = upstream(sql);
        assert!(lower_statement(&statement).is_ok(), "{sql}");
        let sort = OrderBySort::Using(ObjectName(vec![ObjectNamePart::Identifier(Ident::new(
            "<",
        ))]));
        match &mut statement {
            ParserStatement::Query(query) => {
                let OrderByKind::Expressions(items) = &mut query.order_by.as_mut().unwrap().kind
                else {
                    unreachable!()
                };
                items[0].options.sort = Some(sort);
            }
            ParserStatement::CreateIndex(index) => {
                index.columns[0].column.options.sort = Some(sort)
            }
            _ => unreachable!(),
        }
        let error = lower_statement(&statement).unwrap_err();
        assert!(error.to_string().contains("ORDER BY USING"), "{error}");
    }
}

#[test]
fn update_ordering_is_not_silently_discarded() {
    let ParserStatement::Query(query) = upstream("SELECT id FROM t ORDER BY id DESC") else {
        unreachable!()
    };
    let OrderByKind::Expressions(items) = query.order_by.unwrap().kind else {
        unreachable!()
    };
    let ParserStatement::Update(mut update) = upstream("UPDATE t SET id = 1") else {
        unreachable!()
    };
    update.order_by = items;
    assert!(matches!(
        lower_statement(&ParserStatement::Update(update)),
        Err(HawDBError::Semantic(_))
    ));
}

#[test]
fn like_escape_preserves_literal_semantics_without_accepting_expressions() {
    for operator in ["LIKE", "ILIKE"] {
        for (suffix, expected) in [
            ("", SqlLikeEscape::Character('\\')),
            (" ESCAPE ''", SqlLikeEscape::Disabled),
            (" ESCAPE '!'", SqlLikeEscape::Character('!')),
            (" ESCAPE '\u{e9}'", SqlLikeEscape::Character('\u{e9}')),
        ] {
            let sql = format!("SELECT id FROM t WHERE title {operator} $1{suffix}");
            let SqlStatement::Select(select) = parse_postgres_sql(&sql).unwrap() else {
                unreachable!()
            };
            let ExprKind::Like { escape, .. } = select.selection.unwrap().kind else {
                unreachable!()
            };
            assert_eq!(escape, expected, "{sql}");
        }
        for escape in ["'ab'", "1", "NULL", "$2", "escape_column", "('!')"] {
            let sql = format!("SELECT id FROM t WHERE title {operator} $1 ESCAPE {escape}");
            assert!(parse_postgres_sql(&sql).is_err(), "{sql}");
        }
    }
}

#[test]
fn multiple_aliases_are_rejected_in_select_and_returning() {
    for sql in [
        "SELECT id FROM t",
        "INSERT INTO t (id) VALUES (1) RETURNING id",
    ] {
        let mut statement = upstream(sql);
        let item = ParserSelectItem::ExprWithAliases {
            expr: Expr::Identifier(Ident::new("id")),
            aliases: vec![Ident::new("a"), Ident::new("b")],
        };
        match &mut statement {
            ParserStatement::Query(query) => {
                let SetExpr::Select(select) = query.body.as_mut() else {
                    unreachable!()
                };
                select.projection = vec![item];
            }
            ParserStatement::Insert(insert) => insert.returning = Some(vec![item]),
            _ => unreachable!(),
        }
        assert!(
            matches!(lower_statement(&statement), Err(HawDBError::Semantic(_))),
            "{sql}"
        );
    }
}
