// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

use crate::{prepare_postgres_sql, SqlStatement, SqlValue};
use hawdb_core::Value;

#[test]
fn search_index_ddl_parses_names_column_and_analyzer() {
    let SqlStatement::CreateSearchIndex(create) = prepare_postgres_sql(
        "CREATE SEARCH INDEX IF NOT EXISTS events_text ON events (text) WITH (analyzer = 'cjk_ngram')",
    )
    .unwrap()
    .statement
    else {
        panic!("expected CREATE SEARCH INDEX");
    };
    assert_eq!(create.name.name, "events_text");
    assert_eq!(create.table.name, "events");
    assert_eq!(create.column, "text");
    assert_eq!(create.analyzer.as_deref(), Some("cjk_ngram"));
    assert!(create.if_not_exists);
    let SqlStatement::CreateSearchIndex(plain) =
        prepare_postgres_sql("create search index idx on public.docs (\"Body\")")
            .unwrap()
            .statement
    else {
        panic!("expected CREATE SEARCH INDEX");
    };
    assert_eq!(plain.table.schema.as_deref(), Some("public"));
    assert_eq!(plain.column, "Body");
    assert_eq!(plain.analyzer, None);
    assert!(!plain.if_not_exists);
    let SqlStatement::DropSearchIndex(drop) =
        prepare_postgres_sql("DROP SEARCH INDEX IF EXISTS idx;")
            .unwrap()
            .statement
    else {
        panic!("expected DROP SEARCH INDEX");
    };
    assert_eq!(drop.name.name, "idx");
    assert!(drop.if_exists);
    for sql in [
        "CREATE SEARCH INDEX idx ON docs (a, b)",
        "CREATE SEARCH INDEX idx ON docs (body) WITH (analyzer = 1)",
        "CREATE SEARCH INDEX idx ON docs (body) WITH (analyzer = 'a', analyzer = 'b')",
        "CREATE SEARCH INDEX idx ON docs (body) WITH (tokenizer = 'x')",
        "CREATE SEARCH INDEX ON docs (body)",
        "DROP SEARCH INDEX a, b",
    ] {
        assert!(prepare_postgres_sql(sql).is_err(), "{sql}");
    }
}

#[test]
fn text_search_table_function_binds_literals_and_parameters() {
    let prepared = prepare_postgres_sql(
        "SELECT e.id, score FROM text_search('events_text', $1, top_k => $2) AS e \
         WHERE e.stream = $3 ORDER BY score DESC LIMIT 5",
    )
    .unwrap();
    assert_eq!(
        prepared
            .parameters
            .iter()
            .map(|p| p.position)
            .collect::<Vec<_>>(),
        [1, 2, 3]
    );
    let SqlStatement::Select(select) = prepared.statement else {
        panic!("expected SELECT");
    };
    let search = select.text_search().expect("text_search source");
    assert_eq!(
        search.index,
        SqlValue::Literal(Value::String("events_text".into()))
    );
    assert_eq!(search.query, SqlValue::Parameter(1));
    assert_eq!(search.top_k, SqlValue::Parameter(2));
    assert!(select.vector_index().is_none());
    assert_eq!(select.from_alias.as_deref(), Some("e"));
    for sql in [
        "SELECT * FROM text_search('i', 'q')",
        "SELECT * FROM text_search('i', 'q', 3)",
        "SELECT * FROM text_search('i', 'q', top_k => 3, top_k => 4)",
        "SELECT * FROM text_search('i', 'q', limit => 3)",
        "SELECT * FROM text_search(index => 'i', 'q', top_k => 3)",
    ] {
        assert!(prepare_postgres_sql(sql).is_err(), "{sql}");
    }
}
