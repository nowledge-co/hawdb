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

use super::clauses::{reject, CREATE_CLAUSES, INSERT_CLAUSES};
use crate::{
    parse_postgres_sql, prepare_postgres_sql, CreateTableStatement, InsertStatement,
    PostgresParameterMetadata, SqlColumnDefinition, SqlDataType, SqlStatement, SqlTableName,
    SqlTableStorage, SqlValue,
};
use hawdb_core::{HawDBError, Value};

fn assert_error(error: HawDBError, statement: &str, clause: &str) {
    let HawDBError::Semantic(message) = error else {
        panic!("expected a lowering rejection, got {error:?}");
    };
    assert_eq!(
        message,
        format!("unsupported PostgreSQL {statement} clause: {clause}")
    );
}

#[test]
fn every_original_clause_condition_is_rejected_by_lowering() {
    assert_eq!(CREATE_CLAUSES.len(), 59);
    assert_eq!(INSERT_CLAUSES[..15].len(), 15);
    for (statement, clauses) in [("CREATE TABLE", CREATE_CLAUSES), ("INSERT", INSERT_CLAUSES)] {
        assert!(reject(statement, &[]).is_ok());
        for clause in clauses {
            assert_error(reject(statement, &[clause]).unwrap_err(), statement, clause);
        }
    }
}

#[test]
fn local_scope_is_present_even_when_the_global_flag_is_false() {
    // LOCAL is explicit presence, independent of the former AST boolean value.
    assert_error(
        reject("CREATE TABLE", &["LOCAL"]).unwrap_err(),
        "CREATE TABLE",
        "LOCAL",
    );
}

fn assert_source_error(sql: &str, statement: &str, clause: &str) {
    assert_error(parse_postgres_sql(sql).unwrap_err(), statement, clause);
    assert_error(prepare_postgres_sql(sql).unwrap_err(), statement, clause);
}

fn assert_accepted(sql: &str, expected: SqlStatement, positions: &[usize]) {
    assert_eq!(parse_postgres_sql(sql).unwrap(), expected, "source: {sql}");
    let prepared = prepare_postgres_sql(sql).unwrap();
    assert_eq!(prepared.statement, expected, "source: {sql}");
    assert_eq!(
        prepared.parameters,
        positions
            .iter()
            .map(|&position| PostgresParameterMetadata { position })
            .collect::<Vec<_>>()
    );
}

// Expected ASTs are built from inputs, never by parsing another SQL string.
fn source_contract(case: usize) {
    let separator = [" ", "\n", "\t", " /* private comment */ "][case % 4];
    let keyword = |word: &str| {
        if case & 4 == 0 {
            word.to_string()
        } else {
            word.to_ascii_lowercase()
        }
    };
    let table_name = format!("Private_\u{e9}_{}", case / 8);
    let table = SqlTableName {
        schema: Some("PrivateSchema".into()),
        name: table_name.clone(),
    };
    let quoted_table = format!("\"PrivateSchema\".\"{table_name}\"");
    let payload = [
        "private literal",
        "quote's payload",
        "line\nbreak",
        "\u{1f512}\u{e9}",
    ][case / 8 % 4];
    let literal = format!("'{}'", payload.replace('\'', "''"));
    let tokens = |parts: Vec<String>| parts.join(separator);
    let create = tokens(vec![
        keyword("CREATE"),
        keyword("TABLE"),
        quoted_table.clone(),
        format!("(id {}, payload {})", keyword("BIGINT"), keyword("TEXT")),
    ]);
    assert_accepted(
        &create,
        SqlStatement::CreateTable(CreateTableStatement {
            table: table.clone(),
            if_not_exists: false,
            columns: [("id", SqlDataType::BigInt), ("payload", SqlDataType::Text)]
                .into_iter()
                .map(|(name, data_type)| SqlColumnDefinition {
                    name: name.into(),
                    data_type,
                    nullable: true,
                    default: None,
                    primary_key: false,
                    unique: false,
                    references: None,
                })
                .collect(),
            constraints: vec![],
            storage: SqlTableStorage::RowPage,
        }),
        &[],
    );
    let insert = tokens(vec![
        keyword("INSERT"),
        keyword("INTO"),
        quoted_table.clone(),
        "(id, payload)".into(),
        keyword("VALUES"),
        format!("($1, {literal}), ($1, {literal})"),
    ]);
    assert_accepted(
        &insert,
        SqlStatement::Insert(InsertStatement {
            table,
            columns: vec!["id".into(), "payload".into()],
            rows: vec![
                vec![
                    SqlValue::Parameter(1),
                    SqlValue::Literal(Value::String(payload.into()))
                ];
                2
            ],
            on_conflict: None,
            returning: vec![],
        }),
        &[1],
    );
    for (prefix, suffix, clause) in [
        ("CREATE TEMP TABLE", "(id BIGINT)".to_string(), "TEMPORARY"),
        ("CREATE TABLE", format!("AS SELECT {literal}"), "AS query"),
        (
            "CREATE TEMP TABLE",
            format!("AS SELECT {literal}"),
            "TEMPORARY",
        ),
        (
            "CREATE TABLE",
            "(id BIGINT) ON COMMIT DROP".to_string(),
            "ON COMMIT",
        ),
        (
            "CREATE TABLE",
            "(id BIGINT) INHERITS (private_parent)".to_string(),
            "INHERITS",
        ),
        (
            "CREATE TABLE",
            "(id BIGINT) PARTITION BY RANGE (id)".to_string(),
            "PARTITION BY",
        ),
    ] {
        let sql = tokens(vec![keyword(prefix), quoted_table.clone(), suffix]);
        assert_source_error(&sql, "CREATE TABLE", clause);
    }
    let sql = tokens(vec![
        keyword("INSERT INTO"),
        quoted_table,
        keyword("AS"),
        "private_alias (id, payload)".into(),
        keyword("VALUES"),
        format!("($2, {literal})"),
    ]);
    assert_source_error(&sql, "INSERT", "table alias");
}

#[test]
fn accepted_ast_and_multiple_clause_priority_are_preserved() {
    for case in 0..32 {
        source_contract(case);
    }
}

#[test]
#[ignore = "deterministic local-only clause diagnostic campaign"]
fn clause_diagnostics_campaign() {
    every_original_clause_condition_is_rejected_by_lowering();
    local_scope_is_present_even_when_the_global_flag_is_false();
    let mut pairs = 0;
    for (statement, cases) in [
        ("CREATE TABLE", CREATE_CLAUSES),
        ("INSERT", &INSERT_CLAUSES[..15]),
    ] {
        for (index, first) in cases.iter().enumerate() {
            for second in &cases[index + 1..] {
                assert_error(
                    reject(statement, &[second, first]).unwrap_err(),
                    statement,
                    first,
                );
                pairs += 1;
            }
        }
    }
    assert_eq!(pairs, 1816);
    for case in 0..1024 {
        source_contract(case);
    }
    println!("clause diagnostics: {pairs} owned condition pairs; 9216 SQL sources through parse and prepare");
}
