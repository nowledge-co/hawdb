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

#[test]
fn unlogged_tables_are_rejected_without_weakening_durability() {
    let error = parse_postgres_sql("CREATE UNLOGGED TABLE t (id BIGINT)").unwrap_err();
    assert!(matches!(error, HawDBError::Semantic(_)));
    assert!(error.to_string().contains("UNLOGGED"), "{error}");
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

// All rejections exercise source text through the public parser. There is no
// foreign parser AST for a host to mutate or smuggle unrecognized fields into.
#[test]
fn unsupported_alias_mutation_and_ordering_syntax_is_never_executed() {
    for sql in [
        "SELECT id FROM t AS data AT position",
        "INSERT INTO t (id) OUTPUT id VALUES (1)",
        "UPDATE t SET id=1 OUTPUT id",
        "DELETE FROM t OUTPUT id",
        "INSERT ALL INTO t (id) VALUES (1) SELECT 1",
        "INSERT FIRST WHEN condition THEN INTO t (id) VALUES (1) SELECT 1",
        "INSERT INTO t (id) VALUES (1) INTO other (id) VALUES (2)",
        "INSERT INTO t (id) VALUES (1) ELSE INTO other (id) VALUES (2)",
        "SELECT id FROM t ORDER BY id USING <",
        "CREATE INDEX ix ON t (id USING <)",
        "UPDATE t SET id=1 ORDER BY id DESC",
        "SELECT id AS (a,b) FROM t",
        "INSERT INTO t (id) VALUES (1) RETURNING id AS (a,b)",
    ] {
        assert!(
            parse_postgres_sql(sql).is_err(),
            "unsupported syntax was accepted: {sql}"
        );
        assert!(
            crate::prepare_postgres_sql(sql).is_err(),
            "unsupported syntax was prepared: {sql}"
        );
    }
    for sql in [
        "SELECT id FROM t ORDER BY id DESC",
        "CREATE INDEX ix ON t (id DESC)",
    ] {
        assert!(
            parse_postgres_sql(sql).is_ok(),
            "supported ordering rejected: {sql}"
        );
    }
}

#[test]
fn postgres_type_aliases_keep_main_storage_types_and_precision_bounds() {
    for (declaration, expected) in [
        ("INT", SqlDataType::BigInt),
        ("INTEGER", SqlDataType::BigInt),
        ("INT2", SqlDataType::BigInt),
        ("INT4", SqlDataType::BigInt),
        ("SMALLINT", SqlDataType::BigInt),
        ("TINYINT", SqlDataType::BigInt),
        ("FLOAT", SqlDataType::DoublePrecision),
        ("FLOAT(25)", SqlDataType::DoublePrecision),
        ("FLOAT(53)", SqlDataType::DoublePrecision),
        ("VARCHAR", SqlDataType::Text),
        ("VARCHAR(12)", SqlDataType::Text),
        ("CHARACTER VARYING", SqlDataType::Text),
        ("CHAR VARYING(12)", SqlDataType::Text),
    ] {
        let sql = format!("CREATE TABLE aliases (value {declaration})");
        let SqlStatement::CreateTable(table) = parse_postgres_sql(&sql).unwrap() else {
            panic!("expected CREATE TABLE for {declaration}");
        };
        assert_eq!(table.columns[0].data_type, expected, "{declaration}");
    }
    for declaration in [
        "REAL",
        "FLOAT4",
        "FLOAT(0)",
        "FLOAT(24)",
        "FLOAT(54)",
        "FLOAT(25.0)",
        "VARCHAR(0)",
        "VARCHAR(MAX)",
        "VARCHAR(1 CHARACTERS)",
        "VARCHAR(18446744073709551616)",
        "INT(4)",
        "CHAR(12)",
    ] {
        let sql = format!("CREATE TABLE aliases (value {declaration})");
        assert!(parse_postgres_sql(&sql).is_err(), "{declaration}");
    }
}

#[test]
fn omitted_insert_columns_preserve_values_arity_validation() {
    let SqlStatement::Insert(insert) =
        parse_postgres_sql("INSERT INTO bag VALUES (1, 'one'), (2, 'two')").unwrap()
    else {
        panic!("expected INSERT");
    };
    assert!(insert.columns.is_empty());
    assert_eq!(insert.rows.len(), 2);
    assert!(insert.rows.iter().all(|row| row.len() == 2));
    for sql in [
        "INSERT INTO bag VALUES (1, 'one'), (2)",
        "INSERT INTO bag (id) VALUES (1, 'one')",
        "INSERT INTO bag (id, label) VALUES (1)",
    ] {
        let error = parse_postgres_sql(sql).unwrap_err();
        assert!(error.to_string().contains("equal length"), "{sql}: {error}");
    }
}

#[test]
fn parenthesized_scalar_arguments_keep_column_and_aggregate_semantics() {
    for sql in [
        "SELECT ((value)) FROM items",
        "SELECT COUNT(DISTINCT ((items.value))) FROM items",
    ] {
        assert!(parse_postgres_sql(sql).is_ok(), "{sql}");
    }
}

#[test]
fn bulk_values_fit_the_active_composite_lock_workload_without_raising_peg_limits() {
    for literal in ["1", "FALSE", "NULL", "-1", "'value'", "$1"] {
        let row = format!("({literal},{literal},{literal},{literal},{literal})");
        let sql = format!(
            "INSERT INTO bulk (a,b,c,d,e) VALUES {}",
            vec![row; 5001].join(",")
        );
        let SqlStatement::Insert(insert) = parse_postgres_sql(&sql).unwrap() else {
            panic!("expected INSERT");
        };
        assert_eq!(insert.rows.len(), 5001, "{literal}");
        assert!(insert.rows.iter().all(|row| row.len() == 5), "{literal}");
    }
    for sql in [
        "INSERT INTO bulk VALUES (1 + 2)",
        "INSERT INTO bulk VALUES (upper('value'))",
        "INSERT INTO bulk VALUES (-NULL)",
    ] {
        assert!(
            matches!(parse_postgres_sql(sql), Err(HawDBError::Semantic(_))),
            "{sql}"
        );
    }
}
