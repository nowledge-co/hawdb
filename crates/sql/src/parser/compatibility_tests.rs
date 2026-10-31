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
