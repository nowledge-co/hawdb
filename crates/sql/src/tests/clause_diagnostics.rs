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

use crate::{parse_postgres_sql, prepare_postgres_sql};
use hawdb_core::HawDBError;

fn assert_rejection(sql: &str, expected: &str) {
    for error in [
        parse_postgres_sql(sql).unwrap_err(),
        prepare_postgres_sql(sql).unwrap_err(),
    ] {
        let HawDBError::Semantic(message) = error else {
            panic!("expected a lowering rejection, got {error:?}");
        };
        assert_eq!(message, expected, "source: {sql}");
    }
}

#[test]
fn unsupported_create_table_names_the_clause_without_echoing_payloads() {
    for (sql, clause) in [
        ("CREATE TEMP TABLE private_name (id BIGINT)", "TEMPORARY"),
        (
            "CREATE TABLE private_name AS SELECT 'private literal'",
            "AS query",
        ),
        (
            "CREATE TABLE private_name (id BIGINT) ON COMMIT DROP",
            "ON COMMIT",
        ),
        (
            "CREATE TABLE private_name (id BIGINT) INHERITS (private_parent)",
            "INHERITS",
        ),
        (
            "CREATE TABLE private_name (id BIGINT) PARTITION BY RANGE (id)",
            "PARTITION BY",
        ),
    ] {
        assert_rejection(
            sql,
            &format!("unsupported PostgreSQL CREATE TABLE clause: {clause}"),
        );
    }
}

#[test]
fn unsupported_insert_names_the_clause_before_parameter_preparation() {
    for values in ["'private literal'", "$2"] {
        assert_rejection(
            &format!("INSERT INTO private_name AS private_alias (id) VALUES ({values})"),
            "unsupported PostgreSQL INSERT clause: table alias",
        );
    }
}
