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

use crate::{Database, DatabaseConfig, QueryStreamOptions, Value};
use hawdb_storage::config::{RelationalIndexMode, StorageResidencyMode};

#[test]
fn ctes_and_set_operations_read_persistent_snapshots_after_reopen() {
    for (residency, index) in [
        (
            StorageResidencyMode::Materialized,
            RelationalIndexMode::Materialized,
        ),
        (
            StorageResidencyMode::OutOfCore,
            RelationalIndexMode::DemandPaged,
        ),
    ] {
        let path = std::env::temp_dir().join(format!(
            "hawdb-compound-query-{}",
            hawdb_core::generate_uuidv7().unwrap()
        ));
        let config = DatabaseConfig {
            storage_residency_mode: residency,
            relational_index_mode: index,
            ..Default::default()
        };
        {
            let mut database = Database::open_with_config(&path, config.clone()).unwrap();
            database
                .query_sql("CREATE TABLE numbers (id BIGINT PRIMARY KEY, n BIGINT)")
                .unwrap();
            database
                .query_sql("INSERT INTO numbers VALUES (1, 10), (2, 20), (3, 20)")
                .unwrap();
            let mut transaction = database.begin_transaction().unwrap();
            let output = transaction
                .query_sql(
                    "WITH selected(v) AS (SELECT n FROM numbers WHERE id <= 2) \
                     SELECT v FROM selected UNION SELECT 30 AS v ORDER BY v",
                )
                .unwrap();
            assert_eq!(
                output
                    .rows
                    .iter()
                    .map(|row| row["v"].clone())
                    .collect::<Vec<_>>(),
                vec![Value::Int(10), Value::Int(20), Value::Int(30)]
            );
            transaction.rollback();
            database.checkpoint().unwrap();
        }
        {
            let mut database = Database::open_with_config(&path, config).unwrap();
            let output = database
                .query_sql_with_params(
                    "WITH selected(v) AS (SELECT n FROM numbers WHERE n >= $1) \
                     SELECT v FROM selected UNION ALL SELECT $2 AS v ORDER BY v",
                    &[Value::Int(20), Value::Int(5)],
                )
                .unwrap();
            assert_eq!(
                output
                    .rows
                    .iter()
                    .map(|row| row["v"].clone())
                    .collect::<Vec<_>>(),
                vec![Value::Int(5), Value::Int(20), Value::Int(20)]
            );
        }
        std::fs::remove_dir_all(path).unwrap();
    }
}

#[test]
fn subqueries_values_and_derived_tables_read_persistent_snapshots_after_reopen() {
    for (residency, index) in [
        (
            StorageResidencyMode::Materialized,
            RelationalIndexMode::Materialized,
        ),
        (
            StorageResidencyMode::OutOfCore,
            RelationalIndexMode::DemandPaged,
        ),
    ] {
        let path = std::env::temp_dir().join(format!(
            "hawdb-subquery-values-{}",
            hawdb_core::generate_uuidv7().unwrap()
        ));
        let config = DatabaseConfig {
            storage_residency_mode: residency,
            relational_index_mode: index,
            ..Default::default()
        };
        {
            let mut database = Database::open_with_config(&path, config.clone()).unwrap();
            database
                .query_sql("CREATE TABLE numbers (id BIGINT PRIMARY KEY, n BIGINT)")
                .unwrap();
            database
                .query_sql("INSERT INTO numbers VALUES (1, 10), (2, 10), (3, 20)")
                .unwrap();
            let output = database
                .query_sql(
                    "SELECT outer_row.id FROM numbers AS outer_row \
                     WHERE EXISTS (SELECT 1 FROM numbers AS inner_row \
                                   WHERE inner_row.n = outer_row.n \
                                     AND inner_row.id <> outer_row.id) \
                     ORDER BY outer_row.id",
                )
                .unwrap();
            assert_eq!(
                output
                    .rows
                    .iter()
                    .map(|row| row["id"].clone())
                    .collect::<Vec<_>>(),
                vec![Value::Int(1), Value::Int(2)]
            );
            database.checkpoint().unwrap();
        }
        {
            let mut database = Database::open_with_config(&path, config).unwrap();
            let output = database
                .query_sql_with_params(
                    "WITH selected(v) AS (SELECT id FROM numbers WHERE n >= $1) \
                     SELECT input.id FROM (VALUES ($2), ($3), ($2)) input(id) \
                     WHERE input.id IN (SELECT v FROM selected) ORDER BY id",
                    &[Value::Int(10), Value::Int(1), Value::Int(3)],
                )
                .unwrap();
            assert_eq!(
                output
                    .rows
                    .iter()
                    .map(|row| row["id"].clone())
                    .collect::<Vec<_>>(),
                vec![Value::Int(1), Value::Int(1), Value::Int(3)]
            );
        }
        std::fs::remove_dir_all(path).unwrap();
    }
}

#[test]
fn recursive_ctes_read_persistent_snapshots_after_reopen() {
    for (residency, index) in [
        (
            StorageResidencyMode::Materialized,
            RelationalIndexMode::Materialized,
        ),
        (
            StorageResidencyMode::OutOfCore,
            RelationalIndexMode::DemandPaged,
        ),
    ] {
        let path = std::env::temp_dir().join(format!(
            "hawdb-recursive-cte-{}",
            hawdb_core::generate_uuidv7().unwrap()
        ));
        let config = DatabaseConfig {
            storage_residency_mode: residency,
            relational_index_mode: index,
            ..Default::default()
        };
        {
            let mut database = Database::open_with_config(&path, config.clone()).unwrap();
            database
                .query_sql(
                    "CREATE TABLE edges (id BIGINT PRIMARY KEY, parent BIGINT, child BIGINT)",
                )
                .unwrap();
            database
                .query_sql("INSERT INTO edges VALUES (1, 1, 2), (2, 2, 3), (3, 3, 1), (4, 2, 4)")
                .unwrap();
            let read = database.begin_read_transaction().unwrap();
            let profiled = read
                .query_sql_with_params_options_profiled(
                    "WITH RECURSIVE reach(id) AS (\
                         VALUES (1) UNION \
                         SELECT edges.child FROM edges JOIN reach ON edges.parent = reach.id\
                     ) SELECT id FROM reach ORDER BY id",
                    &[],
                    QueryStreamOptions::default(),
                )
                .unwrap();
            assert!(profiled.profile.row_read.recursive_iterations > 0);
            assert_eq!(profiled.profile.row_read.recursive_working_rows_peak, 2);
            assert_eq!(profiled.profile.row_read.recursive_rows_emitted, 4);
            assert!(profiled.profile.row_read.recursive_work >= 4);
            assert_eq!(
                profiled
                    .output
                    .rows
                    .iter()
                    .map(|row| row["id"].clone())
                    .collect::<Vec<_>>(),
                vec![Value::Int(1), Value::Int(2), Value::Int(3), Value::Int(4)]
            );
            drop(read);
            database.checkpoint().unwrap();
        }
        {
            let mut database = Database::open_with_config(&path, config).unwrap();
            let output = database
                .query_sql_with_params(
                    "WITH RECURSIVE walk(n) AS (\
                         VALUES ($1) UNION ALL SELECT n + 1 FROM walk WHERE n < $2\
                     ) SELECT n FROM walk ORDER BY n",
                    &[Value::Int(2), Value::Int(5)],
                )
                .unwrap();
            assert_eq!(
                output
                    .rows
                    .iter()
                    .map(|row| row["n"].clone())
                    .collect::<Vec<_>>(),
                vec![Value::Int(2), Value::Int(3), Value::Int(4), Value::Int(5)]
            );
        }
        std::fs::remove_dir_all(path).unwrap();
    }
}

#[test]
fn exists_binds_names_before_empty_scans_and_short_circuiting() {
    let mut database = Database::new();
    for table in ["t", "e"] {
        database
            .query_sql(&format!("CREATE TABLE {table} (id BIGINT PRIMARY KEY)"))
            .unwrap();
    }
    for populated in [false, true] {
        if populated {
            database.query_sql("INSERT INTO t VALUES (1)").unwrap();
            database.query_sql("INSERT INTO e VALUES (1)").unwrap();
        }
        for prefix in ["", "1 = 1 OR ", "1 = 0 AND "] {
            for inner in [
                "SELECT missing FROM e",
                "SELECT e.missing FROM e",
                "SELECT t.missing FROM e",
                "SELECT id FROM e ORDER BY missing",
                "SELECT id FROM e UNION SELECT id FROM t ORDER BY missing",
                "SELECT 1 FROM e WHERE t.missing = e.id",
                "SELECT 1 FROM e WHERE EXISTS (SELECT missing FROM t)",
                "WITH local(v) AS (VALUES (1)) SELECT missing FROM local",
            ] {
                let sql = format!("SELECT id FROM t WHERE {prefix}EXISTS ({inner})");
                let error = database.query_sql(&sql).expect_err(&sql);
                assert!(error.to_string().contains("missing"), "{sql}: {error}");
            }
            for inner in [
                "SELECT 1 / (id - id) FROM e",
                "SELECT 1 / (t.id - t.id) FROM e",
                "SELECT id AS v FROM e ORDER BY v",
                "SELECT id FROM e UNION SELECT id FROM t ORDER BY id",
                "SELECT 1 FROM e WHERE e.id = t.id",
                "WITH local(v) AS (VALUES (1)) SELECT v FROM local",
            ] {
                let sql = format!("SELECT id FROM t WHERE {prefix}EXISTS ({inner})");
                let result = database
                    .query_sql(&sql)
                    .unwrap_or_else(|error| panic!("{sql}: {error}"));
                assert_eq!(
                    result.rows.len(),
                    usize::from(populated && prefix != "1 = 0 AND "),
                    "{sql}"
                );
            }
        }
    }
}

#[test]
fn exists_join_on_uses_its_join_scope_and_outer_query() {
    let mut database = Database::new();
    for sql in [
        "CREATE TABLE pull_outer (id BIGINT PRIMARY KEY, outer_key BIGINT)",
        "CREATE TABLE a (id BIGINT PRIMARY KEY, outer_key BIGINT)",
        "CREATE TABLE b (id BIGINT PRIMARY KEY)",
        "CREATE TABLE c (k BIGINT PRIMARY KEY)",
        "INSERT INTO pull_outer VALUES (1, 1), (2, 2)",
        "INSERT INTO a VALUES (1, 99)",
        "INSERT INTO b VALUES (2)",
        "INSERT INTO c VALUES (2)",
    ] {
        database.query_sql(sql).unwrap();
    }
    for (prefix, all_rows) in [("", false), ("1 = 1 OR ", true)] {
        let query = format!(
            "SELECT id FROM pull_outer WHERE {prefix}EXISTS (\
                SELECT 1 FROM a, b JOIN c ON id = c.k AND outer_key = c.k\
            ) ORDER BY id"
        );
        let rows = database.query_sql(&query).unwrap().rows;
        let ids: Vec<_> = rows.iter().map(|row| row["id"].clone()).collect();
        assert_eq!(
            ids,
            if all_rows {
                vec![Value::Int(1), Value::Int(2)]
            } else {
                vec![Value::Int(2)]
            },
            "{query}"
        );

        let query = format!(
            "SELECT id FROM pull_outer WHERE {prefix}EXISTS (\
                SELECT 1 FROM a, b JOIN c ON a.id = c.k\
            )"
        );
        let error = database.query_sql(&query).expect_err(&query);
        assert!(matches!(error, crate::HawDBError::Semantic(_)), "{error}");
    }
}

#[test]
fn exists_recursive_cte_binds_implicit_columns_from_the_seed() {
    let mut database = Database::new();
    database
        .query_sql("CREATE TABLE t (id BIGINT PRIMARY KEY)")
        .unwrap();
    database.query_sql("INSERT INTO t VALUES (1)").unwrap();
    for prefix in ["", "1 = 1 OR "] {
        let query = format!(
            "SELECT id FROM t WHERE {prefix}EXISTS (\
                WITH RECURSIVE r AS (\
                    SELECT 1 AS n UNION ALL SELECT n + 1 FROM r WHERE n < 2\
                ) SELECT n FROM r WHERE n = 2\
            )"
        );
        assert_eq!(database.query_sql(&query).unwrap().rows.len(), 1, "{query}");
        let query = format!(
            "SELECT id FROM t WHERE {prefix}EXISTS (\
                WITH RECURSIVE r AS (SELECT 1 AS n UNION ALL SELECT missing FROM r)\
                SELECT n FROM r\
            )"
        );
        let error = database.query_sql(&query).expect_err(&query);
        assert!(
            matches!(error, crate::HawDBError::Semantic(message) if message.contains("missing")),
            "{query}"
        );
    }
}

#[test]
fn exists_with_order_tail_binds_to_projected_output() {
    let mut database = Database::new();
    database
        .query_sql("CREATE TABLE t (id BIGINT PRIMARY KEY)")
        .unwrap();
    database.query_sql("INSERT INTO t VALUES (1)").unwrap();
    for prefix in ["", "1 = 1 OR "] {
        let query = format!(
            "SELECT id FROM t WHERE {prefix}EXISTS (\
                WITH x(v) AS (VALUES (1)) SELECT v AS out_v FROM x ORDER BY out_v\
            )"
        );
        assert_eq!(database.query_sql(&query).unwrap().rows.len(), 1, "{query}");
        let query = format!(
            "SELECT id FROM t WHERE {prefix}EXISTS (\
                WITH x(v) AS (VALUES (1)) SELECT v AS out_v FROM x ORDER BY v\
            )"
        );
        let error = database.query_sql(&query).expect_err(&query);
        assert!(
            matches!(error, crate::HawDBError::Semantic(message) if message == "unknown set-operation ORDER BY column v"),
            "{query}"
        );
    }
}
