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

use crate::{Database, Value};

#[test]
fn reordered_foreign_keys_preserve_pairing_cascades_and_recovery() {
    use hawdb_storage::config::{RelationalIndexMode, StorageResidencyMode};
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
            "hawdb-pg-foreign-{}",
            hawdb_core::generate_uuidv7().unwrap()
        ));
        let config = crate::DatabaseConfig {
            storage_residency_mode: residency,
            relational_index_mode: index,
            ..Default::default()
        };
        let mut database = Database::open_with_config(&path, config.clone()).unwrap();
        for sql in [
            "CREATE TABLE parent (a BIGINT, b TEXT, code BIGINT, tag TEXT, PRIMARY KEY (a, b), UNIQUE (code, tag))",
            "CREATE TABLE child_pk (id BIGINT PRIMARY KEY, cb TEXT, ca BIGINT, FOREIGN KEY (cb, ca) REFERENCES parent (b, a) ON DELETE CASCADE)",
            "CREATE TABLE child_unique (id BIGINT PRIMARY KEY, ct TEXT, cc BIGINT, FOREIGN KEY (ct, cc) REFERENCES parent (tag, code) ON DELETE CASCADE)",
            "CREATE TABLE child_implicit (id BIGINT PRIMARY KEY, ca BIGINT, cb TEXT, FOREIGN KEY (ca, cb) REFERENCES parent ON DELETE CASCADE)",
            "INSERT INTO parent VALUES (1, 'p', 42, 'tag')",
            "INSERT INTO child_pk VALUES (1, 'p', 1), (2, NULL, 999)",
            "INSERT INTO child_unique VALUES (1, 'tag', 42)",
            "INSERT INTO child_implicit VALUES (1, 1, 'p')",
        ] {
            database.query_sql(sql).unwrap_or_else(|error| panic!("{sql}: {error}"));
        }
        for sql in [
            "INSERT INTO child_pk VALUES (3, 'missing', 1)",
            "INSERT INTO child_unique VALUES (3, 'tag', 1)",
            "CREATE TABLE invalid (id BIGINT PRIMARY KEY, a BIGINT, b BIGINT, FOREIGN KEY (a, b) REFERENCES parent (a, a))",
            "CREATE TABLE invalid (id BIGINT PRIMARY KEY, a BIGINT, FOREIGN KEY (a) REFERENCES parent (a))",
        ] {
            assert!(database.query_sql(sql).is_err(), "{sql}");
        }
        database.checkpoint().unwrap();
        assert!(database
            .query_sql("UPDATE parent SET a = 2, tag = 'next' WHERE a = 1 AND b = 'p'")
            .is_err());
        drop(database);
        let mut database = Database::open_with_config(&path, config).unwrap();
        assert_eq!(
            database
                .query_sql("SELECT ca FROM child_pk WHERE id = 1")
                .unwrap()
                .rows[0]["ca"],
            Value::Int(1)
        );
        assert_eq!(
            database
                .query_sql("SELECT ct FROM child_unique WHERE id = 1")
                .unwrap()
                .rows[0]["ct"],
            Value::String("tag".into())
        );
        assert_eq!(
            database
                .query_sql("SELECT ca FROM child_implicit WHERE id = 1")
                .unwrap()
                .rows[0]["ca"],
            Value::Int(1)
        );
        database
            .query_sql("DELETE FROM parent WHERE a = 1 AND b = 'p'")
            .unwrap();
        assert_eq!(
            database.query_sql("SELECT id FROM child_pk").unwrap().rows[0]["id"],
            Value::Int(2)
        );
        assert!(database
            .query_sql("SELECT id FROM child_unique")
            .unwrap()
            .rows
            .is_empty());
        assert!(database
            .query_sql("SELECT id FROM child_implicit")
            .unwrap()
            .rows
            .is_empty());
        drop(database);
        std::fs::remove_dir_all(path).unwrap();
    }
}

#[test]
fn comma_join_uses_composite_keys_and_rebinds_constants() {
    let mut database = Database::new();
    database
        .query_sql("CREATE TABLE orders (id BIGINT PRIMARY KEY, warehouse BIGINT, item BIGINT)")
        .unwrap();
    database.query_sql("CREATE TABLE stock (warehouse BIGINT, item BIGINT, quantity BIGINT, PRIMARY KEY (warehouse, item))").unwrap();
    database
        .query_sql("INSERT INTO orders VALUES (1, 1, 42), (2, 1, 43), (3, 1, 42), (4, 2, 42)")
        .unwrap();
    let rows = (0..1000)
        .flat_map(|item| [format!("(1, {item}, 1)"), format!("(2, {item}, 2)")])
        .collect::<Vec<_>>()
        .join(",");
    database
        .query_sql(&format!("INSERT INTO stock VALUES {rows}"))
        .unwrap();
    let sql = "SELECT COUNT(DISTINCT (o.item)) AS n FROM orders o, stock s WHERE o.warehouse = $1 AND s.warehouse = $2 AND s.item = o.item AND s.quantity < $3";
    for (left, right, quantity, expected) in
        [(1, 1, 2, 2), (1, 2, 2, 0), (2, 2, 3, 1), (1, 1, 1, 0)]
    {
        let parameters = [Value::Int(left), Value::Int(right), Value::Int(quantity)];
        assert_eq!(
            database
                .query_sql_with_params(sql, &parameters)
                .unwrap()
                .rows[0]["n"],
            Value::Int(expected)
        );
        if left == right {
            let plan = database
                .query_sql_with_params(&format!("EXPLAIN {sql}"), &parameters)
                .unwrap();
            assert!(plan.rows.iter().any(|row| matches!(row.get("id"), Some(Value::String(id)) if id.contains("IndexNestedLoop"))), "{plan:?}");
        }
    }
    let outer = database.query_sql("SELECT o.id AS id, s.quantity AS quantity FROM orders o LEFT JOIN stock s ON o.item = s.item AND s.warehouse = o.warehouse WHERE s.quantity IS NULL OR o.id = 1 ORDER BY o.id").unwrap();
    assert_eq!(outer.rows.len(), 1);
    assert_eq!(outer.rows[0]["id"], Value::Int(1));
}

#[test]
fn implicit_insert_preserves_catalog_order_defaults_parameters_and_atomicity() {
    let mut database = Database::new();
    database
        .query_sql("CREATE TABLE items (z BIGINT PRIMARY KEY, a TEXT, n BIGINT DEFAULT '7')")
        .unwrap();
    database
        .query_sql_with_params(
            "INSERT INTO items VALUES ($1, $2), ($3, $4)",
            &[
                Value::Int(1),
                Value::String("one".into()),
                Value::Int(2),
                Value::Null,
            ],
        )
        .unwrap();
    database
        .query_sql("INSERT INTO items (a, z, n) VALUES ('three', 3, 9)")
        .unwrap();
    database
        .query_sql("INSERT INTO items VALUES (4) RETURNING z, n")
        .unwrap();
    database
        .query_sql("INSERT INTO items VALUES (1, 'ignored') ON CONFLICT (z) DO NOTHING")
        .unwrap();
    database.query_sql("INSERT INTO items VALUES (3, 'changed', 11) ON CONFLICT (z) DO UPDATE SET a = excluded.a, n = excluded.n").unwrap();
    for sql in [
        "INSERT INTO items VALUES (5, 'five'), (6)",
        "INSERT INTO items VALUES (5, 'five', 1, 2)",
        "INSERT INTO items (z, a) VALUES (5)",
        "INSERT INTO items (z, z) VALUES (5, 6)",
        "INSERT INTO items VALUES (5, 'five'), (1, 'duplicate')",
        "INSERT INTO items VALUES (5, 'five'), (NULL, 'missing key')",
    ] {
        assert!(database.query_sql(sql).is_err(), "{sql}");
    }
    let rows = database
        .query_sql("SELECT z, a, n FROM items ORDER BY z")
        .unwrap()
        .rows;
    assert_eq!(rows.len(), 4);
    for (index, row) in rows.iter().enumerate() {
        assert_eq!(row["z"], Value::Int(index as i64 + 1));
        assert_eq!(row["n"], Value::Int(if index == 2 { 11 } else { 7 }));
    }
    assert_eq!(rows[0]["a"], Value::String("one".into()));
    assert_eq!(rows[1]["a"], Value::Null);
    assert_eq!(rows[2]["a"], Value::String("changed".into()));
    assert_eq!(rows[3]["a"], Value::Null);
    database
        .query_sql("ALTER TABLE items ADD COLUMN trailing TEXT DEFAULT 'new'")
        .unwrap();
    database
        .query_sql("INSERT INTO items VALUES (5, 'five')")
        .unwrap();
    assert_eq!(
        database
            .query_sql("SELECT trailing FROM items WHERE z = 5")
            .unwrap()
            .rows[0]["trailing"],
        Value::String("new".into())
    );
}

#[test]
fn parenthesized_distinct_arguments_preserve_null_and_empty_semantics() {
    let mut database = Database::new();
    database
        .query_sql("CREATE TABLE items (id BIGINT PRIMARY KEY, value BIGINT)")
        .unwrap();
    for sql in [
        "SELECT COUNT(DISTINCT(value)) AS n FROM items",
        "SELECT COUNT(DISTINCT ((items.value))) AS n FROM items",
    ] {
        assert_eq!(database.query_sql(sql).unwrap().rows[0]["n"], Value::Int(0));
    }
    database
        .query_sql("INSERT INTO items (id, value) VALUES (1, 4), (2, 4), (3, 5), (4, NULL)")
        .unwrap();
    assert_eq!(
        database
            .query_sql("SELECT COUNT(DISTINCT ((value))) AS n FROM items")
            .unwrap()
            .rows[0]["n"],
        Value::Int(2)
    );
    assert_eq!(
        database
            .query_sql_with_params(
                "SELECT COUNT(DISTINCT (value)) AS n FROM items WHERE value < $1",
                &[Value::Int(6)]
            )
            .unwrap()
            .rows[0]["n"],
        Value::Int(2)
    );
}

#[test]
fn type_aliases_report_canonical_storage_types() {
    let mut database = Database::new();
    for (index, (declaration, catalog, value)) in [
        ("INT", "bigint", Value::Int(42)),
        ("INTEGER", "bigint", Value::Int(42)),
        ("SMALLINT", "bigint", Value::Int(42)),
        ("TINYINT", "bigint", Value::Int(42)),
        ("INT2", "bigint", Value::Int(42)),
        ("INT4", "bigint", Value::Int(42)),
        ("FLOAT", "double precision", Value::Float(1.25)),
        ("FLOAT(24)", "double precision", Value::Float(1.25)),
        ("REAL", "double precision", Value::Float(1.25)),
        ("FLOAT4", "double precision", Value::Float(1.25)),
        ("VARCHAR(2)", "text", Value::String("longer".into())),
        ("CHARACTER VARYING", "text", Value::String("longer".into())),
    ]
    .into_iter()
    .enumerate()
    {
        let table = format!("alias_{index}");
        database
            .query_sql(&format!(
                "CREATE TABLE {table} (id BIGINT PRIMARY KEY, value {declaration})"
            ))
            .unwrap();
        database
            .query_sql_with_params(
                &format!("INSERT INTO {table} (id, value) VALUES (1, $1)"),
                std::slice::from_ref(&value),
            )
            .unwrap();
        assert_eq!(
            database
                .query_sql(&format!("SELECT value FROM {table}"))
                .unwrap()
                .rows[0]["value"],
            value
        );
        assert_eq!(database.query_sql(&format!("SELECT data_type FROM information_schema.columns WHERE table_name = '{table}' AND column_name = 'value'")).unwrap().rows[0]["data_type"], Value::String(catalog.into()));
    }
    for declaration in [
        "FLOAT(0)",
        "FLOAT(54)",
        "VARCHAR(0)",
        "VARCHAR(MAX)",
        "INT(4)",
    ] {
        assert!(
            database
                .query_sql(&format!(
                    "CREATE TABLE invalid (id BIGINT PRIMARY KEY, value {declaration})"
                ))
                .is_err(),
            "{declaration}"
        );
    }
}

#[test]
fn string_defaults_are_bound_and_invalid_defaults_leave_no_schema() {
    let mut database = Database::new();
    database.query_sql("CREATE TABLE defaults (id BIGINT PRIMARY KEY, n BIGINT DEFAULT ' -42 ', f DOUBLE PRECISION DEFAULT '1.25', b BOOLEAN DEFAULT 'yes')").unwrap();
    database
        .query_sql("INSERT INTO defaults (id) VALUES (1)")
        .unwrap();
    let output = database.query_sql("SELECT n, f, b FROM defaults").unwrap();
    assert_eq!(output.rows[0]["n"], Value::Int(-42));
    assert_eq!(output.rows[0]["f"], Value::Float(1.25));
    assert_eq!(output.rows[0]["b"], Value::Bool(true));
    for declaration in [
        "BIGINT DEFAULT '1.5'",
        "BIGINT DEFAULT '9223372036854775808'",
        "DOUBLE PRECISION DEFAULT 'oops'",
        "BOOLEAN DEFAULT 'o'",
        "BOOLEAN DEFAULT ''",
    ] {
        assert!(
            database
                .query_sql(&format!(
                    "CREATE TABLE invalid (id BIGINT PRIMARY KEY, n {declaration})"
                ))
                .is_err(),
            "{declaration}"
        );
        assert!(database.query_sql("SELECT id FROM invalid").is_err());
        assert!(database
            .query_sql(&format!(
                "ALTER TABLE defaults ADD COLUMN invalid {declaration}"
            ))
            .is_err());
    }
    assert_eq!(
        database
            .query_sql("SELECT COUNT(*) AS n FROM defaults")
            .unwrap()
            .rows[0]["n"],
        Value::Int(1)
    );
}

#[test]
fn keyless_tables_preserve_duplicates_indexes_and_recovery() {
    use hawdb_storage::config::{RelationalIndexMode, StorageResidencyMode};
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
            "hawdb-keyless-{}",
            hawdb_core::generate_uuidv7().unwrap()
        ));
        let config = crate::DatabaseConfig {
            storage_residency_mode: residency,
            relational_index_mode: index,
            ..Default::default()
        };
        let mut database = Database::open_with_config(&path, config.clone()).unwrap();
        for sql in [
            "CREATE TABLE bag (n BIGINT, label TEXT DEFAULT 'default')",
            "CREATE INDEX bag_n ON bag (n)",
            "INSERT INTO bag VALUES (1, 'same'), (1, 'same'), (NULL, NULL)",
            "CREATE TABLE keyed_by_unique (code BIGINT UNIQUE, label TEXT)",
            "INSERT INTO keyed_by_unique VALUES (1, 'first'), (NULL, 'null'), (NULL, 'null')",
        ] {
            database
                .query_sql(sql)
                .unwrap_or_else(|e| panic!("{sql}: {e}"));
        }
        let rows = database
            .query_sql("SELECT * FROM bag WHERE n = 1")
            .unwrap()
            .rows;
        assert_eq!(rows.len(), 2);
        assert!(rows.iter().all(|row| row.len() == 2));
        database.checkpoint().unwrap();
        for sql in [
            "UPDATE bag SET n = 2 WHERE n = 1",
            "INSERT INTO bag VALUES (2, 'same')",
            "INSERT INTO keyed_by_unique VALUES (1, 'updated') ON CONFLICT (code) DO UPDATE SET label = excluded.label",
        ] { database.query_sql(sql).unwrap_or_else(|e| panic!("{sql}: {e}")); }
        assert!(database
            .query_sql("INSERT INTO keyed_by_unique VALUES (2, 'rollback'), (1, 'duplicate')")
            .is_err());
        drop(database);
        let mut database = Database::open_with_config(&path, config.clone()).unwrap();
        assert_eq!(
            database
                .query_sql("SELECT * FROM bag WHERE n = 2")
                .unwrap()
                .rows
                .len(),
            3
        );
        assert_eq!(
            database
                .query_sql("SELECT * FROM bag WHERE n IS NULL")
                .unwrap()
                .rows
                .len(),
            1
        );
        assert_eq!(
            database
                .query_sql("SELECT label FROM keyed_by_unique WHERE code = 1")
                .unwrap()
                .rows[0]["label"],
            Value::String("updated".into())
        );
        assert!(database
            .query_sql("SELECT * FROM keyed_by_unique WHERE code = 2")
            .unwrap()
            .rows
            .is_empty());
        database
            .query_sql("DELETE FROM bag WHERE n = 2 OR n IS NULL")
            .unwrap();
        database.checkpoint().unwrap();
        drop(database);
        let mut database = Database::open_with_config(&path, config).unwrap();
        database
            .query_sql("INSERT INTO bag VALUES (7), (7)")
            .unwrap();
        assert_eq!(
            database.query_sql("SELECT * FROM bag").unwrap().rows.len(),
            2
        );
        database
            .query_sql("ALTER TABLE bag ADD COLUMN extra TEXT DEFAULT 'added'")
            .unwrap();
        assert_eq!(
            database
                .query_sql("SELECT extra FROM bag WHERE n = 7")
                .unwrap()
                .rows
                .len(),
            2
        );
        drop(database);
        std::fs::remove_dir_all(path).unwrap();
    }
}
