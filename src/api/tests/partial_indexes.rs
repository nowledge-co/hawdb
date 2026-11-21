// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

use super::*;

fn ids(database: &mut Database, sql: &str) -> Vec<i64> {
    database
        .query_sql(sql)
        .unwrap_or_else(|error| panic!("{sql}: {error}"))
        .rows
        .into_iter()
        .map(|row| match row.get("id") {
            Some(Value::Int(id)) => *id,
            value => panic!("unexpected id {value:?}"),
        })
        .collect()
}

fn explain_uses_partial_index(database: &mut Database, sql: &str) -> bool {
    database
        .query_sql(&format!("EXPLAIN {sql}"))
        .unwrap()
        .rows
        .iter()
        .any(|row| {
            matches!(row.get("id"), Some(Value::String(id)) if id.contains("IndexRangeScanExec"))
        })
}

fn assert_rows_and_plans(database: &mut Database) {
    let eligible = "SELECT id FROM items WHERE slug = 'same' AND active = TRUE";
    assert_eq!(ids(database, eligible), [1]);
    assert!(explain_uses_partial_index(database, eligible));

    let missing_proof = "SELECT id FROM items WHERE slug = 'same'";
    assert_eq!(ids(database, missing_proof), [1, 2, 3]);
    assert!(!explain_uses_partial_index(database, missing_proof));

    let indexdef = database
        .query_sql("SELECT indexdef FROM pg_indexes WHERE indexname = 'active_slug_idx'")
        .unwrap();
    assert!(matches!(
        indexdef.rows[0].get("indexdef"),
        Some(Value::String(definition)) if definition.contains("INCLUDE") && definition.contains("WHERE") && definition.contains("active")
    ));
}

#[test]
fn partial_index_survives_wal_checkpoint_and_reopen() {
    let path = unique_test_dir("partial_index_reopen");
    {
        let mut database = Database::open(&path).unwrap();
        database
            .query_sql(
                "CREATE TABLE items (id BIGINT PRIMARY KEY, slug TEXT NOT NULL, active BOOLEAN, note TEXT)",
            )
            .unwrap();
        database
            .query_sql("CREATE UNIQUE INDEX active_slug_idx ON items (slug) INCLUDE (active, note) WHERE active = TRUE")
            .unwrap();
        database
            .query_sql(
                "INSERT INTO items (id, slug, active) VALUES \
                 (1, 'same', TRUE), (2, 'same', FALSE), (3, 'same', NULL)",
            )
            .unwrap();
        let inactive = (10..110)
            .map(|id| format!("({id}, 'inactive-{id}', FALSE)"))
            .collect::<Vec<_>>()
            .join(", ");
        database
            .query_sql(&format!(
                "INSERT INTO items (id, slug, active) VALUES {inactive}"
            ))
            .unwrap();
        assert_rows_and_plans(&mut database);
    }

    {
        let mut database = Database::open_with_config(
            &path,
            DatabaseConfig {
                relational_index_mode: hawdb_storage::config::RelationalIndexMode::Shadow,
                ..DatabaseConfig::default()
            },
        )
        .unwrap();
        assert_rows_and_plans(&mut database);
        database.checkpoint().unwrap();
    }

    {
        let mut database = Database::open_with_config(
            &path,
            DatabaseConfig {
                storage_residency_mode: hawdb_storage::config::StorageResidencyMode::OutOfCore,
                relational_index_mode: hawdb_storage::config::RelationalIndexMode::Authoritative,
                ..DatabaseConfig::default()
            },
        )
        .unwrap();
        assert_rows_and_plans(&mut database);
        database
            .query_sql("UPDATE items SET note = 'new' WHERE id = 1")
            .unwrap();
        let covered = database
            .begin_read_transaction()
            .unwrap()
            .query_sql_with_params_options_profiled(
                "SELECT note FROM items WHERE slug = 'same' AND active = TRUE",
                &[],
                QueryStreamOptions::default(),
            )
            .unwrap();
        assert_eq!(covered.output.rows[0]["note"], Value::String("new".into()));
        assert!(
            covered.profile.operator_cardinality_profiles[0]
                .access_path
                .covering
        );
        assert!(database
            .query_sql("UPDATE items SET active = TRUE WHERE id = 2")
            .is_err());
        database
            .query_sql("UPDATE items SET active = FALSE WHERE id = 1")
            .unwrap();
        database
            .query_sql("UPDATE items SET active = TRUE WHERE id = 2")
            .unwrap();
        assert_eq!(
            ids(
                &mut database,
                "SELECT id FROM items WHERE slug = 'same' AND active = TRUE"
            ),
            [2]
        );
    }

    std::fs::remove_dir_all(path).unwrap();
}
