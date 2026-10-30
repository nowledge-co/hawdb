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

mod extrema;

fn base(db: &mut Database) {
    db.query_sql("CREATE TABLE events (id BIGINT PRIMARY KEY, bucket TEXT, amount BIGINT, enabled BOOLEAN NOT NULL)").unwrap();
}

fn assert_grouped_matches(db: &mut Database) {
    let expected = db.query_sql("SELECT bucket, COUNT(*) AS n, COUNT(amount) AS present, SUM(amount) AS total FROM events WHERE enabled = true GROUP BY bucket").unwrap();
    let actual = db
        .query_sql("SELECT * FROM totals ORDER BY bucket")
        .unwrap();
    let normalize = |output: QueryOutput| {
        let mut rows = output
            .rows
            .iter()
            .map(|row| format!("{row:?}"))
            .collect::<Vec<_>>();
        rows.sort();
        rows
    };
    assert_eq!(normalize(actual), normalize(expected));
}

#[test]
fn materialized_projection_preserves_duplicates_and_retracts_updates() {
    let mut db = Database::new();
    base(&mut db);
    db.query_sql("INSERT INTO events (id, bucket, amount, enabled) VALUES (1, 'a', 10, true), (2, 'a', 10, true), (3, 'b', 3, false)").unwrap();
    db.query_sql(
        "CREATE MATERIALIZED VIEW active AS SELECT bucket, amount FROM events WHERE enabled = true",
    )
    .unwrap();
    assert_eq!(db.query_sql("SELECT * FROM active").unwrap().rows.len(), 2);
    assert_eq!(
        db.query_sql("SELECT * FROM active").unwrap().rows[0].len(),
        2
    );
    db.query_sql("UPDATE events SET id = 4, amount = 20 WHERE id = 1")
        .unwrap();
    db.query_sql("UPDATE events SET enabled = false WHERE id = 2")
        .unwrap();
    db.query_sql("UPDATE events SET enabled = true WHERE id = 3")
        .unwrap();
    assert_eq!(
        db.query_sql("SELECT * FROM active ORDER BY bucket")
            .unwrap()
            .rows,
        db.query_sql("SELECT bucket, amount FROM events WHERE enabled = true ORDER BY bucket")
            .unwrap()
            .rows
    );
    db.query_sql("DELETE FROM events WHERE id = 4").unwrap();
    assert_eq!(
        db.query_sql("SELECT amount FROM active").unwrap().rows[0]["amount"],
        Value::Int(3)
    );
    assert!(db.query_sql("SELECT __hawdb_mv_key FROM active").is_err());
    let columns = db.query_sql("SELECT column_name FROM information_schema.columns WHERE table_name = 'active' ORDER BY ordinal_position").unwrap();
    assert_eq!(columns.rows.len(), 2);
    let table = db.query_sql("SELECT table_type, is_insertable_into FROM information_schema.tables WHERE table_name = 'active'").unwrap();
    assert_eq!(
        table.rows[0]["table_type"],
        Value::String("MATERIALIZED VIEW".into())
    );
    assert_eq!(
        table.rows[0]["is_insertable_into"],
        Value::String("NO".into())
    );
}

#[test]
fn materialized_grouped_changes_match_full_select() {
    let mut db = Database::new();
    base(&mut db);
    db.query_sql("INSERT INTO events (id, bucket, amount, enabled) VALUES (1, 'a', 10, true), (2, NULL, NULL, true), (3, 'a', -10, true), (4, 'b', 7, false)").unwrap();
    db.query_sql("CREATE MATERIALIZED VIEW totals AS SELECT bucket, COUNT(*) AS n, COUNT(amount) AS present, SUM(amount) AS total FROM events WHERE enabled = true GROUP BY bucket").unwrap();
    assert_grouped_matches(&mut db);
    for statement in [
        "UPDATE events SET bucket = NULL, amount = 12 WHERE id = 1",
        "UPDATE events SET amount = 5 WHERE id = 2",
        "UPDATE events SET enabled = true WHERE id = 4",
        "UPDATE events SET amount = NULL WHERE id = 4",
        "DELETE FROM events WHERE id = 3",
        "DELETE FROM events WHERE id = 1",
        "DELETE FROM events WHERE id = 2",
        "DELETE FROM events WHERE id = 4",
    ] {
        db.query_sql(statement).unwrap();
        assert_grouped_matches(&mut db);
    }
    for step in 0..64 {
        let id = step % 7;
        if step % 5 == 0 {
            db.query_sql_with_params("DELETE FROM events WHERE id = $1", &[Value::Int(id)])
                .unwrap();
        } else {
            db.query_sql_with_params(
                "INSERT INTO events (id, bucket, amount, enabled) VALUES ($1, $2, $3, $4) ON CONFLICT (id) DO UPDATE SET bucket = EXCLUDED.bucket, amount = EXCLUDED.amount, enabled = EXCLUDED.enabled",
                &[Value::Int(id), if step % 3 == 0 { Value::Null } else { Value::String(format!("b{}", step % 4)) }, if step % 4 == 0 { Value::Null } else { Value::Int(step - 32) }, Value::Bool(step % 3 != 1)],
            ).unwrap();
        }
        assert_grouped_matches(&mut db);
    }
}

#[test]
fn materialized_maintenance_is_bounded_and_uses_only_changed_rows() {
    use hawdb_storage::relational::{
        RelationalMutationLimits, RelationalOverflowConfig, RelationalValue,
    };
    let mut db = Database::new();
    base(&mut db);
    for id in 0..20 {
        db.query_sql_with_params(
            "INSERT INTO events (id, amount, enabled) VALUES ($1, 1, true)",
            &[Value::Int(id)],
        )
        .unwrap();
    }
    let limit = |rows, bytes| RelationalMutationLimits {
        max_rows: NonZeroUsize::new(rows).unwrap(),
        max_payload_bytes: NonZeroUsize::new(bytes).unwrap(),
    };
    let state = db.runtime.get().unwrap().store.relational_state().clone();
    let create = crate::relational_sql::compile_relational_statement_sql(
        "CREATE MATERIALIZED VIEW stats AS SELECT COUNT(*) AS n FROM events",
        &[],
        &state,
    )
    .unwrap();
    assert!(state
        .stage_transaction(create, limit(2, 4096), RelationalOverflowConfig::default())
        .is_err());
    assert!(state.table_schema("stats").is_none());
    db.query_sql("CREATE MATERIALIZED VIEW stats AS SELECT COUNT(*) AS n FROM events")
        .unwrap();
    let state = db.runtime.get().unwrap().store.relational_state().clone();
    let insert = crate::relational_sql::compile_relational_statement_sql(
        "INSERT INTO events (id, amount, enabled) VALUES (100, 1, true)",
        &[],
        &state,
    )
    .unwrap();
    assert!(state
        .stage_transaction(
            insert.clone(),
            limit(2, 4096),
            RelationalOverflowConfig::default()
        )
        .is_err());
    assert!(state
        .stage_transaction(
            insert.clone(),
            limit(3, 32),
            RelationalOverflowConfig::default()
        )
        .is_err());
    // One new source row, one old aggregate row, and one published aggregate
    // row fit; a rescan of the twenty-row base table cannot fit this budget.
    let staged = state
        .stage_transaction(insert, limit(3, 4096), RelationalOverflowConfig::default())
        .unwrap();
    assert_eq!(
        staged.rows("stats").next().unwrap().1.values()[0],
        RelationalValue::BigInt(21)
    );
    assert_eq!(
        state.rows("stats").next().unwrap().1.values()[0],
        RelationalValue::BigInt(20)
    );
}

#[test]
fn materialized_projection_amplification_obeys_payload_budget() {
    use hawdb_storage::relational::{RelationalMutationLimits, RelationalOverflowConfig};
    let mut db = Database::new();
    base(&mut db);
    db.query_sql_with_params(
        "INSERT INTO events (id, bucket, enabled) VALUES (1, $1, true)",
        &[Value::String("x".repeat(1024))],
    )
    .unwrap();
    let columns = (0..16)
        .map(|position| format!("bucket AS copy_{position}"))
        .collect::<Vec<_>>()
        .join(", ");
    for group in [
        "",
        " GROUP BY bucket",
        " GROUP BY bucket, bucket, bucket, bucket",
    ] {
        let state = db.runtime.get().unwrap().store.relational_state().clone();
        let create = crate::relational_sql::compile_relational_statement_sql(
            &format!("CREATE MATERIALIZED VIEW wide AS SELECT {columns} FROM events{group}"),
            &[],
            &state,
        )
        .unwrap();
        let error = state
            .stage_transaction(
                create,
                RelationalMutationLimits {
                    max_rows: NonZeroUsize::new(10).unwrap(),
                    max_payload_bytes: NonZeroUsize::new(4096).unwrap(),
                },
                RelationalOverflowConfig::default(),
            )
            .unwrap_err();
        assert!(error.to_string().contains("budget"), "{error}");
        assert!(state.table_schema("wide").is_none());
    }
}

#[test]
fn materialized_large_values_and_schema_extension_remain_consistent() {
    let path = unique_test_dir("materialized_overflow_values");
    let payload = "large-value".repeat(2000);
    {
        let mut db = Database::open(&path).unwrap();
        base(&mut db);
        db.query_sql_with_params(
            "INSERT INTO events (id, bucket, amount, enabled) VALUES (1, $1, 3, true)",
            &[Value::String(payload.clone())],
        )
        .unwrap();
        db.query_sql(
            "CREATE MATERIALIZED VIEW mv AS SELECT bucket FROM events WHERE enabled = true",
        )
        .unwrap();
        db.query_sql("ALTER TABLE events ADD COLUMN extra TEXT")
            .unwrap();
        db.checkpoint().unwrap();
    }
    {
        let mut db = Database::open(&path).unwrap();
        assert_eq!(
            db.query_sql("SELECT * FROM mv").unwrap().rows[0]["bucket"],
            Value::String(payload)
        );
        db.query_sql("UPDATE events SET bucket = 'short' WHERE id = 1")
            .unwrap();
        assert_eq!(
            db.query_sql("SELECT * FROM mv").unwrap().rows[0]["bucket"],
            Value::String("short".into())
        );
        db.query_sql("DELETE FROM events WHERE id = 1").unwrap();
        assert!(db.query_sql("SELECT * FROM mv").unwrap().rows.is_empty());
        db.checkpoint().unwrap();
    }
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn materialized_torn_transaction_fails_closed_without_discarding_evidence() {
    let path = unique_test_dir("materialized_torn_wal");
    {
        let mut db = Database::open(&path).unwrap();
        base(&mut db);
        db.query_sql("CREATE MATERIALIZED VIEW stats AS SELECT COUNT(*) AS n, SUM(amount) AS total FROM events").unwrap();
        db.checkpoint().unwrap();
        let mut tx = db.begin_transaction().unwrap();
        tx.query_sql("INSERT INTO events (id, amount, enabled) VALUES (1, 2, true)")
            .unwrap();
        tx.query_sql("INSERT INTO events (id, amount, enabled) VALUES (2, 3, true)")
            .unwrap();
        tx.commit().unwrap();
    }
    let wal = active_wal_path(&path);
    let complete = std::fs::read(&wal).unwrap();
    let torn = &complete[..complete.len() - 5];
    std::fs::write(&wal, torn).unwrap();
    assert!(Database::open_with_config(
        &path,
        crate::DatabaseConfig {
            recovery_mode: crate::RecoveryMode::Strict,
            ..Default::default()
        },
    )
    .and_then(|mut db| db.query_sql("SELECT * FROM stats"))
    .is_err());
    assert_eq!(std::fs::read(&wal).unwrap(), torn);
    std::fs::write(&wal, complete).unwrap();
    {
        let mut db = Database::open(&path).unwrap();
        let row = db.query_sql("SELECT * FROM stats").unwrap();
        assert_eq!(row.rows[0]["n"], Value::Int(2));
        assert_eq!(row.rows[0]["total"], Value::Int(5));
    }
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn materialized_persistent_index_modes_keep_updates_and_drop_consistent() {
    use hawdb_storage::config::RelationalIndexMode;
    for mode in [
        RelationalIndexMode::Shadow,
        RelationalIndexMode::DemandPaged,
    ] {
        let path = unique_test_dir("materialized_persistent_indexes");
        let config = DatabaseConfig {
            relational_index_mode: mode,
            ..DatabaseConfig::default()
        };
        {
            let mut db = Database::open_with_config(&path, config.clone()).unwrap();
            base(&mut db);
            db.query_sql("CREATE MATERIALIZED VIEW stats AS SELECT COUNT(*) AS n, SUM(amount) AS total FROM events").unwrap();
            db.query_sql("INSERT INTO events (id, amount, enabled) VALUES (1, 3, true)")
                .unwrap();
            db.checkpoint().unwrap();
            db.query_sql("UPDATE events SET amount = 9 WHERE id = 1")
                .unwrap();
        }
        {
            let mut db = Database::open_with_config(&path, config.clone()).unwrap();
            assert_eq!(
                db.query_sql("SELECT total FROM stats").unwrap().rows[0]["total"],
                Value::Int(9)
            );
            db.query_sql("DELETE FROM events WHERE id = 1").unwrap();
            assert_eq!(
                db.query_sql("SELECT n FROM stats").unwrap().rows[0]["n"],
                Value::Int(0)
            );
            db.query_sql("DROP MATERIALIZED VIEW stats").unwrap();
            db.checkpoint().unwrap();
        }
        {
            let mut db = Database::open_with_config(&path, config).unwrap();
            assert!(db.query_sql("SELECT * FROM stats").is_err());
            db.query_sql("CREATE MATERIALIZED VIEW stats AS SELECT COUNT(*) AS n FROM events")
                .unwrap();
            assert_eq!(
                db.query_sql("SELECT n FROM stats").unwrap().rows[0]["n"],
                Value::Int(0)
            );
        }
        std::fs::remove_dir_all(path).unwrap();
    }
}

#[test]
fn materialized_foreign_key_cascades_retract_child_rows() {
    let mut db = Database::new();
    db.query_sql("CREATE TABLE parents (id BIGINT PRIMARY KEY)")
        .unwrap();
    db.query_sql("CREATE TABLE children (id BIGINT PRIMARY KEY, parent BIGINT REFERENCES parents(id) ON DELETE CASCADE)").unwrap();
    db.query_sql("INSERT INTO parents (id) VALUES (1), (2)")
        .unwrap();
    db.query_sql("INSERT INTO children (id, parent) VALUES (10, 1), (11, 1), (12, 2)")
        .unwrap();
    db.query_sql("CREATE INDEX children_parent ON children (parent)")
        .unwrap();
    db.query_sql("CREATE MATERIALIZED VIEW counts AS SELECT parent, COUNT(*) AS n, MIN(id) AS first_child FROM children GROUP BY parent").unwrap();
    db.query_sql("DELETE FROM parents WHERE id = 1").unwrap();
    let rows = db.query_sql("SELECT * FROM counts").unwrap().rows;
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0]["parent"], Value::Int(2));
    assert_eq!(rows[0]["n"], Value::Int(1));
    assert_eq!(rows[0]["first_child"], Value::Int(12));
}

#[test]
fn materialized_authoritative_reopen_is_explicitly_rejected() {
    use hawdb_storage::config::{RelationalIndexMode, StorageResidencyMode};
    let path = unique_test_dir("materialized_authoritative");
    {
        let mut db = Database::open_with_config(
            &path,
            DatabaseConfig {
                relational_index_mode: RelationalIndexMode::Shadow,
                ..DatabaseConfig::default()
            },
        )
        .unwrap();
        base(&mut db);
        db.query_sql("CREATE MATERIALIZED VIEW stats AS SELECT COUNT(*) AS n FROM events")
            .unwrap();
        db.checkpoint().unwrap();
    }
    for mode in [
        StorageResidencyMode::Materialized,
        StorageResidencyMode::OutOfCore,
    ] {
        let error = Database::open_with_config(
            &path,
            DatabaseConfig {
                relational_index_mode: RelationalIndexMode::Authoritative,
                storage_residency_mode: mode,
                ..DatabaseConfig::default()
            },
        )
        .and_then(|mut db| db.query_sql("SELECT * FROM stats"))
        .unwrap_err();
        assert!(error.to_string().contains("materialized views"), "{error}");
    }
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn materialized_global_empty_state_and_rollback() {
    let mut db = Database::new();
    base(&mut db);
    db.query_sql(
        "CREATE MATERIALIZED VIEW stats AS SELECT COUNT(*) AS n, SUM(amount) AS total FROM events",
    )
    .unwrap();
    let empty = db.query_sql("SELECT * FROM stats").unwrap();
    assert_eq!(empty.rows[0]["n"], Value::Int(0));
    assert_eq!(empty.rows[0]["total"], Value::Null);
    {
        let mut tx = db.begin_transaction().unwrap();
        tx.query_sql("INSERT INTO events (id, bucket, amount, enabled) VALUES (1, 'a', 4, true)")
            .unwrap();
        assert_eq!(
            tx.query_sql("SELECT n FROM stats").unwrap().rows[0]["n"],
            Value::Int(1)
        );
    }
    assert_eq!(
        db.query_sql("SELECT * FROM stats").unwrap().rows,
        empty.rows
    );
    let epoch = db.commit_epoch().unwrap();
    let mut tx = db.begin_transaction().unwrap();
    tx.query_sql("INSERT INTO events (id, bucket, amount, enabled) VALUES (1, 'a', 4, true)")
        .unwrap();
    tx.query_sql("INSERT INTO events (id, bucket, amount, enabled) VALUES (2, 'a', 6, true)")
        .unwrap();
    tx.commit().unwrap();
    assert_eq!(db.commit_epoch().unwrap(), epoch + 1);
    assert_eq!(
        db.query_sql("SELECT total FROM stats").unwrap().rows[0]["total"],
        Value::Int(10)
    );
    db.query_sql("DELETE FROM events WHERE enabled = true")
        .unwrap();
    assert_eq!(
        db.query_sql("SELECT * FROM stats").unwrap().rows,
        empty.rows
    );
}

#[test]
fn materialized_overflow_rejects_the_entire_transaction() {
    let mut db = Database::new();
    base(&mut db);
    db.query_sql("CREATE MATERIALIZED VIEW stats AS SELECT SUM(amount) AS total FROM events")
        .unwrap();
    db.query_sql("CREATE MATERIALIZED VIEW ids AS SELECT id FROM events")
        .unwrap();
    db.query_sql("INSERT INTO events (id, bucket, amount, enabled) VALUES (1, 'a', 9223372036854775807, true)").unwrap();
    let epoch = db.commit_epoch().unwrap();
    let error = db
        .query_sql("INSERT INTO events (id, bucket, amount, enabled) VALUES (2, 'a', 1, true)")
        .unwrap_err();
    assert!(error.to_string().contains("overflow"));
    assert_eq!(db.commit_epoch().unwrap(), epoch);
    assert_eq!(db.query_sql("SELECT * FROM events").unwrap().rows.len(), 1);
    assert_eq!(db.query_sql("SELECT * FROM ids").unwrap().rows.len(), 1);
    // Removing MIN and adding MAX must not overflow an intermediate i64 sum.
    db.query_sql_with_params(
        "UPDATE events SET amount = $1 WHERE id = 1",
        &[Value::Int(i64::MIN)],
    )
    .unwrap();
    db.query_sql("UPDATE events SET amount = 9223372036854775807 WHERE id = 1")
        .unwrap();
    assert_eq!(
        db.query_sql("SELECT total FROM stats").unwrap().rows[0]["total"],
        Value::Int(i64::MAX)
    );
}

#[test]
fn materialized_ddl_and_write_guards() {
    let mut db = Database::new();
    base(&mut db);
    db.query_sql("INSERT INTO events (id, bucket, amount, enabled) VALUES (1, 'a', 5, true)")
        .unwrap();
    db.query_sql("CREATE MATERIALIZED VIEW mv AS SELECT id, amount FROM events")
        .unwrap();
    db.query_sql("CREATE MATERIALIZED VIEW IF NOT EXISTS mv AS SELECT id FROM events")
        .unwrap();
    for statement in [
        "INSERT INTO mv (id, amount) VALUES (2, 5)",
        "UPDATE mv SET amount = 2 WHERE id = 1",
        "DELETE FROM mv WHERE id = 1",
        "ALTER TABLE mv ADD COLUMN other BIGINT",
        "CREATE INDEX invalid ON mv (__hawdb_mv_key)",
        "CREATE MATERIALIZED VIEW nested AS SELECT id FROM mv",
        "DROP MATERIALIZED VIEW events",
        "CREATE MATERIALIZED VIEW events AS SELECT id FROM events",
    ] {
        assert!(db.query_sql(statement).is_err(), "{statement}");
    }
    let mut tx = db.begin_transaction().unwrap();
    tx.query_sql("DROP MATERIALIZED VIEW mv").unwrap();
    tx.query_sql("CREATE MATERIALIZED VIEW mv AS SELECT id, amount FROM events")
        .unwrap();
    tx.commit().unwrap();
    assert_eq!(db.query_sql("SELECT * FROM mv").unwrap().rows.len(), 1);
    db.query_sql("DROP MATERIALIZED VIEW mv").unwrap();
    db.query_sql("DROP MATERIALIZED VIEW IF EXISTS mv").unwrap();
    assert!(db.query_sql("SELECT * FROM mv").is_err());
}

#[test]
fn materialized_unsupported_definitions_fail_before_publication() {
    let mut db = Database::new();
    base(&mut db);
    for select in [
        "SELECT DISTINCT bucket FROM events",
        "SELECT id FROM events LIMIT 1",
        "SELECT id FROM events ORDER BY id",
        "SELECT random() AS r FROM events",
        "SELECT AVG(amount) AS avg FROM events",
        "SELECT COUNT(DISTINCT bucket) AS n FROM events",
        "SELECT a.id FROM events a JOIN events b ON a.id = b.id",
        "SELECT 1 AS n",
        "SELECT bucket, COUNT(*) AS n FROM events",
        "SELECT SUM(bucket) AS total FROM events",
    ] {
        assert!(
            db.query_sql(&format!("CREATE MATERIALIZED VIEW rejected AS {select}"))
                .is_err(),
            "{select}"
        );
    }
    assert!(db
        .query_sql_with_params(
            "CREATE MATERIALIZED VIEW rejected AS SELECT id FROM events WHERE id = $1",
            &[Value::Int(1)]
        )
        .is_err());
    assert!(db.query_sql("SELECT * FROM rejected").is_err());
    assert!(db
        .query_sql("CREATE MATERIALIZED VIEW rejected COPY GRANTS AS SELECT id FROM events")
        .is_err());
}

#[test]
fn materialized_wal_and_checkpoint_recovery_continue_maintenance() {
    let path = unique_test_dir("materialized_recovery");
    for checkpoint in [false, true] {
        {
            let mut db = Database::open(&path).unwrap();
            if !checkpoint {
                base(&mut db);
                db.query_sql("CREATE MATERIALIZED VIEW totals AS SELECT bucket, COUNT(*) AS n, COUNT(amount) AS present, SUM(amount) AS total FROM events WHERE enabled = true GROUP BY bucket").unwrap();
                db.query_sql(
                    "INSERT INTO events (id, bucket, amount, enabled) VALUES (1, 'a', 7, true)",
                )
                .unwrap();
            } else {
                db.checkpoint().unwrap();
                db.query_sql("UPDATE events SET amount = NULL WHERE id = 1")
                    .unwrap();
            }
        }
        let mut db = Database::open(&path).unwrap();
        assert_grouped_matches(&mut db);
        db.query_sql("UPDATE events SET amount = 11 WHERE id = 1")
            .unwrap();
        assert_grouped_matches(&mut db);
    }
    {
        let mut db = Database::open(&path).unwrap();
        db.query_sql("DROP MATERIALIZED VIEW totals").unwrap();
    }
    {
        let mut db = Database::open(&path).unwrap();
        assert!(db.query_sql("SELECT * FROM totals").is_err());
        assert_eq!(db.query_sql("SELECT * FROM events").unwrap().rows.len(), 1);
    }
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn materialized_redefined_filter_survives_checkpoint() {
    let path = unique_test_dir("materialized_redefined_filter");
    {
        let mut db = Database::open(&path).unwrap();
        base(&mut db);
        db.query_sql(
            "INSERT INTO events (id, amount, enabled) VALUES (1, 10, true), (2, 20, true)",
        )
        .unwrap();
        db.query_sql(
            "CREATE MATERIALIZED VIEW stats AS SELECT COUNT(*) AS n FROM events WHERE amount = 10",
        )
        .unwrap();
        db.checkpoint().unwrap();
        db.query_sql("DROP MATERIALIZED VIEW stats").unwrap();
        db.query_sql(
            "CREATE MATERIALIZED VIEW stats AS SELECT COUNT(*) AS n FROM events WHERE amount = 20",
        )
        .unwrap();
        db.checkpoint().unwrap();
    }
    {
        let mut db = Database::open(&path).unwrap();
        db.query_sql("UPDATE events SET amount = 30 WHERE id = 2")
            .unwrap();
        assert_eq!(
            db.query_sql("SELECT n FROM stats").unwrap().rows[0]["n"],
            Value::Int(0)
        );
    }
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn materialized_concurrent_commits_publish_complete_results() {
    let mut db = Database::new();
    base(&mut db);
    db.query_sql(
        "CREATE MATERIALIZED VIEW stats AS SELECT COUNT(*) AS n, SUM(amount) AS total FROM events",
    )
    .unwrap();
    let db = db.into_concurrent();
    std::thread::scope(|scope| {
        for worker in 0..4 {
            let db = &db;
            scope.spawn(move || {
                for offset in 0..10 {
                    let id = worker * 10 + offset;
                    db.query_sql_with_params(
                        "INSERT INTO events (id, amount, enabled) VALUES ($1, 1, true)",
                        &[Value::Int(id)],
                    )
                    .unwrap();
                    let output = db.query_sql("SELECT n, total FROM stats").unwrap();
                    assert_eq!(output.rows[0]["n"], output.rows[0]["total"]);
                }
            });
        }
    });
    assert_eq!(
        db.query_sql("SELECT n FROM stats").unwrap().rows[0]["n"],
        Value::Int(40)
    );
}
