// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

use super::*;
use hawdb_storage::config::RelationalIndexMode;

#[test]
fn composite_mutations_preserve_atomicity_old_views_and_reopen() {
    for residency in [
        StorageResidencyMode::Materialized,
        StorageResidencyMode::OutOfCore,
    ] {
        let path = unique_test_dir(&format!("composite_mutations_{residency:?}"));
        let mut config = DatabaseConfig {
            storage_residency_mode: residency,
            relational_index_mode: RelationalIndexMode::Shadow,
            ..DatabaseConfig::default()
        };
        {
            let mut db = Database::open_with_config(&path, config.clone()).unwrap();
            db.query_sql("CREATE TABLE order_line (w BIGINT NOT NULL, d BIGINT NOT NULL, o BIGINT NOT NULL, n BIGINT NOT NULL, delivered BIGINT NOT NULL, PRIMARY KEY (w,d,o,n))").unwrap();
            let values = (1..=2)
                .flat_map(|w| {
                    (1..=20).flat_map(move |o| (1..=3).map(move |n| format!("({w},1,{o},{n},0)")))
                })
                .collect::<Vec<_>>()
                .join(",");
            db.query_sql(&format!(
                "INSERT INTO order_line (w,d,o,n,delivered) VALUES {values}"
            ))
            .unwrap();
            db.checkpoint().unwrap();
        }
        config.relational_index_mode = RelationalIndexMode::Authoritative;
        {
            let mut db = Database::open_with_config(&path, config.clone()).unwrap();
            let epoch = db.commit_epoch().unwrap();
            let old = db.begin_read_transaction().unwrap();
            let before = old
                .query_sql("SELECT w,d,o,n,delivered FROM order_line ORDER BY w,d,o,n")
                .unwrap()
                .rows;
            for commit in [false, true] {
                let mut tx = db.begin_transaction().unwrap();
                tx.query_sql_with_params(
                    "UPDATE order_line SET delivered = $1 WHERE w = $2 AND d = $3 AND o = $4",
                    &[Value::Int(1), Value::Int(1), Value::Int(1), Value::Int(2)],
                )
                .unwrap();
                tx.query_sql("DELETE FROM order_line WHERE w = 1 AND d = 1 AND o >= 3 AND o < 4")
                    .unwrap();
                if commit {
                    tx.commit().unwrap();
                } else {
                    assert!(tx
                        .query_sql("INSERT INTO order_line (w,d,o,n,delivered) VALUES (1,1,1,1,0)")
                        .is_err());
                    tx.rollback();
                    assert_eq!(db.commit_epoch().unwrap(), epoch);
                    assert_eq!(
                        db.query_sql("SELECT w,d,o,n,delivered FROM order_line ORDER BY w,d,o,n")
                            .unwrap()
                            .rows,
                        before
                    );
                }
            }
            assert_eq!(db.commit_epoch().unwrap(), epoch + 1);
            assert_eq!(
                old.query_sql("SELECT w,d,o,n,delivered FROM order_line ORDER BY w,d,o,n")
                    .unwrap()
                    .rows,
                before
            );
            assert_committed(&mut db);
        }
        // The first reopen replays WAL over the checkpoint; the second reads
        // the newly checkpointed successor generation.
        for checkpoint in [true, false] {
            let mut db = Database::open_with_config(&path, config.clone()).unwrap();
            assert_committed(&mut db);
            if checkpoint {
                db.checkpoint().unwrap();
            }
        }
        std::fs::remove_dir_all(path).unwrap();
    }
}

fn assert_committed(db: &mut Database) {
    let rows = db
        .query_sql("SELECT w,o,n,delivered FROM order_line ORDER BY w,d,o,n")
        .unwrap()
        .rows;
    assert_eq!(rows.len(), 117);
    for row in rows {
        let target = row["w"] == Value::Int(1) && row["o"] == Value::Int(2);
        assert_eq!(row["delivered"], Value::Int(i64::from(target)));
        assert!(row["w"] != Value::Int(1) || row["o"] != Value::Int(3));
    }
}
