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
use hawdb_storage::config::{RelationalIndexMode, StorageResidencyMode};

fn high_water(database: &Database) -> i64 {
    database
        .runtime
        .get()
        .unwrap()
        .store
        .relational_state()
        .last_row_id("bag")
}

#[test]
fn hidden_row_id_metadata_survives_empty_checkpoints_and_read_only_recovery() {
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
        let path = unique_test_dir("hidden_row_id_metadata");
        let config = DatabaseConfig {
            storage_residency_mode: residency,
            relational_index_mode: index,
            ..Default::default()
        };
        let mut db = Database::open_with_config(&path, config.clone()).unwrap();
        db.query_sql("CREATE TABLE bag (value BIGINT)").unwrap();
        db.checkpoint().unwrap();
        let mut tx = db.begin_transaction().unwrap();
        tx.query_sql("INSERT INTO bag VALUES (1), (1)").unwrap();
        tx.query_sql("DELETE FROM bag WHERE value = 1").unwrap();
        tx.commit().unwrap();
        assert_eq!(high_water(&db), 2);
        assert!(db.query_sql("SELECT * FROM bag").unwrap().rows.is_empty());
        db.checkpoint().unwrap();
        drop(db);
        let mut db = Database::open_with_config(&path, config.clone()).unwrap();
        assert_eq!(high_water(&db), 2);
        db.query_sql("INSERT INTO bag VALUES (2), (2)").unwrap();
        assert_eq!(high_water(&db), 4);
        drop(db);
        let mut read_only = Database::open_with_config(
            &path,
            DatabaseConfig {
                read_only: true,
                recovery_mode: RecoveryMode::Strict,
                ..config.clone()
            },
        )
        .unwrap();
        assert_eq!(high_water(&read_only), 4);
        assert_eq!(
            read_only.query_sql("SELECT * FROM bag").unwrap().rows.len(),
            2
        );
        drop(read_only);
        let mut db = Database::open_with_config(&path, config).unwrap();
        assert_eq!(high_water(&db), 4);
        db.query_sql("INSERT INTO bag VALUES (3)").unwrap();
        assert_eq!(high_water(&db), 5);
        db.query_sql("ALTER TABLE bag ADD COLUMN label TEXT DEFAULT 'kept'")
            .unwrap();
        db.checkpoint().unwrap();
        assert_eq!(high_water(&db), 5);
        drop(db);
        std::fs::remove_dir_all(path).unwrap();
    }
}
