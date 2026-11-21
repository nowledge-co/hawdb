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

use crate::{Database, DatabaseConfig, Value};
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
