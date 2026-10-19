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
use hawdb_storage::config::RelationalIndexMode;

#[derive(Clone, Copy, Debug)]
enum IndexKind {
    UniqueIndex,
    UniqueConstraint,
    NonUnique,
}

fn seed(database: &mut Database, kind: IndexKind, nullable: bool) {
    let suffix = if nullable { "" } else { " NOT NULL" };
    let constraint = if matches!(kind, IndexKind::UniqueConstraint) {
        ", UNIQUE (group_value, sort_value)"
    } else {
        ""
    };
    database
        .query_sql(&format!(
            "CREATE TABLE nullable_groups (id BIGINT PRIMARY KEY, group_value TEXT, \
             sort_value BIGINT{suffix}{constraint})"
        ))
        .unwrap();
    if !matches!(kind, IndexKind::UniqueConstraint) {
        let unique = if matches!(kind, IndexKind::UniqueIndex) {
            "UNIQUE "
        } else {
            ""
        };
        database
            .query_sql(&format!(
                "CREATE {unique}INDEX group_index ON nullable_groups (group_value, sort_value)"
            ))
            .unwrap();
    }
    let (first, third) = if nullable {
        ("NULL", "NULL")
    } else {
        ("0", "2")
    };
    database
        .query_sql(&format!(
            "INSERT INTO nullable_groups (id, group_value, sort_value) VALUES \
             (1, 'g', {first}), (2, 'g', 1), (3, 'g', {third}), \
             (4, 'h', 3), (5, 'i', 4), (6, 'j', 5), (7, NULL, 6)"
        ))
        .unwrap();
    database
        .query_sql("CREATE TABLE group_owners (group_value TEXT PRIMARY KEY, sort_value BIGINT)")
        .unwrap();
    database
        .query_sql("INSERT INTO group_owners (group_value, sort_value) VALUES ('g', 1)")
        .unwrap();
}

fn assert_queries(database: &Database, kind: IndexKind, nullable: bool) {
    let read = database.begin_read_transaction().unwrap();
    for complete in [false, true] {
        let suffix = if complete { " AND sort_value = 1" } else { "" };
        let sql =
            format!("SELECT id FROM nullable_groups WHERE group_value = $1{suffix} ORDER BY id");
        let output = read
            .query_sql_with_params_options_profiled(
                &sql,
                &[Value::String("g".into())],
                QueryStreamOptions::default(),
            )
            .unwrap();
        let expected = if complete { vec![2] } else { vec![1, 2, 3] };
        assert_eq!(
            output
                .output
                .rows
                .iter()
                .map(|row| row["id"].clone())
                .collect::<Vec<_>>(),
            expected.iter().copied().map(Value::Int).collect::<Vec<_>>(),
            "kind={kind:?}, nullable={nullable}, complete={complete}"
        );
        let prefix_is_complete = complete || !nullable || matches!(kind, IndexKind::NonUnique);
        assert_eq!(
            output.profile.operator_cardinality_profiles[0].operator,
            if prefix_is_complete {
                RelationalOperatorKind::IndexRangeScan
            } else {
                RelationalOperatorKind::TableFullScan
            },
            "kind={kind:?}, nullable={nullable}, complete={complete}"
        );
        let join_suffix = if complete {
            " AND r.sort_value = o.sort_value"
        } else {
            ""
        };
        let joined = read
            .query_sql(&format!(
                "SELECT r.id FROM group_owners o INNER JOIN nullable_groups r \
                 ON r.group_value = o.group_value{join_suffix} \
                 WHERE o.group_value = 'g' ORDER BY r.id"
            ))
            .unwrap();
        assert_eq!(
            joined
                .rows
                .iter()
                .map(|row| row["id"].clone())
                .collect::<Vec<_>>(),
            expected.into_iter().map(Value::Int).collect::<Vec<_>>(),
            "join kind={kind:?}, nullable={nullable}, complete={complete}"
        );
    }
}

#[test]
fn nullable_unique_prefixes_preserve_base_and_join_results_in_memory() {
    for kind in [
        IndexKind::UniqueIndex,
        IndexKind::UniqueConstraint,
        IndexKind::NonUnique,
    ] {
        for nullable in [false, true] {
            let mut database = Database::new();
            seed(&mut database, kind, nullable);
            assert_queries(&database, kind, nullable);
        }
    }
}

#[test]
fn nullable_unique_prefixes_preserve_base_and_join_results_after_checkpoint() {
    for (residency, indexes) in [
        (
            crate::StorageResidencyMode::Materialized,
            RelationalIndexMode::Materialized,
        ),
        (
            crate::StorageResidencyMode::OutOfCore,
            RelationalIndexMode::Materialized,
        ),
        (
            crate::StorageResidencyMode::OutOfCore,
            RelationalIndexMode::Authoritative,
        ),
    ] {
        for kind in [
            IndexKind::UniqueIndex,
            IndexKind::UniqueConstraint,
            IndexKind::NonUnique,
        ] {
            let path = unique_test_dir("nullable_unique_prefix");
            let config = DatabaseConfig {
                storage_residency_mode: residency,
                relational_index_mode: indexes,
                ..DatabaseConfig::default()
            };
            let mut seed_config = config.clone();
            if indexes == RelationalIndexMode::Authoritative {
                seed_config.relational_index_mode = RelationalIndexMode::Shadow;
            }
            let mut database = Database::open_with_config(&path, seed_config).unwrap();
            seed(&mut database, kind, true);
            database.checkpoint().unwrap();
            drop(database);
            let database = Database::open_with_config(&path, config).unwrap();
            assert_eq!(
                database
                    .runtime
                    .get()
                    .unwrap()
                    .store
                    .relational_state()
                    .canonical_row_metadata_only(),
                indexes == RelationalIndexMode::Authoritative,
            );
            assert_queries(&database, kind, true);
            drop(database);
            std::fs::remove_dir_all(path).unwrap();
        }
    }
}
