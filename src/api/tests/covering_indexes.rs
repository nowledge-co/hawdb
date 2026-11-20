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
use hawdb_optimizer::RelationalOperatorKind;
use hawdb_storage::config::{RelationalIndexMode, StorageResidencyMode};

fn create_fixture(database: &mut Database) {
    database
        .query_sql(
            "CREATE TABLE covering_events (\
               id BIGINT PRIMARY KEY, \
               tenant BIGINT NOT NULL, \
               payload TEXT, \
               extra TEXT NOT NULL)",
        )
        .unwrap();
    database
        .query_sql(
            "CREATE INDEX covering_events_tenant ON covering_events (tenant) INCLUDE (payload)",
        )
        .unwrap();
    for id in 0..100 {
        let tenant = if id == 99 { 2 } else { id % 2 };
        database
            .query_sql_with_params(
                "INSERT INTO covering_events (id, tenant, payload, extra) VALUES ($1, $2, $3, $4)",
                &[
                    Value::Int(id),
                    Value::Int(tenant),
                    Value::String(format!("payload-{id}")),
                    Value::String(format!("extra-{id}")),
                ],
            )
            .unwrap();
    }
}

fn assert_covering_read(database: &Database, expected_payload: &str, context: &str) {
    let result = database
        .begin_read_transaction()
        .unwrap_or_else(|error| panic!("{context}: begin read transaction: {error}"))
        .query_sql_with_params_options_profiled(
            "SELECT id, payload FROM covering_events WHERE tenant = $1",
            &[Value::Int(2)],
            QueryStreamOptions::default(),
        )
        .unwrap_or_else(|error| panic!("{context}: execute covering read: {error}"));
    assert_eq!(result.output.rows.len(), 1);
    assert_eq!(result.output.rows[0]["id"], Value::Int(99));
    assert_eq!(
        result.output.rows[0]["payload"],
        Value::String(expected_payload.to_string())
    );
    let access = &result.profile.operator_cardinality_profiles[0];
    assert_eq!(access.operator, RelationalOperatorKind::IndexRangeScan);
    assert!(access.access_path.covering);
    assert!(!access.access_path.requires_row_fetch);
}

#[test]
fn include_payload_serves_index_only_reads_and_tracks_live_updates() {
    let mut database = Database::new();
    create_fixture(&mut database);
    assert_covering_read(&database, "payload-99", "materialized baseline");

    database
        .query_sql("UPDATE covering_events SET payload = 'updated' WHERE id = 99")
        .unwrap();
    assert_covering_read(&database, "updated", "materialized live update");

    let ordered = database
        .begin_read_transaction()
        .unwrap()
        .query_sql_with_params_options_profiled(
            "SELECT id, tenant, payload FROM covering_events \
             WHERE tenant >= $1 ORDER BY tenant LIMIT 3",
            &[Value::Int(0)],
            QueryStreamOptions::default(),
        )
        .unwrap();
    assert_eq!(ordered.output.rows.len(), 3);
    assert!(ordered
        .output
        .rows
        .iter()
        .all(|row| row["tenant"] == Value::Int(0)));
    for row in &ordered.output.rows {
        let Value::Int(id) = row["id"] else {
            panic!("covered primary key must be an integer")
        };
        assert_eq!(row["payload"], Value::String(format!("payload-{id}")));
    }
    let ordered_access = &ordered.profile.operator_cardinality_profiles[0];
    assert_eq!(
        ordered_access.operator,
        RelationalOperatorKind::IndexRangeScan
    );
    assert!(ordered_access.access_path.covering);
    assert!(!ordered_access.access_path.requires_row_fetch);

    for (predicate, expected_ids) in [
        ("tenant < 0", Vec::new()),
        ("tenant <= 0", (0..99).filter(|id| id % 2 == 0).collect()),
        ("tenant > 1", vec![99]),
    ] {
        let range = database
            .begin_read_transaction()
            .unwrap()
            .query_sql_with_params_options_profiled(
                &format!(
                    "SELECT id, payload FROM covering_events WHERE {predicate} ORDER BY tenant"
                ),
                &[],
                QueryStreamOptions::default(),
            )
            .unwrap();
        let mut ids = range
            .output
            .rows
            .iter()
            .map(|row| match row["id"] {
                Value::Int(id) => id,
                ref value => panic!("covered range returned non-integer id {value:?}"),
            })
            .collect::<Vec<_>>();
        ids.sort_unstable();
        assert_eq!(ids, expected_ids, "{predicate}");
        let access = &range.profile.operator_cardinality_profiles[0].access_path;
        assert!(access.covering, "{predicate}: {access:?}");
        assert!(!access.requires_row_fetch, "{predicate}: {access:?}");
    }

    let fetched = database
        .begin_read_transaction()
        .unwrap()
        .query_sql_with_params_options_profiled(
            "SELECT id, payload, extra FROM covering_events WHERE tenant = $1",
            &[Value::Int(2)],
            QueryStreamOptions::default(),
        )
        .unwrap();
    let access = &fetched.profile.operator_cardinality_profiles[0];
    assert_eq!(access.operator, RelationalOperatorKind::IndexRangeScan);
    assert!(!access.access_path.covering);
    assert!(access.access_path.requires_row_fetch);
    assert_eq!(
        fetched.output.rows[0]["extra"],
        Value::String("extra-99".into())
    );

    let catalog = database
        .query_sql("SELECT indexdef FROM pg_indexes WHERE indexname = 'covering_events_tenant'")
        .unwrap();
    assert_eq!(
        catalog.rows[0]["indexdef"],
        Value::String(
            "CREATE INDEX \"covering_events_tenant\" ON public.\"covering_events\" USING btree (\"tenant\") INCLUDE (\"payload\")"
                .into()
        )
    );
}

#[test]
fn unique_include_uses_only_key_columns_and_payload_limits_fail_atomically() {
    let mut database = Database::new();
    database
        .query_sql(
            "CREATE TABLE unique_covering (\
               id BIGINT PRIMARY KEY, slug TEXT, payload TEXT NOT NULL)",
        )
        .unwrap();
    database
        .query_sql(
            "CREATE UNIQUE INDEX unique_covering_slug ON unique_covering (slug) INCLUDE (payload)",
        )
        .unwrap();
    database
        .query_sql("INSERT INTO unique_covering (id, slug, payload) VALUES (1, 'one', 'first')")
        .unwrap();
    let duplicate = database
        .query_sql("INSERT INTO unique_covering (id, slug, payload) VALUES (2, 'one', 'different')")
        .unwrap_err();
    assert!(
        duplicate.to_string().contains("duplicate key"),
        "{duplicate}"
    );
    database
        .query_sql(
            "INSERT INTO unique_covering (id, slug, payload) VALUES \
             (3, NULL, 'nullable-a'), (4, NULL, 'nullable-b')",
        )
        .unwrap();
    database
        .query_sql(
            "INSERT INTO unique_covering (id, slug, payload) VALUES (5, 'one', 'upserted') \
             ON CONFLICT (slug) DO UPDATE SET payload = EXCLUDED.payload",
        )
        .unwrap();
    let upserted = database
        .query_sql("SELECT id, payload FROM unique_covering WHERE slug = 'one'")
        .unwrap();
    assert_eq!(upserted.rows.len(), 1);
    assert_eq!(upserted.rows[0]["id"], Value::Int(1));
    assert_eq!(
        upserted.rows[0]["payload"],
        Value::String("upserted".into())
    );

    database
        .query_sql(
            "CREATE TABLE unique_covering_refs (\
               id BIGINT PRIMARY KEY, \
               slug TEXT NOT NULL REFERENCES unique_covering(slug))",
        )
        .unwrap();
    database
        .query_sql("INSERT INTO unique_covering_refs (id, slug) VALUES (1, 'one')")
        .unwrap();
    let missing_target = database
        .query_sql("INSERT INTO unique_covering_refs (id, slug) VALUES (2, 'missing')")
        .unwrap_err();
    assert!(
        missing_target.to_string().contains("no visible target"),
        "{missing_target}"
    );

    let oversized = "x".repeat(20 * 1024);
    let error = database
        .query_sql_with_params(
            "UPDATE unique_covering SET payload = $1 WHERE id = 1",
            &[Value::String(oversized)],
        )
        .unwrap_err();
    assert!(
        error.to_string().contains("covering index entry")
            && error.to_string().contains("exceeding limit"),
        "{error}"
    );
    let row = database
        .query_sql("SELECT payload FROM unique_covering WHERE id = 1")
        .unwrap();
    assert_eq!(row.rows[0]["payload"], Value::String("upserted".into()));
    assert_eq!(
        database
            .query_sql("SELECT id FROM unique_covering ORDER BY id")
            .unwrap()
            .rows
            .len(),
        3
    );
}

#[test]
fn create_include_rejects_existing_externalized_payload_atomically() {
    let mut database = Database::new();
    database
        .query_sql(
            "CREATE TABLE overflow_covering (\
               id BIGINT PRIMARY KEY, tenant BIGINT NOT NULL, payload TEXT NOT NULL)",
        )
        .unwrap();
    let externalized = "x".repeat(8 * 1024);
    database
        .query_sql_with_params(
            "INSERT INTO overflow_covering (id, tenant, payload) VALUES (1, 7, $1)",
            &[Value::String(externalized.clone())],
        )
        .unwrap();

    let error = database
        .query_sql(
            "CREATE INDEX overflow_covering_tenant ON overflow_covering (tenant) INCLUDE (payload)",
        )
        .unwrap_err();
    assert!(
        error
            .to_string()
            .contains("cannot reference an externalized value"),
        "{error}"
    );
    assert!(database
        .query_sql("SELECT indexname FROM pg_indexes WHERE indexname = 'overflow_covering_tenant'",)
        .unwrap()
        .rows
        .is_empty());
    let retained = database
        .query_sql("SELECT payload FROM overflow_covering WHERE id = 1")
        .unwrap();
    assert_eq!(retained.rows[0]["payload"], Value::String(externalized));

    database
        .query_sql("UPDATE overflow_covering SET payload = 'inline' WHERE id = 1")
        .unwrap();
    database
        .query_sql(
            "CREATE INDEX overflow_covering_tenant ON overflow_covering (tenant) INCLUDE (payload)",
        )
        .unwrap();
    let covered = database
        .begin_read_transaction()
        .unwrap()
        .query_sql_with_params_options_profiled(
            "SELECT id, payload FROM overflow_covering WHERE tenant = $1",
            &[Value::Int(7)],
            QueryStreamOptions::default(),
        )
        .unwrap();
    assert_eq!(
        covered.output.rows[0]["payload"],
        Value::String("inline".into())
    );
    assert!(
        covered.profile.operator_cardinality_profiles[0]
            .access_path
            .covering
    );
}

#[test]
fn covering_payload_survives_persistent_generation_live_overlay_and_reopen() {
    for (residency, bootstrap_indexes, serving_indexes) in [
        (
            StorageResidencyMode::Materialized,
            RelationalIndexMode::Materialized,
            RelationalIndexMode::Materialized,
        ),
        (
            StorageResidencyMode::OutOfCore,
            RelationalIndexMode::Shadow,
            RelationalIndexMode::Authoritative,
        ),
    ] {
        let path = unique_test_dir(&format!("covering_index_{residency:?}"));
        let mut config = DatabaseConfig {
            storage_residency_mode: residency,
            relational_index_mode: bootstrap_indexes,
            ..DatabaseConfig::default()
        };
        {
            let mut database = Database::open_with_config(&path, config.clone()).unwrap();
            create_fixture(&mut database);
            database.checkpoint().unwrap();
            if bootstrap_indexes == RelationalIndexMode::Shadow {
                let residency_report = database.storage_residency_report().unwrap();
                let index_report = database
                    .relational_index_shadow_checkpoint_report()
                    .unwrap()
                    .unwrap();
                assert!(
                    residency_report.relational_indexes.base_generation.is_some(),
                    "{residency:?} Shadow checkpoint omitted its relational index generation: {index_report:?}; {residency_report:?}"
                );
            }
        }

        config.relational_index_mode = serving_indexes;
        {
            let mut database = Database::open_with_config(&path, config.clone()).unwrap();
            assert_covering_read(
                &database,
                "payload-99",
                &format!("{residency:?} authoritative checkpoint"),
            );
            let mut transaction = database.begin_transaction().unwrap();
            transaction
                .query_sql("UPDATE covering_events SET payload = 'transaction' WHERE id = 99")
                .unwrap();
            let transaction_row = transaction
                .query_sql("SELECT id, payload FROM covering_events WHERE tenant = 2")
                .unwrap();
            assert_eq!(transaction_row.rows.len(), 1);
            assert_eq!(transaction_row.rows[0]["id"], Value::Int(99));
            assert_eq!(
                transaction_row.rows[0]["payload"],
                Value::String("transaction".into())
            );
            transaction.rollback();
            assert_covering_read(
                &database,
                "payload-99",
                &format!("{residency:?} explicit transaction rollback"),
            );
            database
                .query_sql("UPDATE covering_events SET payload = 'live' WHERE id = 99")
                .unwrap();
            assert_covering_read(
                &database,
                "live",
                &format!("{residency:?} authoritative live overlay"),
            );
        }
        {
            let mut database = Database::open_with_config(&path, config.clone()).unwrap();
            assert_covering_read(
                &database,
                "live",
                &format!("{residency:?} authoritative WAL replay"),
            );
            database.checkpoint().unwrap();
            assert_covering_read(
                &database,
                "live",
                &format!("{residency:?} authoritative successor checkpoint"),
            );
        }
        let database = Database::open_with_config(&path, config).unwrap();
        assert_covering_read(
            &database,
            "live",
            &format!("{residency:?} authoritative successor reopen"),
        );
        drop(database);
        std::fs::remove_dir_all(path).unwrap();
    }
}
