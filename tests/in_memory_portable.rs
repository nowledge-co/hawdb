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

//! The same public-facade contract runs natively and in a real browser.
use hawdb::{Database, DatabaseConfig, RuntimeTaskContext, Value};
use std::collections::BTreeMap;
use std::time::Duration;

#[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
use wasm_bindgen_test::{wasm_bindgen_test as test, wasm_bindgen_test_configure};
#[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
wasm_bindgen_test_configure!(run_in_dedicated_worker);

#[test]
fn writes_parameters_traversal_and_error_recovery_share_one_database() {
    let mut db = Database::new();
    db.query_with_params(
        "CREATE (:Memory {id: $id, title: $title})-[:MENTIONS]->(:Entity {name: 'Browser'})",
        &BTreeMap::from([
            ("id".into(), Value::Int(42)),
            ("title".into(), Value::String("WASM memory".into())),
        ]),
    )
    .unwrap();
    assert!(db.query("this is not Cypher").is_err());
    let output = db
        .query_with_params(
            "MATCH (m:Memory)-[:MENTIONS]->(e:Entity) WHERE m.id = $id RETURN m.title AS title, e.name AS name",
            &BTreeMap::from([("id".into(), Value::Int(42))]),
        )
        .unwrap();
    assert_eq!(output.rows.len(), 1);
    assert_eq!(
        output.rows[0].get("title"),
        Some(&Value::String("WASM memory".into()))
    );
    assert_eq!(
        output.rows[0].get("name"),
        Some(&Value::String("Browser".into()))
    );

    let mut transaction = db.begin_transaction().unwrap();
    transaction.query("CREATE (:Memory {id: 99})").unwrap();
    transaction.rollback();
    assert!(db
        .query("MATCH (m:Memory) WHERE m.id = 99 RETURN m.id AS id")
        .unwrap()
        .rows
        .is_empty());
    let mut transaction = db.begin_transaction().unwrap();
    transaction.query("CREATE (:Memory {id: 100})").unwrap();
    transaction.commit().unwrap();
    assert_eq!(
        db.query("MATCH (m:Memory) WHERE m.id = 100 RETURN m.id AS id")
            .unwrap()
            .rows
            .len(),
        1
    );
}

#[test]
fn browser_clock_and_randomness_generate_distinct_uuidv7_values() {
    let mut db = Database::new();
    db.query_sql("CREATE TABLE items (id BIGINT PRIMARY KEY)")
        .unwrap();
    db.query_sql("INSERT INTO items (id) VALUES (1)").unwrap();
    let first = db.query_sql("SELECT uuidv7() AS id FROM items").unwrap();
    let second = db.query_sql("SELECT uuidv7() AS id FROM items").unwrap();
    assert_ne!(first.rows[0].get("id"), second.rows[0].get("id"));
    let Some(Value::Uuid(id)) = first.rows[0].get("id") else {
        panic!("expected UUID value")
    };
    assert_eq!(id.get_version_num(), 7);
}

#[test]
fn expired_deadline_returns_an_error_without_poisoning_the_database() {
    let mut db = Database::new();
    db.query("CREATE (:Memory {id: 1})").unwrap();
    let context = RuntimeTaskContext::with_timeout(Duration::ZERO);
    assert!(db
        .query_with_context("MATCH (m:Memory) RETURN m.id AS id", &context)
        .is_err());
    assert_eq!(
        db.query("MATCH (m:Memory) RETURN m.id AS id")
            .unwrap()
            .rows
            .len(),
        1
    );
}

#[test]
fn result_budget_returns_an_error_instead_of_partial_rows() {
    let mut db = Database::new_with_config(DatabaseConfig {
        max_read_result_rows: Some(1),
        ..DatabaseConfig::default()
    });
    db.query("CREATE (:Memory {id: 1})").unwrap();
    db.query("CREATE (:Memory {id: 2})").unwrap();
    let error = db.query("MATCH (m:Memory) RETURN m.id AS id").unwrap_err();
    assert!(
        error.to_string().contains("max_read_result_rows"),
        "{error}"
    );
    assert_eq!(
        db.query("MATCH (m:Memory) RETURN m.id AS id LIMIT 1")
            .unwrap()
            .rows
            .len(),
        1
    );
}

#[test]
fn segment_read_waves_keep_order_and_budget_on_the_serial_backend() {
    use hawdb::{
        SegmentBytes, SegmentRangeReader, SegmentReadError, SegmentReadExecutor, SegmentReadRange,
        SegmentReadScheduler,
    };
    use std::num::{NonZeroU64, NonZeroUsize};

    struct MemoryReader;
    impl SegmentRangeReader for MemoryReader {
        fn read_range(&self, range: &SegmentReadRange) -> Result<SegmentBytes, SegmentReadError> {
            Ok(vec![range.offset as u8; range.length.get() as usize].into())
        }
    }

    let schedule = SegmentReadScheduler::new(NonZeroUsize::new(2).unwrap(), NonZeroU64::MIN)
        .schedule([
            SegmentReadRange::new(1, 1, 0, NonZeroU64::MIN),
            SegmentReadRange::new(1, 2, 1, NonZeroU64::MIN),
        ]);
    let mut bytes = Vec::new();
    let report = SegmentReadExecutor::new(NonZeroU64::new(2).unwrap())
        .execute(&MemoryReader, &schedule, |payload| {
            bytes.extend_from_slice(&payload.bytes);
            Ok::<_, std::convert::Infallible>(())
        })
        .unwrap();
    assert_eq!(bytes, [0, 1]);
    assert_eq!(report.range_count, 2);
    let error = SegmentReadExecutor::new(NonZeroU64::MIN)
        .execute(
            &MemoryReader,
            &schedule,
            |_| -> Result<(), std::convert::Infallible> {
                panic!("an over-budget wave must not deliver a partial result");
            },
        )
        .unwrap_err();
    assert!(error.to_string().contains("budget"), "{error}");
}

#[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
#[test]
fn persistent_open_is_rejected() {
    let error = match Database::open("browser-database") {
        Ok(_) => panic!("persistent open must fail"),
        Err(error) => error,
    };
    assert!(error
        .to_string()
        .contains("persistent storage is unavailable"));
}

#[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
#[test]
fn sort_that_needs_disk_spill_fails_explicitly() {
    let mut config = DatabaseConfig::default();
    config.execution_memory.blocking_operator_bytes =
        std::num::NonZeroUsize::new(64 * 1024).unwrap();
    let mut db = Database::new_with_config(config);
    for id in 0..128 {
        db.query_with_params(
            "CREATE (:Memory {id: $id, title: $title})",
            &BTreeMap::from([
                ("id".into(), Value::Int(id)),
                (
                    "title".into(),
                    Value::String(format!("{id:04}{}", "x".repeat(2048))),
                ),
            ]),
        )
        .unwrap();
    }
    let error = db
        .query("MATCH (m:Memory) RETURN m.title AS title ORDER BY title DESC")
        .unwrap_err();
    assert!(
        error.to_string().contains("disk spill is unavailable"),
        "{error}"
    );
    assert_eq!(
        db.query("MATCH (m:Memory) WHERE m.id = 0 RETURN m.id AS id")
            .unwrap()
            .rows
            .len(),
        1
    );
}

#[test]
fn fromless_select_evaluates_once_and_rejects_column_references() {
    let mut db = Database::new();
    let output = db
        .query_sql("SELECT 1 AS one, version() AS server_version")
        .unwrap();
    assert_eq!(output.rows.len(), 1);
    assert_eq!(output.rows[0].get("one"), Some(&Value::Int(1)));
    assert!(matches!(
        output.rows[0].get("server_version"),
        Some(Value::String(version)) if version.starts_with("HawDB ")
    ));
    assert!(db.query_sql("SELECT does_not_exist").is_err());
}

#[test]
fn database_instance_identity_is_stable_and_distinguishes_equal_state() {
    let mut database = Database::new();
    let identity = database.instance_identity().unwrap();
    let another = Database::new();
    assert_eq!(
        database.commit_epoch().unwrap(),
        another.commit_epoch().unwrap()
    );
    assert_ne!(identity, another.instance_identity().unwrap());
    assert_eq!(identity.clone(), identity);
    database.query("CREATE (:Memory {id: 'instance'})").unwrap();
    assert_eq!(database.instance_identity().unwrap(), identity);
    let moved = database;
    assert_eq!(moved.instance_identity().unwrap(), identity);
    drop(moved);
    assert_ne!(Database::new().instance_identity().unwrap(), identity);
}

#[test]
fn consumed_retained_node_snapshot_keeps_old_data_and_task_bound_after_database_drop() {
    use hawdb::{
        RetainedNodeField, RetainedNodeSnapshotCursor, RetainedQueryOptions, RetainedQueryStatus,
    };

    let mut db = Database::new();
    db.query("CREATE NODE TABLE Item").unwrap();
    db.query("CREATE PROPERTY ON NODE TABLE Item(key) TYPE INT")
        .unwrap();
    db.query("CREATE INDEX ON :Item(key)").unwrap();
    db.query_with_params(
        "UNWIND $rows AS row CREATE (:Item {key: row.key, text: 'original'})",
        &BTreeMap::from([(
            "rows".into(),
            Value::List(
                (0..17)
                    .map(|key| Value::Map(BTreeMap::from([("key".into(), Value::Int(key))])))
                    .collect(),
            ),
        )]),
    )
    .unwrap();
    let context = RuntimeTaskContext::default();
    let cancellation = context.cancellation().clone();
    let snapshot = db.begin_read_transaction_with_context(&context).unwrap();
    drop(context);
    let parameters = BTreeMap::from([("key".into(), Value::Int(7))]);
    db.query_with_params(
        "MATCH (n:Item {key: $key}) SET n.text = 'current'",
        &parameters,
    )
    .unwrap();
    let mut cursor: RetainedNodeSnapshotCursor = snapshot
        .into_retained_node_query(
            "MATCH (n:Item {key: $key}) RETURN n AS node",
            &parameters,
            RetainedQueryOptions::default(),
        )
        .unwrap();
    drop(db);
    let view = cursor.next_node().unwrap().unwrap();
    assert!(matches!(view.field("text").unwrap(),
        Some(RetainedNodeField::Value(Value::String(text))) if text == "original"));
    cancellation.cancel();
    assert!(cursor.next_node().is_err());
    assert_eq!(cursor.status(), RetainedQueryStatus::Failed);
    drop(cursor);
    assert_eq!(view.status(), RetainedQueryStatus::Failed);
    assert!(matches!(view.field("text").unwrap(),
        Some(RetainedNodeField::Value(Value::String(text))) if text == "original"));
}
