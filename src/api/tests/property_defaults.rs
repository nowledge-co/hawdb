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

//! Property DEFAULT values (#1683): applied only when a node or relationship
//! is created without the property, persisted in the catalog, backfilled into
//! records that already lack the property, and reported by introspection.

use super::*;
use crate::schema::{DeclaredPropertyDefault, PropertyDefault, PropertySchema};
use crate::RecoveryMode;

/// `[{id, p?}]` parameter rows; `None` omits the `p` key.
fn rows(rows: &[(&str, Option<Value>)]) -> Value {
    Value::List(
        rows.iter()
            .map(|(id, p)| {
                let mut row = BTreeMap::from([("id".to_string(), Value::String(id.to_string()))]);
                if let Some(p) = p {
                    row.insert("p".to_string(), p.clone());
                }
                Value::Map(row)
            })
            .collect(),
    )
}

fn node_value(db: &mut Database, label: &str, id: &str, property: &str) -> Value {
    let output = db
        .query_with_params(
            &format!("MATCH (n:{label} {{id: $id}}) RETURN n.{property} AS value"),
            &BTreeMap::from([("id".to_string(), Value::String(id.to_string()))]),
        )
        .unwrap();
    assert_eq!(output.rows.len(), 1, "{label} {id}");
    output.rows[0]["value"].clone()
}

/// The stored node map, which distinguishes an absent property from an
/// explicitly written NULL.
fn node_map(db: &mut Database, label: &str, id: &str) -> BTreeMap<String, Value> {
    let output = db
        .query_with_params(
            &format!("MATCH (n:{label} {{id: $id}}) RETURN n AS node"),
            &BTreeMap::from([("id".to_string(), Value::String(id.to_string()))]),
        )
        .unwrap();
    match &output.rows[0]["node"] {
        Value::Map(map) => map.clone(),
        other => panic!("node map expected, got {other:?}"),
    }
}

fn rel_value(db: &mut Database, key: i64, property: &str) -> Value {
    let output = db
        .query(&format!(
            "MATCH (:E)-[r:R]->(:E) WHERE r.k = {key} RETURN r.{property} AS value"
        ))
        .unwrap();
    assert_eq!(output.rows.len(), 1, "relationship k={key}");
    output.rows[0]["value"].clone()
}

fn schema<'a>(schemas: &'a [PropertySchema], table: &str, property: &str) -> &'a PropertySchema {
    schemas
        .iter()
        .find(|schema| schema.table == table && schema.property == property)
        .unwrap_or_else(|| panic!("missing schema {table}.{property}"))
}

fn declare_semantics_schema(db: &mut Database) {
    db.query("CREATE NODE TABLE E").unwrap();
    db.query("CREATE PROPERTY ON NODE TABLE E(p) TYPE DOUBLE DEFAULT 0.0")
        .unwrap();
    db.query("CREATE RELATIONSHIP TABLE R").unwrap();
    db.query("CREATE PROPERTY ON RELATIONSHIP TABLE R(w) TYPE INT DEFAULT 7")
        .unwrap();
}

fn write_semantics_table(db: &mut Database) {
    db.query("CREATE (:E {id: 'absent'})").unwrap();
    db.query("CREATE (:E {id: 'literal_null', p: NULL})")
        .unwrap();
    db.query_with_params(
        "CREATE (:E {id: 'param_null', p: $v})",
        &BTreeMap::from([("v".to_string(), Value::Null)]),
    )
    .unwrap();
    db.query("MERGE (e:E {id: 'merge_null'}) ON CREATE SET e.p = NULL")
        .unwrap();
    db.query("MERGE (e:E {id: 'merge_absent'})").unwrap();
    db.query("MERGE (e:E {id: 'merge_value'}) ON CREATE SET e.p = 4.5")
        .unwrap();
    db.query("CREATE (:E {id: 'set_null'})").unwrap();
    db.query("MATCH (e:E {id: 'set_null'}) SET e.p = NULL")
        .unwrap();
    db.query("CREATE (:E {id: 'explicit', p: 2.5})").unwrap();
    // Bulk rows: a row lacking the key gets the default, an explicit NULL
    // in the row stays NULL.
    db.query_with_params(
        "UNWIND $rows AS row CREATE (:E {id: row.id, p: row.p})",
        &BTreeMap::from([(
            "rows".to_string(),
            rows(&[("unwind", None), ("unwind_null", Some(Value::Null))]),
        )]),
    )
    .unwrap();

    db.query("MATCH (a:E {id: 'absent'}), (b:E {id: 'explicit'}) CREATE (a)-[:R {k: 1}]->(b)")
        .unwrap();
    db.query(
        "MATCH (a:E {id: 'absent'}), (b:E {id: 'explicit'}) CREATE (a)-[:R {k: 2, w: NULL}]->(b)",
    )
    .unwrap();
    db.query(
        "MATCH (a:E {id: 'absent'}), (b:E {id: 'explicit'}) MERGE (a)-[r:R {k: 3}]->(b) ON CREATE SET r.w = NULL",
    )
    .unwrap();
    db.query("MATCH (a:E {id: 'absent'}), (b:E {id: 'explicit'}) MERGE (a)-[r:R {k: 4}]->(b)")
        .unwrap();
    db.query("CREATE (:E {id: 'src'})-[:R {k: 5}]->(:E {id: 'dst'})")
        .unwrap();
    db.query("MATCH (a:E {id: 'absent'}), (b:E {id: 'explicit'}) CREATE (a)-[:R {k: 6}]->(b)")
        .unwrap();
    db.query("MATCH (:E)-[r:R {k: 6}]->(:E) SET r.w = NULL")
        .unwrap();
}

fn assert_semantics_table(db: &mut Database) {
    for (id, expected) in [
        ("absent", Value::Float(0.0)),
        ("literal_null", Value::Null),
        ("param_null", Value::Null),
        ("merge_null", Value::Null),
        ("merge_absent", Value::Float(0.0)),
        ("merge_value", Value::Float(4.5)),
        ("set_null", Value::Null),
        ("explicit", Value::Float(2.5)),
        ("unwind", Value::Float(0.0)),
        ("unwind_null", Value::Null),
        ("src", Value::Float(0.0)),
        ("dst", Value::Float(0.0)),
    ] {
        assert_eq!(node_value(db, "E", id, "p"), expected, "node {id}");
    }
    // Explicit NULLs are stored values, never re-filled from the default.
    assert_eq!(
        node_map(db, "E", "literal_null").get("p"),
        Some(&Value::Null)
    );
    for (key, expected) in [
        (1, Value::Int(7)),
        (2, Value::Null),
        (3, Value::Null),
        (4, Value::Int(7)),
        (5, Value::Int(7)),
        (6, Value::Null),
    ] {
        assert_eq!(rel_value(db, key, "w"), expected, "relationship k={key}");
    }
}

#[test]
fn defaults_apply_only_to_properties_absent_at_creation() {
    let path = unique_test_dir("property_defaults_semantics");
    {
        let mut db = Database::open(&path).unwrap();
        declare_semantics_schema(&mut db);
        write_semantics_table(&mut db);
        assert_semantics_table(&mut db);
    }
    // WAL replay keeps both the filled defaults and the explicit NULLs.
    {
        let mut db = Database::open(&path).unwrap();
        assert_semantics_table(&mut db);
        db.checkpoint().unwrap();
    }
    // So does a checkpoint image.
    {
        let mut db = Database::open(&path).unwrap();
        assert_semantics_table(&mut db);
        db.query("CREATE (:E {id: 'after_checkpoint'})").unwrap();
        assert_eq!(
            node_value(&mut db, "E", "after_checkpoint", "p"),
            Value::Float(0.0)
        );
    }
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn transaction_writes_apply_defaults_before_commit() {
    let mut db = Database::new();
    declare_semantics_schema(&mut db);
    let mut transaction = db.begin_transaction().unwrap();
    transaction.query("CREATE (:E {id: 'tx'})").unwrap();
    transaction
        .query("CREATE (:E {id: 'tx_null', p: NULL})")
        .unwrap();
    transaction
        .query("MERGE (e:E {id: 'tx_merge'}) ON CREATE SET e.p = NULL")
        .unwrap();
    let staged = transaction
        .query("MATCH (e:E {id: 'tx'}) RETURN e.p AS value")
        .unwrap();
    assert_eq!(staged.rows[0]["value"], Value::Float(0.0));
    transaction.commit().unwrap();
    assert_eq!(node_value(&mut db, "E", "tx", "p"), Value::Float(0.0));
    assert_eq!(node_value(&mut db, "E", "tx_null", "p"), Value::Null);
    assert_eq!(node_value(&mut db, "E", "tx_merge", "p"), Value::Null);
}

#[test]
fn graph_snapshot_import_fills_rows_lacking_the_key_only() {
    let mut db = Database::new();
    declare_semantics_schema(&mut db);
    {
        let mut runtime = db.runtime.get_mut().unwrap();
        let runtime = &mut *runtime;
        runtime
            .store
            .import_graph_snapshot_rows(
                &mut runtime.catalog,
                vec![
                    (
                        NodeId(100),
                        "E".to_string(),
                        BTreeMap::from([("id".to_string(), Value::String("lacking".into()))]),
                    ),
                    (
                        NodeId(101),
                        "E".to_string(),
                        BTreeMap::from([
                            ("id".to_string(), Value::String("null".into())),
                            ("p".to_string(), Value::Null),
                        ]),
                    ),
                ],
                vec![
                    (
                        crate::store::RelId(100),
                        NodeId(100),
                        NodeId(101),
                        "R".to_string(),
                        BTreeMap::from([("k".to_string(), Value::Int(1))]),
                    ),
                    (
                        crate::store::RelId(101),
                        NodeId(100),
                        NodeId(101),
                        "R".to_string(),
                        BTreeMap::from([
                            ("k".to_string(), Value::Int(2)),
                            ("w".to_string(), Value::Null),
                        ]),
                    ),
                ],
            )
            .unwrap();
    }
    assert_eq!(node_value(&mut db, "E", "lacking", "p"), Value::Float(0.0));
    assert_eq!(node_value(&mut db, "E", "null", "p"), Value::Null);
    assert_eq!(rel_value(&mut db, 1, "w"), Value::Int(7));
    assert_eq!(rel_value(&mut db, 2, "w"), Value::Null);
}

#[test]
fn literal_defaults_round_trip_exactly_and_type_any_stays_open() {
    let path = unique_test_dir("property_defaults_literals");
    let expected = [
        ("b", Value::Bool(true), "TYPE BOOL DEFAULT true"),
        ("i", Value::Int(-3), "TYPE INT DEFAULT -3"),
        ("f", Value::Float(0.25), "TYPE FLOAT DEFAULT 0.25"),
        (
            "s",
            Value::String("default".into()),
            "TYPE STRING DEFAULT 'default'",
        ),
        (
            "braces",
            Value::String("{}".into()),
            "TYPE STRING DEFAULT '{}'",
        ),
        ("empty", Value::List(Vec::new()), "TYPE LIST DEFAULT []"),
        (
            "nested",
            Value::List(vec![Value::Int(1), Value::String("two".into())]),
            "TYPE ANY DEFAULT [1, 'two']",
        ),
        (
            "any",
            Value::String("open".into()),
            "TYPE ANY DEFAULT 'open'",
        ),
    ];
    {
        let mut db = Database::open(&path).unwrap();
        db.query("CREATE NODE TABLE L").unwrap();
        for (property, _, clause) in &expected {
            db.query(&format!(
                "CREATE PROPERTY ON NODE TABLE L({property}) {clause}"
            ))
            .unwrap();
        }
        db.query("CREATE (:L {id: 'wal'})").unwrap();
        // TYPE ANY declares a default without constraining later writes.
        db.query("CREATE (:L {id: 'any_int', any: 1})").unwrap();
        db.query("CREATE (:L {id: 'any_list', any: [true, 2.5]})")
            .unwrap();
    }
    for round in 0..2 {
        let mut db = Database::open(&path).unwrap();
        for (property, value, _) in &expected {
            assert_eq!(
                &node_value(&mut db, "L", "wal", property),
                value,
                "{property}"
            );
            let schemas = db.property_schemas().unwrap();
            assert_eq!(
                schema(&schemas, "L", property).default,
                Some(DeclaredPropertyDefault {
                    value: PropertyDefault::Value(value.clone()),
                    backfill_value: value.clone(),
                }),
                "{property} round {round}"
            );
        }
        assert_eq!(node_value(&mut db, "L", "any_int", "any"), Value::Int(1));
        assert_eq!(
            node_value(&mut db, "L", "any_list", "any"),
            Value::List(vec![Value::Bool(true), Value::Float(2.5)])
        );
        db.query(&format!("CREATE (:L {{id: 'round{round}'}})"))
            .unwrap();
        assert_eq!(
            node_value(&mut db, "L", &format!("round{round}"), "empty"),
            Value::List(Vec::new())
        );
        db.checkpoint().unwrap();
    }
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn invalid_defaults_are_rejected_at_ddl_time() {
    let mut db = Database::new();
    db.query("CREATE NODE TABLE V").unwrap();
    for (statement, message) in [
        (
            "CREATE PROPERTY ON NODE TABLE V(p) TYPE INT DEFAULT 'text'",
            "property schema violation",
        ),
        (
            "CREATE PROPERTY ON NODE TABLE V(p) TYPE INT NOT NULL DEFAULT NULL",
            "property DEFAULT must be a non-NULL literal",
        ),
        (
            "CREATE PROPERTY ON NODE TABLE V(p) TYPE INT DEFAULT $value",
            "property DEFAULT must be a non-NULL literal",
        ),
        (
            "CREATE PROPERTY ON NODE TABLE V(p) TYPE ANY DEFAULT date('2026-01-01')",
            "property DEFAULT must be a non-NULL literal",
        ),
        (
            "CREATE PROPERTY ON NODE TABLE V(p) TYPE STRING DEFAULT current_timestamp()",
            "property schema violation",
        ),
        (
            "CREATE PROPERTY ON NODE TABLE V(p) TYPE LIST DEFAULT [1, $value]",
            "property DEFAULT must be a non-NULL literal",
        ),
    ] {
        let error = db.query(statement).unwrap_err().to_string();
        assert!(error.contains(message), "{statement}: {error}");
    }
    assert!(db.property_schemas().unwrap().is_empty());
}

/// `current_timestamp()` values are `Int` epoch nanoseconds, like HawDB's
/// own `current_timestamp()`.
fn timestamp(value: &Value) -> i64 {
    match value {
        Value::Int(value) => *value,
        other => panic!("epoch-nanosecond timestamp expected, got {other:?}"),
    }
}

/// Wall-clock epoch nanoseconds truncated to whole milliseconds.
fn now_millis() -> i64 {
    let millis = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64;
    millis * 1_000_000
}

#[test]
fn current_timestamp_default_is_one_millisecond_instant_per_transaction() {
    let mut db = Database::new();
    db.query("CREATE NODE TABLE G").unwrap();
    db.query("CREATE PROPERTY ON NODE TABLE G(created_at) TYPE ANY DEFAULT CURRENT_TIMESTAMP()")
        .unwrap();
    let start = now_millis();
    db.query_with_params(
        "UNWIND $rows AS row CREATE (:G {id: row.id})",
        &BTreeMap::from([("rows".to_string(), rows(&vec![("bulk", None); 64]))]),
    )
    .unwrap();
    let rows = db
        .query("MATCH (g:G {id: 'bulk'}) RETURN g.created_at AS created_at")
        .unwrap()
        .rows;
    assert_eq!(rows.len(), 64);
    let bulk = timestamp(&rows[0]["created_at"]);
    assert!(rows.iter().all(|row| timestamp(&row["created_at"]) == bulk));
    assert_eq!(bulk % 1_000_000, 0, "millisecond precision");

    // Two statements of one explicit transaction share the instant.
    let mut transaction = db.begin_transaction().unwrap();
    transaction.query("CREATE (:G {id: 'tx1'})").unwrap();
    std::thread::sleep(std::time::Duration::from_millis(20));
    transaction.query("CREATE (:G {id: 'tx2'})").unwrap();
    transaction.commit().unwrap();
    let tx1 = timestamp(&node_value(&mut db, "G", "tx1", "created_at"));
    assert_eq!(
        tx1,
        timestamp(&node_value(&mut db, "G", "tx2", "created_at"))
    );

    // Separate auto-commit statements are separate transactions.
    db.query("CREATE (:G {id: 'auto1'})").unwrap();
    std::thread::sleep(std::time::Duration::from_millis(20));
    db.query("CREATE (:G {id: 'auto2'})").unwrap();
    let auto1 = timestamp(&node_value(&mut db, "G", "auto1", "created_at"));
    let auto2 = timestamp(&node_value(&mut db, "G", "auto2", "created_at"));
    assert!(auto2 > auto1, "{auto1} < {auto2}");
    let end = now_millis();
    for instant in [bulk, tx1, auto1, auto2] {
        assert!(
            start <= instant && instant <= end,
            "{start} <= {instant} <= {end}"
        );
    }
    // An explicit value or NULL still wins.
    db.query("CREATE (:G {id: 'explicit_null', created_at: NULL})")
        .unwrap();
    assert_eq!(
        node_value(&mut db, "G", "explicit_null", "created_at"),
        Value::Null
    );
}

/// A defaulted `current_timestamp()` value has the same type as an explicit
/// `current_timestamp()` (and as imported TIMESTAMP values), so comparisons
/// and ORDER BY mix them correctly.
#[test]
fn current_timestamp_default_matches_explicit_current_timestamp() {
    let mut db = Database::new();
    db.query("CREATE NODE TABLE T").unwrap();
    db.query("CREATE PROPERTY ON NODE TABLE T(at) TYPE INT DEFAULT current_timestamp()")
        .unwrap();
    let mut transaction = db.begin_transaction().unwrap();
    transaction.query("CREATE (:T {id: 'defaulted'})").unwrap();
    std::thread::sleep(std::time::Duration::from_millis(5));
    transaction
        .query("CREATE (:T {id: 'explicit', at: current_timestamp()})")
        .unwrap();
    transaction.commit().unwrap();
    let defaulted = node_value(&mut db, "T", "defaulted", "at");
    let explicit = node_value(&mut db, "T", "explicit", "at");
    assert!(matches!(defaulted, Value::Int(_)), "{defaulted:?}");
    assert!(matches!(explicit, Value::Int(_)), "{explicit:?}");
    // The default is the transaction's instant truncated to milliseconds, so
    // it never exceeds a later statement's current_timestamp().
    let (defaulted, explicit) = (timestamp(&defaulted), timestamp(&explicit));
    assert!(defaulted <= explicit, "{defaulted} <= {explicit}");
    assert!(explicit - defaulted < 60_000_000_000);
    let ordered = db
        .query("MATCH (t:T) WHERE t.at <= current_timestamp() RETURN t.id AS id ORDER BY t.at")
        .unwrap();
    assert_eq!(
        column_values(&ordered, "id"),
        [
            Value::String("defaulted".into()),
            Value::String("explicit".into())
        ]
    );
    let compared = db
        .query(
            "MATCH (d:T {id: 'defaulted'}), (e:T {id: 'explicit'}) WHERE d.at <= e.at RETURN d.id AS id",
        )
        .unwrap();
    assert_eq!(compared.rows.len(), 1);
}

fn column_values(output: &QueryOutput, name: &str) -> Vec<Value> {
    output.rows.iter().map(|row| row[name].clone()).collect()
}

fn schema_state(db: &Database, table: &str, property: &str) -> SchemaObjectState {
    schema(&db.property_schemas().unwrap(), table, property).state
}

fn seed_backfill_table(db: &mut Database) {
    db.query("CREATE NODE TABLE B").unwrap();
    let ids = old_ids();
    let seed = ids.iter().map(|id| (id.as_str(), None)).collect::<Vec<_>>();
    db.query_with_params(
        "UNWIND $rows AS row CREATE (:B {id: row.id})",
        &BTreeMap::from([("rows".to_string(), rows(&seed))]),
    )
    .unwrap();
    db.query("CREATE (:B {id: 'old_null', q: NULL})").unwrap();
    db.query("CREATE (:B {id: 'old_value', q: 3.0})").unwrap();
    db.query("MATCH (a:B {id: 'old1'}), (b:B {id: 'old2'}) CREATE (a)-[:BR {k: 1}]->(b)")
        .unwrap();
    db.query("MATCH (a:B {id: 'old1'}), (b:B {id: 'old3'}) CREATE (a)-[:BR {k: 2}]->(b)")
        .unwrap();
}

fn old_ids() -> Vec<String> {
    (1..=6).map(|index| format!("old{index}")).collect()
}

#[test]
fn defaulted_property_on_non_empty_table_backfills_through_maintenance() {
    let path = unique_test_dir("property_defaults_backfill");
    {
        let mut db = Database::open(&path).unwrap();
        seed_backfill_table(&mut db);
        db.query("CREATE PROPERTY ON NODE TABLE B(q) TYPE DOUBLE NOT NULL DEFAULT 1.0")
            .unwrap_err();
        db.query("CREATE PROPERTY ON NODE TABLE B(q) TYPE DOUBLE DEFAULT 1.0")
            .unwrap();
        db.query("CREATE PROPERTY ON RELATIONSHIP TABLE BR(w) TYPE INT DEFAULT 9")
            .unwrap();
        assert_eq!(schema_state(&db, "B", "q"), SchemaObjectState::Backfill);
        assert_eq!(schema_state(&db, "BR", "w"), SchemaObjectState::Backfill);
        // Not yet filled: the default is never applied at read time.
        assert_eq!(node_value(&mut db, "B", "old1", "q"), Value::Null);
        assert!(!node_map(&mut db, "B", "old1").contains_key("q"));
        // Records created during the backfill follow the creation rule.
        db.query("CREATE (:B {id: 'during'})").unwrap();
        db.query("CREATE (:B {id: 'during_null', q: NULL})")
            .unwrap();
        assert_eq!(node_value(&mut db, "B", "during", "q"), Value::Float(1.0));
        let plan = db.plan_schema_maintenance().unwrap();
        assert!(plan
            .rows
            .iter()
            .any(|row| row["object"] == Value::String("B.q".into())));

        // One bounded batch fills a prefix and keeps the descriptor in
        // Backfill; the WAL batch is durable before the crash below.
        db.run_bounded_schema_maintenance(2).unwrap();
        assert_eq!(schema_state(&db, "B", "q"), SchemaObjectState::Backfill);
        let filled = old_ids()
            .iter()
            .filter(|id| node_value(&mut db, "B", id, "q") == Value::Float(1.0))
            .count();
        assert_eq!(filled, 2);
    }
    // Reopen without a checkpoint: replay resumes the same Backfill.
    {
        let mut db = Database::open(&path).unwrap();
        assert_eq!(schema_state(&db, "B", "q"), SchemaObjectState::Backfill);
        let filled = old_ids()
            .iter()
            .filter(|id| node_value(&mut db, "B", id, "q") == Value::Float(1.0))
            .count();
        assert_eq!(filled, 2);
        db.checkpoint().unwrap();
        db.run_schema_maintenance().unwrap();
        db.run_schema_maintenance().unwrap();
        assert_eq!(schema_state(&db, "B", "q"), SchemaObjectState::Public);
        assert_eq!(schema_state(&db, "BR", "w"), SchemaObjectState::Public);
    }
    {
        let mut db = Database::open(&path).unwrap();
        for id in old_ids().iter().chain([&"during".to_string()]) {
            assert_eq!(node_value(&mut db, "B", id, "q"), Value::Float(1.0), "{id}");
        }
        // Explicit NULLs written before the declaration stay NULL; present
        // values are untouched.
        assert_eq!(node_value(&mut db, "B", "old_null", "q"), Value::Null);
        assert_eq!(node_value(&mut db, "B", "during_null", "q"), Value::Null);
        assert_eq!(
            node_value(&mut db, "B", "old_value", "q"),
            Value::Float(3.0)
        );
        for key in [1, 2] {
            let output = db
                .query(&format!(
                    "MATCH (:B)-[r:BR]->(:B) WHERE r.k = {key} RETURN r.w AS w"
                ))
                .unwrap();
            assert_eq!(output.rows[0]["w"], Value::Int(9));
        }
    }
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn current_timestamp_backfill_writes_one_declaration_instant() {
    let mut db = Database::new();
    seed_backfill_table(&mut db);
    let before = now_millis();
    db.query("CREATE PROPERTY ON NODE TABLE B(seen_at) TYPE ANY DEFAULT current_timestamp()")
        .unwrap();
    let after = now_millis();
    std::thread::sleep(std::time::Duration::from_millis(20));
    for _ in 0..8 {
        db.run_bounded_schema_maintenance(1).unwrap();
        std::thread::sleep(std::time::Duration::from_millis(2));
    }
    db.run_schema_maintenance().unwrap();
    let values = old_ids()
        .iter()
        .chain(["old_value".to_string(), "old_null".to_string()].iter())
        .map(|id| timestamp(&node_value(&mut db, "B", id, "seen_at")))
        .collect::<Vec<_>>();
    assert!(values.iter().all(|value| *value == values[0]));
    assert!(before <= values[0] && values[0] <= after);
    let declared = schema(&db.property_schemas().unwrap(), "B", "seen_at")
        .default
        .clone()
        .unwrap();
    assert_eq!(declared.value, PropertyDefault::CurrentTimestamp);
    assert_eq!(declared.backfill_value, Value::Int(values[0]));
}

/// A crash that tears the final backfill batch (the one that also advances
/// the descriptor to Validating) must leave a resumable Backfill, never a
/// Validating/Public descriptor over partially filled records.
#[test]
fn torn_final_backfill_batch_reopens_resumable() {
    let path = unique_test_dir("property_defaults_torn_backfill");
    let wal_len_before;
    {
        let mut db = Database::open(&path).unwrap();
        seed_backfill_table(&mut db);
        db.query("CREATE PROPERTY ON NODE TABLE B(q) TYPE DOUBLE DEFAULT 1.0")
            .unwrap();
        db.run_bounded_schema_maintenance(2).unwrap();
        wal_len_before = std::fs::metadata(active_wal_path(&path)).unwrap().len();
        db.run_schema_maintenance().unwrap();
        assert_ne!(schema_state(&db, "B", "q"), SchemaObjectState::Backfill);
    }
    let wal = active_wal_path(&path);
    let wal_len_after = std::fs::metadata(&wal).unwrap().len();
    assert!(wal_len_after > wal_len_before);
    std::fs::OpenOptions::new()
        .write(true)
        .open(&wal)
        .unwrap()
        .set_len(wal_len_before + (wal_len_after - wal_len_before) / 2)
        .unwrap();

    let opened = Database::open_with_config(
        &path,
        DatabaseConfig {
            recovery_mode: RecoveryMode::Strict,
            ..Default::default()
        },
    )
    .unwrap();
    if opened.commit_epoch().is_err() {
        drop(opened);
        let plan =
            DatabaseDoctor::plan_wal_tail_repair(&path, WalDoctorOptions::default()).unwrap();
        DatabaseDoctor::apply_wal_tail_repair(
            &path,
            &plan,
            plan.acknowledge_potential_data_loss(),
            WalDoctorOptions::default(),
        )
        .unwrap();
    } else {
        drop(opened);
    }
    let mut db = Database::open(&path).unwrap();
    assert_eq!(schema_state(&db, "B", "q"), SchemaObjectState::Backfill);
    let filled = old_ids()
        .iter()
        .filter(|id| node_value(&mut db, "B", id, "q") == Value::Float(1.0))
        .count();
    assert_eq!(filled, 2, "the torn batch is lost as a whole");
    db.run_schema_maintenance().unwrap();
    db.run_schema_maintenance().unwrap();
    assert_eq!(schema_state(&db, "B", "q"), SchemaObjectState::Public);
    for id in old_ids() {
        assert_eq!(node_value(&mut db, "B", &id, "q"), Value::Float(1.0));
    }
    drop(db);
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn declared_defaults_are_introspectable_across_reopen() {
    let path = unique_test_dir("property_defaults_introspection");
    {
        let mut db = Database::open(&path).unwrap();
        declare_semantics_schema(&mut db);
        db.query("CREATE PROPERTY ON NODE TABLE E(plain) TYPE STRING")
            .unwrap();
        db.query("CREATE PROPERTY ON NODE TABLE E(at) TYPE ANY DEFAULT current_timestamp()")
            .unwrap();
    }
    for _ in 0..2 {
        let mut db = Database::open(&path).unwrap();
        let schemas = db.property_schemas().unwrap();
        let p = schema(&schemas, "E", "p");
        assert_eq!(p.table_kind, TableKind::Node);
        assert_eq!(p.value_type, PropertyType::Float);
        assert_eq!(p.state, SchemaObjectState::Public);
        assert_eq!(
            p.default.as_ref().map(|default| &default.value),
            Some(&PropertyDefault::Value(Value::Float(0.0)))
        );
        let w = schema(&schemas, "R", "w");
        assert_eq!(w.table_kind, TableKind::Relationship);
        assert_eq!(
            w.default.as_ref().map(|default| &default.value),
            Some(&PropertyDefault::Value(Value::Int(7)))
        );
        assert_eq!(schema(&schemas, "E", "plain").default, None);
        assert_eq!(
            schema(&schemas, "E", "at")
                .default
                .as_ref()
                .map(|default| &default.value),
            Some(&PropertyDefault::CurrentTimestamp)
        );
        db.checkpoint().unwrap();
    }
    std::fs::remove_dir_all(path).unwrap();
}

/// Database files written by main `d5ebd79b8`, before property defaults
/// existed, packed as `u32 path length, path, u64 length, bytes` entries.
const PRE_DEFAULTS_FIXTURE: &[u8] =
    include_bytes!("../../../tests/fixtures/pre_1683_property_descriptors.bundle");

pub(super) fn unpack_fixture(bundle: &[u8], target: &std::path::Path) {
    let mut rest = bundle
        .strip_prefix(b"HAWDBFIX1\n".as_slice())
        .expect("fixture bundle magic");
    while !rest.is_empty() {
        let (length, tail) = rest.split_at(4);
        let length = u32::from_le_bytes(length.try_into().unwrap()) as usize;
        let (relative, tail) = tail.split_at(length);
        let (length, tail) = tail.split_at(8);
        let length = u64::from_le_bytes(length.try_into().unwrap()) as usize;
        let (bytes, tail) = tail.split_at(length);
        let path = target.join(std::str::from_utf8(relative).unwrap());
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        std::fs::write(path, bytes).unwrap();
        rest = tail;
    }
}

/// A database written by HawDB before property defaults existed (generated
/// on main at d5ebd79b8: descriptors in both a checkpoint and the WAL tail)
/// reopens with every descriptor reporting no default.
#[test]
fn database_written_before_property_defaults_reopens_without_defaults() {
    let path = unique_test_dir("property_defaults_pre_change_fixture");
    unpack_fixture(PRE_DEFAULTS_FIXTURE, &path);
    {
        let mut db = Database::open(&path).unwrap();
        let schemas = db.property_schemas().unwrap();
        assert_eq!(schemas.len(), 3);
        for (table, property, value_type) in [
            ("Memory", "score", PropertyType::Float),
            ("Memory", "kind", PropertyType::String),
            ("LINKS", "weight", PropertyType::Int),
        ] {
            let schema = schema(&schemas, table, property);
            assert_eq!(schema.value_type, value_type);
            assert_eq!(schema.state, SchemaObjectState::Public);
            assert_eq!(schema.default, None, "{table}.{property}");
        }
        assert_eq!(
            node_value(&mut db, "Memory", "m1", "score"),
            Value::Float(1.5)
        );
        assert_eq!(node_value(&mut db, "Memory", "m2", "score"), Value::Null);
        assert_eq!(
            node_value(&mut db, "Memory", "m3", "kind"),
            Value::String("note".into())
        );
        db.query("CREATE (:Memory {id: 'm4'})").unwrap();
        assert!(!node_map(&mut db, "Memory", "m4").contains_key("score"));

        // The old database accepts a defaulted declaration and backfills it.
        db.query("CREATE PROPERTY ON NODE TABLE Memory(rank) TYPE INT DEFAULT 0")
            .unwrap();
        db.run_schema_maintenance().unwrap();
        db.run_schema_maintenance().unwrap();
        db.checkpoint().unwrap();
    }
    {
        let mut db = Database::open(&path).unwrap();
        for id in ["m1", "m2", "m3", "m4"] {
            assert_eq!(node_value(&mut db, "Memory", id, "rank"), Value::Int(0));
        }
        assert_eq!(
            schema_state(&db, "Memory", "rank"),
            SchemaObjectState::Public
        );
    }
    std::fs::remove_dir_all(path).unwrap();
}
