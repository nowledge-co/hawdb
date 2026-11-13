// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

//! Schema maintenance through the admitted embedded handle.
//!
//! A host that runs with background maintenance disabled declares a property
//! DEFAULT on data it already wrote. The descriptor enters `BACKFILL`, and only
//! schema maintenance fills the records lacking the property, so the handle
//! must expose the plan, a bounded run and the descriptor states.

use super::*;
use crate::schema::{PropertySchema, SchemaObjectState, TableKind};
use crate::QueryOutput;

fn open_offline_handle(graph_path: &std::path::Path) -> EmbeddedStoreHandle {
    let mut options = EmbeddedStoreOpenOptions::graph_only(graph_path, GraphMode::WritableCutover);
    let mut config = crate::DatabaseConfig::default();
    config.runtime_capabilities.background_maintenance = false;
    options.database_config = Some(config);
    EmbeddedStoreHandle::open_with_options(options).unwrap().0
}

fn run(handle: &EmbeddedStoreHandle, cypher: &str) -> QueryOutput {
    handle
        .query_with_report(cypher)
        .unwrap_or_else(|error| panic!("{cypher}: {error}"))
        .output
}

fn descriptor<'a>(
    schemas: &'a [PropertySchema],
    table: &str,
    property: &str,
) -> Option<&'a PropertySchema> {
    schemas
        .iter()
        .find(|schema| schema.table == table && schema.property == property)
}

fn state(handle: &EmbeddedStoreHandle, table: &str, property: &str) -> SchemaObjectState {
    descriptor(&handle.property_schemas().unwrap(), table, property)
        .unwrap_or_else(|| panic!("missing descriptor {table}.{property}"))
        .state
}

/// The stored node map, which distinguishes an absent property from an
/// explicitly written NULL.
fn entity(handle: &EmbeddedStoreHandle, id: &str) -> BTreeMap<String, Value> {
    let output = run(
        handle,
        &format!("MATCH (n:Entity {{id: '{id}'}}) RETURN n AS node"),
    );
    assert_eq!(output.rows.len(), 1, "Entity {id}");
    match &output.rows[0]["node"] {
        Value::Map(map) => map.clone(),
        other => panic!("node map expected, got {other:?}"),
    }
}

fn relationship_weight(handle: &EmbeddedStoreHandle, key: i64) -> Value {
    let output = run(
        handle,
        &format!("MATCH (:Entity)-[r:MENTIONS]->(:Entity) WHERE r.k = {key} RETURN r.w AS w"),
    );
    assert_eq!(output.rows.len(), 1, "MENTIONS k={key}");
    output.rows[0]["w"].clone()
}

const LACKING: [&str; 5] = ["n1", "n2", "n3", "n4", "n5"];

fn assert_backfilled(handle: &EmbeddedStoreHandle) {
    for id in LACKING {
        assert_eq!(
            entity(handle, id).get("p"),
            Some(&Value::Float(0.0)),
            "{id}"
        );
    }
    // An explicit NULL written before the declaration is kept, not filled.
    assert_eq!(entity(handle, "n_null").get("p"), Some(&Value::Null));
    assert_eq!(entity(handle, "n_value").get("p"), Some(&Value::Float(2.5)));
    // Undeclared properties written before the declaration are untouched.
    assert_eq!(
        entity(handle, "n1").get("pagerank_score"),
        Some(&Value::Float(0.3))
    );
    assert_eq!(relationship_weight(handle, 1), Value::Float(0.0));
    assert_eq!(relationship_weight(handle, 2), Value::Float(0.0));
    assert_eq!(relationship_weight(handle, 3), Value::Null);
    for (table, property) in [("Entity", "p"), ("MENTIONS", "w")] {
        assert_eq!(state(handle, table, property), SchemaObjectState::Public);
    }
}

#[test]
fn default_backfill_completes_through_handle_without_background_maintenance() {
    let path = unique_embedded_store_test_dir("handle_schema_maintenance");
    {
        let handle = open_offline_handle(&path);
        // `Entity` and `MENTIONS` are never declared with CREATE NODE TABLE /
        // CREATE RELATIONSHIP TABLE: schema-less writes only.
        for id in LACKING {
            run(
                &handle,
                &format!("CREATE (:Entity {{id: '{id}', pagerank_score: 0.3}})"),
            );
        }
        run(&handle, "CREATE (:Entity {id: 'n_null', p: NULL})");
        run(&handle, "CREATE (:Entity {id: 'n_value', p: 2.5})");
        for (key, target, weight) in [(1, "n2", ""), (2, "n3", ""), (3, "n4", ", w: NULL")] {
            run(
                &handle,
                &format!(
                    "MATCH (a:Entity {{id: 'n1'}}), (b:Entity {{id: '{target}'}}) \
                     CREATE (a)-[:MENTIONS {{k: {key}{weight}}}]->(b)"
                ),
            );
        }
        // Schema-less writes create no property descriptors, so a later
        // `CREATE PROPERTY ... DEFAULT` declares a fresh one.
        assert!(handle.property_schemas().unwrap().is_empty());
        assert!(handle.plan_schema_maintenance().unwrap().rows.is_empty());

        run(
            &handle,
            "CREATE PROPERTY ON NODE TABLE Entity(p) TYPE ANY DEFAULT 0.0",
        );
        run(
            &handle,
            "CREATE PROPERTY ON RELATIONSHIP TABLE MENTIONS(w) TYPE ANY DEFAULT 0.0",
        );
        let schemas = handle.property_schemas().unwrap();
        assert_eq!(schemas.len(), 2, "{schemas:?}");
        let entity_p = descriptor(&schemas, "Entity", "p").unwrap();
        assert_eq!(entity_p.table_kind, TableKind::Node);
        assert_eq!(entity_p.state, SchemaObjectState::Backfill);
        assert_eq!(
            entity_p.default.as_ref().unwrap().backfill_value,
            Value::Float(0.0)
        );
        assert_eq!(
            descriptor(&schemas, "MENTIONS", "w").unwrap().state,
            SchemaObjectState::Backfill
        );
        // Not yet filled: defaults are never applied at read time.
        assert!(!entity(&handle, "n1").contains_key("p"));

        let plan = handle.plan_schema_maintenance().unwrap();
        for object in ["Entity.p", "MENTIONS.w"] {
            assert!(
                plan.rows
                    .iter()
                    .any(|row| row["object"] == Value::String(object.into())
                        && row["from_state"] == Value::String("backfill".into())),
                "{object} missing from {plan:?}"
            );
        }
        // The plan is read-only.
        assert_eq!(state(&handle, "Entity", "p"), SchemaObjectState::Backfill);

        // One bounded pass fills a prefix and keeps the descriptor resumable.
        let first = handle.run_bounded_schema_maintenance(2).unwrap();
        assert!(!first.rows.is_empty());
        assert_eq!(state(&handle, "Entity", "p"), SchemaObjectState::Backfill);
        let filled = LACKING
            .iter()
            .filter(|id| entity(&handle, id).contains_key("p"))
            .count();
        assert_eq!(filled, 2);

        // Fills are resumable across small budgets.
        let backfilling = |handle: &EmbeddedStoreHandle| {
            handle
                .property_schemas()
                .unwrap()
                .iter()
                .any(|schema| schema.state == SchemaObjectState::Backfill)
        };
        let mut passes = 1;
        while backfilling(&handle) {
            handle.run_bounded_schema_maintenance(2).unwrap();
            passes += 1;
            assert!(passes < 32, "backfill did not converge");
        }
        assert!(passes >= 3, "budget must split the fills, took {passes}");
        assert_eq!(state(&handle, "Entity", "p"), SchemaObjectState::Validating);

        // Validation is one action per descriptor: a budget below its
        // planned estimate leaves it in VALIDATING, and the planned estimate
        // promotes it.
        let plan = handle.plan_schema_maintenance().unwrap();
        let planned = plan
            .rows
            .iter()
            .map(|row| match row["estimated_operations"] {
                Value::Int(operations) => usize::try_from(operations).unwrap(),
                ref other => panic!("estimated_operations {other:?}"),
            })
            .collect::<Vec<_>>();
        assert!(planned.iter().all(|operations| *operations > 2), "{plan:?}");
        handle.run_bounded_schema_maintenance(2).unwrap();
        assert_eq!(state(&handle, "Entity", "p"), SchemaObjectState::Validating);
        handle
            .run_bounded_schema_maintenance(planned.iter().sum())
            .unwrap();
        assert!(handle.plan_schema_maintenance().unwrap().rows.is_empty());
        assert_backfilled(&handle);

        // `TYPE ANY` constrains nothing else: other undeclared properties of
        // the implicit table still accept any value and get no descriptor.
        run(
            &handle,
            "CREATE (:Entity {id: 'later', pagerank_score: 'high', extra: [1, 2]})",
        );
        assert_eq!(entity(&handle, "later").get("p"), Some(&Value::Float(0.0)));
        run(
            &handle,
            "MATCH (n:Entity {id: 'n2'}) SET n.p = 'text', n.extra = true",
        );
        assert_eq!(handle.property_schemas().unwrap().len(), 2);
    }
    {
        // Reopen without a checkpoint: WAL replay restores fills and states.
        let handle = open_offline_handle(&path);
        assert!(handle.plan_schema_maintenance().unwrap().rows.is_empty());
        for id in ["n1", "n3", "n4", "n5"] {
            assert_eq!(
                entity(&handle, id).get("p"),
                Some(&Value::Float(0.0)),
                "{id}"
            );
        }
        assert_eq!(entity(&handle, "n_null").get("p"), Some(&Value::Null));
        assert_eq!(
            entity(&handle, "n_value").get("p"),
            Some(&Value::Float(2.5))
        );
        assert_eq!(state(&handle, "Entity", "p"), SchemaObjectState::Public);
        assert_eq!(state(&handle, "MENTIONS", "w"), SchemaObjectState::Public);
        assert_eq!(relationship_weight(&handle, 1), Value::Float(0.0));
        assert_eq!(relationship_weight(&handle, 3), Value::Null);
        handle.checkpoint().unwrap();
    }
    {
        // And after a checkpoint.
        let handle = open_offline_handle(&path);
        assert_eq!(state(&handle, "Entity", "p"), SchemaObjectState::Public);
        assert_eq!(entity(&handle, "n1").get("p"), Some(&Value::Float(0.0)));
        assert_eq!(entity(&handle, "n_null").get("p"), Some(&Value::Null));
        assert_eq!(relationship_weight(&handle, 2), Value::Float(0.0));
    }
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn create_property_never_silently_drops_a_changed_declaration() {
    let path = unique_embedded_store_test_dir("handle_property_redeclaration");
    let handle = open_offline_handle(&path);
    run(&handle, "CREATE (:Entity {id: 'a'})");
    run(&handle, "CREATE PROPERTY ON NODE TABLE Entity(p) TYPE ANY");
    // Re-declaring the identical descriptor stays idempotent.
    let again = run(&handle, "CREATE PROPERTY ON NODE TABLE Entity(p) TYPE ANY");
    assert_eq!(again.rows[0]["created"], Value::Bool(false));
    // A DEFAULT, type or nullability change on an existing descriptor is an
    // explicit error, never a `created: false` that drops the change.
    for statement in [
        "CREATE PROPERTY ON NODE TABLE Entity(p) TYPE ANY DEFAULT 0.0",
        "CREATE PROPERTY ON NODE TABLE Entity(p) TYPE FLOAT",
        "CREATE PROPERTY ON NODE TABLE Entity(p) TYPE ANY NOT NULL",
    ] {
        let error = handle.query_with_report(statement).unwrap_err();
        assert!(
            error.to_string().contains("already declared"),
            "{statement}: {error}"
        );
    }
    let schemas = handle.property_schemas().unwrap();
    let entity_p = descriptor(&schemas, "Entity", "p").unwrap();
    assert_eq!(entity_p.default, None);
    assert_eq!(entity_p.state, SchemaObjectState::Public);
    assert!(handle.plan_schema_maintenance().unwrap().rows.is_empty());

    // A defaulted descriptor re-declared with the same default is idempotent;
    // a different default is refused.
    run(
        &handle,
        "CREATE PROPERTY ON NODE TABLE Entity(q) TYPE ANY DEFAULT 1",
    );
    let same = run(
        &handle,
        "CREATE PROPERTY ON NODE TABLE Entity(q) TYPE ANY DEFAULT 1",
    );
    assert_eq!(same.rows[0]["created"], Value::Bool(false));
    handle
        .query_with_report("CREATE PROPERTY ON NODE TABLE Entity(q) TYPE ANY DEFAULT 2")
        .unwrap_err();
    handle
        .query_with_report("CREATE PROPERTY ON NODE TABLE Entity(q) TYPE ANY")
        .unwrap_err();
    drop(handle);
    std::fs::remove_dir_all(path).unwrap();
}
