// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

use super::*;
use hawdb_storage::scan::ScanPruningStrategy;

const QUERY: &str = "MATCH (e:Entity {id:$id, active:true}) OPTIONAL MATCH (e)-[r1:RELATES_TO]-() OPTIONAL MATCH ()-[r2:RELATES_TO]->(e) RETURN (count(DISTINCT r1) + count(DISTINCT r2))";

fn parameters(id: Value) -> BTreeMap<String, Value> {
    BTreeMap::from([("id".into(), id)])
}

fn populate(db: &mut Database, indexed: bool) {
    let body = Value::String("x".repeat(32_768));
    for index in 0..32 {
        db.query_with_params(
            "CREATE (:Other {id:$id, body:$body})",
            &BTreeMap::from([
                ("id".into(), Value::Int(index)),
                ("body".into(), body.clone()),
            ]),
        )
        .unwrap();
    }
    for (id, active) in [
        (Value::String("target".into()), true),
        (Value::String("target".into()), true),
        (Value::String("target".into()), false),
        (Value::String("other".into()), true),
        (Value::Null, true),
    ] {
        db.query_with_params(
            "CREATE (:Entity {id:$id, active:$active, body:$body})",
            &BTreeMap::from([
                ("id".into(), id),
                ("active".into(), Value::Bool(active)),
                ("body".into(), body.clone()),
            ]),
        )
        .unwrap();
    }
    db.query("CREATE (:Target {id:'target'})").unwrap();
    for query in [
        "MATCH (e:Entity {id:'target', active:true}), (t:Target {id:'target'}) CREATE (e)-[:RELATES_TO]->(t)",
        "MATCH (e:Entity {id:'target', active:true}), (t:Target {id:'target'}) CREATE (e)-[:RELATES_TO]->(t)",
        "MATCH (e:Entity {id:'target', active:true}), (t:Target {id:'target'}) CREATE (t)-[:RELATES_TO]->(e)",
        "MATCH (e:Entity {id:'target', active:true}) CREATE (e)-[:RELATES_TO]->(e)",
    ] {
        db.query(query).unwrap();
    }
    if indexed {
        db.query("CREATE INDEX ON :Entity(id)").unwrap();
    }
}

fn count(db: &mut Database, query: &str, id: Value) -> Value {
    let output = db.query_with_params(query, &parameters(id)).unwrap();
    assert_eq!(output.rows.len(), 1);
    assert_eq!(output.rows[0].len(), 1);
    output.rows[0].values().next().unwrap().clone()
}

#[test]
fn relationship_count_source_seeks_index_and_preserves_all_source_filters() {
    for indexed in [false, true] {
        let mut db = Database::new();
        populate(&mut db, indexed);
        let output = db
            .explain_analyze_query_with_params(QUERY, &parameters(Value::String("target".into())))
            .unwrap();
        assert!(output
            .physical_plan
            .explain(0)
            .contains("OptionalRelationshipCountSumExec"));
        assert_eq!(output.output.rows[0].values().next(), Some(&Value::Int(12)));
        let reports = &output.execution_profile.scan_pruning_reports;
        let report = reports
            .iter()
            .find(|report| {
                report.target_kind == hawdb_storage::scan::ScanPruningTargetKind::Node
                    && (matches!(
                        &report.strategy,
                        ScanPruningStrategy::PropertyEq { property } if property == "id"
                    ) || matches!(report.strategy, ScanPruningStrategy::FullLabelScan))
            })
            .unwrap();
        assert_eq!(
            report.candidate_count_before_filter,
            if indexed { 3 } else { 5 }
        );
        assert_eq!(report.output_count, 2);
        assert_eq!(report.filtered_out_count, if indexed { 1 } else { 3 });
        for id in [Value::String("missing".into()), Value::Null] {
            assert_eq!(count(&mut db, QUERY, id), Value::Int(0));
        }
    }
}

#[test]
fn relationship_count_source_excludes_wide_properties_under_query_budget() {
    for indexed in [false, true] {
        let mut db = Database::new();
        populate(&mut db, indexed);
        db.config.execution_memory.query_memory_bytes = NonZeroUsize::new(16_384).unwrap();
        db.config.execution_memory.batch_payload_bytes = NonZeroUsize::new(4096).unwrap();
        db.config.execution_memory.blocking_operator_bytes = NonZeroUsize::new(8192).unwrap();
        assert_eq!(
            count(&mut db, QUERY, Value::String("target".into())),
            Value::Int(12)
        );
        let error = db
            .query_with_params(
                "MATCH (e:Entity {id:$id, active:true}) RETURN e",
                &parameters(Value::String("target".into())),
            )
            .unwrap_err();
        assert!(matches!(error, HawDBError::ResourceBudgetExceeded(_)));
        let missing = QUERY.replace(":Entity", ":Absent");
        assert_eq!(count(&mut db, &missing, Value::Null), Value::Int(0));
    }
}

#[test]
fn relationship_count_source_preserves_untyped_and_unfiltered_legs() {
    let mut db = Database::new();
    populate(&mut db, true);
    let untyped = QUERY.replace(":RELATES_TO", "");
    assert_eq!(
        count(&mut db, &untyped, Value::String("target".into())),
        Value::Int(12)
    );
    let all_sources = QUERY.replace(" {id:$id, active:true}", "");
    assert_eq!(count(&mut db, &all_sources, Value::Null), Value::Int(12));
    let ordinary_counts = QUERY.replace("DISTINCT ", "");
    assert_eq!(
        count(&mut db, &ordinary_counts, Value::String("target".into())),
        Value::Int(12)
    );
}

#[test]
fn relationship_count_source_preserves_canonical_and_pinned_views() {
    for indexed in [false, true] {
        let path = unique_test_dir("relationship_count_source");
        {
            let mut db = Database::open(&path).unwrap();
            populate(&mut db, indexed);
            db.checkpoint().unwrap();
        }
        let mut db = Database::open_with_config(
            &path,
            DatabaseConfig {
                storage_residency_mode: hawdb_storage::config::StorageResidencyMode::OutOfCore,
                ..DatabaseConfig::default()
            },
        )
        .unwrap();
        let parameters = parameters(Value::String("target".into()));
        let before = db.query_with_params(QUERY, &parameters).unwrap().rows;
        assert_eq!(before[0].values().next(), Some(&Value::Int(12)));
        let mut snapshot = db.begin_read_transaction().unwrap();
        db.query("MATCH (e:Entity {id:'target'}) SET e.active = false")
            .unwrap();
        db.checkpoint().unwrap();
        assert_eq!(
            snapshot.query_with_params(QUERY, &parameters).unwrap().rows,
            before
        );
        assert_eq!(
            db.query_with_params(QUERY, &parameters).unwrap().rows[0]
                .values()
                .next(),
            Some(&Value::Int(0))
        );
        drop(snapshot);
        drop(db);
        let mut db = Database::open(&path).unwrap();
        assert_eq!(
            count(&mut db, QUERY, Value::String("target".into())),
            Value::Int(0)
        );
        drop(db);
        std::fs::remove_dir_all(path).unwrap();
    }
}
