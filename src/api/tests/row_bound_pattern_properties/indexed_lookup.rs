// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

use super::*;
use crate::executor::ExecutionMemoryConfig;
use hawdb_storage::scan::ScanPruningStrategy;

const SCOPED: &str = "UNWIND $rows AS candidate OPTIONAL MATCH (n:Item {id: candidate.id, active: true, space_id: $space}) WHERE n.rank >= candidate.minimum RETURN candidate.ordinal AS ordinal, n.id AS id, n.rank AS rank ORDER BY ordinal, rank";

fn populate(db: &mut Database, indexed: bool) {
    for key in 0..32 {
        for rank in 0..2 {
            db.query_with_params(
                "CREATE (:Item {id: $id, rank: $rank, active: true, space_id: 'a', body: $body})",
                &BTreeMap::from([
                    ("id".into(), Value::String(format!("k{key}"))),
                    ("rank".into(), Value::Int(rank)),
                    ("body".into(), Value::String("x".repeat(32_768))),
                ]),
            )
            .unwrap();
        }
    }
    for query in [
        "CREATE (:Item {id: 'foreign', rank: 0, active: true, space_id: 'b'})",
        "CREATE (:Item {id: 'inactive', rank: 0, active: false, space_id: 'a'})",
        "CREATE (:Item {id: null, rank: 0, active: true, space_id: 'a'})",
        "CREATE (:Other {id: 'missing', rank: 0, active: true, space_id: 'a'})",
        "CREATE (:Key {id: 'k0'})",
    ] {
        db.query(query).unwrap();
    }
    if indexed {
        db.query("CREATE INDEX ON :Item(id)").unwrap();
    }
}

fn database(indexed: bool) -> Database {
    let mut db = Database::new();
    populate(&mut db, indexed);
    db
}

fn candidate(id: Value, ordinal: i64, minimum: i64) -> Value {
    Value::Map(BTreeMap::from([
        ("id".into(), id),
        ("ordinal".into(), Value::Int(ordinal)),
        ("minimum".into(), Value::Int(minimum)),
    ]))
}

fn params(rows: Vec<Value>, space: &str) -> BTreeMap<String, Value> {
    BTreeMap::from([
        ("rows".into(), Value::List(rows)),
        ("space".into(), Value::String(space.into())),
    ])
}

fn row(ordinal: i64, id: Option<&str>, rank: Option<i64>) -> BTreeMap<String, Value> {
    BTreeMap::from([
        ("ordinal".into(), Value::Int(ordinal)),
        (
            "id".into(),
            id.map(|id| Value::String(id.into())).unwrap_or(Value::Null),
        ),
        ("rank".into(), rank.map(Value::Int).unwrap_or(Value::Null)),
    ])
}

#[test]
fn indexed_lookup_preserves_optional_filters_duplicates_and_parameter_rebinding() {
    let rows = vec![
        candidate(Value::String("k0".into()), 0, 0),
        candidate(Value::String("k0".into()), 0, 0),
        candidate(Value::String("k0".into()), 1, 1),
        candidate(Value::String("k1".into()), 2, 99),
        candidate(Value::String("missing".into()), 3, 0),
        candidate(Value::Null, 4, 0),
        Value::Map(BTreeMap::from([("ordinal".into(), Value::Int(5))])),
        candidate(Value::String("foreign".into()), 6, 0),
        candidate(Value::String("inactive".into()), 7, 0),
    ];
    let expected = vec![
        row(0, Some("k0"), Some(0)),
        row(0, Some("k0"), Some(0)),
        row(0, Some("k0"), Some(1)),
        row(0, Some("k0"), Some(1)),
        row(1, Some("k0"), Some(1)),
        row(2, None, None),
        row(3, None, None),
        row(4, None, None),
        row(5, None, None),
        row(6, None, None),
        row(7, None, None),
    ];
    for indexed in [false, true] {
        let mut db = database(indexed);
        for space in ["a", "b", "a"] {
            let output = db
                .query_with_params(SCOPED, &params(rows.clone(), space))
                .unwrap();
            if space == "a" {
                assert_eq!(output.rows, expected, "indexed={indexed}");
            } else {
                let mut expected = (0..8)
                    .map(|ordinal| row(ordinal, None, None))
                    .collect::<Vec<_>>();
                expected.insert(0, row(0, None, None));
                expected[7] = row(6, Some("foreign"), Some(0));
                assert_eq!(output.rows, expected, "indexed={indexed}");
            }
        }
        let inner = SCOPED.replace("OPTIONAL MATCH", "MATCH");
        let output = db
            .query_with_params(&inner, &params(rows.clone(), "a"))
            .unwrap();
        assert_eq!(output.rows, expected[..5].to_vec(), "indexed={indexed}");
    }
}

#[test]
fn indexed_lookup_reads_matching_keys_and_keeps_wide_properties_unrequested() {
    for source in ["candidate", "candidate.id"] {
        let mut db = database(true);
        let query = format!("UNWIND $rows AS candidate OPTIONAL MATCH (n:Item {{id: {source}, active: true, space_id: $space}}) WHERE n.rank >= 0 RETURN n.id AS id, n.rank AS rank");
        let rows = ["k0", "k0", "k1", "missing"]
            .into_iter()
            .enumerate()
            .map(|(ordinal, id)| {
                if source == "candidate" {
                    Value::String(id.into())
                } else {
                    candidate(Value::String(id.into()), ordinal as i64, 0)
                }
            })
            .collect();
        let output = db
            .explain_analyze_query_with_params(&query, &params(rows, "a"))
            .unwrap();
        assert!(output.physical_plan.explain(0).contains("GraphMatchExec"));
        assert_eq!(output.output.rows.len(), 7);
        let reports = &output.execution_profile.scan_pruning_reports;
        assert_eq!(reports.len(), 4, "{reports:?}");
        assert!(reports.iter().all(|report| matches!(&report.strategy, ScanPruningStrategy::PropertyEq { property } if property == "id")));
        assert_eq!(
            reports
                .iter()
                .map(|report| report.candidate_count_before_filter)
                .sum::<usize>(),
            6
        );
        assert_eq!(
            reports
                .iter()
                .map(|report| report.output_count)
                .sum::<usize>(),
            6
        );
    }
    // A full 32 KiB body cannot fit this source/working budget. The selected
    // projected records and small result do; adding the index must retain that.
    for indexed in [false, true] {
        let mut db = Database::new_with_config(DatabaseConfig {
            execution_memory: ExecutionMemoryConfig {
                blocking_operator_bytes: NonZeroUsize::new(16_384).unwrap(),
                batch_payload_bytes: NonZeroUsize::new(8192).unwrap(),
                batch_rows: NonZeroUsize::new(2).unwrap(),
                ..ExecutionMemoryConfig::default()
            },
            ..DatabaseConfig::default()
        });
        populate(&mut db, indexed);
        let parameters = params(vec![candidate(Value::String("k0".into()), 0, 0)], "a");
        assert_eq!(
            db.query_with_params(SCOPED, &parameters)
                .unwrap()
                .rows
                .len(),
            2
        );
        let full =
            "UNWIND $rows AS candidate MATCH (n:Item {id: candidate.id, active: true}) RETURN n";
        assert!(db.query_with_params(full, &parameters).is_err());
    }
}

#[test]
fn indexed_lookup_supports_native_property_keys_and_both_equality_directions() {
    for indexed in [false, true] {
        let mut db = database(indexed);
        for equality in ["n.id = source.id", "source.id = n.id"] {
            let query = format!("MATCH (source:Key) OPTIONAL MATCH (n:Item {{active: true}}) WHERE n.space_id = 'a' AND {equality} RETURN n.id AS id, n.rank AS rank ORDER BY rank");
            let output = db.query(&query).unwrap();
            assert_eq!(output.rows.len(), 2);
            assert_eq!(output.rows[0]["rank"], Value::Int(0));
            assert_eq!(output.rows[1]["rank"], Value::Int(1));
        }
    }
}

#[test]
fn indexed_lookup_keeps_fallible_keys_conjunct_order_and_limit_stop() {
    for indexed in [false, true] {
        let mut db = database(indexed);
        let parameters = params(vec![Value::Int(1)], "a");
        assert!(db.query_with_params(SCOPED, &parameters).is_err());
        let absent = "UNWIND $rows AS candidate OPTIONAL MATCH (n:Absent {id: candidate.id, active: true}) RETURN n.id AS id";
        assert!(db.query_with_params(absent, &parameters).is_ok());
        let parameters = params(
            vec![candidate(Value::String("k0".into()), 0, 0), Value::Int(1)],
            "a",
        );
        assert_eq!(db.query_with_params("UNWIND $rows AS candidate OPTIONAL MATCH (n:Item {id: candidate.id, active: true}) RETURN n.id AS id LIMIT 1", &parameters).unwrap().rows.len(), 1);
        let parameters = BTreeMap::from([(
            "ids".into(),
            Value::List(vec![Value::String("missing".into())]),
        )]);
        let leading = "UNWIND $ids AS candidate MATCH (n:Item) WHERE head(1) = 1 AND n.id = candidate RETURN n.id";
        assert!(db.query_with_params(leading, &parameters).is_err());
        let trailing = "UNWIND $ids AS candidate MATCH (n:Item) WHERE n.id = candidate AND head(1) = 1 RETURN n.id";
        assert!(db
            .query_with_params(trailing, &parameters)
            .unwrap()
            .rows
            .is_empty());
    }
}

#[test]
fn indexed_lookup_preserves_canonical_and_pinned_snapshot_views() {
    let path = unique_test_dir("row_bound_indexed_lookup");
    {
        let mut db = Database::open(&path).unwrap();
        populate(&mut db, true);
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
    let parameters = params(vec![candidate(Value::String("k0".into()), 0, 0)], "a");
    let before = db.query_with_params(SCOPED, &parameters).unwrap().rows;
    assert_eq!(before.len(), 2);
    let mut snapshot = db.begin_read_transaction().unwrap();
    db.query("MATCH (n:Item {id:'k0'}) SET n.active = false")
        .unwrap();
    db.checkpoint().unwrap();
    assert_eq!(
        snapshot
            .query_with_params(SCOPED, &parameters)
            .unwrap()
            .rows,
        before
    );
    assert_eq!(
        db.query_with_params(SCOPED, &parameters).unwrap().rows,
        vec![row(0, None, None)]
    );
    drop(snapshot);
    drop(db);
    let mut db = Database::open(&path).unwrap();
    assert_eq!(
        db.query_with_params(SCOPED, &parameters).unwrap().rows,
        vec![row(0, None, None)]
    );
    drop(db);
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn indexed_lookup_preserves_disjunction_multistep_and_reused_name_fallbacks() {
    let mut generic = database(false);
    let mut indexed = database(true);
    for query in [
        "UNWIND ['k0'] AS candidate OPTIONAL MATCH (n:Item {active:true}) WHERE n.id = candidate OR n.id = 'foreign' RETURN n.id AS id, n.rank AS rank ORDER BY id, rank",
        "MATCH (source:Key) OPTIONAL MATCH (n:Item {id:source.id, active:true})-[:LINK]->(target:Item) RETURN target.id AS id",
        "UNWIND ['k0'] AS n WITH n AS candidate OPTIONAL MATCH (n:Item {id:candidate, active:true}) RETURN n.id AS id, n.rank AS rank ORDER BY id, rank",
    ] {
        let expected = generic.query(query).unwrap().rows;
        assert_eq!(indexed.query(query).unwrap().rows, expected, "{query}");
    }
}
