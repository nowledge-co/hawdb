// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

use super::*;
use hawdb_storage::scan::ScanPruningStrategy;
use std::collections::BTreeSet;

const EXACT: &str = "MATCH (t:Thread) WHERE (t.thread_id = $thread_id OR t.id = $thread_id) AND COALESCE(t.space_id, 'default') = $space_id RETURN t.id AS id, t.source AS source ORDER BY source";
const CANONICAL: &str = "MATCH (t:Thread) WHERE (t.thread_id = $thread_id OR t.id = $thread_id) AND CASE WHEN t.space_id IS NULL OR trim(t.space_id) = '' THEN 'default' ELSE t.space_id END = $space_id RETURN t.id AS id, t.source AS source ORDER BY source";

fn parameters(selector: Value, space: &str) -> BTreeMap<String, Value> {
    BTreeMap::from([
        ("thread_id".into(), selector),
        ("space_id".into(), Value::String(space.into())),
    ])
}

fn populate(db: &mut Database, indexed: bool, typed: bool) {
    db.query("CREATE NODE TABLE Thread").unwrap();
    db.query(if typed {
        "CREATE PROPERTY ON NODE TABLE Thread(space_id) TYPE STRING"
    } else {
        "CREATE PROPERTY ON NODE TABLE Thread(space_id) TYPE ANY"
    })
    .unwrap();
    let body = Value::String("x".repeat(65_536));
    for (id, thread_id, space, source) in [
        ("physical-a", "needle", Value::String("default".into()), "a"),
        ("needle", "other", Value::String("team".into()), "b"),
        ("needle", "needle", Value::Null, "c"),
        ("physical-d", "needle", Value::String(String::new()), "d"),
        ("physical-e", "needle", Value::String(" ".into()), "e"),
    ] {
        db.query_with_params(
            "CREATE (:Thread {id:$id, thread_id:$thread_id, space_id:$space, source:$source, body:$body})",
            &BTreeMap::from([
                ("id".into(), Value::String(id.into())),
                ("thread_id".into(), Value::String(thread_id.into())),
                ("space".into(), space),
                ("source".into(), Value::String(source.into())),
                ("body".into(), body.clone()),
            ]),
        ).unwrap();
    }
    db.query("CREATE (:Thread {space_id:'default', source:'null-key'})")
        .unwrap();
    for index in 0..96 {
        db.query_with_params(
            "CREATE (:Thread {id:$id, thread_id:$thread_id, space_id:'default', source:'filler', body:$body})",
            &BTreeMap::from([
                ("id".into(), Value::String(format!("physical-{index}"))),
                ("thread_id".into(), Value::String(format!("logical-{index}"))),
                ("body".into(), body.clone()),
            ]),
        ).unwrap();
    }
    if indexed {
        db.query("CREATE INDEX ON :Thread(id)").unwrap();
        db.query("CREATE INDEX ON :Thread(thread_id)").unwrap();
    }
}

fn database(indexed: bool, typed: bool) -> Database {
    let mut db = Database::new();
    populate(&mut db, indexed, typed);
    db
}

fn sources(rows: &crate::executor::QueryRows) -> Vec<Value> {
    rows.iter().map(|row| row["source"].clone()).collect()
}

#[test]
fn residual_union_keeps_exact_and_canonical_space_semantics_and_rebinding() {
    for typed in [false, true] {
        let mut reference = database(false, typed);
        let mut indexed = database(true, typed);
        for (query, expected) in [
            (EXACT, vec!["a", "c"]),
            (CANONICAL, vec!["a", "c", "d", "e"]),
        ] {
            let parameters = parameters(Value::String("needle".into()), "default");
            let plan = indexed
                .explain_query_with_params(query, &parameters)
                .unwrap();
            assert_eq!(
                plan.physical_plan.explain(0).contains("IndexNodeUnionSeek"),
                query == EXACT || typed
            );
            assert_eq!(
                sources(&indexed.query_with_params(query, &parameters).unwrap().rows),
                expected
                    .into_iter()
                    .map(|value| Value::String(value.into()))
                    .collect::<Vec<_>>()
            );
            for (selector, space) in [
                (Value::String("needle".into()), "team"),
                (Value::String("missing".into()), "default"),
                (Value::Null, "default"),
                (Value::String("needle".into()), "default"),
            ] {
                let parameters = self::parameters(selector, space);
                assert_eq!(
                    indexed.query_with_params(query, &parameters).unwrap().rows,
                    reference
                        .query_with_params(query, &parameters)
                        .unwrap()
                        .rows,
                    "typed={typed}, query={query}"
                );
            }
        }
    }
}

#[test]
fn residual_union_deduplicates_nodes_and_applies_scope_before_limit() {
    let mut db = database(true, true);
    let parameters = parameters(Value::String("needle".into()), "default");
    let query = "MATCH (t:Thread) WHERE (t.thread_id = $thread_id OR t.id = $thread_id) AND t.source <> 'none' RETURN t.id AS id, t.source AS source ORDER BY source";
    let output = db
        .explain_analyze_query_with_params(query, &parameters)
        .unwrap();
    assert!(output
        .physical_plan
        .explain(0)
        .contains("IndexNodeUnionSeek"));
    assert_eq!(
        sources(&output.output.rows),
        ["a", "b", "c", "d", "e"]
            .map(|source| Value::String(source.into()))
            .to_vec()
    );
    assert_eq!(
        output
            .output
            .rows
            .iter()
            .filter(|row| row["id"] == Value::String("needle".into()))
            .count(),
        2
    );
    let report = output
        .execution_profile
        .scan_pruning_reports
        .iter()
        .find(|report| matches!(report.strategy, ScanPruningStrategy::OrUnion))
        .unwrap();
    assert_eq!(report.candidate_count_before_filter, 5);
    for query in [EXACT, CANONICAL] {
        let limited = query.replace(" ORDER BY source", " LIMIT 2");
        let wrong_space_first = limited.replace(
            "t.thread_id = $thread_id OR t.id = $thread_id",
            "t.id = $thread_id OR t.thread_id = $thread_id",
        );
        for query in [limited, wrong_space_first] {
            let rows = db.query_with_params(&query, &parameters).unwrap().rows;
            assert_eq!(rows.len(), 2);
            let unique = sources(&rows).into_iter().collect::<BTreeSet<_>>();
            assert_eq!(unique.len(), 2);
            assert!(!unique.contains(&Value::String("b".into())));
        }
    }
}

#[test]
fn residual_union_preserves_errors_before_and_after_unknown_or() {
    for indexed in [false, true] {
        let mut db = database(indexed, false);
        let parameters = parameters(Value::String("missing".into()), "default");
        for predicate in [
            "head(1) = 1 AND (t.thread_id = $thread_id OR t.id = $thread_id)",
            "(t.thread_id = $thread_id OR t.id = $thread_id) AND head(1) = 1",
        ] {
            let query = format!("MATCH (t:Thread) WHERE {predicate} RETURN t.id");
            let plan = db.explain_query_with_params(&query, &parameters).unwrap();
            assert!(!plan.physical_plan.explain(0).contains("IndexNodeUnionSeek"));
            assert!(db
                .query_with_params(&query, &parameters)
                .unwrap_err()
                .to_string()
                .contains("head() requires a list"));
        }
        db.query("MATCH (t:Thread {source:'null-key'}) SET t.space_id = 1")
            .unwrap();
        let plan = db
            .explain_query_with_params(CANONICAL, &parameters)
            .unwrap();
        assert!(!plan.physical_plan.explain(0).contains("IndexNodeUnionSeek"));
        assert!(db
            .query_with_params(CANONICAL, &parameters)
            .unwrap_err()
            .to_string()
            .contains("trim() requires string arguments"));
    }
}

#[test]
fn residual_union_preserves_lookup_cap_and_index_authorization() {
    let mut db = database(true, true);
    let values = (0..33)
        .map(|index| Value::String(format!("physical-{index}")))
        .collect();
    let parameters = BTreeMap::from([("ids".into(), Value::List(values))]);
    let query = "MATCH (t:Thread) WHERE (t.id IN $ids OR t.thread_id IN $ids) AND COALESCE(t.space_id, 'default') = 'default' RETURN t.id AS id";
    let plan = db.explain_query_with_params(query, &parameters).unwrap();
    assert!(!plan.physical_plan.explain(0).contains("IndexNodeUnionSeek"));
    assert_eq!(
        db.query_with_params(query, &parameters).unwrap().rows.len(),
        33
    );

    let mut db = database(false, true);
    db.query("CREATE INDEX ON :Thread(id)").unwrap();
    let parameters = self::parameters(Value::String("needle".into()), "default");
    let plan = db.explain_query_with_params(EXACT, &parameters).unwrap();
    assert!(!plan.physical_plan.explain(0).contains("IndexNodeUnionSeek"));
    assert_eq!(
        sources(&db.query_with_params(EXACT, &parameters).unwrap().rows),
        vec![Value::String("a".into()), Value::String("c".into())]
    );
}

#[test]
fn residual_union_keeps_projection_and_query_memory_limits() {
    let mut db = database(true, true);
    let parameters = parameters(Value::String("needle".into()), "default");
    db.config.execution_memory.query_memory_bytes = NonZeroUsize::new(32_768).unwrap();
    db.config.execution_memory.batch_payload_bytes = NonZeroUsize::new(8192).unwrap();
    db.config.execution_memory.blocking_operator_bytes = NonZeroUsize::new(16_384).unwrap();
    db.config.execution_memory.batch_rows = NonZeroUsize::new(2).unwrap();
    assert_eq!(
        db.query_with_params(EXACT, &parameters).unwrap().rows.len(),
        2
    );
    let full = EXACT.replace(
        "RETURN t.id AS id, t.source AS source ORDER BY source",
        "RETURN t",
    );
    assert!(matches!(
        db.query_with_params(&full, &parameters),
        Err(HawDBError::ResourceBudgetExceeded(_))
    ));
    db.config.execution_memory.query_memory_bytes = NonZeroUsize::new(256 * 1024 * 1024).unwrap();
    db.config.execution_memory.blocking_operator_bytes = NonZeroUsize::new(64).unwrap();
    assert!(matches!(
        db.query_with_params(EXACT, &parameters),
        Err(HawDBError::ResourceBudgetExceeded(_))
    ));
}

#[test]
fn residual_union_preserves_canonical_snapshots_across_updates_and_reopen() {
    let path = unique_test_dir("residual_union");
    {
        let mut db = Database::open(&path).unwrap();
        populate(&mut db, true, true);
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
    let parameters = parameters(Value::String("needle".into()), "default");
    let before = db.query_with_params(CANONICAL, &parameters).unwrap().rows;
    assert_eq!(before.len(), 4);
    let mut snapshot = db.begin_read_transaction().unwrap();
    db.query("MATCH (t:Thread {source:'a'}) SET t.space_id = 'team'")
        .unwrap();
    db.checkpoint().unwrap();
    assert_eq!(
        snapshot
            .query_with_params(CANONICAL, &parameters)
            .unwrap()
            .rows,
        before
    );
    assert_eq!(
        db.query_with_params(CANONICAL, &parameters)
            .unwrap()
            .rows
            .len(),
        3
    );
    drop(snapshot);
    drop(db);
    let mut db = Database::open(&path).unwrap();
    assert_eq!(
        db.query_with_params(CANONICAL, &parameters)
            .unwrap()
            .rows
            .len(),
        3
    );
    drop(db);
    std::fs::remove_dir_all(path).unwrap();
}
