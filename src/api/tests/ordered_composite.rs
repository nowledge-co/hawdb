// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

use super::*;

fn populate(db: &mut Database, nullable: bool, indexed: bool) {
    db.query("CREATE NODE TABLE Item").unwrap();
    for (property, kind) in [("rank", "INT"), ("id", "STRING")] {
        db.query(&format!(
            "CREATE PROPERTY ON NODE TABLE Item({property}) TYPE {kind}{}",
            if nullable { "" } else { " NOT NULL" }
        ))
        .unwrap();
    }
    db.query("CREATE CONSTRAINT ON :Item(id) ASSERT UNIQUE")
        .unwrap();
    let rows = (0..96)
        .map(|id| {
            Value::Map(BTreeMap::from([
                (
                    "kind".into(),
                    Value::String(if id % 5 == 0 { "archive" } else { "note" }.into()),
                ),
                ("rank".into(), Value::Int(id / 4)),
                ("id".into(), Value::String(format!("item-{id:04}"))),
            ]))
        })
        .collect();
    // Load one bounded fixture transaction so native-path assertions do not
    // depend on intermediate per-row statistics publication/cost choices.
    db.query_with_params(
        "UNWIND $rows AS r CREATE (:Item {kind: r.kind, rank: r.rank, id: r.id})",
        &BTreeMap::from([("rows".into(), Value::List(rows))]),
    )
    .unwrap();
    if indexed {
        db.query("CREATE INDEX ON :Item(kind, rank, id)").unwrap();
    }
}

fn queries(descending: bool, size: usize) -> (String, String, String) {
    let (direction, op) = if descending {
        ("DESC", "<")
    } else {
        ("ASC", ">")
    };
    let suffix = format!(
        "RETURN n.rank AS rank, n.id AS id ORDER BY rank {direction}, id {direction} LIMIT {size}"
    );
    (
        format!("MATCH (n:Item) WHERE n.kind = $kind {suffix}"),
        format!("MATCH (n:Item) WHERE n.kind = $kind AND (n.rank {op} $rank OR (n.rank = $rank AND n.id {op} $id)) {suffix}"),
        format!("MATCH (n:Item) WHERE coalesce(n.kind, '') = $kind RETURN n.rank AS rank, n.id AS id ORDER BY coalesce(rank, 0) {direction}, coalesce(id, '') {direction}"),
    )
}

fn assert_pages(
    mut query: impl FnMut(&str, &BTreeMap<String, Value>) -> crate::Result<QueryOutput>,
) {
    for descending in [false, true] {
        for kind in ["note", "archive", "absent"] {
            for size in [1, 7, 32] {
                let (first, next, reference) = queries(descending, size);
                let mut params = BTreeMap::from([("kind".into(), Value::String(kind.into()))]);
                let expected = query(&reference, &params).unwrap().rows.into_rows();
                let mut actual = Vec::new();
                loop {
                    let rows = query(if actual.is_empty() { &first } else { &next }, &params)
                        .unwrap()
                        .rows
                        .into_rows();
                    if rows.is_empty() {
                        break;
                    }
                    let last = rows.last().unwrap();
                    params.insert("rank".into(), last["rank"].clone());
                    params.insert("id".into(), last["id"].clone());
                    actual.extend(rows);
                    assert!(
                        actual.len() <= expected.len(),
                        "duplicate or non-advancing page"
                    );
                }
                assert_eq!(
                    actual, expected,
                    "descending={descending} kind={kind} size={size}"
                );
            }
        }
    }
}

fn assert_native_plan(db: &mut Database, phase: &str) {
    for descending in [false, true] {
        let (first, next, _) = queries(descending, 7);
        let params = BTreeMap::from([
            ("kind".into(), Value::String("note".into())),
            ("rank".into(), Value::Int(12)),
            ("id".into(), Value::String("item-0048".into())),
        ]);
        for query in [&first, &next] {
            let plan = db
                .explain_query_with_params(query, &params)
                .unwrap()
                .physical_plan
                .explain(0);
            assert!(
                plan.contains("IndexNodeCompositeOrderedSeek"),
                "{phase}: {plan}"
            );
            assert!(
                !plan.contains("SortExec") && !plan.contains("TopNExec"),
                "{phase}: {plan}"
            );
            if query == &next {
                assert!(plan.contains("after: Some"), "{phase}: {plan}");
            }
        }
    }
}

#[test]
fn ordered_composite_resident_query_pages_use_native_order() {
    let mut db = Database::new();
    populate(&mut db, false, true);
    assert_native_plan(&mut db, "resident");
    assert_pages(|query, params| db.query_with_params(query, params));
}

#[test]
fn ordered_composite_query_pages_preserve_pinned_checkpoint_delta_and_reopen() {
    let path = unique_test_dir("ordered_composite_query");
    let config = DatabaseConfig {
        storage_residency_mode: hawdb_storage::config::StorageResidencyMode::OutOfCore,
        ..Default::default()
    };
    let mut db = Database::open_with_config(&path, config.clone()).unwrap();
    populate(&mut db, false, true);
    // Before the first checkpoint an OutOfCore handle need not expose an
    // ordered projection. Complete page semantics remain independent of selection.
    assert_pages(|query, params| db.query_with_params(query, params));
    db.checkpoint().unwrap();
    assert_native_plan(&mut db, "checkpoint");
    let mut pinned = db.begin_read_transaction().unwrap();
    db.query("MATCH (n:Item) WHERE n.id = 'item-0001' SET n.rank = 40")
        .unwrap();
    db.query("MATCH (n:Item) WHERE n.id = 'item-0094' SET n.rank = -1")
        .unwrap();
    db.query("MATCH (n:Item) WHERE n.id = 'item-0011' SET n.kind = 'archive'")
        .unwrap();
    db.query("MATCH (n:Item) WHERE n.id = 'item-0012' DELETE n")
        .unwrap();
    db.query("CREATE (:Item {kind: 'note', rank: 12, id: 'new-item'})")
        .unwrap();
    assert_pages(|query, params| pinned.query_with_params(query, params));
    assert_native_plan(&mut db, "delta");
    assert_pages(|query, params| db.query_with_params(query, params));
    db.checkpoint().unwrap();
    assert_pages(|query, params| pinned.query_with_params(query, params));
    drop(pinned);
    drop(db);
    let mut db = Database::open_with_config(&path, config).unwrap();
    assert_native_plan(&mut db, "reopen");
    assert_pages(|query, params| db.query_with_params(query, params));
    drop(db);
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn ordered_composite_nullable_sparse_keys_keep_complete_sort() {
    let mut db = Database::new();
    populate(&mut db, true, true);
    db.query("CREATE (:Item {kind: 'note', rank: -1})").unwrap();
    db.query("CREATE (:Item {kind: 'note', id: 'no-rank'})")
        .unwrap();
    let (query, _, oracle) = queries(false, 200);
    let params = BTreeMap::from([("kind".into(), Value::String("note".into()))]);
    let plan = db
        .explain_query_with_params(&query, &params)
        .unwrap()
        .physical_plan
        .explain(0);
    assert!(!plan.contains("IndexNodeCompositeOrderedSeek"), "{plan}");
    assert!(
        plan.contains("SortExec") || plan.contains("TopNExec"),
        "{plan}"
    );
    // Use the same NULL ordering as the canonical query, while preventing index inference.
    let oracle = oracle
        .replace("coalesce(rank, 0)", "rank")
        .replace("coalesce(id, '')", "id");
    assert_eq!(
        db.query_with_params(&query, &params).unwrap().rows,
        db.query_with_params(&oracle, &params).unwrap().rows
    );
}

#[test]
fn ordered_composite_cache_types_and_runtime_projection_capability_are_fenced() {
    let path = unique_test_dir("ordered_composite_cache");
    let config = DatabaseConfig {
        storage_residency_mode: hawdb_storage::config::StorageResidencyMode::OutOfCore,
        ..Default::default()
    };
    let mut db = Database::open_with_config(&path, config).unwrap();
    populate(&mut db, false, false);
    db.checkpoint().unwrap();
    db.query("CREATE INDEX ON :Item(kind, rank, id)").unwrap();
    let (first, next, _) = queries(false, 7);
    let mut params = BTreeMap::from([
        ("kind".into(), Value::String("note".into())),
        ("rank".into(), Value::Int(12)),
        ("id".into(), Value::String("item-0048".into())),
    ]);
    let plan = db
        .explain_query_with_params(&first, &params)
        .unwrap()
        .physical_plan
        .explain(0);
    assert!(!plan.contains("IndexNodeCompositeOrderedSeek"), "{plan}");
    let old = db.runtime_planning_snapshot().unwrap();
    let ordered_prepared = old.prepare(first.clone(), &params).unwrap();
    let point_params = BTreeMap::from([("lookup".into(), Value::String("item-0001".into()))]);
    let point_prepared = old
        .prepare(
            "MATCH (n:Item) WHERE n.id = $lookup RETURN n.rank AS rank".into(),
            &point_params,
        )
        .unwrap();
    db.checkpoint().unwrap();
    let runtime = db.runtime.get().unwrap();
    let (_, stale_ordered) = ordered_prepared.into_execution(&runtime.catalog, &runtime.store);
    assert!(
        stale_ordered.optimized.is_none(),
        "ordered capability gain must invalidate the old plan"
    );
    let (_, point) = point_prepared.into_execution(&runtime.catalog, &runtime.store);
    assert!(
        point.optimized.is_some(),
        "point legality does not depend on unused composite ordering"
    );
    drop(runtime);
    assert_native_plan(&mut db, "projection-published");
    for rank in [Value::Int(12), Value::Float(12.5), Value::Int(8)] {
        params.insert("rank".into(), rank.clone());
        let plan = db
            .explain_query_with_params(&next, &params)
            .unwrap()
            .physical_plan
            .explain(0);
        assert_eq!(
            plan.contains("IndexNodeCompositeOrderedSeek"),
            matches!(rank, Value::Int(_)),
            "{plan}"
        );
        let oracle = next.replace("ORDER BY rank", "ORDER BY coalesce(rank, 0)");
        assert_eq!(
            db.query_with_params(&next, &params).unwrap().rows,
            db.query_with_params(&oracle, &params).unwrap().rows
        );
    }
    drop(db);
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn ordered_composite_query_preserves_consumer_error_and_retry() {
    let mut db = Database::new();
    populate(&mut db, false, true);
    let (query, _, _) = queries(false, 7);
    let params = BTreeMap::from([("kind".into(), Value::String("note".into()))]);
    let mut snapshot = db.begin_read_transaction().unwrap();
    let expected = crate::HawDBError::Execution("ordered composite consumer refused".into());
    let mut calls = 0;
    let error = snapshot
        .query_request_streaming(
            crate::QueryRequest::new(&query).with_params(&params),
            |_| {
                calls += 1;
                Err(expected.clone())
            },
        )
        .unwrap_err();
    assert_eq!(calls, 1);
    assert_eq!(error, expected);
    let mut rows = Vec::new();
    let report = snapshot
        .query_request_streaming(
            crate::QueryRequest::new(&query).with_params(&params),
            |row| {
                rows.push(row);
                Ok(())
            },
        )
        .unwrap();
    assert_eq!(rows.len(), 7);
    assert_eq!(
        report
            .execution_profile
            .pipeline_memory_report
            .query_memory_completion_bytes,
        0
    );
    assert!(report
        .execution_profile
        .blocking_operator_memory_reports
        .is_empty());
}

/// Ordered composite pages on an out-of-core store include canonical nodes a
/// write to an unindexed property moved into the delta, like an unindexed
/// twin (#2040).
#[test]
fn ordered_composite_pages_include_materialized_base_nodes() {
    let config = DatabaseConfig {
        storage_residency_mode: hawdb_storage::config::StorageResidencyMode::OutOfCore,
        ..Default::default()
    };
    let indexed_path = unique_test_dir("ordered_composite_materialized_indexed");
    let twin_path = unique_test_dir("ordered_composite_materialized_twin");
    let mut pages = Vec::new();
    for (path, indexed) in [(&indexed_path, true), (&twin_path, false)] {
        let mut db = Database::open_with_config(path, config.clone()).unwrap();
        populate(&mut db, false, indexed);
        db.checkpoint().unwrap();
        // Moves canonical notes into the delta without touching a key.
        db.query("MATCH (n:Item) WHERE n.rank IN [3, 7, 20] SET n.extra = 1")
            .unwrap();
        if indexed {
            assert_native_plan(&mut db, "materialized");
        }
        let mut rows = Vec::new();
        for descending in [false, true] {
            let (first, _, _) = queries(descending, 100);
            rows.push(
                db.query_with_params(
                    &first,
                    &BTreeMap::from([("kind".into(), Value::String("note".into()))]),
                )
                .unwrap()
                .rows
                .into_rows()
                .into_iter()
                .map(|row| row.values().cloned().collect::<Vec<_>>())
                .collect::<Vec<_>>(),
            );
        }
        pages.push(rows);
        drop(db);
    }
    assert_eq!(pages[0][0].len(), 76);
    assert_eq!(pages[0], pages[1]);
    std::fs::remove_dir_all(indexed_path).unwrap();
    std::fs::remove_dir_all(twin_path).unwrap();
}
