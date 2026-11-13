// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

//! A schema-only commit (property DEFAULT declaration) must not open a search
//! projection change-log gap for incremental maintenance (#1745).

use super::*;

const DECLARE: &str = "CREATE PROPERTY ON NODE TABLE Entity(pagerank_score) TYPE ANY DEFAULT 0.0";

fn status(handle: &EmbeddedStoreHandle) -> (u64, Option<u64>, Option<u64>) {
    let fresh = handle
        .runtime_status()
        .unwrap()
        .projection_freshness
        .unwrap();
    (
        handle
            .read_store()
            .unwrap()
            .graph
            .database()
            .commit_epoch()
            .unwrap(),
        fresh.source_graph_commit_epoch,
        fresh.durable_source_graph_commit_epoch,
    )
}

fn catch_up(
    handle: &EmbeddedStoreHandle,
) -> crate::Result<crate::search::SearchProjectionCatchUpReport> {
    handle.catch_up_search_projection_with_batch_hydrator(16, 16, 4, |_, _| {
        Ok(SearchProjectionRelationalDelta::default())
    })
}

#[test]
fn schema_only_default_declaration_keeps_incremental_catch_up_live() {
    let (root, handle, options) =
        super::incremental_maintenance_fixture("schema_gap_live", Default::default());
    handle
        .query_with_report("CREATE (:Memory {id: 'm1', title: 'Before'})")
        .unwrap();
    catch_up(&handle).unwrap();
    handle.query_with_report(DECLARE).unwrap();
    catch_up(&handle).unwrap();
    let (graph, _, durable) = status(&handle);
    assert_eq!(
        durable,
        Some(graph),
        "projection behind graph after declare"
    );
    handle
        .query_with_report("CREATE (:Memory {id: 'm2', title: 'After'})")
        .unwrap();
    catch_up(&handle).unwrap();
    drop(handle);

    let (handle, _) = EmbeddedStoreHandle::open_with_options(options).unwrap();
    handle
        .query_with_report("CREATE (:Memory {id: 'm3', title: 'Reopened'})")
        .unwrap();
    catch_up(&handle).unwrap();
    let (graph, _, durable) = status(&handle);
    assert_eq!(durable, Some(graph));
    drop(handle);
    std::fs::remove_dir_all(root).unwrap();
}

#[test]
fn declaration_before_initial_import_keeps_incremental_reopen_live() {
    let root = unique_embedded_store_test_dir("schema_gap_initial_import");
    let graph_path = root.join("graph");
    let search_path = root.join("search");
    let base = EmbeddedStoreOpenOptions::with_search_projection(
        graph_path,
        search_path,
        GraphMode::WritableCutover,
    );
    {
        let (handle, _) = EmbeddedStoreHandle::open_with_options(base.clone()).unwrap();
        handle.query_with_report(DECLARE).unwrap();
        handle
            .query_with_report("CREATE (:Memory {id: 'm1', title: 'Imported'})")
            .unwrap();
        let row = SearchProjectionRow {
            kind: SearchProjectionKind::Memory,
            external_id: "m1".into(),
            title: "Imported".into(),
            body: "legacy import".into(),
            embedding: None,
            source_id: None,
            metadata: BTreeMap::new(),
        };
        let delta = |upserts| SearchProjectionDelta {
            upserts,
            deletes: Vec::new(),
            max_operations: Some(1),
            source_graph_commit_epoch: None,
        };
        handle
            .apply_initial_import_projection_delta(delta(vec![row]), 7)
            .unwrap();
        handle.checkpoint_initial_import_projection().unwrap();
        handle
            .apply_initial_import_projection_delta(delta(Vec::new()), 7)
            .unwrap();
        handle.checkpoint_initial_import_projection().unwrap();
    }
    let options = base.with_incremental_search_maintenance(Default::default());
    let (handle, _) = EmbeddedStoreHandle::open_with_options(options).unwrap();
    catch_up(&handle).unwrap();
    handle
        .query_with_report("CREATE (:Memory {id: 'm2', title: 'Later'})")
        .unwrap();
    catch_up(&handle).unwrap();
    let (graph, _, durable) = status(&handle);
    assert_eq!(durable, Some(graph));
    drop(handle);
    std::fs::remove_dir_all(root).unwrap();
}

#[test]
fn default_backfill_on_existing_records_is_captured_as_record_changes() {
    let (root, handle, _options) =
        super::incremental_maintenance_fixture("schema_gap_backfill", Default::default());
    handle
        .query_with_report("CREATE (:Entity {id: 'e1', name: 'Existing'})")
        .unwrap();
    catch_up(&handle).unwrap();
    handle.query_with_report(DECLARE).unwrap();
    catch_up(&handle).unwrap();
    let before = status(&handle).0;
    for _ in 0..8 {
        handle.run_bounded_schema_maintenance(1024).unwrap();
    }
    let after = status(&handle).0;
    assert!(after > before, "backfill committed nothing");
    let batch = handle
        .read_store()
        .unwrap()
        .graph
        .database()
        .build_search_projection_change_batch_after(before, None)
        .unwrap()
        .unwrap();
    assert!(batch.operation_count() >= 1, "backfill fill not captured");
    let rows = handle
        .query_with_report("MATCH (e:Entity {id: 'e1'}) RETURN e.pagerank_score AS s")
        .unwrap();
    assert_eq!(rows.output.rows[0].get("s"), Some(&Value::Float(0.0)));
    catch_up(&handle).unwrap();
    let (graph, _, durable) = status(&handle);
    assert_eq!(durable, Some(graph));
    drop(handle);
    std::fs::remove_dir_all(root).unwrap();
}
