// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

//! Host-requested search projection staleness markers (#1809): marked through
//! the handle, visible in `runtime_status`, durable across a reopen,
//! idempotent, and refused on read-only handles and for invalid reasons.

use super::*;
use crate::{
    GraphOnlySearchProjectionRebuild, IncrementalSearchMaintenanceOptions, RuntimeTaskContext,
    SearchProjectionRebuildOptions, MAX_SEARCH_PROJECTION_MARKER_REASON_BYTES,
};

fn freshness(handle: &EmbeddedStoreHandle) -> SearchProjectionFreshness {
    handle
        .runtime_status()
        .unwrap()
        .projection_freshness
        .unwrap()
}

fn assert_unmarked(handle: &EmbeddedStoreHandle) {
    let freshness = freshness(handle);
    assert!(!freshness.full_reindex_needed, "{freshness:?}");
    assert!(!freshness.metadata_repair_needed, "{freshness:?}");
}

/// Marks both flags twice and checks the reported reasons.
fn mark_twice(handle: &EmbeddedStoreHandle) {
    for _ in 0..2 {
        let marked = handle
            .mark_search_projection_full_reindex_needed("vector store rebuilt")
            .unwrap();
        assert!(marked.full_reindex_needed);
        assert_eq!(marked.full_reindex_reasons, vec!["vector store rebuilt"]);
    }
    let marked = handle
        .mark_search_projection_full_reindex_needed("embedding model changed")
        .unwrap();
    assert_eq!(
        marked.full_reindex_reasons,
        vec!["vector store rebuilt", "embedding model changed"]
    );
    for reason in ["first repair", "latest repair", "latest repair"] {
        let marked = handle
            .mark_search_projection_metadata_repair_needed(reason)
            .unwrap();
        assert!(marked.metadata_repair_needed);
        assert_eq!(marked.metadata_repair_reasons, vec![reason]);
    }
    assert_marked(handle);
}

fn assert_marked(handle: &EmbeddedStoreHandle) {
    let freshness = freshness(handle);
    assert_eq!(
        freshness.full_reindex_reasons,
        vec!["vector store rebuilt", "embedding model changed"]
    );
    assert_eq!(freshness.metadata_repair_reasons, vec!["latest repair"]);
}

fn assert_invalid_reasons_refused(handle: &EmbeddedStoreHandle) {
    let long = "x".repeat(MAX_SEARCH_PROJECTION_MARKER_REASON_BYTES + 1);
    for reason in [
        "",
        "  ",
        "two\nlines",
        "carriage\rreturn",
        " padded",
        long.as_str(),
    ] {
        for error in [
            handle
                .mark_search_projection_full_reindex_needed(reason)
                .unwrap_err(),
            handle
                .mark_search_projection_metadata_repair_needed(reason)
                .unwrap_err(),
        ] {
            assert!(
                matches!(error, HawDBError::Semantic(_)),
                "{reason:?}: {error:?}"
            );
        }
    }
}

#[test]
fn resident_projection_markers_are_durable_idempotent_and_validated() {
    let root = unique_embedded_store_test_dir("search_markers_resident");
    let options = EmbeddedStoreOpenOptions::with_search_projection(
        root.join("graph"),
        root.join("search"),
        GraphMode::WritableCutover,
    );
    {
        let (handle, _) = EmbeddedStoreHandle::open_with_options(options.clone()).unwrap();
        assert_unmarked(&handle);
        assert_invalid_reasons_refused(&handle);
        assert_unmarked(&handle);
        mark_twice(&handle);
    }
    {
        let (handle, _) = EmbeddedStoreHandle::open_with_options(options.clone()).unwrap();
        assert_marked(&handle);
    }
    {
        // A read-only handle reports the markers and refuses to write them.
        let read_only = EmbeddedStoreOpenOptions::with_search_projection(
            root.join("graph"),
            root.join("search"),
            GraphMode::ShadowReadOnly,
        );
        let (handle, _) = EmbeddedStoreHandle::open_with_options(read_only).unwrap();
        assert_marked(&handle);
        for error in [
            handle
                .mark_search_projection_full_reindex_needed("another")
                .unwrap_err(),
            handle
                .mark_search_projection_metadata_repair_needed("another")
                .unwrap_err(),
        ] {
            assert!(
                matches!(&error, HawDBError::ReadOnly { operation } if operation.starts_with("mark_search_projection_")),
                "{error:?}"
            );
            assert!(error.to_string().contains("read-only"), "{error}");
        }
        assert_marked(&handle);
    }
    std::fs::remove_dir_all(root).unwrap();
}

#[test]
fn incremental_projection_markers_are_durable_idempotent_and_cleared_by_rebuild() {
    let root = unique_embedded_store_test_dir("search_markers_incremental");
    let graph = root.join("graph");
    let search = root.join("projection");
    {
        let options = EmbeddedStoreOpenOptions::graph_only(&graph, GraphMode::WritableCutover);
        let (handle, _) = EmbeddedStoreHandle::open_with_options(options).unwrap();
        handle
            .query_with_report(
                "CREATE (:Memory {id: 'm1', title: 'marker memory', content: 'marker body', space_id: 'default'})",
            )
            .unwrap();
        handle
            .rebuild_out_of_core_search_projection(
                &search,
                SearchProjectionRebuildOptions::default(),
                &mut GraphOnlySearchProjectionRebuild,
                RuntimeTaskContext::default(),
            )
            .unwrap();
    }
    let options = EmbeddedStoreOpenOptions::with_search_projection(
        &graph,
        &search,
        GraphMode::WritableCutover,
    )
    .with_incremental_search_maintenance(IncrementalSearchMaintenanceOptions::default());
    {
        let (handle, _) = EmbeddedStoreHandle::open_with_options(options.clone()).unwrap();
        assert_unmarked(&handle);
        assert_invalid_reasons_refused(&handle);
        mark_twice(&handle);
        // Graph writes keep working while the projection is marked.
        handle
            .query_with_report("CREATE (:ImportState {id: 'cursor'})")
            .unwrap();
    }
    {
        let (handle, _) = EmbeddedStoreHandle::open_with_options(options.clone()).unwrap();
        assert_marked(&handle);
    }
    {
        // A full rebuild publishes a projection without the markers.
        let rebuilt = root.join("rebuilt");
        let (handle, _) = EmbeddedStoreHandle::open_with_options(
            EmbeddedStoreOpenOptions::graph_only(&graph, GraphMode::WritableCutover),
        )
        .unwrap();
        handle
            .rebuild_out_of_core_search_projection(
                &rebuilt,
                SearchProjectionRebuildOptions::default(),
                &mut GraphOnlySearchProjectionRebuild,
                RuntimeTaskContext::default(),
            )
            .unwrap();
        drop(handle);
        let (handle, _) = EmbeddedStoreHandle::open_with_options(
            EmbeddedStoreOpenOptions::with_search_projection(
                &graph,
                &rebuilt,
                GraphMode::WritableCutover,
            )
            .with_incremental_search_maintenance(IncrementalSearchMaintenanceOptions::default()),
        )
        .unwrap();
        assert_unmarked(&handle);
    }
    std::fs::remove_dir_all(root).unwrap();
}

#[test]
fn a_handle_without_a_search_projection_refuses_markers() {
    let root = unique_embedded_store_test_dir("search_markers_graph_only");
    let (handle, _) = EmbeddedStoreHandle::open_with_options(EmbeddedStoreOpenOptions::graph_only(
        root.join("graph"),
        GraphMode::WritableCutover,
    ))
    .unwrap();
    let error = handle
        .mark_search_projection_full_reindex_needed("vector store rebuilt")
        .unwrap_err();
    assert!(matches!(error, HawDBError::Semantic(_)), "{error:?}");
    drop(handle);
    std::fs::remove_dir_all(root).unwrap();
}
