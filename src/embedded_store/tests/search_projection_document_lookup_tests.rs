// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

//! Projection document lookup by id on the resident and out-of-core paths
//! (#1786), and hydration by id of a deleted document (#1767).
//!
//! Documents are written and deleted through the graph and caught up, so
//! the out-of-core projection publishes the deletion through incremental
//! maintenance, as in production. The reader-level tests in `hawdb-search`
//! cover a deletion hidden by mutation-run visibility directly.

use super::*;
use crate::{
    EmbeddedReadSnapshotBudget, IncrementalSearchMaintenanceOptions, ResourceBudgetKind,
    SearchProjectionDocumentLookup,
};

const KEPT: &str = "memory:kept";
const GONE: &str = "memory:gone";
const NEVER: &str = "memory:never";

fn ids(ids: &[&str]) -> Vec<String> {
    ids.iter().map(|id| (*id).to_string()).collect()
}

/// A graph-maintained projection: resident, or out-of-core under
/// incremental maintenance.
fn maintained_handle(out_of_core: bool) -> (std::path::PathBuf, EmbeddedStoreHandle) {
    let (root, handle) = if out_of_core {
        let (root, handle, _) = super::incremental_maintenance_fixture(
            "document_lookup_out_of_core",
            IncrementalSearchMaintenanceOptions::default(),
        );
        (root, handle)
    } else {
        let root = unique_embedded_store_test_dir("document_lookup_resident");
        let (handle, _) = EmbeddedStoreHandle::open_with_options(
            EmbeddedStoreOpenOptions::with_search_projection(
                root.join("graph"),
                root.join("search"),
                GraphMode::WritableCutover,
            ),
        )
        .unwrap();
        (root, handle)
    };
    for id in ["kept", "gone"] {
        handle
            .query_with_report(&format!(
                "CREATE (:Memory {{id: '{id}', title: 'memory {id}', content: 'lookup body {id}', space_id: 'default'}})"
            ))
            .unwrap();
    }
    catch_up(&handle);
    (root, handle)
}

fn catch_up(handle: &EmbeddedStoreHandle) {
    handle
        .catch_up_search_projection_with_batch_hydrator(64, 64, 64, |_, _| {
            Ok(SearchProjectionRelationalDelta::default())
        })
        .unwrap();
}

fn lookup(
    handle: &EmbeddedStoreHandle,
    budget: EmbeddedReadSnapshotBudget,
    request: &[String],
) -> crate::Result<SearchProjectionDocumentLookup> {
    handle.with_bounded_read_snapshot(budget, |snapshot| {
        snapshot.lookup_search_projection_documents(request)
    })
}

const BUDGET: EmbeddedReadSnapshotBudget = EmbeddedReadSnapshotBudget {
    max_rows: 16,
    max_payload_bytes: 1 << 20,
};

fn found_ids(lookup: &SearchProjectionDocumentLookup) -> Vec<&str> {
    lookup
        .documents
        .iter()
        .map(|document| document.id.as_str())
        .collect()
}

#[test]
fn lookup_reports_present_deleted_and_never_published_ids_on_both_paths() {
    for out_of_core in [false, true] {
        let (root, handle) = maintained_handle(out_of_core);
        let before = lookup(&handle, BUDGET, &ids(&[GONE, NEVER, KEPT])).unwrap();
        assert_eq!(
            found_ids(&before),
            [GONE, KEPT],
            "out_of_core={out_of_core}"
        );
        assert_eq!(before.missing_document_ids, [NEVER]);
        assert_eq!(before.out_of_core_metrics.is_some(), out_of_core);

        handle
            .query_with_report("MATCH (m:Memory {id: 'gone'}) DETACH DELETE m")
            .unwrap();
        catch_up(&handle);
        // A deleted id and a never-published id are both absent; request
        // order holds on each side of the result.
        let after = lookup(&handle, BUDGET, &ids(&[GONE, NEVER, KEPT])).unwrap();
        assert_eq!(found_ids(&after), [KEPT], "out_of_core={out_of_core}");
        assert_eq!(after.documents[0], before.documents[1]);
        assert_eq!(after.missing_document_ids, [GONE, NEVER]);
        let none = lookup(&handle, BUDGET, &ids(&[NEVER, GONE])).unwrap();
        assert!(none.documents.is_empty());
        assert_eq!(none.missing_document_ids, [NEVER, GONE]);
        let empty = lookup(&handle, BUDGET, &[]).unwrap();
        assert!(empty.documents.is_empty() && empty.missing_document_ids.is_empty());

        // Hydration by id still refuses the deleted id as a caller mistake
        // on both paths (#1767), alone and beside a present id.
        for request in [ids(&[GONE]), ids(&[KEPT, GONE])] {
            let error = handle.search_projection_documents(&request, 2).unwrap_err();
            assert!(
                matches!(error, HawDBError::Semantic(_)),
                "out_of_core={out_of_core}: {error:?}"
            );
        }

        // A repeated id is malformed, present or not.
        for id in [KEPT, GONE, NEVER] {
            let error = lookup(&handle, BUDGET, &ids(&[id, id])).unwrap_err();
            assert!(
                matches!(error, HawDBError::Semantic(_)),
                "out_of_core={out_of_core}: {error:?}"
            );
        }
        drop(handle);
        std::fs::remove_dir_all(root).unwrap();
    }
}

#[test]
fn lookup_is_charged_to_the_snapshot_row_and_payload_budgets_on_both_paths() {
    for out_of_core in [false, true] {
        let (root, handle) = maintained_handle(out_of_core);
        let assert_rows_refusal = |error: HawDBError| match error {
            HawDBError::ReadBudgetExceeded(details) => {
                assert_eq!(details.resource, ReadBudgetResource::Rows, "{details:?}");
                assert_eq!(details.limit, 2, "{details:?}");
            }
            other => panic!("out_of_core={out_of_core}: {other:?}"),
        };
        let budget = EmbeddedReadSnapshotBudget {
            max_rows: 2,
            ..BUDGET
        };
        // Every requested id could be present, so the request must fit the
        // remaining rows before any read, even when most ids are absent.
        assert_rows_refusal(lookup(&handle, budget, &ids(&[NEVER, GONE, KEPT])).unwrap_err());
        // Found documents are charged cumulatively; absent ids are not.
        let result = handle.with_bounded_read_snapshot(budget, |snapshot| {
            let first = snapshot.lookup_search_projection_documents(&ids(&[KEPT, NEVER]))?;
            assert_eq!(first.documents.len(), 1);
            let second = snapshot.lookup_search_projection_documents(&ids(&[GONE]))?;
            assert_eq!(second.documents.len(), 1);
            assert!(snapshot
                .lookup_search_projection_documents(&ids(&[NEVER]))
                .is_err());
            Ok(())
        });
        result.unwrap();
        assert_rows_refusal(
            handle
                .with_bounded_read_snapshot(budget, |snapshot| {
                    snapshot.lookup_search_projection_documents(&ids(&[KEPT]))?;
                    snapshot.lookup_search_projection_documents(&ids(&[GONE, NEVER]))
                })
                .unwrap_err(),
        );
        // The payload of found documents is charged to the snapshot too.
        let error = lookup(
            &handle,
            EmbeddedReadSnapshotBudget {
                max_payload_bytes: 8,
                ..BUDGET
            },
            &ids(&[NEVER, KEPT]),
        )
        .unwrap_err();
        assert!(
            matches!(
                &error,
                HawDBError::ReadBudgetExceeded(details)
                    if details.resource == ReadBudgetResource::PayloadBytes
            ),
            "out_of_core={out_of_core}: {error:?}"
        );
        drop(handle);
        std::fs::remove_dir_all(root).unwrap();
    }
}

#[test]
fn out_of_core_lookup_count_refusal_names_max_hydrated_documents() {
    let root = unique_embedded_store_test_dir("document_lookup_count_budget");
    let mut index = SearchIndex::open(&root).unwrap();
    for id in ["a", "b"] {
        index
            .upsert(crate::SearchDocument {
                id: format!("memory:{id}"),
                title: id.to_string(),
                content: id.to_string(),
                embedding: None,
                metadata: BTreeMap::new(),
            })
            .unwrap();
    }
    index.checkpoint().unwrap();
    drop(index);
    let handle = EmbeddedStoreHandle::new(EmbeddedStore::new_with_out_of_core_search(
        EmbeddedGraph::from_database(Database::new(), GraphMode::ShadowReadOnly),
        OutOfCoreSearchProjection::open_with_config(
            &root,
            SearchOutOfCoreConfig {
                max_hydrated_documents: std::num::NonZeroUsize::new(2).unwrap(),
                ..SearchOutOfCoreConfig::default()
            },
        )
        .unwrap(),
        RetrievalProjectionAdvisor::default(),
    ))
    .unwrap();
    // The reader cap counts requested ids, present or not.
    match lookup(&handle, BUDGET, &ids(&["memory:a", NEVER, "memory:b"])).unwrap_err() {
        HawDBError::ResourceBudgetExceeded(details) => {
            assert_eq!(details.resource, ResourceBudgetKind::MetadataEntries);
            assert_eq!(details.owner, "search lookup by id", "{details:?}");
            assert_eq!(details.requested, 3, "{details:?}");
            assert_eq!(details.budget, 2, "{details:?}");
            assert_eq!(
                details.budget_option, "SearchOutOfCoreConfig.max_hydrated_documents",
                "{details:?}"
            );
        }
        other => panic!("{other:?}"),
    }
    let within = lookup(&handle, BUDGET, &ids(&["memory:b", NEVER])).unwrap();
    assert_eq!(found_ids(&within), ["memory:b"]);
    assert_eq!(within.missing_document_ids, [NEVER]);
    drop(handle);
    std::fs::remove_dir_all(root).unwrap();
}
