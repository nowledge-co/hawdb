// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

//! Externally owned search projection kinds (#1814).

use super::*;
use crate::search::{SearchDocument, EXTERNALLY_OWNED_KINDS_MARKER};
use crate::IncrementalSearchMaintenanceOptions;
use std::collections::BTreeSet;
use std::path::{Path, PathBuf};

const MEMORY: &str = "memory:m1";
const SOURCE: &str = "source:s1";

fn owned() -> BTreeSet<SearchProjectionKind> {
    BTreeSet::from([SearchProjectionKind::Memory, SearchProjectionKind::Entity])
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum Layout {
    Resident,
    OutOfCore,
}

fn options(
    root: &Path,
    layout: Layout,
    kinds: BTreeSet<SearchProjectionKind>,
) -> EmbeddedStoreOpenOptions {
    let options = EmbeddedStoreOpenOptions::with_search_projection(
        root.join("graph"),
        root.join("search"),
        GraphMode::WritableCutover,
    )
    .with_externally_owned_search_kinds(kinds);
    match layout {
        Layout::Resident => options,
        Layout::OutOfCore => options
            .with_incremental_search_maintenance(IncrementalSearchMaintenanceOptions::default()),
    }
}

/// Opens a fresh store. An out-of-core projection is bootstrapped from an
/// empty checkpointed resident index, as hosts do before incremental mode.
fn open_fresh(
    name: &str,
    layout: Layout,
    kinds: BTreeSet<SearchProjectionKind>,
) -> (PathBuf, EmbeddedStoreHandle) {
    let root = unique_embedded_store_test_dir(name);
    if layout == Layout::OutOfCore {
        let mut index = SearchIndex::open(root.join("search")).unwrap();
        index
            .upsert_projection_row(SearchProjectionRow {
                kind: SearchProjectionKind::Message,
                external_id: "bootstrap".into(),
                title: "bootstrap".into(),
                body: "bootstrap".into(),
                embedding: None,
                source_id: None,
                metadata: BTreeMap::new(),
            })
            .unwrap();
        index.checkpoint().unwrap();
        drop(index);
    }
    let (handle, _) =
        EmbeddedStoreHandle::open_with_options(options(&root, layout, kinds)).unwrap();
    (root, handle)
}

fn catch_up(handle: &EmbeddedStoreHandle) {
    let report = handle
        .catch_up_search_projection_with_batch_hydrator(64, 64, 8, |_, _| {
            Ok(SearchProjectionRelationalDelta::default())
        })
        .unwrap();
    assert!(report.complete, "{report:?}");
}

fn document(handle: &EmbeddedStoreHandle, id: &str) -> Option<SearchDocument> {
    match handle.search_projection_documents(&[id.to_string()], 1) {
        Ok(mut documents) => {
            assert_eq!(documents.len(), 1);
            documents.pop()
        }
        Err(HawDBError::Semantic(message)) if message.contains("does not contain") => None,
        Err(error) => panic!("hydrating {id}: {error}"),
    }
}

fn external_memory() -> SearchProjectionDelta {
    SearchProjectionDelta {
        upserts: vec![SearchProjectionRow {
            kind: SearchProjectionKind::Memory,
            external_id: "m1".into(),
            title: "Host title".into(),
            body: "host supplied body".into(),
            embedding: Some(vec![0.25, 0.5, 0.75]),
            source_id: None,
            metadata: BTreeMap::from([
                ("space_id".to_string(), "default".to_string()),
                ("host_key".to_string(), "host value".to_string()),
            ]),
        }],
        max_operations: Some(1),
        ..Default::default()
    }
}

/// Creates the nodes, applies the host's document and retitles the node.
/// Returns the host document as read back before the graph-only update.
fn host_document_then_graph_update(handle: &EmbeddedStoreHandle) -> SearchDocument {
    handle
        .query_with_report(
            "CREATE (:Memory {id: 'm1', title: 'graph title', content: 'graph body', access_count: 1})",
        )
        .unwrap();
    handle
        .query_with_report("CREATE (:Source {id: 's1', title: 'source title'})")
        .unwrap();
    catch_up(handle);
    handle
        .apply_search_projection_delta_and_checkpoint(external_memory())
        .unwrap();
    let applied = document(handle, MEMORY).expect("host document is readable");
    assert_eq!(applied.embedding, Some(vec![0.25, 0.5, 0.75]));
    handle
        .query_with_report(
            "MATCH (m:Memory {id: 'm1'}) SET m.title = 'retitled', m.access_count = 2",
        )
        .unwrap();
    catch_up(handle);
    applied
}

fn assert_owned_lifecycle(layout: Layout) {
    let (root, handle) = open_fresh(&format!("owned_kinds_{layout:?}"), layout, owned());
    assert_eq!(
        handle.runtime_status().unwrap().json()["projection"]["externally_owned_kinds"],
        serde_json::json!(["memory", "entity"])
    );
    handle
        .query_with_report("CREATE (:Memory {id: 'early', title: 'no host row yet'})")
        .unwrap();
    catch_up(&handle);
    assert_eq!(
        document(&handle, "memory:early"),
        None,
        "the host is the only writer"
    );

    let applied = host_document_then_graph_update(&handle);
    assert_eq!(
        document(&handle, MEMORY),
        Some(applied.clone()),
        "graph-only update kept the host document"
    );
    let source = document(&handle, SOURCE).expect("other kinds stay graph-derived");
    assert_eq!(source.title, "source title");
    if layout == Layout::Resident {
        // The graph-only catch-up entry point honours the same policy.
        handle
            .query_with_report("MATCH (m:Memory {id: 'm1'}) SET m.title = 'again'")
            .unwrap();
        handle.catch_up_search_projection(64, 8).unwrap();
        assert_eq!(document(&handle, MEMORY), Some(applied.clone()));
    }
    drop(handle);

    // Reopen with the recorded set keeps the mode and the document.
    let (handle, _) =
        EmbeddedStoreHandle::open_with_options(options(&root, layout, owned())).unwrap();
    assert_eq!(
        handle
            .runtime_status()
            .unwrap()
            .externally_owned_search_kinds,
        vec![SearchProjectionKind::Memory, SearchProjectionKind::Entity]
    );
    handle
        .query_with_report("MATCH (m:Memory {id: 'm1'}) SET m.access_count = 3")
        .unwrap();
    catch_up(&handle);
    assert_eq!(document(&handle, MEMORY), Some(applied));

    // Deleting the node removes the host document.
    handle
        .query_with_report("MATCH (m:Memory {id: 'm1'}) DETACH DELETE m")
        .unwrap();
    catch_up(&handle);
    assert_eq!(document(&handle, MEMORY), None);
    assert!(document(&handle, SOURCE).is_some());
    drop(handle);
    std::fs::remove_dir_all(root).unwrap();
}

#[test]
fn resident_owned_kinds_keep_host_documents_until_node_delete() {
    assert_owned_lifecycle(Layout::Resident);
}

#[test]
fn out_of_core_owned_kinds_keep_host_documents_until_node_delete() {
    assert_owned_lifecycle(Layout::OutOfCore);
}

#[test]
fn default_open_keeps_graph_derived_replacement() {
    for layout in [Layout::Resident, Layout::OutOfCore] {
        let (root, handle) = open_fresh(
            &format!("default_kinds_{layout:?}"),
            layout,
            BTreeSet::new(),
        );
        host_document_then_graph_update(&handle);
        let replaced = document(&handle, MEMORY).expect("graph-derived document");
        assert_eq!(replaced.title, "retitled");
        assert_eq!(replaced.embedding, None);
        assert!(!root
            .join("search")
            .join(EXTERNALLY_OWNED_KINDS_MARKER)
            .exists());
        assert!(handle
            .runtime_status()
            .unwrap()
            .externally_owned_search_kinds
            .is_empty());
        drop(handle);
        std::fs::remove_dir_all(root).unwrap();
    }
}

#[test]
fn reopen_refuses_a_different_set_and_restores_a_missing_record() {
    for layout in [Layout::Resident, Layout::OutOfCore] {
        let (root, handle) = open_fresh(&format!("owned_kinds_reopen_{layout:?}"), layout, owned());
        drop(handle);
        let record = root.join("search").join(EXTERNALLY_OWNED_KINDS_MARKER);
        let recorded = std::fs::read(&record).unwrap();
        for requested in [
            BTreeSet::new(),
            BTreeSet::from([SearchProjectionKind::Memory]),
        ] {
            let Err(error) =
                EmbeddedStoreHandle::open_with_options(options(&root, layout, requested.clone()))
            else {
                panic!("a different set is refused");
            };
            assert_eq!(
                error,
                HawDBError::SearchProjectionOwnershipMismatch {
                    projection: root.join("search"),
                    recorded: vec!["memory".into(), "entity".into()],
                    requested: requested
                        .iter()
                        .map(|kind| kind.as_str().to_string())
                        .collect(),
                },
                "{layout:?}"
            );
            assert_eq!(
                std::fs::read(&record).unwrap(),
                recorded,
                "a refusal changes nothing"
            );
        }
        // An older binary or operator removed the record: the next open restores it.
        std::fs::remove_file(&record).unwrap();
        let (handle, _) =
            EmbeddedStoreHandle::open_with_options(options(&root, layout, owned())).unwrap();
        assert_eq!(std::fs::read(&record).unwrap(), recorded);
        drop(handle);
        std::fs::remove_dir_all(root).unwrap();
    }
}

#[test]
fn owned_kinds_require_a_search_projection() {
    let root = unique_embedded_store_test_dir("owned_kinds_graph_only");
    let Err(error) = EmbeddedStoreHandle::open_with_options(
        EmbeddedStoreOpenOptions::graph_only(root.join("graph"), GraphMode::WritableCutover)
            .with_externally_owned_search_kinds(owned()),
    ) else {
        panic!("graph-only open refuses owned kinds");
    };
    assert!(matches!(error, HawDBError::Semantic(_)), "{error}");
    assert!(!root.exists(), "refused before opening the graph");
}

/// Every kind the changefeed derives from graph labels can be host-owned:
/// graph-only updates keep each host document and node deletes remove it.
#[test]
fn every_projection_kind_can_be_externally_owned() {
    let kinds = [
        (SearchProjectionKind::Memory, "Memory"),
        (SearchProjectionKind::Message, "Message"),
        (SearchProjectionKind::Entity, "Entity"),
        (SearchProjectionKind::Source, "Source"),
        (SearchProjectionKind::SourceChunk, "SourceChunk"),
        (SearchProjectionKind::Community, "Community"),
    ];
    let all = kinds.iter().map(|(kind, _)| *kind).collect::<BTreeSet<_>>();
    for layout in [Layout::Resident, Layout::OutOfCore] {
        let (root, handle) =
            open_fresh(&format!("owned_all_kinds_{layout:?}"), layout, all.clone());
        for (_, label) in kinds {
            handle
                .query_with_report(&format!("CREATE (:{label} {{id: 'n1', title: 'graph'}})"))
                .unwrap();
        }
        catch_up(&handle);
        let upserts = kinds
            .iter()
            .map(|(kind, _)| SearchProjectionRow {
                kind: *kind,
                external_id: "n1".into(),
                title: format!("host {}", kind.as_str()),
                body: "host body".into(),
                embedding: Some(vec![0.6, 0.8]),
                source_id: None,
                metadata: BTreeMap::from([("space_id".to_string(), "default".to_string())]),
            })
            .collect::<Vec<_>>();
        handle
            .apply_search_projection_delta_and_checkpoint(SearchProjectionDelta {
                max_operations: Some(upserts.len()),
                upserts,
                ..Default::default()
            })
            .unwrap();
        let ids = kinds
            .iter()
            .map(|(kind, _)| format!("{}:n1", kind.as_str()))
            .collect::<Vec<_>>();
        let applied = ids
            .iter()
            .map(|id| document(&handle, id).expect("host document"))
            .collect::<Vec<_>>();
        for (_, label) in kinds {
            handle
                .query_with_report(&format!(
                    "MATCH (n:{label} {{id: 'n1'}}) SET n.title = 'retitled'"
                ))
                .unwrap();
        }
        catch_up(&handle);
        for (id, applied) in ids.iter().zip(&applied) {
            assert_eq!(
                document(&handle, id).as_ref(),
                Some(applied),
                "{layout:?} {id}"
            );
        }
        for (_, label) in kinds {
            handle
                .query_with_report(&format!("MATCH (n:{label} {{id: 'n1'}}) DETACH DELETE n"))
                .unwrap();
        }
        catch_up(&handle);
        for id in &ids {
            assert_eq!(document(&handle, id), None, "{layout:?} {id}");
        }
        drop(handle);
        std::fs::remove_dir_all(root).unwrap();
    }
}
