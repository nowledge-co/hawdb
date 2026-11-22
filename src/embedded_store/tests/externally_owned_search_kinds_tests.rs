// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

//! Externally owned search projection kinds (#1814).

use super::*;
use crate::search::{
    MetadataRepairOptions, SearchDocument, SearchRebuildOptions, EXTERNALLY_OWNED_KINDS_MARKER,
    EXTERNALLY_OWNED_ROWS_MARKER,
};
use crate::{
    ExternallyOwnedRowsToken, GraphOnlySearchProjectionRebuild,
    IncrementalSearchMaintenanceOptions, RuntimeTaskContext, RuntimeWorkPriority,
    SearchProjectionRebuildOptions, SearchProjectionRebuildPhase, SearchProjectionRebuildProgress,
    SearchProjectionRebuildSource,
};
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
    options_at(
        root,
        "search",
        layout,
        kinds,
        GraphMode::WritableCutover,
        None,
    )
}

fn options_at(
    root: &Path,
    search: &str,
    layout: Layout,
    kinds: BTreeSet<SearchProjectionKind>,
    mode: GraphMode,
    config: Option<&DatabaseConfig>,
) -> EmbeddedStoreOpenOptions {
    let mut options = EmbeddedStoreOpenOptions::with_search_projection(
        root.join("graph"),
        root.join(search),
        mode,
    )
    .with_externally_owned_search_kinds(kinds);
    if let Some(config) = config {
        options = options.with_database_config(config.clone());
    }
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
    open_fresh_with(name, layout, kinds, None)
}

fn open_fresh_with(
    name: &str,
    layout: Layout,
    kinds: BTreeSet<SearchProjectionKind>,
    config: Option<&DatabaseConfig>,
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
    let (handle, _) = EmbeddedStoreHandle::open_with_options(options_at(
        &root,
        "search",
        layout,
        kinds,
        GraphMode::WritableCutover,
        config,
    ))
    .unwrap();
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

fn required_token(handle: &EmbeddedStoreHandle) -> ExternallyOwnedRowsToken {
    handle
        .externally_owned_rows_required()
        .unwrap()
        .expect("externally owned rows are required")
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
    let status = handle.runtime_status().unwrap().json();
    assert_eq!(
        status["projection"]["externally_owned_kinds"],
        serde_json::json!(["memory", "entity"])
    );
    // Newly recorded ownership: nothing proves the host's rows are present.
    let required = required_token(&handle);
    assert_eq!(
        status["projection"]["externally_owned_rows_required"]["epoch"],
        serde_json::json!(1)
    );
    assert_eq!(
        status["projection"]["externally_owned_rows_required"]["incarnation"],
        serde_json::json!(required.incarnation.to_string())
    );
    assert!(handle
        .acknowledge_externally_owned_rows_restored(required)
        .unwrap());
    assert_eq!(handle.externally_owned_rows_required().unwrap(), None);
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
        for marker in [EXTERNALLY_OWNED_KINDS_MARKER, EXTERNALLY_OWNED_ROWS_MARKER] {
            assert!(!root.join("search").join(marker).exists());
        }
        assert_eq!(handle.externally_owned_rows_required().unwrap(), None);
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
        let first = required_token(&handle);
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
        // The host observed epoch 1 before the record was restored, which
        // raised epoch 2 in the same incarnation: its stale token must not
        // clear the new requirement.
        let second = required_token(&handle);
        assert_eq!(second.incarnation, first.incarnation);
        assert_eq!(second.epoch, 2);
        assert!(!handle
            .acknowledge_externally_owned_rows_restored(first)
            .unwrap());
        assert_eq!(required_token(&handle), second);
        drop(handle);

        // Read-only openers see the epoch and cannot acknowledge it.
        let (reader, _) = EmbeddedStoreHandle::open_with_options(options_at(
            &root,
            "search",
            layout,
            owned(),
            GraphMode::ShadowReadOnly,
            None,
        ))
        .unwrap();
        assert_eq!(
            reader
                .runtime_status()
                .unwrap()
                .externally_owned_rows_required,
            Some(second)
        );
        assert!(matches!(
            reader.acknowledge_externally_owned_rows_restored(second),
            Err(HawDBError::Semantic(_))
        ));
        drop(reader);

        let (handle, _) =
            EmbeddedStoreHandle::open_with_options(options(&root, layout, owned())).unwrap();
        assert!(handle
            .acknowledge_externally_owned_rows_restored(second)
            .unwrap());
        assert!(!handle
            .acknowledge_externally_owned_rows_restored(second)
            .unwrap());
        assert_eq!(
            handle
                .runtime_status()
                .unwrap()
                .externally_owned_rows_required,
            None
        );
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

fn gap_config() -> DatabaseConfig {
    DatabaseConfig {
        max_search_projection_change_log_entries: Some(2),
        ..DatabaseConfig::default()
    }
}

fn host_row(kind: SearchProjectionKind, id: &str) -> SearchProjectionRow {
    SearchProjectionRow {
        kind,
        external_id: id.into(),
        title: format!("host {id}"),
        body: format!("host body {id}"),
        embedding: Some(vec![0.6, 0.8]),
        source_id: Some(format!("thread-{id}")),
        metadata: BTreeMap::from([
            ("space_id".to_string(), "default".to_string()),
            ("host_key".to_string(), id.to_string()),
        ]),
    }
}

/// Host rows for `kept` and `gone`, then more graph commits than the change
/// log retains: `kept` is retitled, `gone` deleted and `late` created. Returns
/// the host documents before the gap.
fn manufacture_gap(handle: &EmbeddedStoreHandle) -> (SearchDocument, SearchDocument) {
    for statement in [
        "CREATE (:Memory {id: 'kept', title: 'graph kept'})",
        "CREATE (:Memory {id: 'gone', title: 'graph gone'})",
    ] {
        handle.query_with_report(statement).unwrap();
        catch_up(handle);
    }
    handle
        .apply_search_projection_delta_and_checkpoint(SearchProjectionDelta {
            upserts: vec![
                host_row(SearchProjectionKind::Memory, "kept"),
                host_row(SearchProjectionKind::Memory, "gone"),
            ],
            max_operations: Some(2),
            ..Default::default()
        })
        .unwrap();
    let kept = document(handle, "memory:kept").unwrap();
    let gone = document(handle, "memory:gone").unwrap();
    for statement in [
        "MATCH (m:Memory {id: 'kept'}) SET m.title = 'retitled', m.access_count = 9",
        "MATCH (m:Memory {id: 'gone'}) DETACH DELETE m",
        "CREATE (:Memory {id: 'late', title: 'no host row'})",
        "CREATE (:Source {id: 's1', title: 'graph source'})",
        "CREATE (:Label {name: 'bookkeeping'})",
    ] {
        handle.query_with_report(statement).unwrap();
    }
    let error = handle
        .catch_up_search_projection_with_batch_hydrator(64, 64, 8, |_, _| {
            Ok(SearchProjectionRelationalDelta::default())
        })
        .unwrap_err();
    assert!(error.to_string().contains("rebuild"), "{error}");
    (kept, gone)
}

/// The resident recovery for a changefeed gap is a full rebuild from the
/// graph; owned documents of live nodes are carried over unchanged.
#[test]
fn resident_rebuild_after_changefeed_gap_carries_host_documents_over() {
    let config = gap_config();
    let owned = BTreeSet::from([SearchProjectionKind::Memory]);
    let (root, handle) =
        open_fresh_with("owned_gap_resident", Layout::Resident, owned, Some(&config));
    let initial = required_token(&handle);
    assert!(handle
        .acknowledge_externally_owned_rows_restored(initial)
        .unwrap());
    let (kept, gone) = manufacture_gap(&handle);
    {
        let mut store = handle.write_store().unwrap();
        let EmbeddedStore {
            graph,
            search_projection,
            ..
        } = &mut *store;
        let index = search_projection.as_mut().unwrap().index_mut();
        graph
            .database()
            .rebuild_search_projection(index, SearchRebuildOptions::default())
            .unwrap();
        graph
            .database()
            .repair_search_projection_metadata(index, MetadataRepairOptions::default())
            .unwrap();
        index.checkpoint().unwrap();
    }
    assert_eq!(document(&handle, "memory:kept"), Some(kept));
    assert_eq!(document(&handle, &gone.id), None, "deleted during the gap");
    assert_eq!(document(&handle, "memory:late"), None, "no host row yet");
    assert_eq!(document(&handle, SOURCE).unwrap().title, "graph source");
    // Rebuild and repair each raise the epoch in the same incarnation.
    let required = required_token(&handle);
    assert_eq!(required.incarnation, initial.incarnation);
    assert_eq!(required.epoch, 3);
    assert!(handle
        .acknowledge_externally_owned_rows_restored(required)
        .unwrap());
    catch_up(&handle);
    drop(handle);
    std::fs::remove_dir_all(root).unwrap();
}

/// The out-of-core recovery for a changefeed gap rebuilds into a new
/// directory: owned documents of live nodes are carried over from the
/// current projection, or, when it cannot be read, the rebuilt directory
/// publishes a fresh rows-required fence.
#[test]
fn out_of_core_rebuild_after_changefeed_gap_carries_over_or_replaces_the_fence() {
    for carry_over_fails in [false, true] {
        let config = gap_config();
        let owned = BTreeSet::from([SearchProjectionKind::Memory]);
        let (root, handle) = open_fresh_with(
            &format!("owned_gap_out_of_core_{carry_over_fails}"),
            Layout::OutOfCore,
            owned.clone(),
            Some(&config),
        );
        let source_token = required_token(&handle);
        assert!(handle
            .acknowledge_externally_owned_rows_restored(source_token)
            .unwrap());
        let (kept, gone) = manufacture_gap(&handle);
        super::super::search_projection_rebuild::FAIL_CARRY_OVER_PROBE
            .with(|fail| fail.set(carry_over_fails));
        let rebuilt = handle.rebuild_out_of_core_search_projection(
            root.join("rebuilt"),
            SearchProjectionRebuildOptions::default(),
            &mut GraphOnlySearchProjectionRebuild,
            RuntimeTaskContext::default(),
        );
        super::super::search_projection_rebuild::FAIL_CARRY_OVER_PROBE.with(|fail| fail.set(false));
        rebuilt.unwrap();
        drop(handle);

        let (handle, _) = EmbeddedStoreHandle::open_with_options(options_at(
            &root,
            "rebuilt",
            Layout::OutOfCore,
            owned.clone(),
            GraphMode::WritableCutover,
            Some(&config),
        ))
        .unwrap();
        assert_eq!(document(&handle, &gone.id), None);
        assert_eq!(document(&handle, "memory:late"), None);
        assert_eq!(document(&handle, SOURCE).unwrap().title, "graph source");
        // A rebuilt directory is a new projection incarnation. A retained
        // acknowledgement from the source must not clear its requirement.
        let rebuilt_token = required_token(&handle);
        assert_ne!(rebuilt_token.incarnation, source_token.incarnation);
        assert_eq!(rebuilt_token.epoch, 1);
        assert!(!handle
            .acknowledge_externally_owned_rows_restored(source_token)
            .unwrap());
        if carry_over_fails {
            assert_eq!(document(&handle, "memory:kept"), None);
            handle
                .apply_search_projection_delta_and_checkpoint(SearchProjectionDelta {
                    upserts: vec![host_row(SearchProjectionKind::Memory, "kept")],
                    max_operations: Some(1),
                    ..Default::default()
                })
                .unwrap();
            assert!(handle
                .acknowledge_externally_owned_rows_restored(rebuilt_token)
                .unwrap());
        } else {
            assert_eq!(document(&handle, "memory:kept"), Some(kept.clone()));
            assert!(handle
                .acknowledge_externally_owned_rows_restored(rebuilt_token)
                .unwrap());
        }
        assert_eq!(document(&handle, "memory:kept"), Some(kept));
        assert_eq!(handle.externally_owned_rows_required().unwrap(), None);
        catch_up(&handle);
        drop(handle);
        std::fs::remove_dir_all(root).unwrap();
    }
}

/// Rebuilding from an owned projection whose rows sidecar was removed or
/// corrupted never derives a replacement fence from the unreadable epoch.
/// The target receives a fresh incarnation, so a retained acknowledgement
/// from the source cannot clear its repush requirement.
#[test]
fn out_of_core_rebuild_rejects_old_tokens_when_the_source_fence_is_unreadable() {
    for damage in ["missing", "corrupt"] {
        let (root, handle) = open_fresh(
            &format!("owned_rebuild_unreadable_fence_{damage}"),
            Layout::OutOfCore,
            BTreeSet::from([SearchProjectionKind::Memory]),
        );
        let old = required_token(&handle);
        let rows = root.join("search").join(EXTERNALLY_OWNED_ROWS_MARKER);
        if damage == "missing" {
            std::fs::remove_file(rows).unwrap();
        } else {
            std::fs::write(rows, "corrupt prior rows state").unwrap();
        }

        handle
            .rebuild_out_of_core_search_projection(
                root.join("rebuilt"),
                SearchProjectionRebuildOptions::default(),
                &mut GraphOnlySearchProjectionRebuild,
                RuntimeTaskContext::default(),
            )
            .unwrap();
        drop(handle);

        let (rebuilt, _) = EmbeddedStoreHandle::open_with_options(options_at(
            &root,
            "rebuilt",
            Layout::OutOfCore,
            BTreeSet::from([SearchProjectionKind::Memory]),
            GraphMode::WritableCutover,
            None,
        ))
        .unwrap();
        let replacement = required_token(&rebuilt);
        assert_ne!(replacement.incarnation, old.incarnation, "{damage}");
        assert_eq!(replacement.epoch, 1, "{damage}");
        assert!(!rebuilt
            .acknowledge_externally_owned_rows_restored(old)
            .unwrap());
        assert_eq!(required_token(&rebuilt), replacement);
        assert!(rebuilt
            .acknowledge_externally_owned_rows_restored(replacement)
            .unwrap());
        drop(rebuilt);
        std::fs::remove_dir_all(root).unwrap();
    }
}

/// Applies one host delta to `memory:kept` after the first scan batch, while
/// the rebuild holds no store lock.
struct WriteDuringScan<'a> {
    handle: &'a EmbeddedStoreHandle,
    written: bool,
}

impl SearchProjectionRebuildSource for WriteDuringScan<'_> {
    fn progress(&mut self, progress: &SearchProjectionRebuildProgress) {
        if self.written
            || progress.phase != SearchProjectionRebuildPhase::Scan
            || progress.graph_documents == 0
        {
            return;
        }
        self.written = true;
        let mut row = host_row(SearchProjectionKind::Memory, "kept");
        row.body = "written during the rebuild".into();
        self.handle
            .apply_search_projection_delta_and_checkpoint(SearchProjectionDelta {
                upserts: vec![row],
                max_operations: Some(1),
                ..Default::default()
            })
            .unwrap();
    }
}

/// A host write to the current projection during an out-of-core rebuild is
/// not in the target. The rebuild still verifies (owned kinds are excluded
/// from the comparison), and the target publishes a new fence so the host
/// re-pushes.
#[test]
fn host_write_during_out_of_core_rebuild_leaves_the_fence_raised() {
    let config = gap_config();
    let owned = BTreeSet::from([SearchProjectionKind::Memory]);
    let (root, handle) = open_fresh_with(
        "owned_write_during_rebuild",
        Layout::OutOfCore,
        owned.clone(),
        Some(&config),
    );
    let source_token = required_token(&handle);
    assert!(handle
        .acknowledge_externally_owned_rows_restored(source_token)
        .unwrap());
    manufacture_gap(&handle);
    let mut source = WriteDuringScan {
        handle: &handle,
        written: false,
    };
    let options = SearchProjectionRebuildOptions {
        hydration_batch_rows: std::num::NonZeroUsize::new(1).unwrap(),
        priority: RuntimeWorkPriority::Foreground,
        ..SearchProjectionRebuildOptions::default()
    };
    handle
        .rebuild_out_of_core_search_projection(
            root.join("rebuilt"),
            options,
            &mut source,
            RuntimeTaskContext::default(),
        )
        .unwrap();
    assert!(source.written);
    assert_eq!(
        document(&handle, "memory:kept").unwrap().content,
        "written during the rebuild"
    );
    drop(handle);

    let (handle, _) = EmbeddedStoreHandle::open_with_options(options_at(
        &root,
        "rebuilt",
        Layout::OutOfCore,
        owned,
        GraphMode::WritableCutover,
        Some(&config),
    ))
    .unwrap();
    let rebuilt_token = required_token(&handle);
    assert_ne!(rebuilt_token.incarnation, source_token.incarnation);
    assert_eq!(rebuilt_token.epoch, 1);
    let mut row = host_row(SearchProjectionKind::Memory, "kept");
    row.body = "written during the rebuild".into();
    handle
        .apply_search_projection_delta_and_checkpoint(SearchProjectionDelta {
            upserts: vec![row],
            max_operations: Some(1),
            ..Default::default()
        })
        .unwrap();
    assert!(handle
        .acknowledge_externally_owned_rows_restored(rebuilt_token)
        .unwrap());
    assert_eq!(
        document(&handle, "memory:kept").unwrap().content,
        "written during the rebuild"
    );
    drop(handle);
    std::fs::remove_dir_all(root).unwrap();
}

/// An unreadable record is a typed, non-poisoning refusal; reopening with
/// the reset rewrites both records under a fresh incarnation.
#[test]
fn corrupt_ownership_records_are_typed_and_reset_by_an_explicit_open() {
    for layout in [Layout::Resident, Layout::OutOfCore] {
        let (root, handle) = open_fresh(&format!("owned_corrupt_{layout:?}"), layout, owned());
        let original = required_token(&handle);
        drop(handle);
        let search = root.join("search");
        let kinds = search.join(EXTERNALLY_OWNED_KINDS_MARKER);
        let rows = search.join(EXTERNALLY_OWNED_ROWS_MARKER);

        std::fs::write(&kinds, "not a record").unwrap();
        let Err(error) = EmbeddedStoreHandle::open_with_options(options(&root, layout, owned()))
        else {
            panic!("a corrupt set record refuses the open");
        };
        assert!(
            matches!(
                &error,
                HawDBError::SearchProjectionOwnershipMarkerCorrupt { marker, .. }
                    if marker == EXTERNALLY_OWNED_KINDS_MARKER
            ),
            "{error}"
        );
        let Err(error) = EmbeddedStoreHandle::open_with_options(
            options_at(
                &root,
                "search",
                layout,
                owned(),
                GraphMode::ShadowReadOnly,
                None,
            )
            .with_externally_owned_search_kinds_reset(),
        ) else {
            panic!("a read-only open cannot reset");
        };
        assert!(matches!(error, HawDBError::Semantic(_)), "{error}");
        let (handle, _) = EmbeddedStoreHandle::open_with_options(
            options(&root, layout, owned()).with_externally_owned_search_kinds_reset(),
        )
        .unwrap();
        let after_set_reset = required_token(&handle);
        assert_ne!(after_set_reset.incarnation, original.incarnation);
        assert_eq!(after_set_reset.epoch, 1);
        assert!(!handle
            .acknowledge_externally_owned_rows_restored(original)
            .unwrap());
        drop(handle);

        // A corrupt rows state is reported typed, not as rows silently restored.
        std::fs::write(&rows, "not a state").unwrap();
        let Err(error) = EmbeddedStoreHandle::open_with_options(options(&root, layout, owned()))
        else {
            panic!("a corrupt rows state refuses the open");
        };
        assert!(
            matches!(
                error,
                HawDBError::SearchProjectionOwnershipMarkerCorrupt { ref marker, .. }
                    if marker == EXTERNALLY_OWNED_ROWS_MARKER
            ),
            "{error}"
        );
        let (handle, _) = EmbeddedStoreHandle::open_with_options(
            options(&root, layout, owned()).with_externally_owned_search_kinds_reset(),
        )
        .unwrap();
        let after_rows_reset = handle
            .runtime_status()
            .unwrap()
            .externally_owned_rows_required
            .unwrap();
        assert_ne!(after_rows_reset.incarnation, after_set_reset.incarnation);
        assert_eq!(after_rows_reset.epoch, 1);
        assert!(!handle
            .acknowledge_externally_owned_rows_restored(after_set_reset)
            .unwrap());
        assert!(handle
            .acknowledge_externally_owned_rows_restored(after_rows_reset)
            .unwrap());
        drop(handle);
        std::fs::remove_dir_all(root).unwrap();
    }
}

fn assert_not_opted_in(store: &EmbeddedStore) {
    assert_eq!(store.externally_owned_rows_required().unwrap(), None);
    let status = store.runtime_status().unwrap();
    assert!(status.externally_owned_search_kinds.is_empty());
    assert_eq!(status.externally_owned_rows_required, None);
    assert_eq!(
        status.json()["projection"]["externally_owned_kinds"],
        serde_json::json!([])
    );
}

fn assert_opted_in(store: &EmbeddedStore, token: ExternallyOwnedRowsToken) {
    assert_eq!(store.externally_owned_rows_required().unwrap(), Some(token));
    let status = store.runtime_status().unwrap();
    assert_eq!(
        status.externally_owned_search_kinds,
        owned().into_iter().collect::<Vec<_>>()
    );
    assert_eq!(status.externally_owned_rows_required, Some(token));
}

/// A read-only open of a projection prepared before the host opted in is
/// not opted in yet, and observes the fence once a writer records the set,
/// without reopening (#1845).
#[test]
fn read_only_open_of_an_unrecorded_projection_follows_a_later_record() {
    for layout in [Layout::Resident, Layout::OutOfCore] {
        let (root, handle) = open_fresh(
            &format!("owned_unrecorded_reader_{layout:?}"),
            layout,
            BTreeSet::new(),
        );
        drop(handle);
        let search = root.join("search");
        let (reader, _) = EmbeddedStore::open_with_options(options_at(
            &root,
            "search",
            layout,
            owned(),
            GraphMode::ShadowReadOnly,
            None,
        ))
        .unwrap();
        assert!(!search.join(EXTERNALLY_OWNED_KINDS_MARKER).exists());
        assert!(!search.join(EXTERNALLY_OWNED_ROWS_MARKER).exists());
        assert_not_opted_in(&reader);
        assert_eq!(reader.externally_owned_search_kinds(), &owned());

        // A writable open elsewhere records the set while the reader stays
        // open; this graph is held by the reader, so record as that open does.
        crate::search::projection_ownership::reconcile(&search, &owned(), true).unwrap();
        let token = crate::search::projection_ownership::require_rows_state(&search)
            .unwrap()
            .required_token()
            .unwrap();
        assert_eq!(token.epoch, 1);
        assert_opted_in(&reader, token);
        drop(reader);
        std::fs::remove_dir_all(root).unwrap();
    }
}

/// Qualified out-of-core opens may not record either. A qualified open needs
/// a production-qualified projection of 100k documents, so the ownership
/// step is exercised with the decision such an open makes (#1845).
#[test]
fn qualified_out_of_core_open_of_an_unrecorded_projection_follows_a_later_record() {
    let root = unique_embedded_store_test_dir("owned_unrecorded_qualified");
    let search = root.join("search");
    let qualified = EmbeddedStoreOpenOptions::with_qualified_out_of_core_search_projection(
        root.join("graph"),
        &search,
        GraphMode::WritableCutover,
        QualifiedOutOfCoreSearchOptions {
            config: SearchOutOfCoreConfig::default(),
            qualification: SearchLexicalProductionQualificationReport::evaluate(
                0,
                None,
                0,
                false,
                SearchLexicalFeasibilityCoverage::default(),
                SearchLexicalFeasibilityMetrics::default(),
            ),
            expected_identity: production_identity(0),
        },
    )
    .with_externally_owned_search_kinds(owned());
    assert!(!qualified.may_record_externally_owned_search_kinds());
    assert!(matches!(
        EmbeddedStore::open_with_options(
            qualified.clone().with_externally_owned_search_kinds_reset()
        ),
        Err(HawDBError::Semantic(_))
    ));

    let (mut store, _) =
        EmbeddedStore::open_with_options(EmbeddedStoreOpenOptions::with_search_projection(
            root.join("graph"),
            &search,
            GraphMode::WritableCutover,
        ))
        .unwrap();
    store
        .attach_externally_owned_search_kinds(
            Some(search.clone()),
            owned(),
            qualified.may_record_externally_owned_search_kinds(),
            false,
        )
        .unwrap();
    assert!(!search.join(EXTERNALLY_OWNED_KINDS_MARKER).exists());
    assert_not_opted_in(&store);

    // A maintenance open elsewhere records the set later.
    crate::search::projection_ownership::reconcile(&search, &owned(), true).unwrap();
    let token = crate::search::projection_ownership::require_rows_state(&search)
        .unwrap()
        .required_token()
        .unwrap();
    assert_opted_in(&store, token);
    drop(store);
    std::fs::remove_dir_all(root).unwrap();
}

/// A record written after open that differs from the requested set is a
/// mismatch, never silently accepted (#1845).
#[test]
fn read_only_open_reports_a_record_that_differs_from_the_requested_set() {
    let (root, handle) = open_fresh(
        "owned_unrecorded_mismatch",
        Layout::Resident,
        BTreeSet::new(),
    );
    drop(handle);
    let search = root.join("search");
    let (reader, _) = EmbeddedStore::open_with_options(options_at(
        &root,
        "search",
        Layout::Resident,
        owned(),
        GraphMode::ShadowReadOnly,
        None,
    ))
    .unwrap();
    assert_not_opted_in(&reader);
    crate::search::projection_ownership::reconcile(
        &search,
        &BTreeSet::from([SearchProjectionKind::Memory]),
        true,
    )
    .unwrap();
    let expected = HawDBError::SearchProjectionOwnershipMismatch {
        projection: search,
        recorded: vec!["memory".into()],
        requested: vec!["memory".into(), "entity".into()],
    };
    assert_eq!(
        reader.externally_owned_rows_required().unwrap_err(),
        expected
    );
    assert_eq!(reader.runtime_status().unwrap_err(), expected);
    drop(reader);
    std::fs::remove_dir_all(root).unwrap();
}

/// A writable open that recorded the set fails closed if the record is
/// removed out of band while it is open.
#[test]
fn recording_open_fails_closed_when_its_record_disappears() {
    let (root, handle) = open_fresh("owned_record_removed", Layout::Resident, owned());
    required_token(&handle);
    let record = root.join("search").join(EXTERNALLY_OWNED_KINDS_MARKER);
    std::fs::remove_file(&record).unwrap();
    assert!(matches!(
        handle.externally_owned_rows_required(),
        Err(HawDBError::SearchProjectionOwnershipMarkerCorrupt { ref marker, .. })
            if marker == EXTERNALLY_OWNED_KINDS_MARKER
    ));
    drop(handle);
    std::fs::remove_dir_all(root).unwrap();
}
