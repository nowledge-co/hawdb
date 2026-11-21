// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

use super::exact_candidate_tests::{governor, Root};
use super::*;
use crate::search::SearchProjectionPageGeneration;
use crate::{
    Database, DatabaseConfig, HawDBError, RuntimeCancellationToken, RuntimeTaskContext,
    SearchIndex, SearchProjectionDelta, SearchProjectionRow,
};
use std::collections::{BTreeMap, BTreeSet};

fn expected_ids(kinds: &BTreeSet<SearchProjectionKind>) -> Vec<String> {
    let mut ids = (0..18)
        .filter_map(|number| {
            let kind = if number % 4 == 0 {
                SearchProjectionKind::Source
            } else {
                SearchProjectionKind::Memory
            };
            kinds
                .contains(&kind)
                .then(|| format!("{}:{number:04}", kind.as_str()))
        })
        .collect::<Vec<_>>();
    ids.sort();
    ids
}

fn rows() -> Vec<SearchProjectionRow> {
    (0..18)
        .map(|number| SearchProjectionRow {
            kind: if number % 4 == 0 {
                SearchProjectionKind::Source
            } else {
                SearchProjectionKind::Memory
            },
            external_id: format!("{number:04}"),
            title: format!("title {number}"),
            body: format!("body {number}"),
            embedding: None,
            source_id: None,
            metadata: BTreeMap::new(),
        })
        .collect()
}

fn read_config() -> DatabaseConfig {
    DatabaseConfig {
        max_read_result_rows: Some(64),
        max_read_result_payload_bytes: Some(256 * 1_024),
        ..Default::default()
    }
}

fn handle(root: &Root, out_of_core: bool, config: DatabaseConfig) -> EmbeddedStoreHandle {
    let database = Database::new_with_config(config);
    let commit_epoch = database.commit_epoch().unwrap();
    let graph = EmbeddedGraph::from_database_with_runtime_governor(
        database,
        GraphMode::ShadowReadOnly,
        governor(),
    );
    let store = if out_of_core {
        let path = root.0.join("id-page-out-of-core");
        let mut writer = crate::SearchOutOfCoreGenerationWriter::create(
            &path,
            crate::SearchOutOfCoreGenerationBuildOptions {
                source_graph_commit_epoch: Some(commit_epoch),
                ..Default::default()
            },
        )
        .unwrap();
        let mut documents = rows()
            .into_iter()
            .map(SearchProjectionRow::into_document)
            .collect::<Vec<_>>();
        documents.sort_by(|left, right| left.id.cmp(&right.id));
        for document in documents {
            writer.push(document).unwrap();
        }
        writer.finish().unwrap();
        EmbeddedStore::new_with_out_of_core_search(
            graph,
            OutOfCoreSearchProjection::open(path).unwrap(),
            Default::default(),
        )
    } else {
        let mut index = SearchIndex::open(root.0.join("id-page-resident")).unwrap();
        index
            .apply_projection_delta(SearchProjectionDelta {
                upserts: rows(),
                source_graph_commit_epoch: Some(commit_epoch),
                ..Default::default()
            })
            .unwrap();
        EmbeddedStore::new(graph, Some(SearchProjection::from_index(index)))
    };
    EmbeddedStoreHandle::new(store).unwrap()
}

fn collect_pages(
    handle: &EmbeddedStoreHandle,
    kinds: &BTreeSet<SearchProjectionKind>,
    limit: usize,
) -> (Vec<String>, SearchProjectionPageGeneration) {
    let mut after = None;
    let mut generation = None;
    let mut ids = Vec::new();
    loop {
        let page = handle
            .list_search_projection_ids(
                kinds,
                after.as_deref(),
                limit,
                &RuntimeTaskContext::default(),
            )
            .unwrap();
        if let Some(expected) = generation {
            assert_eq!(page.generation(), expected);
        } else {
            generation = Some(page.generation());
        }
        ids.extend(page.ids().iter().cloned());
        let Some(next) = page.next().map(str::to_string) else {
            break;
        };
        assert_eq!(page.ids().last(), Some(&next));
        after = Some(next);
    }
    (ids, generation.unwrap())
}

#[test]
fn resident_and_out_of_core_pages_are_ordered_and_filter_kinds() {
    for out_of_core in [false, true] {
        let root = Root::new(if out_of_core {
            "projection_id_page_ooc"
        } else {
            "projection_id_page_resident"
        });
        let handle = handle(&root, out_of_core, read_config());
        let both = BTreeSet::from([SearchProjectionKind::Memory, SearchProjectionKind::Source]);
        let (actual, generation) = collect_pages(&handle, &both, 5);
        assert_eq!(actual, expected_ids(&both));
        assert!(matches!(
            (out_of_core, generation),
            (false, SearchProjectionPageGeneration::Resident(_))
                | (true, SearchProjectionPageGeneration::OutOfCore { .. })
        ));

        let sources = BTreeSet::from([SearchProjectionKind::Source]);
        let (actual, _) = collect_pages(&handle, &sources, 2);
        assert_eq!(actual, expected_ids(&sources));
    }
}

#[test]
fn pages_retain_only_their_id_and_cursor_bytes_on_both_paths() {
    for out_of_core in [false, true] {
        let root = Root::new(if out_of_core {
            "projection_id_page_memory_ooc"
        } else {
            "projection_id_page_memory_resident"
        });
        let handle = handle(&root, out_of_core, read_config());
        let kinds = BTreeSet::from([SearchProjectionKind::Memory]);
        let before = handle.runtime_governor_snapshot().unwrap();
        let page = handle
            .list_search_projection_ids(&kinds, None, 7, &RuntimeTaskContext::default())
            .unwrap();
        let expected = page.ids().iter().map(|id| id.len() as u64).sum::<u64>()
            + page.next().map_or(0, |next| next.len() as u64);
        assert_eq!(page.retained_id_bytes(), expected);
        let while_alive = handle.runtime_governor_snapshot().unwrap();
        assert_eq!(
            while_alive.admitted_memory_bytes - before.admitted_memory_bytes,
            expected
        );
        drop(page);
        assert_eq!(
            handle
                .runtime_governor_snapshot()
                .unwrap()
                .admitted_memory_bytes,
            before.admitted_memory_bytes
        );
    }
}

#[test]
fn id_pages_fit_when_complete_projection_payload_cannot() {
    for out_of_core in [false, true] {
        let root = Root::new(if out_of_core {
            "projection_id_page_tiny_payload_ooc"
        } else {
            "projection_id_page_tiny_payload_resident"
        });
        let handle = handle(
            &root,
            out_of_core,
            DatabaseConfig {
                max_read_result_rows: Some(64),
                max_read_result_payload_bytes: Some(64),
                ..Default::default()
            },
        );
        let page = handle
            .list_search_projection_ids(
                &BTreeSet::from([SearchProjectionKind::Memory]),
                None,
                4,
                &RuntimeTaskContext::default(),
            )
            .unwrap();
        assert_eq!(page.ids().len(), 4);
        assert!(page.retained_id_bytes() <= 64);
        assert!(page.next().is_some());
        let complete = handle
            .with_bounded_read_snapshot(
                EmbeddedReadSnapshotBudget {
                    max_rows: 64,
                    max_payload_bytes: 64,
                },
                |snapshot| {
                    snapshot
                        .visit_search_projection(1_024, |_| Ok(()))
                        .map(|_| ())
                },
            )
            .unwrap_err();
        assert!(matches!(
            complete,
            HawDBError::ReadBudgetExceeded(ref refusal)
                if refusal.resource == ReadBudgetResource::PayloadBytes
        ));
    }
}

#[test]
fn resident_generation_change_is_visible_between_pages_without_a_pin() {
    let root = Root::new("projection_id_page_generation");
    let handle = handle(&root, false, read_config());
    let kinds = BTreeSet::from([SearchProjectionKind::Memory]);
    let first = handle
        .list_search_projection_ids(&kinds, None, 5, &RuntimeTaskContext::default())
        .unwrap();
    handle
        .apply_search_projection_delta_and_checkpoint(SearchProjectionDelta {
            upserts: vec![SearchProjectionRow {
                kind: SearchProjectionKind::Memory,
                external_id: "zzzz".into(),
                title: "new".into(),
                body: "new".into(),
                embedding: None,
                source_id: None,
                metadata: BTreeMap::new(),
            }],
            source_graph_commit_epoch: Some(2),
            ..Default::default()
        })
        .unwrap();
    let second = handle
        .list_search_projection_ids(&kinds, first.next(), 5, &RuntimeTaskContext::default())
        .unwrap();
    assert_ne!(first.generation(), second.generation());
}

#[test]
fn projection_id_page_honors_typed_cancellation() {
    let root = Root::new("projection_id_page_cancel");
    let handle = handle(&root, true, read_config());
    let kinds = BTreeSet::from([SearchProjectionKind::Memory]);
    let cancellation = RuntimeCancellationToken::new();
    assert!(cancellation.cancel());
    let task = RuntimeTaskContext::without_deadline(cancellation);
    assert!(matches!(
        handle
            .list_search_projection_ids(&kinds, None, 5, &task)
            .unwrap_err(),
        HawDBError::RuntimeTaskStopped(_)
    ));
}

#[test]
fn projection_id_page_admission_refusal_stays_typed() {
    use hawdb_qos::{
        IoConcurrencyBudget, RuntimeGovernor, RuntimeGovernorConfig, RuntimeMemorySnapshot,
        RuntimeResourceBudget, RuntimeResourceSnapshot,
    };
    use std::num::NonZeroUsize;

    let governor = RuntimeGovernor::new(
        RuntimeGovernorConfig::shared_host(),
        RuntimeResourceSnapshot::from_parts(
            RuntimeResourceBudget::from_limits(NonZeroUsize::MIN, None, None),
            RuntimeMemorySnapshot::from_limits(Some(1_024), Some(1_024), None, None, None),
        ),
        IoConcurrencyBudget::new(1, 1),
    );
    let graph = EmbeddedGraph::from_database_with_runtime_governor(
        Database::new_with_config(DatabaseConfig {
            max_read_result_rows: Some(64),
            max_read_result_payload_bytes: Some(4_096),
            ..Default::default()
        }),
        GraphMode::WritableCutover,
        governor,
    );
    let mut index = SearchIndex::in_memory();
    index
        .apply_projection_delta(SearchProjectionDelta {
            upserts: vec![SearchProjectionRow {
                kind: SearchProjectionKind::Memory,
                external_id: "one".into(),
                title: "one".into(),
                body: "one".into(),
                embedding: None,
                source_id: None,
                metadata: BTreeMap::new(),
            }],
            ..Default::default()
        })
        .unwrap();
    let handle = EmbeddedStoreHandle::new(EmbeddedStore::new(
        graph,
        Some(SearchProjection::from_index(index)),
    ))
    .unwrap();
    let error = handle
        .list_search_projection_ids(
            &BTreeSet::from([SearchProjectionKind::Memory]),
            None,
            64,
            &RuntimeTaskContext::default(),
        )
        .unwrap_err();
    assert!(matches!(
        error,
        HawDBError::RuntimeAdmissionRefused(ref refusal)
            if refusal.code == "memory_saturated"
    ));
}
