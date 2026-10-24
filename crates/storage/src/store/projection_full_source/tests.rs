// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

use super::*;

fn budget(work: usize, items: usize, bytes: usize) -> ProjectionFeedLimits {
    ProjectionFeedLimits {
        work: NonZeroUsize::new(work).unwrap(),
        items: NonZeroUsize::new(items).unwrap(),
        bytes: NonZeroUsize::new(bytes).unwrap(),
    }
}

#[test]
fn scoped_full_source_skips_sparse_ids_and_preserves_hot_cold_overlay() {
    for cold in [false, true] {
        let path = std::env::temp_dir().join(format!(
            "hawdb-label-enumeration-{}",
            hawdb_core::generate_uuidv7().unwrap()
        ));
        let mut catalog = Catalog::default();
        let mut store = GraphStore::open(&path, &mut catalog).unwrap();
        let mut records = (0..64)
            .map(|id| {
                (
                    NodeId(id * 1_000_000),
                    "Other".into(),
                    BTreeMap::from([("body".into(), Value::String("x".repeat(8192)))]),
                )
            })
            .collect::<Vec<_>>();
        let first = NodeId(1_000_000_000);
        let second = NodeId(2_000_000_000);
        records.extend([
            (first, "Memory".into(), BTreeMap::new()),
            (second, "Note".into(), BTreeMap::new()),
        ]);
        store
            .import_graph_snapshot_rows(&mut catalog, records, vec![])
            .unwrap();
        let memory = catalog.label_id("Memory").unwrap();
        let note = catalog.label_id("Note").unwrap();
        // Synthetic multi-label baseline, durably sealed before qualification.
        store.nodes.get_mut(&second).unwrap().labels.insert(memory);
        store.checkpoint(&catalog).unwrap();
        if cold {
            drop(store);
            store = GraphStore::open_with_durability_and_replay_config(
                &path,
                &mut catalog,
                DurabilityPolicy::default(),
                WalReplayConfig {
                    residency_mode: StorageResidencyMode::OutOfCore,
                    ..Default::default()
                },
            )
            .unwrap();
            assert!(store.canonical_base_out_of_core);
        }
        // Capture the durable base, then add a resident replacement, tombstone
        // and new node before taking a second snapshot.
        let mut base_source = store
            .capture_projection_source_domains(
                budget(1000, 100, 8192),
                ProjectionFullSourceLimits::default(),
                Default::default(),
                Some(vec![memory, note]),
                Some(vec![]),
            )
            .unwrap();
        store
            .set_node_property(
                &mut catalog,
                "Memory",
                None,
                "title",
                Value::String("updated".into()),
            )
            .unwrap();
        store
            .delete_node_ids_with_limits(&mut catalog, &[second], true, MutationLimits::default())
            .unwrap();
        let inserted = store
            .create_node(&mut catalog, "Note", BTreeMap::new())
            .unwrap();
        let mut source = store
            .capture_projection_source_domains(
                budget(1000, 100, 8192),
                ProjectionFullSourceLimits::default(),
                Default::default(),
                Some(vec![note, memory, memory]),
                Some(vec![]),
            )
            .unwrap();
        let base_epoch = base_source.source_epoch();
        let mut base_ids = BTreeSet::new();
        while !base_source
            .visit_next(budget(16, 1, 256), |item| {
                match item {
                    ProjectionFullSourceItem::GraphNodeCandidate { node_id } => {
                        assert!(base_ids.insert(NodeId(node_id)));
                    }
                    ProjectionFullSourceItem::End { source_epoch } => {
                        assert_eq!(source_epoch, base_epoch)
                    }
                    _ => panic!("no relational domains"),
                }
                Ok(())
            })
            .unwrap()
            .complete
        {}
        assert_eq!(base_ids, BTreeSet::from([first, second]));
        base_source.finish().unwrap();
        let epoch = source.source_epoch();
        // Writers and a newer checkpoint must not retarget the captured labels.
        store
            .create_node(&mut catalog, "Memory", BTreeMap::new())
            .unwrap();
        store.checkpoint(&catalog).unwrap();
        let mut actual = BTreeSet::new();
        for _ in 0..1000 {
            let next = source
                .visit_next(budget(16, 1, 256), |item| {
                    match item {
                        ProjectionFullSourceItem::GraphNodeCandidate { node_id } => {
                            assert!(
                                actual.insert(NodeId(node_id)),
                                "overlapping labels duplicated a node"
                            );
                        }
                        ProjectionFullSourceItem::End { source_epoch } => {
                            assert_eq!(source_epoch, epoch)
                        }
                        _ => panic!("no relational domains selected"),
                    }
                    Ok(())
                })
                .unwrap();
            assert!(next.usage.work <= 16);
            assert!(next.usage.items <= 1);
            if next.complete {
                break;
            }
        }
        assert!(source.is_complete());
        assert_eq!(actual, BTreeSet::from([first, inserted]));
        let usage = source.finish().unwrap();
        assert!(
            usage.items < 100,
            "enumeration must not visit the two-billion-ID allocation range"
        );
        assert!(usage.work < 1000);
        drop(store);
        std::fs::remove_dir_all(path).unwrap();
    }
}

#[test]
fn scoped_full_source_label_work_refusal_is_sticky_and_empty_domains_are_complete() {
    let mut catalog = Catalog::default();
    let mut store = GraphStore::default();
    store
        .create_node(&mut catalog, "Memory", BTreeMap::new())
        .unwrap();
    let memory = catalog.label_id("Memory").unwrap();
    let mut source = store
        .capture_projection_source_domains(
            budget(1, 8, 8192),
            ProjectionFullSourceLimits::default(),
            Default::default(),
            Some(vec![memory]),
            Some(vec![]),
        )
        .unwrap();
    assert!(matches!(
        source.visit_next(budget(16, 1, 256), |_| panic!(
            "refused input must not escape"
        )),
        Err(ProjectionFeedError::BudgetExceeded(
            ProjectionFeedResource::Work
        ))
    ));
    assert!(matches!(
        source.visit_next(budget(16, 1, 256), |_| Ok(())),
        Err(ProjectionFeedError::Failed)
    ));
    let mut empty = store
        .capture_projection_source_domains(
            budget(8, 8, 256),
            ProjectionFullSourceLimits::default(),
            Default::default(),
            Some(vec![]),
            Some(vec![]),
        )
        .unwrap();
    let mut ends = 0;
    let result = empty
        .visit_next(budget(8, 1, 256), |item| {
            assert!(matches!(item, ProjectionFullSourceItem::End { .. }));
            ends += 1;
            Ok(())
        })
        .unwrap();
    assert!(result.complete);
    assert_eq!(ends, 1);
}

#[test]
fn scoped_full_source_cancellation_between_steps_cannot_complete_or_poison_storage() {
    let mut catalog = Catalog::default();
    let mut store = GraphStore::default();
    for _ in 0..2 {
        store
            .create_node(&mut catalog, "Memory", BTreeMap::new())
            .unwrap();
    }
    let token = hawdb_core::RuntimeCancellationToken::new();
    let mut source = store
        .capture_projection_source_domains(
            budget(100, 16, 4096),
            ProjectionFullSourceLimits::default(),
            RuntimeTaskContext::without_deadline(token.clone()),
            Some(vec![catalog.label_id("Memory").unwrap()]),
            Some(vec![]),
        )
        .unwrap();
    let first = source.visit_next(budget(8, 1, 256), |_| Ok(())).unwrap();
    assert!(!first.complete);
    assert_eq!(first.usage.items, 1);
    token.cancel();
    assert!(matches!(
        source.visit_next(budget(8, 1, 256), |_| panic!("cancelled source escaped")),
        Err(ProjectionFeedError::Source(_))
    ));
    assert!(matches!(
        source.visit_next(budget(8, 1, 256), |_| Ok(())),
        Err(ProjectionFeedError::Failed)
    ));
    assert!(!source.is_complete());
    assert!(matches!(source.finish(), Err(ProjectionFeedError::Failed)));
    store.ensure_usable().unwrap();
}
