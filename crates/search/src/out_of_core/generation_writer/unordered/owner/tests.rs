// Copyright 2026 Nowledge
// Licensed under the Apache License, Version 2.0.

use super::super::tests::{document, fixture, limits};
use super::*;

fn stage(reader: &SearchOutOfCoreReader, size: usize) -> SearchOutOfCoreOwnerReplacementWriter {
    stage_with_options(reader, size, Default::default())
}
fn stage_with_options(
    reader: &SearchOutOfCoreReader,
    size: usize,
    options: SearchOutOfCoreGenerationBuildOptions,
) -> SearchOutOfCoreOwnerReplacementWriter {
    let inner = SearchOutOfCoreMutationWriter::create_replacement(
        reader,
        options,
        Default::default(),
        BuildMemory::new(&Default::default()).unwrap(),
    )
    .unwrap();
    from_inner(
        inner,
        SearchOwnerOverlayLimits {
            ordering: limits(size),
            ..Default::default()
        },
        reader.config().max_document_header_bytes.get(),
    )
    .unwrap()
}

#[test]
fn independent_complete_e_sets_match_all_small_owner_states_and_late_c_orders() {
    use std::collections::BTreeMap;
    let (root, reader) = fixture("owner-finite-reference");
    let value = |kind: usize, phase: &str| match kind {
        0 => None,
        1 => Some(document("a", phase)),
        2 => Some(document("b", phase)),
        _ => unreachable!(),
    };
    let mut checked = 0;
    let mut refused = 0;
    for c1 in 0..3 {
        for c2 in 0..3 {
            if c1 == c2 && c1 != 0 {
                continue;
            }
            for e1 in 0..3 {
                for e2 in 0..3 {
                    let final_states = [value(e1, "E"), value(e2, "E")];
                    let mut expected = BTreeMap::new();
                    let mut unique = true;
                    for doc in final_states.iter().flatten() {
                        unique &= expected.insert(doc.id.clone(), doc.clone()).is_none();
                    }
                    for size in [1, 8] {
                        for late in [false, true] {
                            let mut writer = stage(&reader, size);
                            let mut input = vec![
                                ("one", 0, value(c1, "C")),
                                ("two", 0, value(c2, "C")),
                                ("one", 1, final_states[0].clone()),
                                ("two", 1, final_states[1].clone()),
                            ];
                            if late {
                                input.swap(0, 3);
                            }
                            for (key, phase, doc) in input {
                                offer(&mut writer, key, phase, doc);
                            }
                            writer.finish_input(100).unwrap();
                            if !unique {
                                assert!(complete(&mut writer).is_err());
                                assert!(writer.prepare_publication().is_err());
                                refused += 1;
                            } else {
                                complete(&mut writer).unwrap();
                                assert_eq!(writer.report().documents, expected.len());
                                if let Some(run) = writer.inner.ordering.take_reduced() {
                                    let mut source = Reader::open(
                                        run,
                                        &mut writer.inner.control,
                                        &writer.inner.inner.writer,
                                    )
                                    .unwrap();
                                    let mut found = 0;
                                    while let Some(entry) =
                                        source.next(&mut writer.inner.control).unwrap()
                                    {
                                        let doc = expected
                                            .get(entry.id())
                                            .expect("unexpected final document");
                                        writer.inner.control.require_document(&entry, doc).unwrap();
                                        found += 1;
                                    }
                                    assert_eq!(found, expected.len());
                                    source.remove(&writer.inner.control).unwrap();
                                } else {
                                    assert!(expected.is_empty());
                                }
                                drop(writer);
                            }
                            assert_eq!(
                                SearchOutOfCoreReader::open(&root).unwrap().generation(),
                                reader.generation()
                            );
                            no_stages(&root);
                            checked += 1;
                        }
                    }
                }
            }
        }
    }
    assert_eq!(checked, 252);
    assert_eq!(refused, 56);
    drop(reader);
    std::fs::remove_dir_all(root).unwrap();
}

#[test]
fn owner_results_partition_without_reintroducing_old_target_documents() {
    let (root, reader) = fixture("owner-partitioned");
    let mut writer = stage_with_options(
        &reader,
        1,
        SearchOutOfCoreGenerationBuildOptions {
            max_content_documents: NonZeroUsize::new(1).unwrap(),
            ..Default::default()
        },
    );
    for (key, id) in [("one", "x"), ("two", "y")] {
        offer(&mut writer, key, 0, Some(document(id, "new")));
    }
    writer.finish_input(100).unwrap();
    complete(&mut writer).unwrap();
    let (_, candidate) = writer.prepare_publication().unwrap();
    candidate.publish().unwrap();
    let current = SearchOutOfCoreReader::open(&root).unwrap();
    assert_eq!(current.document_count(), 2);
    for id in ["a", "b", "k"] {
        assert!(current.segment_for_document(id).unwrap().is_none());
    }
    assert_eq!(
        current
            .hydrate_documents(&["x".into(), "y".into()])
            .unwrap()
            .documents
            .len(),
        2
    );
    drop(current);
    drop(reader);
    no_stages(&root);
    std::fs::remove_dir_all(root).unwrap();
}
fn quantum() -> SearchMutationSortStepLimits {
    SearchMutationSortStepLimits {
        work: NonZeroUsize::new(16).unwrap(),
        read_bytes: NonZeroU64::new(4096).unwrap(),
        spill_bytes: NonZeroU64::new(4096).unwrap(),
    }
}
fn tick(writer: &mut SearchOutOfCoreOwnerReplacementWriter) -> Result<()> {
    let q = quantum();
    let step = writer.advance_ordering(q)?;
    assert!(step.usage.work <= q.work.get());
    assert!(step.usage.read_bytes <= q.read_bytes.get());
    assert!(step.usage.spill_bytes <= q.spill_bytes.get());
    Ok(())
}
fn offer(
    writer: &mut SearchOutOfCoreOwnerReplacementWriter,
    key: &str,
    phase: u64,
    doc: Option<SearchDocument>,
) {
    while !writer.ready_for_input() {
        tick(writer).unwrap();
    }
    writer.queue_owner_state(key, phase, doc).unwrap();
}
fn complete(writer: &mut SearchOutOfCoreOwnerReplacementWriter) -> Result<()> {
    for _ in 0..20000 {
        if writer.ordering_complete() {
            return Ok(());
        }
        tick(writer)?;
    }
    panic!("owner ordering made no bounded progress");
}
fn no_stages(root: &Path) {
    assert!(!std::fs::read_dir(root).unwrap().any(|entry| entry
        .unwrap()
        .file_type()
        .unwrap()
        .is_dir()));
}

#[test]
fn late_c_rename_delete_and_swaps_resolve_owners_before_document_ids() {
    for size in [1, 2, 16] {
        for swap in [false, true] {
            let (root, reader) = fixture("owner-overlay-final");
            let mut writer = stage(&reader, size);
            let mut first = document(if swap { "b" } else { "z" }, "final-one");
            first.embedding = Some(vec![0.25, 0.75]);
            first.metadata.insert("source-owner".into(), "one".into());
            let second = swap.then(|| document("a", "final-two"));
            offer(&mut writer, "one", 1, Some(first.clone()));
            offer(&mut writer, "two", 0, Some(document("b", "old-two")));
            offer(&mut writer, "one", 0, Some(document("a", "old-one")));
            offer(&mut writer, "two", 2, second.clone());
            offer(&mut writer, "one", 1, Some(first.clone()));
            writer.finish_input(100).unwrap();
            complete(&mut writer).unwrap();
            assert_eq!(writer.report().reduced_owners, 2);
            let (report, candidate) = writer.prepare_publication().unwrap();
            assert_eq!(report.owner_inputs, 5);
            assert_eq!(report.documents, 1 + usize::from(swap));
            assert_eq!(report.sorting.input_operations, 5 + report.documents);
            assert_eq!(
                candidate.expected_active_generation(),
                Some(reader.generation())
            );
            assert_eq!(
                SearchOutOfCoreReader::open(&root).unwrap().document_count(),
                3
            );
            candidate.publish().unwrap();
            let current = SearchOutOfCoreReader::open(&root).unwrap();
            assert_eq!(current.document_count(), report.documents);
            let output = current
                .hydrate_documents(std::slice::from_ref(&first.id))
                .unwrap();
            assert_eq!(output.documents, [first]);
            if let Some(second) = second {
                assert_eq!(
                    current
                        .hydrate_documents(std::slice::from_ref(&second.id))
                        .unwrap()
                        .documents,
                    [second]
                );
            }
            assert_eq!(current.source_graph_commit_epoch(), Some(100));
            assert!(current.manifest.mutation_runs.is_empty());
            drop(current);
            drop(reader);
            no_stages(&root);
            std::fs::remove_dir_all(root).unwrap();
        }
    }
}

#[test]
fn duplicate_owners_and_superseded_same_phase_conflicts_refuse_invisible() {
    for size in [1, 2, 16] {
        for case in [
            "collision",
            "body",
            "title",
            "metadata",
            "vector",
            "absence",
        ] {
            let (root, reader) = fixture("owner-conflict");
            let mut writer = stage(&reader, size);
            let original = reader.generation();
            if case == "collision" {
                offer(&mut writer, "one", 0, Some(document("a", "before")));
                offer(&mut writer, "two", 0, Some(document("b", "same")));
                offer(&mut writer, "one", 1, Some(document("b", "same")));
            } else {
                let first = document("a", "same");
                let mut other = first.clone();
                match case {
                    "body" => other.content = "different".into(),
                    "title" => other.title = "different".into(),
                    "metadata" => {
                        other.metadata.insert("field".into(), "different".into());
                    }
                    "vector" => other.embedding = Some(vec![0.25, 0.75]),
                    _ => {}
                }
                offer(&mut writer, "one", 1, Some(first));
                offer(&mut writer, "one", 2, None);
                offer(&mut writer, "one", 1, (case != "absence").then_some(other));
            }
            writer.finish_input(100).unwrap();
            assert!(complete(&mut writer).is_err(), "size={size} case={case}");
            assert!(writer.advance_ordering(unlimited_step()).is_err());
            assert!(writer.prepare_publication().is_err());
            assert_eq!(
                SearchOutOfCoreReader::open(&root).unwrap().generation(),
                original
            );
            drop(reader);
            no_stages(&root);
            std::fs::remove_dir_all(root).unwrap();
        }
    }
}

#[test]
fn empty_overlay_and_all_owner_tombstones_replace_the_complete_old_target() {
    for with_owner in [false, true] {
        let (root, reader) = fixture("owner-empty");
        let mut writer = stage(&reader, 1);
        if with_owner {
            offer(&mut writer, "one", 1, None);
            offer(&mut writer, "one", 0, Some(document("a", "before")));
        }
        writer.finish_input(100).unwrap();
        complete(&mut writer).unwrap();
        let (report, candidate) = writer.prepare_publication().unwrap();
        assert_eq!(report.documents, 0);
        candidate.publish().unwrap();
        let current = SearchOutOfCoreReader::open(&root).unwrap();
        assert_eq!(current.document_count(), 0);
        drop(current);
        drop(reader);
        no_stages(&root);
        std::fs::remove_dir_all(root).unwrap();
    }
}

#[test]
fn second_sort_keeps_cumulative_admission_and_cancelled_attempts_stay_poisoned() {
    for cancel in [false, true] {
        let (root, reader) = fixture("owner-cumulative");
        let mut writer = stage(&reader, 1);
        let memory = writer.inner.inner.writer.memory.clone();
        writer.inner.control.limits.max_input_operations = NonZeroUsize::new(2).unwrap();
        offer(&mut writer, "one", 0, Some(document("a", "one")));
        offer(&mut writer, "two", 0, Some(document("b", "two")));
        writer.finish_input(100).unwrap();
        if cancel {
            writer.task_context().cancellation().cancel();
        }
        assert!(complete(&mut writer).is_err());
        assert!(writer.queue_owner_state("new", 1, None).is_err());
        assert!(writer.prepare_publication().is_err());
        assert_eq!(memory.ledger.snapshot().used_bytes, 0);
        drop(memory);
        drop(reader);
        no_stages(&root);
        std::fs::remove_dir_all(root).unwrap();
    }
}

#[test]
fn one_actual_governor_permit_covers_both_sorts_and_queued_candidate() {
    use hawdb_qos::{
        IoConcurrencyBudget, RuntimeGovernor, RuntimeGovernorConfig, RuntimeWorkRequest,
    };
    let (root, reader) = fixture("owner-custody");
    let governor = RuntimeGovernor::detect(
        RuntimeGovernorConfig {
            memory_budget_bytes: Some(64 << 20),
            background_task_limit: NonZeroUsize::new(1),
            ..Default::default()
        },
        IoConcurrencyBudget::new(2, 1),
    );
    let request = RuntimeWorkRequest::background_maintenance(16 << 20);
    let admission = crate::SearchGenerationAdmission::acquire(&governor, request).unwrap();
    let mut writer = admission
        .prepare_owner_replacement(
            &reader,
            Default::default(),
            SearchOwnerOverlayLimits {
                ordering: limits(1),
                ..Default::default()
            },
            Default::default(),
        )
        .unwrap();
    drop(reader);
    offer(&mut writer, "one", 0, Some(document("a", "before")));
    offer(&mut writer, "one", 1, Some(document("z", "after")));
    writer.finish_input(100).unwrap();
    assert!(crate::SearchGenerationAdmission::acquire(&governor, request).is_err());
    let (_, candidate) = std::thread::spawn(move || {
        complete(&mut writer).unwrap();
        writer.prepare_publication().unwrap()
    })
    .join()
    .unwrap();
    assert!(crate::SearchGenerationAdmission::acquire(&governor, request).is_err());
    drop(candidate);
    drop(crate::SearchGenerationAdmission::acquire(&governor, request).unwrap());
    no_stages(&root);
    std::fs::remove_dir_all(root).unwrap();
}

#[test]
fn damaged_owner_phase_binding_and_torn_runs_never_produce_a_candidate() {
    for field in ["phase", "key", "body", "checksum", "truncate"] {
        let (root, reader) = fixture("owner-damaged");
        let original = reader.generation();
        let mut writer = stage(&reader, 1);
        offer(&mut writer, "one", 0, Some(document("a", "first-body")));
        offer(&mut writer, "two", 0, Some(document("b", "second-body")));
        while !writer.ready_for_input() {
            tick(&mut writer).unwrap();
        }
        let run = writer.inner.levels.iter().flatten().next().unwrap();
        let path = run.path_for_test().to_path_buf();
        let mut bytes = std::fs::read(&path).unwrap();
        let start = spool::SPOOL_HEADER.len();
        match field {
            "phase" => bytes[start + 9] ^= 1,
            "key" => bytes[start + 17] ^= 1,
            "body" => {
                let key_bytes =
                    u64::from_le_bytes(bytes[start + 1..start + 9].try_into().unwrap()) as usize;
                let frame = start + 19 + key_bytes;
                let body_bytes =
                    u64::from_le_bytes(bytes[frame..frame + 8].try_into().unwrap()) as usize;
                bytes[frame + 16 + body_bytes - 1] ^= 1;
            }
            "checksum" => {
                let end = bytes.len() - 1;
                bytes[end] ^= 1;
            }
            "truncate" => {
                bytes.pop();
            }
            _ => unreachable!(),
        }
        std::fs::write(&path, bytes).unwrap();
        writer.finish_input(100).unwrap();
        assert!(complete(&mut writer).is_err(), "field={field}");
        assert!(writer.prepare_publication().is_err());
        assert_eq!(
            SearchOutOfCoreReader::open(&root).unwrap().generation(),
            original
        );
        drop(reader);
        no_stages(&root);
        std::fs::remove_dir_all(root).unwrap();
    }
}

#[test]
fn cancellation_at_each_sort_boundary_keeps_the_old_head_and_releases_all_memory() {
    for phase in [
        Phase::OwnerSort,
        Phase::Transfer,
        Phase::DocumentSort,
        Phase::Complete,
    ] {
        let (root, reader) = fixture("owner-boundary-cancel");
        let original = reader.generation();
        let mut writer = stage(&reader, 1);
        let memory = writer.inner.inner.writer.memory.clone();
        for index in 0..8 {
            offer(
                &mut writer,
                &index.to_string(),
                0,
                Some(document(&format!("d{index}"), "body")),
            );
        }
        writer.finish_input(100).unwrap();
        let q = SearchMutationSortStepLimits {
            work: NonZeroUsize::new(4).unwrap(),
            ..quantum()
        };
        for _ in 0..20000 {
            if writer.phase == phase {
                break;
            }
            assert!(!writer.ordering_complete(), "skipped requested phase");
            writer.advance_ordering(q).unwrap();
        }
        assert!(writer.phase == phase);
        writer.task_context().cancellation().cancel();
        if phase == Phase::Complete {
            assert!(writer.prepare_publication().is_err());
        } else {
            assert!(writer.advance_ordering(q).is_err());
            assert!(writer.prepare_publication().is_err());
        }
        assert_eq!(memory.ledger.snapshot().used_bytes, 0);
        assert_eq!(
            SearchOutOfCoreReader::open(&root).unwrap().generation(),
            original
        );
        drop(memory);
        drop(reader);
        no_stages(&root);
        std::fs::remove_dir_all(root).unwrap();
    }
}
