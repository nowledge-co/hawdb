// Copyright 2026 Nowledge
// Licensed under the Apache License, Version 2.0.

use super::tests::{document, fixture, limits};
use super::*;
use std::collections::BTreeMap;

fn quantum() -> SearchMutationSortStepLimits {
    SearchMutationSortStepLimits {
        work: NonZeroUsize::new(8).unwrap(),
        read_bytes: NonZeroU64::new(127).unwrap(),
        spill_bytes: NonZeroU64::new(127).unwrap(),
    }
}
fn advance(writer: &mut SearchOutOfCoreUnorderedMutationWriter<'_>) -> SearchMutationSortStep {
    let before = writer.report();
    let step = writer.advance_ordering(quantum()).unwrap();
    let after = writer.report();
    assert_eq!(after.sort_work - before.sort_work, step.usage.work);
    assert_eq!(
        after.reserved_read_bytes - before.reserved_read_bytes,
        step.usage.read_bytes
    );
    assert_eq!(
        after.reserved_spill_bytes - before.reserved_spill_bytes,
        step.usage.spill_bytes
    );
    assert!(step.usage.work <= 8 && step.usage.read_bytes <= 127 && step.usage.spill_bytes <= 127);
    if !step.ready_for_input && !step.ordering_complete {
        assert!(step.usage.work > 0);
    }
    step
}

#[test]
fn tiny_quanta_resume_heap_merge_and_reduction_without_replaying_or_publishing() {
    for buffer in [1, 4, 128] {
        let (root, reader) = fixture("ordering-quanta");
        let mut writer = SearchOutOfCoreGenerationWriter::prepare_unordered_delta(
            &reader,
            Default::default(),
            limits(buffer),
            RuntimeTaskContext::default(),
        )
        .unwrap();
        let original = reader.generation();
        let mut expected: BTreeMap<String, String> = [("a", "old"), ("k", "keep")]
            .map(|(a, b)| (a.into(), b.into()))
            .into();
        let mut steps = 0;
        let mut inputs = 0;
        for index in (0..48).rev() {
            let id = format!("n{index:02}");
            for deleted in [false, true, false] {
                assert!(writer.ready_for_input());
                let before = writer.report();
                if deleted {
                    writer.queue_delete(&id).unwrap();
                } else {
                    writer.queue_upsert(document(&id, "final")).unwrap();
                }
                inputs += 1;
                expected.insert(id.clone(), "final".into());
                let queued = writer.report();
                assert_eq!(queued.sort_work, before.sort_work);
                assert_eq!(queued.reserved_read_bytes, before.reserved_read_bytes);
                assert_eq!(queued.reserved_spill_bytes, before.reserved_spill_bytes);
                while !writer.ready_for_input() {
                    advance(&mut writer);
                    steps += 1;
                }
            }
        }
        writer.queue_delete("b").unwrap();
        inputs += 1;
        // Closing intake while the last input is still pending must preserve it.
        writer.finish_input().unwrap();
        while !writer.ordering_complete() {
            advance(&mut writer);
            steps += 1;
            assert!(steps < 100000);
        }
        assert!(steps > 100);
        assert_eq!(writer.report().input_operations, inputs);
        assert_eq!(writer.report().output_operations, 49);
        let done = advance(&mut writer);
        assert_eq!(done.usage, Default::default());
        assert_eq!(
            SearchOutOfCoreReader::open(&root).unwrap().generation(),
            original
        );
        let (_, candidate) = writer.prepare_publication().unwrap();
        assert_eq!(
            SearchOutOfCoreReader::open(&root).unwrap().generation(),
            original
        );
        candidate.publish().unwrap();
        let current = SearchOutOfCoreReader::open(&root).unwrap();
        let ids: Vec<_> = expected.keys().cloned().collect();
        let actual: BTreeMap<_, _> = current
            .hydrate_documents(&ids)
            .unwrap()
            .documents
            .into_iter()
            .map(|doc| (doc.id, doc.content))
            .collect();
        assert_eq!(actual, expected);
        assert_eq!(current.document_count(), expected.len());
        drop(current);
        drop(reader);
        std::fs::remove_dir_all(root).unwrap();
    }
}

#[test]
fn impossible_quantum_and_cumulative_work_exhaustion_are_sticky() {
    for cumulative in [false, true] {
        let (root, reader) = fixture("ordering-step-refusal");
        let mut caps = limits(1);
        if cumulative {
            caps.max_sort_work = NonZeroUsize::new(2).unwrap();
        }
        let mut writer = SearchOutOfCoreGenerationWriter::prepare_unordered_delta(
            &reader,
            Default::default(),
            caps,
            RuntimeTaskContext::default(),
        )
        .unwrap();
        let memory = writer.inner.writer.memory.clone();
        writer
            .queue_upsert(document("z", &"x".repeat(100)))
            .unwrap();
        writer.finish_input().unwrap();
        // One encoded record exceeds this entire spill quantum. A refusal,
        // rather than an endless zero-progress yield, must be observable.
        let mut refused = false;
        for _ in 0..100 {
            if writer.advance_ordering(quantum()).is_err() {
                refused = true;
                break;
            }
        }
        assert!(refused);
        assert!(writer.advance_ordering(unlimited_step()).is_err());
        assert!(writer.prepare_publication().is_err());
        assert_eq!(memory.ledger.snapshot().used_bytes, 0);
        assert_eq!(
            SearchOutOfCoreReader::open(&root).unwrap().generation(),
            reader.generation()
        );
        drop(reader);
        std::fs::remove_dir_all(root).unwrap();
    }
}

#[test]
fn busy_or_closed_intake_cannot_silently_lose_a_document() {
    for closed in [false, true] {
        let (root, reader) = fixture("ordering-busy");
        let mut writer = SearchOutOfCoreGenerationWriter::prepare_unordered_delta(
            &reader,
            Default::default(),
            limits(1),
            RuntimeTaskContext::default(),
        )
        .unwrap();
        writer.queue_upsert(document("z", "new")).unwrap();
        if closed {
            writer.finish_input().unwrap();
        } else {
            writer.queue_delete("b").unwrap();
            assert!(!writer.ready_for_input());
        }
        assert!(writer.queue_upsert(document("lost", "ignored")).is_err());
        assert!(writer.prepare_publication().is_err());
        assert_eq!(
            SearchOutOfCoreReader::open(&root).unwrap().generation(),
            reader.generation()
        );
        drop(reader);
        std::fs::remove_dir_all(root).unwrap();
    }
}

#[test]
fn cancelled_pending_and_completed_ordering_release_all_owned_files_and_memory() {
    for completed in [false, true] {
        let (root, reader) = fixture("ordering-cancel");
        let task = RuntimeTaskContext::default();
        let mut writer = SearchOutOfCoreGenerationWriter::prepare_unordered_delta(
            &reader,
            Default::default(),
            limits(1),
            task.clone(),
        )
        .unwrap();
        let memory = writer.inner.writer.memory.clone();
        writer.queue_upsert(document("z", "new")).unwrap();
        writer.queue_delete("b").unwrap();
        if completed {
            writer.finish_input().unwrap();
            while !writer.ordering_complete() {
                advance(&mut writer);
            }
        } else {
            advance(&mut writer);
        }
        task.cancellation().cancel();
        assert!(writer.advance_ordering(quantum()).is_err());
        assert!(writer.prepare_publication().is_err());
        assert_eq!(memory.ledger.snapshot().used_bytes, 0);
        assert!(!std::fs::read_dir(&root).unwrap().any(|e| e
            .unwrap()
            .file_type()
            .unwrap()
            .is_dir()));
        assert_eq!(
            SearchOutOfCoreReader::open(&root).unwrap().generation(),
            reader.generation()
        );
        drop(reader);
        std::fs::remove_dir_all(root).unwrap();
    }
}
