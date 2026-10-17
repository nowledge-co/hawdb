// Copyright 2026 Nowledge
// Licensed under the Apache License, Version 2.0.

use super::*;
use std::collections::BTreeMap;
pub(super) fn document(id: &str, content: &str) -> SearchDocument {
    SearchDocument {
        id: id.into(),
        title: id.into(),
        content: content.into(),
        embedding: None,
        metadata: BTreeMap::new(),
    }
}
pub(super) fn fixture(name: &str) -> (std::path::PathBuf, SearchOutOfCoreReader) {
    let root = super::super::super::tests::test_dir(name);
    let mut writer = SearchOutOfCoreGenerationWriter::create(&root, Default::default()).unwrap();
    for (id, body) in [("a", "old"), ("b", "removed"), ("k", "keep")] {
        writer.push(document(id, body)).unwrap();
    }
    writer.finish().unwrap();
    let reader = SearchOutOfCoreReader::open(&root).unwrap();
    (root, reader)
}
pub(super) fn limits(records: usize) -> SearchUnorderedMutationLimits {
    SearchUnorderedMutationLimits {
        max_buffer_records: NonZeroUsize::new(records).unwrap(),
        ..Default::default()
    }
}
#[test]
fn unordered_runs_coalesce_snapshot_upserts_and_keep_the_original_base_fence() {
    for size in [1, 2, 10] {
        let (root, reader) = fixture(&format!("unordered-{size}"));
        let original = reader.generation();
        let mut update = SearchOutOfCoreGenerationWriter::prepare_unordered_delta(
            &reader,
            SearchOutOfCoreGenerationBuildOptions {
                source_graph_commit_epoch: Some(42),
                ..Default::default()
            },
            limits(size),
            RuntimeTaskContext::default(),
        )
        .unwrap();
        update.upsert(document("z", "zee")).unwrap();
        update.delete("b").unwrap();
        update.upsert(document("a", "new")).unwrap();
        update.delete("a").unwrap();
        update.upsert(document("z", "zee")).unwrap();
        update.delete("b").unwrap();
        update.upsert(document("c", "cee")).unwrap();
        let (sorting, candidate) = update.prepare_publication().unwrap();
        assert_eq!(sorting.input_operations, 7);
        assert_eq!(sorting.output_operations, 4);
        assert_eq!(candidate.expected_active_generation(), Some(original));
        assert_eq!(
            SearchOutOfCoreReader::open(&root).unwrap().generation(),
            original
        );
        let (delta, _, _) = candidate.publish().unwrap();
        assert_eq!(delta.after_document_count, 4);
        let current = SearchOutOfCoreReader::open(&root).unwrap();
        assert_eq!(current.source_graph_commit_epoch(), Some(42));
        let output = current
            .hydrate_documents(&["a".into(), "c".into(), "k".into(), "z".into()])
            .unwrap();
        assert_eq!(
            output
                .documents
                .iter()
                .map(|doc| doc.content.as_str())
                .collect::<Vec<_>>(),
            ["new", "cee", "keep", "zee"]
        );
        assert_eq!(current.document_count(), 4);
        drop(current);
        drop(reader);
        std::fs::remove_dir_all(root).unwrap();
    }
}
#[test]
fn conflict_admission_and_ignored_errors_cannot_return_a_candidate() {
    for case in [
        "conflict", "input", "read", "spill", "visits", "buffer", "ignored",
    ] {
        let (root, reader) = fixture(case);
        let original = reader.generation();
        let mut caps = limits(1);
        match case {
            "input" => caps.max_input_operations = NonZeroUsize::new(1).unwrap(),
            "read" => caps.max_read_bytes = NonZeroU64::new(8).unwrap(),
            "spill" => caps.max_spill_bytes = NonZeroU64::new(8).unwrap(),
            "visits" => caps.max_merge_visits = NonZeroUsize::new(1).unwrap(),
            "buffer" => caps.max_buffer_payload_bytes = NonZeroUsize::new(1).unwrap(),
            _ => {}
        }
        let mut update = SearchOutOfCoreGenerationWriter::prepare_unordered_delta(
            &reader,
            Default::default(),
            caps,
            RuntimeTaskContext::default(),
        )
        .unwrap();
        let first = update.upsert(document("a", "new"));
        let second = if case == "ignored" {
            update.delete("")
        } else {
            update.upsert(document(
                "a",
                if case == "conflict" {
                    "different"
                } else {
                    "new"
                },
            ))
        };
        if first.is_err() || second.is_err() {
            assert!(update.delete("valid").is_err());
        }
        assert!(update.prepare_publication().is_err(), "case={case}");
        assert_eq!(
            SearchOutOfCoreReader::open(&root).unwrap().generation(),
            original
        );
        assert!(!std::fs::read_dir(&root).unwrap().any(|entry| entry
            .unwrap()
            .file_type()
            .unwrap()
            .is_dir()));
        drop(reader);
        std::fs::remove_dir_all(root).unwrap();
    }
}
#[test]
fn truncation_and_mutated_runs_fail_before_visibility() {
    for truncate in [false, true] {
        let (root, reader) = fixture("unordered-corruption");
        let original = reader.generation();
        let mut update = SearchOutOfCoreGenerationWriter::prepare_unordered_delta(
            &reader,
            Default::default(),
            limits(1),
            RuntimeTaskContext::default(),
        )
        .unwrap();
        update.upsert(document("c", "cee")).unwrap();
        update.flush().unwrap();
        let path = &update.levels[0].as_ref().unwrap().path_for_test();
        let mut bytes = std::fs::read(path).unwrap();
        if truncate {
            bytes.pop();
        } else {
            let index = bytes.len() - 2;
            bytes[index] ^= 1;
        }
        std::fs::write(path, bytes).unwrap();
        assert!(update.prepare_publication().is_err());
        assert_eq!(
            SearchOutOfCoreReader::open(&root).unwrap().generation(),
            original
        );
        drop(reader);
        std::fs::remove_dir_all(root).unwrap();
    }
}

#[test]
fn equal_crc_and_length_do_not_coalesce_different_documents() {
    use hawdb_integrity::Crc32cHasher;
    let (root, reader) = fixture("unordered-crc-collision");
    // Fixed collision discovered independently over the complete wire grammar.
    // A digest-only reducer silently accepts this pair as the same upsert.
    let a = document("a", "487a6ddf463ff161");
    let b = document("a", "301516450b94bbce");
    let encoded = [&a, &b].map(|doc| DocumentEncoding::new(doc).unwrap().encode().unwrap());
    assert_ne!(encoded[0], encoded[1]);
    assert_eq!(encoded[0].len(), encoded[1].len());
    let checksums = encoded.map(|wire| {
        let mut digest = Crc32cHasher::new();
        digest.update(wire.as_bytes());
        digest.finish()
    });
    assert_eq!(checksums, [0xcf846bec, 0xcf846bec]);
    let mut update = SearchOutOfCoreGenerationWriter::prepare_unordered_delta(
        &reader,
        Default::default(),
        limits(1),
        RuntimeTaskContext::default(),
    )
    .unwrap();
    update.upsert(a).unwrap();
    update.upsert(b).unwrap();
    let error = update.prepare_publication().err().unwrap();
    assert!(error.to_string().contains("conflicting upserts"));
    assert_eq!(
        SearchOutOfCoreReader::open(&root).unwrap().generation(),
        reader.generation()
    );
    drop(reader);
    std::fs::remove_dir_all(root).unwrap();
}

#[test]
fn permutations_and_spill_boundaries_match_the_fixed_snapshot_oracle() {
    for seed in 1..=12u64 {
        for size in [1, 3, 9] {
            let (root, reader) = fixture("unordered-oracle");
            // None is a retract intent. Every Some is the final source-E
            // document, so a final row dominates any older retract intent.
            let mut intents = [
                ("a", None),
                ("a", Some("final")),
                ("b", None),
                ("z", Some("中文")),
                ("z", Some("中文")),
                ("b", None),
                ("c", Some("")),
                ("z", None),
                ("a", Some("final")),
            ];
            let mut random = seed;
            for index in (1..intents.len()).rev() {
                random = random.wrapping_mul(6364136223846793005).wrapping_add(1);
                intents.swap(index, (random >> 32) as usize % (index + 1));
            }
            let mut oracle = BTreeMap::from([("a", "old"), ("b", "removed"), ("k", "keep")]);
            for (id, _) in intents {
                oracle.remove(id);
            }
            for (id, body) in intents {
                if let Some(body) = body {
                    oracle.insert(id, body);
                }
            }
            let mut update = SearchOutOfCoreGenerationWriter::prepare_unordered_delta(
                &reader,
                Default::default(),
                limits(size),
                RuntimeTaskContext::default(),
            )
            .unwrap();
            for (id, body) in intents {
                match body {
                    Some(body) => update.upsert(document(id, body)).unwrap(),
                    None => update.delete(id).unwrap(),
                }
            }
            let (report, candidate) = update.prepare_publication().unwrap();
            assert_eq!(report.input_operations, intents.len());
            assert_eq!(report.output_operations, 4);
            candidate.publish().unwrap();
            let current = SearchOutOfCoreReader::open(&root).unwrap();
            let ids: Vec<_> = oracle.keys().map(|id| (*id).to_string()).collect();
            let output = current.hydrate_documents(&ids).unwrap();
            let actual: BTreeMap<_, _> = output
                .documents
                .iter()
                .map(|doc| (doc.id.as_str(), doc.content.as_str()))
                .collect();
            assert_eq!(actual, oracle, "seed={seed} buffer={size}");
            assert_eq!(current.document_count(), oracle.len());
            assert!(current.hydrate_documents(&["b".into()]).is_err());
            drop(current);
            drop(reader);
            std::fs::remove_dir_all(root).unwrap();
        }
    }
}

#[test]
fn cancellation_releases_buffered_spilled_and_prepared_ownership() {
    for phase in ["buffered", "spilled", "prepared"] {
        let (root, reader) = fixture("unordered-cancel");
        let task = RuntimeTaskContext::default();
        let mut update = SearchOutOfCoreGenerationWriter::prepare_unordered_delta(
            &reader,
            Default::default(),
            limits(1),
            task.clone(),
        )
        .unwrap();
        let memory = update.inner.writer.memory.clone();
        update.upsert(document("z", "new")).unwrap();
        if phase != "buffered" {
            update.upsert(document("a", "changed")).unwrap();
            assert!(update.report().runs_created > 0);
        }
        if phase == "prepared" {
            let (_, candidate) = update.prepare_publication().unwrap();
            task.cancellation().cancel();
            assert!(candidate.publish().is_err());
        } else {
            task.cancellation().cancel();
            assert!(update.delete("b").is_err());
            assert!(update.prepare_publication().is_err());
        }
        assert_eq!(memory.ledger.snapshot().used_bytes, 0, "phase={phase}");
        assert_eq!(
            SearchOutOfCoreReader::open(&root).unwrap().generation(),
            reader.generation()
        );
        assert!(!std::fs::read_dir(&root).unwrap().any(|e| e
            .unwrap()
            .file_type()
            .unwrap()
            .is_dir()));
        drop(reader);
        std::fs::remove_dir_all(root).unwrap();
    }
}

#[test]
fn competing_publication_rejects_the_original_base_after_sorting() {
    let (root, reader) = fixture("unordered-stale-base");
    let prepare = |body| {
        let mut update = SearchOutOfCoreGenerationWriter::prepare_unordered_delta(
            &reader,
            Default::default(),
            limits(1),
            RuntimeTaskContext::default(),
        )
        .unwrap();
        update.upsert(document("a", body)).unwrap();
        update.prepare_publication().unwrap().1
    };
    let stale = prepare("stale");
    prepare("winner").publish().unwrap();
    let winner = SearchOutOfCoreReader::open(&root).unwrap();
    assert!(stale.publish().is_err());
    let current = SearchOutOfCoreReader::open(&root).unwrap();
    assert_eq!(current.generation(), winner.generation());
    assert_eq!(
        current.hydrate_documents(&["a".into()]).unwrap().documents[0].content,
        "winner"
    );
    drop(current);
    drop(winner);
    drop(reader);
    std::fs::remove_dir_all(root).unwrap();
}

#[test]
fn spill_creation_io_failure_is_sticky_and_refunds_the_shared_ledger() {
    let (root, reader) = fixture("unordered-create-failure");
    let mut update = SearchOutOfCoreGenerationWriter::prepare_unordered_delta(
        &reader,
        Default::default(),
        limits(1),
        RuntimeTaskContext::default(),
    )
    .unwrap();
    let memory = update.inner.writer.memory.clone();
    update.upsert(document("a", "new")).unwrap();
    // create_new must fail before the first buffer can be written. This is a
    // real filesystem refusal, rather than an admission error pretending to be IO.
    let collision = update.inner.writer.stage.path.join("unordered-0.run");
    std::fs::write(collision, b"occupied").unwrap();
    assert!(update.delete("b").is_err());
    assert!(update.upsert(document("c", "ignored-error")).is_err());
    assert!(update.prepare_publication().is_err());
    assert_eq!(memory.ledger.snapshot().used_bytes, 0);
    assert_eq!(
        SearchOutOfCoreReader::open(&root).unwrap().generation(),
        reader.generation()
    );
    assert!(!std::fs::read_dir(&root)
        .unwrap()
        .any(|e| e.unwrap().file_type().unwrap().is_dir()));
    drop(reader);
    std::fs::remove_dir_all(root).unwrap();
}

#[test]
fn payload_buffer_does_not_bypass_the_shared_task_memory_reservation() {
    use hawdb_core::RuntimeMemoryReservation;
    let (root, reader) = fixture("unordered-task-memory");
    let mut update = SearchOutOfCoreGenerationWriter::prepare_unordered_delta(
        &reader,
        Default::default(),
        limits(16),
        RuntimeTaskContext::default()
            .with_memory_reservation(RuntimeMemoryReservation::new(4 * 1024 * 1024, 0)),
    )
    .unwrap();
    let memory = update.inner.writer.memory.clone();
    // Fits the explicit 16 MiB payload buffer, but exceeds the task's shared
    // input/spool/metadata ledger. Caller-owned construction precedes this API.
    assert!(update
        .upsert(document("a", &"x".repeat(8 * 1024 * 1024)))
        .is_err());
    assert!(update.prepare_publication().is_err());
    assert_eq!(memory.ledger.snapshot().used_bytes, 0);
    assert_eq!(
        SearchOutOfCoreReader::open(&root).unwrap().generation(),
        reader.generation()
    );
    drop(reader);
    std::fs::remove_dir_all(root).unwrap();
}
