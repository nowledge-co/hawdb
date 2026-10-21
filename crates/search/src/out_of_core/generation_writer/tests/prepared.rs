// Copyright 2026 Nowledge
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use super::*;
use crate::SearchOutOfCoreReader;
use hawdb_core::RuntimeCancellationToken;

fn visible_files(root: &Path) -> BTreeMap<String, Vec<u8>> {
    fs::read_dir(root)
        .unwrap()
        .map(|entry| entry.unwrap())
        .filter(|entry| entry.file_type().unwrap().is_file())
        .map(|entry| {
            (
                entry.file_name().into_string().unwrap(),
                fs::read(entry.path()).unwrap(),
            )
        })
        .collect()
}

fn capture(
    root: &Path,
    partitioned: bool,
    task: RuntimeTaskContext,
) -> SearchOutOfCoreGenerationWriter {
    let mut writer = SearchOutOfCoreGenerationWriter::create_with_context(
        root,
        SearchOutOfCoreGenerationBuildOptions {
            max_content_documents: NonZeroUsize::new(if partitioned { 1 } else { 100 }).unwrap(),
            source_graph_commit_epoch: Some(7),
            ..Default::default()
        },
        task,
    )
    .unwrap();
    writer.push(document(0)).unwrap();
    writer.push(document(1)).unwrap();
    writer
}

fn projection_row(number: usize) -> SearchProjectionRow {
    let source = document(number);
    SearchProjectionRow {
        kind: SearchProjectionKind::Memory,
        external_id: format!("{number:06}"),
        title: source.title,
        body: source.content,
        embedding: source.embedding,
        source_id: None,
        metadata: source.metadata,
    }
}

fn baseline(root: &Path) -> SearchOutOfCoreGenerationBuildReport {
    let mut writer = SearchOutOfCoreGenerationWriter::create(root, Default::default()).unwrap();
    writer.push(document(99)).unwrap();
    writer.finish().unwrap()
}

#[test]
fn prepared_single_and_partitioned_candidates_are_invisible_until_activation() {
    for partitioned in [false, true] {
        let root = test_dir("prepared_invisible");
        let initial = baseline(&root);
        let before = visible_files(&root);
        let writer = capture(&root, partitioned, RuntimeTaskContext::default());
        let memory = writer.memory.clone();
        let candidate = writer.prepare_publication().unwrap();
        assert_eq!(
            candidate.expected_active_generation(),
            Some(initial.generation)
        );
        assert_eq!(candidate.source_graph_commit_epoch(), Some(7));
        assert_eq!(visible_files(&root), before);
        assert_eq!(stage_directories(&root), 1);
        let reader = SearchOutOfCoreReader::open(&root).unwrap();
        assert_eq!(reader.document_count(), 1);
        assert_eq!(
            reader
                .hydrate_documents(&[document(99).id])
                .unwrap()
                .documents,
            vec![document(99)]
        );
        drop(reader);
        // A queued candidate retains resources but no publication lock.
        drop(super::super::super::SearchProjectionPublishLease::acquire(&root).unwrap());
        assert!(memory.ledger.snapshot().used_bytes > 0);
        let report = candidate.publish().unwrap();
        assert_eq!(
            report.published_content_segments,
            if partitioned { 2 } else { 1 }
        );
        assert_eq!(memory.ledger.snapshot().used_bytes, 0);
        assert_eq!(stage_directories(&root), 0);
        let reader = SearchOutOfCoreReader::open(&root).unwrap();
        assert_eq!(reader.source_graph_commit_epoch(), Some(7));
        assert_eq!(
            reader
                .hydrate_documents(&[document(0).id, document(1).id])
                .unwrap()
                .documents,
            vec![document(0), document(1)]
        );
        drop(reader);
        fs::remove_dir_all(root).unwrap();
    }
}

#[test]
fn dropping_or_cancelling_queued_candidates_releases_stages_without_visibility() {
    for partitioned in [false, true] {
        for cancel in [false, true] {
            let root = test_dir("prepared_cancel_drop");
            baseline(&root);
            let before = visible_files(&root);
            let cancellation = RuntimeCancellationToken::new();
            let writer = capture(
                &root,
                partitioned,
                RuntimeTaskContext::without_deadline(cancellation.clone()),
            );
            let memory = writer.memory.clone();
            let candidate = writer.prepare_publication().unwrap();
            if cancel {
                cancellation.cancel();
                assert!(candidate.publish().is_err());
            } else {
                drop(candidate);
            }
            assert_eq!(visible_files(&root), before);
            assert_eq!(stage_directories(&root), 0);
            assert_eq!(memory.ledger.snapshot().used_bytes, 0);
            fs::remove_dir_all(root).unwrap();
        }
    }
}

#[test]
fn competing_publisher_commits_while_artifact_preparation_is_paused() {
    let root = test_dir("prepared_interleaving");
    let writer = capture(&root, false, RuntimeTaskContext::default());
    let memory = writer.memory.clone();
    let candidate = writer
        .prepare_publication_with_artifacts(
            |writer, source, generation| {
                // This callback runs after base capture and before expensive build.
                // Join proves the other operation commits before preparation resumes.
                let competing_root = root.clone();
                let winner = std::thread::spawn(move || baseline(&competing_root))
                    .join()
                    .unwrap();
                assert_eq!(winner.generation, generation);
                writer.build_artifacts(source, generation)
            },
            None,
            false,
        )
        .unwrap();
    let before = visible_files(&root);
    assert!(matches!(
        candidate.publish(),
        Err(HawDBError::TransactionConflict { .. })
    ));
    assert_eq!(visible_files(&root), before);
    assert_eq!(stage_directories(&root), 0);
    assert_eq!(memory.ledger.snapshot().used_bytes, 0);
    let reader = SearchOutOfCoreReader::open(&root).unwrap();
    assert_eq!(
        reader
            .hydrate_documents(&[document(99).id])
            .unwrap()
            .documents,
        vec![document(99)]
    );
    drop(reader);
    fs::remove_dir_all(root).unwrap();
}

#[test]
fn stale_partitioned_candidate_cannot_overwrite_a_competing_generation() {
    let root = test_dir("prepared_partitioned_stale");
    let first = capture(&root, true, RuntimeTaskContext::default())
        .prepare_publication()
        .unwrap();
    let second = capture(&root, true, RuntimeTaskContext::default())
        .prepare_publication()
        .unwrap();
    assert_eq!(first.expected_active_generation(), None);
    let winner = second.publish().unwrap();
    let before = visible_files(&root);
    assert!(matches!(
        first.publish(),
        Err(HawDBError::TransactionConflict { .. })
    ));
    assert_eq!(visible_files(&root), before);
    assert_eq!(stage_directories(&root), 0);
    let reader = SearchOutOfCoreReader::open(&root).unwrap();
    assert_eq!(reader.generation(), winner.generation);
    assert_eq!(reader.document_count(), 2);
    drop(reader);
    fs::remove_dir_all(root).unwrap();
}

#[test]
fn incremental_prepared_update_preserves_report_and_rejects_stale_source_base() {
    for partitioned in [false, true] {
        let root = test_dir("prepared_incremental");
        let initial = baseline(&root);
        let reader = SearchOutOfCoreReader::open(&root).unwrap();
        let prepare = || {
            SearchOutOfCoreGenerationWriter::prepare_delta(
                &reader,
                SearchProjectionDelta {
                    upserts: (0..2).map(projection_row).collect(),
                    deletes: vec![document(99).id],
                    source_graph_commit_epoch: Some(8),
                    ..Default::default()
                },
                SearchOutOfCoreGenerationBuildOptions {
                    max_content_documents: NonZeroUsize::new(if partitioned { 1 } else { 100 })
                        .unwrap(),
                    ..Default::default()
                },
            )
            .unwrap()
            .prepare_publication()
            .unwrap()
        };
        let candidate = prepare();
        let stale = prepare();
        assert_eq!(
            candidate.expected_active_generation(),
            Some(initial.generation)
        );
        assert_eq!(candidate.source_graph_commit_epoch(), Some(8));
        // Original selector and reader are complete throughout preparation.
        assert_eq!(
            SearchOutOfCoreReader::open(&root).unwrap().document_count(),
            1
        );
        let (delta, report, _) = candidate.publish().unwrap();
        assert_eq!(delta.upserted_documents, 2);
        assert_eq!(delta.deleted_documents, 1);
        assert_eq!(report.source_graph_commit_epoch, Some(8));
        let before = visible_files(&root);
        assert!(matches!(
            stale.publish(),
            Err(HawDBError::TransactionConflict { .. })
        ));
        assert_eq!(visible_files(&root), before);
        drop(reader);
        let reader = SearchOutOfCoreReader::open(&root).unwrap();
        assert_eq!(reader.document_count(), 2);
        assert_eq!(
            reader
                .hydrate_documents(&[document(0).id, document(1).id])
                .unwrap()
                .documents,
            vec![
                projection_row(0).into_document(),
                projection_row(1).into_document()
            ]
        );
        drop(reader);
        assert_eq!(stage_directories(&root), 0);
        fs::remove_dir_all(root).unwrap();
    }
}

#[test]
fn queued_candidates_retain_governor_admission_through_publish_or_drop() {
    use crate::SearchGenerationAdmission;
    use hawdb_qos::{
        IoConcurrencyBudget, RuntimeGovernor, RuntimeGovernorConfig, RuntimeWorkRequest,
    };
    for partitioned in [false, true] {
        for publish in [false, true] {
            let root = test_dir("prepared_governed");
            let governor = RuntimeGovernor::detect(
                RuntimeGovernorConfig {
                    memory_budget_bytes: Some(64 * 1024 * 1024),
                    background_task_limit: NonZeroUsize::new(1),
                    ..Default::default()
                },
                IoConcurrencyBudget::new(2, 1),
            );
            let request = RuntimeWorkRequest::background_maintenance(16 * 1024 * 1024);
            let admission = SearchGenerationAdmission::acquire(&governor, request).unwrap();
            let mut writer = admission
                .create_writer(
                    &root,
                    SearchOutOfCoreGenerationBuildOptions {
                        max_content_documents: NonZeroUsize::new(if partitioned { 1 } else { 100 })
                            .unwrap(),
                        ..Default::default()
                    },
                )
                .unwrap();
            writer.writer_mut().push(document(0)).unwrap();
            writer.writer_mut().push(document(1)).unwrap();
            let candidate = writer.prepare_publication().unwrap();
            assert!(SearchGenerationAdmission::acquire(&governor, request).is_err());
            if publish {
                candidate.publish().unwrap();
            } else {
                drop(candidate);
            }
            drop(SearchGenerationAdmission::acquire(&governor, request).unwrap());
            assert_eq!(stage_directories(&root), 0);
            fs::remove_dir_all(root).unwrap();
        }
    }
}

#[test]
fn selector_decode_refusal_does_not_become_a_damaged_replacement_base() {
    let root = test_dir("prepared_base_decode_refusal");
    baseline(&root);
    let path = root.join(OUT_OF_CORE_MANIFEST_FILE);
    let mut manifest: serde_json::Value =
        serde_json::from_slice(&fs::read(&path).unwrap()).unwrap();
    let segment = manifest["body"]["segments"][0].clone();
    manifest["body"]["segments"] = serde_json::Value::Array(vec![segment; 385]);
    let bytes = serde_json::to_vec(&manifest).unwrap();
    assert!(bytes.len() as u64 <= super::super::super::MAX_OUT_OF_CORE_MANIFEST_BYTES);
    fs::write(&path, &bytes).unwrap();
    let decode_bytes = crate::out_of_core::SearchOutOfCoreManifestBody::decode_capacity(
        &bytes,
        &RuntimeTaskContext::default(),
    )
    .unwrap();
    let task = RuntimeTaskContext::default().with_memory_reservation(
        hawdb_core::RuntimeMemoryReservation::new(decode_bytes as u64, 0),
    );
    let writer =
        SearchOutOfCoreGenerationWriter::create_with_context(&root, Default::default(), task)
            .unwrap();
    let memory = writer.memory.clone();
    let retained = memory.ledger.snapshot().used_bytes;
    // Raw retention fits, but decoding does not. Falling back here would turn
    // an unvalidated selector into permission to rebuild a damaged base.
    let io = super::super::io::GenerationIo::new(&memory, &writer.task_context);
    assert_eq!(
        io.read(&path, super::super::super::MAX_OUT_OF_CORE_MANIFEST_BYTES)
            .unwrap()
            .bytes,
        bytes
    );
    let expected = super::super::discovery::active_identity(&root, &memory, &writer.task_context)
        .err()
        .expect("the selector decode must exceed admission");
    let HawDBError::ResourceBudgetExceeded(expected) = expected else {
        panic!("expected a typed decode refusal, got {expected:?}");
    };
    assert_eq!(
        expected.resource,
        hawdb_core::ResourceBudgetKind::MemoryBytes
    );
    assert_eq!(expected.requested, decode_bytes as u64);
    let error = super::super::prepared::CapturedBase::capture(&writer)
        .err()
        .expect("budget refusal must not authorize damaged-base recovery");
    assert!(matches!(&error, HawDBError::ResourceBudgetExceeded(cause) if cause == &expected));
    assert_eq!(memory.ledger.snapshot().used_bytes, retained);
    assert_eq!(fs::read(&path).unwrap(), bytes);
    drop(writer);
    assert_eq!(memory.ledger.snapshot().used_bytes, 0);
    assert_eq!(stage_directories(&root), 0);
    fs::remove_dir_all(root).unwrap();
}

#[test]
fn damaged_selector_replacement_rechecks_exact_bytes_before_installation() {
    for change_base in [false, true] {
        let root = test_dir("prepared_damaged_base");
        baseline(&root);
        fs::write(root.join(OUT_OF_CORE_MANIFEST_FILE), b"damaged selector A").unwrap();
        let candidate = capture(&root, false, RuntimeTaskContext::default())
            .prepare_publication()
            .unwrap();
        if change_base {
            fs::write(root.join(OUT_OF_CORE_MANIFEST_FILE), b"damaged selector B").unwrap();
            let before = visible_files(&root);
            assert!(matches!(
                candidate.publish(),
                Err(HawDBError::StorageIntegrity(_))
            ));
            assert_eq!(visible_files(&root), before);
        } else {
            candidate.publish().unwrap();
            assert_eq!(
                SearchOutOfCoreReader::open(&root).unwrap().document_count(),
                2
            );
        }
        assert_eq!(stage_directories(&root), 0);
        fs::remove_dir_all(root).unwrap();
    }
}

#[cfg(feature = "vector-search")]
#[test]
fn stale_cleanup_preserves_the_current_vector_generation() {
    let root = test_dir("stale_cleanup_current_vectors");
    let stale = crate::SearchIndex::open(&root).unwrap();
    stale.checkpoint().unwrap();
    let mut writer = SearchOutOfCoreGenerationWriter::create(
        &root,
        SearchOutOfCoreGenerationBuildOptions {
            embedding_manifest: Some(SearchEmbeddingManifest {
                model: "test-model".into(),
                version: Some("v1".into()),
                dimension: 2,
            }),
            ..Default::default()
        },
    )
    .unwrap();
    writer.push(document(0)).unwrap();
    let report = writer.finish().unwrap();
    assert!(report.rabitq_artifact_bytes > 0);
    let artifact = root.join(format!("search_rabitq.{}.hawdb", report.generation));
    assert!(artifact.exists());
    let cleanup = stale.retry_projection_cleanup(Default::default());
    assert!(
        artifact.exists(),
        "stale cleanup removed current vector artifact: {cleanup:?}"
    );
    let reader = SearchOutOfCoreReader::open(&root).unwrap();
    assert_eq!(reader.generation(), report.generation);
    assert_eq!(reader.document_count(), 1);
    drop(reader);
    drop(stale);
    fs::remove_dir_all(root).unwrap();
}

#[test]
fn cleanup_defers_without_touching_artifacts_while_publication_owns_the_root() {
    let root = test_dir("cleanup_publication_exclusion");
    let index = crate::SearchIndex::open(&root).unwrap();
    assert!(
        !index.projection_cleanup_report().retry_required,
        "open already owns publication authority"
    );
    index.checkpoint().unwrap();
    let artifact = root.join("search_rabitq.777.hawdb");
    fs::write(&artifact, b"unselected candidate artifact").unwrap();
    let lease = crate::out_of_core::SearchProjectionPublishLease::acquire(&root).unwrap();
    let cleanup = index.retry_projection_cleanup(Default::default());
    assert!(artifact.exists());
    assert_eq!(cleanup.scanned_entries, 0);
    assert_eq!(cleanup.delete_attempts, 0);
    assert!(cleanup.retry_required);
    assert_eq!(
        cleanup
            .failure_kinds
            .get("publication_exclusion_unavailable"),
        Some(&1)
    );
    drop(lease);
    let cleanup = index.retry_projection_cleanup(Default::default());
    assert!(!artifact.exists());
    assert!(!cleanup.retry_required);
    drop(index);
    fs::remove_dir_all(root).unwrap();
}

#[cfg(feature = "vector-search")]
#[test]
fn stale_cleanup_preserves_a_peers_standalone_resident_vectors() {
    let root = test_dir("stale_cleanup_resident_vectors");
    let stale = crate::SearchIndex::open(&root).unwrap();
    stale.checkpoint().unwrap();
    let mut peer = crate::SearchIndex::open(&root).unwrap();
    peer.upsert(document(0)).unwrap();
    peer.checkpoint().unwrap();
    let artifact = crate::latest_rabitq_artifact(&root).unwrap().unwrap().1;
    assert!(artifact.exists());
    stale.retry_projection_cleanup(Default::default());
    assert!(artifact.exists());
    drop(peer);
    let reopened = crate::SearchIndex::open(&root).unwrap();
    assert!(reopened.rabitq_projection().is_some());
    drop(reopened);
    drop(stale);
    fs::remove_dir_all(root).unwrap();
}

fn reject_changed_installed_payload(prefix: &'static str) {
    let root = test_dir("prepared_changed_payload");
    let initial = baseline(&root);
    let old_head = fs::read(root.join(OUT_OF_CORE_MANIFEST_FILE)).unwrap();
    let candidate = capture(&root, false, RuntimeTaskContext::default())
        .prepare_publication()
        .unwrap();
    let name = format!("{prefix}{}.hawdb", initial.generation + 1);
    publication::AFTER_CONTENT_INSTALL.with(|hook| {
        *hook.borrow_mut() = Some(Box::new(move |root| {
            let path = root.join(name);
            let mut bytes = fs::read(&path).unwrap();
            assert!(!bytes.is_empty(), "fixture must exercise nonempty payload");
            let last = bytes.len() - 1;
            bytes[last] ^= 1;
            fs::write(path, bytes).unwrap();
        }));
    });
    let result = candidate.publish();
    assert!(publication::AFTER_CONTENT_INSTALL.with(|hook| hook.borrow().is_none()));
    assert!(
        matches!(result, Err(HawDBError::StorageIntegrity(_))),
        "{result:?}"
    );
    assert_eq!(
        fs::read(root.join(OUT_OF_CORE_MANIFEST_FILE)).unwrap(),
        old_head
    );
    let reader = SearchOutOfCoreReader::open(&root).unwrap();
    assert_eq!(reader.generation(), initial.generation);
    assert_eq!(
        reader
            .hydrate_documents(&[document(99).id])
            .unwrap()
            .documents,
        vec![document(99)]
    );
    drop(reader);
    fs::remove_dir_all(root).unwrap();
}

#[test]
fn same_length_installed_document_corruption_cannot_select_a_generation() {
    reject_changed_installed_payload("search_projection_segment_payloads.");
}

#[test]
fn same_length_installed_metadata_corruption_cannot_select_a_generation() {
    reject_changed_installed_payload("search_projection_metadata_payloads.");
}

#[cfg(feature = "vector-search")]
#[test]
fn same_length_installed_vector_corruption_cannot_select_a_generation() {
    reject_changed_installed_payload("search_projection_vector_payloads.");
}

#[test]
fn same_length_installed_lexical_corruption_cannot_select_a_generation() {
    reject_changed_installed_payload("search_lexical.");
}
