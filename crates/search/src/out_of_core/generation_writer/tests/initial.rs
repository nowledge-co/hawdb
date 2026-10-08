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

fn sources() -> Vec<SearchDocument> {
    (0..4)
        .map(|number| {
            let mut source = document(number);
            source.content = (0..256)
                .map(|word| format!("word{number:02}x{word:04} "))
                .collect();
            source
        })
        .collect()
}

#[test]
fn initial_artifact_admission_splits_oversized_candidates_without_recapturing_bodies() {
    let documents = sources();
    let mut single_content_bytes = 0;
    for source in &documents {
        let root = test_dir("initial_single_content_reference");
        let mut writer =
            SearchOutOfCoreGenerationWriter::create(&root, Default::default()).unwrap();
        writer.push(source.clone()).unwrap();
        let report = writer.finish().unwrap();
        single_content_bytes =
            single_content_bytes.max(report.generation_bytes - report.manifest_bytes);
        fs::remove_dir_all(root).unwrap();
    }
    let mut pair_content_bytes = u64::MAX;
    for pair in documents.chunks(2) {
        let root = test_dir("initial_pair_content_reference");
        let mut writer =
            SearchOutOfCoreGenerationWriter::create(&root, Default::default()).unwrap();
        for source in pair {
            writer.push(source.clone()).unwrap();
        }
        let report = writer.finish().unwrap();
        pair_content_bytes =
            pair_content_bytes.min(report.generation_bytes - report.manifest_bytes);
        fs::remove_dir_all(root).unwrap();
    }
    assert!(pair_content_bytes > single_content_bytes);
    // Bound between measured single and paired closures. This permits the small
    // JSON length differences in their checksums while requiring every pair to split.
    let limit = single_content_bytes + (pair_content_bytes - single_content_bytes) / 2;
    // The unchanged compression workspace alone needs 8 MiB plus 32 KiB.
    // Keep the insufficient budget as a real rejection case, then admit it
    // alongside both writers' state for the adaptive publication case.
    for budget in [8 * 1024 * 1024, 16 * 1024 * 1024] {
        let root = test_dir("initial_adaptive_content");
        let task = RuntimeTaskContext::default()
            .with_memory_reservation(hawdb_core::RuntimeMemoryReservation::new(budget, 0));
        let mut writer = SearchOutOfCoreGenerationWriter::create_with_context(
            &root,
            SearchOutOfCoreGenerationBuildOptions {
                max_content_documents: NonZeroUsize::new(documents.len()).unwrap(),
                max_content_artifact_bytes: NonZeroU64::new(limit).unwrap(),
                ..Default::default()
            },
            task,
        )
        .unwrap();
        for source in &documents {
            writer.push(source.clone()).unwrap();
        }
        let captured_bytes = writer.spool_bytes;
        let memory = writer.memory.clone();
        let result = writer.finish();
        assert_eq!(memory.ledger.snapshot().used_bytes, 0);
        assert!(memory.ledger.snapshot().peak_bytes <= budget as usize);
        if budget == 8 * 1024 * 1024 {
            let error = result.unwrap_err();
            assert!(
                error.to_string().contains("query memory account"),
                "{error}"
            );
            assert!(!root.join(OUT_OF_CORE_MANIFEST_FILE).exists());
            assert_eq!(stage_directories(&root), 0);
            fs::remove_dir_all(root).unwrap();
            continue;
        }
        let report = result.unwrap();
        assert_eq!(report.published_content_segments, documents.len());
        assert_eq!(report.spool_bytes, captured_bytes);
        assert!(!report.cleanup_retry_required);
        assert_eq!(memory.ledger.snapshot().used_bytes, 0);
        assert!(memory.ledger.snapshot().peak_bytes <= budget as usize);
        let reader = super::super::super::SearchOutOfCoreReader::open(&root).unwrap();
        assert_eq!(reader.document_count(), documents.len());
        assert_eq!(
            reader
                .hydrate_documents(
                    &documents
                        .iter()
                        .map(|source| source.id.clone())
                        .collect::<Vec<_>>()
                )
                .unwrap()
                .documents,
            documents
        );
        for (manifest, content) in reader.manifest.segments.iter().zip(&reader.segments) {
            let bytes = manifest.descriptor_len
                + manifest.payload_len
                + manifest.metadata_payload_len
                + manifest.vector_payload_len
                + manifest.layout_len
                + manifest.lexical_manifest_len
                + manifest.rabitq_artifact_len.unwrap_or_default()
                + content.lexical_projection.artifact_len();
            assert!(bytes <= limit);
        }
        drop(reader);
        fs::remove_dir_all(root).unwrap();
    }
}

#[test]
fn initial_prefixes_never_replace_the_active_selector_on_injected_publication_failure() {
    let mut covered_replaces = 0;
    for failure in 1..128 {
        let root = test_dir("initial_prefix_failure");
        let previous = document(99);
        let mut initial =
            SearchOutOfCoreGenerationWriter::create(&root, Default::default()).unwrap();
        initial.push(previous.clone()).unwrap();
        initial.finish().unwrap();
        let before = fs::read(root.join(OUT_OF_CORE_MANIFEST_FILE)).unwrap();
        let mut writer = SearchOutOfCoreGenerationWriter::create(
            &root,
            SearchOutOfCoreGenerationBuildOptions {
                max_content_documents: NonZeroUsize::new(2).unwrap(),
                ..Default::default()
            },
        )
        .unwrap();
        let documents: Vec<_> = (0..5).map(document).collect();
        for source in &documents {
            writer.push(source.clone()).unwrap();
        }
        super::super::io::fail_replace_at(failure);
        let result = writer.finish();
        super::super::io::fail_replace_at(0);
        let reader = super::super::super::SearchOutOfCoreReader::open(&root).unwrap();
        let success = match result {
            Err(error) => {
                assert!(
                    error
                        .to_string()
                        .contains("injected publication replace failure"),
                    "{error}"
                );
                assert_eq!(
                    fs::read(root.join(OUT_OF_CORE_MANIFEST_FILE)).unwrap(),
                    before
                );
                assert_eq!(reader.document_count(), 1);
                assert_eq!(
                    reader
                        .hydrate_documents(std::slice::from_ref(&previous.id))
                        .unwrap()
                        .documents,
                    vec![previous]
                );
                covered_replaces += 1;
                false
            }
            Ok(report) => {
                assert_eq!(report.published_content_segments, 3);
                assert_eq!(reader.document_count(), documents.len());
                assert_eq!(
                    reader
                        .hydrate_documents(
                            &documents
                                .iter()
                                .map(|source| source.id.clone())
                                .collect::<Vec<_>>()
                        )
                        .unwrap()
                        .documents,
                    documents
                );
                true
            }
        };
        assert_eq!(stage_directories(&root), 0);
        drop(reader);
        fs::remove_dir_all(root).unwrap();
        if success {
            break;
        }
    }
    assert!(
        covered_replaces > 3 && covered_replaces < 127,
        "must execute every replacement boundary and then complete"
    );
}

#[test]
fn initial_total_publication_budget_keeps_the_previous_complete_dataset() {
    let root = test_dir("initial_complete_budget");
    let previous = document(99);
    let mut writer = SearchOutOfCoreGenerationWriter::create(&root, Default::default()).unwrap();
    writer.push(previous.clone()).unwrap();
    let reference = writer.finish().unwrap();
    let before = fs::read(root.join(OUT_OF_CORE_MANIFEST_FILE)).unwrap();
    let mut writer = SearchOutOfCoreGenerationWriter::create(
        &root,
        SearchOutOfCoreGenerationBuildOptions {
            max_content_documents: NonZeroUsize::new(1).unwrap(),
            max_generation_bytes: NonZeroU64::new(2 * reference.generation_bytes).unwrap(),
            ..Default::default()
        },
    )
    .unwrap();
    for number in 0..4 {
        writer.push(document(number)).unwrap();
    }
    let error = writer.finish().unwrap_err();
    assert!(
        error
            .to_string()
            .contains("complete initial generation exceeds publication admission"),
        "{error}"
    );
    assert_eq!(
        fs::read(root.join(OUT_OF_CORE_MANIFEST_FILE)).unwrap(),
        before
    );
    let reader = super::super::super::SearchOutOfCoreReader::open(&root).unwrap();
    assert_eq!(
        reader
            .hydrate_documents(std::slice::from_ref(&previous.id))
            .unwrap()
            .documents,
        vec![previous]
    );
    drop(reader);
    fs::remove_dir_all(root).unwrap();
}

#[test]
fn initial_cancellation_and_admission_failure_after_a_private_prefix_preserve_the_old_dataset() {
    use crate::generation_cleanup::once::evidence::{self, Point};
    use std::cell::RefCell;
    use std::rc::Rc;
    for cancelled in [false, true] {
        let root = test_dir("initial_prefix_interruption");
        let previous = document(99);
        let mut writer =
            SearchOutOfCoreGenerationWriter::create(&root, Default::default()).unwrap();
        writer.push(previous.clone()).unwrap();
        writer.finish().unwrap();
        let before = fs::read(root.join(OUT_OF_CORE_MANIFEST_FILE)).unwrap();
        let budget = 16 * 1024 * 1024;
        let task = RuntimeTaskContext::default()
            .with_memory_reservation(hawdb_core::RuntimeMemoryReservation::new(budget, 0));
        let mut writer = SearchOutOfCoreGenerationWriter::create_with_context(
            &root,
            SearchOutOfCoreGenerationBuildOptions {
                max_content_documents: NonZeroUsize::new(2).unwrap(),
                ..Default::default()
            },
            task.clone(),
        )
        .unwrap();
        for number in 0..5 {
            writer.push(document(number)).unwrap();
        }
        let memory = writer.memory.clone();
        let private = writer.stage.path.to_path_buf();
        let private_was_complete = Rc::new(RefCell::new(false));
        let observed = Rc::clone(&private_was_complete);
        let held = Rc::new(RefCell::new(None));
        let retained = Rc::clone(&held);
        let _gate = evidence::at(Point::AfterInitialPartition, move |memory| {
            let prefix = super::super::super::SearchOutOfCoreReader::open(private).unwrap();
            assert_eq!(prefix.document_count(), 2);
            assert_eq!(prefix.manifest.segments.len(), 1);
            drop(prefix);
            *observed.borrow_mut() = true;
            if cancelled {
                task.cancellation().cancel();
            } else {
                let available = budget as usize - memory.ledger.snapshot().used_bytes;
                *retained.borrow_mut() = Some(memory.retained.reserve(available).unwrap());
            }
        });
        let error = writer.finish().unwrap_err();
        assert!(*private_was_complete.borrow());
        let message = error.to_string().to_ascii_lowercase();
        assert!(
            if cancelled {
                message.contains("cancel")
            } else {
                message.contains("memory")
            },
            "{error}"
        );
        assert_eq!(
            fs::read(root.join(OUT_OF_CORE_MANIFEST_FILE)).unwrap(),
            before
        );
        assert_eq!(stage_directories(&root), 0);
        held.borrow_mut().take();
        assert_eq!(memory.ledger.snapshot().used_bytes, 0);
        let reader = super::super::super::SearchOutOfCoreReader::open(&root).unwrap();
        assert_eq!(reader.document_count(), 1);
        assert_eq!(
            reader
                .hydrate_documents(std::slice::from_ref(&previous.id))
                .unwrap()
                .documents,
            vec![previous]
        );
        drop(reader);
        fs::remove_dir_all(root).unwrap();
    }
}

#[test]
#[cfg(feature = "full-text-search")]
fn initial_partition_queries_and_sparse_metadata_match_one_content_segment() {
    use crate::{SearchMode, SearchQueryOptions};
    let roots = [
        test_dir("initial_partition_query"),
        test_dir("initial_single_query"),
    ];
    let mut documents: Vec<_> = (0..6).map(document).collect();
    documents[4]
        .metadata
        .insert("rare".into(), "selected".into());
    documents[2].embedding = None;
    for (index, root) in roots.iter().enumerate() {
        let mut writer = SearchOutOfCoreGenerationWriter::create(
            root,
            SearchOutOfCoreGenerationBuildOptions {
                max_content_documents: NonZeroUsize::new(if index == 0 { 2 } else { 8192 })
                    .unwrap(),
                ..Default::default()
            },
        )
        .unwrap();
        for source in &documents {
            writer.push(source.clone()).unwrap();
        }
        writer.finish().unwrap();
    }
    let candidate = super::super::super::SearchOutOfCoreReader::open(&roots[0]).unwrap();
    let reference = super::super::super::SearchOutOfCoreReader::open(&roots[1]).unwrap();
    assert_eq!(candidate.manifest.segments.len(), 3);
    assert_eq!(reference.manifest.segments.len(), 1);
    let modes = [
        SearchMode::Text,
        #[cfg(feature = "vector-search")]
        SearchMode::Vector,
        #[cfg(feature = "vector-search")]
        SearchMode::Hybrid,
    ];
    for mode in modes {
        for filtered in [false, true] {
            let mut options = SearchQueryOptions {
                limit: 10,
                offset: 0,
                rank_window: None,
                fusion_weights: Default::default(),
                metadata_filters: Default::default(),
                policy_epoch: None,
            };
            if filtered {
                options
                    .metadata_filters
                    .insert("rare".into(), "selected".into());
            }
            let expected = reference
                .search_with_options("graph storage", Some(&[1.0, 0.5]), mode, options.clone())
                .unwrap();
            let actual = candidate
                .search_with_options("graph storage", Some(&[1.0, 0.5]), mode, options.clone())
                .unwrap();
            assert_eq!(actual.result.hits, expected.result.hits);
            assert_eq!(actual.result.total_hits, expected.result.total_hits);
            #[cfg(feature = "vector-search")]
            if mode != SearchMode::Text {
                let compressed = candidate
                    .search_with_options_compressed_vector_projection_mode(
                        "graph storage",
                        Some(&[1.0, 0.5]),
                        mode,
                        options.clone(),
                        crate::CompressedVectorSearchMode::Required,
                    )
                    .unwrap();
                let merged = reference
                    .search_with_options_compressed_vector_projection_mode(
                        "graph storage",
                        Some(&[1.0, 0.5]),
                        mode,
                        options,
                        crate::CompressedVectorSearchMode::Required,
                    )
                    .unwrap();
                assert!(compressed.metrics.rabitq_payload_bytes_read > 0);
                assert_eq!(compressed.result.hits, merged.result.hits);
                assert_eq!(compressed.result.total_hits, merged.result.total_hits);
                assert_eq!(compressed.result.hits, expected.result.hits);
            }
        }
    }
    drop(candidate);
    drop(reference);
    for root in roots {
        fs::remove_dir_all(root).unwrap();
    }
}

#[test]
fn initial_partition_cleanup_debt_is_retriable_from_the_real_root() {
    use crate::generation_cleanup::once::evidence::{self, Point};
    use std::cell::RefCell;
    use std::rc::Rc;
    const CHILD: &str = "HAWDB_INITIAL_STAGE_CLEANUP_TEST_CHILD";
    if std::env::var_os(CHILD).is_none() {
        // Isolate the existing process-wide unlink fault from other tests.
        let output = std::process::Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                concat!(
                    module_path!(),
                    "::initial_partition_cleanup_debt_is_retriable_from_the_real_root"
                )
                .strip_prefix("hawdb_search::")
                .unwrap(),
                "--nocapture",
            ])
            .env(CHILD, "1")
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "child failed: {}\n{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
        return;
    }
    let root = test_dir("initial_partition_cleanup_retry");
    let mut writer = SearchOutOfCoreGenerationWriter::create(
        &root,
        SearchOutOfCoreGenerationBuildOptions {
            max_content_documents: NonZeroUsize::new(2).unwrap(),
            ..Default::default()
        },
    )
    .unwrap();
    for number in 0..5 {
        writer.push(document(number)).unwrap();
    }
    let memory = writer.memory.clone();
    let private = writer.stage.path.to_path_buf();
    let root_for_hook = fs::canonicalize(&root).unwrap();
    let fault = Rc::new(RefCell::new(None));
    let keep_fault = Rc::clone(&fault);
    let _gate = evidence::at(Point::AfterInitialPartition, move |_| {
        let mut stages = Vec::new();
        for parent in [&root_for_hook, &private] {
            for entry in fs::read_dir(parent).unwrap() {
                let path = entry.unwrap().path();
                if path != private
                    && path
                        .extension()
                        .is_some_and(|extension| extension == "stage")
                {
                    stages.push(path);
                }
            }
        }
        assert_eq!(stages.len(), 1);
        *keep_fault.borrow_mut() = Some(super::super::spool::fail_unlink(
            &stages[0].join("documents.spool.hawdb"),
        ));
    });
    let report = writer.finish().unwrap();
    assert!(fault.borrow().as_ref().unwrap().attempts() > 0);
    assert!(report.cleanup_retry_required);
    let reader = super::super::super::SearchOutOfCoreReader::open(&root).unwrap();
    assert_eq!(reader.document_count(), 5);
    drop(reader);
    assert_eq!(stage_directories(&root), 1);
    assert!(memory.ledger.snapshot().used_bytes > 0);
    fault.borrow_mut().take();
    let retry = SearchOutOfCoreGenerationWriter::retry_staging_cleanup(&root, 32).unwrap();
    assert_eq!(
        retry.pending_stages, 0,
        "the real root must own every deferred partition stage"
    );
    assert_eq!(stage_directories(&root), 0);
    assert_eq!(memory.ledger.snapshot().used_bytes, 0);
    fs::remove_dir_all(root).unwrap();
}
