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

fn row(number: usize, body: &str) -> SearchProjectionRow {
    let source = document(number);
    SearchProjectionRow {
        kind: SearchProjectionKind::Memory,
        external_id: format!("{number:06}"),
        title: source.title,
        body: body.into(),
        embedding: source.embedding,
        source_id: None,
        metadata: source.metadata,
    }
}

fn assert_documents(
    reader: &super::super::super::SearchOutOfCoreReader,
    expected: &[SearchDocument],
) {
    assert_eq!(reader.document_count(), expected.len());
    assert_eq!(
        reader
            .hydrate_documents(
                &expected
                    .iter()
                    .map(|source| source.id.clone())
                    .collect::<Vec<_>>()
            )
            .unwrap()
            .documents,
        expected
    );
}

#[test]
fn incremental_ownership_splits_appends_and_replacements_in_one_publication() {
    for replacing in [false, true] {
        let root = test_dir("incremental_ownership_count");
        let previous: Vec<_> = (0..6).map(document).collect();
        let mut initial =
            SearchOutOfCoreGenerationWriter::create(&root, Default::default()).unwrap();
        for source in &previous {
            initial.push(source.clone()).unwrap();
        }
        initial.finish().unwrap();
        let pinned = super::super::super::SearchOutOfCoreReader::open(&root).unwrap();
        let numbers = if replacing { 0..4 } else { 10..14 };
        let upserts: Vec<_> = numbers
            .map(|number| row(number, "new atomic partition content"))
            .collect();
        let mut expected = previous.clone();
        if replacing {
            expected.retain(|source| {
                !upserts
                    .iter()
                    .any(|row| row.clone().into_document().id == source.id)
                    && source.id != previous[5].id
            });
        }
        expected.extend(
            upserts
                .iter()
                .cloned()
                .map(SearchProjectionRow::into_document),
        );
        expected.sort_by(|left, right| left.id.cmp(&right.id));
        let (delta, report, _) = SearchOutOfCoreGenerationWriter::prepare_delta(
            &pinned,
            SearchProjectionDelta {
                upserts,
                deletes: if replacing {
                    vec![previous[5].id.clone()]
                } else {
                    vec![]
                },
                source_graph_commit_epoch: Some(42),
                ..Default::default()
            },
            SearchOutOfCoreGenerationBuildOptions {
                max_content_documents: NonZeroUsize::new(2).unwrap(),
                ..Default::default()
            },
        )
        .unwrap()
        .finish()
        .unwrap();
        assert_eq!(report.published_content_segments, 2);
        assert_eq!(delta.after_document_count, expected.len());
        assert_eq!(report.document_count, expected.len());
        let reopened = super::super::super::SearchOutOfCoreReader::open(&root).unwrap();
        assert_eq!(reopened.document_count(), expected.len());
        assert_eq!(reopened.source_graph_commit_epoch(), Some(42));
        assert_eq!(
            reopened
                .hydrate_documents(
                    &expected
                        .iter()
                        .map(|source| source.id.clone())
                        .collect::<Vec<_>>()
                )
                .unwrap()
                .documents,
            expected
        );
        assert_eq!(
            pinned
                .hydrate_documents(
                    &previous
                        .iter()
                        .map(|source| source.id.clone())
                        .collect::<Vec<_>>()
                )
                .unwrap()
                .documents,
            previous
        );
        assert_eq!(reopened.manifest.segments.len(), 3);
        assert!(reopened.manifest.segments[1..]
            .iter()
            .all(|segment| segment.document_count <= 2));
        assert_eq!(stage_directories(&root), 0);
        drop(reopened);
        drop(pinned);
        fs::remove_dir_all(root).unwrap();
    }
}

#[test]
fn incremental_ownership_admits_an_indivisible_document_above_the_byte_target() {
    let source = row(0, &document(0).content).into_document();
    for incremental in [false, true] {
        let root = test_dir("incremental_ownership_single");
        let options = SearchOutOfCoreGenerationBuildOptions {
            max_content_artifact_bytes: NonZeroU64::new(1).unwrap(),
            ..Default::default()
        };
        let report = if incremental {
            let mut initial =
                SearchOutOfCoreGenerationWriter::create(&root, Default::default()).unwrap();
            initial.push(document(99)).unwrap();
            initial.finish().unwrap();
            let reader = super::super::super::SearchOutOfCoreReader::open(&root).unwrap();
            SearchOutOfCoreGenerationWriter::prepare_delta(
                &reader,
                SearchProjectionDelta {
                    upserts: vec![row(0, &source.content)],
                    ..Default::default()
                },
                options,
            )
            .unwrap()
            .finish()
            .unwrap()
            .1
        } else {
            let mut writer = SearchOutOfCoreGenerationWriter::create(&root, options).unwrap();
            writer.push(source.clone()).unwrap();
            writer.finish().unwrap()
        };
        assert_eq!(report.published_content_segments, 1);
        assert!(report.generation_bytes - report.manifest_bytes > 1);
        let reader = super::super::super::SearchOutOfCoreReader::open(&root).unwrap();
        assert_eq!(
            reader
                .hydrate_documents(std::slice::from_ref(&source.id))
                .unwrap()
                .documents,
            vec![source.clone()]
        );
        drop(reader);
        fs::remove_dir_all(root).unwrap();
    }
}

#[test]
fn incremental_ownership_byte_splits_preserve_prior_retractions_and_query_scores() {
    let root = test_dir("incremental_ownership_byte");
    let mut initial = SearchOutOfCoreGenerationWriter::create(&root, Default::default()).unwrap();
    for number in 0..4 {
        initial.push(document(number)).unwrap();
    }
    initial.finish().unwrap();
    let first = super::super::super::SearchOutOfCoreReader::open(&root).unwrap();
    SearchOutOfCoreGenerationWriter::prepare_delta(
        &first,
        SearchProjectionDelta {
            upserts: vec![row(0, "intermediate graph replacement")],
            ..Default::default()
        },
        Default::default(),
    )
    .unwrap()
    .finish()
    .unwrap();
    let pinned = super::super::super::SearchOutOfCoreReader::open(&root).unwrap();
    let mut previous: Vec<_> = (1..4).map(document).collect();
    previous.insert(0, row(0, "intermediate graph replacement").into_document());
    let upserts: Vec<_> = [0, 1, 2, 10]
        .into_iter()
        .map(|number| row(number, "final graph partition content"))
        .collect();
    let expected: Vec<_> = upserts
        .iter()
        .cloned()
        .map(SearchProjectionRow::into_document)
        .collect();
    let (_, report, _) = SearchOutOfCoreGenerationWriter::prepare_delta(
        &pinned,
        SearchProjectionDelta {
            upserts,
            deletes: vec![document(3).id],
            ..Default::default()
        },
        SearchOutOfCoreGenerationBuildOptions {
            // Every multi-document candidate must split on bytes; each indivisible
            // result exercises the soft target exception independently of count.
            max_content_artifact_bytes: NonZeroU64::new(1).unwrap(),
            ..Default::default()
        },
    )
    .unwrap()
    .finish()
    .unwrap();
    assert_eq!(report.published_content_segments, 4);
    let reopened = super::super::super::SearchOutOfCoreReader::open(&root).unwrap();
    assert_eq!(reopened.manifest.mutation_runs.len(), 2);
    assert_eq!(reopened.manifest.segments.len(), 6);
    assert_documents(&reopened, &expected);
    assert_documents(&pinned, &previous);
    assert!(reopened.hydrate_documents(&[document(3).id]).is_err());
    let merged_root = test_dir("incremental_ownership_query_reference");
    let mut merged =
        SearchOutOfCoreGenerationWriter::create(&merged_root, Default::default()).unwrap();
    for source in &expected {
        merged.push(source.clone()).unwrap();
    }
    merged.finish().unwrap();
    let reference = super::super::super::SearchOutOfCoreReader::open(&merged_root).unwrap();
    assert_eq!(
        reopened.manifest.documents_digest,
        reference.manifest.documents_digest
    );
    #[cfg(feature = "full-text-search")]
    for mode in [
        crate::SearchMode::Text,
        #[cfg(feature = "vector-search")]
        crate::SearchMode::Vector,
        #[cfg(feature = "vector-search")]
        crate::SearchMode::Hybrid,
    ] {
        let options = crate::SearchQueryOptions {
            limit: 20,
            offset: 0,
            rank_window: None,
            fusion_weights: Default::default(),
            metadata_filters: Default::default(),
            policy_epoch: None,
        };
        let actual = reopened
            .search_with_options("graph partition", Some(&[1.0, 0.02]), mode, options.clone())
            .unwrap();
        let expected = reference
            .search_with_options("graph partition", Some(&[1.0, 0.02]), mode, options)
            .unwrap();
        assert_eq!(actual.result.hits, expected.result.hits);
        assert_eq!(actual.result.total_hits, expected.result.total_hits);
    }
    drop(reference);
    drop(reopened);
    drop(pinned);
    drop(first);
    assert_eq!(stage_directories(&root), 0);
    fs::remove_dir_all(root).unwrap();
    fs::remove_dir_all(merged_root).unwrap();
}

#[test]
fn incremental_ownership_single_document_exception_keeps_hard_publication_and_segment_limits() {
    for segment_limit in [false, true] {
        let root = test_dir("incremental_ownership_hard_limit");
        let mut initial =
            SearchOutOfCoreGenerationWriter::create(&root, Default::default()).unwrap();
        initial.push(document(99)).unwrap();
        initial.finish().unwrap();
        let before = fs::read(root.join(OUT_OF_CORE_MANIFEST_FILE)).unwrap();
        let reader = super::super::super::SearchOutOfCoreReader::open(&root).unwrap();
        let mut options = SearchOutOfCoreGenerationBuildOptions {
            max_content_artifact_bytes: NonZeroU64::new(1).unwrap(),
            ..Default::default()
        };
        if segment_limit {
            options.max_segment_compressed_bytes = NonZeroU64::new(1).unwrap();
        } else {
            options.max_generation_bytes = NonZeroU64::new(1).unwrap();
        }
        let error = SearchOutOfCoreGenerationWriter::prepare_delta(
            &reader,
            SearchProjectionDelta {
                upserts: vec![row(0, "hard ceilings still apply")],
                ..Default::default()
            },
            options,
        )
        .unwrap()
        .finish()
        .unwrap_err();
        assert!(!error.to_string().contains("content exceeds"), "{error}");
        assert_eq!(
            fs::read(root.join(OUT_OF_CORE_MANIFEST_FILE)).unwrap(),
            before
        );
        let reopened = super::super::super::SearchOutOfCoreReader::open(&root).unwrap();
        assert_documents(&reopened, &[document(99)]);
        assert_eq!(stage_directories(&root), 0);
        drop(reopened);
        drop(reader);
        fs::remove_dir_all(root).unwrap();
    }
}

#[test]
fn incremental_ownership_prefix_failures_never_select_partial_replacements() {
    let mut covered = 0;
    for failure in 1..128 {
        let root = test_dir("incremental_ownership_failure");
        let mut initial =
            SearchOutOfCoreGenerationWriter::create(&root, Default::default()).unwrap();
        let previous: Vec<_> = (0..4).map(document).collect();
        for source in &previous {
            initial.push(source.clone()).unwrap();
        }
        initial.finish().unwrap();
        let before = fs::read(root.join(OUT_OF_CORE_MANIFEST_FILE)).unwrap();
        let pinned = super::super::super::SearchOutOfCoreReader::open(&root).unwrap();
        let upserts: Vec<_> = (0..3)
            .map(|number| row(number, "complete atomic replacement"))
            .collect();
        let expected: Vec<_> = upserts
            .iter()
            .cloned()
            .map(SearchProjectionRow::into_document)
            .collect();
        let update = SearchOutOfCoreGenerationWriter::prepare_delta(
            &pinned,
            SearchProjectionDelta {
                upserts,
                deletes: vec![document(3).id],
                ..Default::default()
            },
            SearchOutOfCoreGenerationBuildOptions {
                max_content_documents: NonZeroUsize::new(1).unwrap(),
                ..Default::default()
            },
        )
        .unwrap();
        super::super::io::fail_replace_at(failure);
        let result = update.finish();
        super::super::io::fail_replace_at(0);
        let reopened = super::super::super::SearchOutOfCoreReader::open(&root).unwrap();
        let completed = match result {
            Ok((_, report, _)) => {
                assert_eq!(report.published_content_segments, 3);
                assert_documents(&reopened, &expected);
                true
            }
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
                assert_documents(&reopened, &previous);
                covered += 1;
                false
            }
        };
        assert_documents(&pinned, &previous);
        assert_eq!(stage_directories(&root), 0);
        drop(reopened);
        drop(pinned);
        fs::remove_dir_all(root).unwrap();
        if completed {
            break;
        }
    }
    assert!(
        covered > 3 && covered < 127,
        "must traverse all dependency/prefix/final-selector replacement boundaries"
    );
}

#[test]
fn incremental_ownership_cancellation_and_memory_rejection_after_a_prefix_keep_the_old_batch() {
    use crate::generation_cleanup::once::evidence::{self, Point};
    use std::cell::RefCell;
    use std::rc::Rc;
    for cancelled in [false, true] {
        let root = test_dir("incremental_ownership_interruption");
        let mut initial =
            SearchOutOfCoreGenerationWriter::create(&root, Default::default()).unwrap();
        initial.push(document(99)).unwrap();
        initial.finish().unwrap();
        let before = fs::read(root.join(OUT_OF_CORE_MANIFEST_FILE)).unwrap();
        let reader = super::super::super::SearchOutOfCoreReader::open(&root).unwrap();
        let budget = 16 * 1024 * 1024;
        let task = RuntimeTaskContext::default()
            .with_memory_reservation(hawdb_core::RuntimeMemoryReservation::new(budget, 0));
        let update = SearchOutOfCoreGenerationWriter::prepare_delta_with_context(
            &reader,
            SearchProjectionDelta {
                upserts: (0..4)
                    .map(|number| row(number, "pending graph replacement"))
                    .collect(),
                ..Default::default()
            },
            SearchOutOfCoreGenerationBuildOptions {
                max_content_documents: NonZeroUsize::new(1).unwrap(),
                ..Default::default()
            },
            task.clone(),
        )
        .unwrap();
        let held = Rc::new(RefCell::new(None));
        let retained = Rc::clone(&held);
        let observed = Rc::new(RefCell::new(false));
        let reached = Rc::clone(&observed);
        let _gate = evidence::at(Point::AfterInitialPartition, move |memory| {
            *reached.borrow_mut() = true;
            if cancelled {
                task.cancellation().cancel();
            } else {
                let available = budget as usize - memory.ledger.snapshot().used_bytes;
                *retained.borrow_mut() = Some(memory.retained.reserve(available).unwrap());
            }
        });
        let error = update.finish().unwrap_err();
        assert!(*observed.borrow());
        let message = error.to_string().to_ascii_lowercase();
        assert!(
            if cancelled {
                message.contains("cancel")
            } else {
                message.contains("memory")
            },
            "{error}"
        );
        held.borrow_mut().take();
        assert_eq!(
            fs::read(root.join(OUT_OF_CORE_MANIFEST_FILE)).unwrap(),
            before
        );
        assert_eq!(stage_directories(&root), 0);
        let reopened = super::super::super::SearchOutOfCoreReader::open(&root).unwrap();
        assert_documents(&reopened, &[document(99)]);
        drop(reopened);
        drop(reader);
        fs::remove_dir_all(root).unwrap();
    }
}
