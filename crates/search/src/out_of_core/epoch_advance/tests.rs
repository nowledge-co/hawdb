// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

//! #1756: a metadata-only epoch advance writes one selector, reopens at
//! exactly one epoch after an abort, and leaves every content file in place.

use super::*;
use crate::out_of_core::{inject_resident_checkpoint_fault, ResidentCheckpointStep as Step};
use crate::{
    SearchDocument, SearchMode, SearchOutOfCoreGenerationBuildOptions,
    SearchOutOfCoreGenerationWriter, SearchProjectionDelta, SearchProjectionKind,
    SearchProjectionRow, SearchProjectionScanBudget,
};
use hawdb_core::RuntimeTaskContext;
use std::collections::BTreeMap;

struct Dir(PathBuf);

impl Dir {
    fn new(name: &str) -> Self {
        Self(
            std::env::var_os("TEST_TMPDIR")
                .map_or_else(std::env::temp_dir, PathBuf::from)
                .join(format!(
                    "hawdb-epoch-advance-{name}-{}",
                    hawdb_core::generate_uuidv7().unwrap()
                )),
        )
    }
}

impl Drop for Dir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

fn row(id: &str, body: &str, embedding: [f32; 2]) -> SearchProjectionRow {
    SearchProjectionRow {
        kind: SearchProjectionKind::Memory,
        external_id: id.into(),
        title: format!("title {id}"),
        body: body.into(),
        embedding: Some(embedding.to_vec()),
        source_id: None,
        metadata: BTreeMap::new(),
    }
}

fn delta(epoch: Option<u64>, upserts: Vec<SearchProjectionRow>) -> SearchProjectionDelta {
    SearchProjectionDelta {
        upserts,
        deletes: Vec::new(),
        max_operations: None,
        source_graph_commit_epoch: epoch,
    }
}

/// A resident projection with two checkpoints, the last at epoch 5.
fn resident(path: &Path) {
    let mut index = SearchIndex::open(path).unwrap();
    index
        .apply_projection_delta(delta(
            Some(3),
            vec![
                row("a", "epoch advance alpha", [1.0, 0.0]),
                row("b", "epoch advance beta", [0.0, 1.0]),
            ],
        ))
        .unwrap();
    index.checkpoint().unwrap();
    index
        .apply_projection_delta(delta(Some(5), vec![row("c", "advance gamma", [0.6, 0.8])]))
        .unwrap();
    index.checkpoint().unwrap();
}

/// Every file with its bytes and, on Unix, its inode: an advance must not
/// create, replace or rewrite any file other than the selector.
type Files = BTreeMap<String, (Vec<u8>, u64)>;

fn files(root: &Path) -> Files {
    std::fs::read_dir(root)
        .unwrap()
        .map(|entry| {
            let entry = entry.unwrap();
            let name = entry.file_name().to_string_lossy().into_owned();
            #[cfg(unix)]
            let inode = std::os::unix::fs::MetadataExt::ino(&entry.metadata().unwrap());
            #[cfg(not(unix))]
            let inode = 0;
            (name, (std::fs::read(entry.path()).unwrap(), inode))
        })
        .collect()
}

/// Asserts that only the selector changed and returns its new length.
fn assert_only_selector_changed(before: &Files, after: &Files) -> u64 {
    assert_eq!(
        before.keys().collect::<Vec<_>>(),
        after.keys().collect::<Vec<_>>(),
        "an epoch advance created or removed files"
    );
    for (name, (bytes, inode)) in before {
        if name != OUT_OF_CORE_MANIFEST_FILE {
            assert_eq!(&after[name], &(bytes.clone(), *inode), "{name} changed");
        }
    }
    let selector = &after[OUT_OF_CORE_MANIFEST_FILE].0;
    assert_ne!(&before[OUT_OF_CORE_MANIFEST_FILE].0, selector);
    selector.len() as u64
}

fn manifest(root: &Path) -> SearchOutOfCoreManifestBody {
    SearchOutOfCoreManifestBody::decode(
        &std::fs::read(root.join(OUT_OF_CORE_MANIFEST_FILE)).unwrap(),
    )
    .unwrap()
}

fn budget() -> SearchProjectionScanBudget {
    SearchProjectionScanBudget {
        max_rows: 1 << 16,
        max_payload_bytes: 1 << 24,
        max_encoded_record_bytes: 1 << 20,
    }
}

#[derive(Debug, PartialEq)]
struct Content {
    documents: Vec<SearchDocument>,
    hits: Vec<Vec<(String, u64, u64)>>,
}

fn resident_content(index: &SearchIndex) -> Content {
    let mut documents = Vec::new();
    index
        .visit_projection_documents(budget(), &RuntimeTaskContext::default(), &mut |document| {
            documents.push(document.clone());
            Ok(())
        })
        .unwrap();
    let mut modes = Vec::new();
    if cfg!(feature = "full-text-search") {
        modes.push(SearchMode::Text);
    }
    if cfg!(feature = "vector-search") {
        modes.push(SearchMode::Vector);
    }
    let hits = modes
        .into_iter()
        .map(|mode| {
            index
                .search("epoch advance", Some(&[0.7, 0.7]), mode, 10)
                .unwrap()
                .into_iter()
                .map(|hit| (hit.id, hit.score.to_bits(), hit.text_score.to_bits()))
                .collect()
        })
        .collect();
    Content { documents, hits }
}

fn out_of_core_documents(reader: &SearchOutOfCoreReader) -> Vec<SearchDocument> {
    let mut documents = Vec::new();
    reader
        .visit_projection_documents(budget(), &RuntimeTaskContext::default(), &mut |document| {
            documents.push(document.clone());
            Ok(())
        })
        .unwrap();
    documents.sort_by(|left, right| left.id.cmp(&right.id));
    documents
}

#[test]
fn resident_advance_publishes_only_the_selector_and_reopens_at_the_target() {
    let root = Dir::new("resident");
    let path = root.0.join("search");
    resident(&path);
    let index = SearchIndex::open(&path).unwrap();
    let content = resident_content(&index);
    drop(index);
    let generation = manifest(&path).generation;
    let mut index = SearchIndex::open(&path).unwrap();
    let before = files(&path);
    let report = index.advance_source_graph_commit_epoch(8).unwrap();
    let selector_bytes = assert_only_selector_changed(&before, &files(&path));
    assert_eq!(
        report,
        SearchProjectionEpochAdvanceReport {
            source_graph_commit_epoch_before: 5,
            source_graph_commit_epoch_after: 8,
            advanced: true,
            projection_generation: generation + 1,
            content_source_graph_commit_epoch: 5,
            metadata_bytes_written: selector_bytes,
            checked_change_log_entries: 0,
        }
    );
    assert!(selector_bytes < 16 * 1024, "{selector_bytes}");
    let freshness = index.projection_freshness();
    assert_eq!(freshness.source_graph_commit_epoch, Some(8));
    assert_eq!(freshness.durable_source_graph_commit_epoch, Some(8));
    assert!(!freshness.has_uncheckpointed_changes);
    assert_eq!(resident_content(&index), content);
    // The same target is a no-op that writes nothing.
    let unchanged = files(&path);
    let again = index.advance_source_graph_commit_epoch(8).unwrap();
    assert!(!again.advanced);
    assert_eq!(again.metadata_bytes_written, 0);
    assert_eq!(files(&path), unchanged);
    assert!(matches!(
        index.advance_source_graph_commit_epoch(7),
        Err(HawDBError::Semantic(_))
    ));
    drop(index);

    let reopened = SearchIndex::open(&path).unwrap();
    assert_eq!(
        reopened.projection_freshness().source_graph_commit_epoch,
        Some(8)
    );
    assert_eq!(
        reopened
            .projection_freshness()
            .durable_source_graph_commit_epoch,
        Some(8)
    );
    assert_eq!(resident_content(&reopened), content);
    drop(reopened);
    // The out-of-core reader the host reopens with after initial import
    // serves the same documents at the advanced epoch.
    let reader = SearchOutOfCoreReader::open(&path).unwrap();
    assert_eq!(reader.source_graph_commit_epoch(), Some(8));
    let mut expected = content.documents.clone();
    expected.sort_by(|left, right| left.id.cmp(&right.id));
    assert_eq!(out_of_core_documents(&reader), expected);
    drop(reader);

    // Advances chain on the same content stamp, and a later checkpoint
    // restamps every artifact and clears it.
    let mut index = SearchIndex::open(&path).unwrap();
    let chained = index.advance_source_graph_commit_epoch(10).unwrap();
    assert_eq!(chained.content_source_graph_commit_epoch, 5);
    assert_eq!(manifest(&path).content_source_graph_commit_epoch, Some(5));
    index
        .apply_projection_delta(delta(Some(12), vec![row("d", "advance delta", [0.3, 0.9])]))
        .unwrap();
    index.checkpoint().unwrap();
    drop(index);
    let published = manifest(&path);
    assert_eq!(published.source_graph_commit_epoch, Some(12));
    assert_eq!(published.content_source_graph_commit_epoch, None);
    let reopened = SearchIndex::open(&path).unwrap();
    assert_eq!(
        reopened.projection_freshness().source_graph_commit_epoch,
        Some(12)
    );
    assert_eq!(reopened.document_count(), 4);
}

#[test]
fn resident_advance_abort_reopens_at_exactly_one_epoch() {
    let root = Dir::new("resident-abort");
    let oracle = root.0.join("oracle");
    resident(&oracle);
    let content = resident_content(&SearchIndex::open(&oracle).unwrap());
    for (step, expected) in [
        (Step::EpochAdvanceBeforeSelector, 5),
        (Step::EpochAdvanceAfterSelector, 8),
    ] {
        let path = root.0.join(format!("{step:?}"));
        resident(&path);
        {
            let mut index = SearchIndex::open(&path).unwrap();
            let before = files(&path);
            inject_resident_checkpoint_fault(Some(step));
            let error = index.advance_source_graph_commit_epoch(8).unwrap_err();
            inject_resident_checkpoint_fault(None);
            assert!(error.to_string().contains("injected"), "{step:?}: {error}");
            let after = files(&path);
            if expected == 5 {
                // Only the unselected temporary selector can remain.
                assert_eq!(
                    after[OUT_OF_CORE_MANIFEST_FILE],
                    before[OUT_OF_CORE_MANIFEST_FILE]
                );
            } else {
                assert_only_selector_changed(&before, &after);
            }
        }
        let reopened = SearchIndex::open(&path).unwrap();
        let freshness = reopened.projection_freshness();
        assert_eq!(
            freshness.source_graph_commit_epoch,
            Some(expected),
            "{step:?}"
        );
        assert_eq!(
            freshness.durable_source_graph_commit_epoch,
            Some(expected),
            "{step:?}"
        );
        assert_eq!(resident_content(&reopened), content, "{step:?}");
        let reader = SearchOutOfCoreReader::open(&path).unwrap();
        assert_eq!(reader.source_graph_commit_epoch(), Some(expected));
        drop(reader);
        // A retry from either state reaches the target.
        let mut index = reopened;
        index.advance_source_graph_commit_epoch(8).unwrap();
        drop(index);
        assert_eq!(
            SearchIndex::open(&path)
                .unwrap()
                .projection_freshness()
                .source_graph_commit_epoch,
            Some(8)
        );
    }
}

#[test]
fn resident_advance_refuses_uncheckpointed_changes() {
    let root = Dir::new("resident-dirty");
    let path = root.0.join("search");
    resident(&path);
    let refused = |error: HawDBError| match error {
        HawDBError::SearchProjectionEpochAdvanceRefused { reason, .. } => reason,
        other => panic!("expected a typed refusal, got {other:?}"),
    };
    // An applied epoch that was never checkpointed.
    let mut index = SearchIndex::open(&path).unwrap();
    index
        .apply_projection_delta(delta(Some(7), vec![row("d", "dirty", [0.1, 0.2])]))
        .unwrap();
    let before = files(&path);
    assert_eq!(
        refused(index.advance_source_graph_commit_epoch(9).unwrap_err()),
        SearchProjectionEpochAdvanceRefusal::UncheckpointedChanges
    );
    assert_eq!(files(&path), before);
    drop(index);
    // Changed documents at an unchanged epoch differ from the selector.
    let mut index = SearchIndex::open(&path).unwrap();
    index
        .apply_projection_delta(delta(None, vec![row("e", "dirty", [0.1, 0.2])]))
        .unwrap();
    assert_eq!(
        refused(index.advance_source_graph_commit_epoch(9).unwrap_err()),
        SearchProjectionEpochAdvanceRefusal::UncheckpointedChanges
    );
    assert_eq!(files(&path), before);
    // A never-checkpointed projection has nothing to advance.
    let mut fresh = SearchIndex::open(root.0.join("fresh")).unwrap();
    assert_eq!(
        refused(fresh.advance_source_graph_commit_epoch(1).unwrap_err()),
        SearchProjectionEpochAdvanceRefusal::RangeUnavailable
    );
}

fn out_of_core(path: &Path) {
    let options = SearchOutOfCoreGenerationBuildOptions {
        source_graph_commit_epoch: Some(4),
        ..Default::default()
    };
    let mut writer = SearchOutOfCoreGenerationWriter::create(path, options).unwrap();
    for id in ["a", "b", "c"] {
        writer
            .push(row(id, "out of core base", [1.0, 0.5]).into_document())
            .unwrap();
    }
    writer.finish().unwrap();
    let reader = SearchOutOfCoreReader::open(path).unwrap();
    // A mutation generation, so the closure holds segments and a run.
    SearchOutOfCoreGenerationWriter::prepare_delta(
        &reader,
        SearchProjectionDelta {
            upserts: vec![row("b", "out of core edit", [0.2, 0.9])],
            deletes: vec!["memory:c".into()],
            max_operations: None,
            source_graph_commit_epoch: Some(6),
        },
        Default::default(),
    )
    .unwrap()
    .finish()
    .unwrap();
}

#[test]
fn out_of_core_advance_publishes_only_the_selector_and_keeps_publishing() {
    let root = Dir::new("out-of-core");
    let path = root.0.join("search");
    out_of_core(&path);
    let reader = SearchOutOfCoreReader::open(&path).unwrap();
    let documents = out_of_core_documents(&reader);
    let generation = reader.generation();
    let before = files(&path);
    let report = reader.advance_source_graph_commit_epoch(9).unwrap();
    let selector_bytes = assert_only_selector_changed(&before, &files(&path));
    assert_eq!(report.source_graph_commit_epoch_before, 6);
    assert_eq!(report.source_graph_commit_epoch_after, 9);
    assert_eq!(report.projection_generation, generation + 1);
    assert_eq!(report.content_source_graph_commit_epoch, 6);
    assert_eq!(report.metadata_bytes_written, selector_bytes);
    // The selector moved: a stale reader cannot publish over it.
    assert!(matches!(
        reader.advance_source_graph_commit_epoch(11),
        Err(HawDBError::TransactionConflict { .. })
    ));
    let mut shared = std::sync::Arc::new(reader);
    let refresh = SearchOutOfCoreReader::refresh_shared(&mut shared).unwrap();
    assert!(refresh.generation_changed);
    assert_eq!(refresh.opened_content_segments, 0);
    assert_eq!(shared.source_graph_commit_epoch(), Some(9));
    assert_eq!(out_of_core_documents(&shared), documents);
    drop(shared);

    let reopened = SearchOutOfCoreReader::open(&path).unwrap();
    assert_eq!(reopened.source_graph_commit_epoch(), Some(9));
    assert_eq!(out_of_core_documents(&reopened), documents);
    // Incremental publication continues from the advanced epoch.
    let (delta_report, _, _) = SearchOutOfCoreGenerationWriter::prepare_delta(
        &reopened,
        delta(Some(12), vec![row("z", "after advance", [0.4, 0.4])]),
        Default::default(),
    )
    .unwrap()
    .finish()
    .unwrap();
    assert_eq!(delta_report.source_graph_commit_epoch_before, Some(9));
    let published = manifest(&path);
    assert_eq!(published.source_graph_commit_epoch, Some(12));
    assert_eq!(published.content_source_graph_commit_epoch, None);
    let current = SearchOutOfCoreReader::open(&path).unwrap();
    assert_eq!(current.document_count(), 3);
}

#[test]
fn out_of_core_advance_abort_reopens_at_exactly_one_epoch() {
    let root = Dir::new("out-of-core-abort");
    for (step, expected) in [
        (Step::EpochAdvanceBeforeSelector, 6),
        (Step::EpochAdvanceAfterSelector, 9),
    ] {
        let path = root.0.join(format!("{step:?}"));
        out_of_core(&path);
        let reader = SearchOutOfCoreReader::open(&path).unwrap();
        let documents = out_of_core_documents(&reader);
        inject_resident_checkpoint_fault(Some(step));
        let error = reader.advance_source_graph_commit_epoch(9).unwrap_err();
        inject_resident_checkpoint_fault(None);
        assert!(error.to_string().contains("injected"), "{step:?}: {error}");
        drop(reader);
        let reopened = SearchOutOfCoreReader::open(&path).unwrap();
        assert_eq!(
            reopened.source_graph_commit_epoch(),
            Some(expected),
            "{step:?}"
        );
        assert_eq!(out_of_core_documents(&reopened), documents, "{step:?}");
        let report = reopened.advance_source_graph_commit_epoch(9).unwrap();
        assert_eq!(report.advanced, expected != 9, "{step:?}");
    }
}
