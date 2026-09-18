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
use crate::{SearchProjectionKind, SearchProjectionRow};
use hawdb_core::RuntimeMemoryReservation;
use std::collections::BTreeMap;
use std::path::PathBuf;
use std::sync::atomic::{AtomicUsize, Ordering};

struct Fixture(PathBuf);

impl Fixture {
    fn new() -> Self {
        static SEQUENCE: AtomicUsize = AtomicUsize::new(0);
        let path = std::env::temp_dir().join(format!(
            "hawdb-delta-context-{}-{}",
            std::process::id(),
            SEQUENCE.fetch_add(1, Ordering::Relaxed)
        ));
        let mut writer =
            SearchOutOfCoreGenerationWriter::create(&path, Default::default()).unwrap();
        for id in ["a", "c", "e"] {
            writer.push(row(id).into_document()).unwrap();
        }
        writer.finish().unwrap();
        Self(path)
    }
}

impl Drop for Fixture {
    fn drop(&mut self) {
        std::fs::remove_dir_all(&self.0).unwrap();
    }
}

fn row(id: &str) -> SearchProjectionRow {
    SearchProjectionRow {
        kind: SearchProjectionKind::Memory,
        external_id: id.into(),
        title: id.into(),
        body: "delta content".into(),
        embedding: None,
        source_id: None,
        metadata: BTreeMap::new(),
    }
}

fn delta() -> SearchProjectionDelta {
    SearchProjectionDelta {
        upserts: vec![row("z"), row("b"), row("c")],
        deletes: vec!["memory:e".into(), "memory:missing".into()],
        source_graph_commit_epoch: Some(19),
        ..Default::default()
    }
}

#[test]
fn delta_context_survives_prepare_through_finish_and_report_handoff() {
    let root = Fixture::new();
    let reader = SearchOutOfCoreReader::open(&root.0).unwrap();
    let mut base_count = 0;
    let legacy_metrics = reader
        .visit_documents_in_order(&mut |_| {
            base_count += 1;
            Ok(())
        })
        .unwrap();
    assert_eq!(base_count, 3);
    let task = RuntimeTaskContext::default()
        .with_memory_reservation(RuntimeMemoryReservation::new(32 * 1024 * 1024, 0));
    let update = SearchOutOfCoreGenerationWriter::prepare_delta_with_context(
        &reader,
        delta(),
        Default::default(),
        task,
    )
    .unwrap();
    let memory = update.writer.memory.clone();
    let report = update.delta_report();
    assert_eq!(
        update._report_memory.bytes(),
        report.artifact_type.capacity() + report.name.capacity() + report.action.capacity()
    );
    assert!(memory.ledger.snapshot().used_bytes >= update._report_memory.bytes());
    assert_eq!(update.delta_report().after_document_count, 4);
    assert_eq!(update.delta_report().deleted_documents, 1);
    assert_eq!(
        update.source_read_metrics().segment_range_reads,
        legacy_metrics.segment_range_reads
    );
    assert_eq!(
        update.source_read_metrics().segment_bytes_read,
        legacy_metrics.segment_bytes_read
    );
    assert_eq!(
        update.source_read_metrics().hydration_segment_bytes_read,
        legacy_metrics.hydration_segment_bytes_read
    );
    assert!(
        update.source_read_metrics().peak_segment_document_bytes
            <= legacy_metrics.peak_segment_document_bytes
    );
    let (report, built, _) = update.finish().unwrap();
    assert_eq!(built.document_count, 4);
    assert_eq!(memory.ledger.snapshot().used_bytes, 0);
    assert_eq!(report.action, "bounded_generation_update");
    let reader = SearchOutOfCoreReader::open(&root.0).unwrap();
    assert_eq!(reader.source_graph_commit_epoch(), Some(19));
    let ids = ["a", "b", "c", "z"].map(|id| format!("memory:{id}"));
    assert_eq!(
        reader.hydrate_documents(&ids).unwrap().documents,
        ["a", "b", "c", "z"].map(|id| row(id).into_document())
    );
}

#[test]
fn delta_context_cancellation_discards_stage_and_preserves_old_generation() {
    let root = Fixture::new();
    let reader = SearchOutOfCoreReader::open(&root.0).unwrap();
    let task = RuntimeTaskContext::default();
    let update = SearchOutOfCoreGenerationWriter::prepare_delta_with_context(
        &reader,
        delta(),
        Default::default(),
        task.clone(),
    )
    .unwrap();
    let memory = update.writer.memory.clone();
    let stage = update.writer.stage.path.to_path_buf();
    task.cancellation().cancel();
    assert!(update.finish().is_err());
    assert!(!stage.exists());
    assert_eq!(memory.ledger.snapshot().used_bytes, 0);
    let reopened = SearchOutOfCoreReader::open(&root.0).unwrap();
    assert_eq!(reopened.generation(), reader.generation());
    assert_eq!(reopened.document_count(), 3);
    assert!(SearchOutOfCoreGenerationWriter::prepare_delta_with_context(
        &reader,
        delta(),
        Default::default(),
        task
    )
    .is_err());
}

fn assert_no_stage(root: &std::path::Path) {
    assert!(!std::fs::read_dir(root).unwrap().any(|entry| entry
        .unwrap()
        .file_name()
        .to_string_lossy()
        .ends_with(".stage")));
}

#[test]
fn delta_context_input_denial_and_deadline_create_no_stage() {
    let root = Fixture::new();
    let reader = SearchOutOfCoreReader::open(&root.0).unwrap();
    for deadline in [false, true] {
        let mut input = delta();
        input.upserts[0].body.reserve_exact(4 * 1024 * 1024);
        let task = if deadline {
            RuntimeTaskContext::new(Default::default(), Some(std::time::Instant::now()))
        } else {
            RuntimeTaskContext::default()
                .with_memory_reservation(RuntimeMemoryReservation::new(2 * 1024 * 1024, 0))
        };
        assert!(SearchOutOfCoreGenerationWriter::prepare_delta_with_context(
            &reader,
            input,
            Default::default(),
            task
        )
        .is_err());
        assert_no_stage(&root.0);
        assert_eq!(
            SearchOutOfCoreReader::open(&root.0).unwrap().generation(),
            reader.generation()
        );
    }
}

#[test]
fn delta_context_corrupt_base_never_publishes_partial_spool() {
    use std::io::{Seek, SeekFrom, Write};
    let root = Fixture::new();
    let reader = SearchOutOfCoreReader::open(&root.0).unwrap();
    let path = root.0.join(&reader.manifest.payload_file);
    let bytes = std::fs::read(&path).unwrap();
    let mut file = std::fs::OpenOptions::new().write(true).open(&path).unwrap();
    file.seek(SeekFrom::End(-1)).unwrap();
    file.write_all(&[bytes[bytes.len() - 1] ^ 1]).unwrap();
    let error = SearchOutOfCoreGenerationWriter::prepare_delta_with_context(
        &reader,
        delta(),
        Default::default(),
        RuntimeTaskContext::default(),
    )
    .unwrap_err();
    assert!(error.to_string().contains("checksum"), "{error}");
    assert_no_stage(&root.0);
    file.seek(SeekFrom::End(-1)).unwrap();
    file.write_all(&bytes[bytes.len() - 1..]).unwrap();
    drop(file);
    let reopened = SearchOutOfCoreReader::open(&root.0).unwrap();
    assert_eq!(reopened.generation(), reader.generation());
    let ids = ["a", "c", "e"].map(|id| format!("memory:{id}"));
    assert_eq!(
        reopened.hydrate_documents(&ids).unwrap().documents,
        ["a", "c", "e"].map(|id| row(id).into_document())
    );
}

#[test]
fn delta_finish_rechecks_consumer_ownership_before_publication() {
    use crate::out_of_core::{SearchProjectionPublishLease, OUT_OF_CORE_MANIFEST_FILE};
    use crate::SEARCH_SNAPSHOT_FILE;

    let header = "HAWDB_SEARCH_PROJECTION_V1\nprojection_consumer_binding\towner\n";
    for with_context in [false, true] {
        for binding in [
            None,
            Some(header.as_bytes().to_vec()),
            Some(crate::encode_search_snapshot_text(header).unwrap()),
        ] {
            let root = Fixture::new();
            let reader = SearchOutOfCoreReader::open(&root.0).unwrap();
            let manifest = root.0.join(OUT_OF_CORE_MANIFEST_FILE);
            let before = std::fs::read(&manifest).unwrap();
            let update = if with_context {
                SearchOutOfCoreGenerationWriter::prepare_delta_with_context(
                    &reader,
                    delta(),
                    Default::default(),
                    RuntimeTaskContext::default().with_memory_reservation(
                        RuntimeMemoryReservation::new(32 * 1024 * 1024, 0),
                    ),
                )
            } else {
                SearchOutOfCoreGenerationWriter::prepare_delta(&reader, delta(), Default::default())
            }
            .unwrap();
            let memory = update.writer.memory.clone();
            let snapshot = root.0.join(SEARCH_SNAPSHOT_FILE);
            assert!(!snapshot.exists());
            // Ownership can change after preparation has already consumed input.
            let owner = if let Some(bytes) = &binding {
                std::fs::write(&snapshot, bytes).unwrap();
                None
            } else {
                Some(SearchProjectionPublishLease::acquire_for_consumer(&root.0).unwrap())
            };
            let expected = if binding.is_some() {
                "registered projection requires its consumer owner"
            } else {
                "another search projection publication is active"
            };
            let error = update.finish().unwrap_err();
            assert!(error.to_string().contains(expected), "{error}");
            assert_eq!(memory.ledger.snapshot().used_bytes, 0);
            assert_no_stage(&root.0);
            assert_eq!(std::fs::read(&manifest).unwrap(), before);
            if let Some(bytes) = binding {
                assert_eq!(std::fs::read(&snapshot).unwrap(), bytes);
            }
            let ids = ["a", "c", "e"].map(|id| format!("memory:{id}"));
            let reopened = SearchOutOfCoreReader::open(&root.0).unwrap();
            assert_eq!(reopened.generation(), reader.generation());
            assert_eq!(
                reopened.hydrate_documents(&ids).unwrap().documents,
                ["a", "c", "e"].map(|id| row(id).into_document())
            );
            drop(owner);
            drop(SearchProjectionPublishLease::acquire_for_consumer(&root.0).unwrap());
        }
    }
}
