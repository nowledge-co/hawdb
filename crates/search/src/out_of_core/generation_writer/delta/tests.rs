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
use crate::lexical_projection::DocumentsDigest;
use crate::{SearchProjectionKind, SearchProjectionRow};
use hawdb_core::RuntimeMemoryReservation;
use std::collections::{BTreeMap, BTreeSet};
use std::fs;
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

fn append(root: &PathBuf, document: SearchProjectionRow) {
    let reader = SearchOutOfCoreReader::open(root).unwrap();
    let update = SearchOutOfCoreGenerationWriter::prepare_delta(
        &reader,
        SearchProjectionDelta {
            upserts: vec![document],
            ..Default::default()
        },
        Default::default(),
    )
    .unwrap();
    update.finish().unwrap();
}

#[test]
fn append_delta_publishes_a_second_artifact_without_hydrating_the_base() {
    let root = Fixture::new();
    let reader = SearchOutOfCoreReader::open(&root.0).unwrap();
    let base_generation = reader.generation();
    let update = SearchOutOfCoreGenerationWriter::prepare_delta(
        &reader,
        SearchProjectionDelta {
            upserts: vec![row("z"), row("y")],
            source_graph_commit_epoch: Some(19),
            ..Default::default()
        },
        Default::default(),
    )
    .unwrap();
    assert_eq!(update.delta_report().action, "incremental_segment_append");
    assert_eq!(update.delta_report().before_document_count, 3);
    assert_eq!(update.delta_report().after_document_count, 5);
    assert_eq!(update.source_read_metrics().segment_range_reads, 0);
    assert_eq!(update.source_read_metrics().segment_bytes_read, 0);
    assert_eq!(update.source_read_metrics().hydrated_documents, 0);

    let (report, build, source_metrics) = update.finish().unwrap();
    assert_eq!(report.action, "incremental_segment_append");
    assert_eq!(build.document_count, 5);
    assert_eq!(source_metrics.segment_range_reads, 0);

    let reader = SearchOutOfCoreReader::open(&root.0).unwrap();
    assert_eq!(reader.generation(), build.generation);
    assert_eq!(reader.document_count(), 5);
    assert_eq!(reader.source_graph_commit_epoch(), Some(19));
    assert_eq!(reader.manifest.segments.len(), 2);
    assert_eq!(reader.manifest.segments[0].generation, base_generation);
    assert_eq!(reader.manifest.segments[1].generation, build.generation);
    let ids = ["a", "c", "e", "y", "z"].map(|id| format!("memory:{id}"));
    assert_eq!(
        reader.hydrate_documents(&ids).unwrap().documents,
        ["a", "c", "e", "y", "z"].map(|id| row(id).into_document())
    );
    #[cfg(feature = "full-text-search")]
    {
        let output = reader
            .search_with_options(
                "delta",
                None,
                crate::SearchMode::Text,
                crate::SearchQueryOptions {
                    limit: 5,
                    offset: 0,
                    rank_window: None,
                    fusion_weights: Default::default(),
                    metadata_filters: BTreeMap::new(),
                    policy_epoch: None,
                },
            )
            .unwrap();
        assert_eq!(output.result.total_hits, 5);
    }
}

#[test]
fn non_append_update_retains_the_ordered_base_hydration_path() {
    let root = Fixture::new();
    let reader = SearchOutOfCoreReader::open(&root.0).unwrap();
    let update = SearchOutOfCoreGenerationWriter::prepare_delta(
        &reader,
        SearchProjectionDelta {
            upserts: vec![row("b")],
            ..Default::default()
        },
        Default::default(),
    )
    .unwrap();
    assert_eq!(update.delta_report().action, "bounded_generation_update");
    assert_eq!(update.source_read_metrics().hydrated_documents, 3);
    let (report, build, _) = update.finish().unwrap();
    assert_eq!(report.after_document_count, 4);
    assert_eq!(build.document_count, 4);
    let reader = SearchOutOfCoreReader::open(&root.0).unwrap();
    let ids = ["a", "b", "c", "e"].map(|id| format!("memory:{id}"));
    assert_eq!(
        reader.hydrate_documents(&ids).unwrap().documents,
        ["a", "b", "c", "e"].map(|id| row(id).into_document())
    );
}

#[test]
fn mutation_target_resolution_binds_documents_to_their_manifest_content_segment() {
    let root = Fixture::new();
    append(&root.0, row("z"));

    let reader = SearchOutOfCoreReader::open(&root.0).unwrap();
    let targets = reader
        .resolve_mutation_targets(&BTreeSet::from([
            "memory:a".to_string(),
            "memory:z".to_string(),
        ]))
        .unwrap();
    assert_eq!(targets.metrics.hydrated_documents, 2);
    assert_eq!(targets.metrics.segment_range_reads, 2);
    assert_eq!(
        targets.targets["memory:a"].content_segment_id,
        reader.manifest.segments[0].segment_id
    );
    assert_eq!(
        targets.targets["memory:a"].document,
        row("a").into_document()
    );
    assert_eq!(
        targets.targets["memory:z"].content_segment_id,
        reader.manifest.segments[1].segment_id
    );
    assert_eq!(
        targets.targets["memory:z"].document,
        row("z").into_document()
    );
}

#[test]
fn replacement_rewrites_only_the_current_content_segment() {
    let root = Fixture::new();
    append(&root.0, row("z"));
    let reader = SearchOutOfCoreReader::open(&root.0).unwrap();
    let before = reader.manifest.segments.clone();
    let full_metrics = reader.visit_documents_in_order(&mut |_| Ok(())).unwrap();
    let mut replacement = row("c");
    replacement.title = "updated c".to_string();
    replacement.body = "fresh mutation content".to_string();
    let expected = replacement.clone().into_document();

    let update = SearchOutOfCoreGenerationWriter::prepare_delta(
        &reader,
        SearchProjectionDelta {
            upserts: vec![replacement],
            ..Default::default()
        },
        Default::default(),
    )
    .unwrap();
    assert_eq!(update.delta_report().action, "incremental_segment_replace");
    assert_eq!(update.delta_report().before_document_count, 4);
    assert_eq!(update.delta_report().after_document_count, 4);
    assert_eq!(update.source_read_metrics().hydrated_documents, 3);
    assert!(update.source_read_metrics().segment_range_reads < full_metrics.segment_range_reads);
    assert!(update.source_read_metrics().segment_bytes_read < full_metrics.segment_bytes_read);
    let (_, build, _) = update.finish().unwrap();
    assert_eq!(build.document_count, 4);

    let reader = SearchOutOfCoreReader::open(&root.0).unwrap();
    assert_eq!(reader.manifest.segments.len(), 2);
    assert_eq!(reader.manifest.segments[0].segment_id, before[0].segment_id);
    assert_eq!(reader.manifest.segments[0].level, before[0].level);
    assert_ne!(
        reader.manifest.segments[0].payload_file,
        before[0].payload_file
    );
    assert_eq!(reader.manifest.segments[1].generation, before[1].generation);
    assert_eq!(
        reader.manifest.segments[1].payload_file,
        before[1].payload_file
    );
    let ids = ["a", "c", "e", "z"].map(|id| format!("memory:{id}"));
    assert_eq!(
        reader.hydrate_documents(&ids).unwrap().documents,
        vec![
            row("a").into_document(),
            expected,
            row("e").into_document(),
            row("z").into_document()
        ]
    );
    #[cfg(feature = "full-text-search")]
    {
        let output = reader
            .search_with_options(
                "fresh",
                None,
                crate::SearchMode::Text,
                crate::SearchQueryOptions {
                    limit: 5,
                    offset: 0,
                    rank_window: None,
                    fusion_weights: Default::default(),
                    metadata_filters: BTreeMap::new(),
                    policy_epoch: None,
                },
            )
            .unwrap();
        assert_eq!(output.result.total_hits, 1);
        assert_eq!(output.result.hits[0].id, "memory:c");
    }
}

#[test]
fn deletion_rewrites_only_the_current_content_segment_and_updates_manifest_identity() {
    let root = Fixture::new();
    append(&root.0, row("z"));
    let reader = SearchOutOfCoreReader::open(&root.0).unwrap();
    let before = reader.manifest.clone();
    let full_metrics = reader.visit_documents_in_order(&mut |_| Ok(())).unwrap();

    let update = SearchOutOfCoreGenerationWriter::prepare_delta(
        &reader,
        SearchProjectionDelta {
            deletes: vec!["memory:c".to_string()],
            ..Default::default()
        },
        Default::default(),
    )
    .unwrap();
    assert_eq!(update.delta_report().action, "incremental_segment_replace");
    assert_eq!(update.delta_report().before_document_count, 4);
    assert_eq!(update.delta_report().after_document_count, 3);
    assert_eq!(update.delta_report().deleted_documents, 1);
    assert_eq!(update.source_read_metrics().hydrated_documents, 3);
    assert!(update.source_read_metrics().segment_range_reads < full_metrics.segment_range_reads);
    assert!(update.source_read_metrics().segment_bytes_read < full_metrics.segment_bytes_read);
    let (_, build, _) = update.finish().unwrap();
    assert_eq!(build.document_count, 3);

    let reader = SearchOutOfCoreReader::open(&root.0).unwrap();
    assert_eq!(reader.manifest.segments.len(), 2);
    assert_eq!(
        reader.manifest.segments[0].segment_id,
        before.segments[0].segment_id
    );
    assert_eq!(
        reader.manifest.segments[1].generation,
        before.segments[1].generation
    );
    assert_eq!(
        reader.manifest.segments[1].payload_file,
        before.segments[1].payload_file
    );
    assert_eq!(
        reader.manifest.documents_digest,
        DocumentsDigest::replace(
            before.documents_digest,
            before.segments[0].documents_digest,
            reader.manifest.segments[0].documents_digest,
        )
    );
    let ids = ["a", "e", "z"].map(|id| format!("memory:{id}"));
    assert_eq!(
        reader.hydrate_documents(&ids).unwrap().documents,
        ["a", "e", "z"].map(|id| row(id).into_document())
    );
    assert!(reader.hydrate_documents(&["memory:c".to_string()]).is_err());
}

#[test]
fn same_segment_batch_replaces_and_deletes_without_hydrating_other_artifacts() {
    let root = Fixture::new();
    append(&root.0, row("z"));
    let reader = SearchOutOfCoreReader::open(&root.0).unwrap();
    let before = reader.manifest.segments.clone();
    let full_metrics = reader.visit_documents_in_order(&mut |_| Ok(())).unwrap();
    let mut replacement = row("a");
    replacement.body = "batched mutation content".to_string();
    let expected = replacement.clone().into_document();

    let update = SearchOutOfCoreGenerationWriter::prepare_delta(
        &reader,
        SearchProjectionDelta {
            upserts: vec![replacement],
            deletes: vec!["memory:c".to_string()],
            ..Default::default()
        },
        Default::default(),
    )
    .unwrap();
    assert_eq!(update.delta_report().action, "incremental_segment_replace");
    assert_eq!(update.delta_report().before_document_count, 4);
    assert_eq!(update.delta_report().after_document_count, 3);
    assert_eq!(update.delta_report().upserted_documents, 1);
    assert_eq!(update.delta_report().deleted_documents, 1);
    assert_eq!(update.source_read_metrics().hydrated_documents, 3);
    assert!(update.source_read_metrics().segment_range_reads < full_metrics.segment_range_reads);
    assert!(update.source_read_metrics().segment_bytes_read < full_metrics.segment_bytes_read);
    update.finish().unwrap();

    let reader = SearchOutOfCoreReader::open(&root.0).unwrap();
    assert_eq!(reader.manifest.segments[0].segment_id, before[0].segment_id);
    assert_eq!(reader.manifest.segments[1].generation, before[1].generation);
    let ids = ["a", "e", "z"].map(|id| format!("memory:{id}"));
    assert_eq!(
        reader.hydrate_documents(&ids).unwrap().documents,
        vec![expected, row("e").into_document(), row("z").into_document()]
    );
}

#[test]
fn contiguous_segment_batch_replaces_only_its_exact_artifact_range() {
    let root = Fixture::new();
    append(&root.0, row("y"));
    append(&root.0, row("z"));
    let reader = SearchOutOfCoreReader::open(&root.0).unwrap();
    let before = reader.manifest.segments.clone();
    let full_metrics = reader.visit_documents_in_order(&mut |_| Ok(())).unwrap();
    let mut first = row("a");
    first.body = "range mutation first".to_string();
    let mut second = row("y");
    second.body = "range mutation second".to_string();
    let expected_first = first.clone().into_document();
    let expected_second = second.clone().into_document();

    let update = SearchOutOfCoreGenerationWriter::prepare_delta(
        &reader,
        SearchProjectionDelta {
            upserts: vec![first, second],
            ..Default::default()
        },
        Default::default(),
    )
    .unwrap();
    assert_eq!(
        update.delta_report().action,
        "incremental_segment_range_replace"
    );
    assert_eq!(update.delta_report().before_document_count, 5);
    assert_eq!(update.delta_report().after_document_count, 5);
    assert_eq!(update.source_read_metrics().hydrated_documents, 4);
    assert!(update.source_read_metrics().segment_range_reads < full_metrics.segment_range_reads);
    assert!(update.source_read_metrics().segment_bytes_read < full_metrics.segment_bytes_read);
    update.finish().unwrap();

    let reader = SearchOutOfCoreReader::open(&root.0).unwrap();
    assert_eq!(reader.manifest.segments.len(), 2);
    assert_eq!(
        reader.manifest.segments[0].segment_id,
        before
            .iter()
            .map(|segment| segment.segment_id)
            .max()
            .unwrap()
            + 1
    );
    assert_eq!(reader.manifest.segments[0].level, before[0].level);
    assert_eq!(reader.manifest.segments[1].segment_id, before[2].segment_id);
    assert_eq!(reader.manifest.segments[1].generation, before[2].generation);
    let ids = ["a", "c", "e", "y", "z"].map(|id| format!("memory:{id}"));
    assert_eq!(
        reader.hydrate_documents(&ids).unwrap().documents,
        vec![
            expected_first,
            row("c").into_document(),
            row("e").into_document(),
            expected_second,
            row("z").into_document(),
        ]
    );
}

#[test]
fn noncontiguous_segment_batch_retains_the_full_generation_path() {
    let root = Fixture::new();
    append(&root.0, row("y"));
    append(&root.0, row("z"));
    let reader = SearchOutOfCoreReader::open(&root.0).unwrap();
    let full_metrics = reader.visit_documents_in_order(&mut |_| Ok(())).unwrap();

    let update = SearchOutOfCoreGenerationWriter::prepare_delta(
        &reader,
        SearchProjectionDelta {
            upserts: vec![row("a"), row("z")],
            ..Default::default()
        },
        Default::default(),
    )
    .unwrap();
    assert_eq!(update.delta_report().action, "bounded_generation_update");
    assert_eq!(update.source_read_metrics().hydrated_documents, 5);
    assert_eq!(
        update.source_read_metrics().segment_range_reads,
        full_metrics.segment_range_reads
    );
    assert_eq!(
        update.source_read_metrics().segment_bytes_read,
        full_metrics.segment_bytes_read
    );
}

#[test]
fn local_replacement_rejects_a_newer_active_generation() {
    let root = Fixture::new();
    append(&root.0, row("z"));
    let reader = SearchOutOfCoreReader::open(&root.0).unwrap();
    let update = SearchOutOfCoreGenerationWriter::prepare_delta(
        &reader,
        SearchProjectionDelta {
            upserts: vec![row("c")],
            ..Default::default()
        },
        Default::default(),
    )
    .unwrap();

    append(&root.0, row("zz"));
    let active = fs::read(root.0.join(crate::out_of_core::OUT_OF_CORE_MANIFEST_FILE)).unwrap();
    let error = update.finish().unwrap_err();
    assert!(error.to_string().contains("base changed"), "{error}");
    assert_eq!(
        fs::read(root.0.join(crate::out_of_core::OUT_OF_CORE_MANIFEST_FILE)).unwrap(),
        active
    );
    assert_no_stage(&root.0);
}

#[test]
fn contiguous_segment_range_replacement_rejects_a_newer_active_generation() {
    let root = Fixture::new();
    append(&root.0, row("y"));
    append(&root.0, row("z"));
    let reader = SearchOutOfCoreReader::open(&root.0).unwrap();
    let update = SearchOutOfCoreGenerationWriter::prepare_delta(
        &reader,
        SearchProjectionDelta {
            upserts: vec![row("a"), row("y")],
            ..Default::default()
        },
        Default::default(),
    )
    .unwrap();
    assert_eq!(
        update.delta_report().action,
        "incremental_segment_range_replace"
    );

    append(&root.0, row("zz"));
    let active = fs::read(root.0.join(crate::out_of_core::OUT_OF_CORE_MANIFEST_FILE)).unwrap();
    let error = update.finish().unwrap_err();
    assert!(error.to_string().contains("base changed"), "{error}");
    assert_eq!(
        fs::read(root.0.join(crate::out_of_core::OUT_OF_CORE_MANIFEST_FILE)).unwrap(),
        active
    );
    assert_no_stage(&root.0);
}

#[test]
fn cancelled_local_replacement_preserves_the_active_manifest() {
    let root = Fixture::new();
    append(&root.0, row("z"));
    let reader = SearchOutOfCoreReader::open(&root.0).unwrap();
    let before = fs::read(root.0.join(crate::out_of_core::OUT_OF_CORE_MANIFEST_FILE)).unwrap();
    let task = RuntimeTaskContext::default();
    let update = SearchOutOfCoreGenerationWriter::prepare_delta_with_context(
        &reader,
        SearchProjectionDelta {
            upserts: vec![row("c")],
            ..Default::default()
        },
        Default::default(),
        task.clone(),
    )
    .unwrap();
    assert!(task.cancellation().cancel());

    let error = update.finish().unwrap_err();
    assert!(error.to_string().contains("cancel"), "{error}");
    assert_eq!(
        fs::read(root.0.join(crate::out_of_core::OUT_OF_CORE_MANIFEST_FILE)).unwrap(),
        before
    );
    assert_no_stage(&root.0);
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
    assert_eq!(update.source_read_metrics().hydrated_documents, base_count);
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
    let path = root.0.join(
        &reader
            .manifest
            .segments
            .first()
            .expect("manifest has a segment")
            .payload_file,
    );
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
