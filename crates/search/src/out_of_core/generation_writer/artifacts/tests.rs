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

use super::super::tests::{document, test_dir};
use super::*;
use hawdb_core::RuntimeMemoryReservation;
use std::fs;

fn context() -> RuntimeTaskContext {
    RuntimeTaskContext::default()
        .with_memory_reservation(RuntimeMemoryReservation::new(32 * 1024 * 1024, 0))
}

#[test]
fn finished_layout_keeps_its_capacity_after_segment_state_is_released() {
    let root = test_dir("segment_layout_ownership");
    fs::create_dir(&root).unwrap();
    let task = context();
    let memory = BuildMemory::new(&task).unwrap();
    let fields = BTreeSet::from(["kind".into(), "group".into()]);
    let options = SearchOutOfCoreGenerationBuildOptions::default();
    let mut builder =
        SegmentArtifactBuilder::new_with_context(&root, 1, &fields, &options, memory.clone(), task)
            .unwrap();
    builder.push(0, document(0)).unwrap();
    let output = builder.finish(1).unwrap();
    let retained = output.layout.format.capacity()
        + output.layout.segments.capacity() * std::mem::size_of::<SearchOutOfCoreSegmentLayout>();
    assert_eq!(memory.ledger.snapshot().used_bytes, retained);
    assert!(retained > 0);
    let rest = memory
        .retained
        .reserve(32 * 1024 * 1024 - retained)
        .unwrap();
    assert!(memory.input.reserve(1).is_err());
    drop(output);
    assert_eq!(memory.ledger.snapshot().used_bytes, rest.bytes());
    drop(rest);
    assert_eq!(memory.ledger.snapshot().used_bytes, 0);
    assert_eq!(memory.ledger.snapshot().account_count, 3);
    fs::remove_dir_all(root).unwrap();
}

#[test]
fn segment_builder_rejects_an_ordinal_gap_before_mutating() {
    let root = test_dir("segment_document_ordinal_gap");
    fs::create_dir(&root).unwrap();
    let fields = BTreeSet::new();
    let options = SearchOutOfCoreGenerationBuildOptions::default();
    let mut builder = SegmentArtifactBuilder::new(&root, 1, &fields, &options).unwrap();

    let error = builder.push(1, document(0)).unwrap_err();

    assert!(error.to_string().contains("document ordinal"));
    assert!(builder.documents.is_empty());
    assert_eq!(builder.next_document_ordinal, 0);
    fs::remove_dir_all(root).unwrap();
}

#[test]
fn partial_segment_io_failure_poisoning_prevents_finish_and_releases_ownership() {
    let root = test_dir("segment_partial_io");
    fs::create_dir(&root).unwrap();
    let task = context();
    let memory = BuildMemory::new(&task).unwrap();
    let fields = BTreeSet::from(["kind".into()]);
    let options = SearchOutOfCoreGenerationBuildOptions::default();
    let mut builder =
        SegmentArtifactBuilder::new_with_context(&root, 1, &fields, &options, memory.clone(), task)
            .unwrap();
    builder.push(0, document(0)).unwrap();
    // A portable read-only descriptor fails after the document payload write.
    builder.metadata_file = File::open(root.join(STAGE_METADATA_FILE)).unwrap();
    assert!(builder.flush_segment().is_err());
    assert!(builder.document_file.metadata().unwrap().len() > 0);
    assert_eq!(builder.metadata_offset, 0);
    assert!(builder.descriptor.segments.is_empty());
    assert!(builder.layouts.is_empty());
    assert!(builder
        .push(1, document(1))
        .unwrap_err()
        .to_string()
        .contains("already failed"));
    assert!(builder.finish(1).is_err());
    assert_eq!(memory.ledger.snapshot().used_bytes, 0);
    fs::remove_dir_all(root).unwrap();
}

#[test]
fn remove_on_drop_guard_removes_the_file_unless_disarmed() {
    let root = test_dir("remove_on_drop_guard");
    fs::create_dir(&root).unwrap();

    let armed_path = root.join("armed.tmp");
    fs::write(&armed_path, b"armed").unwrap();
    drop(RemoveOnDrop::new(&armed_path));
    assert!(!armed_path.exists());

    let disarmed_path = root.join("disarmed.tmp");
    fs::write(&disarmed_path, b"disarmed").unwrap();
    let mut guard = RemoveOnDrop::new(&disarmed_path);
    guard.disarm();
    drop(guard);
    assert!(disarmed_path.exists());

    fs::remove_dir_all(root).unwrap();
}
