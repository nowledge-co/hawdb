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
use hawdb_core::{RuntimeCancellationToken, RuntimeMemoryReservation};
use std::fs;
use std::mem::size_of;
use std::sync::atomic::{AtomicU64, Ordering};

const BUDGET: usize = 32 * 1024 * 1024;

struct Fixture(PathBuf);

impl Fixture {
    fn new() -> Self {
        static NEXT: AtomicU64 = AtomicU64::new(0);
        let path = std::env::temp_dir().join(format!(
            "hawdb-lexical-artifact-memory-{}-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos(),
            NEXT.fetch_add(1, Ordering::Relaxed),
        ));
        fs::create_dir(&path).unwrap();
        Self(path)
    }

    fn builder(&self, memory: &BuildMemory, task: &RuntimeTaskContext) -> ArtifactBuilder {
        ArtifactBuilder::new_with_context(
            &self.0.join("artifact.hawdb"),
            7,
            LexicalProjectionConfig::default(),
            memory.clone(),
            task.clone(),
        )
        .unwrap()
    }
}

impl Drop for Fixture {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}

fn context() -> (BuildMemory, RuntimeTaskContext) {
    let task = RuntimeTaskContext::default()
        .with_memory_reservation(RuntimeMemoryReservation::new(BUDGET as u64, 0));
    (BuildMemory::new(&task).unwrap(), task)
}

fn used(memory: &BuildMemory) -> usize {
    memory.ledger.snapshot().used_bytes
}

#[test]
fn retained_artifact_terms_release_merge_progress_after_the_input_drops() {
    use crate::build_memory::reserved::ReservedMemory;
    use crate::build_term::Term;
    let fixture = Fixture::new();
    let (memory, task) = context();
    let mut builder = fixture.builder(&memory, &task);
    let capacity = Term::reserved_bytes(5).unwrap();
    let progress = ReservedMemory::new(&memory.spool, capacity).unwrap();
    let term = Term::build_reserved(5, &progress, || Ok("alpha".to_owned())).unwrap();
    let original = term.as_ptr();
    let posting = Posting {
        term,
        ordinal: 0,
        term_frequency: 2,
    };
    builder.push_posting(&posting).unwrap();
    assert_ne!(builder.posting_pending[0].term.as_ptr(), original);
    assert_eq!(builder.posting_strings.bytes(), 5);
    assert!(progress.reserve(1).is_err());
    drop(posting);
    drop(
        progress
            .reserve(capacity)
            .expect("retained artifact must release all temporary merge grants"),
    );
    drop(progress);
    assert_eq!(builder.posting_pending[0].term.as_str(), "alpha");
    drop(builder);
    assert_eq!(used(&memory), 0);
}

#[test]
fn artifact_merge_uses_its_own_task_and_retains_the_supplied_progress() {
    use crate::lexical_projection::{spill_memory::PendingPostings, SpillRuns};
    let fixture = Fixture::new();
    let (memory, task) = context();
    let spill_task = RuntimeTaskContext::default();
    let mut pool = SpillRuns::with_context(
        &fixture.0,
        7,
        Default::default(),
        memory.clone(),
        spill_task.clone(),
    )
    .unwrap();
    pool.prepare(5, 0).unwrap();
    let mut pending = PendingPostings::new(Some(&memory)).unwrap();
    pending
        .push(
            crate::build_term::Term::copy("alpha", Some(&memory)).unwrap(),
            0,
            1,
        )
        .unwrap();
    pending.flush(&mut pool).unwrap();
    drop(pending);
    let mut builder = fixture.builder(&memory, &task);
    builder.push_document(0, "document", 1).unwrap();
    builder.finish_documents().unwrap();
    spill_task.cancellation().cancel();
    builder
        .merge_postings_with_control(&pool.paths, pool.config, &pool.control)
        .unwrap();
    assert_eq!(builder.posting_count, 1);
    assert!(pool.check().unwrap_err().to_string().contains("cancel"));
    drop((builder, pool));
    assert_eq!(used(&memory), 0);
}

#[test]
fn writer_buffer_admission_precedes_file_creation() {
    let fixture = Fixture::new();
    let (memory, task) = context();
    let path = fixture.0.join("artifact.hawdb");
    let path_bytes = path.as_os_str().as_encoded_bytes().len();
    let blocker = memory
        .input
        .reserve(BUDGET - path_bytes - SPILL_IO_BUFFER_BYTES + 1)
        .unwrap();
    assert!(
        ArtifactBuilder::new_with_context(&path, 7, Default::default(), memory.clone(), task,)
            .is_err()
    );
    assert!(!path.exists());
    assert_eq!(used(&memory), blocker.bytes());
    drop(blocker);
    assert_eq!(used(&memory), 0);
}

#[test]
fn document_copy_denial_poisoning_retains_only_admitted_slots() {
    let fixture = Fixture::new();
    let (memory, task) = context();
    let mut builder = fixture.builder(&memory, &task);
    let initial = used(&memory);
    let id = "document";
    let slots = 4 * size_of::<(String, u32)>();
    let blocker = memory
        .input
        .reserve(BUDGET - initial - slots - id.len() + 1)
        .unwrap();
    assert!(builder
        .push_document(0, id, 1)
        .unwrap_err()
        .to_string()
        .contains("query memory"));
    assert!(builder.document_pending.is_empty());
    assert_eq!(builder.document_slots.bytes(), slots);
    assert_eq!(builder.document_strings.bytes(), 0);
    drop(blocker);
    assert!(builder
        .push_document(0, id, 1)
        .unwrap_err()
        .to_string()
        .contains("poisoned"));
    assert!(builder.finish().is_err());
    assert_eq!(used(&memory), 0);
}

#[test]
fn block_directory_key_denial_precedes_payload_io() {
    let fixture = Fixture::new();
    let (memory, task) = context();
    let mut builder = fixture.builder(&memory, &task);
    builder.push_document(0, "document", 1).unwrap();
    let buffered = builder.writer.buffer().to_vec();
    let path = fixture.0.join("artifact.hawdb");
    let physical = fs::read(&path).unwrap();
    let slots = 4 * size_of::<BlockDescriptor>();
    let keys = 2 * "document".len();
    let blocker = memory
        .input
        .reserve(BUDGET - used(&memory) - slots - keys + 1)
        .unwrap();
    assert!(builder
        .finish_documents()
        .unwrap_err()
        .to_string()
        .contains("query memory"));
    assert_eq!(builder.writer.buffer(), buffered);
    assert_eq!(fs::read(&path).unwrap(), physical);
    assert!(builder.blocks.is_empty());
    drop(blocker);
    assert!(builder
        .finish_documents()
        .unwrap_err()
        .to_string()
        .contains("poisoned"));
    drop(builder);
    assert_eq!(used(&memory), 0);
}

#[test]
fn pending_capacity_and_summary_directory_follow_payload_lifetimes() {
    let fixture = Fixture::new();
    let (memory, task) = context();
    let mut builder = fixture.builder(&memory, &task);
    builder.push_document(0, "a", 2).unwrap();
    builder.finish_documents().unwrap();
    assert_eq!(builder.document_strings.bytes(), 0);
    assert_eq!(
        builder.document_slots.bytes(),
        builder.document_pending.capacity() * size_of::<(String, u32)>()
    );
    assert_ne!(builder.document_pending.capacity(), 0);
    let posting = Posting {
        term: "alpha".into(),
        ordinal: 0,
        term_frequency: 2,
    };
    builder.push_posting(&posting).unwrap();
    builder.merge_postings(&[], Default::default()).unwrap();
    assert_eq!(builder.posting_strings.bytes(), 0);
    assert_eq!(
        builder.posting_slots.bytes(),
        builder.posting_pending.capacity() * size_of::<Posting>()
    );
    let summary = builder.finish().unwrap();
    let directory_bytes = summary.blocks.capacity() * size_of::<BlockDescriptor>()
        + summary
            .blocks
            .iter()
            .map(|block| block.min_key.capacity() + block.max_key.capacity())
            .sum::<usize>();
    assert_eq!(used(&memory), directory_bytes);
    let blocker = memory.input.reserve(BUDGET - directory_bytes).unwrap();
    assert!(memory.spool.reserve(1).is_err());
    assert_eq!(summary.posting_count, 1);
    drop(summary);
    assert_eq!(used(&memory), blocker.bytes());
    drop(blocker);
    assert_eq!(used(&memory), 0);
    assert_eq!(memory.ledger.snapshot().account_count, 3);
}

#[test]
fn failed_payload_io_poisoning_prevents_continuing_the_artifact() {
    let fixture = Fixture::new();
    let (memory, task) = context();
    let mut builder = fixture.builder(&memory, &task);
    builder
        .push_document(0, &"x".repeat(SPILL_IO_BUFFER_BYTES * 2), 1)
        .unwrap();
    builder.writer.flush().unwrap();
    // A read-only file deterministically fails when the block reaches the device.
    builder.writer =
        BufWriter::with_capacity(SPILL_IO_BUFFER_BYTES, File::open(&builder.path).unwrap());
    assert!(builder.finish_documents().is_err());
    assert!(builder.blocks.is_empty());
    assert!(builder
        .push_document(1, "later", 1)
        .unwrap_err()
        .to_string()
        .contains("poisoned"));
    assert!(builder.finish().is_err());
    assert_eq!(used(&memory), 0);
}

#[test]
fn cancellation_preserves_uncommitted_block_and_releases_owners() {
    let fixture = Fixture::new();
    let cancellation = RuntimeCancellationToken::new();
    let task = RuntimeTaskContext::without_deadline(cancellation.clone());
    let memory = BuildMemory::new(&task).unwrap();
    let mut builder = fixture.builder(&memory, &task);
    builder.push_document(0, "document", 1).unwrap();
    let offset = builder.offset;
    cancellation.cancel();
    assert!(builder.finish_documents().is_err());
    assert_eq!(builder.offset, offset);
    assert!(builder.blocks.is_empty());
    drop(builder);
    assert_eq!(used(&memory), 0);
}

#[test]
fn posting_copies_are_admitted_before_allocating_payloads() {
    let fixture = Fixture::new();
    let posting = Posting {
        term: "alpha".into(),
        ordinal: 0,
        term_frequency: 1,
    };
    let posting_slots = 4 * size_of::<Posting>();
    let capacities = [posting_slots, posting_slots + posting.term.len()];
    for required in capacities {
        let (memory, task) = context();
        let mut builder = fixture.builder(&memory, &task);
        let blocker = memory
            .input
            .reserve(BUDGET - used(&memory) - required + 1)
            .unwrap();
        let buffered = builder.writer.buffer().to_vec();
        assert!(builder
            .push_posting(&posting)
            .unwrap_err()
            .to_string()
            .contains("query memory"));
        assert!(builder.posting_pending.is_empty());
        assert_eq!(builder.posting_strings.bytes(), 0);
        assert_eq!(builder.writer.buffer(), buffered);
        drop(blocker);
        assert!(builder
            .push_posting(&posting)
            .unwrap_err()
            .to_string()
            .contains("poisoned"));
        drop(builder);
        assert_eq!(used(&memory), 0);
    }
}

#[test]
fn scanned_writer_shares_admission_and_keeps_the_previous_projection_on_denial() {
    use super::super::{LexicalProjectionReader, LexicalProjectionWriter};
    use crate::{SearchAnalyzerLexicon, SearchDocument};
    use std::collections::BTreeMap;

    let fixture = Fixture::new();
    let analyzer = SearchAnalyzerLexicon::default();
    let document = SearchDocument {
        id: "a".into(),
        title: "alpha".into(),
        content: "beta".into(),
        embedding: None,
        metadata: BTreeMap::new(),
    };
    let config = LexicalProjectionConfig::default();
    let previous = LexicalProjectionWriter::new(config)
        .write(
            &fixture.0,
            1,
            None,
            11,
            13,
            std::iter::once(&document),
            &analyzer,
        )
        .unwrap();
    let files = || {
        fs::read_dir(&fixture.0)
            .unwrap()
            .map(|entry| {
                let path = entry.unwrap().path();
                (
                    path.file_name().unwrap().to_owned(),
                    fs::read(path).unwrap(),
                )
            })
            .collect::<BTreeMap<_, _>>()
    };
    let before = files();
    let (memory, task) = context();
    let mut blocker = None;
    let result = LexicalProjectionWriter::new(config)
        .with_context(memory.clone(), task)
        .write_scanned(
            &fixture.0,
            2,
            None,
            11,
            13,
            |consume| {
                consume(0, &document)?;
                assert!(used(&memory) > 0);
                blocker = Some(memory.input.reserve(BUDGET - used(&memory)).unwrap());
                Ok(())
            },
            &analyzer,
        );
    assert!(result.unwrap_err().to_string().contains("query memory"));
    assert_eq!(files(), before);
    let reopened = LexicalProjectionReader::load(&fixture.0, None, 11, 13, config)
        .unwrap()
        .unwrap();
    assert_eq!(reopened.manifest, previous.manifest);
    assert_eq!(used(&memory), blocker.as_ref().unwrap().bytes());
    drop(blocker);
    assert_eq!(used(&memory), 0);
}

#[test]
fn retained_artifact_shares_a_tracked_resident_term_and_its_admission() {
    use crate::build_term::Term;
    let fixture = Fixture::new();
    let (memory, task) = context();
    let mut builder = fixture.builder(&memory, &task);
    let term = Term::copy("alpha", Some(&memory)).unwrap();
    let address = term.as_ptr();
    let posting = Posting {
        term,
        ordinal: 0,
        term_frequency: 2,
    };
    builder.push_posting(&posting).unwrap();
    assert_eq!(builder.posting_pending[0].term.as_ptr(), address);
    assert_eq!(builder.posting_strings.bytes(), 0);
    let before = used(&memory);
    drop(posting);
    assert_eq!(used(&memory), before);
    assert_eq!(builder.posting_pending[0].term.as_str(), "alpha");
    drop(builder);
    assert_eq!(used(&memory), 0);
}
