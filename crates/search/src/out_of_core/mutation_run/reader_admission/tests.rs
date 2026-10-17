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
use crate::build_memory::{read::tests::governor, BuildMemory};
use crate::test_allocation as allocation;
use hawdb_core::RuntimeTaskContext;
use hawdb_qos::RuntimeWorkRequest;
use hawdb_storage::file_descriptors::ProjectFileDescriptors;
use std::path::PathBuf;

struct Fixture {
    root: PathBuf,
    manifest: SearchOutOfCoreMutationRunManifest,
    _project: ProjectFileDescriptors,
}
impl Fixture {
    fn new(count: usize, pretty: bool) -> Self {
        Self::with_counts(&[count], pretty)
    }
    fn with_counts(counts: &[usize], pretty: bool) -> Self {
        // Own the domain explicitly: the general test_dir helper retains a
        // second owner in TLS until thread exit, beyond our allocation audit.
        static SEQUENCE: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
        let sequence = SEQUENCE.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let root = crate::test_temp_dir().join(format!(
            "hawdb-admitted-mutation-{}-{sequence}",
            std::process::id()
        ));
        let project = ProjectFileDescriptors::acquire(
            &root,
            hawdb_storage::file_descriptors::DEFAULT_MAX_OPEN_FILES,
        )
        .unwrap();
        let entries = counts
            .iter()
            .enumerate()
            .map(|(entry, &count)| SearchMutationRunEntry {
                document_id: format!("memory:admitted:{entry:06}"),
                target_segment_id: 7,
                operation: SearchMutationOperation::Replace,
                retraction: SearchMutationRetraction {
                    documents_digest: 11,
                    lexical_document_len: count as u64,
                    unique_terms: (0..count)
                        .map(|index| format!("t{index:06}\"\\雪"))
                        .collect::<Vec<_>>()
                        .into(),
                },
            })
            .collect();
        let body = SearchMutationRunBody::new(3, 5, entries).unwrap();
        let encoded = body.encode().unwrap();
        let bytes = if pretty {
            let value: serde_json::Value = serde_json::from_slice(&encoded).unwrap();
            serde_json::to_string_pretty(&value)
                .unwrap()
                .replace("雪", "\\u96ea")
                .into_bytes()
        } else {
            encoded
        };
        let manifest = SearchOutOfCoreMutationRunManifest {
            generation: 3,
            file: artifact_file(3),
            len: bytes.len() as u64,
            checksum: crate::checksum_bytes(&bytes),
            entry_count: counts.len(),
            analyzer_digest: 5,
        };
        std::fs::write(root.join(&manifest.file), &bytes).unwrap();
        Self {
            root,
            manifest,
            _project: project,
        }
    }
}
impl Drop for Fixture {
    fn drop(&mut self) {
        std::fs::remove_dir_all(&self.root).unwrap();
    }
}

fn warm(memory: &BuildMemory, task: &RuntimeTaskContext) {
    for account in [&memory.input, &memory.spool, &memory.retained] {
        drop(account.reserve(1).unwrap());
    }
    drop(task.acquire_io_wave(std::num::NonZeroUsize::MIN).unwrap());
}

#[test]
fn admitted_mutation_terms_keep_capacity_and_require_fresh_execution_after_producer_close() {
    let _serial = allocation::serial();
    assert_eq!(allocation::live(), 0);
    let fixture = Fixture::new(16385, true);
    let governor = governor();
    let request = RuntimeWorkRequest::background_maintenance(2 << 20).with_io_wave_slots(1);
    let permit = governor.try_admit(request).unwrap();
    let task = permit.bind_task_context(Default::default());
    let memory = BuildMemory::new(&task).unwrap();
    warm(&memory, &task);
    let mut budget = MutationRunBudget::new(16 << 20, 1, 1).unwrap();
    let (run, peak) = allocation::measure(|| {
        let admission = ReadAdmission {
            memory: &memory,
            task: &task,
            permit: &permit,
        };
        SearchMutationRun::open_with_read_admission(
            &fixture.root,
            &fixture.manifest,
            32 << 20,
            5,
            &mut budget,
            Some(&admission),
            32 << 20,
        )
        .unwrap()
    });
    assert!(
        peak <= memory.ledger.snapshot().peak_bytes,
        "peak {peak} exceeds admission"
    );
    assert_eq!(run.entries()[0].retraction.unique_terms.len(), 16385);
    let baseline = allocation::live();
    let (terms, clone_peak) =
        allocation::measure(|| run.entries()[0].retraction.unique_terms.clone());
    assert_eq!(clone_peak, baseline);
    assert_eq!(allocation::live(), baseline);
    drop(run);
    let producer_task = task.clone();
    drop(memory);
    drop(task);
    drop(permit);
    let snapshot = governor.snapshot();
    assert_eq!(snapshot.active_cpu_slots, 0);
    assert_eq!(snapshot.active_background_tasks, 0);
    assert_eq!(snapshot.active_background_io_slots, 0);
    assert!(snapshot.admitted_memory_bytes >= allocation::live() as u64);
    assert!(matches!(terms.cursor(), Err(HawDBError::Execution(_))));
    let task_only_ledger = hawdb_executor::QueryMemoryLedger::new(std::num::NonZeroUsize::MIN);
    let task_only_memory = task_only_ledger.account(
        hawdb_executor::QueryMemoryClass::ExternalRead,
        "task-only cursor",
        std::num::NonZeroUsize::MIN,
    );
    let task_only = QueryContext {
        memory: &task_only_memory,
        task: &producer_task,
        execution: None,
    };
    assert!(matches!(
        terms.cursor_with_query_context(Some(task_only)),
        Err(HawDBError::Execution(_))
    ));
    assert_eq!(task_only_ledger.snapshot().used_bytes, 0);
    drop(task_only_memory);
    drop(task_only_ledger);
    drop(producer_task);
    let permit = governor.try_admit(request).unwrap();
    let task = permit.bind_task_context(Default::default());
    let memory = BuildMemory::new(&task).unwrap();
    warm(&memory, &task);
    let admission = ReadAdmission {
        memory: &memory,
        task: &task,
        permit: &permit,
    };
    let context = QueryContext::from_read_admission(&admission);
    let baseline = memory.ledger.snapshot().used_bytes;
    let mut count = 0;
    terms
        .visit_with_query_context(Some(context), |term| {
            assert!(term.starts_with('t'));
            count += 1;
            Ok(())
        })
        .unwrap();
    assert_eq!(count, 16385);
    assert_eq!(memory.ledger.snapshot().used_bytes, baseline);
    {
        let mut cursor = terms.cursor_with_query_context(Some(context)).unwrap();
        assert!(cursor.next().unwrap().is_some());
        task.cancellation().cancel();
        assert!(matches!(cursor.next(), Err(HawDBError::Execution(_))));
    }
    assert_eq!(memory.ledger.snapshot().used_bytes, baseline);
    assert_eq!(governor.snapshot().active_background_io_slots, 0);
    drop(memory);
    drop(task);
    drop(permit);
    drop(terms);
    drop(fixture);
    assert_eq!(governor.snapshot().admitted_memory_bytes, 0);
    assert_eq!(allocation::live(), 0);
}

#[test]
fn streamed_term_range_digests_match_independent_file_traversal() {
    for pretty in [false, true] {
        for counts in [vec![0], vec![1], vec![512], vec![0, 1, 512]] {
            let fixture = Fixture::with_counts(&counts, pretty);
            let mut budget = MutationRunBudget::new(16 << 20, 1, counts.len()).unwrap();
            let run =
                SearchMutationRun::open(&fixture.root, &fixture.manifest, 32 << 20, 5, &mut budget)
                    .unwrap();
            assert_eq!(run.entries().len(), counts.len());
            for (entry, count) in run.entries().iter().zip(counts) {
                let terms::Terms::Stored {
                    file,
                    offset,
                    bytes,
                    checksum,
                    ..
                } = &entry.retraction.unique_terms
                else {
                    panic!("stored range required")
                };
                assert_eq!(
                    *checksum,
                    terms::range_checksum(&file.file, *offset, *bytes).unwrap()
                );
                assert_eq!(entry.retraction.unique_terms.len(), count);
            }
        }
    }
}

#[test]
fn canonical_mutation_checksum_preserves_budget_refusal_type() {
    let fixture = Fixture::new(32, true);
    let governor = governor();
    let permit = governor
        .try_admit(RuntimeWorkRequest::background_maintenance(2 << 20).with_io_wave_slots(1))
        .unwrap();
    let task = permit.bind_task_context(Default::default());
    let memory = BuildMemory::new(&task).unwrap();
    warm(&memory, &task);
    let admission = ReadAdmission {
        memory: &memory,
        task: &task,
        permit: &permit,
    };
    let mut budget = MutationRunBudget::new(16 << 20, 1, 1).unwrap();
    let run = SearchMutationRun::open_with_read_admission(
        &fixture.root,
        &fixture.manifest,
        32 << 20,
        5,
        &mut budget,
        Some(&admission),
        32 << 20,
    )
    .unwrap();
    let tiny_ledger = hawdb_executor::QueryMemoryLedger::new(std::num::NonZeroUsize::MIN);
    let tiny = tiny_ledger.account(
        hawdb_executor::QueryMemoryClass::ExternalRead,
        "tiny mutation checksum",
        std::num::NonZeroUsize::MIN,
    );
    let context = QueryContext {
        memory: &tiny,
        task: &task,
        execution: Some(&permit),
    };
    let error = read_encoding::checksum(&run.body, context).unwrap_err();
    assert!(matches!(error, HawDBError::Execution(_)), "{error:?}");
    assert_eq!(governor.snapshot().active_background_io_slots, 0);
}
