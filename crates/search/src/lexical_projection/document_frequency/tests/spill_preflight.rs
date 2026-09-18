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
use crate::build_control::observation;
use hawdb_core::RuntimeMemoryReservation;

fn pool(root: &TestRoot) -> (BuildMemory, SpillRuns) {
    let task = RuntimeTaskContext::with_timeout(std::time::Duration::from_secs(60))
        .with_memory_reservation(RuntimeMemoryReservation::new(256 * 1024, 0));
    let memory = BuildMemory::new(&task).unwrap();
    let mut pool =
        SpillRuns::with_context(&root.0, 1, Default::default(), memory.clone(), task).unwrap();
    pool.prepare(5, 6).unwrap();
    (memory, pool)
}

fn input(memory: &BuildMemory, pool: &mut SpillRuns) -> FrequencyRun {
    let records = [("alpha", 0), ("alpha", 0), ("alpha", 1), ("beta", 1)].map(|(text, field)| {
        let mut record = record(text, field, 1, TokenOccurrence::Repeated, 1);
        record.term = Term::copy(text, Some(memory)).unwrap();
        Ok(record)
    });
    write_run(records, pool, &mut FileSpillIo).unwrap()
}

#[test]
fn streamed_posting_size_is_exact_after_duplicate_fields_and_pair_merge() {
    for merge in [false, true] {
        for short in [0, 1] {
            let root = TestRoot::new();
            let (memory, mut pool) = pool(&root);
            let mut run = input(&memory, &mut pool);
            if merge {
                let other = input(&memory, &mut pool);
                let (merged, checks) = observation::measure(|| merge_pair(run, other, &mut pool));
                run = merged.unwrap();
                assert!(
                    checks <= 4 * 6 + 20,
                    "frequency records checkpointed per field: {checks}"
                );
            }
            let id = "source";
            let expected = RUN_HEADER.len() as u64
                + Posting::encoded_parts_len(5, id.len())
                + Posting::encoded_parts_len(4, id.len());
            assert_eq!(run.posting_size.encoded_bytes(id).unwrap(), expected);
            let bytes = pool.bytes;
            let sequence = pool.sequence;
            pool.config.max_spill_bytes = NonZeroU64::new(bytes + expected - short).unwrap();
            pool.config.max_spill_runs = NonZeroUsize::new(sequence + 1).unwrap();
            let result = spill_postings(run, id, if merge { 8 } else { 4 }, &mut pool);
            if short == 0 {
                result.unwrap();
                assert_eq!(pool.sequence, sequence + 1);
                assert_eq!(pool.bytes, bytes + expected);
                assert_eq!(fs::metadata(&pool.paths[0].path).unwrap().len(), expected);
                let mut records = Vec::new();
                visit_merged_postings_with_control(
                    &pool.paths,
                    pool.config,
                    &pool.control,
                    |posting| {
                        records.push((posting.term.to_string(), posting.term_frequency));
                        Ok(())
                    },
                )
                .unwrap();
                assert_eq!(
                    records,
                    vec![
                        ("alpha".into(), if merge { 6 } else { 3 }),
                        ("beta".into(), if merge { 2 } else { 1 }),
                    ]
                );
            } else {
                assert!(result.unwrap_err().to_string().contains("spill bytes"));
                assert_eq!((pool.bytes, pool.sequence), (bytes, sequence));
                assert!(pool.paths.is_empty());
                assert_eq!(root.entries(), 0);
                // The last sequence remains available because no output was attempted.
                let next = pool.next_guard().unwrap();
                assert_eq!(pool.sequence, sequence + 1);
                drop(next);
            }
            drop(pool);
            assert_eq!(root.entries(), 0);
            assert_eq!(memory.ledger.snapshot().used_bytes, 0);
        }
    }
}

#[test]
fn failed_creation_does_not_reuse_a_path_when_cleanup_also_fails() {
    let root = TestRoot::new();
    let (memory, mut pool) = pool(&root);
    let run = input(&memory, &mut pool);
    let blocked = root.0.join(".search-lexical.1.1.tmp");
    fs::create_dir(&blocked).unwrap();
    let bytes = pool.bytes;
    assert!(spill_postings(run, "source", 4, &mut pool).is_err());
    assert_eq!(pool.bytes, bytes);
    assert_eq!(pool.sequence, 2);
    assert!(pool.paths.is_empty());
    assert!(blocked.is_dir());
    let next = pool.next_guard().unwrap();
    assert_ne!(next.path, blocked);
    assert_eq!(pool.sequence, 3);
    drop((next, pool));
    assert_eq!(memory.ledger.snapshot().used_bytes, 0);
}
