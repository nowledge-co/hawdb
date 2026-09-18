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
use crate::lexical_projection::{LexicalProjectionConfig, LexicalProjectionWriter};
use crate::{SearchDocument, SearchIndex};
use hawdb_core::RuntimeMemoryReservation;
use hawdb_executor::QueryMemoryLedger;
use std::cell::RefCell;
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

#[derive(Default)]
struct Observation {
    exits: Vec<(std::thread::ThreadId, usize, QueryMemoryLedger)>,
}

thread_local! {
    static OBSERVING: RefCell<Option<Arc<Mutex<Observation>>>> = const { RefCell::new(None) };
    static WORKER_EXIT: RefCell<Option<WorkerExit>> = const { RefCell::new(None) };
}

pub(super) struct WorkerExit {
    observation: Arc<Mutex<Observation>>,
    ledger: QueryMemoryLedger,
}

impl Drop for WorkerExit {
    fn drop(&mut self) {
        self.observation.lock().unwrap().exits.push((
            std::thread::current().id(),
            self.ledger.snapshot().used_bytes,
            self.ledger.clone(),
        ));
    }
}

pub(super) fn capture(memory: &BuildMemory) -> Option<WorkerExit> {
    OBSERVING.with(|current| {
        current.borrow().as_ref().map(|observation| WorkerExit {
            observation: Arc::clone(observation),
            ledger: memory.ledger.clone(),
        })
    })
}

pub(super) fn install(observation: Option<WorkerExit>) {
    WORKER_EXIT.with(|slot| *slot.borrow_mut() = observation);
}

struct Observe(Arc<Mutex<Observation>>);

impl Observe {
    fn new() -> Self {
        let observation = Arc::new(Mutex::new(Observation::default()));
        OBSERVING.with(|current| {
            assert!(current.replace(Some(Arc::clone(&observation))).is_none());
        });
        Self(observation)
    }

    fn assert_joined(&self, workers: usize) {
        let observation = self.0.lock().unwrap();
        assert_eq!(observation.exits.len(), workers);
        for (worker, bytes, _) in &observation.exits {
            assert_ne!(*worker, std::thread::current().id());
            assert!(
                *bytes >= STACK_BYTES,
                "thread lease released before TLS exit"
            );
        }
    }

    fn assert_released(&self) {
        for (_, _, ledger) in &self.0.lock().unwrap().exits {
            assert_eq!(ledger.snapshot().used_bytes, 0);
        }
    }
}

impl Drop for Observe {
    fn drop(&mut self) {
        OBSERVING.with(|current| current.replace(None));
    }
}

struct Directory(PathBuf);

impl Directory {
    fn new() -> Self {
        static NEXT: AtomicU64 = AtomicU64::new(0);
        let path = std::env::temp_dir().join(format!(
            "hawdb-analyzer-entrypoints-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        std::fs::create_dir(&path).unwrap();
        Self(path)
    }
}

impl Drop for Directory {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

fn document(id: &str, text: &str) -> SearchDocument {
    SearchDocument {
        id: id.into(),
        title: String::new(),
        content: text.into(),
        metadata: BTreeMap::new(),
        embedding: None,
    }
}

fn write(
    path: &Path,
    document: &SearchDocument,
    memory: &BuildMemory,
    task: &RuntimeTaskContext,
) -> Result<()> {
    LexicalProjectionWriter::new(LexicalProjectionConfig::default())
        .with_context(memory.clone(), task.clone())
        .write(
            path,
            1,
            None,
            0,
            0,
            std::iter::once(document),
            &Default::default(),
        )
        .map(drop)
}

#[test]
fn checkpoint_and_delta_entrypoints_join_before_return_and_keep_snapshots() {
    let directory = Directory::new();
    let mut index = SearchIndex::open(&directory.0).unwrap();
    let text = "\u{9f98}\u{9750}\u{9f49} GraphStorage";
    index.upsert(document("a", text)).unwrap();
    index.upsert(document("b", text)).unwrap();
    let checkpoint = Observe::new();
    index.checkpoint().unwrap();
    // Publication reuses the direct rebuild's files without analyzing again.
    checkpoint.assert_joined(1);
    drop(checkpoint);

    let snapshot = index.lexical_delta.lock().unwrap().clone();
    let delta = Observe::new();
    index
        .upsert(document("a", "replacement \u{20000}\u{20001}"))
        .unwrap();
    delta.assert_joined(2);
    index.delete("b");
    delta.assert_joined(3);
    delta.assert_released();
    assert!(!Arc::ptr_eq(
        &snapshot,
        &index.lexical_delta.lock().unwrap()
    ));
    assert!(index.lexical_projection.lock().unwrap().is_some());
    drop(delta);

    let checkpoint = Observe::new();
    index.checkpoint_with_report().unwrap();
    checkpoint.assert_joined(1);
    drop(index);
    checkpoint.assert_released();
    let reopened = SearchIndex::open(&directory.0).unwrap();
    assert_eq!(reopened.documents.len(), 1);
    assert_eq!(
        reopened.documents["a"].content,
        "replacement \u{20000}\u{20001}"
    );
}

#[test]
fn checkpoint_analyzer_denial_and_cancellation_leave_no_artifacts_or_leases() {
    for cancelled in [false, true] {
        let directory = Directory::new();
        let task = RuntimeTaskContext::default()
            .with_memory_reservation(RuntimeMemoryReservation::new(4 * 1024 * 1024, 0));
        let memory = BuildMemory::new(&task).unwrap();
        if cancelled {
            task.cancellation().cancel();
        }
        let observation = Observe::new();
        let error = write(&directory.0, &document("a", WARMUP), &memory, &task).unwrap_err();
        assert!(error.to_string().contains(if cancelled {
            "cancel"
        } else {
            "query_memory_bytes"
        }));
        observation.assert_joined(if cancelled { 0 } else { 1 });
        observation.assert_released();
        assert_eq!(memory.ledger.snapshot().used_bytes, 0);
        assert_eq!(std::fs::read_dir(&directory.0).unwrap().count(), 0);
    }
}

#[test]
fn failed_delta_analysis_joins_then_preserves_the_existing_mutation_fallback() {
    for deleting in [false, true] {
        let directory = Directory::new();
        let mut index = SearchIndex::open(&directory.0).unwrap();
        index.upsert(document("a", WARMUP)).unwrap();
        index.checkpoint().unwrap();
        let old_delta = index.lexical_delta.lock().unwrap().clone();
        index.lexical_config.max_term_bytes = std::num::NonZeroU64::MIN;
        let observation = Observe::new();
        if deleting {
            index.delete("a");
            assert!(index.documents.is_empty());
        } else {
            index.upsert(document("a", WARMUP)).unwrap();
            assert_eq!(index.documents["a"].content, WARMUP);
        }
        observation.assert_joined(1);
        observation.assert_released();
        assert!(index.lexical_projection.lock().unwrap().is_none());
        assert!(!Arc::ptr_eq(
            &old_delta,
            &index.lexical_delta.lock().unwrap()
        ));
    }
}

#[test]
fn non_han_checkpoint_and_delta_keep_the_inline_path() {
    let directory = Directory::new();
    let mut index = SearchIndex::open(&directory.0).unwrap();
    let observation = Observe::new();
    index
        .upsert(document("a", "GraphStorage HTTPServer"))
        .unwrap();
    index.checkpoint().unwrap();
    index.upsert(document("a", "replacement")).unwrap();
    index.delete("a");
    observation.assert_joined(0);
}
