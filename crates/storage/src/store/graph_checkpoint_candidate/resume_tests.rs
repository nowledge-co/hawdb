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
use hawdb_core::{
    RuntimeCancellationReason, RuntimeCancellationToken, RuntimeIoWaveController,
    RuntimeIoWaveError, RuntimeIoWavePermit,
};
use hawdb_qos::{
    IoConcurrencyBudget, RuntimeGovernor, RuntimeGovernorConfig, RuntimeMemorySnapshot,
    RuntimeResourceBudget, RuntimeResourceSnapshot, RuntimeWorkRequest,
};

struct Fixture {
    root: PathBuf,
    catalog: Catalog,
    store: GraphStore,
    pinned: GraphStore,
    candidate: CheckpointCandidate,
    identity: Option<CheckpointSourceIdentity>,
    wal: Vec<u8>,
    manifest: Vec<u8>,
    first_record_end: u64,
    private_wal_end: u64,
}

impl Fixture {
    fn new() -> Self {
        let root = std::env::temp_dir().join(format!(
            "hawdb-checkpoint-resume-{}",
            hawdb_core::generate_uuidv7().unwrap()
        ));
        let mut catalog = Catalog::default();
        let mut store = GraphStore::open(&root, &mut catalog).unwrap();
        store
            .create_node(
                &mut catalog,
                "Memory",
                BTreeMap::from([
                    ("id".into(), Value::Int(1)),
                    ("payload".into(), Value::String("\0界🙂".repeat(30_000))),
                ]),
            )
            .unwrap();
        let pinned = store.checkpoint_source();
        let candidate = pinned
            .prepare_checkpoint_candidate(&catalog)
            .unwrap()
            .unwrap();
        for id in 2..=3 {
            store
                .create_node(
                    &mut catalog,
                    "Memory",
                    BTreeMap::from([
                        ("id".into(), Value::Int(id)),
                        ("payload".into(), Value::String("\0界🙂".repeat(30_000))),
                    ]),
                )
                .unwrap();
        }
        let original = store.durable.as_ref().unwrap();
        let mut cursor = crate::wal::WalRecordCursor::open_range(
            &original.wal_path,
            original.checkpoint_tail_record_limit(),
            original.wal_generation,
            original.wal_replay_start_lsn,
            candidate.captured_wal_bytes,
            original.wal_bytes,
        )
        .unwrap();
        let mut ends = Vec::new();
        let mut bytes = WAL_BINARY_FILE_HEADER_BYTES as u64;
        let mut epoch = pinned.commit_epoch();
        while let WalCursorEvent::Entry { entry, .. } = cursor.next().unwrap() {
            epoch += 1;
            let payload = crate::wal::binary::encode_binary_wal_record(&entry, epoch).unwrap();
            bytes += crate::wal::frame::frame_binary_wal_record(
                candidate.prepared.as_ref().unwrap().generation,
                &payload,
                bytes - WAL_BINARY_FILE_HEADER_BYTES as u64,
            )
            .len() as u64;
            ends.push(bytes);
        }
        assert_eq!(ends.len(), 2);
        Self {
            identity: store.checkpoint_source_identity(),
            wal: fs::read(&original.wal_path).unwrap(),
            manifest: fs::read(original.manifest_path()).unwrap(),
            first_record_end: ends[0],
            private_wal_end: ends[1],
            root,
            catalog,
            store,
            pinned,
            candidate,
        }
    }

    fn private_wal_path(&self) -> PathBuf {
        self.candidate
            .store
            .as_ref()
            .unwrap()
            .durable
            .as_ref()
            .unwrap()
            .wal_path
            .clone()
    }

    fn unchanged_source(&self) {
        let original = self.store.durable.as_ref().unwrap();
        assert_eq!(self.store.checkpoint_source_identity(), self.identity);
        assert_eq!(fs::read(&original.wal_path).unwrap(), self.wal);
        assert_eq!(fs::read(original.manifest_path()).unwrap(), self.manifest);
        self.store.ensure_usable().unwrap();
        assert_eq!(self.pinned.scan_nodes(None).count(), 1);
    }

    fn publish_and_reopen(mut self) {
        self.unchanged_source();
        let expected = self
            .store
            .node_records_owned()
            .collect::<Result<Vec<_>>>()
            .unwrap();
        assert_eq!(
            self.candidate
                .store
                .as_ref()
                .unwrap()
                .node_records_owned()
                .collect::<Result<Vec<_>>>()
                .unwrap(),
            expected
        );
        self.store
            .publish_checkpoint_candidate(&mut self.candidate, None, &BTreeSet::new())
            .unwrap();
        drop(self.candidate);
        drop(self.pinned);
        drop(self.store);
        let reopened = GraphStore::open(&self.root, &mut self.catalog).unwrap();
        assert_eq!(
            reopened
                .node_records_owned()
                .collect::<Result<Vec<_>>>()
                .unwrap(),
            expected
        );
        drop(reopened);
        fs::remove_dir_all(self.root).unwrap();
    }
}

fn governor(bytes: u64) -> RuntimeGovernor {
    RuntimeGovernor::new(
        RuntimeGovernorConfig {
            memory_budget_bytes: Some(bytes),
            background_task_limit: Some(NonZeroUsize::MIN),
            ..RuntimeGovernorConfig::shared_host()
        },
        RuntimeResourceSnapshot::from_parts(
            RuntimeResourceBudget::from_limits(NonZeroUsize::MIN, None, None),
            RuntimeMemorySnapshot::from_limits(Some(1 << 30), Some(1 << 30), None, None, None),
        ),
        IoConcurrencyBudget::new(2, 1),
    )
}

#[derive(Debug)]
struct CutWaves {
    admitted: RuntimeTaskContext,
    private_wal_path: PathBuf,
    after: u64,
    before: u64,
    fired: AtomicBool,
}

impl CutWaves {
    fn check(&self) -> std::result::Result<(), RuntimeIoWaveError> {
        let bytes = fs::metadata(&self.private_wal_path).unwrap().len();
        if bytes > self.after && bytes <= self.before {
            self.fired.store(true, AtomicOrdering::SeqCst);
            self.admitted.cancellation().cancel();
            return Err(RuntimeIoWaveError::Stopped(
                RuntimeCancellationReason::Cancelled,
            ));
        }
        Ok(())
    }
}

impl RuntimeIoWaveController for CutWaves {
    fn acquire(
        &self,
        slots: NonZeroUsize,
        _task: &RuntimeTaskContext,
    ) -> std::result::Result<Box<dyn RuntimeIoWavePermit>, RuntimeIoWaveError> {
        self.check()?;
        Ok(self.admitted.acquire_io_wave(slots)?.unwrap())
    }

    fn try_acquire(
        &self,
        slots: NonZeroUsize,
        task: &RuntimeTaskContext,
    ) -> std::result::Result<Option<Box<dyn RuntimeIoWavePermit>>, RuntimeIoWaveError> {
        self.acquire(slots, task).map(Some)
    }
}

fn cancelled_suffix(after_first_record: bool) {
    let mut fixture = Fixture::new();
    let admitted = governor(4 * 1024 * 1024);
    let permit = admitted
        .try_admit(
            RuntimeWorkRequest::background_maintenance(4 * 1024 * 1024).with_io_wave_slots(1),
        )
        .unwrap();
    let generation = fixture.candidate.prepared.as_ref().unwrap().generation;
    let staging = fixture
        .candidate
        .prepared
        .as_ref()
        .unwrap()
        .staging_path
        .clone();
    let counter = Arc::new(CutWaves {
        admitted: permit.bind_task_context(RuntimeTaskContext::without_deadline(
            RuntimeCancellationToken::new(),
        )),
        private_wal_path: fixture.private_wal_path(),
        after: if after_first_record {
            fixture.first_record_end
        } else {
            fixture.private_wal_end - 1
        },
        before: if after_first_record {
            fixture.private_wal_end - 1
        } else {
            fixture.private_wal_end
        },
        fired: AtomicBool::new(false),
    });
    let task = counter
        .admitted
        .clone()
        .with_io_wave_controller(counter.clone());
    let error = fixture
        .candidate
        .catch_up_with_task_context(&fixture.store, &task)
        .unwrap_err();
    assert!(counter.fired.load(AtomicOrdering::SeqCst));
    assert!(error.to_string().contains("stopped"), "{error}");
    fixture.unchanged_source();
    assert!(
        !fixture.candidate.failed,
        "a safe WAL boundary must retain its candidate: {error}"
    );
    let expected_records = if after_first_record { 1 } else { 2 };
    assert_eq!(
        fixture.candidate.commit_epoch(),
        fixture.pinned.commit_epoch() + expected_records
    );
    assert_eq!(
        fixture.candidate.captured_next_lsn,
        fixture.pinned.durable.as_ref().unwrap().next_lsn + expected_records
    );
    assert!(
        fixture.candidate.finish_catch_up().is_err(),
        "an interrupted prefix cannot be selected before admitted cleanup/sync"
    );
    drop(task);
    drop(counter);
    let fresh = permit.bind_task_context(RuntimeTaskContext::default());
    let replay = fixture
        .candidate
        .catch_up_with_task_context(&fixture.store, &fresh)
        .unwrap();
    assert_eq!(replay.entries, 2);
    assert_eq!(
        fixture.candidate.prepared.as_ref().unwrap().generation,
        generation
    );
    assert_eq!(
        fixture.candidate.prepared.as_ref().unwrap().staging_path,
        staging
    );
    assert_eq!(
        fs::metadata(fixture.private_wal_path()).unwrap().len(),
        fixture.private_wal_end
    );
    fixture.publish_and_reopen();
    drop(fresh);
    drop(permit);
    let closed = admitted.snapshot();
    assert_eq!(closed.active_background_io_slots, 0);
    assert_eq!(closed.active_cpu_slots, 0);
    assert_eq!(closed.active_background_tasks, 0);
    assert_eq!(closed.admitted_memory_bytes, 0);
}

#[test]
fn partial_next_record_write_resumes_the_same_completed_prefix() {
    cancelled_suffix(true);
}

#[test]
fn denied_sync_resumes_the_same_complete_unsynchronized_prefix() {
    cancelled_suffix(false);
}

#[test]
fn pre_apply_memory_denial_keeps_the_same_checkpoint_base() {
    let mut fixture = Fixture::new();
    let generation = fixture.candidate.prepared.as_ref().unwrap().generation;
    let denied = governor(1);
    let permit = denied
        .try_admit(RuntimeWorkRequest::background_maintenance(1).with_io_wave_slots(1))
        .unwrap();
    let task = permit.bind_task_context(RuntimeTaskContext::default());
    let error = fixture
        .candidate
        .catch_up_with_task_context(&fixture.store, &task)
        .unwrap_err();
    assert!(
        error.to_string().contains("remaining reservation"),
        "{error}"
    );
    assert!(
        !fixture.candidate.failed,
        "denial before applying any transaction must not destroy the base"
    );
    fixture.unchanged_source();
    drop(task);
    drop(permit);
    assert_eq!(denied.snapshot().admitted_memory_bytes, 0);
    let allowed = governor(4 * 1024 * 1024);
    let permit = allowed
        .try_admit(
            RuntimeWorkRequest::background_maintenance(4 * 1024 * 1024).with_io_wave_slots(1),
        )
        .unwrap();
    let task = permit.bind_task_context(RuntimeTaskContext::default());
    fixture
        .candidate
        .catch_up_with_task_context(&fixture.store, &task)
        .unwrap();
    assert_eq!(
        fixture.candidate.prepared.as_ref().unwrap().generation,
        generation
    );
    fixture.publish_and_reopen();
    drop(task);
    drop(permit);
    assert_eq!(allowed.snapshot().admitted_memory_bytes, 0);
}

#[path = "resume_tests/failures.rs"]
mod failures;
