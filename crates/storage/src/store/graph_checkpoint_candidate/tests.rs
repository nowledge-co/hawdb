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
    RuntimeCancellationToken, RuntimeIoWaveController, RuntimeIoWaveError, RuntimeIoWavePermit,
    RuntimeIoWaveTryAcquire, RuntimeMemoryError,
};
use hawdb_qos::{
    IoConcurrencyBudget, RuntimeGovernor, RuntimeGovernorConfig, RuntimeMemorySnapshot,
    RuntimeResourceBudget, RuntimeResourceSnapshot, RuntimeWorkRequest,
};
use std::sync::atomic::{AtomicUsize, Ordering};

#[derive(Debug)]
struct CountingWaves {
    admitted: RuntimeTaskContext,
    calls: AtomicUsize,
    cancel_at: usize,
    cancellation: RuntimeCancellationToken,
}

impl CountingWaves {
    fn observe(&self) {
        if self.calls.fetch_add(1, Ordering::SeqCst) + 1 == self.cancel_at {
            self.cancellation.cancel();
        }
    }
}

impl RuntimeIoWaveController for CountingWaves {
    fn acquire(
        &self,
        slots: NonZeroUsize,
        _task: &RuntimeTaskContext,
    ) -> std::result::Result<Box<dyn RuntimeIoWavePermit>, RuntimeIoWaveError> {
        let permit = self
            .admitted
            .acquire_io_wave(slots)?
            .expect("real governor I/O lease");
        self.observe();
        Ok(permit)
    }

    fn try_acquire(
        &self,
        slots: NonZeroUsize,
        _task: &RuntimeTaskContext,
    ) -> std::result::Result<Option<Box<dyn RuntimeIoWavePermit>>, RuntimeIoWaveError> {
        match self.admitted.try_acquire_io_wave(slots)? {
            RuntimeIoWaveTryAcquire::Acquired(Some(permit)) => {
                self.observe();
                Ok(Some(permit))
            }
            RuntimeIoWaveTryAcquire::Pending => Ok(None),
            RuntimeIoWaveTryAcquire::Acquired(None) => {
                panic!("the counting adapter must delegate to a real governor I/O lease")
            }
        }
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

#[test]
fn checkpoint_units_wal_framing_actual_suffix_denies_memory_and_fully_retries() {
    let directory = std::env::temp_dir().join(format!(
        "hawdb-checkpoint-wal-framing-{}",
        hawdb_core::generate_uuidv7().unwrap()
    ));
    let mut catalog = Catalog::default();
    let mut store = GraphStore::open(&directory, &mut catalog).unwrap();
    store
        .create_node(
            &mut catalog,
            "Memory",
            BTreeMap::from([("id".into(), Value::Int(1))]),
        )
        .unwrap();
    let source = store.checkpoint_source();
    let source_catalog = catalog.clone();
    let source_identity = source.checkpoint_source_identity();
    let mut candidate = source
        .prepare_checkpoint_candidate(&source_catalog)
        .unwrap()
        .unwrap();
    store
        .create_node(
            &mut catalog,
            "Memory",
            BTreeMap::from([
                ("id".into(), Value::Int(2)),
                ("payload".into(), Value::String("\0界🙂".repeat(30_000))),
            ]),
        )
        .unwrap();
    let identity = store.checkpoint_source_identity();
    let durable = store.durable.as_ref().unwrap();
    let wal_path = durable.wal_path.clone();
    let manifest_path = durable.manifest_path().to_path_buf();
    let wal = std::fs::read(&wal_path).unwrap();
    let manifest = std::fs::read(&manifest_path).unwrap();
    let expected = store
        .node_records_owned()
        .collect::<crate::Result<Vec<_>>>()
        .unwrap();

    let denied = governor(1);
    let permit = denied
        .try_admit(RuntimeWorkRequest::background_maintenance(1).with_io_wave_slots(1))
        .unwrap();
    let task = permit.bind_task_context(RuntimeTaskContext::default());
    let result = candidate.catch_up_with_task_context(&store, &task);
    assert!(
        result.is_err(),
        "the framed suffix must be admitted before allocation"
    );
    assert!(result
        .unwrap_err()
        .to_string()
        .contains("remaining reservation"));
    assert_eq!(store.checkpoint_source_identity(), identity);
    assert_eq!(std::fs::read(&wal_path).unwrap(), wal);
    assert_eq!(std::fs::read(&manifest_path).unwrap(), manifest);
    store.ensure_usable().unwrap();
    drop(candidate);
    drop(task);
    drop(permit);
    let closed = denied.snapshot();
    assert_eq!(closed.active_background_io_slots, 0);
    assert_eq!(closed.active_cpu_slots, 0);
    assert_eq!(closed.active_background_tasks, 0);
    assert_eq!(closed.admitted_memory_bytes, 0);

    // Recreate the same pinned base and replay the full suffix. Ordinary
    // publication and reopen are the complete-result references.
    let mut candidate = source
        .prepare_checkpoint_candidate(&source_catalog)
        .unwrap()
        .unwrap();
    let allowed = governor(2 * 1024 * 1024);
    let permit = allowed
        .try_admit(
            RuntimeWorkRequest::background_maintenance(2 * 1024 * 1024).with_io_wave_slots(1),
        )
        .unwrap();
    let task = permit.bind_task_context(RuntimeTaskContext::default());
    candidate.catch_up_with_task_context(&store, &task).unwrap();
    store
        .publish_checkpoint_candidate(&mut candidate, None, &Default::default())
        .unwrap();
    drop(candidate);
    drop(task);
    drop(permit);
    assert_eq!(allowed.snapshot().admitted_memory_bytes, 0);
    assert_eq!(source.checkpoint_source_identity(), source_identity);
    drop(source);
    drop(store);
    let recovered = GraphStore::open(&directory, &mut catalog).unwrap();
    assert_eq!(
        recovered
            .node_records_owned()
            .collect::<crate::Result<Vec<_>>>()
            .unwrap(),
        expected
    );
    drop(recovered);
    std::fs::remove_dir_all(directory).unwrap();
}

#[test]
fn checkpoint_units_wal_framing_actual_suffix_cancels_every_io_wave_and_retries_same_admission() {
    let directory = std::env::temp_dir().join(format!(
        "hawdb-checkpoint-wal-io-cuts-{}",
        hawdb_core::generate_uuidv7().unwrap()
    ));
    let mut catalog = Catalog::default();
    let mut store = GraphStore::open(&directory, &mut catalog).unwrap();
    store
        .create_node(
            &mut catalog,
            "Memory",
            BTreeMap::from([("id".into(), Value::Int(1))]),
        )
        .unwrap();
    let source = store.checkpoint_source();
    let source_catalog = catalog.clone();
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
    let identity = store.checkpoint_source_identity();
    let durable = store.durable.as_ref().unwrap();
    let wal_path = durable.wal_path.clone();
    let manifest_path = durable.manifest_path().to_path_buf();
    let wal = std::fs::read(&wal_path).unwrap();
    let manifest = std::fs::read(&manifest_path).unwrap();
    let expected = store
        .node_records_owned()
        .collect::<crate::Result<Vec<_>>>()
        .unwrap();
    let ceiling = 2 * 1024 * 1024;
    let baseline_governor = governor(ceiling);
    let baseline_permit = baseline_governor
        .try_admit(RuntimeWorkRequest::background_maintenance(ceiling).with_io_wave_slots(1))
        .unwrap();
    let counter = Arc::new(CountingWaves {
        admitted: baseline_permit.bind_task_context(RuntimeTaskContext::default()),
        calls: AtomicUsize::new(0),
        cancel_at: usize::MAX,
        cancellation: RuntimeCancellationToken::new(),
    });
    let task = counter
        .admitted
        .clone()
        .with_io_wave_controller(counter.clone());
    let mut candidate = source
        .prepare_checkpoint_candidate(&source_catalog)
        .unwrap()
        .unwrap();
    candidate.catch_up_with_task_context(&store, &task).unwrap();
    let waves = counter.calls.load(Ordering::SeqCst);
    assert!(
        waves > 10,
        "large framed records must release I/O between bounded writes"
    );
    drop(candidate);
    drop(task);
    drop(counter);
    drop(baseline_permit);
    assert_eq!(baseline_governor.snapshot().admitted_memory_bytes, 0);

    for stop in 1..=waves {
        let governor = governor(ceiling);
        let permit = governor
            .try_admit(RuntimeWorkRequest::background_maintenance(ceiling).with_io_wave_slots(1))
            .unwrap();
        let cancellation = RuntimeCancellationToken::new();
        let counter = Arc::new(CountingWaves {
            admitted: permit
                .bind_task_context(RuntimeTaskContext::without_deadline(cancellation.clone())),
            calls: AtomicUsize::new(0),
            cancel_at: stop,
            cancellation,
        });
        let task = counter
            .admitted
            .clone()
            .with_io_wave_controller(counter.clone());
        let mut candidate = source
            .prepare_checkpoint_candidate(&source_catalog)
            .unwrap()
            .unwrap();
        let error = candidate
            .catch_up_with_task_context(&store, &task)
            .unwrap_err();
        assert!(error.to_string().contains("stopped"), "{error:?}");
        assert_eq!(counter.calls.load(Ordering::SeqCst), stop);
        assert_eq!(governor.snapshot().active_background_io_slots, 0);
        assert_eq!(store.checkpoint_source_identity(), identity);
        assert_eq!(std::fs::read(&wal_path).unwrap(), wal);
        assert_eq!(std::fs::read(&manifest_path).unwrap(), manifest);
        store.ensure_usable().unwrap();
        drop(candidate);
        drop(task);
        drop(counter);

        let fresh = permit.bind_task_context(RuntimeTaskContext::default());
        assert!(
            matches!(fresh.reserve_working_memory(ceiling), Err(RuntimeMemoryError::ReservationExceeded { available_bytes, .. }) if available_bytes == ceiling)
        );
        let mut candidate = source
            .prepare_checkpoint_candidate(&source_catalog)
            .unwrap()
            .unwrap();
        let replay = candidate
            .catch_up_with_task_context(&store, &fresh)
            .unwrap();
        assert_eq!(replay.entries, 2);
        assert_eq!(replay.captured_commit_epoch, store.commit_epoch());
        assert_eq!(
            candidate
                .store
                .as_ref()
                .unwrap()
                .node_records_owned()
                .collect::<crate::Result<Vec<_>>>()
                .unwrap(),
            expected
        );
        assert!(
            matches!(fresh.reserve_working_memory(ceiling), Err(RuntimeMemoryError::ReservationExceeded { available_bytes, .. }) if available_bytes == ceiling)
        );
        assert_eq!(governor.snapshot().admissions, 1);
        if stop == waves {
            store
                .publish_checkpoint_candidate(&mut candidate, None, &Default::default())
                .unwrap();
        }
        drop(candidate);
        drop(fresh);
        drop(permit);
        let closed = governor.snapshot();
        assert_eq!(closed.active_background_io_slots, 0);
        assert_eq!(closed.active_background_tasks, 0);
        assert_eq!(closed.admitted_memory_bytes, 0);
    }
    drop(source);
    drop(store);
    let recovered = GraphStore::open(&directory, &mut catalog).unwrap();
    assert_eq!(
        recovered
            .node_records_owned()
            .collect::<crate::Result<Vec<_>>>()
            .unwrap(),
        expected
    );
    assert_eq!(recovered.commit_epoch(), 3);
    drop(recovered);
    std::fs::remove_dir_all(directory).unwrap();
}

#[test]
fn checkpoint_units_wal_payload_actual_suffix_denies_unaccounted_overlap_and_fully_retries() {
    let directory = std::env::temp_dir().join(format!(
        "hawdb-checkpoint-wal-payload-{}",
        hawdb_core::generate_uuidv7().unwrap()
    ));
    let mut catalog = Catalog::default();
    let mut store = GraphStore::open(&directory, &mut catalog).unwrap();
    store
        .create_node(
            &mut catalog,
            "Memory",
            BTreeMap::from([("id".into(), Value::Int(1))]),
        )
        .unwrap();
    let source = store.checkpoint_source();
    let source_catalog = catalog.clone();
    let source_identity = source.checkpoint_source_identity();
    let mut candidate = source
        .prepare_checkpoint_candidate(&source_catalog)
        .unwrap()
        .unwrap();
    store
        .create_node(
            &mut catalog,
            "Memory",
            BTreeMap::from([
                ("id".into(), Value::Int(2)),
                ("payload".into(), Value::String("\0界🙂".repeat(30_000))),
            ]),
        )
        .unwrap();
    let identity = store.checkpoint_source_identity();
    let durable = store.durable.as_ref().unwrap();
    let wal_path = durable.wal_path.clone();
    let manifest_path = durable.manifest_path().to_path_buf();
    let wal = std::fs::read(&wal_path).unwrap();
    let manifest = std::fs::read(&manifest_path).unwrap();
    let expected = store
        .node_records_owned()
        .collect::<crate::Result<Vec<_>>>()
        .unwrap();

    let mut cursor = match WalRecordCursor::open(&wal_path, None).unwrap() {
        WalOpenOutcome::Cursor(cursor) => cursor,
        _ => panic!("the complete source WAL must have a valid header"),
    };
    let mut last = None;
    loop {
        match cursor.next().unwrap() {
            WalCursorEvent::Entry { entry, .. } => last = Some(entry),
            WalCursorEvent::Eof => break,
            _ => panic!("complete source must contain only committed records"),
        }
    }
    drop(cursor);
    let payload = encode_binary_wal_record(&last.unwrap(), store.commit_epoch()).unwrap();
    let ordinary = frame_binary_wal_record(
        candidate.prepared.as_ref().unwrap().generation,
        &payload,
        candidate.candidate_wal_bytes - WAL_BINARY_FILE_HEADER_BYTES as u64,
    );
    // The complete framed output fits, but its simultaneously live encoded
    // payload also needs admission. The old encoder leaves that copy uncharged.
    let ceiling = ordinary.len() as u64 + 24;
    let denied = governor(ceiling);
    let permit = denied
        .try_admit(RuntimeWorkRequest::background_maintenance(ceiling).with_io_wave_slots(1))
        .unwrap();
    let task = permit.bind_task_context(RuntimeTaskContext::default());
    let result = candidate.catch_up_with_task_context(&store, &task);
    assert!(
        result.is_err(),
        "the encoded payload and framed suffix must both be admitted before allocation"
    );
    assert!(result
        .unwrap_err()
        .to_string()
        .contains("remaining reservation"));
    assert_eq!(store.checkpoint_source_identity(), identity);
    assert_eq!(std::fs::read(&wal_path).unwrap(), wal);
    assert_eq!(std::fs::read(&manifest_path).unwrap(), manifest);
    store.ensure_usable().unwrap();
    drop(candidate);
    drop(task);
    drop(permit);
    let closed = denied.snapshot();
    assert_eq!(closed.active_background_io_slots, 0);
    assert_eq!(closed.active_cpu_slots, 0);
    assert_eq!(closed.active_background_tasks, 0);
    assert_eq!(closed.admitted_memory_bytes, 0);

    // Recreate the same pinned base and replay the full suffix. Ordinary
    // publication and reopen are the complete-result references.
    let mut candidate = source
        .prepare_checkpoint_candidate(&source_catalog)
        .unwrap()
        .unwrap();
    let allowed = governor(2 * 1024 * 1024);
    let permit = allowed
        .try_admit(
            RuntimeWorkRequest::background_maintenance(2 * 1024 * 1024).with_io_wave_slots(1),
        )
        .unwrap();
    let task = permit.bind_task_context(RuntimeTaskContext::default());
    candidate.catch_up_with_task_context(&store, &task).unwrap();
    store
        .publish_checkpoint_candidate(&mut candidate, None, &Default::default())
        .unwrap();
    drop(candidate);
    drop(task);
    drop(permit);
    assert_eq!(allowed.snapshot().admitted_memory_bytes, 0);
    assert_eq!(source.checkpoint_source_identity(), source_identity);
    drop(source);
    drop(store);
    let recovered = GraphStore::open(&directory, &mut catalog).unwrap();
    assert_eq!(
        recovered
            .node_records_owned()
            .collect::<crate::Result<Vec<_>>>()
            .unwrap(),
        expected
    );
    drop(recovered);
    std::fs::remove_dir_all(directory).unwrap();
}

#[derive(Debug)]
struct CursorMemoryWaves {
    admitted: RuntimeTaskContext,
    ceiling: u64,
    available_at_acquire: std::sync::Mutex<Vec<u64>>,
}

impl CursorMemoryWaves {
    fn observe(&self) {
        let available = match self.admitted.reserve_working_memory(self.ceiling) {
            Err(RuntimeMemoryError::ReservationExceeded {
                available_bytes, ..
            }) => available_bytes,
            _ => panic!("an audited full-budget request must expose remaining real reservation"),
        };
        self.available_at_acquire.lock().unwrap().push(available);
    }
}

impl RuntimeIoWaveController for CursorMemoryWaves {
    fn acquire(
        &self,
        slots: NonZeroUsize,
        _task: &RuntimeTaskContext,
    ) -> std::result::Result<Box<dyn RuntimeIoWavePermit>, RuntimeIoWaveError> {
        let permit = self
            .admitted
            .acquire_io_wave(slots)?
            .expect("real governor I/O lease");
        self.observe();
        Ok(permit)
    }

    fn try_acquire(
        &self,
        slots: NonZeroUsize,
        _task: &RuntimeTaskContext,
    ) -> std::result::Result<Option<Box<dyn RuntimeIoWavePermit>>, RuntimeIoWaveError> {
        match self.admitted.try_acquire_io_wave(slots)? {
            RuntimeIoWaveTryAcquire::Acquired(Some(permit)) => {
                self.observe();
                Ok(Some(permit))
            }
            RuntimeIoWaveTryAcquire::Pending => Ok(None),
            RuntimeIoWaveTryAcquire::Acquired(None) => {
                panic!("the memory audit must delegate to a real governor I/O lease")
            }
        }
    }
}

#[test]
fn checkpoint_units_wal_cursor_actual_suffix_admits_read_buffer_before_payload_work() {
    let directory = std::env::temp_dir().join(format!(
        "hawdb-checkpoint-wal-cursor-memory-{}",
        hawdb_core::generate_uuidv7().unwrap()
    ));
    let mut catalog = Catalog::default();
    let mut store = GraphStore::open(&directory, &mut catalog).unwrap();
    store
        .create_node(
            &mut catalog,
            "Memory",
            BTreeMap::from([("id".into(), Value::Int(1))]),
        )
        .unwrap();
    let source = store.checkpoint_source();
    let source_catalog = catalog.clone();
    let source_identity = source.checkpoint_source_identity();
    let mut candidate = source
        .prepare_checkpoint_candidate(&source_catalog)
        .unwrap()
        .unwrap();
    store
        .create_node(
            &mut catalog,
            "Memory",
            BTreeMap::from([
                ("id".into(), Value::Int(2)),
                ("payload".into(), Value::String("\0界🙂".repeat(30_000))),
            ]),
        )
        .unwrap();
    let identity = store.checkpoint_source_identity();
    let durable = store.durable.as_ref().unwrap();
    let wal_path = durable.wal_path.clone();
    let manifest_path = durable.manifest_path().to_path_buf();
    let wal = std::fs::read(&wal_path).unwrap();
    let manifest = std::fs::read(&manifest_path).unwrap();
    let expected = store
        .node_records_owned()
        .collect::<crate::Result<Vec<_>>>()
        .unwrap();
    let ceiling = 2 * 1024 * 1024;
    let governor = governor(ceiling);
    let permit = governor
        .try_admit(RuntimeWorkRequest::background_maintenance(ceiling).with_io_wave_slots(1))
        .unwrap();
    let audit = Arc::new(CursorMemoryWaves {
        admitted: permit.bind_task_context(RuntimeTaskContext::default()),
        ceiling,
        available_at_acquire: std::sync::Mutex::new(Vec::new()),
    });
    let task = audit
        .admitted
        .clone()
        .with_io_wave_controller(audit.clone());
    let replay = candidate.catch_up_with_task_context(&store, &task).unwrap();
    let observed = audit.available_at_acquire.lock().unwrap().clone();
    assert!(
        observed.len() >= 3,
        "the real candidate must open output/source and read a captured record"
    );
    // The third wave is at the captured-record read, before encoding/framing
    // can charge their output. A governor-bound cursor must already own its
    // physical 32 KiB read buffer. Later output reservations cannot mask it.
    assert!(
        observed[2] <= ceiling - crate::wal::frame::WAL_BLOCK_BYTES as u64,
        "the captured WAL read buffer is unaccounted before payload work: {observed:?}"
    );
    assert_eq!(replay.entries, 1);
    assert_eq!(replay.captured_commit_epoch, store.commit_epoch());
    assert_eq!(store.checkpoint_source_identity(), identity);
    assert_eq!(std::fs::read(&wal_path).unwrap(), wal);
    assert_eq!(std::fs::read(&manifest_path).unwrap(), manifest);
    assert_eq!(
        candidate
            .store
            .as_ref()
            .unwrap()
            .node_records_owned()
            .collect::<crate::Result<Vec<_>>>()
            .unwrap(),
        expected
    );
    assert!(
        matches!(task.reserve_working_memory(ceiling), Err(RuntimeMemoryError::ReservationExceeded { available_bytes, .. }) if available_bytes == ceiling)
    );
    store
        .publish_checkpoint_candidate(&mut candidate, None, &Default::default())
        .unwrap();
    drop(candidate);
    drop(task);
    drop(audit);
    drop(permit);
    let closed = governor.snapshot();
    assert_eq!(closed.active_background_io_slots, 0);
    assert_eq!(closed.active_cpu_slots, 0);
    assert_eq!(closed.active_background_tasks, 0);
    assert_eq!(closed.admitted_memory_bytes, 0);
    assert_eq!(source.checkpoint_source_identity(), source_identity);
    drop(source);
    drop(store);
    let recovered = GraphStore::open(&directory, &mut catalog).unwrap();
    assert_eq!(
        recovered
            .node_records_owned()
            .collect::<crate::Result<Vec<_>>>()
            .unwrap(),
        expected
    );
    drop(recovered);
    std::fs::remove_dir_all(directory).unwrap();
}
