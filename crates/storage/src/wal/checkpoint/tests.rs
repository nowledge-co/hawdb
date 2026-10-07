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
use crate::background::CheckpointWorkProbe;
use hawdb_qos::{
    IoConcurrencyBudget, LocalQosPolicy, LocalQosScheduler, RuntimeGovernor, RuntimeGovernorConfig,
    RuntimeMemorySnapshot, RuntimeResourceBudget, RuntimeResourceSnapshot, RuntimeTaskContext,
    RuntimeWorkRequest,
};
use std::num::NonZeroUsize;
use std::sync::{atomic::Ordering, Arc};

#[test]
fn checkpoint_units_wal_decode_actual_cursor_splits_a_batch_beyond_default_operation_limit() {
    let directory = std::env::temp_dir().join(format!(
        "hawdb-checkpoint-wal-decode-{}",
        hawdb_core::generate_uuidv7().unwrap()
    ));
    std::fs::create_dir(&directory).unwrap();
    let path = directory.join("captured.wal");
    let operation_count = 1057;
    let entry = WalEntry {
        lsn: 17,
        op: WalOp::Batch(
            (0..operation_count)
                .map(|id| WalOp::DeleteNode {
                    id: crate::NodeId(id as u64),
                })
                .collect(),
        ),
    };
    let payload = binary::encode_binary_wal_record(&entry, 19).unwrap();
    let mut complete_file = frame::encode_binary_wal_header(23, entry.lsn);
    complete_file.extend(frame::frame_binary_wal_record(23, &payload, 0));
    std::fs::write(&path, &complete_file).unwrap();
    let ceiling = 2 * 1024 * 1024;
    let governor = RuntimeGovernor::new(
        RuntimeGovernorConfig {
            memory_budget_bytes: Some(ceiling),
            background_task_limit: Some(NonZeroUsize::MIN),
            ..RuntimeGovernorConfig::shared_host()
        },
        RuntimeResourceSnapshot::from_parts(
            RuntimeResourceBudget::from_limits(NonZeroUsize::MIN, None, None),
            RuntimeMemorySnapshot::from_limits(Some(1 << 30), Some(1 << 30), None, None, None),
        ),
        IoConcurrencyBudget::new(2, 1),
    );
    let permit = governor
        .try_admit(RuntimeWorkRequest::background_maintenance(ceiling).with_io_wave_slots(1))
        .unwrap();
    let scheduler = LocalQosScheduler::new(LocalQosPolicy {
        max_background_operations: Some(1),
        max_total_background_operations: Some(1),
        ..Default::default()
    });
    let probe = Arc::new(CheckpointWorkProbe::default());
    scheduler.set_telemetry_sink(Some(probe.clone()));
    let task = permit.bind_task_context(RuntimeTaskContext::without_deadline(
        probe.cancellation.clone(),
    ));
    let work = CheckpointWorkContext::new(task).with_scheduler(scheduler.clone());
    let mut cursor = CheckpointWalRecordCursor::open_range(
        &path,
        Some(128 * 1024),
        23,
        entry.lsn,
        frame::WAL_BINARY_FILE_HEADER_BYTES as u64,
        complete_file.len() as u64,
        &work,
    )
    .unwrap();
    let recovered = match cursor.next().unwrap() {
        WalCursorEvent::Entry {
            entry,
            payload_len,
            payload_sha256,
            ..
        } => {
            assert_eq!(payload_len, payload.len() as u64);
            assert_eq!(payload_sha256, hawdb_integrity::sha256(&payload));
            entry
        }
        _ => panic!("the complete captured transaction must be decoded"),
    };
    assert_eq!(
        binary::encode_binary_wal_record(&recovered, 19).unwrap(),
        payload
    );
    assert!(matches!(cursor.next().unwrap(), WalCursorEvent::Eof));
    assert_eq!(std::fs::read(&path).unwrap(), complete_file);
    assert_eq!(probe.peak_units.load(Ordering::SeqCst), 1);
    assert!(
        probe.completed.load(Ordering::SeqCst) >= operation_count,
        "a 1057-operation transaction must not decode inside one dataset-sized unit; observed {} completed units",
        probe.completed.load(Ordering::SeqCst),
    );
    drop(recovered);
    drop(cursor);
    drop(work);
    probe.assert_released(&scheduler);
    drop(permit);
    assert_eq!(governor.snapshot().admitted_memory_bytes, 0);
    assert_eq!(governor.snapshot().active_background_io_slots, 0);
    std::fs::remove_dir_all(directory).unwrap();
}
