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
use crate::wal::frame::{frame_binary_wal_record, BinaryWalReadEvent, BinaryWalReader};
use hawdb_qos::{LocalQosPolicy, LocalQosScheduler};
use std::io::Cursor;

#[derive(Debug, PartialEq, Eq)]
enum ReferenceEvent {
    Record(Vec<u8>, u64, u64),
    Torn(u64, String),
    Corrupt(u64, String),
    Eof,
    Error(String),
}

fn ordinary(bytes: &[u8], generation: u64, limit: Option<usize>) -> Vec<ReferenceEvent> {
    let mut reader = BinaryWalReader::new(Cursor::new(bytes), generation, limit);
    let mut out = Vec::new();
    loop {
        let event = match reader.next_event() {
            Ok(BinaryWalReadEvent::Record {
                payload,
                start_offset,
                end_offset,
            }) => ReferenceEvent::Record(payload, start_offset, end_offset),
            Ok(BinaryWalReadEvent::TornTail {
                valid_prefix_len,
                reason,
            }) => ReferenceEvent::Torn(valid_prefix_len, reason),
            Ok(BinaryWalReadEvent::Corrupt { offset, reason }) => {
                ReferenceEvent::Corrupt(offset, reason)
            }
            Ok(BinaryWalReadEvent::Eof) => ReferenceEvent::Eof,
            Err(error) => ReferenceEvent::Error(error.to_string()),
        };
        let terminal = !matches!(event, ReferenceEvent::Record(..));
        out.push(event);
        if terminal {
            return out;
        }
    }
}

fn controlled(
    bytes: &[u8],
    generation: u64,
    limit: Option<usize>,
    work: &CheckpointWorkContext,
) -> Vec<ReferenceEvent> {
    let mut reader =
        CheckpointBinaryWalReader::new(Cursor::new(bytes), generation, limit, work).unwrap();
    let mut out = Vec::new();
    loop {
        let event = match reader.next_event() {
            Ok(CheckpointWalReadEvent::Record {
                payload,
                start_offset,
                end_offset,
            }) => ReferenceEvent::Record(payload.to_vec(), start_offset, end_offset),
            Ok(CheckpointWalReadEvent::TornTail {
                valid_prefix_len,
                reason,
            }) => ReferenceEvent::Torn(valid_prefix_len, reason),
            Ok(CheckpointWalReadEvent::Corrupt { offset, reason }) => {
                ReferenceEvent::Corrupt(offset, reason)
            }
            Ok(CheckpointWalReadEvent::Eof) => ReferenceEvent::Eof,
            Err(error) => ReferenceEvent::Error(error.to_string()),
        };
        let terminal = !matches!(event, ReferenceEvent::Record(..));
        out.push(event);
        if terminal {
            return out;
        }
    }
}

#[test]
fn checkpoint_units_wal_cursor_complete_events_match_ordinary_damage_and_boundary_matrix() {
    let local = LocalQosScheduler::new(LocalQosPolicy {
        max_background_operations: Some(1),
        max_total_background_operations: Some(1),
        ..Default::default()
    });
    let work = CheckpointWorkContext::default().with_scheduler(local.clone());
    for length in [
        0,
        1,
        14,
        15,
        32752,
        32753,
        32754,
        65536,
        3 * WAL_BLOCK_BYTES + 7,
    ] {
        let payload: Vec<_> = (0..length).map(|i| ((i * 71) % 256) as u8).collect();
        let bytes = frame_binary_wal_record(17, &payload, 0);
        for limit in [None, Some(0), Some(length.saturating_sub(1)), Some(length)] {
            assert_eq!(
                controlled(&bytes, 17, limit, &work),
                ordinary(&bytes, 17, limit)
            );
        }
        let mut positions = vec![
            0,
            1,
            4,
            5,
            6,
            7,
            14,
            15,
            bytes.len().saturating_sub(1),
            bytes.len(),
        ];
        positions.extend((0..bytes.len()).step_by(WAL_BLOCK_BYTES));
        for position in positions {
            if position > bytes.len() {
                continue;
            }
            let truncated = &bytes[..position];
            assert_eq!(
                controlled(truncated, 17, None, &work),
                ordinary(truncated, 17, None)
            );
            if position < bytes.len() {
                let mut damaged = bytes.clone();
                damaged[position] ^= 0x80;
                assert_eq!(
                    controlled(&damaged, 17, None, &work),
                    ordinary(&damaged, 17, None)
                );
            }
        }
        assert_eq!(
            controlled(&bytes, 18, None, &work),
            ordinary(&bytes, 18, None)
        );
    }
    // End markers with valid current-generation records later must fail closed;
    // an incomplete chain followed by a live block is corruption as well.
    let full = frame_binary_wal_record(17, &[1; WAL_BLOCK_BYTES - WAL_FRAGMENT_HEADER_BYTES], 0);
    let stale = frame_binary_wal_record(18, &[2; WAL_BLOCK_BYTES - WAL_FRAGMENT_HEADER_BYTES], 0);
    let live = frame_binary_wal_record(17, b"live", 0);
    for prefix in [
        stale,
        vec![0; WAL_BLOCK_BYTES],
        frame_binary_wal_record(17, &[3; 2 * WAL_BLOCK_BYTES], 0)[..WAL_BLOCK_BYTES].to_vec(),
    ] {
        let mut bytes = prefix;
        bytes.extend_from_slice(&live);
        assert_eq!(
            controlled(&bytes, 17, None, &work),
            ordinary(&bytes, 17, None)
        );
    }
    let mut two = full;
    two.extend_from_slice(&live);
    assert_eq!(controlled(&two, 17, None, &work), ordinary(&two, 17, None));
    assert_eq!(local.snapshot().running_background_operations, 0);
}

fn governor(bytes: u64) -> hawdb_qos::RuntimeGovernor {
    hawdb_qos::RuntimeGovernor::new(
        hawdb_qos::RuntimeGovernorConfig {
            memory_budget_bytes: Some(bytes),
            background_task_limit: Some(std::num::NonZeroUsize::MIN),
            ..hawdb_qos::RuntimeGovernorConfig::shared_host()
        },
        hawdb_qos::RuntimeResourceSnapshot::from_parts(
            hawdb_qos::RuntimeResourceBudget::from_limits(std::num::NonZeroUsize::MIN, None, None),
            hawdb_qos::RuntimeMemorySnapshot::from_limits(
                Some(1 << 30),
                Some(1 << 30),
                None,
                None,
                None,
            ),
        ),
        hawdb_qos::IoConcurrencyBudget::new(2, 1),
    )
}

fn read_record(bytes: &[u8], work: &CheckpointWorkContext) -> Result<CheckpointBytes> {
    let mut reader = CheckpointBinaryWalReader::new(Cursor::new(bytes), 17, None, work)?;
    match reader.next_event()? {
        CheckpointWalReadEvent::Record { payload, .. } => Ok(payload),
        event => panic!("complete fixture must return a record: {event:?}"),
    }
}

#[test]
fn checkpoint_units_wal_cursor_admits_exact_input_overlap_and_retains_record_after_execution() {
    use hawdb_core::{RuntimeMemoryError, RuntimeTaskContext};
    let payload = b"payload";
    let framed = frame_binary_wal_record(17, payload, 0);
    let requested = (WAL_BLOCK_BYTES + payload.len()) as u64 + 48;
    let denied = governor(requested - 1);
    let permit = denied
        .try_admit(
            hawdb_qos::RuntimeWorkRequest::background_maintenance(requested - 1)
                .with_io_wave_slots(1),
        )
        .unwrap();
    let work = CheckpointWorkContext::new(permit.bind_task_context(RuntimeTaskContext::default()));
    let result = work.classify(|work| read_record(&framed, work));
    assert!(
        matches!(result, Err(crate::background::CheckpointOperationError::Work(CheckpointWorkError::Memory(RuntimeMemoryError::ReservationExceeded { requested_bytes, available_bytes }))) if requested_bytes == payload.len() as u64 + 24 && available_bytes == payload.len() as u64 + 23)
    );
    drop(work);
    drop(permit);
    assert_eq!(denied.snapshot().admitted_memory_bytes, 0);
    assert_eq!(denied.snapshot().active_background_io_slots, 0);

    let allowed = governor(requested);
    let permit = allowed
        .try_admit(
            hawdb_qos::RuntimeWorkRequest::background_maintenance(requested).with_io_wave_slots(1),
        )
        .unwrap();
    let task = permit.bind_task_context(RuntimeTaskContext::default());
    let work = CheckpointWorkContext::new(task.clone());
    let mut reader = CheckpointBinaryWalReader::new(Cursor::new(&framed), 17, None, &work).unwrap();
    let CheckpointWalReadEvent::Record {
        payload: output, ..
    } = reader.next_event().unwrap()
    else {
        panic!("complete fixture must return a record")
    };
    assert!(matches!(
        task.reserve_working_memory(1),
        Err(RuntimeMemoryError::ReservationExceeded {
            available_bytes: 0,
            ..
        })
    ));
    drop(reader);
    assert!(
        matches!(task.reserve_working_memory(requested), Err(RuntimeMemoryError::ReservationExceeded { available_bytes, .. }) if available_bytes == WAL_BLOCK_BYTES as u64 + 24)
    );
    drop(work);
    drop(task);
    drop(permit);
    let closed = allowed.snapshot();
    assert_eq!(closed.active_background_tasks, 0);
    assert_eq!(closed.active_cpu_slots, 0);
    assert_eq!(closed.active_background_io_slots, 0);
    // A retained allocation keeps the shared admission envelope reserved;
    // working capacity from the dropped read buffer was returned above.
    assert_eq!(closed.admitted_memory_bytes, requested);
    assert_eq!(&*output, payload);
    drop(output);
    assert_eq!(allowed.snapshot().admitted_memory_bytes, 0);
}

#[test]
fn checkpoint_units_wal_cursor_cancels_every_actual_cpu_unit_and_retries_same_reservation() {
    use crate::background::CheckpointWorkProbe;
    use hawdb_core::{RuntimeMemoryError, RuntimeTaskContext};
    use std::sync::{atomic::Ordering, Arc};
    let payload: Vec<_> = (0..3 * WAL_BLOCK_BYTES + 7)
        .map(|i| (i % 256) as u8)
        .collect();
    let framed = frame_binary_wal_record(17, &payload, 0);
    let local = LocalQosScheduler::new(LocalQosPolicy {
        max_background_operations: Some(1),
        max_total_background_operations: Some(1),
        ..Default::default()
    });
    let baseline = Arc::new(CheckpointWorkProbe::default());
    let baseline_work = baseline.context(local.clone());
    let output = baseline_work
        .classify(|work| read_record(&framed, work))
        .unwrap_or_else(|_| panic!("baseline admitted read must complete"));
    assert_eq!(&*output, payload);
    drop(output);
    let units = baseline.completed.load(Ordering::SeqCst);
    assert!(units > 15);
    baseline.assert_released(&local);
    for stop in 0..=units {
        let ceiling = 2 * 1024 * 1024;
        let governor = governor(ceiling);
        let permit = governor
            .try_admit(
                hawdb_qos::RuntimeWorkRequest::background_maintenance(ceiling)
                    .with_io_wave_slots(1),
            )
            .unwrap();
        let probe = Arc::new(CheckpointWorkProbe::default());
        probe.cancel_after.store(stop, Ordering::SeqCst);
        local.set_telemetry_sink(Some(probe.clone()));
        let task = permit.bind_task_context(RuntimeTaskContext::without_deadline(
            probe.cancellation.clone(),
        ));
        let work = CheckpointWorkContext::new(task).with_scheduler(local.clone());
        let held = (stop == 0).then(|| {
            local
                .try_start(hawdb_qos::WorkRequest::background(
                    hawdb_qos::WorkClass::Mutation,
                    1,
                ))
                .unwrap()
        });
        let result = work.classify(|work| read_record(&framed, work));
        assert!(
            matches!(result, Err(crate::background::CheckpointOperationError::Work(CheckpointWorkError::Admission(_))) if stop == 0)
                || matches!(result, Err(crate::background::CheckpointOperationError::Work(CheckpointWorkError::Stopped(_))) if stop > 0)
        );
        drop(held);
        probe.assert_released(&local);
        drop(work);
        local.set_telemetry_sink(None);
        assert_eq!(governor.snapshot().active_background_io_slots, 0);
        let task = permit.bind_task_context(RuntimeTaskContext::default());
        assert!(
            matches!(task.reserve_working_memory(ceiling), Err(RuntimeMemoryError::ReservationExceeded { available_bytes, .. }) if available_bytes == ceiling)
        );
        let work = CheckpointWorkContext::new(task.clone()).with_scheduler(local.clone());
        let output = read_record(&framed, &work).unwrap();
        assert_eq!(&*output, payload);
        drop(output);
        assert!(
            matches!(task.reserve_working_memory(ceiling), Err(RuntimeMemoryError::ReservationExceeded { available_bytes, .. }) if available_bytes == ceiling)
        );
        drop(work);
        drop(task);
        drop(permit);
        assert_eq!(governor.snapshot().admitted_memory_bytes, 0);
        assert_eq!(governor.snapshot().active_background_io_slots, 0);
        assert_eq!(local.snapshot().running_background_operations, 0);
    }
}

struct CountedSource {
    cursor: Cursor<Vec<u8>>,
    bytes: std::sync::Arc<std::sync::atomic::AtomicUsize>,
}

impl std::io::Read for CountedSource {
    fn read(&mut self, out: &mut [u8]) -> std::io::Result<usize> {
        let length = std::io::Read::read(&mut self.cursor, out)?;
        self.bytes
            .fetch_add(length, std::sync::atomic::Ordering::SeqCst);
        Ok(length)
    }
}

impl std::io::Seek for CountedSource {
    fn seek(&mut self, position: std::io::SeekFrom) -> std::io::Result<u64> {
        std::io::Seek::seek(&mut self.cursor, position)
    }
}

#[test]
fn checkpoint_units_wal_cursor_captured_range_reads_only_suffix_and_ignores_later_damage() {
    let prefix = frame_binary_wal_record(17, &vec![5; 40 * WAL_BLOCK_BYTES + 13], 0);
    let payload = vec![9; 2 * WAL_BLOCK_BYTES + 7];
    let suffix = frame_binary_wal_record(17, &payload, prefix.len() as u64);
    let from = (WAL_BINARY_FILE_HEADER_BYTES + prefix.len()) as u64;
    let to = from + suffix.len() as u64;
    let mut file = encode_binary_wal_header(17, 1);
    file.extend_from_slice(&prefix);
    file.extend_from_slice(&suffix);
    file.extend_from_slice(&[0xff; WAL_BLOCK_BYTES]);
    let count = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let source = CountedSource {
        cursor: Cursor::new(file),
        bytes: count.clone(),
    };
    let local = LocalQosScheduler::new(LocalQosPolicy {
        max_background_operations: Some(1),
        max_total_background_operations: Some(1),
        ..Default::default()
    });
    let work = CheckpointWorkContext::default().with_scheduler(local.clone());
    let mut reader = CheckpointBinaryWalReader::range(source, 17, None, from, to, &work).unwrap();
    let CheckpointWalReadEvent::Record {
        payload: actual,
        start_offset,
        end_offset,
    } = reader.next_event().unwrap()
    else {
        panic!("captured suffix must be a complete record")
    };
    assert_eq!(&*actual, payload);
    assert!(start_offset >= from);
    assert_eq!(end_offset, to);
    assert!(matches!(
        reader.next_event().unwrap(),
        CheckpointWalReadEvent::Eof
    ));
    let read = count.load(std::sync::atomic::Ordering::SeqCst);
    assert!(
        read as u64 <= to - from + WAL_BLOCK_BYTES as u64,
        "the reader must seek instead of rescanning the committed prefix: {read}"
    );
    assert!(read < prefix.len());
    assert_eq!(local.snapshot().running_background_operations, 0);
}
