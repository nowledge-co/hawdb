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
use hawdb_core::{RuntimeMemoryError, RuntimeTaskContext};
use hawdb_qos::{
    IoConcurrencyBudget, LocalQosPolicy, LocalQosScheduler, RuntimeGovernor, RuntimeGovernorConfig,
    RuntimeMemorySnapshot, RuntimeResourceBudget, RuntimeResourceSnapshot, RuntimeWorkRequest,
};
use std::io::Write;
use std::num::NonZeroUsize;
use std::sync::atomic::Ordering;
use std::sync::Arc;

fn collect(
    payload: &[u8],
    generation: u64,
    position: u64,
    work: &CheckpointWorkContext,
) -> std::result::Result<Vec<u8>, CheckpointWorkError> {
    let mut stream = CheckpointWalFrameStream::new(generation, payload, position, work)?;
    // The independent test sink owns its comparison bytes. Production writes
    // these borrowed parts directly to a file without a complete output Vec.
    let mut output = Vec::with_capacity(stream.encoded_len());
    while let Some(fragment) = stream.next()? {
        assert!(fragment.encoded_len() < WAL_BLOCK_BYTES + WAL_FRAGMENT_HEADER_BYTES);
        for part in fragment.parts() {
            output.extend_from_slice(part);
        }
    }
    assert_eq!(output.len(), stream.encoded_len());
    Ok(output)
}

fn scheduler() -> LocalQosScheduler {
    LocalQosScheduler::new(LocalQosPolicy {
        max_background_operations: Some(1),
        max_total_background_operations: Some(1),
        ..Default::default()
    })
}

#[test]
fn checkpoint_units_wal_stream_matches_every_position_and_fragment_boundary() {
    let local = scheduler();
    let work = CheckpointWorkContext::default().with_scheduler(local.clone());
    for position in 0..WAL_BLOCK_BYTES as u64 {
        assert_eq!(
            collect(&[0, 1, 255, 3, 0, 5, 9], 17, position, &work).unwrap(),
            frame_binary_wal_record(17, &[0, 1, 255, 3, 0, 5, 9], position)
        );
    }
    for length in [
        0,
        1,
        14,
        15,
        32_752,
        32_753,
        32_754,
        65_536,
        5 * WAL_BLOCK_BYTES + 7,
    ] {
        let payload: Vec<_> = (0..length).map(|i| ((i * 71) % 256) as u8).collect();
        for position in [0, 1, 32_752, 32_753, 32_754, 32_767, u64::MAX] {
            for generation in [0, 1, u64::MAX] {
                assert_eq!(
                    collect(&payload, generation, position, &work).unwrap(),
                    frame_binary_wal_record(generation, &payload, position)
                );
            }
        }
    }
    assert_eq!(local.snapshot().running_background_operations, 0);
}

#[test]
fn checkpoint_units_wal_stream_cancels_every_fragment_and_retries_same_source() {
    let payload: Vec<_> = (0..3 * 64 * 1024 + 7)
        .map(|i| ((i * 71) % 256) as u8)
        .collect();
    let expected = frame_binary_wal_record(31, &payload, 32_767);
    let local = scheduler();
    let baseline = Arc::new(CheckpointWorkProbe::default());
    assert_eq!(
        collect(&payload, 31, 32_767, &baseline.context(local.clone())).unwrap(),
        expected
    );
    let count = baseline.completed.load(Ordering::SeqCst);
    assert!(count > 1);
    for cut in 1..=count {
        let probe = Arc::new(CheckpointWorkProbe::default());
        probe.cancel_after.store(cut, Ordering::SeqCst);
        assert!(matches!(
            collect(&payload, 31, 32_767, &probe.context(local.clone())),
            Err(CheckpointWorkError::Stopped(_))
        ));
        probe.assert_released(&local);
        let retry = Arc::new(CheckpointWorkProbe::default());
        assert_eq!(
            collect(&payload, 31, 32_767, &retry.context(local.clone())).unwrap(),
            expected
        );
        retry.assert_released(&local);
    }
}

#[test]
fn checkpoint_units_wal_stream_writes_native_output_with_one_byte_working_reservation() {
    let payload: Vec<_> = (0..257 * 1024 + 7)
        .map(|i| ((i * 17) % 256) as u8)
        .collect();
    let governor = RuntimeGovernor::new(
        RuntimeGovernorConfig {
            memory_budget_bytes: Some(1),
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
        .try_admit(RuntimeWorkRequest::background_maintenance(1).with_io_wave_slots(1))
        .unwrap();
    let local = scheduler();
    let task = permit.bind_task_context(RuntimeTaskContext::default());
    let work = CheckpointWorkContext::new(task.clone()).with_scheduler(local.clone());
    let path = std::env::temp_dir().join(format!(
        "hawdb-wal-stream-{}",
        hawdb_core::generate_uuidv7().unwrap()
    ));
    let mut file = crate::file_io::File::create(&path).unwrap();
    let mut stream = CheckpointWalFrameStream::new(23, &payload, 32_767, &work).unwrap();
    while let Some(fragment) = stream.next().unwrap() {
        let wave = work.io_wave().unwrap().expect("real governor wave");
        assert_eq!(governor.snapshot().active_background_io_slots, 1);
        for part in fragment.parts() {
            file.write_all(part).unwrap();
        }
        drop(wave);
    }
    file.sync_all().unwrap();
    drop(file);
    assert_eq!(
        std::fs::read(&path).unwrap(),
        frame_binary_wal_record(23, &payload, 32_767)
    );
    // Input ownership belongs to the caller; the framing step needs no payload-
    // sized reservation and has not consumed the single working byte.
    assert!(matches!(
        task.reserve_working_memory(1),
        Err(RuntimeMemoryError::ReservationExceeded {
            available_bytes: 1,
            ..
        })
    ));
    assert_eq!(local.snapshot().running_background_operations, 0);
    assert_eq!(governor.snapshot().active_background_io_slots, 0);
    drop(work);
    drop(task);
    drop(permit);
    assert_eq!(governor.snapshot().admitted_memory_bytes, 0);
    std::fs::remove_file(path).unwrap();
}

#[test]
#[ignore = "Local-only seeded differential campaign; run through the Bazel fuzz target"]
fn checkpoint_wal_verbatim_stream_differential_campaign() {
    let local = scheduler();
    let work = CheckpointWorkContext::default().with_scheduler(local.clone());
    let mut state = 0x7d83_4cb2_948a_e605_u64;
    for case in 0..1024 {
        state ^= state << 13;
        state ^= state >> 7;
        state ^= state << 17;
        let position = state;
        let generation = state.rotate_left(23);
        let length = (state as usize % (8 * WAL_BLOCK_BYTES)) + case % 17;
        let payload: Vec<_> = (0..length)
            .map(|i| (state.wrapping_add(i as u64).rotate_left((i % 63) as u32) >> 31) as u8)
            .collect();
        let actual = collect(&payload, generation, position, &work).unwrap();
        assert_eq!(
            actual,
            frame_binary_wal_record(generation, &payload, position),
            "case {case}"
        );
        // Independently parse the complete generated record, including trailer
        // padding and empty first fragments, under the ordinary reader.
        let mut bytes =
            vec![0; WAL_BINARY_FILE_HEADER_BYTES + (position % WAL_BLOCK_BYTES as u64) as usize];
        bytes.extend_from_slice(&actual);
        let from = (WAL_BINARY_FILE_HEADER_BYTES as u64) + position % WAL_BLOCK_BYTES as u64;
        let to = bytes.len() as u64;
        let mut reader =
            BinaryWalReader::range(std::io::Cursor::new(bytes), generation, None, from, to)
                .unwrap();
        match reader.next_event().unwrap() {
            BinaryWalReadEvent::Record {
                payload: decoded, ..
            } => assert_eq!(decoded, payload),
            _ => panic!("case {case}: complete stream must decode"),
        }
        assert!(matches!(
            reader.next_event().unwrap(),
            BinaryWalReadEvent::Eof
        ));
    }
    assert_eq!(local.snapshot().running_background_operations, 0);
}
