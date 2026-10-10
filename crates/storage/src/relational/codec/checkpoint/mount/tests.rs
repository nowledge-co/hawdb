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
use crate::background::{CheckpointOperationError, CheckpointWorkError};
use hawdb_core::RuntimeTaskContext;
use hawdb_qos::{IoConcurrencyBudget, RuntimeGovernor, RuntimeGovernorConfig};

#[test]
fn input_buffer_denies_before_allocation_and_refunds_after_complete_file_read() {
    const OWNER: u64 = 4096;
    let path = std::env::temp_dir().join(format!(
        "hawdb-relational-input-memory-{}",
        hawdb_core::generate_uuidv7().unwrap()
    ));
    let payload: Vec<_> = (0..INPUT_BYTES + 257).map(|offset| offset as u8).collect();
    let encoded = encode_envelope(CHECKPOINT_MAGIC, 41, payload.clone()).unwrap();
    std::fs::write(&path, &encoded).unwrap();
    let governor = RuntimeGovernor::detect(
        RuntimeGovernorConfig {
            memory_budget_bytes: Some(2 * 1024 * 1024),
            ..RuntimeGovernorConfig::shared_host()
        },
        IoConcurrencyBudget::new(2, 1),
    );
    governor.pin_resources();
    let open = || {
        FileDecodeInput::open_checkpoint(
            &path,
            encoded.len() as u64,
            RelationalDecodeLimits::checkpoint(),
        )
        .unwrap()
        .1
    };
    let denied = governor
        .try_admit_incremental_maintenance(
            OWNER,
            INPUT_BYTES as u64 - 1,
            1,
            RuntimeTaskContext::default(),
        )
        .unwrap();
    let work = CheckpointWorkContext::new(denied.task_context().unwrap().clone());
    let result = work.classify(|work| CheckpointFileInput::new(open(), work).map(drop));
    assert!(
        matches!(
            result,
            Err(CheckpointOperationError::Work(CheckpointWorkError::Memory(
                _
            )))
        ),
        "the input buffer must be admitted before allocation"
    );
    assert_eq!(denied.memory_report().live_accounted_bytes, 0);
    assert_eq!(governor.snapshot().admitted_memory_bytes, OWNER);
    assert_eq!(std::fs::read(&path).unwrap(), encoded);
    drop(work);
    drop(denied);
    assert_eq!(governor.snapshot().admitted_memory_bytes, 0);

    let admitted = governor
        .try_admit_incremental_maintenance(
            OWNER,
            INPUT_BYTES as u64 + 1024,
            1,
            RuntimeTaskContext::default(),
        )
        .unwrap();
    let work = CheckpointWorkContext::new(admitted.task_context().unwrap().clone());
    let mut input = CheckpointFileInput::new(open(), &work).unwrap();
    assert!(admitted.memory_report().live_accounted_bytes >= INPUT_BYTES as u64);
    let mut actual = vec![0; payload.len()];
    input.read_exact(&mut actual).unwrap();
    assert_eq!(actual, payload);
    input.finish().unwrap();
    assert_eq!(admitted.memory_report().live_accounted_bytes, 0);
    assert_eq!(governor.snapshot().admitted_memory_bytes, OWNER);
    assert_eq!(governor.snapshot().active_background_io_slots, 0);
    drop(work);
    drop(admitted);
    assert_eq!(governor.snapshot().admitted_memory_bytes, 0);
    assert_eq!(governor.snapshot().active_background_tasks, 0);
    assert_eq!(std::fs::read(&path).unwrap(), encoded);
    std::fs::remove_file(path).unwrap();
}
