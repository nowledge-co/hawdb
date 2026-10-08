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
use hawdb_qos::{
    IoConcurrencyBudget, RuntimeGovernor, RuntimeGovernorConfig, RuntimeMemorySnapshot,
    RuntimeResourceBudget, RuntimeResourceSnapshot, RuntimeTaskContext, RuntimeWorkRequest,
};
use std::num::NonZeroUsize;

fn governor(ceiling: u64) -> RuntimeGovernor {
    RuntimeGovernor::new(
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
    )
}

#[test]
fn checkpoint_units_wal_decode_memory_retains_owned_strings_and_binary_until_record_drop() {
    let original = WalEntry {
        lsn: 17,
        op: WalOp::SetNodeProperty {
            id: NodeId(3),
            property: "🦀界".repeat(20_000),
            value: Value::Binary(vec![0x9a; 3 * 64 * 1024 + 7]),
        },
    };
    let bytes = encode_binary_wal_record(&original, 19).unwrap();
    let ceiling = 4 * 1024 * 1024;
    let governor = governor(ceiling);
    let permit = governor
        .try_admit(RuntimeWorkRequest::background_maintenance(ceiling))
        .unwrap();
    let work = CheckpointWorkContext::new(permit.bind_task_context(RuntimeTaskContext::default()));
    let entry = match decode_binary_wal_record_with_work_context(&bytes, &work).unwrap() {
        BinaryWalRecordDecode::Entry {
            entry,
            commit_epoch,
        } => {
            assert_eq!(commit_epoch, 19);
            entry
        }
        BinaryWalRecordDecode::Corrupt(_) => panic!("the complete record must decode"),
    };
    assert_eq!(encode_binary_wal_record(&entry, 19).unwrap(), bytes);
    drop(work);
    drop(permit);
    assert_eq!(governor.snapshot().active_background_io_slots, 0);
    assert_eq!(
        governor.snapshot().admitted_memory_bytes,
        ceiling,
        "live decoded owned strings/binary must retain their shared admission envelope after task closure",
    );
    assert_eq!(encode_binary_wal_record(&entry, 19).unwrap(), bytes);
    drop(entry);
    assert_eq!(governor.snapshot().admitted_memory_bytes, 0);
}
