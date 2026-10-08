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

#[test]
fn checkpoint_units_wal_decode_map_memory_keeps_empty_key_map_admitted_until_record_drop() {
    // No String/Vec/Arc buffer is needed for this owned map. Its standard-library
    // node is nevertheless an allocation, and must retain its admission alone.
    let entry = WalEntry {
        lsn: 17,
        op: WalOp::SetNodeProperty {
            id: NodeId(3),
            property: String::new(),
            value: Value::Map(BTreeMap::from([(String::new(), Value::Int(37))])),
        },
    };
    let bytes = encode_binary_wal_record(&entry, 19).unwrap();
    let ceiling = 4 * 1024 * 1024;
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
        .try_admit(RuntimeWorkRequest::background_maintenance(ceiling))
        .unwrap();
    let work = CheckpointWorkContext::new(permit.bind_task_context(RuntimeTaskContext::default()));
    let decoded = match decode_binary_wal_record_with_work_context(&bytes, &work).unwrap() {
        BinaryWalRecordDecode::Entry { entry, .. } => entry,
        BinaryWalRecordDecode::Corrupt(_) => panic!("complete map record must decode"),
    };
    let WalOp::SetNodeProperty {
        value: Value::Map(map),
        ..
    } = &decoded.op
    else {
        panic!("the decoded map must remain owned by the record");
    };
    assert_eq!(map.len(), 1);
    assert_eq!(map.get(""), Some(&Value::Int(37)));
    assert_eq!(encode_binary_wal_record(&decoded, 19).unwrap(), bytes);
    drop(work);
    drop(permit);
    let closed = governor.snapshot();
    assert_eq!(closed.active_cpu_slots, 0);
    assert_eq!(closed.active_background_tasks, 0);
    assert_eq!(closed.admitted_memory_bytes, ceiling);
    assert_eq!(encode_binary_wal_record(&decoded, 19).unwrap(), bytes);
    drop(decoded);
    assert_eq!(governor.snapshot().admitted_memory_bytes, 0);
}
