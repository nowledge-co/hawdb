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
fn checkpoint_units_append_capture_memory_denies_before_allocating_live_row_capacity() {
    let state = live_state(5127);
    let expected = state.checkpoint_rows(5127).unwrap();
    let governor = governor(1);
    let permit = governor
        .try_admit(RuntimeWorkRequest::background_maintenance(1))
        .unwrap();
    let work = CheckpointWorkContext::new(permit.bind_task_context(RuntimeTaskContext::default()));
    let result = state.checkpoint_rows_with_work_context(5127, &work);
    assert!(
        matches!(&result, Err(AppendTableError::Admission(message)) if message.contains("checkpoint memory admission deferred")),
        "capture must reject the one-byte allowance before allocating 5127 row slots; captured {} rows",
        result.as_ref().map_or(0, |rows| rows.len())
    );
    assert_eq!(state.checkpoint_rows(5127).unwrap(), expected);
    assert_eq!(governor.snapshot().active_background_io_slots, 0);
    drop(work);
    drop(permit);
    assert_eq!(governor.snapshot().admitted_memory_bytes, 0);
}

#[test]
fn checkpoint_units_append_capture_memory_retains_owned_rows_after_task_and_source_close() {
    let state = live_state(5127);
    let expected = state.checkpoint_rows(5127).unwrap();
    let ceiling = 16 * 1024 * 1024;
    let governor = governor(ceiling);
    let permit = governor
        .try_admit(RuntimeWorkRequest::background_maintenance(ceiling))
        .unwrap();
    let work = CheckpointWorkContext::new(permit.bind_task_context(RuntimeTaskContext::default()));
    let captured = state
        .checkpoint_rows_with_work_context(5127, &work)
        .unwrap();
    assert_eq!(captured, expected);
    drop(state);
    drop(work);
    drop(permit);
    assert_eq!(governor.snapshot().active_background_io_slots, 0);
    assert_eq!(
        governor.snapshot().admitted_memory_bytes,
        ceiling,
        "the live row array, cloned table names and partition/order keys must retain their admission"
    );
    assert_eq!(captured, expected);
    drop(captured);
    assert_eq!(governor.snapshot().admitted_memory_bytes, 0);
}

#[path = "memory_related_tests.rs"]
mod related_tests;
