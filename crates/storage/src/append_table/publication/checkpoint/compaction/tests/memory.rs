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
use hawdb_core::RuntimeMemoryError;
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
fn checkpoint_units_append_compaction_memory_denies_one_byte_without_data_deferral() {
    let fixture = Fixture::new();
    let authority = fixture.authority();
    let governor = governor(1);
    let permit = governor
        .try_admit(RuntimeWorkRequest::background_maintenance(1).with_io_wave_slots(1))
        .unwrap();
    let work = CheckpointWorkContext::new(permit.bind_task_context(RuntimeTaskContext::default()));
    let result = work.classify(|work| {
        plan_compaction(Some(&fixture.previous), &fixture.live, fixture.config, work)
    });
    assert!(
        matches!(
            &result,
            Err(CheckpointOperationError::Work(CheckpointWorkError::Memory(
                RuntimeMemoryError::ReservationExceeded { .. }
            )))
        ),
        "compaction must reject the working-memory allowance rather than complete or silently defer"
    );
    assert_eq!(fixture.authority(), authority);
    assert_eq!(
        fixture.previous.checkpoint_rows(2062).unwrap(),
        fixture.all[..2062]
    );
    assert_eq!(governor.snapshot().active_background_io_slots, 0);
    drop(work);
    drop(permit);
    assert_eq!(governor.snapshot().admitted_memory_bytes, 0);
}

#[test]
fn checkpoint_units_append_compaction_memory_retains_result_after_task_and_source_close() {
    let fixture = Fixture::new();
    let expected = fixture.all.clone();
    let ceiling = 32 * 1024 * 1024;
    let governor = governor(ceiling);
    let permit = governor
        .try_admit(RuntimeWorkRequest::background_maintenance(ceiling).with_io_wave_slots(1))
        .unwrap();
    let work = CheckpointWorkContext::new(permit.bind_task_context(RuntimeTaskContext::default()));
    let plan = plan_compaction(
        Some(&fixture.previous),
        &fixture.live,
        fixture.config,
        &work,
    )
    .unwrap();
    assert!(plan.due);
    assert_eq!(plan.checkpoint_rows.as_ref().unwrap(), &expected);
    drop(fixture);
    drop(work);
    drop(permit);
    assert_eq!(governor.snapshot().active_background_io_slots, 0);
    assert_eq!(
        governor.snapshot().admitted_memory_bytes,
        ceiling,
        "live compaction rows and their backing array must retain the admitted task envelope"
    );
    assert_eq!(plan.checkpoint_rows.as_ref().unwrap(), &expected);
    drop(plan);
    assert_eq!(governor.snapshot().admitted_memory_bytes, 0);
}

#[path = "memory_related.rs"]
mod related;
