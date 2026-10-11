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
use hawdb_core::RuntimeTaskContext;
use hawdb_qos::{
    IoConcurrencyBudget, LocalQosPolicy, LocalQosScheduler, RuntimeGovernor, RuntimeGovernorConfig,
    RuntimeMaintenanceWork, RuntimeMemorySnapshot, RuntimeResourceBudget, RuntimeResourceSnapshot,
};

pub(super) const OWNER: u64 = 4096;

pub(super) fn admitted(
    ceiling: u64,
) -> (
    RuntimeGovernor,
    RuntimeMaintenanceWork,
    CheckpointWorkContext,
) {
    let governor = RuntimeGovernor::new(
        RuntimeGovernorConfig {
            memory_budget_bytes: Some(4 * 1024 * 1024),
            background_task_limit: Some(NonZeroUsize::MIN),
            ..RuntimeGovernorConfig::shared_host()
        },
        RuntimeResourceSnapshot::from_parts(
            RuntimeResourceBudget::from_limits(NonZeroUsize::MIN, None, None),
            RuntimeMemorySnapshot::from_limits(Some(1 << 30), Some(1 << 30), None, None, None),
        ),
        IoConcurrencyBudget::new(2, 1),
    );
    let admission = governor
        .try_admit_incremental_maintenance(OWNER, ceiling, 1, RuntimeTaskContext::default())
        .unwrap();
    let scheduler = LocalQosScheduler::new(LocalQosPolicy {
        max_background_operations: Some(1),
        max_total_background_operations: Some(1),
        ..Default::default()
    });
    let work = CheckpointWorkContext::new(admission.task_context().unwrap().clone())
        .with_scheduler(scheduler);
    (governor, admission, work)
}

pub(super) fn definition(
    kind: PersistentPropertyProjectionKind,
    property: String,
) -> PersistentPropertyProjectionDefinition {
    PersistentPropertyProjectionDefinition {
        label_id: LabelId(7),
        property,
        kind,
        complete: false,
    }
}
