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

/// Memory that may outlive active work without retaining execution slots.
///
/// This is an additional reservation, charged to both the originating governor
/// and its process-memory policy. It does not admit CPU, task or I/O work.
#[derive(Debug)]
pub struct RuntimeRetainedMemory {
    pub(super) governor: Arc<RuntimeGovernorInner>,
    bytes: u64,
    process_memory: Option<ProcessMemoryReservation>,
}

impl RuntimeRetainedMemory {
    pub const fn bytes(&self) -> u64 {
        self.bytes
    }
}

impl RuntimePermit {
    /// Admits extra memory for state that can survive this operation.
    ///
    /// Reserve before allocating that state. The original working-memory ceiling
    /// remains unchanged, so the two reservations overlap conservatively while
    /// work is active. Failure leaves both existing admission and counters intact.
    /// This does not bypass memory pressure or release another active owner's
    /// resources, and is not a permit to execute background cleanup.
    pub fn reserve_retained_memory(
        &self,
        bytes: u64,
    ) -> Result<RuntimeRetainedMemory, RuntimeAdmissionError> {
        reserve_retained_memory(&self.governor, self.request.priority, bytes)
    }
}

pub(super) fn reserve_retained_memory(
    governor: &Arc<RuntimeGovernorInner>,
    priority: RuntimeWorkPriority,
    bytes: u64,
) -> Result<RuntimeRetainedMemory, RuntimeAdmissionError> {
    let mut state = mutex_lock(&governor.state);
    let capacity =
        state
            .process_memory_policy
            .as_ref()
            .map_or(state.limits.memory_capacity_bytes, |policy| {
                state
                    .limits
                    .memory_capacity_bytes
                    .min(policy.resident_limit_bytes())
            });
    if bytes > capacity {
        return Err(admission_error_value(
            RuntimeAdmissionCode::MemorySaturated,
            bytes,
            capacity,
            false,
        ));
    }
    if priority == RuntimeWorkPriority::Background
        && state.resources.memory.pressure == RuntimeMemoryPressure::Critical
    {
        return Err(admission_error_value(
            RuntimeAdmissionCode::MemoryPressure,
            bytes,
            0,
            true,
        ));
    }
    let available = state
        .limits
        .memory_budget_bytes
        .saturating_sub(state.admitted_memory_bytes);
    if bytes > available {
        return Err(admission_error_value(
            RuntimeAdmissionCode::MemorySaturated,
            bytes,
            available,
            true,
        ));
    }
    let process_memory = state
        .process_memory_policy
        .as_ref()
        .map(|policy| {
            policy
                .try_reserve(bytes)
                .map_err(|error| RuntimeAdmissionError {
                    code: match error.code {
                        ProcessMemoryAdmissionCode::SampleUnavailable => {
                            RuntimeAdmissionCode::MemoryPressure
                        }
                        ProcessMemoryAdmissionCode::ResidentLimitExceeded => {
                            RuntimeAdmissionCode::MemorySaturated
                        }
                    },
                    requested: error.requested,
                    available: error.available,
                    retryable: error.retryable,
                })
        })
        .transpose()?;
    state.admitted_memory_bytes += bytes;
    Ok(RuntimeRetainedMemory {
        governor: governor.clone(),
        bytes,
        process_memory,
    })
}

impl Drop for RuntimeRetainedMemory {
    fn drop(&mut self) {
        // Policy notifications may lock the governor; never deliver them under
        // its state lock. The temporary overlap is conservatively charged.
        self.process_memory.take();
        {
            let mut state = mutex_lock(&self.governor.state);
            state.admitted_memory_bytes = state.admitted_memory_bytes.saturating_sub(self.bytes);
        }
        self.governor.notify_next_admission_waiter();
    }
}

#[cfg(test)]
mod tests;
