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
/// The reservation is charged to both the originating governor and its
/// process-memory policy. It does not admit CPU, task or I/O work.
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

    /// Check whether new work belongs to the governor paying for this state.
    /// This is an identity check only; it grants no execution or memory capacity.
    pub fn shares_governor_with(&self, permit: &RuntimePermit) -> bool {
        Arc::ptr_eq(&self.governor, &permit.governor)
    }

    /// Charges memory that is already allocated and that this owner keeps
    /// alive, without admission.
    ///
    /// Use this only for bytes that stay resident whatever admission decides,
    /// for example a superseded generation that a live reader still holds, so a
    /// refusal could not free them. The charge counts toward admitted memory
    /// and can exceed the current budget; new admissions then wait until it is
    /// released. No process-memory reservation is taken because the host's
    /// resident sample already includes these bytes.
    pub fn charge_resident(&mut self, bytes: u64) {
        if bytes == 0 {
            return;
        }
        let mut state = mutex_lock(&self.governor.state);
        state.admitted_memory_bytes = state.admitted_memory_bytes.saturating_add(bytes);
        self.bytes = self.bytes.saturating_add(bytes);
    }

    pub(super) fn merge(&mut self, incoming: &mut Self) -> bool {
        if !Arc::ptr_eq(&self.governor, &incoming.governor) {
            return false;
        }
        let Some(bytes) = self.bytes.checked_add(incoming.bytes) else {
            return false;
        };
        match (&mut self.process_memory, &mut incoming.process_memory) {
            (None, None) => {}
            (Some(target), Some(source)) => {
                if !target.merge(source) {
                    return false;
                }
            }
            _ => return false,
        }
        self.bytes = bytes;
        incoming.bytes = 0;
        true
    }

    pub(super) fn shrink_to(&mut self, bytes: u64) {
        let released = self.bytes - bytes;
        if let Some(memory) = &mut self.process_memory {
            memory.shrink_to(bytes);
        }
        {
            let mut state = mutex_lock(&self.governor.state);
            state.admitted_memory_bytes -= released;
        }
        self.bytes = bytes;
        self.governor.notify_next_admission_waiter();
    }
}

impl RuntimePermit {
    /// Completes fixed-reservation work while preserving its entire memory charge.
    ///
    /// The transfer does not release and reacquire memory, so another owner cannot
    /// consume the capacity between preparation and execution. CPU, task and I/O
    /// slots are released before return. Retained state must be dropped before
    /// this token. Elastic maintenance permits cannot transfer their controller;
    /// they return `None` and release their resources normally.
    pub fn into_retained_memory(mut self) -> Option<RuntimeRetainedMemory> {
        if self.memory_controller.is_some() {
            return None;
        }
        let retained = RuntimeRetainedMemory {
            governor: Arc::clone(&self.governor),
            bytes: self.request.reserved_memory_bytes(),
            process_memory: self.process_memory_reservation.take(),
        };
        self.request.memory_bytes = 0;
        self.request.result_bytes = 0;
        self.release_inner();
        Some(retained)
    }

    /// An empty retained-memory owner charged to this permit's governor.
    ///
    /// It holds no bytes and never refuses. Grow it only with
    /// [`RuntimeRetainedMemory::charge_resident`] for memory that is already
    /// resident; use [`Self::reserve_retained_memory`] for new allocations.
    pub fn resident_memory_owner(&self) -> RuntimeRetainedMemory {
        RuntimeRetainedMemory {
            governor: Arc::clone(&self.governor),
            bytes: 0,
            process_memory: None,
        }
    }

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
        self.governor
            .reserve_retained_memory(self.request.priority, bytes)
    }
}

impl RuntimeGovernorInner {
    pub(super) fn reserve_retained_memory(
        self: &Arc<Self>,
        priority: RuntimeWorkPriority,
        bytes: u64,
    ) -> Result<RuntimeRetainedMemory, RuntimeAdmissionError> {
        self.reserve_retained_memory_with_floor(priority, bytes, 0)
    }

    pub(super) fn reserve_retained_memory_with_floor(
        self: &Arc<Self>,
        priority: RuntimeWorkPriority,
        bytes: u64,
        irreducible_bytes: u64,
    ) -> Result<RuntimeRetainedMemory, RuntimeAdmissionError> {
        let mut state = mutex_lock(&self.state);
        let capacity = state.process_memory_policy.as_ref().map_or(
            state.limits.memory_capacity_bytes,
            |policy| {
                state
                    .limits
                    .memory_capacity_bytes
                    .min(policy.resident_limit_bytes())
            },
        );
        let capacity = capacity.saturating_sub(irreducible_bytes);
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
            governor: self.clone(),
            bytes,
            process_memory,
        })
    }
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
