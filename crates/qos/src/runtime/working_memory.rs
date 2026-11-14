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

//! Actual background allocations share one working ceiling. Reserved work
//! retains its original working/process reservation through the last lease.
//! Incremental work charges each allocation separately and keeps a fixed
//! controller reservation through its final owner, without retaining execution.

use super::*;
use hawdb_core::{RuntimeMemoryController, RuntimeMemoryError, RuntimeMemoryPermit};

mod incremental;

#[derive(Debug)]
pub(super) struct GovernorMemoryController {
    governor: Arc<RuntimeGovernorInner>,
    ceiling: u64,
    incremental: bool,
    state: Mutex<WorkingMemoryState>,
}

#[derive(Debug)]
struct WorkingMemoryState {
    active: bool,
    charged_bytes: u64,
    peak_charged_bytes: u64,
    reservation: Option<WorkingMemoryReservation>,
}

#[derive(Debug)]
struct WorkingMemoryReservation {
    governor: Arc<RuntimeGovernorInner>,
    bytes: u64,
    process_memory: Option<ProcessMemoryReservation>,
}

#[derive(Debug)]
struct GovernorMemoryPermit {
    controller: Arc<GovernorMemoryController>,
    bytes: u64,
    charged_bytes: u64,
}

#[derive(Debug)]
pub(super) struct GovernorMemoryBinding(pub(super) Arc<GovernorMemoryController>);

impl GovernorMemoryController {
    pub(super) fn new(
        governor: Arc<RuntimeGovernorInner>,
        request: RuntimeWorkRequest,
        process_memory: Option<ProcessMemoryReservation>,
    ) -> Self {
        Self {
            governor: governor.clone(),
            ceiling: request.memory_bytes,
            incremental: false,
            state: Mutex::new(WorkingMemoryState {
                active: true,
                charged_bytes: 0,
                peak_charged_bytes: 0,
                reservation: Some(WorkingMemoryReservation {
                    governor,
                    bytes: request.reserved_memory_bytes(),
                    process_memory,
                }),
            }),
        }
    }

    pub(super) fn enable_incremental(&mut self, ceiling: u64) {
        self.ceiling = ceiling;
        self.incremental = true;
    }

    pub(super) fn ceiling(&self) -> u64 {
        self.ceiling
    }

    pub(super) fn report(&self) -> RuntimeMaintenanceMemoryReport {
        let state = mutex_lock(&self.state);
        RuntimeMaintenanceMemoryReport {
            incremental: self.incremental,
            working_ceiling_bytes: self.ceiling,
            upfront_reserved_bytes: state.reservation.as_ref().map_or(0, |owner| owner.bytes),
            live_accounted_bytes: state.charged_bytes,
            peak_accounted_bytes: state.peak_charged_bytes,
        }
    }

    pub(super) fn close(&self) {
        let reservation = {
            let mut state = mutex_lock(&self.state);
            state.active = false;
            if state.charged_bytes == 0 && !self.incremental {
                state.reservation.take()
            } else {
                None
            }
        };
        // Process-memory notifications may lock the governor. Drop both
        // reservations outside the ledger lock, including on zero usage.
        drop(reservation);
    }
}

impl RuntimeMemoryController for GovernorMemoryBinding {
    fn reserve(
        &self,
        bytes: u64,
        ceiling: u64,
    ) -> Result<Box<dyn RuntimeMemoryPermit>, RuntimeMemoryError> {
        if self.0.incremental {
            return self.0.reserve_incremental(bytes, ceiling);
        }
        if mutex_lock(&self.0.governor.state).resources.memory.pressure
            == RuntimeMemoryPressure::Critical
        {
            return Err(RuntimeMemoryError::Pressure);
        }
        let mut state = mutex_lock(&self.0.state);
        if !state.active {
            return Err(RuntimeMemoryError::Closed);
        }
        let available = self
            .0
            .ceiling
            .min(ceiling)
            .saturating_sub(state.charged_bytes);
        // The lease itself owns one allocation. Include its concrete payload
        // before allocating it; allocator metadata is a platform assumption.
        let charged = bytes
            .checked_add(std::mem::size_of::<GovernorMemoryPermit>() as u64)
            .filter(|charged| *charged <= available)
            .ok_or(RuntimeMemoryError::ReservationExceeded {
                requested_bytes: bytes
                    .saturating_add(std::mem::size_of::<GovernorMemoryPermit>() as u64),
                available_bytes: available,
            })?;
        state.charged_bytes += charged;
        state.peak_charged_bytes = state.peak_charged_bytes.max(state.charged_bytes);
        drop(state);
        Ok(Box::new(GovernorMemoryPermit {
            controller: self.0.clone(),
            bytes,
            charged_bytes: charged,
        }))
    }
}

impl RuntimeMemoryPermit for GovernorMemoryPermit {
    fn bytes(&self) -> u64 {
        self.bytes
    }

    fn as_any_mut(&mut self) -> Option<&mut dyn std::any::Any> {
        Some(self)
    }

    fn merge(
        &mut self,
        mut incoming: Box<dyn RuntimeMemoryPermit>,
    ) -> Result<(), Box<dyn RuntimeMemoryPermit>> {
        let Some(source) = incoming
            .as_any_mut()
            .and_then(|value| value.downcast_mut::<Self>())
        else {
            return Err(incoming);
        };
        if !Arc::ptr_eq(&self.controller, &source.controller) {
            return Err(incoming);
        }
        let Some(bytes) = self.bytes.checked_add(source.bytes) else {
            return Err(incoming);
        };
        let Some(charged) = self.charged_bytes.checked_add(source.charged_bytes) else {
            return Err(incoming);
        };
        // Transfer all charges before destroying the redundant permit. Its
        // zero-charge destructor cannot release the closed controller owner.
        self.bytes = bytes;
        self.charged_bytes = charged;
        source.bytes = 0;
        source.charged_bytes = 0;
        drop(incoming);
        // Refund only the concrete permit payload that was just destroyed.
        // Backing bytes, the surviving permit and the original owner stay live.
        let released = std::mem::size_of::<Self>() as u64;
        {
            let mut state = mutex_lock(&self.controller.state);
            state.charged_bytes -= released;
        }
        self.charged_bytes -= released;
        Ok(())
    }
}

impl Drop for GovernorMemoryPermit {
    fn drop(&mut self) {
        let reservation = {
            let mut state = mutex_lock(&self.controller.state);
            state.charged_bytes -= self.charged_bytes;
            if !state.active && state.charged_bytes == 0 {
                state.reservation.take()
            } else {
                None
            }
        };
        drop(reservation);
    }
}

impl Drop for WorkingMemoryReservation {
    fn drop(&mut self) {
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
