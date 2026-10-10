// Copyright 2026 Nowledge
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Resumable maintenance retains memory while execution is parked.

use super::*;

/// Allocation ownership accounted by one maintenance controller. Lease charges
/// include their concrete permit payload. This is not a process RSS sample or
/// proof that a caller passed every allocation through the controller.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RuntimeMaintenanceMemoryReport {
    pub incremental: bool,
    pub working_ceiling_bytes: u64,
    /// Reserved mode covers allocations within this charge. Incremental mode
    /// charges allocations in addition to this fixed controller reservation.
    pub upfront_reserved_bytes: u64,
    pub live_accounted_bytes: u64,
    pub peak_accounted_bytes: u64,
}

/// One resumable background maintenance operation on a single governor.
///
/// Whole-reservation work retains its original conservative memory/process
/// charge. Incremental work retains a fixed controller charge and admits each
/// allocation separately, refunding it after the allocation is destroyed.
/// Pausing releases CPU and task capacity; physical I/O waves retain their own
/// charges until their guards drop. No second memory admission is needed to
/// resume. Callers must pause only at a cooperative execution boundary, after
/// workers have stopped using the current task context.
#[derive(Debug)]
pub struct RuntimeMaintenanceWork {
    governor: RuntimeGovernor,
    request: RuntimeWorkRequest,
    memory: Arc<working_memory::GovernorMemoryController>,
    execution: Option<RuntimePermit>,
    task: Option<RuntimeTaskContext>,
}

impl RuntimeGovernor {
    /// Admits resumable maintenance with one CPU slot and wave-scoped I/O.
    ///
    /// The supplied parent retains its cancellation, deadline and memory
    /// ceiling. Pausing cancels only the admitted child context.
    pub fn try_admit_resumable_maintenance(
        &self,
        memory_bytes: u64,
        io_slots: usize,
        parent: RuntimeTaskContext,
    ) -> Result<RuntimeMaintenanceWork, RuntimeAdmissionError> {
        self.try_admit_maintenance_memory(memory_bytes, None, io_slots, parent)
    }

    /// Admits a fixed maintenance owner and charges concrete allocations as
    /// their leases are created. `working_ceiling_bytes` limits simultaneous
    /// allocation ownership; it is a ceiling, not an upfront reservation.
    ///
    /// Every new allocation competes for the same governor/process memory as
    /// other work and sheds under critical background pressure. Leases may
    /// survive pause, resume and owner closure without keeping execution live.
    /// Callers must account all allocations through the returned context and
    /// include their shared control structures in `owner_memory_bytes`.
    pub fn try_admit_incremental_maintenance(
        &self,
        owner_memory_bytes: u64,
        working_ceiling_bytes: u64,
        io_slots: usize,
        parent: RuntimeTaskContext,
    ) -> Result<RuntimeMaintenanceWork, RuntimeAdmissionError> {
        self.try_admit_maintenance_memory(
            owner_memory_bytes,
            Some(working_ceiling_bytes),
            io_slots,
            parent,
        )
    }

    fn try_admit_maintenance_memory(
        &self,
        memory_bytes: u64,
        working_ceiling: Option<u64>,
        io_slots: usize,
        parent: RuntimeTaskContext,
    ) -> Result<RuntimeMaintenanceWork, RuntimeAdmissionError> {
        let request =
            RuntimeWorkRequest::background_maintenance(memory_bytes).with_io_wave_slots(io_slots);
        let mut execution = self.try_admit(request)?;
        // Transfer the memory owner before releasing any execution admission.
        // The initial charge remains in the governor and the host RSS policy.
        let mut memory = execution
            .memory_controller
            .take()
            .expect("background maintenance owns a working-memory controller");
        if let Some(ceiling) = working_ceiling {
            Arc::get_mut(&mut memory)
                .expect("working memory has not been bound to a task")
                .enable_incremental(ceiling);
        }
        execution.request.memory_bytes = 0;
        let mut work = RuntimeMaintenanceWork {
            governor: self.clone(),
            request,
            memory,
            execution: Some(execution),
            task: None,
        };
        work.bind(parent);
        Ok(work)
    }
}

impl RuntimeMaintenanceWork {
    pub fn memory_report(&self) -> RuntimeMaintenanceMemoryReport {
        self.memory.report()
    }

    /// Returns the current admitted context, or `None` while paused.
    pub fn task_context(&self) -> Option<&RuntimeTaskContext> {
        self.task.as_ref()
    }

    /// Parks execution without refunding memory owned by the candidate.
    ///
    /// Clones of the old context remain cancelled after a future resume.
    /// Calling this again while paused has no effect.
    pub fn pause(&mut self) {
        if let Some(task) = self.task.take() {
            task.cancellation().cancel();
        }
        self.execution.take();
    }

    /// Reacquires execution capacity on the original governor.
    ///
    /// A denial leaves this owner paused with its retained memory charges.
    /// An already running operation keeps its current context unchanged.
    pub fn try_resume(&mut self, parent: RuntimeTaskContext) -> Result<(), RuntimeAdmissionError> {
        if self.execution.is_none() {
            let request = self.request.with_memory_bytes(0);
            self.execution = Some(self.governor.try_admit(request)?);
            self.bind(parent);
        }
        Ok(())
    }

    fn bind(&mut self, parent: RuntimeTaskContext) {
        let reservation = RuntimeMemoryReservation::new(self.memory.ceiling(), 0);
        let reservation = parent
            .memory_reservation()
            .map_or(reservation, |ceiling| reservation.intersect(ceiling));
        self.task = Some(
            self.execution
                .as_ref()
                .expect("execution was admitted")
                .bind_task_context(parent.child())
                .with_memory_reservation(reservation)
                .with_memory_controller(Arc::new(working_memory::GovernorMemoryBinding(
                    self.memory.clone(),
                ))),
        );
    }
}

impl Drop for RuntimeMaintenanceWork {
    fn drop(&mut self) {
        self.pause();
        self.memory.close();
    }
}

#[cfg(test)]
mod tests;

#[cfg(test)]
mod incremental_tests;
