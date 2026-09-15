use crate::numeric::default_morsel_cpu_ceiling;
use skein_qos::{
    RuntimeGovernorLimits, RuntimeGovernorSnapshot, RuntimeWorkKind, RuntimeWorkPriority,
    RuntimeWorkRequest, WorkClass, WorkPriority, WorkRequest,
};

#[doc(hidden)]
pub const CONTROL_STATEMENT_MEMORY_BYTES: u64 = 1024 * 1024;

/// Execution-owned resource admission derived from a prepared query plan.
///
/// Hosts decide how to construct this from their planner, but executor owns
/// the conversion to runtime slots and bounded resource reservations.
#[doc(hidden)]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RuntimeAdmissionPlan {
    pub work_request: WorkRequest,
    pub is_mutation: bool,
    pub estimated_memory_bytes: u64,
    pub streaming_eligible: bool,
    pub required_io_slots: usize,
    pub parallel_execution_eligible: bool,
    pub max_parallelism: usize,
}

impl RuntimeAdmissionPlan {
    pub fn runtime_work_request(
        &self,
        result_budget_bytes: u64,
        limits: RuntimeGovernorLimits,
    ) -> RuntimeWorkRequest {
        self.runtime_work_request_with_capacity(
            result_budget_bytes,
            limits,
            limits.effective_cpu_slots.get(),
            limits.memory_budget_bytes,
        )
    }

    pub fn runtime_work_request_for_snapshot(
        &self,
        result_budget_bytes: u64,
        snapshot: RuntimeGovernorSnapshot,
    ) -> RuntimeWorkRequest {
        self.runtime_work_request_with_capacity(
            result_budget_bytes,
            snapshot.limits,
            snapshot
                .limits
                .effective_cpu_slots
                .get()
                .saturating_sub(snapshot.active_cpu_slots)
                .max(1),
            snapshot
                .limits
                .memory_budget_bytes
                .saturating_sub(snapshot.admitted_memory_bytes),
        )
    }

    fn runtime_work_request_with_capacity(
        &self,
        result_budget_bytes: u64,
        limits: RuntimeGovernorLimits,
        available_cpu_slots: usize,
        available_memory_bytes: u64,
    ) -> RuntimeWorkRequest {
        let priority = match self.work_request.priority {
            WorkPriority::Foreground => RuntimeWorkPriority::Foreground,
            WorkPriority::Background => RuntimeWorkPriority::Background,
        };
        if self.is_mutation {
            return RuntimeWorkRequest::mutation(priority, self.estimated_memory_bytes);
        }
        let kind = match self.work_request.class {
            WorkClass::Query | WorkClass::Mutation | WorkClass::Analytics => RuntimeWorkKind::Query,
            WorkClass::Projection | WorkClass::Import => RuntimeWorkKind::Maintenance,
            WorkClass::Shadow => RuntimeWorkKind::Control,
        };
        let cpu_slots = self.admitted_cpu_slots(
            result_budget_bytes,
            limits,
            available_cpu_slots,
            available_memory_bytes,
        );
        RuntimeWorkRequest::query(
            priority,
            self.estimated_memory_bytes
                .saturating_mul(u64::try_from(cpu_slots).unwrap_or(u64::MAX)),
            result_budget_bytes,
        )
        .with_cpu_slots(cpu_slots)
        .with_kind(kind)
        .with_io_wave_slots(self.required_io_slots)
    }

    fn admitted_cpu_slots(
        &self,
        result_budget_bytes: u64,
        limits: RuntimeGovernorLimits,
        available_cpu_slots: usize,
        available_memory_bytes: u64,
    ) -> usize {
        if !self.parallel_execution_eligible {
            return 1;
        }
        let cpu_slots = default_morsel_cpu_ceiling(limits.effective_cpu_slots.get())
            .min(self.max_parallelism)
            .min(available_cpu_slots);
        if self.estimated_memory_bytes == 0 {
            return cpu_slots.max(1);
        }
        let memory_slots = available_memory_bytes
            .saturating_sub(result_budget_bytes)
            .checked_div(self.estimated_memory_bytes)
            .and_then(|slots| usize::try_from(slots).ok())
            .unwrap_or_default();
        cpu_slots.min(memory_slots.max(1)).max(1)
    }
}

/// Returns a cheap pre-parse reservation for a source-sized control task.
///
/// This is not a measurement of allocator usage: the source length is available
/// without parsing or walking caller parameters.
#[doc(hidden)]
pub fn runtime_planning_request(
    source_bytes: usize,
    priority: RuntimeWorkPriority,
) -> RuntimeWorkRequest {
    RuntimeWorkRequest::new(priority, RuntimeWorkKind::Control)
        .with_cpu_slots(1)
        .with_memory_bytes(
            CONTROL_STATEMENT_MEMORY_BYTES
                .saturating_add(u64::try_from(source_bytes).unwrap_or(u64::MAX)),
        )
}

#[cfg(test)]
mod tests {
    use super::{runtime_planning_request, RuntimeAdmissionPlan, CONTROL_STATEMENT_MEMORY_BYTES};
    use crate::numeric::MAX_MORSEL_PARALLELISM;
    use skein_qos::{
        RuntimeGovernorLimits, RuntimeIoReservationScope, RuntimeWorkKind, RuntimeWorkPriority,
        WorkClass, WorkRequest,
    };
    use std::num::NonZeroUsize;

    fn limits(cpu_slots: usize, memory_budget_bytes: u64) -> RuntimeGovernorLimits {
        let cpu_slots = NonZeroUsize::new(cpu_slots).expect("test CPU slots are non-zero");
        RuntimeGovernorLimits {
            configured_cpu_slots: cpu_slots,
            effective_cpu_slots: cpu_slots,
            foreground_task_limit: cpu_slots,
            background_task_limit: cpu_slots,
            blocking_task_limit: cpu_slots,
            foreground_io_depth: NonZeroUsize::MIN,
            background_io_depth: NonZeroUsize::MIN,
            memory_capacity_bytes: memory_budget_bytes,
            memory_budget_bytes,
            result_budget_bytes: memory_budget_bytes,
        }
    }

    fn admission(parallel_execution_eligible: bool) -> RuntimeAdmissionPlan {
        RuntimeAdmissionPlan {
            work_request: WorkRequest::foreground(WorkClass::Query, 1),
            is_mutation: false,
            estimated_memory_bytes: 1024,
            streaming_eligible: true,
            required_io_slots: 0,
            parallel_execution_eligible,
            max_parallelism: if parallel_execution_eligible {
                MAX_MORSEL_PARALLELISM
            } else {
                1
            },
        }
    }

    #[test]
    fn default_morsel_request_uses_governed_cpu_and_memory_slots() {
        let request = admission(true).runtime_work_request(1024, limits(8, 64 * 1024));

        assert_eq!(request.cpu_slots, 4);
        assert_eq!(request.memory_bytes, 4 * 1024);
    }

    #[test]
    fn source_segment_io_uses_wave_scoped_runtime_slots() {
        let mut admission = admission(false);
        admission.required_io_slots = 2;

        let request = admission.runtime_work_request(1024, limits(8, 64 * 1024));

        assert_eq!(request.io_slots, 2);
        assert_eq!(
            request.io_reservation_scope,
            RuntimeIoReservationScope::Wave
        );
    }

    #[test]
    fn default_morsel_request_uses_executor_cpu_ceiling() {
        let request = admission(true).runtime_work_request(1024, limits(32, 1024 * 1024));
        assert_eq!(request.cpu_slots, 8);
        assert_eq!(request.memory_bytes, 8 * 1024);

        let request = admission(true).runtime_work_request(1024, limits(64, 1024 * 1024));
        assert_eq!(request.cpu_slots, 16);
        assert_eq!(request.memory_bytes, 16 * 1024);
    }

    #[test]
    fn default_morsel_request_falls_back_for_ineligible_or_tight_memory_work() {
        let serial = admission(false).runtime_work_request(1024, limits(8, 64 * 1024));
        let memory_limited = admission(true).runtime_work_request(2048, limits(8, 3072));
        let load_limited = admission(true).runtime_work_request_with_capacity(
            1024,
            limits(8, 64 * 1024),
            2,
            64 * 1024,
        );

        assert_eq!(serial.cpu_slots, 1);
        assert_eq!(memory_limited.cpu_slots, 1);
        assert_eq!(load_limited.cpu_slots, 2);
        assert_eq!(load_limited.memory_bytes, 2 * 1024);
    }

    #[test]
    fn planning_request_reserves_source_sized_control_memory() {
        let request = runtime_planning_request(512, RuntimeWorkPriority::Background);

        assert_eq!(request.priority, RuntimeWorkPriority::Background);
        assert_eq!(request.kind, RuntimeWorkKind::Control);
        assert_eq!(request.cpu_slots, 1);
        assert_eq!(request.memory_bytes, CONTROL_STATEMENT_MEMORY_BYTES + 512);

        let saturated = runtime_planning_request(usize::MAX, RuntimeWorkPriority::Foreground);
        assert_eq!(saturated.memory_bytes, u64::MAX);
    }
}
