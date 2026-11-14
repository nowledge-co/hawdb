// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

//! Cooperative work-area feedback; hard reservations remain owned by permits.

use super::*;
use hawdb_core::RuntimeMemoryTarget;
use std::sync::Weak;

const RECOVERY_DELAY: Duration = Duration::from_millis(250);

#[derive(Debug)]
pub(super) struct GovernorMemoryTarget {
    governor: Weak<RuntimeGovernorInner>,
    recovery: Mutex<Recovery>,
}

#[derive(Debug, Default)]
struct Recovery {
    shift: u32,
    relief_since: Option<Instant>,
}

impl Recovery {
    fn observe(&mut self, pressure: u32, now: Instant) -> u32 {
        if pressure >= self.shift {
            self.shift = pressure;
            self.relief_since = None;
        } else {
            let since = *self.relief_since.get_or_insert(now);
            if now.saturating_duration_since(since) >= RECOVERY_DELAY {
                self.shift = pressure;
                self.relief_since = None;
            }
        }
        self.shift
    }
}

impl GovernorMemoryTarget {
    pub(super) fn new(governor: &Arc<RuntimeGovernorInner>) -> Self {
        Self {
            governor: Arc::downgrade(governor),
            recovery: Mutex::new(Recovery::default()),
        }
    }
}

impl RuntimeMemoryTarget for GovernorMemoryTarget {
    fn target_bytes(&self, hard_limit: u64) -> u64 {
        let Some(governor) = self.governor.upgrade() else {
            return hard_limit;
        };
        let (pressure, policy) = {
            let state = mutex_lock(&governor.state);
            (
                state.resources.memory.pressure,
                state.process_memory_policy.clone(),
            )
        };
        let mut shift = match pressure {
            RuntimeMemoryPressure::Critical => 2,
            RuntimeMemoryPressure::Elevated => 1,
            RuntimeMemoryPressure::Normal | RuntimeMemoryPressure::Unknown => 0,
        };
        if let Some(policy) = policy {
            let snapshot = policy.snapshot();
            // Admission also pauses for stale/missing samples. Distinguish
            // unavailable feedback from a current sample reporting pressure.
            shift = shift.max(if !snapshot.sample_is_current {
                1
            } else if snapshot.admission_paused {
                2
            } else {
                0
            });
        }
        let shift = mutex_lock(&self.recovery).observe(shift, Instant::now());
        hard_limit >> shift
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pressure_reduces_immediately_but_relief_requires_a_quiet_interval() {
        let mut recovery = Recovery::default();
        let now = Instant::now();
        assert_eq!(recovery.observe(2, now), 2);
        assert_eq!(recovery.observe(0, now), 2);
        assert_eq!(recovery.observe(0, now + RECOVERY_DELAY / 2), 2);
        assert_eq!(recovery.observe(2, now + RECOVERY_DELAY), 2);
        assert_eq!(recovery.observe(0, now + RECOVERY_DELAY), 2);
        assert_eq!(recovery.observe(0, now + RECOVERY_DELAY * 2), 0);
    }

    #[test]
    fn unavailable_process_sample_halves_active_work_areas() {
        let policy = ProcessMemoryPolicy::new(crate::ProcessMemoryPolicyConfig::new(
            std::num::NonZeroU64::new(16 * 1024 * 1024).unwrap(),
        ));
        policy.update(crate::ProcessMemorySnapshot {
            capabilities: crate::ProcessMemoryCapabilities {
                resident_memory: true,
                ..Default::default()
            },
            resident_bytes: 0,
            peak_resident_bytes: 0,
            total_page_faults: None,
            minor_page_faults: None,
            major_page_faults: None,
        });
        let resources = RuntimeResourceSnapshot::from_parts(
            crate::RuntimeResourceBudget::from_limits(NonZeroUsize::new(4).unwrap(), None, None),
            crate::RuntimeMemorySnapshot::from_limits(
                Some(1 << 30),
                Some(1 << 30),
                None,
                None,
                None,
            ),
        );
        let governor = RuntimeGovernor::new_with_process_memory_policy(
            RuntimeGovernorConfig::shared_host(),
            resources,
            IoConcurrencyBudget::new(4, 1),
            policy.clone(),
        );
        let permit = governor
            .try_admit(RuntimeWorkRequest::foreground_query(256 * 1024, 0))
            .unwrap();
        let task = permit.bind_task_context(RuntimeTaskContext::default());
        assert_eq!(task.working_memory_target(4096), 4096);
        policy.clear_sample();
        assert!(policy.snapshot().admission_paused);
        assert!(!policy.snapshot().sample_is_current);
        assert_eq!(task.working_memory_target(4096), 2048);
        assert_eq!(governor.snapshot().admitted_memory_bytes, 256 * 1024);
        assert!(governor
            .try_admit(RuntimeWorkRequest::foreground_query(1, 0))
            .is_err());
        // Actual critical resource pressure still wins over missing feedback.
        let mut critical = resources;
        critical.memory.pressure = RuntimeMemoryPressure::Critical;
        governor.update_resources(critical);
        assert_eq!(task.working_memory_target(4096), 1024);
    }

    #[test]
    fn admitted_queries_and_children_observe_live_pressure_without_revoking_grants() {
        let governor = RuntimeGovernor::new(
            RuntimeGovernorConfig {
                memory_budget_bytes: Some(1024 * 1024),
                ..RuntimeGovernorConfig::shared_host()
            },
            RuntimeResourceSnapshot::from_parts(
                crate::RuntimeResourceBudget::from_limits(
                    NonZeroUsize::new(4).unwrap(),
                    None,
                    None,
                ),
                crate::RuntimeMemorySnapshot::from_limits(
                    Some(1 << 30),
                    Some(1 << 30),
                    None,
                    None,
                    None,
                ),
            ),
            IoConcurrencyBudget::new(4, 1),
        );
        let permit = governor
            .try_admit(RuntimeWorkRequest::foreground_query(256 * 1024, 0))
            .unwrap();
        let background = governor
            .try_admit(RuntimeWorkRequest::background_maintenance(256 * 1024))
            .unwrap();
        let task = permit.bind_task_context(RuntimeTaskContext::default());
        let child = task.child();
        let maintenance = background.bind_task_context(RuntimeTaskContext::default());
        assert_eq!(task.working_memory_target(4096), 4096);
        mutex_lock(&governor.inner.state).resources.memory.pressure =
            RuntimeMemoryPressure::Critical;
        assert_eq!(task.working_memory_target(4096), 1024);
        assert_eq!(child.working_memory_target(4096), 1024);
        assert_eq!(maintenance.working_memory_target(4096), 1024);
        assert_eq!(governor.snapshot().admitted_memory_bytes, 512 * 1024);
        assert_eq!(
            child.memory_reservation().unwrap().memory_bytes(),
            256 * 1024
        );
        assert_eq!(
            task.constrained_by(&RuntimeTaskContext::default())
                .unwrap()
                .working_memory_target(4096),
            1024
        );
        mutex_lock(&governor.inner.state).resources.memory.pressure = RuntimeMemoryPressure::Normal;
        assert_eq!(child.working_memory_target(4096), 1024);
        drop(background);
        drop(permit);
        assert_eq!(governor.snapshot().admitted_memory_bytes, 0);
    }
}
