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

#![forbid(unsafe_code)]

use crate::{StorageDeviceProfile, StorageMediaKind};
#[cfg(any(target_os = "linux", test))]
use hawdb_cgroup::{LinuxCgroupSnapshot, LinuxCgroupValue, LinuxCgroupVersion};
use std::num::NonZeroUsize;
use sysinfo::System;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RuntimeResourceBudget {
    pub host_parallelism: NonZeroUsize,
    pub cgroup_quota_parallelism: Option<NonZeroUsize>,
    pub cpuset_parallelism: Option<NonZeroUsize>,
    pub effective_parallelism: NonZeroUsize,
    pub foreground_parallelism: NonZeroUsize,
    pub background_parallelism: NonZeroUsize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct IoConcurrencyBudget {
    pub foreground_depth: NonZeroUsize,
    pub background_depth: NonZeroUsize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum RuntimeMemoryPressure {
    Normal,
    Elevated,
    Critical,
    #[default]
    Unknown,
}

impl RuntimeMemoryPressure {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Normal => "normal",
            Self::Elevated => "elevated",
            Self::Critical => "critical",
            Self::Unknown => "unknown",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct RuntimeMemorySnapshot {
    pub host_total_bytes: Option<u64>,
    pub host_available_bytes: Option<u64>,
    pub cgroup_limit_bytes: Option<u64>,
    pub cgroup_high_bytes: Option<u64>,
    pub cgroup_current_bytes: Option<u64>,
    pub effective_limit_bytes: Option<u64>,
    pub effective_available_bytes: Option<u64>,
    pub pressure: RuntimeMemoryPressure,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RuntimeResourceSnapshot {
    pub cpu: RuntimeResourceBudget,
    pub memory: RuntimeMemorySnapshot,
}

#[derive(Debug)]
pub(crate) struct RuntimeResourceDetector {
    system: System,
}

impl RuntimeResourceDetector {
    pub(crate) fn new() -> Self {
        Self {
            system: System::new(),
        }
    }

    pub(crate) fn detect(&mut self) -> RuntimeResourceSnapshot {
        let host_parallelism = std::thread::available_parallelism().unwrap_or(NonZeroUsize::MIN);
        self.system.refresh_memory();
        let (host_total_bytes, host_available_bytes) =
            host_memory_values(self.system.total_memory(), self.system.available_memory());

        #[cfg(target_os = "linux")]
        {
            let cgroup = LinuxCgroupSnapshot::detect();
            resource_snapshot_from_cgroup(
                host_parallelism,
                host_total_bytes,
                host_available_bytes,
                &cgroup,
            )
        }

        #[cfg(not(target_os = "linux"))]
        RuntimeResourceSnapshot::from_parts(
            RuntimeResourceBudget::from_limits(host_parallelism, None, None),
            RuntimeMemorySnapshot::from_limits(
                host_total_bytes,
                host_available_bytes,
                None,
                None,
                None,
            ),
        )
    }
}

impl RuntimeResourceBudget {
    pub fn detect() -> Self {
        let host = std::thread::available_parallelism().unwrap_or(NonZeroUsize::MIN);
        #[cfg(target_os = "linux")]
        let (quota, cpuset) = linux_cgroup_cpu_limits();
        #[cfg(not(target_os = "linux"))]
        let (quota, cpuset) = (None, None);
        Self::from_limits(host, quota, cpuset)
    }

    pub fn from_limits(
        host_parallelism: NonZeroUsize,
        cgroup_quota_parallelism: Option<NonZeroUsize>,
        cpuset_parallelism: Option<NonZeroUsize>,
    ) -> Self {
        let effective = [
            Some(host_parallelism),
            cgroup_quota_parallelism,
            cpuset_parallelism,
        ]
        .into_iter()
        .flatten()
        .min()
        .unwrap_or(NonZeroUsize::MIN);
        let background = NonZeroUsize::new((effective.get() / 4).max(1))
            .expect("background parallelism is always non-zero");
        Self {
            host_parallelism,
            cgroup_quota_parallelism,
            cpuset_parallelism,
            effective_parallelism: effective,
            foreground_parallelism: effective,
            background_parallelism: background,
        }
    }
}

impl RuntimeMemorySnapshot {
    pub fn detect() -> Self {
        let mut system = System::new();
        system.refresh_memory();
        let (host_total_bytes, host_available_bytes) =
            host_memory_values(system.total_memory(), system.available_memory());

        #[cfg(target_os = "linux")]
        let (cgroup_limit_bytes, cgroup_high_bytes, cgroup_current_bytes) =
            linux_cgroup_memory_limits();
        #[cfg(not(target_os = "linux"))]
        let (cgroup_limit_bytes, cgroup_high_bytes, cgroup_current_bytes) = (None, None, None);

        Self::from_limits(
            host_total_bytes,
            host_available_bytes,
            cgroup_limit_bytes,
            cgroup_high_bytes,
            cgroup_current_bytes,
        )
    }

    pub fn from_limits(
        host_total_bytes: Option<u64>,
        host_available_bytes: Option<u64>,
        cgroup_limit_bytes: Option<u64>,
        cgroup_high_bytes: Option<u64>,
        cgroup_current_bytes: Option<u64>,
    ) -> Self {
        let cgroup_effective_limit_bytes = [cgroup_limit_bytes, cgroup_high_bytes]
            .into_iter()
            .flatten()
            .min();
        let effective_limit_bytes = [host_total_bytes, cgroup_effective_limit_bytes]
            .into_iter()
            .flatten()
            .min();
        let cgroup_available_bytes = cgroup_effective_limit_bytes
            .zip(cgroup_current_bytes)
            .map(|(limit, current)| limit.saturating_sub(current));
        let effective_available_bytes = [host_available_bytes, cgroup_available_bytes]
            .into_iter()
            .flatten()
            .min();
        let pressure = memory_pressure(
            effective_limit_bytes,
            effective_available_bytes,
            cgroup_high_bytes,
            cgroup_current_bytes,
        );
        Self {
            host_total_bytes,
            host_available_bytes,
            cgroup_limit_bytes,
            cgroup_high_bytes,
            cgroup_current_bytes,
            effective_limit_bytes,
            effective_available_bytes,
            pressure,
        }
    }
}

impl RuntimeResourceSnapshot {
    pub fn detect() -> Self {
        RuntimeResourceDetector::new().detect()
    }

    pub const fn from_parts(cpu: RuntimeResourceBudget, memory: RuntimeMemorySnapshot) -> Self {
        Self { cpu, memory }
    }
}

#[cfg(any(target_os = "linux", test))]
fn resource_snapshot_from_cgroup(
    host_parallelism: NonZeroUsize,
    host_total_bytes: Option<u64>,
    host_available_bytes: Option<u64>,
    cgroup: &LinuxCgroupSnapshot,
) -> RuntimeResourceSnapshot {
    let (cgroup_quota_parallelism, cpuset_parallelism) = cgroup_cpu_limits_from(cgroup);
    let (cgroup_limit_bytes, cgroup_high_bytes, cgroup_current_bytes) =
        cgroup_memory_limits_from(cgroup);
    RuntimeResourceSnapshot::from_parts(
        RuntimeResourceBudget::from_limits(
            host_parallelism,
            cgroup_quota_parallelism,
            cpuset_parallelism,
        ),
        RuntimeMemorySnapshot::from_limits(
            host_total_bytes,
            host_available_bytes,
            cgroup_limit_bytes,
            cgroup_high_bytes,
            cgroup_current_bytes,
        ),
    )
}

impl IoConcurrencyBudget {
    pub fn shared_host() -> Self {
        Self::shared_host_for_device(StorageDeviceProfile::default())
    }

    pub fn shared_host_for_device(device: StorageDeviceProfile) -> Self {
        let foreground = match device.media_kind {
            StorageMediaKind::Rotational => 2,
            StorageMediaKind::NonRotational => device
                .queue_depth_hint
                .map(NonZeroUsize::get)
                .unwrap_or(8)
                .clamp(4, 32),
            StorageMediaKind::Memory => 8,
            StorageMediaKind::Network | StorageMediaKind::Virtual | StorageMediaKind::Unknown => 4,
        };
        let background = (foreground / 4).clamp(1, 4);
        Self::new(foreground, background)
    }

    pub fn mobile_embedded() -> Self {
        Self::mobile_embedded_for_device(StorageDeviceProfile::default())
    }

    pub fn mobile_embedded_for_device(device: StorageDeviceProfile) -> Self {
        let foreground = match device.media_kind {
            StorageMediaKind::Rotational
            | StorageMediaKind::Network
            | StorageMediaKind::Virtual => 1,
            StorageMediaKind::NonRotational | StorageMediaKind::Memory => device
                .queue_depth_hint
                .map(NonZeroUsize::get)
                .unwrap_or(4)
                .clamp(1, 4),
            StorageMediaKind::Unknown => 2,
        };
        Self::new(foreground, 1)
    }

    pub fn new(foreground_depth: usize, background_depth: usize) -> Self {
        Self {
            foreground_depth: NonZeroUsize::new(foreground_depth.max(1))
                .expect("foreground I/O depth is always non-zero"),
            background_depth: NonZeroUsize::new(background_depth.max(1))
                .expect("background I/O depth is always non-zero"),
        }
    }
}

#[cfg(target_os = "linux")]
fn linux_cgroup_cpu_limits() -> (Option<NonZeroUsize>, Option<NonZeroUsize>) {
    cgroup_cpu_limits_from(&LinuxCgroupSnapshot::detect())
}

#[cfg(any(target_os = "linux", test))]
fn cgroup_cpu_limits_from(
    snapshot: &LinuxCgroupSnapshot,
) -> (Option<NonZeroUsize>, Option<NonZeroUsize>) {
    if !cgroup_limits_are_sensed(snapshot.version) {
        return (None, None);
    }
    (
        admitted_cpu_limit(snapshot.cpu_quota_parallelism),
        admitted_cpu_limit(snapshot.cpuset_parallelism),
    )
}

/// Whether the sensed cgroup hierarchy carries limits this crate reads.
///
/// Cgroup v1 and hybrid hierarchies are deliberately not parsed. Their
/// `V1Unsupported` marker means "limits may exist but HawDB does not read
/// them", not "sensing failed": treating it as a failure would derive a
/// zero memory capacity and permanently reject every query on such hosts.
/// Host totals govern instead, and any v1-enforced limit remains the
/// kernel's to enforce. `Unknown` (unreadable procfs, unresolvable v2
/// mount) and per-file `Invalid` values stay fail-closed.
#[cfg(any(target_os = "linux", test))]
const fn cgroup_limits_are_sensed(version: LinuxCgroupVersion) -> bool {
    !matches!(version, LinuxCgroupVersion::V1Unsupported)
}

#[cfg(any(target_os = "linux", test))]
fn admitted_cpu_limit(value: LinuxCgroupValue<NonZeroUsize>) -> Option<NonZeroUsize> {
    match value {
        LinuxCgroupValue::Value(value) => Some(value),
        LinuxCgroupValue::Invalid => Some(NonZeroUsize::MIN),
        LinuxCgroupValue::Absent | LinuxCgroupValue::Unlimited => None,
    }
}

#[cfg(target_os = "linux")]
fn linux_cgroup_memory_limits() -> (Option<u64>, Option<u64>, Option<u64>) {
    cgroup_memory_limits_from(&LinuxCgroupSnapshot::detect())
}

#[cfg(any(target_os = "linux", test))]
fn cgroup_memory_limits_from(
    snapshot: &LinuxCgroupSnapshot,
) -> (Option<u64>, Option<u64>, Option<u64>) {
    if !cgroup_limits_are_sensed(snapshot.version) {
        return (None, None, None);
    }
    (
        admitted_memory_limit(snapshot.memory_limit_bytes),
        admitted_memory_limit(snapshot.memory_high_bytes),
        admitted_memory_current(snapshot.memory_current_bytes),
    )
}

#[cfg(any(target_os = "linux", test))]
fn admitted_memory_limit(value: LinuxCgroupValue<u64>) -> Option<u64> {
    match value {
        LinuxCgroupValue::Value(value) => Some(value),
        LinuxCgroupValue::Invalid => Some(0),
        LinuxCgroupValue::Absent | LinuxCgroupValue::Unlimited => None,
    }
}

#[cfg(any(target_os = "linux", test))]
fn admitted_memory_current(value: LinuxCgroupValue<u64>) -> Option<u64> {
    match value {
        LinuxCgroupValue::Value(value) => Some(value),
        LinuxCgroupValue::Invalid => Some(u64::MAX),
        LinuxCgroupValue::Absent | LinuxCgroupValue::Unlimited => None,
    }
}

fn host_memory_values(total_bytes: u64, available_bytes: u64) -> (Option<u64>, Option<u64>) {
    // A detected total makes zero availability meaningful. Only two zero
    // readings retain sysinfo's unavailable/unsupported fallback.
    (
        (total_bytes > 0).then_some(total_bytes),
        (total_bytes > 0 || available_bytes > 0).then_some(available_bytes),
    )
}

fn memory_pressure(
    effective_limit_bytes: Option<u64>,
    effective_available_bytes: Option<u64>,
    cgroup_high_bytes: Option<u64>,
    cgroup_current_bytes: Option<u64>,
) -> RuntimeMemoryPressure {
    if cgroup_high_bytes
        .zip(cgroup_current_bytes)
        .is_some_and(|(high, current)| current >= high)
    {
        return RuntimeMemoryPressure::Critical;
    }
    let Some((limit, available)) = effective_limit_bytes.zip(effective_available_bytes) else {
        return RuntimeMemoryPressure::Unknown;
    };
    if limit == 0 || u128::from(available) * 100 <= u128::from(limit) * 10 {
        RuntimeMemoryPressure::Critical
    } else if u128::from(available) * 100 <= u128::from(limit) * 25 {
        RuntimeMemoryPressure::Elevated
    } else {
        RuntimeMemoryPressure::Normal
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        RuntimeAdmissionCode, RuntimeGovernor, RuntimeGovernorConfig, RuntimeWorkPriority,
        RuntimeWorkRequest,
    };

    #[test]
    fn host_memory_values_distinguish_exhausted_from_unknown() {
        for (total, available, expected) in [
            (0, 0, (None, None)),
            (0, 1, (None, Some(1))),
            (1, 0, (Some(1), Some(0))),
            (1, 1, (Some(1), Some(1))),
            (u64::MAX, 0, (Some(u64::MAX), Some(0))),
            (u64::MAX, u64::MAX, (Some(u64::MAX), Some(u64::MAX))),
        ] {
            assert_eq!(host_memory_values(total, available), expected);
        }
    }

    fn sampled_host_resources(total: u64, available: u64) -> RuntimeResourceSnapshot {
        let (total, available) = host_memory_values(total, available);
        RuntimeResourceSnapshot::from_parts(
            RuntimeResourceBudget::from_limits(NonZeroUsize::new(8).unwrap(), None, None),
            RuntimeMemorySnapshot::from_limits(total, available, None, None, None),
        )
    }

    #[test]
    fn zero_host_headroom_gates_real_governor_admission() {
        let governor = RuntimeGovernor::new(
            RuntimeGovernorConfig::shared_host(),
            sampled_host_resources(8 << 30, 0),
            IoConcurrencyBudget::new(4, 1),
        );
        let error = governor
            .try_admit(RuntimeWorkRequest::blocking_cpu(
                RuntimeWorkPriority::Foreground,
                1,
            ))
            .unwrap_err();
        assert_eq!(error.code, RuntimeAdmissionCode::MemorySaturated);
        assert_eq!(error.available, 0);
        assert!(error.is_retryable());
        let snapshot = governor.snapshot();
        assert_eq!(
            snapshot.resources.memory.pressure,
            RuntimeMemoryPressure::Critical
        );
        assert_eq!(snapshot.resources.memory.effective_available_bytes, Some(0));
        assert_eq!(snapshot.limits.memory_capacity_bytes, 2 << 30);
        assert_eq!(snapshot.limits.memory_budget_bytes, 0);
        assert_eq!(snapshot.admitted_memory_bytes, 0);
    }

    #[test]
    fn host_headroom_and_cgroup_limits_compose_at_admission() {
        let cgroup = LinuxCgroupSnapshot {
            version: LinuxCgroupVersion::V2,
            memory_limit_bytes: LinuxCgroupValue::Value(4 << 30),
            memory_high_bytes: LinuxCgroupValue::Value(3 << 30),
            memory_current_bytes: LinuxCgroupValue::Value(2 << 30),
            ..LinuxCgroupSnapshot::host()
        };
        let cases = [
            (LinuxCgroupSnapshot::host(), None, 2 << 30),
            (
                LinuxCgroupSnapshot::fail_closed(LinuxCgroupVersion::V1Unsupported),
                None,
                2 << 30,
            ),
            (cgroup, Some(1 << 30), 768 << 20),
            (
                LinuxCgroupSnapshot {
                    memory_current_bytes: LinuxCgroupValue::Value(3 << 30),
                    ..cgroup
                },
                Some(0),
                768 << 20,
            ),
            (
                LinuxCgroupSnapshot {
                    memory_limit_bytes: LinuxCgroupValue::Unlimited,
                    memory_high_bytes: LinuxCgroupValue::Unlimited,
                    ..cgroup
                },
                None,
                2 << 30,
            ),
            (
                LinuxCgroupSnapshot::fail_closed(LinuxCgroupVersion::Unknown),
                Some(0),
                0,
            ),
            (
                LinuxCgroupSnapshot {
                    memory_current_bytes: LinuxCgroupValue::Invalid,
                    ..cgroup
                },
                Some(0),
                768 << 20,
            ),
        ];
        for (cgroup, cgroup_available, capacity) in cases {
            for available in [0, 4, 4 << 30] {
                let (host_total, host_available) = host_memory_values(8 << 30, available);
                let resources = resource_snapshot_from_cgroup(
                    NonZeroUsize::new(8).unwrap(),
                    host_total,
                    host_available,
                    &cgroup,
                );
                let expected_available = available.min(cgroup_available.unwrap_or(available));
                let budget = expected_available / 4;
                assert_eq!(
                    resources.memory.effective_available_bytes,
                    Some(expected_available)
                );
                let governor = RuntimeGovernor::new(
                    RuntimeGovernorConfig::shared_host(),
                    resources,
                    IoConcurrencyBudget::new(4, 1),
                );
                assert_eq!(governor.snapshot().limits.memory_capacity_bytes, capacity);
                assert_eq!(governor.snapshot().limits.memory_budget_bytes, budget);
                if budget > 0 {
                    let permit = governor
                        .try_admit(RuntimeWorkRequest::blocking_cpu(
                            RuntimeWorkPriority::Foreground,
                            budget,
                        ))
                        .unwrap();
                    drop(permit);
                } else {
                    assert_eq!(resources.memory.pressure, RuntimeMemoryPressure::Critical);
                    let error = governor
                        .try_admit(RuntimeWorkRequest::blocking_cpu(
                            RuntimeWorkPriority::Background,
                            0,
                        ))
                        .unwrap_err();
                    assert_eq!(error.code, RuntimeAdmissionCode::MemoryPressure);
                    assert!(error.is_retryable());
                }
                for requested in [budget + 1, capacity + 1] {
                    let error = governor
                        .try_admit(RuntimeWorkRequest::blocking_cpu(
                            RuntimeWorkPriority::Foreground,
                            requested,
                        ))
                        .unwrap_err();
                    assert_eq!(error.code, RuntimeAdmissionCode::MemorySaturated);
                    assert_eq!(error.is_retryable(), requested <= capacity);
                }
                assert_eq!(governor.snapshot().admitted_memory_bytes, 0);
            }
        }
    }

    #[test]
    fn unknown_host_readings_retain_the_fallback_budget() {
        let config = RuntimeGovernorConfig::shared_host();
        let resources = sampled_host_resources(0, 0);
        assert_eq!(resources.memory, RuntimeMemorySnapshot::default());
        let governor = RuntimeGovernor::new(config, resources, IoConcurrencyBudget::new(4, 1));
        assert_eq!(
            governor.snapshot().limits.memory_budget_bytes,
            config.fallback_memory_budget_bytes
        );
        let permit = governor
            .try_admit(RuntimeWorkRequest::blocking_cpu(
                RuntimeWorkPriority::Foreground,
                1,
            ))
            .unwrap();
        drop(permit);
    }

    #[test]
    fn host_headroom_refresh_preserves_permits_and_recovers_admission() {
        let healthy = sampled_host_resources(8 << 30, 4 << 30);
        let exhausted = sampled_host_resources(8 << 30, 0);
        let governor = RuntimeGovernor::new(
            RuntimeGovernorConfig::shared_host(),
            healthy,
            IoConcurrencyBudget::new(4, 1),
        );
        let request = RuntimeWorkRequest::blocking_cpu(RuntimeWorkPriority::Foreground, 128 << 20);
        let permit = governor.try_admit(request).unwrap();
        assert!(governor.update_resources(exhausted));
        let snapshot = governor.snapshot();
        assert_eq!(snapshot.admitted_memory_bytes, request.memory_bytes);
        assert_eq!(snapshot.limits.memory_budget_bytes, request.memory_bytes);
        assert_eq!(snapshot.limits.memory_capacity_bytes, 2 << 30);
        assert_eq!(snapshot.active_foreground_tasks, 1);
        let error = governor.try_admit(request).unwrap_err();
        assert_eq!(error.code, RuntimeAdmissionCode::MemorySaturated);
        assert!(error.is_retryable());

        drop(permit);
        assert!(governor.update_resources(exhausted));
        assert!(!governor.update_resources(exhausted));
        assert_eq!(governor.snapshot().admitted_memory_bytes, 0);
        assert_eq!(governor.snapshot().limits.memory_budget_bytes, 0);
        assert!(governor.try_admit(request).unwrap_err().is_retryable());

        assert!(governor.update_resources(healthy));
        assert_eq!(governor.snapshot().limits.memory_budget_bytes, 1 << 30);
        let permit = governor.try_admit(request).unwrap();
        drop(permit);
        assert_eq!(governor.snapshot().active_foreground_tasks, 0);
    }

    #[test]
    fn effective_budget_uses_the_smallest_cpu_limit() {
        let budget = RuntimeResourceBudget::from_limits(
            NonZeroUsize::new(16).unwrap(),
            NonZeroUsize::new(6),
            NonZeroUsize::new(4),
        );
        assert_eq!(budget.effective_parallelism.get(), 4);
        assert_eq!(budget.foreground_parallelism.get(), 4);
        assert_eq!(budget.background_parallelism.get(), 1);
    }

    #[test]
    fn v1_cgroup_snapshot_falls_back_to_host_limits() {
        let snapshot = LinuxCgroupSnapshot::fail_closed(LinuxCgroupVersion::V1Unsupported);
        assert_eq!(cgroup_cpu_limits_from(&snapshot), (None, None));
        assert_eq!(cgroup_memory_limits_from(&snapshot), (None, None, None));

        let memory =
            RuntimeMemorySnapshot::from_limits(Some(16 << 30), Some(8 << 30), None, None, None);
        assert_eq!(memory.effective_limit_bytes, Some(16 << 30));
        assert_eq!(memory.effective_available_bytes, Some(8 << 30));
    }

    #[test]
    fn unknown_cgroup_snapshot_remains_fail_closed() {
        let snapshot = LinuxCgroupSnapshot::fail_closed(LinuxCgroupVersion::Unknown);
        assert_eq!(
            cgroup_cpu_limits_from(&snapshot),
            (Some(NonZeroUsize::MIN), Some(NonZeroUsize::MIN))
        );
        assert_eq!(
            cgroup_memory_limits_from(&snapshot),
            (Some(0), Some(0), Some(u64::MAX))
        );
    }

    #[test]
    fn invalid_cgroup_values_fail_closed_at_admission_boundary() {
        assert_eq!(
            admitted_cpu_limit(LinuxCgroupValue::Invalid),
            Some(NonZeroUsize::MIN)
        );
        assert_eq!(admitted_memory_limit(LinuxCgroupValue::Invalid), Some(0));
        assert_eq!(
            admitted_memory_current(LinuxCgroupValue::Invalid),
            Some(u64::MAX)
        );

        let memory = RuntimeMemorySnapshot::from_limits(
            Some(16 << 30),
            Some(8 << 30),
            admitted_memory_limit(LinuxCgroupValue::Invalid),
            None,
            admitted_memory_current(LinuxCgroupValue::Invalid),
        );
        assert_eq!(memory.effective_limit_bytes, Some(0));
        assert_eq!(memory.effective_available_bytes, Some(0));
        assert_eq!(memory.pressure, RuntimeMemoryPressure::Critical);
    }

    #[test]
    fn resource_snapshot_derives_cpu_and_memory_from_one_cgroup_sample() {
        let cgroup = LinuxCgroupSnapshot {
            version: LinuxCgroupVersion::V2,
            cpu_quota_parallelism: LinuxCgroupValue::Value(NonZeroUsize::new(6).unwrap()),
            cpuset_parallelism: LinuxCgroupValue::Value(NonZeroUsize::new(4).unwrap()),
            memory_limit_bytes: LinuxCgroupValue::Value(4 << 30),
            memory_high_bytes: LinuxCgroupValue::Value(3 << 30),
            memory_current_bytes: LinuxCgroupValue::Value(2 << 30),
        };
        let snapshot = resource_snapshot_from_cgroup(
            NonZeroUsize::new(16).unwrap(),
            Some(16 << 30),
            Some(8 << 30),
            &cgroup,
        );

        assert_eq!(snapshot.cpu.cgroup_quota_parallelism.unwrap().get(), 6);
        assert_eq!(snapshot.cpu.cpuset_parallelism.unwrap().get(), 4);
        assert_eq!(snapshot.cpu.effective_parallelism.get(), 4);
        assert_eq!(snapshot.memory.cgroup_limit_bytes, Some(4 << 30));
        assert_eq!(snapshot.memory.cgroup_high_bytes, Some(3 << 30));
        assert_eq!(snapshot.memory.cgroup_current_bytes, Some(2 << 30));
        assert_eq!(snapshot.memory.effective_limit_bytes, Some(3 << 30));
        assert_eq!(snapshot.memory.effective_available_bytes, Some(1 << 30));
    }

    #[test]
    fn memory_snapshot_uses_cgroup_headroom_and_high_watermark() {
        let snapshot = RuntimeMemorySnapshot::from_limits(
            Some(16 << 30),
            Some(8 << 30),
            Some(4 << 30),
            Some(3 << 30),
            Some(2 << 30),
        );
        assert_eq!(snapshot.effective_limit_bytes, Some(3 << 30));
        assert_eq!(snapshot.effective_available_bytes, Some(1 << 30));
        assert_eq!(snapshot.pressure, RuntimeMemoryPressure::Normal);

        let pressured = RuntimeMemorySnapshot::from_limits(
            Some(16 << 30),
            Some(8 << 30),
            Some(4 << 30),
            Some(3 << 30),
            Some(3 << 30),
        );
        assert_eq!(pressured.pressure, RuntimeMemoryPressure::Critical);
    }

    #[test]
    fn platform_memory_detection_has_internally_consistent_limits() {
        let snapshot = RuntimeMemorySnapshot::detect();
        if let (Some(limit), Some(available)) = (
            snapshot.effective_limit_bytes,
            snapshot.effective_available_bytes,
        ) {
            assert!(available <= limit);
        }
    }

    #[test]
    fn shared_host_io_budget_uses_device_evidence_instead_of_cpu_count() {
        let device = StorageDeviceProfile::host_provided(
            StorageMediaKind::NonRotational,
            NonZeroUsize::new(12),
        );
        let io = IoConcurrencyBudget::shared_host_for_device(device);
        assert_eq!(io.foreground_depth.get(), 12);
        assert_eq!(io.background_depth.get(), 3);

        let default_ssd =
            StorageDeviceProfile::host_provided(StorageMediaKind::NonRotational, None);
        let io = IoConcurrencyBudget::shared_host_for_device(default_ssd);
        assert_eq!(io.foreground_depth.get(), 8);
        assert_eq!(io.background_depth.get(), 2);
    }

    #[test]
    fn mobile_io_budget_stays_conservative() {
        let device = StorageDeviceProfile::host_provided(
            StorageMediaKind::NonRotational,
            NonZeroUsize::new(32),
        );
        let io = IoConcurrencyBudget::mobile_embedded_for_device(device);
        assert_eq!(io.foreground_depth.get(), 4);
        assert_eq!(io.background_depth.get(), 1);
    }

    #[test]
    fn unknown_device_budget_uses_conservative_defaults() {
        assert_eq!(
            IoConcurrencyBudget::shared_host(),
            IoConcurrencyBudget::shared_host_for_device(StorageDeviceProfile::default())
        );
        assert_eq!(
            IoConcurrencyBudget::mobile_embedded(),
            IoConcurrencyBudget::mobile_embedded_for_device(StorageDeviceProfile::default())
        );
    }
}
