// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

//! Admission keeps a host-detected governor's snapshot fresh (#1879).

use super::*;
use crate::resource::ScriptedHostResources;
use crate::{RuntimeMemorySnapshot, RuntimeResourceBudget};

const GIB: u64 = 1024 * 1024 * 1024;

fn host(available: u64) -> RuntimeResourceSnapshot {
    RuntimeResourceSnapshot::from_parts(
        RuntimeResourceBudget::from_limits(NonZeroUsize::new(4).unwrap(), None, None),
        RuntimeMemorySnapshot::from_limits(Some(8 * GIB), Some(available), None, None, None),
    )
}

fn healthy() -> RuntimeResourceSnapshot {
    host(6 * GIB)
}

fn critical() -> RuntimeResourceSnapshot {
    let critical = host(0);
    assert_eq!(critical.memory.pressure, RuntimeMemoryPressure::Critical);
    critical
}

fn detected(host: &ScriptedHostResources, refresh_interval: Duration) -> RuntimeGovernor {
    RuntimeGovernor::detect_inner(
        RuntimeGovernorConfig::shared_host(),
        IoConcurrencyBudget::new(4, 1),
        None,
        host.detector(),
        refresh_interval,
    )
}

fn admit_background(governor: &RuntimeGovernor) -> Result<RuntimePermit, RuntimeAdmissionError> {
    governor.try_admit(RuntimeWorkRequest::background_maintenance(1))
}

fn assert_memory_pressure(result: Result<RuntimePermit, RuntimeAdmissionError>) {
    let error = result.expect_err("critical host memory must refuse Background work");
    assert_eq!(error.code, RuntimeAdmissionCode::MemoryPressure);
    assert!(error.is_retryable());
}

#[test]
fn admission_sees_host_recovery_from_critical_pressure() {
    let host = ScriptedHostResources::new(critical());
    let governor = detected(&host, Duration::ZERO);
    assert_memory_pressure(admit_background(&governor));

    host.set(healthy());
    drop(admit_background(&governor).expect("recovered host must admit the unchanged retry"));
    assert_eq!(
        governor.snapshot().resources.memory.pressure,
        RuntimeMemoryPressure::Normal
    );
    assert_eq!(host.samples(), 3);
}

#[test]
fn admission_sees_onset_of_critical_pressure() {
    let host = ScriptedHostResources::new(healthy());
    let governor = detected(&host, Duration::ZERO);
    drop(admit_background(&governor).unwrap());

    host.set(critical());
    assert_memory_pressure(admit_background(&governor));
    assert_eq!(
        governor.snapshot().resources.memory.pressure,
        RuntimeMemoryPressure::Critical
    );
}

#[test]
fn admission_samples_the_host_at_most_once_per_interval() {
    let host = ScriptedHostResources::new(critical());
    let governor = detected(&host, Duration::from_secs(3600));
    assert_eq!(host.samples(), 1, "detection samples the host once");

    host.set(healthy());
    for _ in 0..32 {
        assert_memory_pressure(admit_background(&governor));
    }
    assert_eq!(
        host.samples(),
        1,
        "admission inside the interval must not sample"
    );

    // An explicit refresh still samples immediately.
    assert!(governor.refresh_from_host());
    assert_eq!(host.samples(), 2);
    drop(admit_background(&governor).unwrap());
    assert_eq!(host.samples(), 2);
}

#[test]
fn production_detection_uses_the_tokio_adapter_default_interval() {
    let governor = RuntimeGovernor::detect(
        RuntimeGovernorConfig::shared_host(),
        IoConcurrencyBudget::new(4, 1),
    );
    let gate = governor.inner.host_refresh.as_ref().unwrap();
    assert_eq!(gate.interval_nanos, 1_000_000_000);
    assert!(!gate.try_claim(), "detection itself is the first sample");
}

#[test]
fn pinned_detected_governor_keeps_its_snapshot_on_admission() {
    let host = ScriptedHostResources::new(critical());
    let governor = detected(&host, Duration::ZERO);
    governor.pin_resources();

    host.set(healthy());
    for _ in 0..4 {
        assert_memory_pressure(admit_background(&governor));
    }
    assert_eq!(host.samples(), 1);
    assert!(governor.snapshot().resources_pinned);
    assert_eq!(governor.snapshot().resources, critical());
}

#[test]
fn caller_supplied_snapshot_is_never_resampled_on_admission() {
    let governor = RuntimeGovernor::new(
        RuntimeGovernorConfig::shared_host(),
        critical(),
        IoConcurrencyBudget::new(4, 1),
    );
    assert!(governor.inner.host_refresh.is_none());
    for _ in 0..4 {
        assert_memory_pressure(admit_background(&governor));
    }
    assert_eq!(governor.snapshot().resources, critical());

    // An explicit host refresh does not turn admission refresh on either.
    governor.refresh_from_host();
    assert!(governor.inner.host_refresh.is_none());
    governor.update_resources(critical());
    assert_memory_pressure(admit_background(&governor));
    assert_eq!(governor.snapshot().resources, critical());
}

#[test]
fn explicit_refresh_counts_as_the_interval_sample() {
    let gate = HostRefreshGate::new(Duration::from_millis(200));
    std::thread::sleep(Duration::from_millis(250));
    gate.record_sample();
    assert!(
        !gate.try_claim(),
        "a fresh explicit sample defers admission"
    );
    std::thread::sleep(Duration::from_millis(250));
    assert!(gate.try_claim());
    assert!(!gate.try_claim(), "one claim per interval");
}

#[test]
fn concurrent_admission_and_explicit_refresh_do_not_deadlock() {
    let host = ScriptedHostResources::new(healthy());
    let governor = detected(&host, Duration::ZERO);
    std::thread::scope(|scope| {
        for _ in 0..4 {
            scope.spawn(|| {
                for _ in 0..200 {
                    drop(admit_background(&governor));
                }
            });
        }
        scope.spawn(|| {
            for round in 0..200 {
                host.set(if round % 2 == 0 {
                    critical()
                } else {
                    healthy()
                });
                governor.refresh_from_host();
            }
        });
    });
    host.set(healthy());
    drop(admit_background(&governor).unwrap());
    assert_eq!(governor.snapshot().active_background_tasks, 0);
}
