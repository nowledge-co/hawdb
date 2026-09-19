use hawdb::{
    IoConcurrencyBudget, ProcessMemoryCapabilities, ProcessMemoryPolicy, ProcessMemoryPolicyConfig,
    ProcessMemorySnapshot, RuntimeAdmissionCode, RuntimeGovernor, RuntimeGovernorConfig,
    RuntimeMemorySnapshot, RuntimeResourceBudget, RuntimeResourceSnapshot, RuntimeWorkRequest,
};
use std::num::{NonZeroU64, NonZeroUsize};

fn resources() -> RuntimeResourceSnapshot {
    RuntimeResourceSnapshot::from_parts(
        RuntimeResourceBudget::from_limits(NonZeroUsize::MIN, None, None),
        RuntimeMemorySnapshot::from_limits(Some(1_000), Some(1_000), None, None, None),
    )
}

fn sample(resident_bytes: u64) -> ProcessMemorySnapshot {
    ProcessMemorySnapshot {
        capabilities: ProcessMemoryCapabilities {
            resident_memory: true,
            total_page_faults: false,
            split_page_faults: false,
        },
        resident_bytes,
        peak_resident_bytes: resident_bytes,
        total_page_faults: None,
        minor_page_faults: None,
        major_page_faults: None,
    }
}

#[test]
fn public_policy_coordinates_rss_headroom_across_embedded_governors() {
    let policy = ProcessMemoryPolicy::new(ProcessMemoryPolicyConfig::new(
        NonZeroU64::new(100).unwrap(),
    ));
    policy.update(sample(20));
    let first = RuntimeGovernor::new_with_process_memory_policy(
        RuntimeGovernorConfig::shared_host(),
        resources(),
        IoConcurrencyBudget::new(1, 1),
        policy.clone(),
    );
    let second = RuntimeGovernor::new_with_process_memory_policy(
        RuntimeGovernorConfig::shared_host(),
        resources(),
        IoConcurrencyBudget::new(1, 1),
        policy,
    );

    let permit = first
        .try_admit(RuntimeWorkRequest::foreground_query(60, 0).with_blocking(false))
        .unwrap();
    let error = second
        .try_admit(RuntimeWorkRequest::foreground_query(30, 0).with_blocking(false))
        .unwrap_err();
    assert_eq!(error.code, RuntimeAdmissionCode::MemorySaturated);
    assert!(error.is_retryable());

    drop(permit);
    assert!(second
        .try_admit(RuntimeWorkRequest::foreground_query(30, 0).with_blocking(false))
        .is_ok());
}
