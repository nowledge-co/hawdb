use hawdb::{
    EmbeddedQueryError, HawDBEmbedded, HawDBEmbeddedOpenOptions, IoConcurrencyBudget,
    ProcessMemoryCapabilities, ProcessMemoryPolicy, ProcessMemoryPolicyConfig,
    ProcessMemorySnapshot, RuntimeAdmissionCode, RuntimeGovernor, RuntimeGovernorConfig,
    RuntimeMemorySnapshot, RuntimeResourceBudget, RuntimeResourceSnapshot, RuntimeWorkRequest,
    Value,
};
use std::num::{NonZeroU64, NonZeroUsize};
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

const MIB: u64 = 1024 * 1024;
static NEXT_PATH: AtomicU64 = AtomicU64::new(0);

struct TestDirectory(PathBuf);

impl TestDirectory {
    fn new() -> Self {
        Self(std::env::temp_dir().join(format!(
            "hawdb-process-memory-{}-{}",
            std::process::id(),
            NEXT_PATH.fetch_add(1, Ordering::Relaxed),
        )))
    }

    fn options(&self) -> HawDBEmbeddedOpenOptions {
        let mut config = hawdb::DatabaseConfig {
            max_wal_record_bytes: Some(1024 * 1024),
            max_read_result_rows: Some(8),
            max_read_result_payload_bytes: Some(64 * 1024),
            ..hawdb::DatabaseConfig::default()
        };
        config.mutation_limits.max_affected_rows = NonZeroUsize::new(8).unwrap();
        config.mutation_limits.max_operations = NonZeroUsize::new(8).unwrap();
        config.mutation_limits.max_result_rows = NonZeroUsize::new(8).unwrap();
        config.mutation_limits.max_result_payload_bytes = NonZeroUsize::new(64 * 1024).unwrap();
        HawDBEmbeddedOpenOptions::new(self.0.join("graph"))
            .with_config(config)
            .with_resource_snapshot(RuntimeResourceSnapshot::from_parts(
                RuntimeResourceBudget::from_limits(NonZeroUsize::new(2).unwrap(), None, None),
                RuntimeMemorySnapshot::from_limits(
                    Some(512 * MIB),
                    Some(512 * MIB),
                    None,
                    None,
                    None,
                ),
            ))
            .with_runtime_governor_config(RuntimeGovernorConfig {
                memory_budget_bytes: Some(64 * MIB),
                result_budget_bytes: 64 * 1024,
                ..RuntimeGovernorConfig::shared_host()
            })
    }
}

impl Drop for TestDirectory {
    fn drop(&mut self) {
        if self.0.exists() {
            std::fs::remove_dir_all(&self.0).unwrap();
        }
    }
}

fn policy() -> ProcessMemoryPolicy {
    let policy = ProcessMemoryPolicy::new(
        ProcessMemoryPolicyConfig::new(NonZeroU64::new(64 * MIB).unwrap())
            .with_recovery_headroom(8 * MIB)
            .with_sample_max_age(Duration::from_secs(60)),
    );
    policy.update(sample(16 * MIB));
    policy
}

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

#[test]
fn embedded_queries_reject_rss_pressure_before_mutation_and_recover_with_hysteresis() {
    let directory = TestDirectory::new();
    let policy = policy();
    let mut embedded =
        HawDBEmbedded::open_with_process_memory_policy(directory.options(), policy.clone())
            .unwrap();
    embedded
        .query_admitted("CREATE (:Probe {value: 1})")
        .unwrap();
    let resources = embedded.runtime_governor().snapshot();

    policy.update(sample(65 * MIB));
    let error = embedded
        .query_admitted("CREATE (:Probe {value: 2})")
        .unwrap_err();
    assert!(matches!(error, EmbeddedQueryError::Admission(error)
        if error.code == RuntimeAdmissionCode::MemorySaturated && error.is_retryable()));
    assert_eq!(policy.snapshot().available_bytes, Some(0));
    assert_eq!(
        embedded.runtime_governor().snapshot().limits,
        resources.limits
    );

    policy.update(sample(60 * MIB));
    assert!(policy.snapshot().admission_paused);
    assert!(matches!(
        embedded.query_admitted("MATCH ("),
        Err(EmbeddedQueryError::Admission(_))
    ));
    policy.update(sample(16 * MIB));
    assert!(!policy.snapshot().admission_paused);
    let rows = embedded
        .query_admitted("MATCH (p:Probe) RETURN p.value AS value")
        .unwrap()
        .rows;
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].get("value"), Some(&Value::Int(1)));
    assert_eq!(policy.snapshot().unobserved_reserved_bytes, 0);
}

#[test]
fn embedded_refresh_preserves_pinned_resources_and_host_owned_rss_samples() {
    let directory = TestDirectory::new();
    let policy = policy();
    let mut embedded =
        HawDBEmbedded::open_with_process_memory_policy(directory.options(), policy.clone())
            .unwrap();
    let initial = embedded.runtime_governor().snapshot();
    let policy_before = policy.snapshot();
    assert!(initial.resources_pinned);
    assert!(!embedded.refresh_runtime_resources());
    assert_eq!(
        embedded.runtime_governor().snapshot().resources,
        initial.resources
    );
    assert_eq!(
        embedded.runtime_resources().memory,
        initial.resources.memory
    );
    assert_eq!(policy.snapshot(), policy_before);

    let updated = RuntimeResourceSnapshot::from_parts(
        initial.resources.cpu,
        RuntimeMemorySnapshot::from_limits(Some(512 * MIB), Some(32 * MIB), None, None, None),
    );
    assert!(embedded.update_runtime_resources(updated));
    assert!(!embedded.refresh_runtime_resources());
    assert_eq!(embedded.runtime_governor().snapshot().resources, updated);
    assert!(embedded.runtime_governor().snapshot().resources_pinned);
    assert_eq!(policy.snapshot(), policy_before);
}

#[test]
fn embedded_instances_share_reservations_without_charging_observed_growth_twice() {
    let first_directory = TestDirectory::new();
    let second_directory = TestDirectory::new();
    let policy = policy();
    let first =
        HawDBEmbedded::open_with_process_memory_policy(first_directory.options(), policy.clone())
            .unwrap();
    let mut second =
        HawDBEmbedded::open_with_process_memory_policy(second_directory.options(), policy.clone())
            .unwrap();
    let governor = first.runtime_governor();
    let permit = governor
        .try_admit(
            RuntimeWorkRequest::new(
                hawdb::RuntimeWorkPriority::Foreground,
                hawdb::RuntimeWorkKind::Control,
            )
            .with_memory_bytes(48 * MIB),
        )
        .unwrap();
    assert!(matches!(
        second.query_admitted("CREATE (:Probe {value: 1})"),
        Err(EmbeddedQueryError::Admission(_))
    ));

    policy.update(sample(48 * MIB));
    assert_eq!(policy.snapshot().unobserved_reserved_bytes, 16 * MIB);
    assert_eq!(governor.snapshot().admitted_memory_bytes, 48 * MIB);
    assert!(matches!(
        second.query_admitted("MATCH ("),
        Err(EmbeddedQueryError::Admission(_))
    ));
    drop(permit);
    assert_eq!(policy.snapshot().unobserved_reserved_bytes, 0);
    assert_eq!(policy.snapshot().sampled_resident_bytes, Some(48 * MIB));
    policy.update(sample(40 * MIB));
    assert!(second
        .query_admitted("MATCH (p:Probe) RETURN p.value AS value")
        .unwrap()
        .rows
        .is_empty());
}

#[test]
fn embedded_missing_unsupported_and_stale_samples_fail_closed() {
    for mode in 0..3 {
        let directory = TestDirectory::new();
        let policy = ProcessMemoryPolicy::new(
            ProcessMemoryPolicyConfig::new(NonZeroU64::new(64 * MIB).unwrap()).with_sample_max_age(
                if mode == 2 {
                    Duration::ZERO
                } else {
                    Duration::from_secs(60)
                },
            ),
        );
        if mode == 1 {
            policy.update(ProcessMemorySnapshot {
                capabilities: ProcessMemoryCapabilities::default(),
                ..sample(0)
            });
        } else if mode == 2 {
            policy.update(sample(0));
            while policy.snapshot().sample_is_current {
                std::thread::yield_now();
            }
        }
        let mut embedded =
            HawDBEmbedded::open_with_process_memory_policy(directory.options(), policy.clone())
                .unwrap();
        assert!(
            matches!(embedded.query_admitted("CREATE (:Probe {value: 1})"), Err(EmbeddedQueryError::Admission(error))
            if error.code == RuntimeAdmissionCode::MemoryPressure && error.is_retryable())
        );
        assert!(embedded
            .database_mut()
            .query("MATCH (p:Probe) RETURN p.value AS value")
            .unwrap()
            .rows
            .is_empty());
        assert_eq!(policy.snapshot().unobserved_reserved_bytes, 0);
    }
}

#[cfg(feature = "tokio-runtime")]
mod asynchronous {
    use super::*;
    use hawdb::{
        HawDBTokioEmbedded, HawDBTokioEmbeddedError, RuntimeCancellationReason,
        RuntimeCancellationToken, RuntimeTaskContext, TokioRuntimeConfig, TokioTaskError,
    };
    use std::time::Instant;

    fn config() -> TokioRuntimeConfig {
        TokioRuntimeConfig {
            resource_refresh_interval: Duration::ZERO,
            ..TokioRuntimeConfig::default()
        }
    }

    fn after_queued(
        governor: RuntimeGovernor,
        action: impl FnOnce() + Send + 'static,
    ) -> std::thread::JoinHandle<()> {
        std::thread::spawn(move || {
            let deadline = Instant::now() + Duration::from_secs(10);
            while governor.snapshot().queued_admission_waiters == 0 {
                assert!(
                    Instant::now() < deadline,
                    "query never queued for RSS admission"
                );
                std::thread::sleep(Duration::from_millis(1));
            }
            action();
        })
    }

    #[test]
    fn owned_and_borrowed_tokio_wait_for_host_samples_and_cancel_without_mutation() {
        let host_directory = TestDirectory::new();
        let borrowed_directory = TestDirectory::new();
        let policy = policy();
        let owned = HawDBTokioEmbedded::from_owned(
            HawDBEmbedded::open_with_process_memory_policy(
                host_directory.options(),
                policy.clone(),
            )
            .unwrap(),
            config(),
        )
        .unwrap();
        let borrowed = HawDBTokioEmbedded::from_borrowed(
            HawDBEmbedded::open_with_process_memory_policy(
                borrowed_directory.options(),
                policy.clone(),
            )
            .unwrap(),
            owned.runtime().handle().clone(),
            config(),
        );
        for embedded in [&owned, &borrowed] {
            policy.clear_sample();
            let governor = embedded.runtime().governor().clone();
            let cancellation = RuntimeCancellationToken::new();
            let cancel = cancellation.clone();
            let trigger = after_queued(governor.clone(), move || {
                cancel.cancel();
            });
            let result = owned
                .runtime()
                .block_on(embedded.query(
                    "CREATE (:Probe {value: 1})",
                    RuntimeTaskContext::new(
                        cancellation,
                        Some(Instant::now() + Duration::from_secs(10)),
                    ),
                ))
                .unwrap();
            trigger.join().unwrap();
            assert!(matches!(
                result,
                Err(HawDBTokioEmbeddedError::Task(TokioTaskError::Stopped(
                    RuntimeCancellationReason::Cancelled
                )))
            ));
            assert_eq!(governor.snapshot().queued_admission_waiters, 0);
            assert_eq!(governor.snapshot().admitted_memory_bytes, 0);

            policy.update(sample(65 * MIB));
            let recovery = policy.clone();
            let trigger = after_queued(governor.clone(), move || {
                recovery.update(sample(16 * MIB));
            });
            let rows = owned
                .runtime()
                .block_on(embedded.query(
                    "MATCH (p:Probe) RETURN p.value AS value",
                    RuntimeTaskContext::with_timeout(Duration::from_secs(10)),
                ))
                .unwrap()
                .unwrap()
                .rows;
            trigger.join().unwrap();
            assert!(rows.is_empty());
            assert_eq!(governor.snapshot().queued_admission_waiters, 0);
            assert_eq!(policy.snapshot().unobserved_reserved_bytes, 0);
            assert_eq!(
                governor.snapshot().resources,
                host_directory.options().resource_snapshot.unwrap()
            );
            assert!(!embedded.refresh_runtime_resources());
            assert_eq!(policy.snapshot().sampled_resident_bytes, Some(16 * MIB));
        }
    }

    #[test]
    #[cfg(any(target_os = "linux", target_os = "macos", windows))]
    fn native_rss_changes_embedded_admission_without_cgroups() {
        const TEST: &str = "asynchronous::native_rss_changes_embedded_admission_without_cgroups";
        const CHILD: &str = "HAWDB_TEST_PROCESS_MEMORY_CHILD";
        if std::env::var_os(CHILD).is_none() {
            let output = std::process::Command::new(std::env::current_exe().unwrap())
                .args([TEST, "--exact", "--nocapture"])
                .env(CHILD, "1")
                .output()
                .unwrap();
            assert!(
                output.status.success(),
                "native RSS child failed: {}{}",
                String::from_utf8_lossy(&output.stdout),
                String::from_utf8_lossy(&output.stderr)
            );
            assert!(String::from_utf8_lossy(&output.stdout).contains("hawdb-process-rss-native-v1"));
            print!("{}", String::from_utf8_lossy(&output.stdout));
            return;
        }
        let warm_directory = TestDirectory::new();
        {
            let mut warm = HawDBEmbedded::open_with_options(warm_directory.options()).unwrap();
            warm.query_admitted("CREATE (:Probe {value: 1})").unwrap();
            warm.query_admitted("MATCH (p:Probe) RETURN p.value AS value")
                .unwrap();
        }
        let baseline = ProcessMemorySnapshot::capture().unwrap();
        assert!(baseline.capabilities.resident_memory);
        let limit = baseline.resident_bytes + 32 * MIB;
        let policy = ProcessMemoryPolicy::from_current_process(
            ProcessMemoryPolicyConfig::new(NonZeroU64::new(limit).unwrap())
                .with_recovery_headroom(8 * MIB)
                .with_sample_max_age(Duration::from_secs(60)),
        )
        .unwrap();
        let sync_directory = TestDirectory::new();
        let tokio_directory = TestDirectory::new();
        let mut synchronous = HawDBEmbedded::open_with_process_memory_policy(
            sync_directory.options(),
            policy.clone(),
        )
        .unwrap();
        let asynchronous = HawDBTokioEmbedded::from_owned(
            HawDBEmbedded::open_with_process_memory_policy(
                tokio_directory.options(),
                policy.clone(),
            )
            .unwrap(),
            config(),
        )
        .unwrap();
        synchronous
            .query_admitted("CREATE (:Probe {value: 1})")
            .unwrap();
        asynchronous
            .runtime()
            .block_on(asynchronous.query(
                "CREATE (:Probe {value: 1})",
                RuntimeTaskContext::with_timeout(Duration::from_secs(10)),
            ))
            .unwrap()
            .unwrap();
        policy.refresh_from_host().unwrap();
        let before = policy.snapshot().sampled_resident_bytes.unwrap();
        assert!(before + 8 * MIB < limit, "insufficient baseline headroom");

        // An anonymous mapping makes release observable independently of allocator
        // caching. The production policy only observes RSS; it never reclaims memory.
        let mut allocation =
            memmap2::MmapMut::map_anon(usize::try_from(64 * MIB).unwrap()).unwrap();
        let mut random = 7_u64;
        for value in allocation.iter_mut() {
            random ^= random << 13;
            random ^= random >> 7;
            random ^= random << 17;
            *value = random as u8;
        }
        std::hint::black_box(&allocation);
        policy.refresh_from_host().unwrap();
        let elevated = policy.snapshot().sampled_resident_bytes.unwrap();
        assert!(
            elevated >= before + 16 * MIB,
            "bounded allocation did not raise current RSS"
        );
        assert!(policy.snapshot().admission_paused);
        assert!(
            matches!(synchronous.query_admitted("CREATE (:Probe {value: 2})"), Err(EmbeddedQueryError::Admission(error)) if error.is_retryable())
        );
        let rejected = asynchronous
            .runtime()
            .block_on(asynchronous.query(
                "CREATE (:Probe {value: 2})",
                RuntimeTaskContext::with_timeout(Duration::from_millis(100)),
            ))
            .unwrap();
        assert!(matches!(
            rejected,
            Err(HawDBTokioEmbeddedError::Task(TokioTaskError::Stopped(
                RuntimeCancellationReason::DeadlineExceeded
            )))
        ));

        drop(allocation);
        let deadline = Instant::now() + Duration::from_secs(5);
        loop {
            policy.refresh_from_host().unwrap();
            if !policy.snapshot().admission_paused {
                break;
            }
            assert!(
                Instant::now() < deadline,
                "native RSS did not recover after bounded allocation release: {:?}",
                policy.snapshot()
            );
            std::thread::sleep(Duration::from_millis(10));
        }
        let recovered = policy.snapshot().sampled_resident_bytes.unwrap();
        assert_eq!(
            synchronous
                .query_admitted("MATCH (p:Probe) RETURN p.value AS value")
                .unwrap()
                .rows
                .len(),
            1
        );
        assert_eq!(
            asynchronous
                .runtime()
                .block_on(asynchronous.query(
                    "MATCH (p:Probe) RETURN p.value AS value",
                    RuntimeTaskContext::with_timeout(Duration::from_secs(10)),
                ))
                .unwrap()
                .unwrap()
                .rows
                .len(),
            1
        );
        assert_eq!(policy.snapshot().unobserved_reserved_bytes, 0);
        println!("hawdb-process-rss-native-v1 os={} baseline_bytes={before} elevated_bytes={elevated} recovered_bytes={recovered} limit_bytes={limit} allocation_bytes={}", std::env::consts::OS, 64 * MIB);
    }
}
