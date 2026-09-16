#[path = "query/connected_enumeration.rs"]
mod connected_enumeration;
#[path = "query/costed_algorithms.rs"]
mod costed_algorithms;

fn constrained_hash_join_memory() -> skein_executor::ExecutionMemoryConfig {
    skein_executor::ExecutionMemoryConfig {
        blocking_operator_bytes: std::num::NonZeroUsize::new(512)
            .expect("non-zero blocking budget"),
        max_spill_bytes: std::num::NonZeroU64::new(64 * 1024).expect("non-zero spill budget"),
        max_spill_runs: std::num::NonZeroUsize::new(4).expect("non-zero spill run budget"),
        min_spill_free_bytes: std::num::NonZeroU64::MIN,
        spill_directory: std::env::temp_dir().join(format!(
            "skein-hash-join-spill-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .expect("system clock")
                .as_nanos()
        )),
        ..skein_executor::ExecutionMemoryConfig::default()
    }
}
