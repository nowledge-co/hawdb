# Feedback scheduling and checkpoint preparation (#2023)

Owner: `crates/scheduler/src/tests.rs`; `crates/qos/src/runtime/blocking_admission_tests.rs` interactive admission cases; `hawdb_runtime_tokio::tests::scheduled_queries_prioritize_high_and_release_cancelled_queued_work`; `api::query_runtime::tests::fast_scheduling_requires_a_proven_identity_point_read`; `embedded_tokio::concurrency_tests::point_read_and_general_queries_enter_distinct_scheduling_levels`; `api::automatic_checkpoint::tests::planning_retry::cancelled_queued_preparation_releases_its_source_before_worker_available`
Lane: `//crates/scheduler:hawdb_scheduler_tests`, `//crates/qos:hawdb_qos_tests` and `//crates/runtime-tokio:hawdb_runtime_tokio_tests` in `//:hawdb_presubmit_crate_tests`; root unit tests; `//:hawdb_unit_checkpoint_lifecycle_tests` in checkpoint controls
Replay: `cargo test --locked -p hawdb-scheduler`; `cargo test --locked -p hawdb-qos interactive_`; `cargo test --locked -p hawdb-runtime-tokio scheduled_queries_prioritize_high_and_release_cancelled_queued_work`; `cargo test --locked -p hawdb --lib point_read_and_general_queries_enter_distinct_scheduling_levels`; `cargo test --locked -p hawdb --lib cancelled_queued_preparation_releases_its_source_before_worker_available`

The scheduler cases cover pool isolation, feedback, interval-based aging,
bounded admission, cancellation, panic isolation and shutdown across worker
and queue teardown boundaries. Automatic checkpoint preparation uses the
shared background pool. Its cancellation case retains the source when a
queued preparation is stopped before a worker becomes available; the existing
checkpoint lanes cover publication, recovery and headroom. The scheduler does
not own checkpoint publication or the persistent format.

The Tokio non-streaming query adapter dispatches owned Cypher and SQL work
through business feedback queues after resource admission. Only a simple
internal node identity point read enters at high priority; scalar property
indexes can fall back to scans and therefore remain normal. The runtime test
covers high-before-normal dispatch and cancellation while queued, and the
facade test compares query results and accepted-dispatch counters. Streaming
and synchronous borrowed entry points are not covered by this integration.
Admission cases cover the preference among foreground waiters, aging of both
ordinary and background work, and a caller's explicit lower queue rank.
The runtime panic case checks error propagation and resource release.
