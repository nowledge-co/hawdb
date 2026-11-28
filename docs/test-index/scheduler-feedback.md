# Feedback scheduling and checkpoint preparation (#2023)

Owner: `crates/scheduler/src/tests.rs`; `api::automatic_checkpoint::tests::planning_retry::cancelled_queued_preparation_releases_its_source_before_worker_available`
Lane: `//crates/scheduler:hawdb_scheduler_tests` in `//:hawdb_presubmit_crate_tests`; `//:hawdb_unit_checkpoint_lifecycle_tests` in checkpoint controls
Replay: `cargo test --locked -p hawdb-scheduler`; `cargo test --locked -p hawdb --lib cancelled_queued_preparation_releases_its_source_before_worker_available`

The scheduler cases cover pool isolation, feedback, interval-based aging,
bounded admission, cancellation, panic isolation and shutdown across worker
and queue teardown boundaries. Automatic checkpoint preparation uses the
shared background pool. Its cancellation case retains the source when a
queued preparation is stopped before a worker becomes available; the existing
checkpoint lanes cover publication, recovery and headroom. The scheduler does
not own checkpoint publication or the persistent format.
