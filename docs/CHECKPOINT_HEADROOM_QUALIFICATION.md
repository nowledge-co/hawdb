# Checkpoint headroom qualification

The required `//:hawdb_unit_checkpoint_headroom_tests` label keeps the ordinary
checked root test executable, default feature selection, `exclusive` scheduling,
one libtest thread per process, and the existing 900-second deadline. It is a
compatibility aggregate over two explicit targets with workload ownership:

- `//:hawdb_unit_checkpoint_headroom_growing_table_tests` owns the complete
  growing-table steady-insert regression.
- `//:hawdb_unit_checkpoint_headroom_controls_tests` owns every other
  discovered headroom case, including new controls.

The runner rejects unexpected Bazel sharding, empty selections, discovery failures and
substring overlaps that could suppress another selected test. It acknowledges
an unsharded Bazel invocation. Its structural owner checks that the two
selections are complete and disjoint, preserve caller arguments, and propagate
case failures. The public root aggregate continues to include the same label.
No case, fixture, assertion, five-checkpoint target, row/admission envelope,
100-Busy-retry bound, persistence/reopen check or compilation profile is reduced.
The work is separated into execution budgets; no reduction in total test work
or remote CI speedup is claimed. `exclusive` remains required for both targets.
For pull requests, required context
`ci/skein-bazel-test-checkpoint-growing-table` owns the growing-table target and
`ci/skein-bazel-test-checkpoint-controls` owns the remaining controls target.
The main root presubmit excludes both leaves, while the compatibility aggregate
remains available for local and mainline replay.

## Source-bound failure evidence

PR 1534 head `055eea6e9517363ba82cf2531b88d6c20b644cd8` failed the unsharded
31-case target twice on Linux. The second run used integration base
`870a6edd77fa0913dd8ff47bbb200d67a80cd364`; its BEP records 900.274 seconds
of execution, no queue/setup time for the timed-out attempt, and exit code 142.
Both logs stopped in the growing-table insertion case after prior cases passed.
This does not establish that the insertion case independently used 900 seconds.
The second root invocation passed its other 75 targets.

- [First root attempt](https://prow.nowledge.co/view/gs/ci-cd-503413-prow-public/artifacts/native/pr-logs/pull/nowledge-co_hawdb/1534/pull-skein-bazel-test-root/2117425255046713344)
- [Unchanged-head retry](https://prow.nowledge.co/view/gs/ci-cd-503413-prow-public/artifacts/native/pr-logs/pull/nowledge-co_hawdb/1534/pull-skein-bazel-test-root/2117436870001430528)
- [Existing runtime qualification issue](https://github.com/nowledge-co/hawdb/issues/1512)

## Local timing inventory

On macOS arm64, pinned Rust 1.97.1, the unchanged source above and the repository
Bazel checked profile, all 31 discovered cases passed in serial isolated
exact-case processes. Times include process startup and fixture teardown.
This is diagnostic timing, not the aggregate Linux CI invocation or a cold-build
comparison. The carrier build took 85.186 seconds, critical path 31.74 seconds,
with 348 disk-cache hits and one compiler action.

The measured total was 348.466 seconds. The complete growing-table case
used 237.428 seconds; the remaining 30 used 111.039 seconds. This
motivates explicit workload separation rather than round-robin distribution
whose balance changes when unrelated tests are added. A three-second native
stack sample during the growing-table case observed a capacity-waiting frontend
and an active checkpoint thread encoding relational rows and writing scalar
payloads. It did not observe a deadlock in that interval and does not rule out
other stalled runs. The pending buffered-encoder optimization is separate work:
https://github.com/nowledge-co/hawdb/pull/1528.

| Case suffix under `api::automatic_checkpoint::tests` | Seconds |
| --- | ---: |
| `delta_pressure_sustains_generations_and_preserves_all_values_after_reopen` | 8.647 |
| `headroom::active_cleanup::foreground_denial_wakeup_does_not_bypass_an_admitted_retirement` | 2.059 |
| `headroom::admitted_transaction::concurrent_admitted_commit_releases_pressure_wait_after_retirement_denial` | 2.173 |
| `headroom::capacity_wait::critical_discard_wait_returns_busy_without_resetting_its_deadline` | 1.102 |
| `headroom::capacity_wait::critical_preparation_wait_returns_busy_without_resetting_its_deadline` | 1.266 |
| `headroom::capacity_wait::critical_release_wait_returns_busy_without_resetting_its_deadline` | 1.250 |
| `headroom::capacity_wait::discarding_preparation_fences_capacity_growth_until_cleanup_finishes` | 0.011 |
| `headroom::capacity_wait::discarding_preparation_preserves_available_write_capacity` | 0.011 |
| `headroom::capacity_wait::only_completed_checkpoint_or_builder_progress_renews_capacity_deadline` | 0.011 |
| `headroom::capacity_wait::parked_preparation_rejects_no_data_and_a_busy_writer_can_retry_after_checkpoint` | 6.783 |
| `headroom::capacity_wait::releasing_old_owners_fences_capacity_growth_until_cleanup_finishes` | 0.011 |
| `headroom::capacity_wait::releasing_old_owners_preserves_available_write_capacity` | 0.011 |
| `headroom::foreground::admitted_foreground_request_cannot_wait_for_retirement_using_its_only_cpu` | 2.337 |
| `headroom::notifications::clean_idle_owner_waits_for_an_event_instead_of_polling` | 0.295 |
| `headroom::notifications::handoff_waits_for_adoption_without_periodic_wakes_and_then_retires` | 0.564 |
| `headroom::notifications::pressure_waiting_writer_wakes_admission_retry_without_the_timer` | 1.544 |
| `headroom::pending_idle_admission_denial_releases_the_frontend_without_a_host_retry` | 1.098 |
| `headroom::pending_idle_owner_blocks_pressure_growth_before_admission` | 8.733 |
| `headroom::preparation_backoff::preparation_deadline_cannot_bypass_capacity_or_publication_fences` | 1.207 |
| `headroom::preparation_backoff::preparing_backoff_uses_one_deadline_across_repeated_notifications` | 1.030 |
| `headroom::preparation_backoff::pressured_writer_finishes_while_a_real_prepared_prefix_remains_paused` | 10.018 |
| `headroom::retired_cow_cleanup_allows_writes_and_fences_manual_resource_release` | 1.930 |
| `headroom::retiring_owner_blocks_pressure_growth_until_cleanup_finishes` | 8.386 |
| `relational_live_overlay::checkpoint_debt_reports_the_live_overlay_envelope_and_checkpoint_folds_it` | 4.189 |
| `relational_live_overlay::crash_during_pressure_triggered_drain_recovers_whole_acknowledged_rounds` | 19.976 |
| `relational_live_overlay::live_overlay_pressure_uses_the_shared_soft_and_capacity_boundaries` | 0.011 |
| `relational_live_overlay::pinned_reader_survives_pressure_triggered_live_overlay_drains` | 7.732 |
| `relational_live_overlay::pressure_drain_crash_child` | 0.011 |
| `relational_live_overlay::steady_inserts_into_a_growing_table_keep_draining_the_live_overlay` | 237.428 |
| `relational_live_overlay::steady_relational_updates_on_one_handle_never_exhaust_the_live_overlay` | 15.362 |
| `soft_wal_pressure_sustains_multiple_generations_without_host_checkpoints` | 3.281 |

## Progress evidence and replay

The insert fixture emits bounded-interval diagnostics with acknowledged commit
count, Busy retry count, completed checkpoints, owner phase, work-progress
counter, admission/resource observations and live-overlay debt. The headroom
owner retains `--nocapture` so a timeout preserves completed progress instead
of discarding the active test's captured output. These observations do not
replace any assertion or introduce a new retry.

```console
bazel test //:root_unit_partition_script_test //:hawdb_unit_checkpoint_headroom_tests
```

This runs both explicit targets. To inspect the discovered inventory or replay
one complete workload directly:

```console
bazel build //:hawdb_unit_test_binary
scripts/run-root-unit-partition.sh bazel-bin/hawdb_unit_test_binary checkpoint_headroom --list
bazel-bin/hawdb_unit_test_binary api::automatic_checkpoint::tests::relational_live_overlay::steady_inserts_into_a_growing_table_keep_draining_the_live_overlay --exact --test-threads=1 --nocapture
```

Keep complete final-head BEP results for both targets. Passing local timings or
one green retry do not diagnose every cause in issue 1512; retain its progress
and stability qualification separately.
