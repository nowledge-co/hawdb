# Deterministic storage simulation: scoping decision

Status: recommendation for [issue #301](https://github.com/nowledge-co/hawdb/issues/301),
not an approved simulator implementation. Source baseline:
`23c17ca4d258d51e5425e27e28be8280c5398fa7`.

## Recommendation

Do not start full-system deterministic simulation testing (DST) now. Consider a
bounded storage-only feasibility pilot after an explicit investment decision.
It must run the actual WAL/checkpoint/recovery implementation under controlled
I/O and scheduling, not a second implementation of the storage protocol. A fake
segment reader alone does not satisfy that requirement.

Keep TLA+, semantic and storage fuzzing, targeted crash tests, and native platform
qualification. This spike answers the four exploratory questions; merging this
document does not authorize new production APIs, storage formats, a simulator,
or closing the investment decision. No new crate is justified at this stage.

## 1. Which boundaries can actually be substituted?

| Boundary | Existing implementation | What can be controlled today; what is missing |
| --- | --- | --- |
| Segment payload reads | [`SegmentRangeReader` and `SegmentReadExecutor`](../crates/storage/src/scan/reader.rs) | A reader can return bytes/errors or block at a test gate. The executor still uses a concrete Rayon pool and real synchronization. It collects results in schedule order; that does not determine execution order. |
| Async segment reads | [`TokioSegmentReadExecutor`](../crates/runtime-tokio/src/segment_read.rs) | Reuses the same reader through `spawn_blocking`. Existing gated-reader tests control completion and check permit lifetime. Tokio tasks, blocking workers, retry timers, and cancellation are not one simulator-controlled scheduler. |
| Positioned file reads | [`read_exact_at`](../crates/storage/src/io.rs) | Portable Unix, Windows, and fallback implementations exist. The private closure-based short-read loop is testable, but its production entrypoint takes a concrete `File`, not a filesystem backend. |
| I/O admission | [`RuntimeIoWaveController`](../crates/core/src/cancellation.rs) | Injects capacity acquisition, not storage contents, durability, filesystem operations, or every task/timer transition. |
| WAL append and sync | [`DurableStore::append_entry`, `finish_wal_sync_group`, `rollback_failed_wal_write`](../src/store/durable.rs) | Concrete cached `Arc<File>`, `OpenOptions`, `write_all`, `sync_data`, `set_len`, and directory synchronization. Targeted partial-write/rollback/sync failpoints exist, but no interchangeable write backend covers this lifecycle. |
| Checkpoint and manifest publication | [`graph_checkpoint.rs`](../src/store/graph_checkpoint.rs), [`DurableManifest::write`](../src/store/durable.rs), [`durable_replace_file`](../crates/storage/src/durability.rs) | Artifact creation, synchronization, rename, reopen, and reclamation use real filesystem operations. Publication-stage and destination-specific failure hooks are not a virtual filesystem. |
| Concurrent commit scheduling | [`CommitSequencer`](../src/api/concurrent/coordinator.rs) | Real `Mutex`, `Condvar::wait_timeout`, `Instant`, queue ownership, and leadership guards. The post-enqueue barrier and state-level regressions cover chosen interleavings, not seeded control over all runnable transitions. |

The original issue's direct Unix-read description has evolved: the shared
positioned-read helper and the portable Tokio adapter now exist. Neither is an
`io_uring` implementation, and this proposal must not introduce one.

The write path needs a new **internal** substitution boundary before combined
storage simulation is possible. Auditing only `durability.rs` would miss writes
in `DurableStore` and delegated canonical, row-page, overflow, index, and append
publication code. A pilot must inventory every operation reachable by its chosen
fixture, including recovery reads, metadata, directory enumeration, file locks,
and cleanup. Unsupported operations must fail explicitly, never escape to the
host filesystem and silently weaken the determinism claim.

Current test hooks have limited scope: several are thread-local; subprocess crash
hooks select fixed stages. They do not compose into cross-thread, seed-driven
fault scheduling merely by assigning them a common random seed.

## 2. What additional bugs could DST find?

The missing axis is systematic **composition** of implementation-level events:
one writer partially appends while another waits for the group barrier; a delayed
completion overlaps cancellation; publication succeeds before cleanup fails and
another transaction starts. Existing tests already cover many components and
some combinations. DST is not the first storage or concurrency verification layer.

### Retrospective against actual merged fixes

- [#244 / fix #249](https://github.com/nowledge-co/hawdb/commit/91f0a73354627fbc308c144e7a21214db6368d4e):
  a follower's timeout could see neither queue membership nor an active leader
  after completion. The fix also checks the request's completed result.
  `liveness_check_distinguishes_completed_and_orphaned_requests` now directly
  catches this state. A simulator could explore completion/leadership/timeout
  orderings, but only if it controls those transitions; fake disk bytes cannot.
- [#174 / fix #305](https://github.com/nowledge-co/hawdb/commit/43523c9f91944054b153a310962f82355da7c584):
  incomplete append handling now rolls back to a known prefix or poisons the
  handle when the outcome is uncertain. The
  [`wal_tail` tests](../src/api/tests/storage_recovery/wal_tail.rs) already cover
  partial writes, failed rollback/sync, grouped-prefix preservation, and process
  interruption. [`wal_tail_oracle.rs`](../crates/fuzz/src/wal_tail_oracle.rs)
  varies fragment boundaries and residency modes. DST could combine varying
  short-write offsets with group followers, barrier failures, and restart, beyond
  the fixed partial-write position and authored schedules.
- [#203 / fix #268](https://github.com/nowledge-co/hawdb/commit/d0e4742906f8ced610f4be4012df8966654dea64):
  cached append-handle lifecycle and uncertain durability classification were
  hardened. [`wal_group_sync_failure_rejects_commit_and_poisons_until_reopen`](../src/api/tests/concurrent_transactions.rs)
  already proves that a failed acknowledgment can coexist with a complete record
  recovered later. A simulation oracle must allow that uncertain outcome. The
  same change's nested-batch encoder rejection is a unit/semantic-testing concern,
  not evidence that DST is needed.
- [#168 / fix #243](https://github.com/nowledge-co/hawdb/commit/000720daf35cb8e99b6b4dde30c3ad6d3629371a):
  reclamation was moved after published in-memory state adoption and made
  retryable maintenance debt. The current
  [`checkpoint_reclamation_failure_is_reported_and_retried_after_publication`](../src/store.rs)
  checks publication, reopen, debt, and retry. A pilot could add reader-pin
  release, repeated cleanup failures, and subsequent commits at each boundary.

These are plausible coverage opportunities, **not proof that DST would have
found a historical bug earlier**. Each cited bug has an existing regression.
The pilot must demonstrate a reproducible combination not already represented
by those tests, plus a negative control that the combination actually detects.
Finding a new real defect would be stronger evidence, but is not assumed.

## 3. Full-system versus storage-only scope

Full-system DST would additionally need controlled query execution, admission,
background work, caches, search/vector projections, thread pools, clocks, and
entropy sources. That is a broad architectural commitment, not a small adapter
over `SegmentRangeReader`.

The proposed first pilot includes one bounded graph or relational fixture,
concurrent commit requests, WAL append/group sync, checkpoint publication,
recovery, pinned segment reads, and reclamation reachable by that fixture.
Freeze unrelated background work and derived capabilities explicitly. Fail on
an unsupported path; do not quietly replace real recovery with a mock protocol.
This deliberately limited coverage must remain visible in every report.

### Required simulation semantics

- Separate operation submission, observable completion, durable effects, and
  acknowledgment. A failed write or sync may already have effects; `Err` must
  not automatically mean that nothing happened. Include short/partial writes,
  failure before/after effects, delayed completion, and bounded crash points.
- Model file contents and namespace persistence separately. After a crash, choose
  only states permitted by the documented durability profile: retain guaranteed
  synchronized data while allowing specified unsynchronized effects to persist
  or disappear. A universal "all unsynced bytes vanish" model is too strong.
- Treat process termination and power loss as different fault models. Killing
  today's test subprocess does not flush the host's page cache out of existence;
  simulated power loss does not qualify a real disk's firmware or kernel.
- Distinguish Unix rename-plus-directory-sync from Windows write-through replace
  in `durable_replace_file`; do not pretend a Windows directory sync is the Unix
  primitive. Keep Linux, macOS, and Windows native recovery/locking tests.
- Give runnable work stable IDs and choose only enabled transitions. Record
  scheduling and fault decisions, not OS thread IDs or wall-clock timestamps.
  Use a virtual monotonic clock for deadlines; model wall-clock skew separately,
  never by moving monotonic time backward. Inventory entropy, timestamps, and
  iteration order in every admitted code path.
- Bound actors, operations, bytes, pending completions, restart count, and trace
  size. A no-progress limit produces an explicit failure/incomplete result with
  its pending events; it is not a passing campaign or an unproven deadlock claim.

The independent oracle tracks acknowledged commits, explicitly rolled-back
operations, and uncertain outcomes separately. Recovery must retain acknowledged
effects, admit only whole transactions consistent with the allowed commit order,
reject partial batches, and preserve a complete manifest/artifact closure.
Pinned readers must not lose reachable generations. Failed reclamation remains
debt rather than undoing publication. Quiescence must release permits and work
ownership. The oracle must not call the implementation's own recovery/planning
logic to compute expected results.

### Investment gates, not an implementation schedule

1. **Boundary feasibility:** after owner approval, route every operation of the
   chosen fixture through an internal adapter while retaining the real production
   implementation and defaults. Show a complete operation inventory, zero host
   escapes, and unchanged real-backend behavior. Stop if this requires a public
   API or persistence redesign without a separately approved proposal.
2. **Deterministic combined-fault proof:** replay from fresh state, including
   concurrent requests, a persistence fault, and restart in one scenario. Prove
   trace/image equivalence for a fixed source revision and model profile, and
   show historical and compound negative controls fail the intended invariant.
   Trace divergence, missing artifacts, and incomplete exploration must fail
   separately from a storage correctness mismatch.
3. **Value decision:** compare the new scenario coverage, reproducibility,
   execution cost, and maintenance surface against extending today's tests.
   Expand only with evidence and an owner decision. If controlling the selected
   path requires a full-system rewrite or catches only existing authored cases,
   defer DST and retain the useful regressions; do not relabel the limited pilot
   as full-system simulation.

## 4. Seed, replay, ownership, and fuzz integration

Reuse the **campaign conventions and local orchestration**, not a new parallel
testing ecosystem. [`append_main.rs`](../crates/fuzz/src/append_main.rs) provides
stable campaign/case seeds, `case_index % shard_count` selection, a direct replay
command, current reports, and validation of the exact completed prefix on resume.
It does not replace OS scheduling or record every I/O decision. The append test
named `append_state_machine_is_replayable_for_another_seed` checks another seed,
not equality of two physical runs; the WAL-tail oracle separately compares two
fresh-state JSON reports. Neither is byte-for-byte full-system replay evidence.

For an initial pilot, keep private store/coordinator access in root `#[cfg(test)]`
modules and register its manual campaign through the existing Bazel local-fuzz
suite. Keep public-facade semantic campaigns in `hawdb-fuzz`. Do not add a
`hawdb -> hawdb-fuzz -> hawdb` dependency cycle or export internal storage hooks
solely to put a driver in that crate. A later shared helper needs a real ownership
boundary; a separate simulator crate is not a prerequisite.

A future report can use a distinct proposed `hawdb-storage-simulation-v1`
protocol without changing existing v1 campaigns. It must include source revision,
model/profile identity, generator identity, limits, campaign seed, absolute case
index, derived seed, decision trace, initial fixture digest, expected/observed
outcome, and the final simulated durable image digest. Record host timings and
temporary paths outside the canonical replay payload. Define byte identity over
canonical trace and simulated image, not human diagnostics or real disk layout.

Sharding partitions absolute case indexes, never events within a scenario. A
case's generated operations and choices must not change with shard count, resume,
or host parallelism. Resume must validate the new identities and completed prefix;
an incomplete checkpoint cannot become success. Keep the original trace before
reduction. A reducer may remove operations/faults only if replay still satisfies
dependencies and reproduces the same invariant signature; invalid schedules or
setup errors are not reduced reproductions. This reducer is a future requirement,
not a capability supplied by the current append harness.

Keep deterministic fuzz targets local/manual and available through Bazel. Do not
add default or dedicated fuzz CI jobs. No simulator setting becomes an embedded
production environment variable, shell-out, or runtime control plane.

## Executable evidence for this scoping spike

Use the existing production-boundary tests; a new simulator is not needed to
prove that the read seam is injectable or that current write regressions exist:

```sh
bazel test //crates/storage:hawdb_storage_tests \
  //crates/runtime-tokio:hawdb_runtime_tokio_tests \
  //:hawdb_unit_tests //:hawdb_storage_crash_recovery_tests \
  //crates/fuzz:hawdb_fuzz_tests //crates/fuzz:hawdb_fuzz_cli_tests \
  //:hawdb_linux_ci_fuzz_smoke_test --nocache_test_results
```

Inspect actual test logs, not only suite exit codes. In particular, require the
gated async tests `dropping_execute_future_retains_in_flight_io_capacity` and
`read_error_joins_all_submitted_reads_before_returning`, the storage reader's
`panicking_range_read_preserves_completed_waves_and_returns_typed_error`, and the
retrospective tests named above. These checks prove current seams and regression
coverage, **not simulator determinism, new fault coverage, or exhaustive storage
correctness**. Record execution receipts in the PR so this source audit does not
become a permanently stale CI-status document.
