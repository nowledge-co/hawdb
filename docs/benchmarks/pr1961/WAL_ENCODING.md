# Record-local WAL encoding buffers

This report covers its named historical revisions. See the [PR description](https://github.com/nowledge-co/hawdb/pull/1961) for current qualification and review gates.

Issue Number: ref #1960. PR: https://github.com/nowledge-co/hawdb/pull/1961.

## Source change and boundaries

Runtime `ac43fe6b8ee51b50e4587c3509839832a7175c2d` replaces a fresh operation
body allocation per operation and fresh property-value buffers with two
record-local reusable buffers. Both ordinary and prepared WAL encoding call
the same helper. Each buffer retains at most 64 KiB after a frame; larger
capacities are released immediately. Both are dropped when encoding returns.
This is not a process/thread cache. The retained scratch bound is 128 KiB per
in-progress record; total record/output memory remains proportional to its
payload. Complex nested values and property maps retain their existing codec.

Validation, framing, operation ordering, LSN/epoch binding, checksums, WAL
append and every synchronization barrier are unchanged. The encoder still
copies the operation body into the final record. No new format or public API
is introduced. This avoids allocation traffic without making encoding constant
time or explaining previous untraced slow mutations.

Regression ownership remains `//crates/storage:hawdb_storage_tests`. New cases
compare complete ordinary/prepared bytes against the independent checkpoint
encoder after a large-to-small transition, check large scratch release, and
require fewer than 64 native allocations for scalar batches of 128 and 4096
operations. Existing exhaustive operation/value wire fixtures remain active.

## Encoding-only measurements

The manual Cargo benchmark `wal_batch_encode` uses a borrowed, preconstructed
input and ordinary encoding. Setup, a decode/text identity check, complete
per-sample byte comparison and dropping output are outside its encode timer.
Baseline is runtime `21bdf14b` plus the exact same benchmark harness; candidate
is `ac43fe6b8`. Both storage-only builds use identical default features and the
repository global release profile. Frozen binaries use separate Cargo targets.
The ordinary embedded-query comparison below separately exercises prepared
encoding. These microseconds are not synchronized commit/query latency.

Five groups each use 201 immediate A/B or B/A pairs plus two warmups. All
readiness lengths/FNV-1a diagnostic checksums match across variants; every
encoded output matches its local expected bytes and both workers exit zero.
No allocation counter, CPU profiler or syscall tracer is installed in these
timed binaries. Complete raw records, hashes and order/half splits are retained.

| Workload / operations | Baseline p50 (us) | Candidate p50 (us) | Paired median change | Baseline / candidate p99 (us) |
|---|---:|---:|---:|---:|
| node-1 | 0.209 | 0.209 | +0.000% | 0.834 / 1.125 |
| node-128 | 7.292 | 2.625 | -59.986% | 13.417 / 5.291 |
| node-512 | 25.166 | 10.250 | -60.187% | 38.750 / 19.000 |
| relationship-4096 | 180.417 | 63.708 | -64.711% | 218.583 / 85.542 |
| large-small-128 | 66.416 | 63.000 | -4.817% | 149.541 / 90.750 |

`large-small` has a 1 MiB first binary property followed by small integers.
The single-operation median is unchanged, but its observed p95/p99/max are
higher in the candidate. The 128-operation candidate maximum is also higher
(19.958 versus 16.250 us). All samples remain included. Such nanosecond-scale
controls have clock granularity and scheduling limits; they do not establish
a query regression or clear no-regression acceptance. The worker exits after
its final reply, so final-step process cleanup can overlap the peer; these
diagnostic tails are not an isolated tail-qualification protocol. The embedded
query controller retains its separate finish barrier.

The 4096-operation paired median saving is approximately 0.117 ms, rather
than milliseconds. It cannot explain the earlier 6.743 -> 10.338 ms node-write
maximum change. No retrospective attribution is made.

## Embedded operation comparison and phase diagnostics

The following complete ordinary comparison has no CPU profiling, telemetry
sink or interposition. Both variants are v3/carry: baseline `21bdf14b` and
candidate `ac43fe6b8`. Four groups use 60 alternating pairs plus two warmups
each; all 480 measured operations and 16 warmups retain typed checks, scrub,
reopen and successful exits. Other compilation, Node and virtual-machine work
overlaps this window. It is shared-host evidence, excluded from idle acceptance.
[Raw pairs and receipts](https://github.com/nowledge-co/hawdb/blob/6c6d06d23cdbf8577903090cdf2065c814225de2/docs/benchmarks/pr1961/paired-wal-reuse-shared-suite/run-receipts.json) and
[complete distributions](https://github.com/nowledge-co/hawdb/blob/6c6d06d23cdbf8577903090cdf2065c814225de2/docs/benchmarks/pr1961/paired-wal-reuse-shared-suite/summary.json) are retained.

| Nodes / relationships | Mutation | Mutation paired median | Mutation baseline / candidate p50 (ms) | Mutation baseline / candidate p99 (ms) | Checkpoint paired median |
|---|---|---:|---:|---:|---:|
| 128 / 512 | node_property | +0.189% | 5.272 / 5.167 | 13.789 / 28.033 | +0.777% |
| 128 / 512 | relationship_property | -2.136% | 9.771 / 9.723 | 16.713 / 28.095 | +0.177% |
| 512 / 4096 | node_property | -2.770% | 7.529 / 7.423 | 16.154 / 29.267 | +0.170% |
| 512 / 4096 | relationship_property | -3.677% | 31.712 / 30.598 | 44.244 / 59.625 | -1.017% |

All four candidate mutation maxima increase; the small-node paired median
also increases. The larger-node half medians change sign. Small relationship
checkpoint max increases (957.415 -> 1092.447 ms), and the large relationship
checkpoint p95 increases (868.294 -> 919.109 ms). With 60 samples, reported p99
is the observed maximum. Do not remove these samples or attribute them to host
load merely because host load exists. No speedup is inferred across windows.


Ordinary compiler-idle query acceptance remains blocked by ongoing unrelated
compilation and high-CPU virtual-machine activity. The current diagnostic is separately instrumented with `cpu-profile`, an existing WAL
telemetry sink and the buffered syscall interposer. Other Bazel/Cargo/C++ builds
were active. All 24 pairs per group, two warmups, typed values/counts/carry checks,
scrub/reopen and worker exits succeed. Each mutation has exactly one WAL append;
checkpoints have none. This is attribution evidence, not latency acceptance.

| Workload | Baseline / candidate mutation wall median (ms) | CPU median (ms) | WAL fsync median (ms) |
|---|---:|---:|---:|
| node_property | 4.920 / 4.604 | 0.746 / 0.758 | 4.050 / 3.908 |
| relationship_property | 27.537 / 27.672 | 19.096 / 18.837 | 8.416 / 8.650 |

A captured slow candidate node mutation takes 7.0425 ms, with 5.622 ms of
WAL fsync and 1.018 ms process CPU. A baseline slow sample takes 8.2823 ms,
with 6.736 ms fsync and 1.482 ms CPU. These locate synchronous persistence
as a substantial component in these specific new samples. They do not identify
the cause of an earlier untraced outlier. Bulk relationship updates still have
substantial CPU cost. Candidate relationship wall median and fsync median
increase despite lower CPU median; small-node CPU median also increases. These
negative observations remain visible. CPU and fsync overlap and must not be
added as an exclusive wall-time partition. Counters are process-wide; observer
work and scheduling can perturb both variants. The reported wall-minus-fsync
field is a residual, not an isolated CPU measurement.

[All phase samples and distributions](https://github.com/nowledge-co/hawdb/blob/6c6d06d23cdbf8577903090cdf2065c814225de2/docs/benchmarks/pr1961/wal-reuse-profile-trace/profile-summary.json),
[per-call summaries](https://github.com/nowledge-co/hawdb/blob/6c6d06d23cdbf8577903090cdf2065c814225de2/docs/benchmarks/pr1961/wal-reuse-profile-trace/sync-summary.json), compressed raw
traces and [producer receipts](https://github.com/nowledge-co/hawdb/blob/6c6d06d23cdbf8577903090cdf2065c814225de2/docs/benchmarks/pr1961/wal-reuse-profile-trace/run-receipts.json) remain
separate from ordinary measurements. The optional instrumentation is confined
to the manual producer and child process; no new production control plane or
telemetry API is added.

[Earlier checkpoint measurements](SYNC_INVESTIGATION.md) qualify their own
runtime only. Existing foreground-tail acceptance remains open.

### Longer node-tail diagnostic

A separate 512-node / 4096-relationship node-property run completes 120 measured
pairs plus two warmups, with the same CPU/WAL telemetry and buffered tracer.
Every result, carry, scrub/reopen and exit check passes. The host remains busy;
this is attribution evidence, not a replacement for ordinary acceptance.

Baseline/candidate mutation wall medians are 6.916 / 7.023 ms;
CPU medians 3.200 / 3.175 ms; WAL fsync medians
3.779 / 3.753 ms. Candidate wall median is higher despite lower
CPU/fsync medians; these separately aggregated values are not additive.

At baseline pair 63, query wall is 123.530 ms, CPU 3.218 ms and typed WAL fsync
119.711 ms. The trace independently records one successful `F_FULLFSYNC` on
`wal.130.hawdb` lasting 119.658 ms within this same step. A checkpoint file
synchronization later in that step lasts 133.799 ms; it is not part of the query
timer. Candidate's worst query is 13.671 ms with 9.459 ms WAL fsync and 3.373 ms
CPU. Other candidate samples also contain elapsed time outside the recorded
WAL/CPU components; no exclusive scheduler/I/O breakdown is claimed. These
observations locate new slow calls without explaining the earlier uninstrumented
28-59 ms candidate maxima or establishing no-regression from reversed maxima.

Synchronization counts remain unchanged in all three instrumented groups:
122 -> 122 (small node), 131 -> 131 (large relationship), 122 -> 122 (longer
large-node run). This follow-up saves encoding allocations, not durability
calls. [Complete phase records](https://github.com/nowledge-co/hawdb/blob/6c6d06d23cdbf8577903090cdf2065c814225de2/docs/benchmarks/pr1961/wal-reuse-node-tail-profile/profile-summary.json),
[raw call summary](https://github.com/nowledge-co/hawdb/blob/6c6d06d23cdbf8577903090cdf2065c814225de2/docs/benchmarks/pr1961/wal-reuse-node-tail-profile/sync-summary.json), compressed
traces and [source/binary receipts](https://github.com/nowledge-co/hawdb/blob/6c6d06d23cdbf8577903090cdf2065c814225de2/docs/benchmarks/pr1961/wal-reuse-node-tail-profile/run-receipts.json)
are retained. No unfavorable observation is filtered.

## Correctness qualification

[Source-bound CI](https://github.com/nowledge-co/hawdb/blob/6c6d06d23cdbf8577903090cdf2065c814225de2/docs/benchmarks/pr1961/ci-wal-reuse.json) records all nine Prow jobs passing at
`ac43fe6b8ee51b50e4587c3509839832a7175c2d`, integrated with main `09579ce2`.
Root/crates/crash clone records agree on runner `8fa6d6b9d4f9d240b22038b2b63965d805606f11`.
Complete BEP distinguishes root 77 targets (62 executed,
15 cached), crates 106 (69 executed, 37 cached),
including a fresh storage unit execution at 238.410 s. The native allocation
bound and independent-codec regressions are included in that owner. An executed
crash target and its source identity are retained separately. These results do
not establish performance acceptance. All [39 GitHub/Prow checks](https://github.com/nowledge-co/hawdb/blob/6c6d06d23cdbf8577903090cdf2065c814225de2/docs/benchmarks/pr1961/checks-wal-reuse.json)
pass at this source, including Windows, native bindings, Node/Electron and iOS.
Independent approval is absent; later evidence-head checks remain separate. Native strict workspace Clippy, formatting and commit
hooks pass at the source revision. No stopped local Bazel/fuzz work is resumed.

## Replay

Build each source with the same harness in separate Cargo targets and freeze
the resulting binaries as `wal-encode-baseline` / `wal-encode-candidate`. The
current harness/controller add temporal and collection workloads and require
a final `finish` command after all timed pairs. Copy both from the same
revision when replaying; use this document's historical revision for the
original five-group protocol. Mixing historical binaries with the current
controller is not supported.

```sh
cargo build --locked --release -p hawdb-storage --bench wal_batch_encode -j 1
python3 docs/benchmarks/pr1961/run_wal_encode.py "$task_dir/bin" "$task_dir/encode-samples" 201
python3 docs/benchmarks/pr1961/analyze_wal_encode.py "$task_dir/encode-samples"
```

The diagnostic stays outside CI and production dependencies. Strict workspace
all-target Clippy checks its source. Build costs and runtime measurements are
reported separately; baseline build reused its existing target, while the
candidate encoding build populated an empty target. Neither proves a build
speedup or remote CI speedup.

Use `analyze_wal_phase.py` for the complete phase summary including every sample. Archived compressed traces require a scratch copy with their TSV files decompressed before using `analyze_sync_trace.py`.

The shared-host ordinary run uses `run_handoff.py` with the frozen v3/v3 pair.
The bounded diagnostic commands use a newly built native tracer from the
[synchronization recipe](SYNC_INVESTIGATION.md#replay):

```sh
python3 docs/benchmarks/pr1961/run_wal_profile_trace.py "$task_dir/bin" "$task_dir/phase-samples" 24 "$task_dir/trace.dylib"
python3 docs/benchmarks/pr1961/run_node_tail_profile.py "$task_dir/bin" "$task_dir/node-tail-samples" 120 "$task_dir/trace.dylib"
python3 docs/benchmarks/pr1961/analyze_wal_phase.py "$task_dir/node-tail-samples"
```
