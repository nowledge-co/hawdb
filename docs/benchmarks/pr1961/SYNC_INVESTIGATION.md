# Checkpoint synchronization investigation

This report covers its named historical revisions. See the [PR description](https://github.com/nowledge-co/hawdb/pull/1961) for current qualification and review gates.

Issue Number: ref #1960. PR: https://github.com/nowledge-co/hawdb/pull/1961.

## Findings and changes

The earlier [stack and phase investigation](CAUSE_INVESTIGATION.md) located a
substantial synchronous-persistence component, alongside CPU work in larger
relationship batches. Two additional avoidable operations were then confirmed
in the publication implementation:

1. Prepared root handoff reopened `ImmutableObjectStore`, discarding its
   completed namespace/kind synchronization state. Source `1a62156d` retains
   the same owner through completion. Retargeting the low-level public root
   path still opens a fresh owner. Final object-directory, content-authentication
   and head-publication barriers remain required.
2. A fresh, exclusively owned sealed producer was linked into staging, verified
   and synchronized, then linked into its final object namespace. Removing the
   staging name also required synchronizing staging. Source `21bdf14b` installs
   a fresh Unix sealed producer directly after full hashing and file sync. It
   verifies that the final entry names the held source inode, checks the source
   stamp, releases the source descriptor and synchronizes the final directory.
   No staging name is created. Uncertainty poisons the publisher and retains
   the possibly foreign final entry as evidence. Public caller-owned sources
   still copy, other platforms retain staging, and cross-device/unsupported
   links retain the copy fallback. A device mismatch falls back before hashing;
   a kernel-rejected link may already have verified the source.

Neither change weakens the default synchronous durability mode or skips
existing-object verification. No extra WAL, new persistence format, deferred
directory batching, global synchronization cache or production tracing API is
introduced by these two follow-ups. They do not make total checkpoint work
proportional to dirty data.

## Ordinary measurements

Both component comparisons use v3 on both sides, `carry` mode, four groups,
60 immediate A/B or B/A pairs per group and two warmups. Relationship-property
controls still rebuild adjacency. All typed value/count/carry checks, scrub,
reopen and worker exits succeed. No profiling argument, telemetry sink or
interposition library is enabled in these ordinary runs. The exact
[component controller](run_handoff.py), raw data, binary identities and complete
order/half/tail statistics are retained.

The handoff comparison is `6a3781141` versus `1a62156d`, with no compiler/test
load in sampled host entries, but ordinary desktop/application activity and no
continuous isolation. The direct comparison is `1a62156d` versus `21bdf14b`.
Other sessions started Clippy during its latter groups; the entire direct suite
is retained as mixed-load evidence and is excluded from final acceptance.
These are separate windows; do not add their percentage changes.

### Handoff owner reuse

[Raw records and receipts](https://github.com/nowledge-co/hawdb/blob/6c6d06d23cdbf8577903090cdf2065c814225de2/docs/benchmarks/pr1961/paired-handoff-suite/run-receipts.json); [all distributions](https://github.com/nowledge-co/hawdb/blob/6c6d06d23cdbf8577903090cdf2065c814225de2/docs/benchmarks/pr1961/paired-handoff-suite/summary.json).

| Nodes / relationships | Mutation | Checkpoint paired median | Checkpoint baseline / candidate p99 (ms) | Mutation paired median | Mutation baseline / candidate p99 (ms) |
|---|---|---:|---:|---:|---:|
| 128 / 512 | node_property | -2.248% | 662.739 / 780.200 | -1.245% | 5.656 / 6.056 |
| 128 / 512 | relationship_property | -0.232% | 491.035 / 543.515 | +1.016% | 8.907 / 8.890 |
| 512 / 4096 | node_property | -0.435% | 1010.197 / 571.948 | -1.416% | 9.066 / 8.325 |
| 512 / 4096 | relationship_property | -0.242% | 519.255 / 861.183 | -0.396% | 28.243 / 29.768 |

### Direct sealed publication (mixed host load)

[Raw records and receipts](https://github.com/nowledge-co/hawdb/blob/6c6d06d23cdbf8577903090cdf2065c814225de2/docs/benchmarks/pr1961/paired-direct-suite/run-receipts.json); [all distributions](https://github.com/nowledge-co/hawdb/blob/6c6d06d23cdbf8577903090cdf2065c814225de2/docs/benchmarks/pr1961/paired-direct-suite/summary.json).

| Nodes / relationships | Mutation | Checkpoint paired median | Checkpoint baseline / candidate p99 (ms) | Mutation paired median | Mutation baseline / candidate p99 (ms) |
|---|---|---:|---:|---:|---:|
| 128 / 512 | node_property | -1.891% | 789.812 / 600.721 | -2.209% | 6.610 / 10.682 |
| 128 / 512 | relationship_property | -2.831% | 521.219 / 467.977 | -1.053% | 8.670 / 8.408 |
| 512 / 4096 | node_property | -2.313% | 656.563 / 613.095 | +1.060% | 8.604 / 8.030 |
| 512 / 4096 | relationship_property | -1.897% | 642.369 / 701.338 | -0.206% | 26.915 / 27.291 |


With 60 observations, nearest-rank p99 equals the observed maximum. Complete
unfavorable samples remain in both datasets. In particular, direct publication
does not clear foreground/tail acceptance despite lower checkpoint medians.

## Full PR comparison after the synchronization fixes

The original v2 baseline (`5efcc9a2` plus frozen profiling-capable harness,
profiling omitted) is compared with v3 runtime `21bdf14b`. Both use identical
SyncOnEveryWrite, OutOfCore and disabled automatic maintenance settings. The
original `run_paired.py` controller completes all four groups, 60 measured pairs
and two warmups each; all 480 measured operations and 16 warmups retain typed
checks, scrub/reopen and successful exits. [Raw records and receipts](https://github.com/nowledge-co/hawdb/blob/6c6d06d23cdbf8577903090cdf2065c814225de2/docs/benchmarks/pr1961/paired-full-direct-idle-suite/run-receipts.json) and [all statistics](https://github.com/nowledge-co/hawdb/blob/6c6d06d23cdbf8577903090cdf2065c814225de2/docs/benchmarks/pr1961/paired-full-direct-idle-suite/summary.json) remain separate from component runs.

| Nodes / relationships | Mutation | Checkpoint baseline / candidate p50 (ms) | Checkpoint paired median | Checkpoint baseline / candidate p99 (ms) | Mutation paired median | Mutation baseline / candidate p99 (ms) |
|---|---|---:|---:|---:|---:|---:|
| 128 / 512 | node_property | 444.406 / 387.801 | -12.921% | 555.573 / 418.854 | -1.034% | 5.281 / 5.778 |
| 128 / 512 | relationship_property | 448.266 / 441.272 | -2.013% | 685.031 / 560.884 | -0.155% | 9.037 / 7.517 |
| 512 / 4096 | node_property | 484.138 / 403.233 | -16.853% | 559.250 / 460.650 | -0.580% | 6.743 / 10.338 |
| 512 / 4096 | relationship_property | 506.268 / 457.865 | -9.301% | 853.307 / 553.861 | -0.267% | 20.679 / 20.164 |

All checkpoint paired medians and observed maxima decrease in this window.
Foreground paired medians are slightly lower, but both node-mutation maxima
increase: 5.281 -> 5.778 ms and 6.743 -> 10.338 ms. The larger relationship
mutation p95 is also higher (19.575 -> 19.910 ms). Half/order splits and every
sample remain visible. This does not clear the no-regression gate. The PR
remains Draft; checkpoint improvements do not waive foreground tails.

No compiler/test load appeared in early sampled host entries. A sustained
high-CPU Java process appeared during the later window, owned by a separate
Go-built developer tool; its workload was not identified. Thus the directory
name `paired-full-direct-idle-suite` describes the intended window, not proof
of continuous isolation. The small node group completed earlier, so its
mutation maximum cannot be explained by that later Java observation. None of
these outliers has a per-call trace; do not assign their cause retrospectively.

The frozen baseline harness has a portable no-op difference, recorded in
[earlier provenance](https://github.com/nowledge-co/hawdb/blob/6c6d06d23cdbf8577903090cdf2065c814225de2/docs/benchmarks/pr1961/profile-source-metadata.json). Local source comparisons
remain based on `5efcc9a2`; CI integrates main `7aa7b50c`. At final observation,
main had advanced to `6e2ff3df` through a MoonBit binding change. These local
measurements are not exact-current-main runtime qualification.

## Per-call diagnostic evidence

The manual macOS arm64 tracer uses a Mach-O interposition table and an assembly
tail-forwarder for unrecognized `fcntl` commands, preserving Darwin's variadic
argument ABI. The probe covers no-argument, scalar and pointer commands,
successful full-sync/fsync and EBADF preservation. The safe buffered build
produced three probe records. This is an unpublished diagnostic helper, with
no production dependency or CI lane. Interposition layout was checked against
[Apple's primary header](https://raw.githubusercontent.com/apple-oss-distributions/dyld/main/include/mach-o/dyld-interposing.h).

An initial immediate-write logger dirtied the filesystem between barriers,
causing later full-sync calls to include logger I/O. Its timing results are
**rejected**, including every sample in [the initial trace](https://github.com/nowledge-co/hawdb/blob/6c6d06d23cdbf8577903090cdf2065c814225de2/docs/benchmarks/pr1961/handoff-sync-trace/run-receipts.json).
Its raw logs are retained, not used for timing claims. Buffered versions store
bounded events/paths in RAM and write at worker exit only after all timed steps
finish. Their extra path/stat/memory work still perturbs execution; neither
their wall times nor syscall durations are performance acceptance evidence.
Each diagnostic uses 24 pairs for small node and large relationship workloads.

| Component | Small node step sync calls | Large relationship step sync calls |
|---|---:|---:|
| Handoff owner reuse | 144 -> 138 | 156 -> 150 |
| Direct sealed publication | 138 -> 122 | 150 -> 131 |

Counts cover the complete synchronized mutation/checkpoint step, not checkpoint
alone. They show removed calls without proving a proportional latency gain.
Indeed direct-trace median summed sync duration changes from 344.913 to
346.415 ms for the small workload and 386.886 to 396.545 ms for the large one,
despite fewer calls. Both controls and negatives remain in the reports.
Typical paths include object-file synchronization, final object directories,
staging cleanup and mutable snapshot namespace barriers. The doubled
`objects/objects` component reflects the actual store layout.

[Trace/source hashes and host limitations](https://github.com/nowledge-co/hawdb/blob/6c6d06d23cdbf8577903090cdf2065c814225de2/docs/benchmarks/pr1961/sync-source-metadata.json),
[handoff buffered records](https://github.com/nowledge-co/hawdb/blob/6c6d06d23cdbf8577903090cdf2065c814225de2/docs/benchmarks/pr1961/handoff-sync-trace-memory/run-receipts.json),
[direct buffered records](https://github.com/nowledge-co/hawdb/blob/6c6d06d23cdbf8577903090cdf2065c814225de2/docs/benchmarks/pr1961/direct-sync-trace-memory/run-receipts.json) and the
complete compressed TSV logs preserve exact producers. The first buffered
binary came from `trace-memory-captured.c`; the safe successor adds shutdown
disabling before freeing its buffers. Their binary hashes differ. No executable
binaries are committed.

## Correctness and CI

Handoff regression cases cover reuse of completed directory barriers, failure
of a still-required final barrier, root-path retargeting and same-length,
restored-mtime corruption. Each checks complete recovery. Direct-publication
regressions cover staging independence while retaining the final barrier,
source replacement by a different inode with identical bytes/restored mtime,
and PowerLossModel observations before/after the final directory barrier.
They retain foreign evidence and distinguish a lost name before the barrier
from complete durable payload after it. Broader lost/torn/reordered root
publication cases remain with their existing owners. The model's filesystem
synchronization assumptions remain distinct from physical device validation.

[Source-bound CI receipts](https://github.com/nowledge-co/hawdb/blob/6c6d06d23cdbf8577903090cdf2065c814225de2/docs/benchmarks/pr1961/ci-direct.json) record nine successful Prow jobs at
`21bdf14b44fa882257b0cab39e432e86b179b0d9`, integrated with main `7aa7b50c`.
Root, crates and crash clone records agree on runner
`40e8b10a74e12cc691249380fcc4d9bca0ef0722`. Complete BEP records distinguish
root 77 targets (61 executed, 16 cached), crates 106 (69 executed, 37 cached),
storage units executed in 301.883 s, and an executed crash target (6.333 s).
Crash evidence independently reports that runner and ten complete recovery
cases. Passing correctness CI does not establish performance acceptance.
All 39 passing GitHub/Prow checks at this runtime include native bindings, Node/Electron, Windows and iOS. Independent approval is still absent; subsequent documentation-head checks are separate.

Native strict workspace Clippy, formatting and hooks passed before each source
commit. The direct release rebuild took 5m34s using its existing isolated
target; this is not first-build cost or a build-speedup comparison.

## Replay

Rebuild each frozen source in its own Cargo target and freeze matching binaries
as `embedded-paired-baseline` / `embedded-paired-candidate`. Use new fixture
roots. Component sources are listed above; full PR comparison uses the
original `run_paired.py` v2/v3 controller. For a component comparison:

```sh
python3 docs/benchmarks/pr1961/run_handoff.py "$task_dir/bin" "$task_dir/smoke" 1 smoke
python3 docs/benchmarks/pr1961/run_handoff.py "$task_dir/bin" "$task_dir/samples" 60
python3 docs/benchmarks/pr1961/analyze.py "$task_dir/samples"
```

The trace requires native macOS arm64. Build the safe helper and probe before
running the bounded diagnostic; it is never an acceptance command:

```sh
clang -dynamiclib -O2 -Wall -Wextra -Werror docs/benchmarks/pr1961/sync-trace/trace-memory.c docs/benchmarks/pr1961/sync-trace/forward.S -o "$task_dir/trace.dylib"
clang -O2 -Wall -Wextra -Werror docs/benchmarks/pr1961/sync-trace/probe.c -o "$task_dir/probe"
mkdir "$task_dir/probe-records"
HAWDB_SYNC_TRACE_DIR="$task_dir/probe-records" DYLD_INSERT_LIBRARIES="$task_dir/trace.dylib" "$task_dir/probe" "$task_dir/probe-source"
python3 docs/benchmarks/pr1961/run_sync_trace.py "$task_dir/bin" "$task_dir/trace-samples" 24 "$task_dir/trace.dylib"
python3 docs/benchmarks/pr1961/analyze_sync_trace.py "$task_dir/trace-samples"
```

For the archived trace datasets, decompress their `sync-trace/*.tsv.gz` into
a scratch copy before invoking the exact analyzer. Ordinary summaries recompute
directly with `analyze.py`; do not mix traced and ordinary populations.
