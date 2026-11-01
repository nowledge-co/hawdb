# Bounded physical CASE templates, macOS, 2026-10-31

[PR #1378](https://github.com/nowledge-co/hawdb/pull/1378) implements
[issue #1362](https://github.com/nowledge-co/hawdb/issues/1362). The baseline is
`0f611b6decbebd8354f2a30889e03c48287d18d2`; the candidate is
`93661914cc37e4ad988dc8bb6db9e5c46e5acfec`. Both compile the identical
[manual harness](../../../benches/physical_plan_cache.rs), SHA-256
`3cd43e3f114164a04ad9c4ee681968fc3c09870c2e914549cab0c6a822ef8119`.
The baseline overlay adds only that harness and its Cargo bench registration.

The candidate charges the complete physical cache owner before weighted LFU
admission, defaults to 16 MiB per cache and 8 MiB per entry, and permits eligible
CASE templates to reuse physical plans. A payload that cannot fit with even the
minimum table allocation is refused before replacing a hot entry. No row-goal,
NDV, width-cost, storage-format or Mem release-policy change is included here.

## Procedure and measurement boundaries

The host was an Apple M3 Max, 16 logical CPUs, 128 GiB RAM, Darwin 27.0.0,
`aarch64-apple-darwin`, pinned Rust 1.97.1 / LLVM 22.1.6. Builds use the unchanged
repository release profile: optimization 3, thin LTO, one codegen unit, debug
assertions and overflow checks disabled. Toolchain, configuration, lockfile,
harness and executable hashes are recorded in [provenance.json](provenance.json).

Each process creates two synthetic spaces with 512 or 8,192 memories each, an
ID index, and 64-byte or 4,096-byte bodies. The query selects 20, 100 or 512 IDs
from one space and returns ID, body and five parameterized CASE projections.
All five synthetic score columns are floating point. This models the CASE
query phase; host ranking policy and scorer callbacks are outside the fixture.
The caller supplies explicit row and payload budgets. Every request checks all
IDs, complete bodies, all five score bit patterns and released query memory.
One score changes on every measured request.

Each process executes one cold call, three warmup calls and 31 measured calls.
The twelve reuse cells cover all cohort/scope/body combinations. Three fresh
read-handle controls drop the warmup handle and create a new handle per call.
Database-level logical/catalog state follows normal API lifetimes. Each read
handle has a private physical cache.

Three independent plain-release process pairs per cell counterbalance revision
order across cells and rounds. The reported p50 is the median of the three
process medians; complete samples and nearest-rank p95 observations are in
[results.json](results.json). A separate qualification-feature pair per cell
observes Rust allocation traffic and process-wide preparation counters. These
instrumented times are reported separately from ordinary release latency.
There are 120 processes, each validating 35 complete query results.

The timer surrounds the streaming API call and owned-row delivery. Fixture
setup, read-handle creation, post-call result assertions and destructor work
are outside that timer. Allocation observations cover the measurement loop,
including parameter updates, result assertions and harness storage. Preparation
includes cache lookup, binding and optimization on misses; parsing is a separate
counter. These measurements do not isolate pure optimizer CPU time.

All task-owned builds and tests finished before the accepted measurement window,
2026-10-30 19:05:31–19:06:44 UTC. The host is shared; process snapshots observed
zero external Rust compiler processes at the start and end, and each process
records the host load average. This is ARM64 local evidence. It does not establish
x86 performance, remote CI speed, persistence throughput or Mem endpoint latency.

To replay, build both revisions with the same harness using:

```sh
cargo build --locked --release --bench physical_plan_cache --message-format=json
cargo build --locked --release --features qualification --bench physical_plan_cache --message-format=json
```

Use the `compiler-artifact.executable` paths from each build, preserving the
ordinary binary before building the qualification variant. Run one cell per
process, for example:

```text
<ordinary-executable> 512 512 64 31 reused
<ordinary-executable> 512 512 64 31 fork
<qualification-executable> 512 512 64 31 reused
```

Keep build output outside the repository. The full matrix, ordering, arguments,
timestamps, profiles, executable hashes and structured observations are retained
in the two JSON files here. Raw compiler logs, BEP, process snapshots and profiler
output remain outside Git.

## Ordinary release results

The scope column is rows per space; total stored rows are twice that value.
Ratios below one favor the candidate.

| Candidates | Scope rows | Body bytes | Handle | Baseline p50 ms | Candidate p50 ms | Candidate/baseline |
| ---: | ---: | ---: | --- | ---: | ---: | ---: |
| 20 | 512 | 64 | reused | 0.500 | 0.253 | 0.507 |
| 20 | 512 | 4096 | reused | 0.506 | 0.258 | 0.510 |
| 20 | 8192 | 64 | reused | 0.500 | 0.252 | 0.504 |
| 20 | 8192 | 4096 | reused | 0.511 | 0.260 | 0.508 |
| 100 | 512 | 64 | reused | 2.416 | 1.163 | 0.481 |
| 100 | 512 | 4096 | reused | 2.459 | 1.191 | 0.484 |
| 100 | 8192 | 64 | reused | 2.447 | 1.159 | 0.473 |
| 100 | 8192 | 4096 | reused | 2.476 | 1.216 | 0.491 |
| 512 | 512 | 64 | reused | 17.440 | 8.022 | 0.460 |
| 512 | 512 | 4096 | reused | 17.692 | 8.221 | 0.465 |
| 512 | 8192 | 64 | reused | 16.415 | 6.887 | 0.420 |
| 512 | 8192 | 4096 | reused | 16.782 | 7.179 | 0.428 |
| 20 | 512 | 64 | fresh per call | 0.497 | 0.512 | 1.031 |
| 512 | 512 | 64 | fresh per call | 17.301 | 17.812 | 1.029 |
| 512 | 512 | 4096 | fresh per call | 18.659 | 17.984 | 0.964 |

Every candidate reuse process records 31 physical hits; baseline CASE processes
record 31 bypasses. Fresh-handle candidate processes record 31 physical misses.
All 120 processes pass complete result parity and cache-counter assertions.

Cold medians increased by 0.1–5.5% across the twelve reuse cells. The small-scope,
64-byte-body cold calls were 2.620→2.717 ms at 20 candidates,
32.120→33.050 ms at 100, and 747.049→759.600 ms at 512. Each cold median has
three observations. First-use logical preparation remains expensive at 512;
this change targets subsequent physical reuse.

## Preparation and memory observations

These rows use the separately instrumented small-scope, 64-byte-body reuse cells.
Allocation traffic includes the loop's parity checks as described above.

| Candidates | Preparation mean ms, baseline→candidate | Allocated MB/call, baseline→candidate |
| ---: | ---: | ---: |
| 20 | 0.396→0.142 | 2.46→0.67 |
| 100 | 1.931→0.666 | 11.99→3.45 |
| 512 | 13.185→3.446 | 61.06→18.61 |

The incremental process-owned Rust requested heap after the first 512-candidate
call rose from approximately 5.23 MB to 10.51 MB in this small-scope fixture.
That observation includes retained planning/cache state. It is separate from the
independent complete-cache allocation oracle below, whose keys also exercise
scoring metadata and spare capacity:

| CASE candidates | Actual retained requested bytes | Conservative charged bytes |
| ---: | ---: | ---: |
| 20 | 217,320 | 247,682 |
| 100 | 1,020,680 | 1,163,826 |
| 512 | 5,173,536 | 5,892,762 |

The 512-candidate fixture fits the default 8 MiB per-entry limit. The observers
also exercise byte-driven eviction, nested/shared values, list expressions and
complete release. Charges include table spare capacity and charge shared owners
in full per reference. Allocator rounding, fixed cache metadata, caller-owned
clones and concurrent misses are separate. Limits apply per physical cache and
do not define a process RSS limit. The shared logical binding cache has its own
budget; parsed relational SQL templates remain a separate count-limited owner.

## Qualification and build cost

The final source passes pinned formatting, strict workspace/all-target/all-feature
Clippy and staged pre-commit hooks. Required local Shiro/fuzz-smoke, historical
aliases, cache owners and ACL selection pass: 101 targets, 27 executed and 74
cached. The root default general selection passes 128 Rust cases; the supported
minimal cache/CASE/host-scoring/observability/list selection passes 120. Native
cache unit coverage passes 47 cases plus the independent allocation oracle.
Minimal browser WASM strict Clippy passes. The final 30-second MVCC campaign
passes 50,084 cases. [TEST_INDEX](../../TEST_INDEX.md) gives owners and replay.

The complete fuzz command took 1,159.6 s within Bazel, with a 510.1 s critical
path including exclusive qualification. Cached targets retain historical result
durations in provenance and are marked as cached. The final root action took
37.5 s including inventory and 31.46 s for Rust cases; Bazel took 66.2 s after
the command acquired the server. Its 671.0 s caller duration includes 604.8 s
serialized behind the fuzz command. These are local scheduling/build/test
observations.

The first completed full release-harness builds observed during this task took
10m59s baseline and 12m41s candidate, using existing dependency caches while
qualification builds were running. A clean-cache build was not measured. Final
incremental candidate ordinary/qualification builds took 128.6/120.9 s; the
corresponding baseline builds took 110.5/85.3 s. All use the repository profile.

An additional broad minimal `scoring` selection included four fixtures requiring
disabled full-text or graph-analytics capabilities; those requests refused the
missing capabilities. Supported minimal owners and the default-feature scoring
selection pass. Full-tree `prek --all-files` exposes the documented historical
license-header debt; staged headers, formatting and strict Clippy pass.

## Conclusions

Eligible CASE physical reuse approximately halves warm API latency in these
reused-handle fixtures, with lower observed preparation and allocation work.
It retains additional bounded heap and adds some cold-call work. Fresh handles
do not share physical hits and show no stable speedup. Mem benefits therefore
depend on its read-handle lifetime; this report makes no endpoint-wide claim.
The byte cap provides a separate retention contract, including safe oversized
refusal and explicit rejection of opaque compiled-regex ownership.

## Current-main IR coverage follow-up

Runtime `84a373641cdadac74c1e4ac08ac1b2721d603c1b` integrates main
`37bfb4bca8c1296f41eb0d521f2602bd7853459e` (0.22.0 / shortest-path expressions).
It adds exhaustive requested-heap accounting for path functions, path columns
and boxed shortest-path descriptors, including their spare string capacities.
The existing independent facade allocation oracle now retains every path-function
form and both Any/All selections in the actual cache owner and verifies that
tracked allocations are released after drop. The existing shortest-path query
regressions remain intact.

This follow-up passed pinned fmt, workspace/all-target/all-feature strict Clippy,
staged hooks and minimal browser WASM strict Clippy. Cargo default selection
passed 158 tests, supported minimal selection passed 111, the allocation/path
selection passed 17, and the Bazel general selection passed 156. The required
local Bazel closure passed 101 tests (94 executed, seven cached); MVCC passed
3,368 cases in 30,081 ms. Full-tree HawkEye currently reports 616 historical
header failures; changed-file hooks pass. Test profiles, assertions and deadlines
remain unchanged.

The timing/heap traffic comparison above remains bound to its original baseline,
runtime and binaries. This compatibility follow-up does not report a new CASE
latency measurement or a shortest-path speedup.
