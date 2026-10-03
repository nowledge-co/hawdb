# Concurrent SQL snapshot read benchmark

`benches/concurrent_snapshot_reads.rs` measures the public embedded SQL path for
[#226](https://github.com/nowledge-co/hawdb/issues/226). It complements the
sequential `relational_oltp_mix` and writer-only `wal_group_commit` benchmarks.
It does not change either workload or their CI dispatch.

## Workload and interpretation

The release fixture contains 128 logical message threads with 128 messages each,
using a Mem-shaped subset of `thread_messages`: a text primary key, thread key,
order index, 1,024-byte constant content and token count. A composite index supports
ordered message pages. This is a synthetic, highly compressible SQL fixture; it
is not the complete Mem schema, a captured production corpus or an out-of-core
capacity qualification.

Each of the 13 cases copies the same closed checkpoint fixture and reopens it
with default database admission and `SyncOnEveryWrite` durability. Group commit
keeps its disabled default. Two validated read requests initialize the point and
page plans before timing. The timed cases are:

| Case | Readers | Work per reader | Writer work |
| --- | --- | --- | --- |
| Writer only | 0 | None | 512 independent inserts |
| Point reads | 1 / 4 / 8 | 512 primary-key lookups | None or 512 inserts |
| Ordered pages | 1 / 4 / 8 | 256 pages of 32 complete rows | None or 512 inserts |

The writer inserts into a separate logical thread, so the original reader rows
remain exactly checkable. Workers start at a barrier and finish their fixed
work; this is not a fixed-duration steady-state load. Reader/writer populations
can shrink as workers finish. Every request remains in the report, including the
first write and data-cache misses. No cache flushing or cold-device claim is made.

Request latency includes the facade call, planning/snapshot acquisition,
execution and completion recording. It excludes request parameter construction
and result assertions. Phase throughput uses the first worker start through the
last worker finish, including between-request normalization and full result
validation. These two measurement scopes must not be conflated.

Reports retain every request's start offset and duration, all worker lifetimes,
nearest-rank p50/p95/p99, total throughput and the number of read requests wholly
inside the writer's lifetime. Request lifetime overlap can include waiting and
**does not establish simultaneous engine execution**. The regression tests in
[PR #483](https://github.com/nowledge-co/hawdb/pull/483) separately prove that
completion recording can finish while a real writer holds the commit sequencer.
The current `ConcurrentDatabase` implementation also captures new read snapshots
from an independent committed publication. Channel-controlled regressions start
readers while a writer holds the sequencer and while a group awaits its durability
barrier. These tests prove progress and visibility, not latency acceptance. The
September 14 measurements below predate this acquisition change.

Every returned ID, order, payload and token count is checked. After each case,
all database handles close and the database reopens through normal WAL recovery.
The harness checks every original and inserted row, total cardinality and exact
commit epoch. Recovery checks and fixture copy/open/cleanup are outside timing.

## Running and comparing revisions

```sh
cargo bench --bench concurrent_snapshot_reads
bazel run //:hawdb_bench_concurrent_snapshot_reads
```

The Bazel binary is manual and outside the existing CI benchmark dispatch. A
debug build uses a 4-by-16-row fixture, eight point requests or four 16-row pages
per reader, and eight writes. It still executes all 13 cases and complete recovery
checks. Debug output is marked `smoke: true` and is not performance evidence.

The benchmark creates uniquely named owned directories beneath the process's
temporary directory. For durable-storage measurements, place that directory on
the intended filesystem rather than tmpfs. A benchmark child process's `TMPDIR`
may select fixture placement; it is not a production database control plane.

Build both revisions from identical harness bytes, features and release settings
before measuring. Keep all builds/tests terminal during timing. Use alternating
fresh-process pairs, preserve every run and raw sample, record source/binary
hashes and host/filesystem context, and compare the same case across revisions.
Do not infer a general speedup, read admission guarantee, writer scalability,
Cypher result, or single-stream latency acceptance from one host or a smoke run.

## Qualification status

The debug public-path matrix and complete recovery checks pass. Three deliberate
harness faults produce assertion failures: an omitted durable write, a wrong
middle inserted payload, and a wrong stored order index. The earlier three-column
oracle accepted that order-index fault; direct assertions of all five stored
columns now reject it. Restoring the exact source restores the complete
passing matrix. An independent report reconstruction checks all 13 cases,
operation counts, worker/request intervals, quantiles, overlap and throughput.
The final benchmark-owner Clippy check, formatting and whitespace checks pass.
The existing default-feature library dead-code warning was not suppressed.
The final required local fuzz selection passes 78/78 targets, all from cache;
the new manual benchmark was separately executed through Bazel.

## Linux comparison: September 14, 2026 (Asia/Shanghai)

The [paired recording](CONCURRENT_SNAPSHOT_READS_LINUX_RECORDING.json) contains
all six process runs, 78 case summaries, worker lifetimes/quantiles, process
resource usage and raw-output checksums. All 141,312 timed requests and 1,299,456
rows examined by the complete recovery checks passed. Raw per-request outputs,
compiler artifacts and binaries remain in `target/benchmarks/226-concurrent-reads`.

- Baseline engine: `366828ec4133d10aed3b300d93cd2e04bfde5b74`.
- Candidate engine: PR #483, `4e472647baf3a7b8cc249e9faead47773b6400a3`.
- Identical harness SHA-256: `ad7cb12e8197dbc0d82be95e5125e81d9b498562bd6123b094f0d13fd1dc4365`.
- Rust 1.97.1, default features, opt-level 3, thin LTO, one codegen unit;
  matching dependency lockfiles. Both builds completed before measurement.
- AMD Ryzen 7 7735HS, 16 available logical CPUs, Linux 7.1.10-zen,
  NVMe/Btrfs fixture placement. Normal desktop file indexing remained active.
- Process order: baseline/candidate, candidate/baseline, baseline/candidate.
  No agent-started builds or tests overlapped measurement. All six original
  processes finished; no run or case was discarded.

Each cell below is the median of the three corresponding process-level metrics,
not a pooled request percentile. Latencies are microseconds; throughput includes
all timed read/write requests over the entire fixed-work phase.

| Case | Requests/s, baseline -> candidate | Read p95 us, baseline -> candidate | Write p95 us, baseline -> candidate |
| --- | --- | --- | --- |
| Writer only | 78.5 -> 91.7 | - | 16,986.6 -> 17,307.1 |
| Point, 1 reader | 2,695.3 -> 2,920.5 | 2,204.6 -> 2,116.8 | - |
| Point, 1 reader + writer | 136.1 -> 141.7 | 2,270.3 -> 2,762.5 | 29,053.7 -> 32,533.8 |
| Point, 4 readers | 22,159.6 -> 22,218.7 | 174.7 -> 150.7 | - |
| Point, 4 readers + writer | 273.4 -> 491.4 | 223.1 -> 191.3 | 66,370.1 -> 15,361.9 |
| Point, 8 readers | 38,744.9 -> 44,844.1 | 227.7 -> 139.7 | - |
| Point, 8 readers + writer | 714.8 -> 768.9 | 273.7 -> 192.1 | 20,370.3 -> 28,120.6 |
| Page, 1 reader | 638.1 -> 624.5 | 3,444.5 -> 3,619.0 | - |
| Page, 1 reader + writer | 127.7 -> 102.6 | 55,318.5 -> 4,494.6 | 16,066.2 -> 21,851.5 |
| Page, 4 readers | 3,095.2 -> 3,053.9 | 2,714.2 -> 2,862.5 | - |
| Page, 4 readers + writer | 270.6 -> 225.6 | 3,701.0 -> 3,770.9 | 13,943.6 -> 27,812.0 |
| Page, 8 readers | 5,488.4 -> 5,585.2 | 1,764.8 -> 1,701.5 | - |
| Page, 8 readers + writer | 308.7 -> 341.6 | 2,765.0 -> 2,834.8 | 42,969.2 -> 29,446.7 |

The eight-reader, read-only point case improves in all three pairs: throughput
ratios are 1.099, 1.157 and 1.151; read p95 changes are -31.7%, -38.7% and -23.9%.
That observation is specific to this case. Mixed cases and write latency vary
substantially, and the table does not support a general throughput claim.

Single-writer p95 changes by +39.9%, +22.3% and -79.8% in the three pairs. The
median p95 is 16,986.6 -> 17,307.1 us, but that small median difference conceals
large pair-to-pair variation. The cause of that variation is not established.
The **single-stream no-regression criterion remains unproved**; no new tolerance
or admission threshold is selected from these samples.

The read-only phases last 84.5-419.4 ms in these runs; three pairs
do not establish a stable latency-admission threshold. The reader-count cases
add fixed work per worker and warm data caches during execution. Their cross-case
ratios are not a strict strong-scaling experiment.
Compare the same case across revisions; do not interpret apparent superlinear
ratios as a general engine scaling result.

#226 remains open for governed read concurrency, broader representative latency
evidence and the remaining #231/#232 acceptance criteria. PR #483's deterministic
completion-progress proof and its historical SQL measurement have separate scopes
from the current acquisition/publication implementation.

## Linux read-acquisition comparison: October 3, 2026

The [publication recording](CONCURRENT_READ_PUBLICATION_LINUX_RECORDING.json)
preserves two complete campaigns, each containing three alternating fresh-process
pairs and all 78 case summaries. Each campaign validates 141,312 timed requests
and 1,299,456 rows in full recovery scans. Raw samples and preserved binaries are
in `target/benchmarks/226-read-publication` and
`target/benchmarks/226-read-publication-atomic` respectively.

Both campaigns compare main `606e308bdf888fa94b50e9e0e501b2d1934b4886` against the
independent read-publication implementation, using identical harness bytes
(SHA-256 `bae86e60026d86161062eff23d560b1559eae4ab4aa342d633fd8c6714958490`).
The recording includes candidate source hashes, binary hashes, process resource
usage, raw-output checksums, and every case summary. Rust 1.97.1, default Cargo
features, opt-level 3, thin LTO, and one codegen unit are fixed. The persistent
fixture uses `SyncOnEveryWrite` with group commit disabled, on NVMe/Btrfs under
`target`, on an AMD Ryzen 7 7735HS with 16 logical CPUs and Linux 7.1.10-zen1-1-zen.
This is a shared workstation, not an isolated performance host.

The first implementation acquired the publication mutex again for completion
health checks. Its eight-reader read-only page p95 regressed in all three pairs
(+11.1%, +7.2%, +4.7%). The final implementation replaces those repeated
acquisitions with a sticky atomic failure flag and existing store health checks.
The first campaign retained an idle Bazel server; that workspace server was
stopped before the second campaign. No agent-started build or test overlapped
either campaign. These are separate cohorts, not a controlled estimate of the
atomic flag's contribution; do not pool them or attribute the between-cohort
variation to that change alone. No run was discarded or replaced.

An initial build attempt reused a stale artifact across source directories. The
identical binary hash exposed the mistake before timing; that artifact was not
measured as a candidate. Rebuilding the package and verifying distinct hashes
and the new implementation marker established the candidate binaries used here.

The following final-implementation results are medians of three process-level
metrics, not pooled request percentiles. Latencies are microseconds; throughput
includes all timed requests in each fixed-work phase.

| Case | Requests/s, baseline -> candidate | Read p95 us, baseline -> candidate | Write p95 us, baseline -> candidate |
| --- | --- | --- | --- |
| writer-only | 86.1 -> 75.4 | - | 13,137.8 -> 22,596.3 |
| point-1-readers-writer-false | 2,592.5 -> 2,819.0 | 2,417.0 -> 2,193.6 | - |
| point-1-readers-writer-true | 208.6 -> 154.2 | 9,865.1 -> 2,407.8 | 12,776.7 -> 16,190.8 |
| point-4-readers-writer-false | 19,870.3 -> 23,278.1 | 182.0 -> 136.3 | - |
| point-4-readers-writer-true | 343.7 -> 400.3 | 210.7 -> 189.3 | 28,941.7 -> 14,318.3 |
| point-8-readers-writer-false | 40,074.7 -> 43,635.0 | 156.3 -> 148.5 | - |
| point-8-readers-writer-true | 893.2 -> 696.0 | 251.9 -> 162.5 | 13,331.1 -> 16,021.5 |
| page-1-readers-writer-false | 628.2 -> 611.0 | 3,478.2 -> 3,596.7 | - |
| page-1-readers-writer-true | 137.0 -> 109.2 | 148,612.2 -> 4,238.5 | 13,573.3 -> 15,764.8 |
| page-4-readers-writer-false | 3,118.3 -> 2,902.9 | 2,879.7 -> 3,291.2 | - |
| page-4-readers-writer-true | 225.3 -> 222.8 | 66,742.7 -> 3,068.8 | 32,380.0 -> 25,286.7 |
| page-8-readers-writer-false | 5,042.3 -> 5,007.6 | 1,775.4 -> 1,712.7 | - |
| page-8-readers-writer-true | 479.1 -> 405.4 | 89,339.3 -> 2,009.1 | 11,829.8 -> 17,102.5 |

Mixed page-query read p95 falls in every pair (approximately 89.9%-98.6% across
one, four and eight readers). This supports the bounded acquisition-progress
improvement; it does not establish general latency or throughput acceptance.
Writer-only p95 changes by +105.1%, +24.1%, and -5.5%; four-reader read-only page
p95 changes by +27.4%, +3.5%, and +14.3%. Eight-reader mixed point-query write p95
also rises in every pair (+39.8%, +19.3%, +8.6%). The candidate uses more process
CPU time in each final pair. The source of these costs needs further profiling;
snapshot publication and COW retention are candidate explanations, not proven
attributions.

**Performance qualification remains blocked.** In particular, these data do not
satisfy the single-stream no-regression criterion. The change remains a draft
while the regressions are investigated. The short read-only phases, workstation
interference, three-pair sample, and fixed work per reader limit scaling claims.
This synthetic SQL fixture does not qualify representative Mem data, governed
admission, Cypher performance, or production readiness. #226 remains open.
