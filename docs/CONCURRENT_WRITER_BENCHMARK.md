# Fixed-work concurrent writer qualification

`benches/concurrent_writers.rs` measures the embedded optimistic graph transaction
path for #231/#232. It is a local qualification binary, registered in Cargo and
Bazel's manual benchmarks, outside default CI smoke dispatch.

```sh
cargo test --locked --bench concurrent_writers # correctness smoke, not performance
cargo bench --locked --bench concurrent_writers
bazel run -c opt //:hawdb_bench_concurrent_writers
```

## Workload and controls

Each case creates a fresh database with 256 `MvccCounter` nodes. A single setup
transaction inserts them, checkpoint completes, and a full ordered read verifies
the initial state. Setup, checkpoint, verification and reopen are outside the
timed interval. Threads and parameter maps are allocated before a coordinated
start. Timing includes release of the start barrier and joining the workers.

The same total 256 one-statement transactions run with 1, 4 or 8 writers. Each
transaction updates one distinct node from zero to one using the same
parameterized query. The control acquires a host mutex before beginning a
transaction and releases it after commit. The candidate uses the same API and
transaction body without that mutex. This isolates lifecycle serialization on
the **same binary**; it does not reconstruct or measure a historical engine.

The three storage configurations are in-memory with group commit disabled,
durable with group commit disabled, and durable with an explicit benchmark-only
fixed group-commit candidate (8 entries, 1 MiB target, 100 microsecond wait).
This does not enable group commit for production. Each release run performs five
rounds, reversing all case order on alternate rounds. Debug smoke uses 16
transactions and one round. Debug timings are not qualification evidence.

Each JSONL record contains the round, configuration, elapsed time, throughput,
raw transaction and commit latency samples, nearest-rank p50/p95/max latency,
per-worker completion counts and elapsed time, and group-commit counter deltas.
Transaction latency includes waiting for the control mutex; commit latency
begins immediately before `commit`. Group-commit counters are not total fsync
instrumentation when grouping is disabled: zero group syncs does not mean zero
WAL fsyncs. Raw latency samples are grouped by worker, not globally time-ordered.

## Correctness and proof boundary

For `C` transactions and `W` writers, worker `w` owns IDs
`w, w + W, w + 2W, ... < C`. Euclidean division gives every ID in `[0,C)` a
unique quotient and remainder modulo `W`; therefore these sets are disjoint
and their union is exactly the fixture. The workload keeps `C` fixed as `W`
changes. This partition argument is independent of scheduling.

Every transaction must succeed exactly once: there is no retry loop or ignored
error. The runner checks the exact epoch increment, sample count, and all
ordered `(id, value)` rows after execution. Durable cases close and reopen the
WAL-backed database and repeat the epoch and complete row comparison. Grouped
cases also require exact submitted/completed/WAL-entry counts and a nonzero,
non-excessive sync count. Throughput is reported only after these checks pass.
A panic or partial JSONL file is an incomplete run, not a qualifying result;
a full release run has 90 unique round/configuration records.

The [MVCC validation proof](tla/MVCC_VALIDATION_PROOF.md) and its disjoint-commit
reachability probe cover the abstract first-committer-wins rule consumed here.
The benchmark adds executable data and ordinary reopen checks; it introduces no
new commit algorithm and is not a replacement for the model. Its measurements
do not establish a throughput theorem, scheduler fairness, starvation freedom,
process-kill recovery, or a total memory bound. Finite per-worker completion
counts cannot prove fairness under sustained admission.

## Interpretation

Compare paired candidate/control results within each round before summarizing
ratios across rounds. Also compare 4/8-writer candidate throughput against the
1-writer candidate in the same storage configuration. Those are different
questions: outperforming a serialized control at the same writer count does
not by itself establish scaling above a single writer. Report raw variation
and p95 latency alongside throughput. No timing assertion turns noisy local
measurements into a CI correctness gate.

This intentionally small point-update workload may be dominated by snapshot
capture and serialized commit/WAL costs. It is not a representative Mem route
mix or a contention/fairness workload. Absence of scaling or sync amortization
must be reported as an unmet acceptance criterion, rather than selecting only
favorable configurations. Single-stream regression versus an earlier revision
requires a separately built baseline on the same machine; the host-mutex control
alone cannot establish that criterion.

## Local result: 2026-09-25

The [receipt](benchmarks/concurrent_writers_macos_2026_09_25.json) retains all
90 round/configuration records without raw latency arrays, their summaries,
the exact engine revision and harness hash, and the raw JSONL checksum. Raw
arrays remain at `target/benchmarks/232-concurrent-writers/2026-09-25-release.jsonl`.
All 23,040 timed commits, all full-row comparisons and 60 durable reopens passed.
The final debug smoke separately passed 18 cases / 288 commits / 12 reopens.

Environment: Mac15,8, 16 logical CPUs, 128 GiB RAM, macOS 27.0 (26A428),
Rust 1.97.1, default Cargo features, the repository's release benchmark profile
(opt-level 3, thin LTO, one codegen unit). The engine is `5126a98c`; the new
benchmark is identified separately by its hash in the receipt. Databases use
the local temporary filesystem and `Database::open` defaults. OS caches were
not flushed, CPU affinity was not set, and background desktop activity was not
controlled. Process CPU/RSS and physical-device I/O were not measured; this is
throughput/latency and engine sync-counter evidence, not resource qualification.

TPS and p95 are medians of five case-level metrics. Ratios are medians of five
within-round ratios, so they need not equal ratios of the displayed medians.
`Control` means the same-binary host-mutex control, not another revision.

| Storage | Writers | Control TPS | Concurrent TPS | Paired speedup | Speedup vs 1 writer | Concurrent transaction p95, ms |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| Memory | 1 | 6,388.9 | 6,370.3 | 0.993 | 1.000 | 0.163 |
| Memory | 4 | 5,942.2 | 10,769.4 | 1.813 | 1.695 | 0.465 |
| Memory | 8 | 5,806.6 | 9,548.8 | 1.614 | 1.495 | 1.554 |
| Durable, ungrouped | 1 | 164.0 | 163.0 | 0.994 | 1.000 | 6.567 |
| Durable, ungrouped | 4 | 161.6 | 176.0 | 1.102 | 1.066 | 46.007 |
| Durable, ungrouped | 8 | 161.9 | 176.4 | 1.090 | 1.065 | 95.967 |
| Durable, grouped candidate | 1 | 156.0 | 158.2 | 1.010 | 1.000 | 7.453 |
| Durable, grouped candidate | 4 | 158.3 | 171.6 | 1.128 | 1.116 | 36.103 |
| Durable, grouped candidate | 8 | 154.0 | 169.4 | 1.060 | 1.050 | 87.038 |

Memory 4-writer paired speedup ranges from 1.782 to 1.819; 8-writer speedup
ranges from 1.502 to 1.709. Four writers outperform eight on this workload.
Durable results vary much more: the ungrouped 4-writer paired speedup ranges
from **0.620 to 1.130**, including a regression; grouped 4-writer speedup ranges
from 0.996 to 1.873. No case or round was discarded. These short measurements
do not establish a general durable-throughput or latency guarantee.

Every grouped case used **256 shared syncs for 256 commits**, including all
4/8-writer rounds. This is not fsync amortization. Source inspection of the measured `5126a98c` revision explains a
structural restriction: `ConcurrentDatabaseTransaction::commit_with_result`
acquires `LockRequest::database(LockMode::Exclusive)` for optimistic mode before
calling `execute_grouped` and releases it only after that call returns. Thus
these optimistic committers cannot coexist in the group queue. The coordinator
can batch other compatible paths, but this benchmark does not exercise them.
Removing that lock without an equivalent pessimistic/optimistic coordination
protocol would change correctness, not merely scheduling.

The result supports workload-specific memory concurrency and identifies a
concrete remaining group-admission problem. The admission follow-up replaces that X permit with a dedicated O mode; see
the [proof](tla/OPTIMISTIC_COMMIT_ADMISSION_PROOF.md). #232 stays open for
fair admission/starvation coverage, representative durable scaling and
cross-revision single-stream latency evidence. #231's global memory and full
recovery proof obligations also remain open.

## Optimistic admission follow-up: 2026-09-25

The [second receipt](benchmarks/concurrent_writers_macos_2026_09_25_admission.json)
measures engine `804865b7` using the **identical harness hash**, host, features,
profile, fixture, case order and five-round protocol. No agent-started builds or
tests overlapped this measurement. It is a subsequent process, not an
interleaved before/after revision experiment. All 90 cases / 23,040 commits /
60 durable reopens passed. Raw arrays remain at
`target/benchmarks/232-concurrent-writers/2026-09-25-admission-release.jsonl`.

| Storage | Writers | Control TPS | Concurrent TPS | Paired speedup | Speedup vs 1 writer | Concurrent transaction p95, ms |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| Memory | 1 | 6,360.6 | 6,374.8 | 1.005 | 1.000 | 0.163 |
| Memory | 4 | 5,963.5 | 11,237.0 | 1.896 | 1.767 | 0.404 |
| Memory | 8 | 5,866.2 | 10,813.5 | 1.852 | 1.725 | 1.521 |
| Durable, ungrouped | 1 | 162.8 | 159.2 | 0.998 | 1.000 | 7.359 |
| Durable, ungrouped | 4 | 160.6 | 173.4 | 1.067 | 1.082 | 43.555 |
| Durable, ungrouped | 8 | 159.8 | 175.7 | 1.091 | 1.094 | 107.022 |
| Durable, grouped candidate | 1 | 157.9 | 161.4 | 1.022 | 1.000 | 6.617 |
| Durable, grouped candidate | 4 | 158.6 | 567.1 | 3.594 | 3.662 | 8.442 |
| Durable, grouped candidate | 8 | 160.2 | 1,086.8 | 6.784 | 6.867 | 8.734 |

All grouped 4-writer cases used **64 syncs for 256 commits**. Grouped 8-writer
cases used **32, 33, 32, 33 and 37 syncs**, respectively. The previous engine
used 256 in every grouped case. This supports actual sync amortization on this
workload; no timing threshold or synthetic forced batch is used in the benchmark.
The separate regression uses an enqueue gate to verify the safety contract
deterministically, and is not included in these performance numbers.

Grouped 4-writer candidate/control throughput ratios range from 3.491 to 3.613,
and 8-writer ratios from 5.797 to 6.969. In contrast, ungrouped 4-writer ratios
range from **0.630 to 1.127** and grouped 1-writer ratios from **0.619 to 1.634**.
Those unfavorable samples remain in the receipts. The data establishes a
workload-specific batching benefit, not general fairness or single-stream
no-regression. A matched cross-revision latency qualification remains required.

`/usr/bin/time -l` for the entire new process reports 117.04 s wall, 19.06 s user,
9.23 s system, maximum RSS 28,753,920 bytes, and peak footprint 13,910,544 bytes.
These process totals include setup, verification, reopen and JSON serialization;
they are not per-case resource budgets or bounds under long-lived snapshots.
The OS-reported block-operation counters were zero and are not treated as
physical storage-I/O measurements. Engine sync counters above are the relevant
amortization evidence.

## Current MVCC qualification: 2026-09-25

The [third receipt](benchmarks/concurrent_writers_macos_2026_09_25_mvcc.json)
measures engine `56382f61413c49146de7839e925ed13293bb22d4`, after lock fairness,
admitted transaction lifetimes, failure recovery, append-table identities and
explicit relational write intents. The unchanged graph benchmark passed all
90 cases / 23,040 commits / 60 durable reopens. The same host, toolchain, default
features and five-round protocol apply. No agent-started tests or other builds
overlapped execution. Raw samples are retained at
`target/benchmarks/232-concurrent-writers/56382f61-release.jsonl`; the receipt
records their checksum, the harness hash and the release binary hash.

| Storage | Writers | Control TPS | Concurrent TPS | Paired speedup | Speedup vs 1 writer | Concurrent transaction p95, ms |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| Memory | 1 | 6,341.2 | 6,407.1 | 1.010 | 1.000 | 0.161 |
| Memory | 4 | 5,956.0 | 10,965.3 | 1.839 | 1.713 | 0.421 |
| Memory | 8 | 5,856.7 | 10,885.5 | 1.857 | 1.750 | 1.362 |
| Durable, ungrouped | 1 | 162.5 | 161.5 | 0.984 | 1.000 | 7.393 |
| Durable, ungrouped | 4 | 162.4 | 172.4 | 1.062 | 1.080 | 41.498 |
| Durable, ungrouped | 8 | 155.5 | 169.8 | 1.072 | 1.047 | 116.504 |
| Durable, grouped candidate | 1 | 159.9 | 161.2 | 1.015 | 1.000 | 7.326 |
| Durable, grouped candidate | 4 | 163.0 | 573.3 | 3.516 | 3.632 | 8.440 |
| Durable, grouped candidate | 8 | 160.1 | 1,058.8 | 6.525 | 6.813 | 8.561 |

Grouped 4-writer sync counts were **64, 64, 64, 66, 64**; 8-writer counts
were **32, 32, 33, 33, 33**, each for 256 commits. Corresponding paired
throughput ratios span 3.447–3.608 and 5.845–7.307. The current implementation
therefore retains measurable batching and scaling on this graph workload.

Unfavorable samples remain: ungrouped 1-writer paired ratios span 0.590–0.994,
ungrouped 4-writer ratios 0.640–1.087, and grouped 1-writer ratios 0.637–1.062.
These are subsequent runs, not an interleaved cross-revision comparison; no
claim of single-stream no-regression follows. The benchmark does not measure
relational collection cost, sustained contention/retries, governed admission
fairness or long-lived-reader memory. Those acceptance items remain open.

No algorithm or formal model changed in this evidence-only update. The workload
partition proof and referenced commit-validation/admission proofs above still
apply. Process CPU/RSS and physical I/O were not collected for this run.
