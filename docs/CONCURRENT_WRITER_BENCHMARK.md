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
4/8-writer rounds. This is not fsync amortization. Source inspection explains a
structural restriction: `ConcurrentDatabaseTransaction::commit_with_result`
acquires `LockRequest::database(LockMode::Exclusive)` for optimistic mode before
calling `execute_grouped` and releases it only after that call returns. Thus
these optimistic committers cannot coexist in the group queue. The coordinator
can batch other compatible paths, but this benchmark does not exercise them.
Removing that lock without an equivalent pessimistic/optimistic coordination
protocol would change correctness, not merely scheduling.

The result supports workload-specific memory concurrency and identifies a
concrete remaining group-admission problem. #232 stays open for that protocol,
fair admission/starvation coverage, representative durable scaling and
cross-revision single-stream latency evidence. #231's global memory and full
recovery proof obligations also remain open.
