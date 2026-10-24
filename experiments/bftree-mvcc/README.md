# Bf-Tree MVCC experiment

An unpublished, binary-only, independent Cargo workspace for evaluating a
transaction adapter over `bf-tree = 0.5.6`. This is not a selectable HawDB
production backend. The root library does not depend on it, and its sources are
excluded from the published library archive. See [issue #1212](https://github.com/nowledge-co/hawdb/issues/1212).

Run commands from the repository root to inherit the uniform Cargo profiles:

```console
cargo run --locked --manifest-path experiments/bftree-mvcc/Cargo.toml -- demo /tmp/bftree-demo.journal
cargo test --locked --manifest-path experiments/bftree-mvcc/Cargo.toml --all-targets --all-features
cargo fmt --manifest-path experiments/bftree-mvcc/Cargo.toml -- --check
cargo clippy --locked --manifest-path experiments/bftree-mvcc/Cargo.toml --all-targets --all-features -- -D warnings
cargo run --release --locked --manifest-path experiments/bftree-mvcc/Cargo.toml -- bench /tmp/bftree-measurements 512 24
```

The demo journal and benchmark directory must not already exist. They contain
synthetic data only. Runs remain manual; this experiment is not registered in
CI, scheduled jobs, the production workspace, or library dependencies.

## Transaction and recovery contract

A transaction captures one visible epoch. Private point reads overlay its write
set; rollback discards that set. Commit validates first-committer-wins conflicts
against keys changed after its snapshot. Disjoint writes may commit. This is
snapshot isolation, with possible write skew; there is no predicate conflict
tracking or serializability claim. Private range scans are not implemented.

One coordinator serializes commit validation, append, synchronization, index
installation and publication. Each complete transaction is one framed journal
record. The adapter synchronizes that record before installing any index
changes, then releases the complete epoch only after all keys are installed.
Readers do not take the journal coordinator lock. A long Bf-Tree snapshot owns
an epoch, not an iterator or leaf latch. Scans release native iterator guards
between bounded batches and before historical point lookups.

The journal reuses HawDB's binary WAL framing, checksums and strict reader. Its
`BFMVCC01` identity, layout generation and compact transaction payload are
experiment-only and are **not the canonical HawDB WAL format**. Reopen rebuilds
the entire working index. Complete durable transactions are replayed atomically,
including a commit whose response was lost. Stable caller-supplied operation
IDs deduplicate exactly equal mutation batches across reopen; different writes
with the same ID are rejected. This does not deduplicate arbitrary application
computations or automatically re-execute a transaction body.

Any error after append begins reports an uncertain commit and poisons all
handles; an unwind also poisons them. Close all snapshots and store handles,
then reopen to resolve the outcome through replay. A complete durable record
may already have committed. A torn, corrupt or unverified tail fails closed
and preserves evidence: there is no automatic truncation, repair or retry that
assumes rollback. Each journal has one exclusive file-lock owner, retained by
its snapshots. Existing journal layout mismatches are rejected.

`sync_all` is requested for every transaction and for initial journal creation;
on Unix the parent directory is also synchronized. Successful synchronization
is assumed to provide the filesystem's documented durability. Process-exit and
unwind tests are not power-loss, reordered-write or storage-device qualification.
Windows behavior is unqualified. Bf-Tree uses its **memory backend**, with its
own WAL/CPR disabled; the experiment journal supplies recovery. Its 4 MiB Bf-Tree
cache setting is not a total process-memory bound.

## Layouts and performance tradeoffs

| Layout | Current reads | Historical reads | Update cost |
| --- | --- | --- | --- |
| `versioned` | Seek descending epoch keys; scans filter versions with bounded version-run seeks | Seek/filter versions at the captured epoch | One new version per changed key |
| `hybrid` (demo default) | Direct head-key read; scan only logical heads | Use head when visible, otherwise seek history | Archive the previous head before replacing it; usually two index writes |
| `cow` | Existing `CowSegmentedMap`, with shared value payloads | Retained immutable map snapshots | Clone affected pages and publish a private map |

The COW case is a **component reference**, not an end-to-end comparison with
HawDB transactions. All layouts use the same experiment journal, synchronous
commit policy, conflict ledger and receipts. Bf-Tree layouts retain history
without GC; COW history survives through live snapshots. Compare memory and
write costs as well as read latency. No result here establishes an overall
HawDB speedup or larger-than-memory index performance.

The benchmark holds the seeded snapshot while repeatedly overwriting all keys,
then verifies current and old point reads and full scans. Values are 100 bytes;
seed batches contain at most 128 keys and update batches contain at most 64.
Every returned key/value and scan order is checked against the generated data.
It runs three repetitions with rotated layout order in fresh child processes.
It reports per-operation p50/p95/p99, total measured time, synchronous batch
commit latency (including write-set construction), prepare/sync and index
installation phase latencies, reopen time, journal size,
and process resident/peak memory where available. Point reads have 4096 samples
per snapshot; scans have only 32, so scan p99 is a sample maximum rather than a
stable production tail estimate. Reads are warmed once; journals use the local
filesystem. `report.json` preserves each repetition instead of hiding variance.
Use both a small workload (`16 2`) and a retained-history workload (`512 24`).
See [MEASUREMENTS.md](MEASUREMENTS.md) for local evidence and limitations.

The optional [key-comparison probe](simd/README.md) compares original Bf-Tree
ordering with scalar word and ARM NEON kernels in temporary dependency copies.
It adds long-key shapes through `bench-shaped` without selecting a patched
dependency in the default adapter.

## Bounds and coverage

Keys: 64 bytes; values: 1024 bytes; distinct writes: 128 per transaction;
encoded payload: 256 KiB; journal: 32 MiB; returned scan rows: 4096; visited
physical records: 200,000. Exceeding a scan budget returns an error rather than
partial results. Benchmark sizes must fit the retained-version scan budget.
There is no checkpoint, GC, spill policy, group commit, background compaction,
read-only journal opener, stable format compatibility or production integration.
Receipts and conflict stamps consume memory and are bounded indirectly by the
journal cap, not by an independent RSS budget.

Focused tests cover snapshots/private writes/rollback, partial installation,
conflict/disjoint commits, exact operation retries, an independent ordered-map
model with binary/prefix keys, page splits and long histories, tombstones/range
edges, long-snapshot writer progress, field/write limits, exclusive ownership,
layout mismatch, uncertain outcomes/unwind, torn/corrupt/trailing input, and
actual subprocess exits after durability, first-key installation and publication.
The ignored `crash_child` is an internal subprocess entry point invoked by its
parent regression; it is not an omitted manual acceptance case.

Hybrid update installation reuses immutable mutation payloads already retained
for exact retries. Committed batches are indexed directly by epoch, and the latest-key ledger
stores the entry position alongside that epoch; it does not maintain another value cache. Encoded
keys and values use bounded stack buffers. The receipt regression checks sparse
updates, non-monotonic operation IDs, old retries after reopen, deletion and
reinsertion while earlier snapshots remain readable. Version upgrades must
relock this workspace's Cargo.lock along with the other tracked lockfiles.

## Scan preparation follow-up

Scan visibility checks borrow the native iterator's buffer. Only visible output
keys/values, or Hybrid keys needing a historical point seek, become owned rows.
The physical resume key uses a bounded stack buffer; each scan reuses one owned
row batch. Discarded newer/duplicate versions no longer allocate and copy their
payloads. Hybrid history lookups still occur after releasing the native guard.

Every native batch visits at most 32 physical records, including discarded
records. A completely filtered batch advances its resume position rather than
ending the range. At a Versioned batch boundary, an already selected value or
tombstone permits seeking past that key's remaining versions. The encoded
successor stays before all longer logical keys, including a zero-byte extension.
If the last visited version is newer than the snapshot, seek inclusively to its
fixed epoch instead. No iterator or leaf guard survives either seek.

The physical-record cap counts actual records visited; version runs skipped by
an ordered seek do not consume that work budget. The cap, returned-row cap,
snapshot semantics, journal bytes, synchronization and publication boundaries
are unchanged. Focused tests cover empty/deep snapshots, tombstones across
chunks, binary/prefix and maximum-length keys, exact snapshot/range boundaries,
and refusal at the physical-work boundary using distinct invisible keys.

The manual paired comparison driver is
[`measurements/scan-20261024/measure.py`](measurements/scan-20261024/measure.py).
It uses the unchanged native benchmark with five alternating pairs across all
three layouts and small, retained-history and larger workloads. Every paired
journal must have identical bytes. The [source-bound paired report](measurements/scan-20261024/README.md)
retains both five-pair cohorts and their adverse controls separately from the
earlier observations above. An exploratory run during compilation is not final
performance qualification.
