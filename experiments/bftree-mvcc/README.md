# Bf-Tree MVCC experiment

An unpublished, binary-only, independent Cargo workspace for evaluating a
transaction adapter over `bf-tree = 0.5.6`. This is not a selectable HawDB
production backend. The root library does not depend on it, and its sources are
excluded from the published library archive. See [issue #1212](https://github.com/nowledge-co/hawdb/issues/1212)
and the three-direction follow-up [#1237](https://github.com/nowledge-co/hawdb/issues/1237).

Run commands from the repository root to inherit the uniform Cargo profiles:

```console
cargo run --locked --manifest-path experiments/bftree-mvcc/Cargo.toml -- demo /tmp/bftree-demo.journal
cargo run --locked --manifest-path experiments/bftree-mvcc/Cargo.toml -- maintenance-demo /tmp/bftree-maintenance.journal
cargo test --locked --manifest-path experiments/bftree-mvcc/Cargo.toml --all-targets --all-features
cargo fmt --manifest-path experiments/bftree-mvcc/Cargo.toml -- --check
cargo clippy --locked --manifest-path experiments/bftree-mvcc/Cargo.toml --all-targets --all-features -- -D warnings
cargo run --release --locked --manifest-path experiments/bftree-mvcc/Cargo.toml -- bench /tmp/bftree-measurements 512 24
```

The demo journal and benchmark directory must not already exist. They contain
synthetic data only. Runs remain manual; this experiment is not registered in
CI, scheduled jobs, the production workspace, or library dependencies.

## Compile-time storage selection

The versioned and hybrid layouts use the ordered byte store in
[`src/storage.rs`](src/storage.rs). MySQL's shared server and engine boundary
motivates the separation, and LevelDB's bytewise ordering and forward seek
semantics provide the closer model for this interface.
[MySQL storage engine architecture](https://dev.mysql.com/doc/refman/8.4/en/pluggable-storage-overview.html)
and [LevelDB iterator](https://github.com/google/leveldb/blob/main/include/leveldb/iterator.h).

Implement `Storage` and its cursor, then select the type in Rust with
`Store::<MyStorage>::open_with_storage(path, Layout::Hybrid)`. Calls are
statically dispatched. The CLI uses Bf-Tree for these layouts by default, and
the COW reference layout uses its own immutable map snapshots.

The journal is authoritative. A storage implementation creates a fresh index
generation on open and checkpoint reconstruction. Snapshots can retain an
older generation, so fresh instances must have independent state. The MVCC
adapter owns snapshot visibility, synchronization, recovery, and the
journal's layout identity. An alternative implementation can reopen the same
journal with the same MVCC layout.

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
`BFMVCC02` identity, layout generation and compact transaction payload are
experiment-only and are **not the canonical HawDB WAL format**. Reopen rebuilds
the retained index from the last complete checkpoint, then replays its suffix.
The complete preceding journal remains validated. This greenfield experiment
requires recreating earlier `BFMVCC01` databases; it adds no migration to HawDB.
Complete durable transactions are replayed atomically,
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
The last database/transaction owner explicitly releases that lock; incidental
descriptor copies do not extend ownership beyond those handles. A regression
retains a copied descriptor across close/reopen while proving live snapshots
and the next owner still exclude competing opens.

`sync_all` is requested for every transaction, maintenance record and initial journal creation;
on Unix the parent directory is also synchronized. Successful synchronization
is assumed to provide the filesystem's documented durability. Process-exit and
unwind tests alone are not power-loss qualification. Deterministic recovery
images additionally model a lost unacknowledged tail, torn/zeroed writes and
reordered full blocks after a durable checkpoint. These models do not establish
storage-device or filesystem qualification.
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
until explicit snapshot-safe collection; COW history survives through live snapshots. Compare memory and
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

The separate [internal-key, batch and collection probes](structural/README.md)
measure actual internal pivot bytes and preview fallbacks, native head/fence
candidates, batch-local positioning and maintenance costs. The default remains
the original registry dependency. Instrumented builds never qualify timing.
The [complete source-bound report](measurements/structural-20261024/README.md)
includes all five-pair cohorts, adverse controls, maintenance/RSS costs and
the earlier exploratory data.

## Bounded batches, collection and checkpoints

`ReadTxn::get_many` accepts at most 128 logical keys, preserves caller order and
duplicates, and returns both values and observable work counts. It sorts and
deduplicates requests, reuses a native cursor for adjacent keys, and falls back
to independent probes when traversing intervening records is unproductive.
Hybrid historical probes begin only after head iterator guards are released.
Cursor starts and resolutions describe this wrapper; native page crossings can
perform further root searches, so they are not exact root/leaf reuse counts.

Every read transaction and write transaction registers its captured epoch
before releasing the capture gate. `Store::collect_garbage(work_budget)` visits
at most 128 logical keys/removals per call. It preserves every version newer
than the oldest active epoch and the newest anchor at or below that epoch.
A pass holds one horizon and advances a logical-key cursor; releasing a lease
may require another pass before `complete_pass` reports that collection caught
up. There is no TTL that silently invalidates a reader or writer.

A complete framed GC plan is synchronized before native deletion. Recovery
validates and replays the plan if deletion/publication was interrupted. Native
deletion may retain internal page allocations; `Store::checkpoint()` exports
all retained versions, prepares a replacement index, synchronizes one complete
checkpoint record, then publishes the replacement generation. Old views retain
their original index generation. After those views drop, rebuilding permits
the old tree to be released. A checkpoint is bounded to 16 MiB/200,000 rows and
does not increment the transaction epoch.

Receipts keep compact offsets into the original WAL. Only batches referenced
by current heads retain their full payload in the journal's RAM ledger; exact
old retries read and validate their captured record interval. Maintenance does
**not reclaim journal disk capacity or discard receipt evidence**: the 32 MiB
cap still refuses further growth. Full prefix validation and per-operation
receipt metadata remain recovery costs even when checkpointed index replay is
small. Report native retained versions, payload bytes, journal bytes and RSS
separately; they are different measurements.

```console
cargo run --release --locked --manifest-path experiments/bftree-mvcc/Cargo.toml -- bench-advanced /tmp/bftree-advanced.journal hybrid 512 24 prefix64 collect
```

`keep` is the matched no-maintenance control. This workload verifies old and
current points/scans, dense/sparse/missing/duplicate batches, lease-protected
collection, both checkpoints, bounded incremental steps, exact historical
retry, and every current key after reopen. Its reports include maintenance
tails, before/after memory and replay counts; a synchronous checkpoint is
explicit foreground work, not a background-latency claim.

## Bounds and coverage

Keys: 64 bytes; values: 1024 bytes; distinct writes: 128 per transaction;
encoded payload: 256 KiB; journal: 32 MiB; returned scan rows: 4096; visited
returned native records: 200,000. Exceeding a scan budget returns an error rather than
partial results. Benchmark sizes must fit the retained-version scan budget.
There is no spill policy, group commit, automatic background compaction,
read-only database opener, stable format compatibility or production integration.
Receipts and conflict stamps consume memory and are bounded indirectly by the
journal cap, not by an independent RSS budget.

Focused tests cover snapshots/private writes/rollback, partial installation,
conflict/disjoint commits, exact operation retries, an independent ordered-map
model with binary/prefix keys, page splits and long histories, tombstones/range
edges, long-snapshot writer progress, field/write limits, exclusive ownership,
layout mismatch, uncertain outcomes/unwind, torn/corrupt/trailing input, and
actual subprocess exits after durability, first-key installation and publication.
Maintenance adds captured-epoch leases, anchor/tombstone retention, active-writer
conflicts, partial GC, checkpoint publication and post-checkpoint replay,
snapshot/collector races, malformed checksummed maintenance records and
deterministic power-loss images. Shuffled variable-length binary keys and deep
histories exercise native fences across inner levels. The two ignored crash
children are internal subprocess entry points invoked by parent regressions;
they are not omitted manual acceptance cases.

Hybrid update installation reuses immutable mutation payloads already retained
for current heads. The latest-key ledger stores each head's epoch and entry
position; old receipt payloads remain on disk rather than in another value cache. Encoded
keys and values use bounded stack buffers. The receipt regression checks sparse
updates, non-monotonic operation IDs, old retries after reopen, deletion and
reinsertion while earlier snapshots remain readable. Version upgrades must
relock this workspace's Cargo.lock along with the other tracked lockfiles.

## Scan preparation follow-up

Scan visibility checks borrow the native iterator's buffer. Only visible output
keys/values, or Hybrid keys needing a historical point seek, become owned rows.
The physical resume key uses a bounded stack buffer; each scan reuses one owned
row batch. Discarded newer/duplicate versions no longer allocate and copy their
payloads. Hybrid appends visible heads directly until a head needs history. It
then defers that head and the remaining result rows in the window so historical
seeks take place after releasing the native guard. The next Hybrid window seeks
past the previous physical head using its encoded successor.

Every native batch returns at most 32 records, including records discarded by
the adapter. Native deleted slots may be skipped internally and are not counted
as returned records; this is not a bound on latch duration or raw slot visits.
A completely filtered batch advances its resume position rather than
ending the range. At a Versioned batch boundary, an already selected value or
tombstone permits seeking past that key's remaining versions. The encoded
successor stays before all longer logical keys, including a zero-byte extension.
If the last visited version is newer than the snapshot, seek inclusively to its
fixed epoch instead. No iterator or leaf guard survives either seek.

The physical-record cap counts records returned by the native cursor; version runs skipped by
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
