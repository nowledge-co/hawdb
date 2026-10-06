# Pressure-driven automatic checkpoints

This contract implements issue #207 for the embedded Rust facade. It does not
change the commit durability default or authorize HawDB in stable Mem builds.
Implementation and qualification are in progress; this document is not a
completion receipt.

## Required behavior

Writable persistent databases schedule checkpoint work at the existing 70%
soft WAL/delta pressure threshold, or when uncheckpointed data exceeds a finite
host-configured age. A host need not run a checkpoint loop. Read-only,
in-memory, and capability-excluded compositions create no maintenance worker.

Age measures the lifetime of checkpoint debt using a monotonic clock. Appending
another transaction does not reset it. Reopening starts a new finite observation
window for existing debt; wall-clock changes do not delay a live scheduler.
After selecting checkpoint S, age applies only to the retained post-S suffix.
Its conservative monotonic floor is the time S was captured, which is no later
than its first suffix commit. The earlier generation's debt age is discarded;
a suffix-free selection clears the clock. Later suffix appends never reset it.

Only one checkpoint candidate is pending or preparing for a database. Duplicate
triggers coalesce. Work holds the existing background QoS and runtime-governor
leases until its buffers, pinned sources, staging files, and publication work
are released. Admission denial defers work with bounded backoff; it never
loosens memory, disk, FD, WAL, or delta limits.

Commits remain owned by one writer. Preparation performs database-sized work
outside the writer critical section. A checkpoint must make progress despite
commits arriving during preparation; restarting a full build whenever its
source epoch changes is insufficient.

## Snapshot and WAL suffix protocol

Capture a pinned logical source at commit epoch S under the writer. Record the
selected checkpoint generation, active WAL generation/header identity, complete
WAL byte length B, and next LSN L. Source capture occurs after any group flush,
not while unacknowledged group entries are still being accumulated.

Prepare immutable checkpoint artifacts for S in a private future generation.
The existing checkpoint manifest and branch head remain authoritative during
preparation. The ordinary strict prepared-checkpoint publication path retains
its stale-source guard; automatic publication uses a separately checked suffix
protocol.

After preparation, capture a completed source prefix at epoch C and next LSN N.
Read exactly the byte interval from B to the captured complete length. Validate
the same WAL header identity, contiguous LSNs [L,N), complete transaction frames,
and C-S = N-L. An append beyond the captured length is invisible to that pass.
A missing, truncated, corrupt, or changed captured source fails closed.

Reframe the suffix into the private candidate WAL with its new generation and
start LSN L. Fragment generation tags, block positions, and checksums must be
encoded again; copying old-generation framed bytes is invalid. Use a bounded
record cursor that seeks to the captured boundary instead of rescanning the
already checkpointed prefix. It may reread one preceding framing block.

Build the candidate runtime on the new checkpoint base and replay this same
complete suffix. This includes graph/schema, relational rows/indexes/overflow,
append state, search change capture, and exact commit epochs. Keep MVCC history,
transaction identities, and live reader pins from the authoritative runtime;
recovery-style replay alone is not a replacement for conflict metadata.

If commits advance while catching up, advance the candidate through another
bounded captured suffix, rather than rebuilding its checkpoint. Publication
revalidates the exact final writer/WAL identity. Final work in the critical
section is bounded independently of database size. Resource backpressure must
allow the admitted candidate to complete rather than starving it with new work.

Synchronize the complete candidate WAL and checkpoint closure before selecting
the manifest and, for an admitted branch, publishing the authoritative branch
head. Legacy storage selects checkpoint S and a replay interval [L,N) in its
manifest. An admitted branch publishes an immutable root anchored at S and L;
its new private active WAL contains the complete suffix [L,N). The new head
authenticates the generation/header prefix at L, exactly as ordinary branch
commits authenticate a stable prefix and append later complete records. Its
logical epoch is S, while the serving runtime and recovered WAL advance to C.
The immutable root's disposable runtime WAL is empty, so admission replays the
private suffix exactly once. This keeps the immutable checkpoint closure
unchanged throughout catch-up and avoids hashing a database-sized closure or
whole WAL under the final writer barrier. Later sealing binds the same
checkpoint closure and seals this suffix through the ordinary branch path.
Existing framing/manifest/branch-root fields represent both contracts; no
format version or in-place migration is needed.

Adopt the complete candidate runtime only after authoritative publication.
Release source/candidate pins before computing reclaim eligibility. Drop old
runtime maps after releasing the writer barrier. External
readers retain their physical generations and logical snapshots. Reclamation
failure is retained maintenance debt, not a failed acknowledged checkpoint.

## Failure and lifecycle

Before selection, cancellation or I/O/admission failure leaves the old WAL and
head authoritative. Private artifacts are owned and cleaned only by their
serialized job; uncertain publication evidence is retained for recovery.
After uncertain selector publication, reject further writes until ordinary
recovery establishes authority. A successfully committed user transaction must
not be reported as rolled back because later maintenance failed.

Shutdown stops admission of new work, cancels or completes the owned candidate,
joins its worker, and releases all leases. Workers must not retain a strong
ownership cycle. Manual checkpoints, branch selection, and schema-required
checkpoints coordinate with the same candidate owner and publication barrier.
Tokio integration uses the same library contract and the host's existing
runtime admission; no nested blocking wait or helper-process control plane.

## Supported sustained-load envelope

Automatic progress is required when storage can persist checkpoint work faster
than admitted writes consume the remaining WAL/delta headroom, background work
is admitted, and disk/FD/memory budgets cover the declared working set. Preserve
bounded backpressure while a checkpoint is catching up.

Permanent background denial, insufficient disk, an oversized individual
transaction, impossible reader-retention budgets, or sustained writes exceeding
storage capacity can still reject with the existing explicit resource errors.
These cases must preserve complete committed data and remain distinguishable
from scheduler failure. Removing a limit is not an availability fix.

## Qualification required before completion

- Default facade/concurrent/Tokio sustained writes across multiple checkpoint
  generations without a caller checkpoint loop.
- Exact soft/age boundaries, duplicate triggers, admission denial and recovery,
  cancellation/shutdown, and absence of concurrent WAL writers.
- Commits during preparation and catch-up; complete graph/schema, relational,
  append, and search-capture parity after publication and ordinary reopen.
- Historical readers, MVCC conflict history, failed publication, torn WAL tails,
  and generation reclamation with retained evidence.
- A replayable seeded lifecycle/fault campaign with independent expected state;
  controls for lost suffix commits, duplicate work, premature reclamation, and
  leaked leases. Fuzz stays on the local Bazel surface.
- A protocol model covering old/candidate selectors, suffix durability,
  acknowledged writes/checkpoints, lost unsynchronized writes, torn writes,
  and publication ordering. Report modeled filesystem/sync assumptions.
- Paired release measurements against equivalent caller-driven cadence, with
  all repetitions: foreground commit p99, throughput, RSS, tracked reservations,
  temporary disk, admission, and complete durable-result verification.
- Pinned-toolchain formatting and strict workspace Clippy, relevant ordinary
  runtime/storage/optimizer/recovery tests, mandatory local fuzz, and affected
  native/minimal feature profiles. CI and narrow tests do not replace these.

## Current implementation boundary

The storage candidate mounts one private checkpoint base, incrementally
reframes/replays captured WAL intervals, revalidates the final writer identity,
and selects a legacy manifest or admitted branch head. It preserves current
conflict history, consumer acknowledgments and shared fatal-state ownership.
The strict ordinary prepared-checkpoint path retains its stale-source guard.
Focused regressions cover multiple catch-up passes, writes after a capture,
historical snapshots, both durability policies, legacy residency modes,
mixed graph/relational branch recovery, subsequent sealing/forking, missing
private tail bytes, and uncertain legacy manifest publication. They do not
replace the lifecycle, model, fuzz, platform or performance gates above.

The frontend bridge now scopes mutable access with an owned publication guard.
Storage exposes constant-time WAL debt signals and an opaque complete-prefix
identity for worker handoff. Selected-runtime adoption validates store identity,
commit epoch, WAL generation and LSN, restores the actual frontend pin/conflict
ownership, and returns the old runtime for destruction outside the gate. This
prevents a worker's captured pin from becoming a permanent writer watermark.
Replay finalization can run before selector publication; the deferred-reclamation
entry requires it and leaves generation scanning/version pruning out of that
entry. A publication owner must freeze new commits through this finalization
and selection interval. The owner now serializes captured sources, selector
publication, frontend handoff and off-gate retirement. Manual checkpoint
sources retain a suspension through preparation and selection; branch
switching/sealing, backup, row-page and overflow compaction coordinate with
the same owner.

Off-gate retirement publishes its complete reclamation receipt through shared
durable runtime state. The serving frontend observes failed deletions and their
pending file/byte counts, and successful retry clears the same receipt. The
receipt lock never spans filesystem work. A regression exercises selection,
frontend adoption, injected deletion failure, retry and complete ordinary reopen.

Default writable branch admission arms one library-owned worker. It consumes
byte and monotonic age signals, coalesces sources, retries admission, drains
in-flight WAL sync groups and joins at handle closure. Focused native Cargo
tests exercise idle facade/concurrent/Tokio progress, old reader retention,
manual checkpoint coordination and governor denial/restoration.

Explicit branch reclamation suspends the owner and releases its parked internal
source after preparation/retirement is idle. The source's branch/snapshot leases
must not permanently defer reclamation once external candidates and readers
are gone. Source destruction runs outside the publication mutex. The following
mutable frontend guard recaptures the unchanged source identity, so age-based
work can resume without another write. Focused branch regressions retain live
external candidates and unfinished jobs until their actual completion; an owner
regression checks reclamation followed by admission restoration and age-driven
checkpoint progress with no intervening foreground write.

Replacing a captured source while coalescing triggers returns the superseded
snapshot to the frontend guard. That guard releases the publication mutex
before destroying the source's COW maps, catalog and physical pins. A regression
keeps the actual old source alive through submission and releases it after the
guard, with complete old/new source identities. Destruction still occurs on the
calling thread; this narrows the publication critical section and does not
establish a foreground p99 bound or complete retained-source admission.

Captured WAL replay receives the same governor-admitted task context as its
owner. Read/write/synchronization waves use that task's I/O reservation;
cancellation is checked at complete-record boundaries. Selector I/O acquires
its wave before taking the publication lock. A deterministic cancellation
regression stops after a private suffix record has been replayed, verifies
unchanged authoritative bytes and no leaked I/O waves, then checks candidate
cleanup, subsequent writes and complete ordinary reopen.

Checkpoint-base canonical records, property-spill blocks, descriptor leaves and
interior pages now share the already-admitted task context. Source hydration is
inside the record lease; bounded encoding, page construction, metadata hashing
and metadata writes acquire actual per-unit local permits when a local scheduler
is supplied. Each I/O wave is released before a nested builder acquires another
wave. Focused tests construct and completely read back 2,000 records/descriptors
with a per-work operation limit of one and a total limit of four, cancel during
record and interior-page construction, and cancel at every I/O admission point
in a canonical/property-spill fixture. They assert released QoS/I/O leases and
absence of unpublished temporary data. Another storage test verifies unchanged
authority, subsequent writes, retry and complete ordinary reopen after base
encoding cancellation. Only the job's own unpublished temporary paths are
removed; published private files can remain as recovery evidence.

Canonical adjacency now uses the same admitted context for source hydration,
sorting memory-budgeted chunks, spill records, bounded fan-in merge records,
artifact blocks and descriptor pages. Chunk sorting retains the existing
configured memory bound (32 MiB by default); a unit never represents sorting
the complete dataset. Spill payload lengths are checked against the record and
chunk limits before allocation. Temporary-run ownership survives merge-level
replacement and includes partially written runs. Focused tests completely
reopen 2,000 relationships in both directions under one-operation per-work and
four-operation total limits, cancel inside a multi-level merge and at every I/O
admission point, and reject a corrupt spill length before payload allocation.
The automatic path uses demand-paged descriptor trees; the older optional
resident-manifest API still retains its complete descriptor vector.

Persistent property projections share the same context for definition
preparation, source hydration, scalar/composite keys, streaming full-text tokens,
memory-bounded chunk sorts, spill/merge records, artifact blocks and descriptor
pages. Controlled definition sorting checks the raw input against the configured
definition count/byte limits before sorting or deduplication. This prevents
duplicate definitions from making a supposedly bounded sort unbounded; the
older writer API without a task context retains its existing deduplication
behavior. Metadata hashing and publication use bounded units. Temporary-run
ownership covers partial files and all merge levels. Focused tests completely
reopen 2,000 nodes and 2,000 relationships across all six projection kinds,
cancel inside a multi-level merge and at every I/O admission boundary, and
reject excess raw definitions before source hydration. Actual candidate tests
cancel during adjacency and property-projection spilling, verify unchanged
authority and released leases, then write again, retry and completely reopen
all relationships.

Source-scan sidecars now use the same task for source hydration, 128-row segment
summaries, per-row/per-value text encoding, compression and CRC32C in 64 KiB
blocks, and bounded writes with admitted I/O waves. Segment construction moves
the collected rows instead of retaining a second complete clone. Temporary-path
cleanup becomes armed only after this writer creates the file; it removes its
unpublished files and retains private final files as evidence. The existing V1
envelope/descriptor formats and public sidecar error surface are preserved.
Focused coverage checks the old compressed bytes, all 2,000 Source rows and
their summaries/exact cursors against the independent reference, cancellation
during summary/compression work and every sidecar I/O admission, and actual
candidate cancellation followed by unchanged authority, retry and complete graph
reopen. Cancellation tests observe actual artifact creation rather than assume
a fixed number of preceding builder operations.

The automatic owner still holds its whole-candidate local permit: these units
do not cover projected-graph preparation, append/relational builders, statistics,
candidate reopening or replay finalization. Temporary-file cleanup is currently
best effort and lacks a complete retained cleanup-debt/resource ledger. Metadata
buffers and dictionaries can still scale with the dataset. Source-scan still
retains complete projected rows/summaries and whole segment, compression and
descriptor buffers; its work units do not establish a hard byte ledger or an
allocation bound for a large individual row. These focused tests
do not prove cancellation or memory bounds for the entire candidate.

`HawDBAutomaticCheckpoint` independently models one old/candidate handoff and
two schema/data transactions under both durability policies. Its complete
configured safety graph passed TLC (34,275 distinct states). Five deliberately
incorrect protocol controls each produce their expected counterexample; two
additional witnesses demonstrate relaxed acknowledged-write loss and recovery
after a lost synchronous reply. The model includes independent loss/torn/write
reordering of unsynchronized WAL fragments and checkpoint/catalog artifacts,
selector uncertainty, pinned readers and cancellation lease ownership. It
assumes completed synchronization preserves covered bytes and identity checks
detect incomplete/corrupt artifacts. This is bounded protocol evidence, not
Rust refinement, a platform synchronization proof, liveness or runtime fault
campaign coverage. See `docs/tla/README.md` for exact commands and assumptions.

The current whole-candidate operation estimate can exceed the default local
QoS operation limit on a large database. Remaining bounded cancellable build
units and builder-specific memory/retention accounting remain required; the
provisional whole-candidate memory estimate also omits append-state retention
and compaction allowances. Delta pressure,
columnar-shadow parity, the full cancellation/fault/model/platform matrix and
paired release performance qualification are also incomplete. Whole-candidate
memory estimates and narrow idle tests do not prove these requirements.
No full issue completion is claimed.
