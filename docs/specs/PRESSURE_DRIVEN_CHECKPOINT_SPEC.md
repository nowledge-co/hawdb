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

The current whole-candidate operation estimate can exceed the default local
QoS operation limit on a large database. Bounded cancellable build units and
builder-specific memory/retention accounting remain required. Delta pressure,
columnar-shadow parity, the full cancellation/fault/model/platform matrix and
paired release performance qualification are also incomplete. Whole-candidate
memory estimates and narrow idle tests do not prove these requirements.
No full issue completion is claimed.
