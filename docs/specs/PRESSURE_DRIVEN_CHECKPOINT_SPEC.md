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
do not cover all projected-graph preparation, append/relational builders,
statistics serialization/adoption, candidate reopening or replay finalization. Temporary-file cleanup is currently
best effort and lacks a complete retained cleanup-debt/resource ledger. Metadata
buffers and dictionaries can still scale with the dataset. Source-scan still
retains complete projected rows/summaries and whole segment, compression and
descriptor buffers; its work units do not establish a hard byte ledger or an
allocation bound for a large individual row. These focused tests
do not prove cancellation or memory bounds for the entire candidate.

Projected-graph array copying, numeric encoding/decoding and structural
validation now use at most 1,024 numeric elements per work unit. Offset checks
include the boundary between adjacent chunks. The existing V1 text, decoded
arrays, historical tolerances and validation errors are preserved. Compressed
artifact publication uses the admitted task's CRC/compression blocks and I/O
waves. Focused coverage checks complete 2,048-node/4,096-edge arrays against the
independent text reference, cancellation inside numeric arrays, malformed
cross-chunk offsets/indexes, every publication I/O admission, and candidate
cancellation followed by unchanged authority, retry and complete array reopen.
Native checkpoint projection construction now resolves definitions, captures
and hydrates node/relationship records, allocates adjacency entries and inserts
neighbors under actual record units. Ordered neighbor sets preserve sorted,
deduplicated directed edges without an uninterruptible high-degree sort; CSR
and CSC flattening copies at most 1,024 neighbors per unit. The explicit
analytics path remains a reference. Tests compare all arrays against an
independent edge set and analytics over 1,025 nodes with skew, parallel edges,
self-loops and seven label/type filter combinations, including unknown names.
They cancel capture, hydration, adjacency construction and flattening, restore
all permits and retry. Projected names and definition lists now encode hex
bytes in 64 KiB source chunks and decode hex/UTF-8 in 64 KiB decoded chunks,
with cancellation between names and chunks. A split UTF-8 code point is carried
into the next block; existing hex/UTF-8 errors, empty-name list ambiguity and V1
bytes remain unchanged. Tests cover Unicode crossing the 64 KiB boundary,
4,096 definition names, chunk/list cancellation and malformed input against the
independent legacy text decoder. Locating line and field boundaries now scans
at most 64 KiB per admitted unit and retains only the required fields plus one
excess-field sentinel. Boundary tests match standard line/field splitting for
empty strings, trailing separators, CRLF and Unicode around 64 KiB boundaries,
including whole-codec CRLF compatibility; cancellation stops before a complete
256 KiB line/field has been found. Complete arrays/text/compression retention,
malformed numeric token/diagnostic size, reallocations/drop costs and the hard
byte ledger remain open. The automatic owner's whole-candidate local permit and
memory-accounting gaps remain in place; no complete build bound is claimed.

Native checkpoint overlay capture now admits each live delta record before
cloning it, with cancellation between records for canonical, adjacency and
property-projection inputs. Tombstones remain shared COW roots and the inputs
remain ID ordered. An actual out-of-core candidate test cancels capture before
any I/O, preserves authoritative identity/WAL/manifest, and retries with exact
base/delta/replacement/tombstone parity through publication and reopen. The
complete delta vectors remain retained and individual record cloning has no
new byte bound. Checkpoint-only overlay steps now expose each physical merge
or tombstone decision, so canonical, adjacency, property-projection and graph
projection builders admit and cancel skipped records individually. Ordinary
owned iterators retain their complete-result behavior and physical errors are
never hidden by newer deltas or tombstones. A real 512-record base/511-tombstone
candidate cancels before encoding its survivor: the private file contains only
the 24-byte canonical header/generation, authority is unchanged and retry/reopen
returns exactly the survivor. Synthetic step counts, native node/relationship
factories and physical-error checks cover the step contract. Canonical segment
hydration still decodes a complete bounded segment per read, and retained-memory
accounting and cleanup/drop costs remain open.

Checkpoint graph statistics now admit records, labels, property collection,
index entries and composite key components, path lookup/visits, distinct-set
retirement and histogram sampling. Sampling consumes ordered values directly
in at most 1,024-element chunks, preserving the existing endpoint-inclusive
quantiles without a second complete sorted vector. Basic-count copying and
retained out-of-core statistics copy/filter groups under admitted entries and
values. The existing 100,000-visit global path budget and empty truncated output
remain unchanged; cancellation returns a stopped build rather than publishing
partial statistics. Tests compare complete results against the existing
statistics path and independent chain counts, cover histogram thresholds,
late unsupported groups, scalar/composite index entries, retained stale epochs,
path-budget truncation, cancellation/permit restoration and real candidate
retry/reopen in materialized and out-of-core modes. Full fact-set retention,
individual key/value size, map/vector allocations and error/drop cleanup still
lack hard byte bounds. Statistics serialization and final runtime adoption also
remain outside these builder units. No whole-candidate bound is claimed.

Live append-row capture now admits batch traversal and each row before
copying its table/key ownership. Row payloads remain shared. Sorting uses runs
of at most 1,024 rows, one admitted heap-merge output record, and one adjacent
ordering check per unit, preserving table/partition/order semantics and the
existing row-limit/duplicate/count errors. The candidate calls this controlled
capture path. Complete captured vectors/runs remain retained; individual key
size, allocator/drop costs, append segment encoding, compaction, manifests,
publication and reopening still lack complete builder/resource bounds. These
capture units do not justify releasing the owner's whole-candidate permit.

Private append artifact publication now binds directory creation, exclusive
owned temporary creation, 64 KiB writes, synchronization and durable rename to
the admitted task's I/O waves. Cleanup arms only after creation and disarms
once rename completes; interrupted earlier temporary evidence is preserved,
and a lost reply after rename retains the complete published private artifact.
Ordinary publication keeps its existing writer path and bytes. The controlled
metadata, segment, validation and mount steps below share the same task.
Individual schema/key codecs, compaction and retained cleanup/resource
accounting remain incomplete; publication I/O alone does not establish a
complete append-build bound.

Append manifest preparation now copies schema/watermark entries and at most
1,024 prior segment bindings per work unit. Payload encoding admits each schema
record, watermark and segment entry, copies binary strings/records in 64 KiB
chunks, and hashes/copies the manifest closure in 64 KiB chunks. The integrity
closure hashes borrowed header/payload slices without a second complete
integrity-input buffer. Individual schema cloning and WAL-schema encoding,
individual schema/key codecs, compaction and reader retention still have
uncontrolled inner work, and complete buffers lack a hard byte ledger. These metadata units do not establish whole-append-build bounds.

Private append segment construction now scans each partition boundary once,
admits individual ordering/partition/row/value/descriptor work, and encodes
variable-width row payloads and escaped keys in 64 KiB source chunks. Overflow
sample/compression policy and envelope bytes remain unchanged, with compression,
copying, checksums and digests performed in 64 KiB units. Segment compression,
payload/directory copies and integrity likewise use bounded chunks; the segment
body digest hashes borrowed directory/payload slices instead of building a
second complete body buffer. Ordinary segment/row/key/overflow codecs remain
separate references. Variable-width key comparison/descriptor cloning, vector
reallocation, full-buffer retention and Vec-to-Arc conversion still require
hard byte/time accounting. Append compaction and complete
schema/key codec, reader-retention and allocation accounting remain open. These operations do not authorize removing the
whole-candidate permit or claiming a complete append-build bound. Regressions
compare the complete 2,049-row multi-table/partition/overflow segment and its
reopened rows against the ordinary encoder, verify large Unicode/NUL keys and
all scalar row tags, exercise raw/compressed overflow fixtures with cancellation
at every admitted completion, and stop/retry block/directory/compression/copy/
integrity steps with a one-operation class limit and four-operation total limit.

Private append publication validation now admits each schema, previous
binding descriptor, live watermark update, generated watermark, partition
maximum and row-table lookup. Generated maxima are checked directly against
borrowed partition keys instead of cloning a singleton schema map for every
table. Ordinary validation remains the independent result/error reference.
The regression compares every one of 4,097 generated rows across 37 partitions,
combines base and suffix watermarks, verifies identity/schema/unknown-table and
missing/regressed/invalid generated-order errors, and cancels/retries with
released permits and unchanged prior authority. Individual schema/key cloning
and comparison, validation allocation/drop, append compaction and reader
retention still lack complete byte/time control. This validation boundary alone
does not prove whole-append resource bounds.

Private append generation mounting now reads manifests and complete segment
integrity in admitted 64 KiB I/O waves. Manifest integrity hashes borrowed
header/payload slices, and decoding checks each schema/watermark/binding and
binding pair under a work unit. Segment directories admit each descriptor and
ordering pair; generation mounting admits each binding and partition watermark
before validation. The checkpoint candidate uses this path, while ordinary
readers remain result/error references. Individual schema decoding/cloning,
variable-width key decoding/comparison, full directory/manifest allocations,
reader retention and append compaction still lack complete byte/time accounting.
Regressions compare complete manifest decode/state and corruption diagnostics
against the ordinary codec, cancel at every actual mount I/O admission and
representative descriptor/entry boundaries, preserve both published files,
release all work/I/O leases, and retry to an exact 512-row reopen. No full
append resource or cancellation bound is established by these mounts.


Private append compaction now checks descriptors and payload totals per row,
reads compressed blocks in admitted 64 KiB I/O waves, and decompresses in 64 KiB
units. Row traversal, overflow-reference closure validation and hydration have
separate work boundaries; sorted runs and heap outputs reuse the controlled
capture sorter. The private path avoids populating serving reader caches.
Compaction preserves existing row/payload deferral limits, but distinguishes
work admission/cancellation from decoder budget rejection so a stopped candidate
cannot silently proceed as an incremental publication. Individual row/key and
overflow decoding, variable-value cloning, retained buffers and allocation/drop
costs still need inner byte/time accounting. These boundaries do not establish
a complete compaction cancellation or candidate memory bound. Full issue
qualification remains required.

Catalog capture now shares its fixed set of immutable collection roots rather
than copying every schema name/descriptor under the publication guard. Schema
mutation detaches only the changed collection, preserving old source and
candidate identities. The public catalog APIs and serialization inputs remain
unchanged. This addresses schema-size-dependent capture copying; DDL detachment,
last-owner destruction and retained-source
admission still need bounds and foreground release measurements. It does not
establish a complete capture-time or commit-p99 guarantee.

Store statistics capture and prepared-state adoption now share the advanced
statistics root. Basic label/type counters and mutable index samples live in
separate paged COW maps, so updating a sample after capture does not detach all
histograms and path facts. Public statistics DTOs and checkpoint formats remain
unchanged. Controlled materialization and prepared-state sealing admit each
counter/sample and preserve cancellation or QoS rejection without mutating the
source. Full snapshot/write isolation and out-of-core checkpoint/reopen coverage
also preserves initial basic-only and retained advanced-statistics behavior.
This removes database-sized statistics copying from capture/adoption, but page
directory copies, individual DDL detachment, DTO/output buffers, allocations and
final-owner destruction still lack complete byte/time bounds and resource
accounting. These changes do not qualify the entire candidate or commit p99.

The checkpoint V1 text path now validates borrowed search changes in bounded
ordering runs and encodes individual catalog/statistics fields, vector values
and captured keys with cooperative work. Hex conversion emits at most 64 KiB
per unit; formatted fields copy at most 64 KiB through a UTF-8-safe sink without
allocating a complete formatted line. The ordinary encoder remains an
independent complete-byte and validation-order oracle. Body/envelope checksums,
compression, digest calculation and metadata file writes consume controlled
chunks. The shared metadata publisher exclusively creates its temporary file
and arms cleanup only after successful creation. Tests cancel every actual I/O
wave, preserve unowned evidence and old manifest/sidecars, and retry the complete
decoded image. A cancelled reply after complete rename retains the published
private artifact. Complete output/intermediate buffers, reallocations, recursive
value depth, ordered-key output buffers/comparisons and cleanup debt still need hard
resource/time bounds. This does not establish whole-candidate admission safety.

The private legacy relational image encoder now traverses borrowed schemas,
rows and overflow closure under cooperative work boundaries. Variable-width
schema/default/row bytes and payload writes use 64 KiB chunks. File-backed
overflow reads bypass serving caches, retain registered file identity and verify
range CRC32C and full overflow SHA-256 in controlled chunks. File publication
uses exclusive temporary ownership, controlled sync/readback integrity and
rename, preserving complete private artifacts after lost responses. Captured
relational search keys reuse the controlled ordered-key encoder, including
wide Unicode, embedded zero bytes and escaped binary keys. The ordinary codecs
remain independent complete-byte and error-order references. Regressions cover
all 512 logical rows, every actual read/publication I/O admission, representative
CPU/final boundaries, QoS rejection, unchanged source/manifest/sidecars and
complete retry/reopen. Whole schema/row/output buffers, reallocations and map
comparisons still need hard byte/time accounting; relational decode/mount,
row-page/index/overflow publishers and cleanup debt remain incomplete. These
boundaries do not qualify full candidate resources or final-writer latency.

The private relational reopen path now gives the ordinary parser a controlled
64 KiB buffered file input. Inspection, header opening and file-identity
registration acquire I/O admission separately; payload reads/integrity and
bounded buffer copies release their work and I/O leases between units. Logical
positions and complete-request truncation checks remain separate from read-ahead.
Full 512-row/schema/index-posting parity covers both index load modes. Tests
cancel every actual mount I/O wave and representative/final input units, compare
13 ordinary corruption/budget diagnostics, deny admission before I/O and retry
all rows without modifying the source. Parser allocations, UTF-8 validation,
row/schema validation, page construction, reachability/foreign-key traversal,
index reconstruction and reader retention still require controls and accounting.
Read-ahead may discover a physical I/O failure before an earlier malformed
payload field; both paths must fail closed. Input control does not establish
a complete mount cancellation, memory or time bound.

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
