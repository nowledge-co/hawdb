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
Private suffix finalization seals immutable row/index recovery prefixes
and mounts them by their exact prefix manifest and WAL fence. It does not replace
the shared recovery selectors used by ordinary opens. It prepares exclusively
owned selector hard links in the same directory, then selects them only inside
the authoritative publication attempt. Cancellation removes owned unselected
links and preserves existing selectors; an uncertain publication retains the
immutable closure and fails the live writer closed. This relies on same-volume
hard links and the existing rename/directory synchronization assumptions.
Each seal flushes the retained builder before cloning its manifest descriptors,
so later suffixes extend run/page ordinals without rewriting a pinned prefix.
The contiguous source digest can seal a prefix without consuming its hash
state. Only the selected prefix receives the ordinary immutable-generation
alias. The ordinary recovery publication and reader paths remain independently
usable. These prefixes do not establish total memory/disk retention or a
foreground pause bound; descriptor cloning, old prefix files and reader mounts
still need complete admission and cleanup accounting.

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
Replay finalization runs before selector publication; the deferred-reclamation
entry requires it and leaves generation scanning/version pruning out of that
entry. The automatic owner seals and mounts captured prefixes while new commits
remain admitted. It drains existing sync groups and compares the complete
writer identity with the sealed prefix. If that identity advanced, it resumes
background catch-up on the same pinned base rather than replaying or mounting
under the final gate. A publication I/O wave is acquired outside the gate and
released before waiting for an existing foreground sync group. After that
group finishes, the same candidate seals its new complete prefix and retries.
The owner serializes captured sources, selector
publication, frontend handoff and off-gate retirement. Manual checkpoint
sources retain a suspension through preparation and selection; branch
switching/sealing, backup, row-page and overflow compaction coordinate with
the same owner.

Focused multi-prefix coverage retains the same immutable base bytes through
four graph/relational suffixes in both durability policies and both Shadow and
Authoritative index modes. It checks idempotent seals, contiguous candidate WAL
growth, complete graph properties, every relational value and primary-key
posting, and direct read-only recovery before writable recovery. The same test
fails against the prior consuming finalizer. The source-digest test compares
each reusable prefix identity with a fresh consuming digest of the complete
same prefix. These controls do not qualify the full lifecycle campaign,
columnar parity, hard resource ledger, or release foreground performance.
An owner regression parks three actual sealed prefixes, admits intervening
foreground writes, drains a real WAL sync group with one I/O slot, preserves
the same immutable base, adopts the complete prefix and checks ordinary reopen.
It verifies that the background I/O wave is released before waiting for that
group. A deliberately wrong runtime retaining that wave fails the same test
with one active background I/O slot instead of zero; its first log is preserved
and the original source is restored before ordinary qualification.

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

Shutdown now takes parked sources, unadopted selected checkpoints, retired
state and task ownership after joining the worker. It releases them outside
the publication mutex, with selected/retired admission retained until storage
and builder destruction completes. Retaining a report/control observer cannot
keep those sources, open locks or admitted background resources alive. If disk
authority was selected but never adopted, discarding that handoff fails the
old frontend closed; immutable recovery evidence remains on disk.
Two actual-owner regressions keep the control alive across stop, check all
background task/I/O/memory reservations are released, and directly open
read-only before writable recovery with the complete committed property.
Both unchanged regressions fail against the former join-only implementation:
the selected case retains a background task and the denied case retains its
parked source. These cuts do not establish the full shutdown/fault matrix or
complete retained-resource accounting.

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

The controlled relational parser now validates schema columns/defaults,
primary/unique/foreign/index metadata and row scalar types under separate work
boundaries. A borrowed name/position map preserves first-error precedence and
avoids repeated linear column lookups or cloning column names. Ordinary decode,
schema and row validators remain independent references. Complete 1025-column
positions, all row scalar variants, 23 schema-error and six row-error priorities,
representative/final cancellation and admission denial/retry have focused
coverage. Variable-width comparisons/diagnostics, parser allocation and UTF-8
validation, primary-key cloning, row-page construction, index/foreign-key and
closure traversal, retained maps and drop still need byte/time accounting and
controls. These validation boundaries do not qualify full mount resources.

Private relational reconstruction now clones primary/index keys through
64 KiB UTF-8/binary copy boundaries, moves owned rows and postings into capped
pages, borrows index definitions, and traverses overflow closure and foreign
keys under separate work boundaries. It avoids collecting all table names and
avoids detaching/cloning a complete existing index segment before replacement.
Ordinary decoding and index/foreign-key builders remain independent references.
Focused coverage compares all 1025 parent and child rows, every unique/declared/
foreign-support posting, more than one index and posting page, row byte/page
boundaries, forward/backward ranges and exclusive bounds, nullable unique/FK
semantics, and primary/unique/declared FK targets in both index load modes.
Diagnostic priority, representative/final cancellation, admission denial,
source retention and full retry have targeted coverage. Bulk posting page
boundaries may differ from the ordinary incremental builder while preserving
ordered logical contents and existing page caps. Whole decoder buffers and
UTF-8 validation, map comparisons, allocation/reallocation, directory retention
and final-owner destruction still need hard byte/time accounting. These
controls do not qualify whole-candidate memory, time or production admission.

Private relational field decoding now reads/initializes binary values and
validates/copies UTF-8 through 64 KiB blocks. Up to three incomplete scalar
bytes carry across blocks; the first UTF-8 failure retains the ordinary global
byte offset. Whole-field truncation is checked before payload reads, and an
invalid string still consumes its complete field before cumulative byte-budget
validation and UTF-8 reporting, preserving ordinary error precedence. Nested
schema/key/row vectors use separate capacity and per-entry push boundaries;
overflow payload hashing and optional retention have per-block controls.
Private row construction shares the decoded owned values vector directly,
without an Arc-slice allocation and complete array copy. Normal construction
shrinks host-supplied spare capacity first. Logical values, slice access,
snapshot sharing and COW isolation remain unchanged. The extra owned-vector
header changes resident row overhead; both page estimators now include it.
Focused coverage compares complete fields and a 1025-column schema/row against
the independent ordinary decoder, valid and invalid split scalars, global UTF-8
offsets, byte-budget/truncation/input-failure precedence, every overflow hash
unit, representative/final cancellation, admission denial and full retry.
Pointer identity verifies owned vector transfer and retained snapshots verify
COW isolation. Capacity allocation, actual retained capacities, collection
comparisons, final-owner destruction and source/candidate/retention accounting
still need the shared hard resource ledger and release RSS/latency evidence.
These controls do not qualify whole-candidate memory/time or production QoS.

Overflow publication-input validation now streams RAW payloads and Zstd
output through 64 KiB blocks, hashes the borrowed complete envelope in blocks,
and retains only CRC32C state plus up to three incomplete UTF-8 bytes. It does
not construct and discard a complete hydrated value. Header, digest, admission,
decompression/declared-length, checksum and global UTF-8 diagnostic precedence
matches the independent ordinary decoder; caller hydration budget is committed
only after complete validation and a final cancellation check. The checkpoint
publisher binds validation to its existing admitted task in ordinary, retaining
base and exact-reference publication modes. Ordinary publisher callers retain
their independent decoder path. Focused tests compare both codecs/scalar types,
complete generation artifacts and every reachable value in all three modes,
corruption/budget/UTF-8 diagnostics, cancellation at each actual validation unit,
and publication cancellation/denial with unchanged base authority and full retry.
Zstd workspace/internal execution, retained input buffers, descriptor reads,
sorting, publication I/O, temporary-file ownership and cleanup debt still need
their own controls and hard resource accounting. This is validation coverage,
not whole-publisher memory/time, power-loss or production admission qualification.

Actual row-root construction now shares the same admitted task. Visit each
base descriptor through controlled fixed-record and 64 KiB key reads, complete
binding CRC/SHA validation, capped comparisons and copies; release every CPU and
I/O lease before nested callbacks. Root inventory/merge bookkeeping processes
individual entries, transfers the deletion set instead of cloning it, and
compacts fixed-size generation accounting one entry at a time. Root key and
136-byte descriptor writes and artifact integrity, flush and sync use separate
controls. Binding integrity remains generation/ordinal/prefix/lower/upper exact,
and ordinary root readers/writers remain the complete-byte/error references.
Targeted regressions build all 1025 pages through three ordinary publications
under the unchanged default 512 MiB dirty-publication budget, then compare all
1025 descriptors and their complete page values,
callback re-entry, each actual CPU/I/O cancellation and denial/retry, twelve
corruption/range/truncation cases, wide common-prefix key reads/hash/comparisons,
and complete incremental artifacts/all 1024 survivors and physical occupancy.
Schema clone/equality, initial reader opening, relocation page reads/decoding,
preflight sort/closure scans and manifest/selector publication remain separate
work. Collection comparisons/allocations, retained capacities, destruction,
disk/FD/cleanup debt and shared hard resource bounds remain incomplete. This
partial binding does not authorize production per-unit admission or prove the
whole-candidate resource, fault/platform or release-performance requirements.

Row-root manifest generation now binds schema shape validation, schema encoding,
table/occupancy accounting, key-bound comparisons, field copies and complete
root/manifest integrity to the admitted task. The existing index-shadow schema
identity is hashed directly from borrowed fields without a complete schema
identity buffer; zero-escaped text/binary defaults use a reused 64 KiB scratch
buffer, including all-zero inputs. Ordinary schema/manifest encoders remain the
complete byte, digest, decoded-value and diagnostic references. The canonical
manifest binding is computed before immutable publication. Focused regressions
cover all 1025 tables, columns, unique/foreign/index entries and scalar/default
forms, wide Unicode/zero fields, all actual encoding/escape/hash cancellation
points, admission denial with unchanged source and full retry, and 24 invalid
metadata/schema/budget diagnostic cases. These synthetic metadata fixtures do
not qualify physical page recovery; complete publisher/recovery suites provide
separate evidence. Preparation, publication and reader controls are described
separately below. Collection comparisons, relocation/page validation, allocator/
retention/drop and shared hard disk/FD/cleanup bounds remain open. These controls
are insufficient to enable production per-unit admission or close the whole
issue's resource, lifecycle, power-loss/platform and release-performance gates.

Actual row-page file creation, capped 64 KiB manifest/selector writes, file
synchronization, immutable publication and latest selection now acquire separate
CPU/I/O controls. An admitted publisher defers on a contended publication lock
instead of blocking. Publication completes its existing synchronization barrier
once started; cancellation after rename retains the complete immutable prefix,
and cancellation after latest selection may lose the response while the complete
generation remains selected. Every actual CPU and I/O cancellation point in all
three publication modes verifies the selected authority, complete retained bytes,
recoverable complete generations, exclusive temporary cleanup and full retry
with a fresh generation whenever immutable evidence already exists. Separate
regressions cover lock contention and loss of the selector response, then reopen
and compare all rows and physical generation ownership. Preparation and reader
controls are described separately below. Relocation reads/decoding, retained
capacities, allocator/drop costs and shared
hard disk/FD/cleanup-debt bounds require separate qualification. These file
controls do not enable production per-unit admission or qualify the whole
lifecycle, modeled power-loss/platform behavior or release performance.

Row-page preparation also binds schema validation/digests, borrowed schema
equality and schema copying, deletion inventory, dirty-page checks and overflow
reference discovery to the admitted task. Schema equality retains the ordinary
total-order semantics for float defaults, including NaN payloads and negative
zero. An in-place heap sorts dirty-page bounds through capped comparisons and
fixed-size swaps; equal bounds still fail the existing overlap check. Root
resource preflight accounts each table/page entry separately. Overflow closure
lookup admits each fixed 120-byte descriptor read and search step without
hydrating values or holding a lease across nested work. Reference regressions
compare all 1025 schema entries and sorted pages, scalar/default values and
every field's negative control, rejection diagnostic priority and absence of
candidate artifacts, actual cancellation/denial/full retry, and all 1025
referenced values. These controls do not bound map/string comparisons, vector
capacities, allocator/reallocation/destruction, relocation, or the shared
retention/disk/FD/cleanup ledger. Reader controls are described separately below.
The complete publisher
and physical recovery suites remain separate from codec/sort fixtures; default
availability, lifecycle/model/platform and release-performance gates stay open.

Row-root reader opening and decoding bind to the admitted task. Latest, generation-specific
and canonically bound manifest opens now pass the checkpoint task through file
inspection, capped reads, complete integrity, table/schema decoding and artifact
length inspection. Borrowed schema decoding reuses the ordinary field decoder
with controlled 64 KiB reads and schema validation. UTF-8 decoding carries at
most three scalar bytes across blocks and retains global error offsets. The
automatic preparation path uses these controls for its captured row-page base
and private compaction report; ordinary recovery readers remain independent
references. New metadata fixtures exercise complete values, corruption and
binding diagnostics, cancellation and denial with unchanged source/full retry.
Nested schema-shape and digest validation retain typed work failures through
the existing codec diagnostics: cancellation and QoS denial abort with admission
errors, while ordinary invalid metadata retains its original corruption class.
All eight reader fixtures and the complete 58-test row publication suite pass.
Synthetic metadata does not prove
physical page recovery; complete publisher/recovery suites provide separate
evidence. Full manifest/schema buffers, actual capacities, allocator/drop,
relocation/page-view decoding and
shared memory/disk/FD/cleanup bounds remain open. This does not enable production
per-unit admission or complete any whole-issue lifecycle, platform or benchmark
gate.

Actual automatic row-page preparation now binds page encoding and writes to
the admitted checkpoint task. Ordered primary keys, each scalar/value directory
entry, row/page directory entry, variable payload/bound copies and integrity
hashing have separate controls, with at most 64 KiB per copy/hash unit. Full slot
padding initializes in blocks after one reservation; page I/O and its artifact
hashing, flush and sync use separately released I/O waves. Dirty-page bounds
also use controlled keys, including relocated pages. The ordinary encoder/writer
remain independent complete-byte and diagnostic references. Tests cover all
1025 rows/columns, every scalar type, wide Unicode/zero bytes/common-prefix keys,
complete artifact parity in all three modes, each actual encoding/writing CPU
and I/O cancellation, denial, unchanged source authority and full retry.
Allocation and growing-buffer recopy during reservation, actual retained
capacities and destruction remain incomplete. Schema/digest, descriptor
merge/sort and publication controls are described above. Page-view validation,
relocation reads and cleanup debt still need controls and shared hard resources.
This partial binding does not authorize production per-unit admission or claim
whole-publisher memory/time, power-loss or release performance qualification.

Row-page publishers now exclusively create and own their five temporary
artifacts, cleaning only successful creates before releasing their publication
lock. Writers receive the already owned files rather than reopening/truncating
paths. Every preexisting required temporary fails explicitly; non-selecting
candidates preserve an unrelated latest-selector temporary. Failure cleanup
retains renamed immutable artifacts. Tests cover each of five temporary stages
across ordinary persistence, compaction and latest selection, all selected base
artifact bytes and rows, full immutable candidate hydration and retry, and every
pre-selector fault phase with unrelated evidence retained. This local ownership
contract does not bound row encoding/merging, allocator/drop, disk/FD usage or
cleanup debt, and it is not a power-loss or production-admission qualification.

Overflow publishers now create each temporary artifact exclusively and record
cleanup ownership only after that create succeeds. Cleanup runs before the
serialized publication lock is released and never removes an unowned temporary
or a renamed immutable artifact. A non-selecting candidate also retains an
interrupted latest-selector temporary rather than deleting unrelated evidence.
An existing temporary causes an explicit creation failure when that path is
required. Already renamed generation artifacts survive a later selector-create
failure. Ordinary and controlled publication share this ownership rule; input
validation still has an independent ordinary decoder reference. Targeted tests
cover every temporary stage in all three candidate modes plus latest selection,
both validation paths, base-authority bytes, unpublished cleanup, complete
retained generation recovery and retry, and QoS denial before claiming evidence.
Cleanup failures leave disk evidence, but shared maintenance-debt accounting,
retry, disk/FD admission and bounded publication I/O remain incomplete.

Overflow publication now binds input ordering, fixed descriptor traversal,
artifact construction and publication to the checkpoint work context. Controlled
ordering uses an in-place heap with at most three fixed digest comparisons and
one input-record swap per primitive, avoiding an entire sorted-output buffer.
Descriptor reads/encoding, exclusive file creation, 64 KiB writes and integrity,
file synchronization and immutable publication acquire individual CPU/I/O
boundaries; waves end before nested validation/builders. A controlled publisher
defers on a busy publication lock instead of entering a blocking lock wait.
Latest-selector reads admit the ordinary size budget, reject malformed lengths,
and read/validate the fixed 208-byte manifest through controlled operations;
physical read-error priority can differ from the ordinary decoder while both
fail closed. A cancellation after rename retains published private evidence;
retry uses a fresh generation. Cancellation after a completed latest selection
can lose the response while the complete result remains selected and recoverable.
Targeted regressions compare complete bytes/all 1025 values, first-duplicate
diagnostics, sort cancellation, every actual I/O wave in all three candidate
modes, unchanged base authority, cleanup and complete fresh-generation retry,
and completed selection after a lost reply. Exact-compaction reference iteration
now admits each in-memory entry or capped-buffer spill record independently.
Spill merging uses an explicit heap with fixed comparisons/swaps per unit;
visitors run after releasing CPU and I/O leases so nested builders can acquire
the same scheduler. Ordinary iteration remains an independent full ordering,
deduplication and corruption-error reference. Regressions cover all 1025 unique
references with duplicates across multiple runs, nested admission, each actual
unit and I/O-wave cancellation with unchanged source files and full retry,
metadata conflicts and spill corruption, early-stop and callback cancellation,
and complete exact-publication values/three artifact images for a spilled closure.
Sort/spill construction, collection capacities, retained FDs, final-owner cleanup
and shared resource accounting remain incomplete. The controlled encoded-extent
read now initializes,
reads and hashes at most 64 KiB per CPU/I/O unit and verifies the complete CRC/SHA
before returning the owned Vec. Exact publication borrows that vector directly
instead of converting it into an Arc slice with another whole-value copy. The
ordinary reader remains an independent checksum/error reference. Regressions
compare every input byte, range/metadata/CRC/SHA diagnostics, each actual read
unit and I/O-wave cancellation with released leases and unchanged authority,
denial before I/O and full exact-compaction values/artifact bytes. A complete
encoded value is still retained and its allocation occurs before block reads;
these controls do not bound allocator latency or actual retained capacity.
Retained inputs,
allocator/capacity costs, final-owner destruction, disk/FD/cleanup debt and the
shared hard resource ledger remain incomplete; these primitives do not qualify
whole-candidate resources, production per-unit admission or release performance.

Ordinary and metadata-only checkpoint overflow-input collection now consume the
admitted work context in the actual automatic preparation path. Empty tables,
empty dirty-page lists, rows and each scalar field have separate boundaries;
fixed-digest collection preserves conflict/reachability diagnostics and sorted,
deduplicated input bytes. Inline inputs retain their existing shared ownership,
while file-backed inputs with a selected base remain references without I/O.
First-generation materialization uses the captured-identity private reader,
bypasses serving caches, and preserves read-before-accumulated-byte-admission
ordering. A reserved output vector avoids growing-buffer recopy during reads;
detached shared bytes initialize at most 64 KiB per unit in a private uninitialized
Arc and convert only after every byte is initialized. Cancellation drops the
private representation before it can be exposed. Targeted tests compare all
1025 ordinary/delta inputs, shared inline pointers, conflict/closure/source
errors, every actual collection/copy/read unit and I/O cancellation, full retry,
all file bytes and unchanged serving-cache pins/counters. Private reads can
surface physical failures hidden by an ordinary cache hit. Map allocation and
mutation, complete retained capacities, allocator latency, destruction and
shared memory/disk/FD/debt accounting remain incomplete; these boundaries still
do not authorize production per-unit admission or a whole-candidate resource
or release-performance claim.

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

Background maintenance now carries a shared allocation controller through task
clones and children. Explicit allocations charge their requested capacity and
the concrete ownership lease before allocation; a smaller task ceiling does not
reset the shared ledger. Critical memory pressure rejects new allocations.
Closing execution rejects future allocations and releases CPU/task/I/O slots,
while the complete original governor and process-memory reservations survive
until the last actual allocation lease drops. This deliberately conservative
retention does not readmit execution or create another memory reservation.
Foreground admissions retain their existing controller construction path.
The facade exports the typed memory-controller, lease and error contracts.

Controlled manifest reads use a private byte buffer that reserves its actual
capacity before fallible allocation and rejects allocator overgrant. Growth
reserves the complete replacement while the old buffer remains charged, copies
in 64 KiB units and destroys old bytes before releasing their lease. Failed
appends restore the original byte length; all copied source bytes survive
cancellation and a complete retry without leaking either capacity. Existing
metadata/file-size limits and source bytes remain unchanged. The encoded input
buffer holds its lease through decode and releases it after its bytes die.
Allocator bookkeeping/rounding and allocation/destruction latency remain stated
platform assumptions requiring RSS and release measurement evidence.

Immutable overflow copies now keep their exact Vec capacity and concrete shared
cell in one allocation lease. Shared input clones retain that same lease, with
no API to detach the allocation; payload bytes are destroyed before the lease.
The shared cell accounts the pinned standard Arc layout's two reference counters;
its allocator metadata/layout and final deallocation remain platform assumptions.
The controlled file-backed collector carries these owners through a dedicated
checkpoint-write input. Borrowed inline inputs and ordinary publication remain
independent. Logical equality compares references and complete encoded bytes,
so ordinary and admitted inputs produce identical immutable files.

Actual-governor regressions retain empty, single-byte and 64 KiB+1 copies after
execution closes, deny empty shared ownership under a one-byte ceiling, and
reject two overlapping complete buffers by exactly one byte before retrying
every byte. Both original ownership regressions fail against the former untracked
Arc copy. A file-backed collector/publication regression clones every admitted
input, compares all three immutable artifacts with the ordinary publisher,
closes execution, hydrates every value through ordinary generation open, and
checks the reservation survives until the final inputs drop. Seven byte-buffer,
six overflow-input and 32 publisher tests pass with strict workspace Clippy;
every preceding copy cancellation/denial cut and fixture remains exercised.
The first additional overlap probe bypassed the shared controller by exceeding
the static task ceiling; its failure is retained, and the corrected probe uses
the unchanged ceiling to observe shared usage. Source values, limits and the
one-byte denial assertion are unchanged. Input vectors, reference maps and
source ownership still require their own admission; this
does not qualify the complete candidate or enable production per-unit work.

Private file-range reads now admit both the complete return capacity and the
simultaneous scratch capacity before fallible allocation. Scratch initialization
and payload copying use 64 KiB work units. The read unit and I/O wave end before
the separately admitted copy starts, including under a one-operation limit.
Returned bytes retain their allocation lease after execution closes; the private
legacy overflow encoder uses an owning borrowed/allocated value instead of
detaching that lease into a raw Vec. Inline borrowing, captured file identity,
digest diagnostics and serving-cache bypass remain unchanged. These buffers do
not account for file-handle retention, parser/maps or the entire candidate.

Two actual-governor regressions fail against the former raw range Vec: its
reservation disappears while returned data remains alive, and scratch overlap
is never denied when only the complete payload fits. Their byte-identical
fixtures/assertions pass after correction. Further regressions cancel every
completed unit of a three-chunk read and fully retry all bytes, and retain typed
digest-mismatch/truncated-file diagnostics without leaking buffers or I/O slots.
The initial test fixture omitted its declared I/O wave width; that failure is
preserved separately from the corrected before/after ownership reproductions.
Reader, buffer, overflow collector and legacy image tests and strict workspace
Clippy pass on the recorded code snapshot. Allocator metadata/latency, complete
resource qualification, supported-profile/full regression and release evidence
remain separate gates; no whole-issue completion follows from these tests.

Private overflow input lists now admit their complete element capacity, concrete
shared cell and pinned Arc counters before fallible allocation. Immutable clones
and owned iterators retain the same list allocation after execution closes;
they cannot detach its capacity into a raw Vec. Elements retain their existing
inline/shared-byte ownership independently. The collector produces digest order
from its reference workspace; checkpoint publication checks that immutable order and
duplicates before any artifact I/O. Ordinary Vec collectors/publication keep
their existing signatures and sorting path. Two hidden checkpoint publication
methods accept the retained list directly. Allocator layout/metadata, final Arc
header deallocation and allocation/destruction latency remain assumptions.

Two actual-governor regressions fail against the former raw input Vec and pass
with identical bodies after correction: retained inline-only inputs lose their
reservation after task close, and a one-byte task never denies the nonempty list.
Additional tests retain a complete sparse-delta iterator through exhaustion and
task closure, verify clones consume no new list capacity, and reject overlapping
lists by exactly one byte before a full retry. Ten collector, 32 ordinary/private
publisher, seven buffer, 24 reader and eight legacy image tests pass with strict
workspace Clippy on the recorded code snapshot. Ordinary sort
scratch, mounted/captured state, disk/FD and cleanup debt remain separate gaps;
this does not enable production per-unit admission or qualify the full candidate.

Controlled full and sparse-delta overflow collection now replaces its untracked
reference BTreeMap with an admitted fixed-capacity reference vector. A bounded
count pass determines the complete capacity including duplicate occurrences;
allocation is fallible and rejects allocator overgrant. In-place heap sorting
compares at most three fixed-size references and swaps one pair per work unit,
without allocating map nodes or sort scratch. Separate bounded deduplication
preserves complete conflicting-metadata diagnostics and ordinary digest order.
The reference capacity remains charged through input allocation and collection,
then dies before its lease is released. Inline/shared source bytes still have
their independent ownership; source retention is not established by this vector.

Two actual-governor regressions fail against the untracked map and pass with
identical bodies after correction: the live reference working set is absent
from the measured peak, and a ceiling that covers only retained inputs wrongly
allows the complete build. A further test cancels every actual completed full
and delta collection unit, verifies the same real reservation has no leaked
capacity, and retries every result. The existing overlap fixture now includes
the measured simultaneous reference capacity; its exact requested-byte/one-byte
shortfall assertion and all source/clone/full-retry checks remain. Its initial
failure under the new truthful peak is archived. All 13 collector tests pass,
including 1025 distinct values, duplicate references, every former cancellation
cut, file publication and complete ordinary diagnostics. Allocator latency,
remaining builders/maps/source ownership and disk/FD/cleanup debt, full profiles,
regressions/fuzz and paired release performance remain qualification gates.

The controlled schema digest now admits its 64 KiB escaped text/binary scratch
before fallible allocation, initializes it in bounded units and retains its
lease through the final terminator. Scalar defaults admit a fixed 32-byte
capacity before fallible allocation and use the unchanged ordinary codec; the
buffer drops before its lease. Null, boolean, integer, floating-point and UUID
encodings cannot grow that capacity. Unsupported overflow defaults retain the
ordinary diagnostic even when the available memory cannot admit a scalar buffer.
Borrowed schema fields and the ordinary identity bytes remain unchanged.

Two actual-governor regressions fail against the former untracked scratch and
scalar Vecs and pass with identical bodies after correction. Five focused tests
cover one-byte scratch shortfall, every actual cancellation unit and full retry
on the same reservation, critical-memory-pressure recovery without readmission,
all scalar forms, empty and multi-chunk binary values, Unicode/zero escaping,
source identity and unsupported-default diagnostics. Source schema/map ownership,
allocator metadata/latency, other output buffers and complete candidate resources
still need separate qualification. These tests do not qualify the full issue.

The row-page publisher's complete cancellation matrix repeatedly recreated and
synchronized the identical durable source for every CPU/I/O cut. Two full
current-main commands and one unchanged isolated diagnostic reached the existing
900-second limit; complete logs/XML and source identities remain archived. The
fixture now publishes each immutable generation-1 source once, links its complete
authority into each fresh case namespace and synchronizes that directory before
opening the base. Every candidate/cancellation/retry still executes its real
barriers, every CPU/I/O cut and all three modes remain, and every prior assertion
is retained. Added assertions compare each case's starting authority and the
unchanged shared source after every case. Required qualification still uses the
original deadlines, budgets, jobs and complete commands. The complete combined
source tree `735343c6287105eaf91648f8a9249fc6339ed1bd` passed the unchanged full
storage command (1240 passed, 29 ignored), owner/concurrent/graph/pipeline/cascades
regressions, all 96 actually executed mandatory fuzz targets, formatting,
all-file hooks and minimal-native/browser-WASM Clippy. These results precede the
schema scratch/scalar correction above, which requires its own final-source
qualification. The first combined-tree storage run observed an overflow
publication lock-busy failure; its unchanged isolated case and full-command
retry passed. Original log/XML hashes are retained and the contention cause
remains unproven. No whole-issue completion follows from the retry.
The first linked-source fixture omitted creation of the case directory and
failed before the first candidate attempt; its full logs/XML remain archived.
The revised fixture creates that namespace before linking/synchronizing the base.

The complete 92-test QoS suite and five storage memory tests pass, covering
shared/narrowed/concurrent capacity, cancellation, critical-pressure recovery,
retained bytes after execution closes, shared process-policy retention,
replacement overlap, allocator failure and every actual growth/copy cancellation
with full retry. Actual manifest opening rejects its encoded-buffer allocation
one byte below the required capacity, then retries with all decoded metadata and
all five source files unchanged. The preceding allocation-controller source
(`a13a3f563a403c31315b4d1ea218283d213076fd`, tree
`0b6a8fb9c429db2118db0f56f57b59e9ec56c4ac`) passed strict workspace,
minimal-native and WASM Clippy, 162 checkpoint-unit tests, five publication
tests, seven candidate tests, nine owner tests, 59 relational publication
tests and the full storage harness (1205 passed, 29 ignored). Its original
mandatory 96-target fuzz command had one original-limit timeout; the unchanged
full-command retry passed all 96. Original logs and failure/retry receipts remain
retained. These are scoped source checks, not full issue qualification.
Only explicit lease users are tracked:
remaining schema/value/manifest output buffers, maps, graph/append builders,
pinned source/state ownership, disk/FD and cleanup debt still require binding
and qualification. This does not authorize production per-unit admission or
establish any whole-candidate memory, platform or release-performance bound.

Checkpoint debt now reads the existing graph-delta estimate in constant time.
Segmented COW maps maintain the same record, adjacency and posting weights
through bulk construction, insert/replacement, removal and exclusive mutation
guards. Snapshots copy the aggregate with their immutable page directory;
checkpoint/replay generations own their own aggregate. An internal `u128` sum
preserves exact subtraction after a public `u64` estimate saturates. Guard and
retain cleanup also reconcile weights during unwinding, including empty pages
in a partially retained directory. `get_mut` and `entry_or_default` now return
a mutable guard rather than a bare mutable reference; callers release that
guard before another mutation of the same map. Weight implementations must be
stable under shared borrowing and must not panic. This changes a storage-internal
API without changing the facade's host mutation contract or a persistent format.

The automatic owner consumes either WAL or out-of-core delta debt at the exact
70% soft threshold, alongside the existing monotonic age. The estimate is enabled
only with an out-of-core canonical base and its configured delta cap. Reads do
not hydrate records or enumerate map pages. Changed values still incur their
existing logical-value estimate on mutation; this is not a hard allocation or
latency bound. Preparing-phase backpressure checks both limits using the existing
90% threshold and maximum-WAL-record reserve. That reserve is conservative for
the tested small-node stream, but is not yet a proven delta expansion bound for
base hydration, index fanout or multi-operation transactions. It does not
establish default sustained-write availability or the final handoff latency gate.

Four focused storage regressions pass: a 1057-entry mutation/snapshot sequence
with instrumented nonhydrating reads, saturated-total recovery, partial retain
and mutation unwinding, and ordinary persistent mutation/checkpoint/reopen parity
against the independent preceding full-scan estimate in both residency modes.
Two focused owner regressions pass: exact 6999/7000/7001-byte boundaries with a
10,000-byte delta cap independently of WAL/age pressure, and 320 complete
512-byte values across multiple automatic generations and normal reopen with
a configured 16 KiB delta cap. These configurations are explicit fixtures, not
default-resource qualification. Broad COW/storage, supported-profile and
mandatory fuzz qualification of this delta-pressure source remain required.

The current whole-candidate operation estimate can exceed the default local
QoS operation limit on a large database. Remaining bounded cancellable build
units and builder-specific memory/retention accounting remain required; the
provisional whole-candidate memory estimate also omits append-state retention
and compaction allowances. Full delta-pressure resource/availability coverage,
columnar-shadow parity, the full cancellation/fault/model/platform matrix and
paired release performance qualification are also incomplete. Whole-candidate
memory estimates and narrow idle tests do not prove these requirements.
No full issue completion is claimed.
