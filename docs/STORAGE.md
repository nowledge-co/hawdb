# Storage Design

## Current V1 Storage

The current storage implementation is a small durable graph store slice. It is
not an LSM tree and it does not depend on RocksDB or another storage engine.

Files:

- `manifest.skein`: the only published generation pointer. It records the
  checkpoint generation, checkpoint commit epoch, canonical artifact metadata,
  WAL generation, durable replay LSN, next LSN, reader watermark, and optional
  Source sidecar publication.
- `checkpoint.<generation>.skein`: catalog, schema, projection definitions,
  stable operational metadata, and optimizer statistics for one generation,
  written through the default zstd compression envelope. Canonical graph rows
  are delegated to the generation's canonical artifact.
- `canonical.<generation>.skein` and
  `canonical.<generation>.manifest.skein`: immutable ordered node and
  relationship segments with per-segment digests, record bounds, adaptive
  endpoint Bloom filters, and exact-property Bloom summaries.
- `adjacency.<generation>.skein` and
  `adjacency.<generation>.manifest.skein`: rebuildable, generation-bound
  canonical adjacency used by out-of-core traversal.
- `property-index.<generation>.skein` and
  `property-index.<generation>.manifest.skein`: rebuildable, generation-bound
  persistent property projection. The property spill artifact remains part of
  canonical checkpoint input and is not eligible for derived repair.
- `wal.<generation>.skein`: append-only committed mutation records beginning at
  the replay LSN published by the manifest.
- `projected_graphs.skein`: checksummed, checkpoint-generated CSR/CSC
  projection artifacts derived from persisted projected graph definitions,
  written through the default zstd compression envelope.
- `stable_ids.skein`: checksummed persisted stable-ID mapping for records that
  do not carry an `id` property at the physical export boundary, written
  through the default zstd compression envelope.

Recovery:

1. Load `manifest.skein` when present and verify its checksum.
2. Reject unsupported manifest storage versions before using manifest state.
3. Load the checkpoint generation named by the manifest.
4. Verify the checkpoint length, CRC32C, and SHA-256 identity.
5. Reject unsupported checkpoint storage versions before importing records.
6. Replay valid WAL entries in order. When configured, the WAL replay entry
   limit is checked after a record is decoded and before applying it.
7. Reject every database open on a torn WAL tail or checksum mismatch without
   modifying the WAL. `DatabaseDoctor::plan_wal_tail_repair` is a read-only
   dry run for an incomplete final frame. The exact generation-bound plan must
   be acknowledged and passed to `DatabaseDoctor::apply_wal_tail_repair`
   before any bytes are discarded.
8. In materialized mode, rebuild in-memory adjacency and property indexes. In
   out-of-core mode, retain the immutable canonical reader and keep only the
   bounded mutation delta resident.
9. Validate that every recovered relationship references existing source and
   target nodes before accepting the graph state.
10. Verify projected graph artifacts when present. Corrupt artifacts are
   discarded because they are rebuildable derived state, not canonical graph
   state.

Checkpoint publication writes the new canonical artifact, canonical manifest,
checkpoint image, and next WAL generation before atomically replacing
`manifest.skein`. The manifest's durable replay LSN prevents a crash between
checkpoint persistence and old-WAL reclamation from replaying checkpointed
mutations twice. Checkpoint, manifest, and projected graph artifact publication write a
temporary file, sync the file contents, atomically rename it into place, and
sync the parent directory. Checkpoint and projected graph artifact payloads use
zstd inside the required V1 checksummed binary envelope. Manifest and WAL files remain plain text so boot
metadata and append-only mutation records stay inspectable and avoid compression
work on every mutation. This keeps publication durable while avoiding
per-mutation directory syncs, manifest writes, or WAL compression write
amplification.
Stable-ID mapping publication uses the same synced temp-file rename and parent
directory sync boundary as checkpointed artifacts. It is intentionally outside
the graph WAL: first physical export may create mapping entries, but that action
does not mutate graph records or increase WAL replay work.
Search projection snapshots use the same temporary-file, file sync, atomic
rename, and parent-directory sync boundary when `SearchIndex::checkpoint`
publishes `search_projection.skein`; the snapshot uses the same zstd envelope
by default. The projection remains rebuildable and is not part of canonical
graph WAL recovery. Snapshot publication streams the header and one encoded
document record at a time through zstd into a temporary payload, then copies
that payload behind the checksummed envelope before publication. It does not
materialize a corpus-sized plaintext `String` or compressed `Vec` on top of the
resident build state. `SearchIndex::checkpoint_with_report` exposes the
uncompressed and compressed byte counts, largest encoded record, and published
out-of-core generation so qualification can verify the bounded writer path.

`SearchOutOfCoreGenerationWriter` is the larger-than-memory rebuild boundary.
It accepts complete `SearchDocument` values in strictly increasing UTF-8 ID
order, validates embedding identity and finite values, and writes checksummed
length-delimited records to a private same-directory spool. Document count,
logical input bytes, spool bytes, record bytes, descriptor fields, descriptor
working bytes, lexical spill, complete published-generation bytes, and
compressed/uncompressed segment sizes all have explicit admission limits.
Finalization replays the spool instead of
collecting the corpus: one bounded document segment builds the descriptor,
document payload, metadata sidecar, and raw-vector sidecar, while the lexical
writer performs its existing bounded external posting sort. Immutable artifacts
are synced before the active out-of-core manifest is atomically replaced. A
failed input or finalization removes the private stage and leaves the previous
active manifest readable. Mutable checkpoints and streaming finalization share
one process-aware, cross-platform file lease so concurrent publishers cannot
reuse a generation or switch the active manifest backwards; the persistent lock
inode is safe after a crash because ownership is released by the operating
system rather than by deleting a marker.
After a successful switch, the writer applies the same bounded active-plus-one
generation cleanup policy as mutable checkpoints and reports deletion failures
as retry-required evidence without invalidating the durable generation.

The streaming writer deliberately does not publish the mutable compatibility
snapshot, so `SearchIndex::open()` is not accidentally turned into a
larger-than-memory serving owner. `SearchOutOfCoreReader` reopens the generation
with zero resident documents. Raw vector sidecars remain sufficient for exact
bounded vector and hybrid search; TurboQuant candidate construction and its
production qualification remain a separate derived-generation gate. Run
`cargo bench --bench search_generation` for the default 100,000-document build
profile and its RSS, page-fault, spool, descriptor, payload, and peak-segment
evidence. [`SEARCH_GENERATION_BENCHMARK.md`](SEARCH_GENERATION_BENCHMARK.md)
records the reproducible resident-versus-streaming kernel comparison and keeps
it explicitly separate from representative Mem qualification.

WAL entries can represent either a single mutation or a batch commit record.
The relationship pattern create path uses a single batch record for source node,
target node, and relationship creation. Recovery only applies a batch after its
whole record passes checksum validation, so a torn tail cannot leave behind a
half-created path.
Doctor torn-tail repair applies only to the final physical record when it is not
newline-terminated. A newline-terminated record is a complete frame: malformed
UTF-8, a missing or invalid checksum, or a checksum mismatch is corruption even
at the end of the WAL, so recovery fails closed instead of truncating a
potentially acknowledged commit. Doctor is a separate typed operation, not a
database-open mode. Planning holds the exclusive database lease, validates the
manifest identity, WAL generation, framing, checksums, LSN continuity, and
configured scan bounds, and reports the exact retained LSN plus discarded byte
range without modifying files. Applying requires an acknowledgement bound to
that plan, revalidates the manifest and WAL CRC32C/SHA-256 identities, persists
an original-WAL quarantine copy and a prepared audit record, truncates and
syncs the WAL, then publishes an applied audit record. A pending audit record
blocks ordinary open. An interrupted apply can be finalized idempotently only
when the manifest, retained WAL, and quarantine identities still match.

Integrity checks are layered for throughput. WAL records, immutable segment
blocks, cache admission, manifests, and projection envelopes use CRC32C; the
implementation selects hardware acceleration on supported x86-64 CPUs and a
portable fallback elsewhere. Checkpoint and subordinate manifest publication
also records SHA-256. Canonical artifact writers compute CRC32C and SHA-256 in
the same sequential write pass, while request-time reads verify only the block
being admitted to cache. `Database::scrub_storage` is the explicit full scan:
it streams every canonical artifact through CRC32C and SHA-256, validates WAL
framing and LSN continuity, and poisons the open handle on any integrity error.

`max_wal_replay_entries` counts these top-level WAL records, not the child
operations inside a batch, so a budgeted recovery either applies a complete
batch record or rejects the open before applying the next record.

`DatabaseTransaction` owns a transaction-private COW graph workspace. Each
Cypher mutation is applied to that workspace immediately, so later Cypher reads
and mutations observe earlier writes. The transaction retains the exact graph
operations produced by each statement and publishes them, together with staged
relational SQL writes, as one WAL batch and one commit epoch. Rollback drops the
workspace without touching the live store. Statement planning, mutation limits,
and graph constraints are checked against the workspace, so an invalid
statement fails before `COMMIT` and does not alter either the workspace or the
live store.

`ConcurrentDatabase` moves the embedded `Database` behind an in-process commit
coordinator while transaction planning and COW workspace mutation remain outside
the publication critical section. Optimistic transactions use first-committer-
wins validation against their base commit epoch and return an explicit conflict
instead of replaying a stale write set. Pessimistic transactions acquire locks
on first use rather than at `BEGIN`. The lock table supports compatible shared
locks, conflicting exclusive locks, inclusive point locks, bounded ranges with
inclusive or exclusive endpoints, and an unbounded database target that
overlaps every finer-grained resource.

Simple PostgreSQL reads over a primary key acquire shared point or range locks.
Full-table reads acquire the full primary-key range. `INSERT` and `ON CONFLICT
DO NOTHING` acquire exclusive points for the primary key and every declared
unique key, plus shared points for referenced foreign keys. This permits
disjoint primary-key inserts prepared from the same COW epoch to publish in
separate WAL epochs. SQL updates, deletes, schema changes, joins, non-primary-key
predicates, graph mutations, and query shapes whose complete access set cannot
be proven acquire the database target conservatively. The fallback is part of
correctness, not a silent unlocked path.

A pessimistic transaction that waits before its first successful statement
refreshes its private snapshot after the lock is granted. Acquiring a new lock
after an earlier successful statement fails with a retryable serialization
error if the published epoch changed, avoiding execution against a resource
that changed before it was protected. Lock-complete disjoint writes may rebase
their exact staged operations onto the current store at commit; constraint
validation and the durable publication order still run against current state.
Both transaction modes retain one WAL order, durable-before-publish, and one
commit epoch per transaction.

Coordinator waits record every blocker in a multi-owner wait-for graph. Adding
dependencies that close a cycle aborts the current waiter as the deterministic
deadlock victim and releases all of its locks. Wakeup, timeout, commit,
rollback, and drop remove both held locks and wait dependencies.

Every durable open acquires an exclusive process-lifetime lease on the database
directory. The host opens one root `Database` handle and derives sessions,
transactions, and snapshot readers from that handle. A process-local canonical
path registry rejects duplicate handles in one application, while the stable
`owner.skein.lock` sidecar rejects opens from other cooperating applications on
Windows, Linux, and macOS. The sidecar is not canonical state and remains in
place after close so every process locks the same file. Lock contention fails
immediately; it never waits, steals ownership, or falls back to unsafe shared
access.

`DatabaseReadTransaction` owns an immutable catalog and graph snapshot for
read-only Cypher execution. It rejects mutation statements, does not observe
later commits, and remains usable after the writer checkpoints. Active read
transactions register their snapshot commit epoch in a process-local reader pin
registry and unregister on drop. This is an API snapshot slice. The store also
tracks a commit epoch and publishes a checksummed manifest after each successful
checkpoint. The manifest records the checkpoint epoch, the checkpoint-covered
commit epoch, the oldest active reader commit epoch, the safe reclamation commit
epoch, the WAL replay start LSN, and the next WAL LSN. This does not provide an
in-place page-version chain. Read snapshots share immutable COW map pages and
immutable canonical segment readers. A
checkpoint retains all old generations while any snapshot reader is pinned;
after the last pin is released, a later checkpoint keeps the current and
immediately previous generations and reclaims older files.
`Database::storage_reclamation_watermark` exposes the same boundary in
structured form: current commit epoch, optional checkpoint epoch and checkpoint commit epoch, active
oldest reader epoch, computed safe reclaim commit epoch, and whether the store
is durable.
`Database::storage_recovery_report` exposes the strict open-time recovery
boundary in structured form: recovery mode, checkpoint epoch,
checkpoint-covered commit epoch, WAL presence, replay start LSN, next LSN after
replay, replayed WAL record count, configured WAL replay entry bound when
present, recovered commit epoch, and whether the store is durable. Ordinary
open is strict: a torn tail or checksum mismatch fails startup without changing
the WAL. `WalTailRepairPlan` and `WalTailRepairReport` are the separate typed
doctor evidence. The historical `DoctorRepairTornTail` recovery enum value is
retained for report compatibility but database open rejects it.
The CLI command `skein storage-recovery-report [--strict]
[--max-wal-replay-entries <n>] [--require-durable]
[--require-checkpoint-boundary] [--require-bounded-wal-replay]
[--require-clean-tail] <database-path>` opens an existing database read-only
and prints the same report as JSON. Use this as CI or migration evidence for
the real database path, separate from in-memory compatibility fixtures. The
`wal_replay_bounded` readiness flag is true only when the open used an explicit
WAL replay entry bound.

## Out-of-Core Residency

`DatabaseConfig::storage_residency_mode` selects materialized, out-of-core, or
automatic checkpoint loading. Automatic mode materializes small canonical
artifacts and retains larger artifacts behind a bounded `SegmentCache`.
Canonical node, relationship, adjacency, exact-property, executor, search
projection, analytics, and checked snapshot-export paths use owned iterators so
a scan holds at most one decoded segment plus its caller-owned batch or TopN.
Each checkpoint also publishes a generation-bound canonical adjacency sidecar.
Entries are externally sorted by `(direction, endpoint, relationship_type,
neighbor, relationship_id)`. Groups below the dense threshold use one sparse
block; dense groups are split into bounded blocks and store complete
relationship rows to avoid random canonical row lookups. The external merge
uses key-only heap entries, bounded fan-in, and streaming block digests. A
missing or invalid adjacency metadata fails the V1 open rather than falling
back to partial traversal behavior.

`DatabaseDoctor::derived_artifact_health` verifies the full-file CRC32C and
SHA-256 identity of the active adjacency and persistent property projection
against their manifests. `plan_derived_artifact_rebuild` then validates the
canonical segment stream and WAL replay under explicit source-record,
source-byte, replay, temporary-byte, memory, generated-entry, and spill-run
limits. It returns a generation- and source-identity-bound dry-run plan without
changing the database. Canonical segment, checkpoint, property spill, manifest,
or WAL corruption fails closed and is never converted into a derived rebuild.

`apply_derived_artifact_rebuild` revalidates that plan, copies the published
manifest and affected derived files into a checksummed quarantine directory,
and writes a prepared audit record before doing any publication work. It
rebuilds from the verified canonical stream through the bounded external
builders and publishes one new full checkpoint generation with the manifest
last. Ordinary open rejects a pending repair record. If publication is
interrupted after the new manifest becomes durable, applying the same plan
validates the target generation and every quarantined file identity before
writing the applied audit record and unblocking service. It never overwrites a
published derived artifact in place and never silently repairs during open.

Search projection GC is separate from canonical recovery. Lexical, out-of-core,
and TurboQuant artifacts retain the active and immediately previous immutable
generations. `SearchIndex::projection_cleanup_report` exposes attempted,
deleted, deferred, and failed file counts without returning file names, while
`retry_projection_cleanup` runs an explicit retry cycle with bounded deletion
attempts and a bounded pending queue. Open and checkpoint also run the same
cycle. A failed delete, including a Windows sharing violation from a pinned
reader, does not invalidate an already published checkpoint; it remains visible
and retryable, and readiness stays blocked until the backlog is gone.

The cache has a hard byte capacity, stable generation/digest keys, CLOCK
eviction, pin accounting, and fail-closed oversized-entry admission. Endpoint
and property Bloom summaries scale with segment cardinality instead of using a
fixed bitset; false positives fall through to residual decoding and false
negatives are not permitted. `CanonicalReadReport` records considered, pruned,
and read segments, decoded rows, bytes read, and peak segment bytes.

Out-of-core writes copy only touched base records into a mutable delta. Both
foreground WAL append and recovery replay check
`max_out_of_core_delta_bytes` before applying a complete mutation batch. The
admission failure is explicit and never appends or applies a partial batch.
`StorageResidencyReport` exposes canonical bytes and row counts, delta rows and
estimated bytes, the configured delta limit, statistics freshness, and cache
resident, pinned, hit, miss, eviction, admission-rejection, and digest-mismatch
counters.

The ignored
`larger_than_cache_query_reports_process_and_storage_resource_evidence` test is
the reproducible synthetic gate for canonical bytes larger than cache capacity.
It records process RSS, total page-fault deltas, target-specific split fault
counters, intermediate rows, payload bytes, cache residency, evictions, and
rejected cache admissions. Production cutover still requires the same report
from a representative Mem replica through the release-bound typed API.

A first checkpoint that publishes directly into out-of-core mode persists exact
basic counts but does not construct unbounded distinct sets or path maps.
`GraphStatistics::advanced_statistics_complete` and
`StorageResidencyReport::checkpoint_statistics_complete` make that boundary
explicit. Checkpoints written before this flag was introduced remain readable
and retain their historical complete-statistics interpretation.

`Database::refresh_optimizer_statistics_external` rebuilds the advanced
optimizer statistics over canonical base plus WAL delta with explicit memory,
input-record, generated-fact, path-expansion, spill-byte, and spill-run limits.
It emits sorted temporary runs, performs a bounded merge for exact distinct and
path counts, retains bounded deterministic histograms, and removes every run on
success or failure. The refreshed statistics become visible only after a
generation checkpoint publishes them; checkpoint failure restores the prior
live statistics. The report records work, spill, output-state, and publication
measurements. The operation rejects non-durable or materialized stores because
the external refresh is the repair path for stale out-of-core statistics, not a
replacement for the cheaper in-memory computation.

Analytics constructs only the direction required by the selected algorithm,
scans canonical relationships one segment at a time, and checks the memory
budget while collecting node IDs and again before allocating adjacency. The
Source canonical fallback retains only `limit + 1` projected candidates and
has explicit row and payload budgets instead of sorting every Source record in
memory.
`Database::export_canonical_graph_snapshot` and the same method on
`DatabaseReadTransaction` expose the current or pinned graph snapshot as
canonical node and relationship records with a deterministic logical checksum.
The export also carries a stable-identity audit: records with an `id` property
expose that value as their stable ID, while records without one or with duplicate
stable IDs are reported as requiring an external persisted ID mapping before
physical import or delta replay. `CanonicalGraphSnapshotExport::validate`
recomputes the logical checksum and stable-identity audit, checks node and
relationship ID uniqueness, and reports missing relationship endpoints before an
export is handed to an importer, shadow gate, or storage-equivalence oracle.
`CanonicalStableIdMapping` can overlay a caller-persisted mapping for records
that do not carry an `id` property. Applying the mapping recomputes the
stable-identity audit and logical checksum, so `validate().is_import_ready`
remains the gate before first physical import, resumed export, reimport, or
delta comparison.
`Database::export_canonical_graph_snapshot_with_persisted_stable_ids` is the
local physical-export entry point for this path. It generates missing stable IDs
once, writes them to `stable_ids.skein`, and reuses the same mapping after
reopen. The default `export_canonical_graph_snapshot` remains read-only and does
not create persistent export metadata.
`Database::prepare_skein_lightning_bootstrap_export` wraps the same persisted
stable-ID snapshot and the relational state at one database commit epoch in a
Skein Lightning bootstrap manifest. The authoritative logical payload consists
of the GraphStream plus a RelationalStream checkpoint. The latter carries SQL
table schemas, rows, indexes, constraints, and overflow values. The manifest
records both stream checksums and byte lengths, their shared database epoch,
graph schema and row counts, relational table/row/overflow counts, and both
validation reports. Skein Lightning deliberately excludes WAL history, physical
pages, adjacency layouts, statistics, caches, and search or analytics projection
artifacts; those are recovery history or rebuildable physical state rather than
portable user data. The GraphStream remains deterministic canonical text sorted
by labels, relationship type, stable IDs, and endpoints.
The CLI command `skein validate-canonical-snapshot [--require-valid]
[--require-import-ready] <database-path>` opens the database read-only, exports
the current canonical snapshot, and prints the validation report as JSON.
`--require-valid` returns a non-zero status when the snapshot is internally
inconsistent. `--require-import-ready` additionally requires every node and
relationship to have unique stable identity, so the export can enter a physical
import path without first creating an external ID mapping.
The CLI command `skein skein-lightning-bootstrap-manifest [--require-ready]
<database-path>` opens the database read-write, creates or reuses
`stable_ids.skein`, and prints the bootstrap manifest as JSON. `--require-ready`
returns a non-zero status if the manifest's embedded validation is not
import-ready or the relational stream is invalid.
The CLI command `skein skein-lightning-graph-stream [--require-ready]
<database-path>` uses the same bootstrap export path and prints the deterministic
GraphStream text. The final `checksum` line covers the stream body and matches
the manifest's `graph_stream_checksum`.
`skein skein-lightning-relational-stream [--require-ready] <database-path>`
writes the binary relational stream to stdout. It is a component command for
upload pipelines; consumers must preserve the bytes without text conversion.
`SkeinLightningGraphStream::validate_against_manifest`,
`SkeinLightningRelationalStream::validate_against_manifest`, and the CLI command
`skein skein-lightning-verify-export [--require-valid] <database-path>` verify
the local bootstrap artifacts before upload. The report covers GraphStream
format, checksum, count, and endpoint integrity plus relational checkpoint
decode, checksum, epoch, table/row/overflow counts, and manifest agreement.
`skein skein-lightning-bootstrap-bundle [--require-ready] <database-path>`
prints one machine-readable bootstrap evidence bundle containing the manifest,
both stream validation reports, the source database's open-time storage
recovery report, and a ready/blocked export gate decision. Use this as the CI or
upload preflight entry point when the caller needs one JSON artifact instead of
separate manifest and verifier commands. The storage recovery evidence records
the actual open configuration used by the bundle command; callers that require
strict or bounded WAL replay as a hard gate should also run
`storage-recovery-report` with the matching `--require-*` flags. The export gate
keeps a flattened `blockers` list for logs and also reports manifest,
GraphStream, and RelationalStream blocker counts plus grouped blocker messages
so import automation can distinguish snapshot readiness failures from stream
artifact failures without parsing strings.
`skein skein-lightning-stage-bootstrap [--require-ready] <database-path>
<staging-dir>` writes a local staging catalog plus manifest, GraphStream,
`skein_lightning_relational_stream.bin`, and bootstrap bundle artifacts with
atomic file publication and directory sync. The
catalog is the v1 local checkpoint boundary for offline bootstrap upload/resume;
it is outside the graph WAL and does not alter the published graph snapshot. The
catalog also summarizes staged object count, measured byte count, total bytes,
average object size, and per-kind object counts for upload observability.
`skein skein-lightning-verify-staging [--require-ready] <staging-dir>` reopens
that staging catalog without the source database, verifies artifact byte
lengths and checksums, recomputes both stream validations, and checks agreement
between the catalog, manifest, bundle, GraphStream, and RelationalStream
artifacts. Its validation gate keeps flat errors for logs and grouped artifact,
manifest, GraphStream, RelationalStream, bundle, and catalog error arrays for
local upload/resume automation. If the
bundle carries `storage_recovery`, staging verification also checks its protocol,
storage-version presence, and recovered commit epoch against the staged
manifest database epoch, then reports the result in a structured
`storage_recovery_evidence` object. Its artifact summary reports the same count
and byte metrics from the actually measured artifacts. Unknown staging-catalog
or bootstrap-manifest protocol versions block validation instead of being read
on a best-effort basis.
`skein skein-lightning-publish-staging [--require-state-marker]
[--fencing-token <token>] [--expected-database-epoch <epoch>] <staging-dir>
<publish-dir>` verifies a READY staging catalog and atomically writes
`skein_lightning_published_manifest.json`. Repeating the command for the same
manifest is idempotent; attempting to publish a different manifest over an
existing pointer fails instead of overwriting the published database pointer. When
the optional state-marker/fencing preflight is enabled, publish requires a
VALIDATING import marker, matching fencing token, and matching staged manifest
database epoch before writing the pointer.
`skein skein-lightning-verify-published <staging-dir> <publish-dir>` verifies
that the published pointer still references the staged catalog by byte length
and checksum, and that the referenced staging catalog still passes the
source-independent verifier. The report promotes the staging verifier's
`storage_recovery_evidence` to a top-level field so post-publish audit can
inspect recovery readiness without traversing the nested staging report. Its
validation gate keeps flat errors and grouped pointer, catalog, and staging
error arrays so resume automation can distinguish pointer corruption from
staging catalog drift.
`skein skein-lightning-gc-staging-report <staging-dir> <publish-dir>` fails
closed when a published pointer cannot be verified and groups the propagated
published-pointer verification errors for cleanup automation. The GC report also
summarizes total, pinned, and deletable staging bytes so callers can distinguish
published retention from orphan staging space.
`skein skein-lightning-import-status <staging-dir> <publish-dir>` summarizes
CREATED/READY/PUBLISHED/QUARANTINED state and can merge an optional
caller-owned `skein_lightning_import_state.json` marker for
EXPORTING/UPLOADING/MERGING/VALIDATING/FAILED/CANCELED coordinator states. It
groups presence, staging, published-pointer, state-marker, and resource errors
for resume automation. Active coordinator states require the marker to carry the
idempotent retry tuple `import_id`, `task_id`, `fencing_token`, and
`object_digest`; missing fields quarantine the status report before retry. The
report can also summarize an optional caller-owned
`skein_lightning_import_checkpoints.jsonl` append log. Checkpoint entries keep
resume/failure coordinates such as source range, object digest, partition, and
validation rule; object-level checkpoints must include the same idempotent retry
tuple before status automation treats them as resumable. Reusing one complete
checkpoint idempotency tuple for conflicting source-range, partition, or
manifest-digest coordinates blocks the status report instead of allowing resume
automation to amplify a stale retry marker. Checkpoint status also exposes
machine-readable stage, status, failure-rule, and failure-partition counts so
resume monitors can classify progress and failure hot spots without reparsing
the append log. The report promotes `storage_recovery_evidence` from published
verification when present, otherwise from staging verification, so migration
monitors can inspect recovery readiness without traversing nested verifier
reports. The report also includes machine-readable `resume_action`,
`state_marker`, `checkpoint_log`, and `resource_retention` fields that
distinguish staging, publishing, active work, completed, canceled, failed, and
quarantined/manual-repair states without requiring callers to parse
human-readable error strings.
The storage-equivalence regression coverage compares canonical exports from the
same graph after live mutation, WAL replay, checkpoint publication, and
checkpoint recovery, and requires byte-for-byte equal export structures plus a
valid self-validation report.
This is the local export boundary for future GraphStream encoding; it does not
copy local pages, WAL records, adjacency pointers, or rebuildable projection
artifacts.

`Database::storage_version` exposes the currently supported storage version.
`GraphStore::open` also validates the stored version in both the manifest and
checkpoint images at boot. Unsupported versions fail with an explicit storage
compatibility error instead of falling through to a generic parse error or a
checksum-corruption path.

The implementation currently persists:

- node labels
- relationship types
- node records
- relationship records
- relationship properties
- node and relationship table descriptors with durable schema state
- property schema descriptors for node and relationship tables
- unique node-property constraint descriptors
- composite equality index descriptors
- projected graph definitions
- checkpoint-generated projected graph CSR/CSC artifacts
- outgoing adjacency index
- incoming adjacency index

Property indexes cover declared properties only. A write populates the
single-property index for a `(label, property)` pair when a matching equality
descriptor exists in the catalog, and skips it otherwise, so the index cost of
a write is proportional to the properties the schema asked to index rather than
to the properties the record happens to carry. `CREATE INDEX ON :Label(prop)`
declares the pair, backfills the nodes written before the declaration, and
refreshes that pair's distinct-value statistic; without the declaration the
property is simply absent from the index.

Scan pruning is therefore conditional. The pruner offers an index-backed
candidate set only for a declared property and declines for every other one,
which leaves the caller with a full label scan. Declining is what keeps the
optimization sound: an index that covers only part of the data is safe to
consult only where its coverage is known to be complete. Results never depend
on the choice — a query answered from an index returns exactly what the full
scan would. `docs/tla/SkeinPropertyIndexPruning.tla` models both evaluations
side by side and checks them for equality, so a pruning path that read an
incomplete index would surface as a violated invariant rather than as a
silently short answer.

The implementation also maintains rebuildable in-memory property indexes. The
single-property index is keyed by `(label_id, property, value)`, the composite
equality index is keyed by `(label_id, [(property, value), ...])`, and the
full-text candidate index is keyed by `(label_id, property, ngram)`. The
catalog stores persistent equality, composite equality, range, and full-text
index descriptors, while the execution indexes remain rebuildable from
canonical records after checkpoint load or WAL replay. The optimizer can choose
`IndexNodeSeek` for simple label plus property equality predicates when an
equality index descriptor exists, `IndexNodeCompositeSeek` for conjunctions
that bind every property in a composite equality descriptor, `IndexNodeTextSeek`
for `CONTAINS` predicates backed by a full-text descriptor, and
`IndexNodeRangeSeek` for single-bound and conjunctive bounded range predicates
when a range index descriptor exists. Text seeks use the ngram index only as a
candidate source and retain a residual `FilterExec` so exact string containment
semantics remain authoritative. Composite and full-text execution projections
can be rebuilt through `Database::rebuild_bounded_property_index_projections`,
which applies complete descriptor rebuilds that fit a caller-supplied operation
budget and reports estimated operations plus indexed entry counts without
writing WAL. Internal background loops can expose the same work as a
`Projection` `BackgroundWorkPlan` through
`Database::property_index_projection_background_work_plan` or execute it through
bounded background/scheduled wrappers that bind QoS admission to the same
descriptor rebuild budget. Conjunctive range seeks keep the complete `AND`
predicate as a residual filter while using merged lower and upper bounds as the
access path. Statistics now include per-label/property and
per-relationship-type/property distinct counts, one-hop path source/target
coverage distinct counts, bounded multi-hop path source/target coverage
distinct counts, bounded sorted value histograms, and exact-versus-sampled
markers. Histograms use deterministic adaptive samples:
small distinct sets remain exact, medium sets keep up to 256 values, and large
sets keep up to 512 values while always retaining the minimum and maximum
sampled bounds. Range costing uses these histograms for selectivity estimates.
Basic graph counters are maintained incrementally in the store for low-cost
optimizer and monitoring reads: total nodes, total relationships, per-label
counts, and per-relationship-type counts. The wider histogram, property
distinct, and path-cardinality statistics remain rebuildable derived data and
are written to checkpoints for observability and costing. Out-of-core recovery
loads those persisted statistics instead of silently replacing them with
delta-only values. Basic counts remain exact across WAL mutations; wider
statistics retain their checkpoint computation epoch and
`StorageResidencyReport::checkpoint_statistics_stale` remains true until an
explicit bounded statistics refresh is available. A first checkpoint written
directly in out-of-core mode deliberately persists only basic counts and marks
advanced statistics incomplete instead of constructing an unbounded temporary
distinct-value working set.

The catalog also stores persistent property constraint descriptors.
Node unique constraints use
`CREATE CONSTRAINT ON :Label(property) ASSERT UNIQUE`. Relationship unique
constraints use `CREATE CONSTRAINT ON -[:TYPE(property)]-> ASSERT UNIQUE`.
Node existence constraints use
`CREATE CONSTRAINT ON :Label(property) ASSERT EXISTS` or the equivalent
`ASSERT NOT NULL`. Relationship existence constraints use
`CREATE CONSTRAINT ON -[:TYPE(property)]-> ASSERT EXISTS` or the equivalent
`ASSERT NOT NULL`. Constraint creation scans existing canonical records and
fails if duplicate non-null property values already exist for the constrained
node label or relationship type, or if any constrained node or relationship is
missing the required property or stores `NULL`. Subsequent node creation,
relationship pattern creation, merge-created records, and `MATCH ... SET`
updates are validated against the active descriptors before appending the WAL
batch.

Node and relationship table descriptors are persistent catalog metadata created
by `CREATE NODE TABLE Name` and `CREATE RELATIONSHIP TABLE Name`. Descriptor
state transitions are supported through `ALTER NODE TABLE Name SET STATE State`
and `ALTER RELATIONSHIP TABLE Name SET STATE State`, where `State` is
`DELETE_ONLY`, `WRITE_ONLY`, `BACKFILL`, `VALIDATING`, `PUBLIC`, or `GC`. Table
creation also ensures the matching label or relationship type token exists.

Property schema descriptors are persistent catalog metadata created by
`CREATE PROPERTY ON NODE TABLE Name(property) TYPE Type` and
`CREATE PROPERTY ON RELATIONSHIP TABLE Name(property) TYPE Type`, with optional
`NOT NULL`. Supported types are `ANY`, `BOOL`, `INT`, `FLOAT`, `STRING`, and
`LIST`.
Descriptor creation validates existing records for that table before appending
the WAL batch. Later node creation, relationship pattern creation, merge-created
records, and `MATCH ... SET` updates are validated against the active property
schema descriptors before any WAL append. Property descriptor state transitions
are supported through
`ALTER PROPERTY ON NODE TABLE Name(property) SET STATE State` and
`ALTER PROPERTY ON RELATIONSHIP TABLE Name(property) SET STATE State`. Only
`PUBLIC` table and property descriptors participate in write-time and recovery
validation. Promoting a property descriptor to `PUBLIC` validates existing
records before appending the WAL batch.

Schema maintenance is explicit. `Database::run_schema_maintenance` scans table
and property descriptors, validates the next state before appending WAL, and
then writes all selected maintenance operations as one grouped WAL batch.
`BACKFILL` descriptors advance to `VALIDATING`, `VALIDATING` descriptors advance
to `PUBLIC` only after validation, and `GC` descriptors are tombstoned from the
catalog. `Database::plan_schema_maintenance` is a read-only dry-run report for
pending descriptor maintenance, including the source state, target state,
action, and estimated operation count for budget admission.
`Database::run_bounded_schema_maintenance` applies only complete descriptor
maintenance actions that fit a caller-supplied estimated-operation budget, using
one WAL batch for the selected actions and leaving skipped descriptors in their
current state for a later call. This is descriptor-level batching, not yet
inside-object backfill checkpointing. Explicit callers use the direct execution
path without local background admission. Internal
maintenance loops can use the planned wrappers,
`Database::run_planned_background_schema_maintenance` and
`Database::run_planned_scheduled_background_schema_maintenance`, to charge the
current dry-run estimate to the `Mutation` background lane before any descriptor
advancement or maintenance WAL append. The lower-level background wrappers still
accept caller-supplied estimates when the caller already has a stronger external
budget model. For per-tick low-resource loops,
`Database::run_bounded_background_schema_maintenance` and
`Database::run_bounded_scheduled_background_schema_maintenance` bind the same
operation budget to both QoS admission and descriptor-level bounded execution.
WAL replay applies descriptor GC before the database is exposed, while record
pages and index artifacts remain separately rebuildable or reclaimable.

Checkpoint files include a statistics snapshot for observability and future
costing: total node count, total relationship count, per-label counts,
per-relationship-type counts, relationship-type source counts,
label/type/label path cardinalities, bounded exact path cardinalities up to the
current statistics hop limit, per-label/property distinct-value counts, and
per-relationship-type/property distinct-value counts. The statistics snapshot
also records the commit epoch at which it was computed, the histogram sample
limit, and whether each node or relationship property histogram is an exact
value set or a bounded deterministic sample. These statistics are derived data;
the store maintains the basic counter subset incrementally, recomputes the
wider live API view from canonical records, and accepts old checkpoints that do
not contain statistics lines.

Checkpoint also writes `projected_graphs.skein` for every persisted projected
graph definition. The artifact records its format version, projection epoch,
covered commit epoch, node IDs, CSR outgoing offsets and targets, and CSC
incoming offsets and sources. It is checksum-protected and atomically replaced.
Recovery parses valid artifacts into an in-memory cache. Graph algorithm
execution reuses a cached artifact only when the artifact commit epoch equals
the store commit epoch and the stored definition still matches the active
projected graph definition; otherwise it rebuilds from canonical records.
Recovery also filters the in-memory artifact cache with the same commit-epoch
and definition checks, so stale artifacts left by a later WAL replay or changed
projection definition are not exposed through projected graph status metadata.
`Database::rebuild_projected_graph_artifacts` can refresh these derived
artifacts independently of checkpoint publication. `Database::rebuild_derived_artifacts`
wraps the same projected graph refresh in a report-oriented orchestration API
that returns artifact type, name, reusable-state transition, projection epoch,
commit epoch, and graph cardinalities. Neither path appends WAL, truncates WAL,
or publishes a new checkpoint manifest; they only advance the projection epoch
and atomically replace `projected_graphs.skein`.
`Database::schedule_derived_artifact_rebuild` and
`Database::run_next_derived_artifact_job` add a small embedded job state machine
for these projected graph artifacts. Jobs expose pending/running/succeeded/failed
state, attempts, and last error without adding threads or hiding rebuild
failures.
Internal background callers can route the same pending jobs through
`Database::run_next_background_derived_artifact_job` for stateless admission or
`Database::run_next_scheduled_background_derived_artifact_job` for the
caller-driven `LocalQosScheduler` path. The scheduler only tracks running
background operation budgets between start and finish, including optional
per-class budgets for projection, import, analytics, and shadow lanes; it does
not own worker threads, reorder jobs, or gate foreground explicit rebuild
requests.
Callers that maintain their own background loop can build a
`BackgroundWorkPlan` with `BackgroundWorkHint` signals for active topic,
recent delta size, query probability, staleness TTL, freshness SLO, and tenant
budget. Schema maintenance can join the same caller-owned candidate list
through `Database::schema_maintenance_background_work_plan`, which returns a
`Mutation` background work plan using the current dry-run estimate when
descriptor maintenance is pending.
`LocalQosPolicy::evaluate_background_work` returns the existing admission result
plus a deterministic expected-value score and reasons, so the caller can rank or
skip internal background work without moving queue ownership into Skein. If a
candidate carries a tenant budget hint below its estimated operations, the
ranked decision is deferred even when the base background policy would admit it.
`LocalQosPolicy::rank_background_work` and the matching
`LocalQosScheduler` method apply the same evaluation to a caller-owned candidate
list, sort admitted work before deferred/rejected work, then sort by score and
original index for deterministic polling loops.
`Database::schedule_external_content_artifact_job` records content/blob parser
work at the same orchestration boundary. Callers that need structured parser
inputs can use `Database::schedule_external_content_artifact_job_with_payload`
to attach object references, checksums, content type, target projection, or
other application-owned metadata as a `Value::Map`.
`Database::pending_external_content_artifact_jobs` returns a bounded pending
view for caller-owned runtimes that poll parser work without scanning the whole
embedded job history. `Database::failed_external_content_artifact_jobs` returns
the matching bounded failed view for recovery queues and operator-facing parser
diagnostics. `Database::external_content_artifact_job_summary` exposes aggregate
pending, running, succeeded, and failed counts plus the next pending and oldest
failed job ids. It also groups pending and failed counts by action so
action-specific parser, crawler, or embedding runtimes can decide whether to
poll their queue before fetching bounded job details. The graph-kernel job
runner still rejects those jobs with a
graph-kernel-external error and preserves the payload in the failed job report,
while
`Database::run_next_external_content_artifact_job_with` and
`Database::run_external_content_artifact_job_with` let the caller supply the
content artifact runtime and complete either the next pending job or a specific
pending job selected from a bounded poll result. Skein records the state
transition without embedding parsing, crawling, chunking, or large-value runtime
logic. That runtime reads the payload and publishes rebuildable projections back
to Skein through caller-owned output. Successful external content jobs keep the
last structured output rows on the job ledger for lightweight lineage and
operator audit; large parser results, raw bytes, chunks, and projection payloads
remain caller-owned artifacts outside the graph kernel. Bounded succeeded-job
views expose those retained output rows globally or per action without requiring
the external runtime to scan all derived-artifact jobs.
Runtimes that want a standard lightweight lineage shape can use
`ExternalContentArtifactJobCompletion` with
`Database::complete_next_external_content_artifact_job_with` or
`Database::complete_external_content_artifact_job_with`. The completion row
records runtime identity, input/output refs, checksums, projection refs, source
graph epoch, produced-row counts, and small metadata. It deliberately stores
only references and audit metadata, not parser result payloads. The matching
background and scheduled completion runners apply the same standard row shape
while charging parser/crawler work to the `Import` QoS lane.
`ExternalContentArtifactRuntimeManifest` lets a caller-owned runtime declare the
actions it can handle, required payload keys, version, and estimated operation
cost. Skein uses that manifest only to return bounded claimable-job views and an
Import-lane background work plan; it is not a sandbox policy and does not grant
the runtime access to graph-kernel execution.
Internal parser/crawler loops can use
`Database::run_next_background_external_content_artifact_job_with` for stateless
`LocalQosPolicy` admission or
`Database::run_next_scheduled_background_external_content_artifact_job_with` for
`LocalQosScheduler` accounting. These paths charge external content work to the
`Import` class and keep direct caller-owned runtime APIs available for explicit
foreground work. Runtimes that choose a concrete pending job from the bounded
poll result can use `Database::run_background_external_content_artifact_job_with`
or `Database::run_scheduled_background_external_content_artifact_job_with` for
the same Import-lane accounting without falling back to a global next-job claim.
Action-specific runtimes can use
`Database::run_next_scheduled_background_external_content_artifact_job_for_action_with`
for the same accounting without claiming unrelated pending work.
Runtimes that only support a subset of content actions can use
`Database::pending_external_content_artifact_jobs_for_action` and
`Database::run_next_external_content_artifact_job_for_action_with` to poll and
claim only matching actions, such as `parse` or `crawl`, without inspecting or
failing unrelated pending work.
`Database::retry_failed_external_content_artifact_job` explicitly resets failed
external content jobs to pending, preserving the payload and attempt history
while clearing the last error. It does not retry graph-kernel projected artifact
jobs, keeping content parser recovery separate from database-owned artifact
rebuilds.

Search projection rebuild has the same orchestration shape at the search layer:
`SearchIndex::rebuild_derived_artifacts` reports the search projection artifact
type, document counts before and after rebuild, scanned graph nodes, indexed
documents, and lifecycle-marker state. It still uses the bounded all-or-nothing
graph-to-search rebuild path, so a row-limit failure keeps the previous search
projection intact.
Incremental search projection deltas can also run through
`SearchIndex::apply_scheduled_background_projection_delta` or the matching
`Database::apply_scheduled_background_search_projection_delta` facade, which
uses `LocalQosScheduler` to account for in-flight internal background
projection work while keeping the direct delta API available for explicit
foreground callers.
The metadata-only repair path follows the same boundary:
`SearchIndex::repair_background_metadata_from_graph` uses stateless
`LocalQosPolicy` admission, and
`SearchIndex::repair_scheduled_background_metadata_from_graph` uses
`LocalQosScheduler` for in-flight background budget tracking without rewriting
document content or embeddings.

## Durability Policy

The default durability policy is `SyncOnEveryWrite`.

Under this policy, each WAL append is flushed and synchronized before the
committed snapshot is published and the request returns. When the WAL is first
created, its parent directory is synchronized as part of the same durability
boundary.

`SyncOnCheckpoint` remains available as an explicit relaxed policy. It flushes
each WAL append to the operating system without synchronizing every entry, so it
does not satisfy the production response-durability contract.

Callers that need higher ingest throughput SHOULD batch related mutations into
one transaction and one WAL batch rather than weakening the default durability
contract.

## Relationship Locality

The current in-memory adjacency key is:

```text
(node_id, relationship_type_id) -> relationship ids
```

There are two indexes:

- outgoing: `(source, type) -> rel_ids`
- incoming: `(target, type) -> rel_ids`

Checkpoint storage also publishes two physical layouts:

- sparse nodes keep a compact inline/list adjacency representation
- dense nodes use copy-on-write adjacency segments or a B+ tree-like structure
- dense adjacency is ordered by `(direction, endpoint, edge_type, neighbor_id,
  edge_id)`
- hub nodes get isolated storage so they do not pollute ordinary traversal
  locality

## Canonical Property Storage and Projections

Canonical checkpoints keep ordinary properties inline. A top-level value whose
encoded representation exceeds 64 KiB is stored in a generation-bound property
spill artifact instead, and the canonical row contains only its spill ID. Spill
blocks and their manifests are checksummed, read through the bounded segment
cache, included in verified backups, and reclaimed with their canonical
generation.

Declared range and full-text indexes are also published as rebuildable,
generation-bound projection artifacts. Their builders use a bounded external
sort with explicit resident-memory, spill-byte, run-count, merge-fan-in, key,
and generated-entry budgets. A range definition becomes incomplete when a key
exceeds its admitted size; reads then fall back to the canonical scan rather
than using a partial index.

Out-of-core range and full-text reads stream candidate IDs from a complete
projection, fetch the canonical row for an exact residual check, skip base rows
overridden or deleted by the WAL delta, and finally scan the bounded delta.
An absent rebuildable projection permits the canonical fallback. Published
metadata with a corrupt manifest or block fails closed.

## Segmented Lexical Projection

Persistent search checkpoints publish a rebuildable
`search_lexical.<generation>.skein` artifact followed by a checksummed
`search_lexical.manifest.skein`. The manifest binds the artifact to the source
graph epoch, analyzer digest, document snapshot digest, corpus length totals,
and immutable document-length and posting blocks. Builds use bounded external
sort runs and bounded fan-in merges; readers admit one bounded block per query
term and verify artifact and block checksums.

The fallible persisted search path reads only blocks that can contain analyzed
query terms. It computes exact document frequency and average document length
for the authorized metadata candidate set, merges a bounded upsert/delete
mini-delta, and retains only the text page window or hybrid rank window in a
streaming TopK. Reports expose posting bytes read, candidate postings visited,
the exact matching-document count, and whether segmented BM25 was selected.
Checkpoint replaces the base generation and clears the mini-delta. A stale
rebuildable projection falls back to the reference scorer; a declared corrupt
artifact fails closed.

Checkpoint also publishes immutable, generation-named document descriptor,
full-document payload, metadata-only sidecar, vector-only sidecar, sidecar
layout, and lexical manifest artifacts before atomically switching
`search_projection.out_of_core.manifest.skein`. The checksummed layout binds one
metadata and vector range to every descriptor segment. A reader opened before
the switch remains pinned to its generation; an interrupted publication leaves
the previous manifest readable. Segment checksums, descriptor, layout, and
lexical-manifest checksums, document/analyzer digests, source graph epoch, exact
entry counts, and explicit compressed and uncompressed segment limits fail
closed on mismatches. Decompression itself is limited to the admitted byte
count rather than trusting the envelope length.

`SearchOutOfCoreReader` opens only these manifests and descriptors, not the
complete document snapshot. Metadata and ACL predicates prune descriptors,
decode only metadata sidecar ranges, and write matching ordered document IDs
into a temporary, bounded, cross-platform candidate spill. The spill keeps one
bounded ID block in memory; its fallible membership checks feed exact
candidate-scoped BM25 corpus statistics. Scalar vector search reads only vector
sidecar ranges and retains the page window, hybrid rank window, or an explicitly
bounded full-score map. Full title, content, embedding, and metadata payloads
are not touched until final-page hydration. `SearchOutOfCoreReader::hydrate_documents`
provides the same row and payload budgets for projection-owned source chunks or
other payloads.

The read report separates metadata-sidecar, vector-sidecar, and hydration
payload bytes, and also exposes range reads, peak decoded sidecar and full
segment bytes, candidate spill and reread bytes, vector bytes, and final
hydration rows and bytes. `NowledgeMemOutOfCoreSearchProjection` is the typed Mem
read facade over this path. It uses exact scalar vector segment scans; callers
that require a compressed vector projection receive an error rather than a
silent fallback.

The mutable compatibility `SearchIndex::open()` still materializes the complete
snapshot because its rebuild, incremental mutation, and borrowed-document APIs
require stable references. It is a maintenance owner, not the larger-than-memory
production read owner. `NowledgeMemOpenOptions::with_qualified_out_of_core_search_projection`
is the production read-owner boundary: it accepts explicit reader budgets and
opens through the production qualification constructor only after the release
identity, projection generation, source graph epoch, and opened graph commit
epoch match. The admitted embedded handle then uses that pinned generation for
candidate search, bounded hydration, Knowledge Retrieval graph-context
expansion, and Cypher vector seed reads without opening the full-residency
index. Production traffic activation remains gated on representative
differential and resource-profile artifacts for the exact release.

## What This Is Not

The remaining page-store gaps are explicit:

- no multi-process writer protocol; directory ownership is exclusive
- no fine-grained graph-property or relationship-range locking; unsupported
  Cypher and PostgreSQL access shapes conservatively acquire the database target
- no per-key version stamps for validating a newly acquired resource against an
  older transaction snapshot; such transactions abort and retry on epoch drift
- no in-place page-version chain; snapshots use immutable COW pages and pinned
  canonical generations
- no columnar property segments
- no database-owned blob/content parser runtime
- production-sized resource evidence from a representative Mem replica remains
  a cutover artifact rather than a property established by unit tests

The current storage path is suitable for a single embedded owner with bounded
out-of-core Cypher scans and recovery. Typed compatibility reads scan canonical
base plus mutation delta and fail closed on canonical or declared projection
corruption. Direct,
read-transaction, streaming, EXPLAIN ANALYZE, and system SQL reads default to
100,000 rows and 64 MiB of returned payload; a host can request an unbounded
result only by explicitly setting both database limits to `None`. The store
must not be described as a general multi-writer page store. Individual spilled
values remain subject to the configured value-size admission limit.

## Deferred Storage Extensions

The active production backlog is maintained in `TODO.md`. Richer optimizer
statistics and caller-owned blob/content parser integrations are optional
extensions until an active route and measured workload require them; they are
not implicit storage-completion tasks.

Production Search activation is governed by
`specs/PRODUCTION_READINESS_SPEC.md`. Routes may move to
`NowledgeMemOutOfCoreSearchProjection` only after generation-bound differential
parity and representative resource qualification pass.
