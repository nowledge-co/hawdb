# Storage Design

## Current V1 Storage

The current storage implementation is a small durable graph store slice. It is
not an LSM tree and it does not depend on RocksDB or another storage engine.

Files:

- `manifest.hawdb`: the only published generation pointer. It records the
  checkpoint generation, checkpoint commit epoch, canonical artifact metadata,
  WAL generation, durable replay LSN, next LSN, reader watermark, and optional
  Source sidecar publication.
- `checkpoint.<generation>.hawdb`: catalog, schema, projection definitions,
  stable operational metadata, and optimizer statistics for one generation,
  written through the default zstd compression envelope. Canonical graph rows
  are delegated to the generation's canonical artifact.
- `canonical.<generation>.hawdb` and
  `canonical.<generation>.manifest.hawdb`, plus
  `canonical-segment-descriptors-<generation>.pages.hawdb` and
  `canonical-segment-descriptors-<generation>.root.hawdb`: immutable ordered
  node and relationship segments with per-segment digests, record bounds,
  adaptive endpoint Bloom filters, exact-property Bloom summaries, and the
  artifact's property key table. The compact manifest retains only aggregate
  counts and the exact descriptor root. Point, scan, and iterator reads demand
  descriptors without making startup residency depend on segment count.
- `adjacency.<generation>.hawdb`,
  `adjacency-descriptors-<generation>.pages.hawdb`, and
  `adjacency-descriptors-<generation>.root.hawdb`: rebuildable,
  generation-bound canonical adjacency blocks plus the demand-paged descriptor
  tree used by out-of-core traversal. The outer manifest binds the exact data
  and descriptor-root identities; there is no separate adjacency manifest.
- `property-index.<generation>.hawdb`,
  `property-index.<generation>.manifest.hawdb`,
  `property-index-descriptors-<generation>.pages.hawdb`, and
  `property-index-descriptors-<generation>.root.hawdb`: rebuildable,
  generation-bound persistent property projection. Its compact manifest binds
  the exact descriptor root instead of retaining one descriptor per data
  block. Production reads demand-traverse the ordered descriptor prefix and
  fetch only selected data blocks through the shared bounded cache. The
  property spill artifact remains part of canonical checkpoint input and is
  not eligible for derived repair.
- `properties.<generation>.hawdb`,
  `properties.<generation>.manifest.hawdb`,
  `property-spill-descriptors-<generation>.pages.hawdb`, and
  `property-spill-descriptors-<generation>.root.hawdb`: canonical large graph
  property values plus a same-generation demand-paged descriptor tree. The data
  artifact becomes durable before descriptor pages and the compact root. The
  compact manifest retains aggregate counts only; point hydration seeks one
  descriptor and reads one selected block without making startup metadata
  resident in proportion to the spill block count.
- `wal.<generation>.hawdb`: append-only committed mutation records beginning at
  the replay LSN published by the manifest. The only v1 encoding uses binary
  fragment framing with the `SKWALB01` file header. Text WAL bytes and unknown
  headers fail closed rather than entering a compatibility or upgrade path.
- `projected_graphs.hawdb`: checksummed, checkpoint-generated CSR/CSC
  projection artifacts derived from persisted projected graph definitions,
  written through the default zstd compression envelope.
- `stable_ids.hawdb`: small checksummed selector for the independently
  generated stable-ID export/import mapping used by records whose `id`
  property is missing or non-unique. It binds one immutable
  `stable_ids.<generation>.hawdb` artifact. Only the selected generation header
  is opened eagerly; fixed-size, independently checksummed mapping pages remain
  cold and are demand-read through the shared segment cache. Old generations
  remain available while pinned and are reclaimed only after the final pin is
  released.

### Reader compatibility refusals

Relational WAL and full-checkpoint envelopes currently use codec version 3;
embedded append WAL records (including checkpoint schema records) use version 3.
A reader accepts its supported version exactly. An intact envelope requiring a
different version returns `HawDBError::UnsupportedStorageFormat` with
`StorageFormatError::UnsupportedVersion { format, found, supported }`.
An unknown materialized-view expression tag returns `UnsupportedFeature` with
the schema format, feature name, and numeric tag. `StorageFormatError::format()`
is always one of the constants exported from `hawdb` and `hawdb_core`:
`STORAGE_MANIFEST_FORMAT`, `RELATIONAL_DURABLE_CODEC_FORMAT`,
`RELATIONAL_SCHEMA_FORMAT`, `RELATIONAL_ROW_PAGE_FORMAT`,
`RELATIONAL_CHECKPOINT_LAYOUT_FORMAT`, `APPEND_WAL_FORMAT`,
`LOGICAL_DUMP_FORMAT` or `SEARCH_PROJECTION_MANIFEST_FORMAT`. Hosts compare
against these constants instead of matching message text. They are recorded in
`docs/persistent-format.lock`. Current relational schema tags
0 through 5 represent column, COUNT(*), COUNT(column), SUM, MIN, and MAX.
This change does not add a feature bitmap or change any encoded bytes.

`RELATIONAL_CHECKPOINT_LAYOUT_FORMAT` refuses intact checkpoints this reader
cannot serve in the requested configuration, with `UnsupportedFeature` whose
tag is the checkpoint generation:

- relational rows stored only in the canonical row-page root, without a
  full-row `relational.<generation>.hawdb` file, opened in any mode other than
  `OutOfCore` residency with `Authoritative` relational indexes. Releases with
  [#1737](https://github.com/nowledge-co/hawdb/issues/1737) write that layout,
  as does any `OutOfCore` + `Authoritative` checkpoint;
- `Authoritative` relational indexes on a checkpoint that never published a
  persistent relational index generation.

Both are configuration or layout mismatches, not corruption; the open
changes no database file (each branch's disposable `runtime/` mount is
rebuilt). A missing index binding on a database without a checkpoint, and
binding or identity mismatches, remain `StorageIntegrity`.

Compatibility refusal is distinct from malformed data, invalid reserved fields,
truncation, or checksum failure in supported formats. Streaming checkpoint readers finish integrity
verification before reporting an unsupported capability, retaining cooperative
cancellation and I/O admission. A checksum-invalid envelope is still corruption.
The Python binding uses `StorageError` with `kind="unsupported_storage_format"`,
not `IntegrityError`; readiness reports retain the same distinct error class.

Project metadata may open without admitting a branch's data. Data admission and
Mem startup propagate compatibility refusal before publishing readiness. Legacy and branch
admission check selected checkpoint envelopes/schemas and complete sealed/active
WAL records before cleanup or runtime initialization. This adds a bounded validation pass over
those records; it uses the captured active WAL handle and existing decode limits.
WAL doctor planning, apply revalidation, and automatic repair require the same
capabilities, including when resuming an interrupted repair. An unsupported
record followed by a torn tail never authorizes truncation or quarantine.

The storage manifest and checkpoint `version` line (`hawdb-storage-v<N>`)
follows the same rule: an intact version of that family other than the
supported one returns `StorageFormatError::UnsupportedVersion { format:
"storage manifest", found: N, supported }`; any other text is malformed.

Upgrade the reader to a release that supports the writer's codecs and features;
do not downgrade a database containing features that the older reader lacks.
Older binaries cannot acquire this classification retroactively and may still
report corruption. Development-only databases may be explicitly recreated when
their baseline changes, but an open or doctor refusal must not do that implicitly.
No compatibility migration, stable Mem inclusion, or production activation is
authorized by this error-classification change. Retain the original database and
recovery evidence when a reader refuses it.

### Persistent format version

`PERSISTENT_FORMAT_VERSION` (`crates/storage/src/persistent_format.rs`,
re-exported as `hawdb::PERSISTENT_FORMAT_VERSION` and returned by the C ABI's
`hawdb_format_version()`) summarizes every codec above in one integer: equal
values open each other's databases, and a change that makes an existing
database unreadable bumps it. It is not stored in the database; each codec's
own version and refusal stay authoritative. `docs/persistent-format.lock`
pins it together with the persistent codec constants, and the `Persistent
Format` workflow fails while they disagree and states in each release's notes
whether it changed. Development formats still break without migrations;
embedding hosts carry data across a change with a logical dump (export with
the old release, import with the new one). See [LOGICAL_DUMP.md](LOGICAL_DUMP.md).

Recovery:

1. Load `manifest.hawdb` when present and verify its checksum.
2. Reject unsupported manifest storage versions before using manifest state.
3. Load the checkpoint generation named by the manifest.
4. Verify the checkpoint length, CRC32C, and SHA-256 identity.
5. Reject unsupported checkpoint storage versions before importing records.
6. Replay valid WAL entries in order. When configured, the WAL replay entry
   limit is checked after a record is decoded and before applying it.
7. Writable opens default to audited `AutoRepairTornTail` recovery. Repair
   physically incomplete EOF records and, with a valid synchronization fence,
   damaged unsynchronized suffixes. Preserve the discarded bytes and audit
   before truncation. `Strict` rejects damage without modifying the WAL;
   read-only opens require `Strict`. Protected-prefix damage fails closed.
8. In materialized mode, rebuild in-memory adjacency and property indexes. In
   out-of-core mode, retain immutable canonical and adjacency demand readers
   and keep only the bounded mutation delta resident.
9. Validate that every recovered relationship references existing source and
   target nodes before accepting the graph state.
10. Verify projected graph artifacts when present. Corrupt artifacts are
   discarded because they are rebuildable derived state, not canonical graph
   state.

Checkpoint publication writes the new canonical artifact, canonical manifest,
canonical adjacency data, descriptor pages and descriptor root, checkpoint
image, and next WAL generation before atomically replacing `manifest.hawdb`.
The manifest's durable replay LSN prevents a crash between checkpoint
persistence and old-WAL reclamation from replaying checkpointed mutations
twice. Checkpoint, manifest, and projected graph artifact publication write a
temporary file, sync the file contents, atomically rename it into place, and
sync the parent directory. Checkpoint and projected graph artifact payloads use
zstd inside the required V1 checksummed binary envelope. Manifest files remain
plain text so boot metadata stays inspectable. The WAL is binary for every new
generation: records are framed RocksDB-style into 32 KiB blocks as fragment
chains (FULL, or FIRST..MIDDLE*..LAST), each fragment carrying a
`crc32c (4B LE) | length (2B LE) | type (1B) | generation (8B LE)` header whose
masked checksum covers the type byte, the WAL generation, and the payload; a
block tail shorter than one header is zero-filled. Binding the generation into
every checksummed fragment gives recyclable-log discipline: a well-formed
fragment carrying a stale generation reads as end of log. Record payloads keep
the LSN as a fixed envelope field ahead of the batch payload
(`commit_epoch (8B LE) | op_count (4B LE) | ops`), and each op uses hand-rolled
field-tagged varint encoding with append-only op codes and field ids, so
readers skip unknown fields by wire type. WAL records are uncompressed either
way, avoiding per-mutation directory syncs, manifest writes, and WAL
compression write amplification. The V1 text reader, encoder, and repair path
stay in the codebase for generations written before the binary format.
Stable-ID mapping publication uses the same synced temp-file rename and parent
directory sync boundary as checkpointed artifacts. It is intentionally outside
the graph WAL: first physical export may create mapping entries, but that action
does not mutate graph records or increase WAL replay work.
Search projection snapshots use the same temporary-file, file sync, atomic
rename, and parent-directory sync boundary when `SearchIndex::checkpoint`
publishes `search_projection.hawdb`; the snapshot uses the same zstd envelope
by default. An unregistered resident checkpoint is crash-atomic (#1711). It
first synchronizes the projection directory's ancestry, so a directory that
`SearchIndex::open` created cannot lose its entry after a durable selector. It
then writes every artifact under a generation name (the snapshot as
`search_projection.<g>.hawdb`, plus the descriptor, payload, lexical, sidecar
and vector files), each synced and renamed into place. The rename of
`search_projection.out_of_core.manifest.hawdb`, whose receipt already binds the
snapshot's object reference, is the single commit point. Only then are
`search_projection.hawdb`, `search_projection_segment_payloads.hawdb`,
`search_projection_segments.hawdb` and `search_lexical.manifest.hawdb`, in that
order, replaced by hard links (or synced copies) to the selected generation. A loss
before the selector leaves every current name and the selector at the previous
checkpoint. After it, `SearchIndex::open` re-links any current name that differs
from the selected generation, but only from a generation file whose bytes match
the selector's object reference. The repair runs only when the selected
generation snapshot exists and matches its receipt. Older layouts and selectors
from other writers keep the existing validation. Before the selector, every
installed generation file, including the snapshot, is re-read and checked
against the reference the selector records. Reopen also removes alias-link and
generation-write temporaries left by an interrupted checkpoint. On Windows,
which has no stable file identity in std, each reopen hashes the four current
files once. The current and previous generation snapshots stay under the
ordinary content-generation retention. See
[resident checkpoint qualification](SEARCH_SEGMENT_POWER_LOSS_QUALIFICATION.md#resident-checkpoint).

Compatibility: a projection written before #1711 has no generation snapshot
and opens unchanged. The first new checkpoint adds one. After a completed
checkpoint, older binaries see the same current names and selector as before
and open normally. They ignore and never clean up the extra
`search_projection.<g>.hawdb` links. A crash can leave a projection between
its selector and its aliases. If its snapshot alias is still stale, an older
binary refuses it with the existing "resident search snapshot does not match
the published immutable projection" error. Once the snapshot alias has landed,
the older binary's existing identity checks reject any stale descriptor or
lexical alias. It then rebuilds the descriptor or treats the lexical projection
as absent, so it never serves the previous generation's artifacts as the new
one. A newer binary repairs every alias on open. Registered
Consumer checkpoints keep their previous order; their Consumer publication
record selects the producer closure. The projection remains rebuildable and is not part of canonical
graph WAL recovery.

A resident snapshot that disagrees with the selector it is opened beside, such
as the crash state of a HawDB 0.31.0 or earlier checkpoint (#1711) or a damaged
generation snapshot, is refused with the typed
`HawDBError::SearchProjectionMismatch { projection, message }` (#1735). It
displays as `storage error: resident search snapshot does not match the
published immutable projection; incremental catch-up cannot repair it: ...`,
naming `EmbeddedStoreHandle::rebuild_out_of_core_search_projection` and
`Database::rebuild_search_projection` (#1738 changed the text after the
`;`); the C ABI, Bun, Go and MoonBit report
`kind: "search_projection_mismatch"`, and the Python mapping is a `StorageError` with
`kind == "search_projection_mismatch"`, `projection` and `retryable == False`.
Reopening the same directory fails the same way, so it is not retryable as-is:
rebuild the search projection from the authoritative graph, for example with
`EmbeddedStoreHandle::rebuild_out_of_core_search_projection` into a new
directory (see [`SEARCH_PROJECTION_REBUILD.md`](SEARCH_PROJECTION_REBUILD.md)). The check reads only the projection's
own directory. `EmbeddedStore` opens and recovers the graph before the
projection, the refusal writes nothing to it, and a graph-only open of the same
store succeeds with an unpoisoned handle. Other projection refusals, such as a
corrupt or partially written generation artifact, keep their `Storage` or
`StorageIntegrity` kinds. The new variant is a breaking change for exhaustive
`match`es on `HawDBError`.

Snapshot publication streams the header and one encoded
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
bounded vector and hybrid search; RaBitQ candidate construction and its
production qualification remain a separate derived-generation gate. Run
`cargo bench --bench search_generation` for the default 100,000-document build
profile and its RSS, page-fault, spool, descriptor, payload, and peak-segment
evidence. [`SEARCH_GENERATION_BENCHMARK.md`](SEARCH_GENERATION_BENCHMARK.md)
records the reproducible resident-versus-streaming kernel comparison and keeps
it explicitly separate from representative Mem qualification.

An incremental-maintenance projection whose change-log cursor has been trimmed,
or whose directory is damaged or missing, is recovered with
`EmbeddedStoreHandle::rebuild_out_of_core_search_projection`. It pins one graph
read snapshot at epoch E, streams the canonical graph rows (plus host-hydrated
embeddings and host-owned rows) in bounded batches through
`SearchOutOfCoreGenerationWriter::create_unordered_replacement_with_context`,
an external sort into a fresh directory, and stamps the generation with E. A
complete read of the generation and a second scan of the pinned graph must have
the same document count and order-independent content digest before a
no-replace rename installs the private stage at a new target directory. The
current projection directory is never written; the host switches by reopening
on the target, and incremental catch-up resumes from E. Precondition and
verification refusals are the typed, non-storage
`HawDBError::SearchProjectionRebuildRefused`. See
[`SEARCH_PROJECTION_REBUILD.md`](SEARCH_PROJECTION_REBUILD.md).

A projection whose source epoch must catch up with graph commits that changed
nothing it indexes (import bookkeeping, additive schema statements) is moved
with `EmbeddedStoreHandle::advance_search_projection_source_epoch`. It
publishes only a new selector manifest over the active closure, in both the
resident and the incremental out-of-core mode, after proving from the
database's own search change log that every commit in the range has an empty
projection capture. A range with projected changes, a change-log gap or an
unavailable range is refused with the typed, non-storage
`HawDBError::SearchProjectionEpochAdvanceRefused`. See
[`SEARCH_PROJECTION_EPOCH_ADVANCE.md`](SEARCH_PROJECTION_EPOCH_ADVANCE.md).

The search projection selector carries `min_reader_version` when it holds a
field that version-1 readers do not know. A reader refuses a selector that
needs a newer reader before decoding its fields, with
`HawDBError::UnsupportedStorageFormat` (`format() ==
SEARCH_PROJECTION_MANIFEST_FORMAT`) instead of `Storage`. Writers refuse to
publish over such a selector. Version-1 selectors omit the version fields
and keep their earlier bytes. The bump policy is in
[`SEARCH_PROJECTION_MANIFEST_VERSION.md`](SEARCH_PROJECTION_MANIFEST_VERSION.md).

### WAL recovery

WAL entries can represent either a single mutation or a batch commit record.
The relationship pattern create path uses a single batch record for source node,
target node, and relationship creation. Recovery only applies a batch after its
whole record passes checksum validation, so a torn tail cannot leave behind a
half-created path.
Writable opens default to `RecoveryMode::AutoRepairTornTail`; durability still
defaults to `SyncOnEveryWrite`. Recovery never skips over damage to replay
later transactions. It retains the complete transaction prefix before damage:

| Observed bytes | No relaxed synchronization fence | Valid fence, damage outside protected prefix |
| --- | --- | --- |
| Physically truncated fragment header/payload, or a chain awaiting MIDDLE/LAST at EOF | Audited tail repair | Audited tail repair |
| Complete invalid checksum/chain, sequence-invalid fragment, nonzero block trailer | Fail closed | Audited prefix recovery |
| Zero-filled/stale final region or valid fragments after an end-of-log marker | Fail closed | Audited prefix recovery |
| Invalid/missing WAL header, unsupported format, IO error, invalid fence | Fail closed | Fail closed |
| Damage before the fence or published branch prefix | Fail closed | Fail closed |

`Strict` rejects every damaged suffix in this table. A read-only open must
explicitly select `Strict`; requesting automatic repair remains an error.
Read-only CLI, binding and Mem mode convenience paths select `Strict` themselves. The
legacy `DoctorRepairTornTail` enum value remains invalid for open.

The WAL writer appends bytes; it does not preallocate or extend the file ahead
of a write. Nevertheless a filesystem may preserve length or later writes
while losing earlier unsynchronized bytes. An all-zero region, including an
extended zero tail, is therefore not proof of an incomplete EOF record. It
requires a valid relaxed fence before it can be discarded.

### Repair evidence and retry

Explicit doctor planning holds the exclusive database lease and validates the
authoritative selector, checkpoint closure, WAL generation, framing, checksums,
LSN continuity and configured scan bounds. Its plan identifies the retained
length/next LSN and CRC32C/SHA-256 identities of both the retained prefix and
original WAL. Explicit application requires acknowledgement of that exact plan.
Automatic open uses the same protocol under the directory lease.

Protocol `hawdb-wal-doctor-repair-v2` stores only the discarded bytes in
`doctor/quarantine/*.discarded-tail.hawdb`, plus an identity-bound audit. It
streams the retained prefix together with that suffix to verify the original
WAL digest. Evidence and a prepared audit must be durable before truncation;
truncation is synchronized before an applied audit is published. Pending audits
block `Strict`, and automatic open resumes only when all identities still match.
Retry also re-establishes the suffix file/directory and prepared-audit barriers;
a visible rename alone never proves durability. Resuming after truncation
repeats its durability barrier before retiring the pending audit. Old development-only v1 pending records are rejected, not
silently reinterpreted as suffix evidence.

`max_wal_quarantine_bytes` bounds aggregate retained repair suffix bytes,
independently of corrupt-WAL quarantine. Exhaustion preserves prior evidence
and leaves the authoritative WAL unchanged. A same-plan temporary suffix is
resumable without counting it twice. Audit metadata is separately bounded by
its 1 MiB decode limit. No repair may discard bytes before durable evidence.
`HawDBError::WalRecoveryIo` retains the failing operation, IO kind and OS error
code, including Windows sharing/permission failures. A failed operation may
have completed an effect: retry open to resume the audit; do not assume rollback.

`StorageRecoveryReport::wal_repair` exposes `WalTailRepairReport`, including the
typed reason, discarded bytes, retained length/next LSN, evidence directory and
audit/suffix names. `Database::storage_recovery_report` and the typed Nowledge
storage readiness report carry that same value; JSON is derived from it.

### Protected synchronization prefix

A generation admitting `SyncOnCheckpoint` writes creates
`wal.<generation>.hawdb.sync` before its first relaxed append. The fixed 72-byte
SHA-256-protected record binds generation, replay-start LSN, protected byte
length and next LSN. It initially protects every pre-existing byte; it never
retroactively treats earlier synchronous writes as disposable. A missing fence
never grants middle-hole repair, even if the new handle requests relaxed mode.

Before any WAL synchronization in that generation, publish an advanced fence
through a synchronized captured-directory control candidate and directory
barrier. Only then synchronize the WAL and acknowledge the barrier. This order
also covers group flush, explicit source synchronization, failed-append rollback
and repair's own truncate/sync. A completed barrier cannot leave an older fence
that permits deleting its data. An interruption between fence publication and
WAL sync can conservatively protect extra bytes and fail closed. A watermark
written after WAL fsync without its own durable acknowledgement would be unsafe.

Generations that never admitted relaxed writes retain the existing WAL-sync
commit path. Fence publication adds no per-transaction fsync to a continuing
relaxed interval. A transition to synchronous commits on a fenced generation
adds durable fence publication before each sync/group; checkpoint/rotation
starts a new generation. Checkpoint/seal still publish complete durable
checkpoint/immutable WAL closures before advancing their authoritative selector.
Published branch prefixes remain an independent minimum retention boundary.
Generation reclamation retains fence metadata with its WAL generation.

Writable corruption refusals retain the original WAL and existing bounded
content-identified corrupt-WAL quarantine behavior. No valid fragments after
damage authorize skipping an acknowledged transaction. These rules assume
trustworthy completed synchronization and atomic same-directory replacement;
see [power-loss qualification](BRANCH_POWER_LOSS_QUALIFICATION.md) for conditional
Windows namespace semantics and the distinction between model and device proof.

If a WAL write fails partway through an append, the writer truncates and syncs
back to the preceding valid boundary before permitting a retry. No LSN, commit
epoch, or in-memory mutation is published for the failed append. Failed rollback
and failed synchronization of a complete record remain uncertain outcomes that
poison the handle and require reopen. This distinction applies equally to
ordinary and group-commit writes.

The local WAL-tail oracle starts from a fresh database for each seed, records
the complete pre-batch query result, truncates a fragmented batch at header,
block, chain-tail, or seeded interior offsets, and checks strict rejection,
exact prefix recovery, audit bytes, and a subsequent append/reopen. Run it with
`bazel run //crates/fuzz:hawdb_storage_fuzz -- --wal-tail --seed 174 --cases 64`;
every case report includes a `--case-index` replay command. These campaigns and
the existing fuzz suites remain local verification, outside CI jobs.

Integrity checks are layered for throughput. WAL records, immutable segment
blocks, cache admission, manifests, and projection envelopes use CRC32C; the
implementation selects hardware acceleration on supported x86-64 CPUs and a
portable fallback elsewhere. Checkpoint and subordinate manifest publication
also records SHA-256. Canonical artifact writers compute CRC32C and SHA-256 in
the same sequential write pass, while request-time reads verify only the block
being admitted to cache. `Database::scrub_storage` is the explicit full scan:
it streams every canonical artifact through CRC32C and SHA-256, validates WAL
framing and LSN continuity, and poisons the open handle on any integrity error.

Immutable branch objects are not re-hashed when this same process has just
produced them. A streamed object publication hashes the exact bytes it writes
into a new staging inode, syncs it, links it into `objects/` and syncs the
directories; only then does it record that inode's native identity, length and
modification time in a process-local registry, and it skips the staged
read-back. Later in the same process, selector dependency checks, branch mount
and cold immutable-handle validation skip the whole-object identity hash when
the opened descriptor still has exactly that identity, length and modification
time. Everything else still hashes the complete object: objects found on disk
(cold open, another process, any open after a crash or restart, since the
registry is memory-only), reuse of an existing object, reclamation, recovery
dependency checks, `doctor` WAL repair and derived-artifact health checks
(`verify_stored`), and `scrub_storage`. Content-level checks against manifest
digests and all page/block checks on reads are unchanged. The stamp detects a
replaced inode and ordinary later writes. It does not detect media corruption,
a rewrite within one coarse timestamp tick, or a writer that restores the
modification time. Such damage in this exclusively owned directory is refused by
format checksums when read, reported by `scrub_storage`, and refused by
the full identity check on the next cold open; it is never served as valid
data.

Branch mount of an object this process did not publish (including every
object on a new-process open) always hashes the complete object, so each branch
admission still authenticates its whole checkpoint closure before use
([#1440](https://github.com/nowledge-co/hawdb/issues/1440)). Admission then opens the
mounted artifacts through the immutable-handle cache. The next cold handle open
of a mounted object does not hash the same inode a second time: the mount
leaves a one-shot, per-project record of the hashed descriptor's native
identity, length and modification time, and that open skips the hash only when
its descriptor still matches that stamp. The mount takes the stamp before
reading the first payload byte and records it only if it is unchanged once the
hash completes, so a write that races the hash leaves no record. The record has
no time limit: if the object is not opened right after admission it waits for
the next cold open, however late, and the stamp alone then decides. It is
consumed by that open whether or not it matches, cleared before every remount
(so a refused remount leaves none) and dropped on reclamation; any later cold
open of the object, for example after descriptor-cache eviction, hashes it in
full again. Unconsumed records are optional performance state and are capped at
4,096 per project (`MOUNT_VERIFICATION_RECORDS_PER_PROJECT`), evicting the
oldest-inserted record first; an eviction only restores that object's full
hash on its next cold open and is counted in
`FileDescriptorMetrics::mount_verification_evictions`. Each record's payload is
160 bytes on 64-bit targets (reference and stamp plus the insertion-order
entry), so the cap bounds the payload at 640 KiB per project plus `BTreeMap`
node overhead. One admission mounts each object of its checkpoint closure once
(about 50 objects on LDBC SF0.1), well within the cap. The stamp's limits are those
of the publication record above, and page/block checks on reads are unchanged.
`FileDescriptorMetrics::identity_verified_bytes` and
`mount_verification_reuses` report the whole-object identity bytes verified
and the cold opens that reused a mount's verification.

`max_wal_replay_entries` counts these top-level WAL records, not the child
operations inside a batch, so a budgeted recovery either applies a complete
batch record or rejects the open before applying the next record.

Checkpoint reclamation keeps the current and previous generations plus every
physical generation pinned by an active read transaction. Unpinned intermediate
generations are reclaimed even while an older reader remains active. Reclamation
also follows retained row-page, overflow-extent, and append manifests so shared
physical artifacts outlive every reader that can still reference them.

An admitted branch keeps its private WAL beside `branch.head`
(`branches/<id>/wal.<generation>.hawdb`), outside the data root. Each
checkpoint or seal copies the active WAL's records into immutable objects (or
covers them with the new checkpoint), then publishes a head selecting a fresh
successor WAL. Recovery opens only `wal.<head.active_wal.generation>`; child
and sibling branches are created from the sealed parent root, own their own
directory and never read a parent's private WAL. The same reclamation pass
therefore also removes branch WALs with a generation below the selected one
([#1727](https://github.com/nowledge-co/hawdb/issues/1727)). It runs only
after the selecting head is published; it re-reads that head, requires it to
match the runtime's head and synchronizes the branch directory before the
first removal, so a head adopted after a crash with an unsynchronized rename
becomes durable first. Names at or above the selected generation (prepared
successors, in-flight checkpoint candidates, interrupted-publication evidence)
and reader-pinned generations are kept. The branch sweep does not use the
data-root retention of the current and previous checkpoint generations, which
number data-root artifacts; readers never open a branch WAL, so a reader pin
that matches a branch WAL number only delays its removal. The branch sweep also runs when an
open reader without a known pin set defers data-root reclamation, because no
reader opens a branch WAL. An interrupted-publication name below
the selected generation is reclaimed like a data-root generation below the
selected checkpoint: the durable head selecting a newer generation resolves its
uncertainty. The writer's exclusive branch-directory
lease excludes concurrent admission of the same branch. A failed removal or
directory synchronization is reported as `generation_reclamation_pending_*`
debt and retried by the next reclamation pass; a crash between head
publication and removal leaves only an unselected file, which the next
checkpoint after recovery removes. Each WAL directory therefore holds at most
the current and previous generation after a checkpoint.

Every checkpoint of an admitted branch also publishes its closure (durable
manifest, relational checkpoint, canonical/adjacency/property artifacts, row
pages, index shadow, append segments) and a sealed root into the project's
content-addressed object store (`branches/objects/objects/<kind>/<digest>`).
A sealed root has no parent link, so a closure no longer referenced by any
retained root is garbage. The same post-publication pass (foreground
`checkpoint()`, automatic candidate publication and deferred off-gate
retirement) therefore ends with a bounded mark/sweep of that store
([#1736](https://github.com/nowledge-co/hawdb/issues/1736)); a checkpoint with
nothing new to publish retries it only while debt remains. Liveness is the
project-wide mark that explicit `reclaim_branch_storage` uses: the durable head
of every non-deleted catalog branch, every catalog creation base root (a child
or sibling starts from a sealed parent root that may be older than the parent's
current head), and every root registered by a live owner (each runtime's head,
reader snapshots, prepared checkpoint candidates and handoffs, derived-artifact
jobs). An active owner without a root registration defers the pass. The pass
holds the catalog metadata lock, which serializes every object publication and
root registration, from its catalog read to its last removal; an automatic pass
that finds the lock held is deferred rather than waiting behind a publication.
The one publication outside that lock is the bootstrap of `main`
(`initialize_immutable_root_head`), which runs before `main` is a `Ready`
catalog branch; a sweep requires a `Ready` owner, so none can run yet. A
writer's own pre-publication revalidation of its `Ready` record waits for the
lock, so another owner's publication or sweep cannot fail its checkpoint as
busy (branch admission still reports a held lock as busy).
Before the first removal it synchronizes every branch directory whose head it
read and the catalog directory, so a head adopted after a crash with an
unsynchronized rename is durable before anything it superseded disappears.

Each pass lists the namespace and reads only sealed roots during mark;
reachable non-root objects are checked for presence and exact length, so the
cost follows the number of objects and the garbage removed, not the size of the
live data. Selected garbage is fully verified before the first unlink, with the
existing quarantine-evidence rule. An automatic pass keeps the structural
quarantine checks (intent present, bound to the catalog, committed) but
byte-verifies each committed archive once per process and caches the result
by archive directory and committed manifest digest; explicit passes re-verify
every archive every time. The cache relies on committed quarantine archives
being immutable: for the rest of the process, an automatic pass may remove a
corrupt quarantined source on the strength of that earlier verification
without re-reading the archive. An archive
beyond the evidence budget or failing verification never fails an automatic
pass: its quarantined sources are retained and reported
(`object_reclamation_quarantine_unverified`). A corrupt unreachable object
without quarantine evidence is likewise retained, named by kind and digest in
`object_reclamation_corrupt_object` (reason `object_reclamation_corrupt_object`,
counted in `object_reclamation_skipped_*`), and the automatic pass keeps
sweeping; an explicit pass still fails closed on it. Retiring deleted branch
directories, which inspects quarantined private remnants, is left to explicit
reclamation. Automatic passes use
`BranchReclamationLimits::automatic()`: at most
`AUTOMATIC_RECLAMATION_MAX_OBJECTS` (4,096) objects and
`AUTOMATIC_RECLAMATION_MAX_BYTES` (64 MiB) of re-hashed garbage per
checkpoint, because removal verification runs inside the checkpoint while the
metadata lock is held. An object this process published whose inode is
unchanged was hashed when written; it is not re-hashed and does not count
against the byte budget, so the garbage a checkpoint of this process
supersedes is always removable. Objects published by an earlier process (for
example a backlog left by a version without automatic reclamation) are
re-hashed and drain at most 64 MiB per checkpoint, reported as pending.
An object reachable from a retained root that is missing, truncated or not
decodable stops the pass without removing anything and is reported as
integrity damage (`object_reclamation_damaged_live_root`, reason
`object_reclamation_live_object_damaged`), not as backlog.
`BranchReclamationLimits` is a per-pass
budget, not a refusal threshold: at most `max_objects` objects are removed and
at most `max_bytes` of them re-hashed (always at least one object), and the
remainder is reported as pending. An explicit pass also re-hashes up to `max_bytes` of
reachable objects as an integrity check and waits for the metadata lock. A
deferral, a failed listing, barrier or unlink, an object still being read, or
garbage beyond the budget is recorded as `object_reclamation_*` debt in the
storage-pressure snapshot (`object_reclamation_retry_required`,
`object_reclamation_pending_files`, `object_reclamation_pending_bytes`, reason
`object_reclamation_debt`), next to `object_store_files` and
`object_store_bytes` measured by the last pass. The next checkpoint or
`EmbeddedStoreHandle::reclaim_branch_storage` / `Database::reclaim_branch_storage`
retries it. A crash between head publication and removal leaves only
unreachable objects for a later pass. After a checkpoint the store holds the
selected head's closure, the closure of the head it superseded while the
checkpoint source, the retired frontend or a handle's read publication still
pins it (released by the next pass), every pinned reader and branch closure,
and the catalog creation baseline of each branch (for `main`, the root it was
created or bootstrapped from).

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
instead of replaying a stale write set. The one exception is a transaction made
only of `ON CONFLICT DO NOTHING` statements, with no reads and no graph or
Strict Append mutation: the commit sequencer may safely restage it so
concurrent idempotent writers converge to one inserted result and deterministic
conflict no-ops. Pessimistic
transactions acquire locks on first use rather than at `BEGIN`. The lock table
supports compatible shared
locks, conflicting exclusive locks, inclusive point locks, bounded ranges with
inclusive or exclusive endpoints, and an unbounded database target that
overlaps every finer-grained resource.

Hosts that govern writer concurrency can acquire a mutation `RuntimePermit`
from their existing governor and call
`begin_admitted_transaction(options, permit, task_context)`. The permit remains
owned through statements, queued commit, shared durability and result delivery.
Admission waiting remains with the host's existing governor queue. Cancellation
is checked cooperatively before statements and the commit boundary; it does not
interrupt lock waits or turn an already durable success into cancellation.
Read result limits intersect the admitted and configured budgets. Private
mutation staging still uses `MutationLimits`; this is not complete allocation
accounting. Plain transactions remain caller-managed. See the
[ownership proof](tla/TRANSACTION_ADMISSION_LEASE_PROOF.md).

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
refreshes its private snapshot after the lock is granted. After an earlier
successful statement, Snapshot Isolation keeps the original snapshot. A new
relational lock validates the retained versions in its primary-key interval,
or the payload-sensitive summaries in a supported declared secondary-index
interval, under the commit sequencer after lock acquisition. Inserts into empty
intervals, deletes, rekeys and ABA changes require a retry; disjoint commits do
not. Table locks validate their table domain. Unrepresented secondary keys stamp
an index barrier; nullable/unsupported index types and constraint-only indexes
use the table domain. Schema changes, database/read barriers, unavailable
history and validation admission failures retain a conservative retry. Graph
lock discovery retains its existing epoch fence.

Each pessimistic Snapshot Isolation transaction registers a source-history pin
and read-summary lease at begin. This enables additional bounded summary work
for concurrent writers; history uses the existing version-memory budgets. One
validation admits at most 4,096 requests, 64 KiB of copied key metadata and
65,536 visited stamps, seeking ordered version pages rather than scanning
unrelated identities. These limits exclude allocator overhead and existing
schema metadata. Validation failures appear in the existing read-dependency
failure counters. Ordinary snapshot reads do not acquire serializable read
dependencies, and Serializable retains its separate collector and commit check.

Lock-complete disjoint writes may rebase
their exact staged operations onto the current store at commit; constraint
validation and the durable publication order still run against current state.
Both transaction modes retain one WAL order, durable-before-publish, and one
commit epoch per transaction.

Optimistic graph writes already use per-key version validation, while relational
and append operations retain a conservative database version barrier.
Pessimistic lock-protected rebasing is a separate path. Transaction-private
workspaces own a reader pin, including while queued for commit, so checkpoint
reclamation retains their physical generation until rollback or destruction.
Concurrent transaction bodies already execute outside the commit sequencer;
validation and publication remain serialized. [`MVCC_COMMIT_VALIDATION_PROTOCOL.md`](MVCC_COMMIT_VALIDATION_PROTOCOL.md)
maps the implemented identities and validation rule to source, and records the
remaining #231/#232 work: version-memory and reclamation qualification, narrower
domains, and workload qualification. Checkpoints now clean eligible version
tombstones using a separate registry of every storage snapshot, including
sources that could start a later transaction. A new pessimistic lock
acquisition after snapshot drift uses the bounded dependency validation above.

Coordinator waits record every blocker in a multi-owner wait-for graph. Adding
dependencies that close a cycle aborts the current waiter as the deterministic
deadlock victim and releases all of its locks. Wakeup, timeout, commit,
rollback, and drop remove both held locks and wait dependencies.

Hosts can use `HawDBError::is_retryable_transaction_conflict()` for both
optimistic conflicts and pessimistic snapshot/deadlock failures. The latter
carry `TransactionRetryRequired(TransactionRetryError)` with a typed reason
and transaction identity. Calls made after that abort retain the original
reason and set `already_aborted`; retry the complete transaction with a fresh
snapshot. Lock timeouts keep their existing statement/transaction rollback
policy and are not included in this transaction-retry classification. Generic
execution, cancellation, poisoned-manager and storage errors are not promoted
to retryable errors by their message text.

Every durable open acquires an exclusive process-lifetime lease on the database
directory. The host opens one root `Database` handle and derives sessions,
transactions, and snapshot readers from that handle. A process-local canonical
path registry rejects duplicate handles in one application, while the stable
`owner.hawdb.lock` sidecar rejects opens from other cooperating applications on
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
`Database::storage_recovery_report` exposes the open-time recovery boundary:
recovery mode, checkpoint and commit epochs, WAL presence, replay-start/next LSN,
replay counts and bounds, and an optional typed `wal_repair` with evidence paths.
Default writable open repairs only the authorized suffixes described above;
`Strict` rejects them. `WalTailRepairPlan` remains the explicit doctor planning
contract. The historical `DoctorRepairTornTail` enum value is retained for report
compatibility but database open rejects it.

Embedding applications can explicitly call `Database::checkpoint(&mut self)`
or `ConcurrentDatabase::checkpoint(&self)`. These synchronous Rust APIs work
below the automatic soft/age thresholds and with background maintenance
disabled, including the native `default-features = false` profile. Success
means the captured source crossed checkpoint publication and its durability
barrier; enqueueing an automatic task is not success. The calls coordinate with
the automatic owner, preserve pinned readers, and reject read-only handles.
Storage/resource failures and concurrent-source validation errors remain
explicit errors. Hosts own admission for this caller-triggered work and should
execute it on a thread suitable for blocking storage I/O. The
`checkpoint_background` and `checkpoint_scheduled_background` helpers retain
their pressure and background-policy checks.

The CLI command `hawdb storage-recovery-report [--strict]
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
back to partial traversal behavior. The outer durable manifest selects exact
adjacency-data and descriptor-root identities. Normal open verifies the compact
root without loading descriptor pages; endpoint scans demand-read only the
ordered prefix and referenced blocks through the shared byte-bounded cache.
Independent page, byte, descriptor, and tree-height limits bound each scan.
Physical corruption poisons the handle, while admission rejection does not.
Explicit deep scrub bypasses the cache and verifies the complete descriptor and
adjacency closure. Backup, restore, repair, and reclamation retain the data,
descriptor pages, and root together.

Query batch execution opens a `DescriptorLeafScope` on its thread, next to
the canonical point-read scope (#1615). Inside it, adjacency endpoint seeks
keep the last few verified descriptor leaves they reached, each with the page
reference that selected it, its tree root and the codec limits it was
validated under (only cache-resident pages, at most eight and an eighth of the
cache capacity, pinned until the outermost scope closes). A later endpoint
prefix whose complete key range lies strictly inside a retained leaf of the
same tree root and codec limits is answered from that leaf without the root
traversal or any cache lookup; prefixes touching a leaf edge or spanning
leaves use the ordinary traversal. Results, order, poison and limits are
unchanged: the retained leaf counts as one visited page against the page and
page-byte limits. Read reports then show one visited page, no cache hit or
miss, and `descriptor_retained_leaf_hits = 1`.

Persistent property projections use the same descriptor demand boundary for
equality, range, full-text, composite-equality, relationship-equality, and
relationship-range indexes. Normal open reads the bounded definition directory
and descriptor root only; the number of data blocks does not determine startup
RSS. Candidate estimates traverse descriptors under the same page, byte,
descriptor, and height admissions as execution and propagate corruption rather
than hiding it behind a scalar estimate. Read reports include descriptor
storage, decoded bytes, and cache outcomes separately from selected projection
blocks. Deep scrub hashes the complete data and descriptor artifacts, decodes
every block, and requires a contiguous block-id/range closure with exact block
and entry counts. Backup validation and derived repair run that exhaustive
closure check rather than validating only top-level file identities.

Canonical large-property spills also use the descriptor demand boundary. Their
fixed-width key is the block's maximum spill id, so hydration performs one
lower-bound seek and reads only the block whose contiguous id range contains the
requested value. The compact manifest contains no per-block vector. Read reports
separate descriptor and data I/O/cache activity, physical failure poisons the
reader, and block-size admission does not. Deep scrub bypasses both caches,
hashes the complete artifacts, decodes every block, and proves contiguous byte,
block-id, spill-id, block-count, and value-count closure. Backup validation and
storage scrub run this exhaustive check. They open the selected spill reader
before validating canonical records: every spill reference requires the same
generation and source epoch and must be below that manifest's exact value
count. Scrubbing a canonical artifact without its selected spill artifact fails
closed rather than postponing the error until query-time hydration.

Canonical segment descriptors use the same immutable page tree. A checkpoint
synchronizes canonical data first, publishes descriptor pages and the root,
then binds the source epoch, per-kind counts, and exact root integrity in the
compact canonical manifest selected by the outer checkpoint manifest. Normal
open reads the root but no descriptor page payload. Point reads lower-bound
seek one descriptor; scans advance in fixed-size descriptor batches under
independent page, byte, descriptor, and height limits. Reports separate
descriptor and canonical data cache/I/O. Physical failure sticky-poisons the
reader, while admission remains non-poisoning. Backup and storage scrub hash
both complete artifacts, decode every segment, and prove ordered contiguous
closure without warming the shared cache. Their record validation is bound to
the selected property-spill reader, so individually valid canonical and spill
artifacts cannot be accepted as an inconsistent pair.

`DatabaseDoctor::derived_artifact_health` verifies the complete data,
descriptor-page, descriptor-root, and block closure of the active adjacency and
persistent property projection. `plan_derived_artifact_rebuild` then validates
the canonical segment stream and WAL replay under explicit source-record,
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
and RaBitQ artifacts retain the active and immediately previous immutable
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

Every durable mutation also evaluates the complete projected storage-pressure
signal set before opening or writing the WAL. The check includes WAL and
out-of-core delta limits, handle integrity, checkpoint temporary-space reserve,
generation reclamation and reader-pin debt, and cache pressure. Filesystem free
space is sampled on the first append and after each 64 MiB of admitted WAL
writes; cached samples are reduced by the WAL bytes written since the probe, so
the hot commit path does not perform one filesystem query per mutation. A probe
failure or non-admitting pressure state rejects the mutation before the WAL can
grow and reports a stable pressure reason code.

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
path counts, retains bounded deterministic histograms for eligible scalar
properties including `VARCHAR`, and removes every run on success or failure.
Declared `TEXT`, list, map, and mixed-type property groups are excluded before
large values can be cloned,
encoded, sorted, or spilled; the bounded exclusion set is charged to the same
memory budget. The refreshed statistics become visible only after a
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
local physical-export entry point for this path. It scans the canonical
base-plus-delta view, generates missing stable IDs once, publishes an
independent mapping generation, and reuses the same values after reopen.
Ordinary open reads only the bounded mapping header; explicit export
materializes demand-read pages under entry, I/O-byte, and resident-byte limits.
The mapping is durable before an initial-import graph WAL batch, so recovery
cannot expose that batch without its selected identity generation. The default
`export_canonical_graph_snapshot` remains read-only and does not create
persistent export metadata.
`Database::prepare_hawdb_lightning_bootstrap_export` wraps the same persisted
stable-ID snapshot and the relational state at one database commit epoch in a
HawDB Lightning bootstrap manifest. The authoritative logical payload consists
of the GraphStream plus a RelationalStream checkpoint. The latter carries SQL
table schemas, rows, indexes, constraints, and overflow values. The manifest
records both stream checksums and byte lengths, their shared database epoch,
graph schema and row counts, relational table/row/overflow counts, and both
validation reports. HawDB Lightning deliberately excludes WAL history, physical
pages, adjacency layouts, statistics, caches, and search or analytics projection
artifacts; those are recovery history or rebuildable physical state rather than
portable user data. The GraphStream remains deterministic canonical text sorted
by labels, relationship type, stable IDs, and endpoints.
The CLI command `hawdb validate-canonical-snapshot [--require-valid]
[--require-import-ready] <database-path>` opens the database read-only, exports
the current canonical snapshot, and prints the validation report as JSON.
`--require-valid` returns a non-zero status when the snapshot is internally
inconsistent. `--require-import-ready` additionally requires every node and
relationship to have unique stable identity, so the export can enter a physical
import path without first creating an external ID mapping.
The CLI command `hawdb hawdb-lightning-bootstrap-manifest [--require-ready]
<database-path>` opens the database read-write, creates or reuses
`stable_ids.hawdb`, and prints the bootstrap manifest as JSON. `--require-ready`
returns a non-zero status if the manifest's embedded validation is not
import-ready or the relational stream is invalid.
Normal open validates only the stable-identity selector, selected generation
header, their exact agreement, and the generation artifact length;
mapping pages remain cold and are read through the shared bounded page cache.
An explicit storage scrub streams every page without cache insertion and
validates page checksums, global key order, counts, and value encodings while
retaining at most one page and one decoded value.
The CLI command `hawdb hawdb-lightning-graph-stream [--require-ready]
<database-path>` uses the same bootstrap export path and prints the deterministic
GraphStream text. The final `checksum` line covers the stream body and matches
the manifest's `graph_stream_checksum`.
`hawdb hawdb-lightning-relational-stream [--require-ready] <database-path>`
writes the binary relational stream to stdout. It is a component command for
upload pipelines; consumers must preserve the bytes without text conversion.
`HawDBLightningGraphStream::validate_against_manifest`,
`HawDBLightningRelationalStream::validate_against_manifest`, and the CLI command
`hawdb hawdb-lightning-verify-export [--require-valid] <database-path>` verify
the local bootstrap artifacts before upload. The report covers GraphStream
format, checksum, count, and endpoint integrity plus relational checkpoint
decode, checksum, epoch, table/row/overflow counts, and manifest agreement.
`hawdb hawdb-lightning-bootstrap-bundle [--require-ready] <database-path>`
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
`hawdb hawdb-lightning-stage-bootstrap [--require-ready] <database-path>
<staging-dir>` writes a local staging catalog plus manifest, GraphStream,
`hawdb_lightning_relational_stream.bin`, and bootstrap bundle artifacts with
atomic file publication and directory sync. The
catalog is the v1 local checkpoint boundary for offline bootstrap upload/resume;
it is outside the graph WAL and does not alter the published graph snapshot. The
catalog also summarizes staged object count, measured byte count, total bytes,
average object size, and per-kind object counts for upload observability.
`hawdb hawdb-lightning-verify-staging [--require-ready] <staging-dir>` reopens
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
`hawdb hawdb-lightning-publish-staging [--require-state-marker]
[--fencing-token <token>] [--expected-database-epoch <epoch>] <staging-dir>
<publish-dir>` verifies a READY staging catalog and atomically writes
`hawdb_lightning_published_manifest.json`. Repeating the command for the same
manifest is idempotent; attempting to publish a different manifest over an
existing pointer fails instead of overwriting the published database pointer. When
the optional state-marker/fencing preflight is enabled, publish requires a
VALIDATING import marker, matching fencing token, and matching staged manifest
database epoch before writing the pointer.
`hawdb hawdb-lightning-verify-published <staging-dir> <publish-dir>` verifies
that the published pointer still references the staged catalog by byte length
and checksum, and that the referenced staging catalog still passes the
source-independent verifier. The report promotes the staging verifier's
`storage_recovery_evidence` to a top-level field so post-publish audit can
inspect recovery readiness without traversing the nested staging report. Its
validation gate keeps flat errors and grouped pointer, catalog, and staging
error arrays so resume automation can distinguish pointer corruption from
staging catalog drift.
`hawdb hawdb-lightning-gc-staging-report <staging-dir> <publish-dir>` fails
closed when a published pointer cannot be verified and groups the propagated
published-pointer verification errors for cleanup automation. The GC report also
summarizes total, pinned, and deletable staging bytes so callers can distinguish
published retention from orphan staging space.
`hawdb hawdb-lightning-import-status <staging-dir> <publish-dir>` summarizes
CREATED/READY/PUBLISHED/QUARANTINED state and can merge an optional
caller-owned `hawdb_lightning_import_state.json` marker for
EXPORTING/UPLOADING/MERGING/VALIDATING/FAILED/CANCELED coordinator states. It
groups presence, staging, published-pointer, state-marker, and resource errors
for resume automation. Active coordinator states require the marker to carry the
idempotent retry tuple `import_id`, `task_id`, `fencing_token`, and
`object_digest`; missing fields quarantine the status report before retry. The
report can also summarize an optional caller-owned
`hawdb_lightning_import_checkpoints.jsonl` append log. Checkpoint entries keep
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
scan would. `docs/tla/HawDBPropertyIndexPruning.tla` models both evaluations
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
equality index descriptor exists, `IndexNodeUnionSeek` for bounded exact `OR`
predicates only when every property branch has an equality descriptor,
`IndexNodeCompositeSeek` for conjunctions
that bind every property in a composite equality descriptor,
`IndexNodeCompositeRangeSeek` for a non-empty contiguous equality prefix plus
a range on the immediately following composite property, `IndexNodeTextSeek`
for `CONTAINS` predicates backed by a full-text descriptor, and
`IndexNodeRangeSeek` for single-bound and conjunctive bounded range predicates
when a range index descriptor exists. Text seeks use the ngram index only as a
candidate source and retain a residual `FilterExec` so exact string containment
semantics remain authoritative. Composite range scans prune immutable
projection blocks by ordered tuple bounds, validate every candidate against the
canonical row, and merge the COW/WAL overlay before output. A gap in the index
prefix declines this access path rather than reading a misleading partial
projection. Composite and full-text execution projections
can be rebuilt through `Database::rebuild_bounded_property_index_projections`,
which applies complete descriptor rebuilds that fit a caller-supplied operation
budget and reports estimated operations plus indexed entry counts without
writing WAL. Internal background loops can expose the same work as a
`Projection` `BackgroundWorkPlan` through
`Database::property_index_projection_background_work_plan` or execute it through
bounded background/scheduled wrappers that bind QoS admission to the same
descriptor rebuild budget. Conjunctive range seeks keep the complete `AND`
predicate as a residual filter while using merged lower and upper bounds as the
access path. Statistics now include eligible-scalar per-label/property and
per-relationship-type/property distinct counts, one-hop path source/target
coverage distinct counts, bounded multi-hop path source/target coverage
distinct counts, bounded sorted value histograms, and exact-versus-sampled
markers. Declared `TEXT` and container property values do not produce generic
NDV or histogram statistics; `VARCHAR` remains eligible and text indexes remain
independent. Histograms use
deterministic adaptive samples:
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

These basic counters are also the semantic source for exact unfiltered global
count plans. `NodeCountExec` answers total or per-label node counts, while
`RelationshipCountExec` answers total or per-type relationship counts without
decoding records or traversing adjacency. The optimizer selects the relationship
path only for an unconstrained required one-hop pattern; any endpoint label,
relationship property, optional expansion, variable-length expansion, filter,
or distinct aggregate retains the normal execution path. For non-fast-path
node aggregates, the physical scan decodes only predicate, group-key,
aggregate-operand, and index-validation properties and preserves node identity
for downstream aggregation. Large unrelated properties remain cold.

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

Property defaults (#1683) use
`CREATE PROPERTY ON NODE|RELATIONSHIP TABLE Name(property) TYPE Type [NOT NULL] DEFAULT value`.
`value` is a non-NULL literal (`BOOL`, integer, float, string, or a list of
those, including `[]`) or `current_timestamp()`. NULL, parameters, property
references, temporal constructors such as `date('...')`, and other expressions
are rejected by the parser. The default must satisfy the declared `TYPE` and
`NOT NULL` at DDL time; `TYPE ANY DEFAULT value` declares a default without
constraining later writes. A default is applied **only when a record is
created without the property**: Cypher `CREATE` of nodes and relationships
(including `UNWIND ... CREATE` and patterns that create both endpoints),
MERGE-created nodes and relationships, explicit transactions, the direct
`GraphStore` creation APIs, and `GraphStore::import_graph_snapshot_rows`. The
default is written into the WAL record before validation, so recovery,
checkpoints and readers see an ordinary stored value. Defaults are never filled
at read time.

| Write (table `E(p DOUBLE DEFAULT 0.0)`) | `p` reads back |
|---|---|
| `CREATE (:E {id: 'a'})` | `0.0` |
| `CREATE (:E {id: 'b', p: NULL})` | `NULL` |
| `CREATE (:E {id: 'c', p: $v})` with `$v = NULL` | `NULL` |
| `MERGE (e:E {id: 'd'}) ON CREATE SET e.p = NULL` | `NULL` (default first, then `ON CREATE SET`) |
| `CREATE (:E {id: 'e'})` then `SET e.p = NULL` | `NULL` |
| `UNWIND $rows AS row CREATE (:E {p: row.p})`, row without key `p` / with `p: NULL` | `0.0` / `NULL` |
| `GraphStore::import_graph_snapshot_rows` row without key `p` / with `p: NULL` | `0.0` / `NULL` |
| `LOAD CSV ... CREATE (:E {p: row.p})` with an empty field | `0.0` (openCypher: CREATE omits NULL properties) |
| record that existed before the declaration, after maintenance | `0.0` |

HawDB storage distinguishes a property that was never written from one written
as NULL: `CREATE (:E {p: NULL})`, `SET n.p = NULL` and `ON CREATE SET n.p = NULL`
store an explicit NULL entry that survives WAL replay and checkpoints. A
row-sourced CREATE or MERGE create branch whose row map lacks the referenced key
(`row.p` with no `p` in `row`) now leaves the property out instead of storing
NULL, so it receives the default; `UNWIND ... MATCH ... CREATE` relationships
keep an explicit NULL row value instead of dropping it. This differs from
openCypher, where a missing map key reads as NULL, and from Kùzu/Ladybug, which
type a `$rows` list of maps as a fixed-field STRUCT list and refuse the whole
statement when a row lacks a key (measured on Kùzu 0.11.3 and Ladybug 0.15.3).
Statements accepted by those engines therefore store the same values here; only
rows lacking a key, which they refuse, receive the default in HawDB.
`current_timestamp()` defaults use exactly the representation of HawDB's own
Cypher `current_timestamp()`, `timestamp('...')` and `CAST(... AS TIMESTAMP)`:
an `INT` of epoch nanoseconds, so defaulted and explicitly written timestamps
compare and sort together. The value is truncated to whole milliseconds (a
multiple of 1,000,000 ns, matching Kùzu's precision) and is one value per
writing transaction: every record of one auto-commit statement or of one
explicit transaction shares it, matching Kùzu, whereas the Cypher function is
pinned per statement. Declare such properties `TYPE INT` or `TYPE ANY`.
Adding a label to an existing node does not apply defaults.

Declare properties before importing data. Declaring a defaulted property on a
table that already has records lacking it puts the descriptor in `BACKFILL`;
records lacking the property (key absent) are filled by schema maintenance,
while explicitly stored NULLs and other present values are kept. The backfill
value is resolved once at declaration time and persisted with the descriptor,
so a `current_timestamp()` backfill writes the declaration instant to every
pre-existing record even across several bounded maintenance runs. Records
created while the descriptor is in `BACKFILL` already follow the creation rule.
Each maintenance run writes its fills as one WAL batch, and the batch that
fills the last lacking record also advances the descriptor to `VALIDATING`, so
a crash leaves either a resumable `BACKFILL` or a completed one. Existing
present values must already satisfy the descriptor at DDL time; `NOT NULL`
defaulted properties therefore reject tables with stored NULLs.

`Database::property_schemas` reports every descriptor with its table, type,
nullability, state and declared default (`PropertySchema::default`, including
the resolved backfill value). Defaults persist through binary WAL op code 29
(`CreatePropertyWithDefault`; undefaulted descriptors keep op code 5 and their
exact pre-default bytes) and a `property_default` checkpoint catalog line.
Databases written before defaults reopen with no defaults. Binaries that
predate defaults reject a database that declares one (`unknown WAL op code 29`
or `invalid checkpoint line`) instead of silently dropping the default.

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

HawDB never runs schema maintenance on its own: `BackgroundMaintenanceOptions`
with `include_schema_maintenance` only reports a `SchemaMaintenance` candidate,
so a `BACKFILL` descriptor stays in `BACKFILL` until a caller runs maintenance,
whatever `RuntimeCapabilities::background_maintenance` says. Hosts on the
admitted embedded handle (#1739) use `EmbeddedStoreHandle::property_schemas` and
`EmbeddedStoreHandle::plan_schema_maintenance` (read-only, same results as the
`Database` methods, no runtime admission) and
`EmbeddedStoreHandle::run_bounded_schema_maintenance`, which takes one
foreground maintenance admission like `EmbeddedStoreHandle::checkpoint` and then
runs `Database::run_bounded_schema_maintenance` under the handle's write guard.
That explicit call does not require the `background_maintenance` capability, so
an offline host with background maintenance disabled can repeat it with a small
budget while the descriptor is in `BACKFILL`; the `VALIDATING` to `PUBLIC`
advance is one action whose planned estimate is the table's record count, so
the promoting call needs at least that budget. That validation scans every
record of the table while holding the handle's exclusive write guard, so it
blocks writers for the length of a full-table scan; on large tables hosts
should schedule the promoting call like a checkpoint (idle time, preparation
phase) rather than on a latency-sensitive write path.

`CREATE PROPERTY` on a property that already has a descriptor is idempotent only
when type, nullability and default all match (`created: false`). A declaration
that differs in any of them is rejected with an error naming the existing
declaration, instead of being dropped. This is a behaviour change (#1739):
such a re-declaration used to be a silent no-op returning `created: false`. The
refusal is the typed `HawDBError::Schema { surface: Storage, .. }` (displayed
as `storage error: ...`, like other storage-surface schema refusals), not
`HawDBError::Storage`, so hosts can tell a rejected DDL from a storage
failure; the database stays usable. In particular a `DEFAULT` cannot be
attached to an existing undefaulted descriptor this way (`ALTER PROPERTY ...
SET DEFAULT` is tracked in #1740). Schema-less writes such as
`CREATE (:Entity {score: 0.3})` create no property descriptors, so declaring a
defaulted property on a label that was only written schema-lessly creates a
fresh descriptor (implicit table descriptor included) and enters `BACKFILL`; a
`TYPE ANY` declaration constrains no other property of that label.

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

Checkpoint also writes `projected_graphs.hawdb` for every persisted projected
graph definition. The artifact records its format version, projection epoch,
covered commit epoch, projected labels and relationship types, relationship
property predicates, node IDs, CSR outgoing offsets and targets, and CSC
incoming offsets and sources. V2 writes the predicates while recovery still
accepts predicate-free V1 artifacts. It is checksum-protected and atomically
replaced.
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
and atomically replace `projected_graphs.hawdb`.
`Database::schedule_derived_artifact_rebuild` and
`Database::run_next_derived_artifact_job` add a small embedded job state machine
for these projected graph artifacts. Jobs expose pending/running/succeeded/failed
state, attempts, and last error without adding threads or hiding rebuild
failures.
Internal background callers can route the same pending jobs through
`Database::run_next_background_derived_artifact_job` for stateless admission or
`Database::run_next_scheduled_background_derived_artifact_job` for the
database-owned `LocalQosScheduler` path. Shared scheduler handles track running
background operation budgets between start and finish, including optional
per-class budgets for projection, import, analytics, and shadow lanes; it does
not own worker threads, reorder jobs, or gate foreground explicit rebuild
requests. Checkpoint builders use `LocalQosBatchPermit` to cover up to 64
bounded units with one admission; an idle batch is not counted as running and
is reclaimed by any background admission it would refuse (see the pressure
checkpoint spec, #1616).
Callers that maintain their own background loop can build a
`BackgroundWorkPlan` with `BackgroundWorkHint` signals for active topic,
recent delta size, query probability, staleness TTL, freshness SLO, and tenant
budget. Schema maintenance can join the same caller-owned candidate list
through `Database::schema_maintenance_background_work_plan`, which returns a
`Mutation` background work plan using the current dry-run estimate when
descriptor maintenance is pending.
`LocalQosPolicy::evaluate_background_work` returns the existing admission result
plus a deterministic expected-value score and reasons, so the caller can rank or
skip internal background work without moving queue ownership into HawDB. If a
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
pending job selected from a bounded poll result. HawDB records the state
transition without embedding parsing, crawling, chunking, or large-value runtime
logic. That runtime reads the payload and publishes rebuildable projections back
to HawDB through caller-owned output. Successful external content jobs keep the
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
cost. HawDB uses that manifest only to return bounded claimable-job views and an
Import-lane background work plan; it is not a sandbox policy and does not grant
the runtime access to graph-kernel execution.
Internal parser/crawler loops can use
`Database::run_next_background_external_content_artifact_job_with` for stateless
`LocalQosPolicy` admission or
`Database::run_next_scheduled_background_external_content_artifact_job_with` for
database-owned `LocalQosScheduler` accounting. These paths charge external content work to the
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
uses the database-owned `LocalQosScheduler` to account for in-flight internal background
projection work while keeping the direct delta API available for explicit
foreground callers.

Search change-log capture classifies every committed WAL batch before append,
after direct append and during replay with the same rule
(`projection_wal_requires_rebuild`). Additive schema commits record a complete,
usually empty, change-log entry and keep incremental catch-up continuous:
label, relationship-type, node/relationship table and property creation
(including `DEFAULT`), index and constraint creation, and table/property state
changes to `WRITE_ONLY`, `BACKFILL`, `VALIDATING` or `PUBLIC`. Descriptor state
is catalog metadata: graph reads, the search rebuild source and graph-delta
hydration never filter records or values by it; it only gates planner proofs,
typed fast paths, constraint enforcement and which defaults new records receive
at write time. A declared default reaches existing records only through schema
maintenance backfill, whose fills are ordinary point property writes captured
as record changes in the same commit as the state advance. Retiring transitions
(`DELETE_ONLY`, `GC`), descriptor GC and initial-import source marks still raise
the resume floor, so a cursor below them must rebuild.
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

`SyncOnEveryWrite` remains the default power-loss-safe mode. Group commit may
share a flush, but every acknowledged commit must be covered by the completed
barrier. Required recovery state and file/directory entries must be durable.

Hosts may explicitly select `SyncOnCheckpoint` through the typed Rust durability
configuration. It flushes complete transaction WAL to the operating system
before returning but does not synchronize each commit to persistent storage.
Recent acknowledged DDL/DML transactions can therefore be lost on power failure;
atomic recovery is still required. A successful checkpoint or branch seal makes
its covered committed prefix durable. There is no fixed time bound on the loss
window without such a completed barrier.

Branch creation, logical deletion, and head publication always synchronize their
recovery dependencies, regardless of the DDL/DML policy. Delaying memtable/data
page flush or compaction is compatible with power-loss-safe commits when their
WAL is already durable. See the [branch contract](specs/BRANCHING_STORAGE_SPEC.md)
for publication and qualification requirements. This policy does not claim that
all branch paths have already been implemented or qualified.

Callers that need higher ingest throughput SHOULD batch related mutations into
one transaction and one WAL batch rather than weakening the default durability
contract.

## Relationship Locality

The current in-memory adjacency key is:

```text
(node_id, relationship_type_id) -> ordered (neighbor_id, relationship_id) entries
```

There are two indexes:

- outgoing: `(source, type) -> (target, rel_id)`
- incoming: `(target, type) -> (source, rel_id)`

Each posting list is ordered by `(neighbor_id, relationship_id)`. Snapshot
sharing retains an immutable pivot and bounded ordered mutation deltas; reads
merge those structures directly. Typed adjacency expansion therefore does not
allocate or sort a degree-sized relationship-id vector, and its cursor can stop
as soon as the executor reaches a row limit or observes cancellation. The
untyped cross-relationship-type fallback remains a separately admitted
blocking operation.

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

Top-level record property keys are not stored inline as strings. The canonical
writer interns each distinct key — nodes and relationships share one
dictionary per artifact — into a `u32` id in first-seen order, and record
payloads encode `key id, tagged value` pairs. The manifest publishes the key
table as `property_key` lines (id plus hex-encoded UTF-8 key bytes, so keys
containing tabs or newlines survive the tab-separated text format) under the
`HAWDB_CANONICAL_MANIFEST_V1` header. The mandatory
`record_layout\tproperty_key_ids` declaration prevents an inline-key artifact
from being interpreted as the current format. There is no earlier manifest
decoder or checkpoint upgrade path. A record referencing an id outside the key
table, a key table with duplicate keys or non-contiguous ids, and duplicate key
ids within one record all fail closed as corruption. Keys inside nested map
values remain inline strings.

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
`search_lexical.<generation>.hawdb` artifact followed by a checksummed
`search_lexical.manifest.hawdb`. The manifest binds the artifact to the source
graph epoch, analyzer digest, document snapshot digest, corpus length totals,
projection-wide per-term document frequencies, and immutable document-length
and posting blocks. Builds use bounded external sort runs and bounded fan-in
merges; readers admit one bounded block per query term and verify artifact and
block checksums.

The fallible persisted search path reads only blocks that can contain analyzed
query terms. It reads projection-wide term document frequency from the manifest
and uses the manifest's document count and total document length for BM25
normalization. The bounded mini-delta retains the original term set and length
for each overridden base document, allowing upserts and deletes to correct the
statistics to the current projection. Metadata and ACL membership still scope
scored candidates, but do not redefine the BM25 corpus. This keeps filtered and
mini-delta query I/O independent of corpus size: document-length blocks are not
decoded during scoring and every term uses one posting decode pass. The scorer
retains only the text page window or hybrid rank window in a streaming TopK.
Reports expose posting bytes read, candidate postings visited, the exact
matching-document count, and whether segmented BM25 was selected.
Checkpoint replaces the base generation and clears the mini-delta. A stale
rebuildable projection falls back to the reference scorer; a declared corrupt
artifact fails closed.

Checkpoint also publishes immutable, generation-named document descriptor,
full-document payload, metadata-only sidecar, vector-only sidecar, sidecar
layout, and lexical manifest artifacts before atomically switching
`search_projection.out_of_core.manifest.hawdb`. The checksummed layout binds one
metadata and vector range to every descriptor segment. A reader opened before
the switch remains pinned to its generation; an interrupted publication leaves
the previous manifest readable. Segment checksums, descriptor, layout, and
lexical-manifest checksums, document/analyzer digests, source graph epoch, exact
entry counts, and explicit compressed and uncompressed segment limits fail
closed on mismatches. Decompression itself is limited to the admitted byte
count rather than trusting the envelope length.

### Binary vector sidecar (layout V2)

Layout `HAWDB_SEARCH_OUT_OF_CORE_LAYOUT_V2` stores each vector sidecar range
uncompressed as one binary segment (`out_of_core::vector_segment`): an 8-byte
`HDVSEG02` magic; little-endian row count, dimension, head length and ID-blob
length; cumulative ID end offsets; the strictly increasing UTF-8 document IDs
(zero-padded to four bytes); one CRC32C per row; and a CRC32C over the whole
head. Fixed-stride rows of little-endian `f32` values follow. Row ordinals are
implicit (`vector_ordinal_base + index`). The writers store the input `f32`
bits exactly, reject non-finite values, and keep the layout range checksum over
the complete segment.

Serving reads and verifies the head (normally one positioned read), then only
the rows it needs: graph/RaBitQ reranks read just their selected candidates,
coalescing adjacent rows, and full scans read rows in bounded 256 KiB runs.
Every byte that is read is covered by the head CRC or its row CRC, so a corrupt
head or selected row fails closed, while unselected rows are never read or
decoded. Query-admitted reads charge the head, run buffer and decoded row to the
query memory account and observe cancellation between runs. Mapping graph or
RaBitQ ordinals to document IDs for layered visibility reads only the head.

Layout V1 stored zstd-compressed decimal text and required decompressing and
parsing every row of every touched segment on each query (#1469). Out-of-core
search generations are rebuildable projections and HawDB has no persisted
compatibility obligation for development generations (see `AGENTS.md`), so
readers do not decode V1: opening a V1 generation fails closed with a storage
error naming the superseded layout and asking for a projection rebuild. Rebuild
it from canonical data, for example with `SearchOutOfCoreGenerationWriter`, or
let the next resident `SearchIndex::checkpoint` publish a fresh V4 generation;
neither path decodes the superseded sidecars.

### Metadata value postings (layout V4)

Layout `HAWDB_SEARCH_OUT_OF_CORE_LAYOUT_V4` adds an optional `postings` range per
segment, stored in the metadata artifact right after that segment's metadata
sidecar range and validated like it (contiguous offset, exact document count,
compressed-segment limit, range checksum). The uncompressed payload
(`out_of_core::postings`, magic `HDPOST02`) holds the segment's document IDs in
order, one vector flag per document (ordinals are `vector_ordinal_base` plus
the preceding flagged documents), and for every supported scalar field each
value, normalized exactly as filters compare it, with the ascending indexes of
the documents holding it. Ordinary metadata uses trimmed, case-folded values;
`kind` uses its canonical projection aliases; and a missing or empty `space_id`
is indexed as `default`. Multi-valued `labels` and `metadata.*`, plus
`document_id`, remain residual fields. A
field with more than 4,096 distinct values, or a normalized value longer than
256 bytes, in the segment is recorded as not indexed, so free-text metadata is
not copied uncompressed. A segment whose payload would exceed the segment byte limit has no
postings range.

The candidate build compiles every supported `Eq` or `In` predicate into a
postings plan. It intersects those predicates' value lists. With no residual
predicate, the selected documents pass directly, in segment order, through the
same visibility, eligibility and candidate encoding as the scan. With residual
predicates, the metadata sidecar is still read and validated sequentially, but
only posting-selected records decode the residual fields and reach residual
matching. A segment without postings, whose
postings do not index a predicate's field, or whose postings range exceeds
the reader's segment byte budget, uses the metadata scan. Opening does not
apply that budget to postings ranges, so a reader configured tighter than the
writer still opens the projection. The read is
checksummed and, on the query path, charged to the query memory account. The
document table, field and value order, and every document index are validated,
so corrupt postings fail with a storage error. A filter with no posting-supported
predicate keeps the scan, which decodes only the predicates' metadata keys.
`SearchPredicatePushdownReport::postings_segment_count` counts the segments
that used postings without metadata decoding;
`postings_prefiltered_segment_count` counts segments that used postings before
residual metadata matching. The report also records the compiled postings and
residual predicate counts.

Readers still open V2 generations (no postings) and V3 generations (plain
metadata postings). V3 postings fall back to the metadata scan for `kind` and
`space_id`, because their payload lacks the V4 semantic capability. Readers
from before V4 refuse V4 layouts explicitly by their format string.

`SearchOutOfCoreReader` opens only these manifests and descriptors, not the
complete document snapshot. Metadata and ACL predicates prune descriptors,
decode only metadata sidecar ranges, and write matching ordered document IDs
into a temporary, bounded, cross-platform candidate spill. The spill keeps one
bounded ID block in memory; its fallible membership checks scope scored BM25
candidates, while document-frequency and normalization statistics cover the
complete current lexical projection. Scalar vector search reads only vector
sidecar ranges and retains the page window, hybrid rank window, or an explicitly
bounded full-score map. Full title, content, embedding, and metadata payloads
are not touched until final-page hydration. `SearchOutOfCoreReader::hydrate_documents`
provides the same row and payload budgets for projection-owned source chunks or
other payloads.

Final-page hit hydration has two levels (`SearchHitHydration`). The default
`Documents` level returns metadata fields, matched terms/spans and the vector
score recomputed from the stored embedding. Vector-only requests without query
terms have no matched spans, so they read only the hit's metadata sidecar
segment and its own binary vector row instead of the full document payload;
the hits are identical. This covers Knowledge Retrieval and Cypher vector seeds
in vector mode. `SearchOutOfCoreReader::search_with_hydration(..,
SearchHitHydration::ScoresOnly)` skips hydration entirely for callers that only
consume IDs and scores; its hits carry the ranking scores and no metadata or
matched terms.

The read report separates metadata-sidecar, vector-sidecar, and hydration
payload bytes, and also exposes range reads, peak decoded sidecar and full
segment bytes, candidate spill and reread bytes, vector bytes, and final
hydration rows and bytes. `OutOfCoreSearchProjection` is the typed Mem
read facade over this path. It uses exact scalar vector segment scans; callers
that require a compressed vector projection receive an error rather than a
silent fallback.

The mutable compatibility `SearchIndex::open()` still materializes the complete
snapshot because its rebuild, incremental mutation, and borrowed-document APIs
require stable references. It is a maintenance owner, not the larger-than-memory
production read owner. `EmbeddedStoreOpenOptions::with_qualified_out_of_core_search_projection`
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
- canonical row pages and demand-paged persistent indexes are governed by
  `specs/ROW_PAGE_AND_DEMAND_PAGED_INDEX_SPEC.md`; column groups remain
  derived projection artifacts and are not a recovery dependency
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
`OutOfCoreSearchProjection` only after generation-bound differential
parity and representative resource qualification pass.
