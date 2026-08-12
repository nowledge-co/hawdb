# Skein Storage TLA+ Models

These models specify the storage publication and recovery protocols that are
implemented by the embedded Skein library. They are executable specifications,
checked over bounded state spaces by TLC.

Run every model with the repository-default pinned TLC release:

```bash
scripts/check-storage-tla.sh
```

Set `TLA2TOOLS_JAR` to use an existing `tla2tools.jar`, or `TLA_JAVA` to select
a Java 11 or newer runtime. Without `TLA2TOOLS_JAR`, the script downloads TLA+
Tools 1.7.4 and verifies its SHA-256 digest before execution.

Set `TLA_RESULTS_DIR` and `TLA_SOURCE_REVISION` to retain a release artifact.
The artifact contains the exact `.tla` and `.cfg` inputs for every model in
the checked set, one complete TLC log per model, the Java version, and a
revision- and tool-bound manifest. CI
validates the downloaded artifact with:

```bash
scripts/check-storage-tla.sh --verify-results tla-results "$GITHUB_SHA"
```

## Durable WAL and Checkpoint Publication

`SkeinStorageDurability.tla` models the default `SyncOnEveryWrite` path. A WAL
batch becomes a durable commit decision at the WAL sync boundary. Applying that
batch makes it visible, and returning from the mutation acknowledges it. A crash
between sync and acknowledgement may therefore recover a committed batch whose
acknowledgement was not observed, which is the standard ambiguous-commit case.
The model uses strict recovery for ordinary startup. One model epoch represents
one logical batch and its contiguous WAL LSN. Each WAL record is framed as a
fragment chain (FULL, or FIRST..MIDDLE*..LAST) inside fixed-size blocks, every
fragment carrying a type-masked, generation-bound checksum. A record whose
fragment chain is incomplete at end of file (torn tail) fails startup and can
only be discarded through the explicit writable doctor repair mode; a
checksum- or sequence-invalid complete chain always fails closed, and a
fragment carrying a stale WAL generation reads as end of log
(recyclable-log discipline), equivalent to clean EOF.

The model checks:

- one process owns the database directory at a time;
- acknowledged commits are never outside the durable prefix;
- visible commits are durable;
- the active WAL is a contiguous suffix after the manifest checkpoint;
- every durable commit is reachable from the published checkpoint plus WAL;
- a manifest references only a durable checkpoint and prepared WAL generation;
- a failed in-memory apply poisons the handle until crash and reopen;
- complete-chain corruption fails closed instead of exposing partial recovered
  state, including corruption in the final WAL record;
- a torn tail is only ever the unsynced tail append of the active generation,
  which makes it the only doctor-repairable state;
- a stale-generation fragment past the logical tail reads as end of log: it
  never blocks recovery and never becomes doctor-repairable.

The checkpoint actions map directly to `GraphStore::checkpoint_with_reader_epoch`
and `DurableStore::{write_checkpoint,prepare_wal_generation,
publish_checkpoint_manifest}`. WAL actions map to
`DurableStore::{append_entry,finish_wal_append}` and `GraphStore::apply_wal_op`.
When several contiguous records share one sync boundary,
`skein_storage::durability::WalSyncGroupState` owns the accumulated record and
byte counts from deferred append through flush reporting. `DurableStore` remains
the filesystem adapter that performs the shared sync. The model treats each
logical batch as a separate commit; the grouped implementation refines that
boundary only when every member is acknowledged after the shared sync and the
handle is poisoned if the barrier fails.

Mutation testing sizes the instance (`MaxCommit = 3`, `MaxGeneration = 2`,
7,383 distinct states): a doctor repair that accepts a checksum-invalid
complete chain at the tail as torn-tail-repairable and discards it reports
`ActiveWalIsContiguous`, a recovery that classifies a stale-generation
fragment as a torn tail eligible for doctor repair reports
`TornTailIsUnsyncedActiveAppend`, and a doctor repair that discards an
incomplete chain located mid-log rather than at end of file reports
`ActiveWalIsContiguous`.

`SkeinWalGroupCommit.tla` models the bounded request queue and the leader-owned
shared durability barrier directly. Entry count is a hard group bound; the byte
threshold is checked after each request and therefore permits at most one
request-sized overshoot, matching the implementation. Applied group members
remain unobservable until the shared sync succeeds. A successful barrier
completes every member only after its assigned contiguous LSN is durable; a sync
failure or leader panic fails the affected group, poisons the database, releases
leadership, and causes queued followers to fail closed. Weak fairness checks that
every submitted request eventually completes or fails instead of waiting forever.
The model intentionally abstracts the collection delay: fixed and adaptive delay
policies both refine `StartGroup` followed by zero or more bounded collection
steps. The implementation preserves that abstraction by never waiting for a
single queued request, retaining the same entry and byte bounds, and clamping
every adaptive delay to the configured `max_delay`. When no completed fsync
baseline is available, a contended queue falls back to the smaller of the
configured bound and the fixed default delay. This keeps cold-start and
post-idle bursts inside the same bounded scheduling refinement without adding a
lone-request delay. Fsync sampling and evidence admission affect scheduling
only; they do not change WAL ordering, durability, visibility, failure, or
acknowledgement transitions represented by the model.

One write-side transition deliberately over-approximates the implementation:
the model's `BeginCommit` clears an exposed stale-generation tail, as a
recycling writer would overwrite it. The implementation's WAL files are
generation-scoped and never recycled, so that exposure is unreachable from
its own lifecycle; the writer appends at physical end of file. If a stale
region were induced externally and then appended after, the remnant would
surface as a doctor-repairable torn tail where the model recovers without
repair — an availability-only divergence in an externally induced state.
Implementation behavior remains a strict subset of the modeled transitions.

`SkeinWalDoctor.tla` models the destructive repair protocol separately from
ordinary recovery. The torn state it repairs is a record whose fragment chain
is incomplete at end of file; a checksum- or sequence-invalid complete chain
never enters the protocol because strict recovery fails closed on it, and a
stale-generation fragment never does because it reads as end of log.
Planning and applying each hold the exclusive database
directory lease. A repair can mutate the WAL only after an exact source-identity
plan has been acknowledged, a quarantine copy is durable, and a `Prepared` audit
record has been published. The retained WAL is published before the `Applied`
audit record, and the pending record is removed last. Crashes preserve these
durable phases so an interrupted repair either resumes or keeps ordinary open
fail-closed. Stale or unacknowledged plans are rejected without changing the WAL.

## Generation Reclamation

`SkeinGenerationReclamation.tla` models immutable checkpoint generations and the
coarse reader-pin policy used by `DurableStore::reclaim_old_generations`.
Publication requires the target generation to be durable. A pinned reader's
generation remains available, reclamation is disabled while any reader exists,
and the current and immediately previous generations remain after reclamation.

Reader actions map to `Database::begin_read_transaction` and `ReaderPin::drop`.
Publication and reclamation map to `DurableStore::publish_checkpoint_manifest`
and `DurableStore::reclaim_old_generations`.

## Durable Projection Cursor and Catch-up

`SkeinProjectionDurability.tla` models the durable projection framework of
[`../specs/COLUMNAR_CANONICAL_AND_PROJECTION_SPEC.md`](../specs/COLUMNAR_CANONICAL_AND_PROJECTION_SPEC.md)
§6. A projection's only durable incremental progress state is the cursor
inside its manifest; a delta artifact becomes durable before the manifest
replace that both registers it and advances the cursor, so a crash at any
point leaves the previous manifest intact and incremental builds are
idempotent from the persisted cursor. Query-time catch-up over
`(cursor, currentEpoch]` is derived from WAL replay, so serving as Ready is
sound only while the cursor sits at or above the WAL replay floor.
Reclamation may pass the cursor of a projection lagging beyond the staleness
bound, and doing so forces the projection out of Ready until a full rebuild
publishes a current manifest.

The model checks that the cursor and replay floor never pass the canonical
epoch, that a Ready projection can always derive its catch-up window
(serve-soundness: served state equals a full rebuild at the current epoch),
that an in-flight delta covers exactly `(cursor, target]` with no coverage
gap, that a projection below the floor is never served as Ready, and that
the cursor only ever references durable artifact coverage.

Mutation testing sizes the instance (`MaxEpoch = 4`, `StaleLimit = 2`, 679
distinct states): keeping the projection Ready when reclamation passes its
cursor reports `ReadyImpliesCatchUpCoverage`, publishing a manifest without
first making the delta artifact durable reports `CursorIsAlwaysDurable`, and
recovery that ignores the replay floor reports
`ReadyImpliesCatchUpCoverage`. The generation-diff catch-up fallback of
§6.4 is deliberately not modeled; the model treats a below-floor cursor as
requiring rebuild, which over-approximates the implementation conservatively.

## Layered Columnar Visibility and Compaction Identity

`SkeinCompactionVisibility.tla` models the layered read path of
[`../specs/COLUMNAR_CANONICAL_AND_PROJECTION_SPEC.md`](../specs/COLUMNAR_CANONICAL_AND_PROJECTION_SPEC.md)
§3.3: base groups filtered by generation-scoped deletion vectors, delta
groups, and the WAL-backed memtable. Flush publishes a new generation by
marking superseded base rows in the deletion vector and appending delta
rows without rewriting base bytes; compaction publishes a merged
representation that must not change the visible state; readers pin one
immutable generation under the coarse reclamation policy of
`SkeinGenerationReclamation.tla`.

Compaction is modeled as separate preparation and publication actions.
Preparation records the source manifest generation whose immutable group
row ordinals and cumulative deletion vector it consumed. Publication is a
generation-guarded compare-and-swap: if a flush advanced the current
generation while compaction was running, the prepared output is discarded
instead of publishing over the newer deletion frontier. This is the embedded
single-publisher counterpart of the row-id conversion and concurrent delete
bitmap reconciliation required by distributed merge-on-write engines.

Records carry per-key version numbers and the scan is modeled as the set of
emitted versions per key. That choice is what gives the deletion vector's
obligation teeth: under a key-presence abstraction, a flush that appends a
superseding delta row but fails to mask the stale base row is
indistinguishable from a correct merge, because both collapse to "present".

The model checks that the layered read overlaid with the memtable emits
exactly the committed logical state (one current version per live key,
never a stale duplicate) across every interleaving of commits, flush,
compaction, crash, and reclamation; that a pinned reader observes its
recorded durable view for the lifetime of the pin; that pinned and current
generations remain available; that a scan emits at most one version per
key; that deletion vectors only mark rows that exist in the base column;
and that every prepared compaction is an identity transform of the exact
source generation it names.

Mutation testing sizes the instance (two keys, two readers,
`MaxVersion = 2`, `MaxGeneration = 3`, about 176k distinct states): a flush
that marks deletion-vector entries only for deletes but not for
superseding puts reports `LayeredReadEqualsLogicalState`, a compaction that
drops delta rows reports `LayeredReadEqualsLogicalState`, and a flush that
rewrites the published current generation in place instead of publishing
the next one reports `PinnedViewIsImmutable`. Removing the compaction
publication generation guard permits `PrepareCompaction -> Flush ->
PublishCompaction` and reports `LayeredReadEqualsLogicalState`: the stale
output loses the flush's newer deletion vector and delta rows.

## Layered Column-Group Manifest Publication

`SkeinColumnGroupManifest.tla` models the publication protocol in
[`../specs/COLUMNAR_CANONICAL_AND_PROJECTION_SPEC.md`](../specs/COLUMNAR_CANONICAL_AND_PROJECTION_SPEC.md)
§3.6. Newly changed immutable artifacts become durable before their per-table
directories; directories become durable before a single active manifest
atomically selects the complete catalog. Untouched tables keep referencing an
older immutable directory, so checkpoint metadata writes scale with changed
tables rather than total tables.

Each candidate carries the active generation from which it was prepared.
Publication compares that parent with the current manifest while holding the
filesystem publish lease. A candidate made stale by another publication cannot
replace the active manifest. Crashes discard only volatile preparation and
readers; orphan durable artifacts and directories remain unreachable because
recovery opens the active manifest rather than discovering files by directory
scan.

The model checks that the active manifest and every table reference name
durable metadata generations, table-directory generations never lead the
active manifest, one generation never acquires two catalog identities,
prepared directories follow durable artifacts, reader-pinned catalog views
remain immutable, and stale candidates cannot publish.

Mutation testing removes the parent-generation equality from
`PublishCandidate`. TLC then reports `StaleCandidateCannotPublish` after
`BeginCandidate -> MakeArtifactsDurable -> MakeDirectoriesDurable ->
PublishRacingManifest`: the stale candidate remains enabled to overwrite the
generation already selected by the racing publisher. This demonstrates that
the model distinguishes a serialized publish lease from the required
generation compare-and-swap.

## Implementation Refinement Evidence

The Rust tests below exercise the concrete boundaries represented by the model.
They are implementation evidence, not a machine-checked refinement proof.

| Protocol obligation | Implementation boundary | Regression evidence |
| --- | --- | --- |
| WAL sync precedes visibility and apply failure closes the handle | `finish_wal_append`, `apply_wal_op`, `ensure_usable` | `post_wal_apply_failure_poisons_handle_until_reopen` |
| A grouped WAL sync acknowledges every member after one successful barrier or fails the whole group closed | `WalSyncGroupState`, `finish_wal_sync_group`, `CommitSequencer` | `wal_group_commit_shares_one_sync_without_changing_record_order`, `wal_group_sync_failure_rejects_commit_and_poisons_until_reopen`, `panicking_group_commit_task_completes_followers_and_releases_leader` |
| Fixed and adaptive collection policies remain bounded scheduling refinements, use a bounded fallback without a recent baseline, and never delay a lone request | `effective_group_commit_delay`, `wait_for_group_commit_peers`, `WalGroupCommitConfig::adaptive_enabled_after_evidence` | `wal_group_commit_skips_the_coalescing_window_without_contention`, `adaptive_delay_is_derived_from_the_completed_baseline`, `adaptive_delay_uses_bounded_fallback_before_the_completed_sample_floor`, `adaptive_delay_falls_back_after_the_recent_window_expires`, `wal_group_commit_requires_performance_and_recovery_evidence` |
| A torn WAL batch has no partial recovered visibility | `replay_wal` record decode and batch apply | `default_recovery_rejects_torn_wal_tail_until_explicit_doctor_repair`, `doctor_discards_torn_batch_wal_without_partial_path_recovery` |
| Doctor repair binds destructive truncation to an exact acknowledged plan and resumes a durable pending audit | `DatabaseDoctor::{plan_wal_tail_repair,apply_wal_tail_repair}` | `apply_rejects_toctou_change_without_preparing_repair`, `prepared_repair_blocks_open_and_can_continue`, `truncated_pending_repair_is_resumable_and_blocks_open_until_finalized` |
| Complete-record corruption and LSN gaps fail closed | `replay_wal` framing, checksum, and expected-LSN checks | `rejects_and_quarantines_checksum_corruption_at_wal_tail`, `rejects_and_quarantines_checksum_corruption_before_valid_wal_suffix`, `rejects_and_quarantines_non_contiguous_wal_lsn` |
| Checkpoint publication selects one complete generation | checkpoint failpoints and manifest replacement | `checkpoint_publish_failpoints_recover_one_complete_generation`, `subprocess_crash_matrix_recovers_whole_batches_and_artifact_generations` |
| Reader pins prevent generation reclamation | `ReaderPins`, `reclaim_old_generations` | `read_transaction_pins_checkpoint_manifest_until_drop`, `out_of_core_reader_pin_retains_its_canonical_generation_until_drop` |
| Canonical path aliases share one ownership boundary | `DatabaseDirectoryLease::acquire` | `durable_database_open_is_exclusive_until_owner_drops`, `durable_database_rejects_path_alias_until_owner_drops` |
| Stale optimistic commits fail before publication and fine-grained locks preserve compatibility | `commit_mutation_transaction_and_relational`, `LockTable` | `optimistic_transactions_prepare_in_parallel_and_reject_the_stale_committer`, `disjoint_primary_key_point_locks_allow_both_pessimistic_writers_to_commit`, `shared_primary_key_range_blocks_phantoms_but_not_the_excluded_boundary` |
| A deadlock-closing multi-owner wait edge selects one victim and releases its dependencies | `WaitForGraph::register`, `ConcurrentDatabaseTransaction::abort_after_lock_failure` | `point_lock_upgrade_cycle_selects_one_deadlock_victim`, `wait_for_graph_detects_a_cycle_with_multiple_blockers` |
| A stale, mixed, missing, or corrupt Source scan sidecar falls back to the canonical graph | `source_scan::load`, `ScanSegmentManifest::plan_scan` | `checkpoint_publishes_source_scan_and_wal_mutation_invalidates_it`, `corrupted_source_scan_artifact_never_blocks_canonical_graph_recovery` |
| A column-group catalog publishes artifacts and changed table directories before one generation-CAS manifest; reopen ignores orphan candidates and fails closed on referenced corruption | `ColumnGroupTableDirectory::write_immutable`, `ColumnGroupManifest::{publish,open}`, `PublishedColumnGroupCatalog::scrub_artifacts` | `publishes_reopens_and_reuses_untouched_table_directory`, `stale_publishers_are_serialized_and_one_fails_closed`, `orphan_candidate_is_ignored_and_corrupt_published_metadata_fails_closed`, `deep_scrub_detects_payload_corruption_not_read_by_reopen` |
| System schema objects and migration identities publish atomically; invalid, future, read-only, failed-DDL, and crash-recovered states never return a usable partially upgraded handle | `Database::apply_system_schema_registry`, `execute_database_transaction_sql`, `GraphStore::commit_mutation_transaction_and_relational` | `application_system_schema_upgrades_and_reopens_idempotently`, `application_system_schema_upgrade_crash_recovers_a_consistent_registry_and_schema`, `application_system_schema_rejects_changed_applied_migration`, `application_system_schema_rejects_a_database_from_a_newer_binary`, `failed_application_system_schema_upgrade_does_not_publish_version`, `read_only_database_rejects_pending_application_system_schema_upgrade` |
| Skein Lightning derives a registry-complete export without advancing an in-memory source epoch and imports only a valid stream into an empty or verified engine-only target | `Database::skein_lightning_relational_state`, `Database::skein_lightning_initial_import_apply_internal`, `GraphStore::import_skein_snapshot_rows_with_source_fingerprint` | `skein_lightning_initial_import_apply_imports_database_state_into_empty_target`, `skein_lightning_initial_import_rejects_stream_without_engine_registry` |

## In-memory Snapshot Publication

`SkeinConcurrentSnapshots.tla` models `skein-storage::SnapshotCoordinator`.
Readers pin immutable `Arc` snapshots, one writer stages the next epoch, and the
published pointer changes only after the durability callback succeeds.

## Optimistic and Pessimistic Transaction Publication

`SkeinTransactionConcurrency.tla` models the in-process `ConcurrentDatabase`
publication boundary. Optimistic transactions prepare on independent immutable
COW snapshots, acquire the database-exclusive target before publication, and
use first-committer-wins epoch validation. Pessimistic transactions acquire
shared or exclusive point/range spans. Database locks are represented by the
full key set; a point is a singleton and a bounded range is a finite key subset.
The finite-set abstraction deliberately over-approximates interval shapes while
preserving overlap and compatibility safety. Both modes serialize the durable
WAL decision and snapshot publication while readers continue to pin the last
published epoch.

The model checks that stale optimistic transactions cannot publish, only one
transaction owns the commit pipeline, overlapping shared/exclusive lock spans
remain compatible, an optimistic publisher owns the full exclusive span,
commit epochs are unique, uncommitted work is not exposed to snapshot readers,
a deadlock-closing multi-owner dependency selects the current waiter as victim,
the victim releases its locks and dependencies, the wait-for graph stays
acyclic, and a crash after WAL durability recovers the committed epoch.

## Derived Source Segment Publication

`SkeinSourceSegmentPublication.tla` models Source scan sidecars, including the
fixed-name publication order used by `publish_checkpoint_sidecars`. The payload
and descriptor are durably replaced before the checkpoint manifest, so a crash
may leave an old manifest beside a mixed or newer sidecar. Such a sidecar is
never selectable: open-time validation discards it when writable, and every read
falls back to the authoritative graph. A reader selects the sidecar only when
the pinned graph epoch, published manifest epoch, live artifact epoch, and
durably built identity all agree.

## Ordered System Schema Upgrade and Import

`SkeinSystemSchemaUpgrade.tla` models startup validation, ordered migration
staging, the shared WAL durability decision, handle publication, DDL failure,
read-only rejection, crash recovery, and the Skein Lightning registry boundary.
Schema objects and migration records are separate modeled variables so TLC can
detect any publication step that advances one without the other. A usable
handle is absent until the durable pair is current and validated.

The import sub-protocol accepts only a registry-valid stream and an empty or
verified engine-only target. Import visibility follows durability, including
recovery from a crash after sync. The export action records the source epoch
without changing it and always derives a valid engine registry, matching the
non-mutating in-memory export path.

The checked-in instance has two migration versions and explores 10,944
distinct states. Mutation testing confirms that leaving the registry version
unchanged while the schema version reaches the sync boundary violates
`DurableSchemaAndRegistryAreAtomic` after four transitions. This validates that
the model distinguishes atomic publication from schema-only durability.

## CRDT Replication Between Skein Nodes

`SkeinCrdtReplication.tla` models the delta-state CRDT contract in
[`../specs/SKEIN_CRDT_REPLICATION_SPEC.md`](../specs/SKEIN_CRDT_REPLICATION_SPEC.md).
Node and relationship occurrences are ORSWOT observed-remove sets keyed by a
durable dot `<<ReplicaId, counter>>`; the causal context is a version vector;
properties are per-occurrence LWW registers ordered by `<<hlc, ReplicaId>>`.
Anti-entropy is modeled as the state join that per-origin gap-free delta
segments refine, which is also why crash-and-retry re-delivery is safe.

The model checks that replicas with equal causal contexts hold identical
state, that fair anti-entropy converges both replicas, that every live record
and observed timestamp is causally covered, that a replicated edge never
references an occurrence outside the receiver's context, that observed-removed
dots never resurrect, and that occurrence dots stay unique and never exceed
minted operations. Visible-graph referential integrity holds by construction
of `VisibleEdges`: a `DETACH DELETE` concurrent with an incident edge create
leaves the edge masked rather than dangling.

A receiver acknowledges a joined batch only after it is durable, and the
sender records that acknowledged context. `AcknowledgedContextNeverExceedsPeer`
checks that a replica never believes a peer has applied more than it has,
which is what makes acknowledged contexts safe to gate retention on. `Crash`
drops an owed acknowledgement while durable state survives, so the ambiguous
apply-then-crash case is retried; the join is idempotent, so re-applying the
same batch changes nothing.

The checked-in instance is two replicas, one key, two values, and two
operations per replica (about 112k distinct states, three seconds). It was
chosen by mutation testing rather than by size, and it still reports a
violation for each seeded defect: deleting the causal clock merge from `Sync`
reports `LiveRecordsAreCovered`, replacing the edge join with a naive union
reports `ConvergedWhenContextsEqual` and `RemovedDotsStayRemoved`, and making
`Ack` record the sender's context instead of the receiver's applied one
reports `AcknowledgedContextNeverExceedsPeer`. Masked edges remain reachable
at this size. A two-key instance (about 1.8M distinct states, forty-six
seconds on eighteen workers) was also checked with no error; it is not the CI
default because it costs sixteen times more for no additional
defect-detection power on the seeded defects above.

Two limits are worth stating plainly. Three-replica instances do not
terminate at this shape — tracking each replica's view of every peer's
acknowledged context adds a version vector per ordered pair, and the smallest
configuration exceeded seven million distinct states without finishing — so
transitive delivery is argued from the join's algebra rather than checked.
And masked-edge reclamation is deliberately absent: under a state-join
abstraction a premature drop is indistinguishable from an ordinary
observed-remove, so a GC action would pass with or without its stability
guard. That guard protects against a hazard that only exists once delta
segments are modeled as segments, which needs its own model.

Checking this model found a real design defect: with a plain per-replica
commit counter as the LWW timestamp, a replica can overwrite a register it
has already observed with a smaller timestamp, and the stale value wins the
next join. The specification now requires the hybrid logical clock to merge
past every observed timestamp on delta apply, which the model represents as
a Lamport clock merged on each sync round.

## Confirmed-Log Delivery Between Slaves

`SkeinGossipDelivery.tla` models the delivery layer of the master-slave CRDT
contract. It is deliberately separate from `SkeinCrdtReplication.tla`: that
model checks what the join computes once a batch arrives, this one checks
what arrives and what is allowed to. The split is what makes three replicas
checkable.

The rule it exists for is that a slave's local operation stays pending until
the master confirms it, and that gossip carries confirmed operations only.
Pending work reaches the master over the session and nowhere else, so the
master has seen everything that exists anywhere in the deployment.

That rule is what collapses the delivery problem. Gossip carries one totally
ordered confirmation log, so a digest is a single integer and a response is
a contiguous run above it — no version vector on the wire, and no reorder
buffer, because a responder shipping from the position the requester
declared cannot leave a hole. `held` is explicit state rather than derived,
so an action that shipped pending work is expressible and therefore
catchable.

The model checks that a replica holds only confirmed work and its own, that
no replica claims a position beyond the log, that positions are contiguous
and unique, and that a position implies possession of its whole prefix.
Under fair sessions and rounds it also checks that every operation is
confirmed and reaches every replica.

The checked-in instance is three nodes with `n1` as master and two
operations per replica (about 47k distinct states, three seconds). Mutation
testing sizes it: leaking the peer's pending work into a gossip response
reports `HeldIsConfirmedOrOwn`, advancing a position past what was actually
pulled reports `PositionImpliesPrefix`, and reusing a confirmation position
instead of appending reports `LogIsContiguousAndUnique`.

### Losing the Master

`SlaveFairSpec` and `SlavesAgreeWithoutMaster` cover the partition case:
only gossip is fair, so the master may stall forever, and the property is
that slaves still agree on the confirmed prefix. They do not converge on
each other's pending work — that is the stated cost of the confirmation
rule, not a defect. Because TLC takes one specification per configuration,
this is checked out of band:

```bash
sed -e 's/^SPECIFICATION FairSpec/SPECIFICATION SlaveFairSpec/' \
    -e 's/^PROPERTY EventualDelivery/PROPERTY SlavesAgreeWithoutMaster/' \
    docs/tla/SkeinGossipDelivery.cfg > /tmp/partition.cfg
```

It passes, and it has teeth: disabling slave-to-slave gossip so everything
must route through the master makes it fail.

What neither model covers is the composition itself. That fair delivery
plus a convergent join yields a convergent system is argued from the join's
commutativity, associativity, and idempotence, not machine-checked, because
the composed model is the three-replica instance that does not terminate.

## Proof Boundary

TLC exhaustively checks the configured finite instances; it is not a proof of
the Rust implementation, the filesystem, or arbitrary-sized instances. The
models establish safety invariants, not operation latency, bounded-wait
implementation behavior, automatic SQL lock-range inference correctness,
transaction-snapshot refresh or rebase refinement, or starvation freedom, and
rely on these environmental assumptions:

- successful `sync_data` or `sync_all` survives a crash;
- durable file replacement is atomic and the parent-directory sync preserves
  the selected name on supported filesystems;
- the OS file lock provides exclusive ownership for a canonical directory;
- type-masked, generation-bound fragment checksums detect malformed complete
  chains, a fragment carrying a stale WAL generation reads as end of log, and
  doctor repair may discard only an incomplete fragment chain at the
  non-synced WAL tail;
- validated WAL batches replay deterministically, or recovery fails without
  publishing a database handle;
- the model's checkpoint artifact represents the checkpoint, canonical graph,
  adjacency, property spill, and property projection artifacts as one validated
  generation selected by the manifest.

The group-commit liveness property assumes a finite submitted request set and
weakly fair scheduling of the leader, queue drain, durability barrier, request
completion, and poison rejection actions. It does not establish a wall-clock
latency bound for the Rust `Condvar` implementation.

The WAL-doctor liveness property assumes strong fairness for resuming an exact
pending repair and for its truncate, applied-audit, and pending-audit removal
steps. External modification after the durable `Prepared` audit record is outside
that progress assumption and remains a fail-closed integrity error.

`SyncOnCheckpoint` is intentionally outside the acknowledged-commit durability
claim: writes accepted under that policy may be lost before the next successful
checkpoint. Fault-injection, cross-platform recovery, and filesystem tests are
still required to validate that the implementation refines these models and
that the environmental assumptions hold.
