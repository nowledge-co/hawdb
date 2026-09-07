# Skein Storage TLA+ Models

These models specify storage publication and recovery protocols implemented by
the embedded Skein library or required before a planned storage path may be
activated. They are executable specifications checked over bounded state spaces
by TLC.

Run every model through the hermetic `rules_tla` Bazel targets. Bazel resolves
the pinned TLA+ Tools artifact and the `remotejdk_21` runtime selected by the
repository `.bazelrc`; callers do not need `JAVA_HOME`. `--jobs=1` bounds outer
model parallelism because each TLC process already owns an internal worker
pool:

```bash
bazel test //docs/tla:storage_models --jobs=1 --test_output=errors
```

`rules_tla` is maintained locally in `third_party/rules_tla`, based on upstream
0.2.0 with its source revision and MIT license retained. It resolves TLA+ Tools
1.7.4 by version and SHA-256 and uses Bazel's Java runtime toolchain. Each
`.tla`/`.cfg` pair is an individual `tla_check`; the `storage_models` suite is
the CI gate. [`storage_models.bzl`](storage_models.bzl) is the single,
alphabetically ordered model-set manifest consumed by both Bazel and retained
evidence collection. The collector rejects duplicates, a missing model pair,
or any `.tla`/`.cfg` file omitted from that manifest, so its artifact cannot
silently lag the authoritative Bazel suite.

Each model-checking action now declares its complete evidence as an output:
the actual `.tla` and `.cfg` inputs, full TLC log, Java version, JAR digest,
TLC arguments, and outcome/exit code. The `storage_model_evidence` target
materializes these outputs, including on cache hits. Request it alongside the
suite to check each model only once and retain the same action's evidence:

```bash
bazel test //docs/tla:storage_models //docs/tla:storage_model_evidence \
  //docs/tla/tests:rule_contract_tests //:storage_tla_evidence_script_test \
  --jobs=1 --test_output=errors
scripts/check-storage-tla.sh --check-mutants
scripts/check-storage-tla.sh --collect-bazel-results \
  bazel-bin/docs/tla tla-results "$SOURCE_REVISION"
scripts/check-storage-tla.sh --verify-results tla-results "$SOURCE_REVISION"
```

The collector does not launch Java, download TLC, or recompute any state graph.
It requires a successful zero-exit action for every declared model, complete
success logs without errors, byte-identical source/configuration snapshots,
the pinned tool digest, and unchanged full-check arguments. A passing expected
counterexample cannot substitute for successful model evidence. The output
directory must be empty, and the source-revision-bound manifest is published
only after the complete model set passes validation. Cache reuse is retained
evidence from identical action inputs, not a claim of fresh TLC execution.

The standalone `scripts/check-storage-tla.sh` entrypoint remains available for
developer use. Set `TLA_RESULTS_DIR` and `TLA_SOURCE_REVISION` to retain its full
campaign evidence. CI must not run that no-argument entrypoint after Bazel:
collection replaces that duplicate computation, not the model or mutant gates.

### COW liveness scheduling

`SkeinCowPagePublication` uses TLC's `-lncheck final` in both Bazel and the
standalone runner. TLC still explores the complete configured state graph,
checks every invariant, and checks the unchanged liveness property over that
complete graph. Only intermediate liveness scans of growing partial graphs
are deferred; liveness counterexamples may therefore be reported later.
See the [TLC option documentation](https://github.com/tlaplus/tlaplus/blob/5a47802/general/docs/current-tools.md#command-line-options).

The evidence collector requires this model's exact arguments and a completed
success log containing the final complete-state-space temporal check. Other
models keep their original arguments. Model/configuration bytes, state bounds,
transitions, fairness, invariants, liveness properties, worker selection and
timeouts are unchanged. This is not a switch to simulation or safety-only
checking, and moving full verification to a periodic job is a separate decision.

### Model shards

The `tla_test_suite` declaration supports `shard_count`, currently `1`. With
`shard_count = N`, it exposes `storage_models_shard_0` through
`storage_models_shard_<N-1>` and corresponding `_evidence` filegroups. Each
shard checks different complete models; it does not split a single model's
state graph or disable any property. The unsuffixed suite still checks all
models. Increasing this setting alone does not provision more CI jobs.

For example, after configuring two shards, run each shard's suite and evidence
target in its own worker. Retain only the declared evidence outputs, then merge
the extracted TLA directories in zero-based shard order:

```bash
scripts/check-storage-tla.sh --collect-bazel-shards \
  tla-results "$SOURCE_REVISION" shard-0/docs/tla shard-1/docs/tla
scripts/check-storage-tla.sh --verify-results tla-results "$SOURCE_REVISION"
```

The collector infers the shard count from the directory list and checks the
same round-robin assignment as the suite. Every model must appear exactly once
in the correct shard. Partial shards never receive a full-campaign manifest.
The separate mutant gate remains mandatory. CI keeps one shard by default;
timeouts and resource settings are unchanged.

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

The implementation has one unreleased v1 WAL layout: `SKWALB01` binary
framing. Text headers and unknown magic are corruption, not a migration state.
The model therefore has no format-upgrade transition; every modeled WAL record
already satisfies the single binary framing contract before durability actions
begin.

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

## Strict Append Storage

`SkeinAppendTable.tla` models the generic admission and visibility contract for
a strict append table. Each batch belongs to one partition, contains a bounded
non-empty set of order keys, and is admitted only when every key is above that
partition's visible watermark. The complete batch becomes durable through one
WAL transition and visible through one publication transition. Recovery exposes
an exact durable batch prefix, including a durable but previously unpublished
batch, while an unsynced batch disappears. Duplicate and out-of-order batches
are rejected without changing canonical state.

The Rust refinement maps the modeled partition and order key to catalog
column identities, `durableBatches` to relational WAL append records,
`visibleBatchCount` to the published append live view, and `visibleRows` to the
snapshot-visible row set. `AppendState::stage_transaction` validates the
watermark in a private state, while `GraphStore::commit_kernel_write_batch`
publishes the complete state only after the shared WAL accepts it.

`SkeinAppendSegmentPublication.tla` models checkpointing durable append batches
into immutable segments. Segment bytes become durable before their manifest,
and the manifest becomes durable before the active selector changes. Active,
previous, candidate, and reader-pinned generations form the reclamation closure.
The candidate generation remains protected between manifest durability and
selector publication; omitting that protection permits GC to remove a complete
candidate immediately before it becomes canonical. WAL truncation advances only
to an already-published checkpoint epoch, and crash recovery retains the active
checkpoint plus the durable WAL suffix.

The Rust refinement maps candidate phases to append segment builder and
manifest publication calls, generation sets to immutable segment manifests,
reader generations to `SnapshotCoordinator` pins, and `walFloorEpoch` to the
storage reclamation watermark. Segment encoding, checksums, byte admission, and
filesystem operations remain concrete refinement obligations.

`SkeinAppendMixedTransaction.tla` models a single transaction whose private
workspace changes graph state, mutable row-page state, and strict append state.
The complete workspace receives one durability decision and one visible commit
epoch. Publication cannot expose only one component, crash recovery selects a
complete durable state, and a pinned reader retains its original triple while
later transactions commit.

`SkeinGeneratedAppendOrder.tla` models table-wide `commit_sequence`
allocation at the serialized commit boundary. Each accepted request receives
one contiguous interval before its materialized rows enter the WAL history,
while abort and sequence exhaustion leave the allocator unchanged. Pending
intervals remain outside the visible durable prefix. A group sync publishes all
of its intervals together; an uncertain sync poisons the handle, and recovery
retains an exact WAL prefix before reconstructing the next value. The checked
invariants cover interval contiguity, no reuse within the retained history,
durability-before-visibility, no pending visibility, and no consumption on
abort or exhaustion.

Negative-control configurations live under `docs/tla/mutants`. Each enables one
unsafe transition and must violate its declared invariant. They are checked
separately so deliberately failing models cannot enter the positive Bazel suite:

```bash
scripts/check-storage-tla.sh --check-mutants
```

The controls cover publication before WAL sync, skipped partition watermarks,
partial recovery, manifest-before-segment publication, pinned-generation
reclamation, and partial mixed-transaction publication.

## Query-Owned Runtime Memory

`SkeinKnowledgeRetrievalPipeline.tla` models the application-facing retrieval
stage machine over one pinned canonical graph snapshot. Search identities are
metadata-filtered before authorized graph expansion; compact candidates are
reranked and truncated by TopK before canonical output hydration. Query-memory
or result-payload rejection terminates without returning partial hydrated
output. TLC checks snapshot equality, stage completion, authorization before
expansion, TopK-before-hydration, and both resource bounds. The implementation
refinement is documented in
`docs/specs/KNOWLEDGE_RETRIEVAL_PIPELINE_SPEC.md`.

`SkeinQueryMemoryLedger.tla` models the runtime refinement implemented by
`QueryMemoryLedger`, `QueryMemoryAccount`, and RAII `QueryMemoryLease` values.
Every reservation is admitted atomically against its operator-local budget and
the query root before the corresponding executor allocation. Pipeline batches,
blocking state, spill encoding buffers, morsel output, and result
materialization are separate accounts under that root.

`Transfer(source, target)` models a synchronous ownership handoff without a
release/reserve gap and without requiring double root capacity for one retained
object. The implementation's byte-granular transfer validates the target
account and the post-transfer root total before changing either lease; a larger
representation is therefore admitted as part of the same atomic transition.
Aggregate group, partial-group, and spill-merge output uses this transition to
move ownership from blocking state to its pipeline batch. Input rows remain
charged until accumulation completes, and an emitted batch releases its lease
only at the synchronous consumer boundary.

The model distinguishes a streaming completion, which owns no query memory,
from a materialized-result handoff, which may retain an admitted result lease
until the caller-owned result is dropped. The handoff is enabled only after the
pipeline, blocking, spill-staging, and morsel-output accounts reach zero, and a
returned state may retain bytes only in the result account. Failure and
cancellation release all accounts. TLC checks the root and local bounds, this
handoff isolation, and the no-leak terminal-state invariant. The implementation
records root budget, peak charge, completion charge, and account count in
`PipelineMemoryReport`.

Relational ordering refines the blocking-state and spill-staging branches with
`ExternalTopN<RelationalSortRecord>`. Retained state contains typed sort keys,
layout-slot row locators, and an executor-owned stable ordinal rather than
public `Binding` maps. Spill encoding reserves the complete framed payload
before allocation; merge decoding keeps the encoded staging lease live until
the decoded record has been admitted to the blocking account. Normal
completion, early consumer stop, decode failure, cancellation, and unwinding
drop both account leases.

The shared Binding spill fallback used by generic sort, partial and mixed
aggregate, and distinct operators follows the same refinement. The writer
derives the complete record length from the codec before allocating its buffer.
The reader retains the encoded staging lease while constructing the decoded
operator item and admits that item to the query-rooted blocking tracker before
releasing staging. Pair compaction, final fan-in, and emitted sort/distinct
batches preserve a rooted owner across each synchronous ownership transfer;
normal completion and every error path therefore return both accounts to zero.
`NodeCartesianProductExec` uses that shared decoded-record handoff for build
replay and reserves a conservative merged-binding bound before cloning either
side. Its output account transfers to the enclosing pipeline synchronously, so
consumer stop and replay failure cannot strand decoded or output ownership.

Typed adjacency expansion refines the zero-transient-state branch of this
model: `AdjacencyPostingList` keeps live entries ordered in snapshot-owned
storage, and the ordered reader merges canonical and live cursors without
reserving a degree-sized query buffer. The untyped cross-relationship-type and
filtered ordering fallbacks charge every compact key to the query blocking
account before insertion. Expansion result batches hold a pipeline lease while
being constructed and transfer it synchronously at emission; the optional
expanded-node report set is charged to the same blocking account. Root
rejection therefore stops collection and releases all three transient states.
Posting pivots and bounded deltas remain immutable for pinned snapshots, so
their visibility follows `SkeinConcurrentSnapshots.tla`; durable canonical
adjacency ordering and publication continue to follow
`SkeinGraphDescriptorPaging.tla`.

Node, property, composite, range, and text index scans refine the pipeline
account branch through `AccountedBindingBatch`: each row is charged before it
enters the retained batch, row and byte boundaries release only at synchronous
emission, and early stop or error drops the remaining lease. Relationship
existence predicates and optional degree projections use the adjacency visitor
instead of a materialized match vector. Their ordered storage fallback receives
the query blocking account, so any compact cross-type merge keys remain under
the same root.

Traversal materialization refines the same synchronous-transfer rule.
`ShortestPathExec` retains frontier and completed paths in one rooted blocking
account, consumes adjacency through the bounded visitor, and moves hydrated
results into `AccountedBindingSet`. `ThreadRepairStatsExec` retains only its
admitted identity/thread state and output rows while relationship counts remain
streaming. `AccountedBindingSet::emit_batches` releases each owned batch only
as the enclosing pipeline accepts it; early stop drops the remaining lease.

## Bounded Morsel Merge

`SkeinBoundedMorselMerge.tla` models the production shared-pool result path for
parallel morsels. Issuance is limited to one capacity-sized window beyond the
coordinator's consumed ordinal prefix. Completed outputs move through a bounded
channel and an ordinal reorder set before emission; the window advances only
after the expected output crosses the coordinator consumer boundary. This
prevents a slow early morsel from allowing later workers to accumulate an
unbounded reorder tail.

The implementation refinement is
`BoundedExecutor::try_for_each_index_ordered` together with
`SharedPoolMorselScheduler::execute_accounted_ordered`. Worker results hold
`MorselOutput` query-memory leases from before construction through ordered
consumption. Each modeled morsel output maps to one bounded container of typed
`ColumnarBatch` values in the production numeric path. The path selects serial
execution before scheduling when the projection is not fully typed or the whole
container cannot fit, so there is no post-computation serial replay transition.
Its input-reference wave is reserved in a query-rooted pipeline account before
allocation, while the model abstracts that already-admitted immutable input
state. Cancellation, consumer error, and worker panic stop issuance, disconnect
blocked sends, join the shared-pool tasks, and release every lease. TLC checks
the admission window, channel and reorder bounds, emitted-prefix ordering,
terminal cleanup, and eventual success or failure over the bounded instance.

## Generation Reclamation

`SkeinGenerationReclamation.tla` models immutable checkpoint generations and the
coarse reader-pin policy used by `DurableStore::reclaim_old_generations`.
Publication requires the target generation to be durable. A pinned reader's
generation remains available, reclamation is disabled while any reader exists,
and the current and immediately previous generations remain after reclamation.

Reader actions map to `Database::begin_read_transaction` and `ReaderPin::drop`.
Publication and reclamation map to `DurableStore::publish_checkpoint_manifest`
and `DurableStore::reclaim_old_generations`.

## External Search Projection Changefeed

`SkeinProjectionChangefeed.tla` models the unified graph and relational
identity feed. A canonical epoch becomes visible only after its WAL mutation is
durable, and every WAL epoch carries either an exact bounded identity set or a
fixed rebuild barrier. Incremental resume starts at or above the retained
floor, copies whole exact commits, processes every selected commit, and only
then publishes the external projection watermark. Prefix GC advances the
floor by whole commits; an expired cursor or barrier requires a canonical full
rebuild. The model intentionally has no search-document, embedding, ranking,
or SearchIndex-manifest variable because those values remain outside canonical
WAL state.

The runtime refinement is `RelationalPrimaryKeyChangeCapture`, relational WAL
capture encoding, `SearchProjectionChange`,
`Database::build_search_projection_change_batch_after`, and
`Database::apply_search_projection_change_batch`. Checkpoint recovery restores
the same identities and rebuild barriers; entry and byte retention limits
implement the modeled prefix-GC floor.

## Canonical COW Row-Page Publication

`SkeinCowPagePublication.tla` specifies the publication protocol required by
the canonical row-page format before serving activation. A commit is visible
only after its WAL record is durable. The recoverable dirty overlay is exactly
the visible WAL suffix after the published manifest epoch. A checkpoint writes
only dirty pages into a fresh physical generation, reuses immutable clean-page
references from its selected base root, durably publishes page, root, and
generation-manifest artifacts in order, and replaces the latest manifest last.
The generation manifest contains the complete digest-bound table schemas and
descriptor-derived row counts. The model records schema-catalog durability at
the manifest step and forbids selecting or retaining a root without that
catalog.

The model includes a competing checkpoint so a candidate prepared from a stale
base must be rejected. Published generations and epochs cannot regress.
Readers pin manifest generations; reclamation therefore retains the active,
immediately previous, and reader-pinned roots together with their complete
cross-generation page-reference closure. Crash recovery discards volatile
commits, readers, and unpublished candidates while reconstructing the dirty
overlay from durable WAL.

The configured instance uses two readers, two logical commit epochs, four
physical generations, and two logical pages. Page zero represents a durable
graph-only commit with no relational dirty page. TLC checks WAL-before-visible,
manifest-last publication, immutable physical page identity, dirty-page COW,
stale-builder rejection, pinned-root retention, durable reference and schema
closure,
exact row/overflow generation agreement, candidate isolation before outer
checkpoint publication, source-epoch preservation for relocated clean pages,
and crash recovery. Maintenance may begin without a new logical commit and
select any subset of physical source generations for relocation. Every clean
page from a selected generation is relocated: `RowPageRewriteControls::selects`
depends only on that generation's immutable occupancy, and
`RootWriter::write_base_descriptor` applies it to every surviving descriptor.
An exceeded rewrite budget fails preparation instead of publishing a partially
relocated generation. `RelocationSelectsWholeGenerations` checks this contract.
The choice still overapproximates occupancy-based selection: an empty subset
models ordinary base reuse, and every eligible whole-generation choice remains
possible. Independent choices for two clean pages in the same generation are
not implementation behaviors and are not modeled. Numeric occupancy, encoded
file lengths, cancellation, and memory limits are checked by implementation
tests, not this model.
Released candidates reset their root to a canonical absent value. Every action
that consumes the candidate root requires a non-idle phase, and BeginCheckpoint
overwrites it before reuse. This models PreparedCheckpoint destruction and
removes unobservable stale object contents from the state space; it does not
restrict readers, epochs, generations, relocation choices, or liveness.

The model also normalizes retired payloads instead of retaining the contents of
deleted objects indefinitely. `Reclaim` keeps every required root and physical
page unchanged. Only root contents outside the published set and epochs outside
the durable sets become zero; `-1` remains the distinct never-written marker.
Thus the immutable-identity guards still reject reuse of a previously written
page, including one that was reclaimed. Reclamation requires an idle candidate,
so no in-flight relocation can lose its source epoch or base-root contents.
The model checks `RetiredPayloadIsReleased` and
`RetiredRootMetadataIsReleased` alongside the existing closure invariants.

Checkpoint publication also releases per-epoch dirty-page bookkeeping at or
below the selected manifest epoch. Every subsequent `DirtyAfter` call starts
at that epoch or later: the manifest never moves backward, and a candidate
captured before a competing publication cannot pass its base-generation fence.
In-flight candidates retain their own captured dirty set. This abstracts dead
bookkeeping, not a new physical WAL truncation transition or a weakened recovery
suffix. `CheckpointedWalMetadataIsReleased` checks the normalization. No reader,
commit, checkpoint, relocation, crash, or rejection transition is removed, and
all prior invariants and the stale-candidate liveness property remain enabled.

Physical page references use the dense address `generation * MaxPage + page`.
`RefGeneration` and `RefPage` invert it; a model assumption checks the bijection
over the entire configured domain. This changes the representation of the
page-epoch function to an interval-indexed table, avoiding repeated record-key
construction and lookup in TLC. It does not quotient or remove states, change
physical identity, or weaken transitions or properties. The complete configured
record-key and dense-key checks each generated 138,204,089 states and found
23,127,068 distinct states at depth 37, including successful liveness checking.
The fresh-epoch, stale-publication, lost-WAL-suffix and partial-generation mutants
still violate their respective invariants with the dense representation.

Stale-candidate rejection uses `WF_candidatePhase(RejectStaleCandidate)`.
The action requires a non-idle phase and sets the next phase to idle, so it
implies both `candidatePhase' # candidatePhase` and `vars' # vars`.
Consequently, `<<RejectStaleCandidate>>_candidatePhase` and
`<<RejectStaleCandidate>>_vars` each equal `RejectStaleCandidate`, including
their enabled predicates. Their weak fairness formulas are therefore equivalent;
this is not an additional progress assumption or a change to the liveness
property. TLC's [liveness action evaluation](https://github.com/tlaplus/tlaplus/blob/5a47802/tlatools/org.lamport.tlatools/src/tlc2/tool/liveness/LNAction.java#L57)
evaluates the fairness subscript before the action body, so the scalar avoids
constructing and comparing the complete state tuple on every edge. The state
graph, bounds, transitions and checked properties are unchanged.
The complete scalar-subscript check retains the same 138,204,089 generated
states, 23,127,068 distinct states and depth 37, including successful liveness
checking. A disabled-rejection mutant still produces an infinite stale-candidate
counterexample; the fairness formula does not assume eventual termination.

`RelationalRowPagePublicationReport.events` maps the canonical runtime sequence
`CandidateStarted`, `CandidatePagesDurable`, `CandidateRootDurable`,
`CandidateManifestDurable`, `BaseRevalidated`, and
`CanonicalSelectionDeferred` to `BeginCheckpoint`, `PersistCandidatePages`,
`PersistCandidateRoot`, `PersistCandidateManifest`, the generation fence, and
the pre-publication state before `PublishCheckpoint`.
A generation-manifest rename failure refines the state in which page/root
artifacts may be durable while the candidate manifest and outer selector are
not: the generation remains unopenable and the previous canonical root remains
selected. The filesystem-specific obstruction test adds implementation evidence
only; it does not introduce a new protocol transition.
`RelationalRowPagePublisher::persist_generation` leaves the immutable candidate
unselected; `SKEIN_MANIFEST_V1` then binds the exact row and overflow
generations atomically. Recovery opens those exact generations rather than an
independent latest selector. Physical page demand reads and serving activation
remain blocked on their separate implementation and regression evidence.

## Production Content Store Evidence Refinement

The read-only and mutation production runners and the final release-bundle
evaluator are observers over existing storage transitions; they do not add a
new publication, locking, or recovery action. Their formal boundary is the
composition of `SkeinCowPagePublication.tla`, `SkeinTransactionConcurrency.tla`,
`SkeinTransactionIndexOverlay.tla`, `SkeinStorageDurability.tla`, and
`SkeinWalGroupCommit.tla`.

The concrete read-only report refines a pinned current row/index generation:
the observed base generation, visible epoch, recovery delta, live overlay, and
cache pins must remain aligned across opens and cold/warm reads. The mutation
matrix refines one durable commit transition per retained operation, followed
by two distinct recovery observations. The first observation requires a
non-empty WAL suffix to reconstruct both row and index recovery deltas; the
second follows checkpoint publication and requires manifest selection with no
WAL replay and no remaining live or recovery delta. Verification digests must
remain identical across both observations.

`release_bundle::content_store` rechecks these refinement obligations from raw
evidence and rejects missing writer cases, inconsistent latency aggregates,
invalid WAL accounting, stale epochs, and contradictory recovery views even
when a child artifact reports readiness. Its tests are implementation-level
refinement checks; the Bazel `storage_models` suite remains the authoritative
bounded state-machine gate.

## Content Source Replacement

`SkeinContentSourceReplacement.tla` models the Content Store operation that
replaces every chunk owned by one Source. The transaction starts by deleting
the old relational set in its private workspace, admits only requested chunk
identities, and publishes the graph chunk count, relational rows, and document
item count through one durability decision. A shorter target therefore cannot
retain a stale suffix, while an empty target publishes zero rows and counts.

The duplicate-order action represents the unique
`(content_doc_id, chunk_index)` constraint. Rejection preserves the last
accepted workspace and does not abort the surrounding transaction. Commit
visibility follows the durable history, and crash recovery selects the complete
durable replacement or the preceding canonical state. With three chunk
identities and three commit epochs, TLC explores 6,644 distinct states while
checking exact replacement, graph/document count agreement, statement
atomicity, empty replacement, and durable-before-visible publication.

## Content Source Ownership Move

`SkeinContentSourceOwnershipMove.tla` models the Content Store operation that
moves one Source to another workspace without rewriting its chunks. The graph
Source update is staged before the relational content-document update, but the
canonical view can publish only after both values agree and the complete mixed
transaction is durable. Chunk count and payload identity remain invariant
through staging, rollback, publication, and crash recovery.

The missing-Source action snapshots the canonical ownership and commit epoch,
then proves that the no-op changes neither. Crash recovery selects either the
previous complete owner or the complete durable move; it cannot expose one
side of the ownership pair. With two spaces, two chunks, and three commit
epochs, TLC explores 32 distinct states while checking graph/document owner
agreement, content preservation, durable-before-visible publication, rollback,
and missing-owner epoch stability.

## Content Thread Ownership Move

`SkeinContentThreadOwnershipMove.tla` models a bounded batch of Thread
workspace changes whose entries can originate in different spaces. Each owner
is processed independently against its previewed source-space guard, but its
graph Thread, relational document, and message set are changed together in the
transaction workspace. The complete batch becomes canonical only after one
durability decision.

The concrete refinement keeps identity domains explicit: Cypher addresses the
graph Thread by public Thread id, while relational document ownership and
message lookup use the distinct thread storage id. The qualification fixtures
make those values unequal and assert the relational `owner_id` returned by SQL.

The configured batch moves owners `a` and `b` from different spaces to `work`.
The `stale` owner has a mismatched preview and therefore remains unchanged in
all three representations. Payload identity is immutable across partial
staging, rollback, durable publication, and crash recovery. With three owners
and three commit epochs, TLC explores 31 distinct states while checking guarded
workspace derivation, graph/document/message agreement, stale-preview
preservation, payload preservation, and durable-before-visible publication.

## Content Space Merge Ownership

`SkeinContentSpaceMergeOwnership.tla` models the cross-kind ownership batch
used by a Space merge. The selected set contains a Thread, a Source, and a
stale Thread selection. Graph ownership, relational document ownership, and
the dependent message or chunk read lane are staged per owner under one shared
source-space guard. The canonical state changes only after the complete mixed
batch is durable.

The eligible Thread and Source move from `source` to `target`; the stale owner
remains in `stale-current`. Payload identity is invariant through arbitrary
owner staging order, rollback, durable publication, and crash recovery. With
three owners and three commit epochs, TLC explores 31 distinct states while
checking cross-kind ownership agreement, guard behavior, stale-selection
preservation, payload preservation, and durable-before-visible publication.

## Content Thread Message Upsert

`SkeinContentThreadUpsert.tla` models the initial Thread content write as one
mixed graph and relational transaction. The graph Thread, storage-owned
document, two message occurrences, and exact document summary become canonical
only after the complete workspace is durable. Public Thread identity and
relational storage identity remain distinct refinement domains.

The model also exercises an UPSERT conflict for the second occurrence and a
missing-document foreign-key rejection. The conflict may replace mutable
payload but cannot change creation identity; the rejected statement cannot
change the last accepted workspace. Crash recovery selects either the previous
complete state or the complete durable upsert, never a partial graph,
document, message, or summary publication. With two message occurrences and
two commit epochs, TLC explores 75 distinct states.

## Content Thread Message Reconcile

`SkeinContentThreadReconcile.tla` models occurrence-preserving Thread message
reconciliation over two existing occurrences and one inserted occurrence. An
invalid mapping is rejected while the canonical state and commit epoch remain
unchanged. A valid mapping may stage graph/document updates, message orders,
the explicit occurrence anchor, the legacy message-and-old-order anchor, the
new occurrence, and the exact summary in arbitrary order, but only the complete
target may become durable and visible.

The model keeps preserved message and anchor payload identity immutable in both
canonical and transaction workspace state. Published anchors follow the exact
occurrence order before and after reconciliation, and crash recovery selects
either the initial complete state or the durable complete target. The concrete
Rust refinement additionally checks bounded occurrence discovery, duplicate,
incomplete, and unknown mappings, public/storage identity separation, exact
payload digests, live overlay visibility, and checkpoint/reopen identity.

## Content Thread Tail Delete

`SkeinContentThreadTailDelete.tla` models deletion of an exact ordered Thread
tail. The candidate set contains the two occurrences at or beyond the start;
graph count, message set, message-anchor set, and document summary may stage in
any order, but only the complete target may become durable or visible. A
non-message anchor and every retained payload remain unchanged.

The model also records an empty-tail operation as an epoch-preserving no-op,
allows rollback from every partial workspace, and recovers from the durable
history. TLC checks exact candidate identity, graph/document/message count
agreement, surviving anchor validity, retained payload immutability, no partial
canonical publication, and durability-before-visibility. The Rust refinement
adds negative-start clamping, bounded ordered candidate rows, direct row
tombstone evidence, exact byte summary, and checkpoint/reopen digests.

## Content Thread Delete

`SkeinContentThreadDelete.tla` models whole-Thread deletion across the graph
Thread, graph Message nodes, ThreadIdentity nodes, relational message
occurrences, anchors, and the exact union of owned and message-referenced
documents. The owned-document fixture includes an empty owner while the legacy
fixture is message-only, so exact document discovery is an explicit invariant
rather than an assumption derived from one relation.

Each graph and relational component may stage independently, but only the
complete deletion may become durable or visible. Rollback and crash recovery
select a complete old or new state and unrelated payload identity never
changes. Missing and repeated deletes are read-only preflight actions whose
canonical state and commit epoch remain unchanged. The Rust refinement adds
bounded statement budgets, transaction-workspace read-your-own-writes, exact
delete counts, live tombstone evidence, and checkpoint/reopen digests.

## Relational Overflow Publication

`SkeinOverflowPublication.tla` models the generation-bound overflow root used
before a row generation may contain large-value references. A candidate writes
only content digests absent from the selected base root, while reachable shared
digests retain their immutable physical generation and unreachable base
digests are omitted from the candidate root. Extent, descriptor-root, and
generation-manifest artifacts become durable before the latest manifest is
replaced. A competing publisher makes the candidate stale; weak fairness of
the rejection action guarantees that the stale candidate terminates without
changing the selected root.

Readers pin complete immutable roots and their cross-generation physical
extent closure. A row-root binding can name only a published overflow root
whose durable closure is complete. The configured instance explores two
digests, two readers, and three generations. TLC checks manifest-last
visibility, content-addressed reuse, fresh placement for new content, stale
fencing, pinned-root readability, exact row binding, physical-closure
reclamation, and crash removal of volatile candidates.

The runtime refinement is `RelationalOverflowPublisher` and
`RelationalOverflowRootReader`. The publisher writes a fixed-width sorted
descriptor root, publishes the immutable generation manifest, revalidates the
base, and either replaces the standalone latest manifest or returns an
unselected canonical candidate. `RelationalRowPagePublisher` resolves every
reference before candidate creation and persists the exact overflow root
binding in `SKRPGM01`. The outer checkpoint manifest selects both roots, while
`DurableStore::reclaim_old_generations` preserves the physical closure of the
current and previous roots.

`SkeinOverflowExactCompaction.tla` models the separately admitted metadata-only
maintenance path. A source-epoch scan may be rejected before artifacts, become
stale while work is in progress, or admit the exact visible closure. An
admitted operation resolves newly introduced live envelopes, copies reachable
base envelopes without hydration, rewrites both into its fresh physical
generation, persists the exact root and generation manifest, and becomes
visible only through the outer checkpoint manifest. Cancellation, crash, and
stale-source cleanup discard an unpublished candidate. Reclamation preserves
current, previous, and reader-pinned physical generations, while older
unpinned generations may disappear.

The runtime refinement is
`Database::compact_relational_overflow`,
`GraphStore::collect_exact_relational_overflow_closure`,
`RelationalOverflowReferenceSetBuilder`, and
`RelationalOverflowPublisher::persist_generation_exact_references`. The Rust
path adds hard row/page/read/overlay/sort/spill/rewrite admission, cancellation,
checksummed external-sort runs, zero-hydration scanning, exact base-envelope
copy verification, and publication cleanup. The integration test
`exact_overflow_compaction_rewrites_reachable_closure_without_hydration`
checks rejection before selection, cancellation, fresh physical placement,
pinned-reader visibility, later generation reclamation, and reopen identity.
The observational production refinement is
`run_production_content_store_overflow_compaction_qualification`: it runs the
same transition on a caller-owned disposable replica, records its bounded
resource evidence, advances a later generation through parameterized Cypher,
scrubs the selected closure, and proves SQL digest identity after reopen. It
does not add a durable transition to the model. The test
`qualification_proves_exact_rewrite_physical_reclaim_and_reopen` binds that
collector to the modeled exact rewrite, reclamation, and recovery sequence.

## Relational Row-Page Mutation

`SkeinRowPageMutation.tla` models a persistent table-scoped allocator and the
deterministic COW mutation constructed over one pinned base root. An affected
leaf preserves its old logical id for the first output page, allocates only
right-side split pages, removes an empty leaf, and never recycles a deleted id.
The base root, rows, and allocator remain immutable for pinned readers.

Point mutation reads at most one base leaf per operation. Empty-table bootstrap
reads no base page and buffers no more than one admitted page plus one candidate
row. The configured instance has four logical ids, page capacity two, three
keys, and six scenarios: insert with split, update, delete, delete followed by
a split, no-op, and streaming bootstrap. TLC checks root-id uniqueness,
allocator monotonicity, active-id allocation, non-reuse after deletion, exact
mutation rows, dirty/deleted set soundness, split identity, bounded reads,
bounded bootstrap residency, and pinned-base stability. This is a small
exhaustive protocol model; codec byte limits, filesystem publication, and
arbitrary key cardinality remain Rust refinement obligations.

## Disk-Backed Relational Row Recovery

`SkeinRowRecovery.tla` models the runtime integration between one
checkpoint-pinned relational row root, complete WAL replay, immutable row-delta
runs, and the published read view. A valid mount checks generation, source
epoch, and table schemas without opening a descriptor or row-page slot.
Missing, stale, corrupt, schema-invalidated, and over-budget views become
unavailable without changing canonical checkpoint plus WAL recovery.

Every global WAL epoch and same-epoch relational fragment is consumed in
order. The dirty map is bounded. When the next admitted fragment would exceed
that map, the current map becomes one complete immutable run and replay
continues; run-budget exhaustion fails closed. After the complete prefix has
been consumed, the final dirty map becomes a run, the generation manifest is
made durable, and only then may the latest selector expose the complete
base-plus-delta state. A crash before selector publication resets recovery and
leaves the candidate unreachable.

The configured instance uses two keys, two post-checkpoint epochs, one- or
two-fragment row commits, multi-key fragments, a one-entry dirty budget, and a
two-run budget. TLC covers repeated-key replacement, same-epoch fragment
ordering, graph-only advancement, dirty flush, whole-fragment and run-capacity
rejection, schema invalidation, missing/stale/corrupt roots, manifest-last
visibility, candidate crash, a newer row root, pinned-reader stability, and
cold mount. SQL selection is intentionally false inside this recovery-only
model; the composed serving transition is proved by
`SkeinRelationalRowSnapshotRead.tla` and bound to the Rust SQL runtime below.

## Immutable Relational Row Live Views

`SkeinRowLiveView.tla` models publication after a complete recovery view has
been pinned. DML and DDL stage their canonical state and prospective row view
before WAL. A graph-only commit makes its WAL durable first and then stages the
identity-only view advance. WAL durability may lead visible state by one
commit, but canonical state and the current row view advance only after that
durable record exists. DML adds one bounded immutable batch, graph-only commits
advance the global epoch without a batch, and DDL or admission failure makes
the non-authoritative view unavailable.

The model includes a canonical transaction-workspace read, view poisoning, a
process crash before or after WAL durability, and one pinned reader. TLC checks
WAL-before-visibility, exact current-view epoch and contents, cumulative live
admission, fail-closed invalidation, read-your-own-writes independence,
pinned-reader stability, and the still-disabled SQL selection boundary. The
configured instance uses two keys, three post-base epochs, and a two-entry live
budget.

## Authoritative Transaction Index Overlays

`SkeinTransactionIndexOverlay.tla` models one multi-statement authoritative SQL
transaction. Begin pins the committed row and index state. Each abstract
`StageStatement` transition applies one indexed-key change to both private
immutable overlays while consuming independently chosen row and index entry
costs and one synchronized statement version. An exhausted entry or statement
budget rejects the next transition without changing either last accepted
workspace. Reads observe the combined private workspace, not the pinned base
alone. Prepare, durability, publication, rollback, and crash
recovery preserve WAL-before-visibility.

The configured instance uses two keys, three commit epochs, and a two-entry
private overlay. TLC checks the pinned base, row/index workspace and overlay
version agreement, bounded admission, rejected-statement atomicity,
read-your-own-writes, and
durable-before-visible publication. It also enumerates every base visit order
and proves that exact per-locator tombstone consumption plus remaining inserts
returns the current set; a composite-prefix reader therefore does not assume
that callbacks are globally ordered by primary key. A concrete SQL statement
may emit multiple index changes. Entry/byte accounting and commit-time
revalidation after concurrent base advancement remain concrete Rust and
`SkeinIndexPublication.tla` refinement obligations.

## Immutable Relational Row-Delta Runs

`SkeinRowDeltaRuns.tla` models the non-serving disk-backed generation written
by `RelationalRowDeltaBuilder`. WAL-equivalent primary-key changes coalesce in
a bounded dirty map and flush into complete immutable run sets. The generation
manifest becomes durable only after the run set and overflow-reference closure
are complete; the latest manifest changes only after revalidating both the
selected row root and expected previous delta generation.

The model includes a competing delta publisher, a concurrent row-root
publication, missing overflow closure, partial-batch poisoning, process crash,
and a generation-pinned reader. TLC checks that the dirty overlay remains
bounded, replay equals the durable logical prefix, manifests select only
complete run sets, every published and pinned manifest row count equals its
logical state, stale or poisoned candidates remain unreachable, published state
is a complete epoch prefix, every candidate/published/pinned recovery-source
identity denotes that same WAL prefix, and a pinned reader does not drift. SQL
selection is intentionally outside this immutable-run model; the composed
snapshot model and Rust refinement activate the selected generation.

The configured instance uses two keys, a one-entry dirty budget, two
post-checkpoint epochs, four delta generations, and two row-root generations.
The Rust refinement provides the byte-level limits, exact CRC32C/SHA-256
bindings, manifest-last filesystem operations, row-root/delta lock ordering,
and corruption rejection outside the abstract model. `SkeinRowRecovery.tla`
separately proves how complete WAL replay selects that artifact before the live
view may advance.

## Relational Row Snapshot Composition

`SkeinRelationalRowSnapshotRead.tla` models the logical newest-version overlay,
pinned authority, overflow resolution, and serving lifecycle. Its `overlay`
variable is a ghost oracle rather than a resident implementation map.
`SkeinRelationalOverlayStreamingMerge.tla` owns the physical refinement: one
projected head per intersecting recovery run or live batch, newest-epoch
coalescing at the minimum-key frontier, tombstone suppression, source and
distinct-entry admission, a bounded recovery-run file pool, and a peak envelope of the source heads plus one
selected/working head. It proves that early and complete result prefixes stay
ordered and match the same pinned oracle while a newer current view cannot move
the reader. The prepared state retains no run files or decoded heads; lazy
recovery priming opens the admitted sources only when iteration begins,
validates every encoded field while decoding only
the requested field set into owned head values; the model checks both the
projection subset and full-row validation obligations. An overlay overflow reference becomes visible only after resolution
through the state pinned at that same epoch. The implementation publishes the
resolved row and its hydration counters atomically; resolution admission may
fail without poisoning the reader.

The model also distinguishes a ready immutable reader, a database before its
first checkpoint, and a lost serving fence. Only the pre-checkpoint `missing`
state may read the canonical in-memory rows. An unavailable reader rejects the
read as corruption, poisons the SQL database handle, and can never enter
collection or streaming. `RelationalRowRuntime` pins this identity once per
statement, applies one cumulative page/row/byte/overlay/hydration ledger, and
uses a separate scan-field plan so blocking locator pipelines hydrate projected
large values only after the final row is selected.

A schema-changing commit transitions a ready row view to `schemaRequired`.
The model records the schema WAL group becoming durable before a writable
handle may enter `checkpointing`; only manifest-last publication can then
advance the canonical view to `readyAfterSchema`. A crash before WAL durability
keeps the previous ready view, while a crash after durability but before the
manifest returns to `schemaRequired` for retry. A read-only handle may only
reject the reader. `SchemaCheckpointWaitsForDurableWal` and
`SchemaCheckpointPublishesBeforeServing` prevent either durability inversion or
intermediate-state SQL serving.

TLC covers bounded and rejected logical overlays plus streaming merge-source
and peak-buffer admission, live-over-recovery
precedence, recovery/live insertions, replacement accounting, tombstones,
ordered completion, pinned-state overflow resolution and rejection, callback
stop, cancellation, callback panic, corruption poisoning, and a later current
view. Admission, cancellation, and panic remain non-poisoning. The configured
instance explores three keys, two immutable overlay layers, three entry slots,
and eight conservative byte units.

## Sparse Read-Only Relational Recovery

`SkeinSparseRelationalRecovery.tla` composes the source-fenced row and index
artifact obligations for a read-only `OutOfCore` plus `Authoritative` open. It
mounts only canonical schema/count metadata, validates a non-empty WAL source,
and may enter `serving` only when schema is stable, both artifacts exist, both
source identities equal the validated WAL sequence, and the recovered epoch
equals that sequence length. The serving logical row count must come from the
row artifact. Missing or mismatched artifacts and schema-changing WAL enter a
terminal rejected state.

The model enumerates ordered one- and two-record sources, absent and unrelated
row/index sources, availability combinations, stable and changing schemas,
recovered epochs, and final row counts. TLC checks that the sparse path never
materializes checkpoint rows, never publishes artifacts from a read-only
handle, never serves before WAL validation, and never accepts a partially
matching artifact pair. Rust refines the opaque source sequence to WAL
generation, inclusive/exclusive LSN bounds, and the domain-separated ordered
record SHA-256.

## Sparse Writable Relational Recovery Workspace

`SkeinSparseWritableRelationalRecovery.tla` models the per-record writable
workspace and its staged-delta-before-checkpoint hydration rule separately from
production-open activation. A staged present row overrides the immutable
checkpoint and a staged tombstone masks it. The caller must supply exactly the
authenticated access keys plus the correct present/missing shape, and the
workspace is admitted only below its hard entry bound. The staged-delta and
checkpoint source sets are disjoint and together cover the complete access
set; Rust refines those abstract read units into one cumulative per-record byte
bound. Replay changes only the local workspace and the logical relation, exact
row count publication occurs atomically, and the workspace is discarded before
a later record. Missing, drifted, or oversized hydration enters a terminal
rejected state without ever materializing the complete checkpoint.

The model enumerates every checkpoint, staged-present, staged-deleted, access,
supplied-key, supplied-row, insert/delete/no-op, and target combination for two
keys. TLC checks exact hydration, delta-over-checkpoint precedence, bounded
residency, exact committed counts, workspace disposal, and fail-closed rejection.
Rust refines the abstract bound to both entry and conservative resident-byte
limits and refines staged lookup to checksummed immutable runs plus a bounded
dirty map.

## Sparse Relational Live Staging Workspace

`SkeinSparseRelationalLiveStage.tla` models schema-stable live DML after a
metadata-only open. Its supplied workspace contains every actual replay-access
key and may additionally contain unchanged rows needed only for authoritative
unique or foreign-key validation. The model admits only bounded workspaces,
requires exact present/missing hydration from the logical base, preserves every
constraint-support row across staging, publishes the exact logical row count,
and discards the workspace after commit. Missing replay coverage, incorrect
hydration, or oversized input rejects before commit. Rust refines the abstract
support set to rows discovered from one generation-pinned constraint index and
charges both hydrated rows and the retained explicit-key ledger to the same
conservative resident-byte limit.

## Sparse Relational Live Preparation

`SkeinSparseRelationalLivePreparation.tla` models the closure loop before live
staging. A schema-derived initial plan seeds the supplied workspace;
unpublished DML preparation may discover additional exact replay keys and
authoritative constraint-support keys. Only a workspace that contains all
three sets may become closed. Missing keys are admitted one at a time under the
same entry bound, while an uncloseable workspace rejects. Preparation never
publishes canonical state or WAL. Rust refines initial keys to primary-key
point reads, predicate-table range scans, and UPSERT conflict probes, and
refines constraint keys to unique, foreign-key-target, and parent-delete
referrer probes derived from exact row/index change captures.

## Sparse Relational Live Commit Publication

`SkeinSparseRelationalLiveCommit.tla` models the `GraphStore` boundary after
candidate discovery. Exact hydration must close under one hard workspace bound
before canonical state and row/index live views may be staged. Those candidates
remain invisible until WAL is durable, after which all three visible epochs
advance atomically. Any failure before WAL discards the candidates and preserves
the old database. A crash after WAL may discard unpublished in-memory
candidates, but recovery publishes the same durable epoch. Rust refines the
abstract workspace bound to cumulative decoded-row, page, artifact-byte,
overlay-entry, overlay-resident-byte, overflow-hydration, and conservative
workspace entry/resident-byte budgets held by one generation-pinned row reader
and one transaction-scoped authoritative index ledger.

## Sparse Relational Production Activation

`SkeinSparseRelationalActivation.tla` models the `OutOfCore` plus
`Authoritative` selector. The checkpoint mounts metadata-only relation state.
Row and index recovery may finish in either order, but the handle cannot become
active until both immutable views equal the final database epoch. Missing or
mismatched views reject the open. A clean or WAL-recovered active handle then
advances state and both views by exactly one epoch on its first sparse live
commit without rematerializing checkpoint rows. Rust refines the two abstract
recovery steps to source-bound row/index recovery artifacts or writable sparse
replay builders and validates both serving views in
`GraphStore::activate_out_of_core_relational_rows`.

## Relational WAL Replay Access

`SkeinRelationalWalReplayAccess.tla` models the bounded primary-key working set
captured before a schema-stable relational DML record becomes durable. An
authenticated WAL record carries the exact union of predicate-read and changed
keys, including predicate non-matches; an oversized set rejects before
durability. Recovery may hydrate only that set and may apply the transaction
only after authentication, exact-set comparison, and the entry bound all
succeed. Tampered, incomplete, or semantically drifted sets reject without
applying the record.

The model enumerates every subset of three keys with a two-key admission limit,
including the oversized rejection and authenticated/tampered recovery paths.
Rust refines the abstract set to strictly ordered `(table, primary_key)` entries
inside the checksummed relational WAL envelope. Writable metadata-only recovery
uses the authenticated set as its complete hydration domain, recomputes the
actual sparse replay access, and rejects any mismatch before accepting the
transaction.

## Durable Projection Cursor and Catch-up

`SkeinProjectionDurability.tla` models the durable projection framework of
[`../specs/ROW_PAGE_AND_DEMAND_PAGED_INDEX_SPEC.md`](../specs/ROW_PAGE_AND_DEMAND_PAGED_INDEX_SPEC.md)
under "Derived projections". A projection's only durable incremental progress state is the cursor
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
`ReadyImpliesCatchUpCoverage`. Generation-diff catch-up is deliberately not
modeled; the model treats a below-floor cursor as
requiring rebuild, which over-approximates the implementation conservatively.

## Owner-scoped Projection Generation Replacement

`SkeinProjectionGenerationReplacement.tla` models the publish-last owner head
defined by
[`../specs/PROJECTION_GENERATION_REPLACEMENT_SPEC.md`](../specs/PROJECTION_GENERATION_REPLACEMENT_SPEC.md).
A candidate artifact remains invisible while staging and after sealing. The
owner head can select it only after the complete artifact is sealed and only
when the publisher's expected head still matches. Publication therefore
changes visibility from one complete immutable generation to another without
an in-place delete phase or an unbounded keep-ID set.

Readers pin the selected generation. Incremental reclamation can remove an
inactive artifact only when no modeled reader pins it and it is not the active
writer candidate. Crash recovery clears volatile writer and reader state but
retains the last durable owner head and all durable artifacts, including an
abandoned candidate that may be resumed or reclaimed. The model checks that
the active generation is always sealed, staged candidates stay invisible,
pinned readers remain readable, and a sealed writer always names a complete
artifact.

## Derived Columnar Visibility and Compaction Identity

`SkeinCompactionVisibility.tla` models a derived column-group read path
governed by
[`../specs/ROW_PAGE_AND_DEMAND_PAGED_INDEX_SPEC.md`](../specs/ROW_PAGE_AND_DEMAND_PAGED_INDEX_SPEC.md):
base groups filtered by generation-scoped deletion vectors, delta
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

## Derived Layered Column-Group Manifest Publication

`SkeinColumnGroupManifest.tla` models the publication protocol in
[`../specs/ROW_PAGE_AND_DEMAND_PAGED_INDEX_SPEC.md`](../specs/ROW_PAGE_AND_DEMAND_PAGED_INDEX_SPEC.md).
Newly changed immutable artifacts become durable before their per-table
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

## Runtime Memory Admission

`SkeinRuntimeAdmission.tla` models the governor's split between memory
capacity and the dynamic budget beneath it. Capacity is independent of
current headroom, but resource refresh may change it when sensed host or
cgroup policy ceilings change. The model includes zero capacity, dynamic capacity
shrink, and temporary overcommit caused by preserving active permits.
Submission runs the static capacity check before any dynamic gate. A request
within capacity but above the uncommitted budget waits; if refreshed capacity
later falls below that request, its next retry terminates non-retryably.

The model checks that budget never exceeds capacity, over-capacity submission
never enters waiting, terminal rejection occurs only against the capacity
current at that transition, and an over-capacity waiter has a terminating
retry. Refresh may lower capacity beneath existing reservations, so the model
does not assert the false global invariant that admitted bytes always fit the
latest capacity. Instead it proves that admission never creates capacity
overcommit and cannot grow overcommit created by refresh.

Mutation testing sizes the instance (`MaxCapacity = 3`, two waiters): skipping
the static capacity check reports `SubmissionNeverWaitsAboveCapacity`, letting
a refresh raise budget above capacity reports `TypeOK` alongside
`BudgetNeverExceedsCapacity`, failing to terminate a waiter after capacity
shrink reports `OverCapacityWaiterIsRejectable`, and admission that ignores
the uncommitted budget reports `AdmissionNeverCreatesCapacityOvercommit` or
`CapacityOvercommitNeverGrows`. Scheduler liveness remains implementation
evidence in the runtime-tokio timing tests.

`qualify_content_store_memory_profile` is a refinement check at one resource
snapshot. It requires the detected effective limit for the desktop 8 GiB
profile, applies the explicit 512 MiB ceiling for the low-memory capability
profile, recomputes the limit- and headroom-derived capacity/budget, and rejects
any report where the budget exceeds capacity. The TLA+ model proves that this
capacity/budget ordering remains safe across later resource refreshes, waits,
admissions, releases, and capacity shrink.
`run_production_content_store_memory_qualification` evaluates both refinements
from one immutable resource snapshot and binds them to one exact release
identity. The CLI parser only supplies that identity and an existing path for
device classification; it introduces no resource transition. Consequently the
existing `SkeinRuntimeAdmission.tla` safety and liveness obligations apply to
both derived configurations independently.

`SkeinMemoryTierRelease.tla` models the separate release-evidence boundary. A
ready release requires current-identity desktop-policy evidence, explicit
512 MiB policy evidence, a representative production read, and a constrained
512 MiB read, plus separate dynamic-desktop and explicit-512 MiB exact
overflow-compaction reports. Advancing the release identity clears readiness while retaining
the old evidence as stale input, so neither an incomplete matrix nor a workload
from the previous generation can publish readiness. The Rust refinement
independently recomputes both policy reports from raw fields and rejects using
the 512 MiB run as the normal production read or using a configured production
read as capability evidence. It also recomputes scan/spill/rewrite, generation,
resource, physical-reclamation, and scrub fields from both compaction reports;
neither memory profile can substitute for the other.

## Columnar Shadow Checkpoint Integration

`SkeinColumnarShadowIntegration.tla` models the shadow adoption phase of
[`../specs/ROW_PAGE_AND_DEMAND_PAGED_INDEX_SPEC.md`](../specs/ROW_PAGE_AND_DEMAND_PAGED_INDEX_SPEC.md)
under "Derived projections" as a four-phase checkpoint machine — publish the canonical manifest,
publish the shadow key dictionary, publish the shadow manifest, update the
in-memory shadow catalog — with a crash enabled at every boundary and a
reader pinned to the canonical side. The shadow is derived, rebuildable
state: recovery mounts an intact shadow whose source epoch matches the
recovered canonical epoch, keeps an intact stale shadow only as the reuse
parent behind an all-dirty rebuild, and discards a corrupt shadow (the
projected-graph policy) instead of failing the open. A shadow build or
publication failure after the canonical manifest replaced returns success
from the checkpoint call and preserves the dirty state for the retry.
After a successful publish, a bounded best-effort sweep reclaims artifact
files outside the active manifest's reference closure; partial removal
models recorded-and-retried failures, and a crash between publish and
sweep leaves only unreferenced garbage for the next sweep.

The model checks that recovery never fails because of shadow state, that a
mounted catalog always binds an intact published shadow at its own epoch,
that a shadow-behind-canonical gap at rest always stands behind the
all-dirty flag or full dirty coverage, that a checkpoint whose shadow
published leaves the shadow exactly at the canonical epoch, that the
shadow manifest never leads the canonical epoch and always has dictionary
coverage, that the checkpoint result tracks canonical publication only,
that a reader only ever pins published canonical epochs, and that a sweep
never removes a file the active shadow manifest references
(`ActiveClosureRetained` — the shadow has no reader pins, so the closure
is the only retention obligation).

Resource admission stays out of this model deliberately: nested admission
is absent structurally — the shadow builder receives a pre-admitted
`ColumnarShadowAdmission` context by value and holds no governor handle —
and the constrained-governor convergence test
(`pre_admitted_shadow_converges_under_a_constrained_governor`) proves it;
admission semantics are modeled separately by `SkeinRuntimeAdmission.tla`
below.

Mutation testing sizes the instance (`MaxEpoch = 3`, 3,657 distinct
states): a recovery that mounts a corrupt shadow as current reports
`NoStaleShadowMount`, a shadow failure that fails the checkpoint call
although the canonical manifest replaced reports
`CheckpointResultTracksCanonicalOnly`, a recovery that skips the all-dirty
rebuild after an epoch gap reports `StaleShadowGapIsCovered`, a recovery
that fails closed on a corrupt shadow reports
`CanonicalRecoveryIndependentOfShadow`, and a sweep that retains only the
current generation's files instead of the reference closure reports
`ActiveClosureRetained`.

## Implementation Refinement Evidence

The Rust tests below exercise the concrete boundaries represented by the model.
They are implementation evidence, not a machine-checked refinement proof.

| Protocol obligation | Implementation boundary | Regression evidence |
| --- | --- | --- |
| WAL sync precedes visibility and apply failure closes the handle | `finish_wal_append`, `apply_wal_op`, `ensure_usable` | `post_wal_apply_failure_poisons_handle_until_reopen` |
| A grouped WAL sync acknowledges every member after one successful barrier or fails the whole group closed | `WalSyncGroupState`, `finish_wal_sync_group`, `CommitSequencer` | `wal_group_commit_shares_one_sync_without_changing_record_order`, `wal_group_sync_failure_rejects_commit_and_poisons_until_reopen`, `panicking_group_commit_task_completes_followers_and_releases_leader` |
| Fixed and adaptive collection policies remain bounded scheduling refinements, use a bounded fallback without a recent baseline, and never delay a lone request | `effective_group_commit_delay`, `wait_for_group_commit_peers`, `WalGroupCommitConfig::adaptive_enabled_after_evidence` | `wal_group_commit_skips_the_coalescing_window_without_contention`, `adaptive_delay_is_derived_from_the_completed_baseline`, `adaptive_delay_uses_bounded_fallback_before_the_completed_sample_floor`, `adaptive_delay_falls_back_after_the_recent_window_expires`, `wal_group_commit_requires_performance_and_recovery_evidence` |
| A table-scoped row-page allocator persists monotonically, COW point mutations read only affected leaves and preserve the old left id across splits, deleted ids are never reused, and streaming bootstrap stays within one page plus one candidate | `RelationalRowPageIdAllocator`, `RelationalRowPageMutationPlanner`, `RelationalRowPageBootstrap`, `RelationalRowPageRootReader::find_table_page_descriptor` | `mutation_planner_reads_only_affected_leaves_and_splits_deterministically`, `mutation_planner_updates_deletes_and_never_reuses_page_ids`, `streaming_bootstrap_keeps_one_page_plus_one_candidate_row`, `streaming_bootstrap_fails_closed_after_emit_error`, `allocator_exhaustion_is_atomic`, `SkeinRowPageMutation.tla` |
| Canonical checkpoint planning validates the exact pinned base identity, coalesces recovery and live keys within one transient byte envelope, enforces one global dirty-page budget, rewrites only affected leaves, reuses every clean physical descriptor, carries complete digest-bound schemas plus exact descriptor-derived row counts in the generation manifest, and writes no row slot for a graph-only commit | `RelationalRowPageReadView::checkpoint_capture`, `GraphStore::plan_relational_row_page_checkpoint`, `RelationalRowPageMutationPlanner` | `checkpoint_key_capture_coalesces_and_orders_distinct_keys`, `checkpoint_key_capture_rejects_unbounded_transient_state`, `checkpoint_planning_enforces_one_global_dirty_page_budget`, `canonical_checkpoint_rewrites_only_dirty_relational_pages`, `recovered_wal_rows_checkpoint_as_incremental_cow_pages`, `schema_change_rebuilds_the_complete_relational_row_root`, `publish_last_root_round_trips_with_a_concrete_refinement_trace`, `new_table_requires_a_digest_bound_schema_before_artifact_creation`, `SkeinCowPagePublication.tla` |
| Overflow extents are content-addressed, exact reachable roots reuse immutable extents, canonical candidates remain unselected until the outer checkpoint publishes, generation-manifest replace failure cannot select an incomplete root, and reclamation preserves retained physical closure | `RelationalOverflowPublisher::{persist_generation,publish}`, `RelationalOverflowRootReader`, `DurableStore::reclaim_old_generations` | `publish_last_overflow_root_round_trips`, `incremental_root_reuses_content_and_keeps_pinned_generation_readable`, `persisted_overflow_candidate_does_not_change_latest_selection`, `generation_manifest_replace_failure_leaves_the_canonical_overflow_root_unselected`, `canonical_row_overflow_backup_reopen_and_reclaim_follow_physical_closure`, `reclaim_preserves_overflow_extents_referenced_by_retained_roots`, `SkeinOverflowPublication.tla` |
| A canonical row, overflow, or relational-index generation is admitted from one bounded manifest byte image: the outer artifact binding and internal manifest identity are verified before serving without a verify-then-reopen window, while database-scale page payloads remain cold and demand-verified | `RelationalRowPageRootReader::open_bound_generation`, `RelationalOverflowRootReader::open_bound_generation`, `RelationalIndexShadowReader::open_bound_generation_with_cache`, `RelationalIndexRecoveryReader::open_bound_generation_with_cache` | `bound_row_generation_verifies_the_canonical_manifest_image`, `bound_overflow_generation_verifies_the_canonical_manifest_image`, `relational_index_bound_open_verifies_one_canonical_manifest_image`, `corrupt_bound_row_root_fails_database_open`, `SkeinCowPagePublication.tla`, `SkeinOverflowPublication.tla`, `SkeinIndexPublication.tla` |
| Selected graph manifests share one aggregate open admission, reject filesystem growth before unbounded allocation, and verify the durable length, CRC32C, and SHA-256 from the same byte image before decoding. This is an observational refinement of manifest selection and runtime admission; descriptor paging remains a separate obligation. | `GraphManifestOpenBudget`, `read_bound_graph_manifest`, `DurableStore::{open_existing,install_manifest}`, `GraphStore::storage_residency_report` | `graph_manifest_open_budget_is_aggregate_across_published_roots`, `graph_manifest_growth_is_rejected_before_unbounded_read`, `SkeinStorageDurability.tla`, `SkeinRuntimeAdmission.tla` |
| Graph descriptor pages have a bounded, symmetric immutable encoding; parent references bind exact physical identity, range, integrity, source epoch, class, and key range. The canonical adjacency checkpoint streams descriptor references through bounded level runs, durably replaces the adjacency artifact, publishes an immutable page artifact and bounded root, then activates their exact identities through the outer manifest. Root-only reopen reads no page payload. Production prefix scans enforce independent page, byte, descriptor, and height budgets; physical corruption poisons the shared handle while admission does not. Deep scrub bypasses the cache and verifies the complete page/data closure. The formal model separates candidate construction, durable root publication, atomic serving selection, and pinned demand readers. | `ImmutableGraphDescriptorPage::{encode,encode_with_ref,decode,decode_bound}`, `GraphDescriptorTreeBuilder`, `PreparedGraphDescriptorTree::publish`, `GraphDescriptorTreeRootReader::{open_bound}`, `GraphDescriptorTreeDemandReader`, `CanonicalAdjacencyReader::{open_demand_paged,deep_scrub}`, `DurableStore::{write_canonical_adjacency,load_published_canonical_adjacency}` | `leaf_page_round_trips_with_exact_bound_reference`, `streaming_tree_round_trips_with_bounded_residency`, `builder_rejects_non_converging_interior_fanout`, `publication_refuses_to_replace_an_existing_generation_artifact`, `page_artifact_is_durable_before_root_publication`, `bound_open_rejects_root_artifact_or_identity_drift`, `demand_reader_matches_resident_manifest_and_scrubs_without_cache_warming`, `out_of_core_checkpoint_uses_generation_bound_dense_adjacency_and_fails_closed`, `SkeinGraphDescriptorPaging.tla` |
| Canonical segment checkpoints publish an order-preserving descriptor tree after the synchronized data artifact and bind its source epoch, aggregate per-kind counts, and root integrity through the compact canonical manifest selected last. Normal open verifies only the bounded root and page length. Point reads lower-bound seek one descriptor; scans advance through fixed-size descriptor batches with independent page, byte, descriptor, and height admission, and iterators retain one decoded segment. Read reports separate descriptor and data cache/I/O. Deep scrub bypasses caches, hashes both artifacts, decodes every descriptor and record shape, and proves contiguous physical and logical closure. Spill references additionally require the selected same-generation/source-epoch spill reader and an id below its exact value count; backup and storage scrub validate this cross-artifact closure before acceptance. Physical failure sticky-poisons the reader while admission does not. The activation, non-poisoning admission, and pinned demand-reader transitions refine `SkeinGraphDescriptorPaging.tla`; immutable spill-reference closure is checked by the codec and integration regressions. | `CanonicalSegmentDescriptor::{descriptor_tree_key,encode_descriptor_tree_value,decode_descriptor_tree_entry}`, `CanonicalSegmentWriter`, `CanonicalSegmentReader::{get_node_with_report,get_relationship_with_report,scan_nodes_control,scan_relationships_control,deep_scrub}`, `DurableStore::{write_canonical_segments,load_published_canonical_segments,scrub_storage}`, `validate_backup_files` | `canonical_descriptor_demand_reader_is_lazy_and_exhaustively_scrubbed`, `deep_scrub_requires_the_exact_property_spill_closure`, `compact_manifest_residency_is_independent_of_segment_count`, `canonical_segment_admission_does_not_poison_the_reader`, `canonical_scan_streams_more_than_one_descriptor_batch`, `checkpoint_publishes_digest_bound_canonical_segments`, `out_of_core_property_spills_round_trip_and_fail_closed`, `backup_restore_publishes_one_verified_generation`, `SkeinGraphDescriptorPaging.tla` |
| Property-projection equality, range, full-text, composite, and relationship blocks have an order-preserving symmetric descriptor codec and stream into the bounded page-tree state machine. The data artifact is durable before immutable descriptor pages and the root is published last. The selected compact manifest binds that exact root without retaining per-block descriptors. Root-only open keeps page payloads cold; estimates and execution demand-scan ordered prefixes under independent limits, report descriptor costs, and poison on physical failure. Deep scrub, backup, and repair verify the complete data/page/root closure. Atomic activation and pinned demand readers use the same formal transition as canonical adjacency. | `PersistentPropertyProjectionBlockDescriptor::{descriptor_tree_key,encode_descriptor_tree_value,decode_descriptor_tree_entry}`, `PersistentPropertyProjectionWriter::write_fallible`, `PersistentPropertyProjectionReader::{open,scan_equality_candidates,deep_scrub}`, `DurableStore::{write_persistent_property_projection,load_published_property_projection}` | `descriptor_tree_entry_codec_is_order_preserving_and_symmetric`, `external_projection_round_trips_node_composite_and_relationship_candidates`, `deep_scrub_finds_cold_block_corruption_and_poisons_the_reader`, `out_of_core_property_projections_merge_wal_delta_and_fail_closed`, `out_of_core_relationship_property_projection_prunes_and_merges_wal_delta`, `SkeinGraphDescriptorPaging.tla` |
| Property-spill blocks have an order-preserving fixed-width descriptor codec and stream into the same bounded tree builder. The spill data artifact becomes durable before immutable descriptor pages and the root, while the compact outer property manifest binds source epoch, root identity, block count, and value count without a per-block vector. Root-only open keeps page payloads cold. Point hydration uses a lower-bound seek under independent limits, reads one selected block, reports descriptor/data cache and I/O, and sticky-poisons physical failure without poisoning block-size admission. Deep scrub, backup, and storage scrub bypass caches and verify the complete contiguous data/page/root closure. The formal model requires same-generation data durability before descriptor-root publication and covers pinned demand reads. | `PropertySpillBlockDescriptor::{descriptor_tree_key,encode_descriptor_tree_value,decode_descriptor_tree_entry}`, `PropertySpillWriter::{create,finish}`, `PreparedPropertySpillArtifact::publish`, `PropertySpillReader::{open,get_with_report,deep_scrub}`, `DurableStore::{write_canonical_segments,load_published_property_spills}` | `lower_bound_scan_seeks_to_the_first_greater_or_equal_descriptor`, `descriptor_tree_entry_codec_is_order_preserving_and_symmetric`, `spill_blocks_round_trip_and_fail_closed_on_corruption`, `out_of_core_property_spills_round_trip_and_fail_closed`, `SkeinGraphDescriptorPaging.tla` |
| A checkpoint-correlated row root remains cold at mount, replays consecutive global epochs and same-epoch relational fragments through a bounded dirty map into immutable runs, publishes the delta manifest last, and pins only a complete base-plus-delta view | `RelationalState::stage_transaction_with_row_changes`, `RelationalRowDeltaBuilder`, `RelationalRowDeltaReader`, `GraphStore::{mount_relational_row_pages_for_recovery,finish_relational_row_page_recovery}` | `relational_row_change_capture_reports_exact_net_primary_key_changes`, `multiple_relational_fragments_share_one_global_epoch`, `point_lookup_selects_the_newest_value_across_immutable_runs`, `read_only_open_reuses_an_exact_published_row_delta`, `durable_open_replays_wal_into_a_generation_pinned_row_delta`, `SkeinRowRecovery.tla` |
| A live row view stages DML before WAL, publishes only after durability, stages already-durable graph-only identity advances without empty batches, retains bounded immutable DML batches, fails closed on DDL or admission errors, preserves materialized read-your-own-writes, and never drifts pinned snapshots | `RelationalRowPageReadView::advance`, `GraphStore::{stage_relational_row_live_publication,publish_relational_row_live_view,finish_non_relational_commit}` | `capture_validation_rejects_undercharged_and_unordered_changes`, `overlay_admission_is_cumulative_and_atomic`, `durable_open_replays_wal_into_a_generation_pinned_row_delta`, `SkeinRowLiveView.tla` |
| Immutable relational row-delta runs stay within dirty/run/manifest limits, bind the exact base/schema-digest/column-count, exact final per-table row counts, exact WAL generation/LSN/record-sequence source, and any published overflow root, preserve unresolved content-addressed overflow references for same-epoch resolution, publish generation metadata before the latest selector, reject stale row-root or delta publishers, poison partial batches, demand-check run integrity, allow bounded unpublished newest-value lookup, and preserve pinned readers | `RelationalRecoverySourceBuilder`, `RelationalRowDeltaBuilder::{record,lookup_staged,finish_with_state}`, `RelationalRowDeltaReader`, `RelationalRowPageRootReader`, `RelationalOverflowRootReader` | `recovery_source_binds_record_order_and_payload`, `immutable_runs_round_trip_across_bounded_flushes`, `unpublished_lookup_prefers_dirty_values_then_newest_immutable_run`, `read_only_open_reuses_an_exact_published_row_delta`, `delta_schema_must_match_the_base_root_column_count`, `builder_is_poisoned_after_a_partial_epoch_error`, `every_pre_latest_stop_keeps_the_previous_delta_selected`, `stale_builder_cannot_replace_a_newer_delta_root`, `builder_cannot_publish_after_its_row_root_becomes_stale`, `overflow_references_allow_a_pinned_state_resolver_or_exact_root`, `corruption_poisoning_is_demand_driven`, `pinned_generation_remains_readable_after_new_publication`, `SkeinRowDeltaRuns.tla` |
| A relational row demand reader pins one exact row/overflow root, binds each table schema digest and non-zero column count, keeps pages cold until use, validates a cold physical slot exactly once before caching only its strong-tag-bound encoded prefix, rejects page/root shape drift as corruption, streams ordered point/range projections through one page pin, hydrates only selected overflow fields, applies page/byte/row/tree-height/hydration/cancellation limits, and makes corruption poison sticky without poisoning admission or cancellation | `DatabaseReadTransaction::query_sql_with_params_options_context`, `RelationalRowPageDemandReader`, `RelationalRowPageRootReader::read_page_slot_accounted`, `RelationalRowPageView::{open_verified,find_projected_row,decode_projected_row}`, `SegmentCache::{get_verified,insert_verified}` | `pinned_read_transaction_sql_propagates_cancellation_without_poisoning_service`, `point_projection_hydrates_only_selected_overflow_and_reuses_cache`, `verified_compact_entries_require_the_strong_source_tag`, `range_cursor_is_ordered_bounded_and_applies_lower_bound_once`, `admission_rejects_before_unbounded_io_without_poisoning`, `cancellation_and_callback_panic_release_page_pins`, `corrupted_page_poison_is_sticky_but_admission_is_not`, `table_root_column_count_must_match_every_demand_loaded_page`, `SkeinRelationalRowDemandRead.tla`, `SkeinVerifiedRowPageCache.tla` |
| A pinned relational snapshot selects live over recovery over checkpoint, treats tombstones as authoritative, admits merge sources and requested distinct overlay entries, retains recovery files through a bounded LRU pool, validates every encoded recovery field while owning only requested ordinals, keeps at most one projected head per source plus one working head, streams the checkpoint without materializing either complete input, preserves ordered range results and post-base insertions, yields its shared hydration account across nested join callbacks without losing cumulative admission, resolves an unbound overlay overflow reference only through the relational state pinned at the same epoch before callback visibility, publishes resolved values and hydration counters atomically, retains its immutable serving resources in host snapshots, distinguishes pre-checkpoint absence from reader loss, rejects ordinary live admission before WAL, requires manifest-last schema checkpoint publication before a schema-changed view may serve, allows a clean-WAL out-of-core authoritative handle to construct metadata-only state from its self-describing row root only after the row and index serving views are current, never falls back afterward, keeps later views isolated, and poisons only corruption or durability failures | `GraphStore::{load_checkpoint,require_relational_row_live_publication,open_relational_row_snapshot_reader,activate_out_of_core_relational_rows}`, `Database::complete_required_relational_row_checkpoint`, `RelationalRowRuntime`, `RelationalRowPageSnapshotReader::{visit_projected_range,visit_projected_range_resolving}`, `RelationalState::{from_canonical_row_root,hydrate_projected_row_with_context,omit_materialized_rows}`, `RelationalRowPageReadView::{overlay_value_accounted,overlay_range_sources}`, `RelationalRowDeltaReader::{range_sources,visit_range_entries}`, `RelationalRowPageDemandReader::visit_projected_range_with_overlay` | `live_row_admission_rejects_before_wal_append`, `schema_upgrade_publishes_a_canonical_row_checkpoint_before_returning`, `application_system_schema_upgrade_crash_recovers_a_consistent_registry_and_schema`, `canonical_row_reader_unavailability_fails_closed_and_poisons_sql_service`, `durable_open_replays_wal_into_a_generation_pinned_row_delta`, `read_only_out_of_core_authoritative_open_detaches_checkpoint_rows`, `read_only_open_reuses_an_exact_published_row_delta`, `read_only_out_of_core_open_validates_system_schema_through_canonical_rows`, `point_reads_select_live_recovery_checkpoint_and_tombstones`, `range_reads_merge_ordered_rows_and_keep_overlay_after_the_base_tail`, `range_callback_starts_before_the_complete_overlay_is_consumed`, `streaming_range_sources_bound_retained_run_files`, `streaming_range_sources_decode_only_requested_recovery_fields`, `live_overflow_stays_unresolved_until_the_pinned_state_resolves_it`, `large_payload_is_externalized_and_hydrated_with_explicit_budgets`, `overflow_references_allow_a_pinned_state_resolver_or_exact_root`, `overlay_admission_and_cancellation_do_not_poison_the_reader`, `pinned_reader_does_not_observe_a_later_live_view`, `checkpoint_corruption_poison_is_sticky_at_the_composite_reader`, `range_lookup_prunes_runs_and_honors_exclusive_bounds`, `database_sql_uses_bounded_pinned_relational_indexes_with_observable_fallback`, `initial_content_store_tables_are_qualified_through_canonical_row_pages`, `SkeinRelationalRowSnapshotRead.tla`, `SkeinRelationalOverlayStreamingMerge.tla` |
| A read-only `OutOfCore` plus `Authoritative` WAL reopen mounts canonical relation metadata without row decoding, validates the whole WAL, rejects schema-changing records, and serves only after both row and index recovery artifacts match the exact source and epoch; it adopts logical counts from the row manifest and never publishes derived files | `GraphStore::{load_checkpoint,replay_wal,uses_sparse_read_only_relational_recovery,finish_relational_row_page_recovery,finish_relational_index_recovery}`, `RelationalState::adopt_recovered_row_counts`, `RelationalRowDeltaReader::open_latest_with_recovery_fence`, `RelationalIndexRecoveryReader::open_generation_with_cache` | `read_only_authoritative_wal_reuse_stays_metadata_only`, `SkeinSparseRelationalRecovery.tla`, `SkeinRowDeltaRuns.tla`, `SkeinIndexRecovery.tla` |
| A writable `OutOfCore` plus `Authoritative` reopen constructs metadata-only relation state from the row root, replays authenticated WAL through bounded sparse row/index builders, activates only when both views equal the database epoch, and completes the first later sparse live commit without rematerializing checkpoint rows | `GraphStore::{load_checkpoint,stage_recovered_relational_transaction,activate_out_of_core_relational_rows}`, `RelationalState::from_canonical_row_root` | `sparse_writable_recovery_hydrates_checkpoint_and_prior_wal_rows`, `metadata_only_writable_commit_hydrates_live_candidates_and_publishes_rows`, `metadata_only_writable_upsert_hydrates_authoritative_unique_postings`, `SkeinSparseRelationalActivation.tla`, `SkeinSparseWritableRelationalRecovery.tla`, `SkeinSparseRelationalLiveCommit.tla` |
| One schema-stable writable replay record can be evaluated in a materialized workspace containing exactly its authenticated present/missing access entries, with hard entry and conservative resident-byte admission, unpublished staged-delta-before-pinned-checkpoint lookup, differential access-set validation, exact detached-count advancement, bounded overflow retention, and workspace disposal; this is the per-record refinement selected by metadata-only writable recovery | `GraphStore::hydrate_sparse_relational_recovery_access`, `RelationalRowDeltaBuilder::lookup_staged`, `RelationalState::{stage_sparse_transaction_for_authoritative_recovery_with_replay_access,hydrate_sparse_recovery_row_with_context}` | `unpublished_lookup_prefers_dirty_values_then_newest_immutable_run`, `sparse_authoritative_recovery_matches_materialized_predicate_replay`, `sparse_writable_recovery_hydrates_checkpoint_and_prior_wal_rows`, `SkeinSparseWritableRelationalRecovery.tla`, `SkeinRelationalWalReplayAccess.tla` |
| One schema-stable live transaction can stage against a bounded metadata-only workspace that contains every mutation access plus unchanged authoritative constraint-support rows, rejects actual replay access outside that explicit present/missing set, applies unique and foreign-key validation through the existing transaction engine, advances only the mutation's exact detached-count delta, and discards all hydrated rows before publication | `RelationalState::stage_sparse_transaction_with_authoritative_replay_access`, `RelationalSparseLiveStage`, `RelationalConstraintIndex` | `sparse_live_staging_validates_constraints_without_counting_support_rows`, `sparse_live_staging_hydrates_authoritative_unique_conflicts`, `SkeinSparseRelationalLiveStage.tla`, `SkeinRelationalWalReplayAccess.tla` |
| A schema-derived sparse live plan separates direct primary-key points, predicate-table range scans, and UPSERT conflict probes; unpublished preparation then derives exact replay access and unique/FK/referrer probes from real row/index captures, repeats hydration until the bounded set is closed, and never publishes state or WAL | `RelationalState::{plan_sparse_transaction_hydration,prepare_sparse_transaction_for_authoritative_live}`, `RelationalSparseMutationHydrationPlan`, `RelationalSparseLivePreparation` | `sparse_mutation_hydration_plan_separates_points_scans_and_upsert_probes`, `sparse_live_preparation_discovers_foreign_key_constraint_probes`, `sparse_live_preparation_exposes_new_primary_key_hydration`, `SkeinSparseRelationalLivePreparation.tla` |
| A metadata-only `GraphStore` commit closes one cumulatively bounded row/index hydration workspace, stages canonical state and both live views before WAL, rejects without changing epoch/LSN/views, and publishes the same epoch atomically after WAL or recovery | `GraphStore::hydrate_sparse_relational_live_workspace`, `RelationalSparseLiveHydrator`, `RelationalSparseWorkspaceBuilder`, `GraphStore::commit_prepared_mutation_ops` | `metadata_only_writable_commit_hydrates_live_candidates_and_publishes_rows`, `metadata_only_writable_upsert_hydrates_authoritative_unique_postings`, `metadata_only_live_hydration_budget_rejects_before_wal`, `SkeinSparseRelationalLiveCommit.tla` |
| A schema-stable relational DML commit captures every predicate-read, transiently touched, or no-op-conflict-read primary key before WAL, authenticates the strictly ordered bounded set with the logical record, and writable recovery rejects missing, oversized, unordered, or recomputed-set drift before accepting the transaction | `RelationalState::{stage_transaction_with_authoritative_replay_access,stage_transaction_with_index_row_and_replay_access,stage_transaction_with_row_changes_and_replay_access,stage_transaction_for_authoritative_recovery_with_replay_access}`, `encode_relational_wal_batch_with_replay_access`, `GraphStore::{commit_prepared_mutation_ops,stage_recovered_relational_transaction}` | `authoritative_constraint_staging_derives_index_and_row_batches_once`, `replay_access_retains_transient_and_primary_key_working_set`, `replay_access_retains_predicate_non_matches`, `replay_access_retains_noop_upsert_conflict_reads`, `replay_access_limit_rejects_before_durable_staging`, `replay_access_limit_rejects_unbounded_predicate_scan_without_changes`, `wal_encoder_rejects_replay_access_above_decoder_limit`, `replay_access_decoder_rejects_noncanonical_order`, `read_only_authoritative_wal_reuse_stays_metadata_only`, `SkeinRelationalWalReplayAccess.tla` |
| Relational resource evidence names only the row and index views current at the observed database epoch and reports their checkpoint, recovery, and live layers without scanning candidate files or materializing rows | `GraphStore::storage_residency_report`, `RelationalRowPageState::residency_report`, `RelationalIndexShadowState::residency_report` | `relational_storage_residency_tracks_checkpoint_live_and_recovery_views`, `SkeinRelationalRowSnapshotRead.tla`, `SkeinIndexRecovery.tla` |
| Release admission requires current-identity desktop and explicit 512 MiB policy evidence, distinct representative production and constrained 512 MiB reads, and separate desktop plus capability exact overflow-compaction reports; an identity advance makes every retained input stale until recollected | `run_production_content_store_memory_qualification`, `run_production_content_store_overflow_compaction_qualification`, `evaluate_production_release_qualification_bundle`, `release_bundle::{memory,overflow_compaction}` | `complete_raw_artifact_bundle_is_ready`, `memory_profile_matrix_rejects_different_resource_snapshots`, `production_read_cannot_substitute_the_512_mib_capability_run`, `configured_production_read_cannot_substitute_the_512_mib_capability_run`, `overflow_compaction_profiles_cannot_substitute_for_each_other`, `SkeinMemoryTierRelease.tla` |
| The read-only production Content Store runner reopens one pinned serving view per case, obtains and releases one bounded foreground permit per measured read, rejects row/index epoch drift, records sequential manifest/checkpoint-root/WAL/post-replay intervals inside one total-open interval, bounds mandatory pre-query payload-cache requests and residency without allowing pins, eviction, rejection, or corruption, and observes cold/warm demand reads without changing the modeled publication state. The cache counters are observational refinement evidence for the existing cold-page transitions, not a new durable state. The bounded CLI plan parser only constructs this typed runner and has no independent durable transition. | `GraphStore::{open_with_options,finish_open,storage_residency_report}`, `StorageOpenTimings`, `run_production_content_store_storage_qualification`, `skein-content-store-read-qualification` | `storage_recovery_report_tracks_wal_replay_boundary`, `cold_open_defers_row_page_payload_io_until_demand_read`, `relational_index_shadow_publishes_generation_fenced_cold_pages`, `production_content_store_runner_binds_cold_warm_relational_residency`, `content_store_release_gate_recomputes_bounded_open_payload_cache`, `SkeinStorageDurability.tla`, `SkeinRuntimeAdmission.tla`, `SkeinRelationalRowSnapshotRead.tla`, `SkeinIndexRecovery.tla` |
| The writable production Content Store runner keeps the source read-only, commits disjoint frozen insert/update sequences on isolated 1/4/8/10-writer replicas, observes one epoch per durable transaction, reopens non-empty WAL into current row/index recovery views bound to the exact replay-source identity, retains engine-measured open-phase partitions rather than inferring replay time from two opens, then checkpoints and reopens a delta-free manifest-selected view with identical result digests. The bounded CLI plan parser binds caller-owned source and replica paths to this existing typed runner, validates group-commit evidence through engine constructors, and has no independent durable transition. | `RelationalRecoverySourceBuilder`, `RelationalIndexRecoveryBuilder::finish_with_recovery_source`, `StorageOpenTimings`, `run_production_content_store_mutation_qualification`, `skein-content-store-mutation-qualification`, `ConcurrentDatabase::{storage_pressure_snapshot,storage_residency_report,storage_recovery_report}` | `recovery_source_binds_record_order_and_payload`, `production_mutation_runner_qualifies_isolated_writer_matrix`, `content_store_release_gate_recomputes_open_timing_partitions`, `SkeinTransactionConcurrency.tla`, `SkeinWalGroupCommit.tla`, `SkeinStorageDurability.tla`, `SkeinRelationalRowSnapshotRead.tla`, `SkeinIndexRecovery.tla` |
| A torn WAL batch has no partial recovered visibility | `replay_wal` record decode and batch apply | `default_recovery_rejects_torn_wal_tail_until_explicit_doctor_repair`, `doctor_discards_torn_batch_wal_without_partial_path_recovery` |
| Doctor repair binds destructive truncation to an exact acknowledged plan and resumes a durable pending audit | `DatabaseDoctor::{plan_wal_tail_repair,apply_wal_tail_repair}` | `apply_rejects_toctou_change_without_preparing_repair`, `prepared_repair_blocks_open_and_can_continue`, `truncated_pending_repair_is_resumable_and_blocks_open_until_finalized` |
| Complete-record corruption and LSN gaps fail closed | `replay_wal` framing, checksum, and expected-LSN checks | `rejects_and_quarantines_checksum_corruption_at_wal_tail`, `rejects_and_quarantines_checksum_corruption_before_valid_wal_suffix`, `rejects_and_quarantines_non_contiguous_wal_lsn` |
| Checkpoint publication selects one complete graph, row-page, and overflow generation; generation-manifest replace failure and other unbound candidates cannot replace it, while corrupt bound generation metadata fails open closed | checkpoint failpoints, `durable_replace_file`, `RelationalRowPagePublisher::persist_generation`, `RelationalOverflowPublisher::persist_generation`, and `SKEIN_MANIFEST_V1` replacement | `checkpoint_publish_failpoints_recover_one_complete_generation`, `generation_manifest_replace_failure_leaves_the_canonical_row_root_unselected`, `generation_manifest_replace_failure_leaves_the_canonical_overflow_root_unselected`, `platform_obstruction_rejects_canonical_row_and_overflow_manifest_replace`, `canonical_checkpoint_rewrites_only_dirty_relational_pages`, `unbound_row_candidate_does_not_replace_canonical_recovery`, `corrupt_bound_row_root_fails_database_open`, `subprocess_crash_matrix_recovers_whole_batches_and_artifact_generations`, `SkeinCowPagePublication.tla` |
| Reader pins and retained canonical roots prevent reclamation of every referenced physical page or overflow extent; backup and scrub traverse the same closure | `ReaderPins`, `DurableStore::{reclaim_old_generations,backup_to,scrub_storage}` | `read_transaction_pins_checkpoint_manifest_until_drop`, `out_of_core_reader_pin_retains_its_canonical_generation_until_drop`, `canonical_row_overflow_backup_reopen_and_reclaim_follow_physical_closure`, `reclaim_preserves_overflow_extents_referenced_by_retained_roots`, `initial_content_store_tables_are_qualified_through_canonical_row_pages` |
| Canonical path aliases share one ownership boundary | `DatabaseDirectoryLease::acquire` | `durable_database_open_is_exclusive_until_owner_drops`, `durable_database_rejects_path_alias_until_owner_drops` |
| Stale optimistic commits fail before publication; relational and graph logical locks preserve compatibility and stay within a hard budget through covering escalation or rejection | `commit_mutation_transaction_and_relational`, `GraphMutationTransaction::lock_footprint_since`, `LockTable` | `optimistic_transactions_prepare_in_parallel_and_reject_the_stale_committer`, `ordinary_snapshot_select_does_not_block_an_exact_update`, `for_update_point_lock_blocks_exact_update_until_owner_finishes`, `initial_content_store_tables_are_qualified_through_canonical_row_pages`, `relationship_creation_conflicts_with_endpoint_delete_guard`, `narrow_locks_escalate_before_the_next_entry_is_granted`, `lock_table_hard_cap_rejects_without_growing_residency` |
| A failed graph statement restores its COW workspace and pre-statement lock set without discarding earlier successful work | `GraphMutationSavepoint`, `LockSavepoint`, `LockTable::restore_transaction` | `failed_graph_statement_restores_workspace_and_statement_locks`, `graph_lock_failure_restores_the_failed_statement_only`, `statement_savepoint_restores_replaced_lock_and_budget` |
| A deadlock-closing multi-owner wait edge selects one victim and releases its dependencies | `WaitForGraph::register`, `ConcurrentDatabaseTransaction::abort_after_lock_failure` | `point_lock_upgrade_cycle_selects_one_deadlock_victim`, `wait_for_graph_detects_a_cycle_with_multiple_blockers` |
| A stale, mixed, missing, or corrupt Source scan sidecar falls back to the canonical graph | `source_scan::load`, `ScanSegmentManifest::plan_scan` | `checkpoint_publishes_source_scan_and_wal_mutation_invalidates_it`, `corrupted_source_scan_artifact_never_blocks_canonical_graph_recovery` |
| Scalar, ordered composite, and relationship graph-property index payloads stay cold at open, publish with the checkpoint generation, admit the complete definition directory before artifact creation, validate selected candidates against canonical rows, merge the WAL overlay, and verify each selected block before serving; one-hop relationship reads retain adjacency when its estimated work is cheaper | `PersistentPropertyProjectionDefinitionAdmission`, `PersistentPropertyProjectionWriter`, `PersistentPropertyProjectionReader`, `GraphStore::{visit_nodes_by_property_owned,visit_nodes_by_composite_property_owned,visit_adjacent_relationships_with_filter_owned}` | `definition_admission_rejects_before_artifact_creation`, `external_projection_round_trips_node_composite_and_relationship_candidates`, `out_of_core_property_projections_merge_wal_delta_and_fail_closed`, `out_of_core_relationship_property_projection_prunes_and_merges_wal_delta`, `explain_analyze_reports_out_of_core_relationship_projection_pruning`, `SkeinIndexPublication.tla`, `SkeinCompositePropertyProjection.tla`, `SkeinRelationshipPropertyProjection.tla` |
| An authoritative relational open omits transitional materialized postings, pins one current base-plus-recovery-plus-live index view and its complete generation artifacts, validates primary/unique/UPSERT/foreign-key constraints and stages the next visible view before WAL, leaves WAL/rows/index epochs unchanged on rejection, and recovers a durable WAL publication without rebuilding or revalidating postings | `decode_relational_checkpoint_file_with_index_load`, `AuthoritativeRelationalConstraintIndex`, `GraphStore::{snapshot,validate_authoritative_relational_index_open,require_authoritative_relational_index_live_publication}`, `RelationalState::{stage_transaction_with_authoritative_index,stage_transaction_for_authoritative_recovery}` | `checkpoint_decode_can_omit_materialized_postings_and_recovery_keeps_them_omitted`, `authoritative_recovery_does_not_revalidate_durable_non_primary_foreign_keys`, `authoritative_relational_indexes_gate_constraints_and_recover_live_commits`, `authoritative_open_rejects_missing_and_corrupt_bound_generations`, `authoritative_constraint_corruption_rejects_before_wal_and_poisons_service`, `authoritative_sql_never_falls_back_to_materialized_postings`, `SkeinIndexPublication.tla` |
| An authoritative multi-statement SQL transaction pins committed row and index views, merges prior successful statements through cumulative entry/byte-bounded private immutable overlays without assuming composite-prefix callbacks are primary-key ordered, gives queries and primary/unique/UPSERT/foreign-key checks read-your-own-writes, leaves both overlays unchanged on statement rejection, and revalidates the complete group before WAL | `DatabaseTransactionState`, `RelationalTransactionRowView`, `RelationalTransactionIndexView`, `GraphStore::stage_sparse_relational_transaction_statement`, `RelationalIndexReadMode::AuthoritativeTransaction`, `RelationalRowReadMode::Transaction` | `writable_metadata_only_transaction_checkpoints_rows_and_indexes`, `transaction_overlay_admission_is_cumulative_and_atomic`, `transaction_overlay_delete_merge_is_independent_of_prefix_visit_order`, `authoritative_transaction_index_overlay_preserves_multi_statement_ryw`, `initial_content_store_tables_are_qualified_through_canonical_row_pages`, `SkeinTransactionIndexOverlay.tla` |
| A metadata-only checkpoint resolves dirty keys from the final row overlay, conservatively retains the pinned overflow base while merging bounded dirty references, publishes the new row root, rebuilds required indexes by batch-scanning that root through the bounded spill builder, and binds all candidates only through the outer manifest; it never encodes detached state as a legacy full-row checkpoint | `RelationalRowPageReadView::{checkpoint_capture,checkpoint_overlay_row}`, `RelationalOverflowPublisher::persist_generation_retaining_base`, `RelationalIndexShadowWriter::publish_generation_from_source`, `GraphStore::{plan_relational_row_page_checkpoint,prepare_relational_index_candidate}` | `metadata_only_delta_retains_unmentioned_base_extents`, `writable_metadata_only_transaction_checkpoints_rows_and_indexes`, `initial_content_store_tables_are_qualified_through_canonical_row_pages`, `SkeinCowPagePublication.tla`, `SkeinOverflowPublication.tla`, `SkeinIndexPublication.tla`, `SkeinStorageDurability.tla` |
| Exact overflow compaction scans one pinned metadata-only row closure without payload hydration, externally sorts references within memory/spill/run limits, rewrites every reachable envelope into a fresh physical generation, selects it only through the outer checkpoint manifest, preserves current, previous, and reader-pinned roots until reclamation, and emits bound production resource plus physical-deletion evidence from a caller-owned disposable replica. Its CLI is observational; the release gate independently revalidates two profile-specific raw reports. | `Database::compact_relational_overflow`, `GraphStore::collect_exact_relational_overflow_closure`, `RelationalOverflowReferenceSetBuilder`, `RelationalOverflowPublisher::persist_generation_exact_references`, `DurableStore::reclaim_old_generations`, `run_production_content_store_overflow_compaction_qualification`, `skein-content-store-overflow-compaction-qualification`, `release_bundle::overflow_compaction` | `spilled_reference_set_is_repeatable_sorted_and_deduplicated`, `conflicting_metadata_for_one_digest_fails_closed`, `exact_overflow_compaction_rewrites_reachable_closure_without_hydration`, `qualification_proves_exact_rewrite_physical_reclaim_and_reopen`, `overflow_compaction_top_level_ready_cannot_hide_raw_budget_drift`, `SkeinOverflowExactCompaction.tla`, `SkeinMemoryTierRelease.tla` |
| A Source chunk write deletes the previous set, inserts exactly the requested ordered set, updates graph and document counts in the same mixed transaction, preserves the accepted workspace after duplicate-order rejection, supports an empty replacement, and survives checkpoint/reopen without a stale suffix | frozen `upsert_source_chunks` statement group, `GraphMutationTransaction`, `DatabaseTransactionState`, `qualify_source_chunk_replacement` | `initial_content_store_tables_are_qualified_through_canonical_row_pages`, `source_chunk_replacement_is_bound_to_mixed_transaction_evidence`, `SkeinContentSourceReplacement.tla` |
| A Source ownership move publishes graph and relational workspace identity at one epoch, preserves chunk count and payload identity, exposes read-your-own-writes, survives checkpoint/reopen, and leaves a missing owner and commit epoch unchanged | frozen `update_source_document_space` and `source_chunk_count_by_source` statements, `GraphMutationTransaction`, `DatabaseTransactionState`, `qualify_source_ownership_move` | `initial_content_store_tables_are_qualified_through_canonical_row_pages`, `source_ownership_move_is_bound_to_mixed_transaction_evidence`, `SkeinContentSourceOwnershipMove.tla` |
| A guarded Thread ownership batch can move entries from different source spaces while publishing each graph Thread, relational document, and message set together, preserving a stale preview and every non-ownership payload field across restart | frozen `update_owned_document_space_guarded` and `update_thread_messages_space_guarded` statements, `GraphMutationTransaction`, `DatabaseTransactionState`, `qualify_thread_ownership_moves` | `initial_content_store_tables_are_qualified_through_canonical_row_pages`, `thread_ownership_move_is_bound_to_guarded_batch_evidence`, `SkeinContentThreadOwnershipMove.tla` |
| A Space merge publishes eligible graph Thread and Source owners, relational documents, messages, and Source chunk views through one guarded durable batch while a stale selected owner remains unchanged | frozen `update_owned_document_space_guarded` and `update_thread_messages_space_guarded` statements, `GraphMutationTransaction`, `DatabaseTransactionState`, `qualify_space_merge_ownership` | `initial_content_store_tables_are_qualified_through_canonical_row_pages`, `space_merge_ownership_is_bound_to_cross_owner_evidence`, `SkeinContentSpaceMergeOwnership.tla` |
| A Thread content write binds public and storage identities, preserves creation identity across UPSERT conflicts, rolls back one rejected statement, publishes exact summary state, and survives checkpoint/reopen as one complete mixed commit | frozen `upsert_content_document`, `upsert_thread_message`, `thread_document_payload_summary`, and `update_content_document_summary` statements, `GraphMutationTransaction`, `DatabaseTransactionState`, `qualify_thread_message_upsert` | `initial_content_store_tables_are_qualified_through_canonical_row_pages`, `thread_message_upsert_is_bound_to_mixed_transaction_evidence`, `SkeinContentThreadUpsert.tla` |
| A Thread reconciliation validates the complete occurrence mapping before mutation, reorders explicit and legacy anchors with their preserved occurrences, preserves immutable payload identity, and publishes graph, document, messages, anchors, and summary as one durable state | frozen `thread_messages_page`, `update_message_anchor_order`, `update_thread_message_order`, `upsert_thread_message`, and summary statements, `GraphMutationTransaction`, `DatabaseTransactionState`, `qualify_thread_message_reconcile` | `initial_content_store_tables_are_qualified_through_canonical_row_pages`, `thread_message_reconcile_is_bound_to_occurrence_preserving_evidence`, `SkeinContentThreadReconcile.tla` |
| A Thread tail delete selects exact ordered occurrence identities, treats an empty tail as an epoch-preserving no-op, removes only tail message anchors and messages, publishes exact graph/document counts, preserves retained payloads, and survives checkpoint/reopen | frozen `thread_tail_delete_candidates`, `delete_thread_tail_anchors`, `delete_thread_tail_messages`, and summary statements, `GraphMutationTransaction`, `DatabaseTransactionState`, `qualify_thread_tail_delete` | `initial_content_store_tables_are_qualified_through_canonical_row_pages`, `thread_tail_delete_is_bound_to_mixed_transaction_evidence`, `SkeinContentThreadTailDelete.tla` |
| A whole-Thread delete discovers the exact owned and message-referenced document closure, treats missing and repeated deletes as epoch-preserving preflight no-ops, removes graph Thread/identity/Message and relational anchor/message/document ownership in one durable epoch, preserves unrelated payloads, and survives checkpoint/reopen | frozen `thread_owned_document_ids`, `thread_message_document_ids`, `thread_message_count`, `delete_anchors_by_document`, `delete_messages_by_thread`, and `delete_content_document_by_id` statements, `GraphMutationTransaction`, `DatabaseTransactionState`, `qualify_thread_delete` | `initial_content_store_tables_are_qualified_through_canonical_row_pages`, `thread_delete_is_bound_to_mixed_transaction_evidence`, `SkeinContentThreadDelete.tla` |
| A column-group catalog publishes artifacts and changed table directories before one generation-CAS manifest; reopen ignores orphan candidates and fails closed on referenced corruption | `ColumnGroupTableDirectory::write_immutable`, `ColumnGroupManifest::{publish,open}`, `PublishedColumnGroupCatalog::scrub_artifacts` | `publishes_reopens_and_reuses_untouched_table_directory`, `stale_publishers_are_serialized_and_one_fails_closed`, `orphan_candidate_is_ignored_and_corrupt_published_metadata_fails_closed`, `deep_scrub_detects_payload_corruption_not_read_by_reopen` |
| The columnar shadow never influences canonical recovery or checkpoint success; recovery discards a corrupt shadow and rebuilds all-dirty after an epoch gap; a shadow failure preserves dirty state and later converges. Codec body-size symmetry and pre-allocation metadata accounting are finite byte contracts outside the publication model and are checked directly at the Rust refinement boundary. | `GraphStore::{mount_columnar_shadow_for_recovery, record_columnar_shadow_checkpoint}`, `column_group::encoding::{finish_chunk,decompress_body}`, `ShadowMetadataBudget` | `restart_validates_the_shadow_and_replayed_mutations_mark_dirty_tables`, `shadow_publish_failure_never_fails_the_canonical_checkpoint_and_retries`, `shadow_reconstruction_matches_canonical_scan_and_reuses_untouched_tables`, `writer_and_reader_enforce_the_same_chunk_body_limit`, `metadata_budget_rejects_new_schema_before_allocating_or_publishing`, `metadata_budget_is_charged_before_dictionary_serialization` |
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
shared or exclusive logical spans. Relational rows/ranges and graph nodes,
relationships, delete guards, adjacency groups, labels, relationship types,
and allocation domains are all represented by finite key subsets. Database
and covering locks use the full key set. Lock tokens abstract the
implementation's entry/byte budget: each acquisition consumes a finite token,
escalation replaces multiple tokens with one covering lock, and budget
exhaustion terminates the requester. A statement savepoint captures the
transaction's prior locks and tokens; statement rollback restores that exact
set while transaction abort clears it.
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
acyclic, lock tokens never have multiple owners or exceed the configured set,
statement rollback preserves the pre-statement lock set, and a crash after WAL
durability recovers the committed epoch.

## Demand-Paged Index Publication

`SkeinIndexPublication.tla` models one manifest-selected row/index root pair,
generation-CAS publication, durable index pages, cold handle open, demand leaf
reads, cache loss on crash, and first-access corruption. It also models the
opt-in authoritative state machine: open requires a recoverable current
row/index view; a mutation reads and accepts or rejects one constraint page
before WAL; only an accepted mutation may make its WAL durable; and normal
publication or crash recovery advances row and index visible epochs together.
It checks that row and index root epochs never diverge, published roots are
durable and not ahead of canonical state, opening a handle does not warm leaf
pages, a successful lookup has loaded and verified its required page, a corrupt
selected page poisons the handle, a rejected constraint has no durable WAL or
visible effect, and an authoritative advance is recoverable from its bound
root plus durable WAL. `PublishCompetingRoot` represents a newer checkpoint
winning while an older builder is active; the old builder can only take
`RejectStalePublish`. `OpenAuthoritativeHandle` also removes the transitional
materialized-posting residency, and every authoritative constraint, WAL, and
publication action preserves that absence until crash or close.

The graph-property refinement remains rebuildable and merges its
post-checkpoint COW/WAL overlay before returning results.
`SkeinCompositePropertyProjection.tla` makes the composite serving refinement
explicit: a selected block remains cold until lookup, base candidates shadowed
by the overlay are skipped, matching overlay rows are merged, and selected
corruption fails and poisons rather than falling back.
`SkeinRelationshipPropertyProjection.tla` adds the one-hop relationship
selection boundary: only a current-generation projection whose estimated work
does not exceed endpoint adjacency may load its selected block. Both the
projection path and adjacency fallback verify the endpoint and complete
predicate against canonical records before merging the COW/WAL overlay, while
corruption poisons only after the projection has been selected. Definition
count and byte admission precede artifact creation at the Rust refinement
boundary; the model treats the admitted directory as finite. The relational
refinement uses the exact generation binding, recovery/live view, bounded
constraint reader, pre-WAL live-publication gate, and fail-closed
`Authoritative` SQL mode. The model abstracts multilevel navigation and cache
capacity; those remain owned by the index-recovery and page-cache obligations.

`SkeinGraphIndexQualification.tla` models the release-selection gate shared by
the eight persistent graph index classes. Its finite instance uses one property
and one adjacency representative because every class follows the same
generation-indexed evidence function. Differential, recovery, cache lifecycle,
and production evidence are recorded independently for each class and become
stale after a row or index generation change. Cache lifecycle evidence advances
only through constrained cold read, warm read, and cancellation cleanup. An
activation requires the completed lifecycle plus every other obligation at the
current aligned generation. An unqualified or stale class can only take the
canonical fallback, while corruption of a selected current-generation page
transitions the read to a fail-closed outcome. The model does not treat the
process-local Rust counters as authority; they are inputs to production
evidence and remain outside query semantics. The model also represents the
typed all-class matrix as a generation-bound publication: it can become current
only after every modeled class has complete current-generation evidence, and
it becomes stale when rows advance. The checked configuration explores the
finite state space over two index classes and two generations: TLC reaches
1,965,107 distinct states without an invariant violation.
The Rust refinement serializes each case's declared block/byte budgets and raw
cold/warm runs. `evaluate_production_release_qualification_bundle` requires the
all-class matrix and independently reconstructs the complete, ordered,
current-identity evidence set before release admission; top-level matrix
readiness is not a refinement witness. The bounded
`skein-graph-index-qualification` plan parser only constructs this existing
typed all-class matrix with one caller-owned read-only path and has no
independent durable or activation transition.
The bounded `skein-graph-storage-qualification` parser likewise constructs one
existing typed `ShadowReadOnly + OutOfCore` general graph measurement and has
no durable, publication, or activation transition. It shares the graph index
collector's resource-profile parser, so the dynamic desktop policy and the
explicit 512 MiB capability cannot diverge at this refinement boundary. Its
report is evaluated by the release bundle against the same graph resource,
identity, streaming, and fail-closed obligations modeled above; it is not an
additional state machine.

`SkeinRelationalIndexShadowPublication.tla` models the generation-aligned but
non-authoritative relational index candidate. Candidate fixed-slot pages become
durable before the generation-specific candidate manifest, and a canonical
checkpoint may bind that generation only after the candidate contains the
exact root set required by the pinned catalog/schema identity. The concrete
binding additionally records the page and generation-manifest length, CRC32C,
and SHA-256. The model abstracts primary, unique-constraint, declared-unique,
secondary, and foreign-key-support descriptors into one immutable
logical-completeness fact; later page corruption remains separate and is still
detected on access or by full scrub. Canonical publication may also succeed
without a binding, so admission failure and crash-orphaned future candidates
cannot replace or disable the selected checkpoint. Exact bound
generation/epoch open leaves page slots cold. A corrupt candidate is isolated
from canonical open in `Shadow` mode, while an explicitly selected
`DemandPaged` integrity failure fails the indexed read closed. Old selected
generations remain available to already-open handles. This shadow model does
not make an optional binding a uniqueness or foreign-key oracle. The stronger
mandatory-open and constraint-before-WAL contract is modeled separately by the
authoritative actions in `SkeinIndexPublication.tla` and implemented only when
the explicit `Authoritative` mode is selected.

The configured instance uses two non-zero generations, one non-zero commit
epoch, and one demand-loaded page. This is sufficient to cover a selected old
handle while a newer checkpoint wins, a future candidate abandoned before
checkpoint publication, checkpoint publication without a candidate, exact
generation/epoch selection, and both manifest and first-page corruption.

`SkeinRelationalIndexDemandRead.tla` models the generation-pinned demand-reader
mechanics independently from SQL activation. Opening a reader keeps every page
cold. A lookup loads only its ordered root/leaf/posting path while independently
enforcing page, byte, and row budgets. Emitted row locators are always an
ordered prefix of the materialized oracle; only a complete successful
traversal equals the complete oracle, and
an early-stop success is explicitly marked. Admission failure may leave
provisional rows that the caller must discard, while page corruption poisons
only the candidate reader. SQL selection, statement-wide budgets, and
materialized fallback classification are covered by runtime tests rather than
this page-traversal model.

`SkeinRelationalRowDemandRead.tla` models the canonical row-slot reader
independently from SQL activation and recovery/live overlay merge. One exact
row/overflow generation remains pinned while two logical pages start cold and
are loaded one at a time through a one-page cache. The model nondeterministically
chooses requested fields, independent page, byte, row, descriptor-height, and
overflow-hydration budgets, page or overflow corruption, cancellation, callback
panic, and early stop. TLC explores 21,519 distinct states. It checks ordered
prefix emission, full-result equality only on complete success, budget bounds,
one-page pins, terminal pin release, requested-overflow-only hydration,
generation stability, and corruption-only sticky poison. Codec bytes, digest
collision resistance, cross-platform positioned I/O, and the later
base-plus-recovery-plus-live SQL merge remain Rust refinement obligations.

`SkeinIndexRecovery.tla` models recovery and live-delta mechanics independently
from SQL activation. Canonical commits append ordered logical changes after one
checkpoint base. Recovery
replays that prefix into a finite dirty overlay, flushes immutable delta pages,
and publishes a unique candidate generation only after the complete replay is
durable. It checks WAL/oracle equivalence, overlay capacity, base-plus-delta
merge equivalence, candidate-generation isolation, publish-last visibility,
and fail-closed schema invalidation. After recovery publication, the model also
checks a bounded immutable live-change sequence, graph-only epoch advancement,
base-plus-live equivalence, retained pinned-reader state, and fail-closed live
capture invalidation. Graph-only commits append a logical no-op WAL record so
the recovery-source record range remains aligned with the global epoch range.
The published manifest and pinned reader retain the exact replayed WAL prefix;
`CrashCandidate` discards only unpublished candidate
state; the previously published manifest and canonical WAL remain unchanged.
The constraint-qualification invariant interprets membership as an exact
primary, unique, UPSERT-conflict, or foreign-key decision and proves that a
qualification can run only when the pinned live view is current and produces
the same decision as the canonical oracle for every modeled key. Production SQL
selection is still false in every modeled state; qualification does not make
the candidate authoritative.

`SkeinPageCacheAdmission.tla` models the clean immutable page-cache boundary
used by relational base and recovery-delta readers. The cache capacity is a
caller-carved domain below the root memory budget, so its maximum residency
preserves a separate foreground reserve. It checks resident/pinned/reference
accounting, pin-safe eviction, generation/digest/representation identity,
cache-cold open, corruption rejection, foreground bypass when all resident
pages are pinned, and cancellation release. Background population is
weak-fair and always terminates through a hit, admission, unpinned eviction, or
non-blocking bypass; it never waits for or evicts a pin. Dirty pages are outside
this model and remain a later COW page-publication obligation.

`SkeinVerifiedRowPageCache.tla` refines the fixed-slot relational row-page
reader into a compact verified cache entry. A cold read validates the complete
physical slot exactly once before retaining only its encoded prefix together
with the exact strong source tag. A warm read can skip the strong validation
only when that tag matches, and an untagged cache lookup cannot observe the
verified entry. The model checks compact resident-byte accounting, exact source
binding, fail-closed corruption, and the per-read validation count. The durable
row-page layout and publication order are unchanged.

`SkeinStableIdentityPublication.tla` models the independent physical-id to
stable-identity sidecar used by Skein Lightning. Candidate fixed-size pages
become durable in an immutable generation artifact before the checksummed
selector is published. A crash before selector publication leaves the prior
generation selected and makes the unselected complete artifact reclaimable.
Initial-import graph WAL append and visibility require one complete mapping
generation whose declared covered epoch matches the imported graph epoch. A
reader pins one retained immutable generation while a newer generation may
publish; reclamation excludes both the selected and pinned generations.
Corruption is scoped to the selected generation/page, causes the demand read to
fail, and poisons later reads through that handle.
Deep scrub advances only through validated pages, cannot skip a corrupt page,
and succeeds only after covering the complete selected page set.
The model deliberately does not equate this sidecar with the canonical
generation-bound graph `id` property index. Concrete Rust refinement covers
selector/header/file-length agreement, fixed-slot checksums, ordered key
ranges, bounded binary-search I/O, shared-cache identity, materialization
admission, single-page scrub residency, pin-safe generation reclamation,
selector-plus-generation backup binding, and the durable mapping-before-WAL
call order.

`SkeinStatisticsEligibility.tla` models the property-statistics type boundary.
Compact scalar and `VARCHAR` observations may create candidate facts, while
declared `TEXT` and other large values exclude the complete property group. The
mixed-type path includes the adverse ordering where a compact fact is observed
before the unsupported value. Publication occurs only after the complete scan
and filters every excluded group. The model checks that `VARCHAR` remains
publishable, text, large, and mixed groups never enter published NDV or
histogram state, and a fair complete scan eventually publishes its derived
snapshot.

`SkeinExactGraphCounts.tla` models the basic Count Store as part of canonical
graph mutation application. A node may carry multiple labels, while each
relationship has one type. Insert and delete actions update the canonical live
record set, total counter, and every affected label or type counter in one
transition. The model checks non-negative bounded counter state and proves that
total, per-label, and per-type fast-count results equal a full canonical scan
after every modeled mutation. WAL replay refines the same apply transition; WAL
durability and snapshot publication remain owned by `SkeinStorageDurability`
and `SkeinConcurrentSnapshots`.

`SkeinIndexStatistics.tla` models the payload-free sample maintained for each
explicit equality, range, or composite node index. A sample is one coherent
prior index state; canonical key mutations advance both the source epoch and
`updates_since_sample` without rewriting its counters. External resampling pins
a complete candidate and publishes it atomically only while its source epoch is
still current; a mutation during the scan forces candidate discard and leaves
the prior sample unchanged. The model checks that sample size and unique-value
counters remain valid, zero-churn samples equal the canonical index,
recovery-equivalent churn equals sample age, no future state is published, and
the optimizer never uses a sample beyond the configured update budget. The
model also separates explicit direct refresh from caller-owned background
refresh: direct refresh starts without a background permit, background scanning
requires an admitted permit, and publication or source-epoch abort releases
that permit. A budget, decode, or I/O failure follows the same release path
without replacing the prior sample. QoS policy decisions remain modeled
generically by `SkeinRuntimeAdmission.tla`; this model owns the index-refresh
refinement at the scan and publication boundary.

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
