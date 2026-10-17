# MVCC dependency validation and versioned projection publication

Status: design, finite model and initial native publication path in progress for
#231 and #232. Full embedded integration and qualification remain incomplete.
Baseline: main `4a1fdb80472231767923330fed69d5c587f3aa50` (2026-10-15).
This specification preserves the complete acceptance scope of both issues. A
model pass, a dependency helper, or an isolated engine benchmark does not by
itself deliver the embedded publication path.

## Existing implementation and the missing contract

The current [commit protocol](../MVCC_COMMIT_VALIDATION_PROTOCOL.md) already has
immutable COW snapshots, private transaction execution, per-key write stamps,
first-committer-wins validation, serialized WAL/group publication, snapshot pins,
and estimated current/retained version-history budgets. `VersionIndex` is a
conflict index, not a second history of canonical values. Read consumers retain a
compressed conflict baseline while storage snapshots retain their original pin
floor. Restart discards process-local transactions and may reset conflict history;
that is separate from canonical WAL/checkpoint recovery.

The original descriptions in #231/#232 at `8cbe16f8` are historical. They must not
be used as evidence that these mechanisms are absent. The remaining acceptance
includes collector completeness, memory/cleanup qualification, constrained and
predicate paths, recovery composition, writer scaling/fairness, and the retained
single-stream latency gate. Existing benchmark receipts include adverse and
inconclusive latency results; this work does not replace those gates.

The newly required projection path additionally needs read-dependency validation.
Checking a write set only says that a transaction's destinations were not changed;
it does not prove that the graph, predicates, target, or receipt used to compute a
projection are still valid. A successful check is also insufficient if relevant
mutations can occur before the durable output and watermark become visible.

At this baseline, `NowledgeMemEmbeddedStore::catch_up_incremental_search_with_batch_hydrator`
in `src/nowledge_mem/incremental_search.rs` captures a batch, opens a read
transaction, hydrates it, builds its delta, drops the snapshot and applies the
incremental delta within the mutable store operation. The embedded handle's
store-wide writer ownership serializes that work. PR #1032 merged into main at
`cfbea1b` (2026-10-15 17:41:56 UTC). Its owner/fence API is now available on
main and is incorporated into this feature branch. Actual prepared-session
and Mem integration remain required. Keep the PR based directly on main rather
than stacking on its former feature
branch. The merged coarse owner alone does not remove this bottleneck.

## Turso reference and selected lessons

The reference audit uses Turso commit
[`25550a3c8151b192ee53a2e9d290c8c3bf905439`](https://github.com/tursodatabase/turso/tree/25550a3c8151b192ee53a2e9d290c8c3bf905439).
Its [MVCC database](https://github.com/tursodatabase/turso/blob/25550a3c8151b192ee53a2e9d290c8c3bf905439/core/mvcc/database/mod.rs)
keeps per-row version chains in concurrent ordered maps, with transaction IDs or
timestamps marking visibility. Its dual cursor composes those versions with
the underlying B-tree. HawDB already retains canonical COW snapshots plus a
conflict index, so adopting that row-chain representation would require a
separate demonstrated need and recovery argument.

The [clock](https://github.com/tursodatabase/turso/blob/25550a3c8151b192ee53a2e9d290c8c3bf905439/core/mvcc/clock.rs)
allocates a commit timestamp and publishes `Preparing` under one lock, preventing
a reader from capturing a timestamp between those events. Speculative reads of
preparing transactions require dependency tracking and abort propagation. This
delivery preserves HawDB's published-snapshot path; it does not introduce those
speculative transaction dependencies.

The useful protocol lessons are explicit visibility/publication ordering,
separate written and durable prefixes in
[group commit](https://github.com/tursodatabase/turso/blob/25550a3c8151b192ee53a2e9d290c8c3bf905439/core/mvcc/database/group_commit.rs),
and [GC prerequisites](https://github.com/tursodatabase/turso/blob/25550a3c8151b192ee53a2e9d290c8c3bf905439/docs/internals/mvcc/GC.md)
that include materialization and reader/WAL position as well as transaction age.
HawDB's corresponding publication proof must compose source durability, active
generation roots and retained pins; a minimum epoch alone does not prove safe
artifact reclamation. Turso's Hermitage suite explicitly accepts SI write skew;
its implementation is not evidence for claiming general serializability here.

## Target contract

Retain Snapshot Isolation and its documented write-skew boundary. General SSI,
time travel and multi-process writing are not requirements of this delivery.
Keep one ordered graph WAL and the existing default `SyncOnEveryWrite` policy.
Production Mem integration remains a typed embedded Rust library path, subject
to the nightly/development-only release policy in `AGENTS.md`.

One publication attempt has these states:

1. **Captured**: under the authority that owns source/target replacement, capture
   one database instance identity, target incarnation, source snapshot and batch
   cursor. Register all required epoch/generation pins before the view escapes.
   Admit bounded preparation memory, I/O and background work before allocating.
2. **Preparing**: release the outer writer owner. Execute named parameterized
   Cypher/SQL reads through the pinned runtime, collect complete dependencies,
   hydrate within row/payload/work budgets and build private staging artifacts.
   Foreground writers must actually be able to commit during this phase.
3. **Staged**: retain pins and admission leases. Complete and verify every staged
   artifact; none is reachable through the active projection selector. Candidate
   generation and cleanup ownership protect artifacts from concurrent GC.
4. **Validated**: acquire the publication authority and revalidate source
   identity, target incarnation, schema/barriers, precise data dependencies,
   base projection generation and receipt/cursor prerequisites. Establish or
   verify the graph durability barrier covering the captured source epoch.
   Rejection changes neither active output nor durable watermark.
5. **Selected/pending durability**: retain source publication authority through
   the validated selector/read-view exchange. Select the complete preinstalled
   artifact closure, exact covered watermark and operation receipt together;
   source and dependency durability must already cover this selection. The
   proposed consumer protocol may release SourceOwner after this linearization
   point while an owned pending-durability token retains target operation/root
   exclusion through the head barrier. Later source writes do not invalidate the
   earlier selection. Process-local visibility before that barrier is distinct
   from an acknowledged durable publication. An uncertain head result retains
   its possible roots and operation custody; it is not a rollback certificate.
6. **Durable/acknowledged**: complete the required head file/directory barriers,
   then return the durable receipt for that operation. Complete read-view exchange
   belongs at selection, not in a later completion callback that could overwrite
   a newer binding. Release preparation ownership outside SourceOwner. Once the
   durability barrier completes, cancellation cannot turn the attempt into a
   reported rollback. A lost response is recovered by the retained operation
   identity/receipt and durable selector, not by recomputing whether an earlier
   callback returned success.

The current source-validated native publication wrapper still holds SourceOwner
through artifact installation and head synchronization. The shorter consumer lifecycle above is
specified and modeled in the [installed-activation design](../tla/CONSUMER_INSTALLED_ACTIVATION_DESIGN.md),
not implemented by the preparation-only operation token. Its off-owner barrier
must retain target custody, atomic view/receipt selection and fail-closed recovery.

Expensive data preparation belongs outside publication ownership. Measure source
fence hold time, target operation hold time and durability latency separately;
an atomic pointer swap does not make the complete publication free. Do not expose a public
`validate() -> bool` followed by an arbitrary later mutation callback: that API
would leave the validation-to-publication race open.

## Complete dependency identities

The library owns the reusable mechanism and the host owns application bindings,
receipt schemas and their predicates. Host guard reads still go through named,
parameterized runtime statements with explicit row/payload/work budgets. The
host must not replace those reads with an unbounded scan or claim that an opaque
callback has complete dependencies merely because it returned successfully.

A prepared attempt must carry non-forgeable ownership of:

- the exact `DatabaseInstanceIdentity` supplying the snapshot; path, contents and
  commit epoch are insufficient. Reopen or whole-database replacement invalidates
  a prepared token even if data and epochs are equal (#1011);
- the target identity and a monotonic incarnation, advanced for every rebind,
  including A-to-B-to-A. Equality of a current binding value is not ABA protection;
- the pinned source epoch and complete data read dependencies;
- schema and conservative database barriers, including the direct/opaque write
  paths whose narrow footprints have not been proven;
- the expected active projection generation, input cursor, source receipt and
  complete covered batch range;
- retained source/generation pins, cancellation context and admitted staging
  resources for the entire attempt, including queued publication.

Start from existing node/relationship/adjacency and qualified relational-row
identities where their write collectors are complete. Add domain/membership
identities for reads that select a set, and collect them from the AST/logical or
physical plan shape, not REST route names. A scan must record membership/range
information even when it returns zero rows. Looking only at returned rows misses
newly inserted matches and absent-key changes. All writes that can change the
predicate's result must stamp that domain, including insert/delete, indexed value
changes and supported schema/constraint transitions.

Keep these read-invalidation summaries separate from writer conflict intents.
Adding a shared label/table/membership stamp to every writer's validated write
set would incorrectly make disjoint writers in that domain conflict. Validate
the existing destination write set first, then publish the complete invalidation
summaries with that transaction's stamps at the same WAL/publication boundary.
Read validation consumes both precise stamps and summaries; writer validation
must preserve the existing disjoint-write semantics. Native refinement must
prove both completeness and this separation.

Unknown read/write shapes retain an explicit conservative barrier. Such paths
must be observable and bounded, not silently advertised as precise. Qualified
point/domain paths must admit unrelated commits without comparing the complete
database epoch for equality. Narrowing the fallback requires a collector proof
and adversarial tests; it is not a syntactic convenience change.

The read-dependency lifetime pins the same conflict-history floor as a writable
workspace. Pruning may remove stamps strictly older than that floor, not changes
that occurred after it. Compare latest relevant stamps with the capture epoch;
copying arbitrary `VersionIndex` fields out of a compressed read baseline cannot
substitute for a retained lineage pin.

## Graph/Search durability and recovery

Graph and Search remain distinct durable domains. This protocol does not promise
an atomic transaction, automatic rollback or implicit distributed commit across
them. Stage Search output privately; authoritative activation must bind its
artifact closure and progress receipt. If the existing Search backend cannot do
that without exposing intermediate output, add the backend lifecycle operation
before moving its I/O outside the owner. A host-side check around the old visible
mutation sequence is insufficient.

Reuse `crates/storage/src/projection_generation.rs` for owner-scoped bounded
candidate batches, sealing, immutable manifests and active-head publication.
`Database::projection_generation_store` already exposes this component. Its
`publish` checks durable manifest equality, expected-head CAS, non-regressing
source watermark and idempotent generation reuse, then synchronizes the head
replacement. Those checks protect the projection catalog; they do not validate
Graph dependencies, database-instance identity or a host target incarnation.
The catalog lock is not the Graph publication fence. Compose the existing
component under the source/target authority rather than inventing a second
generation catalog or treating its current token as a source-validity proof.
Search backend activation still needs an explicit integration mapping to this
closure; catalog availability alone does not establish that integration.

A durable projection must not refer to graph transactions that may disappear on
power loss. In explicit `SyncOnCheckpoint`, capturing an acknowledged graph epoch
is not proof of a durability barrier. Publication must synchronize/verify the
captured source prefix or return a typed source-not-durable refusal. It must not
silently strengthen or weaken the host's write policy. Checkpointing a later
complete prefix is sufficient if it includes the captured snapshot's dependencies.

Recovery must distinguish: uncommitted staging (eligible for conservative cleanup),
a partial selector installation (old complete selector or fail closed), and a
committed selector with a lost response (retain/reopen the complete new generation).
Never advance a watermark independently of its complete durable output or advance
it beyond the batch/receipt coverage actually established by preparation. Rebinding,
instance replacement or schema changes during preparation cause typed rejection;
a generation already durably selected is recovered by its committed identity.

Candidate pins and durable active selectors are GC roots. Durable branches remain
roots without runtime leases (#778). Runtime fault qualification must cover
lost unsynced writes, torn metadata/data and storage reordering (#820); process
termination alone does not establish hardware power-loss safety.

## Typed failure and resource contract

Distinguish dependency conflict, instance/target replacement, source-not-durable,
stale projection generation/cursor, budget refusal, cancellation before commit,
and uncertain durable publication. Preserve typed retryability. The caller owns a
bounded retry policy; retries recapture/reprepare, rather than replaying a stale
proof. Resolve an uncertain/lost commit receipt before allocating another visible
generation. Cleanup failure retains evidence and its required ownership; it does
not authorize publication or release resources still used by pending I/O.

The dependency set, query outputs, hydration, artifact builders, file handles,
queued publication and cleanup must have explicit admitted limits. Existing
64-MiB/256-MiB version payload estimates do not bound total RSS or all stage memory.
Do not claim global bounds from a finite symbolic lease in the model.

## Initial native implementation boundary

`Database::begin_prepared_read(PreparedReadLimits)` captures a canonical source
registration and an immutable query context under one owner borrow. Its wrapper
executes bounded parameterized Cypher and collects qualified node-plan label
domains before execution. Filter predicates are checked as well as operators:
relationship existence can introduce adjacency dependencies below a node scan.
Unsupported operators/predicates fail closed. Optimizer-folded `EmptyExec` keeps
an observable whole-database epoch dependency. Any ignored statement or dependency
budget error prevents `finish` from producing a usable source proof.

The storage token registers a history pin and enables node-domain invalidation
summaries for its entire lifetime. Ordinary old snapshots cannot start this
registration, because summaries may have been disabled since their capture.
Every accepted source therefore starts at the canonical tip before concurrent
writes can resume. A node insert, property mutation or deletion stamps its label
domains and the all-node domain. Unknown cold label membership stamps a separate
read-only barrier. Existing legacy/database/schema barriers remain conservative.
Destination writes are validated before these summaries are added to the staged
index; the union is admitted before WAL and installed with canonical commit.
When no prepared source exists, ordinary commits do not build those summaries.

Qualified scalar equality/membership reads now collect exact declared index
keys, including necessary EQ/IN conditions under AND in node scan predicates.
The footprint follows semantics rather than the optimizer's scan/seek cost choice.
OR/NOT and conditions outside bounded candidate phases cannot supply that bound.
The active preparation registration enables all declared scalar-index summaries
before any later query chooses its values, closing the capture-to-query gap.
Writers stamp old keys for every matching node mutation, plus new indexed values;
cold preimages use admitted projected reads of indexed properties. Shared reader/
writer key bounds and explicit metadata work/ownership bounds retain domain or
read-barrier fallbacks. Precision counts are exposed on the prepared dependencies.
See the [collector argument and controls](../tla/INDEXED_PROJECTION_READ_VALIDATION_PROOF.md).

Necessary internal-ID EQ/IN root bounds now record existing GraphNode keys,
including absent roots. Qualified non-optional single-hop matches additionally
record directed root adjacency and every matching edge/neighbor identity before
property/label filtering. A bounded metadata prepass composes live postings with
canonical descriptors without materializing relationship property values; encoded
canonical blocks still undergo full validation. Incomplete or
over-budget collection refuses the token. Bounds cannot cross a candidate
selection boundary or full-text source. See the
[conditional argument and eleven controls](../tla/ADJACENCY_PROJECTION_READ_VALIDATION_PROOF.md).
This does not yet replace the Search graph-delta builder or establish a bounded
batch coverage receipt.

`PreparedRead::hydrate_search_projection_nodes` now provides a query-based graph
hydration path for that migration. Native `properties`/`labels` keep user property
names separate from graph metadata. A qualified optional one-hop query preserves
unlabelled roots and performs grouping in the query runtime; the existing Search
shaper converts complete per-node rows. Missing catalog types still retain empty
adjacency dependencies because implicit CREATE may not stamp Schema. Input,
statement, dependency and retained logical-output limits fail the complete
operation, including ignored shape errors. The method returns no watermark or
batch authority. Qualified nonnegative internal-ID EQ/IN roots now use admitted
point access for ordinary, projected LabelScan and generic MATCH sources; cold
label headers reject wrong-label values before ownership. Native large-unrelated
payload regressions exercise actual hydration. Unsupported predicates retain
scan execution, and storage block/cache I/O remains outside a numeric source
visit/RSS bound. See the [ID access refinement](../tla/INTERNAL_ID_POINT_ACCESS_PROOF.md)
and [hydration closure](../tla/PREPARED_SEARCH_GRAPH_HYDRATION_PROOF.md).

`publish_prepared_projection` validates the source identity, captured watermark
and dependencies and calls the existing generation catalog under one exclusive
database borrow. Its expected-head CAS protects the catalog head. It resolves an
already selected identical generation as an idempotent committed receipt before
revalidating an old source token. New publication requires no unfinished WAL sync group and a known durable
source prefix covering the capture. The default typed policy,
`ProjectionSourceDurability::RequireKnownDurable`, refuses an uncovered prefix;
`Synchronize` explicitly synchronizes it without changing subsequent host commits.
Replaying a WAL or choosing synchronous future writes does not certify its prefix.
Runtime durability knowledge starts at the selected checkpoint, advances only
with successful file/namespace barriers, and is reset conservatively on reopen.
Existing WAL names also require a namespace barrier when not already known
persistent. The [source durability model and argument](../tla/PROJECTION_SOURCE_DURABILITY_PROOF.md)
cover these distinctions; byte-level power-loss composition remains outstanding.

Sealed generation tokens now retain candidate GC leases through their last clone.
Sealing transfers writer protection to that lease under the catalog lock. The
publisher rejects tokens from another catalog, and the collector retains active
heads, readers, sealed tokens and open writers as distinct roots. Dropping an
unpublished token makes its candidate eligible for bounded reclamation; dropping
a committed token does not remove the durable head root.

Prepared SQL hydration supports canonical public single-table SELECTs and the
qualified one-parent-primary-key INNER JOIN family described below.
A complete necessary primary-key tuple under AND records the existing relational
row intent, including absent keys and UUID coercion shared with query/writer
keys. Declared leading index tuples cover complete domains. Other admitted
single-table reads retain the complete SQL table domain described below.
Unqualified admitted JOIN scopes retain a Database-epoch fallback; other joins, functions,
system/projection-bound sources and locking/mutation statements fail closed.
SQL and Cypher share one sticky dependency budget. Pre-parse admission uses the
execution frontend's PostgreSQL tokenizer to bound recursive parser/lowering/drop
work, with separate expression/key and existing row/payload limits. See the
[relational collector argument and ten controls](../tla/RELATIONAL_PROJECTION_READ_VALIDATION_PROOF.md).

The Search backend additionally exposes owned `prepare_publication` candidates
for ordinary and partitioned full builds, incremental updates and governed
wrappers. A short capture lease chooses the prospective generation; expensive
artifact/partition construction then uses unique private stages without that
lease. Candidates keep stage cleanup and admission ownership while queued.
Activation reacquires publication exclusion, rechecks the captured selector
(including exact bounded bytes for the existing damaged-selector rebuild path),
and retains exclusion through complete manifest-last installation. Competing
same-name candidates cannot overwrite a winner after stale rejection.

`Database::publish_prepared_search` composes the shared source/dependency/durability
validator with those closed Search activation types under one exclusive database
borrow. No arbitrary callback can implement the sealed activation trait. This
path requires an exact capture watermark, rejects active-watermark regression
and refuses a damaged base whose prior floor cannot be established; changefeed
batch coverage and host
binding/receipt authority still need their own integration. The current legacy
bounded builder has a verified mixed-snapshot omission; the
[pinned-source batch coverage design](MVCC_SEARCH_BATCH_COVERAGE_SPEC.md)
specifies complete C..E preparation in bounded invisible steps and stage-bound
coverage authority. The bounded pinned feed and private resumable external
ordering primitives now have joint native intake/mapping/stage custody and a
closed-output verification phase retaining queryable E until every final
mutation and End is checked by a fixed trusted verifier under shared logical
admission, returning
only an opaque nonpublishable artifact. Independent full-source baseline set/content comparison is also implemented,
including native unique-owner-ID refusal and complete-count checks; its historical
match observation grants no publication authority. The complete versioned
baseline/capture/mapping/coverage
activation session, binding/external-version/receipt protocol and incremental
final index build remain outstanding. Activation still
checksums/installs artifacts and performs cleanup under the Search lease, so a
size-independent publication section has not been delivered. See the
[Search lifecycle argument and ten controls](../tla/SEARCH_PREPARED_PUBLICATION_PROOF.md).

Cold Search graph change capture now reads admitted canonical preimages and
both directed Label postings before WAL, then installs either complete events
or a persisted resume gap after append. Metadata limits require a full rebuild;
physical uncertainty fails before WAL. Full-prefix cold/hot and replay regressions
compare delta results against full rebuild. This does not certify bounded B < E
coverage when hydration reads E. See the
[change-capture argument](../tla/PROJECTION_CHANGE_CAPTURE_PROOF.md).

The catalog path publishes full generations; the native Search path activates
owned backend candidates. It does not yet move actual
incremental Search preparation out of `NowledgeMemEmbeddedStoreHandle`, collect
complete graph composite/range and additional traversal/relational dependencies, validate host binding
incarnations and batch receipts, integrate source-validated Search activation
into Mem catch-up, or qualify power loss and 1/4/8-writer latency. Unsupported value/access shapes deliberately retain
label domains or read barriers; those fallbacks are not advertised as exact keys.
Those requirements remain in scope and must not be replaced by this subset.

## Formal argument and finite-model boundary

The accompanying `HawDBVersionedProjectionPublication` model treats one prepared
full projection, two projected row identities (one initially absent), an unrelated
row, point/scan reads, scoped and broad stamps, monotonic target incarnations,
instance replacement, private two-part artifacts, graph relaxed/synchronous
commits, independent durability barriers, watermark/selector installation, GC,
cancellation, crash/reopen and lost-response acknowledgement.

It keeps an independent, unpruned stamp oracle. Validation uses the production-
shaped prunable index; publication must agree with the oracle. This exposes a
collector omission or unsafe pruning instead of assuming validation is correct.
Finite negative controls remove instance, incarnation, scan scope, barrier,
source/artifact durability, GC retention or publication-fence checks; additional
controls expose early visibility, early watermark and cancellation after commit.
Reachability controls require a disjoint commit during preparation followed by
successful publication, and idempotent acknowledgement after a committed crash.

The intended induction is: complete dependency capture plus retained history
makes index acceptance equivalent to no relevant intervening mutation; publication
exclusion preserves that result through selector commit; durable source coverage
and the durable artifact closure make the selected receipt recoverable; ownership
roots preserve every required source/artifact until the decision. Later graph
writes may make a projection observably stale; the theorem concerns validity at
publication, not equality with the forever-changing live database.

The model abstracts completed file/directory barriers as atomic events. It is
not Rust refinement, a byte-level power-loss model, a proof of arbitrary query
footprint extraction, numeric memory bounds, scheduling fairness or the complete
incremental multi-batch/constraint protocol. Existing MVCC/history/GC/durability
models retain their separate obligations. Native deterministic interleavings,
source-linked collector proofs, fault images and workload qualification are
required before claiming end-to-end delivery. Safety and a reachable successful
path do not prove starvation freedom.

## Delivery and acceptance ledger

| Requirement | Current authoritative evidence | Remaining delivery |
| --- | --- | --- |
| Snapshot reads and per-key writer conflicts | Current commit protocol and native transaction/version tests | Audit collectors and compose every supported path; preserve SI boundary |
| Version retention/admission | Existing history budget/pin models and tests | Churn/long-pin allocator/RSS and cleanup-latency qualification |
| Publication protocol | [Finite safety model and 14 controls](../tla/VERSIONED_PROJECTION_PUBLICATION_PROOF.md), registered safety action and initial source-validation/catalog path | Complete source refinement and embedded integration |
| Search preparation/activation | Owned single/partitioned/update candidates, selector CAS, shared source-validation facade and lifecycle model | Mem catch-up/outer-owner migration, host bindings and bounded batch receipts, size-independent activation I/O and fault/performance composition |
| Precise read dependencies | Pinned tokens, bounded exact scalar-index/domain, internal-ID/single-hop and relational primary-tuple/index-prefix, canonical table domains and qualified parent-PK JOIN collectors, native cold/hot regressions | Graph composite/range/other traversal and additional relational multi-key/range/multiway-join collectors, complete host/source-scope contracts and full completeness qualification |
| Invisible staging and durable activation | Existing `ProjectionGenerationStore` candidate/seal/head protocol and Search mutation components | Compose source validation and Search activation with complete receipt/closure and failure states |
| Complete baseline input | Pinned graph candidate frontier, all canonical relational tables/PKs, tracked callback reads and explicit End; [conditional model/source argument](../tla/PROJECTION_FULL_SOURCE_PROOF.md) | Versioned ownership/capture/mapping contract, artifact-bound session authority and cumulative physical-work/RSS qualification |
| Complete baseline comparison/rebuild | Native historical C set/content comparison and full single/empty/partitioned replacement from complete C input; [source argument/model](../tla/PROJECTION_FULL_REBUILD_PROOF.md) | Host binding/mapping/capture/external-vector authority, C..E overlay to avoid whole-C retries, durable receipts, incremental artifacts and short activation |
| C/E owner overlay | [Conditional algebra, finite model, retained oracles and native artifact-only two-sort reducer](../tla/PROJECTION_OWNER_OVERLAY_PROOF.md), including cross-phase collision and valid ID-swap witnesses | Source-qualified owner/phase framing, complete physical-owner/parent capture, bounded capture retention, versioned contracts, final-E validation and authority; the artifact builder does not remove the full-C fence |
| Joint source/mapping/stage custody | Native C..E session owns pending input, bounded continuation, cumulative invocation/output reservations and private unordered stage; [conditional model/source argument](../tla/PROJECTION_STAGING_SESSION_PROOF.md) | Complete baseline/coverage/binding/external-version/receipt verification and activation, physical-work/RSS governance and actual Mem adoption |
| Embedded owner concurrency | Current catch-up path; #1032 owner/fence merged on main at cfbea1b | Merged main owner available; integrate prepared sessions into the actual path; prove a competing disjoint writer commits while preparation is paused |
| Recovery/power loss | Existing WAL/group/branch proofs, #820 fault harness | Compose new staging/selector/source barriers, cancellation, lost responses and replay |
| Scaling/fairness/latency | Existing 1/4/8-writer harness and retained adverse samples | Small/large catch-up enabled/disabled runs, p50/p95/p99, lock wait/hold time, unchanged latency gates |

Deliver sequentially against main: establish the design/model, implement dependency
capture/validation and its native refinement, implement backend staging/activation,
integrate the outer owner, then finish fault and workload qualification. These can
remain commits in one PR; do not stack PR bases. Keep #231/#232 open until their
full original and added acceptance requirements are proven.

## Relational prefix read refinement (2026-10-16)

Prepared single-table SQL can record necessary leading tuples of declared
indexes, including complete group membership, payload-only changes, empty
results and off-page updates. The new read-only summary family is added only
after writer validation. Bounded complete index/row captures supply old/new
membership and payload stamps; incomplete/admission cases use ReadBarrier.
Unsupported scalar identities now retain the complete single-table domain
described below; qualified JOIN fallbacks are unchanged. Nullable unique
prefixes require nonnullable unbound suffixes; the ordinary base/JOIN selectors
share that correctness condition. See
[the conditional argument and model](../tla/RELATIONAL_INDEX_PREFIX_READS_PROOF.md).
This enables an engine prerequisite for Thread fanout. The qualified joined
SourceChunk family is described below; artifact-bound coverage, actual Mem owner
migration and the original
resource/concurrency qualification remain required.

## Joined SourceChunk read refinement (2026-10-16)

One aliased INNER JOIN to a complete parent PK collects child row/prefix and
parent row dependencies, including absent and filtered parents. The parent tuple
is fixed by necessary child equalities or a bounded same-snapshot child-PK SQL
lookup of join fields. All original graph/relational semantics remain in the
query runtime. Internal lookup limits are one row / 4 KiB, with generated SQL
metadata preflight; all errors remain sticky. Partial ON keys, multiple/outer
joins, ambiguous aliases and unsupported expressions refuse. Unqualified child
scopes and unsupported scalar identities keep whole-epoch fallback. See
[the conditional source argument and finite model](../tla/JOINED_PROJECTION_READS_PROOF.md).
The actual Mem mapper still uses the legacy batch callback and unbounded fanout
loop; this collector does not provide bounded host continuation, stage-bound
coverage, verified baseline/binding receipts, actual owner migration or the
remaining resource/recovery/scaling qualification.

## Full baseline input prerequisite (2026-10-16)

`begin_prepared_full_projection_source` enumerates graph allocation candidates
(including holes), every canonical relational table (including empty and engine
tables), and ordered primary keys from one immutable source. Callback queries
share that source and cancellation context. Metadata/two key slots are admitted
before ownership; logical input and seek counters accumulate across steps.
Failures are sticky and End must be acknowledged. Finish returns conservative
whole-epoch read dependencies, never a verified baseline or stage receipt.

The [source argument, six native tests and finite model](../tla/PROJECTION_FULL_SOURCE_PROOF.md)
cover pinned enumeration and failure custody. Cold range seeks still have
per-seek rather than cumulative physical-I/O limits. Complete source/host effect
mapping, verified baseline document comparison, unique source ownership,
source/capture/mapping/binding/incarnation/operation receipts, the complete
verified C..E activation session, actual Mem owner migration, power-loss
composition and the original
1/4/8-writer latency/fairness/resource qualification remain required. Enumeration
completion must not mint any of those authorities.

## V1 owner source facts and recovery fence (2026-10-17)

The native capture log now retains physical owner-only graph effects, including
Thread deletion, separately from the legacy built-in document event. ID changes
and deletions retain transaction-start labels and old string/absent/unsupported
identity. Other effects require hydration at the pinned end. The retained byte
budget covers these payloads; the existing future-capture retention limit still
applies independently of C snapshot pins.

Persistent GraphStore checkpoints include an explicit V1 owner floor and
transaction/effect rows inside the existing integrity/publication envelope.
Opening an old checkpoint preserves canonical data but fences owner capture at
its head. Missing or malformed V1 facts refuse recovery. Older readers reject
the new extension. The public legacy checkpoint encoder remains an old-format
oracle and does not confer V1 completeness.

See [the contract, source correspondence, tests and finite recovery model](../tla/PROJECTION_OWNER_CAPTURE_PROOF.md).
These internal facts are prerequisites for the original complete bounded C..E
session and actual Mem integration, not coverage or publication authority.


## Bounded V1 owner feed (native enumeration)

`begin_prepared_projection_owner_feed` returns an owned immutable E context and
bounded source cursor. It enumerates physical graph owner changes with borrowed
preimages, existing relational keys and an explicit End; it does not expose
legacy document deletion IDs as owner identity. The cursor applies both resume
floors, shares the legacy feed's private admission/state machine and preserves
repeated transaction inputs. Pinned catalog label-ID lookup supports deleted
owners without a mutable source escape. Missing label names retain precise
node-domain dependencies for implicit creation; explicit DDL retains the schema
barrier. Unrelated property writes do not require whole-epoch validation.
Query failure, callback failure/panic,
source poison, missing V1 facts and exhausted admission prevent completion.

See [the conditional prefix/admission argument, finite owner-input model and
native evidence](../tla/PROJECTION_OWNER_FEED_PROOF.md). End and finish return
enumeration/dependency evidence only. They are not a baseline/mapping/stage or
watermark receipt. Empty SQL DDL can produce no owner input, so a C..E session
must independently bind catalog/schema and mapping versions. Child fanout,
independent vectors, first-C identity or a qualified owner baseline, post-E
new-owner invalidation, final activation/receipts and actual Mem migration
remain original requirements.


## Canonical SQL table read scopes (native E-validation primitive)

Qualified single-table SELECTs without a complete point/index-prefix identity
now retain `ReadRelationalTableDomain` rather than Database epoch equality.
Complete staged row captures stamp changed tables, including tombstones and
payload-only updates, after writer validation and before WAL. Incomplete or
unadmitted summaries retain ReadBarrier; schema/opaque barriers still apply.
This preserves disjoint same-table writer validation and protects empty/off-page
SQL scope observations against later inserts, updates and deletes. Append-store
AppendTable is a different contract. Point/index-prefix and JOIN qualification
remain separately observable. See [the source argument, finite model and native
qualification](../tla/RELATIONAL_TABLE_SCOPE_PROOF.md).

This is an E-validation primitive, not complete C..E stage/coverage or host
mapping authority. The complete native session, source/target contracts,
independent vectors, actual Mem migration, durable recovery and original
resource/concurrency gates remain required.


## Relationship domains and pre-registered E scopes

`begin_prepared_projection_owner_feed_with_scopes` admits complete named node,
relationship-type and canonical SQL table guards on the feed's same pinned E.
ReadRelationshipDomain covers create, payload update and delete, including
relationships outside callback-selected endpoints. Metadata-only identity
resolution uses the published header or borrowed same-transaction create;
unknown identities and exhausted admission retain ReadBarrier. Installation
follows writer validation and successful WAL/canonical publication. Existing
scope keys subsume only their corresponding SQL row/prefix or label-property
keys, without clearing sticky refusal or arbitrary physical dependencies.

See [the source correspondence, conditional proof and finite model](../tla/RELATIONSHIP_READ_SCOPE_PROOF.md).
This additive guard API does not authorize closed mapping, stage coverage,
C..E source continuity or Mem activation. Full integration gates remain open.


## Complete relationship endpoint inputs (capture contract V2)

Native owner capture now enumerates both endpoints of all relationship types for
create, property update and deletion. Borrowed same-transaction identities and
cold identity headers avoid property-body ownership; unknown identity or refused
admission retains a gap. Legacy built-in Search invalidation remains separate:
relationship properties do not cause lexical rewrites of its documents.

The checkpoint capture header advances to 2 while preserving V1 effect encoding.
Old V1/no-owner checkpoints keep canonical data and legacy records but fence V2
completeness at the checkpoint head. Source durability defaults and WAL framing
remain unchanged. See [the source argument, finite model and current native
qualification](../tla/OWNER_ENDPOINT_CAPTURE_PROOF.md). This stronger event
primitive does not confer complete fanout, mapping, stage or activation authority.


## Scoped qualified graph reads (E guard primitive)

The existing qualified physical-ID and single-hop collectors can reuse already
held complete domains. A labeled ID root needs that label's domain; an unlabeled
root needs the all-node domain. Omitting a hop's physical dependency scan needs
both endpoint domains and its relationship-type domain. Untyped edges require
the all-relationship domain. IDs and the root-count cap remain validated before
subsumption. Missing guards retain the existing precise collector and its
refusal behavior; sticky refusal cannot be cleared. Hidden adjacency predicates
and unsupported shapes retain the existing rejection. Scope lookup borrows an
ordered key-family suffix without owning query names or scanning later point
keys. Query execution and its access-path reports remain in the normal runtime;
this changes dependency collection, not Cypher execution semantics.

See [the source-shape argument and finite model](../tla/SCOPED_GRAPH_READ_PROOF.md).
This is neither a closed mapping contract nor complete C..E custody. Native
source identity/schema continuity, reverse fanout, producer/vector contracts,
stage completion, incremental final artifacts and activation remain required.


## Consumer-bound activation and recovery prerequisites (not implemented)

The current registered consumer is a resident SearchIndex lifecycle, not a
standalone immutable-manifest writer. SearchIndex::open_under_lease loads the
resident snapshot, checks its identity against the published immutable manifest,
then validates registered lexical/descriptor and standalone vector artifacts.
ConsumerProjection::read_receipt hashes the resident checkpoint; registry
acknowledgement binds that completed receipt. Generic prepared publication does
not update that resident snapshot or receipt. Its rejection of registered roots
must remain: bypassing it would pair a new immutable E manifest with an old D
resident checkpoint and fail reopening, rather than implement MVCC activation.

A consumer-bound native session therefore needs a durable generation-backed
read view and an atomic selector/receipt contract carrying binding/incarnation,
source E, operation identity and its complete artifact closure. An immutable
precomputed manifest must be selected through bounded metadata; decoding and
rewriting the full active manifest under SourceOwner is not short activation.
Serializing or rehydrating a complete resident E snapshot after E capture also
fails the incremental post-E requirement. Complete source mapping and vectors
remain separate authorities; a metadata selector is not their proof.

ConsumerProjection's lifetime publication lease prevents external writers, but
sharing that lease does not exclude its own apply/checkpoint/directory-publication
operations once preparation escapes SourceOwner. Admission needs one exclusive
operation custody path, with fast typed deferral for competing internal writes,
maintenance and cleanup. A completed receipt must be distinguished from dirty
or failed legacy checkpoints: checkpoint_uuid changes before fallible checkpoint
I/O, while an earlier receipt may remain. Do not mint preparation authority from
an arbitrary in-memory binding. Activation must revalidate source/binding/base
under the fence and return heavy candidate/retired-reader custody on both success
and rejection for disposal outside that fence. A consuming error path that drops
full artifacts under SourceOwner does not satisfy this requirement.

These are source-backed design requirements, not delivered activation APIs or a
new finite-model completion claim. The [cleanup/publication proof](../tla/SEARCH_CLEANUP_PUBLICATION_PROOF.md)
addresses a prerequisite race only. Native closed C-ready/E custody, incremental
artifacts, consumer recovery/receipts, actual Mem adoption and all original gates
remain required.
