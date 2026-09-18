# HawDB Embedded Runtime Specification

## Scope

This specification defines the production contract for concurrent access,
durability, recovery, incremental indexes, resource scheduling, and
observability in the embedded HawDB library.

HawDB is an in-process library. These capabilities MUST be configured and
invoked through Rust APIs. Production correctness MUST NOT depend on a CLI,
helper process, or environment-variable control plane.

## Deployment Profiles

HawDB supports two embedded deployment profiles. A profile selects conservative
defaults and capability availability; it does not change the durable storage
format or Cypher semantics.

### Desktop Bound

`SharedHost` runs inside a host application process on a desktop or server that HawDB shares with other workloads. It behaves like a
local MySQL or Neo4j data engine from the application's perspective, but its
lifecycle, identity, configuration, and telemetry remain owned by the host
application.

- The host opens one database handle and shares it across application workers.
- A durable database directory MUST be owned by exactly one active root handle
  and one application process. Duplicate opens in the same process and opens by
  another process MUST fail immediately. Read transactions and diagnostics MUST
  use the owning handle rather than reopening the directory.
- Foreground reads may use the effective CPU budget.
- Plan cache, FTS, vector candidate indexes, graph analytics, and bounded
  background maintenance may be enabled.
- Expensive capabilities remain explicitly bounded and may be disabled.
- The library MUST NOT expose a production CLI control plane or start a helper
  database process.

### Mobile Embedded

`MobileEmbedded` runs like SQLite inside a mobile application process. Its
defaults MUST prefer bounded memory, bounded result sets, low background
parallelism, and predictable battery and thermal behavior.

- Canonical graph storage, WAL recovery, checksums, incremental base indexes,
  parameterized Cypher, and transactions remain mandatory.
- ACL, graph analytics, approximate vector indexes, large plan caches,
  continuous compaction, and continuous telemetry export MAY be compiled out or
  disabled.
- Disabling an optional capability MUST return a typed capability-unavailable
  error. It MUST NOT silently use an unbounded fallback.
- Mobile and shared-host profiles MUST be able to open the same format version when
  the database does not require a disabled capability.

Explicit configuration overrides MAY further lower resource limits. Raising a
mobile limit is allowed only through an explicit host decision.

### Runtime Capability Gates

Runtime capability checks are independent from resource admission. The default
capability matrix is:

| Capability | SharedHost | MobileEmbedded |
| --- | --- | --- |
| Full-text search | enabled | enabled |
| Vector search | enabled | enabled |
| Graph analytics | enabled | disabled |
| Background maintenance | enabled | disabled |

The host MAY override this matrix through `HawDBEmbeddedOpenOptions`. Query
capabilities MUST be checked before plan-cache lookup or catalog mutation.
Background capabilities MUST be checked before QoS admission or artifact
mutation. Search capability checks MUST happen before selecting a retriever and
MUST NOT silently substitute a different search mode.

Profile-aware hosts MUST use fallible query and search entry points. A disabled
operation returns `HawDBError::CapabilityUnavailable` with a typed
`RuntimeCapability`; non-fallible search convenience methods are intended only
for hosts that keep the corresponding capability enabled. Capability settings
do not alter the durable format, WAL contract, or core Cypher semantics.

Profile compatibility MUST be verified by reopening the same durable database
in both directions. Data and schema written by `SharedHost` remain readable
and writable through core parameterized Cypher under `MobileEmbedded`, and
mobile writes remain readable after reopening as `SharedHost`. A profile
switch does not migrate or rewrite the storage format. Queries that require a
disabled optional capability fail before planning or mutation without
invalidating the shared database.

### Build-Time Capability Gates

The default Cargo build enables `full-text-search`, `vector-search`,
`graph-analytics`, and `background-maintenance`. A constrained host uses
`--no-default-features` and adds back only required capabilities, for example:

```text
cargo build --no-default-features --features full-text-search,vector-search
```

Build-time availability is an upper bound on runtime configuration. A host
cannot re-enable a capability omitted from the build through
`HawDBEmbeddedOpenOptions`, `DatabaseConfig`, or `SearchIndex`; the effective
set is `requested AND compiled`. Parser and plan contracts remain present so an
unavailable query produces the same typed `CapabilityUnavailable` error rather
than an unknown-syntax error. Core storage, WAL, recovery, parameterized Cypher,
and incremental base indexes are never optional features.

## Public Query Surface

Application graph reads and writes MUST use parameterized Cypher through an
admitted embedded query entrypoint. Catalog and operational introspection MUST
use bounded read-only SQL over `system.*` tables when the information is
representable as rows. Catalog tables are `system.tables`, `system.properties`,
`system.indexes`, and `system.constraints`. Runtime and graph introspection use
`system.runtime_status`, `system.runtime_capabilities`,
`system.graph_statistics`, `system.projected_graphs`, and
`system.search_projection_changefeed`. Observability tables are
`system.plan_cache`, `system.slow_queries`, and `system.statement_summary`.
Relational schema discovery SHOULD use the PostgreSQL-compatible read-only
`information_schema.tables`, `information_schema.columns`,
`pg_catalog.pg_tables`, and `pg_catalog.pg_indexes` views. Their column names,
nullability markers, PostgreSQL type names, index presence, and index
definitions MUST follow PostgreSQL conventions for the relational types and
indexes HawDB supports. The `pg_tables` and `pg_indexes` names MUST also resolve
without qualification, matching PostgreSQL's implicit `pg_catalog` lookup.
HawDB MUST NOT invent unstable PostgreSQL OIDs or imply
server-internal catalog semantics that the embedded runtime does not provide.
Every virtual catalog table is subject to the configured row and payload
budgets; exceeding a budget fails the statement instead of returning partial
introspection state.

The live database, a database transaction, and a pinned read transaction MUST
execute the same virtual catalog SQL surface against their respective catalog
snapshots. A database transaction MUST expose transaction-private relational
DDL through the compatibility views. A read transaction MUST NOT expose a
second route-shaped catalog getter surface that can drift from SQL filtering,
projection, ordering, row limits, or payload limits.

A typed Rust operation remains appropriate only when it owns a stable contract
that cannot be represented safely by one statement, including grouped WAL
atomicity, recovery, runtime admission, projection generation publication, or a
bounded multi-statement workflow. New single-query route wrappers and their
request/output DTOs MUST NOT be added to the embedded facade.

An App workflow that combines Cypher and PostgreSQL reads MUST use
`NowledgeMemEmbeddedStoreHandle::with_bounded_read_snapshot`. Its report MUST
identify the pinned commit epoch, the original and remaining row and payload
budgets, and the number of successfully completed Cypher and SQL statements.
A successful external vector seed increments a separate execution counter;
projection presence alone is not evidence that a search statement consumed the
projection.
A statement contributes to those counters only after its output has been
charged to the cumulative budget. Qualification for a mixed workflow MUST fail
closed when either required statement class is absent; a graph-only query
report cannot stand in for the relational half of the workflow. The report
MUST NOT retain query text, parameters, result rows, or payload values.
`NowledgeMemReadSnapshotReport::json` emits the versioned
`hawdb-nowledge-mem-read-snapshot-report-v1` representation of exactly those
redacted fields; it is evidence input and does not claim route readiness by
itself.

Business algorithms that need several independent reads MUST keep those reads
as small named Cypher statements in the host. When all phases require one graph
version, the host MUST execute them through one `DatabaseReadTransaction` and
may record `DatabaseReadTransaction::commit_epoch` with the derived result.
HawDB MUST NOT add an algorithm-specific read DTO merely to assemble those
statement outputs. PageRank planning, membership, visibility, and central-node
reads follow this rule; its grouped score-update and clear mutations remain
typed transaction contracts. GraphMeta state reads also follow this rule:
hosts MUST choose fixed property projections and bind `meta_id`, while stamp
batches and deletes remain typed for grouped WAL and mutation validation.

## Concurrency Model

HawDB supports in-process snapshot MVCC. The term MVCC in this specification
means immutable published snapshots, copy-on-write transaction workspaces, and
commit-epoch visibility. It does not mean PostgreSQL-style tuple version
chains, arbitrary historical `AS OF` reads, or serializable snapshot
isolation.

The `Database` facade supports one mutable owner with any number of pinned read
transactions. `ConcurrentDatabase` additionally permits multiple transactions
to prepare concurrently and provides optimistic and pessimistic coordination.
Both facades share these publication rules:

- Readers pin an immutable published snapshot.
- A writer stages changes without mutating a published snapshot.
- Durable commit decisions and snapshot publication are serialized.
- A staged snapshot becomes visible only after its WAL commit is durable.
- Existing readers continue against their pinned snapshot after publication.
- Foreground work MUST remain admissible while internal background work is
  saturated.

An optimistic concurrent transaction begins from a private copy-on-write
snapshot. At commit it acquires the database-wide exclusive publication span
and applies first-committer-wins validation against its base commit epoch. The
validation is intentionally coarse: any intervening write makes a non-empty
optimistic transaction stale, even when the two write sets are disjoint.

A pessimistic concurrent transaction obtains locks before statement execution.
Supported relational primary-key lookups and inserts may use shared or
exclusive point/range spans. Statements whose complete access span cannot be
derived conservatively use a database-wide lock. Graph mutations are first
staged in a COW statement workspace to derive concrete node, relationship,
allocation, node-delete-guard, and typed adjacency identities. The workspace is
then restored, the normalized lock set is acquired, and the statement is
replayed under those locks. Label and relationship-type locks prevent a stale
snapshot from bypassing constraint validation while disjoint entity locks
still permit concurrent writes. Schema mutations and unsupported access shapes
retain the database-wide fallback. Ordinary graph reads remain pinned snapshot
reads and acquire no logical lock.

Because graph lock identities are derived from the pinned snapshot's concrete
matches, HawDB MUST NOT refresh that snapshot after derivation. If publication
advances before a newly derived graph lock set is admitted, the transaction
fails closed and must be retried. Property writes covered by a uniqueness
constraint acquire an exclusive label or relationship-type lock; ordinary
property writes use shared constraint-subject coverage and retain disjoint
entity concurrency.

Every graph mutation statement owns a graph-workspace and lock-table savepoint;
read-only statements do not construct an unused graph snapshot. An execution
error or bounded lock timeout restores both mutation savepoints, preserving
earlier successful statements and their locks. Deadlock and lock-budget errors
abort the transaction and release every lock because continuing after either
failure would violate the bounded-resource or wait-for-graph contract. Lock
waits are bounded, and abort or drop releases all owned locks and dependencies.
These mechanisms provide the documented lock compatibility and publication
invariants; they MUST NOT be advertised as a general serializable isolation
level.

A transaction-private graph and relational workspace provides read-your-own-
writes. Graph and relational mutations publish atomically in one commit. A
read-only transaction fixes both its logical `visible_commit_epoch` and the
physical checkpoint generation beneath that epoch. Later commits or
checkpoints do not change that pinned identity. Reader pins participate in the
safe reclamation watermark for obsolete generations.

The supported boundary remains one active root handle per database path inside
one application process. `ConcurrentDatabase` is a cloneable coordinator over
that one root; it does not authorize another root handle or process to open the
same directory for writes. Multi-process writers and historical time-travel
queries remain out of scope.

### Concurrency Evidence Map

| Contract | Implementation owner | Required evidence |
| --- | --- | --- |
| Logical visibility is independent of the checkpoint base | `PublishedReadView`, `Database::begin_read_transaction` | `published_read_view_separates_logical_visibility_from_physical_generation` |
| A pinned reader keeps its original snapshot across later commits | `GraphStore::snapshot`, `ReaderPin` | `read_transaction_keeps_snapshot_before_later_commit` |
| Reader pins delay obsolete-generation reclamation | `ReaderPins`, `GraphStore::storage_reclamation_watermark` | `read_transaction_pins_checkpoint_manifest_until_drop`, `out_of_core_reader_pin_retains_its_canonical_generation_until_drop` |
| Optimistic writers use coarse first-committer-wins validation | `ConcurrentDatabaseTransaction::commit`, `GraphStore::commit_mutation_transaction_and_relational` | `optimistic_transactions_prepare_in_parallel_and_reject_the_stale_committer` |
| Pessimistic point/range locks preserve compatibility | `LockManager`, `LockTable` | `disjoint_primary_key_point_locks_allow_both_pessimistic_writers_to_commit`, `shared_primary_key_range_blocks_phantoms_but_not_the_excluded_boundary` |
| Graph entity, uniqueness-subject, and adjacency locks preserve constraints and endpoint lifetime while allowing disjoint writes | `GraphMutationTransaction::lock_footprint_since`, `graph_lock_requests` | `disjoint_graph_node_updates_can_stage_concurrently`, `graph_unique_property_updates_serialize_by_constraint_subject`, `relationship_creation_conflicts_with_endpoint_delete_guard`, `graph_create_allocation_lock_prevents_duplicate_physical_ids` |
| Graph access-set derivation never refreshes to a different snapshot | `ConcurrentDatabaseTransaction::acquire_graph_statement_locks` | `graph_lock_derivation_rejects_a_changed_snapshot` |
| A failed graph statement restores only its workspace and lock delta | `GraphMutationSavepoint`, `LockSavepoint` | `failed_graph_statement_restores_workspace_and_statement_locks`, `graph_lock_failure_restores_the_failed_statement_only`, `statement_savepoint_restores_replaced_lock_and_budget` |
| Deadlock victims terminate and release dependencies | `WaitForGraph`, `ConcurrentDatabaseTransaction::abort_after_lock_failure` | `point_lock_upgrade_cycle_selects_one_deadlock_victim`, `wait_for_graph_detects_a_cycle_with_multiple_blockers` |
| Uncommitted work is private and a durable commit becomes visible atomically | `DatabaseTransactionState`, `CommitSequencer` | `optimistic_transaction_reads_its_private_workspace`, `HawDBTransactionConcurrency.tla` |

The implementation-to-model mapping and release model-check requirements live
in `docs/tla/README.md`. A change to any row in this table MUST update its Rust
evidence and formal model in the same pull request.

## Runtime Resource Budget

The default concurrency budget MUST be derived from the smallest known limit:

1. `std::thread::available_parallelism`;
2. cgroup v2 `cpu.max` when present;
3. the effective or inherited cgroup v2 cpuset when present.

Linux cgroup v2 discovery MUST resolve the unified process path against the
cgroup2 mount root from `/proc/self/mountinfo`; it MUST NOT assume the hierarchy
is mounted directly below `/sys/fs/cgroup`. Memory sizing uses `memory.max`,
`memory.high`, `memory.current`, and derived headroom. `memory.max` is the
kernel hard limit; `memory.high` is the kernel's throttle-and-reclaim
threshold, not an OOM boundary, and HawDB deliberately honors it as its
policy ceiling. Either MAY therefore bound the stable admission capacity
(the smaller wins), while the dynamic admission budget additionally tracks
derived headroom. A request above the stable capacity is rejected
non-retryably; a request above only the dynamic budget is a transient
shortage and MUST be reported retryable. Stable means independent of current
headroom within one resource snapshot, not immutable across policy changes:
a refresh MUST recompute capacity after `memory.max` or `memory.high` changes.
Shrinking capacity MUST NOT revoke active permits. It MAY temporarily leave
the governor overcommitted, in which case additional positive memory
reservations remain blocked until active work releases enough memory. A
waiting request that no longer fits the refreshed capacity MUST terminate
non-retryably on its next admission poll. `max` does not constrain
the host limit. If an enabled controller has an unreadable or invalid quota,
cpuset, memory limit, or usage, runtime detection MUST fail closed to one CPU or
zero memory admission instead of using host-wide resources.

Cgroup v1 resource controllers are explicitly unsupported. A v1-only hierarchy,
or a hybrid hierarchy that assigns CPU, cpuset, or memory to v1, MUST fail closed
and MUST NOT qualify as a production runtime.

Explicit library configuration MAY lower or raise derived defaults. Foreground
requests MAY use the effective CPU budget. Internal background tasks MUST use a
separate conservative budget and pass QoS admission. Background saturation MUST
NOT reject an explicit foreground request.

`SharedHost` defaults reserve most process memory for the host application.
Its stable HawDB capacity is one quarter of the effective host or cgroup policy
ceiling, and its dynamic budget is further bounded by one quarter of sensed
headroom. On an 8 GiB machine this yields at most 2 GiB of automatic HawDB
capacity and typically 1--2 GiB of dynamic budget as host headroom changes. The
budget MAY fall below 1 GiB under pressure; 1 GiB is not a floor. The fraction
is a conservative default, not a universal limit: explicit host configuration
and cgroup policy remain authoritative. A separately configured 512 MiB HawDB
profile is a supported low-memory capability target, not the default ceiling
or a minimum required machine size.

Runtime reports SHOULD expose host, quota, cpuset, effective, foreground, and
background parallelism without including host paths or secret configuration.

CPU concurrency and storage I/O depth are separate budgets. Modern SSD and NVMe
devices expose multiple queues and channels, so foreground scans MAY issue
independent segment reads concurrently up to a bounded I/O depth.

Every admitted operation declares both an I/O slot count and a reservation
scope. A task-scoped reservation occupies those slots for the operation's full
lifetime and is the conservative default for storage paths that do not yet
expose explicit I/O waves. A wave-scoped reservation is checked for static
feasibility at task admission, then acquired and released around each actual
storage wave. `SourceSegmentScan` MUST use wave-scoped reservations, carry its
admitted priority and maximum depth through `RuntimeTaskContext`, and acquire
the declared number of slots before `SegmentReadExecutor` schedules a wave.
The foreground and background counters therefore bound the sum of task-scoped
reservations and live wave-scoped reads independently for each priority class.

- Candidate selection and segment pruning MUST happen before issuing payload
  reads.
- Parallel reads SHOULD operate on coarse, independent ranges; the engine MUST
  avoid turning one scan into unbounded tiny random I/O.
- Foreground and background I/O MUST use separate admission budgets.
- WAL commit ordering, manifest publication, and per-index delta ordering remain
  serialized even when data reads are parallel.
- `SharedHost` defaults SHOULD use multiple foreground I/O slots with a
  bounded upper limit. `MobileEmbedded` defaults MUST use a lower depth.
- The host MAY override I/O depth using device-specific knowledge. The library
  MUST NOT infer a precise hardware queue count from CPU count alone.

Device discovery is path-specific and evidence preserving:

- Linux MAY read the database path's block-device `rotational` and
  `nr_requests` sysfs attributes. A partition must inherit evidence only from
  its actual parent block-device queue.
- Apple platforms MAY classify memory, network, or virtual filesystems. APFS,
  HFS, or another filesystem name does not prove SSD media and MUST remain
  `Unknown` unless the host provides native device evidence.
- Unsupported or unreadable platform metadata yields an `Unknown` device with
  conservative I/O depth. It MUST NOT fall back to a CPU-derived channel count.
- Runtime reports expose only typed media, discovery source, and bounded queue
  hints. Device names, mount paths, and database paths are not included.
- An explicit host profile or I/O budget takes precedence over discovery.

Future async or platform-specific backends MAY use `io_uring`, IOCP, or native
mobile APIs behind the same bounded storage-facing contract. Correctness MUST
not depend on a specific async runtime.

## Commit Durability

The default durability policy is `SyncOnEveryWrite`.

A successful mutation response provides the following guarantee:

> After the response is returned, every mutation in the committed request can
> be recovered after process or machine failure, subject to the guarantees of
> the underlying filesystem and storage device.

The commit order MUST be:

1. validate and stage the complete mutation batch;
2. append one checksummed WAL commit record;
3. flush and synchronize the WAL data;
4. synchronize the parent directory when the WAL file is first created;
5. publish the new snapshot and commit epoch;
6. update rebuildable in-memory projections;
7. return success.

If any operation before publication fails, the staged snapshot MUST NOT become
visible and success MUST NOT be returned. A request that has not returned MAY
be absent after recovery or may be replayed if its complete durable commit
record exists.

`SyncOnCheckpoint` MAY remain available as an explicit relaxed policy, but it
MUST NOT be the embedded default and MUST be reported as non-production-safe
for the response durability contract.

## Recovery And Repair

Every canonical durable artifact MUST carry a version and checksum. Recovery
MUST distinguish:

- a torn or incomplete WAL tail;
- a checksum mismatch in the middle of the WAL;
- a corrupt checkpoint or manifest;
- a corrupt rebuildable index or projection.

Automatic repair is allowed only when the correct state is derivable:

- truncate a torn WAL tail after the last complete committed record;
- rebuild an index or projection from canonical graph state;
- restore a checkpoint from a separately validated previous generation and
  replay the validated WAL suffix.

Middle-of-log corruption and canonical-state ambiguity MUST fail closed.
Corrupt files SHOULD be quarantined with a non-sensitive generated identifier.
Repair reports MUST record the decision, recovered commit epoch, discarded
tail length, and rebuilt artifact kinds without exposing database contents or
absolute paths.

## Incremental Indexes

Canonical graph mutations and index deltas MUST share a commit epoch. Each
incremental index MUST persist:

- format and schema version;
- source graph commit epoch;
- last completely applied delta epoch;
- document or key-space identity;
- checksum.

An index MAY lag canonical state. It MUST NOT claim freshness beyond its durable
watermark. Query planning MAY use a lagging index only when a residual path
preserves correctness; otherwise it MUST catch up or use a canonical scan.

The graph-to-search changefeed uses the graph commit epoch as its stable
mutation identity. One retained changefeed record represents one indivisible
graph commit. Checkpointed records and WAL-only suffix records MUST reconstruct
the same ordered stream after restart. The typed changefeed status MUST expose
the current graph epoch, the earliest resumable source epoch, retained mutation
bounds, and whether the stream is restart-recoverable. A host MUST block
incremental catch-up and request a rebuild when its durable search watermark is
older than the resumable floor.

A host-owned background loop MUST consume the changefeed through bounded library
calls and the persistent database-owned QoS scheduler. Admission applies per indivisible graph
commit batch. A successful batch includes search delta application and a durable
search checkpoint before its permit completes. Deferred or rejected admission
MUST return a typed stop reason without applying the batch; exhausting the
caller batch budget MUST remain distinguishable from reaching the graph
watermark.

ANN, quantized vectors, FTS, BM25, property indexes, segment summaries, and
membership filters are rebuildable. Their corruption MUST NOT make canonical
graph data unrecoverable.

## Runtime Status

The process-lifetime embedded handle MUST expose one typed runtime status
snapshot. The snapshot MUST be collected under one store read guard so graph
epoch, changefeed bounds, and search projection watermarks describe the same
observable state.

The status includes:

- the current graph commit epoch;
- changefeed resume floor, retained mutation bounds, retained count, and
  restart-recoverable state;
- search projection document count, applied source epoch, durable source epoch,
  uncheckpointed state, rebuild and repair markers, and embedding identity;
- derived durable projection lag and staleness.

The status MUST NOT expose database paths, query text, properties, embeddings,
credentials, or host secrets. Hosts MAY combine it with the sanitized open
report and route ownership in their own health endpoint. A configured path or
compiled feature is not evidence that a runtime opened successfully. A host
MUST keep cutover readiness false when the process-lifetime handle is absent,
the runtime status cannot be read, the required projection is stale, a rebuild
or repair marker is active, or the changefeed is not restart-recoverable.

## OpenTelemetry

OpenTelemetry support MUST be optional and disabled by default. Enabling it
MUST occur through a Rust library configuration object. HawDB MUST NOT install
or replace the process-global tracing subscriber implicitly.

The first instrumentation surface SHOULD include:

- query parse, optimize, execute, and result shaping;
- WAL append and synchronization;
- checkpoint publication and recovery;
- index delta application and rebuild;
- background admission, defer, execution, and completion.

Attributes MUST be low-cardinality by default. Raw query text, property values,
embeddings, file paths, tokens, and credentials MUST NOT be exported. Query
fingerprints, operator kinds, commit epochs, row counts, byte counts, durations,
and structured error codes MAY be exported.

Exporter failure MUST NOT fail a database request. Export queues and batch sizes
MUST be bounded so observability cannot exhaust memory.

## Access Control Extension

ACL is an optional, non-default production capability. A build or deployment
that does not enable ACL does not claim an authorization boundary. When ACL is
compiled and enabled, its graph and search entrypoints MUST:

- remain disabled by default for `MobileEmbedded`;
- support compile-time exclusion for applications that do not need it;
- bind authorization context to planning and execution, not only API handlers;
- bind authorization decision values for every execution, including plan-cache
  hits; cached plan identity includes the policy epoch and visibility predicate
  shape but not allowed scope values;
- preserve fail-closed behavior for unsupported or stale policy state;
- avoid storing credentials or allowed scope values in cached plans, logs, or
  telemetry.

Storage-level visibility enforcement is required before ACL can be declared
complete. Parser-only or result-filtering implementations are insufficient.

Production readiness for an ACL-enabled deployment MUST prove policy freshness,
plan-cache isolation by policy epoch and predicate shape, execution-time value
rebinding on cache hits, pre-materialization visibility enforcement, and
redacted telemetry. Missing or stale policy state MUST fail closed.

## Verification

Release validation MUST include:

- model checking of durable-before-publish and pinned-reader invariants;
- concurrent reader/writer and serialized-writer tests;
- crash-point tests before append, after append, after sync, and after publish;
- torn-tail, checksum-corruption, checkpoint-fallback, and derived-index rebuild
  tests;
- cgroup quota and cpuset parser tests;
- incremental index catch-up and stale-watermark tests;
- OpenTelemetry disabled, bounded-export, redaction, and exporter-failure tests.

The complete production evidence and cross-platform release contract is defined
by `PRODUCTION_READINESS_SPEC.md`.
