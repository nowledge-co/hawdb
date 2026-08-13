# Skein Embedded Runtime Specification

## Scope

This specification defines the production contract for concurrent access,
durability, recovery, incremental indexes, resource scheduling, and
observability in the embedded Skein library.

Skein is an in-process library. These capabilities MUST be configured and
invoked through Rust APIs. Production correctness MUST NOT depend on a CLI,
helper process, or environment-variable control plane.

## Deployment Profiles

Skein supports two embedded deployment profiles. A profile selects conservative
defaults and capability availability; it does not change the durable storage
format or Cypher semantics.

### Desktop Bound

`DesktopBound` runs inside a desktop application process. It behaves like a
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
- Mobile and desktop profiles MUST be able to open the same format version when
  the database does not require a disabled capability.

Explicit configuration overrides MAY further lower resource limits. Raising a
mobile limit is allowed only through an explicit host decision.

### Runtime Capability Gates

Runtime capability checks are independent from resource admission. The default
capability matrix is:

| Capability | DesktopBound | MobileEmbedded |
| --- | --- | --- |
| Full-text search | enabled | enabled |
| Vector search | enabled | enabled |
| Graph analytics | enabled | disabled |
| Background maintenance | enabled | disabled |

The host MAY override this matrix through `SkeinEmbeddedOpenOptions`. Query
capabilities MUST be checked before plan-cache lookup or catalog mutation.
Background capabilities MUST be checked before QoS admission or artifact
mutation. Search capability checks MUST happen before selecting a retriever and
MUST NOT silently substitute a different search mode.

Profile-aware hosts MUST use fallible query and search entry points. A disabled
operation returns `SkeinError::CapabilityUnavailable` with a typed
`RuntimeCapability`; non-fallible search convenience methods are intended only
for hosts that keep the corresponding capability enabled. Capability settings
do not alter the durable format, WAL contract, or core Cypher semantics.

Profile compatibility MUST be verified by reopening the same durable database
in both directions. Data and schema written by `DesktopBound` remain readable
and writable through core parameterized Cypher under `MobileEmbedded`, and
mobile writes remain readable after reopening as `DesktopBound`. A profile
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
`SkeinEmbeddedOpenOptions`, `DatabaseConfig`, or `SearchIndex`; the effective
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
indexes Skein supports. The `pg_tables` and `pg_indexes` names MUST also resolve
without qualification, matching PostgreSQL's implicit `pg_catalog` lookup.
Skein MUST NOT invent unstable PostgreSQL OIDs or imply
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

Business algorithms that need several independent reads MUST keep those reads
as small named Cypher statements in the host. When all phases require one graph
version, the host MUST execute them through one `DatabaseReadTransaction` and
may record `DatabaseReadTransaction::commit_epoch` with the derived result.
Skein MUST NOT add an algorithm-specific read DTO merely to assemble those
statement outputs. PageRank planning, membership, visibility, and central-node
reads follow this rule; its grouped score-update and clear mutations remain
typed transaction contracts. GraphMeta state reads also follow this rule:
hosts MUST choose fixed property projections and bind `meta_id`, while stamp
batches and deletes remain typed for grouped WAL and mutation validation.

## Concurrency Model

Skein MUST support concurrent readers and a concurrent writer through
multi-version snapshots:

- Readers pin an immutable published snapshot.
- A writer stages changes without mutating a published snapshot.
- Commits are serialized until write-write conflict detection is specified.
- A staged snapshot becomes visible only after its WAL commit is durable.
- Existing readers continue against their pinned snapshot after publication.
- Foreground work MUST remain admissible while internal background work is
  saturated.

The initial contract is multi-reader, single-writer. Multi-writer execution is
out of scope until conflict detection, abort semantics, and index delta ordering
are modeled and tested. Multi-reader means snapshots derived from the owning
handle inside one application process; it does not permit multiple root handles
or another process to reopen the database directory.

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
threshold, not an OOM boundary, and Skein deliberately honors it as its
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

Runtime reports SHOULD expose host, quota, cpuset, effective, foreground, and
background parallelism without including host paths or secret configuration.

CPU concurrency and storage I/O depth are separate budgets. Modern SSD and NVMe
devices expose multiple queues and channels, so foreground scans MAY issue
independent segment reads concurrently up to a bounded I/O depth.

- Candidate selection and segment pruning MUST happen before issuing payload
  reads.
- Parallel reads SHOULD operate on coarse, independent ranges; the engine MUST
  avoid turning one scan into unbounded tiny random I/O.
- Foreground and background I/O MUST use separate admission budgets.
- WAL commit ordering, manifest publication, and per-index delta ordering remain
  serialized even when data reads are parallel.
- `DesktopBound` defaults SHOULD use multiple foreground I/O slots with a
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
calls and a persistent QoS scheduler. Admission applies per indivisible graph
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
MUST occur through a Rust library configuration object. Skein MUST NOT install
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
