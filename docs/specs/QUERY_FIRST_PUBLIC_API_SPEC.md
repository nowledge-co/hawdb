# Query-First Public API Specification

## Status

This specification defines the release-facing embedded API boundary. It is
normative for production builds.

## Public query boundary

Application graph behavior MUST be expressed as parameterized Cypher and
executed through `Database`, `DatabaseReadTransaction`,
`DatabaseTransaction`, `NowledgeGraphAdapter`, or the bounded
`EmbeddedGraph` query surface. Relational behavior MUST be expressed as
PostgreSQL-dialect SQL through the corresponding SQL entry points.

Idempotent ingestion uses PostgreSQL-style `INSERT ... ON CONFLICT (...) DO
NOTHING RETURNING ...`. `query_sql_with_result` exposes the statement's
provisional affected/conflict counts and returned rows; `commit_with_result`
exposes the outcomes recomputed at the durable commit boundary. These typed
result envelopes coordinate transaction state and do not replace SQL as the
mutation interface.

Strict Append tables use the same SQL boundary. The storage mode is declared
with PostgreSQL `CREATE TABLE ... WITH (...)` storage-parameter syntax; the
parameter names and values are HawDB extensions:

```sql
CREATE TABLE events (
    stream_id TEXT NOT NULL,
    sequence BIGINT NOT NULL,
    payload BYTEA NOT NULL
) WITH (
    storage_mode = 'strict_append',
    partition_key = 'stream_id',
    order_key = 'sequence'
);
```

The initial SQL contract accepts one partition-key column and one order-key
column. `INSERT` is parameterized and append-only. `UPDATE`, `DELETE`,
`ON CONFLICT`, `CREATE INDEX`, and `ALTER TABLE` fail closed for Strict Append
tables. Reads require an exact partition predicate, ascending order by the
order key, an optional exclusive lower bound on that key, and an explicit
`LIMIT`. `EXPLAIN` and `EXPLAIN ANALYZE` expose the
`StrictAppendPartitionScan` path. `system.append_tables` and
`system.append_storage` expose schema and storage-residency state without
making typed row CRUD a production integration surface.

Strict Append tables may opt into a table-wide generated order key:

```sql
CREATE TABLE generated_events (
    stream_id TEXT NOT NULL,
    sequence BIGINT NOT NULL,
    payload BYTEA NOT NULL
) WITH (
    storage_mode = 'strict_append',
    partition_key = 'stream_id',
    order_key = 'sequence',
    generated_order = 'commit_sequence'
);
```

`commit_sequence` requires one `BIGINT NOT NULL` order-key column without a
default. `INSERT` MUST omit that column; caller overrides and Strict Append
`INSERT ... RETURNING` fail closed. Values start at one and are assigned as one
contiguous, table-wide interval in caller row order at the serialized durable
commit boundary. Preparing, aborting, or rejecting a transaction consumes no
values. A transaction with pending generated rows cannot read that table,
because an order key does not exist before commit.

Generated assignments are returned only by the typed durable result:
`AppendCommitResult::mutations` for direct append commits and
`TransactionCommitResult::append_mutations` for mixed transactions. Each
`AppendMutationOutcome` preserves append-write order and caller row order.
Exhaustion fails before WAL publication as
`HawDBError::AppendSequenceExhausted`, retaining the table, prior watermark,
and requested row count as typed fields.
`system.append_tables` exposes `order_mode` and
`generated_order_watermark`; the watermark is null for caller-provided tables.
The assigned full rows and watermark are part of WAL replay and checkpoint
state, so recovery either retains an exact committed prefix or reuses values
from an uncommitted suffix.

Every application-owned read statement MUST have explicit row and payload
budgets. Multiple distinct read phases SHOULD remain separate named
statements. The host MAY normalize requests, account for budgets across
statements, and shape compatibility responses, but MUST NOT reimplement graph
scan, join, filter, sort, aggregate, or traversal semantics.

Application mutations MUST use parameterized Cypher inside
`DatabaseTransaction` when more than one statement must publish atomically.
The transaction owns read-your-own-writes behavior and the single canonical
WAL publication boundary.

## Shared request constraints

`QueryRequest` carries Cypher parameters, authorization, scoring, cancellation,
and read-output limits. `SqlQueryRequest` carries SQL parameters, cancellation,
output limits, and the relational join-planning directive. String overloads
remain convenience entry points. Synchronous database, read-snapshot, and
explicit transaction entry points MUST preserve the same request attachments;
concurrent transactions MUST retain their existing locking and read-dependency
validation. Embedded explicit transactions use the same statement kernel.

Within one explicit concurrent graph statement, initial execution and replay
after pessimistic lock acquisition MUST borrow the same parsed/optimized
preparation and bound request. Restoring the statement savepoint preserves its
schema, and the existing epoch fence MUST remain in force before replay. The
preparation ends with the statement; the next statement binds its own parameters
and prepares against the current private workspace, including staged schema.
The enclosing transaction admission remains held during lock acquisition.

Ordinary Cypher mutations through ConcurrentDatabase and the Mem handle transfer
the admitted preparation into a single-statement concurrent transaction. They
MUST acquire semantic read scopes before creating the private execution snapshot;
a write-only footprint does not protect predicates or an excluded unique candidate.
Node updates lock their label domain, or a unique candidate/internal node ID plus
a shared label-membership guard when assignments preserve unique keys. Candidate
resolution occurs under that membership guard and releases its source before node
lock waiting. Node creation/MERGE/UNWIND additionally protect ID allocation. Other
mutations and schema operations currently use a logical database scope; their
private execution still runs outside the publication owner lock. This conservative
scope is a concurrency limitation, not a claim of fine-grained relationship writes.

After acquiring scopes, execution MUST validate instance (at host boundaries),
branch, schema/ordering capabilities, configuration and request authorization/
scoring against the new workspace. Data-only commits do not invalidate preparation.
The actual staged footprint also participates in the existing lock manager before
commit. Preparation funding survives locking/staging and the admitted permit
survives the WAL/durability boundary. Non-RETURN mutations return the commit summary
with final IDs; SET RETURN retains its prevalidated statement rows. No uncertain
publication failure is retried, and cancellation after durable success cannot
replace acknowledgment. Explicit transactions retain their existing epoch fence.
Node RETURN predicates and complex node mutation predicates use the executor's
shared bounded target preflight before staging IDs. Storage property filters
MUST NOT replace three-valued expression evaluation for RETURN. EXPLAIN ANALYZE
remains diagnostic and MUST NOT enter the mutation path. Positive legacy mutation
summary row caps remain enforced independently of explicit RETURN output limits.

Ordinary SQL schema/data mutations through `ConcurrentDatabase` MUST acquire
semantic scopes before capturing their private workspace. Scope derivation uses
schema and predicate syntax only; it MUST NOT bind volatile INSERT defaults or
read old DELETE rows before waiting. Constraint-preserving UPDATE uses primary-key
ranges when derivable, otherwise a table scope. INSERT/DELETE protect the table;
foreign-key participants (including referenced parents), constraint-key updates,
DDL and Strict Append use a conservative logical database scope. Different table
inserts and independent primary-key updates can stage concurrently.

After capture, SQL MUST recompute the required scope against the fresh schema and
refuse execution unless held locks cover it. A schema-neutral template may bind a
changed schema only within that protection. No old source survives lock waiting,
and defaults are bound once into the private writes. RETURNING budgets and
cancellation are checked before publication; the existing sequencer returns the
durable outcome. Autocommit locking SELECT remains unsupported. Branch lifecycle
commands retain their catalog path.

Tokio ordinary Cypher and SQL mutations MUST use that same concurrent owner and
kernel after execution admission. Its blocking task retains the admitted permit
through queued commit/durability; no second query permit is acquired. Preparation
funding moves with the template and no planning source survives semantic lock
waiting. Data-only commits and physical checkpoints do not impose a global epoch
fence. Admission captures the database instance separately from reusable syntax;
rebuilding discarded preparation MUST NOT silently select a replacement instance.

Direct `with_embedded_mut` callbacks and resource reconfiguration exclude active
Tokio writers through an administrative gate shared by ordinary writers. These
callbacks MUST NOT reenter the same owner. They invalidate the published view
without eagerly opening a selected branch. Coordinator guards publish a complete
new immutable view before unlocking; capture failure or unwind fails closed. A
parked explicit user transaction continues to reject autocommit calls. SQL branch
commands keep their catalog path; context replacement selects a coordinator for
the new instance. Read execution and streaming callbacks hold neither the owner
mutex nor the administrative gate after snapshot capture.

Control/projection procedures retain their conservative exclusive publication
path. SQL private staging does not eliminate storage preparation at serialized
commit. Release A/B and actual owner-lock measurements remain required;
concurrency correctness alone does not establish a performance gain.

The Mem production resource-profile entry point MUST use the same detached read
admission path and consume its preparation once. Evidence identity MUST match the
execution snapshot's canonical commit epoch after admission; a data commit during
admission invalidates that evidence even though the prepared read remains reusable.
Profiling MUST preserve durable-source status, captured manifest metadata and live
shared segment-cache counters without retaining the writable storage owner. Cache
counters and process memory are shared observations; they are not per-query
attribution when other work runs concurrently. Row and payload ceilings remain
bounded by the host configuration and admission, and a failed read returns no
successful qualification report.

Request limits can tighten database limits but MUST NOT widen them. An explicit
request `Some(0)` is a zero-row or zero-payload ceiling, not an unlimited sentinel.
SQL mutation results and explicit Cypher `SET ... RETURN` results MUST be
validated before private state is installed or a commit is published. A refused
explicit transaction statement restores its savepoint without discarding earlier
successful statements. `RETURN count(...)` is charged for its single output row,
not its affected-node count; an empty projected result can satisfy a zero budget,
whereas a zero-match count still returns one row. Cypher mutation staging and
storage summaries continue to use the independent `MutationLimits` contract.

A request context MUST NOT replace an enclosing snapshot or transaction
context. Both cancellation tokens and the earlier deadline apply. CPU, executor,
working-memory and result-memory ceilings are intersected. Work budgets remain
shared, with common ancestors charged once. Cancelling a statement's combined
context does not cancel either enclosing scope. An existing dynamic memory or
I/O controller remains attached; two distinct controllers for the same resource
are rejected because they do not describe a single admission.

Consumer source mappings MUST reserve query-result capacity in addition to
publication working memory. Registered catch-up reserves the configured
per-statement payload allowance as result capacity; physical materialization
may refuse before that logical ceiling. Standalone staging and rebuild hosts
must include result capacity in their explicit generation admission. Both
Cypher and SQL source reads preserve the admitted task's cancellation and
result ceilings. A zero result reservation is not an unlimited-output sentinel;
a failed mapping cannot contribute a successful publication proof.

Prepared query reuse also checks authorization identity, including policy epoch,
and scoring-plan shape. Changed scoring coefficients, query parameters and
reference clocks MUST bind to the current execution. A mismatch uses the normal
planner rather than a plan prepared with different permissions.

`OwnedQueryRequest` and `OwnedSqlQueryRequest` own this same contract across
asynchronous admission. Their borrowed views use the synchronous statement
kernel. Tokio collection and streaming MUST retain authorization, scoring,
cancellation and output limits. `OwnedHostScoringRequest` owns a `Send` host
scorer; preparation reads its validated descriptor without invoking the scorer.
Execution borrows the scorer exclusively, and cancellation or future drop
releases it with the request. A busy or poisoned scorer fails closed. Admitted
autocommit requests MUST reject an open Embedded explicit transaction.

`DatabaseReadTransaction::into_bounded` consumes a pinned transaction and creates
one `BudgetedReadSnapshot`. All Cypher and SQL statements share its
`ReadSnapshotBudget`; per-statement limits intersect the remaining budget.
Successful owned results debit rows and logical payload bytes atomically. A
failed statement delivers no owned result and consumes no output quota. Payload
accounting charges column names per row and value payloads regardless of compact
physical schema storage. EXPLAIN charges its delivered report, not the inner
execution's row count. `ReadSnapshotUsage` counts completed statements and
delivered output. Mem snapshots use this same ledger, including their projection
visit charges, while retaining their existing positive-budget entry contract.

These request contracts do not by themselves establish execution outside outer
locks or end-to-end borrowed result delivery. Those remain separate acceptance
items in issue #1193.

## Borrowed result delivery

Borrowed consumers MUST receive views whose lifetime is bounded by the callback.
Immediate delivery may borrow producer-owned storage only while its source
reservation remains live. Bounded validated delivery MUST retain complete output
until row and payload ceilings have passed; a terminal execution or budget error
must not expose a successful prefix. Compact retained storage shares its schema
and admits copied payloads before allocation. Callback error, cancellation and
unwind MUST retire storage before releasing its result reservation.

The common accumulator accepts native columnar/projected views and exposes
validated compact views through `RowRef::Compact`. Eligible root numeric
projections, including a surrounding SKIP/LIMIT, use `RowRef::Projected` over
the existing lending, owned-input or ordered parallel-morsel scan protocol.
Projection names are admitted once, retain duplicate-alias suffixes, and follow
the same sorted schema as named maps. Payloads remain source/plan-owned during
immediate callbacks; retained output copies only admitted values into compact
segments. `native_projection_rows` records this path in execution reports.

The existing work-limit and out-of-core node-projection eligibility boundaries
remain in force. Other scalar/operator shapes still construct bindings and use
the shared accumulator. This path-specific integration does not establish
allocation or latency improvement for every query; release measurements remain
required.

## Preparation and admission ownership

Resident projection generations use `SearchIndex::read_snapshot()` to retain
immutable document pages, analyzer configuration, descriptor metadata and native
payload handles. Capturing or cloning a generation MUST NOT copy the corpus.
Updating a shared page copies document references rather than document bodies;
retired documents remain alive until their last generation is released.
Range reads keep the captured file identity through pathname replacement and
continue to validate payload checksums. A read snapshot exposes no checkpoint,
marker or Consumer mutation entrypoints.

`document_retained_capacity_bytes()` is a conservative bound for document
buffers and page metadata, including allocated directory capacity after deletion.
It is not an RSS measurement or the complete projection/query memory budget.
`retained_capacity_bytes()` additionally covers analyzer, descriptor, lexical
metadata/delta and vector owners, including complete mapped payload lengths. It
is computed once per captured generation outside the publication lock; this is
a conservative capacity bound, not an allocator or RSS measurement.

Mem bounded snapshots and read reports capture the graph/resident pair after
execution admission, release the publication mutex, and then use
`retain_with_permit` for a nonqueueing retained-memory reservation. A refused
source is dropped before the caller can wait or retry. The generation's bounded
weak cache shares funding for its current host governor; changing governors
preserves existing leases and replaces only that cache. A lease drops its source
before its memory token, and retains no execution slots. Generation reuse checks
document pages, configuration, analyzer/index owners, local marker state and
checkpoint freshness. Capture performs no filesystem reads. External edits to
marker files are observed by the live diagnostic API or after reopening; a read
generation and its query reports retain the locally captured freshness.

Read generations preserve an already captured RaBitQ index. In-memory generations
also reserve a conservative bound for all lazy projection segments, ID maps and
one serialized build workspace before returning a governed lease. The first
vector query builds the missing index once; subsequent reads reuse that generation's
cache. A writer publishes a distinct generation without changing the old cache.
The fixed bound remains charged during the lease, including unused build allowance.

Funding is conservative per generation, not per distinct COW page. Two retained
generations can therefore require close to twice the corpus bound even when most
pages are shared; admission can refuse new reads until an old lease is released.
This is an explicit availability limitation, not a claim that physical usage
necessarily doubled. Page-level shared funding needs a separate ownership design;
this implementation never silently bypasses the configured memory limit.
Standalone candidate search, hydration, retrieval and candidate-evidence helpers
also capture a published pair after admission and execute outside the owner lock.
Admission retains only instance/configuration/capability metadata. Configuration,
projection kind or required memory/I/O changes reject the admitted attempt;
ordinary data publications are accepted and captured after admission. Resident
owners use the same retained generation funding. Retrieval uses the captured
graph through the existing pipeline and intersects its memory/result ceilings
with the admitted context. Search uses the admitted vector execution context.
Hydration retains its explicit document and payload limits.

Generated graph reads use the same admitted preparation and enforce output
limits during execution. Missing, unexpected and wrongly typed parameters MUST
be rejected before preparation or admission. They additionally validate the
generated context's exact graph epoch and parameter contract against the captured
read transaction. An
ordinary data commit can preserve a prepared plan while invalidating its generated
context; that attempt MUST fail as stale. Schema guidance retains its metadata
options and constructs statistics from a detached published graph snapshot.

Plain EXPLAIN uses an immutable read view, including when it describes a write
plan. It MUST NOT execute that plan. EXPLAIN ANALYZE continues to reject every
plan requiring write access, including DDL. Read snapshots and their forks retain
the captured system query settings; per-statement hints apply relative to those
settings without mutating them. Explain output keeps its row/payload limits and
cumulative accounting.

Mem per-statement payload limits do not replace the physical result reservation.
Ordinary/generated reads use the same host result-capacity selection as Embedded
and Tokio, while execution checks the smaller logical payload limit. The host
ceiling is still bounded by database configuration and the governor; compact
container overhead may exhaust that ceiling before the logical output limit.
A caller's stricter physical reservation MUST remain effective after binding
the admitted task. This separation does not claim smaller reservations or a
concurrency improvement from admission alone.

Ordinary mutation entrypoints use the concurrent transaction kernel described
above. Query and resource reports use detached read views. Administrative and
maintenance controls retain their existing ownership contracts. The adapter
gate requires the corresponding runtime evidence in issue #1193.

Admission and execution SHOULD reuse one internally prepared statement when its
execution environment remains compatible. The prepared object MUST NOT retain
a database snapshot or generation pin across an admission wait. Reuse MUST
revalidate branch identity, schema/execution compatibility, database configuration
and system query settings; execution still applies request authorization and
cancellation. Ordinary data commits alone do not invalidate read preparation.

Mem report admission also includes the published host projection's I/O depth
when the prepared plan executes text or vector seeds. This capture retains only
capability metadata, not a source generation. A changed capability is revalidated
after admission and before snapshot execution. Plain
EXPLAIN and graph-only reports do not acquire projection I/O allowances.

Preparation memory MUST remain charged until the prepared AST and plan are
consumed or dropped. Transferring that reservation MUST release preparation's
CPU, blocking-task and I/O slots without a memory release/reacquire gap. Execution
requires a separate admission. Hard capacity checks MUST include any separately
charged memory that the caller cannot release while waiting, but MUST NOT charge
those bytes again. On any admission refusal, synchronous and queued execution
MUST drop rebuildable preparation and recompute the request before deciding
whether execution exceeds hard capacity. Reprepare only after admission under
an execution reservation large enough for planning. Rejection, cancellation,
future drop, error and panic MUST release the associated ownership. This prevents multiple prepared
queries from holding all available memory while waiting for one another.

Admitted SQL retains a parameter-neutral relational template, database instance
identity and configuration, without a source snapshot. Relational access paths
bind against the execution snapshot, so schema and parameter changes cannot
reuse stale physical bindings. A database-instance or configuration change
during admission fails the request. SQL currently admits the configured query
working-memory ceiling conservatively; this is not a physical-plan estimate.
After a retryable refusal, Tokio reparses through the originally captured
template cache under the execution reservation and still checks that original
identity before execution. SQL mutation RETURNING limits apply before commit.

Database and transaction execution borrow this parsed template in the RowPage
and Strict Append mutation compilers. Concurrent lock planning uses the same
AST when validating RowPage mutations. Compilers MUST bind the current schema
and parameters on each invocation; neither bound
rows nor volatile defaults belong in the template cache. Routing to Strict
Append MUST use the execution state's table schema. This removes parser
fallbacks without caching a schema-bound mutation or changing transaction locks.
Ordinary database dispatch also uses the same template to recognize branch
statements without opening an unloaded data runtime. Input/dependency admission
continues to enforce its syntax depth and resource limits separately.

This transition is modeled by
[`HawDBPreparedAdmission`](../tla/PREPARED_ADMISSION_PROOF.md). It does not introduce
a public prepared-statement API or a persistent snapshot cache.

## Removed business facades

Release builds MUST NOT expose application-specific entity, relationship,
Memory, Source, Skill, Thread, Label, Community, or AugmentationJob CRUD batch
methods. Release builds also MUST NOT expose `read_graph_*` route methods or
their route response DTOs.

The old adapters and route response DTOs MUST NOT remain behind `cfg(test)`.
Regression coverage MUST execute parameterized queries through the ordinary
runtime. Route-evidence builders MAY retain private query fixtures, but neither
their statements nor route-specific response shapes are public database APIs.

The route qualification statements live in the facade's private
`graph_route_evidence::queries` module. The readiness crate exposes only the
generic query-inventory and evidence contracts. Single-entity deletion
regressions use bounded parameterized query requests, including scoped
predicates, incident-edge deletion and WAL recovery. PageRank score update and
clear regressions use bounded parameterized Cypher in explicit transactions,
with host-only input normalization and no business CRUD facade or DTOs. They
retain duplicate/missing-ID behavior, projected-entity protection, one-batch WAL
and reopen checks. Community assignment clearing and cleanup also use ordinary
bounded Cypher, preserving scoped matches, NULL semantics, incident edges,
no-op epochs and one-batch WAL/reopen coverage without business adapters or DTOs.
AugmentationJob lifecycle and orphan interruption fixtures use named bounded
Cypher statements, with status selection in the query and explicit transactions
for multi-statement atomicity. Duplicate input and value validation are host
normalization, not database business rules. The old mutation adapters, graph
scans, state-machine DTOs and response DTOs are removed.
GraphMeta stamps and deletes, SchemaMigrationLog idempotent records, and
Community creation, summaries, memberships and reads also use ordinary bounded
queries. Count and page phases stay distinct; filtering, ordering, existence and
identity selection stay in Cypher. MERGE/ON CREATE preserves original timestamps
on repeated requests, and explicit transactions own the single WAL boundary.
Their old DTOs, test read wrappers and mutation adapters are removed.
Generic/scoped entity batches and Source/Skill/Thread deletion fixtures use
bounded IN predicates and DETACH DELETE, preserving projected-idless nodes and
unrelated endpoints without graph inspection in host code.
Property updates, Memory content/metadata, dedup-review timestamps, access/click
counters, decay refresh and normalized-space moves use fixed bounded SET
statements. Optional assignments use separate statements; source-space
normalization, identity matching and filtering remain in Cypher. Host helpers
only validate caller input and normalize duplicate requests. Multi-statement
mutations share an explicit transaction; repeated counter updates observe its
private workspace. These fixtures retain exact field values, missing/idless
target protection, omitted-field behavior, no-op epochs and single-batch
WAL/reopen assertions. Their business adapters and request/result DTOs are
removed, including the former shared property-statement helper.
Memory lifecycle/latest flags and EVOLVES relationship fixtures also use bounded
parameterized statements and explicit transactions. Space predicates and exact
endpoint matching stay in Cypher; host helpers only normalize input. Their three
database adapters, twelve DTOs and the last two Mem business forwarding methods
are removed, including the now-unused business-mutation admission helper. Typed
transaction, recovery, Consumer and projection lifecycle capabilities remain.
Skill usage, metadata, lifecycle and source association fixtures also use fixed
parameterized queries. Optional assignments remain separate statements; source
MERGE/ON CREATE retains original relationship properties and identity on retry.
These eleven runtime cases retain their CI owner, transaction/WAL boundaries
and reopen assertions without the four business methods or fourteen DTOs.
Thread metadata/message counts, contained-message deletion, compaction links
and identity deletion fixtures use fixed bounded parameterized statements.
Timestamp eligibility, endpoint matching, graph traversal and cascade selection
remain in query predicates; input helpers do not inspect graph state. The
fifteen runtime cases preserve transaction/WAL and reopen assertions while
removing five business methods, fifteen DTOs and four test read wrappers.
Label lifecycle, Memory label deletion and both label-transfer fixture families
use ordinary bounded queries. Candidate selection, filtering and deduplication
stay in Cypher; each selected identity is passed to an exact MERGE in the same
explicit transaction. Their twelve runtime cases preserve idless/space refusal,
existing relationship properties, no-op and one-batch recovery semantics while
removing four business methods, twelve DTOs and handwritten graph scans.
Entity-label fixture reads use a pinned public read transaction with separate
bounded entity lookup and label-page statements. Ordering, projection and LIMIT
remain in Cypher; cache and missing/empty distinctions are asserted directly
without the two business read wrappers or eight lifecycle DTOs.
Source lifecycle and metadata fixtures use fixed parameterized statements with
current-state predicates and explicit optional chunk assignments. Their six
runtime cases retain missing/idless protection, first-input normalization,
one-batch WAL and reopen assertions without two business methods or eight DTOs.
Source parsed-metadata fixtures likewise use separate base and optional-detail
statements in one transaction, preserving omitted fields, idless protection and
recovery coverage without the old adapter, four DTOs or assignment factory.
Source revision fixtures use ordinary bounded CREATE with exact external-id
endpoint predicates. Staged readback, committed edge counts and recovery retain
missing/idless guards and default properties without the business adapter, four
DTOs or relationship-request factory.
Parsed Source creation fixtures use fixed MERGE/ON CREATE queries with bounded
readback, preserving repeated/existing identities and exact ingest/default
fields without the adapter, four DTOs or typed entity-create factory.
Source label assignment/deletion fixtures use bounded MERGE/ON CREATE and
DELETE with endpoint predicates, retaining existing properties, missing/idless
guards and recovery assertions without two business methods or eight DTOs.
Source memory-count fixtures use bounded point calculations and fixed SET in
one transaction. The host binds query-computed counts; sequential/floor and
NULL behavior, nonnumeric refusal and one-batch recovery remain covered without
the business adapter, four DTOs or handwritten graph-side aggregation.
Source-reference cleanup fixtures select and delete through bounded ordinary
queries, preserving reference isolation, retained nodes and recovery assertions
without the cleanup adapter, three DTOs or graph-scan candidate collection.
Crystal source association fixtures use bounded MERGE and same-transaction
reads with external-id predicates, retaining finite numeric input checks and
idempotent durable outcomes without the business adapter or its two DTOs.
Entity create/update fixtures use fixed bounded MERGE and SET statements with
external-id predicates. Creation/match fields, first-input duplicate handling,
identity checks, read-only refusal and one-batch recovery remain explicit without
the four business adapters, ten DTOs or dynamic statement builders.
Single and batch relationship deletion fixtures use bounded DELETE statements
with endpoint, relationship-property and scope predicates. They preserve idless
protection, nodes and WAL/reopen boundaries without four mutation adapters,
seven DTOs or a dynamic statement builder.
Relationship property updates use bounded SET and readback with endpoint,
property and scope predicates. Four adapters, seven DTOs and the dynamic SET
builder disappear while assignment, idless, read-only and recovery cases remain.
Relationship CREATE/MERGE fixtures and out-of-core statistics setup now execute
bounded parameterized queries. The last eight business mutation methods, their
fourteen DTOs, test-only analytics DTO module and graph-inspection mutation
helpers are removed. Entity/property reads now use fixed bounded statements
on read snapshots.
Their six read wrappers and eleven request/response types are removed.
Crystal list, community aggregation and source visibility fixtures also use
fixed statements for projection, filtering, sorting and pagination. Their three
read wrappers and twelve DTOs are removed. Community visibility uses an optional
match and separate entity count; community memory phases use bounded aggregation
and ordering statements. Their two wrappers, ten DTOs and host joins/filters/sorts
are removed. Induced-edge fixtures use separate identity, count and ordered-page
statements, removing another wrapper and three DTOs. Relationship seed, scope
and page phases also use ordinary queries, removing two wrappers and four DTOs.
Neighbor fixtures now use fixed hop/direction/scope queries and explicit output
budgets, removing two wrappers and three DTOs. Dense adjacency is checked through
storage layout statistics and ordinary profiled reads, rather than retired
facade reason DTOs. Path/subgraph fixtures now use fixed endpoint, bounded
expansion, scope and edge-page statements, removing the final four wrappers and
their DTOs, host traversals, decoders and test-support exports. No business CRUD
compatibility layer remains under `cfg(test)`. Reusable retrieval, transaction,
recovery, Consumer and projection lifecycle contracts remain. Source cleanup
alone does not establish acceptance: issue #1193 and its PR must record
source-bound runtime, platform and local-fuzz results alongside the release
measurements and their limitations.

## Stable typed boundaries

Branch lifecycle and selection use PostgreSQL query entry points:
`CREATE BRANCH`, `DROP BRANCH`, `USE BRANCH`, `SHOW BRANCHES`, `SHOW BRANCH`, and
`SHOW CURRENT BRANCH`. `DatabaseSession` borrows the mutable database's selection;
selection survives the end of that borrow. Catalog mutations and selection are
rejected inside explicit user transactions. Shared `ConcurrentDatabase` clones
cannot use `USE BRANCH` to retarget their shared runtime. See
[the branching contract](BRANCHING_STORAGE_SPEC.md) for value binding, exact
revision/identity requests, and the remaining project-admission boundaries.

Storage-owned typed helpers implement the same lease/publication/recovery kernel;
they do not introduce a public `open_branch()` alternative control plane.

A typed API remains appropriate only when a stable kernel contract coordinates
behavior that cannot be represented safely by one query language statement.
The retained categories are:

- database open, configuration, sessions, and transactions;
- bounded streaming and query reports;
- WAL, checkpoint, recovery, doctor, and storage inspection;
- schema migration and maintenance orchestration;
- search projection, changefeed, freshness, and rebuild boundaries;
- bounded unified knowledge retrieval over canonical graph identities;
- HawDB Lightning import and canonical snapshot boundaries;
- QoS admission, readiness, qualification, and telemetry evidence.

These APIs MUST remain route-neutral. A new REST route, scheduler operation, or
business entity is not sufficient justification for a new typed database
method.

## Compatibility and formal-model impact

HawDB has no released public compatibility obligation for the removed business
facades. This is an intentional source-breaking cleanup before the first
release.

The cleanup does not change transaction, WAL, checkpoint, MVCC, lock, recovery,
or query execution semantics. Existing TLA+ safety and liveness obligations
therefore remain unchanged. Any future typed boundary that introduces a new
publication, recovery, concurrency, or admission state transition MUST update
the corresponding specification and formal model before release.

## Verification

The release and test builds MUST compile without business type modules or
`read_graph_*` methods. Required validation is:

```console
cargo clippy --workspace --all-targets --all-features -- -D warnings
cargo test --workspace --all-features
bazel test --test_output=errors //...
```


### Publication measurement boundary

Qualification builds expose fixed-memory counters for the actual
`DatabasePublicationOwner` RwLock through the owner and Mem handle. Successful
read/write acquisitions produce separate wait and hold histograms after their
guards release the lock. Wait ends just after acquisition; hold ends just before
unlock. Unlock and histogram update costs are excluded. Panicking callbacks
still contribute their completed guard; failed/poisoned acquisitions do not.
Busy try-write attempts are counted separately. Ordinary builds contain neither
clock reads nor histogram storage/updates.

Snapshots taken during traffic are non-atomic observations. Compare before and
after bucket/count/total deltas only when all measured workers have joined;
lifetime maximums cannot be subtracted to obtain a window maximum. Histogram
percentiles are bounded intervals, not exact latency samples. This owner metric
must not be replaced by concurrent sequencer phase timing. Qualification
allocation counters cover Rust global allocator requests across all threads,
not foreign heaps, allocator rounding or operation-specific ownership. Neither
these diagnostics nor correctness assertions establish a performance gain;
paired uninstrumented release workloads remain mandatory.
