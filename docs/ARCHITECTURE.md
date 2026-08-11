# Skein Architecture

## Goal

Skein is an embedded Rust graph database for Nowledge local runtimes. It is
intended to replace the current Ladybug/Kuzu dependency while preserving the
Cypher-facing behavior that Nowledge relies on today.

The production concurrency, durability, repair, incremental-index, resource
budget, and OpenTelemetry contracts are defined in
[`specs/EMBEDDED_RUNTIME_SPEC.md`](specs/EMBEDDED_RUNTIME_SPEC.md). Architecture
changes in those areas MUST preserve that specification or update it in the
same change.

The first product target is not a general Neo4j clone. The target is the
Nowledge graph data plane:

- embedded database open/create by path
- schema DDL for node labels and relationship types
- Cypher reads and parameterized queries
- transactional graph mutations
- crash-safe local persistence
- deterministic query planning and explain output
- graph projection hooks for rebuildable analytics

Cloud remains PostgreSQL-first. Skein is the local embedded graph engine and can
share logical semantics with Cloud projections, but Cloud canonical state should
continue to live in PostgreSQL facts, edges, jobs, and op-log tables.

## Non-Goals

- Replacing PostgreSQL in Nowledge Cloud.
- Implementing the entire openCypher surface in the first milestone.
- Storing vector embeddings inside the graph engine.
- Providing a distributed graph database.
- Treating graph analytics as canonical state.

## Compatibility Boundary

Nowledge currently uses the Ladybug fork through the local graph wrapper. Skein
must cover the used surface before it can replace that dependency:

- database lifecycle with configurable memory, thread, size, read-only, and
  recovery behavior
- Cypher `MATCH`, `RETURN`, `WHERE`, `ORDER BY`, `LIMIT`, and parameter binding
- DDL for node and relationship tables
- `CREATE`, `MERGE`, `MATCH SET`, `DELETE`, and `DETACH DELETE`
- ACID transactions with explicit begin, commit, and rollback
- checkpoint and WAL recovery
- storage compatibility/version checks
- projected graph lifecycle and Kuzu-style algorithm procedure entry points
  for `project_graph`, `page_rank`, and `louvain`

Arrow integration is not part of the replacement boundary. Nowledge stores graph
identity and scalar properties in the graph database, while vector search remains
outside the graph engine.

## Crate Layout

Skein follows a RisingWave/Chryso-style workspace-and-facade layout. The root
crate remains the stable embedded facade, while implementation crates are split
out as interfaces harden and dependency direction becomes acyclic. The current
crate split includes `skein-core` for common graph primitives,
`skein-cypher` for syntax-only Cypher AST/parser support, `skein-qos` for
local resource classes, background admission, and expected-value ranking,
`skein-plan` for Cypher logical/physical IR, typed phase roots, deterministic
fingerprints, explain rendering, and plan-node metadata, and `skein-optimizer`
for Cascades primitives plus graph-specific catalog, costing, access-path, and
lowering logic. `skein-analytics` owns the storage-neutral immutable CSR/CSC
kernel and deterministic PageRank/Louvain implementations. `skein-evidence`
owns release identity validation and storage crash-recovery evidence contracts.

```text
crates/
  core/                errors, values, ids, catalog names, schema descriptors
  analytics/           immutable CSR/CSC projections and graph algorithms
  evidence/            release identity and crash-recovery evidence contracts
  plan/                logical/physical IR, phase roots, explain, fingerprints
  qos/                 work classes, local admission, background ranking
  optimizer/           Cascades memo/rules/search plus graph cost and lowering
  cypher/              token cursor, parser, AST, parameter model
  storage/             storage protocols, durable primitives, MVCC, indexes
  executor/            physical operators and query execution
  fuzz/                development-only differential oracles and replay bundles
  qualification/       synthetic revision-bound CI qualification workloads
  api/                 stable embedded API facade
```

The public facade should stay in the root `skein` crate. Internal crates should
be allowed to evolve while the embedded API stays small and stable.
`src/cypher.rs`, `src/planner.rs`, and `src/optimizer.rs` are compatibility
re-export facades over their owning crates. `skein-plan` depends only on
`skein-core`, `skein-cypher`, and `skein-ddl`; `skein-optimizer` depends inward
on `skein-plan` and remains free of executor and storage implementations.

`src/analytics.rs` is the compatibility facade and the sole adapter from the
root `GraphStore` to `skein-analytics::ProjectionSource`. The analytics crate
depends inward on `skein-core` and storage record types, but never on the root
database, WAL, query executor, or embedded runtime. This keeps projection scan
and recovery ownership in the embedding layer while making the algorithm
kernel reusable over immutable snapshots.

`src/production_evidence.rs` and `src/crash_recovery_evidence.rs` remain public
compatibility facades over `skein-evidence`. The evidence crate depends only on
`skein-core` plus serialization, so qualification and recovery tools can share
one fail-closed protocol model without depending on the root database runtime.
Blocker-code calculation stays inside that contract instead of becoming a
second public readiness implementation in the facade.

`src/qos.rs` is also a compatibility re-export facade over `skein-qos`. The
QoS crate owns local foreground/background work classes, admission decisions,
background hints, expected-value ranking, and scheduler state. It must remain
free of graph storage, search index, planner, and executor dependencies so
resource policy can be reused by projection, import, schema maintenance, and
retrieval loops without creating ownership cycles.

The storage split is intentionally transitional. `skein-storage` owns reusable
durability primitives, WAL group accounting, immutable snapshot coordination,
canonical segment formats, relational state, and storage indexes. The root
`src/store.rs` still owns the concrete `DurableStore` adapter because checkpoint
encoding currently depends on the root catalog, graph statistics, WAL operation
model, and telemetry facade. New protocol state must move inward to
`skein-storage`; the concrete adapter should move only after those dependencies
have stable inward-facing contracts. This avoids presenting the root facade or
the internal storage crate as an accidental second production API.

Cypher exposes lightweight runtime resource intent through session-scoped
system variables instead of query-shape-specific typed APIs.
`SET system.work_priority`, `SET SYSTEM VARIABLE work_priority`,
`SET system.work_class`, `SET SYSTEM VARIABLE work_class`,
`SET system.estimated_operations`, and
`SET SYSTEM VARIABLE estimated_operations` configure the current query
`WorkRequest` mapping. Per-query `CYPHER system.*` prefixes can override those
variables for one statement without mutating the session or database defaults.
`DatabaseSession::explain_query` and `skein explain-json` surface the effective
`WorkRequest` so callers can audit scheduling intent beside optimizer evidence.
`ExplainOutput` and `ExplainAnalyzeOutput` also implement `Display` with a
TiDB-style tree table. `skein explain` and `skein explain-analyze` print that
table directly, while the existing JSON commands remain the stable
machine-readable artifact path. Per-operator row estimates and runtime rows are
rendered as `N/A` until the executor measures them; root estimates, root output
rows, blocking-operator memory/spill reports, RSS, and page faults are emitted
only from existing typed measurements.
`DatabaseConfig::execution_memory` is the admission boundary for executor-owned
state. Transfer batches have row and byte limits; `DISTINCT`, Cartesian build
sides, aggregate state, path frontiers, expansion candidates, and materialized
fallbacks fail before row publication when their resident estimate exceeds the
blocking-operator budget. Sort, grouped aggregate, and TopN use bounded
two-way external merge passes with cumulative spill-byte and spill-run limits.
Spill files are query-scoped and removed on both success and failure.
Hard limits and background admission remain owned by `DatabaseConfig`,
`LocalQosPolicy`, and caller-owned schedulers.
These variables are runtime state only: they do not write WAL, are rejected
inside graph transactions and read snapshots, and are meant to guide resource
scheduling rather than change query semantics.

The `skein-fuzz` package is intentionally outside the production dependency
graph. Its state-aware generator creates a deterministic graph before selecting
valid query shapes and typed predicates. The plan-differential oracle applies
the mutations once, pins one read snapshot, and compares memo planning with
direct fallback through the statement-scoped
`CYPHER system.optimizer_search` hint. Plan-shaping hints are typed, fail
closed, and bypass the plan cache. The fallback is a differential surface, not
a correctness authority. A Graph TLP oracle independently checks that a query
result equals the bag union of its predicate-true, predicate-false, and
predicate-null partitions on the same snapshot. This supplies a semantic
relation without maintaining a second Cypher or graph executor. A separate
Graph TLP Aggregate oracle applies `count(variable)` to the original and three
partition queries, then requires the original count to equal their checked
sum. Plan-fingerprint novelty remains coverage telemetry and never changes an
oracle verdict. The same campaign generates PostgreSQL-style relational tables
with primary keys, nullable scalar columns, optional indexes, and parameterized
inner/left joins. Its predicate shapes cover scalar comparisons, `IN`, column
comparisons, and nullable boolean composition, with a non-empty unknown
partition for every shape. SQL row TLP and `COUNT(*)` TLP Aggregate execute
through the public embedded SQL API on one pinned snapshot and retain
independent typed replay and fresh-state setup reduction. Failures include
exact typed replay data, a direct `--case-index` reproduction command, and an
oracle-specific reduced mutation sequence; `src/nowledge_fuzz.rs` remains a
readiness smoke rather than a semantic oracle.

The `skein-qualification` package is also outside the production dependency
graph. It owns synthetic, revision-bound CI workloads that exercise the public
embedded facade without turning qualification orchestration into a production
API. Its mixed-runtime soak creates a controlled larger-than-memory fixture,
reopens it through `SkeinTokioEmbedded`, and runs admitted foreground streams,
background external `DISTINCT`, mutation, and checkpoint work concurrently.
The typed report retains latency, process-memory, page-fault, spill, cache,
checkpoint, and runtime-governor measurements. Synthetic reports always carry
`production_eligible=false`; they cannot satisfy production-copy qualification.

The current Cypher crate uses:

```text
crates/cypher/src/lib.rs       public crate facade and re-exports
crates/cypher/src/ast.rs       syntax-only statement and expression types
crates/cypher/src/parser.rs    cursor-based parser entry point
crates/cypher/src/parser/*     parser helpers by statement/expression family
crates/cypher/src/tests.rs     parser coverage for the supported subset
```

## Data Model

Skein stores a property graph:

- `NodeId`: stable internal node identity
- `RelId`: stable internal relationship identity
- `LabelId`: catalog identity for node labels
- `RelTypeId`: catalog identity for relationship types
- `Value`: null, bool, integer, float, string, bytes, list, map, temporal values
- `NodeRecord`: node id, label set, property map
- `RelRecord`: relationship id, source id, target id, type id, property map

Nowledge usage should prefer explicit stable external ids as properties. Internal
ids are storage identities and should not be exposed as durable cross-version
references.

## Storage Design

The initial storage engine should optimize for correctness and embeddability:

- append-only WAL for transactional durability
- immutable or copy-on-write pages for crash recovery
- column families or logical trees for nodes, relationships, properties, and
  indexes
- adjacency indexes by `(source, type, target)` and `(target, type, source)`
- property indexes for high-selectivity equality, composite equality, range,
  and text filters
- catalog metadata versioned independently from data pages

The storage API should be iterator-oriented. The executor should be able to
compose scans, expands, filters, and joins without materializing full graphs.
Adjacency access also exposes a stable ordered view sorted by
`(neighbor_id, relationship_id)` plus sparse/dense group classification. The
executor consumes that ordered view for one-hop and bounded outgoing expansion,
so query traversal order is independent of the current relationship-id index
layout. The current implementation computes the view over in-memory adjacency
indexes; the same API is the boundary for later sparse blocks and copy-on-write
dense segments.

## Cypher Pipeline

```text
Cypher text
  -> AST
  -> Semantic graph query model
  -> Logical plan
  -> Cascades optimizer
  -> Physical plan
  -> Executor
  -> Rows
```

The semantic graph query model is a separate layer between AST and logical plan.
It resolves labels, relationship types, variable scopes, property references,
and cardinality constraints. This keeps parser syntax compatibility separate
from planning semantics.

### Schema-guided GraphRAG queries

GraphRAG callers obtain a bounded `GraphRagSchemaContext` from `Database` or a
pinned `DatabaseReadTransaction`. The context is built only from catalog tokens
and aggregate graph statistics. It contains ranked labels, relationship types,
public or observed properties, one-hop routes, and common two-hop paths. It
never reads node payloads, embeddings, content values, or histogram samples.

The default context limits every collection and properties per subject. A
stable fingerprint covers the returned snapshot and truncation state so a host
can cache prompt context without confusing two schema views. The compact
renderer tells a query generator to:

- generate read-only Cypher;
- parameterize values;
- use only identifiers present in the context;
- keep generated traversal bounded to at most two hops.

For callers that can produce structured output, `GraphRagQueryDraft` is the
preferred boundary. It describes a node, an observed one-hop route, or two
composable observed routes, plus predicates, projections, and a bounded result
limit. `GraphRagSchemaContext::generate_query` checks the schema fingerprint,
identifiers, every route leg, properties, binding scope, predicate shape, and
limit before rendering parameterized Cypher. The result contains the required
parameter names and schema-derived scalar or list type requirements. Generated
queries are opaque outside the core crate, and generation recomputes the
context fingerprint so callers cannot mutate a returned schema context and
smuggle invented identifiers into the execution boundary.
`DatabaseReadTransaction::query_generated_graph_rag` rejects stale schema
context snapshots and invalid parameter maps before submitting the generated
Cypher through the normal query runtime.

Application-bound hosts use the same contract through
`NowledgeMemEmbeddedStoreHandle::graph_rag_schema_context` and
`read_generated_graph_rag`. The handle releases its read lock while the host or
LLM prepares a draft, then opens a fresh pinned read transaction for execution.
An intervening graph commit therefore fails closed as a stale schema context.
The generated query remains subject to `NowledgeMemReadOptions` row and payload
budgets.

Typed two-hop generation validates both one-hop legs independently, including
the shared intermediate label and both relationship types. It does not infer
topology from `GraphRagCommonPathSummary`, because that compact summary does not
preserve the intermediate node and both edge types.

Skein does not embed an LLM and does not execute generated queries through a
special GraphRAG interpreter. The host submits generated Cypher through the
normal query runtime, or through a read transaction when it needs an enforced
read-only snapshot. Parsing, optimization, execution profiling, slow-query
logging, and blackbox aggregation therefore retain their normal ownership.

### Cloud semantic seam

Neither the current Cloud adapter nor Skein Cloud runs the Skein embedded
database. They reuse a narrower, storage-neutral contract:

- a versioned semantic graph catalog for node kinds, relationship endpoint
  types, direction, cardinality, readable properties, derived-field grain, and
  authorization;
- graph logical operators and semantic-preserving rewrite fixtures;
- `skein-optimizer` memo/search/report primitives, with backend-specific
  physical rules and cost inputs;
- a storage-neutral deterministic analytics kernel over immutable CSR/CSC
  snapshots.

The local backend lowers logical operators to Skein scans, indexes, adjacency
expansion, and the embedded executor. The current Nowledge Cloud adapter lowers
the same logical operators to workspace-scoped PostgreSQL joins and bounded
recursive CTEs. The target Skein Cloud backend lowers them to immutable
graph-segment scans, topology expansion, distributed joins/path stages, and
retryable exchanges over a pinned manifest. Physical plans and costs are
deliberately different; logical rows, cardinality/null semantics,
authorization, and completeness diagnostics must match.

Skein Cloud stores canonical graph, value, delta, statistics, checkpoint, and
projection objects in S3. Worker-local SSD/NVMe is a digest-verified cache for
objects and decoded topology/column blocks, never durable database state. A
write becomes visible only after its S3 objects are verified and metadata Raft
publishes the manifest pointer. Eviction, worker restart, or complete cache-disk
loss must preserve committed data and query semantics.

For a Cloud-attached workspace, Skein is an eventually consistent partial
mirror, not a second canonical writer. The current Cloud adapter is authoritative
until migration; Skein Cloud becomes the permanent authority after cutover.
Local query eligibility is therefore a semantic coverage check, not merely
"does this node exist locally." Results must identify the subscription identity
and epoch, filter digest, materialization scope, applied sequence, canonical
head, and whether the requested query is complete within that scope. Standalone
local workspaces remain locally authoritative.

Graph analytics follows the same boundary. Local Skein and small Cloud jobs may
feed immutable source-epoch snapshots into the shared in-process kernel.
Distributed Skein Cloud execution shares the algorithm semantics, message
algebra, fixtures, and output contract without sharing the embedded runtime.
Analytics outputs are rebuildable, versioned projection generations; they
never enter the local graph WAL or either Cloud canonical mutation log. This
repository owns the shared semantic and execution contract; the hosting layer
owns its publication, recovery, and partial-mirror protocols.

The Cypher parser should stay systematic as the supported subset grows. The AST
types define syntax data only; parser entry points dispatch by top-level
statement family; reusable cursor helpers own keyword matching, token
expectations, delimiter handling, whitespace movement, and end-of-input checks.
Statement parsers should compose those helpers rather than open-coding byte
movement or separator loops. This keeps syntax changes reviewable and avoids
leaking semantic validation into parsing.

The parser technology choice is deliberately conservative. Skein should not add
a yacc-style generated grammar for the current Nowledge replacement slice. The
supported Cypher surface is production-query-driven, narrow, and tied to
planner/executor semantics that are still changing. A generated grammar would
make it easier to accept syntax that the semantic graph model cannot execute,
and would add another build-time boundary before the subset has stabilized.

The preferred direction is closer to RisingWave's newer parser organization:
keep the top-level statement flow explicit in Rust, keep token/cursor ownership
separate from AST construction, and use small parser helpers or combinators only
where they reduce local ambiguity for expressions, lists, and delimited forms.
Skein can adopt a real lexer or parser-combinator layer later, but only after a
Nowledge scanner hit proves that the current cursor helpers are becoming the
main source of complexity.

This still preserves the useful `parser_yacc` practice from Chryso: grammar
recognition, AST construction, and semantic validation remain separate
concerns. The current hand-written parser should keep that boundary while
avoiding a generated grammar until the Cypher subset is large and stable enough
to justify it.

The optimizer boundary is:

```text
crates/plan/                    logical/physical IR and typed phase roots
crates/optimizer/               generic Cascades primitives
crates/optimizer/src/graph/     graph catalog, costing, rules, and lowering
src/optimizer.rs                compatibility re-export facade
```

## Logical Plan

Core logical operators:

- `NodeScan`
- `NodeIndexSeek`
- `Expand`
- `ExpandInto`
- `RelScan`
- `Filter`
- `Project`
- `Join`
- `AntiJoin`
- `Optional`
- `Aggregate`
- `Sort`
- `Limit`
- `CreateNode`
- `CreateRel`
- `Merge`
- `SetProperty`
- `Delete`
- `DetachDelete`

Graph patterns should first lower into pattern fragments. The planner can then
enumerate pattern join orders instead of committing too early to query text
order.

## Physical Plan

Skein currently keeps a public `PhysicalPlan` enum as the embedded facade
between optimizer and executor. That shape is intentionally compatibility-first:
it keeps execution, explain output, and deterministic fingerprints stable while
the Nowledge replacement surface is still growing.

The flat enum is a compatibility surface, not the long-term internal ownership
boundary. Skein should migrate incrementally toward a statement root that
separates schema, mutation, query, and procedure plans. Query plans should use a
framework-owned node shape with operator payload, inputs, and physical
properties kept as separate contracts:

- keep `PhysicalPlan` as the public compatibility facade until executor
  contracts are stable
- keep `PhysicalPlanDomainRef` only as a zero-copy compatibility and diagnostic
  view; it classifies the flat facade but does not provide type-level isolation
- lower the facade once into an internal `PlannedStatement` root before moving
  costing, cardinality, properties, memory estimation, and traversal onto the
  decomposed representation
- keep query operators as a smaller closed enum with named payload structs;
  keep child topology owned by the query-node representation
- keep `PhysicalPlanKind`, `PhysicalPlanClass`, `PlanChildren`, `PlanNode`, and
  plan histogram helpers beside the IR in `skein-plan`
- keep `OptimizationSearchReport`, `SelectedPlanTrace`, memo/rule primitives,
  and graph-specific costing/lowering in `skein-optimizer`
- keep deterministic fingerprint helpers split by value, predicate, and
  projection responsibility, and keep access-path candidate composition
  separate from rule execution
- split optimizer tests by plan structure, search costing, aggregate costing,
  and traversal costing so failures retain a clear ownership boundary
- migrate executor dispatch, explain, fingerprinting, and plan-cache identity
  only after the storage-independent analysis paths use the decomposed form
- do not add another graph-optimizer crate unless the graph module develops an
  independently reusable contract and the dependency direction remains acyclic

Core physical operators:

- `SeqNodeScan`
- `IndexNodeSeek`
- `IndexNodeCompositeSeek`
- `IndexNodeTextSeek`
- `SeqRelScan`
- `AdjacencyExpand`
- `ExpandIntoCheck`
- `HashJoin`
- `NestedLoopApply`
- `FilterExec`
- `ProjectExec`
- `SortExec`
- `LimitExec`
- `MutationExec`

For the Nowledge replacement target, adjacency expansion and selective property
index seeks matter more than full relational join sophistication.

## Cascades Optimizer

Skein should use a Cascades model similar to Chryso:

- `Memo`: stores equivalent plan alternatives. The generic group storage lives
  in `skein-optimizer`; `skein_optimizer::graph` stores Cypher-specific
  `GroupExpr` payloads in that crate-owned memo.
- `Group`: represents a logical equivalence class.
- `GroupExpr`: stores an operator plus child group references. Graph-specific
  lowering owns this private payload while public logical operators live in
  `skein-plan`.
- `Rule`: transforms logical expressions into equivalent logical alternatives.
- `ImplementationRule`: maps logical expressions to physical alternatives.
- `CostModel`: scores physical alternatives using graph statistics. The current
  slice applies this to scan-vs-index-seek choices and records a recursive
  selected-plan row/cost summary that includes bounded expand estimates. Basic
  graph counters are maintained incrementally in the store; richer histogram
  and path statistics are still derived from canonical records.
- `PhysicalProperties`: required and delivered ordering, distinctness, and
  binding properties.
- `PlanNode`: a graph-payload-independent trait for walking selected plans and
  building operator/class summaries without parsing explain text.
- `OptimizationSearchReport`: records generic search mode, group count, budget
  warnings, and rule/decision events before the root facade materializes the
  legacy `OptimizerTrace` surface.
- `OptimizerTrace`: deterministic diagnostics for rules, groups, candidates,
  costs, warnings, search limits, and selected physical plan operator/class
  histograms. If a logical plan exceeds `OptimizerConfig::max_groups`, Skein
  does not build an oversized memo; it records a budget warning and selects a
  deterministic direct physical fallback.

Unlike Chryso, Skein needs graph-specific properties:

- bound variables
- preserved path uniqueness mode
- node/relationship identity uniqueness
- ordering
- expected cardinality
- required adjacency direction
- required index coverage

## Rule Families

Logical rewrite rules:

- push predicates into node and relationship scans
- convert property predicates to index seek candidates
- reorder pattern expansions by estimated selectivity
- merge adjacent projections
- remove redundant filters and projections
- normalize commutative predicates
- split conjunctive predicates
- lower `MERGE` into match-or-create where legal

Implementation rules:

- `NodeScan` to `SeqNodeScan`
- `NodeScan + property predicate` to `IndexNodeSeek`
- `Expand` to `AdjacencyExpand`
- selective pattern fragment to `HashJoin`
- correlated pattern fragment to `NestedLoopApply`
- mutation logical nodes to `MutationExec`

The optimizer must have explicit search budgets and deterministic tie-breaking.
Local-first tooling depends on stable plan output for tests and debugging.

## Statistics

The catalog should track:

- node count per label
- relationship count per type and direction
- property null fraction
- property distinct count
- optional histogram or top-k values for indexed properties
- degree distribution summaries per label/type pair
- per-hop exact/fallback path cardinality estimates for bounded expands
- selected physical plan row and cost estimates for optimizer diagnostics

The first cost model can be simple, but it must be structured enough to improve
without changing optimizer APIs.

## Transactions

Transactions should expose:

- read transaction
- write transaction
- commit
- rollback
- checkpoint

The initial concurrency model can be single-writer/multi-reader. This matches
the embedded local runtime shape and is safer than prematurely designing a
high-concurrency server engine.

## Nowledge Integration

The replacement should preserve the current local wrapper shape:

- one embedded database object per path, enforced by a process-local canonical
  path lease and a cross-process exclusive lock held for the handle lifetime
- shared read access through guarded connections or snapshots
- exclusive writes, control operations, and checkpoints
- explicit storage version check at boot
- recovery path for WAL/lock sidecars
- projected graph operations as rebuildable outputs, not canonical data

The preferred front door for new Nowledge integration is parameterized Cypher
through the query runtime. `NowledgeGraphAdapter` keeps
`NowledgeGraphStatement` values for query, explain, and grouped mutation
transaction execution through the same planner and storage paths as `Database`.
Single-query knowledge read facades are not part of `Database` or
`DatabaseReadTransaction`. Compatibility response-shape tests execute their
parameterized Cypher through a test-only extension, while application callers
use the admitted query runtime directly. Typed operations remain only for
grouped WAL atomicity, recovery, admission, generation publication, and bounded
multi-statement workflows. This keeps the compatibility boundary reviewable
without adding an ACL layer to the embedded built-in core, while preserving the
rule that search projections stay outside canonical graph state.

Mem integration should start with side-by-side writes to Kuzu/Ladybug and
Skein, then select the read engine through runtime configuration. Kuzu/Ladybug
remains the default read engine until route evidence proves that Skein can
serve that read family. This avoids a large typed facade migration and keeps the
cutover mechanism simple: write both, read one, compare when requested.

Migration gates use a machine-readable query inventory. `scan-nowledge-inventory`
walks Nowledge Rust source files, extracts conservative Cypher string-literal
call sites, classifies them as read, mutation, schema, procedure, or transaction
control, and emits the audited `required_checks` JSON artifact. The lower-level
JSON importer also accepts scanner-shaped `name` plus `call_sites` objects; each
call site has `name`, `query_family`, `source`, and optional `cypher`. Skein
validates this through `build_compatibility_query_inventory_from_json`, rejects
duplicate check names, and can export the audited artifact through
`compatibility_query_inventory_to_json` for CI reuse. Coverage and shadow gates
then compare that inventory against the public compatibility fixture instead of
relying on an informal checklist. `scan-nowledge-cypher-coverage` scans the same
source tree and reports fixture coverage by normalized Cypher text, which lets
scanner-generated `file:line:hash` call-site names map to existing semantic
fixture names without duplicating fixtures. `scan-nowledge-cypher-coverage-detail`
adds `covered_items` and `missing_items` with source, query family, and Cypher
text so fixture gaps can be closed from real Nowledge call sites. The scanner
keeps Cypher map literals but skips unresolved Rust format templates such as
`{space_clause}` because they are not executable query text until the caller
selects a concrete shape. Gate reports
can be exported as JSON through the coverage, inventory gate, shadow cutover,
and migration gate report helpers; their `decision` fields are lowercase
`ready` or `blocked` strings so CI does not need to parse Rust debug output.
`assess_compatibility_migration_gate_bundle`
packages the four reports into one result, and
`compatibility_migration_gate_bundle_to_json` preserves the same structure for
artifact upload. The `SKEIN_ENABLE_COMPATIBILITY_TOOLS=1
nowledge-cypher-migration-gate [--require-ready]
[--require-cutover-evidence] [--allow-self-shadow] [--shadow-ready]
[--shadow-trace <path>] [--shadow-timeout-ms <ms>]
[--require-rollback-evidence] [--rollback-evidence <text>]
[--require-storage-recovery-evidence] [--storage-recovery-report-json <path>]
[--require-background-maintenance-evidence]
[--background-maintenance-report-json <path>] <root> <shadow-name> <program>
[args...]` CLI command
scans a Nowledge source tree,
runs the public Nowledge core fixture through `ExternalShadowCommand`, uses
normalized Cypher coverage so scanner-generated `file:line:hash` names do not
have to match semantic fixture names, and prints the same migration-gate bundle
JSON. With `--require-ready`, the command exits with an error when the migration
gate decision is blocked. With `--require-cutover-evidence`, the command also
runs the ready preflight and requires previous-wrapper shadow evidence, making it
suitable for isolated release or nightly cutover evidence generation. Production
Mem should consume typed Rust library gates such as
`nowledge_mem_final_cutover_preflight` instead of invoking this CLI path.
External shadow adapter smoke reports include a `dual_engine_evidence` object
that records the Skein primary side, the previous-wrapper shadow side, matched
check count, primary-only count, and readiness. Preflight consumes this field
when present so release automation can distinguish true side-by-side evidence
from primary-only protocol smoke.
With `--require-rollback-evidence`, the same gate also requires caller-supplied
previous-database reopen proof through `--rollback-evidence <text>` before the
migration decision can be ready.
With `--require-storage-recovery-evidence`, cutover evidence also requires a
`skein-storage-recovery-report` artifact from the real database path, including
durable recovery, checkpoint-boundary, bounded-WAL-replay, and clean-tail
readiness. With `--require-background-maintenance-evidence`, cutover evidence
also requires caller-owned maintenance readiness from either the top-level
fixture summary or a `skein-background-maintenance-report` supplied through
`--background-maintenance-report-json`; declared report protocols must match,
but deferred or rejected background work is not a blocker because foreground
user work is intentionally ungated by local background budgets.
The bundle also carries `background_maintenance` resource-readiness diagnostics
from the post-fixture local database, including stable QoS admission strings and
operation totals for caller-owned maintenance loops. These diagnostics are
reported for scheduling visibility and only affect compatibility readiness when
the corresponding background-maintenance evidence requirement is enabled.
`skein-shadow-self` is a JSON-lines self-shadow process for protocol and CLI
smoke testing; it exercises the process boundary but does not replace the
required previous-wrapper parity run. The CLI bundle includes `shadow_run`
metadata with `evidence_kind` set to either `previous_wrapper` or
`protocol_smoke`, so automation can reject smoke evidence without parsing the
shadow command line. Migration gate reports also include caller-owned rollback
evidence fields so release automation can require a previous-database reopen
check while keeping the graph kernel independent from that previous database.
The process protocol is specified in
`docs/EXTERNAL_SHADOW_PROTOCOL.md` so previous-wrapper adapters can be
implemented without depending on internal fixture code.

Cloud integration should not embed Skein as canonical storage. Cloud can reuse
Cypher parsing, logical planning, and graph projection semantics if useful, but
the execution backend remains PostgreSQL-backed facts and edges.

Search integration should also preserve the current Nowledge boundary: semantic
and full-text search are rebuildable projections, not source-of-truth graph
state. Skein therefore keeps `SearchIndex` separate from `GraphStore`. This
allows the graph store to replace Kuzu/Ladybug while the search projection
replaces LanceDB without coupling vector lifecycle state to canonical graph
durability.

Full search rebuilds must be bounded and all-or-nothing. A rebuild first derives
typed projection rows from canonical graph nodes into a temporary map, then
replaces the in-memory projection only after the configured row bound is not
exceeded. Successful rebuilds clear projection lifecycle markers; failed rebuilds
leave the previous projection intact and keep a full-reindex marker.
Caller-owned background loops can rank full search rebuilds, incremental deltas,
and metadata repair as `Projection` work and then execute them through QoS
admission or scheduler wrappers; explicit foreground rebuilds continue to use
the direct APIs. The embedded `Database` facade can also collect rankable
background maintenance candidates across schema maintenance, property-index
projection rebuilds, search projection rebuild/repair, graph-derived search
deltas, and external content artifact jobs; this facade only reports candidate
plans and QoS decisions, leaving worker ownership and execution timing to the
caller. The embedded `Database` facade exposes the same metadata-only repair
over canonical graph evidence, preserving projection text and embeddings while
repairing graph-derived metadata fields.
Persistent search projection snapshots publish through a synced temporary file,
atomic rename, and parent-directory sync, while remaining rebuildable projection
state outside the graph WAL. A graph-derived rebuild records the source graph
commit epoch inside the caller-owned search projection snapshot so retrieval can
compare projection freshness with the live graph snapshot without moving search
state into the graph WAL.
`SearchIndex::rebuild_derived_artifacts` wraps the full rebuild path in a
report-oriented orchestration API with document counts, scanned nodes, indexed
documents, lifecycle-marker state, and lifecycle-marker reasons. Projection
freshness carries the current full-reindex and metadata-repair marker reasons so
retrieval diagnostics can explain why a projection is stale or requires repair.
In-memory projections retain the same marker state for diagnostics; path-backed
projections also persist markers as files.

Metadata-only repairs use the same graph-derived projection row mapping but only
replace document metadata for already-present projection rows. They preserve
existing titles, text content, and embeddings. If repair discovers missing rows,
it marks full reindex as needed because metadata repair cannot create the absent
search documents without becoming a rebuild.

Embedding lifecycle is tracked by an explicit model manifest. The search
projection persists the embedding model name, optional model version, and vector
dimension next to the snapshot. Row writes must match the manifest dimension,
query vectors with mismatched dimensions degrade to the text leg, and model or
dimension manifest changes mark full reindex as needed instead of silently
reusing stale vectors.

The text leg uses BM25-style scoring rather than simple token coverage:
case-insensitive tokenizer output preserves Nowledge-style underscore
identifiers, splits camelCase/snake_case/kebab/path-like identifiers, creates
adjacent chunk bigrams, splits acronym-to-titlecase technical identifiers such
as `LSMTree` and `HTTPServer`, emits conservative CJK bigrams and trigrams for
Chinese/Japanese/Korean knowledge notes, normalizes common English suffixes for
memory/source/thread-style terms, and expands conservative knowledge-retrieval
aliases such as `rag`, `graph_rag`, `graph_retrieval`, and `kg`, plus database
system aliases such as `wal`/`write_ahead_log`, `mvcc`, `lsm`, `csr`, and
`csc`, and migration/projection aliases such as `pg`/`postgres`/`postgresql`,
`pgvector`/`vector_search`, `fts`/`full_text_search`, `lance`/`lancedb`, and
`kuzu`/`ladybug`, plus retrieval-algorithm aliases such as `rrf`/
`reciprocal_rank_fusion`, `ann`/`approximate_nearest_neighbor`, and
`hybrid_retrieve`/`hybrid_retrieval`/`hybrid_search`.
Conservative English stopwords are removed before query
scoring, corpus statistics, and matched-term reporting, while raw compound
identifiers such as `the_source` remain searchable. Term frequency affects rank,
inverse document frequency is computed from the current projection, and
document length normalization prevents verbose rows from dominating short
focused matches. Selected graph-derived projection metadata identifiers
(`kind`, `external_id`, `source_id`, and `space_id`) also contribute analyzer
terms for text fallback, while matched projection-text spans remain limited to
title and content fields. This keeps text fallback useful while the projection
remains rebuildable.

Persistent checkpoints also publish a generation-bound segmented lexical
projection. Its immutable posting and document-length blocks are built with a
bounded external sort and queried through bounded term streams. The persisted
fallible path computes candidate-scoped BM25 statistics after metadata and ACL
filtering, merges a bounded mutation mini-delta, and uses streaming TopK for a
text page window or hybrid rank window. Declared corruption fails closed.

The larger-than-memory read owner is `SearchOutOfCoreReader`, not the mutable
compatibility `SearchIndex`. Checkpoint publishes generation-named descriptor,
full-document payload, metadata-only sidecar, vector-only sidecar, sidecar
layout, and lexical manifest artifacts, then atomically switches a small
manifest that binds their checksums, source graph epoch, analyzer, and document
digest. Readers pin the selected generation. Metadata and ACL predicates decode
only metadata ranges before ranking into a bounded, disk-backed CandidateSet;
BM25 consumes that set through fallible membership checks, vector scoring reads
only vector ranges, and full document ranges are touched only for final-page
hydration. Explicit budgets cover compressed and decompressed segments,
candidate spill and block reads, retained score entries, hydrated rows, and
hydrated bytes. The typed `NowledgeMemOutOfCoreSearchProjection` facade exposes
this read path to Mem.

Production callers must use `NowledgeMemOutOfCoreSearchProjection::open_production`
or `open_production_with_config`. These constructors require a
generation-bound `SearchLexicalProductionQualificationReport` and recompute the
admission decision from raw evidence instead of trusting its `ready` field. The
gate requires exact TopK and score parity across metadata, ACL, hybrid,
incremental, reopen, and corruption cases; at least 100,000 documents; a
larger-than-memory workload; at least 50% selective-query P95 improvement;
posting-proportional query work; RSS within 110% of the storage budget; and no
more than 10% update or checkpoint P95 regression. Rebuilding or republishing
the projection changes its generation and invalidates earlier evidence.

`SearchIndex::open()` deliberately retains full residency for mutable rebuild,
delta, compatibility probe, and borrowed `document()` APIs. Callers must not use
that maintenance owner as the larger-than-memory production Search read path.
Production cutover remains gated on differential shadow results and measured
latency, RSS, payload, and page-fault evidence from a representative replica.
Application-owned analyzer lexicons can register readable phrase or identifier
aliases through normalized alias rules and can add domain stopword rules for
high-frequency application terms. The graph/search kernel keeps only the small
cross-domain default alias and stopword set, while Nowledge-specific lifecycle,
schema relationship vocabulary, and application noise terms stay in the
caller-owned lexicon. `SearchAnalyzerLexicon::nowledge_memory()` provides the
opt-in Nowledge Mem profile, including lifecycle aliases and the conservative
Memory-to-Memory relation bridge used by Nowledge Mem search anchors. The
profile is a retrieval recall bridge only; focus-map decisions about whether an
internal schema handle can lead a graph view remain the caller's responsibility.

Search hits expose the information needed by a knowledge retrieval surface:
fused RRF score, per-child RRF components, vector score, text score, vector
rank, text rank, fallback reasons, projection kind, external ID, source ID,
matched analyzer terms, matched projection-text spans, and projection freshness
derived from the recorded source graph commit epoch, current projection markers,
and embedding manifest state. Hybrid ranking uses weighted reciprocal-rank
fusion over the vector and text child retrievers, preserving each child position
and child RRF component for explainability. Matched spans are byte ranges over
the rebuildable search projection title/content fields; they are not canonical
large-value blob spans. Callers that need bounded candidate growth can use
`SearchIndex::search_with_options` with a rank window, which limits which child
candidates participate in RRF while still reporting each child's total candidate
count. The same options also carry
exact-match metadata filters such as `kind` or `source_id`; filters are applied
before vector scoring, BM25 corpus statistics, retriever candidate counts, and
final truncation so scoped retrieval does not leak unscoped candidates into
ranking diagnostics. `SearchResultSet::candidate_set` reports the exact
projection-local pre-filter set using stable document IDs, including id-space,
representation, cardinality, filtered-out count, exactness, the metadata
filters that produced it, the source graph snapshot commit epoch when the
projection was rebuilt from graph storage, and an optional caller-supplied
policy epoch. `policy_epoch` is report metadata only and does not execute
authorization or filtering policy inside the search projection. This is a
diagnostic boundary only: projection-local positions are not stable graph
identity across projection generations. Higher-level retrieval APIs can use
those fields for score breakdowns, provenance, and stale projection warnings
without making the search projection canonical. Callers that need
response-level diagnostics can use
`SearchIndex::search_with_report` to get the total document count, post-filter
document count, candidate-set report, pre-limit hit count, requested limit, rank
window, truncation flag, truncation reasons, fallback reasons, child retriever
availability, child fallback reasons, candidate counts, child output candidate
sets, top hit IDs, and per-child top candidate ranks and scores.

The stable embedded facade exposes this boundary without owning search state:
`Database::rebuild_search_projection` derives projection rows from the canonical
graph into a caller-owned `SearchIndex`, and the background variants expose the
same rebuild through `LocalQosPolicy` or `LocalQosScheduler` admission for
caller-owned maintenance loops. `DatabaseReadTransaction::rebuild_search_projection`
and `DatabaseReadTransaction::repair_search_projection_metadata` derive the same
projection maintenance inputs from a pinned catalog and graph snapshot.
`Database::retrieve_knowledge`
combines the projection report with the current graph commit epoch and a compact
diagnostics summary. `DatabaseReadTransaction::retrieve_knowledge` uses the same
caller-owned `SearchIndex` while resolving graph seeds and graph context against
the pinned catalog and graph snapshot, so retrieval can stay snapshot-stable
without moving search projection state into the graph store.
Retrieval callers can pass a rank window through
`KnowledgeRetrievalRequest` to bound hybrid child retriever participation, pass
search fusion weights to bias vector or text child retrievers before graph
context expansion, and pass metadata filters that scope both search hits and
graph-native seed candidates. Filter keys align with graph-derived
projection metadata:
`kind` accepts canonical node labels or lowercase projection names, `external_id`
maps to the projected node identity (non-empty `id` when present, otherwise the
canonical node id string) used by search hits and graph context path endpoints,
`source_id` maps through the same non-empty `source_id`/`thread_id`/`source`
projection fallback as search documents, and other keys map to same-name scalar
node properties. `space_id` follows the Nowledge normalized-space rule: missing,
`NULL`, and empty-string values are scoped as `default`. Returned diagnostics
preserve the search limit, rank window, search fusion weights, graph seed budget, graph
context budget, candidate budget, filtered candidate counts, search document
scope, exact search candidate-set report, search hit count, search truncation flag and reasons, search fallback reasons, graph seed counts,
graph-seed truncation flag and reasons, graph context path count, fan-out reason
count, final candidate count, pre-limit merged candidate count, response-level
candidate truncation flag and reasons, graph-context truncation flag and
reasons, projection source graph commit epoch, stale projection warnings,
projection marker warnings, structured projection stale/full-reindex/
metadata-repair flags and marker reasons, and empty-result reasons. Empty-result reasons
reuse the search projection report's own empty-result reasons, distinguishing
empty projections, metadata-filter misses, no matching rows, search fallback
causes such as empty text queries, missing or incompatible query embeddings, or
empty vector rows, and request-budget causes such as search limit zero, disabled
graph seed retrievers, or response-level candidate limit zero. This keeps
Knowledge Retrieval as the
primary application-facing path while preserving the rule that search artifacts
are rebuildable and outside the graph WAL.

`Database::retrieve_knowledge` also includes a bounded graph-native seed
retriever over canonical nodes. `KnowledgeRetrievalRequest::graph_seed_limit`
controls the budget. A limit of zero disables the graph seed leg; otherwise the
facade matches query tokens against stable graph properties such as `id`,
`title`, `name`, `summary`, `content`, `body`, and `text`, then returns
deterministically scored `KnowledgeGraphSeed` entries with canonical entity
snapshots and matched property names. This gives the retriever DAG an explicit
graph child even when the caller has no usable search projection.
At the knowledge facade level, `KnowledgeRetrieverReport` normalizes child
retriever diagnostics for vector, text, and graph seed legs: availability,
candidate count, optional limit, optional rank window, optional fusion weight,
child output candidate set, fallback reasons, truncation flag, truncation
reasons, and top candidate rank, score, provenance metadata, canonical node ID,
matched spans, and graph context path count are exposed in one place.
Retrieval diagnostics also report distinct graph-context node count and
relationship count, matching the bounded traversal diagnostics used by typed
knowledge navigation, and distinguish disabled graph-context budgets from
runtime fan-out truncation through graph-context fallback reasons.
Search child top candidates also carry projection freshness, while graph seed
top candidates leave it empty because they are read directly from canonical
graph state. Search child reports distinguish rank-window trimming from
search-limit truncation and expose their vector/text fusion weights, while graph
seed reports record graph-seed limit truncation and disabled-by-limit fallback
reasons.
`KnowledgeCandidate` then projects returned search hits and graph-native seeds
into one application-facing candidate surface. Each candidate records its source
leg, source-local rank, merged source legs, combined score, score breakdown,
optional canonical node ID, optional canonical entity snapshot, optional search
evidence summary, matched projection spans, matched graph properties, and
graph-context path count. The canonical node ID is explicit so callers do not
treat search projection hit IDs as stable graph identity.
Search-hit and graph-seed
candidates that resolve to the same canonical node are merged by graph identity,
with the search hit kept as the primary leg and the graph seed recorded in
`merged_sources`. `KnowledgeCandidateScoringPolicy` currently supports default
max scoring and weighted sum scoring over search and graph-seed scores, giving
future rerank policies a stable hook while preserving per-leg score provenance.
`KnowledgeRetrievalRequest::candidate_limit` applies a response-level candidate
budget after this merge and reports a fan-out reason when the budget truncates
the merged candidate set. This gives the future retriever DAG a typed candidate
boundary without making the search projection part of canonical graph state.

The same facade performs bounded multi-hop graph context
expansion for returned search hits whose projection metadata maps back to a
canonical graph node and for graph-native seeds matched directly from canonical
nodes. The result includes hop number, source/target node labels, external IDs,
relationship type, path direction, and fan-out reasons when the configured graph
context limit cuts expansion short. Graph context relationship de-duplication is
scoped by retriever seed ID, so the same canonical relationship may explain a
search-hit seed and a graph-native seed independently when both map to the same
canonical node. This keeps per-retriever explanation counts stable while the
global graph-context budget still bounds emitted paths. This gives RAG callers
graph evidence paths without issuing ad hoc Cypher for common neighborhood and
short-path context, including pure graph-seed retrieval when no search
projection is available. Graph context path segments carry relationship
properties so source-provenance, weight, and temporal edge metadata remain
available to callers. It also returns `KnowledgeEvidence` summaries that bind each
search hit to its projection kind, external ID, source ID, canonical node ID,
matched terms, score components, ranks, and graph context path count. This keeps
raw evidence provenance explicit even though the search projection remains
outside canonical graph storage.

For resource-constrained embedded deployments, the application can update the
caller-owned search projection incrementally from canonical graph changes.
`SearchProjectionGraphDeltaRequest` maps bounded canonical node IDs into
projection rows, carries delete document IDs, and exposes the same foreground,
background-QoS, and scheduled-background facades as lower-level
`SearchProjectionDelta`. Graph-derived deltas can carry an explicit
complete-through graph commit epoch only when the caller knows the delta covers
all projection-relevant changes through that epoch, so retrieval diagnostics can
distinguish a fresh incremental projection from a partial one. Delta apply
reports include the source graph commit epoch before and after the update plus a
boolean indicating whether this delta advanced the projection freshness
watermark. Freshness-aware background work plans can derive recent delta
operations and source graph commit lag from the caller-owned projection, letting
the QoS ranker prioritize stale graph-derived projection maintenance by expected
value. This keeps FTS/vector projection maintenance incremental without writing
search state into the graph WAL.

Skein Lightning bootstrap export follows the same resource boundary for
embedded deployments. One export pins a shared database commit epoch and emits
two authoritative logical streams: canonical graph rows and the complete
relational checkpoint, including SQL schemas, rows, and overflow values. Initial
import validates both streams before writing and publishes their contents in one
WAL batch. A corrupt or mismatched stream therefore cannot expose a graph-only
or relational-only target. WAL history, physical pages, statistics, and search
or analytics projection artifacts are excluded and rebuilt through their normal
maintenance paths.

A direct caller can still request
`prepare_skein_lightning_bootstrap_export` without local background admission,
but caller-owned pre-upload or background import loops can first ask
`skein_lightning_bootstrap_export_background_work_plan` for an `Import` lane
estimate and then execute through the background or scheduled background export
facades. The same plan can appear in `background_maintenance_candidates`, so a
caller-owned multi-queue loop can rank Skein Lightning pre-export against
projection, parser, schema, and analytics maintenance before attempting
admission. Deferred background export does not generate stable-ID mapping files,
so QoS rejection cannot create partial import state.

Knowledge reads can bypass the search projection when the caller already has
graph identity. Direct entity lookup and property projection use bounded,
parameterized Cypher on a pinned snapshot instead of route-specific typed read
facades. Relationship, neighborhood, path, and bounded-subgraph reads follow the
same rule. Each phase is a small named statement with parameterized identities,
relationship types, metadata predicates, hop bounds, and explicit result
limits. Variable-length expansion and shortest-path statements remain governed
by executor memory admission and query-runtime row and payload budgets.
`DatabaseReadTransaction` exposes the same bounded query runtime over its pinned
catalog and graph snapshot. Application integration should use parameterized
Cypher and query-runtime reports. This keeps snapshot semantics available
without making typed APIs the primary extension point.

## Implemented Milestones

The milestones below describe the implemented architecture sequence. They are
not an active backlog; current incomplete work is tracked only in `TODO.md`.

1. Parser and AST for the Cypher subset used by Nowledge.
2. In-memory graph store with transactions for semantic and planner tests.
3. Logical plan builder for `MATCH`, `WHERE`, `RETURN`, `CREATE`, `MERGE`, and
   node and relationship `DELETE`.
4. Cascades memo, logical rules, implementation rules, cost model, and explain
   traces.
5. Persistent storage with WAL, checkpoint, catalog versioning, and recovery
   tests.
6. Compatibility wrapper matching the current Nowledge local graph API.
7. Projection and analytics hooks for PageRank/Louvain-compatible workflows.

## Validation

Required test suites:

- parser golden tests for the supported Cypher subset
- semantic scope and binding tests
- logical plan snapshot tests
- optimizer rule and plan-shape tests
- deterministic optimizer trace tests
- read-only parameterized `explain-json` CLI smoke tests for structured
  optimizer search mode, plan-cache hit/miss/admission/disabled/bypass/eviction
  and memory-pressure stats, selected-plan operator/class summaries, and
  effective query `WorkRequest` observability
- transaction commit/rollback tests
- WAL recovery and checkpoint tests
- internal Nowledge-shaped compatibility fixtures against the Skein facade
- external-process shadow adapter tests for Ladybug/Kuzu wrapper wiring
- compatibility tests against the current Nowledge Ladybug-backed wrapper

Before replacing Ladybug in Nowledge, the compatibility suite should run both
engines against the same fixtures through `ExternalShadowCommand` or an
equivalent `CompatibilityShadowEngine` implementation, and compare rows,
mutation effects, error classes, and projected graph outputs. The shadow report
must also pass the compatibility cutover gate: every required check is matched
by the shadow engine, primary-only checks are reported as blockers by default,
and the configured minimum matched-check count is satisfied.
