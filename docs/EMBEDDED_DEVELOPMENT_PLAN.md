# Embedded Graph Database Development Plan

> This document is a historical implementation plan. Current normative
> contracts are indexed by `docs/specs/README.md`, and current incomplete work
> is tracked only in `TODO.md`.
>
> Descriptions below of typed Nowledge business CRUD or route APIs are
> historical evidence only. Those facades are excluded from release builds by
> `docs/specs/QUERY_FIRST_PUBLIC_API_SPEC.md`; current integrations use
> parameterized Cypher or PostgreSQL-dialect SQL.

## Objective

Replace the Nowledge local Ladybug/Kuzu graph data plane with Skein without
changing the product's graph semantics or coupling canonical graph storage to
vector search.

The implementation target is the local embedded engine. PostgreSQL remains the
Cloud source of truth, LanceDB remains a rebuildable local search projection,
and large content bodies remain outside the graph store.

## Source Boundaries

Three codebases define the work:

- `nowledge/mem` defines the compatibility contract through `nmem-graph`, its
  Cypher call sites, schema convergence, recovery behavior, and graph algorithm
  usage.
- Skein owns the new parser, planner, optimizer, executor, storage format, WAL,
  checkpoint, and stable embedded API.
- Chryso is a reference for optimizer structure: memo groups, logical and
  physical rules, physical properties, structured costs, deterministic plan
  fingerprints, and explain traces. SQL-specific operators and statistics are
  not copied into Skein.

## Non-Negotiable Invariants

1. Stable logical node and relationship identities never encode physical page
   locations.
2. A committed mutation batch is fully recovered or not recovered at all.
3. Checkpoints are published atomically and never expose a partially written
   snapshot.
4. Search and graph analytics are rebuildable projections, not canonical graph
   state.
5. Parser output is syntax-only. Parameter binding and semantic validation
   happen before logical planning.
6. The optimizer is deterministic under a fixed catalog, statistics snapshot,
   rule set, and search budget.
7. The first concurrency contract is one writer with snapshot readers.
8. A Ladybug compatibility comparison must pass before any production cutover.
9. Embedded deployments are resource constrained by default: foreground graph
   user requests should not be gated by local background budgets, while
   internal projection, import, analytics, and shadow work must be able to
   defer itself under resource pressure.
10. FTS/BM25 and retrieval projections must support incremental maintenance for
    ordinary row upsert/delete changes; full rebuilds are repair paths, not the
    steady-state update mechanism.
11. Desktop deployments are application-bound in-process databases. Mobile
    deployments are SQLite-like embedded databases with stricter memory,
    background, battery, and thermal budgets.
12. Optional advanced capabilities, including the future ACL extension, may be
    disabled or compiled out on mobile. Core durability, recovery, transactions,
    parameterized Cypher, and incremental base indexes remain mandatory.

## Required Capability Surface

### Embedded API

- open or create by path
- storage format/version inspection
- read-only and recovery configuration
- parameterized query and explain
- bounded in-memory plan cache with observable hit/miss/eviction counters
- explicit read and write transactions
- commit, rollback, and checkpoint
- bounded resource configuration
- basic local QoS hooks owned by `skein-qos` for internal background admission,
  operation budgets, optional per-class background budgets, and deferrable work;
  performance should come from clean architecture and bounded work units before
  low-level tuning

### Cypher and Semantic Analysis

The initial production subset is derived from real Nowledge queries:

- `MATCH`, one-hop and bounded multi-hop patterns
- `WHERE` equality, range predicates, boolean predicates, null checks, and list
  membership
- `RETURN`, aliases, aggregation, ordering, offset, and limit
- `CREATE`, `MERGE`, `SET`, `DELETE`, and `DETACH DELETE`
- parameters for property values, predicates, pagination, and list filters
- Nowledge-used zero-argument `CURRENT_TIMESTAMP()` values as epoch-nanos
  integers
- schema DDL and migration statements used by schema convergence
- projected graph lifecycle and graph algorithm procedure calls

Labels, relationship types, and property names remain identifiers rather than
runtime parameters. This keeps catalog resolution deterministic and prevents a
single prepared query from changing its schema dependencies.

### Storage and Recovery

- versioned catalog tokens and index descriptors
- canonical node, relationship, and property records
- outgoing and incoming adjacency indexes
- equality and composite equality indexes first, followed by range indexes
  where real queries require them
- checksummed batch WAL records with torn-tail detection
- checkpoint epoch and WAL replay boundary in a manifest
- copy-on-write or immutable snapshot pages for readers
- bounded compaction and orphan cleanup

### Optimizer

Reuse Chryso's separation of concerns, not its SQL operator set:

- memo groups store equivalent graph plans
- logical rules normalize and reorder graph patterns
- implementation rules produce scan, seek, expand, join, and mutation choices
- physical properties track bound variables, ordering, uniqueness, adjacency
  direction, and index coverage
- graph statistics track label counts, relationship counts, distinct values,
  and degree summaries
- structured costs expose CPU, random I/O, sequential I/O, and output rows
- deterministic tie-breaking and explicit search budgets make plans testable

The optimizer must not hide storage-specific decisions inside the parser or
executor. Storage capabilities enter through catalog metadata and physical
implementation rules.

## Delivery Phases

### Phase 0: Executable Vertical Slice

Scope:

- parser to executor pipeline
- stable logical IDs
- create and one-hop match
- property equality index seek
- batch WAL, recovery, checkpoint, and mutation transaction facade
- deterministic explain output

Exit gate:

- format, unit, and lint checks pass
- restart and torn-WAL tests pass
- one end-to-end indexed query produces a stable physical plan

Status: complete in the initial Skein MVP.

### Phase 1: Compatibility Front Door

Scope:

- parameterized query, explain, and transaction APIs
- a typed adapter matching the `nmem-graph` execution shape
- query inventory generated from real Nowledge call sites
- dual-engine fixtures comparing rows and error classes

Exit gate:

- every supported query binds parameters before planning
- missing parameters fail before mutation or storage access
- the first read and mutation fixture families match Ladybug behavior

Status: parameter binding is implemented. `NowledgeGraphAdapter` exposes a
parameterized query front door for query, explain, work-request inspection, and
grouped mutation transaction execution without requiring raw string
interpolation. Query-shape-specific knowledge read methods have been removed
from `Database` and `DatabaseReadTransaction`; compatibility response-shape
tests execute the same bounded parameterized Cypher through test-only support.
Stable typed mutations remain for entity and relationship lifecycle work,
normalized-space moves, memory access and content updates, source lifecycle and
metadata work, parsed Source and revision creation, deletes, and grouped WAL
commits.
Memory content/edit updates are exposed as a typed batch for Nowledge content,
title, semantic field, scoring, source, normalized-space, review status,
extraction method, and `reindex_needed` writes.
Scheduler dedup-reviewed updates are exposed as a typed Memory id-list batch
for `dedup_reviewed_at` stamping.
MCP crystal source-link merges are exposed as a typed
`merge_knowledge_crystal_source` API for
`(:Memory)-[:SYNTHESIZED_FROM]->(:Memory)` writes with caller-provided `weight`
and create-only `occasion_key`/`created_at` relationship properties.
Memory lifecycle metadata updates are
also exposed as a typed batch for the Nowledge `metadata`/`is_latest`/
`lifecycle_state`/`updated_at` write shape. Lightweight Memory metadata
replacement writes are exposed as a typed `update_knowledge_memory_metadata_batch`
API for Nowledge `metadata` and optional `updated_at` updates without changing
lifecycle state. Memory EVOLVES edge creation is exposed as a typed
`create_knowledge_memory_evolves_batch` API for Nowledge `add_evolves_edge`
and replacement-relation create shapes with fixed
`(:Memory)-[:EVOLVES]->(:Memory)` endpoints and grouped WAL relationship
creates. Context memory preview reads are exposed as a typed API for Nowledge
semantic-unit title, typed, and label
preview rows with latest/non-crystal filtering. Skill usage-stat updates are
exposed as a typed batch for Nowledge `use_count`, optional `success_rate`,
`last_activity_at`, `updated_at`, and `metadata` writes. Skill metadata-only
replacement writes are exposed as a typed `update_knowledge_skill_metadata_batch`
API for Nowledge `metadata` and `updated_at` updates without changing lifecycle
state. Skill lifecycle and write-state updates are exposed as a typed batch for
Nowledge stage changes, rejections, promotions, compiled metadata, draft bundle
writes, content hashes, and `updated_at` stamping. REST Skills source merges
are exposed as a typed `merge_knowledge_skill_source` API for
`(:Skill)-[:SYNTHESIZED_FROM]->(:Memory)` writes with create-only `weight`,
`occasion_key`, and `created_at` relationship properties. Skill catalog,
detail, state, synthesized-memory evidence, and thread-source reads use fixed
parameterized Cypher with explicit query row and payload budgets. Host code may
coordinate multiple named statements and shape legacy responses, but graph
filtering, joins, ordering, and per-phase limits remain in the query runtime.
These reads do not expose route-specific typed database APIs. Skill rollback
and cleanup deletes are exposed as a typed
`delete_knowledge_skills` API for exact `(:Skill {id})` detach deletes,
including `SYNTHESIZED_FROM` cascade cleanup through the shared WAL-backed
typed entity delete path. Thread compensation deletes are exposed as a typed
`delete_knowledge_threads` API for exact `(:Thread {id})` detach deletes,
including `CONTAINS` and `COMPACTS_TO` relationship cleanup while preserving
Message and Memory endpoint nodes. Thread metadata updates are exposed as a
typed batch for Nowledge `metadata` writes with optional `updated_at`
stamping. Thread denormalized message-count refreshes are exposed as a typed
batch for Nowledge `message_count` writes with optional timestamp stamping and
preserve-newer `updated_at` behavior. Thread list, source, attachment,
render-metadata, ThreadIdentity, and sync-metadata reads use fixed parameterized
Cypher with explicit row and payload budgets; pinned read transactions provide
snapshot consistency for multi-phase host workflows. ThreadIdentity
compensation and cascade cleanup are exposed as a typed
`delete_knowledge_thread_identities` API for exact identity-key deletes and the
Nowledge `public_thread_id`/`input_thread_id`/`thread_uuid` cascade shape.
Thread distilled-memory links are exposed as a typed
`create_knowledge_thread_compaction_link` API for the Nowledge
`(:Thread)-[:COMPACTS_TO]->(:Memory)` write shape with compaction metadata.
Thread-owned Message cleanup is exposed as a typed `delete_knowledge_thread_messages`
API for exact Thread `CONTAINS` Message target-node detach deletes while
preserving the Thread node.
Label lifecycle writes are exposed as a typed batch for Nowledge metadata
updates, canonical-name backfill, and rename/canonical-name updates.
PageRank score persistence and clear operations are exposed as typed batches
for Nowledge Memory and Entity `pagerank_score` writes.
PageRank plan and read helpers are exposed as typed APIs for the Nowledge
graph-count, changed-count, membership split, Memory visibility, and central
entity lookup shapes used around unified PageRank execution.
GraphMeta PageRank and community-detection stamps are exposed as a typed batch
over `meta_id` plus validated state assignments.
Schema migration log writes are exposed as a typed create-once batch over
`SchemaMigrationLog` ids and `applied_at` values. Applied migration id reads are
exposed through a typed list API with deterministic id ordering.
AugmentationJob create/running/progress/completed/failed lifecycle writes are
exposed as a typed batch with explicit status-transition checks.
`DatabaseConfig` provides read-only operation and bounded read-result
configuration. `read_only` opens only existing database directories without
creating missing paths, then rejects Cypher mutations, transaction mutations,
checkpoints, schema maintenance, database-owned projected graph artifact
rebuilds, and derived artifact job execution before they write database-owned
state. `max_read_result_rows` and `max_read_result_payload_bytes` default to
100,000 rows and 64 MiB, respectively. They cap direct, read-transaction,
streaming, EXPLAIN ANALYZE, and system SQL results; callers may further tighten
the limits, while unbounded results require explicitly setting both fields to
`None`. `max_optimizer_groups` caps cascades memo search groups with
the existing deterministic direct physical fallback warning. `recovery_mode`
defaults to strict WAL replay. A torn tail or checksum mismatch rejects every
database open. Torn-tail repair is a separate typed `DatabaseDoctor` operation:
first produce a generation-bound dry-run plan, then explicitly acknowledge that
exact plan before an original-WAL quarantine copy, repair audit, and truncation
can be published. `DoctorRepairTornTail` remains a report-compatibility value
and is rejected by the database-open path.
`max_wal_replay_entries` caps startup WAL replay after valid record decode and
before applying the next record; it counts top-level WAL records rather than
child operations inside a batch, preserving batch replay atomicity. Mutation
queries keep their WAL/commit semantics and are not failed after durable
execution.
`nowledge_memory_core_fixture` and `nowledge_memory_core_inventory` expose the
current Nowledge core compatibility contract as public migration inputs,
including Nowledge-used entity reuse exact, case-insensitive, alias-containment,
same-type bounded scan reads, entity temporal metadata create/update writes, and
the production entity `MENTIONS`/`RELATES_TO` relationship write shapes used by
`entity_write`.
`CompatibilityQueryCallSite` and `build_compatibility_query_inventory` provide a
stable production call-site inventory builder with source metadata and duplicate
check-name validation. `build_compatibility_query_inventory_from_json` and
`build_compatibility_query_inventory_from_json_str` accept scanner JSON
artifacts shaped as `name` plus `call_sites`, while
`compatibility_query_inventory_to_json` exports the validated `required_checks`
artifact for audit or CI reuse. `assess_query_inventory_coverage` turns the
resulting machine-readable required query inventory into covered, missing, and
extra fixture check reports, and `assess_query_inventory_gate` converts that
coverage into `Ready` or `Blocked` with explicit blockers. Coverage, inventory
gate, shadow cutover, and migration gate reports all have JSON exporters with
stable lowercase `decision` values for CI consumption.
`assess_compatibility_migration_gate_bundle` and
`compatibility_migration_gate_bundle_to_json` provide the single-call CI path
that packages coverage, inventory gate, cutover, migration gate, and
`dual_engine_evidence` side-by-side check-count evidence.
`assess_compatibility_migration_gate` combines the inventory gate and shadow
cutover gate into one migration decision. `scan_nowledge_query_inventory` and
the `scan-nowledge-inventory` CLI command provide a production graph-source
scanner for Nowledge Cypher string literals, filtering out non-graph content
store SQL, prompt text, tests, benches, and smoke binaries while emitting the
same audited JSON inventory artifact. `scan-nowledge-cypher-coverage` emits the
same coverage report shape after matching scanner-generated call sites to
fixture checks by normalized Cypher text, preserving source metadata without
requiring duplicate semantic fixture names. `scan-nowledge-cypher-coverage-detail`
also emits `covered_items` and `missing_items` with source, query family, and
Cypher text for fixture work driven by production Nowledge queries. The scanner
filters unresolved Rust format templates such as `{space_clause}` while
preserving valid Cypher map literals such as `{id: $id}` and string values such
as `'{}'`; dynamic query builders should be covered by their concrete
production shapes. The current live scanner coverage gate over the local
Nowledge graph-source tree is complete for the scanned surface:
`nowledge-scanned-inventory` requires 698 Cypher checks, the
`nowledge-memory-core` fixture covers all 698, and `missing_items` is empty.
The scanner excludes vendored `upstream_forks` examples from this
production-source gate. The remaining Phase 1 work is no longer fixture-gap
closure for the current scan; it is to attach the previous wrapper through the
external shadow adapter when migration-gate
evidence is needed, and to rerun the scanner whenever Nowledge adds new graph
call sites. The real previous-wrapper validation workflow is captured in
`docs/NOWLEDGE_PREVIOUS_WRAPPER_PREFLIGHT.md`: copy the live Kuzu, content, and
search state to `/tmp`, run the full exported fixture contract against a
Nowledge-owned persistent wrapper command, smoke the external shadow adapter,
attach storage-recovery and background-maintenance evidence, then run the
migration gate and replacement summary with fail-closed production readiness.
Nowledge Mem integration is side-by-side only during this phase: the existing
Kuzu/Ladybug store remains available while Skein runs as a sibling embedded
graph store behind explicit adapter flags, shadow comparison, and per-surface
cutover evidence. Replacement readiness is not permission to delete or replace
the old database in place; old-store removal requires a later explicit cleanup
phase after rollback and parity evidence exists. The intended repository
integration is to add Skein to the Nowledge Mem repository as a Git submodule,
not to copy Skein source files into the Nowledge Mem tree; adapter code in
Nowledge Mem should depend on that submodule boundary during shadowing and
cutover.
The embedded front door now includes a bounded exact physical-plan LFU cache for
literal and parameterized query/explain paths. Expected-constant-time lookup
uses a hash key over semantically normalized Cypher, the parameter plan shape,
the optimizer environment, optimizer group budget, and access-control policy.
The normalization ignores whitespace, keyword case, and trailing semicolons but
preserves identifiers, parameter names, and literal values that can change the
physical plan. Schema/index or published-statistics generation changes naturally
miss instead of reusing a stale physical plan. This is intentionally not yet a
cross-parameter prepared-plan cache because the current logical plan stores
bound `Value`s. Plan-cache stats also distinguish cache hits, misses,
admissions, disabled-capacity misses, explicit bypasses, evictions, and
capacity-pressure events so production explain artifacts can tell
configuration, statement-shape, admission, and eviction behavior apart without
parsing optimizer decision strings. `foyer` remains a candidate backend once the
cache surface is abstracted, but v1 keeps a small in-process LFU cache in the
`skein-plan-cache` crate to avoid unnecessary runtime/dependency and
memory-growth risk in embedded deployments while keeping the `Database` facade
focused on graph-specific cache keys and cached physical plans.

### Phase 2: Snapshot Transactions and MVCC

Scope:

- committed epoch or LSN per write batch
- immutable reader snapshots
- single-writer serialization
- checkpoint pinning and safe page reclamation

Exit gate:

- readers never observe partial commits
- a long reader survives concurrent commits and checkpoints
- recovery exposes exactly the last durable commit boundary

Status: API-level immutable read snapshots are implemented through
`DatabaseReadTransaction`. A read transaction owns a catalog and graph snapshot,
rejects mutation statements, does not observe later commits, and can continue
after the writer checkpoints. Active readers register their snapshot commit
epoch in a process-local pin registry and unregister on drop. The store also
tracks commit epochs and publishes a checksummed checkpoint manifest with
checkpoint epoch, checkpoint commit epoch, oldest active reader commit epoch,
safe reclaim commit epoch, WAL replay start LSN, and next WAL LSN. The public
`Database::storage_reclamation_watermark` API exposes current commit epoch,
checkpoint boundary, active oldest reader, and safe reclaim commit epoch without
parsing manifest text. Recovery filters projected graph artifact cache entries
that do not match the replayed commit epoch and active projected graph
definition, so stale derived artifacts are not exposed through status metadata.
Checkpoint, manifest, and projected graph artifact publication sync the
published file and parent directory around the atomic rename boundary without
adding per-mutation directory syncs.
Read transactions also expose the typed knowledge entity, ordered bulk entity,
ordered property projection, ordered relationship, neighborhood, path, and
subgraph operations, including metadata-scoped entity, ordered bulk entity,
ordered property projection, ordered relationship, neighborhood, path endpoint,
and subgraph variants, over their pinned graph snapshot, so knowledge navigation
can remain snapshot-stable without constructing ad hoc Cypher.
The mutable typed knowledge facade exposes endpoint-known entity creation for
Memory, Source, Entity, Label, Thread, Skill, and similar graph identities. It
validates labels and property names, enforces `id` property consistency with the
external identity, reports existing identities without writing, and commits
eligible ordered batch creates through one transaction-level grouped WAL batch.
The mutable typed knowledge facade also exposes exact-identity property updates
for lightweight metadata, review-status, and access-field writes; it validates
identifiers, binds assignment values as parameters, and routes through the
existing WAL-backed `MATCH ... SET` mutation path.
Ordered batch property updates use the same typed identity contract for
metadata, review-status, and access-field fan-out, report missing, filtered, and
idless non-writable rows, and commit eligible updates through one
transaction-level grouped WAL batch.
Endpoint-known entity lifecycle cleanup is also exposed as typed detach-delete
over real external `id` properties, with optional metadata filters and the same
WAL-backed `MATCH ... DETACH DELETE` mutation path.
Same-label endpoint-known lifecycle cleanup also has an ordered batch typed
facade for Nowledge `id IN [...]` cleanup paths; it resolves each external id
through the typed identity guard, reports missing, filtered, and idless
non-writable rows, deduplicates eligible ids, and routes the actual write
through one parameterized WAL-backed `MATCH ... WHERE n.id IN $external_ids`
plus `DETACH DELETE n` mutation.
It also exposes exact-identity relationship creation for endpoint-known
Nowledge writes such as mentions, source provenance, labels, evolution, and
compaction links; endpoint filters are applied before writing, relationship
properties are parameter-bound, and the write routes through the existing
WAL-backed `MATCH ... CREATE` mutation path.
Ordered batch relationship creation uses the same endpoint-known facade for
Nowledge ingestion and annotation fan-out paths. It validates and filters each
input row first, skips missing, filtered, or idless non-writable endpoints, then
commits all eligible creates through one transaction-level grouped WAL batch.
Endpoint-known relationship cleanup uses the same typed facade for label/source
relation removal, supports optional relationship-property equality filters, and
routes through the WAL-backed `MATCH ... DELETE r` mutation path.
Ordered batch relationship cleanup uses the same prefiltered endpoint-known
contract and commits eligible deletes through one transaction-level grouped WAL
batch.
Endpoint-known relationship property updates are also exposed through typed
single-row and ordered batch facades for weights, provenance, review fields, and
other lightweight edge metadata. They validate endpoint labels, relationship
types, filter properties, and assignment names, apply endpoint metadata filters
before writing, skip idless projected endpoints, and commit eligible batch
updates through one transaction-level grouped WAL batch.
Page-level MVCC, physical page/segment reclamation, and concurrent writer
coordination remain.

### Phase 3: Cypher Mutation and Query Coverage

Scope:

- `MERGE`, `SET`, `DELETE`, and `DETACH DELETE`
- aggregation, sort, offset, limit, list parameters, and null semantics
- bounded pattern joins and `ExpandInto`
- schema DDL required by Nowledge migrations

Exit gate:

- compatibility fixtures cover every production query family
- unsupported syntax fails with a stable typed error
- no raw string interpolation is needed by the adapter

Status: ordering and pagination are implemented for the current read subset.
`ORDER BY` supports projected aliases and `variable.property` keys with
`ASC`/`DESC`; `SKIP`, `OFFSET`, and `LIMIT` accept literals or bound integer
parameters and reject negative or non-integer values before execution.
Null and list predicates are also implemented for the current read subset:
`IS NULL`, `IS NOT NULL`, and `IN` support literal lists, parameters inside
literal lists such as `[1, $id, 3]`, and bound list parameters, with non-list
`IN` values rejected before execution. Nowledge-used entity alias lookup also
supports `list_contains(e.aliases, $name)` for property-list membership.
Nowledge-used node property patterns in
read `MATCH` clauses, including source and one-hop target node property
patterns, are lowered to the same property equality predicate path. One-hop
unlabeled node matches such as `MATCH (n) WHERE n.id IN $ids RETURN n.id` and
`MATCH (n) WHERE n.id = $node_id SET n.community_id = $community_id` are
supported for current Nowledge graph analysis and community-assignment paths.
Community assignment cleanup is also exposed through
`Database::clear_knowledge_community_assignments`, which accepts explicit label
scopes or an all-node scan, validates label names before WAL, deduplicates
overlapping label scopes, clears only non-null `community_id` values, and
commits eligible clears through one grouped WAL batch.
Memory latest promotion and demotion writes used by EVOLVES workflows are
available as `Database::update_knowledge_memory_latest_batch`, which updates
only `is_latest`, supports the exact `space_id` filter used by in-space
demotion, reports non-writable and duplicate rows, and commits eligible updates
through one grouped WAL batch.
Label canonical collision lookup, missing-canonical backfill, single-Label
usage, canonical-only usage, and all-Label usage reads use seven fixed
parameterized Cypher statements. They scan only `Label` nodes, compute
`HAS_LABEL` counts over any source node type, and expose no route-specific
typed read API. Label Memory distribution reads use one fixed
parameterized, row-bounded Cypher statement for the Nowledge
`COUNT(DISTINCT m)` label stats and OKF label row shapes, with Memory-only
counts, duplicate edge de-duplication, offset/limit pagination, and no WAL
writes. No route-specific typed read API is exposed. Memory label cleanup
writes are available as `Database::delete_knowledge_memory_labels`, covering exact
`(:Memory)-[:HAS_LABEL]->(:Label)` edge removal and all-label edge cleanup for
one Memory through grouped WAL-backed relationship deletes. Label merge
transfer writes are available as `Database::transfer_knowledge_label_memory_edges`,
covering source-label to target-label Memory retargeting with idempotent
target `HAS_LABEL` creation. Memory label carry-over writes are available as
`Database::transfer_knowledge_memory_label_edges`, covering older-Memory to
newer-Memory label copying inside one exact `space_id` with duplicate old label
edges de-duplicated and target `HAS_LABEL` creation kept idempotent.
Endpoint-known Label assignment and field-extensible Label reads use fixed
parameterized Cypher with explicit entity labels, external-id lists,
per-entity limits, allowlisted Label and `HAS_LABEL` projections, and matching
row and payload budgets. Induced edge-list reads use a separate bounded query
with both endpoints constrained by the selected id set. These reads expose no
route-specific typed API or DTO and do not write WAL.
Memory bulk detail, filtered list, and feature-specific projections use fixed
parameterized Cypher. Each business phase owns its projection, filters,
ordering, `LIMIT`, and query budgets; host code derives missing ids and
normalizes spaces without a wide default typed row.
Metadata-related Memory reads use a fixed parameterized query for one normalized
space and the supported `source_id`/`source_thread_id` metadata markers. Each
named business phase selects its fields and supplies explicit ordering, limit,
row, payload, and snapshot budgets.
Memory prefix ownership guards use one fixed parameterized, bounded
`STARTS WITH` query; host code handles `space_id` normalization and response
shaping.
Memory title/content reads use one fixed parameterized, id-bounded Cypher
statement with explicit projection, created-at ordering, `LIMIT`, and pinned
snapshot semantics; host code shapes missing ids.
Memory EVOLVES latest reads use one fixed parameterized successor Cypher
statement with deterministic ordering, `LIMIT`, a matching row budget, and a
pinned snapshot. Source-existence and relationship-count response shaping
remain in separate host-owned bounded query phases.
Memory EVOLVES relation counts use one fixed parameterized aggregate Cypher
statement with explicit source/target labels, relation filtering, deterministic
ordering, `LIMIT`, a matching row budget, and a pinned snapshot. Missing-id and
total-count shaping remains outside the embedded database facade.
Memory crystal synthesis counts use one fixed parameterized aggregate Cypher
statement with explicit Memory endpoint labels, `is_crystal = true`,
deterministic ordering, `LIMIT`, a matching row budget, and a pinned snapshot.
Missing-id and total-count shaping remains outside the embedded database facade.
Memory decay detail reads use one fixed parameterized exact-id Cypher statement
with explicit projection, stable ordering, `LIMIT 1`, a one-row budget, and a
pinned snapshot. Scheduler response shaping remains outside the embedded
database facade.
Memory decay refresh writes are available as
`Database::update_knowledge_memory_decay_refresh_batch`, covering the decay
scheduler's exact-id score-only and score-plus-confidence update shapes. The
typed write validates Memory ids and finite numeric decay/confidence values
before WAL, reports missing, idless, and duplicate rows without writing those
rows, and commits eligible updates through one grouped WAL batch.
Memory cleanup fingerprint reads use one fixed parameterized `m.id IN $ids`
Cypher statement with explicit projection, stable ordering, `LIMIT`, a matching
row budget, and a pinned snapshot. Request-order shaping and missing-id
calculation remain outside the embedded database facade.
Memory EVOLVES neighbor reads use separate fixed outgoing and incoming Cypher
statements. Each owns its node and relationship projection, deterministic
ordering, `LIMIT`, matching row budget, and pinned snapshot.
Memory EVOLVES projected successor reads use separate fixed per-parent Cypher
statements for stable-id and `updated_at DESC` ordering. Each statement owns its
projection, deterministic tie-breakers, `SKIP`, `LIMIT`, matching row budget,
and pinned snapshot. Parent grouping and missing-id shaping remain outside the
embedded database facade.
Memory EVOLVES creates are available as
`Database::create_knowledge_memory_evolves_batch`, covering Nowledge
`add_evolves_edge` and replacement-relation create shapes with caller-supplied
relationship metadata, fixed Memory endpoints, and grouped WAL writes.
Crystal Memory lists, key lookups, community aggregations, and source visibility
reads use separate fixed parameterized Cypher statements. The statements own
their concrete projection, cursor or community scope, deterministic ordering,
`LIMIT`, row and payload budgets, and pinned snapshot. No Crystal route-specific
read API or DTO is exposed, and these reads do not write WAL.
Memory entity mention reads use one fixed parameterized Cypher statement per
Memory with explicit projection, deterministic ordering, `LIMIT`, a matching
row budget, and a shared pinned snapshot. Grouping, missing-id handling, and
distinct-name shaping remain outside the embedded database facade.
Entity mention-count list reads use separate fixed parameterized Cypher
statements for first-page and cursor shapes, with non-empty Entity id/name
filtering, incoming `Memory` `MENTIONS` counts including zero-mention Entities,
deterministic ordering, bounded limits, pinned read-transaction snapshots, and
no WAL writes. No route-specific typed read API is exposed.
Community Entity visibility and Memory ranking reads use separate fixed
parameterized Cypher statements for optional incoming `Memory` `MENTIONS`,
direct `Memory.community_id`, unit-type filters, and crystal filters. Every
phase has an explicit scope, deterministic ordering, `LIMIT`, row and payload
budgets, and a pinned snapshot. No route-specific read API or DTO is exposed.
Related Entity name reads use separate fixed parameterized Cypher statements
for Memory-id batches and Thread-compaction paths. Each owns its distinct-name
projection, deterministic ordering, `LIMIT`, matching row budget, and pinned
snapshot; identity selection and missing-endpoint shaping remain in the host.
Thread ordered message reads use fixed exact-Thread and bounded outgoing
`CONTAINS` queries in one pinned read transaction, with
`COALESCE(r.order_index, m.order_index)` ordering and no route-specific typed
database API.
Bounded Thread list and source reads use fixed parameterized Cypher covering
Thread page, source lookup, normalized-space filtering, favorite metadata,
id/thread-id bulk lookup, and message-count ranking shapes. The statement owns
filters, ordering, projection, and limits; host code owns response shaping and
cross-statement budgets.
Thread compacted-memory reads use fixed Thread identity and bounded outgoing
`COMPACTS_TO` statements in one pinned read transaction. Each named business
phase owns its projection and ordering, avoiding both route-specific typed APIs
and the former wide fixed row.
Memory compacting-Thread reads use fixed exact-Memory and per-Memory bounded
incoming `COMPACTS_TO` statements in one pinned read transaction. Host code
preserves requested-Memory ordering, missing rows, and the cross-statement
budget while each statement owns its Thread/relationship projection and stable
ordering.
Source attribution memory reads use host-owned fixed parameterized Cypher over
incoming `SOURCED_FROM`. Hosts run separate count and page statements, bind a
bounded Source or Memory id set, project only required Memory/Source/edge
fields, and keep related phases on one pinned transaction.
Source metadata timestamp writes are available as
`Database::update_knowledge_source_metadata_batch`, covering Nowledge auto-OCR
metadata updates through one grouped WAL batch.
Source parser completion metadata writes are available as
`Database::update_knowledge_source_parsed_metadata_batch`, covering the
Nowledge parsed-state update shapes for parsed paths, file/url metadata,
summary, checksum, size, timestamps, and optional metadata through one grouped
WAL batch.
Parsed Source creation is available as
`Database::create_knowledge_source_parsed_batch`, covering Nowledge markdown,
URL, PDF, generic file, and markdown import create shapes with fixed parsed
lifecycle defaults and one grouped WAL batch for eligible new Source nodes.
Source latest-version reads use separate fixed parameterized Cypher statements
for original-name/checksum plus space filters. Source revision writes remain
available as `Database::create_knowledge_source_revision_batch` for
fixed-property `REVISED_AS` edge creation through one grouped WAL batch.
Source node cleanup is available as `Database::delete_knowledge_sources`,
covering Nowledge Source `DETACH DELETE` cleanup through the typed entity
delete path and one grouped WAL batch.
Source label assignment and cleanup writes are available as
`Database::assign_knowledge_source_labels_batch` and
`Database::delete_knowledge_source_labels_batch`, covering Nowledge
`Source`-to-`Label` `HAS_LABEL` merge/delete shapes with fixed create-only
edge properties and one grouped WAL batch for eligible relationship writes.
Bounded Source list and summary reads use named host-owned Cypher statements.
Exact detail, count, id page, summary page, and projected page remain distinct
query shapes with bound values, explicit row budgets, and pinned-snapshot
coverage. New Source fields extend a fixed projection rather than a typed route
DTO or dynamic property identifier.
Source-reference relationship cleanup for Nowledge memory/source delete flows
is available as `Database::delete_knowledge_source_reference_relationships`.
It is intentionally scoped to `RELATES_TO.source_reference`, rejects empty
references before WAL, preserves endpoint Entity nodes, and commits eligible
relationship deletes through one grouped WAL batch.
The same delete flow uses host-owned fixed parameterized Cypher for its read
guards. Endpoint discovery returns bounded relationship and endpoint identity
rows; the guard runs exact-entity, incident-count, and incoming-count statements
through one pinned transaction. Only the relationship cleanup remains typed
because it validates and commits the delete set through grouped WAL.
Entity-to-Community membership writes used by entity lifecycle community
assignment are available as `Database::create_knowledge_community_memberships_batch`.
The wrapper is fixed to `Entity` -> `Community` `BELONGS_TO` creation, validates
non-empty ids and finite strengths before WAL, reports missing or non-writable
endpoints, preserves endpoint nodes, and commits eligible memberships through
one grouped WAL batch.
Community detection result creates and scheduler summary refreshes are exposed
through `Database::update_knowledge_communities_batch`. The wrapper validates
non-empty Community ids and names, non-negative numeric counters, and finite
resolutions before WAL, reports existing, missing, duplicate, and non-writable
rows, fixes detection-result `algorithm` to `louvain`, and commits eligible
creates plus summary updates through one grouped WAL batch.
Community summary lists and detail lookups use fixed parameterized Cypher.
Summary-only and summary-presence rankings remain distinct bounded statements;
numeric `community_id` and external `id` detail lookups use separate one-row
statements. No Community list/detail route API or DTO is exposed.
GraphMeta state reads used by PageRank and community detection use host-owned
fixed, parameterized Cypher projections with explicit row budgets. Callers add
a new fixed projection when state grows and use one
`DatabaseReadTransaction` when related reads must remain pinned to a graph
version; property identifiers are never interpolated into query text.
GraphMeta stamp batches and cleanup deletes remain typed. Stamps own grouped
WAL validation and atomicity. Deletes use `meta_id` as the explicit identity,
reject empty identities before WAL, preserve missing-row no-write semantics,
and route eligible cleanup through the WAL-backed `DELETE` path.
Community node cleanup for replace-community and undo-community flows is
available as `Database::delete_knowledge_communities`. It scans only
`Community` nodes, supports the two Nowledge cleanup modes (`DELETE` and
`DETACH DELETE`), preserves non-Community endpoint nodes under detach cleanup,
does not write WAL when no Community nodes exist, and commits eligible deletes
through one grouped WAL batch.
AugmentationJob stale/orphan interruption used by background job cleanup is
available as `Database::interrupt_knowledge_augmentation_jobs`. It scans only
`AugmentationJob` nodes in `pending` or `running` state, validates the
interruption reason before WAL, marks eligible jobs as `failed` with the
production interruption message, does not write WAL when no eligible jobs
exist, and commits eligible updates through one grouped WAL batch.
AugmentationJob status and list reads use host-owned fixed parameterized Cypher
for exact lookup and separate count/page phases. Hosts select the fixed
`started_at DESC` or `created_at DESC` query variant and bind status and limit
values; they do not interpolate identifiers or rely on route-specific DTOs.
Two exact node patterns without a relationship are supported for Nowledge
source-provenance endpoint checks, for example
`MATCH (m:Memory {id: $memory_id}), (s:Source {id: $source_id}) RETURN count(m)`.
The same two-node read plan also accepts the consecutive MATCH spelling used by
entity relationship endpoint checks, for example
`MATCH (source:Entity {id: $source_entity_id}) MATCH (target:Entity {id: $target_entity_id}) RETURN source.id, target.id`.
Count-only one-hop `OPTIONAL MATCH` is supported for Nowledge thread cleanup
reads, including
`MATCH (t:Thread {id: $thread_uuid}) OPTIONAL MATCH (t)-[:CONTAINS]->(m:Message) RETURN COUNT(m)`
and the legacy extracted-reference count over already matched messages. The
Nowledge graph-analysis degree query is also supported as a narrow
row-preserving shape:
`MATCH (e:Entity) OPTIONAL MATCH (e)-[r]-() WITH e, COUNT(r) as degree RETURN e.id, e.name, degree ORDER BY degree DESC LIMIT 10`.
Direct optional source-projection plus count reads such as
`MATCH (l:Label) OPTIONAL MATCH (m:Memory)-[:HAS_LABEL]->(l) RETURN l.id, l.name, COUNT(m) AS usage_count`
are lowered to the same optional-degree operator when all non-count return
items reference the already bound source node.
Nowledge thread bulk-move reads support the normalized-space predicate shape
`CASE WHEN t.space_id IS NULL OR t.space_id = '' THEN 'default' ELSE t.space_id END =/<> $space_id`
used to treat missing and empty thread space IDs as `default`. General OPTIONAL
row preservation, general `WITH`, general `CASE`, and general `COLLECT`
expressions remain outside this bounded subset.
The companion bulk-move write shape
`MATCH (t:Thread) WHERE ... SET t.space_id = $target_space_id, t.updated_at = $updated_at RETURN t.thread_id`
is supported as a single-node update-return path over already matched rows; it
does not add generic row-binding relationship updates.
Nowledge thread distillation reads support the optional source filter shape
`($source IS NULL OR t.source = $source)` by binding parameter null checks into
constant predicates before planning. The business path uses separate fixed
parameterized count and bounded page statements in one pinned read transaction.
The page orders by the production recency expression and carries explicit
offset, limit, row, and payload budgets; no route-specific typed database API
is exposed.
The companion distilled-memory link write is available as
`Database::create_knowledge_thread_compaction_link`, which fixes the
`Thread`/`Memory`/`COMPACTS_TO` shape, validates endpoint ids and compaction
method before WAL, preserves missing or projected-idless endpoints as no-write
rows, and routes eligible creates through the WAL-backed relationship path.
Nowledge source revision history supports the bounded outgoing path read
`MATCH p = (s:Source {id: $source_id})-[:REVISED_AS*1..10]->(older:Source) RETURN older...`
by accepting an unused path binding prefix and reusing the existing finite
multi-hop expand operator. Path values remain unsupported unless a real
Nowledge caller needs to return them.
Nowledge graph path reads support the endpoint-id bounded
`ALL SHORTEST` shape
`MATCH p = (a)-[e* ALL SHORTEST 1..3]-(b) WHERE a.id = $from_id AND b.id = $to_id RETURN properties(nodes(p), 'id') AS node_ids, properties(nodes(p), 'name') AS names, length(p) AS hops`
through a dedicated shortest-path read operator. The operator returns all
simple paths at the first target depth and only supports `properties(nodes(p),
...)` plus `length(p)` projections; generic returned path values and
unbounded shortest-path searches remain outside the supported subset.
Nowledge feed reads support the bounded synthesized-source aggregation
`MATCH (c:Memory)-[:SYNTHESIZED_FROM]->(s:Memory) WHERE c.id IN $ids WITH c, COLLECT(DISTINCT s.id) AS source_ids RETURN c.id, source_ids`
as a direct group-by-property plus collected-property list. General `WITH`
projection chains and general `COLLECT` expressions remain outside this
bounded subset. Hosts combine it with a fixed bounded crystal-identity query on
one `DatabaseReadTransaction` to distinguish present crystals without sources
from missing crystal ids.
Nowledge synthesized-source coverage lookups also support the bounded grouped
aggregate filter shape
`WITH c.id AS cid, count(DISTINCT s.id) AS covered WHERE covered = $n RETURN cid LIMIT 1`
and the two-column title variant returning `cid, ct`. This is implemented as
grouped aggregation over matched rows, a column filter on the aggregate alias,
and final column projection; it is not a general HAVING implementation. Hosts
bind source Memory ids and the required coverage count, choose a fixed bounded
or unbounded query variant, and enforce an explicit result-row budget.
Nowledge community memory reads also support the bounded
`WITH m, COUNT(e) AS entity_count` shape after a one-hop relationship match.
The grouped node is represented as a projected map column, so subsequent
`m.property` and limited scalar projections such as `COALESCE(m.is_latest,
true)` read from that map column, and `ORDER BY entity_count, m.importance`
sorts before the final projection.
The same bounded group-count path accepts `COUNT(DISTINCT variable)` for
Nowledge label distribution queries such as `WITH l, COUNT(DISTINCT m) AS
memory_count`.
It also accepts the Nowledge graph-memory shape where `ORDER BY` and `LIMIT`
are attached to the aggregate `WITH` before the final `RETURN`, for example
`WITH m, COUNT(DISTINCT e) AS mention_breadth ORDER BY mention_breadth DESC,
COALESCE(m.importance, 0.5) DESC LIMIT $top_n RETURN ...`.
The same aggregate-with parser accepts multiple count items after a grouped
variable, including `COUNT(DISTINCT variable.property)` and `COUNT(*)`, for
Nowledge bridge entity reads, including the production variant that filters the
aggregate alias before final projection.
The related community bridge lookup shape also supports a bounded
post-aggregate node lookup:
`WITH e2.community_id AS other_cid, COUNT(*) AS shared_edge_count ORDER BY
shared_edge_count DESC LIMIT $limit MATCH (c:Community) WHERE c.community_id =
other_cid RETURN ...`.
The same post-aggregate lookup operator supports the Nowledge row-preserving
optional community lookup used by community memory counts, returning `NULL`
community fields when no `Community` node exists for a grouped community id.
Community list reads also support the bounded presence-ranking order expression
`CASE WHEN c.ai_summary IS NOT NULL AND c.ai_summary <> '' THEN 0 ELSE 1 END`,
used to order summarized communities before unsummarized communities.
Nowledge graph overview reads also support repeated node labels such as
`(neighbor:Entity:Memory)` as an any-of label set; unknown non-empty labels
produce an empty scan instead of falling back to all nodes. One-hop
Nowledge-used whole-record projections such as `RETURN m` and `RETURN r`
produce structured maps with stable logical identifiers, labels or relationship
type metadata, and scalar properties so repository paths like by-id memory
lookup can avoid raw graph object bindings.
One-hop
undirected relationship reads such as `-[:EVOLVES]-` are supported for the
current Nowledge neighbor and cluster queries; bounded undirected expansion
remains rejected. Anonymous relationship endpoints such as `()` and `(:Label)`
are supported for Nowledge relationship-count reads. Untyped one-hop
relationship reads such as `MATCH (a)-[r]->(b)` are supported for Nowledge
overview edge queries, and `label(r)`/`type(r)` can project the bound
relationship type. The graph-canvas id-set induced subgraph is a fixed bounded
query decoded by the host route. Nowledge-used
`RETURN` projection fallbacks support
`COALESCE(...)` and `LEFT(...)`, including nested forms such as
`COALESCE(m.title, LEFT(COALESCE(m.content, ''), 60))`, and the same limited
expression subset is available for Nowledge-used `ORDER BY COALESCE(...)`
rank fallbacks and `WHERE COALESCE(...)`/`WHERE LEFT(...)` equality and
comparison predicates. Nowledge-used case-insensitive grep predicates such as
`LOWER(COALESCE(m.content, '')) CONTAINS LOWER($needle)` and
`LOWER(e.name) = LOWER($mention)` are supported through the same limited scalar
expression evaluator; broader predicate function composition remains separate
follow-up work.
Parenthesized predicate groups preserve explicit `AND`/`OR` precedence. String
literals support escaped quotes, backslashes, and common control escapes. Graph algorithm procedure options such
as `dampingFactor`, `maxIterations`, and `maxLevels` accept bound parameters and
are type-checked before execution. `COUNT`
aggregation is implemented for `COUNT(*)`, `COUNT(variable)`, and
`COUNT(variable.property)`, including Nowledge-used `COUNT(DISTINCT variable)`
and `COUNT(DISTINCT variable.property)` forms, one-hop relationship matches,
alias-based ordering, `ORDER BY COUNT(variable)` over the projected aggregate
column, and pagination. Nowledge-used `MIN(variable.property)` is
implemented for schema verification reads such as `min(r.weight)`, and
Nowledge-used `AVG(variable.property)` is implemented for health aggregate reads
such as `avg(m.decay_score_cached)`. One-hop
relationship variables can be returned, filtered, sorted, and counted through relationship properties, for example
`RETURN r.weight`, `WHERE r.weight > 1`, `ORDER BY r.weight`, `COUNT(r)`, and
`COUNT(r.weight)`. Stable logical identities can be projected, filtered, and
ordered with `id(m)` and `id(r)` for bound node and relationship variables, and
`id()` filters can select node and relationship mutation targets without
encoding physical page locations. One-hop relationship property patterns such as
`MATCH (m:Memory)-[r:MENTIONS {weight: $weight}]->(e:Entity)` are parsed into
the relationship expand and filter reads, relationship property `SET`, and
relationship `DELETE`; relationship property patterns on bounded multi-hop
patterns are rejected until path/list relationship semantics are implemented.
Nowledge EVOLVES progression list reads are covered as a production-shaped
one-hop read over source node properties, target node properties, relationship
properties, list-parameter filters, and target-property ordering.
One-hop relationship property `SET` and relationship `DELETE` can also split
`WHERE` predicates between the source node and relationship variable, for
example `WHERE m.id = 1 AND r.weight = 3`; mixed node/relationship `OR`
predicates are rejected for mutation filtering until row-binding mutation
semantics exist.
Grouped aggregation is implemented for property projection group keys such as
`RETURN m.kind AS kind, count(*) AS total`; distinct relationship aggregation
matches Nowledge queries such as `count(DISTINCT m)` and
`count(DISTINCT e.id)`. Projection-level `RETURN DISTINCT` deduplicates
projected rows before `ORDER BY`, `SKIP`/`OFFSET`, and `LIMIT`.
Bounded relationship expansion is implemented for finite outgoing patterns such
as `[:TYPE*1..3]`, `[:TYPE*2]`, and `[:TYPE*..3]`; unbounded `*` patterns are
rejected. Mutation coverage includes single-node exact-property `MERGE`:
matching nodes do not write WAL, missing nodes are created, parameters bind
before storage access, and duplicate MERGE statements in one explicit
transaction deduplicate against pending nodes.
Nowledge schema-migration node writes also support
`MERGE (m:SchemaMigrationLog {id: $id}) ON CREATE SET m.applied_at = CURRENT_TIMESTAMP()`;
the match key remains separate from create-only values so existing migration
rows are not narrowed by metadata fields, while newly created rows persist both
the key and create-only properties in one WAL entry.
Relationship `MERGE` is implemented for exact node-property and relationship-
property patterns, reuses existing endpoint nodes, avoids WAL writes when the
full pattern already exists, and deduplicates repeated patterns inside one
explicit transaction.
Nowledge's bounded relationship-copy migration shape
`MATCH (c:Memory)-[r:CRYSTALLIZED_FROM]->(s:Memory) MERGE (c)-[n:SYNTHESIZED_FROM]->(s) ON CREATE SET n.weight = r.contribution_weight, ...`
is implemented for one-hop outgoing matched relationships. It copies selected
properties from the matched relationship into newly created relationships,
reuses existing target relationships on re-run, and persists only ordinary
relationship-create WAL records.
The companion migration verification read
`WHERE NOT EXISTS { MATCH (c)-[:SYNTHESIZED_FROM]->(s) }` is implemented as a
bounded predicate over already-bound endpoints. It checks exact source and
target node ids through the relationship adjacency index and does not introduce
general Cypher subquery execution.
The migration pair-count verification shape
`WITH DISTINCT c.id AS a, s.id AS b RETURN count(*)` is lowered to a bounded
property projection, distinct row set, and global count. Broader `WITH`
projection pipelines remain unsupported until another Nowledge scanner hit
requires them.
Nowledge GraphMeta stamp writes support the bounded
`MERGE (m:GraphMeta {meta_id: 'main'}) SET ...` shape used by PageRank,
community, and lifecycle invalidation paths. Greenfield writes fold SET values
into the create record, and repeated writes inside one explicit transaction
fold into the pending create instead of emitting standalone WAL set records.
Nowledge label assignment also supports matched-endpoint relationship merge:
`MATCH (m:Memory {id: $memory_id}), (l:Label {id: $label_id}) MERGE (m)-[r:HAS_LABEL]->(l) ON CREATE SET ...`.
The relationship match key is kept separate from create-only properties, so
existing labels are not narrowed by edge metadata while newly created HAS_LABEL
edges persist their assignment metadata in one WAL batch.
Nowledge label merge transfer supports the bounded retarget shape
`MATCH (n:Memory)-[:HAS_LABEL]->(src:Label {id: $src}) MATCH (tgt:Label {id: $tgt}) MERGE (n)-[r:HAS_LABEL]->(tgt) ON CREATE SET ...`.
The old label edge selects the source node set, the second `MATCH` selects the
new label target, and the target HAS_LABEL merge is idempotent with one grouped
WAL append for newly created edges.
Nowledge Memory label carry-over supports the bounded shape
`MATCH (older:Memory {id: $older_id})-[:HAS_LABEL]->(label:Label), (newer:Memory {id: $newer_id}) WHERE older.space_id = $space_id AND newer.space_id = $space_id MERGE (newer)-[edge:HAS_LABEL]->(label) ON CREATE SET ...`.
The two Memory endpoints must both match the explicit `space_id`; the old
Memory selects distinct Label targets, and the new Memory edge merge is
idempotent with one grouped WAL append for newly created edges.
Nowledge label upsert supports single-node `MERGE ... ON CREATE SET ... ON
MATCH SET ...` for the bounded label shape, including
`l.canonical_name = COALESCE(l.canonical_name, $canonical)`. Pending creates
inside one explicit transaction fold matched updates into the create record, so
repeated upserts do not add standalone WAL set records before commit.
Nowledge source-provenance relationship creation is implemented for the bounded
two-endpoint form
`MATCH (m:Memory {id: $memory_id}), (s:Source {id: $source_id}) CREATE (m)-[:SOURCED_FROM {...}]->(s)`.
Nowledge EVOLVES writes also support the endpoint-equality form
`MATCH (a:Memory), (b:Memory) WHERE a.id = $older_id AND b.id = $newer_id CREATE (a)-[:EVOLVES {...}]->(b)`.
The typed `Database::create_knowledge_memory_evolves_batch` facade fixes this
Nowledge shape to Memory endpoints and EVOLVES edges, validates ids and
replacement metadata before WAL, reports missing or idless endpoints without
writing, and persists eligible relationships through one grouped WAL append.
Nowledge relationship update paths support multiple assignments on the same
one-hop relationship variable, such as memory-relation review updates filtered
by `r.id`. All matched relationship property writes are emitted as one WAL
batch rather than one durable append per property. The same relationship update
path also supports Nowledge target-node property filters such as
`MATCH (m:Memory {id: $memory_id})-[r:MENTIONS]->(e:Entity {id: $target_id}) SET ...`.
One-hop relationship variable deletion is implemented for patterns such as
`MATCH (m:Memory)-[r:MENTIONS]->(e:Entity) WHERE m.id = 1 DELETE r`; it removes
only the matched relationship, preserves endpoint nodes, persists through WAL
replay, and participates in explicit transaction commit/rollback.
One-hop relationship variable property updates are implemented for patterns such
as `MATCH (m:Memory)-[r:MENTIONS]->(e:Entity) WHERE m.id = 1 SET r.weight = 2`;
they update only the matched relationship records, preserve endpoint nodes,
validate schema and constraints before WAL append, replay from WAL, and
participate in explicit transaction commit/rollback.
Single-node `MATCH ... SET` property updates maintain the property equality
index, persist through WAL replay, and participate in explicit transaction
commit/rollback. Nowledge-used integer self-increment assignments such as
`SET s.memory_count = s.memory_count + 1` are supported for source provenance
repair counters, and the access-tracking form
`SET m.access_count = COALESCE(m.access_count, 0) + 1, m.last_accessed_at = $now`
is supported as a multi-assignment node update. Broader arithmetic SET
expressions remain out of scope.
Nowledge-used `CURRENT_TIMESTAMP()` values in `CREATE` and `SET` bind to the
current UNIX epoch nanoseconds as `Int`, reusing the existing value/WAL encoding
instead of adding a separate timestamp storage type.
Nowledge-used `timestamp(expr)` values bind ISO strings such as
`1970-01-01T00:00:00`, optional `Z` suffixes, fractional seconds, and numeric
epoch values into the same epoch-nanos `Int` representation. This covers
freshness predicates such as `m.updated_at > timestamp($cutoff)` and
relationship property writes such as `created_at: timestamp($now)` without
adding a separate timestamp storage type.
Nowledge-used monthly statistics support `date_part('year', m.created_at)` and
`date_part('month', m.created_at)` as grouped aggregate projection keys over
epoch-nanos timestamps. Other `date_part` components remain outside the
compatibility subset until a real Nowledge call site requires them.
Single-node `MATCH ... DELETE` and `MATCH ... DETACH DELETE`
are implemented for the current predicate subset; regular delete rejects nodes
with attached relationships, detach delete removes attached relationships before
the node, and both paths persist through grouped WAL records. Schema DDL is
implemented for explicit node-label and relationship-type token creation through
`CREATE NODE LABEL` and `CREATE RELATIONSHIP TYPE`; both paths are idempotent,
persist through WAL replay and checkpoint, and participate in explicit
transaction commit/rollback. Explicit equality index DDL is implemented through
`CREATE INDEX ON :Label(property)`, persists through WAL replay and checkpoint,
and feeds optimizer descriptor lookup for equality predicates. Composite
equality index DDL is implemented through `CREATE INDEX ON :Label(a, b)`,
persists through WAL replay and checkpoint, maintains a rebuildable in-memory
composite key index, and enables `IndexNodeCompositeSeek` for conjunctions that
bind every indexed property. Range index DDL is implemented through
`CREATE RANGE INDEX ON :Label(property)`, persists through WAL replay and
checkpoint, and enables `IndexNodeRangeSeek` for
single-bound range predicates and conjunctive bounded range predicates.
Full-text graph index DDL is implemented through
`CREATE FULLTEXT INDEX ON :Label(property)`, persists through WAL replay and
checkpoint, maintains a rebuildable ngram candidate index over canonical string
properties, and enables `IndexNodeTextSeek` for `CONTAINS` predicates while
retaining a residual `FilterExec` for exact string containment semantics.
`WHERE` supports `AND`, `OR`, and unary `NOT` boolean predicates for the current
equality, inequality, range, null, list membership, `CONTAINS`, `STARTS WITH`,
and `ENDS WITH` predicate subset; disjunctions
currently execute as a residual filter rather than an index-union access path.
Per-label/property distinct counts and bounded sorted value histograms are
computed for eligible scalar values including `VARCHAR`, written to checkpoints,
and used by range-index costing for selectivity estimates. Declared `TEXT`, list,
map, and mixed-type groups use the conservative unknown-statistics fallback.
Histogram sampling is
deterministic and adaptive by property cardinality, preserving exact small sets
and expanding sample capacity for medium and large distinct sets. Unique
node property constraint DDL is implemented through
`CREATE CONSTRAINT ON :Label(property) ASSERT UNIQUE`; descriptors persist
through WAL replay and checkpoint, existing duplicate data rejects constraint
creation, and later `CREATE`, `MERGE`, and `SET` mutations are checked before a
WAL batch is appended. Relationship property uniqueness constraint DDL is
implemented through `CREATE CONSTRAINT ON -[:TYPE(property)]-> ASSERT UNIQUE`
with the same existing-data validation, WAL/checkpoint persistence, and
pre-WAL write validation. Node property existence constraint DDL is implemented
through `CREATE CONSTRAINT ON :Label(property) ASSERT EXISTS` and the equivalent
`ASSERT NOT NULL`; descriptors persist through WAL replay and checkpoint,
existing missing or null values reject constraint creation, and later `CREATE`,
`MERGE`, and `SET` mutations are checked before a WAL batch is appended. Node
property existence constraints use `CREATE CONSTRAINT ON :Label(property)`,
while relationship property existence constraints use
`CREATE CONSTRAINT ON -[:TYPE(property)]->`; both forms support `ASSERT EXISTS`
and `ASSERT NOT NULL`. Node
and relationship table descriptor DDL is implemented
through `CREATE NODE TABLE Name` and `CREATE RELATIONSHIP TABLE Name`; table
descriptors persist through WAL replay and checkpoint, expose a `PUBLIC` schema
state by default, and create the matching label/type token. Descriptor state
transitions are implemented through `ALTER ... SET STATE` for table descriptors
and property descriptors, persist through WAL replay and checkpoint, and expose
`DELETE_ONLY`, `WRITE_ONLY`, `BACKFILL`, `VALIDATING`, `PUBLIC`, and `GC`.
Only `PUBLIC` table and property descriptors participate in write-time and
recovery validation; promoting a property descriptor to `PUBLIC` validates
existing records before the state-change WAL batch is appended. Property-level
table schema DDL is implemented through
`CREATE PROPERTY ON NODE TABLE Name(property) TYPE Type` and
`CREATE PROPERTY ON RELATIONSHIP TABLE Name(property) TYPE Type`, supports
optional `NOT NULL`, persists through WAL replay and checkpoint, validates
existing records before descriptor creation, and checks later writes before a
WAL batch is appended. `Database::run_schema_maintenance` advances
`BACKFILL` descriptors to `VALIDATING`, validates `VALIDATING` descriptors
before advancing them to `PUBLIC`, and removes `GC` descriptors through a single
grouped WAL batch. `Database::plan_schema_maintenance` exposes a read-only
dry-run report with per-object source/target states and estimated operation
counts so caller-owned background loops can decide admission before taking the
writer. `Database::run_bounded_schema_maintenance` can then apply a prefix of
complete descriptor-level maintenance actions that fit the caller's operation
budget, leaving the rest resumable through later maintenance calls. The bounded
background variants bind that same budget to QoS admission and execution for
per-tick internal loops. Composite and full-text property-index execution
projections expose `Database::rebuild_bounded_property_index_projections` for
bounded descriptor-level rebuild reports without making those projections
canonical durability. The same work can be exposed as a rankable `Projection`
background plan or executed through bounded background/scheduled wrappers that
charge QoS admission against the descriptor rebuild budget. Projected graph
derived artifacts can be refreshed through a report-oriented
`Database::rebuild_derived_artifacts` entry point; search projection artifacts
expose the same report-oriented rebuild shape through
`SearchIndex::rebuild_derived_artifacts`. Search projections also expose
bounded incremental deltas for ordinary FTS/BM25 row upsert/delete changes:
`SearchIndex::apply_projection_delta` accepts an operation budget and fails
without partial index mutation on budget or embedding-dimension errors;
`Database::apply_search_projection_delta` exposes the same caller-owned
projection boundary beside graph operations without moving search state into
the graph WAL.
Caller-owned search projections can inject `SearchAnalyzerLexicon` rules for
Nowledge lifecycle and schema vocabulary; normalized alias and stopword rules
accept readable phrases or identifiers and keep application vocabulary and
high-frequency application noise terms out of the default graph kernel lexicon.
Internal background callers can use `SearchIndex::apply_background_projection_delta`
or `Database::apply_background_search_projection_delta` to pass the same delta
through `LocalQosPolicy` admission before applying it; callers that need
in-flight background budget tracking can use
`SearchIndex::apply_scheduled_background_projection_delta` or
`Database::apply_scheduled_background_search_projection_delta` with
`LocalQosScheduler`. Full search rebuilds expose a rankable background plan and
background/scheduled rebuild wrappers so internal loops can charge the scan
estimate before replacing the projection. Graph-derived metadata repair exposes
the same split through `SearchIndex::metadata_repair_background_work_plan`,
`SearchIndex::repair_background_metadata_from_graph`, and
`SearchIndex::repair_scheduled_background_metadata_from_graph` while preserving
its bounded, metadata-only repair semantics.
`Database::background_maintenance_candidates` and
`Database::rank_background_maintenance` provide a caller-owned scheduling
surface that gathers pending schema maintenance, property-index projection
rebuilds, missing or stale out-of-core optimizer statistics, search
rebuild/repair work, graph-derived search deltas, and external content artifact
jobs into named `BackgroundWorkPlan`s. The API returns ranked
plans and QoS decisions only; it does not spawn workers or execute background
work on behalf of the embedded application. Executable search-projection graph
delta candidates expose operation counts, upsert/delete counts, optional
max-operation limits, and the complete-through graph commit epoch, so the
application can distinguish bounded incremental FTS maintenance from
freshness-lag planning signals. Background maintenance summaries also expose
typed active-topic, recent-delta, source-graph-lag, query-probability,
staleness, freshness-SLO, and tenant-budget hints, so callers do not need to
parse human-readable ranking reasons. Tenant budget hints below a candidate's
estimated operations are ranked as deferred background work, while foreground
user-triggered rebuilds, deltas, and repairs can still use the direct APIs.
Database-owned
derived artifact jobs expose the same split through
`Database::run_next_background_derived_artifact_job`, which admits internal
background rebuild work through `LocalQosPolicy` while leaving the direct
`run_next_derived_artifact_job` path available for explicit callers. Callers
that want the engine to track in-flight background operation budgets can use
`LocalQosScheduler` with
`Database::run_next_scheduled_background_derived_artifact_job`; this remains
synchronous and caller-driven rather than a built-in thread pool. Schema
maintenance follows the same foreground/background split:
`Database::run_schema_maintenance` remains the explicit, ungated caller path,
while `Database::run_planned_background_schema_maintenance` and
`Database::run_planned_scheduled_background_schema_maintenance` charge the
current dry-run estimate to the `Mutation` background lane before advancing
schema descriptors or appending maintenance WAL. The lower-level
`run_background_schema_maintenance` and
`run_scheduled_background_schema_maintenance` variants remain available when a
caller has its own estimate. `Database::schema_maintenance_background_work_plan`
also lets caller-owned loops rank pending schema maintenance beside projection,
import, analytics, and shadow work before admission. These wrappers preserve the
existing single-batch validation semantics and only add
admission/accounting. Content
artifact jobs are scheduled at the same boundary; callers can attach structured
job payloads for object
references, checksums, parser hints, and projection targets. The default
graph-kernel runner rejects those jobs while preserving the payload in the job
report, and `Database::run_next_external_content_artifact_job_with` lets a
caller-owned content runtime complete parsing/crawling/chunking jobs without
embedding that runtime in Skein. Successful external content jobs retain the
runtime's last structured `QueryOutput` on the job ledger so callers can audit
published projection refs, parser versions, checksums, chunk counts, and other
small lineage fields without storing large parsed content in the graph kernel.
Caller-owned runtimes can read those successful lineage rows through bounded
`Database::succeeded_external_content_artifact_jobs` or action-scoped
`Database::succeeded_external_content_artifact_jobs_for_action` views instead
of scanning the full derived-artifact history.
`ExternalContentArtifactJobCompletion` provides a standard lightweight
completion manifest for caller-owned parser/crawler runtimes that want to report
runtime identity, input/output refs, checksums, projection refs, source graph
epoch, produced-row counts, and small metadata without storing parser payloads
in the graph kernel. `Database::complete_next_external_content_artifact_job_with`
and `Database::complete_external_content_artifact_job_with` convert that
manifest into the retained audit row. The matching background and scheduled
completion runners preserve the same row shape while charging parser/crawler
work to the `Import` QoS lane.
`ExternalContentArtifactRuntimeManifest` lets caller-owned parser/crawler loops
declare supported actions, required payload keys, runtime version, and estimated
operation cost so Skein can expose bounded claimable-job views and a matching
Import-lane background work plan without treating the manifest as a sandbox or
execution permission.
Internal parser/crawler loops can use
`Database::run_next_background_external_content_artifact_job_with` or
`Database::run_next_scheduled_background_external_content_artifact_job_with` to
charge that work to the `Import` background lane while leaving explicit runtime
calls ungated. Action-specific background loops can use
`Database::run_next_background_external_content_artifact_job_for_action_with` or
`Database::run_next_scheduled_background_external_content_artifact_job_for_action_with`
to charge only their own pending work to the same `Import` lane. Before claiming
work, caller-owned loops can expose pending parser/crawler work through
`Database::external_content_artifact_job_background_work_plan` or
`Database::external_content_artifact_job_background_work_plan_for_action`, then
rank it beside projection, schema maintenance, analytics, and shadow work with
the normal `LocalQosPolicy` and `LocalQosScheduler` surfaces. If the runtime
first polls a bounded pending list and chooses a specific job,
`Database::run_background_external_content_artifact_job_with` and
`Database::run_scheduled_background_external_content_artifact_job_with` apply
the same Import-lane admission to that concrete job. Action-specific runtimes
can also poll failed jobs for their own action through
`Database::failed_external_content_artifact_jobs_for_action` and requeue only
their own failed work through
`Database::retry_failed_external_content_artifact_job_for_action`. They can also
read `Database::external_content_artifact_job_summary_for_action` before
claiming work, so resource-constrained parser/crawler loops can account for
only their own pending and failed queue pressure.

An internal compatibility fixture harness is implemented for Nowledge-shaped
query families. It runs setup statements, parameterized Cypher checks, expected
row comparisons, plan-shape assertions, and projected graph checks through the
public `Database` facade. The fixture harness also exposes a generic shadow
engine interface that applies the same setup statements to a second engine,
compares Cypher check rows against the primary Skein run, compares declared
error classes for failing Cypher checks, compares mutation effects through
follow-up effect queries, and can compare projected graph outputs from shadow
engines that implement the projection hook. Engines without that hook still
report projected graph checks as primary-only. `ExternalShadowCommand` provides
a JSON-lines process adapter for this interface, so a Ladybug/Kuzu wrapper can
be attached as a subprocess without linking Kuzu or Python into Skein.
The current fixture covers indexed parameter lookup, null predicates, list
predicates with pagination, entity alias list lookup, entity reuse lookup reads,
entity temporal metadata create/update writes, current timestamp writes,
entity `MENTIONS` and temporal `RELATES_TO` creation writes, entity count reads,
label resolver null-canonical scans/backfills, label rename collision guards,
label existence reads, label remove-all count/delete writes,
source provenance Source endpoint checks, full `SOURCED_FROM` creation writes,
edge-existence/global counts, exact repair candidate scans, source memory-count
reads,
PageRank membership/visibility reads, score persist/clear writes, central-entity
lookup, GraphMeta clear stamps, planner node/relationship totals, and changed
count reads,
community scheduler GraphMeta, candidate scan, member entity, and summary
write queries,
cleanup scheduler seed scans, bounded EVOLVES pair reads, cleanup fingerprint
row fetches, floor-zero engagement `CASE` ordering, and decay scheduler
EVOLVES/crystal synthesis count reads,
wiki export summary entity/crystal/community count reads, topic entity/crystal
ranking reads, entity listing mention-count and cursor reads, community entity anchor
visibility reads, entity id-or-name lookup and community context reads,
community crystal source visibility reads, and community top memory ranking reads,
OKF export community list, entity list, crystal list, and crystal-source entity
community reads, shared OKF/wiki entity mention detail reads, and shared OKF/wiki
related entity reads, plus OKF memory row exports with row-preserving label
collection and OKF label row exports,
schema migration and label node `MERGE ON CREATE SET ... ON MATCH SET`,
GraphMeta `MERGE SET`, matched HAS_LABEL relationship `MERGE ON CREATE SET`,
label canonical lookup, dynamic label updates, label-edge removal, and label
node `DETACH DELETE`,
source-node `DETACH DELETE` cascade checks,
source metadata `timestamp(...)` update writes,
source attribution reads,
source fan-in relationship count reads,
thread compaction attribution reads,
memory created-at bulk reads, memory bulk metadata and space reads,
memory filtered list reads,
entity relationship endpoint checks, count-only optional thread cleanup reads,
thread message target `DETACH DELETE` cleanup writes,
top-entities-by-degree graph analysis reads,
node-detail neighbor count reads,
label usage count reads including direct optional source-projection counts,
agent context activity digest reads,
agent context activity task reads,
agent context stale crystal and EVOLVES cluster reads,
health stale memory count reads,
entity lifecycle impact counts, detail projections, relation/label/community
preview reads, REST write Entity pre-delete guard counts expressed as five
fixed queries on one snapshot, and entity-node `DETACH DELETE` cascade checks,
graph orphan entity reads and cleanup-candidate reads with one-hop relationship
existence predicates,
AugmentationJob lifecycle create/running/progress/completed/failed writes and
status/list reads,
undo-community detection writes for deleting Community nodes, clearing
`community_id`, and resetting GraphMeta community freshness,
CRYSTALLIZED_FROM-to-SYNTHESIZED_FROM relationship-copy migration writes,
thread bulk-move normalized-space selection reads and update-return writes,
thread distillation optional source filters,
context memory semantic-unit title, typed, and label preview reads,
feed synthesized-source id collection reads,
skill synthesized memory id reads,
skill stage projection, active, and builder list reads,
skill detail reads,
skill synthesized memory direct-id and detail reads,
typed Skill synthesized-memory evidence reads,
skill metadata and version reads, metadata writes, and usage-stat writes,
learning memory latest reads,
source parsed path list reads,
community summarized list reads,
community memory type-filter reads,
community entity memory-count reads,
entity mention-count list reads,
community memory coalesced summary reads,
source detail memory-count reads,
source provenance memory detail reads,
source memory id list reads,
memory source provenance id reads, memory metadata update writes,
source memory-count decrement-floor writes,
source relationship source-reference count reads,
source relationship source-reference deletes,
memory compact detail fallback reads,
memory label name list reads,
memory label bulk name reads,
memory label fallback bulk reads,
memory label endpoint bulk reads,
memory label distinct name count reads,
label regex memory connection aggregate reads,
source label bulk name reads,
source label endpoint bulk reads,
source label relationship count reads,
source label relationship merge writes,
source label relationship delete writes,
source detail normalized-space reads,
source detail chunk-count reads,
source detail file-path reads,
source default metadata fallback reads,
source list fallback page reads,
source overview memory-count ranking reads,
source bulk summary fallback reads,
source count reads,
source extracted id list reads,
source extracted lifecycle mark-indexed writes,
source lifecycle indexed chunk-count writes,
source lifecycle state update writes,
source space update writes,
source bulk normalized-space move writes,
source normalized-space id list reads,
memory normalized-space id list reads,
memory bulk normalized-space move writes,
memory normalized-space limit-one reads,
memory candidate normalized-space id reads,
memory candidate normalized-space exclusion reads,
memory normalized-space count reads,
memory normalized-space limited id reads,
memory id-list normalized-space move-returning writes,
memory id-list normalized-space exclusion move-returning writes,
thread normalized-space id pair reads,
thread bulk node-id normalized-space move writes,
thread identity normalized-space id reads, thread metadata reads and update writes,
thread identity bulk normalized-space move writes,
thread normalized-space logical id reads,
memory entity name list reads,
memory entity endpoint bulk reads,
memory review-status bulk reads,
memory ranked overview reads,
thread message ordered reads,
thread compacted-memory count reads,
thread compacted-memory summary reads,
source coverage aggregate-filter reads,
timestamp and `CAST(... AS TIMESTAMP)` cutoff predicates,
relationship min aggregation, scheduler fingerprint max aggregation, node property pattern
reads, anonymous-endpoint relationship counts, one-hop undirected relationship reads, grouped and distinct
aggregation, one-hop relationship property reads through relationship pattern
property filters, source-provenance endpoint existence checks and relationship
creation between already matched endpoint nodes, source revision history bounded
path reads, source fan-in relationship count reads, source metadata
`timestamp(...)` update writes, label canonical
lookup, dynamic label updates, label-edge removal, label node `DETACH DELETE`,
thread message target `DETACH DELETE` cleanup writes,
endpoint-equality EVOLVES relationship creation, source-node `DETACH DELETE`
cascade checks, untyped overview-edge relationship type projection,
Nowledge-style projection fallback expressions and fallback ordering,
relationship property mutation effects with relationship-variable `WHERE`
filters, and relationship-type filtered projected graph construction.
It also covers production-shaped projected graph definition, PageRank, and
hierarchical Louvain procedure calls, with
fixture-declared floating-point tolerances for shadow row comparison and
projected graph PageRank parity. A cutover assessment gate now consumes shadow
reports and returns `Ready` only when the configured minimum matched-check count
is met and, by default, every check is covered by the shadow engine. The
remaining compatibility work is to implement the nowledge/mem Ladybug/Kuzu
wrapper process for that adapter and run the production fixture set through it.

### Phase 4: Costed Cascades Search

Scope:

- split the flat MVP into stable API, parser, catalog, planner, optimizer,
  storage, search, compatibility, and executor boundaries; use internal module
  splits first when public contracts are still moving, and promote boundaries to
  workspace packages only when the dependency direction is acyclic and stable
- keep query-shape-specific read DTOs out of the public facade; retain typed
  surfaces only for grouped WAL, recovery, admission, generation publication,
  and bounded multi-statement contracts, and route integration work through
  parameterized Cypher, query reports, and the shared planner/executor boundary
- `skein-cypher` owns syntax-only AST and parser modules, while root
  `src/cypher.rs` remains a compatibility re-export facade
- Chryso-style rule and cost interfaces
- crate-owned generic memo storage with root-owned graph expression payloads
- persistent statistics and index descriptors
- pattern join ordering and scan/seek/expand costing
- optimizer budget, trace, and plan fingerprint regression tests

Exit gate:

- plan selection changes only with an explainable statistics or rule change
- optimizer-only benchmarks catch search-space regressions
- plan snapshots are deterministic across runs

Current implemented slice:

- persistent equality index descriptors for observed label/property pairs
- checkpointed graph statistics for total nodes, total relationships,
  per-label counts, per-relationship-type counts, relationship-type source
  and target counts, label/type/label one-hop path cardinalities, bounded exact
  multi-hop path cardinalities, per-label/property distinct-value counts, and
  per-relationship-type/property distinct-value counts plus relationship
  property histograms
- statistics freshness metadata with the commit epoch used to compute the
  snapshot, plus histogram sample-limit and node/relationship per-histogram
  sampled/exact markers
- public facade access to index descriptors and statistics for compatibility
  checks and future optimizer costing
- metadata-aware scan/seek costing for simple label plus equality predicates:
  missing descriptors keep `SeqNodeScan + Filter`, selective predicates choose
  `IndexNodeSeek`, and low-selectivity predicates can keep the scan path with an
  explainable optimizer trace decision; indexed property-list predicates can
  choose `IndexNodeMultiSeek` for Nowledge feed and source coverage shapes such
  as `WHERE c.id IN $ids`, including cost-based selection among equality and
  list predicates inside the same conjunction
- metadata-aware expand cardinality estimates for bounded outgoing patterns:
  the optimizer consumes label/type/label path counts and relationship fanout
  summaries, applies relationship-property distinct counts for property pattern
  filters and one-hop relationship-variable equality predicates pushed down
  from `WHERE`, and uses relationship-property histograms for range filters
  over relationship variables, then records per-hop exact/fallback row
  estimates and total estimated rows in the explain trace while keeping the current
  `AdjacencyExpandExec` implementation stable
- `OptimizerTrace::selected_plan_cost` exposes recursive output-row and cost
  estimates for the chosen physical plan; expand rows are scaled by the
  selected input cardinality, so selective seek inputs no longer make the trace
  report full-label expand cost; `selected_plan_cost_breakdown` exposes the
  same selected-plan scalar cost as structured CPU, random-I/O,
  sequential-I/O, and output-row components from `skein-optimizer`; endpoint
  cartesian products report estimated left/right rows, output rows, and product
  cost
- `OptimizerTrace::selected_plan_operator_counts` and
  `OptimizerTrace::selected_plan_class_counts` expose stable selected-plan
  histograms derived from crate-owned physical plan kind/class/children
  metadata and generic `PlanNode` traversal helpers, so diagnostics do not need
  to parse English explain text to detect operator mix or
  schema/mutation/access/traversal/relational/procedure composition
- `OptimizerTrace::selected_plan_properties` exposes conservative delivered
  physical properties for the chosen plan: embedded local plans report
  single-node distribution, and only proven `SortExec` ordering plus
  order-preserving unary wrappers are surfaced. This keeps property diagnostics
  typed without overclaiming index or traversal ordering before enforcer rules
  exist
- `skein-optimizer` owns `OptimizationSearchReport`, `SearchMode`,
  `RuleEvent`, `RuleOutcome`, `SelectedPlanTrace`, and storage-independent
  rule identity/application/batch-runner abstractions, so group-budget fallback
  warnings, selected-plan trace materialization, deterministic rule ordering,
  and rule event recording are crate-level optimizer scaffolding while root
  `src/optimizer.rs` still owns Cypher-specific graph rules and
  catalog-dependent costing
- the first graph-specific implementation rule now uses that scaffold for
  single-property node equality seeks, preserving the existing physical plan and
  fingerprint while recording a stable
  `implementation:node_equality_index_seek` rule event in the optimizer trace
- single-property node `IN` predicates now use the same implementation-rule
  path for `IndexNodeMultiSeek`, preserving legacy decision strings while
  adding a stable `implementation:node_in_index_multi_seek` rule event for
  feed/source lookup diagnostics
- single-property node range predicates now use the implementation-rule path
  for `IndexNodeRangeSeek`, preserving legacy decision strings while adding a
  stable `implementation:node_range_index_seek` rule event for range-index
  diagnostics
- single-property node full-text predicates now use the implementation-rule
  path for `IndexNodeTextSeek`, preserving the residual filter and legacy
  decision string while adding a stable `implementation:node_text_index_seek`
  rule event for FTS/retrieval diagnostics
- conjunctions that cover a full composite equality index now use the
  implementation-rule path for `IndexNodeCompositeSeek`, preserving the residual
  filter and legacy decision string while adding a stable
  `implementation:node_composite_index_seek` rule event for multi-property
  lookup diagnostics
- conjunctions with single-property equality or `IN` index candidates now use
  the implementation-rule path for the existing cost-based best-candidate
  selection, preserving residual filters and legacy decision strings while
  adding a stable `implementation:node_conjunction_index_seek` rule event
- `skein explain-json [--params-json <json-object>] <database-path> <cypher>`
  opens the database read-only and prints the selected plan, fingerprint,
  optimizer search mode, recursive cost, cost breakdown, typed parameter echo,
  effective `WorkRequest` from `SET system.*` or `SET SYSTEM VARIABLE`
  defaults and `CYPHER system.*` hints, warnings, decisions, structured
  operator/class histograms, plan-cache
  hit/miss/eviction counters, and typed rule events as stable JSON for
  migration gates, resource-scheduling dashboards, and CI artifacts;
  legacy decision strings remain for compatibility, while `rule_events` exposes
  `rule`, `outcome`, and `detail` fields for implementation-rule diagnostics
  without parsing English explain text
- `skein explain` and `skein explain-analyze` use the same read-only embedded
  API path and print a TiDB-style operator tree table for interactive use;
  unavailable per-operator estimates or runtime counters are rendered as
  `N/A` instead of being inferred from root-level metrics
- endpoint cartesian products whose flattened inputs all estimate to one row
  choose a stable left-deep physical input order by child cost and fingerprint,
  covering Nowledge endpoint-existence checks without changing broader
  unordered multi-row product semantics
- residual node-property filters use the selected physical plan to recover the
  filtered node variable's label and apply node-property distinct counts or
  histograms for equality, `IN`, and range predicates, so low-selectivity scan
  fallbacks and cross-pattern filters are not forced through the generic
  half-selectivity fallback; residual read-side `id(variable)` filters use
  one-row equality, input-minus-one inequality, and literal-list width estimates
  once the selected physical plan proves the variable is a node or relationship
- grouped aggregate cost estimation uses selected-plan variable labels/types
  and explicit node-property or relationship-property distinct counts for
  simple property group keys, and adds bounded work cost for distinct property
  aggregate targets such as `COUNT(DISTINCT e2.community_id)` plus distinct
  variable targets such as `COUNT(DISTINCT m)`, while non-property or
  missing-statistics grouping keeps the conservative fallback estimate
- optional degree and relationship count-sum costing use source label/property
  distinct counts plus relationship type/source counts for outgoing legs and
  relationship type/target counts for incoming legs in Nowledge cleanup,
  extracted-reference, and mention-count paths instead of a fixed leg-count
  constant
- deterministic physical plan fingerprints are exposed through
  `OptimizerTrace::selected_plan_fingerprint` and `PhysicalPlan::fingerprint`
  for regression tests and future compatibility/shadow comparisons
- optimizer memo search has a hard group budget: when the logical group demand
  exceeds `OptimizerConfig::max_groups`, the optimizer skips memo construction,
  emits an explain warning, and uses a deterministic direct physical fallback
  that preserves current scan/seek and expand diagnostics
- `cargo bench --bench optimizer_smoke` covers optimizer-only range-seek plus
  bounded expand stats, composite seek, text seek, low-selectivity scan
  fallback, production-shaped selective seed + bounded expand + aggregate +
  sort/limit, one-hop relationship-property expand reads, a Nowledge-shaped
  pushed-down relationship equality plus relationship range-filter workload, a
  source-to-memory-to-label cross-pattern aggregate workload, a larger
  source-to-memory-to-entity-to-label grouped workload,
  community-to-synthesized-source coverage aggregate workload plus the
  aggregate-alias coverage filter shape used by source coverage checks,
  feed synthesized-source collection reads backed by indexed `IN` seeks,
  entity bridge-span distinct-property aggregate workload,
  thread-cleanup optional relationship count-sum workload with seed/fanout cost
  tracing, incoming mention optional relationship count-sum and optional degree
  workloads backed by relationship target statistics,
  source-attributed entity community export aggregation with memory unit-type
  filtering and distinct memory/entity counts,
  Skill-to-synthesized-Memory-to-compacting-Thread provenance reads, community
  membership-to-Memory evidence aggregates,
  endpoint-existence and nested endpoint-existence cartesian product cost
  tracing and single-row input ordering, residual node-property filter
  equality/inequality/`IN`/range/null selectivity, residual relationship-property
  inequality/`IN`/null selectivity, residual read-side relationship-id filter
  selectivity, residual string predicate selectivity without double-counting
  full-text index candidates, constant/`OR` predicate selectivity for optional
  parameter filters, grouped node-property aggregate cardinality, selected-plan
  cost stability, deterministic fingerprints, budget-fallback paths, one-hop
  and bounded multi-hop path source/target coverage distinct statistics for
  distinct variable aggregate costing without depending on a storage fixture,
  plus optimizer smoke coverage for bounded multi-hop distinct-target
  aggregates. The benchmark also emits `optimizer_smoke_summaries` and
  `optimizer_smoke_summaries_json` lines with stable per-case rows, cost,
  operator/class counts, and shortened fingerprints so CI and release notes can
  compare Nowledge-shaped optimizer plan drift without parsing full explain
  output. Automation should consume the JSON line when possible

Remaining Phase 4 work:

- richer cross-pattern statistics beyond residual filters, grouped
  node-property aggregates, and bounded path coverage distinct counts
- alternative expand implementation candidates and pattern join-order
  enumeration once multi-pattern logical plans exist
- bounded left-deep join-order enumeration beyond the current all-single-row
  endpoint-product ordering
- broader cross-pattern workload-shaped optimizer benchmark suites beyond the
  current source/memory/entity/community/label/skill smoke cases

### Phase 5: Analytics and Cutover

Scope:

- immutable CSR/CSC projected graph snapshots
- PageRank and Louvain-compatible entry points
- background projection rebuild and versioning
- production shadow reads against Ladybug
- export, rollback, and cutover tooling

Exit gate:

- algorithm outputs meet defined parity tolerances
- shadow reads show no semantic drift on production-shaped fixtures
- rollback can reopen the previous Ladybug database without converting it in
  place

Current implemented slice:

- immutable in-memory CSR/CSC projected graph snapshots derived from the
  canonical graph store or a read-transaction snapshot
- optional node-label and relationship-type filtering for projected graph
  construction
- WAL/checkpoint-persisted projected graph definitions; algorithm calls rebuild
  CSR/CSC snapshots from the canonical graph state at execution time
- checkpoint-generated projected graph artifacts with node IDs, CSR outgoing
  adjacency, CSC incoming adjacency, atomic replacement, and checksum
  validation/discard on recovery
- projected graph artifact format versioning, projection epochs, and public
  status reporting for reusable/stale artifact state
- execution-path reuse of cached projected graph artifacts when commit epoch and
  projected graph definition still match the active store
- explicit background rebuild API that refreshes projected graph artifacts
  without appending WAL, truncating WAL, or publishing a checkpoint manifest
- report-oriented derived artifact rebuild API for projected graph artifacts
- embedded derived-artifact job queue for projected graph rebuilds with
  pending/running/succeeded/failed status, attempt counts, and last-error
  reporting; the queue is intentionally synchronous and caller-driven for the
  embedded engine
- Kuzu-style Cypher procedure entry points:
  `CALL project_graph('Graph', ['Label'], ['TYPE'])`,
  `CALL page_rank('Graph', dampingFactor := 0.85, maxIterations := 20)
  RETURN node, pagerank_score`, and
  `CALL louvain('Graph') RETURN node, louvain_id`
- hierarchical Louvain execution with `maxLevels := N` and optional
  `RETURN node, level, louvain_id`
- PageRank scoring over projected snapshots with dangling-node redistribution
- reverse traversal over incoming CSC sources for analytics and compatibility
  checks
- deterministic Louvain-compatible community assignment over projected snapshots
- fixture-declared floating-point parity tolerances for PageRank rows and
  projected graph shadow outputs
- production-shaped compatibility fixtures for `project_graph`, `page_rank`,
  and hierarchical `louvain` procedure execution
- compatibility cutover gate that converts a shadow report into `Ready` or
  `Blocked` with explicit primary-only coverage blockers
- caller-owned rollback evidence fields in the migration gate so release
  automation can require proof that the previous local graph database can still
  be reopened without making the graph kernel open that database

Remaining Phase 5 work:

- richer caller-owned blob/content parser runtime integration outside the graph
  kernel
- optional `ExternalShadowCommand` wiring to the previous local graph wrapper
  when a specific migration gate needs compatibility evidence
- rollback execution tooling owned by the migration/release layer

## First Implemented Compatibility Slice: Parameters

Skein now represents a parsed value as either a literal or a named parameter.
The planner binds parameters into typed values before creating a logical plan.
The optimizer, executor, and store therefore never handle unresolved parameter
tokens.

The embedded API provides parameterized forms for query, explain, and mutation
transactions. Missing parameters produce a semantic error before any mutation
is appended to the WAL. Extra parameters are tolerated, parameter names are
case-sensitive, and labels or property identifiers cannot be parameterized.

This slice was selected before MVCC because the current Nowledge wrapper relies
heavily on parameterized Cypher. It also proves the intended parser/semantic
boundary without committing to the later storage concurrency design.

## Validation Commands

```bash
cargo fmt --all -- --check
cargo test
cargo clippy --all-targets -- -D warnings
```

The next implementation should build the dual-engine compatibility harness,
then use its first failing production fixture to choose between MVCC work and
additional Cypher coverage. This keeps development driven by the real
replacement boundary rather than by broad openCypher completeness.
