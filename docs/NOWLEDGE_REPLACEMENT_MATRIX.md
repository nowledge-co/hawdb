# Nowledge Replacement Matrix

This document maps HawDB against the current Nowledge Mem local data-plane
needs. The source product boundary is:

Application-facing replacement paths are query-first. Parameterized Cypher and
PostgreSQL-dialect SQL are the production integration surface; old typed
business CRUD and route response facades are removed from both release and test
builds. Route-evidence query fixtures are module-private. Stable typed APIs
remain only for route-neutral kernel contracts listed in
`docs/specs/QUERY_FIRST_PUBLIC_API_SPEC.md`.

- Kuzu/Ladybug is the graph of record for memories, threads, sources, entities,
  relationships, schema migrations, graph algorithms, checkpointing, and
  storage-version checks.
- LanceDB is a rebuildable search projection for semantic vectors, FTS/BM25,
  denormalized filter metadata, and search-table lifecycle markers.
- Large content blobs remain outside the graph/search engine.
- Cloud remains PostgreSQL-first and should not embed HawDB as canonical graph
  storage.

## Kuzu/Ladybug Replacement Surface

| Capability | Nowledge need | HawDB status |
|---|---|---|
| Embedded open/create by path | One local database object per workspace path | Partial: `Database::open`, `GraphStore::open` |
| Read-only and bounded resource configuration | Local callers need read-only opens and hard query budgets | Partial: `DatabaseConfig::read_only` opens only existing database directories without creating missing paths, then rejects Cypher mutations, transaction mutations, checkpoints, schema maintenance, database-owned projected graph artifact rebuilds, and derived artifact job execution before they write database-owned state; content/blob parser artifact jobs are recorded at the derived-artifact boundary with optional structured payloads for caller-owned object references, checksums, parser hints, and projection targets; `Database::external_content_artifact_job_summary` exposes aggregate pending/failed/succeeded health plus action-grouped pending/failed counts, next pending, and oldest failed job ids, `Database::external_content_artifact_job_summary_for_action` exposes the same bounded health counters for one caller-owned action, `Database::pending_external_content_artifact_jobs` exposes a bounded pending poll surface for caller-owned runtimes, `Database::pending_external_content_artifact_jobs_for_action` lets action-specific runtimes poll only work they can handle, `ExternalContentArtifactRuntimeManifest` lets runtimes declare supported actions, required payload keys, version, and estimated operations for bounded claimable-job polling, manifest-scoped run/complete, and Import-lane work planning, `Database::succeeded_external_content_artifact_jobs` plus `Database::succeeded_external_content_artifact_jobs_for_action` expose bounded successful lineage/output rows, and `Database::failed_external_content_artifact_jobs_for_action` plus `Database::retry_failed_external_content_artifact_job_for_action` let those runtimes inspect and requeue only their own failed retry queue; the default graph-kernel runner rejects them as graph-kernel-external work while preserving payloads in job reports, `Database::run_next_external_content_artifact_job_with` plus `Database::run_next_external_content_artifact_job_for_action_with` let caller-owned runtimes complete those jobs and publish rebuildable projections back to HawDB, `ExternalContentArtifactJobCompletion` plus direct completion runners standardize lightweight runtime/input/output/projection/checksum lineage rows without storing parser results in the graph kernel, successful external content jobs retain their last structured output rows for lightweight lineage/audit, `Database::external_content_artifact_job_background_work_plan` and `Database::external_content_artifact_job_background_work_plan_for_action` expose rankable `Import` work plans for pending parser/crawler queues, background wrappers, including action-scoped and manifest-scoped background runners, can charge internal parser/crawler loops to the `Import` QoS lane, explicit search-projection graph-delta maintenance candidates carry the exact executable delta request through ranking, changefeed-backed freshness candidates can derive precise executable search delta requests from source graph commit epochs when the log range covers the caller's freshness, `DatabaseConfig::max_search_projection_change_log_entries` bounds the retained changefeed window for incremental search projection and forces full rebuild when caller freshness falls behind the retained range, freshness candidates fall back to non-executable planning signals when the change log is too new and a full rebuild is required, bounded schema-maintenance background wrappers admit against the actual executable plan cost rather than the caller cap, and over-limit search-projection graph deltas are excluded from background maintenance candidates while direct execution still returns a hard-limit error; `DatabaseConfig::max_read_result_rows` caps direct read query and read-transaction result rows; `DatabaseConfig::max_optimizer_groups` caps cascades memo search groups with deterministic direct physical fallback warnings; every database open uses strict WAL replay, while the separate typed `DatabaseDoctor` API requires a generation-bound dry run and exact data-loss acknowledgement before it quarantines, audits, and truncates an incomplete final record; `DatabaseConfig::max_wal_replay_entries` caps startup WAL replay after valid record decode and before applying the next top-level WAL record, preserving batch replay atomicity; mutation queries keep WAL/commit semantics and are not failed after durable execution |
| Storage version | Boot compatibility checks | Partial: `storage_version()` plus explicit boot-time manifest and checkpoint storage-version validation with unsupported-version errors |
| Cypher reads | `MATCH`, `WHERE`, `RETURN`, ordering, pagination, aggregation, parameter binding | Partial: single-node `MATCH`, comma-separated and consecutive two exact node patterns for Nowledge endpoint existence checks such as `MATCH (m:Memory {id: $memory_id}), (s:Source {id: $source_id}) RETURN count(m)` and `MATCH (source:Entity {id: $source_entity_id}) MATCH (target:Entity {id: $target_entity_id}) RETURN source.id, target.id`, count-only one-hop `OPTIONAL MATCH` forms used by thread cleanup such as `MATCH (t:Thread {id: $thread_uuid}) OPTIONAL MATCH (t)-[:CONTAINS]->(m:Message) RETURN COUNT(m)` and legacy extracted-reference counts, the Nowledge graph-analysis degree shape `MATCH (e:Entity) OPTIONAL MATCH (e)-[r]-() WITH e, COUNT(r) as degree RETURN e.id, e.name, degree ORDER BY degree DESC LIMIT 10`, Nowledge entity lifecycle impact, detail, relation preview, label preview, and community preview reads, Nowledge graph orphan one-hop relationship-existence predicates shaped as `NOT (e)<-[:MENTIONS]-(:Memory)` and `NOT (e)-[:RELATES_TO]-()`, Nowledge schema-migration verification `WHERE NOT EXISTS { MATCH (c)-[:SYNTHESIZED_FROM]->(s) }` over already-bound `CRYSTALLIZED_FROM` endpoints and bounded `WITH DISTINCT c.id AS a, s.id AS b RETURN count(*)` pair counting, Nowledge label usage count reads shaped as `MATCH (l:Label) OPTIONAL MATCH (l)<-[:HAS_LABEL]-(n) WITH l, COUNT(n) as usage_count RETURN ...` and direct optional projection-count reads shaped as `MATCH (l:Label) OPTIONAL MATCH (m:Memory)-[:HAS_LABEL]->(l) RETURN l.id, l.name, COUNT(m) AS usage_count`, one-hop and finite bounded outgoing relationship expansion including source revision history with unused `MATCH p = (...)` path binding, endpoint-id bounded `ALL SHORTEST` graph path reads returning `properties(nodes(p), ...)` lists and `length(p)`, Nowledge-used unlabeled node scans such as `MATCH (n) WHERE n.id IN $ids`, Nowledge-used repeated-label any-of node patterns such as `(neighbor:Entity:Memory)`, Nowledge-used whole-record projections such as `RETURN m` and `RETURN r`, Nowledge-used anonymous relationship endpoints for count reads, Nowledge-used one-hop undirected relationship expansion including node-detail neighbor and edge counts shaped as `MATCH (n)-[r]-(neighbor) WHERE n.id = $node_id RETURN COUNT(DISTINCT neighbor), COUNT(r)`, Nowledge-used source and one-hop target node property patterns, one-hop relationship property pattern filters, one-hop relationship variable property reads in `RETURN`, `WHERE`, `ORDER BY`, `COUNT(r)`, `COUNT(r.property)`, Nowledge-used `MIN(variable.property)`, Nowledge-used `MAX(variable.property)` for scheduler fingerprints, Nowledge-used `AVG(variable.property)` for health aggregates, and read-side `id(variable)` projection/filtering/ordering, equality/inequality/range/null/list/`CONTAINS`/`STARTS WITH`/`ENDS WITH` predicates, Nowledge-used `timestamp($cutoff)` and `CAST($cutoff AS TIMESTAMP)` values for freshness and cleanup predicates encoded as epoch-nanos integers, Nowledge-used `list_contains(e.aliases, $name)` property-list membership, Nowledge thread bulk-move normalized-space predicates shaped as `CASE WHEN t.space_id IS NULL OR t.space_id = '' THEN 'default' ELSE t.space_id END =/<> $space_id`, Nowledge community summary presence-ranking order expressions shaped as `CASE WHEN c.ai_summary IS NOT NULL AND c.ai_summary <> '' THEN 0 ELSE 1 END`, Nowledge thread distillation optional source filters shaped as `($source IS NULL OR t.source = $source)`, parameters inside literal lists for `IN`, parenthesized `AND`/`OR` plus unary `NOT` boolean predicates, escaped string literals, parameterized graph algorithm procedure options, global and grouped `COUNT`, Nowledge-used `COUNT(DISTINCT variable)` and `COUNT(DISTINCT variable.property)`, Nowledge bridge grouped reads with `COUNT(DISTINCT e2.community_id)`, `COUNT(*)`, aggregate alias filtering, bounded post-aggregate Community lookup by grouped column, and row-preserving optional post-aggregate Community lookup, Nowledge feed synthesized-source id reads shaped as `WITH c, COLLECT(DISTINCT s.id) AS source_ids RETURN c.id, source_ids`, Nowledge synthesized-source coverage lookups shaped as `WITH c.id AS cid, count(DISTINCT s.id) AS covered WHERE covered = $n RETURN cid LIMIT 1`, `RETURN DISTINCT`, `ORDER BY`, `SKIP`/`OFFSET`, `LIMIT`, and typed parameters for query/explain/transaction APIs; general OPTIONAL MATCH row preservation, general CASE expressions, general `EXISTS` subqueries, returned path values, relationship variables, relationship property patterns, target node property patterns, and general unbounded shortest-path searches are rejected until path/list relationship semantics exist |
| DDL/schema | Node/rel labels, table descriptors, indexes, constraints, and migrations | Partial: explicit node-label token DDL, relationship-type token DDL, node/relationship table descriptor DDL, table/property descriptor state transitions, property schema DDL, equality, composite equality, range, and full-text property index DDL, node/relationship property uniqueness constraint DDL, node/relationship property existence constraint DDL, `Database::run_schema_maintenance` for BACKFILL/VALIDATING advancement plus descriptor GC with WAL/checkpoint persistence and pre-WAL validation, `Database::plan_schema_maintenance` for read-only pending-work estimates, descriptor-bounded `Database::run_bounded_schema_maintenance` batches, bounded background wrappers that bind QoS admission to descriptor-batch execution, and planned background schema maintenance wrappers that charge dry-run estimates to the `Mutation` QoS lane while preserving the direct caller path; richer online index/content backfill orchestration remains |
| Node mutations | `CREATE`, `MERGE`, `SET`, `DELETE` | Partial: `CREATE` node including Nowledge's variable-bearing `CREATE (j:AugmentationJob {...})` form, single-node exact-property `MERGE`, Nowledge schema-migration `MERGE (m:SchemaMigrationLog {id: $id}) ON CREATE SET m.applied_at = CURRENT_TIMESTAMP()` with match-key-only lookup and create-only value assignments, Nowledge GraphMeta stamp `MERGE (m:GraphMeta {meta_id: 'main'}) SET ...` with greenfield create folding and match-side updates, Nowledge label upsert `MERGE (l:Label {id: $label_id}) ON CREATE SET ... ON MATCH SET l.updated_at = $now, l.canonical_name = COALESCE(l.canonical_name, $canonical)` with match-only updates and pending-transaction create folding, single-node `MATCH ... SET`, Nowledge thread bulk-move `MATCH ... SET ... RETURN t.thread_id` update-return writes over normalized-space predicates, Nowledge AugmentationJob lifecycle status/progress/result/error writes, Nowledge undo-community writes for `MATCH (c:Community) DETACH DELETE c`, `MATCH (n) WHERE n.community_id IS NOT NULL SET n.community_id = NULL`, and GraphMeta community reset, Nowledge-used `CURRENT_TIMESTAMP()` values encoded as epoch-nanos integers, Nowledge-used integer self-increment `SET s.memory_count = s.memory_count + 1` for source provenance counters, Nowledge-used `COALESCE(..., 0) + 1` plus multi-assignment node `SET` for memory access tracking, typed endpoint-known knowledge entity creation wrappers plus ordered batch create wrappers over the WAL-backed `CREATE` path for Memory/Source/Entity/Label/Thread/Skill lifecycle ingestion, with id-consistency validation, existing-identity no-write reporting, and grouped WAL batch commits for eligible rows, typed endpoint-known knowledge entity upsert wrappers plus ordered batch upsert wrappers for lifecycle `MERGE`-style create-or-update paths, with separate create/update property maps, id immutability, projected-idless non-writable reporting, duplicate pending identity no-write reporting, and one grouped WAL batch for eligible create/update rows, typed exact-identity knowledge property update wrappers plus ordered batch property update wrappers over the same WAL-backed `SET` path for lightweight metadata/review-status/access-field writes, with per-input missing/filter/idless reporting and grouped WAL batch commits for eligible rows, typed graph-analysis community assignment cleanup through `Database::clear_knowledge_community_assignments` for label-scoped or all-node `community_id` resets, with pre-WAL label validation, overlapping-label deduplication, non-null filtering, and one grouped WAL batch for eligible clears, typed exact-identity knowledge entity detach-delete wrappers plus same-label ordered batch detach-delete wrappers over the same WAL-backed `DETACH DELETE` path for endpoint-known Memory/Source/Entity/Label/Thread/Skill lifecycle cleanup, including `id IN` batches with per-input missing/filter/idless reporting and deduplicated writes, single-node unlabeled `MATCH (n) ... SET` for Nowledge graph-analysis annotations, single-node `MATCH ... DELETE`/`DETACH DELETE`, Nowledge entity lifecycle `MATCH (e:Entity {id: $id}) DETACH DELETE e`, Nowledge thread cleanup target-node `DETACH DELETE` through `MATCH (t:Thread {id: $thread_uuid})-[:CONTAINS]->(m:Message) DETACH DELETE m`, and `id()` filters for node `SET`/`DELETE` |
| Relationship mutations | relationship tables/groups | Partial: `CREATE (:Label {...})-[:TYPE {...}]->(:Label {...})`, exact-pattern relationship `MERGE`, typed exact-identity relationship creation wrappers and ordered batch creation wrappers for Nowledge endpoint-known writes such as `MENTIONS`, `SOURCED_FROM`, `HAS_LABEL`, `EVOLVES`, and `COMPACTS_TO`, with endpoint metadata filters, identifier validation, parameter-bound relationship properties, per-input missing/filter/idless reporting, and the same WAL-backed `MATCH ... CREATE` path; eligible batch rows are committed through one transaction-level grouped WAL batch, typed endpoint-known relationship upsert wrappers plus ordered batch upsert wrappers for Nowledge endpoint+type `MERGE` writes such as `HAS_LABEL` and `SYNTHESIZED_FROM`, with create-only properties, existing-edge no-write reporting, projected-idless non-writable reporting, duplicate pending edge no-write reporting, and one grouped WAL batch for eligible creates, typed exact-identity relationship property update wrappers and ordered batch update wrappers for endpoint-known weight, provenance, review-field, and lightweight edge metadata writes, with optional relationship-property equality filters and the same WAL-backed `MATCH ... SET r.property` path; eligible batch update rows are committed through one transaction-level grouped WAL batch, typed exact-identity relationship delete wrappers and ordered batch delete wrappers for endpoint-known cleanup such as label/source relation removal with optional relationship-property equality filters and the same WAL-backed `MATCH ... DELETE r` path; eligible batch cleanup rows are committed through one transaction-level grouped WAL batch, Nowledge source-provenance `MATCH (m:Memory {id: $memory_id}), (s:Source {id: $source_id}) CREATE (m)-[:SOURCED_FROM {...}]->(s)` over already-matched endpoint nodes with one grouped WAL append, Nowledge EVOLVES-style `MATCH (a:Memory), (b:Memory) WHERE a.id = $older_id AND b.id = $newer_id CREATE (a)-[:EVOLVES {...}]->(b)` endpoint equality writes over already-matched node sets with one grouped WAL append, Nowledge label assignment `MATCH (m:Memory {id: $memory_id}), (l:Label {id: $label_id}) MERGE (m)-[r:HAS_LABEL]->(l) ON CREATE SET ...` with relationship match-key and create-only properties kept separate, Nowledge label-merge transfer `MATCH (n:Memory)-[:HAS_LABEL]->(src:Label {id: $src}) MATCH (tgt:Label {id: $tgt}) MERGE (n)-[r:HAS_LABEL]->(tgt) ON CREATE SET ...` with old-edge source selection and idempotent target-edge creation, Nowledge schema migration `MATCH (c:Memory)-[r:CRYSTALLIZED_FROM]->(s:Memory) MERGE (c)-[n:SYNTHESIZED_FROM]->(s) ON CREATE SET n.weight = r.contribution_weight, ...` with matched-relationship property copy and idempotent rerun behavior, Nowledge label removal `MATCH (m:Memory {id: $memory_id})-[r:HAS_LABEL]->(l:Label {id: $label_id}) DELETE r` with target-node property filtering, one-hop relationship variable property `SET` through `MATCH (a:Label)-[r:TYPE {key: value}]->(b:Label) SET r.property = value`, Nowledge-used target-filtered and multi-property relationship `SET` on one relationship variable with one WAL batch, one-hop relationship variable `DELETE` through `MATCH (a:Label)-[r:TYPE {key: value}]->(b:Label) DELETE r`, source-node plus relationship-variable `WHERE` filters, and `id()` filters for one-hop relationship `SET`/`DELETE`; mixed node/relationship `OR` predicates are rejected for mutation filtering until row-binding mutation semantics exist |
| Transactions | explicit begin/commit/rollback | Partial: `DatabaseTransaction` provides transaction-private COW Cypher reads and writes with read-your-own-writes semantics, mixed PostgreSQL SQL and Cypher publication in one grouped WAL batch and commit epoch, explicit rollback, and immutable `DatabaseReadTransaction` snapshots; `ConcurrentDatabase` adds in-process optimistic first-committer-wins validation plus pessimistic shared/exclusive point and range locks for primary-key reads, primary/unique-key insert points, and foreign-key reference points, conservative database-target fallback for access sets that cannot be proven, disjoint insert rebasing against current constraint state, bounded waits, and multi-owner wait-for graph deadlock victim selection while retaining one durable WAL publication order; fine-grained graph predicate locks, per-key version validation, and multi-process writers remain out of scope |
| WAL recovery | crash-loop recovery and torn tail handling | Partial: WAL replay with checksum/torn-tail stop, strict recovery mode, configurable top-level replay entry cap, atomic batch replay, and recovery-time relationship endpoint validation before accepting graph state |
| Checkpoint | explicit checkpoint and log pruning | Partial: full snapshot checkpoint, WAL truncate, checksummed manifest publication with parent-directory sync after atomic rename, active reader epoch pins, safe reclamation commit epoch publication, required V1 zstd checkpoint and projected graph artifact payload envelopes, relationship endpoint validation after checkpoint load and WAL replay, projected graph artifact publication with the same rename durability boundary, and structured storage reclamation watermark reporting |
| Adaptive adjacency | sparse local neighborhoods and dense hub handling | Partial: current incoming/outgoing adjacency indexes expose stable ordered adjacency entries sorted by `(neighbor_id, relationship_id)` and sparse/dense group classification at the store API boundary; executor one-hop and bounded outgoing expansion plus knowledge retrieval graph-context consume the ordered view while surfacing dense relationship-type adjacency groups in traversal diagnostics; physical sparse blocks, copy-on-write dense segments, and page-level hub isolation remain |
| Property indexes | selective equality, range, and text lookups with index-backed plans | Partial: persistent equality, composite equality, range, and full-text index descriptors, rebuildable in-memory ordered property indexes, ngram-backed text candidate indexes, bounded descriptor-level rebuild reports for composite and full-text execution projections, rankable `Projection` work plans plus bounded background/scheduled wrappers for internal projection rebuild loops, `IndexNodeSeek`, `IndexNodeCompositeSeek`, `IndexNodeTextSeek`, bounded `IndexNodeRangeSeek` for conjunctive range predicates, checkpointed graph statistics with commit-epoch freshness and histogram sampling metadata, eligible-scalar node-property and relationship-property distinct counts including `VARCHAR`, deterministic adaptive sorted value histograms with exact-versus-sampled markers, conservative unknown-statistics fallback for declared `TEXT` and container values, histogram-backed range selectivity, one-hop and bounded multi-hop path-cardinality summaries, one-hop and bounded multi-hop path source/target coverage distinct counts for aggregate costing, plus relationship-property filter selectivity for pattern filters, pushed-down one-hop relationship-variable equality predicates, and relationship-variable range filters; richer text analyzer parity remains |
| Optimizer diagnostics | explain traces and deterministic plan identity | Partial: explain output includes selected plans, scan/seek costing decisions, expand cardinality estimates with per-hop exact/fallback rows, optional degree and relationship count-sum seed/fanout costing from label/property plus relationship type/source statistics for outgoing legs and relationship type/target statistics for incoming legs, endpoint cartesian product input/output row and cost estimates plus stable cost/fingerprint input ordering for flattened all-single-row endpoint products, residual node-property equality/inequality/`IN`/range/string/null filter selectivity, residual relationship-property equality/inequality/`IN`/range/string/null filter selectivity, residual read-side node/relationship `id(variable)` filter selectivity, conservative constant/`OR` predicate selectivity for optional parameter filters, full-text candidate costing without residual double-counting, grouped node/relationship-property aggregate cardinality from selected-plan variable labels/types and property statistics, bounded work costing for distinct variable/property aggregate targets, one-hop and bounded multi-hop path source/target coverage statistics for distinct variable aggregate targets, recursive selected-plan row/cost summaries, deterministic physical plan fingerprints, bounded memo allocation with informational direct-child-resolution decisions and shared lowering, and an optimizer-only smoke benchmark for range seek, bounded expand stats, composite seek, text seek, residual string filters, optional-source `OR` filters, summary-presence null filters, normalized-space exclusion filters, thread candidate normalized-space multi-seek reads, low-selectivity scan fallback, production-shaped selective seed + bounded expand + aggregate + sort/limit, one-hop relationship-property expand reads, pushed-down relationship equality plus relationship range-filter workload reads, source-memory-label and source-memory-entity-label cross-pattern aggregate reads, source-attributed entity community export aggregate reads, source coverage aggregate-alias filter reads, feed synthesized-source collection reads, Skill-to-synthesized-Memory-to-compacting-Thread provenance reads, Community-to-Entity membership-to-Memory evidence aggregate reads, entity bridge-span distinct-property aggregate reads, bounded multi-hop distinct-target aggregate reads, incoming optional relationship count-sum and optional degree target-stat reads, endpoint-existence and nested endpoint-existence cartesian product reads, residual node-property, relationship-property, and relationship-id filter reads, selected-plan cost, selected physical-plan operator and class count histograms, read-only parameterized `explain-json` CLI output with optimizer search mode and plan-cache stats including disabled-miss and bypass counters for CI artifacts, and budget fallback; broader cross-pattern workload-shaped benchmark suites remain |
| Compatibility front door | parameterized Cypher query-runtime shape for Nowledge calls | Implemented: `NowledgeGraphAdapter` exposes parameterized query, explain, work-request inspection, grouped mutation transaction execution over `NowledgeGraphStatement`, and Knowledge Retrieval over a caller-owned `SearchIndex`. `Database` and `DatabaseReadTransaction` no longer expose single-query knowledge read facades; compatibility response-shape tests use test-only parameterized Cypher support. Public fixture and inventory constructors define the production call-site contract, while coverage, inventory gate, shadow cutover, and migration gate reports remain the CI-facing replacement evidence boundary. |
| Compatibility shadowing | compare HawDB against the previous local graph wrapper or another oracle when needed | Partial: reusable shadow-engine fixture runner compares Cypher rows with fixture-declared floating-point tolerances, declared error classes, mutation effects, and projected graph outputs against a second engine; production-shaped `project_graph`/`page_rank`/hierarchical `louvain` fixtures are covered; cutover assessment reports `Ready` or `Blocked` with explicit primary-only coverage blockers; an external JSON-lines process adapter can connect an optional wrapper without linking another graph engine into HawDB |
| Read concurrency | shared readers, exclusive writes/control | Partial: snapshot readers do not observe later commits, survive checkpoints, publish oldest active reader plus safe reclamation epochs to the manifest, expose the same boundary through `Database::storage_reclamation_watermark`, and provide `export_canonical_graph_snapshot` on both live databases and pinned read transactions with canonical node/relationship records, stable-identity audit, deterministic logical checksum, structured snapshot self-validation, stable-ID import-readiness reporting, caller-persisted `CanonicalStableIdMapping` overlays for records without `id` properties, pinned read-transaction search-projection rebuilds and metadata repair over the snapshot epoch, `stable_ids.hawdb` physical-export mapping persistence without WAL write amplification, `prepare_hawdb_lightning_bootstrap_export`, `hawdb-lightning-bootstrap-manifest`, deterministic `hawdb-lightning-graph-stream` and binary `hawdb-lightning-relational-stream` output, `hawdb-lightning-verify-export` graph checksum/count/endpoint plus relational checksum/epoch/count validation, `hawdb-lightning-bootstrap-bundle` ready/blocked evidence packaging, `hawdb-lightning-stage-bootstrap` local staging catalog publication, `hawdb-lightning-verify-staging` source-independent staging artifact validation, `hawdb-lightning-publish-staging` idempotent local published-manifest pointer publication with optional state-marker/fencing/expected-epoch preflight, `hawdb-lightning-verify-published` published-pointer-to-staging validation, `hawdb-lightning-gc-staging-report` published/pinned artifact protection, and `hawdb-lightning-import-status` CREATED/EXPORTING/UPLOADING/MERGING/VALIDATING/READY/PUBLISHED/FAILED/CANCELED/QUARANTINED state aggregation for HawDB Lightning v1 manifest/dual-stream gating, live/WAL/checkpoint-recovered export equivalence regression coverage, and the `validate-canonical-snapshot` read-only CLI gate for future GraphStream encoding and storage-equivalence oracles; page-level MVCC and physical reclamation remain |
| Projected graph | `PROJECT_GRAPH`, page_rank, louvain | Partial: immutable in-memory CSR/CSC projection over store snapshots with node-label and relationship-type filtering, WAL/checkpoint-persisted projection definitions, checkpoint-generated CSR/CSC projection artifacts with format version, projection epoch, public reusable/stale status, recovery-time filtering of artifacts whose commit epoch or definition no longer matches the replayed graph state, epoch/definition-checked execution reuse, checkpoint-independent background artifact rebuild, report-oriented `Database::rebuild_derived_artifacts` for projected graph artifacts, embedded derived-artifact job queue with pending/running/succeeded/failed status, Kuzu-style `CALL project_graph`, `CALL page_rank`, and `CALL louvain` Cypher procedure entry points, reverse traversal, PageRank scoring with parity tolerances, deterministic Louvain-compatible community assignment, hierarchical Louvain levels via `maxLevels`, production-shaped compatibility fixtures, and cutover gating |

Catalog, runtime, statistics, and projection introspection use the same bounded
SQL surface on live databases and pinned read transactions: `system.tables`,
`system.properties`, `system.indexes`, `system.constraints`,
`system.runtime_status`, `system.runtime_capabilities`,
`system.graph_statistics`, `system.projected_graphs`, and
`system.search_projection_changefeed`. PostgreSQL-compatible relational schema
introspection is available through the read-only `information_schema.tables`,
`information_schema.columns`, `pg_catalog.pg_tables`, and
`pg_catalog.pg_indexes` views. These views report transaction-private DDL from
the same relational snapshot and use the same row and payload admission as
`system.*`; they do not emulate PostgreSQL OIDs or server-internal MVCC
catalogs. The duplicate typed introspection getters are no longer public
integration surfaces. Recovery, admission, generation publication, and grouped
mutation contracts remain typed.

Note: the current compatibility fixture also covers Nowledge-used entity reuse
exact, case-insensitive, alias-containment, same-type bounded scan reads, and
entity temporal metadata create/update writes, entity total count reads, and
entity `MENTIONS`/`RELATES_TO` creation writes. It also covers Nowledge-used
label resolver null-canonical scans/backfills, rename collision guards,
existence reads, and remove-all count/delete writes. Source provenance coverage
includes Source endpoint checks, full `SOURCED_FROM` creation writes,
edge-existence/global counts, exact repair candidate scans, and source
memory-count reads. PageRank coverage includes membership/visibility reads,
score persist/clear writes, central-entity lookup, GraphMeta clear stamps, and
planner node/relationship totals and changed count reads. Community detection
scheduler coverage includes GraphMeta state reads, candidate scans, member
entity lookups, and summary writes. Cleanup scheduler coverage includes bounded
seed scans, EVOLVES pair reads, cleanup fingerprint row fetches, and floor-zero
engagement `CASE` ordering.
External content artifact orchestration also exposes bounded pending/failed
polling, specific-job execution, and explicit failed-job retry for caller-owned
parser runtimes, while keeping database-owned projected graph artifact rebuilds
on the graph-kernel runner.
Migration gate JSON keeps human-readable blockers and adds machine-readable
fixture-mismatch, inventory, and shadow blocker counts, shadow evidence counts,
caller-owned rollback evidence fields, `shadow_run.evidence_kind`, plus grouped
blocker messages so cutover automation can separate scanner coverage gaps,
previous-wrapper parity failures, rollback readiness gaps, and self-shadow
protocol smoke without making HawDB open the previous graph database.
The Nowledge migration-gate library options and CLI can require caller-owned
rollback evidence and carry the supplied previous-database reopen proof into the
gate decision.
HawDB Lightning bootstrap bundle export gates likewise split manifest,
GraphStream, and RelationalStream blockers into counts and grouped messages for
import preflight automation. The two streams share one database commit epoch;
empty-target import publishes graph rows and the relational schema/rows/overflow
checkpoint in one WAL batch.
Staging verification gates also split artifact, manifest, GraphStream,
RelationalStream, bundle, and catalog errors into grouped arrays for offline upload/resume automation,
with staged artifact count/byte summaries for upload observability and
fail-closed catalog/manifest protocol-version checks.
Published-pointer verification gates split pointer, catalog, and staging errors
for HawDB Lightning resume checks.
Staging GC gates also group published-pointer verification errors before
declaring artifacts deletable and report total/pinned/deletable staging bytes.
Import-status reports add a machine-readable `resume_action`, optional
caller-owned state-marker aggregation with active-state idempotency-key
validation, optional caller-owned checkpoint-log aggregation with failure
coordinates, stage/status/failure-count summaries, and idempotency-coordinate
conflict detection, and active-import `resource_retention` policy so automation can
choose staging, publishing, active resume, completion, failure/cancel handling,
quarantine/manual-repair, or artifact-retention handling without parsing
human-readable error strings or deleting READY artifacts before publish.

## Current Compatibility Evidence

The current live scanner coverage gate over the local Nowledge graph-source tree
is complete for the scanned Cypher surface: `nowledge-scanned-inventory`
requires 698 checks, `nowledge-memory-core` covers all 698, and
`missing_items` is empty. The scanner excludes vendored `upstream_forks`
examples from this production-source gate.

This does not by itself complete migration cutover. The remaining evidence gap
is external shadow comparison against the previous local graph wrapper when a
specific migration gate needs oracle-backed parity evidence. Required cutover
evidence now rejects self-shadow protocol smoke runs and requires the shadow
ready preflight to declare `engine_kind: "previous_wrapper"`. Production
cutover runs that require storage or resource evidence must also attach
`hawdb-storage-recovery-report` and `hawdb-background-maintenance-report`
artifacts from the real database path, with matching protocols and full
readiness under the fail-closed cutover evidence rules.

Typed Memory latest updates are exposed through
`Database::update_knowledge_memory_latest_batch` for Nowledge EVOLVES
promotion/demotion writes. The wrapper updates only `is_latest`, supports the
exact `space_id` filter used by in-space demotion, reports missing, filtered,
duplicate, and non-writable rows before writing, and commits eligible updates
through one grouped WAL batch.

The stable way to report "how much of Nowledge can be replaced" is to run
`hawdb nowledge-replacement-summary <migration-gate-json>` over a generated
migration-gate bundle, or add `--compact`/`--max-family-items <n>` when the
bundle contains large per-family diagnostic arrays. Add `--max-blockers <n>`
when the release artifact needs a bounded blocker sample instead of full blocker
strings. The summary intentionally separates three numbers:
`business_surface.covered_per_million` for scanned Cypher coverage,
`shadow_parity.matched_per_million` for previous-wrapper comparison, and
`production_replacement_per_million` for conservative production replacement
readiness. Production replacement stays `0` unless the migration gate is ready,
shadow parity is complete, `cutover_evidence.eligible` is true, and the bundle
shows full per-query-family replacement readiness. Migration bundles also expose
`dual_engine_evidence` at the top level and under `cutover`, with primary/shadow
check counts and primary-only counts for release automation; replacement summary
copies that evidence and fails production readiness when present but not ready.
Adapter bring-up should use
`HAWDB_ENABLE_COMPATIBILITY_TOOLS=1 hawdb external-shadow-adapter-smoke
--require-previous-wrapper ...` first to validate `ready`, `execute_session`,
and `project_graph` wiring, but smoke output does not count as production
cutover evidence.
Production replacement is a side-by-side cutover signal, not an old-store
deletion signal. Nowledge Mem must keep the existing Kuzu/Ladybug database
available while HawDB is introduced as a sibling graph store through explicit
adapter flags, shadow comparison, and rollback evidence. Removing the old store
belongs to a later cleanup phase with its own approval and evidence.

## LanceDB Replacement Surface

| Capability | Nowledge need | HawDB status |
|---|---|---|
| Rebuildable projection | Search is derived, not source of truth | Partial: `SearchIndex` is separate from graph store, exposes report-oriented derived artifact rebuild, supports bounded incremental projection deltas for FTS/BM25 row upsert/delete without forcing full rebuilds, and publishes persistent projection snapshots through synced temp-file rename plus parent-directory sync while staying outside graph WAL |
| Vector search | semantic memory/entity/source search | Partial: exact cosine search, with child retriever input candidate-set reports proving metadata filters are applied before vector scoring and rank-window trimming |
| Metadata filters | scoped retrieval by projection metadata | Partial: exact-match metadata filters on graph-derived document metadata, with `kind` accepting canonical node labels or lowercase projection names, `external_id` using the projected node identity (non-empty `id` when present, otherwise canonical node id string) consistently for search hits, graph-native seeds, and graph context path endpoints, `source_id` using the same non-empty `source_id`/`thread_id`/`source` projection fallback for search hits and graph-native seeds, plus Nowledge normalized-space semantics for `space_id` missing/`NULL`/empty-string values as `default`, applied before vector scoring, reference BM25 term statistics and candidate scoring, retriever candidate counts, rank-window trimming, and final truncation; segmented BM25 intentionally uses projection-wide document frequencies and normalization statistics from its manifest plus bounded mini-delta corrections while filters scope scored candidates, keeping filtered query I/O independent of corpus size and requiring one posting decode pass; an exact projection-local candidate-set report carries id space, representation, cardinality, filtered-out count, exactness, filter metadata, source graph snapshot epoch, and optional caller-supplied policy epoch metadata; the same request filters also scope graph-native seed candidates by label, external ID, and same-name scalar node properties, while direct graph reads express metadata predicates in bounded parameterized Cypher |
| FTS/BM25 | text search and non-vector fallback | Partial: BM25-style term-frequency, inverse-document-frequency, and length-normalized text scoring with case-insensitive Nowledge identifier tokenizer covering camelCase, acronym-to-titlecase technical identifiers, snake_case, kebab/path separators, numeric suffixes, adjacent chunk bigrams, conservative CJK bigrams/trigrams for Chinese/Japanese/Korean knowledge notes, conservative English suffix normalization, conservative English stopword filtering, selected graph-derived projection metadata identifiers (`kind`, `external_id`, `source_id`, `space_id`), configurable application-supplied analyzer lexicons with normalized phrase/identifier alias rules and application-owned stopword rules for Nowledge memory lifecycle/schema aliases such as `crystal`/`crystallization`, `episodic_provenance`/`raw_evidence`, `SOURCED_FROM`/`source_provenance`, `MENTIONS`/`entity_mention`, `EVOLVES`/`memory_evolution`, and `ai_summary`/`community_summary`, plus a conservative default technical alias set for knowledge-retrieval aliases (`rag`/`graph_rag`/`graph_retrieval`/`kg`), database-system aliases (`wal`, `mvcc`, `lsm`, `csr`/`csc`, `snapshot`/`checkpoint`), database import/stream aliases (`HawDBLightning`/`database_import`, `GraphStream`/`graph_export`, `RelationalStream`/`sql_stream`, `projection_freshness`/`projection_staleness`), migration/projection aliases (`pg`/`postgres`/`postgresql`, `pgvector`/`vector_search`, `fts`/`full_text_search`, `lance`/`lancedb`, `kuzu`/`ladybug`), and retrieval-algorithm aliases (`rrf`/`reciprocal_rank_fusion`, `ann`/`approximate_nearest_neighbor`, `hybrid_retrieve`/`hybrid_retrieval`/`hybrid_search`); larger analyzer parity remains |
| Hybrid fusion | vector + FTS score fusion | Partial: weighted RRF-style vector/text fusion with optional rank-window budget and per-child vector/text ranks, child retriever input candidate-set reports, child RRF components, and child scores exposed on each hit |
| Retrieval explainability | score breakdown, provenance, projection freshness, and truncation reasons | Partial: search hits include fused RRF score, per-child RRF components, vector/text scores, vector/text ranks, fallback reasons, projection kind, external ID, source ID, matched analyzer terms, matched projection-text spans, document count, source graph commit epoch, projection rebuild/repair marker state and marker reasons, and embedding manifest freshness; incremental projection delta reports expose source graph commit epoch before/after plus whether the delta advanced the freshness watermark; `SearchIndex::search_with_report` exposes total and post-filter document counts, exact candidate-set report, child retriever availability, child fallback reasons for empty text queries and missing or incompatible vector legs, candidate counts, child output candidate-set reports, top hit IDs, top candidate ranks/scores, total pre-limit matches, requested limit, rank window, fusion weights, truncation flag, truncation reasons, machine-readable empty reason codes with stable string encodings, search-level fallback reasons with stable fallback reason codes, and stable search truncation reason codes; Knowledge Retrieval diagnostics lift search empty reason codes into retrieval-level empty reason codes, add graph-seed/candidate empty codes with stable string encodings, expose exact graph-seed input and output candidate-set reports with metadata-filter cardinality/filter-out counters, expose exact graph-context input seed-node and output expanded-relationship candidate-set reports, expose stable truncation reason codes for rank-window, search-limit, graph-seed, graph-context, and candidate-budget truncation, expose stable graph-seed/graph-context fallback reason codes for disabled retrieval budgets, and expose stable fan-out reason codes plus structured fan-out details emitted from typed fan-out events for dense adjacency and bounded retrieval/traversal limits so callers do not parse English diagnostics strings; direct graph reads use query-runtime row and payload budgets plus EXPLAIN ANALYZE reports instead of route-specific traversal diagnostics |
| Dimension checks | model/dimension changes require rebuild | Partial: persisted embedding model/version/dimension manifests, row dimension validation, query dimension mismatch degradation, and full-reindex marker on model or dimension changes |
| Markers | `.reindex_needed`, `.projection_metadata_repair_needed` | Partial: in-memory and path-backed marker read/write with reason preservation; path-backed projections persist marker files |
| Fail-soft legs | stale vector or FTS should not drop all results | Partial: vector mismatch degrades to text |
| Metadata repair | bounded metadata-only backfill | Partial: graph-derived metadata repair without rewriting content or embeddings, with rankable `Projection` work plans plus stateless `LocalQosPolicy` and database-owned `LocalQosScheduler` wrappers for internal background repair, plus `Database::repair_search_projection_metadata`, `Database::repair_background_search_projection_metadata`, and `Database::repair_scheduled_background_search_projection_metadata` facade methods over canonical graph evidence |
| Full rebuild orchestration | bounded replacement from authoritative graph | Partial: bounded graph-to-search rebuild with all-or-nothing in-memory replacement, graph-derived incremental projection deltas from canonical node IDs with explicit complete-through source graph commit epoch freshness stamping and report-level watermark before/after fields, rankable `Projection` work plans, background/scheduled QoS wrappers, and `SearchIndex::rebuild_derived_artifacts` reporting of document counts, scanned nodes, lifecycle-marker state, and lifecycle-marker reasons |
| Resource and QoS budgets | embedded devices should not starve foreground reads | Partial: foreground user requests are not locally gated by background budgets; search projection rebuild and metadata repair accept row budgets, full search rebuilds, incremental projection deltas, graph-derived incremental projection deltas, and metadata repair can expose rankable `Projection` work plans, incremental projection deltas accept operation budgets and fail without partially mutating the index, `Database::search_projection_rebuild_background_work_plan` exposes graph-derived full search rebuild estimates for caller-owned scheduling, `Database::rebuild_background_search_projection` and `Database::rebuild_scheduled_background_search_projection` gate caller-owned full search projection rebuilds through `LocalQosPolicy` and `LocalQosScheduler`, `Database::search_projection_metadata_repair_background_work_plan` exposes graph-derived metadata repair estimates for caller-owned scheduling, `Database::repair_background_search_projection_metadata` and `Database::repair_scheduled_background_search_projection_metadata` gate metadata-only projection repair through the same QoS surfaces, `Database::search_projection_graph_delta_freshness_background_work_plan` derives rankable graph-delta work hints from explicit recent delta operations and source graph commit lag, `Database::search_projection_freshness_lag_background_work_plan` and unified background maintenance candidates can surface stale search projection graph-delta work from commit lag without requiring the caller to prebuild a delta request, `Database::apply_background_search_projection_graph_delta` applies internal graph-derived projection deltas through `LocalQosPolicy`, `Database::apply_scheduled_background_search_projection_graph_delta` tracks in-flight internal graph-derived projection delta work through `LocalQosScheduler`, `SearchIndex::apply_background_projection_delta` and `Database::apply_background_search_projection_delta` apply internal projection deltas through `LocalQosPolicy`, `SearchIndex::apply_scheduled_background_projection_delta` and `Database::apply_scheduled_background_search_projection_delta` track in-flight internal background projection delta work through `LocalQosScheduler`, graph-derived metadata repair exposes the same background admission and scheduler wrappers, composite/full-text property-index projection rebuilds can expose a rankable `Projection` work plan and use bounded background/scheduled wrappers, HawDB Lightning bootstrap export exposes an `Import` work plan plus background/scheduled wrappers while direct caller exports remain ungated and unified background maintenance ranking can include its pre-export candidate, external content parser/crawler jobs can be charged to the `Import` background lane, database-owned derived artifact rebuild jobs can use `Database::run_next_background_derived_artifact_job` for the same internal background admission while explicit callers keep the direct runner, `LocalQosScheduler` tracks in-flight internal background operation budgets plus optional per-class background budgets for caller-driven projection/import/analytics/shadow lanes without owning worker threads, `BackgroundWorkPlan` and `BackgroundWorkHint` let caller-owned loops rank background work by expected-value signals such as active topic, recent delta size, query probability, source graph commit lag, staleness TTL, freshness SLO, and tenant budget before attempting admission, schema maintenance can expose a rankable `Mutation` work plan from its current dry-run estimate, `Database::background_maintenance_candidates`, `Database::rank_background_maintenance`, and `Database::background_maintenance_summary` gather named schema/property-index/search/HawDB Lightning/external-content candidates with stable parseable typed kinds plus admitted/deferred/rejected operation totals for caller-owned multi-queue loops without starting workers, `LocalQosPolicy::rank_background_work` and `LocalQosScheduler::rank_background_work` provide deterministic admitted-first ordering over caller-owned candidate lists with stable background work reason codes, `WorkClass` and `WorkPriority` expose stable parseable lane and priority string encodings, `QosAdmissionCode` exposes stable parseable string encodings for background defer/reject categories without parsing human-readable reasons, `LocalQosPolicy` admits foreground work while deferring oversized or disabled internal background work, and Knowledge Retrieval exposes search/rank-window/graph-seed/graph-context/candidate budgets, fallback reasons, and truncation reasons; worker ownership remains caller-owned |
| Multi-table projections | memories/messages/entities/sources/chunks/communities | Partial: typed projection rows for memory/message/entity/source/source chunk/community |
| Knowledge Retrieval facade | application-facing retrieval over graph-derived projections | Partial: `Database::rebuild_search_projection` derives a caller-owned search projection from canonical graph evidence, `Database::rebuild_background_search_projection` and `Database::rebuild_scheduled_background_search_projection` expose the same full rebuild behind caller-owned background QoS admission, `DatabaseReadTransaction::rebuild_search_projection` and `DatabaseReadTransaction::repair_search_projection_metadata` derive the same projection maintenance inputs from a pinned graph snapshot, `Database::build_search_projection_graph_delta` and graph-delta apply wrappers derive bounded caller-owned incremental search projection updates from canonical node IDs while preserving explicit complete-through source graph commit epoch freshness, `Database::retrieve_knowledge` returns graph commit epoch, projection freshness, metadata-filtered search hits and graph seeds, unified vector/text/graph-seed retriever reports with child-level limits, rank windows, fusion weights, child output candidate-set reports, fallback reasons for unavailable search legs and disabled graph-seed legs, truncation reasons, top-candidate ranks, scores, provenance metadata, canonical node IDs, matched spans, graph context path counts, and search-child projection freshness, compact retrieval diagnostics with search scope counts, exact search candidate-set reports, search candidate filter-out counts, search/rank-window/fusion-weight/graph-seed/graph-context/candidate budgets, search truncation flag/reasons, search fallback reasons, graph seed counts, graph-seed truncation reasons, graph context path/node/relationship counts, graph-context fallback reasons for disabled budgets, fan-out reason count and messages, returned and pre-limit merged candidate counts, response-level candidate truncation flag/reasons, graph-context truncation flag/reasons, projection source graph commit epoch, projection commit lag, stale projection warnings, projection marker warnings, structured projection stale/full-reindex/metadata-repair flags and marker reasons, and empty-result reasons that distinguish metadata misses, search fallback causes, disabled retriever budgets, and search or response-candidate budget exhaustion, a typed `KnowledgeCandidate` surface that exposes canonical node IDs and merges search-hit and graph-seed candidates by canonical graph identity with response-level candidate budgeting, candidate score breakdown, max and weighted-sum candidate scoring policies, per-hit evidence summaries with source IDs, canonical node IDs, score components, child RRF components, matched terms, matched projection-text spans, and graph context path counts, bounded graph-native seed results over canonical nodes, bounded multi-hop graph context paths with relationship properties for both search hits and graph-native seeds with per-seed relationship de-duplication, search truncation diagnostics plus search projection empty-result reasons for empty projections, metadata-filter misses, fallback causes, no matching rows, and limit-zero empty returns, optional hybrid rank-window, fusion-weight, and fallback diagnostics, and graph fan-out reasons without storing search state in the graph WAL, direct canonical entity lookup and property projection use bounded parameterized Cypher on pinned snapshots, relationship, neighborhood, path, and bounded-subgraph reads use small bounded parameterized Cypher statements over pinned snapshots; route-specific navigation DTOs and typed read facades are not production extension points |

Typed mutation coverage now also includes normalized-space batch moves for
Nowledge Memory, Source, Thread, and ThreadIdentity-style `id` or `thread_id`
lists. The wrapper validates label and identity-property identifiers, applies
the Nowledge rule that missing, `NULL`, and empty `space_id` map to `default`,
supports optional source-space filtering and target-space no-op reporting,
deduplicates pending node writes, returns moved external IDs in caller order,
can stamp `updated_at`, and commits eligible rows through one grouped WAL batch.
It also includes a typed Memory access touch batch for Nowledge
`mark_memories_accessed` and click-dwell writes. The wrapper increments
`access_count`, `clicks`, and `total_dwell_time_ms` through Cypher
`COALESCE(..., 0) + ...` assignments inside one transaction, updates
`last_accessed_at` and `last_clicked_at`, reports missing/idless rows without
writing, and preserves duplicate same-memory touches as separate increments in
the same grouped WAL batch.
Memory content/edit writes are covered by
`Database::update_knowledge_memory_content_batch` for the Nowledge full Memory
update shape. The wrapper updates content, title, semantic field, importance,
confidence, unit type, source, source range, space, `updated_at`,
`reindex_needed`, review status, and extraction method together, validates
non-empty Memory ids plus numeric finite scoring fields before WAL, reports
missing/idless/duplicate rows without writing those rows, and commits eligible
Memory updates through one grouped WAL batch.
Scheduler dedup-reviewed writes are covered by
`Database::update_knowledge_memory_dedup_reviewed_batch` for the Nowledge
`MATCH (m:Memory) WHERE m.id IN $ids SET m.dedup_reviewed_at = $reviewed_at`
shape. The wrapper validates non-empty Memory ids before WAL, reports
missing/idless/duplicate rows without writing those rows, and commits eligible
Memory timestamp stamps through one grouped WAL batch.
Source provenance count writes are covered by a typed Source memory-count
adjustment batch for Nowledge `memory_count + 1` and floor-to-zero decrement
paths. The wrapper validates non-empty Source ids and non-zero deltas before
WAL, treats missing or `NULL` `memory_count` as zero, rejects non-integer
current counts without writing that row, preserves duplicate same-source
adjustments in request order, materializes floor-decrement results as `0`, and
commits eligible per-source updates through one grouped WAL batch.
Source lifecycle writes are covered by a typed batch for Nowledge extracted
mark-indexed, indexed `chunk_count`, and direct lifecycle-state updates. The
wrapper validates Source ids, target/current lifecycle states, and non-negative
chunk counts before WAL, applies optional current-state filtering, reports
missing/idless/duplicate rows without writing, writes `lifecycle_state`,
optional `chunk_count`, and `updated_at`, and commits eligible Source rows
through one grouped WAL batch.
Source metadata-only writes are covered by
`Database::update_knowledge_source_metadata_batch` for the Nowledge auto-OCR
metadata timestamp shape. The wrapper writes only `metadata` and `updated_at`,
validates Source ids before WAL, reports missing/idless/duplicate rows without
writing, and commits eligible Source rows through one grouped WAL batch.
Source parsed metadata writes are covered by
`Database::update_knowledge_source_parsed_metadata_batch` for Nowledge parser
completion updates. The wrapper marks the Source as `parsed`, writes summary,
checksum, size, timestamp, and the optional parsed/file/name/mime/url/metadata
fields used by current parser call sites, validates Source ids, non-empty
checksums, and non-negative sizes before WAL, reports missing/idless/duplicate
rows without writing, and commits eligible Source rows through one grouped WAL
batch.
Source parsed creates are covered by
`Database::create_knowledge_source_parsed_batch` for the Nowledge markdown,
URL, PDF, generic file, and markdown import create shapes. The typed wrapper
accepts only the fields used by current parsed Source creation, fills the
fixed Nowledge defaults for parsed lifecycle, zero chunk/memory counts, and
empty error messages, validates ids, type/name/mime/parsed-path/checksum/space,
non-negative sizes, and positive versions before WAL, reports existing or
duplicate Source ids, and commits eligible Source nodes through one grouped WAL
batch.
Source latest-version lookups use two fixed parameterized Cypher statements:
one for original-name plus space and one for checksum plus space. Both order by
`COALESCE(version, 1) DESC` and use a bounded projection; the host selects the
statement rather than interpolating a predicate. `REVISED_AS` creation remains
typed: the wrapper validates endpoint ids before WAL, resolves exact
Source endpoints, reports missing or idless endpoints without writing, creates
the fixed `REVISED_AS` properties used by Nowledge, and commits eligible
revision edges through one grouped WAL batch.
Source detach deletes are covered by `Database::delete_knowledge_sources` for
the Nowledge Source node cleanup shape. The Source-specific wrapper validates
Source ids before WAL, reuses the existing typed entity `DETACH DELETE` path,
reports missing and idless rows without writing, cascades Source relationships
through storage/WAL, and commits eligible Source node deletes through one
grouped WAL batch.
Source label assignment and cleanup writes are covered by
`Database::assign_knowledge_source_labels_batch` and
`Database::delete_knowledge_source_labels_batch` for the Nowledge
`(:Source)-[:HAS_LABEL]->(:Label)` merge/delete shapes. The typed facades fix
the Source/Label/HAS_LABEL endpoints, validate ids and assignment origins
before WAL, report missing or projected-idless endpoints without writing, keep
existing label edges create-only, preserve endpoint nodes on cleanup, and route
eligible relationship writes through one grouped WAL batch.
Source operational, list, and attribution reads use host-owned fixed
parameterized Cypher. Exact detail, total/count, id page, summary page, and
projected page are separate named statements; hosts select a fixed filter and
ordering variant rather than interpolating identifiers. Source-to-Memory reads
use separate count and bounded page statements over incoming `SOURCED_FROM`,
while bulk Memory/Source attribution binds bounded endpoint id lists. Related
phases execute on one `DatabaseReadTransaction`, project only response fields,
and preserve query errors instead of converting them to empty results.
Memory lifecycle metadata writes are covered by a typed batch for the Nowledge
`metadata`, `is_latest`, `lifecycle_state`, and `updated_at` update shape. The
wrapper validates Memory ids and non-empty lifecycle states before WAL, reports
missing/idless/duplicate rows without writing, and commits eligible Memory rows
through one grouped WAL batch.
Lightweight Memory metadata replacement writes are covered by
`Database::update_knowledge_memory_metadata_batch` for Nowledge `metadata` and
optional `updated_at` update shapes. The wrapper validates Memory ids before
WAL, reports missing, projected-idless, and duplicate rows without writing, and
commits eligible Memory rows through one grouped WAL batch.
Crystal Memory lists, key lookups, community aggregations, and source visibility
reads use separate fixed parameterized Cypher statements. Each statement owns
its `is_crystal` predicate, cursor or community scope, concrete projection,
ordering, `LIMIT`, row and payload budgets, and pinned snapshot. Aggregation and
path-visibility phases remain separate so one query does not accumulate an
unbounded intermediate result. No Crystal route-specific read API or DTO is
exposed, and these reads do not write WAL.
MCP crystal source-link writes are covered by
`Database::merge_knowledge_crystal_source`. The typed write resolves exact
physical `Memory.id` endpoints for the crystal and source Memory nodes, merges
the outgoing `SYNTHESIZED_FROM` edge, creates only the Nowledge-used `weight`,
empty `occasion_key`, and `created_at` relationship properties, reports missing
endpoints and idless endpoints without writing, preserves existing-edge
properties for `MERGE ON CREATE SET` semantics, validates numeric finite
weights before WAL, and uses the WAL-backed relationship write path.
Synthesized-source coverage lookups use a host-owned fixed grouped Cypher
query. It binds the source Memory id set and required distinct coverage count,
scans only `Memory` crystals with outgoing `SYNTHESIZED_FROM` Memory sources,
and projects matching crystal ids/titles plus matched source ids. Bounded and
unbounded host variants are separate statements so query shape remains static.
Callers use a pinned read transaction when coverage and hydration must share a
graph version.
Memory entity mention reads use one fixed parameterized Cypher statement per
Memory with explicit Memory/Entity labels, node and relationship projection,
deterministic ordering, `LIMIT`, a matching row budget, and a shared pinned
snapshot. Grouping, missing-id handling, and distinct-name shaping stay in the
host; no route-specific typed database API is exposed.
Entity mention-count lists use separate fixed parameterized Cypher statements
for the first page and cursor pages. Both scan only named, id-bearing `Entity`
nodes, preserve zero-mention Entities, count incoming `Memory` `MENTIONS`, use
deterministic ordering, apply `LIMIT` with a matching row budget, and run on a
pinned read transaction. No route-specific typed read API is exposed.
REST write Entity delete guards use five small fixed Cypher statements on one
pinned read transaction: Entity resolution, other-Memory mentions, `HAS_LABEL`
relationships, all incident relationships, and incoming relationships. The
host preserves the current incoming-edge double-counting implied by the
production `COUNT(DISTINCT r1) + COUNT(DISTINCT r2)` shape. The actual Entity
delete remains a typed WAL mutation; no route-specific typed guard API is
exposed.
Community Entity visibility and Memory ranking reads use separate fixed
parameterized Cypher statements for optional incoming `Memory` `MENTIONS`,
direct `Memory.community_id`, unit-type filters, and crystal filters. Each phase
has an explicit community scope, deterministic ordering, `LIMIT`, row and
payload budgets, and a pinned snapshot. The host performs only bounded
cross-statement shaping; no Community visibility route API or DTO is exposed.
Related Entity name reads use separate fixed parameterized Cypher statements
for Memory-id batches and Thread-compaction paths. Each has explicit labels,
distinct-name projection, deterministic ordering, `LIMIT`, a matching row
budget, and a pinned snapshot. Identity-specific Thread statements and missing
endpoint shaping stay in the host; no scope-switching typed database API is
exposed.
Context memory previews use two fixed parameterized Cypher statements: one for
title/unit-type rows and one for `HAS_LABEL` expansion. Each statement owns its
latest-state predicate, explicit projection, deterministic ordering, `LIMIT`,
matching row budget, and pinned snapshot; no mode-switching typed database API
is exposed.
Memory bulk detail, filtered list, and feature-specific projections use fixed
parameterized Cypher statements. Each statement owns its exact Memory fields,
space/unit/latest/crystal filters, score or created-at ordering, `LIMIT`, and
row/payload budgets. Host code normalizes spaces and derives missing ids; there
is no wide default row or caller-projection typed API.
Metadata-related Memory detail reads use one fixed parameterized query that
filters normalized space and the supported `source_id`/`source_thread_id`
metadata markers. Each business phase owns its projection, created-at ordering,
`LIMIT`, row budget, and pinned snapshot; no caller-projection typed API is
exposed.
Memory prefix ownership guards use one fixed parameterized `STARTS WITH` query
with an explicit Memory label, projection, stable ordering, `LIMIT`, row budget,
and pinned snapshot. Host code normalizes `space_id`; no route-specific typed
database API is exposed.
Memory title/content id-list reads use one fixed parameterized, id-bounded
Cypher statement with explicit projection, created-at ordering, `LIMIT`, row
budget, and a pinned snapshot. Host code derives missing ids and shapes the
response; no route-specific typed database API is exposed.
Memory EVOLVES latest reads use a fixed parameterized successor Cypher
statement with explicit target projection, deterministic ordering, `LIMIT`, a
matching row budget, and a pinned snapshot. Optional source-existence and
relationship-count phases are separate bounded queries owned by the host; no
REST-specific typed database API is exposed.
Memory EVOLVES relation counts use one fixed parameterized aggregate Cypher
statement with explicit source/target labels, relation filtering, deterministic
ordering, `LIMIT`, a matching row budget, and a pinned snapshot. Missing-id and
total-count response shaping stays in the scheduler; no scheduler-specific
typed database API is exposed.
Memory crystal synthesis counts use one fixed parameterized aggregate Cypher
statement with explicit Memory endpoint labels, `is_crystal = true`,
deterministic ordering, `LIMIT`, a matching row budget, and a pinned snapshot.
Missing-id and total-count shaping stays in the scheduler; no scheduler-specific
typed database API is exposed.
Memory decay detail reads use one fixed parameterized exact-id Cypher statement
with explicit projection, stable ordering, `LIMIT 1`, a one-row budget, and a
pinned snapshot. The scheduler shapes optional and future fields from the query
row; no scheduler-specific typed database API is exposed.
Memory decay refresh writes are covered by
`Database::update_knowledge_memory_decay_refresh_batch`. The typed write covers
the decay scheduler's exact-id score-only and score-plus-confidence update
shapes, validates Memory ids and finite numeric decay/confidence values before
WAL, reports missing, idless, and duplicate rows without writing those rows, and
commits eligible updates through one grouped WAL batch.
Memory cleanup fingerprint reads use one fixed parameterized `m.id IN $ids`
Cypher statement with explicit projection, stable ordering, `LIMIT`, a matching
row budget, and a pinned snapshot. The cleanup scheduler owns request-order
shaping and missing-id calculation; no scheduler-specific typed database API is
exposed.
Memory EVOLVES neighbor reads use separate fixed outgoing and incoming Cypher
statements. Each owns its node and relationship projection, deterministic
ordering, `LIMIT`, matching row budget, and pinned snapshot; no dynamic
direction or caller-projection typed database API is exposed.
Memory EVOLVES projected successor reads use separate fixed per-parent Cypher
statements for stable-id and `updated_at DESC` ordering. Each statement owns its
node and relationship projection, deterministic tie-breakers, `SKIP`, `LIMIT`,
matching row budget, and pinned snapshot. Parent grouping and missing-id
shaping stay in the host; no dynamic projection or pagination DTO is exposed.
Memory EVOLVES edge creation writes are covered by
`Database::create_knowledge_memory_evolves_batch`. The typed write covers
Nowledge `add_evolves_edge` and replacement-relation create shapes, fixes both
endpoints to physical `Memory.id` nodes and the relationship type to
`EVOLVES`, accepts Nowledge-used relation metadata, validates non-empty ids,
non-empty content relations, finite numeric confidence, and non-empty detector
names before WAL, reports missing or idless endpoints without writing, and
commits eligible relationship creates through one grouped WAL batch. Latest
promotion and demotion remains covered by
`Database::update_knowledge_memory_latest_batch`.
Skill usage-stat writes are covered by a typed batch for Nowledge
`use_count`, optional `success_rate`, `last_activity_at`, `updated_at`, and
`metadata` update shapes. The wrapper validates Skill ids, non-negative use
counts, and bounded numeric success rates before WAL, reports missing,
idless, and duplicate rows without writing, and commits eligible Skill rows
through one grouped WAL batch.
Skill metadata replacement writes are covered by
`Database::update_knowledge_skill_metadata_batch` for Nowledge `metadata` and
`updated_at` updates without lifecycle state changes. The wrapper validates
Skill ids before WAL, reports missing, idless, and duplicate rows without
writing, and commits eligible Skill rows through one grouped WAL batch.
Skill lifecycle/write-state updates are covered by a typed batch for Nowledge
stage changes, rejection timestamps, promotion rationale, compiled version
metadata, draft bundle writes, content hashes, bundle paths, triggers, tools,
write origin, and `updated_at` stamping. The wrapper validates Skill ids,
non-empty stages, and non-empty write origins before WAL, reports missing,
idless, and duplicate rows without writing, and commits eligible Skill rows
through one grouped WAL batch.
REST Skills source merges are covered by
`Database::merge_knowledge_skill_source`. The typed write resolves exact
physical `Skill.id` and `Memory.id` endpoints, merges the outgoing
`SYNTHESIZED_FROM` edge, creates only the Nowledge-used `weight`,
`occasion_key`, and `created_at` relationship properties, reports missing
endpoints and idless endpoints without writing, preserves existing-edge
properties for `MERGE ON CREATE SET` semantics, and uses the WAL-backed
relationship write path.
Skill catalog, detail, state, evidence-memory, and thread-source reads use
fixed parameterized Cypher through `Database::query_with_params_bounded` or a
pinned read transaction. Each business phase selects only the fields it needs,
uses an explicit `LIMIT`, and supplies a matching row budget. Multi-phase host
code retains request normalization, cross-statement budget accounting, and
response shaping; it does not scan or join the graph outside the query runtime.
These business reads intentionally have no route-specific typed database API.
Skill detach deletes are covered by `Database::delete_knowledge_skills` for
Nowledge Skill rollback and cleanup paths. This Skill-specific business facade
validates Skill ids before WAL, reuses the typed entity `DETACH DELETE` path
with the fixed `Skill` label, reports missing and idless rows without writing,
cascades `SYNTHESIZED_FROM` evidence relationships through storage/WAL, and
commits eligible Skill node deletes through one grouped WAL batch.
Thread compensation deletes are covered by `Database::delete_knowledge_threads`
for exact Nowledge `(:Thread {id}) DETACH DELETE` cleanup. The Thread-specific
facade validates Thread ids before WAL, reuses the typed entity `DETACH DELETE`
path with the fixed `Thread` label, reports missing and idless rows without
writing, cascades `CONTAINS` and `COMPACTS_TO` relationships through
storage/WAL while preserving Message and Memory endpoint nodes, and commits
eligible Thread node deletes through one grouped WAL batch. Thread-owned
Message node cleanup remains a separate Nowledge cleanup shape.
Thread metadata writes are covered by a typed batch for Nowledge `metadata`
updates with optional `updated_at` stamping. The wrapper validates Thread ids
before WAL, reports missing, idless, and duplicate rows without writing, keeps
metadata-only updates from changing existing timestamps, and commits eligible
Thread rows through one grouped WAL batch.
Thread denormalized message-count writes are covered by a typed batch for
Nowledge `message_count` refreshes with optional `updated_at` stamping and
`preserve_newer_existing_updated_at` semantics. The wrapper validates Thread
ids and non-negative counts before WAL, keeps newer existing timestamps when
requested, reports missing, idless, and duplicate rows without writing, and
commits eligible Thread rows through one grouped WAL batch.
ThreadIdentity compensation and cascade deletes are covered by
`Database::delete_knowledge_thread_identities`. The typed facade validates
exactly one delete mode before WAL, supports the exact
`MATCH (ti:ThreadIdentity {id}) DETACH DELETE ti` compensation shape and the
Nowledge cascade predicate over `public_thread_id`, `input_thread_id`, and
`thread_uuid`, reports matched/deleted identity counts plus deleted node ids,
deduplicates overlapping cascade matches, keeps missing cleanup read-only, and
routes eligible deletes through the WAL-backed Cypher mutation path.
Thread ordered message reads use a pinned read transaction with a fixed exact
Thread lookup followed by a fixed parameterized `CONTAINS` page query. The page
projects only Message and relationship fields needed by the caller, orders by
`COALESCE(r.order_index, m.order_index)`, and carries an explicit `LIMIT`, row
budget, and payload budget. The read intentionally has no route-specific typed
database API.
Thread-owned Message cleanup is covered by
`Database::delete_knowledge_thread_messages` for the Nowledge
`MATCH (t:Thread {id})-[:CONTAINS]->(m:Message) DETACH DELETE m` shape. The
typed facade validates Thread ids before WAL, reports missing Thread rows
without writing, counts matched `CONTAINS` relationships and unique Message
targets without materializing complete Message rows, preserves the Thread node,
deletes only outgoing Message target nodes through the WAL-backed Cypher
mutation path, and keeps empty-thread cleanup read-only.
Thread compacted-memory reads use a fixed physical- or logical-Thread identity
query followed by a fixed bounded `COMPACTS_TO` page query in one pinned read
transaction. Each business phase selects the Memory and relationship fields it
needs instead of materializing the former wide row, while keeping importance,
created-at, identity, and relationship id ordering in the statement. These
reads intentionally have no route-specific or caller-projection typed API.
Thread distilled-memory link writes are covered by
`Database::create_knowledge_thread_compaction_link`. This typed facade fixes
the Nowledge `(:Thread)-[:COMPACTS_TO]->(:Memory)` write shape, validates
Thread ids, Memory ids, and compaction methods before WAL, writes only the
Nowledge-used `compaction_method`, `created_at`, and `properties`
relationship fields, reports missing or projected-idless endpoints without
writing, and routes eligible links through the WAL-backed relationship create
path.
Memory compacting-Thread reads use one fixed exact-Memory query and one fixed
incoming `COMPACTS_TO` page query per requested Memory in a pinned read
transaction. The host preserves input order, missing-Memory shaping, and the
cross-statement budget; the statement owns Thread/relationship projection,
stable ordering, and the per-Memory limit. These reads intentionally have no
route-specific or caller-projection typed API.
Thread list, distinct-source, attachment, source-summary, render metadata,
identity, and sync metadata reads use fixed parameterized Cypher through the
bounded query APIs. Predicates cover physical and logical ids, source, space,
metadata markers, pagination, and exact/prefix/contains lookup; ordering,
projection, `COALESCE` behavior, and per-phase `LIMIT` clauses remain visible
in each statement. Callers use pinned read transactions when several phases
must observe one snapshot. These business reads intentionally have no
route-specific typed database API.
Label lifecycle writes are covered by a typed batch for Nowledge metadata
updates, canonical-name backfill, and rename/canonical-name updates. The
wrapper validates Label ids, non-empty names, and non-empty canonical names
before WAL, reports missing, idless, and duplicate rows without writing, and
commits eligible Label rows through one grouped WAL batch.
Memory label cleanup writes are covered by
`Database::delete_knowledge_memory_labels`. The typed facade fixes the Nowledge
`(:Memory)-[:HAS_LABEL]->(:Label)` cleanup shape, supports exact Memory/Label
edge deletes and all-label deletes for one Memory, validates ids before WAL,
reports missing or projected-idless endpoints without writing, keeps empty
cleanup read-only, and commits eligible relationship deletes through one
grouped WAL batch.
Label merge transfer writes are covered by
`Database::transfer_knowledge_label_memory_edges`. The typed facade fixes the
Nowledge source-label to target-label Memory transfer shape, scans Memory nodes
that already have the source `HAS_LABEL` edge, de-duplicates repeated source
edges, skips projected-idless Memory rows without writing, and idempotently
MERGEs the target `HAS_LABEL` edge with create-only `assigned_by`,
`created_at`, and `properties` fields through one grouped WAL batch.
Memory label carry-over writes are covered by
`Database::transfer_knowledge_memory_label_edges`. The typed facade fixes the
Nowledge older-Memory to newer-Memory label copy shape used during Memory
evolution, requires both Memory endpoints to match the exact `space_id`
predicate before writing, scans distinct Label targets from the older Memory,
skips projected-idless Labels without writing, and idempotently MERGEs
`newer` `HAS_LABEL` edges with create-only `assigned_by = 'system'`,
`created_at`, and `properties = '{}'` through one grouped WAL batch.
Label canonical collision lookup and missing-canonical backfill use four fixed
parameterized Cypher statements for their include/exclude variants. Single-
Label, canonical-only, and all-Label usage reads use another three fixed
statements with `HAS_LABEL` counts over any source node type, deterministic
ordering, explicit `LIMIT`, and matching row budgets. No route-specific Label
read API is exposed.
Label Memory distribution reads use one fixed parameterized Cypher statement
that counts distinct Memory nodes per Label over `HAS_LABEL`, orders by count
and stable Label identity, and applies `SKIP`, `LIMIT`, and a matching row
budget. A caller that needs a total count issues a separate bounded aggregate
query; no route-specific typed read API is exposed.
Label regex Memory connection reads use one fixed parameterized Cypher
statement with explicit Memory/Label projection, grouped `HAS_LABEL` counts,
deterministic ordering, `SKIP`, `LIMIT`, a matching row budget, and a pinned
snapshot. Any total-count phase is a separate host-owned bounded query; no
caller-projection typed database API is exposed.
Endpoint-known `HAS_LABEL` assignment and field-extensible Label projections use
fixed parameterized Cypher. Callers select the concrete entity label, bind a
non-empty external-id list, project only the Label and relationship properties
needed by that phase, and enforce deterministic ordering plus per-entity row
and payload budgets. No route-specific Label read API or DTO is exposed.
Induced edge-list reads use a fixed parameterized Cypher statement whose source
and target ids are both constrained by the selected node-id list. Graph canvas
decodes the bounded relationship projection directly and records its query
report alongside the node-phase reports; no route-specific induced-edge API is
exposed.
PageRank score writes are covered by typed batches for Nowledge Memory and
Entity `pagerank_score` persistence and clear operations. The wrapper accepts
only finite non-negative scores for Memory/Entity identities, reports missing,
idless, duplicate, and clear-only non-writable rows without writing, and commits
eligible score writes or clears through one grouped WAL batch.
PageRank planning and read-side business logic uses host-owned, parameterized
Cypher instead of PageRank-specific request/output DTOs. Node and relationship
counts are separate named statements, while membership, Memory visibility, and
central-entity lookups use bounded list/equality parameters. A host that needs
all count phases from one graph version executes them through one
`DatabaseReadTransaction` and records its `commit_epoch`. PageRank score and
clear mutations remain typed because they validate the whole batch and commit
eligible changes through one grouped WAL boundary.
GraphMeta algorithm stamps are covered by a typed batch for Nowledge PageRank
and community-detection state updates shaped as `MERGE (m:GraphMeta {meta_id})
SET ...`. The wrapper validates non-empty `meta_id` values and property names,
rejects attempts to mutate `meta_id`, creates missing GraphMeta rows, updates
existing rows, reports duplicate stamps without writing, and commits eligible
stamps through one grouped WAL batch.
GraphMeta state reads use host-owned fixed, parameterized Cypher projections
selected for each algorithm state shape. Queries bind the Nowledge `meta_id`
identity, declare an explicit row budget, and share a
`DatabaseReadTransaction` when multiple state reads must observe one graph
version. New state fields are adopted by adding a readable fixed projection,
not by passing property identifiers through a generic typed facade.
GraphMeta stamps and cleanup deletes remain typed contracts. Stamps validate
and commit an eligible batch through one grouped WAL boundary. Deletes reject
empty identities before WAL, do not write WAL for missing rows, and persist
eligible cleanup through the WAL-backed `DELETE` path.
Schema migration log writes are covered by a typed create-once batch for
Nowledge `SchemaMigrationLog` ids shaped as `MERGE ... ON CREATE SET
applied_at`. The wrapper validates non-empty migration ids before WAL, reports
already-applied and duplicate rows without writing, preserves existing
`applied_at` values, and commits eligible new migration rows through one grouped
WAL batch.
Schema migration log reads use host-owned fixed parameterized Cypher. A host
that needs both the total and a bounded page executes named count and page
statements through one `DatabaseReadTransaction`; the page orders by migration
id and projects only `id`, node id, and `applied_at`.
AugmentationJob lifecycle writes are covered by a typed batch for Nowledge job
creation, pending-to-running starts, running progress updates,
running-to-completed results, and pending/running-to-failed errors. The wrapper
validates job ids, job types, progress percentages, progress messages, and
failure messages before WAL, reports missing, existing, status-mismatched, and
duplicate jobs without writing, and commits eligible creates/updates through
one grouped WAL batch.
AugmentationJob status and list reads use host-owned, named parameterized
Cypher. Exact lookup binds `job_id`; list routes execute separate count and page
statements on one read transaction, bind status and limit values, and choose a
fixed `started_at DESC` or `created_at DESC` statement rather than interpolating
an order identifier. Lifecycle and interrupt mutations remain typed because
they validate and commit multiple state transitions through one WAL boundary.
AugmentationJob stale/orphan interrupt writes are covered by
`Database::interrupt_knowledge_augmentation_jobs` for the Nowledge
`interrupt_orphaned_jobs` shape. The wrapper scans only `AugmentationJob`
nodes in `pending` or `running` state, validates the interruption reason before
WAL, marks eligible jobs as `failed` with the production interruption message,
does not write WAL when no eligible jobs exist, and commits eligible updates
through one grouped WAL batch.
Source-reference relationship cleanup is covered by a typed API for Nowledge
memory/source delete flows. `Database::delete_knowledge_source_reference_relationships`
scans only `RELATES_TO.source_reference`, rejects empty references before WAL,
preserves endpoint Entity nodes, and commits eligible relationship deletes
through one grouped WAL batch.
Delete-flow read guards use host-owned named parameterized Cypher statements.
Endpoint discovery projects bounded `RELATES_TO.source_reference` rows for
host-side deduplication. The orphan guard uses separate exact-entity, incident
count, and incoming count statements on one pinned read transaction, preserving
the historical incoming-edge double-counting explicitly. The cleanup mutation
remains typed because it owns validation and one grouped WAL boundary.
Entity-to-Community membership writes are covered by
`Database::create_knowledge_community_memberships_batch` for the Nowledge
entity lifecycle `BELONGS_TO` creation shape. The wrapper validates non-empty
Entity and Community ids plus finite strengths before WAL, reports missing or
non-writable endpoints, preserves endpoint nodes, and commits eligible
memberships through one grouped WAL batch.
Community detection result creation and scheduler summary refresh writes are
covered by `Database::update_knowledge_communities_batch`. The wrapper validates
non-empty Community ids and names, non-negative `community_id` and
`member_count`, and finite `resolution` before WAL, fixes created communities
to the Nowledge `louvain` algorithm marker, reports existing, missing,
duplicate, and non-writable rows, and commits eligible creates plus summary
updates through one grouped WAL batch.
Community summary lists and detail lookups use fixed parameterized Cypher
statements. Summary-only and summary-presence rankings are distinct bounded
statements; numeric `community_id` and external `id` detail lookups are also
distinct statements with a one-row budget. No Community list/detail route API
or DTO is exposed, and these reads do not write WAL.
Community node cleanup is covered by `Database::delete_knowledge_communities`
for Nowledge replace-community and undo-community flows. It scans only
`Community` nodes, supports the two production cleanup modes (`DELETE` and
`DETACH DELETE`), preserves non-Community endpoint nodes under detach cleanup,
does not write WAL when no Community nodes exist, and commits eligible deletes
through one grouped WAL batch.

## Current Direction

HawDB should keep graph and search separated:

```text
canonical graph store
  nodes, relationships, schema tokens, WAL, checkpoint

search projection
  text, embeddings, denormalized filter metadata, lifecycle markers

content store
  large payloads, source chunks, message bodies
```

This matches the existing Nowledge invariant: graph identity is durable,
search is rebuildable, and large content is not duplicated into the graph store.
Resource-constrained embedded deployments should consume background maintenance
summaries through typed QoS hint fields, not by parsing human-readable ranking
reasons.

## Deferred Extensions And Release Use

The active backlog is maintained in `TODO.md`. Page-level MVCC, broader
cross-pattern statistics, additional analyzer families, and caller-owned
content parser integrations are not implicit replacement tasks. They require a
new active route, measured workload, or explicit product boundary before they
enter the backlog.

Further crate splits require a clear ownership boundary, dependency-direction
benefit, compile-time isolation benefit, or stable reuse contract. Crate count
is not itself an implementation goal.

Use `ExternalShadowCommand` with the previous local graph wrapper only when a
specific migration gate requires compatibility evidence. Use
`nowledge-replacement-summary --require-production-ready` as the final reporting
guard after the migration bundle includes previous-wrapper, storage recovery,
resource, and background-maintenance evidence.
