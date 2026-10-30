# HawDB Specification Index

This directory indexes the contracts that define HawDB's supported production
surface. `TODO.md` is a backlog, not a historical completion log. Once an
implemented capability is represented by a contract below, its completed task
must be removed from `TODO.md`.

Normative `MUST`, `MUST NOT`, `SHOULD`, and `MAY` clauses take precedence over
descriptive implementation notes. A code change that alters one of these
contracts must update the corresponding specification in the same change.

| Area | Canonical contract | Supporting design and evidence documents |
| --- | --- | --- |
| Encoded spill merge, cooperative work-area targets and filtered traversal feedback | [`ADAPTIVE_EXECUTION_MEMORY_SPEC.md`](ADAPTIVE_EXECUTION_MEMORY_SPEC.md) | [`FILTERED_DISKANN_SPEC.md`](FILTERED_DISKANN_SPEC.md), [`../TEST_INDEX.md`](../TEST_INDEX.md) |
| Qualified transaction read capture and opt-in serializable commit validation | [`TRANSACTION_READ_DEPENDENCIES.md`](TRANSACTION_READ_DEPENDENCIES.md) | [`MVCC_COW_EVOLUTION_SPEC.md`](MVCC_COW_EVOLUTION_SPEC.md), [`../MVCC_COMMIT_VALIDATION_PROTOCOL.md`](../MVCC_COMMIT_VALIDATION_PROTOCOL.md) |
| Release-facing application API boundary | [`QUERY_FIRST_PUBLIC_API_SPEC.md`](QUERY_FIRST_PUBLIC_API_SPEC.md) | [`../ARCHITECTURE.md`](../ARCHITECTURE.md), [`../NOWLEDGE_REPLACEMENT_MATRIX.md`](../NOWLEDGE_REPLACEMENT_MATRIX.md) |
| Embedded ownership, concurrency, durability, resources, capabilities, ACL, and telemetry | [`EMBEDDED_RUNTIME_SPEC.md`](EMBEDDED_RUNTIME_SPEC.md) | [`../STORAGE.md`](../STORAGE.md), [`../ARCHITECTURE.md`](../ARCHITECTURE.md), [`../DATABASE_INSTANCE_IDENTITY.md`](../DATABASE_INSTANCE_IDENTITY.md) |
| Production traffic admission and release qualification | [`PRODUCTION_READINESS_SPEC.md`](PRODUCTION_READINESS_SPEC.md) | [`../PRODUCTION_CONTENT_STORE_QUALIFICATION.md`](../PRODUCTION_CONTENT_STORE_QUALIFICATION.md), [`../PRODUCTION_CONTENT_STORE_MEMORY_QUALIFICATION.md`](../PRODUCTION_CONTENT_STORE_MEMORY_QUALIFICATION.md), [`../PRODUCTION_GRAPH_STORAGE_QUALIFICATION.md`](../PRODUCTION_GRAPH_STORAGE_QUALIFICATION.md), [`../PRODUCTION_GRAPH_INDEX_QUALIFICATION.md`](../PRODUCTION_GRAPH_INDEX_QUALIFICATION.md), [`../NOWLEDGE_PREVIOUS_WRAPPER_PREFLIGHT.md`](../NOWLEDGE_PREVIOUS_WRAPPER_PREFLIGHT.md), [`../EXTERNAL_SHADOW_PROTOCOL.md`](../EXTERNAL_SHADOW_PROTOCOL.md) |
| Storage format, WAL, checkpoints, COW snapshots, out-of-core reads, and immutable projections | [`../STORAGE.md`](../STORAGE.md) | [`../ARCHITECTURE.md`](../ARCHITECTURE.md), [`../tla/README.md`](../tla/README.md) |
| Durable COW branching, branch identity, agent lifecycle, and global object retention (implementation contract; not yet available) | [`BRANCHING_STORAGE_SPEC.md`](BRANCHING_STORAGE_SPEC.md) | [`../STORAGE.md`](../STORAGE.md), [`../tla/BRANCH_LIFECYCLE_PROOF.md`](../tla/BRANCH_LIFECYCLE_PROOF.md) |
| Unified graph and relational identity changefeed for external search projections | [`SEARCH_PROJECTION_CHANGEFEED_SPEC.md`](SEARCH_PROJECTION_CHANGEFEED_SPEC.md) | [`../STORAGE.md`](../STORAGE.md), [`../tla/README.md`](../tla/README.md), [`PRODUCTION_READINESS_SPEC.md`](PRODUCTION_READINESS_SPEC.md) |
| Row-page canonical storage, demand-paged persistent indexes, startup/recovery boundaries, derived projections, memory budgets, and locking | [`ROW_PAGE_AND_DEMAND_PAGED_INDEX_SPEC.md`](ROW_PAGE_AND_DEMAND_PAGED_INDEX_SPEC.md) | [`../STORAGE.md`](../STORAGE.md), [`../tla/README.md`](../tla/README.md), [`VECTORIZED_MORSEL_EXECUTION_SPEC.md`](VECTORIZED_MORSEL_EXECUTION_SPEC.md), [`POSTGRES_RELATIONAL_CONTENT_STORE_SPEC.md`](POSTGRES_RELATIONAL_CONTENT_STORE_SPEC.md) |
| RaBitQ vector candidate projection, filtering, resource bounds, scalar dispatch, and raw rerank | [`RABITQ_VECTOR_PROJECTION_SPEC.md`](RABITQ_VECTOR_PROJECTION_SPEC.md) | [`../STORAGE.md`](../STORAGE.md), [`PRODUCTION_READINESS_SPEC.md`](PRODUCTION_READINESS_SPEC.md) |
| Lexical compact postings encoding, per-block term dictionary, and encoding-aware scan (design stage; no implemented surface yet) | [`LEXICAL_COMPACT_POSTINGS_SPEC.md`](LEXICAL_COMPACT_POSTINGS_SPEC.md) | [`../SEARCH_BUILD_RESOURCE_OWNERSHIP.md`](../SEARCH_BUILD_RESOURCE_OWNERSHIP.md), [`../STORAGE.md`](../STORAGE.md) |
| Large-document search input, exact streaming analysis, resource ownership, updates, and verified reads (proposed; implementation not yet qualified) | [`LARGE_DOCUMENT_LIFECYCLE_SPEC.md`](LARGE_DOCUMENT_LIFECYCLE_SPEC.md) | [`../SEARCH_GENERATION_CONTEXT.md`](../SEARCH_GENERATION_CONTEXT.md), [`../DOCUMENT_FREQUENCY_SPILL.md`](../DOCUMENT_FREQUENCY_SPILL.md), [`../STREAMING_SEARCH_HYDRATION.md`](../STREAMING_SEARCH_HYDRATION.md) |
| Typed columnar batches, numeric vectorized fragments, morsel admission, fallback, and performance evidence | [`VECTORIZED_MORSEL_EXECUTION_SPEC.md`](VECTORIZED_MORSEL_EXECUTION_SPEC.md) | [`../EXECUTOR_MORSEL_BENCHMARK.md`](../EXECUTOR_MORSEL_BENCHMARK.md), [`../ARCHITECTURE.md`](../ARCHITECTURE.md), [`EMBEDDED_RUNTIME_SPEC.md`](EMBEDDED_RUNTIME_SPEC.md) |
| PostgreSQL-dialect Content Store corpus, relational COW storage, large values, durability boundary, and migration qualification | [`POSTGRES_RELATIONAL_CONTENT_STORE_SPEC.md`](POSTGRES_RELATIONAL_CONTENT_STORE_SPEC.md) | [`../STORAGE.md`](../STORAGE.md), [`PRODUCTION_READINESS_SPEC.md`](PRODUCTION_READINESS_SPEC.md) |
| PostgreSQL SQL/PGQ property-graph DDL, `GRAPH_TABLE`, owned syntax frontend, shared graph IR, and compatibility qualification | [`POSTGRES_SQL_PGQ_SPEC.md`](POSTGRES_SQL_PGQ_SPEC.md) | [`../ARCHITECTURE.md`](../ARCHITECTURE.md), [`POSTGRES_RELATIONAL_CONTENT_STORE_SPEC.md`](POSTGRES_RELATIONAL_CONTENT_STORE_SPEC.md) |
| Convergent CRDT replication and anti-entropy synchronization between HawDB nodes (design stage; no implemented surface yet) | [`HAWDB_CRDT_REPLICATION_SPEC.md`](HAWDB_CRDT_REPLICATION_SPEC.md) | [`../tla/README.md`](../tla/README.md), [`../STORAGE.md`](../STORAGE.md) |
| Engine and application system-schema registration, ordered upgrades, checksum validation, and SQLite snapshot import boundary | [`SYSTEM_SCHEMA_UPGRADE_SPEC.md`](SYSTEM_SCHEMA_UPGRADE_SPEC.md) | [`POSTGRES_RELATIONAL_CONTENT_STORE_SPEC.md`](POSTGRES_RELATIONAL_CONTENT_STORE_SPEC.md), [`../STORAGE.md`](../STORAGE.md) |
| Cypher pipeline, optimizer, executor, statistics, explain output, and fuzzing | [`../ARCHITECTURE.md`](../ARCHITECTURE.md) | [`../EMBEDDED_DEVELOPMENT_PLAN.md`](../EMBEDDED_DEVELOPMENT_PLAN.md) |
| Nowledge graph/search replacement surface and route ownership | [`../NOWLEDGE_REPLACEMENT_MATRIX.md`](../NOWLEDGE_REPLACEMENT_MATRIX.md) | [`../EXTERNAL_SHADOW_PROTOCOL.md`](../EXTERNAL_SHADOW_PROTOCOL.md), [`../NOWLEDGE_PREVIOUS_WRAPPER_PREFLIGHT.md`](../NOWLEDGE_PREVIOUS_WRAPPER_PREFLIGHT.md) |
| External shadow comparison and final cutover evidence | [`../EXTERNAL_SHADOW_PROTOCOL.md`](../EXTERNAL_SHADOW_PROTOCOL.md) | [`../NOWLEDGE_PREVIOUS_WRAPPER_PREFLIGHT.md`](../NOWLEDGE_PREVIOUS_WRAPPER_PREFLIGHT.md) |

Historical PR sequencing and implementation milestones are non-normative. They
may remain in development plans or Git history, but they must not be copied
back into the active backlog after their acceptance contract is represented by
the documents above.

## Exploratory proposals

The following documents are not active production contracts. Their requirement
keywords describe prospective acceptance criteria, not new obligations on
existing APIs or unrelated changes. Adopting a capability requires a separately
scoped implementation review and qualification evidence; until then, the
canonical contracts above govern supported behavior.

| Proposal | Scope and adoption boundary |
| --- | --- |
| [`SOURCE_COHERENT_SEARCH_CATCH_UP_SPEC.md`](SOURCE_COHERENT_SEARCH_CATCH_UP_SPEC.md) | Additive fixed-E private staging and complete-source verification for #1043; conservative full-source cost, all post-E writes refused, registered Consumer/host integration still unqualified |
| [`MATERIALIZED_VIEWS_SPEC.md`](MATERIALIZED_VIEWS_SPEC.md) | Real-time graph/relational views: complete transaction deltas, incremental plans, online bootstrap and historical-epoch publication; phase one is Mem Memory/Space inventories plus Entity usage, read-only snapshot transactions and designated query-path acceptance; exact source epochs, derived state, serving indexes, and later Community/Source/Thread/search consumers require implementation and separate qualification |
| [`MVCC_COW_EVOLUTION_SPEC.md`](MVCC_COW_EVOLUTION_SPEC.md) | Proposed COW snapshot/retention and persistent Consumer lifecycle work, followed by complete transaction read dependencies and separately qualified opt-in serializability; existing SI/publication contracts remain authoritative |
| [`ZERO_COPY_COLUMNAR_INTERCHANGE_SPEC.md`](ZERO_COPY_COLUMNAR_INTERCHANGE_SPEC.md) | Measurement-first, opt-in strict zero-copy delivery with bounded pull and Go/Python ownership; producer-layout, SIMD, graph, and FTS extensions require separate workload evidence and implementation scope |
