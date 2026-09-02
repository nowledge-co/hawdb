# Skein Specification Index

This directory indexes the contracts that define Skein's supported production
surface. `TODO.md` is a backlog, not a historical completion log. Once an
implemented capability is represented by a contract below, its completed task
must be removed from `TODO.md`.

Normative `MUST`, `MUST NOT`, `SHOULD`, and `MAY` clauses take precedence over
descriptive implementation notes. A code change that alters one of these
contracts must update the corresponding specification in the same change.

| Area | Canonical contract | Supporting design and evidence documents |
| --- | --- | --- |
| Release-facing application API boundary | [`QUERY_FIRST_PUBLIC_API_SPEC.md`](QUERY_FIRST_PUBLIC_API_SPEC.md) | [`../ARCHITECTURE.md`](../ARCHITECTURE.md), [`../NOWLEDGE_REPLACEMENT_MATRIX.md`](../NOWLEDGE_REPLACEMENT_MATRIX.md) |
| Embedded ownership, concurrency, durability, resources, capabilities, ACL, and telemetry | [`EMBEDDED_RUNTIME_SPEC.md`](EMBEDDED_RUNTIME_SPEC.md) | [`../STORAGE.md`](../STORAGE.md), [`../ARCHITECTURE.md`](../ARCHITECTURE.md) |
| Production traffic admission and release qualification | [`PRODUCTION_READINESS_SPEC.md`](PRODUCTION_READINESS_SPEC.md) | [`../PRODUCTION_CONTENT_STORE_QUALIFICATION.md`](../PRODUCTION_CONTENT_STORE_QUALIFICATION.md), [`../PRODUCTION_CONTENT_STORE_MEMORY_QUALIFICATION.md`](../PRODUCTION_CONTENT_STORE_MEMORY_QUALIFICATION.md), [`../PRODUCTION_GRAPH_STORAGE_QUALIFICATION.md`](../PRODUCTION_GRAPH_STORAGE_QUALIFICATION.md), [`../PRODUCTION_GRAPH_INDEX_QUALIFICATION.md`](../PRODUCTION_GRAPH_INDEX_QUALIFICATION.md), [`../NOWLEDGE_PREVIOUS_WRAPPER_PREFLIGHT.md`](../NOWLEDGE_PREVIOUS_WRAPPER_PREFLIGHT.md), [`../EXTERNAL_SHADOW_PROTOCOL.md`](../EXTERNAL_SHADOW_PROTOCOL.md) |
| Storage format, WAL, checkpoints, COW snapshots, out-of-core reads, and immutable projections | [`../STORAGE.md`](../STORAGE.md) | [`../ARCHITECTURE.md`](../ARCHITECTURE.md), [`../tla/README.md`](../tla/README.md) |
| Unified graph and relational identity changefeed for external search projections | [`SEARCH_PROJECTION_CHANGEFEED_SPEC.md`](SEARCH_PROJECTION_CHANGEFEED_SPEC.md) | [`../STORAGE.md`](../STORAGE.md), [`../tla/README.md`](../tla/README.md), [`PRODUCTION_READINESS_SPEC.md`](PRODUCTION_READINESS_SPEC.md) |
| Row-page canonical storage, demand-paged persistent indexes, startup/recovery boundaries, derived projections, memory budgets, and locking | [`ROW_PAGE_AND_DEMAND_PAGED_INDEX_SPEC.md`](ROW_PAGE_AND_DEMAND_PAGED_INDEX_SPEC.md) | [`../STORAGE.md`](../STORAGE.md), [`../tla/README.md`](../tla/README.md), [`VECTORIZED_MORSEL_EXECUTION_SPEC.md`](VECTORIZED_MORSEL_EXECUTION_SPEC.md), [`POSTGRES_RELATIONAL_CONTENT_STORE_SPEC.md`](POSTGRES_RELATIONAL_CONTENT_STORE_SPEC.md) |
| RaBitQ vector candidate projection, filtering, resource bounds, scalar dispatch, and raw rerank | [`RABITQ_VECTOR_PROJECTION_SPEC.md`](RABITQ_VECTOR_PROJECTION_SPEC.md) | [`../STORAGE.md`](../STORAGE.md), [`PRODUCTION_READINESS_SPEC.md`](PRODUCTION_READINESS_SPEC.md) |
| Typed columnar batches, numeric vectorized fragments, morsel admission, fallback, and performance evidence | [`VECTORIZED_MORSEL_EXECUTION_SPEC.md`](VECTORIZED_MORSEL_EXECUTION_SPEC.md) | [`../EXECUTOR_MORSEL_BENCHMARK.md`](../EXECUTOR_MORSEL_BENCHMARK.md), [`../ARCHITECTURE.md`](../ARCHITECTURE.md), [`EMBEDDED_RUNTIME_SPEC.md`](EMBEDDED_RUNTIME_SPEC.md) |
| PostgreSQL-dialect Content Store corpus, relational COW storage, large values, durability boundary, and migration qualification | [`POSTGRES_RELATIONAL_CONTENT_STORE_SPEC.md`](POSTGRES_RELATIONAL_CONTENT_STORE_SPEC.md) | [`../STORAGE.md`](../STORAGE.md), [`PRODUCTION_READINESS_SPEC.md`](PRODUCTION_READINESS_SPEC.md) |
| PostgreSQL SQL/PGQ property-graph DDL, `GRAPH_TABLE`, owned syntax frontend, shared graph IR, and compatibility qualification | [`POSTGRES_SQL_PGQ_SPEC.md`](POSTGRES_SQL_PGQ_SPEC.md) | [`../ARCHITECTURE.md`](../ARCHITECTURE.md), [`POSTGRES_RELATIONAL_CONTENT_STORE_SPEC.md`](POSTGRES_RELATIONAL_CONTENT_STORE_SPEC.md) |
| Convergent CRDT replication and anti-entropy synchronization between Skein nodes (design stage; no implemented surface yet) | [`SKEIN_CRDT_REPLICATION_SPEC.md`](SKEIN_CRDT_REPLICATION_SPEC.md) | [`../tla/README.md`](../tla/README.md), [`../STORAGE.md`](../STORAGE.md) |
| Engine and application system-schema registration, ordered upgrades, checksum validation, and SQLite snapshot import boundary | [`SYSTEM_SCHEMA_UPGRADE_SPEC.md`](SYSTEM_SCHEMA_UPGRADE_SPEC.md) | [`POSTGRES_RELATIONAL_CONTENT_STORE_SPEC.md`](POSTGRES_RELATIONAL_CONTENT_STORE_SPEC.md), [`../STORAGE.md`](../STORAGE.md) |
| Cypher pipeline, optimizer, executor, statistics, explain output, and fuzzing | [`../ARCHITECTURE.md`](../ARCHITECTURE.md) | [`../EMBEDDED_DEVELOPMENT_PLAN.md`](../EMBEDDED_DEVELOPMENT_PLAN.md) |
| Nowledge graph/search replacement surface and route ownership | [`../NOWLEDGE_REPLACEMENT_MATRIX.md`](../NOWLEDGE_REPLACEMENT_MATRIX.md) | [`../EXTERNAL_SHADOW_PROTOCOL.md`](../EXTERNAL_SHADOW_PROTOCOL.md), [`../NOWLEDGE_PREVIOUS_WRAPPER_PREFLIGHT.md`](../NOWLEDGE_PREVIOUS_WRAPPER_PREFLIGHT.md) |
| External shadow comparison and final cutover evidence | [`../EXTERNAL_SHADOW_PROTOCOL.md`](../EXTERNAL_SHADOW_PROTOCOL.md) | [`../NOWLEDGE_PREVIOUS_WRAPPER_PREFLIGHT.md`](../NOWLEDGE_PREVIOUS_WRAPPER_PREFLIGHT.md) |

Historical PR sequencing and implementation milestones are non-normative. They
may remain in development plans or Git history, but they must not be copied
back into the active backlog after their acceptance contract is represented by
the documents above.
