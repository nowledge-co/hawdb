# Skein PostgreSQL-Dialect Relational Content Store Specification

## Scope

This specification defines Skein's relational row-storage primitives and the
PostgreSQL-dialect SQL subset required to replace the scoped Nowledge SQLite
Content Store. PostgreSQL is a syntax and semantic reference. Skein MUST NOT
require a PostgreSQL server or client library to execute this workload.

The initial schema scope is:

- `content_documents`;
- `thread_messages`;
- `content_chunks`;
- `content_anchors`;
- `content_migration_state`.

External artifact and blob files remain outside this contract.

## Statement Corpus

`fixtures/nowledge_content_store/postgres_statement_corpus_v1.json` is the
versioned compatibility corpus. It records the source caller, normalized SQL,
parameter types, result columns, deterministic ordering, row budget, payload
budget, and transaction group.

The corpus MUST use the actual Nowledge v3 schema. In particular,
`content_anchors` contains `quote_hash` and `content_message_id`; it does not
contain fabricated `content_hash` or `updated_at` columns.

Every corpus revision has a protocol, revision, and SHA-256 identity. A cutover
gate MUST compare all three values and MUST fail closed when any value differs
from the qualified artifact. `covered` callers have a complete statement
mapping. `partial` callers MUST NOT be treated as cutover-ready.

## SQL Frontend

`skein-sql` owns its SELECT, DDL, and DML AST. Public APIs MUST NOT expose
third-party parser types. The supported parameter contract uses dense,
one-based PostgreSQL `$1`, `$2`, ... positions. Preparation MUST reject a zero
position, a missing position, or supplied parameter cardinality that differs
from the prepared metadata.

The scoped grammar includes:

- table and column aliases, `INNER JOIN`, and `LEFT JOIN`;
- boolean predicates with SQL three-valued null behavior;
- `DISTINCT`, `GROUP BY`, `COUNT`, `SUM`, `MAX`, `COALESCE`, and
  `OCTET_LENGTH`;
- deterministic PostgreSQL null ordering, `LIMIT`, and `OFFSET`;
- `INSERT`, multi-row `VALUES`, `ON CONFLICT`, `UPDATE`, and `DELETE`;
- `CREATE TABLE`, primary/unique/foreign-key constraints, `CREATE INDEX`, and
  append-only `ALTER TABLE ... ADD COLUMN` parsing.

Unsupported syntax MUST fail during parse or binding. It MUST NOT silently use
different semantics.

## Relational Storage

Relational tables are separate from graph node and relationship tables. A
caller MUST NOT encode these rows as synthetic graph nodes.

The scalar storage types are `BOOLEAN`, `BIGINT`, `DOUBLE PRECISION`, `TEXT`,
and `BYTEA`, including nullability and deterministic defaults. Primary keys,
unique constraints, ordered secondary indexes, and foreign keys are evaluated
against the final staged transaction state.

Every relational table MUST declare exactly one primary-key constraint. The
key may contain multiple columns, but multiple independent column-level
`PRIMARY KEY` declarations are invalid and MUST NOT be reinterpreted as a
composite key. A table-level composite primary key makes every participating
column `NOT NULL`, matching PostgreSQL semantics. DDL binding rejects a missing
or ambiguous primary key before creating a storage transaction, and catalog
publication, WAL replay, and checkpoint decode independently enforce the same
invariant.

Rows and index postings use ordered immutable COW pages. Publishing a snapshot
shares all untouched pages. A point insert, update, or delete MUST clone only
the affected row and posting pages, apart from a page split. Creating an index
may scan the table once; ordinary mutations MUST maintain materialized indexes
incrementally. Foreign-key validation MUST inspect changed child rows and MUST
scan a child table only when a referenced visible key is actually removed or
changed.

Transactions are admitted by mutation-row and payload-byte limits before
publication. A constraint or durability failure MUST leave the published epoch
and snapshot unchanged.

## Large Values

Large non-key `TEXT` and `BYTEA` values use immutable overflow envelopes. Key,
unique, foreign-key, and index columns remain inline. The envelope contains a
magic value, codec, scalar type, stored and logical lengths, CRC32C, and a
SHA-256-addressed segment identity.

The default externalization threshold is 4 KiB. Zstd level 3 is selected only
when a 4 KiB sample, or the complete value when smaller, saves at least 25%
and at least 64 bytes. Otherwise the envelope uses the raw codec. This choice
avoids paying full-value Zstd CPU for high-entropy content while preserving the
same overflow and integrity boundary.

Hydration MUST occur after predicate, ordering, offset, and limit selection.
It MUST stage and enforce row, stored-byte, logical-byte, and memory budgets
before publishing budget consumption. Missing, malformed, oversized, or
checksum-invalid envelopes fail closed. Snapshot-pinned overflow segments MUST
remain readable until their rows are no longer reachable from any pinned
snapshot.

## Durability Boundary

`GraphStore` owns relational state beside graph state. A mixed mutation stages
the complete relational COW state and graph validation before append, then
encodes the relational transaction inside the same canonical WAL batch. The
outer record contributes one LSN and one commit epoch; the inner relational
envelope repeats that epoch and carries its own CRC32C and SHA-256 integrity
boundary. A mismatch or a post-WAL apply failure poisons the handle and requires
strict replay after reopen.

Checkpoint publication writes the binary relational checkpoint before the
generation checkpoint. Its length, CRC32C, and SHA-256 are covered by the
checksummed generation checkpoint, which is published by the same durable
manifest as graph canonical segments and the replacement WAL. Backup, restore,
scrub, generation reclamation, and reopen include this artifact and fail closed
when it is missing, corrupt, oversized, or carries a different commit epoch.
There is no second relational WAL, fsync stream, database handle, or production
sidecar API.

## Query Execution Boundary

The existing PostgreSQL `Database::query_sql*` entrypoints dispatch `system.*`
reads to the system-table executor and public-schema DDL, DML, and `SELECT` to
the relational state owned by `GraphStore`. Read transactions pin the same COW
relational snapshot as graph state. This exposes statements rather than
route-specific typed APIs.

The current relational executor implements the frozen corpus semantics,
including joins, aggregation, ordering, distinct, budgets, and late hydration.
It remains a qualification implementation until those operators use the shared
optimizer and batch pipeline.

The qualification access-path selector supports complete composite primary
keys and the longest bound leading equality prefix of composite unique and
secondary indexes. It constructs explicit descriptors and applies Skyline
pruning as a Pareto frontier over constrained columns, equality-prefix length,
unique point lookup, covering and row-fetch properties, and bounded snapshot
cardinality. One path may prune another only when it is no worse in every
comparable dimension and strictly better in at least one. Paths constrained by
different non-superset predicate column sets remain incomparable. The selected
path and its properties are part of the typed query result so qualification can
assert the actual decision.

Qualified conjunctive equality joins derive right-side composite primary-key or
leading index-prefix access. Primary-key joins perform one point lookup per
left row; index-prefix joins visit posting rows without first collecting the
complete posting list. The selected path is reported separately for every
join. Predicates that cannot prove a safe right-side key fall back to a direct,
non-collecting table scan.

Relational aggregate groups retain incremental `COUNT`, `SUM`, `MAX`, and
`COALESCE` state instead of a second copy of every qualified binding. DISTINCT
aggregates retain only their value set. Group keys and dynamic aggregate state
are charged to the shared executor blocking-operator tracker and fail closed
when `blocking_operator_bytes` is exhausted. The qualification input buffer is
still row-oriented until the relational plan is lowered into the shared batch
pipeline; spill and cancellation are therefore not yet claimed here.

This slice does not claim index range scans, sort elimination, covering
projection, or access through equality hidden inside disjunctions. Those
properties MUST remain unset until the executor implements and reports them;
Skyline pruning MUST NOT infer them from index shape alone. Base prefix lookup
and cardinality probes remain bounded by the query intermediate-row admission
limit.

Production activation requires relational scan and index-access paths in the
shared optimizer and batch executor, shared blocking-operator memory and spill
tracking, cancellation, query reports, and `EXPLAIN ANALYZE`. Content routes
MUST remain named parameterized SQL statements; Skein MUST NOT add one typed
API per route.

## Migration Boundary

SQLite snapshotting and `rusqlite` remain in the Nowledge migration adapter.
Skein MUST NOT acquire SQLite as a production dependency. Migration uses stable
keyset pages, durable cursors, idempotent primary-key/content-hash replay, a
legacy write fence or durable obligation protocol, and differential validation
before cutover. The source SQLite database remains unchanged until a separately
authorized decommissioning step.

## Qualification

The focused Rust tests cover:

- parser preparation and every statement in the embedded corpus;
- materialization of the actual v3 schema;
- joins, aggregates, deterministic pages, and late hydration;
- transaction-final primary, unique, and foreign-key behavior;
- immutable row and posting COW pages;
- canonical mixed graph/relational WAL replay, post-WAL apply poisoning,
  checkpoint/reopen, backup/restore, scrub, and corruption;
- raw and Zstd overflow selection, integrity, budgets, snapshot pinning, and
  reachability GC.

`cargo bench --bench relational_overflow` reports inline and overflow write and
hydration P50/P95/P99 for repetitive, varied, and high-entropy payloads from
1 KiB through 64 KiB. The benchmark is micro-level codec evidence only. It MUST
NOT be used as file-backed RSS, page-fault, or end-to-end query evidence.

`cargo bench --bench relational_index_access` compares a full scan with the
selected composite-prefix path at 1,000, 10,000, and 50,000 rows. It reports
P50/P95/P99 latency, matched and one-column-prefix cardinalities, selected
index, selected equality-prefix length, speedup, and process RSS delta. This is
an in-memory access-path microbenchmark; it does not qualify file-backed cache,
page-fault, concurrent writer, or end-to-end SQL behavior.

Production qualification still requires the differential SQLite oracle,
50,000-message and large-source workloads, fault injection through the unified
WAL/checkpoint path, shadow traffic, and Windows, Linux, and macOS evidence.
