# Skein PostgreSQL-Dialect Relational Content Store Specification

## Scope

This specification defines Skein's relational storage primitives and the
PostgreSQL-dialect SQL subset required to replace the scoped Nowledge SQLite
Content Store. PostgreSQL is a syntax and semantic reference. Skein MUST NOT
require a PostgreSQL server or client library to execute this workload.

The durable storage clauses of this contract (private relational checkpoint
`SKRLCKP1`, private relational WAL `SKRLWAL1`, and private overflow
`SKOVFL01`) are superseded by
[`COLUMNAR_CANONICAL_AND_PROJECTION_SPEC.md`](COLUMNAR_CANONICAL_AND_PROJECTION_SPEC.md)
§3–§7 as its phases land: relational tables become columnar tables in the
unified format, sharing one WAL, one blob store, and the durable projection
framework for secondary indexes. The SQL statement corpus, semantics, and
qualification obligations of this document are unchanged by that migration.

The initial schema scope is:

- `content_documents`;
- `thread_messages`;
- `content_chunks`;
- `content_anchors`;
- `content_migration_state`.

External artifact and blob files remain outside this contract.

## Statement Corpus

`crates/qualification/fixtures/nowledge_content_store/content_store_schema_v1.sql`
is the authoritative initial DDL. It contains executable `CREATE TABLE` and
`CREATE INDEX` statements and is versioned independently from the workload.
Production schema initialization MUST execute this ordered DDL or an explicit
append-only successor; it MUST NOT reconstruct schema from test metadata.

`crates/qualification/fixtures/nowledge_content_store/postgres_statement_corpus_v1.json`
is the versioned compatibility workload. It records the source caller,
normalized read and mutation SQL, parameter types, result columns,
deterministic ordering, row budget, payload budget, and transaction group. It
does not own schema DDL.

The corpus MUST use the actual Nowledge v3 schema. In particular,
`content_anchors` contains `quote_hash` and `content_message_id`; it does not
contain fabricated `content_hash` or `updated_at` columns.

The schema and corpus each have an independent protocol, revision, and SHA-256
identity. A cutover gate MUST compare both identities and MUST fail closed when
any value differs from the qualified artifact. `covered` callers have a
complete statement mapping. `partial` callers MUST NOT be treated as
cutover-ready.

The schema and corpus belong to `skein-qualification`, not the default `skein`
facade. `skein-content-store-contract` is a thin developer tool over the typed
qualification API. The default embedded database build therefore does not
carry Mem-specific workload fixtures.

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

Before the first durable checkpoint, newly externalized envelopes may remain
inline in the unpublished COW state. Checkpoint publication writes every
reachable envelope once into the checksummed relational generation artifact.
Publication emits table metadata and rows in bounded chunks and copies one
overflow envelope at a time while incrementally computing the payload digest;
it does not build a second complete checkpoint `Vec` in memory. The manifest
digest is computed by a bounded sequential pass over the temporary artifact
before atomic publication.
The newly published state replaces those resident byte arrays with immutable
file-range descriptors carrying the envelope content digest. Reopen, backup,
restore, and checkpoint rollover reconstruct the same descriptors; hydration
performs a bounded range read and verifies the range digest before decoding the
envelope. Generation reclamation is fenced by the existing read-transaction
watermark, so a pinned relational snapshot retains the generation containing
its ranges. The decoder rejects an oversized artifact from file metadata before
reading payload bytes, then parses rows and validates the outer and per-overflow
digests in one pass with a bounded 64 KiB transfer buffer. It never collects the
checkpoint's overflow section into a resident validation buffer.

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

The relational executor implements the frozen corpus semantics, including
joins, aggregation, ordering, distinct, budgets, and late hydration. Base scans,
primary-key lookups, index-prefix visits, joins, residual filters, projection,
offset, and limit run as a pull-through visitor pipeline. The pipeline checks
the runtime cancellation token after every admitted batch and never constructs
a complete intermediate `Vec` of qualified rows.

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
`COALESCE` state instead of a second copy of every qualified binding. The
executor sends typed binding batches through the shared `TopNExec`, `SortExec`,
and `DistinctExec` implementations. High-cardinality statement `DISTINCT`,
`COUNT(DISTINCT ...)`, ordering, and grouped aggregation therefore share the
executor memory tracker, spill byte/run limits, cleanup rules, and cancellation
checkpoints. Grouped aggregation externally sorts compact row locators and
retains one group state at a time; non-grouped aggregation retains one admitted
incremental state. Large payload hydration remains after the blocking locator
selection unless exact statement `DISTINCT` requires the projected value.

`EXPLAIN SELECT` plans without opening a scan and returns TiDB-style `id`,
`estRows`, `task`, `access object`, and `operator info` columns through the SQL
query path. `EXPLAIN ANALYZE SELECT` executes the same runtime path and adds
`actRows`, `execution info`, `memory`, and `disk`, including intermediate rows,
hydration bytes, blocking-operator peak/budget bytes, and measured spill runs,
rows, and bytes. Unsupported EXPLAIN options fail during binding rather than
changing semantics.

This slice does not claim index range scans, sort elimination, covering
projection, or access through equality hidden inside disjunctions. Those
properties MUST remain unset until the executor implements and reports them;
Skyline pruning MUST NOT infer them from index shape alone. Base prefix lookup
and cardinality probes remain bounded by the query intermediate-row admission
limit.

Content routes MUST remain named parameterized SQL statements; Skein MUST NOT
add one typed API per route. Production activation still requires the
identity-bound migration, differential, recovery, representative-load, and
cross-platform evidence described below; implemented executor mechanics alone
are not cutover evidence.

## Migration Boundary

SQLite snapshotting and `rusqlite` remain in the Nowledge migration adapter.
Skein MUST NOT acquire SQLite as a production dependency. Migration uses stable
keyset pages, durable cursors, idempotent primary-key/content-hash replay, a
legacy write fence or durable obligation protocol, and differential validation
before cutover. The source SQLite database remains unchanged until a separately
authorized decommissioning step.

## Qualification

The focused Rust tests cover:

- parser preparation and every statement in the qualification corpus;
- execution of the independently versioned v3 DDL through the public SQL path;
- joins, aggregates, deterministic pages, and late hydration;
- transaction-final primary, unique, and foreign-key behavior;
- immutable row and posting COW pages;
- canonical mixed graph/relational WAL replay, post-WAL apply poisoning,
  checkpoint/reopen, backup/restore, scrub, and corruption;
- raw and Zstd overflow selection, integrity, budgets, snapshot pinning, and
  reachability GC;
- file-backed overflow checkpoint/reopen/backup/restore and bounded hydration;
- streaming scan/join/filter/projection, runtime cancellation, shared
  sort/distinct/group spill, and SQL `EXPLAIN`/`EXPLAIN ANALYZE` reports.

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
