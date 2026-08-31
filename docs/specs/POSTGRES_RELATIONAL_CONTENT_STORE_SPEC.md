# Skein PostgreSQL-Dialect Relational Content Store Specification

## Scope

This specification defines Skein's relational storage primitives and the
PostgreSQL-dialect SQL subset required to replace the scoped Nowledge SQLite
Content Store. PostgreSQL is a syntax and semantic reference. Skein MUST NOT
require a PostgreSQL server or client library to execute this workload.

The durable migration target is governed by
[`ROW_PAGE_AND_DEMAND_PAGED_INDEX_SPEC.md`](ROW_PAGE_AND_DEMAND_PAGED_INDEX_SPEC.md):
relational tables remain row-oriented, use the shared canonical WAL and large
value extents, and publish primary, unique, and secondary index roots with the
same row generation. Normal open demand-pages those indexes instead of
rebuilding every posting. The SQL statement corpus, semantics, and
qualification obligations of this document are unchanged by that migration.

The canonical schema scope is:

- `content_documents`;
- `thread_messages`;
- `content_chunks`;
- `content_anchors`;
- `content_migration_state`.

This is the Skein projection of Mem App Content Store schema v4, not a claim
that every SQLite control table becomes canonical Skein data. The App's
`content_schema_migrations` and `content_mutation_obligations` tables are
source-only control state. In particular, mutation obligations record delivery
to Skein; storing them in the destination would make the delivery ledger part
of the dataset it coordinates. The Mem integration contract MUST name these
tables explicitly and prove that they are excluded from import and full-data
comparison.

External artifact and blob files remain outside this contract.

## Statement Corpus

`crates/qualification/fixtures/nowledge_content_store/content_store_schema_v1.sql`
is the authoritative initial DDL. It contains executable `CREATE TABLE` and
`CREATE INDEX` statements and is versioned independently from the workload.
Skein has not entered production, so the current greenfield baseline may be
updated destructively when the App schema changes; existing development Skein
databases must then be recreated. After Skein acquires a production data
compatibility obligation, schema initialization MUST execute this ordered DDL
or an explicit append-only successor and MUST NOT reconstruct schema from test
metadata.

`crates/qualification/fixtures/nowledge_content_store/postgres_statement_corpus_v1.json`
is the versioned compatibility workload. It records the source caller,
normalized read and mutation SQL, parameter types, result columns,
deterministic ordering, row budget, payload budget, and transaction group. It
does not own schema DDL.

The corpus MUST use the canonical projection of Nowledge Content Store schema
v4. In particular, `content_anchors` contains `quote_hash` and
`content_message_id`; it does not contain fabricated `content_hash` or
`updated_at` columns. Source-only v4 control tables remain visible in the
integration contract even though their rows are not imported.

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
- `INSERT`, multi-row `VALUES`, `ON CONFLICT`, column-only `RETURNING`,
  `UPDATE`, and `DELETE`;
- `SELECT ... FOR SHARE` and `SELECT ... FOR UPDATE` in pessimistic concurrent
  transactions;
- `CREATE TABLE`, primary/unique/foreign-key constraints, `CREATE INDEX`, and
  append-only `ALTER TABLE ... ADD COLUMN` parsing.

Unsupported syntax MUST fail during parse or binding. It MUST NOT silently use
different semantics.

`INSERT ... ON CONFLICT (...) DO NOTHING RETURNING ...` returns only rows
inserted by that statement. A rejected candidate never contributes a returned
row. The typed transaction API reports `affected_rows` and `conflict_rows` for
the provisional statement outcome and recomputes the same result at commit.
Only the commit result is confirmed. Result row, payload, and affected-row
limits fail the statement before its transaction workspace is advanced.

## Concurrent locking

Ordinary `SELECT` is a snapshot read and acquires no logical row lock. An
explicit `FOR SHARE` or `FOR UPDATE` locking read is accepted only inside a
pessimistic `ConcurrentDatabaseTransaction`. `NOWAIT`, `SKIP LOCKED`, locking
virtual `system.*` tables, and locking reads in optimistic transactions fail
instead of silently degrading.

For a single public table, a predicate that is exactly representable by its
primary key becomes a shared or exclusive point/range request. An UPDATE or
DELETE uses the same primary-key inference with exclusive mode. A predicate
that cannot be represented safely falls back to a table lock, not a database
lock. Joins use deterministic table locks until a separately specified
multi-table row-lock derivation exists. DDL and mutations that rewrite primary,
unique, or foreign-key columns retain the database-exclusive fallback because
their complete constraint lock set is not yet derived.

Requests are deduplicated and acquired in deterministic namespace/key order.
The lock table escalates a transaction's narrow locks for one table before the
next request exceeds the per-table threshold. Escalation acquires the covering
table lock before removing covered locks. The lock table also enforces hard
entry and estimated-byte limits; an over-budget request aborts and releases the
transaction rather than growing unbounded resident state.

## Relational Storage

Relational tables are separate from graph node and relationship tables. A
caller MUST NOT encode these rows as synthetic graph nodes.

The scalar storage types are `BOOLEAN`, `BIGINT`, `DOUBLE PRECISION`, `TEXT`,
and `BYTEA`, including nullability and deterministic defaults. Primary keys,
unique constraints, ordered secondary indexes, and foreign keys are evaluated
against the final staged transaction state.

`BYTEA` binds to the shared `Value::Binary`/`ValueRef::Binary` logical value.
Query projection, comparison, hashing, spill, WAL, checkpoint, and JSON result
boundaries MUST preserve raw bytes without retyping them as text or integer
lists. JSON surfaces use the unambiguous `{ "$binary": "<lowercase-hex>" }`
envelope because JSON has no native byte-string scalar.

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
may scan the table once. Before the first checkpoint, ordinary mutations may
maintain materialized postings incrementally. After authoritative activation,
mutations MUST validate constraints against the pinned persistent base plus
recovery/live deltas and publish bounded index and row overlays; they MUST NOT
reconstruct database-sized posting maps. Foreign-key validation MUST inspect
changed child rows and MUST scan a child table only when a referenced visible
key is actually removed or changed.

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

Row-page and overflow generation manifests use the common durable-replace
boundary. A failed generation-manifest replace may leave already synchronized
immutable page, descriptor, key, or extent artifacts, but it MUST leave the
generation unopenable and MUST NOT change either the independent latest selector
or the outer canonical checkpoint. Reopen may discard those unbound artifacts;
it never infers authority from a filename. Platform tests obstruct the exact
row-page and overflow manifest destinations, including a Windows handle that
denies delete sharing, and require the source candidate to remain available for
diagnosis or cleanup.

Backup copies the physical closure of the selected canonical row/overflow roots,
not every retained logical generation. A reader pin suppresses generation
reclamation. After the pin is released, stale logical manifests may be removed,
while an older overflow extent remains whenever a current row still references
its content digest. Backup restore and ordinary reopen MUST hydrate the same
large value before and after that reclaim boundary.

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
Ordinary reopen verifies the outer relational-checkpoint identity with a bounded
streaming buffer before the file decoder runs; checkpoint size MUST NOT create a
second full-file validation buffer beside decoded relational state.
There is no second relational WAL, fsync stream, database handle, or production
sidecar API.

## Query Execution Boundary

The existing PostgreSQL `Database::query_sql*` entrypoints dispatch `system.*`
reads to the system-table executor and public-schema DDL, DML, and `SELECT` to
the relational state owned by `GraphStore`. Read transactions pin the same COW
relational snapshot as graph state. This exposes statements rather than
route-specific typed APIs.

Every production read statement MUST pass its declared row and payload limits
through `QueryStreamOptions`. Database-level limits remain hard upper bounds.
The payload limit constrains result and projected large-value hydration bytes;
it MUST NOT be reused as the index-page I/O budget. Index reads derive their
separate bounded I/O limit from the admitted segment cache and the engine's
index-read ceiling, so a small result does not make a valid persistent index
page unreadable.

The relational executor implements the frozen corpus semantics, including
joins, aggregation, ordering, distinct, budgets, and late hydration. Base scans,
primary-key lookups, index-prefix visits, joins, residual filters, projection,
offset, and limit run as a pull-through visitor pipeline. The pipeline checks
the runtime cancellation token after every admitted batch and never constructs
a complete intermediate `Vec` of qualified rows.

Before a borrowed full-table scan starts, its residual predicate and projection
MUST bind table-qualified column references to schema ordinals exactly once.
Parameters and literals MUST become typed relational values before the first
row callback. Per-row execution follows PostgreSQL three-valued logic and MUST
short-circuit `AND` after `FALSE` and `OR` after `TRUE`; an `UNKNOWN` left side
still evaluates the right side because `UNKNOWN AND FALSE` is `FALSE` and
`UNKNOWN OR TRUE` is `TRUE`. Projection aliases and wildcard expansion are also
bound once, while the compatibility `Row` conversion remains the final owned
output boundary.

Planning separates fields required to decode a scan from fields whose payloads
must be hydrated. An aggregate-only `OCTET_LENGTH(TEXT|BYTEA)` operand or
non-distinct `COUNT(column)` retains an overflow value as a compact reference;
length reads its validated `uncompressed_bytes`, while count only tests
nullability. Neither case may read, decompress, clone, or retain the payload.
The reference still has to match the pinned row schema and its source binding:
checkpoint and recovery values are validated against their immutable overflow
root, while live values are validated against the pinned canonical row. In all
cases the reference must belong to the reachable overflow closure. If the same
field also participates
in a predicate, join, grouping key, ordering key, raw projection, distinct
aggregate, or another value-sensitive expression, normal hydration remains
mandatory.

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

Relational access-path candidates normalize `estimated_rows` to a minimum of
one before Skyline dominance and final costing. `EXPLAIN` applies the same
floor after `LIMIT` and `OFFSET` arithmetic, including `LIMIT 0`. Empty tables,
missing point keys, and empty result sets still report zero measured `actRows`;
the estimate floor affects plan comparison and diagnostics only.

Qualified conjunctive equality joins derive right-side composite primary-key or
leading index-prefix access. Primary-key joins perform one point lookup per
left row; index-prefix joins visit posting rows without first collecting the
complete posting list. The selected path is reported separately for every
join. Predicates that cannot prove a safe right-side key fall back to a direct,
non-collecting table scan.

Relational aggregate groups retain incremental `COUNT`, `SUM`, `MAX`, and
`COALESCE` state instead of a second copy of every qualified binding. Ordinary
ordering and grouped aggregation send typed sort keys plus compact row locators
through the executor-owned external-order implementation. Statement `DISTINCT`
and `COUNT(DISTINCT ...)` continue to use the shared binding operators because
their projected public values are the comparison state. Both paths share the
query root ledger, operator memory limits, spill byte/run limits, cleanup rules,
and cancellation checkpoints. Grouped aggregation retains one group state at a
time; non-grouped aggregation retains one admitted incremental state. Large
payload hydration remains after the blocking locator selection unless exact
statement `DISTINCT` requires the projected value.

An ungrouped, non-distinct, single-table aggregate consisting only of
`COUNT(*)`, `COUNT(column)`, `SUM(BIGINT column)`, or
`SUM(OCTET_LENGTH(TEXT|BYTEA column))` MUST lower qualified rows into a typed
Bool/Int64 `ColumnarBatch`. A literal-only `COALESCE` around one of those
aggregates remains in the same path and applies its first non-null fallback only
after the aggregate finishes. Batch-local `count_selected`, `count_valid`, and
checked Int64 sum kernels merge into one admitted aggregate state. Length input
uses inline byte length or overflow-reference `uncompressed_bytes` without
payload hydration. The batch is conservatively sized before allocation against
both `batch_rows` and `batch_payload_bytes`, and its pipeline reservation shares
the query root ledger with the aggregate state. Null and overflow semantics MUST
match the row executor. Any expression outside this proven fragment uses the
row implementation for the entire aggregate; it MUST NOT switch paths after
consuming input, and the row path remains the differential oracle.

An ordered index projection whose equality prefix and `ORDER BY` suffix are
fully covered MUST stream compact row locators through an executor-owned typed
`ColumnarBatch`. `OFFSET` and `LIMIT` apply while visiting the ordered cursor,
before row hydration, and the visitor MUST stop after the requested locator is
accepted. Each locator batch is bounded by both `batch_rows` and
`batch_payload_bytes` under the query root ledger. TEXT, BYTEA, and other public
payload values enter neither the locator column nor an intermediate `Binding`;
only the selected locator batch may perform final projection hydration.

A canonical two-column keyset predicate MAY continue that ordered index path
from an exclusive cursor. The accepted shape is one leading equality prefix
followed by `(sort > cursor OR (sort = cursor AND id > cursor_id))` for uniform
ascending order, or the corresponding `<` predicate for uniform descending
order. The cursor values must complete the composite index key, and both order
columns must be non-null. Mixed directions, nullable order columns, incomplete
index suffixes, and non-canonical predicates MUST retain the blocking fallback.
Descending pages traverse the same ascending index in reverse; neither
direction may hydrate rows beyond the bounded locator page. Planned and runtime
explain evidence must distinguish exclusive seek, direction, and early stop.

Blocking relational rows MUST carry a typed `sort_keys + locator + stable
ordinal` record and MUST NOT materialize an executor `Binding`, public `Value`,
or `BTreeMap`. One immutable query-local locator layout owns the table,
qualifier, schema, and primary-key type metadata for the base binding and every
join binding; that metadata MUST NOT be cloned into each retained or spilled
row. A present binding contains its layout slot id and ordered primary-key
scalars, while an absent slot denotes only an optional-join miss. Replay decodes
each scalar against the pinned layout, rejects slot-identity, binding-count,
key-arity, and scalar-type drift, then performs late point hydration through the
statement's pinned row view. Primary-key values are non-null and protected from
overflow externalization, so either condition in a locator is storage
corruption and MUST fail closed. The symmetric typed codec, executor-owned
ordinal, spill staging reservation, and bounded merge are executor-local
details, not a durable storage format.

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

Content Store memory evidence separates policy qualification from workload
qualification. On an 8 GiB desktop limit, the default governor reserves 75% for
the host process and derives at most 2 GiB of Skein capacity; sensed available
headroom normally moves the budget through the 1--2 GiB range and may reduce it
further under pressure. The 512 MiB capability case is a separately configured
bounded run. It proves that the bounded workload can execute within an explicit
512 MiB Skein capacity ceiling; it does not require a 512 MiB host or process
limit. A smaller effective host or cgroup ceiling and sensed headroom remain
authoritative. Neither profile changes SQL semantics or becomes a table-specific
API.

## Migration Boundary

SQLite snapshotting and `rusqlite` remain in the Nowledge migration adapter.
Skein MUST NOT acquire SQLite as a production dependency. Migration uses stable
keyset pages, durable cursors, idempotent primary-key/content-hash replay, a
legacy write fence or durable obligation protocol, and differential validation
before cutover. The source SQLite database remains unchanged until a separately
authorized decommissioning step.

## Qualification

`run_content_store_initial_row_page_qualification` is the typed first-table
storage lifecycle gate. It creates a new evidence database from the frozen DDL,
inserts `content_documents`, `thread_messages`, `content_chunks`, and
`content_anchors` only through frozen PostgreSQL mutation statements, publishes
a checkpoint, and reopens with authoritative persistent indexes. It then
executes the frozen document, message, chunk, aggregate, and anchor reads with
each statement's exact row and payload limits.

The report binds the source revision, corpus and schema identities, qualified
tables, cache capacity, checkpoint generation/epoch, deterministic output
digests, output payload bytes, cache deltas, and parsed `EXPLAIN ANALYZE`
evidence. Success requires `runtime_path=authoritative` and
`row_runtime_path=snapshot_rows`. A post-checkpoint mutation must reappear from
a non-empty WAL recovery delta after reopen, and a later mutation must appear
from a non-empty live row overlay without a checkpoint. Page pins must return
to zero after every read. Cold and warm checkpoint reads must return identical
ordered results.

The same runner qualifies `upsert_source_chunks` as a whole-document mixed
transaction rather than a host-side graph/relational dual write. It replaces a
larger source with an exact shorter ordered set, rejects a duplicate
`(content_doc_id, chunk_index)` statement without changing the prior workspace,
updates the graph `Source.chunk_count` and relational document summary in the
same commit epoch, and proves the exact result after checkpoint/reopen. A
second empty replacement proves that clearing a failed or empty reparse leaves
no stale chunks and publishes zero graph/document counts. Both replacements
execute through the same frozen SQL statements used by the caller inventory;
the evidence also checks chunk identity, order, offsets, token counts,
non-ASCII heading metadata, and caller-supplied content hashes.

The runner then qualifies `patch_source_chunks_space` without introducing a
route-specific ownership API. It seeds an exact Source chunk set, executes the
graph `Source.space_id` update and frozen relational
`update_source_document_space` statement in one mixed transaction, and reads
the joined chunks before commit to prove read-your-own-writes. The published
view MUST expose graph and relational ownership at the same commit epoch while
preserving chunk count, ordering, text, token count, metadata, and content
hashes. A missing Source is an explicit no-op that MUST NOT advance the commit
epoch. The live row overlay and checkpoint/reopen result MUST have identical
ordered output and payload digests. `SkeinContentSourceOwnershipMove.tla`
models the durable publication boundary and the missing-owner no-op.

`patch_thread_space_ownership` is qualified as one bounded guarded batch. Each
input binds one thread storage identity, its previewed source workspace, and
its target workspace. The graph Thread, relational content document, and all
thread messages MUST move together only when their current workspace still
matches that guard. A batch may contain different source workspaces. A stale
preview MUST leave all three representations and their update timestamps
unchanged while other valid entries in the same batch commit. Apart from the
intentional workspace and update-time fields, document and message payloads
MUST retain the same digest through live visibility and checkpoint/reopen.
The graph Thread is addressed by its public Thread id, while the relational
document `owner_id` and message `thread_storage_id` are addressed by the
distinct storage id. Qualification fixtures MUST keep those identities
different and verify both mappings explicitly.
`SkeinContentThreadOwnershipMove.tla` models guarded per-owner staging and the
single durable batch publication.

`patch_moved_space_ownership` extends the same guard to one Space merge that
selects both Thread and Source owners. Every eligible graph owner, relational
document, thread-message set, and Source chunk view MUST publish at one commit
epoch. A selected Thread that no longer belongs to the previewed source space
remains unchanged while the eligible Threads and Source publish through the
live overlay. The typed evidence records expected space per case,
the exact document/message update counts, payload digests, and identical
checkpoint/reopen reads. `SkeinContentSpaceMergeOwnership.tla` proves the
cross-kind durable batch rather than inferring it from two independent moves.

`upsert_thread_messages` is qualified as one mixed transaction over the graph
Thread, relational content document, message occurrences, and document
summary. The graph Thread uses its public Thread id; document `owner_id` and
message lookup use the distinct thread storage id, while each message retains
both identities. Replaying document and message UPSERT statements MUST preserve
their original `created_at` fields while updating the mutable payload selected
by the conflict clause. A rejected missing-document message MUST leave every
previously accepted statement in the transaction workspace unchanged. The
summary count and payload bytes MUST match the final occurrence set before the
single commit becomes visible. Live row-overlay and checkpoint/reopen reads
MUST retain identical ordered output. `SkeinContentThreadUpsert.tla` models
complete durable publication, conflict-time creation identity, and rejected
statement atomicity.

`reconcile_thread_messages_preserving` first reads the bounded ordered
occurrence set and MUST reject a mapping that omits, duplicates, or invents an
existing `content_message_id`. Rejection occurs before the mutation transaction
and MUST NOT advance the commit epoch. A valid mapping may interleave new
occurrences with preserved occurrences. Existing message payload, creation
identity, and anchor payload MUST remain byte-identical while message and
anchor order changes.

Anchors carrying `content_message_id` follow that exact occurrence. A legacy
anchor with a null occurrence id follows only the matching `message_id` at its
previous order, preventing an anchor from moving to another duplicate message.
The graph Thread update, storage-owned document UPSERT, message and anchor
reordering, new message insertion, and exact document summary publish through
one mixed transaction. The v1 Skein schema has no historical unique-order
migration constraint, so it applies target orders directly rather than using
SQLite's temporary negative-order displacement. Live overlay and
checkpoint/reopen output, occurrence identity, and anchor state MUST match.
`SkeinContentThreadReconcile.tla` models mapping rejection, arbitrary staging,
anchor-following, immutable preserved payloads, and complete durable
publication.

`delete_thread_tail` clamps a negative start position to zero before issuing
the frozen bounded candidate query. Candidate rows MUST remain ordered by
`(order_index, content_message_id)` and MUST return both public `message_id` and
stable `content_message_id` so the host can report the exact removed
occurrences. A start beyond the current tail is an explicit no-op and MUST NOT
advance the commit epoch.

For a non-empty tail, the graph Thread count update, deletion of only
`anchor_kind = 'message'` anchors at or beyond the start, message occurrence
deletion, and exact document count/size summary MUST stage in one mixed
transaction. Rollback or a pre-durability crash MUST expose none of those
changes. Retained messages and anchors MUST preserve their full payload and
creation identity. The live view MUST expose row tombstones for deleted
occurrences at the same commit epoch as graph and summary state, and
checkpoint/reopen MUST preserve the same ordered retained rows and anchor
state. `SkeinContentThreadTailDelete.tla` models exact tail selection, empty
no-op behavior, arbitrary partial staging, retained payload identity, and
durable-before-visible publication.

`delete_thread_messages` first performs bounded reads of both documents owned
by the Thread storage identity and documents referenced by its message
occurrences. The exact sorted union determines anchor and empty-document
cleanup; an owned document with no messages and a legacy message-only document
MUST both be discovered. The graph Thread uses its public identity while
relational messages and documents use the distinct storage identity.

A missing or already-deleted Thread MUST terminate after a bounded read-only
preflight. It MUST NOT execute zero-match mutation statements, write WAL, or
advance the commit epoch. For a present Thread, graph Message nodes,
ThreadIdentity nodes, the Thread node, relational anchors, message occurrences,
and now-empty documents MUST stage in one transaction. Transaction workspace
reads MUST observe the complete deletion before commit; rollback and a
pre-durability crash expose the complete old state. Publication exposes the
complete deletion at one epoch, preserves unrelated graph and relational
payloads byte-for-byte, and leaves a live row tombstone until checkpoint.
Checkpoint/reopen MUST preserve the same absence and unrelated payload digest.
`SkeinContentThreadDelete.tla` models exact document discovery, arbitrary
cross-model staging, epoch-preserving preflight no-ops, rollback, atomic
durable publication, and recovery.

This gate deliberately covers the selected storage lifecycle, transaction,
locking, cancellation, injected corruption, and synthetic resource evidence;
it does not claim complete Content Store cutover. The frozen corpus has no
remaining `partial` callers, but isolated resource enforcement, production-copy
measurements, and cross-platform fault injection remain fail-closed. The runner
never embeds its database path or payload contents in the serialized report.

`run_production_content_store_storage_qualification` is the typed read-only
production-copy gate. It opens an already imported Skein replica with an
explicit `read_only`, `OutOfCore`, and `Authoritative` configuration. It never
opens SQLite, imports source rows, writes WAL, checkpoints, or mutates the
source replica. Every case names one frozen read statement and supplies its
parameters only to the query runtime; retained evidence contains statement and
parameter digests rather than SQL parameters, database paths, or result rows.

Each case opens a fresh database handle, binds the expected production identity
to the observed shared graph/relational commit epoch, and performs at least one
cold and one warm bounded read. The report records open latency and WAL replay
work separately from query latency, exact result digests, output and
intermediate rows, hydration bytes, physical row/index I/O, cache deltas, RSS,
and platform-supported page faults. Result rows, latency, physical I/O,
hydration, and cache deltas MUST come from the same profiled SQL execution, not
from a subsequent `EXPLAIN ANALYZE` after the cache has been warmed. Each read
obtains one foreground query
permit from the caller-declared runtime governor, including its bounded result,
working-memory, blocking, and I/O demand. The report retains the derived
capacity and current dynamic memory budget plus admission/completion deltas;
missing admission, rejection, overcommit, or a leaked permit blocks readiness.
The selected relational row and index
views MUST both serve at the database epoch, share their base generation, base
epoch, and visible epoch, and report checkpoint, recovery-delta, and live
overlay bytes independently. A read-only `OutOfCore` plus `Authoritative`
handle MUST report that materialized checkpoint rows are absent while retaining
the exact logical row count. With an empty post-checkpoint WAL it MUST also
report a metadata-only checkpoint state, proving that cold open did not
construct the row oracle. System-schema validation plus ordinary SQL must use
the same canonical row snapshot rather than a direct state scan. Both
canonical row and canonical index artifacts
MUST exceed the configured cache while an individual admitted read wave still
fits: cache admission rejection or a leaked pin blocks readiness.
Per-run physical page and byte limits cover the sum of relational row and index
reads; an index access MUST NOT disappear from the budget merely because row
hydration is reported separately.

Page-fault budgets whose names end in `per_run` apply independently to each
read. The lifecycle profile records cumulative open/read faults for diagnosis,
but MUST NOT compare that aggregate with a per-run limit.

`DesktopBound8Gib`, `Capability512Mib`, and `ConfiguredWorkload` remain
different evidence profiles. The desktop profile requires the observed 8 GiB
effective limit and uses the dynamic governor budget capped at 2 GiB. The 512
MiB profile requires an explicit 512 MiB Skein governor ceiling, honors any
smaller detected host or cgroup ceiling and available headroom, and proves that
peak RSS remains inside the declared 512 MiB envelope. It does not require the
host itself to be limited to 512 MiB, and it is not the default or a universal
activation cutoff. A configured production workload records and enforces its
actual caller-declared envelope. Contract tests prove the runner and redaction
rules, but only a report from the representative imported copy is production
evidence.

`run_production_content_store_mutation_qualification` is the separate writable
production-copy gate. The caller MUST provide one read-only source directory
and four distinct disposable Skein replicas for exactly 1, 4, 8, and 10
writers. The runner canonicalizes every path, opens the source only with
`read_only`, and rejects any replica that aliases the source or another case.
It never copies, checkpoints, or mutates the source. Every worker owns one
explicit conflict domain and executes the same non-empty sequence containing
both a frozen `INSERT` and a frozen `UPDATE`; SQL lowering, parameter arity,
and corpus ownership are checked before any replica is opened for writes.

Each mutation uses a pessimistic transaction and a separate durable commit.
The report retains statement and parameter digests, statement and commit
latency, commit-epoch progression, WAL/group-commit counters, cache/pin/live
row/live index bytes, RSS, and supported page faults. It MUST use an
evidence-validated group-commit configuration, but coalescing on a particular
workload run is diagnostic rather than a readiness invariant: arrival timing
is workload-dependent and the paired group-commit benchmark remains the
activation gate. Commit p95 is compared with a caller-supplied same-shape
accepted-revision reference bound to the same configuration digest and dataset
fingerprint, with a maximum regression budget of 5%, and also with an absolute
case budget.

After writes, the runner drops all handles and reopens the dirty replica before
checkpoint. This open MUST replay non-empty WAL into current row and index
recovery views, preserve every verification digest, and report its total open
latency, engine-measured manifest, checkpoint/root, WAL-replay and post-replay
intervals, and replayed entries and bytes. It then checkpoints, reopens again, and
requires a manifest-selected current view with no WAL replay and no remaining
live or recovery delta. These two open shapes are retained independently; the
runner uses the engine's monotonic phase instrumentation and does not subtract
the two wall-clock durations to infer WAL replay time. The phase sum must fit
inside both the engine total and the caller's enclosing measurement, and the
release evaluator recomputes that invariant from raw evidence.

The writer matrix uses the same resource-profile meanings as the read-only
gate. `Capability512Mib` proves that an explicitly configured Skein workload can
complete inside a 512 MiB envelope; it is neither the default capacity nor a
universal release cutoff. `DesktopBound8Gib` continues to mean an 8 GiB host
whose automatic Skein capacity is dynamically bounded at 2 GiB. Every initial
source and replica row/index artifact MUST exceed the cache, so passing the
matrix cannot depend on full database residency. Synthetic matrix tests prove
the isolation, replay, checkpoint, digest, and evidence contracts only; release
readiness still requires retained reports from disposable copies of the
representative production import.

The final `evaluate_production_release_qualification_bundle` gate requires the
identity-bound two-policy memory matrix, a read-only production-copy report, a
separate read-only report with the explicit 512 MiB ceiling, and the four-case
mutation-replica report. The 8 GiB policy caps automatic Skein capacity at
2 GiB and derives the effective budget from current headroom; 1--2 GiB is the
nominal operating range, not a reservation or lower bound. The 512 MiB report
proves a separate supported capability and cannot stand in for the production
profile. The reports retain their declared resource and statement contracts so
the gate can independently recompute every cold/warm I/O bound, runtime permit,
writer sequence, percentile, epoch, WAL, recovery-delta, checkpoint-fold, and
verification-digest obligation. It resolves statement digests and row/payload
limits against the frozen corpus instead of trusting report-provided SQL
metadata. `ready=true` and an empty child blocker list are descriptive only;
contradictory raw evidence fails the final bundle.

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
- cross-platform row/overflow generation-manifest replace failure, pinned-reader
  retention, physical-closure reclaim, backup/restore, and reopen;
- streaming scan/join/filter/projection, runtime cancellation, shared
  sort/distinct/group spill, and SQL `EXPLAIN`/`EXPLAIN ANALYZE` reports.

`cargo bench --bench relational_overflow` reports inline and overflow write and
hydration P50/P95/P99 for repetitive, varied, and high-entropy payloads from
1 KiB through 64 KiB. The benchmark is micro-level codec evidence only. It MUST
NOT be used as file-backed RSS, page-fault, or end-to-end query evidence.

`cargo bench --bench relational_index_access` compares a full scan with the
selected composite-prefix path at 1,000, 10,000, 50,000, and 100,000 rows in
one target equality prefix, plus an equally sized distractor prefix. It reports
P50/P95/P99 latency and visited-row counts for bounded 25-row forward and
backward pages at first, middle, and deep exclusive cursors. The output includes
target, total, matched, and one-column-prefix cardinalities, selected index,
selected equality-prefix length, speedup, and process RSS delta. This is an
in-memory access-path microbenchmark; it does not qualify file-backed cache,
page-fault, concurrent writer, or end-to-end SQL behavior.

Production qualification still requires the differential SQLite oracle,
50,000-message and large-source workloads, shadow traffic, and retained green
Windows, Linux, and macOS evidence. The deterministic fault-injection tests are
implementation evidence; they do not replace revision-bound platform results.
