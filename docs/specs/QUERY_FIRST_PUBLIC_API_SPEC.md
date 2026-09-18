# Query-First Public API Specification

## Status

This specification defines the release-facing embedded API boundary. It is
normative for production builds.

## Public query boundary

Application graph behavior MUST be expressed as parameterized Cypher and
executed through `Database`, `DatabaseReadTransaction`,
`DatabaseTransaction`, `NowledgeGraphAdapter`, or the bounded
`NowledgeMemGraph` query surface. Relational behavior MUST be expressed as
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

## Removed business facades

Release builds MUST NOT expose application-specific entity, relationship,
Memory, Source, Skill, Thread, Label, Community, or AugmentationJob CRUD batch
methods. Release builds also MUST NOT expose `read_graph_*` route methods or
their route response DTOs.

The old adapters and route response DTOs MUST NOT remain behind `cfg(test)`.
Regression coverage MUST execute parameterized queries through the ordinary
runtime. Route-evidence builders MAY retain private query fixtures, but neither
their statements nor route-specific response shapes are public database APIs.

## Stable typed boundaries

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
