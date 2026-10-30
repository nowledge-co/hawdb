# Synchronous SQL materialized views

HawDB maintains SQL materialized views in the same transaction as their base
RowPage table. A successful commit makes both visible to subsequent reads.
There is no refresh job, asynchronous lag, or `REFRESH` command. Explicit
transactions can read their own staged view changes; rollback discards them.
Existing read transactions retain their ordinary database snapshot. Views do
not introduce a separate history store or a time-travel API.

```sql
CREATE TABLE events (
    id BIGINT PRIMARY KEY,
    category TEXT,
    amount BIGINT,
    active BOOLEAN NOT NULL
);

CREATE MATERIALIZED VIEW category_totals AS
SELECT category, COUNT(*) AS event_count, SUM(amount) AS total_amount
FROM events
WHERE active = true
GROUP BY category;

INSERT INTO events (id, category, amount, active)
VALUES (1, 'work', 12, true);

SELECT * FROM category_totals ORDER BY category;

DROP MATERIALIZED VIEW category_totals;
```

Use `Database::query_sql` or `ConcurrentDatabase::query_sql`. DDL also works
inside explicit SQL transactions. `CREATE MATERIALIZED VIEW IF NOT EXISTS`
accepts an existing materialized view; it does not replace its definition or
silently accept a base table with the same name. `DROP MATERIALIZED VIEW IF
EXISTS` accepts an absent relation, but cannot drop a base table.

## Supported definitions

- One resident RowPage base table, with an optional alias.
- Column projection, aliases, and `*`. Projection preserves duplicate rows even
  when the base primary key is omitted. Wildcards bind at creation time.
- `WHERE` using column-to-literal comparisons, `IS NULL`, `IN`, `NOT IN`, and
  Boolean combinations supported by the relational mutation predicate binder.
- `GROUP BY` columns, including nullable keys and keys omitted from the output.
- `COUNT(*)`, `COUNT(column)`, and `SUM(BIGINT column)`, with or without grouping.
  `SUM` ignores NULL, returns NULL for no non-NULL input, and rejects BIGINT
  overflow before commit. Empty ungrouped aggregates retain one result row;
  empty groups disappear.
- `MIN` and `MAX` of BIGINT or TEXT columns, ignoring NULL and returning NULL
  when no non-NULL input remains. A non-unique source index must start with the
  `GROUP BY` columns in their declared order (an ungrouped aggregate can use any
  non-unique source index). Unique indexes omit NULL keys and are not eligible.
- INSERT, UPSERT, UPDATE (including primary-key or group changes), DELETE, and
  foreign-key cascade changes to the base table.

Definitions reject parameters, arbitrary expressions/functions, joins,
DISTINCT, HAVING, ORDER BY, LIMIT/OFFSET, windows, locking clauses, non-BIGINT
SUM, and aggregates other than COUNT/SUM/MIN/MAX. View-on-view definitions and strict append sources
are not supported. Cypher definitions are deferred. Result queries can use the
normal supported SELECT operations and must request their own ordering.

Views reject direct DML and ALTER TABLE, including through typed relational
writes. `CREATE INDEX` and `CREATE UNIQUE INDEX` are supported on visible output
columns. Index creation preserves existing view state and does not backfill the
source again. Unique view indexes can reject source transactions that would
produce duplicate indexed results; rejection leaves both source and view intact. Foreign keys cannot target
a materialized view. `information_schema.tables` reports `MATERIALIZED VIEW`
and `is_insertable_into = 'NO'`; internal identity and aggregate-state columns
are excluded from SQL wildcards and `information_schema.columns`, and cannot
be explicitly selected.

## Maintenance and admission

Creation synchronously backfills the base table under the transaction budget.
After creation, COUNT/SUM/projection maintenance visits changed primary keys
and their old/new row images. Updates retract old contributions
and add new contributions. Hidden source identities preserve projection bag
semantics; encoded group keys preserve NULL grouping without nullable primary
keys. SUM maintains a separate non-NULL count. Aggregate deltas accumulate in
i128 and are checked when publishing BIGINT results, avoiding false overflow
while replacing extreme values.

MIN/MAX use bounded indexed recomputation for each affected group, including
inserts. The committed source-index prefix is streamed without a posting-list
copy; changed identities are excluded and merged with their new values from
the transaction. Filters are applied to both paths. Unaffected groups are not
scanned. An ungrouped MIN/MAX necessarily visits the whole source index. This
trades per-group read work for no separately persisted ordered/refcount state;
large or skewed groups can exhaust admission even for a one-row source update.

Maintenance shares one row/payload work budget across all affected views,
derived from `RelationalMutationLimits` (the SQL facade uses the default
250,000 row work units and the unchanged 64 MiB estimated payload limit).
This admits the motivating 60,000-row projection backfill's 120,000 work units
without widening every embedded host's foreground payload allowance. These defaults
also govern ordinary relational mutations and schema rewrites using the same
type. The SQL facade and embedded profiles currently use these defaults;
`DatabaseConfig` has no relational-work override. Explicit storage-kernel and
staging limits remain effective for their callers, but do not provide a
host-facing SQL option. The foreground maintenance path does not reserve this
logical payload against the process memory governor. The payload
counter is cumulative logical work, not live allocation or process RSS; larger
defaults admit more synchronous CPU/memory work and can increase commit latency.
Independent WAL record/value bounds, capture limits and read-result limits
remain effective and can refuse work below these ceilings.
The WAL decoder retains a separate 1,000,000-row allowance, and compatibility
tests require it to cover foreground defaults without shrinking when foreground
policy is tightened. Older binaries with the former 100,000-row ceiling may
refuse newly admitted larger row batches. A downgrade can also refuse replay of a small view-DDL WAL
record whose backfill exceeds the older maintenance-work allowance. The byte
encoding and codec version are unchanged; successful writing here does not
guarantee that an older binary can reopen the database.
Reads, output changes, hydration, and aggregate working state are bounded.
Exceeding admission or aggregate limits
rejects the whole transaction before WAL append; results are never truncated.
More views increase write latency and can cause a previously admitted base
batch to exceed maintenance limits. Large initial backfills must fit admission;
background/chunked creation is not implemented.

For creation, N source rows and M output rows cost N + M row units. An
unfiltered projection therefore needs 2N, an ungrouped aggregate needs N + 1
(including an empty source), and a grouped aggregate needs N + the number of
qualifying groups. A filtered projection needs N + the number of matches.
Known lower bounds are checked against the remaining shared budget before
collecting backfill keys; filter/group output counts are charged as discovered.
This preflight does not predict payload admission, which separately includes
key copies, hydrated rows, aggregate state, and output copies. Incremental
MIN/MAX additionally charges every visited prefix entry, including changed
identities skipped during recomputation. Budget exhaustion never publishes a
partial backfill or source mutation.

WAL recovery does not reapply the current foreground row/payload work policy
to already committed transactions, including their synchronous view maintenance.
Decode/integrity bounds and recovery allocation reservations still apply; a
recovery failure retains durable evidence rather than publishing partial state.
Physical compaction and reclamation use their own background resource controls,
not `RelationalMutationLimits`. This separation does not exempt view creation
or maintenance performed as part of a new foreground write.

TiDB similarly separates cleanup work from a user's SQL session: its
[GC worker](https://github.com/pingcap/tidb/blob/e0cfb27df3212f1112bddba141aee3d6f743b3fb/pkg/store/gcworker/gc_worker.go#L251-L264)
uses an internal session allowed to write when disk space is almost full.
TiKV retains independent [GC batch and I/O controls](https://docs.pingcap.com/tidb/stable/tikv-configuration-file/#gc)
and [reserved recovery disk space](https://docs.pingcap.com/tidb/stable/tikv-configuration-file/#reserve-space).
These are design references, not equivalent HawDB memory or disk-reservation
guarantees; HawDB's cumulative payload is not TiDB's live session memory quota.

The current implementation requires materialized relational constraints and
resident source rows. `RelationalIndexMode::Authoritative` is rejected for
databases with materialized views, including reopen and out-of-core activation.
Shadow and demand-paged index modes retain the resident maintenance path.
Transactions that modify a source with dependent views conservatively acquire
a database-wide version footprint for optimistic conflict validation.

## Persistence and recovery

The typed view definition belongs to `RelationalTableSchema`. Materialized rows
and aggregate state use existing relational row pages, overflow storage, and
checkpoint publication. WAL replay deterministically reapplies the same
transaction and derives its view changes. There is no separately committed
view file or acknowledgement. Existing durability policies and complete-WAL
transaction recovery boundaries remain in force.

The relational WAL/checkpoint codec is now version 2, and embedded relational
schema encodings include the view definition. The append WAL codec is version 3
because append schemas embed that shared relational encoding. Earlier development databases
must be recreated; no migration for the earlier development format is supplied.
This follows HawDB's greenfield policy and does not change or migrate legacy or
Cloud data stores. MIN/MAX add expression tags to the existing view-definition
encoding; current readers still read COUNT/SUM definitions. Older readers cannot
open a definition containing the new expression tags.

The regression suite is `api::tests::materialized_views`, discovered by the
existing Cargo library and Bazel root unit-test targets. It covers full-SELECT
differential checks, duplicate projections, NULL and extreme arithmetic,
transaction rollback, bounded incremental work, concurrent publication,
overflow payloads, WAL/checkpoint reopen, and fail-closed torn WAL recovery.
Torn-tail/reopen tests are not a new power-loss qualification: machine-level
durability continues to rely on the storage synchronization assumptions and
qualification described in `AGENTS.md`.

## Thread summaries for Mem

Mem's `integrations/hawdb/schema/content_store_v1.sql` already defines the source
index `idx_thread_messages_space (space_id, thread_storage_id)`. With that schema:

```sql
CREATE MATERIALIZED VIEW thread_activity AS
SELECT space_id, thread_storage_id,
       COUNT(*) AS message_count,
       COUNT(timestamp) AS timestamp_count,
       MIN(timestamp) AS first_message_at,
       MAX(timestamp) AS last_message_at
FROM thread_messages
GROUP BY space_id, thread_storage_id;

CREATE INDEX activity_space_thread
ON thread_activity (space_id, thread_storage_id);

SELECT * FROM thread_activity
WHERE space_id = $1 AND thread_storage_id = $2;
```

Use parameterized embedded queries with explicit row and payload budgets.
These are local content summaries, not complete source-provided activity ranges.
An empty Thread has no grouped row. Missing timestamps remain NULL, duplicate
extrema survive removal of only one contributing message, and changes to a
Space/Thread key retract the old group before publishing the new group.

The Mem fixture is sourced from revision
`0b8fb240e6847c642f3bace52e52d54be5d414bc`. Its canonical `timestamp` column is TEXT.
SQL TEXT extrema compare lexically, so chronological use requires normalized
UTC timestamps with uniform precision and representation, or normalized BIGINT
epoch values in a host-approved schema. Mem's current `thread_activity_stats`
parses timestamps and uses update/create times only when no native timestamps
parse successfully. COUNT(timestamp) counts non-NULL strings, not valid parsed
timestamps; the example does not implement that legacy fallback. Source ranges
and import-completeness provenance must remain independent of local extrema.
No Mem caller or canonical schema is changed here. Adoption still needs a time
normalization contract and full verification against the authoritative stores.
Memory/Entity graph counts and Cypher view definitions remain outside this SQL
slice, as described in [the broader proposal](specs/MATERIALIZED_VIEWS_SPEC.md).

## RisingWave reference

The design borrows continuous incremental materialization and explicit
publication boundaries from RisingWave's
[materialize executor](https://github.com/risingwavelabs/risingwave/blob/main/src/stream/src/executor/mview/materialize.rs).
RisingWave also performs an initial
[backfill](https://docs.risingwave.com/sql/commands/sql-create-mv), but its default
DML visibility does not guarantee immediate read-after-write without
[implicit flush](https://docs.risingwave.com/sql/commands/sql-set-rw-implicit-flush).
HawDB's embedded contract synchronously maintains views before acknowledging
the base transaction instead of introducing a distributed epoch/barrier runtime.
