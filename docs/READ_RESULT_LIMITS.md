# Read result limits

Issue Number: ref #1046, ref #987, ref #976, ref #1679

## Session policy

`SessionConfig::max_result_rows` is an unsigned 64-bit session setting. Its
default is **0**, meaning **no result row-count limit**. Positive values impose
a ceiling on each statement's returned rows, rather than on the number of rows
an aggregate may scan. Independent Cypher and SQL sessions start at 0. Settings
are held in the session and captured by its transactions; they never mutate the
database configuration or another session. Changing a Cypher session while a
transaction is active affects subsequent transactions, not the active one.

```rust
use hawdb::Database;

let mut database = Database::new();
let mut session = database.session();
assert_eq!(session.config().max_result_rows, 0);
session.set_max_result_rows(100_000);
// Execute bounded Cypher or SQL through this session.
session.set_max_result_rows(0); // no session row-count ceiling

let mut sql = database.sql_session();
sql.set_max_result_rows(100_000);
// sql.query(...), including callback transactions, inherits this setting.
```

This is a typed embedded Rust setting; SQL `SET max_result_rows` syntax and
foreign session bindings are not introduced by this change. Its name, default
and zero convention follow [ClickHouse's result-row setting](https://clickhouse.com/docs/reference/settings/session-settings/max-result#max_result_rows).
HawDB enforces positive ceilings strictly and reports errors; it does not adopt
ClickHouse's allowance for a final block to exceed the threshold. SQL/Cypher
`LIMIT 0` still returns zero rows. Maximum representable positive ceilings do
not overflow the execution detector when it computes an extra detection row. Existing explicit per-request `Option` budgets
retain their strict meaning: `Some(0)` admits zero rows, while `None` adds no
request ceiling. These low-level budgets differ from the session setting.

On a 32-bit host, including wasm32, session values at or above `usize::MAX`
(such as 5,000,000,000) impose no additional row-count bound: an in-process
result cannot represent more rows than platform capacity. Positive host
ceilings and finite payload/memory budgets still apply.

## Zero-valued limits

| Boundary | Value | Meaning |
| --- | --- | --- |
| SessionConfig::max_result_rows |0| No session row-count ceiling |
| DatabaseConfig::max_read_result_rows |Some(0) or None| No host row-count ceiling |
| DatabaseConfig::max_read_intermediate_rows |None| Inherit the effective `max_read_result_rows` ceiling |
| DatabaseConfig::max_read_intermediate_rows |Some(0)| No relational intermediate-row ceiling |
| QueryStreamOptions/ExecutionRequest explicit max rows |Some(0)| Admit zero rows; a nonempty result fails before consumer delivery |
| Cypher/SQL LIMIT |0| Produce an empty result |

## Host protection and memory

The existing `DatabaseConfig::max_read_result_rows` remains an optional host
ceiling. Its default is `None`; both `Some(0)` and `None` mean unlimited.
The exported `DEFAULT_MAX_READ_RESULT_ROWS` is an `Option<usize>` with value
`None`, so it cannot be mistaken for a numeric zero-row request budget.
This changes the public constant type from usize to Option<usize>; Rust hosts
that used it as a numeric fallback must update that code when adopting this
development revision.
A positive host ceiling cannot be raised or disabled by a session or cursor.
Session mutation `RETURNING` rows and explicit `COMMIT` summaries apply the same
host/session minimum as reads, before publishing staged writes. Autocommit,
explicit Cypher transactions and SQL sessions enforce this consistently, even
when the session ceiling is zero or greater than the host ceiling. Direct
database mutation APIs retain their separate `MutationLimits` contract.
The SQL frontend does not substitute the former 100,000-row default when the
host is unlimited. Explicit host ceilings continue to bound SQL intermediate
work; a session's result ceiling does not lower that intermediate-work budget.

## Relational intermediate rows

`DatabaseConfig::max_read_intermediate_rows` bounds the rows one relational SQL
statement may scan, join or aggregate, separately from the rows it returns
(#1679). The statement's scan-page, row-read, overlay-entry and logical
index-read budgets derive from it, as they previously derived from
`max_read_result_rows`.

| Setting | Intermediate-row ceiling |
| --- | --- |
| `None` (default, `DEFAULT_MAX_READ_INTERMEDIATE_ROWS`) | The effective `max_read_result_rows` ceiling, exactly as before |
| `Some(0)` | None; byte, page-size and query-memory budgets still apply |
| `Some(n)` | `n`, whether it is above or below the result ceiling |

`DatabaseConfig::read_intermediate_row_limit()` returns the effective value.
`max_read_result_rows`, session settings and request limits still bound
returned rows; they never raise or lower an explicit intermediate ceiling.
A host can therefore keep a 100,025-row result ceiling and set, for example,
`max_read_intermediate_rows = Some(10_000_000)` so verification can run
`SELECT COUNT(*), SUM(OCTET_LENGTH(content)) FROM t` over a larger table, while
`SELECT id FROM t` over the same table is still refused. The C ABI accepts the
same `max_read_intermediate_rows` open option.

A whole-table `COUNT`/`SUM` without `GROUP BY` accumulates while it scans: its
tracked query memory does not grow with the number of input rows, so a larger
intermediate ceiling admits more scan work and time, not more query memory.
Grouped aggregates retain one state per group, bounded by the same ceiling and
by the blocking-operator memory budget; `ORDER BY` and `DISTINCT` keep their own
memory budgets and spill rules.

`PreparedRead` statements keep their `PreparedReadLimits::statement_rows` bound
on both ceilings. An unset intermediate ceiling inherits the lowered result
ceiling exactly as before; an explicit one becomes the smaller of its effective
value and `statement_rows`. Cypher reads have no relational intermediate-row
budget; `max_read_result_rows` only bounds their returned rows, so a graph
`count(n)` over more nodes than the result ceiling already succeeds.

`max_read_result_payload_bytes` remains 64 MiB by default. The default embedded
runtime governor independently allows 16 MiB of retained/result capacity;
physical query memory and plan-derived admitted reservations remain enforced.
A result may therefore refuse a byte budget despite having no row-count limit.
Native SQL `RETURNING` validates the smaller of the configured payload ceiling
and the caller's admitted result capacity before advancing staged state. Zero
bytes admits only empty output. This includes single-statement SQL sessions,
direct transactions and callbacks that catch a statement refusal. INSERT
`RETURNING` is supported; UPDATE/DELETE `RETURNING` retain their existing
unsupported-clause errors. Graph mutation summaries keep the separate
`MutationLimits::max_result_payload_bytes` contract, checked before publication;
the read-output byte setting does not replace that mutation limit.
Python `hawdb.open()` inherits these database defaults. Rust hosts can use the
existing configured constructors and typed runtime-governor configuration.

Raising query-memory bytes alone does not enlarge a plan's admitted reservation.
Record effective admission, execution batch/operator allowances and separately
charged retained source capacity when qualifying large results. Historical
100,000-row default-refusal evidence describes its original source only; it is
not evidence for this changed default. Configured large-result measurements
remain separate from default-budget measurements.

## Completion and ownership

Owned reads validate output before successful return. An over-limit query
errors rather than publishing a truncated `QueryOutput`. Counts and aggregates
must compute complete values or explicitly refuse bounded work. Session-limited
SQL `RETURNING` is checked before publishing the staged mutation; a failed
statement/callback does not commit its partial writes.

The execution-request regressions cover session isolation, reset and captured
transaction settings; positive host ceilings across Cypher mutations, COMMIT
and SQL RETURNING; SQL LIMIT 0; complete aggregates; and SQL output above the
former default. Retained and foreign delivery qualification remains separate from ordinary policy checks.

Retained cursors opened through `DatabaseSession::query_with_params_retained`
and Arrow streams derived from them capture the session ceiling. They may emit
valid prefix batches before a terminal row/payload refusal. Consumers must
treat that error as incomplete execution, never successful EOF. Failures remain
sticky and release the source pin. Arrow reports a nonzero stream error with
diagnostic text. Batch size is not a total-row limit. Dropping all cursor and
batch owners releases their charges. Retained delivery inside an explicit
Cypher transaction remains unsupported and refuses explicitly.

Registered execution-request consumer tests cover default and explicit host
limits across 99,999/100,000/100,001 rows on memory/file databases; complete
owned/retained/Arrow values; sticky refusals; released ownership; unchanged read
epochs; query LIMIT; complete counts; subsequent writes; session isolation,
reset and transaction inheritance; SQL mutation rollback; and SQL output above
the former default, including ordered blocking projection. Targeted unit tests
also preserve independent file-I/O/payload budgets and final EXPLAIN admission.
