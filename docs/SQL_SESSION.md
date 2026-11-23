# In-process SQL sessions

`Database::sql_session()` borrows an embedded database for parameterized
PostgreSQL-dialect SQL. It uses the existing parser, parameter binder, native
transaction workspace, configured read budgets and durable commit path.

```rust
use hawdb::{Database, Value};

fn write_pages(database: &mut Database) -> hawdb::Result<()> {
    let mut session = database.sql_session();
    session.query("CREATE TABLE pages (id BIGINT PRIMARY KEY, body TEXT)", &[])?;
    session.transaction(|transaction| {
        let result = transaction.query(
            "INSERT INTO pages (id, body) VALUES ($1, $2), ($3, $4) \
             ON CONFLICT (id) DO UPDATE SET body = EXCLUDED.body",
            &[Value::Int(1), Value::String("first".into()),
              Value::Int(2), Value::String("second".into())],
        )?;
        assert_eq!(result.mutation.as_ref().unwrap().affected_rows, 2);
        assert!(result.output.rows.is_empty());
        transaction.query("UPDATE pages SET body = $1 WHERE id = $2",
            &[Value::String("updated".into()), Value::Int(1)])?;
        Ok(())
    })
}
```

`query(sql, params)` accepts exactly one statement. Positional parameters are
passed unchanged as `$1`, `$2`, etc. It commits before returning. The existing
`SqlStatementResult` exposes rows in `output.rows` and RowPage DML counts in
`mutation.affected_rows` / `mutation.conflict_rows`; these mutation outcomes
have `provisional = false` after an independent query commits. Read/schema and
Strict Append statements preserve their existing native envelope and do not
manufacture RowPage DML counts.

`transaction(callback)` runs the callback's queries on one
`DatabaseTransaction`, including read-your-own-writes. Results inside the
callback are provisional until the enclosing method succeeds. Returning a
result from the callback does not rewrite its provisional provenance flag.
The first statement error immediately rolls back all prior staged writes and
aborts that query scope. Catching the error cannot make the enclosing
transaction commit; subsequent queries and the final transaction result return
the first error. A callback error or panic also drops the private workspace.
The surrounding session/database remains usable after an ordinary abort.

Commit failures retain native durability semantics: a failure after publication
or a lost response may leave a complete committed operation. Do not infer
rollback from a missing successful response. No WAL format or durability default
changes here; persistent commits keep the configured native synchronization
policy.

`SqlSession::new(database).with_task_context(&context)` forwards the existing
cooperative cancellation, deadline and resource-admission context into each
native transaction and checks it again before handing off to commit.
Query checks remain cooperative; the session adds no interruptible I/O or
whole-process memory guarantee. A read-only database uses one native pinned read
transaction for each query or callback, preserving its epoch and refusing
mutations. `DatabaseConfig` row/payload and execution limits remain enforced by
the same kernel paths.

This session accepts HawDB's existing SQL subset. Unsupported `CREATE EXTENSION`,
PL/pgSQL, triggers, `tsvector`/`to_tsvector`, `jsonb` operators/casts,
`pg_trgm`, `now()` and `::timestamptz` retain existing Parse/Semantic errors.
A GBrain-shaped upsert with `jsonb` and `now()` therefore aborts its transaction.
Column-only `RETURNING` works for `INSERT`, `UPDATE`, `DELETE`, and
`INSERT ... ON CONFLICT DO UPDATE`. Top-level data-modifying CTEs may use those
returned rows in a containing query. Their writes share one transaction/WAL
boundary; the containing query reads the pre-statement table snapshot and the
CTE's returned rows, and any projection, constraint, or output-budget failure
publishes nothing. Each data-modifying CTE requires `RETURNING`, and one
statement cannot target the same table from more than one data-modifying CTE.
Nested data-modifying CTEs remain unsupported.
SQL is never stripped, rewritten or silently skipped. The session does not
execute GBrain's Postgres schema unchanged, start a TCP listener, embed PGLite,
or provide a Node addon.

`VECTOR(n)` is accepted as a relational column schema by `CREATE TABLE` and
`ALTER TABLE ADD COLUMN`. Typed vector parameters support non-NULL writes,
with exact `cosine_distance` and `l2_distance` projections shared with Cypher.
Filter ordinary columns, project distance with an alias, then `ORDER BY` that
alias and `LIMIT` to select the nearest eligible rows. Vector ANN indexes are
not added. See [the vector contract](specs/VECTOR_COLUMN_SCHEMA_SPEC.md) for
encoding, recovery, NULL behavior and unsupported-operation boundaries.
