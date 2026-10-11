# HawDB pure-Go binding

The ordinary API returns owned rows through JSON. `LoadLibrary` loads the
native library using purego, with `CGO_ENABLED=0`; loaded libraries remain mapped
for the process lifetime. `DB.Close` releases the database independently.

## Experimental retained numeric views

`DB.QueryRetained` uses the versioned native numeric ABI directly on 64-bit
hosts. It supports the root cursor's catalog-proven one-label integer/float
comparison, same-property projections and unsigned `id(n)`, with optional
SKIP/LIMIT. Other plans, writable requests and source-reuse requests refuse
explicitly. Older libraries keep their owned API and report an unavailable
retained capability. No JSON result, base64, IPC or `[][]any` is used by this
path; parameter JSON is still input.

```go
cursor, err := db.QueryRetained(
    "MATCH (n:Item) WHERE n.score >= $min RETURN n.score AS score",
    map[string]any{"min": int64(0)}, hawdb.RetainedOptions{},
)
if err != nil {
    return err
}
defer cursor.Close()
for {
    batch, err := cursor.Next()
    if errors.Is(err, io.EOF) {
        break
    }
    if err != nil {
        return err
    }
    rows, err := batch.Rows()
    if err != nil {
        batch.Close()
        return err
    }
    for row := uint64(0); row < rows; row++ {
        value, valid, err := batch.Int64At(0, row)
        if err != nil {
            batch.Close()
            return err
        }
        consume(value, valid)
    }
    if err := batch.Close(); err != nil {
        return err
    }
}
```

The example uses `errors` and `io`, and assumes the `Item.score` property is
declared as INT. Pulls are serial, with no prefetch, two default payload slots,
1,024 inspected records and 1 MiB per batch. Root database and shared runtime
limits apply even when requested batch or slot limits are larger.

Batch scalar getters borrow the existing batch lease, requiring no independent
column handle. `batch.Column(index)` creates a separately admitted column;
`column.Retain` and `batch.Retain` create independent owners sharing the same
payload allocation. Columns survive batch, cursor and database closure.
`BufferInfo` exposes allocation namespace/ID/generation and retained/visible
range; `Info` distinguishes physical rows from selected result rows. Scalar
getters index selected result positions without gathering and preserve validity
and numeric types. `SchemaCopy` explicitly copies schema metadata only.

Close independent owners explicitly. Go value copies share the same release
state; high-level access after release returns `RetainedClosed`. Each owner
retains the library mapping, and scalar reads and release use the same lock.
Finalizers are a leak safety net, not the mechanism for unblocking a pull.

`RetainedBackpressure` is recoverable and never EOF: release held views and retry
the same cursor. Pressure is reported before source advancement and does not
wait for the caller. Collecting all batches or repeatedly retaining views hits
the shared slot/byte/handle allowance. Terminal failures are explicit; previously
produced batches remain provisional until successful `io.EOF`. `State` observes
late completion/failure while a live owner keeps earlier values readable.

This is an experimental numeric adapter. Complete pinned-source/planning
admission, foreign allocation/RSS and performance qualification, Python views
and Arrow export remain unfinished. Engine charges do not bound arbitrary Go
runtime allocations or application collections. Windows amd64/arm64 compile
checks are not Windows execution qualification. Defaults and durability remain
unchanged; see [the retained delivery notes](../../docs/RETAINED_NUMERIC_FOUNDATION.md).

```sh
bazel test //bindings/go:hawdb_go_tests
```

The Bazel target declares its native library and runs with cgo disabled.
