# HawDB Vectorized Morsel Execution Specification

## Scope

This specification defines the storage-neutral columnar batch contract, the
initial vectorized read fragment, morsel resource admission, deterministic
execution, fallback behavior, and performance evidence.

The durable canonical representation is governed by
[`ROW_PAGE_AND_DEMAND_PAGED_INDEX_SPEC.md`](ROW_PAGE_AND_DEMAND_PAGED_INDEX_SPEC.md):
canonical storage is row-oriented and page-bounded. Executor columnar batches
are a storage-neutral in-memory execution format. A row-page adapter decodes
only required fields into reusable typed columns and keeps page pins for at
most one pipeline wave. Vectorization MUST NOT require a second canonical
durable representation.

## Columnar Batch Contract

### Logical types and borrowed values

`hawdb-core::LogicalType` is the shared semantic type vocabulary for graph
property descriptors, PostgreSQL-compatible relational scalars, planning, and
execution. Nullability is a separate slot property; `NULL` does not introduce
another physical column type. Graph `String` and unbounded `Text` remain
distinct logical policies even though both use the UTF-8 column encoding.
`Binary` is a logical relational type even when a particular vectorized
fragment has no binary kernel.

Executor-private identities are not logical types. `SlotType` distinguishes a
logical value slot from `NodeId` and `RelationalRowLocator`, while `ColumnType`
describes the concrete in-memory encoding. Batch construction MUST validate
the logical/physical compatibility once before entering a typed loop.

`ValueRef<'a>` is the borrowed scalar boundary. Fixed-width values are copied;
strings, binary values, lists, and maps borrow their existing payload. Comparison, type
checking, branch selection, and immediate serialization SHOULD consume
`ValueRef` without constructing an owned `Value`. Ownership conversion MUST
be explicit and delayed until a result is retained beyond the current batch or
consumer call.

### Schema-bearing result boundary

Collected query results MUST store one immutable `QuerySchema` plus positional
`Vec<Value>` rows. Column names MUST NOT be copied into a tree node for every
row. Consumers that read `schema()` and `value_rows()` stay on this compact
representation. The legacy `Row = BTreeMap<String, Value>` view is a lazy
formatting compatibility boundary; materializing it MUST not be required by
query execution or by schema-aware embedded consumers. Empty relational reads
MUST retain their projected schema, and every positional row width MUST equal
the schema width.

`ColumnarRowRef` exposes selected columnar rows by slot or name, and `RowRef`
provides one host-facing view over scalar-map fallback rows and columnar rows.
A synchronous consumer MUST NOT retain either view. It MAY call
`RowRef::to_owned_row` when application state needs ownership. The public
borrowed streaming entrypoint retains the existing row and payload budgets and
keeps consumer calls provisional until successful query completion. Its
initial scalar fallback borrows an already materialized row; direct columnar
delivery remains an executor capability until end-to-end qualification proves
that activating it improves the application boundary.

Bulk input must follow the same execution model in the opposite direction:
an application-provided source is decoded into bounded batches, validated and
coerced against `LogicalType`, and passed to an executor write sink that owns
the transaction/WAL boundary. A future `LOAD DATA` or `COPY FROM` syntax is a
front end for that operator chain; it MUST NOT bypass the executor through a
route-specific storage API.

Performance qualification MUST compare owned materialization with borrowed
consumption for production-sized variable-width values. The focused kernel
gate records elapsed time, payload bytes copied, and identical result
checksums. End-to-end qualification additionally records query-ledger peak,
RSS, and payload bytes at the host boundary. A borrowed path that does not
produce a repeatable end-to-end gain must remain an internal capability rather
than become the recommended host entrypoint.

`hawdb-executor` owns the following storage-neutral types:

- dense `SlotId` values and a `BindingSchema`;
- typed `ColumnVector` values with explicit validity;
- `Selection::All`, dense bitmap, and sparse index representations;
- zero-copy projection by sharing immutable column storage;
- typed integer, floating-point, and boolean filter kernels;
- selection-preserving offset/limit;
- validity-aware `COUNT` and checked `BIGINT` `SUM` kernels.

All-valid columns MUST NOT allocate a validity bitmap. Selection kernels MUST
build the final sparse-index or dense-bitmap representation adaptively rather
than construct both representations for every filtered batch. Storage adapters
MUST retain and clear reusable batch buffers instead of replacing their
allocation after each emission.

Missing and `NULL` values MUST be invalid in the comparison column and MUST NOT
pass a range predicate. Integer-to-float comparison and floating-point ordering
MUST match the row executor, including `f64::total_cmp` behavior for NaN.
`COUNT(column)` MUST ignore invalid rows, while `COUNT(*)` counts selected rows.
`BIGINT` `SUM` MUST ignore invalid rows, return `NULL` for an empty input, and
fail closed on overflow rather than wrap.

The initial production fragment is:

```text
SeqNodeScan -> PropertyCompare -> Project -> optional Limit
```

It is eligible only when:

- the scan has one exact node label;
- the predicate compares one property with an integer or floating-point
  literal;
- the catalog has a public `Int` or `Float` descriptor for that property;
- projection expressions are node id, node property, or literal values.

Eligibility MUST be decided before scanning. An unsupported fragment MUST use
the existing row pipeline for the whole fragment; execution MUST NOT switch
between row and columnar evaluation after emitting output.

The in-memory adapter MUST borrow scan rows, retain only the admitted typed
predicate column, optional node-id column, and validity for one batch, run the
same typed selection kernel as the owned adapter, and materialize only selected
projected output. The out-of-core adapter MAY own decoded records while filling
that typed batch, but MUST drop each record before accepting the next one and
remain bounded by row and byte batch limits. Both adapters MUST include the
worst-case validity bitmap and selected-row index scratch in admission. The
validity bitmap and selected-row buffers MUST be reused across batches. A
completed projection MUST NOT retain hidden node bindings that are outside the
projected result scope.

Materialized query output MUST bind its column names once and retain values in
one schema-ordered flat buffer. The final result boundary MUST NOT retain a
`BTreeMap` or a nested value vector per row. Compatibility consumers that ask
for owned named rows MAY materialize maps explicitly; streaming consumers MAY
continue to receive owned named rows when ownership must cross the callback.
This ordinal result boundary does not extend the lending GAT cursor across
joins, blocking operators, spill, callbacks, or public object-safe interfaces.

An eligible parallel worker MUST place a bounded sequence of typed
`ColumnarBatch` values and their selections into the ordinal stream for each
production numeric morsel. It MUST NOT construct per-row `Binding` maps while
the result is queued or waiting in the reorder window. The coordinator
materializes selected bindings only when that ordinal reaches the consumer
boundary. Before scheduling any worker, the fragment MUST use a conservative
schema, validity, selection, and typed-column estimate to prove that the whole
morsel fits the reserved output bytes. A projection that is not fully typed, or
a typed output that cannot fit, MUST select the serial batch path before
decoding or evaluating a morsel. A worker MUST NOT return a serial-fallback
marker after doing parallel work.

## Query Memory Ledger

Every read query MUST own one hierarchical runtime memory ledger. The root
budget is `ExecutionMemoryConfig::query_memory_bytes`; operator-local limits
remain subordinate budgets and MUST NOT be treated as independent capacity.
The initial account classes are pipeline batches, blocking state, spill
staging, morsel output, and result materialization.

A reservation MUST atomically satisfy both its local account budget and the
remaining root budget before the associated allocation is constructed. The
implementation MAY use conservative resident-memory estimates, but MUST NOT
omit an executor-owned buffer merely because another operator already has a
local limit. Spill serialization MUST reserve staging memory before allocating
its complete encoded record buffer, independently from the persistent
spill-byte quota. Spill decoding MUST retain that encoded staging lease until
the decoded sort, aggregate, distinct, or output state has been admitted to its
blocking account. A merge MUST NOT release the selected decoded row before a
replacement row or an emitted batch owns the corresponding root-ledger charge.

Leases MUST release through normal completion, cancellation, error, and panic
unwinding. A streaming row consumer holds only the current transfer lease and
MUST complete with zero query-owned bytes. A materialized result remains
charged at the execution completion boundary because ownership is handed to
the caller; the charge is released when the query result leaves that boundary.
An operator that first produces an admitted result set MUST retain its blocking
ownership until each batch is synchronously accepted by the enclosing pipeline;
returning a bare row vector MUST NOT release the account before that handoff.
Execution profiles MUST expose the root budget, peak aggregate charge,
completion charge, and account count. Root-budget rejection MUST be
fail-closed and identify `query_memory_bytes`.

[`../tla/HawDBQueryMemoryLedger.tla`](../tla/HawDBQueryMemoryLedger.tla)
models atomic hierarchical reservation, result handoff, and cleanup on
failure or cancellation.

## Morsel Contract

A morsel is a scheduling unit containing a bounded row range. It is distinct
from the columnar batch representation. The initial production numeric fragment
amortizes one scheduling decision over a fixed, bounded number of typed batches;
their combined retained size is admitted as one output before worker execution,
and no completed parallel work is discarded. Every morsel has a `PipelineId`,
stable ordinal, start row, and row count.

Admission MUST compute the worker upper bound as:

```text
min(requested workers, morsel count, memory budget / bytes per worker)
```

Positive work MUST fail before execution when one worker cannot fit. Empty work
reserves no workers or memory. Morsel output MUST merge by ordinal unless an
operator defines an explicit order.

The sequential scheduler remains the deterministic differential oracle. On an
admitted embedded query path, the eligible immutable in-memory fragment uses
parallel morsel execution by default when at least two workers fit. Parallel
execution MUST use the shared, runtime-governed pool and MUST NOT create a
thread set per query or per morsel wave. The default per-query soft ceiling is
one quarter of effective CPU capacity, rounded up, with a four-worker floor on
machines that provide at least four slots and a sixteen-worker hard ceiling.
It is further bounded by admitted CPU slots, current capacity, the shared pool,
morsel count, and per-worker memory. Automatic parallel activation requires at
least four morsels per worker so scheduler and merge overhead do not dominate
small scans. Ungoverned low-level executor calls remain serial.

Input references and worker output are retained for at most one admitted worker
window. The complete input-reference wave MUST be reserved in a root-ledger
pipeline-batch account before allocating its pointer vector. The shared-pool
scheduler MUST use a bounded result channel and MUST NOT issue ordinal `n` while
`n >= consumed_prefix + admitted_workers`. This sliding window reserves one
completion opportunity for every issued predecessor and prevents a slow early
morsel from turning the coordinator reorder map into an unbounded buffer.
Workers MUST NOT call the host row consumer or mutate the query observer.

Before constructing one worker result, the scheduler MUST reserve its maximum
retained output bytes in a `MorselOutput` query-memory account. The lease remains
live while the result is queued, reordered, and passed through the coordinator
consumer; the consumer must transfer retained values to another query-owned
account. Completed output merges by stable morsel ordinal before crossing the
consumer boundary. Cancellation, consumer failure, and worker panic MUST close
the issuance window, make blocked sends fail, join all shared-pool tasks, and
release every output lease.

A fragment with an early output limit no larger than one morsel, an out-of-core
source, one admitted CPU slot, or insufficient worker memory MUST execute
serially. CPU slots, memory, cancellation, result bytes, and storage I/O depth
remain separate admission dimensions.

[`../tla/HawDBBoundedMorselMerge.tla`](../tla/HawDBBoundedMorselMerge.tla)
models the sliding issuance window, bounded channel and reorder states,
deterministic prefix emission, and terminal cleanup after cancellation or a
worker panic.

## Observability

Execution profiles MUST expose:

- columnar batch count;
- columnar input and selected row counts;
- consumed morsel count;
- maximum admitted workers;
- peak active workers.
- peak completed morsel outputs and their estimated resident bytes;
- peak out-of-order reorder entries.

These fields MUST be available in structured explain/resource output and in the
printable explain-analyze root summary.

## Performance Evidence

`cargo bench --bench executor_vectorization` compares:

1. row `BTreeMap` predicate evaluation against the typed selection kernel;
2. equivalent row and columnar physical plans over an in-memory graph;
3. serial and default parallel morsel execution over the production columnar
   fragment, in addition to the isolated scheduler comparison.

Both comparisons MUST verify identical checksums before reporting timings. The
report includes median duration, per-iteration duration, rows per second, and
speedup. Row and columnar samples MUST be paired with alternating execution
order and a measurement window long enough to avoid timer-scale noise.
Performance conclusions MUST use optimized builds and MUST include the
end-to-end result; a faster isolated kernel does not qualify a slower query
pipeline.

`HAWDB_EXECUTOR_BENCH_MODE=scheduler` isolates shared-pool scheduling.
`HAWDB_EXECUTOR_BENCH_MODE=micro` isolates the `Int64` and `Float64` comparison
kernels while still checking each columnar result against row evaluation. This
mode is diagnostic evidence only; end-to-end conclusions still require the
default full mode.
`HAWDB_EXECUTOR_BENCH_MODE=morsel` exercises the complete streaming production
fragment with `HAWDB_MORSEL_BENCH_WORKERS` set to 4, 8, or 16. An optional
`HAWDB_MORSEL_BENCH_ROWS` selects one common dataset size, but the benchmark
MUST reject a size that cannot activate the requested workers under default
admission. The report MUST include peak queued output count and bytes plus the
peak reorder-entry count so throughput cannot hide a residency regression.
Local benchmark output MUST identify itself as non-production evidence. The
reproducible workload and directional result are recorded in
[`../EXECUTOR_MORSEL_BENCHMARK.md`](../EXECUTOR_MORSEL_BENCHMARK.md).

CI MUST check compilation, semantics, and deterministic resource bounds. A
fixed speedup threshold is intentionally not a correctness gate because shared
CI hardware is noisy; release qualification records the benchmark artifact on
the target platform.
