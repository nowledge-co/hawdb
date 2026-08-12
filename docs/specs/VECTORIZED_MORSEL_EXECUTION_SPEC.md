# Skein Vectorized Morsel Execution Specification

## Scope

This specification defines the storage-neutral columnar batch contract, the
initial vectorized read fragment, morsel resource admission, deterministic
execution, fallback behavior, and performance evidence.

The durable canonical representation is governed by
[`COLUMNAR_CANONICAL_AND_PROJECTION_SPEC.md`](COLUMNAR_CANONICAL_AND_PROJECTION_SPEC.md):
canonical storage is columnar, and executor columnar batches become zero-copy
views over canonical column chunks as its phases land. Until a phase lands,
the executor projection remains storage-neutral and MUST NOT introduce an
additional durable representation beyond the canonical one.

## Columnar Batch Contract

`skein-executor` owns the following storage-neutral types:

- dense `SlotId` values and a `BindingSchema`;
- typed `ColumnVector` values with explicit validity;
- `Selection::All`, dense bitmap, and sparse index representations;
- zero-copy projection by sharing immutable column storage;
- typed integer and floating-point comparison kernels.

All-valid columns MUST NOT allocate a validity bitmap. Selection kernels MUST
build the final sparse-index or dense-bitmap representation adaptively rather
than construct both representations for every filtered batch. Storage adapters
MUST retain and clear reusable batch buffers instead of replacing their
allocation after each emission.

Missing and `NULL` values MUST be invalid in the comparison column and MUST NOT
pass a range predicate. Integer-to-float comparison and floating-point ordering
MUST match the row executor, including `f64::total_cmp` behavior for NaN.

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

## Morsel Contract

A morsel is a scheduling unit containing a bounded row range. It is distinct
from the columnar batch representation. Every morsel has a `PipelineId`, stable
ordinal, start row, and row count.

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

Input references and worker output are retained for at most one worker wave.
Workers MUST NOT call the host row consumer or mutate the query observer.
Prepared batches merge by stable morsel ordinal on the coordinator before
crossing the consumer boundary. A fragment with an early output limit no larger
than one morsel, an out-of-core source, one admitted CPU slot, or insufficient
worker memory MUST execute serially. CPU slots, memory, cancellation, result
bytes, and storage I/O depth remain separate admission dimensions.

## Observability

Execution profiles MUST expose:

- columnar batch count;
- columnar input and selected row counts;
- consumed morsel count;
- maximum admitted workers;
- peak active workers.

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

`SKEIN_EXECUTOR_BENCH_MODE=scheduler` isolates shared-pool scheduling.
`SKEIN_EXECUTOR_BENCH_MODE=morsel` exercises the complete streaming production
fragment with `SKEIN_MORSEL_BENCH_WORKERS` set to 4, 8, or 16. An optional
`SKEIN_MORSEL_BENCH_ROWS` selects one common dataset size, but the benchmark
MUST reject a size that cannot activate the requested workers under default
admission. Local benchmark output MUST identify itself as non-production
evidence. The reproducible workload and directional result are recorded in
[`../EXECUTOR_MORSEL_BENCHMARK.md`](../EXECUTOR_MORSEL_BENCHMARK.md).

CI MUST check compilation, semantics, and deterministic resource bounds. A
fixed speedup threshold is intentionally not a correctness gate because shared
CI hardware is noisy; release qualification records the benchmark artifact on
the target platform.
