# Host boundary qualification

These local tools measure the ordinary owned Rust, Python and pure-Go APIs for
[issue #976](https://github.com/nowledge-co/hawdb/issues/976) and the measurement
gate in [the interchange spec](../../docs/specs/ZERO_COPY_COLUMNAR_INTERCHANGE_SPEC.md).
They do not enable a strict zero-copy query path or change database defaults.
[Initial observations](RESULTS.md) record partial measurements and remaining gates.
[Current-main Rust controls](RUST_ORDINARY_RESULTS.md) preserve the complete scoped
paired comparison, unfavorable observations and default-budget refusals.

Build every engine dependency together in the same optimized configuration:

```console
bazel build -c opt //:hawdb_bench_host_boundary \
  //bindings/benchmarks:python_boundary //bindings/go/cmd/boundary \
  //bindings/ffi:hawdb_ffi
```

Stage the exact source before running the matrix. The driver records its Git
HEAD and staged tree and rejects unstaged tracked changes before every job and
at completion. It resolves Bazel output symlinks and hashes the binaries and
library actually used. Do not edit or rebuild those sources during a run.

```console
python3 -B bindings/benchmarks/run.py --output /tmp/hawdb-boundary-baseline
```

The full matrix uses sizes 1,000, 100,000 and 1,000,000, all five cases (`select`,
`point`, `fill`, `fill_bulk`, `wide`), both memory and persistent stores, one
discarded iteration and three measured iterations. A small wiring check uses
`--sizes 3 --samples 1`; it does not qualify bulk performance. Large point and
single-write workloads can take substantial time: point runs perform one
query per input row during both warmup and measurement; fill performs one
committed statement per row. The driver imposes no query limit to shorten them.

Every layer receives the same fixed-seed fixture and parameterized statements.
`wide` has 20 columns, including Unicode, embedded NUL, strings, lists, maps,
booleans and nulls. Scores use exactly representable binary fractions. The
schema and an ordered typed checksum distinguish integers from floats and
include recursive values. This fixture alone does not prove all arbitrary
floating-point, UUID or binary round trips.

Read cases seed in 512-row batches and warm the same query path in the same
process and handle before measurement. Fill cases start with an empty indexed
store. Each iteration creates a fresh database. Persistent commits keep
`SyncOnEveryWrite`; no environment variable or benchmark override relaxes it.
The query-boundary timer includes parameter/result conversion and execution.
For `fill` and `fill_bulk`, it also includes the final result-verification read;
it is not a pure write metric. `write_boundary_ns` contains only the committed
write calls and `read_boundary_ns` contains only the result/verification query
calls. Their sum is the unchanged `query_boundary_ns`. No call is removed from
the workload. Older reports without these fields cannot separate the phases.
All clients then perform the same typed checksum work, timed separately.
Use `median_query_boundary_ns` for cross-language boundary comparisons.
`median_elapsed_ns` also includes the language-specific checksum implementation
and must not be treated as isolated crossing overhead. These five-case tools
exercise ordinary owned results; they do not qualify retained or Arrow latency.
Arrow in PR #986 applies to explicit retained read delivery. It is not used by
these write calls. Write measurements are compatibility/regression controls;
Arrow acceptance concerns read execution and delivery performance. A difference
between two mixed workloads is not evidence of an Arrow-caused write regression.
The Go `BenchmarkRetainedBatchScalarReads` separately measures locked scalar
reads of one held native batch, with setup and the first descriptor lookup
outside its timer. It does not measure cursor creation, bulk export or RSS.
The separate `retained_boundary` Python tool and `BenchmarkNumericResultDelivery`
Go benchmark compare complete numeric query consumption. Their scope, raw
samples, default-budget refusals and review follow-up performance criterion are
in [RETAINED_RESULTS.md](RETAINED_RESULTS.md).

`retained_write_control` measures the same bulk `SET` with no cursor, a cursor
closed before writing, or an unconsumed retained cursor held throughout writing.
Each mode uses a separate database with an integer score and an unrequested
fixed-size string per node. Source admission, later consumption of the old
snapshot and verification of the newly written scores are outside the write
timer. Mode order rotates, one iteration is discarded, and all result owners
must release before the next iteration. Persistent writes retain the default
`SyncOnEveryWrite`. No Arrow API is called. `--mode ordinary` can compare an
ordinary baseline without retained APIs; the other modes require this PR's
experimental source admission. This bulk-update control does not replace the
five-case matrix, single-write controls or whole-operation memory measurement.
Scoped source-cache and slow-reader observations, including unfavorable writes
and failed large-result baselines, are in
[RETAINED_WRITE_RESULTS.md](RETAINED_WRITE_RESULTS.md).

```console
bazel build -c opt //bindings/benchmarks:retained_write_control
bazel-bin/bindings/benchmarks/retained_write_control \
  --rows 1000 --pad-bytes 4096 --samples 7 --backend file
```

Process wall time and peak RSS also include fixture parsing, setup, warmup and
the host runtime; they are not query-only resource charges.

Source identity rejects unstaged changes and non-ignored untracked files.
Stage the intended final source and keep experiment artifacts outside the
checkout so their provenance binds to the recorded index tree.

The output directory retains `report.json` and every job's stdout/stderr.
Repeated input files and the driver's own database directories are deleted
only after that job completes; the fixed recipe and input SHA remain recorded.
Budget refusal, process failure, missing output, schema mismatch and checksum
mismatch remain visible. Any such failure makes the driver return nonzero.
A complete experiment containing refusals is not a successful qualification.
Run on an otherwise idle host before making performance comparisons; record
competing work if an exploratory run shares the host.

## Allocation and conversion observations

### Manual two-revision Linux comparison

The Native Bindings workflow has an opt-in `boundary_comparison` dispatch.
It never runs this benchmark on pull requests or ordinary pushes. It builds
both native producers with default Bazel configuration and `-c opt`, finishes
compilation before measurement, and runs the unchanged five-case driver for
Rust, Python and Go. The dispatch defaults retain all three sizes, both
backends, one discarded iteration and three measured iterations. Large cases
can reach the workflow's six-hour execution ceiling; partial reports and raw
failures are uploaded and cannot count as completed qualification.

`compare.py` pins current-main baseline `3cdb2610ba8b984676b80ba22df64821787886d3` and records
the candidate HEAD and staged tree. The update from `5a5c4639` changes only
three test deadline attributes; the adapter target source files are identical.
The isolated baseline adds the identical
ordinary harness and the patch in `main-baseline-adapters.json`: an untimed
in-memory constructor/Go opening adapter. Query, conversion, write and durability
implementations remain the baseline's. The patch is not applied to production
main and refuses any other baseline revision. Build logs, exact staged source,
native/launcher hashes and all raw matrix outcomes remain in the artifact.
Each driver's source/artifact guards still apply. Result ratios are ratios of
medians from sequential baseline/candidate runs, not alternated paired samples.
Any refused, incomplete or failed sample prevents a group speedup claim.

```console
gh workflow run native-bindings.yml --ref feat/978-boundary-baseline \
  -f boundary_comparison=true
```

Narrower `benchmark_sizes`, `benchmark_cases` and `benchmark_samples` inputs
are available for wiring checks and staged qualification; their exact scope is
recorded and does not satisfy omitted workloads. This default-configuration
Linux path avoids the local macOS 27 stripped-dylib loading failure without
changing target flags or repairing benchmark binaries. The ordinary owned
matrix remains separate from retained/Arrow query and memory qualification.

### Instrumented local observations

Instrumented runs are separate from ordinary latency results. Build these
manual targets explicitly:

```console
bazel build -c opt //:hawdb_bench_host_boundary_allocations \
  //bindings/benchmarks:python_boundary //bindings/go/cmd/boundary \
  //bindings/ffi:hawdb_ffi_boundary_profile \
  //bindings/python:_hawdb_boundary_profile
```

For macOS (use `.so` instead of `.dylib` on Linux):

```console
python3 -B bindings/benchmarks/run.py --output /tmp/hawdb-boundary-profile \
  --rust bazel-bin/hawdb_bench_host_boundary_allocations \
  --library bazel-bin/bindings/ffi/libhawdb_ffi_boundary_profile.dylib \
  --python-extension bazel-bin/bindings/python/lib_hawdb_boundary_profile.dylib \
  --native-profiles \
  --cpu-profiles
```

The standalone FFI and Python `boundary-profiling` Cargo features and the
allocation benchmark install a Rust counting allocator only in these
instrumented artifacts. The embedded library and default binding artifacts
keep their ordinary allocator. Native counters cover successful allocation,
deallocation and reallocation requests on native threads. A successful resize
counts one allocation/free pair of requested sizes; this is not proof that
`realloc` copied payload. Live requested bytes and process peak requested bytes
exclude allocator rounding, foreign heaps and non-Rust workspace. Concurrent
cross-field snapshots are observations, not a memory-admission ledger. Peaks
include setup/warmup; deltas cover the measured operation and checksum work.

C and Python additionally time parameter conversion, the existing engine call,
and native result conversion. Python separately records traced host allocation
peaks. Go reports its heap allocation traffic and may save Go CPU `pprof`
profiles; these do not constitute a complete native engine stack profile.
Successful instrumented records must include the applicable native counters;
missing profiles fail qualification even when value parity succeeds. Refusals
before measurement can have no per-operation profile and still remain failures.
No counters claim payload-copy identity or strict result admission.

`--python-extension` selects a fixed artifact; it does not enable profiling.
Ordinary runs also resolve, hash and explicitly load their extension from the
selected Python launcher's runfiles. Use `--native-profiles` for the counting
allocator matrix: every successful Rust/Python/Go record must contain its required
counters. Unexpected counters in an ordinary run fail qualification. Go CPU
profiles are selected separately by `--cpu-profiles` and mark the run instrumented.
Artifact hashes are checked before and after each child and at completion; a
concurrent rebuild terminates qualification instead of mixing revisions. Copy
artifacts and required runfiles into separate before/after directories before
rebuilding for an alternating revision comparison.

`compare.py` independently copies each Python launcher and its complete runfiles
tree before building the next producer. Relative links within that tree remain
relative; external links are dereferenced, and manifests point to the frozen
copies. This includes the matching public Python package, not just `_hawdb.so`.
Runfiles hashes are checked after all builds and before/after each producer
matrix; ordinary binary hashes remain checked around every child. Python
children select their own runfiles and clear inherited Python/runfiles paths.
Bytecode writes are disabled, and preexisting bytecode caches are excluded from
the copy. These are untimed consumer-isolation controls, not engine overrides.

## Mixed growth and indexed point control

The manual `mixed_point_growth` probe warms a point plan at four rows, performs
independent CREATE statements, then measures 100 parameterized point calls.
It supports the raw database API and the admitted host path separately. Bound
EXPLAIN diagnostics run after hot timing because they may refresh statistics;
another 100 calls then measure the refreshed plan. Output preserves individual
times, plans, decisions, execution profiles and exact result verification.
The default `stable` mode repeats the initially warmed parameter. Optional
`rotating` mode changes the bound value on each call and exercises cache misses;
it does not qualify prolonged reuse of the initially warmed small-table plan.

```console
bazel build -c opt //:hawdb_bench_mixed_point_growth
bazel-bin/hawdb_bench_mixed_point_growth 1000 - raw stable
bazel-bin/hawdb_bench_mixed_point_growth 1000 - admitted stable
```

The probe accepts 9 through 10,000 rows and a new file path or `-` for memory.
Point input construction and verification are outside each query timer; growth
timing includes parameters and loop work. Database cache counters do not cover
an admitted read transaction's private cache. Plans, decisions and execution
profiles describe raw Database diagnostics, not private admitted read plans.
Freeze both revision producers and
finish all compilation before a comparison. This exploratory mixed workload
does not replace the fixed ordinary matrix or qualify Arrow or source reuse.

## Retained source-capacity creation control

The manual `retained_source_capacity` benchmark opens four read snapshots before
any retained cursor preflight. It creates and consumes one cursor at a time,
keeping default admission and zero prefetch. Each `creation_ns` timer covers
`into_retained_query` only; fixture setup, snapshot acquisition and full ordered
consumption are outside those timers. Source preflight counts distinguish the
cold directory from subsequent same-generation cursor creations. Timings still
include planning and admission, so they cannot isolate capacity-walk CPU cost.

```console
bazel build -c opt //:hawdb_bench_retained_source_capacity
bazel-bin/hawdb_bench_retained_source_capacity 1000 -
bazel-bin/hawdb_bench_retained_source_capacity 10000 /tmp/new-hawdb-source-control
```

The file argument must name a new path. Preserve any admission refusal rather
than raising the default budget. Output verifies every ordered integer value,
completion, source release and final retained owner/handle charges. This probe
uses the embedded Rust facade; it does not qualify Arrow, source-to-host reuse,
whole-query memory, cross-language performance or the full performance gate.
The [completed cold/warm control](RUST_ORDINARY_RESULTS.md#same-generation-source-capacity-creation-control)
preserves every raw result and hash separately from ordinary query timing.
