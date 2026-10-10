# Host boundary qualification

These local tools measure the ordinary owned Rust, Python and pure-Go APIs for
[issue #976](https://github.com/nowledge-co/hawdb/issues/976) and the measurement
gate in [the interchange spec](../../docs/specs/ZERO_COPY_COLUMNAR_INTERCHANGE_SPEC.md).
They do not enable a strict zero-copy query path or change database defaults.
[Initial observations](RESULTS.md) record partial measurements and remaining gates.

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

`compare.py` pins baseline `69d526a2d2d4b75741a731d0203949ffc1d123fb` and records
the candidate HEAD and staged tree. The isolated baseline adds the identical
ordinary harness and the patch in `main-baseline-adapters.json`: an untimed
in-memory constructor/Go opening adapter and the exhaustive Python error mapping
needed to compile this main revision. Query, conversion, write and durability
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
