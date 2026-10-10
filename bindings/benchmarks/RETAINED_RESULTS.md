# Retained numeric delivery: review follow-up measurements

This experiment advances [PR #986](https://github.com/nowledge-co/hawdb/pull/986)
and references [#976](https://github.com/nowledge-co/hawdb/issues/976) and
[#987](https://github.com/nowledge-co/hawdb/issues/987). Both issues remain open.
The complete samples, failures, source identity and artifact hashes are in
[retained-observations.json](retained-observations.json).

## Completion criterion and scope

The follow-up requires a reproducible performance improvement, matching ordered
results and unchanged default resource/durability limits. For the supported
numeric fixture, use complete query-and-consumption time as the primary metric,
require at least a 10% improvement in every paired round, and report delivery
time separately. Refused baselines cannot establish a speedup. An isolated
scalar-read microbenchmark cannot satisfy this criterion.

The Go cache optimization and Python retained buffers meet this relative
improvement criterion for the measured 1,000/10,000-row fixtures across the
recorded repeats. Arrow does not meet the complete criterion: the final
1,000/memory group includes a 0.632x paired wall-time regression. The overall
Arrow performance gate remains open; do not mark the whole PR complete.
These are
shared-host observations: other Rust/Clippy/browser work was active. They do not
qualify quiet-host absolute latency, a production SLO, the complete #976 matrix,
or general-query zero-copy. Raw variation and the initial run are retained.
The initial Python 1,000/memory run overlapped an exploratory Go matrix; the
table below uses the subsequent sequential repeat.

The fixture returns three non-null numeric columns: a stored integer, unsigned
identity and the repeated stored integer. It uses this parameterized query:

```cypher
MATCH (n:Delivery) WHERE n.score >= $min
RETURN n.score AS score, id(n) AS identity, n.score AS again
```

`min` is zero. Setup writes integer scores in 512-row parameter batches, outside
the timer. Each representation discards a warmup and verifies row count and the
same ordered numeric checksum. No PyArrow-to-list/NumPy conversion is used.
The memory/file backends use the ordinary facade defaults; persistent writes
retain `SyncOnEveryWrite`. The 16 MiB result allowance, retained byte/handle
limits, 1,024-record demanded batches and zero prefetch are unchanged.

## Python: owned, retained buffers and public Arrow stream

CPython 3.14.6 and PyArrow 26.0.0 consume the optimized extension. Seven paired
rounds rotate owned/retained/Arrow order. Each timed operation includes query,
all demanded batches, scalar checksum and lease release. The separate boundary
timer excludes the checksum loop but includes creation, pull, view descriptors
and release. Compare modes within Python; this is not a cross-language checksum
timing comparison.

Sequential-repeat medians, in milliseconds:

| Rows / backend | Owned complete | Retained complete | Arrow complete | Owned boundary | Retained boundary | Arrow boundary |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| 1,000 / memory | 1.339 | 0.604 | 0.675 | 0.935 | 0.212 | 0.281 |
| 1,000 / file | 1.279 | 0.633 | 0.708 | 0.918 | 0.262 | 0.318 |
| 10,000 / memory | 11.381 | 4.912 | 5.618 | 7.643 | 1.245 | 1.669 |
| 10,000 / file | 10.243 | 4.619 | 4.782 | 6.773 | 1.005 | 1.288 |

Median paired complete-operation speedups are 2.11-2.25x for retained buffers
and 1.68-2.13x for Arrow. The worst individual paired round is 1.55x and 1.41x,
respectively. The first matrix also shows improvements; its complete
samples are preserved rather than selecting only the repeat.

An additional post-validation repeat stopped all task compilation/tests before
timing. External Clang/Rust compilers appeared again in the recorded before/after
CPU snapshots, so it was not an idle-host qualification. Its seven-round paired
medians are 1.82-2.35x for retained and 1.39-2.12x for Arrow. Retained improves at
least 1.18x in every pair. Arrow's 1,000/memory group contains the 0.632x outlier
described above; the other three groups improve at least 1.25x in every pair.
All samples remain in the observations file. Do not discard the regression or
count a faster median as satisfaction of the stricter acceptance criterion.

After every successful measured operation, the live database reports zero
retained bytes, buffer owners and view handles. Peak charged retained bytes
across all modes in each process are at most 20,111 bytes. This is the native
retained governor, including descriptor charges; it does not include unadmitted
source pages, planner/input workspace or arbitrary Python/PyArrow allocations.
Process peak RSS includes setup, every representation and its warmups, and is
recorded per process. It cannot attribute RSS to an individual mode or establish
a whole-operation memory bound.

## Go: remove per-scalar foreign descriptor lookup

Before the follow-up, every `Int64At`/`UInt64At` borrows the same column descriptor
again through purego and the native registry. A batch now lazily caches its
immutable descriptors under its existing owner read lock. Copied Go batch
wrappers share the descriptor cache and release state. Independent retains get
their own prepaid descriptor capacity. No numeric payload is copied.

The before snapshot uses Go sources from
`8b7c6fdf17c56011cb9205146f4afafc786f797c`; the after snapshot uses the frozen
follow-up tree. **Both load the identical optimized native library and identical
benchmark source.** This isolates the Go wrapper change rather than comparing
two complete engine revisions. Five rounds alternate before/after order and
use `-benchtime=300ms -count=1`, with ordinary owned results as a control in each
run. Timed iterations include complete query, delivery, the same scalar checksum
and explicit batch/cursor release.

Medians across five rounds:

| Rows / backend | Before retained (ms) | After retained (ms) | Before Go allocs/op | After Go allocs/op | Before Go bytes/op | After Go bytes/op |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| 1,000 / memory | 1.200 | 0.126 | 21,080 | 169 | 1,349,788 | 12,508 |
| 1,000 / file | 1.402 | 0.169 | 21,080 | 169 | 1,349,831 | 12,509 |
| 10,000 / memory | 12.038 | 1.396 | 210,526 | 1,429 | 13,476,354 | 112,336 |
| 10,000 / file | 12.223 | 1.330 | 210,527 | 1,429 | 13,476,452 | 112,329 |

The retained complete-operation medians improve 8.30-9.54x; every paired round
improves at least 4.52x. After-cache retained results are also faster than owned
results in every measured run. Owned control timings vary with host load, so
the raw control values are retained. Go allocation figures cover Go heap
allocation traffic, not native allocations, retained bytes or peak RSS.

The post-validation five-round Go repeat preserves the same improvement: its
retained medians move from 1.544/1.647/16.463/17.370 ms to
0.235/0.248/2.716/2.670 ms in table order (6.06-6.65x). Every pair improves at
least 4.73x. The raw owned controls and allocation counts remain recorded.

## Refusals and unqualified workloads

The 100,000-row owned reference fails on both backends at the default result
budget: its next charge would be 16,777,386 bytes against 16,777,216 allowed.
Python records errors and exits nonzero. The full Go matrix also exits nonzero
with both refusals. No budget is raised, failed group is omitted, or refused
baseline is counted as an accelerated result. Large-result admission remains
work for #987.

This fixture does not qualify indexed point lookups, wide/UTF-8/nested rows,
graph/FTS candidate hydration, SIMD, cold I/O, arbitrary NULLs, foreign allocator
accounting or other Arrow versions/platforms. Current retained production starts
from materialized node pages and constructs the first numeric representation;
it does not establish storage-to-host zero-copy or an indexed-point producer.
Keep point lookup and row hydration on the ordinary explicit path until a
compatible row/page lease and independent workload evidence exist. The layout
decision is detailed in
[the retained foundation](../../docs/RETAINED_NUMERIC_FOUNDATION.md#point-lookups-row-reads-and-columnar-delivery).

## Reproduction and identity

Environment: macOS 27.0.1 (26A434), arm64, Apple M5 Max, 36 GiB RAM, pinned Rust
1.97.1, system Go 1.27.2. Measurements used `bazel build -c opt`; no configuration
file, default allowance or durability override was changed.

Frozen production and benchmark tree:
`738557a05038b258ccf8dbb33ea62d2fe89af0da`, formed by merging current main into
the PR head and applying review fixes. Evidence, documentation and CI wiring
added afterward do not change the measured producer or harness. Binary and harness SHA-256 values are
in the observations file. Keep measurement output outside the checkout.

```console
bazel build -c opt //bindings/benchmarks:retained_boundary //bindings/ffi:hawdb_ffi
PYTHONPATH="$PWD/bazel-out/darwin_arm64-opt/bin/bindings/benchmarks/retained_boundary.runfiles/_main/bindings/python/python" \
  uv run --no-project --python 3.14.6 --with pyarrow==26.0.0 python bindings/benchmarks/retained_boundary.py \
  --rows 10000 --backend memory --mode all --samples 7
cd bindings/go
HAWDB_LIBRARY="$PWD/../../bazel-out/darwin_arm64-opt/bin/bindings/ffi/libhawdb_ffi.dylib" CGO_ENABLED=0 \
  go test -run '^$' -bench 'BenchmarkNumericResultDelivery/rows=(1000|10000)$' -benchtime=300ms -count=1
```

Repeat both backends and row sizes; use an otherwise idle host for latency
qualification. For the Go comparison, extract `bindings/go` from the specified
before commit into a separate directory and copy the identical benchmark file
there. Alternate five before/after rounds, preserving the same native artifact.
Running the full unfiltered Go benchmark intentionally exposes the 100,000-row
budget failures.
