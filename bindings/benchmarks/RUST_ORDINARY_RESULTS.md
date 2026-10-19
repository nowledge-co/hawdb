# Historical ordinary Rust controls

Historical engines: initial baseline `3cdb2610ba8b984676b80ba22df64821787886d3` and
candidate `9630934b84a1d69e7713b4652322bfb08e242f86`; later controls identify their own
engine commits, staged trees and producer hashes in each section. No section measures
the current checkout implicitly.

Regenerate with: `python3 -B bindings/benchmarks/compare.py --sizes 1000,10000 --layers rust --samples 7 --output /tmp/hawdb-rust-boundary`.

Run from the selected clean candidate checkout with its matching harness. This
maintained driver generates a new sequential baseline/candidate comparison; historical
alternating-pair and mixed-growth experiments require their archived drivers and
ordering, not this command alone.

These frozen comparisons record ordinary API controls for
[issue #976](https://github.com/nowledge-co/hawdb/issues/976) and
[the projected point-seek issue](https://github.com/nowledge-co/hawdb/issues/992).
They do not complete the comprehensive performance gate.

## Revisions and measurement boundary

Baseline main is `3cdb2610ba8b984676b80ba22df64821787886d3` with staged tree
`927a6acd92d805c9a8ba8afa3ae24d5024838e66`. The staged additions are the identical
ordinary benchmark harness and untimed opening adapters. Query, conversion,
write and durability implementations remain main's. The earlier main adapter
qualification used `5a5c4639`; the update to `3cdb2610` changes only three test
deadline attributes. The native producer SHA-256 is
`ae0a4d5a25696f0b957e2b5d54a9b992d1ec2f359f71323bf1e2a85be873866d`.

Candidate is `9630934b84a1d69e7713b4652322bfb08e242f86`, tree
`5a720c8c2058ebce6c982af0087e0ba4a0013675`, native producer SHA-256
`b9a82d8fa8bbab8a182255e19afe63a794bdcd40b9d7dc8dda919ba302649cb3`.

Both producers use default `bazel build -c opt //:hawdb_bench_host_boundary`,
pinned Rust 1.97.1 and macOS arm64 on Apple M5 Max with 36 GiB RAM. The five
fixed-seed cases run at 1,000 and 10,000 rows on memory and file backends.
Each group has one discarded pair and seven measured pairs; candidate/baseline
order alternates by iteration. Every iteration opens a fresh database. Reads
include their unchanged warmup. Persistent writes use `SyncOnEveryWrite`.
No Arrow, retained API, allocator instrumentation, budget override or global
compiler override is used. Competing host compilation was present, so these
observations cannot establish an idle-host regression or latency guarantee.

## Observations

Paired speedup is baseline time divided by candidate time. Below one is slower.
The table reports the median of seven within-iteration ratios, rather than the
ratio of the two median times. Query boundary includes verification reads for
write cases. The separate write phase contains only the committed write calls.

| Rows | Case | Backend | Query-boundary speedup | Write-phase speedup |
| ---: | --- | --- | ---: | ---: |
| 1,000 | `select` | memory | 1.045x | - |
| 1,000 | `select` | file | 0.889x | - |
| 1,000 | `point` | memory | 1.128x | - |
| 1,000 | `point` | file | 1.178x | - |
| 1,000 | `fill` | memory | 0.931x | 0.931x |
| 1,000 | `fill` | file | 0.997x | 0.996x |
| 1,000 | `fill_bulk` | memory | 1.049x | 1.034x |
| 1,000 | `fill_bulk` | file | 0.899x | 0.929x |
| 1,000 | `wide` | memory | 0.980x | - |
| 1,000 | `wide` | file | 1.190x | - |
| 10,000 | `select` | memory | 0.915x | - |
| 10,000 | `select` | file | 0.916x | - |
| 10,000 | `point` | memory | 4.013x | - |
| 10,000 | `point` | file | 3.111x | - |
| 10,000 | `fill` | memory | 0.957x | 0.956x |
| 10,000 | `fill` | file | 0.984x | 0.984x |
| 10,000 | `fill_bulk` | memory | 1.017x | 0.999x |
| 10,000 | `fill_bulk` | file | 1.004x | 1.011x |
| 10,000 | `wide` | memory | Refused before timing | - |
| 10,000 | `wide` | file | Refused before timing | - |

All 320 expected records are present: 280 measured and 40 discarded. The 18
successful groups contain 288 records preserving typed checksums, row counts,
durability, profile absence and phase-sum invariants. Fixture, source, staged
index, harness and frozen binary identities pass final verification.

The 10,000-row indexed point workload has median query-boundary speedups of
4.013x in memory and 3.111x on files. Every measured point pair is faster at
that size; minima are 1.443x and 2.521x. It uses the ordinary row path, not
columnar delivery. No conclusion about source-to-host zero-copy follows.

Unfavorable scan and write groups remain in the table and raw evidence.
The 10,000-row memory single-write paired speedup is 0.956x; file is 0.984x.
Shared-host variability and mixed outcomes prevent attribution to retained
cache maintenance or acceptance of write neutrality.

Both 10,000-row wide groups fail during warmup on both producers: the ordinary
`query result (result_materialization)` would charge 16,780,548 bytes against
the unchanged 16,777,216-byte budget. All 32 failed records are preserved.
The native producer reports structured `status=error` with process exit zero;
the driver correctly refuses qualification and exits one. These are budget
refusals, not successful timings or speedups. No result is truncated.

## Evidence and remaining qualification

[Complete evidence JSON](evidence/README.md#rust-ordinary-controls-20261011) preserves
every record, paired ratio, source/artifact manifest, raw stdout/stderr and their
hashes, and the final audit. The original report SHA-256 is
`d5f1266574a88bf877abf9981e8121af31b679a14f36a446f28e343a46242c09`.
The unchanged driver and fixed fixture hashes make consumer parity auditable.

This comparison predates the shared-directory capacity-cache follow-up. It
does not measure that follow-up, Python/Go, 100,000/1,000,000-row qualification,
cold/warm retained creation, allocation profiles, or whole-query memory bounds.
Process peak RSS includes fixture parsing, setup, warmup and the checksum.
It is not the memory held solely by a retained source or result. The full
five-case cross-language performance and memory gates remain open.

## Shared-directory capacity-cache control
The subsequent control uses candidate `8513f167869013ea72e2066508e7e6662cb26bca`,
tree `017f7ea5608d8c963bc0183b5728583db45ba68e`, against the same main
baseline and binary described above. The candidate binary SHA-256 is
`206f996d383829e1e084f320fd89b95793123cc86c9577ed4191fd1134ff3e6a`.
The harness, fixture hashes, defaults, sizes, backend order, discarded pair,
and seven alternating measured pairs are unchanged. Both producer sources and
binaries stayed frozen throughout the experiment. Competing host compilation
was present; this control does not establish cache-change causality.

| Rows | Case | Backend | Query-boundary speedup | Write-phase speedup |
| ---: | --- | --- | ---: | ---: |
| 1,000 | `select` | memory | 0.951x | - |
| 1,000 | `select` | file | 0.924x | - |
| 1,000 | `point` | memory | 1.141x | - |
| 1,000 | `point` | file | 1.188x | - |
| 1,000 | `fill` | memory | 0.971x | 0.969x |
| 1,000 | `fill` | file | 1.027x | 1.027x |
| 1,000 | `fill_bulk` | memory | 1.013x | 1.017x |
| 1,000 | `fill_bulk` | file | 0.949x | 0.958x |
| 1,000 | `wide` | memory | 0.986x | - |
| 1,000 | `wide` | file | 0.948x | - |
| 10,000 | `select` | memory | 0.994x | - |
| 10,000 | `select` | file | 0.995x | - |
| 10,000 | `point` | memory | 2.581x | - |
| 10,000 | `point` | file | 2.511x | - |
| 10,000 | `fill` | memory | 0.986x | 0.986x |
| 10,000 | `fill` | file | 0.977x | 0.977x |
| 10,000 | `fill_bulk` | memory | 1.072x | 1.081x |
| 10,000 | `fill_bulk` | file | 1.073x | 1.080x |
| 10,000 | `wide` | memory | Refused before timing | - |
| 10,000 | `wide` | file | Refused before timing | - |

All 320 records are present, with 288 successful records and 32 unchanged
10,000-row wide budget refusals. The 10,000-row ordinary point groups have
paired medians of 2.581x (memory) and 2.511x (file); all seven measured pairs
are faster at this size. Scans and several write groups remain unfavorable.
The individual-create write-phase medians of paired ratios are 0.986x (memory)
and 0.977x (file) at 10,000 rows. These outcomes cannot close the comprehensive
performance gate or prove write neutrality. This experiment predates the
[read-free CREATE statistics change](../../docs/READ_FREE_CREATE_STATISTICS.md).

[Complete shared-cache control evidence](evidence/README.md#rust-ordinary-shared-cache-controls-20261011)
contains raw stdout/stderr, hashes, all ratios and the final audit. The original
report SHA-256 is `6c27ac81694b3812503221a4acf9b6b86665abe0c5a498a465cc82ae1f788949`.
The earlier `9630934b` observations above remain a separate historical control;
comparing their ratios across runs does not isolate the cache change.

## Read-free CREATE statistics control

This distinct control freezes candidate `6ce232ca14f65a1ec97487231e027e26432196a3`,
tree `3b42526cc8b47182933a1346c4534162160345df`, against the same main
`3cdb2610` baseline and staged adapter tree described above. The candidate native
SHA-256 is `8782210dbc84988e1f9abe6bd5bc2243f99c9bf6d9673c591b7429c5b8beafbe`.
The five cases, two sizes, both backends, default opt configuration, fresh
databases, unchanged durability/budgets and seven alternating measured pairs
are identical to the earlier controls. All compilation completed before timing.

| Rows | Case | Backend | Query-boundary speedup | Write-phase speedup |
| ---: | --- | --- | ---: | ---: |
| 1,000 | `select` | memory | 0.987x | - |
| 1,000 | `select` | file | 1.013x | - |
| 1,000 | `point` | memory | 1.141x | - |
| 1,000 | `point` | file | 1.143x | - |
| 1,000 | `fill` | memory | 5.616x | 5.735x |
| 1,000 | `fill` | file | 1.228x | 1.229x |
| 1,000 | `fill_bulk` | memory | 1.004x | 1.003x |
| 1,000 | `fill_bulk` | file | 1.012x | 1.011x |
| 1,000 | `wide` | memory | 1.023x | - |
| 1,000 | `wide` | file | 1.015x | - |
| 10,000 | `select` | memory | 1.000x | - |
| 10,000 | `select` | file | 0.908x | - |
| 10,000 | `point` | memory | 2.492x | - |
| 10,000 | `point` | file | 2.739x | - |
| 10,000 | `fill` | memory | 51.228x | 52.559x |
| 10,000 | `fill` | file | 2.299x | 2.299x |
| 10,000 | `fill_bulk` | memory | 1.017x | 1.017x |
| 10,000 | `fill_bulk` | file | 1.007x | 1.012x |
| 10,000 | `wide` | memory | Refused before timing | - |
| 10,000 | `wide` | file | Refused before timing | - |

All 320 expected records are present: 288 successful records and the same 32
10,000-row wide warmup budget refusals, with 280 measured and 40 discarded.
The independent-CREATE write-phase paired medians at 10,000 rows are 52.559x
(memory) and 2.299x (file); all seven pairs are faster at that size. The file
ratios range from 1.436x to 5.466x. At 1,000 rows the file median is 1.229x,
but its minimum is 0.764x. The 10,000-row file scan median remains unfavorable
(0.908x). Bulk-create medians remain near one. These mixed outcomes remain
visible and do not establish comprehensive performance acceptance.

Competing host compilation and fuzz processes were observed during the slow
file iteration. They were not terminated. This shared-host control supports
the measured ordinary-write improvement; it does not prove an idle-host latency
guarantee or isolate the optimization from all environmental variation.

[Complete CREATE-control evidence](evidence/README.md#rust-ordinary-create-controls-20261011)
contains all raw streams, hashes, alternating ratios, frozen source/artifact
identities and the final audit. Original report SHA-256:
`6eff00f7330636323e91a7031964f1ce76429307d638cb78dd3120b4142e008a`.
The evidence package SHA-256 is
`11d0f975862d31bc0e6f7610d12e02c89e5646cdd342ff4a41438ef9b38e3d36`.
The earlier two controls are retained unchanged; comparing ratios across runs
does not isolate a single change. None of these ordinary controls uses Arrow.

## Same-generation source-capacity creation control

A separate default-opt manual probe freezes the same `6ce232ca` source/tree.
Its native SHA-256 is
`bdc20c4baadab682bb95d0ea8fab07cb4b1738512915115504a199ca95934c60`.
There are 32 fresh-process records: one discarded and seven measured children
for each size/backend. Each child opens four read snapshots before preflight,
then creates and consumes one retained cursor at a time. Only the first cursor
visits source records for capacity accounting: every child reports `[N, 0, 0, 0]`.
Ordered values, completion and final zero owner/view/source charges are checked.

| Rows | Backend | Cold creation median | Warm creation median | Source capacity |
| ---: | --- | ---: | ---: | ---: |
| 1,000 | memory | 0.282 ms | 0.030 ms | 1,089,344 bytes |
| 1,000 | file | 0.393 ms | 0.036 ms | 1,089,344 bytes |
| 10,000 | memory | 1.860 ms | 0.032 ms | 10,901,992 bytes |
| 10,000 | file | 1.811 ms | 0.027 ms | 10,901,992 bytes |

Warm time is the median of the three warm creations within each child, then
the median of seven measured children. Timers include planning/admission and
exclude setup, snapshot acquisition and consumption. This is same-generation
cold/warm evidence, not a prior-head comparison or a pure capacity-walk timer.
It does not qualify Arrow, source reuse, foreign heaps or whole-operation memory.

[Complete source-capacity evidence](evidence/README.md#rust-source-capacity-controls-20261011)
includes raw streams, hashes, counters, resource observations and the final audit.
Original report SHA-256:
`a52e5df4fefceec0a97cbb1843761f4277e72123688996e0266309832326d19a`.

## Historical Linux matrix failure

The [old Linux run](https://github.com/nowledge-co/hawdb/actions/runs/38079825691)
compares frozen candidate `97bd19e0` and baseline `69d526a2`. GitHub terminates
its job at the six-hour limit. The partial baseline report contains 160 records:
90 successful, 54 process errors and 16 structured errors. Candidate measurement
never starts and 1,000,000-row cases are not reached. No comparative group is
qualified. Large scan budget refusals and incomplete point groups remain failures.

The Python process errors expose a benchmark defect: the baseline extension is
copied, but its launcher/runfiles remain in the shared Bazel output directory.
Building the candidate replaces that package entry point. It imports
`RetainedOptions` from the baseline extension and fails. These Python records
are invalid, independent of the engine performance gate. The comparator now
copies the complete launcher/package/runtime, preserves internal relative links,
dereferences external links and rewrites manifests to the frozen tree. It checks
runfiles hashes after all builds and before/after each producer matrix. Children
use the selected tree without inherited Python/runfiles paths or bytecode writes.

[Preserved partial evidence](evidence/README.md#linux-boundary-timeout-20261011)
contains all baseline raw streams and hashes, source/artifact manifest, timeout
annotations and qualification limits. The primary log SHA-256 is
`c5126615e33f84bdb42bcc4f56112c37cf81768161082fb20c9186be5c43b480`.
A corrected Linux comparison was still required at this snapshot. The corrected
1,000-row comparison follows below; larger sizes remain open. macOS baseline Python remains
unqualified because its default-opt native extension fails to load with
`mis-aligned LINKEDIT string pool`; no compiler override or binary repair is used.

## 1,000-row Linux native controls

The [subsequent Linux run](https://github.com/nowledge-co/hawdb/actions/runs/38102022680)
finishes all 480 records at runtime `6ce232ca` against main `3cdb2610`. It
uses the same five fixed cases, both backends, default opt build and seven
measured samples after one discarded sample. Both builds finish before timing.
The comparator measures all baseline children before candidate children; these
are ratios of medians, not alternating paired medians.

The whole comparison fails: all 80 baseline Python records have the same
package/extension import mismatch. Baseline Rust/Go have 160 successful records;
all 240 candidate records succeed. Both reports are terminal. The 20 complete
Rust/Go groups are independent native controls; the 10 Python groups do not
qualify and their failures are retained.

| Layer | Case | Backend | Query-boundary speedup | Write-phase speedup |
| --- | --- | --- | ---: | ---: |
| go | `fill` | file | 2.514x | 2.534x |
| go | `fill` | memory | 4.410x | 4.516x |
| go | `fill_bulk` | file | 1.018x | 1.017x |
| go | `fill_bulk` | memory | 0.999x | 1.002x |
| go | `point` | file | 1.106x | - |
| go | `point` | memory | 1.066x | - |
| go | `select` | file | 1.011x | - |
| go | `select` | memory | 1.003x | - |
| go | `wide` | file | 0.920x | - |
| go | `wide` | memory | 1.058x | - |
| rust | `fill` | file | 2.716x | 2.732x |
| rust | `fill` | memory | 5.317x | 5.434x |
| rust | `fill_bulk` | file | 1.001x | 0.998x |
| rust | `fill_bulk` | memory | 1.005x | 1.007x |
| rust | `point` | file | 1.095x | - |
| rust | `point` | memory | 1.094x | - |
| rust | `select` | file | 1.034x | - |
| rust | `select` | memory | 1.010x | - |
| rust | `wide` | file | 0.969x | - |
| rust | `wide` | memory | 0.965x | - |

The individual-CREATE write-phase ratios of medians are 5.434x/2.732x for
Rust and 4.516x/2.534x for Go (memory/file). Unfavorable wide-read groups and
the 0.998x Rust file bulk-write result remain visible. This small native phase
does not prove large-size scaling, Python parity, retained/Arrow speedups or
whole-operation memory bounds, and cannot complete the full performance gate.

[Complete Linux native-control evidence](evidence/README.md#linux-ordinary-native-controls-20261011)
contains both full reports, every raw stream/hash, reconstructed fixture-hash
checks, copied native-producer checks and recomputed summaries. Original
comparison SHA-256: `85ed8fbf9d8e75cc3c5c0ce72fe10969486cd621cdf9aadbbf3006e15af5057a`.
A new comparison with independently frozen Python runfiles was still required
at this snapshot; the corrected comparison follows below.

## Mixed growth and point-plan controls

Two separate exploratory controls warm an indexed point query at four rows,
grow the same store to 1,000 rows using independent CREATE statements, then
execute 100 exact-checked point calls before force-refresh diagnostics and
another 100 calls afterward. Each control has 32 fresh-process records: raw and
admitted APIs, one discarded and seven alternating measured producer pairs.
All builds/checks finish before timing. Both use default opt builds, unchanged
budgets/durability and the same probe source on main `3cdb2610` and runtime
`6ce232ca`. Exact staged trees, binaries and probe/consumer source snapshots are
in the separate evidence packages.

`rotating` changes bound IDs. The candidate raw cache has one hit and 99 misses
in each measured child; new parameters trigger replanning. `stable` repeats
the initially warmed ID. Candidate raw has 100 hits/zero misses; baseline has
99 hits/one miss. Database counters do not describe admitted private caches.

| Parameters | API | Growth speedup | Hot point speedup | Post-refresh point speedup |
| --- | --- | ---: | ---: | ---: |
| rotating | raw | 2.251x | 1.219x | 1.256x |
| rotating | admitted | 5.060x | 1.099x | 1.070x |
| stable | raw | 4.068x | 0.959x | 1.269x |
| stable | admitted | 5.123x | 1.121x | 1.112x |

Each point metric is the median of 100 calls within a child, then the median
of seven paired baseline/candidate ratios. Growth includes parameter construction
and loop work; it is not the ordinary matrix's write-only boundary. Every value
is checked outside each point timer. All 64 records succeed.

The unfavorable stable raw result remains: prolonged reuse of the initially
warmed plan does not establish read neutrality. Post-hot diagnostics may refresh
statistics and therefore do not reconstruct the plan used during hot timing.
Their plans/profiles describe the raw Database API, not admitted private read
plans. The mixed-plan quality obligation remains open; neither the small hot
speedups nor post-refresh results complete the comprehensive performance gate.

[Rotating-parameter evidence](evidence/README.md#mixed-point-rotating-controls-20261011)
and [stable-parameter evidence](evidence/README.md#mixed-point-stable-controls-20261011)
preserve every individual timing, raw stream/hash, source/artifact guard receipt,
cache counter, diagnostic and exact source snapshot. These shared-host memory
controls do not qualify file backends, large sizes, Arrow, source reuse or
whole-operation memory bounds.

## Corrected 1,000-row Linux Python isolation comparison

The [corrected Linux run](https://github.com/nowledge-co/hawdb/actions/runs/38103880331) succeeds at delivery head `fc6dd487` against main `3cdb2610`. The production runtime is still the CREATE optimization at `6ce232ca`; this run predates the subsequent index-size-class cache fix. All 480 records succeed and all 30 groups qualify: Rust/Python/Go, five cases, both backends, one discarded and seven measured samples. All builds finish before timing. Ordering remains baseline then candidate; these are ratios of medians, not alternating paired medians.

| Layer | Case | Backend | Query-boundary speedup | Write-phase speedup |
| --- | --- | --- | ---: | ---: |
| go | `fill` | file | 2.486x | 2.505x |
| go | `fill` | memory | 4.449x | 4.554x |
| go | `fill_bulk` | file | 1.025x | 1.014x |
| go | `fill_bulk` | memory | 1.037x | 1.023x |
| go | `point` | file | 1.073x | - |
| go | `point` | memory | 1.059x | - |
| go | `select` | file | 1.011x | - |
| go | `select` | memory | 0.981x | - |
| go | `wide` | file | 0.883x | - |
| go | `wide` | memory | 0.917x | - |
| python | `fill` | file | 2.622x | 2.637x |
| python | `fill` | memory | 5.014x | 5.119x |
| python | `fill_bulk` | file | 1.022x | 1.015x |
| python | `fill_bulk` | memory | 1.021x | 1.011x |
| python | `point` | file | 1.073x | - |
| python | `point` | memory | 1.086x | - |
| python | `select` | file | 1.068x | - |
| python | `select` | memory | 1.051x | - |
| python | `wide` | file | 1.086x | - |
| python | `wide` | memory | 1.079x | - |
| rust | `fill` | file | 2.720x | 2.736x |
| rust | `fill` | memory | 5.245x | 5.359x |
| rust | `fill_bulk` | file | 1.000x | 1.003x |
| rust | `fill_bulk` | memory | 1.010x | 1.010x |
| rust | `point` | file | 1.085x | - |
| rust | `point` | memory | 1.088x | - |
| rust | `select` | file | 1.028x | - |
| rust | `select` | memory | 0.998x | - |
| rust | `wide` | file | 1.008x | - |
| rust | `wide` | memory | 1.006x | - |

Python individual-CREATE write-phase ratios are 5.119x (memory) and 2.637x (file). Its point-query ratios are 1.086x/1.073x. The unfavorable Go memory scan (0.981x), Go wide reads (0.917x/0.883x memory/file), and Rust memory scan (0.998x) remain visible. Near-one bulk medians are not a general regression exclusion. Ordinary APIs in this control use no Arrow or retained result APIs. This small phase resolves the Python comparison defect; it does not qualify large sizes, current cache-fix performance or whole-operation memory.

[Complete corrected Linux evidence](evidence/README.md#linux-ordinary-frozen-python-controls-20261011) preserves both full reports and every raw stream/hash. The audit reconstructs all input hashes, checks values/parity/profiles/phases/RSS, compares copied producer bytes and matching Python wrapper/extension pairs, and recomputes all summaries. Complete runfiles identities are checked in the actual CI process before/after both matrices. GitHub artifact transport does not preserve executable modes or symlink types, so the local audit does not claim a transported runfiles identity recomputation. Original comparison SHA-256: `9fdb41b314a39102367bb841ec556e7b3216d2078370a6549d3c8c10abf48fa8`.

## Maintained index-size-class cache fix

The previous stable-parameter raw control exposes prolonged reuse of a four-row scan plan after growth. The cache now includes the existing scalar-index small-label cost class, derived from maintained counts. It replans on the eight-to-nine crossing and on shrinking back, while preserving same-class and unindexed-label reuse. Pure CREATE/UNWIND CREATE bypass this helper. [Source proof obligations](../../docs/READ_FREE_CREATE_STATISTICS.md) separate threshold observation from general optimality or measured performance.

Two three-producer controls compare main `3cdb2610`, the exact frozen negative-control binary from runtime `6ce232ca`, and the fix staged on `fc6dd487` (tree `7b3ac26300af3efc6b848d648877938241e088c4`). The shared default-opt probe, fixed ID, four initial rows, independent growth statements and 100 exact-checked point calls per phase are identical. Producer ordering rotates among three revisions with alternating direction/API order. One discarded and seven measured iterations give 48 records per size; all 96 succeed. All task-owned builds/checks finish before timing; foreign CPU load may remain.

| Rows | API | main / fix growth | main / fix hot point | previous / fix growth | previous / fix hot point | previous / fix post-refresh point |
| ---: | --- | ---: | ---: | ---: | ---: | ---: |
| 1000 | raw | 3.978x | 1.237x | 1.012x | 1.345x | 0.992x |
| 1000 | admitted | 5.255x | 1.115x | 0.977x | 0.992x | 0.980x |
| 10000 | raw | 26.443x | 4.305x | 0.987x | 4.714x | 0.978x |
| 10000 | admitted | 44.640x | 2.652x | 1.010x | 0.989x | 0.991x |

The raw fix has 99 cache hits/one miss in every child at both sizes; the preserved previous binary has 100 hits/zero misses. At 10,000 rows the hot raw paired median is 4.305x versus main and 4.714x versus the previous binary. Admitted private cache counters are not exposed by this probe: Database diagnostics describe the raw API only.

Growth ratios versus the previous binary remain near one. The admitted hot ratios versus the previous binary are slightly unfavorable (0.992x/0.989x at 1,000/10,000 rows); raw post-refresh ratios are also 0.992x/0.978x. All individual timings and these unfavorable comparisons are retained. The helper therefore does not receive a general write/read neutrality claim from this control. Force-refresh diagnostics run after the hot timer and cannot reconstruct the previously cached hot plan.

[Complete index-class control evidence](evidence/README.md#mixed-point-index-class-controls-20261011) includes every native output, individual timing, phase median, raw hash, RSS observation, producer identity, source snapshot and source/artifact guard receipt. Timers exclude exact value checks; growth includes parameter/loop work. This memory-only shared-host control does not qualify file backends, other languages, arbitrary plan distributions, Arrow, source reuse or whole-operation memory. Ordinary full-case controls on the same fixed producer are reported separately.


| Rows | API | First-call fix median | main / fix first call | previous / fix first call | main / fix total 100 calls | previous / fix total 100 calls |
| ---: | --- | ---: | ---: | ---: | ---: | ---: |
| 1000 | raw | 0.454 ms | 0.816x | 0.312x | 1.163x | 1.208x |
| 1000 | admitted | 0.707 ms | 0.903x | 0.997x | 1.087x | 0.993x |
| 10000 | raw | 3.174 ms | 0.841x | 0.133x | 2.415x | 2.235x |
| 10000 | admitted | 5.745 ms | 0.871x | 0.936x | 1.729x | 0.964x |

The first hot raw call includes the normal cache miss and statistics refresh. It is unfavorable versus main (0.816x/0.841x at 1,000/10,000 rows) and especially versus the old cache-hit producer (0.312x/0.133x). Thus the hot median must not be presented as isolated first-point improvement. Including all 100 timed calls, raw paired medians remain 1.163x/2.415x versus main and 1.208x/2.235x versus the old producer. Admitted totals versus the old producer are unfavorable at 0.993x/0.964x. First-call, total and nearest-rank p95/p99 calculations, including every pair, are included in the same evidence package. The first-call and representative mixed-workload obligations remain open.

## Ordinary Rust controls on the index-class fix

After both default-opt builds finish, a separate full ordinary control freezes the same candidate staged tree `7b3ac26300af3efc6b848d648877938241e088c4` and a main `3cdb2610` adapter tree with the identical current harness. It uses all five fixed cases, both backends, 1,000/10,000 rows, one discarded and seven alternating measured producer pairs. Parameters, seeds, calls, budgets and `SyncOnEveryWrite` are unchanged. No Arrow/retained API or allocation instrumentation is enabled.

| Rows | Case | Backend | Query-boundary speedup | Write-phase speedup |
| ---: | --- | --- | ---: | ---: |
| 1000 | `select` | memory | 0.983x | - |
| 1000 | `select` | file | 0.976x | - |
| 1000 | `point` | memory | 1.143x | - |
| 1000 | `point` | file | 1.134x | - |
| 1000 | `fill` | memory | 5.183x | 5.325x |
| 1000 | `fill` | file | 1.083x | 1.083x |
| 1000 | `fill_bulk` | memory | 1.004x | 1.017x |
| 1000 | `fill_bulk` | file | 1.015x | 1.021x |
| 1000 | `wide` | memory | 1.025x | - |
| 1000 | `wide` | file | 1.018x | - |
| 10000 | `select` | memory | 0.976x | - |
| 10000 | `select` | file | 0.974x | - |
| 10000 | `point` | memory | 2.949x | - |
| 10000 | `point` | file | 2.540x | - |
| 10000 | `fill` | memory | 51.913x | 53.229x |
| 10000 | `fill` | file | 2.076x | 2.078x |
| 10000 | `fill_bulk` | memory | 0.997x | 0.999x |
| 10000 | `fill_bulk` | file | 1.003x | 1.004x |
| 10000 | `wide` | memory | Refused before timing | - |
| 10000 | `wide` | file | Refused before timing | - |

All 320 expected records are present: 288 successes and the same 32 wide warmup refusals at 10,000 rows. Each refusal reports 16,780,548 result-materialization bytes against the unchanged 16,777,216-byte account. Thus the whole matrix qualification flag remains false; there is no successful speedup claim for those groups.

At 10,000 rows the point medians are 2.949x/2.540x and individual-CREATE write-phase medians are 53.229x/2.078x (memory/file). Both sizes have unfavorable scan medians, about 0.974x-0.983x. The 10,000-row memory bulk median is also unfavorable at 0.997x query-boundary and 0.999x write-boundary. These are complete shared-host controls against main, not isolated cache-change causality or comprehensive acceptance.

[Complete ordinary index-class evidence](evidence/README.md#rust-ordinary-index-class-controls-20261011) includes every raw native outcome/hash, all paired ratios, fixture checks and consumer/driver snapshots, source/artifact guards and recomputed summaries. Original report SHA-256: `ffd7fdd4e9291bdb5bc3d8b70ef5b22ba4af8f5295a3826526757324585dde9b`. This control does not establish idle-host guarantees, larger sizes, cross-language neutrality, Arrow/source reuse or whole-operation memory bounds.


## Linux index-class control at 94503681

[The completed Linux run](https://github.com/nowledge-co/hawdb/actions/runs/38107881978)
compares frozen ordinary Rust/Python/Go producers at candidate
`94503681bca75f9a39cc72199525993fd3fe03b6`, before the subsequent bounded
query-local point-statistics change. It retains all five cases, both backends,
1,000 rows, one discarded iteration and seven measured iterations. All 480
records and 30 groups succeed. Baseline-first ratios of medians are not
alternating paired observations and do not establish change causality.

| Case | Rust memory | Rust file | Python memory | Python file | Go memory | Go file |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| select | 0.991x | 1.033x | 1.047x | 1.105x | 1.041x | 1.039x |
| point | 1.095x | 1.100x | 1.097x | 1.095x | 1.066x | 1.073x |
| fill | 5.286x | 2.582x | 5.082x | 2.610x | 4.439x | 2.480x |
| fill_bulk | 0.999x | 0.998x | 1.019x | 1.030x | 1.007x | 0.989x |
| wide | 1.036x | 1.103x | 1.106x | 1.144x | 1.051x | 0.976x |

These are query-boundary ratios; separate write-phase ratios, complete raw
records and all unfavorable samples remain in the
[complete evidence](evidence/README.md#linux-ordinary-index-class-controls-20261011).
The package SHA-256 is
`5a3cab91122fcdad3dfb4f5196a5f3005b443ec95d33920b3a15e6c2ab4ac570`.
Its audit reconstructs every fixture, checks every native result, checksum,
phase sum and RSS receipt, validates native artifact bytes and frozen Python
wrapper/extension/manifest pairs, and recomputes every comparison summary.
GitHub artifact transport does not preserve executable modes and symlink types;
the local audit does not claim to reconstruct the complete transported runfiles
identity. CI checked the original identity before and after the matrices.

All five automatic correctness checks at 94503681 also succeed. This qualifies
that revision's recorded small Linux control only. First-point behavior after
mixed growth, large sizes, allocation/foreign-heap and whole-operation memory
acceptance remain open. The run does not measure Arrow, retained delivery or the
subsequent query-local statistics implementation. Go file wide, Go file bulk,
Rust memory scan and Rust bulk have unfavorable ratios; they are not hidden or
counted as comprehensive performance success.


## Bounded query-local point statistics

This control measures candidate source tree `4eb69e483e3a1dbdec127726746f20d47170d70d`, based on `94503681`, after replacing the full-statistics requirement for eligible simple indexed equality reads with a bounded snapshot-local candidate count. It preserves default opt, the memory backend, fixed query parameters, unchanged durability and 100 point calls after growth from four rows. Ordinary row APIs are used; no Arrow or retained delivery is timed.

The three frozen producers are main `3cdb2610`, the previous immutable index-class-control producer and the query-local-statistics candidate. Previous-control production source is identical to `94503681`; its original binary is retained. All 96 records complete, with one discarded and seven measured rotating producer pairs for each API/size. All owned builds/checks finish before timing; this remains a shared macOS host. Ratios below one are unfavorable.

| Rows | API | Candidate first-call median | previous / candidate first | previous / candidate total 100 | main / candidate total 100 | previous / candidate hot median |
| ---: | --- | ---: | ---: | ---: | ---: | ---: |
| 1000 | raw | 0.126375 ms | 2.806x | 1.073x | 1.315x | 0.992x |
| 1000 | admitted | 0.182584 ms | 3.775x | 1.069x | 1.181x | 0.981x |
| 10000 | raw | 0.148042 ms | 23.231x | 2.169x | 4.822x | 0.993x |
| 10000 | admitted | 0.202875 ms | 27.672x | 1.954x | 3.159x | 0.973x |

At 10,000 rows, first-point paired medians improve 23.231x raw and 27.672x admitted versus the previous producer; all 100 timed calls improve 2.169x/1.954x. Candidate first-call medians are 0.148042/0.202875 ms. The first call remains roughly constant between the two measured sizes, while the previous control paid a full-statistics refresh. These measurements do not prove asymptotic behavior for unmeasured sizes.

| Rows | API | previous / candidate growth | previous / candidate hot p95 | previous / candidate hot p99 | previous / candidate post-refresh median |
| ---: | --- | ---: | ---: | ---: | ---: |
| 1000 | raw | 1.007x | 0.977x | 0.893x | 0.986x |
| 1000 | admitted | 1.003x | 0.953x | 0.986x | 0.985x |
| 10000 | raw | 1.012x | 0.923x | 1.199x | 0.986x |
| 10000 | admitted | 1.003x | 0.944x | 0.916x | 0.978x |

Hot medians versus the previous producer are about 0.8-2.8% unfavorable, and several p95/p99 comparisons are unfavorable. Every sample remains visible. Raw cache counters report 99 hits/one miss; admitted private-cache behavior is not inferred from raw Database diagnostics. Force-refresh diagnostics run after the hot timer and cannot reconstruct its plan. Growth stays close to the previous producer; main-to-candidate growth improvements include earlier CREATE work and are not isolated causality for this point change.

[Complete mixed-growth evidence](evidence/README.md#mixed-point-local-statistics-controls-20261011) preserves each native result/raw stream, all 100 individual timings per phase, first/total/nearest-rank-tail calculations, RSS, source snapshots, frozen producer/consumer identities and before/after guards. Package SHA-256: `602aba2343b335dc9ed9c31b7bf0dfa726cf483b6e1c2dec6f1ede5e9d53e657`. Auditing verifies record sequence, parameter/default checks, native output and each derived summary.

This control does not qualify file backends, other languages, dense/complex read distributions, source-to-host zero copy, larger sizes, foreign heap or whole-operation memory. The bounded probe visits at most nine borrowed candidates and publishes no shared advanced statistics, but ordinary execution/result ownership retains its existing behavior. Comprehensive performance acceptance remains open.

## Ordinary Rust controls on query-local point statistics

The default-opt ordinary control freezes the same candidate source tree `4eb69e483e3a1dbdec127726746f20d47170d70d` and main `3cdb2610` with the identical untimed adapter/harness. All five cases, both backends and 1,000/10,000 rows retain their fixtures, seeds, calls, budgets, `SyncOnEveryWrite` and zero prefetch. One iteration is discarded and seven alternating producer pairs are measured. Owned builds/checks finish before timing. No Arrow/retained API or allocation instrumentation is enabled.

| Rows | Case | Backend | Query-boundary speedup | Write-phase speedup | Read-phase speedup |
| ---: | --- | --- | ---: | ---: | ---: |
| 1000 | `select` | memory | 0.923x | - | 0.923x |
| 1000 | `select` | file | 0.999x | - | 0.999x |
| 1000 | `point` | memory | 1.109x | - | 1.109x |
| 1000 | `point` | file | 1.103x | - | 1.103x |
| 1000 | `fill` | memory | 5.768x | 5.919x | 0.906x |
| 1000 | `fill` | file | 0.987x | 0.987x | 0.934x |
| 1000 | `fill_bulk` | memory | 0.991x | 0.982x | 1.006x |
| 1000 | `fill_bulk` | file | 1.041x | 0.976x | 0.967x |
| 1000 | `wide` | memory | 1.026x | - | 1.026x |
| 1000 | `wide` | file | 0.996x | - | 0.996x |
| 10000 | `select` | memory | 0.999x | - | 0.999x |
| 10000 | `select` | file | 1.012x | - | 1.012x |
| 10000 | `point` | memory | 2.512x | - | 2.512x |
| 10000 | `point` | file | 2.482x | - | 2.482x |
| 10000 | `fill` | memory | 53.614x | 55.075x | 0.938x |
| 10000 | `fill` | file | 1.934x | 1.935x | 0.861x |
| 10000 | `fill_bulk` | memory | 0.999x | 0.996x | 1.027x |
| 10000 | `fill_bulk` | file | 1.087x | 1.091x | 1.044x |
| 10000 | `wide` | memory | Refused before timing | Refused before timing | Refused before timing |
| 10000 | `wide` | file | Refused before timing | Refused before timing | Refused before timing |

All 320 expected records are present: 288 successes and 32 identical 10,000-row wide warmup refusals. Both producers refuse 16,780,548 result-materialization bytes against the unchanged 16,777,216-byte account. The driver exits 1 and the whole-matrix qualification flag is false; the terminal raw-data audit succeeds. Those groups have no successful timing or speedup claim.

At 10,000 rows point paired medians are 2.512x/2.482x and independent-CREATE write-phase medians are 55.075x/1.935x (memory/file). Main-to-candidate improvements include earlier projected-seek/CREATE work and do not isolate this point-statistics change. The 1,000-row memory scan is unfavorable at 0.923x, file individual-CREATE write at 0.987x, and both 1,000-row bulk write medians at 0.982x/0.976x. The 10,000-row memory bulk write median is 0.996x. Several final verification-read medians are unfavorable even where total/write medians improve. No unfavorable sample is dropped or explained away as proven noise.

[Complete ordinary point-statistics evidence](evidence/README.md#rust-ordinary-local-statistics-controls-20261011) preserves native stdout/stderr, individual pairs, source snapshots, frozen driver/consumers, fixtures, producer hashes, process/RSS observations and all recalculated summaries. Original report SHA-256: `3f20ae6b396b853d9d18174b0f5bbdf057e09ec789aefa84c6b5a151dba25027`; package SHA-256: `9ca7b5122b09bee9cc12b0d5f7f511931467700756ff2dce27b156379ed78248`. Source/artifact guards pass before and after children; the independent audit verifies native results, checksums, profile/defaults, phase sums, discard/order policy, RSS and both producer medians and paired ratios.

This remains a small shared-host Rust control. It does not qualify large sizes, cross-language neutrality, Arrow/source reuse or whole-operation memory. Together with the mixed-growth cold gains and unfavorable hot/tail samples, it leaves comprehensive performance acceptance open.

## Integration after the frozen point controls

The measured producer above is production-identical to `b0ae19bd`. The later
integration commit `1d274ccf` merges main `f58062e5` ([#998](https://github.com/nowledge-co/hawdb/pull/998))
and preserves both the projected index fast path and source/retained memory
budget separation. Its updated regression permits a 128 KiB selected source
above a 4 KiB blocking-state cap, verifies its query charge is released, and
then proves a 4 KiB query cap refuses before residual evaluation. The first
integration run exposed the earlier fixture's blocking-cap assumption; that
diagnostic failure is distinct from the corrected contract.

Fixed-source integration validation passes all 100 targets: executor unit and
projection-allocation tests, root general and analytics partitions, and the
required local fuzz/smoke targets. Ten targets execute and 90 are cached. Root
general reports 1,602 passed/1 ignored, analytics 62 passed/3 ignored, executor
unit 430 passed/16 ignored and projection allocation 105 passed. Final
formatting, workspace strict Clippy, minimal-WASM strict Clippy, prek and commit
hooks pass. The first diagnostic run's input-change warnings are not used as
final verification; the fixed-source invocation has no such warnings.

Main subsequently bumps package/build versions to 0.7.0 at `77732a77`
([#1003](https://github.com/nowledge-co/hawdb/pull/1003)). That mechanical
integration preserves every Rust source byte from `1d274ccf`; final versioned
build/lint and focused point checks are tracked separately in the PR.
Neither merge retroactively qualifies the earlier producer's performance for
the final head. New-head cross-language, representative performance and
whole-operation memory acceptance remain open.

## Borrowed point metadata controls (2026-10-11)

The before producer is `71e2b538065aa4f9435cf0e9843ed93a61bf24f2`, tree
`00453de6d8e2d70c431302dbd949bd0a57a46327`. The after producer is staged tree
`261394366bbb5469ca8c10149297d8f810d84cc2` on that head. Its only production
change borrows temporary point-statistics names from the logical plan while
preserving owned `(label, property, K, B)` cache identity. The source archives,
native artifacts, unchanged harness and drivers are frozen independently.
Both producers use default `bazel build -c opt`; no compiler, allocator or
budget override is applied.

All 128 uninstrumented mixed-growth records succeed: 1,000/10,000 rows,
raw/admitted APIs and stable/rotating parameters, with one discarded pair and
seven measured pairs per group. Before/after order alternates. Each phase has
100 separately timed, verified point calls after growth from four rows.
The table gives medians of paired before/after ratios; values below 1 are
unfavorable to the candidate. These are exploratory measurements on a shared
host, without an idle-host claim.

| Rows | API | Parameters | First call | Total 100 calls | Hot median | p95 | p99 | Refreshed median |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 1000 | raw | stable | 0.934x | 1.008x | 1.005x | 1.006x | 0.935x | 0.986x |
| 1000 | raw | rotating | 0.951x | 1.021x | 1.045x | 0.956x | 1.003x | 0.985x |
| 1000 | admitted | stable | 1.011x | 0.967x | 0.991x | 0.822x | 0.969x | 0.989x |
| 1000 | admitted | rotating | 0.995x | 1.014x | 0.917x | 1.028x | 1.032x | 0.981x |
| 10000 | raw | stable | 0.949x | 1.081x | 1.082x | 1.271x | 1.471x | 1.002x |
| 10000 | raw | rotating | 0.940x | 0.813x | 0.895x | 0.623x | 0.999x | 0.969x |
| 10000 | admitted | stable | 1.050x | 1.069x | 1.054x | 1.069x | 1.015x | 1.030x |
| 10000 | admitted | rotating | 1.040x | 1.011x | 1.052x | 1.011x | 1.318x | 1.058x |

Separate existing allocation-profiling binaries first produce 16 successful
controls. Their before/after file paths differ in length, affecting allocation
byte and live/peak observations. Those records remain preserved, but their
byte deltas are not used as optimization evidence. A corrective control uses
equal-length producer, job and database paths, with three alternating
repetitions per workload: all 48 records succeed and reproduce the same deltas.

| Rows | Backend | Before allocation calls | After allocation calls | Fewer calls | Fewer requested bytes |
| --- | --- | --- | --- | --- | --- |
| 1000 | memory | 1,950,001 | 1,946,001 | 4,000 | 20,000 |
| 1000 | file | 1,960,001 | 1,956,001 | 4,000 | 20,000 |
| 10000 | memory | 19,500,001 | 19,460,001 | 40,000 | 200,000 |
| 10000 | file | 19,600,001 | 19,560,001 | 40,000 | 200,000 |

These admitted point controls save four native allocation calls and 20 requested
bytes per query; the implementation removes two temporary strings per eligible
planning call. The 1,000-row scan, individual CREATE, bulk CREATE and wide-read
controls have identical allocation-call/byte counts in all three repetitions.
Reallocation counts, completion live bytes and process peak requested bytes are
identical for every corrected pair. There is no peak-memory reduction claim.
Native requested traffic excludes foreign heaps and allocator rounding, and
instrumented timings are not ordinary latency qualification. The point traffic
remains approximately 1,946/1,956 allocation calls per admitted query; this small
metadata improvement does not establish broad execution-buffer or memory gains.

[Complete metadata control evidence](evidence/README.md#point-statistics-borrowed-metadata-controls-20261011)
preserves all 192 records, raw native streams, per-call timings, fixtures,
producer/source/consumer identities, both drivers, path-confounded observations
and corrected repetitions. Every raw native result, checksum, fixture, timing
phase, RSS observation, percentile and paired summary is independently audited.
Package SHA-256: `ff31a916efb3a160444e3e73a2a7f4c04b4e39c0a591042a7e478f3dbf93adde`.
This proves the bounded allocation reduction, without a consistent latency or
whole-operation memory improvement. The comprehensive performance gate stays
open; no unfavorable latency group is removed or labeled proven noise.

On the same production source, default Bazel general/fuzz verification passes
97/97 targets (23 executed/74 cached). Root general reports 1,603 passed and one
ignored, including all five point-statistics regressions. The added regression
checks actual borrowed names and owned cache identity after the plan is dropped,
with distinct labels/properties at identical cardinality and access-cost class.
Pinned-toolchain formatting, strict workspace/all-target/all-feature Clippy,
minimal-WASM Clippy and prek pass. Documentation and evidence added after timing
do not change the measured Rust production source.

The earlier `71e2b538` five platform/binding checks pass, including installed
wheel tests (159 passed/7 skipped) and pydantic (43 passed/1 skipped). Those checks
precede this ownership refinement. Its manual Linux comparison
[run 38114750497](https://github.com/nowledge-co/hawdb/actions/runs/38114750497)
is still in progress at publication; it must not qualify later source.
The earlier [b0ae19bd run](https://github.com/nowledge-co/hawdb/actions/runs/38112390209)
is cancelled during baseline. Its preserved artifact contains 345 successful
baseline records and no candidate report; raw results are checked but no paired
comparison or performance qualification exists.


## Shared admitted planning templates (2026-10-11)

The [audited evidence package](evidence/README.md#shared-admission-template-controls-20261011) has SHA-256
`b84f70d0a13c190681f7b2f4239fd47d5094182706198cd8ff462c70dd759518`. The before producer is `a58cdf715547734d012376ad0f0715bc5b9c0e0f`,
tree `19d7da21050d93b67f39027bb0ae3e4e58563392`; the after producer is staged tree
`08c9304b4b75865c25878c97a2a9824b9fd68d5b` on that head. Both default-opt native producers,
their source archives, unchanged harnesses, consumer and drivers are frozen.
The delivery adds this document and evidence only after timing and verifies
that every measured Rust source and harness file remains unchanged.

Synchronous admitted reads now share the runtime's template and optimizer
planning caches between admission and execution. Entries use immutable Arc
ownership, so lookup avoids copying a whole template before the execution-owned
binding copy. Public read transactions retain their private caches. All key,
snapshot, semantic-binding, result-budget and resource-permit checks remain.
CurrentTimestamp-containing reads bypass templates recursively; parameterized
timestamp values remain eligible. This is planning reuse on ordinary row APIs.
It is not Arrow source-to-host zero copy or comprehensive performance acceptance.

All 128 uninstrumented mixed-growth records succeed. Each group has one
discarded iteration and seven alternating measured pairs; each hot/refreshed
phase has 100 separately timed and value-verified calls after growth from four
rows. Values below 1 in this table are unfavorable to the candidate. These are
shared-host measurements; process snapshots preserve concurrent activity.

| Rows | API | Parameters | First call | Total 100 calls | Hot median | p95 | p99 | Refreshed median |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 1000 | raw | stable | 1.030x | 1.050x | 1.036x | 1.072x | 1.016x | 1.003x |
| 1000 | raw | rotating | 1.047x | 0.936x | 0.994x | 0.858x | 1.060x | 1.061x |
| 1000 | admitted | stable | 1.091x | 1.365x | 1.357x | 1.406x | 1.468x | 1.356x |
| 1000 | admitted | rotating | 1.034x | 1.106x | 1.110x | 1.170x | 1.227x | 0.952x |
| 10000 | raw | stable | 0.893x | 1.145x | 1.059x | 0.919x | 1.416x | 1.047x |
| 10000 | raw | rotating | 1.080x | 1.201x | 1.011x | 2.126x | 1.864x | 0.972x |
| 10000 | admitted | stable | 1.080x | 1.337x | 1.323x | 1.310x | 1.224x | 1.322x |
| 10000 | admitted | rotating | 1.114x | 0.580x | 1.072x | 0.880x | 0.507x | 0.946x |

Stable admitted hot medians improve, but rotating and raw controls still have
unfavorable first, total, tail or refreshed observations. In particular the
10,000-row admitted rotating total and p99 regress. No unfavorable sample is
removed or treated as proven noise. The unchanged harness retains historical
private-cache labels: candidate admitted counters now describe its shared
runtime cache. Forced diagnostics remain untimed and do not reconstruct the
hot-call plans. The package records that scope correction explicitly.

All 48 instrumented allocation controls succeed, using equal-length binary,
job and database paths and three alternating repetitions per workload. Counts
and requested bytes describe the measured operation; the process peak includes
setup. Instrumented timings are separate from ordinary latency.

| Rows | Backend | Before calls | After calls | Fewer calls | Fewer requested bytes | Additional live bytes | Peak requested change |
| --- | --- | --- | --- | --- | --- | --- | --- |
| 1000 | memory | 1,946,001 | 1,397,001 | 549,000 | 93,228,000 | 9,153 | 0 |
| 10000 | memory | 19,460,001 | 13,970,001 | 5,490,000 | 932,400,000 | 9,159 | 0 |
| 1000 | file | 1,956,001 | 1,407,001 | 549,000 | 93,228,000 | 9,153 | 0 |
| 10000 | file | 19,560,001 | 14,070,001 | 5,490,000 | 932,400,000 | 9,159 | 0 |

Each admitted point query removes 549 native allocation calls. Requested
traffic falls by 93,228 bytes/query at 1,000 rows and 93,240 at 10,000 rows;
cache residency increases by 9,153/9,159 bytes respectively, both before and
after the measured operation. Point process peak requested bytes are unchanged,
as setup dominates. Scan, individual-write, bulk-write and wide-read traffic
also falls in these controls, with additional completion cache residency.
These counters exclude foreign heaps and allocator rounding; fewer requests
are not a proof of lower peak/RSS or a byte-bounded cache. Default cache capacity
remains 128 entries, without a new template-byte cap.

The ordinary Rust matrix preserves all 320 records: five cases, memory/file,
1,000/10,000 rows, one discarded plus seven alternating measured pairs. It uses
ordinary row APIs, SyncOnEveryWrite and zero prefetch. Successful records verify
checksums, output counts, absent allocation instrumentation and phase sums.
The table reports paired baseline/candidate medians; values below 1 regress.

| Rows | Case | Backend | Query boundary | Write boundary | Read boundary | Total elapsed |
| --- | --- | --- | --- | --- | --- | --- |
| 1000 | select | memory | 0.933x | n/a | 0.933x | 0.929x |
| 1000 | select | file | 1.100x | n/a | 1.100x | 1.095x |
| 1000 | point | memory | 1.417x | n/a | 1.417x | 1.415x |
| 1000 | point | file | 1.309x | n/a | 1.309x | 1.306x |
| 1000 | fill | memory | 1.074x | 1.072x | 1.182x | 1.074x |
| 1000 | fill | file | 1.022x | 1.022x | 1.029x | 1.022x |
| 1000 | fill_bulk | memory | 0.999x | 0.977x | 1.140x | 1.001x |
| 1000 | fill_bulk | file | 1.000x | 0.991x | 1.154x | 0.999x |
| 1000 | wide | memory | 0.958x | n/a | 0.958x | 0.960x |
| 1000 | wide | file | 1.103x | n/a | 1.103x | 1.114x |
| 10000 | select | memory | 1.022x | n/a | 1.022x | 1.001x |
| 10000 | select | file | 1.174x | n/a | 1.174x | 1.165x |
| 10000 | point | memory | 1.392x | n/a | 1.392x | 1.390x |
| 10000 | point | file | 1.298x | n/a | 1.298x | 1.298x |
| 10000 | fill | memory | 1.037x | 1.014x | 1.266x | 1.037x |
| 10000 | fill | file | 0.993x | 0.992x | 1.076x | 0.993x |
| 10000 | fill_bulk | memory | 1.107x | 1.101x | 1.236x | 1.107x |
| 10000 | fill_bulk | file | 1.011x | 0.992x | 1.539x | 1.010x |
| 10000 | wide | memory | refused | refused | refused | refused |
| 10000 | wide | file | refused | refused | refused | refused |

Ordinary status counts: `{"error": 32, "ok": 288}`. The default
10,000-row wide workload refuses during warmup because result_materialization
needs 16,780,548 bytes against the unchanged 16,777,216-byte budget. All 32
refusals and raw errors remain in the package; the full matrix is not successful.
No result limit is raised to make the controls pass. The independent audits
verify ordering/counts, raw native fields/hashes, fixtures/checksums, phase sums,
profiles, RSS extraction, percentile and paired summary math, source archives,
producer/consumer/harness identities and terminal/refusal state.

On final measured Rust source, default Bazel general/fuzz verification passes
97/97 targets (14 executed/83 cached). Root general has 1,608 passed and one
ignored. Focused admitted_sync_ tests pass seven cases; volatile-cache
eligibility passes one test covering 11 volatile and three deterministic forms.
Pinned-toolchain format, strict workspace/all-target/all-feature Clippy,
minimal-WASM Clippy and prek pass. Before committing documentation/evidence,
format and strict native checks are repeated on the delivery tree.

The predecessor a58cdf71 automatic platform/binding checks all pass, including
installed-wheel tests (159 passed/7 skipped), pydantic (43 passed/1 skipped)
and PyArrow interoperability (eight passed). Those tests do not qualify new
source. Current delivery CI must be inspected separately. The older 71e2b538
[manual Linux comparison](https://github.com/nowledge-co/hawdb/actions/runs/38114750497)
cannot qualify this refinement either. It is allowed to finish without a
duplicate dispatch that would cancel it.

The comprehensive gate remains open: unfavorable read/write/tail controls,
larger-size and current-head cross-language/platform verification, template-byte
budgets, foreign heaps and whole-operation memory remain unqualified. This
refinement does not establish Arrow source reuse, graph/FTS or SIMD completion.


## Large Linux run and bounded comparison scheduling

The full ordinary run [38172074248](https://github.com/nowledge-co/hawdb/actions/runs/38172074248)
ended cancelled at its configured six-hour window. Its frozen candidate was
`c2a98e2a`, and its baseline was `3cdb2610`. The preserved archive contains only
156 baseline outcomes in 39 groups: 132 successful outcomes and 24 default
16 MiB result-budget refusals for 100,000-row scans. Reconstructed fixture/job
hashes and all ten frozen producer hashes match. There is no candidate report,
no million-row outcome and no qualified comparison group.

The 100,000-row memory point rounds took 584.948-907.756 seconds on the baseline.
The harness creates an equality index and visits every ID in both warmup and
measurement. Index creation alone does not verify the runtime plan. These raw
outcomes remain baseline-only evidence; no speedup is inferred.

The manual comparison accepts `benchmark_layers` and `benchmark_backends` so
independent groups can finish without sharing a single baseline-first deadline.
Both drivers receive the same selection, and the manifest records it. Defaults
remain all three languages, both backends, all three sizes and all five cases.
A selected group retains the original fixture, all-ID work, one discarded
iteration, measured sample count, parity checks and frozen producer checks.
Distinct manual selections have distinct concurrency groups; a dispatch for
one shard must not cancel another shard. This changes scheduling, not workload.

For example, a 100,000-row Go memory point shard uses:

```sh
gh workflow run native-bindings.yml --repo nowledge-co/hawdb \
  --ref feat/978-boundary-baseline -f boundary_comparison=true \
  -f benchmark_sizes=100000 -f benchmark_cases=point \
  -f benchmark_layers=go -f benchmark_backends=memory -f benchmark_samples=3
```

A completed shard qualifies only its recorded groups. Complete acceptance still
requires all original matrix groups, current source identity, failure/refusal
retention, and the broader zero-copy, read/write, graph/FTS, tail-latency and
whole-operation memory gates. This scheduling change does not activate a
retained point-query implementation or qualify Arrow source reuse.
