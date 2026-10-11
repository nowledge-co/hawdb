# Current-main ordinary Rust controls

This frozen comparison supports the ordinary API control for
[PR #986](https://github.com/nowledge-co/hawdb/pull/986),
[issue #976](https://github.com/nowledge-co/hawdb/issues/976) and
[the projected point-seek issue](https://github.com/nowledge-co/hawdb/issues/992).
It does not complete the comprehensive performance gate.

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

[Complete evidence JSON](evidence/rust_ordinary_controls_20261011.json) preserves
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

[Complete shared-cache control evidence](evidence/rust_ordinary_shared_cache_controls_20261011.json)
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

[Complete CREATE-control evidence](evidence/rust_ordinary_create_controls_20261011.json)
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

[Complete source-capacity evidence](evidence/rust_source_capacity_controls_20261011.json)
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

[Preserved partial evidence](evidence/linux_boundary_timeout_20261011.json)
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

[Complete Linux native-control evidence](evidence/linux_ordinary_native_controls_20261011.json)
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

[Rotating-parameter evidence](evidence/mixed_point_rotating_controls_20261011.json)
and [stable-parameter evidence](evidence/mixed_point_stable_controls_20261011.json)
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

[Complete corrected Linux evidence](evidence/linux_ordinary_frozen_python_controls_20261011.json) preserves both full reports and every raw stream/hash. The audit reconstructs all input hashes, checks values/parity/profiles/phases/RSS, compares copied producer bytes and matching Python wrapper/extension pairs, and recomputes all summaries. Complete runfiles identities are checked in the actual CI process before/after both matrices. GitHub artifact transport does not preserve executable modes or symlink types, so the local audit does not claim a transported runfiles identity recomputation. Original comparison SHA-256: `9fdb41b314a39102367bb841ec556e7b3216d2078370a6549d3c8c10abf48fa8`.

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

[Complete index-class control evidence](evidence/mixed_point_index_class_controls_20261011.json) includes every native output, individual timing, phase median, raw hash, RSS observation, producer identity, source snapshot and source/artifact guard receipt. Timers exclude exact value checks; growth includes parameter/loop work. This memory-only shared-host control does not qualify file backends, other languages, arbitrary plan distributions, Arrow, source reuse or whole-operation memory. Ordinary full-case controls on the same fixed producer are reported separately.


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

[Complete ordinary index-class evidence](evidence/rust_ordinary_index_class_controls_20261011.json) includes every raw native outcome/hash, all paired ratios, fixture checks and consumer/driver snapshots, source/artifact guards and recomputed summaries. Original report SHA-256: `ffd7fdd4e9291bdb5bc3d8b70ef5b22ba4af8f5295a3826526757324585dde9b`. This control does not establish idle-host guarantees, larger sizes, cross-language neutrality, Arrow/source reuse or whole-operation memory bounds.
