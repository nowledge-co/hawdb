# macOS relational access qualification

The unchanged release benchmark at `1a3e532da8da8d6874e3d950eebbcec377f055ce`
completed on October 7, 2026 (Asia/Shanghai). All twenty persisted comparisons
returned complete, identical results. The current cost-selected path wins point
lookups, but warm medium and broad prefixes remain substantially slower than a
scan. This supplies current evidence for
[issue #216](https://github.com/nowledge-co/hawdb/issues/216); context-dependent
calibration and policy qualification remain open.

The [recording](benchmarks/relational_access_macos_2026_10_07.json) retains every
first profile, all eleven warm profiles and samples, the original resident cases,
source and binary identities, and hashes of the raw JSON and stdout. Repeated
warm profile fields are factored into `common` plus eleven `samples`; expanding
those objects reproduces the complete original JSON without losing counters or
timings. The [protocol](RELATIONAL_ACCESS_COST_BENCHMARK.md) owns the fixture and
correctness assertions.

## Environment and method

- Mac15,8, Apple M3 Max, sixteen physical/logical CPUs, 128 GiB memory,
  macOS 27.0.1 build 26A434, aarch64. Fixtures used the default macOS temporary
  directory. Device cache and other host activity were not controlled.
- Pinned Rust/Cargo 1.97.1, unchanged release bench profile, optimization level 3,
  thin LTO, one codegen unit, no debug assertions. Release compilation took
  334.028 seconds, separately from the 35.730-second benchmark execution.
- Other owned heavy verification completed before measurement. All 1,840 tracked
  source/build hashes remained unchanged. There were no coefficient, feature,
  storage, cache-capacity, workload or assertion changes.
- Each fixture held 8,192 rows and a 64 MiB HawDB cache: body widths 64/1,024,
  clustered/dispersed layouts, and point/sparse/medium/broad/all-prefix shapes.
  All 480 persisted query executions and four original resident cases passed
  their complete ID/payload oracles, full-consumption and operator assertions.
- Each first query starts from a new HawDB handle, not a demonstrated cold
  physical device. Warm queries reuse that handle and its SQL template; profiles
  report no canonical-row or index file pages for those queries.

Reproduce with the default benchmark and pinned repository toolchain:

```sh
cargo bench --locked --bench relational_index_access
```

The `cost_selected` arm follows actual access selection: point uses primary-key
lookup, sparse/medium/broad prefixes use a non-covering secondary index, and
all-prefix uses a full scan. It is not a forced-index arm. Timing below covers
execution only; open, parse, bind, plan and result validation are excluded and
retained separately in the recording. First values are individual samples;
warm values are the median of eleven samples. The nearest-rank p95 and p99 are
both the maximum of eleven samples, not stable tail-latency estimates.

| Body bytes | Layout | Shape | Matching rows | Selected estimate | First scan ms | First selected ms | Warm scan ms | Warm selected ms |
| ---: | --- | --- | ---: | ---: | ---: | ---: | ---: | ---: |
| 64 | clustered | point | 1 | 1 | 39.697 | 15.903 | 0.707 | 0.007 |
| 64 | clustered | sparse prefix | 81 | 2,731 | 39.525 | 16.820 | 0.729 | 0.928 |
| 64 | clustered | medium prefix | 738 | 2,731 | 39.410 | 25.440 | 0.761 | 7.471 |
| 64 | clustered | broad prefix | 7,373 | 2,731 | 39.758 | 102.770 | 1.001 | 67.033 |
| 64 | clustered | all rows prefix | 8,192 | 8,192 | 39.828 | 39.700 | 1.031 | 0.978 |
| 64 | dispersed | point | 1 | 1 | 39.531 | 15.817 | 0.698 | 0.007 |
| 64 | dispersed | sparse prefix | 81 | 2,731 | 39.404 | 39.448 | 0.730 | 0.743 |
| 64 | dispersed | medium prefix | 738 | 2,731 | 39.484 | 45.304 | 0.787 | 6.628 |
| 64 | dispersed | broad prefix | 7,373 | 2,731 | 40.041 | 104.857 | 0.957 | 66.700 |
| 64 | dispersed | all rows prefix | 8,192 | 8,192 | 39.873 | 39.791 | 1.038 | 1.036 |
| 1,024 | clustered | point | 1 | 1 | 47.535 | 18.248 | 1.060 | 0.009 |
| 1,024 | clustered | sparse prefix | 81 | 2,731 | 47.294 | 19.330 | 1.057 | 1.017 |
| 1,024 | clustered | medium prefix | 738 | 2,731 | 47.232 | 29.113 | 1.111 | 8.014 |
| 1,024 | clustered | broad prefix | 7,373 | 2,731 | 48.080 | 118.295 | 1.561 | 73.369 |
| 1,024 | clustered | all rows prefix | 8,192 | 8,192 | 48.071 | 47.766 | 1.617 | 1.639 |
| 1,024 | dispersed | point | 1 | 1 | 47.923 | 18.514 | 1.072 | 0.010 |
| 1,024 | dispersed | sparse prefix | 81 | 2,731 | 47.926 | 47.866 | 1.101 | 0.879 |
| 1,024 | dispersed | medium prefix | 738 | 2,731 | 47.552 | 53.982 | 1.181 | 7.432 |
| 1,024 | dispersed | broad prefix | 7,373 | 2,731 | 47.560 | 120.018 | 1.589 | 74.774 |
| 1,024 | dispersed | all rows prefix | 8,192 | 8,192 | 47.575 | 47.699 | 1.592 | 1.669 |

## Consequences for the cost contract

Current logical weights are CPU 1, random access 2, sequential access 1 and
output 1. Descriptor-aware costing already charges non-covering row fetches,
so duplicating that implementation would not resolve these observations.

Fresh nonresident prefix statistics estimate all three bucket values at 2,731
rows from the NDV average, while actual cardinalities are 81, 738 and 7,373.
The current estimator loses value-specific skew. Warm medium prefixes cost
6.29–9.82 times the scan execution time, and warm broad prefixes 47.00–69.72
times. Point lookup wins strongly. First-handle clustered sparse/medium prefixes
can favor the index, and the 1,024-byte dispersed sparse warm index also wins;
an unconditional scan policy would regress measured cases.

The row-read reports expose work beyond file I/O. For the 64-byte clustered
broad prefix, a warm scan visits 8,192 rows through 32 logical pages and 32
descriptor reads. The selected index visits 7,373 rows through 7,373 logical
pages and 30,465 descriptor reads. Both report zero file pages. Repeated
point-fetch work persists with cached pages, so global file-I/O weights alone
cannot explain or correct the selection. The existing snapshot multi-point
reader shares selected page reads, but the base index stream currently fetches
one point at a time; any reuse must preserve ordering, stop behavior, cumulative
budgets, snapshot precedence and cancellation.

A further correction must carry its assumptions consistently through candidate
costing, skyline pruning, memo composition, physical plans and profiles. It must
qualify regressions as well as wins and avoid inferring query-specific residency
from a global cache-hit counter. This recording does not establish a universal
hardware weight, a cache-aware runtime policy, a storage-format change, browser
execution, Linux parity, or full issue completion. The older
[Linux baseline](RELATIONAL_ACCESS_COST_LINUX_CALIBRATION.md) uses a different
source revision and machine and is not a controlled before/after comparison.
