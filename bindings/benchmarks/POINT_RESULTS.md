# Projected point seek measurements

Issue Number: ref [#992](https://github.com/nowledge-co/hawdb/issues/992),
ref [#976](https://github.com/nowledge-co/hawdb/issues/976),
ref [#987](https://github.com/nowledge-co/hawdb/issues/987)

The ordinary point benchmark already selects `IndexNodeSeek`. Its materialized
projection visitor nevertheless clones every labeled node. The follow-up uses
the existing scalar index and admits only selected fields of matching nodes.
The path and correctness obligations are in
[PROJECTED_POINT_SEEK.md](../../docs/PROJECTED_POINT_SEEK.md).

All four terminal reports, binary hashes, source identities, individual samples,
errors and checksums are preserved in
[owned-point-observations.json](owned-point-observations.json). The ordinary
before and after matrices each contain 120 successful records: five cases,
three languages, two backends, one discarded iteration and three measured
iterations. Both independent allocation/profile matrices contain 12 successful
point records. No fixture, call count, resource allowance, fsync policy or
benchmark deadline was weakened.

## Point results

Each point workload performs 1,000 distinct parameterized queries against
1,000 nodes, with an identical ordered typed checksum. Query-boundary medians
in milliseconds:

| Backend | Rust before / after | Python before / after | Go before / after |
| --- | ---: | ---: | ---: |
| memory | 306.239 / 59.050 | 345.460 / 63.132 | 404.213 / 61.336 |
| file | 248.215 / 65.303 | 302.715 / 59.462 | 269.161 / 73.077 |

Observed median speedups are 3.68-6.59x. Even the fastest old sample is slower
than the slowest new sample in every point group, by at least 1.49x. These are
historical shared-host comparisons, not alternating frozen binaries on an idle
host; they do not qualify an absolute latency SLO or isolate every source delta.

Separate native profiling confirms the expected allocation change. In the Rust
memory group, requests fall from 7,938,001 to 1,942,001 (75.5% fewer) and requested
bytes fall from 978,067,844 to 254,319,844 (74.0% fewer). Python and Go, and the
file backend, retain the same reduction of 5,996,000 native requests per 1,000
point calls. This evidence counts allocation traffic, not RSS. Peak requested
bytes remain approximately unchanged because the process also includes setup
and warmup. Do not claim a lower process memory bound from these figures.

The old source is head `4f70a5e03742d0391eb77a1d7cad50e358be0437`, tree
`8fd5fd53b4985b8634dbd4979a33c2c623745df4`. The measured candidate tree is
`4d13555901dab518561231dc58d1575ac66a2509`. It merges main
`2d05f068925aabb5af93e360c63c6f2e576eed1f`, which adds search stage cleanup,
and applies the point optimization and its regressions. Later measurement
phase fields do not change production code, but they were not present in
these original timing reports.

## Other workload observations and the completion gate

The complete user gate concerns representative **read** performance and
zero-copy/resource qualification. Write cases are regression controls, not an
expectation that Arrow must accelerate commits. The five-case driver uses
ordinary owned results throughout; none of these records execute Arrow or the
retained read interface.

In these runs, persistent `fill` is slower, and other control workloads vary.
All observations must remain visible. Median query-boundary ratios
`before / after` (greater than one means faster):

| Case / backend | Rust | Python | Go |
| --- | ---: | ---: | ---: |
| select / memory | 1.311 | 1.327 | 1.302 |
| select / file | 1.658 | 1.669 | 2.038 |
| fill / memory | 1.686 | 1.550 | 1.656 |
| fill / file | 0.633 | 0.716 | 0.639 |
| fill_bulk / memory | 2.512 | 4.051 | 1.921 |
| fill_bulk / file | 1.046 | 1.313 | 1.041 |
| wide / memory | 1.136 | 1.639 | 1.253 |
| wide / file | 1.161 | 1.220 | 1.320 |

Neither apparent improvements in unchanged controls nor the fill regression
can be attributed to Arrow. Persistent fill medians move from 5.1-5.7 seconds
to 7.1-9.0 seconds. The point patch changes read execution, not these CREATE
commit paths. Shared-host scheduling/storage effects are possible causes, but
are unverified. A controlled repeat and independent write/I/O evidence are
required before a causal conclusion or regression clearance.

The original `query_boundary_ns` includes both the committed write calls and
the final verification read in `fill`/`fill_bulk`. It is not a pure write timer.
The follow-up records `write_boundary_ns` and `read_boundary_ns` separately,
while preserving their sum and every original call. The verification read is
small relative to the multi-second difference; its presence does not explain
that regression. Older samples cannot retroactively recover the two phases.

The separate [phase-timing-observations.json](phase-timing-observations.json)
records a wiring-only N=8 check of all five cases, three languages and both
backends. All 60 records pass ordered parity and validate that the split timers
sum to the original total. It ran alongside fuzz verification and establishes
measurement consistency, not performance or a write-regression clearance.

This experiment does not complete PR #986. Quiet-host alternating comparisons,
the wider representative read matrix, large-result admission, the existing
Arrow outlier, full source/foreign memory accounting and relevant platform
qualification remain open. Keep #976/#987 open and the PR in Draft.
