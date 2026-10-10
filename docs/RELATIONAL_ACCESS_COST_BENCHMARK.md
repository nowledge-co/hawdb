# Persisted Relational Access Evidence

The `relational_index_access` benchmark retains its original in-memory
`RelationalStore` measurements and adds a separate `persisted` result. This
addresses the measurement prerequisite of [HawDB #216](https://github.com/nowledge-co/hawdb/issues/216): in-memory prefix timings do
not establish the cost of persisted index traversal and canonical row fetches.
The original v1 recording is retained below. Protocol v2 verifies the descriptor-aware
policy in [the cost contract](OPTIMIZER_COST_MODEL.md); this benchmark alone does not
complete that issue.

## Fixture and comparison

The persisted experiment uses the existing embedded SQL and profiled read APIs.
It creates one table, inserts deterministic rows, checkpoints, closes the
writer, and opens a new read-only database for each access path. Seeding uses
materialized rows and shadow index publication. Reading explicitly selects
`StorageResidencyMode::OutOfCore` and `RelationalIndexMode::Authoritative` with
a 64 MiB segment cache. These are fixture-local settings, not new defaults.

Indexed columns have equal-valued, unindexed mirror columns in the same row.
Changing only the predicate column compares an unindexed full scan with the
optimizer-selected access when an index is available. Both return exactly the
same IDs and payloads from the same checkpoint, without
an optimizer hint, a forced-plan public API, or differently populated tables.

The matrix covers:

- 64- and 1,024-byte deterministic ASCII payloads;
- clustered and dispersed bucket membership; the latter uses an odd
  multiplicative permutation of a power-of-two row count;
- a primary-key point lookup, sparse/medium/broad secondary-index prefixes,
  and a deliberately nonselective prefix containing every row;
- the first query on a newly opened handle, followed by repeated queries on
  that handle's pinned read transaction.

Debug-assertion builds use 256 rows and three repeated samples. Release builds
use 8,192 rows and eleven repeated samples. The three disjoint bucket sizes
are `rows / 100`, `rows / 10 - rows / 100`, and `rows - rows / 10`; reports
contain the actual matching count and selectivity, not rounded labels.
There are 20 matrix entries and two access paths per entry.

Each query returns `id` and `body` without `LIMIT`, ordering, or aggregation.
Row and payload admission are explicit. After the measured interval, the
harness checks every payload against the deterministic input generator and
compares the complete sorted ID multiset with the fixture oracle. Sorting is
only for benchmark verification and is not part of the executed query.

## Evidence and assertions

The additive JSON object uses `hawdb-persisted-relational-access-v2` and records
the OS, architecture, smoke/full scale, row count, sample count and cache size.
For each path it includes:

- database-open time separately from first-query time;
- first-query parse, bind, plan and execute timings;
- repeated total-query, execute and plan p50/p95/p99 timings, plus every
  repeated sample's profile;
- selected operator, estimated and actual access rows, complete-consumption
  evidence, intermediate rows and row-fetch requirements;
- canonical row and index logical pages/bytes, file pages/bytes, cache
  hits/misses/rejections, and visited rows;
- snapshot epochs and overflow hydration bytes where applicable.

The harness requires a full scan for the unindexed predicate and a canonical
point get for the primary key. The historical protocol-v2 NDV baseline required
an index for secondary prefixes only when its `6 * estimated_candidates + 2`
logical cost was below the scan cost `3 * rows + 4`. Fresh statistics estimated
the three buckets with `ceil(rows / 3)` and the all-rows prefix with `rows`.
That baseline selected a full scan for all-rows cases and an index for every
bucket, including the broad bucket whose true skew average fanout did not
represent. The [current selection oracle](#current-selection-oracle) uses exact
counts and page geometry instead; broad and all-rows cases select a scan.
The second report is named `cost_selected`, making paired full scans explicit.
Historical v1 reports used `index` and required that operator for every prefix.
The scan's access-row count must equal the complete table size, while point/prefix
access counts must equal their matching counts. These
are different from final result cardinality, which is checked independently.
Canonical primary-key reads use row-page locators directly; an empty separate
index-read profile is expected for that path.

Every path must read canonical snapshot rows, the first query must perform
row-file reads, and secondary probes must use authoritative index pages.
Repeated queries must reuse the SQL template and perform no row/index file
page reads with this cache-sized fixture. Snapshot epochs must agree across
each pair, and no mutable row overlay may participate. Every selected operator is checked against the independent expected policy;
silently changing a selective index case to a scan still fails the experiment.

## Interpretation limits

"First query" means first use of a new HawDB handle, not cold physical disk.
The OS page cache is uncontrolled, the fixture was just written, and earlier
probes can warm OS caches. File-read counters show reads requested by HawDB,
not physical device I/O. Dispersed membership spreads matching rows across
canonical pages; it does not prove random device access or bypass readahead.
Query timing includes the profiled execution path's instrumentation.

The legacy results, the persisted first-query results, and the persisted warm
results are separate observations. Do not combine them into a claimed
speedup over one common baseline. This is a read-only, single-host,
cache-fitting synthetic calibration input, not a concurrency benchmark,
device-independent cost coefficient, full Mem-corpus qualification, or a
release/readiness claim. Larger-than-cache working sets, other hardware,
additional storage modes, joins, and spill require their own measurements
before selecting a general cost policy. There is no wall-clock performance
threshold in the test; correctness and evidence contracts fail closed.

## Example local observation

One v1 release run on 2026-09-09 used macOS 26.6.2 / aarch64, 18 physical CPU
cores, 36 GiB system memory, and Rust 1.97.1 with the default Cargo bench
profile. The fixture ran after local compilation and fuzz jobs completed.
All 20 persisted comparisons and all four original in-memory cases completed;
the persisted paths used 8,192 rows and eleven warm samples each.

The 64-byte, clustered fixture produced these execution-stage measurements.
First-query values are individual observations; warm values are p50 in ms.
Validation is outside the timed interval, and each pair returns identical
complete results.

| Shape | Matching rows | First scan ms | First index ms | Warm scan ms | Warm index ms |
| --- | ---: | ---: | ---: | ---: | ---: |
| Primary-key point | 1 | 23.597 | 0.717 | 1.075 | 0.030 |
| Sparse prefix | 81 | 23.960 | 3.429 | 1.122 | 2.514 |
| Medium prefix | 738 | 22.690 | 24.357 | 1.091 | 21.146 |
| Broad prefix | 7,373 | 22.173 | 222.673 | 1.214 | 202.772 |
| All-rows prefix | 8,192 | 22.642 | 247.095 | 1.214 | 225.499 |

In the all-rows case, the scan reported 32 logical row-page reads; the index
path reported 8,192. Each first query requested the same 32 row-file pages
(32 MiB), and neither warm path read row files. Those logical page counts
are accesses, not counts of distinct pages or proof of repeated decoding.
The indexed path also traverses its separate secondary-index pages.

For the sparse prefix, clustered membership touched one row-file page while
dispersed membership touched 32, despite returning the same number of rows.
The 1,024-byte all-rows fixtures also favored scans in this run: warm scan
execution was about 1.64 ms versus 225-227 ms for the index path. These
observations motivate descriptor-aware row-fetch costing and investigation
of repeated page-access work; they do not establish the source of CPU time.
The sparse clustered case also reverses the faster path between first-query
and warm measurements, so one selectivity-based multiplier cannot represent
both conditions. These are baseline observations, not improvements introduced
by this benchmark or portable cost coefficients.

## Running and verification

A later [Linux release calibration](RELATIONAL_ACCESS_COST_LINUX_CALIBRATION.md)
records the complete 20-case timing matrix and machine-readable profiles at
main `366828ec`. It confirms the warm prefix row-fetch penalty on that
revision and separates locality-dependent first-query observations from
warm-cache results.

The existing benchmark and smoke test execute the persisted module. The manual
release target configures the same binary and its dependencies with Bazel's
optimized compilation mode, leaving the existing smoke registrations intact.
Run it with the default Bazel command configuration for full-scale timing;
the ordinary binary remains the debug-scale smoke/contract check:

```sh
bazel run //:hawdb_bench_relational_index_access_release
bazel run //:hawdb_bench_relational_index_access
bazel test //:hawdb_linux_ci_benchmark_relational_index_access_smoke_test
bazel test //crates/fuzz:hawdb_fuzz_tests //crates/fuzz:hawdb_fuzz_cli_tests //:hawdb_linux_ci_fuzz_smoke_test
```

Negative controls should corrupt a returned payload, change an expected ID,
or make the indexed query use the unindexed mirror. Each must reject the
benchmark before it can publish a successful report. Fuzz remains local; the
existing fuzz targets are not added to CI by this work.

## Current selection oracle

The value-specific planner uses unchanged checkpoint metadata for complete
secondary keys. The harness independently composes the documented logical
policy from the fixture's exact matching count and default 256-row page bound:
scan scalar `3*rows + 4 + 6*pages`, noncovering secondary scalar
`matches*(8 + 7*(floor(log2(pages)) + 1)) + 2`. Each checked descriptor reads
its fixed record and both nonempty bound keys: scans charge those three accesses
sequentially and point-fetch searches charge them randomly. The unchanged
random weight of two is applied once, alongside the existing CPU setup/search
work. These are logical operations, not measured device pages or nanoseconds.
Both payload widths fit the
page-byte limit before that row-count bound; full-scan reports assert the
expected page geometry. A primary-key point keeps its direct point operator.
Sparse prefixes select an index; medium prefixes select an index at the
256-row smoke scale and a scan at the 8192-row optimized scale. Broad/all-row
prefixes select a scan. Complete ID/payload and actual-access-row assertions
remain required for every arm.

Index reports include planning metadata even when that secondary candidate is
rejected in favor of a scan. Such reports have runtime path `not_executed` and
visit zero rows; an executed index reports `authoritative` and adds the complete
locator visits. Both cases must retain the planning metadata I/O. First-handle
and warm reports both include this planning work. Historical v2 artifacts retain
their named source's old NDV selection and do not establish current candidate
timing or improvement.
