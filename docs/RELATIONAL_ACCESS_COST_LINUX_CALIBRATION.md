# Linux Relational Access Calibration

A release run of the unchanged persisted access benchmark at
`366828ec4133d10aed3b300d93cd2e04bfde5b74` completed on September 14, 2026
(Asia/Shanghai). The measurements support charging canonical row-fetch work
separately from scan work in the remaining [#216 cost model](https://github.com/nowledge-co/hawdb/issues/216).
They also show why selectivity alone cannot predict the faster path across
cache states and row layouts. This records a baseline; no optimizer policy
or public interface changes in this delivery.

The [measurement data](benchmarks/relational_access_linux_2026_09_14.json)
contains all 20 persisted comparisons, every first-query profile, all eleven
warm stage samples per path, reported p50/p95/p99 values, and the four original
in-memory results. Warm counters are identical across each path's samples and
are stored once per path. The recording includes the hashes of the original
report and stdout before this reduction. The [benchmark protocol](RELATIONAL_ACCESS_COST_BENCHMARK.md)
defines the fixture and correctness assertions.

## Environment and qualification

- Linux `7.1.10-zen1-1-zen`, x86_64; AMD Ryzen 7 7735HS, eight physical cores
  and sixteen logical CPUs; 28.1 GiB system memory.
- Rust/Cargo 1.97.1; the repository's unchanged Cargo bench profile uses
  optimization level 3, one codegen unit, thin LTO and no debug assertions.
- NVMe-backed Btrfs, with `noatime`, `compress=zstd:3`, `ssd` and
  `discard=async`. Only the benchmark child process's `TMPDIR` pointed into
  the workspace, because the host's default `/tmp` is tmpfs. No Bazel
  configuration or production setting changed.
- All local test work finished before release compilation and measurement.
  The command, including compilation, took 324.979 seconds; this is not
  query-execution time. Other host activity and the OS page cache were not
  controlled.
- All 1,239 tracked source/build file hashes remained unchanged. The fixture
  retained 8,192 rows, a 64 MiB HawDB cache, two body widths, two layouts, five
  shapes, and eleven warm samples. All 480 persisted queries and the four
  original in-memory cases completed; every persisted result's full ID
  multiset and payload matched the oracle. Generated fixture directories
  were removed by the harness.
- Each pair used the same checkpoint. The actual scan, primary-key point-get
  and non-covering authoritative secondary-index operators matched the
  harness assertions. Every warm query reused the SQL template and requested
  zero canonical-row and index file pages. No partial-consumption result was
  accepted.

To reproduce, use the ordinary release benchmark. If `/tmp` is a memory
filesystem, set `TMPDIR` only for this command to an existing directory on the
intended device:

```sh
mkdir -p target/relational-access-fixtures
TMPDIR="$PWD/target/relational-access-fixtures" cargo bench --offline --bench relational_index_access
```

`--offline` requires the dependencies to be available locally. It does not
change the benchmark or compilation profile.

## Execution-stage measurements

All values below are milliseconds. First-query values are individual samples;
warm values are p50 of eleven samples. Database-open time, parsing, binding,
planning and post-query result validation are excluded from these execution
times and retained separately in the data where reported. Each pair returns
identical complete results.

| Body bytes | Layout | Shape | Matching rows | First scan ms | First index ms | Warm scan ms | Warm index ms |
| ---: | --- | --- | ---: | ---: | ---: | ---: | ---: |
| 64 | clustered | point | 1 | 42.697 | 1.342 | 1.127 | 0.031 |
| 64 | clustered | sparse prefix | 81 | 42.395 | 4.043 | 1.173 | 2.514 |
| 64 | clustered | medium prefix | 738 | 43.059 | 27.251 | 1.229 | 21.473 |
| 64 | clustered | broad prefix | 7,373 | 43.102 | 240.706 | 1.517 | 199.606 |
| 64 | clustered | all rows prefix | 8,192 | 41.146 | 266.690 | 1.426 | 226.194 |
| 64 | dispersed | point | 1 | 48.897 | 1.511 | 1.280 | 0.038 |
| 64 | dispersed | sparse prefix | 81 | 48.587 | 50.456 | 1.203 | 2.754 |
| 64 | dispersed | medium prefix | 738 | 44.500 | 64.040 | 1.242 | 20.294 |
| 64 | dispersed | broad prefix | 7,373 | 43.730 | 250.470 | 1.530 | 193.897 |
| 64 | dispersed | all rows prefix | 8,192 | 40.071 | 247.012 | 1.398 | 227.372 |
| 1,024 | clustered | point | 1 | 43.548 | 1.362 | 1.738 | 0.029 |
| 1,024 | clustered | sparse prefix | 81 | 44.212 | 3.730 | 1.798 | 2.342 |
| 1,024 | clustered | medium prefix | 738 | 42.965 | 24.497 | 1.852 | 18.951 |
| 1,024 | clustered | broad prefix | 7,373 | 42.678 | 218.357 | 2.599 | 204.384 |
| 1,024 | clustered | all rows prefix | 8,192 | 44.983 | 252.439 | 2.859 | 225.620 |
| 1,024 | dispersed | point | 1 | 43.373 | 1.409 | 1.831 | 0.037 |
| 1,024 | dispersed | sparse prefix | 81 | 43.608 | 45.435 | 1.813 | 2.306 |
| 1,024 | dispersed | medium prefix | 738 | 44.743 | 65.111 | 1.994 | 20.523 |
| 1,024 | dispersed | broad prefix | 7,373 | 46.516 | 252.359 | 3.137 | 200.245 |
| 1,024 | dispersed | all rows prefix | 8,192 | 44.192 | 274.307 | 3.068 | 228.344 |

With eleven warm samples, the benchmark's nearest-rank p95 and p99 are both
the maximum observed sample; they are not stable tail-latency estimates.
The run contains no latency admission threshold.

## What the access profiles establish

For all widths and layouts, full scans visit 8,192 rows through 32 logical
canonical-row page accesses. Index/point paths visit the matching rows and
report one logical canonical-row page access per returned row. These counts
represent accesses, not unique pages or proof of repeated decoding.

The first-query canonical-row file-page counts are:

| Shape | Scan, either layout | Clustered index | Dispersed index |
| --- | ---: | ---: | ---: |
| Point | 32 | 1 | 1 |
| Sparse prefix | 32 | 1 | 32 |
| Medium prefix | 32 | 4 | 32 |
| Broad prefix | 32 | 29 | 32 |
| All-rows prefix | 32 | 32 | 32 |

Each reported canonical-row file page is 1 MiB; these counts are the same for
both body widths. They exclude separate secondary-index page reads. For the
64-byte clustered all-rows prefix, both paths request 32 MiB of canonical-row
files on the first query, but report 32 versus 8,192 logical row-page accesses.
The index path also requests four index pages totaling 256 KiB. Neither warm
path requests row or index files.

Three consequences matter for the pending cost contract:

- Point gets are faster than scans in all four fixtures, both first and warm.
  The scan remains a useful alternative for non-covering prefixes: all warm
  prefix cases favor the scan in this run, including the 81-row sparse case.
  A rows-only ranking cannot express this distinction.
- Clustered sparse and medium prefixes favor the index on the first query
  and the scan when warm. The corresponding dispersed prefixes favor the
  scan on the first query too. Estimated row count and access kind are equal
  across each clustered/dispersed pair; their page locality differs. A policy
  needs an explicit locality/cache assumption or supporting estimates to
  represent both observations.
- Warm all-rows index execution remains about 226-228 ms while warm scans
  range from 1.398 to 3.068 ms. The observed excess logical row-page work
  motivates investigation of the access path. It does not identify a CPU
  bottleneck or establish a coefficient for physical random I/O.

The earlier macOS run showed the same warm sparse-prefix reversal and broad
row-fetch penalty. These are separate host/revision observations, not a
controlled platform comparison. The present main revision excludes the
pending #196 cache-ownership change, which may affect later calibration.

## Remaining scope

The working set fits the HawDB cache, the fixture was freshly written, and
the OS cache is uncontrolled. First-query reads are requests issued by HawDB,
not cold-device I/O. Btrfs compression and readahead further prevent treating
these byte counts as device transfers. Cache pressure, other storage modes,
joins, spills and concurrent workloads need separate evidence before a
general cost policy can be claimed.

#216 stays open for the shared cost-unit/descriptor contract, cost-aware
skyline and access selection, consistent join memo/runtime/profile costs,
and deliberate plan/result differential qualification. The shared estimator
API still needs its separately scoped approval. This measurement does not
select universal component weights or replace that implementation work.
