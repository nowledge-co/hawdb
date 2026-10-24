# Bf-Tree MVCC scan continuation, 2026-10-24

The runtime source is commit 4c8e93e4c2934b6f545247a658f1ebd776fa9284.
The baseline is d5bcca9e7dd64799ccee4a10a8874542473d1786 merged with main
720d98f8640105add1007040ac608df1e643fc3e, with the same canonical license
headers. Both binaries have byte-identical benchmark, journal adapter, main,
dependency lock and profile sources. Source hashes, reconstructable candidate
patch and binary identities are in source.json, source.patch.gz and each
protocol.json. Prior M5 measurements in MEASUREMENTS.md are separate historical
sources, not this comparison's baseline.

## Change and invariants

Visibility and duplicate-version checks borrow the native iterator buffer.
Only selected result data or Hybrid keys needing history lookup become owned.
The resume key uses bounded stack storage, and one owned row batch is reused.
Each native iterator still visits at most 32 records before releasing its
guard. An output-empty physical chunk continues rather than ending the scan.

At Versioned chunk boundaries, an already selected value/tombstone permits an
ordered seek past the rest of that key's versions, preserving longer keys and
zero-byte extensions. A too-new last version permits an inclusive seek to the
fixed snapshot epoch. The visited-record and returned-row caps remain enforced;
ordered seeks can avoid work rather than charge records they never visit.
The journal, synchronization, conflict/retry and publication code is unchanged.

## Protocol and limits

- Apple M3 Max, 16 physical/logical CPUs, 128 GiB RAM; macOS 27.0.1 arm64.
  Local APFS /private/tmp, device model not collected. Rust 1.97.1, uniform
  repository release profile: opt-level 3, thin LTO, one codegen unit.
- Real bf-tree 0.5.6 memory indexes with the same 4 MiB cache configuration
  and whole-page read caching. Native Bf WAL/CPR remains disabled; the separate
  experiment journal requests sync_all for each complete transaction.
  The cache size is not a total RSS cap or a larger-than-memory qualification.
- Two complete cohorts, each five alternating pairs with rotated layout order,
  reversed workload/executable order on odd pairs, and a fresh process/journal
  for every run. All Versioned/Hybrid/COW cases use the unchanged bench-one
  harness and its warmed point sequence. Values are 100 bytes; fixtures cover
  16 keys/2 overwrite rounds, 512/24 and 2048/48 while retaining the seeded
  snapshot. COW is a component control, not production HawDB.
- Each process times 4,096 point reads and 32 full scans at each of the current
  and old snapshots. Every generated row/order is checked by the harness;
  reopen includes its final-key check. Scan p99 is a sample maximum.
  Small full-commit tails also contain only two samples per process.
- Each cohort has 90 successful processes, 51,900 timed commit batches,
  737,280 timed point reads, 5,760 timed scans and 45 byte-identical paired
  journals. The two cohorts contain 180 processes; no completed sample was
  discarded. Raw stdout/stderr, return codes, order, load, every percentile
  and individual paired ratios are retained.
- Our compilers/tests finished before timing. The host was shared, unreserved
  and unpinned. Other builds were observed in the initial cohort. Confirmation
  started without observed Rust compilers but subsequently observed them at
  the before/after boundaries of 43/90 processes. Those counts are retained;
  boundary sampling does not establish continuous quiescence or measure all
  other CPU/I/O activity. This is local supporting evidence, not stable tails,
  statistical confidence intervals, no-regression acceptance or CI speedup.
- Percentile medians and median within-pair ratios are different summaries;
  a ratio of two separate medians need not equal the paired median.
  Phase percentiles overlap and cannot be added into transaction latency.

Run measure.py with --baseline, --candidate, --output, --journals and --pairs 5.
The original driver is retained in code commit 4c8e93e4c; confirmation only adds
host compiler observations outside native operation timing. The same fixed
binaries were used in both cohorts.

## Initial shared-host cohort

Values are per-process percentile medians in microseconds; ratios are candidate/baseline medians within pairs.

| Keys/rounds | Layout | Current scan p95 before -> after us | Ratio | Old scan p95 before -> after us | Ratio |
| --- | --- | ---: | ---: | ---: | ---: |
| 16/2 | versioned | 11.750 -> 6.083 | 0.537 | 11.208 -> 5.083 | 0.454 |
| 16/2 | hybrid | 5.125 -> 5.625 | 0.890 | 7.958 -> 5.666 | 0.680 |
| 16/2 | cow | 1.916 -> 2.625 | 1.245 | 0.792 -> 1.500 | 2.000 |
| 512/24 | versioned | 1779.583 -> 557.167 | 0.350 | 1883.875 -> 536.292 | 0.347 |
| 512/24 | hybrid | 74.625 -> 52.000 | 0.697 | 193.708 -> 158.750 | 0.844 |
| 512/24 | cow | 18.625 -> 16.583 | 1.027 | 14.791 -> 16.792 | 1.135 |
| 2048/48 | versioned | 16303.125 -> 7558.000 | 0.463 | 16760.708 -> 7335.417 | 0.463 |
| 2048/48 | hybrid | 547.041 -> 674.542 | 0.791 | 2150.791 -> 2658.792 | 1.012 |
| 2048/48 | cow | 59.625 -> 67.208 | 1.063 | 57.125 -> 58.417 | 1.153 |

Costs and adverse controls are retained below; values are paired candidate/baseline ratios except resident memory.

| Keys/rounds | Layout | Current get p99 | Old get p99 | Commit p99 | Reopen | Resident before -> after MiB |
| --- | --- | ---: | ---: | ---: | ---: | ---: |
| 16/2 | versioned | 1.196 | 1.000 | 1.000 | 1.049 | 6.266 -> 6.250 |
| 16/2 | hybrid | 1.000 | 1.000 | 1.017 | 0.897 | 6.234 -> 6.266 |
| 16/2 | cow | 1.000 | 1.000 | 1.003 | 1.158 | 6.047 -> 6.031 |
| 512/24 | versioned | 1.191 | 1.056 | 0.998 | 1.019 | 14.203 -> 14.250 |
| 512/24 | hybrid | 1.000 | 1.024 | 1.534 | 1.002 | 14.422 -> 14.422 |
| 512/24 | cow | 1.000 | 1.000 | 0.641 | 0.978 | 9.250 -> 9.234 |
| 2048/48 | versioned | 1.100 | 1.040 | 1.053 | 1.029 | 52.891 -> 53.125 |
| 2048/48 | hybrid | 0.960 | 0.969 | 0.902 | 1.061 | 53.328 -> 53.562 |
| 2048/48 | cow | 1.000 | 1.000 | 0.923 | 1.006 | 26.031 -> 25.922 |

## Confirmation with compiler observations

Values are per-process percentile medians in microseconds; ratios are candidate/baseline medians within pairs.

| Keys/rounds | Layout | Current scan p95 before -> after us | Ratio | Old scan p95 before -> after us | Ratio |
| --- | --- | ---: | ---: | ---: | ---: |
| 16/2 | versioned | 11.084 -> 6.875 | 0.637 | 10.625 -> 5.208 | 0.477 |
| 16/2 | hybrid | 4.875 -> 4.334 | 1.026 | 8.167 -> 5.542 | 0.647 |
| 16/2 | cow | 2.209 -> 2.166 | 0.945 | 0.709 -> 0.833 | 1.176 |
| 512/24 | versioned | 1676.208 -> 545.750 | 0.336 | 1563.958 -> 541.500 | 0.341 |
| 512/24 | hybrid | 73.958 -> 52.250 | 0.730 | 188.583 -> 161.625 | 0.850 |
| 512/24 | cow | 15.583 -> 16.333 | 1.048 | 15.625 -> 16.500 | 1.051 |
| 2048/48 | versioned | 16875.333 -> 7368.583 | 0.449 | 15978.250 -> 7157.375 | 0.456 |
| 2048/48 | hybrid | 405.041 -> 323.625 | 0.799 | 1591.334 -> 1475.333 | 0.962 |
| 2048/48 | cow | 51.167 -> 61.875 | 1.194 | 52.459 -> 58.958 | 1.145 |

Costs and adverse controls are retained below; values are paired candidate/baseline ratios except resident memory.

| Keys/rounds | Layout | Current get p99 | Old get p99 | Commit p99 | Reopen | Resident before -> after MiB |
| --- | --- | ---: | ---: | ---: | ---: | ---: |
| 16/2 | versioned | 1.000 | 1.000 | 0.996 | 1.016 | 6.266 -> 6.234 |
| 16/2 | hybrid | 1.000 | 0.877 | 1.201 | 0.866 | 6.219 -> 6.250 |
| 16/2 | cow | 1.000 | 1.000 | 0.997 | 0.972 | 6.062 -> 6.031 |
| 512/24 | versioned | 1.000 | 1.000 | 1.187 | 1.021 | 14.250 -> 14.234 |
| 512/24 | hybrid | 1.000 | 1.024 | 0.871 | 1.006 | 14.344 -> 14.391 |
| 512/24 | cow | 1.000 | 1.000 | 0.859 | 0.988 | 9.219 -> 9.188 |
| 2048/48 | versioned | 1.041 | 1.027 | 0.963 | 1.010 | 53.094 -> 53.109 |
| 2048/48 | hybrid | 1.047 | 0.950 | 0.758 | 1.058 | 53.203 -> 53.250 |
| 2048/48 | cow | 1.000 | 1.000 | 1.413 | 1.015 | 26.000 -> 25.938 |

## Interpretation

The 512/24 Versioned current/old scan p95 ratios are 0.350/0.347 initially
and 0.336/0.341 in confirmation: about 65-66% less latency in both cohorts.
Hybrid current scan ratios are 0.697/0.730 (about 27-30% less); its old scans
are 0.844/0.850 (about 15-16% less). At 2048/48 Hybrid current scans improve
about 20% in both cohorts, while its old scans are 1.012/0.962: no clear
large-workload old-scan improvement is established. Small Hybrid current
scan ratios are 0.890/1.026, so no universal small-case win is claimed.

The initial 512 Versioned current-get p99 ratio is adverse at 1.191; it is
1.000 in confirmation. The corresponding large-case ratios are 1.100/1.041.
No point-read gain or strict point-tail ceiling is established.

Hybrid 512 commit p99 is adverse at 1.534 initially and 0.871 in confirmation.
The initial difference is concentrated in prepare/sync (1.544), before any
scan has run, with index installation at 0.926. This observation does not
identify its cause. The unchanged large COW commit control changes from
0.923 to 1.413 across cohorts. Synchronization/load variance prevents a stable
full-commit improvement or regression-ceiling claim. Nothing weakens sync_all
to obtain the scan results. COW still scans faster in these fixtures, and
resident memory is approximately unchanged. All tails, controls and replay/
memory observations remain in the summaries, including adverse ones.

The exploratory-copy-filter directory preserves the earlier first candidate
and all four exploratory observations during compilation. Its source differs
from the final candidate; it is not another final qualification cohort.

## Verification and remaining boundaries

Local source-bound receipts are archived under validation:

- Independent dev/release tests: 19 passed, one directly ignored crash child
  exercised by the parent. Added exact before/after scan comparisons across
  97 versions, all-filtered chunks, tombstones, binary/prefix/max-size keys,
  exact snapshot seeks and durable reopen. A native-index work-budget test
  accepts 200,000 distinct invisible records and refuses the 200,001st.
- Root and independent format/strict all-target/all-feature Clippy passed.
  Actual staged license/fmt/Clippy hooks passed before the code commit.
  Full-tree prek run --all-files fails on 624 historical license-header files
  verified unchanged from main, outside this PR diff. No hook, assertion,
  fixture, timeout or compiler profile is disabled or weakened.
- CARGO_BAZEL_ISOLATED=false CARGO_NET_OFFLINE=true bazel --nohome_rc test
  --jobs=6 --local_test_jobs=2 //crates/fuzz:hawdb_fuzz_tests
  //crates/fuzz:hawdb_fuzz_cli_tests //tools/shiro:shiro_tests
  //tools/shiro:shiro_cli_tests //:hawdb_linux_ci_fuzz_smoke_test:
  all 98 targets executed and passed. Total wall time was 1,838.526 seconds,
  including analysis/build/execution; no BEP-based phase split or CI speedup
  is claimed. The independent experiment is not in those target graphs.
  Earlier timeout causes are not inferred from this successful run.
- cargo run --locked --manifest-path tools/mvcc-fuzz/Cargo.toml -- run
  --seconds 30 --explain: 12,333 complete cases passed in 30,037 ms after
  9m37s compilation. This production campaign does not exercise this adapter.
- Workspace metadata, all-feature normal dependency tree and package archive
  listing confirm the experiment/Bf dependency remains excluded from production.
- The first release baseline build took 6m24s under concurrent verification;
  the final header-aligned baseline and candidate rebuilds took 8.29s/6.71s
  with cached dependencies. These are different cache states, not build gains.

Remote CI and review are separate. The PR's existing hold remains. No native
Windows experiment, checkpoint/GC, private range write overlay, group commit,
serializability, power-loss/write-reordering or production backend integration
is qualified by these component measurements.
