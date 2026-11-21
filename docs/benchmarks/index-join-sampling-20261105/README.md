# Native index join sampling experiment

Source: `00858a8decd2186ad251f983b55d3302a5b95ff0` (HawDB 0.27.0), based on main `0b137fd117c5bf020f4b2dda04a2fd1fdf97e6d0`.

Baseline and candidate use the same release executable, with `join_sampling=None` and `Some(JoinSamplingLimits::default())`. The `refuse` control sets `max_probes=0`. All 40 process runs completed with exact expected counts. This qualifies these fixtures, not general Mem workloads or larger-than-RAM execution.

Apple M5 Max, 18 cores, 36 GiB RAM, 2 TB internal SSD/APFS, macOS 27 arm64, Rust 1.97.1. Default features plus `qualification`; repository release profile (opt-level 3, thin LTO, one codegen unit, checks disabled). Each process builds one shared checkpoint and alternates disabled/enabled opens; an independent process reverses the order. Each open has fresh HawDB caches. OS caches and other workstation activity were uncontrolled.

## Complete warm query latency

Times include parameter binding/cache lookup, preparation, sampling when performed, execution and result delivery. Each ordinary row pools seven warm observations from each order (14 per configuration). Point controls use 100 per order (200); the original seven-repeat point observations remain in the CSV. Ratios divide the two total-latency medians, not separately aggregated components. A ratio below one is a regression.

| Hops / control | Residency | Seeds | Baseline p50 ms | Candidate p50 ms | Baseline / candidate | Baseline p95 ms | Candidate p95 ms |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: |
| one / skewed | materialized | 256 | 1.5139 | 0.6667 | 2.27x | 1.5660 | 0.7097 |
| one / uniform | materialized | 256 | 0.4846 | 0.4798 | 1.01x | 0.5232 | 0.5258 |
| one / skewed | out-of-core | 256 | 2.6119 | 1.1130 | 2.35x | 2.7205 | 1.1511 |
| one / uniform | out-of-core | 256 | 0.9511 | 0.9448 | 1.01x | 0.9949 | 1.0117 |
| two / skewed | materialized | 256 | 3.1587 | 0.9145 | 3.45x | 3.3996 | 1.0099 |
| two / uniform | materialized | 256 | 0.8596 | 0.8609 | 1.00x | 0.9702 | 0.9289 |
| two / skewed | out-of-core | 256 | 5.6041 | 1.4838 | 3.78x | 5.8094 | 1.6029 |
| two / uniform | out-of-core | 256 | 1.6886 | 1.6793 | 1.01x | 1.7152 | 1.7981 |
| two / no-index | materialized | 256 | 2.7903 | 2.7970 | 1.00x | 3.0626 | 2.8963 |
| two / no-index | out-of-core | 256 | 5.1103 | 5.1196 | 1.00x | 5.2930 | 5.3091 |
| two / refuse | materialized | 256 | 2.8143 | 2.8106 | 1.00x | 2.9991 | 2.9489 |
| two / refuse | out-of-core | 256 | 5.0881 | 5.1079 | 1.00x | 5.2729 | 5.3146 |
| one / skewed | materialized | 4 | 0.0454 | 0.0355 | 1.28x | 0.0707 | 0.0630 |
| one / skewed | out-of-core | 4 | 0.0673 | 0.0428 | 1.57x | 0.0922 | 0.0696 |
| two / skewed | materialized | 4 | 0.0722 | 0.0452 | 1.60x | 0.0986 | 0.0727 |
| two / skewed | out-of-core | 4 | 0.1103 | 0.0539 | 2.04x | 0.1386 | 0.0833 |
| point / skewed | materialized | 256 | 0.0152 | 0.0151 | 1.01x | 0.0168 | 0.0161 |
| point / skewed | out-of-core | 256 | 0.0371 | 0.0371 | 1.00x | 0.0382 | 0.0383 |

## First calls, sampling and controls

Every first-call observation is retained below because two observations per configuration are insufficient to describe a latency distribution. The lists are ordered baseline-first process, then candidate-first process. They include cold HawDB caches, parsing and initial optimizer statistics; OS caches are warm/uncontrolled.

| Hops / control | Residency | Seeds | Baseline first calls ms | Candidate first calls ms |
| --- | --- | ---: | --- | --- |
| one / skewed | materialized | 256 | 5.7405, 3.9869 | 3.1630, 3.5162 |
| one / uniform | materialized | 256 | 2.4530, 2.1014 | 2.2413, 2.4568 |
| one / skewed | out-of-core | 256 | 3.7484, 3.2204 | 1.7433, 2.1108 |
| one / uniform | out-of-core | 256 | 1.9721, 1.5178 | 1.6255, 2.0645 |
| two / skewed | materialized | 256 | 8.2290, 7.8152 | 4.9824, 5.7752 |
| two / uniform | materialized | 256 | 4.6790, 3.8357 | 4.5162, 4.1738 |
| two / skewed | out-of-core | 256 | 7.1872, 6.7635 | 2.5348, 3.3218 |
| two / uniform | out-of-core | 256 | 2.9587, 2.6314 | 2.7478, 3.6335 |
| two / no-index | materialized | 256 | 7.3795, 6.8347 | 6.7475, 7.0810 |
| two / no-index | out-of-core | 256 | 6.4997, 5.9907 | 6.1244, 6.3965 |
| two / refuse | materialized | 256 | 7.1791, 6.9132 | 6.8454, 7.3075 |
| two / refuse | out-of-core | 256 | 6.4658, 6.1301 | 6.1964, 6.5174 |
| one / skewed | materialized | 4 | 0.4014, 0.1940 | 0.1943, 0.4216 |
| one / skewed | out-of-core | 4 | 0.4085, 0.2430 | 0.2492, 0.4439 |
| two / skewed | materialized | 4 | 0.4702, 0.2435 | 0.2399, 0.4605 |
| two / skewed | out-of-core | 4 | 0.4768, 0.2514 | 0.2630, 0.4733 |
| point / skewed | materialized | 256 | 2.5098, 2.3035 | 2.2795, 2.4742 |
| point / skewed | out-of-core | 256 | 0.3957, 0.1811 | 0.1767, 0.4010 |

`plans.json` records direction, estimates, probes, prepared entries, read bytes/operations, scratch accounting and elapsed sampling time. Warm queries reuse the physical plan and do not resample. Uniform data keeps forward orientation; missing-index/point controls do not sample; zero-probe controls publish no partial estimates. Source/domain preparation is included, not hidden behind an isolated rank microbenchmark.

The skewed fixture selects reverse orientation and improves one/two-hop complete latency. Uniform and point results do not establish a benefit; inspect their actual deltas before enabling this on a host workload. The small four-seed cases also retain first-call overhead, including regressions. Default activation remains off.

`queries.csv` contains actual wall totals, parse time, preparation including sampling, sampling as a subset, and an execution/delivery/other remainder. The remainder is subtraction within the same observation, not isolated executor CPU. Process user/system CPU in `manifest.json` includes fixture construction and opens and must not be attributed to join execution. No per-query CPU or energy claim is made.

## Reproduction and build cost

See [the contract](../../QUERY_JOIN_SAMPLING.md#qualification) for the generator, budgets and arguments. `manifest.json` supplies every exact argument list and binary SHA-256; fixture setup/open times are separate. First worktree release build: 167.16 s; final source rebuild: 114.11 s, with shared registry/toolchain caches. Compilation is not query latency.

```console
cargo build --locked --release --features qualification --bench index_join_sampling
target/release/deps/index_join_sampling-<hash> two materialized 256 7 skewed
target/release/deps/index_join_sampling-<hash> two materialized 256 7 skewed candidate-first
target/release/deps/index_join_sampling-<hash> two out-of-core 256 7 skewed
target/release/deps/index_join_sampling-<hash> two out-of-core 256 7 skewed candidate-first
```

Larger-than-RAM qualification, checkpoint-base-plus-overlay sampling, broad predicate/path coverage and default-on activation remain open in [#1560](https://github.com/nowledge-co/hawdb/issues/1560).
