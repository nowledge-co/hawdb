# Native index join sampling after allocation preflight

Source: `a23142feefe263990309f6e2620bade9148c7435` (HawDB 0.27.0), based on main `0b137fd117c5bf020f4b2dda04a2fd1fdf97e6d0`.

Baseline and candidate use the same release executable, with `join_sampling=None` and `Some(JoinSamplingLimits::default())`. The `refuse` control sets `max_probes=0`. All 40 process runs completed with exact expected counts. This qualifies these fixtures, not general Mem workloads or larger-than-RAM execution.

Apple M5 Max, 18 cores, 36 GiB RAM, 2 TB internal SSD/APFS, macOS 27 arm64, Rust 1.97.1. Default features plus `qualification`; repository release profile (opt-level 3, thin LTO, one codegen unit, checks disabled). Each process builds one shared checkpoint and alternates disabled/enabled opens; an independent process reverses the order. Each configuration opens new handles; the first query is a physical-plan-cache miss. No explicit storage-cache or OS-cache eviction is performed. Default storage-cache lifecycle and other workstation activity were uncontrolled.

This follow-up includes descriptor/payload allocation preflight and per-entry cancellation checks. The earlier experiment is retained unchanged in the sibling `index-join-sampling-20261105` directory.

## Complete warm query latency

Times include parameter binding/cache lookup, preparation, sampling when performed, execution and result delivery. Each ordinary row pools seven warm observations from each order (14 per configuration). Point controls use 100 per order (200); the original seven-repeat point observations remain in the CSV. Ratios divide the two total-latency medians, not separately aggregated components. A ratio below one is a regression.

| Hops / control | Residency | Seeds | Baseline p50 ms | Candidate p50 ms | Baseline / candidate | Baseline p95 ms | Candidate p95 ms |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: |
| one / skewed | materialized | 256 | 1.6208 | 0.7466 | 2.17x | 1.9665 | 0.9883 |
| one / uniform | materialized | 256 | 0.6368 | 0.6151 | 1.04x | 0.7114 | 0.7257 |
| one / skewed | out-of-core | 256 | 2.8806 | 1.2389 | 2.33x | 3.7465 | 1.4558 |
| one / uniform | out-of-core | 256 | 1.2492 | 1.2881 | 0.97x | 1.5148 | 1.5117 |
| two / skewed | materialized | 256 | 2.6948 | 0.8770 | 3.07x | 3.4435 | 1.0453 |
| two / uniform | materialized | 256 | 0.9131 | 0.9820 | 0.93x | 1.1416 | 1.1558 |
| two / skewed | out-of-core | 256 | 4.9323 | 1.5999 | 3.08x | 5.8739 | 2.3293 |
| two / uniform | out-of-core | 256 | 1.9620 | 1.9459 | 1.01x | 2.5755 | 2.6254 |
| two / no-index | materialized | 256 | 2.9604 | 2.7951 | 1.06x | 3.4613 | 3.7149 |
| two / no-index | out-of-core | 256 | 4.7988 | 4.7701 | 1.01x | 7.4416 | 4.9815 |
| two / refuse | materialized | 256 | 2.6537 | 2.6385 | 1.01x | 3.6284 | 3.4309 |
| two / refuse | out-of-core | 256 | 4.7393 | 4.8046 | 0.99x | 7.4397 | 7.4689 |
| one / skewed | materialized | 4 | 0.0827 | 0.0632 | 1.31x | 0.1180 | 0.1039 |
| one / skewed | out-of-core | 4 | 0.1189 | 0.0829 | 1.44x | 0.1647 | 0.1269 |
| two / skewed | materialized | 4 | 0.1284 | 0.0782 | 1.64x | 0.1524 | 0.1360 |
| two / skewed | out-of-core | 4 | 0.2063 | 0.0985 | 2.09x | 0.2474 | 0.1457 |
| point / skewed | materialized | 256 | 0.0216 | 0.0216 | 1.00x | 0.0228 | 0.0245 |
| point / skewed | out-of-core | 256 | 0.0615 | 0.0677 | 0.91x | 0.0702 | 0.0722 |

## First calls, sampling and controls

Every first-call observation is retained below because two observations per configuration are insufficient to describe a latency distribution. The lists are ordered baseline-first process, then candidate-first process. They include each handle's first query, parsing and initial optimizer statistics. They are not a fully cold-machine measurement; storage/OS cache eviction was not forced.

| Hops / control | Residency | Seeds | Baseline first calls ms | Candidate first calls ms |
| --- | --- | ---: | --- | --- |
| one / skewed | materialized | 256 | 8.0881, 4.4196 | 3.1048, 5.3163 |
| one / uniform | materialized | 256 | 3.5507, 2.6788 | 2.7132, 3.9742 |
| one / skewed | out-of-core | 256 | 5.9473, 3.5193 | 2.0612, 2.9231 |
| one / uniform | out-of-core | 256 | 3.3070, 1.9418 | 2.2549, 3.3098 |
| two / skewed | materialized | 256 | 9.8605, 6.8862 | 4.3653, 7.3041 |
| two / uniform | materialized | 256 | 6.0541, 4.1229 | 4.9411, 6.0405 |
| two / skewed | out-of-core | 256 | 6.2187, 7.2953 | 2.5128, 5.1879 |
| two / uniform | out-of-core | 256 | 4.8842, 3.0717 | 3.4818, 5.3608 |
| two / no-index | materialized | 256 | 10.0839, 6.0462 | 7.1303, 9.5776 |
| two / no-index | out-of-core | 256 | 10.6216, 5.5489 | 5.5439, 6.4833 |
| two / refuse | materialized | 256 | 9.8691, 6.0881 | 6.0786, 9.7110 |
| two / refuse | out-of-core | 256 | 10.4905, 5.5468 | 5.5650, 10.6675 |
| one / skewed | materialized | 4 | 0.8769, 0.2243 | 0.2582, 0.8909 |
| one / skewed | out-of-core | 4 | 0.9563, 0.2631 | 0.3570, 0.9571 |
| two / skewed | materialized | 4 | 0.9657, 0.3316 | 0.3127, 1.0417 |
| two / skewed | out-of-core | 4 | 1.0675, 0.3633 | 0.3707, 1.0781 |
| point / skewed | materialized | 256 | 4.0286, 2.7683 | 2.8617, 3.9280 |
| point / skewed | out-of-core | 256 | 0.8836, 0.2212 | 0.2559, 0.8772 |

`plans.json` records direction, estimates, probes, prepared entries, read bytes/operations, scratch accounting and elapsed sampling time. Warm queries reuse the physical plan and do not resample. Uniform data keeps forward orientation; missing-index/point controls do not sample; zero-probe controls publish no partial estimates. Source/domain preparation is included, not hidden behind an isolated rank microbenchmark.

The skewed fixture selects reverse orientation and improves one/two-hop complete latency. Uniform and point results do not establish a benefit; inspect their actual deltas before enabling this on a host workload. The small four-seed cases also retain first-call overhead, including regressions. The materialized uniform two-hop median regresses about 7.5%, and the OutOfCore point median about 10.1% in this cohort; no universal or zero-overhead improvement is established. Default activation remains off.

`queries.csv` contains actual wall totals, parse time, preparation including sampling, sampling as a subset, and an execution/delivery/other remainder. The remainder is subtraction within the same observation, not isolated executor CPU. Process user/system CPU in `manifest.json` includes fixture construction and opens and must not be attributed to join execution. No per-query CPU or energy claim is made.

## Reproduction and build cost

See [the contract](../../QUERY_JOIN_SAMPLING.md#qualification) for the generator, budgets and arguments. `manifest.json` supplies every exact argument list and binary SHA-256; fixture setup/open times are separate. First worktree release build: 167.16 s; final source rebuild: 139.17 s, with shared registry/toolchain caches. Compilation is not query latency.

```console
cargo build --locked --release --features qualification --bench index_join_sampling
target/release/deps/index_join_sampling-<hash> two materialized 256 7 skewed
target/release/deps/index_join_sampling-<hash> two materialized 256 7 skewed candidate-first
target/release/deps/index_join_sampling-<hash> two out-of-core 256 7 skewed
target/release/deps/index_join_sampling-<hash> two out-of-core 256 7 skewed candidate-first
```

Larger-than-RAM qualification, checkpoint-base-plus-overlay sampling, broad predicate/path coverage and default-on activation remain open in [#1560](https://github.com/nowledge-co/hawdb/issues/1560).
