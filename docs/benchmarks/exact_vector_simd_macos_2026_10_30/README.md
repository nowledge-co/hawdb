# Exact vector arithmetic on native ARM64


The [shared-file follow-up and native x86 review](followup/README.md) retain a stricter same-file control, current-head decimal/tiny fixes, and the earlier adverse observations. The initial tables below remain source-bound to their original implementation.

## Experiment

Issue: [#1339](https://github.com/nowledge-co/hawdb/issues/1339); implementation: [#1346](https://github.com/nowledge-co/hawdb/pull/1346). Baseline production source is `65a0011c20e0225fbe84f791996ea9f4add8b5a0`; candidate production source is `be74735402536ba5e248160f300e6b755d0a58d8`. Subsequent documentation changes do not change the measured implementation.

Apple M3 Max, macOS 27.0.1 (26A434), native `aarch64-apple-darwin`, pinned Rust 1.97.1 / LLVM 22.1.6. Both sources use the repository-wide release profile: opt-level 3, thin LTO, one codegen unit, assertions disabled and binaries unstripped. No native-CPU flags, target-specific optimization, timeout changes or private library copies were used. Binary/harness hashes and measurement conditions are in [environment.json](environment.json). Native x86 execution is delegated to a separate agent; cross-Clippy is not AVX2 runtime evidence.

No task-owned compilation or tests overlapped the accepted timing cohorts. This is a shared host without CPU pinning or clock control. Inputs and OS file caches are warm; this is not a device-cold or full Mem-route benchmark. Wall-clock query timing includes the library search and result hydration, but excludes embeddings, host REST handling, fixture generation, artifact creation, opening and warmup. Physical I/O, RSS, peak memory and full Mem performance were not measured.

An exploratory candidate release build of the earlier evolving implementation took 12m25s with cached external dependencies on the busy host; it was excluded from the accepted cohorts. The frozen candidate rebuild took 6m05s and the baseline build took 3m06s. These are observed build costs under different cache/load conditions, not a compile-speed comparison; none is included in query times.

Replay the component probe on the candidate:

```sh
cargo test --locked --release -p hawdb-core exact_distance_measurement -- --ignored --nocapture --test-threads=1
```

The component fixture has 128 deterministic signed f32 vectors for each of 3, 32, 384, 768, 1536 and 8193 dimensions. Query preparation is inside timing, once per 128-candidate phase. Each process rotates three arms over nine repetitions; three fresh processes yield 1,458 observations. For distance: arm 0 is the frozen original ordered scalar algorithm, arm 1 is the current public unprepared distance, arm 2 is the prepared query. The reference omits repeated public shape/callback setup, conservatively favoring it. For positive Search relevance: arm 0 is the original three-pass score, arm 1 is fused portable scoring with a prepared norm, arm 2 is prepared Auto. The compiler remains free to vectorize portable code. Each arm must produce the same exact checksum. Component measurements use slices, not byte-backed file distances. `automatic_kernel` records the current Auto selection, independently of reference/portable arms.

Replay complete queries with identical copies of the candidate example on both sources:

```sh
cargo run --locked --release --example exact_vector_profile
```

For the baseline, copy only `examples/exact_vector_profile.rs` and its Cargo example declaration from the candidate into a clean detached worktree at the baseline SHA; leave production code unchanged. Build both with the same toolchain/profile, and copy each executable to an immutable path before switching the shared Cargo target directory. Verify the example SHA256 matches [environment.json](environment.json).

The example uses only public embedded HawDB APIs. Fixtures cover 64/1,024 rows, 3/32/384/1,536 dimensions, persistent resident and file readers, and default DiskANN versus exact vector queries: 32 cases. Top-k is 10 and rank window is 128. Empty text/metadata and deterministic synthetic signed vectors keep the fixture reproducible. Each case checks one expected result and two warmups, then times 21 complete queries. Two fresh processes per source run in baseline, candidate, candidate, baseline order, giving 2,688 timed queries. Every hit ID and score bit, and all exposed work counters other than the selected kernel, must match across all sources/processes. Candidate and baseline use their own equivalent fixture directories; warm cache does not establish identical physical I/O latency.

For the x86 handoff, use the same implementation SHA and commands, record CPU/OS/toolchain and confirm `kernel:"avx2"` in common-dimension component output. Also run checked/release Core vector tests, Search `raw_vector_math_tests` and all-feature vector-projection `diskann` tests. No x86 performance claim is made here.

A separate diagnostic replay used `/usr/bin/sample <pid> 1 -file <outside-repo-path>` after the first file/1,024-row/1,536-dimensional default-query result. These diagnostic binaries ran concurrently with each other and Bazel compilation; their elapsed times are excluded. The candidate clip crossed into exact search while the baseline clip remained in default search. [file-profile-summary.json](file-profile-summary.json) retains collapsed sample counts, not raw profiler output. It supports qualitative hotspot identification only, not comparative CPU attribution.

## Results

[component-cases.json](component-cases.json) and [query-cases.json](query-cases.json) retain every timed sample, component checksum, full-query hit/score bits, source/process identity and reported work counters. The component checksums, all 32 query result shapes and all exposed work counts agree exactly. Kernel reporting changes from the old scalar label to the selected exact kernel where applicable.

Tables show candidate/reference elapsed-time ratios; below 1 is faster. Component summaries use the median of three process medians (nine observations per arm/process). Query summaries use the median of two process medians (21 observations per case/process). The final column shows the range of the two paired process ratios; it is not a confidence interval or latency percentile.

| Dimensions | Portable prepared / old positive | Auto prepared / old positive | Auto / portable prepared | Prepared cosine distance / old | Auto L2 / old |
| ---: | ---: | ---: | ---: | ---: | ---: |
| 3 | 1.669 | 3.410 | 2.042 | 1.458 | 1.104 |
| 32 | 1.418 | 0.848 | 0.598 | 0.817 | 1.005 |
| 384 | 0.465 | 0.437 | 0.941 | 0.955 | 0.998 |
| 768 | 0.390 | 0.381 | 0.976 | 0.966 | 1.001 |
| 1536 | 0.364 | 0.357 | 0.981 | 0.984 | 1.012 |
| 8193 | 0.345 | 0.346 | 1.004 | 0.979 | 0.998 |

The deliberately prepared 3-coordinate probe is slower; the private positive Search scorer uses the original scalar score below 16 coordinates. ARM L2 and norm calculation also retain scalar Auto paths. Thus these controls are not evidence that all metrics benefit from SIMD. Forced native correctness tests still exercise every mode and short tail. Prepared portable scoring is slower at 32 dimensions, while native NEON improves that measured control; scalar-only targets were compilation-qualified here, not performance-qualified.

| Residency | Rows | Dimensions | Query | Baseline median (µs) | Candidate median (µs) | Ratio | Paired ratio range |
| --- | ---: | ---: | --- | ---: | ---: | ---: | --- |
| file | 64 | 3 | default | 90.0 | 90.5 | 1.005 | 0.975–1.037 |
| file | 64 | 3 | exact | 87.3 | 88.3 | 1.011 | 0.985–1.039 |
| file | 64 | 32 | default | 248.9 | 247.5 | 0.994 | 0.976–1.013 |
| file | 64 | 32 | exact | 240.9 | 236.5 | 0.982 | 0.978–0.986 |
| file | 64 | 384 | default | 2088.5 | 2141.9 | 1.026 | 0.955–1.096 |
| file | 64 | 384 | exact | 2044.0 | 2082.8 | 1.019 | 0.965–1.073 |
| file | 64 | 1536 | default | 8124.1 | 7724.4 | 0.951 | 0.950–0.951 |
| file | 64 | 1536 | exact | 8119.5 | 7632.4 | 0.940 | 0.927–0.953 |
| file | 1024 | 3 | default | 1087.9 | 1085.4 | 0.998 | 0.989–1.007 |
| file | 1024 | 3 | exact | 981.2 | 984.9 | 1.004 | 0.975–1.032 |
| file | 1024 | 32 | default | 3338.2 | 4466.6 | 1.338 | 1.214–1.463 |
| file | 1024 | 32 | exact | 3458.2 | 4201.8 | 1.215 | 1.123–1.308 |
| file | 1024 | 384 | default | 25540.9 | 25267.7 | 0.989 | 0.983–0.996 |
| file | 1024 | 384 | exact | 29221.0 | 28089.4 | 0.961 | 0.949–0.974 |
| file | 1024 | 1536 | default | 90333.3 | 115934.2 | 1.283 | 1.089–1.475 |
| file | 1024 | 1536 | exact | 103059.8 | 109361.6 | 1.061 | 0.998–1.124 |
| resident | 64 | 3 | default | 32.5 | 33.7 | 1.036 | 0.977–1.101 |
| resident | 64 | 3 | exact | 28.0 | 27.4 | 0.979 | 0.956–1.003 |
| resident | 64 | 32 | default | 39.6 | 41.0 | 1.036 | 1.028–1.044 |
| resident | 64 | 32 | exact | 30.7 | 31.6 | 1.027 | 1.009–1.046 |
| resident | 64 | 384 | default | 135.2 | 102.8 | 0.760 | 0.706–0.812 |
| resident | 64 | 384 | exact | 119.6 | 72.9 | 0.609 | 0.583–0.634 |
| resident | 64 | 1536 | default | 443.1 | 236.7 | 0.534 | 0.529–0.540 |
| resident | 64 | 1536 | exact | 465.2 | 178.8 | 0.384 | 0.379–0.389 |
| resident | 1024 | 3 | default | 591.1 | 609.2 | 1.031 | 1.030–1.031 |
| resident | 1024 | 3 | exact | 576.4 | 560.6 | 0.973 | 0.931–1.018 |
| resident | 1024 | 32 | default | 693.4 | 783.9 | 1.130 | 0.951–1.323 |
| resident | 1024 | 32 | exact | 584.9 | 639.1 | 1.093 | 1.009–1.180 |
| resident | 1024 | 384 | default | 1232.0 | 1115.5 | 0.905 | 0.905–0.906 |
| resident | 1024 | 384 | exact | 1970.7 | 1312.8 | 0.666 | 0.643–0.689 |
| resident | 1024 | 1536 | default | 2899.0 | 2294.8 | 0.792 | 0.747–0.839 |
| resident | 1024 | 1536 | exact | 7535.9 | 3067.8 | 0.407 | 0.388–0.427 |

The diagnostic stack clips are dominated by Zstd decompression, embedding string splitting/float parsing, validation and comparisons. The candidate clip contains seven top-of-stack NEON-kernel samples out of 752 main-thread samples. This suggests that optimizing distance arithmetic alone will not remove the file-query bottleneck; mixed phases and compilation contention prevent using these clips to prove the cause of the measured regression.

## Conclusions

- The measured resident 384/1,536-dimensional exact queries take about 38%–67% of baseline time; default resident queries take about 53%–91%. This supports query-norm reuse/fused scoring for those synthetic resident workloads.
- Positive-score arithmetic at 384–1,536 dimensions takes about 36%–44% of the original three-pass time. Most of that comes from reuse/fusion: incremental native NEON benefit over prepared portable scoring is about 1%–6% there, and the 8,193-dimensional control is slightly slower. Ordinary unprepared cosine distance is approximately neutral. These are distinct from the existing RaBitQ SIMD measurements.
- Small complete-query cases are neutral or slower. File queries do not establish an overall improvement: the 1,024-row 32-dimensional cases are 21%–34% slower, and the 1,536-dimensional default case is 28% slower in this cohort. Keep these adverse results; the resident/component gain does not qualify a file-query speedup. File-path attribution remains required before claiming a universal improvement.
- Coordinate-order f64 accumulation, direct positive scores and scalar fallbacks preserve observed score bits and ties. Runtime CPU guards provide portability; they do not substitute for native x86 execution or for CPU-specific profitability measurements.
- No reduction in physical I/O/RSS, durability change, storage-format change, full Mem-route gain or remote CI speedup is established by this experiment.
