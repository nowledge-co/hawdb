# Shared-file follow-up and native x86 review

## Experiment

Issue [#1339](https://github.com/nowledge-co/hawdb/issues/1339), PR [#1346](https://github.com/nowledge-co/hawdb/pull/1346). Follow-up production source: `5a9330507b86fb3844a1900e48178a396c7a42be`. Baseline is `65a0011c20e0225fbe84f791996ea9f4add8b5a0`; initial SIMD production source is `be74735402536ba5e248160f300e6b755d0a58d8`. The initial report and adverse sequential-process samples remain [unchanged and available](../README.md).

This follow-up uses Apple M3 Max / macOS 27.0.1 and pinned Rust 1.97.1 / LLVM 22.1.6. All binaries use the repository-wide release profile (opt-level 3, thin LTO, codegen-units 1, unstripped). No CPU-native flags, target-local optimization, assertion/timeout changes, format changes or new dependency. Sources are separate immutable executables, with identical example SHA256 `75ce8adf44631758469a927559044bd187bdbfe8175caf242b2eb8aadb3b03c4`. Binary/source hashes and every fixture-file hash are retained in the environment manifests.

The baseline builds each persistent file fixture exactly once. All variants open the same native files; hashes are checked before and after. The fixture has 64/1,024 rows, 3/32/384/1,536 dimensions, deterministic signed vectors, empty text/metadata, top-k 10 and rank window 128. This compares 16 file query cases (default and exact) over 8 fixtures. Each variant/case has 3 fresh reader processes, 5 warmups per mode and 51 timed queries per mode/process. Modes and source order rotate/reverse between requests, balancing positions. Three variants produce 7,344 queries per cohort. Opening, fixture creation, pipe waits and JSON emission are outside each query timer; search and result hydration are inside. Every captured hit ID/score bit and work/read counter must agree, including on warmups.

Replay the protocol with identical copies of `examples/exact_vector_profile.rs` and its Cargo declaration on each source; baseline production files stay unchanged. Build with the pinned repository profile and copy each executable before switching a shared Cargo target directory. For a small manual replay:

```sh
cargo build --locked --release --example exact_vector_profile
./target/release/examples/exact_vector_profile build-file /tmp/hawdb-new-comparison-fixture 1024 384
printf 'default\nexact\nquit\n' | ./target/release/examples/exact_vector_profile file-worker /tmp/hawdb-new-comparison-fixture 1024 384
```

`build-file` requires a newly created path and retains its owned fixture for comparison. `file-worker` opens an existing fixture read-only, emits READY, then accepts default/exact/quit lines and emits one QUERY_RESULT per query. For the recorded matrix, start workers for all variants on the same fixture, warm each mode 5 times, then rotate the three source positions for 51 repetitions while reversing source/mode order on alternate rounds. Repeat with fresh workers 3 times. The experiment drivers/raw stdout/stderr remain outside Git; the protocol, binary/source/fixture hashes and every structured sample are retained here.

Captured invariants include full hit IDs/score bits; segment-range counts/bytes; metadata/vector/hydration bytes; raw-vector and projection payload bytes; hydrated rows/bytes; and the reported scored/reranked/working-set counters. Only the expected candidate-kernel label differs between old scalar and SIMD sources. These are library counters, not physical device I/O or RSS.

Three cohorts are retained: control compares baseline/initial-SIMD/isolated-noinline; contended compares baseline/initial-SIMD/decimal-only candidate; final compares baseline/initial-SIMD/follow-up. The noinline-only control showed no stable benefit and was not retained. The contended cohort suffered severe unrelated compilation/preemption (for example 15/78/164 ms p10/median/p90 for one case); its elapsed ratios are diagnostic, not qualification. Whole-worker CPU user/system time includes opening, warmup, JSON and both modes, so it is not per-query CPU latency. No task-owned builds/tests overlapped the timed cohorts. The final cohort observed two external compiler/formatter processes late in the run; the complete samples/ranges and timestamps are retained. The host is shared with no affinity or clock control, so the final numbers remain observations, not an exclusive-machine guarantee.

The component follow-up alternates 3 fresh initial/follow-up Core release processes, retaining all 2,916 arm observations. Query preparation remains included once per 128-candidate phase, with 9 rotating repetitions per arm. The frozen arithmetic reference omits some public setup and favors the reference. Every checksum must agree. Components use slices; complete file queries exercise canonical native graph inputs and file decoding.

## Results

Files ending in `cases.json` retain every timed sample and every captured invariant. Environment manifests retain sources, binaries, harness/fixture hashes and conditions. `core-before-after.json` retains all component samples/checksums. `contended-*` is deliberately labelled diagnostic; `final-worker-cpu.json` is whole-worker CPU evidence. No compiler, test or raw profiler logs are committed.

The same-file control does not reproduce the initial sequential cohort’s 21%–34% file differences. Initial SIMD queries in that control are approximately neutral or slightly faster (about 0.96–1.02x). This improves isolation but does not establish the cause of the earlier observations: the harness now opens a shared fixture and alternates individual requests. Do not silently delete or relabel the earlier samples.

The final table compares follow-up with the initial SIMD version. Values below 1 mean less elapsed time. Medians are the median of 3 process medians, each with 51 samples. Ranges are the minimum/maximum of 3 paired process-median ratios, not confidence intervals.

| Rows | Dimensions | Query | Initial SIMD median (µs) | Follow-up median (µs) | Follow-up / initial | Paired range |
| ---: | ---: | --- | ---: | ---: | ---: | --- |
| 64 | 3 | default | 97.0 | 95.2 | 0.982 | 0.982–1.005 |
| 64 | 3 | exact | 95.0 | 92.6 | 0.975 | 0.975–1.004 |
| 64 | 32 | default | 253.3 | 255.1 | 1.007 | 0.997–1.009 |
| 64 | 32 | exact | 245.6 | 246.9 | 1.005 | 0.997–1.008 |
| 64 | 384 | default | 2041.4 | 2024.6 | 0.992 | 0.992–1.016 |
| 64 | 384 | exact | 1991.4 | 1986.1 | 0.997 | 0.997–1.000 |
| 64 | 1536 | default | 7748.9 | 7709.5 | 0.995 | 0.995–1.003 |
| 64 | 1536 | exact | 7595.8 | 7635.2 | 1.005 | 0.999–1.005 |
| 1024 | 3 | default | 1124.0 | 1081.1 | 0.962 | 0.956–0.969 |
| 1024 | 3 | exact | 996.9 | 986.5 | 0.990 | 0.990–1.002 |
| 1024 | 32 | default | 3415.0 | 3107.8 | 0.910 | 0.908–0.910 |
| 1024 | 32 | exact | 3495.7 | 3502.8 | 1.002 | 0.992–1.011 |
| 1024 | 384 | default | 25505.4 | 21787.8 | 0.854 | 0.854–0.861 |
| 1024 | 384 | exact | 28164.6 | 28193.7 | 1.001 | 1.001–1.003 |
| 1024 | 1536 | default | 90958.4 | 76403.4 | 0.840 | 0.840–0.862 |
| 1024 | 1536 | exact | 101399.5 | 101257.8 | 0.999 | 0.988–1.006 |

For 3-dimensional prepared cosine distance on ARM, the component prepared/reference ratio changes from 1.495 to 1.104. The new direct scalar path avoids the generic prepared wrapper; public distance/error/checkpoint contracts are retained. ARM L2 already used direct scalar evaluation and remains approximately neutral.

[Initial native x86 reported summary](x86-reported-summary.json) preserves the first cohort. [Current native x86 reported summary](x86-current-reported-summary.json) transcribes the same [source-bound comment](https://github.com/nowledge-co/hawdb/pull/1346#issuecomment-6516743307), updated at **2026-10-30T08:20:30Z** with production `5a9330507b`. These are the native agent’s reported measurements; its raw samples and binary audits were not independently re-read on the ARM host. The Ryzen 7 7735HS / Garuda Linux host uses Rust 1.97.1 and the same repository release profile, without CPU-native flags. All sources use the updated harness SHA above.

The current x86 replay reports checked/release Core vector tests each 10 passed/1 manual ignored; all-feature Search raw math 2 passed, selection/layout 5 passed/3 manual ignored; and all-feature DiskANN 33 passed/1 ignored. All 2,916 primary component observations, 2,688 ordinary queries and 7,344 shared-file queries preserve the reported exact checksums/hit bits/work counters. Shared-file read/hydration counters and every fixture hash also agree. This is focused native qualification; its earlier broad Bazel attempt completed 9/101 targets before interruption, and no current full native x86 fuzz or full Clippy pass is claimed.

At 384–1,536 dimensions, current AVX2 prepared positive scoring takes 34.3%–35.6% of the original three-pass time and 60.7%–61.8% of prepared portable time. Current public cosine/reference is 0.753–0.769; public L2/reference is 0.873–0.911. Ordinary resident exact queries take 41.0%–56.1% of baseline in those dimensions. The 32-dimensional prepared L2 control remains adverse: 1.100x the frozen scalar reference and 1.302x the public unprepared distance.

The new x86 shared-file cohort uses three fresh readers per source/fixture, **3** warmups per mode and 51 timed queries per reader/mode (ARM uses 5 warmups). It rotates sources and alternates modes, keeping other readers waiting. Its 1,024-row default results independently support the new decimal validation path:

| Dimensions | Initial SIMD median (µs) | Follow-up median (µs) | Follow-up / initial | Paired range |
| ---: | ---: | ---: | ---: | --- |
| 32 | 4357.6 | 4003.1 | 0.919 | 0.910–0.938 |
| 384 | 32383.6 | 28874.6 | 0.892 | 0.878–0.892 |
| 1536 | 148114.2 | 132228.7 | 0.893 | 0.871–0.935 |

The all-selected exact controls show no consistent improvement. At 1,024 rows / 1,536 dimensions, shared-file exact current/baseline is 0.980 and current/initial SIMD is 0.993 (paired range 0.967–1.051), but the independent-fixture sequential cohort still records current/baseline **1.054** (paired range 1.037–1.073). The difference between methods remains unexplained; failure to reproduce the earlier +9.6% result in the shared-file control does not prove this patch fixed it. Shared-file 64-row / 3-dimensional exact also retains a **1.062x** current/initial result.

The unpinned tiny component cohort has substantial process variability and changing reference medians, so its reference-relative ratios are not a fixed-reference speedup estimate. A separate supplemental x86 control pins both binaries to logical CPU 2, performs an unmeasured full-process warmup per binary and counterbalances three fresh measured processes. For 4,096 3-dimensional cosine distances, prepared medians fall from **91.885 to 29.005 µs** (current/initial 0.316; paired range 0.314–0.330), while scalar references are 29.036 / 28.033 µs. All 2,916 supplemental component observations preserve checksums. This is a component-only 68.4% reduction; query cohorts remain unpinned, and clocks remain uncontrolled.

## Conclusions

- The default 1,024-row file cases improve by about 9%, 15%, 16% on ARM and 8%, 11%, 11% on x86 at 32, 384, 1,536 dimensions versus initial SIMD in the shared-file cohorts. Every paired default ratio in those cases is below 1; exact/all-selected controls show no consistent improvement. This matches the intended cost reduction: locating commas and validating common finite decimal digits in one scan for unselected coordinates, while retaining all-row checks and original parsing for uncommon forms. The retained adverse exact/sequential observations prevent a universal file-path improvement claim.
- Tiny prepared distances need a direct scalar path even after scalar ISA selection; choosing scalar alone did not remove wrapper overhead. ARM and the separate fixed-CPU native x86 component control support the improvement. SIMD implementation files are unchanged by this follow-up.
- AVX2 contributes a substantial independent improvement on the measured x86 CPU; ARM’s smaller independent NEON gain supports keeping profitability decisions specific to ISA, metric and shape. Neither result justifies weakening f64 ordering or enabling FMA.
- Four-candidate SIMD is a worthwhile next experiment for resident/raw scoring: assign lanes to separate candidates, preserving each candidate’s coordinate order and reusing query loads. Row-major transpose/loading costs, bounded buffers, cancellation/resource budgets, per-row errors and tiny/tail cases need qualification before enabling it. It is not implemented here.
- File queries remain dominated by reading/decompression/decimal decoding. Further kernel-only optimization cannot establish a file-route speedup. Reducing redundant validated payload decoding is a separate next step; do not bypass corrupt unselected rows or change persisted formats merely to improve a microbenchmark.
- No physical I/O, RSS, full Mem-route, device-cold, remote-CI or universal speedup is established. Native x86 summary and ARM samples concern specific sources/profiles/shapes; cross-compilation and interrupted broad runs remain distinct from execution qualification.
