# RaBitQ Vector Projection Benchmark

RaBitQ remains an explicitly selectable compressed candidate backend; the
default DiskANN engine has separate scoring. An explicitly selected RaBitQ
codec is also honored by the ordinary default search entrypoint. Historical
measurements below describe the artifact version and source recorded with each
run; they do not qualify the new version-2 inverse-alignment or L2 estimator. The existing RaBitQ memory and file scan paths select AVX2 automatically on capable
x86/x86_64 CPUs. AArch64 also has a NEON scorer, selected explicitly with
`KernelPreference::Neon`; automatic selection remains scalar there until native
qualification is complete. Other CPUs retain the portable scalar scorer. Both
the default 4-bit and optional 1-bit encodings are supported without changing artifacts,
public APIs, candidate ordering, or the raw-vector reranking stage.

## Faiss SIMD adaptation

The scorer adapts the eight-coordinate sign expansion in
[Faiss `multibit::ip_bitplane_avx2`](https://github.com/facebookresearch/faiss/blob/71c7f482d2da0aa1fbed179485818184a5d7df42/faiss/utils/simd_impl/rabitq_avx2.cpp).
For HawDB's three refinement bits, AVX2 variable shifts unpack eight codes from
exactly three bytes. This replaces upstream BMI2 PEXT and avoids its padded
wide reads. Products use SIMD, but additions retain scalar coordinate order:
there is no FMA, horizontal reassociation, or numerical tolerance relaxation.
The remaining zero to seven coordinates use the scalar decoder. Runtime
feature detection and slice bounds guard the unsafe intrinsic calls.

Upstream MIT notices are retained in
[`kernel/avx2.rs`](../crates/vector-projection/src/kernel/avx2.rs) and
[`kernel/neon.rs`](../crates/vector-projection/src/kernel/neon.rs); crate metadata
declares `Apache-2.0 AND MIT`. No C++ runtime or new dependency is introduced.
Faiss FastScan, IVF/PQ/OPQ, GPU indexes, and collector changes are not included:
they do not supply a verified drop-in improvement to this existing scan path.

The NEON version adapts
[Faiss `multibit::ip_multibit_neon`](https://github.com/facebookresearch/faiss/blob/71c7f482d2da0aa1fbed179485818184a5d7df42/faiss/utils/simd_impl/rabitq_neon.cpp).
It specializes the three refinement bits to two four-lane 32-bit shifts and
keeps the same scalar accumulation order and bounded tail reads as AVX2.
32-bit Arm retains its previous scalar fallback.

## Local full-scan benchmark

```bash
cargo test --locked --release -p hawdb-vector-projection \
  simd_ -- --ignored --nocapture --test-threads=1
```

These ignored local benchmarks are excluded from routine CI. The scan benchmark
compares the unchanged scalar scorer with explicit native AVX2 or NEON through
complete `InMemoryProjection::search` and `FileProjection::search` calls. Timings include
query transformation, filtering, scoring, top-k selection, and worker setup
and merging. They exclude projection construction, artifact open, and final
raw-vector reranking by the embedded database facade.

The deterministic workload has 4,096 rows in four segments, duplicate vectors
for tie handling, top-k 10, and three queries. It covers 1-bit and 4-bit codes;
384, 768, 769, and 1,536 dimensions; 1% and 50% allowlists plus unfiltered
scans; and one or four requested workers. Sparse admission can reduce four
requested workers to one. Each case warms both paths, alternates execution
order across 15 samples, and reports median microseconds. File mappings use a
warm OS page cache. Payload/query CRC32 checksums, actual worker counts,
scored rows, and admitted working bytes are emitted with each result.
Keep raw logs outside Git.

Every measured query asserts bit-identical scores and identical candidate IDs.
All report fields except the selected kernel must match, including memory
admission. The benchmark also checks packed-code parity across dimension tails,
unaligned inputs, signed zeros, subnormals, and mixed finite magnitudes.
Ordinary CI additionally covers all code values and lanes, short input rejection,
empty/sparse/dense allowlists, ties, multiple top-k limits, and parallel merging.

Local observation (2026-10-27): Linux x86_64, AMD Ryzen 7 7735HS, pinned Rust
1.97.1, repository release profile, base
`8300b87f609564ad55ad47c4a129ce6d0d899447`. Three independent runs each
covered all 96 cases with 15 interleaved samples per kernel and case. The table
and ranges use the median of the three per-run medians. Task-owned compilation
was stopped during measurement; the shared host was not CPU-isolated. This is a
synthetic development measurement, not representative Mem workload or production
qualification. Speedup ranged from 3.54–7.01x for 1-bit and 3.88–6.93x for 4-bit,
with no measured regression in this matrix. All 288 comparisons preserved exact
scores, IDs, and reports; payload/query checksums matched across runs.
The following subset uses default 4-bit codes, one worker, and no allowlist:

| Dimensions | Backend | Scalar median (us) | AVX2 median (us) | Speedup |
| ---: | --- | ---: | ---: | ---: |
| 384 | Memory | 7264.7 | 1069.0 | 6.80x |
| 384 | File | 7259.7 | 1071.3 | 6.78x |
| 768 | Memory | 14456.0 | 2107.1 | 6.86x |
| 768 | File | 14467.0 | 2111.3 | 6.85x |
| 769 | Memory | 14475.9 | 2128.3 | 6.80x |
| 769 | File | 14473.8 | 2123.7 | 6.82x |
| 1536 | Memory | 28950.1 | 4176.8 | 6.93x |
| 1536 | File | 29047.7 | 4190.4 | 6.93x |

The kernel adds no heap allocation or per-query lookup table. Existing
32-row cancellation checkpoints remain unchanged. A companion benchmark measures
response to a 1 ms deadline on warm, unfiltered 8,192-row, 1,536-dimensional
scans for both widths, backends, and one/four workers. It reports the median and
maximum deadline overshoot over 15 searches, plus cancelled/completed counts.
These observations include OS scheduling and do not impose a universal latency
threshold. Peak process RSS, cold I/O, full facade latency, and production recall
are outside these local probes. Across three runs, all 360 searches per kernel reached
the deadline. Taking the median of the three per-run case medians, scalar
overshoot ranged from 139.0–244.5 us versus 14.9–56.3 us for AVX2. The worst
observed overshoot across all runs was 514.7 us for scalar and 114.6 us for AVX2.
No ARM/NEON throughput claim is made.

## Initial development validation

The existing Cargo/Bazel crate-test targets discover the new parity tests.
Native platform execution remains required in CI, including explicit NEON parity
on AArch64 and AVX2 parity on capable Linux/Windows x86_64 runners. The ignored
release benchmark is local performance evidence, not a substitute for those
platform checks or representative workload qualification.

Initial local validation on base
`804f1a6139f73470c58167411f6cad871efffdbf` plus the SIMD adaptation is recorded
below. The PR description records checks after porting to current `main`.

- `cargo fmt --all -- --check`: passed.
- `cargo clippy --locked -p hawdb-vector-projection --all-targets --all-features --target <target> -- -D warnings`:
  passed for `x86_64-unknown-linux-gnu`, `aarch64-unknown-linux-gnu`, and
  `x86_64-pc-windows-gnu`. Cross-target checks do not establish native runtime parity.
- The two ignored release benchmarks above: all 96 scan comparisons,
  packed-code differential checks, and 16 deadline-response cases passed.
- `bazel test //crates/fuzz:hawdb_fuzz_tests //crates/fuzz:hawdb_fuzz_cli_tests //:hawdb_linux_ci_fuzz_smoke_test`:
  passed 95 targets (33 executed, 62 cached) after AVX2 integration. The later
  AArch64-only kernel extension is cross-checked above; native NEON execution
  remains pending.
- Ordinary crate regression and native platform tests are registered with
  existing CI targets; they were not run locally and CI results are pending.

## Native AArch64 qualification

Run the same command above on the ARM machine with the pinned Rust toolchain.
It must emit `"kernel":"neon"` for the scan benchmark and validate every timed
query against scalar results. No target flags or C++ dependencies are needed.
The command runs benchmarks serially to avoid measuring their CPU contention.
For the ordinary regression corpus, use:

```bash
cargo test --locked -p hawdb-vector-projection kernel::
```

Record CPU/OS, compiler, build profile, complete benchmark output, and any
regressions. Native correctness and speedup are pending; cross-compilation alone
does not authorize changing AArch64 `Auto` to NEON. Existing application options
can select `VectorSearchKernelPreference::Neon` explicitly for host measurements.

## Full-query SIMD comparison

The existing profiling example also supports an interleaved comparison through
`SearchOutOfCoreReader`'s public compressed-vector query API. This includes
candidate scanning, raw-vector reranking, and result hydration. The fixture has
100,000 deterministic 384-dimensional vectors, default 4-bit codes, unfiltered
top-k 10, rank window 100, and one configured worker. Three queries warm both kernels;
31 measured samples per kernel alternate execution order. Final hit IDs and
score bits must match exactly. Warmups also compare scored/reranked counts,
raw-vector bytes, admitted candidate working bytes, and layer-worker counts.
Open/build and result validation are outside the timers. The emitted JSON retains
every timing.

Choose an unused fixture directory on a real filesystem, keep raw output outside
Git, and stop competing compilation before measurement:

```bash
mkdir -p .scratch
cargo run --locked --release --example rabitq_query_profile -- \
  build .scratch/rabitq-simd-query-fixture
cargo run --locked --release --example rabitq_query_profile -- \
  compare .scratch/rabitq-simd-query-fixture
```

The comparison explicitly selects AVX2 on x86 or NEON on AArch64 and rejects
unsupported hardware. It measures explicit RaBitQ searches, not default DiskANN
or whole application request latency. The original scalar `profile` mode and
its historical evidence below remain unchanged.

On 2026-10-27, the same Ryzen 7 7735HS / Linux x86_64 / Rust 1.97.1 host ran
three independent comparisons on `8300b87f609564ad55ad47c4a129ce6d0d899447`
plus the comparison mode introduced here, using the repository release profile.
Competing task-owned builds were stopped; the host was shared and the file cache
was warm. The build report recorded 100,000 vectors, 4-bit codes, generator seed
`0x994020266f9131c5`, and 20,821,407 RaBitQ artifact bytes.

| Run | Scalar p50 (ms) | AVX2 p50 (ms) | Scalar p95 (ms) | AVX2 p95 (ms) | p50 speedup |
| ---: | ---: | ---: | ---: | ---: | ---: |
| 1 | 1263.7 | 1114.3 | 1285.4 | 1148.4 | 1.134x |
| 2 | 1272.2 | 1123.1 | 1288.5 | 1140.9 | 1.133x |
| 3 | 1264.4 | 1113.6 | 1288.7 | 1137.7 | 1.135x |

Taking the median of the three per-run percentiles gives p50 1264.4 -> 1114.3 ms
and p95 1288.5 -> 1140.9 ms: 1.135x p50 speedup, or 11.9% lower median latency.
Percentiles use nearest rank. All 186 timed queries preserved exact final IDs
and score bits. Warmup reports each scored 100,000 vectors and reranked 1,024 candidates,
with 1,572,864 reported raw-vector bytes, 904,704 admitted candidate working bytes,
and a layer-worker sum of 13 (sequential layers, not simultaneous workers).
The combined comparison processes peaked at 42,020–42,972 KiB RSS, measured by
`wait4`; this is whole-process residency, not a per-kernel memory comparison.

## Earlier scalar baseline and full-query profile

The following baseline and end-to-end profile predate this SIMD port. Their
source revisions and evidence remain unchanged. Candidate-scan speedups above
are not whole-query or default-DiskANN speedups.

## Required Measurements Before SIMD Dispatch

Any native SIMD implementation must be measured against the scalar reference
on the exact same checksummed artifact and query corpus. The report must cover:

- candidate-ID and score-tolerance parity for unfiltered and sparse/dense
  allowlists;
- both 1-bit and 4-bit artifacts, including an odd dimension that crosses a
  refinement-byte boundary;
- dimensions 384, 768, and 1,536, plus an odd dimension;
- in-memory and file-backed scans;
- Linux x86_64 for AVX2 and native AArch64 for NEON;
- throughput, peak admitted working bytes, and cancellation latency.

The scalar path remains the baseline. Raw-vector reranking and recall against
canonical vectors are qualified separately; a faster candidate kernel cannot
change final ranking semantics.

## Reproduction

The ignored `kernel::benchmark::candidate_scan_matrix` test records a manual
scalar baseline. It uses 100,000 and 1,000,000 fixed-seed unit vectors, dimensions
67/384/768/1,536, both bit widths, unfiltered and 1%/50% allowlists, and one/four
requested workers. Every case keeps one warmup and five measured samples.
Requested and admitted workers are reported separately: a sparse allowlist may
admit fewer workers under the unchanged resource policy.

The two phases have different scopes:

- `score_codes` measures the single-threaded integer-code inner-product loop,
  including iteration, a function call and a score-bit accumulator. It excludes
  reconstruction, filtering, top-k, query transformation and fixture creation.
- `projection_search` measures a complete `search()` call over the in-memory
  or file-backed projection. Result validation and destruction are outside the
  timer. Backend order alternates by sample, and candidate IDs plus score bits
  must match the single-worker file reference for each filter.

Both backends consume the same checksummed persisted codes. The in-memory
fixture decodes the artifact instead of rebuilding its quantization. Artifacts
and JSONL outputs are retained on failure, and existing case directories or
output files are never overwritten. Reuse mode fails if an artifact is missing
or does not match the requested case. Fixture/code residency is outside the
reported per-query admitted working bytes; neither value is measured RSS.

Choose an artifact directory on a real filesystem when qualifying the file
backend. The full matrix builds roughly two GiB of artifacts; `/tmp` may be a
memory filesystem. For example, from the repository root:

```sh
mkdir -p .scratch/rabitq-baseline
HAWDB_RABITQ_BENCH_ARTIFACTS="$PWD/.scratch/rabitq-baseline/artifacts" \
HAWDB_RABITQ_BENCH_OUTPUT="$PWD/.scratch/rabitq-baseline/scalar-linux.jsonl" \
HAWDB_RABITQ_BENCH_BUILD=1 \
cargo test --locked --release -p hawdb-vector-projection --features storage-io \
  kernel::benchmark::candidate_scan_matrix -- --exact --ignored --nocapture --test-threads=1
```

Set `HAWDB_RABITQ_BENCH_BUILD=0` and a fresh output filename to reuse the same
artifacts. `HAWDB_RABITQ_BENCH_CASE` optionally selects one case in `0..16`,
ordered by document count, dimension, then bit width as listed above. A selected
case is partial evidence; the full baseline requires all 16 cases. These
environment variables configure the test harness only.

Audit and summarize a completed run with:

```sh
python3 scripts/summarize-rabitq-baseline.py \
  .scratch/rabitq-baseline/scalar-linux.jsonl \
  .scratch/rabitq-baseline/scalar-linux-summary.json
```

The auditor requires every timing group, one warmup and five measurements,
stable candidate bits, valid resource accounting, and a completion marker. It
retains all measured timings and hashes each artifact. Use `--allow-partial`
only for a selected-case probe; its summary is explicitly marked partial.
Use `--artifacts-root` when re-auditing artifacts downloaded to a new location.

The existing Native Bindings workflow has an optional manual baseline mode:

```sh
gh workflow run native-bindings.yml --ref BRANCH --field rabitq_baseline=true
```

This mode records the full matrix on Linux x86_64 and native macOS arm64,
checks the runner architecture, and uploads the raw samples, environment,
summary and original artifacts for 14 days. It is not part of ordinary PR or
push checks. Archive the evidence before it expires when comparing a later
kernel against these exact artifacts.

## Full Query Profiling

The `rabitq_query_profile` example uses the public embedded library facade to
build a separate 100,000-document, 384-dimension, default 4-bit serving
generation. Its fixed-seed xorshift64* unit vectors differ from the scan
matrix's SplitMix64 corpus. The unfiltered query requests ten final hits with
`rank_window = 100`, one worker and default resource budgets. Actual generated
and reranked candidate counts follow the serving policy and are reported;
the requested rank window is not assumed to equal those counts. The helper
verifies that RaBitQ candidate scanning and exact raw-vector reranking both
run, then checks that final hit IDs and score bits remain stable.

```sh
CARGO_PROFILE_RELEASE_DEBUG=1 cargo build --locked --release \
  --example rabitq_query_profile --features vector-search
target/release/examples/rabitq_query_profile build .scratch/rabitq-serving-profile
target/release/examples/rabitq_query_profile profile .scratch/rabitq-serving-profile
```

After three warmups, the helper prints a `ready` record with its PID and query
metrics, then waits for `run` on stdin. Attach a native sampling profiler to
that PID before sending the command. For example, on Linux, run the following
in another terminal and then send `run` to the helper:

```sh
perf record -e cpu-clock:u -F 997 --call-graph dwarf,16384 \
  --pid QUERY_PID --output .scratch/rabitq-query.perf.data
perf report --stdio --inline --input .scratch/rabitq-query.perf.data
```

The helper executes 100 full serving queries after the handshake, reports
each elapsed time, and exits. Sampled user CPU shares include the query loop
and result verification; they are not exact wall-time attribution. Record the
profiler version, event, frequency, lost samples, symbol coverage and binary
hash. The profiling build retains debug information; the ordinary scan matrix
uses the unchanged repository release profile.

The scan matrix alone does not establish end-to-end vector-query profile
share. Dividing its two phase timings is not a substitute for the separate
full-query profile below. Native-platform results and final SIMD qualification
remain separate evidence requirements for #994.

```bash
cargo test -p hawdb-vector-projection
cargo test -p hawdb-qualification production_vector
```

Future benchmark commands and results belong in this document only after their
matching implementation and scalar-parity tests are present.

## Recorded Linux Scalar Baseline

The first complete matrix was measured on 2026-10-17 21:17-21:30 UTC using
main `4c32610910cc78771eeb23d729be7107ff769bf6` plus this manual benchmark.
Kernel arithmetic and dispatch are unchanged. The timed source hashes and
retained binary hash are in the [environment record](benchmarks/rabitq_scalar_linux_2026_10_18/environment.json).
The current kernel, codec, scan and benchmark source hashes match that record.

Environment: Linux x86_64, AMD Ryzen 7 7735HS, 16 logical CPUs, roughly 28 GiB
RAM, pinned Rust 1.97.1, repository release profile, `storage-io`, and an NVMe
Btrfs artifact directory. This is a shared host, not an idle-host qualification.
One-minute load averages before/after were 1.84/1.87. No owned compiler or fuzz
process overlapped the matrix; light source editing, formatting and workflow
validation did occur during the overall run. The file scan is warm-cache.

All 16 corpora and 208 timing groups completed. The independent auditor checked
all 1,266 records, complete warmup/repetition sets, exact returned candidate IDs
and score bits across backends/workers, eligible counts and admitted memory.
No failed sample was omitted. Kernel score checksums are timing controls, not
full per-vector native-kernel parity. The separate full-query profile is below.
The additional native-run evidence is recorded below; this evidence does not complete #994.

- [Raw records](benchmarks/rabitq_scalar_linux_2026_10_18/samples.jsonl)
- [All timings, medians, ranges and artifact SHA-256 values](benchmarks/rabitq_scalar_linux_2026_10_18/summary.json)

Single-worker `score_codes` timings below are nanoseconds per vector. Ranges
cover all five measured repetitions; warmup remains in the raw records.

| Vectors | Dimension | Bits | Median ns/vector [min..max] | Million vectors/s |
| ---: | ---: | ---: | ---: | ---: |
| 100,000 | 67 | 1 | 48.30 [47.66..49.58] | 20.702 |
| 100,000 | 67 | 4 | 154.31 [153.90..154.68] | 6.480 |
| 100,000 | 384 | 1 | 271.50 [270.99..272.35] | 3.683 |
| 100,000 | 384 | 4 | 858.68 [857.87..860.11] | 1.165 |
| 100,000 | 768 | 1 | 537.75 [536.92..540.37] | 1.860 |
| 100,000 | 768 | 4 | 1709.34 [1708.71..1711.34] | 0.585 |
| 100,000 | 1,536 | 1 | 1070.68 [1064.84..1072.51] | 0.934 |
| 100,000 | 1,536 | 4 | 3376.76 [3362.47..3380.25] | 0.296 |
| 1,000,000 | 67 | 1 | 48.28 [47.56..48.38] | 20.714 |
| 1,000,000 | 67 | 4 | 155.14 [155.05..156.53] | 6.446 |
| 1,000,000 | 384 | 1 | 271.11 [270.01..274.89] | 3.688 |
| 1,000,000 | 384 | 4 | 850.39 [847.75..852.22] | 1.176 |
| 1,000,000 | 768 | 1 | 534.85 [533.04..537.64] | 1.870 |
| 1,000,000 | 768 | 4 | 1697.49 [1694.22..1706.01] | 0.589 |
| 1,000,000 | 1,536 | 1 | 1072.61 [1063.92..1073.28] | 0.932 |
| 1,000,000 | 1,536 | 4 | 3245.96 [3240.99..3252.95] | 0.308 |

These are scalar baseline rates, not speedups or end-to-end Mem request rates.
The full projection-search groups, including both filters and actual admitted
worker counts, are retained in the linked summary.

## Recorded Full-Query Profile

A separate public-API serving fixture on the same Linux host was profiled with
`perf 7.2.11-1`, `cpu-clock:u`, 997 Hz, and 16 KiB DWARF call stacks. The build
uses the repository release settings plus `CARGO_PROFILE_RELEASE_DEBUG=1`.
Three warmups precede profiler attachment; the driver enables events and waits
for the acknowledgement before starting 100 repeated queries. The capture
includes query execution, result validation and loop overhead. It excludes
fixture construction, opening the reader and warmups.

The query requested ten results and `rank_window = 100`. The actual report
identified the RaBitQ out-of-core candidate backend and raw-vector final
scores: 100,000 quantized vectors scored, 1,024 candidates reranked, 1,572,864
selected raw-vector bytes, and 904,704 admitted candidate working bytes.
Configured maximum parallelism is one. The worker report sums 13 sequential
artifact layers; it is not thirteen simultaneous workers. The raw-vector byte
counter counts selected embeddings, not all decoded segment data, and the
admitted byte value is not process RSS.

All 100 query iterations preserved final hit IDs and exact score bits. Their
profiled wall times were 1,501.66 / 1,509.96 / 1,557.62 ms (min/median/max).
These include sampling overhead and are not unprofiled production latency.
The capture contains 149,183 samples and reports 22 lost samples in two lost
chunks (0.0147% of recorded plus lost samples). Unresolved leaf addresses
account for 6.915% of sampled user CPU weight; their cost is not assigned to a
HawDB subsystem.

| Resolved self symbol | Sampled user CPU weight |
| --- | ---: |
| `ZSTD_decompressSequences_bmi2.constprop.0` | 21.260% |
| Character splitting (`CharSearcher::next_match`) | 15.266% |
| Decimal number parsing (`parse_number`) | 14.097% |
| `f32::from_str` | 13.630% |
| Float conversion (`compute_float<f32>`) | 5.406% |
| `decode_embedding` | 4.380% |
| `score_codes` | 5.415% |

These are non-overlapping self weights, not inclusive call-tree shares or
exact wall-time attribution. The profile does not justify optimizing the raw
`dot_product` arithmetic as #994's optional fourth slice. It also shows why a
candidate-only throughput gain cannot be presented as the same end-to-end gain.

Source inspection explains the larger preparation cost:
`SearchOutOfCoreReader::read_vector_segment` decompresses a touched sidecar and
`decode_vector_segment` parses all of its textual embeddings before the
RaBitQ rerank loop filters to selected ordinals. Parsing also validates finite
values, dimensions, IDs, bounds and order. A follow-up must preserve those
integrity checks; simply skipping unselected malformed rows is not an
acceptable optimization. No decoder, format, scoring arithmetic or admission
policy is changed in this baseline.

Evidence:

- [Query reports, all latency samples and binary/perf hashes](benchmarks/rabitq_scalar_linux_2026_10_18/profile.json)
- [Exact symbol counts, weights and unresolved coverage](benchmarks/rabitq_scalar_linux_2026_10_18/symbol-summary.json)
- [Complete flat symbol table](benchmarks/rabitq_scalar_linux_2026_10_18/flat-symbols.tsv)
- [Call graph](benchmarks/rabitq_scalar_linux_2026_10_18/callgraph.txt)
- [Recorder warnings and sample totals](benchmarks/rabitq_scalar_linux_2026_10_18/perf.log)
- [Profile build provenance](benchmarks/rabitq_scalar_linux_2026_10_18/profile-build.json)
- [Fixture build report](benchmarks/rabitq_scalar_linux_2026_10_18/profile-fixture.json)
- [Exact local sampling driver](benchmarks/rabitq_scalar_linux_2026_10_18/profile_driver.py), with its [hash helper module](benchmarks/rabitq_scalar_linux_2026_10_18/audit_records.py)

The checked-in text reports strip trailing whitespace; symbol fields and
sample counts are unchanged. Original exports remain with the raw capture.
The 2.31 GiB raw `perf.data` remains outside Git. Its SHA-256 is
`88cd555205c18d4133e9dc1635c59252b8252172281bde691662a84c938453bd`.
The first attachment attempt stopped before the query loop because the driver
rejected perf's NUL-terminated acknowledgement; it is retained separately and
contributes no measurements to this report. Native artifact audits are recorded below. The remaining scalar/native-kernel
qualification gates are still open.


## Native Runner Baselines

The optional [manual workflow run](https://github.com/nowledge-co/hawdb/actions/runs/39074306598)
measured commit `3ae50b58614d8b15e7dc0ca5210e091c070c2e10` on native Linux
x86_64 and macOS arm64. Both jobs completed successfully on 2026-10-17 UTC.
The workflow verifies runner architecture and uses pinned Rust 1.97.1 with
`storage-io` and the unchanged repository release profile. The kernel remains
scalar on both architectures; these are not AVX2 or NEON results.

Both downloads were independently re-audited: each run has all 16 corpora,
208 timing groups and 1,266 records satisfying the complete protocol. Candidate
IDs, exact score bits, eligibility and resource accounting match across backend/worker
variants within that run. All 16 downloaded artifact SHA-256 values and each
recomputed summary match the corresponding runner audit. No sample was
discarded. The first macOS download ended with an unexpected EOF before
extraction; one fresh-connection retry succeeded without rerunning benchmarks.

The platforms also produced identical hashes for all 16 persisted artifacts.
Comparing all 1,248 timing records across platforms found identical returned
candidate IDs/score bits and kernel score checksums. This checks returned top-k
parity and the timing accumulator; it is not full per-vector native-kernel
parity. The [cross-platform accounting](benchmarks/rabitq_scalar_native_2026_10_18/platform-comparison.json)
records that scope.

Linux environment: Ubuntu 24.04, AMD EPYC 7763 VM, four logical CPUs, about
15.6 GiB RAM, x86_64-unknown-linux-gnu Rust host. macOS environment: Darwin
25.6.0 arm64, Apple M1 virtual machine, three logical CPUs, 7 GiB RAM and an
aarch64-apple-darwin Rust host. Both are native architecture jobs; the macOS
runner is virtualized, not a bare-metal idle-host qualification. The file-backed
scans use warm-cache artifacts on each runner's filesystem. Hosted scheduling and
virtualization differ from the local Ryzen baseline; cross-host rates are not
an optimization speedup. No process RSS or cancellation latency is inferred
from admitted working-byte counters.

| Corpus | Linux median ns/vector | macOS median ns/vector |
| --- | ---: | ---: |
| `n100000-d67-b1` | 62.71 | 87.01 |
| `n100000-d67-b4` | 218.56 | 153.51 |
| `n100000-d384-b1` | 362.55 | 541.44 |
| `n100000-d384-b4` | 1207.16 | 998.53 |
| `n100000-d768-b1` | 724.96 | 1117.75 |
| `n100000-d768-b4` | 2404.81 | 1971.11 |
| `n100000-d1536-b1` | 1441.77 | 2239.90 |
| `n100000-d1536-b4` | 4802.43 | 4207.87 |
| `n1000000-d67-b1` | 62.48 | 59.99 |
| `n1000000-d67-b4` | 218.40 | 233.02 |
| `n1000000-d384-b1` | 362.98 | 363.33 |
| `n1000000-d384-b4` | 1207.40 | 872.39 |
| `n1000000-d768-b1` | 722.51 | 812.02 |
| `n1000000-d768-b4` | 2407.42 | 1710.06 |
| `n1000000-d1536-b1` | 1442.64 | 1551.43 |
| `n1000000-d1536-b4` | 4801.82 | 3911.92 |

Linux evidence:

- [Raw records](benchmarks/rabitq_scalar_native_2026_10_18/linux/samples.jsonl)
- [Every timing group and artifact hash](benchmarks/rabitq_scalar_native_2026_10_18/linux/audit.json)
- [CPU](benchmarks/rabitq_scalar_native_2026_10_18/linux/cpu.txt),
  [system](benchmarks/rabitq_scalar_native_2026_10_18/linux/system.txt),
  [toolchain](benchmarks/rabitq_scalar_native_2026_10_18/linux/toolchain.txt),
  [source revision](benchmarks/rabitq_scalar_native_2026_10_18/linux/source.txt)
- [Harness output](benchmarks/rabitq_scalar_native_2026_10_18/linux/run.log)
- [Workflow/artifact provenance and text-copy hashes](benchmarks/rabitq_scalar_native_2026_10_18/linux/provenance.json)

macOS evidence:

- [Raw records](benchmarks/rabitq_scalar_native_2026_10_18/macos/samples.jsonl)
- [Every timing group and artifact hash](benchmarks/rabitq_scalar_native_2026_10_18/macos/audit.json)
- [CPU](benchmarks/rabitq_scalar_native_2026_10_18/macos/cpu.txt),
  [system](benchmarks/rabitq_scalar_native_2026_10_18/macos/system.txt),
  [toolchain](benchmarks/rabitq_scalar_native_2026_10_18/macos/toolchain.txt),
  [source revision](benchmarks/rabitq_scalar_native_2026_10_18/macos/source.txt)
- [Harness output](benchmarks/rabitq_scalar_native_2026_10_18/macos/run.log)
- [Workflow/artifact provenance and text-copy hashes](benchmarks/rabitq_scalar_native_2026_10_18/macos/provenance.json)

Large binary artifacts remain outside Git. The workflow retains its uploads
for 14 days; the source commit, fixture hashes, raw records and summaries are
retained here. Neither this scalar baseline nor the Linux serving profile
completes #994's scalar restructuring, native kernels, parity, cancellation,
resource, WASM or end-to-end qualification.
