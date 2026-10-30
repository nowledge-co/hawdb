# Selected-vector and adjacency layout investigation

## Experiment

Issue: [#1326](https://github.com/nowledge-co/hawdb/issues/1326).
Baseline: HawDB 0.19.0, `7b20df5f72cc40534181cbd753d098519b4e51ec`.
Both alternatives are test-only experiments. Production vector decoding, serving,
adjacency defaults, file formats and host APIs are unchanged.

The host is an Apple M3 Max / 16 logical CPUs / 128 GiB RAM, macOS 27.0.1
(26A434), aarch64, with pinned Rust 1.97.1. Both arms use the **same executable**
and repository-wide release profile: opt-level 3, thin LTO, one codegen unit,
without debug assertions. Default Search features are enabled. Exact source,
lockfile, profile and binary SHA-256 identities, process timestamps and host-load
observations accompany the measurements in [measurements.json](measurements.json).
The [test-only harness](../../../crates/search/src/out_of_core/vector_query/layout_probe.rs)
and [candidate](../../../crates/search/src/out_of_core/vector_query/layout_probe/candidate.rs)
contain the complete fixture and validation procedure.
The host is shared; other workspaces' activity is uncontrolled. These are local
component measurements, not remote CI or full-query qualification.

Replay from the repository root:

```sh
cargo test --locked --release -p hawdb-search --lib layout_probe:: --no-run
cargo test --locked --release -p hawdb-search --lib out_of_core::vector_query::layout_probe::selected_stream_matches_native_decoder_for_every_subset_and_corrupt_rows -- --exact
cargo test --locked --release -p hawdb-search --lib layout_probe:: -- --ignored --nocapture --test-threads=1
```

For accepted timing, copy the produced executable once, then run its exact
`out_of_core::vector_query::layout_probe::{vector_materialization_measurement,adjacency_layout_measurement}`
tests separately with `--exact --ignored --nocapture --test-threads=1`, in three
fresh processes for each probe. The recorded process order is vector, adjacency,
adjacency, vector, vector, adjacency. For the primary cohort, four task-owned Rust compiler processes were temporarily
suspended for the measurements and then resumed; no tests were suspended and no
profile, timeout or test selection changed. Other host activity was uncontrolled.
The earlier loaded-host cohort ran with verification builds active and is retained
separately in [measurements-loaded.json](measurements-loaded.json). Each
shape has one warmup per arm and seven paired samples, alternating baseline /
candidate and candidate / baseline. The summary reports the median of 21 paired
candidate/baseline elapsed ratios; values below 1 mean lower elapsed. It also
retains each process's median ratio and every sample. This is descriptive local
measurement; no confidence interval or population speedup is inferred.

### Vector component

The baseline calls the unchanged native streaming `visit_records` and filters
ordinals in its callback. The test-only candidate materializes embeddings only
for selected ordinals, reusing the existing conservative finite-decimal guard
for excluded values and retaining the native parser for other syntax. It still
validates every row, ID, dimension, value, ordinal, count and ordering and checks
cancellation every 1,024 components. The differential oracle compares exact
selected float bits or error strings for every subset of a three-row boundary
fixture, corrupt excluded rows, malformed layouts and pre-cancellation. Both
arms must refund query admission completely.

Fixtures contain 64 or 1,024 rows, 32/384/1,536 dimensions, deterministic
high-entropy signed finite values in [-1, 1), and ordinary decimal or scientific
encodings. Selection retains 1, 32 or all rows (32 is half only for 64 rows).
Each fixture uses the native compressed snapshot encoder, an actual synchronized
file, and the native checksum/decompression/admission reader. Each iteration
reopens the file. Both arms compute the same f64 weighted checksum on selected
vectors. The timed region includes ledger/file setup, read, decompression,
validation, materialization, checksum computation and refund assertions. It
excludes fixture construction and ANN candidate generation. Small shapes use
eight reads per sample; larger ones use one. Each process contains 36 shapes /
504 arm samples.

The query-memory budget is 32 MiB. Tracked query peaks cover native admission,
not all allocator/host memory. Process RSS includes fixture construction and
both arms and cannot establish an arm-specific saving. Compressed and expanded
bytes are identical between arms: this prototype does **not** implement a binary
vector sidecar, random candidate reads, reduced I/O, full native reranking,
Cypher execution or Mem request serving.

### Adjacency component

Native canonical relationship and adjacency writers/readers create real files
with their existing synchronization and 64 KiB target segments/blocks. Fixtures
contain 64 or 1,024 relationships from one source and one type, unique sorted
relationship/neighbor IDs, and a string property of 0, 128 or 8,192 bytes (all
`x`). The baseline keeps the existing 16 MiB `max_record_bytes`. The diagnostic
256-byte setting makes wide relationships use the existing canonical-reference
representation instead of copying the property record into outgoing and
incoming adjacency. It is a probe using the current knob, **not** a proposed
reduction in the safety limit or a new inline-policy API.

IDs-only scans use native endpoint inputs. Property scans use native entries;
a reference resolves through actual `get_relationship_with_report` canonical
reads. Both arms verify row count and the same identity/neighbor checksum;
property scans additionally assert each body length and include it in the
checksum. They do not compare a complete GraphStore/Cypher result or serialize
full responses. Engine-cache budgets are 0 and 8 MiB per arm, shared between
adjacency and canonical readers. Actual hits, misses and residency are retained.
Cache-disabled runs still have a warm OS cache; bounded-cache runs may evict
blocks and are not assumed all-hit. Each process contains 24 shapes / 336 arm
samples. The timed scan excludes fixture creation, reader creation and prewarm.

`adjacency_payload_bytes_visited` and `canonical_payload_bytes_visited` are native
logical payload-visit counters, **including cache hits**, not physical disk
bytes. Adjacency artifact sizes include both directions but exclude the shared
canonical file, manifests outside that artifact and full-database overhead.
Build timing includes the native adjacency writer's synchronization but excludes
canonical-file construction; it is one build per shape/arm/process, repeated in
query records. Deduplicate those observations before summarizing build cost.
Build memory is the writer's resident estimate, not measured peak RSS. These
fixtures measure immutable component tradeoffs, not WAL/recovery, concurrent
queries, high-degree mixed-type graphs or property-heavy production traffic.

The two probes remain ignored/manual and require the global release profile;
CI features, fixtures, timeouts and compilation policies are unchanged. Final
Search release-test rebuild cost was 18m26s on this busy host. An earlier pilot
built in 3m34s after a roughly 110s initial dependency build attempt. These build timings
are not test execution or performance gains. Compiler/test logs remain outside
Git; only procedure, measurement data and conclusions are retained here.

## Results

The primary cohort completed **2,520 arm samples**, three fresh processes per
probe; the loaded-host cohort contributes another 2,520 samples. All timed
checksum, count, dimension/body-length and admission-refund assertions passed.
The native-decoder bit/error/cancellation oracle passed in the final release
binary. The following tables use the compilation-paused cohort. Every shape,
sample and process median, including adverse observations, is retained in the
JSON data and [summary.json](summary.json). Recompute both summaries with:

```sh
python3 docs/benchmarks/vector_adjacency_layout_2026_10_29/summarize.py > /tmp/summary.json
python3 docs/benchmarks/vector_adjacency_layout_2026_10_29/summarize.py docs/benchmarks/vector_adjacency_layout_2026_10_29/measurements-loaded.json > /tmp/summary-loaded.json
```

### Vector component

Paired elapsed ratios, candidate / baseline:

| Rows | Dimensions | Encoding | Select 1 | Select 32 | Select all |
| ---: | ---: | --- | ---: | ---: | ---: |
| 64 | 32 | decimal | 0.894 | 0.936 | 1.002 |
| 64 | 32 | scientific | 0.987 | 0.994 | 1.003 |
| 64 | 384 | decimal | 0.866 | 0.933 | 1.003 |
| 64 | 384 | scientific | 1.007 | 0.999 | 1.001 |
| 64 | 1,536 | decimal | 0.870 | 0.936 | 1.000 |
| 64 | 1,536 | scientific | 1.003 | 1.006 | 1.005 |
| 1,024 | 32 | decimal | 0.879 | 0.883 | 0.997 |
| 1,024 | 32 | scientific | 1.004 | 0.997 | 0.996 |
| 1,024 | 384 | decimal | 0.873 | 0.876 | 1.001 |
| 1,024 | 384 | scientific | 1.002 | 1.002 | 1.003 |
| 1,024 | 1,536 | decimal | 0.870 | 0.873 | 1.002 |
| 1,024 | 1,536 | scientific | 1.005 | 1.008 | 1.001 |

For 1,024-row decimal segments, selecting 1 or 32 rows reduced component elapsed
by **11.7–13.0%**. The 384-dimensional/select-1 process medians were
0.870 / 0.878 / 0.872, and 1,536-dimensional/select-1 medians were
0.875 / 0.864 / 0.870. Small 64-row decimal/select-1 cases reduced elapsed by
10.6–13.4%; retaining half their rows reduced it by about 6–7%.

Scientific notation did not show a consistent gain: ratios span 0.987–1.008.
Selecting every row was also effectively neutral (0.996–1.003 across encodings).
The scientific/select-32 1,024×1,536 case regressed by 0.8% in the paired median;
this magnitude is not a reliable performance distinction on this shared host.
The optimization opportunity is therefore conditional on candidate sparsity and
encoding, not a universal faster decoder.

All shapes retained identical compressed/expanded byte counts and tracked query
memory peaks between arms. Peaks ranged from 2,786,656 to 2,834,548 bytes within
the 32 MiB admission budget. Decompression and the largest retained row still
establish the peak; this experiment demonstrates no I/O or peak-admission-memory
saving. Process resident/peak observations were 88.72–88.83 MB and include both
arms and fixture construction.

### Adjacency component

8,192-byte relationship bodies, paired elapsed ratios and independent per-arm
median elapsed (microseconds). The ratio is the median of paired ratios, so it
need not equal the quotient of the two elapsed medians.

| Relationships | Engine cache | Read shape | Baseline µs | Reference µs | Paired ratio |
| ---: | --- | --- | ---: | ---: | ---: |
| 64 | disabled | IDs only | 322.17 | 6.38 | 0.018 |
| 64 | disabled | properties | 268.88 | 3,846.62 | 14.269 |
| 64 | 8 MiB | IDs only | 28.08 | 0.92 | 0.034 |
| 64 | 8 MiB | properties | 70.42 | 100.79 | 1.437 |
| 1,024 | disabled | IDs only | 2,516.62 | 20.54 | 0.007 |
| 1,024 | disabled | properties | 1,825.71 | 53,052.62 | 29.273 |
| 1,024 | 8 MiB | IDs only | 265.75 | 4.79 | 0.019 |
| 1,024 | 8 MiB | properties | 668.33 | 971.29 | 1.461 |

The wide-property IDs-only component reduced elapsed by **96.6–99.3%**. But
property reads were **14.3× / 29.3× slower** with the engine cache disabled
(64 / 1,024 relationships), and **1.44× / 1.46× slower** with the 8 MiB cache.
The existing point-resolution path repeatedly visits canonical segments when
retrieving properties. This adverse control prevents recommending global
property externalization.

The 0/128-byte controls remain inline under both settings and have identical
artifact sizes and logical visits. Their paired ratios range **0.984–1.047**;
there is no layout change or evidence of a meaningful benefit for these shapes.
All individual controls remain in the summary rather than being dropped.

| Relationships, 8 KiB body | Inline adjacency artifact | Reference adjacency artifact | Inline adjacency payload visited | Reference adjacency payload visited | Additional canonical payload visited for properties |
| ---: | ---: | ---: | ---: | ---: | ---: |
| 64 | 1,059,004 | 5,314 | 528,356 | 1,322 | 3,642,610 |
| 1,024 | 16,943,158 | 84,034 | 8,453,150 | 20,522 | 58,990,142 |

Wide-property adjacency artifacts became about **99.5% smaller**, excluding the
unchanged shared canonical artifact. Logical payload visits decreased for
IDs-only reads; property reads additionally visited 3.64 / 58.99 MB of canonical
payload. These counters include cache hits and do not measure physical device
reads. Native cache counters and residency are preserved for each sample.

The three independent wide-fixture adjacency builds had median baseline /
reference times of 133.95 / 22.98 ms (64 relationships) and
881.97 / 37.68 ms (1,024). Writer resident estimates were 1,064,704 / 11,392 bytes
and 17,035,264 / 182,272 bytes, respectively. These are three build observations
per arm, not 84 independent builds from the repeated query records. Builds
always wrote the baseline first, and storage synchronization/host load varied;
these observations support lower duplicated payload work, not a qualified
checkpoint-throughput or RSS speedup. Small-property build controls were noisy
and are retained in the raw results. Process resident/peak observations across
adjacency runs were 66.95–68.19 MB, including both arms and source records.

### Loaded-host control

The earlier [loaded summary](summary-loaded.json) is retained to show sensitivity
to contention. Vector full-process times were 43.17 / 46.59 / 58.20 seconds,
versus 14.57 / 14.03 / 13.96 seconds with task-owned compilation suspended;
these process times include fixture construction. Some loaded-host per-process
vector ratios were as extreme as 0.193 or 1.725, including neutral controls.
Those observations are not reliable latency claims. The compilation-paused
vector sparse-decimal results were much more consistent across processes.

The loaded-host wide adjacency control still improved IDs-only reads, but
property reads regressed by 13.75× / 43.50× with cache disabled and
1.43× / 1.67× with an 8 MiB cache. Both cohorts therefore preserve the same
qualitative tradeoff while disagreeing on absolute latency and slowdown size.
No result from the old, lower-entropy pilot substitutes for either final-source
cohort.

## Conclusions

1. **Selective materialization has a measurable component benefit** for sparse
   ordinary-decimal raw vectors: about 12–13% for the 1,024-row cases here.
   A subsequent production change can target the governed streaming path while
   preserving all-row integrity, admission and cancellation. Scientific/dense
   cases need to remain negative controls. Binary ordinal-addressable storage
   is a separate unimplemented proposal; this experiment establishes no reduced
   I/O or full ANN/Cypher/Mem latency improvement.
2. **Keeping large properties out of adjacency helps topology-only reads**, and
   substantially shrinks duplicated adjacency payload. It also shifts work to
   canonical reads and can severely hurt property-heavy access. Keep the current
   defaults. A later design should separate an inline policy from the record
   safety limit and qualify batched canonical hydration, mixed property access,
   storage/cache behavior and graph degree/type distributions before adoption.
3. The deliverable is a reproducible performance investigation and test-only
   prototypes. No production algorithm, format, durability setting, dependency,
   host control plane or CI scheduling/profile changed. Full embedded query,
   mixed-load p99, physical cold-device I/O, recovery qualification and other
   platforms remain outside these measured component results.
