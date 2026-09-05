# Search Generation Benchmark

This benchmark compares the bounded `SearchOutOfCoreGenerationWriter` with the
compatibility `SearchIndex` residency path. It is a kernel regression benchmark,
not representative Mem production qualification.

## Workload

- 100,000 documents in strictly increasing ID order
- 512 content bytes per document
- 16-dimensional `f32` embeddings
- two metadata fields per document
- 132,719,007 logical encoded document bytes
- fresh process and fresh database directory for each mode

Run the two modes separately so process high-water RSS remains comparable:

```bash
SKEIN_SEARCH_GENERATION_BENCH_MODE=streaming \
  cargo bench --bench search_generation
SKEIN_SEARCH_GENERATION_BENCH_MODE=resident \
  cargo bench --bench search_generation
```

## Local Result

Measured on 2026-08-06 with an Apple M5 Max, 18 logical CPUs, 36 GiB RAM,
macOS arm64, and Rust 1.97.1. Each value is one release-mode run and is
directional evidence rather than a statistical latency claim.

| Metric | Resident | Streaming | Change |
| --- | ---: | ---: | ---: |
| Elapsed time | 10,329 ms | 9,144 ms | -11.47% |
| Throughput | 9,681 docs/s | 10,935 docs/s | +12.95% |
| Lifetime peak RSS growth | 251,789,312 B | 88,162,304 B | -64.99% |
| Steady RSS growth | 238,780,416 B | 88,162,304 B | -63.08% |
| Minor page faults | 23,923 | 6,610 | -72.37% |
| Major page faults | 0 | 0 | unchanged |
| Resident documents after build | 100,000 | 0 | -100% |

The streaming run produced an 80,760,459-byte immutable generation from a
134,319,015-byte checksummed spool. Its largest document record was 1,340 bytes,
its largest encoded segment was 170,044 bytes, and it retained no full search
documents after reopening. The observed peak RSS was 2.86 times lower than the
resident path while throughput was higher on this workload.

Production admission still requires the separate representative-copy protocol:
multiple samples, P50/P95/P99 query latency, production corpus identity, steady
and peak RSS, page faults, recovery, and release-bound qualification evidence.

## Single-Pass Generation Build

Issue [#219](https://github.com/nowledge-co/skein/issues/219) removes repeated
source spool decoding. One scan borrows each validated document for lexical
ingest and RaBitQ encoding, then transfers ownership to the segment builder.
The segment and RaBitQ builders finish before lexical external merge, releasing
their staging buffers before that phase. Publication still happens only after
all sinks finish, with the active manifest published last.

Public build options and reports, artifact codecs, v1 formats, ordering, and
each sink's configured memory/spill limits are unchanged. Those limits are
independent, not one shared aggregate reservation: the sink states coexist
during ingest, so unchanged individual limits do **not** imply unchanged peak
RSS. The build does not materialize another complete corpus or clone decoded
documents to fan them out.

### Read and Recovery Evidence

The regression suite instruments actual source `File::read` bytes before
`BufReader`, plus file opens. It compares the fused build with a test-only
three-pass reference using identical options and individual artifact codecs:

- the fused build opens the spool once and reads exactly `report.spool_bytes`;
- the reference reads it three times with vectors enabled and present, or twice
  without a RaBitQ artifact;
- 18 seeded corpora cover empty/text-only/mixed/all-vector inputs, document and
  segment boundaries, multiple RaBitQ segments, and forced lexical spill;
- every published file and the complete build report are equal; reopening and
  full document hydration additionally verify manifest and payload digests;
- invalid spool headers, checksums, truncation, trailing bytes, and individual
  sink/publication budget failures clean staging and preserve the prior active
  generation, which remains fully readable.

This proves fewer bytes read through the spool file API, not a threefold
reduction in physical-device I/O or total build time. Artifact verification,
lexical spill/merge, output writes, and OS caching are separate costs.

```bash
cargo test -p skein-search fused_generation_
cargo test -p skein-search --no-default-features fused_generation_
bazel test //crates/search:skein_search_tests //:skein_unit_tests \
  //crates/fuzz:skein_fuzz_tests //crates/fuzz:skein_fuzz_cli_tests \
  //:skein_linux_ci_fuzz_smoke_test
```

### Local Baseline Comparison

Measured on 2026-09-05 against clean main
`09fa6f616f180616fbd12a902ee2bf557a31eaa6`, using the identical unmodified
`search_generation` streaming benchmark and default limits. Both release
binaries were compiled before measurement. The machine was an Apple M5 Max,
18 logical CPUs, 36 GiB RAM, macOS 26.6.2 (25G83), Rust 1.97.1. Each sample
used a fresh process and database; no cold-cache request was made.

| Execution order | Build | Elapsed | Peak RSS growth | Minor page faults |
| ---: | --- | ---: | ---: | ---: |
| 1 | Three-pass main | 9,443 ms | 142,786,560 B | 13,003 |
| 2 | Fused | 9,158 ms | 145,375,232 B | 10,230 |
| 3 | Fused | 8,989 ms | 148,733,952 B | 10,569 |
| 4 | Three-pass main | 9,566 ms | 142,950,400 B | 13,013 |

Both builds produced the same reported artifact sizes: 92,042,866 generation
bytes from 134,319,015 spool bytes, with zero resident documents after reopen.
Average wall time was 4.5% lower, but average peak RSS growth was 4,186,112 bytes
(about 4 MiB) higher. These two samples per build are directional evidence, not
a statistical performance guarantee or representative production qualification.
In particular, this measurement does not support an unchanged-RSS claim.
