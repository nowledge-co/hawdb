# RaBitQ cosine and squared-L2 component measurement

Issue Number: ref [#1473](https://github.com/nowledge-co/hawdb/issues/1473)

Local macOS AArch64 measurement on 2026-11-03, pinned Rust 1.97.1,
repository checked test profile (optimization 1, debug assertions and overflow
checks enabled), two compile jobs and one test thread. The branch is based on
main `38d5bfe550f67df7d89e3935cd3a718f9ab10e18`; the measured implementation and
fixture hashes are in [source-sha256.txt](source-sha256.txt). No profile or
machine configuration was changed. Initial test compilation took 2m16s; the
final-source incremental compilation took 38.56s. The final probe ran in 1.68s.
Compilation is excluded from the numbers below.

Replay from the repository root:

```sh
CARGO_BUILD_JOBS=2 cargo test --locked -p hawdb-search --all-features --lib \
  rabitq_metric_measurement -- --ignored --nocapture --test-threads=1
```

The ignored probe is manual-only, not a CI performance gate. It generates
32-row and 2,048-row nonunit Float32 corpora with 1,536 dimensions, one
independent deterministic query and generator seed `0x1473`. Each width/metric
uses the same rows for a given size. Explicit exhaustive Search supplies the
recall reference. For each 64/256 candidate window, three Auto queries are
compared with the scalar path for identical final IDs and score bits. Build
work is measured separately and reused between windows. This is a resident
compressed-candidate plus exact-rerank query; it does not measure file I/O,
route latency, concurrent load or release-mode throughput.

| Scoring | Bits | Window | Build ms | Query median ms | Recall at 10 | Raw vector bytes read |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| ProjectionMetric | 1 | 64 | 45.293 | 5.901 | 0.90 | 393216 |
| ProjectionMetric | 1 | 256 | 45.293 | 6.462 | 1.00 | 1572864 |
| ProjectionMetric | 4 | 64 | 388.844 | 38.201 | 1.00 | 393216 |
| ProjectionMetric | 4 | 256 | 388.844 | 38.529 | 1.00 | 1572864 |
| LinearSquaredL2 | 1 | 64 | 44.156 | 5.954 | 1.00 | 393216 |
| LinearSquaredL2 | 1 | 256 | 44.156 | 6.713 | 1.00 | 1572864 |
| LinearSquaredL2 | 4 | 64 | 388.883 | 38.358 | 1.00 | 393216 |
| LinearSquaredL2 | 4 | 256 | 388.883 | 39.049 | 1.00 | 1572864 |

For 2,048 rows the complete raw Float32 corpus is 12,582,912 bytes. The
64-candidate queries rerank exactly 64 rows (393,216 bytes); 256-candidate
queries rerank 256 rows (1,572,864 bytes). Candidate codes are still scanned
across all eligible rows: there is no IVF partitioning or sublinear-work claim.
All 32-row controls cover their complete corpus and return recall 1.0. Single
query recall, especially with varied vector norms, does not establish production
quality, a universal window size or a deterministic ANN recall guarantee.

Version-2 candidate payloads occupy 442,368 bytes at 1 bit and 1,622,016 bytes
at 4 bits for 2,048 rows; norms contribute 16,384 bytes in either mode. These
exclude retained canonical raw vectors and mapping/metadata capacity. The build
admission receipt includes the complete scalar quantizer scratch and bounds the
per-segment workspace. Full per-size retained/build receipts and timings are in
[measurements.jsonl](measurements.jsonl). Do not interpret packed-code size as
whole-database compression or these unpaired timings as an old/new speedup.
