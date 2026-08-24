# TurboQuant Vector Projection Benchmark

This benchmark records local kernel-regression evidence for the x86_64 AVX2
TurboQuant candidate scanner. It is directional development evidence, not Mem
production qualification.

## Change Under Test

The baseline is Skein commit `474b8bb98dea6cc0390149c1c6f661f18c2d8d13`
with scalar/auto benchmark instrumentation applied. Its AVX2 kernel decodes 32
packed coordinates at a time, performs four `_mm256_i32gather_ps` centroid
lookups, and accumulates all products through one dependency chain.

The optimized kernel keeps the fixed 16-entry `f32` centroid table in two YMM
registers. Each eight-coordinate group permutes both table halves with
`vpermd`, selects the matching half with `vblendvps`, and updates one of four
independent accumulators. A tree reduction combines those accumulators after
the vector loop. Runtime AVX2 detection, the scalar tail, public APIs, and the
artifact format are unchanged.

## Workload

- release profile with thin LTO and one codegen unit
- 8,192 deterministic in-memory vectors in eight 1,024-row segments
- 384 dimensions for the before/after comparison
- TopK 10
- one admitted scan worker
- sorted allowlists selecting 1%, 10%, 50%, and 100% of documents
- five samples per case, reported as median scored documents per second
- `KernelPreference::Auto`, which selected AVX2 on the benchmark host

The numerator is the number of eligible documents scored. Elapsed time covers
the complete in-memory candidate search, including query transformation,
allowlist bitmap construction, block iteration, scoring, and bounded TopK
maintenance. Projection construction is outside the timed region.

## Environment

Measured on 2026-08-24 with an AMD Ryzen 7 7735HS, 8 cores and 16 logical CPUs,
Linux 7.1.8 x86_64, and Rust 1.97.1.

## AVX2 Before And After

| Allowlist density | Gather baseline | Register LUT | Change |
| ---: | ---: | ---: | ---: |
| 1% | 4,845,763 docs/s | 5,478,354 docs/s | +13.05% |
| 10% | 6,209,910 docs/s | 10,766,383 docs/s | +73.37% |
| 50% | 5,980,942 docs/s | 10,979,497 docs/s | +83.57% |
| 100% | 6,194,629 docs/s | 11,046,699 docs/s | +78.33% |

The 1% case scores only 82 documents, so query transformation, mask creation,
block traversal, and TopK setup dominate it. The 10% through 100% cases better
represent the kernel change and show a 73% to 84% throughput increase.

## Post-change Scalar Comparison

The dense single-worker case was also measured at the common 384, 768, and
1,536 dimensions. These values compare the scalar and runtime-selected AVX2
paths in the optimized tree; they are not old-versus-new AVX2 measurements.

| Dimensions | Scalar | AVX2 | AVX2 / scalar |
| ---: | ---: | ---: | ---: |
| 384 | 3,345,029 docs/s | 11,046,699 docs/s | 3.30x |
| 768 | 1,736,282 docs/s | 6,605,370 docs/s | 3.80x |
| 1,536 | 872,689 docs/s | 3,639,829 docs/s | 4.17x |

## Reproduction

The benchmark emits adjacent scalar and auto cases and records both the
requested and selected kernels:

```bash
cargo bench --bench vector_projection_scan
SKEIN_BENCH_VECTOR_DIMENSION=768 cargo bench --bench vector_projection_scan
SKEIN_BENCH_VECTOR_DIMENSION=1536 cargo bench --bench vector_projection_scan
```

Generate and inspect release assembly with:

```bash
cargo rustc -p skein-vector-projection --release --lib -- --emit=asm
rg -n 'score_avx2|vgather|vpermd|vblendvps' target/release/deps -g '*.s'
```

The optimized hot loop should contain `vpermd` and `vblendvps`, retain four
YMM accumulators without spills, and contain no `vgather` instruction.

## Interpretation And Limits

This result isolates candidate-search CPU work on one AMD AVX2 host. It does
not cover file-backed I/O, canonical raw-vector reranking, concurrent query
contention, Intel microarchitectures, Windows x86_64, or non-AVX2 fallback
machines. Multi-worker values are intentionally omitted because short local
samples were scheduler-sensitive.

Production admission still requires the representative-data, cross-platform,
latency, recall, RSS, page-fault, cancellation, and recovery evidence defined
by the [TurboQuant vector projection contract](specs/TURBOQUANT_VECTOR_PROJECTION_SPEC.md).
