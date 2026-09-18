# RaBitQ Vector Projection Benchmark

The 1-bit and 4-bit RaBitQ projections currently share one portable scalar
scoring kernel. Their code layout follows Faiss: an LSB-first sign plane and,
for multi-bit codes, a separate LSB-first refinement plane. There are no AVX2
or NEON throughput claims yet. On Arm, the explicit NEON preference reaches
the same scalar fallback that Faiss currently uses for RaBitQ; it is not a
vectorized kernel. Measurements from a different quantization format are not
comparable and are not evidence for RaBitQ.

Build options default to 1-bit standard RaBitQ. Use 4-bit
only through an explicit `RaBitQBitWidth::Four` selection and qualify its
recall and resource profile separately.

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

```bash
cargo test -p hawdb-vector-projection
cargo test -p hawdb-qualification production_vector
```

Future benchmark commands and results belong in this document only after their
matching implementation and scalar-parity tests are present.
