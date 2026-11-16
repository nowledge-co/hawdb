# Default WASM SIMD exact arithmetic

## Scope and source

Manual component measurements on macOS 27.0.1 (26A434), arm64, Node.js
v26.13.1 / V8 14.6.202.34-node.40, Rust 1.99.1 / LLVM 23.1.1. Base main:
`d5bced9d6a8f138c561454fb36cb27a37ddaf7b6`. The production kernel/config digests and all
structured timed samples are in [samples.json](samples.json). Raw compiler and
runner logs remain outside Git. No global `+simd128` flag was used.

The baseline forces the current ordered scalar kernel; the comparison uses
the default kernel with exactly the same inputs, mode, work checkpoints and
result checksumming. This isolates kernel selection from query preparation,
query-norm reuse, graph/SQL execution, index traversal and hydration. These
numbers are not a whole-query, browser, production, or cross-engine speedup.

## Method and replay

Use the repository-wide release profile without overrides. Each group has
128 candidates, either raw f32 slices or encoded little-endian candidates
starting at a one-byte offset. There are 128 iterations below 64 dimensions,
otherwise 16; the query is shared. Three untimed warmups per arm precede
11 timed samples per arm, alternating arm order. Every invocation verifies
the exact four-sum checksum. The dimension-3 default uses scalar, preserving
the existing below-16 threshold. The other defaults use `simd128`.

```sh
CARGO_TARGET_WASM32_UNKNOWN_UNKNOWN_RUNNER=wasm-bindgen-test-runner \
  CARGO_BUILD_JOBS=2 \
  cargo test --locked --release -p hawdb-core --target wasm32-unknown-unknown \
    --lib vector::kernel::measurement::wasm_kernel_measurement \
    -- --include-ignored --nocapture
```

The first release WASM test build in this worktree took 24.44 s, including
dependencies; linking the added comparison after that build took 11.34 s.
The comparison's 1,232 timed samples across 56 groups plus warmups completed
in 3.71 s. Build time is separate from the table's execution measurements.
Do not use native `--ignored` alone with this runner: `--include-ignored`
actually executes the ignored WASM measurement.

## Results

Speedup is median scalar time divided by median default time; higher is
faster. Ratios below 1 are regressions, including timer/noise variation in
the dimension-3 same-scalar control. Absolute nanosecond samples and medians
are retained in the linked JSON.

| Dimensions | Input | Cosine | Prepared cosine | L2 | Norm |
| ---: | --- | ---: | ---: | ---: | ---: |
| 3 | Slices | 1.010x | 1.002x | 0.999x | 0.976x |
| 3 | Unaligned bytes | 1.005x | 0.998x | 0.998x | 1.002x |
| 16 | Slices | 1.442x | 1.307x | 1.316x | 1.125x |
| 16 | Unaligned bytes | 1.341x | 1.228x | 1.181x | 1.116x |
| 32 | Slices | 1.353x | 1.316x | 1.169x | 1.155x |
| 32 | Unaligned bytes | 1.400x | 1.296x | 1.179x | 1.151x |
| 384 | Slices | 1.133x | 1.229x | 1.153x | 1.159x |
| 384 | Unaligned bytes | 1.145x | 1.184x | 1.182x | 1.254x |
| 768 | Slices | 1.128x | 1.157x | 1.062x | 1.151x |
| 768 | Unaligned bytes | 1.019x | 1.200x | 1.105x | 1.141x |
| 1536 | Slices | 1.095x | 1.140x | 1.092x | 1.121x |
| 1536 | Unaligned bytes | 1.097x | 1.167x | 1.090x | 1.120x |
| 8193 | Slices | 1.073x | 1.142x | 1.081x | 1.061x |
| 8193 | Unaligned bytes | 1.079x | 1.121x | 1.072x | 1.080x |

The 16-dimensional cases improve by 1.116x to 1.442x; the 384-to-8,193
dimensional cases improve by 1.019x to 1.254x on this runtime. Large vectors
remain constrained by coordinate-order scalar accumulation, which preserves
the existing exact-distance contract. No reassociation, f32 accumulation,
relaxed SIMD or FMA is used to manufacture a larger gain. The short control
and all workload groups preserve exact sum checksums.

## Validation limits

The WASM kernel suite covers bitwise scalar/SIMD equality, non-finite inputs,
signed zero/subnormal/extreme values, unaligned loads, tails, tiny vectors,
work boundaries and cancellation precedence. Separate minimal-facade tests
cover SQL/Cypher expressions in a dedicated browser Worker. Compilation and
instruction inspection alone do not establish runtime correctness, and one
V8/arm64 run does not qualify other browser engines or architectures.
After capture, a lint-only repair changed the measurement's release-profile
guard from a constant assertion to an if/panic guard. The timed body and the
production kernels did not change. Browser Worker integration is added and
cross-target compiled; a complete browser runtime pass is not established by
this capture. The ChromeDriver runner returned HTTP 404 before executing tests.
SIMD-capable engines are a module requirement; older engines cannot select
a scalar fallback inside a module containing SIMD instructions.
