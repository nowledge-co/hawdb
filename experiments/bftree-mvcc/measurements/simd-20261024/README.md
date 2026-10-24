# Bf-Tree key comparison and RocksDB audit — 2026-10-24

## Scope and reference implementations

This probes Bf-Tree 0.5.6 at upstream commit
`3998ce8643dec4e98b07f2d11cad67bf2c2d28d1`, on top of the bounded-scan MVCC
adapter from HawDB PR #1213 head `56bed74dc0ccc388a0cfff27b27b96785ec72f81`.
The default adapter dependency remains unmodified. Only the manual temporary
copies select an alternative comparison kernel.

The RocksDB audit used current upstream commit
`ea313605367281013c2c81158069e88411516357` (2026-10-23), with primary sources:

- [FastLocalBloomImpl](https://github.com/facebook/rocksdb/blob/ea313605367281013c2c81158069e88411516357/util/bloom_impl.h#L94-L108)
  explicitly optimizes Bloom queries with AVX2. Its
  [query kernel](https://github.com/facebook/rocksdb/blob/ea313605367281013c2c81158069e88411516357/util/bloom_impl.h#L231-L340)
  derives eight probes, loads a bounded cache-local region and uses
  permute/blend/bit tests. This is SIMD filtering, not a B-tree multi-slot
  lower-bound implementation. Its layout has no Bf-Tree reference-bit atomics.
- The default [BytewiseComparator](https://github.com/facebook/rocksdb/blob/ea313605367281013c2c81158069e88411516357/util/comparator.cc#L32-L40)
  delegates to `Slice::compare`; that
  [implementation](https://github.com/facebook/rocksdb/blob/ea313605367281013c2c81158069e88411516357/include/rocksdb/slice.h#L286-L299)
  uses `memcmp` and a length tie-break. It is not evidence for replacing all
  byte comparisons with hand-written AVX/NEON. The platform `memcmp` may itself
  use SIMD.
- The [data-block hash index](https://github.com/facebook/rocksdb/wiki/Data-Block-Hash-Index)
  reduces binary-search work for point lookups and falls back for range seeks.
  It illustrates avoiding comparisons at the appropriate level, rather than
  making each comparison wider. No RocksDB throughput number is transferred
  to this experiment.
- The [MVCC iterator](https://github.com/facebook/rocksdb/blob/ea313605367281013c2c81158069e88411516357/db/db_iter.cc)
  reseeks after many skipped versions of one user key. The existing HawDB
  experiment already has bounded version-run seeks. This is an algorithmic
  analogue, not an assertion that the two iterators have identical semantics.

The inference for Bf-Tree is to measure a specific remaining-key comparison
first, and reserve batch SIMD for a layout with useful independent comparison
lanes. Existing encoded numeric benchmark keys share their first four bytes
within each namespace; simply vectorizing those inner previews offers poor
discrimination. A vector load spanning `LeafKVMeta` would also overlap its
concurrently updated atomic reference/value-length field. This probe changes
neither metadata layout nor locking and loads only immutable key slices.

## Method

See the [probe instructions](../../simd/README.md) and checked-in
`source.json`, `protocol.json`, `summary.json.gz`, `results.jsonl.gz` and
`validation/` receipts. All adapter/benchmark Rust sources are byte-identical
across the three measured variants; only the four dependency call sites and
new comparison module select different remaining-key ordering kernels.

The variants are original slice comparison, safe scalar eight-byte big-endian
word comparison, and explicit ARM NEON sixteen-byte comparison with scalar
word tail. Original two/four-byte previews and binary search remain. Equal
prefixes order by length; zero bytes and arbitrary binary keys retain slice
ordering. The default adapter does not select the manual patch.

The machine is Apple M3 Max, 16 logical CPUs, 128 GiB RAM, macOS 27.0.1 ARM64;
journals are on local APFS. Rust is the repository-pinned 1.97.1, with the
global release profile: optimization 3, thin LTO, one codegen unit, unwind,
and no custom compiler flags or target CPU. Bf-Tree working indexes remain in
memory, with the same 4 MiB cache configuration, not a process-RSS cap.

Five repetitions rotate/reverse variants/layouts/cases. Each variant uses a
fresh process and journal. All Versioned/Hybrid/COW layouts cover numeric
eight-byte keys at 16 keys/2 overwrite rounds, 512/24 and 2048/48, plus 512/24
with two synthetic 64-byte key shapes. `prefix64` has 56 common leading bytes;
`spread64` has a numeric ID followed by 56 varying bytes. These are not
captured Mem production distributions.

Every timed run retains the seeded snapshot, uses 100-byte values and the same
complete-batch journal `sync_all` before index installation/publication.
Updates use at most 64 keys; seed batches at most 128. The native harness
checks every returned point value, scan key/value/order and reopened final
key. Each three-way case must produce byte-identical journal hashes. COW uses
the unchanged algorithm and is a control, not a production HawDB benchmark.

Each process has 4096 current and 4096 old point reads, 32 current and 32 old
full scans. Scan p99 is a maximum, not a stable tail estimate. Reported ratios
are medians of within-repetition ratios, not ratios of separate medians. The
shared machine is not reserved; compiler counts are boundary observations,
not continuous proof of quiescence. No stable regression ceiling or production
speedup follows from this matrix.


## Outcome and decision

Keep both alternative kernels as **manual probes only**. This cohort does not
qualify either one as the default. SIMD instructions really execute, but
explicit NEON does not establish a consistent advantage over word comparison
across the tested layouts and shapes. Positive and adverse observations are
kept together; they are not isolated causal estimates of kernel cost.

All **225 matrix processes** succeeded: **95,130 timed commit batches,
1,843,200 timed point reads and 14,400 timed scans**. All **75 journal triplets**
were byte-identical, independently rechecked after the cohort; see
[journal-hashes.json](journal-hashes.json). The 27 earlier 16-key/one-round
shape/layout smoke processes are separate in the validation logs and excluded
from these counts. No correctness, visibility or recovery mismatch occurred.

Rust compilation was observed at before/after boundaries of **54/225** matrix
processes, with up to four compilers. One-minute load ranged from **8.13 to
52.77** on 16 logical CPUs. The unchanged COW large-numeric control has a NEON
binary/upstream current-scan p95 median paired ratio of **1.519**, with one
pair at **49.162**. These observations prevent a strong algorithm-level tail
or regression claim. No sample is removed and no quiet-host subset replaces
the full cohort. Boundary counts do not detect all competing work.

- Hybrid `spread64` current/old scan p95 ratios for NEON/upstream are
  **0.803/0.800**. Direct NEON/word ratios are only **0.968/0.965**; individual
  pairs range **0.823–1.084 / 0.803–1.103**. Even this favorable case does not
  consistently beat the scalar word kernel in every repetition.
- Versioned large-numeric current/old scans have NEON/upstream ratios
  **1.300/1.476**, and NEON/word **1.276/1.189**. Preserve the adverse result,
  while the COW control prevents attributing the entire difference to NEON.
- Neither word comparison nor explicit NEON yields a qualified universal point,
  commit-tail, replay or RSS improvement. Changes in `prepare_and_sync` and COW
  commit times also occur in work that does not use the new comparator. There
  is no claimed `sync_all` speedup or weakened durability.

For example, absolute five-process medians for Hybrid `spread64` current scan
p95 are 144,042 / 115,667 / 119,625 ns for upstream/word/NEON. The current-get
p99 medians are 834 / 833 / 750 ns. These **absolute medians must not be divided
to reconstruct the paired ratios** above. The full summary preserves both.

### Comparison with the original implementation

Every cell is `word/upstream ; NEON/upstream`, the median of five matched
ratios. Values below one are favorable. Full p50/p95/p99/total observations,
absolute medians and individual paired ratios are in the gzip-compressed
[summary](summary.json.gz); the driver writes plain `summary.json` before
archival. All 15 cases, including COW controls, appear here.

| Shape / keys / rounds | Layout | Current get p99 | Old get p99 | Current scan p95 | Old scan p95 | Commit p99 | Install p99 |
| --- | --- | --- | --- | --- | --- | --- | --- |
| numeric8 / 16 / 2 | versioned | 1.000 ; 1.000 | 1.000 ; 1.196 | 1.153 ; 1.027 | 1.204 ; 1.190 | 1.018 ; 1.013 | 0.953 ; 1.009 |
| numeric8 / 16 / 2 | hybrid | 0.856 ; 0.856 | 1.000 ; 0.916 | 1.159 ; 1.158 | 0.906 ; 0.962 | 0.993 ; 0.995 | 1.071 ; 0.974 |
| numeric8 / 16 / 2 | cow | 1.000 ; 1.000 | 1.000 ; 1.000 | 1.019 ; 0.962 | 0.870 ; 1.310 | 1.009 ; 0.977 | 0.808 ; 0.860 |
| numeric8 / 512 / 24 | versioned | 1.080 ; 0.892 | 0.895 ; 0.947 | 0.986 ; 1.058 | 0.998 ; 1.027 | 1.045 ; 0.832 | 0.908 ; 0.891 |
| numeric8 / 512 / 24 | hybrid | 0.997 ; 0.997 | 0.962 ; 0.985 | 0.846 ; 0.861 | 0.896 ; 0.943 | 1.541 ; 0.813 | 1.077 ; 1.314 |
| numeric8 / 512 / 24 | cow | 1.000 ; 0.995 | 1.000 ; 0.874 | 0.997 ; 1.029 | 1.045 ; 1.057 | 1.324 ; 0.936 | 1.353 ; 1.188 |
| numeric8 / 2048 / 48 | versioned | 1.082 ; 1.107 | 1.107 ; 1.034 | 1.112 ; 1.300 | 1.106 ; 1.476 | 0.879 ; 1.006 | 1.088 ; 1.152 |
| numeric8 / 2048 / 48 | hybrid | 1.049 ; 1.000 | 1.177 ; 1.183 | 1.036 ; 0.968 | 1.032 ; 1.034 | 0.793 ; 1.088 | 1.005 ; 0.862 |
| numeric8 / 2048 / 48 | cow | 1.000 ; 0.891 | 1.098 ; 1.084 | 1.032 ; 1.519 | 1.029 ; 1.451 | 1.079 ; 1.004 | 0.699 ; 0.985 |
| prefix64 / 512 / 24 | versioned | 0.986 ; 1.055 | 0.957 ; 0.961 | 1.031 ; 1.026 | 1.028 ; 1.007 | 0.666 ; 0.480 | 0.745 ; 0.827 |
| prefix64 / 512 / 24 | hybrid | 0.923 ; 1.092 | 0.993 ; 1.056 | 1.020 ; 0.934 | 1.045 ; 0.943 | 1.436 ; 1.204 | 1.135 ; 1.046 |
| prefix64 / 512 / 24 | cow | 1.112 ; 1.000 | 1.000 ; 0.899 | 1.149 ; 0.949 | 1.254 ; 1.055 | 1.136 ; 1.403 | 1.178 ; 1.152 |
| spread64 / 512 / 24 | versioned | 0.976 ; 1.035 | 1.029 ; 1.017 | 1.033 ; 1.149 | 1.004 ; 1.194 | 1.162 ; 1.190 | 1.320 ; 1.133 |
| spread64 / 512 / 24 | hybrid | 0.947 ; 0.895 | 0.943 ; 0.841 | 0.908 ; 0.803 | 0.871 ; 0.800 | 0.969 ; 0.820 | 0.688 ; 0.631 |
| spread64 / 512 / 24 | cow | 1.000 ; 1.000 | 0.998 ; 0.998 | 1.112 ; 1.112 | 1.124 ; 0.997 | 0.991 ; 1.161 | 0.899 ; 1.015 |

### Direct NEON versus word comparison

These ratios are computed directly within each repetition, not by dividing
columns from the preceding table. Recovery and memory costs remain visible
for all layouts; RSS ratios close to one do not prove a strict memory ceiling.

| Shape / keys | Layout | Current get p99 | Old get p99 | Current scan p95 | Old scan p95 | Reopen | Resident bytes |
| --- | --- | --- | --- | --- | --- | --- | --- |
| numeric8 / 16 | versioned | 1.000 | 1.196 | 0.887 | 1.221 | 0.997 | 1.010 |
| numeric8 / 16 | hybrid | 1.000 | 0.997 | 0.888 | 1.068 | 0.981 | 1.005 |
| numeric8 / 16 | cow | 1.000 | 1.000 | 1.131 | 1.501 | 1.001 | 0.997 |
| numeric8 / 512 | versioned | 0.827 | 1.042 | 1.017 | 0.999 | 1.020 | 1.000 |
| numeric8 / 512 | hybrid | 1.000 | 1.000 | 0.989 | 1.053 | 0.980 | 1.002 |
| numeric8 / 512 | cow | 0.856 | 0.997 | 1.081 | 1.011 | 1.016 | 1.000 |
| numeric8 / 2048 | versioned | 1.046 | 0.976 | 1.276 | 1.189 | 1.223 | 1.001 |
| numeric8 / 2048 | hybrid | 0.999 | 1.005 | 0.936 | 0.954 | 0.978 | 0.998 |
| numeric8 / 2048 | cow | 0.891 | 0.910 | 1.130 | 1.143 | 1.029 | 1.002 |
| prefix64 / 512 | versioned | 0.975 | 0.960 | 0.995 | 0.977 | 0.955 | 1.003 |
| prefix64 / 512 | hybrid | 1.084 | 1.088 | 1.082 | 1.101 | 0.998 | 1.001 |
| prefix64 / 512 | cow | 1.000 | 0.910 | 0.903 | 0.844 | 0.934 | 1.006 |
| spread64 / 512 | versioned | 1.117 | 1.143 | 1.236 | 1.212 | 1.151 | 1.000 |
| spread64 / 512 | hybrid | 0.908 | 0.937 | 0.968 | 0.965 | 0.965 | 1.002 |
| spread64 / 512 | cow | 0.998 | 0.910 | 1.029 | 0.919 | 0.996 | 1.002 |

## Verification and reconstruction

The [prepare/build/measure commands](../../simd/README.md) reproduce the source
transformation and protocol from this checkout. `source.json` records every
compiled adapter and dependency Rust source, manifests and locks. It verifies
identical harness sources and only the one intended dependency lock-origin
change. `protocol.json` records driver and fixed-executable hashes. Native
sources were fixed before the matrix and remained unchanged throughout it.
Full synced journals remain outside the repository at the path in
`cohort.json`; their hashes and sizes are retained here.

- Original, word and NEON snapshots each passed **21 tests in dev and release**,
  with one ignored child exercised by its parent (nine crash-boundary/layout
  subprocess cases). The two added kernel oracle tests cover binary ordering,
  lengths, tails, 16 pointer offsets, every mismatch position and random pairs.
  The full adapter model, range/physical/row boundaries, conflicts, private
  writes, uncertainty and reopen tests run with each actual patched dependency.
- All three snapshots passed strict all-target/all-feature **adapter Clippy**.
  Unchanged upstream local-path builds emit nine `unexpected cfg(spdk)`
  warnings; no lint level is weakened. The kernel itself passed standalone
  `clippy-driver -D warnings` metadata compilation for native macOS ARM64,
  Linux ARM64 and Windows x86_64. Cross compilation is not native platform or
  x86 SIMD qualification.
- Scoped final-binary disassembly proves that explicit NEON survives
  optimization: one vector `cmeq` site in each NEON leaf/inner `lower_bound`.
  Those functions contain 165/180 instructions, versus original 96/142 and
  word 122/119. This is static code size, not executed instruction count or a
  causal performance proof. Original `memcmp` may use SIMD internally.
  Dumps and exact symbols are in `validation/`; upstream MIT terms for the
  generated Bf-Tree disassembly are retained in `BF_TREE_LICENSE.md` there.
- Root workspace, all-feature normal dependency tree and `hawdb` archive-list
  checks exclude the experiment and Bf-Tree. No production API, WAL format,
  default synchronization, page layout, crate split or CI lane changes.
- Required local Bazel fuzz commands passed **98 cached targets** (zero newly
  executed this invocation); the prior scan work executed those same unchanged
  production targets. The independent production MVCC campaign passed
  **46,528 complete cases in 30,000 ms**. Neither runs this adapter; its tests
  are the separate manual results above. No remote CI speedup is claimed.
- `prek install` and `prek run --all-files` ran. The all-files audit reports
  **624 historical license failures**, verified unchanged from main and
  outside this delivery. This is not a green full-tree license audit. Required
  final root/independent formatting, strict Clippy and dev/release tests all
  passed on the final source. Staged license/fmt/Clippy hooks passed as well;
  see the `final-*` and `staged-prek` receipts.

Build caches were reused. Release build stages took 41.11 / 26.27 / 7.24 s
for upstream/word/NEON while unrelated work changed; see `validation/build.json`
for every stage and command. These are not cold-build or compiler-speed
comparisons. The experiment remains unqualified for production, power loss,
out-of-memory operation, checkpoints/GC or full-engine performance.
