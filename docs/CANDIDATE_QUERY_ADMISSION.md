# Candidate query ownership

Out-of-core candidate construction now uses the same operation-scoped working
account as lexical streams. The query root is created before candidate work,
not after it. Its configured/task cap is shared with the existing score account;
there are still two accounts, not one account or independent budget per block.

## Admitted data and lifetimes

- The spilled-set box, exact block-directory slots and copied endpoint IDs are
  admitted before creating a spill file or allocating the directory. The box
  has an outer owner so its allocation drops before its charge.
- Metadata range bytes are admitted before acquiring an I/O-wave permit and
  allocating the read buffer. A failed admission cannot start a positioned read.
  The implementation reuses the existing cross-platform positioned-read helper.
- Compressed text, metadata rows, candidate-selection indices and encoded block
  bytes have separate leases under that root. Decoded metadata preflight uses
  fixed borrowed fields, validates the row count and checks capacity arithmetic
  before allocating rows. IDs and metadata strings include their requested
  capacity; each map pair uses the existing conservative 2,048-byte B-tree
  occupancy/split envelope. Duplicate keys can over-admit, not evade the bound.
- Predicate evaluation borrows the decoded document. Its temporary JSON/CSV
  lists and Unicode-normalized comparisons have a conservative envelope derived
  from raw field length, comma count and expected-value length. Selection is
  recorded once; the encoder does not evaluate predicates again or clone IDs.
  Block and remaining-spill limits reject before encoded-buffer allocation.
- Cache misses retain the previous decoded block while admitting the incoming
  raw and decoded block. Validation or admission failure leaves the old cache
  intact. Replacement is serialized by the existing cache mutex; concurrent
  readers share the same root. Hits also observe cancellation.
- Candidate vector allowlists retain their capacity charge when moved into the
  vector consumer, including after the spill itself is dropped. This covers the
  allowlist, not all vector-scoring allocations.

The private `Admitted<T>` wrapper exposes only a borrow, with no `Clone`, mutable
view or consuming extraction. Its value drops before its lease. Dropping the
query/reader handle cannot release a buffer that a consumer still owns.

Candidate block traversal validates every length, UTF-8 ID, strict ID ordering
and the exact entry count before allocating decoded entries. The allowlist
also validates before materializing a block and checks total capacity and
strict ordinal order. A forged count cannot drive an initial allocation or
grow a vector by appending more records than its admitted capacity.

Cancellation checkpoints surround read/decode/write work and occur within
row/block traversal. An ordinary filesystem call or one native decompression
cannot be interrupted mid-call. Failure drops owned buffers and closes the
file before cleanup. `create_new` arms file cleanup only after successful
creation: a path collision must never delete another owner's existing file.
Publication manifests are not changed by candidate construction or failure.

## Bounded metadata decompression

The shared compressed-envelope parser now uses borrowed fields and a bitmask,
not a growable header-field collection. Candidate metadata decoding reserves
the envelope's exact advertised output length before allocation, rather than
allowing a streaming decoder to grow toward the configured segment limit.
Length/checksum/UTF-8 validation still fails closed.

The qualified path is dictionary-free, non-streaming modern zstd frames under
the pinned zstd 1.5.7 implementation (`zstd-safe` 7.2.4 / `zstd-sys`
2.0.16+zstd.1.5.7). It reserves 1 MiB for the native context before creation and
checks `DCtx::sizeof` before and after decompression. Native version changes
require requalification. Source anchors in the vendored zstd tree are
`ZSTD_estimateDCtxSize`, `ZSTD_createDCtx_advanced`, `ZSTD_decompressMultiFrame`
and `ZSTD_decompressDCtx` in `lib/decompress/zstd_decompress.c`, plus the fixed
literal workspace in `zstd_decompress_internal.h` and `zstd_decompress_block.c`.
The fixed destination slice provides history without a separate streaming
window; this path does not install a dictionary or use the legacy decoder.

Frame extents and modern magic are checked before context creation. Modern
concatenated frames remain supported. Legacy and skippable frames are rejected
by this candidate reader because those paths are outside its qualification;
the current writer does not emit them. The general snapshot streaming reader
retains its preceding decompression path. Neither the writer's v1 bytes nor
public reader signatures change.

These are source-qualified requested-capacity envelopes, not allocator/RSS,
native three-platform measurement or complete query-memory claims. Persistent
reader/cache ownership, predicate pruning/report containers, path/control
allocations, vector scans/results, ranking/fusion, hydration, public output and
persistent delta state still require their own complete ownership boundaries.
No new global host budget, I/O backend, dependency, migration, Bazel runtime
setting or default/dedicated fuzz CI is introduced.

## Verification

Normal tests cover real published metadata and spill files, exact/one-short
roots, retained metadata/cache/allowlists, competing accounts, cache corruption
and four-thread replacement, before-I/O rejection, cancellation, file collision
ownership, unchanged publication and public-query recovery. Compressed input
tests include empty/large output, concatenated frames, forged lengths/checksums,
invalid UTF-8, corrupt extents, duplicate headers and unqualified frames.
Predicate parity includes JSON/CSV lists and Unicode lowercase expansion.

The manual `skein_search_candidate_admission_fuzz_tests` target uses seed
`0x206ca11` for 1,000 candidate groups with independent wire/filter/decoder
oracles, exact/one-short shared roots and 4,000 mutated blocks. A second seed,
`0x206ec0de`, covers 256 concatenated-frame envelopes, independently generated
UTF-8/NUL text, exact/one-short roots, 768 corrupt inputs and cancellation.
Every outcome checks release; the candidate groups retain two account records.
The native decompressor's internal implementation is not copied into an oracle.

```bash
cargo test -p skein-search --all-features candidate_admission -- --nocapture
cargo test -p skein-search --all-features candidate_admission_campaign -- --ignored --nocapture
bazel test --nocache_test_results \
  //crates/search:skein_search_tests \
  //crates/vector-projection:skein_vector_projection_tests \
  //crates/fuzz:skein_fuzz_tests \
  //crates/fuzz:skein_fuzz_cli_tests \
  //:skein_linux_ci_fuzz_smoke_test
```

Deliberately omitted directory, read-buffer, decoded-metadata, decoded-cache,
native-workspace and allowlist charges must make their targeted regressions
fail. Arming the cleanup guard before successful creation must also fail the
collision regression. Restore all negative controls before positive checks.
Full #206 still requires its distinct native-platform and representative-corpus
acceptance gates; this boundary is not a standalone completed-issue claim.
