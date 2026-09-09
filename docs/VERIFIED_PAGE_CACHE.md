# Verified immutable page cache hits

Stable-identity and relational-index readers retain a private integrity proof
alongside each immutable physical page slot in `SegmentCache`. A disk miss must
pass the complete page codec, including CRC32C and SHA-256, before this proof can
be admitted. Cache insertion still checks the slot's content digest and rejects
immutable-identity or exact-byte collisions.

On a verified hit, the codec skips repeated CRC32C/SHA-256 computation, but still
checks page headers, extents, padding, structure, and the current reader's codec
limits. Readers still enforce the selected generation, page ID, source epoch
where applicable, traversal semantics, and request budgets. A reader with
stricter limits cannot inherit the admitting reader's limits.

## Trust and ownership boundaries

- Public `SegmentCache::insert` validates only the cache content digest. It does
  not establish page integrity; these entries still take the complete codec
  verification path on every hit.
- A fully validated insertion may promote an identical resident raw entry.
  Promotion requires exact-byte equality, not just a CRC match. Raw reinsertion
  cannot downgrade an existing proof. Older unverified leases remain unverified.
- The proof belongs to an immutable cache entry/lease, not a naked `Arc<[u8]>`.
  Exporting bytes and inserting them into another cache loses that proof.
  Safe `Arc::make_mut` cannot modify the resident allocation while it is shared.
- Eviction discards the proof. A later raw insertion must be verified again.
- Existing compact row-page source tags remain separate: untagged lookups still
  cannot retrieve those compact representations.
- This trusts immutable process memory after admission; it does not continuously
  detect RAM corruption. Stable-identity deep scrub bypasses the cache and checks
  every physical page, so disk damage is not hidden by a previously healthy hit.
  Public index-page decoding also always performs full integrity verification.

## Local verification

```bash
bazel test //crates/storage:skein_storage_tests //crates/storage:skein_storage_verified_page_cache_fuzz_tests //crates/fuzz:skein_fuzz_tests //crates/fuzz:skein_fuzz_cli_tests //:skein_linux_ci_fuzz_smoke_test
cargo test -p skein-storage --release verified_page_cache_read_benchmark -- --ignored --nocapture
```

The manual differential campaign generates all four index-page kinds, compares
cached decoding with full decoding under seven reader policies, and exercises
CRC/SHA/payload/padding corruption, promotion, exported pins, and eviction. It is
not part of default or dedicated CI jobs.

The benchmark compares identical warm stable-identity lookups using public raw
cache admission (full verification per hit) and reader-verified admission at
1/2/4/8 threads. It is a same-code control for digest work, not a historical
checkout benchmark or proof of near-linear cache scaling. The global mutex and
exact pinned-byte entry scan are unchanged. Issue #196 remains open until
sharding, the ownership decision for O(1) pin accounting, and the complete
multi-threaded scaling acceptance criteria are independently satisfied.
