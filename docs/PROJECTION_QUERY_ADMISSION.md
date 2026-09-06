# RaBitQ projection search buffer admission

`search_projection` now plans and checks its standalone working-memory envelope
before allocating the transformed query, top-k heaps, allowlist masks or worker
threads. Public options/output signatures, v1 projection bytes and I/O backends
are unchanged. This is a prerequisite for shared-root query admission, not a
claim that the projection backend already holds a query-memory lease.

## Allocation and lifetime boundaries

Every segment writes into its worker's existing top-k heap. There is no separate
segment heap, segment result vector or per-segment sort. Parallel workers move
their heap entries directly into a merge heap without an intermediate vector.
The single-worker path does not allocate a merge heap at all.

Final conversion uses one explicitly sized result vector while the final heap
still exists; it does not depend on unspecified iterator allocation reuse.
Sorting is in-place and unstable: score total order followed by unique document
ID already determines the entire order, including zero-query ties.

The requested top-k is capped at the corpus count and, when present, allowlist
length. These are upper bounds on possible hits, so even `usize::MAX` requests
retain identical results without allocating unusable heap capacity. An allowlist
may contain absent IDs; the bound is not a claim that every ID will produce a hit.

Before scanning, let:

- `Q` be query dimension times the size of an f32;
- `H` be the capped limit times the actual heap-entry size;
- `O` be that limit times the output-hit size;
- `M` be the largest segment's allowlist mask, or zero without an allowlist;
- `P` be the existing largest file-segment payload allowance, or zero in memory;
- `W = P + M + H + 512 KiB + 1 KiB` be the per-worker allowance;
- `G = Q + max(H, O) + 1 KiB` be the global allowance.

| Phase | Concurrent buffer ownership |
| --- | --- |
| Single-worker scan | Query, one worker heap and its current mask |
| Parallel scan/merge | Query, merge heap, all live worker heaps/masks |
| Final conversion | Query, final heap and output vector; workers have joined |
| Empty result fast path | Query only, preserving query validation |

Nonempty searches require at least `G + W`. The worker count is capped by
available memory, requested/task parallelism, segment count and allowlist density.
The report records `G + workers * W`, including the previous conservative stack
allowance on the single-worker path. At final conversion, the released worker
allowances cover the final heap while the global allowance covers the output.
No additional sorting buffer is needed. Empty-result paths still enforce `Q`
before transformation; zero top-k does not bypass the budget or vector validation.

Capacity multiplication checks both arithmetic overflow and the Rust allocation
addressability limit. Envelope sums/products use checked arithmetic and fail
closed even when the configured limit is `usize::MAX`; saturation cannot turn
an unrepresentable request into an admitted one.

`FileProjection` currently borrows a validated mmap, not a copied read buffer.
`P` preserves the prior conservative file working-set allowance. It is not a
lease for the mapping's full host lifetime. Fixed thread allowances are also
not measured process RSS or a bound on allocator/OS/runtime implementation costs.

## Verification

Seven normal regressions cover pre-allocation/segment-entry rejection, exact
budget success, all empty paths, huge limits, one heap across many segments,
worker/allowlist/task ceilings, virtual overflow, cancellation and rejection of
partial heaps after a corrupt segment. Query/heap allocation probes count the
production allocation boundaries on the calling thread; they are not a global
allocator or worker-stack RSS measurement.

The manual seed `0x2065ca11` campaign uses 192 corpora and both in-memory and
reopened file artifacts, one/four-bit codes and five budget boundaries. It checks
results against independent packed-bit decoding and full sorting, and checks
admission against an independent phase model. The unchanged query transform is
shared with the oracle; this is quantized ranking parity, not an approximate
recall or transform-accuracy qualification. Fuzz stays out of ordinary and
dedicated CI jobs.

```bash
cargo test -p skein-vector-projection
cargo test -p skein-vector-projection projection_search_memory_campaign \
  -- --ignored --nocapture
bazel test --nocache_test_results \
  //crates/search:skein_search_tests \
  //crates/vector-projection:skein_vector_projection_tests \
  //crates/fuzz:skein_fuzz_tests \
  //crates/fuzz:skein_fuzz_cli_tests \
  //:skein_linux_ci_fuzz_smoke_test
```

Five negative controls must fail assertions: moving admission after the query
allocation, introducing a redundant segment heap, omitting the merge/output
allowance, omitting the mask, and replacing checked sums with saturation. Restore
all controls before positive verification.

## Remaining #206 boundary

The planner and heap changes are private to `skein-vector-projection`. The
query-memory ledger currently belongs to `skein-executor`; this change neither
adds that dependency nor publishes a new cross-crate admission API. Connecting
the envelope to the existing shared query root before entry and retaining the
returned hits' owner are still required. Charging selected ordinals afterward
does not cover this earlier lifetime. Combined component limits, mapping/reader
host ownership and the remaining boundaries in `VECTOR_QUERY_ADMISSION.md`
also remain open. No partial completed-issue claim is made.
