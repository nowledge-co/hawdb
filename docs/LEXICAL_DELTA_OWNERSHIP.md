# Resident lexical delta ownership

The resident `SearchIndex` keeps one generation-bound pair: an immutable lexical
reader and the mini-delta whose base statistics were derived from that reader.
A single private `RwLock` protects the pair. Concurrent text queries borrow it
through scoring; publication and invalidation replace both members atomically.

The read guard is acquired after vector execution and released immediately after
lexical scoring, before result shaping or telemetry. Queries do not clone the
delta's document maps, term trees or strings. An error or unwind releases the
guard. This is ordinary cross-platform standard-library synchronization, not a
new public snapshot or storage API. It does not promise a bounded writer-wait
time: the platform's read/write lock fairness policy still applies.

Previously a query cloned the reader and delta under separate mutexes. Besides
duplicating resident memory outside query admission, that allowed a concurrent
checkpoint to pair an old reader with a new generation's empty delta. A single
read guard prevents that mismatch without serializing concurrent readers.

## Mutation ownership

Base terms retain the analyzer's existing frequency tree; membership checks do
not need a second tree without frequency values. Replacements borrow the old
base while checking the proposed logical size, then transfer it only after all
fallible analysis and admission checks pass. Replacing an upsert updates its
existing entry. Moving between upserts and tombstones preserves the owned ID
buffer and base tree. Repeated deletion is idempotent. Deleting an insertion
that never existed in the generation creates no tombstone.

`LexicalMiniDelta`, `DeltaDocument` and `BaseDocumentTerms` deliberately do not
implement `Clone`. Pinned-reader tests transfer delta ownership instead of
silently copying it. Error fallback still invalidates the resident projection
and serves the authoritative documents; publication rebuilds the projection.

## Verification

Normal regressions cover analyzer-tree node and key-buffer identity, repeated
replacement/delete/resurrection, exact logical-cap replacement, rejected
mutations, original-generation statistics and generation-local insertions.
Real `SearchIndex` regressions pause one query, prove a second query can complete,
build the next projection, and prove the pair cannot swap until the first query
finishes. Queries before and after publication must retain identical BM25 scores.
Other regressions cover unwind release and atomic fallback after admission failure.

```bash
cargo test -p skein-search --all-features mini_delta
cargo test -p skein-search --no-default-features mini_delta
cargo test -p skein-search --all-features lexical_state_tests
cargo test -p skein-search --all-features projection_state_machine_campaign -- --ignored --nocapture
cargo test -p skein-search --no-default-features projection_state_machine_campaign -- --ignored --nocapture
bazel test --nocache_test_results //crates/search:skein_search_tests \
  //crates/fuzz:skein_fuzz_tests //crates/fuzz:skein_fuzz_cli_tests \
  //:skein_linux_ci_fuzz_smoke_test
```

The existing manual state-machine campaign checks unchanged key/base ownership
across seeded mutations alongside its independent DF/BM25 oracle, filters,
reopen, failed publication and pinned-generation checks. No fuzz target is added
to CI. The real host text-query tests require `full-text-search`; private delta
and state-machine coverage runs with both full and minimal features.

## Remaining admission boundary

Eliminating duplicate owners is not complete resident-memory admission. The
mini-delta's current `resident_bytes` counter remains a logical size estimate,
not a capacity or allocator measurement. Initial analysis, tree allocation and
old/new update overlap still need persistent ownership leases tied to an
appropriate host admission contract. Tokenizer/Jieba retained workspace,
cross-crate output/reader contracts, combined caps, representative-corpus
measurements and exact-head native qualification remain separate #206 gates.
No v1 bytes, production limits, dependency versions or I/O backend change here.
