# Compact lexical projection: local fuzz campaigns

These deterministic campaigns exercise the private production implementations
in `skein-search`. They do not copy the codec into another crate, expose a
test-only public storage API, or enable an alternate serving path. They belong
to the compact v1 lexical layout tracked by #206; they do not establish that
issue's complete resource, native-platform or representative-corpus acceptance.

## Running and replaying

The required local suite includes four lexical targets and one record-codec
target, all manual:

```bash
bazel test --nocache_test_results \
  //crates/fuzz:skein_fuzz_tests \
  //crates/fuzz:skein_fuzz_cli_tests \
  //:skein_linux_ci_fuzz_smoke_test
```

Run only the lexical campaigns with Cargo:

```bash
cargo test -p skein-search --lib lexical_projection::tests::fuzz:: \
  -- --ignored --nocapture
cargo test -p skein-search --no-default-features --lib \
  lexical_projection::tests::fuzz:: -- --ignored --nocapture
```

Each campaign is an ignored Rust test and has a separate manual Bazel target
named `//crates/search:skein_search_lexical_<campaign>_fuzz_tests`. Campaign names
are `posting`, `dictionary`, `doclist`, and `state_machine`. Bazel supplies the
exact Rust test name and `--ignored`; each target must report one executed test,
not a successful zero-test filter. Ordinary Cargo and search unit-test targets
compile these tests but do not execute them. Do not add these campaigns to
default or dedicated CI jobs. The existing native regressions are separate.

The generator and seeds are fixed in source, making the commands above replay
the complete inputs and action sequences. Logs report accepted/rejected byte
cases, all posting encoding modes, and state-machine transition counts. A
failed filesystem campaign leaves its uniquely named temporary fixture intact;
a successful campaign removes only its own fixture, after dropping readers.

The generation-input campaign is
`//crates/search:skein_search_record_fuzz_tests`, or:

```bash
cargo test -p skein-search --lib document_codec::tests::bounded_record_bytes_campaign \
  -- --exact --ignored --nocapture
```

It uses seed `0x206c0dec` and 25,000 cases. Each valid document compares exact
preflight length and single-buffer encoding with the preceding independent wire
construction, including arbitrary f32 bit patterns, UTF-8/NUL and metadata.
One-byte-under-limit encoding must reject before the output-allocation boundary.
Mutated records exercise byte flips, deletion, non-ASCII hex, truncation and
extra fields. Accepted records must re-encode/decode canonically; they need not
match the original document. No panic-catching success path is used. Ordinary
generation tests separately repair the spool checksum around corrupt hex and
prove rejection before sink consumption, unchanged publication and cleanup.

Each accepted canonical record also goes through the production shared-input
decoder with exact retained-capacity admission, followed by a one-byte-short
budget. The latter must reject before the decoder allocation boundary. Owned
leases must match string/vector capacities plus the metadata-container allowance
and release to zero after drop. This extends input-memory coverage, not the
complete fused-sink budget or allocator/RSS coverage.

## Coverage and independent checks

- `posting`: seed 206, 100,000 valid-encode plus mutated-decode pairs. All three
  modes (128-entry SIMD, varint tails, wide-delta fallback), full-width ordinals
  and term frequencies, truncation, extension, arbitrary bytes and structured
  mutations. Accepted frames must independently agree with their serialized
  count, first/last ordinal, max-TF summary and extent, and roundtrip exactly in
  logical postings.
- `dictionary`: seed `0x206f57`, 32 generated dictionaries and 512 mutations
  each. Keys include shared prefixes, UTF-8 and embedded NUL; metadata uses
  full-width values. Half the cases repair both the FST's masked CRC32C and the
  outer CRC32C where extents allow, so validation cannot stop at a checksum
  gate in every case. Accepted dictionaries must have ordered unique keys,
  match each independent ordered metadata record, preserve lookup/iteration
  agreement, and rebuild/reopen with the same logical mapping. Validation
  callbacks have an asserted work bound, not a catch-and-ignore panic handler.
- `doclist`: seed `0x206d0c`, 2,048 mutations each of lengths 1, 127, 128, 129,
  256, 257 and 513. The real writer creates frame and skip bytes; the real cursor
  reads bounded slices at physical offsets above 32 bits. Cases mutate payloads
  and metadata, with and without repaired frame/skip checksums. Accepted output
  must independently match every frame summary, checksum and skip record; its
  DF, order, physical end and exhausted-cursor behavior are checked. The input
  slice may contain bytes after the metadata-selected doclist, as a real shared
  artifact does; those are not incorrectly treated as doclist trailing bytes.
- `state_machine`: seeds 206, 18 and `0x5eed`, starting from 257 documents with
  a term that must cross the SIMD/tail/skip boundary. Per seed, 33 directed
  transitions ensure update/update/delete/delete/reinsert, insert/delete,
  reopen, publish, failed publish and rejected operations all occur. Another
  64 transitions use seeded random ordering and affected keys. After every
  transition, a reference model recomputes corpus statistics and BM25 from the
  current document map; full results and a bounded rank window are compared
  under a seeded exclusion filter. This is 588 reference comparisons overall,
  in addition to publication and old-reader checks.

The state oracle shares the analyzer and BM25 parameters but does not call the
production scorer, DF accounting, delta merge or top-k collector. It checks
live delta visibility across reopen, pinned old readers across publication,
manifest preservation and temporary cleanup after early/late build failure,
cancelled queries, memory/cache rejection and recovery on subsequent queries.
Rejected-operation transitions also cancel a real lexical build after a
generation-dependent input prefix, checking unchanged publication and temporary
cleanup before the next reference query.
Reported physical byte categories must sum exactly; cache residency stays
within capacity and query pins return to zero.

Mutation controls remove the last-ordinal check, skip-checksum check, or reverse
the delta DF subtraction, one at a time. The relevant campaign must fail under
each control; all temporary production mutations must be restored before
positive validation. These checks demonstrate non-vacuous oracles, not exhaustive
proof against every possible defect.

Structurally valid mutated bytes may represent different data. Their oracle is
structural consistency and logical roundtrip, not equality with the original
unmutated corpus. Corpus parity is checked separately on uncorrupted states.
The campaigns do not measure whole-host RSS, provide a libFuzzer coverage-guided
search, validate the outer fused generation's complete admission ledger, or
substitute for same-corpus before/after byte evidence and native platform tests.
