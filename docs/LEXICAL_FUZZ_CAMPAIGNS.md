# Compact lexical projection: local fuzz campaigns

These deterministic campaigns exercise the private production implementations
in `skein-search`. They do not copy the codec into another crate, expose a
test-only public storage API, or enable an alternate serving path. They belong
to the compact v1 lexical layout tracked by #206; they do not establish that
issue's complete resource, native-platform or representative-corpus acceptance.

## Running and replaying

The required local suite includes nine lexical targets, one record-codec target
and segment/RaBitQ/candidate/vector/projection-search admission targets, all manual:

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
are `analyzer`, `artifact`, `merge`, `posting`, `dictionary`, `dictionary_memory`,
`doclist`, `query_memory`, and `state_machine`. Bazel supplies the
exact Rust test name and `--ignored`; each target must report one executed test,
not a successful zero-test filter. Ordinary Cargo and search unit-test targets
compile these tests but do not execute them. Do not add these campaigns to
default or dedicated CI jobs. The existing native regressions are separate.

The generator and seeds are fixed in source, making the commands above replay
the complete inputs and action sequences. Logs report accepted/rejected byte
cases, all posting encoding modes, and state-machine transition counts. A
failed filesystem campaign leaves its uniquely named temporary fixture intact;
a successful campaign removes only its own fixture, after dropping readers.

The query-memory campaign uses seed `0x2065c0e` for 512 queries against a real
published lexical artifact, with replacements/deletes/inserts, filters and six
retention windows. Scores are compared with the existing independent document
frequency/BM25 oracle. Each query shares its root with competing live work,
retries exact/one-short peak budgets, and keeps returned score IDs charged.
Thirty-two cancelled traversals release charges and cache pins. See
`LEXICAL_QUERY_ADMISSION.md` for the ownership boundary and remaining query work.

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

The segment campaign is
`//crates/search:skein_search_segment_admission_fuzz_tests`, or:

```bash
cargo test -p skein-search --lib segment_admission_campaign \
  -- --ignored --nocapture
```

Seed `0x2065e67` generates 1,000 document groups, 1,000 descriptors and 3,000
document/metadata/vector payloads. Independent preceding encoders check exact
bytes and decompression, including Unicode/NUL, arbitrary f32 values, missing
vectors, JSON/CSV metadata and full-width ordinals. Each descriptor and payload
has exact/one-short shared-root retries (4,000 each); every outcome releases all
tracked memory and retains three accounts. Thirty-two cancelled compressions
must not enter zstd. Normal tests separately prove retained layout lifetime,
before-allocation denial, publication rollback and compression block boundaries.
See `SEGMENT_BUILD_ADMISSION.md` for dependency envelopes and remaining gates.

The RaBitQ campaign is
`//crates/search:skein_search_rabitq_admission_fuzz_tests`, or:

```bash
cargo test -p skein-search --all-features --lib rabitq_admission_campaign \
  -- --ignored --nocapture
```

Seed `0x2064ab1` generates 500 groups across all 64 combinations of eight vector
dimensions, one/four-bit encoding and four segment-row limits. Finite float bit
patterns, zero/missing vectors, identity escaping, epochs and transform seeds
vary. Artifact bytes match an independent direct-writer schedule. Each group
checks exact/one-short root budgets and complete release with three accounts;
16 cancellations must stop before backend reentry. See `RABITQ_BUILD_ADMISSION.md`
for source-qualified bounds and the distinct remaining serving-reader gate.

The candidate campaign is
`//crates/search:skein_search_candidate_admission_fuzz_tests`, or:

```bash
cargo test -p skein-search --all-features candidate_admission_campaign \
  -- --ignored --nocapture
```

Seed `0x206ca11` covers 1,000 groups, exact/one-short shared roots, competing
owners and 4,000 mutated candidate blocks. Independent equality filtering and
direct wire/cursor oracles check ID/count/order rather than calling production
predicate or decoder helpers. UTF-8/NUL IDs, optional ordinals and cancellation
are included. Seed `0x206ec0de` also checks 256 concatenated-frame envelopes,
256 exact/one-short roots, 768 corrupt inputs and 16 cancellations. Output is
compared with independently generated text and corruption with the preceding
streaming decoder. See `CANDIDATE_QUERY_ADMISSION.md` for admitted lifetimes,
source-qualified zstd bounds and the remaining full-query/native gates.

The raw-vector/result campaign is
`//crates/search:skein_search_vector_admission_fuzz_tests`, or:

```bash
cargo test -p skein-search --all-features vector_admission_campaign \
  -- --ignored --nocapture
```

Seed `0x206cec70` supplies 128 queries to scalar and explicitly attached RaBitQ
rerank paths. Independent cosine/top-k results, zero/negative queries, metadata
filters, five retention windows, competing score owners and exact/one-short
root retries are checked. The same campaign without default features covers
the scalar path. These are raw-sidecar/retained-score checks, not proof of the
RaBitQ backend's still-separate workspace envelope or complete query/RSS limits.

The projection-search campaign is
`//crates/vector-projection:skein_vector_projection_search_memory_fuzz_tests`, or:

```bash
cargo test -p skein-vector-projection projection_search_memory_campaign \
  -- --ignored --nocapture
```

Seed `0x2065ca11` generates 192 corpora across one/four-bit encodings, seven
dimensions and ten corpus sizes. Both in-memory and reopened mapped artifacts
exercise five budgets, including one-byte-short rejection before query-buffer
allocation. Independent bit decoding and full sorting check quantized ranking;
an independent phase envelope checks worker counts and memory reports. Zero
queries, empty/missing/sparse/dense allowlists, huge top-k requests and real
parallel scans are included. This qualifies the standalone buffer envelope,
not a shared-root lease or full reader/output lifetime ownership. See
`PROJECTION_QUERY_ADMISSION.md` for the exact boundary and negative controls.
See `VECTOR_QUERY_ADMISSION.md` for the remaining boundaries.

## Coverage and independent checks

- `dictionary_memory`: seed `0x206fc7`, 2,000 key groups compare actual dictionary
  staging, recursive partitions and lookup/iteration with an independent ordered
  input map. Unicode/NUL/shared-prefix keys, full-width metadata and block caps
  from 512 bytes to 64 KiB exercise builder, output, validation and directory
  overlap. Exact and one-short operation budgets are checked for each group;
  63 cancelled finishes must stop before another build or output delivery.
  All outcomes release tracked memory and retain exactly three accounts.
  Normal tests additionally exercise the 1,024-term automatic-flush boundary.
- `merge`: seed `0x206ae12`, 6,000 groups with up to eight runs compare the real
  k-way cursor against an independent ordered-tuple union. Duplicates, empty
  runs, Unicode/NUL terms and full-width ordinals/TFs are included. Every group
  has exact and one-short shared-root retries; 188 consumer cancellations must
  prevent another record decode. Another 12,000 mutated run byte strings must
  agree with a separate direct decoder/order oracle. All outcomes return
  tracked memory to zero and retain exactly three operation accounts.
- `artifact`: seed `0x206a47`, 12,000 document groups compare the actual document-map
  encoder with independent direct v1 bytes. Unicode/NUL IDs, full-width u32 lengths,
  block flush/reuse and cancellation are included. Each group is retried with its
  exact observed requested-capacity peak and a one-byte-short budget; all results
  and failures must release the operation's three-account working set.
- `analyzer`: seed `0x206a11`, 12,000 document/lexicon combinations compare
  incremental production frequencies and weighted length with the preceding
  frozen allocating token-list oracle from `2b9caa56`. Four lexicons include overlapping alias rules and
  stopwords; title/content/metadata mix identifiers, punctuation, CJK, NUL and
  Unicode case boundaries. Exact token limits succeed, one-short limits reject,
  and cancelled child contexts do not cancel their parent. Every result/error
  releases retained charges and the operation keeps exactly three accounts.
  A second seed `0x206b11` compares 12,000 ordered identifier outputs against that
  oracle for both serving and streaming paths, with exact/one-short shared-memory
  limits, correct output prefixes on failure, and lease release. The oracle
  retains the preceding character-vector/parts/ngram/suffix algorithms rather
  than sharing their new production recipes. A scalar sweep covers lowercase
  admission/release for all 1,112,064 Unicode scalars. Jieba/TLS allocation and
  shared dictionary ownership are still outside this admission evidence.
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
