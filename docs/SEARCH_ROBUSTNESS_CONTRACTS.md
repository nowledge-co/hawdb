# Search robustness contracts

Lexical build and mini-delta token emission, unchanged analyzer semantics,
early admission, and remaining whole-run working units are documented in
[`INCREMENTAL_DOCUMENT_ANALYSIS.md`](INCREMENTAL_DOCUMENT_ANALYSIS.md).

This document records the disposition of all seven items in issue #230 and the
generation record admission prerequisite for #392. The changes preserve the
v1 persisted codecs and public Rust option/report fields.
Recall report JSON gains sampling metadata; the analyzer fingerprint changes
because supplementary Han n-grams change derived term contents.

## Lexical persistence and failure admission

The format tests exercise the existing validation boundaries directly:

- A stale envelope checksum fails before manifest use. Recomputed checksums
  do not bypass path, block-boundary, format or count validation.
- Query-time block CRC32C and whole-artifact reopen checks reject bit flips.
- Posting decoders reject every truncated prefix and trailing data. Spill
  readers reject partial records, oversized lengths and incomplete headers;
  EOF at a complete record boundary is valid because runs have no record-count
  footer. This does not claim that a record-boundary truncation is detectable.
- Spill bytes, spill-run counts (including compaction outputs), fan-in and
  block limits fail without changing the published manifest/artifact. Failed
  builds remove their temporary files. Exact read-block admission succeeds;
  one byte below the largest block fails.
- Mini-delta insert, replacement and delete rejection preserve prior contents,
  accounting and scores. A later operation with sufficient budget succeeds.

These are corruption and resource-limit tests, not an adversarial authenticity
claim: CRC32C is not a cryptographic signature. Existing recovery and fuzz suites
remain complementary and fuzz is not added to CI.

## Recall sampling

Samples use Floyd sampling without replacement driven by SplitMix64, with
rejection sampling for integer bounds. The fixed seed is `534b45494e524543`.
Selection uses at most 128 entries, O(samples) memory and O(samples log samples)
selection work, followed by the existing ascending eligible-document scan.
It does not shuffle or allocate an index for the whole corpus. A fixed seed
makes replay deterministic, not statistically independent across repeated runs.

The report's `sampling` JSON object names the method, seed and query source.
Queries are still indexed vectors, with the query document's own hit removed.
This in-corpus self-query proxy can overestimate held-out production-query
recall. Neither the new sampling nor a ready report proves held-out performance;
production qualification retains its separate identity/evidence gates.

## Vector delta headroom: intentional correctness bound

The proposed `top_k + min(delta.len(), top_k)` cap is not generally safe.
For k=1, the two highest-ranked base rows can both have been updated to poor
vectors. Fetching only two base hits then discards the third row even when it
is now the best result. More than k base rows can be shadowed.

`search_with_delta` therefore retains `k + delta.len()` headroom and documents
its cost. The supplied base callback must obey the scan memory budget and fail
closed when the enlarged request cannot be admitted. `should_optimize` remains
the rebuild signal, not automatic background work. A 256-case independent
latest-value-map oracle covers shadowing and allowlists and demonstrates the
unsafe cap's failures; a real base scan verifies memory admission. This issue
does not claim constant-k base work, a new delta backend, or an RSS improvement.

## Supplementary Han and cleanup generations

N-gram classification reuses the existing Han predicate used by the jieba
adapter, including its supplementary ranges. Kana and Hangul remain supported.
The analyzer fingerprint suffix changes so old derived lexical projections
are not reused under the new token rules. Stale fingerprints request a rebuild;
they do not imply corrupt canonical data. No persisted format version changes.

`search_lexical.manifest.<generation>.skein` now belongs to the lexical
retention domain, just like its corresponding artifact. A 512-case matrix with
different lexical/out-of-core generations checks their identical retention
decisions; quarantine parsing and the unversioned live manifest remain covered.

## Vector score boundary and backend ownership

The search facade's final vector relevance is positive-only: exact cosine
similarity is clamped to [0,1], and nonpositive values produce no vector hit.
RaBitQ estimates remain signed candidate-ranking hints; reranking is the final
score authority. A negative-vector document may still match a text retriever.
Tests cover opposite, orthogonal and identical vectors and resident/persisted
facade results. Existing ordinal differential tests also retain negative
candidate estimates across resident and file-backed projections.

The private `RaBitQCandidateProjectionStorage` enum is intentionally retained.
The referenced `SegmentReader` is private to `skein-vector-projection`, not
`skein-search`; both storage variants already call its shared scan kernel.
Two small ownership-dispatch matches do not duplicate the algorithm. Exposing
scan buffers/per-segment implementation types merely to remove those matches
would create an unnecessary cross-crate API. This is not the separately
requested injectable storage-engine API.

## Generation record admission (#392)

Generation ingestion computes the exact encoded record length before writing
the framed spool record. The counter and encoder share the existing wire grammar:
UTF-8 text is lowercase hex, floats retain Rust's `Display` representation, and
field, vector, and metadata separators are unchanged. The counter checks size
arithmetic and does not scan or copy text bytes; it formats vector components
without collecting per-component strings.

Per-record, cumulative logical-byte, spool-byte (including frame headers), and
descriptor-field admission all precede record encoding. Accepted records
stream into the spool using bounded hex scratch space.
Metadata field names move from the admitted document rather than being cloned
before admission. Segment batching also uses the counter instead of encoding
and discarding a complete record just to measure it.

Regression coverage includes exact/one-short limits, cumulative admission,
unchanged spool framing, an 8 MiB rejected source, a 1,024-case legacy-codec
differential, and a 48-case public generation/reopen/hydration campaign. Rejected
replacement generations must preserve the active generation and clean staging.
The public campaign is part of the existing local storage fuzz test target.

This is an allocation-order prerequisite, not completion of large-document
support. The caller still owns a complete document, and spool decoding produces
another complete document; analyzer and descriptor state have separate resident
costs. Segment payload encoding is described below. No process-RSS bound,
new source/term limit, streaming-source
API, analyzer change, or persisted-format change is implied. The fixed lexical
4 MiB source ceiling remains pending the complete #392 lifecycle work.

## Segment payload encoding (#392)

The generation segment builder counts each complete document, metadata and vector
text payload, including its header, before encoding it. It writes the existing V1
grammar directly through zstd using bounded hex scratch, with incremental raw and
compressed checksums. The compressed destination checks its admitted limit before
reserving memory; geometric growth reuses existing capacity and stays within that
limit. The final envelope is inserted in place and must fit the same limit.

This removes whole uncompressed segment strings, document/sidecar encoding
temporaries, and a separately owned copy of the compressed payload. Segment
grouping, vector ordinals, codec level, envelope fields, defaults and publication
order remain unchanged. An encoding failure still drops staging and preserves
the previously published generation.

The segment's owned documents and descriptor still reside in memory. zstd has
native scratch state, and the allocator may internally copy or round a reservation.
The compressed limit bounds requested destination capacity; it is not a total
build-memory or process-RSS limit. The raw limit remains a representation limit,
and the 4 MiB lexical source guard remains in force.

On Linux with the default features, a public writer regression measured Rust
allocator requests during `finish()` after analyzer warmup. Input construction and
subsequent hydration are outside the window. The fixture is one document with a
whitespace body, a small title and metadata; both versions verify the complete
document after reopening. Against main `366828ec`:

| Body bytes | Previous total requested | Streaming total requested | Previous largest request | Streaming largest request |
| ---: | ---: | ---: | ---: | ---: |
| 1,048,576 | 19,083,795 | 4,403,396 | 4,194,524 | 1,048,576 |
| 3,145,728 | 46,348,315 | 10,696,543 | 12,583,132 | 4,194,304 |

These count allocator requests, including reallocations, rather than live memory,
RSS, native zstd allocations or throughput. The remaining largest requests match
the geometric decoded-input buffer. The observation uses the instrumentation retained at `6cd5df15`. The production
encoder is unchanged in the final revision; its regression shares the cumulative
allocation counter with the independent identifier work in PR #482. It requires
total requests below eight times source bytes, a bound the unchanged baseline
fails after both complete round trips succeed.

Coverage retains an independent legacy text/envelope oracle for all three payloads:
empty fields, Unicode, separators, float edge cases, optional/global vector
ordinals, 128 seeded record sets, and hex/zstd buffer boundaries through a 1 MiB
field. Every case compares compressed bytes, decoded text, exact budgets and
one-short budgets. Separate tests cover rejected growth without mutation, capacity
reuse, short writes and I/O errors. Five deliberate faults in raw/compressed
admission, checksums, ordinals and capacity reuse produce test assertions.

Use `cargo test -p skein-search` and the default Bazel search matrix plus the
required local fuzz suite. `//crates/search:skein_search_segment_allocation_tests`
executes the public regression under Bazel. This slice does not complete the
large-document lifecycle or replace #206's complete-corpus qualification.

## Descriptor serialization admission (#392)

Descriptor serialization uses the shared bounded hex sink for the existing V3
grammar. A counting pass and a checksum pass determine the complete encoded size,
including the variable-length checksum footer, before opening the temporary file.
After admission, the descriptor streams to a buffered file, flushes and syncs,
then uses the existing atomic replacement path. Resident checkpoints use the
same encoder and retain their existing interface; generation builds pass their
existing descriptor limit. The in-memory descriptor and its working-set ledger
are unchanged.

The encoder no longer builds per-value hex strings, a dictionary join, the
complete descriptor body, or another complete body with its checksum appended.
Its scratch consists of fixed 8 KiB hex and I/O buffers plus a small footer.
This is not a total descriptor-memory or RSS bound: dictionary construction,
metadata normalization, the decoded source and reader decoding remain separate
resident costs. The existing 4 MiB lexical source guard remains in force.

A Linux default-feature regression observes public `writer.finish()` after a
warmup generation. Each fixture has one metadata value containing two tokens
separated by padding, retaining the complete value in the descriptor without
introducing a large token-frequency workload. Input construction and subsequent
hydration are outside the allocation window. Both implementations compare the
complete document after reopening. The baseline is PR #485 at `eb89261c`:

| Metadata value bytes | Previous total requested bytes | Streaming total requested bytes |
| ---: | ---: | ---: |
| 1,048,576 | 39,007,243 | 5,448,560 |
| 3,145,728 | 118,701,513 | 13,839,662 |

These are Rust allocator requests, including reallocations, rather than peak live
memory, native zstd allocations, RSS or throughput. The fixture regression uses
a cumulative bound of twelve times the value size; that is a test bound, not a
new production resource policy. The unchanged baseline fails it after both full
reopen/hydration checks succeed.

The retained previous encoder is a test-only byte oracle. Coverage includes
empty and optional fields, Unicode, numeric/timestamp ranges, multiple segments,
256 seeded descriptors, and the legacy representation of empty dictionary values.
Tests check every short-write boundary through the footer, exact/one-short size
admission before touching active or temporary files, and bounded write chunks
for a large field. Public generation coverage proves that a rejected replacement
preserves all previous artifacts and reader results, while exact admission
publishes a readable generation. Six deliberate admission, checksum, dictionary,
range, materialization and writer-budget faults produce assertions.

`//crates/search:skein_search_descriptor_allocation_tests` exposes the public
regression through the existing Bazel search matrix. This slice depends on the
private streaming grammar from PR #485 and does not complete the remaining
large-document lifecycle or #206's complete-corpus acceptance.

## Descriptor dictionary construction (#392)

Generation descriptors accumulate fields and unique values under the existing
working-set ledger. The builder charges field names, document bounds, unique
normalized values and the existing layout estimate before retaining each
component. A failed admission stops before appending the current segment's
payloads. Duplicate values do not consume another retained-value charge, and
previous segments stay included in the projected total. The 256/192/32-byte
component estimates and 96-byte layout estimate are unchanged; they are not
allocator-capacity or process-RSS accounting.

The private generation builder visits label values without collecting a complete
array. It validates the entire JSON string array first, then visits trimmed,
nonempty strings one at a time. An invalid element, malformed escape, or trailing
input selects the existing whole-input CSV fallback before any JSON value is
emitted. Resource or visitor errors propagate unchanged and never select that
fallback. The descriptor's query behavior, presence counts, normalized dictionary,
numeric/timestamp ranges, and existing V3 representation are unchanged.

Already normalized values remain borrowed until a new dictionary entry needs
ownership. The existing whole-value Unicode lowercase mapping still handles
contextual final sigma; enum values retain their ASCII-only mapping and kind
aliases. One changed-case normalized value and the JSON decoder's largest
escaped-string scratch remain explicit resident units. The source document,
retained descriptor and collection capacity also remain resident. This change
does not claim that the descriptor limit bounds all temporary or process memory,
remove the lexical 4 MiB guard, or complete #392's resource/lifecycle work.

A Linux default-feature regression measures cumulative Rust allocation requests
during warmed public `writer.finish()`. Inputs use repeated labels; complete
source documents are compared after reopen and hydration. Input construction,
spooling via `push`, and subsequent hydration are outside the measurement window.
The baseline is PR #486 at `4d2f86fa`, using identical Cargo profiles:

| Label representation | Label count | Source value bytes | Previous requested bytes | Incremental requested bytes |
| --- | ---: | ---: | ---: | ---: |
| CSV | 32,768 | 65,535 | 4,013,989 | 2,401,255 |
| CSV | 131,072 | 262,143 | 9,224,103 | 2,794,469 |
| JSON | 32,768 | 131,073 | 4,472,759 | 2,794,485 |
| JSON | 131,072 | 524,289 | 11,059,129 | 4,367,351 |

The regression bounds requested-byte growth by eight times input-byte growth
between the two sizes, excluding the fixed analyzer/generation floor. That is
a fixture check, not a new production budget. All four unchanged-baseline
round trips succeed before its allocation-growth assertion fails. These are
allocator requests, not live allocations, native memory, RSS or throughput.

The retained resident descriptor builder and its original accounting function
provide an independent semantic/ledger oracle. Tests cover exact/one-short
limits, previous segments, duplicates, missing/empty/default fields, kind aliases,
Unicode mappings, JSON/CSV fallback and 256 seeded summaries. A failed second
segment preserves all three payload lengths and the first descriptor entry.
Nine deliberate admission, duplicate-charge, retained-segment, range, JSON-tail,
visitor-error, Unicode-context, label-collection and late-admission faults fail
assertions and are restored before qualification. Existing public generation
failure/cleanup and complete-document hydration tests remain part of the owner
suite. The allocation regression runs through the existing descriptor allocation
Bazel target; no new CI or fuzz target is introduced.

## Verification

```bash
cargo test -p skein-search --lib
cargo test -p skein-vector-projection
cargo clippy -p skein-search -p skein-vector-projection --all-targets --all-features -- -D warnings
bazel test --nocache_test_results //crates/search:skein_search_tests \
  //crates/vector-projection:skein_vector_projection_tests //:skein_unit_tests \
  //crates/fuzz:skein_fuzz_tests //crates/fuzz:skein_fuzz_cli_tests \
  //:skein_linux_ci_fuzz_smoke_test
```

Feature-independent lexical, sampler, classifier, cleanup and scalar-score
tests must also execute without default features. The broader pre-existing
minimal-suite capability assumptions are tracked separately in #313 / #314.
