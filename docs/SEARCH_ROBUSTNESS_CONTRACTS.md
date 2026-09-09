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

Generation ingestion computes the exact encoded record length before allocating
the record buffer. The counter and encoder share the existing wire grammar:
UTF-8 text is lowercase hex, floats retain Rust's `Display` representation, and
field, vector, and metadata separators are unchanged. The counter checks size
arithmetic and does not scan or copy text bytes; it formats vector components
without collecting per-component strings.

Per-record, cumulative logical-byte, spool-byte (including frame headers), and
descriptor-field admission all precede record materialization. Accepted records
use fallible reservation and write directly into their final record buffer.
Metadata field names move from the admitted document rather than being cloned
before admission. Segment batching also uses the counter instead of encoding
and discarding a complete record just to measure it.

Regression coverage includes exact/one-short limits, cumulative admission,
unchanged spool framing, an 8 MiB rejected source, a 1,024-case legacy-codec
differential, and a 48-case public generation/reopen/hydration campaign. Rejected
replacement generations must preserve the active generation and clean staging.
The public campaign is part of the existing local storage fuzz test target.

This is an allocation-order prerequisite, not completion of large-document
support. The caller-owned document and admitted encoded record still coexist;
decoder, analyzer, metadata/vector sidecar, and segment buffers have separate
resident costs. No process-RSS bound, new source/term limit, streaming-source
API, analyzer change, or persisted-format change is implied. The fixed lexical
4 MiB source ceiling remains pending the complete #392 lifecycle work.

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
