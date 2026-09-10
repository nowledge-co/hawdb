# Document-local frequency spill

The lexical generation writer can spill one document's distinct-term analysis
state without treating physical chunks as separate search documents. This is a
private execution improvement for issue #392, not completion of large-document
support or a change to the source, term, analyzer, or published-format contract.

## Execution

Small documents use the existing in-memory frequency accumulator. Before a new
term would exceed its allowance, the writer releases any corpus posting buffer
that competes for the estimated build-memory budget. If the document still does
not fit, it transitions to sorted staging runs without replaying already analyzed
source. The previous accumulator's complete counts and last-field markers are
preserved in the first run.

Later occurrences retain a `(term, field)` summary: the sum of ordinary repeated
weights and the earliest occurrence's ordinal/type. A phrase alias contributes
only if the first occurrence of either type in that field was a unique alias.
Summing independently finalized chunk frequencies or applying exact-Posting
deduplication would violate this rule.

Binary carry merging keeps one run per level, bounding live document-run metadata
by the logarithm of the configured run ceiling. This initial implementation uses
two-way merges within the existing maximum fan-in; it does not promise optimal
I/O at larger configured fan-in. Document and corpus runs share cumulative spill
bytes, run sequence numbers, and the run-count ceiling. Removing an input does
not refund cumulative write charges.

After reduction, a first bounded pass determines the complete document length.
A second pass emits one posting per term/document with that length. No complete
final term map is re-created. This preserves tf, one df contribution, corpus
statistics, matching counts, BM25 scores and tie behavior.

## Admission and cleanup

The spill path reserves fixed merge/read/write progress, bounded key storage,
run metadata and final-posting space before filling its record buffer. That
buffer charges its vector capacity and owned term capacities before reservation
and insertion. The resident fast path and existing corpus posting buffer still
use their pre-existing estimated charges; these are not allocator or whole-process
RSS limits. Input strings, opaque identifier/Jieba scratch, artifact/dictionary
construction and host-owned state remain separate resident floors. Complete
shared resource-governor/allocator accounting remains work under #392/#186.

The private `SKNDOCF1` files contain sorted field summaries with a count/checksum
footer. They are staging-only and never appear in published manifests. Readers
check bounded lengths, UTF-8, field/summary validity, strict key order, record
count and checksum. Every header, complete record and footer is admitted before
its physical write. Pending outputs and merge inputs stay under cleanup ownership;
file handles close before unlink for cross-platform behavior. Permanent filesystem
refusal to remove a path remains best-effort cleanup, not a claim of guaranteed
deletion. No io_uring or platform-specific I/O is introduced.

Consumer errors and I/O failures discard private document state and prevent
generation publication. This does not introduce a public cancellation API or
claim that a host deadline currently interrupts every analyzer/merge operation.
Generation rebuild/update uses the new path; the independently bounded in-memory
mini-delta representation is unchanged.

## Artifact block encoding

Document-map and posting blocks retain their v1 wire grammar and existing
target-block boundaries. Before writing a block, the encoder follows that grammar
through a counting sink: checked string/count representation, exact encoded
length, the local block limit and the next offset/identity must all fit. This
pass does not copy or hash the field bytes.

The admitted block then streams directly through the existing buffered artifact
writer. Individual writes are at most 8192 bytes; CRC32C covers only bytes
actually accepted by the writer, including short/interrupted-write handling.
No complete encoded block vector, per-block seek, flush or fsync is needed.
Descriptors, offsets and completed posting counts advance only after the full
block write succeeds. Partial-write errors retain the existing staging cleanup
and publication ownership; they do not publish a descriptor for the prefix.

This removes one encoded-block-sized temporary allocation, not the pending
document/posting vectors, owned keys, dictionary/descriptor state, whole-artifact
digest buffer, or the input/analyzer working sets. The extra sizing traversal
and per-field checksum updates are explicit CPU tradeoffs, not a throughput
improvement claim. Shared allocator/RSS governance remains separate work.

An independent legacy-wire oracle and a 512-case local campaign cover both block
kinds, exact/one-short admission, Unicode and long fields, bounded writes,
short/zero/interrupted writes and fault prefixes. Integration checks cover block
boundaries, term statistics, rejected public generation publication, old-reader
hydration and a subsequent successful generation. The campaign is a manual target
in the existing local fuzz suite; no fuzz CI job is added.

## Verification

Tests compare against the frozen materialized analyzer traversal, including all
term frequencies and lengths, physical split/merge order, resolved-prefix
transitions, title weights, metadata fields, identifiers, aliases and Unicode/CJK.
Generation tests compare all postings and df/corpus statistics, exact scores and
ties, high-budget build followed by lower-budget reopen, and failed-publication
retention. The public generation writer is exercised through hydration and
generation updates/deletes. Run tests cover byte admission before observed writes,
exact/one-short capacity, shared quotas, every truncated prefix, byte corruption,
short/partial writes, flush failure, read/consumer failure and unwind cleanup.

The explicit 128-case local campaign varies analyzer inputs and memory-derived
split boundaries, then checks exact and one-short cumulative spill admission.
It is included only in the existing local Bazel fuzz suite, not a CI fuzz job.

```sh
cargo test -p skein-search --all-features -- --include-ignored
cargo clippy -p skein-search --all-features --all-targets -- -D warnings
bazel test --nocache_test_results \
  //crates/search:presubmit_tests //:skein_unit_tests \
  //crates/fuzz:skein_fuzz_tests //crates/fuzz:skein_fuzz_cli_tests \
  //:skein_linux_ci_fuzz_smoke_test
```

The 4 MiB source default and 4096-byte term default remain unchanged. Input/spool,
decoder/hydration, minimum analyzer working units, mini-delta lifecycle, shared
resource governance, cancellation and complete-corpus qualification must still be
addressed before removing the fixed source ceiling. #325's long-token policy and
#206's complete-corpus acceptance remain independent gates.
