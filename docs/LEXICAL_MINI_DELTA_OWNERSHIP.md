# Lexical Mini-Delta Ownership

The persistent `SearchIndex` lexical path previously deep-cloned historical
base terms during replacement, deletion and restoration. Each query also cloned
the complete mini-delta before scoring. A small current document could therefore
cause document-sized temporary allocations repeatedly until checkpointing.

Queries now retain an immutable `Arc<LexicalMiniDelta>` at the existing delta
capture point and release the mutation mutex before scoring and positioned I/O.
Writers use `Arc::make_mut`: a uniquely owned delta updates in place; a retained
snapshot causes only the document-ID maps to be copied. Map values own immutable
per-document records through `Arc`, and replacements/tombstones share immutable
base term sets. This keeps both changed and unchanged documents in older delta
snapshots valid without copying their term maps.

The private document records deliberately do not implement `Clone`. Reusing
base terms is an ownership operation; modifying a document still creates and
admits a new analyzed record. Admission failure leaves the logical delta state
unchanged. Checkpoint and projection invalidation replace the active delta,
while already retained delta snapshots remain valid.

Public Rust interfaces, query results, analyzer behavior, tf/df/document lengths,
BM25, logical admission estimates, defaults and persisted formats are unchanged.
This does not introduce a new transaction or atomic reader/checkpoint protocol.

## Allocation evidence

The regression uses the public persistent `SearchIndex` path and
`try_search_with_options`, and checks the reported segmented lexical backend.
It checkpoints a document with 64 or 4,096 input identifiers, then replaces
the content with the same small document. A query term sorting after the old
terms avoids unrelated posting-block decoding in the measured query. Current
documents and complete results are identical between the two fixture sizes.
The test covers query, repeated update, deletion, restoration and checkpoint
reopen; fixture creation and input construction are outside measurement.

These are allocator-requested bytes, including reallocations, from a Linux
x86_64 debug test run with Rust 1.97.1. They are not RSS, peak live memory,
elapsed-time improvements or portable exact byte thresholds.

| Operation | Before, 64 identifiers | Before, 4,096 identifiers | After, 64 identifiers | After, 4,096 identifiers |
| --- | ---: | ---: | ---: | ---: |
| Query | 20,512 | 591,900 | 9,279 | 9,291 |
| Repeated update | 10,794 | 582,170 | 1,208 | 1,208 |
| Delete | 10,378 | 581,754 | 376 | 376 |
| Restore | 10,794 | 582,170 | 1,208 | 1,208 |

The original implementation fails the growth assertion on the same input.
The regression allows 8 KiB of incidental allocation growth instead of pinning
allocator-specific exact totals. The instrumentation is shared with the
existing identifier allocation test and runs in separate integration-test
binaries, not in the production allocator.

## Correctness and verification

Retained-snapshot tests cover replacement, deletion, restoration, exact/one-short
mini-delta admission, analyzer rejection, checkpoint reset and invalidation.
An independent legacy analyzer traversal checks complete corpus statistics,
term frequencies through BM25 scores, matching counts and active logical byte
charges. A local three-seed campaign runs 64 lifecycle operations per seed while
retaining older snapshots. It remains ignored in ordinary tests and is included
only in the existing explicit local fuzz suite.

```sh
cargo test -p skein-search --all-features -- --include-ignored
cargo clippy -p skein-search --all-targets --all-features -- -D warnings
bazel test //crates/search:presubmit_tests
bazel test //crates/fuzz:skein_fuzz_tests //crates/fuzz:skein_fuzz_cli_tests //:skein_linux_ci_fuzz_smoke_test
```

## Remaining #392 boundaries

This removes repeated retained-state copies; it does not complete large-document
support. The caller still owns complete source strings, initial/new-document
analysis still has its admitted map and opaque analyzer working sets, and the
mini-delta limit still governs its existing logical active-state estimate.
Retained snapshots, reference-count metadata and other process allocations do
not become an exact RSS ledger through this change. Initial analysis before
final mini-delta admission and first-time base-term construction remain separate
working units.

Shared host/process governance, streaming input, adaptive profiles, cancellation
and the complete supported lifecycle/corpus qualification remain in #392/#186.
The fixed 4 MiB source ceiling and #206's full-corpus requirement remain intact.
